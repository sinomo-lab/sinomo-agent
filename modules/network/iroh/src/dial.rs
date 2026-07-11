//! Iroh dial client for daemon control RPCs (the
//! `send_iroh_daemon_control_request_*` family moved out of
//! `crates/sinomo-network/src/roaming.rs` in stage C).
//!
//! The module-ABI dial works on whole frames: the caller supplies the encoded
//! request frame and receives the raw response frame bytes. Retry semantics
//! match the canonical route client: connection-establishment failures retry
//! within one aggregate connect deadline. Once any request bytes may have been
//! accepted, the attempt returns to the route layer so a retry can be freshly
//! signed instead of reusing a consumed daemon-auth nonce.

use crate::frames::{IrohProxyByteBudget, read_proxy_frame_async, write_all_frame_async};
use crate::proxy::{
    IROH_DAEMON_PROXY_ERROR_ROUTE, IROH_ROAMING_PROTOCOL, sinomo_iroh_endpoint_builder,
};
use crate::{
    IROH_DAEMON_COMMAND_MAX_BYTES,
    endpoint::{IrohEndpointRecord, iroh_endpoint_record_from_address},
};
use sinomo_core::network::{
    DaemonRpcTimeouts, NetworkDeliveryState, NetworkError, NetworkTimeoutPhase,
    OBJECT_STREAM_MAX_CHUNK_BYTES, ObjectRpcResponse, ObjectRpcResponseStreamMode,
    RawByteStreamChunkOptions, decode_single_object_frame_from_slice, encode_object_frame_to_vec,
    object_rpc_response_stream_mode, read_raw_byte_stream_chunks_to_vec_async,
};
use sinomo_core::node::{ClusterId, NodeId};
use std::collections::HashMap;
use std::time::Duration;

const IROH_DAEMON_RPC_MAX_ATTEMPTS: usize = 3;
const IROH_DAEMON_RPC_RETRY_INITIAL: Duration = Duration::from_millis(40);
const IROH_DAEMON_RPC_RETRY_MAX: Duration = Duration::from_millis(250);

/// Long-lived client side of the Iroh daemon transport.
///
/// One module instance owns one Iroh endpoint and one cached connection per
/// remote endpoint public key. Each daemon RPC gets its own bidirectional
/// stream on the cached connection; broken connections are evicted so normal
/// retry/recovery can reconnect.
#[derive(Default)]
pub struct IrohDaemonDialer {
    endpoint: tokio::sync::Mutex<Option<iroh::Endpoint>>,
    connections: tokio::sync::Mutex<HashMap<String, CachedIrohConnection>>,
}

struct CachedIrohConnection {
    connection: iroh::endpoint::Connection,
}

#[derive(Clone, Copy, Debug)]
struct IrohConnectDeadline {
    expires_at: Option<tokio::time::Instant>,
    timeout: Option<Duration>,
}

impl IrohConnectDeadline {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            expires_at: timeout.map(|timeout| tokio::time::Instant::now() + timeout),
            timeout,
        }
    }

    fn remaining(&self) -> Result<Option<Duration>, NetworkError> {
        let Some(expires_at) = self.expires_at else {
            return Ok(None);
        };
        let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(iroh_daemon_command_timeout_error(
                self.timeout.unwrap_or_default(),
            ));
        }
        Ok(Some(remaining))
    }
}

impl IrohDaemonDialer {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn dial_daemon_control_frame_async(
        &self,
        address: &str,
        request_frame: &[u8],
        timeouts: DaemonRpcTimeouts,
    ) -> Result<Vec<u8>, NetworkError> {
        let record = iroh_endpoint_record_from_address(
            address,
            &ClusterId::from_legacy_named("cluster_iroh_dial_placeholder")
                .expect("static cluster id"),
            &NodeId::from_legacy_named("node_iroh_dial_placeholder").expect("static node id"),
        )?;
        let remote_addr = iroh_endpoint_addr_from_record(&record)?;
        let cache_key = iroh_daemon_connection_cache_key(&record);
        let connect_deadline = IrohConnectDeadline::new(timeouts.connect_timeout);
        let mut attempt = 0_usize;
        loop {
            attempt += 1;
            match dial_iroh_daemon_control_frame_attempt_async(
                self,
                &cache_key,
                address,
                remote_addr.clone(),
                request_frame,
                timeouts,
                connect_deadline,
            )
            .await
            {
                Ok(response) => return Ok(response),
                Err(error) => {
                    let retry = iroh_daemon_control_attempt_should_retry(&error, attempt);
                    let error = error.into_network_error();
                    if !retry {
                        return Err(error);
                    }
                    let remaining = connect_deadline.remaining()?;
                    let Some(delay) = iroh_daemon_rpc_retry_delay_with_budget(attempt, remaining)
                    else {
                        return Err(error);
                    };
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub async fn shutdown(&self) {
        let connections = {
            let mut guard = self.connections.lock().await;
            std::mem::take(&mut *guard)
        };
        for cached in connections.into_values() {
            cached.connection.close(0_u8.into(), b"module shutdown");
        }
        if let Some(endpoint) = self.endpoint.lock().await.take() {
            endpoint.close().await;
        }
    }

    async fn endpoint(
        &self,
        deadline: IrohConnectDeadline,
    ) -> Result<iroh::Endpoint, NetworkError> {
        let mut guard = iroh_mutex_lock_before_connect_deadline(&self.endpoint, deadline).await?;
        if let Some(endpoint) = guard.as_ref()
            && !endpoint.is_closed()
        {
            return Ok(endpoint.clone());
        }
        let bind = async {
            sinomo_iroh_endpoint_builder()
                .map_err(|source| source.to_string())?
                .bind()
                .await
                .map_err(|source| source.to_string())
        };
        let endpoint = if let Some(connect_timeout) = deadline.remaining()? {
            match tokio::time::timeout(connect_timeout, bind).await {
                Ok(Ok(endpoint)) => endpoint,
                Ok(Err(source)) => {
                    return Err(NetworkError::Io {
                        operation: "bind iroh client endpoint".to_string(),
                        detail: source.to_string(),
                    });
                }
                Err(_) => return Err(iroh_daemon_command_timeout_error(connect_timeout)),
            }
        } else {
            match bind.await {
                Ok(endpoint) => endpoint,
                Err(source) => {
                    return Err(NetworkError::Io {
                        operation: "bind iroh client endpoint".to_string(),
                        detail: source.to_string(),
                    });
                }
            }
        };
        if let Some(connect_timeout) = deadline.remaining()? {
            let online_timeout = connect_timeout.min(Duration::from_millis(1000));
            let _ = tokio::time::timeout(online_timeout, endpoint.online()).await;
        }
        *guard = Some(endpoint.clone());
        Ok(endpoint)
    }

    async fn connection(
        &self,
        cache_key: &str,
        _address: &str,
        remote_addr: iroh::EndpointAddr,
        deadline: IrohConnectDeadline,
    ) -> Result<iroh::endpoint::Connection, NetworkError> {
        {
            let connections =
                iroh_mutex_lock_before_connect_deadline(&self.connections, deadline).await?;
            if let Some(cached) = connections.get(cache_key) {
                return Ok(cached.connection.clone());
            }
        }

        let endpoint = self.endpoint(deadline).await?;
        let connect = endpoint.connect(remote_addr, IROH_ROAMING_PROTOCOL.as_bytes());
        let connection = if let Some(connect_timeout) = deadline.remaining()? {
            match tokio::time::timeout(connect_timeout, connect).await {
                Ok(Ok(connection)) => connection,
                Ok(Err(source)) => {
                    return Err(NetworkError::Io {
                        operation: "connect iroh endpoint".to_string(),
                        detail: source.to_string(),
                    });
                }
                Err(_) => return Err(iroh_daemon_command_timeout_error(connect_timeout)),
            }
        } else {
            match connect.await {
                Ok(connection) => connection,
                Err(source) => {
                    return Err(NetworkError::Io {
                        operation: "connect iroh endpoint".to_string(),
                        detail: source.to_string(),
                    });
                }
            }
        };
        let mut connections =
            iroh_mutex_lock_before_connect_deadline(&self.connections, deadline).await?;
        if let Some(cached) = connections.get(cache_key) {
            // Another request connected while this one was in flight. Reuse
            // the installed connection and let the duplicate close on drop.
            return Ok(cached.connection.clone());
        }
        connections.insert(
            cache_key.to_string(),
            CachedIrohConnection {
                connection: connection.clone(),
            },
        );
        Ok(connection)
    }

    async fn evict_connection(&self, cache_key: &str, reason: &'static [u8]) {
        let removed = self.connections.lock().await.remove(cache_key);
        if let Some(cached) = removed {
            cached.connection.close(0_u8.into(), reason);
        }
    }
}

async fn iroh_mutex_lock_before_connect_deadline<'a, T>(
    mutex: &'a tokio::sync::Mutex<T>,
    deadline: IrohConnectDeadline,
) -> Result<tokio::sync::MutexGuard<'a, T>, NetworkError> {
    let Some(expires_at) = deadline.expires_at else {
        return Ok(mutex.lock().await);
    };
    match tokio::time::timeout_at(expires_at, mutex.lock()).await {
        Ok(guard) => Ok(guard),
        Err(_) => Err(iroh_daemon_command_timeout_error(
            deadline.timeout.unwrap_or_default(),
        )),
    }
}

/// Dials one daemon control request frame to an `iroh://` address and returns
/// the raw response frame bytes.
#[cfg(test)]
pub async fn dial_iroh_daemon_control_frame_async(
    address: &str,
    request_frame: &[u8],
    timeouts: DaemonRpcTimeouts,
) -> Result<Vec<u8>, NetworkError> {
    let dialer = IrohDaemonDialer::new();
    let result = dialer
        .dial_daemon_control_frame_async(address, request_frame, timeouts)
        .await;
    dialer.shutdown().await;
    result
}

async fn dial_iroh_daemon_control_frame_attempt_async(
    dialer: &IrohDaemonDialer,
    cache_key: &str,
    address: &str,
    remote_addr: iroh::EndpointAddr,
    request_frame: &[u8],
    timeouts: DaemonRpcTimeouts,
    connect_deadline: IrohConnectDeadline,
) -> Result<Vec<u8>, IrohDaemonControlAttemptError> {
    let connection = dialer
        .connection(cache_key, address, remote_addr, connect_deadline)
        .await
        .map_err(IrohDaemonControlAttemptError::BeforeRequestAccepted)?;

    let open = connection.open_bi();
    let (mut send, mut recv) = if let Some(connect_timeout) = connect_deadline
        .remaining()
        .map_err(IrohDaemonControlAttemptError::BeforeRequestAccepted)?
    {
        match tokio::time::timeout(connect_timeout, open).await {
            Ok(Ok(streams)) => streams,
            Ok(Err(source)) => {
                dialer
                    .evict_connection(cache_key, b"open stream failed")
                    .await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "open iroh daemon stream".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
            Err(_) => {
                dialer
                    .evict_connection(cache_key, b"open stream timed out")
                    .await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    iroh_daemon_command_timeout_error(connect_timeout),
                ));
            }
        }
    } else {
        match open.await {
            Ok(streams) => streams,
            Err(source) => {
                dialer
                    .evict_connection(cache_key, b"open stream failed")
                    .await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "open iroh daemon stream".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
        }
    };

    let write_result = write_all_frame_async(
        &mut send,
        request_frame,
        "write iroh daemon command",
        timeouts.idle_timeout,
    )
    .await;
    if let Err(source) = write_result {
        dialer.evict_connection(cache_key, b"write failed").await;
        drop(send);
        drop(recv);
        return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(source));
    }
    if let Err(source) = send.finish() {
        dialer.evict_connection(cache_key, b"finish failed").await;
        drop(send);
        drop(recv);
        return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(
            NetworkError::Io {
                operation: "finish iroh daemon command".to_string(),
                detail: source.to_string(),
            },
        ));
    }
    // One response frame per dial; a fresh budget keeps the frame-size
    // accounting shared with the proxy path. If that response announces a
    // streamed object-RPC body, the body chunks belong to the same generic
    // network transfer and are collected into the returned response frame for
    // the pinned v1 module ABI.
    let response_budget =
        IrohProxyByteBudget::new(crate::frames::IROH_DAEMON_PROXY_COMMAND_BUDGET_BYTES);
    let response_frame = match read_proxy_frame_async(
        &mut recv,
        &response_budget,
        "response",
        timeouts.idle_timeout,
    )
    .await
    {
        Ok((frame, _permit)) => frame,
        Err(error) => {
            dialer.evict_connection(cache_key, b"read failed").await;
            drop(recv);
            return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(
                iroh_daemon_response_frame_error(error),
            ));
        }
    };
    if let Some(error) = iroh_daemon_proxy_forward_error(&response_frame) {
        return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(error));
    }
    let response =
        match collect_streamed_response_body_if_needed(&mut recv, response_frame, timeouts).await {
            Ok(response) => response,
            Err(error) => {
                dialer.evict_connection(cache_key, b"read failed").await;
                drop(recv);
                return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(
                    iroh_daemon_response_frame_error(error),
                ));
            }
        };
    drop(recv);
    Ok(response)
}

fn iroh_daemon_proxy_forward_error(response_frame: &[u8]) -> Option<NetworkError> {
    let response: ObjectRpcResponse =
        decode_single_object_frame_from_slice(response_frame, IROH_DAEMON_COMMAND_MAX_BYTES)
            .ok()?;
    if response.route != IROH_DAEMON_PROXY_ERROR_ROUTE {
        return None;
    }
    let detail = response
        .error_detail()
        .err()
        .map(|error| error.to_string())
        .unwrap_or_else(|| "iroh daemon proxy reported a forwarding failure".to_string());
    Some(NetworkError::delivery_failed(
        "forward iroh command to target daemon",
        NetworkDeliveryState::PossiblyApplied,
        detail,
    ))
}

async fn collect_streamed_response_body_if_needed<R>(
    reader: &mut R,
    response_frame: Vec<u8>,
    timeouts: DaemonRpcTimeouts,
) -> Result<Vec<u8>, NetworkError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut response: ObjectRpcResponse =
        decode_single_object_frame_from_slice(&response_frame, IROH_DAEMON_COMMAND_MAX_BYTES)?;
    if object_rpc_response_stream_mode(&response)
        != ObjectRpcResponseStreamMode::RawStreamedDaemonBody
    {
        return Ok(response_frame);
    }
    let mut body = Vec::new();
    read_raw_byte_stream_chunks_to_vec_async(
        reader,
        &mut body,
        RawByteStreamChunkOptions {
            chunk_bytes: OBJECT_STREAM_MAX_CHUNK_BYTES,
            max_total_bytes: u64::try_from(IROH_DAEMON_COMMAND_MAX_BYTES).ok(),
            idle_timeout: timeouts.idle_timeout,
        },
    )
    .await?;
    response.body = body;
    encode_object_frame_to_vec(&response)
}

fn iroh_daemon_connection_cache_key(record: &IrohEndpointRecord) -> String {
    record.endpoint_public_key.clone()
}

#[derive(Debug)]
enum IrohDaemonControlAttemptError {
    BeforeRequestAccepted(NetworkError),
    AfterRequestAccepted(NetworkError),
}

impl IrohDaemonControlAttemptError {
    fn into_network_error(self) -> NetworkError {
        match self {
            Self::BeforeRequestAccepted(error) => {
                iroh_error_with_delivery_state(error, NetworkDeliveryState::DefinitelyNotSent)
            }
            Self::AfterRequestAccepted(error) => {
                iroh_error_with_delivery_state(error, NetworkDeliveryState::PossiblyApplied)
            }
        }
    }
}

fn iroh_daemon_control_attempt_should_retry(
    error: &IrohDaemonControlAttemptError,
    attempt: usize,
) -> bool {
    if attempt >= IROH_DAEMON_RPC_MAX_ATTEMPTS {
        return false;
    }
    matches!(
        error,
        IrohDaemonControlAttemptError::BeforeRequestAccepted(_)
    )
}

fn iroh_error_with_delivery_state(
    error: NetworkError,
    delivery_state: NetworkDeliveryState,
) -> NetworkError {
    if error.delivery_state().is_some() {
        error
    } else {
        NetworkError::delivery_failed(
            "send iroh daemon command",
            delivery_state,
            error.to_string(),
        )
    }
}

fn iroh_daemon_response_frame_error(error: NetworkError) -> NetworkError {
    match error {
        NetworkError::Io { detail, .. } => NetworkError::Io {
            operation: "read iroh daemon response".to_string(),
            detail,
        },
        error => error,
    }
}

fn iroh_daemon_rpc_retry_delay_with_budget(
    attempt: usize,
    remaining: Option<Duration>,
) -> Option<Duration> {
    if attempt >= IROH_DAEMON_RPC_MAX_ATTEMPTS
        || remaining.is_some_and(|remaining| remaining <= Duration::from_millis(5))
    {
        return None;
    }
    let shift = attempt.saturating_sub(1).min(5);
    let delay = IROH_DAEMON_RPC_RETRY_INITIAL
        .saturating_mul(2_u32.saturating_pow(u32::try_from(shift).unwrap_or(5)))
        .min(IROH_DAEMON_RPC_RETRY_MAX);
    Some(match remaining {
        Some(remaining) => delay.min(remaining.saturating_sub(Duration::from_millis(1))),
        None => delay,
    })
}

fn iroh_daemon_command_timeout_error(timeout: Duration) -> NetworkError {
    NetworkError::timeout(
        "send iroh daemon command",
        NetworkTimeoutPhase::Connect,
        timeout,
        NetworkDeliveryState::DefinitelyNotSent,
    )
}

/// Reconstructs an `iroh::EndpointAddr` from the transport-neutral record
/// (`sinomo-network` keeps the record type; this module owns the iroh-typed
/// half).
pub(crate) fn iroh_endpoint_addr_from_record(
    record: &IrohEndpointRecord,
) -> Result<iroh::EndpointAddr, NetworkError> {
    let public_key =
        record
            .endpoint_public_key
            .parse()
            .map_err(|source| NetworkError::Serialization {
                detail: format!("invalid iroh public key: {source}"),
            })?;
    let mut addr = iroh::EndpointAddr::new(public_key);
    for direct in &record.direct_addresses {
        let socket = direct
            .parse()
            .map_err(|source| NetworkError::Serialization {
                detail: format!("invalid iroh direct address `{direct}`: {source}"),
            })?;
        addr = addr.with_ip_addr(socket);
    }
    if let Some(relay_url) = &record.relay_url {
        let relay = relay_url
            .parse()
            .map_err(|source| NetworkError::Serialization {
                detail: format!("invalid iroh relay url `{relay_url}`: {source}"),
            })?;
        addr = addr.with_relay_url(relay);
    }
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_forward_error_becomes_ambiguous_transport_failure() {
        let response = ObjectRpcResponse::encode_error(
            IROH_DAEMON_PROXY_ERROR_ROUTE,
            "local daemon response timed out",
        )
        .expect("proxy error response");
        let frame = encode_object_frame_to_vec(&response).expect("proxy error frame");

        let error = iroh_daemon_proxy_forward_error(&frame).expect("proxy forwarding error");
        assert_eq!(
            error.delivery_state(),
            Some(NetworkDeliveryState::PossiblyApplied)
        );
        assert!(
            error
                .to_string()
                .contains("local daemon response timed out")
        );

        let attempt = IrohDaemonControlAttemptError::AfterRequestAccepted(error);
        assert_eq!(
            attempt.into_network_error().delivery_state(),
            Some(NetworkDeliveryState::PossiblyApplied)
        );
    }

    #[test]
    fn ordinary_application_error_is_not_a_proxy_transport_failure() {
        let response = ObjectRpcResponse::encode_error("/test/application", "rejected")
            .expect("application error response");
        let frame = encode_object_frame_to_vec(&response).expect("application error frame");

        assert!(iroh_daemon_proxy_forward_error(&frame).is_none());
    }

    #[test]
    fn attempt_retry_is_limited_to_pre_accept_failures() {
        let before_accept =
            IrohDaemonControlAttemptError::BeforeRequestAccepted(NetworkError::Io {
                operation: "connect iroh endpoint".to_string(),
                detail: "connect failed".to_string(),
            });
        assert!(iroh_daemon_control_attempt_should_retry(&before_accept, 1));

        let after_accept = IrohDaemonControlAttemptError::AfterRequestAccepted(NetworkError::Io {
            operation: "read iroh daemon response".to_string(),
            detail: "peer closed before object frame length (4 bytes expected)".to_string(),
        });
        assert!(!iroh_daemon_control_attempt_should_retry(&after_accept, 1));
        assert!(!iroh_daemon_control_attempt_should_retry(&before_accept, 3));
    }

    #[test]
    fn attempt_error_exposes_delivery_certainty() {
        let before = IrohDaemonControlAttemptError::BeforeRequestAccepted(NetworkError::Io {
            operation: "connect iroh endpoint".to_string(),
            detail: "connect failed".to_string(),
        })
        .into_network_error();
        assert_eq!(
            before.delivery_state(),
            Some(NetworkDeliveryState::DefinitelyNotSent)
        );

        let after = IrohDaemonControlAttemptError::AfterRequestAccepted(NetworkError::Io {
            operation: "read iroh daemon response".to_string(),
            detail: "peer closed".to_string(),
        })
        .into_network_error();
        assert_eq!(
            after.delivery_state(),
            Some(NetworkDeliveryState::PossiblyApplied)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn connect_deadline_tracks_one_aggregate_budget() {
        let deadline = IrohConnectDeadline::new(Some(Duration::from_secs(5)));
        assert_eq!(deadline.remaining(), Ok(Some(Duration::from_secs(5))));
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(deadline.remaining(), Ok(Some(Duration::from_secs(2))));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(matches!(
            deadline.remaining(),
            Err(NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Connect,
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn connect_deadline_bounds_mutex_queueing() {
        let mutex = std::sync::Arc::new(tokio::sync::Mutex::new(()));
        let held = mutex.lock().await;
        let waiting = mutex.clone();
        let deadline = IrohConnectDeadline::new(Some(Duration::from_secs(5)));
        let task = tokio::spawn(async move {
            let _guard = iroh_mutex_lock_before_connect_deadline(&waiting, deadline).await?;
            Ok::<(), NetworkError>(())
        });
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(5)).await;
        let error = task
            .await
            .expect("mutex wait task")
            .expect_err("queued lock acquisition must respect the connect deadline");

        assert!(matches!(
            error,
            NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Connect,
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            }
        ));
        drop(held);
    }

    #[tokio::test]
    async fn streamed_response_body_is_collected_for_module_dial_abi() {
        use sinomo_core::network::{
            ObjectRpcResponseStreamMode, set_object_rpc_response_stream_mode,
        };
        use tokio::io::AsyncWriteExt;

        let mut response = ObjectRpcResponse::from_encoded_body("/test/stream", Vec::new());
        set_object_rpc_response_stream_mode(
            &mut response,
            ObjectRpcResponseStreamMode::RawStreamedDaemonBody,
        );
        let response_frame = encode_object_frame_to_vec(&response).expect("response frame");

        let (mut writer, mut reader) = tokio::io::duplex(128);
        let writer_task = tokio::spawn(async move {
            writer
                .write_all(&5_u64.to_be_bytes())
                .await
                .expect("first chunk length");
            writer.write_all(b"hello").await.expect("first chunk body");
            writer
                .write_all(&6_u64.to_be_bytes())
                .await
                .expect("second chunk length");
            writer
                .write_all(b" world")
                .await
                .expect("second chunk body");
            writer
                .write_all(&0_u64.to_be_bytes())
                .await
                .expect("end chunk length");
        });

        let collected = collect_streamed_response_body_if_needed(
            &mut reader,
            response_frame,
            DaemonRpcTimeouts::default(),
        )
        .await
        .expect("collect streamed body");
        writer_task.await.expect("writer task");
        let collected_response: ObjectRpcResponse =
            decode_single_object_frame_from_slice(&collected, IROH_DAEMON_COMMAND_MAX_BYTES)
                .expect("decode collected response");

        assert_eq!(collected_response.body, b"hello world");
        assert_eq!(
            object_rpc_response_stream_mode(&collected_response),
            ObjectRpcResponseStreamMode::RawStreamedDaemonBody
        );
    }

    #[test]
    fn rpc_retry_delay_is_bounded_by_attempts_and_deadline() {
        assert_eq!(
            iroh_daemon_rpc_retry_delay_with_budget(1, Some(Duration::from_secs(5))),
            Some(Duration::from_millis(40))
        );
        assert_eq!(
            iroh_daemon_rpc_retry_delay_with_budget(2, Some(Duration::from_secs(5))),
            Some(Duration::from_millis(80))
        );
        assert_eq!(
            iroh_daemon_rpc_retry_delay_with_budget(1, Some(Duration::from_millis(10))),
            Some(Duration::from_millis(9))
        );
        assert_eq!(
            iroh_daemon_rpc_retry_delay_with_budget(3, Some(Duration::from_secs(5))),
            None
        );
        assert_eq!(
            iroh_daemon_rpc_retry_delay_with_budget(1, Some(Duration::from_millis(5))),
            None
        );
    }
}
