//! Iroh dial client for daemon control RPCs (the
//! `send_iroh_daemon_control_request_*` family moved out of
//! `crates/sinomo-network/src/roaming.rs` in stage C).
//!
//! The module-ABI dial works on whole frames: the caller supplies the encoded
//! request frame and receives the raw response frame bytes. Retry semantics
//! match the former in-crate client: connection-establishment failures always
//! retry (bounded), post-accept failures retry only for retry-safe requests
//! whose response read failed on a peer close.

use crate::frames::{IrohProxyByteBudget, read_proxy_frame_async, write_all_frame_async};
use crate::proxy::{IROH_ROAMING_PROTOCOL, IrohEndpointCloseGuard, sinomo_iroh_endpoint_builder};
use sinomo_core::network::{
    CLUSTER_HEALTH_ROUTE, DaemonControlRequest, DaemonRpcTimeouts, IROH_DAEMON_COMMAND_MAX_BYTES,
    IrohEndpointRecord, NetworkError, decode_single_object_frame_from_slice,
    iroh_endpoint_record_from_address,
};
use sinomo_core::node::{ClusterId, NodeId};
use std::time::Duration;

const IROH_DAEMON_RPC_MAX_ATTEMPTS: usize = 3;
const IROH_DAEMON_RPC_RETRY_INITIAL: Duration = Duration::from_millis(40);
const IROH_DAEMON_RPC_RETRY_MAX: Duration = Duration::from_millis(250);

/// Dials one daemon control request frame to an `iroh://` address and returns
/// the raw response frame bytes.
pub async fn dial_iroh_daemon_control_frame_async(
    address: &str,
    request_frame: &[u8],
    timeouts: DaemonRpcTimeouts,
) -> Result<Vec<u8>, NetworkError> {
    let record = iroh_endpoint_record_from_address(
        address,
        &ClusterId::from_legacy_named("cluster_iroh_dial_placeholder").expect("static cluster id"),
        &NodeId::from_legacy_named("node_iroh_dial_placeholder").expect("static node id"),
    )?;
    let remote_addr = iroh_endpoint_addr_from_record(&record)?;
    // Retry-safety needs the typed route; an undecodable frame is treated as
    // not retry-safe after the request may have been accepted.
    let request: Option<DaemonControlRequest> =
        decode_single_object_frame_from_slice(request_frame, IROH_DAEMON_COMMAND_MAX_BYTES).ok();
    let mut attempt = 0_usize;
    loop {
        attempt += 1;
        match dial_iroh_daemon_control_frame_attempt_async(
            remote_addr.clone(),
            request_frame,
            timeouts,
        )
        .await
        {
            Ok(response) => return Ok(response),
            Err(error) => {
                let retry =
                    iroh_daemon_control_attempt_should_retry(request.as_ref(), &error, attempt);
                let error = error.into_network_error();
                if !retry {
                    return Err(error);
                }
                let Some(delay) =
                    iroh_daemon_rpc_retry_delay_with_budget(attempt, timeouts.connect_timeout)
                else {
                    return Err(error);
                };
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn dial_iroh_daemon_control_frame_attempt_async(
    remote_addr: iroh::EndpointAddr,
    request_frame: &[u8],
    timeouts: DaemonRpcTimeouts,
) -> Result<Vec<u8>, IrohDaemonControlAttemptError> {
    let bind = async {
        sinomo_iroh_endpoint_builder()
            .map_err(|source| source.to_string())?
            .bind()
            .await
            .map_err(|source| source.to_string())
    };
    let endpoint = if let Some(connect_timeout) = timeouts.connect_timeout {
        match tokio::time::timeout(connect_timeout, bind).await {
            Ok(Ok(endpoint)) => endpoint,
            Ok(Err(source)) => {
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "bind iroh client endpoint".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
            Err(_) => {
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    iroh_daemon_command_timeout_error(connect_timeout),
                ));
            }
        }
    } else {
        match bind.await {
            Ok(endpoint) => endpoint,
            Err(source) => {
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "bind iroh client endpoint".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
        }
    };
    let endpoint = IrohEndpointCloseGuard::new(tokio::runtime::Handle::current(), endpoint);
    if let Some(connect_timeout) = timeouts.connect_timeout {
        let online_timeout = connect_timeout.min(Duration::from_millis(1000));
        let _ = tokio::time::timeout(online_timeout, endpoint.endpoint().online()).await;
    }

    let connect = endpoint
        .endpoint()
        .connect(remote_addr, IROH_ROAMING_PROTOCOL.as_bytes());
    let connection = if let Some(connect_timeout) = timeouts.connect_timeout {
        match tokio::time::timeout(connect_timeout, connect).await {
            Ok(Ok(connection)) => connection,
            Ok(Err(source)) => {
                endpoint.close().await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "connect iroh endpoint".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
            Err(_) => {
                endpoint.close().await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    iroh_daemon_command_timeout_error(connect_timeout),
                ));
            }
        }
    } else {
        match connect.await {
            Ok(connection) => connection,
            Err(source) => {
                endpoint.close().await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "connect iroh endpoint".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
        }
    };

    let open = connection.open_bi();
    let (mut send, mut recv) = if let Some(connect_timeout) = timeouts.connect_timeout {
        match tokio::time::timeout(connect_timeout, open).await {
            Ok(Ok(streams)) => streams,
            Ok(Err(source)) => {
                connection.close(0_u8.into(), b"open stream failed");
                drop(connection);
                endpoint.close().await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "open iroh daemon stream".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
            Err(_) => {
                connection.close(0_u8.into(), b"open stream timed out");
                drop(connection);
                endpoint.close().await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    iroh_daemon_command_timeout_error(connect_timeout),
                ));
            }
        }
    } else {
        match open.await {
            Ok(streams) => streams,
            Err(source) => {
                connection.close(0_u8.into(), b"open stream failed");
                drop(connection);
                endpoint.close().await;
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
        connection.close(0_u8.into(), b"write failed");
        drop(send);
        drop(recv);
        drop(connection);
        endpoint.close().await;
        return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(source));
    }
    if let Err(source) = send.finish() {
        connection.close(0_u8.into(), b"finish failed");
        drop(send);
        drop(recv);
        drop(connection);
        endpoint.close().await;
        return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(
            NetworkError::Io {
                operation: "finish iroh daemon command".to_string(),
                detail: source.to_string(),
            },
        ));
    }
    // One response frame per dial; a fresh budget keeps the frame-size
    // accounting shared with the proxy path.
    let response_budget =
        IrohProxyByteBudget::new(crate::frames::IROH_DAEMON_PROXY_COMMAND_BUDGET_BYTES);
    let response = match read_proxy_frame_async(
        &mut recv,
        &response_budget,
        "response",
        timeouts.idle_timeout,
    )
    .await
    {
        Ok((frame, _permit)) => frame,
        Err(error) => {
            connection.close(0_u8.into(), b"read failed");
            drop(recv);
            drop(connection);
            endpoint.close().await;
            return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(
                iroh_daemon_response_frame_error(error),
            ));
        }
    };
    connection.close(0_u8.into(), b"done");
    drop(recv);
    drop(connection);
    endpoint.close().await;
    Ok(response)
}

#[derive(Debug)]
enum IrohDaemonControlAttemptError {
    BeforeRequestAccepted(NetworkError),
    AfterRequestAccepted(NetworkError),
}

impl IrohDaemonControlAttemptError {
    fn into_network_error(self) -> NetworkError {
        match self {
            Self::BeforeRequestAccepted(error) | Self::AfterRequestAccepted(error) => error,
        }
    }
}

fn iroh_daemon_control_attempt_should_retry(
    request: Option<&DaemonControlRequest>,
    error: &IrohDaemonControlAttemptError,
    attempt: usize,
) -> bool {
    if attempt >= IROH_DAEMON_RPC_MAX_ATTEMPTS {
        return false;
    }
    match error {
        IrohDaemonControlAttemptError::BeforeRequestAccepted(_) => true,
        IrohDaemonControlAttemptError::AfterRequestAccepted(error) => {
            request.is_some_and(|request| {
                iroh_daemon_control_request_allows_response_retry(request)
                    && iroh_daemon_response_error_is_retryable(error)
            })
        }
    }
}

fn iroh_daemon_control_request_allows_response_retry(request: &DaemonControlRequest) -> bool {
    if request.is_response_retry_safe() {
        return true;
    }
    // Read-only cluster KV routes are retry-safe over the proxy. Route names
    // are string contracts owned by sinomo-kv; spelled out here so the module
    // does not link the KV crate for an allowlist (mirrors the former
    // in-crate client).
    matches!(
        request.route.as_str(),
        CLUSTER_HEALTH_ROUTE | "/cluster/kv/range-prefix" | "/cluster/kv/snapshot"
    )
}

fn iroh_daemon_response_error_is_retryable(error: &NetworkError) -> bool {
    matches!(
        error,
        NetworkError::Io { operation, detail }
            if operation == "read iroh daemon response"
                && (detail.starts_with("peer closed before object frame")
                    || detail.starts_with("peer closed during object frame"))
    )
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
    NetworkError::Io {
        operation: "send iroh daemon command".to_string(),
        detail: format!("timed out after {}ms", timeout.as_millis()),
    }
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
    use sinomo_core::network::ObjectRpcRequest;

    #[test]
    fn response_retry_is_limited_to_idempotent_routes() {
        let retry_safe = ObjectRpcRequest::encode_body("/test/retry-safe", "test", &())
            .expect("retry-safe request")
            .with_response_retry_safe();
        assert!(iroh_daemon_control_request_allows_response_retry(
            &retry_safe
        ));

        let range = ObjectRpcRequest::encode_body("/cluster/kv/range-prefix", "cluster.kv", &())
            .expect("range request");
        assert!(iroh_daemon_control_request_allows_response_retry(&range));

        for route in ["/test/mutate", "/test/finalize", "/test/build"] {
            let request = ObjectRpcRequest::encode_body(route, "test", &()).expect("rpc request");
            assert!(
                !iroh_daemon_control_request_allows_response_retry(&request),
                "{route} should not be retried after the remote may have accepted the request"
            );
        }
    }

    #[test]
    fn response_retry_classifier_matches_remote_close_only() {
        assert!(iroh_daemon_response_error_is_retryable(&NetworkError::Io {
            operation: "read iroh daemon response".to_string(),
            detail: "peer closed before object frame length (4 bytes expected)".to_string(),
        }));
        assert!(iroh_daemon_response_error_is_retryable(&NetworkError::Io {
            operation: "read iroh daemon response".to_string(),
            detail: "peer closed during object frame body: read 3 of 8 bytes".to_string(),
        }));
        assert!(!iroh_daemon_response_error_is_retryable(
            &NetworkError::Io {
                operation: "read iroh daemon response".to_string(),
                detail: "timed out after 1000ms".to_string(),
            }
        ));
        assert!(!iroh_daemon_response_error_is_retryable(
            &NetworkError::Io {
                operation: "connect iroh endpoint".to_string(),
                detail: "peer closed before object frame length (4 bytes expected)".to_string(),
            }
        ));
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
