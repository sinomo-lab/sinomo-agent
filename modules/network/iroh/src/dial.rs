//! Iroh dial client for raw, full-duplex daemon RPC streams.
//!
//! Connection establishment is retried within one aggregate connect budget.
//! No object-RPC or transfer bytes are interpreted in this module.

use crate::endpoint::{IrohEndpointRecord, iroh_endpoint_record_from_address};
use crate::proxy::{IROH_ROAMING_PROTOCOL, sinomo_iroh_endpoint_builder};
use sinomo_core::network::{
    DaemonRpcTimeouts, NetworkDeliveryState, NetworkError, NetworkTimeoutPhase,
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

    /// Opens one native QUIC bidirectional stream without interpreting the
    /// bytes carried over it. The caller/session engine owns all RPC and
    /// transfer framing above this point.
    pub async fn open_daemon_stream_async(
        &self,
        address: &str,
        timeouts: DaemonRpcTimeouts,
    ) -> Result<(iroh::endpoint::SendStream, iroh::endpoint::RecvStream), NetworkError> {
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
            match open_iroh_daemon_stream_attempt_async(
                self,
                &cache_key,
                address,
                remote_addr.clone(),
                connect_deadline,
            )
            .await
            {
                Ok(stream) => return Ok(stream),
                Err(error) => {
                    let error = iroh_error_with_delivery_state(
                        error,
                        NetworkDeliveryState::DefinitelyNotSent,
                    );
                    if attempt >= IROH_DAEMON_RPC_MAX_ATTEMPTS {
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

async fn open_iroh_daemon_stream_attempt_async(
    dialer: &IrohDaemonDialer,
    cache_key: &str,
    address: &str,
    remote_addr: iroh::EndpointAddr,
    connect_deadline: IrohConnectDeadline,
) -> Result<(iroh::endpoint::SendStream, iroh::endpoint::RecvStream), NetworkError> {
    let connection = dialer
        .connection(cache_key, address, remote_addr, connect_deadline)
        .await?;
    let open = connection.open_bi();
    if let Some(connect_timeout) = connect_deadline.remaining()? {
        match tokio::time::timeout(connect_timeout, open).await {
            Ok(Ok(streams)) => Ok(streams),
            Ok(Err(source)) => {
                dialer
                    .evict_connection(cache_key, b"open stream failed")
                    .await;
                Err(NetworkError::Io {
                    operation: "open iroh daemon stream".to_string(),
                    detail: source.to_string(),
                })
            }
            Err(_) => {
                dialer
                    .evict_connection(cache_key, b"open stream timed out")
                    .await;
                Err(iroh_daemon_command_timeout_error(connect_timeout))
            }
        }
    } else {
        match open.await {
            Ok(streams) => Ok(streams),
            Err(source) => {
                dialer
                    .evict_connection(cache_key, b"open stream failed")
                    .await;
                Err(NetworkError::Io {
                    operation: "open iroh daemon stream".to_string(),
                    detail: source.to_string(),
                })
            }
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

fn iroh_daemon_connection_cache_key(record: &IrohEndpointRecord) -> String {
    record.endpoint_public_key.clone()
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
    fn open_error_exposes_definitely_unsent_delivery_certainty() {
        let before = iroh_error_with_delivery_state(
            NetworkError::Io {
                operation: "connect iroh endpoint".to_string(),
                detail: "connect failed".to_string(),
            },
            NetworkDeliveryState::DefinitelyNotSent,
        );
        assert_eq!(
            before.delivery_state(),
            Some(NetworkDeliveryState::DefinitelyNotSent)
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
