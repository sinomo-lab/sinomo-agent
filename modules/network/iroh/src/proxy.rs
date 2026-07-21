//! Iroh daemon proxy: endpoint builder + relay-mode config, secret-key
//! load/create, and the accept/forward loop that relays inbound full-duplex
//! byte streams to the local daemon control address over TCP.
//!
//! Moved out of `crates/sinomo-network/src/roaming.rs` in stage C of
//! docs/plans/r0-r1-crate-boundary-refactor.md. The proxy deliberately does
//! not interpret object-RPC/session frames or collect transfer bodies.

use crate::endpoint::{IrohEndpointRecord, IrohRelayMode, iroh_endpoint_dir};
use fs2::FileExt;
use sinomo_core::ClusterTransportKind;
use sinomo_core::network::{
    AuthenticatedTransportMetadata, ClusterPaths, DEFAULT_NETWORK_TIMEOUT,
    DirectStreamCapabilities, NetworkError, read_object_file, read_optional_object_file,
    write_authenticated_transport_prelude, write_object_file_create_new_serialized,
};
use sinomo_core::node::{ClusterId, NodeId};
#[cfg(test)]
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

pub const IROH_ROAMING_PROTOCOL: &str = "sinomo.iroh-roaming.v1";
pub const IROH_AUTHENTICATED_LINK_EXPORTER_LABEL: &[u8] =
    b"EXPORTER-sinomo-iroh-authenticated-link-v1";
const SINOMO_IROH_RELAY_URLS_ENV: &str = "SINOMO_IROH_RELAY_URLS";
const IROH_DAEMON_PROXY_RESTART_BACKOFF_MIN: Duration = Duration::from_secs(5);
const IROH_DAEMON_PROXY_RESTART_BACKOFF_MAX: Duration = Duration::from_secs(300);
const IROH_DAEMON_PROXY_RESTART_JITTER_MAX: Duration = Duration::from_secs(30);
const IROH_DAEMON_PROXY_STABLE_RUNTIME: Duration = Duration::from_secs(60);
const IROH_SECRET_KEY_LOCK_WAIT: Duration = Duration::from_secs(2);
const IROH_SECRET_KEY_LOCK_RETRY_INITIAL: Duration = Duration::from_millis(2);
const IROH_SECRET_KEY_LOCK_RETRY_MAX: Duration = Duration::from_millis(40);
const IROH_DAEMON_PROXY_MAX_CONNECTIONS: usize = 64;
const IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS: usize = 128;
// This is a lifecycle timeout for an otherwise healthy pooled Iroh
// connection, not a no-progress timeout for an active transport stream.
const IROH_DAEMON_PROXY_CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const SINOMO_DEFAULT_IROH_RELAY_URLS: &[&str] = &[
    "https://use1-1.relay.n0.iroh-canary.iroh.link./",
    "https://usw1-1.relay.n0.iroh-canary.iroh.link./",
    "https://euc1-1.relay.n0.iroh-canary.iroh.link./",
    "https://aps1-1.relay.n0.iroh-canary.iroh.link./",
];

/// Relay URL list from `SINOMO_IROH_RELAY_URLS` (comma-separated), falling
/// back to the Sinomo defaults when unset/empty.
fn sinomo_iroh_relay_url_strings_from_env(value: Option<String>) -> Vec<String> {
    let Some(value) = value else {
        return SINOMO_DEFAULT_IROH_RELAY_URLS
            .iter()
            .map(|url| (*url).to_string())
            .collect();
    };
    let urls: Vec<_> = value
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if urls.is_empty() {
        SINOMO_DEFAULT_IROH_RELAY_URLS
            .iter()
            .map(|url| (*url).to_string())
            .collect()
    } else {
        urls
    }
}

fn sinomo_iroh_relay_mode() -> Result<iroh::RelayMode, NetworkError> {
    let Some(relay_env) = std::env::var(SINOMO_IROH_RELAY_URLS_ENV).ok() else {
        return Ok(iroh::RelayMode::Default);
    };
    if relay_env.split(',').all(|entry| entry.trim().is_empty()) {
        return Ok(iroh::RelayMode::Default);
    }
    let relay_urls = sinomo_iroh_relay_url_strings_from_env(Some(relay_env));
    let parsed = relay_urls
        .iter()
        .map(|url| {
            url.parse().map_err(|source| NetworkError::Serialization {
                detail: format!(
                    "invalid iroh relay url `{url}` from {SINOMO_IROH_RELAY_URLS_ENV}: {source}"
                ),
            })
        })
        .collect::<Result<Vec<iroh::RelayUrl>, NetworkError>>()?;
    Ok(iroh::RelayMode::custom(parsed))
}

pub(crate) fn sinomo_iroh_endpoint_builder() -> Result<iroh::endpoint::Builder, NetworkError> {
    Ok(iroh::Endpoint::builder(iroh::endpoint::presets::N0).relay_mode(sinomo_iroh_relay_mode()?))
}

fn iroh_endpoint_key_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    iroh_endpoint_dir(paths).join(format!("{}.secret.sobj", node_id.as_str()))
}

fn iroh_endpoint_key_lock_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    iroh_endpoint_dir(paths).join(format!("{}.secret.lock", node_id.as_str()))
}

fn storage_io(path: &std::path::Path, detail: impl std::fmt::Display) -> NetworkError {
    NetworkError::StorageIo {
        path: path.display().to_string(),
        detail: detail.to_string(),
    }
}

#[cfg(test)]
static IROH_SECRET_KEY_CREATE_NEW_ATTEMPTS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
fn record_iroh_secret_key_create_new_attempt_for_test(path: &PathBuf) {
    if let Ok(mut attempts) = IROH_SECRET_KEY_CREATE_NEW_ATTEMPTS_FOR_TEST.lock() {
        *attempts.entry(path.clone()).or_default() += 1;
    }
}

#[cfg(test)]
fn reset_iroh_secret_key_create_new_attempts_for_test(path: &PathBuf) {
    if let Ok(mut attempts) = IROH_SECRET_KEY_CREATE_NEW_ATTEMPTS_FOR_TEST.lock() {
        attempts.remove(path);
    }
}

#[cfg(test)]
fn iroh_secret_key_create_new_attempts_for_test(path: &PathBuf) -> u64 {
    IROH_SECRET_KEY_CREATE_NEW_ATTEMPTS_FOR_TEST
        .lock()
        .ok()
        .and_then(|attempts| attempts.get(path).copied())
        .unwrap_or(0)
}

fn iroh_secret_key_lock_retry_delay(attempts: u32) -> Duration {
    let shift = attempts.min(5);
    IROH_SECRET_KEY_LOCK_RETRY_INITIAL
        .saturating_mul(2_u32.saturating_pow(shift))
        .min(IROH_SECRET_KEY_LOCK_RETRY_MAX)
}

fn file_lock_is_busy(source: &std::io::Error) -> bool {
    source.kind() == std::io::ErrorKind::WouldBlock
        || matches!(source.raw_os_error(), Some(32 | 33))
}

fn lock_iroh_secret_key_with_wait(
    paths: &ClusterPaths,
    node_id: &NodeId,
    wait: Duration,
) -> Result<fs::File, NetworkError> {
    let lock_path = iroh_endpoint_key_lock_path(paths, node_id);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(|source| storage_io(parent, source))?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| storage_io(&lock_path, source))?;
    let deadline = std::time::Instant::now() + wait;
    let mut attempts = 0_u32;
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(source) if file_lock_is_busy(&source) => {
                let now = std::time::Instant::now();
                if wait.is_zero() || now >= deadline {
                    return Err(storage_io(
                        &lock_path,
                        format!("timed out waiting for Iroh secret key lock after {wait:?}"),
                    ));
                }
                let delay = iroh_secret_key_lock_retry_delay(attempts)
                    .min(deadline.saturating_duration_since(now));
                std::thread::sleep(delay);
                attempts = attempts.saturating_add(1);
            }
            Err(source) => return Err(storage_io(&lock_path, source)),
        }
    }
}

fn lock_iroh_secret_key(paths: &ClusterPaths, node_id: &NodeId) -> Result<fs::File, NetworkError> {
    lock_iroh_secret_key_with_wait(paths, node_id, IROH_SECRET_KEY_LOCK_WAIT)
}

/// Loads the node's iroh secret key, creating (and persisting) one when
/// absent. A file lock serializes concurrent creators; losers re-read the
/// winner's key.
pub(crate) fn load_or_create_iroh_secret_key(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<iroh::SecretKey, NetworkError> {
    let key_path = iroh_endpoint_key_path(paths, node_id);
    if let Some(key) = read_optional_object_file(&key_path)? {
        return Ok(key);
    }
    let _key_lock = lock_iroh_secret_key(paths, node_id)?;
    if let Some(key) = read_optional_object_file(&key_path)? {
        return Ok(key);
    }
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent).map_err(|source| storage_io(parent, source))?;
    }
    let key = iroh::SecretKey::generate();
    #[cfg(test)]
    record_iroh_secret_key_create_new_attempt_for_test(&key_path);
    if write_object_file_create_new_serialized(&key_path, &key)? {
        Ok(key)
    } else {
        read_object_file(&key_path)
    }
}

/// Builds the persisted endpoint record from the endpoint's bound address.
pub(crate) fn iroh_endpoint_record_from_addr(
    cluster_id: &ClusterId,
    node_id: &NodeId,
    addr: iroh::EndpointAddr,
) -> IrohEndpointRecord {
    let relay_url = addr.relay_urls().next().map(|relay| relay.to_string());
    let direct_addresses = addr.ip_addrs().map(|address| address.to_string()).collect();
    let relay_mode = relay_url
        .as_ref()
        .map(|relay_url| IrohRelayMode::Custom {
            relay_url: relay_url.clone(),
        })
        .unwrap_or(IrohRelayMode::Default);
    IrohEndpointRecord {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        endpoint_public_key: addr.id.to_string(),
        direct_addresses,
        relay_url,
        relay_mode,
        expires_after_tick: 0,
    }
}

fn iroh_daemon_proxy_restart_backoff(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(8);
    IROH_DAEMON_PROXY_RESTART_BACKOFF_MIN
        .saturating_mul(2_u32.saturating_pow(shift))
        .min(IROH_DAEMON_PROXY_RESTART_BACKOFF_MAX)
}

fn iroh_daemon_proxy_restart_delay(consecutive_failures: u32, node_id: &NodeId) -> Duration {
    let base = iroh_daemon_proxy_restart_backoff(consecutive_failures);
    let jitter_window_ms = ((base.as_millis() / 10) as u64)
        .min(IROH_DAEMON_PROXY_RESTART_JITTER_MAX.as_millis() as u64);
    if jitter_window_ms == 0 {
        return base;
    }
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in node_id.as_str().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= u64::from(consecutive_failures);
    hash = hash.wrapping_mul(0x100000001b3);
    base + Duration::from_millis(hash % (jitter_window_ms + 1))
}

fn try_acquire_iroh_proxy_permit(
    limiter: &Arc<tokio::sync::Semaphore>,
    limit: usize,
    scope: &str,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    match limiter.clone().try_acquire_owned() {
        Ok(permit) => Some(permit),
        Err(_) => {
            eprintln!("sinomo iroh daemon proxy dropping {scope}: active limit {limit} reached");
            None
        }
    }
}

async fn accept_iroh_proxy_command_stream_with_idle_timeout<F, T, E>(
    accept: F,
    timeout: Duration,
) -> Result<Option<T>, NetworkError>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match tokio::time::timeout(timeout, accept).await {
        Ok(Ok(streams)) => Ok(Some(streams)),
        Ok(Err(source)) => Err(NetworkError::Io {
            operation: "accept iroh daemon command stream".to_string(),
            detail: source.to_string(),
        }),
        Err(_) => Ok(None),
    }
}

fn iroh_proxy_command_stream_accept_error_is_peer_close(error: &NetworkError) -> bool {
    let NetworkError::Io { operation, detail } = error else {
        return false;
    };
    operation == "accept iroh daemon command stream"
        && (detail.contains("closed by peer")
            || detail == "timed out"
            || detail.contains("stopped"))
}

fn log_iroh_daemon_proxy_task_join(label: &str, result: Result<(), tokio::task::JoinError>) {
    match result {
        Ok(()) => {}
        Err(error) if error.is_cancelled() => {}
        Err(error) => {
            eprintln!("sinomo iroh daemon proxy {label} task stopped: {error}");
        }
    }
}

async fn abort_and_drain_iroh_daemon_proxy_tasks(
    tasks: &mut tokio::task::JoinSet<()>,
    label: &str,
) {
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        log_iroh_daemon_proxy_task_join(label, result);
    }
}

/// Bridges one native QUIC bidirectional stream to the local daemon without
/// looking at object-RPC or transfer frames. Backpressure and half-close are
/// preserved in both directions, so large bodies remain streaming end to end.
async fn bridge_iroh_stream_to_daemon(
    address: &str,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    metadata: &AuthenticatedTransportMetadata,
) -> Result<(), NetworkError> {
    use tokio::io::AsyncWriteExt;

    let connect = tokio::net::TcpStream::connect(address);
    let mut stream = tokio::time::timeout(DEFAULT_NETWORK_TIMEOUT, connect)
        .await
        .map_err(|_| {
            NetworkError::timeout(
                format!("connect daemon control address {address}"),
                sinomo_core::network::NetworkTimeoutPhase::Connect,
                DEFAULT_NETWORK_TIMEOUT,
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            )
        })?
        .map_err(|source| NetworkError::Io {
            operation: format!("connect daemon control address {address}"),
            detail: source.to_string(),
        })?;
    write_authenticated_transport_prelude(&mut stream, metadata).await?;
    let (mut daemon_read, mut daemon_write) = stream.into_split();

    let request = async {
        tokio::io::copy(&mut recv, &mut daemon_write)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "forward iroh stream request stream".to_string(),
                detail: source.to_string(),
            })?;
        daemon_write
            .shutdown()
            .await
            .map_err(|source| NetworkError::Io {
                operation: "finish forwarded iroh stream request stream".to_string(),
                detail: source.to_string(),
            })?;
        Ok::<(), NetworkError>(())
    };
    let response = async {
        tokio::io::copy(&mut daemon_read, &mut send)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "forward daemon response stream".to_string(),
                detail: source.to_string(),
            })?;
        send.finish().map_err(|source| NetworkError::Io {
            operation: "finish forwarded daemon response stream".to_string(),
            detail: source.to_string(),
        })?;
        Ok::<(), NetworkError>(())
    };
    tokio::try_join!(request, response)?;
    Ok(())
}

/// Closes an iroh endpoint on drop even along error paths (mirrors the
/// former roaming.rs close guard).
pub(crate) struct IrohEndpointCloseGuard {
    endpoint: Option<iroh::Endpoint>,
    runtime: tokio::runtime::Handle,
}

impl IrohEndpointCloseGuard {
    pub(crate) fn new(runtime: tokio::runtime::Handle, endpoint: iroh::Endpoint) -> Self {
        Self {
            endpoint: Some(endpoint),
            runtime,
        }
    }

    pub(crate) fn endpoint(&self) -> &iroh::Endpoint {
        self.endpoint
            .as_ref()
            .expect("iroh endpoint close guard missing endpoint")
    }

    pub(crate) async fn close(mut self) {
        if let Some(endpoint) = self.endpoint.take() {
            endpoint.close().await;
        }
    }
}

impl Drop for IrohEndpointCloseGuard {
    fn drop(&mut self) {
        let Some(endpoint) = self.endpoint.take() else {
            return;
        };
        if endpoint.is_closed() {
            return;
        }
        let endpoint_for_close = endpoint.clone();
        let spawn_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.runtime.spawn(async move {
                endpoint_for_close.close().await;
            })
        }));
        if spawn_result.is_err() {
            eprintln!("sinomo iroh endpoint close guard could not schedule close");
            std::mem::forget(endpoint);
        }
    }
}

/// Configuration for one proxy instance (derived from the network-module
/// start config).
#[derive(Clone, Debug)]
pub struct IrohProxyConfig {
    pub paths: ClusterPaths,
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub daemon_forward_address: String,
}

/// Running proxy handle: the advertised record, a shutdown signal, and the
/// supervisor task (which restarts the proxy with backoff on failure).
pub struct IrohDaemonProxy {
    record: IrohEndpointRecord,
    endpoint: tokio::sync::watch::Receiver<Option<iroh::Endpoint>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    supervisor: tokio::task::JoinHandle<()>,
}

impl IrohDaemonProxy {
    pub fn record(&self) -> &IrohEndpointRecord {
        &self.record
    }

    pub fn endpoint_receiver(&self) -> tokio::sync::watch::Receiver<Option<iroh::Endpoint>> {
        self.endpoint.clone()
    }

    /// Signals shutdown and waits (bounded) for the supervisor to stop,
    /// aborting it when it does not stop in time.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let mut supervisor = self.supervisor;
        if tokio::time::timeout(DEFAULT_NETWORK_TIMEOUT, &mut supervisor)
            .await
            .is_err()
        {
            supervisor.abort();
            eprintln!("sinomo iroh daemon proxy supervisor did not stop in time");
        }
    }
}

enum IrohDaemonProxyReady {
    Async(tokio::sync::oneshot::Sender<Result<IrohEndpointRecord, NetworkError>>),
}

impl IrohDaemonProxyReady {
    fn send(self, result: Result<IrohEndpointRecord, NetworkError>) {
        match self {
            Self::Async(sender) => {
                let _ = sender.send(result);
            }
        }
    }
}

fn send_iroh_daemon_proxy_ready(
    ready: &mut Option<IrohDaemonProxyReady>,
    result: Result<IrohEndpointRecord, NetworkError>,
) {
    if let Some(sender) = ready.take() {
        sender.send(result);
    }
}

/// Starts the proxy supervisor on `runtime` and waits for the first
/// successful bind (the persisted endpoint record).
pub async fn start_iroh_daemon_proxy(
    runtime: tokio::runtime::Handle,
    config: IrohProxyConfig,
) -> Result<IrohDaemonProxy, NetworkError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (endpoint_tx, endpoint_rx) = tokio::sync::watch::channel(None);
    let supervisor_runtime = runtime.clone();
    let task = runtime.spawn(run_iroh_daemon_proxy_supervisor(
        supervisor_runtime,
        config,
        Some(IrohDaemonProxyReady::Async(tx)),
        endpoint_tx,
        shutdown_rx,
    ));
    let record = match rx.await {
        Ok(Ok(record)) => record,
        Ok(Err(error)) => {
            let _ = shutdown_tx.send(true);
            task.abort();
            return Err(error);
        }
        Err(source) => {
            let _ = shutdown_tx.send(true);
            task.abort();
            return Err(NetworkError::Io {
                operation: "start iroh daemon proxy".to_string(),
                detail: source.to_string(),
            });
        }
    };
    Ok(IrohDaemonProxy {
        record,
        endpoint: endpoint_rx,
        shutdown: shutdown_tx,
        supervisor: task,
    })
}

fn iroh_daemon_proxy_shutdown_requested(shutdown: &tokio::sync::watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

async fn iroh_daemon_proxy_sleep_or_shutdown(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    delay: Duration,
) -> bool {
    if iroh_daemon_proxy_shutdown_requested(shutdown) {
        return true;
    }
    tokio::select! {
        () = tokio::time::sleep(delay) => false,
        changed = shutdown.changed() => changed.is_err() || iroh_daemon_proxy_shutdown_requested(shutdown),
    }
}

async fn run_iroh_daemon_proxy_supervisor(
    runtime: tokio::runtime::Handle,
    config: IrohProxyConfig,
    mut first_ready: Option<IrohDaemonProxyReady>,
    endpoint: tokio::sync::watch::Sender<Option<iroh::Endpoint>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut restart_failures = 0_u32;
    loop {
        if iroh_daemon_proxy_shutdown_requested(&shutdown) {
            return;
        }
        let key_paths = config.paths.clone();
        let key_node_id = config.node_id.clone();
        let secret_key = match runtime
            .spawn_blocking(move || load_or_create_iroh_secret_key(&key_paths, &key_node_id))
            .await
            .map_err(|source| NetworkError::Io {
                operation: "load iroh secret key".to_string(),
                detail: source.to_string(),
            })
            .and_then(|result| result)
        {
            Ok(secret_key) => secret_key,
            Err(error) => {
                if first_ready.is_some() {
                    send_iroh_daemon_proxy_ready(&mut first_ready, Err(error));
                    return;
                }
                eprintln!("sinomo iroh daemon proxy restart failed: {error}");
                restart_failures = restart_failures.saturating_add(1);
                if iroh_daemon_proxy_sleep_or_shutdown(
                    &mut shutdown,
                    iroh_daemon_proxy_restart_delay(restart_failures, &config.node_id),
                )
                .await
                {
                    return;
                }
                continue;
            }
        };
        let run_started = std::time::Instant::now();
        let result = run_iroh_daemon_proxy(
            runtime.clone(),
            config.clone(),
            secret_key,
            first_ready.take(),
            endpoint.clone(),
            shutdown.clone(),
        )
        .await;
        if iroh_daemon_proxy_shutdown_requested(&shutdown) {
            return;
        }
        if let Err(error) = result {
            eprintln!("sinomo iroh daemon proxy stopped: {error}");
        } else {
            eprintln!("sinomo iroh daemon proxy stopped");
        }
        if run_started.elapsed() >= IROH_DAEMON_PROXY_STABLE_RUNTIME {
            restart_failures = 0;
        } else {
            restart_failures = restart_failures.saturating_add(1);
        }
        if iroh_daemon_proxy_sleep_or_shutdown(
            &mut shutdown,
            iroh_daemon_proxy_restart_delay(restart_failures, &config.node_id),
        )
        .await
        {
            return;
        }
    }
}

async fn run_iroh_daemon_proxy(
    runtime: tokio::runtime::Handle,
    config: IrohProxyConfig,
    secret_key: iroh::SecretKey,
    mut ready: Option<IrohDaemonProxyReady>,
    published_endpoint: tokio::sync::watch::Sender<Option<iroh::Endpoint>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), NetworkError> {
    let endpoint_secret_key = secret_key.clone();
    let builder = match sinomo_iroh_endpoint_builder() {
        Ok(builder) => builder,
        Err(error) => {
            send_iroh_daemon_proxy_ready(&mut ready, Err(error.clone()));
            return Err(error);
        }
    };
    let endpoint = match builder
        .secret_key(secret_key)
        .alpns(vec![IROH_ROAMING_PROTOCOL.as_bytes().to_vec()])
        .bind()
        .await
    {
        Ok(endpoint) => endpoint,
        Err(source) => {
            let error = NetworkError::Io {
                operation: "bind iroh endpoint".to_string(),
                detail: source.to_string(),
            };
            send_iroh_daemon_proxy_ready(&mut ready, Err(error.clone()));
            return Err(error);
        }
    };
    let endpoint = IrohEndpointCloseGuard::new(runtime.clone(), endpoint);
    let _ = tokio::time::timeout(DEFAULT_NETWORK_TIMEOUT, endpoint.endpoint().online()).await;
    let record = iroh_endpoint_record_from_addr(
        &config.cluster_id,
        &config.node_id,
        endpoint.endpoint().addr(),
    );
    // Persist the endpoint record; the on-disk record under
    // `ClusterPaths::iroh` is the pinned cross-ring contract that
    // sinomo-network keeps reading.
    let persist_paths = config.paths.clone();
    let persist_record = record.clone();
    let persist_result = runtime
        .spawn_blocking(move || persist_record.write(&persist_paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "persist iroh endpoint record".to_string(),
            detail: source.to_string(),
        })
        .and_then(|result| result);
    if let Err(error) = persist_result {
        eprintln!("sinomo iroh daemon endpoint record write failed: {error}");
    }
    published_endpoint.send_replace(Some(endpoint.endpoint().clone()));
    struct PublishedEndpointGuard(tokio::sync::watch::Sender<Option<iroh::Endpoint>>);
    impl Drop for PublishedEndpointGuard {
        fn drop(&mut self) {
            self.0.send_replace(None);
        }
    }
    let _published_endpoint_guard = PublishedEndpointGuard(published_endpoint);
    send_iroh_daemon_proxy_ready(&mut ready, Ok(record));

    let connection_limiter = Arc::new(tokio::sync::Semaphore::new(
        IROH_DAEMON_PROXY_MAX_CONNECTIONS,
    ));
    let command_limiter = Arc::new(tokio::sync::Semaphore::new(
        IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS,
    ));
    let daemon_address = config.daemon_forward_address.clone();
    let mut connection_tasks = tokio::task::JoinSet::new();
    loop {
        let connecting = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || iroh_daemon_proxy_shutdown_requested(&shutdown) {
                    endpoint.close().await;
                    abort_and_drain_iroh_daemon_proxy_tasks(&mut connection_tasks, "connection").await;
                    return Ok(());
                }
                continue;
            }
            joined = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(result) = joined {
                    log_iroh_daemon_proxy_task_join("connection", result);
                }
                continue;
            }
            connecting = endpoint.endpoint().accept() => connecting,
        };
        let Some(connecting) = connecting else {
            break;
        };
        let Some(connection_permit) = try_acquire_iroh_proxy_permit(
            &connection_limiter,
            IROH_DAEMON_PROXY_MAX_CONNECTIONS,
            "connection",
        ) else {
            continue;
        };
        let daemon_address = daemon_address.clone();
        let command_limiter = command_limiter.clone();
        let local_secret_key = endpoint_secret_key.clone();
        let mut connection_shutdown = shutdown.clone();
        connection_tasks.spawn(async move {
            let _connection_permit = connection_permit;
            let Ok(connection) = connecting.await else {
                return;
            };
            let peer_transport_identity = connection.remote_id().to_string();
            let peer_transport_key = connection.remote_id();
            let mut channel_binding = [0_u8; 32];
            if let Err(error) = connection.export_keying_material(
                &mut channel_binding,
                IROH_AUTHENTICATED_LINK_EXPORTER_LABEL,
                &[],
            ) {
                tracing::debug!(?error, "iroh daemon proxy could not derive channel binding");
                connection.close(0_u8.into(), b"missing channel binding");
                return;
            }
            let mut command_tasks = tokio::task::JoinSet::new();
            loop {
                let accepted = tokio::select! {
                    biased;
                    changed = connection_shutdown.changed() => {
                        if changed.is_err()
                            || iroh_daemon_proxy_shutdown_requested(&connection_shutdown)
                        {
                            connection.close(0_u8.into(), b"daemon shutdown");
                            break;
                        }
                        continue;
                    }
                    joined = command_tasks.join_next(), if !command_tasks.is_empty() => {
                        if let Some(result) = joined {
                            log_iroh_daemon_proxy_task_join("command", result);
                        }
                        continue;
                    }
                    accepted = accept_iroh_proxy_command_stream_with_idle_timeout(
                        connection.accept_bi(),
                        IROH_DAEMON_PROXY_CONNECTION_IDLE_TIMEOUT,
                    ) => accepted,
                };
                let (send, recv) = match accepted {
                    Ok(Some(streams)) => streams,
                    Ok(None) => {
                        connection.close(0_u8.into(), b"idle timeout");
                        break;
                    }
                    Err(error) => {
                        if iroh_proxy_command_stream_accept_error_is_peer_close(&error) {
                            tracing::debug!(
                                "sinomo iroh daemon proxy command stream closed by peer: {error}"
                            );
                        } else {
                            eprintln!(
                                "sinomo iroh daemon proxy command stream accept failed: {error}"
                            );
                        }
                        break;
                    }
                };
                let Some(command_permit) = try_acquire_iroh_proxy_permit(
                    &command_limiter,
                    IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS,
                    "command stream",
                ) else {
                    drop(send);
                    drop(recv);
                    continue;
                };
                let daemon_address = daemon_address.clone();
                let peer_transport_identity = peer_transport_identity.clone();
                let local_secret_key = local_secret_key.clone();
                command_tasks.spawn(async move {
                    let _command_permit = command_permit;
                    let mut send = send;
                    let mut recv = recv;
                    let transport_proof = match crate::auth::authenticate_inbound_stream(
                        &local_secret_key,
                        &peer_transport_key,
                        &channel_binding,
                        &mut send,
                        &mut recv,
                    )
                    .await
                    {
                        Ok(proof) => proof,
                        Err(error) => {
                            tracing::debug!(%error, "iroh daemon stream authentication failed");
                            return;
                        }
                    };
                    let metadata = match AuthenticatedTransportMetadata::try_new(
                        ClusterTransportKind::Iroh,
                        peer_transport_identity,
                        channel_binding,
                        transport_proof,
                        DirectStreamCapabilities::multiplexed_full_duplex(
                            IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS as u32,
                        ),
                    ) {
                        Ok(metadata) => metadata,
                        Err(error) => {
                            tracing::debug!(%error, "iroh daemon proxy rejected authenticated metadata");
                            return;
                        }
                    };
                    if let Err(error) =
                        bridge_iroh_stream_to_daemon(&daemon_address, send, recv, &metadata)
                            .await
                    {
                        tracing::debug!(%error, "iroh daemon stream bridge stopped");
                    }
                });
            }
            abort_and_drain_iroh_daemon_proxy_tasks(&mut command_tasks, "command").await;
        });
    }
    endpoint.close().await;
    abort_and_drain_iroh_daemon_proxy_tasks(&mut connection_tasks, "connection").await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::local::SinomoDataDir;
    use sinomo_core::network::DaemonRpcTimeouts;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn default_iroh_relay_urls_match_iroh_n0_preset_hosts() {
        for relay_url in sinomo_iroh_relay_url_strings_from_env(None) {
            let host = relay_url
                .strip_prefix("https://")
                .and_then(|rest| rest.split('/').next())
                .expect("https relay host");
            assert!(
                host.ends_with(".relay.n0.iroh-canary.iroh.link."),
                "default relay should follow the Iroh N0 preset relay map: {relay_url}"
            );
        }
    }

    #[test]
    fn sinomo_default_iroh_relays_use_stable_network_hosts() {
        let urls = sinomo_iroh_relay_url_strings_from_env(None);

        assert_eq!(
            urls,
            vec![
                "https://use1-1.relay.n0.iroh-canary.iroh.link./".to_string(),
                "https://usw1-1.relay.n0.iroh-canary.iroh.link./".to_string(),
                "https://euc1-1.relay.n0.iroh-canary.iroh.link./".to_string(),
                "https://aps1-1.relay.n0.iroh-canary.iroh.link./".to_string(),
            ]
        );
        assert!(urls.iter().all(|url| url.contains("iroh-canary")));
    }

    #[test]
    fn sinomo_iroh_relay_env_ignores_empty_entries() {
        let urls = sinomo_iroh_relay_url_strings_from_env(Some(
            " https://relay-a.example./, ,https://relay-b.example./ ".to_string(),
        ));

        assert_eq!(
            urls,
            vec![
                "https://relay-a.example./".to_string(),
                "https://relay-b.example./".to_string(),
            ]
        );
    }

    #[test]
    fn iroh_daemon_proxy_restart_backoff_grows_and_caps() {
        assert_eq!(
            iroh_daemon_proxy_restart_backoff(0),
            IROH_DAEMON_PROXY_RESTART_BACKOFF_MIN
        );
        assert_eq!(
            iroh_daemon_proxy_restart_backoff(1),
            IROH_DAEMON_PROXY_RESTART_BACKOFF_MIN
        );
        assert_eq!(
            iroh_daemon_proxy_restart_backoff(2),
            IROH_DAEMON_PROXY_RESTART_BACKOFF_MIN * 2
        );
        assert_eq!(
            iroh_daemon_proxy_restart_backoff(32),
            IROH_DAEMON_PROXY_RESTART_BACKOFF_MAX
        );
    }

    #[test]
    fn iroh_daemon_proxy_restart_delay_adds_bounded_node_jitter() {
        let node_a = NodeId::from_legacy_named("node_iroh_restart_jitter_a").expect("node a");
        let node_b = NodeId::from_legacy_named("node_iroh_restart_jitter_b").expect("node b");
        let failures = 3;
        let base = iroh_daemon_proxy_restart_backoff(failures);
        let jitter_max = IROH_DAEMON_PROXY_RESTART_JITTER_MAX
            .min(Duration::from_millis((base.as_millis() / 10) as u64));

        let delay_a = iroh_daemon_proxy_restart_delay(failures, &node_a);
        let delay_b = iroh_daemon_proxy_restart_delay(failures, &node_b);

        assert_eq!(
            delay_a,
            iroh_daemon_proxy_restart_delay(failures, &node_a),
            "restart jitter must be deterministic for a given node and failure count"
        );
        assert!(
            delay_a >= base && delay_a <= base + jitter_max,
            "restart delay should stay inside the bounded jitter window"
        );
        assert!(
            delay_b >= base && delay_b <= base + jitter_max,
            "restart delay should stay inside the bounded jitter window"
        );
        assert_ne!(
            delay_a, delay_b,
            "different nodes should not reconnect in lockstep after proxy failures"
        );
    }

    #[test]
    fn iroh_daemon_proxy_admission_limit_releases_permits() {
        assert!(IROH_DAEMON_PROXY_MAX_CONNECTIONS >= 2);
        assert!(IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS >= IROH_DAEMON_PROXY_MAX_CONNECTIONS);
        let estimated_proxy_fd_budget = IROH_DAEMON_PROXY_MAX_CONNECTIONS
            + IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS.saturating_mul(2)
            + 64;
        assert!(
            estimated_proxy_fd_budget <= 512,
            "proxy admission should leave room under a 1024 open-file soft limit"
        );
        let limiter = Arc::new(tokio::sync::Semaphore::new(2));
        let first = try_acquire_iroh_proxy_permit(&limiter, 2, "test").expect("first permit");
        let second = try_acquire_iroh_proxy_permit(&limiter, 2, "test").expect("second permit");

        assert!(
            try_acquire_iroh_proxy_permit(&limiter, 2, "test").is_none(),
            "admission should reject work once all permits are active"
        );

        drop(first);
        let replacement =
            try_acquire_iroh_proxy_permit(&limiter, 2, "test").expect("replacement permit");
        drop(second);
        drop(replacement);

        assert_eq!(limiter.available_permits(), 2);
    }

    #[test]
    fn iroh_daemon_proxy_peer_closed_accept_is_not_error_noise() {
        let closed = NetworkError::Io {
            operation: "accept iroh daemon command stream".to_string(),
            detail: "closed by peer: done (code 0)".to_string(),
        };
        assert!(iroh_proxy_command_stream_accept_error_is_peer_close(
            &closed
        ));

        let timed_out = NetworkError::Io {
            operation: "accept iroh daemon command stream".to_string(),
            detail: "timed out".to_string(),
        };
        assert!(iroh_proxy_command_stream_accept_error_is_peer_close(
            &timed_out
        ));

        let unrelated = NetworkError::Io {
            operation: "accept iroh daemon command stream".to_string(),
            detail: "permission denied".to_string(),
        };
        assert!(!iroh_proxy_command_stream_accept_error_is_peer_close(
            &unrelated
        ));
    }

    #[tokio::test]
    async fn iroh_daemon_proxy_command_accept_times_out_idle_connections() {
        let started = Instant::now();
        let accepted = accept_iroh_proxy_command_stream_with_idle_timeout(
            std::future::pending::<Result<(), std::io::Error>>(),
            Duration::from_millis(5),
        )
        .await
        .expect("idle accept timeout");

        assert_eq!(accepted, None);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "idle proxy command accept should be bounded"
        );
    }

    #[tokio::test]
    async fn iroh_daemon_proxy_command_accept_returns_stream_before_idle_timeout() {
        let accepted = accept_iroh_proxy_command_stream_with_idle_timeout(
            async { Ok::<_, std::io::Error>(42_u8) },
            Duration::from_secs(1),
        )
        .await
        .expect("accepted command stream");

        assert_eq!(accepted, Some(42));
    }

    #[test]
    fn iroh_secret_key_creation_is_atomic_under_concurrency() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-iroh-secret-concurrent");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        let node_id = NodeId::from_legacy_named("node_iroh_secret_concurrent").expect("node");
        let key_path = iroh_endpoint_key_path(&paths, &node_id);
        reset_iroh_secret_key_create_new_attempts_for_test(&key_path);
        let threads = 8;
        let barrier = Arc::new(Barrier::new(threads));
        let handles = (0..threads)
            .map(|_| {
                let paths = paths.clone();
                let node_id = node_id.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    load_or_create_iroh_secret_key(&paths, &node_id)
                        .expect("load or create iroh secret")
                        .public()
                        .to_string()
                })
            })
            .collect::<Vec<_>>();

        let mut public_keys = handles
            .into_iter()
            .map(|handle| handle.join().expect("iroh secret worker"))
            .collect::<Vec<_>>();
        public_keys.sort();
        public_keys.dedup();
        assert_eq!(
            public_keys.len(),
            1,
            "concurrent Iroh secret initialization must converge on one persisted endpoint identity"
        );
        let persisted_public_key = load_or_create_iroh_secret_key(&paths, &node_id)
            .expect("load persisted iroh secret")
            .public()
            .to_string();
        assert_eq!(persisted_public_key, public_keys[0]);
        assert_eq!(
            iroh_secret_key_create_new_attempts_for_test(&key_path),
            1,
            "concurrent Iroh secret initialization should serialize creation so only one temporary key is written"
        );
        let key_lock_path = iroh_endpoint_key_lock_path(&paths, &node_id);
        std::fs::remove_file(&key_lock_path).expect("remove creation lock");
        let steady_state_public_key = load_or_create_iroh_secret_key(&paths, &node_id)
            .expect("load existing iroh secret")
            .public()
            .to_string();
        assert_eq!(steady_state_public_key, public_keys[0]);
        assert!(
            !key_lock_path.exists(),
            "steady-state Iroh secret loads should read the existing key without creating a lock file"
        );
    }

    #[test]
    fn iroh_secret_key_lock_wait_is_bounded() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-iroh-secret-lock-timeout");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        let node_id = NodeId::from_legacy_named("node_iroh_secret_lock_timeout").expect("node");
        std::fs::create_dir_all(iroh_endpoint_dir(&paths)).expect("iroh dir");
        let key_lock_path = iroh_endpoint_key_lock_path(&paths, &node_id);
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let holder = thread::spawn({
            let key_lock_path = key_lock_path.clone();
            move || {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&key_lock_path)
                    .expect("open lock");
                file.lock_exclusive().expect("hold lock");
                held_tx.send(()).expect("notify lock held");
                thread::sleep(Duration::from_millis(100));
            }
        });
        held_rx.recv().expect("wait for lock holder");

        let started = Instant::now();
        let error = lock_iroh_secret_key_with_wait(&paths, &node_id, Duration::from_millis(20))
            .expect_err("busy Iroh secret lock should time out");
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "Iroh secret lock timeout should be bounded, elapsed {:?}",
            started.elapsed()
        );
        assert!(
            matches!(error, NetworkError::StorageIo { ref detail, .. } if detail.contains("timed out waiting for Iroh secret key lock")),
            "unexpected lock error: {error}"
        );

        holder.join().expect("join lock holder");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iroh_daemon_proxy_preserves_full_duplex_bytes_after_authenticated_prelude() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind daemon fixture");
        let daemon_address = listener
            .local_addr()
            .expect("daemon fixture address")
            .to_string();
        let fixture = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept daemon fixture");
            let metadata = sinomo_core::network::read_authenticated_transport_prelude(&mut stream)
                .await
                .expect("authenticated transport prelude");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .await
                .expect("read daemon fixture request");
            assert_eq!(request, b"raw request bytes");
            stream
                .write_all(b"raw response bytes")
                .await
                .expect("write daemon fixture response");
            metadata
        });

        let root = sinomo_testdir::TestDir::new("sinomo-network-iroh-proxy");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let proxy = start_iroh_daemon_proxy(
            tokio::runtime::Handle::current(),
            IrohProxyConfig {
                paths,
                cluster_id: ClusterId::from_legacy_named("cluster_iroh_proxy").expect("cluster"),
                node_id: NodeId::from_legacy_named("node_iroh_proxy").expect("node"),
                daemon_forward_address: daemon_address,
            },
        )
        .await
        .expect("start iroh proxy");

        let dialer = crate::dial::IrohDaemonDialer::new();
        let opened = dialer
            .open_daemon_stream_async(
                &proxy.record().endpoint().address,
                DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5)),
            )
            .await
            .expect("open authenticated iroh daemon stream");
        let channel_binding = opened.channel_binding;
        let mut send = opened.send;
        let mut recv = opened.recv;
        tokio::io::AsyncWriteExt::write_all(&mut send, b"raw request bytes")
            .await
            .expect("write raw request");
        send.finish().expect("finish raw request");
        let mut response = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut response)
            .await
            .expect("read raw response");

        assert_eq!(response, b"raw response bytes");
        dialer.shutdown().await;
        proxy.shutdown().await;
        let metadata = fixture.await.expect("daemon fixture");
        assert_eq!(metadata.transport, ClusterTransportKind::Iroh);
        assert_eq!(metadata.channel_binding, channel_binding);
        assert!(!metadata.peer_transport_identity.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_advertised_endpoints_prove_stable_peer_identities_and_same_exporter() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let source_forward = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused source forward fixture");
        let source_forward_address = source_forward
            .local_addr()
            .expect("source forward address")
            .to_string();
        let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target daemon fixture");
        let target_address = target_listener
            .local_addr()
            .expect("target daemon fixture address")
            .to_string();
        let target_fixture = tokio::spawn(async move {
            let (mut stream, _) = target_listener.accept().await.expect("accept target");
            let metadata = sinomo_core::network::read_authenticated_transport_prelude(&mut stream)
                .await
                .expect("target authenticated prelude");
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.expect("read target");
            assert_eq!(request, b"stable-identity-proof");
            stream.write_all(b"accepted").await.expect("write target");
            metadata
        });

        let source_root =
            sinomo_testdir::TestDir::new("sinomo-network-iroh-shared-source-endpoint");
        let source_paths = ClusterPaths::new(&SinomoDataDir::new(&source_root));
        source_paths.ensure().expect("source cluster paths");
        let target_root =
            sinomo_testdir::TestDir::new("sinomo-network-iroh-shared-target-endpoint");
        let target_paths = ClusterPaths::new(&SinomoDataDir::new(&target_root));
        target_paths.ensure().expect("target cluster paths");
        let cluster_id =
            ClusterId::from_legacy_named("cluster_iroh_shared_identity").expect("cluster");
        let source = start_iroh_daemon_proxy(
            tokio::runtime::Handle::current(),
            IrohProxyConfig {
                paths: source_paths,
                cluster_id: cluster_id.clone(),
                node_id: NodeId::from_legacy_named("node_iroh_shared_identity_source")
                    .expect("source node"),
                daemon_forward_address: source_forward_address,
            },
        )
        .await
        .expect("start source proxy");
        let target = start_iroh_daemon_proxy(
            tokio::runtime::Handle::current(),
            IrohProxyConfig {
                paths: target_paths,
                cluster_id,
                node_id: NodeId::from_legacy_named("node_iroh_shared_identity_target")
                    .expect("target node"),
                daemon_forward_address: target_address,
            },
        )
        .await
        .expect("start target proxy");
        let source_transport_identity = source.record().endpoint_public_key.clone();
        let target_transport_identity = target.record().endpoint_public_key.clone();

        let dialer = crate::dial::IrohDaemonDialer::new();
        dialer.use_shared_endpoint(source.endpoint_receiver()).await;
        let opened = dialer
            .open_daemon_stream_async(
                &target.record().endpoint().address,
                DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5)),
            )
            .await
            .expect("connect source advertised endpoint to target");
        assert_eq!(opened.peer_transport_identity, target_transport_identity);
        let channel_binding = opened.channel_binding;
        let outbound_metadata = AuthenticatedTransportMetadata::try_new(
            ClusterTransportKind::Iroh,
            opened.peer_transport_identity.clone(),
            channel_binding,
            opened.transport_proof.clone(),
            DirectStreamCapabilities::multiplexed_full_duplex(1),
        )
        .expect("outbound authenticated metadata");
        assert!(crate::auth::verify_transport_metadata(
            &source_transport_identity,
            &outbound_metadata
        ));
        let mut send = opened.send;
        let mut recv = opened.recv;
        send.write_all(b"stable-identity-proof")
            .await
            .expect("write proof");
        send.finish().expect("finish proof");
        let mut response = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut response)
            .await
            .expect("read response");
        assert_eq!(response, b"accepted");

        let inbound = target_fixture.await.expect("target fixture");
        assert_eq!(inbound.peer_transport_identity, source_transport_identity);
        assert_eq!(inbound.channel_binding, channel_binding);
        assert!(crate::auth::verify_transport_metadata(
            &target_transport_identity,
            &inbound
        ));
        dialer.shutdown().await;
        source.shutdown().await;
        target.shutdown().await;
        drop(source_forward);
    }
}
