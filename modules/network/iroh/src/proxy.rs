//! Iroh daemon proxy: endpoint builder + relay-mode config, secret-key
//! load/create, and the accept/forward loop that relays inbound command
//! frames to the local daemon control address over TCP.
//!
//! Moved out of `crates/sinomo-network/src/roaming.rs` in stage C of
//! docs/plans/r0-r1-crate-boundary-refactor.md. Behavior is unchanged: the
//! proxy accepts bidirectional iroh streams, reads one command frame per
//! stream, forwards it verbatim to the daemon control address, and writes the
//! daemon's response frame back.

use crate::frames::{
    IROH_DAEMON_PROXY_COMMAND_BUDGET_BYTES, IrohProxyByteBudget, IrohProxyByteBudgetPermit,
    read_proxy_frame_async, write_all_frame_async,
};
use fs2::FileExt;
use sinomo_core::network::{
    ClusterPaths, DaemonControlRequest, DaemonRpcTimeouts, IROH_DAEMON_COMMAND_MAX_BYTES,
    IrohEndpointRecord, IrohRelayMode, NetworkError, ObjectRpcResponse,
    decode_single_object_frame_from_slice, encode_object_frame_to_vec, read_object_file,
    read_optional_object_file, write_object_file_create_new_serialized,
};
use sinomo_core::node::{ClusterId, NodeId};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub const IROH_ROAMING_PROTOCOL: &str = "sinomo.iroh-roaming.v1";
const IROH_DAEMON_ONLINE_TIMEOUT: Duration = Duration::from_secs(5);
const SINOMO_IROH_RELAY_URLS_ENV: &str = "SINOMO_IROH_RELAY_URLS";
const IROH_DAEMON_PROXY_RESTART_BACKOFF_MIN: Duration = Duration::from_secs(5);
const IROH_DAEMON_PROXY_RESTART_BACKOFF_MAX: Duration = Duration::from_secs(300);
const IROH_DAEMON_PROXY_RESTART_JITTER_MAX: Duration = Duration::from_secs(30);
const IROH_DAEMON_PROXY_STABLE_RUNTIME: Duration = Duration::from_secs(60);
const IROH_SECRET_KEY_LOCK_WAIT: Duration = Duration::from_secs(2);
const IROH_SECRET_KEY_LOCK_RETRY_INITIAL: Duration = Duration::from_millis(2);
const IROH_SECRET_KEY_LOCK_RETRY_MAX: Duration = Duration::from_millis(40);
pub const IROH_DAEMON_PROXY_ERROR_ROUTE: &str = "/iroh-daemon-proxy/error";
const IROH_DAEMON_PROXY_MAX_CONNECTIONS: usize = 64;
const IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS: usize = 128;
const IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT: Duration = Duration::from_secs(30);
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
    paths.iroh.join(format!("{}.secret.sobj", node_id.as_str()))
}

fn iroh_endpoint_key_lock_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    paths.iroh.join(format!("{}.secret.lock", node_id.as_str()))
}

fn storage_io(path: &std::path::Path, detail: impl std::fmt::Display) -> NetworkError {
    NetworkError::StorageIo {
        path: path.display().to_string(),
        detail: detail.to_string(),
    }
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

fn lock_iroh_secret_key(paths: &ClusterPaths, node_id: &NodeId) -> Result<fs::File, NetworkError> {
    let wait = IROH_SECRET_KEY_LOCK_WAIT;
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

/// Best-effort route hint for error responses when the inbound frame cannot
/// be forwarded (decodes the command frame's route when possible).
fn iroh_proxy_route_hint(payload: &[u8]) -> String {
    let Ok(request) = decode_single_object_frame_from_slice::<DaemonControlRequest>(
        payload,
        IROH_DAEMON_COMMAND_MAX_BYTES,
    ) else {
        return IROH_DAEMON_PROXY_ERROR_ROUTE.to_string();
    };
    request.route
}

fn encode_iroh_proxy_error_response_frame(
    budget: &IrohProxyByteBudget,
    route: impl Into<String>,
    detail: impl Into<String>,
) -> Result<(Vec<u8>, IrohProxyByteBudgetPermit), NetworkError> {
    let response = ObjectRpcResponse::encode_error(route, detail)?;
    let frame = encode_object_frame_to_vec(&response)?;
    let permit = budget.try_acquire(frame.len())?;
    Ok((frame, permit))
}

async fn write_iroh_proxy_error_response<W>(
    writer: &mut W,
    budget: &IrohProxyByteBudget,
    route: impl Into<String>,
    detail: impl Into<String>,
) -> Result<(), NetworkError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let (frame, _permit) = encode_iroh_proxy_error_response_frame(budget, route, detail)?;
    write_all_frame_async(
        writer,
        &frame,
        "write iroh daemon proxy frame",
        Some(IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT),
    )
    .await
}

/// Forwards one command frame to the daemon control address over TCP and
/// reads the daemon's response frame.
async fn forward_daemon_control_payload_async(
    address: &str,
    payload: &[u8],
    timeouts: DaemonRpcTimeouts,
    budget: &IrohProxyByteBudget,
) -> Result<(Vec<u8>, IrohProxyByteBudgetPermit), NetworkError> {
    use tokio::io::AsyncWriteExt;

    let connect = tokio::net::TcpStream::connect(address);
    let mut stream = if let Some(connect_timeout) = timeouts.connect_timeout {
        tokio::time::timeout(connect_timeout, connect)
            .await
            .map_err(|_| NetworkError::Io {
                operation: format!("connect daemon control address {address}"),
                detail: format!("timed out after {}ms", connect_timeout.as_millis()),
            })?
    } else {
        connect.await
    }
    .map_err(|source| NetworkError::Io {
        operation: format!("connect daemon control address {address}"),
        detail: source.to_string(),
    })?;
    write_all_frame_async(
        &mut stream,
        payload,
        &format!("write daemon control address {address}"),
        timeouts.idle_timeout,
    )
    .await?;
    let shutdown = stream.shutdown();
    if let Some(idle_timeout) = timeouts.idle_timeout {
        tokio::time::timeout(idle_timeout, shutdown)
            .await
            .map_err(|_| NetworkError::Io {
                operation: format!("finish daemon control address {address}"),
                detail: format!("timed out after {}ms", idle_timeout.as_millis()),
            })?
    } else {
        shutdown.await
    }
    .map_err(|source| NetworkError::Io {
        operation: format!("finish daemon control address {address}"),
        detail: source.to_string(),
    })?;
    read_proxy_frame_async(
        &mut stream,
        budget,
        "command response",
        timeouts.idle_timeout,
    )
    .await
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
    shutdown: tokio::sync::watch::Sender<bool>,
    supervisor: tokio::task::JoinHandle<()>,
}

impl IrohDaemonProxy {
    pub fn record(&self) -> &IrohEndpointRecord {
        &self.record
    }

    /// Signals shutdown and waits (bounded) for the supervisor to stop,
    /// aborting it when it does not stop in time.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let mut supervisor = self.supervisor;
        if tokio::time::timeout(Duration::from_secs(10), &mut supervisor)
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
    let supervisor_runtime = runtime.clone();
    let task = runtime.spawn(run_iroh_daemon_proxy_supervisor(
        supervisor_runtime,
        config,
        Some(IrohDaemonProxyReady::Async(tx)),
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
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), NetworkError> {
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
    let _ = tokio::time::timeout(IROH_DAEMON_ONLINE_TIMEOUT, endpoint.endpoint().online()).await;
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
    send_iroh_daemon_proxy_ready(&mut ready, Ok(record));

    let connection_limiter = Arc::new(tokio::sync::Semaphore::new(
        IROH_DAEMON_PROXY_MAX_CONNECTIONS,
    ));
    let command_limiter = Arc::new(tokio::sync::Semaphore::new(
        IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS,
    ));
    let command_byte_budget = IrohProxyByteBudget::new(IROH_DAEMON_PROXY_COMMAND_BUDGET_BYTES);
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
        let command_byte_budget = command_byte_budget.clone();
        let mut connection_shutdown = shutdown.clone();
        connection_tasks.spawn(async move {
            let _connection_permit = connection_permit;
            let Ok(connection) = connecting.await else {
                return;
            };
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
                let (mut send, mut recv) = match accepted {
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
                    let detail = format!(
                        "iroh daemon proxy command stream rejected: active limit {} reached",
                        IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS
                    );
                    if write_iroh_proxy_error_response(
                        &mut send,
                        &command_byte_budget,
                        IROH_DAEMON_PROXY_ERROR_ROUTE,
                        detail,
                    )
                    .await
                    .is_ok()
                    {
                        let _ = send.finish();
                    }
                    continue;
                };
                let daemon_address = daemon_address.clone();
                let command_byte_budget = command_byte_budget.clone();
                command_tasks.spawn(async move {
                    let _command_permit = command_permit;
                    let (request, request_budget_permit) = match read_proxy_frame_async(
                        &mut recv,
                        &command_byte_budget,
                        "command request",
                        Some(IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            let detail =
                                format!("iroh daemon proxy command request rejected: {error}");
                            eprintln!("sinomo {detail}");
                            if write_iroh_proxy_error_response(
                                &mut send,
                                &command_byte_budget,
                                IROH_DAEMON_PROXY_ERROR_ROUTE,
                                detail,
                            )
                            .await
                            .is_ok()
                            {
                                let _ = send.finish();
                            }
                            return;
                        }
                    };
                    let route_hint = iroh_proxy_route_hint(&request);
                    let forward_budget = command_byte_budget.clone();
                    let response_result = forward_daemon_control_payload_async(
                        &daemon_address,
                        &request,
                        DaemonRpcTimeouts::connect_only(IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT),
                        &forward_budget,
                    )
                    .await;
                    let (response, response_budget_permit) = match response_result {
                        Ok(result) => result,
                        Err(error) => {
                            let detail = format!("iroh daemon proxy command failed: {error}");
                            eprintln!("sinomo {detail}");
                            drop(request_budget_permit);
                            if write_iroh_proxy_error_response(
                                &mut send,
                                &command_byte_budget,
                                route_hint,
                                detail,
                            )
                            .await
                            .is_ok()
                            {
                                let _ = send.finish();
                            }
                            return;
                        }
                    };
                    drop(request_budget_permit);
                    if write_all_frame_async(
                        &mut send,
                        &response,
                        "write iroh daemon proxy frame",
                        Some(IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT),
                    )
                    .await
                    .is_ok()
                    {
                        let _ = send.finish();
                    }
                    drop(response_budget_permit);
                });
            }
            abort_and_drain_iroh_daemon_proxy_tasks(&mut command_tasks, "command").await;
        });
    }
    endpoint.close().await;
    abort_and_drain_iroh_daemon_proxy_tasks(&mut connection_tasks, "connection").await;
    Ok(())
}
