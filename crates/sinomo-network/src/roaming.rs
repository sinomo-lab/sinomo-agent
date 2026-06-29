use crate::ClusterPaths;
use crate::error::NetworkError;
#[cfg(any(feature = "iroh-transport", test))]
use crate::io_observer;
use crate::object::DaemonRpcTimeouts;
use crate::object_file_store::{read_object_file, write_object_file};
#[cfg(feature = "iroh-transport")]
use crate::object_file_store::{
    read_optional_object_file, write_object_file_create_new_serialized,
};
#[cfg(test)]
use crate::peer::{EndpointCandidateStatus, PeerEndpointCandidate};
use crate::prelude::*;
#[cfg(feature = "iroh-transport")]
use fs2::FileExt;
#[cfg(test)]
use sinomo_kv::KvEntry;
#[cfg(feature = "iroh-transport")]
use std::fs;

#[cfg(feature = "iroh-transport")]
const IROH_ROAMING_PROTOCOL: &str = "sinomo.iroh-roaming.v1";
pub const IROH_DAEMON_COMMAND_MAX_BYTES: usize = crate::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES;
#[cfg(feature = "iroh-transport")]
const IROH_DAEMON_ONLINE_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(any(feature = "iroh-transport", test))]
const SINOMO_IROH_RELAY_URLS_ENV: &str = "SINOMO_IROH_RELAY_URLS";
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_RESTART_BACKOFF_MIN: Duration = Duration::from_secs(5);
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_RESTART_BACKOFF_MAX: Duration = Duration::from_secs(300);
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_RESTART_JITTER_MAX: Duration = Duration::from_secs(30);
#[cfg(feature = "iroh-transport")]
const IROH_DAEMON_PROXY_STABLE_RUNTIME: Duration = Duration::from_secs(60);
#[cfg(any(feature = "iroh-transport", test))]
const IROH_DAEMON_RPC_MAX_ATTEMPTS: usize = 3;
#[cfg(any(feature = "iroh-transport", test))]
const IROH_DAEMON_RPC_RETRY_INITIAL: Duration = Duration::from_millis(40);
#[cfg(any(feature = "iroh-transport", test))]
const IROH_DAEMON_RPC_RETRY_MAX: Duration = Duration::from_millis(250);
#[cfg(feature = "iroh-transport")]
const IROH_SECRET_KEY_LOCK_WAIT: Duration = Duration::from_secs(2);
#[cfg(feature = "iroh-transport")]
const IROH_SECRET_KEY_LOCK_RETRY_INITIAL: Duration = Duration::from_millis(2);
#[cfg(feature = "iroh-transport")]
const IROH_SECRET_KEY_LOCK_RETRY_MAX: Duration = Duration::from_millis(40);
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_ERROR_ROUTE: &str = "/iroh-daemon-proxy/error";
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_MAX_CONNECTIONS: usize = 128;
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS: usize = 512;
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_COMMAND_BUDGET_BYTES: usize =
    IROH_DAEMON_COMMAND_MAX_BYTES.saturating_add(12);
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
#[cfg(any(feature = "iroh-transport", test))]
const SINOMO_DEFAULT_IROH_RELAY_URLS: &[&str] = &[
    "https://use1-1.relay.n0.iroh-canary.iroh.link./",
    "https://usw1-1.relay.n0.iroh-canary.iroh.link./",
    "https://euc1-1.relay.n0.iroh-canary.iroh.link./",
    "https://aps1-1.relay.n0.iroh-canary.iroh.link./",
];

#[cfg(test)]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct WebRtcSignalingEnvelope {
    pub cluster_id: ClusterId,
    pub exchange_id: String,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub sequence: u64,
    pub expires_after_tick: u64,
    pub message: WebRtcSignalMessage,
}

#[cfg(test)]
impl WebRtcSignalingEnvelope {
    pub fn offer(
        cluster_id: ClusterId,
        exchange_id: impl Into<String>,
        source_node_id: NodeId,
        target_node_id: NodeId,
        sdp: impl Into<String>,
        expires_after_tick: u64,
    ) -> Self {
        Self {
            cluster_id,
            exchange_id: exchange_id.into(),
            source_node_id,
            target_node_id,
            sequence: 1,
            expires_after_tick,
            message: WebRtcSignalMessage::Offer { sdp: sdp.into() },
        }
    }

    pub fn answer(
        cluster_id: ClusterId,
        exchange_id: impl Into<String>,
        source_node_id: NodeId,
        target_node_id: NodeId,
        sdp: impl Into<String>,
        expires_after_tick: u64,
    ) -> Self {
        Self {
            cluster_id,
            exchange_id: exchange_id.into(),
            source_node_id,
            target_node_id,
            sequence: 2,
            expires_after_tick,
            message: WebRtcSignalMessage::Answer { sdp: sdp.into() },
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum WebRtcSignalMessage {
    Offer { sdp: String },
    Answer { sdp: String },
    IceCandidate(IceCandidateRecord),
    EndOfCandidates,
}

#[cfg(test)]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct IceCandidateRecord {
    pub cluster_id: ClusterId,
    pub exchange_id: String,
    pub node_id: NodeId,
    pub candidate_id: String,
    pub candidate_type: IceCandidateType,
    pub address: String,
    pub priority: u32,
    pub raw_candidate: String,
    pub expires_after_tick: u64,
}

#[cfg(test)]
impl IceCandidateRecord {
    pub fn endpoint(&self) -> ClusterEndpointRecord {
        let (transport, privacy, source_priority) = match self.candidate_type {
            IceCandidateType::Host => (
                ClusterTransportKind::WebRtc,
                ClusterEndpointPrivacy::Lan,
                30,
            ),
            IceCandidateType::ServerReflexive => (
                ClusterTransportKind::WebRtc,
                ClusterEndpointPrivacy::InternetDirect,
                60,
            ),
            IceCandidateType::PeerReflexive => (
                ClusterTransportKind::WebRtc,
                ClusterEndpointPrivacy::InternetDirect,
                70,
            ),
            IceCandidateType::Relay => (
                ClusterTransportKind::TurnRelay,
                ClusterEndpointPrivacy::Relayed,
                120,
            ),
        };
        ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::generate(),
            node_id: self.node_id.clone(),
            transport,
            address: self.address.clone(),
            source: ClusterEndpointSource::Ice,
            privacy,
            priority: source_priority,
            last_observed_latency_ms: None,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum IceCandidateType {
    Host,
    ServerReflexive,
    PeerReflexive,
    Relay,
}

#[cfg(test)]
pub(crate) struct WebRtcKvExchange;

#[cfg(test)]
impl WebRtcKvExchange {
    pub fn signal_key(cluster_id: &ClusterId, exchange_id: &str, sequence: u64) -> String {
        format!("/webrtc/signals/{cluster_id}/{exchange_id}/{sequence:020}")
    }

    pub fn candidate_key(
        cluster_id: &ClusterId,
        exchange_id: &str,
        node_id: &NodeId,
        candidate_id: &str,
    ) -> String {
        format!("/webrtc/candidates/{cluster_id}/{exchange_id}/{node_id}/{candidate_id}")
    }

    pub fn signal_prefix(cluster_id: &ClusterId, exchange_id: &str) -> String {
        format!("/webrtc/signals/{cluster_id}/{exchange_id}/")
    }

    pub fn candidate_prefix(cluster_id: &ClusterId, exchange_id: &str) -> String {
        format!("/webrtc/candidates/{cluster_id}/{exchange_id}/")
    }

    pub fn put_signal(
        envelope: WebRtcSignalingEnvelope,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOp, NetworkError> {
        let key = Self::signal_key(
            &envelope.cluster_id,
            &envelope.exchange_id,
            envelope.sequence,
        );
        KvOp::put_serde(key, &envelope, lease_id).map_err(Into::into)
    }

    pub fn put_candidate(
        candidate: IceCandidateRecord,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOp, NetworkError> {
        let key = Self::candidate_key(
            &candidate.cluster_id,
            &candidate.exchange_id,
            &candidate.node_id,
            &candidate.candidate_id,
        );
        KvOp::put_serde(key, &candidate, lease_id).map_err(Into::into)
    }

    pub fn signal_from_entry(entry: &KvEntry) -> Result<WebRtcSignalingEnvelope, NetworkError> {
        entry.decode().map_err(Into::into)
    }

    pub fn candidate_from_entry(entry: &KvEntry) -> Result<IceCandidateRecord, NetworkError> {
        entry.decode().map_err(Into::into)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IrohEndpointRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub endpoint_public_key: String,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "crate::fallible_serde::deserialize_vec"
    )]
    pub direct_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
    pub relay_mode: IrohRelayMode,
    pub expires_after_tick: u64,
}

impl IrohEndpointRecord {
    pub fn read(paths: &ClusterPaths, node_id: &NodeId) -> Result<Self, NetworkError> {
        read_iroh_endpoint_record(paths, node_id)
    }

    pub fn from_relay_url(
        cluster_id: ClusterId,
        node_id: NodeId,
        endpoint_public_key: impl Into<String>,
        relay_url: impl Into<String>,
    ) -> Self {
        let relay_url = relay_url.into();
        Self {
            cluster_id,
            node_id,
            endpoint_public_key: endpoint_public_key.into(),
            direct_addresses: Vec::new(),
            relay_url: Some(relay_url.clone()),
            relay_mode: IrohRelayMode::Custom { relay_url },
            expires_after_tick: 0,
        }
    }

    pub fn endpoint(&self) -> ClusterEndpointRecord {
        let has_relay = self.relay_url.is_some();
        let address = if has_relay {
            let mut query = Vec::new();
            if let Some(relay_url) = &self.relay_url {
                query.push(format!("relay={relay_url}"));
            }
            format!("iroh://{}?{}", self.endpoint_public_key, query.join("&"))
        } else {
            format!("iroh://{}", self.endpoint_public_key)
        };

        ClusterEndpointRecord {
            endpoint_id: stable_iroh_endpoint_id(&self.node_id, &self.endpoint_public_key),
            node_id: self.node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address,
            source: ClusterEndpointSource::Rendezvous,
            privacy: if has_relay {
                ClusterEndpointPrivacy::Relayed
            } else {
                ClusterEndpointPrivacy::InternetDirect
            },
            priority: 90,
            last_observed_latency_ms: None,
        }
    }

    pub fn write(&self, paths: &ClusterPaths) -> Result<(), NetworkError> {
        write_iroh_endpoint_record(paths, self)
    }
}

pub struct IrohDaemonProxyHandle {
    record: IrohEndpointRecord,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl IrohDaemonProxyHandle {
    pub fn endpoint_record(&self) -> &IrohEndpointRecord {
        &self.record
    }

    pub fn endpoint(&self) -> ClusterEndpointRecord {
        self.record.endpoint()
    }

    pub fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    pub fn abort(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }

    pub async fn join(mut self) -> Result<(), NetworkError> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.await.map_err(|source| NetworkError::Io {
            operation: "iroh daemon proxy supervisor".to_string(),
            detail: source.to_string(),
        })
    }
}

impl Drop for IrohDaemonProxyHandle {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

pub(crate) fn stable_iroh_endpoint_id(
    node_id: &NodeId,
    endpoint_public_key: &str,
) -> ClusterEndpointId {
    ClusterEndpointId::derived(
        "sinomo.iroh.endpoint",
        format!("{}\0{endpoint_public_key}", node_id.as_str()),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum IrohRelayMode {
    Disabled,
    Default,
    Custom { relay_url: String },
}

#[cfg(test)]
pub(crate) fn default_sinomo_iroh_relay_urls() -> Vec<String> {
    sinomo_iroh_relay_url_strings_from_env(std::env::var(SINOMO_IROH_RELAY_URLS_ENV).ok())
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn sinomo_iroh_relay_url_strings_from_env(value: Option<String>) -> Vec<String> {
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

#[cfg(feature = "iroh-transport")]
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

#[cfg(feature = "iroh-transport")]
fn sinomo_iroh_endpoint_builder() -> Result<iroh::endpoint::Builder, NetworkError> {
    Ok(iroh::Endpoint::builder(iroh::endpoint::presets::N0).relay_mode(sinomo_iroh_relay_mode()?))
}

#[cfg(test)]
pub(crate) struct IrohKvExchange;

#[cfg(test)]
impl IrohKvExchange {
    pub fn endpoint_key(cluster_id: &ClusterId, node_id: &NodeId) -> String {
        format!("/iroh/endpoints/{cluster_id}/{node_id}")
    }

    pub fn endpoint_prefix(cluster_id: &ClusterId) -> String {
        format!("/iroh/endpoints/{cluster_id}/")
    }

    pub fn put_endpoint(
        endpoint: IrohEndpointRecord,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOp, NetworkError> {
        let key = Self::endpoint_key(&endpoint.cluster_id, &endpoint.node_id);
        KvOp::put_serde(key, &endpoint, lease_id).map_err(Into::into)
    }

    pub fn endpoint_from_entry(entry: &KvEntry) -> Result<IrohEndpointRecord, NetworkError> {
        entry.decode().map_err(Into::into)
    }
}

#[cfg(feature = "iroh-transport")]
pub(crate) fn iroh_endpoint_key_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    paths.iroh.join(format!("{}.secret.sobj", node_id.as_str()))
}

#[cfg(feature = "iroh-transport")]
fn iroh_endpoint_key_lock_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    paths.iroh.join(format!("{}.secret.lock", node_id.as_str()))
}

fn iroh_endpoint_record_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    paths
        .iroh
        .join(format!("{}.endpoint.sobj", node_id.as_str()))
}

#[cfg(feature = "iroh-transport")]
pub(crate) fn lock_iroh_secret_key_with_wait(
    paths: &ClusterPaths,
    node_id: &NodeId,
    wait: Duration,
) -> Result<fs::File, NetworkError> {
    let lock_path = iroh_endpoint_key_lock_path(paths, node_id);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| NetworkError::StorageIo {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })?;
    let deadline = std::time::Instant::now() + wait;
    let mut attempts = 0_u32;
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(source) if file_lock_is_busy(&source) => {
                let now = std::time::Instant::now();
                if wait.is_zero() || now >= deadline {
                    return Err(NetworkError::StorageIo {
                        path: lock_path.display().to_string(),
                        detail: format!(
                            "timed out waiting for Iroh secret key lock after {:?}",
                            wait
                        ),
                    });
                }
                let delay = iroh_secret_key_lock_retry_delay(attempts)
                    .min(deadline.saturating_duration_since(now));
                thread::sleep(delay);
                attempts = attempts.saturating_add(1);
            }
            Err(source) => {
                return Err(NetworkError::StorageIo {
                    path: lock_path.display().to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
}

#[cfg(feature = "iroh-transport")]
fn lock_iroh_secret_key(paths: &ClusterPaths, node_id: &NodeId) -> Result<fs::File, NetworkError> {
    lock_iroh_secret_key_with_wait(paths, node_id, IROH_SECRET_KEY_LOCK_WAIT)
}

#[cfg(feature = "iroh-transport")]
fn iroh_secret_key_lock_retry_delay(attempts: u32) -> Duration {
    let shift = attempts.min(5);
    IROH_SECRET_KEY_LOCK_RETRY_INITIAL
        .saturating_mul(2_u32.saturating_pow(shift))
        .min(IROH_SECRET_KEY_LOCK_RETRY_MAX)
}

#[cfg(feature = "iroh-transport")]
fn file_lock_is_busy(source: &std::io::Error) -> bool {
    source.kind() == std::io::ErrorKind::WouldBlock
        || matches!(source.raw_os_error(), Some(32 | 33))
}

#[cfg(feature = "iroh-transport")]
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
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let key = iroh::SecretKey::generate();
    if write_object_file_create_new_serialized(&key_path, &key)? {
        Ok(key)
    } else {
        read_object_file(&key_path)
    }
}

async fn spawn_roaming_disk_task_on_runtime<T, F>(
    runtime: &tokio::runtime::Handle,
    operation: &'static str,
    task: F,
) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, NetworkError> + Send + 'static,
{
    runtime
        .spawn_blocking(task)
        .await
        .map_err(|source| NetworkError::Io {
            operation: operation.to_string(),
            detail: source.to_string(),
        })?
}

#[cfg(feature = "iroh-transport")]
pub(crate) async fn load_or_create_iroh_secret_key_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<iroh::SecretKey, NetworkError> {
    let paths = paths.clone();
    let node_id = node_id.clone();
    spawn_roaming_disk_task_on_runtime(runtime, "load iroh secret key", move || {
        load_or_create_iroh_secret_key(&paths, &node_id)
    })
    .await
}

#[cfg(all(feature = "iroh-transport", test))]
pub(crate) async fn iroh_public_key_for_node_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<String, NetworkError> {
    Ok(
        load_or_create_iroh_secret_key_async(runtime, paths, node_id)
            .await?
            .public()
            .to_string(),
    )
}

pub(crate) fn read_iroh_endpoint_record(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<IrohEndpointRecord, NetworkError> {
    let path = iroh_endpoint_record_path(paths, node_id);
    read_object_file(&path)
}

pub(crate) async fn read_iroh_endpoint_record_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<IrohEndpointRecord, NetworkError> {
    let paths = paths.clone();
    let node_id = node_id.clone();
    spawn_roaming_disk_task_on_runtime(runtime, "read iroh endpoint record", move || {
        IrohEndpointRecord::read(&paths, &node_id)
    })
    .await
}

pub(crate) fn write_iroh_endpoint_record(
    paths: &ClusterPaths,
    record: &IrohEndpointRecord,
) -> Result<(), NetworkError> {
    write_object_file(&iroh_endpoint_record_path(paths, &record.node_id), record)
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) async fn write_iroh_endpoint_record_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    record: &IrohEndpointRecord,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let record = record.clone();
    spawn_roaming_disk_task_on_runtime(runtime, "write iroh endpoint record", move || {
        record.write(&paths)
    })
    .await
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn try_acquire_iroh_proxy_permit(
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

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn iroh_proxy_route_hint(payload: &[u8]) -> String {
    let Ok(request) = crate::decode_single_object_frame_from_slice::<crate::DaemonControlRequest>(
        payload,
        IROH_DAEMON_COMMAND_MAX_BYTES,
    ) else {
        return IROH_DAEMON_PROXY_ERROR_ROUTE.to_string();
    };
    request.route
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn iroh_daemon_proxy_restart_backoff(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(8);
    IROH_DAEMON_PROXY_RESTART_BACKOFF_MIN
        .saturating_mul(2_u32.saturating_pow(shift))
        .min(IROH_DAEMON_PROXY_RESTART_BACKOFF_MAX)
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn iroh_daemon_proxy_restart_delay(
    consecutive_failures: u32,
    node_id: &NodeId,
) -> Duration {
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

#[cfg(any(feature = "iroh-transport", test))]
#[derive(Clone, Debug)]
pub(crate) struct IrohProxyByteBudget {
    inner: Arc<Mutex<IrohProxyByteBudgetState>>,
    max_bytes: usize,
}

#[cfg(any(feature = "iroh-transport", test))]
#[derive(Debug, Default)]
struct IrohProxyByteBudgetState {
    in_flight_bytes: usize,
}

#[cfg(any(feature = "iroh-transport", test))]
#[derive(Debug)]
pub(crate) struct IrohProxyByteBudgetPermit {
    budget: IrohProxyByteBudget,
    bytes: usize,
}

#[cfg(any(feature = "iroh-transport", test))]
impl IrohProxyByteBudget {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(IrohProxyByteBudgetState::default())),
            max_bytes,
        }
    }

    pub(crate) fn try_acquire(
        &self,
        frame_bytes: usize,
    ) -> Result<IrohProxyByteBudgetPermit, NetworkError> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let would_use = match state.in_flight_bytes.checked_add(frame_bytes) {
            Some(would_use) => would_use,
            None => {
                return Err(NetworkError::IrohProxyFrameBudgetExceeded {
                    frame_bytes,
                    in_flight_bytes: state.in_flight_bytes,
                    max_in_flight_bytes: self.max_bytes,
                });
            }
        };
        if frame_bytes > self.max_bytes || would_use > self.max_bytes {
            return Err(NetworkError::IrohProxyFrameBudgetExceeded {
                frame_bytes,
                in_flight_bytes: state.in_flight_bytes,
                max_in_flight_bytes: self.max_bytes,
            });
        }
        state.in_flight_bytes = would_use;
        Ok(IrohProxyByteBudgetPermit {
            budget: self.clone(),
            bytes: frame_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn in_flight_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .in_flight_bytes
    }
}

#[cfg(any(feature = "iroh-transport", test))]
impl Drop for IrohProxyByteBudgetPermit {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.in_flight_bytes = state.in_flight_bytes.saturating_sub(self.bytes);
    }
}

#[cfg(any(feature = "iroh-transport", test))]
fn reserve_iroh_proxy_frame(
    len_prefix: Vec<u8>,
    body_bytes: usize,
    budget: &IrohProxyByteBudget,
) -> Result<(Vec<u8>, IrohProxyByteBudgetPermit), NetworkError> {
    if body_bytes > IROH_DAEMON_COMMAND_MAX_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: body_bytes,
            max_bytes: IROH_DAEMON_COMMAND_MAX_BYTES,
        });
    }
    let frame_bytes = crate::object::object_frame_total_bytes(len_prefix.len(), body_bytes)?;
    let permit = budget.try_acquire(frame_bytes)?;
    let mut frame = crate::object::try_zeroed_vec(frame_bytes, "reserve iroh daemon proxy frame")?;
    frame[..len_prefix.len()].copy_from_slice(&len_prefix);
    Ok((frame, permit))
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn encode_iroh_proxy_error_response_frame(
    budget: &IrohProxyByteBudget,
    route: impl Into<String>,
    detail: impl Into<String>,
) -> Result<(Vec<u8>, IrohProxyByteBudgetPermit), NetworkError> {
    let response = crate::ObjectRpcResponse::encode_error(route, detail)?;
    let frame = crate::encode_object_frame_to_vec(&response)?;
    let permit = budget.try_acquire(frame.len())?;
    Ok((frame, permit))
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) async fn write_iroh_proxy_frame_async_with_idle_timeout<W>(
    writer: &mut W,
    frame: &[u8],
    scope: &str,
    timeout: Duration,
) -> Result<(), NetworkError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    crate::object::write_all_async_frame(
        writer,
        frame,
        Some(timeout),
        "write iroh daemon proxy frame",
        scope,
    )
    .await
}

#[cfg(feature = "iroh-transport")]
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
    write_iroh_proxy_frame_async_with_idle_timeout(
        writer,
        &frame,
        "proxy error response",
        IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT,
    )
    .await
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) async fn read_iroh_proxy_frame_async<R>(
    reader: &mut R,
    budget: &IrohProxyByteBudget,
    scope: &str,
) -> Result<(Vec<u8>, IrohProxyByteBudgetPermit), NetworkError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut len_bytes = [0_u8; 4];
    read_exact_iroh_proxy_frame_async(
        reader,
        &mut len_bytes,
        format!("read iroh daemon {scope} frame length"),
        "length",
    )
    .await?;
    let mut len_prefix = Vec::new();
    crate::object::try_extend_from_slice(
        &mut len_prefix,
        &len_bytes,
        "read iroh daemon frame prefix",
    )?;
    let extended_len_bytes =
        if u32::from_be_bytes(len_bytes) == crate::OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL {
            let mut extended_len_bytes = [0_u8; 8];
            read_exact_iroh_proxy_frame_async(
                reader,
                &mut extended_len_bytes,
                format!("read iroh daemon {scope} frame extended length"),
                "extended length",
            )
            .await?;
            crate::object::try_extend_from_slice(
                &mut len_prefix,
                &extended_len_bytes,
                "read iroh daemon frame extended prefix",
            )?;
            Some(extended_len_bytes)
        } else {
            None
        };
    let body_bytes = crate::object::object_frame_len_from_prefix(
        len_bytes,
        extended_len_bytes,
        IROH_DAEMON_COMMAND_MAX_BYTES,
    )?;
    let prefix_bytes = len_prefix.len();
    let (mut frame, permit) = reserve_iroh_proxy_frame(len_prefix, body_bytes, budget)?;
    read_exact_iroh_proxy_frame_async(
        reader,
        &mut frame[prefix_bytes..],
        format!("read iroh daemon {scope} frame body"),
        "body",
    )
    .await?;
    if let Some(observer) = io_observer() {
        observer.on_net_recv(frame.len() as u64);
    }
    Ok((frame, permit))
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) async fn read_iroh_proxy_frame_async_with_idle_timeout<R>(
    reader: &mut R,
    budget: &IrohProxyByteBudget,
    scope: &str,
    timeout: Duration,
) -> Result<(Vec<u8>, IrohProxyByteBudgetPermit), NetworkError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut len_bytes = [0_u8; 4];
    read_exact_iroh_proxy_frame_async_with_idle_timeout(
        reader,
        &mut len_bytes,
        timeout,
        format!("read iroh daemon {scope} frame length"),
        "length",
    )
    .await?;
    let mut len_prefix = Vec::new();
    crate::object::try_extend_from_slice(
        &mut len_prefix,
        &len_bytes,
        "read iroh daemon frame prefix",
    )?;
    let extended_len_bytes =
        if u32::from_be_bytes(len_bytes) == crate::OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL {
            let mut extended_len_bytes = [0_u8; 8];
            read_exact_iroh_proxy_frame_async_with_idle_timeout(
                reader,
                &mut extended_len_bytes,
                timeout,
                format!("read iroh daemon {scope} frame extended length"),
                "extended length",
            )
            .await?;
            crate::object::try_extend_from_slice(
                &mut len_prefix,
                &extended_len_bytes,
                "read iroh daemon frame extended prefix",
            )?;
            Some(extended_len_bytes)
        } else {
            None
        };
    let body_bytes = crate::object::object_frame_len_from_prefix(
        len_bytes,
        extended_len_bytes,
        IROH_DAEMON_COMMAND_MAX_BYTES,
    )?;
    let prefix_bytes = len_prefix.len();
    let (mut frame, permit) = reserve_iroh_proxy_frame(len_prefix, body_bytes, budget)?;
    read_exact_iroh_proxy_frame_async_with_idle_timeout(
        reader,
        &mut frame[prefix_bytes..],
        timeout,
        format!("read iroh daemon {scope} frame body"),
        "body",
    )
    .await?;
    if let Some(observer) = io_observer() {
        observer.on_net_recv(frame.len() as u64);
    }
    Ok((frame, permit))
}

#[cfg(any(feature = "iroh-transport", test))]
async fn read_exact_iroh_proxy_frame_async<R>(
    reader: &mut R,
    buf: &mut [u8],
    operation: String,
    part: &'static str,
) -> Result<(), NetworkError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let expected = buf.len();
    let mut read = 0_usize;
    while read < expected {
        match reader.read(&mut buf[read..]).await {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation,
                    detail: iroh_proxy_frame_eof_detail(part, read, expected),
                });
            }
            Ok(n) => read = read.saturating_add(n),
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation,
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(any(feature = "iroh-transport", test))]
async fn read_exact_iroh_proxy_frame_async_with_idle_timeout<R>(
    reader: &mut R,
    buf: &mut [u8],
    idle_timeout: Duration,
    operation: String,
    part: &'static str,
) -> Result<(), NetworkError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let expected = buf.len();
    let mut read = 0_usize;
    while read < expected {
        match tokio::time::timeout(idle_timeout, reader.read(&mut buf[read..])).await {
            Ok(Ok(0)) => {
                return Err(NetworkError::Io {
                    operation,
                    detail: iroh_proxy_frame_eof_detail(part, read, expected),
                });
            }
            Ok(Ok(n)) => read = read.saturating_add(n),
            Ok(Err(source)) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Ok(Err(source)) => {
                return Err(NetworkError::Io {
                    operation,
                    detail: source.to_string(),
                });
            }
            Err(_) => {
                return Err(NetworkError::Io {
                    operation,
                    detail: format!(
                        "timed out after {}ms without {part} read progress",
                        idle_timeout.as_millis()
                    ),
                });
            }
        }
    }
    Ok(())
}

#[cfg(any(feature = "iroh-transport", test))]
fn iroh_proxy_frame_eof_detail(part: &str, read: usize, expected: usize) -> String {
    if read == 0 {
        format!("peer closed before object frame {part} ({expected} bytes expected)")
    } else {
        format!("peer closed during object frame {part}: read {read} of {expected} bytes")
    }
}

#[cfg(feature = "iroh-transport")]
enum IrohDaemonProxyReady {
    Async(tokio::sync::oneshot::Sender<Result<IrohEndpointRecord, NetworkError>>),
}

#[cfg(feature = "iroh-transport")]
impl IrohDaemonProxyReady {
    fn send(self, result: Result<IrohEndpointRecord, NetworkError>) {
        match self {
            Self::Async(sender) => {
                let _ = sender.send(result);
            }
        }
    }
}

#[cfg(feature = "iroh-transport")]
fn send_iroh_daemon_proxy_ready(
    ready: &mut Option<IrohDaemonProxyReady>,
    result: Result<IrohEndpointRecord, NetworkError>,
) {
    if let Some(sender) = ready.take() {
        sender.send(result);
    }
}

#[cfg(feature = "iroh-transport")]
pub async fn start_iroh_daemon_proxy(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    cluster_id: ClusterId,
    node_id: NodeId,
    daemon_address: String,
) -> Result<IrohDaemonProxyHandle, NetworkError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let supervisor_runtime = runtime.clone();
    let task = runtime.spawn(run_iroh_daemon_proxy_supervisor(
        supervisor_runtime,
        paths,
        cluster_id,
        node_id,
        daemon_address,
        Some(IrohDaemonProxyReady::Async(tx)),
    ));
    let record = match rx.await {
        Ok(Ok(record)) => record,
        Ok(Err(error)) => {
            task.abort();
            return Err(error);
        }
        Err(source) => {
            task.abort();
            return Err(NetworkError::Io {
                operation: "start iroh daemon proxy".to_string(),
                detail: source.to_string(),
            });
        }
    };
    Ok(IrohDaemonProxyHandle {
        record,
        task: Some(task),
    })
}

#[cfg(not(feature = "iroh-transport"))]
pub async fn start_iroh_daemon_proxy(
    _runtime: tokio::runtime::Handle,
    _paths: ClusterPaths,
    _cluster_id: ClusterId,
    _node_id: NodeId,
    _daemon_address: String,
) -> Result<IrohDaemonProxyHandle, NetworkError> {
    Err(NetworkError::Io {
        operation: "start iroh daemon proxy".to_string(),
        detail: "sinomo-network was built without iroh-transport".to_string(),
    })
}

#[cfg(feature = "iroh-transport")]
async fn run_iroh_daemon_proxy_supervisor(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    cluster_id: ClusterId,
    node_id: NodeId,
    daemon_address: String,
    mut first_ready: Option<IrohDaemonProxyReady>,
) {
    let mut restart_failures = 0_u32;
    loop {
        let secret_key =
            match load_or_create_iroh_secret_key_async(&runtime, &paths, &node_id).await {
                Ok(secret_key) => secret_key,
                Err(error) => {
                    if first_ready.is_some() {
                        send_iroh_daemon_proxy_ready(&mut first_ready, Err(error));
                        return;
                    }
                    eprintln!("sinomo iroh daemon proxy restart failed: {error}");
                    restart_failures = restart_failures.saturating_add(1);
                    tokio::time::sleep(iroh_daemon_proxy_restart_delay(restart_failures, &node_id))
                        .await;
                    continue;
                }
            };
        let run_started = std::time::Instant::now();
        let result = run_iroh_daemon_proxy(
            runtime.clone(),
            paths.clone(),
            cluster_id.clone(),
            node_id.clone(),
            daemon_address.clone(),
            secret_key,
            first_ready.take(),
        )
        .await;
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
        tokio::time::sleep(iroh_daemon_proxy_restart_delay(restart_failures, &node_id)).await;
    }
}

#[cfg(feature = "iroh-transport")]
async fn run_iroh_daemon_proxy(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    cluster_id: ClusterId,
    node_id: NodeId,
    daemon_address: String,
    secret_key: iroh::SecretKey,
    mut ready: Option<IrohDaemonProxyReady>,
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
    let _ = tokio::time::timeout(IROH_DAEMON_ONLINE_TIMEOUT, endpoint.online()).await;
    let record = iroh_endpoint_record_from_addr(&cluster_id, &node_id, endpoint.addr());
    let persist_result = write_iroh_endpoint_record_async(&runtime, &paths, &record)
        .await
        .map_err(|error| match error {
            NetworkError::Io { detail, .. } => NetworkError::Io {
                operation: "persist iroh endpoint record".to_string(),
                detail,
            },
            other => other,
        });
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
    while let Some(connecting) = endpoint.accept().await {
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
        let connection_runtime = runtime.clone();
        runtime.spawn(async move {
            let _connection_permit = connection_permit;
            let Ok(connection) = connecting.await else {
                return;
            };
            loop {
                let (mut send, mut recv) = match accept_iroh_proxy_command_stream_with_idle_timeout(
                    connection.accept_bi(),
                    IROH_DAEMON_PROXY_CONNECTION_IDLE_TIMEOUT,
                )
                .await
                {
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
                connection_runtime.spawn(async move {
                    let _command_permit = command_permit;
                    let (request, request_budget_permit) =
                        match read_iroh_proxy_frame_async_with_idle_timeout(
                            &mut recv,
                            &command_byte_budget,
                            "command request",
                            IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT,
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
                    if write_iroh_proxy_frame_async_with_idle_timeout(
                        &mut send,
                        &response,
                        "proxy response",
                        IROH_DAEMON_PROXY_STREAM_IO_TIMEOUT,
                    )
                    .await
                    .is_ok()
                    {
                        let _ = send.finish();
                    }
                    drop(response_budget_permit);
                });
            }
        });
    }
    Ok(())
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) async fn accept_iroh_proxy_command_stream_with_idle_timeout<F, T, E>(
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

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn iroh_proxy_command_stream_accept_error_is_peer_close(error: &NetworkError) -> bool {
    let NetworkError::Io { operation, detail } = error else {
        return false;
    };
    operation == "accept iroh daemon command stream"
        && (detail.contains("closed by peer") || detail.contains("stopped"))
}

#[cfg(feature = "iroh-transport")]
pub fn iroh_endpoint_record_from_addr(
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

#[cfg(test)]
pub(crate) fn iroh_endpoint_record_from_cluster_endpoint(
    endpoint: &ClusterEndpointRecord,
    cluster_id: &ClusterId,
) -> Result<IrohEndpointRecord, NetworkError> {
    if endpoint.transport != ClusterTransportKind::Iroh {
        return Err(NetworkError::Io {
            operation: "parse iroh endpoint".to_string(),
            detail: format!("endpoint {} is not an iroh endpoint", endpoint.endpoint_id),
        });
    }
    iroh_endpoint_record_from_address(&endpoint.address, cluster_id, &endpoint.node_id)
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn iroh_endpoint_record_from_address(
    address: &str,
    cluster_id: &ClusterId,
    node_id: &NodeId,
) -> Result<IrohEndpointRecord, NetworkError> {
    let rest = address
        .strip_prefix("iroh://")
        .ok_or_else(|| NetworkError::Serialization {
            detail: format!("invalid iroh endpoint address `{address}`"),
        })?;
    let (public_key, query) = rest.split_once('?').unwrap_or((rest, ""));
    if public_key.trim().is_empty() {
        return Err(NetworkError::Serialization {
            detail: format!("invalid iroh endpoint address `{address}`"),
        });
    }
    let mut direct_addresses = Vec::new();
    let mut relay_url = None;
    for part in query.split('&').filter(|part| !part.trim().is_empty()) {
        if let Some(value) = part.strip_prefix("direct=") {
            direct_addresses.extend(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned),
            );
        } else if let Some(value) = part.strip_prefix("relay=") {
            if !value.trim().is_empty() {
                relay_url = Some(value.to_string());
            }
        }
    }
    let relay_mode = relay_url
        .as_ref()
        .map(|relay_url| IrohRelayMode::Custom {
            relay_url: relay_url.clone(),
        })
        .unwrap_or(IrohRelayMode::Default);
    Ok(IrohEndpointRecord {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        endpoint_public_key: public_key.to_string(),
        direct_addresses,
        relay_url,
        relay_mode,
        expires_after_tick: 0,
    })
}

#[cfg(feature = "iroh-transport")]
async fn send_iroh_daemon_control_request_to_address_async(
    address: &str,
    request: &crate::DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    let record = iroh_endpoint_record_from_address(
        address,
        &ClusterId::from_legacy_named("cluster_iroh_dial_placeholder").expect("static cluster id"),
        &NodeId::from_legacy_named("node_iroh_dial_placeholder").expect("static node id"),
    )?;
    send_iroh_daemon_control_request_async(record, request, timeouts).await
}

#[cfg(not(feature = "iroh-transport"))]
async fn send_iroh_daemon_control_request_to_address_async(
    address: &str,
    _request: &crate::DaemonControlRequest,
    _timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    Err(NetworkError::Io {
        operation: "send iroh daemon rpc".to_string(),
        detail: format!("cannot dial {address}; sinomo-network was built without iroh-transport"),
    })
}

#[cfg(feature = "iroh-transport")]
async fn send_iroh_daemon_control_request_async(
    record: IrohEndpointRecord,
    request: &crate::DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    let remote_addr = iroh_endpoint_addr_from_record(&record)?;
    let request_bytes = crate::encode_object_frame_to_vec(request)?;
    let mut attempt = 0_usize;
    loop {
        attempt += 1;
        match send_iroh_daemon_control_request_attempt_async(
            remote_addr.clone(),
            &request_bytes,
            timeouts,
        )
        .await
        {
            Ok(response) => return Ok(response),
            Err(error) => {
                let retry = iroh_daemon_control_attempt_should_retry(request, &error, attempt);
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

#[cfg(feature = "iroh-transport")]
async fn send_iroh_daemon_control_request_attempt_async(
    remote_addr: iroh::EndpointAddr,
    request_bytes: &[u8],
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, IrohDaemonControlAttemptError> {
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
    if let Some(connect_timeout) = timeouts.connect_timeout {
        let online_timeout = connect_timeout.min(Duration::from_millis(1000));
        let _ = tokio::time::timeout(online_timeout, endpoint.online()).await;
    }

    let connect = endpoint.connect(remote_addr, IROH_ROAMING_PROTOCOL.as_bytes());
    let connection = if let Some(connect_timeout) = timeouts.connect_timeout {
        match tokio::time::timeout(connect_timeout, connect).await {
            Ok(Ok(connection)) => connection,
            Ok(Err(source)) => {
                close_iroh_endpoint(&endpoint).await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "connect iroh endpoint".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
            Err(_) => {
                close_iroh_endpoint(&endpoint).await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    iroh_daemon_command_timeout_error(connect_timeout),
                ));
            }
        }
    } else {
        match connect.await {
            Ok(connection) => connection,
            Err(source) => {
                close_iroh_endpoint(&endpoint).await;
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
                close_iroh_endpoint(&endpoint).await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "open iroh daemon stream".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
            Err(_) => {
                connection.close(0_u8.into(), b"open stream timed out");
                close_iroh_endpoint(&endpoint).await;
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
                close_iroh_endpoint(&endpoint).await;
                return Err(IrohDaemonControlAttemptError::BeforeRequestAccepted(
                    NetworkError::Io {
                        operation: "open iroh daemon stream".to_string(),
                        detail: source.to_string(),
                    },
                ));
            }
        }
    };

    let write_result = if let Some(idle_timeout) = timeouts.idle_timeout {
        write_iroh_daemon_control_request_bytes_with_idle_timeout(
            &mut send,
            request_bytes,
            idle_timeout,
        )
        .await
    } else {
        write_iroh_daemon_control_request_bytes(&mut send, request_bytes).await
    };
    match write_result {
        Ok(()) => {}
        Err(source) => {
            connection.close(0_u8.into(), b"write failed");
            close_iroh_endpoint(&endpoint).await;
            return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(source));
        }
    }
    if let Err(source) = send.finish() {
        connection.close(0_u8.into(), b"finish failed");
        close_iroh_endpoint(&endpoint).await;
        return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(
            NetworkError::Io {
                operation: "finish iroh daemon command".to_string(),
                detail: source.to_string(),
            },
        ));
    }
    let response = match read_iroh_daemon_control_response_async(&mut recv, timeouts).await {
        Ok(response) => response,
        Err(error) => {
            connection.close(0_u8.into(), b"read failed");
            close_iroh_endpoint(&endpoint).await;
            return Err(IrohDaemonControlAttemptError::AfterRequestAccepted(error));
        }
    };
    connection.close(0_u8.into(), b"done");
    close_iroh_endpoint(&endpoint).await;
    Ok(response)
}

#[cfg(feature = "iroh-transport")]
#[derive(Debug)]
enum IrohDaemonControlAttemptError {
    BeforeRequestAccepted(NetworkError),
    AfterRequestAccepted(NetworkError),
}

#[cfg(feature = "iroh-transport")]
impl IrohDaemonControlAttemptError {
    fn into_network_error(self) -> NetworkError {
        match self {
            Self::BeforeRequestAccepted(error) | Self::AfterRequestAccepted(error) => error,
        }
    }
}

#[cfg(feature = "iroh-transport")]
fn iroh_daemon_control_attempt_should_retry(
    request: &crate::DaemonControlRequest,
    error: &IrohDaemonControlAttemptError,
    attempt: usize,
) -> bool {
    if attempt >= IROH_DAEMON_RPC_MAX_ATTEMPTS {
        return false;
    }
    match error {
        IrohDaemonControlAttemptError::BeforeRequestAccepted(_) => true,
        IrohDaemonControlAttemptError::AfterRequestAccepted(error) => {
            iroh_daemon_control_request_allows_response_retry(request)
                && iroh_daemon_response_error_is_retryable(error)
        }
    }
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn iroh_daemon_control_request_allows_response_retry(
    request: &crate::DaemonControlRequest,
) -> bool {
    if request.is_response_retry_safe() {
        return true;
    }
    matches!(
        request.route.as_str(),
        crate::CLUSTER_HEALTH_ROUTE
            | sinomo_kv::CLUSTER_KV_RANGE_PREFIX_ROUTE
            | sinomo_kv::CLUSTER_KV_SNAPSHOT_ROUTE
    )
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn iroh_daemon_response_error_is_retryable(error: &NetworkError) -> bool {
    matches!(
        error,
        NetworkError::Io { operation, detail }
            if operation == "read iroh daemon response"
                && (detail.starts_with("peer closed before object frame")
                    || detail.starts_with("peer closed during object frame"))
    )
}

#[cfg(test)]
pub(crate) fn iroh_daemon_rpc_retry_delay(attempt: usize, remaining: Duration) -> Option<Duration> {
    iroh_daemon_rpc_retry_delay_with_budget(attempt, Some(remaining))
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn iroh_daemon_rpc_retry_delay_with_budget(
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

#[cfg(feature = "iroh-transport")]
fn iroh_daemon_command_timeout_error(timeout: Duration) -> NetworkError {
    NetworkError::Io {
        operation: "send iroh daemon command".to_string(),
        detail: format!("timed out after {}ms", timeout.as_millis()),
    }
}

#[cfg(feature = "iroh-transport")]
async fn write_iroh_daemon_control_request_bytes<W>(
    writer: &mut W,
    bytes: &[u8],
) -> Result<(), NetworkError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    writer
        .write_all(bytes)
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write iroh daemon command".to_string(),
            detail: source.to_string(),
        })?;
    writer.flush().await.map_err(|source| NetworkError::Io {
        operation: "write iroh daemon command".to_string(),
        detail: source.to_string(),
    })
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) async fn write_iroh_daemon_control_request_bytes_with_idle_timeout<W>(
    writer: &mut W,
    bytes: &[u8],
    idle_timeout: Duration,
) -> Result<(), NetworkError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    let expected = bytes.len();
    let mut written = 0_usize;
    while written < expected {
        match tokio::time::timeout(idle_timeout, writer.write(&bytes[written..])).await {
            Ok(Ok(0)) => {
                return Err(NetworkError::Io {
                    operation: "write iroh daemon command".to_string(),
                    detail: format!(
                        "peer stopped accepting command frame: wrote {written} of {expected} bytes"
                    ),
                });
            }
            Ok(Ok(n)) => written = written.saturating_add(n),
            Ok(Err(source)) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Ok(Err(source)) => {
                return Err(NetworkError::Io {
                    operation: "write iroh daemon command".to_string(),
                    detail: source.to_string(),
                });
            }
            Err(_) => {
                return Err(NetworkError::Io {
                    operation: "write iroh daemon command".to_string(),
                    detail: format!(
                        "timed out after {}ms without write progress",
                        idle_timeout.as_millis()
                    ),
                });
            }
        }
    }
    match tokio::time::timeout(idle_timeout, writer.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(NetworkError::Io {
            operation: "write iroh daemon command".to_string(),
            detail: source.to_string(),
        }),
        Err(_) => Err(NetworkError::Io {
            operation: "write iroh daemon command".to_string(),
            detail: format!(
                "timed out after {}ms without flush progress",
                idle_timeout.as_millis()
            ),
        }),
    }
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) async fn read_iroh_daemon_control_response_async<R>(
    reader: &mut R,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let response = crate::read_object_frame_async(
        reader,
        IROH_DAEMON_COMMAND_MAX_BYTES,
        crate::ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout),
    )
    .await;
    match response {
        Ok(response) => Ok(response),
        Err(error) => Err(iroh_daemon_response_frame_error(error)),
    }
}

#[cfg(test)]
pub(crate) fn decode_iroh_daemon_control_response_bytes(
    response: &[u8],
) -> Result<crate::DaemonControlResponse, NetworkError> {
    crate::decode_single_object_frame_from_slice(response, IROH_DAEMON_COMMAND_MAX_BYTES)
        .map_err(iroh_daemon_response_frame_error)
}

#[cfg(any(feature = "iroh-transport", test))]
fn iroh_daemon_response_frame_error(error: NetworkError) -> NetworkError {
    match error {
        NetworkError::Io { detail, .. } => NetworkError::Io {
            operation: "read iroh daemon response".to_string(),
            detail,
        },
        error => error,
    }
}

#[cfg(feature = "iroh-transport")]
async fn close_iroh_endpoint(endpoint: &iroh::Endpoint) {
    let _ = tokio::time::timeout(Duration::from_millis(250), endpoint.close()).await;
}

#[cfg(feature = "iroh-transport")]
fn iroh_endpoint_addr_from_record(
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

pub(crate) async fn send_daemon_rpc_to_cluster_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    let control_request = daemon_rpc_control_request(auth, request);
    let response = if crate::object_rpc_request_expects_raw_daemon_response(request) {
        send_daemon_control_rpc_request_raw_response_to_cluster_endpoint_async(
            endpoint,
            &control_request,
            timeouts,
        )
        .await?
    } else {
        send_daemon_control_request_to_cluster_endpoint_async(endpoint, &control_request, timeouts)
            .await?
    };
    response.error_detail()?;
    Ok(response)
}

fn daemon_rpc_control_request(
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
) -> crate::DaemonControlRequest {
    match auth {
        Some(auth) => request.clone().with_auth(auth.to_vec()),
        None => request.clone(),
    }
}

async fn send_daemon_control_request_to_cluster_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &crate::DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    match endpoint.transport {
        ClusterTransportKind::Iroh => {
            send_iroh_daemon_control_request_to_address_async(&endpoint.address, request, timeouts)
                .await
        }
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            send_daemon_control_request_to_address_async(&endpoint.address, request, timeouts).await
        }
        _ => Err(NetworkError::Io {
            operation: format!("send daemon rpc to {}", endpoint.address),
            detail: format!("unsupported daemon rpc transport {:?}", endpoint.transport),
        }),
    }
}

async fn send_daemon_control_rpc_request_raw_response_to_cluster_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &crate::DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            crate::send_daemon_control_rpc_request_raw_response_async(
                &endpoint.address,
                request,
                timeouts,
                IROH_DAEMON_COMMAND_MAX_BYTES,
            )
            .await
        }
        ClusterTransportKind::Iroh => Err(NetworkError::Io {
            operation: format!("send streamed daemon rpc to {}", endpoint.address),
            detail: "streamed raw RPC responses are not supported over iroh proxy transport"
                .to_string(),
        }),
        _ => Err(NetworkError::Io {
            operation: format!("send streamed daemon rpc to {}", endpoint.address),
            detail: format!("unsupported daemon rpc transport {:?}", endpoint.transport),
        }),
    }
}

async fn send_daemon_control_request_to_address_async(
    address: &str,
    request: &crate::DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    if daemon_rpc_address_uses_iroh(address) {
        send_iroh_daemon_control_request_to_address_async(address, request, timeouts).await
    } else {
        crate::send_daemon_control_request_async(
            address,
            request,
            timeouts,
            IROH_DAEMON_COMMAND_MAX_BYTES,
        )
        .await
    }
}

fn daemon_rpc_address_uses_iroh(address: &str) -> bool {
    address.trim_start().starts_with("iroh://")
}

#[cfg(feature = "iroh-transport")]
fn daemon_control_timeout_detail(timeout: Duration) -> String {
    format!("timed out after {}ms", timeout.as_millis())
}

#[cfg(test)]
pub(crate) fn configure_daemon_control_stream_idle_timeout(
    stream: &std::net::TcpStream,
    address: &str,
    timeout: Duration,
    read: bool,
    write: bool,
) -> Result<(), NetworkError> {
    if read {
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|source| NetworkError::Io {
                operation: format!("configure daemon control address {address}"),
                detail: source.to_string(),
            })?;
    }
    if write {
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|source| NetworkError::Io {
                operation: format!("configure daemon control address {address}"),
                detail: source.to_string(),
            })?;
    }
    Ok(())
}

#[cfg(feature = "iroh-transport")]
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
                detail: daemon_control_timeout_detail(connect_timeout),
            })?
    } else {
        connect.await
    }
    .map_err(|source| NetworkError::Io {
        operation: format!("connect daemon control address {address}"),
        detail: source.to_string(),
    })?;
    if let Some(idle_timeout) = timeouts.idle_timeout {
        crate::object::write_all_async_frame(
            &mut stream,
            payload,
            Some(idle_timeout),
            "write daemon control address",
            "body",
        )
        .await
        .map_err(|error| match error {
            NetworkError::Io { detail, .. } => NetworkError::Io {
                operation: format!("write daemon control address {address}"),
                detail,
            },
            other => other,
        })?;
    } else {
        write_iroh_daemon_control_request_bytes(&mut stream, payload)
            .await
            .map_err(|error| match error {
                NetworkError::Io { detail, .. } => NetworkError::Io {
                    operation: format!("write daemon control address {address}"),
                    detail,
                },
                other => other,
            })?;
    }
    if let Some(idle_timeout) = timeouts.idle_timeout {
        tokio::time::timeout(idle_timeout, stream.shutdown())
            .await
            .map_err(|_| NetworkError::Io {
                operation: format!("finish daemon control address {address}"),
                detail: daemon_control_timeout_detail(idle_timeout),
            })?
            .map_err(|source| NetworkError::Io {
                operation: format!("finish daemon control address {address}"),
                detail: source.to_string(),
            })?;
    } else {
        stream.shutdown().await.map_err(|source| NetworkError::Io {
            operation: format!("finish daemon control address {address}"),
            detail: source.to_string(),
        })?;
    }
    if let Some(idle_timeout) = timeouts.idle_timeout {
        read_iroh_proxy_frame_async_with_idle_timeout(
            &mut stream,
            budget,
            "command response",
            idle_timeout,
        )
        .await
    } else {
        read_iroh_proxy_frame_async(&mut stream, budget, "command response").await
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct ClusterAuthMaterial {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub node_public_key: String,
    pub certificate: ClusterCertificate,
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct ClusterAuthVerifier {
    members: BTreeMap<NodeId, ClusterMemberRecord>,
}

#[cfg(test)]
impl ClusterAuthVerifier {
    pub fn new(members: impl IntoIterator<Item = ClusterMemberRecord>) -> Self {
        Self {
            members: members
                .into_iter()
                .map(|member| (member.node_id.clone(), member))
                .collect(),
        }
    }

    pub fn verify_peer(
        &self,
        local_cluster_id: &ClusterId,
        peer: &ClusterAuthMaterial,
        now: OffsetDateTime,
    ) -> Result<(), NetworkError> {
        if &peer.cluster_id != local_cluster_id {
            return Err(NetworkError::ClusterMismatch {
                expected: local_cluster_id.clone(),
                actual: peer.cluster_id.clone(),
            });
        }
        if peer.certificate.cluster_id != *local_cluster_id {
            return Err(NetworkError::ClusterMismatch {
                expected: local_cluster_id.clone(),
                actual: peer.certificate.cluster_id.clone(),
            });
        }
        if peer.certificate.node_id != peer.node_id {
            return Err(NetworkError::CertificateNodeMismatch {
                material_node_id: peer.node_id.clone(),
                certificate_node_id: peer.certificate.node_id.clone(),
            });
        }
        if peer.certificate.public_key != peer.node_public_key {
            return Err(NetworkError::CertificateKeyMismatch {
                node_id: peer.node_id.clone(),
            });
        }
        if peer.certificate.expires_at <= now {
            return Err(NetworkError::CertificateExpired {
                node_id: peer.node_id.clone(),
            });
        }
        if peer.certificate.signature.trim().is_empty() {
            return Err(NetworkError::CertificateSignatureMissing {
                node_id: peer.node_id.clone(),
            });
        }

        let member =
            self.members
                .get(&peer.node_id)
                .ok_or_else(|| NetworkError::UnauthorizedNode {
                    node_id: peer.node_id.clone(),
                })?;
        if !member.state.is_authorized() {
            return Err(NetworkError::UnauthorizedNode {
                node_id: peer.node_id.clone(),
            });
        }
        if member.node_public_key != peer.node_public_key {
            return Err(NetworkError::CertificateKeyMismatch {
                node_id: peer.node_id.clone(),
            });
        }
        Ok(())
    }

    pub fn establish_mutual_session(
        &self,
        local: &ClusterAuthMaterial,
        remote: &ClusterAuthMaterial,
        now: OffsetDateTime,
    ) -> Result<AuthenticatedClusterSession, NetworkError> {
        self.verify_peer(&local.cluster_id, local, now)?;
        self.verify_peer(&local.cluster_id, remote, now)?;
        Ok(AuthenticatedClusterSession {
            cluster_id: local.cluster_id.clone(),
            local_node_id: local.node_id.clone(),
            remote_node_id: remote.node_id.clone(),
            established_at: now,
        })
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct AuthenticatedClusterSession {
    pub cluster_id: ClusterId,
    pub local_node_id: NodeId,
    pub remote_node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub established_at: OffsetDateTime,
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct LanConnectionPlanner;

#[cfg(test)]
impl LanConnectionPlanner {
    pub fn select_direct_candidate<'a>(
        candidates: &'a [PeerEndpointCandidate],
    ) -> Option<&'a PeerEndpointCandidate> {
        candidates
            .iter()
            .filter(|candidate| {
                matches!(
                    candidate.endpoint.transport,
                    ClusterTransportKind::LanQuic | ClusterTransportKind::LanTcp
                ) && matches!(
                    candidate.endpoint.privacy,
                    ClusterEndpointPrivacy::Lan | ClusterEndpointPrivacy::LocalOnly
                ) && candidate.status != EndpointCandidateStatus::Failed
            })
            .min_by_key(|candidate| {
                let transport_score = match candidate.endpoint.transport {
                    ClusterTransportKind::LanQuic => 0,
                    ClusterTransportKind::LanTcp => 100,
                    _ => 1_000,
                };
                transport_score + u32::from(candidate.score)
            })
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) enum RoamingPathSelection {
    Direct(PeerEndpointCandidate),
    Iroh(PeerEndpointCandidate),
    WebRtc(PeerEndpointCandidate),
    Relay(PeerEndpointCandidate),
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct RoamingConnectionPlanner;

#[cfg(test)]
impl RoamingConnectionPlanner {
    pub fn select_path(candidates: &[PeerEndpointCandidate]) -> Option<RoamingPathSelection> {
        if let Some(candidate) = LanConnectionPlanner::select_direct_candidate(candidates) {
            return Some(RoamingPathSelection::Direct(candidate.clone()));
        }

        if let Some(candidate) = candidates
            .iter()
            .filter(|candidate| {
                candidate.endpoint.transport == ClusterTransportKind::Iroh
                    && candidate.status != EndpointCandidateStatus::Failed
            })
            .min_by_key(|candidate| {
                let privacy_score = match candidate.endpoint.privacy {
                    ClusterEndpointPrivacy::InternetDirect => 30,
                    ClusterEndpointPrivacy::Relayed => 80,
                    ClusterEndpointPrivacy::Lan => 120,
                    ClusterEndpointPrivacy::LocalOnly => 200,
                };
                privacy_score + u32::from(candidate.score)
            })
        {
            return Some(RoamingPathSelection::Iroh(candidate.clone()));
        }

        if let Some(candidate) = candidates
            .iter()
            .filter(|candidate| {
                candidate.endpoint.transport == ClusterTransportKind::WebRtc
                    && candidate.status != EndpointCandidateStatus::Failed
            })
            .min_by_key(|candidate| {
                let privacy_score = match candidate.endpoint.privacy {
                    ClusterEndpointPrivacy::Lan => 20,
                    ClusterEndpointPrivacy::InternetDirect => 40,
                    ClusterEndpointPrivacy::LocalOnly => 80,
                    ClusterEndpointPrivacy::Relayed => 300,
                };
                privacy_score + u32::from(candidate.score)
            })
        {
            return Some(RoamingPathSelection::WebRtc(candidate.clone()));
        }

        candidates
            .iter()
            .filter(|candidate| {
                matches!(
                    candidate.endpoint.transport,
                    ClusterTransportKind::TurnRelay | ClusterTransportKind::HttpsRelay
                ) && candidate.status != EndpointCandidateStatus::Failed
            })
            .min_by_key(|candidate| {
                let relay_score = match candidate.endpoint.transport {
                    ClusterTransportKind::TurnRelay => 100,
                    ClusterTransportKind::HttpsRelay => 200,
                    _ => 500,
                };
                relay_score + u32::from(candidate.score)
            })
            .cloned()
            .map(RoamingPathSelection::Relay)
    }
}
