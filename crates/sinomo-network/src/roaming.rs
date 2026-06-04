use crate::error::NetworkError;
use crate::peer::{EndpointCandidateStatus, PeerEndpointCandidate};
use crate::prelude::*;
use crate::storage::{ClusterPaths, read_object_file, write_object_file};

pub const WEBRTC_SIGNALING_PROTOCOL: &str = "sinomo.webrtc-signaling.v1";
pub const IROH_ROAMING_PROTOCOL: &str = "sinomo.iroh-roaming.v1";
pub const IROH_DAEMON_COMMAND_MAX_BYTES: usize = 128 * 1024 * 1024;
pub const IROH_DAEMON_ONLINE_TIMEOUT: Duration = Duration::from_secs(5);
pub const SINOMO_IROH_RELAY_URLS_ENV: &str = "SINOMO_IROH_RELAY_URLS";
#[cfg(feature = "iroh-transport")]
const IROH_DAEMON_PROXY_RESTART_BACKOFF: Duration = Duration::from_secs(5);
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_MAX_CONNECTIONS: usize = 128;
#[cfg(any(feature = "iroh-transport", test))]
pub(crate) const IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS: usize = 512;
const SINOMO_DEFAULT_IROH_RELAY_URLS: &[&str] = &[
    "https://use1-1.relay.n0.iroh-canary.iroh.link./",
    "https://usw1-1.relay.n0.iroh-canary.iroh.link./",
    "https://euc1-1.relay.n0.iroh-canary.iroh.link./",
    "https://aps1-1.relay.n0.iroh-canary.iroh.link./",
];

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WebRtcSignalingEnvelope {
    pub cluster_id: ClusterId,
    pub exchange_id: String,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub sequence: u64,
    pub expires_after_tick: u64,
    pub message: WebRtcSignalMessage,
}

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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WebRtcSignalMessage {
    Offer { sdp: String },
    Answer { sdp: String },
    IceCandidate(IceCandidateRecord),
    EndOfCandidates,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IceCandidateRecord {
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum IceCandidateType {
    Host,
    ServerReflexive,
    PeerReflexive,
    Relay,
}

pub struct WebRtcKvExchange;

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub direct_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
    pub relay_mode: IrohRelayMode,
    pub expires_after_tick: u64,
}

impl IrohEndpointRecord {
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
}

pub fn stable_iroh_endpoint_id(node_id: &NodeId, endpoint_public_key: &str) -> ClusterEndpointId {
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

pub fn default_sinomo_iroh_relay_urls() -> Vec<String> {
    sinomo_iroh_relay_url_strings_from_env(std::env::var(SINOMO_IROH_RELAY_URLS_ENV).ok())
}

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

#[cfg(feature = "iroh-transport")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IrohEndpointDialConfig {
    pub endpoint_public_key: iroh::PublicKey,
    pub alpn: Vec<u8>,
    pub relay_url: Option<String>,
    pub direct_addresses: Vec<String>,
}

#[cfg(feature = "iroh-transport")]
impl IrohEndpointDialConfig {
    pub fn from_record(record: &IrohEndpointRecord) -> Result<Self, NetworkError> {
        let endpoint_public_key =
            record
                .endpoint_public_key
                .parse()
                .map_err(|source| NetworkError::Serialization {
                    detail: format!("invalid iroh public key: {source}"),
                })?;
        Ok(Self {
            endpoint_public_key,
            alpn: IROH_ROAMING_PROTOCOL.as_bytes().to_vec(),
            relay_url: record.relay_url.clone(),
            direct_addresses: record.direct_addresses.clone(),
        })
    }
}

pub struct IrohKvExchange;

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

pub fn iroh_endpoint_key_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    paths.iroh.join(format!("{}.secret.sobj", node_id.as_str()))
}

pub fn iroh_endpoint_record_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    paths
        .iroh
        .join(format!("{}.endpoint.sobj", node_id.as_str()))
}

#[cfg(feature = "iroh-transport")]
pub fn load_or_create_iroh_secret_key(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<iroh::SecretKey, NetworkError> {
    let key_path = iroh_endpoint_key_path(paths, node_id);
    if key_path.exists() {
        return read_object_file(&key_path);
    }
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let key = iroh::SecretKey::generate();
    write_object_file(&key_path, &key)?;
    Ok(key)
}

#[cfg(not(feature = "iroh-transport"))]
pub fn load_or_create_iroh_secret_key(
    _paths: &ClusterPaths,
    _node_id: &NodeId,
) -> Result<(), NetworkError> {
    Err(NetworkError::Io {
        operation: "iroh secret key".to_string(),
        detail: "sinomo-network was built without iroh-transport".to_string(),
    })
}

#[cfg(feature = "iroh-transport")]
pub fn iroh_public_key_for_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<String, NetworkError> {
    Ok(load_or_create_iroh_secret_key(paths, node_id)?
        .public()
        .to_string())
}

#[cfg(not(feature = "iroh-transport"))]
pub fn iroh_public_key_for_node(
    _paths: &ClusterPaths,
    _node_id: &NodeId,
) -> Result<String, NetworkError> {
    Err(NetworkError::Io {
        operation: "iroh public key".to_string(),
        detail: "sinomo-network was built without iroh-transport".to_string(),
    })
}

pub fn read_iroh_endpoint_record(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<IrohEndpointRecord, NetworkError> {
    let path = iroh_endpoint_record_path(paths, node_id);
    read_object_file(&path)
}

pub fn write_iroh_endpoint_record(
    paths: &ClusterPaths,
    record: &IrohEndpointRecord,
) -> Result<(), NetworkError> {
    write_object_file(&iroh_endpoint_record_path(paths, &record.node_id), record)
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

#[cfg(feature = "iroh-transport")]
pub fn start_iroh_daemon_proxy(
    paths: ClusterPaths,
    cluster_id: ClusterId,
    node_id: NodeId,
    daemon_address: String,
) -> Result<IrohEndpointRecord, NetworkError> {
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name(format!("sinomo-iroh-{}", node_id.as_str()))
        .spawn(move || {
            let mut first_ready = Some(tx);
            loop {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(source) => {
                        let error = NetworkError::Io {
                            operation: "start iroh runtime".to_string(),
                            detail: source.to_string(),
                        };
                        if let Some(ready) = first_ready.take() {
                            let _ = ready.send(Err(error));
                            return;
                        }
                        eprintln!("sinomo iroh daemon proxy restart failed: {error}");
                        thread::sleep(IROH_DAEMON_PROXY_RESTART_BACKOFF);
                        continue;
                    }
                };
                let secret_key = match load_or_create_iroh_secret_key(&paths, &node_id) {
                    Ok(secret_key) => secret_key,
                    Err(error) => {
                        if let Some(ready) = first_ready.take() {
                            let _ = ready.send(Err(error));
                            return;
                        }
                        eprintln!("sinomo iroh daemon proxy restart failed: {error}");
                        runtime.shutdown_timeout(Duration::from_millis(250));
                        thread::sleep(IROH_DAEMON_PROXY_RESTART_BACKOFF);
                        continue;
                    }
                };
                let result = runtime.block_on(run_iroh_daemon_proxy(
                    paths.clone(),
                    cluster_id.clone(),
                    node_id.clone(),
                    daemon_address.clone(),
                    secret_key,
                    first_ready.take(),
                ));
                runtime.shutdown_timeout(Duration::from_millis(250));
                if let Err(error) = result {
                    eprintln!("sinomo iroh daemon proxy stopped: {error}");
                } else {
                    eprintln!("sinomo iroh daemon proxy stopped");
                }
                thread::sleep(IROH_DAEMON_PROXY_RESTART_BACKOFF);
            }
        })
        .map_err(|source| NetworkError::Io {
            operation: "spawn iroh daemon proxy".to_string(),
            detail: source.to_string(),
        })?;
    rx.recv().map_err(|source| NetworkError::Io {
        operation: "start iroh daemon proxy".to_string(),
        detail: source.to_string(),
    })?
}

#[cfg(not(feature = "iroh-transport"))]
pub fn start_iroh_daemon_proxy(
    _paths: ClusterPaths,
    _cluster_id: ClusterId,
    _node_id: NodeId,
    _daemon_address: String,
) -> Result<IrohEndpointRecord, NetworkError> {
    Err(NetworkError::Io {
        operation: "start iroh daemon proxy".to_string(),
        detail: "sinomo-network was built without iroh-transport".to_string(),
    })
}

#[cfg(feature = "iroh-transport")]
async fn run_iroh_daemon_proxy(
    paths: ClusterPaths,
    cluster_id: ClusterId,
    node_id: NodeId,
    daemon_address: String,
    secret_key: iroh::SecretKey,
    mut ready: Option<mpsc::Sender<Result<IrohEndpointRecord, NetworkError>>>,
) -> Result<(), NetworkError> {
    let builder = match sinomo_iroh_endpoint_builder() {
        Ok(builder) => builder,
        Err(error) => {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(error.clone()));
            }
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
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(error.clone()));
            }
            return Err(error);
        }
    };
    let _ = tokio::time::timeout(IROH_DAEMON_ONLINE_TIMEOUT, endpoint.online()).await;
    let record = iroh_endpoint_record_from_addr(&cluster_id, &node_id, endpoint.addr());
    if let Err(error) = write_iroh_endpoint_record(&paths, &record) {
        eprintln!("sinomo iroh daemon endpoint record write failed: {error}");
    }
    if let Some(ready) = ready.take() {
        let _ = ready.send(Ok(record));
    }

    let connection_limiter = Arc::new(tokio::sync::Semaphore::new(
        IROH_DAEMON_PROXY_MAX_CONNECTIONS,
    ));
    let command_limiter = Arc::new(tokio::sync::Semaphore::new(
        IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS,
    ));
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
        tokio::spawn(async move {
            let _connection_permit = connection_permit;
            let Ok(connection) = connecting.await else {
                return;
            };
            loop {
                let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                    break;
                };
                let Some(command_permit) = try_acquire_iroh_proxy_permit(
                    &command_limiter,
                    IROH_DAEMON_PROXY_MAX_COMMAND_STREAMS,
                    "command stream",
                ) else {
                    continue;
                };
                let daemon_address = daemon_address.clone();
                tokio::spawn(async move {
                    let _command_permit = command_permit;
                    let Ok(request) = recv.read_to_end(IROH_DAEMON_COMMAND_MAX_BYTES).await else {
                        return;
                    };
                    let Ok(response) = tokio::task::spawn_blocking(move || {
                        let timeout = iroh_daemon_command_forward_timeout(&request);
                        forward_daemon_control_payload(&daemon_address, &request, timeout)
                    })
                    .await
                    .unwrap_or_else(|source| {
                        Err(NetworkError::Io {
                            operation: "forward iroh daemon command".to_string(),
                            detail: source.to_string(),
                        })
                    }) else {
                        return;
                    };
                    if send.write_all(&response).await.is_ok() {
                        let _ = send.finish();
                    }
                });
            }
        });
    }
    Ok(())
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

pub fn iroh_endpoint_record_from_cluster_endpoint(
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

pub fn iroh_endpoint_record_from_address(
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
pub fn send_iroh_daemon_rpc_raw(
    address: &str,
    command: &str,
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
    timeout: Duration,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    let record = iroh_endpoint_record_from_address(
        address,
        &ClusterId::from_legacy_named("cluster_iroh_dial_placeholder").expect("static cluster id"),
        &NodeId::from_legacy_named("node_iroh_dial_placeholder").expect("static node id"),
    )?;
    block_on_daemon_rpc(async move {
        match tokio::time::timeout(
            timeout.saturating_add(Duration::from_millis(250)),
            send_iroh_daemon_rpc_raw_async(record, command, auth, request, timeout),
        )
        .await
        {
            Ok(response) => response,
            Err(_) => Err(iroh_daemon_command_timeout_error(timeout)),
        }
    })
}

#[cfg(not(feature = "iroh-transport"))]
pub fn send_iroh_daemon_rpc_raw(
    address: &str,
    _command: &str,
    _auth: Option<&[u8]>,
    _request: &crate::ObjectRpcRequest,
    _timeout: Duration,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    Err(NetworkError::Io {
        operation: "send iroh daemon rpc".to_string(),
        detail: format!("cannot dial {address}; sinomo-network was built without iroh-transport"),
    })
}

#[cfg(feature = "iroh-transport")]
async fn send_iroh_daemon_rpc_raw_async(
    record: IrohEndpointRecord,
    command: &str,
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
    timeout: Duration,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    let control_request = match auth {
        Some(auth) => {
            crate::DaemonControlRequest::authenticated_rpc(command, request.clone(), auth.to_vec())
        }
        None => crate::DaemonControlRequest::rpc(command, request.clone()),
    };
    let response =
        send_iroh_daemon_control_request_async(record, &control_request, timeout).await?;
    response.rpc.ok_or_else(|| NetworkError::Serialization {
        detail: "iroh daemon returned a non-rpc response".to_string(),
    })
}

#[cfg(feature = "iroh-transport")]
async fn send_iroh_daemon_control_request_async(
    record: IrohEndpointRecord,
    request: &crate::DaemonControlRequest,
    timeout: Duration,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    let remote_addr = iroh_endpoint_addr_from_record(&record)?;
    let started = std::time::Instant::now();
    let remaining = || {
        timeout
            .checked_sub(started.elapsed())
            .unwrap_or_else(|| Duration::from_millis(1))
    };
    let endpoint = match tokio::time::timeout(remaining(), async {
        sinomo_iroh_endpoint_builder()
            .map_err(|source| source.to_string())?
            .bind()
            .await
            .map_err(|source| source.to_string())
    })
    .await
    {
        Ok(Ok(endpoint)) => endpoint,
        Ok(Err(source)) => {
            return Err(NetworkError::Io {
                operation: "bind iroh client endpoint".to_string(),
                detail: source.to_string(),
            });
        }
        Err(_) => return Err(iroh_daemon_command_timeout_error(timeout)),
    };
    let online_timeout = remaining().min(Duration::from_millis(1000));
    let _ = tokio::time::timeout(online_timeout, endpoint.online()).await;
    let connection = match tokio::time::timeout(
        remaining(),
        endpoint.connect(remote_addr, IROH_ROAMING_PROTOCOL.as_bytes()),
    )
    .await
    {
        Ok(Ok(connection)) => connection,
        Ok(Err(source)) => {
            close_iroh_endpoint(&endpoint).await;
            return Err(NetworkError::Io {
                operation: "connect iroh endpoint".to_string(),
                detail: source.to_string(),
            });
        }
        Err(_) => {
            close_iroh_endpoint(&endpoint).await;
            return Err(iroh_daemon_command_timeout_error(timeout));
        }
    };
    let (mut send, mut recv) = match tokio::time::timeout(remaining(), connection.open_bi()).await {
        Ok(Ok(streams)) => streams,
        Ok(Err(source)) => {
            connection.close(0_u8.into(), b"open stream failed");
            close_iroh_endpoint(&endpoint).await;
            return Err(NetworkError::Io {
                operation: "open iroh daemon stream".to_string(),
                detail: source.to_string(),
            });
        }
        Err(_) => {
            connection.close(0_u8.into(), b"open stream timed out");
            close_iroh_endpoint(&endpoint).await;
            return Err(iroh_daemon_command_timeout_error(timeout));
        }
    };
    let mut request_bytes = Vec::new();
    crate::write_object_frame_blocking(&mut request_bytes, request)?;
    match tokio::time::timeout(remaining(), send.write_all(&request_bytes)).await {
        Ok(Ok(())) => {}
        Ok(Err(source)) => {
            connection.close(0_u8.into(), b"write failed");
            close_iroh_endpoint(&endpoint).await;
            return Err(NetworkError::Io {
                operation: "write iroh daemon command".to_string(),
                detail: source.to_string(),
            });
        }
        Err(_) => {
            connection.close(0_u8.into(), b"write timed out");
            close_iroh_endpoint(&endpoint).await;
            return Err(iroh_daemon_command_timeout_error(timeout));
        }
    }
    send.finish().map_err(|source| NetworkError::Io {
        operation: "finish iroh daemon command".to_string(),
        detail: source.to_string(),
    })?;
    let response =
        match tokio::time::timeout(remaining(), recv.read_to_end(IROH_DAEMON_COMMAND_MAX_BYTES))
            .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(source)) => {
                connection.close(0_u8.into(), b"read failed");
                close_iroh_endpoint(&endpoint).await;
                return Err(NetworkError::Io {
                    operation: "read iroh daemon response".to_string(),
                    detail: source.to_string(),
                });
            }
            Err(_) => {
                connection.close(0_u8.into(), b"read timed out");
                close_iroh_endpoint(&endpoint).await;
                return Err(iroh_daemon_command_timeout_error(timeout));
            }
        };
    connection.close(0_u8.into(), b"done");
    close_iroh_endpoint(&endpoint).await;
    let mut response = std::io::Cursor::new(response);
    let response: crate::DaemonControlResponse =
        crate::read_object_frame_blocking(&mut response, IROH_DAEMON_COMMAND_MAX_BYTES)?;
    Ok(response)
}

#[cfg(feature = "iroh-transport")]
fn iroh_daemon_command_timeout_error(timeout: Duration) -> NetworkError {
    NetworkError::Io {
        operation: "send iroh daemon command".to_string(),
        detail: format!("timed out after {}ms", timeout.as_millis()),
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

pub fn send_daemon_rpc_to_cluster_endpoint(
    endpoint: &ClusterEndpointRecord,
    command: &str,
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
    timeout: Duration,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    match endpoint.transport {
        ClusterTransportKind::Iroh => {
            send_iroh_daemon_rpc_raw(&endpoint.address, command, auth, request, timeout)
        }
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            send_daemon_rpc_raw(&endpoint.address, command, auth, request, timeout)
        }
        _ => Err(NetworkError::Io {
            operation: format!("send daemon rpc to {}", endpoint.address),
            detail: format!("unsupported daemon rpc transport {:?}", endpoint.transport),
        }),
    }
}

/// Drive a daemon-RPC future to completion from synchronous code, whether or not the
/// caller is already inside a Tokio runtime.
///
/// These helpers are synchronous wrappers over async I/O. They are called both with no
/// ambient runtime (e.g. the CLI) and from within one: the daemon's service-seam adapter
/// drives handlers via `block_on`, and a handler may then make a synchronous
/// daemon-to-daemon RPC. Building a fresh runtime and calling `block_on` inline panics
/// ("cannot start a runtime from within a runtime") when nested. So when an ambient
/// runtime exists we drive the future on it via `block_in_place` (the daemon adapter's
/// runtime is multi-threaded); otherwise we spin up a short-lived current-thread runtime.
fn block_on_daemon_rpc<F, T>(future: F) -> Result<T, NetworkError>
where
    F: std::future::Future<Output = Result<T, NetworkError>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return tokio::task::block_in_place(|| handle.block_on(future));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| NetworkError::Io {
            operation: "start daemon control runtime".to_string(),
            detail: source.to_string(),
        })?;
    let output = runtime.block_on(future);
    runtime.shutdown_timeout(Duration::from_millis(250));
    output
}

pub fn send_daemon_rpc_raw(
    address: &str,
    command: &str,
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
    timeout: Duration,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    block_on_daemon_rpc(send_daemon_rpc_raw_async(
        address, command, auth, request, timeout,
    ))
}

pub async fn send_daemon_rpc_raw_async(
    address: &str,
    command: &str,
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
    timeout: Duration,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    let control_request = match auth {
        Some(auth) => {
            crate::DaemonControlRequest::authenticated_rpc(command, request.clone(), auth.to_vec())
        }
        None => crate::DaemonControlRequest::rpc(command, request.clone()),
    };
    let response = send_daemon_control_request_async(address, &control_request, timeout).await?;
    let rpc = response.rpc.ok_or_else(|| NetworkError::Serialization {
        detail: format!("daemon control address {address} returned a non-rpc response"),
    })?;
    rpc.error_detail()?;
    Ok(rpc)
}

async fn send_daemon_control_request_async(
    address: &str,
    request: &crate::DaemonControlRequest,
    timeout: Duration,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    let mut stream = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(address))
        .await
        .map_err(|_| NetworkError::Io {
            operation: format!("connect daemon control address {address}"),
            detail: format!("timed out after {}ms", timeout.as_millis()),
        })?
        .map_err(|source| NetworkError::Io {
            operation: format!("connect daemon control address {address}"),
            detail: source.to_string(),
        })?;
    tokio::time::timeout(
        timeout,
        crate::write_object_frame_async(&mut stream, request),
    )
    .await
    .map_err(|_| NetworkError::Io {
        operation: format!("write daemon control address {address}"),
        detail: format!("timed out after {}ms", timeout.as_millis()),
    })??;
    tokio::io::AsyncWriteExt::shutdown(&mut stream)
        .await
        .map_err(|source| NetworkError::Io {
            operation: format!("finish daemon control address {address}"),
            detail: source.to_string(),
        })?;
    let response: crate::DaemonControlResponse = tokio::time::timeout(
        timeout,
        crate::read_object_frame_async(&mut stream, IROH_DAEMON_COMMAND_MAX_BYTES),
    )
    .await
    .map_err(|_| NetworkError::Io {
        operation: format!("read daemon control address {address}"),
        detail: format!("timed out after {}ms", timeout.as_millis()),
    })?
    .map_err(|source| NetworkError::Io {
        operation: format!("read daemon control address {address}"),
        detail: source.to_string(),
    })?;
    Ok(response)
}

#[cfg(feature = "iroh-transport")]
fn forward_daemon_control_payload(
    address: &str,
    payload: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, NetworkError> {
    let socket_address = address
        .to_socket_addrs()
        .map_err(|source| NetworkError::Io {
            operation: format!("resolve daemon control address {address}"),
            detail: source.to_string(),
        })?
        .next()
        .ok_or_else(|| NetworkError::Io {
            operation: format!("resolve daemon control address {address}"),
            detail: "no socket addresses resolved".to_string(),
        })?;
    let mut stream = TcpStream::connect_timeout(&socket_address, timeout).map_err(|source| {
        NetworkError::Io {
            operation: format!("connect daemon control address {address}"),
            detail: source.to_string(),
        }
    })?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|source| NetworkError::Io {
            operation: format!("configure daemon control address {address}"),
            detail: source.to_string(),
        })?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|source| NetworkError::Io {
            operation: format!("configure daemon control address {address}"),
            detail: source.to_string(),
        })?;
    stream
        .write_all(payload)
        .map_err(|source| NetworkError::Io {
            operation: format!("write daemon control address {address}"),
            detail: source.to_string(),
        })?;
    let _ = stream.shutdown(Shutdown::Write);
    let response: crate::DaemonControlResponse =
        crate::read_object_frame_blocking(&mut stream, IROH_DAEMON_COMMAND_MAX_BYTES).map_err(
            |source| NetworkError::Io {
                operation: format!("read daemon control address {address}"),
                detail: source.to_string(),
            },
        )?;
    let mut response_bytes = Vec::new();
    crate::write_object_frame_blocking(&mut response_bytes, &response)?;
    Ok(response_bytes)
}

#[cfg(any(test, feature = "iroh-transport"))]
pub(crate) fn iroh_daemon_command_forward_timeout(payload: &[u8]) -> Duration {
    let mut cursor = std::io::Cursor::new(payload);
    let request: Result<crate::DaemonControlRequest, _> =
        crate::read_object_frame_blocking(&mut cursor, IROH_DAEMON_COMMAND_MAX_BYTES);
    match request {
        Ok(request)
            if request.rpc.as_ref().is_some_and(|rpc| {
                matches!(
                    rpc.route.as_str(),
                    crate::ROLLOUT_BUILD_ROUTE
                        | crate::ROLLOUT_BINARY_ROUTE
                        | crate::ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE
                        | crate::ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE
                )
            }) || matches!(
                request.command.strip_prefix("RPC "),
                Some(
                    crate::ROLLOUT_BUILD_ROUTE
                        | crate::ROLLOUT_BINARY_ROUTE
                        | crate::ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE
                        | crate::ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE
                )
            ) =>
        {
            Duration::from_secs(600)
        }
        _ => Duration::from_secs(10),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterAuthMaterial {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub node_public_key: String,
    pub certificate: ClusterCertificate,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterAuthVerifier {
    members: BTreeMap<NodeId, ClusterMemberRecord>,
}

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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AuthenticatedClusterSession {
    pub cluster_id: ClusterId,
    pub local_node_id: NodeId,
    pub remote_node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub established_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default)]
pub struct LanConnectionPlanner;

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
                )
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RoamingPathSelection {
    Direct(PeerEndpointCandidate),
    Iroh(PeerEndpointCandidate),
    WebRtc(PeerEndpointCandidate),
    Relay(PeerEndpointCandidate),
}

#[derive(Clone, Debug, Default)]
pub struct RoamingConnectionPlanner;

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
