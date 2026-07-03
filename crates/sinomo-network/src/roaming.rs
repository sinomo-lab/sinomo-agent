//! Roaming transport support: the transport-neutral iroh pieces this crate
//! keeps (the `IrohEndpointRecord` data type, `iroh://` parse/format, stable
//! endpoint ids, record file I/O) plus registry-aware daemon RPC dispatch.
//!
//! The iroh-SDK-backed implementation (endpoint builder, secret keys, proxy
//! supervisor, dial client) is module-only: it lives in the
//! `modules/network/iroh` dylib (stage C of
//! docs/plans/r0-r1-crate-boundary-refactor.md). Dispatch reaches it through
//! the process-wide network-module registry; iroh endpoints and `iroh://`
//! addresses that no registered module claims are un-dialable.

use crate::ClusterPaths;
use crate::error::NetworkError;
use crate::object::DaemonRpcTimeouts;
use crate::object_file_store::{read_object_file, write_object_file};
#[cfg(any(test, feature = "test-support"))]
use crate::peer::{EndpointCandidateStatus, PeerEndpointCandidate};
use crate::prelude::*;
#[cfg(any(test, feature = "test-support"))]
use sinomo_kv::KvEntry;
#[cfg(any(test, feature = "test-support"))]
use sinomo_kv::{KvOp, LeaseId};

pub const IROH_DAEMON_COMMAND_MAX_BYTES: usize = crate::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES;

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WebRtcSignalMessage {
    Offer { sdp: String },
    Answer { sdp: String },
    IceCandidate(IceCandidateRecord),
    EndOfCandidates,
}

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum IceCandidateType {
    Host,
    ServerReflexive,
    PeerReflexive,
    Relay,
}

#[cfg(any(test, feature = "test-support"))]
pub struct WebRtcKvExchange;

#[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
    pub fn put_signal(
        envelope: WebRtcSignalingEnvelope,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOp, NetworkError> {
        let key = Self::signal_key(
            &envelope.cluster_id,
            &envelope.exchange_id,
            envelope.sequence,
        );
        KvOp::put_serde(key, &envelope, lease_id).map_err(NetworkError::directory)
    }

    #[cfg(any(test, feature = "test-support"))]
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
        KvOp::put_serde(key, &candidate, lease_id).map_err(NetworkError::directory)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn signal_from_entry(entry: &KvEntry) -> Result<WebRtcSignalingEnvelope, NetworkError> {
        entry.decode().map_err(NetworkError::directory)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn candidate_from_entry(entry: &KvEntry) -> Result<IceCandidateRecord, NetworkError> {
        entry.decode().map_err(NetworkError::directory)
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

#[cfg(any(test, feature = "test-support"))]
pub struct IrohKvExchange;

#[cfg(any(test, feature = "test-support"))]
impl IrohKvExchange {
    pub fn endpoint_key(cluster_id: &ClusterId, node_id: &NodeId) -> String {
        format!("/iroh/endpoints/{cluster_id}/{node_id}")
    }

    pub fn endpoint_prefix(cluster_id: &ClusterId) -> String {
        format!("/iroh/endpoints/{cluster_id}/")
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn put_endpoint(
        endpoint: IrohEndpointRecord,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOp, NetworkError> {
        let key = Self::endpoint_key(&endpoint.cluster_id, &endpoint.node_id);
        KvOp::put_serde(key, &endpoint, lease_id).map_err(NetworkError::directory)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn endpoint_from_entry(entry: &KvEntry) -> Result<IrohEndpointRecord, NetworkError> {
        entry.decode().map_err(NetworkError::directory)
    }
}

fn iroh_endpoint_record_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    paths
        .iroh
        .join(format!("{}.endpoint.sobj", node_id.as_str()))
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

pub(crate) fn read_iroh_endpoint_record(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<IrohEndpointRecord, NetworkError> {
    let path = iroh_endpoint_record_path(paths, node_id);
    read_object_file(&path)
}

pub async fn read_iroh_endpoint_record_async(
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

#[cfg(any(test, feature = "test-support"))]
pub async fn write_iroh_endpoint_record_async(
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

#[cfg(any(test, feature = "test-support"))]
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

/// Parses an `iroh://<public-key>?direct=...&relay=...` address into an
/// [`IrohEndpointRecord`]. Unconditionally public since stage C: the
/// `iroh://` parse/format contract stays in this crate while dylib network
/// modules reuse it through the sinomo-core facade.
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

pub async fn send_daemon_rpc_to_cluster_endpoint_async(
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
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            send_daemon_control_request_to_address_async(&endpoint.address, request, timeouts).await
        }
        ref transport => {
            // Registry-aware dispatch (stage C): transports without a
            // built-in path — iroh included, its dialer lives in the
            // modules/network/iroh dylib — dial through a registered module
            // when one claims them; otherwise keep the historical
            // unsupported-transport error.
            if let Some(module) =
                crate::network_module::registered_network_module_for_transport(transport)
            {
                return dial_daemon_control_request_via_network_module(
                    module.as_ref(),
                    endpoint,
                    request,
                    timeouts,
                )
                .await;
            }
            Err(NetworkError::Io {
                operation: format!("send daemon rpc to {}", endpoint.address),
                detail: format!("unsupported daemon rpc transport {:?}", endpoint.transport),
            })
        }
    }
}

/// Dials one daemon control request through a runtime network module: the
/// typed request is framed, the module returns the raw response frame, and
/// the frame is decoded back into the typed response.
async fn dial_daemon_control_request_via_network_module(
    module: &dyn crate::network_module::NetworkModule,
    endpoint: &ClusterEndpointRecord,
    request: &crate::DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    let frame = crate::object::encode_object_frame_to_vec(request)?;
    let response = module.dial_rpc(endpoint, &frame, timeouts).await?;
    crate::object::decode_single_object_frame_from_slice(&response, IROH_DAEMON_COMMAND_MAX_BYTES)
}

/// Synthesizes the endpoint record handed to a network module when dispatch
/// starts from a bare address string (scheme sniff) instead of a cluster
/// endpoint record. Mirrors the placeholder identities the (since-removed)
/// built-in iroh address dial historically used.
fn cluster_endpoint_record_for_module_address_dial(
    address: &str,
    transport: ClusterTransportKind,
) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::derived("sinomo.network-module.dial", address),
        node_id: NodeId::from_legacy_named("node_network_module_dial_placeholder")
            .expect("static node id"),
        transport,
        address: address.to_string(),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::InternetDirect,
        priority: 0,
        last_observed_latency_ms: None,
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
        ClusterTransportKind::Iroh => {
            // Registry-aware dispatch (stage C): raw/streamed responses only
            // route through a module that declares `supports_raw_response`
            // (the iroh module does not — its proxy re-frames responses).
            if let Some(module) = crate::network_module::registered_network_module_for_transport(
                &ClusterTransportKind::Iroh,
            ) && module.descriptor().supports_raw_response
            {
                // DaemonControlResponse == ObjectRpcResponse, so the module
                // frame decode is identical to the non-raw path.
                return dial_daemon_control_request_via_network_module(
                    module.as_ref(),
                    endpoint,
                    request,
                    timeouts,
                )
                .await;
            }
            Err(NetworkError::Io {
                operation: format!("send streamed daemon rpc to {}", endpoint.address),
                detail: "streamed raw RPC responses are not supported over iroh proxy transport"
                    .to_string(),
            })
        }
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
        // Registry-aware dispatch (stage C): `iroh://` strings can ride on
        // LanTcp-tagged endpoint records, so this scheme sniff — not only the
        // transport-kind arm above — must consult the registry. Iroh dialing
        // is module-only; without a registered module claiming the scheme the
        // address is un-dialable.
        if let Some(module) =
            crate::network_module::registered_network_module_for_address_scheme("iroh")
        {
            let endpoint = cluster_endpoint_record_for_module_address_dial(
                address,
                ClusterTransportKind::Iroh,
            );
            return dial_daemon_control_request_via_network_module(
                module.as_ref(),
                &endpoint,
                request,
                timeouts,
            )
            .await;
        }
        return Err(NetworkError::Io {
            operation: format!("send daemon rpc to {address}"),
            detail: format!(
                "unsupported daemon rpc transport {:?}",
                ClusterTransportKind::Iroh
            ),
        });
    }
    crate::send_daemon_control_request_async(
        address,
        request,
        timeouts,
        IROH_DAEMON_COMMAND_MAX_BYTES,
    )
    .await
}

fn daemon_rpc_address_uses_iroh(address: &str) -> bool {
    address.trim_start().starts_with("iroh://")
}

#[cfg(any(test, feature = "test-support"))]
pub fn configure_daemon_control_stream_idle_timeout(
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

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterAuthMaterial {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub node_public_key: String,
    pub certificate: ClusterCertificate,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Default)]
pub struct ClusterAuthVerifier {
    members: BTreeMap<NodeId, ClusterMemberRecord>,
}

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AuthenticatedClusterSession {
    pub cluster_id: ClusterId,
    pub local_node_id: NodeId,
    pub remote_node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub established_at: OffsetDateTime,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Default)]
pub struct LanConnectionPlanner;

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RoamingPathSelection {
    Direct(PeerEndpointCandidate),
    Iroh(PeerEndpointCandidate),
    WebRtc(PeerEndpointCandidate),
    Relay(PeerEndpointCandidate),
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Default)]
pub struct RoamingConnectionPlanner;

#[cfg(any(test, feature = "test-support"))]
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
