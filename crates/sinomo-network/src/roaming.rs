//! Roaming transport support and registry-aware daemon RPC dispatch.
//!
//! Built-in loopback/LAN-TCP paths stay in this crate. Additional transports
//! are loaded at runtime through the network-module registry and are dialed by
//! transport kind or address scheme.

use crate::error::NetworkError;
use crate::object::DaemonRpcTimeouts;
#[cfg(any(test, feature = "test-support"))]
use crate::peer::{EndpointCandidateStatus, PeerEndpointCandidate};
use crate::prelude::*;
use crate::{NetworkDuplexStream, NetworkHealthPolicy};
#[cfg(any(test, feature = "test-support"))]
use sinomo_kv::KvEntry;
#[cfg(any(test, feature = "test-support"))]
use sinomo_kv::{KvOp, LeaseId};

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

pub async fn send_daemon_rpc_to_cluster_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    let response =
        send_daemon_rpc_response_to_cluster_endpoint_async(endpoint, auth, request, timeouts)
            .await?;
    response.error_detail()?;
    Ok(response)
}

/// Opens the transport-neutral byte stream used by the canonical object-RPC
/// session engine. Endpoint selection and retry stay in `route`; transports do
/// not see object heads or transfer frames.
pub(crate) async fn open_daemon_rpc_stream_to_cluster_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    health: NetworkHealthPolicy,
) -> Result<Box<dyn NetworkDuplexStream>, NetworkError> {
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            if endpoint
                .address
                .starts_with(crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX)
            {
                return Err(NetworkError::Io {
                    operation: "open object RPC session stream".to_string(),
                    detail: "legacy multiplexed TCP-tunnel endpoints do not expose a canonical byte stream"
                        .to_string(),
                });
            }
            let stream = crate::AsyncDaemonControlStream::connect_with_options(
                &endpoint.address,
                health.into(),
            )
            .await?;
            Ok(Box::new(stream.into_inner()))
        }
        ref transport => {
            let module = crate::network_module::registered_network_module_for_transport(transport)
                .ok_or_else(|| NetworkError::Io {
                    operation: format!("open object RPC session to {}", endpoint.address),
                    detail: format!(
                        "unsupported object RPC session transport {:?}",
                        endpoint.transport
                    ),
                })?;
            module.open_rpc_stream(endpoint, health).await
        }
    }
}

/// Send a daemon RPC while preserving a received application error frame.
///
/// Routed delivery uses this form so a valid peer response terminates transport
/// retry. Public endpoint callers retain the historical behavior of decoding
/// application errors into `NetworkError`.
pub async fn send_daemon_rpc_response_to_cluster_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    auth: Option<&[u8]>,
    request: &crate::ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    let control_request = daemon_rpc_control_request(auth, request);
    send_daemon_control_request_to_cluster_endpoint_async(endpoint, &control_request, timeouts)
        .await
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
/// typed request is framed, the module returns the encoded response frame, and
/// the frame is decoded back into the typed response.
async fn dial_daemon_control_request_via_network_module(
    module: &dyn crate::network_module::NetworkModule,
    endpoint: &ClusterEndpointRecord,
    request: &crate::DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    let frame = crate::object::encode_object_frame_to_vec(request)?;
    let response = module.dial_rpc(endpoint, &frame, timeouts).await?;
    crate::object::decode_single_object_frame_from_slice(
        &response,
        crate::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
    )
}

/// Synthesizes the endpoint record handed to a network module when dispatch
/// starts from a bare address string (scheme sniff) instead of a cluster
/// endpoint record.
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

async fn send_daemon_control_request_to_address_async(
    address: &str,
    request: &crate::DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<crate::DaemonControlResponse, NetworkError> {
    if let Some(scheme) = crate::network_module::endpoint_address_scheme(address) {
        if let Some(module) =
            crate::network_module::registered_network_module_for_address_scheme(scheme)
        {
            let transport = module
                .descriptor()
                .transports
                .first()
                .cloned()
                .unwrap_or(ClusterTransportKind::HttpsRelay);
            let endpoint = cluster_endpoint_record_for_module_address_dial(address, transport);
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
            detail: format!("unsupported daemon rpc address scheme `{scheme}`"),
        });
    }
    crate::send_daemon_control_request_async(
        address,
        request,
        timeouts,
        crate::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
    )
    .await
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
    Module(PeerEndpointCandidate),
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
                !matches!(
                    candidate.endpoint.transport,
                    ClusterTransportKind::InProcess
                        | ClusterTransportKind::Loopback
                        | ClusterTransportKind::LanTcp
                        | ClusterTransportKind::LanQuic
                        | ClusterTransportKind::WebRtc
                        | ClusterTransportKind::TurnRelay
                        | ClusterTransportKind::HttpsRelay
                ) && candidate.status != EndpointCandidateStatus::Failed
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
            return Some(RoamingPathSelection::Module(candidate.clone()));
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
