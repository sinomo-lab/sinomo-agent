//! Peer registry, mesh contracts, and event routing skeletons.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterCertificate, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource,
    ClusterId, ClusterMemberRecord, ClusterTransportKind, EventEnvelope, EventId, LatencyClass,
    NodeId, RoutingScope, SessionId,
};
use sinomo_events::{EventSequence, EventStore, EventStoreError, StoredEvent};
use sinomo_kv::{KvEntry, KvOp, LeaseId};
use sinomo_node::{NodeAvailability, NodeHealth, NodeProfile};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::rc::Rc;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Default)]
pub struct PeerRegistry {
    cluster_id: Option<ClusterId>,
    nodes: BTreeMap<NodeId, NodeProfile>,
    cluster_members: BTreeMap<NodeId, ClusterMemberRecord>,
    peer_states: BTreeMap<NodeId, PeerState>,
    endpoint_candidates: BTreeMap<NodeId, Vec<PeerEndpointCandidate>>,
    sessions: BTreeMap<SessionId, BTreeSet<NodeId>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cluster_id(&self) -> Option<&ClusterId> {
        self.cluster_id.as_ref()
    }

    pub fn set_cluster_id(&mut self, cluster_id: ClusterId) {
        self.cluster_id = Some(cluster_id);
    }

    pub fn register_node(&mut self, profile: NodeProfile) -> Option<NodeProfile> {
        let node_id = profile.node_id.clone();
        self.peer_states
            .entry(node_id.clone())
            .or_insert_with(|| PeerState::new(NodeEndpoint::in_process(node_id.clone())));
        self.nodes.insert(node_id, profile)
    }

    pub fn upsert_cluster_member(
        &mut self,
        member: ClusterMemberRecord,
    ) -> Option<ClusterMemberRecord> {
        self.cluster_id
            .get_or_insert_with(|| member.cluster_id.clone());
        let node_id = member.node_id.clone();
        if !member.endpoints.is_empty() {
            self.endpoint_candidates.insert(
                node_id.clone(),
                member
                    .endpoints
                    .iter()
                    .cloned()
                    .map(PeerEndpointCandidate::discovered)
                    .collect(),
            );
        }
        self.cluster_members.insert(node_id, member)
    }

    pub fn cluster_member(&self, node_id: &NodeId) -> Option<&ClusterMemberRecord> {
        self.cluster_members.get(node_id)
    }

    pub fn cluster_members(&self) -> Vec<&ClusterMemberRecord> {
        self.cluster_members.values().collect()
    }

    pub fn replace_endpoint_candidates(
        &mut self,
        node_id: NodeId,
        candidates: Vec<PeerEndpointCandidate>,
    ) {
        self.endpoint_candidates.insert(node_id, candidates);
    }

    pub fn endpoint_candidates(&self, node_id: &NodeId) -> &[PeerEndpointCandidate] {
        self.endpoint_candidates
            .get(node_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn best_endpoint_candidate(&self, node_id: &NodeId) -> Option<&PeerEndpointCandidate> {
        self.endpoint_candidates
            .get(node_id)
            .and_then(|candidates| candidates.iter().min_by_key(|candidate| candidate.score))
    }

    pub fn join_session(
        &mut self,
        session_id: SessionId,
        node_id: NodeId,
    ) -> Result<(), NetworkError> {
        if !self.nodes.contains_key(&node_id) {
            return Err(NetworkError::UnknownNode { node_id });
        }

        self.sessions.entry(session_id).or_default().insert(node_id);
        Ok(())
    }

    pub fn node(&self, node_id: &NodeId) -> Option<&NodeProfile> {
        self.nodes.get(node_id)
    }

    pub fn peer_state(&self, node_id: &NodeId) -> Option<&PeerState> {
        self.peer_states.get(node_id)
    }

    pub fn is_routable(&self, node_id: &NodeId) -> bool {
        self.peer_states
            .get(node_id)
            .map(|state| state.status.can_route_reactive())
            .unwrap_or(true)
    }

    pub fn record_heartbeat(
        &mut self,
        node_id: &NodeId,
        tick: u64,
    ) -> Result<PeerStatus, NetworkError> {
        let Some(profile) = self.nodes.get_mut(node_id) else {
            return Err(NetworkError::UnknownNode {
                node_id: node_id.clone(),
            });
        };
        let state = self
            .peer_states
            .entry(node_id.clone())
            .or_insert_with(|| PeerState::new(NodeEndpoint::in_process(node_id.clone())));

        state.status = PeerStatus::Healthy;
        state.link.last_seen_tick = Some(tick);
        state.link.missed_heartbeats = 0;
        profile.set_health(NodeHealth::available());
        Ok(state.status.clone())
    }

    pub fn record_missed_heartbeat(
        &mut self,
        node_id: &NodeId,
        policy: HeartbeatPolicy,
    ) -> Result<PeerStatus, NetworkError> {
        let Some(profile) = self.nodes.get_mut(node_id) else {
            return Err(NetworkError::UnknownNode {
                node_id: node_id.clone(),
            });
        };
        let state = self
            .peer_states
            .entry(node_id.clone())
            .or_insert_with(|| PeerState::new(NodeEndpoint::in_process(node_id.clone())));

        state.link.missed_heartbeats = state.link.missed_heartbeats.saturating_add(1);
        state.status = if state.link.missed_heartbeats >= policy.offline_after_missed {
            profile.set_health(NodeHealth {
                availability: NodeAvailability::Offline,
                message: Some("network heartbeat missed offline threshold".to_string()),
            });
            PeerStatus::Offline
        } else if state.link.missed_heartbeats >= policy.suspect_after_missed {
            profile.set_health(NodeHealth::degraded(
                "network heartbeat missed suspect threshold",
            ));
            PeerStatus::Suspect
        } else {
            profile.set_health(NodeHealth::degraded("network heartbeat missed"));
            PeerStatus::Degraded
        };

        Ok(state.status.clone())
    }

    pub fn session_nodes(&self, session_id: &SessionId) -> Vec<NodeId> {
        self.sessions
            .get(session_id)
            .map(|nodes| nodes.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn routable_session_nodes(&self, session_id: &SessionId) -> Vec<NodeId> {
        self.sessions
            .get(session_id)
            .map(|nodes| {
                nodes
                    .iter()
                    .filter(|node_id| self.is_routable(node_id))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn routable_network_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .keys()
            .filter(|node_id| self.is_routable(node_id))
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct InProcessRouter {
    registry: PeerRegistry,
    next_delivery_id: u64,
}

impl InProcessRouter {
    pub fn new(registry: PeerRegistry) -> Self {
        Self {
            registry,
            next_delivery_id: 1,
        }
    }

    pub fn registry(&self) -> &PeerRegistry {
        &self.registry
    }

    pub fn registry_mut(&mut self) -> &mut PeerRegistry {
        &mut self.registry
    }

    pub fn route_event(
        &mut self,
        stored_event: &StoredEvent,
    ) -> Result<Vec<RouteDelivery>, NetworkError> {
        let targets = self.resolve_targets(&stored_event.event)?;
        let mut deliveries = Vec::with_capacity(targets.len());

        for target_node_id in targets {
            let delivery_id = RouteDeliveryId(self.next_delivery_id);
            self.next_delivery_id += 1;
            deliveries.push(RouteDelivery {
                delivery_id,
                event_sequence: stored_event.sequence,
                event_id: stored_event.event.event_id.clone(),
                target_node_id,
                latency_class: stored_event.event.routing.latency_class.clone(),
                status: DeliveryStatus::Queued,
            });
        }

        Ok(deliveries)
    }

    fn resolve_targets(&self, event: &EventEnvelope) -> Result<Vec<NodeId>, NetworkError> {
        if let Some(target_node_id) = &event.target_node_id {
            if self.registry.node(target_node_id).is_none() {
                return Err(NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                });
            }
            return Ok(vec![target_node_id.clone()]);
        }

        match event.routing.scope {
            RoutingScope::Local => Ok(vec![event.origin_node_id.clone()]),
            RoutingScope::Session => {
                if let Some(session_id) = &event.session_id {
                    let nodes = self.registry.routable_session_nodes(session_id);
                    if nodes.is_empty() {
                        Ok(vec![event.origin_node_id.clone()])
                    } else {
                        Ok(nodes)
                    }
                } else {
                    Ok(vec![event.origin_node_id.clone()])
                }
            }
            RoutingScope::TrustedNetwork | RoutingScope::DurableGlobal => {
                Ok(self.registry.routable_network_nodes())
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NodeEndpoint {
    pub node_id: NodeId,
    pub transport: TransportKind,
    pub address: String,
}

impl NodeEndpoint {
    pub fn in_process(node_id: NodeId) -> Self {
        Self {
            address: format!("in-process://{node_id}"),
            node_id,
            transport: TransportKind::InProcess,
        }
    }

    pub fn from_cluster_endpoint(endpoint: &ClusterEndpointRecord) -> Self {
        Self {
            node_id: endpoint.node_id.clone(),
            transport: TransportKind::from_cluster_transport(&endpoint.transport),
            address: endpoint.address.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TransportKind {
    InProcess,
    Loopback,
    Tcp,
    Quic,
    Iroh,
    WebRtc,
    TurnRelay,
    HttpsRelay,
}

impl TransportKind {
    pub fn from_cluster_transport(transport: &sinomo_core::ClusterTransportKind) -> Self {
        match transport {
            sinomo_core::ClusterTransportKind::InProcess => Self::InProcess,
            sinomo_core::ClusterTransportKind::Loopback => Self::Loopback,
            sinomo_core::ClusterTransportKind::LanTcp => Self::Tcp,
            sinomo_core::ClusterTransportKind::LanQuic => Self::Quic,
            sinomo_core::ClusterTransportKind::Iroh => Self::Iroh,
            sinomo_core::ClusterTransportKind::WebRtc => Self::WebRtc,
            sinomo_core::ClusterTransportKind::TurnRelay => Self::TurnRelay,
            sinomo_core::ClusterTransportKind::HttpsRelay => Self::HttpsRelay,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PeerEndpointCandidate {
    pub endpoint: ClusterEndpointRecord,
    pub status: EndpointCandidateStatus,
    pub score: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

impl PeerEndpointCandidate {
    pub fn discovered(endpoint: ClusterEndpointRecord) -> Self {
        let score = endpoint.priority;
        Self {
            endpoint,
            status: EndpointCandidateStatus::Discovered,
            score,
            failure_reason: None,
        }
    }

    pub fn healthy(mut self, observed_latency_ms: u32) -> Self {
        self.endpoint.last_observed_latency_ms = Some(observed_latency_ms);
        self.status = EndpointCandidateStatus::Healthy;
        self.score = self
            .endpoint
            .priority
            .saturating_add((observed_latency_ms / 10).min(u32::from(u16::MAX)) as u16);
        self
    }

    pub fn failed(mut self, reason: impl Into<String>) -> Self {
        self.status = EndpointCandidateStatus::Failed;
        self.failure_reason = Some(reason.into());
        self.score = u16::MAX;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EndpointCandidateStatus {
    Discovered,
    Probing,
    Healthy,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanQuicEndpointConfig {
    pub bind_address: String,
    pub alpn: String,
    pub max_frame_bytes: usize,
}

impl Default for LanQuicEndpointConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:21355".to_string(),
            alpn: "sinomo-cluster-v1".to_string(),
            max_frame_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LanQuicFrameCodec {
    pub max_frame_bytes: usize,
}

impl Default for LanQuicFrameCodec {
    fn default() -> Self {
        Self {
            max_frame_bytes: LanQuicEndpointConfig::default().max_frame_bytes,
        }
    }
}

impl LanQuicFrameCodec {
    pub fn encode(&self, frame: &NetworkFrame) -> Result<Vec<u8>, NetworkError> {
        let bytes = serde_json::to_vec(frame).map_err(|source| NetworkError::Serialization {
            detail: source.to_string(),
        })?;
        if bytes.len() > self.max_frame_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: bytes.len(),
                max_bytes: self.max_frame_bytes,
            });
        }
        Ok(bytes)
    }

    pub fn decode(&self, bytes: &[u8]) -> Result<NetworkFrame, NetworkError> {
        if bytes.len() > self.max_frame_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: bytes.len(),
                max_bytes: self.max_frame_bytes,
            });
        }
        serde_json::from_slice(bytes).map_err(|source| NetworkError::Serialization {
            detail: source.to_string(),
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct MdnsDiscoveryCache {
    records: BTreeMap<String, MdnsDiscoveryRecord>,
}

impl MdnsDiscoveryCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&mut self, record: MdnsDiscoveryRecord) {
        self.records.insert(record.record_id.clone(), record);
    }

    pub fn expire_after_tick(&mut self, tick: u64) -> usize {
        let before = self.records.len();
        self.records
            .retain(|_, record| record.expires_after_tick > tick);
        before.saturating_sub(self.records.len())
    }

    pub fn endpoint_candidates_for_node(&self, node_id: &NodeId) -> Vec<PeerEndpointCandidate> {
        self.records
            .values()
            .filter(|record| &record.endpoint.node_id == node_id)
            .map(|record| PeerEndpointCandidate::discovered(record.endpoint.clone()))
            .collect()
    }

    pub fn endpoints_for_cluster(&self, cluster_id: &ClusterId) -> Vec<ClusterEndpointRecord> {
        self.records
            .values()
            .filter(|record| &record.cluster_id == cluster_id)
            .map(|record| record.endpoint.clone())
            .collect()
    }
}

pub const SINOMO_LAN_DISCOVERY_PROTOCOL: &str = "sinomo.lan-discovery.v1";
pub const SINOMO_LAN_DISCOVERY_MULTICAST: Ipv4Addr = Ipv4Addr::new(239, 255, 77, 77);
pub const SINOMO_LAN_DISCOVERY_PORT: u16 = 21_359;
pub const SINOMO_LAN_DISCOVERY_TTL_TICKS: u64 = 10;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MdnsDiscoveryAdvertisement {
    pub protocol: String,
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<ClusterEndpointRecord>,
    pub advertised_tick: u64,
    pub ttl_ticks: u64,
}

impl MdnsDiscoveryAdvertisement {
    pub fn new(
        cluster_id: ClusterId,
        node_id: NodeId,
        endpoints: Vec<ClusterEndpointRecord>,
        advertised_tick: u64,
    ) -> Self {
        Self {
            protocol: SINOMO_LAN_DISCOVERY_PROTOCOL.to_string(),
            cluster_id,
            node_id,
            endpoints,
            advertised_tick,
            ttl_ticks: SINOMO_LAN_DISCOVERY_TTL_TICKS,
        }
    }

    pub fn records(&self) -> Vec<MdnsDiscoveryRecord> {
        self.endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.node_id == self.node_id
                    && matches!(
                        endpoint.transport,
                        ClusterTransportKind::LanTcp | ClusterTransportKind::LanQuic
                    )
                    && endpoint.privacy == ClusterEndpointPrivacy::Lan
            })
            .map(|endpoint| {
                let mut endpoint = endpoint.clone();
                endpoint.source = ClusterEndpointSource::Mdns;
                MdnsDiscoveryRecord {
                    record_id: format!(
                        "{}:{}:{}:{}",
                        self.cluster_id,
                        self.node_id,
                        cluster_transport_kind_label(&endpoint.transport),
                        endpoint.address
                    ),
                    cluster_id: self.cluster_id.clone(),
                    endpoint,
                    advertised_tick: self.advertised_tick,
                    expires_after_tick: self.advertised_tick.saturating_add(self.ttl_ticks),
                }
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct MdnsDiscoverySocket {
    socket: UdpSocket,
    multicast_address: SocketAddrV4,
}

impl MdnsDiscoverySocket {
    pub fn bind_default() -> Result<Self, NetworkError> {
        Self::bind_port(SINOMO_LAN_DISCOVERY_PORT)
    }

    pub fn bind_port(port: u16) -> Result<Self, NetworkError> {
        let socket =
            UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).map_err(|source| {
                NetworkError::Io {
                    operation: "bind lan discovery socket".to_string(),
                    detail: source.to_string(),
                }
            })?;
        socket
            .set_nonblocking(true)
            .map_err(|source| NetworkError::Io {
                operation: "set lan discovery nonblocking".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .set_multicast_loop_v4(true)
            .map_err(|source| NetworkError::Io {
                operation: "enable lan discovery multicast loopback".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .set_multicast_ttl_v4(16)
            .map_err(|source| NetworkError::Io {
                operation: "set lan discovery multicast ttl".to_string(),
                detail: source.to_string(),
            })?;
        socket
            .join_multicast_v4(&SINOMO_LAN_DISCOVERY_MULTICAST, &Ipv4Addr::UNSPECIFIED)
            .map_err(|source| NetworkError::Io {
                operation: "join lan discovery multicast group".to_string(),
                detail: source.to_string(),
            })?;

        Ok(Self {
            socket,
            multicast_address: SocketAddrV4::new(SINOMO_LAN_DISCOVERY_MULTICAST, port),
        })
    }

    pub fn announce(
        &self,
        advertisement: &MdnsDiscoveryAdvertisement,
    ) -> Result<usize, NetworkError> {
        let bytes =
            serde_json::to_vec(advertisement).map_err(|source| NetworkError::Serialization {
                detail: source.to_string(),
            })?;
        self.socket
            .send_to(&bytes, self.multicast_address)
            .map_err(|source| NetworkError::Io {
                operation: "send lan discovery advertisement".to_string(),
                detail: source.to_string(),
            })
    }

    pub fn drain(&self) -> Result<Vec<MdnsDiscoveryAdvertisement>, NetworkError> {
        let mut advertisements = Vec::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((received, _source)) => {
                    let advertisement: MdnsDiscoveryAdvertisement =
                        serde_json::from_slice(&buffer[..received]).map_err(|source| {
                            NetworkError::Serialization {
                                detail: source.to_string(),
                            }
                        })?;
                    if advertisement.protocol == SINOMO_LAN_DISCOVERY_PROTOCOL {
                        advertisements.push(advertisement);
                    }
                }
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => break,
                Err(source) => {
                    return Err(NetworkError::Io {
                        operation: "receive lan discovery advertisement".to_string(),
                        detail: source.to_string(),
                    });
                }
            }
        }
        Ok(advertisements)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MdnsDiscoveryRecord {
    pub record_id: String,
    pub cluster_id: ClusterId,
    pub endpoint: ClusterEndpointRecord,
    pub advertised_tick: u64,
    pub expires_after_tick: u64,
}

fn cluster_transport_kind_label(transport: &ClusterTransportKind) -> &'static str {
    match transport {
        ClusterTransportKind::InProcess => "in-process",
        ClusterTransportKind::Loopback => "loopback",
        ClusterTransportKind::LanTcp => "lan-tcp",
        ClusterTransportKind::LanQuic => "lan-quic",
        ClusterTransportKind::Iroh => "iroh",
        ClusterTransportKind::WebRtc => "webrtc",
        ClusterTransportKind::TurnRelay => "turn-relay",
        ClusterTransportKind::HttpsRelay => "https-relay",
    }
}

impl MdnsDiscoveryRecord {
    pub fn lan_quic(
        cluster_id: ClusterId,
        node_id: NodeId,
        address: impl Into<String>,
        advertised_tick: u64,
        ttl_ticks: u64,
    ) -> Self {
        let address = address.into();
        let record_id = format!("{cluster_id}:{node_id}:lan-quic:{address}");
        Self {
            record_id,
            cluster_id,
            endpoint: ClusterEndpointRecord {
                endpoint_id: sinomo_core::ClusterEndpointId::generate(),
                node_id,
                transport: ClusterTransportKind::LanQuic,
                address,
                source: ClusterEndpointSource::Mdns,
                privacy: ClusterEndpointPrivacy::Lan,
                priority: 10,
                last_observed_latency_ms: None,
            },
            advertised_tick,
            expires_after_tick: advertised_tick.saturating_add(ttl_ticks),
        }
    }
}

pub const WEBRTC_SIGNALING_PROTOCOL: &str = "sinomo.webrtc-signaling.v1";
pub const IROH_ROAMING_PROTOCOL: &str = "sinomo.iroh-roaming.v1";

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
            endpoint_id: sinomo_core::ClusterEndpointId::generate(),
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
        let value =
            serde_json::to_vec(&envelope).map_err(|source| NetworkError::Serialization {
                detail: source.to_string(),
            })?;
        Ok(KvOp::Put {
            key,
            value,
            lease_id,
        })
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
        let value =
            serde_json::to_vec(&candidate).map_err(|source| NetworkError::Serialization {
                detail: source.to_string(),
            })?;
        Ok(KvOp::Put {
            key,
            value,
            lease_id,
        })
    }

    pub fn signal_from_entry(entry: &KvEntry) -> Result<WebRtcSignalingEnvelope, NetworkError> {
        serde_json::from_slice(&entry.value).map_err(|source| NetworkError::Serialization {
            detail: source.to_string(),
        })
    }

    pub fn candidate_from_entry(entry: &KvEntry) -> Result<IceCandidateRecord, NetworkError> {
        serde_json::from_slice(&entry.value).map_err(|source| NetworkError::Serialization {
            detail: source.to_string(),
        })
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
        let has_direct_address = !self.direct_addresses.is_empty();
        let address = if has_direct_address {
            format!(
                "iroh://{}?direct={}",
                self.endpoint_public_key,
                self.direct_addresses.join(",")
            )
        } else if let Some(relay_url) = &self.relay_url {
            format!("iroh://{}?relay={relay_url}", self.endpoint_public_key)
        } else {
            format!("iroh://{}", self.endpoint_public_key)
        };

        ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::generate(),
            node_id: self.node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address,
            source: ClusterEndpointSource::Rendezvous,
            privacy: if has_direct_address {
                ClusterEndpointPrivacy::InternetDirect
            } else {
                ClusterEndpointPrivacy::Relayed
            },
            priority: if has_direct_address { 40 } else { 90 },
            last_observed_latency_ms: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum IrohRelayMode {
    Disabled,
    Default,
    Custom { relay_url: String },
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
        let value =
            serde_json::to_vec(&endpoint).map_err(|source| NetworkError::Serialization {
                detail: source.to_string(),
            })?;
        Ok(KvOp::Put {
            key,
            value,
            lease_id,
        })
    }

    pub fn endpoint_from_entry(entry: &KvEntry) -> Result<IrohEndpointRecord, NetworkError> {
        serde_json::from_slice(&entry.value).map_err(|source| NetworkError::Serialization {
            detail: source.to_string(),
        })
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PeerState {
    pub endpoint: NodeEndpoint,
    pub status: PeerStatus,
    pub link: LinkState,
}

impl PeerState {
    pub fn new(endpoint: NodeEndpoint) -> Self {
        Self {
            link: LinkState::new(endpoint.node_id.clone()),
            endpoint,
            status: PeerStatus::Unknown,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum PeerStatus {
    Healthy,
    Degraded,
    Suspect,
    Offline,
    Unknown,
}

impl PeerStatus {
    pub fn can_route_reactive(&self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded | Self::Unknown)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LinkState {
    pub peer_node_id: NodeId,
    pub last_seen_tick: Option<u64>,
    pub missed_heartbeats: u16,
}

impl LinkState {
    pub fn new(peer_node_id: NodeId) -> Self {
        Self {
            peer_node_id,
            last_seen_tick: None,
            missed_heartbeats: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct HeartbeatPolicy {
    pub suspect_after_missed: u16,
    pub offline_after_missed: u16,
}

impl Default for HeartbeatPolicy {
    fn default() -> Self {
        Self {
            suspect_after_missed: 2,
            offline_after_missed: 3,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Heartbeat {
    pub node_id: NodeId,
    pub tick: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MeshMessageId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MeshMessage {
    pub message_id: MeshMessageId,
    pub origin_node_id: NodeId,
    pub target_node_id: NodeId,
    pub body: MeshMessageBody,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MeshMessageBody {
    Event {
        delivery_id: RouteDeliveryId,
        event: Box<EventEnvelope>,
    },
    Ack(DeliveryAck),
    Heartbeat(Heartbeat),
    Protocol(ProtocolMessage),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProtocolMessage {
    pub protocol: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

impl ProtocolMessage {
    pub fn new(protocol: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            protocol: protocol.into(),
            payload,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkFrameId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NetworkFrame {
    pub frame_id: NetworkFrameId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub message: MeshMessage,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InboundFrame {
    pub received_by: NodeId,
    pub frame: NetworkFrame,
}

pub trait NetworkTransport {
    fn local_node_id(&self) -> &NodeId;
    fn send(&mut self, target_node_id: &NodeId, frame: NetworkFrame) -> Result<(), NetworkError>;
    fn drain_incoming(&mut self) -> Result<Vec<InboundFrame>, NetworkError>;
}

#[derive(Clone, Debug, Default)]
pub struct InProcessNetwork {
    inner: Rc<RefCell<InProcessNetworkState>>,
}

impl InProcessNetwork {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&self, node_id: NodeId) -> InProcessTransport {
        self.inner
            .borrow_mut()
            .mailboxes
            .entry(node_id.clone())
            .or_default();

        InProcessTransport {
            local_node_id: node_id,
            inner: Rc::clone(&self.inner),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct InProcessNetworkState {
    mailboxes: BTreeMap<NodeId, VecDeque<InboundFrame>>,
}

#[derive(Clone, Debug)]
pub struct InProcessTransport {
    local_node_id: NodeId,
    inner: Rc<RefCell<InProcessNetworkState>>,
}

impl NetworkTransport for InProcessTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn send(&mut self, target_node_id: &NodeId, frame: NetworkFrame) -> Result<(), NetworkError> {
        if &frame.source_node_id != self.local_node_id() {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: frame.source_node_id,
            });
        }

        if &frame.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: frame.target_node_id,
            });
        }

        let mut inner = self.inner.borrow_mut();
        let mailbox =
            inner
                .mailboxes
                .get_mut(target_node_id)
                .ok_or_else(|| NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                })?;

        mailbox.push_back(InboundFrame {
            received_by: target_node_id.clone(),
            frame,
        });
        Ok(())
    }

    fn drain_incoming(&mut self) -> Result<Vec<InboundFrame>, NetworkError> {
        let mut inner = self.inner.borrow_mut();
        let mailbox = inner
            .mailboxes
            .get_mut(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;

        Ok(mailbox.drain(..).collect())
    }
}

#[derive(Clone, Debug)]
pub struct NetworkService<T> {
    router: InProcessRouter,
    transport: T,
    ledger: DeliveryLedger,
    seen_events: BTreeSet<(NodeId, EventId)>,
    next_frame_id: u64,
    next_message_id: u64,
}

impl<T: NetworkTransport> NetworkService<T> {
    pub fn new(registry: PeerRegistry, transport: T) -> Self {
        Self {
            router: InProcessRouter::new(registry),
            transport,
            ledger: DeliveryLedger::new(RetryPolicy::default()),
            seen_events: BTreeSet::new(),
            next_frame_id: 1,
            next_message_id: 1,
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.ledger.retry_policy = retry_policy;
        self
    }

    pub fn registry(&self) -> &PeerRegistry {
        self.router.registry()
    }

    pub fn registry_mut(&mut self) -> &mut PeerRegistry {
        self.router.registry_mut()
    }

    pub fn ledger(&self) -> &DeliveryLedger {
        &self.ledger
    }

    pub fn enqueue_event(
        &mut self,
        stored_event: &StoredEvent,
    ) -> Result<Vec<RouteDelivery>, NetworkError> {
        let deliveries = self.router.route_event(stored_event)?;
        let mut accepted = Vec::new();

        for delivery in deliveries {
            if self
                .ledger
                .enqueue(delivery.clone(), stored_event.event.clone())
                .is_some()
            {
                accepted.push(delivery);
            }
        }

        Ok(accepted)
    }

    pub fn pump_outbound(&mut self) -> Result<usize, NetworkError> {
        let mut sent = 0;
        let delivery_ids = self.ledger.ready_delivery_ids();

        for delivery_id in delivery_ids {
            let Some(record) = self.ledger.record(&delivery_id).cloned() else {
                continue;
            };

            if record.delivery.target_node_id != *self.transport.local_node_id()
                && !self.registry().is_routable(&record.delivery.target_node_id)
            {
                self.ledger.mark_for_retry(delivery_id)?;
                continue;
            }

            let frame = self.event_frame(&record);
            match self.transport.send(&record.delivery.target_node_id, frame) {
                Ok(()) => {
                    self.ledger.mark_sent(delivery_id)?;
                    sent += 1;
                }
                Err(error) => {
                    self.ledger.mark_attempt_failed(delivery_id)?;
                    if self
                        .ledger
                        .record(&delivery_id)
                        .is_some_and(|record| record.delivery.status == DeliveryStatus::Failed)
                    {
                        return Err(error);
                    }
                }
            }
        }

        Ok(sent)
    }

    pub fn process_incoming<S: EventStore>(
        &mut self,
        store: &mut S,
    ) -> Result<Vec<InboundOutcome>, NetworkError> {
        let inbound_frames = self.transport.drain_incoming()?;
        let mut outcomes = Vec::with_capacity(inbound_frames.len());

        for inbound in inbound_frames {
            match inbound.frame.message.body {
                MeshMessageBody::Event { delivery_id, event } => {
                    let key = (event.origin_node_id.clone(), event.event_id.clone());
                    let duplicate = self.seen_events.contains(&key);

                    if !duplicate {
                        match store.append((*event).clone()) {
                            Ok(_) => {
                                self.seen_events.insert(key);
                                outcomes.push(InboundOutcome::EventAccepted {
                                    delivery_id,
                                    event_id: event.event_id.clone(),
                                });
                            }
                            Err(EventStoreError::DuplicateEventId { .. }) => {
                                self.seen_events.insert(key);
                                outcomes.push(InboundOutcome::EventDuplicate {
                                    delivery_id,
                                    event_id: event.event_id.clone(),
                                });
                            }
                            Err(error) => return Err(NetworkError::EventStore(error)),
                        }
                    } else {
                        outcomes.push(InboundOutcome::EventDuplicate {
                            delivery_id,
                            event_id: event.event_id.clone(),
                        });
                    }

                    let ack = DeliveryAck {
                        delivery_id,
                        event_id: event.event_id,
                        target_node_id: self.transport.local_node_id().clone(),
                    };
                    let ack_frame = self.ack_frame(inbound.frame.source_node_id.clone(), ack);
                    self.transport
                        .send(&inbound.frame.source_node_id, ack_frame)?;
                }
                MeshMessageBody::Ack(ack) => {
                    self.ledger
                        .ack(ack.delivery_id, &inbound.frame.source_node_id)?;
                    outcomes.push(InboundOutcome::AckAccepted {
                        delivery_id: ack.delivery_id,
                        target_node_id: ack.target_node_id,
                    });
                }
                MeshMessageBody::Heartbeat(heartbeat) => {
                    let status = self
                        .router
                        .registry_mut()
                        .record_heartbeat(&heartbeat.node_id, heartbeat.tick)?;
                    let requeued = self.ledger.requeue_target(&heartbeat.node_id);
                    outcomes.push(InboundOutcome::HeartbeatReceived {
                        node_id: heartbeat.node_id,
                        status,
                        requeued,
                    });
                }
                MeshMessageBody::Protocol(protocol_message) => {
                    outcomes.push(InboundOutcome::ProtocolIgnored {
                        protocol: protocol_message.protocol,
                        source_node_id: inbound.frame.source_node_id,
                    });
                }
            }
        }

        Ok(outcomes)
    }

    pub fn send_heartbeat(
        &mut self,
        target_node_id: &NodeId,
        tick: u64,
    ) -> Result<(), NetworkError> {
        let heartbeat = Heartbeat {
            node_id: self.transport.local_node_id().clone(),
            tick,
        };
        let frame = self.heartbeat_frame(target_node_id.clone(), heartbeat);
        self.transport.send(target_node_id, frame)
    }

    fn event_frame(&mut self, record: &DeliveryRecord) -> NetworkFrame {
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: self.transport.local_node_id().clone(),
            target_node_id: record.delivery.target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: self.transport.local_node_id().clone(),
                target_node_id: record.delivery.target_node_id.clone(),
                body: MeshMessageBody::Event {
                    delivery_id: record.delivery.delivery_id,
                    event: Box::new(record.event.clone()),
                },
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;
        frame
    }

    fn ack_frame(&mut self, target_node_id: NodeId, ack: DeliveryAck) -> NetworkFrame {
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: self.transport.local_node_id().clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: self.transport.local_node_id().clone(),
                target_node_id,
                body: MeshMessageBody::Ack(ack),
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;
        frame
    }

    fn heartbeat_frame(&mut self, target_node_id: NodeId, heartbeat: Heartbeat) -> NetworkFrame {
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: self.transport.local_node_id().clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: self.transport.local_node_id().clone(),
                target_node_id,
                body: MeshMessageBody::Heartbeat(heartbeat),
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;
        frame
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeliveryAck {
    pub delivery_id: RouteDeliveryId,
    pub event_id: EventId,
    pub target_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InboundOutcome {
    EventAccepted {
        delivery_id: RouteDeliveryId,
        event_id: EventId,
    },
    EventDuplicate {
        delivery_id: RouteDeliveryId,
        event_id: EventId,
    },
    AckAccepted {
        delivery_id: RouteDeliveryId,
        target_node_id: NodeId,
    },
    HeartbeatReceived {
        node_id: NodeId,
        status: PeerStatus,
        requeued: usize,
    },
    ProtocolIgnored {
        protocol: String,
        source_node_id: NodeId,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: u16,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 3 }
    }
}

#[derive(Clone, Debug)]
pub struct DeliveryLedger {
    retry_policy: RetryPolicy,
    records: BTreeMap<RouteDeliveryId, DeliveryRecord>,
    active_by_event_target: BTreeMap<(EventId, NodeId), RouteDeliveryId>,
}

impl DeliveryLedger {
    pub fn new(retry_policy: RetryPolicy) -> Self {
        Self {
            retry_policy,
            records: BTreeMap::new(),
            active_by_event_target: BTreeMap::new(),
        }
    }

    pub fn enqueue(
        &mut self,
        delivery: RouteDelivery,
        event: EventEnvelope,
    ) -> Option<RouteDeliveryId> {
        let key = (delivery.event_id.clone(), delivery.target_node_id.clone());
        if self.active_by_event_target.contains_key(&key) {
            return None;
        }

        let delivery_id = delivery.delivery_id;
        self.active_by_event_target.insert(key, delivery_id);
        self.records.insert(
            delivery_id,
            DeliveryRecord {
                delivery,
                event,
                attempts: 0,
            },
        );
        Some(delivery_id)
    }

    pub fn record(&self, delivery_id: &RouteDeliveryId) -> Option<&DeliveryRecord> {
        self.records.get(delivery_id)
    }

    pub fn records(&self) -> Vec<&DeliveryRecord> {
        self.records.values().collect()
    }

    pub fn ready_delivery_ids(&self) -> Vec<RouteDeliveryId> {
        self.records
            .iter()
            .filter_map(|(delivery_id, record)| {
                matches!(
                    record.delivery.status,
                    DeliveryStatus::Queued | DeliveryStatus::Retrying
                )
                .then_some(*delivery_id)
            })
            .collect()
    }

    pub fn mark_sent(&mut self, delivery_id: RouteDeliveryId) -> Result<(), NetworkError> {
        let record = self
            .records
            .get_mut(&delivery_id)
            .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
        record.attempts += 1;
        record.delivery.status = DeliveryStatus::Sent;
        Ok(())
    }

    pub fn mark_for_retry(&mut self, delivery_id: RouteDeliveryId) -> Result<(), NetworkError> {
        let record = self
            .records
            .get_mut(&delivery_id)
            .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
        if record.delivery.status != DeliveryStatus::Delivered {
            record.delivery.status = DeliveryStatus::Retrying;
        }
        Ok(())
    }

    pub fn mark_attempt_failed(
        &mut self,
        delivery_id: RouteDeliveryId,
    ) -> Result<(), NetworkError> {
        let record = self
            .records
            .get_mut(&delivery_id)
            .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
        record.attempts += 1;
        if record.attempts >= self.retry_policy.max_attempts {
            record.delivery.status = DeliveryStatus::Failed;
        } else {
            record.delivery.status = DeliveryStatus::Retrying;
        }
        Ok(())
    }

    pub fn requeue_target(&mut self, target_node_id: &NodeId) -> usize {
        let mut requeued = 0;
        for record in self.records.values_mut() {
            if &record.delivery.target_node_id == target_node_id
                && record.delivery.status != DeliveryStatus::Delivered
            {
                record.delivery.status = DeliveryStatus::Retrying;
                requeued += 1;
            }
        }
        requeued
    }

    pub fn ack(
        &mut self,
        delivery_id: RouteDeliveryId,
        acking_node_id: &NodeId,
    ) -> Result<(), NetworkError> {
        let record = self
            .records
            .get_mut(&delivery_id)
            .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
        if &record.delivery.target_node_id != acking_node_id {
            return Err(NetworkError::AckTargetMismatch {
                delivery_id,
                expected_target_node_id: record.delivery.target_node_id.clone(),
                acking_node_id: acking_node_id.clone(),
            });
        }

        record.delivery.status = DeliveryStatus::Delivered;
        Ok(())
    }
}

impl Default for DeliveryLedger {
    fn default() -> Self {
        Self::new(RetryPolicy::default())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DeliveryRecord {
    pub delivery: RouteDelivery,
    pub event: EventEnvelope,
    pub attempts: u16,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RouteDeliveryId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RouteDelivery {
    pub delivery_id: RouteDeliveryId,
    pub event_sequence: EventSequence,
    pub event_id: sinomo_core::EventId,
    pub target_node_id: NodeId,
    pub latency_class: LatencyClass,
    pub status: DeliveryStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryStatus {
    Queued,
    Sent,
    Retrying,
    Delivered,
    Failed,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum NetworkError {
    #[error("unknown node: {node_id}")]
    UnknownNode { node_id: NodeId },
    #[error("unauthorized cluster node: {node_id}")]
    UnauthorizedNode { node_id: NodeId },
    #[error("frame source {source_node_id} does not match local transport node {local_node_id}")]
    FrameSourceMismatch {
        local_node_id: NodeId,
        source_node_id: NodeId,
    },
    #[error(
        "frame target {frame_target_node_id} does not match requested target {requested_target_node_id}"
    )]
    FrameTargetMismatch {
        requested_target_node_id: NodeId,
        frame_target_node_id: NodeId,
    },
    #[error("unknown delivery: {delivery_id:?}")]
    UnknownDelivery { delivery_id: RouteDeliveryId },
    #[error(
        "delivery {delivery_id:?} acked by {acking_node_id}, expected {expected_target_node_id}"
    )]
    AckTargetMismatch {
        delivery_id: RouteDeliveryId,
        expected_target_node_id: NodeId,
        acking_node_id: NodeId,
    },
    #[error("cluster mismatch: expected {expected}, got {actual}")]
    ClusterMismatch {
        expected: ClusterId,
        actual: ClusterId,
    },
    #[error(
        "certificate node {certificate_node_id} does not match material node {material_node_id}"
    )]
    CertificateNodeMismatch {
        material_node_id: NodeId,
        certificate_node_id: NodeId,
    },
    #[error("certificate key does not match member key for {node_id}")]
    CertificateKeyMismatch { node_id: NodeId },
    #[error("certificate expired for {node_id}")]
    CertificateExpired { node_id: NodeId },
    #[error("certificate signature missing for {node_id}")]
    CertificateSignatureMissing { node_id: NodeId },
    #[error("network frame is {actual_bytes} bytes; max is {max_bytes}")]
    FrameTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("network serialization error: {detail}")]
    Serialization { detail: String },
    #[error("network I/O error during {operation}: {detail}")]
    Io { operation: String, detail: String },
    #[error(transparent)]
    EventStore(#[from] EventStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        ClusterCertificate, ClusterCertificateId, ClusterEndpointPrivacy, ClusterEndpointRecord,
        ClusterEndpointSource, ClusterId, ClusterMemberRecord, ClusterTransportKind, Confidence,
        EventEnvelope, EventId, EventKind, LatencyClass, NodeId, PrivacyClass, RoutingMetadata,
        RoutingScope, Salience, SessionId, Source,
    };
    use sinomo_events::{EventStore, InMemoryEventStore, ReplayQuery};

    fn node(id: &str) -> NodeProfile {
        let mut profile = NodeProfile::local(id).expect("profile");
        profile.node_id = NodeId::new(id).expect("node id");
        profile
    }

    fn fixture_event(id: &str, origin_node_id: NodeId) -> EventEnvelope {
        EventEnvelope::new(
            EventId::new(id).expect("event"),
            time::OffsetDateTime::now_utc(),
            origin_node_id,
            Source::new("test.network").expect("source"),
            EventKind::new("network.test.event").expect("kind"),
            Salience::new(0.5).expect("salience"),
            Confidence::new(1.0).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
    }

    fn certificate(
        cluster_id: ClusterId,
        node_id: NodeId,
        issuer_node_id: NodeId,
        public_key: &str,
        expires_at: time::OffsetDateTime,
    ) -> ClusterCertificate {
        ClusterCertificate {
            certificate_id: ClusterCertificateId::generate(),
            cluster_id,
            node_id,
            issuer_node_id,
            public_key: public_key.to_string(),
            capability_scope: Vec::new(),
            issued_at: time::OffsetDateTime::now_utc(),
            expires_at,
            signature: "sig".to_string(),
        }
    }

    #[test]
    fn peer_registry_tracks_cluster_members_and_endpoint_candidates() {
        let cluster_id = ClusterId::new("cluster_home").expect("cluster");
        let node_id = NodeId::new("node_desktop").expect("node");
        let timestamp = time::OffsetDateTime::now_utc();
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            node_id.clone(),
            "desktop",
            "pk_desktop",
            timestamp,
        );
        member.endpoints.push(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_lan").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::LanQuic,
            address: "192.168.1.20:21353".to_string(),
            source: ClusterEndpointSource::Mdns,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        });
        member.endpoints.push(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_webrtc").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::WebRtc,
            address: "webrtc://candidate".to_string(),
            source: ClusterEndpointSource::Ice,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 50,
            last_observed_latency_ms: None,
        });
        let mut registry = PeerRegistry::new();

        registry.upsert_cluster_member(member.clone());

        assert_eq!(registry.cluster_id(), Some(&cluster_id));
        assert_eq!(registry.cluster_member(&node_id), Some(&member));
        assert_eq!(registry.endpoint_candidates(&node_id).len(), 2);
        assert_eq!(
            registry
                .best_endpoint_candidate(&node_id)
                .expect("best endpoint")
                .endpoint
                .transport,
            ClusterTransportKind::LanQuic
        );
    }

    #[test]
    fn node_endpoint_maps_cluster_roaming_transports() {
        let node_id = NodeId::new("node_phone").expect("node");
        let endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_ice").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::WebRtc,
            address: "webrtc://candidate".to_string(),
            source: ClusterEndpointSource::Ice,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 20,
            last_observed_latency_ms: Some(40),
        };

        let network_endpoint = NodeEndpoint::from_cluster_endpoint(&endpoint);

        assert_eq!(network_endpoint.node_id, node_id);
        assert_eq!(network_endpoint.transport, TransportKind::WebRtc);
        assert_eq!(network_endpoint.address, "webrtc://candidate");
    }

    #[test]
    fn node_endpoint_maps_iroh_transport() {
        let node_id = NodeId::new("node_phone").expect("node");
        let endpoint = ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_iroh").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address: "iroh://iroh_pk".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 40,
            last_observed_latency_ms: None,
        };

        let network_endpoint = NodeEndpoint::from_cluster_endpoint(&endpoint);

        assert_eq!(network_endpoint.node_id, node_id);
        assert_eq!(network_endpoint.transport, TransportKind::Iroh);
        assert_eq!(network_endpoint.address, "iroh://iroh_pk");
    }

    #[test]
    fn lan_quic_frame_codec_round_trips_network_frames_and_limits_size() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let event = fixture_event("evt_codec", node_a.clone());
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(7),
            source_node_id: node_a.clone(),
            target_node_id: node_b.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(8),
                origin_node_id: node_a,
                target_node_id: node_b,
                body: MeshMessageBody::Event {
                    delivery_id: RouteDeliveryId(9),
                    event: Box::new(event),
                },
            },
        };
        let codec = LanQuicFrameCodec {
            max_frame_bytes: 16 * 1024,
        };

        let encoded = codec.encode(&frame).expect("encode");
        let decoded = codec.decode(&encoded).expect("decode");

        assert_eq!(decoded, frame);
        assert!(matches!(
            LanQuicFrameCodec { max_frame_bytes: 1 }.encode(&frame),
            Err(NetworkError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn mdns_discovery_cache_returns_lan_quic_candidates_and_expires_records() {
        let cluster_id = ClusterId::new("cluster_home").expect("cluster");
        let node_id = NodeId::new("node_desktop").expect("node");
        let mut cache = MdnsDiscoveryCache::new();
        cache.upsert(MdnsDiscoveryRecord::lan_quic(
            cluster_id.clone(),
            node_id.clone(),
            "192.168.1.20:21355",
            10,
            5,
        ));

        let candidates = cache.endpoint_candidates_for_node(&node_id);

        assert_eq!(cache.endpoints_for_cluster(&cluster_id).len(), 1);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].endpoint.transport,
            ClusterTransportKind::LanQuic
        );
        assert_eq!(cache.expire_after_tick(14), 0);
        assert_eq!(cache.expire_after_tick(15), 1);
        assert!(cache.endpoint_candidates_for_node(&node_id).is_empty());
    }

    #[test]
    fn cluster_auth_verifier_establishes_mutual_session_for_authorized_members() {
        let cluster_id = ClusterId::new("cluster_home").expect("cluster");
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let now = time::OffsetDateTime::now_utc();
        let expires_at = now + time::Duration::hours(1);
        let member_a =
            ClusterMemberRecord::local_voter(cluster_id.clone(), node_a.clone(), "a", "pk_a", now);
        let member_b =
            ClusterMemberRecord::local_voter(cluster_id.clone(), node_b.clone(), "b", "pk_b", now);
        let verifier = ClusterAuthVerifier::new(vec![member_a, member_b]);
        let material_a = ClusterAuthMaterial {
            cluster_id: cluster_id.clone(),
            node_id: node_a.clone(),
            node_public_key: "pk_a".to_string(),
            certificate: certificate(
                cluster_id.clone(),
                node_a.clone(),
                node_a.clone(),
                "pk_a",
                expires_at,
            ),
        };
        let material_b = ClusterAuthMaterial {
            cluster_id: cluster_id.clone(),
            node_id: node_b.clone(),
            node_public_key: "pk_b".to_string(),
            certificate: certificate(
                cluster_id.clone(),
                node_b.clone(),
                node_a.clone(),
                "pk_b",
                expires_at,
            ),
        };

        let session = verifier
            .establish_mutual_session(&material_a, &material_b, now)
            .expect("mutual auth");

        assert_eq!(session.cluster_id, cluster_id);
        assert_eq!(session.local_node_id, node_a);
        assert_eq!(session.remote_node_id, node_b);
    }

    #[test]
    fn cluster_auth_verifier_rejects_expired_or_wrong_key_certificate() {
        let cluster_id = ClusterId::new("cluster_home").expect("cluster");
        let node_a = NodeId::new("node_a").expect("node");
        let now = time::OffsetDateTime::now_utc();
        let member =
            ClusterMemberRecord::local_voter(cluster_id.clone(), node_a.clone(), "a", "pk_a", now);
        let verifier = ClusterAuthVerifier::new(vec![member]);
        let wrong_key = ClusterAuthMaterial {
            cluster_id: cluster_id.clone(),
            node_id: node_a.clone(),
            node_public_key: "pk_a".to_string(),
            certificate: certificate(
                cluster_id.clone(),
                node_a.clone(),
                node_a.clone(),
                "pk_other",
                now + time::Duration::hours(1),
            ),
        };
        let expired = ClusterAuthMaterial {
            cluster_id: cluster_id.clone(),
            node_id: node_a.clone(),
            node_public_key: "pk_a".to_string(),
            certificate: certificate(cluster_id, node_a.clone(), node_a, "pk_a", now),
        };

        assert!(matches!(
            verifier.verify_peer(&wrong_key.cluster_id, &wrong_key, now),
            Err(NetworkError::CertificateKeyMismatch { .. })
        ));
        assert!(matches!(
            verifier.verify_peer(&expired.cluster_id, &expired, now),
            Err(NetworkError::CertificateExpired { .. })
        ));
    }

    #[test]
    fn lan_connection_planner_prefers_lan_quic_over_relays() {
        let node_id = NodeId::new("node_desktop").expect("node");
        let relay = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_relay").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::TurnRelay,
            address: "turn://relay".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 1,
            last_observed_latency_ms: None,
        });
        let lan = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_lan").expect("endpoint"),
            node_id,
            transport: ClusterTransportKind::LanQuic,
            address: "192.168.1.20:21355".to_string(),
            source: ClusterEndpointSource::Mdns,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        });

        let candidates = [relay, lan];
        let selected =
            LanConnectionPlanner::select_direct_candidate(&candidates).expect("direct candidate");

        assert_eq!(selected.endpoint.transport, ClusterTransportKind::LanQuic);
    }

    #[test]
    fn webrtc_kv_exchange_round_trips_offer_answer_and_ice_candidates() {
        let cluster_id = ClusterId::new("cluster_home").expect("cluster");
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let exchange_id = "exchange_1";
        let mut store = sinomo_kv::InProcessKvStore::new();
        store
            .apply_command(sinomo_kv::KvCommand::Op(
                WebRtcKvExchange::put_signal(
                    WebRtcSignalingEnvelope::offer(
                        cluster_id.clone(),
                        exchange_id,
                        node_a.clone(),
                        node_b.clone(),
                        "v=0 offer",
                        10,
                    ),
                    None,
                )
                .expect("offer op"),
            ))
            .expect("put offer");
        store
            .apply_command(sinomo_kv::KvCommand::Op(
                WebRtcKvExchange::put_signal(
                    WebRtcSignalingEnvelope::answer(
                        cluster_id.clone(),
                        exchange_id,
                        node_b.clone(),
                        node_a.clone(),
                        "v=0 answer",
                        10,
                    ),
                    None,
                )
                .expect("answer op"),
            ))
            .expect("put answer");
        let relay_candidate = IceCandidateRecord {
            cluster_id: cluster_id.clone(),
            exchange_id: exchange_id.to_string(),
            node_id: node_b.clone(),
            candidate_id: "relay_1".to_string(),
            candidate_type: IceCandidateType::Relay,
            address: "turn://relay.example:3478".to_string(),
            priority: 10,
            raw_candidate: "candidate:relay".to_string(),
            expires_after_tick: 10,
        };
        store
            .apply_command(sinomo_kv::KvCommand::Op(
                WebRtcKvExchange::put_candidate(relay_candidate.clone(), None)
                    .expect("candidate op"),
            ))
            .expect("put candidate");

        let signals =
            store.range_prefix(&WebRtcKvExchange::signal_prefix(&cluster_id, exchange_id));
        let candidates = store.range_prefix(&WebRtcKvExchange::candidate_prefix(
            &cluster_id,
            exchange_id,
        ));
        let parsed_offer = WebRtcKvExchange::signal_from_entry(&signals[0]).expect("offer");
        let parsed_candidate =
            WebRtcKvExchange::candidate_from_entry(&candidates[0]).expect("candidate");

        assert_eq!(signals.len(), 2);
        assert!(matches!(
            parsed_offer.message,
            WebRtcSignalMessage::Offer { .. }
        ));
        assert_eq!(parsed_candidate, relay_candidate);
        assert_eq!(
            parsed_candidate.endpoint().transport,
            ClusterTransportKind::TurnRelay
        );
    }

    #[test]
    fn iroh_kv_exchange_round_trips_endpoint_records() {
        let cluster_id = ClusterId::new("cluster_home").expect("cluster");
        let node_id = NodeId::new("node_mobile").expect("node");
        let endpoint = IrohEndpointRecord {
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            endpoint_public_key: "iroh_public_key".to_string(),
            direct_addresses: vec!["203.0.113.10:21356".to_string()],
            relay_url: Some("https://relay.example".to_string()),
            relay_mode: IrohRelayMode::Default,
            expires_after_tick: 50,
        };
        let mut store = sinomo_kv::InProcessKvStore::new();
        store
            .apply_command(sinomo_kv::KvCommand::Op(
                IrohKvExchange::put_endpoint(endpoint.clone(), None).expect("endpoint op"),
            ))
            .expect("put endpoint");

        let entries = store.range_prefix(&IrohKvExchange::endpoint_prefix(&cluster_id));
        let parsed = IrohKvExchange::endpoint_from_entry(&entries[0]).expect("endpoint");
        let cluster_endpoint = parsed.endpoint();

        assert_eq!(entries.len(), 1);
        assert_eq!(parsed, endpoint);
        assert_eq!(cluster_endpoint.node_id, node_id);
        assert_eq!(cluster_endpoint.transport, ClusterTransportKind::Iroh);
        assert_eq!(
            cluster_endpoint.privacy,
            ClusterEndpointPrivacy::InternetDirect
        );
        assert!(
            cluster_endpoint
                .address
                .contains("direct=203.0.113.10:21356")
        );
    }

    #[test]
    fn roaming_connection_planner_uses_webrtc_then_turn_fallback_when_lan_missing() {
        let node_id = NodeId::new("node_mobile").expect("node");
        let webrtc = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_webrtc").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::WebRtc,
            address: "webrtc://srflx".to_string(),
            source: ClusterEndpointSource::Ice,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 60,
            last_observed_latency_ms: None,
        })
        .healthy(35);
        let turn = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_turn").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::TurnRelay,
            address: "turn://relay".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 120,
            last_observed_latency_ms: None,
        });
        let selected = RoamingConnectionPlanner::select_path(&[turn.clone(), webrtc.clone()])
            .expect("roaming path");

        assert!(matches!(
            selected,
            RoamingPathSelection::WebRtc(PeerEndpointCandidate { endpoint, .. })
                if endpoint.address == "webrtc://srflx"
        ));

        let failed_webrtc = webrtc.failed("ice failed");
        let fallback =
            RoamingConnectionPlanner::select_path(&[failed_webrtc, turn]).expect("fallback path");

        assert!(matches!(
            fallback,
            RoamingPathSelection::Relay(PeerEndpointCandidate { endpoint, .. })
                if endpoint.transport == ClusterTransportKind::TurnRelay
        ));
    }

    #[test]
    fn roaming_connection_planner_prefers_iroh_before_webrtc_and_relay() {
        let node_id = NodeId::new("node_mobile").expect("node");
        let iroh = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_iroh").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address: "iroh://iroh_public_key?relay=https://relay.example".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        });
        let webrtc = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_webrtc").expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::WebRtc,
            address: "webrtc://srflx".to_string(),
            source: ClusterEndpointSource::Ice,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 10,
            last_observed_latency_ms: None,
        });
        let relay = PeerEndpointCandidate::discovered(ClusterEndpointRecord {
            endpoint_id: sinomo_core::ClusterEndpointId::new("endpoint_turn").expect("endpoint"),
            node_id,
            transport: ClusterTransportKind::TurnRelay,
            address: "turn://relay".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 5,
            last_observed_latency_ms: None,
        });

        let selected =
            RoamingConnectionPlanner::select_path(&[relay, webrtc, iroh]).expect("roaming path");

        assert!(matches!(
            selected,
            RoamingPathSelection::Iroh(PeerEndpointCandidate { endpoint, .. })
                if endpoint.transport == ClusterTransportKind::Iroh
        ));
    }

    #[test]
    fn routes_session_events_to_participants() {
        let session_id = SessionId::new("sess_demo").expect("session");
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_mobile"));
        registry.register_node(node("node_desktop"));
        registry
            .join_session(
                session_id.clone(),
                NodeId::new("node_mobile").expect("node"),
            )
            .expect("join mobile");
        registry
            .join_session(
                session_id.clone(),
                NodeId::new("node_desktop").expect("node"),
            )
            .expect("join desktop");

        let event = EventEnvelope::new(
            EventId::new("evt_route").expect("event"),
            time::OffsetDateTime::now_utc(),
            NodeId::new("node_mobile").expect("node"),
            Source::new("test.route").expect("source"),
            EventKind::new("evolution.job.scoped").expect("kind"),
            Salience::new(0.8).expect("salience"),
            Confidence::new(1.0).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_session(session_id);

        let mut store = InMemoryEventStore::new();
        let receipt = store.append(event).expect("append");
        let stored = store
            .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
            .expect("replay")
            .pop()
            .expect("stored event");

        let mut router = InProcessRouter::new(registry);
        let deliveries = router.route_event(&stored).expect("route");
        assert_eq!(deliveries.len(), 2);
    }

    #[test]
    fn in_process_transport_delivers_typed_frames() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let network = InProcessNetwork::new();
        let mut transport_a = network.add_node(node_a.clone());
        let mut transport_b = network.add_node(node_b.clone());
        let event = fixture_event("evt_transport", node_a.clone());

        let frame = NetworkFrame {
            frame_id: NetworkFrameId(1),
            source_node_id: node_a.clone(),
            target_node_id: node_b.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(1),
                origin_node_id: node_a,
                target_node_id: node_b.clone(),
                body: MeshMessageBody::Event {
                    delivery_id: RouteDeliveryId(1),
                    event: Box::new(event.clone()),
                },
            },
        };

        transport_a
            .send(&node_b, frame.clone())
            .expect("send frame");
        let inbound = transport_b.drain_incoming().expect("drain");

        assert_eq!(inbound.len(), 1);
        assert_eq!(inbound[0].received_by, node_b);
        assert_eq!(inbound[0].frame, frame);
    }

    #[test]
    fn in_process_transport_rejects_unknown_targets() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_missing = NodeId::new("node_missing").expect("node");
        let network = InProcessNetwork::new();
        let mut transport_a = network.add_node(node_a.clone());
        let event = fixture_event("evt_missing_target", node_a.clone());

        let error = transport_a
            .send(
                &node_missing,
                NetworkFrame {
                    frame_id: NetworkFrameId(1),
                    source_node_id: node_a.clone(),
                    target_node_id: node_missing.clone(),
                    message: MeshMessage {
                        message_id: MeshMessageId(1),
                        origin_node_id: node_a,
                        target_node_id: node_missing.clone(),
                        body: MeshMessageBody::Event {
                            delivery_id: RouteDeliveryId(1),
                            event: Box::new(event),
                        },
                    },
                },
            )
            .expect_err("missing target should fail");

        assert_eq!(
            error,
            NetworkError::UnknownNode {
                node_id: node_missing
            }
        );
    }

    #[test]
    fn network_service_delivers_event_and_processes_ack() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let session_id = SessionId::new("sess_network").expect("session");
        let network = InProcessNetwork::new();
        let transport_a = network.add_node(node_a.clone());
        let transport_b = network.add_node(node_b.clone());
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry.register_node(node("node_b"));
        registry
            .join_session(session_id.clone(), node_a.clone())
            .expect("join a");
        registry
            .join_session(session_id.clone(), node_b.clone())
            .expect("join b");
        let mut service_a = NetworkService::new(registry.clone(), transport_a);
        let mut service_b = NetworkService::new(registry, transport_b);
        let mut store_a = InMemoryEventStore::new();
        let mut store_b = InMemoryEventStore::new();
        let event = fixture_event("evt_service_delivery", node_a.clone()).with_session(session_id);
        let receipt = store_a.append(event).expect("append");
        let stored = store_a
            .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
            .expect("replay")
            .pop()
            .expect("stored");

        let deliveries = service_a.enqueue_event(&stored).expect("enqueue");
        assert_eq!(deliveries.len(), 2);
        assert_eq!(service_a.pump_outbound().expect("send"), 2);

        let outcomes = service_b
            .process_incoming(&mut store_b)
            .expect("process event");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(store_b.len(), 1);

        let ack_outcomes = service_a
            .process_incoming(&mut store_a)
            .expect("process ack");
        assert_eq!(ack_outcomes.len(), 2);
        assert_eq!(store_a.len(), 1);
        assert_eq!(
            ack_outcomes
                .iter()
                .filter(|outcome| matches!(outcome, InboundOutcome::AckAccepted { .. }))
                .count(),
            1
        );
        let delivered = service_a
            .ledger()
            .records()
            .into_iter()
            .filter(|record| record.delivery.status == DeliveryStatus::Delivered)
            .count();
        assert_eq!(delivered, 1);
    }

    #[test]
    fn delivery_ledger_suppresses_duplicate_event_targets() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let network = InProcessNetwork::new();
        let transport_a = network.add_node(node_a.clone());
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry.register_node(node("node_b"));
        let mut service = NetworkService::new(registry, transport_a);
        let mut store = InMemoryEventStore::new();
        let event =
            fixture_event("evt_duplicate_delivery", node_a.clone()).with_target_node(node_b);
        let receipt = store.append(event).expect("append");
        let stored = store
            .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
            .expect("replay")
            .pop()
            .expect("stored");

        assert_eq!(service.enqueue_event(&stored).expect("first").len(), 1);
        assert_eq!(service.enqueue_event(&stored).expect("duplicate").len(), 0);
        assert_eq!(service.ledger().records().len(), 1);
    }

    #[test]
    fn inbound_duplicate_event_is_acked_without_duplicate_append() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let network = InProcessNetwork::new();
        let transport_a = network.add_node(node_a.clone());
        let transport_b = network.add_node(node_b.clone());
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry.register_node(node("node_b"));
        let mut service_a = NetworkService::new(registry.clone(), transport_a);
        let mut service_b = NetworkService::new(registry, transport_b);
        let event = fixture_event("evt_duplicate_inbound", node_a.clone()).with_target_node(node_b);
        let mut store_b = InMemoryEventStore::new();

        for frame_id in [1, 2] {
            let frame = NetworkFrame {
                frame_id: NetworkFrameId(frame_id),
                source_node_id: node_a.clone(),
                target_node_id: service_b.transport.local_node_id().clone(),
                message: MeshMessage {
                    message_id: MeshMessageId(frame_id),
                    origin_node_id: node_a.clone(),
                    target_node_id: service_b.transport.local_node_id().clone(),
                    body: MeshMessageBody::Event {
                        delivery_id: RouteDeliveryId(1),
                        event: Box::new(event.clone()),
                    },
                },
            };
            service_a
                .transport
                .send(service_b.transport.local_node_id(), frame)
                .expect("send duplicate");
        }

        let outcomes = service_b
            .process_incoming(&mut store_b)
            .expect("process duplicates");
        assert!(matches!(outcomes[0], InboundOutcome::EventAccepted { .. }));
        assert!(matches!(outcomes[1], InboundOutcome::EventDuplicate { .. }));
        assert_eq!(store_b.len(), 1);
    }

    #[test]
    fn retry_policy_marks_delivery_failed_after_max_attempts() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let network = InProcessNetwork::new();
        let transport_a = network.add_node(node_a.clone());
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry.register_node(node("node_b"));
        let mut service = NetworkService::new(registry, transport_a)
            .with_retry_policy(RetryPolicy { max_attempts: 2 });
        let mut store = InMemoryEventStore::new();
        let event = fixture_event("evt_retry_failed", node_a.clone()).with_target_node(node_b);
        let receipt = store.append(event).expect("append");
        let stored = store
            .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
            .expect("replay")
            .pop()
            .expect("stored");
        let delivery = service.enqueue_event(&stored).expect("enqueue")[0].clone();

        assert!(service.pump_outbound().is_ok());
        assert_eq!(
            service
                .ledger()
                .record(&delivery.delivery_id)
                .unwrap()
                .delivery
                .status,
            DeliveryStatus::Retrying
        );
        let error = service
            .pump_outbound()
            .expect_err("final attempt should return send failure");
        assert!(matches!(error, NetworkError::UnknownNode { .. }));
        assert_eq!(
            service
                .ledger()
                .record(&delivery.delivery_id)
                .unwrap()
                .delivery
                .status,
            DeliveryStatus::Failed
        );
    }

    #[test]
    fn ack_rejects_wrong_target_node() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let node_c = NodeId::new("node_c").expect("node");
        let mut ledger = DeliveryLedger::new(RetryPolicy::default());
        let event = fixture_event("evt_wrong_ack", node_a);
        let delivery = RouteDelivery {
            delivery_id: RouteDeliveryId(1),
            event_sequence: EventSequence(1),
            event_id: event.event_id.clone(),
            target_node_id: node_b.clone(),
            latency_class: LatencyClass::Interactive,
            status: DeliveryStatus::Queued,
        };
        ledger.enqueue(delivery, event).expect("enqueue");

        let error = ledger
            .ack(RouteDeliveryId(1), &node_c)
            .expect_err("wrong target should fail");
        assert!(matches!(error, NetworkError::AckTargetMismatch { .. }));
        assert_eq!(
            ledger
                .record(&RouteDeliveryId(1))
                .unwrap()
                .delivery
                .target_node_id,
            node_b
        );
    }

    #[test]
    fn heartbeat_marks_peer_healthy_and_updates_node_health() {
        let node_a = NodeId::new("node_a").expect("node");
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));

        let status = registry.record_heartbeat(&node_a, 7).expect("heartbeat");

        assert_eq!(status, PeerStatus::Healthy);
        assert_eq!(
            registry
                .peer_state(&node_a)
                .expect("peer state")
                .link
                .last_seen_tick,
            Some(7)
        );
        assert_eq!(
            registry.node(&node_a).expect("node").health.availability,
            NodeAvailability::Available
        );
    }

    #[test]
    fn missed_heartbeats_degrade_then_mark_offline_without_removing_session() {
        let node_a = NodeId::new("node_a").expect("node");
        let session_id = SessionId::new("sess_heartbeat").expect("session");
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry
            .join_session(session_id.clone(), node_a.clone())
            .expect("join");

        assert_eq!(
            registry
                .record_missed_heartbeat(&node_a, HeartbeatPolicy::default())
                .expect("missed one"),
            PeerStatus::Degraded
        );
        assert_eq!(
            registry
                .record_missed_heartbeat(&node_a, HeartbeatPolicy::default())
                .expect("missed two"),
            PeerStatus::Suspect
        );
        assert_eq!(
            registry
                .record_missed_heartbeat(&node_a, HeartbeatPolicy::default())
                .expect("missed three"),
            PeerStatus::Offline
        );
        assert_eq!(
            registry.node(&node_a).expect("node").health.availability,
            NodeAvailability::Offline
        );
        assert_eq!(registry.session_nodes(&session_id), vec![node_a]);
    }

    #[test]
    fn heartbeat_recovery_requeues_failed_delivery_for_drain() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let network = InProcessNetwork::new();
        let transport_a = network.add_node(node_a.clone());
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry.register_node(node("node_b"));
        let mut service_a = NetworkService::new(registry.clone(), transport_a)
            .with_retry_policy(RetryPolicy { max_attempts: 2 });
        let mut store_a = InMemoryEventStore::new();
        let event =
            fixture_event("evt_recovery_requeue", node_a.clone()).with_target_node(node_b.clone());
        let receipt = store_a.append(event).expect("append");
        let stored = store_a
            .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
            .expect("replay")
            .pop()
            .expect("stored");
        let delivery_id = service_a.enqueue_event(&stored).expect("enqueue")[0].delivery_id;

        assert_eq!(service_a.pump_outbound().expect("first attempt"), 0);
        assert!(service_a.pump_outbound().is_err());
        assert_eq!(
            service_a
                .ledger()
                .record(&delivery_id)
                .expect("record")
                .delivery
                .status,
            DeliveryStatus::Failed
        );

        let transport_b = network.add_node(node_b.clone());
        let mut service_b = NetworkService::new(registry, transport_b);
        let mut store_b = InMemoryEventStore::new();
        service_b
            .send_heartbeat(&node_a, 42)
            .expect("send heartbeat");

        let outcomes = service_a
            .process_incoming(&mut store_a)
            .expect("process heartbeat");
        assert!(matches!(
            outcomes.as_slice(),
            [InboundOutcome::HeartbeatReceived {
                status: PeerStatus::Healthy,
                requeued: 1,
                ..
            }]
        ));
        assert_eq!(
            service_a
                .ledger()
                .record(&delivery_id)
                .expect("record")
                .delivery
                .status,
            DeliveryStatus::Retrying
        );

        assert_eq!(service_a.pump_outbound().expect("drain recovered"), 1);
        service_b
            .process_incoming(&mut store_b)
            .expect("receive recovered event");
        assert_eq!(store_b.len(), 1);
    }

    #[test]
    fn session_routing_excludes_offline_participants() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let session_id = SessionId::new("sess_offline_route").expect("session");
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry.register_node(node("node_b"));
        registry
            .join_session(session_id.clone(), node_a.clone())
            .expect("join a");
        registry
            .join_session(session_id.clone(), node_b.clone())
            .expect("join b");
        for _ in 0..3 {
            registry
                .record_missed_heartbeat(&node_b, HeartbeatPolicy::default())
                .expect("miss heartbeat");
        }
        let mut store = InMemoryEventStore::new();
        let event = fixture_event("evt_skip_offline", node_a.clone()).with_session(session_id);
        let receipt = store.append(event).expect("append");
        let stored = store
            .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
            .expect("replay")
            .pop()
            .expect("stored");

        let mut router = InProcessRouter::new(registry);
        let deliveries = router.route_event(&stored).expect("route");

        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].target_node_id, node_a);
    }

    #[test]
    fn direct_target_offline_buffers_instead_of_sending() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let network = InProcessNetwork::new();
        let transport_a = network.add_node(node_a.clone());
        let mut transport_b = network.add_node(node_b.clone());
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry.register_node(node("node_b"));
        for _ in 0..3 {
            registry
                .record_missed_heartbeat(&node_b, HeartbeatPolicy::default())
                .expect("miss heartbeat");
        }
        let mut service = NetworkService::new(registry, transport_a);
        let mut store = InMemoryEventStore::new();
        let event =
            fixture_event("evt_direct_offline", node_a.clone()).with_target_node(node_b.clone());
        let receipt = store.append(event).expect("append");
        let stored = store
            .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
            .expect("replay")
            .pop()
            .expect("stored");
        let delivery_id = service.enqueue_event(&stored).expect("enqueue")[0].delivery_id;

        assert_eq!(service.pump_outbound().expect("buffer offline"), 0);
        assert_eq!(transport_b.drain_incoming().expect("drain").len(), 0);
        assert_eq!(
            service
                .ledger()
                .record(&delivery_id)
                .expect("record")
                .delivery
                .status,
            DeliveryStatus::Retrying
        );
    }

    #[test]
    fn trusted_network_routing_skips_offline_peers() {
        let node_a = NodeId::new("node_a").expect("node");
        let node_b = NodeId::new("node_b").expect("node");
        let node_c = NodeId::new("node_c").expect("node");
        let mut registry = PeerRegistry::new();
        registry.register_node(node("node_a"));
        registry.register_node(node("node_b"));
        registry.register_node(node("node_c"));
        for _ in 0..3 {
            registry
                .record_missed_heartbeat(&node_b, HeartbeatPolicy::default())
                .expect("miss heartbeat");
        }
        registry
            .record_heartbeat(&node_c, 1)
            .expect("mark c healthy");
        let mut event = fixture_event("evt_trusted_network", node_a);
        event.routing.scope = RoutingScope::TrustedNetwork;
        let mut store = InMemoryEventStore::new();
        let receipt = store.append(event).expect("append");
        let stored = store
            .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))
            .expect("replay")
            .pop()
            .expect("stored");

        let mut router = InProcessRouter::new(registry);
        let deliveries = router.route_event(&stored).expect("route");
        let targets: BTreeSet<NodeId> = deliveries
            .into_iter()
            .map(|delivery| delivery.target_node_id)
            .collect();

        assert_eq!(
            targets,
            BTreeSet::from([stored.event.origin_node_id, node_c])
        );
    }
}
