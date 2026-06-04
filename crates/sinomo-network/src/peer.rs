use crate::error::NetworkError;
use crate::prelude::*;
use std::time::Instant;

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
        if member.endpoints.is_empty() {
            self.endpoint_candidates.remove(&node_id);
        } else {
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
    #[serde(default, with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl ProtocolMessage {
    pub fn new(protocol: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            protocol: protocol.into(),
            payload,
        }
    }

    pub fn encode<T: serde::Serialize + ?Sized>(
        protocol: impl Into<String>,
        payload: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self::new(
            protocol,
            crate::object::SinomoObjectCodec::default().encode(payload)?,
        ))
    }

    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        crate::object::SinomoObjectCodec::default().decode(&self.payload)
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

    fn drain_incoming_limited(
        &mut self,
        max_frames: usize,
    ) -> Result<Vec<InboundFrame>, NetworkError> {
        let _ = max_frames;
        self.drain_incoming()
    }
}

pub(crate) const SEEN_EVENT_CACHE_CAPACITY: usize = 16_384;

#[derive(Clone, Debug)]
pub(crate) struct SeenEventCache {
    capacity: usize,
    keys: BTreeSet<(NodeId, EventId)>,
    order: VecDeque<(NodeId, EventId)>,
}

impl SeenEventCache {
    pub(crate) fn new() -> Self {
        Self::with_capacity(SEEN_EVENT_CACHE_CAPACITY)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            keys: BTreeSet::new(),
            order: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn contains(&self, key: &(NodeId, EventId)) -> bool {
        self.keys.contains(key)
    }

    pub(crate) fn insert(&mut self, key: (NodeId, EventId)) {
        if !self.keys.insert(key.clone()) {
            return;
        }
        self.order.push_back(key);
        while self.keys.len() > self.capacity {
            let Some(evicted) = self.order.pop_front() else {
                return;
            };
            self.keys.remove(&evicted);
        }
    }
}

/// Core event-ingest step used by the [`NetworkService`] actor's inbound dispatch:
/// dedups on `(origin_node_id, event_id)` and, for a new event, appends it to `store`.
/// A store-level `DuplicateEventId` is folded into the same idempotent dedup result.
fn ingest_event<S: EventStore>(
    seen_events: &mut SeenEventCache,
    store: &mut S,
    delivery_id: RouteDeliveryId,
    event: &EventEnvelope,
) -> Result<InboundOutcome, NetworkError> {
    let key = (event.origin_node_id.clone(), event.event_id.clone());
    if seen_events.contains(&key) {
        return Ok(InboundOutcome::EventDuplicate {
            delivery_id,
            event_id: event.event_id.clone(),
        });
    }
    match store.append(event.clone()) {
        Ok(_) => {
            seen_events.insert(key);
            Ok(InboundOutcome::EventAccepted {
                delivery_id,
                event_id: event.event_id.clone(),
            })
        }
        Err(EventStoreError::DuplicateEventId { .. }) => {
            seen_events.insert(key);
            Ok(InboundOutcome::EventDuplicate {
                delivery_id,
                event_id: event.event_id.clone(),
            })
        }
        Err(error) => Err(NetworkError::EventStore(error)),
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NetworkStreamId(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkStreamChunkId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkStreamOpen {
    pub stream_id: NetworkStreamId,
    pub protocol: String,
    #[serde(default, with = "serde_bytes")]
    pub metadata: Vec<u8>,
}

impl NetworkStreamOpen {
    pub fn encode_metadata<T: serde::Serialize + ?Sized>(
        stream_id: NetworkStreamId,
        protocol: impl Into<String>,
        metadata: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            stream_id,
            protocol: protocol.into(),
            metadata: crate::object::SinomoObjectCodec::default().encode(metadata)?,
        })
    }

    pub fn decode_metadata<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        crate::object::SinomoObjectCodec::default().decode(&self.metadata)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkStreamFrame {
    Open(NetworkStreamOpen),
    Data {
        stream_id: NetworkStreamId,
        chunk_id: NetworkStreamChunkId,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    End {
        stream_id: NetworkStreamId,
    },
    Reset {
        stream_id: NetworkStreamId,
        reason: String,
    },
}

pub trait NetworkStreamTransport: NetworkTransport {
    fn send_stream_frame(
        &mut self,
        target_node_id: &NodeId,
        frame: NetworkStreamFrame,
    ) -> Result<(), NetworkError>;

    fn drain_stream_frames(&mut self) -> Result<Vec<(NodeId, NetworkStreamFrame)>, NetworkError>;
}

#[derive(Clone, Debug, Default)]
pub struct InProcessNetwork {
    inner: Arc<Mutex<InProcessNetworkState>>,
}

pub(crate) const IN_PROCESS_MAILBOX_CAPACITY: usize = 4096;
pub(crate) const DELIVERY_LEDGER_CAPACITY: usize = 8192;
pub(crate) const DELIVERY_LEDGER_TERMINAL_RECORD_CAP: usize = 1024;

impl InProcessNetwork {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&self, node_id: NodeId) -> InProcessTransport {
        self.inner
            .lock()
            .expect("in-process network lock")
            .mailboxes
            .entry(node_id.clone())
            .or_default();

        InProcessTransport {
            local_node_id: node_id,
            inner: Arc::clone(&self.inner),
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
    inner: Arc<Mutex<InProcessNetworkState>>,
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

        let mut inner = self.inner.lock().map_err(|_| NetworkError::Serialization {
            detail: "in-process network lock poisoned".to_string(),
        })?;
        let mailbox =
            inner
                .mailboxes
                .get_mut(target_node_id)
                .ok_or_else(|| NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                })?;
        if mailbox.len() >= IN_PROCESS_MAILBOX_CAPACITY {
            return Err(NetworkError::MailboxFull {
                node_id: target_node_id.clone(),
                max_frames: IN_PROCESS_MAILBOX_CAPACITY,
            });
        }

        mailbox.push_back(InboundFrame {
            received_by: target_node_id.clone(),
            frame,
        });
        Ok(())
    }

    fn drain_incoming(&mut self) -> Result<Vec<InboundFrame>, NetworkError> {
        let mut inner = self.inner.lock().map_err(|_| NetworkError::Serialization {
            detail: "in-process network lock poisoned".to_string(),
        })?;
        let mailbox = inner
            .mailboxes
            .get_mut(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;

        Ok(mailbox.drain(..).collect())
    }

    fn drain_incoming_limited(
        &mut self,
        max_frames: usize,
    ) -> Result<Vec<InboundFrame>, NetworkError> {
        let mut inner = self.inner.lock().map_err(|_| NetworkError::Serialization {
            detail: "in-process network lock poisoned".to_string(),
        })?;
        let mailbox = inner
            .mailboxes
            .get_mut(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;
        let take = mailbox.len().min(max_frames.max(1));

        Ok(mailbox.drain(..take).collect())
    }
}

#[derive(Clone, Debug)]
pub struct NetworkService<T> {
    router: InProcessRouter,
    pub(crate) transport: T,
    ledger: DeliveryLedger,
    seen_events: SeenEventCache,
    next_frame_id: u64,
    next_message_id: u64,
}

impl<T: NetworkTransport> NetworkService<T> {
    pub fn new(registry: PeerRegistry, transport: T) -> Self {
        Self {
            router: InProcessRouter::new(registry),
            transport,
            ledger: DeliveryLedger::new(RetryPolicy::default()),
            seen_events: SeenEventCache::new(),
            next_frame_id: 1,
            next_message_id: 1,
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.ledger.retry_policy = retry_policy;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_seen_event_cache_capacity(mut self, capacity: usize) -> Self {
        self.seen_events = SeenEventCache::with_capacity(capacity);
        self
    }

    fn registry(&self) -> &PeerRegistry {
        self.router.registry()
    }

    fn ledger(&self) -> &DeliveryLedger {
        &self.ledger
    }

    fn next_outbound_wake_at(&self) -> Option<Instant> {
        self.ledger.next_outbound_wake_at()
    }

    fn enqueue_event(
        &mut self,
        stored_event: &StoredEvent,
    ) -> Result<Vec<RouteDelivery>, NetworkError> {
        let deliveries = self.router.route_event(stored_event)?;
        let mut accepted = Vec::new();

        for mut delivery in deliveries {
            if delivery.target_node_id != *self.transport.local_node_id()
                && !self.registry().is_routable(&delivery.target_node_id)
            {
                delivery.status = DeliveryStatus::WaitingForPeer;
            }
            if self
                .ledger
                .enqueue(delivery.clone(), stored_event.event.clone())
                .map(|delivery_id| delivery_id.is_some())?
            {
                accepted.push(delivery);
            }
        }

        Ok(accepted)
    }

    fn pump_outbound(&mut self) -> Result<usize, NetworkError> {
        self.pump_outbound_limited(NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT)
    }

    fn pump_outbound_limited(&mut self, max_deliveries: usize) -> Result<usize, NetworkError> {
        self.ledger.expire_ack_deadlines_at(Instant::now());
        let mut sent = 0;
        let delivery_ids = self
            .ledger
            .ready_delivery_ids_limited(max_deliveries.max(1))
            .into_iter();

        for delivery_id in delivery_ids {
            let Some(record) = self.ledger.record(&delivery_id).cloned() else {
                continue;
            };

            if record.delivery.target_node_id != *self.transport.local_node_id()
                && !self.registry().is_routable(&record.delivery.target_node_id)
            {
                self.ledger.mark_waiting_for_peer(delivery_id)?;
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

    fn process_incoming<S: EventStore>(
        &mut self,
        store: &mut S,
    ) -> Result<Vec<InboundOutcome>, NetworkError> {
        let inbound_frames = self
            .transport
            .drain_incoming_limited(NETWORK_SERVICE_INBOUND_BATCH_LIMIT)?;
        let mut outcomes = Vec::with_capacity(inbound_frames.len());

        for inbound in inbound_frames {
            match inbound.frame.message.body {
                MeshMessageBody::Event { delivery_id, event } => {
                    let outcome = ingest_event(&mut self.seen_events, store, delivery_id, &event)?;
                    outcomes.push(outcome);

                    let event_id = event.event_id.clone();
                    let ack_target_node_id = inbound.frame.source_node_id.clone();
                    let ack = DeliveryAck {
                        delivery_id,
                        event_id: event_id.clone(),
                        target_node_id: self.transport.local_node_id().clone(),
                    };
                    let ack_frame = self.ack_frame(ack_target_node_id.clone(), ack);
                    if let Err(error) = self.transport.send(&ack_target_node_id, ack_frame) {
                        outcomes.push(InboundOutcome::AckSendFailed {
                            delivery_id,
                            event_id,
                            target_node_id: ack_target_node_id,
                            detail: error.to_string(),
                        });
                    }
                }
                MeshMessageBody::Ack(ack) => {
                    match self
                        .ledger
                        .ack(ack.delivery_id, &inbound.frame.source_node_id)
                    {
                        Ok(()) => outcomes.push(InboundOutcome::AckAccepted {
                            delivery_id: ack.delivery_id,
                            target_node_id: ack.target_node_id,
                        }),
                        Err(NetworkError::UnknownDelivery { delivery_id }) => {
                            outcomes.push(InboundOutcome::AckIgnored {
                                delivery_id,
                                target_node_id: ack.target_node_id,
                            });
                        }
                        Err(error) => return Err(error),
                    }
                }
                MeshMessageBody::Heartbeat(heartbeat) => {
                    let was_routable = self.registry().is_routable(&heartbeat.node_id);
                    let status = self
                        .router
                        .registry_mut()
                        .record_heartbeat(&heartbeat.node_id, heartbeat.tick)?;
                    let requeued = if was_routable {
                        self.ledger.requeue_target(&heartbeat.node_id)
                    } else {
                        self.ledger
                            .requeue_target_after_recovery(&heartbeat.node_id)
                    };
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

    fn send_heartbeat(&mut self, target_node_id: &NodeId, tick: u64) -> Result<(), NetworkError> {
        let heartbeat = Heartbeat {
            node_id: self.transport.local_node_id().clone(),
            tick,
        };
        let frame = self.heartbeat_frame(target_node_id.clone(), heartbeat);
        self.transport.send(target_node_id, frame)
    }

    fn record_missed_heartbeat(
        &mut self,
        target_node_id: &NodeId,
        policy: HeartbeatPolicy,
    ) -> Result<(PeerStatus, usize), NetworkError> {
        let status = self
            .router
            .registry_mut()
            .record_missed_heartbeat(target_node_id, policy)?;
        let waiting = if status.can_route_reactive() {
            0
        } else {
            self.ledger.mark_target_waiting_for_peer(target_node_id)
        };
        Ok((status, waiting))
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

/// Bounded command-queue depth for a spawned [`NetworkService`] actor. Once this many
/// commands are in flight, [`NetworkServiceHandle`] methods await room (backpressure).
const NETWORK_SERVICE_QUEUE_CAPACITY: usize = 256;
pub(crate) const NETWORK_SERVICE_INBOUND_BATCH_LIMIT: usize = 256;
pub(crate) const NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT: usize = 256;

/// A unit of work run on the actor's owned [`NetworkService`] and event store.
type NetworkServiceJob<T, S> = Box<dyn FnOnce(&mut NetworkService<T>, &mut S) + Send>;

impl<T: NetworkTransport + Send + 'static> NetworkService<T> {
    /// Run this service as an asynchronous actor. The actor owns the transport, mesh
    /// state, and event `store`, and serially drains/dispatches inbound frames, sends
    /// acks, and flushes outbound deliveries in its own task. All interaction goes
    /// through the returned [`NetworkServiceHandle`] over a bounded command queue —
    /// this is the single message path for the service.
    pub fn spawn<S: EventStore + Send + 'static>(self, store: S) -> NetworkServiceHandle<T, S> {
        let (commands, mut requests) =
            tokio::sync::mpsc::channel::<NetworkServiceJob<T, S>>(NETWORK_SERVICE_QUEUE_CAPACITY);
        let task = tokio::spawn(async move {
            let mut service = self;
            let mut store = store;
            while let Some(job) = requests.recv().await {
                job(&mut service, &mut store);
            }
            store
        });
        NetworkServiceHandle { commands, task }
    }
}

/// Handle to a [`NetworkService`] running as an async actor (see
/// [`NetworkService::spawn`]). The actor processes commands — and the inbound frames it
/// drains from the transport — serially in its own task, so the shared mesh state needs
/// no locking. Each method enqueues one command on the bounded queue and awaits its
/// reply; backpressure falls out of the bounded queue when commands outpace the actor.
pub struct NetworkServiceHandle<T, S> {
    commands: tokio::sync::mpsc::Sender<NetworkServiceJob<T, S>>,
    task: tokio::task::JoinHandle<S>,
}

impl<T, S> NetworkServiceHandle<T, S>
where
    T: NetworkTransport + Send + 'static,
    S: EventStore + Send + 'static,
{
    async fn run_job<R, F>(&self, job: F) -> Result<R, NetworkError>
    where
        R: Send + 'static,
        F: FnOnce(&mut NetworkService<T>, &mut S) -> R + Send + 'static,
    {
        let (reply, response) = tokio::sync::oneshot::channel();
        let request: NetworkServiceJob<T, S> = Box::new(move |service, store| {
            let _ = reply.send(job(service, store));
        });
        self.commands
            .send(request)
            .await
            .map_err(|_| network_service_stopped())?;
        response.await.map_err(|_| network_service_stopped())
    }

    /// Route `stored_event` to its targets and accept the resulting deliveries.
    pub async fn enqueue_event(
        &self,
        stored_event: StoredEvent,
    ) -> Result<Vec<RouteDelivery>, NetworkError> {
        self.run_job(move |service, _store| service.enqueue_event(&stored_event))
            .await?
    }

    /// Flush ready outbound deliveries through the transport, returning the count sent.
    pub async fn pump_outbound(&self) -> Result<usize, NetworkError> {
        self.run_job(|service, _store| service.pump_outbound())
            .await?
    }

    /// Drain the transport and dispatch every pending inbound frame (events, acks,
    /// heartbeats, protocol), returning the outcomes in arrival order.
    pub async fn process_incoming(&self) -> Result<Vec<InboundOutcome>, NetworkError> {
        self.run_job(|service, store| service.process_incoming(store))
            .await?
    }

    /// Send a heartbeat to `target_node_id`.
    pub async fn send_heartbeat(
        &self,
        target_node_id: NodeId,
        tick: u64,
    ) -> Result<(), NetworkError> {
        self.run_job(move |service, _store| service.send_heartbeat(&target_node_id, tick))
            .await?
    }

    /// Record a missed heartbeat for `target_node_id` and park its pending deliveries
    /// when the peer crosses into a non-routable state.
    pub async fn record_missed_heartbeat(
        &self,
        target_node_id: NodeId,
        policy: HeartbeatPolicy,
    ) -> Result<(PeerStatus, usize), NetworkError> {
        self.run_job(move |service, _store| {
            service.record_missed_heartbeat(&target_node_id, policy)
        })
        .await?
    }

    /// Snapshot the delivery-ledger records.
    pub async fn ledger_records(&self) -> Result<Vec<DeliveryRecord>, NetworkError> {
        self.run_job(|service, _store| service.ledger().records().into_iter().cloned().collect())
            .await
    }

    /// Earliest time a future [`NetworkServiceHandle::pump_outbound`] could make
    /// progress for queued or retrying work. Schedulers can use this to sleep through
    /// retry backoff windows instead of polling while no delivery is ready.
    pub async fn next_outbound_wake_at(&self) -> Result<Option<Instant>, NetworkError> {
        self.run_job(|service, _store| service.next_outbound_wake_at())
            .await
    }

    /// Stop the actor and return the owned event store for inspection.
    pub async fn shutdown(self) -> S {
        drop(self.commands);
        self.task.await.expect("network service actor panicked")
    }
}

fn network_service_stopped() -> NetworkError {
    NetworkError::Io {
        operation: "network-service-command".to_string(),
        detail: "network service actor is no longer running".to_string(),
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
    AckIgnored {
        delivery_id: RouteDeliveryId,
        target_node_id: NodeId,
    },
    AckSendFailed {
        delivery_id: RouteDeliveryId,
        event_id: EventId,
        target_node_id: NodeId,
        detail: String,
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
    capacity: usize,
    records: BTreeMap<RouteDeliveryId, DeliveryRecord>,
    active_by_event_target: BTreeMap<(EventId, NodeId), RouteDeliveryId>,
    retry_not_before: BTreeMap<RouteDeliveryId, Instant>,
    ack_deadline: BTreeMap<RouteDeliveryId, Instant>,
}

impl DeliveryLedger {
    pub fn new(retry_policy: RetryPolicy) -> Self {
        Self::with_capacity(retry_policy, DELIVERY_LEDGER_CAPACITY)
    }

    pub(crate) fn with_capacity(retry_policy: RetryPolicy, capacity: usize) -> Self {
        Self {
            retry_policy,
            capacity: capacity.max(1),
            records: BTreeMap::new(),
            active_by_event_target: BTreeMap::new(),
            retry_not_before: BTreeMap::new(),
            ack_deadline: BTreeMap::new(),
        }
    }

    pub fn enqueue(
        &mut self,
        delivery: RouteDelivery,
        event: EventEnvelope,
    ) -> Result<Option<RouteDeliveryId>, NetworkError> {
        let key = (delivery.event_id.clone(), delivery.target_node_id.clone());
        if self.active_by_event_target.contains_key(&key) {
            return Ok(None);
        }
        self.prune_terminal_records_to_capacity();
        if self.records.len() >= self.capacity {
            return Err(NetworkError::DeliveryLedgerFull {
                max_records: self.capacity,
            });
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
        Ok(Some(delivery_id))
    }

    pub fn record(&self, delivery_id: &RouteDeliveryId) -> Option<&DeliveryRecord> {
        self.records.get(delivery_id)
    }

    pub fn records(&self) -> Vec<&DeliveryRecord> {
        self.records.values().collect()
    }

    pub fn ready_delivery_ids(&self) -> Vec<RouteDeliveryId> {
        self.ready_delivery_ids_at(Instant::now())
    }

    pub(crate) fn ready_delivery_ids_at(&self, now: Instant) -> Vec<RouteDeliveryId> {
        self.ready_delivery_ids_limited_at(now, usize::MAX)
    }

    pub(crate) fn ready_delivery_ids_limited(&self, max_deliveries: usize) -> Vec<RouteDeliveryId> {
        self.ready_delivery_ids_limited_at(Instant::now(), max_deliveries)
    }

    pub(crate) fn ready_delivery_ids_limited_at(
        &self,
        now: Instant,
        max_deliveries: usize,
    ) -> Vec<RouteDeliveryId> {
        let max_deliveries = max_deliveries.max(1);
        let mut ready_by_priority = [
            Vec::with_capacity(max_deliveries.min(NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT)),
            Vec::with_capacity(max_deliveries.min(NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT)),
            Vec::with_capacity(max_deliveries.min(NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT)),
            Vec::with_capacity(max_deliveries.min(NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT)),
        ];
        for (delivery_id, record) in &self.records {
            if !delivery_is_ready(record, *delivery_id, &self.retry_not_before, now) {
                continue;
            }
            let priority = latency_class_ready_priority(&record.delivery.latency_class) as usize;
            if ready_by_priority[priority].len() < max_deliveries {
                ready_by_priority[priority].push(*delivery_id);
            }
        }

        let mut ready = Vec::with_capacity(max_deliveries.min(self.records.len()));
        for bucket in ready_by_priority {
            let remaining = max_deliveries.saturating_sub(ready.len());
            if remaining == 0 {
                break;
            }
            ready.extend(bucket.into_iter().take(remaining));
        }
        ready
    }

    pub fn next_outbound_wake_at(&self) -> Option<Instant> {
        self.next_outbound_wake_at_at(Instant::now())
    }

    pub(crate) fn next_outbound_wake_at_at(&self, now: Instant) -> Option<Instant> {
        let mut next_wake: Option<Instant> = None;
        for (delivery_id, record) in &self.records {
            match record.delivery.status {
                DeliveryStatus::Queued => return Some(now),
                DeliveryStatus::Retrying => match self.retry_not_before.get(delivery_id).copied() {
                    Some(deadline) if deadline > now => {
                        next_wake =
                            Some(next_wake.map_or(deadline, |existing| existing.min(deadline)));
                    }
                    _ => return Some(now),
                },
                DeliveryStatus::Sent => match self.ack_deadline.get(delivery_id).copied() {
                    Some(deadline) if deadline > now => {
                        next_wake =
                            Some(next_wake.map_or(deadline, |existing| existing.min(deadline)));
                    }
                    _ => return Some(now),
                },
                _ => {}
            }
        }
        next_wake
    }

    pub fn mark_sent(&mut self, delivery_id: RouteDeliveryId) -> Result<(), NetworkError> {
        self.mark_sent_at(delivery_id, Instant::now())
    }

    pub(crate) fn mark_sent_at(
        &mut self,
        delivery_id: RouteDeliveryId,
        now: Instant,
    ) -> Result<(), NetworkError> {
        let record = self
            .records
            .get_mut(&delivery_id)
            .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
        record.attempts += 1;
        record.delivery.status = DeliveryStatus::Sent;
        self.retry_not_before.remove(&delivery_id);
        self.ack_deadline
            .insert(delivery_id, now + ack_timeout_for_record(record));
        Ok(())
    }

    pub fn mark_for_retry(&mut self, delivery_id: RouteDeliveryId) -> Result<(), NetworkError> {
        let record = self
            .records
            .get_mut(&delivery_id)
            .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
        if record.delivery.status != DeliveryStatus::Delivered {
            record.delivery.status = DeliveryStatus::Retrying;
            self.retry_not_before.remove(&delivery_id);
            self.ack_deadline.remove(&delivery_id);
        }
        Ok(())
    }

    pub fn mark_waiting_for_peer(
        &mut self,
        delivery_id: RouteDeliveryId,
    ) -> Result<(), NetworkError> {
        let record = self
            .records
            .get_mut(&delivery_id)
            .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
        if !record.delivery.status.is_terminal() {
            record.delivery.status = DeliveryStatus::WaitingForPeer;
            self.retry_not_before.remove(&delivery_id);
            self.ack_deadline.remove(&delivery_id);
        }
        Ok(())
    }

    pub fn mark_attempt_failed(
        &mut self,
        delivery_id: RouteDeliveryId,
    ) -> Result<(), NetworkError> {
        self.mark_attempt_failed_at(delivery_id, Instant::now())
    }

    pub(crate) fn mark_attempt_failed_at(
        &mut self,
        delivery_id: RouteDeliveryId,
        now: Instant,
    ) -> Result<(), NetworkError> {
        let record = self
            .records
            .get_mut(&delivery_id)
            .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
        record.attempts += 1;
        if record.attempts >= self.retry_policy.max_attempts {
            record.delivery.status = DeliveryStatus::Failed;
            self.retry_not_before.remove(&delivery_id);
            self.ack_deadline.remove(&delivery_id);
        } else {
            record.delivery.status = DeliveryStatus::Retrying;
            let delay = retry_delay_for_record(record);
            self.retry_not_before.insert(delivery_id, now + delay);
            self.ack_deadline.remove(&delivery_id);
        }
        self.prune_terminal_records_to_retention();
        Ok(())
    }

    pub(crate) fn expire_ack_deadlines_at(&mut self, now: Instant) -> usize {
        let expired = self
            .ack_deadline
            .iter()
            .filter_map(|(delivery_id, deadline)| (*deadline <= now).then_some(*delivery_id))
            .collect::<Vec<_>>();
        let mut expired_count = 0;
        for delivery_id in expired {
            let Some(record) = self.records.get_mut(&delivery_id) else {
                self.ack_deadline.remove(&delivery_id);
                continue;
            };
            if record.delivery.status != DeliveryStatus::Sent {
                self.ack_deadline.remove(&delivery_id);
                continue;
            }
            if record.attempts >= self.retry_policy.max_attempts {
                record.delivery.status = DeliveryStatus::Failed;
                self.retry_not_before.remove(&delivery_id);
            } else {
                record.delivery.status = DeliveryStatus::Retrying;
                let delay = retry_delay_for_record(record);
                self.retry_not_before.insert(delivery_id, now + delay);
            }
            self.ack_deadline.remove(&delivery_id);
            expired_count += 1;
        }
        if expired_count > 0 {
            self.prune_terminal_records_to_retention();
        }
        expired_count
    }

    pub fn requeue_target(&mut self, target_node_id: &NodeId) -> usize {
        self.requeue_target_matching(target_node_id, false)
    }

    pub fn requeue_target_after_recovery(&mut self, target_node_id: &NodeId) -> usize {
        self.requeue_target_matching(target_node_id, true)
    }

    fn requeue_target_matching(
        &mut self,
        target_node_id: &NodeId,
        include_in_flight: bool,
    ) -> usize {
        let mut requeued = 0;
        for (delivery_id, record) in self.records.iter_mut() {
            if &record.delivery.target_node_id != target_node_id {
                continue;
            }
            let should_requeue = matches!(
                record.delivery.status,
                DeliveryStatus::Failed | DeliveryStatus::WaitingForPeer
            ) || (include_in_flight
                && record.delivery.status == DeliveryStatus::Sent);
            if should_requeue {
                record.delivery.status = DeliveryStatus::Retrying;
                record.attempts = 0;
                self.retry_not_before.remove(delivery_id);
                self.ack_deadline.remove(delivery_id);
                requeued += 1;
            }
        }
        requeued
    }

    pub fn mark_target_waiting_for_peer(&mut self, target_node_id: &NodeId) -> usize {
        let mut waiting = 0;
        for (delivery_id, record) in self.records.iter_mut() {
            if &record.delivery.target_node_id == target_node_id
                && !record.delivery.status.is_terminal()
                && record.delivery.status != DeliveryStatus::WaitingForPeer
            {
                record.delivery.status = DeliveryStatus::WaitingForPeer;
                self.retry_not_before.remove(delivery_id);
                self.ack_deadline.remove(delivery_id);
                waiting += 1;
            }
        }
        waiting
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
        self.retry_not_before.remove(&delivery_id);
        self.ack_deadline.remove(&delivery_id);
        self.prune_terminal_records_to_retention();
        Ok(())
    }

    fn retained_terminal_record_limit(&self) -> usize {
        (self.capacity / 4)
            .max(1)
            .min(DELIVERY_LEDGER_TERMINAL_RECORD_CAP)
    }

    fn prune_terminal_records_to_retention(&mut self) {
        let limit = self.retained_terminal_record_limit();
        loop {
            let terminal_count = self
                .records
                .values()
                .filter(|record| record.delivery.status.is_terminal())
                .count();
            if terminal_count <= limit {
                return;
            }
            let Some(delivery_id) = self.records.iter().find_map(|(delivery_id, record)| {
                record.delivery.status.is_terminal().then_some(*delivery_id)
            }) else {
                return;
            };
            self.remove_record(delivery_id);
        }
    }

    fn prune_terminal_records_to_capacity(&mut self) {
        while self.records.len() >= self.capacity {
            let Some(delivery_id) = self.records.iter().find_map(|(delivery_id, record)| {
                record.delivery.status.is_terminal().then_some(*delivery_id)
            }) else {
                return;
            };
            self.remove_record(delivery_id);
        }
    }

    fn remove_record(&mut self, delivery_id: RouteDeliveryId) -> Option<DeliveryRecord> {
        let record = self.records.remove(&delivery_id)?;
        self.retry_not_before.remove(&delivery_id);
        self.ack_deadline.remove(&delivery_id);
        self.active_by_event_target.remove(&(
            record.delivery.event_id.clone(),
            record.delivery.target_node_id.clone(),
        ));
        Some(record)
    }
}

const MAX_DELIVERY_RETRY_BACKOFF: Duration = Duration::from_secs(30);
const MAX_DELIVERY_RETRY_JITTER: Duration = Duration::from_secs(2);

fn latency_class_ready_priority(latency_class: &LatencyClass) -> u8 {
    match latency_class {
        LatencyClass::Reactive => 0,
        LatencyClass::Interactive => 1,
        LatencyClass::Background => 2,
        LatencyClass::Archive => 3,
    }
}

pub(crate) fn ack_timeout_for_latency_class(latency_class: &LatencyClass) -> Duration {
    match latency_class {
        LatencyClass::Reactive => Duration::from_millis(250),
        LatencyClass::Interactive => Duration::from_secs(1),
        LatencyClass::Background => Duration::from_secs(10),
        LatencyClass::Archive => Duration::from_secs(30),
    }
}

fn ack_timeout_for_record(record: &DeliveryRecord) -> Duration {
    ack_timeout_for_latency_class(&record.delivery.latency_class)
}

fn delivery_is_ready(
    record: &DeliveryRecord,
    delivery_id: RouteDeliveryId,
    retry_not_before: &BTreeMap<RouteDeliveryId, Instant>,
    now: Instant,
) -> bool {
    match record.delivery.status {
        DeliveryStatus::Queued => true,
        DeliveryStatus::Retrying => retry_not_before
            .get(&delivery_id)
            .is_none_or(|deadline| *deadline <= now),
        _ => false,
    }
}

fn retry_delay_for_record(record: &DeliveryRecord) -> Duration {
    let base = match record.delivery.latency_class {
        LatencyClass::Reactive => Duration::from_millis(25),
        LatencyClass::Interactive => Duration::from_millis(100),
        LatencyClass::Background => Duration::from_secs(1),
        LatencyClass::Archive => Duration::from_secs(5),
    };
    let shift = record.attempts.saturating_sub(1).min(8) as u32;
    let backoff = base
        .saturating_mul(2_u32.saturating_pow(shift))
        .min(MAX_DELIVERY_RETRY_BACKOFF);
    backoff + retry_jitter_for_record(record, backoff)
}

fn retry_jitter_for_record(record: &DeliveryRecord, backoff: Duration) -> Duration {
    if !matches!(
        record.delivery.latency_class,
        LatencyClass::Background | LatencyClass::Archive
    ) {
        return Duration::ZERO;
    }

    let jitter_window_ms =
        ((backoff.as_millis() / 10) as u64).min(MAX_DELIVERY_RETRY_JITTER.as_millis() as u64);
    if jitter_window_ms == 0 {
        return Duration::ZERO;
    }
    let jitter_ms = record.delivery.delivery_id.0.wrapping_mul(37) % (jitter_window_ms + 1);
    Duration::from_millis(jitter_ms)
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
    pub event_id: EventId,
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
    WaitingForPeer,
    Delivered,
    Failed,
}

impl DeliveryStatus {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Delivered | Self::Failed)
    }
}
