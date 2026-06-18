use crate::error::NetworkError;
use crate::prelude::*;
use crate::rpc::ObjectRpcRequest;
use arc_swap::ArcSwap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Condvar;
use std::time::Instant;

#[derive(Clone, Debug, Default)]
pub struct PeerRegistry {
    cluster_id: Option<ClusterId>,
    nodes: BTreeMap<NodeId, NodeProfile>,
    cluster_members: BTreeMap<NodeId, ClusterMemberRecord>,
    peer_states: BTreeMap<NodeId, PeerState>,
    endpoint_candidates: BTreeMap<NodeId, Vec<PeerEndpointCandidate>>,
    sessions: BTreeMap<SessionId, BTreeSet<NodeId>>,
    sessions_by_node: BTreeMap<NodeId, BTreeSet<SessionId>>,
    routable_nodes: BTreeSet<NodeId>,
    routable_sessions: BTreeMap<SessionId, BTreeSet<NodeId>>,
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
        let can_route = self
            .peer_states
            .entry(node_id.clone())
            .or_insert_with(|| PeerState::new(NodeEndpoint::in_process(node_id.clone())))
            .status
            .can_route_reactive();
        self.set_node_routable_index(&node_id, can_route);
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
            .and_then(|candidates| {
                candidates
                    .iter()
                    .filter(|candidate| candidate.status != EndpointCandidateStatus::Failed)
                    .min_by_key(|candidate| candidate.score)
            })
    }

    pub fn join_session(
        &mut self,
        session_id: SessionId,
        node_id: NodeId,
    ) -> Result<(), NetworkError> {
        if !self.nodes.contains_key(&node_id) {
            return Err(NetworkError::UnknownNode { node_id });
        }

        self.sessions
            .entry(session_id.clone())
            .or_default()
            .insert(node_id.clone());
        self.sessions_by_node
            .entry(node_id.clone())
            .or_default()
            .insert(session_id.clone());
        if self.is_routable(&node_id) {
            self.routable_sessions
                .entry(session_id)
                .or_default()
                .insert(node_id);
        }
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
        let status = state.status.clone();
        self.set_node_routable_index(node_id, true);
        Ok(status)
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
        let status = if state.link.missed_heartbeats >= policy.offline_after_missed {
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
        state.status = status.clone();

        self.set_node_routable_index(node_id, status.can_route_reactive());
        Ok(status)
    }

    pub fn session_nodes(&self, session_id: &SessionId) -> Vec<NodeId> {
        self.sessions
            .get(session_id)
            .map(|nodes| nodes.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn routable_session_nodes(&self, session_id: &SessionId) -> Vec<NodeId> {
        self.routable_sessions
            .get(session_id)
            .map(|nodes| nodes.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn routable_network_nodes(&self) -> Vec<NodeId> {
        self.routable_nodes.iter().cloned().collect()
    }

    fn set_node_routable_index(&mut self, node_id: &NodeId, can_route: bool) {
        if can_route {
            self.routable_nodes.insert(node_id.clone());
        } else {
            self.routable_nodes.remove(node_id);
        }
        for session_id in self
            .sessions_by_node
            .get(node_id)
            .into_iter()
            .flat_map(|sessions| sessions.iter())
            .cloned()
            .collect::<Vec<_>>()
        {
            if can_route {
                self.routable_sessions
                    .entry(session_id)
                    .or_default()
                    .insert(node_id.clone());
            } else {
                let should_remove = if let Some(nodes) = self.routable_sessions.get_mut(&session_id)
                {
                    nodes.remove(node_id);
                    nodes.is_empty()
                } else {
                    false
                };
                if should_remove {
                    self.routable_sessions.remove(&session_id);
                }
            }
        }
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InProcessObjectRpcMessage {
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub request: ObjectRpcRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InboundObjectRpcMessage {
    pub received_by: NodeId,
    pub message: InProcessObjectRpcMessage,
}

pub type AsyncNetworkTransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AsyncNetworkSendOptions {
    timeout: Option<Duration>,
    nonblocking: bool,
}

impl AsyncNetworkSendOptions {
    pub const fn timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            nonblocking: false,
        }
    }

    pub fn from_timeout(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            nonblocking: false,
        }
    }

    pub const fn nonblocking() -> Self {
        Self {
            timeout: None,
            nonblocking: true,
        }
    }

    pub const fn timeout_duration(self) -> Option<Duration> {
        self.timeout
    }

    pub const fn is_nonblocking(self) -> bool {
        self.nonblocking
    }
}

pub trait AsyncNetworkTransport {
    fn local_node_id(&self) -> &NodeId;

    fn send_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        frame: NetworkFrame,
        options: AsyncNetworkSendOptions,
    ) -> AsyncNetworkTransportFuture<'a, Result<(), NetworkError>>;

    fn drain_incoming_async<'a>(
        &'a mut self,
        max_frames: usize,
    ) -> AsyncNetworkTransportFuture<'a, Result<Vec<InboundFrame>, NetworkError>>;

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> AsyncNetworkTransportFuture<'a, Result<Option<usize>, NetworkError>>;

    fn wait_for_incoming_async<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> AsyncNetworkTransportFuture<'a, Result<bool, NetworkError>>;
}

pub(crate) const SEEN_EVENT_CACHE_CAPACITY: usize = 16_384;

#[derive(Clone, Debug)]
pub(crate) struct SeenEventCache {
    capacity: usize,
    keys: HashSet<(NodeId, EventId)>,
    order: VecDeque<(NodeId, EventId)>,
}

impl SeenEventCache {
    pub(crate) fn new() -> Self {
        Self::with_capacity(SEEN_EVENT_CACHE_CAPACITY)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            keys: HashSet::new(),
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
fn event_addresses_local_node(event: &EventEnvelope, local_node_id: &NodeId) -> bool {
    if let Some(target_node_id) = &event.target_node_id {
        return target_node_id == local_node_id;
    }

    match &event.routing.scope {
        RoutingScope::Local => &event.origin_node_id == local_node_id,
        RoutingScope::Session => {
            event.session_id.is_some() || &event.origin_node_id == local_node_id
        }
        RoutingScope::TrustedNetwork | RoutingScope::DurableGlobal => true,
    }
}

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

/// Synchronous in-memory frame harness.
///
/// This remains available for DFS and tests that drive frames manually. It is
/// intentionally not an [`AsyncNetworkTransport`]; actor code uses
/// [`AsyncInProcessNetwork`] or another async transport.
#[derive(Clone, Debug, Default)]
pub struct InProcessNetwork {
    inner: Arc<InProcessNetworkInner>,
}

pub(crate) const IN_PROCESS_MAILBOX_CAPACITY: usize = 4096;
pub(crate) const DELIVERY_LEDGER_CAPACITY: usize = 8192;
pub(crate) const DELIVERY_LEDGER_TERMINAL_RECORD_CAP: usize = 1024;
const LATENCY_CLASS_READY_PRIORITY_COUNT: usize = 4;

impl InProcessNetwork {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&self, node_id: NodeId) -> InProcessTransport {
        let mut state = self.inner.state.lock().expect("in-process network lock");
        state.mailboxes.entry(node_id.clone()).or_default();
        state
            .object_rpc_mailboxes
            .entry(node_id.clone())
            .or_default();
        drop(state);

        InProcessTransport {
            local_node_id: node_id,
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug, Default)]
struct InProcessNetworkInner {
    state: Mutex<InProcessNetworkState>,
    incoming: Condvar,
}

#[derive(Clone, Debug, Default)]
struct InProcessNetworkState {
    mailboxes: BTreeMap<NodeId, VecDeque<InboundFrame>>,
    object_rpc_mailboxes: BTreeMap<NodeId, VecDeque<InboundObjectRpcMessage>>,
}

#[derive(Clone, Debug)]
pub struct InProcessTransport {
    local_node_id: NodeId,
    inner: Arc<InProcessNetworkInner>,
}

impl InProcessTransport {
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub fn send(
        &mut self,
        target_node_id: &NodeId,
        frame: NetworkFrame,
    ) -> Result<(), NetworkError> {
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

        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
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
        drop(inner);
        self.inner.incoming.notify_all();
        Ok(())
    }

    pub fn send_object_rpc_request(
        &mut self,
        target_node_id: &NodeId,
        request: ObjectRpcRequest,
    ) -> Result<(), NetworkError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
                detail: "in-process network lock poisoned".to_string(),
            })?;
        let mailbox = inner
            .object_rpc_mailboxes
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

        mailbox.push_back(InboundObjectRpcMessage {
            received_by: target_node_id.clone(),
            message: InProcessObjectRpcMessage {
                source_node_id: self.local_node_id.clone(),
                target_node_id: target_node_id.clone(),
                request,
            },
        });
        drop(inner);
        self.inner.incoming.notify_all();
        Ok(())
    }

    pub fn drain_incoming(&mut self) -> Result<Vec<InboundFrame>, NetworkError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
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

    pub fn drain_incoming_object_rpc_requests(
        &mut self,
    ) -> Result<Vec<InboundObjectRpcMessage>, NetworkError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
                detail: "in-process network lock poisoned".to_string(),
            })?;
        let mailbox = inner
            .object_rpc_mailboxes
            .get_mut(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;

        Ok(mailbox.drain(..).collect())
    }

    pub fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        let inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
                detail: "in-process network lock poisoned".to_string(),
            })?;
        let mailbox =
            inner
                .mailboxes
                .get(&self.local_node_id)
                .ok_or_else(|| NetworkError::UnknownNode {
                    node_id: self.local_node_id.clone(),
                })?;
        Ok(Some(mailbox.len()))
    }

    pub fn pending_incoming_object_rpc_request_count(&self) -> Result<Option<usize>, NetworkError> {
        let inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
                detail: "in-process network lock poisoned".to_string(),
            })?;
        let mailbox = inner
            .object_rpc_mailboxes
            .get(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;
        Ok(Some(mailbox.len()))
    }

    pub fn wait_for_incoming(&self, timeout: Duration) -> Result<bool, NetworkError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
                detail: "in-process network lock poisoned".to_string(),
            })?;
        if !inner.mailboxes.contains_key(&self.local_node_id) {
            return Err(NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            });
        }
        if inner
            .mailboxes
            .get(&self.local_node_id)
            .is_some_and(|mailbox| !mailbox.is_empty())
            || timeout.is_zero()
        {
            return Ok(inner
                .mailboxes
                .get(&self.local_node_id)
                .is_some_and(|mailbox| !mailbox.is_empty()));
        }
        let local_node_id = self.local_node_id.clone();
        let (guard, _timeout) = self
            .inner
            .incoming
            .wait_timeout_while(inner, timeout, |state| {
                state
                    .mailboxes
                    .get(&local_node_id)
                    .is_some_and(|mailbox| mailbox.is_empty())
            })
            .map_err(|_| NetworkError::Serialization {
                detail: "in-process network lock poisoned".to_string(),
            })?;
        inner = guard;
        let mailbox =
            inner
                .mailboxes
                .get(&self.local_node_id)
                .ok_or_else(|| NetworkError::UnknownNode {
                    node_id: self.local_node_id.clone(),
                })?;
        Ok(!mailbox.is_empty())
    }

    pub fn wait_for_incoming_object_rpc_request(
        &self,
        timeout: Duration,
    ) -> Result<bool, NetworkError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
                detail: "in-process network lock poisoned".to_string(),
            })?;
        if !inner.object_rpc_mailboxes.contains_key(&self.local_node_id) {
            return Err(NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            });
        }
        if inner
            .object_rpc_mailboxes
            .get(&self.local_node_id)
            .is_some_and(|mailbox| !mailbox.is_empty())
            || timeout.is_zero()
        {
            return Ok(inner
                .object_rpc_mailboxes
                .get(&self.local_node_id)
                .is_some_and(|mailbox| !mailbox.is_empty()));
        }
        let local_node_id = self.local_node_id.clone();
        let (guard, _timeout) = self
            .inner
            .incoming
            .wait_timeout_while(inner, timeout, |state| {
                state
                    .object_rpc_mailboxes
                    .get(&local_node_id)
                    .is_some_and(|mailbox| mailbox.is_empty())
            })
            .map_err(|_| NetworkError::Serialization {
                detail: "in-process network lock poisoned".to_string(),
            })?;
        inner = guard;
        let mailbox = inner
            .object_rpc_mailboxes
            .get(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;
        Ok(!mailbox.is_empty())
    }

    pub fn drain_incoming_limited(
        &mut self,
        max_frames: usize,
    ) -> Result<Vec<InboundFrame>, NetworkError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| NetworkError::Serialization {
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

#[derive(Clone)]
pub struct AsyncInProcessNetwork {
    inner: Arc<AsyncInProcessNetworkInner>,
}

impl Default for AsyncInProcessNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncInProcessNetwork {
    pub fn new() -> Self {
        Self::with_mailbox_capacity(IN_PROCESS_MAILBOX_CAPACITY)
    }

    pub fn with_mailbox_capacity(mailbox_capacity: usize) -> Self {
        Self::with_mailbox_capacity_and_per_target_send_limit(mailbox_capacity, mailbox_capacity)
    }

    pub fn with_mailbox_capacity_and_per_target_send_limit(
        mailbox_capacity: usize,
        per_target_send_limit: usize,
    ) -> Self {
        Self {
            inner: Arc::new(AsyncInProcessNetworkInner {
                mailbox_capacity: mailbox_capacity.max(1),
                per_target_send_limit: per_target_send_limit.max(1),
                targets: ArcSwap::from_pointee(BTreeMap::new()),
                target_update_lock: Mutex::new(()),
            }),
        }
    }

    pub fn add_node(&self, node_id: NodeId) -> AsyncInProcessTransport {
        let (sender, incoming) = tokio::sync::mpsc::channel(self.inner.mailbox_capacity);
        let (object_rpc_sender, object_rpc_incoming) =
            tokio::sync::mpsc::channel(self.inner.mailbox_capacity);
        let target = AsyncInProcessTarget {
            sender,
            object_rpc_sender,
            mailbox_capacity: self.inner.mailbox_capacity,
            send_limiter: Arc::new(tokio::sync::Semaphore::new(
                self.inner.per_target_send_limit,
            )),
            object_rpc_send_limiter: Arc::new(tokio::sync::Semaphore::new(
                self.inner.per_target_send_limit,
            )),
        };
        let _guard = self
            .inner
            .target_update_lock
            .lock()
            .expect("async in-process network target update lock");
        let mut targets = (*self.inner.targets.load_full()).clone();
        targets.insert(node_id.clone(), target);
        self.inner.targets.store(Arc::new(targets));

        AsyncInProcessTransport {
            local_node_id: node_id,
            inner: Arc::clone(&self.inner),
            incoming,
            pending: VecDeque::new(),
            object_rpc_incoming,
            object_rpc_pending: VecDeque::new(),
        }
    }
}

struct AsyncInProcessNetworkInner {
    mailbox_capacity: usize,
    per_target_send_limit: usize,
    targets: ArcSwap<BTreeMap<NodeId, AsyncInProcessTarget>>,
    target_update_lock: Mutex<()>,
}

#[derive(Clone)]
struct AsyncInProcessTarget {
    sender: tokio::sync::mpsc::Sender<InboundFrame>,
    object_rpc_sender: tokio::sync::mpsc::Sender<InboundObjectRpcMessage>,
    mailbox_capacity: usize,
    send_limiter: Arc<tokio::sync::Semaphore>,
    object_rpc_send_limiter: Arc<tokio::sync::Semaphore>,
}

pub struct AsyncInProcessTransport {
    local_node_id: NodeId,
    inner: Arc<AsyncInProcessNetworkInner>,
    incoming: tokio::sync::mpsc::Receiver<InboundFrame>,
    pending: VecDeque<InboundFrame>,
    object_rpc_incoming: tokio::sync::mpsc::Receiver<InboundObjectRpcMessage>,
    object_rpc_pending: VecDeque<InboundObjectRpcMessage>,
}

impl AsyncInProcessTransport {
    fn validate_outbound_frame(
        &self,
        target_node_id: &NodeId,
        frame: &NetworkFrame,
    ) -> Result<(), NetworkError> {
        if frame.source_node_id != self.local_node_id {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: frame.source_node_id.clone(),
            });
        }

        if &frame.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: frame.target_node_id.clone(),
            });
        }

        Ok(())
    }

    fn target_channel(
        &self,
        target_node_id: &NodeId,
    ) -> Result<AsyncInProcessTarget, NetworkError> {
        self.inner
            .targets
            .load_full()
            .get(target_node_id)
            .cloned()
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })
    }

    fn pending_count(&self) -> usize {
        self.pending.len() + self.incoming.len()
    }

    fn pending_object_rpc_count(&self) -> usize {
        self.object_rpc_pending.len() + self.object_rpc_incoming.len()
    }

    pub async fn send_object_rpc_request_async(
        &mut self,
        target_node_id: &NodeId,
        request: ObjectRpcRequest,
        options: AsyncNetworkSendOptions,
    ) -> Result<(), NetworkError> {
        let target = self.target_channel(target_node_id)?;
        send_async_in_process_object_rpc_message(
            target,
            &self.local_node_id,
            target_node_id,
            request,
            options,
        )
        .await
    }

    pub async fn drain_incoming_object_rpc_requests(
        &mut self,
        max_messages: usize,
    ) -> Result<Vec<InboundObjectRpcMessage>, NetworkError> {
        let take = max_messages.max(1);
        let mut messages = self.object_rpc_pending.drain(..).collect::<Vec<_>>();
        if messages.len() > take {
            let retained = messages.split_off(take);
            self.object_rpc_pending.extend(retained);
            return Ok(messages);
        }
        while messages.len() < take {
            let Ok(message) = self.object_rpc_incoming.try_recv() else {
                break;
            };
            messages.push(message);
        }
        Ok(messages)
    }

    pub async fn pending_incoming_object_rpc_request_count(
        &mut self,
    ) -> Result<Option<usize>, NetworkError> {
        Ok(Some(self.pending_object_rpc_count()))
    }

    pub async fn wait_for_incoming_object_rpc_request(
        &mut self,
        timeout: Duration,
    ) -> Result<bool, NetworkError> {
        if self.pending_object_rpc_count() > 0 {
            return Ok(true);
        }
        if timeout.is_zero() {
            return Ok(false);
        }
        match tokio::time::timeout(timeout, self.object_rpc_incoming.recv()).await {
            Ok(Some(message)) => {
                self.object_rpc_pending.push_back(message);
                Ok(true)
            }
            Ok(None) => Err(NetworkError::Io {
                operation: "async in-process object rpc receive".to_string(),
                detail: format!("object rpc mailbox for {} is closed", self.local_node_id),
            }),
            Err(_) => Ok(false),
        }
    }
}

impl AsyncNetworkTransport for AsyncInProcessTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn send_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        frame: NetworkFrame,
        options: AsyncNetworkSendOptions,
    ) -> AsyncNetworkTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            self.validate_outbound_frame(target_node_id, &frame)?;
            let target = self.target_channel(target_node_id)?;
            send_async_in_process_frame(target, target_node_id, frame, options).await
        })
    }

    fn drain_incoming_async<'a>(
        &'a mut self,
        max_frames: usize,
    ) -> AsyncNetworkTransportFuture<'a, Result<Vec<InboundFrame>, NetworkError>> {
        Box::pin(async move {
            let take = max_frames.max(1);
            let mut frames = self.pending.drain(..).collect::<Vec<_>>();
            if frames.len() > take {
                let retained = frames.split_off(take);
                self.pending.extend(retained);
                return Ok(frames);
            }
            while frames.len() < take {
                let Ok(frame) = self.incoming.try_recv() else {
                    break;
                };
                frames.push(frame);
            }
            Ok(frames)
        })
    }

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> AsyncNetworkTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async move { Ok(Some(self.pending_count())) })
    }

    fn wait_for_incoming_async<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> AsyncNetworkTransportFuture<'a, Result<bool, NetworkError>> {
        Box::pin(async move {
            if self.pending_count() > 0 {
                return Ok(true);
            }
            if timeout.is_zero() {
                return Ok(false);
            }
            match tokio::time::timeout(timeout, self.incoming.recv()).await {
                Ok(Some(frame)) => {
                    self.pending.push_back(frame);
                    Ok(true)
                }
                Ok(None) => Err(NetworkError::Io {
                    operation: "async in-process receive".to_string(),
                    detail: format!("mailbox for {} is closed", self.local_node_id),
                }),
                Err(_) => Ok(false),
            }
        })
    }
}

async fn send_async_in_process_frame(
    target: AsyncInProcessTarget,
    target_node_id: &NodeId,
    frame: NetworkFrame,
    options: AsyncNetworkSendOptions,
) -> Result<(), NetworkError> {
    let inbound = InboundFrame {
        received_by: target_node_id.clone(),
        frame,
    };
    if options.is_nonblocking() {
        let _permit =
            target
                .send_limiter
                .try_acquire_owned()
                .map_err(|_| NetworkError::MailboxFull {
                    node_id: target_node_id.clone(),
                    max_frames: target.mailbox_capacity,
                })?;
        return target
            .sender
            .try_send(inbound)
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => NetworkError::MailboxFull {
                    node_id: target_node_id.clone(),
                    max_frames: target.mailbox_capacity,
                },
                tokio::sync::mpsc::error::TrySendError::Closed(_) => NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                },
            });
    }
    match options.timeout_duration() {
        Some(timeout) => {
            let deadline = tokio::time::Instant::now() + timeout;
            let _permit = tokio::time::timeout_at(deadline, target.send_limiter.acquire_owned())
                .await
                .map_err(|_| async_transport_send_timed_out(target_node_id, timeout))?
                .map_err(|source| NetworkError::Io {
                    operation: format!("async transport send to {target_node_id}"),
                    detail: format!("send limiter closed: {source}"),
                })?;
            tokio::time::timeout_at(deadline, target.sender.send(inbound))
                .await
                .map_err(|_| async_transport_send_timed_out(target_node_id, timeout))?
        }
        None => {
            let _permit = target
                .send_limiter
                .acquire_owned()
                .await
                .map_err(|source| NetworkError::Io {
                    operation: format!("async transport send to {target_node_id}"),
                    detail: format!("send limiter closed: {source}"),
                })?;
            target.sender.send(inbound).await
        }
    }
    .map_err(|_| NetworkError::UnknownNode {
        node_id: target_node_id.clone(),
    })
}

async fn send_async_in_process_object_rpc_message(
    target: AsyncInProcessTarget,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    request: ObjectRpcRequest,
    options: AsyncNetworkSendOptions,
) -> Result<(), NetworkError> {
    let inbound = InboundObjectRpcMessage {
        received_by: target_node_id.clone(),
        message: InProcessObjectRpcMessage {
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            request,
        },
    };
    if options.is_nonblocking() {
        let _permit = target
            .object_rpc_send_limiter
            .try_acquire_owned()
            .map_err(|_| NetworkError::MailboxFull {
                node_id: target_node_id.clone(),
                max_frames: target.mailbox_capacity,
            })?;
        return target
            .object_rpc_sender
            .try_send(inbound)
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => NetworkError::MailboxFull {
                    node_id: target_node_id.clone(),
                    max_frames: target.mailbox_capacity,
                },
                tokio::sync::mpsc::error::TrySendError::Closed(_) => NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                },
            });
    }
    match options.timeout_duration() {
        Some(timeout) => {
            let deadline = tokio::time::Instant::now() + timeout;
            let _permit =
                tokio::time::timeout_at(deadline, target.object_rpc_send_limiter.acquire_owned())
                    .await
                    .map_err(|_| async_transport_send_timed_out(target_node_id, timeout))?
                    .map_err(|source| NetworkError::Io {
                        operation: format!("async object rpc transport send to {target_node_id}"),
                        detail: format!("send limiter closed: {source}"),
                    })?;
            tokio::time::timeout_at(deadline, target.object_rpc_sender.send(inbound))
                .await
                .map_err(|_| async_transport_send_timed_out(target_node_id, timeout))?
        }
        None => {
            let _permit = target
                .object_rpc_send_limiter
                .acquire_owned()
                .await
                .map_err(|source| NetworkError::Io {
                    operation: format!("async object rpc transport send to {target_node_id}"),
                    detail: format!("send limiter closed: {source}"),
                })?;
            target.object_rpc_sender.send(inbound).await
        }
    }
    .map_err(|_| NetworkError::UnknownNode {
        node_id: target_node_id.clone(),
    })
}

fn async_transport_send_timed_out(target_node_id: &NodeId, timeout: Duration) -> NetworkError {
    NetworkError::Io {
        operation: format!("async transport send to {target_node_id}"),
        detail: format!("timed out after {}ms", timeout.as_millis()),
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

impl<T> NetworkService<T> {
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
}

impl<T: AsyncNetworkTransport> NetworkService<T> {
    fn enqueue_event_async(
        &mut self,
        stored_event: &StoredEvent,
    ) -> Result<Vec<RouteDelivery>, NetworkError> {
        let deliveries = self.router.route_event(stored_event)?;
        let local_node_id = AsyncNetworkTransport::local_node_id(&self.transport).clone();
        let mut accepted = Vec::new();

        for mut delivery in deliveries {
            if delivery.target_node_id != local_node_id
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

    async fn pending_incoming_count_async(&mut self) -> Result<Option<usize>, NetworkError> {
        self.transport.pending_incoming_count_async().await
    }

    async fn pump_outbound_limited_async(
        &mut self,
        max_deliveries: usize,
        send_deadline: Option<tokio::time::Instant>,
    ) -> Result<usize, NetworkError> {
        if max_deliveries == 0 {
            return Ok(0);
        }
        self.ledger.expire_ack_deadlines_at(Instant::now());
        let local_node_id = AsyncNetworkTransport::local_node_id(&self.transport).clone();
        let mut sent = 0;
        let delivery_ids = self
            .ledger
            .ready_delivery_ids_limited(max_deliveries)
            .into_iter();

        for delivery_id in delivery_ids {
            let Some(record) = self.ledger.record(&delivery_id).cloned() else {
                continue;
            };

            if record.delivery.target_node_id != local_node_id
                && !self.registry().is_routable(&record.delivery.target_node_id)
            {
                self.ledger.mark_waiting_for_peer(delivery_id)?;
                continue;
            }

            let target_node_id = record.delivery.target_node_id.clone();
            let frame = self.event_frame_for_local_node(&local_node_id, &record);
            match self
                .transport
                .send_async(
                    &target_node_id,
                    frame,
                    actor_send_options_for_deadline(send_deadline),
                )
                .await
            {
                Ok(()) => {
                    self.ledger.mark_sent(delivery_id)?;
                    sent += 1;
                }
                Err(NetworkError::MailboxFull { .. }) => {
                    self.ledger.mark_backpressured(delivery_id)?;
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

    async fn process_incoming_with_send_deadline_async<S: EventStore>(
        &mut self,
        store: &mut S,
        max_inbound_frames: usize,
        send_deadline: Option<tokio::time::Instant>,
    ) -> Result<Vec<InboundOutcome>, NetworkError> {
        if self.transport.pending_incoming_count_async().await? == Some(0) {
            return Ok(Vec::new());
        }
        let inbound_frames = self
            .transport
            .drain_incoming_async(max_inbound_frames)
            .await?;
        let mut outcomes = Vec::with_capacity(inbound_frames.len());
        let local_node_id = AsyncNetworkTransport::local_node_id(&self.transport).clone();

        for inbound in inbound_frames {
            let source_node_id = inbound.frame.source_node_id.clone();
            let message_origin_node_id = inbound.frame.message.origin_node_id.clone();
            let frame_target_node_id = inbound.frame.target_node_id.clone();
            let message_target_node_id = inbound.frame.message.target_node_id.clone();
            if source_node_id != message_origin_node_id
                || inbound.received_by != local_node_id
                || frame_target_node_id != local_node_id
                || message_target_node_id != local_node_id
            {
                outcomes.push(InboundOutcome::FrameIgnored {
                    source_node_id,
                    message_origin_node_id,
                    received_by: inbound.received_by,
                    frame_target_node_id,
                    message_target_node_id,
                });
                continue;
            }
            match inbound.frame.message.body {
                MeshMessageBody::Event { delivery_id, event } => {
                    let event_origin_node_id = event.origin_node_id.clone();
                    let event_target_node_id = event.target_node_id.clone();
                    if event_origin_node_id != inbound.frame.source_node_id
                        || !event_addresses_local_node(&event, &local_node_id)
                    {
                        outcomes.push(InboundOutcome::EventIgnored {
                            delivery_id,
                            event_id: event.event_id.clone(),
                            origin_node_id: event_origin_node_id,
                            source_node_id: inbound.frame.source_node_id,
                            target_node_id: event_target_node_id,
                            local_node_id: local_node_id.clone(),
                        });
                        continue;
                    }

                    let outcome = ingest_event(&mut self.seen_events, store, delivery_id, &event)?;
                    outcomes.push(outcome);

                    let event_id = event.event_id.clone();
                    let ack_target_node_id = inbound.frame.source_node_id.clone();
                    let ack = DeliveryAck {
                        delivery_id,
                        event_id: event_id.clone(),
                        target_node_id: local_node_id.clone(),
                    };
                    let ack_frame = self.ack_frame_for_local_node(
                        &local_node_id,
                        ack_target_node_id.clone(),
                        ack,
                    );
                    if let Err(error) = self
                        .transport
                        .send_async(
                            &ack_target_node_id,
                            ack_frame,
                            actor_send_options_for_deadline(send_deadline),
                        )
                        .await
                    {
                        outcomes.push(InboundOutcome::AckSendFailed {
                            delivery_id,
                            event_id,
                            target_node_id: ack_target_node_id,
                            detail: error.to_string(),
                        });
                    }
                }
                MeshMessageBody::Ack(ack) => {
                    let acking_node_id = inbound.frame.source_node_id;
                    match self
                        .ledger
                        .ack(ack.delivery_id, &ack.event_id, &acking_node_id)
                    {
                        Ok(()) => outcomes.push(InboundOutcome::AckAccepted {
                            delivery_id: ack.delivery_id,
                            target_node_id: acking_node_id,
                        }),
                        Err(NetworkError::UnknownDelivery { delivery_id }) => {
                            outcomes.push(InboundOutcome::AckIgnored {
                                delivery_id,
                                target_node_id: acking_node_id,
                            });
                        }
                        Err(NetworkError::AckEventMismatch { delivery_id, .. }) => {
                            outcomes.push(InboundOutcome::AckIgnored {
                                delivery_id,
                                target_node_id: acking_node_id,
                            });
                        }
                        Err(NetworkError::AckTargetMismatch { delivery_id, .. }) => {
                            outcomes.push(InboundOutcome::AckIgnored {
                                delivery_id,
                                target_node_id: acking_node_id,
                            });
                        }
                        Err(error) => return Err(error),
                    }
                }
                MeshMessageBody::Heartbeat(heartbeat) => {
                    let source_node_id = inbound.frame.source_node_id;
                    if heartbeat.node_id != source_node_id {
                        outcomes.push(InboundOutcome::HeartbeatIgnored {
                            node_id: heartbeat.node_id,
                            source_node_id,
                        });
                        continue;
                    }
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

    async fn process_incoming_after_wait_with_deadline_async<S: EventStore>(
        &mut self,
        store: &mut S,
        timeout: Duration,
        max_inbound_frames: usize,
        command_deadline: Option<tokio::time::Instant>,
    ) -> Result<Vec<InboundOutcome>, NetworkError> {
        let wait_timeout = match remaining_async_command_timeout(command_deadline) {
            Some(remaining) => timeout.min(remaining),
            None => timeout,
        };
        if !self.transport.wait_for_incoming_async(wait_timeout).await? {
            return Ok(Vec::new());
        }
        self.process_incoming_with_send_deadline_async(store, max_inbound_frames, command_deadline)
            .await
    }

    async fn send_heartbeat_with_transport_deadline_async(
        &mut self,
        target_node_id: &NodeId,
        frame: NetworkFrame,
        send_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), NetworkError> {
        self.transport
            .send_async(
                target_node_id,
                frame,
                actor_send_options_for_deadline(send_deadline),
            )
            .await
    }

    fn record_missed_heartbeat_async(
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

    fn event_frame_for_local_node(
        &mut self,
        local_node_id: &NodeId,
        record: &DeliveryRecord,
    ) -> NetworkFrame {
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: local_node_id.clone(),
            target_node_id: record.delivery.target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: local_node_id.clone(),
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

    fn ack_frame_for_local_node(
        &mut self,
        local_node_id: &NodeId,
        target_node_id: NodeId,
        ack: DeliveryAck,
    ) -> NetworkFrame {
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: local_node_id.clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: local_node_id.clone(),
                target_node_id,
                body: MeshMessageBody::Ack(ack),
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;
        frame
    }

    fn heartbeat_frame_for_local_node(
        &mut self,
        local_node_id: &NodeId,
        target_node_id: NodeId,
        heartbeat: Heartbeat,
    ) -> NetworkFrame {
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: local_node_id.clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: local_node_id.clone(),
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
/// commands are in flight, [`AsyncNetworkServiceHandle`] methods await room (backpressure).
const NETWORK_SERVICE_QUEUE_CAPACITY: usize = 256;
pub(crate) const NETWORK_SERVICE_INBOUND_BATCH_LIMIT: usize = 256;
pub(crate) const NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkServiceActorOptions {
    pub command_queue_capacity: usize,
    pub inbound_batch_limit: usize,
    pub outbound_batch_limit: usize,
}

impl NetworkServiceActorOptions {
    fn normalized(self) -> Self {
        Self {
            command_queue_capacity: self.command_queue_capacity.max(1),
            inbound_batch_limit: self.inbound_batch_limit.max(1),
            outbound_batch_limit: self.outbound_batch_limit.max(1),
        }
    }
}

impl Default for NetworkServiceActorOptions {
    fn default() -> Self {
        Self {
            command_queue_capacity: NETWORK_SERVICE_QUEUE_CAPACITY,
            inbound_batch_limit: NETWORK_SERVICE_INBOUND_BATCH_LIMIT,
            outbound_batch_limit: NETWORK_SERVICE_OUTBOUND_BATCH_LIMIT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkServiceCommandBackpressure {
    WaitForCapacity,
    FailIfFull,
}

impl Default for NetworkServiceCommandBackpressure {
    fn default() -> Self {
        Self::WaitForCapacity
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NetworkServiceCommandOptions {
    timeout: Option<Duration>,
    backpressure: NetworkServiceCommandBackpressure,
}

impl NetworkServiceCommandOptions {
    pub const fn timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            backpressure: NetworkServiceCommandBackpressure::WaitForCapacity,
        }
    }

    pub const fn fail_if_queue_full() -> Self {
        Self {
            timeout: None,
            backpressure: NetworkServiceCommandBackpressure::FailIfFull,
        }
    }

    pub const fn timeout_and_fail_if_queue_full(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            backpressure: NetworkServiceCommandBackpressure::FailIfFull,
        }
    }

    pub const fn timeout_duration(self) -> Option<Duration> {
        self.timeout
    }

    pub const fn backpressure(self) -> NetworkServiceCommandBackpressure {
        self.backpressure
    }
}

enum AsyncNetworkServiceCommand {
    EnqueueEvent {
        stored_event: StoredEvent,
        reply: tokio::sync::oneshot::Sender<Result<Vec<RouteDelivery>, NetworkError>>,
    },
    PumpOutbound {
        send_deadline: Option<tokio::time::Instant>,
        reply: tokio::sync::oneshot::Sender<Result<usize, NetworkError>>,
    },
    ProcessIncoming {
        send_deadline: Option<tokio::time::Instant>,
        reply: tokio::sync::oneshot::Sender<Result<Vec<InboundOutcome>, NetworkError>>,
    },
    ProcessIncomingAfterWait {
        timeout: Duration,
        command_deadline: Option<tokio::time::Instant>,
        reply: tokio::sync::oneshot::Sender<Result<Vec<InboundOutcome>, NetworkError>>,
    },
    PendingIncomingCount {
        reply: tokio::sync::oneshot::Sender<Result<Option<usize>, NetworkError>>,
    },
    SendHeartbeat {
        target_node_id: NodeId,
        tick: u64,
        send_deadline: Option<tokio::time::Instant>,
        reply: tokio::sync::oneshot::Sender<Result<(), NetworkError>>,
    },
    RecordMissedHeartbeat {
        target_node_id: NodeId,
        policy: HeartbeatPolicy,
        reply: tokio::sync::oneshot::Sender<Result<(PeerStatus, usize), NetworkError>>,
    },
    LedgerRecords {
        reply: tokio::sync::oneshot::Sender<Result<Vec<DeliveryRecord>, NetworkError>>,
    },
    NextOutboundWakeAt {
        reply: tokio::sync::oneshot::Sender<Result<Option<Instant>, NetworkError>>,
    },
}

impl AsyncNetworkServiceCommand {
    fn caller_is_waiting(&self) -> bool {
        match self {
            Self::EnqueueEvent { reply, .. } => !reply.is_closed(),
            Self::PumpOutbound { reply, .. } => !reply.is_closed(),
            Self::ProcessIncoming { reply, .. } => !reply.is_closed(),
            Self::ProcessIncomingAfterWait { reply, .. } => !reply.is_closed(),
            Self::PendingIncomingCount { reply } => !reply.is_closed(),
            Self::SendHeartbeat { reply, .. } => !reply.is_closed(),
            Self::RecordMissedHeartbeat { reply, .. } => !reply.is_closed(),
            Self::LedgerRecords { reply } => !reply.is_closed(),
            Self::NextOutboundWakeAt { reply } => !reply.is_closed(),
        }
    }
}

fn remaining_async_command_timeout(deadline: Option<tokio::time::Instant>) -> Option<Duration> {
    deadline.map(|deadline| {
        deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default()
    })
}

fn actor_send_options_for_deadline(
    deadline: Option<tokio::time::Instant>,
) -> AsyncNetworkSendOptions {
    match remaining_async_command_timeout(deadline) {
        Some(timeout) => AsyncNetworkSendOptions::timeout(timeout),
        None => AsyncNetworkSendOptions::nonblocking(),
    }
}

impl<T: AsyncNetworkTransport + Send + 'static> NetworkService<T> {
    /// Run the async transport actor on the caller-owned Tokio runtime.
    pub fn spawn_on_runtime<S: EventStore + Send + 'static>(
        self,
        runtime: tokio::runtime::Handle,
        store: S,
    ) -> AsyncNetworkServiceHandle<S> {
        self.spawn_on_runtime_with_options(runtime, store, NetworkServiceActorOptions::default())
    }

    /// Run the async transport actor on the caller-owned Tokio runtime with explicit backpressure
    /// and batch limits.
    pub fn spawn_on_runtime_with_options<S: EventStore + Send + 'static>(
        self,
        runtime: tokio::runtime::Handle,
        store: S,
        options: NetworkServiceActorOptions,
    ) -> AsyncNetworkServiceHandle<S> {
        let options = options.normalized();
        let (commands, mut requests) = tokio::sync::mpsc::channel::<AsyncNetworkServiceCommand>(
            options.command_queue_capacity,
        );
        let task = runtime.spawn(async move {
            let mut service = self;
            let mut store = store;
            while let Some(command) = requests.recv().await {
                if !command.caller_is_waiting() {
                    continue;
                }
                match command {
                    AsyncNetworkServiceCommand::EnqueueEvent {
                        stored_event,
                        reply,
                    } => {
                        let _ = reply.send(service.enqueue_event_async(&stored_event));
                    }
                    AsyncNetworkServiceCommand::PumpOutbound {
                        send_deadline,
                        mut reply,
                    } => {
                        tokio::select! {
                            result = service.pump_outbound_limited_async(
                                options.outbound_batch_limit,
                                send_deadline,
                            ) => {
                                let _ = reply.send(result);
                            }
                            _ = reply.closed() => {}
                        }
                    }
                    AsyncNetworkServiceCommand::ProcessIncoming {
                        send_deadline,
                        reply,
                    } => {
                        let _ = reply.send(
                            service
                                .process_incoming_with_send_deadline_async(
                                    &mut store,
                                    options.inbound_batch_limit,
                                    send_deadline,
                                )
                                .await,
                        );
                    }
                    AsyncNetworkServiceCommand::ProcessIncomingAfterWait {
                        timeout,
                        command_deadline,
                        mut reply,
                    } => {
                        tokio::select! {
                            result = service.process_incoming_after_wait_with_deadline_async(
                                &mut store,
                                timeout,
                                options.inbound_batch_limit,
                                command_deadline,
                            ) => {
                                let _ = reply.send(result);
                            }
                            _ = reply.closed() => {}
                        }
                    }
                    AsyncNetworkServiceCommand::PendingIncomingCount { reply } => {
                        let _ = reply.send(service.pending_incoming_count_async().await);
                    }
                    AsyncNetworkServiceCommand::SendHeartbeat {
                        target_node_id,
                        tick,
                        send_deadline,
                        mut reply,
                    } => {
                        let local_node_id =
                            AsyncNetworkTransport::local_node_id(&service.transport).clone();
                        let heartbeat = Heartbeat {
                            node_id: local_node_id.clone(),
                            tick,
                        };
                        let frame = service.heartbeat_frame_for_local_node(
                            &local_node_id,
                            target_node_id.clone(),
                            heartbeat,
                        );
                        tokio::select! {
                            result = service.send_heartbeat_with_transport_deadline_async(
                                &target_node_id,
                                frame,
                                send_deadline,
                            ) => {
                                let _ = reply.send(result);
                            }
                            _ = reply.closed() => {}
                        }
                    }
                    AsyncNetworkServiceCommand::RecordMissedHeartbeat {
                        target_node_id,
                        policy,
                        reply,
                    } => {
                        let _ = reply
                            .send(service.record_missed_heartbeat_async(&target_node_id, policy));
                    }
                    AsyncNetworkServiceCommand::LedgerRecords { reply } => {
                        let _ = reply.send(Ok(service
                            .ledger()
                            .records()
                            .into_iter()
                            .cloned()
                            .collect()));
                    }
                    AsyncNetworkServiceCommand::NextOutboundWakeAt { reply } => {
                        let _ = reply.send(Ok(service.next_outbound_wake_at()));
                    }
                }
            }
            store
        });
        AsyncNetworkServiceHandle { commands, task }
    }
}

/// Handle to a [`NetworkService`] running with an [`AsyncNetworkTransport`] actor
/// (see [`NetworkService::spawn_on_runtime`]). Transport send/drain work is awaited inside
/// the actor instead of running through synchronous transport calls.
pub struct AsyncNetworkServiceHandle<S> {
    commands: tokio::sync::mpsc::Sender<AsyncNetworkServiceCommand>,
    task: tokio::task::JoinHandle<S>,
}

impl<S> AsyncNetworkServiceHandle<S>
where
    S: Send + 'static,
{
    pub fn available_command_capacity(&self) -> usize {
        self.commands.capacity()
    }

    pub fn max_command_capacity(&self) -> usize {
        self.commands.max_capacity()
    }

    pub fn is_command_queue_closed(&self) -> bool {
        self.commands.is_closed()
    }

    pub fn with_command_options(
        &self,
        options: NetworkServiceCommandOptions,
    ) -> AsyncNetworkServiceCommands<'_, S> {
        AsyncNetworkServiceCommands {
            handle: self,
            options,
        }
    }

    pub fn with_command_timeout(&self, timeout: Duration) -> AsyncNetworkServiceCommands<'_, S> {
        self.with_command_options(NetworkServiceCommandOptions::timeout(timeout))
    }

    async fn request<R>(
        &self,
        make_command: impl FnOnce(
            tokio::sync::oneshot::Sender<Result<R, NetworkError>>,
        ) -> AsyncNetworkServiceCommand,
    ) -> Result<R, NetworkError>
    where
        R: Send + 'static,
    {
        let (reply, response) = tokio::sync::oneshot::channel();
        self.commands
            .send(make_command(reply))
            .await
            .map_err(|_| network_service_stopped())?;
        response.await.map_err(|_| network_service_stopped())?
    }

    async fn request_with_options<R>(
        &self,
        operation: &'static str,
        options: NetworkServiceCommandOptions,
        make_command: impl FnOnce(
            tokio::sync::oneshot::Sender<Result<R, NetworkError>>,
            Option<tokio::time::Instant>,
        ) -> AsyncNetworkServiceCommand,
    ) -> Result<R, NetworkError>
    where
        R: Send + 'static,
    {
        if matches!(
            options.backpressure(),
            NetworkServiceCommandBackpressure::FailIfFull
        ) {
            return self
                .request_with_fail_fast_queue(operation, options, make_command)
                .await;
        }

        let Some(timeout) = options.timeout_duration() else {
            let (reply, response) = tokio::sync::oneshot::channel();
            self.commands
                .send(make_command(reply, None))
                .await
                .map_err(|_| network_service_stopped())?;
            return response.await.map_err(|_| network_service_stopped())?;
        };

        let deadline = tokio::time::Instant::now() + timeout;
        let (reply, response) = tokio::sync::oneshot::channel();
        let command = make_command(reply, Some(deadline));
        let permit = tokio::time::timeout_at(deadline, self.commands.reserve())
            .await
            .map_err(|_| network_service_command_timed_out(operation, timeout))?
            .map_err(|_| network_service_stopped())?;
        permit.send(command);
        tokio::time::timeout_at(deadline, response)
            .await
            .map_err(|_| network_service_command_timed_out(operation, timeout))?
            .map_err(|_| network_service_stopped())?
    }

    async fn request_with_fail_fast_queue<R>(
        &self,
        operation: &'static str,
        options: NetworkServiceCommandOptions,
        make_command: impl FnOnce(
            tokio::sync::oneshot::Sender<Result<R, NetworkError>>,
            Option<tokio::time::Instant>,
        ) -> AsyncNetworkServiceCommand,
    ) -> Result<R, NetworkError>
    where
        R: Send + 'static,
    {
        let deadline = options
            .timeout_duration()
            .map(|timeout| tokio::time::Instant::now() + timeout);
        let (reply, response) = tokio::sync::oneshot::channel();
        let command = make_command(reply, deadline);
        let permit = self.commands.try_reserve().map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                network_service_command_queue_full(operation, self.commands.max_capacity())
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => network_service_stopped(),
        })?;
        permit.send(command);
        if let Some(timeout) = options.timeout_duration() {
            let deadline = deadline.expect("deadline set with timeout");
            return tokio::time::timeout_at(deadline, response)
                .await
                .map_err(|_| network_service_command_timed_out(operation, timeout))?
                .map_err(|_| network_service_stopped())?;
        }
        response.await.map_err(|_| network_service_stopped())?
    }

    pub async fn enqueue_event(
        &self,
        stored_event: StoredEvent,
    ) -> Result<Vec<RouteDelivery>, NetworkError> {
        self.request(|reply| AsyncNetworkServiceCommand::EnqueueEvent {
            stored_event,
            reply,
        })
        .await
    }

    pub async fn pump_outbound(&self) -> Result<usize, NetworkError> {
        self.request(|reply| AsyncNetworkServiceCommand::PumpOutbound {
            send_deadline: None,
            reply,
        })
        .await
    }

    pub async fn process_incoming(&self) -> Result<Vec<InboundOutcome>, NetworkError> {
        self.request(|reply| AsyncNetworkServiceCommand::ProcessIncoming {
            send_deadline: None,
            reply,
        })
        .await
    }

    pub async fn process_incoming_after_wait(
        &self,
        timeout: Duration,
    ) -> Result<Vec<InboundOutcome>, NetworkError> {
        self.request(
            |reply| AsyncNetworkServiceCommand::ProcessIncomingAfterWait {
                timeout,
                command_deadline: None,
                reply,
            },
        )
        .await
    }

    pub async fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        self.request(|reply| AsyncNetworkServiceCommand::PendingIncomingCount { reply })
            .await
    }

    pub async fn send_heartbeat(
        &self,
        target_node_id: NodeId,
        tick: u64,
    ) -> Result<(), NetworkError> {
        self.request(|reply| AsyncNetworkServiceCommand::SendHeartbeat {
            target_node_id,
            tick,
            send_deadline: None,
            reply,
        })
        .await
    }

    pub async fn record_missed_heartbeat(
        &self,
        target_node_id: NodeId,
        policy: HeartbeatPolicy,
    ) -> Result<(PeerStatus, usize), NetworkError> {
        self.request(|reply| AsyncNetworkServiceCommand::RecordMissedHeartbeat {
            target_node_id,
            policy,
            reply,
        })
        .await
    }

    pub async fn ledger_records(&self) -> Result<Vec<DeliveryRecord>, NetworkError> {
        self.request(|reply| AsyncNetworkServiceCommand::LedgerRecords { reply })
            .await
    }

    pub async fn next_outbound_wake_at(&self) -> Result<Option<Instant>, NetworkError> {
        self.request(|reply| AsyncNetworkServiceCommand::NextOutboundWakeAt { reply })
            .await
    }

    pub async fn shutdown(self) -> S {
        self.try_shutdown()
            .await
            .expect("network service actor stopped unexpectedly")
    }

    pub async fn try_shutdown(self) -> Result<S, NetworkError> {
        let Self { commands, task } = self;
        drop(commands);
        task.await.map_err(network_service_actor_join_error)
    }

    pub async fn shutdown_with_timeout(self, timeout: Duration) -> Result<S, NetworkError> {
        let Self { commands, mut task } = self;
        drop(commands);
        match tokio::time::timeout(timeout, &mut task).await {
            Ok(result) => result.map_err(network_service_actor_join_error),
            Err(_) => {
                task.abort();
                Err(network_service_command_timed_out("shutdown", timeout))
            }
        }
    }

    pub fn abort(self) {
        let Self { commands, task } = self;
        drop(commands);
        task.abort();
    }
}

pub struct AsyncNetworkServiceCommands<'a, S> {
    handle: &'a AsyncNetworkServiceHandle<S>,
    options: NetworkServiceCommandOptions,
}

impl<S> AsyncNetworkServiceCommands<'_, S>
where
    S: Send + 'static,
{
    pub async fn enqueue_event(
        &self,
        stored_event: StoredEvent,
    ) -> Result<Vec<RouteDelivery>, NetworkError> {
        self.handle
            .request_with_options("enqueue-event", self.options, |reply, _deadline| {
                AsyncNetworkServiceCommand::EnqueueEvent {
                    stored_event,
                    reply,
                }
            })
            .await
    }

    pub async fn pump_outbound(&self) -> Result<usize, NetworkError> {
        self.handle
            .request_with_options("pump-outbound", self.options, |reply, deadline| {
                AsyncNetworkServiceCommand::PumpOutbound {
                    send_deadline: deadline,
                    reply,
                }
            })
            .await
    }

    pub async fn process_incoming(&self) -> Result<Vec<InboundOutcome>, NetworkError> {
        self.handle
            .request_with_options("process-incoming", self.options, |reply, deadline| {
                AsyncNetworkServiceCommand::ProcessIncoming {
                    send_deadline: deadline,
                    reply,
                }
            })
            .await
    }

    pub async fn process_incoming_after_wait(
        &self,
        timeout: Duration,
    ) -> Result<Vec<InboundOutcome>, NetworkError> {
        self.handle
            .request_with_options(
                "process-incoming-after-wait",
                self.options,
                |reply, deadline| AsyncNetworkServiceCommand::ProcessIncomingAfterWait {
                    timeout,
                    command_deadline: deadline,
                    reply,
                },
            )
            .await
    }

    pub async fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        self.handle
            .request_with_options(
                "pending-incoming-count",
                self.options,
                |reply, _deadline| AsyncNetworkServiceCommand::PendingIncomingCount { reply },
            )
            .await
    }

    pub async fn send_heartbeat(
        &self,
        target_node_id: NodeId,
        tick: u64,
    ) -> Result<(), NetworkError> {
        self.handle
            .request_with_options("send-heartbeat", self.options, |reply, deadline| {
                AsyncNetworkServiceCommand::SendHeartbeat {
                    target_node_id,
                    tick,
                    send_deadline: deadline,
                    reply,
                }
            })
            .await
    }

    pub async fn record_missed_heartbeat(
        &self,
        target_node_id: NodeId,
        policy: HeartbeatPolicy,
    ) -> Result<(PeerStatus, usize), NetworkError> {
        self.handle
            .request_with_options(
                "record-missed-heartbeat",
                self.options,
                |reply, _deadline| AsyncNetworkServiceCommand::RecordMissedHeartbeat {
                    target_node_id,
                    policy,
                    reply,
                },
            )
            .await
    }

    pub async fn ledger_records(&self) -> Result<Vec<DeliveryRecord>, NetworkError> {
        self.handle
            .request_with_options("ledger-records", self.options, |reply, _deadline| {
                AsyncNetworkServiceCommand::LedgerRecords { reply }
            })
            .await
    }

    pub async fn next_outbound_wake_at(&self) -> Result<Option<Instant>, NetworkError> {
        self.handle
            .request_with_options("next-outbound-wake-at", self.options, |reply, _deadline| {
                AsyncNetworkServiceCommand::NextOutboundWakeAt { reply }
            })
            .await
    }
}

fn network_service_actor_join_error(source: tokio::task::JoinError) -> NetworkError {
    NetworkError::Io {
        operation: "network-service-actor".to_string(),
        detail: source.to_string(),
    }
}

fn network_service_stopped() -> NetworkError {
    NetworkError::Io {
        operation: "network-service-command".to_string(),
        detail: "network service actor is no longer running".to_string(),
    }
}

fn network_service_command_queue_full(operation: &str, max_commands: usize) -> NetworkError {
    NetworkError::NetworkServiceCommandQueueFull {
        operation: operation.to_string(),
        max_commands,
    }
}

fn network_service_command_timed_out(operation: &str, timeout: Duration) -> NetworkError {
    NetworkError::Io {
        operation: format!("network-service-command {operation}"),
        detail: format!("timed out after {}ms", timeout.as_millis()),
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
    EventIgnored {
        delivery_id: RouteDeliveryId,
        event_id: EventId,
        origin_node_id: NodeId,
        source_node_id: NodeId,
        target_node_id: Option<NodeId>,
        local_node_id: NodeId,
    },
    FrameIgnored {
        source_node_id: NodeId,
        message_origin_node_id: NodeId,
        received_by: NodeId,
        frame_target_node_id: NodeId,
        message_target_node_id: NodeId,
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
    HeartbeatIgnored {
        node_id: NodeId,
        source_node_id: NodeId,
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
    parkable_records_by_target: BTreeMap<NodeId, BTreeSet<RouteDeliveryId>>,
    requeueable_records_by_target: BTreeMap<NodeId, BTreeSet<RouteDeliveryId>>,
    active_by_event_target: BTreeMap<(EventId, NodeId), RouteDeliveryId>,
    retry_not_before: BTreeMap<RouteDeliveryId, Instant>,
    retry_deadline_ids: BTreeMap<Instant, BTreeSet<RouteDeliveryId>>,
    retry_deadline_ids_by_priority:
        [BTreeMap<Instant, BTreeSet<RouteDeliveryId>>; LATENCY_CLASS_READY_PRIORITY_COUNT],
    ack_deadline: BTreeMap<RouteDeliveryId, Instant>,
    ack_deadline_ids: BTreeMap<Instant, BTreeSet<RouteDeliveryId>>,
    queued_record_ids: BTreeSet<RouteDeliveryId>,
    queued_record_ids_by_priority: [BTreeSet<RouteDeliveryId>; LATENCY_CLASS_READY_PRIORITY_COUNT],
    immediate_retry_ids: BTreeSet<RouteDeliveryId>,
    immediate_retry_ids_by_priority:
        [BTreeSet<RouteDeliveryId>; LATENCY_CLASS_READY_PRIORITY_COUNT],
    sent_without_ack_deadline_ids: BTreeSet<RouteDeliveryId>,
    terminal_record_ids: BTreeSet<RouteDeliveryId>,
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
            parkable_records_by_target: BTreeMap::new(),
            requeueable_records_by_target: BTreeMap::new(),
            active_by_event_target: BTreeMap::new(),
            retry_not_before: BTreeMap::new(),
            retry_deadline_ids: BTreeMap::new(),
            retry_deadline_ids_by_priority: std::array::from_fn(|_| BTreeMap::new()),
            ack_deadline: BTreeMap::new(),
            ack_deadline_ids: BTreeMap::new(),
            queued_record_ids: BTreeSet::new(),
            queued_record_ids_by_priority: std::array::from_fn(|_| BTreeSet::new()),
            immediate_retry_ids: BTreeSet::new(),
            immediate_retry_ids_by_priority: std::array::from_fn(|_| BTreeSet::new()),
            sent_without_ack_deadline_ids: BTreeSet::new(),
            terminal_record_ids: BTreeSet::new(),
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
        let target_node_id = delivery.target_node_id.clone();
        self.active_by_event_target.insert(key, delivery_id);
        self.clear_delivery_index_state(delivery_id);
        self.index_deadline_free_status(delivery_id, &delivery.status, &delivery.latency_class);
        self.index_target_status(delivery_id, &target_node_id, &delivery.status);
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
        self.ready_delivery_ids_limited_at_inner(now, max_deliveries, |_| {})
    }

    #[cfg(test)]
    pub(crate) fn ready_delivery_ids_limited_at_counting(
        &self,
        now: Instant,
        max_deliveries: usize,
        inspected: &mut usize,
    ) -> Vec<RouteDeliveryId> {
        self.ready_delivery_ids_limited_at_inner(now, max_deliveries, |_| {
            *inspected = inspected.saturating_add(1);
        })
    }

    fn ready_delivery_ids_limited_at_inner(
        &self,
        now: Instant,
        max_deliveries: usize,
        mut on_inspect: impl FnMut(RouteDeliveryId),
    ) -> Vec<RouteDeliveryId> {
        if max_deliveries == 0 {
            return Vec::new();
        }
        let mut ready = Vec::with_capacity(max_deliveries.min(self.records.len()));
        'priorities: for priority in 0..LATENCY_CLASS_READY_PRIORITY_COUNT {
            for delivery_id in self.queued_record_ids_by_priority[priority].iter().copied() {
                if self.push_ready_candidate(
                    delivery_id,
                    priority,
                    max_deliveries,
                    &mut ready,
                    &mut on_inspect,
                ) {
                    break 'priorities;
                }
            }
            for delivery_id in self.immediate_retry_ids_by_priority[priority]
                .iter()
                .copied()
            {
                if self.push_ready_candidate(
                    delivery_id,
                    priority,
                    max_deliveries,
                    &mut ready,
                    &mut on_inspect,
                ) {
                    break 'priorities;
                }
            }
            for (_, delivery_ids) in self.retry_deadline_ids_by_priority[priority].range(..=now) {
                for delivery_id in delivery_ids.iter().copied() {
                    if self.push_ready_candidate(
                        delivery_id,
                        priority,
                        max_deliveries,
                        &mut ready,
                        &mut on_inspect,
                    ) {
                        break 'priorities;
                    }
                }
            }
        }
        ready
    }

    fn push_ready_candidate(
        &self,
        delivery_id: RouteDeliveryId,
        priority: usize,
        max_deliveries: usize,
        ready: &mut Vec<RouteDeliveryId>,
        on_inspect: &mut impl FnMut(RouteDeliveryId),
    ) -> bool {
        on_inspect(delivery_id);
        let Some(record) = self.records.get(&delivery_id) else {
            return ready.len() >= max_deliveries;
        };
        if latency_class_ready_priority(&record.delivery.latency_class) as usize == priority {
            ready.push(delivery_id);
        }
        ready.len() >= max_deliveries
    }

    pub fn next_outbound_wake_at(&self) -> Option<Instant> {
        self.next_outbound_wake_at_at(Instant::now())
    }

    pub(crate) fn next_outbound_wake_at_at(&self, now: Instant) -> Option<Instant> {
        if !self.queued_record_ids.is_empty()
            || !self.immediate_retry_ids.is_empty()
            || !self.sent_without_ack_deadline_ids.is_empty()
        {
            return Some(now);
        }
        let retry_wake = self
            .retry_deadline_ids
            .first_key_value()
            .map(|(deadline, _)| *deadline);
        let ack_wake = self
            .ack_deadline_ids
            .first_key_value()
            .map(|(deadline, _)| *deadline);
        let wake = match (retry_wake, ack_wake) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(wake), None) | (None, Some(wake)) => Some(wake),
            (None, None) => None,
        }?;
        Some(if wake <= now { now } else { wake })
    }

    pub fn mark_sent(&mut self, delivery_id: RouteDeliveryId) -> Result<(), NetworkError> {
        self.mark_sent_at(delivery_id, Instant::now())
    }

    pub(crate) fn mark_sent_at(
        &mut self,
        delivery_id: RouteDeliveryId,
        now: Instant,
    ) -> Result<(), NetworkError> {
        let (deadline, target_node_id) = {
            let record = self
                .records
                .get_mut(&delivery_id)
                .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
            record.attempts += 1;
            record.delivery.status = DeliveryStatus::Sent;
            (
                now + ack_timeout_for_record(record),
                record.delivery.target_node_id.clone(),
            )
        };
        self.clear_delivery_index_state(delivery_id);
        self.insert_ack_deadline(delivery_id, deadline);
        self.set_target_status(delivery_id, &target_node_id, &DeliveryStatus::Sent);
        Ok(())
    }

    pub fn mark_for_retry(&mut self, delivery_id: RouteDeliveryId) -> Result<(), NetworkError> {
        let retry_target = {
            let record = self
                .records
                .get_mut(&delivery_id)
                .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
            if record.delivery.status == DeliveryStatus::Delivered {
                None
            } else {
                record.delivery.status = DeliveryStatus::Retrying;
                Some((
                    record.delivery.target_node_id.clone(),
                    record.delivery.latency_class.clone(),
                ))
            }
        };
        if let Some((target_node_id, latency_class)) = retry_target {
            self.clear_delivery_index_state(delivery_id);
            self.insert_immediate_retry_index(delivery_id, &latency_class);
            self.set_target_status(delivery_id, &target_node_id, &DeliveryStatus::Retrying);
        }
        Ok(())
    }

    pub fn mark_waiting_for_peer(
        &mut self,
        delivery_id: RouteDeliveryId,
    ) -> Result<(), NetworkError> {
        let target_node_id = {
            let record = self
                .records
                .get_mut(&delivery_id)
                .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
            if record.delivery.status.is_terminal() {
                None
            } else {
                record.delivery.status = DeliveryStatus::WaitingForPeer;
                Some(record.delivery.target_node_id.clone())
            }
        };
        if let Some(target_node_id) = target_node_id {
            self.clear_delivery_index_state(delivery_id);
            self.set_target_status(
                delivery_id,
                &target_node_id,
                &DeliveryStatus::WaitingForPeer,
            );
        }
        Ok(())
    }

    pub fn mark_attempt_failed(
        &mut self,
        delivery_id: RouteDeliveryId,
    ) -> Result<(), NetworkError> {
        self.mark_attempt_failed_at(delivery_id, Instant::now())
    }

    pub fn mark_backpressured(&mut self, delivery_id: RouteDeliveryId) -> Result<(), NetworkError> {
        self.mark_backpressured_at(delivery_id, Instant::now())
    }

    pub(crate) fn mark_backpressured_at(
        &mut self,
        delivery_id: RouteDeliveryId,
        now: Instant,
    ) -> Result<(), NetworkError> {
        let (retry_deadline, target_node_id) = {
            let record = self
                .records
                .get_mut(&delivery_id)
                .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
            if record.delivery.status == DeliveryStatus::Delivered {
                return Ok(());
            }
            record.delivery.status = DeliveryStatus::Retrying;
            (
                now + retry_delay_for_record(record),
                record.delivery.target_node_id.clone(),
            )
        };
        self.clear_delivery_index_state(delivery_id);
        self.insert_retry_deadline(delivery_id, retry_deadline);
        self.set_target_status(delivery_id, &target_node_id, &DeliveryStatus::Retrying);
        Ok(())
    }

    pub(crate) fn mark_attempt_failed_at(
        &mut self,
        delivery_id: RouteDeliveryId,
        now: Instant,
    ) -> Result<(), NetworkError> {
        let (new_retry_deadline, target_node_id, status) = {
            let record = self
                .records
                .get_mut(&delivery_id)
                .ok_or(NetworkError::UnknownDelivery { delivery_id })?;
            record.attempts += 1;
            if record.attempts >= self.retry_policy.max_attempts {
                record.delivery.status = DeliveryStatus::Failed;
                (
                    None,
                    record.delivery.target_node_id.clone(),
                    DeliveryStatus::Failed,
                )
            } else {
                record.delivery.status = DeliveryStatus::Retrying;
                (
                    Some(now + retry_delay_for_record(record)),
                    record.delivery.target_node_id.clone(),
                    DeliveryStatus::Retrying,
                )
            }
        };
        self.clear_delivery_index_state(delivery_id);
        if let Some(deadline) = new_retry_deadline {
            self.insert_retry_deadline(delivery_id, deadline);
        } else {
            self.terminal_record_ids.insert(delivery_id);
        }
        self.set_target_status(delivery_id, &target_node_id, &status);
        self.prune_terminal_records_to_retention();
        Ok(())
    }

    pub(crate) fn expire_ack_deadlines_at(&mut self, now: Instant) -> usize {
        let mut expired_count = 0;
        while let Some((deadline, _)) = self.ack_deadline_ids.first_key_value() {
            if *deadline > now {
                break;
            }
            let deadline = *deadline;
            let Some((_, delivery_ids)) = self.ack_deadline_ids.pop_first() else {
                break;
            };
            for delivery_id in delivery_ids {
                if self.ack_deadline.get(&delivery_id) == Some(&deadline)
                    && self.expire_ack_deadline_delivery_at(delivery_id, now)
                {
                    expired_count += 1;
                }
            }
        }
        while let Some(delivery_id) = self.sent_without_ack_deadline_ids.first().copied() {
            if self.expire_ack_deadline_delivery_at(delivery_id, now) {
                expired_count += 1;
            }
        }
        if expired_count > 0 {
            self.prune_terminal_records_to_retention();
        }
        expired_count
    }

    fn expire_ack_deadline_delivery_at(
        &mut self,
        delivery_id: RouteDeliveryId,
        now: Instant,
    ) -> bool {
        if !self.records.contains_key(&delivery_id) {
            self.clear_delivery_index_state(delivery_id);
            return false;
        }
        let mut stale_non_sent_deadline = false;
        let (new_retry_deadline, target_node_id, status) = {
            let record = self
                .records
                .get_mut(&delivery_id)
                .expect("record existence checked before ack expiry reconciliation");
            if record.delivery.status != DeliveryStatus::Sent {
                stale_non_sent_deadline = true;
                (
                    None,
                    record.delivery.target_node_id.clone(),
                    record.delivery.status.clone(),
                )
            } else if record.attempts >= self.retry_policy.max_attempts {
                record.delivery.status = DeliveryStatus::Failed;
                (
                    None,
                    record.delivery.target_node_id.clone(),
                    DeliveryStatus::Failed,
                )
            } else {
                record.delivery.status = DeliveryStatus::Retrying;
                (
                    Some(now + retry_delay_for_record(record)),
                    record.delivery.target_node_id.clone(),
                    DeliveryStatus::Retrying,
                )
            }
        };
        if stale_non_sent_deadline {
            self.clear_ack_deadline(delivery_id);
            self.sent_without_ack_deadline_ids.remove(&delivery_id);
            return false;
        }
        self.clear_delivery_index_state(delivery_id);
        if let Some(deadline) = new_retry_deadline {
            self.insert_retry_deadline(delivery_id, deadline);
        } else {
            self.terminal_record_ids.insert(delivery_id);
        }
        self.set_target_status(delivery_id, &target_node_id, &status);
        true
    }

    #[cfg(test)]
    pub(crate) fn clear_ack_deadline_for_test(&mut self, delivery_id: RouteDeliveryId) {
        self.clear_ack_deadline(delivery_id);
        if self
            .records
            .get(&delivery_id)
            .is_some_and(|record| record.delivery.status == DeliveryStatus::Sent)
        {
            self.sent_without_ack_deadline_ids.insert(delivery_id);
        }
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
        self.requeue_target_matching_inner(target_node_id, include_in_flight, |_| {})
    }

    fn requeue_target_matching_inner(
        &mut self,
        target_node_id: &NodeId,
        include_in_flight: bool,
        mut on_inspect: impl FnMut(RouteDeliveryId),
    ) -> usize {
        let mut requeued = 0;
        let mut requeued_ids = Vec::new();
        for delivery_id in self.requeueable_target_delivery_ids(target_node_id) {
            on_inspect(delivery_id);
            let Some(record) = self.records.get_mut(&delivery_id) else {
                continue;
            };
            let should_requeue = matches!(
                record.delivery.status,
                DeliveryStatus::Failed | DeliveryStatus::WaitingForPeer
            ) || (include_in_flight
                && record.delivery.status == DeliveryStatus::Sent);
            if should_requeue {
                record.delivery.status = DeliveryStatus::Retrying;
                record.attempts = 0;
                requeued_ids.push((
                    delivery_id,
                    record.delivery.target_node_id.clone(),
                    record.delivery.latency_class.clone(),
                ));
                requeued += 1;
            }
        }
        for (delivery_id, target_node_id, latency_class) in requeued_ids {
            self.clear_delivery_index_state(delivery_id);
            self.insert_immediate_retry_index(delivery_id, &latency_class);
            self.set_target_status(delivery_id, &target_node_id, &DeliveryStatus::Retrying);
        }
        requeued
    }

    #[cfg(test)]
    pub(crate) fn requeue_target_after_recovery_counting_for_test(
        &mut self,
        target_node_id: &NodeId,
        inspected: &mut usize,
    ) -> usize {
        self.requeue_target_matching_inner(target_node_id, true, |_| {
            *inspected = inspected.saturating_add(1);
        })
    }

    pub fn mark_target_waiting_for_peer(&mut self, target_node_id: &NodeId) -> usize {
        self.mark_target_waiting_for_peer_inner(target_node_id, |_| {})
    }

    fn mark_target_waiting_for_peer_inner(
        &mut self,
        target_node_id: &NodeId,
        mut on_inspect: impl FnMut(RouteDeliveryId),
    ) -> usize {
        let mut waiting = 0;
        let mut waiting_ids = Vec::new();
        for delivery_id in self.parkable_target_delivery_ids(target_node_id) {
            on_inspect(delivery_id);
            let Some(record) = self.records.get_mut(&delivery_id) else {
                continue;
            };
            if !record.delivery.status.is_terminal()
                && record.delivery.status != DeliveryStatus::WaitingForPeer
            {
                record.delivery.status = DeliveryStatus::WaitingForPeer;
                waiting_ids.push((delivery_id, record.delivery.target_node_id.clone()));
                waiting += 1;
            }
        }
        for (delivery_id, target_node_id) in waiting_ids {
            self.clear_delivery_index_state(delivery_id);
            self.set_target_status(
                delivery_id,
                &target_node_id,
                &DeliveryStatus::WaitingForPeer,
            );
        }
        waiting
    }

    #[cfg(test)]
    pub(crate) fn mark_target_waiting_for_peer_counting_for_test(
        &mut self,
        target_node_id: &NodeId,
        inspected: &mut usize,
    ) -> usize {
        self.mark_target_waiting_for_peer_inner(target_node_id, |_| {
            *inspected = inspected.saturating_add(1);
        })
    }

    pub fn ack(
        &mut self,
        delivery_id: RouteDeliveryId,
        event_id: &EventId,
        acking_node_id: &NodeId,
    ) -> Result<(), NetworkError> {
        let target_node_id = {
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
            if &record.delivery.event_id != event_id {
                return Err(NetworkError::AckEventMismatch {
                    delivery_id,
                    expected_event_id: record.delivery.event_id.clone(),
                    acking_event_id: event_id.clone(),
                });
            }
            record.delivery.status = DeliveryStatus::Delivered;
            record.delivery.target_node_id.clone()
        };
        self.clear_delivery_index_state(delivery_id);
        self.terminal_record_ids.insert(delivery_id);
        self.set_target_status(delivery_id, &target_node_id, &DeliveryStatus::Delivered);
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
        while self.terminal_record_ids.len() > limit {
            let Some(delivery_id) = self.terminal_record_ids.first().copied() else {
                return;
            };
            self.remove_record(delivery_id);
        }
    }

    fn prune_terminal_records_to_capacity(&mut self) {
        while self.records.len() >= self.capacity {
            let Some(delivery_id) = self.terminal_record_ids.first().copied() else {
                return;
            };
            self.remove_record(delivery_id);
        }
    }

    fn remove_record(&mut self, delivery_id: RouteDeliveryId) -> Option<DeliveryRecord> {
        let record = self.records.remove(&delivery_id)?;
        self.clear_delivery_index_state(delivery_id);
        self.clear_target_status(delivery_id, &record.delivery.target_node_id);
        self.active_by_event_target.remove(&(
            record.delivery.event_id.clone(),
            record.delivery.target_node_id.clone(),
        ));
        Some(record)
    }

    fn parkable_target_delivery_ids(&self, target_node_id: &NodeId) -> Vec<RouteDeliveryId> {
        self.parkable_records_by_target
            .get(target_node_id)
            .map(|delivery_ids| delivery_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    fn requeueable_target_delivery_ids(&self, target_node_id: &NodeId) -> Vec<RouteDeliveryId> {
        self.requeueable_records_by_target
            .get(target_node_id)
            .map(|delivery_ids| delivery_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    fn set_target_status(
        &mut self,
        delivery_id: RouteDeliveryId,
        target_node_id: &NodeId,
        status: &DeliveryStatus,
    ) {
        self.clear_target_status(delivery_id, target_node_id);
        self.index_target_status(delivery_id, target_node_id, status);
    }

    fn index_target_status(
        &mut self,
        delivery_id: RouteDeliveryId,
        target_node_id: &NodeId,
        status: &DeliveryStatus,
    ) {
        match status {
            DeliveryStatus::Queued | DeliveryStatus::Retrying => {
                self.parkable_records_by_target
                    .entry(target_node_id.clone())
                    .or_default()
                    .insert(delivery_id);
            }
            DeliveryStatus::Sent => {
                self.parkable_records_by_target
                    .entry(target_node_id.clone())
                    .or_default()
                    .insert(delivery_id);
                self.requeueable_records_by_target
                    .entry(target_node_id.clone())
                    .or_default()
                    .insert(delivery_id);
            }
            DeliveryStatus::WaitingForPeer | DeliveryStatus::Failed => {
                self.requeueable_records_by_target
                    .entry(target_node_id.clone())
                    .or_default()
                    .insert(delivery_id);
            }
            DeliveryStatus::Delivered => {}
        }
    }

    fn clear_target_status(&mut self, delivery_id: RouteDeliveryId, target_node_id: &NodeId) {
        remove_target_index_entry(
            &mut self.parkable_records_by_target,
            target_node_id,
            delivery_id,
        );
        remove_target_index_entry(
            &mut self.requeueable_records_by_target,
            target_node_id,
            delivery_id,
        );
    }

    fn index_deadline_free_status(
        &mut self,
        delivery_id: RouteDeliveryId,
        status: &DeliveryStatus,
        latency_class: &LatencyClass,
    ) {
        match status {
            DeliveryStatus::Queued => {
                self.insert_queued_index(delivery_id, latency_class);
            }
            DeliveryStatus::Retrying => {
                self.insert_immediate_retry_index(delivery_id, latency_class);
            }
            DeliveryStatus::Sent => {
                self.sent_without_ack_deadline_ids.insert(delivery_id);
            }
            DeliveryStatus::Delivered | DeliveryStatus::Failed => {
                self.terminal_record_ids.insert(delivery_id);
            }
            DeliveryStatus::WaitingForPeer => {}
        }
    }

    fn clear_delivery_index_state(&mut self, delivery_id: RouteDeliveryId) {
        self.queued_record_ids.remove(&delivery_id);
        self.immediate_retry_ids.remove(&delivery_id);
        for queued_ids in &mut self.queued_record_ids_by_priority {
            queued_ids.remove(&delivery_id);
        }
        for retry_ids in &mut self.immediate_retry_ids_by_priority {
            retry_ids.remove(&delivery_id);
        }
        self.sent_without_ack_deadline_ids.remove(&delivery_id);
        self.terminal_record_ids.remove(&delivery_id);
        self.clear_retry_deadline(delivery_id);
        self.clear_ack_deadline(delivery_id);
    }

    fn insert_queued_index(&mut self, delivery_id: RouteDeliveryId, latency_class: &LatencyClass) {
        self.queued_record_ids.insert(delivery_id);
        self.queued_record_ids_by_priority[latency_class_ready_priority(latency_class) as usize]
            .insert(delivery_id);
    }

    fn insert_immediate_retry_index(
        &mut self,
        delivery_id: RouteDeliveryId,
        latency_class: &LatencyClass,
    ) {
        self.immediate_retry_ids.insert(delivery_id);
        self.immediate_retry_ids_by_priority[latency_class_ready_priority(latency_class) as usize]
            .insert(delivery_id);
    }

    fn insert_retry_deadline(&mut self, delivery_id: RouteDeliveryId, deadline: Instant) {
        self.clear_retry_deadline(delivery_id);
        self.immediate_retry_ids.remove(&delivery_id);
        for retry_ids in &mut self.immediate_retry_ids_by_priority {
            retry_ids.remove(&delivery_id);
        }
        self.retry_not_before.insert(delivery_id, deadline);
        self.retry_deadline_ids
            .entry(deadline)
            .or_default()
            .insert(delivery_id);
        if let Some(priority) = self.record_ready_priority(delivery_id) {
            self.retry_deadline_ids_by_priority[priority]
                .entry(deadline)
                .or_default()
                .insert(delivery_id);
        }
    }

    fn clear_retry_deadline(&mut self, delivery_id: RouteDeliveryId) -> Option<Instant> {
        let deadline = self.retry_not_before.remove(&delivery_id)?;
        remove_deadline_index_entry(&mut self.retry_deadline_ids, deadline, delivery_id);
        for index in &mut self.retry_deadline_ids_by_priority {
            remove_deadline_index_entry(index, deadline, delivery_id);
        }
        Some(deadline)
    }

    fn record_ready_priority(&self, delivery_id: RouteDeliveryId) -> Option<usize> {
        self.records
            .get(&delivery_id)
            .map(|record| latency_class_ready_priority(&record.delivery.latency_class) as usize)
    }

    fn insert_ack_deadline(&mut self, delivery_id: RouteDeliveryId, deadline: Instant) {
        self.clear_ack_deadline(delivery_id);
        self.sent_without_ack_deadline_ids.remove(&delivery_id);
        self.ack_deadline.insert(delivery_id, deadline);
        self.ack_deadline_ids
            .entry(deadline)
            .or_default()
            .insert(delivery_id);
    }

    fn clear_ack_deadline(&mut self, delivery_id: RouteDeliveryId) -> Option<Instant> {
        let deadline = self.ack_deadline.remove(&delivery_id)?;
        remove_deadline_index_entry(&mut self.ack_deadline_ids, deadline, delivery_id);
        Some(deadline)
    }
}

fn remove_deadline_index_entry(
    index: &mut BTreeMap<Instant, BTreeSet<RouteDeliveryId>>,
    deadline: Instant,
    delivery_id: RouteDeliveryId,
) {
    let should_remove = if let Some(delivery_ids) = index.get_mut(&deadline) {
        delivery_ids.remove(&delivery_id);
        delivery_ids.is_empty()
    } else {
        false
    };
    if should_remove {
        index.remove(&deadline);
    }
}

fn remove_target_index_entry(
    index: &mut BTreeMap<NodeId, BTreeSet<RouteDeliveryId>>,
    target_node_id: &NodeId,
    delivery_id: RouteDeliveryId,
) {
    let should_remove = if let Some(delivery_ids) = index.get_mut(target_node_id) {
        delivery_ids.remove(&delivery_id);
        delivery_ids.is_empty()
    } else {
        false
    };
    if should_remove {
        index.remove(target_node_id);
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
