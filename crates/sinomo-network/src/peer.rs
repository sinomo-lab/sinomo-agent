use crate::error::NetworkError;
use crate::prelude::*;

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
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NetworkStreamId(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkStreamChunkId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NetworkStreamOpen {
    pub stream_id: NetworkStreamId,
    pub protocol: String,
    #[serde(default)]
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
    pub(crate) transport: T,
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
    Delivered,
    Failed,
}
