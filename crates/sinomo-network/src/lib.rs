//! Peer registry, mesh contracts, and event routing skeletons.

use serde::{Deserialize, Serialize};
use sinomo_core::{EventEnvelope, EventId, LatencyClass, NodeId, RoutingScope, SessionId};
use sinomo_events::{EventSequence, EventStore, EventStoreError, StoredEvent};
use sinomo_node::{NodeAvailability, NodeHealth, NodeProfile};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use thiserror::Error;

#[derive(Clone, Debug, Default)]
pub struct PeerRegistry {
    nodes: BTreeMap<NodeId, NodeProfile>,
    peer_states: BTreeMap<NodeId, PeerState>,
    sessions: BTreeMap<SessionId, BTreeSet<NodeId>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_node(&mut self, profile: NodeProfile) -> Option<NodeProfile> {
        let node_id = profile.node_id.clone();
        self.peer_states
            .entry(node_id.clone())
            .or_insert_with(|| PeerState::new(NodeEndpoint::in_process(node_id.clone())));
        self.nodes.insert(node_id, profile)
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
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TransportKind {
    InProcess,
    Tcp,
    Quic,
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
    #[error(transparent)]
    EventStore(#[from] EventStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        Confidence, EventEnvelope, EventId, EventKind, LatencyClass, NodeId, PrivacyClass,
        RoutingMetadata, RoutingScope, Salience, SessionId, Source,
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
