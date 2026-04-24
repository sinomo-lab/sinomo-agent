//! Peer registry, mesh contracts, and event routing skeletons.

use serde::{Deserialize, Serialize};
use sinomo_core::{EventEnvelope, LatencyClass, NodeId, RoutingScope, SessionId};
use sinomo_events::{EventSequence, StoredEvent};
use sinomo_node::NodeProfile;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use thiserror::Error;

#[derive(Clone, Debug, Default)]
pub struct PeerRegistry {
    nodes: BTreeMap<NodeId, NodeProfile>,
    sessions: BTreeMap<SessionId, BTreeSet<NodeId>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_node(&mut self, profile: NodeProfile) -> Option<NodeProfile> {
        self.nodes.insert(profile.node_id.clone(), profile)
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

    pub fn session_nodes(&self, session_id: &SessionId) -> Vec<NodeId> {
        self.sessions
            .get(session_id)
            .map(|nodes| nodes.iter().cloned().collect())
            .unwrap_or_default()
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
                    let nodes = self.registry.session_nodes(session_id);
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
                Ok(self.registry.nodes.keys().cloned().collect())
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
    Event(EventEnvelope),
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
                body: MeshMessageBody::Event(event.clone()),
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
                        body: MeshMessageBody::Event(event),
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
}
