//! In-process peer registry and event routing skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{EventEnvelope, LatencyClass, NodeId, RoutingScope, SessionId};
use sinomo_events::{EventSequence, StoredEvent};
use sinomo_node::NodeProfile;
use std::collections::{BTreeMap, BTreeSet};
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
}
