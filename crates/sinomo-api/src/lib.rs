//! Operator/debug/admin API skeleton over local services.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    Confidence, EventEnvelope, EventId, EventKind, LatencyClass, NodeId, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, Source, TaskId,
};
use sinomo_events::{EventStore, EventStoreError, ReplayQuery, StoredEvent};
use sinomo_node::{NodeAvailability, NodeProfile};
use sinomo_supervision::{SupervisionIndex, SupervisionSnapshot};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct HealthReport {
    pub node_id: NodeId,
    pub status: ApiHealthStatus,
    pub checked_nodes: usize,
    #[serde(with = "time::serde::rfc3339")]
    pub checked_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ApiHealthStatus {
    Healthy,
    Degraded,
    Offline,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OperatorCommand {
    pub command_id: TaskId,
    pub target_node_id: NodeId,
    pub kind: OperatorCommandKind,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum OperatorCommandKind {
    EngageKillSwitch,
    ReleaseKillSwitch,
    CancelTask { request_id: TaskId },
}

#[derive(Clone, Debug)]
pub struct ApiService {
    node_id: NodeId,
}

impl ApiService {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn health(&self, nodes: &[NodeProfile]) -> HealthReport {
        let status = if nodes
            .iter()
            .any(|node| node.health.availability == NodeAvailability::Offline)
        {
            ApiHealthStatus::Offline
        } else if nodes
            .iter()
            .any(|node| node.health.availability == NodeAvailability::Degraded)
        {
            ApiHealthStatus::Degraded
        } else {
            ApiHealthStatus::Healthy
        };

        HealthReport {
            node_id: self.node_id.clone(),
            status,
            checked_nodes: nodes.len(),
            checked_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn replay<S: EventStore>(
        &self,
        store: &S,
        query: ReplayQuery,
    ) -> Result<Vec<StoredEvent>, ApiError> {
        Ok(store.replay(query)?)
    }

    pub fn supervision_snapshot<S: EventStore>(
        &self,
        store: &S,
    ) -> Result<SupervisionSnapshot, ApiError> {
        let mut index = SupervisionIndex::new();
        for event in store.replay(ReplayQuery::all())? {
            index.ingest(event);
        }
        Ok(index.snapshot().clone())
    }

    pub fn operator_command_event(&self, command: OperatorCommand) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("api.operator").expect("valid source"),
            EventKind::new("operator.control.requested").expect("valid event kind"),
            Salience::new(0.82).expect("valid salience"),
            Confidence::new(0.96).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(command.target_node_id.clone())
        .with_payload(serde_json::json!({ "command": command }))
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ApiError {
    #[error(transparent)]
    EventStore(#[from] EventStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{EventKind, Source};
    use sinomo_events::InMemoryEventStore;

    #[test]
    fn api_exposes_replay_and_supervision_snapshot() {
        let node = NodeId::new("node_desktop").expect("node");
        let mut store = InMemoryEventStore::new();
        let event = EventEnvelope::new(
            EventId::new("evt_blocked").expect("event"),
            OffsetDateTime::now_utc(),
            node.clone(),
            Source::new("actions.executor").expect("source"),
            EventKind::new("action.sandbox.blocked").expect("kind"),
            Salience::new(0.88).expect("salience"),
            Confidence::new(0.95).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "requesting_node_id": "node_mobile",
            "executing_node_id": "node_desktop",
            "reason": "remote authority blocked",
        }));
        store.append(event).expect("append");

        let api = ApiService::new(node);
        let replay = api.replay(&store, ReplayQuery::all()).expect("replay");
        let snapshot = api.supervision_snapshot(&store).expect("snapshot");

        assert_eq!(replay.len(), 1);
        assert_eq!(snapshot.incidents.len(), 1);
    }
}
