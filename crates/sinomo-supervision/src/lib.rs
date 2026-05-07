//! Supervision incident traces and audit view skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{DeviceId, EventId, NodeId};
use sinomo_events::{EventSequence, StoredEvent};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SupervisionSnapshot {
    #[serde(default)]
    pub incidents: Vec<IncidentTrace>,
    #[serde(default)]
    pub workspace_audits: Vec<WorkspaceAuditView>,
    #[serde(default)]
    pub artifact_audits: Vec<ArtifactAuditView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IncidentTrace {
    pub event_id: EventId,
    pub sequence: EventSequence,
    pub kind: String,
    pub requesting_node_id: Option<NodeId>,
    pub executing_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub target_device_id: Option<DeviceId>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkspaceAuditView {
    pub event_id: EventId,
    pub sequence: EventSequence,
    pub kind: String,
    pub workspace_id: Option<String>,
    pub object_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ArtifactAuditView {
    pub event_id: EventId,
    pub sequence: EventSequence,
    pub kind: String,
    pub artifact_id: Option<String>,
    pub rollout_plan_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SupervisionIndex {
    snapshot: SupervisionSnapshot,
}

impl SupervisionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ingest(&mut self, stored: StoredEvent) {
        let kind = stored.event.kind.as_str().to_string();
        if is_incident_kind(&kind) {
            self.snapshot.incidents.push(IncidentTrace {
                event_id: stored.event.event_id.clone(),
                sequence: stored.sequence,
                kind: kind.clone(),
                requesting_node_id: node_from_payload(&stored, "requesting_node_id")
                    .or_else(|| Some(stored.event.origin_node_id.clone())),
                executing_node_id: node_from_payload(&stored, "executing_node_id"),
                target_node_id: stored.event.target_node_id.clone(),
                target_device_id: stored.event.target_device_id.clone(),
                reason: string_from_payload(&stored, "reason"),
            });
        }

        if kind.starts_with("dfs.") {
            self.snapshot.workspace_audits.push(WorkspaceAuditView {
                event_id: stored.event.event_id.clone(),
                sequence: stored.sequence,
                kind: kind.clone(),
                workspace_id: string_from_payload(&stored, "workspace_id"),
                object_id: string_from_payload(&stored, "object_id"),
                reason: string_from_payload(&stored, "reason"),
            });
        }

        if kind.starts_with("module.")
            || kind.starts_with("registry.")
            || kind.starts_with("rollout.")
        {
            self.snapshot.artifact_audits.push(ArtifactAuditView {
                event_id: stored.event.event_id.clone(),
                sequence: stored.sequence,
                kind,
                artifact_id: string_from_payload(&stored, "artifact_id"),
                rollout_plan_id: string_from_payload(&stored, "rollout_plan_id"),
                reason: string_from_payload(&stored, "reason"),
            });
        }
    }

    pub fn snapshot(&self) -> &SupervisionSnapshot {
        &self.snapshot
    }
}

pub fn is_incident_kind(kind: &str) -> bool {
    kind.starts_with("safety.")
        || kind == "action.sandbox.blocked"
        || kind == "dfs.policy.violation"
        || kind == "module.policy.violation"
}

fn string_from_payload(stored: &StoredEvent, key: &str) -> Option<String> {
    stored
        .event
        .payload
        .get(key)
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
}

fn node_from_payload(stored: &StoredEvent, key: &str) -> Option<NodeId> {
    string_from_payload(stored, key).and_then(|value| NodeId::new(value).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        Confidence, EventEnvelope, EventKind, LatencyClass, PrivacyClass, RoutingMetadata,
        RoutingScope, Salience, Source,
    };
    use time::OffsetDateTime;

    #[test]
    fn index_collects_incident_and_workspace_views() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let event = EventEnvelope::new(
            EventId::from_legacy_named("evt_blocked").expect("event"),
            OffsetDateTime::now_utc(),
            node.clone(),
            Source::new("dfs.supervision").expect("source"),
            EventKind::new("dfs.policy.violation").expect("kind"),
            Salience::new(0.9).expect("salience"),
            Confidence::new(0.95).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "requesting_node_id": node,
            "workspace_id": "workspace_test",
            "object_id": "obj_test",
            "reason": "owner mismatch",
        }));
        let stored = StoredEvent {
            sequence: EventSequence(1),
            appended_at: OffsetDateTime::now_utc(),
            event,
        };

        let mut index = SupervisionIndex::new();
        index.ingest(stored);

        assert_eq!(index.snapshot().incidents.len(), 1);
        assert_eq!(index.snapshot().workspace_audits.len(), 1);
    }
}
