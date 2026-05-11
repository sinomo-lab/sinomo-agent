//! Event validation, append, and replay backbone.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    Confidence, EventEnvelope, EventKind, EvidenceRef, Id, LatencyClass, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_node::NodeId;
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

pub type EventId = sinomo_core::Id;
pub type SessionId = sinomo_core::Id;
pub type EntityId = sinomo_core::Id;
pub type InteractiveEnvironmentId = Id;
pub type InteractiveTrajectoryId = Id;
pub type InteractiveActionId = Id;
pub type InteractiveObservationId = Id;

pub const INTERACTIVE_STATE_OBSERVED: &str = "interactive.state.observed";
pub const INTERACTIVE_ACTION_REQUESTED: &str = "interactive.action.requested";
pub const INTERACTIVE_TRAJECTORY_STEP_RECORDED: &str = "interactive.trajectory.step.recorded";

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::StorageRing;
    layout.register_escape_path(
        "sinomo-events",
        StorageRing::R1,
        "events/interactive/trajectories",
        "interactive environment trajectory JSON, screenshots, and replay artifacts",
    )?;
    layout.register_escape_path(
        "sinomo-events",
        StorageRing::R1,
        "events/interactive/screenshots",
        "interactive environment screenshot artifacts",
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractiveEnvironmentDescriptor {
    pub environment_id: InteractiveEnvironmentId,
    pub kind: String,
    pub adapter: String,
    pub display_name: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InteractiveStatePacket {
    pub environment: InteractiveEnvironmentDescriptor,
    pub observation_id: InteractiveObservationId,
    pub trajectory_id: InteractiveTrajectoryId,
    pub step_index: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<InteractiveTask>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pose: Option<InteractivePose>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inventory: Vec<InteractiveInventoryItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nearby_blocks: Vec<InteractiveWorldObject>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nearby_entities: Vec<InteractiveWorldObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hunger: Option<f32>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractiveTask {
    pub task_id: String,
    pub instruction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InteractivePose {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub z: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yaw: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pitch: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractiveInventoryItem {
    pub slot: String,
    pub item: String,
    pub count: u32,
    #[serde(default)]
    pub selected: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InteractiveWorldObject {
    pub kind: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distance: Option<f32>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InteractiveActionMacro {
    pub action_id: InteractiveActionId,
    pub verb: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ticks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_condition: Option<String>,
    #[serde(default)]
    pub parameters: serde_json::Value,
}

impl InteractiveActionMacro {
    pub fn new(verb: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            action_id: InteractiveActionId::generate(),
            verb: verb.into(),
            target: target.into(),
            duration_ticks: None,
            stop_condition: None,
            parameters: serde_json::Value::Null,
        }
    }

    pub fn with_duration_ticks(mut self, duration_ticks: u32) -> Self {
        self.duration_ticks = Some(duration_ticks);
        self
    }

    pub fn with_stop_condition(mut self, stop_condition: impl Into<String>) -> Self {
        self.stop_condition = Some(stop_condition.into());
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractiveArtifactKind {
    Screenshot,
    TrajectoryJson,
    ReplayBundle,
    ActionTrace,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractiveArtifactRef {
    pub uri: String,
    pub kind: InteractiveArtifactKind,
    pub content_hash: String,
    pub byte_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InteractiveTrajectoryStep {
    pub trajectory_id: InteractiveTrajectoryId,
    pub step_index: u64,
    pub observation_id: InteractiveObservationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<InteractiveStatePacket>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<InteractiveActionMacro>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<InteractiveArtifactRef>,
}

pub fn interactive_state_event(
    origin_node_id: NodeId,
    session_id: SessionId,
    source: Source,
    state: InteractiveStatePacket,
    artifacts: Vec<InteractiveArtifactRef>,
) -> EventEnvelope {
    let mut event = EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        origin_node_id,
        source,
        EventKind::new(INTERACTIVE_STATE_OBSERVED).expect("event kind"),
        Salience::new(0.62).expect("salience"),
        Confidence::new(0.9).expect("confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session_id)
    .with_payload(serde_json::json!({
        "state": state,
        "artifacts": artifacts,
    }));
    event.evidence.refs = artifacts
        .iter()
        .map(|artifact| EvidenceRef {
            uri: artifact.uri.clone(),
            kind: match artifact.kind {
                InteractiveArtifactKind::Screenshot => sinomo_core::EvidenceKind::Screen,
                _ => sinomo_core::EvidenceKind::Artifact,
            },
            content_hash: Some(artifact.content_hash.clone()),
        })
        .collect();
    event
}

pub fn interactive_action_event(
    origin_node_id: NodeId,
    session_id: SessionId,
    source: Source,
    action: InteractiveActionMacro,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        origin_node_id,
        source,
        EventKind::new(INTERACTIVE_ACTION_REQUESTED).expect("event kind"),
        Salience::new(0.7).expect("salience"),
        Confidence::new(0.88).expect("confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Reactive,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session_id)
    .with_payload(serde_json::json!({ "action": action }))
}

pub fn trajectory_step_event(
    origin_node_id: NodeId,
    session_id: SessionId,
    source: Source,
    step: InteractiveTrajectoryStep,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        origin_node_id,
        source,
        EventKind::new(INTERACTIVE_TRAJECTORY_STEP_RECORDED).expect("event kind"),
        Salience::new(0.48).expect("salience"),
        Confidence::new(0.93).expect("confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session_id)
    .with_payload(serde_json::json!({ "step": step }))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InteractiveArtifactStore {
    root: PathBuf,
}

impl InteractiveArtifactStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn write_json_artifact<T: Serialize>(
        &self,
        trajectory_id: &InteractiveTrajectoryId,
        relative_name: impl AsRef<Path>,
        value: &T,
        kind: InteractiveArtifactKind,
    ) -> Result<InteractiveArtifactRef, InteractiveArtifactError> {
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|source| InteractiveArtifactError::Serialize(source.to_string()))?;
        self.write_binary_artifact(trajectory_id, relative_name, &bytes, kind)
    }

    pub fn write_binary_artifact(
        &self,
        trajectory_id: &InteractiveTrajectoryId,
        relative_name: impl AsRef<Path>,
        bytes: &[u8],
        kind: InteractiveArtifactKind,
    ) -> Result<InteractiveArtifactRef, InteractiveArtifactError> {
        let relative_name = normalize_artifact_name(relative_name.as_ref())?;
        let relative_path = Path::new("events")
            .join("interactive")
            .join("trajectories")
            .join(trajectory_id.as_str())
            .join(relative_name);
        let path = self.root.join(&relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| InteractiveArtifactError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        let mut file = fs::File::create(&path).map_err(|source| InteractiveArtifactError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        file.write_all(bytes)
            .map_err(|source| InteractiveArtifactError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        Ok(InteractiveArtifactRef {
            uri: format!("sinomo-data://{}", relative_path.to_string_lossy()),
            kind,
            content_hash: blake3::hash(bytes).to_hex().to_string(),
            byte_len: bytes.len() as u64,
        })
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum InteractiveArtifactError {
    #[error("invalid artifact path: {path}")]
    InvalidPath { path: String },
    #[error("failed to serialize artifact: {0}")]
    Serialize(String),
    #[error("artifact I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
}

fn normalize_artifact_name(path: &Path) -> Result<PathBuf, InteractiveArtifactError> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(InteractiveArtifactError::InvalidPath {
            path: path.display().to_string(),
        });
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => normalized.push(value),
            _ => {
                return Err(InteractiveArtifactError::InvalidPath {
                    path: path.display().to_string(),
                });
            }
        }
    }
    Ok(normalized)
}

#[derive(Clone, Debug, Default)]
pub struct EventValidator;

impl EventValidator {
    pub fn validate(&self, event: &EventEnvelope) -> Result<(), EventValidationError> {
        if event.source.as_str().trim().is_empty() {
            return Err(EventValidationError::EmptySource);
        }

        if event.kind.as_str().trim().is_empty() {
            return Err(EventValidationError::EmptyKind);
        }

        if let Some(span) = event.span
            && !span.is_valid()
        {
            return Err(EventValidationError::InvalidSpan {
                start_ms: span.start_ms,
                end_ms: span.end_ms,
            });
        }

        if event.links.references_event(&event.event_id) {
            return Err(EventValidationError::SelfReferentialLink {
                event_id: event.event_id.clone(),
            });
        }

        Ok(())
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum EventValidationError {
    #[error("event source cannot be empty")]
    EmptySource,
    #[error("event kind cannot be empty")]
    EmptyKind,
    #[error("event span start_ms {start_ms} cannot be greater than end_ms {end_ms}")]
    InvalidSpan { start_ms: u64, end_ms: u64 },
    #[error("event {event_id} cannot link to itself")]
    SelfReferentialLink { event_id: EventId },
}

pub trait EventStore {
    fn append(&mut self, event: EventEnvelope) -> Result<AppendReceipt, EventStoreError>;
    fn replay(&self, query: ReplayQuery) -> Result<Vec<StoredEvent>, EventStoreError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AppendReceipt {
    pub event_id: EventId,
    pub sequence: EventSequence,
    #[serde(with = "time::serde::rfc3339")]
    pub appended_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoredEvent {
    pub sequence: EventSequence,
    #[serde(with = "time::serde::rfc3339")]
    pub appended_at: OffsetDateTime,
    pub event: EventEnvelope,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EventSequence(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EventCursor {
    pub sequence: EventSequence,
}

impl From<EventSequence> for EventCursor {
    fn from(sequence: EventSequence) -> Self {
        Self { sequence }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReplayQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_sequence: Option<EventSequence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub start_time: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub end_time: Option<OffsetDateTime>,
    pub direction: ReplayDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl ReplayQuery {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn by_session(session_id: SessionId) -> Self {
        Self {
            session_id: Some(session_id),
            ..Self::default()
        }
    }

    pub fn by_kind_prefix(kind_prefix: impl Into<String>) -> Self {
        Self {
            kind_prefix: Some(kind_prefix.into()),
            ..Self::default()
        }
    }

    pub fn with_origin_node(mut self, node_id: NodeId) -> Self {
        self.origin_node_id = Some(node_id);
        self
    }

    pub fn after(mut self, sequence: EventSequence) -> Self {
        self.after_sequence = Some(sequence);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl Default for ReplayQuery {
    fn default() -> Self {
        Self {
            session_id: None,
            kind_prefix: None,
            origin_node_id: None,
            after_sequence: None,
            start_time: None,
            end_time: None,
            direction: ReplayDirection::Forward,
            limit: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ReplayDirection {
    Forward,
    Reverse,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum EventStoreError {
    #[error(transparent)]
    Validation(#[from] EventValidationError),
    #[error("event id already exists: {event_id}")]
    DuplicateEventId { event_id: EventId },
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryEventStore {
    validator: EventValidator,
    events: Vec<StoredEvent>,
    event_ids: HashSet<EventId>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_validator(validator: EventValidator) -> Self {
        Self {
            validator,
            events: Vec::new(),
            event_ids: HashSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn matches_query(stored: &StoredEvent, query: &ReplayQuery) -> bool {
        if let Some(session_id) = &query.session_id
            && stored.event.session_id.as_ref() != Some(session_id)
        {
            return false;
        }

        if let Some(kind_prefix) = &query.kind_prefix
            && !stored.event.kind.as_str().starts_with(kind_prefix)
        {
            return false;
        }

        if let Some(origin_node_id) = &query.origin_node_id
            && &stored.event.origin_node_id != origin_node_id
        {
            return false;
        }

        if let Some(after_sequence) = query.after_sequence
            && stored.sequence <= after_sequence
        {
            return false;
        }

        if let Some(start_time) = query.start_time
            && stored.event.time < start_time
        {
            return false;
        }

        if let Some(end_time) = query.end_time
            && stored.event.time > end_time
        {
            return false;
        }

        true
    }
}

impl EventStore for InMemoryEventStore {
    fn append(&mut self, event: EventEnvelope) -> Result<AppendReceipt, EventStoreError> {
        self.validator.validate(&event)?;

        if self.event_ids.contains(&event.event_id) {
            return Err(EventStoreError::DuplicateEventId {
                event_id: event.event_id,
            });
        }

        let sequence = EventSequence((self.events.len() as u64) + 1);
        let appended_at = OffsetDateTime::now_utc();
        let event_id = event.event_id.clone();

        self.event_ids.insert(event_id.clone());
        self.events.push(StoredEvent {
            sequence,
            appended_at,
            event,
        });

        Ok(AppendReceipt {
            event_id,
            sequence,
            appended_at,
        })
    }

    fn replay(&self, query: ReplayQuery) -> Result<Vec<StoredEvent>, EventStoreError> {
        let mut events: Vec<StoredEvent> = self
            .events
            .iter()
            .filter(|stored| Self::matches_query(stored, &query))
            .cloned()
            .collect();

        if query.direction == ReplayDirection::Reverse {
            events.reverse();
        }

        if let Some(limit) = query.limit {
            events.truncate(limit);
        }

        Ok(events)
    }
}

pub fn kind_has_prefix(kind: &EventKind, prefix: &str) -> bool {
    kind.as_str().starts_with(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        Confidence, EventEnvelope, EventKind, EventLinks, EventSpan, LatencyClass, PrivacyClass,
        RoutingMetadata, RoutingScope, Salience, Source,
    };
    use sinomo_node::NodeId;

    fn fixture_event(id: &str, session_id: Option<&str>, kind: &str) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            EventId::from_legacy_named(id).expect("event id"),
            OffsetDateTime::from_unix_timestamp(1_776_809_414).expect("timestamp"),
            NodeId::from_legacy_named("node_desktop").expect("node id"),
            Source::new("test.fixture").expect("source"),
            EventKind::new(kind).expect("kind"),
            Salience::new(0.5).expect("salience"),
            Confidence::new(0.9).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        );

        if let Some(session_id) = session_id {
            event =
                event.with_session(SessionId::from_legacy_named(session_id).expect("session id"));
        }

        event
    }

    #[test]
    fn validator_rejects_invalid_span() {
        let mut event = fixture_event("evt_invalid_span", None, "speech.transcript.final");
        event.span = Some(EventSpan::new(10, 1));

        let error = EventValidator
            .validate(&event)
            .expect_err("invalid span should fail");
        assert!(matches!(error, EventValidationError::InvalidSpan { .. }));
    }

    #[test]
    fn validator_rejects_self_reference() {
        let mut event = fixture_event("evt_self_ref", None, "agent.trace.created");
        event.links = EventLinks {
            caused_by: vec![EventId::from_legacy_named("evt_self_ref").expect("event id")],
            related_to: Vec::new(),
            supersedes: Vec::new(),
        };

        let error = EventValidator
            .validate(&event)
            .expect_err("self reference should fail");
        assert!(matches!(
            error,
            EventValidationError::SelfReferentialLink { .. }
        ));
    }

    #[test]
    fn store_rejects_duplicate_event_ids() {
        let mut store = InMemoryEventStore::new();
        let event = fixture_event("evt_duplicate", None, "speech.transcript.final");

        store.append(event.clone()).expect("first append");
        let error = store.append(event).expect_err("duplicate append");

        assert!(matches!(error, EventStoreError::DuplicateEventId { .. }));
    }

    #[test]
    fn replay_filters_by_session_kind_and_cursor() {
        let mut store = InMemoryEventStore::new();
        store
            .append(fixture_event(
                "evt_one",
                Some("sess_main"),
                "speech.transcript.partial",
            ))
            .expect("append one");
        let second = store
            .append(fixture_event(
                "evt_two",
                Some("sess_main"),
                "speech.transcript.final",
            ))
            .expect("append two");
        store
            .append(fixture_event(
                "evt_three",
                Some("sess_other"),
                "agent.trace.created",
            ))
            .expect("append three");

        let session_events = store
            .replay(ReplayQuery::by_session(
                SessionId::from_legacy_named("sess_main").expect("session id"),
            ))
            .expect("replay by session");
        assert_eq!(session_events.len(), 2);

        let speech_events = store
            .replay(ReplayQuery::by_kind_prefix("speech."))
            .expect("replay by kind");
        assert_eq!(speech_events.len(), 2);

        let after_second = store
            .replay(ReplayQuery::all().after(second.sequence))
            .expect("replay after cursor");
        assert_eq!(after_second.len(), 1);
        assert_eq!(
            after_second[0].event.event_id,
            EventId::from_legacy_named("evt_three").expect("event")
        );
    }

    #[test]
    fn interactive_state_action_and_trajectory_events_are_generic() {
        let node = NodeId::from_legacy_named("node_game").expect("node");
        let session = SessionId::from_legacy_named("sess_game").expect("session");
        let trajectory_id = InteractiveTrajectoryId::from_legacy_named("traj_game").expect("traj");
        let state = InteractiveStatePacket {
            environment: InteractiveEnvironmentDescriptor {
                environment_id: InteractiveEnvironmentId::from_legacy_named("env_minecraft")
                    .expect("env"),
                kind: "voxel-survival".to_string(),
                adapter: "minestudio".to_string(),
                display_name: "MineStudio".to_string(),
                metadata: serde_json::Value::Null,
            },
            observation_id: InteractiveObservationId::from_legacy_named("obs_game").expect("obs"),
            trajectory_id: trajectory_id.clone(),
            step_index: 7,
            task: Some(InteractiveTask {
                task_id: "collect_dirt".to_string(),
                instruction: "Collect dirt blocks from the ground.".to_string(),
                source_uri: Some("mcu://simple/collect_dirt".to_string()),
            }),
            pose: None,
            inventory: vec![InteractiveInventoryItem {
                slot: "hotbar.0".to_string(),
                item: "minecraft:wooden_shovel".to_string(),
                count: 1,
                selected: true,
            }],
            nearby_blocks: Vec::new(),
            nearby_entities: Vec::new(),
            health: Some(20.0),
            hunger: Some(20.0),
            metadata: serde_json::Value::Null,
        };
        let action = InteractiveActionMacro::new("dig", "ground")
            .with_duration_ticks(20)
            .with_stop_condition("block broken");

        let state_event = interactive_state_event(
            node.clone(),
            session.clone(),
            Source::new("benchmark.minestudio").expect("source"),
            state.clone(),
            Vec::new(),
        );
        let action_event = interactive_action_event(
            node.clone(),
            session.clone(),
            Source::new("benchmark.minestudio").expect("source"),
            action.clone(),
        );
        let step_event = trajectory_step_event(
            node,
            session,
            Source::new("benchmark.minestudio").expect("source"),
            InteractiveTrajectoryStep {
                trajectory_id,
                step_index: 7,
                observation_id: state.observation_id.clone(),
                state: Some(state),
                action: Some(action),
                artifacts: Vec::new(),
            },
        );

        assert_eq!(state_event.kind.as_str(), INTERACTIVE_STATE_OBSERVED);
        assert_eq!(action_event.kind.as_str(), INTERACTIVE_ACTION_REQUESTED);
        assert_eq!(
            step_event.kind.as_str(),
            INTERACTIVE_TRAJECTORY_STEP_RECORDED
        );
        assert_eq!(step_event.payload["step"]["action"]["verb"], "dig");
    }

    #[test]
    fn interactive_artifact_store_writes_under_registered_tree() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-events-artifact-test-{}",
            InteractiveTrajectoryId::generate()
        ));
        let store = InteractiveArtifactStore::new(&root);
        let trajectory_id =
            InteractiveTrajectoryId::from_legacy_named("traj_artifact").expect("trajectory");
        let artifact = store
            .write_json_artifact(
                &trajectory_id,
                "step-0001/state.json",
                &serde_json::json!({"ok": true}),
                InteractiveArtifactKind::TrajectoryJson,
            )
            .expect("write artifact");

        assert!(artifact.uri.contains("events/interactive/trajectories"));
        assert_eq!(artifact.byte_len, 16);
        let written = root
            .join("events")
            .join("interactive")
            .join("trajectories")
            .join(trajectory_id.as_str())
            .join("step-0001/state.json");
        assert!(written.exists());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn interactive_artifact_store_rejects_path_escape() {
        let store = InteractiveArtifactStore::new(std::env::temp_dir());
        let trajectory_id =
            InteractiveTrajectoryId::from_legacy_named("traj_escape").expect("trajectory");
        let error = store
            .write_binary_artifact(
                &trajectory_id,
                "../outside.png",
                b"nope",
                InteractiveArtifactKind::Screenshot,
            )
            .expect_err("escape must fail");
        assert!(matches!(
            error,
            InteractiveArtifactError::InvalidPath { .. }
        ));
    }

    #[test]
    fn interactive_storage_paths_are_registered() {
        let mut layout = sinomo_storage::StorageLayout::new();
        register_storage_paths(&mut layout).expect("register");
        assert!(
            layout
                .owner_for("events/interactive/trajectories/01hxy6qa9k0t9vr4g9n8d3q5bf/step.json")
                .is_some()
        );
        assert!(
            layout
                .owner_for("events/interactive/screenshots/shot.png")
                .is_some()
        );
    }
}
