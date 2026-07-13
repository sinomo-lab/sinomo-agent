//! Event validation, append, and replay backbone.

use serde::{Deserialize, Serialize};
use sinomo_base::{
    Confidence, EventEnvelope, EventKind, EvidenceRef, Id, LatencyClass, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_node::NodeId;
use std::collections::HashSet;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

pub type EventId = sinomo_base::Id;
pub type SessionId = sinomo_base::Id;
pub type EntityId = sinomo_base::Id;
pub type InteractiveEnvironmentId = Id;
pub type InteractiveTrajectoryId = Id;
pub type InteractiveActionId = Id;
pub type InteractiveObservationId = Id;

pub const INTERACTIVE_STATE_OBSERVED: &str = "interactive.state.observed";
pub const INTERACTIVE_ACTION_REQUESTED: &str = "interactive.action.requested";
pub const INTERACTIVE_TRAJECTORY_STEP_RECORDED: &str = "interactive.trajectory.step.recorded";
pub const EVENT_LOG_RELATIVE_PATH: &str = "events/event-log.jsonl";
pub const EVENT_LOG_LOCK_RELATIVE_PATH: &str = "events/event-log.jsonl.lock";

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
    )?;
    layout.register_human_text("sinomo-events", StorageRing::R1, EVENT_LOG_RELATIVE_PATH)?;
    layout.register_binary(
        "sinomo-events",
        StorageRing::R1,
        EVENT_LOG_LOCK_RELATIVE_PATH,
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
                InteractiveArtifactKind::Screenshot => sinomo_base::EvidenceKind::Screen,
                _ => sinomo_base::EvidenceKind::Artifact,
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
    #[error("event store io error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("event store serialization error at {path}: {detail}")]
    Serialization { path: String, detail: String },
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

#[derive(Clone, Debug)]
pub struct JsonlEventStore {
    validator: EventValidator,
    path: PathBuf,
}

impl JsonlEventStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            validator: EventValidator,
            path: path.into(),
        }
    }

    pub fn with_validator(path: impl Into<PathBuf>, validator: EventValidator) -> Self {
        Self {
            validator,
            path: path.into(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn lock_path(&self) -> PathBuf {
        let mut name = self
            .path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("event-log"))
            .to_os_string();
        name.push(".lock");
        self.path.with_file_name(name)
    }

    fn acquire_exclusive_lock(&self) -> Result<fs::File, EventStoreError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|source| EventStoreError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        let lock_path = self.lock_path();
        let lock = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| EventStoreError::Io {
                path: lock_path.display().to_string(),
                detail: source.to_string(),
            })?;
        fs2::FileExt::lock_exclusive(&lock).map_err(|source| EventStoreError::Io {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(lock)
    }

    /// Make every previously appended event durable before a caller removes
    /// external recovery evidence that protects replay. This syncs both the log
    /// contents and, when present, the parent directory entry.
    pub fn sync_all(&self) -> Result<(), EventStoreError> {
        let _lock = self.acquire_exclusive_lock()?;
        let file = match fs::File::open(&self.path) {
            Ok(file) => file,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(EventStoreError::Io {
                    path: self.path.display().to_string(),
                    detail: source.to_string(),
                });
            }
        };
        file.sync_all().map_err(|source| EventStoreError::Io {
            path: self.path.display().to_string(),
            detail: source.to_string(),
        })?;
        if let Some(parent) = self.path.parent() {
            sinomo_storage::durable_file::sync_directory(parent).map_err(|source| {
                EventStoreError::Io {
                    path: parent.display().to_string(),
                    detail: source.to_string(),
                }
            })?;
        }
        Ok(())
    }

    fn read_events(&self) -> Result<Vec<StoredEvent>, EventStoreError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        // Callers hold the sidecar lock, so repairing a tail interrupted between
        // writing JSON and its newline cannot race another append. A malformed
        // newline-terminated record is not repairable and remains a hard error.
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|source| EventStoreError::Io {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| EventStoreError::Io {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })?;
        let mut events = Vec::new();
        let mut offset = 0_usize;
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            let newline = remaining.iter().position(|byte| *byte == b'\n');
            let (raw_line, terminated, next_offset) = match newline {
                Some(index) => (&remaining[..index], true, offset + index + 1),
                None => (remaining, false, bytes.len()),
            };
            let raw_line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
            if raw_line.iter().all(u8::is_ascii_whitespace) {
                offset = next_offset;
                continue;
            }
            match serde_json::from_slice::<StoredEvent>(raw_line) {
                Ok(stored) => {
                    events.push(stored);
                    if !terminated {
                        // The JSON payload became durable but the trailing newline
                        // did not. Preserve the valid record and terminate it before
                        // a later append, which would otherwise concatenate JSON.
                        file.seek(SeekFrom::End(0))
                            .and_then(|_| file.write_all(b"\n"))
                            .and_then(|_| file.sync_all())
                            .map_err(|source| EventStoreError::Io {
                                path: self.path.display().to_string(),
                                detail: source.to_string(),
                            })?;
                    }
                }
                Err(_) if !terminated => {
                    // Only an invalid, non-newline-terminated final record is
                    // provably a torn append. Keep every complete prior record.
                    file.set_len(offset as u64)
                        .and_then(|_| file.sync_all())
                        .map_err(|source| EventStoreError::Io {
                            path: self.path.display().to_string(),
                            detail: source.to_string(),
                        })?;
                    break;
                }
                Err(source) => {
                    return Err(EventStoreError::Serialization {
                        path: self.path.display().to_string(),
                        detail: source.to_string(),
                    });
                }
            }
            offset = next_offset;
        }
        Ok(events)
    }
}

impl EventStore for JsonlEventStore {
    fn append(&mut self, event: EventEnvelope) -> Result<AppendReceipt, EventStoreError> {
        self.validator.validate(&event)?;
        // Sequence allocation, duplicate detection, and append form one
        // cross-thread/process critical section. Without this file lock, two
        // daemon services can both observe the same tail and allocate it twice.
        let _lock = self.acquire_exclusive_lock()?;
        let events = self.read_events()?;
        if events
            .iter()
            .any(|stored| stored.event.event_id == event.event_id)
        {
            return Err(EventStoreError::DuplicateEventId {
                event_id: event.event_id,
            });
        }
        let sequence =
            EventSequence(events.last().map(|stored| stored.sequence.0).unwrap_or(0) + 1);
        let appended_at = OffsetDateTime::now_utc();
        let event_id = event.event_id.clone();
        let stored = StoredEvent {
            sequence,
            appended_at,
            event,
        };
        let line =
            serde_json::to_string(&stored).map_err(|source| EventStoreError::Serialization {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| EventStoreError::Io {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })?;
        writeln!(file, "{line}").map_err(|source| EventStoreError::Io {
            path: self.path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(AppendReceipt {
            event_id,
            sequence,
            appended_at,
        })
    }

    fn replay(&self, query: ReplayQuery) -> Result<Vec<StoredEvent>, EventStoreError> {
        let _lock = self.acquire_exclusive_lock()?;
        let mut events: Vec<StoredEvent> = self
            .read_events()?
            .into_iter()
            .filter(|stored| InMemoryEventStore::matches_query(stored, &query))
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
    use sinomo_base::{
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
    fn jsonl_event_store_persists_and_replays_events() {
        let root = sinomo_testdir::TestDir::new("sinomo-events-jsonl-test");
        let path = root.join("events").join("event-log.jsonl");
        let mut store = JsonlEventStore::new(&path);
        let event = fixture_event("evt_jsonl", None, "sandbox.instance.created");

        let receipt = store.append(event.clone()).expect("append");
        assert_eq!(receipt.sequence, EventSequence(1));
        store.sync_all().expect("durably sync event log");

        let reopened = JsonlEventStore::new(&path);
        let events = reopened
            .replay(ReplayQuery::by_kind_prefix("sandbox.instance"))
            .expect("replay");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, EventSequence(1));
        assert_eq!(events[0].event.event_id, event.event_id);
        assert_eq!(events[0].event.kind.as_str(), "sandbox.instance.created");
    }

    #[test]
    fn jsonl_event_store_sync_is_idempotent_before_and_after_creation() {
        let root = sinomo_testdir::TestDir::new("sinomo-events-jsonl-sync-test");
        let path = root.join("events").join("event-log.jsonl");
        let mut store = JsonlEventStore::new(&path);
        store.sync_all().expect("missing log is already durable");
        store
            .append(fixture_event(
                "evt_jsonl_sync",
                None,
                "sandbox.instance.created",
            ))
            .expect("append");
        store.sync_all().expect("first sync");
        store.sync_all().expect("repeat sync");
    }

    #[test]
    fn jsonl_event_store_rejects_duplicate_event_ids() {
        let root = sinomo_testdir::TestDir::new("sinomo-events-jsonl-duplicate-test");
        let path = root.join("events").join("event-log.jsonl");
        let mut store = JsonlEventStore::new(&path);
        let event = fixture_event("evt_jsonl_duplicate", None, "sandbox.instance.created");

        store.append(event.clone()).expect("first append");
        let error = store.append(event).expect_err("duplicate append");
        assert!(matches!(error, EventStoreError::DuplicateEventId { .. }));
    }

    #[test]
    fn jsonl_event_store_recovers_only_a_torn_final_record() {
        let root = sinomo_testdir::TestDir::new("sinomo-events-jsonl-torn-tail-test");
        let path = root.join("events").join("event-log.jsonl");
        let mut store = JsonlEventStore::new(&path);
        store
            .append(fixture_event(
                "evt_jsonl_before_torn_one",
                None,
                "sandbox.command.completed",
            ))
            .expect("first append");
        store
            .append(fixture_event(
                "evt_jsonl_before_torn_two",
                None,
                "sandbox.command.completed",
            ))
            .expect("second append");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open event log tail");
        file.write_all(br#"{"sequence":3,"event"#)
            .expect("seed torn final record");
        file.sync_all().expect("sync torn tail");

        let replayed = store.replay(ReplayQuery::all()).expect("repair and replay");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].sequence, EventSequence(1));
        assert_eq!(replayed[1].sequence, EventSequence(2));

        let after_torn = fixture_event("evt_jsonl_after_torn", None, "sandbox.command.completed");
        let receipt = store
            .append(after_torn.clone())
            .expect("append after repairing torn tail");
        assert_eq!(receipt.sequence, EventSequence(3));
        let replayed = store.replay(ReplayQuery::all()).expect("final replay");
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[2].event.event_id, after_torn.event_id);
    }

    #[test]
    fn jsonl_event_store_serializes_concurrent_sequence_allocation_and_duplicates() {
        const WRITERS: usize = 12;
        let root = sinomo_testdir::TestDir::new("sinomo-events-jsonl-concurrent-test");
        let path = root.join("events").join("event-log.jsonl");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(WRITERS));
        let mut joins = Vec::new();
        for index in 0..WRITERS {
            let barrier = std::sync::Arc::clone(&barrier);
            let path = path.clone();
            joins.push(std::thread::spawn(move || {
                let mut store = JsonlEventStore::new(path);
                let event = fixture_event(
                    &format!("evt_jsonl_concurrent_{index}"),
                    None,
                    "sandbox.command.completed",
                );
                barrier.wait();
                store.append(event)
            }));
        }
        let mut sequences = joins
            .into_iter()
            .map(|join| {
                join.join()
                    .expect("writer thread")
                    .expect("append")
                    .sequence
                    .0
            })
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=WRITERS as u64).collect::<Vec<_>>());

        let duplicate = fixture_event(
            "evt_jsonl_concurrent_duplicate",
            None,
            "sandbox.command.completed",
        );
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let joins = (0..2)
            .map(|_| {
                let path = path.clone();
                let event = duplicate.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut store = JsonlEventStore::new(path);
                    barrier.wait();
                    store.append(event)
                })
            })
            .collect::<Vec<_>>();
        let results = joins
            .into_iter()
            .map(|join| join.join().expect("duplicate writer thread"))
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(EventStoreError::DuplicateEventId { .. })))
                .count(),
            1
        );
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
        let root = sinomo_testdir::TestDir::new("sinomo-events-artifact-test");
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
    }

    #[test]
    fn interactive_artifact_store_rejects_path_escape() {
        let root = sinomo_testdir::TestDir::new("sinomo-events-artifact-escape");
        let store = InteractiveArtifactStore::new(&root);
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
        assert!(layout.owner_for(EVENT_LOG_RELATIVE_PATH).is_some());
        assert!(layout.owner_for(EVENT_LOG_LOCK_RELATIVE_PATH).is_some());
    }
}
