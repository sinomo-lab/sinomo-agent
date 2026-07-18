use crate::TaskId;
use crate::agent::{
    AgentAbortRecord, AgentCompactionSummary, AgentEvent, AgentFollowUpMessage, AgentItem,
    AgentRole, AgentRunContext, AgentSession, AgentSessionControlSnapshot, AgentSessionProjection,
    AgentSteeringMessage, AgentStopReason, AgentStoragePaths, AgentTaskLifecycleStatus,
    AgentTaskSnapshot, agent_item_id,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_lm::{RealtimeAudioSampleFormat, RealtimeVideoFrameFormat};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{
    VfsAppendOptions, VfsDirectoryCursor, VfsError, VfsHandle, VfsObjectKind, VfsRemoveOptions,
    VfsWriteOptions, mounted_sifs_local_reads,
};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};

pub const AGENT_SESSION_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const AGENT_SESSION_AUDIO_SEGMENT_SCHEMA_VERSION: u32 = 1;
pub const AGENT_SESSION_VIDEO_FRAME_SCHEMA_VERSION: u32 = 1;
pub const AGENT_TASK_ACTIVITY_PROJECTION_SCHEMA_VERSION: u32 = 1;
pub const AGENT_TASK_ACTIVITY_SIFS_ROOT: &str = "/services/sinomo/agent-task-activity";
const AGENT_TASK_ACTIVITY_PENDING_MIRROR_DIR: &str = "roaming-pending";
const AGENT_TASK_ACTIVITY_PENDING_RETRY_LIMIT: usize = 8;
const MAX_AGENT_TASK_ACTIVITY_OWNER_BYTES: usize = 1_024;
const MAX_AGENT_TASK_ACTIVITY_KIND_BYTES: usize = 64;
const MAX_AGENT_TASK_ACTIVITY_SUMMARY_BYTES: usize = 512;
const MAX_AGENT_TASK_ACTIVITY_SUMMARY_SCAN_BYTES: usize = MAX_AGENT_TASK_ACTIVITY_SUMMARY_BYTES * 4;
const FALLBACK_AGENT_TASK_ACTIVITY_KIND: &str = "background_task";
const FALLBACK_AGENT_TASK_ACTIVITY_SUMMARY: &str = "Background activity";

/// Provider-neutral, agent-owned background activity exposed to historical
/// recall.
///
/// This is intentionally not an [`AgentTaskSnapshot`]. A control snapshot may
/// contain private execution details such as model-session artifact handles,
/// active operation identifiers, and raw event identifiers. Only fields that
/// are stable across model and provider session formats enter this projection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTaskActivityProjection {
    pub activity_id: TaskId,
    pub owner_agent_key: String,
    pub activity_kind: String,
    pub summary: String,
    pub status: AgentTaskLifecycleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_summary: Option<String>,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
}

impl AgentTaskActivityProjection {
    fn from_snapshot(snapshot: &AgentTaskSnapshot) -> Option<Self> {
        let owner_agent_key = snapshot
            .agent_definition_key
            .as_deref()
            .filter(|key| valid_activity_owner(key))?
            .to_string();
        Some(Self {
            activity_id: snapshot.task.task_id.clone(),
            owner_agent_key,
            activity_kind: sanitize_activity_kind(&snapshot.task.kind),
            // `AgentTaskRef::summary` is the user-facing task description. It
            // is the only free-form private snapshot field eligible for this
            // projection, and it still passes through a bounded sanitizer.
            summary: sanitize_activity_summary(&snapshot.task.summary),
            status: snapshot.status.clone(),
            // Runtime progress may contain provider errors, cancellation
            // reasons, host paths, or model-session handles. Never copy it to
            // the roaming/searchable record; derive a stable label instead.
            progress_summary: Some(activity_status_label(&snapshot.status).to_string()),
            updated_at: snapshot.updated_at.clone(),
        })
    }

    fn sanitized_for_roaming(&self) -> Result<Self, AgentSessionStoreError> {
        if !valid_activity_owner(&self.owner_agent_key) {
            return Err(AgentSessionStoreError::Serialization {
                detail: "task activity projection owner is invalid".to_string(),
            });
        }
        Ok(Self {
            activity_id: self.activity_id.clone(),
            owner_agent_key: self.owner_agent_key.clone(),
            activity_kind: sanitize_activity_kind(&self.activity_kind),
            summary: sanitize_activity_summary(&self.summary),
            status: self.status.clone(),
            progress_summary: Some(activity_status_label(&self.status).to_string()),
            updated_at: self.updated_at,
        })
    }
}

fn valid_activity_owner(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= MAX_AGENT_TASK_ACTIVITY_OWNER_BYTES
        && !value.chars().any(char::is_control)
        && !looks_like_private_activity_reference(value)
}

fn sanitize_activity_kind(value: &str) -> String {
    let value = value.trim();
    if value.is_empty()
        || value.len() > MAX_AGENT_TASK_ACTIVITY_KIND_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        || looks_like_private_activity_reference(value)
    {
        return FALLBACK_AGENT_TASK_ACTIVITY_KIND.to_string();
    }
    value.to_ascii_lowercase()
}

fn sanitize_activity_summary(value: &str) -> String {
    let normalized = bounded_normalized_activity_text(value);
    let lower = normalized.to_ascii_lowercase();
    if normalized.is_empty()
        || [
            "provider error:",
            "provider failure:",
            "model error:",
            "model failure:",
            "cancelled:",
            "canceled:",
            "cancellation reason:",
            "failed:",
        ]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
    {
        return FALLBACK_AGENT_TASK_ACTIVITY_SUMMARY.to_string();
    }

    let mut sanitized =
        String::with_capacity(normalized.len().min(MAX_AGENT_TASK_ACTIVITY_SUMMARY_BYTES));
    for token in normalized.split(' ') {
        let token = if looks_like_private_activity_reference(token) {
            "[redacted]"
        } else {
            token
        };
        let separator_bytes = usize::from(!sanitized.is_empty());
        if sanitized.len() + separator_bytes >= MAX_AGENT_TASK_ACTIVITY_SUMMARY_BYTES {
            break;
        }
        if separator_bytes == 1 {
            sanitized.push(' ');
        }
        let remaining = MAX_AGENT_TASK_ACTIVITY_SUMMARY_BYTES - sanitized.len();
        let mut end = token.len().min(remaining);
        while end > 0 && !token.is_char_boundary(end) {
            end -= 1;
        }
        sanitized.push_str(&token[..end]);
        if end < token.len() {
            break;
        }
    }
    if sanitized.is_empty() {
        FALLBACK_AGENT_TASK_ACTIVITY_SUMMARY.to_string()
    } else {
        sanitized
    }
}

fn bounded_normalized_activity_text(value: &str) -> String {
    let mut normalized =
        String::with_capacity(value.len().min(MAX_AGENT_TASK_ACTIVITY_SUMMARY_SCAN_BYTES));
    let mut pending_space = false;
    for character in value.chars() {
        if character.is_control() || character.is_whitespace() {
            pending_space = !normalized.is_empty();
            continue;
        }
        let separator_bytes = usize::from(pending_space && !normalized.is_empty());
        if normalized.len() + separator_bytes + character.len_utf8()
            > MAX_AGENT_TASK_ACTIVITY_SUMMARY_SCAN_BYTES
        {
            break;
        }
        if separator_bytes == 1 {
            normalized.push(' ');
        }
        normalized.push(character);
        pending_space = false;
    }
    normalized
}

fn looks_like_private_activity_reference(value: &str) -> bool {
    let value = value.trim_matches(|character: char| {
        matches!(
            character,
            '(' | ')' | '[' | ']' | '{' | '}' | '<' | '>' | '"' | '\''
        )
    });
    let lower = value.to_ascii_lowercase();
    let bytes = value.as_bytes();
    let windows_absolute = bytes.windows(3).any(|window| {
        window[0].is_ascii_alphabetic() && window[1] == b':' && matches!(window[2], b'/' | b'\\')
    });
    let detail_separator = value
        .bytes()
        .any(|byte| matches!(byte, b'=' | b':' | b'/' | b'\\' | b'_'));
    value.starts_with('/')
        || value.starts_with("~/")
        || value.starts_with("~\\")
        || value.starts_with("\\\\")
        || value.starts_with("//")
        || value.contains("=/")
        || value.contains("=\\")
        || value.contains("://")
        || windows_absolute
        || (lower.contains("session") && (detail_separator || value.len() > 32))
        || ((lower.contains("provider") || lower.contains("model"))
            && (lower.contains("error") || lower.contains("failure") || lower.contains("failed")))
}

fn activity_status_label(status: &AgentTaskLifecycleStatus) -> &'static str {
    match status {
        AgentTaskLifecycleStatus::Pending => "queued",
        AgentTaskLifecycleStatus::Running => "in progress",
        AgentTaskLifecycleStatus::Completed => "completed",
        AgentTaskLifecycleStatus::Failed => "failed",
        AgentTaskLifecycleStatus::Cancelled => "cancelled",
        AgentTaskLifecycleStatus::TimedOut => "timed out",
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct AgentTaskActivityProjectionRecord {
    schema_version: u32,
    activity: AgentTaskActivityProjection,
}

impl AgentTaskActivityProjectionRecord {
    fn new(activity: AgentTaskActivityProjection) -> Self {
        Self {
            schema_version: AGENT_TASK_ACTIVITY_PROJECTION_SCHEMA_VERSION,
            activity,
        }
    }
}

/// One bounded page from the latest typed background-activity projection.
///
/// `available` is false when this agent has no new-format projection. Callers
/// must not fall back to raw model-session storage; they can surface a partial
/// result instead.
#[derive(Clone, Debug)]
pub struct AgentTaskActivityProjectionPage {
    pub activities: Vec<AgentTaskActivityProjection>,
    /// Directory continuation immediately after a record that was deferred
    /// because its body did not fit the caller's remaining byte budget. This
    /// cursor may be used only when explicitly skipping an intrinsically
    /// oversized record.
    pub next_cursor: Option<VfsDirectoryCursor>,
    /// Directory continuation immediately before the deferred record. Retry
    /// this cursor with a fresh hard budget instead of advancing past it.
    pub deferred_cursor: Option<VfsDirectoryCursor>,
    pub consumed_entries: usize,
    pub omitted_entries: usize,
    pub read_bytes: u64,
    pub available: bool,
    /// Serialized size of the first otherwise-valid record that did not fit.
    pub required_bytes: Option<u64>,
    pub deferred_for_fresh_budget: bool,
}

pub trait AgentSessionStore: Send + Sync {
    fn load_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSession>, AgentSessionStoreError>;

    fn save_session(&self, session: &AgentSession) -> Result<(), AgentSessionStoreError>;

    fn append_items(
        &self,
        session_id: &SessionId,
        items: &[AgentItem],
    ) -> Result<(), AgentSessionStoreError>;

    fn append_projection(
        &self,
        session_id: &SessionId,
        projection: &AgentSessionProjection,
    ) -> Result<(), AgentSessionStoreError>;

    fn append_record(&self, record: &AgentSessionStoreRecord)
    -> Result<(), AgentSessionStoreError>;

    fn load_control_snapshot(
        &self,
        _session_id: &SessionId,
    ) -> Result<Option<AgentSessionControlSnapshot>, AgentSessionStoreError> {
        Ok(None)
    }

    fn load_control_events(
        &self,
        _session_id: &SessionId,
    ) -> Result<Vec<AgentEvent>, AgentSessionStoreError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionSnapshot {
    pub schema_version: u32,
    pub session: AgentSession,
}

impl AgentSessionSnapshot {
    pub fn new(session: AgentSession) -> Self {
        Self {
            schema_version: AGENT_SESSION_SNAPSHOT_SCHEMA_VERSION,
            session,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionStoreRecord {
    pub record_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub recorded_at: time::OffsetDateTime,
    pub record: AgentSessionStoreRecordKind,
}

impl AgentSessionStoreRecord {
    pub fn new(session_id: SessionId, record: AgentSessionStoreRecordKind) -> Self {
        Self {
            record_id: TaskId::generate(),
            session_id,
            recorded_at: time::OffsetDateTime::now_utc(),
            record,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionItemRecord {
    pub record_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub recorded_at: time::OffsetDateTime,
    pub item_id: TaskId,
    pub item: AgentItem,
}

impl AgentSessionItemRecord {
    pub fn new(session_id: SessionId, item: AgentItem) -> Self {
        let item_id = agent_item_id(&item);
        Self {
            record_id: TaskId::generate(),
            session_id,
            recorded_at: time::OffsetDateTime::now_utc(),
            item_id,
            item,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionProjectionRecord {
    pub record_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub recorded_at: time::OffsetDateTime,
    pub projection: AgentSessionProjection,
}

impl AgentSessionProjectionRecord {
    pub fn new(session_id: SessionId, projection: AgentSessionProjection) -> Self {
        Self {
            record_id: TaskId::generate(),
            session_id,
            recorded_at: time::OffsetDateTime::now_utc(),
            projection,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentAudioDirection {
    Input,
    Output,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentAudioSegmentInput {
    pub direction: AgentAudioDirection,
    pub sequence: u64,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    pub sample_rate_hz: u32,
    pub channels: u8,
    pub sample_format: RealtimeAudioSampleFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default)]
    pub end_of_turn: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub metadata: serde_json::Value,
}

impl AgentAudioSegmentInput {
    pub fn new(
        direction: AgentAudioDirection,
        sequence: u64,
        provider: impl Into<String>,
        model: impl Into<String>,
        sample_rate_hz: u32,
        sample_format: RealtimeAudioSampleFormat,
    ) -> Self {
        Self {
            direction,
            sequence,
            provider: provider.into(),
            model: model.into(),
            provider_session_id: None,
            sample_rate_hz,
            channels: 1,
            sample_format,
            text: None,
            end_of_turn: false,
            duration_ms: None,
            metadata: serde_json::Value::Null,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentAudioDataRef {
    pub relative_path: String,
    pub byte_len: u64,
    pub blake3: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentAudioSegmentRecord {
    pub schema_version: u32,
    pub segment_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub recorded_at: time::OffsetDateTime,
    pub direction: AgentAudioDirection,
    pub sequence: u64,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    pub sample_rate_hz: u32,
    pub channels: u8,
    pub sample_format: RealtimeAudioSampleFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default)]
    pub end_of_turn: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    pub audio: AgentAudioDataRef,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub metadata: serde_json::Value,
}

impl AgentAudioSegmentRecord {
    fn new(
        segment_id: TaskId,
        session_id: SessionId,
        input: AgentAudioSegmentInput,
        audio: AgentAudioDataRef,
    ) -> Self {
        Self {
            schema_version: AGENT_SESSION_AUDIO_SEGMENT_SCHEMA_VERSION,
            segment_id,
            session_id,
            recorded_at: time::OffsetDateTime::now_utc(),
            direction: input.direction,
            sequence: input.sequence,
            provider: input.provider,
            model: input.model,
            provider_session_id: input.provider_session_id,
            sample_rate_hz: input.sample_rate_hz,
            channels: input.channels,
            sample_format: input.sample_format,
            text: input.text,
            end_of_turn: input.end_of_turn,
            duration_ms: input.duration_ms,
            audio,
            metadata: input.metadata,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentVideoDirection {
    Input,
    Output,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentVideoFrameInput {
    pub direction: AgentVideoDirection,
    pub sequence: u64,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    pub format: RealtimeVideoFrameFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub metadata: serde_json::Value,
}

impl AgentVideoFrameInput {
    pub fn new(
        direction: AgentVideoDirection,
        sequence: u64,
        provider: impl Into<String>,
        model: impl Into<String>,
        format: RealtimeVideoFrameFormat,
    ) -> Self {
        Self {
            direction,
            sequence,
            provider: provider.into(),
            model: model.into(),
            provider_session_id: None,
            format,
            width: None,
            height: None,
            timestamp_ms: None,
            metadata: serde_json::Value::Null,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentVideoDataRef {
    pub relative_path: String,
    pub byte_len: u64,
    pub blake3: String,
    pub mime_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentVideoFrameRecord {
    pub schema_version: u32,
    pub frame_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub recorded_at: time::OffsetDateTime,
    pub direction: AgentVideoDirection,
    pub sequence: u64,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    pub format: RealtimeVideoFrameFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    pub frame: AgentVideoDataRef,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub metadata: serde_json::Value,
}

impl AgentVideoFrameRecord {
    fn new(
        frame_id: TaskId,
        session_id: SessionId,
        input: AgentVideoFrameInput,
        frame: AgentVideoDataRef,
    ) -> Self {
        Self {
            schema_version: AGENT_SESSION_VIDEO_FRAME_SCHEMA_VERSION,
            frame_id,
            session_id,
            recorded_at: time::OffsetDateTime::now_utc(),
            direction: input.direction,
            sequence: input.sequence,
            provider: input.provider,
            model: input.model,
            provider_session_id: input.provider_session_id,
            format: input.format,
            width: input.width,
            height: input.height,
            timestamp_ms: input.timestamp_ms,
            frame,
            metadata: input.metadata,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentSessionStoreRecordKind {
    SnapshotSaved {
        item_count: usize,
        steering_count: usize,
        follow_up_count: usize,
        aborted: bool,
    },
    TurnStarted {
        provider: String,
        model: String,
        role: AgentRole,
        #[serde(alias = "policy_id")]
        surface_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_steps: Option<usize>,
        #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
        run_context: AgentRunContext,
    },
    TurnCompleted {
        stop_reason: AgentStopReason,
        item_count: usize,
        event_count: usize,
    },
    ItemsAppended {
        item_count: usize,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        item_ids: Vec<TaskId>,
    },
    ProjectionSaved {
        projection_id: TaskId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous_projection_id: Option<TaskId>,
        retained_item_count: usize,
    },
    ControlEventPublished {
        event: crate::agent::AgentEvent,
    },
    ControlSnapshotSaved {
        snapshot: AgentSessionControlSnapshot,
    },
    AudioSegmentAppended {
        segment_id: TaskId,
        direction: AgentAudioDirection,
        sequence: u64,
        provider: String,
        model: String,
        byte_len: u64,
        sample_rate_hz: u32,
        sample_format: RealtimeAudioSampleFormat,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<String>,
        end_of_turn: bool,
    },
    VideoFrameAppended {
        frame_id: TaskId,
        direction: AgentVideoDirection,
        sequence: u64,
        provider: String,
        model: String,
        byte_len: u64,
        format: RealtimeVideoFrameFormat,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        width: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        height: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timestamp_ms: Option<u64>,
    },
    TaskStatusChanged {
        snapshot: AgentTaskSnapshot,
    },
    SteeringQueued {
        steering: AgentSteeringMessage,
    },
    FollowUpQueued {
        follow_up: AgentFollowUpMessage,
    },
    SteeringRemoved {
        steering: AgentSteeringMessage,
    },
    FollowUpRemoved {
        follow_up: AgentFollowUpMessage,
    },
    Compacted {
        summary: AgentCompactionSummary,
        item_count: usize,
    },
    Aborted {
        abort: AgentAbortRecord,
    },
}

#[derive(Default)]
pub struct NoopAgentSessionStore;

impl AgentSessionStore for NoopAgentSessionStore {
    fn load_session(
        &self,
        _session_id: &SessionId,
    ) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        Ok(None)
    }

    fn save_session(&self, _session: &AgentSession) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }

    fn append_items(
        &self,
        _session_id: &SessionId,
        _items: &[AgentItem],
    ) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }

    fn append_projection(
        &self,
        _session_id: &SessionId,
        _projection: &AgentSessionProjection,
    ) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }

    fn append_record(
        &self,
        _record: &AgentSessionStoreRecord,
    ) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }
}

/// Stable roaming store for sanitized background-activity projections.
///
/// The VFS is injected so normal hosts can use distributed SIFS while tests and
/// embedded hosts can provide another implementation. The directory is bound
/// to a digest of the host-injected owner, and every body repeats that owner.
#[derive(Clone)]
pub struct AgentTaskActivityProjectionStore {
    vfs: VfsHandle,
    guarded_reads: bool,
}

impl AgentTaskActivityProjectionStore {
    pub fn from_vfs(vfs: VfsHandle) -> Self {
        Self {
            vfs,
            guarded_reads: true,
        }
    }

    #[cfg(test)]
    fn from_test_vfs_without_no_link_guards(vfs: VfsHandle) -> Self {
        Self {
            vfs,
            guarded_reads: false,
        }
    }

    pub fn save_activity(
        &self,
        activity: &AgentTaskActivityProjection,
    ) -> Result<(), AgentSessionStoreError> {
        let activity = activity.sanitized_for_roaming()?;
        let directory = task_activity_sifs_dir_for_agent(&activity.owner_agent_key);
        let path = format!("{directory}/{}.json", activity.activity_id.as_str());
        let bytes = serialize_task_activity_projection(&activity)?;
        self.vfs
            .create_dir_all(&directory)
            .map_err(|source| activity_projection_vfs_error(&directory, source))?;
        self.vfs
            .write_with_options(&path, bytes, VfsWriteOptions::service_artifact())
            .map_err(|source| activity_projection_vfs_error(&path, source))
    }

    pub fn load_page(
        &self,
        agent_definition_key: &str,
        after: Option<&VfsDirectoryCursor>,
        max_entries: usize,
        max_serialized_bytes: u64,
    ) -> Result<AgentTaskActivityProjectionPage, AgentSessionStoreError> {
        load_task_activity_projection_page_from_vfs(
            &self.vfs,
            &task_activity_sifs_dir_for_agent(agent_definition_key),
            agent_definition_key,
            after,
            max_entries,
            max_serialized_bytes,
            self.guarded_reads,
        )
    }
}

#[derive(Clone)]
pub struct FileAgentSessionStore {
    paths: AgentStoragePaths,
    vfs: VfsHandle,
    live_streams: Arc<Mutex<Vec<FileAgentSessionStoreLiveStream>>>,
    roaming_activity_data_dir: Option<PathBuf>,
    roaming_activity_store: Arc<Mutex<Option<AgentTaskActivityProjectionStore>>>,
}

#[derive(Clone)]
struct FileAgentSessionStoreLiveStream {
    session_id: SessionId,
    sender: mpsc::Sender<AgentSessionStoreRecord>,
}

impl FileAgentSessionStore {
    pub fn new(paths: AgentStoragePaths) -> Self {
        let vfs = VfsHandle::local(paths.root.clone()).expect("agent session VFS root");
        Self {
            paths,
            vfs,
            live_streams: Arc::new(Mutex::new(Vec::new())),
            roaming_activity_data_dir: None,
            roaming_activity_store: Arc::new(Mutex::new(None)),
        }
    }

    pub fn for_data_dir(data_dir: &SinomoDataDir) -> Self {
        let mut store = Self::new(AgentStoragePaths::from_data_dir(data_dir));
        store.roaming_activity_data_dir = Some(data_dir.root().to_path_buf());
        store
    }

    #[cfg(test)]
    fn with_roaming_activity_store(self, store: AgentTaskActivityProjectionStore) -> Self {
        *self
            .roaming_activity_store
            .lock()
            .expect("roaming activity store cache") = Some(store);
        self
    }

    pub fn paths(&self) -> &AgentStoragePaths {
        &self.paths
    }

    fn save_task_activity_projection(
        &self,
        snapshot: &AgentTaskSnapshot,
    ) -> Result<(), AgentSessionStoreError> {
        let Some(activity) = AgentTaskActivityProjection::from_snapshot(snapshot) else {
            // Legacy/unowned activities intentionally do not enter the new
            // searchable authority projection.
            return Ok(());
        };
        let bytes = serialize_task_activity_projection(&activity)?;
        let path = self
            .paths
            .task_activity_path_for_agent(&activity.owner_agent_key, &activity.activity_id);
        self.replace_file(&path, &bytes)?;

        // Record the roaming obligation durably before attempting SIFS. A
        // failed final-state mirror must remain observable on this node and a
        // later activity write gets a bounded opportunity to retry it.
        let pending_path =
            self.pending_activity_mirror_path(&activity.owner_agent_key, &activity.activity_id);
        self.replace_file(&pending_path, &bytes)?;

        // The durable local projection and pending marker must succeed. SIFS is
        // still best effort so a cluster/DFS outage cannot fail ordinary local
        // session persistence.
        if let Some(store) = self.roaming_activity_store() {
            self.retry_pending_activity_mirrors(
                &activity.owner_agent_key,
                &store,
                AGENT_TASK_ACTIVITY_PENDING_RETRY_LIMIT,
            );
        }
        Ok(())
    }

    fn pending_activity_mirror_dir(&self, agent_definition_key: &str) -> PathBuf {
        self.paths
            .task_activity_dir
            .join(AGENT_TASK_ACTIVITY_PENDING_MIRROR_DIR)
            .join(
                blake3::hash(agent_definition_key.as_bytes())
                    .to_hex()
                    .as_str(),
            )
    }

    fn pending_activity_mirror_path(
        &self,
        agent_definition_key: &str,
        activity_id: &TaskId,
    ) -> PathBuf {
        self.pending_activity_mirror_dir(agent_definition_key)
            .join(format!("{}.json", activity_id.as_str()))
    }

    fn retry_pending_activity_mirrors(
        &self,
        agent_definition_key: &str,
        roaming: &AgentTaskActivityProjectionStore,
        limit: usize,
    ) {
        let directory = self.pending_activity_mirror_dir(agent_definition_key);
        let Ok(directory) = self.vfs_path_for(&directory) else {
            return;
        };
        let Ok(page) = self.vfs.list_page_if_exists(&directory, None, limit) else {
            return;
        };
        for entry in page.entries {
            if entry.kind != VfsObjectKind::File || !entry.name.ends_with(".json") {
                continue;
            }
            let Some(size_bytes) = entry.size_bytes else {
                continue;
            };
            let Ok(read) = self.vfs.read_prefix(&entry.path, size_bytes) else {
                continue;
            };
            if read.truncated || read.bytes.len() as u64 != size_bytes {
                continue;
            }
            let Ok(record) =
                serde_json::from_slice::<AgentTaskActivityProjectionRecord>(&read.bytes)
            else {
                continue;
            };
            if record.schema_version != AGENT_TASK_ACTIVITY_PROJECTION_SCHEMA_VERSION
                || record.activity.owner_agent_key != agent_definition_key
                || entry.name.strip_suffix(".json") != Some(record.activity.activity_id.as_str())
            {
                continue;
            }
            if roaming.save_activity(&record.activity).is_ok() {
                let _ = self
                    .vfs
                    .remove_with_options(&entry.path, VfsRemoveOptions::file().missing_ok());
            }
        }
    }

    /// Whether this node knows that at least one sanitized activity projection
    /// has not yet reached its roaming SIFS read model.
    pub fn has_pending_task_activity_mirrors(
        &self,
        agent_definition_key: &str,
    ) -> Result<bool, AgentSessionStoreError> {
        let directory = self.pending_activity_mirror_dir(agent_definition_key);
        let directory = self.vfs_path_for(&directory)?;
        self.vfs
            .list_page_if_exists(&directory, None, 1)
            .map(|page| !page.entries.is_empty())
            .map_err(|source| session_vfs_error(Path::new(&directory), source))
    }

    fn roaming_activity_store(&self) -> Option<AgentTaskActivityProjectionStore> {
        let mut cached = self.roaming_activity_store.lock().ok()?;
        if let Some(store) = cached.as_ref() {
            return Some(store.clone());
        }
        let data_dir = self.roaming_activity_data_dir.clone()?;
        let vfs = mounted_sifs_local_reads(Some(data_dir)).ok()?;
        let store = AgentTaskActivityProjectionStore::from_vfs(vfs);
        *cached = Some(store.clone());
        Some(store)
    }

    fn save_task_activity_projections_for_record(
        &self,
        record: &AgentSessionStoreRecord,
    ) -> Result<(), AgentSessionStoreError> {
        match &record.record {
            AgentSessionStoreRecordKind::TaskStatusChanged { snapshot } => {
                self.save_task_activity_projection(snapshot)
            }
            AgentSessionStoreRecordKind::ControlSnapshotSaved { snapshot } => {
                for task in &snapshot.tasks {
                    self.save_task_activity_projection(task)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Load a bounded page of latest typed activities for exactly one agent.
    ///
    /// The directory name is a digest of the host-injected agent key, and each
    /// decoded body repeats that owner. Both must agree before an activity is
    /// returned. Bodies with missing/oversized metadata, invalid schemas, or a
    /// mismatched owner are advanced past and reported as omitted.
    pub fn load_task_activity_projection_page(
        &self,
        agent_definition_key: &str,
        after: Option<&VfsDirectoryCursor>,
        max_entries: usize,
        max_serialized_bytes: u64,
    ) -> Result<AgentTaskActivityProjectionPage, AgentSessionStoreError> {
        let directory = self.paths.task_activity_dir_for_agent(agent_definition_key);
        let directory_path = self.vfs_path_for(&directory)?;
        load_task_activity_projection_page_from_vfs(
            &self.vfs,
            &directory_path,
            agent_definition_key,
            after,
            max_entries,
            max_serialized_bytes,
            false,
        )
    }

    pub fn subscribe_records(
        &self,
        session_id: &SessionId,
    ) -> mpsc::Receiver<AgentSessionStoreRecord> {
        let (sender, receiver) = mpsc::channel();
        self.live_streams
            .lock()
            .expect("agent session store live stream lock poisoned")
            .push(FileAgentSessionStoreLiveStream {
                session_id: session_id.clone(),
                sender,
            });
        receiver
    }

    pub fn append_audio_segment(
        &self,
        session_id: &SessionId,
        input: AgentAudioSegmentInput,
        bytes: &[u8],
    ) -> Result<AgentAudioSegmentRecord, AgentSessionStoreError> {
        let segment_id = TaskId::generate();
        let file_name = format!(
            "{:08}-{}-{}.raw",
            input.sequence,
            match input.direction {
                AgentAudioDirection::Input => "input",
                AgentAudioDirection::Output => "output",
            },
            segment_id.as_str()
        );
        let relative_path = format!("audio/{file_name}");
        let path = self
            .paths
            .session_audio_dir_for_session(session_id)
            .join(&file_name);
        self.replace_file(&path, bytes)?;
        let audio = AgentAudioDataRef {
            relative_path,
            byte_len: bytes.len() as u64,
            blake3: blake3::hash(bytes).to_hex().to_string(),
        };
        let record =
            AgentAudioSegmentRecord::new(segment_id, session_id.clone(), input.clone(), audio);
        self.append_jsonl_record(
            &self
                .paths
                .session_audio_segments_path_for_session(session_id),
            &record,
        )?;
        self.append_record(&AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::AudioSegmentAppended {
                segment_id: record.segment_id.clone(),
                direction: record.direction,
                sequence: record.sequence,
                provider: record.provider.clone(),
                model: record.model.clone(),
                byte_len: record.audio.byte_len,
                sample_rate_hz: record.sample_rate_hz,
                sample_format: record.sample_format,
                text: record.text.clone(),
                end_of_turn: record.end_of_turn,
            },
        ))?;
        Ok(record)
    }

    pub fn append_video_frame(
        &self,
        session_id: &SessionId,
        input: AgentVideoFrameInput,
        bytes: &[u8],
    ) -> Result<AgentVideoFrameRecord, AgentSessionStoreError> {
        let frame_id = TaskId::generate();
        let file_name = format!(
            "{:08}-{}-{}.{}",
            input.sequence,
            match input.direction {
                AgentVideoDirection::Input => "input",
                AgentVideoDirection::Output => "output",
            },
            frame_id.as_str(),
            input.format.extension()
        );
        let relative_path = format!("video/{file_name}");
        let path = self
            .paths
            .session_video_dir_for_session(session_id)
            .join(&file_name);
        self.replace_file(&path, bytes)?;
        let frame = AgentVideoDataRef {
            relative_path,
            byte_len: bytes.len() as u64,
            blake3: blake3::hash(bytes).to_hex().to_string(),
            mime_type: input.format.mime_type().to_string(),
        };
        let record = AgentVideoFrameRecord::new(frame_id, session_id.clone(), input.clone(), frame);
        self.append_jsonl_record(
            &self.paths.session_video_frames_path_for_session(session_id),
            &record,
        )?;
        self.append_record(&AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::VideoFrameAppended {
                frame_id: record.frame_id.clone(),
                direction: record.direction,
                sequence: record.sequence,
                provider: record.provider.clone(),
                model: record.model.clone(),
                byte_len: record.frame.byte_len,
                format: record.format,
                width: record.width,
                height: record.height,
                timestamp_ms: record.timestamp_ms,
            },
        ))?;
        Ok(record)
    }

    pub fn load_audio_segments(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<AgentAudioSegmentRecord>, AgentSessionStoreError> {
        let path = self
            .paths
            .session_audio_segments_path_for_session(session_id);
        let mut segments = Vec::new();
        for record in self.read_jsonl_records::<AgentAudioSegmentRecord>(&path)? {
            if record.schema_version != AGENT_SESSION_AUDIO_SEGMENT_SCHEMA_VERSION {
                return Err(AgentSessionStoreError::UnsupportedSnapshotSchema {
                    path: path.display().to_string(),
                    schema_version: record.schema_version,
                });
            }
            segments.push(record);
        }
        Ok(segments)
    }

    pub fn load_video_frames(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<AgentVideoFrameRecord>, AgentSessionStoreError> {
        let path = self.paths.session_video_frames_path_for_session(session_id);
        let mut frames = Vec::new();
        for record in self.read_jsonl_records::<AgentVideoFrameRecord>(&path)? {
            if record.schema_version != AGENT_SESSION_VIDEO_FRAME_SCHEMA_VERSION {
                return Err(AgentSessionStoreError::UnsupportedSnapshotSchema {
                    path: path.display().to_string(),
                    schema_version: record.schema_version,
                });
            }
            frames.push(record);
        }
        Ok(frames)
    }

    fn publish_record(&self, record: &AgentSessionStoreRecord) {
        let mut streams = self
            .live_streams
            .lock()
            .expect("agent session store live stream lock poisoned");
        streams.retain(|stream| {
            if stream.session_id != record.session_id {
                return true;
            }
            stream.sender.send(record.clone()).is_ok()
        });
    }

    fn read_optional_bytes(&self, path: &Path) -> Result<Option<Vec<u8>>, AgentSessionStoreError> {
        let vfs_path = self.vfs_path_for(path)?;
        self.vfs
            .read_optional(&vfs_path)
            .map_err(|source| session_vfs_error(path, source))
    }

    fn read_optional_text(&self, path: &Path) -> Result<Option<String>, AgentSessionStoreError> {
        let Some(bytes) = self.read_optional_bytes(path)? else {
            return Ok(None);
        };
        String::from_utf8(bytes).map(Some).map_err(|source| {
            AgentSessionStoreError::Deserialization {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })
    }

    fn read_jsonl_records<T>(&self, path: &Path) -> Result<Vec<T>, AgentSessionStoreError>
    where
        T: DeserializeOwned,
    {
        let Some(contents) = self.read_optional_text(path)? else {
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        for line in contents.lines() {
            if line.trim().is_empty() {
                continue;
            }
            records.push(serde_json::from_str(line).map_err(|source| {
                AgentSessionStoreError::Deserialization {
                    path: path.display().to_string(),
                    detail: source.to_string(),
                }
            })?);
        }
        Ok(records)
    }

    fn append_jsonl_record<T: Serialize>(
        &self,
        path: &Path,
        record: &T,
    ) -> Result<(), AgentSessionStoreError> {
        let vfs_path = self.vfs_path_for(path)?;
        if let Some(parent) = vfs_parent_path(&vfs_path) {
            self.vfs
                .create_dir_all(parent)
                .map_err(|source| session_vfs_error(path, source))?;
        }
        let mut line =
            serde_json::to_vec(record).map_err(|source| AgentSessionStoreError::Serialization {
                detail: source.to_string(),
            })?;
        line.push(b'\n');
        self.vfs
            .append_with_options(&vfs_path, line, VfsAppendOptions::local())
            .map_err(|source| session_vfs_error(path, source))
    }

    fn replace_file(&self, path: &Path, bytes: &[u8]) -> Result<(), AgentSessionStoreError> {
        let vfs_path = self.vfs_path_for(path)?;
        if let Some(parent) = vfs_parent_path(&vfs_path) {
            self.vfs
                .create_dir_all(parent)
                .map_err(|source| session_vfs_error(path, source))?;
        }
        self.vfs
            .write_with_options(&vfs_path, bytes, VfsWriteOptions::local_replace())
            .map_err(|source| session_vfs_error(path, source))
    }

    fn vfs_path_for(&self, path: &Path) -> Result<String, AgentSessionStoreError> {
        relative_vfs_path(&self.paths.root, path)
    }
}

impl AgentSessionStore for FileAgentSessionStore {
    fn load_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        let path = self.paths.session_snapshot_path_for_session(session_id);
        let Some(contents) = self.read_optional_text(&path)? else {
            return Ok(None);
        };
        let snapshot: AgentSessionSnapshot = serde_json::from_str(&contents).map_err(|source| {
            AgentSessionStoreError::Deserialization {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
        if snapshot.schema_version != AGENT_SESSION_SNAPSHOT_SCHEMA_VERSION {
            return Err(AgentSessionStoreError::UnsupportedSnapshotSchema {
                path: path.display().to_string(),
                schema_version: snapshot.schema_version,
            });
        }
        if &snapshot.session.session_id != session_id {
            return Err(AgentSessionStoreError::Deserialization {
                path: path.display().to_string(),
                detail: format!(
                    "snapshot session id `{}` does not match requested session id `{session_id}`",
                    snapshot.session.session_id
                ),
            });
        }
        Ok(Some(snapshot.session))
    }

    fn save_session(&self, session: &AgentSession) -> Result<(), AgentSessionStoreError> {
        let snapshot = AgentSessionSnapshot::new(session.clone());
        let bytes = serde_json::to_vec_pretty(&snapshot).map_err(|source| {
            AgentSessionStoreError::Serialization {
                detail: source.to_string(),
            }
        })?;
        let path = self
            .paths
            .session_snapshot_path_for_session(&session.session_id);
        self.replace_file(&path, &bytes)?;
        self.append_record(&AgentSessionStoreRecord::new(
            session.session_id.clone(),
            AgentSessionStoreRecordKind::SnapshotSaved {
                item_count: session.transcript.items.len(),
                steering_count: session.steering_queue.len(),
                follow_up_count: session.follow_up_queue.len(),
                aborted: session.abort.is_some(),
            },
        ))
    }

    fn append_items(
        &self,
        session_id: &SessionId,
        items: &[AgentItem],
    ) -> Result<(), AgentSessionStoreError> {
        if items.is_empty() {
            return Ok(());
        }
        let path = self.paths.session_items_path_for_session(session_id);
        for item in items {
            self.append_jsonl_record(
                &path,
                &AgentSessionItemRecord::new(session_id.clone(), item.clone()),
            )?;
        }
        self.append_record(&AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::ItemsAppended {
                item_count: items.len(),
                item_ids: items.iter().map(agent_item_id).collect(),
            },
        ))
    }

    fn append_projection(
        &self,
        session_id: &SessionId,
        projection: &AgentSessionProjection,
    ) -> Result<(), AgentSessionStoreError> {
        let path = self.paths.session_projections_path_for_session(session_id);
        self.append_jsonl_record(
            &path,
            &AgentSessionProjectionRecord::new(session_id.clone(), projection.clone()),
        )?;
        self.append_record(&AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::ProjectionSaved {
                projection_id: projection.projection_id.clone(),
                previous_projection_id: projection.previous_projection_id.clone(),
                retained_item_count: projection.retained_item_ids.len(),
            },
        ))
    }

    fn append_record(
        &self,
        record: &AgentSessionStoreRecord,
    ) -> Result<(), AgentSessionStoreError> {
        let path = self
            .paths
            .session_events_path_for_session(&record.session_id);
        self.append_jsonl_record(&path, record)?;
        self.save_task_activity_projections_for_record(record)?;
        self.publish_record(record);
        Ok(())
    }

    fn load_control_snapshot(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSessionControlSnapshot>, AgentSessionStoreError> {
        let path = self.paths.session_events_path_for_session(session_id);
        let mut latest = None;
        for record in self.read_jsonl_records::<AgentSessionStoreRecord>(&path)? {
            if let AgentSessionStoreRecordKind::ControlSnapshotSaved { snapshot } = record.record {
                latest = Some(snapshot);
            }
        }
        Ok(latest)
    }

    fn load_control_events(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<AgentEvent>, AgentSessionStoreError> {
        let path = self.paths.session_events_path_for_session(session_id);
        let mut events = Vec::new();
        for record in self.read_jsonl_records::<AgentSessionStoreRecord>(&path)? {
            if let AgentSessionStoreRecordKind::ControlEventPublished { event } = record.record {
                events.push(event);
            }
        }
        Ok(events)
    }
}

fn serialize_task_activity_projection(
    activity: &AgentTaskActivityProjection,
) -> Result<Vec<u8>, AgentSessionStoreError> {
    serde_json::to_vec(&AgentTaskActivityProjectionRecord::new(activity.clone())).map_err(
        |source| AgentSessionStoreError::Serialization {
            detail: source.to_string(),
        },
    )
}

fn task_activity_sifs_dir_for_agent(agent_definition_key: &str) -> String {
    format!(
        "{AGENT_TASK_ACTIVITY_SIFS_ROOT}/{}",
        blake3::hash(agent_definition_key.as_bytes()).to_hex()
    )
}

fn load_task_activity_projection_page_from_vfs(
    vfs: &VfsHandle,
    directory_path: &str,
    agent_definition_key: &str,
    after: Option<&VfsDirectoryCursor>,
    max_entries: usize,
    max_serialized_bytes: u64,
    guarded: bool,
) -> Result<AgentTaskActivityProjectionPage, AgentSessionStoreError> {
    let directory_metadata = if guarded {
        vfs.stat_beneath_no_links(directory_path)
    } else {
        vfs.stat(directory_path)
    };
    match directory_metadata {
        Ok(metadata) if metadata.kind == VfsObjectKind::Directory => {}
        Ok(_) => {
            return Ok(AgentTaskActivityProjectionPage {
                activities: Vec::new(),
                next_cursor: None,
                deferred_cursor: None,
                consumed_entries: 0,
                omitted_entries: 1,
                read_bytes: 0,
                available: false,
                required_bytes: None,
                deferred_for_fresh_budget: false,
            });
        }
        Err(VfsError::NotFound { .. }) => {
            return Ok(AgentTaskActivityProjectionPage {
                activities: Vec::new(),
                next_cursor: None,
                deferred_cursor: None,
                consumed_entries: 0,
                omitted_entries: 0,
                read_bytes: 0,
                available: false,
                required_bytes: None,
                deferred_for_fresh_budget: false,
            });
        }
        Err(source) => return Err(activity_projection_vfs_error(directory_path, source)),
    }
    let mut cursor = after.cloned();
    let mut next_cursor = cursor.clone();
    let mut consumed_entries = 0usize;
    let mut remaining_bytes = max_serialized_bytes;
    let mut omitted_entries = 0usize;
    let mut read_bytes = 0u64;
    let mut activities = Vec::with_capacity(max_entries);
    while consumed_entries < max_entries {
        // One directory entry at a time preserves both continuations around a
        // deferred body: retry-before and explicit-skip-after. Guarded paging
        // also binds every continuation to the exact opened directory version.
        let page = if guarded {
            vfs.list_page_beneath_no_links(directory_path, cursor.as_ref(), 1)
        } else {
            vfs.list_page(directory_path, cursor.as_ref(), 1)
        }
        .map_err(|source| activity_projection_vfs_error(directory_path, source))?;
        let Some(entry) = page.entries.first() else {
            next_cursor = None;
            break;
        };
        let after_entry = page.next_cursor.clone();
        if !matches!(entry.kind, VfsObjectKind::File | VfsObjectKind::Artifact)
            || !entry.name.ends_with(".json")
        {
            consumed_entries = consumed_entries.saturating_add(1);
            omitted_entries = omitted_entries.saturating_add(1);
            cursor = after_entry;
            next_cursor = cursor.clone();
            if cursor.is_none() {
                break;
            }
            continue;
        }
        let Some(serialized_bytes) = entry.size_bytes else {
            consumed_entries = consumed_entries.saturating_add(1);
            omitted_entries = omitted_entries.saturating_add(1);
            cursor = after_entry;
            next_cursor = cursor.clone();
            if cursor.is_none() {
                break;
            }
            continue;
        };
        if serialized_bytes > remaining_bytes {
            return Ok(AgentTaskActivityProjectionPage {
                activities,
                next_cursor: after_entry,
                deferred_cursor: cursor,
                consumed_entries,
                omitted_entries,
                read_bytes,
                available: true,
                required_bytes: Some(serialized_bytes),
                deferred_for_fresh_budget: true,
            });
        }
        remaining_bytes = remaining_bytes.saturating_sub(serialized_bytes);
        let read = if guarded {
            vfs.read_range_beneath_no_links(&entry.path, 0, serialized_bytes)
                .map(|read| {
                    let exact_metadata = read.metadata.size_bytes == Some(serialized_bytes)
                        && matches!(
                            read.metadata.kind,
                            VfsObjectKind::File | VfsObjectKind::Artifact
                        );
                    (read.bytes, read.truncated, exact_metadata)
                })
        } else {
            vfs.read_prefix(&entry.path, serialized_bytes)
                .map(|read| (read.bytes, read.truncated, true))
        };
        let (bytes, truncated, exact_metadata) = match read {
            Ok(read) => read,
            Err(VfsError::NotFound { .. }) => {
                consumed_entries = consumed_entries.saturating_add(1);
                omitted_entries = omitted_entries.saturating_add(1);
                cursor = after_entry;
                next_cursor = cursor.clone();
                if cursor.is_none() {
                    break;
                }
                continue;
            }
            Err(source) => return Err(activity_projection_vfs_error(&entry.path, source)),
        };
        read_bytes = read_bytes.saturating_add(bytes.len() as u64);
        consumed_entries = consumed_entries.saturating_add(1);
        cursor = after_entry;
        next_cursor = cursor.clone();
        if truncated || bytes.len() as u64 != serialized_bytes || !exact_metadata {
            omitted_entries = omitted_entries.saturating_add(1);
            if cursor.is_none() {
                break;
            }
            continue;
        }
        let projection: AgentTaskActivityProjectionRecord = match serde_json::from_slice(&bytes) {
            Ok(projection) => projection,
            Err(_) => {
                omitted_entries = omitted_entries.saturating_add(1);
                if cursor.is_none() {
                    break;
                }
                continue;
            }
        };
        let activity = match projection.activity.sanitized_for_roaming() {
            Ok(activity) => activity,
            Err(_) => {
                omitted_entries = omitted_entries.saturating_add(1);
                if cursor.is_none() {
                    break;
                }
                continue;
            }
        };
        if projection.schema_version != AGENT_TASK_ACTIVITY_PROJECTION_SCHEMA_VERSION
            || activity.owner_agent_key != agent_definition_key
            || entry.name.strip_suffix(".json") != Some(activity.activity_id.as_str())
        {
            omitted_entries = omitted_entries.saturating_add(1);
            if cursor.is_none() {
                break;
            }
            continue;
        }
        activities.push(activity);
        if cursor.is_none() {
            break;
        }
    }
    Ok(AgentTaskActivityProjectionPage {
        activities,
        next_cursor,
        deferred_cursor: None,
        consumed_entries,
        omitted_entries,
        read_bytes,
        available: true,
        required_bytes: None,
        deferred_for_fresh_budget: false,
    })
}

fn activity_projection_vfs_error(path: &str, source: VfsError) -> AgentSessionStoreError {
    AgentSessionStoreError::Io {
        path: path.to_string(),
        detail: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;

    #[test]
    fn file_session_store_rejects_snapshot_under_the_wrong_session_path() {
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-session-id-mismatch");
        let data_dir = SinomoDataDir::new(&data_root);
        let store = FileAgentSessionStore::for_data_dir(&data_dir);
        let stored_id = SessionId::from_legacy_named("sess_stored_identity").expect("session");
        let requested_id =
            SessionId::from_legacy_named("sess_requested_identity").expect("session");
        store
            .save_session(&AgentSession::new(stored_id.clone(), AgentRole::Main))
            .expect("save source snapshot");
        let stored_path = store.paths.session_snapshot_path_for_session(&stored_id);
        let requested_path = store.paths.session_snapshot_path_for_session(&requested_id);
        fs::create_dir_all(
            requested_path
                .parent()
                .expect("requested session directory"),
        )
        .expect("create requested session directory");
        fs::copy(stored_path, requested_path).expect("place snapshot under wrong session path");

        let error = store
            .load_session(&requested_id)
            .expect_err("mismatched embedded session id must be rejected");
        assert!(
            error
                .to_string()
                .contains("does not match requested session id")
        );
    }

    #[test]
    fn file_session_store_live_streams_appended_records_for_matching_session() {
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-live-session-store");
        let data_dir = SinomoDataDir::new(&data_root);
        let store = FileAgentSessionStore::for_data_dir(&data_dir);
        let session_id = SessionId::from_legacy_named("sess_live_store").expect("session");
        let other_session_id =
            SessionId::from_legacy_named("sess_other_live_store").expect("session");
        let receiver = store.subscribe_records(&session_id);
        let other_receiver = store.subscribe_records(&other_session_id);
        let appender = store.clone();
        let record = AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::SnapshotSaved {
                item_count: 1,
                steering_count: 0,
                follow_up_count: 0,
                aborted: false,
            },
        );

        appender.append_record(&record).expect("append record");

        let received = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("live record");
        assert_eq!(received.record_id, record.record_id);
        assert_eq!(received.session_id, session_id);
        assert!(other_receiver.try_recv().is_err());
        let events = fs::read_to_string(
            AgentStoragePaths::from_data_dir(&data_dir)
                .session_events_path_for_session(&session_id),
        )
        .expect("events");
        assert!(events.contains("\"snapshot_saved\""));
    }

    #[test]
    fn typed_task_activity_projection_pages_without_raw_session_storage() {
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-task-activity-projection");
        let data_dir = SinomoDataDir::new(&data_root);
        let roaming_store = AgentTaskActivityProjectionStore::from_test_vfs_without_no_link_guards(
            VfsHandle::memory(),
        );
        let store = FileAgentSessionStore::new(AgentStoragePaths::from_data_dir(&data_dir))
            .with_roaming_activity_store(roaming_store.clone());
        let session_id = SessionId::from_legacy_named("task_activity_projection").expect("session");
        let mut first = AgentTaskSnapshot::new(
            crate::AgentTaskRef::new("maintenance", "first projected task"),
            crate::AgentTaskLifecycleStatus::Running,
        )
        .with_agent_definition_key("main-agent");
        first.active_op_id = Some(TaskId::generate());
        first.result_ref = Some("private://model-session-artifact".to_string());
        let second = AgentTaskSnapshot::new(
            crate::AgentTaskRef::new("maintenance", "second projected task"),
            crate::AgentTaskLifecycleStatus::Completed,
        )
        .with_agent_definition_key("main-agent");
        for snapshot in [&first, &second] {
            store
                .append_record(&AgentSessionStoreRecord::new(
                    session_id.clone(),
                    AgentSessionStoreRecordKind::TaskStatusChanged {
                        snapshot: snapshot.clone(),
                    },
                ))
                .expect("append typed task status");
        }
        let mut first_updated = first.clone();
        first_updated.status = crate::AgentTaskLifecycleStatus::Completed;
        first_updated.progress_summary = Some(
            "PROVIDER_ERROR_SENTINEL cancelled because /home/ix/private \
             PRIVATE_SESSION_SENTINEL"
                .to_string(),
        );
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::TaskStatusChanged {
                    snapshot: first_updated.clone(),
                },
            ))
            .expect("replace latest typed task status");

        let local_projection = fs::read_to_string(
            store
                .paths
                .task_activity_path_for_agent("main-agent", &first.task.task_id),
        )
        .expect("sanitized local activity projection");
        assert!(!local_projection.contains("private://model-session-artifact"));
        assert!(!local_projection.contains("result_ref"));
        assert!(!local_projection.contains("active_op_id"));
        assert!(!local_projection.contains("event_ids"));
        assert!(!local_projection.contains("PROVIDER_ERROR_SENTINEL"));
        assert!(!local_projection.contains("/home/ix/private"));
        assert!(!local_projection.contains("PRIVATE_SESSION_SENTINEL"));

        // The new search authority is independent of the raw session log.
        fs::remove_dir_all(&store.paths.sessions_dir).expect("remove raw sessions");

        let oversized = store
            .load_task_activity_projection_page("main-agent", None, 1, 1)
            .expect("bounded oversized page");
        assert!(oversized.available);
        assert!(oversized.activities.is_empty());
        assert_eq!(oversized.consumed_entries, 0);
        assert_eq!(oversized.omitted_entries, 0);
        assert_eq!(oversized.read_bytes, 0);
        assert!(oversized.required_bytes.is_some_and(|bytes| bytes > 1));
        assert!(oversized.deferred_for_fresh_budget);
        assert!(oversized.deferred_cursor.is_none());
        assert!(oversized.next_cursor.is_some());

        let first_page = store
            .load_task_activity_projection_page("main-agent", None, 1, u64::MAX)
            .expect("first activity page");
        assert_eq!(first_page.activities.len(), 1);
        assert_eq!(first_page.consumed_entries, 1);
        assert!(first_page.read_bytes > 0);
        let second_page = store
            .load_task_activity_projection_page(
                "main-agent",
                first_page.next_cursor.as_ref(),
                1,
                u64::MAX,
            )
            .expect("second activity page");
        assert_eq!(second_page.activities.len(), 1);
        assert!(second_page.next_cursor.is_none());
        let projected = first_page
            .activities
            .into_iter()
            .chain(second_page.activities)
            .collect::<Vec<_>>();
        let mut projected_ids = projected
            .iter()
            .map(|activity| activity.activity_id.clone())
            .collect::<Vec<_>>();
        projected_ids.sort();
        let mut expected_ids = vec![first.task.task_id, second.task.task_id];
        expected_ids.sort();
        assert_eq!(projected_ids, expected_ids);
        let projected_first = projected
            .iter()
            .find(|activity| activity.activity_id == first_updated.task.task_id)
            .expect("updated first projection");
        assert_eq!(
            projected_first.status,
            crate::AgentTaskLifecycleStatus::Completed
        );
        assert_eq!(
            projected_first.progress_summary.as_deref(),
            Some("completed")
        );

        let roaming = roaming_store
            .load_page("main-agent", None, 8, u64::MAX)
            .expect("roaming SIFS activity page");
        assert!(roaming.available);
        assert_eq!(roaming.activities.len(), 2);
        assert!(roaming.activities.iter().any(|activity| {
            activity.activity_id == first_updated.task.task_id
                && activity.status == crate::AgentTaskLifecycleStatus::Completed
        }));

        let unauthorized = store
            .load_task_activity_projection_page("other-agent", None, 1, u64::MAX)
            .expect("other agent projection");
        assert!(!unauthorized.available);
        assert!(unauthorized.activities.is_empty());
    }

    #[test]
    fn roaming_task_activity_projection_bounds_and_scrubs_private_runtime_text() {
        let roaming = AgentTaskActivityProjectionStore::from_test_vfs_without_no_link_guards(
            VfsHandle::memory(),
        );
        let snapshot = AgentTaskSnapshot::new(
            crate::AgentTaskRef::new(
                "MODEL_SESSION_ERROR_SENTINEL".repeat(8),
                format!(
                    "Review release notes\tthen\ncheck /home/ix/private \
                     C:\\Users\\private PRIVATE_SESSION_SENTINEL {}",
                    "x".repeat(MAX_AGENT_TASK_ACTIVITY_SUMMARY_BYTES * 4)
                ),
            ),
            crate::AgentTaskLifecycleStatus::Failed,
        )
        .with_agent_definition_key("main-agent");
        let mut projection =
            AgentTaskActivityProjection::from_snapshot(&snapshot).expect("owned projection");
        assert_eq!(projection.activity_kind, FALLBACK_AGENT_TASK_ACTIVITY_KIND);
        assert!(
            projection
                .summary
                .starts_with("Review release notes then check")
        );
        assert!(!projection.summary.contains("/home/ix/private"));
        // Defend the public roaming-store boundary too: callers cannot smuggle
        // a private runtime diagnostic through a manually constructed value.
        projection.activity_kind = "MODEL_SESSION_ERROR_SENTINEL".repeat(8);
        projection.summary = format!(
            "Review release notes\tthen\ncheck /home/ix/private \
             C:\\Users\\private PRIVATE_SESSION_SENTINEL {}",
            "x".repeat(MAX_AGENT_TASK_ACTIVITY_SUMMARY_BYTES * 4)
        );
        projection.progress_summary = Some(
            "PROVIDER_ERROR_SENTINEL: cancelled at /home/ix/private for \
             PRIVATE_SESSION_SENTINEL"
                .to_string(),
        );

        roaming.save_activity(&projection).expect("save projection");
        let page = roaming
            .load_page("main-agent", None, 1, u64::MAX)
            .expect("load projection");
        let activity = page.activities.first().expect("projected activity");
        let serialized = serde_json::to_string(activity).expect("serialize activity");

        assert_eq!(activity.activity_kind, FALLBACK_AGENT_TASK_ACTIVITY_KIND);
        assert_eq!(activity.progress_summary.as_deref(), Some("failed"));
        assert!(
            activity
                .summary
                .starts_with("Review release notes then check")
        );
        assert!(activity.summary.contains("[redacted]"));
        assert!(activity.summary.len() <= MAX_AGENT_TASK_ACTIVITY_SUMMARY_BYTES);
        assert!(!activity.summary.chars().any(char::is_control));
        for private in [
            "MODEL_SESSION_ERROR_SENTINEL",
            "PROVIDER_ERROR_SENTINEL",
            "/home/ix/private",
            "C:\\Users\\private",
            "PRIVATE_SESSION_SENTINEL",
        ] {
            assert!(!serialized.contains(private), "leaked {private}");
        }
    }

    #[test]
    fn unavailable_roaming_projection_does_not_fail_local_session_persistence() {
        let data_root = sinomo_testdir::TestDir::new("sinomo-agent-task-activity-local-projection");
        let data_dir = SinomoDataDir::new(&data_root);
        let unavailable_roaming = AgentTaskActivityProjectionStore::from_vfs(
            VfsHandle::memory()
                .scoped("/outside-activity-root")
                .expect("scoped memory VFS"),
        );
        let store = FileAgentSessionStore::new(AgentStoragePaths::from_data_dir(&data_dir))
            .with_roaming_activity_store(unavailable_roaming);
        let session_id =
            SessionId::from_legacy_named("task_activity_local_projection").expect("session");
        let snapshot = AgentTaskSnapshot::new(
            crate::AgentTaskRef::new("maintenance", "persist despite roaming outage"),
            crate::AgentTaskLifecycleStatus::Running,
        )
        .with_agent_definition_key("main-agent");

        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id,
                AgentSessionStoreRecordKind::TaskStatusChanged {
                    snapshot: snapshot.clone(),
                },
            ))
            .expect("local persistence must not depend on roaming SIFS");

        let local = store
            .load_task_activity_projection_page("main-agent", None, 1, u64::MAX)
            .expect("local activity projection page");
        assert!(local.available);
        assert_eq!(local.activities.len(), 1);
        assert_eq!(local.activities[0].activity_id, snapshot.task.task_id);
        assert!(
            store
                .has_pending_task_activity_mirrors("main-agent")
                .expect("pending mirror marker")
        );

        let recovered_roaming =
            AgentTaskActivityProjectionStore::from_test_vfs_without_no_link_guards(
                VfsHandle::memory(),
            );
        *store
            .roaming_activity_store
            .lock()
            .expect("roaming store cache") = Some(recovered_roaming.clone());
        let later = AgentTaskSnapshot::new(
            crate::AgentTaskRef::new("maintenance", "retry pending roaming projection"),
            crate::AgentTaskLifecycleStatus::Completed,
        )
        .with_agent_definition_key("main-agent");
        store
            .append_record(&AgentSessionStoreRecord::new(
                SessionId::generate(),
                AgentSessionStoreRecordKind::TaskStatusChanged { snapshot: later },
            ))
            .expect("later activity retries bounded pending mirrors");
        assert!(
            !store
                .has_pending_task_activity_mirrors("main-agent")
                .expect("cleared pending mirror marker")
        );
        let recovered = recovered_roaming
            .load_page("main-agent", None, 8, u64::MAX)
            .expect("recovered roaming activities");
        assert_eq!(recovered.activities.len(), 2);
    }

    #[test]
    fn file_session_store_persists_audio_sidecars_and_metadata() {
        let data_dir = SinomoDataDir::new(std::env::temp_dir().join(format!(
            "sinomo-agent-audio-session-store-{}",
            TaskId::generate()
        )));
        let store = FileAgentSessionStore::for_data_dir(&data_dir);
        let session_id = SessionId::from_legacy_named("sess_audio_store").expect("session");
        let input = AgentAudioSegmentInput::new(
            AgentAudioDirection::Input,
            7,
            "openai",
            "gpt-realtime-2",
            24_000,
            RealtimeAudioSampleFormat::Pcm16Le,
        );

        let record = store
            .append_audio_segment(&session_id, input, &[0, 1, 2, 3])
            .expect("append segment");

        assert_eq!(record.sequence, 7);
        assert_eq!(record.audio.byte_len, 4);
        let audio_path = AgentStoragePaths::from_data_dir(&data_dir)
            .session_dir_for_session(&session_id)
            .join(&record.audio.relative_path);
        assert_eq!(
            fs::read(audio_path).expect("audio sidecar"),
            vec![0, 1, 2, 3]
        );
        let loaded = store
            .load_audio_segments(&session_id)
            .expect("load segments");
        assert_eq!(loaded, vec![record]);
        let events = fs::read_to_string(
            AgentStoragePaths::from_data_dir(&data_dir)
                .session_events_path_for_session(&session_id),
        )
        .expect("events");
        assert!(events.contains("\"audio_segment_appended\""));
    }

    #[test]
    fn file_session_store_persists_video_sidecars_and_metadata() {
        let data_dir = SinomoDataDir::new(std::env::temp_dir().join(format!(
            "sinomo-agent-video-session-store-{}",
            TaskId::generate()
        )));
        let store = FileAgentSessionStore::for_data_dir(&data_dir);
        let session_id = SessionId::from_legacy_named("sess_video_store").expect("session");
        let mut input = AgentVideoFrameInput::new(
            AgentVideoDirection::Input,
            3,
            "openai",
            "gpt-realtime-2",
            RealtimeVideoFrameFormat::Jpeg,
        );
        input.width = Some(640);
        input.height = Some(480);
        input.timestamp_ms = Some(1200);

        let record = store
            .append_video_frame(&session_id, input, &[255, 216, 255, 217])
            .expect("append frame");

        assert_eq!(record.sequence, 3);
        assert_eq!(record.frame.byte_len, 4);
        assert_eq!(record.frame.mime_type, "image/jpeg");
        let frame_path = AgentStoragePaths::from_data_dir(&data_dir)
            .session_dir_for_session(&session_id)
            .join(&record.frame.relative_path);
        assert_eq!(
            fs::read(frame_path).expect("video sidecar"),
            vec![255, 216, 255, 217]
        );
        let loaded = store.load_video_frames(&session_id).expect("load frames");
        assert_eq!(loaded, vec![record]);
        let events = fs::read_to_string(
            AgentStoragePaths::from_data_dir(&data_dir)
                .session_events_path_for_session(&session_id),
        )
        .expect("events");
        assert!(events.contains("\"video_frame_appended\""));
    }
}

fn relative_vfs_path(root: &Path, path: &Path) -> Result<String, AgentSessionStoreError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|source| AgentSessionStoreError::Io {
            path: path.display().to_string(),
            detail: format!("path is outside agent storage root: {source}"),
        })?;
    let mut segments = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(segment) => {
                let Some(segment) = segment.to_str() else {
                    return Err(AgentSessionStoreError::Io {
                        path: path.display().to_string(),
                        detail: "path segment is not valid UTF-8".to_string(),
                    });
                };
                segments.push(segment.to_string());
            }
            Component::CurDir => {}
            _ => {
                return Err(AgentSessionStoreError::Io {
                    path: path.display().to_string(),
                    detail: "path contains unsupported component".to_string(),
                });
            }
        }
    }
    Ok(segments.join("/"))
}

fn vfs_parent_path(path: &str) -> Option<&str> {
    path.rsplit_once('/').map(|(parent, _)| parent)
}

fn session_vfs_error(path: &Path, source: VfsError) -> AgentSessionStoreError {
    AgentSessionStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AgentSessionStoreError {
    #[error("I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("failed to serialize session store record: {detail}")]
    Serialization { detail: String },
    #[error("failed to deserialize session snapshot at {path}: {detail}")]
    Deserialization { path: String, detail: String },
    #[error("unsupported session snapshot schema {schema_version} at {path}")]
    UnsupportedSnapshotSchema { path: String, schema_version: u32 },
}
