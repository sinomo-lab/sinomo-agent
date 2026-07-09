use crate::TaskId;
use crate::agent::{
    AgentAbortRecord, AgentCompactionSummary, AgentEvent, AgentFollowUpMessage, AgentItem,
    AgentRole, AgentRunContext, AgentSession, AgentSessionControlSnapshot, AgentSessionProjection,
    AgentSteeringMessage, AgentStopReason, AgentStoragePaths, AgentTaskSnapshot, agent_item_id,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_lm::{RealtimeAudioSampleFormat, RealtimeVideoFrameFormat};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{VfsAppendOptions, VfsError, VfsHandle, VfsWriteOptions};
use std::path::{Component, Path};
use std::sync::{Arc, Mutex, mpsc};

pub const AGENT_SESSION_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const AGENT_SESSION_AUDIO_SEGMENT_SCHEMA_VERSION: u32 = 1;
pub const AGENT_SESSION_VIDEO_FRAME_SCHEMA_VERSION: u32 = 1;

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
    #[serde(with = "time::serde::rfc3339")]
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
    #[serde(with = "time::serde::rfc3339")]
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
    #[serde(with = "time::serde::rfc3339")]
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
    #[serde(with = "time::serde::rfc3339")]
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
    #[serde(with = "time::serde::rfc3339")]
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

#[derive(Clone)]
pub struct FileAgentSessionStore {
    paths: AgentStoragePaths,
    vfs: VfsHandle,
    live_streams: Arc<Mutex<Vec<FileAgentSessionStoreLiveStream>>>,
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
        }
    }

    pub fn for_data_dir(data_dir: &SinomoDataDir) -> Self {
        Self::new(AgentStoragePaths::from_data_dir(data_dir))
    }

    pub fn paths(&self) -> &AgentStoragePaths {
        &self.paths
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;

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
