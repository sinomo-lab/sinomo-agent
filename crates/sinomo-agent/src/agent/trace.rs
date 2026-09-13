//! Trace records, storage paths, and append-only trace writers.

use super::{
    AgentCompactionSummary, AgentItem, AgentLoopDecision, AgentModelEvent, AgentRole,
    AgentRunContext, AgentStopReason, AgentToolOutcome, ContextAssemblyReport, ContextDelta,
    ContextFrame, Deserialize, EventEnvelope, Path, PathBuf, Serialize, SessionId, SinomoDataDir,
    TaskId, VfsAppendOptions, VfsHandle,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTraceRecord {
    pub record_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub recorded_at: time::OffsetDateTime,
    pub record: AgentTraceRecordKind,
}

impl AgentTraceRecord {
    pub fn new(session_id: SessionId, record: AgentTraceRecordKind) -> Self {
        Self {
            record_id: TaskId::generate(),
            session_id,
            recorded_at: time::OffsetDateTime::now_utc(),
            record,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentTraceRecordKind {
    RunStarted {
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
    ContextReport {
        report: ContextAssemblyReport,
    },
    ContextFrame {
        frame: ContextFrame,
        delta: ContextDelta,
    },
    ModelEvent {
        event: AgentModelEvent,
    },
    Item {
        item: AgentItem,
    },
    ToolOutcome {
        outcome: AgentToolOutcome,
    },
    Compaction {
        summary: AgentCompactionSummary,
    },
    Event {
        event: EventEnvelope,
    },
    RunCompleted {
        stop_reason: AgentStopReason,
        final_decision: AgentLoopDecision,
    },
}

pub trait AgentTraceStore: Send + Sync {
    fn append(&self, record: &AgentTraceRecord) -> Result<(), AgentTraceStoreError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentStoragePaths {
    pub root: PathBuf,
    pub sessions_dir: PathBuf,
    /// Latest typed background-activity projections, partitioned by a hash of
    /// the owning agent definition key. This is deliberately outside raw
    /// model-session directories so bounded history search never scans them.
    pub task_activity_dir: PathBuf,
    pub channels_dir: PathBuf,
    pub traces_dir: PathBuf,
    pub evidence_dir: PathBuf,
}

impl AgentStoragePaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            sessions_dir: root.join("sessions"),
            task_activity_dir: root.join("task-activity"),
            channels_dir: root.join("channels"),
            traces_dir: root.join("traces"),
            evidence_dir: root.join("evidence"),
            root,
        }
    }

    pub fn from_data_dir(data_dir: &SinomoDataDir) -> Self {
        Self::new(data_dir.root().join("events").join("agent"))
    }

    pub fn trace_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.traces_dir
            .join(format!("{}.jsonl", session_id.as_str()))
    }

    pub fn session_dir_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.sessions_dir.join(session_id.as_str())
    }

    pub fn session_snapshot_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id)
            .join("snapshot.json")
    }

    pub fn session_events_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id)
            .join("events.jsonl")
    }

    pub fn session_items_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id).join("items.jsonl")
    }

    pub fn session_projections_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id)
            .join("projections.jsonl")
    }

    pub fn task_activity_dir_for_agent(&self, agent_definition_key: &str) -> PathBuf {
        self.task_activity_dir.join(
            blake3::hash(agent_definition_key.as_bytes())
                .to_hex()
                .as_str(),
        )
    }

    pub fn task_activity_path_for_agent(
        &self,
        agent_definition_key: &str,
        task_id: &TaskId,
    ) -> PathBuf {
        self.task_activity_dir_for_agent(agent_definition_key)
            .join(format!("{}.json", task_id.as_str()))
    }

    pub fn session_audio_dir_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id).join("audio")
    }

    pub fn session_audio_segments_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_audio_dir_for_session(session_id)
            .join("segments.jsonl")
    }

    pub fn session_video_dir_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir_for_session(session_id).join("video")
    }

    pub fn session_video_frames_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_video_dir_for_session(session_id)
            .join("frames.jsonl")
    }

    pub fn channel_dir_for_channel(&self, channel_id: &SessionId) -> PathBuf {
        self.channels_dir.join(channel_id.as_str())
    }

    pub fn channel_snapshot_path_for_channel(&self, channel_id: &SessionId) -> PathBuf {
        self.channel_dir_for_channel(channel_id)
            .join("snapshot.json")
    }

    pub fn channel_messages_path_for_channel(&self, channel_id: &SessionId) -> PathBuf {
        self.channel_dir_for_channel(channel_id)
            .join("messages.jsonl")
    }

    pub fn channel_events_path_for_channel(&self, channel_id: &SessionId) -> PathBuf {
        self.channel_dir_for_channel(channel_id)
            .join("events.jsonl")
    }

    pub fn evidence_dir_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.evidence_dir.join(session_id.as_str())
    }

    pub fn sifs_evidence_path_for_session(
        &self,
        session_id: &SessionId,
        file_name: impl AsRef<str>,
    ) -> String {
        format!(
            "/events/agent/evidence/{}/{}",
            session_id.as_str(),
            file_name.as_ref().trim_start_matches('/')
        )
    }
}

#[derive(Default)]
pub struct NoopAgentTraceStore;

impl AgentTraceStore for NoopAgentTraceStore {
    fn append(&self, _record: &AgentTraceRecord) -> Result<(), AgentTraceStoreError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileAgentTraceStore {
    pub(super) path: PathBuf,
}

impl FileAgentTraceStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn for_session(paths: &AgentStoragePaths, session_id: &SessionId) -> Self {
        Self::new(paths.trace_path_for_session(session_id))
    }

    pub fn for_data_dir_session(data_dir: &SinomoDataDir, session_id: &SessionId) -> Self {
        Self::for_session(&AgentStoragePaths::from_data_dir(data_dir), session_id)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl AgentTraceStore for FileAgentTraceStore {
    fn append(&self, record: &AgentTraceRecord) -> Result<(), AgentTraceStoreError> {
        let parent = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let file_name = self
            .path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .ok_or_else(|| AgentTraceStoreError::Io {
                path: self.path.display().to_string(),
                detail: "trace path does not end with a UTF-8 file name".to_string(),
            })?;
        let vfs = VfsHandle::local(&parent).map_err(|source| AgentTraceStoreError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
        vfs.create_dir_all("")
            .map_err(|source| AgentTraceStoreError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        let mut line =
            serde_json::to_vec(record).map_err(|source| AgentTraceStoreError::Serialization {
                detail: source.to_string(),
            })?;
        line.push(b'\n');
        vfs.append_with_options(file_name, line, VfsAppendOptions::local())
            .map_err(|source| AgentTraceStoreError::Io {
                path: self.path.display().to_string(),
                detail: source.to_string(),
            })
    }
}

#[derive(Clone, Debug)]
pub(super) struct SessionRoutedFileAgentTraceStore {
    pub(super) paths: AgentStoragePaths,
}

impl SessionRoutedFileAgentTraceStore {
    pub(super) fn new(paths: AgentStoragePaths) -> Self {
        Self { paths }
    }
}

impl AgentTraceStore for SessionRoutedFileAgentTraceStore {
    fn append(&self, record: &AgentTraceRecord) -> Result<(), AgentTraceStoreError> {
        FileAgentTraceStore::for_session(&self.paths, &record.session_id).append(record)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AgentTraceStoreError {
    #[error("I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("failed to serialize trace record: {detail}")]
    Serialization { detail: String },
}
