use crate::TaskId;
use crate::agent::{
    AgentAbortRecord, AgentCompactionSummary, AgentFollowUpMessage, AgentItem, AgentRole,
    AgentRunContext, AgentSession, AgentSessionProjection, AgentSteeringMessage, AgentStopReason,
    AgentStoragePaths, agent_item_id,
};
use serde::{Deserialize, Serialize};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const AGENT_SESSION_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

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
        policy_id: String,
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
    SteeringQueued {
        steering: AgentSteeringMessage,
    },
    FollowUpQueued {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileAgentSessionStore {
    paths: AgentStoragePaths,
}

impl FileAgentSessionStore {
    pub fn new(paths: AgentStoragePaths) -> Self {
        Self { paths }
    }

    pub fn for_data_dir(data_dir: &SinomoDataDir) -> Self {
        Self::new(AgentStoragePaths::from_data_dir(data_dir))
    }

    pub fn paths(&self) -> &AgentStoragePaths {
        &self.paths
    }
}

impl AgentSessionStore for FileAgentSessionStore {
    fn load_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        let path = self.paths.session_snapshot_path_for_session(session_id);
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path).map_err(|source| AgentSessionStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
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
        replace_file(&path, &bytes)?;
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
            append_jsonl_record(
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
        append_jsonl_record(
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
        append_jsonl_record(&path, record)
    }
}

fn append_jsonl_record<T: Serialize>(
    path: &Path,
    record: &T,
) -> Result<(), AgentSessionStoreError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|source| AgentSessionStoreError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let line =
        serde_json::to_string(record).map_err(|source| AgentSessionStoreError::Serialization {
            detail: source.to_string(),
        })?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| AgentSessionStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    writeln!(file, "{line}").map_err(|source| AgentSessionStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn replace_file(path: &Path, bytes: &[u8]) -> Result<(), AgentSessionStoreError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|source| AgentSessionStoreError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let tmp_path = temporary_path_for(path);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|source| AgentSessionStoreError::Io {
                path: tmp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|source| AgentSessionStoreError::Io {
                path: tmp_path.display().to_string(),
                detail: source.to_string(),
            })?;
    }
    rename_replacing(&tmp_path, path)
}

fn temporary_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("snapshot");
    path.with_file_name(format!(".{file_name}.{}.tmp", TaskId::generate()))
}

#[cfg(not(windows))]
fn rename_replacing(source: &Path, target: &Path) -> Result<(), AgentSessionStoreError> {
    fs::rename(source, target).map_err(|error| AgentSessionStoreError::Io {
        path: target.display().to_string(),
        detail: error.to_string(),
    })
}

#[cfg(windows)]
fn rename_replacing(source: &Path, target: &Path) -> Result<(), AgentSessionStoreError> {
    match fs::rename(source, target) {
        Ok(()) => Ok(()),
        Err(error) if target.exists() => {
            fs::remove_file(target).map_err(|remove_error| AgentSessionStoreError::Io {
                path: target.display().to_string(),
                detail: remove_error.to_string(),
            })?;
            fs::rename(source, target).map_err(|rename_error| AgentSessionStoreError::Io {
                path: target.display().to_string(),
                detail: format!(
                    "{error}; retry after removing existing file failed: {rename_error}"
                ),
            })
        }
        Err(error) => Err(AgentSessionStoreError::Io {
            path: target.display().to_string(),
            detail: error.to_string(),
        }),
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
