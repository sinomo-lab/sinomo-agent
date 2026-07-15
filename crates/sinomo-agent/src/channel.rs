use crate::{
    AgentDefinition, AgentItem, AgentRole, AgentSession, AgentStoragePaths,
    AgentToolRuntimeDefinition, InteractionModel, TaskId, WorkspaceRef,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelError, LanguageModelMessage, LanguageModelRequest, LanguageModelResponseFormat,
    LanguageModelRole, LanguageModelToolChoice,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{VfsAppendOptions, VfsError, VfsHandle, VfsObjectKind, VfsWriteOptions};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Component, Path, PathBuf};

pub const AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const AGENT_CHANNEL_APP_USER_ID: &str = "user.app.local";
pub const AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT: usize = 400;
pub const AGENT_CHANNEL_MESSAGE_RECONCILIATION_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannel {
    pub channel_id: SessionId,
    pub title: String,
    #[serde(default)]
    pub channel_kind: AgentChannelKind,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
    #[serde(with = "crate::time_serde::rfc3339::option")]
    pub deleted_at: Option<time::OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<AgentChannelUser>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub participants: Vec<AgentChannelParticipant>,
    /// Ordered resources granted to every agent participating in this
    /// conversation. The first workspace is the default sandbox cwd.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workspaces: Vec<WorkspaceRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript: Vec<AgentChannelMessage>,
    #[serde(default = "default_next_channel_sequence")]
    pub next_sequence: u64,
}

impl AgentChannel {
    pub fn new(channel_id: SessionId, title: impl Into<String>) -> Self {
        let now = time::OffsetDateTime::now_utc();
        Self {
            channel_id,
            title: title.into(),
            channel_kind: AgentChannelKind::Chat,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            users: vec![AgentChannelUser::app_local()],
            participants: Vec::new(),
            workspaces: Vec::new(),
            transcript: Vec::new(),
            next_sequence: 1,
        }
    }

    pub fn with_default_title(channel_id: SessionId) -> Self {
        Self::new(channel_id, "New chat")
    }

    pub fn new_channel(channel_id: SessionId, title: impl Into<String>) -> Self {
        let mut channel = Self::new(channel_id, title);
        channel.channel_kind = AgentChannelKind::Channel;
        channel
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    pub fn rename(&mut self, title: impl Into<String>) {
        self.title = title.into();
        self.touch();
    }

    pub fn tombstone(&mut self) {
        self.deleted_at = Some(time::OffsetDateTime::now_utc());
        self.touch();
    }

    pub fn ensure_user(&mut self, user: AgentChannelUser) {
        if !self
            .users
            .iter()
            .any(|existing| existing.user_id == user.user_id)
        {
            self.users.push(user);
            self.touch();
        }
    }

    pub fn ensure_participant(
        &mut self,
        agent_key: impl Into<String>,
        agent_name: impl Into<String>,
    ) -> TaskId {
        let agent_key = agent_key.into();
        let agent_name = agent_name.into();
        if let Some(index) = self
            .participants
            .iter()
            .position(|participant| participant.agent_key == agent_key)
        {
            let (participant_id, changed) = {
                let participant = &mut self.participants[index];
                let changed = participant.agent_name != agent_name || !participant.enabled;
                if changed {
                    participant.agent_name = agent_name;
                    participant.enabled = true;
                }
                (participant.participant_id.clone(), changed)
            };
            if changed {
                self.touch();
            }
            return participant_id;
        }

        let order = self
            .participants
            .iter()
            .map(|participant| participant.order)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let participant = AgentChannelParticipant {
            participant_id: TaskId::generate(),
            agent_key,
            agent_name,
            agent_session_id: SessionId::generate(),
            enabled: true,
            order,
            tool_runtime: AgentToolRuntimeDefinition::default(),
            observed_through_sequence: 0,
            message_reconciliation_version: AGENT_CHANNEL_MESSAGE_RECONCILIATION_VERSION,
            observed_message_ids: Vec::new(),
        };
        let participant_id = participant.participant_id.clone();
        self.participants.push(participant);
        self.touch();
        participant_id
    }

    pub fn disable_participant(&mut self, participant_id: &TaskId) -> bool {
        if let Some(participant) = self
            .participants
            .iter_mut()
            .find(|participant| &participant.participant_id == participant_id)
        {
            participant.enabled = false;
            self.touch();
            return true;
        }
        false
    }

    pub fn enabled_participants_in_order(&self) -> Vec<AgentChannelParticipant> {
        let mut participants = self
            .participants
            .iter()
            .filter(|participant| participant.enabled)
            .cloned()
            .collect::<Vec<_>>();
        participants.sort_by_key(|participant| participant.order);
        participants
    }

    pub fn participant(&self, participant_id: &TaskId) -> Option<&AgentChannelParticipant> {
        self.participants
            .iter()
            .find(|participant| &participant.participant_id == participant_id)
    }

    pub fn participant_mut(
        &mut self,
        participant_id: &TaskId,
    ) -> Option<&mut AgentChannelParticipant> {
        self.participants
            .iter_mut()
            .find(|participant| &participant.participant_id == participant_id)
    }

    pub fn primary_workspace(&self) -> Option<&WorkspaceRef> {
        self.workspaces.first()
    }

    pub fn add_workspace(&mut self, workspace: WorkspaceRef) -> bool {
        if let Some(existing) = self
            .workspaces
            .iter_mut()
            .find(|existing| existing.id == workspace.id)
        {
            if existing == &workspace {
                return false;
            }
            *existing = workspace;
        } else {
            self.workspaces.push(workspace);
        }
        self.touch();
        true
    }

    pub fn remove_workspace(&mut self, workspace_id: &str) -> bool {
        let previous_len = self.workspaces.len();
        self.workspaces
            .retain(|workspace| workspace.id != workspace_id);
        if self.workspaces.len() == previous_len {
            return false;
        }
        self.touch();
        true
    }

    pub fn move_workspace(&mut self, workspace_id: &str, target_index: usize) -> bool {
        let Some(current_index) = self
            .workspaces
            .iter()
            .position(|workspace| workspace.id == workspace_id)
        else {
            return false;
        };
        let workspace = self.workspaces.remove(current_index);
        let target_index = target_index.min(self.workspaces.len());
        self.workspaces.insert(target_index, workspace);
        if current_index == target_index {
            return false;
        }
        self.touch();
        true
    }

    pub fn append_user_message(
        &mut self,
        user_id: impl Into<String>,
        display_name: impl Into<String>,
        content: impl Into<String>,
        source: AgentChannelSource,
    ) -> AgentChannelMessage {
        self.append_user_message_with_attachments(
            user_id,
            display_name,
            content,
            source,
            Vec::new(),
        )
    }

    pub fn append_user_message_with_attachments(
        &mut self,
        user_id: impl Into<String>,
        display_name: impl Into<String>,
        content: impl Into<String>,
        source: AgentChannelSource,
        attachments: Vec<AgentChannelAttachment>,
    ) -> AgentChannelMessage {
        let message = self.next_message(
            AgentChannelAuthor::User {
                user_id: user_id.into(),
                display_name: display_name.into(),
            },
            AgentChannelMessageKind::Normal,
            content,
            source,
            attachments,
        );
        self.transcript.push(message.clone());
        self.trim_transcript_to_recent(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT);
        self.touch();
        message
    }

    pub fn append_agent_message(
        &mut self,
        participant: &AgentChannelParticipant,
        content: impl Into<String>,
        source: AgentChannelSource,
    ) -> AgentChannelMessage {
        let message = self.next_message(
            AgentChannelAuthor::Agent {
                participant_id: participant.participant_id.clone(),
                agent_key: participant.agent_key.clone(),
                agent_name: participant.agent_name.clone(),
                agent_session_id: participant.agent_session_id.clone(),
            },
            AgentChannelMessageKind::Normal,
            content,
            source,
            Vec::new(),
        );
        self.transcript.push(message.clone());
        self.trim_transcript_to_recent(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT);
        self.touch();
        message
    }

    pub fn append_system_info(
        &mut self,
        content: impl Into<String>,
        source: AgentChannelSource,
    ) -> AgentChannelMessage {
        self.append_system_message(AgentChannelMessageKind::Info, content, source)
    }

    pub fn append_system_error(
        &mut self,
        content: impl Into<String>,
        source: AgentChannelSource,
    ) -> AgentChannelMessage {
        self.append_system_message(AgentChannelMessageKind::Error, content, source)
    }

    fn append_system_message(
        &mut self,
        message_kind: AgentChannelMessageKind,
        content: impl Into<String>,
        source: AgentChannelSource,
    ) -> AgentChannelMessage {
        let message = self.next_message(
            AgentChannelAuthor::System,
            message_kind,
            content,
            source,
            Vec::new(),
        );
        self.transcript.push(message.clone());
        self.trim_transcript_to_recent(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT);
        self.touch();
        message
    }

    pub fn trim_transcript_to_recent(&mut self, limit: usize) {
        if limit == 0 {
            self.transcript.clear();
            return;
        }
        let excess = self.transcript.len().saturating_sub(limit);
        if excess > 0 {
            self.transcript.drain(0..excess);
        }
    }

    pub fn recent_transcript_snapshot(&self, limit: usize) -> Self {
        let mut snapshot = self.clone();
        snapshot.trim_transcript_to_recent(limit);
        snapshot
    }

    fn next_message(
        &mut self,
        author: AgentChannelAuthor,
        message_kind: AgentChannelMessageKind,
        content: impl Into<String>,
        source: AgentChannelSource,
        attachments: Vec<AgentChannelAttachment>,
    ) -> AgentChannelMessage {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        AgentChannelMessage {
            message_id: TaskId::generate(),
            sequence,
            author,
            message_kind,
            content: content.into(),
            created_at: time::OffsetDateTime::now_utc(),
            source,
            attachments,
        }
    }

    fn touch(&mut self) {
        self.updated_at = time::OffsetDateTime::now_utc();
    }
}

fn default_next_channel_sequence() -> u64 {
    1
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentChannelKind {
    #[default]
    Chat,
    Channel,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentChannelUser {
    pub user_id: String,
    pub display_name: String,
    pub source: AgentChannelSource,
}

impl AgentChannelUser {
    pub fn app_local() -> Self {
        Self {
            user_id: AGENT_CHANNEL_APP_USER_ID.to_string(),
            display_name: "User".to_string(),
            source: AgentChannelSource::app(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentChannelParticipant {
    pub participant_id: TaskId,
    pub agent_key: String,
    pub agent_name: String,
    pub agent_session_id: SessionId,
    pub enabled: bool,
    pub order: u32,
    #[serde(
        default,
        skip_serializing_if = "AgentToolRuntimeDefinition::is_default"
    )]
    pub tool_runtime: AgentToolRuntimeDefinition,
    /// Legacy presentation-sequence watermark retained for snapshot compatibility.
    #[serde(default)]
    pub observed_through_sequence: u64,
    /// Version of stable message-ID reconciliation used by this participant.
    /// Zero denotes a legacy snapshot that only has the sequence watermark.
    #[serde(default)]
    pub message_reconciliation_version: u32,
    /// Stable message IDs already projected into this participant's session.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_message_ids: Vec<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentChannelMessage {
    pub message_id: TaskId,
    pub sequence: u64,
    pub author: AgentChannelAuthor,
    #[serde(default)]
    pub message_kind: AgentChannelMessageKind,
    pub content: String,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub source: AgentChannelSource,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AgentChannelAttachment>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentChannelAttachment {
    pub attachment_id: TaskId,
    pub file_name: String,
    pub original_path: String,
    pub stored_path: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub source: AgentChannelSource,
}

impl AgentChannelAttachment {
    pub fn new(
        file_name: impl Into<String>,
        original_path: impl Into<String>,
        stored_path: impl Into<String>,
        size_bytes: u64,
        source: AgentChannelSource,
    ) -> Self {
        Self {
            attachment_id: TaskId::generate(),
            file_name: file_name.into(),
            original_path: original_path.into(),
            stored_path: stored_path.into(),
            size_bytes,
            content_type: None,
            created_at: time::OffsetDateTime::now_utc(),
            source,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentChannelAuthor {
    User {
        user_id: String,
        display_name: String,
    },
    Agent {
        participant_id: TaskId,
        agent_key: String,
        agent_name: String,
        agent_session_id: SessionId,
    },
    System,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentChannelMessageKind {
    #[default]
    Normal,
    Info,
    Error,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentChannelSource {
    pub adapter: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_channel_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub metadata: serde_json::Value,
}

impl AgentChannelSource {
    pub fn app() -> Self {
        Self {
            adapter: "sinomo-app".to_string(),
            external_channel_id: None,
            external_message_id: None,
            external_user_id: None,
            metadata: serde_json::Value::Null,
        }
    }

    pub fn system(metadata: serde_json::Value) -> Self {
        Self {
            adapter: "sinomo-agent".to_string(),
            metadata,
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelTurn {
    pub turn_id: TaskId,
    pub channel_id: SessionId,
    pub trigger_message_id: TaskId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decisions: Vec<AgentChannelSpeakDecision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub response_message_ids: Vec<TaskId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub system_message_ids: Vec<TaskId>,
    /// Visible messages committed with the turn before their per-message
    /// materializations are written. Readers repair any missing immutable
    /// message records from this idempotent outbox after a process crash.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outbox_messages: Vec<AgentChannelMessage>,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelSpeakDecision {
    pub participant_id: TaskId,
    pub agent_key: String,
    pub agent_name: String,
    pub agent_session_id: SessionId,
    pub speak: bool,
    pub reason: String,
    pub confidence: f32,
}

impl AgentChannelSpeakDecision {
    pub fn silent(
        participant: &AgentChannelParticipant,
        reason: impl Into<String>,
        confidence: f32,
    ) -> Self {
        Self {
            participant_id: participant.participant_id.clone(),
            agent_key: participant.agent_key.clone(),
            agent_name: participant.agent_name.clone(),
            agent_session_id: participant.agent_session_id.clone(),
            speak: false,
            reason: reason.into(),
            confidence,
        }
    }

    pub fn speak(
        participant: &AgentChannelParticipant,
        reason: impl Into<String>,
        confidence: f32,
    ) -> Self {
        Self {
            participant_id: participant.participant_id.clone(),
            agent_key: participant.agent_key.clone(),
            agent_name: participant.agent_name.clone(),
            agent_session_id: participant.agent_session_id.clone(),
            speak: true,
            reason: reason.into(),
            confidence,
        }
    }
}

pub trait AgentChannelStore: Send + Sync {
    fn save_channel(&self, channel: &AgentChannel) -> Result<(), AgentChannelStoreError>;
    fn load_channel(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<AgentChannel>, AgentChannelStoreError>;
    fn list_channels(&self) -> Result<Vec<AgentChannel>, AgentChannelStoreError>;
    fn append_message(
        &self,
        channel_id: &SessionId,
        message: &AgentChannelMessage,
    ) -> Result<(), AgentChannelStoreError>;
    fn append_turn(
        &self,
        channel_id: &SessionId,
        turn: &AgentChannelTurn,
    ) -> Result<(), AgentChannelStoreError>;
    fn save_attachment(
        &self,
        channel_id: &SessionId,
        attachment_id: &TaskId,
        file_name: &str,
        bytes: &[u8],
    ) -> Result<String, AgentChannelStoreError>;
}

impl<T> AgentChannelStore for &T
where
    T: AgentChannelStore + ?Sized,
{
    fn save_channel(&self, channel: &AgentChannel) -> Result<(), AgentChannelStoreError> {
        (**self).save_channel(channel)
    }

    fn load_channel(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
        (**self).load_channel(channel_id)
    }

    fn list_channels(&self) -> Result<Vec<AgentChannel>, AgentChannelStoreError> {
        (**self).list_channels()
    }

    fn append_message(
        &self,
        channel_id: &SessionId,
        message: &AgentChannelMessage,
    ) -> Result<(), AgentChannelStoreError> {
        (**self).append_message(channel_id, message)
    }

    fn append_turn(
        &self,
        channel_id: &SessionId,
        turn: &AgentChannelTurn,
    ) -> Result<(), AgentChannelStoreError> {
        (**self).append_turn(channel_id, turn)
    }

    fn save_attachment(
        &self,
        channel_id: &SessionId,
        attachment_id: &TaskId,
        file_name: &str,
        bytes: &[u8],
    ) -> Result<String, AgentChannelStoreError> {
        (**self).save_attachment(channel_id, attachment_id, file_name, bytes)
    }
}

#[derive(Clone, Debug)]
pub struct FileAgentChannelStore {
    paths: AgentStoragePaths,
    vfs: VfsHandle,
}

impl FileAgentChannelStore {
    pub fn new(paths: AgentStoragePaths) -> Self {
        let vfs = VfsHandle::local(paths.root.clone()).expect("agent channel VFS root");
        Self { paths, vfs }
    }

    pub fn for_data_dir(data_dir: &SinomoDataDir) -> Self {
        Self::new(AgentStoragePaths::from_data_dir(data_dir))
    }

    pub fn paths(&self) -> &AgentStoragePaths {
        &self.paths
    }

    fn channel_dir(&self, channel_id: &SessionId) -> PathBuf {
        self.paths.channel_dir_for_channel(channel_id)
    }

    fn attachment_path(
        &self,
        channel_id: &SessionId,
        attachment_id: &TaskId,
        file_name: &str,
    ) -> PathBuf {
        self.channel_dir(channel_id)
            .join("attachments")
            .join(attachment_id.to_string())
            .join(safe_agent_channel_attachment_file_name(file_name))
    }

    fn read_optional_bytes(&self, path: &Path) -> Result<Option<Vec<u8>>, AgentChannelStoreError> {
        let vfs_path = self.vfs_path_for(path)?;
        self.vfs
            .read_optional(&vfs_path)
            .map_err(|source| channel_vfs_error(path, source))
    }

    fn write_bytes(&self, path: &Path, bytes: &[u8]) -> Result<(), AgentChannelStoreError> {
        let vfs_path = self.vfs_path_for(path)?;
        if let Some(parent) = vfs_parent_path(&vfs_path) {
            self.vfs
                .create_dir_all(parent)
                .map_err(|source| channel_vfs_error(path, source))?;
        }
        self.vfs
            .write_with_options(&vfs_path, bytes, VfsWriteOptions::local_replace())
            .map_err(|source| channel_vfs_error(path, source))
    }

    fn append_json_line<T: Serialize>(
        &self,
        path: &Path,
        value: &T,
    ) -> Result<(), AgentChannelStoreError> {
        let vfs_path = self.vfs_path_for(path)?;
        if let Some(parent) = vfs_parent_path(&vfs_path) {
            self.vfs
                .create_dir_all(parent)
                .map_err(|source| channel_vfs_error(path, source))?;
        }
        let mut line =
            serde_json::to_vec(value).map_err(|source| AgentChannelStoreError::Serialization {
                detail: source.to_string(),
            })?;
        line.push(b'\n');
        self.vfs
            .append_with_options(&vfs_path, line, VfsAppendOptions::local())
            .map_err(|source| channel_vfs_error(path, source))
    }

    fn load_recent_messages(
        &self,
        channel_id: &SessionId,
        limit: usize,
    ) -> Result<Vec<AgentChannelMessage>, AgentChannelStoreError> {
        let path = self.paths.channel_messages_path_for_channel(channel_id);
        let Some(bytes) = self.read_optional_bytes(&path)? else {
            return Ok(Vec::new());
        };
        let mut messages = parse_json_lines::<AgentChannelMessage>(&bytes)?;
        trim_vec_to_recent(&mut messages, limit);
        Ok(messages)
    }

    fn vfs_path_for(&self, path: &Path) -> Result<String, AgentChannelStoreError> {
        relative_vfs_path(&self.paths.root, path)
    }
}

impl AgentChannelStore for FileAgentChannelStore {
    fn save_channel(&self, channel: &AgentChannel) -> Result<(), AgentChannelStoreError> {
        let channel = channel.recent_transcript_snapshot(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT);
        let snapshot = AgentChannelSnapshot {
            schema_version: AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION,
            channel: &channel,
        };
        let bytes = serde_json::to_vec_pretty(&snapshot).map_err(|source| {
            AgentChannelStoreError::Serialization {
                detail: source.to_string(),
            }
        })?;
        let path = self
            .paths
            .channel_snapshot_path_for_channel(&channel.channel_id);
        self.write_bytes(&path, &bytes)
    }

    fn load_channel(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
        let path = self.paths.channel_snapshot_path_for_channel(channel_id);
        let Some(bytes) = self.read_optional_bytes(&path)? else {
            return Ok(None);
        };
        let snapshot =
            serde_json::from_slice::<OwnedAgentChannelSnapshot>(&bytes).map_err(|source| {
                AgentChannelStoreError::Serialization {
                    detail: source.to_string(),
                }
            })?;
        if snapshot.schema_version != AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION {
            return Err(AgentChannelStoreError::Serialization {
                detail: format!(
                    "unsupported agent channel snapshot schema version {}",
                    snapshot.schema_version
                ),
            });
        }
        let mut channel = snapshot.channel;
        let recent =
            self.load_recent_messages(channel_id, AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT)?;
        if !recent.is_empty() {
            channel.transcript = recent;
        }
        Ok(Some(channel))
    }

    fn list_channels(&self) -> Result<Vec<AgentChannel>, AgentChannelStoreError> {
        let channels_vfs_path = self.vfs_path_for(&self.paths.channels_dir)?;
        let mut channels = Vec::new();
        for entry in self
            .vfs
            .list_if_exists(&channels_vfs_path)
            .map_err(|source| channel_vfs_error(&self.paths.channels_dir, source))?
        {
            if entry.kind != VfsObjectKind::Directory {
                continue;
            }
            let Ok(channel_id) = entry.name.parse::<SessionId>() else {
                continue;
            };
            if let Some(channel) = self.load_channel(&channel_id)? {
                if !channel.is_deleted() {
                    channels.push(channel);
                }
            }
        }
        channels.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        Ok(channels)
    }

    fn append_message(
        &self,
        channel_id: &SessionId,
        message: &AgentChannelMessage,
    ) -> Result<(), AgentChannelStoreError> {
        self.append_json_line(
            &self.paths.channel_messages_path_for_channel(channel_id),
            message,
        )
    }

    fn append_turn(
        &self,
        channel_id: &SessionId,
        turn: &AgentChannelTurn,
    ) -> Result<(), AgentChannelStoreError> {
        self.append_json_line(
            &self.paths.channel_events_path_for_channel(channel_id),
            turn,
        )
    }

    fn save_attachment(
        &self,
        channel_id: &SessionId,
        attachment_id: &TaskId,
        file_name: &str,
        bytes: &[u8],
    ) -> Result<String, AgentChannelStoreError> {
        let path = self.attachment_path(channel_id, attachment_id, file_name);
        self.write_bytes(&path, bytes)?;
        Ok(path.display().to_string())
    }
}

pub fn safe_agent_channel_attachment_file_name(file_name: &str) -> String {
    let trimmed = file_name.trim();
    let mut safe = String::new();
    for ch in trimmed.chars() {
        let disallowed = matches!(
            ch,
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0'
        ) || ch.is_control();
        if disallowed {
            safe.push('_');
        } else {
            safe.push(ch);
        }
    }
    let safe = safe.trim_matches(['.', ' ']).to_string();
    if safe.is_empty() {
        "attachment".to_string()
    } else {
        safe
    }
}

#[derive(Serialize)]
struct AgentChannelSnapshot<'a> {
    schema_version: u32,
    channel: &'a AgentChannel,
}

#[derive(Deserialize)]
struct OwnedAgentChannelSnapshot {
    schema_version: u32,
    channel: AgentChannel,
}

fn relative_vfs_path(root: &Path, path: &Path) -> Result<String, AgentChannelStoreError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|source| AgentChannelStoreError::Io {
            path: path.display().to_string(),
            detail: format!("path is outside agent channel storage root: {source}"),
        })?;
    let mut segments = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(segment) => {
                let Some(segment) = segment.to_str() else {
                    return Err(AgentChannelStoreError::Io {
                        path: path.display().to_string(),
                        detail: "path segment is not valid UTF-8".to_string(),
                    });
                };
                segments.push(segment.to_string());
            }
            Component::CurDir => {}
            _ => {
                return Err(AgentChannelStoreError::Io {
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

fn parse_json_lines<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, AgentChannelStoreError> {
    let text =
        std::str::from_utf8(bytes).map_err(|source| AgentChannelStoreError::Serialization {
            detail: source.to_string(),
        })?;
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str::<T>(line).map_err(|source| {
                AgentChannelStoreError::Serialization {
                    detail: format!("line {}: {source}", index + 1),
                }
            })
        })
        .collect()
}

fn trim_vec_to_recent<T>(items: &mut Vec<T>, limit: usize) {
    if limit == 0 {
        items.clear();
        return;
    }
    let excess = items.len().saturating_sub(limit);
    if excess > 0 {
        items.drain(0..excess);
    }
}

fn channel_vfs_error(path: &Path, source: VfsError) -> AgentChannelStoreError {
    AgentChannelStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AgentChannelStoreError {
    #[error("I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("failed to serialize agent channel: {detail}")]
    Serialization { detail: String },
}

pub fn sync_participant_session(
    channel: &AgentChannel,
    participant: &mut AgentChannelParticipant,
    session: &mut AgentSession,
) {
    let stable_item_ids = session
        .transcript
        .items
        .iter()
        .chain(session.raw_transcript.items.iter())
        .map(crate::agent_item_id)
        .collect::<BTreeSet<_>>();
    let mut observed_message_ids = participant
        .observed_message_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for message in &channel.transcript {
        if stable_item_ids.contains(&channel_message_agent_item_id(&message.message_id)) {
            observed_message_ids.insert(message.message_id.clone());
        }
    }

    if participant.message_reconciliation_version < AGENT_CHANNEL_MESSAGE_RECONCILIATION_VERSION {
        // A legacy participant only recorded a presentation-sequence watermark.
        // Seed the stable IDs represented by that watermark without replaying
        // them into the model session, then use IDs exclusively from now on.
        observed_message_ids.extend(
            channel
                .transcript
                .iter()
                .filter(|message| message.sequence <= participant.observed_through_sequence)
                .map(|message| message.message_id.clone()),
        );
        participant.message_reconciliation_version = AGENT_CHANNEL_MESSAGE_RECONCILIATION_VERSION;
    }

    let mut next_step = session.transcript.items.len();
    let mut session_changed = false;
    for message in &channel.transcript {
        participant.observed_through_sequence =
            participant.observed_through_sequence.max(message.sequence);
        if observed_message_ids.contains(&message.message_id) {
            continue;
        }
        if message.content.trim().is_empty() && message.attachments.is_empty() {
            observed_message_ids.insert(message.message_id.clone());
            continue;
        }
        let item =
            channel_message_agent_item(message, participant, next_step, channel.channel_kind);
        session.transcript.push(item.clone());
        session.raw_transcript.push(item);
        session_changed = true;
        observed_message_ids.insert(message.message_id.clone());
        next_step = next_step.saturating_add(1);
    }

    // Channel snapshots retain only the recent transcript, so retaining IDs for
    // that same bounded window is sufficient and keeps participant metadata small.
    participant.observed_message_ids = channel
        .transcript
        .iter()
        .filter(|message| observed_message_ids.contains(&message.message_id))
        .map(|message| message.message_id.clone())
        .collect();
    if session_changed {
        session.advance_state_revision();
    }
}

/// Stable identity for the session item projected from one visible channel message.
///
/// Sequence numbers are presentation cursors and can change when an eventually
/// replicated message is inserted into the timeline. Message IDs do not, so the
/// derived item ID is the reconciliation key across retries and devices.
pub fn channel_message_agent_item_id(message_id: &TaskId) -> TaskId {
    TaskId::derived(
        "sinomo-agent.channel-message-item.v1",
        message_id.as_str().as_bytes(),
    )
}

fn channel_message_agent_item(
    message: &AgentChannelMessage,
    participant: &AgentChannelParticipant,
    step_index: usize,
    channel_kind: AgentChannelKind,
) -> AgentItem {
    let item_id = channel_message_agent_item_id(&message.message_id);
    let content = channel_message_content_for_agent(message);
    match &message.author {
        AgentChannelAuthor::User { display_name, .. } => AgentItem::UserMessage {
            item_id,
            step_index,
            content: if channel_kind == AgentChannelKind::Chat {
                content
            } else {
                format!("{display_name}: {content}")
            },
            event_id: None,
        },
        AgentChannelAuthor::Agent { participant_id, .. }
            if participant_id == &participant.participant_id =>
        {
            AgentItem::AssistantText {
                item_id,
                step_index,
                content,
            }
        }
        AgentChannelAuthor::Agent { agent_name, .. } => AgentItem::UserMessage {
            item_id,
            step_index,
            content: format!("{agent_name}: {content}"),
            event_id: None,
        },
        AgentChannelAuthor::System => AgentItem::UserMessage {
            item_id,
            step_index,
            content: format!("System: {content}"),
            event_id: None,
        },
    }
}

pub fn mark_participant_message_observed(
    participant: &mut AgentChannelParticipant,
    message_id: &TaskId,
    sequence: u64,
) {
    participant.message_reconciliation_version = AGENT_CHANNEL_MESSAGE_RECONCILIATION_VERSION;
    participant.observed_through_sequence = participant.observed_through_sequence.max(sequence);
    if !participant
        .observed_message_ids
        .iter()
        .any(|observed| observed == message_id)
    {
        participant.observed_message_ids.push(message_id.clone());
    }
    let excess = participant
        .observed_message_ids
        .len()
        .saturating_sub(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT);
    if excess > 0 {
        participant.observed_message_ids.drain(0..excess);
    }
}

#[derive(Clone, Debug)]
pub struct AgentChannelSpeakGate<'a, M> {
    model: &'a M,
    provider: String,
    model_id: String,
    max_recent_messages: usize,
}

impl<'a, M> AgentChannelSpeakGate<'a, M>
where
    M: InteractionModel,
{
    pub fn new(model: &'a M, provider: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            model,
            provider: provider.into(),
            model_id: model_id.into(),
            max_recent_messages: 12,
        }
    }

    pub fn with_max_recent_messages(mut self, max_recent_messages: usize) -> Self {
        self.max_recent_messages = max_recent_messages;
        self
    }

    pub fn evaluate(
        &self,
        channel: &AgentChannel,
        participant: &AgentChannelParticipant,
        latest_user_message: &AgentChannelMessage,
    ) -> Result<AgentChannelSpeakDecision, AgentChannelSpeakGateError> {
        let request = self.request(channel, participant, latest_user_message);
        let response = self.model.complete(&request).map_err(|source| {
            AgentChannelSpeakGateError::Provider {
                detail: source.to_string(),
            }
        })?;
        let contract =
            serde_json::from_str::<SpeakGateContract>(&response.content).map_err(|source| {
                AgentChannelSpeakGateError::Parse {
                    detail: source.to_string(),
                    raw: response.content.clone(),
                }
            })?;
        Ok(AgentChannelSpeakDecision {
            participant_id: participant.participant_id.clone(),
            agent_key: participant.agent_key.clone(),
            agent_name: participant.agent_name.clone(),
            agent_session_id: participant.agent_session_id.clone(),
            speak: contract.speak,
            reason: contract.reason,
            confidence: contract.confidence.clamp(0.0, 1.0),
        })
    }

    pub fn request(
        &self,
        channel: &AgentChannel,
        participant: &AgentChannelParticipant,
        latest_user_message: &AgentChannelMessage,
    ) -> LanguageModelRequest {
        let recent = channel
            .transcript
            .iter()
            .rev()
            .take(self.max_recent_messages)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|message| render_gate_message(&message))
            .collect::<Vec<_>>()
            .join("\n");
        let latest = render_gate_message(latest_user_message);
        LanguageModelRequest {
            provider: self.provider.clone(),
            model: self.model_id.clone(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: "You decide whether one named agent should speak in a shared channel. Return only JSON matching {\"speak\": boolean, \"reason\": string, \"confidence\": number}. Speak only when the agent can add useful, non-duplicative value.".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: format!(
                        "Channel title: {}\nParticipant: {} ({})\nLatest user message: {latest}\nRecent channel timeline:\n{recent}",
                        channel.title, participant.agent_name, participant.agent_key
                    ),
                },
            ],
            items: Vec::new(),
            tools: Vec::new(),
            tool_choice: Some(LanguageModelToolChoice::None),
            parallel_tool_calls: Some(false),
            temperature: Some(0.0),
            max_output_tokens: Some(160),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::json!({
                "channel_id": channel.channel_id,
                "participant_id": participant.participant_id,
                "agent_key": participant.agent_key,
                "agent_session_id": participant.agent_session_id,
                "purpose": "agent-channel-speak-gate"
            }),
        }
    }
}

#[derive(Deserialize)]
struct SpeakGateContract {
    speak: bool,
    reason: String,
    confidence: f32,
}

#[derive(Debug, thiserror::Error)]
pub enum AgentChannelSpeakGateError {
    #[error("speak gate provider failed: {detail}")]
    Provider { detail: String },
    #[error("speak gate returned invalid JSON: {detail}; raw={raw}")]
    Parse { detail: String, raw: String },
}

impl From<LanguageModelError> for AgentChannelSpeakGateError {
    fn from(source: LanguageModelError) -> Self {
        Self::Provider {
            detail: source.to_string(),
        }
    }
}

fn render_gate_message(message: &AgentChannelMessage) -> String {
    let author = match &message.author {
        AgentChannelAuthor::User { display_name, .. } => display_name.as_str(),
        AgentChannelAuthor::Agent { agent_name, .. } => agent_name.as_str(),
        AgentChannelAuthor::System => "System",
    };
    format!(
        "[{}] {author}: {}",
        message.sequence,
        channel_message_content_for_agent(message)
    )
}

fn channel_message_content_for_agent(message: &AgentChannelMessage) -> String {
    if message.attachments.is_empty() {
        return message.content.clone();
    }
    let mut content = message.content.clone();
    if !content.trim().is_empty() {
        content.push_str("\n\n");
    }
    content.push_str("Attachments:\n");
    for attachment in &message.attachments {
        content.push_str("- ");
        content.push_str(&attachment.file_name);
        content.push_str(" at ");
        content.push_str(&attachment.stored_path);
        content.push_str(" (");
        content.push_str(&attachment.size_bytes.to_string());
        content.push_str(" bytes)\n");
    }
    content.trim_end().to_string()
}

pub trait AgentChannelTurnExecutor {
    fn decide_speak(
        &mut self,
        channel: &AgentChannel,
        participant: &AgentChannelParticipant,
        latest_user_message: &AgentChannelMessage,
        session: &AgentSession,
    ) -> Result<AgentChannelSpeakDecision, AgentChannelExecutorError>;

    fn speak(
        &mut self,
        channel: &AgentChannel,
        participant: &AgentChannelParticipant,
        session: &mut AgentSession,
        on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
    ) -> Result<String, AgentChannelExecutorError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentChannelRuntimeEvent {
    Token {
        channel_id: SessionId,
        participant_id: TaskId,
        agent_key: String,
        agent_name: String,
        agent_session_id: SessionId,
        text: String,
    },
    System {
        channel_id: SessionId,
        message_id: TaskId,
        message_kind: AgentChannelMessageKind,
        text: String,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
pub struct AgentChannelExecutorError {
    pub detail: String,
}

impl AgentChannelExecutorError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AgentChannelRuntimeError {
    #[error("channel has been deleted")]
    Deleted,
    #[error("participant not found: {participant_id}")]
    ParticipantNotFound { participant_id: TaskId },
    #[error("agent channel store failed: {source}")]
    Store {
        #[from]
        source: AgentChannelStoreError,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelTurnReport {
    pub turn: AgentChannelTurn,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub turns: Vec<AgentChannelTurn>,
    pub latest_user_message: AgentChannelMessage,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelLoopConfig {
    pub max_agent_messages_per_ingress: usize,
}

impl Default for AgentChannelLoopConfig {
    fn default() -> Self {
        Self {
            max_agent_messages_per_ingress: 8,
        }
    }
}

pub struct AgentChannelRuntime<S, E>
where
    S: AgentChannelStore,
    E: AgentChannelTurnExecutor,
{
    channel: AgentChannel,
    store: S,
    executor: E,
    sessions: BTreeMap<TaskId, AgentSession>,
    loop_config: AgentChannelLoopConfig,
}

impl<S, E> AgentChannelRuntime<S, E>
where
    S: AgentChannelStore,
    E: AgentChannelTurnExecutor,
{
    pub fn new(channel: AgentChannel, store: S, executor: E) -> Self {
        let sessions = channel
            .participants
            .iter()
            .map(|participant| {
                (
                    participant.participant_id.clone(),
                    AgentSession::new(
                        participant.agent_session_id.clone(),
                        agent_role_for_key(&participant.agent_key),
                    ),
                )
            })
            .collect();
        Self {
            channel,
            store,
            executor,
            sessions,
            loop_config: AgentChannelLoopConfig::default(),
        }
    }

    pub fn with_loop_config(mut self, loop_config: AgentChannelLoopConfig) -> Self {
        self.loop_config = loop_config;
        self
    }

    pub fn channel(&self) -> &AgentChannel {
        &self.channel
    }

    pub fn channel_mut(&mut self) -> &mut AgentChannel {
        &mut self.channel
    }

    pub fn participant_session(&self, participant_id: &TaskId) -> Option<&AgentSession> {
        self.sessions.get(participant_id)
    }

    pub fn participant_session_mut(
        &mut self,
        participant_id: &TaskId,
    ) -> Option<&mut AgentSession> {
        self.sessions.get_mut(participant_id)
    }

    pub fn insert_participant_session(&mut self, participant_id: TaskId, session: AgentSession) {
        self.sessions.insert(participant_id, session);
    }

    pub fn add_participant(
        &mut self,
        agent_key: impl Into<String>,
        agent_name: impl Into<String>,
    ) -> Result<TaskId, AgentChannelRuntimeError> {
        let agent_key = agent_key.into();
        let participant_id = self
            .channel
            .ensure_participant(agent_key.clone(), agent_name);
        let participant = self
            .channel
            .participant(&participant_id)
            .expect("participant just created")
            .clone();
        self.sessions
            .entry(participant_id.clone())
            .or_insert_with(|| {
                AgentSession::new(
                    participant.agent_session_id.clone(),
                    agent_role_for_key(&agent_key),
                )
            });
        self.store.save_channel(&self.channel)?;
        Ok(participant_id)
    }

    pub fn remove_participant(
        &mut self,
        participant_id: &TaskId,
    ) -> Result<bool, AgentChannelRuntimeError> {
        let removed = self.channel.disable_participant(participant_id);
        self.store.save_channel(&self.channel)?;
        Ok(removed)
    }

    pub fn send_user_message(
        &mut self,
        content: impl Into<String>,
        on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
    ) -> Result<AgentChannelTurnReport, AgentChannelRuntimeError> {
        self.send_user_message_from(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            content,
            AgentChannelSource::app(),
            on_event,
        )
    }

    pub fn send_user_message_from(
        &mut self,
        user_id: impl Into<String>,
        display_name: impl Into<String>,
        content: impl Into<String>,
        source: AgentChannelSource,
        on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
    ) -> Result<AgentChannelTurnReport, AgentChannelRuntimeError> {
        self.send_user_message_from_with_attachments(
            user_id,
            display_name,
            content,
            source,
            Vec::new(),
            on_event,
        )
    }

    pub fn send_user_message_from_with_attachments(
        &mut self,
        user_id: impl Into<String>,
        display_name: impl Into<String>,
        content: impl Into<String>,
        source: AgentChannelSource,
        attachments: Vec<AgentChannelAttachment>,
        on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
    ) -> Result<AgentChannelTurnReport, AgentChannelRuntimeError> {
        if self.channel.is_deleted() {
            return Err(AgentChannelRuntimeError::Deleted);
        }
        let latest_user_message = self.channel.append_user_message_with_attachments(
            user_id,
            display_name,
            content,
            source,
            attachments,
        );
        self.store
            .append_message(&self.channel.channel_id, &latest_user_message)?;
        self.store.save_channel(&self.channel)?;

        let turns = self.process_pending_channel_messages(latest_user_message.clone(), on_event)?;
        let turn = turns.first().cloned().unwrap_or_else(|| AgentChannelTurn {
            turn_id: TaskId::generate(),
            channel_id: self.channel.channel_id.clone(),
            trigger_message_id: latest_user_message.message_id.clone(),
            decisions: Vec::new(),
            response_message_ids: Vec::new(),
            system_message_ids: Vec::new(),
            outbox_messages: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        });
        Ok(AgentChannelTurnReport {
            turn,
            turns,
            latest_user_message,
        })
    }

    fn process_pending_channel_messages(
        &mut self,
        initial_message: AgentChannelMessage,
        on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
    ) -> Result<Vec<AgentChannelTurn>, AgentChannelRuntimeError> {
        let mut queue = VecDeque::from([(initial_message, true)]);
        let mut turns = Vec::new();
        let mut emitted_agent_messages = 0usize;
        let mut limit_notice_emitted = false;

        while let Some((trigger_message, emit_no_response_if_silent)) = queue.pop_front() {
            let outcome = self.process_trigger_message(
                &trigger_message,
                emit_no_response_if_silent,
                &mut emitted_agent_messages,
                &mut limit_notice_emitted,
                on_event,
            )?;
            for message in outcome.response_messages {
                if emitted_agent_messages < self.loop_config.max_agent_messages_per_ingress {
                    queue.push_back((message, false));
                }
            }
            turns.push(outcome.turn);
            if emitted_agent_messages >= self.loop_config.max_agent_messages_per_ingress {
                queue.clear();
            }
        }

        self.store.save_channel(&self.channel)?;
        Ok(turns)
    }

    fn process_trigger_message(
        &mut self,
        trigger_message: &AgentChannelMessage,
        emit_no_response_if_silent: bool,
        emitted_agent_messages: &mut usize,
        limit_notice_emitted: &mut bool,
        on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
    ) -> Result<AgentChannelTriggerOutcome, AgentChannelRuntimeError> {
        let mut decisions = Vec::new();
        let mut response_message_ids = Vec::new();
        let mut system_message_ids = Vec::new();
        let mut response_messages = Vec::new();
        let mut outbox_messages = Vec::new();
        let participants = self.channel.enabled_participants_in_order();
        let trigger_author_participant_id = trigger_agent_participant_id(trigger_message);
        for participant in participants {
            let participant_id = participant.participant_id.clone();
            if trigger_author_participant_id.as_ref() == Some(&participant_id) {
                continue;
            }
            if *emitted_agent_messages >= self.loop_config.max_agent_messages_per_ingress {
                if !*limit_notice_emitted {
                    let system = self.channel.append_system_info(
                        format!(
                            "Agent channel paused after reaching the per-message response limit ({}).",
                            self.loop_config.max_agent_messages_per_ingress
                        ),
                        AgentChannelSource::system(serde_json::json!({
                            "channel_id": self.channel.channel_id,
                            "event_type": "agent_channel_response_limit",
                            "max_agent_messages_per_ingress": self.loop_config.max_agent_messages_per_ingress
                        })),
                    );
                    on_event(AgentChannelRuntimeEvent::System {
                        channel_id: self.channel.channel_id.clone(),
                        message_id: system.message_id.clone(),
                        message_kind: system.message_kind.clone(),
                        text: system.content.clone(),
                    });
                    system_message_ids.push(system.message_id.clone());
                    outbox_messages.push(system);
                    *limit_notice_emitted = true;
                }
                break;
            }
            self.ensure_session_for_participant(&participant);
            self.sync_participant(&participant_id)?;
            let current_participant = self
                .channel
                .participant(&participant_id)
                .cloned()
                .ok_or_else(|| AgentChannelRuntimeError::ParticipantNotFound {
                    participant_id: participant_id.clone(),
                })?;
            let session = self.sessions.get(&participant_id).ok_or_else(|| {
                AgentChannelRuntimeError::ParticipantNotFound {
                    participant_id: participant_id.clone(),
                }
            })?;
            let decision = match self.executor.decide_speak(
                &self.channel,
                &current_participant,
                trigger_message,
                session,
            ) {
                Ok(decision) => decision,
                Err(error) => {
                    let decision = AgentChannelSpeakDecision::silent(
                        &current_participant,
                        format!("speak gate failed: {}", error.detail),
                        0.0,
                    );
                    let system = self.channel.append_system_error(
                        format!(
                            "{} did not respond because its speak gate failed: {}",
                            current_participant.agent_name, error.detail
                        ),
                        AgentChannelSource::system(serde_json::json!({
                            "channel_id": self.channel.channel_id,
                            "participant_id": current_participant.participant_id,
                            "agent_key": current_participant.agent_key,
                            "agent_session_id": current_participant.agent_session_id,
                            "error_type": "speak_gate"
                        })),
                    );
                    on_event(AgentChannelRuntimeEvent::System {
                        channel_id: self.channel.channel_id.clone(),
                        message_id: system.message_id.clone(),
                        message_kind: system.message_kind.clone(),
                        text: system.content.clone(),
                    });
                    system_message_ids.push(system.message_id.clone());
                    outbox_messages.push(system);
                    decision
                }
            };
            decisions.push(decision.clone());
            if !decision.speak {
                continue;
            }

            let Some(session) = self.sessions.get_mut(&participant_id) else {
                return Err(AgentChannelRuntimeError::ParticipantNotFound { participant_id });
            };
            let response =
                match self
                    .executor
                    .speak(&self.channel, &current_participant, session, on_event)
                {
                    Ok(response) => response,
                    Err(error) => {
                        let system = self.channel.append_system_error(
                            format!(
                                "{} failed while responding: {}",
                                current_participant.agent_name, error.detail
                            ),
                            AgentChannelSource::system(serde_json::json!({
                                "channel_id": self.channel.channel_id,
                                "participant_id": current_participant.participant_id,
                                "agent_key": current_participant.agent_key,
                                "agent_session_id": current_participant.agent_session_id,
                                "error_type": "agent_response"
                            })),
                        );
                        on_event(AgentChannelRuntimeEvent::System {
                            channel_id: self.channel.channel_id.clone(),
                            message_id: system.message_id.clone(),
                            message_kind: system.message_kind.clone(),
                            text: system.content.clone(),
                        });
                        system_message_ids.push(system.message_id.clone());
                        outbox_messages.push(system);
                        continue;
                    }
                };
            if response.trim().is_empty() {
                continue;
            }
            let message = self.channel.append_agent_message(
                &current_participant,
                response,
                AgentChannelSource::app(),
            );
            response_message_ids.push(message.message_id.clone());
            response_messages.push(message.clone());
            outbox_messages.push(message.clone());
            *emitted_agent_messages = (*emitted_agent_messages).saturating_add(1);
            if let Some(participant) = self.channel.participant_mut(&participant_id) {
                mark_participant_message_observed(
                    participant,
                    &message.message_id,
                    message.sequence,
                );
            }
        }

        if emit_no_response_if_silent && response_message_ids.is_empty() {
            let system = self.channel.append_system_info(
                "No agent responded.",
                AgentChannelSource::system(serde_json::json!({
                    "channel_id": self.channel.channel_id,
                    "event_type": "no_agent_responded"
                })),
            );
            on_event(AgentChannelRuntimeEvent::System {
                channel_id: self.channel.channel_id.clone(),
                message_id: system.message_id.clone(),
                message_kind: system.message_kind.clone(),
                text: system.content.clone(),
            });
            system_message_ids.push(system.message_id.clone());
            outbox_messages.push(system);
        }

        let turn = AgentChannelTurn {
            turn_id: TaskId::generate(),
            channel_id: self.channel.channel_id.clone(),
            trigger_message_id: trigger_message.message_id.clone(),
            decisions,
            response_message_ids,
            system_message_ids,
            outbox_messages,
            created_at: time::OffsetDateTime::now_utc(),
        };
        // The turn is the durable commit point. Keep message creation and
        // runtime-event order above unchanged, but do not expose any immutable
        // channel message without the audit record that can repair it.
        self.store.append_turn(&self.channel.channel_id, &turn)?;
        for message in &turn.outbox_messages {
            self.store
                .append_message(&self.channel.channel_id, message)?;
        }
        Ok(AgentChannelTriggerOutcome {
            turn,
            response_messages,
        })
    }

    fn ensure_session_for_participant(&mut self, participant: &AgentChannelParticipant) {
        self.sessions
            .entry(participant.participant_id.clone())
            .or_insert_with(|| {
                AgentSession::new(
                    participant.agent_session_id.clone(),
                    agent_role_for_key(&participant.agent_key),
                )
            });
    }

    fn sync_participant(
        &mut self,
        participant_id: &TaskId,
    ) -> Result<(), AgentChannelRuntimeError> {
        let channel = self.channel.clone();
        let Some(participant) = self.channel.participant_mut(participant_id) else {
            return Err(AgentChannelRuntimeError::ParticipantNotFound {
                participant_id: participant_id.clone(),
            });
        };
        let Some(session) = self.sessions.get_mut(participant_id) else {
            return Err(AgentChannelRuntimeError::ParticipantNotFound {
                participant_id: participant_id.clone(),
            });
        };
        sync_participant_session(&channel, participant, session);
        Ok(())
    }
}

struct AgentChannelTriggerOutcome {
    turn: AgentChannelTurn,
    response_messages: Vec<AgentChannelMessage>,
}

fn trigger_agent_participant_id(message: &AgentChannelMessage) -> Option<TaskId> {
    match &message.author {
        AgentChannelAuthor::Agent { participant_id, .. } => Some(participant_id.clone()),
        AgentChannelAuthor::User { .. } | AgentChannelAuthor::System => None,
    }
}

fn agent_role_for_key(agent_key: &str) -> AgentRole {
    AgentDefinition::built_in(agent_key)
        .map(|definition| definition.role)
        .unwrap_or(AgentRole::Main)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_lm::{LanguageModelResponse, LanguageModelUsage};
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn channel_workspaces_are_ordered_and_first_is_default_cwd() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let first = WorkspaceRef::local_path("/work/first");
        let second = WorkspaceRef::sifs("shared", "/teams/shared");

        assert!(channel.add_workspace(first.clone()));
        assert!(channel.add_workspace(second.clone()));
        assert_eq!(channel.primary_workspace(), Some(&first));
        assert!(channel.move_workspace(&second.id, 0));
        assert_eq!(channel.primary_workspace(), Some(&second));
        assert!(channel.remove_workspace(&second.id));
        assert_eq!(channel.primary_workspace(), Some(&first));
    }

    #[derive(Clone, Default)]
    struct MemoryChannelStore {
        channels: Arc<Mutex<BTreeMap<SessionId, AgentChannel>>>,
        messages: Arc<Mutex<Vec<AgentChannelMessage>>>,
        turns: Arc<Mutex<Vec<AgentChannelTurn>>>,
    }

    impl AgentChannelStore for MemoryChannelStore {
        fn save_channel(&self, channel: &AgentChannel) -> Result<(), AgentChannelStoreError> {
            self.channels
                .lock()
                .expect("channels lock")
                .insert(channel.channel_id.clone(), channel.clone());
            Ok(())
        }

        fn load_channel(
            &self,
            channel_id: &SessionId,
        ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
            Ok(self
                .channels
                .lock()
                .expect("channels lock")
                .get(channel_id)
                .cloned())
        }

        fn list_channels(&self) -> Result<Vec<AgentChannel>, AgentChannelStoreError> {
            Ok(self
                .channels
                .lock()
                .expect("channels lock")
                .values()
                .filter(|channel| !channel.is_deleted())
                .cloned()
                .collect())
        }

        fn append_message(
            &self,
            _channel_id: &SessionId,
            message: &AgentChannelMessage,
        ) -> Result<(), AgentChannelStoreError> {
            self.messages
                .lock()
                .expect("messages lock")
                .push(message.clone());
            Ok(())
        }

        fn append_turn(
            &self,
            _channel_id: &SessionId,
            turn: &AgentChannelTurn,
        ) -> Result<(), AgentChannelStoreError> {
            self.turns.lock().expect("turns lock").push(turn.clone());
            Ok(())
        }

        fn save_attachment(
            &self,
            channel_id: &SessionId,
            attachment_id: &TaskId,
            file_name: &str,
            _bytes: &[u8],
        ) -> Result<String, AgentChannelStoreError> {
            Ok(format!(
                "memory://{}/attachments/{}/{}",
                channel_id,
                attachment_id,
                safe_agent_channel_attachment_file_name(file_name)
            ))
        }
    }

    struct ScriptExecutor {
        decisions: BTreeMap<String, Result<bool, String>>,
        responses: BTreeMap<String, Result<String, String>>,
        seen_by_speaker: Arc<Mutex<Vec<(String, Vec<String>)>>>,
    }

    impl ScriptExecutor {
        fn new() -> Self {
            Self {
                decisions: BTreeMap::new(),
                responses: BTreeMap::new(),
                seen_by_speaker: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_decision(mut self, agent_key: &str, speak: bool) -> Self {
            self.decisions.insert(agent_key.to_string(), Ok(speak));
            self
        }

        fn with_decision_error(mut self, agent_key: &str, detail: &str) -> Self {
            self.decisions
                .insert(agent_key.to_string(), Err(detail.to_string()));
            self
        }

        fn with_response(mut self, agent_key: &str, response: &str) -> Self {
            self.responses
                .insert(agent_key.to_string(), Ok(response.to_string()));
            self
        }

        fn seen_by_speaker(&self) -> Arc<Mutex<Vec<(String, Vec<String>)>>> {
            self.seen_by_speaker.clone()
        }
    }

    impl AgentChannelTurnExecutor for ScriptExecutor {
        fn decide_speak(
            &mut self,
            _channel: &AgentChannel,
            participant: &AgentChannelParticipant,
            _latest_user_message: &AgentChannelMessage,
            _session: &AgentSession,
        ) -> Result<AgentChannelSpeakDecision, AgentChannelExecutorError> {
            match self.decisions.get(&participant.agent_key) {
                Some(Ok(true)) => Ok(AgentChannelSpeakDecision::speak(
                    participant,
                    "scripted",
                    0.9,
                )),
                Some(Ok(false)) | None => Ok(AgentChannelSpeakDecision::silent(
                    participant,
                    "scripted silent",
                    0.9,
                )),
                Some(Err(detail)) => Err(AgentChannelExecutorError::new(detail.clone())),
            }
        }

        fn speak(
            &mut self,
            _channel: &AgentChannel,
            participant: &AgentChannelParticipant,
            session: &mut AgentSession,
            on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
        ) -> Result<String, AgentChannelExecutorError> {
            let seen = session
                .transcript
                .items
                .iter()
                .filter_map(|item| match item {
                    AgentItem::UserMessage { content, .. } => Some(content.clone()),
                    AgentItem::AssistantText { content, .. } => Some(content.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            self.seen_by_speaker
                .lock()
                .expect("seen lock")
                .push((participant.agent_key.clone(), seen));
            let response = self
                .responses
                .get(&participant.agent_key)
                .cloned()
                .unwrap_or_else(|| Ok(String::new()))
                .map_err(AgentChannelExecutorError::new)?;
            if !response.is_empty() {
                on_event(AgentChannelRuntimeEvent::Token {
                    channel_id: SessionId::generate(),
                    participant_id: participant.participant_id.clone(),
                    agent_key: participant.agent_key.clone(),
                    agent_name: participant.agent_name.clone(),
                    agent_session_id: participant.agent_session_id.clone(),
                    text: response.clone(),
                });
                let item = AgentItem::AssistantText {
                    item_id: TaskId::generate(),
                    step_index: session.transcript.items.len(),
                    content: response.clone(),
                };
                session.transcript.push(item.clone());
                session.raw_transcript.push(item);
            }
            Ok(response)
        }
    }

    #[test]
    fn channel_creation_stores_distinct_agent_sessions() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let first = channel.ensure_participant("main-agent", "Main");
        let second = channel.ensure_participant("coding-agent", "Coding");
        assert_ne!(first, second);
        let first_session = channel
            .participant(&first)
            .expect("first participant")
            .agent_session_id
            .clone();
        let second_session = channel
            .participant(&second)
            .expect("second participant")
            .agent_session_id
            .clone();
        assert_ne!(first_session, second_session);
    }

    #[test]
    fn ensure_participant_does_not_touch_an_unchanged_participant() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let participant_id = channel.ensure_participant("main-agent", "Main");
        channel.updated_at = time::OffsetDateTime::UNIX_EPOCH;

        let unchanged_id = channel.ensure_participant("main-agent", "Main");

        assert_eq!(unchanged_id, participant_id);
        assert_eq!(channel.updated_at, time::OffsetDateTime::UNIX_EPOCH);

        channel.ensure_participant("main-agent", "Main renamed");
        assert_ne!(channel.updated_at, time::OffsetDateTime::UNIX_EPOCH);
        assert_eq!(
            channel
                .participant(&participant_id)
                .expect("participant")
                .agent_name,
            "Main renamed"
        );
    }

    #[test]
    fn participant_reconciliation_fields_default_for_legacy_snapshots() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let participant_id = channel.ensure_participant("main-agent", "Main");
        channel
            .participant_mut(&participant_id)
            .expect("participant")
            .observed_through_sequence = 7;
        let mut value = serde_json::to_value(&channel).expect("serialize channel");
        let participant = value["participants"][0]
            .as_object_mut()
            .expect("participant object");
        participant.remove("message_reconciliation_version");
        participant.remove("observed_message_ids");

        let decoded: AgentChannel = serde_json::from_value(value).expect("legacy channel");
        let participant = decoded
            .participant(&participant_id)
            .expect("decoded participant");
        assert_eq!(participant.observed_through_sequence, 7);
        assert_eq!(participant.message_reconciliation_version, 0);
        assert!(participant.observed_message_ids.is_empty());
    }

    #[test]
    fn remove_and_readd_preserves_participant_session_state() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let participant_id = channel.ensure_participant("main-agent", "Main");
        let session_id = channel
            .participant(&participant_id)
            .expect("participant")
            .agent_session_id
            .clone();
        assert!(channel.disable_participant(&participant_id));
        assert!(
            !channel
                .participant(&participant_id)
                .expect("disabled participant")
                .enabled
        );
        let readded_id = channel.ensure_participant("main-agent", "Main renamed");
        let readded = channel
            .participant(&readded_id)
            .expect("readded participant");
        assert_eq!(readded_id, participant_id);
        assert_eq!(readded.agent_session_id, session_id);
        assert!(readded.enabled);
        assert_eq!(readded.agent_name, "Main renamed");
    }

    #[test]
    fn sequential_orchestration_lets_later_agents_see_earlier_replies() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        channel.ensure_participant("main-agent", "Main");
        channel.ensure_participant("coding-agent", "Coding");
        let executor = ScriptExecutor::new()
            .with_decision("main-agent", true)
            .with_response("main-agent", "First reply")
            .with_decision("coding-agent", true)
            .with_response("coding-agent", "Second reply");
        let seen = executor.seen_by_speaker();
        let mut runtime =
            AgentChannelRuntime::new(channel, MemoryChannelStore::default(), executor)
                .with_loop_config(AgentChannelLoopConfig {
                    max_agent_messages_per_ingress: 2,
                });
        let mut events = Vec::new();
        let report = runtime
            .send_user_message("What should we do?", &mut |event| events.push(event))
            .expect("send");
        assert_eq!(report.turn.response_message_ids.len(), 2);

        let seen = seen.lock().expect("seen lock").clone();
        let coding_seen = seen
            .iter()
            .find(|(agent_key, _)| agent_key == "coding-agent")
            .expect("coding speaker")
            .1
            .clone();
        assert!(
            coding_seen
                .iter()
                .any(|content| content.contains("Main: First reply")),
            "later participant transcript should include earlier channel reply: {coding_seen:?}"
        );
        assert_eq!(
            runtime
                .channel()
                .transcript
                .iter()
                .filter(|message| matches!(message.author, AgentChannelAuthor::Agent { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn authored_channel_reply_is_not_reinserted_into_its_session() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let participant_id = channel.ensure_participant("main-agent", "Main");
        let executor = ScriptExecutor::new()
            .with_decision("main-agent", true)
            .with_response("main-agent", "One authored reply.");
        let mut runtime =
            AgentChannelRuntime::new(channel, MemoryChannelStore::default(), executor)
                .with_loop_config(AgentChannelLoopConfig {
                    max_agent_messages_per_ingress: 1,
                });

        let report = runtime
            .send_user_message("Please answer once.", &mut |_| {})
            .expect("send");
        let response_id = report.turn.response_message_ids[0].clone();
        let before = runtime
            .participant_session(&participant_id)
            .expect("participant session")
            .transcript
            .items
            .len();
        assert!(
            runtime
                .channel()
                .participant(&participant_id)
                .expect("participant")
                .observed_message_ids
                .contains(&response_id)
        );

        runtime
            .sync_participant(&participant_id)
            .expect("repeat participant sync");

        assert_eq!(
            runtime
                .participant_session(&participant_id)
                .expect("participant session")
                .transcript
                .items
                .len(),
            before
        );
    }

    #[test]
    fn agent_replies_can_trigger_other_agents_without_echoing_to_author() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        channel.ensure_participant("main-agent", "Main");
        channel.ensure_participant("coding-agent", "Coding");
        let executor = ScriptExecutor::new()
            .with_decision("main-agent", true)
            .with_response("main-agent", "Main proposes a plan.")
            .with_decision("coding-agent", true)
            .with_response("coding-agent", "Coding adds implementation detail.");
        let seen = executor.seen_by_speaker();
        let mut runtime =
            AgentChannelRuntime::new(channel, MemoryChannelStore::default(), executor)
                .with_loop_config(AgentChannelLoopConfig {
                    max_agent_messages_per_ingress: 3,
                });

        let report = runtime
            .send_user_message("Discuss this.", &mut |_| {})
            .expect("send");

        assert_eq!(report.turns.len(), 2);
        assert_eq!(report.turns[0].response_message_ids.len(), 2);
        assert!(
            report.turns[1].response_message_ids.len() == 1,
            "the queued agent reply should trigger one other-agent response"
        );
        let seen = seen.lock().expect("seen lock").clone();
        assert!(
            seen.iter().any(|(agent_key, transcript)| {
                agent_key == "coding-agent"
                    && transcript
                        .iter()
                        .any(|content| content.contains("Main: Main proposes a plan."))
            }),
            "other agents should see the authoring agent's reply: {seen:?}"
        );
    }

    #[test]
    fn speak_gate_false_and_error_paths_emit_channel_events() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        channel.ensure_participant("main-agent", "Main");
        channel.ensure_participant("coding-agent", "Coding");
        let executor = ScriptExecutor::new()
            .with_decision("main-agent", false)
            .with_decision_error("coding-agent", "provider not configured");
        let mut runtime =
            AgentChannelRuntime::new(channel, MemoryChannelStore::default(), executor);
        let mut events = Vec::new();
        let report = runtime
            .send_user_message("Anyone?", &mut |event| events.push(event))
            .expect("send");
        assert!(report.turn.response_message_ids.is_empty());
        assert_eq!(
            report
                .turn
                .decisions
                .iter()
                .filter(|decision| decision.speak)
                .count(),
            0
        );
        assert_eq!(report.turn.system_message_ids.len(), 2);
        assert_eq!(report.turn.outbox_messages.len(), 2);
        assert_eq!(
            report
                .turn
                .outbox_messages
                .iter()
                .map(|message| message.message_id.clone())
                .collect::<Vec<_>>(),
            report.turn.system_message_ids,
            "every visible system notice must be recoverable from the turn outbox in sequence order"
        );
        assert!(
            runtime.channel().transcript.iter().any(|message| {
                message.message_kind == AgentChannelMessageKind::Error
                    && message.content.contains("speak gate failed")
            }),
            "gate errors should be stored as system error messages"
        );
        assert!(
            runtime.channel().transcript.iter().any(|message| {
                message.message_kind == AgentChannelMessageKind::Info
                    && message.content == "No agent responded."
            }),
            "no-speaker turns should store a system info message"
        );
        assert!(events.iter().any(|event| {
            matches!(
                event,
                AgentChannelRuntimeEvent::System {
                    message_kind: AgentChannelMessageKind::Error,
                    ..
                }
            )
        }));
    }

    #[test]
    fn participant_session_sync_maps_channel_messages() {
        let mut channel = AgentChannel::new_channel(SessionId::generate(), "Participant sync test");
        let main_id = channel.ensure_participant("main-agent", "Main");
        let coding_id = channel.ensure_participant("coding-agent", "Coding");
        let main = channel.participant(&main_id).expect("main").clone();
        let user = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "Please inspect this.",
            AgentChannelSource::app(),
        );
        let main_reply =
            channel.append_agent_message(&main, "I will look first.", AgentChannelSource::app());
        let system = channel.append_system_error(
            "Provider warning.",
            AgentChannelSource::system(serde_json::json!({"error_type": "provider"})),
        );
        let mut coding = channel.participant(&coding_id).expect("coding").clone();
        let mut session = AgentSession::new(coding.agent_session_id.clone(), AgentRole::Main);
        sync_participant_session(&channel, &mut coding, &mut session);
        let rendered = session
            .transcript
            .items
            .iter()
            .filter_map(|item| match item {
                AgentItem::UserMessage { content, .. } => Some(content.clone()),
                AgentItem::AssistantText { content, .. } => Some(content.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rendered,
            vec![
                "Alice: Please inspect this.".to_string(),
                "Main: I will look first.".to_string(),
                "System: Provider warning.".to_string(),
            ]
        );
        assert_eq!(
            session
                .transcript
                .items
                .iter()
                .map(crate::agent_item_id)
                .collect::<Vec<_>>(),
            [&user, &main_reply, &system]
                .into_iter()
                .map(|message| channel_message_agent_item_id(&message.message_id))
                .collect::<Vec<_>>()
        );
        assert_eq!(coding.observed_through_sequence, 3);
    }

    #[test]
    fn participant_session_sync_reconciles_retries_and_backdated_messages_by_id() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let coding_id = channel.ensure_participant("coding-agent", "Coding");
        let first = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "First message.",
            AgentChannelSource::app(),
        );
        let second = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "Second message.",
            AgentChannelSource::app(),
        );
        let mut coding = channel.participant(&coding_id).expect("coding").clone();
        let mut session = AgentSession::new(coding.agent_session_id.clone(), AgentRole::Main);

        sync_participant_session(&channel, &mut coding, &mut session);
        sync_participant_session(&channel, &mut coding, &mut session);
        assert_eq!(session.transcript.items.len(), 2, "retry must deduplicate");
        assert_eq!(session.raw_transcript.items.len(), 2);

        let mut backdated = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Remote",
            "Arrived late but belongs first.",
            AgentChannelSource::app(),
        );
        let mut renumbered_first = first.clone();
        let mut renumbered_second = second.clone();
        backdated.sequence = 1;
        renumbered_first.sequence = 2;
        renumbered_second.sequence = 3;
        channel.transcript = vec![backdated.clone(), renumbered_first, renumbered_second];
        channel.next_sequence = 4;

        sync_participant_session(&channel, &mut coding, &mut session);
        sync_participant_session(&channel, &mut coding, &mut session);

        assert_eq!(session.transcript.items.len(), 3);
        assert_eq!(session.raw_transcript.items.len(), 3);
        let item_ids = session
            .transcript
            .items
            .iter()
            .map(crate::agent_item_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            item_ids,
            [&first, &second, &backdated]
                .into_iter()
                .map(|message| channel_message_agent_item_id(&message.message_id))
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(coding.observed_through_sequence, 3);
    }

    #[test]
    fn participant_session_sync_seeds_legacy_watermark_without_rewriting_history() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let coding_id = channel.ensure_participant("coding-agent", "Coding");
        let first = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "First legacy message.",
            AgentChannelSource::app(),
        );
        let second = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "Second legacy message.",
            AgentChannelSource::app(),
        );
        let third = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "New stable message.",
            AgentChannelSource::app(),
        );
        let mut coding = channel.participant(&coding_id).expect("coding").clone();
        coding.observed_through_sequence = 2;
        coding.message_reconciliation_version = 0;
        coding.observed_message_ids.clear();
        let mut session = AgentSession::new(coding.agent_session_id.clone(), AgentRole::Main);
        let first_legacy_item_id = TaskId::generate();
        let second_legacy_item_id = TaskId::generate();
        session.transcript.items = vec![
            AgentItem::UserMessage {
                item_id: first_legacy_item_id.clone(),
                step_index: 0,
                content: "Alice: First legacy message.".to_string(),
                event_id: None,
            },
            AgentItem::UserMessage {
                item_id: second_legacy_item_id.clone(),
                step_index: 1,
                content: "Alice: Second legacy message.".to_string(),
                event_id: None,
            },
        ];
        session.raw_transcript = session.transcript.clone();
        session.projection = Some(crate::AgentSessionProjection {
            projection_id: TaskId::generate(),
            previous_projection_id: None,
            created_at: time::OffsetDateTime::now_utc(),
            reason: "legacy projection".to_string(),
            summary: "retain legacy channel items".to_string(),
            source_item_count: 2,
            source_item_ids: vec![first_legacy_item_id.clone(), second_legacy_item_id.clone()],
            retained_item_ids: vec![second_legacy_item_id.clone()],
            working_state: crate::AgentWorkingState::default(),
        });
        let projection_before = session.projection.clone();

        sync_participant_session(&channel, &mut coding, &mut session);
        sync_participant_session(&channel, &mut coding, &mut session);

        assert_eq!(
            session
                .transcript
                .items
                .iter()
                .map(crate::agent_item_id)
                .collect::<Vec<_>>(),
            vec![
                first_legacy_item_id,
                second_legacy_item_id,
                channel_message_agent_item_id(&third.message_id),
            ]
        );
        assert_eq!(session.raw_transcript, session.transcript);
        assert_eq!(session.projection, projection_before);
        assert_eq!(coding.observed_through_sequence, 3);
        assert_eq!(
            coding.message_reconciliation_version,
            AGENT_CHANNEL_MESSAGE_RECONCILIATION_VERSION
        );
        assert_eq!(
            coding.observed_message_ids,
            vec![first.message_id, second.message_id, third.message_id]
        );
    }

    #[test]
    fn participant_session_sync_honors_unrecoverable_legacy_watermark() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let coding_id = channel.ensure_participant("coding-agent", "Coding");
        channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "Already observed but absent from the legacy transcript.",
            AgentChannelSource::app(),
        );
        channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "Also already observed.",
            AgentChannelSource::app(),
        );
        let new_message = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "Alice",
            "New after restart.",
            AgentChannelSource::app(),
        );
        let mut coding = channel.participant(&coding_id).expect("coding").clone();
        coding.observed_through_sequence = 2;
        coding.message_reconciliation_version = 0;
        coding.observed_message_ids.clear();
        let mut session = AgentSession::new(coding.agent_session_id.clone(), AgentRole::Main);

        sync_participant_session(&channel, &mut coding, &mut session);
        sync_participant_session(&channel, &mut coding, &mut session);

        assert_eq!(session.transcript.items.len(), 1);
        assert_eq!(
            crate::agent_item_id(&session.transcript.items[0]),
            channel_message_agent_item_id(&new_message.message_id)
        );
        assert_eq!(coding.observed_message_ids.len(), 3);
    }

    #[derive(Clone)]
    struct GateModel {
        response: Result<String, String>,
        requests: Arc<Mutex<Vec<LanguageModelRequest>>>,
    }

    impl GateModel {
        fn success(content: &str) -> Self {
            Self {
                response: Ok(content.to_string()),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn failure(detail: &str) -> Self {
            Self {
                response: Err(detail.to_string()),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl InteractionModel for GateModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.requests
                .lock()
                .expect("requests lock")
                .push(request.clone());
            match &self.response {
                Ok(content) => Ok(LanguageModelResponse {
                    provider: request.provider.clone(),
                    model: request.model.clone(),
                    content: content.clone(),
                    items: Vec::new(),
                    tool_calls: Vec::new(),
                    usage: Some(LanguageModelUsage {
                        input_tokens: Some(1),
                        output_tokens: Some(1),
                    }),
                }),
                Err(detail) => Err(LanguageModelError::ProviderInvocationFailed {
                    provider: request.provider.clone(),
                    model: request.model.clone(),
                    detail: detail.clone(),
                }),
            }
        }
    }

    #[test]
    fn speak_gate_parses_json_contract_and_disables_tools() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let participant_id = channel.ensure_participant("main-agent", "Main");
        let latest = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "Should you answer?",
            AgentChannelSource::app(),
        );
        let participant = channel.participant(&participant_id).expect("participant");
        let model = GateModel::success(r#"{"speak":true,"reason":"helpful","confidence":0.75}"#);
        let gate = AgentChannelSpeakGate::new(&model, "fake", "fake-model");
        let decision = gate
            .evaluate(&channel, participant, &latest)
            .expect("gate decision");
        assert!(decision.speak);
        assert_eq!(decision.reason, "helpful");
        let requests = model.requests.lock().expect("requests lock");
        let request = requests.first().expect("request");
        assert!(request.tools.is_empty());
        assert_eq!(request.tool_choice, Some(LanguageModelToolChoice::None));
        assert_eq!(
            request.response_format,
            LanguageModelResponseFormat::JsonObject
        );
    }

    #[test]
    fn speak_gate_reports_parse_and_provider_errors() {
        let mut channel = AgentChannel::with_default_title(SessionId::generate());
        let participant_id = channel.ensure_participant("main-agent", "Main");
        let latest = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "Should you answer?",
            AgentChannelSource::app(),
        );
        let participant = channel.participant(&participant_id).expect("participant");
        let parse_model = GateModel::success("not json");
        let parse_error = AgentChannelSpeakGate::new(&parse_model, "fake", "fake")
            .evaluate(&channel, participant, &latest)
            .expect_err("parse error");
        assert!(matches!(
            parse_error,
            AgentChannelSpeakGateError::Parse { .. }
        ));

        let provider_model = GateModel::failure("not logged in");
        let provider_error = AgentChannelSpeakGate::new(&provider_model, "fake", "fake")
            .evaluate(&channel, participant, &latest)
            .expect_err("provider error");
        assert!(matches!(
            provider_error,
            AgentChannelSpeakGateError::Provider { .. }
        ));
    }

    #[test]
    fn file_channel_store_round_trips_and_lists_non_deleted_channels() {
        let data_dir = temp_data_dir("agent-channel-store");
        let store = FileAgentChannelStore::for_data_dir(&data_dir);
        let mut channel = AgentChannel::new(SessionId::generate(), "Round trip");
        let message =
            channel.append_system_info("stored", AgentChannelSource::system(serde_json::json!({})));
        store.save_channel(&channel).expect("save channel");
        store
            .append_message(&channel.channel_id, &message)
            .expect("append message");
        let loaded = store
            .load_channel(&channel.channel_id)
            .expect("load channel")
            .expect("channel exists");
        assert_eq!(loaded.title, "Round trip");
        assert_eq!(loaded.transcript.len(), 1);
        assert_eq!(store.list_channels().expect("list").len(), 1);

        let mut deleted = loaded.clone();
        deleted.tombstone();
        store.save_channel(&deleted).expect("save tombstone");
        assert!(store.list_channels().expect("list hidden").is_empty());
    }

    #[test]
    fn file_channel_store_loads_recent_messages_from_durable_log() {
        let data_dir = temp_data_dir("agent-channel-long-log");
        let store = FileAgentChannelStore::for_data_dir(&data_dir);
        let mut channel = AgentChannel::new(SessionId::generate(), "Long log");
        let total = AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT + 25;
        for index in 0..total {
            let message = channel.append_system_info(
                format!("message {index}"),
                AgentChannelSource::system(serde_json::json!({})),
            );
            store
                .append_message(&channel.channel_id, &message)
                .expect("append message");
        }
        store.save_channel(&channel).expect("save channel");

        let loaded = store
            .load_channel(&channel.channel_id)
            .expect("load channel")
            .expect("channel exists");
        assert_eq!(
            loaded.transcript.len(),
            AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT
        );
        assert_eq!(
            loaded
                .transcript
                .first()
                .map(|message| message.content.as_str()),
            Some("message 25")
        );
        assert_eq!(
            loaded
                .transcript
                .last()
                .map(|message| message.content.as_str()),
            Some("message 424")
        );
    }

    #[test]
    fn file_channel_store_saves_attachment_and_round_trips_metadata() {
        let data_dir = temp_data_dir("agent-channel-attachment");
        let store = FileAgentChannelStore::for_data_dir(&data_dir);
        let mut channel = AgentChannel::new(SessionId::generate(), "Attachments");
        let attachment_id = TaskId::generate();
        let stored_path = store
            .save_attachment(
                &channel.channel_id,
                &attachment_id,
                "..\\unsafe:name.txt",
                b"hello",
            )
            .expect("save attachment");
        assert_eq!(
            std::fs::read(&stored_path).expect("read attachment"),
            b"hello"
        );
        assert!(stored_path.contains("unsafe_name.txt"));

        let attachment = AgentChannelAttachment {
            attachment_id,
            file_name: "unsafe:name.txt".to_string(),
            original_path: "C:\\input\\unsafe:name.txt".to_string(),
            stored_path,
            size_bytes: 5,
            content_type: None,
            created_at: time::OffsetDateTime::now_utc(),
            source: AgentChannelSource::app(),
        };
        channel.append_user_message_with_attachments(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "see attached",
            AgentChannelSource::app(),
            vec![attachment],
        );
        store.save_channel(&channel).expect("save channel");

        let loaded = store
            .load_channel(&channel.channel_id)
            .expect("load channel")
            .expect("channel exists");
        let loaded_attachment = &loaded.transcript[0].attachments[0];
        assert_eq!(loaded_attachment.file_name, "unsafe:name.txt");
        assert_eq!(loaded_attachment.size_bytes, 5);
        assert_eq!(
            loaded_attachment.original_path,
            "C:\\input\\unsafe:name.txt"
        );
    }

    fn temp_data_dir(slug: &str) -> SinomoDataDir {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("sinomo-{slug}-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp data dir");
        SinomoDataDir::new(path)
    }
}
