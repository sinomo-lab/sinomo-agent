//! Stable, GUI-facing surface over the main-agent chat runtime.
//!
//! The graphical app (`sinomo-app`) drives the *same* interaction runtime the
//! terminal UI uses, but needs a small, owned, type-erased API rather than the
//! crate-internal handler types. Everything here returns plain data so the app
//! never depends on `tui` internals.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_runtime::cluster;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
use sinomo_agent::AgentChannelUser;
use sinomo_agent::{
    AGENT_CHANNEL_APP_USER_ID, AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT,
    AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION, AgentCatalog, AgentChannel, AgentChannelAttachment,
    AgentChannelAuthor, AgentChannelExecutorError, AgentChannelKind, AgentChannelLoopConfig,
    AgentChannelMessage, AgentChannelMessageKind, AgentChannelParticipant, AgentChannelRuntime,
    AgentChannelRuntimeEvent, AgentChannelSource, AgentChannelSpeakDecision, AgentChannelStore,
    AgentChannelStoreError, AgentChannelTurn, AgentChannelTurnExecutor, AgentContextAttachment,
    AgentDefinition, AgentItem, AgentModelEvent, AgentRuntimeControlHost,
    AgentRuntimeControlRegistry, AgentRuntimeEvent, AgentRuntimeEventPayload, AgentSession,
    AgentSessionControlResult, AgentSessionFollowUpRequest, AgentSessionInterruptRequest,
    AgentSessionReattachState, AgentSessionResumeRequest, AgentSessionResumeResponse,
    AgentSessionSteerRequest, AgentTaskLifecycleStatus, AgentTaskSnapshot, AgentToolProgressEvent,
    ContextAssemblyReport, InteractionOutput, InteractionSession, InteractionSurface,
    InteractionSurfaceKind, SkillActivationScope, SkillCatalogEntry, SkillCatalogSifsStore,
    SkillCatalogSource, SkillCatalogStore, SkillCatalogWrite, TaskId, render_common_skill_markdown,
    safe_agent_channel_attachment_file_name,
};
use sinomo_lm::{
    AnthropicConfig, CLAUDE_DEFAULT_MODEL, ClaudeCodeConfig, CodexCliConfig, GeminiConfig,
    GitHubCopilotConfig, OpenAiConfig, PROVIDER_DEFAULT_MODEL, ProviderReasoningEffort,
};
use sinomo_runtime::cluster::ClusterHealthRow;
use sinomo_runtime::events::{InMemoryEventStore, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_runtime::node::NodeId;
use std::collections::BTreeSet;
use time::format_description::well_known::Rfc3339;

use crate::commands::{
    ClusterInitOptions, ClusterInviteOptions, ClusterJoinOptions, cluster_invite_with_paths,
    cluster_join_with_paths, cluster_leave_with_paths, initialize_single_node_cluster_with_paths,
    read_cluster_state as read_cli_cluster_state,
};
use crate::error::CliError;
use crate::tui::{
    self, DefaultMainAgentChatModel, InteractionRuntime, RuntimeMainAgentInteractionHandler,
};

type Handler = RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>;
const APP_CHAT_RUNTIME_DRAIN_INTERVAL: Duration = Duration::from_millis(25);
const APP_CHAT_RUNTIME_START_TIMEOUT: Duration = Duration::from_secs(5);
const APP_CHAT_RUNTIME_SILENCE_TIMEOUT: Duration = Duration::from_secs(45);
const APP_CHAT_ATTACHMENT_TEXT_EXCERPT_BYTES: usize = 24 * 1024;
pub const SIFS_AGENT_CHANNELS_ROOT: &str = "/services/sinomo-agent/channels";
const SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE: u64 = 128;
const SIFS_AGENT_CHANNEL_LOG_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SifsChunkedLogIndex {
    schema_version: u32,
    entry_count: u64,
    updated_unix: i64,
}

impl Default for SifsChunkedLogIndex {
    fn default() -> Self {
        Self {
            schema_version: SIFS_AGENT_CHANNEL_LOG_SCHEMA_VERSION,
            entry_count: 0,
            updated_unix: 0,
        }
    }
}

/// Lightweight channel metadata for list views.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelSummary {
    pub channel_id: SessionId,
    pub title: String,
    #[serde(default)]
    pub channel_kind: AgentChannelKind,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub deleted_at: Option<time::OffsetDateTime>,
    #[serde(default)]
    pub enabled_participant_count: usize,
    #[serde(default)]
    pub participant_names: Vec<String>,
}

impl AgentChannelSummary {
    fn from_channel(channel: &AgentChannel) -> Self {
        Self {
            channel_id: channel.channel_id.clone(),
            title: channel.title.clone(),
            channel_kind: channel.channel_kind,
            updated_at: channel.updated_at,
            deleted_at: channel.deleted_at,
            enabled_participant_count: channel
                .participants
                .iter()
                .filter(|participant| participant.enabled)
                .count(),
            participant_names: channel
                .enabled_participants_in_order()
                .into_iter()
                .map(|participant| {
                    if participant.agent_name.trim().is_empty() {
                        participant.agent_key
                    } else {
                        participant.agent_name
                    }
                })
                .collect(),
        }
    }

    fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

/// The active agent / model selection, as plain strings.
#[derive(Clone, Debug, Default)]
pub struct ChatRuntimeInfo {
    pub agent: String,
    pub provider: String,
    pub model: String,
    pub account: Option<String>,
    pub reasoning_effort: Option<String>,
}

impl From<tui::AppRuntimeInfo> for ChatRuntimeInfo {
    fn from(info: tui::AppRuntimeInfo) -> Self {
        ChatRuntimeInfo {
            agent: info.agent,
            provider: info.provider,
            model: info.model,
            account: info.account,
            reasoning_effort: info.reasoning_effort,
        }
    }
}

/// Tool runtime placement for chat and channel turns.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatToolRuntimeSelection {
    pub sandbox_node_id: Option<String>,
}

impl ChatToolRuntimeSelection {
    fn from_agent_definition(definition: &AgentDefinition) -> Self {
        Self {
            sandbox_node_id: definition.tool_runtime.sandbox_node_id.clone(),
        }
    }
}

/// A streamed event produced while the agent answers.
#[derive(Clone, Debug)]
pub enum ChatStreamEvent {
    /// A visible answer fragment.
    Token(String),
    /// A visible answer fragment with channel participant metadata.
    AgentToken { speaker: ChatSpeaker, text: String },
    /// A transient progress / status note (e.g. "waiting for model runtime").
    Notice(String),
    /// A channel-scoped system information/error message.
    ChannelSystem {
        channel_id: String,
        message_id: String,
        kind: String,
        text: String,
    },
    /// A model-visible context budget report for the next sampling step.
    Context(ChatContextSnapshot),
    /// A native/model tool call started.
    ToolCallStarted(ChatToolCallSnapshot),
    /// Incremental tool-call arguments from providers that stream them.
    ToolCallDelta {
        step_index: usize,
        call_id: Option<String>,
        tool: Option<String>,
        arguments_delta: String,
    },
    /// A native/model tool call completed.
    ToolCallCompleted(ChatToolCallSnapshot),
    /// Local tool/process execution progress.
    Process(ChatProcessSnapshot),
    /// Token accounting for the just-finished turn.
    Usage(ChatUsageSnapshot),
}

/// One lane in the model-visible context budget bar.
#[derive(Clone, Debug, Default)]
pub struct ChatContextLane {
    pub kind: String,
    pub label: String,
    pub bytes: usize,
    pub priority: u8,
}

/// Compact context report for the app UI.
#[derive(Clone, Debug, Default)]
pub struct ChatContextSnapshot {
    pub step_index: usize,
    pub provider: String,
    pub model: String,
    pub total_bytes: usize,
    pub max_context_bytes: Option<usize>,
    pub compaction_threshold_bytes: Option<usize>,
    pub lanes: Vec<ChatContextLane>,
    pub included_count: usize,
    pub omitted_count: usize,
}

/// Compact model/native tool-call progress for the app UI.
#[derive(Clone, Debug, Default)]
pub struct ChatToolCallSnapshot {
    pub step_index: usize,
    pub call_index: Option<usize>,
    pub call_id: Option<String>,
    pub tool: String,
    pub input: Option<String>,
    pub input_preview: Option<String>,
}

/// Compact process/tool progress for the app UI.
#[derive(Clone, Debug, Default)]
pub struct ChatProcessSnapshot {
    pub process_id: String,
    pub command: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub stdout: String,
    pub stderr: String,
    pub stdout_tail: String,
    pub stderr_tail: String,
}

/// Token usage for the just-finished model response.
#[derive(Clone, Debug, Default)]
pub struct ChatUsageSnapshot {
    pub provider: String,
    pub model: String,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

/// Speaker metadata attached to channel-backed agent output.
#[derive(Clone, Debug, Default)]
pub struct ChatSpeaker {
    pub channel_id: String,
    pub participant_id: String,
    pub agent_key: String,
    pub agent_name: String,
    pub agent_session_id: String,
}

/// A channel participant exposed to the app.
#[derive(Clone, Debug, Default)]
pub struct ChatParticipant {
    pub participant_id: String,
    pub agent_key: String,
    pub agent_name: String,
    pub agent_session_id: String,
    pub enabled: bool,
    pub order: u32,
    pub tool_runtime: ChatToolRuntimeSelection,
}

/// Compact background task state for the app UI.
#[derive(Clone, Debug, Default)]
pub struct ChatTaskSnapshot {
    pub task_id: String,
    pub kind: String,
    pub summary: String,
    pub status: String,
    pub progress_summary: Option<String>,
    pub result_ref: Option<String>,
    pub active_op_id: Option<String>,
}

/// One visible message restored into a GUI chat runtime.
#[derive(Clone, Debug)]
pub struct ChatHistoryMessage {
    pub role: String,
    pub text: String,
}

/// A local file selected by the GUI for upload into the chat/channel attachment store.
#[derive(Clone, Debug)]
pub struct ChatAttachmentInput {
    pub path: PathBuf,
}

/// Outcome of a live chat-runtime control operation.
#[derive(Clone, Debug, Default)]
pub struct ChatControlStatus {
    pub session_id: String,
    pub accepted: bool,
    pub runtime_attached: bool,
    pub sequence: u64,
    pub event_count: usize,
    pub active_turn: bool,
    pub task_count: usize,
    pub terminal_result_count: usize,
    pub tasks: Vec<ChatTaskSnapshot>,
}

/// Snapshot of the live chat runtime's current control state.
#[derive(Clone, Debug, Default)]
pub struct ChatControlState {
    pub session_id: String,
    pub accepted: bool,
    pub runtime_attached: bool,
    pub reattach_state: AgentSessionReattachState,
    pub reattach_active_turn_persisted: bool,
    pub reattach_running_task_count: usize,
    pub reattach_persisted_event_count: usize,
    pub active_turn: bool,
    pub next_sequence: u64,
    pub task_count: usize,
    pub tasks: Vec<ChatTaskSnapshot>,
    pub event_cursor_offset: usize,
    pub steering_count: usize,
    pub follow_up_count: usize,
    pub detail: Option<String>,
}

/// A selectable agent.
#[derive(Clone, Debug)]
pub struct AgentSummary {
    pub key: String,
    pub name: String,
    pub description: String,
    pub tool_runtime: ChatToolRuntimeSelection,
}

/// A skill catalog entry exposed to the GUI.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillSummary {
    pub id: String,
    pub version: String,
    pub description: String,
    pub source_kind: String,
    pub source_id: Option<String>,
    pub source_version: Option<String>,
    pub active: bool,
    pub status: String,
    pub sifs_path: String,
    pub content_hash: String,
}

impl SkillSummary {
    fn from_entry(entry: &SkillCatalogEntry, active: bool) -> Self {
        Self {
            id: entry.skill_id.clone(),
            version: entry.version.clone(),
            description: entry.description.clone(),
            source_kind: entry.source.kind.as_str().to_string(),
            source_id: entry.source.source_id.clone(),
            source_version: entry.source.source_version.clone(),
            active,
            status: entry.status.as_str().to_string(),
            sifs_path: entry.sifs_path.clone(),
            content_hash: entry.content_hash.clone(),
        }
    }
}

/// A full skill detail exposed to management surfaces.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillDetail {
    pub summary: SkillSummary,
    pub manifest_path: String,
    pub base_hash: Option<String>,
    pub target_agents: Vec<String>,
    pub model_families: Vec<String>,
    pub activation_tags: Vec<String>,
    pub required_capabilities: Vec<String>,
    pub text: String,
}

impl SkillDetail {
    fn from_entry(entry: &SkillCatalogEntry, active: bool, text: String) -> Self {
        Self {
            summary: SkillSummary::from_entry(entry, active),
            manifest_path: entry.manifest_path.clone(),
            base_hash: entry.base_hash.clone(),
            target_agents: entry.target_agents.clone(),
            model_families: entry.model_families.clone(),
            activation_tags: entry.activation_tags.clone(),
            required_capabilities: entry.required_capabilities.clone(),
            text,
        }
    }
}

/// Result of importing, exporting, or activating a skill from the app.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SkillMutationResult {
    pub status: String,
    pub skill: SkillSummary,
    pub path: Option<String>,
    pub scope: Option<String>,
}

/// A selectable language-model provider with a small starter model list.
#[derive(Clone, Debug)]
pub struct ProviderSummary {
    pub id: String,
    pub account: String,
    pub name: String,
    pub models: Vec<String>,
}

/// The app's current local cluster state, as plain strings.
#[derive(Clone, Debug, Default)]
pub struct AppClusterStatus {
    pub clustered: bool,
    pub cluster_id: Option<String>,
    pub local_node_id: Option<String>,
    pub summary: Option<String>,
    pub error: Option<String>,
}

/// An invitation generated for a new node.
#[derive(Clone, Debug)]
pub struct AppClusterInvite {
    pub uri: String,
    pub join_command: String,
    pub expires_at: String,
    pub persisted_key: String,
}

/// Daemon-backed SIFS access for app agent-channel persistence.
#[derive(Clone)]
pub struct AgentChatSifsStorage {
    address: String,
    runtime: tokio::runtime::Handle,
}

impl AgentChatSifsStorage {
    pub fn new(address: impl Into<String>, runtime: &tokio::runtime::Handle) -> Self {
        Self {
            address: address.into(),
            runtime: runtime.clone(),
        }
    }

    pub fn open_channel_store(&self) -> Result<SifsAgentChannelStore, String> {
        SifsAgentChannelStore::open_daemon_on_runtime(&self.address, &self.runtime)
    }
}

/// SIFS-backed channel store used by the app chat runtime.
pub struct SifsAgentChannelStore {
    browser: SifsBrowser,
}

impl SifsAgentChannelStore {
    pub fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        SifsBrowser::open(data_dir).map(Self::from_browser)
    }

    pub fn open_daemon_on_runtime(
        address: impl Into<String>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        SifsBrowser::open_daemon_on_runtime(address, runtime).map(Self::from_browser)
    }

    pub fn from_browser(browser: SifsBrowser) -> Self {
        Self { browser }
    }

    fn channel_dir(channel_id: &SessionId) -> String {
        format!("{}/{}", SIFS_AGENT_CHANNELS_ROOT, channel_id.as_str())
    }

    fn snapshot_path(channel_id: &SessionId) -> String {
        format!("{}/snapshot.json", Self::channel_dir(channel_id))
    }

    fn message_chunks_dir(channel_id: &SessionId) -> String {
        format!("{}/messages", Self::channel_dir(channel_id))
    }

    fn message_index_path(channel_id: &SessionId) -> String {
        format!("{}/index.json", Self::message_chunks_dir(channel_id))
    }

    fn message_chunk_path(channel_id: &SessionId, sequence: u64) -> String {
        let chunk = sequence.saturating_sub(1) / SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE;
        format!("{}/{chunk:08}.jsonl", Self::message_chunks_dir(channel_id))
    }

    fn turn_chunks_dir(channel_id: &SessionId) -> String {
        format!("{}/turns", Self::channel_dir(channel_id))
    }

    fn turn_index_path(channel_id: &SessionId) -> String {
        format!("{}/index.json", Self::turn_chunks_dir(channel_id))
    }

    fn turn_chunk_path(channel_id: &SessionId, sequence: u64) -> String {
        let chunk = sequence / SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE;
        format!("{}/{chunk:08}.jsonl", Self::turn_chunks_dir(channel_id))
    }

    fn attachment_path(channel_id: &SessionId, attachment_id: &TaskId, file_name: &str) -> String {
        format!(
            "{}/attachments/{}/{}",
            Self::channel_dir(channel_id),
            attachment_id,
            safe_agent_channel_attachment_file_name(file_name)
        )
    }

    fn sorted_visible_summaries(
        summaries: impl IntoIterator<Item = AgentChannelSummary>,
    ) -> Vec<AgentChannelSummary> {
        let mut summaries = summaries
            .into_iter()
            .filter(|summary| !summary.is_deleted())
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        summaries
    }

    fn ensure_parent(&self, path: &str) -> Result<(), AgentChannelStoreError> {
        if let Some(parent) = self.browser.parent(path) {
            self.browser
                .create_dir_all(&parent)
                .map_err(|detail| channel_store_io(path, detail))?;
        }
        Ok(())
    }

    fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), AgentChannelStoreError> {
        self.ensure_parent(path)?;
        self.browser
            .write_file(path, bytes)
            .map_err(|detail| channel_store_io(path, detail))
    }

    fn append_json_line<T: Serialize>(
        &self,
        path: &str,
        value: &T,
    ) -> Result<(), AgentChannelStoreError> {
        let mut bytes = match self.browser.read_file(path) {
            Ok(bytes) => bytes,
            Err(detail) if sifs_error_is_not_found(&detail) => Vec::new(),
            Err(detail) => return Err(channel_store_io(path, detail)),
        };
        if !bytes.is_empty() && !bytes.ends_with(b"\n") {
            bytes.push(b'\n');
        }
        let line = serde_json::to_string(value).map_err(|source| {
            AgentChannelStoreError::Serialization {
                detail: source.to_string(),
            }
        })?;
        bytes.extend_from_slice(line.as_bytes());
        bytes.push(b'\n');
        self.write_file(path, &bytes)
    }

    fn append_message_chunk(
        &self,
        channel_id: &SessionId,
        message: &AgentChannelMessage,
    ) -> Result<(), AgentChannelStoreError> {
        let sequence = message.sequence.max(1);
        self.append_json_line(&Self::message_chunk_path(channel_id, sequence), message)?;
        self.update_chunked_log_index_count(&Self::message_index_path(channel_id), sequence)
    }

    fn append_indexed_chunk<T: Serialize>(
        &self,
        index_path: &str,
        chunk_path: impl FnOnce(u64) -> String,
        value: &T,
    ) -> Result<(), AgentChannelStoreError> {
        let mut index = self.read_chunked_log_index(index_path)?;
        let sequence = index.entry_count;
        self.append_json_line(&chunk_path(sequence), value)?;
        index.entry_count = index.entry_count.saturating_add(1);
        index.updated_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        self.write_file(
            index_path,
            &serde_json::to_vec_pretty(&index).map_err(|source| {
                AgentChannelStoreError::Serialization {
                    detail: source.to_string(),
                }
            })?,
        )
    }

    fn update_chunked_log_index_count(
        &self,
        index_path: &str,
        entry_count: u64,
    ) -> Result<(), AgentChannelStoreError> {
        let mut index = self.read_chunked_log_index(index_path)?;
        if index.entry_count >= entry_count {
            return Ok(());
        }
        index.entry_count = entry_count;
        index.updated_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        self.write_file(
            index_path,
            &serde_json::to_vec_pretty(&index).map_err(|source| {
                AgentChannelStoreError::Serialization {
                    detail: source.to_string(),
                }
            })?,
        )
    }

    fn read_chunked_log_index(
        &self,
        index_path: &str,
    ) -> Result<SifsChunkedLogIndex, AgentChannelStoreError> {
        let bytes = match self.browser.read_file(index_path) {
            Ok(bytes) => bytes,
            Err(detail) if sifs_error_is_not_found(&detail) => {
                return Ok(SifsChunkedLogIndex::default());
            }
            Err(detail) => return Err(channel_store_io(index_path, detail)),
        };
        let index = serde_json::from_slice::<SifsChunkedLogIndex>(&bytes).map_err(|source| {
            AgentChannelStoreError::Serialization {
                detail: source.to_string(),
            }
        })?;
        if index.schema_version != SIFS_AGENT_CHANNEL_LOG_SCHEMA_VERSION {
            return Err(AgentChannelStoreError::Serialization {
                detail: format!(
                    "unsupported channel log schema version {}",
                    index.schema_version
                ),
            });
        }
        Ok(index)
    }

    pub fn load_recent_messages(
        &self,
        channel_id: &SessionId,
        limit: usize,
    ) -> Result<Vec<AgentChannelMessage>, AgentChannelStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let index = self.read_chunked_log_index(&Self::message_index_path(channel_id))?;
        if index.entry_count == 0 {
            return Ok(Vec::new());
        }
        self.load_recent_indexed_message_chunks(channel_id, limit, index.entry_count)
    }

    fn load_recent_indexed_message_chunks(
        &self,
        channel_id: &SessionId,
        limit: usize,
        entry_count: u64,
    ) -> Result<Vec<AgentChannelMessage>, AgentChannelStoreError> {
        let first_sequence = entry_count.saturating_sub(limit as u64).saturating_add(1);
        let first_chunk = first_sequence.saturating_sub(1) / SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE;
        let last_chunk = entry_count.saturating_sub(1) / SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE;
        let mut collected = Vec::new();
        for chunk in (first_chunk..=last_chunk).rev() {
            let path = format!("{}/{chunk:08}.jsonl", Self::message_chunks_dir(channel_id));
            let bytes = match self.browser.read_file(&path) {
                Ok(bytes) => bytes,
                Err(detail) if sifs_error_is_not_found(&detail) => continue,
                Err(detail) => return Err(channel_store_io(&path, detail)),
            };
            let mut messages = parse_json_lines::<AgentChannelMessage>(&bytes)?;
            messages.reverse();
            for message in messages {
                if message.sequence < first_sequence || message.sequence > entry_count {
                    continue;
                }
                collected.push(message);
                if collected.len() >= limit {
                    break;
                }
            }
            if collected.len() >= limit {
                break;
            }
        }
        collected.reverse();
        Ok(collected)
    }

    fn channel_ids_from_sifs(&self) -> Result<BTreeSet<SessionId>, AgentChannelStoreError> {
        let entries = self
            .browser
            .list_if_exists(SIFS_AGENT_CHANNELS_ROOT)
            .map_err(|detail| channel_store_io(SIFS_AGENT_CHANNELS_ROOT, detail))?;
        Ok(entries
            .into_iter()
            .filter(|entry| entry.is_dir)
            .filter_map(|entry| entry.name.parse::<SessionId>().ok())
            .collect())
    }

    fn channel_summaries_from_ids(
        &self,
        ids: &BTreeSet<SessionId>,
    ) -> Result<Vec<AgentChannelSummary>, AgentChannelStoreError> {
        let mut summaries = Vec::new();
        let paths = ids.iter().map(Self::snapshot_path).collect::<Vec<_>>();
        for (path, result) in self
            .browser
            .read_files(&paths)
            .map_err(|detail| channel_store_io(SIFS_AGENT_CHANNELS_ROOT, detail))?
        {
            let bytes = match result {
                Ok(bytes) => bytes,
                Err(detail) if sifs_error_is_not_found(&detail) => continue,
                Err(detail) => return Err(channel_store_io(&path, detail)),
            };
            let snapshot = serde_json::from_slice::<OwnedSifsAgentChannelSnapshot>(&bytes)
                .map_err(|source| AgentChannelStoreError::Serialization {
                    detail: source.to_string(),
                })?;
            if snapshot.schema_version != AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION {
                return Err(AgentChannelStoreError::Serialization {
                    detail: format!(
                        "unsupported agent channel snapshot schema version {}",
                        snapshot.schema_version
                    ),
                });
            }
            summaries.push(AgentChannelSummary::from_channel(&snapshot.channel));
        }
        summaries.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        Ok(summaries)
    }

    pub fn list_channel_summaries(
        &self,
    ) -> Result<Vec<AgentChannelSummary>, AgentChannelStoreError> {
        let ids = self.channel_ids_from_sifs()?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(Self::sorted_visible_summaries(
            self.channel_summaries_from_ids(&ids)?,
        ))
    }
}

impl AgentChannelStore for SifsAgentChannelStore {
    fn save_channel(&self, channel: &AgentChannel) -> Result<(), AgentChannelStoreError> {
        let channel = channel.recent_transcript_snapshot(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT);
        let snapshot = SifsAgentChannelSnapshot {
            schema_version: AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION,
            channel: &channel,
        };
        let bytes = serde_json::to_vec_pretty(&snapshot).map_err(|source| {
            AgentChannelStoreError::Serialization {
                detail: source.to_string(),
            }
        })?;
        self.write_file(&Self::snapshot_path(&channel.channel_id), &bytes)
    }

    fn load_channel(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
        let path = Self::snapshot_path(channel_id);
        let bytes = match self.browser.read_file(&path) {
            Ok(bytes) => bytes,
            Err(detail) if sifs_error_is_not_found(&detail) => return Ok(None),
            Err(detail) => return Err(channel_store_io(&path, detail)),
        };
        let snapshot =
            serde_json::from_slice::<OwnedSifsAgentChannelSnapshot>(&bytes).map_err(|source| {
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
        let mut channels = Vec::new();
        for channel_id in self.channel_ids_from_sifs()? {
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
        self.append_message_chunk(channel_id, message)
    }

    fn append_turn(
        &self,
        channel_id: &SessionId,
        turn: &AgentChannelTurn,
    ) -> Result<(), AgentChannelStoreError> {
        let index_path = Self::turn_index_path(channel_id);
        self.append_indexed_chunk(
            &index_path,
            |sequence| Self::turn_chunk_path(channel_id, sequence),
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
        let path = Self::attachment_path(channel_id, attachment_id, file_name);
        self.write_file(&path, bytes)?;
        Ok(path)
    }
}

#[derive(Serialize)]
struct SifsAgentChannelSnapshot<'a> {
    schema_version: u32,
    channel: &'a AgentChannel,
}

#[derive(Deserialize)]
struct OwnedSifsAgentChannelSnapshot {
    schema_version: u32,
    channel: AgentChannel,
}

fn channel_store_io(path: &str, detail: String) -> AgentChannelStoreError {
    AgentChannelStoreError::Io {
        path: path.to_string(),
        detail,
    }
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

/// One running chat conversation backed by the main-agent runtime.
pub struct AgentChat {
    data_dir: SinomoDataDir,
    daemon_connected: bool,
    channel_store: Box<dyn AgentChannelStore>,
    channel: AgentChannel,
    runtime: InteractionRuntime<InMemoryEventStore, Handler>,
    tool_runtime: ChatToolRuntimeSelection,
    tool_runtime_explicit_override: bool,
}

impl AgentChat {
    /// Build a fresh conversation. `daemon_connected` selects cluster vs. local
    /// config resolution for model switching.
    pub fn new(data_dir: SinomoDataDir, daemon_connected: bool) -> Result<Self, CliError> {
        Self::with_runtime_control_registry(
            data_dir,
            daemon_connected,
            AgentRuntimeControlRegistry::new(),
        )
    }

    pub fn with_runtime_control_registry(
        data_dir: SinomoDataDir,
        daemon_connected: bool,
        runtime_control_registry: AgentRuntimeControlRegistry,
    ) -> Result<Self, CliError> {
        let channel_store =
            SifsAgentChannelStore::open(&data_dir).map_err(|detail| CliError::Parse {
                detail: format!("open agent channel SIFS store failed: {detail}"),
            })?;
        Self::with_runtime_control_registry_and_channel_store(
            data_dir,
            daemon_connected,
            runtime_control_registry,
            Box::new(channel_store),
        )
    }

    pub fn with_runtime_control_registry_and_sifs(
        data_dir: SinomoDataDir,
        daemon_connected: bool,
        runtime_control_registry: AgentRuntimeControlRegistry,
        storage: AgentChatSifsStorage,
    ) -> Result<Self, CliError> {
        let channel_store = storage
            .open_channel_store()
            .map_err(|detail| CliError::Parse {
                detail: format!("open agent channel SIFS store failed: {detail}"),
            })?;
        Self::with_runtime_control_registry_and_channel_store(
            data_dir,
            daemon_connected,
            runtime_control_registry,
            Box::new(channel_store),
        )
    }

    fn with_runtime_control_registry_and_channel_store(
        data_dir: SinomoDataDir,
        daemon_connected: bool,
        runtime_control_registry: AgentRuntimeControlRegistry,
        channel_store: Box<dyn AgentChannelStore>,
    ) -> Result<Self, CliError> {
        let handler = tui::app_build_chat_handler(&data_dir, None)?
            .with_runtime_control_registry(runtime_control_registry);
        let session = app_session(None);
        let mut runtime =
            InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());
        runtime
            .handler_mut()
            .replace_agent_session(&session, agent_transcript_from_history(&[]))?;
        let mut channel = AgentChannel::with_default_title(session.session_id.clone());
        let info: ChatRuntimeInfo = tui::app_runtime_info(runtime.handler()).into();
        let tool_runtime = default_tool_runtime_for_agent(&data_dir, &info.agent);
        let align_primary_session = should_align_primary_participant(&channel);
        let participant_id = ensure_primary_participant(&mut channel, &info, align_primary_session);
        set_participant_tool_runtime(&mut channel, &participant_id, &tool_runtime);
        Ok(Self {
            data_dir,
            daemon_connected,
            channel_store,
            channel,
            runtime,
            tool_runtime,
            tool_runtime_explicit_override: false,
        })
    }

    /// Begin a brand-new chat (clears the in-memory transcript/context).
    pub fn reset_session(&mut self) {
        let session = app_session(None);
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), session.clone());
        let _ = self
            .runtime
            .handler_mut()
            .replace_agent_session(&session, agent_transcript_from_history(&[]));
        self.channel = AgentChannel::with_default_title(session.session_id.clone());
        let info = self.runtime_info();
        if !self.tool_runtime_explicit_override {
            self.tool_runtime = default_tool_runtime_for_agent(&self.data_dir, &info.agent);
        }
        let participant_id = ensure_primary_participant(&mut self.channel, &info, true);
        let tool_runtime = self.tool_runtime.clone();
        set_participant_tool_runtime(&mut self.channel, &participant_id, &tool_runtime);
    }

    /// Adopt an existing channel id for continued turns, using the backend channel
    /// store as the only source of durable timeline state.
    pub fn resume_session(
        &mut self,
        session_id: &str,
        history: &[ChatHistoryMessage],
    ) -> Result<(), CliError> {
        let session = app_session(Some(session_id));
        let info = self.runtime_info();
        let (mut channel, should_save_after_resume, loaded_existing_channel) =
            match self.channel_store.load_channel(&session.session_id) {
                Ok(Some(channel)) => (channel, true, true),
                Ok(None) => (
                    AgentChannel::with_default_title(session.session_id.clone()),
                    false,
                    false,
                ),
                Err(error) => {
                    return Err(CliError::Parse {
                        detail: format!("load chat channel failed: {error}"),
                    });
                }
            };
        let align_primary_session = should_align_primary_participant(&channel);
        let participant_id = ensure_primary_participant(&mut channel, &info, align_primary_session);
        if !self.tool_runtime_explicit_override {
            self.tool_runtime = channel
                .participant(&participant_id)
                .map(|participant| {
                    chat_tool_runtime_from_agent_runtime_definition(&participant.tool_runtime)
                })
                .filter(|tool_runtime| !tool_runtime.is_default())
                .unwrap_or_else(|| default_tool_runtime_for_agent(&self.data_dir, &info.agent));
        }
        if !loaded_existing_channel {
            let tool_runtime = self.tool_runtime.clone();
            set_participant_tool_runtime(&mut channel, &participant_id, &tool_runtime);
        }
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), session.clone());
        self.runtime
            .handler_mut()
            .replace_agent_session(&session, agent_transcript_from_channel(&channel, history))?;
        self.channel = channel;
        if should_save_after_resume {
            self.save_channel()?;
        }
        Ok(())
    }

    pub fn session_id(&self) -> String {
        self.channel.channel_id.to_string()
    }

    pub fn runtime_info(&self) -> ChatRuntimeInfo {
        tui::app_runtime_info(self.runtime.handler()).into()
    }

    pub fn tool_runtime(&self) -> ChatToolRuntimeSelection {
        self.tool_runtime.clone()
    }

    pub fn set_tool_runtime_node(
        &mut self,
        sandbox_node_id: Option<String>,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        let normalized = sandbox_node_id
            .map(|node_id| node_id.trim().to_string())
            .filter(|node_id| !node_id.is_empty());
        if let Some(node_id) = &normalized {
            parse_chat_tool_runtime_node_id(node_id)?;
        }
        self.tool_runtime.sandbox_node_id = normalized;
        self.tool_runtime_explicit_override = true;
        Ok(self.tool_runtime())
    }

    fn configure_tool_runtime_for_session(
        &mut self,
        session_id: &SessionId,
    ) -> Result<(), CliError> {
        let sandbox_node_id = parse_chat_tool_runtime_selection(&self.tool_runtime)?;
        self.runtime
            .handler_mut()
            .configure_tool_runtime_for_session(&self.data_dir, session_id, sandbox_node_id)
    }

    pub fn channel_id(&self) -> String {
        self.channel.channel_id.to_string()
    }

    pub fn participants(&self) -> Vec<ChatParticipant> {
        self.channel
            .participants
            .iter()
            .map(chat_participant)
            .collect()
    }

    /// Live control registry for this chat runtime's active agent sessions.
    pub fn runtime_control_registry(&self) -> AgentRuntimeControlRegistry {
        self.runtime.handler().runtime_control_registry()
    }

    /// Queue steering for the active agent session.
    pub fn steer(&self, message: impl Into<String>) -> Result<ChatControlStatus, CliError> {
        let session_id = self.runtime.session().session_id.clone();
        let result = self
            .runtime_control_registry()
            .steer(AgentSessionSteerRequest {
                session_id,
                message: message.into(),
            })
            .map_err(agent_control_cli_error)?;
        Ok(chat_control_status(result))
    }

    /// Queue a user follow-up for the active agent session.
    pub fn follow_up(&self, message: impl Into<String>) -> Result<ChatControlStatus, CliError> {
        let session_id = self.runtime.session().session_id.clone();
        let result = self
            .runtime_control_registry()
            .follow_up(AgentSessionFollowUpRequest {
                session_id,
                message: message.into(),
            })
            .map_err(agent_control_cli_error)?;
        Ok(chat_control_status(result))
    }

    /// Request interruption of the active agent turn.
    pub fn interrupt(&self, reason: impl Into<String>) -> Result<ChatControlStatus, CliError> {
        let session_id = self.runtime.session().session_id.clone();
        let result = self
            .runtime_control_registry()
            .interrupt(AgentSessionInterruptRequest {
                session_id,
                reason: reason.into(),
            })
            .map_err(agent_control_cli_error)?;
        Ok(chat_control_status(result))
    }

    /// Inspect the live runtime state for the active chat session.
    pub fn control_state(&self) -> Result<ChatControlState, CliError> {
        let session_id = self.runtime.session().session_id.clone();
        let response = self
            .runtime_control_registry()
            .resume(AgentSessionResumeRequest { session_id })
            .map_err(agent_control_cli_error)?;
        Ok(chat_control_state(response))
    }

    /// Send a user message and stream the agent's reply through `on_event`.
    pub fn send(
        &mut self,
        text: &str,
        on_event: &mut dyn FnMut(ChatStreamEvent),
    ) -> Result<(), CliError> {
        self.send_with_attachments(text, &[], on_event)
    }

    /// Send a user message with local files copied into the channel attachment store.
    pub fn send_with_attachments(
        &mut self,
        text: &str,
        attachments: &[ChatAttachmentInput],
        on_event: &mut dyn FnMut(ChatStreamEvent),
    ) -> Result<(), CliError> {
        self.reload_current_channel()?;
        if self.channel.channel_kind == AgentChannelKind::Channel {
            return self.send_channel_with_attachments(text, attachments, on_event);
        }
        let session_id = self.runtime.session().session_id.clone();
        self.configure_tool_runtime_for_session(&session_id)?;
        let speaker = self.primary_speaker();
        let attachments = self.store_chat_attachments(attachments)?;
        let mut source = AgentChannelSource::app();
        if !attachments.is_empty() {
            source.metadata = serde_json::json!({
                "attachment_count": attachments.len()
            });
        }
        let context_attachments = agent_context_attachments_from_channel_attachments(&attachments);
        let user_message = self.channel.append_user_message_with_attachments(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            text,
            source,
            attachments,
        );
        self.channel_store
            .append_message(&self.channel.channel_id, &user_message)
            .map_err(|source| agent_channel_store_cli_error("append chat user message", source))?;
        self.save_channel()?;

        let mut streamed_answer = String::new();
        let mut wrapped_event = |event: ChatStreamEvent| match event {
            ChatStreamEvent::Token(text) => {
                streamed_answer.push_str(&text);
                on_event(ChatStreamEvent::AgentToken {
                    speaker: speaker.clone(),
                    text,
                });
            }
            ChatStreamEvent::AgentToken { speaker, text } => {
                streamed_answer.push_str(&text);
                on_event(ChatStreamEvent::AgentToken { speaker, text });
            }
            event => on_event(event),
        };

        let result = (|| {
            let mut emit = |output: InteractionOutput| forward_output(output, &mut wrapped_event);
            let trailing = self
                .runtime
                .ingest_user_input_with_context_attachments_streaming(
                    text,
                    context_attachments.clone(),
                    &mut emit,
                )?;
            for output in trailing {
                forward_output(output, &mut wrapped_event);
            }
            drain_runtime_events(&mut self.runtime, &mut wrapped_event)
        })();

        match result {
            Ok(()) => {
                let mut response_message_ids = Vec::new();
                let mut system_message_ids = Vec::new();
                if !streamed_answer.trim().is_empty() {
                    let participant = self
                        .primary_participant()
                        .cloned()
                        .unwrap_or_else(|| participant_from_speaker(&speaker));
                    let message = self.channel.append_agent_message(
                        &participant,
                        streamed_answer,
                        AgentChannelSource::app(),
                    );
                    response_message_ids.push(message.message_id.clone());
                    self.channel_store
                        .append_message(&self.channel.channel_id, &message)
                        .map_err(|source| {
                            agent_channel_store_cli_error("append chat agent message", source)
                        })?;
                } else {
                    let message = self.channel.append_system_info(
                        "No agent responded.",
                        AgentChannelSource::system(serde_json::json!({
                            "channel_id": self.channel.channel_id,
                            "event_type": "no_agent_responded"
                        })),
                    );
                    system_message_ids.push(message.message_id.clone());
                    self.channel_store
                        .append_message(&self.channel.channel_id, &message)
                        .map_err(|source| {
                            agent_channel_store_cli_error("append chat system message", source)
                        })?;
                    emit_channel_system(&message, &self.channel.channel_id, on_event);
                }
                self.append_channel_turn(
                    user_message.message_id,
                    response_message_ids,
                    system_message_ids,
                )?;
                self.save_channel()?;
            }
            Err(error) => {
                let message = self.channel.append_system_error(
                    format!("Error: {error}"),
                    AgentChannelSource::system(serde_json::json!({
                        "channel_id": self.channel.channel_id,
                        "participant_id": speaker.participant_id,
                        "agent_key": speaker.agent_key,
                        "agent_session_id": speaker.agent_session_id,
                        "error_type": "agent_turn"
                    })),
                );
                self.channel_store
                    .append_message(&self.channel.channel_id, &message)
                    .map_err(|source| {
                        agent_channel_store_cli_error("append chat error message", source)
                    })?;
                emit_channel_system(&message, &self.channel.channel_id, on_event);
                self.append_channel_turn(
                    user_message.message_id,
                    Vec::new(),
                    vec![message.message_id.clone()],
                )?;
                self.save_channel()?;
            }
        }
        Ok(())
    }

    fn send_channel_with_attachments(
        &mut self,
        text: &str,
        attachments: &[ChatAttachmentInput],
        on_event: &mut dyn FnMut(ChatStreamEvent),
    ) -> Result<(), CliError> {
        let attachments = self.store_chat_attachments(attachments)?;
        let mut source = AgentChannelSource::app();
        if !attachments.is_empty() {
            source.metadata = serde_json::json!({
                "attachment_count": attachments.len()
            });
        }
        let context_attachments = agent_context_attachments_from_channel_attachments(&attachments);
        let data_dir = self.data_dir.clone();
        let daemon_connected = self.daemon_connected;
        let previous_agent = self.runtime_info().agent;
        let mut channel = self.channel.clone();
        for participant in &mut channel.participants {
            participant.observed_through_sequence = 0;
        }
        let executor = AppAgentChannelExecutor {
            data_dir: &data_dir,
            daemon_connected,
            runtime: &mut self.runtime,
            context_attachments,
            tool_runtime: self.tool_runtime.clone(),
            tool_runtime_explicit_override: self.tool_runtime_explicit_override,
        };
        let store = self.channel_store.as_ref();
        let mut channel_runtime = AgentChannelRuntime::new(channel, store, executor)
            .with_loop_config(AgentChannelLoopConfig::default());
        let mut channel_event = |event| forward_channel_runtime_event(event, on_event);
        let result = channel_runtime.send_user_message_from_with_attachments(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            text,
            source,
            attachments,
            &mut channel_event,
        );
        self.channel = channel_runtime.channel().clone();
        drop(channel_runtime);
        if let Some(definition) = resolve_agent(&data_dir, &previous_agent) {
            let _ = tui::app_switch_agent(
                self.runtime.handler_mut(),
                &data_dir,
                definition,
                daemon_connected,
            );
        }
        result.map(|_| ()).map_err(|source| CliError::Parse {
            detail: format!("channel turn failed: {source}"),
        })
    }

    fn store_chat_attachments(
        &self,
        attachments: &[ChatAttachmentInput],
    ) -> Result<Vec<AgentChannelAttachment>, CliError> {
        let mut stored = Vec::new();
        for attachment in attachments {
            let path = attachment.path.as_path();
            let metadata = fs::metadata(path).map_err(|source| CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
            if !metadata.is_file() {
                return Err(CliError::Parse {
                    detail: format!("attachment is not a file: {}", path.display()),
                });
            }
            let bytes = fs::read(path).map_err(|source| CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
            let file_name = attachment_file_name(path);
            let content_type = detect_attachment_content_type(path, &bytes);
            let model_visible =
                model_visible_attachment_projection(&file_name, &bytes, content_type.as_deref());
            let attachment_id = TaskId::generate();
            let stored_path = self
                .channel_store
                .save_attachment(&self.channel.channel_id, &attachment_id, &file_name, &bytes)
                .map_err(|source| CliError::Parse {
                    detail: format!("save attachment failed: {source}"),
                })?;
            stored.push(AgentChannelAttachment {
                attachment_id,
                file_name,
                original_path: path.display().to_string(),
                stored_path,
                size_bytes: metadata.len(),
                content_type: content_type.clone(),
                created_at: time::OffsetDateTime::now_utc(),
                source: AgentChannelSource {
                    metadata: serde_json::json!({
                        "ingress": "local-file",
                        "original_path": path.display().to_string(),
                        "content_type": content_type,
                        "model_visible": model_visible
                    }),
                    ..AgentChannelSource::app()
                },
            });
        }
        Ok(stored)
    }

    /// Switch the active provider/model/account; returns the new selection.
    pub fn set_model(
        &mut self,
        provider: impl Into<String>,
        model: impl Into<String>,
        account: impl Into<String>,
    ) -> Result<ChatRuntimeInfo, CliError> {
        let info = tui::app_switch_model(
            self.runtime.handler_mut(),
            &self.data_dir,
            provider.into(),
            model.into(),
            account.into(),
            self.daemon_connected,
        )?;
        Ok(info.into())
    }

    /// Switch to a named built-in / stored agent definition.
    pub fn set_agent(&mut self, agent_key: &str) -> Result<ChatRuntimeInfo, CliError> {
        let definition =
            resolve_agent(&self.data_dir, agent_key).ok_or_else(|| CliError::Parse {
                detail: format!("unknown agent `{agent_key}`"),
            })?;
        let info = tui::app_switch_agent(
            self.runtime.handler_mut(),
            &self.data_dir,
            definition.clone(),
            self.daemon_connected,
        )?;
        let info: ChatRuntimeInfo = info.into();
        if self.channel.channel_kind == AgentChannelKind::Channel {
            let participant_id = ensure_primary_participant(&mut self.channel, &info, false);
            let tool_runtime = ChatToolRuntimeSelection::from_agent_definition(&definition);
            set_participant_tool_runtime(&mut self.channel, &participant_id, &tool_runtime);
        } else {
            for participant in &mut self.channel.participants {
                participant.enabled = false;
            }
            if !self.tool_runtime_explicit_override {
                self.tool_runtime = ChatToolRuntimeSelection::from_agent_definition(&definition);
            }
            let align_primary_session = should_align_primary_participant(&self.channel);
            let participant_id =
                ensure_primary_participant(&mut self.channel, &info, align_primary_session);
            let tool_runtime = self.tool_runtime.clone();
            set_participant_tool_runtime(&mut self.channel, &participant_id, &tool_runtime);
        }
        self.save_channel()?;
        Ok(info)
    }

    fn primary_participant(&self) -> Option<&AgentChannelParticipant> {
        self.channel
            .participants
            .iter()
            .filter(|participant| participant.enabled)
            .min_by_key(|participant| participant.order)
    }

    fn primary_speaker(&mut self) -> ChatSpeaker {
        let info = self.runtime_info();
        let align_primary_session = should_align_primary_participant(&self.channel);
        let participant_id =
            ensure_primary_participant(&mut self.channel, &info, align_primary_session);
        let tool_runtime = self.tool_runtime.clone();
        set_participant_tool_runtime(&mut self.channel, &participant_id, &tool_runtime);
        self.primary_participant()
            .map(|participant| chat_speaker(&self.channel.channel_id, participant))
            .unwrap_or_else(|| ChatSpeaker {
                channel_id: self.channel.channel_id.to_string(),
                agent_key: info.agent.clone(),
                agent_name: info.agent,
                ..ChatSpeaker::default()
            })
    }

    fn append_channel_turn(
        &self,
        trigger_message_id: TaskId,
        response_message_ids: Vec<TaskId>,
        system_message_ids: Vec<TaskId>,
    ) -> Result<(), CliError> {
        let turn = AgentChannelTurn {
            turn_id: TaskId::generate(),
            channel_id: self.channel.channel_id.clone(),
            trigger_message_id,
            decisions: Vec::new(),
            response_message_ids,
            system_message_ids,
            created_at: time::OffsetDateTime::now_utc(),
        };
        self.channel_store
            .append_turn(&self.channel.channel_id, &turn)
            .map_err(|source| agent_channel_store_cli_error("append chat turn", source))
    }

    fn save_channel(&self) -> Result<(), CliError> {
        self.channel_store
            .save_channel(&self.channel)
            .map_err(|source| agent_channel_store_cli_error("save chat channel", source))
    }

    fn reload_current_channel(&mut self) -> Result<(), CliError> {
        match self.channel_store.load_channel(&self.channel.channel_id) {
            Ok(Some(channel)) => {
                self.channel = channel;
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(source) => Err(agent_channel_store_cli_error("load chat channel", source)),
        }
    }
}

fn agent_channel_store_cli_error(action: &str, source: AgentChannelStoreError) -> CliError {
    CliError::Parse {
        detail: format!("{action} failed: {source}"),
    }
}

struct AppAgentChannelExecutor<'a> {
    data_dir: &'a SinomoDataDir,
    daemon_connected: bool,
    runtime: &'a mut InteractionRuntime<InMemoryEventStore, Handler>,
    context_attachments: Vec<AgentContextAttachment>,
    tool_runtime: ChatToolRuntimeSelection,
    tool_runtime_explicit_override: bool,
}

impl AgentChannelTurnExecutor for AppAgentChannelExecutor<'_> {
    fn decide_speak(
        &mut self,
        _channel: &AgentChannel,
        participant: &AgentChannelParticipant,
        _latest_user_message: &AgentChannelMessage,
        _session: &AgentSession,
    ) -> Result<AgentChannelSpeakDecision, AgentChannelExecutorError> {
        Ok(AgentChannelSpeakDecision::speak(
            participant,
            "agent session will decide whether to publish or stay silent",
            1.0,
        ))
    }

    fn speak(
        &mut self,
        channel: &AgentChannel,
        participant: &AgentChannelParticipant,
        session: &mut AgentSession,
        on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
    ) -> Result<String, AgentChannelExecutorError> {
        let definition = resolve_agent(self.data_dir, &participant.agent_key).ok_or_else(|| {
            AgentChannelExecutorError::new(format!(
                "unknown channel agent `{}`",
                participant.agent_key
            ))
        })?;
        tui::app_switch_agent(
            self.runtime.handler_mut(),
            self.data_dir,
            definition,
            self.daemon_connected,
        )
        .map_err(channel_executor_cli_error)?;

        let interaction_session = app_session(Some(&participant.agent_session_id.to_string()));
        let tool_runtime = if self.tool_runtime_explicit_override {
            self.tool_runtime.clone()
        } else {
            chat_tool_runtime_from_agent_runtime_definition(&participant.tool_runtime)
        };
        let sandbox_node_id =
            parse_chat_tool_runtime_selection(&tool_runtime).map_err(channel_executor_cli_error)?;
        self.runtime
            .handler_mut()
            .configure_tool_runtime_for_session(
                self.data_dir,
                &interaction_session.session_id,
                sandbox_node_id,
            )
            .map_err(channel_executor_cli_error)?;
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), interaction_session.clone());
        self.runtime
            .handler_mut()
            .replace_full_agent_session(&interaction_session, session.clone())
            .map_err(channel_executor_cli_error)?;

        let mut streamed_answer = String::new();
        let mut forward = |event: ChatStreamEvent| {
            if let ChatStreamEvent::Token(text) = event {
                streamed_answer.push_str(&text);
                on_event(AgentChannelRuntimeEvent::Token {
                    channel_id: channel.channel_id.clone(),
                    participant_id: participant.participant_id.clone(),
                    agent_key: participant.agent_key.clone(),
                    agent_name: participant.agent_name.clone(),
                    agent_session_id: participant.agent_session_id.clone(),
                    text,
                });
            }
        };
        let mut emit = |output: InteractionOutput| forward_output(output, &mut forward);
        let prompt = channel_agent_turn_prompt(channel, participant);
        let trailing = self
            .runtime
            .ingest_user_input_with_context_attachments_streaming(
                &prompt,
                self.context_attachments.clone(),
                &mut emit,
            )
            .map_err(channel_executor_cli_error)?;
        for output in trailing {
            forward_output(output, &mut forward);
        }
        drain_runtime_events(self.runtime, &mut forward).map_err(channel_executor_cli_error)?;
        if let Some(updated_session) = self
            .runtime
            .handler()
            .agent_session_snapshot()
            .map_err(channel_executor_cli_error)?
        {
            *session = updated_session;
        }
        Ok(streamed_answer)
    }
}

fn channel_executor_cli_error(error: CliError) -> AgentChannelExecutorError {
    AgentChannelExecutorError::new(error.to_string())
}

fn channel_agent_turn_prompt(
    channel: &AgentChannel,
    participant: &AgentChannelParticipant,
) -> String {
    let latest = channel
        .transcript
        .last()
        .map(render_visible_channel_message)
        .unwrap_or_else(|| "No visible channel messages yet.".to_string());
    format!(
        "You are {agent_name} ({agent_key}) in the shared Sinomo channel `{title}`.\n\
Every visible channel message is shared with every other enabled agent. \
Decide whether you should publish a reply to the latest visible message.\n\
If you have nothing useful, non-duplicative, or timely to add, return response_mode `silent` with empty text. \
If you reply, make `text` the exact message to publish in the channel.\n\n\
Latest visible channel message:\n{latest}",
        agent_name = participant.agent_name,
        agent_key = participant.agent_key,
        title = channel.title,
    )
}

fn render_visible_channel_message(message: &AgentChannelMessage) -> String {
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

fn forward_channel_runtime_event(
    event: AgentChannelRuntimeEvent,
    on_event: &mut dyn FnMut(ChatStreamEvent),
) {
    match event {
        AgentChannelRuntimeEvent::Token {
            channel_id,
            participant_id,
            agent_key,
            agent_name,
            agent_session_id,
            text,
        } => on_event(ChatStreamEvent::AgentToken {
            speaker: ChatSpeaker {
                channel_id: channel_id.to_string(),
                participant_id: participant_id.to_string(),
                agent_key,
                agent_name,
                agent_session_id: agent_session_id.to_string(),
            },
            text,
        }),
        AgentChannelRuntimeEvent::System {
            channel_id,
            message_id,
            message_kind,
            text,
        } => on_event(ChatStreamEvent::ChannelSystem {
            channel_id: channel_id.to_string(),
            message_id: message_id.to_string(),
            kind: match message_kind {
                AgentChannelMessageKind::Normal => "message",
                AgentChannelMessageKind::Info => "info",
                AgentChannelMessageKind::Error => "error",
            }
            .to_string(),
            text,
        }),
    }
}

fn chat_control_status(result: AgentSessionControlResult) -> ChatControlStatus {
    ChatControlStatus {
        session_id: result.snapshot.session_id.to_string(),
        accepted: result.accepted,
        runtime_attached: true,
        sequence: result.sequence,
        event_count: result.events.len(),
        active_turn: result.snapshot.active_turn.is_some(),
        task_count: result.snapshot.tasks.len(),
        terminal_result_count: result.terminal_results().len(),
        tasks: result
            .snapshot
            .tasks
            .iter()
            .map(chat_task_snapshot)
            .collect(),
    }
}

fn chat_control_state(response: AgentSessionResumeResponse) -> ChatControlState {
    let (active_turn, next_sequence, task_count) = response
        .control_snapshot
        .as_ref()
        .map(|snapshot| {
            (
                snapshot.active_turn.is_some(),
                snapshot.next_sequence,
                snapshot.tasks.len(),
            )
        })
        .unwrap_or((false, 0, 0));
    let (steering_count, follow_up_count) = response
        .session
        .as_ref()
        .map(|session| (session.steering_queue.len(), session.follow_up_queue.len()))
        .unwrap_or((0, 0));
    ChatControlState {
        session_id: response.session_id.to_string(),
        accepted: response.accepted,
        runtime_attached: response.runtime_attached,
        reattach_state: response.reattach.state.clone(),
        reattach_active_turn_persisted: response.reattach.active_turn_persisted,
        reattach_running_task_count: response.reattach.running_task_count,
        reattach_persisted_event_count: response.reattach.persisted_event_count,
        active_turn,
        next_sequence,
        task_count,
        tasks: response
            .control_snapshot
            .as_ref()
            .map(|snapshot| snapshot.tasks.iter().map(chat_task_snapshot).collect())
            .unwrap_or_default(),
        event_cursor_offset: response.event_cursor.offset,
        steering_count,
        follow_up_count,
        detail: response.detail,
    }
}

fn chat_task_snapshot(task: &AgentTaskSnapshot) -> ChatTaskSnapshot {
    ChatTaskSnapshot {
        task_id: task.task.task_id.to_string(),
        kind: task.task.kind.clone(),
        summary: task.task.summary.clone(),
        status: chat_task_status(&task.status).to_string(),
        progress_summary: task.progress_summary.clone(),
        result_ref: task.result_ref.clone(),
        active_op_id: task.active_op_id.as_ref().map(ToString::to_string),
    }
}

fn chat_task_status(status: &AgentTaskLifecycleStatus) -> &'static str {
    match status {
        AgentTaskLifecycleStatus::Pending => "pending",
        AgentTaskLifecycleStatus::Running => "running",
        AgentTaskLifecycleStatus::Completed => "completed",
        AgentTaskLifecycleStatus::Failed => "failed",
        AgentTaskLifecycleStatus::Cancelled => "cancelled",
        AgentTaskLifecycleStatus::TimedOut => "timed-out",
    }
}

fn agent_control_cli_error(error: sinomo_agent::AgentRuntimeControlError) -> CliError {
    CliError::Parse {
        detail: format!("agent runtime control: {error}"),
    }
}

fn agent_transcript_from_history(history: &[ChatHistoryMessage]) -> sinomo_agent::AgentTranscript {
    let mut transcript = sinomo_agent::AgentTranscript::new();
    for (index, message) in history.iter().enumerate() {
        if message.text.trim().is_empty() {
            continue;
        }
        match message.role.as_str() {
            "user" => transcript.push(AgentItem::UserMessage {
                item_id: TaskId::generate(),
                step_index: index,
                content: message.text.clone(),
                event_id: None,
            }),
            "agent" | "assistant" => transcript.push(AgentItem::AssistantText {
                item_id: TaskId::generate(),
                step_index: index,
                content: message.text.clone(),
            }),
            _ => {}
        }
    }
    transcript
}

fn agent_transcript_from_channel(
    channel: &AgentChannel,
    fallback_history: &[ChatHistoryMessage],
) -> sinomo_agent::AgentTranscript {
    if channel.transcript.is_empty() {
        return agent_transcript_from_history(fallback_history);
    }
    let mut transcript = sinomo_agent::AgentTranscript::new();
    for (index, message) in channel.transcript.iter().enumerate() {
        if (message.content.trim().is_empty() && message.attachments.is_empty())
            || message.message_kind != AgentChannelMessageKind::Normal
        {
            continue;
        }
        let content = channel_message_content_for_agent(message);
        match &message.author {
            AgentChannelAuthor::User { .. } => transcript.push(AgentItem::UserMessage {
                item_id: TaskId::generate(),
                step_index: index,
                content,
                event_id: None,
            }),
            AgentChannelAuthor::Agent { .. } => transcript.push(AgentItem::AssistantText {
                item_id: TaskId::generate(),
                step_index: index,
                content,
            }),
            AgentChannelAuthor::System => {}
        }
    }
    transcript
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

fn agent_context_attachments_from_channel_attachments(
    attachments: &[AgentChannelAttachment],
) -> Vec<AgentContextAttachment> {
    attachments
        .iter()
        .map(|attachment| AgentContextAttachment {
            kind: "chat_file_attachment".to_string(),
            summary: format!(
                "{} at {} ({} bytes)",
                attachment.file_name, attachment.stored_path, attachment.size_bytes
            ),
            payload_json: serde_json::json!({
                "attachment_id": attachment.attachment_id,
                "file_name": attachment.file_name,
                "original_path": attachment.original_path,
                "stored_path": attachment.stored_path,
                "size_bytes": attachment.size_bytes,
                "content_type": attachment.content_type,
                "model_visible": attachment
                    .source
                    .metadata
                    .get("model_visible")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                "channel_source": attachment.source
            }),
            payload_original_bytes: Some(attachment.size_bytes.min(usize::MAX as u64) as usize),
            payload_model_visible_truncated: false,
        })
        .collect()
}

fn detect_attachment_content_type(path: &Path, bytes: &[u8]) -> Option<String> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("image/png".to_string());
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Some("image/jpeg".to_string());
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("image/gif".to_string());
    }
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)?;
    let content_type = match extension.as_str() {
        "txt" | "text" | "log" => "text/plain",
        "md" | "markdown" => "text/markdown",
        "json" | "jsonl" => "application/json",
        "toml" => "application/toml",
        "yaml" | "yml" => "application/yaml",
        "csv" => "text/csv",
        "tsv" => "text/tab-separated-values",
        "xml" => "application/xml",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" | "cjs" => "text/javascript",
        "ts" | "tsx" => "text/typescript",
        "rs" => "text/x-rust",
        "py" => "text/x-python",
        "sh" | "bash" | "zsh" | "ps1" => "text/x-shellscript",
        "sql" => "application/sql",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "pdf" => "application/pdf",
        _ => return None,
    };
    Some(content_type.to_string())
}

fn model_visible_attachment_projection(
    file_name: &str,
    bytes: &[u8],
    content_type: Option<&str>,
) -> serde_json::Value {
    if let Some(text) = text_attachment_excerpt(bytes, content_type) {
        return text;
    }
    if let Some(image) = image_attachment_metadata(file_name, bytes, content_type) {
        return image;
    }
    serde_json::json!({
        "projection": "metadata_only",
        "note": "Attachment bytes are stored in SIFS; this model-visible context includes metadata only for this file type."
    })
}

fn text_attachment_excerpt(bytes: &[u8], content_type: Option<&str>) -> Option<serde_json::Value> {
    let text = std::str::from_utf8(bytes).ok()?;
    if !is_text_attachment(content_type) && !looks_like_text(text) {
        return None;
    }
    let (excerpt, truncated) = utf8_prefix(text, APP_CHAT_ATTACHMENT_TEXT_EXCERPT_BYTES);
    Some(serde_json::json!({
        "projection": "text_excerpt",
        "encoding": "utf-8",
        "bytes_included": excerpt.len(),
        "truncated": truncated,
        "text": excerpt
    }))
}

fn is_text_attachment(content_type: Option<&str>) -> bool {
    let Some(content_type) = content_type else {
        return false;
    };
    content_type.starts_with("text/")
        || matches!(
            content_type,
            "application/json"
                | "application/toml"
                | "application/yaml"
                | "application/xml"
                | "application/sql"
        )
}

fn looks_like_text(text: &str) -> bool {
    let sample = text.chars().take(2048);
    let mut count = 0usize;
    let mut control = 0usize;
    for ch in sample {
        count += 1;
        if ch.is_control() && !matches!(ch, '\n' | '\r' | '\t') {
            control += 1;
        }
    }
    count > 0 && control * 20 <= count
}

fn utf8_prefix(text: &str, max_bytes: usize) -> (&str, bool) {
    if text.len() <= max_bytes {
        return (text, false);
    }
    let mut end = max_bytes.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    (&text[..end], true)
}

fn image_attachment_metadata(
    file_name: &str,
    bytes: &[u8],
    content_type: Option<&str>,
) -> Option<serde_json::Value> {
    let (format, dimensions) = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        ("png", png_dimensions(bytes))
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        ("jpeg", jpeg_dimensions(bytes))
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        ("gif", gif_dimensions(bytes))
    } else if matches!(
        content_type,
        Some("image/png" | "image/jpeg" | "image/gif" | "image/webp")
    ) {
        (
            content_type?.strip_prefix("image/").unwrap_or("image"),
            None,
        )
    } else {
        return None;
    };
    Some(serde_json::json!({
        "projection": "image_metadata",
        "format": format,
        "content_type": content_type,
        "dimensions": dimensions.map(|(width, height)| serde_json::json!({
            "width": width,
            "height": height
        })),
        "note": format!(
            "{file_name} is stored in SIFS. Pixel-level image reasoning is not available through the current text-only chat request path."
        )
    }))
}

fn png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 24 || !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return None;
    }
    Some((
        u32::from_be_bytes(bytes[16..20].try_into().ok()?),
        u32::from_be_bytes(bytes[20..24].try_into().ok()?),
    ))
}

fn gif_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 10 || !(bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a")) {
        return None;
    }
    Some((
        u16::from_le_bytes(bytes[6..8].try_into().ok()?) as u32,
        u16::from_le_bytes(bytes[8..10].try_into().ok()?) as u32,
    ))
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 4 || !bytes.starts_with(b"\xff\xd8") {
        return None;
    }
    let mut index = 2usize;
    while index + 3 < bytes.len() {
        if bytes[index] != 0xff {
            index += 1;
            continue;
        }
        while index < bytes.len() && bytes[index] == 0xff {
            index += 1;
        }
        if index >= bytes.len() {
            return None;
        }
        let marker = bytes[index];
        index += 1;
        if matches!(marker, 0xd8 | 0xd9 | 0x01) || (0xd0..=0xd7).contains(&marker) {
            continue;
        }
        if index + 1 >= bytes.len() {
            return None;
        }
        let segment_len = u16::from_be_bytes([bytes[index], bytes[index + 1]]) as usize;
        if segment_len < 2 || index + segment_len > bytes.len() {
            return None;
        }
        if matches!(
            marker,
            0xc0 | 0xc1
                | 0xc2
                | 0xc3
                | 0xc5
                | 0xc6
                | 0xc7
                | 0xc9
                | 0xca
                | 0xcb
                | 0xcd
                | 0xce
                | 0xcf
        ) && segment_len >= 7
        {
            let height = u16::from_be_bytes([bytes[index + 3], bytes[index + 4]]) as u32;
            let width = u16::from_be_bytes([bytes[index + 5], bytes[index + 6]]) as u32;
            return Some((width, height));
        }
        index += segment_len;
    }
    None
}

fn attachment_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| "attachment".to_string())
}

fn ensure_primary_participant(
    channel: &mut AgentChannel,
    info: &ChatRuntimeInfo,
    align_session_to_channel: bool,
) -> TaskId {
    let agent_key = if info.agent.trim().is_empty() {
        "main-agent"
    } else {
        info.agent.as_str()
    };
    let agent_name = agent_key;
    let participant_id = channel.ensure_participant(agent_key, agent_name);
    if align_session_to_channel {
        let channel_id = channel.channel_id.clone();
        if let Some(participant) = channel.participant_mut(&participant_id) {
            participant.agent_session_id = channel_id;
            participant.order = 1;
            participant.enabled = true;
        }
    }
    participant_id
}

fn set_participant_tool_runtime(
    channel: &mut AgentChannel,
    participant_id: &TaskId,
    tool_runtime: &ChatToolRuntimeSelection,
) {
    if let Some(participant) = channel.participant_mut(participant_id) {
        participant.tool_runtime.sandbox_node_id = tool_runtime.sandbox_node_id.clone();
    }
}

fn default_tool_runtime_for_agent(
    data_dir: &SinomoDataDir,
    agent_key: &str,
) -> ChatToolRuntimeSelection {
    resolve_agent(data_dir, agent_key)
        .as_ref()
        .map(ChatToolRuntimeSelection::from_agent_definition)
        .unwrap_or_default()
}

fn chat_tool_runtime_from_agent_runtime_definition(
    tool_runtime: &sinomo_agent::AgentToolRuntimeDefinition,
) -> ChatToolRuntimeSelection {
    ChatToolRuntimeSelection {
        sandbox_node_id: tool_runtime.sandbox_node_id.clone(),
    }
}

impl ChatToolRuntimeSelection {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

fn should_align_primary_participant(channel: &AgentChannel) -> bool {
    channel.channel_kind != AgentChannelKind::Channel && channel.transcript.is_empty()
}

fn chat_speaker(channel_id: &SessionId, participant: &AgentChannelParticipant) -> ChatSpeaker {
    ChatSpeaker {
        channel_id: channel_id.to_string(),
        participant_id: participant.participant_id.to_string(),
        agent_key: participant.agent_key.clone(),
        agent_name: participant.agent_name.clone(),
        agent_session_id: participant.agent_session_id.to_string(),
    }
}

fn chat_participant(participant: &AgentChannelParticipant) -> ChatParticipant {
    ChatParticipant {
        participant_id: participant.participant_id.to_string(),
        agent_key: participant.agent_key.clone(),
        agent_name: participant.agent_name.clone(),
        agent_session_id: participant.agent_session_id.to_string(),
        enabled: participant.enabled,
        order: participant.order,
        tool_runtime: chat_tool_runtime_from_agent_runtime_definition(&participant.tool_runtime),
    }
}

fn participant_from_speaker(speaker: &ChatSpeaker) -> AgentChannelParticipant {
    let participant_id = speaker
        .participant_id
        .parse::<TaskId>()
        .unwrap_or_else(|_| TaskId::generate());
    let agent_session_id = speaker
        .agent_session_id
        .parse::<SessionId>()
        .unwrap_or_else(|_| SessionId::generate());
    AgentChannelParticipant {
        participant_id,
        agent_key: speaker.agent_key.clone(),
        agent_name: speaker.agent_name.clone(),
        agent_session_id,
        enabled: true,
        order: 1,
        tool_runtime: sinomo_agent::AgentToolRuntimeDefinition::default(),
        observed_through_sequence: 0,
    }
}

fn emit_channel_system(
    message: &AgentChannelMessage,
    channel_id: &SessionId,
    on_event: &mut dyn FnMut(ChatStreamEvent),
) {
    on_event(ChatStreamEvent::ChannelSystem {
        channel_id: channel_id.to_string(),
        message_id: message.message_id.to_string(),
        kind: match &message.message_kind {
            AgentChannelMessageKind::Normal => "message",
            AgentChannelMessageKind::Info => "info",
            AgentChannelMessageKind::Error => "error",
        }
        .to_string(),
        text: message.content.clone(),
    });
}

fn drain_runtime_events(
    runtime: &mut InteractionRuntime<InMemoryEventStore, Handler>,
    on_event: &mut dyn FnMut(ChatStreamEvent),
) -> Result<(), CliError> {
    drain_runtime_events_with_limits(
        || runtime.handler_mut().drain_runtime_events(),
        on_event,
        APP_CHAT_RUNTIME_START_TIMEOUT,
        APP_CHAT_RUNTIME_SILENCE_TIMEOUT,
        APP_CHAT_RUNTIME_DRAIN_INTERVAL,
    )
}

fn drain_runtime_events_with_limits(
    mut drain_events: impl FnMut() -> Result<Vec<AgentRuntimeEvent>, CliError>,
    on_event: &mut dyn FnMut(ChatStreamEvent),
    start_timeout: Duration,
    silence_timeout: Duration,
    drain_interval: Duration,
) -> Result<(), CliError> {
    let mut saw_worker = false;
    let mut terminal = false;
    let mut failure = None;
    let mut stream_state = ChatStreamState::default();
    let started_at = Instant::now();
    let mut last_event_at = started_at;

    loop {
        let events = drain_events()?;
        if events.is_empty() {
            if saw_worker && !terminal {
                if last_event_at.elapsed() >= silence_timeout {
                    return Err(CliError::ExternalCommand {
                        command: "agent runtime".to_string(),
                        detail: format!(
                            "did not produce a terminal event within {}s after the last progress event; the selected provider may be waiting for interactive authentication or be unavailable",
                            silence_timeout.as_secs()
                        ),
                    });
                }
                if drain_interval > Duration::ZERO {
                    thread::sleep(drain_interval);
                }
                continue;
            }
            if !terminal && started_at.elapsed() < start_timeout {
                if drain_interval > Duration::ZERO {
                    thread::sleep(drain_interval);
                }
                continue;
            }
            break;
        }
        last_event_at = Instant::now();

        for event in events {
            match runtime_event_to_chat(event, &mut stream_state, on_event) {
                RuntimeDrainState::Running => {
                    saw_worker = true;
                }
                RuntimeDrainState::TerminalOk => {
                    terminal = true;
                }
                RuntimeDrainState::TerminalErr(detail) => {
                    terminal = true;
                    failure = Some(detail);
                }
                RuntimeDrainState::Noop => {}
            }
        }

        if terminal {
            break;
        }
        if saw_worker {
            thread::sleep(APP_CHAT_RUNTIME_DRAIN_INTERVAL);
        } else {
            break;
        }
    }

    if let Some(detail) = failure {
        return Err(CliError::ExternalCommand {
            command: "agent runtime".to_string(),
            detail,
        });
    }
    Ok(())
}

enum RuntimeDrainState {
    Noop,
    Running,
    TerminalOk,
    TerminalErr(String),
}

#[derive(Default)]
struct ChatStreamState {
    visible_text: String,
    structured_json: String,
}

impl ChatStreamState {
    fn append_plain_token(&mut self, token: &str) {
        self.structured_json.clear();
        self.visible_text.push_str(token);
    }

    fn emit_text_suffix(&mut self, text: &str, on_event: &mut dyn FnMut(ChatStreamEvent)) {
        if text.is_empty() {
            return;
        }
        if self.visible_text.trim().is_empty() {
            self.visible_text.clear();
            self.visible_text.push_str(text);
            on_event(ChatStreamEvent::Token(text.to_string()));
        } else if text.starts_with(self.visible_text.as_str()) {
            let suffix = &text[self.visible_text.len()..];
            if !suffix.is_empty() {
                self.visible_text.push_str(suffix);
                on_event(ChatStreamEvent::Token(suffix.to_string()));
            }
        } else if self.visible_text.as_str() != text {
            self.visible_text.clear();
            self.visible_text.push_str(text);
            on_event(ChatStreamEvent::Token(text.to_string()));
        }
    }

    fn emit_final_text(&mut self, text: &str, on_event: &mut dyn FnMut(ChatStreamEvent)) {
        self.structured_json.clear();
        self.emit_text_suffix(text, on_event);
    }

    fn append_structured_delta(&mut self, delta: &str, on_event: &mut dyn FnMut(ChatStreamEvent)) {
        if self.structured_json.is_empty() {
            self.structured_json.push_str(delta);
        } else if delta.starts_with(self.structured_json.as_str()) {
            self.structured_json.clear();
            self.structured_json.push_str(delta);
        } else {
            self.structured_json.push_str(delta);
        }

        if let Some(text) = structured_json_text_prefix(&self.structured_json) {
            self.emit_text_suffix(&text, on_event);
        }
    }
}

fn runtime_event_to_chat(
    event: AgentRuntimeEvent,
    stream_state: &mut ChatStreamState,
    on_event: &mut dyn FnMut(ChatStreamEvent),
) -> RuntimeDrainState {
    match event.payload {
        AgentRuntimeEventPayload::WorkerStarted { action, .. } => {
            on_event(ChatStreamEvent::Notice(format!(
                "agent task started: {action}"
            )));
            RuntimeDrainState::Running
        }
        AgentRuntimeEventPayload::WorkerCompleted { .. } => RuntimeDrainState::TerminalOk,
        AgentRuntimeEventPayload::WorkerFailed { detail, .. } => {
            RuntimeDrainState::TerminalErr(detail)
        }
        AgentRuntimeEventPayload::TurnFinalDecision { decision, .. } => {
            let text = decision.text.trim();
            if !text.is_empty() {
                stream_state.emit_final_text(text, on_event);
            }
            RuntimeDrainState::Running
        }
        AgentRuntimeEventPayload::ModelProgress { event, .. } => {
            forward_model_event(event, stream_state, on_event);
            RuntimeDrainState::Running
        }
        AgentRuntimeEventPayload::ToolProgress { event, .. } => {
            forward_tool_event(event, on_event);
            RuntimeDrainState::Running
        }
        AgentRuntimeEventPayload::ControlEvent { .. } => RuntimeDrainState::Noop,
    }
}

fn forward_model_event(
    event: AgentModelEvent,
    stream_state: &mut ChatStreamState,
    on_event: &mut dyn FnMut(ChatStreamEvent),
) {
    match event {
        AgentModelEvent::Started {
            provider, model, ..
        } => on_event(ChatStreamEvent::Notice(format!(
            "model started: {provider}/{model}"
        ))),
        AgentModelEvent::ContextAssembled { report, .. } => {
            on_event(ChatStreamEvent::Context(chat_context_snapshot(report)));
        }
        AgentModelEvent::AssistantTextDelta { delta, .. } => {
            if !stream_state.structured_json.is_empty()
                || tui::starts_structured_agent_json_delta(&delta)
            {
                stream_state.append_structured_delta(&delta, on_event);
                return;
            }
            for event in split_chunk(&delta) {
                if let ChatStreamEvent::Token(token) = &event {
                    stream_state.append_plain_token(token);
                }
                on_event(event);
            }
        }
        AgentModelEvent::AssistantReasoningSummary { summary, .. } => {
            if !summary.trim().is_empty() {
                on_event(ChatStreamEvent::Notice(summary));
            }
        }
        AgentModelEvent::ToolCallStarted {
            step_index,
            call_index,
            call_id,
            tool,
        } => {
            on_event(ChatStreamEvent::ToolCallStarted(ChatToolCallSnapshot {
                step_index,
                call_index,
                call_id,
                tool,
                input: None,
                input_preview: None,
            }));
        }
        AgentModelEvent::ToolCallDelta {
            step_index,
            call_id,
            tool,
            arguments_delta,
        } => {
            on_event(ChatStreamEvent::ToolCallDelta {
                step_index,
                call_id,
                tool,
                arguments_delta,
            });
        }
        AgentModelEvent::ToolCallCompleted {
            step_index,
            call_index,
            call_id,
            call,
        } => {
            on_event(ChatStreamEvent::ToolCallCompleted(chat_tool_call_snapshot(
                step_index, call_index, call_id, call,
            )));
        }
        AgentModelEvent::Usage {
            provider,
            model,
            usage,
            ..
        } => on_event(ChatStreamEvent::Usage(ChatUsageSnapshot {
            provider,
            model,
            input_tokens: usage.as_ref().and_then(|usage| usage.input_tokens),
            output_tokens: usage.as_ref().and_then(|usage| usage.output_tokens),
        })),
        AgentModelEvent::Finished { .. } => {}
        AgentModelEvent::Error { detail, .. } => {
            on_event(ChatStreamEvent::Notice(format!("model failed: {detail}")));
        }
        AgentModelEvent::Aborted { reason } => {
            on_event(ChatStreamEvent::Notice(format!("model aborted: {reason}")));
        }
    }
}

fn structured_json_text_prefix(raw: &str) -> Option<String> {
    let mut json = raw.trim_start();
    if let Some(after_fence) = json.strip_prefix("```json") {
        json = after_fence.trim_start_matches(['\r', '\n']);
    } else if let Some(after_fence) = json.strip_prefix("```") {
        json = after_fence.trim_start_matches(['\r', '\n']);
    }
    let value_start = json_text_value_start(json)?;
    decode_json_string_prefix(&json[value_start..])
}

fn json_text_value_start(json: &str) -> Option<usize> {
    let mut search_from = 0;
    while let Some(relative) = json[search_from..].find("\"text\"") {
        let key_start = search_from + relative;
        if is_escaped_quote(json, key_start) {
            search_from = key_start + 1;
            continue;
        }
        let mut cursor = key_start + "\"text\"".len();
        cursor += json[cursor..]
            .chars()
            .take_while(|ch| ch.is_whitespace())
            .map(char::len_utf8)
            .sum::<usize>();
        if !json[cursor..].starts_with(':') {
            search_from = key_start + 1;
            continue;
        }
        cursor += ':'.len_utf8();
        cursor += json[cursor..]
            .chars()
            .take_while(|ch| ch.is_whitespace())
            .map(char::len_utf8)
            .sum::<usize>();
        if json[cursor..].starts_with('"') {
            return Some(cursor + '"'.len_utf8());
        }
        return None;
    }
    None
}

fn is_escaped_quote(text: &str, quote_index: usize) -> bool {
    let mut slash_count = 0;
    for byte in text[..quote_index].as_bytes().iter().rev() {
        if *byte == b'\\' {
            slash_count += 1;
        } else {
            break;
        }
    }
    slash_count % 2 == 1
}

fn decode_json_string_prefix(text: &str) -> Option<String> {
    let mut output = String::new();
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => return Some(output),
            '\\' => {
                let escaped = chars.next()?;
                match escaped {
                    '"' => output.push('"'),
                    '\\' => output.push('\\'),
                    '/' => output.push('/'),
                    'b' => output.push('\u{0008}'),
                    'f' => output.push('\u{000c}'),
                    'n' => output.push('\n'),
                    'r' => output.push('\r'),
                    't' => output.push('\t'),
                    'u' => {
                        let mut digits = String::new();
                        for _ in 0..4 {
                            let digit = chars.next()?;
                            if !digit.is_ascii_hexdigit() {
                                return Some(output);
                            }
                            digits.push(digit);
                        }
                        if let Ok(value) = u32::from_str_radix(&digits, 16)
                            && let Some(decoded) = char::from_u32(value)
                        {
                            output.push(decoded);
                        }
                    }
                    _ => return Some(output),
                }
            }
            _ => output.push(ch),
        }
    }
    Some(output)
}

fn forward_tool_event(event: AgentToolProgressEvent, on_event: &mut dyn FnMut(ChatStreamEvent)) {
    match event {
        AgentToolProgressEvent::ProcessStarted { process } => {
            on_event(ChatStreamEvent::Process(chat_process_snapshot(process)));
        }
        AgentToolProgressEvent::ProcessOutput { process } => {
            on_event(ChatStreamEvent::Process(chat_process_snapshot(process)));
        }
        AgentToolProgressEvent::ProcessCompleted { process } => {
            on_event(ChatStreamEvent::Process(chat_process_snapshot(process)));
        }
        AgentToolProgressEvent::Message { summary, .. } => {
            if !summary.trim().is_empty() {
                on_event(ChatStreamEvent::Notice(summary));
            }
        }
    }
}

fn forward_output(output: InteractionOutput, on_event: &mut dyn FnMut(ChatStreamEvent)) {
    match output {
        InteractionOutput::Chunk(chunk) => {
            for event in split_chunk(&chunk) {
                on_event(event);
            }
        }
        InteractionOutput::Line(line) => {
            for event in split_chunk(&line) {
                on_event(event);
            }
        }
        InteractionOutput::ModelUsage {
            provider, model, ..
        } => on_event(ChatStreamEvent::Usage(ChatUsageSnapshot {
            provider,
            model,
            input_tokens: None,
            output_tokens: None,
        })),
        InteractionOutput::Exit => {}
    }
}

fn chat_context_snapshot(report: ContextAssemblyReport) -> ChatContextSnapshot {
    ChatContextSnapshot {
        step_index: report.step_index,
        provider: report.provider,
        model: report.model,
        total_bytes: report.budget_estimate.total_bytes,
        max_context_bytes: report.budget_estimate.max_context_bytes,
        compaction_threshold_bytes: report.budget_estimate.compaction_threshold_bytes,
        lanes: report
            .budget_estimate
            .lanes
            .into_iter()
            .map(|lane| ChatContextLane {
                kind: lane.kind,
                label: lane.label,
                bytes: lane.bytes,
                priority: lane.priority,
            })
            .collect(),
        included_count: report.included.len(),
        omitted_count: report.omitted.len(),
    }
}

fn chat_tool_call_snapshot(
    step_index: usize,
    call_index: Option<usize>,
    call_id: Option<String>,
    call: sinomo_agent::AgentToolCall,
) -> ChatToolCallSnapshot {
    ChatToolCallSnapshot {
        step_index,
        call_index,
        call_id: call_id.or(call.call_id),
        tool: call.tool,
        input: json_display(&call.input),
        input_preview: json_preview(&call.input, 220),
    }
}

fn chat_process_snapshot(process: sinomo_agent::AgentProcessSnapshot) -> ChatProcessSnapshot {
    ChatProcessSnapshot {
        process_id: process.process_id,
        command: process.command,
        status: process.status.as_str().to_string(),
        exit_code: process.exit_code,
        elapsed_ms: process.elapsed_ms,
        stdout: process.stdout.clone(),
        stderr: process.stderr.clone(),
        stdout_tail: tail_chars(&process.stdout, 240),
        stderr_tail: tail_chars(&process.stderr, 240),
    }
}

fn json_display(value: &serde_json::Value) -> Option<String> {
    if value.is_null() {
        return None;
    }
    serde_json::to_string_pretty(value).ok()
}

fn json_preview(value: &serde_json::Value, max_chars: usize) -> Option<String> {
    if value.is_null() {
        return None;
    }
    serde_json::to_string(value)
        .ok()
        .map(|raw| truncate_chars(raw, max_chars))
}

fn tail_chars(value: &str, max_chars: usize) -> String {
    let chars = value.chars().count();
    if chars <= max_chars {
        return value.to_string();
    }
    let tail: String = value
        .chars()
        .skip(chars.saturating_sub(max_chars))
        .collect();
    format!("...{tail}")
}

fn truncate_chars(value: String, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value;
    }
    let head: String = value.chars().take(max_chars.saturating_sub(3)).collect();
    format!("{head}...")
}

/// Separate inline `<sinomo_progress>…</sinomo_progress>` status markers from
/// visible answer text.
fn split_chunk(chunk: &str) -> Vec<ChatStreamEvent> {
    if let Some(rest) = chunk.strip_prefix("<sinomo_progress>") {
        if let Some(progress) = rest.strip_suffix("</sinomo_progress>") {
            return vec![ChatStreamEvent::Notice(progress.to_string())];
        }
    }
    vec![ChatStreamEvent::Token(chunk.to_string())]
}

fn app_session(session_id: Option<&str>) -> InteractionSession {
    use std::str::FromStr;
    let node_id = NodeId::from_legacy_named("node_app").expect("valid app node id");
    let session_id = session_id
        .and_then(|id| SessionId::from_str(id).ok())
        .unwrap_or_else(SessionId::generate);
    InteractionSession::new(
        session_id,
        node_id,
        InteractionSurface::new("surface.app", InteractionSurfaceKind::App),
    )
}

fn parse_chat_tool_runtime_selection(
    selection: &ChatToolRuntimeSelection,
) -> Result<Option<NodeId>, CliError> {
    selection
        .sandbox_node_id
        .as_deref()
        .map(parse_chat_tool_runtime_node_id)
        .transpose()
}

fn parse_chat_tool_runtime_node_id(value: &str) -> Result<NodeId, CliError> {
    NodeId::new(value.to_string())
        .or_else(|_| NodeId::from_legacy_named(value.to_string()))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox runtime node id `{value}`: {source}"),
        })
}

/// All agents available for selection (stored definitions first, else built-ins).
pub fn agent_summaries(data_dir: &SinomoDataDir) -> Vec<AgentSummary> {
    let catalog =
        AgentCatalog::load_from_data_dir(data_dir).unwrap_or_else(|_| AgentCatalog::built_ins());
    catalog
        .list()
        .map(|definition| AgentSummary {
            key: definition.key().to_string(),
            name: if definition.name.is_empty() {
                definition.key().to_string()
            } else {
                definition.name.clone()
            },
            description: definition.description.clone(),
            tool_runtime: ChatToolRuntimeSelection::from_agent_definition(definition),
        })
        .collect()
}

fn resolve_agent(data_dir: &SinomoDataDir, key: &str) -> Option<AgentDefinition> {
    let catalog =
        AgentCatalog::load_from_data_dir(data_dir).unwrap_or_else(|_| AgentCatalog::built_ins());
    catalog.get(key).cloned()
}

/// List skills available to app management surfaces.
pub fn skill_summaries(
    data_dir: &SinomoDataDir,
    include_all: bool,
) -> Result<Vec<SkillSummary>, String> {
    let store =
        SkillCatalogSifsStore::open(Some(data_dir.root())).map_err(|error| error.to_string())?;
    let catalog = store.load_catalog().map_err(|error| error.to_string())?;
    let entries = if include_all {
        catalog.entries.iter().collect::<Vec<_>>()
    } else {
        catalog.active_entries_for_scopes([SkillActivationScope::Cluster.as_str()])
    };
    Ok(entries
        .into_iter()
        .map(|entry| {
            SkillSummary::from_entry(entry, skill_entry_is_active_for_app(&catalog, entry))
        })
        .collect())
}

/// Read one skill descriptor plus its stored `SKILL.md` text.
pub fn skill_detail(
    data_dir: &SinomoDataDir,
    skill_id: &str,
    version: Option<&str>,
) -> Result<SkillDetail, String> {
    let store =
        SkillCatalogSifsStore::open(Some(data_dir.root())).map_err(|error| error.to_string())?;
    let catalog = store.load_catalog().map_err(|error| error.to_string())?;
    let entry = select_skill_entry_for_app(&catalog, skill_id, version)?;
    let text = store
        .read_skill_text(&entry)
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    Ok(SkillDetail::from_entry(
        &entry,
        skill_entry_is_active_for_app(&catalog, &entry),
        text,
    ))
}

/// Import a community-style skill file or directory containing `SKILL.md`.
pub fn import_skill(
    data_dir: &SinomoDataDir,
    path: &Path,
    skill_id: Option<String>,
    version: Option<String>,
    activate: bool,
) -> Result<SkillMutationResult, String> {
    let version = version.unwrap_or_else(|| "1.0.0".to_string());
    let path = if path.is_dir() {
        path.join(sinomo_agent::SKILL_FILE_NAME)
    } else {
        path.to_path_buf()
    };
    let text = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let store =
        SkillCatalogSifsStore::open(Some(data_dir.root())).map_err(|error| error.to_string())?;
    let mut write = SkillCatalogWrite::from_markdown_skill(
        skill_id,
        version,
        SkillCatalogSource::user("app-user"),
        text,
    )
    .map_err(|error| error.to_string())?;
    if activate {
        write = write.activate_for_scope(
            SkillActivationScope::Cluster.as_str(),
            "app",
            "skill import",
        );
    }
    let entry = write.entry.clone();
    store
        .write_skill_version(write)
        .map_err(|error| error.to_string())?;
    Ok(SkillMutationResult {
        status: "imported".to_string(),
        skill: SkillSummary::from_entry(&entry, activate),
        path: Some(path.display().to_string()),
        scope: activate.then(|| SkillActivationScope::Cluster.as_str().to_string()),
    })
}

/// Export a catalog skill as portable Markdown, optionally writing it to disk.
pub fn export_skill(
    data_dir: &SinomoDataDir,
    skill_id: &str,
    version: Option<&str>,
    output: Option<&Path>,
) -> Result<SkillMutationResult, String> {
    let store =
        SkillCatalogSifsStore::open(Some(data_dir.root())).map_err(|error| error.to_string())?;
    let catalog = store.load_catalog().map_err(|error| error.to_string())?;
    let entry = select_skill_entry_for_app(&catalog, skill_id, version)?;
    let text = store
        .read_skill_text(&entry)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("skill `{}` has no readable SKILL.md body", entry.skill_id))?;
    let rendered = render_common_skill_markdown(&entry, &text);
    let output_path = output.map(|path| {
        if path.is_dir() {
            path.join(sinomo_agent::SKILL_FILE_NAME)
        } else {
            path.to_path_buf()
        }
    });
    if let Some(path) = output_path.as_deref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        fs::write(path, rendered.as_bytes()).map_err(|error| error.to_string())?;
    }
    Ok(SkillMutationResult {
        status: "exported".to_string(),
        skill: SkillSummary::from_entry(&entry, skill_entry_is_active_for_app(&catalog, &entry)),
        path: output_path.map(|path| path.display().to_string()),
        scope: None,
    })
}

/// Activate an existing skill version for the cluster scope.
pub fn activate_skill(
    data_dir: &SinomoDataDir,
    skill_id: &str,
    version: Option<&str>,
) -> Result<SkillMutationResult, String> {
    let store =
        SkillCatalogSifsStore::open(Some(data_dir.root())).map_err(|error| error.to_string())?;
    let catalog = store.load_catalog().map_err(|error| error.to_string())?;
    let entry = select_skill_entry_for_app(&catalog, skill_id, version)?;
    let scope = SkillActivationScope::Cluster.as_str();
    store
        .activate_skill_version(&entry, scope, "app", "skill activation")
        .map_err(|error| error.to_string())?;
    Ok(SkillMutationResult {
        status: "activated".to_string(),
        skill: SkillSummary::from_entry(&entry, true),
        path: None,
        scope: Some(scope.to_string()),
    })
}

fn select_skill_entry_for_app(
    catalog: &sinomo_agent::SkillCatalog,
    skill_id: &str,
    version: Option<&str>,
) -> Result<SkillCatalogEntry, String> {
    if let Some(version) = version {
        return catalog
            .entry(skill_id, version)
            .cloned()
            .ok_or_else(|| format!("unknown skill version {skill_id}@{version}"));
    }
    if let Some(active) =
        catalog.active_entry_for_scope(SkillActivationScope::Cluster.as_str(), skill_id)
    {
        return Ok(active.clone());
    }
    let matches = catalog
        .entries
        .iter()
        .filter(|entry| entry.skill_id == skill_id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [entry] => Ok((*entry).clone()),
        [] => Err(format!("unknown skill `{skill_id}`")),
        _ => Err(format!("skill `{skill_id}` has multiple versions")),
    }
}

fn skill_entry_is_active_for_app(
    catalog: &sinomo_agent::SkillCatalog,
    entry: &SkillCatalogEntry,
) -> bool {
    catalog.activations.iter().any(|activation| {
        activation.skill_id == entry.skill_id
            && activation.version == entry.version
            && activation.content_hash == entry.content_hash
    })
}

/// Curated provider list with a small starter set of models each.
pub fn provider_summaries() -> Vec<ProviderSummary> {
    vec![
        ProviderSummary {
            id: "anthropic".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Anthropic".to_string(),
            models: dedup(vec![
                AnthropicConfig::default().default_model,
                "claude-opus-4-8".to_string(),
                "claude-sonnet-4-6".to_string(),
                "claude-haiku-4-5".to_string(),
            ]),
        },
        ProviderSummary {
            id: "claude".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Claude".to_string(),
            models: dedup(vec![
                CLAUDE_DEFAULT_MODEL.to_string(),
                "claude-opus-4-8".to_string(),
                "claude-sonnet-4-6".to_string(),
                "claude-haiku-4-5".to_string(),
            ]),
        },
        ProviderSummary {
            id: "openai".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "OpenAI".to_string(),
            models: dedup(vec![
                OpenAiConfig::default().default_model,
                "gpt-5".to_string(),
                "o4-mini".to_string(),
            ]),
        },
        ProviderSummary {
            id: "gemini".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Google Gemini".to_string(),
            models: dedup(vec![
                GeminiConfig::default().default_model,
                "gemini-2.5-pro".to_string(),
                "gemini-2.5-flash".to_string(),
            ]),
        },
        ProviderSummary {
            id: "claude-code".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Claude Code (CLI)".to_string(),
            models: dedup(vec![ClaudeCodeConfig::default().model]),
        },
        ProviderSummary {
            id: "codex-cli".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Codex (CLI)".to_string(),
            models: dedup(vec![CodexCliConfig::default().model]),
        },
        ProviderSummary {
            id: "github-copilot".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "GitHub Copilot".to_string(),
            models: dedup(vec![GitHubCopilotConfig::default().default_model]),
        },
    ]
}

/// Reasoning-effort values, lowest to highest.
pub fn reasoning_efforts() -> Vec<String> {
    ProviderReasoningEffort::VALUES
        .iter()
        .map(|value| value.to_string())
        .collect()
}

/// Default model identifier used when no provider is configured yet.
pub fn fallback_model() -> &'static str {
    PROVIDER_DEFAULT_MODEL
}

/// Read the local cluster status without requiring the app to depend on CLI internals.
pub fn cluster_status(data_dir: &SinomoDataDir) -> AppClusterStatus {
    let paths = ClusterPaths::new(data_dir);
    match read_cli_cluster_state(&paths) {
        Ok(state) => {
            let cluster_id = state.cluster_id.to_string();
            let local_node_id = state.local_node_id.to_string();
            let member_state = state
                .members
                .iter()
                .find(|member| member.node_id == state.local_node_id)
                .map(|member| format!("{:?}", member.state).to_lowercase())
                .unwrap_or_else(|| "unknown".to_string());
            AppClusterStatus {
                clustered: true,
                cluster_id: Some(cluster_id.clone()),
                local_node_id: Some(local_node_id),
                summary: Some(format!(
                    "cluster {} · {}",
                    app_short_id(&cluster_id, 14),
                    member_state
                )),
                error: None,
            }
        }
        Err(CliError::ClusterOrphan) => AppClusterStatus::default(),
        Err(error) => AppClusterStatus {
            error: Some(error.to_string()),
            ..AppClusterStatus::default()
        },
    }
}

/// Read cluster health rows using the same direct probe fallback as the CLI.
pub fn cluster_health_rows(data_dir: &SinomoDataDir) -> Result<Vec<ClusterHealthRow>, CliError> {
    let paths = ClusterPaths::new(data_dir);
    let state = read_cli_cluster_state(&paths)?;
    cluster::cluster_health_rows_with_probe(
        &paths,
        &state,
        &crate::CliClusterStatusProbe { paths: &paths },
        false,
        time::OffsetDateTime::now_utc(),
    )
    .map_err(Into::into)
}

/// Ensure this data directory has a local single-node cluster, without printing CLI output.
pub fn ensure_single_node_cluster(
    data_dir: &SinomoDataDir,
    name: &str,
) -> Result<AppClusterStatus, CliError> {
    let paths = ClusterPaths::new(data_dir);
    match read_cli_cluster_state(&paths) {
        Ok(_) => return Ok(cluster_status(data_dir)),
        Err(CliError::ClusterOrphan) => {}
        Err(error) => return Err(error),
    }

    let init = initialize_single_node_cluster_with_paths(
        ClusterInitOptions {
            cluster_id: None,
            node_id: None,
            name: app_cluster_name(name),
        },
        &paths,
    );
    if let Err(error) = init {
        let status = cluster_status(data_dir);
        if status.clustered {
            return Ok(status);
        }
        return Err(error);
    }
    Ok(cluster_status(data_dir))
}

/// Generate a cluster invitation from the local node.
pub fn create_cluster_invite(data_dir: &SinomoDataDir) -> Result<AppClusterInvite, CliError> {
    let paths = ClusterPaths::new(data_dir);
    let rendered = cluster_invite_with_paths(ClusterInviteOptions::default(), &paths)?;
    Ok(AppClusterInvite {
        uri: rendered.uri,
        join_command: rendered.join_command,
        expires_at: rendered
            .invite
            .expires_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string()),
        persisted_key: rendered.persisted_key,
    })
}

/// Join a cluster using a raw invite URI, join command, JSON payload, or file reference.
pub fn join_cluster(
    data_dir: &SinomoDataDir,
    invite: &str,
    name: &str,
) -> Result<AppClusterStatus, CliError> {
    let invite = invite.trim();
    if invite.is_empty() {
        return Err(CliError::Parse {
            detail: "enter an invitation link before joining".to_string(),
        });
    }
    let paths = ClusterPaths::new(data_dir);
    cluster_join_with_paths(
        ClusterJoinOptions {
            invite: invite.to_string(),
            cluster_id: None,
            node_id: None,
            name: if name.trim().is_empty() {
                "Sinomo App".to_string()
            } else {
                name.trim().to_string()
            },
            name_explicit: true,
        },
        &paths,
    )?;
    Ok(cluster_status(data_dir))
}

/// Leave the current cluster and reinitialize this node as a single-node cluster.
pub fn leave_cluster(data_dir: &SinomoDataDir) -> Result<AppClusterStatus, CliError> {
    let paths = ClusterPaths::new(data_dir);
    cluster_leave_with_paths(&paths)?;
    Ok(cluster_status(data_dir))
}

fn dedup(values: Vec<String>) -> Vec<String> {
    let mut seen = Vec::new();
    for value in values {
        if !value.is_empty() && !seen.contains(&value) {
            seen.push(value);
        }
    }
    seen
}

fn app_short_id(id: &str, max_chars: usize) -> String {
    if id.chars().count() <= max_chars {
        id.to_string()
    } else {
        let keep = max_chars.saturating_sub(3);
        let head: String = id.chars().take(keep).collect();
        format!("{head}...")
    }
}

fn app_cluster_name(name: &str) -> String {
    let name = name.trim();
    if name.is_empty() {
        "Sinomo App".to_string()
    } else {
        name.to_string()
    }
}

// ----------------------------------------------------------------------------
// SIFS browsing
// ----------------------------------------------------------------------------

use crate::file_manager::{FileManagerBackend, FileManagerEntryKind};
use crate::sifs_file_manager::{SifsFileManagerBackend, SifsMetadataPrimaryTransferReport};

/// One SIFS directory entry.
#[derive(Clone, Debug)]
pub struct SifsEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub is_link: bool,
    pub size: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SifsListSortKey {
    #[default]
    Name,
    Kind,
    Size,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SifsListSortDirection {
    #[default]
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SifsListOptions {
    pub sort_key: SifsListSortKey,
    pub sort_direction: SifsListSortDirection,
    pub offset: usize,
    pub limit: Option<usize>,
}

impl Default for SifsListOptions {
    fn default() -> Self {
        Self {
            sort_key: SifsListSortKey::Name,
            sort_direction: SifsListSortDirection::Ascending,
            offset: 0,
            limit: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SifsEntryPage {
    pub entries: Vec<SifsEntry>,
    pub total_count: usize,
    pub offset: usize,
    pub limit: Option<usize>,
}

/// SIFS metadata for a selected object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsObjectDetails {
    pub file_system_id: String,
    pub object_id: String,
    pub path: String,
    pub kind: String,
    pub version: String,
    pub content_hash: Option<String>,
    pub size_bytes: Option<u64>,
    pub link_count: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub accessed_at_unix_nanos: u64,
    pub modified_at_unix_nanos: u64,
    pub created_at_unix_nanos: u64,
    pub changed_at_unix_nanos: u64,
    pub policy: String,
    pub current_locations: Vec<String>,
    pub local_pins: Vec<SifsPinDetails>,
    pub health: Option<SifsObjectHealth>,
    pub metadata_shard: Option<SifsMetadataShardDetails>,
}

/// Metadata shard placement for the selected SIFS path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsMetadataShardDetails {
    pub shard_id: String,
    pub primary_node_id: String,
    pub replica_node_ids: Vec<String>,
    pub epoch: u64,
    pub local_node_id: String,
    pub local_is_primary: bool,
    pub local_is_replica: bool,
}

/// Result of transferring a selected path's metadata shard primary to this node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsMetadataPrimaryTransferOutcome {
    pub file_system_id: String,
    pub shard_id: String,
    pub previous_primary_node_id: String,
    pub new_primary_node_id: String,
    pub previous_epoch: u64,
    pub new_epoch: u64,
    pub changed: bool,
    pub metadata_shard: SifsMetadataShardDetails,
}

/// Local DFS pin state for a selected SIFS object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsPinDetails {
    pub pinned_by_node_id: String,
    pub mode: String,
    pub created_at_unix_nanos: Option<u64>,
}

/// Operator-facing DFS health for a selected SIFS object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsObjectHealth {
    pub agent_state: String,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub missing_replica_nodes: Vec<String>,
}

/// Bounded operator health for the mounted SIFS namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsHealthSummary {
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub pending_write_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub pending_namespace_job_count: usize,
    pub failed_namespace_job_count: usize,
    pub metadata_shard_count: usize,
    pub repair_attempt_count: usize,
}

/// Bounded bytes read for a file preview or lightweight inspection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsFileRead {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

/// A handle to browse SIFS (the distributed filesystem).
pub struct SifsBrowser {
    backend: SifsBrowserBackend,
    node_id: String,
}

enum SifsBrowserBackend {
    Local(SifsFileManagerBackend),
    Remote(RemoteSifsBrowser),
}

struct RemoteSifsBrowser {
    address: String,
    timeout: Duration,
    runtime: Option<tokio::runtime::Handle>,
}

impl SifsBrowser {
    pub fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let backend = SifsFileManagerBackend::open(data_dir)?;
        let node_id = backend.node_label();
        Ok(Self {
            backend: SifsBrowserBackend::Local(backend),
            node_id,
        })
    }

    pub fn open_read_only(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let backend = SifsFileManagerBackend::open_read_only(data_dir)?;
        let node_id = backend.node_label();
        Ok(Self {
            backend: SifsBrowserBackend::Local(backend),
            node_id,
        })
    }

    pub fn open_daemon(address: impl Into<String>) -> Result<Self, String> {
        let backend = RemoteSifsBrowser {
            address: address.into(),
            timeout: Duration::from_secs(30),
            runtime: None,
        };
        Self::open_remote_backend(backend)
    }

    pub fn open_daemon_on_runtime(
        address: impl Into<String>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        let backend = RemoteSifsBrowser {
            address: address.into(),
            timeout: Duration::from_secs(30),
            runtime: Some(runtime.clone()),
        };
        Self::open_remote_backend(backend)
    }

    fn open_remote_backend(backend: RemoteSifsBrowser) -> Result<Self, String> {
        let status: sinomo_runtime::api::DaemonDfsBrowserStatusResponse = backend.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_STATUS_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserStatusRequest::node_only(),
        )?;
        Ok(Self {
            backend: SifsBrowserBackend::Remote(backend),
            node_id: status.node_id,
        })
    }

    /// The node SIFS objects are placed on locally.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Read a bounded namespace health summary for the current SIFS mount.
    pub fn health_summary(&self) -> Result<Option<SifsHealthSummary>, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend
                .operator_status()
                .map(|status| status.map(sifs_health_from_status)),
            SifsBrowserBackend::Remote(remote) => {
                let status = remote.status()?;
                if let Some(health) = status.health {
                    Ok(Some(sifs_health_summary_from_rpc(health)))
                } else if let Some(error) = status.health_error {
                    Err(error)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// List a directory (directories first, then files, name-sorted).
    pub fn list(&self, path: &str) -> Result<Vec<SifsEntry>, String> {
        let entries = match &self.backend {
            SifsBrowserBackend::Local(backend) => backend
                .list_dir(path)?
                .into_iter()
                .map(sifs_entry_from_file_manager)
                .collect(),
            SifsBrowserBackend::Remote(remote) => remote.list(path, false, false)?,
        };
        Ok(sort_sifs_entries(entries))
    }

    pub fn list_if_exists(&self, path: &str) -> Result<Vec<SifsEntry>, String> {
        let entries = match &self.backend {
            SifsBrowserBackend::Local(backend) => match backend.list_dir(path) {
                Ok(entries) => entries
                    .into_iter()
                    .map(sifs_entry_from_file_manager)
                    .collect(),
                Err(error) if sifs_error_is_not_found(&error) => Vec::new(),
                Err(error) => return Err(error),
            },
            SifsBrowserBackend::Remote(remote) => remote.list(path, false, true)?,
        };
        Ok(sort_sifs_entries(entries))
    }

    pub fn list_shallow(&self, path: &str) -> Result<Vec<SifsEntry>, String> {
        let entries = match &self.backend {
            SifsBrowserBackend::Local(backend) => backend
                .list_dir_shallow(path)?
                .into_iter()
                .map(sifs_entry_from_file_manager)
                .collect(),
            SifsBrowserBackend::Remote(remote) => remote.list(path, true, false)?,
        };
        Ok(sort_sifs_entries(entries))
    }

    pub fn list_shallow_page(
        &self,
        path: &str,
        options: SifsListOptions,
    ) -> Result<SifsEntryPage, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => {
                let entries = backend
                    .list_dir_shallow(path)?
                    .into_iter()
                    .map(sifs_entry_from_file_manager)
                    .collect::<Vec<_>>();
                Ok(page_sifs_entries(entries, options))
            }
            SifsBrowserBackend::Remote(remote) => remote.list_page(path, true, false, options),
        }
    }

    pub fn stat(&self, path: &str) -> Result<SifsObjectDetails, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.dfs_stat(path).map(|stat| {
                let local_pins = backend
                    .local_pins_for_path(&stat.file_system_id, &stat.path)
                    .unwrap_or_default();
                let local_node_id = backend.node_label();
                let metadata_shard = backend
                    .metadata_shard_for_path(&stat.path)
                    .ok()
                    .flatten()
                    .map(|shard| sifs_metadata_shard_from_config(shard, local_node_id.clone()));
                sifs_details_from_stat(stat, local_pins, metadata_shard)
            }),
            SifsBrowserBackend::Remote(remote) => remote.stat(path),
        }
    }

    pub fn read_file(&self, path: &str) -> Result<Vec<u8>, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.read_file(path),
            SifsBrowserBackend::Remote(remote) => remote.read_file(path),
        }
    }

    pub fn read_file_prefix(&self, path: &str, max_bytes: u64) -> Result<SifsFileRead, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => {
                let read = backend.read_file_prefix(path, max_bytes)?;
                Ok(SifsFileRead {
                    bytes: read.bytes,
                    truncated: read.truncated,
                })
            }
            SifsBrowserBackend::Remote(remote) => remote.read_file_prefix(path, max_bytes),
        }
    }

    pub fn read_files(
        &self,
        paths: &[String],
    ) -> Result<Vec<(String, Result<Vec<u8>, String>)>, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => Ok(paths
                .iter()
                .map(|path| (path.clone(), backend.read_file(path)))
                .collect()),
            SifsBrowserBackend::Remote(remote) => remote.read_files(paths),
        }
    }

    pub fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.write_file(path, bytes),
            SifsBrowserBackend::Remote(remote) => remote.write_file(path, bytes),
        }
    }

    pub fn create_dir_all(&self, path: &str) -> Result<(), String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.create_dir_all(path),
            SifsBrowserBackend::Remote(remote) => remote.create_dir_all(path),
        }
    }

    pub fn create_file(&self, path: &str) -> Result<(), String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.create_file(path),
            SifsBrowserBackend::Remote(remote) => remote.write_file(path, &[]),
        }
    }

    pub fn create_dir(&self, path: &str) -> Result<(), String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.create_dir(path),
            SifsBrowserBackend::Remote(remote) => remote.create_dir_all(path),
        }
    }

    pub fn rename(&self, from: &str, to: &str) -> Result<(), String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.rename(from, to),
            SifsBrowserBackend::Remote(remote) => remote.rename(from, to),
        }
    }

    pub fn delete(&self, path: &str, directory: bool) -> Result<(), String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => {
                if directory {
                    backend.delete_dir(path)
                } else {
                    backend.delete_file(path)
                }
            }
            SifsBrowserBackend::Remote(remote) => remote.delete(path, directory),
        }
    }

    pub fn request_maintenance_refresh(&self) -> Result<(), String> {
        match &self.backend {
            SifsBrowserBackend::Local(_) => Ok(()),
            SifsBrowserBackend::Remote(remote) => remote.request_maintenance_refresh(),
        }
    }

    pub fn pin_content(&self, path: &str) -> Result<String, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.pin_content(path),
            SifsBrowserBackend::Remote(remote) => remote.pin_content(path),
        }
    }

    pub fn unpin_content(&self, path: &str) -> Result<bool, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.unpin_content(path),
            SifsBrowserBackend::Remote(remote) => remote.unpin_content(path),
        }
    }

    pub fn pin_metadata(&self, path: &str) -> Result<String, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.pin_metadata(path),
            SifsBrowserBackend::Remote(remote) => remote.pin_metadata(path),
        }
    }

    pub fn unpin_metadata(&self, path: &str) -> Result<bool, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.unpin_metadata(path),
            SifsBrowserBackend::Remote(remote) => remote.unpin_metadata(path),
        }
    }

    pub fn make_metadata_primary(
        &self,
        path: &str,
    ) -> Result<SifsMetadataPrimaryTransferOutcome, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend
                .transfer_metadata_primary_to_local(path, None)
                .map(sifs_metadata_primary_transfer_from_backend),
            SifsBrowserBackend::Remote(remote) => remote.make_metadata_primary(path),
        }
    }

    pub fn parent(&self, path: &str) -> Option<String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.parent_path(path),
            SifsBrowserBackend::Remote(_) => parent_sifs_browser_path(path),
        }
    }

    pub fn normalize(&self, path: &str) -> String {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend
                .normalize_path(path)
                .unwrap_or_else(|_| "/".to_string()),
            SifsBrowserBackend::Remote(_) => {
                normalize_sifs_browser_path(path).unwrap_or_else(|_| "/".to_string())
            }
        }
    }
}

impl RemoteSifsBrowser {
    fn status(&self) -> Result<sinomo_runtime::api::DaemonDfsBrowserStatusResponse, String> {
        self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_STATUS_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserStatusRequest::with_health(),
        )
    }

    fn rpc<Req, Resp>(&self, route: &str, body: &Req) -> Result<Resp, String>
    where
        Req: serde::Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        let request =
            ObjectRpcRequest::encode_body(route, sinomo_runtime::api::DAEMON_RPC_CATEGORY, body)
                .map_err(|error| error.to_string())?;
        let response = if let Some(runtime) = &self.runtime {
            runtime
                .block_on(sinomo_runtime::api::send_daemon_rpc_async(
                    &self.address,
                    &request,
                    sinomo_runtime::network::DaemonRpcTimeouts::connect_and_idle(self.timeout),
                    runtime,
                ))
                .map_err(|error| error.to_string())?
        } else {
            sinomo_runtime::api::send_daemon_rpc(&self.address, &request, self.timeout)
                .map_err(|error| error.to_string())?
        };
        response.decode_body().map_err(|error| error.to_string())
    }

    fn list(
        &self,
        path: &str,
        shallow: bool,
        missing_is_empty: bool,
    ) -> Result<Vec<SifsEntry>, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserListResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_LIST_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow,
                missing_is_empty,
                sort_key: sinomo_runtime::api::DaemonDfsBrowserSortKey::Name,
                sort_direction: sinomo_runtime::api::DaemonDfsBrowserSortDirection::Ascending,
                offset: 0,
                limit: None,
            },
        )?;
        Ok(response
            .entries
            .into_iter()
            .map(sifs_entry_from_rpc)
            .collect())
    }

    fn list_page(
        &self,
        path: &str,
        shallow: bool,
        missing_is_empty: bool,
        options: SifsListOptions,
    ) -> Result<SifsEntryPage, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserListResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_LIST_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow,
                missing_is_empty,
                sort_key: daemon_sort_key_from_sifs(options.sort_key),
                sort_direction: daemon_sort_direction_from_sifs(options.sort_direction),
                offset: options.offset,
                limit: options.limit,
            },
        )?;
        Ok(SifsEntryPage {
            entries: response
                .entries
                .into_iter()
                .map(sifs_entry_from_rpc)
                .collect(),
            total_count: response.total_count,
            offset: response.offset,
            limit: response.limit,
        })
    }

    fn stat(&self, path: &str) -> Result<SifsObjectDetails, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserStatResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_STAT_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserStatRequest {
                path: path.to_string(),
            },
        )?;
        Ok(sifs_details_from_rpc(response))
    }

    fn read_file(&self, path: &str) -> Result<Vec<u8>, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserReadFileResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserReadFileRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.bytes)
    }

    fn read_file_prefix(&self, path: &str, max_bytes: u64) -> Result<SifsFileRead, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserReadFilePrefixResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserReadFilePrefixRequest {
                path: path.to_string(),
                max_bytes,
            },
        )?;
        Ok(SifsFileRead {
            bytes: response.bytes,
            truncated: response.truncated,
        })
    }

    fn read_files(
        &self,
        paths: &[String],
    ) -> Result<Vec<(String, Result<Vec<u8>, String>)>, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserReadFilesResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserReadFilesRequest {
                paths: paths.to_vec(),
            },
        )?;
        Ok(response
            .files
            .into_iter()
            .map(|file| {
                let result = match (file.bytes, file.error) {
                    (Some(bytes), _) => Ok(bytes),
                    (_, Some(error)) => Err(error),
                    (None, None) => Err("daemon returned no bytes".to_string()),
                };
                (file.path, result)
            })
            .collect())
    }

    fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonDfsBrowserWriteFileResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserWriteFileRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
            },
        )?;
        Ok(())
    }

    fn create_dir_all(&self, path: &str) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonDfsBrowserCreateDirAllResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserCreateDirAllRequest {
                path: path.to_string(),
            },
        )?;
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonDfsBrowserRenameResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_RENAME_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserRenameRequest {
                from: from.to_string(),
                to: to.to_string(),
            },
        )?;
        Ok(())
    }

    fn delete(&self, path: &str, directory: bool) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonDfsBrowserDeleteResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_DELETE_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserDeleteRequest {
                path: path.to_string(),
                directory,
            },
        )?;
        Ok(())
    }

    fn request_maintenance_refresh(&self) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonClusterMaintenanceRefreshResponse = self.rpc(
            sinomo_runtime::api::DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
            &sinomo_runtime::api::DaemonClusterMaintenanceRefreshRequest,
        )?;
        Ok(())
    }

    fn pin_content(&self, path: &str) -> Result<String, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserPinContentResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserPinContentRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.node_id)
    }

    fn unpin_content(&self, path: &str) -> Result<bool, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserUnpinContentResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserUnpinContentRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.removed)
    }

    fn pin_metadata(&self, path: &str) -> Result<String, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserPinMetadataResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserPinMetadataRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.node_id)
    }

    fn unpin_metadata(&self, path: &str) -> Result<bool, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserUnpinMetadataResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserUnpinMetadataRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.removed)
    }

    fn make_metadata_primary(
        &self,
        path: &str,
    ) -> Result<SifsMetadataPrimaryTransferOutcome, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserTransferMetadataPrimaryResponse = self
            .rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserTransferMetadataPrimaryRequest {
                path: path.to_string(),
                expected_epoch: None,
            },
        )?;
        Ok(sifs_metadata_primary_transfer_from_rpc(response))
    }
}

fn sifs_entry_from_file_manager(entry: crate::file_manager::FileManagerEntry) -> SifsEntry {
    SifsEntry {
        name: entry.name,
        path: entry.path,
        is_dir: matches!(entry.kind, FileManagerEntryKind::Directory),
        is_link: matches!(entry.kind, FileManagerEntryKind::Symlink),
        size: entry.size,
    }
}

fn sifs_entry_from_rpc(entry: sinomo_runtime::api::DaemonDfsBrowserEntry) -> SifsEntry {
    SifsEntry {
        name: entry.name,
        path: entry.path,
        is_dir: matches!(entry.kind, sinomo_runtime::dfs::DfsObjectKind::Directory),
        is_link: matches!(entry.kind, sinomo_runtime::dfs::DfsObjectKind::Symlink),
        size: entry.size,
    }
}

fn sifs_details_from_stat(
    stat: sinomo_runtime::dfs::DfsStat,
    local_pins: Vec<sinomo_runtime::dfs::DfsPinRecord>,
    metadata_shard: Option<SifsMetadataShardDetails>,
) -> SifsObjectDetails {
    SifsObjectDetails {
        file_system_id: stat.file_system_id.to_string(),
        object_id: stat.object_id.to_string(),
        path: stat.path,
        kind: sifs_kind_label(&stat.kind).to_string(),
        version: stat.version.to_string(),
        content_hash: stat.content_hash,
        size_bytes: stat.size_bytes,
        link_count: stat.link_count,
        mode: stat.mode,
        uid: stat.uid,
        gid: stat.gid,
        accessed_at_unix_nanos: stat.accessed_at_unix_nanos,
        modified_at_unix_nanos: stat.modified_at_unix_nanos,
        created_at_unix_nanos: stat.created_at_unix_nanos,
        changed_at_unix_nanos: stat.changed_at_unix_nanos,
        policy: sifs_policy_label(&stat.policy).to_string(),
        current_locations: stat
            .current_locations
            .into_iter()
            .map(|node_id| node_id.to_string())
            .collect(),
        local_pins: local_pins.into_iter().map(sifs_pin_from_record).collect(),
        health: None,
        metadata_shard,
    }
}

fn sifs_details_from_rpc(
    response: sinomo_runtime::api::DaemonDfsBrowserStatResponse,
) -> SifsObjectDetails {
    SifsObjectDetails {
        file_system_id: response.file_system_id,
        object_id: response.object_id,
        path: response.path,
        kind: sifs_kind_label(&response.kind).to_string(),
        version: response.version,
        content_hash: response.content_hash,
        size_bytes: response.size_bytes,
        link_count: response.link_count,
        mode: response.mode,
        uid: response.uid,
        gid: response.gid,
        accessed_at_unix_nanos: response.accessed_at_unix_nanos,
        modified_at_unix_nanos: response.modified_at_unix_nanos,
        created_at_unix_nanos: response.created_at_unix_nanos,
        changed_at_unix_nanos: response.changed_at_unix_nanos,
        policy: sifs_policy_label(&response.policy).to_string(),
        current_locations: response.current_locations,
        local_pins: response
            .local_pins
            .into_iter()
            .map(sifs_pin_from_rpc)
            .collect(),
        health: response.health.map(sifs_object_health_from_rpc),
        metadata_shard: response.metadata_shard.map(sifs_metadata_shard_from_rpc),
    }
}

fn sifs_metadata_shard_from_config(
    shard: sinomo_runtime::dfs::DfsMetadataShardConfig,
    local_node_id: String,
) -> SifsMetadataShardDetails {
    let primary_node_id = shard.primary_node_id.to_string();
    let replica_node_ids = shard
        .replica_node_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let local_is_primary = primary_node_id == local_node_id;
    let local_is_replica = replica_node_ids
        .iter()
        .any(|node_id| node_id == &local_node_id);
    SifsMetadataShardDetails {
        shard_id: shard.shard_id,
        primary_node_id,
        replica_node_ids,
        epoch: shard.epoch,
        local_node_id,
        local_is_primary,
        local_is_replica,
    }
}

fn sifs_metadata_shard_from_rpc(
    shard: sinomo_runtime::api::DaemonDfsBrowserMetadataShard,
) -> SifsMetadataShardDetails {
    SifsMetadataShardDetails {
        shard_id: shard.shard_id,
        primary_node_id: shard.primary_node_id,
        replica_node_ids: shard.replica_node_ids,
        epoch: shard.epoch,
        local_node_id: shard.local_node_id,
        local_is_primary: shard.local_is_primary,
        local_is_replica: shard.local_is_replica,
    }
}

fn sifs_metadata_primary_transfer_from_backend(
    report: SifsMetadataPrimaryTransferReport,
) -> SifsMetadataPrimaryTransferOutcome {
    let metadata_shard = sifs_metadata_shard_from_config(report.shard, report.local_node_id);
    SifsMetadataPrimaryTransferOutcome {
        file_system_id: report.file_system_id,
        shard_id: report.shard_id,
        previous_primary_node_id: report.previous_primary_node_id,
        new_primary_node_id: report.new_primary_node_id,
        previous_epoch: report.previous_epoch,
        new_epoch: report.new_epoch,
        changed: report.changed,
        metadata_shard,
    }
}

fn sifs_metadata_primary_transfer_from_rpc(
    response: sinomo_runtime::api::DaemonDfsBrowserTransferMetadataPrimaryResponse,
) -> SifsMetadataPrimaryTransferOutcome {
    SifsMetadataPrimaryTransferOutcome {
        file_system_id: response.file_system_id,
        shard_id: response.shard_id,
        previous_primary_node_id: response.previous_primary_node_id,
        new_primary_node_id: response.new_primary_node_id,
        previous_epoch: response.previous_epoch,
        new_epoch: response.new_epoch,
        changed: response.changed,
        metadata_shard: sifs_metadata_shard_from_rpc(response.metadata_shard),
    }
}

fn sifs_pin_from_record(pin: sinomo_runtime::dfs::DfsPinRecord) -> SifsPinDetails {
    SifsPinDetails {
        pinned_by_node_id: pin.pinned_by_node_id.to_string(),
        mode: sifs_pin_mode_label(&pin.mode).to_string(),
        created_at_unix_nanos: u64::try_from(pin.created_at.unix_timestamp_nanos()).ok(),
    }
}

fn sifs_pin_from_rpc(pin: sinomo_runtime::api::DaemonDfsBrowserPinRecord) -> SifsPinDetails {
    SifsPinDetails {
        pinned_by_node_id: pin.pinned_by_node_id,
        mode: sifs_pin_mode_label(&pin.mode).to_string(),
        created_at_unix_nanos: pin.created_at_unix_nanos,
    }
}

fn sifs_object_health_from_rpc(
    health: sinomo_runtime::api::DaemonDfsBrowserObjectHealth,
) -> SifsObjectHealth {
    SifsObjectHealth {
        agent_state: sifs_agent_state_label(&health.agent_state).to_string(),
        version_count: health.version_count,
        receipt_count: health.receipt_count,
        intent_count: health.intent_count,
        open_conflict_count: health.open_conflict_count,
        pending_repair_count: health.pending_repair_count,
        missing_replica_nodes: health.missing_replica_nodes,
    }
}

fn sifs_health_from_status(status: sinomo_runtime::dfs::DfsOperatorStatus) -> SifsHealthSummary {
    SifsHealthSummary {
        file_system_count: status.file_system_count,
        object_count: status.object_count,
        version_count: status.version_count,
        pending_write_count: status.pending_write_count,
        open_conflict_count: status.open_conflict_count,
        pending_repair_count: status.pending_repair_count,
        pending_namespace_job_count: status.pending_namespace_job_count,
        failed_namespace_job_count: status.failed_namespace_job_count,
        metadata_shard_count: status.metadata_shards.len(),
        repair_attempt_count: status.repair_attempts.len(),
    }
}

fn sifs_health_summary_from_rpc(
    health: sinomo_runtime::api::DaemonDfsBrowserHealthSummary,
) -> SifsHealthSummary {
    SifsHealthSummary {
        file_system_count: health.file_system_count,
        object_count: health.object_count,
        version_count: health.version_count,
        pending_write_count: health.pending_write_count,
        open_conflict_count: health.open_conflict_count,
        pending_repair_count: health.pending_repair_count,
        pending_namespace_job_count: health.pending_namespace_job_count,
        failed_namespace_job_count: health.failed_namespace_job_count,
        metadata_shard_count: health.metadata_shard_count,
        repair_attempt_count: health.repair_attempt_count,
    }
}

fn sifs_kind_label(kind: &sinomo_runtime::dfs::DfsObjectKind) -> &'static str {
    match kind {
        sinomo_runtime::dfs::DfsObjectKind::Directory => "directory",
        sinomo_runtime::dfs::DfsObjectKind::File => "file",
        sinomo_runtime::dfs::DfsObjectKind::Symlink => "symlink",
        sinomo_runtime::dfs::DfsObjectKind::Hardlink => "hardlink",
        sinomo_runtime::dfs::DfsObjectKind::Artifact => "artifact",
        sinomo_runtime::dfs::DfsObjectKind::Mount => "mount",
        sinomo_runtime::dfs::DfsObjectKind::Tombstone => "tombstone",
    }
}

fn sifs_policy_label(policy: &sinomo_runtime::core::DfsObjectPolicy) -> &'static str {
    match policy {
        sinomo_runtime::core::DfsObjectPolicy::LocalOwned => "local-owned",
        sinomo_runtime::core::DfsObjectPolicy::ReplicatedConfirmed => "replicated-confirmed",
        sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual => "replicated-eventual",
        sinomo_runtime::core::DfsObjectPolicy::AppendOnly => "append-only",
        sinomo_runtime::core::DfsObjectPolicy::EphemeralLocal => "ephemeral-local",
    }
}

fn sifs_pin_mode_label(mode: &sinomo_runtime::dfs::DfsPinMode) -> &'static str {
    match mode {
        sinomo_runtime::dfs::DfsPinMode::Metadata => "metadata",
        sinomo_runtime::dfs::DfsPinMode::Content => "content",
    }
}

fn sifs_agent_state_label(state: &sinomo_runtime::dfs::DfsAgentObjectState) -> &'static str {
    match state {
        sinomo_runtime::dfs::DfsAgentObjectState::Current => "current",
        sinomo_runtime::dfs::DfsAgentObjectState::PendingWrite => "pending-write",
        sinomo_runtime::dfs::DfsAgentObjectState::PendingRepair => "pending-repair",
        sinomo_runtime::dfs::DfsAgentObjectState::Conflicted => "conflicted",
        sinomo_runtime::dfs::DfsAgentObjectState::MetadataOnly => "metadata-only",
    }
}

fn sort_sifs_entries(mut entries: Vec<SifsEntry>) -> Vec<SifsEntry> {
    sort_sifs_entries_with(
        &mut entries,
        SifsListSortKey::Name,
        SifsListSortDirection::Ascending,
    );
    entries
}

fn page_sifs_entries(mut entries: Vec<SifsEntry>, options: SifsListOptions) -> SifsEntryPage {
    sort_sifs_entries_with(&mut entries, options.sort_key, options.sort_direction);
    let total_count = entries.len();
    let offset = options.offset.min(total_count);
    let entries = match options.limit {
        Some(limit) => entries.into_iter().skip(offset).take(limit).collect(),
        None => entries.into_iter().skip(offset).collect(),
    };
    SifsEntryPage {
        entries,
        total_count,
        offset,
        limit: options.limit,
    }
}

fn sort_sifs_entries_with(
    entries: &mut [SifsEntry],
    sort_key: SifsListSortKey,
    sort_direction: SifsListSortDirection,
) {
    entries.sort_by(|left, right| {
        let directory_order = sifs_entry_kind_rank(left).cmp(&sifs_entry_kind_rank(right));
        if directory_order != std::cmp::Ordering::Equal {
            return directory_order;
        }
        let ordering = match sort_key {
            SifsListSortKey::Name => compare_sifs_entry_names(left, right),
            SifsListSortKey::Kind => sifs_entry_kind_rank(left)
                .cmp(&sifs_entry_kind_rank(right))
                .then_with(|| compare_sifs_entry_names(left, right)),
            SifsListSortKey::Size => left
                .size
                .unwrap_or(0)
                .cmp(&right.size.unwrap_or(0))
                .then_with(|| compare_sifs_entry_names(left, right)),
        };
        match sort_direction {
            SifsListSortDirection::Ascending => ordering,
            SifsListSortDirection::Descending => ordering.reverse(),
        }
    });
}

fn sifs_entry_kind_rank(entry: &SifsEntry) -> u8 {
    if entry.is_dir {
        0
    } else if entry.is_link {
        1
    } else {
        2
    }
}

fn compare_sifs_entry_names(left: &SifsEntry, right: &SifsEntry) -> std::cmp::Ordering {
    left.name
        .to_ascii_lowercase()
        .cmp(&right.name.to_ascii_lowercase())
        .then_with(|| left.name.cmp(&right.name))
}

fn daemon_sort_key_from_sifs(
    sort_key: SifsListSortKey,
) -> sinomo_runtime::api::DaemonDfsBrowserSortKey {
    match sort_key {
        SifsListSortKey::Name => sinomo_runtime::api::DaemonDfsBrowserSortKey::Name,
        SifsListSortKey::Kind => sinomo_runtime::api::DaemonDfsBrowserSortKey::Kind,
        SifsListSortKey::Size => sinomo_runtime::api::DaemonDfsBrowserSortKey::Size,
    }
}

fn daemon_sort_direction_from_sifs(
    direction: SifsListSortDirection,
) -> sinomo_runtime::api::DaemonDfsBrowserSortDirection {
    match direction {
        SifsListSortDirection::Ascending => {
            sinomo_runtime::api::DaemonDfsBrowserSortDirection::Ascending
        }
        SifsListSortDirection::Descending => {
            sinomo_runtime::api::DaemonDfsBrowserSortDirection::Descending
        }
    }
}

fn sifs_error_is_not_found(error: &str) -> bool {
    error.contains("not found:")
}

fn normalize_sifs_browser_path(path: &str) -> Result<String, String> {
    let path = path.trim();
    let path = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if path.bytes().any(|byte| byte == 0) {
        return Err(format!("invalid SIFS path: {path}"));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn parent_sifs_browser_path(path: &str) -> Option<String> {
    let path = normalize_sifs_browser_path(path).ok()?;
    if path == "/" {
        return None;
    }
    let (parent, _) = path.rsplit_once('/')?;
    Some(if parent.is_empty() {
        "/".to_string()
    } else {
        parent.to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_agent::{
        AgentInteractionState, AgentLoopDecision, AgentLoopResponseMode, AgentRole, AgentStopReason,
    };
    use std::collections::VecDeque;

    fn chat_event_label(event: ChatStreamEvent) -> String {
        match event {
            ChatStreamEvent::Token(token) => format!("token:{token}"),
            ChatStreamEvent::AgentToken { speaker, text } => {
                format!("agent-token:{}:{text}", speaker.agent_key)
            }
            ChatStreamEvent::Notice(notice) => format!("notice:{notice}"),
            ChatStreamEvent::ChannelSystem { kind, text, .. } => {
                format!("system:{kind}:{text}")
            }
            ChatStreamEvent::Context(context) => {
                format!("context:{}:{}", context.step_index, context.total_bytes)
            }
            ChatStreamEvent::ToolCallStarted(call) => format!("tool-start:{}", call.tool),
            ChatStreamEvent::ToolCallDelta {
                arguments_delta, ..
            } => format!("tool-delta:{arguments_delta}"),
            ChatStreamEvent::ToolCallCompleted(call) => format!("tool-done:{}", call.tool),
            ChatStreamEvent::Process(process) => {
                format!("process:{}:{}", process.status, process.command)
            }
            ChatStreamEvent::Usage(usage) => format!("usage:{}/{}", usage.provider, usage.model),
        }
    }

    fn final_decision(text: &str) -> AgentLoopDecision {
        AgentLoopDecision {
            response_mode: AgentLoopResponseMode::Answer,
            text: text.to_string(),
            tool_calls: Vec::new(),
            background_handoff: None,
            next_state: AgentInteractionState::Idle,
        }
    }

    fn runtime_event(payload: AgentRuntimeEventPayload) -> AgentRuntimeEvent {
        AgentRuntimeEvent::new(SessionId::generate(), payload)
    }

    #[test]
    fn app_chat_text_attachment_projection_includes_bounded_excerpt() {
        let text = format!(
            "heading\n{}\nTAIL",
            "a".repeat(APP_CHAT_ATTACHMENT_TEXT_EXCERPT_BYTES + 512)
        );
        let projection =
            model_visible_attachment_projection("note.md", text.as_bytes(), Some("text/markdown"));

        assert_eq!(
            projection
                .get("projection")
                .and_then(serde_json::Value::as_str),
            Some("text_excerpt")
        );
        assert_eq!(
            projection
                .get("truncated")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        let excerpt = projection
            .get("text")
            .and_then(serde_json::Value::as_str)
            .expect("excerpt");
        assert!(excerpt.starts_with("heading"));
        assert!(!excerpt.contains("TAIL"));
    }

    #[test]
    fn app_chat_image_attachment_projection_reports_dimensions() {
        let mut png = Vec::new();
        png.extend_from_slice(b"\x89PNG\r\n\x1a\n");
        png.extend_from_slice(&13u32.to_be_bytes());
        png.extend_from_slice(b"IHDR");
        png.extend_from_slice(&640u32.to_be_bytes());
        png.extend_from_slice(&360u32.to_be_bytes());

        let projection = model_visible_attachment_projection("screen.png", &png, Some("image/png"));

        assert_eq!(
            projection
                .get("projection")
                .and_then(serde_json::Value::as_str),
            Some("image_metadata")
        );
        assert_eq!(projection["dimensions"]["width"].as_u64(), Some(640));
        assert_eq!(projection["dimensions"]["height"].as_u64(), Some(360));
        assert!(
            projection
                .get("note")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|note| note.contains("Pixel-level image reasoning"))
        );
    }

    #[test]
    fn app_chat_context_attachment_carries_model_visible_projection() {
        let attachment_id = TaskId::generate();
        let attachment = AgentChannelAttachment {
            attachment_id,
            file_name: "brief.txt".to_string(),
            original_path: "C:\\input\\brief.txt".to_string(),
            stored_path: "/services/sinomo-agent/channels/demo/attachments/brief.txt".to_string(),
            size_bytes: 5,
            content_type: Some("text/plain".to_string()),
            created_at: time::OffsetDateTime::now_utc(),
            source: AgentChannelSource {
                metadata: serde_json::json!({
                    "model_visible": {
                        "projection": "text_excerpt",
                        "text": "hello"
                    }
                }),
                ..AgentChannelSource::app()
            },
        };

        let context = agent_context_attachments_from_channel_attachments(&[attachment]);

        assert_eq!(context.len(), 1);
        assert_eq!(
            context[0].payload_json["model_visible"]["projection"].as_str(),
            Some("text_excerpt")
        );
        assert_eq!(
            context[0].payload_json["model_visible"]["text"].as_str(),
            Some("hello")
        );
    }

    #[test]
    fn app_api_ensure_single_node_cluster_initializes_missing_cluster() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-api-ensure-cluster");
        let data_dir = SinomoDataDir::new(&root);

        assert!(!cluster_status(&data_dir).clustered);

        let status = ensure_single_node_cluster(&data_dir, " App Node ").expect("ensure cluster");
        assert!(status.clustered);

        let state = read_cli_cluster_state(&ClusterPaths::new(&data_dir)).expect("cluster state");
        assert_eq!(state.cluster_name, "App Node");
        assert_eq!(state.local_node_display_name, "App Node");
        assert_eq!(status.cluster_id, Some(state.cluster_id.to_string()));
        assert_eq!(status.local_node_id, Some(state.local_node_id.to_string()));
    }

    #[test]
    fn app_api_ensure_single_node_cluster_is_idempotent() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-api-ensure-existing-cluster");
        let data_dir = SinomoDataDir::new(&root);

        let first =
            ensure_single_node_cluster(&data_dir, "First App").expect("first ensure cluster");
        let second =
            ensure_single_node_cluster(&data_dir, "Second App").expect("second ensure cluster");

        let state = read_cli_cluster_state(&ClusterPaths::new(&data_dir)).expect("cluster state");
        assert_eq!(second.cluster_id, first.cluster_id);
        assert_eq!(second.local_node_id, first.local_node_id);
        assert_eq!(state.cluster_name, "First App");
        assert_eq!(state.local_node_display_name, "First App");
    }

    #[test]
    fn chat_mapper_emits_final_decision_without_streaming_delta() {
        let mut stream_state = ChatStreamState::default();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));

        let state = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                sequence: 1,
                op_id: TaskId::generate(),
                decision: final_decision("ready"),
                stop_reason: AgentStopReason::FinalDecision,
            }),
            &mut stream_state,
            &mut on_event,
        );

        assert!(matches!(state, RuntimeDrainState::Running));
        assert_eq!(events, vec!["token:ready"]);
        assert_eq!(stream_state.visible_text, "ready");
    }

    #[test]
    fn chat_mapper_appends_only_final_suffix_after_streaming_delta() {
        let mut stream_state = ChatStreamState::default();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));
        let op_id = TaskId::generate();

        let first = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::ModelProgress {
                sequence: 1,
                op_id: op_id.clone(),
                event: AgentModelEvent::AssistantTextDelta {
                    step_index: 0,
                    delta: "hello".to_string(),
                },
            }),
            &mut stream_state,
            &mut on_event,
        );
        let final_state = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                sequence: 2,
                op_id,
                decision: final_decision("hello world"),
                stop_reason: AgentStopReason::FinalDecision,
            }),
            &mut stream_state,
            &mut on_event,
        );

        assert!(matches!(first, RuntimeDrainState::Running));
        assert!(matches!(final_state, RuntimeDrainState::Running));
        assert_eq!(events, vec!["token:hello", "token: world"]);
        assert_eq!(stream_state.visible_text, "hello world");
    }

    #[test]
    fn chat_mapper_streams_structured_agent_json_text_delta() {
        let mut stream_state = ChatStreamState::default();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));
        let op_id = TaskId::generate();

        let first = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::ModelProgress {
                sequence: 1,
                op_id: op_id.clone(),
                event: AgentModelEvent::AssistantTextDelta {
                    step_index: 0,
                    delta: serde_json::json!({
                        "response_mode": "answer",
                        "text": "ready",
                        "tool_calls": [],
                        "background_handoff": null,
                        "next_state": "listening"
                    })
                    .to_string(),
                },
            }),
            &mut stream_state,
            &mut on_event,
        );
        let final_state = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                sequence: 2,
                op_id,
                decision: final_decision("ready"),
                stop_reason: AgentStopReason::FinalDecision,
            }),
            &mut stream_state,
            &mut on_event,
        );

        assert!(matches!(first, RuntimeDrainState::Running));
        assert!(matches!(final_state, RuntimeDrainState::Running));
        assert_eq!(events, vec!["token:ready"]);
        assert_eq!(stream_state.visible_text, "ready");
    }

    #[test]
    fn chat_mapper_streams_partial_structured_agent_json_text_delta() {
        let mut stream_state = ChatStreamState::default();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));
        let op_id = TaskId::generate();

        let first = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::ModelProgress {
                sequence: 1,
                op_id: op_id.clone(),
                event: AgentModelEvent::AssistantTextDelta {
                    step_index: 0,
                    delta: "{\n".to_string(),
                },
            }),
            &mut stream_state,
            &mut on_event,
        );
        let second = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::ModelProgress {
                sequence: 2,
                op_id: op_id.clone(),
                event: AgentModelEvent::AssistantTextDelta {
                    step_index: 0,
                    delta: r#""response_mode":"answer","text":"ready","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#.to_string(),
                },
            }),
            &mut stream_state,
            &mut on_event,
        );
        let final_state = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                sequence: 3,
                op_id,
                decision: final_decision("ready"),
                stop_reason: AgentStopReason::FinalDecision,
            }),
            &mut stream_state,
            &mut on_event,
        );

        assert!(matches!(first, RuntimeDrainState::Running));
        assert!(matches!(second, RuntimeDrainState::Running));
        assert!(matches!(final_state, RuntimeDrainState::Running));
        assert_eq!(events, vec!["token:ready"]);
        assert_eq!(stream_state.visible_text, "ready");
    }

    #[test]
    fn chat_mapper_streams_long_structured_agent_json_text_increments() {
        let mut stream_state = ChatStreamState::default();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));
        let op_id = TaskId::generate();

        for (sequence, delta) in [
            (
                1,
                r##"{"response_mode":"answer","text":"# Draft\n"##.to_string(),
            ),
            (2, "Body paragraph".to_string()),
            (
                3,
                r#"\nDone","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#
                    .to_string(),
            ),
        ] {
            runtime_event_to_chat(
                runtime_event(AgentRuntimeEventPayload::ModelProgress {
                    sequence,
                    op_id: op_id.clone(),
                    event: AgentModelEvent::AssistantTextDelta {
                        step_index: 0,
                        delta,
                    },
                }),
                &mut stream_state,
                &mut on_event,
            );
        }
        let final_state = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                sequence: 4,
                op_id,
                decision: final_decision("# Draft\nBody paragraph\nDone"),
                stop_reason: AgentStopReason::FinalDecision,
            }),
            &mut stream_state,
            &mut on_event,
        );

        assert!(matches!(final_state, RuntimeDrainState::Running));
        assert_eq!(
            events,
            vec!["token:# Draft\n", "token:Body paragraph", "token:\nDone"]
        );
        assert_eq!(stream_state.visible_text, "# Draft\nBody paragraph\nDone");
    }

    #[test]
    fn progress_lines_are_forwarded_as_notices() {
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));

        forward_output(
            InteractionOutput::Line(
                "<sinomo_progress>waiting for configured model runtime</sinomo_progress>"
                    .to_string(),
            ),
            &mut on_event,
        );

        assert_eq!(events, vec!["notice:waiting for configured model runtime"]);
    }

    #[test]
    fn progress_model_deltas_are_forwarded_as_notices() {
        let mut stream_state = ChatStreamState::default();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));

        forward_model_event(
            AgentModelEvent::AssistantTextDelta {
                step_index: 0,
                delta: "<sinomo_progress>waiting for configured model runtime</sinomo_progress>"
                    .to_string(),
            },
            &mut stream_state,
            &mut on_event,
        );

        assert_eq!(events, vec!["notice:waiting for configured model runtime"]);
        assert!(stream_state.visible_text.is_empty());
    }

    #[test]
    fn app_api_runtime_drain_times_out_after_worker_progress_without_terminal_event() {
        let op_id = TaskId::generate();
        let mut batches = VecDeque::from([
            vec![runtime_event(AgentRuntimeEventPayload::WorkerStarted {
                sequence: 1,
                op_id,
                action: "start_turn".to_string(),
                timeout_ms: None,
            })],
            Vec::new(),
        ]);
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));

        let error = drain_runtime_events_with_limits(
            || Ok(batches.pop_front().unwrap_or_default()),
            &mut on_event,
            Duration::from_secs(1),
            Duration::ZERO,
            Duration::ZERO,
        )
        .expect_err("missing terminal event should time out");

        assert!(
            error
                .to_string()
                .contains("did not produce a terminal event"),
            "timeout error should explain the stalled runtime: {error}"
        );
        assert_eq!(events, vec!["notice:agent task started: start_turn"]);
    }

    #[test]
    fn app_chat_resume_explicit_channel_preserves_participant_session_id() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-resume");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Team channel");
        channel.ensure_user(AgentChannelUser::app_local());
        let participant_id = channel.ensure_participant("main-agent", "main-agent");
        let participant_session_id = channel
            .participant(&participant_id)
            .expect("participant")
            .agent_session_id
            .clone();
        channel
            .participant_mut(&participant_id)
            .expect("participant")
            .tool_runtime
            .sandbox_node_id = Some("channel-node".to_string());
        assert_ne!(participant_session_id, channel_id);
        store.save_channel(&channel).expect("save channel");

        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume explicit channel");

        let participant = chat
            .participants()
            .into_iter()
            .find(|participant| participant.agent_key == "main-agent")
            .expect("main participant");
        assert_eq!(
            participant.agent_session_id,
            participant_session_id.to_string()
        );
        assert_eq!(
            participant.tool_runtime.sandbox_node_id.as_deref(),
            Some("channel-node")
        );
    }

    #[test]
    fn app_chat_new_chat_uses_agent_tool_runtime_default() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-agent-tool-runtime");
        let data_dir = SinomoDataDir::new(&root);
        let definition =
            AgentDefinition::custom("worker-agent", AgentRole::Main, "Use the worker sandbox.")
                .with_sandbox_runtime_node_id("worker-node");
        let definition_store = sinomo_agent::AgentDefinitionSifsStore::open(Some(data_dir.root()))
            .expect("open agent definition store");
        sinomo_agent::AgentDefinitionStore::save_definition(&definition_store, &definition)
            .expect("save agent definition");

        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        chat.set_agent("worker-agent").expect("set agent");

        assert_eq!(
            chat.tool_runtime().sandbox_node_id.as_deref(),
            Some("worker-node")
        );
        let participant = chat
            .participants()
            .into_iter()
            .find(|participant| participant.agent_key == "worker-agent")
            .expect("worker participant");
        assert_eq!(
            participant.tool_runtime.sandbox_node_id.as_deref(),
            Some("worker-node")
        );
    }

    #[test]
    fn app_chat_channel_set_agent_stamps_agent_tool_runtime_default() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-agent-tool-runtime");
        let data_dir = SinomoDataDir::new(&root);
        let definition =
            AgentDefinition::custom("worker-agent", AgentRole::Main, "Use the worker sandbox.")
                .with_sandbox_runtime_node_id("worker-node");
        let definition_store = sinomo_agent::AgentDefinitionSifsStore::open(Some(data_dir.root()))
            .expect("open agent definition store");
        sinomo_agent::AgentDefinitionStore::save_definition(&definition_store, &definition)
            .expect("save agent definition");

        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Team channel");
        channel.ensure_user(AgentChannelUser::app_local());
        store.save_channel(&channel).expect("save channel");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume channel");
        chat.set_agent("worker-agent").expect("set channel agent");

        let saved = SifsAgentChannelStore::open(&data_dir)
            .expect("reopen channel store")
            .load_channel(&channel_id)
            .expect("load channel")
            .expect("channel saved");
        let participant = saved
            .enabled_participants_in_order()
            .into_iter()
            .find(|participant| participant.agent_key == "worker-agent")
            .expect("worker participant");
        assert_eq!(
            participant.tool_runtime.sandbox_node_id.as_deref(),
            Some("worker-node")
        );
    }

    #[test]
    fn sifs_channel_summaries_track_snapshot_metadata_without_index_cache() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-summaries");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Team channel");
        channel.ensure_user(AgentChannelUser::app_local());
        channel.ensure_participant("main-agent", "main-agent");
        store.save_channel(&channel).expect("save channel");

        let summaries = store.list_channel_summaries().expect("list summaries");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].channel_id, channel_id);
        assert_eq!(summaries[0].title, "Team channel");
        assert_eq!(summaries[0].channel_kind, AgentChannelKind::Channel);
        assert_eq!(summaries[0].enabled_participant_count, 1);

        channel.rename("Release triage");
        store.save_channel(&channel).expect("save renamed channel");
        let summaries = store
            .list_channel_summaries()
            .expect("list renamed summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].title, "Release triage");

        channel.tombstone();
        store
            .save_channel(&channel)
            .expect("save tombstoned channel");
        assert!(
            store
                .list_channel_summaries()
                .expect("list tombstoned summaries")
                .is_empty(),
            "deleted channels should remain in SIFS but stay hidden from summaries"
        );
        assert!(
            SifsBrowser::open(&data_dir)
                .expect("open SIFS")
                .read_file("/services/sinomo-agent/channel-index.json")
                .is_err(),
            "channel summaries should not create a shared mutable index cache"
        );
    }

    #[test]
    fn sifs_channel_summaries_skip_channel_dirs_without_snapshots() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-missing-snapshot");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        SifsBrowser::open(&data_dir)
            .expect("open SIFS")
            .create_dir_all(&SifsAgentChannelStore::channel_dir(&channel_id))
            .expect("create partial channel dir");

        let summaries = store.list_channel_summaries().expect("list summaries");

        assert!(
            summaries.is_empty(),
            "partial channel directories without snapshots should be ignored"
        );
    }

    #[test]
    fn sifs_channel_messages_are_chunked_and_recent_snapshot_is_bounded() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-chunked-log");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Long running channel");
        channel.ensure_user(AgentChannelUser::app_local());

        let total = AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT + 25;
        for index in 0..total {
            let message = channel.append_user_message(
                AGENT_CHANNEL_APP_USER_ID,
                "User",
                format!("message {index}"),
                AgentChannelSource::app(),
            );
            store
                .append_message(&channel.channel_id, &message)
                .expect("append message");
        }
        let turn = AgentChannelTurn {
            turn_id: TaskId::generate(),
            channel_id: channel.channel_id.clone(),
            trigger_message_id: channel
                .transcript
                .last()
                .expect("recent message")
                .message_id
                .clone(),
            decisions: Vec::new(),
            response_message_ids: Vec::new(),
            system_message_ids: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        };
        store
            .append_turn(&channel.channel_id, &turn)
            .expect("append turn");
        store.save_channel(&channel).expect("save channel");

        let snapshot_bytes = SifsBrowser::open(&data_dir)
            .expect("open SIFS")
            .read_file(&SifsAgentChannelStore::snapshot_path(&channel_id))
            .expect("snapshot");
        let snapshot = serde_json::from_slice::<OwnedSifsAgentChannelSnapshot>(&snapshot_bytes)
            .expect("snapshot json");
        assert_eq!(
            snapshot.channel.transcript.len(),
            AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT
        );

        let first_chunk = SifsBrowser::open(&data_dir)
            .expect("open SIFS")
            .read_file(&SifsAgentChannelStore::message_chunk_path(&channel_id, 1))
            .expect("first chunk");
        assert!(
            String::from_utf8_lossy(&first_chunk).contains("message 0"),
            "first chunk should retain the beginning of the durable timeline"
        );
        let message_index = SifsBrowser::open(&data_dir)
            .expect("open SIFS")
            .read_file(&SifsAgentChannelStore::message_index_path(&channel_id))
            .expect("message index");
        let message_index = serde_json::from_slice::<SifsChunkedLogIndex>(&message_index)
            .expect("message index json");
        assert_eq!(message_index.entry_count, total as u64);

        let turn_index = SifsBrowser::open(&data_dir)
            .expect("open SIFS")
            .read_file(&SifsAgentChannelStore::turn_index_path(&channel_id))
            .expect("turn index");
        let turn_index =
            serde_json::from_slice::<SifsChunkedLogIndex>(&turn_index).expect("turn index json");
        assert_eq!(turn_index.entry_count, 1);

        let loaded = store
            .load_channel(&channel_id)
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
    fn app_chat_send_surfaces_missing_codex_auth() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-codex-auth");
        let data_dir = SinomoDataDir::new(&root);
        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        chat.set_model(
            sinomo_lm::CODEX_PROVIDER_ID,
            "gpt-5.2-codex",
            sinomo_lm::DEFAULT_PROVIDER_ACCOUNT,
        )
        .expect("switch to codex");

        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));
        chat.send("hello", &mut on_event)
            .expect("missing Codex auth is emitted as a channel system error");
        let error = events
            .iter()
            .find(|event| event.starts_with("system:error:"))
            .cloned()
            .expect("missing Codex auth should emit a system error");

        assert!(
            error.contains("authentication required for codex"),
            "missing auth should be surfaced directly: {error}"
        );
        assert!(
            error.contains("sinomo providers auth codex"),
            "error should include the operator auth command: {error}"
        );
    }

    #[test]
    fn app_chat_send_to_explicit_channel_uses_channel_runtime() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-runtime");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Team channel");
        channel.ensure_user(AgentChannelUser::app_local());
        channel.ensure_participant("main-agent", "main-agent");
        store.save_channel(&channel).expect("save channel");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume channel");
        chat.set_model(
            sinomo_lm::CODEX_PROVIDER_ID,
            "gpt-5.2-codex",
            sinomo_lm::DEFAULT_PROVIDER_ACCOUNT,
        )
        .expect("switch to codex");

        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));
        chat.send("hello channel", &mut on_event)
            .expect("channel runtime converts provider failure into a channel system event");

        assert!(
            events.iter().any(|event| {
                event.starts_with("system:error:")
                    && event.contains("authentication required for codex")
            }),
            "channel runtime should surface participant failures as channel system events: {events:?}"
        );
        let saved = SifsAgentChannelStore::open(&data_dir)
            .expect("reopen channel store")
            .load_channel(&channel_id)
            .expect("load channel")
            .expect("channel saved");
        assert!(saved.transcript.iter().any(|message| {
            matches!(message.author, AgentChannelAuthor::User { .. })
                && message.content == "hello channel"
        }));
        assert!(saved.transcript.iter().any(|message| {
            message.message_kind == AgentChannelMessageKind::Error
                && message
                    .content
                    .contains("authentication required for codex")
        }));
    }
}
