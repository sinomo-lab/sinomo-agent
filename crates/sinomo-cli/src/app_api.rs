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
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
use sinomo_agent::AgentChannelUser;
use sinomo_agent::{
    AGENT_CHANNEL_APP_USER_ID, AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT,
    AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION, AGENT_SESSION_SNAPSHOT_SCHEMA_VERSION, AgentCatalog,
    AgentChannel, AgentChannelAttachment, AgentChannelAuthor, AgentChannelExecutorError,
    AgentChannelKind, AgentChannelLoopConfig, AgentChannelMessage, AgentChannelMessageKind,
    AgentChannelParticipant, AgentChannelRuntime, AgentChannelRuntimeEvent, AgentChannelSource,
    AgentChannelSpeakDecision, AgentChannelStore, AgentChannelStoreError, AgentChannelTurn,
    AgentChannelTurnExecutor, AgentContextAttachment, AgentDefinition, AgentEvent,
    AgentEventPayload, AgentItem, AgentModelEvent, AgentRuntimeControlHost,
    AgentRuntimeControlRegistry, AgentRuntimeEvent, AgentRuntimeEventPayload, AgentSession,
    AgentSessionControlResult, AgentSessionFollowUpRequest, AgentSessionInterruptRequest,
    AgentSessionReattachState, AgentSessionRemoveQueuedInputRequest, AgentSessionResumeRequest,
    AgentSessionResumeResponse, AgentSessionSnapshot, AgentSessionSteerRequest, AgentSessionStore,
    AgentTaskLifecycleStatus, AgentTaskSnapshot, AgentToolProgressEvent, ContextAssemblyReport,
    FileAgentSessionStore, InteractionOutput, InteractionSession, InteractionSurface,
    InteractionSurfaceKind, SkillActivationScope, SkillCatalogEntry, SkillCatalogSifsStore,
    SkillCatalogSource, SkillCatalogStore, SkillCatalogWrite, TaskId,
    channel_message_agent_item_id, mark_participant_message_observed, render_common_skill_markdown,
    safe_agent_channel_attachment_file_name, sync_participant_session,
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
use std::collections::{BTreeMap, BTreeSet};
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
const APP_CHAT_ATTACHMENT_TEXT_EXCERPT_BYTES: usize = 24 * 1024;
pub const SIFS_AGENT_CHANNELS_ROOT: &str = "/services/sinomo-agent/channels";
const SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE: u64 = 128;
const SIFS_AGENT_CHANNEL_METADATA_SCHEMA_VERSION: u32 = 1;
const SIFS_AGENT_SESSION_CHECKPOINT_SCHEMA_VERSION: u32 = 1;

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

/// Sequence-bounded channel timeline page for app transcript rendering.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelMessagePage {
    pub channel_id: SessionId,
    pub messages: Vec<AgentChannelMessage>,
    pub start_sequence: Option<u64>,
    pub end_sequence: Option<u64>,
    pub has_older: bool,
    pub has_newer: bool,
    pub total_messages: u64,
    pub chunk_size: u64,
}

impl AgentChannelMessagePage {
    fn empty(channel_id: &SessionId, total_messages: u64) -> Self {
        Self {
            channel_id: channel_id.clone(),
            messages: Vec::new(),
            start_sequence: None,
            end_sequence: None,
            has_older: false,
            has_newer: false,
            total_messages,
            chunk_size: SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE,
        }
    }
}

/// A bounded timeline update plus the immutable record IDs covered by its scan.
///
/// The IDs are a discovery cursor rather than an ordering cursor. Remembering
/// the set lets a caller find a record that replicates late even when its
/// timestamp places it outside the current presentation tail.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelMessageReconciliation {
    pub page: AgentChannelMessagePage,
    pub discovered_message_ids: Vec<String>,
}

/// Coarse chunk-level timeline metadata for long-history navigation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelTimelineOverview {
    pub channel_id: SessionId,
    pub total_messages: u64,
    pub oldest_sequence: Option<u64>,
    pub newest_sequence: Option<u64>,
    pub chunk_size: u64,
    pub segments: Vec<AgentChannelTimelineSegment>,
}

/// A chunk-level timeline segment suitable for overview rails and jump lists.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentChannelTimelineSegment {
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub message_count: u64,
    pub first_created_unix: Option<i64>,
    pub last_created_unix: Option<i64>,
    pub preview: Option<String>,
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
    /// Device-local directory used for this chat's tools. The canonical copy
    /// is persisted in `AgentSession.workspace_binding` so resume is safe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<PathBuf>,
}

impl ChatToolRuntimeSelection {
    fn from_agent_definition(definition: &AgentDefinition) -> Self {
        Self {
            sandbox_node_id: definition.tool_runtime.sandbox_node_id.clone(),
            workspace_root: None,
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

/// The source snapshot selected by the GUI for upload into the chat/channel attachment store.
///
/// Picker/drop files stay path-backed until the chat worker accepts the turn. Files already read
/// by the Files viewer carry their immutable bytes directly so the app never has to reopen a
/// mutable source or make a temporary staging file an accidental source of authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChatAttachmentSource {
    LocalPath(PathBuf),
    LocalSnapshot {
        provenance: ChatLocalAttachmentProvenance,
        bytes: Arc<[u8]>,
    },
    SifsSnapshot {
        provenance: ChatSifsAttachmentProvenance,
        bytes: Arc<[u8]>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChatLocalAttachmentProvenance {
    pub original_path: String,
    pub canonical_path: Option<String>,
    pub modified_at_unix_nanos: Option<u64>,
    pub unsaved_editor_buffer: bool,
}

impl ChatLocalAttachmentProvenance {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            original_path: path.into(),
            canonical_path: None,
            modified_at_unix_nanos: None,
            unsaved_editor_buffer: false,
        }
    }

    pub fn source_path(&self) -> &str {
        self.canonical_path
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .unwrap_or(&self.original_path)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChatSifsAttachmentProvenance {
    pub path: String,
    pub object_id: Option<String>,
    pub version: Option<String>,
    pub content_hash: Option<String>,
    pub policy: Option<String>,
    pub unsaved_editor_buffer: bool,
}

impl ChatSifsAttachmentProvenance {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            object_id: None,
            version: None,
            content_hash: None,
            policy: None,
            unsaved_editor_buffer: false,
        }
    }
}

/// A file selected by the GUI for upload into the chat/channel attachment store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatAttachmentInput {
    pub file_name: String,
    pub source: ChatAttachmentSource,
}

impl ChatAttachmentInput {
    pub fn local(path: PathBuf) -> Self {
        let file_name = attachment_file_name(&path);
        Self {
            file_name,
            source: ChatAttachmentSource::LocalPath(path),
        }
    }

    pub fn sifs_snapshot(
        file_name: impl Into<String>,
        path: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            file_name: file_name.into(),
            source: ChatAttachmentSource::SifsSnapshot {
                provenance: ChatSifsAttachmentProvenance::new(path),
                bytes: bytes.into(),
            },
        }
    }

    pub fn local_snapshot(
        file_name: impl Into<String>,
        path: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            file_name: file_name.into(),
            source: ChatAttachmentSource::LocalSnapshot {
                provenance: ChatLocalAttachmentProvenance::new(path),
                bytes: bytes.into(),
            },
        }
    }

    pub fn local_snapshot_with_provenance(
        file_name: impl Into<String>,
        provenance: ChatLocalAttachmentProvenance,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            file_name: file_name.into(),
            source: ChatAttachmentSource::LocalSnapshot {
                provenance,
                bytes: bytes.into(),
            },
        }
    }

    pub fn sifs_snapshot_with_provenance(
        file_name: impl Into<String>,
        provenance: ChatSifsAttachmentProvenance,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            file_name: file_name.into(),
            source: ChatAttachmentSource::SifsSnapshot {
                provenance,
                bytes: bytes.into(),
            },
        }
    }
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
    /// The steering/follow-up message this control call queued, if any —
    /// carries the queue id the UI needs to offer removal.
    pub queued_input: Option<ChatQueuedTurnInput>,
    /// The queue id this control call removed, if any.
    pub removed_input_id: Option<String>,
}

/// Which session queue a pending turn input rides in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatQueuedTurnInputKind {
    /// Consumed by the next model step of the running turn.
    Steering,
    /// Runs as a fresh user turn once the active one completes.
    FollowUp,
}

/// A steering/follow-up message sitting in a session queue, not yet consumed.
/// Queued items are visible and removable in the composer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatQueuedTurnInput {
    pub input_id: String,
    pub kind: ChatQueuedTurnInputKind,
    pub message: String,
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
    /// Authoritative queue contents (ids + texts) from the session snapshot,
    /// so clients can render removable pending chips.
    pub queued_inputs: Vec<ChatQueuedTurnInput>,
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
    #[cfg(test)]
    message_record_body_reads: std::sync::atomic::AtomicUsize,
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
        Self {
            browser,
            #[cfg(test)]
            message_record_body_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn channel_dir(channel_id: &SessionId) -> String {
        format!("{}/{}", SIFS_AGENT_CHANNELS_ROOT, channel_id.as_str())
    }

    fn snapshot_path(channel_id: &SessionId) -> String {
        format!("{}/snapshot.json", Self::channel_dir(channel_id))
    }

    fn metadata_records_dir(channel_id: &SessionId) -> String {
        format!("{}/metadata/records", Self::channel_dir(channel_id))
    }

    fn metadata_record_path(channel_id: &SessionId, record_id: &str) -> String {
        format!(
            "{}/{}.json",
            Self::metadata_records_dir(channel_id),
            record_id
        )
    }

    fn message_chunks_dir(channel_id: &SessionId) -> String {
        format!("{}/messages", Self::channel_dir(channel_id))
    }

    #[cfg(test)]
    fn message_index_path(channel_id: &SessionId) -> String {
        format!("{}/index.json", Self::message_chunks_dir(channel_id))
    }

    #[cfg(test)]
    fn message_chunk_path(channel_id: &SessionId, sequence: u64) -> String {
        let chunk = sequence.saturating_sub(1) / SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE;
        Self::message_chunk_path_by_index(channel_id, chunk)
    }

    #[cfg(test)]
    fn message_chunk_path_by_index(channel_id: &SessionId, chunk: u64) -> String {
        format!("{}/{chunk:08}.jsonl", Self::message_chunks_dir(channel_id))
    }

    fn message_records_dir(channel_id: &SessionId) -> String {
        format!("{}/records", Self::message_chunks_dir(channel_id))
    }

    fn message_record_path(channel_id: &SessionId, message_id: &TaskId) -> String {
        format!(
            "{}/{}.json",
            Self::message_records_dir(channel_id),
            message_id.as_str()
        )
    }

    fn turn_chunks_dir(channel_id: &SessionId) -> String {
        format!("{}/turns", Self::channel_dir(channel_id))
    }

    #[cfg(test)]
    fn turn_index_path(channel_id: &SessionId) -> String {
        format!("{}/index.json", Self::turn_chunks_dir(channel_id))
    }

    #[cfg(test)]
    fn turn_chunk_path(channel_id: &SessionId, sequence: u64) -> String {
        let chunk = sequence / SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE;
        format!("{}/{chunk:08}.jsonl", Self::turn_chunks_dir(channel_id))
    }

    fn turn_records_dir(channel_id: &SessionId) -> String {
        format!("{}/records", Self::turn_chunks_dir(channel_id))
    }

    fn turn_record_path(channel_id: &SessionId, turn_id: &TaskId) -> String {
        format!(
            "{}/{}.json",
            Self::turn_records_dir(channel_id),
            turn_id.as_str()
        )
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

    fn write_immutable_json<T: Serialize>(
        &self,
        path: &str,
        value: &T,
    ) -> Result<(), AgentChannelStoreError> {
        let bytes = serde_json::to_vec_pretty(value).map_err(|source| {
            AgentChannelStoreError::Serialization {
                detail: source.to_string(),
            }
        })?;
        match self.browser.read_file(path) {
            Ok(existing) if existing == bytes => return Ok(()),
            Ok(_) => {
                return Err(AgentChannelStoreError::Io {
                    path: path.to_string(),
                    detail: "immutable channel record already exists with different content"
                        .to_string(),
                });
            }
            Err(detail) if sifs_error_is_not_found(&detail) => {}
            Err(detail) => return Err(channel_store_io(path, detail)),
        }
        self.write_file(path, &bytes)
    }

    fn append_message_record(
        &self,
        channel_id: &SessionId,
        message: &AgentChannelMessage,
    ) -> Result<(), AgentChannelStoreError> {
        self.write_immutable_json(
            &Self::message_record_path(channel_id, &message.message_id),
            message,
        )
    }

    fn append_turn_record(
        &self,
        channel_id: &SessionId,
        turn: &AgentChannelTurn,
    ) -> Result<(), AgentChannelStoreError> {
        self.write_immutable_json(&Self::turn_record_path(channel_id, &turn.turn_id), turn)
    }

    fn append_metadata_record(
        &self,
        channel: &AgentChannel,
    ) -> Result<SifsAgentChannelMetadataRecord, AgentChannelStoreError> {
        let mut channel = channel.clone();
        // Message bodies have their own immutable records. Keeping them out of
        // metadata operations makes a participant/title update independent of
        // transcript size while preserving `next_sequence` and observations.
        channel.transcript.clear();
        let record_id = blake3::hash(&canonical_json_bytes(&channel)?)
            .to_hex()
            .to_string();
        let record = SifsAgentChannelMetadataRecord {
            schema_version: SIFS_AGENT_CHANNEL_METADATA_SCHEMA_VERSION,
            record_id: record_id.clone(),
            channel,
        };
        self.write_immutable_json(
            &Self::metadata_record_path(&record.channel.channel_id, &record_id),
            &record,
        )?;
        Ok(record)
    }

    fn paths_in_dir_with_suffix(
        &self,
        dir: &str,
        suffix: &str,
    ) -> Result<Vec<String>, AgentChannelStoreError> {
        let mut paths = self
            .browser
            .list_if_exists(dir)
            .map_err(|detail| channel_store_io(dir, detail))?
            .into_iter()
            .filter(|entry| !entry.is_dir && entry.name.ends_with(suffix))
            .map(|entry| entry.path)
            .collect::<Vec<_>>();
        paths.sort();
        Ok(paths)
    }

    fn load_json_documents<T: DeserializeOwned>(
        &self,
        paths: &[String],
    ) -> Result<Vec<T>, AgentChannelStoreError> {
        Ok(self
            .load_json_documents_with_paths(paths)?
            .into_iter()
            .map(|(_, value)| value)
            .collect())
    }

    fn load_json_documents_with_paths<T: DeserializeOwned>(
        &self,
        paths: &[String],
    ) -> Result<Vec<(String, T)>, AgentChannelStoreError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut values = Vec::with_capacity(paths.len());
        for (path, result) in self
            .browser
            .read_files(paths)
            .map_err(|detail| channel_store_io(SIFS_AGENT_CHANNELS_ROOT, detail))?
        {
            let bytes = match result {
                Ok(bytes) => bytes,
                Err(detail) if sifs_error_is_not_found(&detail) => continue,
                Err(detail) => return Err(channel_store_io(&path, detail)),
            };
            let value = serde_json::from_slice(&bytes).map_err(|source| {
                AgentChannelStoreError::Serialization {
                    detail: format!("invalid channel record at {path}: {source}"),
                }
            })?;
            values.push((path, value));
        }
        Ok(values)
    }

    fn load_message_record_documents(
        &self,
        paths: &[String],
    ) -> Result<Vec<(String, AgentChannelMessage)>, AgentChannelStoreError> {
        #[cfg(test)]
        self.message_record_body_reads
            .fetch_add(paths.len(), std::sync::atomic::Ordering::Relaxed);
        self.load_json_documents_with_paths(paths)
    }

    #[cfg(test)]
    fn message_record_body_read_count(&self) -> usize {
        self.message_record_body_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn load_legacy_json_lines<T: DeserializeOwned>(
        &self,
        dir: &str,
    ) -> Result<Vec<T>, AgentChannelStoreError> {
        let paths = self.paths_in_dir_with_suffix(dir, ".jsonl")?;
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        for (path, result) in self
            .browser
            .read_files(&paths)
            .map_err(|detail| channel_store_io(dir, detail))?
        {
            let bytes = match result {
                Ok(bytes) => bytes,
                Err(detail) if sifs_error_is_not_found(&detail) => continue,
                Err(detail) => return Err(channel_store_io(&path, detail)),
            };
            values.extend(parse_json_lines::<T>(&bytes)?);
        }
        Ok(values)
    }

    fn load_all_messages(
        &self,
        channel_id: &SessionId,
    ) -> Result<Vec<AgentChannelMessage>, AgentChannelStoreError> {
        // Legacy indexes were mutable caches and could lag concurrent writes.
        // Enumerate the actual JSONL chunks and immutable records instead.
        let mut messages = self.load_legacy_json_lines(&Self::message_chunks_dir(channel_id))?;
        let record_paths =
            self.paths_in_dir_with_suffix(&Self::message_records_dir(channel_id), ".json")?;
        messages.extend(
            self.load_message_record_documents(&record_paths)?
                .into_iter()
                .map(|(_, message)| message),
        );
        canonicalize_channel_messages(messages)
    }

    pub fn load_turns(
        &self,
        channel_id: &SessionId,
    ) -> Result<Vec<AgentChannelTurn>, AgentChannelStoreError> {
        let mut turns = self.load_legacy_json_lines(&Self::turn_chunks_dir(channel_id))?;
        let record_paths =
            self.paths_in_dir_with_suffix(&Self::turn_records_dir(channel_id), ".json")?;
        turns.extend(self.load_json_documents(&record_paths)?);
        canonicalize_channel_turns(turns)
    }

    fn message_page_from_records(
        channel_id: &SessionId,
        messages: Vec<AgentChannelMessage>,
        start_sequence: u64,
        end_sequence: u64,
        limit: usize,
    ) -> AgentChannelMessagePage {
        let total_messages = messages.len() as u64;
        if limit == 0 || total_messages == 0 {
            return AgentChannelMessagePage::empty(channel_id, total_messages);
        }
        let start_sequence = start_sequence.max(1).min(total_messages);
        let end_sequence = end_sequence.max(start_sequence).min(total_messages);
        let messages = messages
            .into_iter()
            .filter(|message| {
                message.sequence >= start_sequence && message.sequence <= end_sequence
            })
            .take(limit)
            .collect::<Vec<_>>();
        let start_sequence = messages.first().map(|message| message.sequence);
        let end_sequence = messages.last().map(|message| message.sequence);
        AgentChannelMessagePage {
            channel_id: channel_id.clone(),
            messages,
            start_sequence,
            end_sequence,
            has_older: start_sequence.is_some_and(|sequence| sequence > 1),
            has_newer: end_sequence.is_some_and(|sequence| sequence < total_messages),
            total_messages,
            chunk_size: SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE,
        }
    }

    fn empty_message_page_after_end(
        channel_id: &SessionId,
        total_messages: u64,
    ) -> AgentChannelMessagePage {
        let mut page = AgentChannelMessagePage::empty(channel_id, total_messages);
        page.has_older = total_messages > 0;
        page
    }

    fn empty_message_page_before_start(
        channel_id: &SessionId,
        total_messages: u64,
    ) -> AgentChannelMessagePage {
        let mut page = AgentChannelMessagePage::empty(channel_id, total_messages);
        page.has_newer = total_messages > 0;
        page
    }

    fn load_channel_snapshot_document(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<OwnedSifsAgentChannelSnapshot>, AgentChannelStoreError> {
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
        Ok(Some(snapshot))
    }

    #[cfg(test)]
    fn load_channel_snapshot_only(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
        Ok(self
            .load_channel_snapshot_document(channel_id)?
            .map(|snapshot| snapshot.channel))
    }

    fn write_channel_snapshot_cache(
        &self,
        channel: &AgentChannel,
        applied_metadata_record_ids: &[String],
    ) -> Result<(), AgentChannelStoreError> {
        let snapshot = SifsAgentChannelSnapshot {
            schema_version: AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION,
            applied_metadata_record_ids,
            channel,
        };
        let bytes = serde_json::to_vec_pretty(&snapshot).map_err(|source| {
            AgentChannelStoreError::Serialization {
                detail: source.to_string(),
            }
        })?;
        self.write_file(&Self::snapshot_path(&channel.channel_id), &bytes)
    }

    /// Resolve channel metadata from immutable state records. `snapshot.json`
    /// is only a materialized cache: a concurrent writer may replace it with a
    /// cache covering fewer records, but the next read discovers and folds the
    /// omitted records back in.
    fn load_channel_metadata(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<(AgentChannel, Vec<String>)>, AgentChannelStoreError> {
        let snapshot = self.load_channel_snapshot_document(channel_id)?;
        let mut channel = snapshot.as_ref().map(|snapshot| snapshot.channel.clone());
        let mut applied = snapshot
            .as_ref()
            .map(|snapshot| {
                snapshot
                    .applied_metadata_record_ids
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let record_paths =
            self.paths_in_dir_with_suffix(&Self::metadata_records_dir(channel_id), ".json")?;
        let missing_paths = record_paths
            .into_iter()
            .filter(|path| {
                json_record_id_from_path(path).is_some_and(|record_id| !applied.contains(record_id))
            })
            .collect::<Vec<_>>();
        let mut repaired = snapshot.is_none() && !missing_paths.is_empty();
        for (path, record) in
            self.load_json_documents_with_paths::<SifsAgentChannelMetadataRecord>(&missing_paths)?
        {
            if record.schema_version != SIFS_AGENT_CHANNEL_METADATA_SCHEMA_VERSION {
                return Err(AgentChannelStoreError::Serialization {
                    detail: format!(
                        "unsupported channel metadata record schema version {} at {path}",
                        record.schema_version
                    ),
                });
            }
            let Some(path_record_id) = json_record_id_from_path(&path) else {
                continue;
            };
            if path_record_id != record.record_id {
                return Err(AgentChannelStoreError::Serialization {
                    detail: format!(
                        "channel metadata record id {} does not match path {path}",
                        record.record_id
                    ),
                });
            }
            if &record.channel.channel_id != channel_id {
                return Err(AgentChannelStoreError::Serialization {
                    detail: format!(
                        "channel metadata record {} belongs to {} instead of {channel_id}",
                        record.record_id, record.channel.channel_id
                    ),
                });
            }
            channel = Some(match channel {
                Some(existing) => merge_channel_snapshot(existing, record.channel)?,
                None => record.channel,
            });
            applied.insert(record.record_id);
            repaired = true;
        }

        let Some(channel) = channel else {
            return Ok(None);
        };
        let applied = applied.into_iter().collect::<Vec<_>>();
        if repaired {
            self.write_channel_snapshot_cache(&channel, &applied)?;
        }
        Ok(Some((channel, applied)))
    }

    pub fn load_channel_summary(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<AgentChannelSummary>, AgentChannelStoreError> {
        Ok(self
            .load_channel_metadata(channel_id)?
            .map(|(channel, _)| AgentChannelSummary::from_channel(&channel)))
    }

    pub fn load_recent_messages(
        &self,
        channel_id: &SessionId,
        limit: usize,
    ) -> Result<Vec<AgentChannelMessage>, AgentChannelStoreError> {
        Ok(self.load_recent_message_page(channel_id, limit)?.messages)
    }

    /// Discover immutable records not covered by a caller's prior scan.
    ///
    /// Unlike a tail read, this cannot miss a delayed/backdated record. The
    /// directory listing is metadata-only and bodies are fetched only for IDs
    /// absent from `known_message_ids`. On the first scan the returned page is
    /// bounded to `tail_limit`; subsequent scans return every newly discovered
    /// record so the caller can advance its cursor without gaps.
    pub fn load_message_reconciliation_page(
        &self,
        channel_id: &SessionId,
        known_message_ids: &BTreeSet<String>,
        tail_limit: usize,
    ) -> Result<AgentChannelMessageReconciliation, AgentChannelStoreError> {
        let legacy_messages = self
            .load_legacy_json_lines::<AgentChannelMessage>(&Self::message_chunks_dir(channel_id))?;
        let record_paths =
            self.paths_in_dir_with_suffix(&Self::message_records_dir(channel_id), ".json")?;
        let missing_record_paths = record_paths
            .into_iter()
            .filter(|path| {
                json_record_id_from_path(path)
                    .is_some_and(|message_id| !known_message_ids.contains(message_id))
            })
            .collect::<Vec<_>>();
        let loaded_records = self.load_message_record_documents(&missing_record_paths)?;

        let mut messages = legacy_messages
            .into_iter()
            .filter(|message| !known_message_ids.contains(message.message_id.as_str()))
            .collect::<Vec<_>>();
        for (path, message) in loaded_records {
            if json_record_id_from_path(&path) != Some(message.message_id.as_str()) {
                return Err(AgentChannelStoreError::Serialization {
                    detail: format!(
                        "channel message record {} does not match path {path}",
                        message.message_id
                    ),
                });
            }
            messages.push(message);
        }
        messages = deduplicate_channel_messages_preserving_sequence(messages)?;

        let mut newly_discovered_message_ids = messages
            .iter()
            .map(|message| message.message_id.to_string())
            .collect::<Vec<_>>();
        newly_discovered_message_ids.sort();
        newly_discovered_message_ids.dedup();
        let total_messages = known_message_ids
            .len()
            .saturating_add(newly_discovered_message_ids.len())
            .min(u64::MAX as usize) as u64;

        let page = if known_message_ids.is_empty() {
            let messages = canonicalize_channel_messages(messages)?;
            if tail_limit == 0 || messages.is_empty() {
                AgentChannelMessagePage::empty(channel_id, total_messages)
            } else {
                let first_sequence = total_messages
                    .saturating_sub(tail_limit as u64)
                    .saturating_add(1);
                Self::message_page_from_records(
                    channel_id,
                    messages,
                    first_sequence,
                    total_messages,
                    tail_limit,
                )
            }
        } else {
            messages.sort_by(|left, right| {
                left.sequence
                    .cmp(&right.sequence)
                    .then_with(|| left.created_at.cmp(&right.created_at))
                    .then_with(|| left.message_id.cmp(&right.message_id))
            });
            let start_sequence = messages.iter().map(|message| message.sequence).min();
            let end_sequence = messages.iter().map(|message| message.sequence).max();
            AgentChannelMessagePage {
                channel_id: channel_id.clone(),
                messages,
                start_sequence,
                end_sequence,
                has_older: total_messages > newly_discovered_message_ids.len() as u64,
                has_newer: false,
                total_messages,
                chunk_size: SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE,
            }
        };
        Ok(AgentChannelMessageReconciliation {
            page,
            discovered_message_ids: newly_discovered_message_ids,
        })
    }

    pub fn load_recent_message_page(
        &self,
        channel_id: &SessionId,
        limit: usize,
    ) -> Result<AgentChannelMessagePage, AgentChannelStoreError> {
        let messages = self.load_all_messages(channel_id)?;
        let total_messages = messages.len() as u64;
        if limit == 0 || total_messages == 0 {
            return Ok(AgentChannelMessagePage::empty(channel_id, 0));
        }
        let first_sequence = total_messages
            .saturating_sub(limit as u64)
            .saturating_add(1);
        Ok(Self::message_page_from_records(
            channel_id,
            messages,
            first_sequence,
            total_messages,
            limit,
        ))
    }

    pub fn load_message_page_from(
        &self,
        channel_id: &SessionId,
        start_sequence: u64,
        limit: usize,
    ) -> Result<AgentChannelMessagePage, AgentChannelStoreError> {
        let messages = self.load_all_messages(channel_id)?;
        let total_messages = messages.len() as u64;
        if limit == 0 || total_messages == 0 {
            return Ok(AgentChannelMessagePage::empty(channel_id, total_messages));
        }
        if start_sequence > total_messages {
            return Ok(Self::empty_message_page_after_end(
                channel_id,
                total_messages,
            ));
        }
        let start_sequence = start_sequence.max(1).min(total_messages);
        let end_sequence = start_sequence
            .saturating_add(limit as u64)
            .saturating_sub(1)
            .min(total_messages);
        Ok(Self::message_page_from_records(
            channel_id,
            messages,
            start_sequence,
            end_sequence,
            limit,
        ))
    }

    pub fn load_message_page_before(
        &self,
        channel_id: &SessionId,
        before_sequence: u64,
        limit: usize,
    ) -> Result<AgentChannelMessagePage, AgentChannelStoreError> {
        let messages = self.load_all_messages(channel_id)?;
        let total_messages = messages.len() as u64;
        if limit == 0 || total_messages == 0 {
            return Ok(AgentChannelMessagePage::empty(channel_id, total_messages));
        }
        if before_sequence <= 1 {
            return Ok(Self::empty_message_page_before_start(
                channel_id,
                total_messages,
            ));
        }
        let end_sequence = before_sequence.saturating_sub(1).min(total_messages);
        if end_sequence == 0 {
            return Ok(Self::empty_message_page_before_start(
                channel_id,
                total_messages,
            ));
        }
        let start_sequence = end_sequence.saturating_sub(limit as u64).saturating_add(1);
        Ok(Self::message_page_from_records(
            channel_id,
            messages,
            start_sequence,
            end_sequence,
            limit,
        ))
    }

    pub fn load_timeline_overview(
        &self,
        channel_id: &SessionId,
    ) -> Result<AgentChannelTimelineOverview, AgentChannelStoreError> {
        let messages = self.load_all_messages(channel_id)?;
        let total_messages = messages.len() as u64;
        if messages.is_empty() {
            return Ok(AgentChannelTimelineOverview {
                channel_id: channel_id.clone(),
                total_messages: 0,
                oldest_sequence: None,
                newest_sequence: None,
                chunk_size: SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE,
                segments: Vec::new(),
            });
        }

        let mut segments = Vec::new();
        for messages in messages.chunks(SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE as usize) {
            let first = messages.first().expect("non-empty message segment");
            let last = messages.last().expect("non-empty message segment");
            segments.push(AgentChannelTimelineSegment {
                start_sequence: first.sequence,
                end_sequence: last.sequence,
                message_count: messages.len() as u64,
                first_created_unix: Some(first.created_at.unix_timestamp()),
                last_created_unix: Some(last.created_at.unix_timestamp()),
                preview: messages.iter().find_map(timeline_preview_for_message),
            });
        }

        Ok(AgentChannelTimelineOverview {
            channel_id: channel_id.clone(),
            total_messages,
            oldest_sequence: segments.first().map(|segment| segment.start_sequence),
            newest_sequence: segments.last().map(|segment| segment.end_sequence),
            chunk_size: SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE,
            segments,
        })
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
        for channel_id in ids {
            if let Some((channel, _)) = self.load_channel_metadata(channel_id)? {
                summaries.push(AgentChannelSummary::from_channel(&channel));
            }
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
        let record = self.append_metadata_record(channel)?;
        let loaded = self.load_channel_metadata(&channel.channel_id)?;
        // A routed DFS write can commit before this node's namespace view has
        // observed the immutable record. Fold the exact record we just wrote
        // into the materialized cache instead of turning normal propagation
        // delay into a failed chat send. A later read still enumerates and
        // reconciles all immutable records, including concurrent writers.
        let (mut merged, applied) = reconcile_written_metadata_record(loaded, record)?;
        // Preserve a bounded transcript in the materialized cache for legacy
        // readers. Current readers replace it from immutable message records.
        merged.transcript = channel
            .recent_transcript_snapshot(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT)
            .transcript;
        merged.next_sequence = merged.next_sequence.max(channel.next_sequence);
        self.write_channel_snapshot_cache(&merged, &applied)
    }

    fn load_channel(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
        let Some((mut channel, _)) = self.load_channel_metadata(channel_id)? else {
            return Ok(None);
        };
        // A turn record is the commit point for a visible response. Repair its
        // per-message materialization before paging so a crash between the two
        // immutable writes cannot hide an answer after restart.
        for turn in self.load_turns(channel_id)? {
            for message in turn.outbox_messages {
                self.append_message_record(channel_id, &message)?;
            }
        }
        let recent =
            self.load_recent_message_page(channel_id, AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT)?;
        if !recent.messages.is_empty() {
            channel.transcript = recent.messages;
            channel.next_sequence = recent.total_messages.saturating_add(1);
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
        self.append_message_record(channel_id, message)
    }

    fn append_turn(
        &self,
        channel_id: &SessionId,
        turn: &AgentChannelTurn,
    ) -> Result<(), AgentChannelStoreError> {
        self.append_turn_record(channel_id, turn)
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

fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, AgentChannelStoreError> {
    serde_json::to_vec(value).map_err(|source| AgentChannelStoreError::Serialization {
        detail: source.to_string(),
    })
}

fn merge_channel_snapshot(
    existing: AgentChannel,
    candidate: AgentChannel,
) -> Result<AgentChannel, AgentChannelStoreError> {
    if existing.channel_id != candidate.channel_id {
        return Err(AgentChannelStoreError::Serialization {
            detail: "cannot merge snapshots for different agent channels".to_string(),
        });
    }

    let candidate_is_newer = candidate.updated_at > existing.updated_at
        || (candidate.updated_at == existing.updated_at
            && canonical_json_bytes(&candidate)? > canonical_json_bytes(&existing)?);
    let mut merged = if candidate_is_newer {
        candidate.clone()
    } else {
        existing.clone()
    };
    merged.created_at = existing.created_at.min(candidate.created_at);
    merged.updated_at = existing.updated_at.max(candidate.updated_at);
    merged.deleted_at = match (existing.deleted_at, candidate.deleted_at) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    };

    let mut transcript = existing.transcript.clone();
    transcript.extend(candidate.transcript.clone());
    merged.transcript = canonicalize_channel_messages(transcript)?;
    if merged.transcript.len() > AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT {
        let excess = merged
            .transcript
            .len()
            .saturating_sub(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT);
        merged.transcript.drain(0..excess);
    }
    let retained_message_ids = merged
        .transcript
        .iter()
        .map(|message| message.message_id.clone())
        .collect::<BTreeSet<_>>();
    let can_bound_observations_from_transcript =
        !existing.transcript.is_empty() && !candidate.transcript.is_empty();

    let (newer, older) = if candidate_is_newer {
        (&candidate, &existing)
    } else {
        (&existing, &candidate)
    };
    let mut users = newer
        .users
        .iter()
        .cloned()
        .map(|user| (user.user_id.clone(), user))
        .collect::<BTreeMap<_, _>>();
    for user in &older.users {
        users
            .entry(user.user_id.clone())
            .or_insert_with(|| user.clone());
    }
    merged.users = users.into_values().collect();

    let mut participants = newer
        .participants
        .iter()
        .cloned()
        .map(|participant| (participant.agent_key.clone(), participant))
        .collect::<BTreeMap<_, _>>();
    for participant in &older.participants {
        match participants.entry(participant.agent_key.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(participant.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let merged_participant = entry.get_mut();
                merged_participant.observed_through_sequence = merged_participant
                    .observed_through_sequence
                    .max(participant.observed_through_sequence);
                merged_participant.message_reconciliation_version = merged_participant
                    .message_reconciliation_version
                    .max(participant.message_reconciliation_version);
                merged_participant
                    .observed_message_ids
                    .extend(participant.observed_message_ids.iter().cloned());
            }
        }
    }
    merged.participants = participants.into_values().collect();
    for participant in &mut merged.participants {
        if can_bound_observations_from_transcript {
            participant
                .observed_message_ids
                .retain(|message_id| retained_message_ids.contains(message_id));
        }
        participant.observed_message_ids.sort();
        participant.observed_message_ids.dedup();
    }
    merged.participants.sort_by(|left, right| {
        left.order
            .cmp(&right.order)
            .then_with(|| left.agent_key.cmp(&right.agent_key))
            .then_with(|| left.participant_id.cmp(&right.participant_id))
    });
    merged.next_sequence = existing
        .next_sequence
        .max(candidate.next_sequence)
        .max(merged.transcript.len() as u64 + 1);
    Ok(merged)
}

fn canonicalize_channel_messages(
    messages: Vec<AgentChannelMessage>,
) -> Result<Vec<AgentChannelMessage>, AgentChannelStoreError> {
    let mut messages = deduplicate_channel_messages_preserving_sequence(messages)?;
    messages.sort_by(|left, right| {
        left.sequence
            .cmp(&right.sequence)
            .then_with(|| left.created_at.cmp(&right.created_at))
            .then_with(|| left.message_id.cmp(&right.message_id))
    });
    for (index, message) in messages.iter_mut().enumerate() {
        message.sequence = (index as u64).saturating_add(1);
    }
    Ok(messages)
}

fn deduplicate_channel_messages_preserving_sequence(
    messages: Vec<AgentChannelMessage>,
) -> Result<Vec<AgentChannelMessage>, AgentChannelStoreError> {
    let mut by_id = BTreeMap::<TaskId, AgentChannelMessage>::new();
    for message in messages {
        match by_id.entry(message.message_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(message);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                if !channel_message_records_equivalent(entry.get(), &message) {
                    return Err(AgentChannelStoreError::Serialization {
                        detail: format!(
                            "conflicting immutable channel message records for {}",
                            message.message_id
                        ),
                    });
                }
            }
        }
    }
    Ok(by_id.into_values().collect())
}

fn channel_message_records_equivalent(
    left: &AgentChannelMessage,
    right: &AgentChannelMessage,
) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.sequence = 0;
    right.sequence = 0;
    left == right
}

fn canonicalize_channel_turns(
    turns: Vec<AgentChannelTurn>,
) -> Result<Vec<AgentChannelTurn>, AgentChannelStoreError> {
    let mut by_id = BTreeMap::<TaskId, AgentChannelTurn>::new();
    for turn in turns {
        match by_id.entry(turn.turn_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(turn);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                if entry.get() != &turn {
                    return Err(AgentChannelStoreError::Serialization {
                        detail: format!(
                            "conflicting immutable channel turn records for {}",
                            turn.turn_id
                        ),
                    });
                }
            }
        }
    }

    let mut turns = by_id.into_values().collect::<Vec<_>>();
    turns.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.turn_id.cmp(&right.turn_id))
    });
    Ok(turns)
}

#[derive(Serialize)]
struct SifsAgentChannelSnapshot<'a> {
    schema_version: u32,
    applied_metadata_record_ids: &'a [String],
    channel: &'a AgentChannel,
}

#[derive(Deserialize)]
struct OwnedSifsAgentChannelSnapshot {
    schema_version: u32,
    #[serde(default)]
    applied_metadata_record_ids: Vec<String>,
    channel: AgentChannel,
}

#[derive(Serialize, Deserialize)]
struct SifsAgentChannelMetadataRecord {
    schema_version: u32,
    record_id: String,
    channel: AgentChannel,
}

fn reconcile_written_metadata_record(
    loaded: Option<(AgentChannel, Vec<String>)>,
    record: SifsAgentChannelMetadataRecord,
) -> Result<(AgentChannel, Vec<String>), AgentChannelStoreError> {
    let Some((mut merged, mut applied)) = loaded else {
        return Ok((record.channel, vec![record.record_id]));
    };
    if !applied.contains(&record.record_id) {
        merged = merge_channel_snapshot(merged, record.channel)?;
        applied.push(record.record_id);
        applied.sort();
    }
    Ok((merged, applied))
}

fn json_record_id_from_path(path: &str) -> Option<&str> {
    path.rsplit('/').next()?.strip_suffix(".json")
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

fn timeline_preview_for_message(message: &AgentChannelMessage) -> Option<String> {
    let content = message.content.trim();
    if content.is_empty() {
        return None;
    }
    let author = match &message.author {
        AgentChannelAuthor::User { display_name, .. } => {
            if display_name.trim().is_empty() {
                "User"
            } else {
                display_name.trim()
            }
        }
        AgentChannelAuthor::Agent { agent_name, .. } => {
            if agent_name.trim().is_empty() {
                "Agent"
            } else {
                agent_name.trim()
            }
        }
        AgentChannelAuthor::System => "System",
    };
    let mut preview = content.split_whitespace().collect::<Vec<_>>().join(" ");
    if preview.chars().count() > 120 {
        preview = preview.chars().take(117).collect::<String>();
        preview.push_str("...");
    }
    Some(format!("{author}: {preview}"))
}

/// Immutable, channel-scoped copy of the canonical model session. The visible
/// channel timeline remains authoritative for chat history; this checkpoint
/// carries the private execution state a second device cannot reconstruct from
/// visible messages alone (compaction projection, working state, tool evidence,
/// and pending control inputs).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct SifsAgentSessionCheckpoint {
    schema_version: u32,
    checkpoint_id: TaskId,
    channel_id: SessionId,
    participant_id: TaskId,
    agent_key: String,
    agent_session_id: SessionId,
    /// Snapshot of the channel audience at publication time. Loading requires
    /// an exact match so a membership change cannot silently widen access to an
    /// older model-session checkpoint.
    audience_user_ids: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: time::OffsetDateTime,
    snapshot: AgentSessionSnapshot,
}

/// SIFS checkpoint store kept separate from the node-local agent session store.
/// Each write gets a fresh task/ULID name, so readers never observe an in-place
/// replacement and can fall back past an incomplete or corrupt newest record.
struct SifsAgentSessionCheckpointStore {
    browser: SifsBrowser,
}

impl SifsAgentSessionCheckpointStore {
    fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        SifsBrowser::open(data_dir).map(|browser| Self { browser })
    }

    fn open_daemon_on_runtime(
        address: impl Into<String>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        SifsBrowser::open_daemon_on_runtime(address, runtime).map(|browser| Self { browser })
    }

    fn checkpoint_dir(
        channel_id: &SessionId,
        participant_id: &TaskId,
        session_id: &SessionId,
    ) -> String {
        format!(
            "{}/{}/session-checkpoints/v{}/{}/{}/",
            SIFS_AGENT_CHANNELS_ROOT,
            channel_id.as_str(),
            SIFS_AGENT_SESSION_CHECKPOINT_SCHEMA_VERSION,
            participant_id.as_str(),
            session_id.as_str(),
        )
    }

    fn audience_user_ids(channel: &AgentChannel) -> Vec<String> {
        let mut audience = channel
            .users
            .iter()
            .map(|user| user.user_id.trim().to_string())
            .filter(|user_id| !user_id.is_empty())
            .collect::<Vec<_>>();
        audience.sort();
        audience.dedup();
        audience
    }

    fn publish(
        &self,
        channel: &AgentChannel,
        participant: &AgentChannelParticipant,
        session: &AgentSession,
    ) -> Result<TaskId, String> {
        if session.session_id != participant.agent_session_id {
            return Err(format!(
                "agent session {} does not match channel participant session {}",
                session.session_id, participant.agent_session_id
            ));
        }
        let audience_user_ids = Self::audience_user_ids(channel);
        if !audience_user_ids
            .iter()
            .any(|user_id| user_id == AGENT_CHANNEL_APP_USER_ID)
        {
            return Err(format!(
                "channel {} does not authorize the app user for session checkpoint publication",
                channel.channel_id
            ));
        }
        let checkpoint_id = TaskId::generate();
        let record = SifsAgentSessionCheckpoint {
            schema_version: SIFS_AGENT_SESSION_CHECKPOINT_SCHEMA_VERSION,
            checkpoint_id: checkpoint_id.clone(),
            channel_id: channel.channel_id.clone(),
            participant_id: participant.participant_id.clone(),
            agent_key: participant.agent_key.clone(),
            agent_session_id: participant.agent_session_id.clone(),
            audience_user_ids,
            created_at: time::OffsetDateTime::now_utc(),
            snapshot: AgentSessionSnapshot::new(session.clone()),
        };
        let dir = Self::checkpoint_dir(
            &channel.channel_id,
            &participant.participant_id,
            &participant.agent_session_id,
        );
        self.browser.create_dir_all(&dir)?;
        let path = format!("{dir}{}.json", checkpoint_id.as_str());
        let bytes = serde_json::to_vec_pretty(&record)
            .map_err(|source| format!("serialize agent session checkpoint: {source}"))?;
        self.browser.write_file(&path, &bytes)?;
        Ok(checkpoint_id)
    }

    fn load_latest(
        &self,
        channel: &AgentChannel,
        participant: &AgentChannelParticipant,
    ) -> Result<Option<AgentSession>, String> {
        let dir = Self::checkpoint_dir(
            &channel.channel_id,
            &participant.participant_id,
            &participant.agent_session_id,
        );
        let mut candidates = self
            .browser
            .list_if_exists(&dir)?
            .into_iter()
            .filter(|entry| !entry.is_dir && entry.name.ends_with(".json"))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(None);
        }
        // TaskId is ULID-backed, so filename order is publication order. Read
        // newest-first and retain the immutable older records as recovery points.
        candidates.sort_by(|left, right| right.name.cmp(&left.name));
        let expected_audience = Self::audience_user_ids(channel);
        let mut rejected = Vec::new();
        for candidate in candidates {
            let bytes = match self.browser.read_file(&candidate.path) {
                Ok(bytes) => bytes,
                Err(error) => {
                    rejected.push(format!("{}: {error}", candidate.name));
                    continue;
                }
            };
            let record = match serde_json::from_slice::<SifsAgentSessionCheckpoint>(&bytes) {
                Ok(record) => record,
                Err(error) => {
                    rejected.push(format!("{}: invalid JSON: {error}", candidate.name));
                    continue;
                }
            };
            let expected_name = format!("{}.json", record.checkpoint_id.as_str());
            let valid = record.schema_version == SIFS_AGENT_SESSION_CHECKPOINT_SCHEMA_VERSION
                && record.snapshot.schema_version == AGENT_SESSION_SNAPSHOT_SCHEMA_VERSION
                && record.channel_id == channel.channel_id
                && record.participant_id == participant.participant_id
                && record.agent_key == participant.agent_key
                && record.agent_session_id == participant.agent_session_id
                && record.snapshot.session.session_id == participant.agent_session_id
                && record.audience_user_ids == expected_audience
                && record
                    .audience_user_ids
                    .iter()
                    .any(|user_id| user_id == AGENT_CHANNEL_APP_USER_ID)
                && candidate.name == expected_name;
            if valid {
                return Ok(Some(record.snapshot.session));
            }
            rejected.push(format!("{}: scope or schema mismatch", candidate.name));
        }
        Err(format!(
            "no valid agent session checkpoint for channel {} participant {} ({})",
            channel.channel_id,
            participant.participant_id,
            rejected.join("; ")
        ))
    }
}

fn select_resumable_agent_session(
    local: Option<AgentSession>,
    roaming: Option<AgentSession>,
) -> Result<Option<AgentSession>, String> {
    let (local, roaming) = match (local, roaming) {
        (None, None) => return Ok(None),
        (Some(local), None) => return Ok(Some(local)),
        (None, Some(roaming)) => return Ok(Some(roaming)),
        (Some(local), Some(roaming)) => (local, roaming),
    };
    if local.session_id != roaming.session_id || local.role != roaming.role {
        return Err(format!(
            "local and roaming agent sessions disagree on identity for {}",
            local.session_id
        ));
    }
    let reconciled_agent_key = match (
        local.agent_definition_key.as_deref(),
        roaming.agent_definition_key.as_deref(),
    ) {
        (Some(local_key), Some(roaming_key)) if local_key != roaming_key => {
            return Err(format!(
                "local and roaming agent sessions disagree on agent definition for {}",
                local.session_id
            ));
        }
        (Some(key), _) | (_, Some(key)) => Some(key.to_string()),
        (None, None) => None,
    };
    if local.workspace_binding != roaming.workspace_binding {
        return Err(format!(
            "local and roaming agent sessions disagree on workspace binding for {}",
            local.session_id
        ));
    }
    if local == roaming {
        return Ok(Some(roaming));
    }
    let local_items = local
        .raw_transcript
        .items
        .iter()
        .map(app_agent_item_id)
        .collect::<BTreeSet<_>>();
    let roaming_items = roaming
        .raw_transcript
        .items
        .iter()
        .map(app_agent_item_id)
        .collect::<BTreeSet<_>>();
    let local_contains_roaming = local_items.is_superset(&roaming_items);
    let roaming_contains_local = roaming_items.is_superset(&local_items);
    let private_control_matches = local.steering_queue == roaming.steering_queue
        && local.follow_up_queue == roaming.follow_up_queue
        && local.abort == roaming.abort;
    let mut selected = match (local_contains_roaming, roaming_contains_local) {
        // A node-local append may exist if the SIFS publication failed after
        // the local durable save. Never discard that strictly newer evidence.
        (true, false) if private_control_matches => local,
        // Normal multi-device resume: a successful remote publication has
        // advanced beyond this device's stale local snapshot.
        (false, true) if private_control_matches => roaming,
        (true, false) | (false, true) => {
            return Err(format!(
                "local and roaming agent sessions {} have concurrent transcript and private control-state changes",
                local.session_id
            ));
        }
        (true, true) => {
            match local.state_revision.cmp(&roaming.state_revision) {
                std::cmp::Ordering::Greater => local,
                std::cmp::Ordering::Less => roaming,
                std::cmp::Ordering::Equal if local.state_revision > 0 => {
                    return Err(format!(
                        "local and roaming agent sessions {} have divergent state at revision {}",
                        local.session_id, local.state_revision
                    ));
                }
                std::cmp::Ordering::Equal => {
                    // Legacy snapshots predate logical revisions. Preserve the
                    // old projection-age fallback only for those records; all
                    // newly mutated sessions use the revision above.
                    let local_projection_at = local
                        .projection
                        .as_ref()
                        .map(|projection| projection.created_at);
                    let roaming_projection_at = roaming
                        .projection
                        .as_ref()
                        .map(|projection| projection.created_at);
                    if local_projection_at > roaming_projection_at {
                        local
                    } else {
                        roaming
                    }
                }
            }
        }
        (false, false) => {
            return Err(format!(
                "local and roaming agent sessions {} have divergent raw transcripts",
                local.session_id
            ));
        }
    };
    selected.agent_definition_key = reconciled_agent_key;
    Ok(Some(selected))
}

fn app_agent_item_id(item: &AgentItem) -> TaskId {
    match item {
        AgentItem::UserMessage { item_id, .. }
        | AgentItem::AssistantText { item_id, .. }
        | AgentItem::AssistantReasoningSummary { item_id, .. }
        | AgentItem::ToolCall { item_id, .. }
        | AgentItem::ToolResult { item_id, .. }
        | AgentItem::AsyncTaskUpdate { item_id, .. }
        | AgentItem::ContextInjection { item_id, .. }
        | AgentItem::CompactionSummary { item_id, .. }
        | AgentItem::ModelUsage { item_id, .. }
        | AgentItem::Error { item_id, .. }
        | AgentItem::PatchSnapshot { item_id, .. }
        | AgentItem::VerificationResult { item_id, .. }
        | AgentItem::CodingRunState { item_id, .. } => item_id.clone(),
    }
}

/// One running chat conversation backed by the main-agent runtime.
pub struct AgentChat {
    data_dir: SinomoDataDir,
    daemon_connected: bool,
    channel_store: Box<dyn AgentChannelStore>,
    session_checkpoint_store: SifsAgentSessionCheckpointStore,
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
        let session_checkpoint_store =
            SifsAgentSessionCheckpointStore::open(&data_dir).map_err(|detail| CliError::Parse {
                detail: format!("open agent session checkpoint SIFS store failed: {detail}"),
            })?;
        Self::with_runtime_control_registry_and_stores(
            data_dir,
            daemon_connected,
            runtime_control_registry,
            Box::new(channel_store),
            session_checkpoint_store,
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
        let session_checkpoint_store = SifsAgentSessionCheckpointStore::open_daemon_on_runtime(
            &storage.address,
            &storage.runtime,
        )
        .map_err(|detail| CliError::Parse {
            detail: format!("open agent session checkpoint SIFS store failed: {detail}"),
        })?;
        Self::with_runtime_control_registry_and_stores(
            data_dir,
            daemon_connected,
            runtime_control_registry,
            Box::new(channel_store),
            session_checkpoint_store,
        )
    }

    fn with_runtime_control_registry_and_stores(
        data_dir: SinomoDataDir,
        daemon_connected: bool,
        runtime_control_registry: AgentRuntimeControlRegistry,
        channel_store: Box<dyn AgentChannelStore>,
        session_checkpoint_store: SifsAgentSessionCheckpointStore,
    ) -> Result<Self, CliError> {
        let handler = tui::app_build_chat_handler(&data_dir, None)?
            .with_runtime_control_registry(runtime_control_registry);
        let session = app_session(&data_dir, None);
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
            session_checkpoint_store,
            channel,
            runtime,
            tool_runtime,
            tool_runtime_explicit_override: false,
        })
    }

    /// Begin a brand-new chat (clears the in-memory transcript/context).
    pub fn reset_session(&mut self) {
        let session = app_session(&self.data_dir, None);
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
        let session = app_session(&self.data_dir, Some(session_id));
        let info = self.runtime_info();
        let (mut channel, loaded_existing_channel) =
            match self.channel_store.load_channel(&session.session_id) {
                Ok(Some(channel)) => (channel, true),
                Ok(None) => (
                    AgentChannel::with_default_title(session.session_id.clone()),
                    false,
                ),
                Err(error) => {
                    return Err(CliError::Parse {
                        detail: format!("load chat channel failed: {error}"),
                    });
                }
            };
        let channel_before_resume = channel.clone();
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), session.clone());
        let local_session = self
            .runtime
            .handler()
            .load_persisted_agent_session(&session.session_id)?;
        // An existing channel already has durable membership. Consult that
        // selection and the persisted session identity before the handler's
        // process-local default can add or re-enable a participant.
        let participant_id = resume_primary_participant_id(
            &mut channel,
            &info,
            loaded_existing_channel,
            local_session.as_ref(),
        );
        let mut participant = participant_id
            .as_ref()
            .and_then(|participant_id| channel.participant(participant_id))
            .cloned()
            .unwrap_or_else(|| outer_channel_runtime_participant(&channel, &info));
        let durable_participant_before_resume = participant_id
            .as_ref()
            .and_then(|participant_id| channel_before_resume.participant(participant_id))
            .cloned();
        let participant_agent_key = participant.agent_key.clone();
        if !self.tool_runtime_explicit_override {
            let participant_tool_runtime =
                chat_tool_runtime_from_agent_runtime_definition(&participant.tool_runtime);
            self.tool_runtime = if participant_tool_runtime.is_default() {
                default_tool_runtime_for_agent(&self.data_dir, &participant_agent_key)
            } else {
                participant_tool_runtime
            };
        }
        if !loaded_existing_channel && let Some(participant_id) = participant_id.as_ref() {
            let tool_runtime = self.tool_runtime.clone();
            set_participant_tool_runtime(&mut channel, participant_id, &tool_runtime);
            participant = channel
                .participant(participant_id)
                .cloned()
                .ok_or_else(|| CliError::Parse {
                    detail: format!(
                        "primary participant {participant_id} disappeared while resuming channel {}",
                        channel.channel_id
                    ),
                })?;
        }
        let roaming_session = if participant_id.is_some() {
            self.session_checkpoint_store
                .load_latest(&channel, &participant)
                .map_err(|detail| CliError::Parse {
                    detail: format!("load roaming agent session checkpoint failed: {detail}"),
                })?
        } else {
            None
        };
        let persisted_session = select_resumable_agent_session(local_session, roaming_session)
            .map_err(|detail| CliError::Parse {
                detail: format!("select canonical agent session failed: {detail}"),
            })?;
        let previous_agent_key = self.runtime_info().agent;
        match persisted_session {
            Some(mut persisted) => {
                let persisted_agent_key = match persisted.agent_definition_key.clone() {
                    Some(key) => key,
                    None if durable_participant_before_resume.as_ref().is_some_and(
                        |durable_participant| {
                            durable_participant.agent_session_id == persisted.session_id
                        },
                    ) =>
                    {
                        participant.agent_key.clone()
                    }
                    None => {
                        return Err(CliError::Parse {
                            detail: format!(
                                "legacy agent session `{}` has no persisted agent definition and no durable channel participant proves its identity; select the agent explicitly before resuming",
                                persisted.session_id
                            ),
                        });
                    }
                };
                let definition = resolve_agent(&self.data_dir, &persisted_agent_key).ok_or_else(
                    || CliError::Parse {
                        detail: format!(
                            "agent session `{}` references unknown agent definition `{persisted_agent_key}`",
                            persisted.session_id
                        ),
                    },
                )?;
                if let Some(durable_participant) = durable_participant_before_resume
                    .as_ref()
                    .filter(|durable_participant| {
                        durable_participant.agent_session_id == persisted.session_id
                    })
                    && durable_participant.agent_key != persisted_agent_key
                {
                    return Err(CliError::Parse {
                        detail: format!(
                            "agent session `{}` belongs to `{persisted_agent_key}`, but its durable channel participant belongs to `{}`",
                            persisted.session_id, durable_participant.agent_key
                        ),
                    });
                }
                if durable_participant_before_resume.is_none() {
                    if let Some(participant_id) = participant_id.as_ref() {
                        if let Some(channel_participant) = channel.participant_mut(participant_id) {
                            channel_participant.agent_key = persisted_agent_key.clone();
                            channel_participant.agent_name = definition.name.clone();
                            participant = channel_participant.clone();
                        }
                    } else {
                        participant.agent_key = persisted_agent_key.clone();
                        participant.agent_name = definition.name.clone();
                    }
                    if !self.tool_runtime_explicit_override {
                        self.tool_runtime =
                            ChatToolRuntimeSelection::from_agent_definition(&definition);
                    }
                }
                persisted.agent_definition_key = Some(persisted_agent_key);
                if !self.tool_runtime_explicit_override {
                    // Resolve the durable binding while configuring the tool
                    // registry below so a live SIFS mount guard is retained
                    // before its process-local path is exposed to the app.
                    self.tool_runtime.workspace_root = None;
                }
                let channel_view = channel.clone();
                if let Some(participant_id) = participant_id.as_ref() {
                    let channel_id = channel.channel_id.clone();
                    let channel_participant =
                        channel
                            .participant_mut(participant_id)
                            .ok_or_else(|| CliError::Parse {
                                detail: format!(
                                    "primary participant {participant_id} disappeared while reconciling channel {}",
                                    channel_id
                                ),
                            })?;
                    sync_participant_session(&channel_view, channel_participant, &mut persisted);
                    participant = channel_participant.clone();
                } else {
                    sync_participant_session(&channel_view, &mut participant, &mut persisted);
                }
                FileAgentSessionStore::for_data_dir(&self.data_dir)
                    .save_session(&persisted)
                    .map_err(|source| CliError::Parse {
                        detail: format!("persist reconciled chat session failed: {source}"),
                    })?;
                if participant_id.is_some() {
                    self.session_checkpoint_store
                        .publish(&channel, &participant, &persisted)
                        .map_err(|detail| CliError::Parse {
                            detail: format!(
                                "publish reconciled agent session checkpoint failed: {detail}"
                            ),
                        })?;
                }
                tui::app_prepare_agent(
                    self.runtime.handler_mut(),
                    &self.data_dir,
                    definition,
                    self.daemon_connected,
                )?;
                if let Err(error) = self
                    .runtime
                    .handler_mut()
                    .replace_full_agent_session(&session, persisted)
                {
                    if let Some(previous) = resolve_agent(&self.data_dir, &previous_agent_key) {
                        let _ = tui::app_prepare_agent(
                            self.runtime.handler_mut(),
                            &self.data_dir,
                            previous,
                            self.daemon_connected,
                        );
                    }
                    return Err(error);
                }
            }
            None => {
                let definition =
                    resolve_agent(&self.data_dir, &participant.agent_key).ok_or_else(|| {
                        CliError::Parse {
                            detail: format!(
                                "chat participant references unknown agent definition `{}`",
                                participant.agent_key
                            ),
                        }
                    })?;
                tui::app_prepare_agent(
                    self.runtime.handler_mut(),
                    &self.data_dir,
                    definition,
                    self.daemon_connected,
                )?;
                if let Err(error) = self.runtime.handler_mut().replace_agent_session(
                    &session,
                    agent_transcript_from_channel(&channel, history),
                ) {
                    if let Some(previous) = resolve_agent(&self.data_dir, &previous_agent_key) {
                        let _ = tui::app_prepare_agent(
                            self.runtime.handler_mut(),
                            &self.data_dir,
                            previous,
                            self.daemon_connected,
                        );
                    }
                    return Err(error);
                }
            }
        }
        self.channel = channel;
        self.configure_tool_runtime_for_session(&session.session_id)?;
        if loaded_existing_channel && self.channel != channel_before_resume {
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
        let mut selection = self.tool_runtime.clone();
        selection.sandbox_node_id = normalized;
        self.set_tool_runtime(selection)
    }

    pub fn set_tool_runtime(
        &mut self,
        mut selection: ChatToolRuntimeSelection,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        selection.sandbox_node_id = selection
            .sandbox_node_id
            .map(|node_id| node_id.trim().to_string())
            .filter(|node_id| !node_id.is_empty());
        if let Some(node_id) = selection.sandbox_node_id.as_deref() {
            parse_chat_tool_runtime_node_id(node_id)?;
        }
        if let Some(root) = selection.workspace_root.as_ref() {
            if root.as_os_str().is_empty() {
                return Err(CliError::Parse {
                    detail: "workspace directory cannot be empty".to_string(),
                });
            }
        }

        let session_id = self.runtime.session().session_id.clone();
        let sandbox_node_id = parse_chat_tool_runtime_selection(&selection)?;
        let selected_root = self
            .runtime
            .handler_mut()
            .configure_app_tool_runtime_for_session(
                &self.data_dir,
                &session_id,
                sandbox_node_id,
                selection.workspace_root.as_deref(),
                self.daemon_connected,
            )?;
        selection.workspace_root = selected_root;
        self.tool_runtime = selection;
        self.tool_runtime_explicit_override = true;
        let info = self.runtime_info();
        let participant_id = if self.channel.channel_kind == AgentChannelKind::Channel {
            selected_enabled_participant_id(&self.channel, &info, &session_id)
        } else {
            Some(ensure_primary_participant(&mut self.channel, &info, true))
        };
        if let Some(participant_id) = participant_id.as_ref() {
            let tool_runtime = self.tool_runtime.clone();
            set_participant_tool_runtime(&mut self.channel, participant_id, &tool_runtime);
        }
        // A group channel's outer runtime is not necessarily a member runtime.
        // Persist its local workspace binding, but publish a roaming member
        // checkpoint only when the session has an exact durable member identity.
        // Empty/all-disabled groups deliberately leave membership untouched.
        self.persist_current_session_checkpoint(participant_id.is_some())?;
        Ok(self.tool_runtime())
    }

    fn configure_tool_runtime_for_session(
        &mut self,
        session_id: &SessionId,
    ) -> Result<(), CliError> {
        let sandbox_node_id = parse_chat_tool_runtime_selection(&self.tool_runtime)?;
        let selected_root = self
            .runtime
            .handler_mut()
            .configure_app_tool_runtime_for_session(
                &self.data_dir,
                session_id,
                sandbox_node_id,
                self.tool_runtime.workspace_root.as_deref(),
                self.daemon_connected,
            )?;
        self.tool_runtime.workspace_root = selected_root;
        Ok(())
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
        let status = chat_control_status(result);
        self.persist_current_session_checkpoint(false)?;
        Ok(status)
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
        let status = chat_control_status(result);
        self.persist_current_session_checkpoint(false)?;
        Ok(status)
    }

    /// Remove a queued steering/follow-up message before the runtime consumes
    /// it (queued items are visible and removable).
    pub fn remove_queued_input(&self, input_id: TaskId) -> Result<ChatControlStatus, CliError> {
        let session_id = self.runtime.session().session_id.clone();
        let result = self
            .runtime_control_registry()
            .remove_queued_input(AgentSessionRemoveQueuedInputRequest {
                session_id,
                input_id,
            })
            .map_err(agent_control_cli_error)?;
        let status = chat_control_status(result);
        self.persist_current_session_checkpoint(false)?;
        Ok(status)
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
        let status = chat_control_status(result);
        self.persist_current_session_checkpoint(false)?;
        Ok(status)
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
        let source = app_channel_source(
            self.runtime.session(),
            (!attachments.is_empty()).then(|| {
                serde_json::json!({
                    "attachment_count": attachments.len()
                })
            }),
        );
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
                .ingest_user_input_with_item_id_and_context_attachments_streaming(
                    text,
                    channel_message_agent_item_id(&user_message.message_id),
                    context_attachments.clone(),
                    &mut emit,
                )?;
            for output in trailing {
                forward_output(output, &mut wrapped_event);
            }
            drain_runtime_events(&mut self.runtime, &mut wrapped_event)
        })();

        let user_item_id = channel_message_agent_item_id(&user_message.message_id);
        let user_message_persisted = self
            .runtime
            .handler()
            .agent_session_snapshot()?
            .is_some_and(|session| {
                session
                    .raw_transcript
                    .items
                    .iter()
                    .any(|item| app_agent_item_id(item) == user_item_id)
            });
        if user_message_persisted {
            self.mark_primary_message_observed(&user_message);
        }
        self.save_channel()?;

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
                        app_channel_source(self.runtime.session(), None),
                    );
                    response_message_ids.push(message.message_id.clone());
                    self.append_channel_turn(
                        user_message.message_id.clone(),
                        response_message_ids.clone(),
                        Vec::new(),
                        vec![message.clone()],
                    )?;
                    self.channel_store
                        .append_message(&self.channel.channel_id, &message)
                        .map_err(|source| {
                            agent_channel_store_cli_error("append chat agent message", source)
                        })?;
                    self.mark_primary_message_observed(&message);
                } else {
                    let message = self.channel.append_system_info(
                        "No agent responded.",
                        AgentChannelSource::system(serde_json::json!({
                            "channel_id": self.channel.channel_id,
                            "event_type": "no_agent_responded"
                        })),
                    );
                    system_message_ids.push(message.message_id.clone());
                    self.append_channel_turn(
                        user_message.message_id.clone(),
                        Vec::new(),
                        system_message_ids.clone(),
                        vec![message.clone()],
                    )?;
                    self.channel_store
                        .append_message(&self.channel.channel_id, &message)
                        .map_err(|source| {
                            agent_channel_store_cli_error("append chat system message", source)
                        })?;
                    self.mark_primary_message_observed(&message);
                    emit_channel_system(&message, &self.channel.channel_id, on_event);
                }
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
                self.append_channel_turn(
                    user_message.message_id,
                    Vec::new(),
                    vec![message.message_id.clone()],
                    vec![message.clone()],
                )?;
                self.channel_store
                    .append_message(&self.channel.channel_id, &message)
                    .map_err(|source| {
                        agent_channel_store_cli_error("append chat error message", source)
                    })?;
                self.mark_primary_message_observed(&message);
                emit_channel_system(&message, &self.channel.channel_id, on_event);
                self.save_channel()?;
            }
        }
        self.persist_primary_session_checkpoint()?;
        Ok(())
    }

    fn send_channel_with_attachments(
        &mut self,
        text: &str,
        attachments: &[ChatAttachmentInput],
        on_event: &mut dyn FnMut(ChatStreamEvent),
    ) -> Result<(), CliError> {
        let attachments = self.store_chat_attachments(attachments)?;
        let source = app_channel_source(
            self.runtime.session(),
            (!attachments.is_empty()).then(|| {
                serde_json::json!({
                    "attachment_count": attachments.len()
                })
            }),
        );
        let context_attachments = agent_context_attachments_from_channel_attachments(&attachments);
        let data_dir = self.data_dir.clone();
        let daemon_connected = self.daemon_connected;
        let previous_agent = self.runtime_info().agent;
        let previous_interaction_session = self.runtime.session().clone();
        let previous_agent_session = self.runtime.handler().agent_session_snapshot()?;
        let channel = self.channel.clone();
        let session_store = FileAgentSessionStore::for_data_dir(&data_dir);
        let mut participant_sessions = Vec::new();
        for participant in channel.participants.clone() {
            let local_session = session_store
                .load_session(&participant.agent_session_id)
                .map_err(|source| CliError::Parse {
                    detail: format!(
                        "load channel agent session `{}` failed: {source}",
                        participant.agent_session_id
                    ),
                })?;
            let roaming_session = self
                .session_checkpoint_store
                .load_latest(&channel, &participant)
                .map_err(|detail| CliError::Parse {
                    detail: format!(
                        "load roaming channel agent session `{}` failed: {detail}",
                        participant.agent_session_id
                    ),
                })?;
            let session = select_resumable_agent_session(local_session, roaming_session).map_err(
                |detail| CliError::Parse {
                    detail: format!(
                        "select canonical channel agent session `{}` failed: {detail}",
                        participant.agent_session_id
                    ),
                },
            )?;
            if let Some(session) = session {
                session_store
                    .save_session(&session)
                    .map_err(|source| CliError::Parse {
                        detail: format!(
                            "persist channel agent session `{}` locally failed: {source}",
                            participant.agent_session_id
                        ),
                    })?;
                participant_sessions.push((participant.participant_id, session));
            }
        }
        let executor = AppAgentChannelExecutor {
            data_dir: &data_dir,
            daemon_connected,
            runtime: &mut self.runtime,
            session_checkpoint_store: &self.session_checkpoint_store,
            context_attachments,
            tool_runtime: self.tool_runtime.clone(),
            tool_runtime_explicit_override: self.tool_runtime_explicit_override,
        };
        let store = self.channel_store.as_ref();
        let mut channel_runtime = AgentChannelRuntime::new(channel, store, executor)
            .with_loop_config(AgentChannelLoopConfig::default());
        for (participant_id, session) in participant_sessions {
            channel_runtime.insert_participant_session(participant_id, session);
        }
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
        self.runtime.replace_store_and_session(
            InMemoryEventStore::new(),
            previous_interaction_session.clone(),
        );
        if let Some(definition) = resolve_agent(&data_dir, &previous_agent) {
            tui::app_prepare_agent(
                self.runtime.handler_mut(),
                &data_dir,
                definition,
                daemon_connected,
            )?;
        } else {
            return Err(CliError::Parse {
                detail: format!(
                    "cannot restore previous agent definition `{previous_agent}` after channel turn"
                ),
            });
        }
        match previous_agent_session {
            Some(previous_agent_session) => self.runtime.handler_mut().replace_full_agent_session(
                &previous_interaction_session,
                previous_agent_session,
            )?,
            None => self.runtime.handler_mut().replace_agent_session(
                &previous_interaction_session,
                sinomo_agent::AgentTranscript::new(),
            )?,
        }
        self.configure_tool_runtime_for_session(&previous_interaction_session.session_id)?;
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
            match &attachment.source {
                ChatAttachmentSource::LocalPath(path) => {
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
                    stored.push(self.store_chat_attachment_bytes(
                        &attachment_file_name(path),
                        &path.display().to_string(),
                        "local-file",
                        &bytes,
                        None,
                    )?);
                }
                ChatAttachmentSource::LocalSnapshot { provenance, bytes } => {
                    stored.push(self.store_chat_attachment_bytes(
                        &attachment.file_name,
                        provenance.source_path(),
                        "local-snapshot",
                        bytes.as_ref(),
                        Some(serde_json::json!(provenance)),
                    )?);
                }
                ChatAttachmentSource::SifsSnapshot { provenance, bytes } => {
                    stored.push(self.store_chat_attachment_bytes(
                        &attachment.file_name,
                        &provenance.path,
                        "sifs-snapshot",
                        bytes.as_ref(),
                        Some(serde_json::json!(provenance)),
                    )?);
                }
            }
        }
        Ok(stored)
    }

    fn store_chat_attachment_bytes(
        &self,
        file_name: &str,
        original_path: &str,
        ingress: &str,
        bytes: &[u8],
        snapshot_provenance: Option<serde_json::Value>,
    ) -> Result<AgentChannelAttachment, CliError> {
        let file_name = if file_name.trim().is_empty() {
            attachment_file_name(Path::new(original_path))
        } else {
            file_name.trim().to_string()
        };
        let content_type = detect_attachment_content_type(Path::new(original_path), bytes);
        let model_visible =
            model_visible_attachment_projection(&file_name, bytes, content_type.as_deref());
        let attachment_id = TaskId::generate();
        let stored_path = self
            .channel_store
            .save_attachment(&self.channel.channel_id, &attachment_id, &file_name, bytes)
            .map_err(|source| CliError::Parse {
                detail: format!("save attachment failed: {source}"),
            })?;
        Ok(AgentChannelAttachment {
            attachment_id,
            file_name,
            original_path: original_path.to_string(),
            stored_path,
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            content_type: content_type.clone(),
            created_at: time::OffsetDateTime::now_utc(),
            source: app_channel_source(self.runtime.session(), {
                let mut metadata = serde_json::json!({
                    "ingress": ingress,
                    "original_path": original_path,
                    "content_type": content_type,
                    "model_visible": model_visible
                });
                if let Some(snapshot_provenance) = snapshot_provenance {
                    metadata["snapshot_provenance"] = snapshot_provenance;
                }
                Some(metadata)
            }),
        })
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
        let info = if self.channel.channel_kind == AgentChannelKind::Channel {
            // Adding/enabling a channel participant must not pair the outer
            // handler's existing session with a different definition. Each
            // participant turn prepares its own definition immediately before
            // restoring that participant's durable session.
            let mut info = self.runtime_info();
            info.agent = definition.key().to_string();
            info
        } else {
            let info = tui::app_switch_agent(
                self.runtime.handler_mut(),
                &self.data_dir,
                definition.clone(),
                self.daemon_connected,
            )?;
            ChatRuntimeInfo::from(info)
        };
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
        if self.channel.channel_kind == AgentChannelKind::Channel {
            self.save_channel()?;
        } else if self.runtime.handler().agent_session_snapshot()?.is_some() {
            self.persist_primary_session_checkpoint()?;
        } else {
            self.save_channel()?;
        }
        Ok(info)
    }

    fn primary_participant(&self) -> Option<&AgentChannelParticipant> {
        self.channel
            .participants
            .iter()
            .filter(|participant| participant.enabled)
            .min_by_key(|participant| participant.order)
    }

    fn mark_primary_message_observed(&mut self, message: &AgentChannelMessage) {
        let Some(participant_id) = self
            .primary_participant()
            .map(|participant| participant.participant_id.clone())
        else {
            return;
        };
        if let Some(participant) = self.channel.participant_mut(&participant_id) {
            mark_participant_message_observed(participant, &message.message_id, message.sequence);
        }
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

    fn persist_primary_session_checkpoint(&self) -> Result<(), CliError> {
        self.persist_current_session_checkpoint(true)
    }

    fn persist_current_session_checkpoint(
        &self,
        persist_channel_metadata: bool,
    ) -> Result<(), CliError> {
        let session = self
            .runtime
            .handler()
            .agent_session_snapshot()?
            .ok_or_else(|| CliError::Parse {
                detail: format!(
                    "agent runtime has no canonical session for channel {}",
                    self.channel.channel_id
                ),
            })?;
        let participant = if self.channel.channel_kind == AgentChannelKind::Channel {
            session
                .agent_definition_key
                .as_deref()
                .filter(|key| !key.trim().is_empty())
                .and_then(|session_agent_key| {
                    self.channel.participants.iter().find(|participant| {
                        participant.enabled
                            && participant.agent_key == session_agent_key
                            && participant.agent_session_id == session.session_id
                    })
                })
        } else {
            let participant = self.primary_participant().ok_or_else(|| CliError::Parse {
                detail: format!(
                    "channel {} has no enabled participant for session checkpoint",
                    self.channel.channel_id
                ),
            })?;
            let session_agent_key = session
                .agent_definition_key
                .as_deref()
                .filter(|key| !key.trim().is_empty())
                .ok_or_else(|| CliError::Parse {
                    detail: format!(
                        "canonical agent session {} has no agent definition for participant {}",
                        session.session_id, participant.participant_id
                    ),
                })?;
            if session_agent_key != participant.agent_key {
                return Err(CliError::Parse {
                    detail: format!(
                        "canonical agent session {} belongs to `{session_agent_key}`, but primary participant {} belongs to `{}`",
                        session.session_id, participant.participant_id, participant.agent_key
                    ),
                });
            }
            if session.session_id != participant.agent_session_id {
                return Err(CliError::Parse {
                    detail: format!(
                        "canonical agent session {} does not match primary participant {} session {}",
                        session.session_id,
                        participant.participant_id,
                        participant.agent_session_id
                    ),
                });
            }
            Some(participant)
        };
        if persist_channel_metadata {
            self.save_channel()?;
        }
        FileAgentSessionStore::for_data_dir(&self.data_dir)
            .save_session(&session)
            .map_err(|source| CliError::Parse {
                detail: format!("persist canonical chat session failed: {source}"),
            })?;
        if let Some(participant) = participant {
            self.session_checkpoint_store
                .publish(&self.channel, participant, &session)
                .map_err(|detail| CliError::Parse {
                    detail: format!("publish roaming agent session checkpoint failed: {detail}"),
                })?;
        }
        Ok(())
    }

    fn append_channel_turn(
        &self,
        trigger_message_id: TaskId,
        response_message_ids: Vec<TaskId>,
        system_message_ids: Vec<TaskId>,
        outbox_messages: Vec<AgentChannelMessage>,
    ) -> Result<(), CliError> {
        let turn = AgentChannelTurn {
            turn_id: TaskId::generate(),
            channel_id: self.channel.channel_id.clone(),
            trigger_message_id,
            decisions: Vec::new(),
            response_message_ids,
            system_message_ids,
            outbox_messages,
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
    session_checkpoint_store: &'a SifsAgentSessionCheckpointStore,
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
        if session.session_id != participant.agent_session_id {
            return Err(AgentChannelExecutorError::new(format!(
                "channel participant `{}` expects session `{}`, but received session `{}`",
                participant.participant_id, participant.agent_session_id, session.session_id
            )));
        }
        match session.agent_definition_key.as_deref() {
            Some(key) if key != participant.agent_key => {
                return Err(AgentChannelExecutorError::new(format!(
                    "channel participant `{}` belongs to `{}`, but session `{}` belongs to `{key}`",
                    participant.participant_id, participant.agent_key, session.session_id
                )));
            }
            None => {
                // The durable participant/session association is explicit
                // channel identity evidence for a legacy checkpoint.
                session.agent_definition_key = Some(participant.agent_key.clone());
            }
            Some(_) => {}
        }
        tui::app_prepare_agent(
            self.runtime.handler_mut(),
            self.data_dir,
            definition,
            self.daemon_connected,
        )
        .map_err(channel_executor_cli_error)?;

        let interaction_session = app_session(
            self.data_dir,
            Some(&participant.agent_session_id.to_string()),
        );
        let mut tool_runtime = if self.tool_runtime_explicit_override {
            self.tool_runtime.clone()
        } else {
            chat_tool_runtime_from_agent_runtime_definition(&participant.tool_runtime)
        };
        if tool_runtime.workspace_root.is_none() {
            tool_runtime.workspace_root = session
                .workspace_binding
                .as_ref()
                .and_then(|binding| binding.local_root.clone());
        }
        let sandbox_node_id =
            parse_chat_tool_runtime_selection(&tool_runtime).map_err(channel_executor_cli_error)?;
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), interaction_session.clone());
        self.runtime
            .handler_mut()
            .replace_full_agent_session(&interaction_session, session.clone())
            .map_err(channel_executor_cli_error)?;
        self.runtime
            .handler_mut()
            .configure_app_tool_runtime_for_session(
                self.data_dir,
                &interaction_session.session_id,
                sandbox_node_id,
                tool_runtime.workspace_root.as_deref(),
                self.daemon_connected,
            )
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
        let system_context = channel_agent_turn_system_context(channel, participant);
        let trailing = self
            .runtime
            .ingest_transient_system_context_with_attachments_streaming(
                &system_context,
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
        FileAgentSessionStore::for_data_dir(self.data_dir)
            .save_session(session)
            .map_err(|source| {
                AgentChannelExecutorError::new(format!(
                    "persist channel agent session `{}` failed: {source}",
                    participant.agent_session_id
                ))
            })?;
        self.session_checkpoint_store
            .publish(channel, participant, session)
            .map_err(|detail| {
                AgentChannelExecutorError::new(format!(
                    "publish roaming channel agent session `{}` failed: {detail}",
                    participant.agent_session_id
                ))
            })?;
        Ok(streamed_answer)
    }
}

fn channel_executor_cli_error(error: CliError) -> AgentChannelExecutorError {
    AgentChannelExecutorError::new(error.to_string())
}

fn channel_agent_turn_system_context(
    channel: &AgentChannel,
    participant: &AgentChannelParticipant,
) -> String {
    format!(
        "# Shared channel turn\n\n\
You are {agent_name} ({agent_key}) in the shared Sinomo channel `{title}`.\n\
Every visible channel message is shared with every other enabled agent. \
The latest visible message is already the final durable item in the session transcript. \
Decide whether you should publish a reply to it.\n\
If you have nothing useful, non-duplicative, or timely to add, return response_mode `silent` with empty text. \
If you reply, make `text` the exact message to publish in the channel. \
These turn instructions are transient control context and must not be repeated in the reply.",
        agent_name = participant.agent_name,
        agent_key = participant.agent_key,
        title = channel.title,
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
        queued_input: chat_queued_input_from_events(&result.events),
        removed_input_id: chat_removed_input_id_from_events(&result.events),
    }
}

/// Extract the steering/follow-up input a control call queued, if any.
pub fn chat_queued_input_from_events(events: &[AgentEvent]) -> Option<ChatQueuedTurnInput> {
    events.iter().find_map(|event| match &event.payload {
        AgentEventPayload::SteeringQueued { steering } => Some(ChatQueuedTurnInput {
            input_id: steering.steering_id.to_string(),
            kind: ChatQueuedTurnInputKind::Steering,
            message: steering.message.clone(),
        }),
        AgentEventPayload::FollowUpQueued { follow_up } => Some(ChatQueuedTurnInput {
            input_id: follow_up.follow_up_id.to_string(),
            kind: ChatQueuedTurnInputKind::FollowUp,
            message: follow_up.message.clone(),
        }),
        _ => None,
    })
}

/// Extract the queue id a control call removed, if any.
pub fn chat_removed_input_id_from_events(events: &[AgentEvent]) -> Option<String> {
    events.iter().find_map(|event| match &event.payload {
        AgentEventPayload::SteeringRemoved { steering } => Some(steering.steering_id.to_string()),
        AgentEventPayload::FollowUpRemoved { follow_up } => {
            Some(follow_up.follow_up_id.to_string())
        }
        _ => None,
    })
}

/// Mirror a session's pending queues (steering first — it is consumed sooner).
pub fn chat_queued_inputs_from_session(session: &AgentSession) -> Vec<ChatQueuedTurnInput> {
    session
        .steering_queue
        .iter()
        .map(|steering| ChatQueuedTurnInput {
            input_id: steering.steering_id.to_string(),
            kind: ChatQueuedTurnInputKind::Steering,
            message: steering.message.clone(),
        })
        .chain(
            session
                .follow_up_queue
                .iter()
                .map(|follow_up| ChatQueuedTurnInput {
                    input_id: follow_up.follow_up_id.to_string(),
                    kind: ChatQueuedTurnInputKind::FollowUp,
                    message: follow_up.message.clone(),
                }),
        )
        .collect()
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
    let queued_inputs = response
        .session
        .as_ref()
        .map(chat_queued_inputs_from_session)
        .unwrap_or_default();
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
        queued_inputs,
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
                item_id: channel_message_agent_item_id(&message.message_id),
                step_index: index,
                content,
                event_id: None,
            }),
            AgentChannelAuthor::Agent { .. } => transcript.push(AgentItem::AssistantText {
                item_id: channel_message_agent_item_id(&message.message_id),
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

fn selected_enabled_participant_id(
    channel: &AgentChannel,
    info: &ChatRuntimeInfo,
    runtime_session_id: &SessionId,
) -> Option<TaskId> {
    let enabled = || {
        channel
            .participants
            .iter()
            .filter(|participant| participant.enabled)
    };
    enabled()
        .find(|participant| {
            participant.agent_key == info.agent
                && participant.agent_session_id == *runtime_session_id
        })
        .or_else(|| enabled().find(|participant| participant.agent_key == info.agent))
        .or_else(|| enabled().min_by_key(|participant| participant.order))
        .map(|participant| participant.participant_id.clone())
}

fn resume_primary_participant_id(
    channel: &mut AgentChannel,
    info: &ChatRuntimeInfo,
    loaded_existing_channel: bool,
    local_session: Option<&AgentSession>,
) -> Option<TaskId> {
    if loaded_existing_channel {
        let exact_local = local_session
            .and_then(|session| {
                session
                    .agent_definition_key
                    .as_deref()
                    .map(|agent_key| (agent_key, &session.session_id))
            })
            .and_then(|(agent_key, session_id)| {
                channel
                    .participants
                    .iter()
                    .filter(|participant| participant.enabled)
                    .find(|participant| {
                        participant.agent_key == agent_key
                            && participant.agent_session_id == *session_id
                    })
            });
        let durable_primary = channel
            .participants
            .iter()
            .filter(|participant| participant.enabled)
            .min_by_key(|participant| participant.order);
        if channel.channel_kind == AgentChannelKind::Channel {
            let durable_outer_identity = channel
                .participants
                .iter()
                .filter(|participant| participant.enabled)
                .find(|participant| participant.agent_key == info.agent);
            return exact_local
                .or(durable_outer_identity)
                .or(durable_primary)
                .map(|participant| participant.participant_id.clone());
        }
        if let Some(participant) = exact_local.or(durable_primary) {
            return Some(participant.participant_id.clone());
        }
    }
    let align_primary_session = should_align_primary_participant(channel);
    Some(ensure_primary_participant(
        channel,
        info,
        align_primary_session,
    ))
}

/// Runtime-only identity used to open an existing group channel that currently
/// has no enabled participants. It is deliberately not inserted into channel
/// membership or used for roaming participant checkpoints.
fn outer_channel_runtime_participant(
    channel: &AgentChannel,
    info: &ChatRuntimeInfo,
) -> AgentChannelParticipant {
    let agent_key = if info.agent.trim().is_empty() {
        "main-agent".to_string()
    } else {
        info.agent.clone()
    };
    AgentChannelParticipant {
        participant_id: TaskId::generate(),
        agent_key: agent_key.clone(),
        agent_name: agent_key,
        agent_session_id: channel.channel_id.clone(),
        enabled: true,
        order: 0,
        tool_runtime: sinomo_agent::AgentToolRuntimeDefinition::default(),
        observed_through_sequence: 0,
        message_reconciliation_version: sinomo_agent::AGENT_CHANNEL_MESSAGE_RECONCILIATION_VERSION,
        observed_message_ids: Vec::new(),
    }
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
        workspace_root: None,
    }
}

impl ChatToolRuntimeSelection {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

fn should_align_primary_participant(channel: &AgentChannel) -> bool {
    channel.channel_kind != AgentChannelKind::Channel
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
        message_reconciliation_version: sinomo_agent::AGENT_CHANNEL_MESSAGE_RECONCILIATION_VERSION,
        observed_message_ids: Vec::new(),
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
        None,
        None,
        APP_CHAT_RUNTIME_DRAIN_INTERVAL,
    )
}

fn drain_runtime_events_with_limits(
    mut drain_events: impl FnMut() -> Result<Vec<AgentRuntimeEvent>, CliError>,
    on_event: &mut dyn FnMut(ChatStreamEvent),
    start_timeout: Option<Duration>,
    silence_timeout: Option<Duration>,
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
                if let Some(silence_timeout) = silence_timeout
                    && last_event_at.elapsed() >= silence_timeout
                {
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
            if !terminal
                && start_timeout.is_none_or(|start_timeout| started_at.elapsed() < start_timeout)
            {
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

fn app_session(data_dir: &SinomoDataDir, session_id: Option<&str>) -> InteractionSession {
    use std::str::FromStr;
    let node_id = read_cli_cluster_state(&ClusterPaths::new(data_dir))
        .map(|state| state.local_node_id)
        .unwrap_or_else(|_| NodeId::from_legacy_named("node_app").expect("valid app node id"));
    let session_id = session_id
        .and_then(|id| SessionId::from_str(id).ok())
        .unwrap_or_else(SessionId::generate);
    InteractionSession::new(
        session_id,
        node_id,
        InteractionSurface::new("surface.app", InteractionSurfaceKind::App),
    )
}

fn app_channel_source(
    session: &InteractionSession,
    additional_metadata: Option<serde_json::Value>,
) -> AgentChannelSource {
    let mut metadata = serde_json::Map::from_iter([
        (
            "origin_node_id".to_string(),
            serde_json::Value::String(session.node_id.to_string()),
        ),
        (
            "origin_surface_id".to_string(),
            serde_json::Value::String(session.surface.surface_id.clone()),
        ),
    ]);
    if let Some(serde_json::Value::Object(additional)) = additional_metadata {
        metadata.extend(additional);
    }
    AgentChannelSource {
        metadata: serde_json::Value::Object(metadata),
        ..AgentChannelSource::app()
    }
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
    agent_summaries_with_error(data_dir).0
}

/// Like [`agent_summaries`], but also reports the catalog-load error (if any) that
/// forced the fallback to built-ins. The stored catalog lives in SIFS, so this can
/// be slow or fail while the distributed file system is unhealthy — callers off the
/// UI thread use the error to surface a degraded-state notice.
pub fn agent_summaries_with_error(data_dir: &SinomoDataDir) -> (Vec<AgentSummary>, Option<String>) {
    match AgentCatalog::load_from_data_dir(data_dir) {
        Ok(catalog) => (agent_catalog_summaries(&catalog), None),
        Err(error) => (
            agent_catalog_summaries(&AgentCatalog::built_ins()),
            Some(error.to_string()),
        ),
    }
}

/// Summaries for the built-in agent catalog only. Does no data-dir or SIFS I/O, so
/// it is safe on startup paths that must not block on the distributed file system.
pub fn built_in_agent_summaries() -> Vec<AgentSummary> {
    agent_catalog_summaries(&AgentCatalog::built_ins())
}

fn agent_catalog_summaries(catalog: &AgentCatalog) -> Vec<AgentSummary> {
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

/// Exact visible version returned by a safe SIFS editor save.
pub type SifsWriteReceipt = sinomo_runtime::api::DaemonDfsBrowserWriteReceipt;

/// Typed result of a fail-closed SIFS compare-and-swap replacement.
pub type SifsReplaceFileIfVersionOutcome =
    sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome;

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
            timeout: sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
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
            timeout: sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
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

    /// Read the exact immutable object version described by a prior stat.
    pub fn read_object_version(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<Vec<u8>, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => {
                backend.read_object_version(object_id, version, content_hash, size_bytes)
            }
            SifsBrowserBackend::Remote(remote) => {
                remote.read_object_version(object_id, version, content_hash, size_bytes)
            }
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

    /// Replace a file only if both its identity and version still match a
    /// previously loaded snapshot.
    pub fn replace_file_if_version(
        &self,
        path: &str,
        expected_object_id: &str,
        expected_version: &str,
        bytes: &[u8],
    ) -> Result<SifsReplaceFileIfVersionOutcome, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => {
                backend.replace_file_if_version(path, expected_object_id, expected_version, bytes)
            }
            SifsBrowserBackend::Remote(remote) => {
                remote.replace_file_if_version(path, expected_object_id, expected_version, bytes)
            }
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

    fn read_object_version(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<Vec<u8>, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserReadObjectVersionResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserReadObjectVersionRequest {
                object_id: object_id.to_string(),
                version: version.to_string(),
                content_hash: content_hash.to_string(),
                size_bytes: Some(size_bytes),
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

    fn replace_file_if_version(
        &self,
        path: &str,
        expected_object_id: &str,
        expected_version: &str,
        bytes: &[u8],
    ) -> Result<SifsReplaceFileIfVersionOutcome, String> {
        let request = ObjectRpcRequest::encode_body(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
            sinomo_runtime::api::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
                expected_object_id: expected_object_id.to_string(),
                expected_version: expected_version.to_string(),
            },
        )
        .map_err(|error| error.to_string())?;
        let response = if let Some(runtime) = &self.runtime {
            runtime.block_on(sinomo_runtime::api::send_daemon_rpc_async(
                &self.address,
                &request,
                sinomo_runtime::network::DaemonRpcTimeouts::connect_and_idle(self.timeout),
                runtime,
            ))
        } else {
            sinomo_runtime::api::send_daemon_rpc(&self.address, &request, self.timeout)
        };
        match response {
            Ok(response) => {
                response.error_detail().map_err(|error| error.to_string())?;
                match response.decode_body::<
                    sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionResponse,
                >() {
                    Ok(response) => Ok(response.outcome),
                    Err(error) => self.reconcile_ambiguous_conditional_replace(
                        path,
                        bytes,
                        &format!("the daemon returned an unreadable success response: {error}"),
                    ),
                }
            }
            Err(error)
                if error.delivery_state()
                    == Some(sinomo_runtime::network::NetworkDeliveryState::PossiblyApplied) =>
            {
                self.reconcile_ambiguous_conditional_replace(path, bytes, &error.to_string())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    fn reconcile_ambiguous_conditional_replace(
        &self,
        path: &str,
        requested_bytes: &[u8],
        original_error: &str,
    ) -> Result<SifsReplaceFileIfVersionOutcome, String> {
        let details = self.stat(path).map_err(|reconcile_error| {
            format!(
                "conditional SIFS save outcome is unknown: {original_error}; reconciliation stat failed: {reconcile_error}"
            )
        })?;
        let content_hash = details.content_hash.as_deref().ok_or_else(|| {
            format!(
                "conditional SIFS save outcome is unknown: {original_error}; the current version has no content hash"
            )
        })?;
        let size_bytes = details.size_bytes.ok_or_else(|| {
            format!(
                "conditional SIFS save outcome is unknown: {original_error}; the current version has no size"
            )
        })?;
        let current_bytes = self
            .read_object_version(
                &details.object_id,
                &details.version,
                content_hash,
                size_bytes,
            )
            .map_err(|reconcile_error| {
                format!(
                    "conditional SIFS save outcome is unknown: {original_error}; exact-version reconciliation failed: {reconcile_error}"
                )
            })?;
        reconcile_conditional_replace_bytes(
            sinomo_runtime::api::DaemonDfsBrowserWriteReceipt {
                object_id: details.object_id,
                version: details.version,
                content_hash: details.content_hash,
                size_bytes: details.size_bytes,
            },
            &current_bytes,
            requested_bytes,
            original_error,
        )
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

fn reconcile_conditional_replace_bytes(
    receipt: SifsWriteReceipt,
    current_bytes: &[u8],
    requested_bytes: &[u8],
    original_error: &str,
) -> Result<SifsReplaceFileIfVersionOutcome, String> {
    use sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome as Outcome;

    if current_bytes != requested_bytes {
        return Err(format!(
            "conditional SIFS save outcome is unknown: {original_error}; the current immutable version differs from the requested contents"
        ));
    }
    Ok(Outcome::Committed {
        receipt,
        maintenance_warning: Some(format!(
            "the daemon response was not received, but the current immutable SIFS version was verified byte-for-byte: {original_error}"
        )),
    })
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};

    #[test]
    fn ambiguous_conditional_replace_only_commits_after_exact_byte_verification() {
        let receipt = SifsWriteReceipt {
            object_id: "object-current".to_string(),
            version: "version-current".to_string(),
            content_hash: Some("hash-current".to_string()),
            size_bytes: Some(5),
        };
        let outcome = reconcile_conditional_replace_bytes(
            receipt.clone(),
            b"saved",
            b"saved",
            "response timed out",
        )
        .expect("identical immutable bytes reconcile the save");
        assert!(matches!(
            outcome,
            SifsReplaceFileIfVersionOutcome::Committed {
                receipt: committed,
                maintenance_warning: Some(_),
            } if committed == receipt
        ));

        let error =
            reconcile_conditional_replace_bytes(receipt, b"other", b"saved", "response timed out")
                .expect_err("different bytes must preserve an unknown outcome");
        assert!(error.contains("differs from the requested contents"));
    }

    fn json_lines<T: Serialize>(values: &[T]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(
                serde_json::to_string(value)
                    .expect("serialize JSONL test value")
                    .as_bytes(),
            );
            bytes.push(b'\n');
        }
        bytes
    }

    fn channel_turn(channel_id: &SessionId, trigger_message_id: &TaskId) -> AgentChannelTurn {
        AgentChannelTurn {
            turn_id: TaskId::generate(),
            channel_id: channel_id.clone(),
            trigger_message_id: trigger_message_id.clone(),
            decisions: Vec::new(),
            response_message_ids: Vec::new(),
            system_message_ids: Vec::new(),
            outbox_messages: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        }
    }

    struct CrashAfterTurnChannelStore {
        inner: SifsAgentChannelStore,
        turn_committed: AtomicBool,
        fail_next_materialization: AtomicBool,
    }

    impl CrashAfterTurnChannelStore {
        fn new(inner: SifsAgentChannelStore) -> Self {
            Self {
                inner,
                turn_committed: AtomicBool::new(false),
                fail_next_materialization: AtomicBool::new(true),
            }
        }
    }

    impl AgentChannelStore for CrashAfterTurnChannelStore {
        fn save_channel(&self, channel: &AgentChannel) -> Result<(), AgentChannelStoreError> {
            self.inner.save_channel(channel)
        }

        fn load_channel(
            &self,
            channel_id: &SessionId,
        ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
            self.inner.load_channel(channel_id)
        }

        fn list_channels(&self) -> Result<Vec<AgentChannel>, AgentChannelStoreError> {
            self.inner.list_channels()
        }

        fn append_message(
            &self,
            channel_id: &SessionId,
            message: &AgentChannelMessage,
        ) -> Result<(), AgentChannelStoreError> {
            if self.turn_committed.load(Ordering::SeqCst)
                && self.fail_next_materialization.swap(false, Ordering::SeqCst)
            {
                return Err(AgentChannelStoreError::Io {
                    path: format!("simulated://{channel_id}/{}", message.message_id),
                    detail: "simulated crash before channel message materialization".to_string(),
                });
            }
            self.inner.append_message(channel_id, message)
        }

        fn append_turn(
            &self,
            channel_id: &SessionId,
            turn: &AgentChannelTurn,
        ) -> Result<(), AgentChannelStoreError> {
            self.inner.append_turn(channel_id, turn)?;
            self.turn_committed.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn save_attachment(
            &self,
            channel_id: &SessionId,
            attachment_id: &TaskId,
            file_name: &str,
            bytes: &[u8],
        ) -> Result<String, AgentChannelStoreError> {
            self.inner
                .save_attachment(channel_id, attachment_id, file_name, bytes)
        }
    }

    struct FixedChannelReply(&'static str);

    impl AgentChannelTurnExecutor for FixedChannelReply {
        fn decide_speak(
            &mut self,
            _channel: &AgentChannel,
            participant: &AgentChannelParticipant,
            _latest_user_message: &AgentChannelMessage,
            _session: &AgentSession,
        ) -> Result<AgentChannelSpeakDecision, AgentChannelExecutorError> {
            Ok(AgentChannelSpeakDecision::speak(
                participant,
                "scripted crash recovery response",
                1.0,
            ))
        }

        fn speak(
            &mut self,
            _channel: &AgentChannel,
            _participant: &AgentChannelParticipant,
            _session: &mut AgentSession,
            _on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
        ) -> Result<String, AgentChannelExecutorError> {
            Ok(self.0.to_string())
        }
    }

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
    fn chat_attachment_snapshot_inputs_preserve_bytes_and_provenance() {
        let local = ChatAttachmentInput::local_snapshot(
            "local.txt",
            "C:\\workspace\\local.txt",
            b"local snapshot".to_vec(),
        );
        assert_eq!(local.file_name, "local.txt");
        match local.source {
            ChatAttachmentSource::LocalSnapshot { provenance, bytes } => {
                assert_eq!(provenance.original_path, "C:\\workspace\\local.txt");
                assert_eq!(provenance.canonical_path, None);
                assert!(!provenance.unsaved_editor_buffer);
                assert_eq!(bytes.as_ref(), b"local snapshot");
            }
            source => panic!("expected local snapshot, got {source:?}"),
        }

        let sifs = ChatAttachmentInput::sifs_snapshot(
            "remote.rs",
            "/workspace/remote.rs",
            b"remote snapshot".to_vec(),
        );
        match sifs.source {
            ChatAttachmentSource::SifsSnapshot { provenance, bytes } => {
                assert_eq!(provenance.path, "/workspace/remote.rs");
                assert_eq!(provenance.object_id, None);
                assert!(!provenance.unsaved_editor_buffer);
                assert_eq!(bytes.as_ref(), b"remote snapshot");
            }
            source => panic!("expected SIFS snapshot, got {source:?}"),
        }
    }

    #[test]
    fn chat_attachment_store_writes_snapshot_bytes_without_reopening_source() {
        let root = sinomo_testdir::TestDir::new("sinomo-chat-attachment-snapshot");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        let snapshot = b"exact bytes already displayed by Files".to_vec();
        let sifs_snapshot = b"exact SIFS editor bytes".to_vec();

        let stored = chat
            .store_chat_attachments(&[
                ChatAttachmentInput::local_snapshot_with_provenance(
                    "displayed.txt",
                    ChatLocalAttachmentProvenance {
                        original_path: "C:\\missing-now\\displayed.txt".to_string(),
                        canonical_path: Some("C:\\workspace\\displayed.txt".to_string()),
                        modified_at_unix_nanos: Some(42),
                        unsaved_editor_buffer: true,
                    },
                    snapshot.clone(),
                ),
                ChatAttachmentInput::sifs_snapshot_with_provenance(
                    "remote.rs",
                    ChatSifsAttachmentProvenance {
                        path: "/workspace/remote.rs".to_string(),
                        object_id: Some("object-remote".to_string()),
                        version: Some("version-7".to_string()),
                        content_hash: Some("sha256:remote".to_string()),
                        policy: Some("editable".to_string()),
                        unsaved_editor_buffer: false,
                    },
                    sifs_snapshot.clone(),
                ),
            ])
            .expect("store immutable snapshot");

        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].original_path, "C:\\workspace\\displayed.txt");
        assert_eq!(stored[0].size_bytes, snapshot.len() as u64);
        assert_eq!(
            stored[0].source.metadata["ingress"].as_str(),
            Some("local-snapshot")
        );
        assert_eq!(
            stored[0].source.metadata["snapshot_provenance"]["original_path"].as_str(),
            Some("C:\\missing-now\\displayed.txt")
        );
        assert_eq!(
            stored[0].source.metadata["snapshot_provenance"]["canonical_path"].as_str(),
            Some("C:\\workspace\\displayed.txt")
        );
        assert_eq!(
            stored[0].source.metadata["snapshot_provenance"]["modified_at_unix_nanos"].as_u64(),
            Some(42)
        );
        assert_eq!(
            stored[0].source.metadata["snapshot_provenance"]["unsaved_editor_buffer"].as_bool(),
            Some(true)
        );
        assert_eq!(
            stored[1].source.metadata["snapshot_provenance"]["object_id"].as_str(),
            Some("object-remote")
        );
        assert_eq!(
            stored[1].source.metadata["snapshot_provenance"]["version"].as_str(),
            Some("version-7")
        );
        assert_eq!(
            stored[1].source.metadata["snapshot_provenance"]["content_hash"].as_str(),
            Some("sha256:remote")
        );
        assert_eq!(
            stored[1].source.metadata["snapshot_provenance"]["policy"].as_str(),
            Some("editable")
        );
        let browser = SifsBrowser::open(&data_dir).expect("open attachment store");
        assert_eq!(
            browser
                .read_file(&stored[0].stored_path)
                .expect("read stored attachment"),
            snapshot
        );
        assert_eq!(
            browser
                .read_file(&stored[1].stored_path)
                .expect("read stored SIFS snapshot attachment"),
            sifs_snapshot
        );
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
            Some(Duration::from_secs(1)),
            Some(Duration::ZERO),
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
    fn channel_metadata_write_reconciles_before_namespace_visibility() {
        let channel_id = SessionId::generate();
        let stale = AgentChannel::new_channel(channel_id.clone(), "Shared channel");
        let mut written = stale.clone();
        let participant_id = written.ensure_participant("main-agent", "Main agent");
        let record = SifsAgentChannelMetadataRecord {
            schema_version: SIFS_AGENT_CHANNEL_METADATA_SCHEMA_VERSION,
            record_id: "written-record".to_string(),
            channel: written,
        };

        let (merged, applied) = reconcile_written_metadata_record(
            Some((stale, vec!["older-record".to_string()])),
            record,
        )
        .expect("reconcile committed record");

        assert!(merged.participant(&participant_id).is_some());
        assert_eq!(
            applied,
            vec!["older-record".to_string(), "written-record".to_string()]
        );
    }

    #[test]
    fn channel_metadata_write_bootstraps_cache_when_listing_is_stale() {
        let channel = AgentChannel::new_channel(SessionId::generate(), "New channel");
        let channel_id = channel.channel_id.clone();
        let record = SifsAgentChannelMetadataRecord {
            schema_version: SIFS_AGENT_CHANNEL_METADATA_SCHEMA_VERSION,
            record_id: "first-record".to_string(),
            channel,
        };

        let (merged, applied) =
            reconcile_written_metadata_record(None, record).expect("bootstrap cache");

        assert_eq!(merged.channel_id, channel_id);
        assert_eq!(applied, vec!["first-record".to_string()]);
    }

    #[test]
    fn roaming_session_reconciliation_rejects_identity_and_workspace_drift() {
        let session_id = SessionId::generate();
        let local = AgentSession::new(session_id.clone(), AgentRole::Deliberation)
            .with_agent_definition_key("coding-agent");
        let mut roaming = local.clone();
        roaming.agent_definition_key = Some("main-agent".to_string());
        let identity_error = select_resumable_agent_session(Some(local.clone()), Some(roaming))
            .expect_err("agent identity drift must be rejected");
        assert!(identity_error.contains("disagree on agent definition"));

        let mut local = local;
        local.workspace_binding = Some(sinomo_agent::WorkspaceRef::local_path("/workspace/a"));
        let mut roaming = local.clone();
        roaming.workspace_binding = Some(sinomo_agent::WorkspaceRef::local_path("/workspace/b"));
        let workspace_error = select_resumable_agent_session(Some(local), Some(roaming))
            .expect_err("workspace drift must be rejected");
        assert!(workspace_error.contains("disagree on workspace binding"));
    }

    #[test]
    fn primary_checkpoint_rejects_participant_agent_identity_drift() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-checkpoint-agent-drift");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        let participant = chat
            .channel
            .participants
            .iter_mut()
            .find(|participant| participant.enabled)
            .expect("enabled primary participant");
        participant.agent_key = "coding-agent".to_string();

        let error = chat
            .persist_primary_session_checkpoint()
            .expect_err("mismatched participant must not be checkpointed");

        assert!(
            error.to_string().contains("belongs to `main-agent`")
                && error.to_string().contains("belongs to `coding-agent`"),
            "checkpoint error should explain both identities: {error}"
        );
    }

    #[test]
    fn roaming_session_reconciliation_uses_state_revision_for_control_queues() {
        let session_id = SessionId::generate();
        let mut roaming = AgentSession::new(session_id, AgentRole::Deliberation)
            .with_agent_definition_key("main-agent");
        roaming.state_revision = 1;
        let mut local = roaming.clone();
        local
            .steering_queue
            .push(sinomo_agent::AgentSteeringMessage::new(
                "local-only steering",
            ));
        local.state_revision = 2;

        let selected = select_resumable_agent_session(Some(local.clone()), Some(roaming.clone()))
            .expect("select locally newer control state")
            .expect("session");
        assert_eq!(selected.steering_queue, local.steering_queue);

        roaming
            .follow_up_queue
            .push(sinomo_agent::AgentFollowUpMessage::new("roaming follow-up"));
        roaming.state_revision = 3;
        let selected = select_resumable_agent_session(Some(local), Some(roaming.clone()))
            .expect("select roaming newer control state")
            .expect("session");
        assert_eq!(selected.follow_up_queue, roaming.follow_up_queue);

        let mut divergent = roaming.clone();
        divergent
            .steering_queue
            .push(sinomo_agent::AgentSteeringMessage::new(
                "concurrent steering",
            ));
        let error = select_resumable_agent_session(Some(roaming), Some(divergent))
            .expect_err("same revision with different private state must conflict");
        assert!(error.contains("divergent state at revision 3"), "{error}");

        let session_id = SessionId::generate();
        let mut transcript_branch = AgentSession::new(session_id, AgentRole::Deliberation)
            .with_agent_definition_key("main-agent");
        transcript_branch.state_revision = 2;
        transcript_branch
            .raw_transcript
            .push(AgentItem::UserMessage {
                item_id: TaskId::generate(),
                step_index: 0,
                content: "offline transcript append".to_string(),
                event_id: None,
            });
        transcript_branch.transcript = transcript_branch.raw_transcript.clone();
        let mut control_branch = AgentSession::new(
            transcript_branch.session_id.clone(),
            AgentRole::Deliberation,
        )
        .with_agent_definition_key("main-agent");
        control_branch.state_revision = 2;
        control_branch
            .follow_up_queue
            .push(sinomo_agent::AgentFollowUpMessage::new("offline follow-up"));
        let error = select_resumable_agent_session(Some(transcript_branch), Some(control_branch))
            .expect_err("concurrent transcript and control branches must conflict");
        assert!(error.contains("concurrent transcript and private control-state"));
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
    fn app_chat_resume_group_channel_does_not_add_or_reenable_process_default() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-membership-resume");
        let data_dir = SinomoDataDir::new(&root);
        let definition =
            AgentDefinition::custom("worker-agent", AgentRole::Main, "Work with the team.");
        let definition_store = sinomo_agent::AgentDefinitionSifsStore::open(Some(data_dir.root()))
            .expect("open agent definition store");
        sinomo_agent::AgentDefinitionStore::save_definition(&definition_store, &definition)
            .expect("save worker definition");
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");

        for (default_disabled, include_worker, expected_outer_agent) in [
            (false, true, "worker-agent"),
            (true, true, "worker-agent"),
            (false, false, "main-agent"),
            (true, false, "main-agent"),
        ] {
            let channel_id = SessionId::generate();
            let mut channel = AgentChannel::new_channel(channel_id.clone(), "Durable team");
            channel.ensure_user(AgentChannelUser::app_local());
            if default_disabled {
                let default_id = channel.ensure_participant("main-agent", "main-agent");
                assert!(channel.disable_participant(&default_id));
            }
            if include_worker {
                channel.ensure_participant("worker-agent", "Worker agent");
            }
            let durable_participants = channel.participants.clone();
            store.save_channel(&channel).expect("save durable channel");

            let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
            assert_eq!(chat.runtime_info().agent, "main-agent");
            chat.resume_session(&channel_id.to_string(), &[])
                .expect("resume durable group channel");

            assert_eq!(chat.runtime_info().agent, expected_outer_agent);
            assert_eq!(
                chat.channel.participants, durable_participants,
                "resume must select an enabled durable participant without changing membership"
            );
            assert_eq!(
                store
                    .load_channel(&channel_id)
                    .expect("reload durable channel")
                    .expect("durable channel")
                    .participants,
                durable_participants,
                "resume must not persist a process-default participant or enablement change"
            );
        }
    }

    #[test]
    fn app_chat_group_tool_runtime_update_preserves_membership_and_session_identity() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-tool-runtime-identity");
        let data_dir = SinomoDataDir::new(&root);
        let definition =
            AgentDefinition::custom("worker-agent", AgentRole::Main, "Work with the team.");
        let definition_store = sinomo_agent::AgentDefinitionSifsStore::open(Some(data_dir.root()))
            .expect("open agent definition store");
        sinomo_agent::AgentDefinitionStore::save_definition(&definition_store, &definition)
            .expect("save worker definition");
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Durable team");
        channel.ensure_user(AgentChannelUser::app_local());
        let default_id = channel.ensure_participant("main-agent", "main-agent");
        assert!(channel.disable_participant(&default_id));
        let worker_id = channel.ensure_participant("worker-agent", "Worker agent");
        let durable_participants = channel.participants.clone();
        let worker_session_id = channel
            .participant(&worker_id)
            .expect("worker participant")
            .agent_session_id
            .clone();
        assert_ne!(worker_session_id, channel_id);
        store.save_channel(&channel).expect("save durable channel");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume durable group channel");
        chat.set_tool_runtime_node(Some("updated-node".to_string()))
            .expect("update selected participant tool runtime");

        assert_eq!(chat.channel.participants.len(), durable_participants.len());
        for durable in &durable_participants {
            let updated = chat
                .channel
                .participant(&durable.participant_id)
                .expect("durable participant remains present");
            assert_eq!(updated.agent_key, durable.agent_key);
            assert_eq!(updated.agent_name, durable.agent_name);
            assert_eq!(updated.agent_session_id, durable.agent_session_id);
            assert_eq!(updated.enabled, durable.enabled);
            assert_eq!(updated.order, durable.order);
        }
        assert_eq!(
            chat.channel
                .participant(&worker_id)
                .expect("worker participant")
                .tool_runtime
                .sandbox_node_id
                .as_deref(),
            Some("updated-node")
        );
        assert_eq!(
            chat.channel
                .participant(&default_id)
                .expect("disabled default participant")
                .tool_runtime
                .sandbox_node_id,
            durable_participants
                .iter()
                .find(|participant| participant.participant_id == default_id)
                .expect("durable disabled participant")
                .tool_runtime
                .sandbox_node_id
        );

        let saved = store
            .load_channel(&channel_id)
            .expect("reload durable channel")
            .expect("durable channel");
        let saved_worker = saved.participant(&worker_id).expect("saved worker");
        assert_eq!(saved_worker.agent_session_id, worker_session_id);
        assert_eq!(
            saved_worker.tool_runtime.sandbox_node_id.as_deref(),
            Some("updated-node")
        );
        assert!(
            !saved
                .participant(&default_id)
                .expect("saved default")
                .enabled
        );
    }

    #[test]
    fn app_chat_group_tool_runtime_update_preserves_empty_and_all_disabled_membership() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-tool-runtime-empty");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");

        for all_disabled in [false, true] {
            let channel_id = SessionId::generate();
            let mut channel = AgentChannel::new_channel(channel_id.clone(), "Quiet team");
            channel.ensure_user(AgentChannelUser::app_local());
            if all_disabled {
                let participant_id = channel.ensure_participant("main-agent", "main-agent");
                assert!(channel.disable_participant(&participant_id));
            }
            let durable_participants = channel.participants.clone();
            store.save_channel(&channel).expect("save durable channel");

            let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
            chat.resume_session(&channel_id.to_string(), &[])
                .expect("resume quiet group channel");
            chat.set_tool_runtime_node(Some("outer-node".to_string()))
                .expect("update outer runtime without a member");

            assert_eq!(chat.channel.participants, durable_participants);
            assert_eq!(
                store
                    .load_channel(&channel_id)
                    .expect("reload durable channel")
                    .expect("durable channel")
                    .participants,
                durable_participants,
                "tool runtime selection must not create or re-enable group membership"
            );
            let outer_session = chat
                .runtime
                .handler()
                .agent_session_snapshot()
                .expect("outer session snapshot")
                .expect("outer session");
            assert!(
                FileAgentSessionStore::for_data_dir(&data_dir)
                    .load_session(&outer_session.session_id)
                    .expect("load persisted outer session")
                    .is_some(),
                "the runtime-only outer session should still persist locally"
            );
        }
    }

    #[test]
    fn app_chat_group_controls_skip_non_member_roaming_checkpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-control-checkpoint");
        let data_dir = SinomoDataDir::new(&root);
        let definition =
            AgentDefinition::custom("worker-agent", AgentRole::Main, "Work with the team.");
        let definition_store = sinomo_agent::AgentDefinitionSifsStore::open(Some(data_dir.root()))
            .expect("open agent definition store");
        sinomo_agent::AgentDefinitionStore::save_definition(&definition_store, &definition)
            .expect("save worker definition");
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Durable team");
        channel.ensure_user(AgentChannelUser::app_local());
        let worker_id = channel.ensure_participant("worker-agent", "Worker agent");
        let durable_participants = channel.participants.clone();
        store.save_channel(&channel).expect("save durable channel");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume durable group channel");
        let steering = chat.steer("focus on correctness").expect("queue steering");
        let steering_id = steering
            .queued_input
            .expect("steering queue item")
            .input_id
            .parse::<TaskId>()
            .expect("steering id");
        chat.follow_up("run the focused checks")
            .expect("queue follow-up");
        chat.remove_queued_input(steering_id)
            .expect("remove steering");
        chat.interrupt("operator requested stop")
            .expect("request interrupt");

        assert_eq!(chat.channel.participants, durable_participants);
        let outer_session = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&channel_id)
            .expect("load outer session")
            .expect("persisted outer session");
        assert!(outer_session.steering_queue.is_empty());
        assert_eq!(outer_session.follow_up_queue.len(), 1);
        let worker = chat.channel.participant(&worker_id).expect("worker");
        assert!(
            chat.session_checkpoint_store
                .load_latest(&chat.channel, worker)
                .expect("load worker checkpoint")
                .is_none(),
            "outer channel control state must not be published as the worker checkpoint"
        );
    }

    #[test]
    fn app_chat_group_controls_publish_exact_member_checkpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-control-member");
        let data_dir = SinomoDataDir::new(&root);
        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        chat.channel.channel_kind = AgentChannelKind::Channel;
        let participant = chat
            .primary_participant()
            .expect("primary participant")
            .clone();
        assert_eq!(
            participant.agent_session_id,
            chat.runtime.session().session_id,
            "fresh chat identity should be exact before promoting it to a group"
        );
        chat.save_channel().expect("save promoted group channel");

        chat.steer("preserve the exact member checkpoint")
            .expect("queue steering");

        let checkpoint = chat
            .session_checkpoint_store
            .load_latest(&chat.channel, &participant)
            .expect("load participant checkpoint")
            .expect("exact participant checkpoint");
        assert_eq!(checkpoint.session_id, participant.agent_session_id);
        assert_eq!(
            checkpoint.agent_definition_key.as_deref(),
            Some(participant.agent_key.as_str())
        );
        assert_eq!(checkpoint.steering_queue.len(), 1);
    }

    #[test]
    fn app_chat_resume_restores_canonical_projection_and_raw_history() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-canonical-resume");
        let data_dir = SinomoDataDir::new(&root);
        let session_id = SessionId::generate();
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let mut channel = AgentChannel::new(session_id.clone(), "Compacted chat");
        let visible = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "visible tail",
            AgentChannelSource::app(),
        );
        channel_store
            .append_message(&session_id, &visible)
            .expect("append visible message");
        channel_store.save_channel(&channel).expect("save channel");

        let mut persisted = AgentSession::new(session_id.clone(), AgentRole::Main)
            .with_agent_definition_key("main-agent");
        let raw_item_id = TaskId::generate();
        persisted.raw_transcript.push(AgentItem::UserMessage {
            item_id: raw_item_id.clone(),
            step_index: 0,
            content: "raw pre-compaction constraint".to_string(),
            event_id: None,
        });
        persisted.transcript.push(AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 1,
            content: "visible tail".to_string(),
            event_id: None,
        });
        persisted.projection = Some(sinomo_agent::AgentSessionProjection {
            projection_id: TaskId::generate(),
            previous_projection_id: None,
            created_at: time::OffsetDateTime::now_utc(),
            reason: "context budget".to_string(),
            summary: "preserve the pre-compaction constraint".to_string(),
            source_item_count: 2,
            source_item_ids: vec![raw_item_id],
            retained_item_ids: Vec::new(),
            working_state: sinomo_agent::AgentWorkingState::default(),
        });
        persisted
            .steering_queue
            .push(sinomo_agent::AgentSteeringMessage::new(
                "queued before restart",
            ));
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&persisted)
            .expect("save canonical session");

        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        chat.resume_session(&session_id.to_string(), &[])
            .expect("resume canonical session");
        let resumed = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("runtime attached");

        assert!(
            resumed
                .raw_transcript
                .items
                .starts_with(&persisted.raw_transcript.items),
            "canonical raw history must be preserved before channel reconciliation"
        );
        assert_eq!(resumed.projection, persisted.projection);
        assert_eq!(resumed.steering_queue, persisted.steering_queue);
        let visible_id = channel_message_agent_item_id(&visible.message_id);
        let reconciled_count = resumed
            .raw_transcript
            .items
            .iter()
            .filter(|item| {
                matches!(
                    item,
                    AgentItem::UserMessage { item_id, .. }
                        | AgentItem::AssistantText { item_id, .. }
                        if item_id == &visible_id
                )
            })
            .count();
        assert_eq!(
            reconciled_count, 1,
            "visible channel message is reconciled once"
        );

        let reconciled_len = resumed.raw_transcript.items.len();
        chat.resume_session(&session_id.to_string(), &[])
            .expect("resume canonical session again");
        let resumed_again = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("runtime attached");
        assert_eq!(resumed_again.raw_transcript.items.len(), reconciled_len);
        assert_eq!(resumed_again.projection, persisted.projection);
    }

    #[test]
    fn app_chat_resume_restores_exact_coding_agent_and_workspace() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-coding-resume");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("create coding workspace");
        let workspace = fs::canonicalize(workspace).expect("canonical coding workspace");
        let session_id = SessionId::generate();

        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let mut channel = AgentChannel::new(session_id.clone(), "Coding chat");
        channel.ensure_user(AgentChannelUser::app_local());
        let participant_id = channel.ensure_participant("coding-agent", "Coding agent");
        channel
            .participant_mut(&participant_id)
            .expect("coding participant")
            .agent_session_id = session_id.clone();
        channel_store.save_channel(&channel).expect("save channel");

        let mut persisted = AgentSession::new(session_id.clone(), AgentRole::Deliberation)
            .with_agent_definition_key("coding-agent");
        persisted.workspace_binding =
            Some(sinomo_agent::WorkspaceRef::local_path(workspace.clone()));
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&persisted)
            .expect("save coding session");

        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        chat.resume_session(&session_id.to_string(), &[])
            .expect("resume coding session");

        assert_eq!(chat.runtime_info().agent, "coding-agent");
        assert_eq!(
            chat.tool_runtime().workspace_root.as_ref(),
            Some(&workspace)
        );
        let resumed = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("runtime attached");
        assert_eq!(
            resumed.agent_definition_key.as_deref(),
            Some("coding-agent")
        );
        assert_eq!(resumed.workspace_binding, persisted.workspace_binding);
    }

    #[test]
    fn app_chat_reopens_the_enabled_agent_after_a_direct_switch() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-switched-agent-resume");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let workspace = root.join("plain-workspace");
        fs::create_dir_all(&workspace).expect("create generic workspace");
        let workspace = fs::canonicalize(workspace).expect("canonical generic workspace");
        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        let session_id = chat.session_id();

        chat.set_agent("coding-agent")
            .expect("switch direct chat to coding agent");
        chat.set_tool_runtime(ChatToolRuntimeSelection {
            workspace_root: Some(workspace.clone()),
            ..ChatToolRuntimeSelection::default()
        })
        .expect("bind generic workspace before using coding tools");
        assert_eq!(chat.runtime_info().agent, "coding-agent");
        drop(chat);

        let mut reopened = AgentChat::new(data_dir, false).expect("reopen chat runtime");
        reopened
            .resume_session(&session_id, &[])
            .expect("resume switched direct chat");
        assert_eq!(reopened.runtime_info().agent, "coding-agent");
        assert_eq!(
            reopened.tool_runtime().workspace_root.as_deref(),
            Some(workspace.as_path()),
            "direct-agent resume must retain the selected generic workspace"
        );
        let enabled = reopened
            .channel
            .enabled_participants_in_order()
            .into_iter()
            .map(|participant| participant.agent_key.clone())
            .collect::<Vec<_>>();
        assert_eq!(enabled, vec!["coding-agent".to_string()]);
    }

    #[test]
    fn app_chat_roams_latest_canonical_session_checkpoint_across_data_dirs() {
        let root_a = sinomo_testdir::TestDir::new("sinomo-app-chat-roaming-session-a");
        let root_b = sinomo_testdir::TestDir::new("sinomo-app-chat-roaming-session-b");
        let data_dir_a = SinomoDataDir::new(&root_a);
        let data_dir_b = SinomoDataDir::new(&root_b);
        let channel_id = SessionId::generate();
        let shared_channel_store =
            SifsAgentChannelStore::open(&data_dir_a).expect("open shared channel store");
        let mut channel = AgentChannel::new(channel_id.clone(), "Roaming compacted chat");
        let participant_id = channel.ensure_participant("main-agent", "main-agent");
        channel
            .participant_mut(&participant_id)
            .expect("main participant")
            .agent_session_id = channel_id.clone();
        let visible = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "visible shared tail",
            AgentChannelSource::app(),
        );
        shared_channel_store
            .append_message(&channel_id, &visible)
            .expect("append shared visible message");
        shared_channel_store
            .save_channel(&channel)
            .expect("save shared channel");
        drop(shared_channel_store);

        let mut seeded = AgentSession::new(channel_id.clone(), AgentRole::Main)
            .with_agent_definition_key("main-agent");
        let raw_item_id = TaskId::generate();
        seeded.raw_transcript.push(AgentItem::UserMessage {
            item_id: raw_item_id.clone(),
            step_index: 0,
            content: "private pre-compaction constraint".to_string(),
            event_id: None,
        });
        seeded.transcript.push(AgentItem::UserMessage {
            item_id: raw_item_id.clone(),
            step_index: 0,
            content: "private pre-compaction constraint".to_string(),
            event_id: None,
        });
        let mut working_state = sinomo_agent::AgentWorkingState::default();
        working_state.active_objective = Some("finish roaming verification".to_string());
        working_state.files_modified = vec!["src/roaming.rs".to_string()];
        seeded.projection = Some(sinomo_agent::AgentSessionProjection {
            projection_id: TaskId::generate(),
            previous_projection_id: None,
            created_at: time::OffsetDateTime::now_utc(),
            reason: "context budget".to_string(),
            summary: "first roaming checkpoint".to_string(),
            source_item_count: 1,
            source_item_ids: vec![raw_item_id],
            retained_item_ids: Vec::new(),
            working_state,
        });
        seeded
            .steering_queue
            .push(sinomo_agent::AgentSteeringMessage::new(
                "queued on device A",
            ));
        FileAgentSessionStore::for_data_dir(&data_dir_a)
            .save_session(&seeded)
            .expect("save device A canonical session");

        let mut chat_a = AgentChat::with_runtime_control_registry_and_stores(
            data_dir_a.clone(),
            false,
            AgentRuntimeControlRegistry::new(),
            Box::new(
                SifsAgentChannelStore::open(&data_dir_a)
                    .expect("reopen shared channel store for A"),
            ),
            SifsAgentSessionCheckpointStore::open(&data_dir_a)
                .expect("open shared checkpoint store for A"),
        )
        .expect("device A chat runtime");
        chat_a
            .resume_session(&channel_id.to_string(), &[])
            .expect("device A resume and checkpoint");

        // Publish a second canonical version containing post-compaction working
        // state and a durable tool outcome; B must choose this newest record.
        let mut newest = chat_a
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("device A runtime snapshot")
            .expect("device A session attached");
        let tool_result_id = TaskId::generate();
        newest.raw_transcript.push(AgentItem::ToolResult {
            item_id: tool_result_id.clone(),
            step_index: 2,
            call_index: 0,
            call_id: TaskId::generate(),
            native_call_id: Some("call-roaming".to_string()),
            tool_name: Some("read_file".to_string()),
            status: sinomo_agent::AgentToolStatus::Succeeded,
            summary: "read shared source".to_string(),
            output: serde_json::json!({"content": "durable tool evidence"}),
            evidence: sinomo_agent::AgentToolEvidence::default(),
        });
        let previous_projection_id = newest
            .projection
            .as_ref()
            .map(|projection| projection.projection_id.clone());
        let mut newest_working_state = newest
            .projection
            .as_ref()
            .map(|projection| projection.working_state.clone())
            .unwrap_or_default();
        newest_working_state.next_action = Some("resume on device B".to_string());
        newest.projection = Some(sinomo_agent::AgentSessionProjection {
            projection_id: TaskId::generate(),
            previous_projection_id,
            created_at: time::OffsetDateTime::now_utc(),
            reason: "manual roaming checkpoint".to_string(),
            summary: "newest roaming checkpoint".to_string(),
            source_item_count: newest.raw_transcript.items.len(),
            source_item_ids: vec![tool_result_id],
            retained_item_ids: Vec::new(),
            working_state: newest_working_state,
        });
        FileAgentSessionStore::for_data_dir(&data_dir_a)
            .save_session(&newest)
            .expect("save newest device A session");
        let channel_a = chat_a.channel.clone();
        let participant_a = channel_a
            .participant(&participant_id)
            .expect("device A participant");
        chat_a
            .session_checkpoint_store
            .publish(&channel_a, participant_a, &newest)
            .expect("publish newest roaming checkpoint");
        drop(chat_a);

        assert!(
            FileAgentSessionStore::for_data_dir(&data_dir_b)
                .load_session(&channel_id)
                .expect("load absent device B session")
                .is_none(),
            "device B starts without a node-local canonical session"
        );
        let mut chat_b = AgentChat::with_runtime_control_registry_and_stores(
            data_dir_b.clone(),
            false,
            AgentRuntimeControlRegistry::new(),
            Box::new(
                SifsAgentChannelStore::open(&data_dir_a).expect("open shared channel store for B"),
            ),
            SifsAgentSessionCheckpointStore::open(&data_dir_a)
                .expect("open shared checkpoint store for B"),
        )
        .expect("device B chat runtime");
        chat_b
            .resume_session(&channel_id.to_string(), &[])
            .expect("device B roaming resume");
        let resumed_b = chat_b
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("device B runtime snapshot")
            .expect("device B session attached");

        assert_eq!(resumed_b.raw_transcript, newest.raw_transcript);
        assert_eq!(resumed_b.transcript, newest.transcript);
        assert_eq!(resumed_b.projection, newest.projection);
        assert_eq!(resumed_b.steering_queue, newest.steering_queue);
        assert_eq!(
            FileAgentSessionStore::for_data_dir(&data_dir_b)
                .load_session(&channel_id)
                .expect("load device B local session")
                .expect("device B local session persisted"),
            resumed_b,
            "the roaming checkpoint is materialized into B's local durable store"
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
    fn app_chat_workspace_selection_is_persisted_in_the_agent_session() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-workspace-binding");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let workspace = root.join("selected-directory");
        fs::create_dir_all(&workspace).expect("create selected directory");
        let canonical_workspace = fs::canonicalize(&workspace).expect("canonical workspace");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.set_agent("coding-agent").expect("select coding agent");
        let selection = chat
            .set_tool_runtime(ChatToolRuntimeSelection {
                workspace_root: Some(workspace),
                ..ChatToolRuntimeSelection::default()
            })
            .expect("bind workspace");

        assert_eq!(
            selection.workspace_root.as_deref(),
            Some(canonical_workspace.as_path())
        );
        let session = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("attached session");
        assert_eq!(
            session
                .workspace_binding
                .as_ref()
                .and_then(|binding| binding.local_root.as_deref()),
            Some(canonical_workspace.as_path())
        );
        let persisted = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&session.session_id)
            .expect("load session")
            .expect("persisted session");
        assert_eq!(persisted.workspace_binding, session.workspace_binding);
    }

    #[test]
    fn portable_sifs_workspace_binding_remaps_across_device_mounts() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-portable-sifs-workspace");
        let mount_a = root.join("device-a/data/mounts/sifs/by-process/111");
        let mount_b = root.join("device-b/data/mounts/sifs/by-process/222");
        let workspace_a = mount_a.join("teams/shared-workspace");
        let workspace_b = mount_b.join("teams/shared-workspace");
        fs::create_dir_all(&workspace_a).expect("create device A workspace");
        fs::create_dir_all(&workspace_b).expect("create device B workspace");

        let binding_a =
            crate::workspace_ref_for_root_with_verified_sifs_mount(&workspace_a, Some(&mount_a))
                .expect("create portable device A binding");
        let binding_b =
            crate::workspace_ref_for_root_with_verified_sifs_mount(&workspace_b, Some(&mount_b))
                .expect("create portable device B binding");
        assert_eq!(binding_a.local_root, None);
        assert_eq!(
            binding_a.sifs_root.as_deref(),
            Some("/teams/shared-workspace")
        );
        assert_eq!(binding_a.write_policy.as_deref(), Some("live-posix"));
        let session_id = SessionId::generate();
        crate::validate_agent_session_workspace_binding(&session_id, &binding_a, &binding_b)
            .expect("same logical SIFS root matches across device-local prefixes");

        let store =
            FileAgentSessionStore::for_data_dir(&SinomoDataDir::new(root.join("session-store")));
        let (created, resumed) = crate::load_or_create_agent_run_session(
            &store,
            session_id.clone(),
            AgentRole::Deliberation,
            "coding-agent",
            Some(&binding_a),
        )
        .expect("persist device A logical binding");
        assert!(!resumed);
        assert_eq!(created.workspace_binding.as_ref(), Some(&binding_a));
        let (resumed_session, resumed) = crate::load_or_create_agent_run_session(
            &store,
            session_id.clone(),
            AgentRole::Deliberation,
            "coding-agent",
            Some(&binding_b),
        )
        .expect("resume same logical workspace from device B prefix");
        assert!(resumed);
        assert_eq!(resumed_session.workspace_binding.as_ref(), Some(&binding_a));

        let remapped = crate::workspace_binding_root_with_verified_sifs_mount(
            &session_id,
            &binding_a,
            &mount_b,
        )
        .expect("remap binding on device B");
        assert_eq!(
            remapped,
            fs::canonicalize(&workspace_b).expect("canonical B workspace")
        );

        let other_workspace_b = mount_b.join("teams/other-workspace");
        fs::create_dir_all(&other_workspace_b).expect("create mismatched workspace");
        let mismatch = crate::workspace_ref_for_root_with_verified_sifs_mount(
            &other_workspace_b,
            Some(&mount_b),
        )
        .expect("create mismatched binding");
        let error =
            crate::validate_agent_session_workspace_binding(&session_id, &binding_a, &mismatch)
                .expect_err("different logical SIFS roots must not resume as the same workspace");
        assert!(error.to_string().contains("/teams/shared-workspace"));
        assert!(error.to_string().contains("/teams/other-workspace"));
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
        let outer_agent = chat.runtime_info().agent;
        let outer_session_key = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("outer session snapshot")
            .expect("outer session")
            .agent_definition_key;
        chat.set_agent("worker-agent").expect("set channel agent");
        assert_eq!(chat.runtime_info().agent, outer_agent);
        assert_eq!(
            chat.runtime
                .handler()
                .agent_session_snapshot()
                .expect("outer session snapshot after participant add")
                .expect("outer session after participant add")
                .agent_definition_key,
            outer_session_key,
            "adding a group participant must not rebind the outer session"
        );

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
    fn sifs_channel_snapshot_merge_rejects_stale_metadata_and_resurrection() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-stale-snapshot");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let base = time::OffsetDateTime::now_utc();
        let mut stale = AgentChannel::new_channel(channel_id.clone(), "Stale title");
        stale.updated_at = base;
        let mut authoritative = stale.clone();
        authoritative.title = "Current title".to_string();
        authoritative.updated_at = base + time::Duration::seconds(2);
        authoritative.deleted_at = Some(base + time::Duration::seconds(3));

        store
            .save_channel(&authoritative)
            .expect("save authoritative tombstone");
        store
            .save_channel(&stale)
            .expect("merge stale snapshot without resurrection");

        let saved = store
            .load_channel_snapshot_only(&channel_id)
            .expect("load snapshot")
            .expect("saved snapshot");
        assert_eq!(saved.title, "Current title");
        assert_eq!(saved.deleted_at, authoritative.deleted_at);
        assert_eq!(saved.updated_at, authoritative.updated_at);
    }

    #[test]
    fn channel_snapshot_merge_preserves_concurrent_distinct_participants_and_users() {
        let channel_id = SessionId::generate();
        let base = time::OffsetDateTime::now_utc();
        let mut left = AgentChannel::new_channel(channel_id.clone(), "Shared channel");
        left.ensure_user(AgentChannelUser {
            user_id: "user.left".to_string(),
            display_name: "Left".to_string(),
            source: AgentChannelSource::app(),
        });
        left.ensure_participant("left-agent", "Left agent");
        left.updated_at = base + time::Duration::seconds(2);

        let mut right = AgentChannel::new_channel(channel_id, "Shared channel");
        right.ensure_user(AgentChannelUser {
            user_id: "user.right".to_string(),
            display_name: "Right".to_string(),
            source: AgentChannelSource::app(),
        });
        right.ensure_participant("right-agent", "Right agent");
        right.updated_at = base + time::Duration::seconds(1);

        let merged = merge_channel_snapshot(left, right).expect("merge channel snapshots");
        assert!(merged.users.iter().any(|user| user.user_id == "user.left"));
        assert!(merged.users.iter().any(|user| user.user_id == "user.right"));
        assert!(
            merged
                .participants
                .iter()
                .any(|participant| participant.agent_key == "left-agent")
        );
        assert!(
            merged
                .participants
                .iter()
                .any(|participant| participant.agent_key == "right-agent")
        );
    }

    #[test]
    fn channel_snapshot_merge_bounds_observed_ids_to_retained_transcript() {
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id, "Long channel");
        let participant_id = channel.ensure_participant("main-agent", "Main");
        for index in 0..(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT + 25) {
            let message = channel.append_user_message(
                AGENT_CHANNEL_APP_USER_ID,
                "User",
                format!("message {index}"),
                AgentChannelSource::app(),
            );
            channel
                .participant_mut(&participant_id)
                .expect("participant")
                .observed_message_ids
                .push(message.message_id);
        }
        let mut stale = channel.clone();
        stale.updated_at -= time::Duration::seconds(1);

        let merged = merge_channel_snapshot(channel, stale).expect("merge long channel");
        let participant = merged
            .participant(&participant_id)
            .expect("merged participant");
        assert_eq!(
            merged.transcript.len(),
            AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT
        );
        assert_eq!(
            participant.observed_message_ids.len(),
            AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT
        );
        assert!(participant.observed_message_ids.iter().all(|message_id| {
            merged
                .transcript
                .iter()
                .any(|message| &message.message_id == message_id)
        }));
    }

    #[test]
    fn shared_channel_runtime_outbox_repairs_a_crash_before_message_materialization() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-channel-turn-outbox");
        let data_dir = SinomoDataDir::new(&root);
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Crash recovery");
        channel.ensure_user(AgentChannelUser::app_local());
        channel.ensure_participant("main-agent", "main-agent");
        let store = CrashAfterTurnChannelStore::new(
            SifsAgentChannelStore::open(&data_dir).expect("open channel store"),
        );
        let mut runtime =
            AgentChannelRuntime::new(channel, store, FixedChannelReply("durable answer"))
                .with_loop_config(AgentChannelLoopConfig {
                    max_agent_messages_per_ingress: 1,
                });

        // The failpoint rejects the first response materialization after the
        // runtime has durably committed the turn and its ordered outbox.
        let result = runtime.send_user_message("recover this turn", &mut |_| {});
        assert!(
            matches!(
                result,
                Err(sinomo_agent::AgentChannelRuntimeError::Store { .. })
            ),
            "the failpoint must interrupt message materialization: {result:?}"
        );
        drop(runtime);

        let reopened = SifsAgentChannelStore::open(&data_dir).expect("reopen channel store");
        let turns = reopened
            .load_turns(&channel_id)
            .expect("load committed turn");
        assert_eq!(
            turns.len(),
            1,
            "the response must already have a turn audit"
        );
        assert_eq!(turns[0].outbox_messages.len(), 1);
        let response = turns[0].outbox_messages[0].clone();
        assert_eq!(response.content, "durable answer");
        assert_eq!(
            turns[0].response_message_ids,
            vec![response.message_id.clone()]
        );
        assert!(
            reopened
                .load_all_messages(&channel_id)
                .expect("load messages before recovery")
                .iter()
                .all(|message| message.message_id != response.message_id),
            "the failpoint must leave the response unmaterialized"
        );

        let recovered = reopened
            .load_channel(&channel_id)
            .expect("load recovered channel")
            .expect("recovered channel exists");
        assert_eq!(
            recovered
                .transcript
                .iter()
                .filter(|message| message.message_id == response.message_id)
                .count(),
            1,
            "the committed response is materialized exactly once"
        );
        assert_eq!(
            recovered
                .transcript
                .iter()
                .find(|message| message.message_id == response.message_id)
                .map(|message| message.content.as_str()),
            Some("durable answer")
        );

        let recovered_again = reopened
            .load_channel(&channel_id)
            .expect("reload recovered channel")
            .expect("recovered channel exists again");
        assert_eq!(
            recovered_again
                .transcript
                .iter()
                .filter(|message| message.message_id == response.message_id)
                .count(),
            1,
            "replaying the turn outbox must stay idempotent"
        );
    }

    #[test]
    fn sifs_channel_messages_are_immutable_and_recent_snapshot_is_bounded() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-immutable-log");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Long running channel");
        channel.ensure_user(AgentChannelUser::app_local());

        let total = AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT + 25;
        let mut first_message_id = None;
        for index in 0..total {
            let message = channel.append_user_message(
                AGENT_CHANNEL_APP_USER_ID,
                "User",
                format!("message {index}"),
                AgentChannelSource::app(),
            );
            first_message_id.get_or_insert_with(|| message.message_id.clone());
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
            outbox_messages: Vec::new(),
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

        let first_record = SifsBrowser::open(&data_dir)
            .expect("open SIFS")
            .read_file(&SifsAgentChannelStore::message_record_path(
                &channel_id,
                &first_message_id.expect("first message id"),
            ))
            .expect("first immutable message record");
        assert!(
            String::from_utf8_lossy(&first_record).contains("message 0"),
            "immutable records should retain the beginning of the durable timeline"
        );
        let browser = SifsBrowser::open(&data_dir).expect("open SIFS");
        assert!(
            browser
                .read_file(&SifsAgentChannelStore::message_index_path(&channel_id))
                .is_err(),
            "new message appends must not create a shared mutable index"
        );
        assert!(
            browser
                .read_file(&SifsAgentChannelStore::turn_index_path(&channel_id))
                .is_err(),
            "new turn appends must not create a shared mutable index"
        );
        assert!(
            browser
                .read_file(&SifsAgentChannelStore::turn_record_path(
                    &channel_id,
                    &turn.turn_id,
                ))
                .is_ok(),
            "turns should be stored as immutable records"
        );
        assert_eq!(
            store.load_turns(&channel_id).expect("load turns"),
            vec![turn]
        );

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
        assert_eq!(loaded.next_sequence, total as u64 + 1);

        let recent_page = store
            .load_recent_message_page(&channel_id, AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT)
            .expect("load recent page");
        assert_eq!(recent_page.total_messages, total as u64);
        assert_eq!(recent_page.start_sequence, Some(26));
        assert_eq!(recent_page.end_sequence, Some(425));
        assert!(recent_page.has_older);
        assert!(!recent_page.has_newer);

        let older_page = store
            .load_message_page_before(
                &channel_id,
                recent_page.start_sequence.expect("recent start"),
                AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT,
            )
            .expect("load older page");
        assert_eq!(older_page.start_sequence, Some(1));
        assert_eq!(older_page.end_sequence, Some(25));
        assert!(!older_page.has_older);
        assert!(older_page.has_newer);

        let newer_page = store
            .load_message_page_from(
                &channel_id,
                older_page
                    .end_sequence
                    .expect("older end")
                    .saturating_add(1),
                AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT,
            )
            .expect("load newer page");
        assert_eq!(newer_page.start_sequence, Some(26));
        assert_eq!(newer_page.end_sequence, Some(425));
        assert!(newer_page.has_older);
        assert!(!newer_page.has_newer);

        let after_end = store
            .load_message_page_from(&channel_id, total as u64 + 1, 10)
            .expect("load after end page");
        assert!(after_end.messages.is_empty());
        assert!(after_end.has_older);
        assert!(!after_end.has_newer);

        let overview = store
            .load_timeline_overview(&channel_id)
            .expect("load timeline overview");
        assert_eq!(overview.total_messages, total as u64);
        assert_eq!(overview.oldest_sequence, Some(1));
        assert_eq!(overview.newest_sequence, Some(425));
        assert_eq!(overview.segments.len(), 4);
        assert_eq!(overview.segments[0].start_sequence, 1);
        assert_eq!(overview.segments[0].end_sequence, 128);
        assert!(
            overview.segments[0]
                .preview
                .as_deref()
                .is_some_and(|preview| preview.contains("message 0"))
        );
    }

    #[test]
    fn sifs_channel_store_merges_legacy_and_immutable_records() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-log-migration");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Migrated channel");
        let first = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "legacy first",
            AgentChannelSource::app(),
        );
        let second = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "legacy second",
            AgentChannelSource::app(),
        );
        let legacy_turn = channel_turn(&channel_id, &second.message_id);
        store.save_channel(&channel).expect("save channel snapshot");

        let browser = SifsBrowser::open(&data_dir).expect("open SIFS browser");
        browser
            .create_dir_all(&SifsAgentChannelStore::message_chunks_dir(&channel_id))
            .expect("create legacy message dir");
        let legacy_messages = json_lines(&[first.clone(), second.clone()]);
        browser
            .write_file(
                &SifsAgentChannelStore::message_chunk_path(&channel_id, 1),
                &legacy_messages,
            )
            .expect("write legacy message chunk");
        browser
            .write_file(
                &SifsAgentChannelStore::message_index_path(&channel_id),
                br#"{"schema_version":1,"entry_count":1,"updated_unix":0}"#,
            )
            .expect("write deliberately stale legacy message index");
        browser
            .create_dir_all(&SifsAgentChannelStore::turn_chunks_dir(&channel_id))
            .expect("create legacy turn dir");
        let legacy_turns = json_lines(std::slice::from_ref(&legacy_turn));
        browser
            .write_file(
                &SifsAgentChannelStore::turn_chunk_path(&channel_id, 0),
                &legacy_turns,
            )
            .expect("write legacy turn chunk");
        browser
            .write_file(
                &SifsAgentChannelStore::turn_index_path(&channel_id),
                br#"{"schema_version":1,"entry_count":0,"updated_unix":0}"#,
            )
            .expect("write deliberately stale legacy turn index");

        store
            .append_message(&channel_id, &second)
            .expect("write duplicate immutable message");
        store
            .append_turn(&channel_id, &legacy_turn)
            .expect("write duplicate immutable turn");
        let third = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "immutable third",
            AgentChannelSource::app(),
        );
        store
            .append_message(&channel_id, &third)
            .expect("write new immutable message");

        let loaded = store
            .load_channel(&channel_id)
            .expect("load migrated channel")
            .expect("migrated channel exists");
        assert_eq!(
            loaded
                .transcript
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>(),
            vec!["legacy first", "legacy second", "immutable third"]
        );
        assert_eq!(
            loaded
                .transcript
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(loaded.next_sequence, 4);
        assert_eq!(
            store.load_turns(&channel_id).expect("load merged turns"),
            vec![legacy_turn]
        );
        assert_eq!(
            browser
                .read_file(&SifsAgentChannelStore::message_chunk_path(&channel_id, 1))
                .expect("legacy chunk remains readable"),
            legacy_messages,
            "new appends must not rewrite legacy JSONL chunks"
        );
    }

    #[test]
    fn sifs_channel_store_reconciles_two_stale_store_snapshots() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-stale-stores");
        let data_dir = SinomoDataDir::new(&root);
        let store_a = SifsAgentChannelStore::open(&data_dir).expect("open store A");
        let store_b = SifsAgentChannelStore::open(&data_dir).expect("open store B");
        let channel_id = SessionId::generate();
        store_a
            .save_channel(&AgentChannel::new_channel(
                channel_id.clone(),
                "Shared channel",
            ))
            .expect("save base channel");
        let mut channel_a = store_a
            .load_channel(&channel_id)
            .expect("load channel A")
            .expect("channel A exists");
        let mut channel_b = store_b
            .load_channel(&channel_id)
            .expect("load channel B")
            .expect("channel B exists");

        let message_a = channel_a.append_user_message(
            "device-a",
            "Device A",
            "message from A",
            AgentChannelSource::app(),
        );
        let message_b = channel_b.append_user_message(
            "device-b",
            "Device B",
            "message from B",
            AgentChannelSource::app(),
        );
        assert_eq!(
            message_a.sequence, message_b.sequence,
            "both stores are stale"
        );
        let turn_a = channel_turn(&channel_id, &message_a.message_id);
        let turn_b = channel_turn(&channel_id, &message_b.message_id);

        store_a
            .append_message(&channel_id, &message_a)
            .expect("append A");
        store_a.append_turn(&channel_id, &turn_a).expect("turn A");
        store_a.save_channel(&channel_a).expect("snapshot A");
        store_b
            .append_message(&channel_id, &message_b)
            .expect("append B");
        store_b.append_turn(&channel_id, &turn_b).expect("turn B");
        store_b
            .save_channel(&channel_b)
            .expect("stale snapshot B overwrites cache");

        let loaded = store_a
            .load_channel(&channel_id)
            .expect("load reconciled channel")
            .expect("reconciled channel exists");
        let contents = loaded
            .transcript
            .iter()
            .map(|message| message.content.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            contents,
            BTreeSet::from(["message from A", "message from B"])
        );
        assert_eq!(
            loaded
                .transcript
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(loaded.next_sequence, 3);
        let turns = store_a.load_turns(&channel_id).expect("load both turns");
        assert_eq!(turns.len(), 2);
        assert_eq!(
            turns
                .iter()
                .map(|turn| turn.turn_id.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([turn_a.turn_id, turn_b.turn_id])
        );
    }

    #[test]
    fn sifs_channel_store_keeps_concurrent_immutable_writers() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-concurrent-writers");
        let data_dir = SinomoDataDir::new(&root);
        let setup = SifsAgentChannelStore::open(&data_dir).expect("open setup store");
        let channel_id = SessionId::generate();
        let base = AgentChannel::new_channel(channel_id.clone(), "Concurrent channel");
        setup.save_channel(&base).expect("save base channel");
        let mut left = base.clone();
        let mut right = base;
        let left_message = left.append_user_message(
            "left-device",
            "Left",
            "left concurrent message",
            AgentChannelSource::app(),
        );
        let right_message = right.append_user_message(
            "right-device",
            "Right",
            "right concurrent message",
            AgentChannelSource::app(),
        );
        let left_turn = channel_turn(&channel_id, &left_message.message_id);
        let right_turn = channel_turn(&channel_id, &right_message.message_id);
        let browser = SifsBrowser::open(&data_dir).expect("open setup browser");
        browser
            .create_dir_all(&SifsAgentChannelStore::message_records_dir(&channel_id))
            .expect("create message records dir");
        browser
            .create_dir_all(&SifsAgentChannelStore::turn_records_dir(&channel_id))
            .expect("create turn records dir");

        let barrier = Arc::new(Barrier::new(3));
        let spawn_writer = |message: AgentChannelMessage, turn: AgentChannelTurn| {
            let data_dir = data_dir.clone();
            let channel_id = channel_id.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let store = SifsAgentChannelStore::open(&data_dir).expect("open writer store");
                barrier.wait();
                store
                    .append_message(&channel_id, &message)
                    .expect("append concurrent message");
                store
                    .append_turn(&channel_id, &turn)
                    .expect("append concurrent turn");
            })
        };
        let left_writer = spawn_writer(left_message, left_turn);
        let right_writer = spawn_writer(right_message, right_turn);
        barrier.wait();
        left_writer.join().expect("left writer joins");
        right_writer.join().expect("right writer joins");

        let loaded = setup
            .load_channel(&channel_id)
            .expect("load concurrent channel")
            .expect("concurrent channel exists");
        assert_eq!(loaded.transcript.len(), 2);
        assert_eq!(
            loaded
                .transcript
                .iter()
                .map(|message| message.content.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["left concurrent message", "right concurrent message"])
        );
        assert_eq!(
            loaded
                .transcript
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(setup.load_turns(&channel_id).expect("load turns").len(), 2);
    }

    #[test]
    fn sifs_channel_metadata_records_preserve_barrier_concurrent_participant_adds() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-concurrent-metadata");
        let data_dir = SinomoDataDir::new(&root);
        let setup = SifsAgentChannelStore::open(&data_dir).expect("open setup store");
        let channel_id = SessionId::generate();
        let base = AgentChannel::new_channel(channel_id.clone(), "Shared channel");
        setup.save_channel(&base).expect("save base channel");

        let mut left = setup
            .load_channel(&channel_id)
            .expect("load left base")
            .expect("left base exists");
        let mut right = left.clone();
        left.ensure_participant("left-agent", "Left agent");
        right.ensure_participant("right-agent", "Right agent");

        let barrier = Arc::new(Barrier::new(3));
        let spawn_save = |channel: AgentChannel| {
            let data_dir = data_dir.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let store = SifsAgentChannelStore::open(&data_dir).expect("open metadata writer");
                barrier.wait();
                store.save_channel(&channel).expect("save metadata record");
            })
        };
        let left_writer = spawn_save(left);
        let right_writer = spawn_save(right);
        barrier.wait();
        left_writer.join().expect("left metadata writer joins");
        right_writer.join().expect("right metadata writer joins");

        let loaded = SifsAgentChannelStore::open(&data_dir)
            .expect("reopen channel store")
            .load_channel(&channel_id)
            .expect("load reconciled metadata")
            .expect("channel exists");
        assert!(
            loaded
                .participants
                .iter()
                .any(|participant| participant.agent_key == "left-agent")
        );
        assert!(
            loaded
                .participants
                .iter()
                .any(|participant| participant.agent_key == "right-agent")
        );
    }

    #[test]
    fn sifs_channel_metadata_records_keep_tombstone_across_concurrent_rename() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-tombstone-rename");
        let data_dir = SinomoDataDir::new(&root);
        let setup = SifsAgentChannelStore::open(&data_dir).expect("open setup store");
        let channel_id = SessionId::generate();
        let base = AgentChannel::new_channel(channel_id.clone(), "Original title");
        setup.save_channel(&base).expect("save base channel");

        let mut tombstoned = base.clone();
        tombstoned.deleted_at = Some(base.updated_at + time::Duration::seconds(1));
        tombstoned.updated_at = base.updated_at + time::Duration::seconds(1);
        let mut renamed = base;
        renamed.title = "Renamed elsewhere".to_string();
        renamed.updated_at += time::Duration::seconds(2);

        let barrier = Arc::new(Barrier::new(3));
        let spawn_save = |channel: AgentChannel| {
            let data_dir = data_dir.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let store = SifsAgentChannelStore::open(&data_dir).expect("open metadata writer");
                barrier.wait();
                store.save_channel(&channel).expect("save metadata record");
            })
        };
        let tombstone_writer = spawn_save(tombstoned.clone());
        let rename_writer = spawn_save(renamed.clone());
        barrier.wait();
        tombstone_writer
            .join()
            .expect("tombstone metadata writer joins");
        rename_writer.join().expect("rename metadata writer joins");

        let loaded = SifsAgentChannelStore::open(&data_dir)
            .expect("reopen channel store")
            .load_channel(&channel_id)
            .expect("load reconciled metadata")
            .expect("channel exists");
        assert_eq!(loaded.title, "Renamed elsewhere");
        assert_eq!(loaded.deleted_at, tombstoned.deleted_at);
        assert_eq!(loaded.updated_at, renamed.updated_at);
    }

    #[test]
    fn sifs_channel_reconciliation_discovers_backdated_record_without_rereading_bodies() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-discovery-cursor");
        let data_dir = SinomoDataDir::new(&root);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Long channel");
        store.save_channel(&channel).expect("save channel");
        let initial_count = AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT + 25;
        for index in 0..initial_count {
            let message = channel.append_user_message(
                AGENT_CHANNEL_APP_USER_ID,
                "User",
                format!("message {index}"),
                AgentChannelSource::app(),
            );
            store
                .append_message(&channel_id, &message)
                .expect("append initial message");
        }

        let initial = store
            .load_message_reconciliation_page(
                &channel_id,
                &BTreeSet::new(),
                AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT,
            )
            .expect("load initial reconciliation page");
        assert_eq!(
            initial.page.messages.len(),
            AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT
        );
        assert_eq!(initial.discovered_message_ids.len(), initial_count);
        assert_eq!(store.message_record_body_read_count(), initial_count);
        let known = initial
            .discovered_message_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        let unchanged = store
            .load_message_reconciliation_page(
                &channel_id,
                &known,
                AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT,
            )
            .expect("scan unchanged records");
        assert!(unchanged.page.messages.is_empty());
        assert_eq!(
            store.message_record_body_read_count(),
            initial_count,
            "an unchanged reconciliation scan must list IDs without rereading record bodies"
        );

        let mut backdated = channel.append_user_message(
            "remote-device",
            "Remote",
            "severely backdated but newly replicated",
            AgentChannelSource::app(),
        );
        // Model a delayed writer that authored from a stale presentation
        // cursor as well as carrying an old wall-clock timestamp.
        backdated.sequence = 1;
        backdated.created_at = channel.created_at - time::Duration::days(30);
        SifsAgentChannelStore::open(&data_dir)
            .expect("open remote writer")
            .append_message(&channel_id, &backdated)
            .expect("append backdated message");

        let ordinary_tail = SifsAgentChannelStore::open(&data_dir)
            .expect("open tail reader")
            .load_recent_message_page(&channel_id, AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT)
            .expect("load ordinary tail");
        assert!(
            ordinary_tail
                .messages
                .iter()
                .all(|message| message.message_id != backdated.message_id),
            "the test must place the new record outside the bounded presentation tail"
        );

        let reconciled = store
            .load_message_reconciliation_page(
                &channel_id,
                &known,
                AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT,
            )
            .expect("reconcile newly replicated record");
        assert_eq!(
            reconciled
                .page
                .messages
                .iter()
                .map(|message| message.message_id.clone())
                .collect::<Vec<_>>(),
            vec![backdated.message_id]
        );
        assert_eq!(reconciled.discovered_message_ids.len(), 1);
        assert_eq!(
            store.message_record_body_read_count(),
            initial_count + 1,
            "only the newly discovered immutable record body should be read"
        );
    }

    #[test]
    fn app_chat_send_surfaces_missing_codex_auth() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-codex-auth");
        let data_dir = SinomoDataDir::new(&root);
        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
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

        let session_id = chat.runtime.session().session_id.clone();
        let before_resume = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("attached session");
        let before_raw_len = before_resume.raw_transcript.items.len();
        assert_eq!(
            before_raw_len, 0,
            "provider authentication failed before the runtime accepted the user input"
        );
        drop(chat);

        let mut resumed =
            AgentChat::new(data_dir.clone(), false).expect("replacement chat runtime");
        resumed
            .resume_session(&session_id.to_string(), &[])
            .expect("resume failed direct chat");
        let after_resume = resumed
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("resumed snapshot")
            .expect("attached resumed session");
        assert_eq!(after_resume.raw_transcript.items.len(), 1);
        assert!(matches!(
            &after_resume.raw_transcript.items[0],
            AgentItem::UserMessage { content, .. } if content == "hello"
        ));
        let reconciled_raw_len = after_resume.raw_transcript.items.len();
        drop(resumed);

        let mut resumed_again =
            AgentChat::new(data_dir, false).expect("second replacement chat runtime");
        resumed_again
            .resume_session(&session_id.to_string(), &[])
            .expect("second resume failed direct chat");
        let after_second_resume = resumed_again
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("second resumed snapshot")
            .expect("attached second resumed session");
        assert_eq!(
            after_second_resume.raw_transcript.items.len(),
            reconciled_raw_len,
            "once reconciled, durable channel records must not be inserted again"
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
