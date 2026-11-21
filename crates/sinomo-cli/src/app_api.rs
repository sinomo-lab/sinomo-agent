//! Stable, GUI-facing surfaces over CLI-owned application workflows.
//!
//! The graphical app (`sinomo-app`) drives the same chat, cluster, and DFS
//! authority paths as the terminal UI, but needs small owned APIs rather than
//! crate-internal handler types. Everything here returns plain data so the app
//! never depends on `tui` internals or bypasses daemon-owned mutations.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
pub use sinomo_runtime::api::app::chat::{
    AgentSummary, ChatProcessSnapshot, ChatToolCallRef, ChatToolCallSnapshot,
    ChatToolOutputSnapshot, ProviderSummary,
};
pub use sinomo_runtime::api::app::skill::{SkillDetail, SkillMutationResult, SkillSummary};
mod transcript;
use sinomo_runtime::cluster;
use std::cell::RefCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
use sinomo_agent::AgentChannelUser;
use sinomo_agent::{
    AGENT_CHANNEL_APP_USER_ID, AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT,
    AGENT_CHANNEL_SNAPSHOT_SCHEMA_VERSION, AGENT_CHANNEL_WORKSPACE_CAUSAL_VERSION,
    AGENT_CHANNEL_WORKSPACE_PROJECTION_VERSION, AGENT_SESSION_SNAPSHOT_SCHEMA_VERSION,
    AgentActivityRef, AgentCatalog, AgentChannel, AgentChannelAttachment, AgentChannelAudience,
    AgentChannelAuthor, AgentChannelExecutorError, AgentChannelKind, AgentChannelLoopConfig,
    AgentChannelMessage, AgentChannelMessageKind, AgentChannelParticipant, AgentChannelRuntime,
    AgentChannelRuntimeEvent, AgentChannelSingleAgentPolicy, AgentChannelSource,
    AgentChannelSpeakDecision, AgentChannelSpeakInput, AgentChannelSpeech, AgentChannelStore,
    AgentChannelStoreError, AgentChannelTurn, AgentChannelTurnExecutor, AgentContextAttachment,
    AgentDefinition, AgentEvent, AgentEventPayload, AgentItem, AgentModelEvent, AgentRunContext,
    AgentRuntimeControlHost, AgentRuntimeControlRegistry, AgentRuntimeEvent,
    AgentRuntimeEventPayload, AgentSession, AgentSessionControlResult, AgentSessionFollowUpRequest,
    AgentSessionInterruptRequest, AgentSessionReattachState, AgentSessionRemoveQueuedInputRequest,
    AgentSessionResumeRequest, AgentSessionResumeResponse, AgentSessionSnapshot,
    AgentSessionSteerRequest, AgentSessionStore, AgentTaskLifecycleStatus, AgentTaskSnapshot,
    AgentToolCall, AgentToolDescriptor, AgentToolEvidence, AgentToolInvocationMode,
    AgentToolOrchestrator, AgentToolOutcome, AgentToolPlan, AgentToolPlanner,
    AgentToolProgressEvent, AgentToolRegistry, AgentToolStatus, AgentToolSurface,
    ContextAssemblyReport, FileAgentSessionStore, InteractionOutput, InteractionSession,
    InteractionSurface, InteractionSurfaceKind, SkillActivationScope, SkillCatalogEntry,
    SkillCatalogSifsStore, SkillCatalogSource, SkillCatalogStore, SkillCatalogWrite, TaskId,
    WorkspaceRef, channel_message_agent_item_id, read_agent_skill_package,
    render_common_skill_markdown, safe_agent_channel_attachment_file_name,
    sync_participant_session, write_agent_skill_package,
};
use sinomo_lm::{
    AnthropicConfig, CLAUDE_DEFAULT_MODEL, ClaudeCodeConfig, CodexCliConfig, GitHubCopilotConfig,
    OpenAiConfig, PROVIDER_DEFAULT_MODEL, ProviderReasoningEffort,
};
use sinomo_runtime::cluster::ClusterHealthRow;
use sinomo_runtime::events::{InMemoryEventStore, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_runtime::node::NodeId;
use sinomo_runtime::vfs::{VfsDirectoryCursor, VfsError, VfsHandle, VfsObjectKind};
use std::collections::{BTreeMap, BTreeSet};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::commands::{
    ClusterInitOptions, ClusterInviteOptions, ClusterJoinOptions, cluster_invite_with_paths,
    cluster_join_with_paths, cluster_leave_with_paths, initialize_single_node_cluster_with_paths,
    read_cluster_state as read_cli_cluster_state,
};
use crate::error::CliError;
use crate::tui::{
    self, DefaultMainAgentChatModel, InteractionRuntime, RuntimeMainAgentInteractionHandler,
};
use crate::{workspace_binding_root_for_data_dir, workspace_ref_for_selected_root};

#[path = "chat_tool_output.rs"]
mod tool_output;
use tool_output::snapshot as tool_output_snapshot;

type Handler = RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>;
const APP_CHAT_RUNTIME_DRAIN_INTERVAL: Duration = Duration::from_millis(25);
const APP_CHAT_ATTACHMENT_TEXT_EXCERPT_BYTES: usize = 24 * 1024;
pub const SIFS_AGENT_CHANNELS_ROOT: &str = "/services/sinomo-agent/channels";
const SIFS_PATH_ACCESS_CAPABILITY: &str = sinomo_runtime::dfs::SIFS_PATH_ACCESS_CAPABILITY;
/// Starts the error for a cluster whose service catalog lists no SIFS
/// path-access provider yet, as while a daemon is still publishing services.
pub(crate) const NO_SIFS_PROVIDER_ERROR: &str = "no cluster service instance provides";
const SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE: u64 = 128;
const SIFS_AGENT_CHANNEL_METADATA_SCHEMA_VERSION: u32 = 1;
const SIFS_AGENT_CHANNEL_HISTORY_METADATA_SCHEMA_VERSION: u32 = 1;
const SIFS_AGENT_SESSION_CHECKPOINT_SCHEMA_VERSION: u32 = 1;
/// The daemon deliberately rejects a browser read quickly when its route limit is saturated or
/// another SIFS request holds the shared service lock. Files and chat-list workers routinely
/// overlap, so absorb those explicit transient signals here instead of presenting normal UI
/// concurrency as SIFS unavailability.
const SIFS_BROWSER_READ_RETRY_BUDGET: Duration = Duration::from_secs(3);
const SIFS_BROWSER_READ_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(25);
const SIFS_BROWSER_READ_RETRY_MAX_DELAY: Duration = Duration::from_millis(250);
const SIFS_IMMUTABLE_WRITE_RECONCILIATION_BUDGET: Duration = Duration::from_secs(5);

/// Remove a SIFS directory tree. Links are deleted, never followed.
fn remove_sifs_tree(browser: &SifsBrowser, path: &str) -> Result<(), String> {
    for entry in browser.list_if_exists(path)? {
        if entry.is_dir && !entry.is_link {
            remove_sifs_tree(browser, &entry.path)?;
        } else {
            match browser.delete(&entry.path, false) {
                Ok(()) => {}
                Err(detail) if sifs_error_is_not_found(&detail) => {}
                Err(detail) => return Err(detail),
            }
        }
    }
    match browser.delete(path, true) {
        Ok(()) => Ok(()),
        Err(detail) if sifs_error_is_not_found(&detail) => Ok(()),
        Err(detail) => Err(detail),
    }
}

fn sifs_agent_channel_dir_at(channels_root: &str, channel_id: &SessionId) -> String {
    format!("{channels_root}/{}", channel_id.as_str())
}

fn sifs_agent_channel_history_projection_path_at(
    channels_root: &str,
    channel_id: &SessionId,
) -> String {
    format!(
        "{}/metadata/history-current.json",
        sifs_agent_channel_dir_at(channels_root, channel_id)
    )
}

fn sifs_agent_channel_metadata_records_dir_at(
    channels_root: &str,
    channel_id: &SessionId,
) -> String {
    format!(
        "{}/metadata/records",
        sifs_agent_channel_dir_at(channels_root, channel_id)
    )
}

fn sifs_agent_channel_metadata_record_path_at(
    channels_root: &str,
    channel_id: &SessionId,
    record_id: &str,
) -> String {
    format!(
        "{}/{}.json",
        sifs_agent_channel_metadata_records_dir_at(channels_root, channel_id),
        record_id
    )
}

fn sifs_agent_channel_message_chunks_dir_at(channels_root: &str, channel_id: &SessionId) -> String {
    format!(
        "{}/messages",
        sifs_agent_channel_dir_at(channels_root, channel_id)
    )
}

fn sifs_agent_channel_message_records_dir_at(
    channels_root: &str,
    channel_id: &SessionId,
) -> String {
    format!(
        "{}/records",
        sifs_agent_channel_message_chunks_dir_at(channels_root, channel_id)
    )
}

fn sifs_agent_channel_message_record_path_at(
    channels_root: &str,
    channel_id: &SessionId,
    message_id: &TaskId,
) -> String {
    format!(
        "{}/{}.json",
        sifs_agent_channel_message_records_dir_at(channels_root, channel_id),
        message_id.as_str()
    )
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
    pub(crate) fn empty(channel_id: &SessionId, total_messages: u64) -> Self {
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

/// One bounded page of immutable channel-message records for history search.
///
/// `record_offset` is the stable ascending directory position consumed by the
/// history cursor.
#[derive(Clone, Debug)]
pub(crate) struct AgentChannelImmutableMessageRecord {
    pub record_offset: usize,
    pub message: AgentChannelMessage,
}

#[derive(Clone, Debug)]
pub(crate) struct AgentChannelImmutableMessageRecordPage {
    pub records: Vec<AgentChannelImmutableMessageRecord>,
    pub next_cursor: Option<VfsDirectoryCursor>,
    pub consumed_entries: usize,
    /// Actual message-body bytes read, including bodies later omitted as
    /// truncated or malformed. Callers must charge this value to their scan
    /// budget instead of summing only successfully decoded records.
    pub read_bytes: u64,
    pub omitted_records: usize,
    /// Serialized size of the first valid record that did not fit this
    /// invocation's remaining body budget. The directory cursor has not been
    /// consumed; callers can retry it with a fresh hard budget or explicitly
    /// account for and skip an intrinsically oversized record.
    pub required_bytes: Option<u64>,
}

/// One message decoded from a bounded legacy stable-channel JSONL chunk.
#[derive(Clone, Debug)]
pub(crate) struct AgentChannelLegacyMessageRecord {
    pub record_offset: usize,
    /// Byte position immediately after this JSONL record, or zero when this
    /// record finishes the chunk.
    pub next_byte_offset: usize,
    pub message: AgentChannelMessage,
}

#[derive(Clone, Debug)]
pub(crate) struct AgentChannelLegacyMessageRecordPage {
    pub records: Vec<AgentChannelLegacyMessageRecord>,
    /// Directory continuation after the current chunk. It is consumed only
    /// when `next_byte_offset` is zero.
    pub next_cursor: Option<VfsDirectoryCursor>,
    pub current_entry_name: Option<String>,
    pub current_object_id: Option<String>,
    pub current_version: Option<String>,
    pub next_byte_offset: usize,
    pub next_discarding_line: bool,
    pub deferred_for_fresh_budget: bool,
    pub consumed_entries: usize,
    pub read_bytes: u64,
    pub omitted_records: usize,
}

/// Bounded authorization projection used only by historical activity search.
///
/// An incomplete projection never carries a channel: callers must fail closed
/// instead of trusting a stale snapshot when the immutable metadata fold does
/// not fit the supplied scan budget.
#[derive(Clone, Debug)]
pub(crate) struct AgentChannelHistoryMetadataRead {
    pub channel: Option<AgentChannelHistoryMetadata>,
    pub scanned_items: usize,
    pub scanned_bytes: u64,
    pub omitted_records: usize,
    pub complete: bool,
}

/// Compact, transcript-free current channel state used by history search.
///
/// This intentionally excludes message bodies, workspace state, provider
/// sessions, and participant observation sets. The source-record and
/// metadata-directory versions make a stale cache fail closed (or fall back to
/// the bounded immutable metadata fold) after any concurrent metadata append.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct AgentChannelHistoryMetadata {
    pub channel_id: SessionId,
    pub title: String,
    pub channel_kind: AgentChannelKind,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub deleted_at: Option<time::OffsetDateTime>,
    pub participants: Vec<AgentChannelHistoryParticipant>,
}

impl AgentChannelHistoryMetadata {
    fn from_channel(channel: &AgentChannel) -> Self {
        Self {
            channel_id: channel.channel_id.clone(),
            title: channel.title.clone(),
            channel_kind: channel.channel_kind,
            updated_at: channel.updated_at,
            deleted_at: channel.deleted_at,
            participants: channel
                .participants
                .iter()
                .map(|participant| AgentChannelHistoryParticipant {
                    agent_key: participant.agent_key.clone(),
                    enabled: participant.enabled,
                })
                .collect(),
        }
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct AgentChannelHistoryParticipant {
    pub agent_key: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct SifsAgentChannelHistoryMetadataProjection {
    schema_version: u32,
    channel: AgentChannelHistoryMetadata,
    source_record_id: String,
    metadata_records_object_id: String,
    metadata_records_version: String,
}

impl SifsAgentChannelHistoryMetadataProjection {
    fn matches_revision(
        &self,
        channel_id: &SessionId,
        object_id: Option<&str>,
        version: Option<&str>,
    ) -> bool {
        self.schema_version == SIFS_AGENT_CHANNEL_HISTORY_METADATA_SCHEMA_VERSION
            && self.channel.channel_id == *channel_id
            && object_id == Some(self.metadata_records_object_id.as_str())
            && version == Some(self.metadata_records_version.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AgentChannelMetadataNamespaceRevision {
    Missing,
    Versioned {
        object_id: String,
        version: String,
    },
    /// A namespace without both identity fields cannot prove a stable fold.
    Unversioned,
}

fn incomplete_channel_history_metadata(
    scanned_items: usize,
    scanned_bytes: u64,
    omitted_records: usize,
) -> AgentChannelHistoryMetadataRead {
    AgentChannelHistoryMetadataRead {
        channel: None,
        scanned_items,
        scanned_bytes,
        omitted_records,
        complete: false,
    }
}

fn channel_metadata_namespace_revision(
    history_vfs: &VfsHandle,
    records_dir: &str,
) -> Result<AgentChannelMetadataNamespaceRevision, AgentChannelStoreError> {
    match history_vfs.stat_beneath_no_links(records_dir) {
        Ok(metadata) => match (metadata.object_id, metadata.version) {
            (Some(object_id), Some(version)) => {
                Ok(AgentChannelMetadataNamespaceRevision::Versioned { object_id, version })
            }
            _ => Ok(AgentChannelMetadataNamespaceRevision::Unversioned),
        },
        Err(VfsError::NotFound { .. }) => Ok(AgentChannelMetadataNamespaceRevision::Missing),
        Err(source) => Err(channel_store_io(records_dir, source.to_string())),
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

pub struct AgentChannelTranscript {
    pub reconciliation: AgentChannelMessageReconciliation,
    pub overview: Option<AgentChannelTimelineOverview>,
    pub sources: sinomo_runtime::api::app::chat::ChatHistorySources,
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
    /// First workspace used as the sandbox cwd. The chat/channel's ordered
    /// `workspaces` list is authoritative; the legacy session binding is read
    /// only when resuming snapshots written before that list existed.
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
    /// A step-indexed assistant fragment emitted directly by the model.
    AssistantTextDelta {
        step_index: usize,
        text: String,
    },
    /// Provider-emitted reasoning summary. This is explicit model output, not inferred reasoning.
    ReasoningDelta {
        step_index: usize,
        delta: String,
    },
    /// Completed provider-emitted reasoning summary.
    ReasoningSummary {
        step_index: usize,
        summary: String,
    },
    /// A visible answer fragment with channel participant metadata.
    AgentToken {
        speaker: ChatSpeaker,
        text: String,
    },
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
    ToolArtifacts(AgentToolProgressEvent),
    ToolOutput(ChatToolOutputSnapshot),
    /// The agent session started an operation; its activity is journaled
    /// under this id.
    OpStarted {
        op_id: TaskId,
    },
    /// A channel participant starts acting; the activity that follows is its.
    SpeakerStarted(ChatSpeaker),
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
    /// Concise model/UI-safe warning after an accepted control operation whose
    /// durable stable-history/checkpoint follow-up could not be completed.
    pub warning: Option<String>,
    /// The session is paused (interrupted); the next message resumes it.
    pub paused: bool,
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
    /// The session is paused (interrupted); the next message resumes it.
    pub paused: bool,
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
    data_dir: SinomoDataDir,
    address: String,
    runtime: tokio::runtime::Handle,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AgentChatSifsProvider {
    LocalDaemon,
    ClusterNode(NodeId),
}

impl AgentChatSifsStorage {
    pub fn new(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        Self {
            data_dir,
            address: address.into(),
            runtime: runtime.clone(),
        }
    }

    fn provider(&self) -> Result<AgentChatSifsProvider, String> {
        // The live service catalog owns provider availability. Reopening a
        // store must not pin a withdrawn provider for the lifetime of a chat
        // view. Resolution is a read; existing handles keep their exact
        // operation ownership and never replay an uncertain write.
        self.discover_provider()
    }

    fn discover_provider(&self) -> Result<AgentChatSifsProvider, String> {
        let client = sinomo_runtime::api::DaemonApiClient::at_address(self.address.clone());
        let status = client
            .status()
            .map_err(|error| format!("cannot read connected daemon status: {error}"))?;
        let Some(cluster) = status.cluster else {
            // Standalone daemons have no cluster KV in which to publish
            // instances. The local registry is the only provider catalog.
            return SifsBrowser::open_daemon_on_runtime(&self.address, &self.runtime)
                .map(|_| AgentChatSifsProvider::LocalDaemon)
                .map_err(|error| format!("local filesystem provider unavailable: {error}"));
        };
        // The daemon's service catalog resolves ready, reachable providers
        // from its routing evidence; the chat uses its first choice.
        let providers = client
            .service_providers(SIFS_PATH_ACCESS_CAPABILITY)
            .map_err(|error| format!("cannot resolve filesystem providers: {error}"))?;
        let Some(node_id) = providers.into_iter().next() else {
            tracing::warn!(
                required_capability = SIFS_PATH_ACCESS_CAPABILITY,
                local_node_id = %cluster.node_id,
                daemon_address = %self.address,
                "cluster SIFS provider resolution found no ready reachable provider"
            );
            return Err(format!(
                "{NO_SIFS_PROVIDER_ERROR} {SIFS_PATH_ACCESS_CAPABILITY}"
            ));
        };
        Ok(AgentChatSifsProvider::ClusterNode(node_id))
    }

    pub fn open_channel_store(&self) -> Result<SifsAgentChannelStore, String> {
        match self.provider()? {
            AgentChatSifsProvider::LocalDaemon => {
                SifsAgentChannelStore::open_daemon_on_runtime(&self.address, &self.runtime)
            }
            AgentChatSifsProvider::ClusterNode(node_id) => {
                SifsAgentChannelStore::open_daemon_node_on_runtime(
                    self.data_dir.clone(),
                    &self.address,
                    node_id,
                    &self.runtime,
                )
            }
        }
    }

    /// Build a cheap read transport into the selected SIFS provider. Keeping
    /// this handle beside the channel/checkpoint stores prevents app catalog
    /// reads from mounting and hydrating a second process-local DFS instance.
    pub fn open_agent_catalog_vfs(&self) -> Result<VfsHandle, String> {
        match self.provider()? {
            AgentChatSifsProvider::LocalDaemon => {
                sinomo_runtime::vfs::daemon_sifs(self.address.clone())
                    .map_err(|error| error.to_string())
            }
            AgentChatSifsProvider::ClusterNode(_) => {
                // Catalog handles outlive an individual turn. Resolve the
                // current advertised provider before each new operation.
                sinomo_runtime::vfs::discovered_daemon_sifs(
                    self.data_dir.clone(),
                    self.address.clone(),
                )
                .map_err(|error| error.to_string())
            }
        }
    }
}

/// SIFS-backed channel store used by the app chat runtime.
pub struct SifsAgentChannelStore {
    browser: SifsBrowser,
    channels_root: String,
    #[cfg(test)]
    message_record_body_reads: std::sync::atomic::AtomicUsize,
}

impl SifsAgentChannelStore {
    pub(crate) fn provider_node_id(&self) -> &str {
        self.browser.node_id()
    }

    pub fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        SifsBrowser::open(data_dir).map(Self::from_browser)
    }

    pub fn open_daemon_with_timeout(
        address: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, String> {
        SifsBrowser::open_daemon_with_timeout(address, timeout).map(Self::from_browser)
    }

    pub fn open_daemon_on_runtime(
        address: impl Into<String>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        SifsBrowser::open_daemon_on_runtime(address, runtime).map(Self::from_browser)
    }

    /// Open a daemon-backed channel store below an isolated SIFS root.
    ///
    /// Live probes use this to keep test channels out of the user-visible
    /// production channel catalog.
    pub fn open_daemon_on_runtime_at_root(
        address: impl Into<String>,
        runtime: &tokio::runtime::Handle,
        channels_root: impl Into<String>,
    ) -> Result<Self, String> {
        let browser = SifsBrowser::open_daemon_on_runtime(address, runtime)?;
        Self::from_browser_at_root(browser, channels_root)
    }

    pub fn open_daemon_node_on_runtime(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
        node_id: NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        SifsBrowser::open_daemon_node_on_runtime(data_dir, address, node_id, runtime)
            .map(Self::from_browser)
    }

    pub fn from_browser(browser: SifsBrowser) -> Self {
        Self {
            browser,
            channels_root: SIFS_AGENT_CHANNELS_ROOT.to_string(),
            #[cfg(test)]
            message_record_body_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn from_browser_at_root(
        browser: SifsBrowser,
        channels_root: impl Into<String>,
    ) -> Result<Self, String> {
        let channels_root = channels_root.into();
        let channels_root = channels_root.trim_end_matches('/');
        if channels_root.is_empty()
            || !channels_root.starts_with('/')
            || channels_root.bytes().any(|byte| byte == 0)
            || channels_root
                .split('/')
                .any(|part| matches!(part, "." | ".."))
            || channels_root.split('/').skip(1).any(|part| part.is_empty())
        {
            return Err(format!(
                "agent channel store root must be a normalized absolute SIFS path: {channels_root}"
            ));
        }
        Ok(Self {
            browser,
            channels_root: channels_root.to_string(),
            #[cfg(test)]
            message_record_body_reads: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Delete the complete temporary cluster-test run containing this store.
    ///
    /// The production channel root and arbitrary SIFS paths are rejected. This
    /// lets a cross-node channel expectation clean up the write-side fixture
    /// after it has proved convergence.
    pub fn delete_isolated_test_run(&self) -> Result<(), String> {
        const TEST_RUNS_ROOT: &str = "/services/cluster-tests/runs";
        let suffix = self
            .channels_root
            .strip_prefix(&format!("{TEST_RUNS_ROOT}/"))
            .ok_or_else(|| {
                format!(
                    "refusing to delete agent channel store outside {TEST_RUNS_ROOT}: {}",
                    self.channels_root
                )
            })?;
        let run_id = suffix
            .split('/')
            .next()
            .filter(|part| !part.is_empty())
            .ok_or_else(|| {
                format!(
                    "isolated agent channel store has no test run identity: {}",
                    self.channels_root
                )
            })?;
        let run_root = format!("{TEST_RUNS_ROOT}/{run_id}");
        remove_sifs_tree(&self.browser, &run_root)
    }

    /// Permanently remove a deleted channel's content once its retention
    /// window has passed: messages, attachments, session checkpoints, and any
    /// legacy snapshot. The metadata records stay so the deletion tombstone
    /// keeps winning when replicas merge. Refuses channels that are not
    /// deleted. Returns whether anything was removed.
    pub fn purge_deleted_channel_content(&self, channel_id: &SessionId) -> Result<bool, String> {
        let Some((mut channel, applied)) = self
            .load_channel_metadata(channel_id)
            .map_err(|error| error.to_string())?
            .filter(|(channel, _)| channel.deleted_at.is_some())
        else {
            return Err(format!(
                "refusing to purge channel {channel_id}: it is not deleted"
            ));
        };
        let channel_dir = self.channel_dir(channel_id);
        let metadata_dir = format!("{channel_dir}/metadata");
        let snapshot_path = self.snapshot_path(channel_id);
        let mut removed = false;
        for entry in self.browser.list_if_exists(&channel_dir)? {
            let path = entry.path.trim_end_matches('/');
            // The metadata records keep the tombstone; the snapshot cache is
            // rewritten below without any transcript.
            if path == metadata_dir || path == snapshot_path {
                continue;
            }
            if entry.is_dir && !entry.is_link {
                remove_sifs_tree(&self.browser, &entry.path)?;
            } else {
                match self.browser.delete(&entry.path, false) {
                    Ok(()) => {}
                    Err(detail) if sifs_error_is_not_found(&detail) => continue,
                    Err(detail) => return Err(detail),
                }
            }
            removed = true;
        }
        if removed {
            channel.transcript.clear();
            self.write_channel_snapshot_cache(&channel, &applied)
                .map_err(|error| error.to_string())?;
        }
        Ok(removed)
    }

    fn channel_dir(&self, channel_id: &SessionId) -> String {
        sifs_agent_channel_dir_at(&self.channels_root, channel_id)
    }

    fn snapshot_path(&self, channel_id: &SessionId) -> String {
        format!("{}/snapshot.json", self.channel_dir(channel_id))
    }

    fn metadata_records_dir(&self, channel_id: &SessionId) -> String {
        sifs_agent_channel_metadata_records_dir_at(&self.channels_root, channel_id)
    }

    fn metadata_record_path(&self, channel_id: &SessionId, record_id: &str) -> String {
        sifs_agent_channel_metadata_record_path_at(&self.channels_root, channel_id, record_id)
    }

    fn history_metadata_projection_path(&self, channel_id: &SessionId) -> String {
        sifs_agent_channel_history_projection_path_at(&self.channels_root, channel_id)
    }

    fn message_chunks_dir(&self, channel_id: &SessionId) -> String {
        sifs_agent_channel_message_chunks_dir_at(&self.channels_root, channel_id)
    }

    #[cfg(test)]
    fn message_index_path(&self, channel_id: &SessionId) -> String {
        format!("{}/index.json", self.message_chunks_dir(channel_id))
    }

    #[cfg(test)]
    fn message_chunk_path(&self, channel_id: &SessionId, sequence: u64) -> String {
        let chunk = sequence.saturating_sub(1) / SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE;
        self.message_chunk_path_by_index(channel_id, chunk)
    }

    #[cfg(test)]
    fn message_chunk_path_by_index(&self, channel_id: &SessionId, chunk: u64) -> String {
        format!("{}/{chunk:08}.jsonl", self.message_chunks_dir(channel_id))
    }

    fn message_records_dir(&self, channel_id: &SessionId) -> String {
        sifs_agent_channel_message_records_dir_at(&self.channels_root, channel_id)
    }

    fn message_record_path(&self, channel_id: &SessionId, message_id: &TaskId) -> String {
        sifs_agent_channel_message_record_path_at(&self.channels_root, channel_id, message_id)
    }

    fn turn_chunks_dir(&self, channel_id: &SessionId) -> String {
        format!("{}/turns", self.channel_dir(channel_id))
    }

    #[cfg(test)]
    fn turn_index_path(&self, channel_id: &SessionId) -> String {
        format!("{}/index.json", self.turn_chunks_dir(channel_id))
    }

    #[cfg(test)]
    fn turn_chunk_path(&self, channel_id: &SessionId, sequence: u64) -> String {
        let chunk = sequence / SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE;
        format!("{}/{chunk:08}.jsonl", self.turn_chunks_dir(channel_id))
    }

    fn turn_records_dir(&self, channel_id: &SessionId) -> String {
        format!("{}/records", self.turn_chunks_dir(channel_id))
    }

    fn turn_record_path(&self, channel_id: &SessionId, turn_id: &TaskId) -> String {
        format!(
            "{}/{}.json",
            self.turn_records_dir(channel_id),
            turn_id.as_str()
        )
    }

    fn attachment_path(
        &self,
        channel_id: &SessionId,
        attachment_id: &TaskId,
        file_name: &str,
    ) -> String {
        format!(
            "{}/attachments/{}/{}",
            self.channel_dir(channel_id),
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
        summaries.sort_by_key(|summary| std::cmp::Reverse(summary.updated_at));
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

    fn reconcile_immutable_write(
        &self,
        path: &str,
        bytes: &[u8],
        original_detail: String,
    ) -> Result<(), AgentChannelStoreError> {
        let deadline = Instant::now() + SIFS_IMMUTABLE_WRITE_RECONCILIATION_BUDGET;
        // A materialized exact immutable key/payload is a consumer observation,
        // not proof that a lost operation receipt settled. Never submit another
        // mutation while the original write's outcome is unknown.
        let remote = match &self.browser.backend {
            SifsBrowserBackend::Remote(remote) => Some(
                remote.with_operation_deadline(Some(
                    remote
                        .operation_deadline
                        .map_or(deadline, |existing| existing.min(deadline)),
                )),
            ),
            SifsBrowserBackend::Local(_) => None,
        };
        let deadline = remote
            .as_ref()
            .and_then(|remote| remote.operation_deadline)
            .unwrap_or(deadline);
        let mut delay = SIFS_BROWSER_READ_RETRY_INITIAL_DELAY;
        let mut last_detail = original_detail.clone();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(channel_store_io(
                    path,
                    format!(
                        "immutable channel write outcome remained unknown after byte-for-byte reconciliation: {original_detail}; last observation: {last_detail}"
                    ),
                ));
            }
            let read = match &remote {
                Some(remote) => remote.read_file(path),
                None => self.browser.read_file(path),
            };
            match read {
                Ok(existing) if existing == bytes => return Ok(()),
                Ok(_) => {
                    return Err(channel_store_io(
                        path,
                        format!(
                            "immutable channel record differs after an ambiguous write: {original_detail}"
                        ),
                    ));
                }
                Err(detail) if sifs_error_is_not_found(&detail) => {
                    last_detail = detail;
                }
                Err(detail) if sifs_immutable_write_should_reconcile(&detail) => {
                    last_detail = detail;
                }
                Err(detail) => {
                    return Err(channel_store_io(
                        path,
                        format!(
                            "cannot verify an ambiguous immutable channel write ({original_detail}); read-back failed: {detail}"
                        ),
                    ));
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                thread::sleep(delay.min(remaining));
                delay = (delay * 2).min(SIFS_BROWSER_READ_RETRY_MAX_DELAY);
            }
        }
    }

    /// Best-effort materialization over immutable channel records.
    ///
    /// Concurrent writers may race the optimistic DFS version check. The
    /// immutable records remain authoritative, so the losing cache writer must
    /// not turn a committed mutation into an application failure; readers will
    /// reject or repair the stale projection from those records.
    fn write_materialized_cache(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<(), AgentChannelStoreError> {
        match self.write_file(path, bytes) {
            Err(AgentChannelStoreError::Io { path, detail }) => {
                tracing::warn!(
                    %path,
                    %detail,
                    "repairable SIFS channel cache write deferred"
                );
                Ok(())
            }
            result => result,
        }
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
        match self.write_file(path, &bytes) {
            Ok(()) => Ok(()),
            Err(AgentChannelStoreError::Io { detail, .. })
                if sifs_immutable_write_should_reconcile(&detail) =>
            {
                self.reconcile_immutable_write(path, &bytes, detail)
            }
            Err(error) => Err(error),
        }
    }

    fn append_message_record(
        &self,
        channel_id: &SessionId,
        message: &AgentChannelMessage,
    ) -> Result<(), AgentChannelStoreError> {
        self.write_immutable_json(
            &self.message_record_path(channel_id, &message.message_id),
            message,
        )
    }

    pub(crate) fn load_immutable_message_record(
        &self,
        channel_id: &SessionId,
        message_id: &TaskId,
    ) -> Result<Option<AgentChannelMessage>, AgentChannelStoreError> {
        let path = self.message_record_path(channel_id, message_id);
        let bytes = match self.browser.read_file(&path) {
            Ok(bytes) => bytes,
            Err(detail) if sifs_error_is_not_found(&detail) => return Ok(None),
            Err(detail) => return Err(channel_store_io(&path, detail)),
        };
        let message = serde_json::from_slice::<AgentChannelMessage>(&bytes).map_err(|source| {
            AgentChannelStoreError::Serialization {
                detail: format!("deserialize immutable channel message {path}: {source}"),
            }
        })?;
        if &message.message_id != message_id {
            return Err(AgentChannelStoreError::Serialization {
                detail: format!(
                    "immutable channel message id {} does not match path {path}",
                    message.message_id
                ),
            });
        }
        Ok(Some(message))
    }

    fn append_turn_record(
        &self,
        channel_id: &SessionId,
        turn: &AgentChannelTurn,
    ) -> Result<(), AgentChannelStoreError> {
        self.write_immutable_json(&self.turn_record_path(channel_id, &turn.turn_id), turn)
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
            &self.metadata_record_path(&record.channel.channel_id, &record_id),
            &record,
        )?;
        Ok(record)
    }

    fn write_history_metadata_projection(
        &self,
        channel_id: &SessionId,
        source_record_id: &str,
    ) -> Result<(), AgentChannelStoreError> {
        const STABLE_FOLD_ATTEMPTS: usize = 3;

        let records_dir = self.metadata_records_dir(channel_id);
        for _ in 0..STABLE_FOLD_ATTEMPTS {
            let before = self
                .browser
                .stat(&records_dir)
                .map_err(|detail| channel_store_io(&records_dir, detail))?;
            // Fold immutable records only. The materialized snapshot can be
            // ahead of this node's directory view after a routed write, so it
            // cannot be paired with the visible directory watermark.
            let record_paths = self.paths_in_dir_with_suffix(&records_dir, ".json")?;
            let mut channel = None;
            let mut source_record_seen = false;
            for (path, record) in self
                .load_json_documents_with_paths::<SifsAgentChannelMetadataRecord>(&record_paths)?
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
                    return Err(AgentChannelStoreError::Serialization {
                        detail: format!("channel metadata record path has no id: {path}"),
                    });
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
                source_record_seen |= record.record_id == source_record_id;
                channel = Some(match channel {
                    Some(existing) => merge_channel_snapshot(existing, record.channel)?,
                    None => record.channel,
                });
            }
            let after = self
                .browser
                .stat(&records_dir)
                .map_err(|detail| channel_store_io(&records_dir, detail))?;
            if before.object_id != after.object_id
                || before.version != after.version
                || !source_record_seen
            {
                continue;
            }
            let Some(channel) = channel else {
                continue;
            };
            let projection = SifsAgentChannelHistoryMetadataProjection {
                schema_version: SIFS_AGENT_CHANNEL_HISTORY_METADATA_SCHEMA_VERSION,
                channel: AgentChannelHistoryMetadata::from_channel(&channel),
                source_record_id: source_record_id.to_string(),
                metadata_records_object_id: after.object_id,
                metadata_records_version: after.version,
            };
            let bytes = serde_json::to_vec_pretty(&projection).map_err(|source| {
                AgentChannelStoreError::Serialization {
                    detail: source.to_string(),
                }
            })?;
            // An append after the second stat only makes this older watermark
            // fail validation. It can never stamp this fold as covering the
            // newer record, even if this cache write wins the race.
            return self.write_materialized_cache(
                &self.history_metadata_projection_path(channel_id),
                &bytes,
            );
        }

        // This projection is an optimization. Under sustained concurrent
        // metadata writes, leave it stale so readers reject it and use their
        // bounded immutable fold instead of failing the channel mutation.
        Ok(())
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
        self.record_bytes(paths)?
            .into_iter()
            .map(|(path, bytes)| {
                let value = Self::decode_record(&path, &bytes)?;
                Ok((path, value))
            })
            .collect()
    }

    fn decode_record<T: DeserializeOwned>(
        path: &str,
        bytes: &[u8],
    ) -> Result<T, AgentChannelStoreError> {
        serde_json::from_slice(bytes).map_err(|source| AgentChannelStoreError::Serialization {
            detail: format!("invalid channel record at {path}: {source}"),
        })
    }

    fn record_bytes(
        &self,
        paths: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, AgentChannelStoreError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut records = Vec::with_capacity(paths.len());
        for (path, result) in self
            .browser
            .read_files(paths)
            .map_err(|detail| channel_store_io(&self.channels_root, detail))?
        {
            if let Some(bytes) = channel_optional_bulk_read_bytes(&path, result)? {
                records.push((path, bytes));
            }
        }
        Ok(records)
    }

    fn message_record_bytes(
        &self,
        paths: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, AgentChannelStoreError> {
        #[cfg(test)]
        self.message_record_body_reads
            .fetch_add(paths.len(), std::sync::atomic::Ordering::Relaxed);
        self.record_bytes(paths)
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
        for (_, bytes) in self.record_bytes(&paths)? {
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
        let mut messages = self.load_legacy_json_lines(&self.message_chunks_dir(channel_id))?;
        let record_paths =
            self.paths_in_dir_with_suffix(&self.message_records_dir(channel_id), ".json")?;
        for (path, bytes) in self.message_record_bytes(&record_paths)? {
            messages.push(Self::decode_record(&path, &bytes)?);
        }
        canonicalize_channel_messages(messages)
    }

    pub fn load_turns(
        &self,
        channel_id: &SessionId,
    ) -> Result<Vec<AgentChannelTurn>, AgentChannelStoreError> {
        let mut turns = self.load_legacy_json_lines(&self.turn_chunks_dir(channel_id))?;
        let record_paths =
            self.paths_in_dir_with_suffix(&self.turn_records_dir(channel_id), ".json")?;
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
        let path = self.snapshot_path(channel_id);
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
        self.write_materialized_cache(&self.snapshot_path(&channel.channel_id), &bytes)
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
            self.paths_in_dir_with_suffix(&self.metadata_records_dir(channel_id), ".json")?;
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

    /// Load authoritative channel metadata without message or turn bodies.
    /// This ordinary loader may repair the materialized metadata cache; bounded
    /// history authorization uses
    /// [`Self::load_channel_metadata_for_history_from_vfs`].
    pub fn load_channel_metadata_only(
        &self,
        channel_id: &SessionId,
    ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
        Ok(self
            .load_channel_metadata(channel_id)?
            .map(|(mut channel, _)| {
                channel.transcript.clear();
                channel
            }))
    }

    /// Prove current channel authorization within explicit metadata budgets.
    ///
    /// The repairable `snapshot.json` cache deliberately retains a bounded
    /// transcript for legacy readers, so an authorization check must not read
    /// it before membership is known. This projection instead folds only the
    /// immutable metadata records, whose transcripts are cleared at write
    /// time. If the proof does not fit, it returns an incomplete projection
    /// with no channel and the caller must skip it.
    #[cfg(test)]
    pub(crate) fn load_channel_metadata_for_history(
        &self,
        history_vfs: &VfsHandle,
        channel_id: &SessionId,
        max_items: usize,
        max_bytes: u64,
    ) -> Result<AgentChannelHistoryMetadataRead, AgentChannelStoreError> {
        Self::load_channel_metadata_for_history_from_vfs(
            history_vfs,
            &self.channels_root,
            channel_id,
            max_items,
            max_bytes,
        )
    }

    pub(crate) fn load_channel_metadata_for_history_from_vfs(
        history_vfs: &VfsHandle,
        channels_root: &str,
        channel_id: &SessionId,
        max_items: usize,
        max_bytes: u64,
    ) -> Result<AgentChannelHistoryMetadataRead, AgentChannelStoreError> {
        const METADATA_PAGE_SIZE: usize = 128;

        let mut scanned_items = 0usize;
        let mut scanned_bytes = 0u64;
        let omitted_records = 0usize;
        let projection_path =
            sifs_agent_channel_history_projection_path_at(channels_root, channel_id);
        let projection_metadata = match history_vfs.stat_beneath_no_links(&projection_path) {
            Ok(metadata) => Some(metadata),
            Err(VfsError::NotFound { .. }) => None,
            Err(source) => {
                return Err(channel_store_io(&projection_path, source.to_string()));
            }
        };
        if let Some(before) = projection_metadata {
            scanned_items = scanned_items.saturating_add(1);
            let Some(size_bytes) = before.size_bytes else {
                return Ok(incomplete_channel_history_metadata(
                    scanned_items,
                    scanned_bytes,
                    1,
                ));
            };
            if scanned_items > max_items || size_bytes > max_bytes {
                let charged_items = scanned_items.min(max_items);
                return Ok(incomplete_channel_history_metadata(
                    charged_items,
                    scanned_bytes,
                    usize::from(charged_items > 0),
                ));
            }
            let read = history_vfs
                .read_range_beneath_no_links(&projection_path, 0, size_bytes)
                .map_err(|source| channel_store_io(&projection_path, source.to_string()))?;
            scanned_bytes = scanned_bytes.saturating_add(read.bytes.len() as u64);
            let stable_read = !read.truncated
                && read.bytes.len() as u64 == size_bytes
                && before.object_id == read.metadata.object_id
                && before.version == read.metadata.version
                && read.metadata.size_bytes == Some(size_bytes)
                && matches!(
                    read.metadata.kind,
                    VfsObjectKind::File | VfsObjectKind::Artifact
                );
            let projection = stable_read
                .then(|| {
                    serde_json::from_slice::<SifsAgentChannelHistoryMetadataProjection>(&read.bytes)
                        .ok()
                })
                .flatten();
            if let Some(projection) = projection {
                let records_dir =
                    sifs_agent_channel_metadata_records_dir_at(channels_root, channel_id);
                let source_record = sifs_agent_channel_metadata_record_path_at(
                    channels_root,
                    channel_id,
                    &projection.source_record_id,
                );
                let current_records = history_vfs.stat_beneath_no_links(&records_dir);
                let source_exists = history_vfs.stat_beneath_no_links(&source_record).is_ok();
                if source_exists
                    && current_records.is_ok_and(|metadata| {
                        projection.matches_revision(
                            channel_id,
                            metadata.object_id.as_deref(),
                            metadata.version.as_deref(),
                        )
                    })
                {
                    return Ok(AgentChannelHistoryMetadataRead {
                        channel: Some(projection.channel),
                        scanned_items,
                        scanned_bytes,
                        omitted_records,
                        complete: true,
                    });
                }
            }
            // The cache changed, is malformed, or does not cover the current
            // immutable metadata namespace. Fall back to the bounded fold;
            // never authorize from stale compact state.
        }
        let records_dir = sifs_agent_channel_metadata_records_dir_at(channels_root, channel_id);
        let records_revision_before =
            channel_metadata_namespace_revision(history_vfs, &records_dir)?;
        let mut channel = None;
        let mut after = None;
        loop {
            let remaining_items = max_items.saturating_sub(scanned_items);
            if remaining_items == 0 {
                return Ok(incomplete_channel_history_metadata(
                    scanned_items,
                    scanned_bytes,
                    omitted_records,
                ));
            }
            let page = history_vfs
                .list_page_beneath_no_links_if_exists(
                    &records_dir,
                    after.as_ref(),
                    remaining_items.min(METADATA_PAGE_SIZE),
                )
                .map_err(|source| channel_store_io(&records_dir, source.to_string()))?;
            if page.entries.is_empty() {
                break;
            }
            for entry in page.entries {
                scanned_items = scanned_items.saturating_add(1);
                if !matches!(entry.kind, VfsObjectKind::File | VfsObjectKind::Artifact)
                    || !entry.name.ends_with(".json")
                {
                    return Ok(incomplete_channel_history_metadata(
                        scanned_items,
                        scanned_bytes,
                        omitted_records.saturating_add(1),
                    ));
                }
                let Some(path_record_id) = json_record_id_from_path(&entry.path) else {
                    return Ok(incomplete_channel_history_metadata(
                        scanned_items,
                        scanned_bytes,
                        omitted_records.saturating_add(1),
                    ));
                };
                let Some(size_bytes) = entry.size_bytes else {
                    return Ok(incomplete_channel_history_metadata(
                        scanned_items,
                        scanned_bytes,
                        omitted_records.saturating_add(1),
                    ));
                };
                if size_bytes > max_bytes.saturating_sub(scanned_bytes) {
                    return Ok(incomplete_channel_history_metadata(
                        scanned_items,
                        scanned_bytes,
                        omitted_records.saturating_add(1),
                    ));
                }
                let read = history_vfs
                    .read_range_beneath_no_links(&entry.path, 0, size_bytes)
                    .map_err(|source| channel_store_io(&entry.path, source.to_string()))?;
                scanned_bytes = scanned_bytes.saturating_add(read.bytes.len() as u64);
                if read.truncated
                    || read.bytes.len() as u64 != size_bytes
                    || read.metadata.size_bytes != Some(size_bytes)
                    || !matches!(
                        read.metadata.kind,
                        VfsObjectKind::File | VfsObjectKind::Artifact
                    )
                {
                    return Ok(incomplete_channel_history_metadata(
                        scanned_items,
                        scanned_bytes,
                        omitted_records.saturating_add(1),
                    ));
                }
                let record = serde_json::from_slice::<SifsAgentChannelMetadataRecord>(&read.bytes)
                    .map_err(|source| AgentChannelStoreError::Serialization {
                        detail: source.to_string(),
                    })?;
                if record.schema_version != SIFS_AGENT_CHANNEL_METADATA_SCHEMA_VERSION
                    || path_record_id != record.record_id
                    || &record.channel.channel_id != channel_id
                {
                    return Err(AgentChannelStoreError::Serialization {
                        detail: "channel metadata record identity is inconsistent".to_string(),
                    });
                }
                channel = Some(match channel {
                    Some(existing) => merge_channel_snapshot(existing, record.channel)?,
                    None => record.channel,
                });
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            after = Some(next);
        }

        let records_revision_after =
            channel_metadata_namespace_revision(history_vfs, &records_dir)?;
        let namespace_is_stable = match (&records_revision_before, &records_revision_after) {
            (
                AgentChannelMetadataNamespaceRevision::Versioned {
                    object_id: before_object,
                    version: before_version,
                },
                AgentChannelMetadataNamespaceRevision::Versioned {
                    object_id: after_object,
                    version: after_version,
                },
            ) => before_object == after_object && before_version == after_version,
            (
                AgentChannelMetadataNamespaceRevision::Missing,
                AgentChannelMetadataNamespaceRevision::Missing,
            ) => channel.is_none(),
            _ => false,
        };
        if !namespace_is_stable {
            // A concurrent metadata append can revoke membership. Never mark a
            // mixed or stale immutable fold complete; the caller retries from
            // the same activity cursor with a fresh bounded proof.
            return Ok(incomplete_channel_history_metadata(
                scanned_items,
                scanned_bytes,
                omitted_records,
            ));
        }

        Ok(AgentChannelHistoryMetadataRead {
            channel: channel
                .as_ref()
                .map(AgentChannelHistoryMetadata::from_channel),
            scanned_items,
            scanned_bytes,
            omitted_records,
            complete: true,
        })
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
        use sinomo_runtime::api::app::chat::{ChatHistoryLoadMode, ChatHistoryLoadRequest};
        Ok(self
            .read_transcript(
                channel_id,
                &ChatHistoryLoadRequest {
                    session_id: channel_id.to_string(),
                    mode: ChatHistoryLoadMode::ReconcileLatest,
                    sequence: None,
                    limit: tail_limit,
                    include_overview: false,
                    known_message_ids: known_message_ids.iter().cloned().collect(),
                    known_sources: None,
                },
            )?
            .reconciliation)
    }

    fn delta_from_messages(
        channel_id: &SessionId,
        messages: Vec<AgentChannelMessage>,
        known_message_ids: &BTreeSet<String>,
    ) -> Result<AgentChannelMessageReconciliation, AgentChannelStoreError> {
        let mut messages = deduplicate_channel_messages_preserving_sequence(messages)?;

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

        messages.sort_by(|left, right| {
            left.sequence
                .cmp(&right.sequence)
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.message_id.cmp(&right.message_id))
        });
        let start_sequence = messages.iter().map(|message| message.sequence).min();
        let end_sequence = messages.iter().map(|message| message.sequence).max();
        let page = AgentChannelMessagePage {
            channel_id: channel_id.clone(),
            messages,
            start_sequence,
            end_sequence,
            has_older: total_messages > newly_discovered_message_ids.len() as u64,
            has_newer: false,
            total_messages,
            chunk_size: SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE,
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
        Ok(Self::page_from_messages(
            channel_id,
            self.load_all_messages(channel_id)?,
            start_sequence,
            limit,
        ))
    }

    fn page_from_messages(
        channel_id: &SessionId,
        messages: Vec<AgentChannelMessage>,
        start_sequence: u64,
        limit: usize,
    ) -> AgentChannelMessagePage {
        let total_messages = messages.len() as u64;
        if limit == 0 || total_messages == 0 {
            return AgentChannelMessagePage::empty(channel_id, total_messages);
        }
        if start_sequence > total_messages {
            return Self::empty_message_page_after_end(channel_id, total_messages);
        }
        let start_sequence = start_sequence.max(1).min(total_messages);
        let end_sequence = start_sequence
            .saturating_add(limit as u64)
            .saturating_sub(1)
            .min(total_messages);
        Self::message_page_from_records(channel_id, messages, start_sequence, end_sequence, limit)
    }

    pub fn load_message_page_before(
        &self,
        channel_id: &SessionId,
        before_sequence: u64,
        limit: usize,
    ) -> Result<AgentChannelMessagePage, AgentChannelStoreError> {
        Ok(Self::page_before_messages(
            channel_id,
            self.load_all_messages(channel_id)?,
            before_sequence,
            limit,
        ))
    }

    fn page_before_messages(
        channel_id: &SessionId,
        messages: Vec<AgentChannelMessage>,
        before_sequence: u64,
        limit: usize,
    ) -> AgentChannelMessagePage {
        let total_messages = messages.len() as u64;
        if limit == 0 || total_messages == 0 {
            return AgentChannelMessagePage::empty(channel_id, total_messages);
        }
        if before_sequence <= 1 {
            return Self::empty_message_page_before_start(channel_id, total_messages);
        }
        let end_sequence = before_sequence.saturating_sub(1).min(total_messages);
        if end_sequence == 0 {
            return Self::empty_message_page_before_start(channel_id, total_messages);
        }
        let start_sequence = end_sequence.saturating_sub(limit as u64).saturating_add(1);
        Self::message_page_from_records(channel_id, messages, start_sequence, end_sequence, limit)
    }

    /// Read at most `limit` immutable message-record bodies in stable
    /// ascending record-id order.
    ///
    /// This is intentionally separate from the app timeline pager above. The
    /// timeline still folds legacy JSONL chunks for presentation compatibility;
    /// an agent history search must never turn a bounded request into an
    /// unbounded legacy transcript read. The supplied VFS cursor bounds both
    /// directory metadata and selected body reads.
    #[cfg(test)]
    pub(crate) fn load_immutable_message_record_page(
        &self,
        history_vfs: &VfsHandle,
        channel_id: &SessionId,
        after: Option<&VfsDirectoryCursor>,
        record_offset: usize,
        limit: usize,
        max_serialized_bytes: u64,
    ) -> Result<AgentChannelImmutableMessageRecordPage, AgentChannelStoreError> {
        Self::load_immutable_message_record_page_with_observer(
            history_vfs,
            &self.channels_root,
            channel_id,
            after,
            record_offset,
            limit,
            max_serialized_bytes,
            &|| {
                #[cfg(test)]
                self.message_record_body_reads
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
        )
    }

    pub(crate) fn load_immutable_message_record_page_from_vfs(
        history_vfs: &VfsHandle,
        channel_id: &SessionId,
        after: Option<&VfsDirectoryCursor>,
        record_offset: usize,
        limit: usize,
        max_serialized_bytes: u64,
    ) -> Result<AgentChannelImmutableMessageRecordPage, AgentChannelStoreError> {
        Self::load_immutable_message_record_page_with_observer(
            history_vfs,
            SIFS_AGENT_CHANNELS_ROOT,
            channel_id,
            after,
            record_offset,
            limit,
            max_serialized_bytes,
            &|| {},
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn load_immutable_message_record_page_with_observer(
        history_vfs: &VfsHandle,
        channels_root: &str,
        channel_id: &SessionId,
        after: Option<&VfsDirectoryCursor>,
        record_offset: usize,
        limit: usize,
        max_serialized_bytes: u64,
        body_read_observer: &dyn Fn(),
    ) -> Result<AgentChannelImmutableMessageRecordPage, AgentChannelStoreError> {
        let dir = sifs_agent_channel_message_records_dir_at(channels_root, channel_id);
        let page = history_vfs
            .list_page_beneath_no_links_if_exists(&dir, after, limit)
            .map_err(|source| channel_store_io(&dir, source.to_string()))?;
        let mut consumed_entries = 0usize;
        let mut remaining_serialized_bytes = max_serialized_bytes;
        let mut read_bytes = 0u64;
        let mut omitted_records = 0usize;
        let mut records = Vec::with_capacity(page.entries.len());
        for (index, entry) in page.entries.iter().enumerate() {
            if !matches!(entry.kind, VfsObjectKind::File | VfsObjectKind::Artifact)
                || !entry.name.ends_with(".json")
            {
                consumed_entries = consumed_entries.saturating_add(1);
                omitted_records = omitted_records.saturating_add(1);
                continue;
            }
            let Some(serialized_bytes) = entry.size_bytes else {
                consumed_entries = consumed_entries.saturating_add(1);
                omitted_records = omitted_records.saturating_add(1);
                continue;
            };
            if serialized_bytes > remaining_serialized_bytes {
                return Ok(AgentChannelImmutableMessageRecordPage {
                    records,
                    next_cursor: page.next_cursor,
                    consumed_entries,
                    read_bytes,
                    omitted_records,
                    required_bytes: Some(serialized_bytes),
                });
            }
            remaining_serialized_bytes =
                remaining_serialized_bytes.saturating_sub(serialized_bytes);
            // Directory metadata is only a discovery hint. Bound the actual
            // body read as well, then require the immutable record to still
            // have exactly the advertised size before deserializing it.
            body_read_observer();
            let read = history_vfs
                .read_range_beneath_no_links(&entry.path, 0, serialized_bytes)
                .map_err(|source| channel_store_io(&entry.path, source.to_string()))?;
            read_bytes = read_bytes.saturating_add(read.bytes.len() as u64);
            if read.truncated
                || read.bytes.len() as u64 != serialized_bytes
                || read.metadata.size_bytes != Some(serialized_bytes)
                || !matches!(
                    read.metadata.kind,
                    VfsObjectKind::File | VfsObjectKind::Artifact
                )
            {
                consumed_entries = consumed_entries.saturating_add(1);
                omitted_records = omitted_records.saturating_add(1);
                continue;
            }
            let message: AgentChannelMessage = match serde_json::from_slice(&read.bytes) {
                Ok(message) => message,
                Err(_) => {
                    consumed_entries = consumed_entries.saturating_add(1);
                    omitted_records = omitted_records.saturating_add(1);
                    continue;
                }
            };
            if json_record_id_from_path(&entry.path) != Some(message.message_id.as_str()) {
                return Err(AgentChannelStoreError::Serialization {
                    detail: format!(
                        "channel message record {} does not match path {}",
                        message.message_id, entry.path
                    ),
                });
            }
            records.push(AgentChannelImmutableMessageRecord {
                record_offset: record_offset.saturating_add(index),
                message,
            });
            consumed_entries = consumed_entries.saturating_add(1);
        }
        Ok(AgentChannelImmutableMessageRecordPage {
            records,
            next_cursor: page.next_cursor,
            consumed_entries,
            read_bytes,
            omitted_records,
            required_bytes: None,
        })
    }

    /// Read a bounded slice of the pre-immutable stable-channel JSONL log.
    ///
    /// Only `messages/*.jsonl` is eligible: the materialized channel snapshot,
    /// provider sessions, and mutable legacy index are never opened. Guarded
    /// range reads page arbitrarily large chunks without following links. Once
    /// a chunk is opened, its exact object/version is bound into the cursor so
    /// a continuation cannot mix bytes from a replacement object.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn load_legacy_message_record_page_from_vfs(
        history_vfs: &VfsHandle,
        channels_root: &str,
        channel_id: &SessionId,
        after: Option<&VfsDirectoryCursor>,
        expected_entry_name: Option<&str>,
        expected_object_id: Option<&str>,
        expected_version: Option<&str>,
        record_offset: usize,
        byte_offset: usize,
        discarding_line: bool,
        allow_discard_partial_line: bool,
        limit: usize,
        max_serialized_bytes: u64,
    ) -> Result<AgentChannelLegacyMessageRecordPage, AgentChannelStoreError> {
        let dir = sifs_agent_channel_message_chunks_dir_at(channels_root, channel_id);
        let page = history_vfs
            .list_page_beneath_no_links_if_exists(&dir, after, 1)
            .map_err(|source| channel_store_io(&dir, source.to_string()))?;
        let Some(entry) = page.entries.first() else {
            if byte_offset != 0
                || discarding_line
                || expected_entry_name.is_some()
                || expected_object_id.is_some()
                || expected_version.is_some()
            {
                return Err(channel_store_io(
                    &dir,
                    "invalid legacy message cursor: chunk is unavailable".to_string(),
                ));
            }
            return Ok(AgentChannelLegacyMessageRecordPage {
                records: Vec::new(),
                next_cursor: None,
                current_entry_name: None,
                current_object_id: None,
                current_version: None,
                next_byte_offset: 0,
                next_discarding_line: false,
                deferred_for_fresh_budget: false,
                consumed_entries: 0,
                read_bytes: 0,
                omitted_records: 0,
            });
        };
        if expected_entry_name.is_some_and(|expected| expected != entry.name.as_str()) {
            return Err(channel_store_io(
                &entry.path,
                "invalid legacy message cursor: chunk changed".to_string(),
            ));
        }

        let is_chunk = matches!(entry.kind, VfsObjectKind::File | VfsObjectKind::Artifact)
            && entry.name.ends_with(".jsonl");
        if !is_chunk {
            if byte_offset != 0
                || discarding_line
                || expected_entry_name.is_some()
                || expected_object_id.is_some()
                || expected_version.is_some()
            {
                return Err(channel_store_io(
                    &entry.path,
                    "invalid legacy message cursor: expected JSONL chunk".to_string(),
                ));
            }
            return Ok(AgentChannelLegacyMessageRecordPage {
                records: Vec::new(),
                next_cursor: page.next_cursor,
                current_entry_name: None,
                current_object_id: None,
                current_version: None,
                next_byte_offset: 0,
                next_discarding_line: false,
                deferred_for_fresh_budget: false,
                consumed_entries: 1,
                read_bytes: 0,
                omitted_records: 0,
            });
        }
        let read = history_vfs
            .read_range_beneath_no_links(&entry.path, byte_offset as u64, max_serialized_bytes)
            .map_err(|source| channel_store_io(&entry.path, source.to_string()))?;
        let read_bytes = read.bytes.len() as u64;
        if !matches!(
            read.metadata.kind,
            VfsObjectKind::File | VfsObjectKind::Artifact
        ) || read.metadata.size_bytes != entry.size_bytes
        {
            return Err(channel_store_io(
                &entry.path,
                "invalid legacy message cursor: chunk metadata changed".to_string(),
            ));
        }
        let object_id = read.metadata.object_id.clone().ok_or_else(|| {
            channel_store_io(
                &entry.path,
                "invalid legacy message cursor: chunk has no stable object identity".to_string(),
            )
        })?;
        let version = read.metadata.version.clone().ok_or_else(|| {
            channel_store_io(
                &entry.path,
                "invalid legacy message cursor: chunk has no stable version".to_string(),
            )
        })?;
        if expected_object_id.is_some_and(|expected| expected != object_id.as_str())
            || expected_version.is_some_and(|expected| expected != version.as_str())
        {
            return Err(channel_store_io(
                &entry.path,
                "invalid legacy message cursor: chunk identity changed".to_string(),
            ));
        }

        let mut records = Vec::new();
        let mut consumed_entries = 0usize;
        let mut omitted_records = 0usize;
        let mut cursor = 0usize;
        let mut discarding = discarding_line;
        if discarding {
            if let Some(line_end) = read.bytes.iter().position(|byte| *byte == b'\n') {
                cursor = line_end.saturating_add(1);
                consumed_entries = consumed_entries.saturating_add(1);
                omitted_records = omitted_records.saturating_add(1);
                discarding = false;
            } else if read.truncated {
                return Ok(AgentChannelLegacyMessageRecordPage {
                    records,
                    next_cursor: page.next_cursor,
                    current_entry_name: Some(entry.name.clone()),
                    current_object_id: Some(object_id),
                    current_version: Some(version),
                    next_byte_offset: byte_offset.saturating_add(read.bytes.len()),
                    next_discarding_line: true,
                    deferred_for_fresh_budget: false,
                    consumed_entries,
                    read_bytes,
                    omitted_records,
                });
            } else {
                consumed_entries = consumed_entries.saturating_add(1);
                omitted_records = omitted_records.saturating_add(1);
                cursor = read.bytes.len();
                discarding = false;
            }
        }
        while cursor < read.bytes.len() && consumed_entries < limit {
            let relative_end = read.bytes[cursor..].iter().position(|byte| *byte == b'\n');
            let Some(relative_end) = relative_end else {
                if read.truncated {
                    // A fresh-range budget proves this line is intrinsically
                    // oversized only when the line began at the range start.
                    // Complete earlier records can consume enough of an
                    // otherwise fresh range to split a normal following line;
                    // defer that tail so its own fresh range starts here.
                    if allow_discard_partial_line && cursor == 0 {
                        return Ok(AgentChannelLegacyMessageRecordPage {
                            records,
                            next_cursor: page.next_cursor,
                            current_entry_name: Some(entry.name.clone()),
                            current_object_id: Some(object_id),
                            current_version: Some(version),
                            next_byte_offset: byte_offset.saturating_add(read.bytes.len()),
                            next_discarding_line: true,
                            deferred_for_fresh_budget: false,
                            consumed_entries,
                            read_bytes,
                            omitted_records,
                        });
                    }
                    return Ok(AgentChannelLegacyMessageRecordPage {
                        records,
                        next_cursor: page.next_cursor,
                        current_entry_name: Some(entry.name.clone()),
                        current_object_id: Some(object_id),
                        current_version: Some(version),
                        next_byte_offset: byte_offset.saturating_add(cursor),
                        next_discarding_line: false,
                        deferred_for_fresh_budget: true,
                        consumed_entries,
                        read_bytes,
                        omitted_records,
                    });
                }
                // A final JSONL record need not end with a newline.
                let line = &read.bytes[cursor..];
                let line_record_offset = record_offset.saturating_add(consumed_entries);
                consumed_entries = consumed_entries.saturating_add(1);
                cursor = read.bytes.len();
                if line.iter().all(u8::is_ascii_whitespace) {
                    omitted_records = omitted_records.saturating_add(1);
                } else if let Ok(message) = serde_json::from_slice::<AgentChannelMessage>(line) {
                    let immutable_path = sifs_agent_channel_message_record_path_at(
                        channels_root,
                        channel_id,
                        &message.message_id,
                    );
                    match history_vfs.stat_beneath_no_links(&immutable_path) {
                        Ok(_) => {}
                        Err(VfsError::NotFound { .. }) => {
                            records.push(AgentChannelLegacyMessageRecord {
                                record_offset: line_record_offset,
                                next_byte_offset: 0,
                                message,
                            });
                        }
                        Err(source) => {
                            return Err(channel_store_io(&immutable_path, source.to_string()));
                        }
                    }
                } else {
                    omitted_records = omitted_records.saturating_add(1);
                }
                break;
            };
            let line_end = cursor.saturating_add(relative_end);
            let following = if line_end < read.bytes.len() {
                line_end.saturating_add(1)
            } else {
                line_end
            };
            let line = &read.bytes[cursor..line_end];
            let line_record_offset = record_offset.saturating_add(consumed_entries);
            consumed_entries = consumed_entries.saturating_add(1);
            cursor = following;
            if line.iter().all(u8::is_ascii_whitespace) {
                omitted_records = omitted_records.saturating_add(1);
                continue;
            }
            let message = match serde_json::from_slice::<AgentChannelMessage>(line) {
                Ok(message) => message,
                Err(_) => {
                    omitted_records = omitted_records.saturating_add(1);
                    continue;
                }
            };
            let immutable_path = sifs_agent_channel_message_record_path_at(
                channels_root,
                channel_id,
                &message.message_id,
            );
            match history_vfs.stat_beneath_no_links(&immutable_path) {
                Ok(_) => continue,
                Err(VfsError::NotFound { .. }) => {}
                Err(source) => {
                    return Err(channel_store_io(&immutable_path, source.to_string()));
                }
            }
            records.push(AgentChannelLegacyMessageRecord {
                record_offset: line_record_offset,
                next_byte_offset: if cursor < read.bytes.len() || read.truncated {
                    byte_offset.saturating_add(cursor)
                } else {
                    0
                },
                message,
            });
        }
        let next_byte_offset = if cursor < read.bytes.len() || read.truncated {
            byte_offset.saturating_add(cursor)
        } else {
            0
        };
        Ok(AgentChannelLegacyMessageRecordPage {
            records,
            next_cursor: page.next_cursor,
            current_entry_name: Some(entry.name.clone()),
            current_object_id: Some(object_id),
            current_version: Some(version),
            next_byte_offset,
            next_discarding_line: discarding,
            deferred_for_fresh_budget: false,
            consumed_entries,
            read_bytes,
            omitted_records,
        })
    }

    pub fn load_timeline_overview(
        &self,
        channel_id: &SessionId,
    ) -> Result<AgentChannelTimelineOverview, AgentChannelStoreError> {
        Ok(Self::overview_from_messages(
            channel_id,
            &self.load_all_messages(channel_id)?,
        ))
    }

    fn overview_from_messages(
        channel_id: &SessionId,
        messages: &[AgentChannelMessage],
    ) -> AgentChannelTimelineOverview {
        let total_messages = messages.len() as u64;
        if messages.is_empty() {
            return AgentChannelTimelineOverview {
                channel_id: channel_id.clone(),
                total_messages: 0,
                oldest_sequence: None,
                newest_sequence: None,
                chunk_size: SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE,
                segments: Vec::new(),
            };
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

        AgentChannelTimelineOverview {
            channel_id: channel_id.clone(),
            total_messages,
            oldest_sequence: segments.first().map(|segment| segment.start_sequence),
            newest_sequence: segments.last().map(|segment| segment.end_sequence),
            chunk_size: SIFS_AGENT_CHANNEL_LOG_CHUNK_SIZE,
            segments,
        }
    }

    fn channel_ids_from_sifs(&self) -> Result<BTreeSet<SessionId>, AgentChannelStoreError> {
        let entries = self
            .browser
            .list_if_exists(&self.channels_root)
            .map_err(|detail| channel_store_io(&self.channels_root, detail))?;
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
        summaries.sort_by_key(|summary| std::cmp::Reverse(summary.updated_at));
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

    /// Summaries of deleted channels, which the visible listing hides.
    pub fn list_deleted_channel_summaries(
        &self,
    ) -> Result<Vec<AgentChannelSummary>, AgentChannelStoreError> {
        let ids = self.channel_ids_from_sifs()?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .channel_summaries_from_ids(&ids)?
            .into_iter()
            .filter(|summary| summary.deleted_at.is_some())
            .collect())
    }
}

impl AgentChannelStore for SifsAgentChannelStore {
    fn save_channel(&self, channel: &AgentChannel) -> Result<(), AgentChannelStoreError> {
        let record = self.append_metadata_record(channel)?;
        let source_record_id = record.record_id.clone();
        let loaded = self.load_channel_metadata(&channel.channel_id)?;
        // A routed DFS write can commit before this node's namespace view has
        // observed the immutable record. Fold the exact record we just wrote
        // into the materialized cache instead of turning normal propagation
        // delay into a failed chat send. A later read still enumerates and
        // reconciles all immutable records, including concurrent writers.
        let (mut merged, applied) = reconcile_written_metadata_record(loaded, record)?;
        self.write_history_metadata_projection(&channel.channel_id, &source_record_id)?;
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

    fn load_message(
        &self,
        channel_id: &SessionId,
        message_id: &TaskId,
    ) -> Result<Option<AgentChannelMessage>, AgentChannelStoreError> {
        self.load_immutable_message_record(channel_id, message_id)
    }

    fn list_channels(&self) -> Result<Vec<AgentChannel>, AgentChannelStoreError> {
        let mut channels = Vec::new();
        for channel_id in self.channel_ids_from_sifs()? {
            if let Some(channel) = self.load_channel(&channel_id)?
                && !channel.is_deleted()
            {
                channels.push(channel);
            }
        }
        channels.sort_by_key(|channel| std::cmp::Reverse(channel.updated_at));
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
        let path = self.attachment_path(channel_id, attachment_id, file_name);
        self.write_file(&path, bytes)?;
        Ok(path)
    }
}

/// Materialize one provider-neutral user message in a stable SIFS chat.
///
/// The message id and timestamp must come from the stable surface event that
/// accepted the input. Replaying the same event is idempotent; a conflicting
/// body under the same id is rejected instead of rewriting history.
pub fn project_stable_chat_user_message(
    store: &dyn AgentChannelStore,
    channel_id: &SessionId,
    message_id: TaskId,
    created_at: OffsetDateTime,
    content: &str,
    source: AgentChannelSource,
) -> Result<(), AgentChannelStoreError> {
    let channel = store
        .load_channel(channel_id)?
        .ok_or_else(|| AgentChannelStoreError::Io {
            path: channel_id.to_string(),
            detail: "stable chat channel does not exist".to_string(),
        })?;
    project_stable_chat_message(
        store,
        channel,
        message_id,
        created_at,
        AgentChannelAuthor::User {
            user_id: AGENT_CHANNEL_APP_USER_ID.to_string(),
            display_name: "User".to_string(),
        },
        content,
        source,
    )
}

/// Materialize one provider-neutral agent message in a stable SIFS chat.
///
/// The channel's current durable membership is the authority. Realtime/provider
/// output may never create or re-enable a participant.
#[allow(clippy::too_many_arguments)]
pub fn project_stable_chat_agent_message(
    store: &SifsAgentChannelStore,
    channel_id: &SessionId,
    agent_key: &str,
    agent_name: &str,
    agent_session_id: &SessionId,
    message_id: TaskId,
    created_at: OffsetDateTime,
    content: &str,
    source: AgentChannelSource,
) -> Result<(), AgentChannelStoreError> {
    let channel = store
        .load_channel(channel_id)?
        .ok_or_else(|| AgentChannelStoreError::Io {
            path: channel_id.to_string(),
            detail: "stable chat channel does not exist".to_string(),
        })?;
    let agent_key = if agent_key.trim().is_empty() {
        "main-agent"
    } else {
        agent_key.trim()
    };
    let _ = (agent_name, agent_session_id);
    let participant = channel
        .participants
        .iter()
        .find(|participant| participant.agent_key == agent_key && participant.enabled)
        .ok_or_else(|| AgentChannelStoreError::Serialization {
            detail: format!(
                "stable chat agent `{agent_key}` is not an enabled channel participant"
            ),
        })?;
    let author = AgentChannelAuthor::Agent {
        participant_id: participant.participant_id.clone(),
        agent_key: participant.agent_key.clone(),
        agent_name: participant.agent_name.clone(),
        agent_session_id: participant.agent_session_id.clone(),
    };
    project_stable_chat_message(
        store, channel, message_id, created_at, author, content, source,
    )
}

fn project_stable_chat_message(
    store: &dyn AgentChannelStore,
    channel: AgentChannel,
    message_id: TaskId,
    created_at: OffsetDateTime,
    author: AgentChannelAuthor,
    content: &str,
    source: AgentChannelSource,
) -> Result<(), AgentChannelStoreError> {
    if channel.is_deleted() {
        return Err(AgentChannelStoreError::Io {
            path: channel.channel_id.to_string(),
            detail: "stable chat channel is deleted".to_string(),
        });
    }
    if let Some(existing) = store.load_message(&channel.channel_id, &message_id)? {
        if existing.author != author
            || existing.message_kind != AgentChannelMessageKind::Normal
            || existing.content != content
            || existing.created_at != created_at
            || existing.source != source
            || !existing.attachments.is_empty()
        {
            return Err(AgentChannelStoreError::Serialization {
                detail: format!(
                    "stable chat message {message_id} already exists with different content"
                ),
            });
        }
        return Ok(());
    }

    let message = AgentChannelMessage {
        message_id,
        sequence: channel.next_sequence,
        author,
        message_kind: AgentChannelMessageKind::Normal,
        content: content.to_string(),
        created_at,
        source,
        attachments: Vec::new(),
        produced_by: None,
        audience: AgentChannelAudience::Everyone,
        reply_to: None,
    };
    // Surface projection is not an authority-changing operation. Append only
    // the immutable message record: saving a channel snapshot derived before a
    // concurrent revocation could otherwise restore stale membership.
    store.append_message(&channel.channel_id, &message)
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

    reconcile_channel_workspace_projection(&mut merged, &existing, &candidate, newer)?;

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
                let merged_membership = (
                    merged_participant.membership_revision,
                    merged_participant.enabled,
                );
                let candidate_membership = (participant.membership_revision, participant.enabled);
                if candidate_membership.0 > merged_membership.0
                    || (candidate_membership.0 == merged_membership.0
                        && !candidate_membership.1
                        && merged_membership.1)
                {
                    merged_participant.enabled = candidate_membership.1;
                    merged_participant.membership_revision = candidate_membership.0;
                }
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

fn reconcile_channel_workspace_projection(
    merged: &mut AgentChannel,
    left: &AgentChannel,
    right: &AgentChannel,
    newer: &AgentChannel,
) -> Result<(), AgentChannelStoreError> {
    let left_authoritative = left.has_authoritative_workspace_projection();
    let right_authoritative = right.has_authoritative_workspace_projection();
    match (left_authoritative, right_authoritative) {
        (false, false) => {
            copy_channel_workspace_projection(merged, newer);
        }
        (true, false) => copy_channel_workspace_projection(merged, left),
        (false, true) => copy_channel_workspace_projection(merged, right),
        (true, true) => {
            let mut left = left.clone();
            let mut right = right.clone();
            left.ensure_workspace_causal_register();
            right.ensure_workspace_causal_register();

            let head_ids = left
                .workspace_causal_heads
                .keys()
                .chain(right.workspace_causal_heads.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut heads = BTreeMap::new();
            for head_id in head_ids {
                match (
                    left.workspace_causal_heads.get(&head_id),
                    right.workspace_causal_heads.get(&head_id),
                ) {
                    (Some(left_value), Some(right_value)) if left_value == right_value => {
                        heads.insert(head_id, left_value.clone());
                    }
                    (Some(_), Some(_)) => {
                        // A valid generated head id cannot carry two values.
                        // Treat a malformed/colliding head as exact empty so it
                        // remains conservatively sticky in later joins.
                        heads.insert(head_id, Vec::new());
                    }
                    (Some(left_value), None)
                        if !right.workspace_causal_tokens.contains(&head_id) =>
                    {
                        heads.insert(head_id, left_value.clone());
                    }
                    (None, Some(right_value))
                        if !left.workspace_causal_tokens.contains(&head_id) =>
                    {
                        heads.insert(head_id, right_value.clone());
                    }
                    (Some(_), None) | (None, Some(_)) | (None, None) => {}
                }
            }

            merged.workspace_causal_tokens = left
                .workspace_causal_tokens
                .union(&right.workspace_causal_tokens)
                .cloned()
                .collect();
            merged.workspace_causal_heads = heads;
            merged.workspace_causal_version = AGENT_CHANNEL_WORKSPACE_CAUSAL_VERSION;
            merged.workspaces = merged.materialized_workspace_projection();
            merged.workspace_revision = left.workspace_revision.max(right.workspace_revision);
            merged.workspace_grant_revisions = merged
                .workspaces
                .iter()
                .map(|workspace| {
                    let revision = left
                        .workspace_grant_revisions
                        .get(&workspace.id)
                        .copied()
                        .unwrap_or_default()
                        .max(
                            right
                                .workspace_grant_revisions
                                .get(&workspace.id)
                                .copied()
                                .unwrap_or_default(),
                        );
                    (workspace.id.clone(), revision)
                })
                .collect();
            merged.workspace_projection_version = left
                .workspace_projection_version
                .max(right.workspace_projection_version)
                .max(AGENT_CHANNEL_WORKSPACE_PROJECTION_VERSION);
        }
    }
    Ok(())
}

fn copy_channel_workspace_projection(target: &mut AgentChannel, source: &AgentChannel) {
    target.workspaces = source.workspaces.clone();
    target.workspace_projection_version = source.workspace_projection_version;
    target.workspace_revision = source.workspace_revision;
    target.workspace_grant_revisions = source.workspace_grant_revisions.clone();
    target.workspace_causal_tokens = source.workspace_causal_tokens.clone();
    target.workspace_causal_heads = source.workspace_causal_heads.clone();
    target.workspace_causal_version = source.workspace_causal_version;
}

fn canonicalize_channel_messages(
    messages: Vec<AgentChannelMessage>,
) -> Result<Vec<AgentChannelMessage>, AgentChannelStoreError> {
    Ok(sequence_channel_messages(
        deduplicate_channel_messages_preserving_sequence(messages)?,
    ))
}

fn sequence_channel_messages(mut messages: Vec<AgentChannelMessage>) -> Vec<AgentChannelMessage> {
    messages.sort_by(|left, right| {
        left.sequence
            .cmp(&right.sequence)
            .then_with(|| left.created_at.cmp(&right.created_at))
            .then_with(|| left.message_id.cmp(&right.message_id))
    });
    for (index, message) in messages.iter_mut().enumerate() {
        message.sequence = (index as u64).saturating_add(1);
    }
    messages
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

    fn open_daemon_node_on_runtime(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
        node_id: NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        SifsBrowser::open_daemon_node_on_runtime(data_dir, address, node_id, runtime)
            .map(|browser| Self { browser })
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
    // Workspace authority belongs to the channel. Ignore this legacy shadow
    // while comparing private session state, but retain the shadow from the
    // complete snapshot selected below. Splicing a local binding into a newer
    // roaming snapshot would pair state that never existed together.
    let mut comparable_local = local.clone();
    let mut comparable_roaming = roaming.clone();
    comparable_local.agent_definition_key = reconciled_agent_key.clone();
    comparable_roaming.agent_definition_key = reconciled_agent_key.clone();
    comparable_local.workspace_binding = None;
    comparable_roaming.workspace_binding = None;
    if comparable_local == comparable_roaming {
        let mut selected = roaming;
        selected.agent_definition_key = reconciled_agent_key;
        return Ok(Some(selected));
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

/// Shared executor view used by model-facing adapters such as realtime voice.
///
/// Cloning this value keeps the same [`AgentToolRegistry`] implementation
/// instances and final authority checks; only the model transport and result
/// delivery differ from the regular turn-based agent loop.
#[derive(Clone)]
pub struct AgentChatToolContext {
    attribution: sinomo_runtime::api::app::usage::UsageAttribution,
    registry: AgentToolRegistry,
    session: InteractionSession,
    plan: AgentToolPlan,
    surface: AgentToolSurface,
    run_context: AgentRunContext,
    descriptors: Vec<AgentToolDescriptor>,
}

impl AgentChatToolContext {
    fn new(
        registry: AgentToolRegistry,
        session: InteractionSession,
        definition: AgentDefinition,
        module_tool_projection: sinomo_agent::AgentModuleToolProjection,
        provider: &str,
        model: &str,
    ) -> Self {
        let config = sinomo_agent::AgentConfig::new(provider, model)
            .with_agent_definition(definition.clone())
            .with_module_tool_projection(module_tool_projection);
        let plan = AgentToolPlanner::plan(
            registry.descriptors(),
            &config.module_tool_projection,
            std::iter::empty(),
            config.definition.tools.clone(),
            true,
            config.visible_toolsets.clone(),
            registry.generation(),
        )
        .for_invocation_mode(AgentToolInvocationMode::Realtime);
        let surface = AgentToolSurface::for_definition(&definition);
        let executable_descriptors = plan
            .entries
            .iter()
            .filter(|entry| entry.executable && plan.is_model_visible(&entry.descriptor.spec.name))
            .map(|entry| entry.descriptor.clone())
            .collect();
        let (descriptors, _) = surface.filter_tool_descriptors(executable_descriptors);
        Self {
            attribution: sinomo_runtime::api::app::usage::UsageAttribution {
                agent: Some(definition.key().to_string()),
                session: Some(session.session_id.to_string()),
                chat: session.conversation_id.as_ref().map(ToString::to_string),
                participant: session.participant_id.as_ref().map(ToString::to_string),
                origin: "realtime".into(),
                purpose: "tool_call".into(),
                ..Default::default()
            },
            registry,
            session,
            plan,
            surface,
            run_context: AgentRunContext::default(),
            descriptors,
        }
    }

    pub fn descriptors(&self) -> &[AgentToolDescriptor] {
        &self.descriptors
    }

    pub fn execute(
        &self,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: sinomo_agent::AgentCancellationToken,
    ) -> AgentToolOutcome {
        let _attribution = sinomo_lm::usage::enter_attribution(self.attribution.clone());
        if !self
            .descriptors
            .iter()
            .any(|descriptor| descriptor.spec.name == call.tool)
        {
            let summary = format!(
                "tool `{}` is not exposed to the realtime model interaction",
                call.tool
            );
            let output = serde_json::json!({
                "error": "tool_not_available_for_invocation_mode",
                "invocation_mode": "realtime",
                "tool": call.tool,
            });
            let outcome = AgentToolOutcome {
                call_id: TaskId::generate(),
                step_index: 0,
                call_index,
                call: call.clone(),
                status: AgentToolStatus::Blocked,
                summary: summary.clone(),
                output: output.clone(),
                event_id: None,
                evidence: AgentToolEvidence::availability_only(false, summary)
                    .with_output_value(&output, false),
            };
            self.registry
                .record_usage(&self.session, &outcome.call_id, &outcome, 0, false);
            return outcome;
        }
        AgentToolOrchestrator::new(&self.surface, None, &self.run_context)
            .execute_with_cancellation(
                &self.registry,
                &self.session,
                &self.plan,
                0,
                call_index,
                call,
                cancellation_token,
            )
    }
}

/// One running chat conversation backed by the main-agent runtime.
pub struct AgentChat {
    data_dir: SinomoDataDir,
    daemon_connected: bool,
    agent_catalog_vfs: VfsHandle,
    agent_catalog_authoritative: bool,
    channel_store: Box<dyn AgentChannelStore>,
    session_checkpoint_store: SifsAgentSessionCheckpointStore,
    channel: AgentChannel,
    runtime: InteractionRuntime<InMemoryEventStore, Handler>,
    /// Runtimes of channel agent participants, kept across turns.
    participant_runtimes: BTreeMap<TaskId, InteractionRuntime<InMemoryEventStore, Handler>>,
    /// The channel `participant_runtimes` belong to.
    participant_runtimes_channel: Option<SessionId>,
    /// Channel participants without a runtime whose sessions hold detached
    /// tool calls not yet continued with; their runtimes are created to
    /// recover them.
    participants_awaiting_recovery: BTreeSet<TaskId>,
    tool_runtime: ChatToolRuntimeSelection,
    tool_runtime_explicit_override: bool,
    resume_sync_warnings: Vec<String>,
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
        if daemon_connected {
            return Err(CliError::DaemonNotRunning {
                detail: "connected GUI chat construction requires a filesystem provider transport"
                    .to_string(),
            });
        }
        let channel_store =
            SifsAgentChannelStore::open(&data_dir).map_err(|detail| CliError::Parse {
                detail: format!("open agent channel SIFS store failed: {detail}"),
            })?;
        let session_checkpoint_store =
            SifsAgentSessionCheckpointStore::open(&data_dir).map_err(|detail| CliError::Parse {
                detail: format!("open agent session checkpoint SIFS store failed: {detail}"),
            })?;
        let agent_catalog_vfs =
            sinomo_runtime::vfs::mounted_sifs(Some(data_dir.root())).map_err(|source| {
                CliError::Parse {
                    detail: format!("open local fallback agent catalog SIFS failed: {source}"),
                }
            })?;
        Self::with_runtime_control_registry_and_stores(
            data_dir,
            daemon_connected,
            runtime_control_registry,
            agent_catalog_vfs,
            false,
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
        let agent_catalog_vfs =
            storage
                .open_agent_catalog_vfs()
                .map_err(|detail| CliError::Parse {
                    detail: format!("open filesystem provider agent catalog SIFS failed: {detail}"),
                })?;
        let channel_store = storage
            .open_channel_store()
            .map_err(|detail| CliError::Parse {
                detail: format!("open agent channel SIFS store failed: {detail}"),
            })?;
        let session_checkpoint_store =
            match storage.provider().map_err(|detail| CliError::Parse {
                detail: format!("resolve agent filesystem provider failed: {detail}"),
            })? {
                AgentChatSifsProvider::LocalDaemon => {
                    SifsAgentSessionCheckpointStore::open_daemon_on_runtime(
                        &storage.address,
                        &storage.runtime,
                    )
                }
                AgentChatSifsProvider::ClusterNode(node_id) => {
                    SifsAgentSessionCheckpointStore::open_daemon_node_on_runtime(
                        storage.data_dir.clone(),
                        &storage.address,
                        node_id,
                        &storage.runtime,
                    )
                }
            }
            .map_err(|detail| CliError::Parse {
                detail: format!("open agent session checkpoint SIFS store failed: {detail}"),
            })?;
        Self::with_runtime_control_registry_and_stores(
            data_dir,
            daemon_connected,
            runtime_control_registry,
            agent_catalog_vfs,
            true,
            Box::new(channel_store),
            session_checkpoint_store,
        )
    }

    fn with_runtime_control_registry_and_stores(
        data_dir: SinomoDataDir,
        daemon_connected: bool,
        runtime_control_registry: AgentRuntimeControlRegistry,
        agent_catalog_vfs: VfsHandle,
        agent_catalog_authoritative: bool,
        channel_store: Box<dyn AgentChannelStore>,
        session_checkpoint_store: SifsAgentSessionCheckpointStore,
    ) -> Result<Self, CliError> {
        let runtime_skill_catalog_vfs = daemon_connected.then(|| agent_catalog_vfs.clone());
        let handler =
            tui::app_build_chat_handler(&data_dir, daemon_connected, runtime_skill_catalog_vfs)?
                .with_runtime_control_registry(runtime_control_registry);
        let session = app_session(&data_dir, None);
        let mut runtime =
            InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());
        runtime
            .handler_mut()
            .replace_agent_session(&session, agent_transcript_from_history(&[]))?;
        let mut channel = AgentChannel::with_default_title(session.session_id.clone());
        let info: ChatRuntimeInfo = tui::app_runtime_info(runtime.handler()).into();
        let tool_runtime =
            ChatToolRuntimeSelection::from_agent_definition(runtime.handler().agent_definition());
        let align_primary_session = should_align_primary_participant(&channel);
        let participant_id = ensure_primary_participant(&mut channel, &info, align_primary_session);
        set_participant_tool_runtime(&mut channel, &participant_id, &tool_runtime);
        Ok(Self {
            data_dir,
            daemon_connected,
            agent_catalog_vfs,
            agent_catalog_authoritative,
            channel_store,
            session_checkpoint_store,
            channel,
            runtime,
            participant_runtimes: BTreeMap::new(),
            participant_runtimes_channel: None,
            participants_awaiting_recovery: BTreeSet::new(),
            tool_runtime,
            tool_runtime_explicit_override: false,
            resume_sync_warnings: Vec::new(),
        })
    }

    #[cfg(test)]
    fn use_injected_authoritative_agent_catalog_for_test(&mut self) {
        self.agent_catalog_authoritative = true;
    }

    /// Begin a brand-new chat (clears the in-memory transcript/context).
    pub fn reset_session(&mut self) {
        self.resume_sync_warnings.clear();
        self.participants_awaiting_recovery.clear();
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
            self.tool_runtime = ChatToolRuntimeSelection::from_agent_definition(
                self.runtime.handler().agent_definition(),
            );
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
        let previous_store = self.runtime.store().clone();
        let previous_interaction_session = self.runtime.session().clone();
        let previous_channel = self.channel.clone();
        let previous_tool_runtime = self.tool_runtime.clone();
        let previous_tool_runtime_explicit_override = self.tool_runtime_explicit_override;
        let previous_resume_sync_warnings = self.resume_sync_warnings.clone();
        let handler_snapshot = self
            .runtime
            .handler_mut()
            .transition_snapshot(&previous_interaction_session)?;

        match self.resume_session_inner(session_id, history) {
            Ok(()) => {
                self.find_participants_awaiting_recovery()?;
                Ok(())
            }
            Err(error) => {
                let data_dir = self.data_dir.clone();
                let rollback = self
                    .runtime
                    .handler_mut()
                    .restore_transition_snapshot(&data_dir, handler_snapshot);
                self.runtime
                    .replace_store_and_session(previous_store, previous_interaction_session);
                self.channel = previous_channel;
                self.tool_runtime = previous_tool_runtime;
                self.tool_runtime_explicit_override = previous_tool_runtime_explicit_override;
                self.resume_sync_warnings = previous_resume_sync_warnings;
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(CliError::Parse {
                        detail: format!(
                            "resume chat session failed: {error}; rollback failed: {rollback}"
                        ),
                    }),
                }
            }
        }
    }

    fn resume_session_inner(
        &mut self,
        session_id: &str,
        history: &[ChatHistoryMessage],
    ) -> Result<(), CliError> {
        self.resume_sync_warnings.clear();
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
        if !loaded_existing_channel && (local_session.is_some() || !history.is_empty()) {
            // Only an existing standalone session needs legacy workspace
            // migration. A fresh ID already has an authoritative empty grant.
            channel.workspace_projection_version = 0;
        }
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
                default_tool_runtime_for_agent(
                    &self.agent_catalog_vfs,
                    self.agent_catalog_authoritative,
                    &participant_agent_key,
                )?
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
        // A group channel's outer runtime and its participants have distinct
        // sessions. Participant checkpoints belong to load_participant_session;
        // only an aligned chat participant can restore this outer session.
        let roaming_session =
            if participant_id.is_some() && participant.agent_session_id == session.session_id {
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
                let definition = resolve_agent(
                    &self.agent_catalog_vfs,
                    self.agent_catalog_authoritative,
                    &persisted_agent_key,
                )?
                .ok_or_else(|| CliError::Parse {
                        detail: format!(
                            "agent session `{}` references unknown agent definition `{persisted_agent_key}`",
                            persisted.session_id
                        ),
                    })?;
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
                let session_participant_id = participant_id.as_ref().filter(|participant_id| {
                    channel
                        .participant(participant_id)
                        .is_some_and(|participant| {
                            participant.agent_session_id == persisted.session_id
                        })
                });
                if let Some(participant_id) = session_participant_id {
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
                } else {
                    sync_participant_session(&channel_view, &mut participant, &mut persisted);
                }
                FileAgentSessionStore::for_data_dir(&self.data_dir)
                    .save_session(&persisted)
                    .map_err(|source| CliError::Parse {
                        detail: format!("persist reconciled chat session failed: {source}"),
                    })?;
                tui::app_switch_agent(
                    self.runtime.handler_mut(),
                    &self.data_dir,
                    definition,
                    self.daemon_connected,
                )?;
                tui::app_restore_agent_session(
                    self.runtime.handler_mut(),
                    &self.data_dir,
                    &session,
                    Some(persisted),
                    sinomo_agent::AgentTranscript::new(),
                )?;
            }
            None => {
                let definition = resolve_agent(
                    &self.agent_catalog_vfs,
                    self.agent_catalog_authoritative,
                    &participant.agent_key,
                )?
                .ok_or_else(|| CliError::Parse {
                    detail: format!(
                        "chat participant references unknown agent definition `{}`",
                        participant.agent_key
                    ),
                })?;
                tui::app_switch_agent(
                    self.runtime.handler_mut(),
                    &self.data_dir,
                    definition,
                    self.daemon_connected,
                )?;
                tui::app_restore_agent_session(
                    self.runtime.handler_mut(),
                    &self.data_dir,
                    &session,
                    None,
                    agent_transcript_from_channel(&channel, history),
                )?;
            }
        }
        self.channel = channel;
        self.configure_tool_runtime_for_session(&session.session_id)
            .map_err(|error| CliError::Parse {
                detail: format!("configure resumed chat tool runtime failed: {error}"),
            })?;
        if (!loaded_existing_channel || self.channel != channel_before_resume)
            && let Err(error) = self.save_channel()
        {
            self.resume_sync_warnings
                .push(format!("save reconciled chat channel failed: {error}"));
        }
        if let Some(projected_session) = self.runtime.handler().agent_session_snapshot()?
            && !self.is_unstarted_default_chat()
            && let Some(projected_agent_key) = projected_session
                .agent_definition_key
                .as_deref()
                .filter(|key| !key.trim().is_empty())
            && let Some(projected_participant) = self.channel.participants.iter().find(|entry| {
                entry.enabled
                    && entry.agent_session_id == projected_session.session_id
                    && entry.agent_key == projected_agent_key
            })
            && let Err(detail) = self.session_checkpoint_store.publish(
                &self.channel,
                projected_participant,
                &projected_session,
            )
        {
            self.resume_sync_warnings.push(format!(
                "publish projected agent session checkpoint failed: {detail}"
            ));
        }
        Ok(())
    }

    /// Return non-fatal SIFS synchronization failures from the latest resume.
    /// The local runtime remains usable when a roaming checkpoint cannot be published.
    pub fn take_resume_sync_warnings(&mut self) -> Vec<String> {
        std::mem::take(&mut self.resume_sync_warnings)
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

    pub fn tool_context(&self) -> AgentChatToolContext {
        let handler = self.runtime.handler();
        let info = tui::app_runtime_info(handler);
        let mut session = self.runtime.session().clone();
        session.conversation_id = Some(self.channel.channel_id.clone());
        session.participant_id = self
            .channel
            .participants
            .iter()
            .find(|p| p.agent_session_id == session.session_id)
            .map(|p| p.participant_id.clone());
        AgentChatToolContext::new(
            handler.tool_registry(),
            session,
            handler.agent_definition().clone(),
            handler.module_tool_projection(),
            &info.provider,
            &info.model,
        )
    }

    pub fn workspaces(&self) -> Vec<sinomo_agent::WorkspaceRef> {
        self.channel.workspaces.clone()
    }

    /// Refresh presentation from the conversation owner without creating a grant.
    /// Tool execution reconciles this projection into its sandbox before each turn.
    pub fn reload_workspaces(&mut self) -> Result<(), CliError> {
        self.reload_current_channel()?;
        self.tool_runtime.workspace_root = channel_primary_workspace_root(
            &self.data_dir,
            &self.channel,
            &self.runtime.session().session_id,
        )?;
        Ok(())
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
        selection.workspace_root = None;
        self.set_tool_runtime(selection)
    }

    pub fn set_tool_runtime(
        &mut self,
        selection: ChatToolRuntimeSelection,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        self.set_tool_runtime_with_workspace_replacement(selection, None)
    }

    /// Replace the chat/channel's complete ordered workspace grant set. This
    /// is the explicit authority-changing API; `set_tool_runtime(None)` keeps
    /// the durable projection so reconnect and node-only updates cannot revoke
    /// or recreate grants from stale UI state.
    pub fn replace_workspaces(
        &mut self,
        workspaces: Vec<WorkspaceRef>,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        let mut selection = self.tool_runtime.clone();
        selection.workspace_root = None;
        self.set_tool_runtime_with_workspace_replacement(selection, Some(workspaces))
    }

    /// Revoke every workspace from this conversation while retaining the
    /// session's sandbox attachment and other tool-runtime placement settings.
    pub fn clear_workspaces(&mut self) -> Result<ChatToolRuntimeSelection, CliError> {
        self.reload_current_channel()?;
        self.replace_workspaces(Vec::new())
    }

    /// Replace the conversation's complete workspace grant set with one
    /// user-selected folder, or revoke all workspace access when absent.
    pub fn replace_workspace_root(
        &mut self,
        workspace_root: Option<PathBuf>,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        let workspaces = workspace_root
            .map(|root| {
                workspace_ref_for_selected_root(&self.data_dir, &root)
                    .map(|workspace| workspace.binding().clone())
            })
            .transpose()?
            .into_iter()
            .collect();
        self.replace_workspaces(workspaces)
    }

    /// Apply an explicit addition to the latest conversation grant set. Existing
    /// roots retain their order, and adding an attached folder is idempotent.
    pub fn add_workspace_root(
        &mut self,
        root: PathBuf,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        self.reload_current_channel()?;
        let workspace = workspace_ref_for_selected_root(&self.data_dir, &root)?
            .binding()
            .clone();
        let mut workspaces = self.channel.workspaces.clone();
        if !workspaces
            .iter()
            .any(|existing| existing.id == workspace.id)
        {
            workspaces.push(workspace);
        }
        self.replace_workspaces(workspaces)
    }

    pub fn remove_workspace(
        &mut self,
        workspace_id: &str,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        self.reload_current_channel()?;
        let workspaces = self
            .channel
            .workspaces
            .iter()
            .filter(|workspace| workspace.id != workspace_id)
            .cloned()
            .collect();
        self.replace_workspaces(workspaces)
    }

    pub fn make_workspace_default(
        &mut self,
        workspace_id: &str,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        self.reload_current_channel()?;
        let mut workspaces = self.channel.workspaces.clone();
        let index = workspaces
            .iter()
            .position(|workspace| workspace.id == workspace_id)
            .ok_or_else(|| CliError::Parse {
                detail: "workspace is no longer attached to this conversation".into(),
            })?;
        let workspace = workspaces.remove(index);
        workspaces.insert(0, workspace);
        self.replace_workspaces(workspaces)
    }

    fn set_tool_runtime_with_workspace_replacement(
        &mut self,
        mut selection: ChatToolRuntimeSelection,
        workspace_replacement: Option<Vec<WorkspaceRef>>,
    ) -> Result<ChatToolRuntimeSelection, CliError> {
        selection.sandbox_node_id = selection
            .sandbox_node_id
            .map(|node_id| node_id.trim().to_string())
            .filter(|node_id| !node_id.is_empty());
        if let Some(node_id) = selection.sandbox_node_id.as_deref() {
            parse_chat_tool_runtime_node_id(node_id)?;
        }
        if let Some(root) = selection.workspace_root.as_ref()
            && root.as_os_str().is_empty()
        {
            return Err(CliError::Parse {
                detail: "workspace directory cannot be empty".to_string(),
            });
        }

        let session_id = self.runtime.session().session_id.clone();
        let sandbox_node_id = parse_chat_tool_runtime_selection(&selection)?;
        let previous_channel = self.channel.clone();
        if let Some(workspaces) = workspace_replacement {
            self.channel.replace_workspaces(workspaces);
        } else if let Some(root) = selection.workspace_root.as_ref() {
            let workspace = workspace_ref_for_selected_root(&self.data_dir, root)?
                .binding()
                .clone();
            let workspace_id = workspace.id.clone();
            self.channel.add_workspace(workspace);
            self.channel.move_workspace(&workspace_id, 0);
        }
        let conversation_workspace =
            match channel_primary_workspace_root(&self.data_dir, &self.channel, &session_id) {
                Ok(workspace) => workspace,
                Err(error) => {
                    self.channel = previous_channel;
                    return Err(error);
                }
            };
        let requested_workspace_root = conversation_workspace
            .as_deref()
            .or(selection.workspace_root.as_deref());
        let workspace_projection = self
            .channel
            .has_authoritative_workspace_projection()
            .then_some(self.channel.workspaces.as_slice());
        let selected_root = match self
            .runtime
            .handler_mut()
            .configure_app_tool_runtime_for_session(
                &self.data_dir,
                &session_id,
                sandbox_node_id,
                workspace_projection,
                requested_workspace_root,
                self.daemon_connected,
            ) {
            Ok(root) => root,
            Err(error) => {
                self.channel = previous_channel;
                return Err(error);
            }
        };
        if self.channel.workspaces.is_empty()
            && let Some(root) = selected_root.as_ref()
        {
            let workspace = workspace_ref_for_selected_root(&self.data_dir, root)?
                .binding()
                .clone();
            self.channel.add_workspace(workspace);
        }
        self.channel.mark_workspace_projection_authoritative();
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
        // Workspace grants belong to the channel and must be persisted even
        // when this group runtime is not an exact member. The checkpoint helper
        // independently skips roaming publication when no member matches.
        self.persist_current_session_checkpoint(true)?;
        Ok(self.tool_runtime())
    }

    fn configure_tool_runtime_for_session(
        &mut self,
        session_id: &SessionId,
    ) -> Result<(), CliError> {
        let sandbox_node_id = parse_chat_tool_runtime_selection(&self.tool_runtime)?;
        let conversation_workspace =
            channel_primary_workspace_root(&self.data_dir, &self.channel, session_id)?;
        let requested_workspace_root = conversation_workspace
            .as_deref()
            .or(self.tool_runtime.workspace_root.as_deref());
        let workspace_projection = self
            .channel
            .has_authoritative_workspace_projection()
            .then_some(self.channel.workspaces.as_slice());
        let selected_root = self
            .runtime
            .handler_mut()
            .configure_app_tool_runtime_for_session(
                &self.data_dir,
                session_id,
                sandbox_node_id,
                workspace_projection,
                requested_workspace_root,
                self.daemon_connected,
            )?;
        if self.channel.workspaces.is_empty()
            && let Some(root) = selected_root.as_ref()
        {
            self.channel.add_workspace(
                workspace_ref_for_selected_root(&self.data_dir, root)?
                    .binding()
                    .clone(),
            );
        }
        self.channel.mark_workspace_projection_authoritative();
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

    /// Stop routing control to this chat's live runtime. Called when the
    /// chat's host goes away so the runtime is released with it.
    pub fn release_runtime_control(&self) -> Result<(), CliError> {
        for runtime in self.participant_runtimes.values() {
            runtime.handler().release_runtime_control()?;
        }
        self.runtime.handler().release_runtime_control()
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
        let mut persistence_degraded = false;
        if let Err(error) = project_accepted_chat_control_input(
            self.channel_store.as_ref(),
            &self.channel.channel_id,
            &result,
        ) {
            tracing::warn!(%error, "accepted steering was not projected to stable chat history");
            persistence_degraded = true;
        }
        let mut status = chat_control_status(result);
        if let Err(error) = self.persist_current_session_checkpoint(false) {
            tracing::warn!(%error, "accepted steering checkpoint was not persisted");
            persistence_degraded = true;
        }
        if persistence_degraded {
            status.warning =
                Some("control accepted, but durable chat persistence is unavailable".to_string());
        }
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
        let mut persistence_degraded = false;
        if let Err(error) = project_accepted_chat_control_input(
            self.channel_store.as_ref(),
            &self.channel.channel_id,
            &result,
        ) {
            tracing::warn!(%error, "accepted follow-up was not projected to stable chat history");
            persistence_degraded = true;
        }
        let mut status = chat_control_status(result);
        if let Err(error) = self.persist_current_session_checkpoint(false) {
            tracing::warn!(%error, "accepted follow-up checkpoint was not persisted");
            persistence_degraded = true;
        }
        if persistence_degraded {
            status.warning =
                Some("control accepted, but durable chat persistence is unavailable".to_string());
        }
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

    /// Interrupt the session, which pauses it: no new model requests start,
    /// running tools and processes finish, and the next message resumes it.
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

    /// Cancel the active turn now, stopping running tools and processes.
    /// Work in progress may be lost.
    pub fn cancel(&self, reason: impl Into<String>) -> Result<ChatControlStatus, CliError> {
        let session_id = self.runtime.session().session_id.clone();
        let result = self
            .runtime_control_registry()
            .cancel(sinomo_agent::AgentSessionCancelRequest {
                session_id,
                reason: reason.into(),
            })
            .map_err(agent_control_cli_error)?;
        let status = chat_control_status(result);
        self.persist_current_session_checkpoint(false)?;
        Ok(status)
    }

    /// Lift a pause so the session can run turns again.
    pub fn unpause(&self) -> Result<ChatControlStatus, CliError> {
        let session_id = self.runtime.session().session_id.clone();
        let result = self
            .runtime_control_registry()
            .unpause(sinomo_agent::AgentSessionUnpauseRequest { session_id })
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
        self.send_with_attachments_cancellable(text, attachments, None, on_event)
    }

    pub(crate) fn send_with_attachments_cancellable(
        &mut self,
        text: &str,
        attachments: &[ChatAttachmentInput],
        cancellation: Option<sinomo_agent::AgentCancellationToken>,
        on_event: &mut dyn FnMut(ChatStreamEvent),
    ) -> Result<(), CliError> {
        if cancellation
            .as_ref()
            .is_some_and(sinomo_agent::AgentCancellationToken::is_cancelled)
        {
            return Ok(());
        }
        self.reload_current_channel()?;
        let primary = self.prepare_primary_participant()?;
        let attachments = self.store_chat_attachments(attachments)?;
        if self.channel.channel_kind == AgentChannelKind::Chat
            && self.channel.next_sequence == 1
            && self.channel.title == "New chat"
        {
            let first_line = text.lines().map(str::trim).find(|line| !line.is_empty());
            let title = first_line.map(str::to_string).or_else(|| {
                attachments
                    .first()
                    .map(|file| format!("File: {}", file.file_name))
            });
            if let Some(title) = title {
                let mut chars = title.chars();
                let mut title = chars.by_ref().take(72).collect::<String>();
                if chars.next().is_some() {
                    title.push('…');
                }
                self.channel.rename(title);
            }
        }
        let source = app_channel_source(
            self.runtime.session(),
            (!attachments.is_empty()).then(|| {
                serde_json::json!({
                    "attachment_count": attachments.len()
                })
            }),
        );
        let context_attachments = agent_context_attachments_from_channel_attachments(&attachments);
        self.run_on_channel_cancellable(
            primary,
            context_attachments,
            on_event,
            ChannelAction::Send {
                text,
                source,
                attachments,
            },
            cancellation,
        )
    }

    /// Whether a participant's detached tool calls completed and wait for
    /// [`Self::continue_async_updates`].
    pub fn has_async_updates(&self) -> bool {
        !self.participants_awaiting_recovery.is_empty()
            || self.runtime.handler().has_async_updates()
            || self
                .participant_runtimes
                .values()
                .any(|runtime| runtime.handler().has_async_updates())
    }

    /// Whether channel participants have work in flight: a running turn,
    /// detached tool calls, or completions not yet continued with. (The
    /// chat's own session reports through the runtime control registry.)
    pub fn has_participant_work(&self) -> bool {
        !self.participants_awaiting_recovery.is_empty()
            || self
                .participant_runtimes
                .values()
                .any(|runtime| runtime.handler().has_running_work())
    }

    /// Completed or not-yet-restored continuations stay durable. Only live
    /// participant effects keep a shutting-down host and its mounts resident.
    pub(crate) fn has_live_participant_work(&self) -> bool {
        self.participant_runtimes
            .values()
            .any(|runtime| runtime.handler().has_live_work())
    }

    /// Wake `waker` when the work of this chat's agent sessions changes: the
    /// chat's own and those of the channel participants with a runtime.
    pub(crate) fn watch_sessions(&self, waker: &sinomo_agent::AgentSessionWaker) {
        waker.watch(&self.runtime.session().session_id);
        for runtime in self.participant_runtimes.values() {
            waker.watch(&runtime.session().session_id);
        }
    }

    /// Note the channel participants whose sessions recorded detached tool
    /// calls the session has not continued with. A restarted or reopened
    /// channel has no participant runtimes until they speak, so these are
    /// recovered on the next continuation instead.
    fn find_participants_awaiting_recovery(&mut self) -> Result<(), CliError> {
        self.participants_awaiting_recovery.clear();
        if self.channel.channel_kind != AgentChannelKind::Channel {
            return Ok(());
        }
        let store = FileAgentSessionStore::for_data_dir(&self.data_dir);
        for participant in self.channel.enabled_participants_in_order() {
            if self
                .participant_runtimes
                .contains_key(&participant.participant_id)
            {
                continue;
            }
            let calls = store
                .load_detached_calls(&participant.agent_session_id)
                .map_err(|source| CliError::Parse {
                    detail: format!(
                        "load detached calls of channel agent session `{}` failed: {source}",
                        participant.agent_session_id
                    ),
                })?;
            if !calls.is_empty() {
                self.participants_awaiting_recovery
                    .insert(participant.participant_id);
            }
        }
        Ok(())
    }

    /// Create the runtimes of participants awaiting recovery; attaching their
    /// sessions queues their completions (or records lost calls as
    /// interrupted).
    fn recover_awaiting_participants(&mut self) -> Result<(), CliError> {
        if self.participants_awaiting_recovery.is_empty() {
            return Ok(());
        }
        if self.participant_runtimes_channel.as_ref() != Some(&self.channel.channel_id) {
            self.participant_runtimes.clear();
            self.participant_runtimes_channel = Some(self.channel.channel_id.clone());
        }
        let runtime_control_registry = self.runtime.handler().runtime_control_registry();
        for participant_id in std::mem::take(&mut self.participants_awaiting_recovery) {
            let Some(participant) = self.channel.participant(&participant_id).cloned() else {
                continue;
            };
            if self.participant_runtimes.contains_key(&participant_id) {
                continue;
            }
            let Some(mut session) = self.load_participant_session(&participant)? else {
                continue;
            };
            let runtime = build_participant_runtime(
                ParticipantRuntimeContext {
                    data_dir: &self.data_dir,
                    agent_catalog_vfs: &self.agent_catalog_vfs,
                    agent_catalog_authoritative: self.agent_catalog_authoritative,
                    daemon_connected: self.daemon_connected,
                    runtime_control_registry: &runtime_control_registry,
                },
                &participant,
                &mut session,
            )?;
            self.participant_runtimes.insert(participant_id, runtime);
        }
        Ok(())
    }

    /// Continue every participant whose detached tool calls completed. Each
    /// takes a turn with the result and posts on its own. Returns whether any
    /// continued.
    pub fn continue_async_updates(
        &mut self,
        on_event: &mut dyn FnMut(ChatStreamEvent),
    ) -> Result<bool, CliError> {
        if !self.has_async_updates() {
            return Ok(false);
        }
        self.reload_current_channel()?;
        self.recover_awaiting_participants()?;
        let primary = self.prepare_primary_participant()?;
        let mut pending = Vec::new();
        if let Some(primary) = &primary {
            pending.extend(
                self.runtime
                    .handler()
                    .take_async_updates()
                    .into_iter()
                    .map(|update| (primary.clone(), update)),
            );
        }
        for (participant_id, runtime) in &self.participant_runtimes {
            pending.extend(
                runtime
                    .handler()
                    .take_async_updates()
                    .into_iter()
                    .map(|update| (participant_id.clone(), update)),
            );
        }
        let continued = !pending.is_empty();
        for (participant_id, update) in pending {
            self.run_on_channel(
                primary.clone(),
                Vec::new(),
                on_event,
                ChannelAction::Continue {
                    participant_id,
                    update,
                },
            )?;
        }
        Ok(continued)
    }

    /// A chat's agent participant, answered on the chat's own live runtime so
    /// steering and interrupts reach it; none in a multi-agent channel.
    fn prepare_primary_participant(&mut self) -> Result<Option<TaskId>, CliError> {
        if self.channel.channel_kind == AgentChannelKind::Channel {
            return Ok(None);
        }
        let session_id = self.runtime.session().session_id.clone();
        self.configure_tool_runtime_for_session(&session_id)?;
        self.primary_speaker();
        Ok(self
            .primary_participant()
            .map(|participant| participant.participant_id.clone()))
    }

    /// Run `action` on the channel runtime: a chat is a channel with one agent
    /// and the single-agent policy; every participant runs on its own runtime.
    fn run_on_channel(
        &mut self,
        primary: Option<TaskId>,
        context_attachments: Vec<AgentContextAttachment>,
        on_event: &mut dyn FnMut(ChatStreamEvent),
        action: ChannelAction<'_>,
    ) -> Result<(), CliError> {
        self.run_on_channel_cancellable(primary, context_attachments, on_event, action, None)
    }

    fn run_on_channel_cancellable(
        &mut self,
        primary: Option<TaskId>,
        context_attachments: Vec<AgentContextAttachment>,
        on_event: &mut dyn FnMut(ChatStreamEvent),
        action: ChannelAction<'_>,
        cancellation: Option<sinomo_agent::AgentCancellationToken>,
    ) -> Result<(), CliError> {
        let channel_mode = primary.is_none();
        if self.participant_runtimes_channel.as_ref() != Some(&self.channel.channel_id) {
            self.participant_runtimes.clear();
            self.participant_runtimes_channel = Some(self.channel.channel_id.clone());
        }
        let participant_sessions = self.participant_sessions(primary.as_ref())?;

        let channel = self.channel.clone();
        let sink = RefCell::new(on_event);
        let store = self.channel_store.as_ref();
        let runtime_control_registry = self.runtime.handler().runtime_control_registry();
        let model = self.runtime_info();
        let executor = ParticipantExecutor {
            primary: primary.map(|participant_id| (participant_id, &mut self.runtime)),
            runtimes: &mut self.participant_runtimes,
            data_dir: &self.data_dir,
            agent_catalog_vfs: &self.agent_catalog_vfs,
            agent_catalog_authoritative: self.agent_catalog_authoritative,
            daemon_connected: self.daemon_connected,
            runtime_control_registry,
            model,
            session_checkpoint_store: &self.session_checkpoint_store,
            channel_store: store,
            tool_runtime: self.tool_runtime.clone(),
            tool_runtime_explicit_override: self.tool_runtime_explicit_override,
            context_attachments,
            cancellation,
            on_event: &sink,
        };
        let mut channel_runtime = AgentChannelRuntime::new(channel, store, executor)
            .with_loop_config(AgentChannelLoopConfig::default());
        if !channel_mode {
            channel_runtime = channel_runtime.with_policy(AgentChannelSingleAgentPolicy);
        }
        for (participant_id, session) in participant_sessions {
            channel_runtime.insert_participant_session(participant_id, session);
        }
        let mut forward =
            |event| forward_channel_runtime_event(event, &mut |event| (sink.borrow_mut())(event));
        let result = match action {
            ChannelAction::Send {
                text,
                source,
                attachments,
            } => channel_runtime
                .send_user_message_from_with_attachments(
                    AGENT_CHANNEL_APP_USER_ID,
                    "User",
                    text,
                    source,
                    attachments,
                    &mut forward,
                )
                .map(|_| ()),
            ChannelAction::Continue {
                participant_id,
                update,
            } => channel_runtime
                .continue_with_async_update(&participant_id, &update, &mut forward)
                .map(|_| ()),
        };
        self.channel = channel_runtime.channel().clone();
        drop(channel_runtime);
        result.map_err(|source| CliError::Parse {
            detail: format!("chat turn failed: {source}"),
        })?;
        if !channel_mode {
            self.persist_primary_session_checkpoint()?;
        }
        Ok(())
    }

    /// The current session of each enabled participant: the live runtime's
    /// for a participant that has one, otherwise the newest of its local and
    /// roaming checkpoints. Disabled participants take no part in a turn, and
    /// in a chat a disabled one shares the chat's session with the agent that
    /// replaced it, so its older checkpoint no longer describes that session.
    fn participant_sessions(
        &self,
        primary: Option<&TaskId>,
    ) -> Result<Vec<(TaskId, AgentSession)>, CliError> {
        let mut sessions = Vec::new();
        for participant in self
            .channel
            .participants
            .iter()
            .filter(|participant| participant.enabled)
        {
            let live = if primary == Some(&participant.participant_id) {
                Some(&self.runtime)
            } else {
                self.participant_runtimes.get(&participant.participant_id)
            };
            if let Some(runtime) = live {
                if let Some(session) = runtime.handler().agent_session_snapshot()? {
                    sessions.push((participant.participant_id.clone(), session));
                }
                continue;
            }
            if let Some(session) = self.load_participant_session(participant)? {
                sessions.push((participant.participant_id.clone(), session));
            }
        }
        Ok(sessions)
    }

    /// A participant's newest checkpoint, local or roaming, saved locally.
    fn load_participant_session(
        &self,
        participant: &AgentChannelParticipant,
    ) -> Result<Option<AgentSession>, CliError> {
        let session_store = FileAgentSessionStore::for_data_dir(&self.data_dir);
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
            .load_latest(&self.channel, participant)
            .map_err(|detail| CliError::Parse {
                detail: format!(
                    "load roaming channel agent session `{}` failed: {detail}",
                    participant.agent_session_id
                ),
            })?;
        let session =
            select_resumable_agent_session(local_session, roaming_session).map_err(|detail| {
                CliError::Parse {
                    detail: format!(
                        "select canonical channel agent session `{}` failed: {detail}",
                        participant.agent_session_id
                    ),
                }
            })?;
        if let Some(session) = &session {
            session_store
                .save_session(session)
                .map_err(|source| CliError::Parse {
                    detail: format!(
                        "persist channel agent session `{}` locally failed: {source}",
                        participant.agent_session_id
                    ),
                })?;
        }
        Ok(session)
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
        let definition = resolve_agent(
            &self.agent_catalog_vfs,
            self.agent_catalog_authoritative,
            agent_key,
        )?
        .ok_or_else(|| CliError::Parse {
            detail: format!("unknown agent `{agent_key}`"),
        })?;
        if !definition.supports_interactive_chat() {
            return Err(CliError::Parse {
                detail: format!(
                    "agent `{agent_key}` has role `{:?}` and is not available on interactive chat surfaces",
                    definition.role
                ),
            });
        }
        match read_cli_cluster_state(&ClusterPaths::new(&self.data_dir)) {
            Ok(state) => crate::validate_agent_local_placement(&definition, &state.local_node_id)?,
            Err(_)
                if definition.placement.home_server_node_id.is_some()
                    || definition.tool_runtime.sandbox_node_id.is_some() =>
            {
                return Err(CliError::Parse {
                    detail: format!(
                        "agent `{agent_key}` declares node placement, but the local node identity is unavailable"
                    ),
                });
            }
            Err(_) => {}
        }
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
            let participant_ids = self
                .channel
                .participants
                .iter()
                .map(|participant| participant.participant_id.clone())
                .collect::<Vec<_>>();
            for participant_id in participant_ids {
                self.channel.set_participant_enabled(&participant_id, false);
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
        if let Some(participant) = participant.filter(|_| !self.is_unstarted_default_chat()) {
            self.session_checkpoint_store
                .publish(&self.channel, participant, &session)
                .map_err(|detail| CliError::Parse {
                    detail: format!("publish roaming agent session checkpoint failed: {detail}"),
                })?;
        }
        Ok(())
    }
    /// Default drafts stay local until their first message; explicit channel
    /// names, agent choices and workspace grants still publish immediately.
    fn is_unstarted_default_chat(&self) -> bool {
        self.channel.channel_kind == AgentChannelKind::Chat
            && self.channel.next_sequence == 1
            && self.channel.title == "New chat"
            && !self.channel.is_deleted()
            && self.channel.workspace_revision == 0
            && self.channel.workspaces.is_empty()
            && self
                .channel
                .participants
                .iter()
                .all(|agent| agent.agent_key == "main-agent")
    }

    fn save_channel(&self) -> Result<(), CliError> {
        if self.is_unstarted_default_chat() {
            return Ok(());
        }
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

type ChatRuntime = InteractionRuntime<InMemoryEventStore, Handler>;

/// What a chat asks its channel runtime to do.
enum ChannelAction<'a> {
    /// The user sent a message.
    Send {
        text: &'a str,
        source: AgentChannelSource,
        attachments: Vec<AgentChannelAttachment>,
    },
    /// A participant's detached tool call completed.
    Continue {
        participant_id: TaskId,
        update: sinomo_agent::AsyncTaskUpdate,
    },
}

/// What an agent is told when it continues with completed background work.
const ASYNC_CONTINUATION_CONTEXT: &str = "A tool call you ran in the background has finished; its result is in the async update. Continue the work with it and tell the user what happened.";

/// Runs every agent participant on its own runtime: the chat's live runtime
/// for a chat's agent, and a runtime kept per participant for channel agents.
/// No participant's session is swapped through another's runtime.
struct ParticipantExecutor<'a, 'b> {
    /// A chat's agent, answered on the chat's own runtime.
    primary: Option<(TaskId, &'a mut ChatRuntime)>,
    runtimes: &'a mut BTreeMap<TaskId, ChatRuntime>,
    data_dir: &'a SinomoDataDir,
    agent_catalog_vfs: &'a VfsHandle,
    agent_catalog_authoritative: bool,
    daemon_connected: bool,
    runtime_control_registry: AgentRuntimeControlRegistry,
    /// The chat's model selection, which channel agents run with.
    model: ChatRuntimeInfo,
    session_checkpoint_store: &'a SifsAgentSessionCheckpointStore,
    channel_store: &'a dyn AgentChannelStore,
    tool_runtime: ChatToolRuntimeSelection,
    tool_runtime_explicit_override: bool,
    context_attachments: Vec<AgentContextAttachment>,
    cancellation: Option<sinomo_agent::AgentCancellationToken>,
    on_event: &'a RefCell<&'b mut dyn FnMut(ChatStreamEvent)>,
}

impl ParticipantExecutor<'_, '_> {
    /// The participant's runtime, created on first use and brought up to date
    /// with messages delivered into `session` since it last ran.
    fn runtime_for(
        &mut self,
        channel: &AgentChannel,
        participant: &AgentChannelParticipant,
        session: &mut AgentSession,
    ) -> Result<&mut ChatRuntime, CliError> {
        let is_primary = self
            .primary
            .as_ref()
            .is_some_and(|(participant_id, _)| participant_id == &participant.participant_id);
        if !is_primary && !self.runtimes.contains_key(&participant.participant_id) {
            let runtime = self.new_participant_runtime(participant, session)?;
            self.runtimes
                .insert(participant.participant_id.clone(), runtime);
        }
        let runtime = match &mut self.primary {
            Some((participant_id, runtime)) if participant_id == &participant.participant_id => {
                &mut **runtime
            }
            _ => self
                .runtimes
                .get_mut(&participant.participant_id)
                .expect("participant runtime was just created"),
        };
        let live = runtime.handler().agent_session_snapshot()?;
        if live.as_ref().is_some_and(|live| {
            live.session_id == session.session_id && live.state_revision != session.state_revision
        }) {
            let interaction_session = runtime.session().clone();
            tui::app_restore_agent_session(
                runtime.handler_mut(),
                self.data_dir,
                &interaction_session,
                Some(session.clone()),
                sinomo_agent::AgentTranscript::new(),
            )?;
        }
        if !is_primary {
            let current: ChatRuntimeInfo = tui::app_runtime_info(runtime.handler()).into();
            if (&current.provider, &current.model, &current.account)
                != (&self.model.provider, &self.model.model, &self.model.account)
            {
                tui::app_switch_model(
                    runtime.handler_mut(),
                    self.data_dir,
                    self.model.provider.clone(),
                    self.model.model.clone(),
                    self.model
                        .account
                        .clone()
                        .unwrap_or_else(|| sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string()),
                    self.daemon_connected,
                )?;
            }
            // Channel workspaces can change between turns.
            let mut tool_runtime = if self.tool_runtime_explicit_override {
                self.tool_runtime.clone()
            } else {
                chat_tool_runtime_from_agent_runtime_definition(&participant.tool_runtime)
            };
            if tool_runtime.workspace_root.is_none() {
                tool_runtime.workspace_root = channel_primary_workspace_root(
                    self.data_dir,
                    channel,
                    &participant.agent_session_id,
                )?;
            }
            let sandbox_node_id = parse_chat_tool_runtime_selection(&tool_runtime)?;
            let workspace_projection = channel
                .has_authoritative_workspace_projection()
                .then_some(channel.workspaces.as_slice());
            let session_id = runtime.session().session_id.clone();
            runtime
                .handler_mut()
                .configure_app_tool_runtime_for_session(
                    self.data_dir,
                    &session_id,
                    sandbox_node_id,
                    workspace_projection,
                    tool_runtime.workspace_root.as_deref(),
                    self.daemon_connected,
                )?;
        }
        Ok(runtime)
    }

    fn new_participant_runtime(
        &self,
        participant: &AgentChannelParticipant,
        session: &mut AgentSession,
    ) -> Result<ChatRuntime, CliError> {
        build_participant_runtime(
            ParticipantRuntimeContext {
                data_dir: self.data_dir,
                agent_catalog_vfs: self.agent_catalog_vfs,
                agent_catalog_authoritative: self.agent_catalog_authoritative,
                daemon_connected: self.daemon_connected,
                runtime_control_registry: &self.runtime_control_registry,
            },
            participant,
            session,
        )
    }
}

/// What building a channel participant's runtime needs from its chat.
struct ParticipantRuntimeContext<'a> {
    data_dir: &'a SinomoDataDir,
    agent_catalog_vfs: &'a VfsHandle,
    agent_catalog_authoritative: bool,
    daemon_connected: bool,
    runtime_control_registry: &'a AgentRuntimeControlRegistry,
}

/// A channel participant's own runtime, attached to `session`. Attaching
/// recovers the session's interrupted turn and detached tool calls.
fn build_participant_runtime(
    context: ParticipantRuntimeContext<'_>,
    participant: &AgentChannelParticipant,
    session: &mut AgentSession,
) -> Result<ChatRuntime, CliError> {
    let definition = resolve_agent(
        context.agent_catalog_vfs,
        context.agent_catalog_authoritative,
        &participant.agent_key,
    )?
    .ok_or_else(|| CliError::Parse {
        detail: format!("unknown channel agent `{}`", participant.agent_key),
    })?;
    if session.session_id != participant.agent_session_id {
        return Err(CliError::Parse {
            detail: format!(
                "channel participant `{}` expects session `{}`, but received session `{}`",
                participant.participant_id, participant.agent_session_id, session.session_id
            ),
        });
    }
    match session.agent_definition_key.as_deref() {
        Some(key) if key != participant.agent_key => {
            return Err(CliError::Parse {
                detail: format!(
                    "channel participant `{}` belongs to `{}`, but session `{}` belongs to `{key}`",
                    participant.participant_id, participant.agent_key, session.session_id
                ),
            });
        }
        // The durable participant/session association is explicit channel
        // identity evidence for a legacy checkpoint.
        None => session.agent_definition_key = Some(participant.agent_key.clone()),
        Some(_) => {}
    }
    let runtime_skill_catalog_vfs = context
        .daemon_connected
        .then(|| context.agent_catalog_vfs.clone());
    let handler = tui::app_build_chat_handler(
        context.data_dir,
        context.daemon_connected,
        runtime_skill_catalog_vfs,
    )?
    .with_runtime_control_registry(context.runtime_control_registry.clone());
    let interaction_session = app_session(
        context.data_dir,
        Some(&participant.agent_session_id.to_string()),
    );
    let mut runtime = InteractionRuntime::new(
        InMemoryEventStore::new(),
        handler,
        interaction_session.clone(),
    );
    tui::app_switch_agent(
        runtime.handler_mut(),
        context.data_dir,
        definition,
        context.daemon_connected,
    )?;
    tui::app_restore_agent_session(
        runtime.handler_mut(),
        context.data_dir,
        &interaction_session,
        Some(session.clone()),
        sinomo_agent::AgentTranscript::new(),
    )?;
    Ok(runtime)
}

impl AgentChannelTurnExecutor for ParticipantExecutor<'_, '_> {
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
        input: AgentChannelSpeakInput<'_>,
        _on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
    ) -> Result<AgentChannelSpeech, AgentChannelExecutorError> {
        let is_primary = self
            .primary
            .as_ref()
            .is_some_and(|(participant_id, _)| participant_id == &participant.participant_id);
        let sink = self.on_event;
        let store = self.channel_store;
        let data_dir = self.data_dir;
        let checkpoints = self.session_checkpoint_store;
        let context_attachments = self.context_attachments.clone();
        let cancellation = self.cancellation.clone();
        let speaker = chat_speaker(&channel.channel_id, participant);
        let runtime = self
            .runtime_for(channel, participant, session)
            .map_err(channel_executor_cli_error)?;
        if !is_primary {
            // Clients attribute the activity that follows to this participant.
            (sink.borrow_mut())(ChatStreamEvent::SpeakerStarted(speaker.clone()));
        }

        runtime.set_conversation_id(
            channel.channel_id.clone(),
            participant.participant_id.clone(),
        );
        let channel_id = channel.channel_id.clone();
        let mut streamed_answer = String::new();
        let mut streamed_step = None;
        // A send can run more than one operation (queued follow-ups); the
        // reply closes the last.
        let mut op_id = None;
        let mut forward = |event: ChatStreamEvent| {
            let mut on_event = sink.borrow_mut();
            match event {
                ChatStreamEvent::OpStarted { op_id: started } => op_id = Some(started),
                ChatStreamEvent::ToolArtifacts(event) => {
                    if let Some(output) = tool_output_snapshot(store, data_dir, &channel_id, event)
                    {
                        on_event(ChatStreamEvent::ToolOutput(output));
                    }
                }
                ChatStreamEvent::Token(text) => {
                    streamed_answer.push_str(&text);
                    on_event(ChatStreamEvent::AgentToken {
                        speaker: speaker.clone(),
                        text,
                    });
                }
                ChatStreamEvent::AssistantTextDelta { step_index, text } => {
                    if streamed_step != Some(step_index) {
                        streamed_step = Some(step_index);
                        streamed_answer.clear();
                    }
                    streamed_answer.push_str(&text);
                    on_event(ChatStreamEvent::AssistantTextDelta { step_index, text });
                }
                ChatStreamEvent::AgentToken { speaker, text } => {
                    streamed_answer.push_str(&text);
                    on_event(ChatStreamEvent::AgentToken { speaker, text });
                }
                event => on_event(event),
            }
        };
        runtime
            .handler_mut()
            .set_admission_cancellation(cancellation);
        let result = (|| {
            let mut emit = |output: InteractionOutput| forward_output(output, &mut forward);
            let trailing = match input {
                AgentChannelSpeakInput::Message(message) => runtime
                    .ingest_user_input_with_item_id_and_context_attachments_streaming(
                        &message.content,
                        channel_message_agent_item_id(&message.message_id),
                        context_attachments,
                        &mut emit,
                    )?,
                AgentChannelSpeakInput::Observe => runtime
                    .ingest_transient_system_context_with_attachments_streaming(
                        &channel_agent_turn_system_context(channel, participant),
                        context_attachments,
                        &mut emit,
                    )?,
                AgentChannelSpeakInput::AsyncUpdate(update) => {
                    let context = if is_primary {
                        ASYNC_CONTINUATION_CONTEXT.to_string()
                    } else {
                        format!(
                            "{}\n\n{ASYNC_CONTINUATION_CONTEXT}",
                            channel_agent_turn_system_context(channel, participant)
                        )
                    };
                    runtime.ingest_async_update_streaming(&context, update, &mut emit)?
                }
            };
            for output in trailing {
                forward_output(output, &mut forward);
            }
            drain_runtime_events(runtime, &mut forward)
        })();
        runtime.handler_mut().set_admission_cancellation(None);

        if let Some(updated) = runtime
            .handler()
            .agent_session_snapshot()
            .map_err(channel_executor_cli_error)?
        {
            *session = updated;
        }
        if !is_primary {
            // The chat's own session is saved by the chat; channel agents'
            // sessions are saved here, after each turn.
            FileAgentSessionStore::for_data_dir(data_dir)
                .save_session(session)
                .map_err(|source| {
                    AgentChannelExecutorError::new(format!(
                        "persist channel agent session `{}` failed: {source}",
                        participant.agent_session_id
                    ))
                })?;
            checkpoints
                .publish(channel, participant, session)
                .map_err(|detail| {
                    AgentChannelExecutorError::new(format!(
                        "publish roaming channel agent session `{}` failed: {detail}",
                        participant.agent_session_id
                    ))
                })?;
        }
        let produced_by = op_id.map(|op_id| AgentActivityRef {
            participant_id: participant.participant_id.to_string(),
            session_id: session.session_id.clone(),
            op_id,
        });
        match result {
            Ok(()) => Ok(AgentChannelSpeech {
                text: streamed_answer,
                produced_by,
                source: Some(app_channel_source(runtime.session(), None)),
            }),
            Err(error) => {
                Err(AgentChannelExecutorError::new(error.to_string()).with_produced_by(produced_by))
            }
        }
    }
}

fn channel_executor_cli_error(error: CliError) -> AgentChannelExecutorError {
    AgentChannelExecutorError::new(error.to_string())
}

fn channel_primary_workspace_root(
    data_dir: &SinomoDataDir,
    channel: &AgentChannel,
    session_id: &SessionId,
) -> Result<Option<PathBuf>, CliError> {
    channel
        .primary_workspace()
        .map(|workspace| {
            workspace_binding_root_for_data_dir(data_dir, session_id, workspace)
                .map(|resolved| resolved.root().to_path_buf())
        })
        .transpose()
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
        AgentChannelRuntimeEvent::ToolProgress { event } => forward_tool_event(event, on_event),
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
        warning: None,
        paused: result.snapshot.paused,
    }
}

/// Persist the user-visible input accepted by a steering/follow-up control
/// event into the stable chat timeline.
///
/// Queue ids and event timestamps are generated by the agent control plane and
/// remain stable across retries. Raw agent/model session records are neither
/// opened nor copied into the channel projection.
pub fn project_accepted_chat_control_input(
    store: &dyn AgentChannelStore,
    stable_channel_id: &SessionId,
    result: &AgentSessionControlResult,
) -> Result<(), AgentChannelStoreError> {
    if !result.accepted {
        return Ok(());
    }
    let queued = result.events.iter().find_map(|event| match &event.payload {
        AgentEventPayload::SteeringQueued { steering } => Some((
            &steering.steering_id,
            steering.message.as_str(),
            "steering",
            event,
        )),
        AgentEventPayload::FollowUpQueued { follow_up } => Some((
            &follow_up.follow_up_id,
            follow_up.message.as_str(),
            "follow_up",
            event,
        )),
        _ => None,
    });
    let Some((input_id, message, input_kind, event)) = queued else {
        return Ok(());
    };
    if event.session_id != result.snapshot.session_id {
        return Err(AgentChannelStoreError::Serialization {
            detail: "accepted chat control event belongs to a different session".to_string(),
        });
    }
    let source = AgentChannelSource {
        adapter: "sinomo-chat-control".to_string(),
        external_channel_id: Some(stable_channel_id.to_string()),
        external_message_id: Some(input_id.to_string()),
        metadata: serde_json::json!({
            "surface_event": "queued_turn_input",
            "input_kind": input_kind,
        }),
        ..AgentChannelSource::default()
    };
    project_stable_chat_user_message(
        store,
        stable_channel_id,
        input_id.clone(),
        event.emitted_at,
        message,
        source,
    )
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
        paused: response
            .session
            .as_ref()
            .is_some_and(AgentSession::is_paused),
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
        AgentTaskLifecycleStatus::Paused => "paused",
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
                native_items: Vec::new(),
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
                native_items: Vec::new(),
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
        membership_revision: 1,
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
    agent_catalog_vfs: &VfsHandle,
    agent_catalog_authoritative: bool,
    agent_key: &str,
) -> Result<ChatToolRuntimeSelection, CliError> {
    Ok(
        resolve_agent(agent_catalog_vfs, agent_catalog_authoritative, agent_key)?
            .as_ref()
            .map(ChatToolRuntimeSelection::from_agent_definition)
            .unwrap_or_default(),
    )
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
    active_step_index: Option<usize>,
}

impl ChatStreamState {
    fn begin_model_step(&mut self, step_index: usize) {
        if self.active_step_index != Some(step_index) {
            self.active_step_index = Some(step_index);
            self.visible_text.clear();
            self.structured_json.clear();
        }
    }

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
        AgentRuntimeEventPayload::WorkerStarted { action, op_id, .. } => {
            on_event(ChatStreamEvent::OpStarted { op_id });
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
        AgentModelEvent::AssistantTextDelta { step_index, delta } => {
            stream_state.begin_model_step(step_index);
            if !stream_state.structured_json.is_empty()
                || tui::starts_structured_agent_json_delta(&delta)
            {
                stream_state.append_structured_delta(&delta, on_event);
                return;
            }
            for event in split_chunk(&delta) {
                match event {
                    ChatStreamEvent::Token(token) => {
                        stream_state.append_plain_token(&token);
                        on_event(ChatStreamEvent::AssistantTextDelta {
                            step_index,
                            text: token,
                        });
                    }
                    event => on_event(event),
                }
            }
        }
        AgentModelEvent::AssistantReasoningDelta { step_index, delta } => {
            if !delta.is_empty() {
                on_event(ChatStreamEvent::ReasoningDelta { step_index, delta });
            }
        }
        AgentModelEvent::AssistantReasoningSummary {
            step_index,
            summary,
        } => {
            if !summary.trim().is_empty() {
                on_event(ChatStreamEvent::ReasoningSummary {
                    step_index,
                    summary,
                });
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
        AgentModelEvent::Paused { reason } => {
            on_event(ChatStreamEvent::Notice(format!("session paused: {reason}")));
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
    for byte in text.as_bytes()[..quote_index].iter().rev() {
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
        event @ AgentToolProgressEvent::Artifacts { .. } => {
            on_event(ChatStreamEvent::ToolArtifacts(event))
        }
        AgentToolProgressEvent::ProcessStarted { process, call }
        | AgentToolProgressEvent::ProcessOutput { process, call }
        | AgentToolProgressEvent::ProcessCompleted { process, call } => {
            on_event(ChatStreamEvent::Process(chat_process_snapshot(
                process, call,
            )));
        }
        AgentToolProgressEvent::Message { summary, .. } => {
            if !summary.trim().is_empty() {
                on_event(ChatStreamEvent::Notice(summary));
            }
        }
        AgentToolProgressEvent::DetachedStarted {
            tool_call_id,
            tool,
            call,
            ..
        } => {
            if let Some(call) = call {
                on_event(ChatStreamEvent::ToolOutput(ChatToolOutputSnapshot {
                    tool_call_id: Some(tool_call_id.to_string()),
                    call: ChatToolCallSnapshot {
                        step_index: call.step_index,
                        call_index: Some(call.call_index),
                        call_id: call.native_call_id,
                        tool: call.tool,
                        input: None,
                        input_preview: None,
                    },
                    status: "detached".to_string(),
                    files: Vec::new(),
                }));
            }
            on_event(ChatStreamEvent::Notice(format!(
                "{tool} continues in the background"
            )));
        }
        // The host continues the session with the completion; see
        // `AgentChat::continue_async_updates`.
        AgentToolProgressEvent::DetachedCompleted { .. } => {}
    }
}

/// Each requested agent operation's activity as a client trace; what the
/// daemon serves on `/chat/session/activity`.
pub fn session_activity(
    data_dir: &SinomoDataDir,
    request: sinomo_runtime::api::app::chat::ChatSessionActivityRequest,
) -> sinomo_runtime::api::app::chat::ChatSessionActivityResponse {
    crate::chat_service::activity::session_activity(data_dir, request)
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

fn chat_process_snapshot(
    process: sinomo_agent::AgentProcessSnapshot,
    call: Option<sinomo_agent::AgentToolCallScope>,
) -> ChatProcessSnapshot {
    ChatProcessSnapshot {
        call: call.map(|call| ChatToolCallRef {
            tool_call_id: Some(call.tool_call_id.to_string()),
            step_index: call.step_index,
            call_index: Some(call.call_index),
            call_id: call.native_call_id,
            tool: call.tool,
        }),
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
    if let Some(rest) = chunk.strip_prefix("<sinomo_progress>")
        && let Some(progress) = rest.strip_suffix("</sinomo_progress>")
    {
        return vec![ChatStreamEvent::Notice(progress.to_string())];
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
    let conversation_id = session_id.clone();
    let mut session = InteractionSession::new(
        session_id,
        node_id,
        InteractionSurface::new("surface.app", InteractionSurfaceKind::App),
    );
    session.conversation_id = Some(conversation_id);
    session
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

/// All agents available for selection through an already-selected SIFS read
/// transport (stored definitions first, else built-ins on an explicit error).
pub fn agent_summaries(agent_catalog_vfs: &VfsHandle) -> Vec<AgentSummary> {
    agent_summaries_with_error(agent_catalog_vfs).0
}

/// Like [`agent_summaries`], but also reports the catalog-load error (if any) that
/// forced the fallback to built-ins. Production callers supply the resident
/// daemon VFS so this never mounts a second process-local DFS. Callers off the
/// UI thread use the error to surface a degraded-state notice.
pub fn agent_summaries_with_error(
    agent_catalog_vfs: &VfsHandle,
) -> (Vec<AgentSummary>, Option<String>) {
    match AgentCatalog::load_from_vfs(agent_catalog_vfs.clone()) {
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
        .filter(|definition| definition.supports_interactive_chat())
        .map(|definition| AgentSummary {
            key: definition.key().to_string(),
            name: if definition.name.is_empty() {
                definition.key().to_string()
            } else {
                definition.name.clone()
            },
            description: definition.description.clone(),
            module_id: definition.package.provenance.module_id.clone(),
            module_version: definition.package.provenance.module_version.clone(),
            tool_runtime: sinomo_runtime::api::app::chat::ChatToolRuntimeSelection {
                sandbox_node_id: definition.tool_runtime.sandbox_node_id.clone(),
                workspace_root: None,
                ..Default::default()
            },
        })
        .collect()
}

/// Resolve an exact agent definition without touching SIFS for built-ins.
///
/// The VFS opener is deliberately lazy: startup and degraded-mode surfaces can
/// continue to use the immutable built-in definitions when the resident daemon
/// is unavailable, while custom and module-provided definitions still fail
/// closed unless their authoritative catalog can be read.
pub fn resolve_agent_definition_with_catalog_vfs<E>(
    key: &str,
    open_catalog_vfs: impl FnOnce() -> Result<VfsHandle, E>,
) -> Result<Option<AgentDefinition>, String>
where
    E: std::fmt::Display,
{
    if let Some(definition) = AgentDefinition::built_in(key) {
        return Ok(Some(definition));
    }
    let agent_catalog_vfs = open_catalog_vfs().map_err(|error| error.to_string())?;
    AgentCatalog::load_from_vfs(agent_catalog_vfs)
        .map(|catalog| catalog.get(key).cloned())
        .map_err(|source| format!("load configured agent catalog SIFS failed: {source}"))
}

fn resolve_agent(
    agent_catalog_vfs: &VfsHandle,
    agent_catalog_authoritative: bool,
    key: &str,
) -> Result<Option<AgentDefinition>, CliError> {
    resolve_agent_definition_with_catalog_vfs(key, || {
        if agent_catalog_authoritative {
            Ok(agent_catalog_vfs.clone())
        } else {
            Err(CliError::DaemonNotRunning {
                detail:
                    "custom and module agent definitions require the resident daemon SIFS catalog"
                        .to_string(),
            })
        }
    })
    .map_err(|detail| CliError::Parse { detail })
}

/// List skills available to app management surfaces.
fn skill_summary_from_entry(entry: &SkillCatalogEntry, active: bool) -> SkillSummary {
    SkillSummary {
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

fn skill_detail_from_entry(entry: &SkillCatalogEntry, active: bool, text: String) -> SkillDetail {
    SkillDetail {
        summary: skill_summary_from_entry(entry, active),
        manifest_path: entry.manifest_path.clone(),
        base_hash: entry.base_hash.clone(),
        license: entry.license.clone(),
        compatibility: entry.compatibility.clone(),
        allowed_tools: entry.allowed_tools.clone(),
        metadata: entry.metadata.clone(),
        resources: entry
            .resources
            .iter()
            .map(|resource| resource.relative_path.clone())
            .collect(),
        target_agents: entry.target_agents.clone(),
        model_families: entry.model_families.clone(),
        activation_tags: entry.activation_tags.clone(),
        required_capabilities: entry.required_capabilities.clone(),
        text,
    }
}

pub fn skill_summaries(
    catalog_vfs: &VfsHandle,
    include_all: bool,
) -> Result<Vec<SkillSummary>, String> {
    let store = SkillCatalogSifsStore::from_vfs(catalog_vfs.clone());
    let catalog = store.load_catalog().map_err(|error| error.to_string())?;
    let entries = if include_all {
        catalog.entries.iter().collect::<Vec<_>>()
    } else {
        catalog.active_entries_for_scopes([SkillActivationScope::Cluster.as_str()])
    };
    Ok(entries
        .into_iter()
        .map(|entry| {
            skill_summary_from_entry(entry, skill_entry_is_active_for_app(&catalog, entry))
        })
        .collect())
}

/// Read one skill descriptor plus its stored `SKILL.md` text.
pub fn skill_detail(
    catalog_vfs: &VfsHandle,
    skill_id: &str,
    version: Option<&str>,
) -> Result<SkillDetail, String> {
    let store = SkillCatalogSifsStore::from_vfs(catalog_vfs.clone());
    let catalog = store.load_catalog().map_err(|error| error.to_string())?;
    let entry = select_skill_entry_for_app(&catalog, skill_id, version)?;
    let text = store
        .read_skill_text(&entry)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "skill `{}@{}` catalog entry references missing content at {}",
                entry.skill_id, entry.version, entry.sifs_path
            )
        })?;
    Ok(skill_detail_from_entry(
        &entry,
        skill_entry_is_active_for_app(&catalog, &entry),
        text,
    ))
}

/// Validate and import an Agent Skills package directory (or its `SKILL.md`).
pub fn import_skill(
    data_dir: &SinomoDataDir,
    path: &Path,
    skill_id: Option<String>,
    version: Option<String>,
    activate: bool,
) -> Result<SkillMutationResult, String> {
    let version = version.unwrap_or_else(|| "1.0.0".to_string());
    let package = read_agent_skill_package(path).map_err(|error| error.to_string())?;
    let store =
        SkillCatalogSifsStore::open(Some(data_dir.root())).map_err(|error| error.to_string())?;
    let mut write = SkillCatalogWrite::from_agent_skill_package(
        skill_id,
        version,
        SkillCatalogSource::user("app-user"),
        package,
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
        skill: skill_summary_from_entry(&entry, activate),
        path: Some(path.display().to_string()),
        scope: activate.then(|| SkillActivationScope::Cluster.as_str().to_string()),
    })
}

/// Export a catalog skill as a portable Agent Skills package.
pub fn export_skill(
    catalog_vfs: &VfsHandle,
    skill_id: &str,
    version: Option<&str>,
    output: Option<&Path>,
) -> Result<SkillMutationResult, String> {
    let store = SkillCatalogSifsStore::from_vfs(catalog_vfs.clone());
    let catalog = store.load_catalog().map_err(|error| error.to_string())?;
    let entry = select_skill_entry_for_app(&catalog, skill_id, version)?;
    let package = store
        .read_skill_package(&entry)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("skill `{}` has no readable package", entry.skill_id))?;
    let rendered = render_common_skill_markdown(&entry, &package.markdown)
        .map_err(|error| error.to_string())?;
    if output.is_some_and(|path| path.is_file()) {
        return Err("Agent Skills exports require a directory path".to_string());
    }
    let output_path = output.map(|path| {
        if path.file_name().and_then(|name| name.to_str()) == Some(entry.skill_id.as_str()) {
            path.to_path_buf()
        } else {
            path.join(&entry.skill_id)
        }
    });
    if let Some(path) = output_path.as_deref() {
        write_agent_skill_package(
            path,
            &sinomo_agent::AgentSkillPackage {
                markdown: rendered,
                files: package.files,
            },
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(SkillMutationResult {
        status: "exported".to_string(),
        skill: skill_summary_from_entry(&entry, skill_entry_is_active_for_app(&catalog, &entry)),
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
        skill: skill_summary_from_entry(&entry, true),
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
            capabilities: app_provider_capabilities("anthropic"),
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
            capabilities: app_provider_capabilities("claude"),
        },
        ProviderSummary {
            id: "openai".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "OpenAI".to_string(),
            models: dedup(vec![
                OpenAiConfig::default().default_model,
                "gpt-5.6-sol".to_string(),
                "gpt-5.6-terra".to_string(),
                "gpt-5.6-luna".to_string(),
            ]),
            capabilities: app_provider_capabilities("openai"),
        },
        ProviderSummary {
            id: "grok".into(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.into(),
            name: "Grok (xAI)".into(),
            models: sinomo_lm::provider_known_model_ids("grok"),
            capabilities: app_provider_capabilities("grok"),
        },
        ProviderSummary {
            id: "kimi".into(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.into(),
            name: "Kimi (Moonshot)".into(),
            models: sinomo_lm::provider_known_model_ids("kimi"),
            capabilities: app_provider_capabilities("kimi"),
        },
        ProviderSummary {
            id: "gemini".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Google Gemini".to_string(),
            models: sinomo_lm::provider_known_model_ids("gemini"),
            capabilities: app_provider_capabilities("gemini"),
        },
        ProviderSummary {
            id: "claude-code".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Claude Code (CLI)".to_string(),
            models: dedup(vec![ClaudeCodeConfig::default().model]),
            capabilities: app_provider_capabilities("claude-code"),
        },
        ProviderSummary {
            id: "codex-cli".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Codex (CLI)".to_string(),
            models: dedup(vec![CodexCliConfig::default().model]),
            capabilities: app_provider_capabilities("codex-cli"),
        },
        ProviderSummary {
            id: "github-copilot".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "GitHub Copilot".to_string(),
            models: dedup(vec![GitHubCopilotConfig::default().default_model]),
            capabilities: app_provider_capabilities("github-copilot"),
        },
        ProviderSummary {
            id: sinomo_lm::OLLAMA_PROVIDER_ID.to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Ollama (local)".to_string(),
            models: vec![sinomo_lm::OLLAMA_DEFAULT_MODEL.to_string()],
            capabilities: app_provider_capabilities(sinomo_lm::OLLAMA_PROVIDER_ID),
        },
        ProviderSummary {
            id: sinomo_lm::LM_STUDIO_PROVIDER_ID.to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "LM Studio (local)".to_string(),
            models: vec![sinomo_lm::LM_STUDIO_DEFAULT_MODEL.to_string()],
            capabilities: app_provider_capabilities(sinomo_lm::LM_STUDIO_PROVIDER_ID),
        },
    ]
}

fn app_provider_capabilities(
    provider: &str,
) -> sinomo_runtime::api::app::chat::ProviderCapabilities {
    let capabilities = sinomo_lm::provider_capabilities(provider);
    sinomo_runtime::api::app::chat::ProviderCapabilities {
        responses_http: capabilities.responses_http,
        responses_websocket: capabilities.responses_websocket,
        realtime_websocket: capabilities.realtime_websocket,
        realtime_webrtc: capabilities.realtime_webrtc,
        web_search: capabilities.web_search,
        image_generation: capabilities.image_generation,
        image_editing: capabilities.image_editing,
        transcription: capabilities.transcription,
        local_model_management: capabilities.local_model_management,
        mcp: capabilities.mcp,
    }
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// Compact visible-version metadata returned alongside the page. Entries may be absent when
    /// talking to an older daemon or a lightweight local backend.
    pub metadata: Vec<SifsEntryMetadata>,
    pub total_count: usize,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsEntryMetadata {
    pub path: String,
    pub object_id: String,
    pub version: String,
    pub modified_at_unix_nanos: u64,
    pub policy: String,
    pub current_locations: Vec<String>,
    pub placement: Option<SifsEntryPlacement>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsEntryPlacement {
    pub agent_state: String,
    pub desired_replicas: usize,
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
    pub placement: Option<sinomo_runtime::dfs::DfsResolvedPlacementPolicy>,
}

pub type SifsPlacementPolicy = sinomo_runtime::dfs::DfsPlacementPolicy;
pub type SifsPlacementScope = sinomo_runtime::dfs::DfsPlacementScope;
pub type SifsPlacementPreview = sinomo_runtime::dfs::DfsPlacementPreview;
pub type SifsPlacementPreviewToken = sinomo_runtime::dfs::DfsPlacementPreviewToken;
pub type SifsPlacementReconciliationJob = sinomo_runtime::dfs::DfsPlacementReconciliationJob;
pub type SifsPlacementActivityStatus =
    sinomo_runtime::dfs::DfsPlacementReconciliationActivityStatus;
pub type SifsPlacementActivityCursor =
    sinomo_runtime::dfs::DfsPlacementReconciliationActivityCursor;
pub type SifsPlacementActivitySummary =
    sinomo_runtime::dfs::DfsPlacementReconciliationActivitySummary;
pub type SifsPlacementActivityPage = sinomo_runtime::dfs::DfsPlacementReconciliationActivityPage;
pub type SifsPlacementActivityRequest =
    sinomo_runtime::api::DaemonDfsBrowserPlacementActivityRequest;

/// Stable DFS identifiers used by the operator views.
pub type DfsFileSystemId = sinomo_runtime::dfs::DfsFileSystemId;
pub type DfsObjectId = sinomo_runtime::dfs::DfsObjectId;
pub type DfsVersion = sinomo_runtime::dfs::DfsVersion;

/// Bounded, daemon-owned DFS operator projections exposed to the GUI.
pub type DfsOperatorStatus = sinomo_runtime::dfs::DfsOperatorStatus;
pub type DfsOperatorObjectView = sinomo_runtime::dfs::DfsOperatorObjectView;
pub type DfsOperatorInspectionCursor = sinomo_runtime::dfs::DfsOperatorInspectionCursor;
pub type DfsOperatorRepairView = sinomo_runtime::dfs::DfsOperatorRepairView;
pub type DfsOperatorRepairPage = sinomo_runtime::dfs::DfsOperatorRepairPage;

/// Typed GUI client for the daemon-owned DFS operator surface.
///
/// Reads stay bounded by the DFS inspection contract. Conflict resolution and
/// version selection are sent to the daemon as authority-owning commands; this
/// client never edits metadata locally and never retries a mutation.
#[derive(Clone)]
pub struct DfsOperatorClient {
    address: String,
    timeout: Duration,
    runtime: Option<tokio::runtime::Handle>,
}

impl DfsOperatorClient {
    pub fn open_daemon(address: impl Into<String>) -> Result<Self, String> {
        Self::open_daemon_with_timeout(address, sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT)
    }

    pub fn open_daemon_with_timeout(
        address: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, String> {
        Self::open_remote(address.into(), timeout, None)
    }

    pub fn open_daemon_on_runtime(
        address: impl Into<String>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        Self::open_remote(
            address.into(),
            sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
            Some(runtime.clone()),
        )
    }

    fn open_remote(
        address: String,
        timeout: Duration,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Result<Self, String> {
        let client = Self {
            address,
            timeout,
            runtime,
        };
        // Match `SifsBrowser`'s open semantics: prove the supplied endpoint
        // serves the expected contract before handing a live handle to the UI.
        client.status()?;
        Ok(client)
    }

    /// Read the daemon's bounded DFS status projection.
    pub fn status(&self) -> Result<DfsOperatorStatus, String> {
        match self.rpc(sinomo_runtime::dfs::DaemonDfsInspectionRequest::Status)? {
            sinomo_runtime::dfs::DaemonDfsInspectionResponse::Status { status } => Ok(status),
            response => Err(unexpected_dfs_operator_response("status", &response)),
        }
    }

    /// Inspect the authoritative object currently visible at a filesystem path.
    pub fn object_by_path(
        &self,
        file_system_id: DfsFileSystemId,
        path: impl Into<String>,
    ) -> Result<Option<DfsOperatorObjectView>, String> {
        match self.rpc(sinomo_runtime::dfs::DaemonDfsInspectionRequest::Path {
            file_system_id,
            path: path.into(),
        })? {
            sinomo_runtime::dfs::DaemonDfsInspectionResponse::Path { object } => Ok(object),
            response => Err(unexpected_dfs_operator_response("path", &response)),
        }
    }

    /// Inspect one object by its stable DFS identity.
    pub fn object(&self, object_id: DfsObjectId) -> Result<Option<DfsOperatorObjectView>, String> {
        match self.rpc(sinomo_runtime::dfs::DaemonDfsInspectionRequest::Object { object_id })? {
            sinomo_runtime::dfs::DaemonDfsInspectionResponse::Object { object } => Ok(object),
            response => Err(unexpected_dfs_operator_response("object", &response)),
        }
    }

    /// Read one explicitly bounded page of conflict records.

    /// Read the versions relevant to selected conflict records on an object.

    /// Resolve the selected conflict records through the daemon's DFS authority.

    /// Restore retained content using an exact destination and stable operation identity.
    pub fn restore_file_history(
        &self,
        body: &sinomo_runtime::dfs::DfsFileHistoryRestoreRequest,
    ) -> Result<sinomo_runtime::dfs::DfsFileHistoryRestoreResponse, String> {
        self.history_rpc(
            sinomo_runtime::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE,
            body,
        )
    }

    /// Read one bounded page of explicitly retained file versions.
    pub fn list_file_history(
        &self,
        body: &sinomo_runtime::dfs::DfsFileHistoryListRequest,
    ) -> Result<sinomo_runtime::dfs::DfsFileHistoryListResponse, String> {
        retry_transient_sifs_browser_read(|| {
            self.history_rpc(
                sinomo_runtime::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE,
                body,
            )
        })
    }

    /// Read one bounded range of an explicitly retained file version.
    pub fn read_file_history(
        &self,
        body: &sinomo_runtime::dfs::DfsFileHistoryReadRequest,
    ) -> Result<sinomo_runtime::dfs::DfsFileHistoryReadOutcome, String> {
        retry_transient_sifs_browser_read(|| {
            self.history_rpc(
                sinomo_runtime::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE,
                body,
            )
        })
    }

    /// Capture the current version, add a named pin, or release one.
    pub fn file_history_pin(
        &self,
        body: &sinomo_runtime::dfs::DfsFileHistoryPinRequest,
    ) -> Result<sinomo_runtime::dfs::DfsFileHistoryPinResponse, String> {
        self.history_rpc(
            sinomo_runtime::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
            body,
        )
    }

    /// Read the filesystem's automatic capture rules.
    pub fn file_history_policy(
        &self,
        file_system_id: DfsFileSystemId,
    ) -> Result<sinomo_runtime::dfs::DfsFileHistoryPolicy, String> {
        retry_transient_sifs_browser_read(|| {
            self.history_rpc(
                sinomo_runtime::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE,
                &sinomo_runtime::dfs::DfsFileHistoryPolicyRequest {
                    file_system_id: file_system_id.clone(),
                },
            )
        })
    }

    fn history_rpc<Req, Resp>(&self, route: &str, body: &Req) -> Result<Resp, String>
    where
        Req: serde::Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            route,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            body,
        )
        .map_err(|error| error.to_string())?;
        self.send_request(&request)?
            .decode_body()
            .map_err(|error| error.to_string())
    }

    /// Read one explicitly bounded page of pending replica repairs.
    pub fn repair_page(
        &self,
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        limit: usize,
        cursor: Option<DfsOperatorInspectionCursor>,
    ) -> Result<DfsOperatorRepairPage, String> {
        let response = self.rpc(sinomo_runtime::dfs::DaemonDfsInspectionRequest::Repair {
            file_system_id,
            object_id,
            limit: Some(limit),
            cursor,
        })?;
        dfs_operator_repair_page_from_response(response)
    }

    fn rpc(
        &self,
        body: sinomo_runtime::dfs::DaemonDfsInspectionRequest,
    ) -> Result<sinomo_runtime::dfs::DaemonDfsInspectionResponse, String> {
        let request = sinomo_runtime::dfs::dfs_inspection_rpc_request(&body)
            .map_err(|error| error.to_string())?;
        let response = self.send_request(&request)?;
        sinomo_runtime::dfs::decode_dfs_inspection_rpc_response(&response)
            .map_err(|error| error.to_string())
    }

    fn send_request(
        &self,
        request: &sinomo_runtime::network::ObjectRpcRequest,
    ) -> Result<sinomo_runtime::network::ObjectRpcResponse, String> {
        if let Some(runtime) = &self.runtime {
            sinomo_runtime::api::send_daemon_rpc_on_runtime(
                &self.address,
                request,
                self.timeout,
                runtime,
            )
            .map_err(|error| error.to_string())
        } else {
            sinomo_runtime::api::send_daemon_rpc(&self.address, request, self.timeout)
                .map_err(|error| error.to_string())
        }
    }
}

fn dfs_operator_repair_page_from_response(
    response: sinomo_runtime::dfs::DaemonDfsInspectionResponse,
) -> Result<DfsOperatorRepairPage, String> {
    match response {
        sinomo_runtime::dfs::DaemonDfsInspectionResponse::Repair {
            repairs,
            next_cursor,
        } => Ok(DfsOperatorRepairPage {
            repairs,
            next_cursor,
        }),
        response => Err(unexpected_dfs_operator_response("repair", &response)),
    }
}

fn unexpected_dfs_operator_response(
    expected: &str,
    response: &sinomo_runtime::dfs::DaemonDfsInspectionResponse,
) -> String {
    let actual = match response {
        sinomo_runtime::dfs::DaemonDfsInspectionResponse::Status { .. } => "status",
        sinomo_runtime::dfs::DaemonDfsInspectionResponse::FileSystem { .. } => "file-system",
        sinomo_runtime::dfs::DaemonDfsInspectionResponse::Path { .. } => "path",
        sinomo_runtime::dfs::DaemonDfsInspectionResponse::Object { .. } => "object",

        sinomo_runtime::dfs::DaemonDfsInspectionResponse::Repair { .. } => "repair",
    };
    format!("daemon returned DFS {actual} response to {expected} request")
}

pub type SifsPlacementUpdateOutcome = sinomo_runtime::api::DaemonDfsBrowserPlacementUpdateResponse;

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

#[derive(Clone)]
pub(crate) struct RemoteSifsBrowser {
    address: String,
    timeout: Duration,
    runtime: Option<tokio::runtime::Handle>,
    target: Option<(SinomoDataDir, NodeId)>,
    operation_deadline: Option<std::time::Instant>,
    direct_main_owner: bool,
    #[cfg(test)]
    test_rpc: Option<RemoteSifsBrowserTestRpc>,
    #[cfg(test)]
    test_result_rpc: Option<RemoteSifsBrowserTestResultRpc>,
}

#[cfg(test)]
type RemoteSifsBrowserTestRpc = Arc<
    dyn Fn(
            ObjectRpcRequest,
            Duration,
        ) -> Result<
            sinomo_runtime::network::ObjectRpcResponse,
            sinomo_runtime::api::DaemonApiError,
        > + Send
        + Sync,
>;

#[cfg(test)]
type RemoteSifsBrowserTestResultRpc = Arc<
    dyn Fn(
            ObjectRpcRequest,
            Duration,
        )
            -> Result<sinomo_runtime::network::ObjectRpcResult, sinomo_runtime::api::DaemonApiError>
        + Send
        + Sync,
>;

impl std::fmt::Debug for RemoteSifsBrowser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteSifsBrowser")
            .field("address", &self.address)
            .field("target_node", &self.target.as_ref().map(|(_, node)| node))
            .finish_non_exhaustive()
    }
}

impl SifsBrowser {
    #[cfg(test)]
    pub(crate) fn from_test_local_backend(backend: SifsFileManagerBackend) -> Self {
        Self {
            node_id: backend.node_label(),
            backend: SifsBrowserBackend::Local(backend),
        }
    }

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
        Self::open_daemon_with_timeout(address, sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT)
    }

    pub fn open_daemon_with_timeout(
        address: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, String> {
        let backend = RemoteSifsBrowser {
            address: address.into(),
            timeout,
            runtime: None,
            target: None,
            operation_deadline: None,
            direct_main_owner: false,
            #[cfg(test)]
            test_rpc: None,
            #[cfg(test)]
            test_result_rpc: None,
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
            target: None,
            operation_deadline: None,
            direct_main_owner: false,
            #[cfg(test)]
            test_rpc: None,
            #[cfg(test)]
            test_result_rpc: None,
        };
        Self::open_remote_backend(backend)
    }

    pub fn open_daemon_node_on_runtime(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
        node_id: NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        let backend = RemoteSifsBrowser {
            address: address.into(),
            timeout: sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
            runtime: Some(runtime.clone()),
            target: Some((data_dir, node_id)),
            operation_deadline: None,
            direct_main_owner: false,
            #[cfg(test)]
            test_rpc: None,
            #[cfg(test)]
            test_result_rpc: None,
        };
        Self::open_remote_backend(backend)
    }

    fn open_remote_backend(backend: RemoteSifsBrowser) -> Result<Self, String> {
        let status: sinomo_runtime::api::DaemonDfsBrowserStatusResponse = backend.read_rpc(
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

    pub fn file_attachment(
        &self,
        request: &sinomo_runtime::dfs::DfsAttachmentRequest,
    ) -> Result<sinomo_runtime::dfs::DfsAttachmentResponse, String> {
        if let sinomo_runtime::dfs::DfsAttachmentRequest::Put {
            source,
            metadata,
            expected,
            operation_id,
            bytes,
        } = request
        {
            return self.upload_file_attachment(
                &sinomo_runtime::dfs::DfsAttachmentUploadRequest {
                    generation: false,
                    source: source.clone(),
                    metadata: metadata.clone(),
                    expected: expected.clone(),
                    operation_id: operation_id.clone(),
                    payload_bytes: bytes.len() as u64,
                },
                std::io::Cursor::new(bytes.clone()),
            );
        }
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.file_attachment(request.clone()),
            SifsBrowserBackend::Remote(remote) => {
                if request.is_mutation() {
                    remote.rpc(sinomo_runtime::dfs::DFS_ATTACHMENT_ROUTE, request)
                } else {
                    remote.read_rpc(sinomo_runtime::dfs::DFS_ATTACHMENT_ROUTE, request)
                }
            }
        }
    }

    pub fn upload_file_attachment<R: std::io::Read + std::io::Seek + Send + 'static>(
        &self,
        request: &sinomo_runtime::dfs::DfsAttachmentUploadRequest,
        reader: R,
    ) -> Result<sinomo_runtime::dfs::DfsAttachmentResponse, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => {
                backend.upload_file_attachment(request.clone(), reader)
            }
            SifsBrowserBackend::Remote(remote) => {
                let head = ObjectRpcRequest::encode_body(
                    sinomo_runtime::dfs::DFS_ATTACHMENT_UPLOAD_ROUTE,
                    sinomo_runtime::api::DAEMON_RPC_CATEGORY,
                    request,
                )
                .map_err(|e| e.to_string())?;
                let body = sinomo_runtime::network::ObjectRpcBodySource::from_reader(
                    reader,
                    Some(request.payload_bytes),
                );
                let (data_dir, target) = match &remote.target {
                    Some((data_dir, node)) => (data_dir.clone(), Some(node.clone())),
                    None => (
                        SinomoDataDir::from_env_or_home().map_err(|e| e.to_string())?,
                        None,
                    ),
                };
                sinomo_runtime::api::transport::send_daemon_call_value_on_runtime(
                    data_dir,
                    &remote.address,
                    target,
                    sinomo_runtime::network::ObjectRpcCall::new(head, body),
                    remote.timeout,
                    remote.runtime.as_ref(),
                )
                .map_err(|e| e.to_string())
            }
        }
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
                let placement = backend.resolved_placement(&stat).ok();
                sifs_details_from_stat(stat, local_pins, metadata_shard, placement)
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
    pub fn read_object_range(
        &self,
        request: &sinomo_runtime::api::DaemonDfsBrowserReadObjectRangeRequest,
    ) -> Result<Vec<u8>, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.read_object_range(request),
            SifsBrowserBackend::Remote(remote) => remote.read_object_range(request),
        }
    }

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

    pub fn preview_placement(
        &self,
        path: &str,
        policy: Option<SifsPlacementPolicy>,
        scope: SifsPlacementScope,
    ) -> Result<SifsPlacementPreview, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.preview_placement(path, policy, scope),
            SifsBrowserBackend::Remote(remote) => remote.preview_placement(path, policy, scope),
        }
    }

    pub fn update_placement(
        &self,
        path: &str,
        policy: Option<SifsPlacementPolicy>,
        scope: SifsPlacementScope,
        expected_preview_token: SifsPlacementPreviewToken,
    ) -> Result<SifsPlacementUpdateOutcome, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => {
                backend.update_placement(path, policy, scope, expected_preview_token)
            }
            SifsBrowserBackend::Remote(remote) => {
                remote.update_placement(path, policy, scope, expected_preview_token)
            }
        }
    }

    pub fn placement_progress(
        &self,
        job_id: &str,
    ) -> Result<SifsPlacementReconciliationJob, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.placement_progress(job_id),
            SifsBrowserBackend::Remote(remote) => remote.placement_progress(job_id),
        }
    }

    pub fn placement_activity(
        &self,
        request: SifsPlacementActivityRequest,
    ) -> Result<SifsPlacementActivityPage, String> {
        match &self.backend {
            SifsBrowserBackend::Local(backend) => backend.placement_activity(
                &request.statuses,
                request.path_prefix.as_deref(),
                request.cursor.as_ref(),
                request.limit,
            ),
            SifsBrowserBackend::Remote(remote) => remote.placement_activity(request),
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
    /// Typed client for the already-checked main owner of this exact directory.
    /// Authentication and routing still use the ordinary daemon client path.
    pub(crate) fn for_main_owner(
        data_dir: SinomoDataDir,
        address: String,
        node_id: NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        Self {
            address,
            timeout: sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
            runtime: Some(runtime.clone()),
            target: Some((data_dir, node_id)),
            operation_deadline: None,
            direct_main_owner: true,
            #[cfg(test)]
            test_rpc: None,
            #[cfg(test)]
            test_result_rpc: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_rpc(mut self, rpc: RemoteSifsBrowserTestRpc) -> Self {
        self.test_rpc = Some(rpc);
        self
    }

    #[cfg(test)]
    fn with_test_result_rpc(mut self, rpc: RemoteSifsBrowserTestResultRpc) -> Self {
        self.test_result_rpc = Some(rpc);
        self
    }

    pub(crate) fn with_operation_deadline(&self, deadline: Option<std::time::Instant>) -> Self {
        let mut client = self.clone();
        client.operation_deadline = deadline;
        client
    }

    fn status(&self) -> Result<sinomo_runtime::api::DaemonDfsBrowserStatusResponse, String> {
        self.read_rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_STATUS_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserStatusRequest::with_health(),
        )
    }

    pub(crate) fn read_rpc<Req, Resp>(&self, route: &str, body: &Req) -> Result<Resp, String>
    where
        Req: serde::Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        self.retry_read(|| self.rpc(route, body))
    }

    fn retry_read<T>(&self, mut read: impl FnMut() -> Result<T, String>) -> Result<T, String> {
        if let Some(deadline) = self.operation_deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            return sinomo_runtime::api::dfs::retry_dfs_browser_read(remaining, |_| read());
        }
        retry_transient_sifs_browser_read(read)
    }

    pub(crate) fn rpc<Req, Resp>(&self, route: &str, body: &Req) -> Result<Resp, String>
    where
        Req: serde::Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        let request =
            ObjectRpcRequest::encode_body(route, sinomo_runtime::api::DAEMON_RPC_CATEGORY, body)
                .map_err(|error| error.to_string())?;
        self.rpc_response(request)
            .map_err(|error| error.to_string())?
            .decode_body()
            .map_err(|error| error.to_string())
    }

    fn rpc_response(
        &self,
        request: ObjectRpcRequest,
    ) -> Result<sinomo_runtime::network::ObjectRpcResponse, sinomo_runtime::api::DaemonApiError>
    {
        let timeout = self.rpc_timeout()?;
        let request = if self.operation_deadline.is_some()
            && request.route == sinomo_runtime::api::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
        {
            sinomo_runtime::dfs::with_dfs_request_budget(request, timeout)
        } else {
            request
        };
        #[cfg(test)]
        if let Some(rpc) = &self.test_rpc {
            return rpc(request, timeout);
        }
        if self.target.is_some() {
            if self.direct_main_owner {
                return self.rpc_main_owner_value(request, timeout, |result| async move {
                    let response = result
                        .into_materialized_value_response(usize::MAX)
                        .await
                        .map_err(sinomo_runtime::api::DaemonApiError::from)?;
                    response.error_detail()?;
                    Ok(response)
                });
            }
            return self.rpc_target_response(request, timeout);
        }
        if let Some(runtime) = &self.runtime {
            sinomo_runtime::api::send_daemon_rpc_on_runtime(
                &self.address,
                &request,
                timeout,
                runtime,
            )
        } else {
            sinomo_runtime::api::send_daemon_rpc(&self.address, &request, timeout)
        }
    }

    fn rpc_target_response(
        &self,
        request: ObjectRpcRequest,
        remaining: Duration,
    ) -> Result<sinomo_runtime::network::ObjectRpcResponse, sinomo_runtime::api::DaemonApiError>
    {
        let (data_dir, target_node_id) = self.target.as_ref().expect("node-scoped browser target");
        let health = self.timeout.min(remaining);
        // A catalog-selected provider keeps the same main-only forwarding and
        // signer/data binding. Transport health is not an operation deadline:
        // only the caller's explicit absolute deadline bounds the whole call,
        // including materializing its returned value.
        sinomo_runtime::api::transport::block_on_daemon_client(Box::pin(async move {
            let overall =
                self.operation_deadline
                    .map(|deadline| {
                        deadline
                        .checked_duration_since(Instant::now())
                        .filter(|remaining| !remaining.is_zero())
                        .ok_or_else(|| sinomo_runtime::api::DaemonApiError::NetworkDelivery {
                            detail: "SIFS operation deadline expired before the node-target request"
                                .into(),
                            delivery_state:
                                sinomo_runtime::network::NetworkDeliveryState::DefinitelyNotSent,
                        })
                    })
                    .transpose()?;
            let deadline = sinomo_runtime::api::transport::daemon_api_overall_deadline(overall);
            sinomo_runtime::api::transport::run_daemon_api_with_overall_deadline(
                "SIFS node-target value call",
                deadline,
                Box::pin(async move {
                    let runtime = tokio::runtime::Handle::current();
                    let result =
                        sinomo_runtime::api::transport::send_daemon_object_rpc_call_with_affinity_async(
                            data_dir.clone(),
                            &self.address,
                            sinomo_runtime::api::DAEMON_OBJECT_RPC_FORWARD_ROUTE,
                            target_node_id,
                            sinomo_runtime::network::ObjectRpcCall::value(request),
                            health,
                            None,
                            overall,
                            false,
                            sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                            sinomo_runtime::network::DaemonRpcTimeouts::connect_and_idle(health),
                            None,
                            None,
                            &runtime,
                        )
                        .await?;
                    sinomo_runtime::api::transport::decode_object_rpc_value_response(result).await
                }),
            )
            .await
        }))
    }

    fn rpc_timeout(&self) -> Result<Duration, sinomo_runtime::api::DaemonApiError> {
        let timeout = match self.operation_deadline {
            Some(deadline) => deadline
                .checked_duration_since(std::time::Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| sinomo_runtime::api::DaemonApiError::NetworkDelivery {
                    detail: "SIFS operation deadline expired before the daemon request".into(),
                    delivery_state:
                        sinomo_runtime::network::NetworkDeliveryState::DefinitelyNotSent,
                })?,
            None => self.timeout,
        };
        Ok(timeout)
    }

    // The same checked main endpoint, signer, health limits and original
    // absolute deadline serve both finalizers. CAS keeps the raw value path.
    fn rpc_main_owner_value<T, F, Fut>(
        &self,
        request: ObjectRpcRequest,
        timeout: Duration,
        finish: F,
    ) -> Result<T, sinomo_runtime::api::DaemonApiError>
    where
        T: Send + 'static,
        F: FnOnce(sinomo_runtime::network::ObjectRpcResult) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, sinomo_runtime::api::DaemonApiError>>
            + Send
            + 'static,
    {
        let (data_dir, _) = self
            .target
            .as_ref()
            .expect("main owner client has its checked target");
        let timeouts =
            sinomo_runtime::network::DaemonRpcTimeouts::connect_and_idle(self.timeout.min(timeout));
        let mut options = sinomo_runtime::network::ClusterObjectRpcOptions::new(timeouts);
        if self.operation_deadline.is_some() {
            options = options.with_overall_timeout(timeout);
        }
        let runtime = self
            .runtime
            .as_ref()
            .expect("main owner client has its runtime");
        sinomo_runtime::api::transport::block_on_daemon_client_on_owner_runtime(
            runtime,
            Box::pin(async move {
                let call = async move {
                    #[cfg(test)]
                    if let Some(rpc) = &self.test_result_rpc {
                        return finish(rpc(request, timeout)?).await;
                    }
                    let result = sinomo_runtime::api::transport::call_daemon_address_rpc_async(
                        data_dir.clone(),
                        &self.address,
                        sinomo_runtime::network::ObjectRpcCall::value(request),
                        options,
                        runtime,
                    )
                    .await?;
                    finish(result).await
                };
                if self.operation_deadline.is_some() {
                    match tokio::time::timeout(timeout, call).await {
                        Ok(result) => result,
                        Err(_) => Err(sinomo_runtime::api::DaemonApiError::NetworkDelivery {
                            detail: "resident SIFS request exceeded its operation deadline".into(),
                            delivery_state:
                                sinomo_runtime::network::NetworkDeliveryState::PossiblyApplied,
                        }),
                    }
                } else {
                    call.await
                }
            }),
        )
    }

    fn rpc_read_file(
        &self,
        path: &str,
    ) -> Result<sinomo_runtime::api::DaemonDfsBrowserReadFileResponse, String> {
        let request = ObjectRpcRequest::encode_body(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            sinomo_runtime::api::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::api::DaemonDfsBrowserReadFileRequest {
                path: path.to_string(),
            },
        )
        .map_err(|error| error.to_string())?;
        let timeout = self.rpc_timeout().map_err(|error| error.to_string())?;
        #[cfg(test)]
        if let Some(rpc) = &self.test_rpc {
            return rpc(request, timeout)
                .map_err(|error| error.to_string())?
                .decode_body()
                .map_err(|error| error.to_string());
        }
        self.rpc_main_owner_value(request, timeout, |result| async move {
            // Decode directly from the bounded ownership ring into the final
            // typed payload; do not retain a second encoded whole-file vector.
            result
                .decode_body::<sinomo_runtime::api::DaemonDfsBrowserReadFileResponse>()
                .await
                .map_err(sinomo_runtime::api::DaemonApiError::from)
        })
        .map_err(|error| error.to_string())
    }

    fn list(
        &self,
        path: &str,
        shallow: bool,
        missing_is_empty: bool,
    ) -> Result<Vec<SifsEntry>, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserListResponse = self.read_rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_LIST_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow,
                missing_is_empty,
                sort_key: sinomo_runtime::api::DaemonDfsBrowserSortKey::Name,
                sort_direction: sinomo_runtime::api::DaemonDfsBrowserSortDirection::Ascending,
                offset: 0,
                limit: None,
                include_metadata: false,
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
        let response: sinomo_runtime::api::DaemonDfsBrowserListResponse = self.read_rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_LIST_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow,
                missing_is_empty,
                sort_key: daemon_sort_key_from_sifs(options.sort_key),
                sort_direction: daemon_sort_direction_from_sifs(options.sort_direction),
                offset: options.offset,
                limit: options.limit,
                include_metadata: true,
            },
        )?;
        let mut metadata = Vec::new();
        let entries = response
            .entries
            .into_iter()
            .map(|entry| {
                if let Some(summary) = sifs_entry_metadata_from_rpc(&entry) {
                    metadata.push(summary);
                }
                sifs_entry_from_rpc(entry)
            })
            .collect();
        Ok(SifsEntryPage {
            entries,
            metadata,
            total_count: response.total_count,
            offset: response.offset,
            limit: response.limit,
        })
    }

    fn stat(&self, path: &str) -> Result<SifsObjectDetails, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserStatOutcomeResponse = self.read_rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_STAT_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserStatRequest {
                path: path.to_string(),
                // File-detail panes commonly ask for the same object several
                // times while rendering. Reuse only a very recent result;
                // callers that require strict freshness use zero.
                max_staleness_millis: 250,
            },
        )?;
        let response = response.into_result().map_err(|error| error.to_string())?;
        Ok(sifs_details_from_rpc(response))
    }

    pub(crate) fn read_file(&self, path: &str) -> Result<Vec<u8>, String> {
        if self.direct_main_owner {
            return self
                .retry_read(|| self.rpc_read_file(path))?
                .into_result()
                .map_err(|error| error.to_string());
        }
        let response: sinomo_runtime::api::DaemonDfsBrowserReadFileResponse = self.read_rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserReadFileRequest {
                path: path.to_string(),
            },
        )?;
        response.into_result().map_err(|error| error.to_string())
    }

    pub(crate) fn read_object_range(
        &self,
        request: &sinomo_runtime::api::DaemonDfsBrowserReadObjectRangeRequest,
    ) -> Result<Vec<u8>, String> {
        let reply: sinomo_runtime::api::DaemonDfsBrowserReadObjectRangeResponse = self.read_rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_OBJECT_RANGE_ROUTE,
            request,
        )?;
        if reply.request != *request
            || reply.bytes.len() as u64
                != request
                    .size_bytes
                    .saturating_sub(request.offset)
                    .min(u64::from(request.length))
        {
            return Err("SIFS returned a different version or range".into());
        }
        Ok(reply.bytes)
    }

    pub(crate) fn read_object_version(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<Vec<u8>, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserReadObjectVersionResponse = self
            .read_rpc(
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

    pub(crate) fn read_file_prefix(
        &self,
        path: &str,
        max_bytes: u64,
    ) -> Result<SifsFileRead, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserReadFilePrefixResponse = self.read_rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserReadFilePrefixRequest {
                path: path.to_string(),
                max_bytes,
            },
        )?;
        let (bytes, truncated) = response.into_result().map_err(|error| error.to_string())?;
        Ok(SifsFileRead { bytes, truncated })
    }

    pub(crate) fn read_files(
        &self,
        paths: &[String],
    ) -> Result<Vec<(String, Result<Vec<u8>, String>)>, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserReadFilesResponse = self.read_rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserReadFilesRequest {
                paths: paths.to_vec(),
            },
        )?;
        Ok(response
            .files
            .into_iter()
            .map(|file| {
                let path = file.path.clone();
                let result = file.into_result().map_err(|error| error.to_string());
                (path, result)
            })
            .collect())
    }

    pub(crate) fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonDfsBrowserWriteFileResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserWriteFileRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
            },
        )?;
        Ok(())
    }

    pub(crate) fn replace_file_if_version(
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
        let response = self.rpc_response(request);
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

    pub(crate) fn create_dir_all(&self, path: &str) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonDfsBrowserCreateDirAllResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserCreateDirAllRequest {
                path: path.to_string(),
            },
        )?;
        Ok(())
    }

    pub(crate) fn rename(&self, from: &str, to: &str) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonDfsBrowserRenameResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_RENAME_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserRenameRequest {
                from: from.to_string(),
                to: to.to_string(),
            },
        )?;
        Ok(())
    }

    pub(crate) fn delete(&self, path: &str, directory: bool) -> Result<(), String> {
        let _: sinomo_runtime::api::DaemonDfsBrowserDeleteResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_DELETE_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserDeleteRequest {
                path: path.to_string(),
                directory,
            },
        )?;
        Ok(())
    }

    pub(crate) fn request_maintenance_refresh(&self) -> Result<(), String> {
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

    pub(crate) fn preview_placement(
        &self,
        path: &str,
        policy: Option<SifsPlacementPolicy>,
        scope: SifsPlacementScope,
    ) -> Result<SifsPlacementPreview, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserPlacementPreviewResponse = self
            .read_rpc(
                sinomo_runtime::api::DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
                &sinomo_runtime::api::DaemonDfsBrowserPlacementPreviewRequest {
                    path: path.to_string(),
                    policy,
                    scope,
                },
            )?;
        Ok(response.preview)
    }

    pub(crate) fn update_placement(
        &self,
        path: &str,
        policy: Option<SifsPlacementPolicy>,
        scope: SifsPlacementScope,
        expected_preview_token: SifsPlacementPreviewToken,
    ) -> Result<SifsPlacementUpdateOutcome, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserPlacementUpdateResponse = self.rpc(
            sinomo_runtime::api::DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
            &sinomo_runtime::api::DaemonDfsBrowserPlacementUpdateV2Request {
                path: path.to_string(),
                policy,
                scope,
                expected_preview_token,
                timeout_millis: Some(
                    self.rpc_timeout()
                        .map_err(|error| error.to_string())?
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                ),
            },
        )?;
        Ok(response)
    }

    fn placement_progress(&self, job_id: &str) -> Result<SifsPlacementReconciliationJob, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserPlacementProgressResponse = self
            .read_rpc(
                sinomo_runtime::api::DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE,
                &sinomo_runtime::api::DaemonDfsBrowserPlacementProgressRequest {
                    job_id: job_id.to_string(),
                },
            )?;
        Ok(response.reconciliation)
    }

    fn placement_activity(
        &self,
        request: SifsPlacementActivityRequest,
    ) -> Result<SifsPlacementActivityPage, String> {
        let response: sinomo_runtime::api::DaemonDfsBrowserPlacementActivityResponse = self
            .read_rpc(
                sinomo_runtime::api::DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE,
                &request,
            )?;
        Ok(response.page)
    }
}

fn retry_transient_sifs_browser_read<T>(
    work: impl FnMut() -> Result<T, String>,
) -> Result<T, String> {
    retry_transient_sifs_browser_read_with_policy(
        work,
        SIFS_BROWSER_READ_RETRY_BUDGET,
        SIFS_BROWSER_READ_RETRY_INITIAL_DELAY,
        SIFS_BROWSER_READ_RETRY_MAX_DELAY,
    )
}

fn retry_transient_sifs_browser_read_with_policy<T>(
    mut work: impl FnMut() -> Result<T, String>,
    budget: Duration,
    initial_delay: Duration,
    max_delay: Duration,
) -> Result<T, String> {
    let started = Instant::now();
    let mut delay = initial_delay.min(max_delay);
    loop {
        match work() {
            Ok(value) => return Ok(value),
            Err(error) if sifs_browser_read_should_retry(&error) => {
                let Some(remaining) = budget.checked_sub(started.elapsed()) else {
                    return Err(error);
                };
                if remaining.is_zero() {
                    return Err(error);
                }
                thread::sleep(delay.min(remaining));
                delay = (delay * 2).min(max_delay);
            }
            Err(error) => return Err(error),
        }
    }
}

fn sifs_browser_read_should_retry(error: &str) -> bool {
    sinomo_runtime::api::dfs::dfs_browser_read_should_retry(error)
}

fn sifs_immutable_write_should_reconcile(error: &str) -> bool {
    error.contains("PossiblyApplied")
        || error.contains("delivery state is possibly-applied")
        || (error.contains("retained metadata authority generation")
            && error.contains("is not ready"))
        || error.contains("content is unavailable from every eligible replica")
        || sinomo_runtime::api::dfs::read_retry::dfs_browser_admission_is_busy(error)
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

fn sifs_entry_metadata_from_rpc(
    entry: &sinomo_runtime::api::DaemonDfsBrowserEntry,
) -> Option<SifsEntryMetadata> {
    let metadata = entry.metadata.as_ref()?;
    Some(SifsEntryMetadata {
        path: entry.path.clone(),
        object_id: metadata.object_id.clone(),
        version: metadata.version.clone(),
        modified_at_unix_nanos: metadata.modified_at_unix_nanos,
        policy: sifs_policy_label(&metadata.policy).to_string(),
        current_locations: metadata.current_locations.clone(),
        placement: metadata
            .placement
            .as_ref()
            .map(|placement| SifsEntryPlacement {
                agent_state: sifs_agent_state_label(&placement.agent_state).to_string(),
                desired_replicas: placement.desired_replicas,
            }),
    })
}

fn sifs_details_from_stat(
    stat: sinomo_runtime::dfs::DfsStat,
    local_pins: Vec<sinomo_runtime::dfs::DfsPinRecord>,
    metadata_shard: Option<SifsMetadataShardDetails>,
    placement: Option<sinomo_runtime::dfs::DfsResolvedPlacementPolicy>,
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
        placement,
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
        placement: response.placement,
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
        metadata: Vec::new(),
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
    error.starts_with("SIFS path not found:")
}

fn channel_optional_bulk_read_bytes(
    path: &str,
    result: Result<Vec<u8>, String>,
) -> Result<Option<Vec<u8>>, AgentChannelStoreError> {
    match result {
        Ok(bytes) => Ok(Some(bytes)),
        Err(detail) if sifs_error_is_not_found(&detail) => Ok(None),
        Err(detail) => Err(channel_store_io(path, detail)),
    }
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
#[path = "app_api/node_scoped_rpc_tests.rs"]
mod node_scoped_rpc_tests;

#[cfg(test)]
#[path = "app_api/channel_lost_reply_native_tests.rs"]
mod channel_lost_reply_native_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_agent::{
        AgentInteractionState, AgentLoopDecision, AgentLoopResponseMode, AgentRole, AgentStopReason,
    };
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};

    fn native_fixture_local_node_id(data: &SinomoDataDir) -> NodeId {
        sinomo_runtime::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data))
            .expect("read the actual native fixture identity")
            .local_node_id
    }

    /// A placement-policy unit has no requester identity, while its injected
    /// catalog/channel/checkpoint transport has an actual separate native owner.
    /// This is not public daemonless DFS initialization or cluster acceptance.
    fn chat_with_native_stores_without_local_identity(
        caller: SinomoDataDir,
        provider: &SinomoDataDir,
    ) -> AgentChat {
        assert!(read_cli_cluster_state(&ClusterPaths::new(&caller)).is_err());
        let catalog = sinomo_runtime::vfs::mounted_sifs(Some(provider.root()))
            .expect("actual initialized provider catalog transport");
        let channels = SifsAgentChannelStore::open(provider).expect("native provider channels");
        let checkpoints =
            SifsAgentSessionCheckpointStore::open(provider).expect("native provider checkpoints");
        let chat = AgentChat::with_runtime_control_registry_and_stores(
            caller,
            false,
            AgentRuntimeControlRegistry::new(),
            catalog,
            true,
            Box::new(channels),
            checkpoints,
        )
        .expect("build placement-policy runtime with explicit native stores");
        assert!(
            read_cli_cluster_state(&ClusterPaths::new(&chat.data_dir)).is_err(),
            "constructing the unit must not initialize a caller and then claim it lacks identity"
        );
        chat
    }

    /// These two tests isolate session/registry transaction rollback. Inject an
    /// existing typed backend over an explicit actual native test namespace;
    /// canonical notes/private HOME/Main transport remain separate integration
    /// obligations exercised by the daemon-backed consumer lifecycle.
    fn inject_native_notes_for_session_rollback_unit(chat: &AgentChat, data: &SinomoDataDir) {
        let vfs = sinomo_runtime::vfs::mounted_sifs(Some(data.root()))
            .expect("actual native notes fixture transport");
        let backend = sinomo_agent::VfsMemoryNoteBackend::from_vfs(vfs, "main-agent")
            .expect("typed injected native notes backend");
        let notes = sinomo_agent::memory_notes::registry(
            sinomo_agent::MemoryNoteAgentToolConfig::from_backend(backend),
        );
        chat.runtime
            .handler()
            .tool_registry()
            .replace_registered_tools_from(&notes);
    }

    fn assert_actual_local_sandbox(chat: &AgentChat, data: &SinomoDataDir, node: &NodeId) {
        let session = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("actual runtime session")
            .expect("runtime attached");
        let attachment = session
            .sandbox_attachment
            .expect("normal lifecycle attached a sandbox");
        assert_eq!(attachment.server_id, node.to_string());
        let id = sinomo_runtime::sandbox::SandboxId::new(attachment.sandbox_id)
            .expect("native sandbox identity");
        let status = crate::sandbox_lifecycle_client(data)
            .status(
                Some(node),
                sinomo_runtime::sandbox::SandboxStatusRequest {
                    sandbox_id: id.clone(),
                },
            )
            .expect("real local in-process sandbox lifecycle owner returns status");
        assert_eq!(status.manifest.sandbox_id, id);
    }


    fn typed_read_owner(data_dir: &SinomoDataDir) -> (tokio::runtime::Runtime, RemoteSifsBrowser) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let client = RemoteSifsBrowser::for_main_owner(
            data_dir.clone(),
            "127.0.0.1:1".into(),
            NodeId::from_legacy_named("typed-read-owner").unwrap(),
            runtime.handle(),
        );
        (runtime, client)
    }

    #[test]
    #[ignore = "requires explicit fresh DATA and an externally measured 2 GiB/no-swap owner"]
    fn actual_browser_owner_large_read_memory_control() {
        use sinomo_runtime::dfs::{DfsWriteOptions, test_support::DfsTestRuntime};
        use sinomo_runtime::network::{ClusterObjectRpcOptions, ObjectRpcCall};
        use std::sync::atomic::AtomicUsize;

        const BYTES: usize = 256 * 1024 * 1024;
        const BASE: &str = "/services/cluster-tests/browser-owner-memory-01";
        let root = std::path::PathBuf::from(
            std::env::var_os("SINOMO_BROWSER_MEMORY_DATA_DIR")
                .expect("explicit fresh browser memory control DATA directory"),
        );
        assert!(root.is_absolute());
        assert_eq!(
            std::fs::symlink_metadata(&root)
                .expect_err("the control must not reuse existing DATA")
                .kind(),
            std::io::ErrorKind::NotFound
        );
        std::fs::create_dir(&root).expect("create only the explicit fresh DATA directory");
        let data = SinomoDataDir::new(root);
        let started = std::time::Instant::now();
        let phase = |name: &str| {
            eprintln!(
                "browser-memory-control {}",
                serde_json::json!({
                    "phase": name,
                    "pid": std::process::id(),
                    "elapsed_ns": started.elapsed().as_nanos(),
                    "unix_ns": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
                    "data": data.root(),
                    "scope": "in-process local owner; not six-node/main-shadow acceptance",
                })
            );
        };
        phase("setup-start");
        let (native, node) =
            DfsTestRuntime::start_single_node_in_data_dir("browser-owner-memory-control", data.clone())
                .expect("start the actual native owner");
        phase("setup-complete");
        let mut writer = native.client(&node).expect("ordinary native write client");
        writer
            .posix_mkdir_all(BASE)
            .expect("create the test namespace once");
        for name in ["large-a.bin", "large-b.bin"] {
            phase(&format!("seed-{name}-start"));
            let path = format!("{BASE}/{name}");
            let pattern = format!("{path}|sinomo-dfs-large|sinomo-dfs-payload|");
            assert_eq!(pattern.len(), 96);
            let mut payload = vec![0; BYTES];
            for chunk in payload.chunks_mut(pattern.len()) {
                chunk.copy_from_slice(&pattern.as_bytes()[..chunk.len()]);
            }
            let outcome = writer
                .posix_write_file(
                    &path,
                    &payload,
                    DfsWriteOptions::replicated_eventual(vec![node.clone()]),
                )
                .expect("the original production seed write must succeed without replay");
            assert_eq!(outcome.size_bytes, Some(BYTES as u64));
            assert!(outcome.current_locations.contains(&node));
            drop(payload);
            phase(&format!("seed-{name}-payload-dropped"));
        }
        drop(writer);
        native.checkpoint_rpc_observation_window().unwrap();
        phase("seeds-complete");

        // Adapt only transport to the registered host with its actual local signer
        // and generation. Keep the native result; never materialize an encoded Vec.
        let transport = native
            .component_context(&node)
            .unwrap()
            .object_rpc_transport()
            .unwrap();
        let runtime = native.runtime_handle();
        let target = node.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let mut client = RemoteSifsBrowser::for_main_owner(
            data.clone(),
            "actual-native-browser-memory-owner".into(),
            node.clone(),
            &runtime,
        )
        .with_test_result_rpc(Arc::new(move |request, remaining| {
            assert_eq!(
                request.route,
                sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE
            );
            assert_eq!(
                count.fetch_add(1, Ordering::SeqCst),
                0,
                "one original read only"
            );
            transport
                .call(
                    &target,
                    ObjectRpcCall::value(request),
                    ClusterObjectRpcOptions::default().with_overall_timeout(remaining),
                    true,
                )
                .map_err(sinomo_runtime::api::DaemonApiError::from)
        }));
        let path = format!("{BASE}/large-b.bin");
        let pattern = format!("{path}|sinomo-dfs-large|sinomo-dfs-payload|");
        assert_eq!(pattern.len(), 96);
        phase("read-start");
        client = client.with_operation_deadline(Some(
            std::time::Instant::now() + sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
        ));
        let actual = client
            .rpc_read_file(&path)
            .expect("original real browser RPC must complete within its ordinary deadline")
            .into_result()
            .expect("original browser read must return found content");
        phase("read-returned");
        assert_eq!(actual.len(), BYTES);
        assert!(
            actual
                .chunks(pattern.len())
                .all(|chunk| chunk == &pattern.as_bytes()[..chunk.len()])
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(actual);
        phase("read-result-dropped");
        drop(client);
        drop(runtime);
        let completed: Vec<_> = native
            .rpc_observations()
            .into_iter()
            .filter(|call| {
                call.source == node
                    && call.target == node
                    && call.route == sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE
                    && call.phase == sinomo_runtime::dfs::test_support::DfsTestRpcPhase::Completed
            })
            .collect();
        assert_eq!(completed.len(), 1);
        let signer = completed[0]
            .signer
            .as_ref()
            .expect("actual receiver signer");
        assert!(signer.is_local());
        assert_eq!(signer.cluster_identity().unwrap().0, node.as_str());
        drop(completed);
        phase("drain-start");
        native.close().expect("drain the exact native owner");
        phase("drain-complete");
    }

    #[test]
    fn resident_bulk_read_rejects_unknown_owner_route_without_legacy_fallback() {
        let root = sinomo_testdir::TestDir::new("resident-bulk-read-unknown-route");
        let (_runtime, client) = typed_read_owner(&SinomoDataDir::new(root.path()));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let client = client.with_test_result_rpc(Arc::new(move |request, _| {
            assert_eq!(
                request.route,
                sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE
            );
            observed.fetch_add(1, Ordering::SeqCst);
            Err(sinomo_runtime::api::DaemonApiError::UnknownRoute {
                route: request.route,
            })
        }));
        let error = client.read_file("/services/large.bin").unwrap_err();
        assert!(error.contains("read-file-v7"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn resident_file_read_decodes_chunked_owned_value_and_preserves_all_bytes() {
        let root = sinomo_testdir::TestDir::new("resident-typed-chunked-read");
        let (_runtime, client) = typed_read_owner(&SinomoDataDir::new(root.path()));
        let expected = (0..196608usize)
            .map(|index| index as u8)
            .collect::<Vec<_>>();
        let payload = expected.clone();
        let chunks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = chunks.clone();
        let client = client.with_test_result_rpc(Arc::new(move |request, _| {
            assert_eq!(
                request.route,
                sinomo_runtime::api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE
            );
            let read: sinomo_runtime::api::DaemonDfsBrowserReadFileRequest =
                request.decode_body().unwrap();
            assert_eq!(read.path, "/services/chunked.bin");
            let encoded = sinomo_runtime::network::ObjectRpcResponse::encode_body(
                &request.route,
                &sinomo_runtime::api::DaemonDfsBrowserReadFileResponse::found(payload.clone()),
            )
            .unwrap();
            let (sender, body) = sinomo_runtime::network::ObjectRpcIncomingBody::test_channel(
                Some(encoded.body.len() as u64),
                1,
            );
            let count = count.clone();
            tokio::spawn(async move {
                for chunk in encoded.body.chunks(8192) {
                    sender.send(chunk.to_vec()).await.unwrap();
                    count.fetch_add(1, Ordering::SeqCst);
                }
            });
            Ok(
                sinomo_runtime::network::ObjectRpcResult::from_forwarded_body(
                    sinomo_runtime::network::ObjectRpcResponse::from_encoded_body(
                        request.route,
                        Vec::new(),
                    ),
                    body,
                    true,
                )
                .unwrap(),
            )
        }));
        assert_eq!(client.read_file("/services/chunked.bin").unwrap(), expected);
        assert!(chunks.load(Ordering::SeqCst) > 1);
    }

    #[test]
    fn resident_file_read_rejects_remote_error_before_waiting_for_body_without_fallback() {
        let root = sinomo_testdir::TestDir::new("resident-typed-read-error");
        let (_runtime, client) = typed_read_owner(&SinomoDataDir::new(root.path()));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let retained = Arc::new(std::sync::Mutex::new(None));
        let sender_slot = retained.clone();
        let client = client.with_test_result_rpc(Arc::new(move |request, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            let (sender, body) =
                sinomo_runtime::network::ObjectRpcIncomingBody::test_channel(Some(100), 1);
            *sender_slot.lock().unwrap() = Some(sender);
            Ok(
                sinomo_runtime::network::ObjectRpcResult::from_forwarded_body(
                    sinomo_runtime::network::ObjectRpcResponse::encode_error(
                        request.route,
                        "remote read rejected",
                    )
                    .unwrap(),
                    body,
                    true,
                )
                .unwrap(),
            )
        }));
        let error = client.read_file("/services/rejected.bin").unwrap_err();
        assert!(error.contains("remote read rejected"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            retained
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .try_send(vec![0])
                .is_err()
        );
    }

    #[test]
    fn resident_file_read_preserves_typed_absence_and_rejects_semantic_attachments() {
        let root = sinomo_testdir::TestDir::new("resident-typed-read-outcomes");
        let (_runtime, client) = typed_read_owner(&SinomoDataDir::new(root.path()));
        let missing = client
            .clone()
            .with_test_result_rpc(Arc::new(move |request, _| {
                Ok(sinomo_runtime::network::ObjectRpcResult::value(
                    sinomo_runtime::network::ObjectRpcResponse::encode_body(
                        &request.route,
                        &sinomo_runtime::api::DaemonDfsBrowserReadFileResponse::not_found(
                            "/services/missing",
                        ),
                    )
                    .unwrap(),
                ))
            }));
        assert!(
            missing
                .read_file("/services/missing")
                .unwrap_err()
                .starts_with("SIFS path not found:")
        );
        let attached = client.with_test_result_rpc(Arc::new(move |request, _| {
            let encoded = sinomo_runtime::network::ObjectRpcResponse::encode_body(
                &request.route,
                &sinomo_runtime::api::DaemonDfsBrowserReadFileResponse::found(vec![1, 2, 3]),
            )
            .unwrap();
            Ok(sinomo_runtime::network::ObjectRpcResult::new(
                sinomo_runtime::network::ObjectRpcResponse::from_encoded_body(
                    request.route,
                    Vec::new(),
                ),
                sinomo_runtime::network::ObjectRpcIncomingBody::bytes(encoded.body),
            ))
        }));
        assert!(
            attached
                .read_file("/services/attached")
                .unwrap_err()
                .contains("semantic attachment")
        );
    }

    #[test]
    fn resident_file_read_original_deadline_cancels_an_unfinished_typed_body() {
        let root = sinomo_testdir::TestDir::new("resident-typed-read-deadline");
        let (_runtime, client) = typed_read_owner(&SinomoDataDir::new(root.path()));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let retained = Arc::new(std::sync::Mutex::new(None));
        let sender_slot = retained.clone();
        let client = client
            .with_test_result_rpc(Arc::new(move |request, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                let (sender, body) =
                    sinomo_runtime::network::ObjectRpcIncomingBody::test_channel(Some(100), 1);
                *sender_slot.lock().unwrap() = Some(sender);
                Ok(
                    sinomo_runtime::network::ObjectRpcResult::from_forwarded_body(
                        sinomo_runtime::network::ObjectRpcResponse::from_encoded_body(
                            request.route,
                            Vec::new(),
                        ),
                        body,
                        true,
                    )
                    .unwrap(),
                )
            }))
            .with_operation_deadline(Some(std::time::Instant::now() + Duration::from_millis(100)));
        let error = client.read_file("/services/stalled.bin").unwrap_err();
        assert!(error.contains("exceeded its operation deadline"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            retained
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .try_send(vec![0])
                .is_err()
        );
    }

    #[test]
    fn chat_agent_summaries_expose_only_interactive_roles() {
        let ids = built_in_agent_summaries()
            .into_iter()
            .map(|summary| summary.key)
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["coding-agent", "main-agent"]);
    }

    #[test]
    fn dfs_operator_repair_response_maps_page_and_rejects_other_variants() {
        let file_system_id =
            DfsFileSystemId::from_legacy_named("app_api_repair_filesystem").expect("filesystem");
        let object_id = DfsObjectId::from_legacy_named("app_api_repair_object").expect("object");
        let version = DfsVersion::from_legacy_named("app_api_repair_version").expect("version");
        let missing_replica_node_id =
            NodeId::from_legacy_named("app_api_repair_node").expect("node");
        let repair = DfsOperatorRepairView {
            intent_id: "intent-app-api-repair".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some("blake3:repair".to_string()),
            missing_replica_node_id: missing_replica_node_id.clone(),
        };
        let next_cursor = DfsOperatorInspectionCursor {
            file_system_id,
            object_cursor: None,
            object_id: Some(object_id),
            item_key: Some(
                sinomo_runtime::dfs::DfsOperatorInspectionItemCursor::Repair {
                    intent_id: repair.intent_id.clone(),
                    version,
                    missing_replica_node_id,
                },
            ),
        };

        let page = dfs_operator_repair_page_from_response(
            sinomo_runtime::dfs::DaemonDfsInspectionResponse::Repair {
                repairs: vec![repair.clone()],
                next_cursor: Some(next_cursor.clone()),
            },
        )
        .expect("repair response");
        assert_eq!(page.repairs, vec![repair]);
        assert_eq!(page.next_cursor, Some(next_cursor));

        let error = dfs_operator_repair_page_from_response(
            sinomo_runtime::dfs::DaemonDfsInspectionResponse::Object { object: None },
        )
        .expect_err("object response must not satisfy a repair request");
        assert_eq!(
            error,
            "daemon returned DFS object response to repair request"
        );
    }

    #[test]
    fn app_catalog_summaries_use_the_supplied_read_transport() {
        let root = sinomo_testdir::TestDir::new("app-catalog-read-vfs");
        let vfs = VfsHandle::memory();

        let agents = agent_summaries(&vfs);
        let skills = skill_summaries(&vfs, true).expect("empty memory skill catalog");

        assert!(agents.iter().any(|agent| agent.key == "main-agent"));
        assert!(skills.is_empty());
        assert!(
            !root.join("dfs").exists(),
            "app summary reads must not construct a mounted DFS from a data directory"
        );
    }

    #[test]
    fn daemonless_chat_resolution_uses_built_ins_but_rejects_custom_keys() {
        let root = sinomo_testdir::TestDir::new("daemonless-chat-agent-resolution");
        let vfs = VfsHandle::memory();

        let built_in = resolve_agent(&vfs, false, "coding-agent")
            .expect("built-in resolution must not require daemon SIFS")
            .expect("coding agent");
        assert_eq!(built_in.key(), "coding-agent");

        let error = resolve_agent(&vfs, false, "custom-agent")
            .expect_err("custom agent resolution requires resident daemon SIFS");
        assert!(error.to_string().contains("resident daemon SIFS catalog"));
        assert!(
            !root.join("dfs").exists(),
            "daemonless exact resolution must not construct local DFS state"
        );
    }

    #[test]
    fn connected_gui_chat_rejects_a_missing_filesystem_provider_before_local_mount() {
        let root = sinomo_testdir::TestDir::new("connected-chat-missing-filesystem-provider");
        let data_dir = SinomoDataDir::new(root.join("data"));

        let error = match AgentChat::with_runtime_control_registry(
            data_dir.clone(),
            true,
            AgentRuntimeControlRegistry::new(),
        ) {
            Ok(_) => panic!("connected chat must require a filesystem provider"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("filesystem provider transport"));
        assert!(
            !data_dir.root().join("dfs").exists(),
            "a missing connected transport must fail before mounting local DFS"
        );
    }

    #[test]
    fn gui_chat_runtime_loads_resident_skill_instructions_and_resources_without_local_dfs() {
        let root = sinomo_testdir::TestDir::new("app-chat-runtime-resident-skills");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let vfs = VfsHandle::memory();
        let markdown = "---\nname: resident-gui-skill\ndescription: Exercise resident GUI loading.\n---\n\n# Resident GUI skill\nFollow the resident instructions.\n";
        let resource = sinomo_agent::AgentSkillPackageFile {
            relative_path: "references/checklist.md".to_string(),
            bytes: b"# Checklist\nVerify resident resources.\n".to_vec(),
        };
        let write = SkillCatalogWrite::from_agent_skill_package(
            None,
            "1.0.0",
            SkillCatalogSource::user("gui-runtime-test"),
            sinomo_agent::AgentSkillPackage {
                markdown: markdown.to_string(),
                files: vec![resource.clone()],
            },
        )
        .expect("skill package")
        .activate_for_scope(
            SkillActivationScope::Cluster.as_str(),
            "test",
            "GUI runtime regression",
        );
        let entry = write.entry.clone();
        let key = sinomo_agent::skill_catalog_entry_text_key(&entry);
        SkillCatalogSifsStore::from_vfs(vfs.clone())
            .write_skill_version(write)
            .expect("persist resident skill");
        assert!(!data_dir.root().join("dfs").exists());

        let handler = tui::app_build_chat_handler(&data_dir, true, Some(vfs))
            .expect("GUI chat handler with resident skill catalog");
        let (catalog, texts, resources) = tui::app_runtime_skill_catalog_for_test(&handler);

        assert!(catalog.entry("resident-gui-skill", "1.0.0").is_some());
        assert_eq!(texts.get(&key).map(String::as_str), Some(markdown));
        assert_eq!(resources.get(&key), Some(&vec![resource]));
        assert!(
            !data_dir.root().join("dfs").exists(),
            "resident GUI catalog loading must not mount process-local DFS state"
        );
    }

    #[test]
    fn gui_chat_runtime_rejects_missing_resident_skill_package() {
        let root = sinomo_testdir::TestDir::new("app-chat-runtime-missing-skill");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let vfs = VfsHandle::memory();
        let write = SkillCatalogWrite::from_markdown_skill(
            None,
            "1.0.0",
            SkillCatalogSource::user("gui-runtime-test"),
            "---\nname: missing-gui-skill\ndescription: Must fail closed.\n---\n\n# Missing GUI skill\n",
        )
        .expect("skill write");
        let skill_path = write.entry.sifs_path.clone();
        SkillCatalogSifsStore::from_vfs(vfs.clone())
            .write_skill_version(write)
            .expect("persist skill fixture");
        vfs.remove(&skill_path)
            .expect("remove referenced package text");

        let error = match tui::app_build_chat_handler(&data_dir, true, Some(vfs)) {
            Ok(_) => panic!("GUI runtime must reject a missing referenced package"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("references missing package content")
        );
        assert!(!data_dir.root().join("dfs").exists());
    }

    #[test]
    fn gui_chat_runtime_rejects_malformed_resident_skill_package() {
        let root = sinomo_testdir::TestDir::new("app-chat-runtime-malformed-skill");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let vfs = VfsHandle::memory();
        let write = SkillCatalogWrite::from_markdown_skill(
            None,
            "1.0.0",
            SkillCatalogSource::user("gui-runtime-test"),
            "---\nname: malformed-gui-skill\ndescription: Must fail closed.\n---\n\n# Malformed GUI skill\n",
        )
        .expect("skill write");
        let skill_path = write.entry.sifs_path.clone();
        SkillCatalogSifsStore::from_vfs(vfs.clone())
            .write_skill_version(write)
            .expect("persist skill fixture");
        vfs.write(&skill_path, b"not-a-sinomo-object")
            .expect("corrupt referenced package");

        let error = match tui::app_build_chat_handler(&data_dir, true, Some(vfs)) {
            Ok(_) => panic!("GUI runtime must reject malformed referenced packages"),
            Err(error) => error,
        };

        assert!(!error.to_string().is_empty());
        assert!(!data_dir.root().join("dfs").exists());
    }

    #[test]
    fn agent_channel_bulk_loader_skips_only_typed_confirmed_absence() {
        let missing = sinomo_runtime::api::DaemonDfsBrowserFileRead::not_found(
            "/services/sinomo-agent/channels/missing.json",
        )
        .into_result()
        .expect_err("typed missing item")
        .to_string();
        assert!(
            channel_optional_bulk_read_bytes(
                "/services/sinomo-agent/channels/missing.json",
                Err(missing),
            )
            .expect("confirmed absence is optional")
            .is_none()
        );

        let unavailable = sinomo_runtime::api::DaemonDfsBrowserFileRead::content_unavailable(
            "/services/sinomo-agent/channels/present.json",
            "content source returned not found: authoritative metadata still exists",
        )
        .into_result()
        .expect_err("typed unavailable item")
        .to_string();
        assert!(
            channel_optional_bulk_read_bytes(
                "/services/sinomo-agent/channels/present.json",
                Err(unavailable),
            )
            .is_err(),
            "metadata-present content unavailability must fail the hydration"
        );
    }

    #[test]
    fn remote_sifs_reads_retry_only_transient_browser_admission_failures() {
        let mut attempts = 0;
        let value = retry_transient_sifs_browser_read_with_policy(
            || {
                attempts += 1;
                if attempts < 3 {
                    Err(
                        "DFS browser service is busy after waiting 100ms; retry the read"
                            .to_string(),
                    )
                } else {
                    Ok("ready")
                }
            },
            Duration::from_millis(100),
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .expect("an admitted retry should recover");
        assert_eq!(value, "ready");
        assert_eq!(attempts, 3);

        let mut saturation_attempts = 0;
        let value = retry_transient_sifs_browser_read_with_policy(
            || {
                saturation_attempts += 1;
                if saturation_attempts == 1 {
                    Err(
                        "object rpc route /daemon/dfs/browser/status is saturated: max 4 in-flight requests"
                            .to_string(),
                    )
                } else {
                    Ok("admitted")
                }
            },
            Duration::from_millis(100),
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .expect("route saturation should be retried");
        assert_eq!(value, "admitted");
        assert_eq!(saturation_attempts, 2);

        let mut unrelated_saturation_attempts = 0;
        let error = retry_transient_sifs_browser_read_with_policy(
            || {
                unrelated_saturation_attempts += 1;
                Err::<(), _>(
                    "object rpc route /daemon/agent/session is saturated: max 4 in-flight requests"
                        .to_string(),
                )
            },
            Duration::from_millis(100),
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .expect_err("unrelated route saturation must not be retried");
        assert!(error.contains("/daemon/agent/session"));
        assert_eq!(unrelated_saturation_attempts, 1);

        let mut terminal_attempts = 0;
        let error = retry_transient_sifs_browser_read_with_policy(
            || {
                terminal_attempts += 1;
                Err::<(), _>("device authorization was denied".to_string())
            },
            Duration::from_millis(100),
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .expect_err("non-busy failures must not be retried");
        assert_eq!(error, "device authorization was denied");
        assert_eq!(terminal_attempts, 1);
    }

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

    struct RejectMessageChannelStore {
        inner: SifsAgentChannelStore,
    }

    impl AgentChannelStore for RejectMessageChannelStore {
        fn save_channel(&self, channel: &AgentChannel) -> Result<(), AgentChannelStoreError> {
            self.inner.save_channel(channel)
        }

        fn load_channel(
            &self,
            channel_id: &SessionId,
        ) -> Result<Option<AgentChannel>, AgentChannelStoreError> {
            self.inner.load_channel(channel_id)
        }

        fn load_message(
            &self,
            channel_id: &SessionId,
            message_id: &TaskId,
        ) -> Result<Option<AgentChannelMessage>, AgentChannelStoreError> {
            self.inner.load_message(channel_id, message_id)
        }

        fn list_channels(&self) -> Result<Vec<AgentChannel>, AgentChannelStoreError> {
            self.inner.list_channels()
        }

        fn append_message(
            &self,
            channel_id: &SessionId,
            message: &AgentChannelMessage,
        ) -> Result<(), AgentChannelStoreError> {
            Err(AgentChannelStoreError::Io {
                path: format!("simulated://{channel_id}/{}", message.message_id),
                detail: "simulated stable history outage".to_string(),
            })
        }

        fn append_turn(
            &self,
            channel_id: &SessionId,
            turn: &AgentChannelTurn,
        ) -> Result<(), AgentChannelStoreError> {
            self.inner.append_turn(channel_id, turn)
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
            _input: AgentChannelSpeakInput<'_>,
            _on_event: &mut dyn FnMut(AgentChannelRuntimeEvent),
        ) -> Result<AgentChannelSpeech, AgentChannelExecutorError> {
            Ok(AgentChannelSpeech::text(self.0))
        }
    }

    fn chat_event_label(event: ChatStreamEvent) -> String {
        match event {
            ChatStreamEvent::Token(token) => format!("token:{token}"),
            ChatStreamEvent::AssistantTextDelta { step_index, text } => {
                format!("assistant-delta:{step_index}:{text}")
            }
            ChatStreamEvent::ReasoningDelta { step_index, delta } => {
                format!("reasoning-delta:{step_index}:{delta}")
            }
            ChatStreamEvent::ReasoningSummary {
                step_index,
                summary,
            } => {
                format!("reasoning:{step_index}:{summary}")
            }
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
            ChatStreamEvent::ToolArtifacts(_) => "artifacts".into(),
            ChatStreamEvent::OpStarted { .. } => "op-started".into(),
            ChatStreamEvent::SpeakerStarted(speaker) => format!("speaker:{}", speaker.agent_name),
            ChatStreamEvent::ToolOutput(_) => "tool-output".into(),
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        assert_eq!(events, vec!["assistant-delta:0:hello", "token: world"]);
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
    fn model_reasoning_and_step_text_keep_their_stream_kinds() {
        let mut stream_state = ChatStreamState::default();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));

        forward_model_event(
            AgentModelEvent::AssistantReasoningDelta {
                step_index: 2,
                delta: "Inspecting ".to_string(),
            },
            &mut stream_state,
            &mut on_event,
        );
        forward_model_event(
            AgentModelEvent::AssistantReasoningSummary {
                step_index: 2,
                summary: "Context checked".to_string(),
            },
            &mut stream_state,
            &mut on_event,
        );
        forward_model_event(
            AgentModelEvent::AssistantTextDelta {
                step_index: 2,
                delta: "I will read the file.".to_string(),
            },
            &mut stream_state,
            &mut on_event,
        );

        assert_eq!(
            events,
            vec![
                "reasoning-delta:2:Inspecting ",
                "reasoning:2:Context checked",
                "assistant-delta:2:I will read the file.",
            ]
        );
        assert_eq!(stream_state.visible_text, "I will read the file.");
    }

    #[test]
    fn final_decision_does_not_repeat_last_model_step_after_intermediate_output() {
        let mut stream_state = ChatStreamState::default();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));

        for (step_index, delta) in [(0, "intermediate"), (1, "final answer")] {
            forward_model_event(
                AgentModelEvent::AssistantTextDelta {
                    step_index,
                    delta: delta.to_string(),
                },
                &mut stream_state,
                &mut on_event,
            );
        }
        stream_state.emit_final_text("final answer", &mut on_event);

        assert_eq!(
            events,
            vec![
                "assistant-delta:0:intermediate",
                "assistant-delta:1:final answer",
            ]
        );
        assert_eq!(stream_state.visible_text, "final answer");
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
        assert_eq!(
            events,
            vec!["op-started", "notice:agent task started: start_turn"]
        );
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
    fn roaming_session_reconciliation_rejects_identity_and_ignores_legacy_workspace_drift() {
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
        let reconciled = select_resumable_agent_session(Some(local), Some(roaming))
            .expect("legacy workspace drift is not session identity drift")
            .expect("reconciled session");
        assert_eq!(
            reconciled.workspace_binding,
            Some(sinomo_agent::WorkspaceRef::local_path("/workspace/b"))
        );
    }

    #[test]
    fn primary_checkpoint_rejects_participant_agent_identity_drift() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-checkpoint-agent-drift");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        roaming.workspace_binding = Some(sinomo_agent::WorkspaceRef::local_path("/workspace/b"));
        let mut local = roaming.clone();
        local.workspace_binding = Some(sinomo_agent::WorkspaceRef::local_path("/workspace/a"));
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
        assert_eq!(selected.workspace_binding, local.workspace_binding);

        roaming
            .follow_up_queue
            .push(sinomo_agent::AgentFollowUpMessage::new("roaming follow-up"));
        roaming.state_revision = 3;
        let selected = select_resumable_agent_session(Some(local), Some(roaming.clone()))
            .expect("select roaming newer control state")
            .expect("session");
        assert_eq!(selected.follow_up_queue, roaming.follow_up_queue);
        assert_eq!(selected.workspace_binding, roaming.workspace_binding);

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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let local_node_id = native_fixture_local_node_id(&data_dir);
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
            .sandbox_node_id = Some(local_node_id.to_string());
        assert_ne!(participant_session_id, channel_id);
        store.save_channel(&channel).expect("save channel");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume explicit channel");
        assert_actual_local_sandbox(&chat, &data_dir, &local_node_id);

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
            Some(local_node_id.as_str())
        );
    }

    #[test]
    fn app_chat_resume_group_channel_does_not_add_or_reenable_process_default() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-membership-resume");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
            chat.use_injected_authoritative_agent_catalog_for_test();
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
    fn app_chat_resume_group_outer_session_does_not_claim_participant_checkpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-outer-session-resume");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Durable team");
        channel.ensure_user(AgentChannelUser::app_local());
        let participant_id = channel.ensure_participant("main-agent", "main-agent");
        let participant_session_id = channel
            .participant(&participant_id)
            .expect("main participant")
            .agent_session_id
            .clone();
        let durable_participants = channel.participants.clone();
        store.save_channel(&channel).expect("save channel");

        let outer_session = AgentSession::new(channel_id.clone(), AgentRole::Main)
            .with_agent_definition_key("main-agent");
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&outer_session)
            .expect("save outer channel session");

        // A completed participant turn publishes its own roaming checkpoint.
        // The channel's outer runtime is a different session and must not try
        // to reconcile that checkpoint with its local outer-session record.
        let participant_session =
            AgentSession::new(participant_session_id.clone(), AgentRole::Main)
                .with_agent_definition_key("main-agent");
        let checkpoints = SifsAgentSessionCheckpointStore::open(&data_dir).expect("checkpoints");
        checkpoints
            .publish(
                &channel,
                channel.participant(&participant_id).expect("participant"),
                &participant_session,
            )
            .expect("publish participant checkpoint");

        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume group outer session");

        assert_ne!(participant_session_id, channel_id);
        assert_eq!(chat.channel.participants, durable_participants);
        assert_eq!(
            chat.runtime
                .handler()
                .agent_session_snapshot()
                .expect("outer runtime snapshot")
                .expect("outer runtime")
                .session_id,
            channel_id
        );
        assert_eq!(
            checkpoints
                .load_latest(
                    &channel,
                    channel.participant(&participant_id).expect("participant"),
                )
                .expect("retained participant checkpoint"),
            Some(participant_session.clone()),
            "resuming the outer runtime must not replace its participant's checkpoint"
        );
        assert_eq!(
            chat.load_participant_session(
                channel.participant(&participant_id).expect("participant"),
            )
            .expect("restore participant through its own session owner"),
            Some(participant_session)
        );
        assert!(
            chat.take_resume_sync_warnings().is_empty(),
            "an outer channel session must not be published as a participant checkpoint"
        );
    }

    #[test]
    fn app_chat_group_tool_runtime_update_preserves_membership_and_session_identity() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-tool-runtime-identity");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let local_node_id = native_fixture_local_node_id(&data_dir);
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
        chat.use_injected_authoritative_agent_catalog_for_test();
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume durable group channel");
        chat.set_tool_runtime_node(Some(local_node_id.to_string()))
            .expect("update selected participant tool runtime");
        assert_actual_local_sandbox(&chat, &data_dir, &local_node_id);

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
            Some(local_node_id.as_str())
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
            Some(local_node_id.as_str())
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
        // Writable workspaces must be outside the protected Sinomo data root.
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let local_node_id = native_fixture_local_node_id(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");

        for all_disabled in [false, true] {
            let channel_id = SessionId::generate();
            let workspace = root.join(format!("workspace-{channel_id}"));
            fs::create_dir_all(&workspace).expect("create group workspace");
            let workspace = fs::canonicalize(workspace).expect("canonical group workspace");
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
            chat.set_tool_runtime(ChatToolRuntimeSelection {
                sandbox_node_id: Some(local_node_id.to_string()),
                workspace_root: Some(workspace.clone()),
            })
            .expect("update outer runtime without a member");
            assert_actual_local_sandbox(&chat, &data_dir, &local_node_id);

            assert_eq!(chat.channel.participants, durable_participants);
            let saved = store
                .load_channel(&channel_id)
                .expect("reload durable channel")
                .expect("durable channel");
            assert_eq!(
                saved.participants, durable_participants,
                "tool runtime selection must not create or re-enable group membership"
            );
            assert_eq!(
                saved
                    .primary_workspace()
                    .and_then(|workspace| workspace.local_root.as_deref()),
                Some(workspace.as_path()),
                "workspace authority persists without an exact group member"
            );
            chat.clear_workspaces()
                .expect("revoke group workspace without a member");
            let cleared = store
                .load_channel(&channel_id)
                .expect("reload cleared durable channel")
                .expect("cleared durable channel");
            assert!(cleared.workspaces.is_empty());
            assert!(cleared.has_authoritative_workspace_projection());
            assert_eq!(cleared.participants, durable_participants);
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        chat.use_injected_authoritative_agent_catalog_for_test();
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
    fn accepted_control_returns_warning_when_stable_projection_fails() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-control-history-warning");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let channel_store = RejectMessageChannelStore {
            inner: SifsAgentChannelStore::open(&data_dir).expect("open channel store"),
        };
        let checkpoint_store =
            SifsAgentSessionCheckpointStore::open(&data_dir).expect("checkpoint store");
        let agent_catalog_vfs =
            sinomo_runtime::vfs::mounted_sifs(Some(data_dir.root())).expect("agent catalog VFS");
        let chat = AgentChat::with_runtime_control_registry_and_stores(
            data_dir,
            false,
            AgentRuntimeControlRegistry::new(),
            agent_catalog_vfs,
            false,
            Box::new(channel_store),
            checkpoint_store,
        )
        .expect("chat runtime");
        chat.save_channel().expect("save initial channel");

        let status = chat
            .steer("accepted despite history outage")
            .expect("accepted control remains successful");
        assert!(status.accepted);
        assert_eq!(
            status.warning.as_deref(),
            Some("control accepted, but durable chat persistence is unavailable")
        );
        let session = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("session snapshot")
            .expect("session");
        assert_eq!(session.steering_queue.len(), 1);
        assert_eq!(
            session.steering_queue[0].message,
            "accepted despite history outage"
        );
    }

    #[test]
    fn app_chat_group_controls_publish_exact_member_checkpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-control-member");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("create coding workspace");
        let workspace = fs::canonicalize(workspace).expect("canonical coding workspace");
        let session_id = SessionId::generate();

        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let mut channel = AgentChannel::new(session_id.clone(), "Coding chat");
        channel.workspace_projection_version = 0;
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
    fn app_chat_resume_uses_conversation_workspace_over_legacy_session_shadow() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-workspace-authority");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let conversation_workspace = root.join("conversation-workspace");
        let stale_session_workspace = root.join("stale-session-workspace");
        fs::create_dir_all(&conversation_workspace).expect("create conversation workspace");
        fs::create_dir_all(&stale_session_workspace).expect("create stale session workspace");
        let conversation_workspace =
            fs::canonicalize(conversation_workspace).expect("canonical conversation workspace");
        let stale_session_workspace =
            fs::canonicalize(stale_session_workspace).expect("canonical stale session workspace");
        let session_id = SessionId::generate();

        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let mut channel = AgentChannel::new(session_id.clone(), "Coding chat");
        channel.ensure_user(AgentChannelUser::app_local());
        channel.add_workspace(sinomo_agent::WorkspaceRef::local_path(
            conversation_workspace.clone(),
        ));
        let participant_id = channel.ensure_participant("coding-agent", "Coding agent");
        channel
            .participant_mut(&participant_id)
            .expect("coding participant")
            .agent_session_id = session_id.clone();
        channel_store.save_channel(&channel).expect("save channel");

        let mut persisted = AgentSession::new(session_id.clone(), AgentRole::Deliberation)
            .with_agent_definition_key("coding-agent");
        persisted.workspace_binding = Some(sinomo_agent::WorkspaceRef::local_path(
            stale_session_workspace,
        ));
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&persisted)
            .expect("save coding session");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.resume_session(&session_id.to_string(), &[])
            .expect("resume coding session");

        assert_eq!(
            chat.tool_runtime().workspace_root.as_deref(),
            Some(conversation_workspace.as_path())
        );
        assert_eq!(
            chat.workspaces()
                .first()
                .and_then(|workspace| workspace.local_root.as_deref()),
            Some(conversation_workspace.as_path())
        );
        let resumed = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("runtime attached");
        assert_eq!(
            resumed.workspace_binding,
            Some(sinomo_agent::WorkspaceRef::local_path(
                conversation_workspace.clone()
            ))
        );
        let checkpoint = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&session_id)
            .expect("load session checkpoint")
            .expect("session checkpoint");
        assert_eq!(checkpoint.workspace_binding, resumed.workspace_binding);
        let participant = chat
            .primary_participant()
            .expect("primary participant")
            .clone();
        let roaming_checkpoint = chat
            .session_checkpoint_store
            .load_latest(&chat.channel, &participant)
            .expect("load roaming checkpoint")
            .expect("roaming checkpoint");
        assert_eq!(
            roaming_checkpoint.workspace_binding, resumed.workspace_binding,
            "resume publishes the post-projection compatibility shadow"
        );
    }

    #[test]
    fn app_chat_resume_preserves_authoritative_empty_workspace_revocation() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-empty-workspace-authority");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let session_id = SessionId::generate();
        let channel_store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let mut channel = AgentChannel::new(session_id.clone(), "Main chat");
        let participant_id = channel.ensure_participant("main-agent", "Main agent");
        channel
            .participant_mut(&participant_id)
            .expect("main participant")
            .agent_session_id = session_id.clone();
        channel_store.save_channel(&channel).expect("save channel");

        let mut persisted = AgentSession::new(session_id.clone(), AgentRole::Main)
            .with_agent_definition_key("main-agent");
        persisted.workspace_binding = Some(sinomo_agent::WorkspaceRef::local_path(
            "/stale/legacy/workspace",
        ));
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&persisted)
            .expect("save stale session shadow");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.resume_session(&session_id.to_string(), &[])
            .expect("resume authoritative empty channel");

        assert!(chat.workspaces().is_empty());
        let resumed = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("runtime attached");
        assert_eq!(resumed.workspace_binding, None);
        let local_checkpoint = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&session_id)
            .expect("load local checkpoint")
            .expect("local checkpoint");
        assert_eq!(local_checkpoint.workspace_binding, None);
        let participant = chat
            .primary_participant()
            .expect("primary participant")
            .clone();
        let roaming_checkpoint = chat
            .session_checkpoint_store
            .load_latest(&chat.channel, &participant)
            .expect("load roaming checkpoint")
            .expect("roaming checkpoint");
        assert_eq!(roaming_checkpoint.workspace_binding, None);
    }

    #[test]
    fn app_chat_resume_configuration_failure_restores_previous_recall_authority() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-resume-authority-rollback");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        inject_native_notes_for_session_rollback_unit(&chat, &data_dir);
        let previous_interaction_session = chat.runtime.session().clone();
        let previous_channel = chat.channel.clone();
        let previous_tool_runtime = chat.tool_runtime.clone();
        let memory_path = "resume/rollback-authority.md";
        let main_write = tui::execute_tui_workspace_tool(
            chat.runtime.handler(),
            &previous_interaction_session,
            0,
            "memory_write",
            serde_json::json!({
                "path": memory_path,
                "markdown": "main authority survives failed resume",
            }),
        );
        assert_eq!(main_write.status, sinomo_agent::AgentToolStatus::Succeeded);

        let target_session_id = SessionId::generate();
        let mut target_channel = AgentChannel::with_default_title(target_session_id.clone());
        let participant_id = target_channel.ensure_participant("coding-agent", "coding-agent");
        let participant = target_channel
            .participant_mut(&participant_id)
            .expect("coding participant");
        participant.agent_session_id = target_session_id.clone();
        // An explicitly present but empty node id reaches configuration only
        // after the target agent/session has been restored, then fails parsing.
        participant.tool_runtime.sandbox_node_id = Some(String::new());
        chat.channel_store
            .save_channel(&target_channel)
            .expect("save target channel");
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(
                &AgentSession::new(target_session_id.clone(), AgentRole::Deliberation)
                    .with_agent_definition_key("coding-agent"),
            )
            .expect("save target coding session");

        let error = chat
            .resume_session(target_session_id.as_str(), &[])
            .expect_err("invalid target tool runtime must reject resume");

        assert!(
            error
                .to_string()
                .contains("configure resumed chat tool runtime failed"),
            "unexpected resume error: {error}"
        );
        assert_eq!(chat.runtime_info().agent, "main-agent");
        assert_eq!(chat.runtime.session(), &previous_interaction_session);
        assert_eq!(chat.channel, previous_channel);
        assert_eq!(chat.tool_runtime, previous_tool_runtime);
        let restored = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("restored main session");
        assert_eq!(restored.agent_definition_key.as_deref(), Some("main-agent"));
        assert_eq!(restored.session_id, previous_interaction_session.session_id);
        let main_read = tui::execute_tui_workspace_tool(
            chat.runtime.handler(),
            &previous_interaction_session,
            1,
            "memory_read",
            serde_json::json!({ "path": memory_path }),
        );
        assert_eq!(
            main_read.output["markdown"],
            "main authority survives failed resume"
        );
    }

    #[test]
    fn app_chat_resume_validation_failure_restores_outer_and_canonical_runtime() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-resume-validation-rollback");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        inject_native_notes_for_session_rollback_unit(&chat, &data_dir);
        let previous_interaction_session = chat.runtime.session().clone();
        let previous_channel = chat.channel.clone();
        let previous_tool_runtime = chat.tool_runtime.clone();
        let memory_path = "resume/validation-rollback.md";
        let main_write = tui::execute_tui_workspace_tool(
            chat.runtime.handler(),
            &previous_interaction_session,
            0,
            "memory_write",
            serde_json::json!({
                "path": memory_path,
                "markdown": "main authority survives validation failure",
            }),
        );
        assert_eq!(main_write.status, sinomo_agent::AgentToolStatus::Succeeded);

        let target_session_id = SessionId::generate();
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&AgentSession::new(
                target_session_id.clone(),
                AgentRole::Deliberation,
            ))
            .expect("save identity-less target session");

        chat.resume_session(target_session_id.as_str(), &[])
            .expect_err("identity-less canonical session must reject resume");

        assert_eq!(chat.runtime_info().agent, "main-agent");
        assert_eq!(chat.runtime.session(), &previous_interaction_session);
        assert_eq!(chat.channel, previous_channel);
        assert_eq!(chat.tool_runtime, previous_tool_runtime);
        let restored = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("restored main session");
        assert_eq!(restored.session_id, previous_interaction_session.session_id);
        assert_eq!(restored.agent_definition_key.as_deref(), Some("main-agent"));
        let registry = chat.runtime_control_registry();
        assert!(
            registry
                .contains_session(&previous_interaction_session.session_id)
                .expect("previous route")
        );
        assert!(
            !registry
                .contains_session(&target_session_id)
                .expect("target route")
        );
        let main_read = tui::execute_tui_workspace_tool(
            chat.runtime.handler(),
            &previous_interaction_session,
            1,
            "memory_read",
            serde_json::json!({ "path": memory_path }),
        );
        assert_eq!(
            main_read.output["markdown"],
            "main authority survives validation failure"
        );
    }

    #[test]
    fn app_chat_runs_turns_after_switching_a_started_chat_between_agents() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-switch-started-agent");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        let session_id = chat.session_id();
        chat.prepare_primary_participant()
            .expect("prepare main-agent participant");
        // The main agent's session was checkpointed, as after a turn.
        chat.persist_primary_session_checkpoint()
            .expect("checkpoint main-agent session");

        for agent in ["coding-agent", "main-agent"] {
            chat.set_agent(agent).expect("switch agent");
            let primary = chat
                .prepare_primary_participant()
                .expect("prepare switched participant");
            let sessions = chat
                .participant_sessions(primary.as_ref())
                .expect("a turn resolves the participants' sessions after the switch");
            assert_eq!(
                sessions
                    .iter()
                    .map(|(_, session)| session.agent_definition_key.as_deref())
                    .collect::<Vec<_>>(),
                vec![Some(agent)],
                "only the enabled agent takes part in the chat's turns"
            );
            assert_eq!(sessions[0].1.session_id.to_string(), session_id);
        }
    }

    #[test]
    fn app_chat_reopens_the_enabled_agent_after_a_direct_switch() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-switched-agent-resume");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        let _native_data_dir_a = crate::native_test_fixture::NativeCliFixture::start(&data_dir_a);
        let data_dir_b = SinomoDataDir::new(&root_b);
        let _native_data_dir_b = crate::native_test_fixture::NativeCliFixture::start(&data_dir_b);
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
        let working_state = sinomo_agent::AgentWorkingState {
            active_objective: Some("finish roaming verification".to_string()),
            files_modified: vec!["src/roaming.rs".to_string()],
            ..sinomo_agent::AgentWorkingState::default()
        };
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

        let agent_catalog_vfs_a = sinomo_runtime::vfs::mounted_sifs(Some(data_dir_a.root()))
            .expect("device A agent catalog VFS");
        let mut chat_a = AgentChat::with_runtime_control_registry_and_stores(
            data_dir_a.clone(),
            false,
            AgentRuntimeControlRegistry::new(),
            agent_catalog_vfs_a,
            false,
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
        let agent_catalog_vfs_b = sinomo_runtime::vfs::mounted_sifs(Some(data_dir_b.root()))
            .expect("device B agent catalog VFS");
        let mut chat_b = AgentChat::with_runtime_control_registry_and_stores(
            data_dir_b.clone(),
            false,
            AgentRuntimeControlRegistry::new(),
            agent_catalog_vfs_b,
            false,
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
    fn app_chat_rejects_agent_placement_without_local_node_identity() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-agent-tool-runtime");
        let data_dir = SinomoDataDir::new(root.join("uninitialized-caller"));
        let provider_data = SinomoDataDir::new(root.join("actual-native-provider"));
        let _provider = crate::native_test_fixture::NativeCliFixture::start(&provider_data);
        let definition =
            AgentDefinition::custom("worker-agent", AgentRole::Main, "Use the worker sandbox.")
                .with_sandbox_runtime_node_id("worker-node");
        let definition_store =
            sinomo_agent::AgentDefinitionSifsStore::open(Some(provider_data.root()))
                .expect("open agent definition store");
        sinomo_agent::AgentDefinitionStore::save_definition(&definition_store, &definition)
            .expect("save agent definition");

        let mut chat = chat_with_native_stores_without_local_identity(data_dir, &provider_data);
        chat.use_injected_authoritative_agent_catalog_for_test();
        let error = chat
            .set_agent("worker-agent")
            .expect_err("placement cannot be silently ignored");
        assert!(
            error
                .to_string()
                .contains("local node identity is unavailable")
        );
    }

    #[test]
    fn app_chat_persists_conversation_workspace_and_default_sandbox_attachment() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-workspace-binding");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        assert_eq!(chat.workspaces().len(), 1);
        assert_eq!(
            chat.workspaces()[0].local_root.as_deref(),
            Some(canonical_workspace.as_path()),
            "the chat owns the authoritative ordered workspace grant"
        );
        let expected_sandbox_id =
            sinomo_agent::local_workspace::agent_sandbox_id_for_session(&session.session_id);
        assert_eq!(
            session
                .sandbox_attachment
                .as_ref()
                .map(|attachment| attachment.sandbox_id.as_str()),
            Some(expected_sandbox_id.as_str())
        );
        assert!(
            sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(
                data_dir.root().join("sandbox/runtime")
            )
            .load(&expected_sandbox_id)
            .is_ok(),
            "normal chat setup provisions the persistent sandbox before tools run"
        );
        let persisted = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&session.session_id)
            .expect("load session")
            .expect("persisted session");
        assert_eq!(persisted.workspace_binding, session.workspace_binding);
        assert_eq!(persisted.sandbox_attachment, session.sandbox_attachment);
    }

    #[test]
    fn app_chat_workspace_tools_execute_through_the_attached_simple_sandbox() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-simple-sandbox-workspace");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("create workspace");
        let workspace = fs::canonicalize(workspace).expect("canonical workspace");
        let mut permissions = sinomo_runtime::node::LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        sinomo_runtime::node::LocalNodePermissionStore::new(data_dir.root())
            .save(&permissions)
            .expect("grant local command execution for the fixture");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.set_agent("coding-agent").expect("select coding agent");
        chat.replace_workspace_root(Some(workspace.clone()))
            .expect("select workspace");
        let session = chat.runtime.session().clone();

        let structured_write = tui::execute_tui_workspace_tool(
            chat.runtime.handler(),
            &session,
            0,
            "write_file",
            serde_json::json!({
                "path": "structured.txt",
                "content": "structured workspace write\n",
            }),
        );
        assert_eq!(
            structured_write.status,
            sinomo_agent::AgentToolStatus::Succeeded
        );

        let command = if cfg!(windows) {
            "echo sandbox-ok>command.txt"
        } else {
            "printf sandbox-ok > command.txt"
        };
        let command_write = tui::execute_tui_workspace_tool(
            chat.runtime.handler(),
            &session,
            1,
            "run_command",
            serde_json::json!({ "command": command, "timeout_ms": 5_000 }),
        );
        assert_eq!(
            command_write.status,
            sinomo_agent::AgentToolStatus::Succeeded,
            "simple-sandbox command failed: {}",
            command_write.summary
        );
        assert_eq!(
            fs::read_to_string(workspace.join("structured.txt"))
                .expect("structured workspace output"),
            "structured workspace write\n"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("command.txt"))
                .expect("sandbox command workspace output")
                .trim(),
            "sandbox-ok"
        );
        assert_eq!(
            command_write
                .evidence
                .metadata
                .pointer("/sandbox/runtime")
                .and_then(serde_json::Value::as_str),
            Some("simple_sandbox")
        );
    }

    #[test]
    fn app_chat_explicit_workspace_clear_revokes_durable_and_runtime_authority() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-workspace-clear");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let workspace = root.join("selected-directory");
        fs::create_dir_all(&workspace).expect("create selected directory");
        let workspace = fs::canonicalize(workspace).expect("canonical workspace");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.set_agent("coding-agent").expect("select coding agent");
        chat.set_tool_runtime(ChatToolRuntimeSelection {
            workspace_root: Some(workspace.clone()),
            ..ChatToolRuntimeSelection::default()
        })
        .expect("bind workspace");
        let preserved = chat
            .set_tool_runtime(ChatToolRuntimeSelection::default())
            .expect("node-only style update preserves workspace authority");
        assert_eq!(
            preserved.workspace_root.as_deref(),
            Some(workspace.as_path())
        );
        let attached = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("attached session");
        let sandbox_id =
            sinomo_agent::local_workspace::agent_sandbox_id_for_session(&attached.session_id);
        assert_eq!(
            attached
                .sandbox_attachment
                .as_ref()
                .expect("sandbox attachment")
                .sandbox_id,
            sandbox_id.to_string()
        );

        let cleared = chat.clear_workspaces().expect("clear workspace authority");
        assert_eq!(cleared.workspace_root, None);
        assert!(chat.workspaces().is_empty());
        assert!(chat.channel.has_authoritative_workspace_projection());
        chat.tool_runtime.workspace_root = Some(workspace);
        let node_only = chat
            .set_tool_runtime_node(None)
            .expect("node-only update ignores stale cached workspace");
        assert_eq!(node_only.workspace_root, None);
        assert!(chat.workspaces().is_empty());

        let session = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("attached session");
        assert_eq!(session.workspace_binding, None);
        assert_eq!(session.sandbox_attachment, attached.sandbox_attachment);
        let persisted = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&session.session_id)
            .expect("load session")
            .expect("persisted session");
        assert_eq!(persisted.workspace_binding, None);
        let saved_channel = chat
            .channel_store
            .load_channel(&chat.channel.channel_id)
            .expect("load channel")
            .expect("saved channel");
        assert!(saved_channel.workspaces.is_empty());
        assert!(saved_channel.has_authoritative_workspace_projection());

        let (manifest, _) = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(
            data_dir.root().join("sandbox/runtime"),
        )
        .load(&sandbox_id)
        .expect("load attached sandbox");
        assert!(manifest.mounts.iter().all(|mount| {
            mount.mount_path.as_str() != "/work"
                && !mount.mount_path.as_str().starts_with("/workspaces/")
        }));
    }

    #[test]
    fn app_chat_folder_selection_replaces_the_complete_workspace_grant() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-workspace-replace");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let first = root.join("first-workspace");
        let second = root.join("second-workspace");
        fs::create_dir_all(&first).expect("create first workspace");
        fs::create_dir_all(&second).expect("create second workspace");
        let first = fs::canonicalize(first).expect("canonical first workspace");
        let second = fs::canonicalize(second).expect("canonical second workspace");

        let mut chat = AgentChat::new(data_dir, false).expect("chat runtime");
        chat.set_agent("coding-agent").expect("select coding agent");
        let selected = chat
            .replace_workspace_root(Some(first.clone()))
            .expect("select first workspace");
        assert_eq!(selected.workspace_root.as_deref(), Some(first.as_path()));
        assert_eq!(chat.workspaces().len(), 1);

        let replaced = chat
            .replace_workspace_root(Some(second.clone()))
            .expect("replace workspace");
        assert_eq!(replaced.workspace_root.as_deref(), Some(second.as_path()));
        assert_eq!(chat.workspaces().len(), 1);
        assert_eq!(
            chat.workspaces()[0].local_root.as_deref(),
            Some(second.as_path()),
            "folder selection must not silently retain the previous workspace as an additional grant"
        );

        let revoked = chat.replace_workspace_root(None).expect("revoke workspace");
        assert_eq!(revoked.workspace_root, None);
        assert!(chat.workspaces().is_empty());
        assert!(chat.channel.has_authoritative_workspace_projection());
    }

    #[test]
    fn rejected_remote_sandbox_selection_does_not_mutate_session_or_channel_authority() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-remote-sandbox-rollback");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let workspace = root.join("selected-directory");
        fs::create_dir_all(&workspace).expect("create selected directory");
        let workspace = fs::canonicalize(workspace).expect("canonical workspace");
        let remote_node = NodeId::from_legacy_named("remote-sandbox-node").expect("remote node");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.set_agent("coding-agent").expect("select coding agent");
        ensure_single_node_cluster(&data_dir, "Local node").expect("initialize local cluster");
        let cluster = read_cli_cluster_state(&ClusterPaths::new(&data_dir)).expect("cluster state");
        assert_ne!(remote_node, cluster.local_node_id);
        let session_before = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("attached session");
        let channel_before = chat.channel.clone();

        let error = chat
            .set_tool_runtime(ChatToolRuntimeSelection {
                sandbox_node_id: Some(remote_node.to_string()),
                workspace_root: Some(workspace),
            })
            .expect_err("remote provisioning needs the daemon's sandbox service");
        assert!(
            error.to_string().contains("requires a running daemon"),
            "{error}"
        );

        let session_after = chat
            .runtime
            .handler()
            .agent_session_snapshot()
            .expect("runtime snapshot")
            .expect("attached session");
        assert_eq!(session_after, session_before);
        assert_eq!(chat.channel, channel_before);
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
    fn app_chat_channel_rejects_unenforceable_agent_tool_runtime_default() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-agent-tool-runtime");
        let data_dir = SinomoDataDir::new(root.join("uninitialized-caller"));
        let provider_data = SinomoDataDir::new(root.join("actual-native-provider"));
        let _provider = crate::native_test_fixture::NativeCliFixture::start(&provider_data);
        let definition =
            AgentDefinition::custom("worker-agent", AgentRole::Main, "Use the worker sandbox.")
                .with_sandbox_runtime_node_id("worker-node");
        let definition_store =
            sinomo_agent::AgentDefinitionSifsStore::open(Some(provider_data.root()))
                .expect("open agent definition store");
        sinomo_agent::AgentDefinitionStore::save_definition(&definition_store, &definition)
            .expect("save agent definition");

        let store = SifsAgentChannelStore::open(&provider_data).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Team channel");
        channel.ensure_user(AgentChannelUser::app_local());
        store.save_channel(&channel).expect("save channel");

        let mut chat = chat_with_native_stores_without_local_identity(data_dir, &provider_data);
        chat.use_injected_authoritative_agent_catalog_for_test();
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
        let error = chat
            .set_agent("worker-agent")
            .expect_err("channel participant placement cannot be silently ignored");
        assert!(
            error
                .to_string()
                .contains("local node identity is unavailable")
        );
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

        let saved = SifsAgentChannelStore::open(&provider_data)
            .expect("reopen channel store")
            .load_channel(&channel_id)
            .expect("load channel")
            .expect("channel saved");
        assert!(
            saved
                .enabled_participants_in_order()
                .into_iter()
                .all(|participant| participant.agent_key != "worker-agent")
        );
    }

    #[test]
    fn sifs_channel_store_can_use_an_isolated_test_root() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-isolated-root");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let isolated_root = "/services/cluster-tests/runs/test-run/agent-channels";
        let isolated = SifsAgentChannelStore::from_browser_at_root(
            SifsBrowser::open(&data_dir).expect("open isolated SIFS browser"),
            isolated_root,
        )
        .expect("open isolated channel store");
        let channel = AgentChannel::new_channel(SessionId::generate(), "Isolated test channel");

        isolated
            .save_channel(&channel)
            .expect("save isolated channel");

        let production = SifsAgentChannelStore::open(&data_dir).expect("open production store");
        assert!(
            production
                .list_channel_summaries()
                .expect("list production channels")
                .is_empty(),
            "isolated test channels must not enter the product channel catalog"
        );
        assert!(
            SifsBrowser::open(&data_dir)
                .expect("open verification browser")
                .read_file(&format!(
                    "{isolated_root}/{}/snapshot.json",
                    channel.channel_id
                ))
                .is_ok()
        );
        isolated
            .delete_isolated_test_run()
            .expect("remove isolated test run");
        assert!(
            SifsBrowser::open(&data_dir)
                .expect("open cleanup verification browser")
                .stat("/services/cluster-tests/runs/test-run")
                .is_err()
        );
    }

    #[test]
    fn sifs_channel_store_rejects_unsafe_roots() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-unsafe-root");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        for candidate in [
            "",
            "/",
            "relative",
            "//services/cluster-tests",
            "/services/../sinomo-agent/channels",
        ] {
            assert!(
                SifsAgentChannelStore::from_browser_at_root(
                    SifsBrowser::open(&data_dir).expect("open SIFS browser"),
                    candidate,
                )
                .is_err(),
                "unsafe root should be rejected: {candidate:?}"
            );
        }
    }

    #[test]
    fn sifs_channel_summaries_track_snapshot_metadata_without_index_cache() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-summaries");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        SifsBrowser::open(&data_dir)
            .expect("open SIFS")
            .create_dir_all(&store.channel_dir(&channel_id))
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
    fn channel_snapshot_merge_preserves_identity_and_resolves_concurrent_grants_least_privilege() {
        let channel_id = SessionId::generate();
        let base = time::OffsetDateTime::now_utc();
        let mut left = AgentChannel::new_channel(channel_id.clone(), "Shared channel");
        left.ensure_user(AgentChannelUser {
            user_id: "user.left".to_string(),
            display_name: "Left".to_string(),
            source: AgentChannelSource::app(),
        });
        left.ensure_participant("left-agent", "Left agent");
        left.add_workspace(sinomo_agent::WorkspaceRef::local_path("/work/left"));
        left.updated_at = base + time::Duration::seconds(2);

        let mut right = AgentChannel::new_channel(channel_id, "Shared channel");
        right.ensure_user(AgentChannelUser {
            user_id: "user.right".to_string(),
            display_name: "Right".to_string(),
            source: AgentChannelSource::app(),
        });
        right.ensure_participant("right-agent", "Right agent");
        right.add_workspace(sinomo_agent::WorkspaceRef::sifs("right", "/work/right"));
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
        assert!(
            merged.workspaces.is_empty(),
            "equal-revision divergent grants reconcile to their exact intersection"
        );
    }

    #[test]
    fn channel_snapshot_merge_does_not_resurrect_revoked_workspace_access() {
        let channel_id = SessionId::generate();
        let base = time::OffsetDateTime::now_utc();
        let mut original = AgentChannel::new_channel(channel_id, "Workspace revocation");
        original.add_workspace(sinomo_agent::WorkspaceRef::local_path("/work/primary"));
        original.add_workspace(sinomo_agent::WorkspaceRef::local_path("/work/revoked"));
        original.updated_at = base;
        let mut revoked = original.clone();
        assert!(revoked.remove_workspace("/work/revoked"));
        assert!(revoked.remove_workspace("/work/primary"));
        revoked.updated_at = base + time::Duration::seconds(1);

        let merged =
            merge_channel_snapshot(original.clone(), revoked).expect("merge workspace revocation");
        assert!(merged.workspaces.is_empty());
        assert!(merged.has_authoritative_workspace_projection());

        let merged_again = merge_channel_snapshot(merged, original.clone())
            .expect("merge stale snapshot after workspace revocation");
        assert!(merged_again.workspaces.is_empty());

        let mut stale_renamed = original.clone();
        stale_renamed.rename("Renamed from stale replica");
        stale_renamed.updated_at = base + time::Duration::seconds(10);
        let merged_after_stale_rename = merge_channel_snapshot(merged_again, stale_renamed)
            .expect("merge later unrelated stale update after workspace revocation");
        assert!(
            merged_after_stale_rename.workspaces.is_empty(),
            "unrelated newer channel metadata must not resurrect older workspace grants"
        );
    }

    #[test]
    fn channel_snapshot_merge_fences_repeated_edits_from_a_stale_grant_base() {
        let channel_id = SessionId::generate();
        let mut base = AgentChannel::new_channel(channel_id, "Causal workspace merge");
        let original = sinomo_agent::WorkspaceRef::local_path("/work/shared");
        assert!(base.add_workspace(original.clone()));

        let mut revoked = base.clone();
        assert!(revoked.clear_workspaces());

        let mut stale_edited = base.clone();
        let first_edit = original.clone().with_owner_node("stale-owner");
        assert!(stale_edited.add_workspace(first_edit));
        let second_edit = original
            .clone()
            .with_owner_node("stale-owner")
            .with_write_policy("stale-write-policy");
        assert!(stale_edited.add_workspace(second_edit.clone()));
        assert!(
            stale_edited.workspace_revision > revoked.workspace_revision,
            "the regression requires a stale branch with the larger local counter"
        );

        let merged = merge_channel_snapshot(revoked, stale_edited.clone())
            .expect("merge stale grant edits with revocation");
        assert!(
            merged.workspaces.is_empty(),
            "an incomparable stale edit cannot resurrect revoked access"
        );
        assert!(
            merged
                .workspace_causal_tokens
                .is_superset(&stale_edited.workspace_causal_tokens),
            "the safe merge retains the stale branch's causal context"
        );

        let mut explicitly_regranted = merged;
        assert!(explicitly_regranted.add_workspace(second_edit.clone()));
        let remerged = merge_channel_snapshot(explicitly_regranted, stale_edited)
            .expect("merge observed explicit regrant");
        assert_eq!(remerged.workspaces, vec![second_edit]);
    }

    #[test]
    fn channel_snapshot_merge_honors_explicit_empty_against_an_unseen_concurrent_grant() {
        let channel_id = SessionId::generate();
        let base = AgentChannel::new_channel(channel_id, "Concurrent empty projection");
        let workspace = sinomo_agent::WorkspaceRef::local_path("/work/concurrent");

        let mut added = base.clone();
        assert!(added.add_workspace(workspace));
        let mut explicitly_empty = base;
        assert!(explicitly_empty.clear_workspaces());
        assert!(
            explicitly_empty.workspaces.is_empty()
                && !explicitly_empty.workspace_causal_tokens.is_empty()
        );

        let merged = merge_channel_snapshot(explicitly_empty, added)
            .expect("merge explicit empty with unseen grant");
        assert!(
            merged.workspaces.is_empty(),
            "an explicit exact clear must fence a concurrent unseen grant"
        );
    }

    #[test]
    fn channel_snapshot_merge_treats_tokenless_authoritative_writer_as_mixed_version() {
        let channel_id = SessionId::generate();
        let mut base = AgentChannel::new_channel(channel_id, "Mixed writer workspace merge");
        let workspace = sinomo_agent::WorkspaceRef::local_path("/work/mixed-writer");
        assert!(base.add_workspace(workspace.clone()));

        // Simulate an older writer that understood authoritative channel
        // workspaces but dropped the newer causal fields while revoking access.
        let mut tokenless_revocation = base.clone();
        tokenless_revocation.workspaces.clear();
        tokenless_revocation.workspace_grant_revisions.clear();
        tokenless_revocation.workspace_causal_tokens.clear();
        tokenless_revocation.workspace_causal_heads.clear();
        tokenless_revocation.workspace_causal_version = 0;
        tokenless_revocation.workspace_revision =
            tokenless_revocation.workspace_revision.saturating_add(1);

        let mut stale_current_writer = base;
        assert!(
            stale_current_writer.add_workspace(
                workspace
                    .with_owner_node("stale-current-writer")
                    .with_write_policy("stale-current-write-policy"),
            )
        );

        let left_first =
            merge_channel_snapshot(tokenless_revocation.clone(), stale_current_writer.clone())
                .expect("merge tokenless revocation with current stale edit");
        let right_first = merge_channel_snapshot(stale_current_writer, tokenless_revocation)
            .expect("reverse mixed-version merge");
        assert!(left_first.workspaces.is_empty());
        assert!(right_first.workspaces.is_empty());
        assert_eq!(left_first.workspaces, right_first.workspaces);
        assert!(left_first.workspace_causal_version > 0);
    }

    #[test]
    fn channel_snapshot_workspace_register_is_associative_for_observed_regrant() {
        let channel_id = SessionId::generate();
        let workspace = sinomo_agent::WorkspaceRef::local_path("/work/associative");
        let mut base = AgentChannel::new_channel(channel_id, "Associative workspace merge");
        assert!(base.add_workspace(workspace.clone()));

        let mut revoked = base.clone();
        assert!(revoked.clear_workspaces());
        let mut concurrent_grant = base;
        assert!(concurrent_grant.replace_workspaces(vec![workspace.clone()]));
        let mut observed_regrant = revoked.clone();
        assert!(observed_regrant.add_workspace(workspace.clone()));

        let left_grouped = merge_channel_snapshot(
            merge_channel_snapshot(revoked.clone(), concurrent_grant.clone())
                .expect("merge revoke and concurrent grant"),
            observed_regrant.clone(),
        )
        .expect("merge observed regrant into conflict");
        let right_grouped = merge_channel_snapshot(
            revoked.clone(),
            merge_channel_snapshot(concurrent_grant.clone(), observed_regrant.clone())
                .expect("merge concurrent and observed grants"),
        )
        .expect("merge revoke into grant heads");
        let permuted = merge_channel_snapshot(
            merge_channel_snapshot(observed_regrant, revoked)
                .expect("merge observed regrant and revoke"),
            concurrent_grant,
        )
        .expect("merge final concurrent grant");

        assert_eq!(left_grouped.workspaces, vec![workspace.clone()]);
        assert_eq!(right_grouped.workspaces, vec![workspace.clone()]);
        assert_eq!(permuted.workspaces, vec![workspace]);
        assert_eq!(left_grouped.workspaces, right_grouped.workspaces);
        assert_eq!(left_grouped.workspaces, permuted.workspaces);
    }

    #[test]
    fn channel_snapshot_workspace_register_is_associative_for_cyclic_causal_claims() {
        let channel_id = SessionId::generate();
        let workspace = WorkspaceRef::local_path("/work/cyclic-causal-claims");
        let head_a = TaskId::generate();
        let head_b = TaskId::generate();
        let head_c = TaskId::generate();

        let mut left = AgentChannel::new_channel(channel_id.clone(), "Cyclic causal claims");
        left.workspace_causal_tokens = BTreeSet::from([head_a.clone(), head_b.clone()]);
        left.workspace_causal_heads = BTreeMap::from([(head_a.clone(), vec![workspace.clone()])]);
        left.workspaces = vec![workspace.clone()];

        let mut right = left.clone();
        right.workspace_causal_heads = BTreeMap::from([(head_b, Vec::new())]);
        right.workspaces.clear();

        let mut independent = left.clone();
        independent.workspace_causal_tokens = BTreeSet::from([head_c.clone()]);
        independent.workspace_causal_heads =
            BTreeMap::from([(head_c.clone(), vec![workspace.clone()])]);

        let left_grouped = merge_channel_snapshot(
            merge_channel_snapshot(left.clone(), right.clone())
                .expect("merge cyclic causal claims"),
            independent.clone(),
        )
        .expect("merge independent head after cyclic claims");
        let right_grouped = merge_channel_snapshot(
            left,
            merge_channel_snapshot(right, independent).expect("merge independent and cyclic claim"),
        )
        .expect("merge remaining cyclic claim");

        assert_eq!(left_grouped.workspaces, vec![workspace]);
        assert_eq!(left_grouped.workspaces, right_grouped.workspaces);
        assert_eq!(
            left_grouped.workspace_causal_tokens,
            right_grouped.workspace_causal_tokens
        );
        assert_eq!(
            left_grouped.workspace_causal_heads,
            right_grouped.workspace_causal_heads
        );
        assert_eq!(
            left_grouped
                .workspace_causal_heads
                .keys()
                .collect::<Vec<_>>(),
            vec![&head_c]
        );
    }

    #[test]
    fn channel_snapshot_merge_never_trusts_unknown_equal_causal_versions() {
        let channel_id = SessionId::generate();
        let workspace = sinomo_agent::WorkspaceRef::local_path("/work/unknown-protocol");
        let mut empty = AgentChannel::new_channel(channel_id, "Unknown causal protocol");
        assert!(empty.clear_workspaces());
        empty.workspace_causal_version = 999;

        let mut granted = empty.clone();
        granted.workspaces = vec![workspace];
        granted.workspace_causal_version = 999;

        let merged = merge_channel_snapshot(empty, granted)
            .expect("merge equal unknown causal protocols conservatively");
        assert!(merged.workspaces.is_empty());
        assert_eq!(
            merged.workspace_causal_version,
            AGENT_CHANNEL_WORKSPACE_CAUSAL_VERSION
        );
    }

    #[test]
    fn channel_snapshot_merge_legacy_empty_does_not_erase_authoritative_grants() {
        let channel_id = SessionId::generate();
        let base = time::OffsetDateTime::now_utc();
        let mut authoritative =
            AgentChannel::new_channel(channel_id.clone(), "Workspace migration");
        authoritative.add_workspace(sinomo_agent::WorkspaceRef::local_path("/work/current"));
        authoritative.updated_at = base;

        let mut legacy_empty = AgentChannel::new_channel(channel_id, "Workspace migration");
        legacy_empty.workspace_projection_version = 0;
        legacy_empty.updated_at = base + time::Duration::seconds(1);

        let merged = merge_channel_snapshot(authoritative, legacy_empty)
            .expect("merge legacy empty workspace snapshot");
        assert_eq!(merged.workspaces.len(), 1);
        assert_eq!(merged.workspaces[0].id, "/work/current");
        assert!(merged.has_authoritative_workspace_projection());
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
    fn accepted_chat_control_input_projects_idempotent_user_message() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-stable-control-input");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let agent_session_id = SessionId::generate();
        let mut channel = AgentChannel::with_default_title(channel_id.clone());
        channel.ensure_participant("main-agent", "Main");
        store.save_channel(&channel).expect("save channel");
        let input_id = TaskId::generate();
        let event = AgentEvent::new(
            TaskId::generate(),
            agent_session_id.clone(),
            AgentEventPayload::SteeringQueued {
                steering: sinomo_agent::AgentSteeringMessage {
                    steering_id: input_id.clone(),
                    message: "preserve this constraint".to_string(),
                },
            },
        );
        let result = AgentSessionControlResult {
            sequence: 3,
            accepted: true,
            events: vec![event.clone()],
            outcome: None,
            snapshot: sinomo_agent::AgentSessionControlSnapshot {
                session_id: agent_session_id,
                next_sequence: 4,
                active_turn: None,
                tasks: Vec::new(),
                buffered_event_count: 0,
                paused: false,
            },
        };

        project_accepted_chat_control_input(&store, &channel_id, &result)
            .expect("first projection");
        project_accepted_chat_control_input(&store, &channel_id, &result)
            .expect("idempotent replay");

        let loaded = store
            .load_channel(&channel_id)
            .expect("load projected channel")
            .expect("projected channel");
        let matches = loaded
            .transcript
            .iter()
            .filter(|message| message.message_id == input_id)
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].content, "preserve this constraint");
        assert_eq!(matches[0].created_at, event.emitted_at);
        assert!(matches!(
            &matches[0].author,
            AgentChannelAuthor::User { .. }
        ));
        assert_eq!(matches[0].source.adapter, "sinomo-chat-control");
        assert_eq!(
            matches[0].source.external_channel_id.as_deref(),
            Some(channel_id.as_str())
        );
        assert_eq!(matches[0].source.metadata["input_kind"], "steering");
    }

    #[test]
    fn late_realtime_projection_cannot_restore_revoked_membership() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-late-realtime-revocation");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::with_default_title(channel_id.clone());
        let participant_id = channel.ensure_participant("main-agent", "Main");
        let participant_session_id = channel
            .participant(&participant_id)
            .expect("participant")
            .agent_session_id
            .clone();
        store.save_channel(&channel).expect("save enabled channel");

        assert!(channel.disable_participant(&participant_id));
        channel.updated_at += time::Duration::nanoseconds(1);
        store.save_channel(&channel).expect("save revocation");

        let error = project_stable_chat_agent_message(
            &store,
            &channel_id,
            "main-agent",
            "Main",
            &participant_session_id,
            TaskId::generate(),
            time::OffsetDateTime::now_utc(),
            "late provider output",
            AgentChannelSource::app(),
        )
        .expect_err("revoked member must reject late output");
        assert!(
            error
                .to_string()
                .contains("not an enabled channel participant")
        );

        let loaded = store
            .load_channel(&channel_id)
            .expect("load revoked channel")
            .expect("channel");
        assert!(
            loaded
                .participants
                .iter()
                .any(|participant| participant.agent_key == "main-agent" && !participant.enabled)
        );
        assert!(
            !loaded
                .transcript
                .iter()
                .any(|message| { message.content == "late provider output" })
        );
    }

    #[test]
    fn participant_membership_revision_survives_stale_newer_metadata() {
        let channel_id = SessionId::generate();
        let mut enabled = AgentChannel::with_default_title(channel_id);
        let participant_id = enabled.ensure_participant("main-agent", "Main");
        let mut revoked = enabled.clone();
        assert!(revoked.disable_participant(&participant_id));
        revoked.updated_at = enabled.updated_at + time::Duration::nanoseconds(1);

        let mut stale_unrelated = enabled.clone();
        stale_unrelated.title = "newer unrelated title".to_string();
        stale_unrelated.updated_at = revoked.updated_at + time::Duration::nanoseconds(1);
        let merged = merge_channel_snapshot(revoked.clone(), stale_unrelated)
            .expect("merge stale unrelated metadata");
        let participant = merged.participant(&participant_id).expect("participant");
        assert!(
            !participant.enabled,
            "stale metadata must not restore access"
        );
        assert_eq!(
            participant.membership_revision,
            revoked
                .participant(&participant_id)
                .expect("revoked participant")
                .membership_revision
        );

        let mut explicitly_reenabled = revoked;
        explicitly_reenabled.ensure_participant("main-agent", "Main");
        let reenabled = merge_channel_snapshot(merged, explicitly_reenabled)
            .expect("merge causally newer re-enable");
        assert!(
            reenabled
                .participant(&participant_id)
                .is_some_and(|participant| participant.enabled),
            "an explicit causally newer re-enable must still win"
        );
    }

    #[test]
    fn history_message_page_uses_directory_size_and_defers_oversized_bodies() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-history-bounded-message-page");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "History bounds");
        store.save_channel(&channel).expect("save channel");
        let message = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "bounded immutable history body",
            AgentChannelSource::app(),
        );
        store
            .append_message(&channel_id, &message)
            .expect("append immutable message");
        let history_vfs = sinomo_runtime::vfs::mounted_sifs(Some(data_dir.root().to_path_buf()))
            .expect("open history VFS");

        let reads_before = store.message_record_body_read_count();
        let deferred = store
            .load_immutable_message_record_page(&history_vfs, &channel_id, None, 0, 1, 1)
            .expect("scan oversized record");
        assert!(deferred.records.is_empty());
        assert_eq!(deferred.consumed_entries, 0);
        assert_eq!(deferred.read_bytes, 0);
        assert_eq!(deferred.omitted_records, 0);
        assert!(deferred.required_bytes.is_some_and(|bytes| bytes > 1));
        assert_eq!(
            store.message_record_body_read_count(),
            reads_before,
            "an entry larger than the remaining budget must defer without a body read"
        );

        let loaded = store
            .load_immutable_message_record_page(&history_vfs, &channel_id, None, 0, 1, u64::MAX)
            .expect("read bounded record");
        assert_eq!(loaded.records.len(), 1);
        assert_eq!(loaded.records[0].message.message_id, message.message_id);
        assert!(loaded.read_bytes > 1);
        assert_eq!(store.message_record_body_read_count(), reads_before + 1);
    }

    #[test]
    fn history_message_page_charges_malformed_body_bytes_to_the_scan() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-history-malformed-message");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let malformed_path = format!("{}/malformed.json", store.message_records_dir(&channel_id));
        let malformed = b"{not valid channel message json";
        store
            .write_file(&malformed_path, malformed)
            .expect("write malformed immutable record fixture");
        let history_vfs = sinomo_runtime::vfs::mounted_sifs(Some(data_dir.root().to_path_buf()))
            .expect("open history VFS");

        let page = store
            .load_immutable_message_record_page(&history_vfs, &channel_id, None, 0, 1, u64::MAX)
            .expect("scan malformed record");
        assert!(page.records.is_empty());
        assert_eq!(page.consumed_entries, 1);
        assert_eq!(page.read_bytes, malformed.len() as u64);
        assert_eq!(page.omitted_records, 1);
    }

    #[test]
    fn bounded_history_metadata_ignores_snapshot_transcript_and_fails_closed() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-history-bounded-authorization");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Private history");
        let participant_id = channel.ensure_participant("main-agent", "Main");
        channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "private transcript sentinel",
            AgentChannelSource::app(),
        );
        store
            .save_channel(&channel)
            .expect("save authorized channel");

        let mut revoked = channel.clone();
        assert!(revoked.disable_participant(&participant_id));
        revoked.updated_at = channel.updated_at + time::Duration::nanoseconds(1);
        store
            .append_metadata_record(&revoked)
            .expect("append revocation without refreshing snapshot cache");
        let stale_snapshot = store
            .load_channel_snapshot_only(&channel_id)
            .expect("read stale snapshot")
            .expect("snapshot exists");
        assert!(
            stale_snapshot
                .participants
                .iter()
                .any(|participant| participant.agent_key == "main-agent" && participant.enabled)
        );
        assert!(
            stale_snapshot
                .transcript
                .iter()
                .any(|message| message.content == "private transcript sentinel")
        );
        store
            .write_file(
                &store.snapshot_path(&channel_id),
                b"private transcript sentinel; not valid snapshot JSON",
            )
            .expect("replace repairable snapshot with unreadable transcript fixture");

        let history_vfs = sinomo_runtime::vfs::mounted_sifs(Some(data_dir.root().to_path_buf()))
            .expect("open history VFS");
        let no_item_budget = store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, 0, u64::MAX)
            .expect("bounded metadata read");
        assert!(!no_item_budget.complete);
        assert!(no_item_budget.channel.is_none());
        assert_eq!(no_item_budget.scanned_items, 0);
        assert_eq!(no_item_budget.omitted_records, 0);

        let oversized_metadata = store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, usize::MAX, 1)
            .expect("oversized metadata read");
        assert!(!oversized_metadata.complete);
        assert!(oversized_metadata.channel.is_none());
        assert_eq!(oversized_metadata.scanned_items, 1);
        assert_eq!(oversized_metadata.scanned_bytes, 0);
        assert_eq!(oversized_metadata.omitted_records, 1);
        assert!(oversized_metadata.omitted_records <= oversized_metadata.scanned_items);

        let single_record_budget = store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, 1, u64::MAX)
            .expect("single-record metadata read");
        assert!(!single_record_budget.complete);
        assert!(single_record_budget.channel.is_none());

        let folded = store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, usize::MAX, u64::MAX)
            .expect("fold complete metadata history");
        assert!(folded.complete);
        assert_eq!(folded.omitted_records, 0);
        let folded_channel = folded.channel.expect("folded channel");
        assert!(
            folded_channel
                .participants
                .iter()
                .any(|participant| participant.agent_key == "main-agent" && !participant.enabled)
        );
    }

    #[test]
    fn compact_history_metadata_proves_current_membership_in_one_bounded_item() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-history-compact-authorization");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Compact history");
        let participant_id = channel.ensure_participant("main-agent", "Main");
        channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "transcript body must not enter compact authorization",
            AgentChannelSource::app(),
        );
        store.save_channel(&channel).expect("save channel");

        let projection_bytes = store
            .browser
            .read_file(&store.history_metadata_projection_path(&channel_id))
            .expect("read compact projection");
        let projection_text = String::from_utf8(projection_bytes).expect("projection text");
        assert!(!projection_text.contains("transcript body"));

        let history_vfs = sinomo_runtime::vfs::mounted_sifs(Some(data_dir.root().to_path_buf()))
            .expect("open history VFS");
        let authorized = store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, 1, u64::MAX)
            .expect("load compact authorization");
        assert!(authorized.complete);
        assert_eq!(authorized.scanned_items, 1);
        assert!(
            authorized
                .channel
                .expect("channel")
                .participants
                .iter()
                .any(|participant| participant.agent_key == "main-agent" && participant.enabled)
        );

        assert!(channel.disable_participant(&participant_id));
        channel.updated_at += time::Duration::nanoseconds(1);
        store.save_channel(&channel).expect("save revocation");
        let revoked = store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, 1, u64::MAX)
            .expect("load compact revocation");
        assert!(revoked.complete);
        assert_eq!(revoked.scanned_items, 1);
        assert!(
            revoked
                .channel
                .expect("channel")
                .participants
                .iter()
                .any(|participant| participant.agent_key == "main-agent" && !participant.enabled)
        );
    }

    #[test]
    fn compact_history_projection_refolds_immutable_revocation_before_publish() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-history-projection-revocation-race");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Concurrent revocation");
        let participant_id = channel.ensure_participant("main-agent", "Main");
        store.save_channel(&channel).expect("save enabled channel");
        let stale_source = store
            .append_metadata_record(&channel)
            .expect("retain stale writer source");

        let mut revoked = channel.clone();
        assert!(revoked.disable_participant(&participant_id));
        revoked.updated_at = channel.updated_at + time::Duration::nanoseconds(1);
        store
            .append_metadata_record(&revoked)
            .expect("append concurrent revocation");

        // A writer that began from the old source must fold the immutable
        // namespace again. It may not stamp its stale membership with the
        // post-revocation directory watermark.
        store
            .write_history_metadata_projection(&channel_id, &stale_source.record_id)
            .expect("publish stable projection");

        let history_vfs = sinomo_runtime::vfs::mounted_sifs(Some(data_dir.root().to_path_buf()))
            .expect("open history VFS");
        let current = store
            .load_channel_metadata_for_history(&history_vfs, &channel_id, 1, u64::MAX)
            .expect("load compact current authorization");
        assert!(current.complete);
        assert_eq!(current.scanned_items, 1);
        assert!(
            current
                .channel
                .expect("channel")
                .participants
                .iter()
                .any(|participant| participant.agent_key == "main-agent" && !participant.enabled)
        );
    }

    #[test]
    fn shared_channel_runtime_outbox_repairs_a_crash_before_message_materialization() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-channel-turn-outbox");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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

        use sinomo_runtime::api::app::chat::{ChatHistoryLoadMode, ChatHistoryLoadRequest};
        let mut request = ChatHistoryLoadRequest {
            session_id: channel_id.to_string(),
            mode: ChatHistoryLoadMode::Initial,
            sequence: None,
            limit: 1,
            include_overview: true,
            known_message_ids: Vec::new(),
            known_sources: Some(Default::default()),
        };
        let reads = reopened.message_record_body_read_count();
        assert!(
            reopened
                .load_channel_metadata_only(&channel_id)
                .unwrap()
                .unwrap()
                .transcript
                .is_empty()
        );
        assert_eq!(reopened.message_record_body_read_count(), reads);
        assert!(
            reopened
                .load_immutable_message_record(&channel_id, &response.message_id)
                .unwrap()
                .is_none()
        );
        let initial = reopened.read_transcript(&channel_id, &request).unwrap();
        assert_eq!(initial.reconciliation.page.messages.len(), 1);
        assert_eq!(
            initial.reconciliation.page.messages[0].message_id,
            response.message_id
        );
        assert_eq!(initial.reconciliation.discovered_message_ids.len(), 2);
        assert_eq!(initial.overview.unwrap().total_messages, 2);
        assert_eq!(
            reopened.message_record_body_read_count(),
            reads + 1,
            "read the existing prompt once for both the page and overview"
        );
        assert!(
            reopened
                .load_immutable_message_record(&channel_id, &response.message_id)
                .unwrap()
                .is_some()
        );
        request.mode = ChatHistoryLoadMode::ReconcileLatest;
        request.include_overview = false;
        request.known_message_ids = initial.reconciliation.discovered_message_ids;
        request.known_sources = Some(initial.sources);
        let refresh = reopened.read_transcript(&channel_id, &request).unwrap();
        assert_eq!(refresh.reconciliation.page.total_messages, 2);
        assert!(refresh.reconciliation.page.messages.is_empty());
        assert_eq!(reopened.message_record_body_read_count(), reads + 1);

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

        // The view stays open while a second writer crashes. Its cursor has
        // seen the first turn only, so the new outbox must still be recovered.
        let mut writer = AgentChannelRuntime::new(
            recovered_again,
            CrashAfterTurnChannelStore::new(SifsAgentChannelStore::open(&data_dir).unwrap()),
            FixedChannelReply("second durable answer"),
        )
        .with_loop_config(AgentChannelLoopConfig {
            max_agent_messages_per_ingress: 1,
        });
        assert!(matches!(
            writer.send_user_message("second turn", &mut |_| {}),
            Err(sinomo_agent::AgentChannelRuntimeError::Store { .. })
        ));
        drop(writer);
        let before = reopened.message_record_body_read_count();
        let after_crash = reopened.read_transcript(&channel_id, &request).unwrap();
        assert_eq!(after_crash.reconciliation.page.total_messages, 4);
        assert_eq!(
            after_crash
                .reconciliation
                .page
                .messages
                .iter()
                .filter(|m| m.content == "second durable answer")
                .count(),
            1
        );
        assert_eq!(reopened.message_record_body_read_count(), before + 1);
        request
            .known_message_ids
            .extend(after_crash.reconciliation.discovered_message_ids);
        request.known_sources = Some(after_crash.sources);
        assert!(
            reopened
                .read_transcript(&channel_id, &request)
                .unwrap()
                .reconciliation
                .page
                .messages
                .is_empty()
        );
        assert_eq!(reopened.message_record_body_read_count(), before + 1);
    }

    #[test]
    fn sifs_channel_messages_are_immutable_and_recent_snapshot_is_bounded() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-immutable-log");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
            .read_file(&store.snapshot_path(&channel_id))
            .expect("snapshot");
        let snapshot = serde_json::from_slice::<OwnedSifsAgentChannelSnapshot>(&snapshot_bytes)
            .expect("snapshot json");
        assert_eq!(
            snapshot.channel.transcript.len(),
            AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT
        );

        let first_record = SifsBrowser::open(&data_dir)
            .expect("open SIFS")
            .read_file(
                &store
                    .message_record_path(&channel_id, &first_message_id.expect("first message id")),
            )
            .expect("first immutable message record");
        assert!(
            String::from_utf8_lossy(&first_record).contains("message 0"),
            "immutable records should retain the beginning of the durable timeline"
        );
        let browser = SifsBrowser::open(&data_dir).expect("open SIFS");
        assert!(
            browser
                .read_file(&store.message_index_path(&channel_id))
                .is_err(),
            "new message appends must not create a shared mutable index"
        );
        assert!(
            browser
                .read_file(&store.turn_index_path(&channel_id))
                .is_err(),
            "new turn appends must not create a shared mutable index"
        );
        assert!(
            browser
                .read_file(&store.turn_record_path(&channel_id, &turn.turn_id))
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
            .create_dir_all(&store.message_chunks_dir(&channel_id))
            .expect("create legacy message dir");
        let legacy_messages = json_lines(&[first.clone(), second.clone()]);
        browser
            .write_file(&store.message_chunk_path(&channel_id, 1), &legacy_messages)
            .expect("write legacy message chunk");
        browser
            .write_file(
                &store.message_index_path(&channel_id),
                br#"{"schema_version":1,"entry_count":1,"updated_unix":0}"#,
            )
            .expect("write deliberately stale legacy message index");
        browser
            .create_dir_all(&store.turn_chunks_dir(&channel_id))
            .expect("create legacy turn dir");
        let legacy_turns = json_lines(std::slice::from_ref(&legacy_turn));
        browser
            .write_file(&store.turn_chunk_path(&channel_id, 0), &legacy_turns)
            .expect("write legacy turn chunk");
        browser
            .write_file(
                &store.turn_index_path(&channel_id),
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
                .read_file(&store.message_chunk_path(&channel_id, 1))
                .expect("legacy chunk remains readable"),
            legacy_messages,
            "new appends must not rewrite legacy JSONL chunks"
        );
    }

    #[test]
    fn sifs_channel_store_reconciles_two_stale_store_snapshots() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-stale-stores");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
            .create_dir_all(&setup.message_records_dir(&channel_id))
            .expect("create message records dir");
        browser
            .create_dir_all(&setup.turn_records_dir(&channel_id))
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
    fn transcript_refresh_detects_growth_in_a_known_legacy_chunk() {
        use sinomo_runtime::api::app::chat::{ChatHistoryLoadMode, ChatHistoryLoadRequest};
        let root = sinomo_testdir::TestDir::new("chat-legacy-incremental");
        let data = SinomoDataDir::new(&root);
        let _native = crate::native_test_fixture::NativeCliFixture::start(&data);
        let store = SifsAgentChannelStore::open(&data).unwrap();
        let id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(id.clone(), "Legacy");
        store.save_channel(&channel).unwrap();
        let first = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "older",
            AgentChannelSource::app(),
        );
        let chunk = store.message_chunk_path(&id, 1);
        let mut jsonl = serde_json::to_vec(&first).unwrap();
        jsonl.push(b'\n');
        store.write_file(&chunk, &jsonl).unwrap();
        let mut request = ChatHistoryLoadRequest {
            session_id: id.to_string(),
            mode: ChatHistoryLoadMode::Initial,
            sequence: None,
            limit: 1,
            include_overview: true,
            known_message_ids: Vec::new(),
            known_sources: Some(Default::default()),
        };
        let initial = store.read_transcript(&id, &request).unwrap();
        assert_eq!(initial.reconciliation.page.messages[0].content, "older");
        assert_eq!(initial.sources.legacy_chunks.len(), 1);
        request.mode = ChatHistoryLoadMode::ReconcileLatest;
        request.include_overview = false;
        request.known_message_ids = initial.reconciliation.discovered_message_ids;
        request.known_sources = Some(initial.sources);
        assert!(
            store
                .read_transcript(&id, &request)
                .unwrap()
                .reconciliation
                .page
                .messages
                .is_empty()
        );
        let next = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "arrived later",
            AgentChannelSource::app(),
        );
        jsonl.extend(serde_json::to_vec(&next).unwrap());
        jsonl.push(b'\n');
        store.write_file(&chunk, &jsonl).unwrap();
        let refresh = store.read_transcript(&id, &request).unwrap();
        assert_eq!(refresh.reconciliation.page.messages, vec![next.clone()]);
        assert_eq!(refresh.reconciliation.page.total_messages, 2);
        // An older view checkpoint may retain a different set of message IDs.
        request.known_sources = Some(refresh.sources);
        request.known_message_ids = vec![next.message_id.to_string()];
        let partial = store.read_transcript(&id, &request).unwrap();
        assert_eq!(partial.reconciliation.page.messages, vec![first]);
    }

    #[test]
    fn app_chat_send_surfaces_missing_codex_auth() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-codex-auth");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
        let user_inputs = before_resume
            .raw_transcript
            .items
            .iter()
            .filter_map(|item| match item {
                AgentItem::UserMessage { content, .. } => Some(content),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(user_inputs.len(), 1, "retain the failed turn's user input");
        let input: serde_json::Value =
            serde_json::from_str(user_inputs[0]).expect("structured agent input");
        assert_eq!(
            input.pointer("/input/latest_event/payload/text"),
            Some(&serde_json::json!("hello"))
        );
        assert!(before_resume.raw_transcript.items.iter().any(|item| {
            matches!(item, AgentItem::Error { detail, .. } if detail.contains("authentication required for codex"))
        }));
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
        assert_eq!(
            serde_json::to_value(&after_resume.raw_transcript).expect("resumed transcript"),
            serde_json::to_value(&before_resume.raw_transcript).expect("original transcript"),
            "resume must retain the failed turn without duplicating its durable channel message"
        );
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
    fn reopened_channel_recovers_a_participants_lost_detached_call() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-channel-detached-recovery");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Team channel");
        channel.ensure_user(AgentChannelUser::app_local());
        let participant_id = channel.ensure_participant("main-agent", "main-agent");
        let agent_session_id = channel
            .participant(&participant_id)
            .expect("participant")
            .agent_session_id
            .clone();
        store.save_channel(&channel).expect("save channel");
        // An earlier process ran the participant's detached call and stopped.
        let sessions = FileAgentSessionStore::for_data_dir(&data_dir);
        let mut session = AgentSession::new(agent_session_id.clone(), AgentRole::Main);
        session.agent_definition_key = Some("main-agent".to_string());
        sessions
            .save_session(&session)
            .expect("save participant session");
        let tool_call_id = TaskId::generate();
        sessions
            .append_record(&sinomo_agent::AgentSessionStoreRecord::new(
                agent_session_id.clone(),
                sinomo_agent::AgentSessionStoreRecordKind::DetachedCallStarted {
                    task_id: TaskId::generate(),
                    tool_call_id: tool_call_id.clone(),
                    tool: "exec_command".to_string(),
                },
            ))
            .expect("record detached call");

        let mut chat = AgentChat::new(data_dir.clone(), false).expect("chat runtime");
        chat.resume_session(&channel_id.to_string(), &[])
            .expect("resume channel");
        assert!(
            chat.has_async_updates() && chat.has_participant_work(),
            "the participant's unfinished call is found without a runtime"
        );
        chat.set_model(
            sinomo_lm::CODEX_PROVIDER_ID,
            "gpt-5.2-codex",
            sinomo_lm::DEFAULT_PROVIDER_ACCOUNT,
        )
        .expect("switch to codex");

        let mut events = Vec::new();
        let continued = chat
            .continue_async_updates(&mut |event| events.push(chat_event_label(event)))
            .expect("continue");

        assert!(continued);
        assert!(
            events.iter().any(|event| event == "speaker:main-agent"),
            "the participant took the continuation: {events:?}"
        );
        assert!(!chat.has_async_updates(), "the completion was taken once");
        let calls = sessions
            .load_detached_calls(&agent_session_id)
            .expect("load detached calls");
        assert!(
            calls
                .iter()
                .all(|call| call.completion.as_ref().is_some_and(|update| {
                    update.status == sinomo_agent::AsyncTaskStatus::Failed
                        && update.tool_call_id.as_ref() == Some(&tool_call_id)
                })),
            "the lost call is recorded as interrupted: {calls:?}"
        );
    }

    #[test]
    fn app_chat_send_to_explicit_channel_uses_channel_runtime() {
        let root = sinomo_testdir::TestDir::new("sinomo-app-chat-channel-runtime");
        let data_dir = SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
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
