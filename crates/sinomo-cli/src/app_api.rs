//! Stable, GUI-facing surface over the main-agent chat runtime.
//!
//! The graphical app (`sinomo-app`) drives the *same* interaction runtime the
//! terminal UI uses, but needs a small, owned, type-erased API rather than the
//! crate-internal handler types. Everything here returns plain data so the app
//! never depends on `tui` internals.

use std::thread;
use std::time::{Duration, Instant};

use sinomo_agent::{
    AgentCatalog, AgentDefinition, AgentItem, AgentModelEvent, AgentRuntimeControlHost,
    AgentRuntimeControlRegistry, AgentRuntimeEvent, AgentRuntimeEventPayload,
    AgentSessionControlResult, AgentSessionFollowUpRequest, AgentSessionInterruptRequest,
    AgentSessionReattachState, AgentSessionResumeRequest, AgentSessionResumeResponse,
    AgentSessionSteerRequest, AgentTaskLifecycleStatus, AgentTaskSnapshot, AgentToolProgressEvent,
    ContextAssemblyReport, InteractionOutput, InteractionSession, InteractionSurface,
    InteractionSurfaceKind, TaskId,
};
use sinomo_lm::{
    AnthropicConfig, ClaudeCodeConfig, CodexCliConfig, GeminiConfig, GitHubCopilotConfig,
    OpenAiConfig, PROVIDER_DEFAULT_MODEL, ProviderReasoningEffort,
};
use sinomo_runtime::events::{InMemoryEventStore, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::ClusterPaths;
use sinomo_runtime::node::NodeId;
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

/// A streamed event produced while the agent answers.
#[derive(Clone, Debug)]
pub enum ChatStreamEvent {
    /// A visible answer fragment.
    Token(String),
    /// A transient progress / status note (e.g. "waiting for model runtime").
    Notice(String),
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

/// Outcome of a live chat-runtime control operation.
#[derive(Clone, Debug, Default)]
pub struct ChatControlStatus {
    pub session_id: String,
    pub accepted: bool,
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
}

/// A selectable language-model provider with a small starter model list.
#[derive(Clone, Debug)]
pub struct ProviderSummary {
    pub id: String,
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

/// One running chat conversation backed by the main-agent runtime.
pub struct AgentChat {
    data_dir: SinomoDataDir,
    daemon_connected: bool,
    runtime: InteractionRuntime<InMemoryEventStore, Handler>,
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
        let handler = tui::app_build_chat_handler(&data_dir, None)?
            .with_runtime_control_registry(runtime_control_registry);
        let session = app_session(None);
        let mut runtime =
            InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());
        runtime
            .handler_mut()
            .replace_agent_session(&session, agent_transcript_from_history(&[]))?;
        Ok(Self {
            data_dir,
            daemon_connected,
            runtime,
        })
    }

    /// Begin a brand-new session (clears the in-memory transcript/context).
    pub fn reset_session(&mut self) {
        let session = app_session(None);
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), session.clone());
        let _ = self
            .runtime
            .handler_mut()
            .replace_agent_session(&session, agent_transcript_from_history(&[]));
    }

    /// Adopt an existing session id for continued turns (fresh in-memory context;
    /// the visible transcript is restored by the caller from its own store).
    pub fn resume_session(
        &mut self,
        session_id: &str,
        history: &[ChatHistoryMessage],
    ) -> Result<(), CliError> {
        let session = app_session(Some(session_id));
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), session.clone());
        self.runtime
            .handler_mut()
            .replace_agent_session(&session, agent_transcript_from_history(history))?;
        Ok(())
    }

    pub fn session_id(&self) -> String {
        self.runtime.session().session_id.to_string()
    }

    pub fn runtime_info(&self) -> ChatRuntimeInfo {
        tui::app_runtime_info(self.runtime.handler()).into()
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
        let mut emit = |output: InteractionOutput| forward_output(output, on_event);
        let trailing = self.runtime.ingest_user_input_streaming(text, &mut emit)?;
        for output in trailing {
            forward_output(output, on_event);
        }
        drain_runtime_events(&mut self.runtime, on_event)?;
        Ok(())
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
            definition,
            self.daemon_connected,
        )?;
        Ok(info.into())
    }
}

fn chat_control_status(result: AgentSessionControlResult) -> ChatControlStatus {
    ChatControlStatus {
        session_id: result.snapshot.session_id.to_string(),
        accepted: result.accepted,
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

fn drain_runtime_events(
    runtime: &mut InteractionRuntime<InMemoryEventStore, Handler>,
    on_event: &mut dyn FnMut(ChatStreamEvent),
) -> Result<(), CliError> {
    let mut saw_worker = false;
    let mut terminal = false;
    let mut failure = None;
    let mut streamed_text = String::new();
    let start_deadline = Instant::now() + APP_CHAT_RUNTIME_START_TIMEOUT;

    loop {
        let events = runtime.handler_mut().drain_runtime_events()?;
        if events.is_empty() {
            if saw_worker && !terminal {
                thread::sleep(APP_CHAT_RUNTIME_DRAIN_INTERVAL);
                continue;
            }
            if !terminal && Instant::now() < start_deadline {
                thread::sleep(APP_CHAT_RUNTIME_DRAIN_INTERVAL);
                continue;
            }
            break;
        }

        for event in events {
            match runtime_event_to_chat(event, &mut streamed_text, on_event) {
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
        return Err(CliError::Parse { detail });
    }
    Ok(())
}

enum RuntimeDrainState {
    Noop,
    Running,
    TerminalOk,
    TerminalErr(String),
}

fn runtime_event_to_chat(
    event: AgentRuntimeEvent,
    streamed_text: &mut String,
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
                if streamed_text.trim().is_empty() {
                    streamed_text.push_str(text);
                    on_event(ChatStreamEvent::Token(text.to_string()));
                } else if text.starts_with(streamed_text.as_str()) {
                    let suffix = &text[streamed_text.len()..];
                    if !suffix.is_empty() {
                        streamed_text.push_str(suffix);
                        on_event(ChatStreamEvent::Token(suffix.to_string()));
                    }
                }
            }
            RuntimeDrainState::Running
        }
        AgentRuntimeEventPayload::ModelProgress { event, .. } => {
            forward_model_event(event, streamed_text, on_event);
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
    streamed_text: &mut String,
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
            for event in split_chunk(&delta) {
                if let ChatStreamEvent::Token(token) = &event {
                    streamed_text.push_str(token);
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
        stdout_tail: tail_chars(&process.stdout, 240),
        stderr_tail: tail_chars(&process.stderr, 240),
    }
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
        })
        .collect()
}

fn resolve_agent(data_dir: &SinomoDataDir, key: &str) -> Option<AgentDefinition> {
    let catalog =
        AgentCatalog::load_from_data_dir(data_dir).unwrap_or_else(|_| AgentCatalog::built_ins());
    catalog.get(key).cloned()
}

/// Curated provider list with a small starter set of models each.
pub fn provider_summaries() -> Vec<ProviderSummary> {
    vec![
        ProviderSummary {
            id: "anthropic".to_string(),
            name: "Anthropic".to_string(),
            models: dedup(vec![
                AnthropicConfig::default().default_model,
                "claude-opus-4-8".to_string(),
                "claude-sonnet-4-6".to_string(),
                "claude-haiku-4-5".to_string(),
            ]),
        },
        ProviderSummary {
            id: "openai".to_string(),
            name: "OpenAI".to_string(),
            models: dedup(vec![
                OpenAiConfig::default().default_model,
                "gpt-5".to_string(),
                "o4-mini".to_string(),
            ]),
        },
        ProviderSummary {
            id: "gemini".to_string(),
            name: "Google Gemini".to_string(),
            models: dedup(vec![
                GeminiConfig::default().default_model,
                "gemini-2.5-pro".to_string(),
                "gemini-2.5-flash".to_string(),
            ]),
        },
        ProviderSummary {
            id: "claude-code".to_string(),
            name: "Claude Code (CLI)".to_string(),
            models: dedup(vec![ClaudeCodeConfig::default().model]),
        },
        ProviderSummary {
            id: "codex-cli".to_string(),
            name: "Codex (CLI)".to_string(),
            models: dedup(vec![CodexCliConfig::default().model]),
        },
        ProviderSummary {
            id: "github-copilot".to_string(),
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
use crate::sifs_file_manager::SifsFileManagerBackend;

/// One SIFS directory entry.
#[derive(Clone, Debug)]
pub struct SifsEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub is_link: bool,
    pub size: Option<u64>,
}

/// A handle to browse SIFS (the distributed filesystem). Degraded fallback can
/// read local content store metadata, while daemon-backed operation spans the
/// cluster.
pub struct SifsBrowser {
    backend: SifsFileManagerBackend,
    node_id: String,
}

impl SifsBrowser {
    pub fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let backend = SifsFileManagerBackend::open(data_dir)?;
        let node_id = backend.node_label();
        Ok(Self { backend, node_id })
    }

    /// The node SIFS objects are placed on locally.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// List a directory (directories first, then files, name-sorted).
    pub fn list(&self, path: &str) -> Result<Vec<SifsEntry>, String> {
        let mut entries: Vec<SifsEntry> = self
            .backend
            .list_dir(path)?
            .into_iter()
            .map(|entry| SifsEntry {
                name: entry.name,
                path: entry.path,
                is_dir: matches!(entry.kind, FileManagerEntryKind::Directory),
                is_link: matches!(entry.kind, FileManagerEntryKind::Symlink),
                size: entry.size,
            })
            .collect();
        entries.sort_by(|a, b| {
            (b.is_dir as u8)
                .cmp(&(a.is_dir as u8))
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        });
        Ok(entries)
    }

    pub fn read_file(&self, path: &str) -> Result<Vec<u8>, String> {
        self.backend.read_file(path)
    }

    pub fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), String> {
        self.backend.write_file(path, bytes)
    }

    pub fn create_dir_all(&self, path: &str) -> Result<(), String> {
        self.backend.create_dir_all(path)
    }

    pub fn parent(&self, path: &str) -> Option<String> {
        self.backend.parent_path(path)
    }

    pub fn normalize(&self, path: &str) -> String {
        self.backend
            .normalize_path(path)
            .unwrap_or_else(|_| "/".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_agent::{
        AgentInteractionState, AgentLoopDecision, AgentLoopResponseMode, AgentStopReason,
    };

    fn chat_event_label(event: ChatStreamEvent) -> String {
        match event {
            ChatStreamEvent::Token(token) => format!("token:{token}"),
            ChatStreamEvent::Notice(notice) => format!("notice:{notice}"),
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
        let mut streamed_text = String::new();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));

        let state = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                sequence: 1,
                op_id: TaskId::generate(),
                decision: final_decision("ready"),
                stop_reason: AgentStopReason::FinalDecision,
            }),
            &mut streamed_text,
            &mut on_event,
        );

        assert!(matches!(state, RuntimeDrainState::Running));
        assert_eq!(events, vec!["token:ready"]);
        assert_eq!(streamed_text, "ready");
    }

    #[test]
    fn chat_mapper_appends_only_final_suffix_after_streaming_delta() {
        let mut streamed_text = String::new();
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
            &mut streamed_text,
            &mut on_event,
        );
        let final_state = runtime_event_to_chat(
            runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                sequence: 2,
                op_id,
                decision: final_decision("hello world"),
                stop_reason: AgentStopReason::FinalDecision,
            }),
            &mut streamed_text,
            &mut on_event,
        );

        assert!(matches!(first, RuntimeDrainState::Running));
        assert!(matches!(final_state, RuntimeDrainState::Running));
        assert_eq!(events, vec!["token:hello", "token: world"]);
        assert_eq!(streamed_text, "hello world");
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
        let mut streamed_text = String::new();
        let mut events = Vec::new();
        let mut on_event = |event| events.push(chat_event_label(event));

        forward_model_event(
            AgentModelEvent::AssistantTextDelta {
                step_index: 0,
                delta: "<sinomo_progress>waiting for configured model runtime</sinomo_progress>"
                    .to_string(),
            },
            &mut streamed_text,
            &mut on_event,
        );

        assert_eq!(events, vec!["notice:waiting for configured model runtime"]);
        assert!(streamed_text.is_empty());
    }
}
