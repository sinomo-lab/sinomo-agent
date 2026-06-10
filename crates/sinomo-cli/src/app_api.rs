//! Stable, GUI-facing surface over the main-agent chat runtime.
//!
//! The graphical app (`sinomo-app`) drives the *same* interaction runtime the
//! terminal UI uses, but needs a small, owned, type-erased API rather than the
//! crate-internal handler types. Everything here returns plain data so the app
//! never depends on `tui` internals.

use std::thread;
use std::time::{Duration, Instant};

use sinomo_agent::{
    AgentCatalog, AgentDefinition, AgentItem, AgentModelEvent, AgentRuntimeEvent,
    AgentRuntimeEventPayload, AgentToolProgressEvent, InteractionOutput, InteractionSession,
    InteractionSurface, InteractionSurfaceKind, TaskId,
};
use sinomo_lm::{
    AnthropicConfig, ClaudeCodeConfig, CodexCliConfig, GeminiConfig, GitHubCopilotConfig,
    OpenAiConfig, PROVIDER_DEFAULT_MODEL, ProviderReasoningEffort,
};
use sinomo_runtime::events::{InMemoryEventStore, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::NodeId;

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
    /// Token accounting for the just-finished turn.
    Usage { provider: String, model: String },
}

/// One visible message restored into a GUI chat runtime.
#[derive(Clone, Debug)]
pub struct ChatHistoryMessage {
    pub role: String,
    pub text: String,
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
        let handler = tui::app_build_chat_handler(&data_dir, None)?;
        let runtime =
            InteractionRuntime::new(InMemoryEventStore::new(), handler, app_session(None));
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
        AgentModelEvent::ToolCallStarted { tool, .. } => {
            on_event(ChatStreamEvent::Notice(format!("using tool: {tool}")));
        }
        AgentModelEvent::ToolCallDelta { .. } => {}
        AgentModelEvent::ToolCallCompleted { call, .. } => {
            on_event(ChatStreamEvent::Notice(format!(
                "tool completed: {}",
                call.tool
            )));
        }
        AgentModelEvent::Usage {
            provider, model, ..
        } => on_event(ChatStreamEvent::Usage { provider, model }),
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
            on_event(ChatStreamEvent::Notice(format!(
                "command started: {}",
                process.command
            )));
        }
        AgentToolProgressEvent::ProcessOutput { .. } => {}
        AgentToolProgressEvent::ProcessCompleted { process } => {
            on_event(ChatStreamEvent::Notice(format!(
                "command finished: {}",
                process.command
            )));
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
        } => on_event(ChatStreamEvent::Usage { provider, model }),
        InteractionOutput::Exit => {}
    }
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

fn dedup(values: Vec<String>) -> Vec<String> {
    let mut seen = Vec::new();
    for value in values {
        if !value.is_empty() && !seen.contains(&value) {
            seen.push(value);
        }
    }
    seen
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

/// A handle to browse SIFS (the distributed filesystem). In local mode this is
/// backed by the local content store + metadata; with a daemon it spans the
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
            ChatStreamEvent::Usage { provider, model } => format!("usage:{provider}/{model}"),
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
