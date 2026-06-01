//! Bounded interactive controller skeleton.

use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelError, LanguageModelMessage, LanguageModelProvider, LanguageModelRequest,
    LanguageModelResponse, LanguageModelResponseFormat, LanguageModelRole,
    LanguageModelStreamEvent,
};
use sinomo_runtime::core::{
    ActionRequest, Confidence, ContextPacket, EventEnvelope, EventKind, Id, LatencyClass,
    PrivacyClass, RoutingMetadata, RoutingScope, Salience, Source, TriggerAction, TriggerDecision,
};
use sinomo_runtime::device::{DeviceKind, DeviceProfile};
use sinomo_runtime::events::{
    EntityId, EventId, EventStore, ReplayDirection, ReplayQuery, SessionId, StoredEvent,
};
use sinomo_runtime::node::NodeId;
use thiserror::Error;

mod agent;
pub mod tools;

pub use agent::*;
pub use tools::*;

pub type TaskId = Id;

pub fn register_storage_paths(
    layout: &mut sinomo_runtime::storage::StorageLayout,
) -> Result<(), sinomo_runtime::storage::StorageContractError> {
    layout.register_escape_path(
        "sinomo-agent",
        sinomo_runtime::storage::StorageRing::R2,
        "events/agent",
        "agent traces, context assembly reports, model events, and tool evidence artifacts",
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolSpec {
    pub kind: AgentToolKind,
    pub name: String,
    pub description: String,
    pub async_default: bool,
}

impl AgentToolSpec {
    pub fn new(
        kind: AgentToolKind,
        name: impl Into<String>,
        description: impl Into<String>,
        async_default: bool,
    ) -> Self {
        Self {
            kind,
            name: name.into(),
            description: description.into(),
            async_default,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolKind {
    StartAsyncTask,
    InspectAsyncTask,
    CancelAsyncTask,
    RunShellCommand,
    ReadFile,
    WriteFile,
    SearchFiles,
    ApplyPatch,
    StartCommandSession,
    InspectCommandSession,
    WriteCommandStdin,
    CancelCommandSession,
    RequestUserInput,
    ReadSifs,
    WriteSifs,
    ListSifs,
    ClusterStatus,
    HandoffToMaid,
    ObserveVision,
}

pub fn default_main_agent_tools() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::StartAsyncTask,
            "start_async_task",
            "Start slow work and return a durable task id immediately.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::InspectAsyncTask,
            "inspect_async_task",
            "Read progress, result references, and cancellation state for a task.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::CancelAsyncTask,
            "cancel_async_task",
            "Request cancellation for an active background task.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::RunShellCommand,
            "run_shell_command",
            "Run a bounded shell command on the selected node.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::ReadSifs,
            "read_sifs",
            "Read a file from the cluster root filesystem.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::WriteSifs,
            "write_sifs",
            "Write a file to an allowed SIFS path.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::ListSifs,
            "list_sifs",
            "List a SIFS directory with session-aware defaults.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::ClusterStatus,
            "cluster_status",
            "Summarize local quorum, node placement, and degraded services.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::HandoffToMaid,
            "handoff_to_maid",
            "Delegate maintenance, cleanup, rollout, or cluster-health work.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::ObserveVision,
            "observe_vision",
            "Attach a current screen/camera observation for multimodal reasoning.",
            false,
        ),
    ]
}

pub fn default_coding_agent_tools() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "Read a workspace file into model-visible text with a bounded byte budget.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::SearchFiles,
            "search",
            "Search workspace text files for a literal query.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::ApplyPatch,
            "apply_patch",
            "Apply a scoped patch to workspace files available in the current sandbox.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::RunShellCommand,
            "run_command",
            "Run a bounded workspace command in the current sandbox.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::StartCommandSession,
            "start_command_session",
            "Start a workspace command session with incremental output, stdin, timeout, and cancellation support.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::InspectCommandSession,
            "inspect_command_session",
            "Inspect incremental output and status for a workspace command session.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::WriteCommandStdin,
            "write_command_stdin",
            "Write stdin to a running workspace command session.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::CancelCommandSession,
            "cancel_command_session",
            "Cancel a running workspace command session.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::RequestUserInput,
            "request_user_input",
            "Ask the user for a short clarification through the interactive runtime.",
            true,
        ),
    ]
}

pub fn main_agent_system_prompt(tools: &[AgentToolSpec]) -> String {
    let tools_json = serde_json::to_string_pretty(tools).expect("tool specs serialize");
    format!(
        r#"You are Sinomo, the user-facing main agent.

Operate as a real-time, event-driven agent. Keep live interaction responsive:
- never block the conversation on slow work;
- start async tasks for slow shell, research, coding, maintenance, or file work;
- treat async progress and results as session events;
- hand cluster maintenance, cleanup, rollout, and diagnostics to the maid agent by default;
- use SIFS as the default filesystem root;
- use the local Rust MiniCPM-V 4.6 runtime as the core live model when available, and use stronger models only for deliberate background work.

Return only JSON shaped as:
{{
  "response_mode": "answer|acknowledge|clarify|defer|silent",
  "text": "short user-facing response",
  "tool_calls": [
    {{
      "tool": "tool name",
      "input": {{}},
      "async": true,
      "summary": "why this tool is being used"
    }}
  ],
  "maid_handoff": null,
  "next_state": "speaking|listening|waiting|delegating|recovering|idle"
}}

Available tools:
```json
{tools_json}
```"#
    )
}

pub fn maid_agent_system_prompt() -> String {
    "You are Sinomo's maid agent. Stay silent unless escalation is needed. Coalesce maintenance events, inspect evidence through files and tools, prefer minimal operational fixes, and report concise findings back to the main agent.".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AsyncTaskUpdate {
    pub kind: String,
    pub task_id: TaskId,
    pub status: AsyncTaskStatus,
    pub summary: String,
    pub needs_user_attention: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    pub next_action_hint: AsyncTaskNextAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AsyncTaskStatus {
    Running,
    Progress,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AsyncTaskNextAction {
    ContinueSilently,
    TellUser,
    AskUser,
    HandoffToMaid,
    InspectResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentLoopInput {
    pub session: InteractionSession,
    pub latest_event: EventEnvelope,
    pub context: BoundedContext,
    #[serde(default)]
    pub async_update: Option<AsyncTaskUpdate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evolution_context: Option<AgentEvolutionContext>,
    #[serde(default)]
    pub tool_specs: Vec<AgentToolSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentLoopDecision {
    pub response_mode: AgentLoopResponseMode,
    pub text: String,
    #[serde(default)]
    pub tool_calls: Vec<AgentToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maid_handoff: Option<MaidHandoffRequest>,
    pub next_state: AgentInteractionState,
}

impl AgentLoopDecision {
    pub fn silent() -> Self {
        Self {
            response_mode: AgentLoopResponseMode::Silent,
            text: String::new(),
            tool_calls: Vec::new(),
            maid_handoff: None,
            next_state: AgentInteractionState::Idle,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentLoopResponseMode {
    Answer,
    Acknowledge,
    Clarify,
    Defer,
    Silent,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentInteractionState {
    Speaking,
    Listening,
    Waiting,
    Delegating,
    Recovering,
    Idle,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolCall {
    pub tool: String,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub r#async: bool,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MaidHandoffRequest {
    pub reason: String,
    pub evidence_ref: String,
    pub priority: MaidHandoffPriority,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MaidHandoffPriority {
    Routine,
    Important,
    Emergency,
}

pub struct MainAgentLoop<M> {
    model: M,
    provider: String,
    model_id: String,
    tools: Vec<AgentToolSpec>,
}

impl<M: InteractionModel> MainAgentLoop<M> {
    pub fn new(model: M, provider: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            model,
            provider: provider.into(),
            model_id: model_id.into(),
            tools: default_main_agent_tools(),
        }
    }

    pub fn with_tools(mut self, tools: Vec<AgentToolSpec>) -> Self {
        self.tools = tools;
        self
    }

    pub fn decide(&self, input: &AgentLoopInput) -> Result<AgentLoopDecision, AgentError> {
        self.decide_with_model(input, |model, request| model.complete(request))
    }

    pub fn decide_streaming(
        &self,
        input: &AgentLoopInput,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<AgentLoopDecision, AgentError> {
        self.decide_with_model(input, |model, request| {
            model.complete_streaming(request, on_delta)
        })
    }

    fn decide_with_model(
        &self,
        input: &AgentLoopInput,
        complete: impl FnOnce(
            &M,
            &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError>,
    ) -> Result<AgentLoopDecision, AgentError> {
        if let Some(update) = &input.async_update
            && !update.needs_user_attention
            && update.next_action_hint == AsyncTaskNextAction::ContinueSilently
        {
            return Ok(AgentLoopDecision::silent());
        }
        let request = self.request(input)?;
        let response = complete(&self.model, &request)?;
        parse_agent_loop_decision(&response.content)
    }

    pub fn request(&self, input: &AgentLoopInput) -> Result<LanguageModelRequest, AgentError> {
        let tools = if input.tool_specs.is_empty() {
            self.tools.as_slice()
        } else {
            input.tool_specs.as_slice()
        };
        Ok(LanguageModelRequest {
            provider: self.provider.clone(),
            model: self.model_id.clone(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: main_agent_system_prompt(tools),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: serde_json::to_string_pretty(input).map_err(|source| {
                        AgentError::ModelRequest {
                            detail: source.to_string(),
                        }
                    })?,
                },
            ],
            items: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: Some(0.2),
            max_output_tokens: Some(900),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::json!({
                "loop": "main-agent",
                "session_id": input.session.session_id,
                "event_id": input.latest_event.event_id
            }),
        })
    }
}

pub fn parse_agent_loop_decision(raw: &str) -> Result<AgentLoopDecision, AgentError> {
    let trimmed = raw.trim();
    let json = trimmed
        .strip_prefix("```json")
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    match serde_json::from_str(json) {
        Ok(decision) => Ok(decision),
        Err(source) => {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(json)
                && let Some(text) = value.get("text").and_then(serde_json::Value::as_str)
            {
                return Ok(AgentLoopDecision {
                    response_mode: value
                        .get("response_mode")
                        .and_then(serde_json::Value::as_str)
                        .and_then(parse_agent_loop_response_mode)
                        .unwrap_or(AgentLoopResponseMode::Answer),
                    text: text.to_string(),
                    tool_calls: value
                        .get("tool_calls")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .unwrap_or_default(),
                    maid_handoff: value
                        .get("maid_handoff")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok()),
                    next_state: value
                        .get("next_state")
                        .and_then(serde_json::Value::as_str)
                        .and_then(parse_agent_interaction_state)
                        .unwrap_or(AgentInteractionState::Listening),
                });
            }
            if trimmed.is_empty() {
                Err(AgentError::ModelResponse {
                    detail: format!("{source}: {raw}"),
                })
            } else {
                Ok(AgentLoopDecision {
                    response_mode: AgentLoopResponseMode::Answer,
                    text: trimmed.to_string(),
                    tool_calls: Vec::new(),
                    maid_handoff: None,
                    next_state: AgentInteractionState::Listening,
                })
            }
        }
    }
}

fn parse_agent_loop_response_mode(value: &str) -> Option<AgentLoopResponseMode> {
    match value {
        "answer" => Some(AgentLoopResponseMode::Answer),
        "acknowledge" => Some(AgentLoopResponseMode::Acknowledge),
        "clarify" => Some(AgentLoopResponseMode::Clarify),
        "defer" => Some(AgentLoopResponseMode::Defer),
        "silent" => Some(AgentLoopResponseMode::Silent),
        _ => None,
    }
}

fn parse_agent_interaction_state(value: &str) -> Option<AgentInteractionState> {
    match value {
        "speaking" => Some(AgentInteractionState::Speaking),
        "listening" => Some(AgentInteractionState::Listening),
        "waiting" => Some(AgentInteractionState::Waiting),
        "delegating" => Some(AgentInteractionState::Delegating),
        "recovering" => Some(AgentInteractionState::Recovering),
        "idle" => Some(AgentInteractionState::Idle),
        _ => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractionSession {
    pub session_id: SessionId,
    pub node_id: NodeId,
    pub surface: InteractionSurface,
}

impl InteractionSession {
    pub fn new(session_id: SessionId, node_id: NodeId, surface: InteractionSurface) -> Self {
        Self {
            session_id,
            node_id,
            surface,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractionSurface {
    pub surface_id: String,
    pub kind: InteractionSurfaceKind,
}

impl InteractionSurface {
    pub fn new(surface_id: impl Into<String>, kind: InteractionSurfaceKind) -> InteractionSurface {
        InteractionSurface {
            surface_id: surface_id.into(),
            kind,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractionSurfaceKind {
    CommandLine,
    App,
    Api,
    Voice,
    Remote,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractionInputKind {
    Message,
    Command,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InteractionOutput {
    Chunk(String),
    Line(String),
    Exit,
}

pub trait InteractionModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError>;

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let _ = on_delta;
        self.complete(request)
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        on_event(LanguageModelStreamEvent::Started {
            provider: request.provider.clone(),
            model: request.model.clone(),
        });
        let mut emitted_delta = false;
        let response = self.complete_streaming(request, &mut |delta| {
            emitted_delta = true;
            on_event(LanguageModelStreamEvent::TextDelta {
                delta: delta.to_string(),
            });
        })?;
        if !emitted_delta && !response.content.is_empty() {
            on_event(LanguageModelStreamEvent::TextDelta {
                delta: response.content.clone(),
            });
        }
        if let Some(usage) = &response.usage {
            on_event(LanguageModelStreamEvent::Usage {
                usage: usage.clone(),
            });
        }
        on_event(LanguageModelStreamEvent::Finished {
            finish_reason: None,
        });
        Ok(response)
    }
}

impl<T: LanguageModelProvider> InteractionModel for T {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete(self, request)
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming(self, request, on_delta)
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming_events(self, request, on_event)
    }
}

pub trait EventEnvelopeExt {
    fn caused_by(self, event_id: &EventId) -> Self;
}

impl EventEnvelopeExt for EventEnvelope {
    fn caused_by(mut self, event_id: &EventId) -> Self {
        self.links.caused_by.push(event_id.clone());
        self
    }
}

pub fn user_interaction_input_event(session: &InteractionSession, text: &str) -> EventEnvelope {
    let input_kind = if text.trim_start().starts_with('/') {
        InteractionInputKind::Command
    } else {
        InteractionInputKind::Message
    };
    interaction_event(
        session,
        "interaction.input.received",
        serde_json::json!({
            "text": text,
            "input_kind": input_kind,
            "surface": session.surface,
        }),
    )
}

pub fn interaction_event(
    session: &InteractionSession,
    kind: &str,
    payload: serde_json::Value,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        time::OffsetDateTime::now_utc(),
        session.node_id.clone(),
        Source::new("interaction.surface").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.62).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session.session_id.clone())
    .with_payload(payload)
}

pub fn interaction_model_request<S: EventStore>(
    store: &S,
    session: &InteractionSession,
    message_event_id: &EventId,
    text: &str,
    provider: &str,
    model: &str,
) -> Result<LanguageModelRequest, AgentError> {
    let recent = store.replay(ReplayQuery {
        session_id: Some(session.session_id.clone()),
        kind_prefix: Some("interaction.model.responded".to_string()),
        origin_node_id: None,
        after_sequence: None,
        start_time: None,
        end_time: None,
        direction: ReplayDirection::Reverse,
        limit: Some(6),
    })?;
    let mut recent_turns: Vec<String> = recent
        .into_iter()
        .rev()
        .filter_map(|stored| {
            stored
                .event
                .payload
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect();
    if recent_turns.is_empty() {
        recent_turns.push("No prior assistant turns in this session.".to_string());
    }

    Ok(LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        messages: vec![
            LanguageModelMessage {
                role: LanguageModelRole::System,
                content: INTERACTION_SYSTEM_PROMPT.to_string(),
            },
            LanguageModelMessage {
                role: LanguageModelRole::User,
                content: serde_json::to_string_pretty(&serde_json::json!({
                    "session_id": session.session_id,
                    "surface": session.surface,
                    "input_event_id": message_event_id,
                    "recent_assistant_turns": recent_turns,
                    "user_input": text
                }))
                .expect("interaction request serializes"),
            },
        ],
        items: Vec::new(),
        tools: Vec::new(),
        tool_choice: None,
        parallel_tool_calls: None,
        temperature: Some(0.4),
        max_output_tokens: Some(800),
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::json!({
            "loop": "interactive",
            "node_id": session.node_id,
            "session_id": session.session_id,
            "input_event_id": message_event_id
        }),
    })
}

const INTERACTION_SYSTEM_PROMPT: &str = r#"You are Sinomo's command-line conversation loop.
Respond directly to the user in plain text.
Use the supplied event ids and session metadata as context, but do not expose internal event JSON unless asked.
Slash commands are handled by Sinomo outside the model; ordinary user input reaches you only after being recorded as an interaction event."#;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BoundedContext {
    pub session_id: SessionId,
    pub events: Vec<StoredEvent>,
    pub packet: ContextPacket,
}

pub struct BoundedContextBuilder {
    event_limit: usize,
}

impl BoundedContextBuilder {
    pub fn new(event_limit: usize) -> Self {
        Self { event_limit }
    }

    pub fn build<S: EventStore>(
        &self,
        store: &S,
        session_id: SessionId,
    ) -> Result<BoundedContext, AgentError> {
        let mut events = store.replay(ReplayQuery {
            session_id: Some(session_id.clone()),
            kind_prefix: None,
            origin_node_id: None,
            after_sequence: None,
            start_time: None,
            end_time: None,
            direction: ReplayDirection::Reverse,
            limit: Some(self.event_limit),
        })?;
        events.reverse();
        let packet = ContextPacket {
            salient_events: events
                .iter()
                .filter(|event| event.event.salience.get() >= 0.7)
                .map(|event| event.event.event_id.clone())
                .collect(),
            memory_records: Vec::<EntityId>::new(),
        };
        Ok(BoundedContext {
            session_id,
            events,
            packet,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InteractiveTurn {
    pub response: AgentResponse,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_request: Option<ActionRequest>,
    pub trace_event: EventEnvelope,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentResponse {
    pub mode: AgentResponseMode,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentResponseMode {
    Acknowledge,
    Clarify,
    Answer,
    Defer,
}

#[derive(Clone, Debug)]
pub struct InteractiveAgent {
    node_id: NodeId,
    action_targets: Vec<DeviceProfile>,
}

impl InteractiveAgent {
    pub fn new(node_id: NodeId, action_targets: Vec<DeviceProfile>) -> Self {
        Self {
            node_id,
            action_targets,
        }
    }

    pub fn handle_trigger(
        &self,
        decision: &TriggerDecision,
        context: &BoundedContext,
    ) -> InteractiveTurn {
        let context_text = context_payload_text(context);
        let action_target = self.screen_target();
        let action_request = if matches!(
            decision.action,
            TriggerAction::Acknowledge | TriggerAction::Interrupt | TriggerAction::Alert
        ) && context_text.contains("alert")
        {
            action_target.as_ref().map(|device| ActionRequest {
                request_id: TaskId::generate(),
                origin_node_id: self.node_id.clone(),
                target_node_id: Some(device.node_id.clone()),
                target_device_id: Some(device.device_id.clone()),
                kind: "ui.inspect.alert".to_string(),
                payload: serde_json::json!({
                    "reason": decision.reason,
                    "context_event_count": context.events.len()
                }),
            })
        } else {
            None
        };

        let response = match decision.action {
            TriggerAction::StaySilent => AgentResponse {
                mode: AgentResponseMode::Defer,
                text: "I will keep watching this context.".to_string(),
            },
            TriggerAction::Acknowledge => AgentResponse {
                mode: AgentResponseMode::Acknowledge,
                text: "I see the alert context and will check it.".to_string(),
            },
            TriggerAction::Interrupt => AgentResponse {
                mode: AgentResponseMode::Acknowledge,
                text: "This looks urgent; I am checking the visible alert now.".to_string(),
            },
            TriggerAction::Alert => AgentResponse {
                mode: AgentResponseMode::Acknowledge,
                text: "A safety alert needs attention.".to_string(),
            },
            TriggerAction::Escalate => AgentResponse {
                mode: AgentResponseMode::Clarify,
                text: "I found a salient cross-modal observation and need a little more context."
                    .to_string(),
            },
        };

        let trace_event = self.trace_event(decision, context, &response, action_request.as_ref());

        InteractiveTurn {
            response,
            action_request,
            trace_event,
        }
    }

    fn screen_target(&self) -> Option<DeviceProfile> {
        self.action_targets
            .iter()
            .find(|device| matches!(device.kind, DeviceKind::Screen))
            .cloned()
    }

    fn trace_event(
        &self,
        decision: &TriggerDecision,
        context: &BoundedContext,
        response: &AgentResponse,
        action_request: Option<&ActionRequest>,
    ) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            EventId::generate(),
            time::OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("agent.interactive").expect("valid source"),
            EventKind::new("agent.turn.completed").expect("valid kind"),
            Salience::new(0.64).expect("valid salience"),
            Confidence::new(0.88).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_session(context.session_id.clone())
        .with_payload(serde_json::json!({
            "trigger_event_id": decision.trigger_event_id,
            "trigger_action": decision.action,
            "response": response,
            "action_request": action_request,
            "bounded_context_event_count": context.events.len(),
            "salient_events": context.packet.salient_events,
        }));
        event
            .links
            .caused_by
            .push(decision.trigger_event_id.clone());
        event
    }
}

fn context_payload_text(context: &BoundedContext) -> String {
    let mut text = String::new();
    for event in &context.events {
        collect_json_text(&event.event.payload, &mut text);
    }
    text.to_ascii_lowercase()
}

fn collect_json_text(value: &serde_json::Value, text: &mut String) {
    match value {
        serde_json::Value::String(value) => {
            text.push(' ');
            text.push_str(value);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_json_text(value, text);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_json_text(value, text);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Error)]
pub enum AgentError {
    #[error(transparent)]
    EventStore(#[from] sinomo_runtime::events::EventStoreError),
    #[error(transparent)]
    Model(#[from] LanguageModelError),
    #[error("failed to build model request: {detail}")]
    ModelRequest { detail: String },
    #[error("failed to parse model response: {detail}")]
    ModelResponse { detail: String },
    #[error("agent trace store failed: {detail}")]
    TraceStore { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{
        Confidence, EventEnvelope, EventKind, LatencyClass, PrivacyClass, RoutingMetadata,
        RoutingScope, Salience, Source,
    };
    use sinomo_runtime::device::{DeviceId, DeviceState};
    use sinomo_runtime::events::{EventId, EventStore, InMemoryEventStore};
    use sinomo_runtime::node::NodeId;

    struct FakeInteractionModel {
        response: &'static str,
    }

    impl InteractionModel for FakeInteractionModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            assert_eq!(
                request.response_format,
                LanguageModelResponseFormat::JsonObject
            );
            assert!(request.messages[0].content.contains("Available tools"));
            Ok(LanguageModelResponse {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                content: self.response.to_string(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }
    }

    fn event(id: &str, text: &str, salience: f32) -> EventEnvelope {
        EventEnvelope::new(
            EventId::from_legacy_named(id).expect("event"),
            time::OffsetDateTime::now_utc(),
            NodeId::from_legacy_named("node_desktop").expect("node"),
            Source::new("test.agent").expect("source"),
            EventKind::new("speech.transcript.final").expect("kind"),
            Salience::new(salience).expect("salience"),
            Confidence::new(0.9).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_session(SessionId::from_legacy_named("sess_agent").expect("session"))
        .with_payload(serde_json::json!({ "text": text }))
    }

    #[test]
    fn bounded_context_limits_recent_events() {
        let mut store = InMemoryEventStore::new();
        store.append(event("evt_1", "one", 0.1)).expect("append");
        store.append(event("evt_2", "two", 0.8)).expect("append");
        store.append(event("evt_3", "three", 0.9)).expect("append");

        let context = BoundedContextBuilder::new(2)
            .build(
                &store,
                SessionId::from_legacy_named("sess_agent").expect("session"),
            )
            .expect("context");

        assert_eq!(context.events.len(), 2);
        assert_eq!(context.packet.salient_events.len(), 2);
    }

    #[test]
    fn interaction_input_event_tags_slash_commands() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_interaction").expect("session"),
            NodeId::from_legacy_named("node_interaction").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );

        let event = user_interaction_input_event(&session, " /status");

        assert_eq!(
            event
                .payload
                .get("input_kind")
                .and_then(serde_json::Value::as_str),
            Some("command")
        );
        assert_eq!(event.session_id, Some(session.session_id));
    }

    #[test]
    fn interaction_events_can_record_causality() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_interaction").expect("session"),
            NodeId::from_legacy_named("node_interaction").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        let cause = EventId::from_legacy_named("evt_cause").expect("event");

        let event = interaction_event(
            &session,
            "interaction.message.received",
            serde_json::json!({ "text": "hello" }),
        )
        .caused_by(&cause);

        assert_eq!(event.links.caused_by, vec![cause]);
    }

    #[test]
    fn interaction_model_request_includes_recent_assistant_turns() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_interaction").expect("session"),
            NodeId::from_legacy_named("node_interaction").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        let mut store = InMemoryEventStore::new();
        store
            .append(interaction_event(
                &session,
                "interaction.model.responded",
                serde_json::json!({ "text": "previous answer" }),
            ))
            .expect("append");
        let message_event_id = EventId::from_legacy_named("evt_message").expect("event");

        let request = interaction_model_request(
            &store,
            &session,
            &message_event_id,
            "what next?",
            "provider",
            "model",
        )
        .expect("request");

        assert_eq!(request.provider, "provider");
        assert_eq!(request.model, "model");
        assert!(request.messages[1].content.contains("previous answer"));
        assert!(request.messages[1].content.contains("what next?"));
    }

    #[test]
    fn agent_routes_alert_action_to_screen_owner() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let screen = DeviceProfile {
            device_id: DeviceId::from_legacy_named("device_screen").expect("device"),
            node_id: node.clone(),
            display_name: "screen".to_string(),
            kind: DeviceKind::Screen,
            capabilities: Vec::new(),
            permissions: Vec::new(),
            state: DeviceState::available(),
        };
        let context = BoundedContext {
            session_id: SessionId::from_legacy_named("sess_agent").expect("session"),
            events: vec![StoredEvent {
                sequence: sinomo_runtime::events::EventSequence(1),
                appended_at: time::OffsetDateTime::now_utc(),
                event: event("evt_alert", "urgent alert", 0.9),
            }],
            packet: ContextPacket::default(),
        };
        let decision = TriggerDecision {
            trigger_event_id: EventId::from_legacy_named("evt_alert").expect("event"),
            action: TriggerAction::Acknowledge,
            reason: "alert".to_string(),
        };

        let turn =
            InteractiveAgent::new(node.clone(), vec![screen]).handle_trigger(&decision, &context);

        let action = turn.action_request.expect("action");
        assert_eq!(action.target_node_id, Some(node));
        assert_eq!(action.kind, "ui.inspect.alert");
    }

    #[test]
    fn main_agent_loop_requests_json_tool_decision() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_loop").expect("session"),
            NodeId::from_legacy_named("node_loop").expect("node"),
            InteractionSurface::new("surface.loop", InteractionSurfaceKind::CommandLine),
        );
        let latest_event = user_interaction_input_event(&session, "check the cluster later");
        let context = BoundedContext {
            session_id: session.session_id.clone(),
            events: Vec::new(),
            packet: ContextPacket::default(),
        };
        let loop_input = AgentLoopInput {
            session,
            latest_event,
            context,
            async_update: None,
            evolution_context: None,
            tool_specs: default_main_agent_tools(),
        };
        let agent_loop = MainAgentLoop::new(
            FakeInteractionModel {
                response: r#"{
                    "response_mode": "acknowledge",
                    "text": "I will check that in the background.",
                    "tool_calls": [{
                        "tool": "handoff_to_maid",
                        "input": {"reason": "cluster check"},
                        "async": true,
                        "summary": "Ask maid to inspect cluster health."
                    }],
                    "maid_handoff": {
                        "reason": "cluster health check",
                        "evidence_ref": "sifs:/services/tasks/task_1/input.json",
                        "priority": "routine"
                    },
                    "next_state": "delegating"
                }"#,
            },
            "fake",
            "fake",
        );

        let decision = agent_loop.decide(&loop_input).expect("decision");

        assert_eq!(decision.response_mode, AgentLoopResponseMode::Acknowledge);
        assert_eq!(decision.tool_calls[0].tool, "handoff_to_maid");
        assert_eq!(decision.next_state, AgentInteractionState::Delegating);
    }

    #[test]
    fn non_json_model_response_becomes_plain_answer() {
        let decision = parse_agent_loop_decision("A short plain response.").expect("decision");

        assert_eq!(decision.response_mode, AgentLoopResponseMode::Answer);
        assert_eq!(decision.text, "A short plain response.");
        assert!(decision.tool_calls.is_empty());
        assert_eq!(decision.next_state, AgentInteractionState::Listening);
    }

    #[test]
    fn partial_json_model_response_gets_agent_defaults() {
        let decision =
            parse_agent_loop_decision(r#"{"response_mode":"answer","text":"Hello from Sinomo."}"#)
                .expect("decision");

        assert_eq!(decision.response_mode, AgentLoopResponseMode::Answer);
        assert_eq!(decision.text, "Hello from Sinomo.");
        assert!(decision.tool_calls.is_empty());
        assert_eq!(decision.maid_handoff, None);
        assert_eq!(decision.next_state, AgentInteractionState::Listening);
    }

    #[test]
    fn quiet_async_update_does_not_call_model() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_loop").expect("session"),
            NodeId::from_legacy_named("node_loop").expect("node"),
            InteractionSurface::new("surface.loop", InteractionSurfaceKind::CommandLine),
        );
        let latest_event = user_interaction_input_event(&session, "hello");
        let context = BoundedContext {
            session_id: session.session_id.clone(),
            events: Vec::new(),
            packet: ContextPacket::default(),
        };
        let loop_input = AgentLoopInput {
            session,
            latest_event,
            context,
            async_update: Some(AsyncTaskUpdate {
                kind: "async_task_update".to_string(),
                task_id: TaskId::from_legacy_named("task_background").expect("task"),
                status: AsyncTaskStatus::Progress,
                summary: "still running".to_string(),
                needs_user_attention: false,
                result_ref: None,
                next_action_hint: AsyncTaskNextAction::ContinueSilently,
            }),
            evolution_context: None,
            tool_specs: default_main_agent_tools(),
        };
        let agent_loop = MainAgentLoop::new(
            FakeInteractionModel {
                response: "this should not be used",
            },
            "fake",
            "fake",
        );

        let decision = agent_loop.decide(&loop_input).expect("decision");

        assert_eq!(decision.response_mode, AgentLoopResponseMode::Silent);
        assert!(decision.tool_calls.is_empty());
    }
}
