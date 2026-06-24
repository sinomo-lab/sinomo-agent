//! "Sinomo" tab — the AI chat experience.
//!
//! Like the Settings and Files tabs, this is a fully *composed* `sui-widgets`
//! tree: [`view`] returns a responsive host ([`ChatView`]) over pre-built pods
//! (wide split with the agents/sessions sidebar, narrow transcript-only), each
//! assembled from stock `ListView` / `ScrollView` / `Select` / `TextArea` /
//! `Button` / `Separator` / `Label` widgets, plus a small set of
//! bespoke painters for the parts whose *shape* changes live.
//!
//! The data layer — [`ChatState`] + its methods, the [`ChatHandle`] worker
//! thread, the provider/model fetch, and send/stream — is UNCHANGED from the
//! hand-painted version. Only the view is rebuilt.
//!
//! Two regions need care:
//!
//! * **Streaming transcript.** The message list grows when a turn is sent, and
//!   the last (agent) message's text grows token-by-token while streaming —
//!   frequently. Rebuilding the whole transcript per token would be wasteful, so
//!   the transcript is a single live-reading painter ([`TranscriptView`]) that
//!   re-reads `ChatState.messages` in `paint` each frame and draws the bubbles
//!   (the same wrapped-text bubble math the hand-painted UI used). It owns its
//!   own scroll (auto-stick-to-bottom while streaming) exactly as before, and
//!   self-invalidates by diffing a content summary on each `RedrawRequested`.
//!
//! * **Streaming pump.** A worker thread streams tokens back over a channel. An
//!   invisible [`ChatPump`] widget keeps the *existing* request-animation-frame
//!   pump alive: while a reply streams (or there was recent activity) it drains
//!   the worker each frame, applies events to `ChatState`, and re-requests an
//!   animation frame — the same wiring the old `ChatView::pump` had.
//!
//! The agents/sessions sidebar and the composer's provider/model/effort
//! selectors are stock widgets. The model `Select` (and the effort selector's
//! visibility) depend on the current provider + fetched model list, which change
//! at runtime, so the composer row is wrapped in a [`RebuildOnChange`] keyed on
//! a small signature of the selection state.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use sui::{
    Alignment, ArrangeCtx, Color, Constraints, DefaultTheme, Event, EventCtx, FontWeight,
    IconButton, IconGlyph, Insets, KeyState, Label, ListItem, ListView, MeasureCtx, PaintCtx,
    PointerEventKind, Rect, ScrollDelta, SemanticsCtx, Separator, Size, SizedBox, Stack, Surface,
    SurfaceBorder, SurfaceRole, TextArea, Widget, WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor,
    draw_glyph,
};

use sinomo_cli::app_api::{
    AgentSummary, ChatContextSnapshot, ChatHistoryMessage, ChatProcessSnapshot, ChatTaskSnapshot,
    ChatToolCallSnapshot, ChatUsageSnapshot, ProviderSummary, agent_summaries, reasoning_efforts,
};
use sinomo_runtime::{events::SessionId, local::SinomoDataDir};

use crate::backend::Backend;
use crate::backend::chat_engine::{ChatCommand, ChatEvent, ChatHandle, ChatRole, RuntimeInfo};
use crate::backend::sessions::{self, StoredMessage};
use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::widgets;

/// Below this content width the sidebar collapses away (transcript + composer only).
const WIDE_THRESHOLD: f32 = 620.0;
const SIDEBAR_W: f32 = 250.0;
const ACTIVITY_RAIL_W: f32 = 286.0;
const OVERVIEW_H: f32 = 86.0;
const COMPOSER_H: f32 = 132.0;
const PAD: f32 = 16.0;
const SIDEBAR_ROW_H: f32 = 36.0;
const SELECTOR_H: f32 = 30.0;
const COMPOSER_PAD_Y: f32 = 12.0;
const COMPOSER_SECTION_GAP: f32 = 10.0;
const INPUT_ACTION_GAP: f32 = 10.0;
const SEND_BUTTON_SIZE: f32 = 40.0;
const COMPOSER_TEXT_SIZE: f32 = 13.5;
const COMPOSER_LINE_HEIGHT: f32 = 19.0;

/// A shared closure that resolves the current widget theme — threaded into every control so the
/// Appearance toggle restyles the whole tab live, with no rebuild. (Same pattern as the other tabs.)
type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

// ---------------------------------------------------------------------------
// State (data layer) — the worker, send/stream, provider/model fetch, and the
// per-event reducer are UNCHANGED from the hand-painted version. The composed
// view drives state exclusively through the small action methods at the bottom
// of this block (so callbacks stay a clean clone-and-mutate).
// ---------------------------------------------------------------------------

/// One message in the visible transcript.
#[derive(Clone, Debug)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub text: String,
    pub source: ChatMessageSource,
    pub kind: ChatMessageKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatMessageSource {
    User,
    Model,
    System,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatMessageKind {
    Message,
    Info,
    Error,
}

impl ChatMessage {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::User,
            text: text.into(),
            source: ChatMessageSource::User,
            kind: ChatMessageKind::Message,
        }
    }

    pub fn model(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Agent,
            text: text.into(),
            source: ChatMessageSource::Model,
            kind: ChatMessageKind::Message,
        }
    }

    pub fn system_info(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Agent,
            text: text.into(),
            source: ChatMessageSource::System,
            kind: ChatMessageKind::Info,
        }
    }

    pub fn system_error(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Agent,
            text: text.into(),
            source: ChatMessageSource::System,
            kind: ChatMessageKind::Error,
        }
    }

    fn from_stored(message: StoredMessage) -> Self {
        let role = if message.role == "user" {
            ChatRole::User
        } else {
            ChatRole::Agent
        };
        let source = ChatMessageSource::from_stored(message.source.as_deref(), &message.role);
        let kind = ChatMessageKind::from_stored(message.kind.as_deref(), source);
        Self {
            role,
            text: message.text,
            source,
            kind,
        }
    }

    fn to_stored(&self) -> StoredMessage {
        StoredMessage {
            role: self.stored_role().to_string(),
            text: self.text.clone(),
            source: Some(self.source.stored().to_string()),
            kind: Some(self.kind.stored().to_string()),
        }
    }

    fn stored_role(&self) -> &'static str {
        match self.role {
            ChatRole::User => "user",
            ChatRole::Agent => "agent",
        }
    }

    fn history_role(&self) -> &'static str {
        match self.source {
            ChatMessageSource::User => "user",
            ChatMessageSource::Model => "agent",
            ChatMessageSource::System => "system",
        }
    }

    fn is_model_placeholder(&self) -> bool {
        self.role == ChatRole::Agent
            && self.source == ChatMessageSource::Model
            && self.text.trim().is_empty()
    }
}

impl ChatMessageSource {
    fn stored(self) -> &'static str {
        match self {
            ChatMessageSource::User => "user",
            ChatMessageSource::Model => "model",
            ChatMessageSource::System => "system",
        }
    }

    fn from_stored(source: Option<&str>, role: &str) -> Self {
        match source {
            Some("user") => ChatMessageSource::User,
            Some("model") => ChatMessageSource::Model,
            Some("system") => ChatMessageSource::System,
            _ if role == "user" => ChatMessageSource::User,
            _ if role == "system" => ChatMessageSource::System,
            _ => ChatMessageSource::Model,
        }
    }
}

impl ChatMessageKind {
    fn stored(self) -> &'static str {
        match self {
            ChatMessageKind::Message => "message",
            ChatMessageKind::Info => "info",
            ChatMessageKind::Error => "error",
        }
    }

    fn from_stored(kind: Option<&str>, source: ChatMessageSource) -> Self {
        match kind {
            Some("error") => ChatMessageKind::Error,
            Some("info") => ChatMessageKind::Info,
            Some("message") => ChatMessageKind::Message,
            _ if source == ChatMessageSource::System => ChatMessageKind::Info,
            _ => ChatMessageKind::Message,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityTone {
    Neutral,
    Running,
    Good,
    Warn,
    Bad,
}

#[derive(Clone, Debug)]
pub struct ChatActivityEntry {
    pub title: String,
    pub detail: String,
    pub tone: ActivityTone,
}

#[derive(Clone, Debug, Default)]
pub struct ChatControlOverview {
    pub active_turn: bool,
    pub task_count: usize,
    pub terminal_result_count: usize,
    pub steering_count: usize,
    pub follow_up_count: usize,
    pub runtime_attached: bool,
    pub tasks: Vec<ChatTaskSnapshot>,
}

#[derive(Clone, Debug)]
pub struct ChatToolActivity {
    pub step_index: usize,
    pub call_index: Option<usize>,
    pub call_id: Option<String>,
    pub tool: String,
    pub input_preview: String,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RuntimeHealth {
    label: &'static str,
    detail: String,
    tone: ActivityTone,
}

/// A previously-created in-session reference (sidebar list).
pub struct SessionRef {
    pub id: String,
    pub title: String,
}

/// State backing the chat tab.
pub struct ChatState {
    pub data_dir: Arc<SinomoDataDir>,
    pub handle: ChatHandle,
    pub session_id: String,
    pub info: RuntimeInfo,
    pub messages: Vec<ChatMessage>,
    pub input: String,
    pub cursor: usize,
    pub streaming: bool,
    pub notice: Option<String>,
    pub context: Option<ChatContextSnapshot>,
    pub usage: Option<ChatUsageSnapshot>,
    pub activity: Vec<ChatActivityEntry>,
    pub tool_calls: Vec<ChatToolActivity>,
    pub control: ChatControlOverview,
    pub scroll: f32,
    pub stick_bottom: bool,
    pub agents: Vec<AgentSummary>,
    pub providers: Vec<ProviderSummary>,
    pub efforts: Vec<String>,
    pub provider_idx: usize,
    pub model_idx: usize,
    pub effort_idx: usize,
    /// True while a provider's model list is being fetched from sinomo-lm.
    pub models_loading: bool,
    pub sessions: Vec<SessionRef>,
    pending_session_reset: Option<String>,
}

impl ChatState {
    pub fn new(backend: &Backend) -> Self {
        let daemon_connected = backend.connection.is_daemon();
        let handle = ChatHandle::spawn(
            (*backend.data_dir).clone(),
            daemon_connected,
            backend.agent_runtime_control.clone(),
        );

        let agents = agent_summaries(&backend.data_dir);
        let providers = crate::backend::config::cluster_provider_summaries(backend.config.as_ref());
        let efforts = reasoning_efforts();
        let effort_idx = efforts.iter().position(|e| e == "medium").unwrap_or(0);
        let sessions = sessions::list(&backend.data_dir)
            .into_iter()
            .map(|meta| SessionRef {
                id: meta.id,
                title: meta.title,
            })
            .collect();

        let mut state = Self {
            data_dir: backend.data_dir.clone(),
            handle,
            session_id: String::new(),
            info: RuntimeInfo::default(),
            messages: Vec::new(),
            input: String::new(),
            cursor: 0,
            streaming: false,
            notice: None,
            context: None,
            usage: None,
            activity: Vec::new(),
            tool_calls: Vec::new(),
            control: ChatControlOverview::default(),
            scroll: 0.0,
            stick_bottom: true,
            agents,
            providers,
            efforts,
            provider_idx: 0,
            model_idx: 0,
            effort_idx,
            models_loading: false,
            sessions,
            pending_session_reset: None,
        };
        // Eagerly fetch the default/initial provider's models so the model picker is populated
        // without requiring a manual provider toggle. (Results arrive async via `ChatEvent::Models`;
        // a no-op if there is no provider or the list is already populated.)
        state.maybe_fetch_models();
        state
    }

    fn title_from_messages(messages: &[ChatMessage]) -> String {
        messages
            .iter()
            .find(|m| m.role == ChatRole::User)
            .map(|m| {
                let line = m.text.lines().next().unwrap_or("").trim();
                let title: String = line.chars().take(42).collect();
                if title.is_empty() {
                    "New chat".to_string()
                } else {
                    title
                }
            })
            .unwrap_or_else(|| "New chat".to_string())
    }

    /// Persist the current session and refresh the sidebar list.
    fn persist(&mut self) {
        let session_id = self.session_id.clone();
        let messages = self.messages.clone();
        self.persist_messages(&session_id, &messages, true);
    }

    fn persist_messages(
        &mut self,
        session_id: &str,
        messages: &[ChatMessage],
        report_visible_failures: bool,
    ) {
        if session_id.is_empty() || messages.is_empty() {
            return;
        }
        let title = Self::title_from_messages(messages);
        let stored: Vec<StoredMessage> = messages.iter().map(ChatMessage::to_stored).collect();
        match sessions::save_with_report(&self.data_dir, session_id, &title, &stored) {
            Ok(_) => {}
            Err(error) if report_visible_failures => {
                let detail = error.to_string();
                self.notice = Some(format!("session save failed: {detail}"));
                self.push_activity("session save", detail, ActivityTone::Warn);
            }
            Err(_) => {}
        }
        self.sessions = sessions::list(&self.data_dir)
            .into_iter()
            .map(|meta| SessionRef {
                id: meta.id,
                title: meta.title,
            })
            .collect();
    }

    fn push_activity(
        &mut self,
        title: impl Into<String>,
        detail: impl Into<String>,
        tone: ActivityTone,
    ) {
        self.activity.push(ChatActivityEntry {
            title: title.into(),
            detail: detail.into(),
            tone,
        });
        let excess = self.activity.len().saturating_sub(18);
        if excess > 0 {
            self.activity.drain(0..excess);
        }
    }

    fn record_tool_call(
        &mut self,
        prefix: &str,
        call: ChatToolCallSnapshot,
        status: &str,
        tone: ActivityTone,
    ) {
        let mut detail = format!("step {}", call.step_index);
        if let Some(call_index) = call.call_index {
            detail.push_str(&format!(" · call {}", call_index + 1));
        }
        if let Some(input) = call.input_preview.clone().filter(|input| !input.is_empty()) {
            detail.push_str(&format!(" · {input}"));
        }
        let tool_title = call.tool.clone();
        self.upsert_tool_activity_from_snapshot(call, status);
        self.push_activity(format!("{prefix} {tool_title}"), detail, tone);
    }

    fn upsert_tool_activity_from_snapshot(&mut self, call: ChatToolCallSnapshot, status: &str) {
        let input_preview = call.input_preview.unwrap_or_default();
        if let Some(existing) = self.tool_calls.iter_mut().find(|existing| {
            tool_activity_matches(
                existing,
                call.step_index,
                call.call_index,
                call.call_id.as_deref(),
                Some(call.tool.as_str()),
            )
        }) {
            existing.tool = call.tool;
            if !input_preview.trim().is_empty() {
                existing.input_preview = compact_line(&input_preview, 120);
            }
            existing.status = status.to_string();
            return;
        }

        self.tool_calls.push(ChatToolActivity {
            step_index: call.step_index,
            call_index: call.call_index,
            call_id: call.call_id,
            tool: call.tool,
            input_preview: compact_line(&input_preview, 120),
            status: status.to_string(),
        });
        let excess = self.tool_calls.len().saturating_sub(8);
        if excess > 0 {
            self.tool_calls.drain(0..excess);
        }
    }

    fn record_tool_delta(
        &mut self,
        step_index: usize,
        call_id: Option<String>,
        tool: Option<String>,
        arguments_delta: &str,
    ) {
        let Some(existing) = self.tool_calls.iter_mut().find(|existing| {
            tool_activity_matches(
                existing,
                step_index,
                None,
                call_id.as_deref(),
                tool.as_deref(),
            )
        }) else {
            return;
        };
        if let Some(tool) = tool.filter(|tool| !tool.trim().is_empty()) {
            existing.tool = tool;
        }
        if !arguments_delta.trim().is_empty() {
            existing.input_preview = compact_line(
                &format!("{}{}", existing.input_preview, arguments_delta),
                120,
            );
        }
        existing.status = "streaming".to_string();
    }

    fn record_process(&mut self, process: ChatProcessSnapshot) {
        let tone = match process.status.as_str() {
            "completed" => ActivityTone::Good,
            "failed" | "cancelled" => ActivityTone::Bad,
            "running" => ActivityTone::Running,
            _ => ActivityTone::Neutral,
        };
        let mut detail = format!("{} ms", process.elapsed_ms);
        if let Some(code) = process.exit_code {
            detail.push_str(&format!(" · exit {code}"));
        }
        if !process.stderr_tail.trim().is_empty() {
            detail.push_str(" · stderr ");
            detail.push_str(process.stderr_tail.trim());
        } else if !process.stdout_tail.trim().is_empty() {
            detail.push_str(" · stdout ");
            detail.push_str(process.stdout_tail.trim());
        }
        self.push_activity(process.command, detail, tone);
    }

    fn runtime_health(&self) -> RuntimeHealth {
        if let Some(task) = self
            .control
            .tasks
            .iter()
            .find(|task| task_tone(&task.status) == ActivityTone::Bad)
        {
            return RuntimeHealth {
                label: "ATTENTION",
                detail: format!("{}: {}", task.status, compact_line(&task.summary, 72)),
                tone: ActivityTone::Bad,
            };
        }

        if let Some(entry) = self
            .activity
            .iter()
            .rev()
            .find(|entry| entry.tone == ActivityTone::Bad)
        {
            return RuntimeHealth {
                label: "ATTENTION",
                detail: format!(
                    "{}: {}",
                    compact_line(&entry.title, 32),
                    compact_line(&entry.detail, 72)
                ),
                tone: ActivityTone::Bad,
            };
        }

        if let Some(detail) = self.runtime_degraded_detail() {
            return RuntimeHealth {
                label: "DEGRADED",
                detail,
                tone: ActivityTone::Warn,
            };
        }

        if self.streaming || self.control.active_turn {
            return RuntimeHealth {
                label: "RUNNING",
                detail: format!(
                    "{} task(s), {} queued",
                    self.control.task_count,
                    self.control.steering_count + self.control.follow_up_count
                ),
                tone: ActivityTone::Running,
            };
        }

        if let Some(task) = self
            .control
            .tasks
            .iter()
            .find(|task| matches!(task.status.as_str(), "running" | "pending"))
        {
            return RuntimeHealth {
                label: "RUNNING",
                detail: format!("{}: {}", task.status, compact_line(&task.summary, 72)),
                tone: ActivityTone::Running,
            };
        }

        if !self.info.provider.is_empty() && !self.info.model.is_empty() {
            return RuntimeHealth {
                label: "READY",
                detail: format!("{} / {}", self.info.provider, self.info.model),
                tone: ActivityTone::Good,
            };
        }

        RuntimeHealth {
            label: "PENDING",
            detail: "runtime discovery pending".to_string(),
            tone: ActivityTone::Neutral,
        }
    }

    fn runtime_degraded_detail(&self) -> Option<String> {
        if let Some(notice) = self.notice.as_deref()
            && text_mentions_runtime_problem(notice)
        {
            return Some(compact_line(notice, 96));
        }

        if !self.control.runtime_attached
            && (self.control.active_turn
                || self.control.task_count > 0
                || self.control.terminal_result_count > 0
                || self.control.steering_count > 0
                || self.control.follow_up_count > 0)
        {
            return Some("runtime control metadata is detached".to_string());
        }

        self.activity
            .iter()
            .rev()
            .filter(|entry| entry.tone == ActivityTone::Warn)
            .find_map(|entry| {
                let combined = format!("{} {}", entry.title, entry.detail);
                text_mentions_runtime_problem(&combined).then(|| compact_line(&combined, 96))
            })
    }

    fn event_belongs_to_visible_session(&self, session_id: &str) -> bool {
        session_id.is_empty() || self.session_id.is_empty() || self.session_id == session_id
    }

    fn route_background_session_event(&mut self, session_id: &str, event: ChatEvent) -> bool {
        if self.event_belongs_to_visible_session(session_id) {
            return false;
        }

        let mut transcript =
            sessions::load_transcript(&self.data_dir, session_id).unwrap_or_default();
        let mut messages: Vec<ChatMessage> = transcript
            .messages
            .drain(..)
            .map(ChatMessage::from_stored)
            .collect();
        let changed = match event {
            ChatEvent::Token { text, .. } => {
                Self::append_model_token(&mut messages, &text);
                true
            }
            ChatEvent::TurnDone { .. } => Self::finish_turn_messages(&mut messages),
            ChatEvent::Error { error, .. } => {
                Self::apply_error_message(&mut messages, &error);
                true
            }
            _ => false,
        };
        if changed {
            self.persist_messages(session_id, &messages, false);
        }
        true
    }

    fn append_model_token(messages: &mut Vec<ChatMessage>, token: &str) {
        if let Some(last) = messages.last_mut()
            && last.role == ChatRole::Agent
            && last.source == ChatMessageSource::Model
        {
            last.text.push_str(token);
            return;
        }
        messages.push(ChatMessage::model(token));
    }

    fn finish_turn_messages(messages: &mut [ChatMessage]) -> bool {
        if matches!(messages.last(), Some(m) if m.is_model_placeholder())
            && let Some(last) = messages.last_mut()
        {
            last.text = "(no response)".to_string();
            last.source = ChatMessageSource::System;
            last.kind = ChatMessageKind::Info;
            return true;
        }
        false
    }

    fn apply_error_message(messages: &mut Vec<ChatMessage>, error: &str) {
        let text = format!("Error: {error}");
        if let Some(last) = messages.last_mut()
            && last.is_model_placeholder()
        {
            last.text = text;
            last.source = ChatMessageSource::System;
            last.kind = ChatMessageKind::Error;
        } else {
            messages.push(ChatMessage::system_error(text));
        }
    }

    fn reset_visible_session(&mut self, session_id: String) {
        self.session_id = session_id;
        self.messages.clear();
        self.input.clear();
        self.cursor = 0;
        self.streaming = false;
        self.notice = None;
        self.context = None;
        self.usage = None;
        self.activity.clear();
        self.tool_calls.clear();
        self.control = ChatControlOverview::default();
        self.scroll = 0.0;
        self.stick_bottom = true;
    }

    /// Open a stored session: load its transcript for display and tell the worker
    /// to continue under that id.
    fn open_session(&mut self, index: usize) {
        let Some(session) = self.sessions.get(index) else {
            return;
        };
        let id = session.id.clone();
        let transcript = sessions::load_transcript(&self.data_dir, &id).unwrap_or_default();
        let messages: Vec<ChatMessage> = transcript
            .messages
            .into_iter()
            .map(ChatMessage::from_stored)
            .collect();
        let history: Vec<ChatHistoryMessage> = messages
            .iter()
            .map(|m| ChatHistoryMessage {
                role: m.history_role().to_string(),
                text: m.text.clone(),
            })
            .collect();
        self.messages = messages;
        self.session_id = id.clone();
        self.streaming = false;
        self.notice = None;
        self.context = None;
        self.usage = None;
        self.activity.clear();
        self.tool_calls.clear();
        self.control = ChatControlOverview::default();
        self.scroll = f32::MAX;
        self.stick_bottom = true;
        self.handle.send_command(ChatCommand::Resume {
            session_id: id,
            history,
        });
    }

    fn current_provider(&self) -> Option<&ProviderSummary> {
        self.providers.get(self.provider_idx)
    }

    fn current_model(&self) -> String {
        let Some(provider) = self.current_provider() else {
            return self.info.model.clone();
        };
        if let Some(model) = provider.models.get(self.model_idx) {
            return model.clone();
        }
        let runtime_matches_provider = self.info.provider == provider.id
            && self
                .info
                .account
                .as_deref()
                .is_none_or(|account| account == provider.account);
        if runtime_matches_provider {
            self.info.model.clone()
        } else {
            String::new()
        }
    }

    /// Whether the reasoning-effort selector should be shown: only for models that take one.
    fn effort_visible(&self) -> bool {
        let provider = self
            .current_provider()
            .map(|p| p.id.clone())
            .unwrap_or_default();
        let model = self.current_model();
        !provider.is_empty() && sinomo_lm::model_supports_reasoning_effort(&provider, &model)
    }

    /// The current model list for the active provider (for the composer's model `Select`).
    fn current_models(&self) -> Vec<String> {
        self.current_provider()
            .map(|p| p.models.clone())
            .unwrap_or_default()
    }

    fn apply_event(&mut self, event: ChatEvent) -> bool {
        if let Some(session_id) = event.session_id().map(str::to_string)
            && self.route_background_session_event(&session_id, event.clone())
        {
            return false;
        }
        match event {
            ChatEvent::Ready { session_id, info } => {
                self.info = info;
                self.sync_selection_from_info();
                if self.session_id.is_empty() || self.session_id == session_id {
                    self.session_id = session_id;
                }
            }
            ChatEvent::Token { text, .. } => {
                self.notice = None;
                Self::append_model_token(&mut self.messages, &text);
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
            }
            ChatEvent::Notice { notice, .. } => self.notice = Some(notice),
            ChatEvent::Context { context, .. } => {
                self.notice = Some(format!(
                    "context assembled: {}",
                    format_bytes(context.total_bytes)
                ));
                self.context = Some(context);
            }
            ChatEvent::ToolCallStarted { call, .. } => {
                self.record_tool_call("tool", call, "running", ActivityTone::Running);
                self.notice = Some("tool call requested".to_string());
            }
            ChatEvent::ToolCallDelta {
                step_index,
                call_id,
                tool,
                arguments_delta,
                ..
            } => {
                self.record_tool_delta(step_index, call_id.clone(), tool.clone(), &arguments_delta);
                if !arguments_delta.trim().is_empty() {
                    self.push_activity(
                        format!(
                            "tool input {}",
                            tool.unwrap_or_else(|| "stream".to_string())
                        ),
                        format!(
                            "step {step_index} · {}",
                            compact_line(&arguments_delta, 120)
                        ),
                        ActivityTone::Neutral,
                    );
                }
            }
            ChatEvent::ToolCallCompleted { call, .. } => {
                self.record_tool_call("tool done", call, "completed", ActivityTone::Good);
            }
            ChatEvent::Process { process, .. } => {
                self.record_process(process);
            }
            ChatEvent::Usage { usage, .. } => {
                self.usage = Some(usage.clone());
                let input = usage
                    .input_tokens
                    .map(|tokens| tokens.to_string())
                    .unwrap_or_else(|| "?".to_string());
                let output = usage
                    .output_tokens
                    .map(|tokens| tokens.to_string())
                    .unwrap_or_else(|| "?".to_string());
                self.push_activity(
                    "model usage",
                    format!("{} input · {} output", input, output),
                    ActivityTone::Neutral,
                );
            }
            ChatEvent::TurnDone { .. } => {
                self.streaming = false;
                self.control.active_turn = false;
                self.notice = None;
                Self::finish_turn_messages(&mut self.messages);
                self.persist();
            }
            ChatEvent::ControlStatus(status) => {
                self.control.active_turn = status.active_turn;
                self.control.task_count = status.task_count;
                self.control.terminal_result_count = status.terminal_result_count;
                self.control.tasks = status.tasks;
                self.notice = Some(format!(
                    "agent control {} at sequence {}",
                    if status.accepted {
                        "accepted"
                    } else {
                        "rejected"
                    },
                    status.sequence
                ));
                self.push_activity(
                    "runtime control",
                    format!(
                        "{} task(s), {} terminal result(s)",
                        status.task_count, status.terminal_result_count
                    ),
                    if status.accepted {
                        ActivityTone::Good
                    } else {
                        ActivityTone::Warn
                    },
                );
            }
            ChatEvent::ControlState(state) => {
                self.control.active_turn = state.active_turn;
                self.control.task_count = state.task_count;
                self.control.tasks = state.tasks;
                self.control.steering_count = state.steering_count;
                self.control.follow_up_count = state.follow_up_count;
                self.control.runtime_attached = state.runtime_attached;
                self.notice = Some(format!(
                    "agent control: {} steering, {} follow-ups",
                    state.steering_count, state.follow_up_count
                ));
            }
            ChatEvent::RuntimeInfo(info) => {
                self.info = info;
                self.sync_selection_from_info();
            }
            ChatEvent::SessionReset { session_id } => {
                let acknowledged_pending =
                    self.pending_session_reset.as_deref() == Some(session_id.as_str());
                if acknowledged_pending {
                    self.pending_session_reset = None;
                    if !self.messages.is_empty() || self.streaming {
                        self.session_id = session_id;
                        return true;
                    }
                }
                self.session_id = session_id;
                self.messages.clear();
                self.streaming = false;
                self.notice = None;
                self.context = None;
                self.usage = None;
                self.activity.clear();
                self.tool_calls.clear();
                self.control = ChatControlOverview::default();
                self.scroll = 0.0;
            }
            ChatEvent::Models { provider, models } => {
                let mut updated_current = false;
                for (idx, summary) in self.providers.iter_mut().enumerate() {
                    if summary.id == provider {
                        let merged = merge_model_lists(&summary.models, &models);
                        if !merged.is_empty() {
                            summary.models = merged;
                        }
                        updated_current |= idx == self.provider_idx;
                    }
                }
                // Re-align the model index to the current model if it's in the list.
                if updated_current {
                    self.model_idx = self.providers[self.provider_idx]
                        .models
                        .iter()
                        .position(|m| *m == self.info.model)
                        .unwrap_or(0);
                }
                self.models_loading = false;
            }
            ChatEvent::Error { error, .. } => {
                self.streaming = false;
                self.control.active_turn = false;
                self.push_activity("agent error", error.clone(), ActivityTone::Bad);
                Self::apply_error_message(&mut self.messages, &error);
                self.notice = None;
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
                self.persist();
            }
        }
        true
    }

    fn sync_selection_from_info(&mut self) {
        let matched_provider = if let Some(idx) = self.providers.iter().position(|p| {
            p.id == self.info.provider
                && self
                    .info
                    .account
                    .as_deref()
                    .is_none_or(|account| p.account == account)
        }) {
            self.provider_idx = idx;
            if let Some(midx) = self.providers[idx]
                .models
                .iter()
                .position(|m| *m == self.info.model)
            {
                self.model_idx = midx;
            }
            true
        } else {
            false
        };
        if let Some(effort) = &self.info.effort
            && let Some(idx) = self.efforts.iter().position(|e| e == effort)
        {
            self.effort_idx = idx;
        }
        // The runtime just told us which provider is active; if its model list has not been fetched
        // yet, kick the async fetch now so the model picker populates for the default provider
        // (without waiting for the user to toggle providers).
        self.maybe_fetch_models();
        if !matched_provider && !self.providers.is_empty() && !self.current_model().is_empty() {
            self.push_model_selection();
        }
    }

    fn submit(&mut self) {
        let text = self.input.trim().to_string();
        if text.is_empty() {
            return;
        }
        if self.providers.is_empty() {
            self.notice = Some("set up a provider before sending".to_string());
            return;
        }
        if self.streaming {
            self.handle.send_command(ChatCommand::FollowUp {
                session_id: self.session_id.clone(),
                message: text.clone(),
            });
            self.messages.push(ChatMessage::user(text));
            self.input.clear();
            self.cursor = 0;
            self.notice = Some("queued follow-up".to_string());
            self.stick_bottom = true;
            self.scroll = f32::MAX;
            self.persist();
            return;
        }
        self.messages.push(ChatMessage::user(text.clone()));
        self.messages.push(ChatMessage::model(String::new()));
        self.context = None;
        self.usage = None;
        self.activity.clear();
        self.tool_calls.clear();
        self.control.active_turn = true;
        self.input.clear();
        self.cursor = 0;
        self.streaming = true;
        self.stick_bottom = true;
        self.scroll = f32::MAX;
        self.notice = Some("thinking…".to_string());
        self.persist();
        self.handle.send_command(ChatCommand::Send {
            session_id: self.session_id.clone(),
            text,
        });
    }

    fn interrupt_turn(&mut self) {
        if !self.streaming {
            return;
        }
        self.handle.send_command(ChatCommand::Interrupt {
            session_id: self.session_id.clone(),
            reason: "user interrupted from app chat".to_string(),
        });
        self.notice = Some("interrupting…".to_string());
    }

    // -- Composed-view action methods (mutation entry points for the widgets) --

    /// Whether the composer has a non-empty draft that can be submitted.
    fn can_send(&self) -> bool {
        !self.input.trim().is_empty() && !self.providers.is_empty()
    }

    /// Replace the configured provider-account list after Settings changes it.
    pub fn refresh_providers(&mut self, providers: Vec<ProviderSummary>) {
        let previous = self
            .current_provider()
            .map(|provider| (provider.id.clone(), provider.account.clone()));
        self.providers = providers;
        self.models_loading = false;
        if self.providers.is_empty() {
            self.provider_idx = 0;
            self.model_idx = 0;
            return;
        }
        self.provider_idx = previous
            .and_then(|(provider, account)| {
                self.providers
                    .iter()
                    .position(|candidate| candidate.id == provider && candidate.account == account)
            })
            .unwrap_or(0);
        let model_len = self.current_provider().map(|p| p.models.len()).unwrap_or(0);
        if self.model_idx >= model_len {
            self.model_idx = 0;
        }
        self.maybe_fetch_models();
        if !self.current_model().is_empty() {
            self.push_model_selection();
        }
    }

    /// Update the composer draft (bound to the composer `TextArea` via its `on_change`).
    fn set_draft(&mut self, text: String) {
        self.cursor = text.len();
        self.input = text;
    }

    /// Select an agent by sidebar index and tell the worker to switch.
    fn select_agent(&mut self, index: usize) {
        if let Some(agent) = self.agents.get(index) {
            let key = agent.key.clone();
            self.handle.send_command(ChatCommand::SetAgent(key));
        }
    }

    /// Whether `index` is the currently-active agent (matched by key or display name).
    fn is_active_agent(&self, index: usize) -> bool {
        self.agents
            .get(index)
            .map(|a| a.name == self.info.agent || a.key == self.info.agent)
            .unwrap_or(false)
    }

    /// Start a fresh session (the "New chat" sidebar row).
    fn new_session(&mut self) {
        self.persist();
        let interrupted_session_id = self.session_id.clone();
        let session_id = SessionId::generate().to_string();
        if self.streaming || self.control.active_turn {
            self.handle.send_command(ChatCommand::Interrupt {
                session_id: interrupted_session_id,
                reason: "user started a new chat from app chat".to_string(),
            });
        }
        self.reset_visible_session(session_id.clone());
        self.pending_session_reset = Some(session_id.clone());
        self.handle
            .send_command(ChatCommand::NewSession { session_id });
    }

    /// Push the current provider/model selection to the worker.
    fn push_model_selection(&mut self) {
        let provider = self
            .current_provider()
            .map(|p| p.id.clone())
            .unwrap_or_default();
        let account = self
            .current_provider()
            .map(|p| p.account.clone())
            .unwrap_or_else(|| sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string());
        let model = self.current_model();
        if !provider.is_empty() {
            self.handle.send_command(ChatCommand::SetModel {
                provider,
                model,
                account,
            });
        }
    }

    /// Select a provider by index: reset the model to the first, fetch its models if needed, and
    /// push the new selection to the worker.
    fn set_provider(&mut self, index: usize) {
        if index >= self.providers.len() {
            return;
        }
        self.provider_idx = index;
        self.model_idx = 0;
        self.maybe_fetch_models();
        self.push_model_selection();
    }

    /// Select a model by index within the current provider and push it to the worker.
    fn set_model(&mut self, index: usize) {
        let len = self.current_provider().map(|p| p.models.len()).unwrap_or(0);
        if index < len {
            self.model_idx = index;
            self.push_model_selection();
        }
    }

    /// Select a reasoning-effort option by index.
    fn set_effort(&mut self, index: usize) {
        if index < self.efforts.len() {
            self.effort_idx = index;
        }
    }

    /// Ask the worker to fetch the current provider's models from sinomo-lm, unless they are already
    /// loaded or a fetch is in flight. Results arrive via [`ChatEvent::Models`].
    fn maybe_fetch_models(&mut self) {
        if self.models_loading {
            return;
        }
        let needs_fetch = self
            .current_provider()
            .map(|p| p.models.len() <= 1)
            .unwrap_or(false);
        if !needs_fetch {
            return;
        }
        if let Some(provider) = self.current_provider().map(|p| p.id.clone()) {
            self.models_loading = true;
            self.handle
                .send_command(ChatCommand::FetchModels { provider });
        }
    }
}

fn merge_model_lists(existing: &[String], fetched: &[String]) -> Vec<String> {
    let mut models = Vec::with_capacity(existing.len() + fetched.len());
    for model in existing.iter().chain(fetched.iter()) {
        if model.trim().is_empty() || models.iter().any(|candidate| candidate == model) {
            continue;
        }
        models.push(model.clone());
    }
    models
}

// ---------------------------------------------------------------------------
// Theme reader + small helpers (mirrors the Settings / Files tabs).
// ---------------------------------------------------------------------------

/// Clone the shared theme reader into a fresh `Fn() -> DefaultTheme` for one control.
fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

fn composer_control_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || {
        let mut theme = reader();
        theme.typography.body_font_size = COMPOSER_TEXT_SIZE;
        theme.typography.body_line_height = COMPOSER_LINE_HEIGHT;
        theme
    }
}

/// Resolve the app chrome palette for the current theme (used by the `Label` color readers and the
/// bespoke painters so muted/accent text matches the chrome).
fn palette(shared: &Shared) -> Palette {
    Palette::from_theme(&shared.theme(), widgets::theme_is_dark(shared))
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f32 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f32 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn compact_line(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    let head: String = collapsed
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect();
    format!("{head}...")
}

fn tone_color(pal: &Palette, tone: ActivityTone) -> sui::Color {
    match tone {
        ActivityTone::Neutral => pal.text_faint,
        ActivityTone::Running => pal.accent,
        ActivityTone::Good => pal.good,
        ActivityTone::Warn => pal.warn,
        ActivityTone::Bad => pal.bad,
    }
}

fn task_tone(status: &str) -> ActivityTone {
    match status {
        "running" | "pending" => ActivityTone::Running,
        "completed" => ActivityTone::Good,
        "failed" | "cancelled" | "timed-out" => ActivityTone::Bad,
        _ => ActivityTone::Neutral,
    }
}

fn tool_status_tone(status: &str) -> ActivityTone {
    match status {
        "running" | "streaming" => ActivityTone::Running,
        "completed" => ActivityTone::Good,
        "failed" | "cancelled" | "timed-out" => ActivityTone::Bad,
        _ => ActivityTone::Neutral,
    }
}

fn tool_activity_matches(
    existing: &ChatToolActivity,
    step_index: usize,
    call_index: Option<usize>,
    call_id: Option<&str>,
    tool: Option<&str>,
) -> bool {
    if existing.step_index != step_index {
        return false;
    }
    if let (Some(existing_id), Some(call_id)) = (existing.call_id.as_deref(), call_id) {
        return existing_id == call_id;
    }
    if let (Some(existing_index), Some(call_index)) = (existing.call_index, call_index) {
        return existing_index == call_index;
    }
    if let Some(tool) = tool {
        return existing.tool == tool;
    }
    false
}

fn text_mentions_runtime_problem(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "sifs mirror failed",
        "dfs",
        "sandbox",
        "policy",
        "unavailable",
        "denied",
        "rejected",
        "blocked",
        "timed out",
        "timeout",
        "failed",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// A small uppercase section heading `Label` (AGENTS / SESSIONS), color tracking the theme.
fn heading(shared: &Shared, text: &str) -> Label {
    let s = shared.clone();
    Label::new(text)
        .style({
            let theme = shared.theme();
            sui::TextStyle {
                font_size: 11.0,
                line_height: 18.0,
                weight: FontWeight::SEMIBOLD,
                ..theme.body_text_style()
            }
        })
        .color_when(move || palette(&s).text_faint)
}

// ---------------------------------------------------------------------------
// Sidebar — agents + sessions ListViews (rebuilt when the lists change).
// ---------------------------------------------------------------------------

/// Build the AGENTS ListView: one row per agent, the active one selected, selecting one switches.
fn agents_list(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let items: Vec<ListItem> = shared
        .chat()
        .agents
        .iter()
        .map(|agent| ListItem::new(agent.name.clone()).leading_text("✦"))
        .collect();
    let s = shared.clone();
    ListView::new("chat-agents")
        .items(items)
        .row_height(SIDEBAR_ROW_H)
        .theme_when(clone_reader(reader))
        .selected_when({
            let s = s.clone();
            move || {
                let chat = s.chat();
                (0..chat.agents.len()).find(|i| chat.is_active_agent(*i))
            }
        })
        .on_change(move |index, _label| {
            s.chat_mut().select_agent(index);
        })
}

/// Build the SESSIONS ListView: a "New chat" row followed by session history. Selecting row 0
/// starts a fresh session; later rows open that stored session.
fn sessions_list(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let mut items = vec![ListItem::new("New chat").leading_text("+")];
    items.extend(
        shared
            .chat()
            .sessions
            .iter()
            .map(|session| ListItem::new(session.title.clone()).leading_text("◷")),
    );
    let s = shared.clone();
    ListView::new("chat-sessions")
        .items(items)
        // The session list is a drill-in action list, not a persistent selection: never keep a
        // row highlighted (mirrors the Settings narrow menu).
        .selected_when(|| None::<usize>)
        .row_height(SIDEBAR_ROW_H)
        .theme_when(clone_reader(reader))
        .on_change(move |index, _label| {
            let mut chat = s.chat_mut();
            if index == 0 {
                chat.new_session();
            } else {
                chat.open_session(index - 1);
            }
        })
}

/// The sidebar column: AGENTS heading + list, then SESSIONS heading + list. Wrapped in a
/// [`RebuildOnChange`] keyed on the agent/session signature so it rebuilds when those lists change
/// (e.g. a new session is persisted) but not on every token.
fn sidebar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || sidebar_signature(&s)
        },
        move |_sig| {
            let column = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(SizedBox::new().height(18.0))
                .with_child(InsetX::new(heading(&s, "AGENTS"), 18.0))
                .with_child(SizedBox::new().height(2.0))
                .with_child(InsetX::new(agents_list(&s, &r), 8.0))
                .with_child(SizedBox::new().height(16.0))
                .with_child(InsetX::new(heading(&s, "SESSIONS"), 18.0))
                .with_child(SizedBox::new().height(2.0))
                .with_child(InsetX::new(sessions_list(&s, &r), 8.0));
            WidgetPod::new(Surface::sidebar(column).fill().theme_when(clone_reader(&r)))
        },
    )
}

/// A signature of the sidebar's list contents (agents are static; sessions + the active agent
/// change). Used to drive the sidebar [`RebuildOnChange`].
fn sidebar_signature(shared: &Shared) -> (String, String) {
    let chat = shared.chat();
    let active = chat
        .agents
        .iter()
        .enumerate()
        .find(|(i, _)| chat.is_active_agent(*i))
        .map(|(_, a)| a.key.clone())
        .unwrap_or_default();
    let sessions = chat
        .sessions
        .iter()
        .map(|s| s.title.as_str())
        .collect::<Vec<_>>()
        .join("\u{1f}");
    (active, sessions)
}

struct RuntimeOverview {
    shared: Shared,
}

impl RuntimeOverview {
    fn new(shared: Shared) -> Self {
        Self { shared }
    }
}

impl Widget for RuntimeOverview {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(constraints.max.width, OVERVIEW_H))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        ctx.fill_bounds(pal.titlebar);
        widgets::hairline_bottom(ctx, bounds, pal.border);

        let chat = self.shared.chat();
        let title_style = pal.text_style(&theme, 12.5, FontWeight::SEMIBOLD, pal.text);
        let meta_style = pal.text_style(&theme, 11.5, FontWeight::NORMAL, pal.text_faint);
        let label_style = pal.text_style(&theme, 10.5, FontWeight::MEDIUM, pal.text_muted);
        let x = bounds.x() + PAD;
        let top = bounds.y() + 10.0;
        let health = chat.runtime_health();
        let chip_w = 112.0_f32.min((bounds.width() * 0.34).max(92.0));
        let chip = Rect::new(
            bounds.x() + bounds.width() - PAD - chip_w,
            top - 1.0,
            chip_w,
            20.0,
        );
        draw_health_chip(ctx, &pal, &theme, chip, &health);
        let title_w = (chip.x() - x - 10.0).max(80.0);
        let runtime = if chat.info.provider.is_empty() || chat.info.model.is_empty() {
            "runtime pending".to_string()
        } else {
            format!("{} / {}", chat.info.provider, chat.info.model)
        };
        widgets::draw_text_in(ctx, Rect::new(x, top, title_w, 18.0), &runtime, title_style);

        let usage = chat
            .usage
            .as_ref()
            .map(|usage| {
                let output = usage
                    .output_tokens
                    .map(format_token_count)
                    .unwrap_or_else(|| "?".to_string());
                let input = usage
                    .input_tokens
                    .map(format_token_count)
                    .unwrap_or_else(|| "?".to_string());
                format!("{input} in · {output} out")
            })
            .unwrap_or_else(|| "usage pending".to_string());
        let monitor = format!(
            "{} · {} tasks · {} queued",
            if chat.streaming || chat.control.active_turn {
                "active"
            } else {
                "idle"
            },
            chat.control.task_count,
            chat.control.steering_count + chat.control.follow_up_count
        );
        let monitor = if matches!(health.tone, ActivityTone::Warn | ActivityTone::Bad) {
            format!("{} · {monitor}", compact_line(&health.detail, 42))
        } else {
            monitor
        };
        widgets::draw_text_in(
            ctx,
            Rect::new(x, top + 20.0, title_w, 16.0),
            &format!("{monitor} · {usage}"),
            meta_style,
        );

        let bar = Rect::new(
            x,
            bounds.y() + 58.0,
            (bounds.width() - PAD * 2.0).max(0.0),
            10.0,
        );
        widgets::fill_round_rect(ctx, bar, pal.field, 5.0);
        if let Some(context) = &chat.context {
            let denominator = context
                .max_context_bytes
                .or(context.compaction_threshold_bytes)
                .unwrap_or(context.total_bytes.max(1))
                .max(1);
            let mut offset = 0.0;
            let used_w = (bar.width() * (context.total_bytes as f32 / denominator as f32))
                .clamp(2.0, bar.width());
            let lane_total: usize = context.lanes.iter().map(|lane| lane.bytes).sum();
            if lane_total > 0 {
                for lane in &context.lanes {
                    if lane.bytes == 0 {
                        continue;
                    }
                    let lane_w = (used_w * lane.bytes as f32 / lane_total as f32)
                        .max(2.0)
                        .min(used_w - offset);
                    if lane_w <= 0.0 {
                        continue;
                    }
                    let segment = Rect::new(bar.x() + offset, bar.y(), lane_w, bar.height());
                    widgets::fill_round_rect(
                        ctx,
                        segment,
                        context_lane_color(&pal, lane.kind.as_str()),
                        5.0,
                    );
                    offset += lane_w;
                    if offset >= used_w {
                        break;
                    }
                }
            }
            widgets::draw_text_in(
                ctx,
                Rect::new(x, bounds.y() + 40.0, bounds.width() - PAD * 2.0, 16.0),
                &format!(
                    "context {} / {} · {} source(s) · {} omitted",
                    format_bytes(context.total_bytes),
                    format_bytes(denominator),
                    context.included_count,
                    context.omitted_count
                ),
                label_style,
            );
        } else {
            widgets::draw_text_in(
                ctx,
                Rect::new(x, bounds.y() + 40.0, bounds.width() - PAD * 2.0, 16.0),
                "context pending",
                label_style,
            );
        }
    }
}

fn draw_health_chip(
    ctx: &mut PaintCtx,
    pal: &Palette,
    theme: &DefaultTheme,
    rect: Rect,
    health: &RuntimeHealth,
) {
    widgets::fill_round_rect(ctx, rect, pal.field, 6.0);
    let color = tone_color(pal, health.tone);
    widgets::fill_round_rect(
        ctx,
        Rect::new(rect.x() + 8.0, rect.y() + 7.0, 6.0, 6.0),
        color,
        3.0,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 20.0, rect.y() + 2.0, rect.width() - 24.0, 16.0),
        health.label,
        pal.text_style(theme, 10.0, FontWeight::SEMIBOLD, color),
    );
}

fn context_lane_color(pal: &Palette, kind: &str) -> Color {
    match kind {
        "fixed_prompt" => pal.accent,
        "current_work" | "context_attachments" | "async_update" => pal.good,
        "recent_tail" => pal.warn,
        "retrieved_memories" => pal.selected,
        "old_summaries" => pal.text_faint,
        _ => pal.border_strong,
    }
}

fn format_token_count(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}m", tokens as f32 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k", tokens as f32 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn context_lane_summary(label: &str, bytes: usize, total: usize) -> String {
    let share = if total == 0 {
        0.0
    } else {
        bytes as f32 * 100.0 / total as f32
    };
    format!(
        "{} · {} · {:.0}%",
        compact_line(label, 28),
        format_bytes(bytes),
        share
    )
}

struct ActivityRail {
    shared: Shared,
}

impl ActivityRail {
    fn new(shared: Shared) -> Self {
        Self { shared }
    }
}

impl Widget for ActivityRail {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.max
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        ctx.fill_bounds(pal.sidebar);
        widgets::hairline_left(ctx, bounds, pal.border);

        let chat = self.shared.chat();
        let x = bounds.x() + 14.0;
        let w = (bounds.width() - 28.0).max(0.0);
        let heading_style = pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_muted);
        let text_style = pal.text_style(&theme, 12.0, FontWeight::MEDIUM, pal.text);
        let detail_style = pal.text_style(&theme, 10.5, FontWeight::NORMAL, pal.text_faint);
        widgets::draw_text_in(
            ctx,
            Rect::new(x, bounds.y() + 14.0, w, 16.0),
            "ACTIVITY",
            heading_style.clone(),
        );

        let mut y = bounds.y() + 42.0;
        if chat.control.tasks.is_empty()
            && chat.tool_calls.is_empty()
            && chat.activity.is_empty()
            && chat.context.is_none()
        {
            widgets::draw_text_in(
                ctx,
                Rect::new(x, y, w, 18.0),
                "No runtime activity yet",
                detail_style,
            );
            return;
        }

        if !chat.control.tasks.is_empty() {
            widgets::draw_text_in(
                ctx,
                Rect::new(x, y, w, 14.0),
                "TASKS",
                heading_style.clone(),
            );
            y += 22.0;
            for task in chat.control.tasks.iter().take(4) {
                if y + 42.0 > bounds.y() + bounds.height() {
                    return;
                }
                let dot = Rect::new(x, y + 5.0, 7.0, 7.0);
                widgets::fill_round_rect(ctx, dot, tone_color(&pal, task_tone(&task.status)), 3.5);
                let detail = task
                    .progress_summary
                    .as_deref()
                    .or(task.result_ref.as_deref())
                    .unwrap_or(task.kind.as_str());
                widgets::draw_text_in(
                    ctx,
                    Rect::new(x + 14.0, y, w - 14.0, 16.0),
                    &compact_line(&task.summary, 34),
                    text_style.clone(),
                );
                widgets::draw_text_in(
                    ctx,
                    Rect::new(x + 14.0, y + 17.0, w - 14.0, 16.0),
                    &format!("{} · {}", task.status, compact_line(detail, 40)),
                    detail_style.clone(),
                );
                y += 42.0;
            }
            y += 4.0;
        }

        if !chat.tool_calls.is_empty() {
            widgets::draw_text_in(
                ctx,
                Rect::new(x, y, w, 14.0),
                "TOOLS",
                heading_style.clone(),
            );
            y += 22.0;
            for call in chat.tool_calls.iter().rev().take(3) {
                if y + 42.0 > bounds.y() + bounds.height() {
                    return;
                }
                let dot = Rect::new(x, y + 5.0, 7.0, 7.0);
                widgets::fill_round_rect(
                    ctx,
                    dot,
                    tone_color(&pal, tool_status_tone(&call.status)),
                    3.5,
                );
                let mut detail = format!("{} · step {}", call.status, call.step_index);
                if let Some(input) = (!call.input_preview.trim().is_empty())
                    .then(|| compact_line(&call.input_preview, 30))
                {
                    detail.push_str(" · ");
                    detail.push_str(&input);
                }
                let detail = compact_line(&detail, 34);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(x + 14.0, y, w - 14.0, 16.0),
                    &compact_line(&call.tool, 34),
                    text_style.clone(),
                );
                widgets::draw_text_in(
                    ctx,
                    Rect::new(x + 14.0, y + 17.0, w - 14.0, 16.0),
                    &detail,
                    detail_style.clone(),
                );
                y += 42.0;
            }
            y += 4.0;
        }

        if let Some(context) = &chat.context {
            if y + 40.0 > bounds.y() + bounds.height() {
                return;
            }
            widgets::draw_text_in(
                ctx,
                Rect::new(x, y, w, 14.0),
                "CONTEXT",
                heading_style.clone(),
            );
            y += 20.0;

            let lane_total: usize = context.lanes.iter().map(|lane| lane.bytes).sum();
            let visible_lane_count = context.lanes.iter().filter(|lane| lane.bytes > 0).count();
            let mut drawn = 0usize;
            for lane in context.lanes.iter().filter(|lane| lane.bytes > 0).take(4) {
                if y + 24.0 > bounds.y() + bounds.height() {
                    return;
                }
                let dot = Rect::new(x, y + 5.0, 7.0, 7.0);
                widgets::fill_round_rect(
                    ctx,
                    dot,
                    context_lane_color(&pal, lane.kind.as_str()),
                    3.5,
                );
                let label = if lane.label.trim().is_empty() {
                    lane.kind.as_str()
                } else {
                    lane.label.as_str()
                };
                widgets::draw_text_in(
                    ctx,
                    Rect::new(x + 14.0, y, w - 14.0, 16.0),
                    &context_lane_summary(label, lane.bytes, lane_total),
                    detail_style.clone(),
                );
                y += 22.0;
                drawn += 1;
            }
            if visible_lane_count > drawn {
                widgets::draw_text_in(
                    ctx,
                    Rect::new(x + 14.0, y, w - 14.0, 16.0),
                    &format!("{} more lane(s)", visible_lane_count - drawn),
                    detail_style.clone(),
                );
                y += 20.0;
            }
            y += 4.0;
        }

        if chat.activity.is_empty() {
            return;
        }

        widgets::draw_text_in(ctx, Rect::new(x, y, w, 14.0), "EVENTS", heading_style);
        y += 22.0;
        for entry in chat.activity.iter().rev().take(10) {
            if y + 48.0 > bounds.y() + bounds.height() {
                break;
            }
            let dot = Rect::new(x, y + 5.0, 7.0, 7.0);
            widgets::fill_round_rect(ctx, dot, tone_color(&pal, entry.tone), 3.5);
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 14.0, y, w - 14.0, 16.0),
                &compact_line(&entry.title, 34),
                text_style.clone(),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 14.0, y + 17.0, w - 14.0, 16.0),
                &compact_line(&entry.detail, 54),
                detail_style.clone(),
            );
            y += 44.0;
        }
    }
}

// ---------------------------------------------------------------------------
// Transcript — a live-reading painter that owns its own scroll (auto-stick to
// bottom while streaming). Re-reads `ChatState.messages` each `paint`, so a
// streamed token never rebuilds the widget tree. The repaint each streaming
// frame is driven by `ChatView`'s pump (which `request_paint`s as it drains
// tokens); a parent repaint repaints this whole subtree, so the transcript
// stays in sync without its own self-invalidation. The bubble layout/measure
// math is the same the hand-painted UI used.
// ---------------------------------------------------------------------------

struct TranscriptView {
    shared: Shared,
    content_height: Cell<f32>,
    viewport_height: Cell<f32>,
    bounds: Cell<Rect>,
}

impl TranscriptView {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            content_height: Cell::new(0.0),
            viewport_height: Cell::new(0.0),
            bounds: Cell::new(Rect::ZERO),
        }
    }
}

impl Widget for TranscriptView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(p)
                if p.kind == PointerEventKind::Scroll && self.bounds.get().contains(p.position) =>
            {
                if let Some(delta) = p.scroll_delta {
                    let dy = match delta {
                        ScrollDelta::Lines(v) => v.y * 48.0,
                        ScrollDelta::Pixels(v) => v.y,
                    };
                    let max = (self.content_height.get() - self.viewport_height.get()).max(0.0);
                    let mut chat = self.shared.chat_mut();
                    chat.scroll = (chat.scroll - dy).clamp(0.0, max);
                    chat.stick_bottom = chat.scroll >= max - 2.0;
                    drop(chat);
                    ctx.request_paint();
                }
            }
            _ => {}
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.max
    }

    fn arrange(&mut self, _ctx: &mut ArrangeCtx, bounds: Rect) {
        self.bounds.set(bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let area = ctx.bounds();
        self.bounds.set(area);
        self.viewport_height.set(area.height());

        let chat = self.shared.chat();
        if chat.messages.is_empty() {
            self.paint_empty_state(ctx, &theme, &pal, area, &chat);
            return;
        }

        ctx.push_clip_rect(area);

        let max_bubble = (area.width() * 0.74).clamp(200.0, 620.0);
        let inner_w = max_bubble - 24.0;
        let body = pal.text_style(&theme, 14.0, FontWeight::NORMAL, pal.text);
        let line_h = body.line_height;

        // Measure pass: wrap each message and compute its bubble box (releases the immutable
        // `measure` borrow of `ctx` before drawing).
        struct Laid {
            is_user: bool,
            kind: ChatMessageKind,
            lines: Vec<String>,
            bubble_w: f32,
            bubble_h: f32,
        }
        let mut laid: Vec<Laid> = Vec::with_capacity(chat.messages.len());
        let mut total = PAD;
        {
            let measure = |s: &str| {
                ctx.measure_text(s, body.clone())
                    .map(|m| m.width)
                    .unwrap_or(0.0)
            };
            for msg in &chat.messages {
                let is_user = msg.role == ChatRole::User;
                let text = if msg.text.is_empty() {
                    "…".to_string()
                } else {
                    msg.text.clone()
                };
                let lines = widgets::wrap_lines(&text, inner_w, measure);
                let widest = lines.iter().map(|l| measure(l)).fold(0.0_f32, f32::max);
                let bubble_w = widest.min(inner_w) + 24.0;
                let bubble_h = lines.len().max(1) as f32 * line_h + 20.0;
                total += bubble_h + 12.0;
                laid.push(Laid {
                    is_user,
                    kind: msg.kind,
                    lines,
                    bubble_w,
                    bubble_h,
                });
            }
        }
        total += PAD;
        self.content_height.set(total);

        let max_scroll = (total - area.height()).max(0.0);
        let scroll = if chat.stick_bottom {
            max_scroll
        } else {
            chat.scroll.clamp(0.0, max_scroll)
        };

        // Draw pass: only mutable ctx ops here.
        let mut y = area.y() + PAD - scroll;
        for item in &laid {
            let is_error = item.kind == ChatMessageKind::Error;
            let (bx, bubble_color, text_color, border) = if item.is_user {
                (
                    area.x() + area.width() - PAD - item.bubble_w,
                    pal.accent,
                    pal.on_accent,
                    None,
                )
            } else if is_error {
                (area.x() + PAD, pal.panel, pal.text, Some(pal.bad))
            } else {
                (area.x() + PAD, pal.panel, pal.text, Some(pal.border))
            };
            if y + item.bubble_h >= area.y() && y <= area.y() + area.height() {
                let bubble = Rect::new(bx, y, item.bubble_w, item.bubble_h);
                // C4 elevation: a very subtle drop shadow lifts each message bubble off the
                // transcript background. Drawn BEFORE the fill so it sits behind. We use the toolkit's
                // smallest box-shadow token (~5% black, 2px blur) so the lift stays on the theme's own
                // elevation scale — restrained, since the agent bubbles also carry a border.
                sui::paint_theme_shadow(ctx, bubble, [12.0; 4], &theme.shadows.box_shadow.xs);
                match border {
                    Some(bc) => widgets::round_panel(ctx, bubble, bubble_color, bc, 12.0),
                    None => widgets::fill_round_rect(ctx, bubble, bubble_color, 12.0),
                }
                let style = sui::TextStyle {
                    color: text_color,
                    ..body.clone()
                };
                let mut ty = y + 10.0;
                for line in &item.lines {
                    ctx.draw_text(
                        Rect::new(bx + 12.0, ty, item.bubble_w - 24.0, line_h + 2.0),
                        line,
                        style.clone(),
                    );
                    ty += line_h;
                }
            }
            y += item.bubble_h + 12.0;
        }
        ctx.pop_clip();

        // Streaming / typing notice, pinned to the bottom of the transcript.
        if chat.streaming
            && let Some(notice) = &chat.notice
        {
            let ns = pal.text_style(&theme, 11.5, FontWeight::MEDIUM, pal.text_faint);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    area.x() + PAD,
                    area.y() + area.height() - 22.0,
                    area.width() - PAD * 2.0,
                    18.0,
                ),
                notice,
                ns,
            );
        }
    }
}

impl TranscriptView {
    fn paint_empty_state(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        area: Rect,
        chat: &ChatState,
    ) {
        let cx = area.x() + area.width() / 2.0;
        let cy = area.y() + area.height() / 2.0;
        // A real Lucide Sparkles glyph for the empty-state mark (tinted to the muted chrome color),
        // replacing the arbitrary ✦ text rune.
        let mark_side = 40.0;
        draw_glyph(
            ctx,
            IconGlyph::Sparkles,
            Rect::new(
                cx - mark_side / 2.0,
                cy - 40.0 - mark_side / 2.0,
                mark_side,
                mark_side,
            ),
            pal.text_faint,
        );
        let title = pal.text_style(theme, 17.0, FontWeight::SEMIBOLD, pal.text_muted);
        let agent = if chat.info.agent.is_empty() {
            "Sinomo"
        } else {
            chat.info.agent.as_str()
        };
        widgets::draw_centered(ctx, &format!("Chat with {agent}"), title, cx, cy + 2.0);
        let sub = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text_faint);
        widgets::draw_centered(ctx, "Type a message below to begin.", sub, cx, cy + 26.0);
    }
}

// ---------------------------------------------------------------------------
// Composer — provider / model / effort `Select`s, the message `TextArea`, and a
// Send `Button`. The model `Select` (and effort visibility) depend on the
// provider + fetched model list, so the selector row is wrapped in a
// `RebuildOnChange` keyed on the selection signature.
// ---------------------------------------------------------------------------

/// The selector row signature: provider index, model index, model count, effort visibility, and a
/// loading flag — anything that changes the *set* of options or which selectors are shown. (The
/// selected-index reactivity is handled by each `Select`'s `selected_when`; this only drives the
/// rebuild when the option lists themselves change.)
fn selector_signature(shared: &Shared) -> (usize, usize, usize, bool, bool, String) {
    let chat = shared.chat();
    (
        chat.provider_idx,
        chat.providers.len(),
        chat.current_models().len(),
        chat.effort_visible(),
        chat.models_loading,
        chat.providers
            .iter()
            .map(|provider| format!("{}@{}:{}", provider.id, provider.account, provider.name))
            .collect::<Vec<_>>()
            .join("\u{1f}"),
    )
}

/// Build the provider / model / effort selector row for the current selection.
fn selectors(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    if shared.chat().providers.is_empty() {
        let setup = {
            let s = shared.clone();
            sui::Button::new("Setup provider")
                .icon(IconGlyph::Add)
                .theme_when(composer_control_reader(reader))
                .on_press(move || open_provider_setup(&s))
        };
        let row = Stack::horizontal()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(SizedBox::new().height(SELECTOR_H).with_child(setup));
        return WidgetPod::new(row);
    }

    let provider = {
        let s = shared.clone();
        let names: Vec<String> = shared
            .chat()
            .providers
            .iter()
            .map(|p| p.name.clone())
            .collect();
        sui::Select::new("chat-provider")
            .options(names)
            .placeholder("Provider")
            .theme_when(composer_control_reader(reader))
            .selected_when({
                let s = s.clone();
                move || Some(s.chat().provider_idx)
            })
            .on_change(move |index, _label| {
                s.chat_mut().set_provider(index);
            })
    };

    let model = {
        let s = shared.clone();
        let models = shared.chat().current_models();
        let loading = shared.chat().models_loading;
        let placeholder = if loading {
            "Loading models…"
        } else if models.is_empty() {
            "No models"
        } else {
            "Model"
        };
        sui::Select::new("chat-model")
            .options(models)
            .placeholder(placeholder)
            .theme_when(composer_control_reader(reader))
            .selected_when({
                let s = s.clone();
                move || Some(s.chat().model_idx)
            })
            .on_change(move |index, _label| {
                s.chat_mut().set_model(index);
            })
    };

    let mut row = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center)
        .with_child(SizedBox::new().height(SELECTOR_H).with_child(provider))
        .with_child(SizedBox::new().height(SELECTOR_H).with_child(model));

    if shared.chat().effort_visible() {
        let s = shared.clone();
        let efforts = shared.chat().efforts.clone();
        let effort = sui::Select::new("chat-effort")
            .options(efforts)
            .placeholder("Effort")
            .theme_when(composer_control_reader(reader))
            .selected_when({
                let s = s.clone();
                move || Some(s.chat().effort_idx)
            })
            .on_change(move |index, _label| {
                s.chat_mut().set_effort(index);
            });
        row = row.with_child(SizedBox::new().height(SELECTOR_H).with_child(effort));
    }

    WidgetPod::new(row)
}

fn open_provider_setup(shared: &Shared) {
    let mut app = shared.borrow_mut();
    app.active_tab = Tab::Settings;
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.open_provider_add();
    }
}

/// The composer's bottom row: a flexible message `TextArea` and a fixed-size action slot. The input
/// is bound to the draft via `on_change`; Enter submits normally, or queues a follow-up during an
/// active stream. The action control swaps from Send to Interrupt while streaming.
fn input_row(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let input = {
        let s = shared.clone();
        let draft = shared.chat().input.clone();
        TextArea::new("chat-input")
            .value(draft)
            .placeholder("Message Sinomo…  (⏎ to send, ⇧⏎ for newline)")
            .padding(Insets {
                left: 12.0,
                top: 8.0,
                right: 12.0,
                bottom: 8.0,
            })
            .theme_when(composer_control_reader(reader))
            .on_change({
                let s = s.clone();
                move |value| {
                    s.chat_mut().set_draft(value);
                }
            })
            // Enter sends, Shift+Enter inserts a newline (the on_submit only fires while the
            // TextArea is focused — Enter only reaches a focused input). While streaming, `submit()`
            // queues the draft as a follow-up for the active runtime turn.
            .on_submit(move |_text| {
                s.chat_mut().submit();
            })
    };

    let streaming = shared.chat().streaming;
    let send: WidgetPod = if streaming {
        let s = shared.clone();
        WidgetPod::new(
            IconButton::new(IconGlyph::Close, "Interrupt")
                .size(SEND_BUTTON_SIZE)
                .icon_size(17.0)
                .theme_when(clone_reader(reader))
                .on_press(move || {
                    s.chat_mut().interrupt_turn();
                }),
        )
    } else {
        let s = shared.clone();
        WidgetPod::new(
            IconButton::new(IconGlyph::Send, "Send")
                .size(SEND_BUTTON_SIZE)
                .icon_size(17.0)
                .theme_when(clone_reader(reader))
                .enabled_when({
                    let s = s.clone();
                    move || s.chat().can_send()
                })
                .on_press(move || {
                    s.chat_mut().submit();
                }),
        )
    };

    let row = ComposerInputRow::new(
        WidgetPod::new(input),
        send,
        SEND_BUTTON_SIZE,
        INPUT_ACTION_GAP,
    );
    WidgetPod::new(row)
}

/// The full composer: a titlebar-surfaced panel with the selector row above the input row. Wrapped
/// so the selector row rebuilds when the option lists change. A fixed `COMPOSER_H` tall.
fn composer(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let selector_host = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || selector_signature(&s)
            },
            move |_sig| selectors(&s, &r),
        )
    };

    // The input row is rebuilt when the streaming flag flips (Send <-> Interrupt). The draft text is
    // owned by the `TextArea`, so we do NOT rebuild on every keystroke.
    let input_host = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || s.chat().streaming
            },
            move |_streaming| input_row(&s, &r),
        )
    };

    let input_h =
        (COMPOSER_H - (COMPOSER_PAD_Y * 2.0) - SELECTOR_H - COMPOSER_SECTION_GAP).max(48.0);
    let body = Stack::vertical()
        .spacing(0.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(COMPOSER_PAD_Y))
        .with_child(InsetX::new(selector_host, PAD))
        .with_child(SizedBox::new().height(COMPOSER_SECTION_GAP))
        .with_child(
            SizedBox::new()
                .height(input_h)
                .with_child(InsetX::new(input_host, PAD)),
        )
        .with_child(SizedBox::new().height(COMPOSER_PAD_Y));

    Surface::new(SurfaceRole::Titlebar, body)
        .border(SurfaceBorder::Top)
        .fill()
        .theme_when(clone_reader(reader))
}

// ---------------------------------------------------------------------------
// Composed pods (wide split / narrow) + responsive host.
// ---------------------------------------------------------------------------

/// The main column: the transcript fills, with the composer docked at the bottom. The streaming
/// request-animation-frame pump lives on [`ChatView`] (a reliable ancestor of every control), so no
/// invisible overlay widget is needed here.
fn main_column(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    widgets::Dock::new(TranscriptView::new(shared.clone()))
        .top(OVERVIEW_H, RuntimeOverview::new(shared.clone()))
        .bottom(COMPOSER_H, composer(shared, reader))
}

fn wide_pod(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let work_area = Split::right_fixed(
        main_column(shared, reader),
        ActivityRail::new(shared.clone()),
        ACTIVITY_RAIL_W,
        reader,
    );
    let row = Split::left_fixed(sidebar(shared, reader), work_area, SIDEBAR_W, reader);
    WidgetPod::new(Surface::window(row).fill().theme_when(clone_reader(reader)))
}

fn narrow_pod(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    WidgetPod::new(
        Surface::window(main_column(shared, reader))
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

/// The chat tab: a thin responsive host owning two pre-built pods (wide split with sidebar / narrow
/// transcript-only) and delegating to the one chosen by the available width. The choice is made in
/// `measure` (constraints-driven) and cached so `arrange`/`paint`/event routing stay in sync.
/// Mirrors the Settings / Files hosts.
///
/// `ChatView` additionally owns the streaming **pump**: it is a reliable ancestor of every control
/// (the sidebar/composer `RebuildOnChange` hosts and the transcript), and it is reliably on the
/// event path for the pointer / keyboard gestures that start a turn — so it is the right place to
/// drive the request-animation-frame loop. (A sibling overlay widget could not: in sui an event
/// only reaches widgets on the `path_to(target)`, and `RedrawRequested` / external wakes route to
/// the *root* only, so an off-path overlay would never receive the kick.) While a reply streams (or
/// there was recent activity) the pump drains the worker each frame, applies events to `ChatState`,
/// and — when the parts the `RebuildOnChange` hosts key on change — forces a full window relayout so
/// those hosts rebuild and re-measure (a worker-driven change arrives outside any pointer gesture).
pub struct ChatView {
    shared: Shared,
    wide: WidgetPod,
    narrow: WidgetPod,
    use_wide: Cell<bool>,
    /// Coarse signature of the selection / session / streaming state the inner `RebuildOnChange`
    /// hosts key on; a change forces a full relayout so they rebuild.
    last_sig: Cell<u64>,
    /// Whether an animation frame is currently requested (so we don't stack duplicate requests).
    pump_alive: Cell<bool>,
    /// Frames since the last state change; bounds the idle tail so we stop requesting frames.
    idle_frames: Cell<u32>,
}

/// Idle-frame budget: once this many frames pass with no state change (and nothing streaming) the
/// pump stops requesting animation frames, so an idle tab never busy-loops.
const PUMP_IDLE_BUDGET: u32 = 90;

impl ChatView {
    pub fn new(shared: Shared) -> Self {
        let theme_reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let wide = wide_pod(&shared, &theme_reader);
        let narrow = narrow_pod(&shared, &theme_reader);
        let last_sig = Cell::new(view_signature(&shared));
        Self {
            shared,
            wide,
            narrow,
            use_wide: Cell::new(true),
            last_sig,
            pump_alive: Cell::new(false),
            idle_frames: Cell::new(0),
        }
    }

    fn active(&self) -> &WidgetPod {
        if self.use_wide.get() {
            &self.wide
        } else {
            &self.narrow
        }
    }

    fn active_mut(&mut self) -> &mut WidgetPod {
        if self.use_wide.get() {
            &mut self.wide
        } else {
            &mut self.narrow
        }
    }

    /// Ensure the request-animation-frame pump is running (idempotent).
    fn ensure_pump(&self, ctx: &mut EventCtx) {
        if !self.pump_alive.get() {
            ctx.request_animation_frame();
            self.pump_alive.set(true);
        }
        // Any fresh gesture resets the idle tail so a just-dispatched command is drained promptly.
        self.idle_frames.set(0);
    }

    /// Force a full window relayout (re-measure + re-arrange + repaint) for this frame.
    ///
    /// The streaming worker mutates `ChatState` outside any pointer gesture, and the
    /// sidebar/composer `RebuildOnChange` hosts only `refresh()` when their `measure()` runs. A
    /// plain `ctx.request_measure()` here would only invalidate `ChatView`'s own measure path, which
    /// can short-circuit before reaching those hosts. Targeting the *window* makes sui's
    /// `build_measure_scope` upgrade the pass to `force_all`, so every pod (including those hosts)
    /// re-measures and the newly-built option lists / Send<->Interrupt swap take effect.
    fn force_window_relayout(&self, ctx: &mut EventCtx) {
        use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};
        let window = InvalidationTarget::Window(ctx.window_id());
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
        ctx.request_paint();
    }

    /// One pump tick: drain the worker, apply events, and relayout/repaint as needed; keep the loop
    /// alive while streaming or recently active.
    fn pump(&self, ctx: &mut EventCtx) {
        self.pump_alive.set(false);
        let events = self.shared.chat().handle.drain();
        let had = !events.is_empty();
        if had {
            let mut chat = self.shared.chat_mut();
            for event in events {
                chat.apply_event(event);
            }
        }
        let streaming = self.shared.chat().streaming;

        // Recompute the coarse signature each tick: a worker event (RuntimeInfo / Models / TurnDone,
        // or a token that resolved the selection) may have changed the selector option lists, the
        // sidebar sessions, or the streaming flag (Send <-> Interrupt) — anything the inner
        // `RebuildOnChange` hosts key on. On a signature change, force a full relayout so those hosts
        // rebuild; on a bare token (same signature) just repaint the live-reading transcript.
        let sig = view_signature(&self.shared);
        let sig_changed = sig != self.last_sig.get();
        if sig_changed {
            self.last_sig.set(sig);
            self.force_window_relayout(ctx);
        } else if had {
            ctx.request_paint();
        }

        if had || sig_changed {
            self.idle_frames.set(0);
        } else {
            self.idle_frames
                .set(self.idle_frames.get().saturating_add(1));
        }
        if streaming || self.idle_frames.get() < PUMP_IDLE_BUDGET {
            ctx.request_animation_frame();
            self.pump_alive.set(true);
        }
    }
}

/// A coarse signature of the parts of `ChatState` whose change requires a host relayout (the
/// session/agent lists, provider/model/effort selection, and the streaming flag — anything the
/// inner `RebuildOnChange` hosts key on). Cheap to compute each redraw.
fn view_signature(shared: &Shared) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let chat = shared.chat();
    let mut h = DefaultHasher::new();
    chat.provider_idx.hash(&mut h);
    chat.model_idx.hash(&mut h);
    chat.effort_idx.hash(&mut h);
    chat.current_models().len().hash(&mut h);
    chat.effort_visible().hash(&mut h);
    chat.streaming.hash(&mut h);
    chat.models_loading.hash(&mut h);
    chat.providers.len().hash(&mut h);
    for provider in &chat.providers {
        provider.id.hash(&mut h);
        provider.account.hash(&mut h);
        provider.name.hash(&mut h);
    }
    chat.sessions.len().hash(&mut h);
    for s in &chat.sessions {
        s.title.hash(&mut h);
    }
    chat.info.agent.hash(&mut h);
    h.finish()
}

impl Widget for ChatView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        // The streaming pump: an animation-frame wake drains the worker and relayouts. (The wake
        // targets `ChatView` because `ChatView` requested it, so it routes here reliably.)
        if let Event::Wake(sui::WakeEvent::AnimationFrame { .. }) = event {
            if self.shared.is_ready() {
                self.pump(ctx);
            }
            return;
        }

        if !self.shared.is_ready() {
            return;
        }

        // Any pointer or key activity over the tab is a moment the worker may have pending events:
        // a turn just started (Send click / Enter submit), a selection changed (sidebar / Select),
        // or the initial Ready/Models fetch landed in the channel. Make sure the pump is running so
        // those are drained. (Pointer Move/Enter are plentiful in the live app, so the initial model
        // fetch is drained promptly without requiring an explicit click.)
        let pointer_or_key = matches!(event, Event::Pointer(_))
            || matches!(event, Event::Keyboard(k) if k.state == KeyState::Pressed);
        if pointer_or_key {
            self.ensure_pump(ctx);
        }

        // A pointer release / key press can also have just rebuilt a `RebuildOnChange` host (a
        // sidebar/Select on-change fired during this gesture); relayout so it re-measures. The
        // `measure` pass refreshes `last_sig` to the post-change signature, so the next pump tick
        // does not redundantly relayout for a change this gesture already handled.
        let commit = matches!(event, Event::Pointer(p) if matches!(p.kind, PointerEventKind::Up))
            || matches!(event, Event::Keyboard(k) if k.state == KeyState::Pressed);
        if commit {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let use_wide = constraints.max.width >= WIDE_THRESHOLD;
        self.use_wide.set(use_wide);
        self.last_sig.set(view_signature(&self.shared));
        self.active_mut().measure(ctx, constraints)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.active_mut().arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.active().paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.active().semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(self.active());
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        // CRITICAL: forward to the active pod so pointer / keyboard / focus reach the controls.
        if self.use_wide.get() {
            visitor.visit(&mut self.wide);
        } else {
            visitor.visit(&mut self.narrow);
        }
    }
}

// ---------------------------------------------------------------------------
// RebuildOnChange<K> — rebuild a child subtree when a watched key changes.
// (Same shape as the Files-tab helper: collection widgets take a fixed item
// vector at build time, so when the underlying lists change we rebuild rather
// than bind a reader.)
// ---------------------------------------------------------------------------

struct RebuildOnChange<K: PartialEq + Clone> {
    key_fn: Box<dyn Fn() -> K>,
    build: Box<dyn Fn(&K) -> WidgetPod>,
    last_key: K,
    child: WidgetPod,
}

impl<K: PartialEq + Clone> RebuildOnChange<K> {
    fn new<KF, BF>(key_fn: KF, build: BF) -> Self
    where
        KF: Fn() -> K + 'static,
        BF: Fn(&K) -> WidgetPod + 'static,
    {
        let last_key = key_fn();
        let child = build(&last_key);
        Self {
            key_fn: Box::new(key_fn),
            build: Box::new(build),
            last_key,
            child,
        }
    }

    fn refresh(&mut self) -> bool {
        let key = (self.key_fn)();
        if key != self.last_key {
            self.child = (self.build)(&key);
            self.last_key = key;
            true
        } else {
            false
        }
    }
}

impl<K: PartialEq + Clone + 'static> Widget for RebuildOnChange<K> {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        // A pointer release can be the gesture that just changed the watched key (e.g. a `Select`'s
        // on-change fired during this very gesture). Refresh + relayout immediately so the rebuilt
        // subtree is measured this frame. (Worker-driven, gesture-less changes are handled by
        // `ChatView`, which forces a full window relayout — that re-runs our `measure`, which also
        // refreshes — so we do not depend on the never-delivered `RedrawRequested` here.)
        let pointer_up = matches!(
            event,
            Event::Pointer(p) if matches!(p.kind, PointerEventKind::Up)
        );
        if pointer_up && self.refresh() {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.refresh();
        self.child.measure(ctx, constraints)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.child.arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.child.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.child.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.child);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.child);
    }
}

struct ComposerInputRow {
    input: WidgetPod,
    action: WidgetPod,
    action_side: f32,
    gap: f32,
}

impl ComposerInputRow {
    fn new(input: WidgetPod, action: WidgetPod, action_side: f32, gap: f32) -> Self {
        Self {
            input,
            action,
            action_side: action_side.max(0.0),
            gap: gap.max(0.0),
        }
    }

    fn layout_rects(&self, bounds: Rect) -> (Rect, Rect) {
        let side = self
            .action_side
            .min(bounds.width())
            .min(bounds.height())
            .max(0.0);
        let gap = if side > 0.0 && bounds.width() > side {
            self.gap.min(bounds.width() - side).max(0.0)
        } else {
            0.0
        };
        let input_width = (bounds.width() - side - gap).max(0.0);
        let input = Rect::new(bounds.x(), bounds.y(), input_width, bounds.height());
        let action = Rect::new(
            bounds.x() + input_width + gap,
            bounds.y() + ((bounds.height() - side) * 0.5).max(0.0),
            side,
            side,
        );
        (input, action)
    }
}

impl Widget for ComposerInputRow {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            constraints.min.width.max(320.0)
        };
        let height = if constraints.max.height.is_finite() {
            constraints.max.height
        } else {
            constraints.min.height.max(self.action_side)
        };
        let bounds = Rect::new(0.0, 0.0, width, height);
        let (input, action) = self.layout_rects(bounds);
        self.input.measure(
            ctx,
            Constraints::tight(Size::new(input.width(), input.height())),
        );
        self.action.measure(
            ctx,
            Constraints::tight(Size::new(action.width(), action.height())),
        );
        constraints.clamp(Size::new(width, height))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let (input, action) = self.layout_rects(bounds);
        self.input.arrange(ctx, input);
        self.action.arrange(ctx, action);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.input.paint(ctx);
        self.action.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.input.semantics(ctx);
        self.action.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.input);
        visitor.visit(&self.action);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.input);
        visitor.visit(&mut self.action);
    }
}

// ---------------------------------------------------------------------------
// Small layout helpers (Split / InsetX) — mirrors the Files tab.
// ---------------------------------------------------------------------------

/// A horizontal split with one fixed-width pane and one flexible pane, a 1px vertical [`Separator`]
/// between. Either the left or the right pane may be the fixed one (a plain horizontal `Stack` lets
/// the first child consume the full width before the fixed pane is measured).
struct Split {
    left: WidgetPod,
    separator: WidgetPod,
    right: WidgetPod,
    fixed_width: f32,
    fixed_is_left: bool,
}

const SPLIT_SEPARATOR_W: f32 = 1.0;

impl Split {
    /// Left pane fixed `left_width`, right pane flexible.
    fn left_fixed<L, R>(left: L, right: R, left_width: f32, reader: &ThemeReader) -> Self
    where
        L: Widget + 'static,
        R: Widget + 'static,
    {
        Self {
            left: WidgetPod::new(left),
            separator: WidgetPod::new(Separator::vertical().theme_when(clone_reader(reader))),
            right: WidgetPod::new(right),
            fixed_width: left_width.max(0.0),
            fixed_is_left: true,
        }
    }

    /// Right pane fixed `right_width`, left pane flexible.
    fn right_fixed<L, R>(left: L, right: R, right_width: f32, reader: &ThemeReader) -> Self
    where
        L: Widget + 'static,
        R: Widget + 'static,
    {
        Self {
            left: WidgetPod::new(left),
            separator: WidgetPod::new(Separator::vertical().theme_when(clone_reader(reader))),
            right: WidgetPod::new(right),
            fixed_width: right_width.max(0.0),
            fixed_is_left: false,
        }
    }

    /// (left_width, right_width) for a given total width.
    fn split_widths(&self, total: f32) -> (f32, f32) {
        let fixed = self.fixed_width.min((total - SPLIT_SEPARATOR_W).max(0.0));
        let flex = (total - fixed - SPLIT_SEPARATOR_W).max(0.0);
        if self.fixed_is_left {
            (fixed, flex)
        } else {
            (flex, fixed)
        }
    }
}

impl Widget for Split {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let total = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            self.fixed_width + 320.0 + SPLIT_SEPARATOR_W
        };
        let height = constraints.max.height;
        let (left_w, right_w) = self.split_widths(total);
        self.left
            .measure(ctx, Constraints::tight(Size::new(left_w, height)));
        self.separator.measure(
            ctx,
            Constraints::tight(Size::new(SPLIT_SEPARATOR_W, height)),
        );
        self.right
            .measure(ctx, Constraints::tight(Size::new(right_w, height)));
        Size::new(total, height)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let (left_w, right_w) = self.split_widths(bounds.width());
        self.left.arrange(
            ctx,
            Rect::new(bounds.x(), bounds.y(), left_w, bounds.height()),
        );
        self.separator.arrange(
            ctx,
            Rect::new(
                bounds.x() + left_w,
                bounds.y(),
                SPLIT_SEPARATOR_W,
                bounds.height(),
            ),
        );
        self.right.arrange(
            ctx,
            Rect::new(
                bounds.x() + left_w + SPLIT_SEPARATOR_W,
                bounds.y(),
                right_w,
                bounds.height(),
            ),
        );
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.left.paint(ctx);
        self.separator.paint(ctx);
        self.right.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.left.semantics(ctx);
        self.separator.semantics(ctx);
        self.right.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.left);
        visitor.visit(&self.separator);
        visitor.visit(&self.right);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.left);
        visitor.visit(&mut self.separator);
        visitor.visit(&mut self.right);
    }
}

/// Insets a single child horizontally by `pad` on each side (the vertical axis is left to the
/// caller). Keeps content off the panel edges without pulling in the container `Padding` widget.
struct InsetX {
    child: WidgetPod,
    pad: f32,
}

impl InsetX {
    fn new<W>(child: W, pad: f32) -> Self
    where
        W: Widget + 'static,
    {
        Self {
            child: WidgetPod::new(child),
            pad: pad.max(0.0),
        }
    }
}

impl Widget for InsetX {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let inner_max = Size::new(
            (constraints.max.width - self.pad * 2.0).max(0.0),
            constraints.max.height,
        );
        let inner = Constraints::new(Size::ZERO, inner_max);
        let child = self.child.measure(ctx, inner);
        Size::new(child.width + self.pad * 2.0, child.height)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let inner = Rect::new(
            bounds.x() + self.pad,
            bounds.y(),
            (bounds.width() - self.pad * 2.0).max(0.0),
            bounds.height(),
        );
        self.child.arrange(ctx, inner);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.child.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.child.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.child);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.child);
    }
}

/// Build the chat tab content.
pub fn view(shared: Shared) -> impl Widget {
    ChatView::new(shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use std::time::{Duration, Instant};

    fn seed_default_provider(state: &mut ChatState) {
        if state.providers.is_empty() {
            state.providers.push(ProviderSummary {
                id: "openai".to_string(),
                account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
                name: "OpenAI".to_string(),
                models: vec!["gpt-5".to_string()],
            });
        }
        state.models_loading = false;
    }

    #[test]
    fn composer_control_reader_keeps_footer_text_compact() {
        let reader: ThemeReader = Rc::new(DefaultTheme::default);
        let theme = composer_control_reader(&reader)();
        let body = theme.body_text_style();
        let placeholder = theme.placeholder_text_style();

        assert_eq!(body.font_size, COMPOSER_TEXT_SIZE);
        assert_eq!(body.line_height, COMPOSER_LINE_HEIGHT);
        assert_eq!(placeholder.font_size, COMPOSER_TEXT_SIZE);
        assert_eq!(placeholder.line_height, COMPOSER_LINE_HEIGHT);
    }

    #[test]
    fn submit_pushes_user_and_agent_placeholder_and_sets_streaming() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("hello there".to_string());
        state.submit();
        assert!(state.streaming, "submitting begins a streaming turn");
        assert_eq!(state.messages.len(), 2, "user message + agent placeholder");
        assert_eq!(state.messages[0].role, ChatRole::User);
        assert_eq!(state.messages[0].source, ChatMessageSource::User);
        assert_eq!(state.messages[0].kind, ChatMessageKind::Message);
        assert_eq!(state.messages[0].text, "hello there");
        assert_eq!(state.messages[1].role, ChatRole::Agent);
        assert_eq!(state.messages[1].source, ChatMessageSource::Model);
        assert_eq!(state.messages[1].kind, ChatMessageKind::Message);
        assert!(
            state.messages[1].text.is_empty(),
            "agent placeholder is blank"
        );
        assert!(state.input.is_empty(), "the draft is cleared on submit");
    }

    #[test]
    fn empty_submit_is_noop_and_streaming_submit_queues_follow_up() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        // Empty draft → nothing happens.
        state.submit();
        assert!(!state.streaming);
        assert!(state.messages.is_empty());
        // While streaming, a submit is queued as a follow-up for the active runtime turn.
        state.set_draft("queued".to_string());
        state.streaming = true;
        state.submit();
        assert_eq!(state.messages.len(), 1, "queued follow-up is echoed");
        assert_eq!(state.messages[0].role, ChatRole::User);
        assert_eq!(state.messages[0].source, ChatMessageSource::User);
        assert_eq!(state.messages[0].text, "queued");
        assert!(state.input.is_empty(), "the queued draft is cleared");
        assert!(
            state.streaming,
            "queueing follow-up keeps active stream state"
        );
        assert_eq!(state.notice.as_deref(), Some("queued follow-up"));
    }

    #[test]
    fn new_session_clears_visible_chat_immediately_and_routes_stale_turn_events() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        let old_session_id = SessionId::from_legacy_named("sess_old_chat")
            .expect("session id")
            .to_string();
        state.session_id = old_session_id.clone();
        state.messages = vec![
            ChatMessage::user("old question"),
            ChatMessage::model("partial answer"),
        ];
        state.input = "draft".to_string();
        state.cursor = state.input.len();
        state.streaming = true;
        state.notice = Some("thinking…".to_string());
        state.control.active_turn = true;

        state.new_session();
        let new_session_id = state.session_id.clone();

        assert!(
            state.messages.is_empty(),
            "New chat clears the transcript immediately"
        );
        assert!(state.input.is_empty(), "New chat clears the composer draft");
        assert!(!state.streaming, "New chat leaves the visible chat idle");
        assert_eq!(
            state.pending_session_reset.as_deref(),
            Some(new_session_id.as_str())
        );

        state.apply_event(ChatEvent::Error {
            session_id: Some(old_session_id.clone()),
            error: "old provider failed".to_string(),
        });
        assert!(
            state.messages.is_empty(),
            "stale errors from the abandoned turn must not repopulate the new chat"
        );
        let old_transcript = sessions::load_transcript(&state.data_dir, &old_session_id)
            .expect("old transcript remains persisted");
        let old_error = old_transcript
            .messages
            .last()
            .expect("old error message persisted");
        assert_eq!(old_error.source.as_deref(), Some("system"));
        assert_eq!(old_error.kind.as_deref(), Some("error"));
        assert!(old_error.text.contains("old provider failed"));

        state.set_draft("new question".to_string());
        state.submit();
        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.messages[0].text, "new question");
        assert!(
            state.streaming,
            "sending is allowed while the worker reset ack is pending"
        );

        state.apply_event(ChatEvent::SessionReset {
            session_id: new_session_id,
        });
        assert!(state.pending_session_reset.is_none());
    }

    #[test]
    fn session_scoped_events_update_background_transcript_without_visible_mutation() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        let visible_session_id = SessionId::from_legacy_named("sess_visible_chat")
            .expect("visible session id")
            .to_string();
        let background_session_id = SessionId::from_legacy_named("sess_background_chat")
            .expect("background session id")
            .to_string();
        state.session_id = visible_session_id.clone();
        state.messages = vec![
            ChatMessage::user("visible question"),
            ChatMessage::model(""),
        ];
        state.streaming = true;
        state.control.active_turn = true;

        let background_messages = vec![
            ChatMessage::user("background question"),
            ChatMessage::model(String::new()),
        ];
        state.persist_messages(&background_session_id, &background_messages, false);

        state.apply_event(ChatEvent::Token {
            session_id: background_session_id.clone(),
            text: "background answer".to_string(),
        });
        state.apply_event(ChatEvent::TurnDone {
            session_id: background_session_id.clone(),
        });

        assert_eq!(
            state.session_id, visible_session_id,
            "background events keep the visible session selected"
        );
        assert!(
            state.streaming,
            "background TurnDone must not end visible work"
        );
        assert_eq!(
            state.messages[1].text, "",
            "background tokens must not append to the visible placeholder"
        );
        let background = sessions::load_transcript(&state.data_dir, &background_session_id)
            .expect("background transcript saved");
        assert_eq!(background.messages.len(), 2);
        assert_eq!(background.messages[1].text, "background answer");
        assert_eq!(background.messages[1].source.as_deref(), Some("model"));
        assert_eq!(background.messages[1].kind.as_deref(), Some("message"));
    }

    #[test]
    fn interrupt_turn_only_updates_notice_while_streaming() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);

        state.interrupt_turn();
        assert!(state.notice.is_none(), "idle interrupt is a no-op");

        state.streaming = true;
        state.interrupt_turn();
        assert_eq!(state.notice.as_deref(), Some("interrupting…"));
        assert!(state.streaming, "interrupt request waits for runtime event");
    }

    #[test]
    fn streamed_tokens_append_to_the_last_agent_message() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("hi".to_string());
        state.submit();
        // Tokens append to the trailing (empty) agent placeholder rather than pushing new bubbles.
        state.apply_event(ChatEvent::Token {
            session_id: state.session_id.clone(),
            text: "Hel".to_string(),
        });
        state.apply_event(ChatEvent::Token {
            session_id: state.session_id.clone(),
            text: "lo".to_string(),
        });
        assert_eq!(state.messages.len(), 2, "tokens append, not push");
        assert_eq!(state.messages[1].text, "Hello");
        // TurnDone clears streaming and the notice.
        state.apply_event(ChatEvent::TurnDone {
            session_id: state.session_id.clone(),
        });
        assert!(!state.streaming, "TurnDone ends the stream");
        assert!(
            !state.control.active_turn,
            "TurnDone clears the active turn marker"
        );
        assert!(state.notice.is_none());
    }

    #[test]
    fn stored_message_metadata_distinguishes_system_errors_from_model_text() {
        let stored = ChatMessage::system_error("Error: provider unavailable").to_stored();

        assert_eq!(stored.role, "agent");
        assert_eq!(stored.source.as_deref(), Some("system"));
        assert_eq!(stored.kind.as_deref(), Some("error"));

        let restored = ChatMessage::from_stored(stored);
        assert_eq!(restored.role, ChatRole::Agent);
        assert_eq!(restored.source, ChatMessageSource::System);
        assert_eq!(restored.kind, ChatMessageKind::Error);
        assert_eq!(restored.history_role(), "system");
    }

    #[test]
    fn empty_turn_done_marks_placeholder_as_system_info() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("hi".to_string());
        state.submit();

        state.apply_event(ChatEvent::TurnDone {
            session_id: state.session_id.clone(),
        });

        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.messages[1].text, "(no response)");
        assert_eq!(state.messages[1].source, ChatMessageSource::System);
        assert_eq!(state.messages[1].kind, ChatMessageKind::Info);
    }

    #[test]
    fn turn_error_replaces_pending_agent_placeholder() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("hi".to_string());
        state.submit();

        state.apply_event(ChatEvent::Error {
            session_id: Some(state.session_id.clone()),
            error: "authentication required for codex: no Sinomo Codex ChatGPT sign-in found; run `sinomo providers auth codex`"
                .to_string(),
        });

        assert!(!state.streaming, "provider errors end the visible stream");
        assert!(
            !state.control.active_turn,
            "provider errors clear the active turn marker"
        );
        assert_eq!(state.messages.len(), 2, "the placeholder is reused");
        assert_eq!(state.messages[1].role, ChatRole::Agent);
        assert_eq!(state.messages[1].source, ChatMessageSource::System);
        assert_eq!(state.messages[1].kind, ChatMessageKind::Error);
        assert!(
            state.messages[1]
                .text
                .contains("authentication required for codex"),
            "the provider setup error is visible in the transcript"
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "agent error" && entry.tone == ActivityTone::Bad)
        );

        state.apply_event(ChatEvent::TurnDone {
            session_id: state.session_id.clone(),
        });
        assert!(
            !state.messages[1].text.contains("(no response)"),
            "TurnDone must not overwrite an already-visible error"
        );
    }

    #[test]
    fn control_events_update_notice_without_touching_streaming_state() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.streaming = true;

        state.apply_event(ChatEvent::ControlStatus(
            sinomo_cli::app_api::ChatControlStatus {
                session_id: "sess_ui_control".to_string(),
                accepted: true,
                sequence: 3,
                event_count: 1,
                active_turn: true,
                task_count: 0,
                terminal_result_count: 0,
                tasks: Vec::new(),
            },
        ));
        assert!(state.streaming);
        assert_eq!(
            state.notice.as_deref(),
            Some("agent control accepted at sequence 3")
        );

        state.apply_event(ChatEvent::ControlState(
            sinomo_cli::app_api::ChatControlState {
                session_id: "sess_ui_control".to_string(),
                accepted: true,
                steering_count: 2,
                follow_up_count: 1,
                ..Default::default()
            },
        ));
        assert!(state.streaming);
        assert_eq!(
            state.notice.as_deref(),
            Some("agent control: 2 steering, 1 follow-ups")
        );
    }

    #[test]
    fn structured_runtime_events_update_context_usage_and_activity() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.streaming = true;
        let session_id = state.session_id.clone();

        state.apply_event(ChatEvent::Context {
            session_id: session_id.clone(),
            context: ChatContextSnapshot {
                step_index: 0,
                provider: "codex".to_string(),
                model: "gpt-5.5-codex".to_string(),
                total_bytes: 4096,
                max_context_bytes: Some(8192),
                compaction_threshold_bytes: Some(7000),
                lanes: vec![sinomo_cli::app_api::ChatContextLane {
                    kind: "recent_tail".to_string(),
                    label: "recent transcript".to_string(),
                    bytes: 1024,
                    priority: 2,
                }],
                included_count: 3,
                omitted_count: 1,
            },
        });
        assert_eq!(
            state.context.as_ref().map(|ctx| ctx.total_bytes),
            Some(4096)
        );
        assert_eq!(state.notice.as_deref(), Some("context assembled: 4.0 KB"));

        state.apply_event(ChatEvent::ToolCallCompleted {
            session_id: session_id.clone(),
            call: ChatToolCallSnapshot {
                step_index: 0,
                call_index: Some(0),
                call_id: Some("call_1".to_string()),
                tool: "run_command".to_string(),
                input_preview: Some("{\"cmd\":\"date\"}".to_string()),
            },
        });
        state.apply_event(ChatEvent::Process {
            session_id: session_id.clone(),
            process: ChatProcessSnapshot {
                process_id: "proc_1".to_string(),
                command: "date".to_string(),
                status: "completed".to_string(),
                exit_code: Some(0),
                elapsed_ms: 12,
                stdout_tail: "Mon Jun 15".to_string(),
                stderr_tail: String::new(),
            },
        });
        state.apply_event(ChatEvent::Usage {
            session_id: session_id.clone(),
            usage: ChatUsageSnapshot {
                provider: "codex".to_string(),
                model: "gpt-5.5-codex".to_string(),
                input_tokens: Some(120),
                output_tokens: Some(18),
            },
        });

        assert_eq!(
            state.usage.as_ref().and_then(|usage| usage.input_tokens),
            Some(120)
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title.contains("run_command"))
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "date" && entry.detail.contains("exit 0"))
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "model usage" && entry.detail.contains("120 input"))
        );
    }

    #[test]
    fn tool_call_monitor_tracks_started_streaming_and_completed_calls() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let session_id = state.session_id.clone();

        state.apply_event(ChatEvent::ToolCallStarted {
            session_id: session_id.clone(),
            call: ChatToolCallSnapshot {
                step_index: 0,
                call_index: Some(0),
                call_id: Some("call_1".to_string()),
                tool: "run_command".to_string(),
                input_preview: None,
            },
        });
        assert_eq!(state.tool_calls.len(), 1);
        assert_eq!(state.tool_calls[0].status, "running");

        state.apply_event(ChatEvent::ToolCallDelta {
            session_id: session_id.clone(),
            step_index: 0,
            call_id: Some("call_1".to_string()),
            tool: Some("run_command".to_string()),
            arguments_delta: "{\"cmd\":\"date\"}".to_string(),
        });
        assert_eq!(state.tool_calls.len(), 1);
        assert_eq!(state.tool_calls[0].status, "streaming");
        assert!(state.tool_calls[0].input_preview.contains("date"));

        state.apply_event(ChatEvent::ToolCallCompleted {
            session_id: session_id.clone(),
            call: ChatToolCallSnapshot {
                step_index: 0,
                call_index: Some(0),
                call_id: Some("call_1".to_string()),
                tool: "run_command".to_string(),
                input_preview: Some("{\"cmd\":\"date\"}".to_string()),
            },
        });
        assert_eq!(state.tool_calls.len(), 1);
        assert_eq!(state.tool_calls[0].status, "completed");
        assert_eq!(state.tool_calls[0].tool, "run_command");
    }

    #[test]
    fn runtime_health_surfaces_degraded_cluster_and_persistence_state() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.notice = Some("session save failed: DFS services shard replay delayed".to_string());

        let health = state.runtime_health();
        assert_eq!(health.label, "DEGRADED");
        assert_eq!(health.tone, ActivityTone::Warn);
        assert!(
            health.detail.contains("session save failed"),
            "health detail should preserve the operator-relevant degraded signal"
        );

        state.notice = None;
        state.control.active_turn = true;
        state.control.task_count = 1;
        state.control.runtime_attached = false;
        let health = state.runtime_health();
        assert_eq!(health.label, "DEGRADED");
        assert_eq!(health.tone, ActivityTone::Warn);
        assert!(health.detail.contains("detached"));
    }

    #[test]
    fn runtime_health_prioritizes_failed_tasks_and_errors() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.control.tasks.push(ChatTaskSnapshot {
            task_id: "task_failed".to_string(),
            kind: "verification".to_string(),
            summary: "cluster readiness gate".to_string(),
            status: "failed".to_string(),
            progress_summary: None,
            result_ref: None,
            active_op_id: None,
        });

        let health = state.runtime_health();
        assert_eq!(health.label, "ATTENTION");
        assert_eq!(health.tone, ActivityTone::Bad);
        assert!(health.detail.contains("cluster readiness gate"));

        state.control.tasks.clear();
        state.apply_event(ChatEvent::Error {
            session_id: Some(state.session_id.clone()),
            error: "provider stream failed".to_string(),
        });
        let health = state.runtime_health();
        assert_eq!(health.label, "ATTENTION");
        assert_eq!(health.tone, ActivityTone::Bad);
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "agent error" && entry.tone == ActivityTone::Bad)
        );
    }

    #[test]
    fn context_lane_summary_formats_bytes_and_share() {
        assert_eq!(
            context_lane_summary("recent turn tail", 2_048, 8_192),
            "recent turn tail · 2.0 KB · 25%"
        );
        assert_eq!(context_lane_summary("empty", 0, 0), "empty · 0 B · 0%");
        let long = context_lane_summary(
            "very long prompt reference label that should compact",
            1_048_576,
            2_097_152,
        );
        assert!(long.starts_with("very long prompt referenc... · 1.0 MB · 50%"));
    }

    #[test]
    fn can_send_requires_nonempty_draft() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        assert!(!state.can_send(), "empty draft cannot send");
        state.set_draft("ok".to_string());
        seed_default_provider(&mut state);
        assert!(state.can_send(), "a non-empty draft can send");
        state.streaming = true;
        assert!(
            state.can_send(),
            "a non-empty draft can queue while streaming"
        );
        state.pending_session_reset = Some(SessionId::generate().to_string());
        assert!(
            state.can_send(),
            "a pending new-session ack must not disable the Send button"
        );
    }

    #[test]
    fn set_provider_resets_model_index() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        if state.providers.len() < 2 {
            return; // environment without multiple configured providers
        }
        state.model_idx = 3;
        state.set_provider(1);
        assert_eq!(state.provider_idx, 1);
        assert_eq!(state.model_idx, 0, "switching providers resets the model");
    }

    /// Issue 2: the default/initial provider's full model list must be fetched eagerly at
    /// construction. Configured accounts seed a usable default model immediately, then
    /// `ChatState::new` kicks the async full-list fetch.
    #[test]
    fn new_fetches_default_provider_models() {
        let backend = Backend::local_fallback();
        let state = ChatState::new(&backend);
        if state.providers.is_empty() {
            // No configured provider → nothing to fetch (correctly a no-op).
            assert!(!state.models_loading);
            return;
        }
        assert!(
            state
                .current_provider()
                .is_some_and(|p| !p.models.is_empty()),
            "providers are seeded with a usable default model"
        );
        assert!(
            state.models_loading,
            "the default provider's model fetch is kicked at construction"
        );
    }

    /// The runtime resolving a provider (Ready / RuntimeInfo) must also trigger a model fetch for
    /// the newly-resolved provider when its list is still empty, so the picker populates even if the
    /// worker reports a provider other than index 0.
    #[test]
    fn runtime_info_triggers_model_fetch_for_resolved_provider() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let Some(provider_id) = state.providers.first().map(|p| p.id.clone()) else {
            return; // no configured provider in this environment
        };
        // Pretend the initial fetch already completed (so we can observe a fresh one being kicked).
        state.models_loading = false;
        state.apply_event(ChatEvent::RuntimeInfo(RuntimeInfo {
            agent: "main".to_string(),
            provider: provider_id,
            model: String::new(),
            account: None,
            effort: None,
        }));
        assert!(
            state.models_loading,
            "resolving the provider via RuntimeInfo re-kicks the model fetch when the list is empty"
        );
    }

    #[test]
    fn empty_model_fetch_preserves_seeded_provider_model() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers = vec![ProviderSummary {
            id: "deepseek".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "DeepSeek".to_string(),
            models: vec!["deepseek-v4-flash".to_string()],
        }];
        state.provider_idx = 0;
        state.model_idx = 0;
        state.models_loading = true;

        state.apply_event(ChatEvent::Models {
            provider: "deepseek".to_string(),
            models: Vec::new(),
        });

        assert_eq!(
            state.current_models(),
            vec!["deepseek-v4-flash".to_string()],
            "failed model discovery must not wipe the configured/default model"
        );
        assert_eq!(state.current_model(), "deepseek-v4-flash");
        assert!(!state.models_loading);
    }

    #[test]
    fn hidden_runtime_provider_does_not_leak_into_visible_model_selection() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers = vec![ProviderSummary {
            id: "deepseek".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "DeepSeek".to_string(),
            models: vec!["deepseek-v4-flash".to_string()],
        }];
        state.provider_idx = 0;
        state.model_idx = 99;
        state.models_loading = false;

        state.apply_event(ChatEvent::RuntimeInfo(RuntimeInfo {
            agent: "main".to_string(),
            provider: "github-copilot".to_string(),
            model: "gpt-5.4".to_string(),
            account: Some(sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string()),
            effort: None,
        }));

        assert_eq!(
            state.current_model(),
            "",
            "an unlisted runtime provider must not supply the selected provider's model"
        );
        state.model_idx = 0;
        assert_eq!(
            state.current_model(),
            "deepseek-v4-flash",
            "the visible provider keeps its own configured model"
        );
    }

    /// Issue 1: Enter-to-send. The composer wires `TextArea::on_submit` to `ChatState::submit`, so a
    /// plain Enter starts a turn. `submit()` is the exact entry point that wiring calls; assert it
    /// begins a streaming turn from a non-empty draft (and is a no-op otherwise — covered above).
    #[test]
    fn submit_is_the_enter_to_send_entry_point() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("ping".to_string());
        assert!(state.can_send(), "a non-empty draft is sendable on Enter");
        // What `on_submit(|_| s.chat_mut().submit())` runs:
        state.submit();
        assert!(state.streaming, "Enter-to-send starts a streaming turn");
        assert_eq!(
            state.messages.first().map(|m| m.text.as_str()),
            Some("ping")
        );
        assert!(state.input.is_empty(), "the draft is cleared after sending");
    }

    #[test]
    #[ignore]
    fn live_app_chat_streams_and_persists_session_locally() {
        if std::env::var_os("SINOMO_LIVE_APP_CHAT_SMOKE").is_none() {
            return;
        }
        let backend = Backend::connect(|_| {});
        let mut state = ChatState::new(&backend);
        wait_for(&mut state, Duration::from_secs(20), |state| {
            !state.session_id.is_empty()
        });

        let provider =
            std::env::var("SINOMO_LIVE_APP_CHAT_PROVIDER").unwrap_or_else(|_| "codex".to_string());
        let model =
            std::env::var("SINOMO_LIVE_APP_CHAT_MODEL").unwrap_or_else(|_| "gpt-5.5".to_string());

        state.handle.send_command(ChatCommand::SetModel {
            provider: provider.clone(),
            model: model.clone(),
            account: "default".to_string(),
        });
        wait_for(&mut state, Duration::from_secs(20), |state| {
            state.info.provider == provider && state.info.model == model
        });

        let prompt = "Reply only with this exact text: sinomo-app-live-ok";
        state.set_draft(prompt.to_string());
        state.submit();
        wait_for(&mut state, Duration::from_secs(90), |state| {
            !state.streaming
                && state.messages.iter().any(|message| {
                    message.role == ChatRole::Agent && message.text.contains("sinomo-app-live-ok")
                })
        });

        let session_id = state.session_id.clone();
        assert!(!session_id.is_empty(), "session id is assigned");

        let reloaded =
            sessions::load_transcript(&state.data_dir, &session_id).expect("reload session");
        assert!(
            reloaded
                .messages
                .iter()
                .any(|message| message.role == "user" && message.text == prompt),
            "user prompt persisted"
        );
        assert!(
            reloaded
                .messages
                .iter()
                .any(|message| message.role == "agent"
                    && message.text.contains("sinomo-app-live-ok")),
            "agent response persisted"
        );

        if let Some(notice) = state.notice.as_deref()
            && notice.contains("session save failed")
        {
            assert!(
                state
                    .activity
                    .iter()
                    .any(|entry| entry.title == "session save" && entry.tone == ActivityTone::Warn),
                "SIFS save failures should be visible as degraded app-layer activity"
            );
        }
    }

    fn wait_for(state: &mut ChatState, timeout: Duration, predicate: impl Fn(&ChatState) -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let events = state.handle.drain();
            for event in events {
                state.apply_event(event);
            }
            if predicate(state) {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let snapshot = state
            .messages
            .iter()
            .map(|message| message.text.as_str())
            .collect::<Vec<_>>()
            .join("\n---\n");
        panic!(
            "condition was not met within {:?}; streaming={}, notice={:?}, messages={}",
            timeout, state.streaming, state.notice, snapshot
        );
    }
}
