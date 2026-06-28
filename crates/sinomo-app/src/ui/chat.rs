//! "Sinomo" tab — the AI chat experience.
//!
//! Like the Settings and Files tabs, this is a fully *composed* `sui-widgets`
//! tree: [`view`] returns a responsive host ([`ChatView`]) over pre-built pods
//! (wide split with the agents/chats sidebar, narrow transcript-only), each
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
//! The agents/chats sidebar and the composer's provider/model/effort
//! selectors are stock widgets. The model `Select` (and the effort selector's
//! visibility) depend on the current provider + fetched model list, which change
//! at runtime, so the composer row is wrapped in a [`RebuildOnChange`] keyed on
//! a small signature of the selection state.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sui::{
    Alignment, ArrangeCtx, Color, Constraints, DefaultTheme, Event, EventCtx, Flex, FlexItem,
    FontWeight, IconButton, IconGlyph, Insets, KeyState, Label, ListItem, ListView, MeasureCtx,
    PaintCtx, Point, PointerButton, PointerEventKind, Rect, ScrollDelta, ScrollState, ScrollView,
    SemanticsAction, SemanticsCtx, SemanticsNode, SemanticsRole, Separator, Size, SizedBox, Stack,
    StackHostOptions, Surface, SurfaceBorder, SurfaceRole, TextArea, TextInput, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor, draw_glyph,
};

use sinomo_agent::{
    AgentChannel, AgentChannelAuthor, AgentChannelKind, AgentChannelMessageKind, AgentChannelStore,
};
use sinomo_cli::app_api::{
    AgentChatSifsStorage, AgentSummary, ChatContextSnapshot, ChatHistoryMessage,
    ChatProcessSnapshot, ChatTaskSnapshot, ChatToolCallSnapshot, ChatUsageSnapshot,
    ProviderSummary, SifsAgentChannelStore, agent_summaries, reasoning_efforts,
};
use sinomo_runtime::{events::SessionId, local::SinomoDataDir};

use crate::backend::Backend;
use crate::backend::chat_engine::{
    ChatCommand, ChatEvent, ChatEventWake, ChatHandle, ChatRole, RuntimeInfo,
};
use crate::backend::sessions::{self, StoredMessage};
use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::settings::SettingsSection;
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
const SESSION_LIST_UI_TIMEOUT: Duration = Duration::from_secs(8);

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RuntimeRecoveryTarget {
    Providers,
    Connection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryCardAction {
    RetryChatList,
    CheckSifs,
    OpenConnectionSettings,
    OpenProviderSetup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChatQuickAction {
    PlanChange,
    CheckCluster,
    ReviewRepo,
    ManageChannel,
}

impl RecoveryCardAction {
    fn label(self) -> &'static str {
        match self {
            RecoveryCardAction::RetryChatList => "Retry now",
            RecoveryCardAction::CheckSifs => "Check SIFS",
            RecoveryCardAction::OpenConnectionSettings => "Open settings",
            RecoveryCardAction::OpenProviderSetup => "Provider setup",
        }
    }

    fn icon(self) -> IconGlyph {
        match self {
            RecoveryCardAction::RetryChatList => IconGlyph::Redo,
            RecoveryCardAction::CheckSifs => IconGlyph::Storage,
            RecoveryCardAction::OpenConnectionSettings => IconGlyph::Alert,
            RecoveryCardAction::OpenProviderSetup => IconGlyph::Add,
        }
    }

    fn description(self) -> &'static str {
        match self {
            RecoveryCardAction::RetryChatList => "Retry the chat list",
            RecoveryCardAction::CheckSifs => "Open SIFS browser",
            RecoveryCardAction::OpenConnectionSettings => "Connection settings",
            RecoveryCardAction::OpenProviderSetup => "Provider setup",
        }
    }
}

impl ChatQuickAction {
    const CHAT: [Self; 3] = [Self::PlanChange, Self::CheckCluster, Self::ReviewRepo];
    const CHANNEL: [Self; 3] = [Self::ManageChannel, Self::PlanChange, Self::CheckCluster];

    fn label(self) -> &'static str {
        match self {
            Self::PlanChange => "Plan a change",
            Self::CheckCluster => "Check cluster",
            Self::ReviewRepo => "Review repo",
            Self::ManageChannel => "Manage channel",
        }
    }

    fn prompt(self) -> Option<&'static str> {
        match self {
            Self::PlanChange => Some(
                "Plan the next code change in this repository and identify the safest first step.",
            ),
            Self::CheckCluster => {
                Some("Check the local cluster status and explain anything that needs attention.")
            }
            Self::ReviewRepo => Some(
                "Review the current repository state and point out the most important issue to fix next.",
            ),
            Self::ManageChannel => None,
        }
    }

    fn icon(self) -> IconGlyph {
        match self {
            Self::PlanChange => IconGlyph::Sparkles,
            Self::CheckCluster => IconGlyph::Storage,
            Self::ReviewRepo => IconGlyph::Search,
            Self::ManageChannel => IconGlyph::Chat,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::PlanChange => "Prepare a planning prompt",
            Self::CheckCluster => "Prepare a cluster status prompt",
            Self::ReviewRepo => "Prepare a repository review prompt",
            Self::ManageChannel => "Open channel agents and settings",
        }
    }
}

impl RuntimeRecoveryTarget {
    fn key(self) -> &'static str {
        match self {
            RuntimeRecoveryTarget::Providers => "providers",
            RuntimeRecoveryTarget::Connection => "connection",
        }
    }

    fn footer_label(self) -> &'static str {
        match self {
            RuntimeRecoveryTarget::Providers => "Provider setup",
            RuntimeRecoveryTarget::Connection => "Connection",
        }
    }

    fn footer_min_width(self) -> f32 {
        match self {
            RuntimeRecoveryTarget::Providers => 142.0,
            RuntimeRecoveryTarget::Connection => 116.0,
        }
    }

    fn empty_title(self) -> &'static str {
        match self {
            RuntimeRecoveryTarget::Providers => "Provider setup required",
            RuntimeRecoveryTarget::Connection => "Chat needs connection attention",
        }
    }

    fn empty_hint(self) -> &'static str {
        match self {
            RuntimeRecoveryTarget::Providers => {
                "Add credentials or choose an available model from provider setup."
            }
            RuntimeRecoveryTarget::Connection => {
                "Retry now. If the problem continues, inspect SIFS from Files or open Connection settings."
            }
        }
    }

    fn settings_section(self) -> SettingsSection {
        match self {
            RuntimeRecoveryTarget::Providers => SettingsSection::Providers,
            RuntimeRecoveryTarget::Connection => SettingsSection::Connection,
        }
    }
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

/// A previously-created chat or channel reference (sidebar list).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRef {
    pub id: String,
    pub title: String,
    pub kind: ConversationKind,
    pub updated_unix: i64,
    pub participant_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationKind {
    Chat,
    Channel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum SessionKindFilter {
    All,
    Chats,
    Channels,
}

impl SessionKindFilter {
    const ALL: [Self; 3] = [Self::All, Self::Chats, Self::Channels];

    fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Chats => "Chats",
            Self::Channels => "Channels",
        }
    }

    fn heading(self) -> &'static str {
        match self {
            Self::All => "CONVERSATIONS",
            Self::Chats => "CHATS",
            Self::Channels => "CHANNELS",
        }
    }

    fn semantic_name(self) -> &'static str {
        match self {
            Self::All => "Show all conversations",
            Self::Chats => "Show chats only",
            Self::Channels => "Show channels only",
        }
    }

    fn matches(self, kind: ConversationKind) -> bool {
        match self {
            Self::All => true,
            Self::Chats => kind == ConversationKind::Chat,
            Self::Channels => kind == ConversationKind::Channel,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChannelAgentRef {
    participant_id: String,
    agent_name: String,
    agent_key: String,
}

/// State backing the chat tab.
pub struct ChatState {
    pub data_dir: Arc<SinomoDataDir>,
    sifs_daemon: Option<sessions::SifsDaemonAccess>,
    pub handle: ChatHandle,
    pub session_id: String,
    pub info: RuntimeInfo,
    pub messages: Vec<ChatMessage>,
    pub input: String,
    pub cursor: usize,
    draft_revision: u64,
    pub streaming: bool,
    pub notice: Option<String>,
    pub context: Option<ChatContextSnapshot>,
    pub usage: Option<ChatUsageSnapshot>,
    pub activity: Vec<ChatActivityEntry>,
    pub tool_calls: Vec<ChatToolActivity>,
    pub control: ChatControlOverview,
    activity_panel_open: bool,
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
    session_filter: String,
    session_kind_filter: SessionKindFilter,
    pending_session_reset: Option<String>,
    session_edit_id: Option<String>,
    session_title_draft: String,
    session_delete_confirm_id: Option<String>,
    pending_delete_id: Option<String>,
    delete_rx: Option<Receiver<DeleteWorkerResult>>,
    channel_create_rx: Option<Receiver<ChannelCreateWorkerResult>>,
    session_list_rx: Option<Receiver<SessionListWorkerResult>>,
    session_list_loading: bool,
    session_list_started_at: Option<Instant>,
    session_list_slow_notice_shown: bool,
    channel_agent_idx: usize,
}

struct DeleteWorkerResult {
    session: SessionRef,
    kind: ConversationKind,
    result: Result<(), String>,
}

struct ChannelCreateWorkerResult {
    session: SessionRef,
    result: Result<SessionListWorkerResult, String>,
}

struct SessionListWorkerResult {
    sessions: Vec<SessionRef>,
    error: Option<String>,
}

impl ChatState {
    pub fn new(backend: &Backend) -> Self {
        Self::new_with_waker(backend, None)
    }

    pub fn new_with_waker(backend: &Backend, waker: Option<sui::UiHandle>) -> Self {
        let daemon_connected = backend.connection.is_daemon();
        let daemon_address = backend.connection.daemon_address().map(str::to_string);
        let sifs_storage = daemon_address
            .as_deref()
            .map(|address| AgentChatSifsStorage::new(address, backend.app_runtime.handle()));
        let event_wake: Option<ChatEventWake> =
            waker.map(|waker| Arc::new(move || waker.wake()) as ChatEventWake);
        let handle = ChatHandle::spawn(
            (*backend.data_dir).clone(),
            daemon_connected,
            backend.agent_runtime_control.clone(),
            sifs_storage,
            event_wake,
        );

        let agents = agent_summaries(&backend.data_dir);
        let providers = crate::backend::config::cluster_provider_summaries(backend.config.as_ref());
        let efforts = reasoning_efforts();
        let effort_idx = efforts.iter().position(|e| e == "medium").unwrap_or(0);
        let sifs_daemon = daemon_address
            .map(|address| sessions::SifsDaemonAccess::new(address, backend.app_runtime.handle()));
        let (sessions, session_list_error) = if sifs_daemon.is_some() {
            (Vec::new(), None)
        } else {
            list_conversations_with_error(&backend.data_dir, None)
        };

        let mut state = Self {
            data_dir: backend.data_dir.clone(),
            sifs_daemon,
            handle,
            session_id: String::new(),
            info: RuntimeInfo::default(),
            messages: Vec::new(),
            input: String::new(),
            cursor: 0,
            draft_revision: 0,
            streaming: false,
            notice: None,
            context: None,
            usage: None,
            activity: Vec::new(),
            tool_calls: Vec::new(),
            control: ChatControlOverview::default(),
            activity_panel_open: false,
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
            session_filter: String::new(),
            session_kind_filter: SessionKindFilter::All,
            pending_session_reset: None,
            session_edit_id: None,
            session_title_draft: String::new(),
            session_delete_confirm_id: None,
            pending_delete_id: None,
            delete_rx: None,
            channel_create_rx: None,
            session_list_rx: None,
            session_list_loading: false,
            session_list_started_at: None,
            session_list_slow_notice_shown: false,
            channel_agent_idx: 0,
        };
        if let Some(detail) = session_list_error {
            state.notice = Some(format!("chat list failed: {detail}"));
            state.replace_activity("chat list", detail, ActivityTone::Warn);
        }
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

    /// Persist the current chat and refresh the sidebar list.
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
        match self.save_chat_record(session_id, &title, &stored) {
            Ok(_) => {}
            Err(error) if report_visible_failures => {
                let detail = error.to_string();
                self.notice = Some(format!("chat save failed: {detail}"));
                self.push_activity("chat save", detail, ActivityTone::Warn);
            }
            Err(_) => {}
        }
        self.refresh_sessions();
    }

    fn refresh_sessions(&mut self) {
        let (sessions, error) =
            list_conversations_with_error(&self.data_dir, self.sifs_daemon.as_ref());
        self.apply_session_list_result(SessionListWorkerResult { sessions, error });
    }

    pub fn start_session_list_load(&mut self, waker: Option<sui::UiHandle>) {
        if self.sifs_daemon.is_none() {
            self.refresh_sessions();
            return;
        }
        if self.session_list_loading {
            return;
        }
        self.session_list_slow_notice_shown = false;
        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let worker_waker = waker.clone();
        let spawn_result = thread::Builder::new()
            .name("sinomo-app-chat-list".to_string())
            .spawn(move || {
                let (sessions, error) =
                    list_conversations_with_error(&data_dir, sifs_daemon.as_ref());
                let _ = tx.send(SessionListWorkerResult { sessions, error });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });

        if let Err(error) = spawn_result {
            let detail = error.to_string();
            self.notice = Some(format!("chat list failed: {detail}"));
            self.replace_activity("chat list", detail, ActivityTone::Warn);
            self.session_list_rx = None;
            self.session_list_loading = false;
            self.session_list_started_at = None;
            self.session_list_slow_notice_shown = false;
            return;
        }

        self.session_list_rx = Some(rx);
        self.session_list_loading = true;
        self.session_list_started_at = Some(Instant::now());
    }

    pub fn session_list_is_loading(&self) -> bool {
        self.session_list_loading
    }

    pub fn pump_session_list(&mut self) -> bool {
        let result = match self.session_list_rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(result)) => result,
            Some(Err(TryRecvError::Empty)) => {
                if self.session_list_loading
                    && !self.session_list_slow_notice_shown
                    && self
                        .session_list_started_at
                        .is_some_and(|started| started.elapsed() >= SESSION_LIST_UI_TIMEOUT)
                {
                    let detail = format!(
                        "SIFS chat list is still loading after {} seconds",
                        SESSION_LIST_UI_TIMEOUT.as_secs()
                    );
                    self.session_list_slow_notice_shown = true;
                    self.replace_activity("chat list", detail, ActivityTone::Running);
                    return true;
                }
                return false;
            }
            None => return false,
            Some(Err(TryRecvError::Disconnected)) => {
                self.session_list_rx = None;
                self.session_list_loading = false;
                self.session_list_started_at = None;
                self.session_list_slow_notice_shown = false;
                self.notice = Some("chat list failed: worker stopped".to_string());
                self.replace_activity(
                    "chat list",
                    "worker stopped before loading chats from SIFS".to_string(),
                    ActivityTone::Warn,
                );
                return true;
            }
        };
        self.session_list_rx = None;
        self.session_list_loading = false;
        self.session_list_started_at = None;
        self.session_list_slow_notice_shown = false;
        self.apply_session_list_result(result);
        true
    }

    pub fn pump_worker_events(&mut self) -> bool {
        let events = self.handle.drain();
        let changed = !events.is_empty();
        for event in events {
            self.apply_event(event);
        }
        changed
    }

    fn apply_session_list_result(&mut self, result: SessionListWorkerResult) {
        let SessionListWorkerResult { sessions, error } = result;
        self.sessions = sessions;
        match error {
            Some(detail) => {
                self.notice = Some(format!("chat list failed: {detail}"));
                self.replace_activity("chat list", detail, ActivityTone::Warn);
            }
            None => {
                if self
                    .notice
                    .as_deref()
                    .is_some_and(|notice| notice.starts_with("chat list failed:"))
                {
                    self.notice = None;
                }
                self.activity.retain(|entry| entry.title != "chat list");
            }
        }
    }

    fn upsert_session_ref(&mut self, session: SessionRef) {
        self.sessions.retain(|existing| existing.id != session.id);
        self.sessions.push(session);
        sort_conversations(&mut self.sessions);
    }

    #[cfg(test)]
    fn retry_chat_list(&mut self) {
        self.refresh_sessions();
    }

    fn retry_chat_list_with_waker(&mut self, waker: Option<sui::UiHandle>) {
        self.start_session_list_load(waker);
    }

    fn save_chat_record(
        &self,
        session_id: &str,
        title: &str,
        stored: &[StoredMessage],
    ) -> std::io::Result<()> {
        #[cfg(test)]
        if self.sifs_daemon.is_none() {
            return sessions::save_with_report(&self.data_dir, session_id, title, stored);
        }
        sessions::save_with_report_via_daemon(
            &self.data_dir,
            self.sifs_daemon.as_ref(),
            session_id,
            title,
            stored,
        )
    }

    fn load_chat_transcript(&self, session_id: &str) -> Option<sessions::StoredTranscript> {
        #[cfg(test)]
        if self.sifs_daemon.is_none() {
            return sessions::load_transcript(&self.data_dir, session_id);
        }
        sessions::load_transcript_via_daemon(&self.data_dir, self.sifs_daemon.as_ref(), session_id)
    }

    fn rename_chat_record(&self, session_id: &str, title: &str) -> std::io::Result<()> {
        #[cfg(test)]
        let sifs_result = if self.sifs_daemon.is_none() {
            sessions::rename(&self.data_dir, session_id, title)
        } else {
            sessions::rename_via_daemon(
                &self.data_dir,
                self.sifs_daemon.as_ref(),
                session_id,
                title,
            )
        };
        #[cfg(not(test))]
        let sifs_result = sessions::rename_via_daemon(
            &self.data_dir,
            self.sifs_daemon.as_ref(),
            session_id,
            title,
        );
        let channel_result =
            rename_chat_channel(&self.data_dir, self.sifs_daemon.as_ref(), session_id, title);
        match (sifs_result, channel_result) {
            (Ok(()), _) | (_, Ok(())) => Ok(()),
            (Err(error), Err(_)) => Err(error),
        }
    }

    fn agent_channel_store(&self) -> std::io::Result<SifsAgentChannelStore> {
        open_agent_channel_store(&self.data_dir, self.sifs_daemon.as_ref())
    }

    fn rename_channel(&self, id: &str, title: &str) -> std::io::Result<()> {
        let channel_id = parse_channel_id(id)?;
        let store = self.agent_channel_store()?;
        let mut channel = store
            .load_channel(&channel_id)
            .map_err(channel_store_io_error)?
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "channel not found")
            })?;
        channel.channel_kind = AgentChannelKind::Channel;
        channel.rename(title);
        store.save_channel(&channel).map_err(channel_store_io_error)
    }

    fn load_channel_messages(&self, id: &str) -> Option<Vec<ChatMessage>> {
        let channel_id = id.parse::<SessionId>().ok()?;
        let channel = self
            .agent_channel_store()
            .ok()?
            .load_channel(&channel_id)
            .ok()
            .flatten()?;
        Some(channel_messages(&channel))
    }

    fn channel_agent_refs(&self, id: &str) -> Vec<ChannelAgentRef> {
        let Ok(channel_id) = id.parse::<SessionId>() else {
            return Vec::new();
        };
        let Ok(store) = self.agent_channel_store() else {
            return Vec::new();
        };
        let Ok(Some(channel)) = store.load_channel(&channel_id) else {
            return Vec::new();
        };
        let mut participants = channel.participants.clone();
        participants.sort_by_key(|participant| participant.order);
        participants
            .into_iter()
            .filter(|participant| participant.enabled)
            .map(|participant| ChannelAgentRef {
                participant_id: participant.participant_id.to_string(),
                agent_name: participant.agent_name,
                agent_key: participant.agent_key,
            })
            .collect()
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

    fn replace_activity(
        &mut self,
        title: impl Into<String>,
        detail: impl Into<String>,
        tone: ActivityTone,
    ) {
        let title = title.into();
        self.activity.retain(|entry| entry.title != title);
        self.push_activity(title, detail, tone);
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

        if let Some(provider) = self.current_provider() {
            let model = self.current_model();
            if !model.is_empty() {
                return RuntimeHealth {
                    label: "READY",
                    detail: format!("{} / {}", provider.id, model),
                    tone: ActivityTone::Good,
                };
            }
        }

        RuntimeHealth {
            label: "SETUP",
            detail: "choose a provider and model".to_string(),
            tone: ActivityTone::Neutral,
        }
    }

    fn activity_panel_needs_attention(&self) -> bool {
        matches!(
            self.runtime_health().tone,
            ActivityTone::Running | ActivityTone::Warn | ActivityTone::Bad
        )
    }

    fn activity_panel_visible(&self) -> bool {
        self.activity_panel_open || self.activity_panel_needs_attention()
    }

    fn toggle_activity_panel(&mut self) {
        self.activity_panel_open = !self.activity_panel_open;
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

    fn runtime_recovery_target(&self) -> Option<RuntimeRecoveryTarget> {
        if self.connection_attention_detail().is_some() {
            return Some(RuntimeRecoveryTarget::Connection);
        }

        if self.providers.is_empty() {
            return Some(RuntimeRecoveryTarget::Providers);
        }

        let health = self.runtime_health();
        if !matches!(health.tone, ActivityTone::Warn | ActivityTone::Bad) {
            return None;
        }

        let detail = health.detail.to_ascii_lowercase();
        if ["chat list", "chat save", "sifs", "dfs", "daemon", "cluster"]
            .iter()
            .any(|needle| detail.contains(needle))
        {
            return Some(RuntimeRecoveryTarget::Connection);
        }

        if [
            "auth",
            "credential",
            "provider",
            "model",
            "api key",
            "sign-in",
            "sign in",
            "login",
        ]
        .iter()
        .any(|needle| detail.contains(needle))
        {
            Some(RuntimeRecoveryTarget::Providers)
        } else {
            Some(RuntimeRecoveryTarget::Connection)
        }
    }

    fn shows_recovery_empty_card(&self) -> bool {
        self.messages.is_empty() && self.runtime_recovery_target().is_some()
    }

    pub fn connection_attention_detail(&self) -> Option<String> {
        self.connection_attention_detail_full()
            .map(|detail| compact_line(&detail, 112))
    }

    pub fn connection_attention_detail_full(&self) -> Option<String> {
        let mut candidates = Vec::new();
        if let Some(notice) = self.notice.as_deref().filter(|text| !text.is_empty()) {
            candidates.push(notice.to_string());
        }
        for task in self
            .control
            .tasks
            .iter()
            .filter(|task| task_tone(&task.status) == ActivityTone::Bad)
        {
            candidates.push(format!("{}: {}", task.status, task.summary));
        }
        for entry in self
            .activity
            .iter()
            .rev()
            .filter(|entry| matches!(entry.tone, ActivityTone::Warn | ActivityTone::Bad))
        {
            candidates.push(format!("{}: {}", entry.title, entry.detail));
        }

        if let Some(detail) = candidates
            .into_iter()
            .find(|detail| text_mentions_connection_problem(detail))
        {
            return Some(detail);
        }

        let health = self.runtime_health();
        if matches!(health.tone, ActivityTone::Warn | ActivityTone::Bad)
            && text_mentions_connection_problem(&health.detail)
        {
            Some(health.detail)
        } else {
            None
        }
    }

    fn should_offer_chat_list_retry(&self) -> bool {
        self.connection_attention_detail_full()
            .is_some_and(|detail| detail.to_ascii_lowercase().contains("chat list"))
    }

    fn should_offer_sifs_check(&self) -> bool {
        self.connection_attention_detail_full()
            .is_some_and(|detail| detail_should_offer_sifs_check(&detail))
    }

    fn recovery_empty_detail(&self, target: RuntimeRecoveryTarget) -> String {
        match target {
            RuntimeRecoveryTarget::Providers if self.providers.is_empty() => {
                "No language model provider is configured yet.".to_string()
            }
            RuntimeRecoveryTarget::Providers => compact_line(&self.runtime_health().detail, 112),
            RuntimeRecoveryTarget::Connection => self
                .connection_attention_detail_full()
                .as_deref()
                .and_then(connection_recovery_display_detail)
                .unwrap_or_else(|| {
                    let detail = self
                        .runtime_degraded_detail()
                        .unwrap_or_else(|| self.runtime_health().detail);
                    connection_recovery_display_detail(&detail)
                        .unwrap_or_else(|| compact_line(&detail, 112))
                }),
        }
    }

    fn recovery_empty_title(&self, target: RuntimeRecoveryTarget) -> &'static str {
        match target {
            RuntimeRecoveryTarget::Providers => target.empty_title(),
            RuntimeRecoveryTarget::Connection => {
                let detail = self
                    .connection_attention_detail_full()
                    .unwrap_or_else(|| self.runtime_health().detail);
                let lower = detail.to_ascii_lowercase();
                if lower.contains("channel create") || lower.contains("channel store") {
                    "Channel needs connection attention"
                } else if lower.contains("chat list") {
                    "Chat list needs connection attention"
                } else if lower.contains("chat save") {
                    "Chat save needs connection attention"
                } else {
                    target.empty_title()
                }
            }
        }
    }

    fn recovery_empty_hint(&self, target: RuntimeRecoveryTarget) -> &'static str {
        match target {
            RuntimeRecoveryTarget::Providers => target.empty_hint(),
            RuntimeRecoveryTarget::Connection => {
                let detail = self
                    .connection_attention_detail_full()
                    .unwrap_or_else(|| self.runtime_health().detail);
                let lower = detail.to_ascii_lowercase();
                if lower.contains("channel create") {
                    "Open Connection settings if channel creation keeps failing; inspect SIFS from Files for storage diagnostics."
                } else if lower.contains("chat save") {
                    "Open Connection settings if saving keeps failing; retry the chat list after access is restored."
                } else {
                    target.empty_hint()
                }
            }
        }
    }

    fn event_belongs_to_visible_session(&self, session_id: &str) -> bool {
        session_id.is_empty() || self.session_id.is_empty() || self.session_id == session_id
    }

    fn route_background_session_event(&mut self, session_id: &str, event: ChatEvent) -> bool {
        if self.event_belongs_to_visible_session(session_id) {
            return false;
        }

        let mut transcript = self.load_chat_transcript(session_id).unwrap_or_default();
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
            ChatEvent::AgentToken { speaker, text, .. } => {
                Self::append_model_token_from_speaker(&mut messages, &speaker.agent_name, &text);
                true
            }
            ChatEvent::SystemMessage { kind, text, .. } => {
                Self::apply_system_message(&mut messages, &kind, &text);
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

    fn append_model_token_from_speaker(
        messages: &mut Vec<ChatMessage>,
        speaker: &str,
        token: &str,
    ) {
        let speaker = if speaker.trim().is_empty() {
            "Agent"
        } else {
            speaker.trim()
        };
        let prefix = format!("{speaker}: ");
        if let Some(last) = messages.last_mut()
            && last.role == ChatRole::Agent
            && last.source == ChatMessageSource::Model
            && (last.text.is_empty() || last.text.starts_with(&prefix))
        {
            if last.text.is_empty() {
                last.text.push_str(&prefix);
            }
            last.text.push_str(token);
            return;
        }
        messages.push(ChatMessage::model(format!("{prefix}{token}")));
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

    fn apply_system_message(messages: &mut Vec<ChatMessage>, kind: &str, text: &str) {
        let message_kind = if kind == "error" {
            ChatMessageKind::Error
        } else {
            ChatMessageKind::Info
        };
        if let Some(last) = messages.last_mut()
            && last.is_model_placeholder()
        {
            last.text = text.to_string();
            last.source = ChatMessageSource::System;
            last.kind = message_kind;
        } else if message_kind == ChatMessageKind::Error {
            messages.push(ChatMessage::system_error(text));
        } else {
            messages.push(ChatMessage::system_info(text));
        }
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

    fn ensure_visible_session_id(&mut self) -> String {
        if self.session_id.is_empty() {
            self.session_id = SessionId::generate().to_string();
        }
        self.session_id.clone()
    }

    fn reset_visible_session(&mut self, session_id: String) {
        let preserved_notice = self
            .notice
            .as_ref()
            .filter(|notice| text_mentions_connection_problem(notice))
            .cloned();
        let preserved_activity: Vec<ChatActivityEntry> = self
            .activity
            .iter()
            .filter(|entry| {
                matches!(entry.tone, ActivityTone::Warn | ActivityTone::Bad)
                    && text_mentions_connection_problem(&format!(
                        "{} {}",
                        entry.title, entry.detail
                    ))
            })
            .cloned()
            .collect();

        self.session_id = session_id;
        self.messages.clear();
        self.input.clear();
        self.cursor = 0;
        self.streaming = false;
        self.notice = preserved_notice;
        self.context = None;
        self.usage = None;
        self.activity = preserved_activity;
        self.tool_calls.clear();
        self.control = ChatControlOverview::default();
        self.scroll = 0.0;
        self.stick_bottom = true;
        self.session_edit_id = None;
        self.session_title_draft.clear();
        self.session_delete_confirm_id = None;
    }

    fn visible_session_has_started(&self) -> bool {
        self.messages
            .iter()
            .any(|message| message.role == ChatRole::User)
    }

    fn current_history(&self) -> Vec<ChatHistoryMessage> {
        self.messages
            .iter()
            .map(|message| ChatHistoryMessage {
                role: message.history_role().to_string(),
                text: message.text.clone(),
            })
            .collect()
    }

    fn current_session_ref(&self) -> Option<&SessionRef> {
        self.sessions
            .iter()
            .find(|session| session.id == self.session_id)
    }

    fn current_is_channel(&self) -> bool {
        self.current_session_ref()
            .is_some_and(|session| session.kind == ConversationKind::Channel)
    }

    fn empty_conversation_title(&self) -> String {
        if let Some(session) = self.current_session_ref() {
            if session.kind == ConversationKind::Channel {
                format!("Channel: {}", session.title)
            } else {
                session.title.clone()
            }
        } else {
            let agent = if self.info.agent.is_empty() {
                "Sinomo"
            } else {
                self.info.agent.as_str()
            };
            format!("Chat with {agent}")
        }
    }

    fn empty_conversation_hint(&self) -> String {
        if let Some(session) = self
            .current_session_ref()
            .filter(|session| session.kind == ConversationKind::Channel)
        {
            format!(
                "{} · send a message to the channel.",
                active_agents_label(session.participant_count)
            )
        } else {
            "Type a message below to begin.".to_string()
        }
    }

    fn begin_edit_session(&mut self, session_id: &str) {
        let Some(session) = self
            .sessions
            .iter()
            .find(|session| session.id == session_id)
            .cloned()
        else {
            return;
        };
        self.session_edit_id = Some(session.id);
        self.session_title_draft = session.title;
        self.session_delete_confirm_id = None;
    }

    fn begin_edit_current_session(&mut self) {
        let Some(session_id) = self.current_session_ref().map(|session| session.id.clone()) else {
            return;
        };
        self.begin_edit_session(&session_id);
    }

    fn close_session_edit(&mut self) {
        self.session_edit_id = None;
        self.session_title_draft.clear();
        self.session_delete_confirm_id = None;
    }

    fn set_session_title_draft(&mut self, title: String) {
        self.session_title_draft = title;
    }

    fn set_session_filter(&mut self, filter: String) {
        self.session_filter = filter;
    }

    fn set_session_kind_filter(&mut self, filter: SessionKindFilter) {
        self.session_kind_filter = filter;
    }

    fn can_save_session_title(&self) -> bool {
        let Some(edit_id) = self.session_edit_id.as_deref() else {
            return false;
        };
        let title = self.session_title_draft.trim();
        !title.is_empty()
            && self
                .sessions
                .iter()
                .find(|session| session.id == edit_id)
                .is_some_and(|session| session.title != title)
    }

    fn save_session_title(&mut self) {
        let Some(edit_id) = self.session_edit_id.clone() else {
            return;
        };
        let kind = self
            .sessions
            .iter()
            .find(|session| session.id == edit_id)
            .map(|session| session.kind)
            .unwrap_or(ConversationKind::Chat);
        let title = self.session_title_draft.trim().to_string();
        if title.is_empty() {
            self.notice = Some("chat title cannot be empty".to_string());
            return;
        }
        let result = match kind {
            ConversationKind::Chat => self.rename_chat_record(&edit_id, &title),
            ConversationKind::Channel => self.rename_channel(&edit_id, &title),
        };
        match result {
            Ok(()) => {
                self.session_title_draft = title;
                self.refresh_sessions();
            }
            Err(error) => {
                let detail = error.to_string();
                let label = if kind == ConversationKind::Channel {
                    "channel"
                } else {
                    "chat"
                };
                self.notice = Some(format!("{label} rename failed: {detail}"));
                self.push_activity(format!("{label} rename"), detail, ActivityTone::Warn);
            }
        }
    }

    fn request_delete_session(&mut self, session_id: &str) {
        let Some(session) = self
            .sessions
            .iter()
            .find(|session| session.id == session_id)
            .cloned()
        else {
            return;
        };
        if self.pending_delete_id.is_some() {
            self.notice = Some("wait for the current delete to finish".to_string());
            return;
        }
        if self.streaming || self.control.active_turn {
            let label = if session.kind == ConversationKind::Channel {
                "channel"
            } else {
                "chat"
            };
            self.notice = Some(format!(
                "wait for the current turn before deleting this {label}"
            ));
            return;
        }
        self.session_edit_id = Some(session.id.clone());
        self.session_title_draft = session.title;
        self.session_delete_confirm_id = Some(session.id);
    }

    fn cancel_delete_session(&mut self) {
        self.session_delete_confirm_id = None;
    }

    fn confirm_delete_session(&mut self, waker: Option<sui::UiHandle>) {
        let Some(session_id) = self.session_delete_confirm_id.clone() else {
            return;
        };
        let Some(session) = self
            .sessions
            .iter()
            .find(|session| session.id == session_id)
            .cloned()
        else {
            self.session_delete_confirm_id = None;
            self.session_edit_id = None;
            self.session_title_draft.clear();
            return;
        };
        let kind = session.kind;
        if self.streaming || self.control.active_turn {
            let label = if kind == ConversationKind::Channel {
                "channel"
            } else {
                "chat"
            };
            self.notice = Some(format!(
                "wait for the current turn before deleting this {label}"
            ));
            return;
        }
        if self.pending_delete_id.is_some() {
            self.notice = Some("wait for the current delete to finish".to_string());
            return;
        }

        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let worker_session_id = session_id.clone();
        let worker_session = session.clone();
        let worker_waker = waker.clone();
        let spawn_result = thread::Builder::new()
            .name("sinomo-app-chat-delete".to_string())
            .spawn(move || {
                let result = delete_conversation_records(
                    &data_dir,
                    sifs_daemon.as_ref(),
                    &worker_session_id,
                    kind,
                )
                .map_err(|error| error.to_string());
                let _ = tx.send(DeleteWorkerResult {
                    session: worker_session,
                    kind,
                    result,
                });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });

        if let Err(error) = spawn_result {
            let label = if kind == ConversationKind::Channel {
                "channel"
            } else {
                "chat"
            };
            self.notice = Some(format!("{label} delete failed: {error}"));
            self.push_activity(
                format!("{label} delete"),
                error.to_string(),
                ActivityTone::Warn,
            );
            return;
        }

        self.delete_rx = Some(rx);
        self.pending_delete_id = Some(session_id.clone());
        self.session_delete_confirm_id = None;
        self.session_edit_id = None;
        self.session_title_draft.clear();
        self.sessions.retain(|session| session.id != session_id);
        if self.session_id == session_id {
            let new_session_id = SessionId::generate().to_string();
            self.reset_visible_session(new_session_id.clone());
            self.pending_session_reset = Some(new_session_id.clone());
            self.handle.send_command(ChatCommand::NewSession {
                session_id: new_session_id,
            });
        }
        let label = if kind == ConversationKind::Channel {
            "channel"
        } else {
            "chat"
        };
        self.notice = Some(format!("deleted {label}"));
    }

    pub fn pump_delete(&mut self) -> bool {
        let mut changed = false;
        loop {
            let result = match self.delete_rx.as_ref().map(|rx| rx.try_recv()) {
                Some(Ok(result)) => result,
                Some(Err(TryRecvError::Empty)) | None => break,
                Some(Err(TryRecvError::Disconnected)) => {
                    if let Some(session_id) = self.pending_delete_id.take() {
                        self.notice = Some("chat delete failed: worker stopped".to_string());
                        self.push_activity(
                            "chat delete",
                            format!("worker stopped before deleting {session_id}"),
                            ActivityTone::Warn,
                        );
                        changed = true;
                    }
                    self.delete_rx = None;
                    break;
                }
            };
            if self.apply_delete_result(result) {
                changed = true;
            }
        }
        changed
    }

    pub fn pump_channel_create(&mut self) -> bool {
        let result = match self.channel_create_rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(result)) => result,
            Some(Err(TryRecvError::Empty)) | None => return false,
            Some(Err(TryRecvError::Disconnected)) => {
                self.channel_create_rx = None;
                self.notice = Some("channel create failed: worker stopped".to_string());
                self.push_activity(
                    "channel create",
                    "worker stopped before saving the channel to SIFS".to_string(),
                    ActivityTone::Warn,
                );
                return true;
            }
        };

        self.channel_create_rx = None;
        match result.result {
            Ok(list_result) => {
                let session = result.session;
                self.apply_session_list_result(list_result);
                if !self
                    .sessions
                    .iter()
                    .any(|existing| existing.id == session.id)
                {
                    self.upsert_session_ref(session);
                }
                if self
                    .notice
                    .as_deref()
                    .is_some_and(|notice| notice == "creating channel...")
                {
                    self.notice = None;
                }
                self.replace_activity(
                    "channel create",
                    "Saved channel to SIFS".to_string(),
                    ActivityTone::Good,
                );
            }
            Err(detail) => {
                self.notice = Some(format!("channel create failed: {detail}"));
                self.push_activity("channel create", detail, ActivityTone::Warn);
            }
        }
        true
    }

    fn apply_delete_result(&mut self, result: DeleteWorkerResult) -> bool {
        if self.pending_delete_id.as_deref() != Some(result.session.id.as_str()) {
            return false;
        }
        self.pending_delete_id = None;
        self.delete_rx = None;
        let label = if result.kind == ConversationKind::Channel {
            "channel"
        } else {
            "chat"
        };
        match result.result {
            Ok(()) => {
                self.push_activity(format!("{label} delete"), "completed", ActivityTone::Good);
            }
            Err(detail) => {
                if !self
                    .sessions
                    .iter()
                    .any(|session| session.id == result.session.id)
                {
                    self.sessions.push(result.session);
                    sort_conversations(&mut self.sessions);
                }
                self.notice = Some(format!("{label} delete failed: {detail}"));
                self.push_activity(format!("{label} delete"), detail, ActivityTone::Warn);
            }
        }
        true
    }

    /// Open a stored chat: load its transcript for display and tell the worker
    /// to continue under that id.
    fn open_session(&mut self, index: usize) {
        let Some(session) = self.sessions.get(index).cloned() else {
            return;
        };
        let id = session.id.clone();
        let messages: Vec<ChatMessage> = match session.kind {
            ConversationKind::Chat => self
                .load_chat_transcript(&id)
                .unwrap_or_default()
                .messages
                .into_iter()
                .map(ChatMessage::from_stored)
                .collect(),
            ConversationKind::Channel => self.load_channel_messages(&id).unwrap_or_else(|| {
                self.load_chat_transcript(&id)
                    .unwrap_or_default()
                    .messages
                    .into_iter()
                    .map(ChatMessage::from_stored)
                    .collect()
            }),
        };
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
        self.session_edit_id = None;
        self.session_title_draft.clear();
        self.session_delete_confirm_id = None;
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
        if let Some(session_id) = event.session_id().map(str::to_string) {
            if self.session_id.is_empty() {
                self.session_id = session_id.clone();
            } else if self.route_background_session_event(&session_id, event.clone()) {
                return false;
            }
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
            ChatEvent::AgentToken { speaker, text, .. } => {
                self.notice = None;
                Self::append_model_token_from_speaker(
                    &mut self.messages,
                    &speaker.agent_name,
                    &text,
                );
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
            }
            ChatEvent::Notice { notice, .. } => self.notice = Some(notice),
            ChatEvent::SystemMessage { kind, text, .. } => {
                Self::apply_system_message(&mut self.messages, &kind, &text);
                self.notice = None;
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
            }
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
                if let Some(detail) = connection_problem_summary(&error) {
                    self.push_activity("chat connection", detail.clone(), ActivityTone::Warn);
                    if !self
                        .notice
                        .as_deref()
                        .is_some_and(text_mentions_connection_problem)
                    {
                        self.notice = Some(format!("chat connection failed: {detail}"));
                    }
                } else {
                    self.push_activity("agent error", error.clone(), ActivityTone::Bad);
                    self.notice = None;
                }
                Self::apply_error_message(&mut self.messages, &error);
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
        let session_id = self.ensure_visible_session_id();
        if self.streaming {
            self.handle.send_command(ChatCommand::FollowUp {
                session_id: session_id.clone(),
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
        self.handle
            .send_command(ChatCommand::Send { session_id, text });
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

    fn clear_draft(&mut self) {
        if self.input.is_empty() {
            return;
        }
        self.input.clear();
        self.cursor = 0;
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    fn apply_quick_action(&mut self, action: ChatQuickAction) {
        if action == ChatQuickAction::ManageChannel {
            self.begin_edit_current_session();
            return;
        }
        let Some(prompt) = action.prompt() else {
            return;
        };
        self.input = prompt.to_string();
        self.cursor = self.input.len();
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    /// Select an agent by sidebar index and tell the worker to switch.
    fn select_agent(&mut self, index: usize) {
        let Some(agent) = self.agents.get(index) else {
            return;
        };
        if self.current_is_channel() {
            self.channel_agent_idx = index;
            self.begin_edit_current_session();
            self.notice = Some("use channel config to add agents".to_string());
            return;
        }
        if self.visible_session_has_started() {
            self.notice = Some("start a new chat before switching agents".to_string());
            return;
        }
        let key = agent.key.clone();
        let session_id = self.ensure_visible_session_id();
        let history = self.current_history();
        self.info.agent = key.clone();
        self.handle.send_command(ChatCommand::SetAgent {
            session_id,
            key,
            history,
        });
    }

    /// Whether `index` is the currently-active agent (matched by key or display name).
    fn is_active_agent(&self, index: usize) -> bool {
        self.agents
            .get(index)
            .map(|a| a.name == self.info.agent || a.key == self.info.agent)
            .unwrap_or(false)
    }

    /// Start a fresh session from the sidebar "New chat" action.
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

    /// Start a fresh explicit channel from the sidebar "New channel" action.
    fn new_channel(&mut self, waker: Option<sui::UiHandle>) {
        if self.channel_create_rx.is_some() {
            self.notice = Some("wait for the current channel to finish creating".to_string());
            return;
        }
        self.persist();
        let interrupted_session_id = self.session_id.clone();
        let session_id = SessionId::generate().to_string();
        if self.streaming || self.control.active_turn {
            self.handle.send_command(ChatCommand::Interrupt {
                session_id: interrupted_session_id,
                reason: "user started a new channel from app chat".to_string(),
            });
        }
        let mut channel = AgentChannel::new_channel(
            session_id
                .parse::<SessionId>()
                .unwrap_or_else(|_| SessionId::generate()),
            "New channel",
        );
        channel.ensure_user(sinomo_agent::AgentChannelUser::app_local());
        let (agent_key, agent_name) = self.default_channel_agent();
        channel.ensure_participant(agent_key, agent_name);
        let session_ref = session_ref_for_channel(&channel);

        self.reset_visible_session(session_id.clone());
        self.pending_session_reset = Some(session_id.clone());
        self.upsert_session_ref(session_ref.clone());
        self.session_edit_id = Some(session_id.clone());
        self.session_title_draft = session_ref.title.clone();
        self.session_delete_confirm_id = None;
        self.channel_agent_idx = 0;
        self.notice = Some("creating channel...".to_string());
        self.replace_activity(
            "channel create",
            "Saving channel to SIFS".to_string(),
            ActivityTone::Running,
        );
        self.handle
            .send_command(ChatCommand::NewSession { session_id });

        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let worker_channel = channel;
        let worker_session = session_ref;
        let worker_waker = waker.clone();
        let spawn_result = thread::Builder::new()
            .name("sinomo-app-channel-create".to_string())
            .spawn(move || {
                let result = open_agent_channel_store(&data_dir, sifs_daemon.as_ref())
                    .and_then(|store| {
                        store
                            .save_channel(&worker_channel)
                            .map_err(channel_store_io_error)?;
                        let (sessions, error) =
                            list_conversations_with_error(&data_dir, sifs_daemon.as_ref());
                        Ok(SessionListWorkerResult { sessions, error })
                    })
                    .map_err(|error| error.to_string());
                let _ = tx.send(ChannelCreateWorkerResult {
                    session: worker_session,
                    result,
                });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });

        match spawn_result {
            Ok(_) => {
                self.channel_create_rx = Some(rx);
            }
            Err(error) => {
                self.channel_create_rx = None;
                let detail = error.to_string();
                self.notice = Some(format!("channel create failed: {detail}"));
                self.push_activity("channel create", detail, ActivityTone::Warn);
            }
        }
    }

    fn default_channel_agent(&self) -> (String, String) {
        if let Some(agent) = self
            .agents
            .iter()
            .find(|agent| agent.key == self.info.agent || agent.name == self.info.agent)
            .or_else(|| self.agents.first())
        {
            return (agent.key.clone(), agent.name.clone());
        }
        let key = if self.info.agent.trim().is_empty() {
            "main-agent".to_string()
        } else {
            self.info.agent.clone()
        };
        (key.clone(), key)
    }

    fn set_channel_agent_idx(&mut self, index: usize) {
        self.channel_agent_idx = index.min(self.agents.len().saturating_sub(1));
    }

    fn add_selected_agent_to_channel(&mut self) {
        let Some(channel_id) = self.session_edit_id.clone() else {
            return;
        };
        if !self
            .sessions
            .iter()
            .any(|session| session.id == channel_id && session.kind == ConversationKind::Channel)
        {
            return;
        }
        let Some(agent) = self.agents.get(self.channel_agent_idx) else {
            return;
        };
        let agent_key = agent.key.clone();
        let agent_name = agent.name.clone();
        let store = match self.agent_channel_store() {
            Ok(store) => store,
            Err(error) => {
                let detail = error.to_string();
                self.notice = Some(format!("channel config failed: {detail}"));
                self.push_activity("channel config", detail, ActivityTone::Warn);
                return;
            }
        };
        let Ok(parsed_channel_id) = channel_id.parse::<SessionId>() else {
            self.notice = Some("channel config failed: invalid channel id".to_string());
            return;
        };
        let mut channel = match store.load_channel(&parsed_channel_id) {
            Ok(Some(channel)) => channel,
            Ok(None) => AgentChannel::new_channel(parsed_channel_id, "New channel"),
            Err(error) => {
                let detail = error.to_string();
                self.notice = Some(format!("channel config failed: {detail}"));
                self.push_activity("channel config", detail, ActivityTone::Warn);
                return;
            }
        };
        channel.channel_kind = AgentChannelKind::Channel;
        channel.ensure_participant(agent_key, agent_name.clone());
        match store.save_channel(&channel) {
            Ok(()) => {
                self.refresh_sessions();
                self.notice = Some(format!("added {agent_name} to channel"));
            }
            Err(error) => {
                let detail = error.to_string();
                self.notice = Some(format!("channel config failed: {detail}"));
                self.push_activity("channel config", detail, ActivityTone::Warn);
            }
        }
    }

    fn remove_channel_agent(&mut self, channel_id: &str, participant_id: &str) {
        let Ok(parsed_channel_id) = channel_id.parse::<SessionId>() else {
            self.notice = Some("channel config failed: invalid channel id".to_string());
            return;
        };
        let Ok(parsed_participant_id) = participant_id.parse::<sinomo_agent::TaskId>() else {
            self.notice = Some("channel config failed: invalid participant id".to_string());
            return;
        };
        let store = match self.agent_channel_store() {
            Ok(store) => store,
            Err(error) => {
                let detail = error.to_string();
                self.notice = Some(format!("channel config failed: {detail}"));
                self.push_activity("channel config", detail, ActivityTone::Warn);
                return;
            }
        };
        let mut channel = match store.load_channel(&parsed_channel_id) {
            Ok(Some(channel)) => channel,
            Ok(None) => {
                self.notice = Some("channel config failed: channel not found".to_string());
                return;
            }
            Err(error) => {
                let detail = error.to_string();
                self.notice = Some(format!("channel config failed: {detail}"));
                self.push_activity("channel config", detail, ActivityTone::Warn);
                return;
            }
        };
        if !channel.disable_participant(&parsed_participant_id) {
            self.notice = Some("channel config failed: agent not found".to_string());
            return;
        }
        match store.save_channel(&channel) {
            Ok(()) => {
                self.refresh_sessions();
                self.notice = Some("removed agent from channel".to_string());
            }
            Err(error) => {
                let detail = error.to_string();
                self.notice = Some(format!("channel config failed: {detail}"));
                self.push_activity("channel config", detail, ActivityTone::Warn);
            }
        }
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

fn list_conversations_with_error(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&sessions::SifsDaemonAccess>,
) -> (Vec<SessionRef>, Option<String>) {
    let (chat_metas, chat_error) = list_chat_metas_with_error(data_dir, sifs_daemon);
    let mut conversations = chat_metas
        .into_iter()
        .map(|meta| SessionRef {
            id: meta.id,
            title: meta.title,
            kind: ConversationKind::Chat,
            updated_unix: meta.updated_unix,
            participant_count: 1,
        })
        .collect::<Vec<_>>();

    let channel_result = open_agent_channel_store(data_dir, sifs_daemon).and_then(|store| {
        store
            .list_channel_summaries()
            .map_err(channel_store_io_error)
    });
    let channel_error = match channel_result {
        Ok(channels) => {
            let mut seen_channel_ids = BTreeSet::new();
            for channel in channels {
                if channel.channel_kind != AgentChannelKind::Channel {
                    continue;
                }
                let id = channel.channel_id.to_string();
                if !seen_channel_ids.insert(id.clone()) {
                    continue;
                }
                conversations.retain(|conversation| {
                    !(conversation.id == id && conversation.kind == ConversationKind::Chat)
                });
                conversations.push(SessionRef {
                    id,
                    title: if channel.title.trim().is_empty() {
                        "New channel".to_string()
                    } else {
                        channel.title
                    },
                    kind: ConversationKind::Channel,
                    updated_unix: channel.updated_at.unix_timestamp(),
                    participant_count: channel.enabled_participant_count,
                });
            }
            None
        }
        Err(error) => Some(error.to_string()),
    };

    sort_conversations(&mut conversations);
    let error = chat_error
        .filter(|_| conversations.is_empty())
        .or_else(|| channel_error.filter(|_| conversations.is_empty()));
    (conversations, error)
}

fn session_ref_for_channel(channel: &AgentChannel) -> SessionRef {
    SessionRef {
        id: channel.channel_id.to_string(),
        title: if channel.title.trim().is_empty() {
            "New channel".to_string()
        } else {
            channel.title.clone()
        },
        kind: ConversationKind::Channel,
        updated_unix: channel.updated_at.unix_timestamp(),
        participant_count: channel.enabled_participants_in_order().len(),
    }
}

fn list_chat_metas_with_error(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&sessions::SifsDaemonAccess>,
) -> (Vec<sessions::SessionMeta>, Option<String>) {
    #[cfg(test)]
    if sifs_daemon.is_none() {
        return (sessions::list(data_dir), None);
    }
    match sessions::list_with_report_via_daemon(data_dir, sifs_daemon) {
        Ok(metas) => (metas, None),
        Err(error) => (Vec::new(), Some(error)),
    }
}

fn sort_conversations(conversations: &mut [SessionRef]) {
    conversations.sort_by(|left, right| {
        right
            .updated_unix
            .cmp(&left.updated_unix)
            .then_with(|| left.title.cmp(&right.title))
    });
}

fn delete_conversation_records(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&sessions::SifsDaemonAccess>,
    id: &str,
    kind: ConversationKind,
) -> std::io::Result<()> {
    match kind {
        ConversationKind::Chat => {
            let chat_result = delete_chat_record(data_dir, sifs_daemon, id);
            let channel_result = tombstone_channel_record_in(data_dir, sifs_daemon, id);
            match (chat_result, channel_result) {
                (Ok(()), _) | (_, Ok(true)) => Ok(()),
                (Err(error), Ok(false)) | (Err(error), Err(_)) => Err(error),
            }
        }
        ConversationKind::Channel => {
            let result = tombstone_channel_record_in(data_dir, sifs_daemon, id).map(|_| ());
            let _ = delete_chat_record(data_dir, sifs_daemon, id);
            result
        }
    }
}

fn delete_chat_record(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&sessions::SifsDaemonAccess>,
    id: &str,
) -> std::io::Result<()> {
    #[cfg(test)]
    if sifs_daemon.is_none() {
        return sessions::delete(data_dir, id);
    }
    sessions::delete_via_daemon(data_dir, sifs_daemon, id)
}

fn tombstone_channel_record_in(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&sessions::SifsDaemonAccess>,
    id: &str,
) -> std::io::Result<bool> {
    let channel_id = parse_channel_id(id)?;
    let store = open_agent_channel_store(data_dir, sifs_daemon)?;
    let mut channel = match store
        .load_channel(&channel_id)
        .map_err(channel_store_io_error)?
    {
        Some(channel) => channel,
        None => return Ok(false),
    };
    channel.tombstone();
    store
        .save_channel(&channel)
        .map_err(channel_store_io_error)?;
    Ok(true)
}

fn rename_chat_channel(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&sessions::SifsDaemonAccess>,
    id: &str,
    title: &str,
) -> std::io::Result<()> {
    let channel_id = parse_channel_id(id)?;
    let store = open_agent_channel_store(data_dir, sifs_daemon)?;
    let mut channel = store
        .load_channel(&channel_id)
        .map_err(channel_store_io_error)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "chat not found"))?;
    if channel.channel_kind != AgentChannelKind::Chat {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "chat not found",
        ));
    }
    channel.rename(title);
    store.save_channel(&channel).map_err(channel_store_io_error)
}

fn channel_messages(channel: &AgentChannel) -> Vec<ChatMessage> {
    channel
        .transcript
        .iter()
        .filter_map(|message| {
            if message.content.trim().is_empty() {
                return None;
            }
            match &message.author {
                AgentChannelAuthor::User { .. } => Some(ChatMessage::user(message.content.clone())),
                AgentChannelAuthor::Agent { agent_name, .. } => {
                    let label = if agent_name.trim().is_empty() {
                        "Agent"
                    } else {
                        agent_name
                    };
                    Some(ChatMessage::model(format!("{label}: {}", message.content)))
                }
                AgentChannelAuthor::System => {
                    if message.message_kind == AgentChannelMessageKind::Error {
                        Some(ChatMessage::system_error(message.content.clone()))
                    } else {
                        Some(ChatMessage::system_info(message.content.clone()))
                    }
                }
            }
        })
        .collect()
}

fn open_agent_channel_store(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&sessions::SifsDaemonAccess>,
) -> std::io::Result<SifsAgentChannelStore> {
    match sifs_daemon {
        Some(daemon) => daemon
            .open_browser()
            .map(SifsAgentChannelStore::from_browser)
            .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail)),
        None => open_local_agent_channel_store(data_dir),
    }
}

fn open_local_agent_channel_store(
    data_dir: &SinomoDataDir,
) -> std::io::Result<SifsAgentChannelStore> {
    SifsAgentChannelStore::open(data_dir)
        .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail))
}

fn parse_channel_id(id: &str) -> std::io::Result<SessionId> {
    id.parse::<SessionId>().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid channel id: {error}"),
        )
    })
}

fn channel_store_io_error(error: sinomo_agent::AgentChannelStoreError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, error.to_string())
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

fn secondary_button_theme(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || {
        let mut theme = reader();
        theme.palette.accent = theme.palette.control;
        theme.palette.accent_hover = theme.palette.control_hover;
        theme.palette.accent_pressed = theme.palette.control_active;
        theme.palette.accent_border = theme.palette.border;
        theme.palette.accent_border_hover = theme.palette.border_hover;
        theme.palette.accent_border_focus = theme.palette.border_focus;
        theme.palette.accent_text = theme.palette.text;
        theme
    }
}

fn danger_button_theme(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || {
        let mut theme = reader();
        let danger = theme.palette.danger;
        let contrast = theme.palette.danger_text;
        let border = theme.palette.border;
        theme.palette.accent = danger;
        theme.palette.accent_hover = mix_color(danger, contrast, 0.08);
        theme.palette.accent_pressed = mix_color(danger, contrast, 0.16);
        theme.palette.accent_border = mix_color(danger, border, 0.12);
        theme.palette.accent_border_hover = mix_color(danger, border, 0.20);
        theme.palette.accent_border_focus = danger;
        theme.palette.accent_text = contrast;
        theme.palette.focus_ring = danger.with_alpha(theme.palette.focus_ring.alpha);
        theme
    }
}

fn mix_color(base: Color, toward: Color, amount: f32) -> Color {
    let amount = amount.clamp(0.0, 1.0);
    Color::new(
        base.space,
        base.red + (toward.red - base.red) * amount,
        base.green + (toward.green - base.green) * amount,
        base.blue + (toward.blue - base.blue) * amount,
        base.alpha + (toward.alpha - base.alpha) * amount,
    )
    .clamped()
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

fn context_capacity_bytes(context: &ChatContextSnapshot) -> usize {
    context
        .max_context_bytes
        .or(context.compaction_threshold_bytes)
        .unwrap_or(context.total_bytes.max(1))
        .max(1)
}

fn context_count_label(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn context_overview_summary(chat: &ChatState) -> String {
    if let Some(context) = &chat.context {
        return format!(
            "Context {} of {} · {} · {}",
            format_bytes(context.total_bytes),
            format_bytes(context_capacity_bytes(context)),
            context_count_label(context.included_count, "source", "sources"),
            context_count_label(context.omitted_count, "omitted", "omitted")
        );
    }

    if chat.streaming {
        return "Assembling context for this response".to_string();
    }

    if let Some(target) = chat.runtime_recovery_target() {
        return match target {
            RuntimeRecoveryTarget::Providers => "Context waits for provider setup".to_string(),
            RuntimeRecoveryTarget::Connection => "Context waits for cluster connection".to_string(),
        };
    }

    if chat
        .messages
        .iter()
        .any(|message| message.source == ChatMessageSource::User)
    {
        "Context will refresh on the next send".to_string()
    } else {
        "Context will assemble when you send a message".to_string()
    }
}

fn runtime_overview_title(chat: &ChatState) -> String {
    if let Some(provider) = chat.current_provider() {
        let model = chat.current_model();
        if !model.is_empty() {
            return format!("{} / {}", provider.name, model);
        }
        return format!("{} / Choose a model", provider.name);
    }

    if !chat.info.provider.is_empty() && !chat.info.model.is_empty() {
        return format!("{} / {}", chat.info.provider, chat.info.model);
    }

    "Choose a provider".to_string()
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
        "auth",
        "credential",
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

fn text_mentions_connection_problem(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "chat connection",
        "chat list",
        "chat save",
        "chat unavailable",
        "sifs",
        "dfs",
        "daemon",
        "cluster",
        "device authorization",
        "session store",
        "channel store",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn connection_problem_summary(text: &str) -> Option<String> {
    if !text_mentions_connection_problem(text) {
        return None;
    }

    let lower = text.to_ascii_lowercase();
    let denied = lower.contains("device authorization") || lower.contains("denied");
    let summary = if lower.contains("agent channel") || lower.contains("channel store") {
        if denied {
            "Agent channel storage could not open through SIFS; device authorization was denied."
        } else {
            "Agent channel storage could not open through SIFS."
        }
    } else if lower.contains("chat list") {
        if denied {
            "Chat list storage could not be read; device authorization was denied."
        } else {
            "Chat list storage could not be read through SIFS."
        }
    } else if lower.contains("chat save") {
        if denied {
            "Chat history could not be saved; device authorization was denied."
        } else {
            "Chat history could not be saved through SIFS."
        }
    } else if lower.contains("device authorization") {
        "Device authorization blocked chat storage."
    } else if lower.contains("daemon") || lower.contains("cluster") {
        "Chat storage could not reach the cluster service."
    } else {
        "Chat storage needs SIFS access."
    };
    Some(summary.to_string())
}

fn connection_recovery_display_detail(text: &str) -> Option<String> {
    if !text_mentions_connection_problem(text) {
        return None;
    }

    let lower = text.to_ascii_lowercase();
    let summary = if lower.contains("agent channel")
        || lower.contains("channel create")
        || lower.contains("channel store")
    {
        "Sinomo could not open this chat channel."
    } else if lower.contains("chat list") {
        "Sinomo could not load saved chats."
    } else if lower.contains("chat save") {
        "Sinomo could not save chat history."
    } else if lower.contains("daemon") || lower.contains("cluster") {
        "The local cluster service is not reachable."
    } else {
        "Chat needs connection attention."
    };
    Some(summary.to_string())
}

fn activity_visible_detail(text: &str) -> String {
    connection_recovery_display_detail(text).unwrap_or_else(|| compact_line(text, 96))
}

fn detail_should_offer_sifs_check(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "sifs",
        "dfs",
        "session store",
        "channel store",
        "device authorization",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn activity_status_summary(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let summary = if lower.contains("agent channel")
        || lower.contains("channel create")
        || lower.contains("channel store")
    {
        Some("Channel storage needs recovery")
    } else if lower.contains("chat list") {
        Some("Chat history needs recovery")
    } else if lower.contains("chat save") {
        Some("Chat history cannot be saved")
    } else if lower.contains("daemon") || lower.contains("cluster") {
        Some("Cluster service needs attention")
    } else if text_mentions_connection_problem(text) {
        Some("Chat storage needs recovery")
    } else if lower.contains("runtime control metadata") && lower.contains("detached") {
        Some("Runtime controls need refresh")
    } else {
        None
    };

    summary
        .map(str::to_string)
        .unwrap_or_else(|| activity_visible_detail(text))
}

fn count_label(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn activity_work_summary(chat: &ChatState) -> String {
    let active = chat.streaming || chat.control.active_turn;
    let tasks = chat.control.task_count.max(chat.control.tasks.len());
    let tools = chat.tool_calls.len();
    let queued = chat.control.steering_count + chat.control.follow_up_count;

    if !active && tasks == 0 && tools == 0 && queued == 0 {
        return "Idle · no queued work".to_string();
    }

    let mut parts = vec![if active { "Active" } else { "Idle" }.to_string()];
    if tasks > 0 {
        parts.push(count_label(tasks, "task", "tasks"));
    }
    if queued > 0 {
        parts.push(count_label(queued, "queued turn", "queued turns"));
    }
    if tools > 0 {
        parts.push(count_label(tools, "tool update", "tool updates"));
    }
    parts.join(" · ")
}

fn activity_button_label(chat: &ChatState) -> &'static str {
    match chat.runtime_health().tone {
        ActivityTone::Bad | ActivityTone::Warn => "Review",
        ActivityTone::Running => "Monitor",
        ActivityTone::Good | ActivityTone::Neutral => "Details",
    }
}

fn activity_button_description(chat: &ChatState) -> String {
    let health = chat.runtime_health();
    let status = activity_status_summary(&health.detail);
    format!(
        "Show or hide runtime details. {}: {}. {}",
        health.label,
        compact_line(&status, 72),
        activity_work_summary(chat)
    )
}

fn usage_summary(chat: &ChatState) -> Option<String> {
    chat.usage.as_ref().map(|usage| {
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
}

fn transcript_message_display_text(message: &ChatMessage) -> String {
    if message.kind != ChatMessageKind::Error {
        return if message.text.is_empty() {
            "…".to_string()
        } else {
            message.text.clone()
        };
    }

    let detail = message
        .text
        .strip_prefix("Error:")
        .unwrap_or(message.text.as_str())
        .trim();
    if detail.is_empty() {
        return "Error\nNo detail was recorded.".to_string();
    }

    if text_mentions_connection_problem(detail) {
        let lower = detail.to_ascii_lowercase();
        let denied = lower.contains("device authorization") || lower.contains("denied");
        let summary = if lower.contains("agent channel") || lower.contains("channel store") {
            if denied {
                "This chat needs storage authorization."
            } else {
                "Sinomo could not open this chat channel."
            }
        } else if lower.contains("chat list") {
            "Saved chats could not be loaded."
        } else if lower.contains("chat save") {
            "This chat could not be saved."
        } else if lower.contains("device authorization") {
            "This chat action needs storage authorization."
        } else if lower.contains("daemon") || lower.contains("cluster") {
            "The local cluster service is not reachable."
        } else {
            "The chat runtime could not reach a required cluster service."
        };
        let next = if detail_should_offer_sifs_check(detail) {
            "Check SIFS first. Connection settings has low-level detail."
        } else {
            "Open Connection settings for low-level detail."
        };
        format!("Connection issue\n{summary}\n{next}")
    } else {
        format!("Error\n{detail}")
    }
}

fn transcript_message_recovery_action(message: &ChatMessage) -> Option<RecoveryCardAction> {
    if message.kind != ChatMessageKind::Error || !text_mentions_connection_problem(&message.text) {
        return None;
    }

    if detail_should_offer_sifs_check(&message.text) {
        Some(RecoveryCardAction::CheckSifs)
    } else {
        Some(RecoveryCardAction::OpenConnectionSettings)
    }
}

/// A small uppercase section heading `Label` (AGENTS / CHATS), color tracking the theme.
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

fn chats_heading(shared: &Shared) -> impl Widget + use<> {
    let s = shared.clone();
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || {
                let chat = s.chat();
                chats_heading_text(&chat)
            }
        },
        move |text| WidgetPod::new(heading(&s, text)),
    )
}

fn muted_label(shared: &Shared, text: impl Into<String>) -> Label {
    let s = shared.clone();
    Label::new(text).color_when(move || palette(&s).text_muted)
}

fn dialog_section_label(shared: &Shared, text: impl Into<String>) -> Label {
    let s = shared.clone();
    let theme = shared.theme();
    Label::new(text)
        .style(sui::TextStyle {
            font_size: 12.0,
            line_height: 18.0,
            weight: FontWeight::SEMIBOLD,
            ..theme.body_text_style()
        })
        .color_when(move || palette(&s).text_muted)
}

fn session_meta_label(session: &SessionRef) -> String {
    let updated = session_updated_label(session.updated_unix);
    match session.kind {
        ConversationKind::Chat => format!("Chat · {updated}"),
        ConversationKind::Channel => {
            format!("{} · {updated}", plural_agents(session.participant_count))
        }
    }
}

fn session_kind_badge_label(kind: ConversationKind) -> &'static str {
    match kind {
        ConversationKind::Chat => "Chat",
        ConversationKind::Channel => "Channel",
    }
}

fn session_kind_badge_width(kind: ConversationKind) -> f32 {
    match kind {
        ConversationKind::Chat => 42.0,
        ConversationKind::Channel => 62.0,
    }
}

fn normalized_session_filter(filter: &str) -> String {
    filter.trim().to_ascii_lowercase()
}

fn session_kind_filter_label(kind: ConversationKind) -> &'static str {
    match kind {
        ConversationKind::Chat => "chat",
        ConversationKind::Channel => "channel",
    }
}

fn session_matches_filter(session: &SessionRef, filter: &str) -> bool {
    if filter.is_empty() {
        return true;
    }
    session.title.to_ascii_lowercase().contains(filter)
        || session_meta_label(session)
            .to_ascii_lowercase()
            .contains(filter)
        || session_kind_filter_label(session.kind).contains(filter)
}

fn session_matches_kind_filter(session: &SessionRef, kind_filter: SessionKindFilter) -> bool {
    kind_filter.matches(session.kind)
}

fn session_kind_filter_count(sessions: &[SessionRef], kind_filter: SessionKindFilter) -> usize {
    sessions
        .iter()
        .filter(|session| session_matches_kind_filter(session, kind_filter))
        .count()
}

fn filtered_session_indices(
    sessions: &[SessionRef],
    filter: &str,
    kind_filter: SessionKindFilter,
) -> Vec<usize> {
    sessions
        .iter()
        .enumerate()
        .filter_map(|(index, session)| {
            (session_matches_kind_filter(session, kind_filter)
                && session_matches_filter(session, filter))
            .then_some(index)
        })
        .collect()
}

fn chats_heading_text(chat: &ChatState) -> String {
    let kind_filter = chat.session_kind_filter;
    let total = chat
        .sessions
        .iter()
        .filter(|session| session_matches_kind_filter(session, kind_filter))
        .count();
    let filter = normalized_session_filter(&chat.session_filter);
    if filter.is_empty() {
        format!("{} · {total}", kind_filter.heading())
    } else {
        let shown = chat
            .sessions
            .iter()
            .filter(|session| {
                session_matches_kind_filter(session, kind_filter)
                    && session_matches_filter(session, &filter)
            })
            .count();
        format!("{} · {shown}/{total}", kind_filter.heading())
    }
}

fn empty_sessions_label(filter: &str, kind_filter: SessionKindFilter) -> &'static str {
    if !filter.is_empty() {
        return "No matching conversations";
    }
    match kind_filter {
        SessionKindFilter::All => "No conversations yet",
        SessionKindFilter::Chats => "No chats yet",
        SessionKindFilter::Channels => "No channels yet",
    }
}

fn plural_agents(count: usize) -> String {
    if count == 1 {
        "1 agent".to_string()
    } else {
        format!("{count} agents")
    }
}

fn active_agents_label(count: usize) -> String {
    if count == 1 {
        "1 active agent".to_string()
    } else {
        format!("{count} active agents")
    }
}

fn session_updated_label(updated_unix: i64) -> String {
    if updated_unix <= 0 {
        return "not saved".to_string();
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(updated_unix);
    let age = now.saturating_sub(updated_unix);
    if age < 60 {
        "just now".to_string()
    } else if age < 60 * 60 {
        format!("{}m ago", age / 60)
    } else if age < 24 * 60 * 60 {
        format!("{}h ago", age / (60 * 60))
    } else {
        format!("{}d ago", age / (24 * 60 * 60))
    }
}

fn agent_list_detail(agent: &AgentSummary) -> String {
    match agent.key.as_str() {
        "main-agent" => return "Interactive assistant".to_string(),
        "coding-agent" => return "Code and verification".to_string(),
        "evolution-agent" => return "Self-evolution workflows".to_string(),
        "background-agent" => return "Maintenance + cluster support".to_string(),
        _ => {}
    }
    let description = agent.description.trim();
    if !description.is_empty() {
        return compact_line(description, 36);
    }
    "Agent participant".to_string()
}

// ---------------------------------------------------------------------------
// Sidebar — agents + sessions ListViews (rebuilt when the lists change).
// ---------------------------------------------------------------------------

/// Build the AGENTS ListView: one row per agent, the active one selected, selecting one switches.
fn agents_list(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let items: Vec<ListItem> = {
        let chat = shared.chat();
        chat.agents
            .iter()
            .map(|agent| {
                let mut item = ListItem::new(agent.name.clone())
                    .detail(agent_list_detail(agent))
                    .leading_icon(IconGlyph::Sparkles);
                if chat.info.agent == agent.key {
                    item = item.trailing("Active");
                }
                item
            })
            .collect()
    };
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

/// Build the CHATS ListView: stored chats/channels only. Creation actions live
/// above the scroll area so the list itself behaves like persistent history.
fn sessions_list(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (sessions, filter, kind_filter, loading) = {
        let chat = shared.chat();
        (
            chat.sessions.clone(),
            normalized_session_filter(&chat.session_filter),
            chat.session_kind_filter,
            chat.session_list_loading,
        )
    };
    let visible_indices = filtered_session_indices(&sessions, &filter, kind_filter);
    let mut items = visible_indices
        .iter()
        .map(|index| {
            let session = sessions[*index].clone();
            ListItem::new(session.title.clone())
                .with_child(ChatSessionRow::new(shared.clone(), session))
        })
        .collect::<Vec<_>>();
    if loading && sessions.is_empty() {
        items.push(
            ListItem::new("Loading chats...")
                .leading_text("...")
                .disabled(),
        );
    } else if visible_indices.is_empty() {
        items.push(
            ListItem::new(empty_sessions_label(&filter, kind_filter))
                .leading_text(" ")
                .disabled(),
        );
    }
    let s = shared.clone();
    ListView::new("chat-sessions")
        .items(items)
        .selected_when({
            let s = s.clone();
            move || {
                let chat = s.chat();
                let filter = normalized_session_filter(&chat.session_filter);
                filtered_session_indices(&chat.sessions, &filter, chat.session_kind_filter)
                    .into_iter()
                    .position(|session_index| chat.sessions[session_index].id == chat.session_id)
                    .map(|index| index)
            }
        })
        .row_height(SIDEBAR_ROW_H)
        .theme_when(clone_reader(reader))
        .on_change(move |index, _label| {
            let mut chat = s.chat_mut();
            let filter = normalized_session_filter(&chat.session_filter);
            let visible_indices =
                filtered_session_indices(&chat.sessions, &filter, chat.session_kind_filter);
            if let Some(session_index) = visible_indices.get(index) {
                chat.open_session(*session_index);
            }
        })
}

fn session_create_actions(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let new_chat = {
        let s = shared.clone();
        sui::Button::new("New chat")
            .icon(IconGlyph::Chat)
            .min_height(32.0)
            .theme_when(clone_reader(reader))
            .on_press(move || {
                s.chat_mut().new_session();
            })
    };
    let new_channel = {
        let s = shared.clone();
        sui::Button::new("New channel")
            .icon(IconGlyph::Add)
            .min_height(32.0)
            .theme_when(secondary_button_theme(reader))
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.chat_mut().new_channel(waker);
            })
    };

    Flex::vertical()
        .main_gap(6.0)
        .align_items(Alignment::Stretch)
        .with_child(new_chat)
        .with_child(new_channel)
}

fn session_filter_input(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let value = shared.chat().session_filter.clone();
    TextInput::new("chat-session-filter")
        .value(value)
        .placeholder("Search chats")
        .theme_when(clone_reader(reader))
        .on_change(move |value| {
            s.chat_mut().set_session_filter(value);
        })
}

fn sessions_scroll_signature(
    shared: &Shared,
) -> (
    String,
    String,
    SessionKindFilter,
    String,
    String,
    String,
    String,
    usize,
    bool,
) {
    let chat = shared.chat();
    let sessions = chat
        .sessions
        .iter()
        .map(|s| format!("{}:{:?}:{}:{}", s.id, s.kind, s.title, s.participant_count))
        .collect::<Vec<_>>()
        .join("\u{1f}");
    (
        sessions,
        chat.session_filter.clone(),
        chat.session_kind_filter,
        chat.session_id.clone(),
        chat.session_edit_id.clone().unwrap_or_default(),
        chat.session_delete_confirm_id.clone().unwrap_or_default(),
        chat.pending_delete_id.clone().unwrap_or_default(),
        chat.channel_agent_idx,
        chat.session_list_loading,
    )
}

fn sessions_scroll(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    ScrollView::vertical(sessions_list(shared, reader))
        .name("chat-sessions-scroll")
        .state(scroll_state.clone())
        .theme(shared.theme())
}

fn sessions_scroll_rebuild(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    let sessions_scroll_state = scroll_state.clone();
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || sessions_scroll_signature(&s)
        },
        move |_sig| WidgetPod::new(sessions_scroll(&s, &r, &sessions_scroll_state)),
    )
}

struct SessionKindFilterControl {
    shared: Shared,
    hovered: Option<SessionKindFilter>,
    pressed: Option<SessionKindFilter>,
}

impl SessionKindFilterControl {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            hovered: None,
            pressed: None,
        }
    }

    fn active(&self) -> SessionKindFilter {
        self.shared.chat().session_kind_filter
    }

    fn slot_rect(bounds: Rect, slot: usize) -> Rect {
        let slot_count = SessionKindFilter::ALL.len().max(1);
        let width = bounds.width() / slot_count as f32;
        let x = bounds.x() + width * slot as f32;
        let w = if slot + 1 == slot_count {
            bounds.max_x() - x
        } else {
            width
        };
        Rect::new(x, bounds.y(), w.max(0.0), bounds.height())
    }

    fn hit_filter(bounds: Rect, position: Point) -> Option<SessionKindFilter> {
        if !bounds.contains(position) {
            return None;
        }
        let slot_count = SessionKindFilter::ALL.len().max(1);
        let slot_width = (bounds.width() / slot_count as f32).max(1.0);
        let slot = ((position.x - bounds.x()) / slot_width).floor() as usize;
        SessionKindFilter::ALL
            .get(slot.min(slot_count - 1))
            .copied()
    }

    fn widget_id(parent: sui::WidgetId, slot: usize) -> sui::WidgetId {
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(37)
                .wrapping_add(18_017)
                .wrapping_add(slot as u64),
        )
    }

    fn request_filter_refresh(ctx: &mut EventCtx) {
        use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};

        let window = InvalidationTarget::Window(ctx.window_id());
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
        ctx.request_paint();
        ctx.request_semantics();
    }
}

impl Widget for SessionKindFilterControl {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = Self::hit_filter(ctx.bounds(), pointer.position);
                if self.hovered != hovered {
                    self.hovered = hovered;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                let Some(filter) = Self::hit_filter(ctx.bounds(), pointer.position) else {
                    return;
                };
                self.pressed = Some(filter);
                ctx.request_pointer_capture(pointer.pointer_id);
                ctx.request_paint();
                ctx.set_handled();
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                let hit = Self::hit_filter(ctx.bounds(), pointer.position);
                if let (Some(pressed), Some(hit)) = (self.pressed.take(), hit)
                    && pressed == hit
                {
                    self.shared.chat_mut().set_session_kind_filter(hit);
                    Self::request_filter_refresh(ctx);
                }
                ctx.release_pointer_capture(pointer.pointer_id);
                ctx.set_handled();
            }
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Leave => {
                if self.hovered.take().is_some() {
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Cancel => {
                if self.pressed.take().is_some() {
                    ctx.release_pointer_capture(pointer.pointer_id);
                    ctx.request_paint();
                    ctx.set_handled();
                }
                if self.hovered.take().is_some() {
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            _ => {}
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(
            if constraints.max.width.is_finite() {
                constraints.max.width
            } else {
                210.0
            },
            28.0,
        ))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let sessions = { self.shared.chat().sessions.clone() };
        let active = self.active();
        widgets::round_panel(ctx, bounds, pal.field, pal.border.with_alpha(0.72), 7.0);
        for (slot, filter) in SessionKindFilter::ALL.iter().copied().enumerate() {
            let rect = Self::slot_rect(bounds, slot).inflate(-1.0, -1.0);
            let selected = filter == active;
            let fill = if self.pressed == Some(filter) || selected {
                pal.selected
            } else if self.hovered == Some(filter) {
                pal.hover
            } else {
                Color::srgba(0.0, 0.0, 0.0, 0.0)
            };
            let border = if selected {
                pal.accent.with_alpha(0.52)
            } else {
                Color::srgba(0.0, 0.0, 0.0, 0.0)
            };
            if selected || self.hovered == Some(filter) || self.pressed == Some(filter) {
                widgets::round_panel(ctx, rect, fill, border, 6.0);
            }
            let label = format!(
                "{} {}",
                filter.label(),
                session_kind_filter_count(&sessions, filter)
            );
            let text_color = if selected { pal.accent } else { pal.text_muted };
            ctx.push_clip_rect(rect);
            widgets::draw_text_in(
                ctx,
                Rect::new(rect.x() + 7.0, rect.y() + 5.0, rect.width() - 10.0, 16.0),
                &label,
                pal.text_style(&theme, 10.0, FontWeight::SEMIBOLD, text_color),
            );
            ctx.pop_clip();
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let sessions = { self.shared.chat().sessions.clone() };
        let active = self.active();
        for (slot, filter) in SessionKindFilter::ALL.iter().copied().enumerate() {
            let mut node = SemanticsNode::new(
                Self::widget_id(ctx.widget_id(), slot),
                SemanticsRole::Button,
                Self::slot_rect(ctx.bounds(), slot),
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some(filter.semantic_name().to_string());
            node.description = Some(format!(
                "{} visible conversation(s)",
                session_kind_filter_count(&sessions, filter)
            ));
            node.actions = vec![SemanticsAction::Activate];
            node.state.hovered = self.hovered == Some(filter);
            node.state.selected = active == filter;
            ctx.push(node);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChatSessionRowHit {
    Row,
    Edit,
}

struct ChatSessionRow {
    shared: Shared,
    session: SessionRef,
    hovered: bool,
    pressed: Option<ChatSessionRowHit>,
}

impl ChatSessionRow {
    fn new(shared: Shared, session: SessionRef) -> Self {
        Self {
            shared,
            session,
            hovered: false,
            pressed: None,
        }
    }

    fn active(&self) -> bool {
        self.shared.chat().session_id == self.session.id
    }

    fn editing(&self) -> bool {
        self.shared.chat().session_edit_id.as_deref() == Some(self.session.id.as_str())
    }

    fn show_edit_button(&self) -> bool {
        self.hovered || self.active() || self.editing()
    }

    fn edit_rect(&self, bounds: Rect) -> Rect {
        let side = 22.0_f32.min(bounds.height()).max(0.0);
        Rect::new(
            bounds.max_x() - side - 2.0,
            bounds.y() + (bounds.height() - side) * 0.5,
            side,
            side,
        )
    }

    fn hit_at(&self, bounds: Rect, position: Point) -> Option<ChatSessionRowHit> {
        if !bounds.contains(position) {
            return None;
        }
        if self.edit_rect(bounds).contains(position) {
            Some(ChatSessionRowHit::Edit)
        } else {
            Some(ChatSessionRowHit::Row)
        }
    }

    fn edit_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
        sui::WidgetId::new(parent.get().wrapping_mul(37).wrapping_add(9_011))
    }
}

impl Widget for ChatSessionRow {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = ctx.bounds().contains(pointer.position);
                if self.hovered != hovered {
                    self.hovered = hovered;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                let Some(hit) = self.hit_at(ctx.bounds(), pointer.position) else {
                    return;
                };
                self.pressed = Some(hit);
                ctx.request_focus();
                ctx.request_pointer_capture(pointer.pointer_id);
                ctx.request_paint();
                ctx.set_handled();
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                let hit = self.hit_at(ctx.bounds(), pointer.position);
                if let (Some(pressed), Some(hit)) = (self.pressed.take(), hit)
                    && pressed == hit
                {
                    match hit {
                        ChatSessionRowHit::Row => {
                            let index = {
                                let chat = self.shared.chat();
                                chat.sessions
                                    .iter()
                                    .position(|session| session.id == self.session.id)
                            };
                            if let Some(index) = index {
                                self.shared.chat_mut().open_session(index);
                            }
                        }
                        ChatSessionRowHit::Edit => {
                            self.shared.chat_mut().begin_edit_session(&self.session.id);
                        }
                    }
                    ctx.request_measure();
                    ctx.request_paint();
                    ctx.request_semantics();
                }
                ctx.release_pointer_capture(pointer.pointer_id);
                ctx.set_handled();
            }
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Leave => {
                if self.hovered {
                    self.hovered = false;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Cancel => {
                if self.pressed.take().is_some() {
                    ctx.release_pointer_capture(pointer.pointer_id);
                    ctx.request_paint();
                    ctx.set_handled();
                }
                if self.hovered {
                    self.hovered = false;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Keyboard(key)
                if ctx.is_focused()
                    && key.state == KeyState::Pressed
                    && matches!(key.key.as_str(), "Enter" | " ") =>
            {
                let index = {
                    let chat = self.shared.chat();
                    chat.sessions
                        .iter()
                        .position(|session| session.id == self.session.id)
                };
                if let Some(index) = index {
                    self.shared.chat_mut().open_session(index);
                    ctx.request_measure();
                    ctx.request_paint();
                    ctx.request_semantics();
                    ctx.set_handled();
                }
            }
            _ => {}
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(
            if constraints.max.width.is_finite() {
                constraints.max.width
            } else {
                210.0
            },
            SIDEBAR_ROW_H,
        ))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let active = self.active();
        let show_edit = self.show_edit_button();
        let edit = self.edit_rect(bounds);
        let label_color = if active { pal.text } else { pal.text_muted };
        let title_style = pal.text_style(&theme, 12.5, FontWeight::MEDIUM, label_color);
        let meta_style = pal.text_style(&theme, 10.5, FontWeight::NORMAL, pal.text_faint);
        let badge_style = pal.text_style(
            &theme,
            9.5,
            FontWeight::SEMIBOLD,
            if active { pal.accent } else { pal.text_faint },
        );
        let badge = Rect::new(
            bounds.x() + 2.0,
            bounds.y() + 7.0,
            session_kind_badge_width(self.session.kind),
            18.0,
        );
        let badge_fill = if active { pal.selected } else { pal.field };
        let badge_border = if active {
            pal.accent.with_alpha(0.42)
        } else {
            pal.border.with_alpha(0.72)
        };
        widgets::round_panel(ctx, badge, badge_fill, badge_border, 5.0);
        widgets::draw_text_in(
            ctx,
            Rect::new(badge.x() + 7.0, badge.y() + 1.0, badge.width() - 12.0, 15.0),
            session_kind_badge_label(self.session.kind),
            badge_style,
        );

        if show_edit {
            let (fill, border, glyph_color) = if self.pressed == Some(ChatSessionRowHit::Edit) {
                (pal.selected, pal.accent, pal.accent)
            } else if self.editing() {
                (pal.field, pal.accent, pal.accent)
            } else if active {
                (pal.field, pal.border_strong.with_alpha(0.58), pal.accent)
            } else {
                (pal.hover, pal.border.with_alpha(0.72), pal.text_muted)
            };
            widgets::round_panel(ctx, edit, fill, border, 6.0);
            draw_glyph(
                ctx,
                IconGlyph::MoreHorizontal,
                edit.inflate(-4.0, -4.0),
                glyph_color,
            );
        }

        let edit_gap = if show_edit { 28.0 } else { 0.0 };
        let title_x = badge.max_x() + 8.0;
        let title_right = bounds.max_x() - edit_gap - 5.0;
        let title_rect = Rect::new(
            title_x,
            bounds.y() + 2.0,
            (title_right - title_x).max(0.0),
            17.0,
        );
        ctx.push_clip_rect(title_rect);
        widgets::draw_text_in(ctx, title_rect, &self.session.title, title_style);
        ctx.pop_clip();

        let meta_rect = Rect::new(
            title_x,
            bounds.y() + 18.0,
            (title_right - title_x).max(0.0),
            15.0,
        );
        ctx.push_clip_rect(meta_rect);
        widgets::draw_text_in(
            ctx,
            meta_rect,
            &session_meta_label(&self.session),
            meta_style,
        );
        ctx.pop_clip();
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let row_id = ctx.widget_id();
        let mut node = SemanticsNode::new(row_id, SemanticsRole::ListItem, ctx.bounds());
        node.name = Some(self.session.title.clone());
        node.description = Some(match self.session.kind {
            ConversationKind::Chat => "Chat".to_string(),
            ConversationKind::Channel => {
                format!("Channel, {} agents", self.session.participant_count)
            }
        });
        node.state.focused = ctx.is_focused();
        node.state.hovered = self.hovered;
        node.state.selected = self.active();
        node.actions = vec![SemanticsAction::Focus, SemanticsAction::Activate];
        ctx.push(node);

        let mut edit = SemanticsNode::new(
            Self::edit_widget_id(row_id),
            SemanticsRole::Button,
            self.edit_rect(ctx.bounds()),
        );
        edit.parent = Some(row_id);
        edit.name = Some(format!("Edit {}", self.session.title));
        edit.description = Some(match self.session.kind {
            ConversationKind::Chat => "Rename or delete this chat".to_string(),
            ConversationKind::Channel => {
                "Rename, delete, or manage agents for this channel".to_string()
            }
        });
        edit.actions = vec![SemanticsAction::Activate];
        edit.state.hovered = self.hovered;
        edit.state.selected = self.editing();
        ctx.push(edit);
    }

    fn accepts_focus(&self) -> bool {
        true
    }
}

/// The sidebar column: AGENTS heading + list, then CHATS heading + list. Wrapped in a
/// [`RebuildOnChange`] keyed on the agent/chat signature so it rebuilds when those lists change
/// (e.g. a new chat is persisted) but not on every token.
fn sidebar(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    let sessions_scroll_state = scroll_state.clone();
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || sidebar_signature(&s)
        },
        move |_sig| {
            let column = Flex::vertical()
                .main_gap(4.0)
                .align_items(Alignment::Stretch)
                .with_child(SizedBox::new().height(18.0))
                .with_child(InsetX::new(heading(&s, "AGENTS"), 18.0))
                .with_child(SizedBox::new().height(2.0))
                .with_child(InsetX::new(agents_list(&s, &r), 8.0))
                .with_child(SizedBox::new().height(16.0))
                .with_child(InsetX::new(chats_heading(&s), 18.0))
                .with_child(SizedBox::new().height(6.0))
                .with_child(InsetX::new(
                    SizedBox::new()
                        .height(28.0)
                        .with_child(SessionKindFilterControl::new(s.clone())),
                    8.0,
                ))
                .with_child(SizedBox::new().height(6.0))
                .with_child(InsetX::new(
                    SizedBox::new()
                        .height(30.0)
                        .with_child(session_filter_input(&s, &r)),
                    8.0,
                ))
                .with_child(SizedBox::new().height(6.0))
                .with_child(InsetX::new(session_create_actions(&s, &r), 8.0))
                .with_child(SizedBox::new().height(8.0))
                .with_item(
                    InsetX::new(sessions_scroll_rebuild(&s, &r, &sessions_scroll_state), 8.0),
                    FlexItem::flex(1.0).min_height(0.0),
                );
            WidgetPod::new(Surface::sidebar(column).fill().theme_when(clone_reader(&r)))
        },
    )
}

/// A signature of the sidebar's list contents (agents are static; sessions + the active agent
/// change). Used to drive the sidebar [`RebuildOnChange`].
fn sidebar_signature(
    shared: &Shared,
) -> (
    String,
    String,
    SessionKindFilter,
    String,
    String,
    String,
    usize,
    bool,
) {
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
        .map(|s| format!("{}:{:?}:{}:{}", s.id, s.kind, s.title, s.participant_count))
        .collect::<Vec<_>>()
        .join("\u{1f}");
    (
        active,
        sessions,
        chat.session_kind_filter,
        chat.session_id.clone(),
        chat.session_edit_id.clone().unwrap_or_default(),
        chat.session_delete_confirm_id.clone().unwrap_or_default(),
        chat.channel_agent_idx,
        chat.session_list_loading,
    )
}

fn session_edit_dialog(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    let (session, draft, confirming_delete, participants, agent_keys, agent_options) = {
        let chat = shared.chat();
        let edit_id = chat.session_edit_id.as_deref()?;
        let session = chat
            .sessions
            .iter()
            .find(|session| session.id == edit_id)
            .cloned()?;
        let participants = if session.kind == ConversationKind::Channel {
            chat.channel_agent_refs(&session.id)
        } else {
            Vec::new()
        };
        let agent_keys = chat
            .agents
            .iter()
            .map(|agent| agent.key.clone())
            .collect::<Vec<_>>();
        let agent_options = chat
            .agents
            .iter()
            .map(|agent| agent.name.clone())
            .collect::<Vec<_>>();
        (
            session,
            chat.session_title_draft.clone(),
            chat.session_delete_confirm_id.as_deref() == Some(edit_id),
            participants,
            agent_keys,
            agent_options,
        )
    };
    let session_id = session.id.clone();
    let is_channel = session.kind == ConversationKind::Channel;
    let title = if is_channel {
        "Edit channel"
    } else {
        "Edit chat"
    };
    let placeholder = if is_channel {
        "Channel title"
    } else {
        "Chat title"
    };

    let input = {
        let s = shared.clone();
        TextInput::new(format!("chat-session-title-{session_id}"))
            .value(draft)
            .placeholder(placeholder)
            .theme_when(clone_reader(reader))
            .on_change(move |value| {
                s.chat_mut().set_session_title_draft(value);
            })
    };
    let save = {
        let s = shared.clone();
        sui::Button::new("Save")
            .icon(IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || s.chat().can_save_session_title()
            })
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().save_session_title();
                ctx.request_measure();
                ctx.request_paint();
            })
    };
    let done = {
        let s = shared.clone();
        sui::Button::new("Done")
            .icon(IconGlyph::Close)
            .theme_when(secondary_button_theme(reader))
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().close_session_edit();
                ctx.request_measure();
                ctx.request_paint();
            })
    };

    let mut body = Stack::vertical()
        .spacing(10.0)
        .alignment(Alignment::Stretch)
        .with_child(dialog_section_label(shared, "Name"))
        .with_child(SizedBox::new().height(32.0).with_child(input))
        .with_child(
            Flex::horizontal()
                .gap(6.0)
                .align_items(Alignment::Center)
                .spacer()
                .with_child(done)
                .with_child(save),
        );

    if is_channel {
        body = body
            .with_child(Separator::horizontal().theme_when(clone_reader(reader)))
            .with_child(channel_agents_editor(
                shared,
                reader,
                session_id.clone(),
                participants,
                agent_keys,
                agent_options,
            ));
    }

    body = body.with_child(Separator::horizontal().theme_when(clone_reader(reader)));
    let delete_label = if is_channel {
        "Delete channel"
    } else {
        "Delete chat"
    };
    if confirming_delete {
        let confirm_title = compact_line(&session.title, 34);
        let confirm = {
            let s = shared.clone();
            sui::Button::new(delete_label)
                .icon(IconGlyph::Trash)
                .theme_when(danger_button_theme(reader))
                .on_press_with_ctx(move |ctx| {
                    let waker = s.borrow().waker.clone();
                    s.chat_mut().confirm_delete_session(waker);
                    ctx.request_measure();
                    ctx.request_paint();
                })
        };
        let cancel = {
            let s = shared.clone();
            sui::Button::new("Cancel")
                .icon(IconGlyph::Close)
                .theme_when(secondary_button_theme(reader))
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().cancel_delete_session();
                    ctx.request_measure();
                    ctx.request_paint();
                })
        };
        body = body
            .with_child(dialog_section_label(shared, "Danger zone"))
            .with_child(muted_label(
                shared,
                format!("Delete \"{confirm_title}\" from SIFS?"),
            ))
            .with_child(
                Flex::horizontal()
                    .gap(6.0)
                    .align_items(Alignment::Center)
                    .with_child(cancel)
                    .spacer()
                    .with_child(confirm),
            );
    } else {
        let delete_session_id = session.id.clone();
        let delete = {
            let s = shared.clone();
            sui::Button::new(delete_label)
                .icon(IconGlyph::Trash)
                .theme_when(danger_button_theme(reader))
                .enabled_when({
                    let s = s.clone();
                    move || {
                        let chat = s.chat();
                        !chat.streaming
                            && !chat.control.active_turn
                            && chat.pending_delete_id.is_none()
                    }
                })
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().request_delete_session(&delete_session_id);
                    ctx.request_measure();
                    ctx.request_paint();
                })
        };
        let delete_hint = if is_channel {
            "Removes this channel timeline and its channel membership record."
        } else {
            "Removes this chat timeline from the SIFS-backed chat list."
        };
        body = body
            .with_child(dialog_section_label(shared, "Danger zone"))
            .with_child(muted_label(shared, delete_hint))
            .with_child(
                Flex::horizontal()
                    .gap(6.0)
                    .align_items(Alignment::Center)
                    .spacer()
                    .with_child(delete),
            );
    }

    let s = shared.clone();
    Some(WidgetPod::new(
        SessionEditPopup::new(title, body, shared.theme()).on_dismiss(move || {
            s.chat_mut().close_session_edit();
        }),
    ))
}

fn channel_agents_editor(
    shared: &Shared,
    reader: &ThemeReader,
    channel_id: String,
    participants: Vec<ChannelAgentRef>,
    agent_keys: Vec<String>,
    agent_options: Vec<String>,
) -> impl Widget + use<> {
    let participant_count = participants.len();
    let assigned_agent_keys = participants
        .iter()
        .map(|participant| participant.agent_key.clone())
        .collect::<Vec<_>>();
    let mut list = Stack::vertical()
        .spacing(4.0)
        .alignment(Alignment::Stretch)
        .with_child(
            Flex::horizontal()
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(dialog_section_label(shared, "Agents"))
                .spacer()
                .with_child(muted_label(shared, format!("{} active", participant_count))),
        );

    if participants.is_empty() {
        list = list.with_child(muted_label(shared, "No agents assigned yet."));
    } else {
        for participant in participants {
            let row_channel_id = channel_id.clone();
            let participant_id = participant.participant_id.clone();
            let label = if participant.agent_key == participant.agent_name {
                participant.agent_name
            } else {
                format!("{} ({})", participant.agent_name, participant.agent_key)
            };
            let remove_label = label.clone();
            let remove = {
                let s = shared.clone();
                IconButton::new(IconGlyph::Trash, format!("Remove {remove_label}"))
                    .size(26.0)
                    .icon_size(14.0)
                    .theme_when(clone_reader(reader))
                    .on_press_with_ctx(move |ctx| {
                        s.chat_mut()
                            .remove_channel_agent(&row_channel_id, &participant_id);
                        ctx.request_measure();
                        ctx.request_paint();
                    })
            };
            list = list.with_child(ChannelAgentRow::new(shared.clone(), label, remove));
        }
    }

    let picker = {
        let s = shared.clone();
        sui::Select::new(format!("channel-agent-picker-{channel_id}"))
            .options(agent_options)
            .placeholder("Agent")
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || {
                    let chat = s.chat();
                    (!chat.agents.is_empty()).then_some(
                        chat.channel_agent_idx
                            .min(chat.agents.len().saturating_sub(1)),
                    )
                }
            })
            .on_change(move |index, _label| {
                s.chat_mut().set_channel_agent_idx(index);
            })
    };
    let add = {
        let s = shared.clone();
        sui::Button::new("Add agent")
            .icon(IconGlyph::Add)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                let agent_keys = agent_keys.clone();
                let assigned_agent_keys = assigned_agent_keys.clone();
                move || {
                    let chat = s.chat();
                    let Some(agent_key) = agent_keys.get(chat.channel_agent_idx) else {
                        return false;
                    };
                    !assigned_agent_keys
                        .iter()
                        .any(|assigned_key| assigned_key == agent_key)
                }
            })
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().add_selected_agent_to_channel();
                ctx.request_measure();
                ctx.request_paint();
            })
    };

    Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(list)
        .with_child(SizedBox::new().height(SELECTOR_H).with_child(picker))
        .with_child(
            Flex::horizontal()
                .gap(6.0)
                .align_items(Alignment::Center)
                .spacer()
                .with_child(add),
        )
}

struct ChannelAgentRow {
    shared: Shared,
    label: String,
    remove: WidgetPod,
}

impl ChannelAgentRow {
    const HEIGHT: f32 = 32.0;
    const REMOVE_SIZE: f32 = 26.0;
    const GAP: f32 = 8.0;
    const ICON_SIZE: f32 = 15.0;

    fn new<W>(shared: Shared, label: String, remove: W) -> Self
    where
        W: Widget + 'static,
    {
        Self {
            shared,
            label,
            remove: WidgetPod::new(remove),
        }
    }
}

impl Widget for ChannelAgentRow {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.remove.measure(
            ctx,
            Constraints::tight(Size::new(Self::REMOVE_SIZE, Self::REMOVE_SIZE)),
        );
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            280.0
        };
        constraints.clamp(Size::new(width, Self::HEIGHT))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let y = bounds.y() + (bounds.height() - Self::REMOVE_SIZE) * 0.5;
        self.remove.arrange(
            ctx,
            Rect::new(
                bounds.max_x() - Self::REMOVE_SIZE,
                y,
                Self::REMOVE_SIZE,
                Self::REMOVE_SIZE,
            ),
        );
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        widgets::fill_round_rect(ctx, bounds, pal.field, 6.0);
        let icon_rect = Rect::new(
            bounds.x() + 9.0,
            bounds.y() + (bounds.height() - Self::ICON_SIZE) * 0.5,
            Self::ICON_SIZE,
            Self::ICON_SIZE,
        );
        draw_glyph(ctx, IconGlyph::Sparkles, icon_rect, pal.accent);
        let label_rect = Rect::new(
            icon_rect.max_x() + 7.0,
            bounds.y(),
            (bounds.max_x() - icon_rect.max_x() - Self::REMOVE_SIZE - Self::GAP - 7.0).max(0.0),
            bounds.height(),
        );
        let style = pal.text_style(&theme, 12.5, FontWeight::NORMAL, pal.text);
        ctx.push_clip_rect(label_rect);
        widgets::draw_text_in(ctx, label_rect, &self.label, style);
        ctx.pop_clip();
        self.remove.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.remove.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.remove);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.remove);
    }
}

struct SessionEditPopup {
    theme: Box<DefaultTheme>,
    title: String,
    body: WidgetPod,
    panel_rect: Rect,
    body_rect: Rect,
    close_hovered: bool,
    on_dismiss: Option<Box<dyn FnMut()>>,
}

impl SessionEditPopup {
    const OUTER_MARGIN: f32 = 24.0;
    const PANEL_MAX_WIDTH: f32 = 460.0;
    const PANEL_MIN_WIDTH: f32 = 300.0;
    const PANEL_PADDING: f32 = 16.0;
    const TITLE_HEIGHT: f32 = 26.0;
    const BODY_GAP: f32 = 12.0;
    const CORNER_RADIUS: f32 = 10.0;
    const CLOSE_SIZE: f32 = 28.0;

    fn new<W>(title: impl Into<String>, body: W, theme: DefaultTheme) -> Self
    where
        W: Widget + 'static,
    {
        Self {
            theme: Box::new(theme),
            title: title.into(),
            body: WidgetPod::new(body),
            panel_rect: Rect::ZERO,
            body_rect: Rect::ZERO,
            close_hovered: false,
            on_dismiss: None,
        }
    }

    fn on_dismiss<F>(mut self, on_dismiss: F) -> Self
    where
        F: FnMut() + 'static,
    {
        self.on_dismiss = Some(Box::new(on_dismiss));
        self
    }

    fn dismiss(&mut self, ctx: &mut EventCtx) {
        if let Some(on_dismiss) = &mut self.on_dismiss {
            on_dismiss();
        }
        ctx.request_measure();
        ctx.request_paint();
        ctx.request_semantics();
        ctx.set_handled();
    }

    fn title_style(&self) -> sui::TextStyle {
        sui::TextStyle {
            font_size: self.theme.metrics.dialog_title_font_size,
            line_height: self.theme.metrics.dialog_title_line_height,
            color: self.theme.palette.text,
            ..self.theme.body_text_style()
        }
    }

    fn close_rect(&self) -> Rect {
        Rect::new(
            self.panel_rect.max_x() - Self::PANEL_PADDING - Self::CLOSE_SIZE,
            self.panel_rect.y() + Self::PANEL_PADDING - 2.0,
            Self::CLOSE_SIZE,
            Self::CLOSE_SIZE,
        )
    }
}

impl Widget for SessionEditPopup {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = self.close_rect().contains(pointer.position);
                if self.close_hovered != hovered {
                    self.close_hovered = hovered;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary)
                    && self.close_rect().contains(pointer.position) =>
            {
                self.dismiss(ctx);
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary)
                    && !self.panel_rect.contains(pointer.position) =>
            {
                self.dismiss(ctx);
            }
            Event::Pointer(pointer)
                if matches!(
                    pointer.kind,
                    PointerEventKind::Leave | PointerEventKind::Cancel
                ) =>
            {
                if self.close_hovered {
                    self.close_hovered = false;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Keyboard(key) if key.state == KeyState::Pressed && key.key == "Escape" => {
                self.dismiss(ctx);
            }
            _ => {}
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let viewport = constraints.clamp(Size::new(
            if constraints.max.width.is_finite() {
                constraints.max.width
            } else {
                920.0
            },
            if constraints.max.height.is_finite() {
                constraints.max.height
            } else {
                640.0
            },
        ));
        let available_width = (viewport.width - Self::OUTER_MARGIN * 2.0).max(0.0);
        let fallback_min = available_width.min(Self::PANEL_MIN_WIDTH);
        let panel_width = available_width
            .min(Self::PANEL_MAX_WIDTH)
            .max(fallback_min)
            .max(0.0);
        let body_width = (panel_width - Self::PANEL_PADDING * 2.0).max(0.0);
        let body_max_height = (viewport.height
            - Self::OUTER_MARGIN * 2.0
            - Self::PANEL_PADDING * 2.0
            - Self::TITLE_HEIGHT
            - Self::BODY_GAP)
            .max(0.0);
        let body_size = self.body.measure(
            ctx,
            Constraints::new(Size::ZERO, Size::new(body_width, body_max_height)),
        );
        let panel_height =
            Self::PANEL_PADDING * 2.0 + Self::TITLE_HEIGHT + Self::BODY_GAP + body_size.height;
        let panel_x = ((viewport.width - panel_width) * 0.5).max(0.0);
        let panel_y = ((viewport.height - panel_height) * 0.5).max(Self::OUTER_MARGIN);
        self.panel_rect = Rect::new(panel_x, panel_y, panel_width, panel_height);
        self.body_rect = Rect::new(
            panel_x + Self::PANEL_PADDING,
            panel_y + Self::PANEL_PADDING + Self::TITLE_HEIGHT + Self::BODY_GAP,
            body_width,
            body_size.height,
        );
        viewport
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let panel_width = self.panel_rect.width().min(bounds.width().max(0.0));
        let panel_height = self.panel_rect.height().min(bounds.height().max(0.0));
        let panel_x = bounds.x() + ((bounds.width() - panel_width) * 0.5).max(0.0);
        let panel_y = bounds.y()
            + ((bounds.height() - panel_height) * 0.5)
                .max(Self::OUTER_MARGIN.min(bounds.height().max(0.0)));
        self.panel_rect = Rect::new(panel_x, panel_y, panel_width, panel_height);
        let body_size = self.body.measured_size();
        self.body_rect = Rect::new(
            panel_x + Self::PANEL_PADDING,
            panel_y + Self::PANEL_PADDING + Self::TITLE_HEIGHT + Self::BODY_GAP,
            body_size.width,
            body_size.height,
        );
        self.body.arrange(ctx, self.body_rect);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let panel = self.panel_rect;
        let scrim = if self.theme.surfaces.dark {
            Color::srgba(0.0, 0.0, 0.0, 0.42)
        } else {
            Color::srgba(0.08, 0.12, 0.18, 0.14)
        };
        ctx.fill_rect(ctx.bounds(), scrim);
        sui::paint_theme_shadow(
            ctx,
            panel,
            [Self::CORNER_RADIUS; 4],
            &self.theme.shadows.box_shadow.xl,
        );
        widgets::fill_round_rect(ctx, panel, self.theme.palette.border, Self::CORNER_RADIUS);
        widgets::fill_round_rect(
            ctx,
            panel.inflate(-1.0, -1.0),
            self.theme.palette.surface_raised,
            (Self::CORNER_RADIUS - 1.0).max(0.0),
        );

        let title_rect = Rect::new(
            panel.x() + Self::PANEL_PADDING,
            panel.y() + Self::PANEL_PADDING,
            (panel.width() - Self::PANEL_PADDING * 2.0 - Self::CLOSE_SIZE - 8.0).max(0.0),
            Self::TITLE_HEIGHT,
        );
        widgets::draw_text_in(ctx, title_rect, &self.title, self.title_style());
        let close = self.close_rect();
        if self.close_hovered {
            widgets::fill_round_rect(ctx, close, self.theme.palette.control_hover, 6.0);
        }
        draw_glyph(
            ctx,
            IconGlyph::Close,
            close.inflate(-6.0, -6.0),
            self.theme.palette.text_muted,
        );
        self.body.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let dialog_id = ctx.widget_id();
        let mut node = SemanticsNode::new(dialog_id, SemanticsRole::Dialog, self.panel_rect);
        node.name = Some(self.title.clone());
        ctx.push(node);
        let mut close = SemanticsNode::new(
            sui::WidgetId::new(dialog_id.get().wrapping_mul(41).wrapping_add(7_133)),
            SemanticsRole::Button,
            self.close_rect(),
        );
        close.parent = Some(dialog_id);
        close.name = Some("Close edit dialog".to_string());
        close.description =
            Some("Dismiss this editor without saving pending title changes".to_string());
        close.actions = vec![SemanticsAction::Activate];
        close.state.hovered = self.close_hovered;
        ctx.push(close);
        self.body.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.body);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.body);
    }
}

fn session_edit_dialog_signature(shared: &Shared) -> String {
    let chat = shared.chat();
    let Some(edit_id) = chat.session_edit_id.as_deref() else {
        return String::new();
    };
    let sessions = chat
        .sessions
        .iter()
        .map(|session| {
            format!(
                "{}:{:?}:{}:{}",
                session.id, session.kind, session.title, session.participant_count
            )
        })
        .collect::<Vec<_>>()
        .join("\u{1f}");
    format!(
        "{}|{}|{}|{}|{}",
        edit_id,
        chat.session_delete_confirm_id
            .as_deref()
            .unwrap_or_default(),
        chat.channel_agent_idx,
        chat.agents.len(),
        sessions
    )
}

struct ChatDialogHost {
    shared: Shared,
    reader: ThemeReader,
    body: WidgetPod,
    dialog: Option<WidgetPod>,
    dialog_signature: String,
}

impl ChatDialogHost {
    fn new<W>(body: W, shared: Shared, reader: ThemeReader) -> Self
    where
        W: Widget + 'static,
    {
        let mut host = Self {
            shared,
            reader,
            body: WidgetPod::new(body),
            dialog: None,
            dialog_signature: String::new(),
        };
        host.sync_dialog();
        host
    }

    fn sync_dialog(&mut self) {
        let signature = session_edit_dialog_signature(&self.shared);
        if signature == self.dialog_signature {
            return;
        }
        self.dialog_signature = signature;
        self.dialog = session_edit_dialog(&self.shared, &self.reader);
    }
}

impl Widget for ChatDialogHost {
    fn event(&mut self, ctx: &mut EventCtx, _event: &Event) {
        let signature = session_edit_dialog_signature(&self.shared);
        if signature != self.dialog_signature {
            ctx.request_measure();
            ctx.request_paint();
            ctx.request_semantics();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.sync_dialog();
        let size = constraints.clamp(Size::new(
            if constraints.max.width.is_finite() {
                constraints.max.width
            } else {
                920.0
            },
            if constraints.max.height.is_finite() {
                constraints.max.height
            } else {
                640.0
            },
        ));
        let tight = Constraints::tight(size);
        self.body.measure(ctx, tight);
        if let Some(dialog) = &mut self.dialog {
            dialog.measure(ctx, tight);
        }
        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.body.arrange(ctx, bounds);
        if let Some(dialog) = &mut self.dialog {
            dialog.arrange(ctx, bounds);
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.body.paint(ctx);
        if let Some(dialog) = &self.dialog {
            dialog.paint(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.body.semantics(ctx);
        if let Some(dialog) = &self.dialog {
            dialog.semantics(ctx);
        }
    }

    fn stack_host_options(&self) -> Option<StackHostOptions> {
        Some(StackHostOptions::default())
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.body);
        if let Some(dialog) = &self.dialog {
            visitor.visit(dialog);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.body);
        if let Some(dialog) = &mut self.dialog {
            visitor.visit(dialog);
        }
    }
}

struct RuntimeOverview {
    shared: Shared,
    activity_hovered: Cell<bool>,
    activity_pressed: Cell<bool>,
}

impl RuntimeOverview {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            activity_hovered: Cell::new(false),
            activity_pressed: Cell::new(false),
        }
    }

    fn health_chip_width(bounds: Rect) -> f32 {
        112.0_f32.min((bounds.width() * 0.34).max(92.0))
    }

    fn health_chip_rect(bounds: Rect) -> Rect {
        let chip_w = Self::health_chip_width(bounds);
        let top = bounds.y() + 10.0;
        Rect::new(
            bounds.x() + bounds.width() - PAD - chip_w,
            top - 1.0,
            chip_w,
            20.0,
        )
    }

    fn activity_button_rect(bounds: Rect) -> Option<Rect> {
        let chip = Self::health_chip_rect(bounds);
        let button_w = 94.0_f32.min((bounds.width() * 0.24).max(78.0));
        let rect = Rect::new(chip.x() - 8.0 - button_w, chip.y(), button_w, chip.height());
        (rect.x() >= bounds.x() + PAD + 88.0).then_some(rect)
    }

    fn activity_button_hit(&self, bounds: Rect, position: Point) -> bool {
        Self::activity_button_rect(bounds).is_some_and(|rect| rect.contains(position))
    }

    fn request_activity_refresh(ctx: &mut EventCtx) {
        ctx.request_measure();
        ctx.request_arrange();
        ctx.request_paint();
        ctx.request_semantics();
    }
}

impl Widget for RuntimeOverview {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = self.activity_button_hit(ctx.bounds(), pointer.position);
                if self.activity_hovered.get() != hovered {
                    self.activity_hovered.set(hovered);
                    Self::request_activity_refresh(ctx);
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if self.activity_button_hit(ctx.bounds(), pointer.position) {
                    self.activity_pressed.set(true);
                    ctx.request_pointer_capture(pointer.pointer_id);
                    Self::request_activity_refresh(ctx);
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if self.activity_pressed.take() {
                    if self.activity_button_hit(ctx.bounds(), pointer.position) {
                        self.shared.chat_mut().toggle_activity_panel();
                    }
                    ctx.release_pointer_capture(pointer.pointer_id);
                    Self::request_activity_refresh(ctx);
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if matches!(
                    pointer.kind,
                    PointerEventKind::Leave | PointerEventKind::Cancel
                ) =>
            {
                let had_state =
                    self.activity_hovered.replace(false) || self.activity_pressed.take();
                if had_state {
                    ctx.release_pointer_capture(pointer.pointer_id);
                    Self::request_activity_refresh(ctx);
                    ctx.set_handled();
                }
            }
            _ => {}
        }
    }

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
        let chip = Self::health_chip_rect(bounds);
        let activity_button = Self::activity_button_rect(bounds);
        if let Some(button) = activity_button {
            let activity_label = activity_button_label(&chat);
            draw_activity_overview_button(
                ctx,
                &pal,
                &theme,
                button,
                activity_label,
                chat.activity_panel_visible(),
                chat.activity_panel_needs_attention(),
                self.activity_hovered.get(),
                self.activity_pressed.get(),
            );
        }
        draw_health_chip(ctx, &pal, &theme, chip, &health);
        let title_right = activity_button.map_or(chip.x(), |button| button.x());
        let title_w = (title_right - x - 10.0).max(80.0);
        let runtime = runtime_overview_title(&chat);
        widgets::draw_text_in(ctx, Rect::new(x, top, title_w, 18.0), &runtime, title_style);

        let usage = usage_summary(&chat);
        let monitor = activity_work_summary(&chat);
        let monitor = if matches!(health.tone, ActivityTone::Warn | ActivityTone::Bad) {
            let visible_detail = activity_status_summary(&health.detail);
            format!("{} · {monitor}", compact_line(&visible_detail, 42))
        } else {
            monitor
        };
        let monitor = if let Some(usage) = usage {
            format!("{monitor} · {usage}")
        } else {
            monitor
        };
        widgets::draw_text_in(
            ctx,
            Rect::new(x, top + 20.0, title_w, 16.0),
            &monitor,
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
            let denominator = context_capacity_bytes(context);
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
        }
        widgets::draw_text_in(
            ctx,
            Rect::new(x, bounds.y() + 40.0, bounds.width() - PAD * 2.0, 16.0),
            &context_overview_summary(&chat),
            label_style,
        );
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let Some(rect) = Self::activity_button_rect(ctx.bounds()) else {
            return;
        };
        let chat = self.shared.chat();
        let mut node = SemanticsNode::new(
            sui::WidgetId::new(ctx.widget_id().get().wrapping_mul(47).wrapping_add(12_101)),
            SemanticsRole::Button,
            rect,
        );
        node.parent = Some(ctx.widget_id());
        node.name = Some(activity_button_label(&chat).to_string());
        node.description = Some(activity_button_description(&chat));
        node.actions = vec![SemanticsAction::Activate];
        node.state.hovered = self.activity_hovered.get();
        node.state.selected = chat.activity_panel_visible();
        ctx.push(node);
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

fn draw_activity_overview_button(
    ctx: &mut PaintCtx,
    pal: &Palette,
    theme: &DefaultTheme,
    rect: Rect,
    label: &str,
    active: bool,
    attention: bool,
    hovered: bool,
    pressed: bool,
) {
    let fill = if pressed {
        pal.selected
    } else if active {
        pal.field
    } else if hovered {
        pal.hover
    } else {
        pal.titlebar
    };
    widgets::fill_round_rect(ctx, rect, fill, 6.0);
    let tone = if attention {
        ActivityTone::Warn
    } else if active {
        ActivityTone::Good
    } else {
        ActivityTone::Neutral
    };
    let color = tone_color(pal, tone);
    draw_glyph(
        ctx,
        if attention {
            IconGlyph::Alert
        } else {
            IconGlyph::History
        },
        Rect::new(rect.x() + 6.0, rect.y() + 4.0, 12.0, 12.0),
        color,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 22.0, rect.y() + 2.0, rect.width() - 26.0, 16.0),
        label,
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
    detail_expanded: Cell<bool>,
    detail_hovered: Cell<bool>,
    detail_pressed: Cell<bool>,
}

impl ActivityRail {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            detail_expanded: Cell::new(false),
            detail_hovered: Cell::new(false),
            detail_pressed: Cell::new(false),
        }
    }

    fn status_card_rect(bounds: Rect) -> Rect {
        let x = bounds.x() + 14.0;
        let w = (bounds.width() - 28.0).max(0.0);
        Rect::new(x, bounds.y() + 42.0, w, 78.0)
    }

    fn detail_toggle_rect(status_card: Rect) -> Rect {
        Rect::new(
            status_card.max_x() - 104.0,
            status_card.y() + 8.0,
            92.0,
            24.0,
        )
    }

    fn detail_panel_rect(status_card: Rect) -> Rect {
        Rect::new(
            status_card.x(),
            status_card.max_y() + 8.0,
            status_card.width(),
            92.0,
        )
    }

    fn detail_entry(chat: &ChatState, health: &RuntimeHealth) -> Option<ChatActivityEntry> {
        chat.activity
            .iter()
            .rev()
            .find(|entry| matches!(entry.tone, ActivityTone::Warn | ActivityTone::Bad))
            .cloned()
            .or_else(|| {
                matches!(health.tone, ActivityTone::Warn | ActivityTone::Bad).then(|| {
                    ChatActivityEntry {
                        title: health.label.to_string(),
                        detail: health.detail.clone(),
                        tone: health.tone,
                    }
                })
            })
    }

    fn detail_toggle_hit(&self, bounds: Rect, position: Point) -> bool {
        let has_detail = {
            let chat = self.shared.chat();
            let health = chat.runtime_health();
            Self::detail_entry(&chat, &health).is_some()
        };
        has_detail && Self::detail_toggle_rect(Self::status_card_rect(bounds)).contains(position)
    }

    fn request_detail_refresh(ctx: &mut EventCtx) {
        ctx.request_paint();
        ctx.request_semantics();
    }
}

impl Widget for ActivityRail {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = self.detail_toggle_hit(ctx.bounds(), pointer.position);
                if self.detail_hovered.get() != hovered {
                    self.detail_hovered.set(hovered);
                    Self::request_detail_refresh(ctx);
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if self.detail_toggle_hit(ctx.bounds(), pointer.position) {
                    self.detail_pressed.set(true);
                    ctx.request_pointer_capture(pointer.pointer_id);
                    Self::request_detail_refresh(ctx);
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if self.detail_pressed.take() {
                    if self.detail_toggle_hit(ctx.bounds(), pointer.position) {
                        self.detail_expanded.set(!self.detail_expanded.get());
                    }
                    ctx.release_pointer_capture(pointer.pointer_id);
                    Self::request_detail_refresh(ctx);
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if matches!(
                    pointer.kind,
                    PointerEventKind::Leave | PointerEventKind::Cancel
                ) =>
            {
                let had_state = self.detail_hovered.replace(false) || self.detail_pressed.take();
                if had_state {
                    ctx.release_pointer_capture(pointer.pointer_id);
                    Self::request_detail_refresh(ctx);
                    ctx.set_handled();
                }
            }
            _ => {}
        }
    }

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
        let bottom = bounds.y() + bounds.height();
        let health = chat.runtime_health();
        let status_card = Self::status_card_rect(bounds);
        let detail_entry = Self::detail_entry(&chat, &health);
        if status_card.y() + status_card.height() <= bottom {
            draw_activity_status_card(ctx, &pal, &theme, status_card, &chat, &health);
            if detail_entry.is_some() {
                draw_activity_detail_toggle(
                    ctx,
                    &pal,
                    &theme,
                    Self::detail_toggle_rect(status_card),
                    self.detail_expanded.get(),
                    self.detail_hovered.get(),
                    self.detail_pressed.get(),
                );
            }
            y += status_card.height() + 16.0;
            if self.detail_expanded.get()
                && let Some(entry) = detail_entry.as_ref()
            {
                let panel = Self::detail_panel_rect(status_card);
                if panel.max_y() + 8.0 <= bottom {
                    draw_activity_detail_panel(ctx, &pal, &theme, panel, entry);
                    y = panel.max_y() + 16.0;
                }
            }
        }

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
            if y + 22.0 > bottom {
                return;
            }
            widgets::draw_text_in(
                ctx,
                Rect::new(x, y, w, 14.0),
                "TASKS",
                heading_style.clone(),
            );
            y += 22.0;
            for task in chat.control.tasks.iter().take(4) {
                if y + 42.0 > bottom {
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
            if y + 22.0 > bottom {
                return;
            }
            widgets::draw_text_in(
                ctx,
                Rect::new(x, y, w, 14.0),
                "TOOLS",
                heading_style.clone(),
            );
            y += 22.0;
            for call in chat.tool_calls.iter().rev().take(3) {
                if y + 42.0 > bottom {
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
            if y + 40.0 > bottom {
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
                if y + 24.0 > bottom {
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

        if y + 22.0 > bottom {
            return;
        }
        widgets::draw_text_in(ctx, Rect::new(x, y, w, 14.0), "EVENTS", heading_style);
        y += 22.0;
        let mut visible = 0usize;
        for entry in chat.activity.iter().rev().take(10) {
            if y + 54.0 > bottom {
                break;
            }
            draw_activity_event_card(ctx, &pal, &theme, Rect::new(x, y, w, 46.0), entry);
            y += 52.0;
            visible += 1;
        }
        if chat.activity.len() > visible && y + 18.0 <= bottom {
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 12.0, y, w - 24.0, 16.0),
                &format!("{} more event(s)", chat.activity.len() - visible),
                detail_style,
            );
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let has_detail = {
            let chat = self.shared.chat();
            let health = chat.runtime_health();
            Self::detail_entry(&chat, &health).is_some()
        };
        if !has_detail {
            return;
        }

        let action = if self.detail_expanded.get() {
            "Hide detail"
        } else {
            "Show detail"
        };
        let mut node = SemanticsNode::new(
            sui::WidgetId::new(ctx.widget_id().get().wrapping_mul(43).wrapping_add(12_001)),
            SemanticsRole::Button,
            Self::detail_toggle_rect(Self::status_card_rect(ctx.bounds())),
        );
        node.parent = Some(ctx.widget_id());
        node.name = Some(action.to_string());
        node.description = Some("Activity detail".to_string());
        node.actions = vec![SemanticsAction::Activate];
        node.state.hovered = self.detail_hovered.get();
        node.state.selected = self.detail_expanded.get();
        ctx.push(node);
    }
}

fn draw_activity_status_card(
    ctx: &mut PaintCtx,
    pal: &Palette,
    theme: &DefaultTheme,
    rect: Rect,
    chat: &ChatState,
    health: &RuntimeHealth,
) {
    widgets::round_panel(ctx, rect, pal.panel, pal.border, 8.0);
    let color = tone_color(pal, health.tone);
    widgets::fill_round_rect(
        ctx,
        Rect::new(rect.x() + 10.0, rect.y() + 12.0, 3.0, rect.height() - 24.0),
        color,
        1.5,
    );

    let chip = Rect::new(rect.x() + 22.0, rect.y() + 10.0, 112.0, 20.0);
    draw_health_chip(ctx, pal, theme, chip, health);

    let detail_style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text);
    let visible_detail = activity_status_summary(&health.detail);
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 22.0, rect.y() + 36.0, rect.width() - 34.0, 16.0),
        &compact_line(&visible_detail, 34),
        detail_style,
    );

    let detail = activity_work_summary(chat);
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 22.0, rect.y() + 55.0, rect.width() - 34.0, 15.0),
        &compact_line(&detail, 34),
        pal.text_style(theme, 10.5, FontWeight::NORMAL, pal.text_faint),
    );
}

fn draw_activity_detail_toggle(
    ctx: &mut PaintCtx,
    pal: &Palette,
    theme: &DefaultTheme,
    rect: Rect,
    expanded: bool,
    hovered: bool,
    pressed: bool,
) {
    let fill = if pressed {
        pal.selected
    } else if hovered {
        pal.hover
    } else {
        pal.field
    };
    widgets::round_panel(ctx, rect, fill, pal.border, 7.0);
    let icon = if expanded {
        IconGlyph::ChevronUp
    } else {
        IconGlyph::ChevronDown
    };
    draw_glyph(
        ctx,
        icon,
        Rect::new(rect.x() + 8.0, rect.y() + 5.0, 14.0, 14.0),
        pal.text_muted,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(
            rect.x() + 26.0,
            rect.y(),
            rect.width() - 32.0,
            rect.height(),
        ),
        if expanded {
            "Hide detail"
        } else {
            "Show detail"
        },
        pal.text_style(theme, 10.0, FontWeight::SEMIBOLD, pal.text_muted),
    );
}

fn draw_activity_detail_panel(
    ctx: &mut PaintCtx,
    pal: &Palette,
    theme: &DefaultTheme,
    rect: Rect,
    entry: &ChatActivityEntry,
) {
    widgets::round_panel(ctx, rect, pal.panel, pal.border, 7.0);
    widgets::fill_round_rect(
        ctx,
        Rect::new(rect.x() + 10.0, rect.y() + 13.0, 7.0, 7.0),
        tone_color(pal, entry.tone),
        3.5,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 24.0, rect.y() + 7.0, rect.width() - 34.0, 16.0),
        &compact_line(&entry.title, 36),
        pal.text_style(theme, 12.0, FontWeight::SEMIBOLD, pal.text),
    );

    let detail_style = pal.text_style(theme, 10.5, FontWeight::NORMAL, pal.text_faint);
    let measure = |text: &str| {
        ctx.measure_text(text, detail_style.clone())
            .map(|m| m.width)
            .unwrap_or(0.0)
    };
    let lines = widgets::wrap_lines(&entry.detail, rect.width() - 24.0, measure);
    let mut y = rect.y() + 29.0;
    for line in lines.iter().take(3) {
        widgets::draw_text_in(
            ctx,
            Rect::new(
                rect.x() + 12.0,
                y,
                rect.width() - 24.0,
                detail_style.line_height + 2.0,
            ),
            line,
            detail_style.clone(),
        );
        y += detail_style.line_height + 2.0;
    }
}

fn draw_activity_event_card(
    ctx: &mut PaintCtx,
    pal: &Palette,
    theme: &DefaultTheme,
    rect: Rect,
    entry: &ChatActivityEntry,
) {
    widgets::round_panel(ctx, rect, pal.field, pal.border, 7.0);
    let color = tone_color(pal, entry.tone);
    widgets::fill_round_rect(
        ctx,
        Rect::new(rect.x() + 10.0, rect.y() + 13.0, 7.0, 7.0),
        color,
        3.5,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 24.0, rect.y() + 7.0, rect.width() - 34.0, 16.0),
        &compact_line(&entry.title, 34),
        pal.text_style(theme, 12.0, FontWeight::SEMIBOLD, pal.text),
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 24.0, rect.y() + 25.0, rect.width() - 34.0, 15.0),
        &compact_line(&activity_visible_detail(&entry.detail), 50),
        pal.text_style(theme, 10.5, FontWeight::NORMAL, pal.text_faint),
    );
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
    hovered_recovery_action: Cell<Option<RecoveryCardAction>>,
    pressed_recovery_action: Cell<Option<RecoveryCardAction>>,
    hovered_quick_action: Cell<Option<ChatQuickAction>>,
    pressed_quick_action: Cell<Option<ChatQuickAction>>,
    transcript_recovery_actions: RefCell<Vec<(RecoveryCardAction, Rect)>>,
}

impl TranscriptView {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            content_height: Cell::new(0.0),
            viewport_height: Cell::new(0.0),
            bounds: Cell::new(Rect::ZERO),
            hovered_recovery_action: Cell::new(None),
            pressed_recovery_action: Cell::new(None),
            hovered_quick_action: Cell::new(None),
            pressed_quick_action: Cell::new(None),
            transcript_recovery_actions: RefCell::new(Vec::new()),
        }
    }

    fn empty_state_center(area: Rect) -> (f32, f32) {
        let lift = if area.height() > 260.0 { 28.0 } else { 0.0 };
        (
            area.x() + area.width() / 2.0,
            area.y() + area.height() / 2.0 - lift,
        )
    }

    fn quick_action_width(action: ChatQuickAction) -> f32 {
        match action {
            ChatQuickAction::PlanChange => 138.0,
            ChatQuickAction::CheckCluster => 128.0,
            ChatQuickAction::ReviewRepo => 116.0,
            ChatQuickAction::ManageChannel => 136.0,
        }
    }

    fn quick_actions(chat: &ChatState) -> &'static [ChatQuickAction] {
        if chat.current_is_channel() {
            &ChatQuickAction::CHANNEL
        } else {
            &ChatQuickAction::CHAT
        }
    }

    fn quick_action_rects(area: Rect, actions: &[ChatQuickAction]) -> Vec<(ChatQuickAction, Rect)> {
        if area.height() < 180.0 || area.width() < 260.0 {
            return Vec::new();
        }
        let (_, cy) = Self::empty_state_center(area);
        let gap = 8.0;
        let button_h = 30.0;
        let widths = actions
            .iter()
            .map(|action| Self::quick_action_width(*action))
            .collect::<Vec<_>>();
        let row_w = widths.iter().sum::<f32>() + gap * actions.len().saturating_sub(1) as f32;
        let max_w = (area.width() - PAD * 4.0).max(0.0).min(520.0);

        if row_w <= max_w {
            let mut x = area.x() + (area.width() - row_w) * 0.5;
            let y = cy + 58.0;
            return actions
                .into_iter()
                .zip(widths)
                .map(|(action, width)| {
                    let rect = Rect::new(x, y, width, button_h);
                    x += width + gap;
                    (*action, rect)
                })
                .collect();
        }

        let width = max_w.clamp(180.0, 240.0);
        let stack_h =
            button_h * actions.len() as f32 + gap * actions.len().saturating_sub(1) as f32;
        let x = area.x() + (area.width() - width) * 0.5;
        let y0 = cy + 54.0 - stack_h * 0.5;
        actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                (
                    *action,
                    Rect::new(x, y0 + index as f32 * (button_h + gap), width, button_h),
                )
            })
            .collect()
    }

    fn recovery_card_rect(area: Rect) -> Rect {
        let cx = area.x() + area.width() / 2.0;
        let cy = area.y() + area.height() / 2.0;
        let card_w = (area.width() - 56.0).max(220.0).min(560.0);
        let card_h = 188.0_f32.min((area.height() - 48.0).max(156.0));
        Rect::new(cx - card_w / 2.0, cy - card_h / 2.0, card_w, card_h)
    }

    fn recovery_card_text_rect(card: Rect) -> Rect {
        Rect::new(
            card.x() + 60.0,
            card.y() + 17.0,
            (card.width() - 82.0).max(120.0),
            (card.height() - 34.0).max(0.0),
        )
    }

    fn recovery_card_actions(
        chat: &ChatState,
        target: RuntimeRecoveryTarget,
    ) -> Vec<RecoveryCardAction> {
        match target {
            RuntimeRecoveryTarget::Providers => vec![RecoveryCardAction::OpenProviderSetup],
            RuntimeRecoveryTarget::Connection => {
                let mut actions = Vec::new();
                if chat.should_offer_chat_list_retry() {
                    actions.push(RecoveryCardAction::RetryChatList);
                }
                if chat.should_offer_sifs_check() {
                    actions.push(RecoveryCardAction::CheckSifs);
                }
                actions.push(RecoveryCardAction::OpenConnectionSettings);
                actions
            }
        }
    }

    fn recovery_action_is_primary(
        chat: &ChatState,
        target: RuntimeRecoveryTarget,
        action: RecoveryCardAction,
    ) -> bool {
        let actions = Self::recovery_card_actions(chat, target);
        match action {
            RecoveryCardAction::RetryChatList | RecoveryCardAction::OpenProviderSetup => true,
            RecoveryCardAction::CheckSifs => !actions.contains(&RecoveryCardAction::RetryChatList),
            RecoveryCardAction::OpenConnectionSettings => {
                !actions.contains(&RecoveryCardAction::RetryChatList)
                    && !actions.contains(&RecoveryCardAction::CheckSifs)
            }
        }
    }

    fn recovery_card_action_width(action: RecoveryCardAction) -> f32 {
        match action {
            RecoveryCardAction::RetryChatList => 112.0,
            RecoveryCardAction::CheckSifs => 116.0,
            RecoveryCardAction::OpenConnectionSettings => 128.0,
            RecoveryCardAction::OpenProviderSetup => 142.0,
        }
    }

    fn recovery_action_rects(
        area: Rect,
        target: RuntimeRecoveryTarget,
        chat: &ChatState,
    ) -> Vec<(RecoveryCardAction, Rect)> {
        let card = Self::recovery_card_rect(area);
        let text = Self::recovery_card_text_rect(card);
        let actions = Self::recovery_card_actions(chat, target);
        let button_h = 28.0;
        let gap = 8.0;

        if actions.len() > 1 {
            let widths: Vec<f32> = actions
                .iter()
                .map(|action| Self::recovery_card_action_width(*action))
                .collect();
            let row_w = widths.iter().sum::<f32>() + actions.len().saturating_sub(1) as f32 * gap;
            if row_w <= text.width() {
                let y = card.y() + card.height() - button_h - 14.0;
                let mut x = text.x();
                return actions
                    .iter()
                    .zip(widths)
                    .map(|(action, width)| {
                        let rect = Rect::new(x, y, width, button_h);
                        x += width + gap;
                        (*action, rect)
                    })
                    .collect();
            }
        }

        let stack_h =
            actions.len() as f32 * button_h + actions.len().saturating_sub(1) as f32 * gap;
        let y0 = card.y() + card.height() - stack_h - 12.0;
        let button_w = actions
            .iter()
            .map(|action| Self::recovery_card_action_width(*action))
            .fold(0.0_f32, f32::max)
            .min(text.width())
            .max(text.width().min(112.0));
        actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                (
                    *action,
                    Rect::new(
                        text.x(),
                        y0 + index as f32 * (button_h + gap),
                        button_w,
                        button_h,
                    ),
                )
            })
            .collect()
    }

    fn recovery_action_at(&self, bounds: Rect, position: Point) -> Option<RecoveryCardAction> {
        let target = {
            let chat = self.shared.chat();
            chat.messages
                .is_empty()
                .then(|| chat.runtime_recovery_target())
                .flatten()
        };
        if let Some(target) = target {
            let chat = self.shared.chat();
            return Self::recovery_action_rects(bounds, target, &chat)
                .into_iter()
                .find_map(|(action, rect)| rect.contains(position).then_some(action));
        }

        self.transcript_recovery_actions
            .borrow()
            .iter()
            .find_map(|(action, rect)| rect.contains(position).then_some(*action))
    }

    fn quick_action_at(&self, bounds: Rect, position: Point) -> Option<ChatQuickAction> {
        {
            let chat = self.shared.chat();
            if !chat.messages.is_empty() || chat.runtime_recovery_target().is_some() {
                return None;
            }
            let actions = Self::quick_actions(&chat).to_vec();
            return Self::quick_action_rects(bounds, &actions)
                .into_iter()
                .find_map(|(action, rect)| rect.contains(position).then_some(action));
        }
    }

    fn activate_recovery_action(&self, action: RecoveryCardAction) {
        match action {
            RecoveryCardAction::RetryChatList => {
                let waker = { self.shared.borrow().waker.clone() };
                self.shared.chat_mut().retry_chat_list_with_waker(waker);
            }
            RecoveryCardAction::CheckSifs => open_files_tab(&self.shared),
            RecoveryCardAction::OpenConnectionSettings => {
                open_settings_section(&self.shared, SettingsSection::Connection)
            }
            RecoveryCardAction::OpenProviderSetup => open_provider_setup(&self.shared),
        }
    }

    fn activate_quick_action(&self, action: ChatQuickAction) {
        self.shared.chat_mut().apply_quick_action(action);
    }

    fn recovery_action_widget_id(parent: sui::WidgetId, slot: usize) -> sui::WidgetId {
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(37)
                .wrapping_add(8_001 + slot as u64),
        )
    }

    fn quick_action_widget_id(parent: sui::WidgetId, slot: usize) -> sui::WidgetId {
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(41)
                .wrapping_add(9_101 + slot as u64),
        )
    }

    fn empty_text_widget_id(parent: sui::WidgetId, slot: usize) -> sui::WidgetId {
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(43)
                .wrapping_add(9_501 + slot as u64),
        )
    }

    fn request_recovery_action_refresh(ctx: &mut EventCtx) {
        use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};

        let window = InvalidationTarget::Window(ctx.window_id());
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
        ctx.request_paint();
        ctx.request_semantics();
    }
}

impl Widget for TranscriptView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let quick_hovered = self.quick_action_at(ctx.bounds(), pointer.position);
                let recovery_hovered = if quick_hovered.is_none() {
                    self.recovery_action_at(ctx.bounds(), pointer.position)
                } else {
                    None
                };
                if self.hovered_quick_action.get() != quick_hovered
                    || self.hovered_recovery_action.get() != recovery_hovered
                {
                    self.hovered_quick_action.set(quick_hovered);
                    self.hovered_recovery_action.set(recovery_hovered);
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if let Some(action) = self.quick_action_at(ctx.bounds(), pointer.position) {
                    self.pressed_quick_action.set(Some(action));
                    ctx.request_pointer_capture(pointer.pointer_id);
                    ctx.request_paint();
                    ctx.set_handled();
                } else if let Some(action) = self.recovery_action_at(ctx.bounds(), pointer.position)
                {
                    self.pressed_recovery_action.set(Some(action));
                    ctx.request_pointer_capture(pointer.pointer_id);
                    ctx.request_paint();
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if let Some(pressed) = self.pressed_quick_action.take() {
                    let released = self.quick_action_at(ctx.bounds(), pointer.position);
                    if released == Some(pressed) {
                        self.activate_quick_action(pressed);
                    }
                    ctx.release_pointer_capture(pointer.pointer_id);
                    Self::request_recovery_action_refresh(ctx);
                    ctx.set_handled();
                    return;
                }
                let pressed = self.pressed_recovery_action.take();
                if let Some(pressed) = pressed {
                    let released = self.recovery_action_at(ctx.bounds(), pointer.position);
                    if released == Some(pressed) {
                        self.activate_recovery_action(pressed);
                    }
                    ctx.release_pointer_capture(pointer.pointer_id);
                    Self::request_recovery_action_refresh(ctx);
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if matches!(
                    pointer.kind,
                    PointerEventKind::Leave | PointerEventKind::Cancel
                ) =>
            {
                let had_hover = self.hovered_recovery_action.take().is_some();
                let had_press = self.pressed_recovery_action.take().is_some();
                let had_quick_hover = self.hovered_quick_action.take().is_some();
                let had_quick_press = self.pressed_quick_action.take().is_some();
                if had_press || had_quick_press {
                    ctx.release_pointer_capture(pointer.pointer_id);
                }
                if had_hover || had_press || had_quick_hover || had_quick_press {
                    ctx.request_paint();
                    ctx.request_semantics();
                    ctx.set_handled();
                }
            }
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
        self.transcript_recovery_actions.borrow_mut().clear();

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
            action: Option<RecoveryCardAction>,
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
                let text = transcript_message_display_text(msg);
                let lines = widgets::wrap_lines(&text, inner_w, measure);
                let widest = lines.iter().map(|l| measure(l)).fold(0.0_f32, f32::max);
                let action = transcript_message_recovery_action(msg);
                let action_w = action
                    .map(Self::recovery_card_action_width)
                    .unwrap_or_default();
                let bubble_w = widest.min(inner_w).max(action_w) + 24.0;
                let action_h = if action.is_some() { 38.0 } else { 0.0 };
                let bubble_h = lines.len().max(1) as f32 * line_h + 20.0 + action_h;
                total += bubble_h + 12.0;
                laid.push(Laid {
                    is_user,
                    kind: msg.kind,
                    lines,
                    bubble_w,
                    bubble_h,
                    action,
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
        let mut action_rects = Vec::new();
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
                if let Some(action) = item.action {
                    let action_w =
                        Self::recovery_card_action_width(action).min(item.bubble_w - 24.0);
                    let action_rect =
                        Rect::new(bx + 12.0, y + item.bubble_h - 38.0, action_w, 28.0);
                    self.paint_recovery_action(ctx, &theme, &pal, action, action_rect, true);
                    action_rects.push((action, action_rect));
                }
            }
            y += item.bubble_h + 12.0;
        }
        *self.transcript_recovery_actions.borrow_mut() = action_rects;
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

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let (messages_empty, target) = {
            let chat = self.shared.chat();
            (
                chat.messages.is_empty(),
                chat.messages
                    .is_empty()
                    .then(|| chat.runtime_recovery_target())
                    .flatten(),
            )
        };
        if target.is_none() {
            if messages_empty {
                let (title, hint) = {
                    let chat = self.shared.chat();
                    (
                        chat.empty_conversation_title(),
                        chat.empty_conversation_hint(),
                    )
                };
                let (cx, cy) = Self::empty_state_center(ctx.bounds());
                let text_width = (ctx.bounds().width() - PAD * 4.0).max(120.0).min(560.0);
                for (slot, (name, y, height)) in
                    [(title, cy + 2.0, 22.0_f32), (hint, cy + 26.0, 18.0_f32)]
                        .into_iter()
                        .enumerate()
                {
                    let mut node = SemanticsNode::new(
                        Self::empty_text_widget_id(ctx.widget_id(), slot),
                        SemanticsRole::Text,
                        Rect::new(cx - text_width / 2.0, y, text_width, height),
                    );
                    node.parent = Some(ctx.widget_id());
                    node.name = Some(name);
                    ctx.push(node);
                }
                let actions = {
                    let chat = self.shared.chat();
                    Self::quick_actions(&chat).to_vec()
                };
                for (slot, (action, rect)) in Self::quick_action_rects(ctx.bounds(), &actions)
                    .into_iter()
                    .enumerate()
                {
                    let mut node = SemanticsNode::new(
                        Self::quick_action_widget_id(ctx.widget_id(), slot),
                        SemanticsRole::Button,
                        rect,
                    );
                    node.parent = Some(ctx.widget_id());
                    node.name = Some(action.label().to_string());
                    node.description = Some(action.description().to_string());
                    node.actions = vec![SemanticsAction::Activate];
                    node.state.hovered = self.hovered_quick_action.get() == Some(action);
                    ctx.push(node);
                }
                return;
            }
            for (slot, (action, rect)) in self
                .transcript_recovery_actions
                .borrow()
                .iter()
                .copied()
                .enumerate()
            {
                let mut node = SemanticsNode::new(
                    Self::recovery_action_widget_id(ctx.widget_id(), 100 + slot),
                    SemanticsRole::Button,
                    rect,
                );
                node.parent = Some(ctx.widget_id());
                node.name = Some(action.label().to_string());
                node.description = Some(action.description().to_string());
                node.actions = vec![SemanticsAction::Activate];
                node.state.hovered = self.hovered_recovery_action.get() == Some(action);
                ctx.push(node);
            }
            return;
        }
        let Some(target) = target else { return };

        let chat = self.shared.chat();
        for (slot, (action, rect)) in Self::recovery_action_rects(ctx.bounds(), target, &chat)
            .into_iter()
            .enumerate()
        {
            let mut node = SemanticsNode::new(
                Self::recovery_action_widget_id(ctx.widget_id(), slot),
                SemanticsRole::Button,
                rect,
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some(action.label().to_string());
            node.description = Some(action.description().to_string());
            node.actions = vec![SemanticsAction::Activate];
            node.state.hovered = self.hovered_recovery_action.get() == Some(action);
            ctx.push(node);
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
        if let Some(target) = chat.runtime_recovery_target() {
            self.paint_recovery_empty_state(ctx, theme, pal, area, chat, target);
            return;
        }

        let (cx, cy) = Self::empty_state_center(area);
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
        widgets::draw_centered(ctx, &chat.empty_conversation_title(), title, cx, cy + 2.0);
        let sub = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text_faint);
        let hint = chat.empty_conversation_hint();
        widgets::draw_centered(ctx, &hint, sub, cx, cy + 26.0);

        for (action, rect) in Self::quick_action_rects(area, Self::quick_actions(chat)) {
            self.paint_quick_action(ctx, theme, pal, action, rect);
        }
    }

    fn paint_recovery_empty_state(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        area: Rect,
        chat: &ChatState,
        target: RuntimeRecoveryTarget,
    ) {
        let card = Self::recovery_card_rect(area);
        let text = Self::recovery_card_text_rect(card);
        sui::paint_theme_shadow(ctx, card, [10.0; 4], &theme.shadows.box_shadow.xs);
        widgets::round_panel(ctx, card, pal.panel, pal.warn, 12.0);

        let icon_side = 28.0;
        draw_glyph(
            ctx,
            IconGlyph::Alert,
            Rect::new(card.x() + 18.0, card.y() + 20.0, icon_side, icon_side),
            pal.warn,
        );

        let title_style = pal.text_style(theme, 15.0, FontWeight::SEMIBOLD, pal.text);
        widgets::draw_text_in(
            ctx,
            Rect::new(text.x(), card.y() + 17.0, text.width(), 22.0),
            chat.recovery_empty_title(target),
            title_style,
        );

        let detail_style = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text_muted);
        let detail = chat.recovery_empty_detail(target);
        let detail_lines = {
            let measure = |text: &str| {
                ctx.measure_text(text, detail_style.clone())
                    .map(|m| m.width)
                    .unwrap_or(0.0)
            };
            widgets::wrap_lines(&detail, text.width(), measure)
        };
        let mut y = card.y() + 50.0;
        for line in detail_lines.iter().take(2) {
            widgets::draw_text_in(
                ctx,
                Rect::new(text.x(), y, text.width(), detail_style.line_height + 3.0),
                line,
                detail_style.clone(),
            );
            y += detail_style.line_height + 2.0;
        }

        let hint_style = pal.text_style(theme, 11.5, FontWeight::NORMAL, pal.text_faint);
        let hint_lines = {
            let measure = |text: &str| {
                ctx.measure_text(text, hint_style.clone())
                    .map(|m| m.width)
                    .unwrap_or(0.0)
            };
            widgets::wrap_lines(chat.recovery_empty_hint(target), text.width(), measure)
        };
        y = y.max(card.y() + 86.0);
        let action_top = Self::recovery_action_rects(area, target, chat)
            .into_iter()
            .map(|(_, rect)| rect.y())
            .fold(card.y() + card.height(), f32::min);
        for line in hint_lines.iter().take(2) {
            if y + hint_style.line_height + 3.0 > action_top - 4.0 {
                break;
            }
            widgets::draw_text_in(
                ctx,
                Rect::new(text.x(), y, text.width(), hint_style.line_height + 3.0),
                line,
                hint_style.clone(),
            );
            y += hint_style.line_height + 2.0;
        }

        for (action, rect) in Self::recovery_action_rects(area, target, chat) {
            let primary = Self::recovery_action_is_primary(chat, target, action);
            self.paint_recovery_action(ctx, theme, pal, action, rect, primary);
        }
    }

    fn paint_recovery_action(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        action: RecoveryCardAction,
        rect: Rect,
        primary: bool,
    ) {
        let hovered = self.hovered_recovery_action.get() == Some(action);
        let pressed = self.pressed_recovery_action.get() == Some(action);
        let (fill, border, text_color, icon_color) = if primary {
            let fill = if pressed { pal.selected } else { pal.accent };
            (fill, fill, pal.on_accent, pal.on_accent)
        } else {
            let fill = if pressed {
                pal.selected
            } else if hovered {
                pal.hover
            } else {
                pal.field
            };
            (fill, pal.border, pal.text_muted, pal.text_muted)
        };

        widgets::round_panel(ctx, rect, fill, border, 8.0);
        let icon_side = 14.0;
        let icon = Rect::new(
            rect.x() + 10.0,
            rect.y() + (rect.height() - icon_side) * 0.5,
            icon_side,
            icon_side,
        );
        draw_glyph(ctx, action.icon(), icon, icon_color);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                icon.max_x() + 6.0,
                rect.y(),
                (rect.width() - icon_side - 26.0).max(0.0),
                rect.height(),
            ),
            action.label(),
            pal.text_style(theme, 10.5, FontWeight::SEMIBOLD, text_color),
        );
    }

    fn paint_quick_action(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        action: ChatQuickAction,
        rect: Rect,
    ) {
        let hovered = self.hovered_quick_action.get() == Some(action);
        let pressed = self.pressed_quick_action.get() == Some(action);
        let fill = if pressed {
            pal.selected
        } else if hovered {
            pal.hover
        } else {
            pal.panel
        };
        widgets::round_panel(ctx, rect, fill, pal.border, 8.0);
        let icon_side = 14.0;
        let icon = Rect::new(
            rect.x() + 10.0,
            rect.y() + (rect.height() - icon_side) * 0.5,
            icon_side,
            icon_side,
        );
        let icon_color = if hovered { pal.accent } else { pal.text_muted };
        draw_glyph(ctx, action.icon(), icon, icon_color);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                icon.max_x() + 6.0,
                rect.y(),
                (rect.width() - icon_side - 26.0).max(0.0),
                rect.height(),
            ),
            action.label(),
            pal.text_style(theme, 10.5, FontWeight::SEMIBOLD, pal.text_muted),
        );
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
fn selector_signature(shared: &Shared) -> (usize, usize, usize, bool, bool, bool, String, String) {
    let chat = shared.chat();
    (
        chat.provider_idx,
        chat.providers.len(),
        chat.current_models().len(),
        chat.effort_visible(),
        chat.models_loading,
        chat.shows_recovery_empty_card(),
        chat.providers
            .iter()
            .map(|provider| format!("{}@{}:{}", provider.id, provider.account, provider.name))
            .collect::<Vec<_>>()
            .join("\u{1f}"),
        chat.runtime_recovery_target()
            .map(RuntimeRecoveryTarget::key)
            .unwrap_or("none")
            .to_string(),
    )
}

/// Build the provider / model / effort selector row for the current selection.
fn selectors(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    if shared.chat().providers.is_empty() {
        if shared.chat().shows_recovery_empty_card() {
            let recovery_target = shared.chat().runtime_recovery_target();
            let s = shared.clone();
            let mut row = Stack::horizontal()
                .spacing(8.0)
                .alignment(Alignment::Center)
                .with_child(
                    SizedBox::new().height(SELECTOR_H).with_child(
                        Label::new("Provider setup required")
                            .font_size(12.0)
                            .color_when(move || palette(&s).text_faint),
                    ),
                );
            if recovery_target == Some(RuntimeRecoveryTarget::Connection) {
                row = row.with_child(provider_setup_button(shared, reader));
            }
            return WidgetPod::new(row);
        }

        let row = Stack::horizontal()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(provider_setup_button(shared, reader));
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

    if !shared.chat().shows_recovery_empty_card() {
        if let Some(action) = retry_chat_list_button(shared, reader) {
            row = row.with_child(action);
        }

        if let Some(action) = check_sifs_button(shared, reader) {
            row = row.with_child(action);
        }

        if let Some(action) = runtime_recovery_button(shared, reader) {
            row = row.with_child(action);
        }
    }

    WidgetPod::new(row)
}

fn provider_setup_button(shared: &Shared, reader: &ThemeReader) -> SizedBox {
    let s = shared.clone();
    SizedBox::new().height(SELECTOR_H).with_child(
        sui::Button::new("Setup provider")
            .icon(IconGlyph::Add)
            .theme_when(composer_control_reader(reader))
            .on_press(move || open_provider_setup(&s)),
    )
}

fn runtime_recovery_button(shared: &Shared, reader: &ThemeReader) -> Option<SizedBox> {
    let target = shared.chat().runtime_recovery_target()?;
    let s = shared.clone();
    Some(
        SizedBox::new().height(SELECTOR_H).with_child(
            sui::Button::new(target.footer_label())
                .icon(IconGlyph::Alert)
                .min_width(target.footer_min_width())
                .theme_when(composer_control_reader(reader))
                .on_press(move || open_settings_section(&s, target.settings_section())),
        ),
    )
}

fn retry_chat_list_button(shared: &Shared, reader: &ThemeReader) -> Option<SizedBox> {
    let chat = shared.chat();
    let target = chat.runtime_recovery_target()?;
    if target != RuntimeRecoveryTarget::Connection {
        return None;
    }
    if !chat.should_offer_chat_list_retry() {
        return None;
    }
    let s = shared.clone();
    Some(
        SizedBox::new().height(SELECTOR_H).with_child(
            sui::Button::new("Retry chat list")
                .icon(IconGlyph::Redo)
                .min_width(132.0)
                .theme_when(composer_control_reader(reader))
                .on_press(move || {
                    let waker = { s.borrow().waker.clone() };
                    s.chat_mut().retry_chat_list_with_waker(waker);
                }),
        ),
    )
}

fn check_sifs_button(shared: &Shared, reader: &ThemeReader) -> Option<SizedBox> {
    let chat = shared.chat();
    let target = chat.runtime_recovery_target()?;
    if target != RuntimeRecoveryTarget::Connection {
        return None;
    }
    if !chat.should_offer_sifs_check() {
        return None;
    }
    let s = shared.clone();
    Some(
        SizedBox::new().height(SELECTOR_H).with_child(
            sui::Button::new("Check SIFS")
                .icon(IconGlyph::Storage)
                .min_width(120.0)
                .theme_when(composer_control_reader(reader))
                .on_press(move || open_files_tab(&s)),
        ),
    )
}

fn open_settings_section(shared: &Shared, section: SettingsSection) {
    let mut app = shared.borrow_mut();
    app.active_tab = Tab::Settings;
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.open(section);
    }
}

fn open_files_tab(shared: &Shared) {
    shared.borrow_mut().active_tab = Tab::Files;
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
fn composer_action_signature(shared: &Shared) -> (bool, bool) {
    let chat = shared.chat();
    (chat.streaming, !chat.input.is_empty())
}

fn composer_actions(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (streaming, has_draft) = composer_action_signature(shared);
    if streaming {
        let s = shared.clone();
        WidgetPod::new(
            Flex::horizontal()
                .align_items(Alignment::Center)
                .spacer()
                .with_child(
                    IconButton::new(IconGlyph::Close, "Interrupt")
                        .size(SEND_BUTTON_SIZE)
                        .icon_size(17.0)
                        .theme_when(clone_reader(reader))
                        .on_press(move || {
                            s.chat_mut().interrupt_turn();
                        }),
                ),
        )
    } else if has_draft {
        let clear = {
            let s = shared.clone();
            IconButton::new(IconGlyph::Close, "Clear draft")
                .size(SEND_BUTTON_SIZE)
                .icon_size(16.0)
                .theme_when(secondary_button_theme(reader))
                .on_press(move || {
                    s.chat_mut().clear_draft();
                })
        };
        let send = {
            let s = shared.clone();
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
                })
        };
        WidgetPod::new(
            Flex::horizontal()
                .gap(6.0)
                .align_items(Alignment::Center)
                .spacer()
                .with_child(clear)
                .with_child(send),
        )
    } else {
        let s = shared.clone();
        WidgetPod::new(
            Flex::horizontal()
                .align_items(Alignment::Center)
                .spacer()
                .with_child(
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
                ),
        )
    }
}

fn input_row(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let input = {
        let s = shared.clone();
        let draft = shared.chat().input.clone();
        let provider_ready = !shared.chat().providers.is_empty();
        let placeholder = if provider_ready {
            "Message Sinomo...  (Enter to send, Shift+Enter for newline)"
        } else {
            "Set up a provider before sending."
        };
        let mut input = TextArea::new("chat-input")
            .value(draft)
            .placeholder(placeholder)
            .padding(Insets {
                left: 12.0,
                top: 8.0,
                right: 12.0,
                bottom: 8.0,
            })
            .theme_when(composer_control_reader(reader));
        if provider_ready {
            input = input.on_change({
                let s = s.clone();
                move |value| {
                    s.chat_mut().set_draft(value);
                }
            });
            // Enter sends, Shift+Enter inserts a newline (the on_submit only fires while the
            // TextArea is focused — Enter only reaches a focused input). While streaming, `submit()`
            // queues the draft as a follow-up for the active runtime turn.
            input = input.on_submit(move |_text| {
                s.chat_mut().submit();
            });
        } else {
            input = input.read_only();
        }
        input
    };

    let action = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        WidgetPod::new(RebuildOnChange::new(
            {
                let s = s.clone();
                move || composer_action_signature(&s)
            },
            move |_sig| composer_actions(&s, &r),
        ))
    };

    let row = ComposerInputRow::new(
        WidgetPod::new(input),
        action,
        SEND_BUTTON_SIZE * 2.0 + 6.0,
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

    // The input row is rebuilt when the streaming flag flips (Send <-> Interrupt), when provider
    // availability changes (editable composer <-> setup-required read-only state), and when a
    // programmatic quick action replaces the draft. Normal typing does not bump the revision, so the
    // focused `TextArea` is not rebuilt on every keystroke.
    let input_host = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || {
                    let chat = s.chat();
                    (
                        chat.streaming,
                        chat.providers.is_empty(),
                        chat.draft_revision,
                    )
                }
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

fn chat_work_area(shared: &Shared, reader: &ThemeReader) -> RebuildOnChange<bool> {
    let key_shared = shared.clone();
    let build_shared = shared.clone();
    let build_reader = Rc::clone(reader);
    RebuildOnChange::new(
        move || key_shared.chat().activity_panel_visible(),
        move |activity_visible| {
            if *activity_visible {
                WidgetPod::new(Split::right_fixed(
                    main_column(&build_shared, &build_reader),
                    ActivityRail::new(build_shared.clone()),
                    ACTIVITY_RAIL_W,
                    &build_reader,
                ))
            } else {
                WidgetPod::new(main_column(&build_shared, &build_reader))
            }
        },
    )
}

fn wide_pod(
    shared: &Shared,
    reader: &ThemeReader,
    sessions_scroll_state: &ScrollState,
) -> WidgetPod {
    let work_area = chat_work_area(shared, reader);
    let row = Split::left_fixed(
        sidebar(shared, reader, sessions_scroll_state),
        work_area,
        SIDEBAR_W,
        reader,
    );
    let dialog_host = ChatDialogHost::new(row, shared.clone(), Rc::clone(reader));
    WidgetPod::new(
        Surface::window(dialog_host)
            .fill()
            .theme_when(clone_reader(reader)),
    )
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
        let sessions_scroll_state = ScrollState::new();
        let wide = wide_pod(&shared, &theme_reader, &sessions_scroll_state);
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
        let changed = {
            let mut chat = self.shared.chat_mut();
            let worker_changed = chat.pump_worker_events();
            let list_changed = chat.pump_session_list();
            let delete_changed = chat.pump_delete();
            let channel_create_changed = chat.pump_channel_create();
            worker_changed || list_changed || delete_changed || channel_create_changed
        };
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
        } else if changed {
            ctx.request_paint();
        }

        if changed || sig_changed {
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
    chat.activity_panel_visible().hash(&mut h);
    chat.session_list_loading.hash(&mut h);
    chat.session_kind_filter.hash(&mut h);
    chat.runtime_recovery_target()
        .map(RuntimeRecoveryTarget::key)
        .unwrap_or("none")
        .hash(&mut h);
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
    action_width: f32,
    action_side: f32,
    gap: f32,
}

impl ComposerInputRow {
    fn new(
        input: WidgetPod,
        action: WidgetPod,
        action_width: f32,
        action_side: f32,
        gap: f32,
    ) -> Self {
        Self {
            input,
            action,
            action_width: action_width.max(0.0),
            action_side: action_side.max(0.0),
            gap: gap.max(0.0),
        }
    }

    fn layout_rects(&self, bounds: Rect) -> (Rect, Rect) {
        let height = self
            .action_side
            .min(bounds.width())
            .min(bounds.height())
            .max(0.0);
        let width = self.action_width.min(bounds.width()).max(0.0);
        let gap = if width > 0.0 && bounds.width() > width {
            self.gap.min(bounds.width() - width).max(0.0)
        } else {
            0.0
        };
        let input_width = (bounds.width() - width - gap).max(0.0);
        let input = Rect::new(bounds.x(), bounds.y(), input_width, bounds.height());
        let action = Rect::new(
            bounds.x() + input_width + gap,
            bounds.y() + ((bounds.height() - height) * 0.5).max(0.0),
            width,
            height,
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
    use crate::state::AppState;
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

    fn seed_test_agents(state: &mut ChatState) {
        state.agents = vec![
            AgentSummary {
                key: "main-agent".to_string(),
                name: "main-agent".to_string(),
                description: String::new(),
            },
            AgentSummary {
                key: "coding-agent".to_string(),
                name: "coding-agent".to_string(),
                description: String::new(),
            },
        ];
        state.info.agent = "main-agent".to_string();
    }

    #[test]
    fn session_meta_label_exposes_kind_agents_and_save_state() {
        let channel = SessionRef {
            id: "channel-meta".to_string(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 0,
            participant_count: 2,
        };
        let chat = SessionRef {
            id: "chat-meta".to_string(),
            title: "Old question".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 0,
            participant_count: 1,
        };

        assert_eq!(session_meta_label(&channel), "2 agents · not saved");
        assert_eq!(session_meta_label(&chat), "Chat · not saved");
        assert_eq!(session_kind_badge_label(channel.kind), "Channel");
        assert_eq!(session_kind_badge_label(chat.kind), "Chat");
        assert!(
            session_kind_badge_width(channel.kind) > session_kind_badge_width(chat.kind),
            "channel badge needs extra width for its longer label"
        );
    }

    #[test]
    fn session_filter_matches_title_kind_and_metadata() {
        let channel = SessionRef {
            id: "channel-filter".to_string(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 0,
            participant_count: 2,
        };
        let chat = SessionRef {
            id: "chat-filter".to_string(),
            title: "Old question".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 0,
            participant_count: 1,
        };

        assert!(session_matches_filter(
            &channel,
            &normalized_session_filter("release")
        ));
        assert!(session_matches_filter(
            &channel,
            &normalized_session_filter("channel")
        ));
        assert!(session_matches_filter(
            &channel,
            &normalized_session_filter("2 agents")
        ));
        assert!(session_matches_filter(
            &chat,
            &normalized_session_filter("chat")
        ));
        assert!(!session_matches_filter(
            &chat,
            &normalized_session_filter("release")
        ));
    }

    #[test]
    fn session_kind_filter_limits_visible_indices() {
        let sessions = vec![
            SessionRef {
                id: "chat-filter-a".to_string(),
                title: "Current chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 0,
                participant_count: 1,
            },
            SessionRef {
                id: "channel-filter".to_string(),
                title: "Release triage".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 0,
                participant_count: 2,
            },
            SessionRef {
                id: "chat-filter-b".to_string(),
                title: "Release note question".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 0,
                participant_count: 1,
            },
        ];

        assert_eq!(
            filtered_session_indices(&sessions, "", SessionKindFilter::All),
            vec![0, 1, 2]
        );
        assert_eq!(
            filtered_session_indices(&sessions, "", SessionKindFilter::Chats),
            vec![0, 2]
        );
        assert_eq!(
            filtered_session_indices(&sessions, "", SessionKindFilter::Channels),
            vec![1]
        );
        assert_eq!(
            filtered_session_indices(
                &sessions,
                &normalized_session_filter("release"),
                SessionKindFilter::Channels,
            ),
            vec![1]
        );
        assert_eq!(
            filtered_session_indices(
                &sessions,
                &normalized_session_filter("release"),
                SessionKindFilter::Chats,
            ),
            vec![2]
        );
    }

    #[test]
    fn agent_list_detail_uses_catalog_description_and_built_in_fallbacks() {
        let described = AgentSummary {
            key: "custom-agent".to_string(),
            name: "Custom".to_string(),
            description: "Investigates product workflows and summarizes risky edges.".to_string(),
        };
        let coding = AgentSummary {
            key: "coding-agent".to_string(),
            name: "coding-agent".to_string(),
            description: String::new(),
        };

        assert_eq!(
            agent_list_detail(&described),
            "Investigates product workflows an..."
        );
        assert_eq!(agent_list_detail(&coding), "Code and verification");
    }

    fn isolated_chat_state(_slug: &str) -> (Backend, ChatState) {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.sessions.clear();
        (backend, state)
    }

    #[test]
    fn typing_draft_does_not_rebuild_focused_composer() {
        let (_backend, mut state) = isolated_chat_state("typing-draft");
        let revision = state.draft_revision;

        state.set_draft("h".to_string());
        state.set_draft("hello".to_string());

        assert_eq!(state.input, "hello");
        assert_eq!(state.cursor, "hello".len());
        assert_eq!(
            state.draft_revision, revision,
            "typing must not change the programmatic composer rebuild key"
        );
    }

    fn chat_sessions_list_y(window: &sui_testing::TestWindow) -> sui::Result<f32> {
        let snapshot = window.snapshot()?;
        Ok(snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::List && node.name.as_deref() == Some("chat-sessions")
            })
            .expect("chat sessions list semantics node")
            .bounds
            .y())
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
    fn submit_before_ready_assigns_session_id_and_indexes_session() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.session_id.clear();

        state.set_draft("persist me before ready".to_string());
        state.submit();

        let session_id = state.session_id.clone();
        assert!(
            !session_id.is_empty(),
            "submitting before Ready must still create a visible session id"
        );
        let transcript =
            sessions::load_transcript(&state.data_dir, &session_id).expect("transcript saved");
        assert_eq!(transcript.messages.len(), 2);
        assert_eq!(transcript.messages[0].text, "persist me before ready");
        assert!(
            state
                .sessions
                .iter()
                .any(|session| session.id == session_id),
            "new conversation should appear in the sessions list after submit"
        );

        state.apply_event(ChatEvent::Ready {
            session_id: SessionId::generate().to_string(),
            info: RuntimeInfo::default(),
        });
        assert_eq!(
            state.session_id, session_id,
            "a late initial Ready event must not move the visible conversation to another id"
        );
    }

    #[test]
    fn agent_switch_is_blocked_after_start_but_allowed_after_new_chat() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_test_agents(&mut state);
        state.session_id = SessionId::generate().to_string();
        state.messages = vec![ChatMessage::user("already started")];

        state.select_agent(1);

        assert_eq!(
            state.info.agent, "main-agent",
            "started sessions keep their original agent"
        );
        assert_eq!(
            state.notice.as_deref(),
            Some("start a new chat before switching agents")
        );

        state.new_session();
        state.select_agent(1);

        assert!(
            state.messages.is_empty(),
            "new chat creates an empty switchable session"
        );
        assert_eq!(
            state.info.agent, "coding-agent",
            "agent selection is allowed again in a fresh chat"
        );
    }

    #[test]
    fn empty_conversation_title_follows_selected_saved_chat() {
        let (_backend, mut state) = isolated_chat_state("empty-chat-title");
        let session_id = SessionId::generate().to_string();
        state.sessions = vec![SessionRef {
            id: session_id.clone(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
        }];
        state.session_id = session_id;
        state.info.agent = "main-agent".to_string();

        assert_eq!(state.empty_conversation_title(), "Release triage");
        assert_eq!(
            state.empty_conversation_hint(),
            "Type a message below to begin."
        );
    }

    #[test]
    fn empty_conversation_title_follows_selected_channel() {
        let (_backend, mut state) = isolated_chat_state("empty-channel-title");
        let channel_id = SessionId::generate().to_string();
        state.sessions = vec![SessionRef {
            id: channel_id.clone(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 1,
            participant_count: 2,
        }];
        state.session_id = channel_id;
        state.info.agent = "main-agent".to_string();

        assert_eq!(state.empty_conversation_title(), "Channel: Release triage");
        assert_eq!(
            state.empty_conversation_hint(),
            "2 active agents · send a message to the channel."
        );
    }

    #[test]
    fn session_rename_updates_sidebar_metadata_and_preserves_title() {
        let (_backend, mut state) = isolated_chat_state("rename");
        let session_id = SessionId::generate().to_string();
        state.session_id = session_id.clone();
        state.messages = vec![
            ChatMessage::user("original prompt"),
            ChatMessage::model("original answer"),
        ];
        state.persist();

        state.begin_edit_current_session();
        assert_eq!(state.session_edit_id.as_deref(), Some(session_id.as_str()));
        state.set_session_title_draft("Renamed chat".to_string());
        assert!(state.can_save_session_title());
        state.save_session_title();

        assert_eq!(state.session_edit_id.as_deref(), Some(session_id.as_str()));
        assert_eq!(
            state
                .sessions
                .iter()
                .find(|session| session.id == session_id)
                .map(|session| session.title.as_str()),
            Some("Renamed chat")
        );
        let transcript =
            sessions::load_transcript(&state.data_dir, &session_id).expect("transcript preserved");
        assert_eq!(transcript.messages[0].text, "original prompt");
        assert_eq!(transcript.messages[1].text, "original answer");

        state.persist();
        let meta = sessions::list(&state.data_dir)
            .into_iter()
            .find(|meta| meta.id == session_id)
            .expect("renamed session listed");
        assert_eq!(meta.title, "Renamed chat");
        assert_eq!(meta.title_source.as_deref(), Some("user"));
    }

    #[test]
    fn session_delete_hides_session_and_resets_visible_chat() {
        let (_backend, mut state) = isolated_chat_state("delete");
        let session_id = SessionId::generate().to_string();
        state.session_id = session_id.clone();
        state.messages = vec![
            ChatMessage::user("delete this prompt"),
            ChatMessage::model("delete this answer"),
        ];
        state.persist();

        state.request_delete_session(&session_id);
        assert_eq!(
            state.session_delete_confirm_id.as_deref(),
            Some(session_id.as_str())
        );
        state.confirm_delete_session(None);

        assert!(
            state
                .sessions
                .iter()
                .all(|session| session.id != session_id),
            "deleted sessions leave the sidebar list"
        );
        assert!(state.messages.is_empty());
        assert_ne!(state.session_id, session_id);
        assert_eq!(
            state.pending_session_reset.as_deref(),
            Some(state.session_id.as_str())
        );
        assert_eq!(
            state.pending_delete_id.as_deref(),
            Some(session_id.as_str()),
            "SIFS tombstoning continues in the background"
        );

        wait_for(&mut state, Duration::from_secs(20), |state| {
            state.pending_delete_id.is_none()
        });
        assert!(
            sessions::load_transcript(&state.data_dir, &session_id).is_none(),
            "deleted sessions cannot be reopened"
        );
    }

    #[test]
    fn new_channel_creates_channel_entry_and_opens_config() {
        let (_backend, mut state) = isolated_chat_state("new-channel");
        seed_test_agents(&mut state);

        state.new_channel(None);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_session_reset.is_none() && state.channel_create_rx.is_none()
        });

        let channel_id = state.session_id.clone();
        assert!(
            state.messages.is_empty(),
            "new channel starts with an empty visible transcript"
        );
        assert_eq!(
            state.session_edit_id.as_deref(),
            Some(channel_id.as_str()),
            "new channels open their edit popup"
        );
        let listed = state
            .sessions
            .iter()
            .find(|session| session.id == channel_id)
            .expect("channel listed");
        assert_eq!(listed.kind, ConversationKind::Channel);
        assert_eq!(listed.participant_count, 1);

        let parsed = channel_id.parse::<SessionId>().expect("channel id");
        let channel = state
            .agent_channel_store()
            .expect("channel store")
            .load_channel(&parsed)
            .expect("load channel")
            .expect("channel saved");
        assert_eq!(channel.channel_kind, AgentChannelKind::Channel);
        let participants = channel.enabled_participants_in_order();
        assert_eq!(participants.len(), 1);
        assert_eq!(participants[0].agent_key, "main-agent");
        assert_ne!(
            participants[0].agent_session_id, channel.channel_id,
            "explicit channel participants keep their own agent sessions"
        );
    }

    #[test]
    fn channel_config_adds_selected_agent_and_updates_sidebar_count() {
        let (_backend, mut state) = isolated_chat_state("channel-add-agent");
        seed_test_agents(&mut state);
        state.new_channel(None);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_session_reset.is_none() && state.channel_create_rx.is_none()
        });
        let channel_id = state.session_id.clone();

        state.set_channel_agent_idx(1);
        state.add_selected_agent_to_channel();

        let parsed = channel_id.parse::<SessionId>().expect("channel id");
        let channel = state
            .agent_channel_store()
            .expect("channel store")
            .load_channel(&parsed)
            .expect("load channel")
            .expect("channel saved");
        let participants = channel.enabled_participants_in_order();
        assert_eq!(participants.len(), 2);
        assert!(
            participants
                .iter()
                .any(|participant| participant.agent_key == "coding-agent")
        );
        assert_eq!(
            state
                .sessions
                .iter()
                .find(|session| session.id == channel_id)
                .map(|session| session.participant_count),
            Some(2)
        );
        assert_eq!(
            state
                .channel_agent_refs(&channel_id)
                .into_iter()
                .map(|participant| participant.agent_name)
                .collect::<Vec<_>>(),
            vec!["main-agent".to_string(), "coding-agent".to_string()]
        );
    }

    #[test]
    fn channel_config_removes_agent_and_updates_sidebar_count() {
        let (_backend, mut state) = isolated_chat_state("channel-remove-agent");
        seed_test_agents(&mut state);
        state.new_channel(None);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_session_reset.is_none() && state.channel_create_rx.is_none()
        });
        let channel_id = state.session_id.clone();
        state.set_channel_agent_idx(1);
        state.add_selected_agent_to_channel();

        let coding_participant_id = state
            .channel_agent_refs(&channel_id)
            .into_iter()
            .find(|participant| participant.agent_key == "coding-agent")
            .map(|participant| participant.participant_id)
            .expect("coding participant listed");

        state.remove_channel_agent(&channel_id, &coding_participant_id);

        let parsed = channel_id.parse::<SessionId>().expect("channel id");
        let channel = state
            .agent_channel_store()
            .expect("channel store")
            .load_channel(&parsed)
            .expect("load channel")
            .expect("channel saved");
        let participants = channel.enabled_participants_in_order();
        assert_eq!(participants.len(), 1);
        assert!(
            participants
                .iter()
                .all(|participant| participant.agent_key != "coding-agent")
        );
        assert_eq!(
            state
                .sessions
                .iter()
                .find(|session| session.id == channel_id)
                .map(|session| session.participant_count),
            Some(1)
        );
    }

    #[test]
    fn session_edit_dialog_done_button_closes_in_rendered_view() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let session_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Dialog chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 1,
                participant_count: 1,
            }];
            chat.session_id = session_id.clone();
            chat.session_edit_id = Some(session_id.clone());
            chat.session_title_draft = "Dialog chat".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat dialog")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Done")
            .click()?;

        assert!(
            shared.chat().session_edit_id.is_none(),
            "Done should close the edit dialog"
        );

        Ok(())
    }

    #[test]
    fn session_edit_dialog_close_button_closes_in_rendered_view() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let session_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Closable chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 1,
                participant_count: 1,
            }];
            chat.session_id = session_id.clone();
            chat.session_edit_id = Some(session_id.clone());
            chat.session_title_draft = "Closable chat".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat dialog close")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Close edit dialog")
            .click()?;

        assert!(
            shared.chat().session_edit_id.is_none(),
            "close button should dismiss the edit dialog"
        );

        Ok(())
    }

    #[test]
    fn rendered_session_edit_button_opens_dialog() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let session_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Dialog chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 1,
                participant_count: 1,
            }];
            chat.session_id = session_id.clone();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat edit button")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Edit Dialog chat")
            .click()?;

        assert_eq!(
            shared.chat().session_edit_id.as_deref(),
            Some(session_id.as_str()),
            "row edit button should open the edit dialog"
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Done")
            .click()?;
        assert!(shared.chat().session_edit_id.is_none());

        Ok(())
    }

    #[test]
    fn rendered_channel_edit_dialog_shows_agent_list() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let data_dir = backend.data_dir.clone();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Team channel");
        channel.ensure_participant("main-agent", "main-agent");
        channel.ensure_participant("coding-agent", "coding-agent");
        SifsAgentChannelStore::open(&data_dir)
            .expect("open channel store")
            .save_channel(&channel)
            .expect("save channel");
        let session_id = channel_id.to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Team channel".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 2,
            }];
            chat.session_id = session_id.clone();
            chat.models_loading = false;
        }

        let runtime = crate::build_runtime(shared.clone())?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Edit Team channel")
            .click()?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;

        for label in [
            "Close edit dialog",
            "Delete channel",
            "Remove main-agent",
            "Remove coding-agent",
        ] {
            assert!(
                snapshot.accessibility.nodes.iter().any(|node| {
                    node.role == SemanticsRole::Button && node.name.as_deref() == Some(label)
                }),
                "{label} control should be visible in the channel edit dialog"
            );
        }
        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("2 active")),
            "channel edit dialog should show the active-agent count"
        );

        Ok(())
    }

    #[test]
    fn rendered_channel_edit_dialog_disables_duplicate_agent_add() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let data_dir = backend.data_dir.clone();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Ops channel");
        channel.ensure_participant("main-agent", "main-agent");
        SifsAgentChannelStore::open(&data_dir)
            .expect("open channel store")
            .save_channel(&channel)
            .expect("save channel");
        let session_id = channel_id.to_string();
        {
            let mut chat = shared.chat_mut();
            seed_test_agents(&mut chat);
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Ops channel".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 1,
            }];
            chat.session_id = session_id.clone();
            chat.session_edit_id = Some(session_id);
            chat.session_title_draft = "Ops channel".to_string();
            chat.channel_agent_idx = 0;
            chat.models_loading = false;
        }

        let runtime = crate::build_runtime(shared.clone())?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        let add_agent = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button && node.name.as_deref() == Some("Add agent")
            })
            .expect("Add agent button should be visible");

        assert!(
            add_agent.state.disabled,
            "Add agent should be disabled when the selected agent is already assigned"
        );

        Ok(())
    }

    #[test]
    fn clicking_rendered_chat_row_opens_session_without_refcell_panic() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let first_id = SessionId::generate().to_string();
        let second_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![
                SessionRef {
                    id: first_id.clone(),
                    title: "Current chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 1,
                    participant_count: 1,
                },
                SessionRef {
                    id: second_id.clone(),
                    title: "Target chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 2,
                    participant_count: 1,
                },
            ];
            chat.session_id = first_id;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat rows")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Target chat")
            .with_description("Chat")
            .click()?;

        assert_eq!(shared.chat().session_id, second_id);

        Ok(())
    }

    #[test]
    fn rendered_chat_session_filter_opens_matching_session() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let first_id = SessionId::generate().to_string();
        let second_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![
                SessionRef {
                    id: first_id.clone(),
                    title: "Current chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 1,
                    participant_count: 1,
                },
                SessionRef {
                    id: second_id.clone(),
                    title: "Release triage".to_string(),
                    kind: ConversationKind::Channel,
                    updated_unix: 2,
                    participant_count: 2,
                },
            ];
            chat.session_id = first_id;
            chat.session_filter = "release".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat filtered rows")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let snapshot = window.snapshot()?;

        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("CONVERSATIONS · 1/2")),
            "chat heading should show filtered and total counts"
        );
        assert!(
            !snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Current chat")),
            "non-matching chats should not be rendered in the filtered list"
        );

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Release triage")
            .with_description("Channel, 2 agents")
            .click()?;

        assert_eq!(shared.chat().session_id, second_id);

        Ok(())
    }

    #[test]
    fn rendered_session_kind_filter_limits_sidebar_rows() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let first_id = SessionId::generate().to_string();
        let second_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![
                SessionRef {
                    id: first_id.clone(),
                    title: "Current chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 1,
                    participant_count: 1,
                },
                SessionRef {
                    id: second_id.clone(),
                    title: "Release triage".to_string(),
                    kind: ConversationKind::Channel,
                    updated_unix: 2,
                    participant_count: 2,
                },
            ];
            chat.session_id = first_id;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat kind filtered rows")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show channels only")
            .with_description("1 visible conversation(s)")
            .click()?;
        app.run_until_idle()?;

        assert_eq!(
            shared.chat().session_kind_filter,
            SessionKindFilter::Channels
        );
        let snapshot = window.snapshot()?;
        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("CHANNELS · 1")),
            "channel heading should show the channel-only count"
        );
        assert!(
            !snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Current chat")),
            "chat rows should not be rendered in the channel-only list"
        );

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Release triage")
            .with_description("Channel, 2 agents")
            .click()?;

        assert_eq!(shared.chat().session_id, second_id);

        Ok(())
    }

    #[test]
    fn rendered_empty_chat_quick_action_fills_composer_draft() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions.clear();
            chat.messages.clear();
            chat.input.clear();
            chat.session_id = SessionId::generate().to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat quick actions")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Check cluster")
            .with_description("Prepare a cluster status prompt")
            .click()?;

        let chat = shared.chat();
        let check_cluster_prompt = ChatQuickAction::CheckCluster
            .prompt()
            .expect("prompt action");
        assert_eq!(chat.input, check_cluster_prompt);
        assert_eq!(
            chat.cursor,
            check_cluster_prompt.len(),
            "quick action should place the composer cursor at the end of the prepared draft"
        );
        assert!(
            chat.draft_revision > 0,
            "quick action should trigger exactly the programmatic composer refresh path"
        );
        let revision_after_quick_action = chat.draft_revision;
        drop(chat);

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Clear draft")
            .click()?;

        let chat = shared.chat();
        assert!(
            chat.input.is_empty(),
            "clear draft should empty the composer"
        );
        assert_eq!(chat.cursor, 0, "clear draft should reset the cursor");
        assert!(
            chat.draft_revision > revision_after_quick_action,
            "clear draft should trigger the programmatic composer refresh path"
        );

        Ok(())
    }

    #[test]
    fn rendered_empty_channel_surfaces_active_agent_count() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions = vec![SessionRef {
                id: channel_id.clone(),
                title: "Release triage".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 2,
            }];
            chat.session_id = channel_id.clone();
            chat.messages.clear();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel empty state")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            window
                .get_by_text("2 active agents · send a message to the channel.")
                .count()?
                >= 1,
            "selected channel empty state should show the active-agent count"
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Manage channel")
            .with_description("Open channel agents and settings")
            .click()?;
        assert_eq!(
            shared.chat().session_edit_id.as_deref(),
            Some(channel_id.as_str()),
            "Manage channel should open the selected channel editor"
        );

        Ok(())
    }

    #[test]
    fn rendered_sidebar_create_actions_are_explicit_buttons() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions.clear();
            chat.messages.clear();
            chat.input.clear();
            chat.models_loading = false;
            chat.session_edit_id = None;
        }

        let initial_session_id = shared.chat().session_id.clone();
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sidebar create actions")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("New chat")
            .click()?;
        let new_chat_id = shared.chat().session_id.clone();
        assert_ne!(
            new_chat_id, initial_session_id,
            "New chat button should start a fresh visible session"
        );
        assert!(
            shared.chat().session_edit_id.is_none(),
            "New chat should not open the edit dialog"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("New channel")
            .click()?;
        let chat = shared.chat();
        assert_ne!(
            chat.session_id, new_chat_id,
            "New channel button should start a separate visible session"
        );
        assert_eq!(
            chat.session_edit_id.as_deref(),
            Some(chat.session_id.as_str()),
            "New channel should open its edit dialog"
        );

        Ok(())
    }

    #[test]
    fn rendered_chat_sessions_scroll_with_sui_scroll_view() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let sessions = (0..48)
            .map(|index| SessionRef {
                id: SessionId::generate().to_string(),
                title: format!("Scrollable chat {index:02}"),
                kind: ConversationKind::Chat,
                updated_unix: index,
                participant_count: 1,
            })
            .collect::<Vec<_>>();
        let active_id = sessions[0].id.clone();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = sessions;
            chat.session_id = active_id;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sessions scroll")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        let before_y = chat_sessions_list_y(&window)?;
        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("chat-sessions-scroll")
            .scroll_pixels(sui::Vector::new(0.0, -260.0))?;
        let after_y = chat_sessions_list_y(&window)?;

        assert!(
            after_y < before_y - 100.0,
            "sessions list should move when the SUI ScrollView scrolls: before={before_y}, after={after_y}"
        );

        let selected_id = shared.chat().sessions[10].id.clone();
        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Scrollable chat 10")
            .with_description("Chat")
            .click()?;
        let selected_y = chat_sessions_list_y(&window)?;
        assert_eq!(shared.chat().session_id, selected_id);
        assert!(
            (selected_y - after_y).abs() < 2.0,
            "selecting a chat rebuilds the sidebar but should preserve scroll position: before_select={after_y}, after_select={selected_y}"
        );

        Ok(())
    }

    #[test]
    fn chat_sessions_scroll_state_clamps_after_list_shrinks() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            chat.sessions = (0..48)
                .map(|index| SessionRef {
                    id: SessionId::generate().to_string(),
                    title: format!("Clamp chat {index:02}"),
                    kind: ConversationKind::Chat,
                    updated_unix: index,
                    participant_count: 1,
                })
                .collect();
            chat.session_id = chat.sessions[0].id.clone();
            chat.models_loading = false;
        }

        let theme_reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let scroll_state = ScrollState::new();
        let shrink = {
            let s = shared.clone();
            sui::Button::new("Shrink")
                .theme_when(clone_reader(&theme_reader))
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().sessions.truncate(18);
                    use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};
                    let window = InvalidationTarget::Window(ctx.window_id());
                    ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
                    ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
                    ctx.request_paint();
                    ctx.request_semantics();
                })
        };
        let root_widget = Flex::horizontal()
            .align_items(Alignment::Stretch)
            .with_item(
                sidebar(&shared, &theme_reader, &scroll_state),
                FlexItem::fixed(SIDEBAR_W),
            )
            .with_item(shrink, FlexItem::fixed(120.0));

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sessions scroll clamp")
                    .root(root_widget),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("chat-sessions-scroll")
            .scroll_pixels(sui::Vector::new(0.0, -2000.0))?;
        let before_offset = scroll_state.current_offset().y;
        let before_max = scroll_state.max_offset().y;
        assert!(
            before_offset > 100.0,
            "test setup should start scrolled away from the top"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Shrink")
            .click()?;
        let after_offset = scroll_state.current_offset().y;
        let after_max = scroll_state.max_offset().y;

        assert!(
            after_max < before_max,
            "shrinking the chat list should reduce the maximum offset"
        );
        assert!(
            after_offset <= after_max + 1.0,
            "scroll offset should clamp to the shorter list: offset={after_offset}, max={after_max}"
        );
        assert!(
            after_offset > 0.0,
            "shrinking the chat list should not reset scroll to the top while content still overflows"
        );

        Ok(())
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
        assert!(
            state.notice.is_none(),
            "New chat clears transient per-turn notices"
        );
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
    fn new_session_preserves_connection_attention_until_retry_succeeds() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.notice = Some("chat list failed: device authorization was denied".to_string());
        state.push_activity(
            "chat list",
            "device authorization was denied",
            ActivityTone::Warn,
        );
        state.set_draft("draft that should clear".to_string());

        state.new_session();

        assert!(state.messages.is_empty());
        assert!(state.input.is_empty());
        assert_eq!(
            state.notice.as_deref(),
            Some("chat list failed: device authorization was denied")
        );
        assert!(
            state.activity.iter().any(|entry| {
                entry.title == "chat list" && entry.detail == "device authorization was denied"
            }),
            "New chat must not hide unresolved storage attention"
        );
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
        assert_eq!(
            state.recovery_empty_title(RuntimeRecoveryTarget::Connection),
            "Chat list needs connection attention"
        );
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
    fn transcript_error_display_defers_connection_detail_to_settings() {
        let connection = ChatMessage::system_error(
            "Error: chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied",
        );
        let rendered = transcript_message_display_text(&connection);
        assert!(rendered.starts_with("Connection issue\n"));
        assert!(rendered.contains("This chat needs storage authorization."));
        assert!(rendered.contains("Check SIFS first. Connection settings has low-level detail."));
        assert!(!rendered.contains("failed to parse command"));
        assert!(!rendered.contains("SIFS store failed"));
        assert!(!rendered.contains("device authorization was denied"));

        let provider = ChatMessage::system_error("Error: provider unavailable");
        assert_eq!(
            transcript_message_display_text(&provider),
            "Error\nprovider unavailable"
        );
    }

    #[test]
    fn rendered_storage_error_message_opens_files() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.notice = None;
            chat.messages.push(ChatMessage::system_error(
                "Error: chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied",
            ));
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Connection error message action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Check SIFS")
            .with_description("Open SIFS browser")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn rendered_daemon_error_message_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.notice = None;
            chat.messages.push(ChatMessage::system_error(
                "Error: chat unavailable: daemon connection failed",
            ));
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Daemon error message action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Open settings")
            .with_description("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn activity_panel_is_quiet_until_requested_or_needed() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.info.provider = "openai".to_string();
        state.info.model = "gpt-5".to_string();

        assert!(
            !state.activity_panel_visible(),
            "ready idle chat should leave the activity rail closed"
        );

        state.toggle_activity_panel();
        assert!(state.activity_panel_visible());
        state.toggle_activity_panel();
        assert!(!state.activity_panel_visible());

        state.streaming = true;
        assert!(
            state.activity_panel_visible(),
            "active streaming should keep runtime activity visible"
        );
        state.streaming = false;
        assert!(!state.activity_panel_visible());

        state.push_activity(
            "chat list",
            "device authorization was denied",
            ActivityTone::Warn,
        );
        assert!(
            state.activity_panel_visible(),
            "connection warnings should force the activity rail open"
        );
    }

    #[test]
    fn timed_out_session_list_load_can_recover_from_late_result() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let (tx, rx) = mpsc::channel();
        state.session_list_rx = Some(rx);
        state.session_list_loading = true;
        state.session_list_started_at = Some(
            Instant::now()
                .checked_sub(SESSION_LIST_UI_TIMEOUT + Duration::from_millis(1))
                .expect("past timestamp"),
        );
        state.session_list_slow_notice_shown = false;

        assert!(
            state.pump_session_list(),
            "slow load should mutate visible state once"
        );
        assert!(state.session_list_loading);
        assert!(state.session_list_slow_notice_shown);
        assert!(state.notice.is_none());
        assert!(state.connection_attention_detail().is_none());
        let chat_list_statuses = state
            .activity
            .iter()
            .filter(|entry| entry.title == "chat list")
            .collect::<Vec<_>>();
        assert_eq!(chat_list_statuses.len(), 1);
        assert_eq!(chat_list_statuses[0].tone, ActivityTone::Running);
        assert!(
            chat_list_statuses[0].detail.contains("still loading"),
            "slow load should be a running status, not a failure"
        );
        assert!(
            !state.pump_session_list(),
            "slow status should not duplicate while the worker is still pending"
        );
        assert_eq!(
            state
                .activity
                .iter()
                .filter(|entry| entry.title == "chat list")
                .count(),
            1
        );

        let session_id = SessionId::generate().to_string();
        tx.send(SessionListWorkerResult {
            sessions: vec![SessionRef {
                id: session_id.clone(),
                title: "Recovered chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 7,
                participant_count: 1,
            }],
            error: None,
        })
        .expect("send late result");

        assert!(
            state.pump_session_list(),
            "late worker result should still apply"
        );
        assert_eq!(state.sessions.len(), 1);
        assert_eq!(state.sessions[0].id, session_id);
        assert!(!state.session_list_loading);
        assert!(!state.session_list_slow_notice_shown);
        assert!(state.notice.is_none());
        assert!(
            state
                .activity
                .iter()
                .all(|entry| entry.title != "chat list"),
            "successful late result should clear stale chat-list warnings"
        );
    }

    #[test]
    fn rendered_activity_button_toggles_idle_activity_panel() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.info.provider = "openai".to_string();
            chat.info.model = "gpt-5".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat activity button")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Details")
            .click()?;
        assert!(
            shared.chat().activity_panel_visible(),
            "Activity button should open the idle activity rail"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Details")
            .click()?;
        assert!(
            !shared.chat().activity_panel_visible(),
            "Activity button should close the idle activity rail"
        );

        Ok(())
    }

    #[test]
    fn activity_button_label_tracks_runtime_state() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.info.provider = "openai".to_string();
        state.info.model = "gpt-5".to_string();
        state.models_loading = false;

        assert_eq!(activity_button_label(&state), "Details");
        assert!(
            activity_button_description(&state).contains("READY"),
            "quiet runtime description should include the readiness state"
        );

        state.streaming = true;
        assert_eq!(activity_button_label(&state), "Monitor");
        state.streaming = false;

        state.push_activity(
            "chat list",
            "device authorization was denied",
            ActivityTone::Warn,
        );
        assert_eq!(activity_button_label(&state), "Review");
        assert!(
            activity_button_description(&state).contains("Chat history needs recovery"),
            "warning description should use the user-facing recovery summary"
        );
    }

    #[test]
    fn runtime_overview_uses_visible_model_selection_before_runtime_info() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.info = RuntimeInfo::default();
        state.models_loading = false;

        assert_eq!(runtime_overview_title(&state), "OpenAI / gpt-5");

        let health = state.runtime_health();
        assert_eq!(health.label, "READY");
        assert_eq!(health.detail, "openai / gpt-5");
        assert_eq!(health.tone, ActivityTone::Good);
    }

    #[test]
    fn activity_rail_detail_toggle_expands_latest_warning() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.push_activity(
                "chat connection",
                "Agent channel storage could not open through SIFS: device authorization was denied",
                ActivityTone::Warn,
            );
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Activity detail")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show detail")
            .with_description("Activity detail")
            .click()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Hide detail")
                .with_description("Activity detail")
                .count()?,
            1
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Hide detail")
            .with_description("Activity detail")
            .click()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show detail")
                .with_description("Activity detail")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn connection_problem_summary_hides_parser_plumbing() {
        let summary = connection_problem_summary(
            "chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied",
        )
        .expect("connection summary");

        assert_eq!(
            summary,
            "Agent channel storage could not open through SIFS; device authorization was denied."
        );
        assert!(!summary.contains("failed to parse command"));
    }

    #[test]
    fn connection_recovery_display_detail_hides_low_level_storage_errors() {
        let chat_list =
            connection_recovery_display_detail("chat list failed: device authorization was denied")
                .expect("chat list display detail");
        assert_eq!(chat_list, "Sinomo could not load saved chats.");
        assert!(!chat_list.contains("device authorization"));
        assert!(!chat_list.contains("SIFS"));
        assert!(!chat_list.contains("storage"));

        let channel = connection_recovery_display_detail(
            "channel create failed: device authorization was denied",
        )
        .expect("channel display detail");
        assert_eq!(channel, "Sinomo could not open this chat channel.");
        assert!(!channel.contains("device authorization"));
        assert!(!channel.contains("SIFS"));
        assert!(!channel.contains("storage"));

        assert!(connection_recovery_display_detail("provider unavailable").is_none());
    }

    #[test]
    fn collapsed_activity_detail_uses_connection_summary() {
        let visible = activity_visible_detail("chat list failed: device authorization was denied");
        assert_eq!(visible, "Sinomo could not load saved chats.");
        assert!(!visible.contains("device authorization"));
        assert!(!visible.contains("storage"));

        let neutral = activity_visible_detail("context assembled: 4.0 KB");
        assert_eq!(neutral, "context assembled: 4.0 KB");
    }

    #[test]
    fn activity_status_summary_keeps_health_card_actionable() {
        assert_eq!(
            activity_status_summary("chat list: device authorization was denied"),
            "Chat history needs recovery"
        );
        assert_eq!(
            activity_status_summary(
                "chat connection: Agent channel storage could not open through SIFS; device authorization was denied.",
            ),
            "Channel storage needs recovery"
        );
        assert_eq!(
            activity_status_summary("runtime control metadata is detached"),
            "Runtime controls need refresh"
        );

        let neutral = activity_status_summary("context assembled: 4.0 KB");
        assert_eq!(neutral, "context assembled: 4.0 KB");
    }

    #[test]
    fn activity_work_summary_hides_zero_count_noise() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);

        assert_eq!(activity_work_summary(&state), "Idle · no queued work");
        assert_eq!(usage_summary(&state), None);

        state.control.active_turn = true;
        state.control.task_count = 2;
        state.control.follow_up_count = 1;
        state.tool_calls.push(ChatToolActivity {
            step_index: 0,
            call_index: Some(0),
            call_id: Some("call-test".to_string()),
            tool: "shell".to_string(),
            input_preview: "cargo check".to_string(),
            status: "running".to_string(),
        });

        assert_eq!(
            activity_work_summary(&state),
            "Active · 2 tasks · 1 queued turn · 1 tool update"
        );
    }

    #[test]
    fn usage_summary_only_renders_real_usage() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        assert_eq!(usage_summary(&state), None);

        state.usage = Some(ChatUsageSnapshot {
            provider: "DeepSeek".to_string(),
            model: "deepseek-v4-flash".to_string(),
            input_tokens: Some(1200),
            output_tokens: Some(42),
        });

        assert_eq!(usage_summary(&state), Some("1.2k in · 42 out".to_string()));
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
        state.notice = Some("chat save failed: DFS services shard replay delayed".to_string());

        let health = state.runtime_health();
        assert_eq!(health.label, "DEGRADED");
        assert_eq!(health.tone, ActivityTone::Warn);
        assert!(
            health.detail.contains("chat save failed"),
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
    fn connection_worker_errors_are_activity_not_agent_failures() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);

        state.apply_event(ChatEvent::Error {
            session_id: Some(state.session_id.clone()),
            error: "chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied".to_string(),
        });

        assert!(
            state.activity.iter().any(|entry| {
                entry.title == "chat connection"
                    && entry.tone == ActivityTone::Warn
                    && entry.detail.contains("Agent channel storage")
                    && !entry.detail.contains("failed to parse command")
            }),
            "storage-derived worker errors should read as chat connection recovery"
        );
        assert!(
            state
                .activity
                .iter()
                .all(|entry| entry.title != "agent error"),
            "connection failures should not be labeled as generic agent failures"
        );
        let health = state.runtime_health();
        assert_eq!(health.label, "DEGRADED");
        assert_eq!(health.tone, ActivityTone::Warn);
        assert!(health.detail.contains("chat connection"));
        assert!(!health.detail.contains("failed to parse command"));
    }

    #[test]
    fn runtime_recovery_target_routes_to_actionable_settings_section() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers.clear();
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Providers)
        );
        assert_eq!(
            RuntimeRecoveryTarget::Providers.footer_label(),
            "Provider setup"
        );
        assert_eq!(
            state.recovery_empty_detail(RuntimeRecoveryTarget::Providers),
            "No language model provider is configured yet."
        );

        seed_default_provider(&mut state);
        state.notice = Some("chat save failed: DFS services shard replay delayed".to_string());
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
        assert_eq!(
            RuntimeRecoveryTarget::Connection.footer_label(),
            "Connection"
        );
        assert!(
            state
                .recovery_empty_detail(RuntimeRecoveryTarget::Connection)
                .contains("Sinomo could not save chat history")
        );

        state.notice = Some("chat list failed: device authorization was denied".to_string());
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
        assert_eq!(
            state.recovery_empty_title(RuntimeRecoveryTarget::Connection),
            "Chat list needs connection attention"
        );
        assert_eq!(
            state.recovery_empty_detail(RuntimeRecoveryTarget::Connection),
            "Sinomo could not load saved chats."
        );
        assert!(
            !state
                .recovery_empty_detail(RuntimeRecoveryTarget::Connection)
                .contains("device authorization")
        );
        assert!(
            !state
                .recovery_empty_detail(RuntimeRecoveryTarget::Connection)
                .contains("storage")
        );

        state.notice = Some("channel create failed: device authorization was denied".to_string());
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
        assert_eq!(
            state.recovery_empty_title(RuntimeRecoveryTarget::Connection),
            "Channel needs connection attention"
        );
        assert_eq!(
            state.recovery_empty_detail(RuntimeRecoveryTarget::Connection),
            "Sinomo could not open this chat channel."
        );
        assert_eq!(
            state.recovery_empty_hint(RuntimeRecoveryTarget::Connection),
            "Open Connection settings if channel creation keeps failing; inspect SIFS from Files for storage diagnostics."
        );

        state.notice = Some(
            "authentication required for codex: no Sinomo Codex ChatGPT sign-in found".to_string(),
        );
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Providers)
        );

        state.notice = None;
        state.info.provider = "deepseek".to_string();
        state.info.model = "deepseek-chat".to_string();
        assert_eq!(state.runtime_recovery_target(), None);
    }

    #[test]
    fn retry_chat_list_clears_stale_connection_warning_after_success() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.notice = Some("chat list failed: device authorization was denied".to_string());
        state.push_activity(
            "chat list",
            "device authorization was denied",
            ActivityTone::Warn,
        );

        state.retry_chat_list();

        assert!(state.notice.is_none());
        assert!(
            state
                .activity
                .iter()
                .all(|entry| entry.title != "chat list")
        );
        assert_eq!(state.runtime_recovery_target(), None);
    }

    #[test]
    fn rendered_retry_chat_list_button_uses_existing_refresh_path() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.messages.push(ChatMessage::user("Existing context"));
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat retry")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Retry chat list")
            .click()?;

        let chat = shared.chat();
        assert!(chat.notice.is_none());
        assert!(chat.activity.iter().all(|entry| entry.title != "chat list"));
        assert_eq!(chat.runtime_recovery_target(), None);

        Ok(())
    }

    #[test]
    fn rendered_empty_recovery_card_suppresses_duplicate_footer_actions() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card duplicate actions")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry chat list")
                .count()?,
            0,
            "the footer retry button should not duplicate the recovery card action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Connection")
                .count()?,
            0,
            "the footer settings button should not duplicate the recovery card action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry now")
                .with_description("Retry the chat list")
                .count()?,
            1,
            "the card keeps a single primary retry action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Open settings")
                .with_description("Connection settings")
                .count()?,
            1,
            "the card keeps a single settings action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Check SIFS")
                .with_description("Open SIFS browser")
                .count()?,
            1,
            "the card exposes the SIFS diagnostic path without duplicating it in the footer"
        );

        Ok(())
    }

    #[test]
    fn rendered_recovery_card_retry_button_uses_existing_refresh_path() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card retry")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Retry now")
            .with_description("Retry the chat list")
            .click()?;

        let chat = shared.chat();
        assert!(chat.notice.is_none());
        assert!(chat.activity.iter().all(|entry| entry.title != "chat list"));
        assert_eq!(chat.runtime_recovery_target(), None);

        Ok(())
    }

    #[test]
    fn rendered_recovery_card_settings_button_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card settings")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Open settings")
            .with_description("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn rendered_recovery_card_check_sifs_opens_files() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card SIFS")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Check SIFS")
            .with_description("Open SIFS browser")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn recovery_card_pointer_check_sifs_opens_files() -> sui::Result<()> {
        use sui::{
            Event, Point, PointerButton, PointerButtons, PointerEvent, PointerEventKind,
            WindowEvent,
        };

        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card pointer")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let root = window.root();
        root.dispatch_event(Event::Window(WindowEvent::Resized(Size::new(
            1280.0, 720.0,
        ))))?;
        app.run_until_idle()?;

        let pos = Point::new(580.0, 402.0);
        root.dispatch_event(Event::Pointer(PointerEvent::new(
            PointerEventKind::Move,
            pos,
        )))?;
        let mut down = PointerEvent::new(PointerEventKind::Down, pos);
        down.button = Some(PointerButton::Primary);
        down.buttons = PointerButtons::new(1);
        root.dispatch_event(Event::Pointer(down))?;
        let mut up = PointerEvent::new(PointerEventKind::Up, pos);
        up.button = Some(PointerButton::Primary);
        root.dispatch_event(Event::Pointer(up))?;
        app.run_until_idle()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn rendered_channel_recovery_card_hides_chat_list_retry() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice =
                Some("channel create failed: device authorization was denied".to_string());
            chat.push_activity(
                "channel create",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel recovery card settings")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry now")
                .count()?,
            0,
            "channel creation failures should not offer a chat-list retry action"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Open settings")
            .with_description("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn rendered_channel_recovery_footer_offers_sifs_check_without_retry() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice =
                Some("channel create failed: device authorization was denied".to_string());
            chat.push_activity(
                "channel create",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.messages.push(ChatMessage::user("Existing context"));
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel recovery footer settings")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry chat list")
                .count()?,
            0,
            "the footer should not offer chat-list retry for channel failures"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Check SIFS")
                .count()?,
            1,
            "the footer should still expose the SIFS diagnostic path for channel failures"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Check SIFS")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn rendered_channel_recovery_footer_settings_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice =
                Some("channel create failed: device authorization was denied".to_string());
            chat.push_activity(
                "channel create",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.messages.push(ChatMessage::user("Existing context"));
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel recovery footer connection")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn rendered_recovery_card_provider_button_opens_provider_setup() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            chat.providers.clear();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card provider")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Setup provider")
                .count()?,
            0,
            "the composer setup button should not duplicate the recovery card action"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Provider setup")
            .with_description("Provider setup")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        let settings = shared.settings();
        assert_eq!(settings.selected, Some(SettingsSection::Providers));
        assert!(settings.provider_add_open);

        Ok(())
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
    fn context_overview_summary_describes_lifecycle_state() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.info.provider = "openai".to_string();
        state.info.model = "gpt-5".to_string();
        state.activity.clear();
        state.notice = None;

        assert_eq!(
            context_overview_summary(&state),
            "Context will assemble when you send a message"
        );

        state.messages.push(ChatMessage::user("hello"));
        assert_eq!(
            context_overview_summary(&state),
            "Context will refresh on the next send"
        );

        state.streaming = true;
        assert_eq!(
            context_overview_summary(&state),
            "Assembling context for this response"
        );

        state.streaming = false;
        state.context = Some(ChatContextSnapshot {
            step_index: 0,
            provider: "openai".to_string(),
            model: "gpt-5".to_string(),
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
        });
        assert_eq!(
            context_overview_summary(&state),
            "Context 4.0 KB of 8.0 KB · 3 sources · 1 omitted"
        );
    }

    #[test]
    fn context_overview_summary_routes_blocked_context_to_setup() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers.clear();
        state.models_loading = false;
        state.activity.clear();
        state.notice = None;

        assert_eq!(
            context_overview_summary(&state),
            "Context waits for provider setup"
        );

        seed_default_provider(&mut state);
        state.info.provider = "openai".to_string();
        state.info.model = "gpt-5".to_string();
        state.activity.push(ChatActivityEntry {
            title: "chat list".to_string(),
            detail: "SIFS unavailable: daemon not reachable".to_string(),
            tone: ActivityTone::Warn,
        });

        assert_eq!(
            context_overview_summary(&state),
            "Context waits for cluster connection"
        );
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
            && notice.contains("chat save failed")
        {
            assert!(
                state
                    .activity
                    .iter()
                    .any(|entry| entry.title == "chat save" && entry.tone == ActivityTone::Warn),
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
            state.pump_session_list();
            state.pump_channel_create();
            state.pump_delete();
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
