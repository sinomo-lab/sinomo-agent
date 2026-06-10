//! "Sinomo" tab — the AI chat experience.
//!
//! Like the Settings and Files tabs, this is a fully *composed* `sui-widgets`
//! tree: [`view`] returns a responsive host ([`ChatView`]) over pre-built pods
//! (wide split with the agents/sessions sidebar, narrow transcript-only), each
//! assembled from stock `ListView` / `ScrollView` / `Select` / `TextArea` /
//! `Button` / `Spinner` / `Separator` / `Label` widgets, plus a small set of
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
    Alignment, ArrangeCtx, Constraints, DefaultTheme, Event, EventCtx, FontWeight, IconButton,
    IconGlyph, Insets, KeyState, Label, ListItem, ListView, MeasureCtx, PaintCtx, PointerEventKind,
    Rect, ScrollDelta, SemanticsCtx, Separator, Size, SizedBox, Stack, Surface, SurfaceBorder,
    SurfaceRole, TextArea, Widget, WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor, draw_glyph,
};

use sinomo_cli::app_api::{
    AgentSummary, ChatHistoryMessage, ProviderSummary, agent_summaries, reasoning_efforts,
};
use sinomo_runtime::local::SinomoDataDir;

use crate::backend::Backend;
use crate::backend::chat_engine::{ChatCommand, ChatEvent, ChatHandle, ChatRole, RuntimeInfo};
use crate::backend::sessions::{self, StoredMessage};
use crate::state::Shared;
use crate::theme::Palette;
use crate::ui::widgets;

/// Below this content width the sidebar collapses away (transcript + composer only).
const WIDE_THRESHOLD: f32 = 620.0;
const SIDEBAR_W: f32 = 250.0;
const COMPOSER_H: f32 = 132.0;
const PAD: f32 = 16.0;
const SIDEBAR_ROW_H: f32 = 36.0;
const SELECTOR_H: f32 = 30.0;
const COMPOSER_PAD_Y: f32 = 12.0;
const COMPOSER_SECTION_GAP: f32 = 10.0;
const INPUT_ACTION_GAP: f32 = 10.0;
const SEND_BUTTON_SIZE: f32 = 40.0;

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
pub struct ChatMessage {
    pub role: ChatRole,
    pub text: String,
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
}

impl ChatState {
    pub fn new(backend: &Backend) -> Self {
        let daemon_connected = backend.connection.is_daemon();
        let handle = ChatHandle::spawn((*backend.data_dir).clone(), daemon_connected);

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
        };
        // Eagerly fetch the default/initial provider's models so the model picker is populated
        // without requiring a manual provider toggle. (Results arrive async via `ChatEvent::Models`;
        // a no-op if there is no provider or the list is already populated.)
        state.maybe_fetch_models();
        state
    }

    /// Title derived from the first user message (for the sidebar).
    fn derived_title(&self) -> String {
        self.messages
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
        if self.session_id.is_empty() || self.messages.is_empty() {
            return;
        }
        let title = self.derived_title();
        let stored: Vec<StoredMessage> = self
            .messages
            .iter()
            .map(|m| StoredMessage {
                role: match m.role {
                    ChatRole::User => "user".to_string(),
                    ChatRole::Agent => "agent".to_string(),
                },
                text: m.text.clone(),
            })
            .collect();
        let _ = sessions::save(&self.data_dir, &self.session_id, &title, &stored);
        self.sessions = sessions::list(&self.data_dir)
            .into_iter()
            .map(|meta| SessionRef {
                id: meta.id,
                title: meta.title,
            })
            .collect();
    }

    /// Open a stored session: load its transcript for display and tell the worker
    /// to continue under that id.
    fn open_session(&mut self, index: usize) {
        let Some(session) = self.sessions.get(index) else {
            return;
        };
        let id = session.id.clone();
        let transcript = sessions::load_transcript(&self.data_dir, &id).unwrap_or_default();
        let history: Vec<ChatHistoryMessage> = transcript
            .messages
            .iter()
            .map(|m| ChatHistoryMessage {
                role: m.role.clone(),
                text: m.text.clone(),
            })
            .collect();
        self.messages = transcript
            .messages
            .into_iter()
            .map(|m| ChatMessage {
                role: if m.role == "user" {
                    ChatRole::User
                } else {
                    ChatRole::Agent
                },
                text: m.text,
            })
            .collect();
        self.session_id = id.clone();
        self.streaming = false;
        self.notice = None;
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
        self.current_provider()
            .and_then(|p| p.models.get(self.model_idx))
            .cloned()
            .unwrap_or_else(|| self.info.model.clone())
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
        match event {
            ChatEvent::Ready { session_id, info } => {
                self.session_id = session_id;
                self.info = info;
                self.sync_selection_from_info();
            }
            ChatEvent::Token(token) => {
                self.notice = None;
                if let Some(last) = self.messages.last_mut() {
                    if last.role == ChatRole::Agent {
                        last.text.push_str(&token);
                    } else {
                        self.messages.push(ChatMessage {
                            role: ChatRole::Agent,
                            text: token,
                        });
                    }
                } else {
                    self.messages.push(ChatMessage {
                        role: ChatRole::Agent,
                        text: token,
                    });
                }
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
            }
            ChatEvent::Notice(notice) => self.notice = Some(notice),
            ChatEvent::TurnDone => {
                self.streaming = false;
                self.notice = None;
                if matches!(self.messages.last(), Some(m) if m.role == ChatRole::Agent && m.text.trim().is_empty())
                    && let Some(last) = self.messages.last_mut()
                {
                    last.text = "(no response)".to_string();
                }
                self.persist();
            }
            ChatEvent::RuntimeInfo(info) => {
                self.info = info;
                self.sync_selection_from_info();
            }
            ChatEvent::SessionReset { session_id } => {
                self.session_id = session_id;
                self.messages.clear();
                self.streaming = false;
                self.notice = None;
                self.scroll = 0.0;
            }
            ChatEvent::Models { provider, models } => {
                if let Some(idx) = self.providers.iter().position(|p| p.id == provider) {
                    self.providers[idx].models = models;
                    // Re-align the model index to the current model if it's in the list.
                    if idx == self.provider_idx {
                        self.model_idx = self.providers[idx]
                            .models
                            .iter()
                            .position(|m| *m == self.info.model)
                            .unwrap_or(0);
                    }
                }
                self.models_loading = false;
            }
            ChatEvent::Error(error) => {
                self.streaming = false;
                self.messages.push(ChatMessage {
                    role: ChatRole::Agent,
                    text: format!("⚠ {error}"),
                });
                self.notice = None;
            }
        }
        true
    }

    fn sync_selection_from_info(&mut self) {
        if let Some(idx) = self
            .providers
            .iter()
            .position(|p| p.id == self.info.provider)
        {
            self.provider_idx = idx;
            if let Some(midx) = self.providers[idx]
                .models
                .iter()
                .position(|m| *m == self.info.model)
            {
                self.model_idx = midx;
            }
        }
        if let Some(effort) = &self.info.effort
            && let Some(idx) = self.efforts.iter().position(|e| e == effort)
        {
            self.effort_idx = idx;
        }
        // The runtime just told us which provider is active; if its model list has not been fetched
        // yet, kick the async fetch now so the model picker populates for the default provider
        // (without waiting for the user to toggle providers).
        self.maybe_fetch_models();
    }

    fn submit(&mut self) {
        let text = self.input.trim().to_string();
        if text.is_empty() || self.streaming {
            return;
        }
        self.messages.push(ChatMessage {
            role: ChatRole::User,
            text: text.clone(),
        });
        self.messages.push(ChatMessage {
            role: ChatRole::Agent,
            text: String::new(),
        });
        self.input.clear();
        self.cursor = 0;
        self.streaming = true;
        self.stick_bottom = true;
        self.scroll = f32::MAX;
        self.notice = Some("thinking…".to_string());
        self.handle.send_command(ChatCommand::Send(text));
    }

    // -- Composed-view action methods (mutation entry points for the widgets) --

    /// Whether the Send button should be enabled (non-empty draft, not streaming).
    fn can_send(&self) -> bool {
        !self.input.trim().is_empty() && !self.streaming
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
        self.handle.send_command(ChatCommand::NewSession);
    }

    /// Push the current provider/model selection to the worker.
    fn push_model_selection(&mut self) {
        let provider = self
            .current_provider()
            .map(|p| p.id.clone())
            .unwrap_or_default();
        let model = self.current_model();
        if !provider.is_empty() {
            self.handle.send_command(ChatCommand::SetModel {
                provider,
                model,
                account: "default".to_string(),
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
            .map(|p| p.models.is_empty())
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

// ---------------------------------------------------------------------------
// Theme reader + small helpers (mirrors the Settings / Files tabs).
// ---------------------------------------------------------------------------

/// Clone the shared theme reader into a fresh `Fn() -> DefaultTheme` for one control.
fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

/// Resolve the app chrome palette for the current theme (used by the `Label` color readers and the
/// bespoke painters so muted/accent text matches the chrome).
fn palette(shared: &Shared) -> Palette {
    Palette::from_theme(&shared.theme(), widgets::theme_is_dark(shared))
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
            let (bx, bubble_color, text_color, border) = if item.is_user {
                (
                    area.x() + area.width() - PAD - item.bubble_w,
                    pal.accent,
                    pal.on_accent,
                    None,
                )
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
fn selector_signature(shared: &Shared) -> (usize, usize, usize, bool, bool) {
    let chat = shared.chat();
    (
        chat.provider_idx,
        chat.providers.len(),
        chat.current_models().len(),
        chat.effort_visible(),
        chat.models_loading,
    )
}

/// Build the provider / model / effort selector row for the current selection.
fn selectors(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
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
            .theme_when(clone_reader(reader))
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
            .theme_when(clone_reader(reader))
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
            .theme_when(clone_reader(reader))
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

/// The composer's bottom row: a flexible message `TextArea` and a fixed-size send action slot. The
/// input is bound to the draft via `on_change`; the send action is wired to `ChatState::submit`.
/// The Send control swaps to a `Spinner` while streaming.
fn input_row(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let input = {
        let s = shared.clone();
        let draft = shared.chat().input.clone();
        let theme = shared.theme();
        TextArea::new("chat-input")
            .value(draft)
            .placeholder("Message Sinomo…  (⏎ to send, ⇧⏎ for newline)")
            .text_style(sui::TextStyle {
                font_size: 13.5,
                line_height: 19.0,
                ..theme.body_text_style()
            })
            .padding(Insets {
                left: 12.0,
                top: 8.0,
                right: 12.0,
                bottom: 8.0,
            })
            .theme_when(clone_reader(reader))
            .on_change({
                let s = s.clone();
                move |value| {
                    s.chat_mut().set_draft(value);
                }
            })
            // Enter sends, Shift+Enter inserts a newline (the on_submit only fires while the
            // TextArea is focused — Enter only reaches a focused input). `submit()` already no-ops
            // when the draft is empty or a turn is streaming, so this matches `can_send`.
            .on_submit(move |_text| {
                s.chat_mut().submit();
            })
    };

    let streaming = shared.chat().streaming;
    let send: WidgetPod = if streaming {
        WidgetPod::new(
            sui::Spinner::new("chat-sending")
                .size(22.0)
                .theme_when(clone_reader(reader)),
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

    // The input row is rebuilt when the streaming flag flips (Send <-> Spinner). The draft text is
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
        .bottom(COMPOSER_H, composer(shared, reader))
}

fn wide_pod(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let row = Split::left_fixed(
        sidebar(shared, reader),
        main_column(shared, reader),
        SIDEBAR_W,
        reader,
    );
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
    /// re-measures and the newly-built option lists / Send<->Spinner swap take effect.
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
        // sidebar sessions, or the streaming flag (Send <-> Spinner) — anything the inner
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

    #[test]
    fn submit_pushes_user_and_agent_placeholder_and_sets_streaming() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.set_draft("hello there".to_string());
        state.submit();
        assert!(state.streaming, "submitting begins a streaming turn");
        assert_eq!(state.messages.len(), 2, "user message + agent placeholder");
        assert_eq!(state.messages[0].role, ChatRole::User);
        assert_eq!(state.messages[0].text, "hello there");
        assert_eq!(state.messages[1].role, ChatRole::Agent);
        assert!(
            state.messages[1].text.is_empty(),
            "agent placeholder is blank"
        );
        assert!(state.input.is_empty(), "the draft is cleared on submit");
    }

    #[test]
    fn empty_or_streaming_submit_is_a_noop() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        // Empty draft → nothing happens.
        state.submit();
        assert!(!state.streaming);
        assert!(state.messages.is_empty());
        // While streaming, a submit is ignored.
        state.set_draft("queued".to_string());
        state.streaming = true;
        state.submit();
        assert!(
            state.messages.is_empty(),
            "a submit while streaming is ignored"
        );
        assert_eq!(state.input, "queued", "the draft is preserved");
    }

    #[test]
    fn streamed_tokens_append_to_the_last_agent_message() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.set_draft("hi".to_string());
        state.submit();
        // Tokens append to the trailing (empty) agent placeholder rather than pushing new bubbles.
        state.apply_event(ChatEvent::Token("Hel".to_string()));
        state.apply_event(ChatEvent::Token("lo".to_string()));
        assert_eq!(state.messages.len(), 2, "tokens append, not push");
        assert_eq!(state.messages[1].text, "Hello");
        // TurnDone clears streaming and the notice.
        state.apply_event(ChatEvent::TurnDone);
        assert!(!state.streaming, "TurnDone ends the stream");
        assert!(state.notice.is_none());
    }

    #[test]
    fn can_send_requires_nonempty_draft_and_not_streaming() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        assert!(!state.can_send(), "empty draft cannot send");
        state.set_draft("ok".to_string());
        assert!(state.can_send(), "a non-empty draft can send");
        state.streaming = true;
        assert!(!state.can_send(), "cannot send while streaming");
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

    /// Issue 2: the default/initial provider's model list must be fetched eagerly at construction —
    /// without waiting for the user to toggle providers — so the model picker is never stuck on
    /// "No models". `cluster_provider_summaries` always seeds providers with empty model lists, so
    /// when there is a default provider, `ChatState::new` must have kicked the async fetch
    /// (`models_loading` set; the result lands later via `ChatEvent::Models`).
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
                .is_some_and(|p| p.models.is_empty()),
            "providers are seeded with empty model lists (fetched on demand)"
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

    /// Issue 1: Enter-to-send. The composer wires `TextArea::on_submit` to `ChatState::submit`, so a
    /// plain Enter starts a turn. `submit()` is the exact entry point that wiring calls; assert it
    /// begins a streaming turn from a non-empty draft (and is a no-op otherwise — covered above).
    #[test]
    fn submit_is_the_enter_to_send_entry_point() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
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
}
