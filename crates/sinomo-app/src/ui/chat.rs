//! "Sinomo" tab — the AI chat experience.
//!
//! A single self-contained widget owns the whole tab (agents + sessions sidebar,
//! transcript, and the composer with provider/model/effort selectors). Keeping it
//! monolithic avoids cross-widget invalidation: when streamed tokens mutate state
//! we just re-measure/paint this one widget.

use std::cell::Cell;
use std::sync::Arc;

use sui::{
    ArrangeCtx, Constraints, Event, EventCtx, EventPhase, FontWeight, KeyState, MeasureCtx,
    PaintCtx, Point, PointerButton, PointerEventKind, Rect, ScrollDelta, Size, Widget,
};

use sinomo_cli::app_api::{AgentSummary, ProviderSummary, agent_summaries, reasoning_efforts};
use sinomo_runtime::local::SinomoDataDir;

use crate::backend::Backend;
use crate::backend::chat_engine::{ChatCommand, ChatEvent, ChatHandle, ChatRole, RuntimeInfo};
use crate::backend::sessions::{self, StoredMessage};
use crate::state::Shared;
use crate::theme::Palette;
use crate::ui::widgets;

const SIDEBAR_W: f32 = 250.0;
const COMPOSER_H: f32 = 132.0;
const PAD: f32 = 16.0;
const ROW_H: f32 = 34.0;
const SELECTOR_H: f32 = 30.0;

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
    pub focused: bool,
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

        Self {
            data_dir: backend.data_dir.clone(),
            handle,
            session_id: String::new(),
            info: RuntimeInfo::default(),
            messages: Vec::new(),
            input: String::new(),
            cursor: 0,
            focused: false,
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
        }
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
        self.handle.send_command(ChatCommand::Resume(id));
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

    fn current_effort(&self) -> String {
        self.efforts
            .get(self.effort_idx)
            .cloned()
            .unwrap_or_else(|| "medium".to_string())
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
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Hit {
    Agent(usize),
    NewChat,
    Session(usize),
    /// One of the three composer selectors (0 = provider, 1 = model, 2 = effort).
    Selector(usize),
    /// An item inside the currently-open selector dropdown.
    MenuItem(usize),
    Send,
    Input,
}

pub struct ChatView {
    shared: Shared,
    sidebar_rect: Rect,
    messages_rect: Rect,
    composer_rect: Rect,
    input_rect: Rect,
    send_rect: Rect,
    selector_rects: [Rect; 3],
    /// Which selector's dropdown is open (`None` = closed), and its painted geometry.
    open_menu: Option<usize>,
    menu_rect: Rect,
    menu_item_rects: Vec<Rect>,
    /// Whether the reasoning-effort selector is shown (only for models that support it).
    effort_visible: bool,
    newchat_rect: Rect,
    agent_rects: Vec<Rect>,
    session_rects: Vec<Rect>,
    sel_w: [f32; 3],
    content_height: Cell<f32>,
    viewport_height: Cell<f32>,
    hovered: Option<Hit>,
    pump_alive: bool,
    idle_frames: u32,
}

impl ChatView {
    pub fn new(shared: Shared) -> Self {
        Self {
            shared,
            sidebar_rect: Rect::ZERO,
            messages_rect: Rect::ZERO,
            composer_rect: Rect::ZERO,
            input_rect: Rect::ZERO,
            send_rect: Rect::ZERO,
            selector_rects: [Rect::ZERO; 3],
            open_menu: None,
            menu_rect: Rect::ZERO,
            menu_item_rects: Vec::new(),
            effort_visible: true,
            newchat_rect: Rect::ZERO,
            agent_rects: Vec::new(),
            session_rects: Vec::new(),
            sel_w: [120.0, 150.0, 90.0],
            content_height: Cell::new(0.0),
            viewport_height: Cell::new(0.0),
            hovered: None,
            pump_alive: false,
            idle_frames: 0,
        }
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        (theme, pal)
    }

    fn ensure_pump(&mut self, ctx: &mut EventCtx) {
        if !self.pump_alive {
            ctx.request_animation_frame();
            self.pump_alive = true;
        }
    }

    fn hit_test(&self, p: Point) -> Option<Hit> {
        // An open dropdown floats above everything, so its items take precedence.
        if self.open_menu.is_some() {
            for (i, rect) in self.menu_item_rects.iter().enumerate() {
                if rect.contains(p) {
                    return Some(Hit::MenuItem(i));
                }
            }
        }
        if self.send_rect.contains(p) {
            return Some(Hit::Send);
        }
        for (i, rect) in self.selector_rects.iter().enumerate() {
            if rect.contains(p) {
                return Some(Hit::Selector(i));
            }
        }
        if self.input_rect.contains(p) {
            return Some(Hit::Input);
        }
        if self.newchat_rect.contains(p) {
            return Some(Hit::NewChat);
        }
        for (i, rect) in self.agent_rects.iter().enumerate() {
            if rect.contains(p) {
                return Some(Hit::Agent(i));
            }
        }
        for (i, rect) in self.session_rects.iter().enumerate() {
            if rect.contains(p) {
                return Some(Hit::Session(i));
            }
        }
        None
    }

    fn handle_click(&mut self, hit: Hit, ctx: &mut EventCtx) {
        match hit {
            Hit::Selector(which) => {
                // Toggle this selector's dropdown (clicking the open one closes it).
                let opening = self.open_menu != Some(which);
                self.open_menu = if opening { Some(which) } else { None };
                // Lazily fetch the model list the first time the model menu is opened.
                if opening && which == 1 {
                    self.maybe_fetch_models();
                }
                ctx.request_arrange();
                ctx.request_paint();
                self.ensure_pump(ctx);
                return;
            }
            Hit::MenuItem(index) => {
                if let Some(which) = self.open_menu {
                    self.apply_selector_choice(which, index, ctx);
                }
                return;
            }
            _ => {}
        }

        {
            let mut state = self.shared.chat_mut();
            match hit {
                Hit::Input => {
                    state.focused = true;
                    ctx.request_focus();
                }
                Hit::Send => {
                    drop(state);
                    self.shared.chat_mut().submit();
                }
                Hit::NewChat => {
                    state.handle.send_command(ChatCommand::NewSession);
                }
                Hit::Agent(i) => {
                    if let Some(agent) = state.agents.get(i) {
                        let key = agent.key.clone();
                        state.handle.send_command(ChatCommand::SetAgent(key));
                    }
                }
                Hit::Session(i) => {
                    state.open_session(i);
                }
                Hit::Selector(_) | Hit::MenuItem(_) => {}
            }
        }
        ctx.request_measure();
        ctx.request_arrange();
        ctx.request_paint();
        self.ensure_pump(ctx);
    }

    /// Apply a dropdown choice: set the selected index, push the new provider/model to
    /// the chat worker, and close the menu. (`which`: 0 = provider, 1 = model, 2 = effort.)
    fn apply_selector_choice(&mut self, which: usize, index: usize, ctx: &mut EventCtx) {
        {
            let mut state = self.shared.chat_mut();
            match which {
                0 => {
                    if index < state.providers.len() {
                        state.provider_idx = index;
                        state.model_idx = 0;
                    }
                }
                1 => {
                    let len = state
                        .current_provider()
                        .map(|p| p.models.len())
                        .unwrap_or(0);
                    if index < len {
                        state.model_idx = index;
                    }
                }
                _ => {
                    if index < state.efforts.len() {
                        state.effort_idx = index;
                    }
                }
            }
            let provider = state
                .current_provider()
                .map(|p| p.id.clone())
                .unwrap_or_default();
            let model = state.current_model();
            if !provider.is_empty() {
                state.handle.send_command(ChatCommand::SetModel {
                    provider,
                    model,
                    account: "default".to_string(),
                });
            }
        }
        self.open_menu = None;
        ctx.request_measure();
        ctx.request_arrange();
        ctx.request_paint();
        self.ensure_pump(ctx);
    }

    /// Ask the worker to fetch the current provider's models from sinomo-lm, unless they
    /// are already loaded or a fetch is in flight. Results arrive via `ChatEvent::Models`.
    fn maybe_fetch_models(&mut self) {
        let mut state = self.shared.chat_mut();
        if state.models_loading {
            return;
        }
        let needs_fetch = state
            .current_provider()
            .map(|p| p.models.is_empty())
            .unwrap_or(false);
        if !needs_fetch {
            return;
        }
        if let Some(provider) = state.current_provider().map(|p| p.id.clone()) {
            state.models_loading = true;
            state
                .handle
                .send_command(ChatCommand::FetchModels { provider });
        }
    }

    fn handle_key(&mut self, key: &sui::KeyboardEvent, ctx: &mut EventCtx) {
        let mut state = self.shared.chat_mut();
        if !state.focused {
            return;
        }
        let ctrl = key.modifiers.control || key.modifiers.meta;
        match key.key.as_str() {
            "Enter" | "Return" => {
                if key.modifiers.shift {
                    let c = state.cursor;
                    state.input.insert(c, '\n');
                    state.cursor += 1;
                } else {
                    drop(state);
                    self.shared.chat_mut().submit();
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                    self.ensure_pump(ctx);
                    return;
                }
            }
            "Backspace" => {
                let c = state.cursor;
                if c > 0 {
                    let prev = widgets::prev_char_boundary(&state.input, c);
                    state.input.replace_range(prev..c, "");
                    state.cursor = prev;
                }
            }
            "Delete" => {
                let c = state.cursor;
                if c < state.input.len() {
                    let next = widgets::next_char_boundary(&state.input, c);
                    state.input.replace_range(c..next, "");
                }
            }
            "ArrowLeft" => {
                state.cursor = widgets::prev_char_boundary(&state.input, state.cursor);
            }
            "ArrowRight" => {
                state.cursor = widgets::next_char_boundary(&state.input, state.cursor);
            }
            "Home" => state.cursor = 0,
            "End" => state.cursor = state.input.len(),
            _ => {
                if ctrl {
                    return;
                }
                if let Some(text) = &key.text
                    && !text.is_empty()
                    && !text.chars().any(|c| c.is_control())
                {
                    let c = state.cursor;
                    state.input.insert_str(c, text);
                    state.cursor += text.len();
                }
            }
        }
        ctx.request_paint();
    }

    fn pump(&mut self, ctx: &mut EventCtx) {
        self.pump_alive = false;
        let events = self.shared.chat().handle.drain();
        let had = !events.is_empty();
        if had {
            let mut state = self.shared.chat_mut();
            for event in events {
                state.apply_event(event);
            }
        }
        let streaming = self.shared.chat().streaming;
        if had {
            self.idle_frames = 0;
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        } else {
            self.idle_frames = self.idle_frames.saturating_add(1);
        }
        // Keep pumping while a reply streams or there was recent activity; then
        // go idle so we don't repaint forever.
        if streaming || self.idle_frames < 90 {
            ctx.request_animation_frame();
            self.pump_alive = true;
        }
    }
}

impl Widget for ChatView {
    fn accepts_focus(&self) -> bool {
        true
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let theme = self.shared.theme();
        let style = sui::TextStyle {
            font_size: 12.5,
            weight: FontWeight::MEDIUM,
            ..theme.body_text_style()
        };
        let state = self.shared.chat();
        let labels = [
            state
                .current_provider()
                .map(|p| p.name.clone())
                .unwrap_or_else(|| "Provider".to_string()),
            state.current_model(),
            state.current_effort(),
        ];
        drop(state);
        for (i, label) in labels.iter().enumerate() {
            let w = ctx
                .layout()
                .measure_text(format!("◇ {label}"), style.clone())
                .map(|m| m.width + 38.0) // glyph + label + trailing caret
                .unwrap_or(120.0);
            self.sel_w[i] = w.clamp(90.0, 280.0);
        }
        constraints.max
    }

    fn arrange(&mut self, _ctx: &mut ArrangeCtx, bounds: Rect) {
        self.sidebar_rect = Rect::new(bounds.x(), bounds.y(), SIDEBAR_W, bounds.height());
        let panel_x = bounds.x() + SIDEBAR_W;
        let panel_w = bounds.width() - SIDEBAR_W;

        self.composer_rect = Rect::new(
            panel_x,
            bounds.y() + bounds.height() - COMPOSER_H,
            panel_w,
            COMPOSER_H,
        );
        self.messages_rect = Rect::new(
            panel_x,
            bounds.y(),
            panel_w,
            (bounds.height() - COMPOSER_H).max(0.0),
        );

        // Composer internals.
        let cx = self.composer_rect.x() + PAD;
        let cw = self.composer_rect.width() - PAD * 2.0;
        let sel_y = self.composer_rect.y() + 14.0;

        // The reasoning-effort selector only shows for models that actually take one.
        self.effort_visible = {
            let state = self.shared.chat();
            let provider = state
                .current_provider()
                .map(|p| p.id.clone())
                .unwrap_or_default();
            let model = state.current_model();
            !provider.is_empty() && sinomo_lm::model_supports_reasoning_effort(&provider, &model)
        };
        let selector_count = if self.effort_visible { 3 } else { 2 };
        let mut x = cx;
        for i in 0..3 {
            if i < selector_count {
                self.selector_rects[i] = Rect::new(x, sel_y, self.sel_w[i], SELECTOR_H);
                x += self.sel_w[i] + 8.0;
            } else {
                self.selector_rects[i] = Rect::ZERO;
            }
        }
        let input_y = sel_y + SELECTOR_H + 12.0;
        let input_h =
            (self.composer_rect.y() + self.composer_rect.height() - 14.0 - input_y).max(0.0);
        let send_w = 84.0;
        self.input_rect = Rect::new(cx, input_y, (cw - send_w - 10.0).max(40.0), input_h);
        self.send_rect = Rect::new(cx + cw - send_w, input_y + input_h - 40.0, send_w, 40.0);

        // Sidebar internals.
        let agents_n = self.shared.chat().agents.len();
        let sessions_n = self.shared.chat().sessions.len();
        let sx = self.sidebar_rect.x() + 12.0;
        let sw = SIDEBAR_W - 24.0;
        let mut y = self.sidebar_rect.y() + 52.0; // below "Agents" header
        self.agent_rects = (0..agents_n)
            .map(|_| {
                let r = Rect::new(sx, y, sw, ROW_H);
                y += ROW_H + 2.0;
                r
            })
            .collect();
        y += 40.0; // gap + "Sessions" header
        self.newchat_rect = Rect::new(sx, y, sw, ROW_H);
        y += ROW_H + 6.0;
        self.session_rects = (0..sessions_n)
            .map(|_| {
                let r = Rect::new(sx, y, sw, ROW_H);
                y += ROW_H + 2.0;
                r
            })
            .collect();

        // Open-dropdown geometry. The composer sits at the bottom of the tab, so the
        // selector dropdowns open upward over the transcript.
        self.menu_item_rects.clear();
        self.menu_rect = Rect::ZERO;
        if let Some(which) = self.open_menu {
            let count = {
                let state = self.shared.chat();
                match which {
                    0 => state.providers.len(),
                    // Always at least one row, for the "Loading…"/"No models" placeholder.
                    1 => state
                        .current_provider()
                        .map(|p| p.models.len())
                        .unwrap_or(0)
                        .max(1),
                    _ => state.efforts.len(),
                }
            };
            if count > 0 {
                let anchor = self.selector_rects[which];
                let item_h = 28.0;
                let menu_h = count as f32 * item_h + 8.0;
                let menu_w = anchor.width().max(170.0);
                let menu_x = anchor.x();
                let menu_y = (anchor.y() - menu_h - 6.0).max(bounds.y() + 8.0);
                self.menu_rect = Rect::new(menu_x, menu_y, menu_w, menu_h);
                let mut iy = menu_y + 4.0;
                for _ in 0..count {
                    self.menu_item_rects
                        .push(Rect::new(menu_x + 4.0, iy, menu_w - 8.0, item_h));
                    iy += item_h;
                }
            }
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        ctx.fill_bounds(pal.window);
        self.paint_sidebar(ctx, &theme, &pal);
        self.paint_messages(ctx, &theme, &pal);
        self.paint_composer(ctx, &theme, &pal);
        // Painted last so an open selector dropdown floats above the transcript.
        self.paint_dropdown(ctx, &theme, &pal);
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if let Event::Wake(sui::WakeEvent::AnimationFrame { .. }) = event {
            self.pump(ctx);
            return;
        }
        if ctx.phase() != EventPhase::Target {
            return;
        }
        match event {
            Event::Pointer(p) => match p.kind {
                PointerEventKind::Move => {
                    let h = self.hit_test(p.position);
                    if h != self.hovered {
                        self.hovered = h;
                        ctx.request_paint();
                    }
                }
                PointerEventKind::Down if p.button == Some(PointerButton::Primary) => {
                    let hit = self.hit_test(p.position);
                    // A click outside an open dropdown (and not on a selector chip)
                    // dismisses it without triggering the underlying control.
                    if self.open_menu.is_some()
                        && !matches!(hit, Some(Hit::MenuItem(_)) | Some(Hit::Selector(_)))
                    {
                        self.open_menu = None;
                        ctx.set_handled();
                        ctx.request_paint();
                        self.ensure_pump(ctx);
                        return;
                    }
                    if !self.composer_rect.contains(p.position)
                        && !self.input_rect.contains(p.position)
                    {
                        self.shared.chat_mut().focused = false;
                    }
                    if let Some(hit) = hit {
                        self.handle_click(hit, ctx);
                        ctx.set_handled();
                    } else if self.messages_rect.contains(p.position) {
                        self.shared.chat_mut().focused = false;
                        ctx.request_paint();
                    }
                    self.ensure_pump(ctx);
                }
                PointerEventKind::Scroll => {
                    if self.messages_rect.contains(p.position)
                        && let Some(delta) = p.scroll_delta
                    {
                        let dy = match delta {
                            ScrollDelta::Lines(v) => v.y * 48.0,
                            ScrollDelta::Pixels(v) => v.y,
                        };
                        let max = (self.content_height.get() - self.viewport_height.get()).max(0.0);
                        let mut state = self.shared.chat_mut();
                        state.scroll = (state.scroll - dy).clamp(0.0, max);
                        state.stick_bottom = state.scroll >= max - 2.0;
                        ctx.request_paint();
                    }
                }
                _ => {}
            },
            Event::Keyboard(key) if key.state == KeyState::Pressed => {
                if key.key == "Escape" && self.open_menu.take().is_some() {
                    ctx.set_handled();
                    ctx.request_paint();
                    self.ensure_pump(ctx);
                    return;
                }
                self.handle_key(key, ctx);
                self.ensure_pump(ctx);
            }
            _ => {}
        }
    }
}

impl ChatView {
    fn paint_sidebar(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        ctx.fill_rect(self.sidebar_rect, pal.sidebar);
        widgets::hairline_right(ctx, self.sidebar_rect, pal.border);
        let state = self.shared.chat();

        let header = pal.text_style(theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                self.sidebar_rect.x() + 14.0,
                self.sidebar_rect.y() + 24.0,
                200.0,
                20.0,
            ),
            "AGENTS",
            header.clone(),
        );

        for (i, rect) in self.agent_rects.iter().enumerate() {
            let agent = &state.agents[i];
            let active = agent.name == state.info.agent || agent.key == state.info.agent;
            self.paint_row(
                ctx,
                theme,
                pal,
                *rect,
                &agent.name,
                active,
                self.hovered == Some(Hit::Agent(i)),
                Some("✸"),
            );
        }

        // Sessions header (just above new-chat).
        let sessions_header_y = self.newchat_rect.y() - 28.0;
        widgets::draw_text_in(
            ctx,
            Rect::new(self.sidebar_rect.x() + 14.0, sessions_header_y, 200.0, 20.0),
            "SESSIONS",
            header,
        );
        self.paint_row(
            ctx,
            theme,
            pal,
            self.newchat_rect,
            "New chat",
            false,
            self.hovered == Some(Hit::NewChat),
            Some("+"),
        );
        for (i, rect) in self.session_rects.iter().enumerate() {
            let title = &state.sessions[i].title;
            self.paint_row(
                ctx,
                theme,
                pal,
                *rect,
                title,
                false,
                self.hovered == Some(Hit::Session(i)),
                Some("◷"),
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn paint_row(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        rect: Rect,
        label: &str,
        active: bool,
        hovered: bool,
        glyph: Option<&str>,
    ) {
        if active {
            widgets::fill_round_rect(ctx, rect, pal.selected, 8.0);
        } else if hovered {
            widgets::fill_round_rect(ctx, rect, pal.hover, 8.0);
        }
        let color = if active { pal.text } else { pal.text_muted };
        let mut tx = rect.x() + 12.0;
        if let Some(g) = glyph {
            let gs = pal.text_style(
                theme,
                12.0,
                FontWeight::MEDIUM,
                if active { pal.accent } else { pal.text_faint },
            );
            widgets::draw_text_in(ctx, Rect::new(tx, rect.y(), 18.0, rect.height()), g, gs);
            tx += 22.0;
        }
        let style = pal.text_style(
            theme,
            13.0,
            if active {
                FontWeight::SEMIBOLD
            } else {
                FontWeight::MEDIUM
            },
            color,
        );
        let elided = elide(label, 26);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                tx,
                rect.y(),
                rect.x() + rect.width() - tx - 8.0,
                rect.height(),
            ),
            &elided,
            style,
        );
    }

    fn paint_messages(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        let area = self.messages_rect;
        self.viewport_height.set(area.height());
        ctx.push_clip_rect(area);

        let state = self.shared.chat();
        if state.messages.is_empty() {
            ctx.pop_clip();
            self.paint_empty_state(ctx, theme, pal, area, &state);
            return;
        }

        let max_bubble = (area.width() * 0.74).clamp(200.0, 620.0);
        let inner_w = max_bubble - 24.0;
        let body = pal.text_style(theme, 14.0, FontWeight::NORMAL, pal.text);
        let line_h = body.line_height;

        // Measure pass: wrap each message and compute its bubble box. Done first
        // so the immutable `measure` borrow of `ctx` is released before drawing.
        struct Laid {
            is_user: bool,
            lines: Vec<String>,
            bubble_w: f32,
            bubble_h: f32,
        }
        let mut laid: Vec<Laid> = Vec::with_capacity(state.messages.len());
        let mut total = PAD;
        {
            let measure = |s: &str| {
                ctx.measure_text(s, body.clone())
                    .map(|m| m.width)
                    .unwrap_or(0.0)
            };
            for msg in &state.messages {
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
        let scroll = if state.stick_bottom {
            max_scroll
        } else {
            state.scroll.clamp(0.0, max_scroll)
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

        // streaming/typing notice
        if state.streaming
            && let Some(notice) = &state.notice
        {
            let ns = pal.text_style(theme, 11.5, FontWeight::MEDIUM, pal.text_faint);
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

    fn paint_empty_state(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        area: Rect,
        state: &ChatState,
    ) {
        let cx = area.x() + area.width() / 2.0;
        let cy = area.y() + area.height() / 2.0;
        let mark = pal.text_style(theme, 34.0, FontWeight::BOLD, pal.text_faint);
        widgets::draw_centered(ctx, "✦", mark, cx, cy - 40.0);
        let title = pal.text_style(theme, 17.0, FontWeight::SEMIBOLD, pal.text_muted);
        let agent = if state.info.agent.is_empty() {
            "Sinomo"
        } else {
            state.info.agent.as_str()
        };
        widgets::draw_centered(ctx, &format!("Chat with {agent}"), title, cx, cy + 2.0);
        let sub = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text_faint);
        widgets::draw_centered(ctx, "Type a message below to begin.", sub, cx, cy + 26.0);
    }

    fn paint_composer(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        ctx.fill_rect(self.composer_rect, pal.titlebar);
        widgets::hairline_top(ctx, self.composer_rect, pal.border);
        let state = self.shared.chat();

        let labels = [
            state
                .current_provider()
                .map(|p| p.name.clone())
                .unwrap_or_else(|| "Provider".into()),
            state.current_model(),
            state.current_effort(),
        ];
        let glyphs = ["◇", "◆", "✲"];
        for (i, rect) in self.selector_rects.iter().enumerate() {
            if i == 2 && !self.effort_visible {
                continue; // effort selector hidden for non-reasoning models
            }
            let active = self.hovered == Some(Hit::Selector(i)) || self.open_menu == Some(i);
            let border = if active { pal.accent } else { pal.border };
            widgets::round_panel(ctx, *rect, pal.field, border, SELECTOR_H / 2.0);
            let gs = pal.text_style(theme, 11.0, FontWeight::MEDIUM, pal.accent);
            widgets::draw_text_in(
                ctx,
                Rect::new(rect.x() + 10.0, rect.y(), 16.0, rect.height()),
                glyphs[i],
                gs,
            );
            let ts = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    rect.x() + 26.0,
                    rect.y(),
                    (rect.width() - 44.0).max(10.0),
                    rect.height(),
                ),
                &elide(&labels[i], 20),
                ts,
            );
            // Trailing caret signalling the dropdown.
            let caret = pal.text_style(theme, 9.0, FontWeight::SEMIBOLD, pal.text_muted);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    rect.x() + rect.width() - 18.0,
                    rect.y(),
                    14.0,
                    rect.height(),
                ),
                "▾",
                caret,
            );
        }

        // Input field.
        let focused = state.focused;
        let border_color = if focused { pal.accent } else { pal.border };
        widgets::round_panel(ctx, self.input_rect, pal.field, border_color, 10.0);
        let body = pal.text_style(theme, 13.5, FontWeight::NORMAL, pal.text);
        let pad = 10.0;
        let text_area = Rect::new(
            self.input_rect.x() + pad,
            self.input_rect.y() + pad,
            (self.input_rect.width() - pad * 2.0).max(20.0),
            (self.input_rect.height() - pad * 2.0).max(16.0),
        );
        ctx.push_clip_rect(self.input_rect);
        if state.input.is_empty() && !focused {
            let ph = pal.text_style(theme, 13.5, FontWeight::NORMAL, pal.text_faint);
            widgets::draw_text_in(ctx, text_area, "Message Sinomo…  (⏎ to send)", ph);
        } else {
            // Measure pass (releases the immutable ctx borrow before drawing).
            let (lines, caret_x) = {
                let measure = |s: &str| {
                    ctx.measure_text(s, body.clone())
                        .map(|m| m.width)
                        .unwrap_or(0.0)
                };
                let lines = widgets::wrap_lines(&state.input, text_area.width(), measure);
                let caret_x =
                    text_area.x() + measure(lines.last().map(String::as_str).unwrap_or(""));
                (lines, caret_x)
            };
            let mut ty = text_area.y();
            for line in &lines {
                ctx.draw_text(
                    Rect::new(text_area.x(), ty, text_area.width(), body.line_height + 2.0),
                    line,
                    body.clone(),
                );
                ty += body.line_height;
            }
            if focused {
                let caret_y =
                    text_area.y() + lines.len().saturating_sub(1) as f32 * body.line_height;
                ctx.fill_rect(
                    Rect::new(caret_x + 1.0, caret_y, 1.5, body.line_height),
                    pal.accent,
                );
            }
        }
        ctx.pop_clip();

        // Send button.
        let send_hover = self.hovered == Some(Hit::Send);
        let enabled = !state.input.trim().is_empty() && !state.streaming;
        let send_color = if !enabled {
            pal.field
        } else if send_hover {
            pal.accent_hover
        } else {
            pal.accent
        };
        widgets::fill_round_rect(ctx, self.send_rect, send_color, 10.0);
        let label_color = if enabled {
            pal.on_accent
        } else {
            pal.text_faint
        };
        let ls = pal.text_style(theme, 13.0, FontWeight::SEMIBOLD, label_color);
        let label = if state.streaming { "•••" } else { "Send" };
        let lw = ctx
            .measure_text(label, ls.clone())
            .map(|m| m.width)
            .unwrap_or(30.0);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                self.send_rect.x() + (self.send_rect.width() - lw) / 2.0,
                self.send_rect.y(),
                lw + 4.0,
                self.send_rect.height(),
            ),
            label,
            ls,
        );
    }

    /// Paint the open selector dropdown (a panel of options anchored above its chip).
    fn paint_dropdown(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        let Some(which) = self.open_menu else {
            return;
        };
        if self.menu_item_rects.is_empty() {
            return;
        }

        let state = self.shared.chat();
        let loading = state.models_loading;
        let (options, selected): (Vec<String>, usize) = match which {
            0 => (
                state.providers.iter().map(|p| p.name.clone()).collect(),
                state.provider_idx,
            ),
            1 => (
                state
                    .current_provider()
                    .map(|p| p.models.clone())
                    .unwrap_or_default(),
                state.model_idx,
            ),
            _ => (state.efforts.clone(), state.effort_idx),
        };
        drop(state);

        widgets::round_panel(ctx, self.menu_rect, pal.panel, pal.border, 10.0);

        // Model menu: a placeholder row while fetching, or when nothing is available.
        if which == 1 && options.is_empty() {
            if let Some(rect) = self.menu_item_rects.first() {
                let label = if loading {
                    "Loading models…"
                } else {
                    "No models available"
                };
                let style = pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text_faint);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(
                        rect.x() + 10.0,
                        rect.y(),
                        (rect.width() - 20.0).max(10.0),
                        rect.height(),
                    ),
                    label,
                    style,
                );
            }
            return;
        }

        let item_style = pal.text_style(theme, 12.5, FontWeight::MEDIUM, pal.text);
        for (i, rect) in self.menu_item_rects.iter().enumerate() {
            let Some(label) = options.get(i) else {
                continue;
            };
            let is_selected = i == selected;
            let hovered = self.hovered == Some(Hit::MenuItem(i));
            if is_selected {
                widgets::fill_round_rect(ctx, *rect, pal.selected, 7.0);
            } else if hovered {
                widgets::fill_round_rect(ctx, *rect, pal.hover, 7.0);
            }
            let color = if is_selected {
                pal.text
            } else {
                pal.text_muted
            };
            let style = sui::TextStyle {
                color,
                ..item_style.clone()
            };
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    rect.x() + 10.0,
                    rect.y(),
                    (rect.width() - 32.0).max(10.0),
                    rect.height(),
                ),
                &elide(label, 30),
                style,
            );
            if is_selected {
                let check = pal.text_style(theme, 11.5, FontWeight::SEMIBOLD, pal.accent);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(
                        rect.x() + rect.width() - 20.0,
                        rect.y(),
                        14.0,
                        rect.height(),
                    ),
                    "✓",
                    check,
                );
            }
        }
    }
}

fn elide(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        text.to_string()
    } else {
        let mut s: String = chars[..max_chars.saturating_sub(1)].iter().collect();
        s.push('…');
        s
    }
}

/// Build the chat tab content.
pub fn view(shared: Shared) -> impl Widget {
    ChatView::new(shared)
}
