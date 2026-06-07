//! "Settings" tab — a nested, responsive settings page.
//!
//! Settings are grouped into top-level [`SettingsSection`]s. When the tab is wide
//! enough, the section menu stays pinned in a left sidebar (the "previous level")
//! next to the selected section's detail — a master/detail layout. When it's
//! narrow, it collapses to a single column: the menu, which you drill into to
//! reach a detail page with a back affordance.

use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use sui::{
    Constraints, Event, EventCtx, EventPhase, FontWeight, KeyState, MeasureCtx, PaintCtx,
    PointerButton, PointerEventKind, Rect, ScrollDelta, Size, Widget,
};

use sinomo_runtime::config::ConfigClient;

use crate::backend::Backend;
use crate::backend::config::{
    ConfigEditKind, ConfigItem, ProviderForm, cycle_provider_reasoning_effort, provider_forms,
    resolve_items, run_provider_device_login, set_bool, set_provider_api_key, set_provider_base_url,
    set_provider_enabled, set_provider_model, set_value,
};
use crate::state::Shared;
use crate::theme::{AppThemeMode, Palette};
use crate::ui::widgets;

const ROW_H: f32 = 46.0;
const CARD_W: f32 = 720.0;
const PAD: f32 = 22.0;
/// Below this content width the tab collapses to a single drill-down column.
const WIDE_THRESHOLD: f32 = 700.0;
const SIDEBAR_W: f32 = 256.0;
const SIDEBAR_ITEM_H: f32 = 40.0;
const MENU_ROW_H: f32 = 58.0;

/// A top-level settings destination. Each renders its own detail page.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SettingsSection {
    Appearance,
    Connection,
    Node,
    Cluster,
    Providers,
    Configuration,
    About,
}

impl SettingsSection {
    pub const ALL: [SettingsSection; 7] = [
        SettingsSection::Appearance,
        SettingsSection::Connection,
        SettingsSection::Node,
        SettingsSection::Cluster,
        SettingsSection::Providers,
        SettingsSection::Configuration,
        SettingsSection::About,
    ];

    pub fn title(self) -> &'static str {
        match self {
            SettingsSection::Appearance => "Appearance",
            SettingsSection::Connection => "Connection",
            SettingsSection::Node => "Node",
            SettingsSection::Cluster => "Cluster",
            SettingsSection::Providers => "Providers",
            SettingsSection::Configuration => "Advanced",
            SettingsSection::About => "About",
        }
    }
}

/// State backing the settings tab.
pub struct SettingsState {
    /// The config service the tab reads/writes through (no local config state).
    pub config: Arc<dyn ConfigClient>,
    pub config_items: Vec<ConfigItem>,
    pub config_error: Option<String>,
    pub provider_forms: Vec<ProviderForm>,
    pub mode_label: String,
    pub status_detail: String,
    pub node_id: String,
    pub role: String,
    pub clustered: bool,
    pub cluster_summary: Option<String>,
    pub version: String,
    /// The open section. `None` means "at the menu level" (only meaningful when
    /// the layout is narrow; when wide we always show a section).
    pub selected: Option<SettingsSection>,
    pub scroll: f32,
    /// The active inline config editor in the Advanced section, if any.
    pub editing: Option<ConfigEdit>,
    /// An in-progress provider device-login flow (Providers section), if any.
    pub device_login: Option<DeviceLogin>,
}

/// An in-progress inline text edit — either a raw config key (Advanced) or a provider field.
pub struct ConfigEdit {
    pub target: EditTarget,
    pub draft: String,
    pub cursor: usize,
    /// Set when a commit failed (e.g. invalid value); shown inline, editing stays open.
    pub error: Option<String>,
}

/// What an inline edit writes to on commit.
#[derive(Clone)]
pub enum EditTarget {
    /// A raw config key in the Advanced section.
    ConfigKey { key: String, kind: ConfigEditKind },
    /// A field of a provider account (or its credential) in the Providers section.
    Provider {
        provider: String,
        field: ProviderField,
    },
}

/// A text-editable field of a provider in the Providers section.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProviderField {
    ApiKey,
    Model,
    BaseUrl,
}

impl ConfigEdit {
    fn targets_config_key(&self, key: &str) -> bool {
        matches!(&self.target, EditTarget::ConfigKey { key: k, .. } if k == key)
    }

    fn targets_provider(&self, provider: &str, field: ProviderField) -> bool {
        matches!(
            &self.target,
            EditTarget::Provider { provider: p, field: f } if p == provider && *f == field
        )
    }
}

/// An in-progress provider OAuth device-login flow (codex / github-copilot), driven on a
/// background thread and drained when the UI is woken.
pub struct DeviceLogin {
    pub provider: String,
    pub status: DeviceLoginStatus,
    rx: Receiver<DeviceLoginEvent>,
}

#[derive(Clone)]
pub enum DeviceLoginStatus {
    /// Requesting a device code.
    Starting,
    /// The user must enter `user_code` at `verification_uri`.
    Prompt {
        verification_uri: String,
        user_code: String,
    },
    /// Authorized and credentials saved.
    Done,
    /// The flow failed (expired, denied, network).
    Failed(String),
}

/// Messages from the device-login worker thread.
enum DeviceLoginEvent {
    Prompt {
        verification_uri: String,
        user_code: String,
    },
    Done,
    Failed(String),
}

/// What a key press in the inline editor asks the view to do next.
enum EditOutcome {
    Continue,
    Commit,
    Cancel,
}

impl SettingsState {
    pub fn new(backend: &Backend) -> Self {
        let config = backend.config.clone();
        let (config_items, config_error) = match config.catalog(None) {
            Ok(_) => (resolve_items(config.as_ref()), None),
            Err(error) => (Vec::new(), Some(error.to_string())),
        };
        let provider_forms = provider_forms(config.as_ref());

        let cluster_summary = match &backend.connection {
            crate::backend::Connection::Daemon(status)
            | crate::backend::Connection::InProcessDaemon(status) => {
                status.cluster.as_ref().map(|cluster| {
                    format!(
                        "cluster {} · {:?}",
                        short(&format!("{:?}", cluster.cluster_id), 14),
                        cluster.member_state
                    )
                })
            }
            crate::backend::Connection::LocalFallback => None,
        };

        Self {
            config,
            config_items,
            config_error,
            provider_forms,
            mode_label: backend.mode_label(),
            status_detail: backend.status_detail(),
            node_id: backend.node.node_id.to_string(),
            role: format!("{:?}", backend.node.role).to_lowercase(),
            clustered: backend.connection.is_clustered(),
            cluster_summary,
            version: env!("CARGO_PKG_VERSION").to_string(),
            selected: None,
            scroll: 0.0,
            editing: None,
            device_login: None,
        }
    }

    /// Open a section's detail page, resetting the scroll offset.
    fn open(&mut self, section: SettingsSection) {
        self.selected = Some(section);
        self.scroll = 0.0;
    }

    /// Return to the menu level (narrow layout), resetting the scroll offset.
    fn back_to_menu(&mut self) {
        self.selected = None;
        self.scroll = 0.0;
    }

    fn toggle_config(&mut self, index: usize) {
        let Some(item) = self.config_items.get(index) else {
            return;
        };
        let Some(current) = item.bool_value else {
            return;
        };
        let key = item.key.clone();
        if set_bool(self.config.as_ref(), &key, !current).is_ok() {
            self.reload();
        }
    }

    /// Reload config + provider state from the service (after a write).
    fn reload(&mut self) {
        self.config_items = resolve_items(self.config.as_ref());
        self.provider_forms = provider_forms(self.config.as_ref());
    }

    /// Start editing the value of the config item at `index` (Advanced). No-op for booleans (they
    /// use the toggle) and for the item already being edited.
    fn begin_config_edit(&mut self, index: usize) {
        let Some(item) = self.config_items.get(index) else {
            return;
        };
        if item.edit_kind == ConfigEditKind::Bool {
            return;
        }
        if self
            .editing
            .as_ref()
            .is_some_and(|e| e.targets_config_key(&item.key))
        {
            return;
        }
        let draft = item.edit_value.clone();
        let cursor = draft.len();
        self.editing = Some(ConfigEdit {
            target: EditTarget::ConfigKey {
                key: item.key.clone(),
                kind: item.edit_kind,
            },
            draft,
            cursor,
            error: None,
        });
    }

    /// Start editing a provider field (Providers). The API key starts empty (the stored key is
    /// never surfaced); model / base URL seed from the current value.
    fn begin_provider_field_edit(&mut self, index: usize, field: ProviderField) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        let provider = form.provider.clone();
        if self
            .editing
            .as_ref()
            .is_some_and(|e| e.targets_provider(&provider, field))
        {
            return;
        }
        let draft = match field {
            ProviderField::ApiKey => String::new(),
            ProviderField::Model => form.default_model.clone(),
            ProviderField::BaseUrl => form.api_base_url.clone(),
        };
        let cursor = draft.len();
        self.editing = Some(ConfigEdit {
            target: EditTarget::Provider { provider, field },
            draft,
            cursor,
            error: None,
        });
    }

    fn cancel_edit(&mut self) {
        self.editing = None;
    }

    /// Commit the active edit to its target. On success reloads; on error keeps the editor open
    /// with the message shown inline.
    fn commit_edit(&mut self) {
        let Some(edit) = self.editing.take() else {
            return;
        };
        let result = match &edit.target {
            EditTarget::ConfigKey { key, kind } => {
                set_value(self.config.as_ref(), key, *kind, &edit.draft)
            }
            EditTarget::Provider { provider, field } => match field {
                ProviderField::ApiKey => {
                    set_provider_api_key(self.config.as_ref(), provider, &edit.draft)
                }
                ProviderField::Model => {
                    set_provider_model(self.config.as_ref(), provider, &edit.draft)
                }
                ProviderField::BaseUrl => {
                    set_provider_base_url(self.config.as_ref(), provider, &edit.draft)
                }
            },
        };
        match result {
            Ok(()) => self.reload(),
            Err(error) => {
                self.editing = Some(ConfigEdit {
                    error: Some(error),
                    ..edit
                });
            }
        }
    }

    fn toggle_provider(&mut self, index: usize) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        let provider = form.provider.clone();
        let enabled = !form.enabled;
        if set_provider_enabled(self.config.as_ref(), &provider, enabled).is_ok() {
            self.reload();
        }
    }

    fn cycle_provider_effort(&mut self, index: usize) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        let provider = form.provider.clone();
        if cycle_provider_reasoning_effort(self.config.as_ref(), &provider).is_ok() {
            self.reload();
        }
    }

    /// Start a provider's OAuth device-login flow on a background thread. Progress + the result
    /// arrive via a channel drained on the UI wake (`pump_device_login`); `waker` lets the worker
    /// refresh the UI as it proceeds.
    fn begin_device_login(&mut self, index: usize, waker: Option<sui::Waker>) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        if !form.uses_device_login {
            return;
        }
        let provider = form.provider.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker_provider = provider.clone();
        let spawned = std::thread::Builder::new()
            .name("sinomo-app-device-login".to_string())
            .spawn(move || {
                let result =
                    run_provider_device_login(&worker_provider, |verification_uri, user_code| {
                        let _ = tx.send(DeviceLoginEvent::Prompt {
                            verification_uri,
                            user_code,
                        });
                        if let Some(waker) = &waker {
                            waker.wake();
                        }
                    });
                let _ = tx.send(match result {
                    Ok(()) => DeviceLoginEvent::Done,
                    Err(error) => DeviceLoginEvent::Failed(error),
                });
                if let Some(waker) = &waker {
                    waker.wake();
                }
            })
            .is_ok();
        if spawned {
            self.device_login = Some(DeviceLogin {
                provider,
                status: DeviceLoginStatus::Starting,
                rx,
            });
        }
    }

    /// Drain the device-login worker's events (called by the shell on the UI wake). Returns
    /// whether anything changed. On success, refreshes provider state and clears the flow.
    pub fn pump_device_login(&mut self) -> bool {
        let events: Vec<DeviceLoginEvent> = match self.device_login.as_ref() {
            Some(login) => login.rx.try_iter().collect(),
            None => return false,
        };
        if events.is_empty() {
            return false;
        }

        let mut done = false;
        let mut next_status = None;
        for event in events {
            next_status = Some(match event {
                DeviceLoginEvent::Prompt {
                    verification_uri,
                    user_code,
                } => DeviceLoginStatus::Prompt {
                    verification_uri,
                    user_code,
                },
                DeviceLoginEvent::Done => {
                    done = true;
                    DeviceLoginStatus::Done
                }
                DeviceLoginEvent::Failed(error) => DeviceLoginStatus::Failed(error),
            });
        }
        if let (Some(login), Some(status)) = (self.device_login.as_mut(), next_status) {
            login.status = status;
        }
        if done {
            self.reload();
            self.device_login = None;
        }
        true
    }

    /// Apply a key press to the active editor. Returns whether the view should commit, cancel, or
    /// keep editing.
    fn edit_key(&mut self, key: &sui::KeyboardEvent) -> EditOutcome {
        let Some(edit) = self.editing.as_mut() else {
            return EditOutcome::Continue;
        };
        let ctrl = key.modifiers.control || key.modifiers.meta;
        match key.key.as_str() {
            "Enter" | "Return" => return EditOutcome::Commit,
            "Escape" | "Esc" => return EditOutcome::Cancel,
            "Backspace" => {
                if edit.cursor > 0 {
                    let prev = widgets::prev_char_boundary(&edit.draft, edit.cursor);
                    edit.draft.replace_range(prev..edit.cursor, "");
                    edit.cursor = prev;
                    edit.error = None;
                }
            }
            "Delete" => {
                if edit.cursor < edit.draft.len() {
                    let next = widgets::next_char_boundary(&edit.draft, edit.cursor);
                    edit.draft.replace_range(edit.cursor..next, "");
                    edit.error = None;
                }
            }
            "ArrowLeft" => edit.cursor = widgets::prev_char_boundary(&edit.draft, edit.cursor),
            "ArrowRight" => edit.cursor = widgets::next_char_boundary(&edit.draft, edit.cursor),
            "Home" => edit.cursor = 0,
            "End" => edit.cursor = edit.draft.len(),
            _ => {
                if ctrl {
                    return EditOutcome::Continue;
                }
                if let Some(text) = &key.text
                    && !text.is_empty()
                    && !text.chars().any(|c| c.is_control())
                {
                    let c = edit.cursor;
                    edit.draft.insert_str(c, text);
                    edit.cursor += text.len();
                    edit.error = None;
                }
            }
        }
        EditOutcome::Continue
    }
}

#[derive(Clone, PartialEq)]
enum Hit {
    ThemeMode(AppThemeMode),
    ConfigToggle(usize),
    /// Click a non-boolean config value to edit it inline (Advanced section).
    EditField(usize),
    /// Toggle a provider enabled/disabled (Providers section).
    ProviderToggle(usize),
    /// Edit a provider's text field inline (Providers section).
    ProviderEdit(usize, ProviderField),
    /// Cycle a provider's reasoning effort (Providers section).
    ProviderEffort(usize),
    /// Start a provider's OAuth device-login flow (Providers section).
    ProviderSignIn(usize),
    Open(SettingsSection),
    Back,
}

pub struct SettingsView {
    shared: Shared,
    hit_rects: RefCell<Vec<(Hit, Rect)>>,
    content_height: Cell<f32>,
    viewport_height: Cell<f32>,
}

impl SettingsView {
    pub fn new(shared: Shared) -> Self {
        Self {
            shared,
            hit_rects: RefCell::new(Vec::new()),
            content_height: Cell::new(0.0),
            viewport_height: Cell::new(0.0),
        }
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        (theme, pal)
    }

    /// A short, right-aligned value preview for a section, shown in the menu.
    fn section_preview(&self, section: SettingsSection) -> String {
        let settings = self.shared.settings();
        match section {
            SettingsSection::Appearance => self.shared.borrow().theme_mode.label().to_string(),
            SettingsSection::Connection => settings.mode_label.clone(),
            SettingsSection::Node => settings.role.clone(),
            SettingsSection::Cluster => {
                if settings.clustered {
                    "Connected".to_string()
                } else {
                    "Local".to_string()
                }
            }
            SettingsSection::Providers => {
                let enabled = settings.provider_forms.iter().filter(|f| f.enabled).count();
                format!("{enabled} enabled")
            }
            SettingsSection::Configuration => {
                if settings.config_error.is_some() {
                    "Unavailable".to_string()
                } else {
                    format!("{} keys", settings.config_items.len())
                }
            }
            SettingsSection::About => format!("v{}", settings.version),
        }
    }
}

impl Widget for SettingsView {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.max
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        let bounds = ctx.bounds();
        ctx.fill_bounds(pal.window);
        self.hit_rects.borrow_mut().clear();

        let wide = bounds.width() >= WIDE_THRESHOLD;
        let selected = self.shared.settings().selected;

        if wide {
            // Master/detail: the section menu stays pinned as a sidebar next to
            // the selected section's detail.
            let sidebar = Rect::new(bounds.x(), bounds.y(), SIDEBAR_W, bounds.height());
            let detail = Rect::new(
                bounds.x() + SIDEBAR_W,
                bounds.y(),
                bounds.width() - SIDEBAR_W,
                bounds.height(),
            );
            let active = selected.unwrap_or(SettingsSection::Appearance);
            self.paint_sidebar(ctx, &theme, &pal, sidebar, active);
            self.paint_detail(ctx, &theme, &pal, detail, active, false);
        } else {
            // Single column: the menu, drilled into per section.
            match selected {
                None => self.paint_menu(ctx, &theme, &pal, bounds),
                Some(section) => self.paint_detail(ctx, &theme, &pal, bounds, section, true),
            }
        }
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target {
            return;
        }
        match event {
            // Keyboard drives the inline config editor when one is open.
            Event::Keyboard(key) if key.state == KeyState::Pressed => {
                if self.shared.settings().editing.is_some() {
                    let outcome = self.shared.settings_mut().edit_key(key);
                    match outcome {
                        EditOutcome::Commit => self.shared.settings_mut().commit_edit(),
                        EditOutcome::Cancel => self.shared.settings_mut().cancel_edit(),
                        EditOutcome::Continue => {}
                    }
                    ctx.request_paint();
                    ctx.set_handled();
                }
            }
            Event::Pointer(p) => match p.kind {
                PointerEventKind::Down if p.button == Some(PointerButton::Primary) => {
                    let hit = self
                        .hit_rects
                        .borrow()
                        .iter()
                        .find(|(_, rect)| rect.contains(p.position))
                        .map(|(hit, _)| hit.clone());

                    // Clicking anywhere other than the field being edited commits it first.
                    let clicked_active = hit.as_ref().is_some_and(|h| self.is_active_edit_hit(h));
                    if !clicked_active && self.shared.settings().editing.is_some() {
                        self.shared.settings_mut().commit_edit();
                    }

                    if let Some(hit) = hit {
                        match hit {
                            Hit::ThemeMode(mode) => {
                                self.shared.borrow_mut().theme_mode = mode;
                            }
                            Hit::ConfigToggle(i) => {
                                self.shared.settings_mut().toggle_config(i);
                            }
                            Hit::EditField(i) => {
                                self.shared.settings_mut().begin_config_edit(i);
                                ctx.request_focus();
                            }
                            Hit::ProviderToggle(i) => {
                                self.shared.settings_mut().toggle_provider(i);
                            }
                            Hit::ProviderEdit(i, field) => {
                                self.shared.settings_mut().begin_provider_field_edit(i, field);
                                ctx.request_focus();
                            }
                            Hit::ProviderEffort(i) => {
                                self.shared.settings_mut().cycle_provider_effort(i);
                            }
                            Hit::ProviderSignIn(i) => {
                                let waker = self.shared.borrow().waker.clone();
                                self.shared.settings_mut().begin_device_login(i, waker);
                            }
                            Hit::Open(section) => {
                                self.shared.settings_mut().open(section);
                            }
                            Hit::Back => {
                                self.shared.settings_mut().back_to_menu();
                            }
                        }
                        ctx.set_handled();
                    }
                    ctx.request_paint();
                }
                PointerEventKind::Scroll => {
                    if let Some(delta) = p.scroll_delta {
                        let dy = match delta {
                            ScrollDelta::Lines(v) => v.y * 48.0,
                            ScrollDelta::Pixels(v) => v.y,
                        };
                        let max = (self.content_height.get() - self.viewport_height.get()).max(0.0);
                        let mut settings = self.shared.settings_mut();
                        settings.scroll = (settings.scroll - dy).clamp(0.0, max);
                        ctx.request_paint();
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn accepts_focus(&self) -> bool {
        true
    }
}

impl SettingsView {
    /// Whether `hit` targets the field currently being edited (so clicking it should not
    /// commit-and-close the editor).
    fn is_active_edit_hit(&self, hit: &Hit) -> bool {
        let settings = self.shared.settings();
        let Some(edit) = settings.editing.as_ref() else {
            return false;
        };
        match hit {
            Hit::EditField(i) => settings
                .config_items
                .get(*i)
                .is_some_and(|item| edit.targets_config_key(&item.key)),
            Hit::ProviderEdit(i, field) => settings
                .provider_forms
                .get(*i)
                .is_some_and(|form| edit.targets_provider(&form.provider, *field)),
            _ => false,
        }
    }

    /// Wide layout: the section list, pinned on the left, with `active` highlighted.
    fn paint_sidebar(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        area: Rect,
        active: SettingsSection,
    ) {
        ctx.push_clip_rect(area);
        widgets::hairline_right(ctx, area, pal.border);

        let heading = pal.text_style(theme, 11.5, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(
            ctx,
            Rect::new(area.x() + 18.0, area.y() + 16.0, area.width() - 28.0, 18.0),
            "SETTINGS",
            heading,
        );

        let mut y = area.y() + 44.0;
        for section in SettingsSection::ALL {
            let row = Rect::new(area.x() + 8.0, y, area.width() - 16.0, SIDEBAR_ITEM_H);
            let is_active = section == active;
            if is_active {
                widgets::fill_round_rect(ctx, row, pal.selected, 9.0);
            }
            let color = if is_active { pal.text } else { pal.text_muted };
            let weight = if is_active {
                FontWeight::SEMIBOLD
            } else {
                FontWeight::MEDIUM
            };
            let style = pal.text_style(theme, 13.5, weight, color);
            widgets::draw_text_in(
                ctx,
                Rect::new(row.x() + 12.0, row.y(), row.width() - 24.0, SIDEBAR_ITEM_H),
                section.title(),
                style,
            );
            self.hit_rects.borrow_mut().push((Hit::Open(section), row));
            y += SIDEBAR_ITEM_H + 2.0;
        }
        ctx.pop_clip();
    }

    /// Narrow layout: the full-width menu of sections (title + value + chevron).
    fn paint_menu(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette, bounds: Rect) {
        self.viewport_height.set(bounds.height());
        let card_w = CARD_W.min(bounds.width() - PAD * 2.0).max(0.0);
        let x = bounds.x() + (bounds.width() - card_w) / 2.0;
        let max_scroll = (self.content_height.get() - bounds.height()).max(0.0);
        let scroll = self.shared.settings().scroll.clamp(0.0, max_scroll);

        ctx.push_clip_rect(bounds);
        let mut y = bounds.y() + PAD - scroll;

        let title_style = pal.text_style(theme, 22.0, FontWeight::BOLD, pal.text);
        widgets::draw_text_in(ctx, Rect::new(x + 4.0, y, card_w, 30.0), "Settings", title_style);
        y += 44.0;

        let h = SettingsSection::ALL.len() as f32 * MENU_ROW_H;
        self.card_bg(ctx, pal, x, y, card_w, h);
        let title_style = pal.text_style(theme, 14.5, FontWeight::MEDIUM, pal.text);
        let preview_style = pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_muted);
        let chevron_style = pal.text_style(theme, 18.0, FontWeight::NORMAL, pal.text_faint);

        for (i, section) in SettingsSection::ALL.iter().enumerate() {
            let ry = y + i as f32 * MENU_ROW_H;
            let row = Rect::new(x, ry, card_w, MENU_ROW_H);
            if i > 0 {
                ctx.fill_rect(Rect::new(x + 16.0, ry, card_w - 16.0, 1.0), pal.border);
            }
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 18.0, ry, card_w - 220.0, MENU_ROW_H),
                section.title(),
                title_style.clone(),
            );

            // Trailing chevron, with the value preview to its left.
            let chevron_x = x + card_w - 26.0;
            widgets::draw_text_in(
                ctx,
                Rect::new(chevron_x, ry, 18.0, MENU_ROW_H),
                "›",
                chevron_style.clone(),
            );
            let preview = self.section_preview(*section);
            let pw_max = card_w - 220.0;
            let preview = elide_to(&preview, pw_max, ctx, &preview_style);
            let pw = ctx
                .measure_text(preview.clone(), preview_style.clone())
                .map(|m| m.width)
                .unwrap_or(0.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(chevron_x - 12.0 - pw, ry, pw + 4.0, MENU_ROW_H),
                &preview,
                preview_style.clone(),
            );

            self.hit_rects.borrow_mut().push((Hit::Open(*section), row));
        }
        y += h;
        ctx.pop_clip();

        let content_height = (y + scroll) - bounds.y() + PAD;
        self.content_height.set(content_height);
    }

    /// A section's detail page. `show_back` adds a narrow-layout nav bar with a
    /// back affordance; otherwise (wide) it shows a plain title aligned with the card.
    fn paint_detail(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        area: Rect,
        section: SettingsSection,
        show_back: bool,
    ) {
        let card_w = CARD_W.min(area.width() - PAD * 2.0).max(0.0);
        let x = area.x() + (area.width() - card_w) / 2.0;
        let header_h = if show_back { 52.0 } else { 60.0 };

        // Fixed header (not scrolled).
        if show_back {
            let bar = Rect::new(area.x(), area.y(), area.width(), header_h);
            let back_style = pal.text_style(theme, 14.0, FontWeight::MEDIUM, pal.accent);
            let back_label = "‹  Settings";
            let bw = ctx
                .measure_text(back_label, back_style.clone())
                .map(|m| m.width)
                .unwrap_or(80.0);
            let chip = Rect::new(area.x() + 12.0, area.y(), bw + 16.0, header_h);
            widgets::draw_text_in(ctx, chip, back_label, back_style);
            self.hit_rects.borrow_mut().push((Hit::Back, chip));

            let title_style = pal.text_style(theme, 15.0, FontWeight::SEMIBOLD, pal.text);
            let tw = ctx
                .measure_text(section.title(), title_style.clone())
                .map(|m| m.width)
                .unwrap_or(0.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(bar.x() + (bar.width() - tw) / 2.0, bar.y(), tw + 4.0, header_h),
                section.title(),
                title_style,
            );
            widgets::hairline_bottom(ctx, bar, pal.border);
        } else {
            let title_style = pal.text_style(theme, 22.0, FontWeight::BOLD, pal.text);
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 2.0, area.y() + 18.0, card_w, 30.0),
                section.title(),
                title_style,
            );
        }

        // Scrolled body.
        let body = Rect::new(
            area.x(),
            area.y() + header_h,
            area.width(),
            (area.height() - header_h).max(0.0),
        );
        self.viewport_height.set(body.height());
        let max_scroll = (self.content_height.get() - body.height()).max(0.0);
        let scroll = self.shared.settings().scroll.clamp(0.0, max_scroll);

        ctx.push_clip_rect(body);
        let mut y = body.y() + PAD - scroll;
        y = self.paint_section_body(ctx, theme, pal, section, x, y, card_w);
        ctx.pop_clip();

        let content_height = (y + scroll) - body.y() + PAD;
        self.content_height.set(content_height);
    }

    /// Paint the card(s) for a single section, returning the bottom `y`.
    #[allow(clippy::too_many_arguments)]
    fn paint_section_body(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        section: SettingsSection,
        x: f32,
        y: f32,
        w: f32,
    ) -> f32 {
        let settings = self.shared.settings();
        match section {
            SettingsSection::Appearance => self.theme_row(ctx, theme, pal, x, y, w),
            SettingsSection::Connection => {
                let rows = vec![
                    ("Mode", settings.mode_label.clone()),
                    ("Status", settings.status_detail.clone()),
                ];
                self.value_card(ctx, theme, pal, x, y, w, &rows)
            }
            SettingsSection::Node => {
                let rows = vec![
                    ("Node ID", short(&settings.node_id, 30)),
                    ("Role", settings.role.clone()),
                ];
                self.value_card(ctx, theme, pal, x, y, w, &rows)
            }
            SettingsSection::Cluster => {
                let rows = if settings.clustered {
                    vec![(
                        "Cluster",
                        settings
                            .cluster_summary
                            .clone()
                            .unwrap_or_else(|| "connected".to_string()),
                    )]
                } else {
                    vec![
                        ("State", "running locally".to_string()),
                        ("Nodes", "this device only".to_string()),
                    ]
                };
                self.value_card(ctx, theme, pal, x, y, w, &rows)
            }
            SettingsSection::Providers => self.provider_cards(
                ctx,
                theme,
                pal,
                x,
                y,
                w,
                &settings.provider_forms,
                settings.editing.as_ref(),
                settings.device_login.as_ref(),
            ),
            SettingsSection::Configuration => {
                if let Some(error) = &settings.config_error {
                    let rows = vec![("Error", error.clone())];
                    self.value_card(ctx, theme, pal, x, y, w, &rows)
                } else if settings.config_items.is_empty() {
                    let rows = vec![("", "No configuration keys".to_string())];
                    self.value_card(ctx, theme, pal, x, y, w, &rows)
                } else {
                    self.config_card(
                        ctx,
                        theme,
                        pal,
                        x,
                        y,
                        w,
                        &settings.config_items,
                        settings.editing.as_ref(),
                    )
                }
            }
            SettingsSection::About => {
                let rows = vec![
                    ("Sinomo App", format!("v{}", settings.version)),
                    ("Toolkit", "SUI".to_string()),
                ];
                self.value_card(ctx, theme, pal, x, y, w, &rows)
            }
        }
    }

    fn card_bg(&self, ctx: &mut PaintCtx, pal: &Palette, x: f32, y: f32, w: f32, h: f32) {
        widgets::round_panel(ctx, Rect::new(x, y, w, h), pal.panel, pal.border, 12.0);
    }

    #[allow(clippy::too_many_arguments)]
    fn value_card(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
        rows: &[(&str, String)],
    ) -> f32 {
        let h = rows.len() as f32 * ROW_H;
        self.card_bg(ctx, pal, x, y, w, h);
        let label_style = pal.text_style(theme, 13.5, FontWeight::MEDIUM, pal.text);
        let value_style = pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_muted);
        for (i, (label, value)) in rows.iter().enumerate() {
            let ry = y + i as f32 * ROW_H;
            if i > 0 {
                ctx.fill_rect(Rect::new(x + 16.0, ry, w - 16.0, 1.0), pal.border);
            }
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, ry, 220.0, ROW_H),
                label,
                label_style.clone(),
            );
            let vw = w - 16.0 - 236.0;
            let value = elide_to(value, vw, ctx, &value_style);
            let tw = ctx
                .measure_text(value.clone(), value_style.clone())
                .map(|m| m.width)
                .unwrap_or(0.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(x + w - 16.0 - tw, ry, tw + 4.0, ROW_H),
                &value,
                value_style.clone(),
            );
        }
        y + h
    }

    fn theme_row(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
    ) -> f32 {
        let h = ROW_H;
        self.card_bg(ctx, pal, x, y, w, h);
        let label_style = pal.text_style(theme, 13.5, FontWeight::MEDIUM, pal.text);
        widgets::draw_text_in(ctx, Rect::new(x + 16.0, y, 200.0, h), "Theme", label_style);

        let modes = [
            AppThemeMode::Light,
            AppThemeMode::Dark,
            AppThemeMode::System,
        ];
        let current = self.shared.borrow().theme_mode;
        let seg_w = 84.0;
        let seg_h = 28.0;
        let total = seg_w * 3.0;
        let sx = x + w - 16.0 - total;
        let sy = y + (h - seg_h) / 2.0;
        widgets::round_panel(
            ctx,
            Rect::new(sx, sy, total, seg_h),
            pal.field,
            pal.border,
            seg_h / 2.0,
        );
        for (i, mode) in modes.iter().enumerate() {
            let seg = Rect::new(sx + i as f32 * seg_w, sy, seg_w, seg_h);
            let active = current == *mode;
            if active {
                widgets::fill_round_rect(
                    ctx,
                    seg.inflate(-2.0, -2.0),
                    pal.accent,
                    (seg_h - 4.0) / 2.0,
                );
            }
            let color = if active {
                pal.on_accent
            } else {
                pal.text_muted
            };
            let style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, color);
            let label = mode.label();
            let tw = ctx
                .measure_text(label, style.clone())
                .map(|m| m.width)
                .unwrap_or(30.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    seg.x() + (seg.width() - tw) / 2.0,
                    seg.y(),
                    tw + 4.0,
                    seg.height(),
                ),
                label,
                style,
            );
            self.hit_rects
                .borrow_mut()
                .push((Hit::ThemeMode(*mode), seg));
        }
        y + h
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn config_card(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
        items: &[ConfigItem],
        editing: Option<&ConfigEdit>,
    ) -> f32 {
        let h = items.len() as f32 * ROW_H;
        self.card_bg(ctx, pal, x, y, w, h);
        let label_style = pal.text_style(theme, 13.0, FontWeight::MEDIUM, pal.text);
        let value_style = pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text_muted);
        let tag_style = pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text_faint);
        let edit_style = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text);

        let field_w = 288.0_f32.min((w - 220.0).max(140.0));
        let key_w = (w - field_w - 48.0).max(80.0);

        for (i, item) in items.iter().enumerate() {
            let ry = y + i as f32 * ROW_H;
            if i > 0 {
                ctx.fill_rect(Rect::new(x + 16.0, ry, w - 16.0, 1.0), pal.border);
            }
            let active = editing.filter(|e| e.targets_config_key(&item.key));

            let key = item.key.trim_start_matches('/');
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, ry + 4.0, key_w, 20.0),
                key,
                label_style.clone(),
            );
            // Second line: the resolved source layer, or the edit error when a commit failed.
            match active.and_then(|e| e.error.as_deref()) {
                Some(err) => {
                    let err_style = pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.accent);
                    widgets::draw_text_in(
                        ctx,
                        Rect::new(x + 16.0, ry + 22.0, key_w, 16.0),
                        err,
                        err_style,
                    );
                }
                None => {
                    widgets::draw_text_in(
                        ctx,
                        Rect::new(x + 16.0, ry + 22.0, key_w, 16.0),
                        item.source,
                        tag_style.clone(),
                    );
                }
            }

            match item.bool_value {
                Some(on) => {
                    let track_w = 42.0;
                    let track_h = 24.0;
                    let tx = x + w - 16.0 - track_w;
                    let ty = ry + (ROW_H - track_h) / 2.0;
                    let track = Rect::new(tx, ty, track_w, track_h);
                    widgets::fill_round_rect(
                        ctx,
                        track,
                        if on { pal.accent } else { pal.field },
                        track_h / 2.0,
                    );
                    if !on {
                        widgets::round_panel(ctx, track, pal.field, pal.border, track_h / 2.0);
                    }
                    let knob = track_h - 6.0;
                    let kx = if on {
                        tx + track_w - knob - 3.0
                    } else {
                        tx + 3.0
                    };
                    widgets::fill_round_rect(
                        ctx,
                        Rect::new(kx, ty + 3.0, knob, knob),
                        if on { pal.on_accent } else { pal.text_muted },
                        knob / 2.0,
                    );
                    self.hit_rects
                        .borrow_mut()
                        .push((Hit::ConfigToggle(i), track));
                }
                None => {
                    let field_h = 28.0;
                    let fx = x + w - 16.0 - field_w;
                    let fy = ry + (ROW_H - field_h) / 2.0;
                    let field = Rect::new(fx, fy, field_w, field_h);
                    if let Some(edit) = active {
                        // Active inline editor: bordered field, left-aligned draft, caret.
                        widgets::round_panel(ctx, field, pal.field, pal.accent, 7.0);
                        let inner = Rect::new(fx + 9.0, fy, field_w - 18.0, field_h);
                        widgets::draw_text_in(ctx, inner, &edit.draft, edit_style.clone());
                        let upto = edit.cursor.min(edit.draft.len());
                        let cw = ctx
                            .measure_text(&edit.draft[..upto], edit_style.clone())
                            .map(|m| m.width)
                            .unwrap_or(0.0);
                        let caret_x = (fx + 9.0 + cw).min(fx + field_w - 9.0);
                        ctx.fill_rect(
                            Rect::new(caret_x, fy + 5.0, 1.5, field_h - 10.0),
                            pal.accent,
                        );
                    } else {
                        // Read-only value, right-aligned; click anywhere in the field to edit.
                        let display = if item.edit_value.is_empty() {
                            "—".to_string()
                        } else {
                            elide_to(&item.edit_value, field_w - 14.0, ctx, &value_style)
                        };
                        let tw = ctx
                            .measure_text(display.clone(), value_style.clone())
                            .map(|m| m.width)
                            .unwrap_or(0.0);
                        widgets::draw_text_in(
                            ctx,
                            Rect::new(x + w - 16.0 - tw, ry, tw + 4.0, ROW_H),
                            &display,
                            value_style.clone(),
                        );
                    }
                    self.hit_rects.borrow_mut().push((Hit::EditField(i), field));
                }
            }
        }
        y + h
    }

    /// The Providers section: one card per configurable provider — enabled toggle, API key,
    /// default model, base URL, and (when applicable) reasoning effort.
    #[allow(clippy::too_many_arguments)]
    fn provider_cards(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
        forms: &[ProviderForm],
        editing: Option<&ConfigEdit>,
        device_login: Option<&DeviceLogin>,
    ) -> f32 {
        let name_style = pal.text_style(theme, 14.5, FontWeight::SEMIBOLD, pal.text);
        let label_style = pal.text_style(theme, 12.5, FontWeight::MEDIUM, pal.text);
        let value_style = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text_muted);
        let edit_style = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text);
        let chip_style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text);
        let tag_style = pal.text_style(theme, 10.5, FontWeight::NORMAL, pal.text_faint);

        let field_w = 300.0_f32.min((w - 200.0).max(160.0));
        let card_gap = 14.0;
        let mut cy = y;

        for (index, form) in forms.iter().enumerate() {
            let mut rows = 4u32; // header + API key + model + base URL
            if form.supports_reasoning {
                rows += 1;
            }
            let card_h = rows as f32 * ROW_H;
            self.card_bg(ctx, pal, x, cy, w, card_h);

            // Header: provider name + enabled toggle.
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, cy, w - 80.0, ROW_H),
                &form.display_name,
                name_style.clone(),
            );
            let track_w = 42.0;
            let track_h = 24.0;
            let tx = x + w - 16.0 - track_w;
            let ty = cy + (ROW_H - track_h) / 2.0;
            let track = Rect::new(tx, ty, track_w, track_h);
            let on = form.enabled;
            widgets::fill_round_rect(
                ctx,
                track,
                if on { pal.accent } else { pal.field },
                track_h / 2.0,
            );
            if !on {
                widgets::round_panel(ctx, track, pal.field, pal.border, track_h / 2.0);
            }
            let knob = track_h - 6.0;
            let kx = if on { tx + track_w - knob - 3.0 } else { tx + 3.0 };
            widgets::fill_round_rect(
                ctx,
                Rect::new(kx, ty + 3.0, knob, knob),
                if on { pal.on_accent } else { pal.text_muted },
                knob / 2.0,
            );
            self.hit_rects
                .borrow_mut()
                .push((Hit::ProviderToggle(index), track));

            // Authentication: the OAuth device-login flow, or an API key, per provider.
            let mut row_y = cy + ROW_H;
            if form.uses_device_login {
                self.provider_auth_row(
                    ctx, pal, x, row_y, w, field_w, &label_style, &value_style, &chip_style,
                    &tag_style, index, form, device_login,
                );
            } else {
                let api_key_value = if form.has_api_key {
                    "Configured"
                } else {
                    "Not set"
                };
                self.provider_field_row(
                    ctx, pal, x, row_y, w, field_w, "API key", &label_style, api_key_value,
                    &value_style, &edit_style, index, &form.provider, ProviderField::ApiKey, editing,
                );
            }
            row_y += ROW_H;
            self.provider_field_row(
                ctx, pal, x, row_y, w, field_w, "Default model", &label_style, &form.default_model,
                &value_style, &edit_style, index, &form.provider, ProviderField::Model, editing,
            );
            row_y += ROW_H;
            self.provider_field_row(
                ctx, pal, x, row_y, w, field_w, "API base URL", &label_style, &form.api_base_url,
                &value_style, &edit_style, index, &form.provider, ProviderField::BaseUrl, editing,
            );
            row_y += ROW_H;

            // Reasoning effort (when applicable): a chip you click to cycle.
            if form.supports_reasoning {
                ctx.fill_rect(Rect::new(x + 16.0, row_y, w - 16.0, 1.0), pal.border);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(x + 16.0, row_y, w - field_w - 40.0, ROW_H),
                    "Reasoning effort",
                    label_style.clone(),
                );
                let effort = form
                    .reasoning_effort
                    .clone()
                    .unwrap_or_else(|| "default".to_string());
                let chip_w = 132.0;
                let chip_h = 28.0;
                let chip = Rect::new(
                    x + w - 16.0 - chip_w,
                    row_y + (ROW_H - chip_h) / 2.0,
                    chip_w,
                    chip_h,
                );
                widgets::round_panel(ctx, chip, pal.field, pal.border, chip_h / 2.0);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(chip.x() + 14.0, chip.y(), chip_w - 28.0, chip_h),
                    &effort,
                    chip_style.clone(),
                );
                self.hit_rects
                    .borrow_mut()
                    .push((Hit::ProviderEffort(index), chip));
            }

            cy += card_h + card_gap;
        }

        if forms.is_empty() { y } else { cy - card_gap }
    }

    /// One labeled, click-to-edit provider field (shared by the Providers card rows).
    #[allow(clippy::too_many_arguments)]
    fn provider_field_row(
        &self,
        ctx: &mut PaintCtx,
        pal: &Palette,
        x: f32,
        row_y: f32,
        w: f32,
        field_w: f32,
        label: &str,
        label_style: &sui::TextStyle,
        read_value: &str,
        value_style: &sui::TextStyle,
        edit_style: &sui::TextStyle,
        index: usize,
        provider: &str,
        field: ProviderField,
        editing: Option<&ConfigEdit>,
    ) {
        ctx.fill_rect(Rect::new(x + 16.0, row_y, w - 16.0, 1.0), pal.border);
        widgets::draw_text_in(
            ctx,
            Rect::new(x + 16.0, row_y, w - field_w - 40.0, ROW_H),
            label,
            label_style.clone(),
        );

        let fh = 28.0;
        let fx = x + w - 16.0 - field_w;
        let fy = row_y + (ROW_H - fh) / 2.0;
        let frect = Rect::new(fx, fy, field_w, fh);
        let active = editing.filter(|e| e.targets_provider(provider, field));
        if let Some(edit) = active {
            widgets::round_panel(ctx, frect, pal.field, pal.accent, 7.0);
            let inner = Rect::new(fx + 9.0, fy, field_w - 18.0, fh);
            widgets::draw_text_in(ctx, inner, &edit.draft, edit_style.clone());
            let upto = edit.cursor.min(edit.draft.len());
            let cw = ctx
                .measure_text(&edit.draft[..upto], edit_style.clone())
                .map(|m| m.width)
                .unwrap_or(0.0);
            let caret_x = (fx + 9.0 + cw).min(fx + field_w - 9.0);
            ctx.fill_rect(Rect::new(caret_x, fy + 5.0, 1.5, fh - 10.0), pal.accent);
        } else {
            let display = if read_value.is_empty() {
                "—".to_string()
            } else {
                elide_to(read_value, field_w - 14.0, ctx, value_style)
            };
            let tw = ctx
                .measure_text(display.clone(), value_style.clone())
                .map(|m| m.width)
                .unwrap_or(0.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(fx + field_w - tw - 6.0, row_y, tw + 8.0, ROW_H),
                &display,
                value_style.clone(),
            );
        }
        self.hit_rects
            .borrow_mut()
            .push((Hit::ProviderEdit(index, field), frect));
    }

    /// The authentication row for a device-login provider: status (signed in / device code /
    /// error) plus a sign-in button.
    #[allow(clippy::too_many_arguments)]
    fn provider_auth_row(
        &self,
        ctx: &mut PaintCtx,
        pal: &Palette,
        x: f32,
        row_y: f32,
        w: f32,
        field_w: f32,
        label_style: &sui::TextStyle,
        value_style: &sui::TextStyle,
        chip_style: &sui::TextStyle,
        tag_style: &sui::TextStyle,
        index: usize,
        form: &ProviderForm,
        device_login: Option<&DeviceLogin>,
    ) {
        ctx.fill_rect(Rect::new(x + 16.0, row_y, w - 16.0, 1.0), pal.border);

        let active = device_login.filter(|d| d.provider == form.provider);
        let (status_text, url, button): (String, Option<&str>, Option<&str>) =
            match active.map(|d| &d.status) {
                Some(DeviceLoginStatus::Starting) => ("Requesting code…".to_string(), None, None),
                Some(DeviceLoginStatus::Prompt {
                    verification_uri,
                    user_code,
                }) => (
                    format!("Code {user_code}"),
                    Some(verification_uri.as_str()),
                    None,
                ),
                Some(DeviceLoginStatus::Done) => ("Signed in".to_string(), None, None),
                Some(DeviceLoginStatus::Failed(error)) => {
                    (format!("Failed: {error}"), None, Some("Sign in"))
                }
                None if form.signed_in => ("Signed in".to_string(), None, Some("Re-authenticate")),
                None => ("Not signed in".to_string(), None, Some("Sign in")),
            };

        // Label — at the top when a verification URL sub-line is shown, else vertically centered.
        let key_w = (w - field_w - 48.0).max(80.0);
        if let Some(url) = url {
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, row_y + 4.0, key_w, 20.0),
                "Authentication",
                label_style.clone(),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, row_y + 22.0, key_w, 16.0),
                url,
                tag_style.clone(),
            );
        } else {
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, row_y, key_w, ROW_H),
                "Authentication",
                label_style.clone(),
            );
        }

        // Sign-in button chip on the far right (when applicable).
        let mut right_edge = x + w - 16.0;
        if let Some(button) = button {
            let label_w = ctx
                .measure_text(button, chip_style.clone())
                .map(|m| m.width)
                .unwrap_or(48.0);
            let chip_w = label_w + 24.0;
            let chip_h = 28.0;
            let chip = Rect::new(
                right_edge - chip_w,
                row_y + (ROW_H - chip_h) / 2.0,
                chip_w,
                chip_h,
            );
            widgets::round_panel(ctx, chip, pal.field, pal.accent, chip_h / 2.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(chip.x() + 12.0, chip.y(), chip_w - 24.0, chip_h),
                button,
                chip_style.clone(),
            );
            self.hit_rects
                .borrow_mut()
                .push((Hit::ProviderSignIn(index), chip));
            right_edge -= chip_w + 10.0;
        }

        // Status text, right-aligned before the button.
        let status_w = ctx
            .measure_text(status_text.clone(), value_style.clone())
            .map(|m| m.width)
            .unwrap_or(0.0);
        widgets::draw_text_in(
            ctx,
            Rect::new(right_edge - status_w, row_y, status_w + 4.0, ROW_H),
            &status_text,
            value_style.clone(),
        );
    }
}

fn elide_to(text: &str, max_w: f32, ctx: &PaintCtx, style: &sui::TextStyle) -> String {
    let full = ctx
        .measure_text(text, style.clone())
        .map(|m| m.width)
        .unwrap_or(0.0);
    if full <= max_w {
        return text.to_string();
    }
    let chars: Vec<char> = text.chars().collect();
    let mut n = chars.len();
    while n > 1 {
        n -= 1;
        let candidate: String = chars[..n].iter().collect::<String>() + "…";
        let w = ctx
            .measure_text(candidate.clone(), style.clone())
            .map(|m| m.width)
            .unwrap_or(0.0);
        if w <= max_w {
            return candidate;
        }
    }
    "…".to_string()
}

fn short(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max - 1).collect();
        format!("{head}…")
    }
}

/// Build the settings tab content.
pub fn view(shared: Shared) -> impl Widget {
    SettingsView::new(shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;

    fn press(name: &str, text: Option<&str>) -> sui::KeyboardEvent {
        let mut key = sui::KeyboardEvent::new(name, KeyState::Pressed);
        key.text = text.map(|t| t.to_string());
        key
    }

    /// A settings state plus the index of a non-boolean (text-editable) config key, or `None`
    /// when this environment has no such keys registered.
    fn editable_state() -> Option<(SettingsState, usize)> {
        let backend = Backend::local_fallback();
        let state = SettingsState::new(&backend);
        let idx = state
            .config_items
            .iter()
            .position(|item| item.edit_kind != ConfigEditKind::Bool)?;
        Some((state, idx))
    }

    #[test]
    fn inline_editor_types_into_the_draft() {
        let Some((mut state, idx)) = editable_state() else {
            return;
        };
        state.begin_config_edit(idx);
        assert!(state.editing.is_some(), "begin_edit opens the inline editor");
        let before = state.editing.as_ref().unwrap().draft.len();

        let outcome = state.edit_key(&press("X", Some("X")));
        assert!(matches!(outcome, EditOutcome::Continue));
        let edit = state.editing.as_ref().expect("still editing");
        assert_eq!(edit.draft.len(), before + 1);
        assert!(edit.draft.contains('X'));
        assert_eq!(edit.cursor, edit.draft.len(), "cursor advances with the insert");
    }

    #[test]
    fn enter_commits_and_escape_cancels() {
        let Some((mut state, idx)) = editable_state() else {
            return;
        };
        state.begin_config_edit(idx);
        assert!(matches!(
            state.edit_key(&press("Enter", None)),
            EditOutcome::Commit
        ));
        state.cancel_edit();

        state.begin_config_edit(idx);
        assert!(matches!(
            state.edit_key(&press("Escape", None)),
            EditOutcome::Cancel
        ));
        state.cancel_edit();
        assert!(state.editing.is_none());
    }

    #[test]
    fn booleans_use_the_toggle_not_the_editor() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        if let Some(idx) = state
            .config_items
            .iter()
            .position(|item| item.edit_kind == ConfigEditKind::Bool)
        {
            state.begin_config_edit(idx);
            assert!(
                state.editing.is_none(),
                "booleans are toggled, never text-edited"
            );
        }
    }

    #[test]
    fn provider_editor_targets_the_right_field() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        assert!(
            !state.provider_forms.is_empty(),
            "the configurable provider list is non-empty"
        );

        state.begin_provider_field_edit(0, ProviderField::ApiKey);
        let provider = state.provider_forms[0].provider.clone();
        let edit = state.editing.as_ref().expect("provider editor open");
        assert!(edit.targets_provider(&provider, ProviderField::ApiKey));
        assert!(!edit.targets_provider(&provider, ProviderField::Model));
        assert!(edit.draft.is_empty(), "API key starts blank, never surfaced");

        let outcome = state.edit_key(&press("k", Some("k")));
        assert!(matches!(outcome, EditOutcome::Continue));
        assert_eq!(state.editing.as_ref().unwrap().draft, "k");
    }

    #[test]
    fn device_login_pump_applies_prompt_then_clears_on_done() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let (tx, rx) = std::sync::mpsc::channel();
        state.device_login = Some(DeviceLogin {
            provider: "codex".to_string(),
            status: DeviceLoginStatus::Starting,
            rx,
        });

        assert!(!state.pump_device_login(), "no events yet");

        tx.send(DeviceLoginEvent::Prompt {
            verification_uri: "https://example.test/device".to_string(),
            user_code: "ABCD-1234".to_string(),
        })
        .unwrap();
        assert!(state.pump_device_login());
        match &state.device_login.as_ref().unwrap().status {
            DeviceLoginStatus::Prompt { user_code, .. } => assert_eq!(user_code, "ABCD-1234"),
            _ => panic!("expected the prompt status"),
        }

        tx.send(DeviceLoginEvent::Done).unwrap();
        assert!(state.pump_device_login());
        assert!(
            state.device_login.is_none(),
            "a completed flow is cleared back to the resting state"
        );
    }
}
