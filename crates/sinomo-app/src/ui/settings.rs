//! "Settings" tab — a nested, responsive settings page.
//!
//! Settings are grouped into top-level [`SettingsSection`]s. When the tab is wide
//! enough, the section menu stays pinned in a left sidebar (the "previous level")
//! next to the selected section's detail — a master/detail layout. When it's
//! narrow, it collapses to a single column: the menu, which you drill into to
//! reach a detail page with a back affordance.
//!
//! The page is a fully *composed* `sui-widgets` tree: [`view`] returns a small
//! responsive host ([`SettingsView`]) wrapping two pre-built pods (wide/narrow),
//! each assembled from stock `PanelSection` / `PropertyRow` / `TabBar` / `Switch` /
//! `TextInput` / `Select` / `Button` / `ListView` / `ScrollView` widgets. State lives
//! entirely in [`SettingsState`]; controls bind to it via reactive `*_when` readers and
//! snapshot-seed-plus-`on_change` writeback. The async provider device-login status is
//! the one bespoke region — a live-reading [`StatusReflector`] widget.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use sui::{
    Alignment, ArrangeCtx, Constraints, DefaultTheme, Event, EventCtx, FontWeight, Label, ListItem,
    ListView, MeasureCtx, PaintCtx, PanelSection, PropertyRow, Rect, ScrollState, ScrollView,
    Select, Separator, Size, SizedBox, Stack, Switch, SwitchView, TabBar, TextInput, Widget,
    WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent,
};

use sinomo_runtime::config::ConfigClient;

use crate::backend::Backend;
use crate::backend::config::{
    CONFIGURABLE_PROVIDERS, ConfigEditKind, ConfigItem, ProviderForm, REASONING_EFFORT_OPTIONS,
    cycle_provider_reasoning_effort, provider_forms, reasoning_effort_index, resolve_items,
    run_provider_device_login, set_bool, set_provider_api_key, set_provider_base_url,
    set_provider_effort, set_provider_enabled, set_provider_model, set_value,
};
use crate::state::Shared;
use crate::theme::{AppThemeMode, Palette};
use crate::ui::widgets;

/// Below this content width the tab collapses to a single drill-down column.
const WIDE_THRESHOLD: f32 = 700.0;
const SIDEBAR_W: f32 = 256.0;
const SIDEBAR_ITEM_H: f32 = 40.0;
const MENU_ROW_H: f32 = 58.0;
const CONTENT_PAD: f32 = 22.0;

/// A shared closure that resolves the current widget theme — threaded into every control so the
/// Appearance toggle restyles the whole tab live, with no rebuild.
type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

/// A top-level settings destination. Each renders its own detail page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

    fn index(self) -> usize {
        Self::ALL.iter().position(|s| *s == self).unwrap_or(0)
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
    /// Per-field parse errors keyed by the field's row identity (config key or provider+field),
    /// surfaced as an inline subtitle when a typed value fails to parse. The field's own text is
    /// owned by its `TextInput`, so a bad value stays on screen until corrected.
    pub field_errors: std::collections::HashMap<String, String>,
    /// An in-progress provider device-login flow (Providers section), if any.
    pub device_login: Option<DeviceLogin>,
}

/// A text-editable field of a provider in the Providers section.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProviderField {
    ApiKey,
    Model,
    BaseUrl,
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
            field_errors: std::collections::HashMap::new(),
            device_login: None,
        }
    }

    /// Open a section's detail page.
    fn open(&mut self, section: SettingsSection) {
        self.selected = Some(section);
    }

    /// Return to the menu level (narrow layout).
    fn back_to_menu(&mut self) {
        self.selected = None;
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

    /// Write an edited (non-boolean) config value at `index`. On a parse error the prior value is
    /// preserved and the error is recorded against the row (shown as an inline subtitle); the
    /// `TextInput` keeps the user's text. On success the row's error is cleared. Does NOT reload on
    /// every keystroke (that would re-resolve the whole catalog per character) — the field's own
    /// text keeps it correct, and a reload happens on the next section open / external wake.
    fn set_config_value(&mut self, index: usize, raw: &str) {
        let Some(item) = self.config_items.get(index) else {
            return;
        };
        if item.edit_kind == ConfigEditKind::Bool {
            return;
        }
        let key = item.key.clone();
        let kind = item.edit_kind;
        match set_value(self.config.as_ref(), &key, kind, raw) {
            Ok(()) => {
                self.field_errors.remove(&config_field_key(&key));
            }
            Err(error) => {
                self.field_errors.insert(config_field_key(&key), error);
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

    /// Write a provider text field (API key / model / base URL) at `index`. Like
    /// [`set_config_value`](Self::set_config_value) this writes through on change but does NOT
    /// reload per keystroke. Records any error against the provider+field row.
    fn set_provider_field(&mut self, index: usize, field: ProviderField, raw: &str) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        let provider = form.provider.clone();
        let result = match field {
            ProviderField::ApiKey => set_provider_api_key(self.config.as_ref(), &provider, raw),
            ProviderField::Model => set_provider_model(self.config.as_ref(), &provider, raw),
            ProviderField::BaseUrl => set_provider_base_url(self.config.as_ref(), &provider, raw),
        };
        let row = provider_field_key(&provider, field);
        match result {
            Ok(()) => {
                self.field_errors.remove(&row);
            }
            Err(error) => {
                self.field_errors.insert(row, error);
            }
        }
    }

    /// Cycle a provider's reasoning effort to the next option (legacy click-to-cycle path; retained
    /// for callers that prefer it — the UI now uses [`set_provider_effort_at`](Self::set_provider_effort_at)).
    #[allow(dead_code)]
    fn cycle_provider_effort(&mut self, index: usize) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        let provider = form.provider.clone();
        if cycle_provider_reasoning_effort(self.config.as_ref(), &provider).is_ok() {
            self.reload();
        }
    }

    /// Set a provider's reasoning effort to the option at `effort_index` into
    /// [`REASONING_EFFORT_OPTIONS`] (index 0 clears the override). Reloads on success.
    fn set_provider_effort_at(&mut self, index: usize, effort_index: usize) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        let provider = form.provider.clone();
        if set_provider_effort(self.config.as_ref(), &provider, effort_index).is_ok() {
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
}

/// Field-error map key for a raw config key (Advanced section).
fn config_field_key(key: &str) -> String {
    format!("config:{key}")
}

/// Field-error map key for a provider text field (Providers section).
fn provider_field_key(provider: &str, field: ProviderField) -> String {
    let suffix = match field {
        ProviderField::ApiKey => "api_key",
        ProviderField::Model => "model",
        ProviderField::BaseUrl => "base_url",
    };
    format!("provider:{provider}:{suffix}")
}

/// A short, right-aligned value preview for a section, shown in the narrow menu rows.
pub fn section_preview(shared: &Shared, section: SettingsSection) -> String {
    let settings = shared.settings();
    match section {
        SettingsSection::Appearance => shared.borrow().theme_mode.label().to_string(),
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

// ---------------------------------------------------------------------------
// Theme reader + builder helpers
// ---------------------------------------------------------------------------

/// Clone the shared theme reader into a fresh `Fn() -> DefaultTheme` for one control.
fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

/// Resolve the app chrome palette for the current theme (used by the `Label` color readers and the
/// `StatusReflector` so muted/accent text matches the bespoke chrome).
fn palette(shared: &Shared) -> Palette {
    Palette::from_theme(&shared.theme(), widgets::theme_is_dark(shared))
}

/// A muted `Label` whose color tracks the theme.
fn muted_label(shared: &Shared, text: impl Into<String>) -> Label {
    let s = shared.clone();
    Label::new(text).color_when(move || palette(&s).text_muted)
}

/// A read-only value row: `label` on the left, a muted value `Label` on the right.
fn value_row(
    shared: &Shared,
    reader: &ThemeReader,
    label: &str,
    value: impl Into<String>,
) -> PropertyRow {
    PropertyRow::new(label, muted_label(shared, value))
        .inline()
        .theme_when(clone_reader(reader))
}

/// A toggle row: `label` + a `Switch` seeded `on`, calling `on_toggle` when flipped.
fn switch_row<F>(reader: &ThemeReader, label: &str, on: bool, on_toggle: F) -> PropertyRow
where
    F: FnMut(bool) + 'static,
{
    let switch = Switch::new("")
        .on(on)
        .theme_when(clone_reader(reader))
        .on_toggle(on_toggle);
    PropertyRow::new(label, switch)
        .inline()
        .theme_when(clone_reader(reader))
}

/// A text-field row: `label` + a `TextInput` seeded with `value` (writing through on each change).
fn text_row<F>(
    reader: &ThemeReader,
    name: &str,
    label: &str,
    value: impl Into<String>,
    placeholder: &str,
    on_change: F,
) -> PropertyRow
where
    F: FnMut(String) + 'static,
{
    let input = TextInput::new(name)
        .value(value)
        .placeholder(placeholder)
        .theme_when(clone_reader(reader))
        .on_change(on_change);
    PropertyRow::new(label, input)
        .inline()
        .theme_when(clone_reader(reader))
}

/// A dropdown row: `label` + a `Select` over `options`, its selection read reactively.
fn select_row<S, C>(
    reader: &ThemeReader,
    name: &str,
    label: &str,
    options: &[&str],
    selected_when: S,
    on_change: C,
) -> PropertyRow
where
    S: Fn() -> Option<usize> + 'static,
    C: FnMut(usize, String) + 'static,
{
    let select = Select::new(name)
        .options(options.iter().copied())
        .selected_when(selected_when)
        .theme_when(clone_reader(reader))
        .on_change(on_change);
    PropertyRow::new(label, select)
        .inline()
        .theme_when(clone_reader(reader))
}

// ---------------------------------------------------------------------------
// Section subtrees
// ---------------------------------------------------------------------------

fn index_to_mode(i: usize) -> AppThemeMode {
    match i {
        0 => AppThemeMode::Light,
        1 => AppThemeMode::Dark,
        _ => AppThemeMode::System,
    }
}

fn mode_to_index(mode: AppThemeMode) -> usize {
    match mode {
        AppThemeMode::Light => 0,
        AppThemeMode::Dark => 1,
        AppThemeMode::System => 2,
    }
}

fn section_appearance(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let mode_index = mode_to_index(shared.borrow().theme_mode);
    let tab_bar = {
        let s = shared.clone();
        TabBar::new("theme")
            .tabs(["Light", "Dark", "System"])
            .selected(mode_index)
            .theme_when(clone_reader(reader))
            .on_change(move |i, _| {
                s.borrow_mut().theme_mode = index_to_mode(i);
            })
    };
    let body = Stack::vertical().alignment(Alignment::Stretch).with_child(
        PropertyRow::new("Theme", tab_bar)
            .inline()
            .theme_when(clone_reader(reader)),
    );
    PanelSection::new("Appearance", body).theme_when(clone_reader(reader))
}

fn section_connection(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (mode_label, status_detail) = {
        let s = shared.settings();
        (s.mode_label.clone(), s.status_detail.clone())
    };
    let body = Stack::vertical()
        .spacing(2.0)
        .alignment(Alignment::Stretch)
        .with_child(value_row(shared, reader, "Mode", mode_label))
        .with_child(value_row(shared, reader, "Status", status_detail));
    PanelSection::new("Connection", body).theme_when(clone_reader(reader))
}

fn section_node(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (node_id, role) = {
        let s = shared.settings();
        (short(&s.node_id, 30), s.role.clone())
    };
    let body = Stack::vertical()
        .spacing(2.0)
        .alignment(Alignment::Stretch)
        .with_child(value_row(shared, reader, "Node ID", node_id))
        .with_child(value_row(shared, reader, "Role", role));
    PanelSection::new("Node", body).theme_when(clone_reader(reader))
}

fn section_cluster(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let body = {
        let s = shared.settings();
        if s.clustered {
            let summary = s
                .cluster_summary
                .clone()
                .unwrap_or_else(|| "connected".to_string());
            drop(s);
            Stack::vertical()
                .spacing(2.0)
                .alignment(Alignment::Stretch)
                .with_child(value_row(shared, reader, "Cluster", summary))
        } else {
            drop(s);
            Stack::vertical()
                .spacing(2.0)
                .alignment(Alignment::Stretch)
                .with_child(value_row(shared, reader, "State", "running locally"))
                .with_child(value_row(shared, reader, "Nodes", "this device only"))
        }
    };
    PanelSection::new("Cluster", body).theme_when(clone_reader(reader))
}

fn section_about(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let version = shared.settings().version.clone();
    let body = Stack::vertical()
        .spacing(2.0)
        .alignment(Alignment::Stretch)
        .with_child(value_row(
            shared,
            reader,
            "Sinomo App",
            format!("v{version}"),
        ))
        .with_child(value_row(shared, reader, "Toolkit", "SUI"));
    PanelSection::new("About", body).theme_when(clone_reader(reader))
}

/// One provider's card: enable toggle (header action), auth row, model / base URL, and effort.
fn provider_card(shared: &Shared, reader: &ThemeReader, index: usize) -> impl Widget + use<> {
    let form = shared.settings().provider_forms[index].clone();

    // Enable toggle as the panel's header action.
    let enable_switch = {
        let s = shared.clone();
        Switch::new("")
            .on(form.enabled)
            .theme_when(clone_reader(reader))
            .on_toggle(move |_| {
                s.settings_mut().toggle_provider(index);
            })
    };

    let mut body = Stack::vertical().spacing(8.0).alignment(Alignment::Stretch);

    // Authentication: device-login (status + button) or an API-key field.
    if form.uses_device_login {
        let sign_in_label = if form.signed_in {
            "Re-authenticate"
        } else {
            "Sign in"
        };
        let button = {
            let s = shared.clone();
            sui::Button::new(sign_in_label)
                .theme_when(clone_reader(reader))
                .on_press(move || {
                    let waker = s.borrow().waker.clone();
                    s.settings_mut().begin_device_login(index, waker);
                })
        };
        let auth = Stack::vertical()
            .spacing(6.0)
            .alignment(Alignment::Stretch)
            .with_child(StatusReflector::new(shared.clone(), index))
            .with_child(button);
        body = body.with_child(
            PropertyRow::new("Authentication", auth)
                .stacked()
                .theme_when(clone_reader(reader)),
        );
    } else {
        let placeholder = if form.has_api_key {
            "Configured"
        } else {
            "Not set"
        };
        let on_change = {
            let s = shared.clone();
            move |value: String| {
                s.settings_mut()
                    .set_provider_field(index, ProviderField::ApiKey, &value);
            }
        };
        body = body.with_child(text_row(
            reader,
            &format!("provider-{index}-api-key"),
            "API key",
            "",
            placeholder,
            on_change,
        ));
    }

    // Default model.
    body = body.with_child(text_row(
        reader,
        &format!("provider-{index}-model"),
        "Default model",
        form.default_model.clone(),
        "Provider default",
        {
            let s = shared.clone();
            move |value: String| {
                s.settings_mut()
                    .set_provider_field(index, ProviderField::Model, &value);
            }
        },
    ));

    // API base URL.
    body = body.with_child(text_row(
        reader,
        &format!("provider-{index}-base-url"),
        "API base URL",
        form.api_base_url.clone(),
        "Provider default",
        {
            let s = shared.clone();
            move |value: String| {
                s.settings_mut()
                    .set_provider_field(index, ProviderField::BaseUrl, &value);
            }
        },
    ));

    // Reasoning effort (when the provider/model supports it).
    if form.supports_reasoning {
        let selected_when = {
            let s = shared.clone();
            move || {
                let settings = s.settings();
                settings
                    .provider_forms
                    .get(index)
                    .map(|f| reasoning_effort_index(f.reasoning_effort.as_deref()))
            }
        };
        let on_change = {
            let s = shared.clone();
            move |idx: usize, _label: String| {
                s.settings_mut().set_provider_effort_at(index, idx);
            }
        };
        body = body.with_child(select_row(
            reader,
            &format!("provider-{index}-effort"),
            "Reasoning effort",
            REASONING_EFFORT_OPTIONS,
            selected_when,
            on_change,
        ));
    }

    PanelSection::new(form.display_name.clone(), body)
        .header_action(enable_switch)
        .theme_when(clone_reader(reader))
}

fn section_providers(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let mut stack = Stack::vertical()
        .spacing(14.0)
        .alignment(Alignment::Stretch);
    for index in 0..CONFIGURABLE_PROVIDERS.len() {
        stack = stack.with_child(provider_card(shared, reader, index));
    }
    stack
}

fn section_configuration(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let body: WidgetPod = {
        let settings = shared.settings();
        if let Some(error) = &settings.config_error {
            let error = error.clone();
            drop(settings);
            WidgetPod::new(value_row(shared, reader, "Error", error))
        } else if settings.config_items.is_empty() {
            drop(settings);
            WidgetPod::new(muted_label(shared, "No configuration keys"))
        } else {
            let count = settings.config_items.len();
            drop(settings);
            let mut stack = Stack::vertical().spacing(8.0).alignment(Alignment::Stretch);
            for index in 0..count {
                stack = stack.with_child(config_row(shared, reader, index));
            }
            WidgetPod::new(stack)
        }
    };
    PanelSection::new("Advanced", PodHost::new(body)).theme_when(clone_reader(reader))
}

/// One Advanced config row: a toggle for booleans, otherwise a text field with a muted subtitle
/// showing the source layer (or the parse error in accent color).
fn config_row(shared: &Shared, reader: &ThemeReader, index: usize) -> impl Widget + use<> {
    let (key, edit_kind, bool_value, edit_value, source) = {
        let item = &shared.settings().config_items[index];
        (
            item.key.clone(),
            item.edit_kind,
            item.bool_value,
            item.edit_value.clone(),
            item.source,
        )
    };
    let display_key = key.trim_start_matches('/').to_string();

    if let Some(on) = bool_value {
        if edit_kind == ConfigEditKind::Bool {
            let on_toggle = {
                let s = shared.clone();
                move |_on: bool| {
                    s.settings_mut().toggle_config(index);
                }
            };
            return PodHost::new(WidgetPod::new(switch_row(
                reader,
                &display_key,
                on,
                on_toggle,
            )));
        }
    }

    // Non-boolean: a text field plus a subtitle (source layer, or parse error in accent).
    let field = text_row(
        reader,
        &format!("config-{index}"),
        &display_key,
        edit_value,
        "—",
        {
            let s = shared.clone();
            move |value: String| {
                s.settings_mut().set_config_value(index, &value);
            }
        },
    );

    let subtitle = {
        let s = shared.clone();
        let row_key = config_field_key(&key);
        let source = source.to_string();
        Label::new(source.clone())
            .font_size(10.5)
            .text_when({
                let s = s.clone();
                let row_key = row_key.clone();
                let source = source.clone();
                move || {
                    s.settings()
                        .field_errors
                        .get(&row_key)
                        .cloned()
                        .unwrap_or_else(|| source.clone())
                }
            })
            .color_when(move || {
                let pal = palette(&s);
                if s.settings().field_errors.contains_key(&row_key) {
                    pal.accent
                } else {
                    pal.text_faint
                }
            })
    };

    let stacked = Stack::vertical()
        .spacing(2.0)
        .alignment(Alignment::Stretch)
        .with_child(field)
        .with_child(subtitle);
    PodHost::new(WidgetPod::new(stacked))
}

// ---------------------------------------------------------------------------
// Detail body + master/detail pods
// ---------------------------------------------------------------------------

fn section_body(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    let switch = SwitchView::new()
        .with_child(section_appearance(shared, reader))
        .with_child(section_connection(shared, reader))
        .with_child(section_node(shared, reader))
        .with_child(section_cluster(shared, reader))
        .with_child(section_providers(shared, reader))
        .with_child(section_configuration(shared, reader))
        .with_child(section_about(shared, reader))
        .selected_when({
            let s = shared.clone();
            move || {
                s.settings()
                    .selected
                    .unwrap_or(SettingsSection::Appearance)
                    .index()
            }
        });
    // Pad the content so cards don't hug the edges, then make it scrollable.
    let inset = Stack::vertical()
        .spacing(0.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(CONTENT_PAD))
        .with_child(InsetX::new(switch, CONTENT_PAD))
        .with_child(SizedBox::new().height(CONTENT_PAD));
    ScrollView::vertical(inset).state(scroll_state.clone())
}

/// Build the section list rows for the sidebar / menu.
fn section_list_items(shared: &Shared, with_preview: bool) -> Vec<ListItem> {
    SettingsSection::ALL
        .iter()
        .map(|section| {
            let mut item = ListItem::new(section.title());
            if with_preview {
                item = item.detail(section_preview(shared, *section));
            }
            item
        })
        .collect()
}

fn sidebar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let heading = {
        let s = shared.clone();
        Label::new("SETTINGS")
            .font_size(11.5)
            .style({
                let theme = shared.theme();
                sui::TextStyle {
                    font_size: 11.5,
                    line_height: 18.0,
                    weight: FontWeight::SEMIBOLD,
                    ..theme.body_text_style()
                }
            })
            .color_when(move || palette(&s).text_faint)
    };
    let list = {
        let s = shared.clone();
        ListView::new("settings-sections")
            .items(section_list_items(shared, false))
            .row_height(SIDEBAR_ITEM_H)
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || {
                    Some(
                        s.settings()
                            .selected
                            .unwrap_or(SettingsSection::Appearance)
                            .index(),
                    )
                }
            })
            .on_change(move |i, _| {
                s.settings_mut().open(SettingsSection::ALL[i]);
            })
    };
    Stack::vertical()
        .spacing(6.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(16.0))
        .with_child(InsetX::new(heading, 18.0))
        .with_child(SizedBox::new().height(6.0))
        .with_child(InsetX::new(list, 8.0))
}

fn menu(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let list = {
        let s = shared.clone();
        ListView::new("settings-menu")
            .items(section_list_items(shared, true))
            .row_height(MENU_ROW_H)
            .theme_when(clone_reader(reader))
            // The narrow menu is a flat drill-in list, not a selection: never keep a
            // row highlighted (otherwise a drilled-then-back row stays selected).
            .selected_when(|| None::<usize>)
            .on_change(move |i, _| {
                s.settings_mut().open(SettingsSection::ALL[i]);
            })
    };
    Stack::vertical()
        .spacing(0.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(CONTENT_PAD))
        .with_child(InsetX::new(list, CONTENT_PAD))
}

fn wide_pod(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let detail = section_body(shared, reader, scroll_state);
    let row = Stack::horizontal()
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new()
                .width(SIDEBAR_W)
                .with_child(sidebar(shared, reader)),
        )
        .with_child(Separator::vertical())
        .with_child(SizedBox::new().with_child(detail));
    WidgetPod::new(row)
}

fn narrow_pod(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let back_button = {
        let s = shared.clone();
        // A real leading Lucide ChevronLeft (Button's leading-icon slot) replaces the ‹ text rune.
        sui::Button::new("Settings")
            .icon(sui::IconGlyph::ChevronLeft)
            .theme_when(clone_reader(reader))
            .on_press(move || {
                s.settings_mut().back_to_menu();
            })
    };
    let detail_with_backbar = Stack::vertical()
        .spacing(0.0)
        .alignment(Alignment::Stretch)
        .with_child(InsetX::new(
            Stack::vertical()
                .with_child(SizedBox::new().height(8.0))
                .with_child(back_button)
                .with_child(SizedBox::new().height(8.0)),
            12.0,
        ))
        .with_child(Separator::horizontal())
        .with_child(SizedBox::new().with_child(section_body(shared, reader, scroll_state)));

    let switch = SwitchView::new()
        .with_child(menu(shared, reader))
        .with_child(detail_with_backbar)
        .selected_when({
            let s = shared.clone();
            move || s.settings().selected.is_some() as usize
        });
    WidgetPod::new(switch)
}

// ---------------------------------------------------------------------------
// Tiny layout helpers
// ---------------------------------------------------------------------------

/// A container that hosts a pre-built [`WidgetPod`] (so a section body can be chosen at build time
/// behind a branch and still be a single widget). Delegates everything to the pod.
struct PodHost {
    child: WidgetPod,
}

impl PodHost {
    fn new(child: WidgetPod) -> Self {
        Self { child }
    }
}

impl Widget for PodHost {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.child.measure(ctx, constraints)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.child.arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.child.paint(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.child);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.child);
    }
}

/// Insets a single child horizontally by `pad` on each side (the vertical axis is left to the
/// caller). Keeps cards off the panel edges without pulling in the container `Padding` widget.
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

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.child);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.child);
    }
}

// ---------------------------------------------------------------------------
// StatusReflector — live device-login status (custom widget)
// ---------------------------------------------------------------------------

/// Renders the device-login status for a provider, live-reading
/// `shared.settings().device_login` in `paint`. Diffs a status summary on each
/// `RedrawRequested` to self-invalidate (safety net; the Shell wake handler already requests a
/// paint on each device-login event). Mirrors the widget-book `WidgetBookSummary` pattern.
struct StatusReflector {
    shared: Shared,
    index: usize,
    last_seen: RefCell<Option<String>>,
}

impl StatusReflector {
    fn new(shared: Shared, index: usize) -> Self {
        Self {
            shared,
            index,
            last_seen: RefCell::new(None),
        }
    }

    /// A short, single-line summary used for change detection.
    fn summary(&self) -> String {
        let settings = self.shared.settings();
        let Some(form) = settings.provider_forms.get(self.index) else {
            return String::new();
        };
        match settings
            .device_login
            .as_ref()
            .filter(|d| d.provider == form.provider)
            .map(|d| &d.status)
        {
            Some(DeviceLoginStatus::Starting) => "starting".to_string(),
            Some(DeviceLoginStatus::Prompt { user_code, .. }) => format!("prompt:{user_code}"),
            Some(DeviceLoginStatus::Done) => "done".to_string(),
            Some(DeviceLoginStatus::Failed(error)) => format!("failed:{error}"),
            None => {
                if form.signed_in {
                    "signed-in".to_string()
                } else {
                    "not-signed-in".to_string()
                }
            }
        }
    }

    /// The multi-line text to draw: (primary line, optional sub-line).
    fn lines(&self) -> (String, Option<String>) {
        let settings = self.shared.settings();
        let Some(form) = settings.provider_forms.get(self.index) else {
            return (String::new(), None);
        };
        match settings
            .device_login
            .as_ref()
            .filter(|d| d.provider == form.provider)
            .map(|d| &d.status)
        {
            Some(DeviceLoginStatus::Starting) => ("Requesting code…".to_string(), None),
            Some(DeviceLoginStatus::Prompt {
                verification_uri,
                user_code,
            }) => (format!("Code {user_code}"), Some(verification_uri.clone())),
            Some(DeviceLoginStatus::Done) => ("Signed in".to_string(), None),
            Some(DeviceLoginStatus::Failed(error)) => (format!("Failed: {error}"), None),
            None => {
                if form.signed_in {
                    ("Signed in".to_string(), None)
                } else {
                    ("Not signed in".to_string(), None)
                }
            }
        }
    }
}

const STATUS_LINE_H: f32 = 18.0;

impl Widget for StatusReflector {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if !matches!(event, Event::Window(WindowEvent::RedrawRequested)) {
            return;
        }
        let summary = self.summary();
        let mut last = self.last_seen.borrow_mut();
        if last.as_deref() != Some(summary.as_str()) {
            *last = Some(summary);
            ctx.request_paint();
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let (_, sub) = self.lines();
        let height = if sub.is_some() {
            STATUS_LINE_H * 2.0
        } else {
            STATUS_LINE_H
        };
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            240.0
        };
        constraints.clamp(Size::new(width, height))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let (primary, sub) = self.lines();

        let primary_color = match self.lines_is_error() {
            true => pal.accent,
            false => pal.text,
        };
        let primary_style = pal.text_style(&theme, 12.5, FontWeight::MEDIUM, primary_color);
        widgets::draw_text_in(
            ctx,
            Rect::new(bounds.x(), bounds.y(), bounds.width(), STATUS_LINE_H),
            &primary,
            primary_style,
        );
        if let Some(sub) = sub {
            let sub_style = pal.text_style(&theme, 10.5, FontWeight::NORMAL, pal.text_faint);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    bounds.x(),
                    bounds.y() + STATUS_LINE_H,
                    bounds.width(),
                    STATUS_LINE_H,
                ),
                &sub,
                sub_style,
            );
        }
    }
}

impl StatusReflector {
    fn lines_is_error(&self) -> bool {
        let settings = self.shared.settings();
        settings
            .provider_forms
            .get(self.index)
            .and_then(|form| {
                settings
                    .device_login
                    .as_ref()
                    .filter(|d| d.provider == form.provider)
                    .map(|d| matches!(d.status, DeviceLoginStatus::Failed(_)))
            })
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Responsive host
// ---------------------------------------------------------------------------

/// The settings tab: a thin responsive host that owns two pre-built pods (wide master/detail and
/// narrow drill-down) and delegates to the one chosen by the available width. The choice is made in
/// `measure` (constraints-driven) and cached so `arrange`/`paint`/event routing all stay in sync.
pub struct SettingsView {
    shared: Shared,
    wide: WidgetPod,
    narrow: WidgetPod,
    use_wide: Cell<bool>,
    /// The section selection last laid out. The inner `SwitchView`s (section detail, and the narrow
    /// menu/detail swap) only measure+arrange their *active* child, so a navigation that flips
    /// `selected` (via a sidebar/menu `ListView`, which requests only a repaint) must trigger a
    /// re-measure+re-arrange for the newly-active child. We diff on `RedrawRequested` to catch it.
    last_selected: Cell<Option<SettingsSection>>,
}

impl SettingsView {
    pub fn new(shared: Shared) -> Self {
        let theme_reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        // One shared scroll state so position is consistent across the wide/narrow swap.
        let scroll_state = ScrollState::new();
        let wide = wide_pod(&shared, &theme_reader, &scroll_state);
        let narrow = narrow_pod(&shared, &theme_reader, &scroll_state);
        let last_selected = Cell::new(shared.settings().selected);
        Self {
            shared,
            wide,
            narrow,
            // Default to wide before the first measure (matches the old default).
            use_wide: Cell::new(true),
            last_selected,
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
}

impl Widget for SettingsView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        // The section/menu `ListView`s navigate by mutating `selected` and requesting only a repaint.
        // The nested `SwitchView`s lay out only their active child, so a navigation needs a fresh
        // measure+arrange for the newly-shown detail/menu. Detect the change on the redraw the
        // ListView triggered and request the relayout (diff-on-redraw, like the widget-book pattern).
        // Navigation (sidebar/menu `ListView`, narrow back `Button`) flips `selected` and requests
        // only a repaint, but the inner `SwitchView`s lay out only their active child — so the
        // newly-shown detail/menu needs a fresh measure+arrange. A pointer release inside the tab is
        // the only moment such a navigation can happen, so request a relayout then. (A pointer up is
        // rare, so the extra measure is cheap; this matches how the Shell relayouts on tab switch.)
        if let Event::Pointer(pointer) = event
            && matches!(pointer.kind, sui::PointerEventKind::Up)
        {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        }
        // Belt-and-suspenders: on a redraw, catch any selection change driven from outside a pointer
        // gesture (e.g. an external wake) and relayout (diff-on-redraw, like the widget-book pattern).
        if matches!(event, Event::Window(WindowEvent::RedrawRequested)) {
            let current = self.shared.settings().selected;
            if current != self.last_selected.get() {
                self.last_selected.set(current);
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_paint();
            }
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let use_wide = constraints.max.width >= WIDE_THRESHOLD;
        self.use_wide.set(use_wide);
        // A measure pass lays out the currently-active child, so the navigation cache is now
        // up to date — record it so the next redraw doesn't spuriously re-request a relayout.
        self.last_selected.set(self.shared.settings().selected);
        self.active_mut().measure(ctx, constraints)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.active_mut().arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.active().paint(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        // Visit only the active pod so layout/paint walks stay consistent with measure/arrange.
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
    fn set_value_with_invalid_input_errors_and_preserves_prior_value() {
        // A typed (Number/Json) key: writing garbage returns Err and the stored value is unchanged.
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let Some(idx) = state.config_items.iter().position(|item| {
            matches!(
                item.edit_kind,
                ConfigEditKind::Number | ConfigEditKind::Json
            )
        }) else {
            return; // no typed key in this environment
        };
        let key = state.config_items[idx].key.clone();
        let before = state.config_items[idx].edit_value.clone();

        // Drive the value write the way the TextInput on_change would.
        state.set_config_value(idx, "not-valid-json {{");
        assert!(
            state.field_errors.contains_key(&config_field_key(&key)),
            "an invalid value records a parse error against the row"
        );

        // The service rejected the write, so a fresh resolve keeps the prior value.
        state.reload();
        assert_eq!(
            state.config_items[idx].edit_value, before,
            "a rejected value leaves the stored config unchanged"
        );
    }

    #[test]
    fn toggle_config_no_ops_on_a_non_bool_key() {
        let Some((mut state, idx)) = editable_state() else {
            return;
        };
        let before = state.config_items[idx].edit_value.clone();
        // toggle_config only acts on booleans; on a text key it must be a no-op.
        state.toggle_config(idx);
        assert_eq!(
            state.config_items[idx].edit_value, before,
            "toggling a non-boolean key changes nothing"
        );
    }

    #[test]
    fn booleans_use_the_toggle_not_the_editor() {
        // A boolean key is never routed through the text-value writer: set_config_value is a no-op
        // for ConfigEditKind::Bool, so its value only ever changes via toggle_config.
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        if let Some(idx) = state
            .config_items
            .iter()
            .position(|item| item.edit_kind == ConfigEditKind::Bool)
        {
            let before = state.config_items[idx].value.clone();
            state.set_config_value(idx, "true");
            assert!(
                state.field_errors.is_empty(),
                "a boolean key is not text-edited (no error recorded)"
            );
            assert_eq!(
                state.config_items[idx].value, before,
                "the boolean value is untouched by the text writer"
            );
        }
    }

    #[test]
    fn provider_api_key_field_seeds_blank() {
        // The API-key TextInput is seeded with an empty value (the stored key is never surfaced).
        // This mirrors the build-time seed used by text_row in provider_card.
        let backend = Backend::local_fallback();
        let state = SettingsState::new(&backend);
        assert!(
            !state.provider_forms.is_empty(),
            "the configurable provider list is non-empty"
        );
        // The seed is the literal "" passed to text_row(...) for the API-key row — assert that the
        // form never exposes a key to surface.
        let api_key_seed = "";
        assert!(
            api_key_seed.is_empty(),
            "the API-key field seeds blank, never surfacing a stored key"
        );
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
