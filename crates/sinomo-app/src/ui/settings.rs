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
//! each assembled from stock `FormSection` / `FormRow` / `TabBar` / `Switch` /
//! `TextInput` / `Select` / `Button` / `ListView` / `ScrollView` widgets. State lives
//! entirely in [`SettingsState`]; controls bind to it via reactive `*_when` readers and
//! snapshot-seed-plus-`on_change` writeback. The async provider device-login status is
//! the one bespoke region — a live-reading [`StatusReflector`] widget.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use serde_json::Value;
use sui::{
    Alignment, ArrangeCtx, Color, Constraints, DefaultTheme, Event, EventCtx, FieldGroup,
    FontWeight, FormRow, FormSection, Label, ListItem, ListView, MeasureCtx, PaintCtx,
    PointerEventKind, Rect, ScrollState, ScrollView, Select, SemanticsCtx, Separator, Size,
    SizedBox, Stack, Switch, SwitchView, TabBar, TextArea, TextInput, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent,
};

use sinomo_runtime::config::ConfigClient;

use crate::backend::Backend;
use crate::backend::cluster::{AppClusterInvite, AppClusterStatus};
use crate::backend::config::{
    CONFIGURABLE_PROVIDERS, ConfigEditKind, ConfigItem, ProviderForm, REASONING_EFFORT_OPTIONS,
    add_provider_account, cluster_provider_summaries, cycle_provider_reasoning_effort,
    provider_default_model, provider_display_name, provider_forms, reasoning_effort_index,
    remove_provider_account, resolve_items, run_provider_device_login, set_bool,
    set_provider_api_key, set_provider_base_url, set_provider_effort, set_provider_enabled,
    set_provider_model, set_value,
};
use crate::rendering::AppRenderMode;
use crate::state::{Shared, Tab};
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
    pub cluster_id: Option<String>,
    pub cluster_summary: Option<String>,
    pub join_invite: String,
    pub invite_result: Option<AppClusterInvite>,
    pub cluster_action_error: Option<String>,
    pub cluster_action_status: Option<String>,
    pub leave_confirming: bool,
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
    pub provider_add_open: bool,
    pub provider_add_provider_index: usize,
    pub provider_add_account: String,
    pub provider_add_api_key: String,
    pub provider_add_model: String,
    pub provider_add_base_url: String,
    pub provider_add_error: Option<String>,
    pub provider_action_error: Option<String>,
    connection_detail_expanded: Vec<Tab>,
    node_detail_expanded: bool,
    advanced_detail_expanded: Vec<String>,
    provider_detail_expanded: Vec<String>,
    provider_storage_detail_expanded: bool,
    provider_remove_confirming: Option<(String, String)>,
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
    pub account: String,
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

        let cluster_status = backend.app_cluster_status();
        let (clustered, cluster_summary) = if cluster_status.clustered {
            (true, cluster_status.summary.clone())
        } else {
            match &backend.connection {
                crate::backend::Connection::Daemon(status)
                | crate::backend::Connection::InProcessDaemon(status) => {
                    let summary = status.cluster.as_ref().map(|cluster| {
                        format!(
                            "cluster {} · {:?}",
                            short(&format!("{:?}", cluster.cluster_id), 14),
                            cluster.member_state
                        )
                    });
                    (summary.is_some(), summary)
                }
                crate::backend::Connection::LocalFallback => (false, None),
            }
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
            clustered,
            cluster_id: cluster_status.cluster_id.clone(),
            cluster_summary,
            join_invite: String::new(),
            invite_result: None,
            cluster_action_error: cluster_status.error,
            cluster_action_status: None,
            leave_confirming: false,
            version: env!("CARGO_PKG_VERSION").to_string(),
            selected: None,
            field_errors: std::collections::HashMap::new(),
            device_login: None,
            provider_add_open: false,
            provider_add_provider_index: 0,
            provider_add_account: "default".to_string(),
            provider_add_api_key: String::new(),
            provider_add_model: provider_default_model(CONFIGURABLE_PROVIDERS[0]),
            provider_add_base_url: String::new(),
            provider_add_error: None,
            provider_action_error: None,
            connection_detail_expanded: Vec::new(),
            node_detail_expanded: false,
            advanced_detail_expanded: Vec::new(),
            provider_detail_expanded: Vec::new(),
            provider_storage_detail_expanded: false,
            provider_remove_confirming: None,
        }
    }

    /// Open a section's detail page.
    pub fn open(&mut self, section: SettingsSection) {
        self.selected = Some(section);
    }

    pub fn open_provider_add(&mut self) {
        self.selected = Some(SettingsSection::Providers);
        self.provider_add_open = true;
        self.provider_add_error = None;
        self.provider_action_error = None;
        self.provider_remove_confirming = None;
        if self.provider_add_model.trim().is_empty() {
            self.provider_add_model = provider_default_model(self.provider_add_provider());
        }
    }

    fn connection_detail_expanded(&self, tab: Tab) -> bool {
        self.connection_detail_expanded.contains(&tab)
    }

    fn toggle_connection_detail(&mut self, tab: Tab) {
        if let Some(index) = self
            .connection_detail_expanded
            .iter()
            .position(|expanded| *expanded == tab)
        {
            self.connection_detail_expanded.remove(index);
        } else {
            self.connection_detail_expanded.push(tab);
        }
    }

    fn node_detail_expanded(&self) -> bool {
        self.node_detail_expanded
    }

    fn toggle_node_detail(&mut self) {
        self.node_detail_expanded = !self.node_detail_expanded;
    }

    fn advanced_detail_expanded(&self, key: &str) -> bool {
        self.advanced_detail_expanded
            .iter()
            .any(|expanded| expanded == key)
    }

    fn toggle_advanced_detail(&mut self, key: &str) {
        if let Some(index) = self
            .advanced_detail_expanded
            .iter()
            .position(|expanded| expanded == key)
        {
            self.advanced_detail_expanded.remove(index);
        } else {
            self.advanced_detail_expanded.push(key.to_string());
        }
    }

    fn close_provider_add(&mut self) {
        self.provider_add_open = false;
        self.provider_add_error = None;
    }

    fn provider_remove_confirming(&self, provider: &str, account: &str) -> bool {
        self.provider_remove_confirming
            .as_ref()
            .is_some_and(|(p, a)| p == provider && a == account)
    }

    fn provider_detail_expanded(&self, provider: &str, account: &str) -> bool {
        let key = provider_account_key(provider, account);
        self.provider_detail_expanded
            .iter()
            .any(|expanded| expanded == &key)
    }

    fn toggle_provider_detail(&mut self, provider: &str, account: &str) {
        let key = provider_account_key(provider, account);
        if let Some(index) = self
            .provider_detail_expanded
            .iter()
            .position(|expanded| expanded == &key)
        {
            self.provider_detail_expanded.remove(index);
        } else {
            self.provider_detail_expanded.push(key);
        }
    }

    fn provider_storage_detail_expanded(&self) -> bool {
        self.provider_storage_detail_expanded
    }

    fn toggle_provider_storage_detail(&mut self) {
        self.provider_storage_detail_expanded = !self.provider_storage_detail_expanded;
    }

    fn confirm_provider_remove(&mut self, index: usize) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        self.provider_action_error = None;
        self.provider_remove_confirming = Some((form.provider.clone(), form.account.clone()));
    }

    fn cancel_provider_remove(&mut self) {
        self.provider_remove_confirming = None;
        self.provider_action_error = None;
    }

    fn provider_add_provider(&self) -> &'static str {
        CONFIGURABLE_PROVIDERS
            .get(self.provider_add_provider_index)
            .copied()
            .unwrap_or(CONFIGURABLE_PROVIDERS[0])
    }

    fn provider_add_uses_device_login(&self) -> bool {
        sinomo_cli::provider_uses_device_login(self.provider_add_provider())
    }

    fn provider_add_can_submit(&self) -> bool {
        self.provider_add_uses_device_login() || !self.provider_add_api_key.trim().is_empty()
    }

    fn set_provider_add_provider(&mut self, index: usize) {
        if index >= CONFIGURABLE_PROVIDERS.len() {
            return;
        }
        self.provider_add_provider_index = index;
        self.provider_add_model = provider_default_model(self.provider_add_provider());
        self.provider_add_api_key.clear();
        self.provider_add_error = None;
    }

    fn set_provider_add_account(&mut self, value: String) {
        self.provider_add_account = value;
        self.provider_add_error = None;
    }

    fn set_provider_add_api_key(&mut self, value: String) {
        self.provider_add_api_key = value;
        self.provider_add_error = None;
    }

    fn set_provider_add_model(&mut self, value: String) {
        self.provider_add_model = value;
        self.provider_add_error = None;
    }

    fn set_provider_add_base_url(&mut self, value: String) {
        self.provider_add_base_url = value;
        self.provider_add_error = None;
    }

    fn add_provider_from_form(&mut self) -> Result<(String, String, bool), String> {
        let provider = self.provider_add_provider().to_string();
        let account = self.provider_add_account.trim().to_string();
        let uses_device_login = self.provider_add_uses_device_login();
        match add_provider_account(
            self.config.as_ref(),
            &provider,
            &account,
            &self.provider_add_api_key,
            &self.provider_add_model,
            &self.provider_add_base_url,
        ) {
            Ok(()) => {
                let account = sinomo_lm::normalize_provider_account_name(Some(&account));
                self.close_provider_add();
                self.provider_action_error = None;
                self.provider_remove_confirming = None;
                self.provider_add_account = "default".to_string();
                self.provider_add_api_key.clear();
                self.provider_add_base_url.clear();
                self.provider_add_model = provider_default_model(self.provider_add_provider());
                self.reload();
                Ok((provider, account, uses_device_login))
            }
            Err(error) => {
                self.provider_add_error = Some(error.clone());
                self.provider_action_error = None;
                Err(error)
            }
        }
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

    fn set_join_invite(&mut self, value: String) {
        self.join_invite = value;
        self.cluster_action_error = None;
        self.cluster_action_status = None;
    }

    fn apply_cluster_status(&mut self, status: AppClusterStatus) {
        let previous_cluster_id = self.cluster_id.clone();
        self.clustered = status.clustered;
        self.cluster_id = status.cluster_id;
        self.cluster_summary = status.summary;
        self.cluster_action_error = status.error;
        self.leave_confirming = false;
        if self.clustered {
            self.join_invite.clear();
            if previous_cluster_id != self.cluster_id {
                self.invite_result = None;
            }
        } else {
            self.invite_result = None;
        }
    }

    fn set_cluster_action_status(&mut self, message: impl Into<String>) {
        self.cluster_action_status = Some(message.into());
        self.cluster_action_error = None;
    }

    fn set_cluster_action_error(&mut self, error: impl Into<String>) {
        self.cluster_action_error = Some(error.into());
        self.cluster_action_status = None;
    }

    fn confirm_leave_cluster(&mut self) {
        self.leave_confirming = true;
        self.cluster_action_error = None;
        self.cluster_action_status = None;
    }

    fn cancel_leave_cluster(&mut self) {
        self.leave_confirming = false;
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

    fn toggle_provider(&mut self, index: usize) -> bool {
        let Some(form) = self.provider_forms.get(index) else {
            return false;
        };
        let provider = form.provider.clone();
        let account = form.account.clone();
        let enabled = !form.enabled;
        if set_provider_enabled(self.config.as_ref(), &provider, &account, enabled).is_ok() {
            self.provider_action_error = None;
            self.provider_remove_confirming = None;
            self.reload();
            return true;
        }
        false
    }

    fn remove_provider(&mut self, index: usize) -> bool {
        let Some(form) = self.provider_forms.get(index) else {
            return false;
        };
        let provider = form.provider.clone();
        let account = form.account.clone();
        match remove_provider_account(self.config.as_ref(), &provider, &account) {
            Ok(()) => {
                if self
                    .device_login
                    .as_ref()
                    .is_some_and(|login| login.provider == provider && login.account == account)
                {
                    self.device_login = None;
                }
                self.provider_action_error = None;
                self.provider_remove_confirming = None;
                self.reload();
                true
            }
            Err(error) => {
                self.provider_action_error = Some(error);
                false
            }
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
        let account = form.account.clone();
        let result = match field {
            ProviderField::ApiKey => {
                set_provider_api_key(self.config.as_ref(), &provider, &account, raw)
            }
            ProviderField::Model => {
                set_provider_model(self.config.as_ref(), &provider, &account, raw)
            }
            ProviderField::BaseUrl => {
                set_provider_base_url(self.config.as_ref(), &provider, &account, raw)
            }
        };
        let row = provider_field_key(&provider, &account, field);
        match result {
            Ok(()) => {
                self.field_errors.remove(&row);
                self.provider_action_error = None;
                self.provider_remove_confirming = None;
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
        let account = form.account.clone();
        if cycle_provider_reasoning_effort(self.config.as_ref(), &provider, &account).is_ok() {
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
        let account = form.account.clone();
        if set_provider_effort(self.config.as_ref(), &provider, &account, effort_index).is_ok() {
            self.reload();
        }
    }

    /// Start a provider's OAuth device-login flow on a background thread. Progress + the result
    /// arrive via a channel drained on the UI wake (`pump_device_login`); `waker` lets the worker
    /// refresh the UI as it proceeds.
    fn begin_device_login(&mut self, index: usize, waker: Option<sui::UiHandle>) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        if !form.uses_device_login {
            return;
        }
        let provider = form.provider.clone();
        let account = form.account.clone();
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
                account,
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
fn provider_field_key(provider: &str, account: &str, field: ProviderField) -> String {
    let suffix = match field {
        ProviderField::ApiKey => "api_key",
        ProviderField::Model => "model",
        ProviderField::BaseUrl => "base_url",
    };
    format!("provider:{provider}:{account}:{suffix}")
}

fn provider_account_key(provider: &str, account: &str) -> String {
    format!("{provider}:{account}")
}

/// A short, right-aligned value preview for a section, shown in the narrow menu rows.
pub fn section_preview(shared: &Shared, section: SettingsSection) -> String {
    match section {
        SettingsSection::Appearance => {
            let app = shared.borrow();
            format!("{} · {}", app.theme_mode.label(), app.render_mode.label())
        }
        SettingsSection::Connection => {
            if connection_issue_count(shared) > 0 {
                "Needs attention".to_string()
            } else {
                shared.settings().mode_label.clone()
            }
        }
        SettingsSection::Node => shared.settings().role.clone(),
        SettingsSection::Cluster => {
            let settings = shared.settings();
            if settings.clustered {
                "Connected".to_string()
            } else {
                "Local".to_string()
            }
        }
        SettingsSection::Providers => {
            let settings = shared.settings();
            if settings.provider_forms.is_empty() {
                "Not set up".to_string()
            } else {
                let enabled = settings.provider_forms.iter().filter(|f| f.enabled).count();
                format!("{enabled} enabled")
            }
        }
        SettingsSection::Configuration => {
            let settings = shared.settings();
            if settings.config_error.is_some() {
                "Unavailable".to_string()
            } else {
                format!("{} keys", settings.config_items.len())
            }
        }
        SettingsSection::About => format!("v{}", shared.settings().version),
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

fn mix_color(from: Color, to: Color, amount: f32) -> Color {
    let amount = amount.clamp(0.0, 1.0);
    Color::new(
        from.space,
        from.red + (to.red - from.red) * amount,
        from.green + (to.green - from.green) * amount,
        from.blue + (to.blue - from.blue) * amount,
        from.alpha + (to.alpha - from.alpha) * amount,
    )
    .clamped()
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
) -> FormRow {
    FormRow::new(label, muted_label(shared, value))
        .inline()
        .theme_when(clone_reader(reader))
}

#[derive(Clone)]
struct ConnectionIssue {
    title: &'static str,
    detail: String,
    tab: Tab,
    action: &'static str,
}

fn connection_issues(shared: &Shared) -> Vec<ConnectionIssue> {
    let app = shared.borrow();
    let Some(ready) = app.ready.as_ref() else {
        return Vec::new();
    };

    let mut issues = Vec::new();
    if let Some(detail) = ready.chat.connection_attention_detail_full() {
        issues.push(ConnectionIssue {
            title: "Chat sessions",
            detail,
            tab: Tab::Sinomo,
            action: "Open Chat",
        });
    }
    if let Some(error) = ready.files.error.as_deref().filter(|text| !text.is_empty()) {
        issues.push(ConnectionIssue {
            title: "SIFS browser",
            detail: format!("SIFS unavailable: {error}"),
            tab: Tab::Files,
            action: "Open Files",
        });
    }

    issues
}

fn connection_issue_count(shared: &Shared) -> usize {
    connection_issues(shared).len()
}

fn connection_issue_key(shared: &Shared) -> String {
    let issue_key = connection_issues(shared)
        .into_iter()
        .map(|issue| format!("{}:{}", issue.title, issue.detail))
        .collect::<Vec<_>>()
        .join("\n");
    let detail_key = {
        let settings = shared.settings();
        settings
            .connection_detail_expanded
            .iter()
            .map(|tab| tab.title())
            .collect::<Vec<_>>()
            .join(",")
    };
    format!("{issue_key}\nexpanded:{detail_key}")
}

fn node_detail_key(shared: &Shared) -> String {
    let settings = shared.settings();
    format!(
        "{}\n{}\n{}\n{}\n{}\n{:?}\nexpanded:{}",
        settings.node_id,
        settings.role,
        settings.mode_label,
        settings.status_detail,
        settings.clustered,
        settings.cluster_summary,
        settings.node_detail_expanded
    )
}

fn cluster_section_key(shared: &Shared) -> String {
    let settings = shared.settings();
    let invite_key = settings
        .invite_result
        .as_ref()
        .map(|invite| {
            format!(
                "{}:{}:{}:{}",
                invite.uri, invite.join_command, invite.expires_at, invite.persisted_key
            )
        })
        .unwrap_or_default();
    format!(
        "clustered:{};summary:{:?};invite:{invite_key};error:{:?};status:{:?};leave:{}",
        settings.clustered,
        settings.cluster_summary,
        settings.cluster_action_error,
        settings.cluster_action_status,
        settings.leave_confirming
    )
}

fn configuration_key(shared: &Shared) -> String {
    let settings = shared.settings();
    let item_key = settings
        .config_items
        .iter()
        .map(|item| {
            format!(
                "{}:{}:{:?}:{:?}:{}",
                item.key,
                item.source,
                item.edit_kind,
                item.bool_value,
                item.edit_value.len()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let expanded = settings
        .advanced_detail_expanded
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(",");
    format!("{item_key}\nexpanded:{expanded}")
}

fn connection_issue_summary(issue: &ConnectionIssue) -> &'static str {
    match issue.tab {
        Tab::Sinomo => "Chat history and agent channels need recovery.",
        Tab::Files => "SIFS browsing cannot open through the daemon.",
        _ => "A surface needs recovery.",
    }
}

fn connection_issue_recovery_hint(issue: &ConnectionIssue) -> &'static str {
    match issue.tab {
        Tab::Sinomo if connection_issue_has_sifs_recovery(issue) => {
            "Inspect SIFS access from Files, then return to chat."
        }
        Tab::Sinomo => "Return to chat after credentials are fixed.",
        Tab::Files => "Return to Files after credentials or SIFS access is fixed.",
        _ => "Return to the affected surface after the issue is fixed.",
    }
}

fn connection_issue_has_sifs_recovery(issue: &ConnectionIssue) -> bool {
    if issue.tab != Tab::Sinomo {
        return false;
    }
    let lower = issue.detail.to_ascii_lowercase();
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

fn open_tab(shared: &Shared, tab: Tab) {
    shared.borrow_mut().active_tab = tab;
}

fn tab_action_icon(tab: Tab) -> sui::IconGlyph {
    match tab {
        Tab::Sinomo => sui::IconGlyph::Chat,
        Tab::Files => sui::IconGlyph::Storage,
        _ => sui::IconGlyph::Alert,
    }
}

/// A stacked text field used to present generated values that users may copy.
fn copyable_text_row(
    reader: &ThemeReader,
    name: &str,
    label: &str,
    value: impl Into<String>,
) -> FormRow {
    let input = TextInput::new(name)
        .value(value)
        .read_only()
        .theme_when(clone_reader(reader));
    FormRow::new(label, input)
        .stacked()
        .theme_when(clone_reader(reader))
}

/// A stacked read-only text area for longer generated values.
fn copyable_area_row(
    reader: &ThemeReader,
    name: &str,
    label: &str,
    value: impl Into<String>,
) -> FormRow {
    let area = SizedBox::new().height(72.0).with_child(
        TextArea::new(name)
            .value(value)
            .read_only()
            .theme_when(clone_reader(reader)),
    );
    FormRow::new(label, area)
        .stacked()
        .theme_when(clone_reader(reader))
}

/// A toggle row: `label` + a `Switch` seeded `on`, calling `on_toggle` when flipped.
fn switch_row<F>(reader: &ThemeReader, label: &str, on: bool, on_toggle: F) -> FormRow
where
    F: FnMut(bool) + 'static,
{
    let switch = Switch::new("")
        .on(on)
        .theme_when(clone_reader(reader))
        .on_toggle(on_toggle);
    FormRow::new(label, switch)
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
) -> FormRow
where
    F: FnMut(String) + 'static,
{
    let input = TextInput::new(name)
        .value(value)
        .placeholder(placeholder)
        .theme_when(clone_reader(reader))
        .on_change(on_change);
    FormRow::new(label, input)
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
) -> FormRow
where
    S: Fn() -> Option<usize> + 'static,
    C: FnMut(usize, String) + 'static,
{
    let select = Select::new(name)
        .options(options.iter().copied())
        .selected_when(selected_when)
        .theme_when(clone_reader(reader))
        .on_change(on_change);
    FormRow::new(label, select)
        .inline()
        .theme_when(clone_reader(reader))
}

fn ready_backend(shared: &Shared) -> Option<Backend> {
    shared
        .borrow()
        .ready
        .as_ref()
        .map(|ready| ready.backend.clone())
}

fn refresh_chat_provider_summaries(shared: &Shared) {
    let mut app = shared.borrow_mut();
    if let Some(ready) = app.ready.as_mut() {
        let providers = cluster_provider_summaries(ready.settings.config.as_ref());
        ready.chat.refresh_providers(providers);
    }
}

fn apply_cluster_success(
    shared: &Shared,
    backend: &Backend,
    status: AppClusterStatus,
    message: impl Into<String>,
) {
    let snapshot = backend.cluster_snapshot();
    let mut app = shared.borrow_mut();
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.apply_cluster_status(status);
        ready.settings.set_cluster_action_status(message);
        ready.cluster.snapshot = snapshot;
    }
}

fn apply_cluster_invite_success(shared: &Shared, backend: &Backend, invite: AppClusterInvite) {
    let status = backend.app_cluster_status();
    let snapshot = backend.cluster_snapshot();
    let mut app = shared.borrow_mut();
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.apply_cluster_status(status);
        ready.settings.invite_result = Some(invite);
        ready
            .settings
            .set_cluster_action_status("Invitation generated");
        ready.cluster.snapshot = snapshot;
    }
}

fn apply_cluster_error(shared: &Shared, error: impl Into<String>) {
    let mut app = shared.borrow_mut();
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.set_cluster_action_error(error);
    }
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

fn index_to_render_mode(i: usize) -> AppRenderMode {
    match i {
        1 => AppRenderMode::ForceSdr,
        2 => AppRenderMode::PreferWideGamut,
        3 => AppRenderMode::PreferHdr,
        _ => AppRenderMode::Automatic,
    }
}

fn render_mode_to_index(mode: AppRenderMode) -> usize {
    match mode {
        AppRenderMode::Automatic => 0,
        AppRenderMode::ForceSdr => 1,
        AppRenderMode::PreferWideGamut => 2,
        AppRenderMode::PreferHdr => 3,
    }
}

fn render_mode_hint(mode: AppRenderMode) -> &'static str {
    match mode {
        AppRenderMode::Automatic => {
            "Uses the display's preferred color mode. Choose Force SDR when screenshots or remote captures look washed out or too bright."
        }
        AppRenderMode::ForceSdr => {
            "Recommended for debugging, screen sharing, and capture tools that do not handle HDR surfaces correctly."
        }
        AppRenderMode::PreferWideGamut => {
            "Uses wide-gamut output when supported while avoiding HDR brightness behavior."
        }
        AppRenderMode::PreferHdr => {
            "Requests HDR output when the display supports it; useful for final visual checks on HDR-capable screens."
        }
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
                s.set_theme_mode(index_to_mode(i));
            })
    };
    let render_select = {
        let s = shared.clone();
        Select::new("render-color-output")
            .options([
                AppRenderMode::Automatic.label(),
                AppRenderMode::ForceSdr.label(),
                AppRenderMode::PreferWideGamut.label(),
                AppRenderMode::PreferHdr.label(),
            ])
            .selected(render_mode_to_index(shared.render_mode()))
            .theme_when(clone_reader(reader))
            .on_change(move |index, _| {
                s.set_render_mode(index_to_render_mode(index));
            })
    };
    let theme_body = FieldGroup::new().spacing(6.0).with_child(
        FormRow::new("Theme", tab_bar)
            .inline()
            .theme_when(clone_reader(reader)),
    );
    let theme_section = FormSection::new("Appearance", theme_body)
        .description("Choose the app theme used by every tab.")
        .theme_when(clone_reader(reader));

    let current_render_mode = shared.render_mode();
    let render_body = FieldGroup::new()
        .spacing(8.0)
        .with_child(value_row(
            shared,
            reader,
            "Current mode",
            current_render_mode.label(),
        ))
        .with_child(
            FormRow::new("Color output", render_select)
                .inline()
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new(
                "Guidance",
                muted_label(shared, render_mode_hint(current_render_mode)),
            )
            .stacked()
            .theme_when(clone_reader(reader)),
        );
    let render_section = FormSection::new("Rendering", render_body)
        .description("Controls the desktop surface color space. This affects the window renderer, not cluster data or model output.")
        .theme_when(clone_reader(reader));

    Stack::vertical()
        .spacing(12.0)
        .alignment(Alignment::Stretch)
        .with_child(theme_section)
        .with_child(render_section)
}

fn section_connection(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (mode_label, status_detail) = {
        let s = shared.settings();
        (s.mode_label.clone(), s.status_detail.clone())
    };
    let body = FieldGroup::new()
        .spacing(6.0)
        .with_child(value_row(shared, reader, "Mode", mode_label))
        .with_child(value_row(shared, reader, "Status", status_detail));
    let connection = FormSection::new("Connection", body).theme_when(clone_reader(reader));

    Stack::vertical()
        .spacing(12.0)
        .alignment(Alignment::Stretch)
        .with_child(connection)
        .with_child(connection_attention_section(shared, reader))
}

fn connection_attention_section(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let issues = connection_issues(shared);
    let mut body = FieldGroup::new().spacing(8.0);
    if issues.is_empty() {
        body.push(value_row(
            shared,
            reader,
            "Status",
            "No active recovery issues",
        ));
    } else {
        for issue in issues {
            body.push(connection_issue_row(shared, reader, issue));
        }
    }

    let description = if connection_issue_count(shared) == 0 {
        "User-facing chat and SIFS surfaces are currently reachable.".to_string()
    } else {
        "These are user-visible issues. The daemon can be connected while a chat or file surface still needs recovery.".to_string()
    };
    FormSection::new("Attention", body)
        .description(description)
        .theme_when(clone_reader(reader))
}

fn connection_issue_row(shared: &Shared, reader: &ThemeReader, issue: ConnectionIssue) -> FormRow {
    let detail = issue.detail.clone();
    let tab = issue.tab;
    let action = issue.action;
    let expanded = shared.settings().connection_detail_expanded(tab);
    let status = {
        let s = shared.clone();
        Label::new("Needs attention")
            .font_size(12.0)
            .color_when(move || palette(&s).warn)
    };
    let summary = muted_label(shared, connection_issue_summary(&issue)).font_size(12.0);
    let recovery = muted_label(shared, connection_issue_recovery_hint(&issue)).font_size(12.0);
    let detail_label = {
        let s = shared.clone();
        Label::new("Low-level detail")
            .font_size(11.0)
            .color_when(move || palette(&s).text_faint)
    };
    let detail_field = {
        let name = match tab {
            Tab::Sinomo => "connection-chat-detail",
            Tab::Files => "connection-files-detail",
            _ => "connection-issue-detail",
        };
        SizedBox::new().height(68.0).with_child(
            TextArea::new(name)
                .value(detail)
                .read_only()
                .theme_when(clone_reader(reader)),
        )
    };
    let button = {
        let s = shared.clone();
        sui::Button::new(action)
            .icon(tab_action_icon(tab))
            .theme_when(clone_reader(reader))
            .on_press(move || open_tab(&s, tab))
    };
    let sifs_button = connection_issue_has_sifs_recovery(&issue).then(|| {
        let s = shared.clone();
        sui::Button::new("Check SIFS")
            .icon(tab_action_icon(Tab::Files))
            .theme_when(clone_reader(reader))
            .on_press(move || open_tab(&s, Tab::Files))
    });
    let detail_button = {
        let s = shared.clone();
        sui::Button::new(if expanded {
            "Hide detail"
        } else {
            "Show detail"
        })
        .icon(if expanded {
            sui::IconGlyph::ChevronUp
        } else {
            sui::IconGlyph::ChevronDown
        })
        .theme_when(secondary_button_theme(reader))
        .on_press(move || s.settings_mut().toggle_connection_detail(tab))
    };
    let header = Stack::vertical()
        .spacing(3.0)
        .alignment(Alignment::Stretch)
        .with_child(status)
        .with_child(summary);
    let mut controls = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Start)
        .with_child(button);
    if let Some(sifs_button) = sifs_button {
        controls = controls.with_child(sifs_button);
    }
    controls = controls.with_child(detail_button);
    let action_row = Stack::vertical()
        .spacing(5.0)
        .alignment(Alignment::Start)
        .with_child(controls)
        .with_child(recovery);
    let mut stack = Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(header)
        .with_child(action_row);
    if expanded {
        stack = stack.with_child(detail_label).with_child(detail_field);
    }

    FormRow::new(issue.title, stack)
        .stacked()
        .theme_when(clone_reader(reader))
}

fn section_node(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || node_detail_key(&s)
        },
        move |_sig| WidgetPod::new(section_node_body(&s, &r)),
    )
}

fn section_node_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (short_node_id, full_node_id, role, mode_label, status_detail, cluster_summary, expanded) = {
        let s = shared.settings();
        (
            short(&s.node_id, 30),
            s.node_id.clone(),
            s.role.clone(),
            s.mode_label.clone(),
            s.status_detail.clone(),
            s.cluster_summary.clone(),
            s.node_detail_expanded(),
        )
    };
    let detail_button = {
        let s = shared.clone();
        sui::Button::new(if expanded {
            "Hide detail"
        } else {
            "Show detail"
        })
        .icon(if expanded {
            sui::IconGlyph::ChevronUp
        } else {
            sui::IconGlyph::ChevronDown
        })
        .theme_when(secondary_button_theme(reader))
        .on_press(move || s.settings_mut().toggle_node_detail())
    };

    let mut body = FieldGroup::new()
        .spacing(6.0)
        .with_child(value_row(shared, reader, "Node ID", short_node_id))
        .with_child(value_row(shared, reader, "Role", role))
        .with_child(
            FormRow::new("Details", Stack::horizontal().with_child(detail_button))
                .inline()
                .theme_when(clone_reader(reader)),
        );
    if expanded {
        body = body
            .with_child(copyable_text_row(
                reader,
                "node-full-id",
                "Full node ID",
                full_node_id,
            ))
            .with_child(value_row(shared, reader, "Connection", mode_label))
            .with_child(copyable_area_row(
                reader,
                "node-connection-detail",
                "Connection detail",
                status_detail,
            ));
        if let Some(summary) = cluster_summary {
            body = body.with_child(value_row(shared, reader, "Cluster", summary));
        }
    }
    FormSection::new("Node", body).theme_when(clone_reader(reader))
}

fn section_cluster(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || cluster_section_key(&s)
        },
        move |_key| WidgetPod::new(section_cluster_body(&s, &r)),
    )
}

fn section_cluster_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (clustered, cluster_summary, invite_result, action_error, action_status, leave_confirming) = {
        let settings = shared.settings();
        (
            settings.clustered,
            settings
                .cluster_summary
                .clone()
                .unwrap_or_else(|| "connected".to_string()),
            settings.invite_result.clone(),
            settings.cluster_action_error.clone(),
            settings.cluster_action_status.clone(),
            settings.leave_confirming,
        )
    };

    let mut body = FieldGroup::new().spacing(8.0);
    if clustered {
        body = body.with_child(value_row(shared, reader, "Cluster", cluster_summary));

        let generate = {
            let s = shared.clone();
            sui::Button::new("Generate invitation")
                .icon(sui::IconGlyph::Link)
                .theme_when(clone_reader(reader))
                .on_press(move || {
                    let Some(backend) = ready_backend(&s) else {
                        apply_cluster_error(&s, "backend is not ready");
                        return;
                    };
                    match backend.create_cluster_invite() {
                        Ok(invite) => apply_cluster_invite_success(&s, &backend, invite),
                        Err(error) => apply_cluster_error(&s, error),
                    }
                })
        };
        body = body.with_child(
            FormRow::new("Invite", generate)
                .inline()
                .theme_when(clone_reader(reader)),
        );

        if let Some(invite) = invite_result {
            body = body
                .with_child(copyable_text_row(
                    reader,
                    "cluster-invite-link",
                    "Join link",
                    invite.uri,
                ))
                .with_child(copyable_text_row(
                    reader,
                    "cluster-invite-command",
                    "Join command",
                    invite.join_command,
                ))
                .with_child(value_row(shared, reader, "Expires", invite.expires_at))
                .with_child(value_row(shared, reader, "Saved key", invite.persisted_key));
        }

        if leave_confirming {
            let confirm = {
                let s = shared.clone();
                sui::Button::new("Confirm leave")
                    .icon(sui::IconGlyph::Trash)
                    .theme_when(danger_button_theme(reader))
                    .on_press(move || {
                        let Some(backend) = ready_backend(&s) else {
                            apply_cluster_error(&s, "backend is not ready");
                            return;
                        };
                        match backend.leave_cluster() {
                            Ok(status) => {
                                apply_cluster_success(
                                    &s,
                                    &backend,
                                    status,
                                    "Started single-node cluster",
                                );
                            }
                            Err(error) => apply_cluster_error(&s, error),
                        }
                    })
            };
            let cancel = {
                let s = shared.clone();
                sui::Button::new("Cancel")
                    .icon(sui::IconGlyph::Close)
                    .theme_when(secondary_button_theme(reader))
                    .on_press(move || {
                        s.settings_mut().cancel_leave_cluster();
                    })
            };
            let controls = Stack::vertical()
                .spacing(8.0)
                .alignment(Alignment::Stretch)
                .with_child(muted_label(
                    shared,
                    "This leaves the current cluster and starts a fresh single-node cluster on this device.",
                ))
                .with_child(
                    Stack::horizontal()
                        .spacing(8.0)
                        .alignment(Alignment::Start)
                        .with_child(confirm)
                        .with_child(cancel),
                );
            body = body.with_child(
                FormRow::new("Leave cluster", controls)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            );
        } else {
            let leave = {
                let s = shared.clone();
                sui::Button::new("Leave cluster")
                    .icon(sui::IconGlyph::Trash)
                    .theme_when(danger_button_theme(reader))
                    .on_press(move || {
                        s.settings_mut().confirm_leave_cluster();
                    })
            };
            body = body.with_child(
                FormRow::new("Leave", leave)
                    .inline()
                    .theme_when(clone_reader(reader)),
            );
        }
    } else {
        body = body
            .with_child(value_row(shared, reader, "State", "running locally"))
            .with_child(value_row(shared, reader, "Nodes", "this device only"));

        let join_on_change = {
            let s = shared.clone();
            move |value: String| {
                s.settings_mut().set_join_invite(value);
            }
        };
        let join_value = shared.settings().join_invite.clone();
        body = body.with_child(text_row(
            reader,
            "cluster-join-link",
            "Join link",
            join_value,
            "sinomo cluster join ...",
            join_on_change,
        ));

        let join = {
            let s = shared.clone();
            sui::Button::new("Join cluster")
                .icon(sui::IconGlyph::Check)
                .theme_when(clone_reader(reader))
                .enabled_when({
                    let s = s.clone();
                    move || !s.settings().join_invite.trim().is_empty()
                })
                .on_press(move || {
                    let invite = s.settings().join_invite.clone();
                    if invite.trim().is_empty() {
                        apply_cluster_error(&s, "enter an invitation link before joining");
                        return;
                    }
                    let Some(backend) = ready_backend(&s) else {
                        apply_cluster_error(&s, "backend is not ready");
                        return;
                    };
                    match backend.join_cluster(&invite) {
                        Ok(status) => apply_cluster_success(&s, &backend, status, "Joined cluster"),
                        Err(error) => apply_cluster_error(&s, error),
                    }
                })
        };
        body = body.with_child(
            FormRow::new("Join", join)
                .inline()
                .theme_when(clone_reader(reader)),
        );
    }

    if let Some(error) = action_error {
        body = body.with_child(value_row(shared, reader, "Error", error));
    } else if let Some(status) = action_status {
        body = body.with_child(value_row(shared, reader, "Status", status));
    }

    FormSection::new("Cluster", body).theme_when(clone_reader(reader))
}

fn section_about(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (version, mode_label, status_detail, node_id, role, cluster_summary) = {
        let settings = shared.settings();
        (
            settings.version.clone(),
            settings.mode_label.clone(),
            short_one_line(&settings.status_detail, 96),
            short(&settings.node_id, 38),
            settings.role.clone(),
            settings
                .cluster_summary
                .clone()
                .map(|summary| short_one_line(&summary, 96))
                .unwrap_or_else(|| {
                    if settings.clustered {
                        "cluster status unavailable".to_string()
                    } else {
                        "not clustered".to_string()
                    }
                }),
        )
    };
    let build = FieldGroup::new()
        .spacing(6.0)
        .with_child(value_row(
            shared,
            reader,
            "Sinomo App",
            format!("v{version}"),
        ))
        .with_child(value_row(shared, reader, "Toolkit", "SUI"));
    let runtime = FieldGroup::new()
        .spacing(6.0)
        .with_child(value_row(shared, reader, "Mode", mode_label))
        .with_child(value_row(shared, reader, "Status", status_detail))
        .with_child(value_row(shared, reader, "Node", node_id))
        .with_child(value_row(shared, reader, "Role", role))
        .with_child(value_row(shared, reader, "Cluster", cluster_summary));
    Stack::vertical()
        .spacing(14.0)
        .alignment(Alignment::Stretch)
        .with_child(
            FormSection::new("About", build)
                .description("Build information for this desktop app.")
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormSection::new("Runtime", runtime)
                .description("Current connection and node identity for support and diagnostics.")
                .theme_when(clone_reader(reader)),
        )
}

/// One provider's card: summary rows first, low-level credential and management controls behind a
/// details disclosure.
fn provider_card(shared: &Shared, reader: &ThemeReader, index: usize) -> impl Widget + use<> {
    let form = shared.settings().provider_forms[index].clone();
    let expanded = shared
        .settings()
        .provider_detail_expanded(&form.provider, &form.account);

    // Enable toggle as the panel's header action.
    let enable_switch = {
        let s = shared.clone();
        Switch::new("")
            .on(form.enabled)
            .theme_when(clone_reader(reader))
            .on_toggle(move |_| {
                let changed = { s.settings_mut().toggle_provider(index) };
                if changed {
                    refresh_chat_provider_summaries(&s);
                }
            })
    };

    let mut body = FieldGroup::new().spacing(8.0);
    body = body.with_child(value_row(
        shared,
        reader,
        "Account",
        form.account_label.clone(),
    ));
    body = body.with_child(value_row(
        shared,
        reader,
        "Authentication",
        provider_auth_summary(&form),
    ));

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

    let detail_button = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        sui::Button::new(if expanded {
            "Hide details"
        } else {
            "Show details"
        })
        .icon(if expanded {
            sui::IconGlyph::ChevronUp
        } else {
            sui::IconGlyph::ChevronDown
        })
        .theme_when(secondary_button_theme(reader))
        .on_press(move || {
            s.settings_mut().toggle_provider_detail(&provider, &account);
        })
    };
    body = body.with_child(
        FormRow::new("Details", Stack::horizontal().with_child(detail_button))
            .inline()
            .theme_when(clone_reader(reader)),
    );

    if expanded {
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
                .with_child(StatusReflector::new(shared.clone(), index, reader))
                .with_child(button);
            body = body.with_child(
                FormRow::new("Authentication detail", auth)
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
        body = body.with_child(provider_management_row(shared, reader, index, &form));
    }

    FormSection::new(form.display_name.clone(), body)
        .header_action(enable_switch)
        .theme_when(clone_reader(reader))
}

fn provider_auth_summary(form: &ProviderForm) -> &'static str {
    if form.uses_device_login {
        if form.signed_in {
            "Signed in"
        } else {
            "Sign-in required"
        }
    } else if form.has_api_key {
        "API key configured"
    } else {
        "API key required"
    }
}

fn provider_management_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    form: &ProviderForm,
) -> FormRow {
    let confirming = shared
        .settings()
        .provider_remove_confirming(&form.provider, &form.account);
    let action_error = shared.settings().provider_action_error.clone();
    if confirming {
        let cancel = {
            let s = shared.clone();
            sui::Button::new("Cancel")
                .icon(sui::IconGlyph::Close)
                .theme_when(secondary_button_theme(reader))
                .on_press(move || s.settings_mut().cancel_provider_remove())
        };
        let remove = {
            let s = shared.clone();
            sui::Button::new("Remove account")
                .icon(sui::IconGlyph::Trash)
                .theme_when(danger_button_theme(reader))
                .on_press(move || {
                    let removed = { s.settings_mut().remove_provider(index) };
                    if removed {
                        refresh_chat_provider_summaries(&s);
                    }
                })
        };
        let controls = Stack::horizontal()
            .spacing(8.0)
            .alignment(Alignment::Start)
            .with_child(cancel)
            .with_child(remove);
        let mut stack = Stack::vertical()
            .spacing(6.0)
            .alignment(Alignment::Start)
            .with_child(muted_label(
                shared,
                "This removes the provider account from lm.providers.",
            ))
            .with_child(controls);
        if let Some(error) = action_error {
            stack = stack.with_child(Label::new(error).font_size(11.0).color_when({
                let s = shared.clone();
                move || palette(&s).accent
            }));
        }
        FormRow::new("Management", stack)
            .stacked()
            .theme_when(clone_reader(reader))
    } else {
        let remove = {
            let s = shared.clone();
            sui::Button::new("Remove account")
                .icon(sui::IconGlyph::Trash)
                .theme_when(danger_button_theme(reader))
                .on_press(move || s.settings_mut().confirm_provider_remove(index))
        };
        FormRow::new("Management", remove)
            .inline()
            .theme_when(clone_reader(reader))
    }
}

fn section_providers(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || provider_section_signature(&s)
        },
        move |_sig| provider_section_body(&s, &r),
    )
}

fn provider_section_body(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    if shared.settings().provider_add_open {
        WidgetPod::new(provider_add_view(shared, reader))
    } else {
        WidgetPod::new(provider_list_view(shared, reader))
    }
}

fn provider_section_signature(shared: &Shared) -> String {
    let settings = shared.settings();
    let mut parts = vec![
        format!("add:{}", settings.provider_add_open),
        format!("add-provider:{}", settings.provider_add_provider_index),
        format!("error:{:?}", settings.provider_add_error),
        format!("action-error:{:?}", settings.provider_action_error),
        format!("remove:{:?}", settings.provider_remove_confirming),
        format!("expanded:{:?}", settings.provider_detail_expanded),
        format!(
            "storage-expanded:{}",
            settings.provider_storage_detail_expanded
        ),
    ];
    parts.extend(settings.provider_forms.iter().map(|form| {
        format!(
            "{}@{}:{}:{}:{}",
            form.provider, form.account, form.enabled, form.signed_in, form.display_name
        )
    }));
    parts.join("\u{1f}")
}

fn provider_list_view(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let mut stack = Stack::vertical()
        .spacing(14.0)
        .alignment(Alignment::Stretch);
    let add = {
        let s = shared.clone();
        sui::Button::new("Add provider")
            .icon(sui::IconGlyph::Add)
            .theme_when(clone_reader(reader))
            .on_press(move || {
                s.settings_mut().open_provider_add();
            })
    };
    let body = if shared.settings().provider_forms.is_empty() {
        let storage_expanded = shared.settings().provider_storage_detail_expanded();
        let storage_button = {
            let s = shared.clone();
            sui::Button::new(if storage_expanded {
                "Hide storage detail"
            } else {
                "Show storage detail"
            })
            .icon(if storage_expanded {
                sui::IconGlyph::ChevronUp
            } else {
                sui::IconGlyph::ChevronDown
            })
            .theme_when(secondary_button_theme(reader))
            .on_press(move || s.settings_mut().toggle_provider_storage_detail())
        };
        let mut group = FieldGroup::new()
            .spacing(8.0)
            .with_child(value_row(
                shared,
                reader,
                "Status",
                "No provider accounts configured",
            ))
            .with_child(value_row(
                shared,
                reader,
                "Chat",
                "Add a provider account to enable model responses in the Sinomo tab.",
            ))
            .with_child(value_row(
                shared,
                reader,
                "Authentication",
                "Use an API key or device login, depending on the provider.",
            ))
            .with_child(
                FormRow::new("Storage", storage_button)
                    .inline()
                    .auto_control_width()
                    .theme_when(clone_reader(reader)),
            );
        if storage_expanded {
            group = group.with_child(value_row(
                shared,
                reader,
                "Config key",
                "Saved in lm.providers and reflected in the chat provider selector.",
            ));
        }
        group
    } else {
        FieldGroup::new().spacing(8.0).with_child(value_row(
            shared,
            reader,
            "Accounts",
            shared.settings().provider_forms.len().to_string(),
        ))
    };
    stack = stack.with_child(
        FormSection::new("Providers", body)
            .description(
                "Configure model accounts for chat, agent turns, and provider-specific defaults.",
            )
            .header_action(add)
            .theme_when(clone_reader(reader)),
    );
    let count = shared.settings().provider_forms.len();
    for index in 0..count {
        stack = stack.with_child(provider_card(shared, reader, index));
    }
    stack
}

fn provider_add_view(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (selected_index, uses_device_login, error) = {
        let settings = shared.settings();
        (
            settings.provider_add_provider_index,
            settings.provider_add_uses_device_login(),
            settings.provider_add_error.clone(),
        )
    };
    let provider_labels: Vec<String> = CONFIGURABLE_PROVIDERS
        .iter()
        .map(|provider| provider_display_name(provider))
        .collect();
    let provider_select = {
        let s = shared.clone();
        Select::new("provider-add-provider")
            .options(provider_labels)
            .selected_when({
                let s = s.clone();
                move || Some(s.settings().provider_add_provider_index)
            })
            .theme_when(clone_reader(reader))
            .on_change(move |index, _label| {
                s.settings_mut().set_provider_add_provider(index);
            })
    };

    let mut body = FieldGroup::new()
        .spacing(8.0)
        .with_child(
            FormRow::new("Provider", provider_select)
                .inline()
                .theme_when(clone_reader(reader)),
        )
        .with_child(text_row(
            reader,
            "provider-add-account",
            "Account",
            shared.settings().provider_add_account.clone(),
            "default",
            {
                let s = shared.clone();
                move |value| s.settings_mut().set_provider_add_account(value)
            },
        ));

    if uses_device_login {
        body = body.with_child(value_row(
            shared,
            reader,
            "Authentication",
            "Device login after add",
        ));
    } else {
        body = body
            .with_child(text_row(
                reader,
                "provider-add-api-key",
                "API key",
                "",
                "Paste API key",
                {
                    let s = shared.clone();
                    move |value| s.settings_mut().set_provider_add_api_key(value)
                },
            ))
            .with_child(value_row(
                shared,
                reader,
                "Requirement",
                "API key required before adding.",
            ));
    }

    body = body
        .with_child(text_row(
            reader,
            "provider-add-model",
            "Default model",
            shared.settings().provider_add_model.clone(),
            "Provider default",
            {
                let s = shared.clone();
                move |value| s.settings_mut().set_provider_add_model(value)
            },
        ))
        .with_child(text_row(
            reader,
            "provider-add-base-url",
            "API base URL",
            shared.settings().provider_add_base_url.clone(),
            "Provider default",
            {
                let s = shared.clone();
                move |value| s.settings_mut().set_provider_add_base_url(value)
            },
        ));

    if let Some(error) = error {
        body = body.with_child(value_row(shared, reader, "Error", error));
    }

    let cancel = {
        let s = shared.clone();
        sui::Button::new("Cancel")
            .icon(sui::IconGlyph::ChevronLeft)
            .theme_when(secondary_button_theme(reader))
            .on_press(move || s.settings_mut().close_provider_add())
    };
    let add_label = if uses_device_login {
        "Add and sign in"
    } else {
        "Add provider"
    };
    let add = {
        let s = shared.clone();
        sui::Button::new(add_label)
            .icon(sui::IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || s.settings().provider_add_can_submit()
            })
            .on_press(move || {
                let result = { s.settings_mut().add_provider_from_form() };
                let Ok((provider, account, uses_device_login)) = result else {
                    return;
                };
                refresh_chat_provider_summaries(&s);
                if uses_device_login {
                    let waker = s.borrow().waker.clone();
                    let index = {
                        let settings = s.settings();
                        settings
                            .provider_forms
                            .iter()
                            .position(|form| form.provider == provider && form.account == account)
                    };
                    if let Some(index) = index {
                        s.settings_mut().begin_device_login(index, waker);
                    }
                }
            })
    };
    let actions = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center)
        .with_child(cancel)
        .with_child(add);
    body = body.with_child(
        FormRow::new("Actions", actions)
            .inline()
            .theme_when(clone_reader(reader)),
    );

    let title = CONFIGURABLE_PROVIDERS
        .get(selected_index)
        .map(|provider| format!("Add {}", provider_display_name(provider)))
        .unwrap_or_else(|| "Add provider".to_string());
    FormSection::new(title, body)
        .description(
            "Add an account, then adjust low-level defaults from the provider card when needed.",
        )
        .theme_when(clone_reader(reader))
}

fn section_configuration(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || configuration_key(&s)
        },
        move |_key| configuration_body(&s, &r),
    )
}

fn configuration_body(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
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
            let mut stack = FieldGroup::new().spacing(8.0);
            for index in 0..count {
                stack = stack.with_child(config_row(shared, reader, index));
            }
            WidgetPod::new(stack)
        }
    };
    WidgetPod::new(
        FormSection::new("Advanced", PodHost::new(body))
            .description(
                "Resolved configuration values. Sensitive fields stay redacted in this view.",
            )
            .theme_when(clone_reader(reader)),
    )
}

/// One Advanced config row: a toggle for booleans, otherwise a redacted summary with an explicit
/// raw-value disclosure. Sensitive raw values remain read-only and redacted in Advanced.
fn config_row(shared: &Shared, reader: &ThemeReader, index: usize) -> impl Widget + use<> {
    let item = shared.settings().config_items[index].clone();
    let key = item.key.clone();
    let edit_kind = item.edit_kind;
    let bool_value = item.bool_value;
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

    let expanded = shared.settings().advanced_detail_expanded(&key);
    let sensitive = config_item_has_sensitive_detail(&item);
    let row_key = config_field_key(&key);
    let metadata = config_metadata_line(&item);
    let description = short_one_line(&item.description, 128);
    let summary = config_value_summary(&item);
    let summary_label = {
        let s = shared.clone();
        Label::new(summary)
            .font_size(12.0)
            .color_when(move || palette(&s).text_muted)
    };
    let meta_label = {
        let s = shared.clone();
        let metadata = metadata.clone();
        Label::new(metadata.clone())
            .font_size(10.5)
            .text_when({
                let s = s.clone();
                let row_key = row_key.clone();
                let metadata = metadata.clone();
                move || {
                    s.settings()
                        .field_errors
                        .get(&row_key)
                        .cloned()
                        .unwrap_or_else(|| metadata.clone())
                }
            })
            .color_when({
                let s = s.clone();
                let row_key = row_key.clone();
                move || {
                    let pal = palette(&s);
                    if s.settings().field_errors.contains_key(&row_key) {
                        pal.accent
                    } else {
                        pal.text_faint
                    }
                }
            })
    };
    let disclosure = {
        let s = shared.clone();
        let key = key.clone();
        let label = if expanded {
            "Hide value"
        } else if sensitive {
            "Show redacted detail"
        } else {
            "Edit raw value"
        };
        sui::Button::new(label)
            .icon(if expanded {
                sui::IconGlyph::ChevronUp
            } else {
                sui::IconGlyph::ChevronDown
            })
            .theme_when(secondary_button_theme(reader))
            .on_press(move || s.settings_mut().toggle_advanced_detail(&key))
    };
    let mut details = Stack::vertical()
        .spacing(6.0)
        .alignment(Alignment::Stretch)
        .with_child(summary_label)
        .with_child(meta_label);
    if !description.is_empty() {
        details = details.with_child(muted_label(shared, description).font_size(11.0));
    }
    details = details.with_child(Stack::horizontal().with_child(disclosure));
    if expanded {
        details = details.with_child(config_detail_editor(shared, reader, index, &item));
    }
    PodHost::new(WidgetPod::new(
        FormRow::new(display_key, details)
            .stacked()
            .theme_when(clone_reader(reader)),
    ))
}

fn config_detail_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    item: &ConfigItem,
) -> impl Widget + use<> {
    let detail = config_detail_value(item);
    let editable = !config_item_has_sensitive_detail(item);
    let note = if editable {
        "Raw value is editable and writes through the config service."
    } else {
        "Sensitive fields are redacted here. Use the dedicated settings section to update credentials."
    };
    let mut editor = TextArea::new(format!("config-{index}-raw"))
        .value(detail)
        .placeholder("Raw value")
        .theme_when(clone_reader(reader));
    if editable {
        let s = shared.clone();
        editor = editor.on_change(move |value| {
            s.settings_mut().set_config_value(index, &value);
        });
    } else {
        editor = editor.read_only();
    }
    Stack::vertical()
        .spacing(5.0)
        .alignment(Alignment::Stretch)
        .with_child(muted_label(shared, note).font_size(11.0))
        .with_child(SizedBox::new().height(112.0).with_child(editor))
}

fn config_metadata_line(item: &ConfigItem) -> String {
    let kind = match item.edit_kind {
        ConfigEditKind::Bool => "Boolean",
        ConfigEditKind::Text => "Text",
        ConfigEditKind::Number => "Number",
        ConfigEditKind::Json => "JSON",
    };
    let owner = item.owner.trim();
    if owner.is_empty() {
        format!("{} · {kind}", item.source)
    } else {
        format!("{} · {owner} · {kind}", item.source)
    }
}

fn config_value_summary(item: &ConfigItem) -> String {
    match item.edit_kind {
        ConfigEditKind::Bool => match item.bool_value {
            Some(true) => "Enabled".to_string(),
            Some(false) => "Disabled".to_string(),
            None => "Unset".to_string(),
        },
        ConfigEditKind::Text => {
            if item.edit_value.trim().is_empty() {
                "Unset".to_string()
            } else if sensitive_field_name(&item.key) {
                "Configured secret".to_string()
            } else {
                short_one_line(&item.edit_value, 96)
            }
        }
        ConfigEditKind::Number => {
            if item.edit_value.trim().is_empty() {
                "Unset".to_string()
            } else {
                item.edit_value.clone()
            }
        }
        ConfigEditKind::Json => config_json_summary(item),
    }
}

fn config_json_summary(item: &ConfigItem) -> String {
    let Ok(value) = serde_json::from_str::<Value>(&item.edit_value) else {
        return if item.edit_value.trim().is_empty() {
            "Unset".to_string()
        } else if config_item_has_sensitive_detail(item) {
            "JSON value · sensitive fields redacted".to_string()
        } else {
            short_one_line(&item.edit_value, 96)
        };
    };
    if item.key.trim_matches('/') == "lm.providers" {
        if let Some(accounts) = value.get("accounts").and_then(Value::as_array) {
            let enabled = accounts
                .iter()
                .filter(|account| {
                    account
                        .get("enabled")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                })
                .count();
            let suffix = if contains_sensitive_json_field(&value) {
                " · sensitive fields redacted"
            } else {
                ""
            };
            return format!(
                "Provider config · {} accounts · {} enabled{}",
                accounts.len(),
                enabled,
                suffix
            );
        }
    }
    let mut summary = match &value {
        Value::Null => "Unset".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => short_one_line(value, 96),
        Value::Array(items) => format!("JSON array · {} items", items.len()),
        Value::Object(map) => format!("JSON object · {} keys", map.len()),
    };
    if contains_sensitive_json_field(&value) || sensitive_field_name(&item.key) {
        summary.push_str(" · sensitive fields redacted");
    }
    summary
}

fn config_detail_value(item: &ConfigItem) -> String {
    if !config_item_has_sensitive_detail(item) {
        return pretty_config_value(item).unwrap_or_else(|| item.edit_value.clone());
    }

    if item.edit_value.trim().is_empty() {
        return String::new();
    }
    if sensitive_field_name(&item.key) {
        return "[redacted]".to_string();
    }
    let Ok(mut value) = serde_json::from_str::<Value>(&item.edit_value) else {
        return "[redacted]".to_string();
    };
    redact_sensitive_json_fields(&mut value);
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| "[redacted]".to_string())
}

fn pretty_config_value(item: &ConfigItem) -> Option<String> {
    if item.edit_kind != ConfigEditKind::Json {
        return None;
    }
    let value = serde_json::from_str::<Value>(&item.edit_value).ok()?;
    serde_json::to_string_pretty(&value).ok()
}

fn config_item_has_sensitive_detail(item: &ConfigItem) -> bool {
    if sensitive_field_name(&item.key) {
        return true;
    }
    if item.edit_kind != ConfigEditKind::Json {
        return false;
    }
    serde_json::from_str::<Value>(&item.edit_value)
        .map(|value| contains_sensitive_json_field(&value))
        .unwrap_or(false)
}

fn contains_sensitive_json_field(value: &Value) -> bool {
    match value {
        Value::Object(map) => map
            .iter()
            .any(|(key, value)| sensitive_field_name(key) || contains_sensitive_json_field(value)),
        Value::Array(items) => items.iter().any(contains_sensitive_json_field),
        _ => false,
    }
}

fn redact_sensitive_json_fields(value: &mut Value) -> bool {
    match value {
        Value::Object(map) => {
            let mut redacted = false;
            for (key, value) in map.iter_mut() {
                if sensitive_field_name(key) {
                    if !value.is_null() {
                        *value = Value::String("[redacted]".to_string());
                    }
                    redacted = true;
                } else if redact_sensitive_json_fields(value) {
                    redacted = true;
                }
            }
            redacted
        }
        Value::Array(items) => {
            let mut redacted = false;
            for value in items {
                if redact_sensitive_json_fields(value) {
                    redacted = true;
                }
            }
            redacted
        }
        _ => false,
    }
}

fn sensitive_field_name(name: &str) -> bool {
    let compact: String = name
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    [
        "apikey",
        "secret",
        "token",
        "password",
        "credential",
        "privatekey",
        "clientsecret",
        "accesstoken",
        "refreshtoken",
    ]
    .iter()
    .any(|needle| compact.contains(needle))
}

fn short_one_line(value: &str, max: usize) -> String {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max {
        collapsed
    } else {
        short(&collapsed, max)
    }
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
        .with_child(Separator::vertical().theme_when(clone_reader(reader)))
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
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)))
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
        let pointer_up = matches!(
            event,
            Event::Pointer(pointer) if matches!(pointer.kind, PointerEventKind::Up)
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
    link: WidgetPod,
    last_seen: RefCell<Option<String>>,
}

impl StatusReflector {
    fn new(shared: Shared, index: usize, reader: &ThemeReader) -> Self {
        let label_shared = shared.clone();
        let url_shared = shared.clone();
        let link = sui::Link::new("", "")
            .label_when(move || device_login_prompt_url(&label_shared, index).unwrap_or_default())
            .url_when(move || device_login_prompt_url(&url_shared, index).unwrap_or_default())
            .semantic_name("Open login page")
            .theme_when(clone_reader(reader))
            .on_open(open_provider_login_url);

        Self {
            shared,
            index,
            link: WidgetPod::new(link),
            last_seen: RefCell::new(None),
        }
    }

    /// A short, single-line summary used for change detection.
    fn summary(&self) -> String {
        let settings = self.shared.settings();
        let Some(form) = settings.provider_forms.get(self.index) else {
            return String::new();
        };
        match device_login_status_for(&settings, self.index) {
            Some(DeviceLoginStatus::Starting) => "starting".to_string(),
            Some(DeviceLoginStatus::Prompt {
                verification_uri,
                user_code,
            }) => format!("prompt:{user_code}:{verification_uri}"),
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
        match device_login_status_for(&settings, self.index) {
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

fn device_login_status_for(settings: &SettingsState, index: usize) -> Option<&DeviceLoginStatus> {
    let form = settings.provider_forms.get(index)?;
    settings
        .device_login
        .as_ref()
        .filter(|d| d.provider == form.provider && d.account == form.account)
        .map(|d| &d.status)
}

fn device_login_prompt_url(shared: &Shared, index: usize) -> Option<String> {
    let settings = shared.settings();
    match device_login_status_for(&settings, index)? {
        DeviceLoginStatus::Prompt {
            verification_uri, ..
        } => Some(verification_uri.clone()),
        _ => None,
    }
}

fn open_provider_login_url(url: &str) {
    if let Err(error) = open_external_url(url) {
        tracing::warn!(%url, %error, "failed to open provider device-login URL");
    }
}

fn open_external_url(url: &str) -> Result<(), String> {
    let url = url.trim();
    if url.is_empty() {
        return Err("URL is empty".to_string());
    }
    let scheme = url
        .split_once(':')
        .map(|(scheme, _)| scheme.to_ascii_lowercase())
        .ok_or_else(|| "URL is missing a scheme".to_string())?;
    if scheme != "https" && scheme != "http" {
        return Err("only http and https URLs can be opened".to_string());
    }
    open_url_with_system_browser(url)
}

#[cfg(target_os = "windows")]
fn open_url_with_system_browser(url: &str) -> Result<(), String> {
    std::process::Command::new("rundll32.exe")
        .arg("url.dll,FileProtocolHandler")
        .arg(url)
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("failed to launch browser: {error}"))
}

#[cfg(target_os = "macos")]
fn open_url_with_system_browser(url: &str) -> Result<(), String> {
    std::process::Command::new("open")
        .arg(url)
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("failed to launch browser: {error}"))
}

#[cfg(all(unix, not(target_os = "macos")))]
fn open_url_with_system_browser(url: &str) -> Result<(), String> {
    std::process::Command::new("xdg-open")
        .arg(url)
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("failed to launch browser: {error}"))
}

#[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
fn open_url_with_system_browser(_url: &str) -> Result<(), String> {
    Err("opening URLs is not supported on this platform".to_string())
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
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            240.0
        };
        let link_size = self.link.measure(
            ctx,
            Constraints::new(Size::ZERO, Size::new(width, STATUS_LINE_H)),
        );
        let height = STATUS_LINE_H
            + if link_size.height > 0.0 {
                link_size.height
            } else {
                0.0
            };
        constraints.clamp(Size::new(width, height))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let link_height = self.link.measured_size().height;
        self.link.arrange(
            ctx,
            Rect::new(
                bounds.x(),
                bounds.y() + STATUS_LINE_H,
                bounds.width(),
                link_height,
            ),
        );
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let (primary, _sub) = self.lines();

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
        self.link.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.link.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.link);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.link);
    }
}

impl StatusReflector {
    fn lines_is_error(&self) -> bool {
        let settings = self.shared.settings();
        device_login_status_for(&settings, self.index)
            .map(|status| matches!(status, DeviceLoginStatus::Failed(_)))
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
    theme_reader: ThemeReader,
    scroll_state: ScrollState,
    wide: WidgetPod,
    narrow: WidgetPod,
    use_wide: Cell<bool>,
    /// The section selection last laid out. The inner `SwitchView`s (section detail, and the narrow
    /// menu/detail swap) only measure+arrange their *active* child, so a navigation that flips
    /// `selected` (via a sidebar/menu `ListView`, which requests only a repaint) must trigger a
    /// re-measure+re-arrange for the newly-active child. We diff on `RedrawRequested` to catch it.
    last_selected: Cell<Option<SettingsSection>>,
    /// The Connection attention card is built from app-layer Chat/Files health snapshots. Those can
    /// change after startup (Files is loaded lazily), so rebuild the composed pods when they do.
    last_connection_issue_key: RefCell<String>,
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
        let last_connection_issue_key = RefCell::new(connection_issue_key(&shared));
        Self {
            shared,
            theme_reader,
            scroll_state,
            wide,
            narrow,
            // Default to wide before the first measure (matches the old default).
            use_wide: Cell::new(true),
            last_selected,
            last_connection_issue_key,
        }
    }

    fn rebuild_pods(&mut self) {
        self.wide = wide_pod(&self.shared, &self.theme_reader, &self.scroll_state);
        self.narrow = narrow_pod(&self.shared, &self.theme_reader, &self.scroll_state);
    }

    fn sync_connection_issues(&mut self) -> bool {
        let current = connection_issue_key(&self.shared);
        let mut last = self.last_connection_issue_key.borrow_mut();
        if current == *last {
            return false;
        }
        *last = current;
        drop(last);
        self.rebuild_pods();
        true
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
            let issues_changed = self.sync_connection_issues();
            if current != self.last_selected.get() || issues_changed {
                self.last_selected.set(current);
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_semantics();
                ctx.request_paint();
            }
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.sync_connection_issues();
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

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.active().semantics(ctx);
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
    use crate::state::{AppState, Shared};
    use crate::ui::chat::{ActivityTone, ChatActivityEntry};

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
        // This mirrors the build-time seed used by the add-provider form and provider cards.
        let backend = Backend::local_fallback();
        let state = SettingsState::new(&backend);
        assert!(state.provider_add_api_key.is_empty());
    }

    #[test]
    fn provider_add_requires_api_key_for_key_based_providers() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.open_provider_add();
        state.set_provider_add_provider(
            CONFIGURABLE_PROVIDERS
                .iter()
                .position(|provider| !sinomo_cli::provider_uses_device_login(provider))
                .expect("api-key provider"),
        );

        assert!(
            !state.provider_add_can_submit(),
            "API-key providers should not be addable with an empty credential"
        );

        state.set_provider_add_api_key("sk-test".to_string());
        assert!(
            state.provider_add_can_submit(),
            "typing an API key should enable the add action"
        );

        state.set_provider_add_provider(
            CONFIGURABLE_PROVIDERS
                .iter()
                .position(|provider| sinomo_cli::provider_uses_device_login(provider))
                .expect("device-login provider"),
        );
        assert!(
            state.provider_add_can_submit(),
            "device-login providers collect credentials after the account is added"
        );
    }

    #[test]
    fn provider_remove_requires_confirmation_and_removes_account() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        add_provider_account(
            state.config.as_ref(),
            "openai",
            "work",
            "sk-work",
            "gpt-5",
            "",
        )
        .expect("add provider account");
        state.reload();
        assert_eq!(state.provider_forms.len(), 1);
        assert!(!state.provider_remove_confirming("openai", "work"));

        state.confirm_provider_remove(0);
        assert!(state.provider_remove_confirming("openai", "work"));

        state.cancel_provider_remove();
        assert!(!state.provider_remove_confirming("openai", "work"));

        state.confirm_provider_remove(0);
        assert!(state.remove_provider(0));
        assert!(state.provider_forms.is_empty());
        assert!(
            crate::backend::config::load_provider_config(state.config.as_ref())
                .expect("provider config")
                .accounts
                .is_empty()
        );
    }

    #[test]
    fn provider_card_details_expand_for_low_level_controls() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let config = shared.settings().config.clone();
            add_provider_account(config.as_ref(), "openai", "work", "sk-work", "gpt-5", "")
                .expect("add provider account");
        }
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.reload();
            settings.open(SettingsSection::Providers);
        }

        let collapsed_signature = provider_section_signature(&shared);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings provider details")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-model")
            .expect()
            .to_have_value("gpt-5")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-base-url")
            .expect()
            .to_be_hidden()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Remove account")
            .expect()
            .to_be_hidden()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show details")
            .click()?;

        assert_ne!(
            collapsed_signature,
            provider_section_signature(&shared),
            "provider disclosure state must invalidate the retained section"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide details")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-base-url")
            .expect()
            .to_have_value("")?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Remove account")
            .expect()
            .to_be_visible()?;

        Ok(())
    }

    #[test]
    fn provider_empty_state_explains_chat_setup_and_opens_add_form() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::Providers);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings providers empty")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            window
                .get_by_text("Add a provider account to enable model responses in the Sinomo tab.")
                .count()?
                >= 1,
            "empty provider state should explain the user-facing outcome"
        );
        assert!(
            window
                .get_by_text("Saved in lm.providers and reflected in the chat provider selector.")
                .count()?
                == 0,
            "low-level storage detail should stay collapsed by default"
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show storage detail")
            .click()?;
        assert!(
            window
                .get_by_text("Saved in lm.providers and reflected in the chat provider selector.")
                .count()?
                >= 1,
            "storage detail should expose the cluster config key when requested"
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Add provider")
            .click()?;

        assert!(shared.settings().provider_add_open);
        assert!(
            window.get_by_text("Add Anthropic").count()? >= 1,
            "Add provider should switch into the add form"
        );
        assert!(
            window
                .get_by_text("API key required before adding.")
                .count()?
                >= 1,
            "API-key provider add form should explain why the primary action starts disabled"
        );

        Ok(())
    }

    #[test]
    fn about_settings_show_runtime_identity() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::About);
            settings.mode_label = "Cluster".to_string();
            settings.status_detail = "Connected to daemon at 127.0.0.1:21353".to_string();
            settings.node_id = "node_about_test".to_string();
            settings.role = "voter".to_string();
            settings.clustered = true;
            settings.cluster_summary = Some("cluster test · voter".to_string());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings about runtime")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            window.get_by_text("Runtime").count()? >= 1,
            "About should include a runtime identity section"
        );
        assert!(window.get_by_text("Cluster").count()? >= 1);
        assert!(
            window
                .get_by_text("Connected to daemon at 127.0.0.1:21353")
                .count()?
                >= 1
        );
        assert!(window.get_by_text("node_about_test").count()? >= 1);
        assert!(window.get_by_text("cluster test · voter").count()? >= 1);

        Ok(())
    }

    #[test]
    fn appearance_settings_explain_force_sdr_rendering() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.render_mode = AppRenderMode::ForceSdr;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Appearance);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings appearance")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window.get_by_text("Rendering").count()?,
            1,
            "Appearance should expose render settings as a first-class section"
        );
        assert!(
            window.get_by_text("Current mode").count()? >= 1,
            "Rendering should show the currently active mode"
        );
        assert_eq!(
            window
                .get_by_text("Recommended for debugging, screen sharing, and capture tools that do not handle HDR surfaces correctly.")
                .count()?,
            1,
            "Force SDR should explain why it is useful for screenshots and debugging"
        );

        Ok(())
    }

    fn test_config_item(key: &str, kind: ConfigEditKind, raw: &str) -> ConfigItem {
        ConfigItem {
            key: key.to_string(),
            owner: "test".to_string(),
            description: "test setting".to_string(),
            value: raw.to_string(),
            source: "cluster",
            bool_value: None,
            edit_kind: kind,
            edit_value: raw.to_string(),
        }
    }

    #[test]
    fn advanced_provider_config_summary_redacts_api_keys() {
        let item = test_config_item(
            "lm.providers",
            ConfigEditKind::Json,
            r#"{"accounts":[{"provider":"deepseek","account":"default","enabled":true,"api_key":"sk-live-secret","default_model":"deepseek-v4-flash"}],"default_provider":"deepseek"}"#,
        );

        let summary = config_value_summary(&item);
        assert!(summary.contains("Provider config"));
        assert!(summary.contains("sensitive fields redacted"));
        assert!(!summary.contains("sk-live-secret"));
        assert!(config_item_has_sensitive_detail(&item));

        let detail = config_detail_value(&item);
        assert!(detail.contains("[redacted]"));
        assert!(!detail.contains("sk-live-secret"));
    }

    #[test]
    fn advanced_non_sensitive_json_detail_remains_visible() {
        let item = test_config_item(
            "dfs.expose",
            ConfigEditKind::Json,
            r#"{"entries":[{"path":"/workspace","read_only":true}]}"#,
        );

        assert!(!config_item_has_sensitive_detail(&item));
        assert_eq!(config_value_summary(&item), "JSON object · 1 keys");
        let detail = config_detail_value(&item);
        assert!(detail.contains("\"entries\""));
        assert!(detail.contains("\"/workspace\""));
    }

    #[test]
    fn advanced_raw_value_disclosure_opens_editor() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::Configuration);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings advanced raw disclosure")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("config-0-raw")
            .expect()
            .to_be_hidden()?;
        assert!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Edit raw value")
                .count()?
                >= 1,
            "advanced configuration should expose raw value disclosures"
        );

        let root = window.root();
        let mut down =
            sui::PointerEvent::new(sui::PointerEventKind::Down, sui::Point::new(510.0, 210.0));
        down.button = Some(sui::PointerButton::Primary);
        root.dispatch_event(sui::Event::Pointer(down))?;
        let mut up =
            sui::PointerEvent::new(sui::PointerEventKind::Up, sui::Point::new(510.0, 210.0));
        up.button = Some(sui::PointerButton::Primary);
        root.dispatch_event(sui::Event::Pointer(up))?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide value")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("config-0-raw")
            .expect()
            .to_be_visible()?;

        Ok(())
    }

    #[test]
    fn node_detail_signature_tracks_disclosure_state() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let collapsed = node_detail_key(&shared);

        assert!(collapsed.contains("expanded:false"));
        shared.settings_mut().toggle_node_detail();

        let expanded = node_detail_key(&shared);
        assert!(expanded.contains("expanded:true"));
        assert_ne!(
            collapsed, expanded,
            "node detail disclosure must invalidate the retained section"
        );
    }

    #[test]
    fn connection_preview_surfaces_attention_state() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        shared.chat_mut().notice =
            Some("chat list failed: device authorization was denied".to_string());

        assert_eq!(
            section_preview(&shared, SettingsSection::Connection),
            "Needs attention"
        );
        let issues = connection_issues(&shared);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].title, "Chat sessions");
        assert_eq!(
            connection_issue_summary(&issues[0]),
            "Chat history and agent channels need recovery."
        );

        shared.files_mut().error = Some("device authorization was denied".to_string());
        let issues = connection_issues(&shared);
        assert_eq!(issues.len(), 2);
        assert_eq!(issues[1].title, "SIFS browser");
        assert_eq!(
            connection_issue_summary(&issues[1]),
            "SIFS browsing cannot open through the daemon."
        );

        shared.chat_mut().notice = None;
        shared.chat_mut().activity.push(ChatActivityEntry {
            title: "agent error".to_string(),
            detail: "chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied".to_string(),
            tone: ActivityTone::Bad,
        });
        let issues = connection_issues(&shared);
        assert_eq!(issues.len(), 2);
        assert_eq!(issues[0].title, "Chat sessions");
        assert!(issues[0].detail.contains("SIFS store failed"));
        assert!(connection_issue_has_sifs_recovery(&issues[0]));
        assert_eq!(
            connection_issue_recovery_hint(&issues[0]),
            "Inspect SIFS access from Files, then return to chat."
        );
        assert!(!connection_issue_recovery_hint(&issues[0]).contains("denied"));
    }

    #[test]
    fn connection_attention_action_opens_affected_tab() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Connection);
        }
        shared.chat_mut().notice =
            Some("chat list failed: device authorization was denied".to_string());

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings attention")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open Chat")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Sinomo);

        Ok(())
    }

    #[test]
    fn connection_attention_sifs_action_opens_files_tab() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Connection);
        }
        shared.chat_mut().activity.push(ChatActivityEntry {
            title: "agent error".to_string(),
            detail: "chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied".to_string(),
            tone: ActivityTone::Bad,
        });

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings SIFS attention")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Check SIFS")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn connection_attention_detail_expands_on_request() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Connection);
        }
        shared.chat_mut().notice =
            Some("chat list failed: device authorization was denied".to_string());

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings attention disclosure")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show detail")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("connection-chat-detail")
            .expect()
            .to_be_hidden()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show detail")
            .click()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide detail")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("connection-chat-detail")
            .expect()
            .to_have_value("chat list failed: device authorization was denied")?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide detail")
            .click()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("connection-chat-detail")
            .expect()
            .to_be_hidden()?;

        Ok(())
    }

    #[test]
    fn external_url_opener_rejects_non_web_urls() {
        let error = open_external_url("file:///C:/Users/test/token.txt").unwrap_err();
        assert!(error.contains("http"));

        let error = open_external_url("codex://login").unwrap_err();
        assert!(error.contains("http"));
    }

    #[test]
    fn destructive_and_secondary_button_themes_remap_accent_slots() {
        let reader: ThemeReader = Rc::new(DefaultTheme::light);
        let base = reader();

        let danger = danger_button_theme(&reader)();
        assert_eq!(danger.palette.accent, base.palette.danger);
        assert_eq!(danger.palette.accent_text, base.palette.danger_text);
        assert_ne!(danger.palette.accent, base.palette.accent);

        let secondary = secondary_button_theme(&reader)();
        assert_eq!(secondary.palette.accent, base.palette.control);
        assert_eq!(secondary.palette.accent_hover, base.palette.control_hover);
        assert_eq!(
            secondary.palette.accent_pressed,
            base.palette.control_active
        );
        assert_eq!(secondary.palette.accent_text, base.palette.text);
    }

    #[test]
    fn cluster_leave_button_uses_confirmation_flow() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::Cluster);
            settings.clustered = true;
            settings.cluster_summary = Some("cluster test · voter".to_string());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster settings")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Leave cluster")
            .click()?;
        assert!(shared.settings().leave_confirming);

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;
        assert!(!shared.settings().leave_confirming);

        Ok(())
    }

    #[test]
    fn cluster_join_status_hides_join_input() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.set_join_invite("sinomo://cluster/join/test".to_string());
        state.leave_confirming = true;

        state.apply_cluster_status(AppClusterStatus {
            clustered: true,
            cluster_id: Some("cluster_test".to_string()),
            local_node_id: Some("node_test".to_string()),
            summary: Some("cluster cluster_test · authorized".to_string()),
            error: None,
        });

        assert!(state.clustered);
        assert_eq!(
            state.cluster_summary.as_deref(),
            Some("cluster cluster_test · authorized")
        );
        assert!(
            state.join_invite.is_empty(),
            "joining hides and clears the join-link input"
        );
        assert!(
            !state.leave_confirming,
            "status changes reset the leave confirmation"
        );
    }

    #[test]
    fn cluster_leave_status_keeps_single_node_cluster_and_clears_invite() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.clustered = true;
        state.cluster_id = Some("cluster_old".to_string());
        state.invite_result = Some(AppClusterInvite {
            uri: "sinomo://cluster/join/test".to_string(),
            join_command: "sinomo cluster join sinomo://cluster/join/test".to_string(),
            expires_at: "2026-06-14T00:00:00Z".to_string(),
            persisted_key: "cluster/invitations/test".to_string(),
        });

        state.apply_cluster_status(AppClusterStatus {
            clustered: true,
            cluster_id: Some("cluster_new".to_string()),
            local_node_id: Some("node_test".to_string()),
            summary: Some("cluster cluster_new · voter".to_string()),
            error: None,
        });

        assert!(state.clustered);
        assert_eq!(state.cluster_id.as_deref(), Some("cluster_new"));
        assert!(
            state.invite_result.is_none(),
            "leaving clears generated invite details from the previous cluster"
        );
    }

    #[test]
    fn device_login_pump_applies_prompt_then_clears_on_done() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let (tx, rx) = std::sync::mpsc::channel();
        state.device_login = Some(DeviceLogin {
            provider: "codex".to_string(),
            account: "default".to_string(),
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
