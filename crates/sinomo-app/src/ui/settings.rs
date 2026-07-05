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
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use serde_json::Value;
use sinomo_cli::app_api::{SkillDetail, SkillMutationResult, SkillSummary};
use sui::{
    Alignment, ArrangeCtx, Color, Constraints, DefaultTheme, Event, EventCtx, FieldGroup,
    FontWeight, FormRow, FormSection, Label, ListItem, ListView, MeasureCtx, PaintCtx,
    PointerEventKind, Rect, ScrollState, ScrollView, Select, SemanticsCtx, Separator, Size,
    SizedBox, Stack, Switch, SwitchView, TabBar, TextArea, TextInput, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent,
};

use sinomo_runtime::config::{
    ConfigClient, ConfigSchema, ConfigSchemaField, ConfigSchemaValue, ConfigWriteTarget,
};
use sinomo_runtime::local::SinomoDataDir;

use crate::backend::Backend;
use crate::backend::cluster::{AppClusterInvite, AppClusterStatus};
use crate::backend::config::{
    CONFIGURABLE_PROVIDERS, ConfigEditKind, ConfigItem, ProviderForm, REASONING_EFFORT_OPTIONS,
    add_provider_account, cluster_provider_summaries, cycle_provider_reasoning_effort,
    provider_default_model, provider_display_name, provider_forms, reasoning_effort_index,
    remove_provider_account, resolve_items, run_provider_device_login, set_bool,
    set_provider_api_key, set_provider_base_url, set_provider_chat_default, set_provider_effort,
    set_provider_enabled, set_provider_model, set_value,
};
use crate::backend::modules::ModuleConfigSnapshot;
use crate::rendering::AppRenderMode;
use crate::state::{Shared, Tab};
use crate::theme::{AppThemeMode, Palette};
use crate::ui::widgets;
use sinomo_runtime::module::{ModuleRuntimeConfigSource, resolve_module_runtime_config};

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
    Modules,
    Skills,
    Configuration,
    About,
}

impl SettingsSection {
    pub const ALL: [SettingsSection; 9] = [
        SettingsSection::Appearance,
        SettingsSection::Connection,
        SettingsSection::Node,
        SettingsSection::Cluster,
        SettingsSection::Providers,
        SettingsSection::Modules,
        SettingsSection::Skills,
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
            SettingsSection::Modules => "Modules",
            SettingsSection::Skills => "Skills",
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
    pub data_dir: Arc<SinomoDataDir>,
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
    pub cluster_node_id: Option<String>,
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
    pub provider_action_status: Option<ProviderActionStatus>,
    pub module_snapshot: ModuleConfigSnapshot,
    pub module_selected_node_id: Option<String>,
    pub module_action_error: Option<String>,
    pub module_action_status: Option<String>,
    pub skills: Vec<SkillSummary>,
    pub skill_detail: Option<SkillDetail>,
    pub skill_selected: Option<(String, String)>,
    pub skills_include_all: bool,
    pub skill_import_path: String,
    pub skill_export_path: String,
    pub skill_action_error: Option<String>,
    pub skill_action_status: Option<String>,
    pub skill_text_expanded: bool,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderActionStatus {
    pub provider: String,
    pub account: String,
    pub message: String,
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
        let data_dir = backend.data_dir.clone();
        let skills_include_all = true;
        let (skills, skill_selected, skill_detail, skill_action_error) =
            load_initial_skill_state(data_dir.as_ref(), skills_include_all);
        let module_snapshot = backend.module_config_snapshot(None);
        let module_selected_node_id = module_snapshot
            .selected_node
            .as_ref()
            .map(|node| node.summary.node_id.clone());

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
            data_dir,
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
            cluster_node_id: cluster_status.local_node_id.clone(),
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
            provider_action_status: None,
            module_snapshot,
            module_selected_node_id,
            module_action_error: None,
            module_action_status: None,
            skills,
            skill_detail,
            skill_selected,
            skills_include_all,
            skill_import_path: String::new(),
            skill_export_path: String::new(),
            skill_action_error,
            skill_action_status: None,
            skill_text_expanded: false,
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
        self.provider_action_status = None;
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

    fn selected_module_node_index(&self) -> Option<usize> {
        let selected = self.module_selected_node_id.as_ref()?;
        self.module_snapshot
            .nodes
            .iter()
            .position(|node| &node.node_id == selected)
    }

    fn select_module_node_index(&mut self, backend: &Backend, index: usize) {
        let Some(node) = self.module_snapshot.nodes.get(index) else {
            return;
        };
        self.module_selected_node_id = Some(node.node_id.clone());
        self.module_action_error = None;
        self.module_action_status = None;
        self.reload_modules(backend);
    }

    fn reload_modules(&mut self, backend: &Backend) {
        self.module_snapshot =
            backend.module_config_snapshot(self.module_selected_node_id.as_deref());
        self.module_selected_node_id = self
            .module_snapshot
            .selected_node
            .as_ref()
            .map(|node| node.summary.node_id.clone());
    }

    fn set_cluster_module_default(
        &mut self,
        backend: &Backend,
        module_id: &str,
        enabled: bool,
    ) -> bool {
        match backend.set_cluster_module_default(module_id, enabled) {
            Ok(()) => {
                self.module_action_error = None;
                self.module_action_status = Some(format!(
                    "Cluster default saved for {}",
                    module_display_id(module_id)
                ));
                self.reload_modules(backend);
                true
            }
            Err(error) => {
                self.module_action_error = Some(error);
                self.module_action_status = None;
                false
            }
        }
    }

    fn set_node_module_override(
        &mut self,
        backend: &Backend,
        module_id: &str,
        enabled: Option<bool>,
    ) -> bool {
        let Some(node_id) = self.module_selected_node_id.clone() else {
            self.module_action_error = Some("select a node first".to_string());
            self.module_action_status = None;
            return false;
        };
        match backend.set_node_module_override(&node_id, module_id, enabled) {
            Ok(()) => {
                self.module_action_error = None;
                self.module_action_status = Some(match enabled {
                    Some(true) => {
                        format!("Node override enabled for {}", module_display_id(module_id))
                    }
                    Some(false) => format!(
                        "Node override disabled for {}",
                        module_display_id(module_id)
                    ),
                    None => format!("Node override cleared for {}", module_display_id(module_id)),
                });
                self.reload_modules(backend);
                true
            }
            Err(error) => {
                self.module_action_error = Some(error);
                self.module_action_status = None;
                false
            }
        }
    }

    fn selected_skill_index(&self) -> Option<usize> {
        let (selected_id, selected_version) = self.skill_selected.as_ref()?;
        self.skills
            .iter()
            .position(|skill| &skill.id == selected_id && &skill.version == selected_version)
    }

    fn select_skill_index(&mut self, index: usize) {
        let Some(skill) = self.skills.get(index) else {
            return;
        };
        self.skill_selected = Some((skill.id.clone(), skill.version.clone()));
        self.skill_action_error = None;
        self.skill_text_expanded = false;
        self.refresh_selected_skill_detail();
    }

    fn set_skills_include_all(&mut self, include_all: bool) {
        if self.skills_include_all == include_all {
            return;
        }
        self.skills_include_all = include_all;
        self.reload_skills();
    }

    fn set_skill_import_path(&mut self, value: String) {
        self.skill_import_path = value;
        self.skill_action_error = None;
        self.skill_action_status = None;
    }

    fn set_skill_export_path(&mut self, value: String) {
        self.skill_export_path = value;
        self.skill_action_error = None;
        self.skill_action_status = None;
    }

    fn toggle_skill_text(&mut self) {
        self.skill_text_expanded = !self.skill_text_expanded;
    }

    fn reload_skills(&mut self) {
        let previous = self.skill_selected.clone();
        match sinomo_cli::app_api::skill_summaries(self.data_dir.as_ref(), self.skills_include_all)
        {
            Ok(skills) => {
                self.skills = skills;
                self.skill_selected = previous
                    .filter(|(id, version)| {
                        self.skills
                            .iter()
                            .any(|skill| &skill.id == id && &skill.version == version)
                    })
                    .or_else(|| {
                        self.skills
                            .first()
                            .map(|skill| (skill.id.clone(), skill.version.clone()))
                    });
                self.refresh_selected_skill_detail();
            }
            Err(error) => {
                self.skills.clear();
                self.skill_selected = None;
                self.skill_detail = None;
                self.skill_action_error = Some(error);
            }
        }
    }

    fn refresh_selected_skill_detail(&mut self) {
        let Some((skill_id, version)) = self.skill_selected.clone() else {
            self.skill_detail = None;
            return;
        };
        match sinomo_cli::app_api::skill_detail(self.data_dir.as_ref(), &skill_id, Some(&version)) {
            Ok(detail) => {
                self.skill_detail = Some(detail);
            }
            Err(error) => {
                self.skill_detail = None;
                self.skill_action_error = Some(error);
            }
        }
    }

    fn import_skill_from_path(&mut self) -> bool {
        let path = self.skill_import_path.trim();
        if path.is_empty() {
            self.skill_action_error = Some("enter a skill file or directory path".to_string());
            self.skill_action_status = None;
            return false;
        }
        let path = PathBuf::from(path);
        match sinomo_cli::app_api::import_skill(self.data_dir.as_ref(), &path, None, None, true) {
            Ok(result) => {
                self.skills_include_all = true;
                self.skill_selected = Some((result.skill.id.clone(), result.skill.version.clone()));
                self.skill_action_status = Some(skill_mutation_message(&result));
                self.skill_action_error = None;
                self.skill_text_expanded = false;
                self.reload_skills();
                true
            }
            Err(error) => {
                self.skill_action_error = Some(error);
                self.skill_action_status = None;
                false
            }
        }
    }

    fn export_selected_skill(&mut self) -> bool {
        let Some((skill_id, version)) = self.skill_selected.clone() else {
            self.skill_action_error = Some("select a skill to export".to_string());
            self.skill_action_status = None;
            return false;
        };
        let output = self.skill_export_path.trim();
        if output.is_empty() {
            self.skill_action_error = Some("enter an export path".to_string());
            self.skill_action_status = None;
            return false;
        }
        let output = PathBuf::from(output);
        match sinomo_cli::app_api::export_skill(
            self.data_dir.as_ref(),
            &skill_id,
            Some(&version),
            Some(&output),
        ) {
            Ok(result) => {
                self.skill_action_status = Some(skill_mutation_message(&result));
                self.skill_action_error = None;
                true
            }
            Err(error) => {
                self.skill_action_error = Some(error);
                self.skill_action_status = None;
                false
            }
        }
    }

    fn activate_selected_skill(&mut self) -> bool {
        let Some((skill_id, version)) = self.skill_selected.clone() else {
            self.skill_action_error = Some("select a skill to activate".to_string());
            self.skill_action_status = None;
            return false;
        };
        match sinomo_cli::app_api::activate_skill(self.data_dir.as_ref(), &skill_id, Some(&version))
        {
            Ok(result) => {
                self.skill_selected = Some((result.skill.id.clone(), result.skill.version.clone()));
                self.skill_action_status = Some(skill_mutation_message(&result));
                self.skill_action_error = None;
                self.reload_skills();
                true
            }
            Err(error) => {
                self.skill_action_error = Some(error);
                self.skill_action_status = None;
                false
            }
        }
    }

    fn confirm_provider_remove(&mut self, index: usize) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        self.provider_action_error = None;
        self.provider_action_status = None;
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
                self.set_provider_status(
                    provider.clone(),
                    account.clone(),
                    "Account added to lm.providers",
                );
                Ok((provider, account, uses_device_login))
            }
            Err(error) => {
                self.provider_add_error = Some(error.clone());
                self.provider_action_error = None;
                self.provider_action_status = None;
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
        self.cluster_node_id = status.local_node_id;
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

    fn set_config_schema_bool(&mut self, index: usize, path: Vec<String>, value: bool) {
        self.set_config_schema_value(index, path, Value::Bool(value));
    }

    fn set_config_schema_text(
        &mut self,
        index: usize,
        path: Vec<String>,
        schema: ConfigSchemaValue,
        raw: &str,
    ) {
        match parse_schema_editor_value(&schema, raw) {
            Ok(value) => self.set_config_schema_value(index, path, value),
            Err(error) => {
                if let Some(item) = self.config_items.get(index) {
                    self.field_errors
                        .insert(config_schema_field_key(&item.key, &path), error);
                }
            }
        }
    }

    fn set_config_schema_value(&mut self, index: usize, path: Vec<String>, value: Value) {
        let Some(item) = self.config_items.get(index) else {
            return;
        };
        let key = item.key.clone();
        let mut next = item.raw_value.clone();
        let error_key = config_schema_field_key(&key, &path);
        if let Err(error) = set_json_path_value(&mut next, &path, value) {
            self.field_errors.insert(error_key, error);
            return;
        }
        match self
            .config
            .set(&key, next.clone(), ConfigWriteTarget::Auto)
            .map_err(|error| error.to_string())
        {
            Ok(_) => {
                if let Some(item) = self.config_items.get_mut(index) {
                    item.update_value(next);
                }
                self.field_errors.remove(&error_key);
            }
            Err(error) => {
                self.field_errors.insert(error_key, error);
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
            self.set_provider_status(
                provider,
                account,
                if enabled {
                    "Provider enabled in lm.providers"
                } else {
                    "Provider disabled in lm.providers"
                },
            );
            return true;
        }
        false
    }

    fn set_provider_chat_default(&mut self, index: usize) -> bool {
        let Some(form) = self.provider_forms.get(index) else {
            return false;
        };
        let provider = form.provider.clone();
        let account = form.account.clone();
        match set_provider_chat_default(self.config.as_ref(), &provider, &account) {
            Ok(()) => {
                self.provider_action_error = None;
                self.provider_remove_confirming = None;
                self.reload();
                self.set_provider_status(provider, account, "Chat default saved to lm.providers");
                true
            }
            Err(error) => {
                self.provider_action_error = Some(error);
                self.provider_action_status = None;
                false
            }
        }
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
                self.provider_action_status = None;
                self.provider_remove_confirming = None;
                self.reload();
                true
            }
            Err(error) => {
                self.provider_action_error = Some(error);
                self.provider_action_status = None;
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
                self.set_provider_status(provider, account, provider_field_saved_message(field));
                self.provider_remove_confirming = None;
            }
            Err(error) => {
                self.field_errors.insert(row, error);
                self.provider_action_status = None;
            }
        }
    }

    fn set_provider_status(
        &mut self,
        provider: impl Into<String>,
        account: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.provider_action_status = Some(ProviderActionStatus {
            provider: provider.into(),
            account: account.into(),
            message: message.into(),
        });
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
            self.set_provider_status(provider, account, "Reasoning effort saved to lm.providers");
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
        let done_account = if done {
            self.device_login
                .as_ref()
                .map(|login| (login.provider.clone(), login.account.clone()))
        } else {
            None
        };
        if done {
            self.reload();
            if let Some((provider, account)) = done_account {
                self.set_provider_status(provider, account, "Sign-in saved to lm.providers");
            }
            self.device_login = None;
        }
        true
    }
}

fn load_initial_skill_state(
    data_dir: &SinomoDataDir,
    include_all: bool,
) -> (
    Vec<SkillSummary>,
    Option<(String, String)>,
    Option<SkillDetail>,
    Option<String>,
) {
    let skills = match sinomo_cli::app_api::skill_summaries(data_dir, include_all) {
        Ok(skills) => skills,
        Err(error) => return (Vec::new(), None, None, Some(error)),
    };
    let selected = skills
        .first()
        .map(|skill| (skill.id.clone(), skill.version.clone()));
    let detail = selected.as_ref().and_then(|(skill_id, version)| {
        sinomo_cli::app_api::skill_detail(data_dir, skill_id, Some(version)).ok()
    });
    (skills, selected, detail, None)
}

fn skill_mutation_message(result: &SkillMutationResult) -> String {
    let mut message = format!(
        "{} {}@{}",
        result.status, result.skill.id, result.skill.version
    );
    if let Some(scope) = &result.scope {
        message.push_str(" for ");
        message.push_str(scope);
    }
    if let Some(path) = &result.path {
        message.push_str(" at ");
        message.push_str(path);
    }
    message
}

/// Field-error map key for a raw config key (Advanced section).
fn config_field_key(key: &str) -> String {
    format!("config:{key}")
}

fn config_schema_field_key(key: &str, path: &[String]) -> String {
    if path.is_empty() {
        return format!("config-schema:{key}:$");
    }
    format!("config-schema:{key}:{}", path.join("."))
}

fn parse_schema_editor_value(schema: &ConfigSchemaValue, raw: &str) -> Result<Value, String> {
    let trimmed = raw.trim();
    match schema {
        ConfigSchemaValue::Bool => Ok(Value::Bool(trimmed.eq_ignore_ascii_case("true"))),
        ConfigSchemaValue::I64 { .. } => trimmed
            .parse::<i64>()
            .map(|value| Value::Number(value.into()))
            .map_err(|error| format!("invalid i64: {error}")),
        ConfigSchemaValue::U64 { .. } => trimmed
            .parse::<u64>()
            .map(|value| Value::Number(value.into()))
            .map_err(|error| format!("invalid u64: {error}")),
        ConfigSchemaValue::F64 { .. } => {
            let value = trimmed
                .parse::<f64>()
                .map_err(|error| format!("invalid number: {error}"))?;
            serde_json::Number::from_f64(value)
                .map(Value::Number)
                .ok_or_else(|| "invalid finite number".to_string())
        }
        ConfigSchemaValue::String { .. } => Ok(Value::String(raw.to_string())),
        ConfigSchemaValue::StringList
        | ConfigSchemaValue::Object { .. }
        | ConfigSchemaValue::Map { .. }
        | ConfigSchemaValue::List { .. }
        | ConfigSchemaValue::Json => {
            serde_json::from_str(trimmed).map_err(|error| format!("invalid JSON: {error}"))
        }
    }
}

fn set_json_path_value(root: &mut Value, path: &[String], value: Value) -> Result<(), String> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }
    let mut current = root;
    for segment in &path[..path.len() - 1] {
        if !current.is_object() {
            *current = Value::Object(serde_json::Map::new());
        }
        let map = current
            .as_object_mut()
            .ok_or_else(|| format!("{segment} is not an object"))?;
        current = map
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
    }
    if !current.is_object() {
        *current = Value::Object(serde_json::Map::new());
    }
    let map = current
        .as_object_mut()
        .ok_or_else(|| "parent value is not an object".to_string())?;
    map.insert(path[path.len() - 1].clone(), value);
    Ok(())
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

fn provider_field_saved_message(field: ProviderField) -> &'static str {
    match field {
        ProviderField::ApiKey => "API key saved to lm.providers",
        ProviderField::Model => "Default model saved to lm.providers",
        ProviderField::BaseUrl => "API base URL saved to lm.providers",
    }
}

fn provider_account_key(provider: &str, account: &str) -> String {
    format!("{provider}:{account}")
}

fn provider_enabled_count(forms: &[ProviderForm]) -> usize {
    forms.iter().filter(|form| form.enabled).count()
}

fn provider_ready_count(forms: &[ProviderForm]) -> usize {
    forms
        .iter()
        .filter(|form| form.enabled && form.signed_in)
        .count()
}

fn provider_accounts_summary(forms: &[ProviderForm]) -> String {
    let total = forms.len();
    let enabled = provider_enabled_count(forms);
    format!("{total} total · {enabled} enabled")
}

fn provider_ready_summary(forms: &[ProviderForm]) -> String {
    let enabled = provider_enabled_count(forms);
    let ready = provider_ready_count(forms);
    if forms.is_empty() {
        "0 ready".to_string()
    } else if enabled == 0 {
        "0 ready · all accounts disabled".to_string()
    } else if ready == enabled {
        format!("{ready} ready")
    } else {
        let blocked = enabled.saturating_sub(ready);
        if blocked == 1 {
            format!("{ready} ready · 1 account needs credentials")
        } else {
            format!("{ready} ready · {blocked} accounts need credentials")
        }
    }
}

fn provider_sidebar_preview(forms: &[ProviderForm]) -> String {
    if forms.is_empty() {
        return "Not set up".to_string();
    }
    let enabled = provider_enabled_count(forms);
    let ready = provider_ready_count(forms);
    if ready > 0 {
        format!("{ready} ready")
    } else if enabled > 0 {
        "Needs credentials".to_string()
    } else {
        "Disabled".to_string()
    }
}

fn module_sidebar_preview(snapshot: &ModuleConfigSnapshot) -> String {
    if snapshot.modules.is_empty() {
        return "No modules".to_string();
    }
    format!(
        "{} modules · {} nodes",
        snapshot.modules.len(),
        snapshot.nodes.len()
    )
}

fn skills_sidebar_preview(skills: &[SkillSummary]) -> String {
    if skills.is_empty() {
        return "No skills".to_string();
    }
    let active = skills.iter().filter(|skill| skill.active).count();
    if active == 0 {
        format!("{} versions", skills.len())
    } else {
        format!("{active} active")
    }
}

fn skill_title(skill: &SkillSummary) -> String {
    format!("{} @ {}", skill.id, skill.version)
}

fn skill_source_label(skill: &SkillSummary) -> String {
    match (&skill.source_id, &skill.source_version) {
        (Some(id), Some(version)) => {
            format!("{}:{} ({})", skill.source_kind, id, version)
        }
        (Some(id), None) => format!("{}:{}", skill.source_kind, id),
        (None, Some(version)) => format!("{} ({})", skill.source_kind, version),
        (None, None) => skill.source_kind.clone(),
    }
}

fn skill_list_item(skill: &SkillSummary) -> ListItem {
    let state = if skill.active {
        "active"
    } else {
        &skill.status
    };
    ListItem::new(skill_title(skill)).detail(format!("{} · {}", state, skill_source_label(skill)))
}

fn provider_model_label(form: &ProviderForm) -> String {
    let model = form.default_model.trim();
    if model.is_empty() {
        "provider default".to_string()
    } else {
        model.to_string()
    }
}

fn provider_credential_note(form: &ProviderForm) -> &'static str {
    if form.uses_device_login {
        "sign-in required"
    } else {
        "API key required"
    }
}

fn provider_chat_default_summary(forms: &[ProviderForm]) -> String {
    if forms.is_empty() {
        return "not configured".to_string();
    }
    let (form, fallback) = forms
        .iter()
        .find(|form| form.is_default)
        .map(|form| (form, false))
        .or_else(|| {
            forms
                .iter()
                .find(|form| form.enabled && form.signed_in)
                .or_else(|| forms.iter().find(|form| form.enabled))
                .or_else(|| forms.first())
                .map(|form| (form, true))
        })
        .expect("forms is not empty");

    let mut notes = Vec::new();
    if fallback {
        notes.push("fallback");
    }
    if !form.enabled {
        notes.push("disabled");
    } else if !form.signed_in {
        notes.push(provider_credential_note(form));
    }

    let summary = format!("{} / {}", form.display_name, provider_model_label(form));
    if notes.is_empty() {
        summary
    } else {
        format!("{summary} ({})", notes.join(", "))
    }
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
        SettingsSection::Node => app_surface_summary(&shared.settings().role),
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
            provider_sidebar_preview(&settings.provider_forms)
        }
        SettingsSection::Modules => {
            let settings = shared.settings();
            module_sidebar_preview(&settings.module_snapshot)
        }
        SettingsSection::Skills => {
            let settings = shared.settings();
            skills_sidebar_preview(&settings.skills)
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

fn positive_label(shared: &Shared, text: impl Into<String>) -> Label {
    let s = shared.clone();
    Label::new(text).color_when(move || palette(&s).good)
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
    if let Some(detail) = ready.files.connection_attention_detail() {
        issues.push(ConnectionIssue {
            title: "SIFS access",
            detail,
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
        "{}\n{}\n{}\n{}\n{}\n{:?}\n{:?}\nexpanded:{}",
        settings.node_id,
        settings.role,
        settings.mode_label,
        settings.status_detail,
        settings.clustered,
        settings.cluster_node_id,
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

fn skills_section_key(shared: &Shared) -> String {
    let settings = shared.settings();
    let skill_key = settings
        .skills
        .iter()
        .map(|skill| {
            format!(
                "{}@{}:{}:{}:{}",
                skill.id, skill.version, skill.active, skill.status, skill.content_hash
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "all:{};selected:{:?};detail:{:?};text:{};import:{};export:{};error:{:?};status:{:?}\n{}",
        settings.skills_include_all,
        settings.skill_selected,
        settings
            .skill_detail
            .as_ref()
            .map(|detail| detail.summary.content_hash.as_str()),
        settings.skill_text_expanded,
        settings.skill_import_path.len(),
        settings.skill_export_path.len(),
        settings.skill_action_error,
        settings.skill_action_status,
        skill_key
    )
}

fn connection_issue_summary(issue: &ConnectionIssue) -> &'static str {
    match issue.tab {
        Tab::Sinomo => "Chat history and agent channels need recovery.",
        Tab::Files => "SIFS file access needs recovery.",
        _ => "A surface needs recovery.",
    }
}

fn connection_issue_recovery_hint(issue: &ConnectionIssue) -> &'static str {
    match issue.tab {
        Tab::Sinomo if connection_issue_has_sifs_recovery(issue) => {
            "Inspect SIFS access from Files, then return to chat."
        }
        Tab::Sinomo => "Return to chat after credentials are fixed.",
        Tab::Files => "Return to Files after SIFS authorization or daemon access is fixed.",
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
    let (
        short_surface_id,
        full_surface_id,
        surface_summary,
        mode_label,
        status_detail,
        cluster_node_id,
        cluster_summary,
        expanded,
    ) = {
        let s = shared.settings();
        (
            short(&s.node_id, 30),
            s.node_id.clone(),
            app_surface_summary(&s.role),
            s.mode_label.clone(),
            s.status_detail.clone(),
            s.cluster_node_id.clone(),
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
        .with_child(value_row(shared, reader, "App surface", surface_summary))
        .with_child(value_row(shared, reader, "Connection", mode_label.clone()))
        .with_child(
            FormRow::new("Details", Stack::horizontal().with_child(detail_button))
                .inline()
                .theme_when(clone_reader(reader)),
        );
    if expanded {
        body = body
            .with_child(value_row(
                shared,
                reader,
                "App surface ID",
                short_surface_id,
            ))
            .with_child(copyable_text_row(
                reader,
                "app-surface-full-id",
                "Full app surface ID",
                full_surface_id,
            ))
            .with_child(copyable_area_row(
                reader,
                "node-connection-detail",
                "Connection detail",
                status_detail,
            ));
        if let Some(cluster_node_id) = cluster_node_id {
            body = body
                .with_child(value_row(
                    shared,
                    reader,
                    "Daemon node ID",
                    short(&cluster_node_id, 30),
                ))
                .with_child(copyable_text_row(
                    reader,
                    "daemon-node-full-id",
                    "Full daemon node ID",
                    cluster_node_id,
                ));
        }
        if let Some(summary) = cluster_summary {
            body = body.with_child(value_row(shared, reader, "Cluster", summary));
        }
    }
    FormSection::new("Node", body)
        .description("App surface identity and the attached daemon or cluster connection.")
        .theme_when(clone_reader(reader))
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

        body = body.with_child(
            FormRow::new("Health", cluster_dashboard_button(shared, reader))
                .inline()
                .theme_when(clone_reader(reader)),
        );

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

        body = body.with_child(
            FormRow::new("Health", cluster_dashboard_button(shared, reader))
                .inline()
                .theme_when(clone_reader(reader)),
        );

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

fn cluster_dashboard_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    sui::Button::new("Open Cluster dashboard")
        .icon(sui::IconGlyph::Storage)
        .theme_when(secondary_button_theme(reader))
        .on_press(move || open_tab(&s, Tab::Cluster))
}

fn section_about(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (
        version,
        mode_label,
        status_detail,
        app_surface_id,
        surface_role,
        cluster_node_id,
        cluster_summary,
    ) = {
        let settings = shared.settings();
        (
            settings.version.clone(),
            settings.mode_label.clone(),
            short_one_line(&settings.status_detail, 96),
            short(&settings.node_id, 38),
            app_surface_summary(&settings.role),
            settings
                .cluster_node_id
                .as_deref()
                .map(|node_id| short(node_id, 38)),
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
    let mut runtime = FieldGroup::new()
        .spacing(6.0)
        .with_child(value_row(shared, reader, "Mode", mode_label))
        .with_child(value_row(shared, reader, "Status", status_detail))
        .with_child(value_row(shared, reader, "App surface", app_surface_id))
        .with_child(value_row(shared, reader, "Surface role", surface_role));
    if let Some(cluster_node_id) = cluster_node_id {
        runtime = runtime.with_child(value_row(shared, reader, "Daemon node", cluster_node_id));
    }
    runtime = runtime.with_child(value_row(shared, reader, "Cluster", cluster_summary));
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
                .description(
                    "Current connection plus app surface and daemon identity for diagnostics.",
                )
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
    let chat_default_control = if form.is_default {
        Stack::horizontal()
            .alignment(Alignment::Center)
            .with_child(positive_label(shared, "Chat default"))
    } else {
        let s = shared.clone();
        let ready_for_chat = form.enabled && form.signed_in;
        let button = sui::Button::new("Use for chat")
            .icon(sui::IconGlyph::Check)
            .theme_when(secondary_button_theme(reader))
            .enabled(ready_for_chat)
            .on_press(move || {
                let changed = { s.settings_mut().set_provider_chat_default(index) };
                if changed {
                    refresh_chat_provider_summaries(&s);
                }
            });
        Stack::horizontal()
            .alignment(Alignment::Center)
            .with_child(button)
    };
    body = body.with_child(
        FormRow::new("Chat", chat_default_control)
            .inline()
            .theme_when(clone_reader(reader)),
    );
    let provider_status = shared
        .settings()
        .provider_action_status
        .as_ref()
        .filter(|status| status.provider == form.provider && status.account == form.account)
        .map(|status| status.message.clone());
    if let Some(message) = provider_status {
        body = body.with_child(
            FormRow::new("Last change", positive_label(shared, message))
                .inline()
                .theme_when(clone_reader(reader)),
        );
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
        format!("action-status:{:?}", settings.provider_action_status),
        format!("remove:{:?}", settings.provider_remove_confirming),
        format!("expanded:{:?}", settings.provider_detail_expanded),
        format!(
            "storage-expanded:{}",
            settings.provider_storage_detail_expanded
        ),
    ];
    parts.extend(settings.provider_forms.iter().map(|form| {
        format!(
            "{}@{}:{}:{}:{}:{}:{}",
            form.provider,
            form.account,
            form.enabled,
            form.signed_in,
            form.is_default,
            form.default_model,
            form.display_name
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
    let provider_forms = shared.settings().provider_forms.clone();
    let body = if provider_forms.is_empty() {
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
        FieldGroup::new()
            .spacing(8.0)
            .with_child(value_row(
                shared,
                reader,
                "Accounts",
                provider_accounts_summary(&provider_forms),
            ))
            .with_child(value_row(
                shared,
                reader,
                "Ready for chat",
                provider_ready_summary(&provider_forms),
            ))
            .with_child(value_row(
                shared,
                reader,
                "Chat default",
                provider_chat_default_summary(&provider_forms),
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

const MODULE_OVERRIDE_OPTIONS: &[&str] = &["Inherit", "Enable", "Disable"];

fn section_modules(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || module_section_key(&s)
        },
        move |_key| WidgetPod::new(section_modules_body(&s, &r)),
    )
}

fn module_section_key(shared: &Shared) -> String {
    let settings = shared.settings();
    let snapshot = &settings.module_snapshot;
    let nodes = snapshot
        .nodes
        .iter()
        .map(|node| {
            format!(
                "{}:{}:{}:{}",
                node.node_id, node.display_name, node.availability, node.local
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let modules = snapshot
        .modules
        .iter()
        .map(|module| {
            format!(
                "{}:{}:{}:{}",
                module.module_id, module.label, module.backend, module.detail
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let cluster = snapshot
        .cluster_config
        .modules
        .iter()
        .map(|(module_id, setting)| format!("{module_id}:{}", setting.enabled))
        .collect::<Vec<_>>()
        .join(",");
    let selected = snapshot
        .selected_node
        .as_ref()
        .map(|node| {
            let config = node
                .config
                .modules
                .iter()
                .map(|(module_id, setting)| format!("{module_id}:{}", setting.enabled))
                .collect::<Vec<_>>()
                .join(",");
            format!("{}:{:?}:{}", node.summary.node_id, node.error, config)
        })
        .unwrap_or_default();
    format!(
        "selected:{:?};error:{:?};status:{:?};warnings:{:?}\n{nodes}\n{modules}\n{cluster}\n{selected}",
        settings.module_selected_node_id,
        settings.module_action_error,
        settings.module_action_status,
        snapshot.warnings
    )
}

fn section_modules_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (snapshot, action_error, action_status) = {
        let settings = shared.settings();
        (
            settings.module_snapshot.clone(),
            settings.module_action_error.clone(),
            settings.module_action_status.clone(),
        )
    };

    let node_options = snapshot
        .nodes
        .iter()
        .map(module_node_option)
        .collect::<Vec<_>>();
    let node_select = {
        let s = shared.clone();
        Select::new("module-config-node")
            .options(node_options)
            .selected_when({
                let s = s.clone();
                move || s.settings().selected_module_node_index()
            })
            .theme_when(clone_reader(reader))
            .on_change(move |index, _label| {
                if let Some(backend) = ready_backend(&s) {
                    s.settings_mut().select_module_node_index(&backend, index);
                }
            })
    };
    let refresh = {
        let s = shared.clone();
        sui::Button::new("Refresh")
            .icon(sui::IconGlyph::Redo)
            .theme_when(secondary_button_theme(reader))
            .on_press(move || {
                if let Some(backend) = ready_backend(&s) {
                    s.settings_mut().reload_modules(&backend);
                }
            })
    };

    let mut summary_body = FieldGroup::new()
        .spacing(8.0)
        .with_child(value_row(
            shared,
            reader,
            "Cluster defaults",
            module_cluster_summary(&snapshot),
        ))
        .with_child(
            FormRow::new("Node", node_select)
                .inline()
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new(
                "Actions",
                Stack::horizontal()
                    .spacing(8.0)
                    .alignment(Alignment::Start)
                    .with_child(refresh),
            )
            .inline()
            .theme_when(clone_reader(reader)),
        );
    if let Some(node) = snapshot.selected_node.as_ref() {
        summary_body = summary_body.with_child(value_row(
            shared,
            reader,
            "Selected",
            format!(
                "{} · {}",
                node.summary.display_name,
                short(&node.summary.node_id, 16)
            ),
        ));
        if let Some(error) = node.error.as_ref() {
            summary_body =
                summary_body.with_child(value_row(shared, reader, "Node config", error.clone()));
        }
    }
    if !snapshot.warnings.is_empty() {
        summary_body = summary_body.with_child(value_row(
            shared,
            reader,
            "Warnings",
            snapshot.warnings.join("; "),
        ));
    }
    if let Some(error) = action_error {
        summary_body = summary_body.with_child(value_row(shared, reader, "Error", error));
    } else if let Some(status) = action_status {
        summary_body = summary_body.with_child(value_row(shared, reader, "Status", status));
    }

    let mut stack = Stack::vertical()
        .spacing(14.0)
        .alignment(Alignment::Stretch)
        .with_child(
            FormSection::new("Modules", summary_body)
                .description(
                    "Configure cluster defaults and node-local overrides for runtime modules.",
                )
                .theme_when(clone_reader(reader)),
        );
    if snapshot.modules.is_empty() {
        let empty = FieldGroup::new().spacing(8.0).with_child(value_row(
            shared,
            reader,
            "Status",
            "No installed or configured modules were found",
        ));
        return stack
            .with_child(FormSection::new("Catalog", empty).theme_when(clone_reader(reader)));
    }
    for module in snapshot.modules {
        stack = stack.with_child(module_config_section(shared, reader, module));
    }
    stack
}

fn module_config_section(
    shared: &Shared,
    reader: &ThemeReader,
    module: crate::backend::modules::ModuleConfigModuleSummary,
) -> impl Widget + use<> {
    let module_id = module.module_id.clone();
    let (cluster_enabled, effective, selected_node_label) = {
        let settings = shared.settings();
        let cluster_enabled = settings
            .module_snapshot
            .cluster_config
            .setting(&module_id)
            .map(|setting| setting.enabled)
            .unwrap_or(false);
        let effective = settings.module_snapshot.selected_node.as_ref().map(|node| {
            resolve_module_runtime_config(
                &node.config,
                &settings.module_snapshot.cluster_config,
                &module_id,
            )
        });
        let selected_node_label = settings
            .module_snapshot
            .selected_node
            .as_ref()
            .map(|node| node.summary.display_name.clone())
            .unwrap_or_else(|| "No node selected".to_string());
        (cluster_enabled, effective, selected_node_label)
    };

    let cluster_switch = switch_row(reader, "Cluster default", cluster_enabled, {
        let s = shared.clone();
        let module_id = module_id.clone();
        move |enabled| {
            if let Some(backend) = ready_backend(&s) {
                s.settings_mut()
                    .set_cluster_module_default(&backend, &module_id, enabled);
            }
        }
    });

    let node_override = {
        let s = shared.clone();
        let selected_module_id = module_id.clone();
        select_row(
            reader,
            &format!("module-override-{module_id}"),
            "Node override",
            MODULE_OVERRIDE_OPTIONS,
            {
                let s = s.clone();
                let module_id = module_id.clone();
                move || {
                    let settings = s.settings();
                    Some(
                        settings
                            .module_snapshot
                            .selected_node
                            .as_ref()
                            .and_then(|node| node.config.setting(&module_id))
                            .map(|setting| if setting.enabled { 1 } else { 2 })
                            .unwrap_or(0),
                    )
                }
            },
            move |index, _label| {
                let enabled = match index {
                    1 => Some(true),
                    2 => Some(false),
                    _ => None,
                };
                if let Some(backend) = ready_backend(&s) {
                    s.settings_mut().set_node_module_override(
                        &backend,
                        &selected_module_id,
                        enabled,
                    );
                }
            },
        )
    };

    let mut body = FieldGroup::new()
        .spacing(8.0)
        .with_child(value_row(
            shared,
            reader,
            "Module ID",
            module_display_id(&module.module_id),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Backend",
            format!("{} · {}", module.backend, module.detail),
        ))
        .with_child(cluster_switch)
        .with_child(node_override)
        .with_child(value_row(
            shared,
            reader,
            "Effective",
            effective
                .map(module_effective_label)
                .unwrap_or(selected_node_label),
        ));

    if let Some(node) = shared.settings().module_snapshot.selected_node.as_ref()
        && let Some(error) = node.error.as_ref()
    {
        body = body.with_child(value_row(shared, reader, "Node error", error.clone()));
    }

    FormSection::new(module.label.clone(), body).theme_when(clone_reader(reader))
}

fn module_node_option(node: &crate::backend::modules::ModuleConfigNodeSummary) -> String {
    let suffix = if node.local { " · local" } else { "" };
    format!(
        "{} · {}{}",
        node.display_name,
        short(&node.node_id, 12),
        suffix
    )
}

fn module_cluster_summary(snapshot: &ModuleConfigSnapshot) -> String {
    let enabled = snapshot
        .cluster_config
        .modules
        .values()
        .filter(|setting| setting.enabled)
        .count();
    let configured = snapshot.cluster_config.modules.len();
    if configured == 0 {
        "No cluster defaults".to_string()
    } else {
        format!("{enabled} enabled · {configured} configured")
    }
}

fn module_effective_label(config: sinomo_runtime::module::ResolvedModuleRuntimeConfig) -> String {
    let state = if config.enabled {
        "Enabled"
    } else {
        "Disabled"
    };
    match config.source {
        ModuleRuntimeConfigSource::Node => format!("{state} from node override"),
        ModuleRuntimeConfigSource::Cluster => format!("{state} from cluster default"),
        ModuleRuntimeConfigSource::DisabledDefault => "Disabled by default".to_string(),
    }
}

fn module_display_id(module_id: &str) -> String {
    short_one_line(module_id, 72)
}

fn section_skills(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || skills_section_key(&s)
        },
        move |_key| WidgetPod::new(section_skills_body(&s, &r)),
    )
}

fn section_skills_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (
        skills,
        include_all,
        import_path,
        export_path,
        action_error,
        action_status,
        detail,
        text_expanded,
    ) = {
        let settings = shared.settings();
        (
            settings.skills.clone(),
            settings.skills_include_all,
            settings.skill_import_path.clone(),
            settings.skill_export_path.clone(),
            settings.skill_action_error.clone(),
            settings.skill_action_status.clone(),
            settings.skill_detail.clone(),
            settings.skill_text_expanded,
        )
    };

    let mut summary_body = FieldGroup::new()
        .spacing(8.0)
        .with_child(value_row(
            shared,
            reader,
            "Catalog",
            skills_sidebar_preview(&skills),
        ))
        .with_child(switch_row(reader, "Show all versions", include_all, {
            let s = shared.clone();
            move |on| s.settings_mut().set_skills_include_all(on)
        }))
        .with_child(text_row(
            reader,
            "skill-import-path",
            "Import path",
            import_path,
            "/path/to/SKILL.md",
            {
                let s = shared.clone();
                move |value| s.settings_mut().set_skill_import_path(value)
            },
        ));
    let import = {
        let s = shared.clone();
        sui::Button::new("Import skill")
            .icon(sui::IconGlyph::Add)
            .theme_when(clone_reader(reader))
            .on_press(move || {
                s.settings_mut().import_skill_from_path();
            })
    };
    let refresh = {
        let s = shared.clone();
        sui::Button::new("Refresh")
            .icon(sui::IconGlyph::Redo)
            .theme_when(secondary_button_theme(reader))
            .on_press(move || {
                s.settings_mut().reload_skills();
            })
    };
    summary_body = summary_body.with_child(
        FormRow::new(
            "Actions",
            Stack::horizontal()
                .spacing(8.0)
                .alignment(Alignment::Start)
                .with_child(import)
                .with_child(refresh),
        )
        .inline()
        .theme_when(clone_reader(reader)),
    );
    if let Some(error) = action_error {
        summary_body = summary_body.with_child(value_row(shared, reader, "Error", error));
    } else if let Some(status) = action_status {
        summary_body = summary_body.with_child(value_row(shared, reader, "Status", status));
    }

    let summary = FormSection::new("Skills", summary_body)
        .description("Manage imported and generated SKILL.md files used by agents.")
        .theme_when(clone_reader(reader));

    let catalog = if skills.is_empty() {
        let body = FieldGroup::new()
            .spacing(8.0)
            .with_child(value_row(
                shared,
                reader,
                "Status",
                "No skills in the local catalog",
            ))
            .with_child(value_row(
                shared,
                reader,
                "Import",
                "Add a SKILL.md file to make it available to agents.",
            ));
        WidgetPod::new(FormSection::new("Catalog", body).theme_when(clone_reader(reader)))
    } else {
        let items = skills.iter().map(skill_list_item).collect::<Vec<_>>();
        let list = {
            let s = shared.clone();
            ListView::new("skill-catalog")
                .items(items)
                .row_height(MENU_ROW_H)
                .theme_when(clone_reader(reader))
                .selected_when({
                    let s = s.clone();
                    move || s.settings().selected_skill_index()
                })
                .on_change(move |index, _| {
                    s.settings_mut().select_skill_index(index);
                })
        };
        WidgetPod::new(
            FormSection::new("Catalog", list)
                .description("Select a skill version to inspect, activate, or export.")
                .theme_when(clone_reader(reader)),
        )
    };

    let detail = detail
        .map(|detail| {
            WidgetPod::new(skill_detail_section(
                shared,
                reader,
                detail,
                export_path,
                text_expanded,
            ))
        })
        .unwrap_or_else(|| {
            WidgetPod::new(
                FormSection::new(
                    "Selected skill",
                    FieldGroup::new().with_child(value_row(
                        shared,
                        reader,
                        "Status",
                        "Select a skill to view details",
                    )),
                )
                .theme_when(clone_reader(reader)),
            )
        });

    Stack::vertical()
        .spacing(14.0)
        .alignment(Alignment::Stretch)
        .with_child(summary)
        .with_child(PodHost::new(catalog))
        .with_child(PodHost::new(detail))
}

fn skill_detail_section(
    shared: &Shared,
    reader: &ThemeReader,
    detail: SkillDetail,
    export_path: String,
    text_expanded: bool,
) -> impl Widget + use<> {
    let summary = detail.summary.clone();
    let mut body = FieldGroup::new()
        .spacing(8.0)
        .with_child(value_row(
            shared,
            reader,
            "Version",
            summary.version.clone(),
        ))
        .with_child(value_row(
            shared,
            reader,
            "State",
            if summary.active {
                "active"
            } else {
                &summary.status
            },
        ))
        .with_child(value_row(
            shared,
            reader,
            "Source",
            skill_source_label(&summary),
        ))
        .with_child(value_row(
            shared,
            reader,
            "SIFS path",
            short_one_line(&summary.sifs_path, 96),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Content hash",
            short(&summary.content_hash, 48),
        ));
    if let Some(base_hash) = detail.base_hash.as_ref() {
        body = body.with_child(value_row(shared, reader, "Base hash", short(base_hash, 48)));
    }
    if !detail.activation_tags.is_empty() {
        body = body.with_child(value_row(
            shared,
            reader,
            "Tags",
            detail.activation_tags.join(", "),
        ));
    }
    if !detail.required_capabilities.is_empty() {
        body = body.with_child(value_row(
            shared,
            reader,
            "Capabilities",
            detail.required_capabilities.join(", "),
        ));
    }

    body = body.with_child(text_row(
        reader,
        "skill-export-path",
        "Export path",
        export_path,
        "/path/to/SKILL.md",
        {
            let s = shared.clone();
            move |value| s.settings_mut().set_skill_export_path(value)
        },
    ));

    let activate = {
        let s = shared.clone();
        sui::Button::new("Activate")
            .icon(sui::IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled(!summary.active)
            .on_press(move || {
                s.settings_mut().activate_selected_skill();
            })
    };
    let export = {
        let s = shared.clone();
        sui::Button::new("Export skill")
            .icon(sui::IconGlyph::Download)
            .theme_when(secondary_button_theme(reader))
            .on_press(move || {
                s.settings_mut().export_selected_skill();
            })
    };
    let text = {
        let s = shared.clone();
        sui::Button::new(if text_expanded {
            "Hide SKILL.md"
        } else {
            "Show SKILL.md"
        })
        .icon(if text_expanded {
            sui::IconGlyph::ChevronUp
        } else {
            sui::IconGlyph::ChevronDown
        })
        .theme_when(secondary_button_theme(reader))
        .on_press(move || s.settings_mut().toggle_skill_text())
    };
    body = body.with_child(
        FormRow::new(
            "Actions",
            Stack::horizontal()
                .spacing(8.0)
                .alignment(Alignment::Start)
                .with_child(activate)
                .with_child(export)
                .with_child(text),
        )
        .inline()
        .theme_when(clone_reader(reader)),
    );

    if text_expanded {
        body = body.with_child(
            FormRow::new(
                "SKILL.md",
                SizedBox::new().height(220.0).with_child(
                    TextArea::new("skill-markdown")
                        .value(detail.text)
                        .read_only()
                        .theme_when(clone_reader(reader)),
                ),
            )
            .stacked()
            .theme_when(clone_reader(reader)),
        );
    }

    FormSection::new(skill_title(&summary), body)
        .description(short_one_line(&summary.description, 160))
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
        } else if item.schema.is_some() {
            "Edit fields"
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
        if let Some(schema) = &item.schema {
            details =
                details.with_child(config_schema_editor(shared, reader, index, &item, schema));
        } else {
            details = details.with_child(config_detail_editor(shared, reader, index, &item));
        }
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

fn config_schema_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    item: &ConfigItem,
    schema: &ConfigSchema,
) -> impl Widget + use<> {
    let mut stack = Stack::vertical().spacing(8.0).alignment(Alignment::Stretch);
    if !schema.description.trim().is_empty() {
        stack = stack.with_child(muted_label(shared, schema.description.clone()).font_size(11.0));
    }
    stack.with_child(PodHost::new(config_schema_value_editor(
        shared,
        reader,
        index,
        &item.raw_value,
        &schema.value,
        Vec::new(),
        &schema.name,
        "",
    )))
}

fn config_schema_value_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    schema: &ConfigSchemaValue,
    path: Vec<String>,
    label: &str,
    description: &str,
) -> WidgetPod {
    match schema {
        ConfigSchemaValue::Object { fields, .. } => config_schema_object_editor(
            shared,
            reader,
            index,
            value,
            fields,
            path,
            label,
            description,
        ),
        ConfigSchemaValue::Map { value: item_schema } => config_schema_map_editor(
            shared,
            reader,
            index,
            value,
            item_schema,
            path,
            label,
            description,
        ),
        ConfigSchemaValue::List { .. }
        | ConfigSchemaValue::StringList
        | ConfigSchemaValue::Json => config_schema_json_row(
            shared,
            reader,
            index,
            value,
            schema,
            path,
            label,
            description,
        ),
        ConfigSchemaValue::Bool
        | ConfigSchemaValue::I64 { .. }
        | ConfigSchemaValue::U64 { .. }
        | ConfigSchemaValue::F64 { .. }
        | ConfigSchemaValue::String { .. } => config_schema_primitive_row(
            shared,
            reader,
            index,
            value,
            schema,
            path,
            label,
            description,
        ),
    }
}

fn config_schema_object_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    fields: &[ConfigSchemaField],
    path: Vec<String>,
    label: &str,
    description: &str,
) -> WidgetPod {
    let mut group = FieldGroup::new().spacing(8.0);
    if !description.trim().is_empty() {
        group = group.with_child(value_row(shared, reader, label, description));
    }
    for field in fields {
        let mut field_path = path.clone();
        field_path.push(field.name.clone());
        let field_value = value
            .as_object()
            .and_then(|map| map.get(&field.name))
            .unwrap_or(&Value::Null);
        group = group.with_child(PodHost::new(config_schema_value_editor(
            shared,
            reader,
            index,
            field_value,
            &field.value,
            field_path,
            &field.label,
            &field.description,
        )));
    }
    WidgetPod::new(group)
}

fn config_schema_map_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    item_schema: &ConfigSchemaValue,
    path: Vec<String>,
    label: &str,
    description: &str,
) -> WidgetPod {
    let mut group = FieldGroup::new().spacing(8.0);
    let entries = value
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut summary_body = Stack::vertical()
        .spacing(4.0)
        .alignment(Alignment::Stretch)
        .with_child(muted_label(shared, format!("{} entries", entries.len())).font_size(12.0));
    if !description.trim().is_empty() {
        summary_body =
            summary_body.with_child(muted_label(shared, description.to_string()).font_size(11.0));
    }
    group = group.with_child(
        FormRow::new(label, summary_body)
            .stacked()
            .theme_when(clone_reader(reader)),
    );
    for (entry_key, entry_value) in entries {
        let mut entry_path = path.clone();
        entry_path.push(entry_key.clone());
        match item_schema {
            ConfigSchemaValue::Object { fields, .. } => {
                for field in fields {
                    let mut field_path = entry_path.clone();
                    field_path.push(field.name.clone());
                    let field_value = entry_value
                        .as_object()
                        .and_then(|map| map.get(&field.name))
                        .unwrap_or(&Value::Null);
                    let field_label = format!(
                        "{} {}",
                        short_one_line(&entry_key, 42),
                        field.label.to_ascii_lowercase()
                    );
                    group = group.with_child(PodHost::new(config_schema_value_editor(
                        shared,
                        reader,
                        index,
                        field_value,
                        &field.value,
                        field_path,
                        &field_label,
                        &field.description,
                    )));
                }
            }
            other => {
                group = group.with_child(PodHost::new(config_schema_value_editor(
                    shared,
                    reader,
                    index,
                    &entry_value,
                    other,
                    entry_path,
                    &entry_key,
                    "",
                )));
            }
        }
    }
    WidgetPod::new(group)
}

fn config_schema_primitive_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    schema: &ConfigSchemaValue,
    path: Vec<String>,
    label: &str,
    description: &str,
) -> WidgetPod {
    match schema {
        ConfigSchemaValue::Bool => {
            let current = value.as_bool().unwrap_or(false);
            let row = switch_row(reader, label, current, {
                let s = shared.clone();
                let path = path.clone();
                move |enabled| {
                    s.settings_mut()
                        .set_config_schema_bool(index, path.clone(), enabled);
                }
            });
            WidgetPod::new(row)
        }
        ConfigSchemaValue::String { choices, secret } if !choices.is_empty() && !*secret => {
            let options = choices
                .iter()
                .map(|choice| {
                    if choice.label.trim().is_empty() {
                        choice.value.clone()
                    } else {
                        choice.label.clone()
                    }
                })
                .collect::<Vec<_>>();
            let select = Select::new(schema_control_name(index, &path))
                .options(options)
                .selected_when({
                    let s = shared.clone();
                    let path = path.clone();
                    let choices = choices.clone();
                    move || {
                        let item = s.settings().config_items.get(index)?.clone();
                        let current = config_value_at_path(&item.raw_value, &path)?.as_str()?;
                        choices.iter().position(|choice| choice.value == current)
                    }
                })
                .theme_when(clone_reader(reader))
                .on_change({
                    let s = shared.clone();
                    let path = path.clone();
                    let choices = choices.clone();
                    move |choice_index, _label| {
                        if let Some(choice) = choices.get(choice_index) {
                            s.settings_mut().set_config_schema_value(
                                index,
                                path.clone(),
                                Value::String(choice.value.clone()),
                            );
                        }
                    }
                });
            let mut body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(select);
            if !description.trim().is_empty() {
                body =
                    body.with_child(muted_label(shared, description.to_string()).font_size(11.0));
            }
            WidgetPod::new(
                FormRow::new(label, body)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            )
        }
        ConfigSchemaValue::String { secret, .. } if *secret => {
            WidgetPod::new(value_row(shared, reader, label, "Configured secret"))
        }
        ConfigSchemaValue::String { .. }
        | ConfigSchemaValue::I64 { .. }
        | ConfigSchemaValue::U64 { .. }
        | ConfigSchemaValue::F64 { .. } => {
            let value_text = schema_scalar_edit_value(schema, value);
            let mut body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(
                    TextInput::new(schema_control_name(index, &path))
                        .value(value_text)
                        .placeholder(schema_placeholder(schema))
                        .theme_when(clone_reader(reader))
                        .on_change({
                            let s = shared.clone();
                            let path = path.clone();
                            let schema = schema.clone();
                            move |raw| {
                                s.settings_mut().set_config_schema_text(
                                    index,
                                    path.clone(),
                                    schema.clone(),
                                    &raw,
                                );
                            }
                        }),
                );
            let error_key = {
                let settings = shared.settings();
                settings
                    .config_items
                    .get(index)
                    .map(|item| config_schema_field_key(&item.key, &path))
            };
            let detail = error_key
                .and_then(|key| shared.settings().field_errors.get(&key).cloned())
                .unwrap_or_else(|| schema_field_detail(schema, description));
            if !detail.trim().is_empty() {
                body = body.with_child(muted_label(shared, detail).font_size(11.0));
            }
            WidgetPod::new(
                FormRow::new(label, body)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            )
        }
        _ => config_schema_json_row(
            shared,
            reader,
            index,
            value,
            schema,
            path,
            label,
            description,
        ),
    }
}

fn config_schema_json_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    schema: &ConfigSchemaValue,
    path: Vec<String>,
    label: &str,
    description: &str,
) -> WidgetPod {
    let value_text = if value.is_null() {
        String::new()
    } else {
        serde_json::to_string_pretty(value).unwrap_or_default()
    };
    let mut body = Stack::vertical()
        .spacing(4.0)
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new().height(92.0).with_child(
                TextArea::new(schema_control_name(index, &path))
                    .value(value_text)
                    .placeholder("JSON value")
                    .theme_when(clone_reader(reader))
                    .on_change({
                        let s = shared.clone();
                        let path = path.clone();
                        let schema = schema.clone();
                        move |raw| {
                            s.settings_mut().set_config_schema_text(
                                index,
                                path.clone(),
                                schema.clone(),
                                &raw,
                            );
                        }
                    }),
            ),
        );
    let detail = schema_field_detail(schema, description);
    if !detail.trim().is_empty() {
        body = body.with_child(muted_label(shared, detail).font_size(11.0));
    }
    WidgetPod::new(
        FormRow::new(label, body)
            .stacked()
            .theme_when(clone_reader(reader)),
    )
}

fn config_value_at_path<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

fn schema_control_name(index: usize, path: &[String]) -> String {
    if path.is_empty() {
        return format!("config-{index}-schema-root");
    }
    let path = path
        .iter()
        .map(|segment| {
            segment
                .chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                        ch
                    } else {
                        '-'
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("-");
    format!("config-{index}-schema-{path}")
}

fn schema_scalar_edit_value(schema: &ConfigSchemaValue, value: &Value) -> String {
    match schema {
        ConfigSchemaValue::String { .. } => value.as_str().unwrap_or_default().to_string(),
        ConfigSchemaValue::I64 { .. }
        | ConfigSchemaValue::U64 { .. }
        | ConfigSchemaValue::F64 { .. } => {
            if value.is_null() {
                String::new()
            } else {
                value.to_string()
            }
        }
        _ => String::new(),
    }
}

fn schema_placeholder(schema: &ConfigSchemaValue) -> &'static str {
    match schema {
        ConfigSchemaValue::String { .. } => "Text",
        ConfigSchemaValue::I64 { .. } => "Integer",
        ConfigSchemaValue::U64 { .. } => "Unsigned integer",
        ConfigSchemaValue::F64 { .. } => "Number",
        _ => "Value",
    }
}

fn schema_field_detail(schema: &ConfigSchemaValue, description: &str) -> String {
    let mut details = Vec::new();
    if !description.trim().is_empty() {
        details.push(description.to_string());
    }
    if let Some(constraint) = schema_constraint_summary(schema) {
        details.push(constraint);
    }
    details.join(" · ")
}

fn schema_constraint_summary(schema: &ConfigSchemaValue) -> Option<String> {
    match schema {
        ConfigSchemaValue::I64 { range }
        | ConfigSchemaValue::U64 { range }
        | ConfigSchemaValue::F64 { range } => range.as_ref().and_then(format_number_range),
        ConfigSchemaValue::String { choices, .. } if !choices.is_empty() => Some(format!(
            "Allowed: {}",
            choices
                .iter()
                .map(|choice| choice.value.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
        _ => None,
    }
}

fn format_number_range(range: &sinomo_runtime::config::ConfigNumberRange) -> Option<String> {
    match (range.min, range.max) {
        (Some(min), Some(max)) => Some(format!("Range: {min} to {max}")),
        (Some(min), None) => Some(format!("Minimum: {min}")),
        (None, Some(max)) => Some(format!("Maximum: {max}")),
        (None, None) => None,
    }
}

fn config_metadata_line(item: &ConfigItem) -> String {
    let kind = match item.edit_kind {
        ConfigEditKind::Bool => "Boolean",
        ConfigEditKind::Text => "Text",
        ConfigEditKind::Number => "Number",
        ConfigEditKind::Json => "JSON",
    };
    let owner = item.owner.trim();
    let schema = item
        .schema
        .as_ref()
        .map(|schema| schema.name.as_str())
        .unwrap_or(kind);
    if owner.is_empty() {
        format!("{} · {schema}", item.source)
    } else {
        format!("{} · {owner} · {schema}", item.source)
    }
}

fn config_value_summary(item: &ConfigItem) -> String {
    if let Some(schema) = &item.schema {
        let mut summary = config_schema_value_summary(&item.raw_value, &schema.value, &schema.name);
        if schema_has_secret(&schema.value) || contains_sensitive_json_field(&item.raw_value) {
            summary.push_str(" · sensitive fields redacted");
        }
        return summary;
    }
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

fn config_schema_value_summary(value: &Value, schema: &ConfigSchemaValue, name: &str) -> String {
    match schema {
        ConfigSchemaValue::Object { fields, .. } => {
            let Some(map) = value.as_object() else {
                return format!("{name} · unset");
            };
            let mut parts = Vec::new();
            for field in fields.iter().take(2) {
                let value = map.get(&field.name).unwrap_or(&Value::Null);
                parts.push(format!(
                    "{}: {}",
                    field.label,
                    config_schema_inline_summary(value, &field.value)
                ));
            }
            if parts.is_empty() {
                format!("{name} · {} fields", map.len())
            } else {
                format!("{name} · {}", parts.join(" · "))
            }
        }
        other => format!("{name} · {}", config_schema_inline_summary(value, other)),
    }
}

fn config_schema_inline_summary(value: &Value, schema: &ConfigSchemaValue) -> String {
    match schema {
        ConfigSchemaValue::Bool => match value.as_bool() {
            Some(true) => "enabled".to_string(),
            Some(false) => "disabled".to_string(),
            None => "unset".to_string(),
        },
        ConfigSchemaValue::I64 { .. }
        | ConfigSchemaValue::U64 { .. }
        | ConfigSchemaValue::F64 { .. } => {
            if value.is_null() {
                "unset".to_string()
            } else {
                value.to_string()
            }
        }
        ConfigSchemaValue::String { secret, .. } if *secret => "configured secret".to_string(),
        ConfigSchemaValue::String { .. } => value
            .as_str()
            .map(|text| short_one_line(text, 64))
            .unwrap_or_else(|| "unset".to_string()),
        ConfigSchemaValue::StringList | ConfigSchemaValue::List { .. } => value
            .as_array()
            .map(|items| format!("{} items", items.len()))
            .unwrap_or_else(|| "0 items".to_string()),
        ConfigSchemaValue::Map { .. } => value
            .as_object()
            .map(|items| format!("{} entries", items.len()))
            .unwrap_or_else(|| "0 entries".to_string()),
        ConfigSchemaValue::Object { .. } => value
            .as_object()
            .map(|items| format!("{} fields", items.len()))
            .unwrap_or_else(|| "unset".to_string()),
        ConfigSchemaValue::Json => config_json_value_summary(value),
    }
}

fn config_json_value_summary(value: &Value) -> String {
    match value {
        Value::Null => "unset".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => short_one_line(value, 64),
        Value::Array(items) => format!("{} items", items.len()),
        Value::Object(map) => format!("{} fields", map.len()),
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
    if item
        .schema
        .as_ref()
        .is_some_and(|schema| schema_has_secret(&schema.value))
    {
        return true;
    }
    if item.edit_kind != ConfigEditKind::Json {
        return false;
    }
    serde_json::from_str::<Value>(&item.edit_value)
        .map(|value| contains_sensitive_json_field(&value))
        .unwrap_or(false)
}

fn schema_has_secret(schema: &ConfigSchemaValue) -> bool {
    match schema {
        ConfigSchemaValue::String { secret, .. } => *secret,
        ConfigSchemaValue::Object { fields, .. } => {
            fields.iter().any(|field| schema_has_secret(&field.value))
        }
        ConfigSchemaValue::Map { value } | ConfigSchemaValue::List { value } => {
            schema_has_secret(value)
        }
        _ => false,
    }
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
        .with_child(section_modules(shared, reader))
        .with_child(section_skills(shared, reader))
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

fn app_surface_summary(role: &str) -> String {
    let role = role.trim();
    match role {
        "" => "app surface".to_string(),
        "workstation" => "desktop app surface".to_string(),
        _ => format!("{role} app surface"),
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

    fn provider_section_signature_for_state(settings: &SettingsState) -> String {
        let mut parts = vec![
            format!("action-error:{:?}", settings.provider_action_error),
            format!("action-status:{:?}", settings.provider_action_status),
            format!("remove:{:?}", settings.provider_remove_confirming),
        ];
        parts.extend(settings.provider_forms.iter().map(|form| {
            format!(
                "{}@{}:{}:{}",
                form.provider, form.account, form.is_default, form.default_model
            )
        }));
        parts.join("\u{1f}")
    }

    fn temp_skill_backend(label: &str) -> (Backend, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "sinomo-app-settings-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let backend = Backend::local_fallback_for_data_dir(SinomoDataDir::new(root.clone()));
        (backend, root)
    }

    fn write_skill_file(root: &std::path::Path, name: &str, body: &str) -> PathBuf {
        let path = root.join("skills").join(name).join("SKILL.md");
        std::fs::create_dir_all(path.parent().expect("skill parent")).expect("skill dir");
        std::fs::write(
            &path,
            format!(
                "---\nname: {name}\ndescription: Skill for Settings tests.\n---\n\n# {name}\n\n{body}\n"
            ),
        )
        .expect("skill file");
        path
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
    fn schema_config_edit_updates_nested_module_field() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let idx = state
            .config_items
            .iter()
            .position(|item| item.key == sinomo_runtime::module::MODULE_NODE_CONFIG_KEY)
            .expect("module node config item");
        assert_eq!(
            state.config_items[idx]
                .schema
                .as_ref()
                .map(|schema| schema.name.as_str()),
            Some("ModuleRuntimeConfig")
        );

        let path = vec![
            "modules".to_string(),
            "observe".to_string(),
            "enabled".to_string(),
        ];
        state.set_config_schema_bool(idx, path.clone(), true);
        assert!(
            !state.field_errors.contains_key(&config_schema_field_key(
                sinomo_runtime::module::MODULE_NODE_CONFIG_KEY,
                &path
            )),
            "valid nested schema write should not record an error"
        );
        assert_eq!(
            config_value_at_path(&state.config_items[idx].raw_value, &path)
                .and_then(Value::as_bool),
            Some(true)
        );

        let stored = state
            .config
            .get(sinomo_runtime::module::MODULE_NODE_CONFIG_KEY)
            .expect("stored module config");
        assert_eq!(
            config_value_at_path(&stored.value, &path).and_then(Value::as_bool),
            Some(true)
        );
        assert!(config_value_summary(&state.config_items[idx]).contains("ModuleRuntimeConfig"));
        assert!(config_value_summary(&state.config_items[idx]).contains("1 entries"));
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
    fn skills_state_imports_exports_and_activates_versions() {
        let (backend, root) = temp_skill_backend("skills-state");
        let first_path = write_skill_file(&root, "shortcut-skill", "The shortcut is alpha-lane.");
        let second_path =
            write_skill_file(&root, "shortcut-skill-v2", "The shortcut is beta-lane.");

        let mut state = SettingsState::new(&backend);
        assert!(state.skills.is_empty());

        state.set_skill_import_path(first_path.display().to_string());
        assert!(state.import_skill_from_path());
        assert_eq!(state.skills.len(), 1);
        assert_eq!(
            state.skill_selected,
            Some(("shortcut-skill".to_string(), "1.0.0".to_string()))
        );
        assert!(
            state
                .skill_detail
                .as_ref()
                .expect("skill detail")
                .text
                .contains("alpha-lane")
        );

        sinomo_cli::app_api::import_skill(
            backend.data_dir.as_ref(),
            &second_path,
            Some("shortcut-skill".to_string()),
            Some("2.0.0".to_string()),
            false,
        )
        .expect("inactive skill version");
        state.reload_skills();
        let v2 = state
            .skills
            .iter()
            .position(|skill| skill.id == "shortcut-skill" && skill.version == "2.0.0")
            .expect("v2 skill");
        state.select_skill_index(v2);
        assert!(
            !state
                .skill_detail
                .as_ref()
                .expect("v2 detail")
                .summary
                .active
        );
        assert!(state.activate_selected_skill());
        assert!(
            state
                .skill_detail
                .as_ref()
                .expect("activated detail")
                .summary
                .active
        );

        let export_path = root.join("exports").join("shortcut-skill.md");
        state.set_skill_export_path(export_path.display().to_string());
        assert!(state.export_selected_skill());
        let exported = std::fs::read_to_string(&export_path).expect("exported skill");
        assert!(exported.contains("name: shortcut-skill"));
        assert!(exported.contains("beta-lane"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn settings_skills_section_renders_management_controls() -> sui::Result<()> {
        let (backend, root) = temp_skill_backend("skills-render");
        let skill_path = write_skill_file(&root, "render-skill", "Rendered skill detail.");
        sinomo_cli::app_api::import_skill(backend.data_dir.as_ref(), &skill_path, None, None, true)
            .expect("import render skill");
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Skills);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings skills")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window.get_by_text("Skills").expect().to_be_visible()?;
        window
            .get_by_text("render-skill @ 1.0.0")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("skill-import-path")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("skill-export-path")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Import skill")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Export skill")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show SKILL.md")
            .click()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("skill-markdown")
            .expect()
            .to_have_value(
                "---\nname: render-skill\ndescription: Skill for Settings tests.\n---\n\n# render-skill\n\nRendered skill detail.\n",
            )?;

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn provider_model_edit_reports_saved_status_and_persists() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let config = shared.settings().config.clone();
            add_provider_account(config.as_ref(), "openai", "work", "sk-work", "gpt-5", "")
                .expect("add provider account");
        }
        shared.settings_mut().reload();
        let before_signature = provider_section_signature(&shared);

        shared
            .settings_mut()
            .set_provider_field(0, ProviderField::Model, "gpt-5.1");

        assert_eq!(
            shared.settings().provider_action_status.clone(),
            Some(ProviderActionStatus {
                provider: "openai".to_string(),
                account: "work".to_string(),
                message: "Default model saved to lm.providers".to_string(),
            })
        );
        let config_client = shared.settings().config.clone();
        let config = crate::backend::config::load_provider_config(config_client.as_ref())
            .expect("provider config");
        let account = config
            .accounts
            .iter()
            .find(|account| account.provider == "openai" && account.account == "work")
            .expect("work account");
        assert_eq!(account.default_model.as_deref(), Some("gpt-5.1"));

        assert_ne!(
            before_signature,
            provider_section_signature(&shared),
            "provider saved status must invalidate the retained provider section"
        );
    }

    #[test]
    fn provider_chat_default_action_reports_status_and_persists() {
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
        .expect("add openai provider account");
        add_provider_account(
            state.config.as_ref(),
            "deepseek",
            "default",
            "sk-deep",
            "deepseek-v4-flash",
            "",
        )
        .expect("add deepseek provider account");
        state.reload();
        let before_signature = provider_section_signature_for_state(&state);

        assert!(state.set_provider_chat_default(1));

        assert_eq!(
            state.provider_action_status.as_ref(),
            Some(&ProviderActionStatus {
                provider: "deepseek".to_string(),
                account: "default".to_string(),
                message: "Chat default saved to lm.providers".to_string(),
            })
        );
        let config = crate::backend::config::load_provider_config(state.config.as_ref())
            .expect("provider config");
        assert_eq!(config.default_provider.as_deref(), Some("deepseek"));
        assert_eq!(config.default_account.as_deref(), Some("default"));
        assert_eq!(config.default_model.as_deref(), Some("deepseek-v4-flash"));
        assert!(state.provider_forms[1].is_default);
        assert!(!state.provider_forms[0].is_default);
        assert_ne!(
            before_signature,
            provider_section_signature_for_state(&state),
            "chat-default changes must invalidate the retained provider section"
        );
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
    fn module_settings_load_catalog_and_save_local_override() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        assert!(
            state.module_selected_node_id.is_some(),
            "module settings should select the local node by default"
        );
        assert!(
            state
                .module_snapshot
                .modules
                .iter()
                .any(|module| module.module_id
                    == sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID),
            "module settings should include built-in module targets"
        );

        assert!(state.set_node_module_override(
            &backend,
            sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID,
            Some(true),
        ));
        let selected = state
            .module_snapshot
            .selected_node
            .as_ref()
            .expect("selected node");
        assert_eq!(
            selected
                .config
                .setting(sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID)
                .map(|setting| setting.enabled),
            Some(true)
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
    fn provider_overview_summarizes_ready_and_chat_default() {
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
        .expect("add openai provider account");
        add_provider_account(
            state.config.as_ref(),
            "deepseek",
            "default",
            "sk-deep",
            "deepseek-v4-flash",
            "",
        )
        .expect("add deepseek provider account");
        state.reload();

        assert_eq!(
            provider_accounts_summary(&state.provider_forms),
            "2 total · 2 enabled"
        );
        assert_eq!(provider_ready_summary(&state.provider_forms), "2 ready");
        assert_eq!(provider_sidebar_preview(&state.provider_forms), "2 ready");
        assert_eq!(
            provider_chat_default_summary(&state.provider_forms),
            "OpenAI (work) / gpt-5"
        );

        set_provider_enabled(state.config.as_ref(), "openai", "work", false)
            .expect("disable default provider account");
        state.reload();
        assert_eq!(provider_ready_summary(&state.provider_forms), "1 ready");
        assert_eq!(
            provider_chat_default_summary(&state.provider_forms),
            "OpenAI (work) / gpt-5 (disabled)"
        );
    }

    #[test]
    fn provider_list_overview_exposes_operational_summary() -> sui::Result<()> {
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

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings providers overview")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            window.get_by_text("1 total · 1 enabled").count()? >= 1,
            "provider overview should report total and enabled accounts"
        );
        assert!(
            window.get_by_text("1 ready").count()? >= 1,
            "provider overview should report accounts ready for chat"
        );
        assert!(
            window.get_by_text("OpenAI (work) / gpt-5").count()? >= 1,
            "provider overview should report the chat default and model"
        );

        Ok(())
    }

    #[test]
    fn provider_card_can_set_chat_default_from_card_action() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let config = shared.settings().config.clone();
            add_provider_account(config.as_ref(), "openai", "work", "sk-work", "gpt-5", "")
                .expect("add openai provider account");
            add_provider_account(
                config.as_ref(),
                "deepseek",
                "default",
                "sk-deep",
                "deepseek-v4-flash",
                "",
            )
            .expect("add deepseek provider account");
        }
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.reload();
            settings.open(SettingsSection::Providers);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings provider chat default")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            window.get_by_text("Chat default").count()? >= 1,
            "provider card should identify the current chat default"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Use for chat")
            .click()?;
        window
            .get_by_text("Chat default saved to lm.providers")
            .expect()
            .to_be_visible()?;

        let config =
            crate::backend::config::load_provider_config(shared.settings().config.as_ref())
                .expect("provider config");
        assert_eq!(config.default_provider.as_deref(), Some("deepseek"));
        assert_eq!(config.default_account.as_deref(), Some("default"));
        assert_eq!(config.default_model.as_deref(), Some("deepseek-v4-flash"));

        Ok(())
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
    fn provider_card_exposes_last_saved_status() -> sui::Result<()> {
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
            settings.set_provider_status("openai", "work", "Default model saved to lm.providers");
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings provider saved status")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            window
                .get_by_text("Default model saved to lm.providers")
                .count()?
                >= 1,
            "provider card should expose write-through save feedback"
        );

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
            settings.cluster_node_id = Some("daemon_node_about_test".to_string());
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
        assert!(window.get_by_text("voter app surface").count()? >= 1);
        assert!(window.get_by_text("daemon_node_about_test").count()? >= 1);
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
        let raw_value =
            serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()));
        ConfigItem {
            key: key.to_string(),
            owner: "test".to_string(),
            description: "test setting".to_string(),
            value: raw.to_string(),
            source: "cluster",
            bool_value: None,
            edit_kind: kind,
            edit_value: raw.to_string(),
            raw_value,
            schema: None,
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
    fn node_settings_summary_keeps_ids_in_details() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::Node);
            settings.node_id = "01node-full-id-for-test".to_string();
            settings.role = "workstation".to_string();
            settings.mode_label = "Cluster".to_string();
            settings.status_detail = "Connected to daemon at 127.0.0.1:21353".to_string();
            settings.cluster_node_id = Some("01daemon-full-id-for-test".to_string());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings node summary")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window.get_by_text("App surface").expect().to_be_visible()?;
        window
            .get_by_text("desktop app surface")
            .expect()
            .to_be_visible()?;
        window.get_by_text("Connection").expect().to_be_visible()?;
        window.get_by_text("Cluster").expect().to_be_visible()?;
        window
            .get_by_text("App surface ID")
            .expect()
            .to_be_hidden()?;
        window
            .get_by_text("Daemon node ID")
            .expect()
            .to_be_hidden()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("app-surface-full-id")
            .expect()
            .to_be_hidden()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("daemon-node-full-id")
            .expect()
            .to_be_hidden()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show detail")
            .click()?;
        window
            .get_by_text("App surface ID")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Daemon node ID")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("app-surface-full-id")
            .expect()
            .to_have_value("01node-full-id-for-test")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("daemon-node-full-id")
            .expect()
            .to_have_value("01daemon-full-id-for-test")?;

        Ok(())
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
        assert_eq!(issues[1].title, "SIFS access");
        assert_eq!(
            connection_issue_summary(&issues[1]),
            "SIFS file access needs recovery."
        );

        {
            let mut files = shared.files_mut();
            files.error = None;
            files.details_error = Some("selected object metadata failed".to_string());
        }
        let issues = connection_issues(&shared);
        assert_eq!(issues.len(), 2);
        assert_eq!(
            issues[1].detail,
            "SIFS metadata unavailable: selected object metadata failed"
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
    fn cluster_settings_open_dashboard_action_switches_to_cluster_tab() -> sui::Result<()> {
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
                    .title("Cluster settings dashboard action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open Cluster dashboard")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Cluster);

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
