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
//! snapshot-seed-plus-`on_change` writeback. Async provider device-login status is
//! rebuilt as ordinary `Label` / `Link` composition when its typed state changes.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use serde_json::Value;
use sinomo_agent::{AgentDefinition, AgentOutputFormat, AgentPackageDefinition, AgentRole};
use sinomo_cli::app_api::{
    AgentSummary, ProviderSummary, SkillDetail, SkillMutationResult, SkillSummary, agent_summaries,
};
use sui::{
    Alignment, ArrangeCtx, Constraints, ContextMenu, ControlSize, DefaultTheme, Event, EventCtx,
    FieldGroup, Flex, FlexItem, FlexWrap, FontWeight, FormRow, FormSection, IconGlyph, Label,
    ListItem, ListView, MeasureCtx, MenuItem, NumberInput, PaintCtx, ProgressBar, RebuildOnChange,
    Rect, ScrollState, ScrollView, Select, SelectionScope, SemanticTone, SemanticsCtx, Separator,
    Size, SizedBox, Spinner, Stack, StatusBadge, Switch, SwitchView, TabBar, TextArea, TextCommand,
    TextInput, ThemeTextToken, TrailingSlotRow, VirtualList, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor,
};

use sinomo_runtime::config::{
    ConfigClient, ConfigKey, ConfigSchema, ConfigSchemaField, ConfigSchemaValue, ConfigWriteTarget,
};
use sinomo_runtime::local::SinomoDataDir;

use crate::backend::Backend;
use crate::backend::agents::{
    AgentConfigSnapshot, AgentConfigSummary, AgentReadiness, AgentValidationReport,
    agent_definition_is_editable, agent_role_label, agent_source_label,
    built_in_agent_config_snapshot,
};
use crate::backend::cluster::{AppClusterInvite, AppClusterStatus, ClusterSnapshot};
use crate::backend::config::{
    CONFIGURABLE_PROVIDERS, ConfigEditKind, ConfigItem, ProviderForm, REASONING_EFFORT_OPTIONS,
    TEXT_VERBOSITY_OPTIONS, add_provider_account, cluster_provider_summaries, load_provider_health,
    mark_provider_health_stale, provider_default_model, provider_display_name, provider_forms,
    reasoning_effort_index, remove_provider_account, remove_provider_health, resolve_items,
    run_provider_device_login, set_bool, set_provider_api_key, set_provider_base_url,
    set_provider_chat_default, set_provider_effort, set_provider_enabled, set_provider_model,
    set_provider_reasoning_budget, set_provider_text_verbosity, set_provider_vendor_options,
    set_value, test_provider_connection, text_verbosity_index,
};
use crate::backend::modules::{ModuleConfigSnapshot, ModuleObservedState};
use crate::rendering::AppRenderMode;
use crate::state::{Shared, Tab};
use crate::theme::{AppTextScale, AppThemeMode, Palette};
use crate::ui::widgets;
use sinomo_runtime::module::ModuleRuntimeConfigSource;

/// Budget for each SIFS-backed section (skills, stored agents) of the full settings
/// snapshot; past this the section reports itself unavailable instead of wedging the
/// preload while the distributed file system is unhealthy.
const SETTINGS_SIFS_LOAD_TIMEOUT: Duration = Duration::from_secs(8);

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
    Agents,
    Modules,
    Skills,
    Configuration,
    About,
}

impl SettingsSection {
    pub const ALL: [SettingsSection; 10] = [
        SettingsSection::Appearance,
        SettingsSection::Connection,
        SettingsSection::Node,
        SettingsSection::Cluster,
        SettingsSection::Providers,
        SettingsSection::Agents,
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
            SettingsSection::Agents => "Agents",
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
    pub join_confirming: bool,
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
    provider_health: HashMap<String, ProviderHealthState>,
    pub agent_snapshot: AgentConfigSnapshot,
    pub agent_selected_id: Option<String>,
    pub agent_form: AgentForm,
    pub agent_action_error: Option<String>,
    pub agent_action_status: Option<String>,
    pub agent_delete_confirming: Option<String>,
    pub agent_import_path: String,
    pub agent_export_path: String,
    pub agent_validation_report: Option<AgentValidationReport>,
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
    /// Text controls write drafts here. A daemon/config write only starts from an explicit Save
    /// action, so typing never waits for the RPC timeout and failed saves retain the user's input.
    field_drafts: HashMap<String, String>,
    /// One typed, whole-definition draft per schema-backed config key. Field editors mutate this
    /// value locally and an explicit Save writes the complete validated definition, so sibling
    /// edits compose instead of racing stale whole-object writes.
    config_drafts: HashMap<String, Value>,
    /// Rebuild only for structural draft changes (list/map add/remove, optional unset, Reset).
    /// Scalar text controls retain their own edit/cursor state between keystrokes.
    config_ui_revision: u64,
    initial_load_rx: Option<Receiver<SettingsInitialLoadResult>>,
    initial_load_started: bool,
    operation_lane: Option<SettingsOperationLane>,
    next_operation_request_id: u64,
    pending_operations: HashMap<SettingsOperationScope, u64>,
    provider_refresh_needed: bool,
    provider_refresh_health_invalidation: Option<(String, String)>,
    server_generations: SettingsGenerations,
    draft_generations: SettingsGenerations,
    connection_detail_expanded: Vec<Tab>,
    node_detail_expanded: bool,
    advanced_detail_expanded: Vec<String>,
    provider_detail_expanded: Vec<String>,
    provider_storage_detail_expanded: bool,
    provider_remove_confirming: Option<(String, String)>,
}

#[derive(Clone, Debug, PartialEq)]
struct ConfigItemStructure {
    key: String,
    owner: String,
    source: &'static str,
    module_id: Option<String>,
    edit_kind: ConfigEditKind,
    bool_value: Option<bool>,
    read_only: bool,
    raw_value: Value,
    schema: Option<ConfigSchema>,
}

impl From<&ConfigItem> for ConfigItemStructure {
    fn from(item: &ConfigItem) -> Self {
        Self {
            key: item.key.clone(),
            owner: item.owner.clone(),
            source: item.source,
            module_id: item.module_id.clone(),
            edit_kind: item.edit_kind,
            bool_value: item.bool_value,
            read_only: item.read_only,
            raw_value: item.raw_value.clone(),
            schema: item.schema.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsConnectionIssueStructure {
    title: &'static str,
    detail: String,
    tab: Tab,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsConnectionStructure {
    theme: (AppThemeMode, AppTextScale),
    mode_label: String,
    status_detail: String,
    issues: Vec<SettingsConnectionIssueStructure>,
    expanded: Vec<Tab>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsNodeStructure {
    theme: (AppThemeMode, AppTextScale),
    node_id: String,
    role: String,
    mode_label: String,
    status_detail: String,
    clustered: bool,
    cluster_node_id: Option<String>,
    cluster_summary: Option<String>,
    expanded: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsClusterStructure {
    theme: (AppThemeMode, AppTextScale),
    clustered: bool,
    cluster_summary: Option<String>,
    invite: Option<(String, String, String, String)>,
    leave_confirming: bool,
    join_confirming: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct SettingsConfigurationStructure {
    theme: (AppThemeMode, AppTextScale),
    items: Vec<ConfigItemStructure>,
    expanded: Vec<String>,
    ui_revision: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct SettingsProviderStructure {
    theme: (AppThemeMode, AppTextScale),
    add_open: bool,
    add_provider_index: usize,
    action_error: Option<String>,
    action_status: Option<ProviderActionStatus>,
    remove_confirming: Option<(String, String)>,
    expanded: Vec<String>,
    storage_expanded: bool,
    providers: Vec<(ProviderForm, ProviderHealthState)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsAgentsStructure {
    theme: (AppThemeMode, AppTextScale),
    agents: Vec<AgentConfigSummary>,
    error: Option<String>,
    selected_id: Option<String>,
    delete_confirming: Option<String>,
    validation_report: Option<AgentValidationReport>,
    editor_previous_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsAgentRow {
    theme: (AppThemeMode, AppTextScale),
    agent: AgentConfigSummary,
}

#[derive(Clone, Debug, PartialEq)]
struct SettingsModuleNodeStructure {
    node_id: String,
    display_name: String,
    availability: String,
    local: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct SettingsModuleSummaryStructure {
    module_id: String,
    label: String,
    backend: String,
    detail: String,
    desired_enabled: Option<bool>,
    effective_source: ModuleRuntimeConfigSource,
    effective_enabled: bool,
    observed: ModuleObservedState,
    observed_detail: String,
}

#[derive(Clone, Debug, PartialEq)]
struct SettingsSelectedModuleNodeStructure {
    node_id: String,
    error: Option<String>,
    config: Vec<(String, bool)>,
}

#[derive(Clone, Debug, PartialEq)]
struct SettingsModulesStructure {
    theme: (AppThemeMode, AppTextScale),
    selected_node_id: Option<String>,
    action_error: Option<String>,
    action_status: Option<String>,
    warnings: Vec<String>,
    config_ui_revision: u64,
    nodes: Vec<SettingsModuleNodeStructure>,
    modules: Vec<SettingsModuleSummaryStructure>,
    cluster_config: Vec<(String, bool)>,
    selected_node: Option<SettingsSelectedModuleNodeStructure>,
    module_settings: Vec<ConfigItemStructure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsSkillsStructure {
    theme: (AppThemeMode, AppTextScale),
    include_all: bool,
    selected: Option<(String, String)>,
    detail_content_hash: Option<String>,
    text_expanded: bool,
    skills: Vec<SkillSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsSkillRow {
    theme: (AppThemeMode, AppTextScale),
    skill: SkillSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsDeviceLoginStructure {
    primary: String,
    prompt_url: Option<String>,
    error: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct SettingsStructureSnapshot {
    selected: Option<SettingsSection>,
    theme: (AppThemeMode, AppTextScale),
    connection: SettingsConnectionStructure,
    node: SettingsNodeStructure,
    cluster: SettingsClusterStructure,
    configuration: SettingsConfigurationStructure,
    providers: SettingsProviderStructure,
    agents: SettingsAgentsStructure,
    modules: SettingsModulesStructure,
    skills: SettingsSkillsStructure,
    device_logins: Vec<SettingsDeviceLoginStructure>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SettingsStructureKey(Option<Box<SettingsStructureSnapshot>>);

impl SettingsStructureKey {
    pub(crate) fn empty() -> Self {
        Self(None)
    }
}

pub(crate) fn settings_structure_key(shared: &Shared) -> SettingsStructureKey {
    let theme = {
        let app = shared.borrow();
        (app.theme_mode, app.text_scale)
    };
    let issues = connection_issues(shared)
        .into_iter()
        .map(|issue| SettingsConnectionIssueStructure {
            title: issue.title,
            detail: issue.detail,
            tab: issue.tab,
        })
        .collect();
    let (
        selected,
        connection,
        node,
        cluster,
        configuration,
        providers,
        agents,
        modules,
        skills,
        device_login_count,
    ) = {
        let settings = shared.settings();
        let invite = settings.invite_result.as_ref().map(|invite| {
            (
                invite.uri.clone(),
                invite.join_command.clone(),
                invite.expires_at.to_string(),
                invite.persisted_key.clone(),
            )
        });
        let configuration = SettingsConfigurationStructure {
            theme,
            items: settings
                .config_items
                .iter()
                .map(ConfigItemStructure::from)
                .collect(),
            expanded: settings.advanced_detail_expanded.clone(),
            ui_revision: settings.config_ui_revision,
        };
        let providers = SettingsProviderStructure {
            theme,
            add_open: settings.provider_add_open,
            add_provider_index: settings.provider_add_provider_index,
            action_error: settings.provider_action_error.clone(),
            action_status: settings.provider_action_status.clone(),
            remove_confirming: settings.provider_remove_confirming.clone(),
            expanded: settings.provider_detail_expanded.clone(),
            storage_expanded: settings.provider_storage_detail_expanded,
            providers: settings
                .provider_forms
                .iter()
                .map(|form| {
                    (
                        form.clone(),
                        settings
                            .provider_health(&form.provider, &form.account)
                            .clone(),
                    )
                })
                .collect(),
        };
        let agents = SettingsAgentsStructure {
            theme,
            agents: settings.agent_snapshot.agents.clone(),
            error: settings.agent_snapshot.error.clone(),
            selected_id: settings.agent_selected_id.clone(),
            delete_confirming: settings.agent_delete_confirming.clone(),
            validation_report: settings.agent_validation_report.clone(),
            editor_previous_id: settings.agent_form.previous_id.clone(),
        };
        let snapshot = &settings.module_snapshot;
        let modules = SettingsModulesStructure {
            theme,
            selected_node_id: settings.module_selected_node_id.clone(),
            action_error: settings.module_action_error.clone(),
            action_status: settings.module_action_status.clone(),
            warnings: snapshot.warnings.clone(),
            config_ui_revision: settings.config_ui_revision,
            nodes: snapshot
                .nodes
                .iter()
                .map(|node| SettingsModuleNodeStructure {
                    node_id: node.node_id.clone(),
                    display_name: node.display_name.clone(),
                    availability: node.availability.clone(),
                    local: node.local,
                })
                .collect(),
            modules: snapshot
                .modules
                .iter()
                .map(|module| SettingsModuleSummaryStructure {
                    module_id: module.module_id.clone(),
                    label: module.label.clone(),
                    backend: module.backend.clone(),
                    detail: module.detail.clone(),
                    desired_enabled: module.desired_enabled,
                    effective_source: module.effective_source,
                    effective_enabled: module.effective_enabled,
                    observed: module.observed,
                    observed_detail: module.observed_detail.clone(),
                })
                .collect(),
            cluster_config: snapshot
                .cluster_config
                .modules
                .iter()
                .map(|(module_id, setting)| (module_id.clone(), setting.enabled))
                .collect(),
            selected_node: snapshot.selected_node.as_ref().map(|node| {
                SettingsSelectedModuleNodeStructure {
                    node_id: node.summary.node_id.clone(),
                    error: node.error.clone(),
                    config: node
                        .config
                        .modules
                        .iter()
                        .map(|(module_id, setting)| (module_id.clone(), setting.enabled))
                        .collect(),
                }
            }),
            module_settings: settings
                .config_items
                .iter()
                .filter(|item| item.module_id.is_some())
                .map(ConfigItemStructure::from)
                .collect(),
        };
        let skills = SettingsSkillsStructure {
            theme,
            include_all: settings.skills_include_all,
            selected: settings.skill_selected.clone(),
            detail_content_hash: settings
                .skill_detail
                .as_ref()
                .map(|detail| detail.summary.content_hash.clone()),
            text_expanded: settings.skill_text_expanded,
            skills: settings.skills.clone(),
        };
        (
            settings.selected,
            SettingsConnectionStructure {
                theme,
                mode_label: settings.mode_label.clone(),
                status_detail: settings.status_detail.clone(),
                issues,
                expanded: settings.connection_detail_expanded.clone(),
            },
            SettingsNodeStructure {
                theme,
                node_id: settings.node_id.clone(),
                role: settings.role.clone(),
                mode_label: settings.mode_label.clone(),
                status_detail: settings.status_detail.clone(),
                clustered: settings.clustered,
                cluster_node_id: settings.cluster_node_id.clone(),
                cluster_summary: settings.cluster_summary.clone(),
                expanded: settings.node_detail_expanded,
            },
            SettingsClusterStructure {
                theme,
                clustered: settings.clustered,
                cluster_summary: settings.cluster_summary.clone(),
                invite,
                leave_confirming: settings.leave_confirming,
                join_confirming: settings.join_confirming,
            },
            configuration,
            providers,
            agents,
            modules,
            skills,
            settings.provider_forms.len(),
        )
    };
    let device_logins = (0..device_login_count)
        .map(|index| {
            let (primary, prompt_url) = device_login_lines(shared, index);
            SettingsDeviceLoginStructure {
                primary,
                prompt_url,
                error: device_login_is_error(shared, index),
            }
        })
        .collect();
    SettingsStructureKey(Some(Box::new(SettingsStructureSnapshot {
        selected,
        theme,
        connection,
        node,
        cluster,
        configuration,
        providers,
        agents,
        modules,
        skills,
        device_logins,
    })))
}

/// Independent generations keep a slow preload or operation result from replacing newer state in
/// the same Settings surface. Server generations protect snapshots; draft generations protect
/// text, selection, confirmation, and other user-owned state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SettingsGenerations {
    config: u64,
    providers: u64,
    agents: u64,
    modules: u64,
    skills: u64,
    cluster: u64,
}

impl SettingsGenerations {
    fn get(self, domain: SettingsOperationDomain) -> u64 {
        match domain {
            SettingsOperationDomain::Config => self.config,
            SettingsOperationDomain::Providers => self.providers,
            SettingsOperationDomain::Agents => self.agents,
            SettingsOperationDomain::Modules => self.modules,
            SettingsOperationDomain::Skills => self.skills,
            SettingsOperationDomain::Cluster => self.cluster,
        }
    }

    fn bump(&mut self, domain: SettingsOperationDomain) {
        let generation = match domain {
            SettingsOperationDomain::Config => &mut self.config,
            SettingsOperationDomain::Providers => &mut self.providers,
            SettingsOperationDomain::Agents => &mut self.agents,
            SettingsOperationDomain::Modules => &mut self.modules,
            SettingsOperationDomain::Skills => &mut self.skills,
            SettingsOperationDomain::Cluster => &mut self.cluster,
        };
        *generation = generation.wrapping_add(1);
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum SettingsOperationDomain {
    Config,
    Providers,
    Agents,
    Modules,
    Skills,
    Cluster,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum SettingsOperationScope {
    ConfigField(String),
    Providers,
    Agents,
    Modules,
    Skills,
    Cluster,
}

impl SettingsOperationScope {
    fn domain(&self) -> SettingsOperationDomain {
        match self {
            Self::ConfigField(_) => SettingsOperationDomain::Config,
            Self::Providers => SettingsOperationDomain::Providers,
            Self::Agents => SettingsOperationDomain::Agents,
            Self::Modules => SettingsOperationDomain::Modules,
            Self::Skills => SettingsOperationDomain::Skills,
            Self::Cluster => SettingsOperationDomain::Cluster,
        }
    }
}

struct SettingsInitialLoadResult {
    settings: SettingsState,
    server_generations: SettingsGenerations,
    draft_generations: SettingsGenerations,
}

struct SettingsOperationLane {
    tx: Sender<SettingsOperationRequest>,
    rx: Receiver<SettingsOperationResult>,
}

struct SettingsOperationRequest {
    request_id: u64,
    scope: SettingsOperationScope,
    draft_generation: u64,
    backend: Backend,
    operation: SettingsOperation,
}

enum SettingsOperation {
    SetConfigValue {
        key: String,
        kind: ConfigEditKind,
        raw: String,
        draft_key: String,
    },
    SetConfigDocument {
        key: String,
        value: Value,
        saved_draft: Option<ConfigSavedDraft>,
    },
    SetConfigBool {
        key: String,
        value: bool,
    },
    AddProvider {
        provider: String,
        account: String,
        api_key: String,
        model: String,
        base_url: String,
        uses_device_login: bool,
    },
    RefreshProviders {
        invalidate_health: Option<(String, String)>,
    },
    SetProviderEnabled {
        provider: String,
        account: String,
        enabled: bool,
    },
    SetProviderChatDefault {
        provider: String,
        account: String,
    },
    RemoveProvider {
        provider: String,
        account: String,
    },
    SetProviderField {
        provider: String,
        account: String,
        field: ProviderField,
        raw: String,
        draft_key: String,
    },
    SetProviderEffort {
        provider: String,
        account: String,
        effort_index: usize,
    },
    SetProviderVerbosity {
        provider: String,
        account: String,
        verbosity_index: usize,
    },
    TestProviderConnection {
        provider: String,
        account: String,
    },
    RefreshAgents {
        selected_id: Option<String>,
    },
    SaveAgent {
        definition: AgentDefinition,
        previous_id: Option<String>,
    },
    DeleteAgent {
        id: String,
    },
    ImportAgent {
        path: PathBuf,
    },
    ExportAgent {
        id: String,
        output: PathBuf,
    },
    ValidateAgent {
        id: String,
    },
    TestAgent {
        id: String,
    },
    RefreshModules {
        selected_node_id: Option<String>,
    },
    SetClusterModuleDefault {
        selected_node_id: Option<String>,
        module_id: String,
        enabled: bool,
    },
    SetNodeModuleOverride {
        node_id: String,
        module_id: String,
        enabled: Option<bool>,
    },
    RefreshSkills {
        include_all: bool,
        selected: Option<(String, String)>,
    },
    ImportSkill {
        path: PathBuf,
    },
    ExportSkill {
        skill_id: String,
        version: String,
        output: PathBuf,
        include_all: bool,
    },
    ActivateSkill {
        skill_id: String,
        version: String,
        include_all: bool,
    },
    CreateClusterInvite,
    LeaveCluster,
    JoinCluster {
        invite: String,
    },
}

struct SettingsOperationResult {
    request_id: u64,
    scope: SettingsOperationScope,
    draft_generation: u64,
    payload: SettingsOperationPayload,
}

enum SettingsOperationPayload {
    Config(Result<ConfigOperationSuccess, String>),
    Providers(Result<ProviderOperationSuccess, String>),
    ProviderHealth(ProviderHealthOperationResult),
    Agents(Result<AgentOperationSuccess, String>),
    Modules(Result<ModuleOperationSuccess, String>),
    Skills(Result<SkillOperationSuccess, String>),
    Cluster(Result<ClusterOperationSuccess, String>),
}

struct ProviderHealthOperationResult {
    provider: String,
    account: String,
    result: Result<sinomo_lm::LanguageModelProviderHealthSnapshot, String>,
}

struct ConfigOperationSuccess {
    items: Vec<ConfigItem>,
    saved_draft: Option<ConfigSavedDraft>,
}

#[derive(Clone, Debug)]
enum ConfigSavedDraft {
    Raw { key: String, value: String },
    Schema { key: String, value: Value },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ConfigPathSegment {
    Field(String),
    Index(usize),
}

impl From<&str> for ConfigPathSegment {
    fn from(value: &str) -> Self {
        Self::Field(value.to_string())
    }
}

impl From<String> for ConfigPathSegment {
    fn from(value: String) -> Self {
        Self::Field(value)
    }
}

struct ProviderOperationSuccess {
    items: Vec<ConfigItem>,
    forms: Vec<ProviderForm>,
    chat_providers: Vec<ProviderSummary>,
    health: HashMap<String, ProviderHealthState>,
    effect: ProviderOperationEffect,
}

enum ProviderOperationEffect {
    Refreshed,
    Added {
        provider: String,
        account: String,
        uses_device_login: bool,
    },
    Updated {
        provider: String,
        account: String,
        message: String,
        saved_draft: Option<(String, String)>,
        invalidates_health: bool,
    },
    Removed {
        provider: String,
        account: String,
    },
}

struct AgentOperationSuccess {
    snapshot: AgentConfigSnapshot,
    chat_agents: Vec<AgentSummary>,
    effect: AgentOperationEffect,
}

enum AgentOperationEffect {
    Refreshed,
    Saved(String),
    Deleted(String),
    Imported(String),
    Exported(PathBuf),
    Validated(AgentValidationReport),
    Tested(AgentValidationReport),
}

struct ModuleOperationSuccess {
    snapshot: ModuleConfigSnapshot,
    message: Option<String>,
}

struct SkillOperationSuccess {
    skills: Vec<SkillSummary>,
    selected: Option<(String, String)>,
    detail: Option<SkillDetail>,
    message: Option<String>,
}

struct ClusterOperationSuccess {
    status: AppClusterStatus,
    snapshot: ClusterSnapshot,
    effect: ClusterOperationEffect,
}

enum ClusterOperationEffect {
    Invite(AppClusterInvite),
    Left,
    Joined,
}

#[derive(Default)]
struct SettingsOperationEffects {
    changed: bool,
    chat_providers: Option<Vec<ProviderSummary>>,
    chat_agents: Option<Vec<AgentSummary>>,
    cluster_snapshot: Option<ClusterSnapshot>,
    provider_device_login: Option<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct AgentForm {
    previous_id: Option<String>,
    definition: AgentDefinition,
    id: String,
    name: String,
    description: String,
    role_index: usize,
    system_prompt: String,
    skills_text: String,
    visible_toolsets_text: String,
    allow_tools_text: String,
    deny_tools_text: String,
    provider: String,
    model: String,
    temperature: String,
    max_output_tokens: String,
    max_steps: String,
    max_tool_calls_per_step: String,
    max_model_repair_attempts: String,
    sandbox_node_id: String,
    output_format_index: usize,
}

/// A text-editable field of a provider in the Providers section.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProviderField {
    ApiKey,
    Model,
    BaseUrl,
    ReasoningBudget,
    VendorOptions,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct ProviderHealthState {
    checking: bool,
    last_verified_at_unix: Option<i64>,
    last_checked_at_unix: Option<i64>,
    error: Option<String>,
    models: Vec<String>,
    model_discovery_error: Option<String>,
    usage: Option<sinomo_lm::LanguageModelProviderUsage>,
    usage_error: Option<String>,
    stale: bool,
}

impl ProviderHealthState {
    fn from_snapshot(snapshot: sinomo_lm::LanguageModelProviderHealthSnapshot) -> Self {
        Self {
            checking: false,
            last_verified_at_unix: snapshot.last_verified_at_unix,
            last_checked_at_unix: Some(snapshot.checked_at_unix),
            error: snapshot.error,
            models: snapshot.models,
            model_discovery_error: snapshot.model_discovery_error,
            usage: snapshot.usage,
            usage_error: snapshot.usage_error,
            stale: snapshot.stale,
        }
    }
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

const AGENT_ROLE_OPTIONS: &[&str] = &["Main", "Background", "Deliberation", "Evolution"];
const AGENT_OUTPUT_FORMAT_OPTIONS: &[&str] = &["JSON decision", "Plain text"];

impl AgentForm {
    fn from_definition(definition: AgentDefinition) -> Self {
        let previous_id = Some(definition.key().to_string());
        Self::from_definition_with_previous(definition, previous_id)
    }

    fn new_custom(id: String) -> Self {
        let definition = AgentDefinition::custom(
            id.clone(),
            AgentRole::Main,
            "You are a helpful Sinomo agent.",
        );
        Self::from_definition_with_previous(definition, None)
    }

    fn duplicate_from(mut definition: AgentDefinition, id: String) -> Self {
        definition.package = AgentPackageDefinition::local();
        definition.id = id.clone();
        definition.name = id;
        Self::from_definition_with_previous(definition, None)
    }

    fn from_definition_with_previous(
        definition: AgentDefinition,
        previous_id: Option<String>,
    ) -> Self {
        let temperature = definition
            .model_policy
            .temperature
            .or(definition.temperature)
            .map(|value| value.to_string())
            .unwrap_or_default();
        let max_output_tokens = definition
            .model_policy
            .max_output_tokens
            .or(definition.max_output_tokens)
            .map(|value| value.to_string())
            .unwrap_or_default();
        let provider = definition.model_policy.provider.clone().unwrap_or_default();
        let model = definition.model_policy.model.clone().unwrap_or_default();
        let role_index = role_index(&definition.role);
        let output_format_index = output_format_index(&definition.output_format);
        Self {
            previous_id,
            id: definition.key().to_string(),
            name: definition.name.clone(),
            description: definition.description.clone(),
            role_index,
            system_prompt: definition.system_prompt.clone(),
            skills_text: definition.skills.join("\n"),
            visible_toolsets_text: definition.tool_surface.visible_toolsets.join("\n"),
            allow_tools_text: definition.tool_surface.allow_tools.join("\n"),
            deny_tools_text: definition.tool_surface.deny_tools.join("\n"),
            provider,
            model,
            temperature,
            max_output_tokens,
            max_steps: definition
                .runtime_limits
                .max_steps
                .map(|value| value.to_string())
                .unwrap_or_default(),
            max_tool_calls_per_step: definition
                .runtime_limits
                .max_tool_calls_per_step
                .map(|value| value.to_string())
                .unwrap_or_default(),
            max_model_repair_attempts: definition
                .runtime_limits
                .max_model_repair_attempts
                .map(|value| value.to_string())
                .unwrap_or_default(),
            sandbox_node_id: definition
                .tool_runtime
                .sandbox_node_id
                .clone()
                .unwrap_or_default(),
            output_format_index,
            definition,
        }
    }

    fn to_definition(&self) -> Result<AgentDefinition, String> {
        let mut definition = self.definition.clone();
        definition.package = AgentPackageDefinition::local();
        definition.id = self.id.trim().to_string();
        definition.name = self.name.trim().to_string();
        definition.description = self.description.trim().to_string();
        definition.role = role_for_index(self.role_index);
        definition.system_prompt = self.system_prompt.clone();
        definition.skills = parse_agent_list(&self.skills_text);
        definition.tool_surface.visible_toolsets = parse_agent_list(&self.visible_toolsets_text);
        definition.tool_surface.allow_tools = parse_agent_list(&self.allow_tools_text);
        definition.tool_surface.deny_tools = parse_agent_list(&self.deny_tools_text);
        definition.model_policy.provider = optional_trimmed_string(&self.provider);
        definition.model_policy.model = optional_trimmed_string(&self.model);
        let temperature = parse_optional_f32("temperature", &self.temperature)?;
        definition.temperature = temperature;
        definition.model_policy.temperature = temperature;
        let max_output_tokens = parse_optional_u32("max output tokens", &self.max_output_tokens)?;
        definition.max_output_tokens = max_output_tokens;
        definition.model_policy.max_output_tokens = max_output_tokens;
        definition.runtime_limits.max_steps = parse_optional_usize("max steps", &self.max_steps)?;
        definition.runtime_limits.max_tool_calls_per_step =
            parse_optional_usize("max tool calls per step", &self.max_tool_calls_per_step)?;
        definition.runtime_limits.max_model_repair_attempts =
            parse_optional_usize("max model repair attempts", &self.max_model_repair_attempts)?;
        definition.tool_runtime.sandbox_node_id = optional_trimmed_string(&self.sandbox_node_id);
        definition.output_format = output_format_for_index(self.output_format_index);
        definition.validate().map_err(|error| error.to_string())?;
        Ok(definition)
    }

    fn can_save(&self) -> bool {
        !self.id.trim().is_empty()
            && !self.name.trim().is_empty()
            && !self.system_prompt.trim().is_empty()
    }
}

impl SettingsState {
    pub fn new(backend: &Backend) -> Self {
        Self::new_with_sifs_timeout(backend, SETTINGS_SIFS_LOAD_TIMEOUT)
    }

    /// Full settings snapshot with a bound on the SIFS-backed sections (skills and
    /// the stored agent catalog). When the distributed file system is unhealthy those
    /// loads can grind through multi-second network timeouts; past `sifs_timeout`
    /// each affected section reports itself unavailable instead of wedging the whole
    /// snapshot — cluster status, connection info, and config stay usable.
    pub(crate) fn new_with_sifs_timeout(backend: &Backend, sifs_timeout: Duration) -> Self {
        let config = backend.config.clone();
        let (config_items, config_error) = match config.catalog(None) {
            Ok(_) => (resolve_items(config.as_ref()), None),
            Err(error) => (Vec::new(), Some(error.to_string())),
        };
        let provider_forms = provider_forms(config.as_ref());
        let provider_health = provider_health_states(load_provider_health(config.as_ref()));
        let data_dir = backend.data_dir.clone();
        let skills_include_all = true;
        let skills_data_dir = data_dir.clone();
        let (skills, skill_selected, skill_detail, skill_action_error) =
            match crate::backend::run_with_timeout("skill-catalog", sifs_timeout, move || {
                load_initial_skill_state(skills_data_dir.as_ref(), skills_include_all)
            }) {
                Ok(state) => state,
                Err(error) => {
                    tracing::warn!(%error, "settings skill catalog load degraded");
                    (Vec::new(), None, None, Some(error))
                }
            };
        let agent_backend = backend.clone();
        let agent_snapshot =
            match crate::backend::run_with_timeout("agent-catalog", sifs_timeout, move || {
                agent_backend.agent_config_snapshot(None)
            }) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    tracing::warn!(%error, "settings agent catalog load degraded");
                    built_in_agent_config_snapshot(None, Some(error))
                }
            };
        let agent_selected_id = agent_snapshot
            .selected_agent
            .as_ref()
            .map(|agent| agent.key().to_string());
        let agent_form = agent_snapshot
            .selected_agent
            .clone()
            .map(AgentForm::from_definition)
            .unwrap_or_else(|| AgentForm::new_custom("custom-agent".to_string()));
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
                crate::backend::Connection::LocalFallback { .. } => (false, None),
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
            join_confirming: false,
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
            provider_health,
            agent_snapshot,
            agent_selected_id,
            agent_form,
            agent_action_error: None,
            agent_action_status: None,
            agent_delete_confirming: None,
            agent_import_path: String::new(),
            agent_export_path: String::new(),
            agent_validation_report: None,
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
            field_drafts: HashMap::new(),
            config_drafts: HashMap::new(),
            config_ui_revision: 0,
            initial_load_rx: None,
            initial_load_started: false,
            operation_lane: None,
            next_operation_request_id: 0,
            pending_operations: HashMap::new(),
            provider_refresh_needed: false,
            provider_refresh_health_invalidation: None,
            server_generations: SettingsGenerations::default(),
            draft_generations: SettingsGenerations::default(),
            connection_detail_expanded: Vec::new(),
            node_detail_expanded: false,
            advanced_detail_expanded: Vec::new(),
            provider_detail_expanded: Vec::new(),
            provider_storage_detail_expanded: false,
            provider_remove_confirming: None,
        }
    }

    /// Cheap startup state for the interactive app. The full settings snapshot can touch config,
    /// skills, modules, and cluster state, so it is preloaded after the shell is interactive.
    pub fn new_lightweight(backend: &Backend) -> Self {
        let data_dir = backend.data_dir.clone();
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
                crate::backend::Connection::LocalFallback { .. } => (false, None),
            }
        };
        Self {
            data_dir,
            config: backend.config.clone(),
            config_items: Vec::new(),
            config_error: Some("Settings data is loading.".to_string()),
            provider_forms: Vec::new(),
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
            join_confirming: false,
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
            provider_health: HashMap::new(),
            agent_snapshot: AgentConfigSnapshot::default(),
            agent_selected_id: None,
            agent_form: AgentForm::new_custom("custom-agent".to_string()),
            agent_action_error: None,
            agent_action_status: None,
            agent_delete_confirming: None,
            agent_import_path: String::new(),
            agent_export_path: String::new(),
            agent_validation_report: None,
            module_snapshot: ModuleConfigSnapshot::default(),
            module_selected_node_id: None,
            module_action_error: None,
            module_action_status: None,
            skills: Vec::new(),
            skill_detail: None,
            skill_selected: None,
            skills_include_all: true,
            skill_import_path: String::new(),
            skill_export_path: String::new(),
            skill_action_error: None,
            skill_action_status: None,
            skill_text_expanded: false,
            field_drafts: HashMap::new(),
            config_drafts: HashMap::new(),
            config_ui_revision: 0,
            initial_load_rx: None,
            initial_load_started: false,
            operation_lane: None,
            next_operation_request_id: 0,
            pending_operations: HashMap::new(),
            provider_refresh_needed: false,
            provider_refresh_health_invalidation: None,
            server_generations: SettingsGenerations::default(),
            draft_generations: SettingsGenerations::default(),
            connection_detail_expanded: Vec::new(),
            node_detail_expanded: false,
            advanced_detail_expanded: Vec::new(),
            provider_detail_expanded: Vec::new(),
            provider_storage_detail_expanded: false,
            provider_remove_confirming: None,
        }
    }

    pub fn begin_initial_load(&mut self, backend: Backend, waker: Option<sui::UiHandle>) -> bool {
        if self.initial_load_started || self.initial_load_rx.is_some() {
            return false;
        }
        let (tx, rx) = mpsc::channel();
        let server_generations = self.server_generations;
        let draft_generations = self.draft_generations;
        match thread::Builder::new()
            .name("sinomo-app-settings-load".to_string())
            .spawn(move || {
                let settings = SettingsState::new(&backend);
                let _ = tx.send(SettingsInitialLoadResult {
                    settings,
                    server_generations,
                    draft_generations,
                });
                if let Some(waker) = waker {
                    waker.wake();
                }
            }) {
            Ok(_) => {
                self.initial_load_rx = Some(rx);
                self.initial_load_started = true;
                true
            }
            Err(error) => {
                self.config_error = Some(format!("Settings data failed to load: {error}"));
                self.initial_load_started = false;
                false
            }
        }
    }

    pub fn pump_initial_load(&mut self) -> bool {
        let result = self
            .initial_load_rx
            .as_ref()
            .and_then(|rx| rx.try_recv().ok());
        let Some(result) = result else {
            return false;
        };
        self.initial_load_rx = None;
        self.initial_load_started = true;
        self.merge_initial_load(result);
        true
    }

    /// Rebind Settings after the connection state machine installs a new daemon backend. In-flight
    /// calls cannot be cancelled while an RPC is blocked, so dropping the lane/result receiver is
    /// the synchronization boundary: old results can finish, but can no longer mutate live UI
    /// state. Drafts remain intact and affected actions become explicitly retryable.
    pub(crate) fn rebind_backend(&mut self, backend: &Backend) {
        let retry_message = "Connection changed before the action completed; retry it.";
        let pending_scopes = self.pending_operations.keys().cloned().collect::<Vec<_>>();
        for scope in &pending_scopes {
            match scope {
                SettingsOperationScope::ConfigField(key) => {
                    self.field_errors
                        .insert(key.clone(), retry_message.to_string());
                }
                SettingsOperationScope::Providers => {
                    self.provider_action_error = Some(retry_message.to_string());
                    self.provider_action_status = None;
                }
                SettingsOperationScope::Agents => {
                    self.agent_action_error = Some(retry_message.to_string());
                    self.agent_action_status = None;
                }
                SettingsOperationScope::Modules => {
                    self.module_action_error = Some(retry_message.to_string());
                    self.module_action_status = None;
                }
                SettingsOperationScope::Skills => {
                    self.skill_action_error = Some(retry_message.to_string());
                    self.skill_action_status = None;
                }
                SettingsOperationScope::Cluster => {
                    self.set_cluster_action_error(retry_message);
                }
            }
        }
        self.pending_operations.clear();
        self.operation_lane = None;
        self.provider_refresh_needed = false;
        self.provider_refresh_health_invalidation = None;
        self.initial_load_rx = None;
        self.initial_load_started = false;
        self.config = backend.config.clone();
        self.config_error = Some("Refreshing Settings after reconnect…".to_string());
        for domain in [
            SettingsOperationDomain::Config,
            SettingsOperationDomain::Providers,
            SettingsOperationDomain::Agents,
            SettingsOperationDomain::Modules,
            SettingsOperationDomain::Skills,
            SettingsOperationDomain::Cluster,
        ] {
            self.server_generations.bump(domain);
        }
    }

    /// Merge only server-owned fields whose domain has not advanced since the preload began.
    /// Drafts, confirmations, action feedback, expansion state, and the operation lane always stay
    /// owned by the live state. This is deliberately field-wise: replacing `self` here used to
    /// erase join links, agent edits, and completed action feedback entered while loading.
    fn merge_initial_load(&mut self, result: SettingsInitialLoadResult) {
        let loaded = result.settings;
        self.mode_label = loaded.mode_label;
        self.status_detail = loaded.status_detail;
        self.node_id = loaded.node_id;
        self.role = loaded.role;
        self.version = loaded.version;

        if self.server_generations.config == result.server_generations.config {
            self.config_items = loaded.config_items;
            self.config_error = loaded.config_error;
        }
        if self.server_generations.providers == result.server_generations.providers {
            self.provider_forms = loaded.provider_forms;
            self.provider_health = loaded.provider_health;
        }
        if self.server_generations.cluster == result.server_generations.cluster {
            self.clustered = loaded.clustered;
            self.cluster_id = loaded.cluster_id;
            self.cluster_node_id = loaded.cluster_node_id;
            self.cluster_summary = loaded.cluster_summary;
            if result.draft_generations.cluster == 0
                && self.draft_generations.cluster == result.draft_generations.cluster
                && self.cluster_action_error.is_none()
                && self.cluster_action_status.is_none()
            {
                self.cluster_action_error = loaded.cluster_action_error;
            }
        }
        if self.server_generations.agents == result.server_generations.agents {
            self.agent_snapshot = loaded.agent_snapshot;
            if result.draft_generations.agents == 0
                && self.draft_generations.agents == result.draft_generations.agents
            {
                self.agent_selected_id = loaded.agent_selected_id;
                self.agent_form = loaded.agent_form;
            }
        }
        if self.server_generations.modules == result.server_generations.modules {
            self.module_snapshot = loaded.module_snapshot;
            if result.draft_generations.modules == 0
                && self.draft_generations.modules == result.draft_generations.modules
            {
                self.module_selected_node_id = loaded.module_selected_node_id;
            }
        }
        if self.server_generations.skills == result.server_generations.skills {
            self.skills = loaded.skills;
            if result.draft_generations.skills == 0
                && self.draft_generations.skills == result.draft_generations.skills
            {
                self.skill_selected = loaded.skill_selected;
                self.skill_detail = loaded.skill_detail;
            }
            if self.skill_action_error.is_none() {
                self.skill_action_error = loaded.skill_action_error;
            }
        }
    }

    fn mark_draft_changed(&mut self, domain: SettingsOperationDomain) {
        self.draft_generations.bump(domain);
        if domain == SettingsOperationDomain::Agents {
            self.agent_validation_report = None;
        }
    }

    fn set_field_draft(&mut self, domain: SettingsOperationDomain, key: String, value: String) {
        self.field_drafts.insert(key, value);
        self.mark_draft_changed(domain);
    }

    fn field_draft_or(&self, key: &str, fallback: impl FnOnce() -> String) -> String {
        self.field_drafts.get(key).cloned().unwrap_or_else(fallback)
    }

    fn operation_pending(&self, scope: &SettingsOperationScope) -> bool {
        self.pending_operations.contains_key(scope)
    }

    fn domain_pending(&self, domain: SettingsOperationDomain) -> bool {
        self.pending_operations
            .keys()
            .any(|scope| scope.domain() == domain)
    }

    fn queue_operation(
        &mut self,
        backend: Backend,
        waker: Option<sui::UiHandle>,
        scope: SettingsOperationScope,
        operation: SettingsOperation,
    ) -> bool {
        // One in-flight request owns a scope until its matching result arrives. In particular,
        // ignore key-repeat/double-click activation of destructive actions instead of enqueueing a
        // second mutation whose eventual "not found" could mask the first request's success.
        if self.operation_pending(&scope) {
            return false;
        }
        if self.operation_lane.is_none() {
            let (request_tx, request_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let worker_waker = waker;
            let spawn = thread::Builder::new()
                .name("sinomo-app-settings-operations".to_string())
                .spawn(move || settings_operation_worker(request_rx, result_tx, worker_waker));
            if let Err(error) = spawn {
                self.apply_operation_queue_error(
                    &scope,
                    format!("could not start Settings operation: {error}"),
                );
                return false;
            }
            self.operation_lane = Some(SettingsOperationLane {
                tx: request_tx,
                rx: result_rx,
            });
        }

        let request_id = self.next_operation_request_id;
        self.next_operation_request_id = self.next_operation_request_id.wrapping_add(1);
        let draft_generation = self.draft_generations.get(scope.domain());
        self.mark_operation_pending(&operation);
        self.pending_operations.insert(scope.clone(), request_id);
        let request = SettingsOperationRequest {
            request_id,
            scope: scope.clone(),
            draft_generation,
            backend,
            operation,
        };
        let sent = self
            .operation_lane
            .as_ref()
            .is_some_and(|lane| lane.tx.send(request).is_ok());
        if !sent {
            self.pending_operations.remove(&scope);
            self.operation_lane = None;
            self.apply_operation_queue_error(
                &scope,
                "Settings background worker stopped; retry the action".to_string(),
            );
        }
        sent
    }

    fn mark_operation_pending(&mut self, operation: &SettingsOperation) {
        match operation {
            SettingsOperation::SetConfigValue { draft_key, .. } => {
                self.field_errors
                    .insert(draft_key.clone(), "Saving…".to_string());
            }
            SettingsOperation::SetConfigDocument {
                saved_draft: Some(ConfigSavedDraft::Schema { key, .. }),
                ..
            } => {
                self.field_errors
                    .insert(config_schema_field_key(key, &[]), "Saving…".to_string());
            }
            SettingsOperation::SetConfigDocument { .. }
            | SettingsOperation::SetConfigBool { .. } => {}
            SettingsOperation::AddProvider { .. } => {
                self.provider_add_error = None;
                self.provider_action_error = None;
                self.provider_action_status = None;
            }
            SettingsOperation::RefreshProviders { .. } => {}
            SettingsOperation::SetProviderEnabled {
                provider, account, ..
            }
            | SettingsOperation::SetProviderChatDefault { provider, account }
            | SettingsOperation::RemoveProvider { provider, account }
            | SettingsOperation::SetProviderField {
                provider, account, ..
            }
            | SettingsOperation::SetProviderEffort {
                provider, account, ..
            }
            | SettingsOperation::SetProviderVerbosity {
                provider, account, ..
            } => {
                self.provider_action_error = None;
                self.set_provider_status(provider.clone(), account.clone(), "Saving…");
            }
            SettingsOperation::TestProviderConnection { provider, account } => {
                self.provider_action_error = None;
                self.provider_action_status = None;
                let health = self
                    .provider_health
                    .entry(provider_account_key(provider, account))
                    .or_default();
                health.checking = true;
                health.error = None;
            }
            SettingsOperation::RefreshAgents { .. } => {
                self.agent_action_error = None;
                self.agent_action_status = Some("Refreshing agents…".to_string());
            }
            SettingsOperation::SaveAgent { .. } => {
                self.agent_action_error = None;
                self.agent_action_status = Some("Saving agent…".to_string());
            }
            SettingsOperation::DeleteAgent { .. } => {
                self.agent_action_error = None;
                self.agent_action_status = Some("Deleting agent…".to_string());
            }
            SettingsOperation::ImportAgent { .. } => {
                self.agent_action_error = None;
                self.agent_action_status = Some("Importing agent package…".to_string());
            }
            SettingsOperation::ExportAgent { .. } => {
                self.agent_action_error = None;
                self.agent_action_status = Some("Exporting agent package…".to_string());
            }
            SettingsOperation::ValidateAgent { .. } => {
                self.agent_action_error = None;
                self.agent_action_status = Some("Validating agent…".to_string());
            }
            SettingsOperation::TestAgent { .. } => {
                self.agent_action_error = None;
                self.agent_action_status = Some("Testing agent readiness…".to_string());
            }
            SettingsOperation::RefreshModules { .. } => {
                self.module_action_error = None;
                self.module_action_status = Some("Refreshing modules…".to_string());
            }
            SettingsOperation::SetClusterModuleDefault { .. }
            | SettingsOperation::SetNodeModuleOverride { .. } => {
                self.module_action_error = None;
                self.module_action_status = Some("Saving module configuration…".to_string());
            }
            SettingsOperation::RefreshSkills { .. } => {
                self.skill_action_error = None;
                self.skill_action_status = Some("Refreshing skills…".to_string());
            }
            SettingsOperation::ImportSkill { .. } => {
                self.skill_action_error = None;
                self.skill_action_status = Some("Importing skill…".to_string());
            }
            SettingsOperation::ExportSkill { .. } => {
                self.skill_action_error = None;
                self.skill_action_status = Some("Exporting skill…".to_string());
            }
            SettingsOperation::ActivateSkill { .. } => {
                self.skill_action_error = None;
                self.skill_action_status = Some("Activating skill…".to_string());
            }
            SettingsOperation::CreateClusterInvite => {
                self.cluster_action_error = None;
                self.cluster_action_status = Some("Generating invitation…".to_string());
            }
            SettingsOperation::LeaveCluster => {
                self.cluster_action_error = None;
                self.cluster_action_status = Some("Leaving cluster…".to_string());
            }
            SettingsOperation::JoinCluster { .. } => {
                self.cluster_action_error = None;
                self.cluster_action_status = Some("Migrating to cluster…".to_string());
            }
        }
    }

    fn apply_operation_queue_error(&mut self, scope: &SettingsOperationScope, error: String) {
        match scope.domain() {
            SettingsOperationDomain::Config => {
                if let SettingsOperationScope::ConfigField(key) = scope {
                    self.field_errors.insert(key.clone(), error);
                }
            }
            SettingsOperationDomain::Providers => {
                self.provider_action_error = Some(error);
                self.provider_action_status = None;
            }
            SettingsOperationDomain::Agents => {
                self.agent_action_error = Some(error);
                self.agent_action_status = None;
            }
            SettingsOperationDomain::Modules => {
                self.module_action_error = Some(error);
                self.module_action_status = None;
            }
            SettingsOperationDomain::Skills => {
                self.skill_action_error = Some(error);
                self.skill_action_status = None;
            }
            SettingsOperationDomain::Cluster => self.set_cluster_action_error(error),
        }
    }

    fn pump_operations(&mut self) -> SettingsOperationEffects {
        let results = match self.operation_lane.as_ref() {
            Some(lane) => lane.rx.try_iter().collect::<Vec<_>>(),
            None => return SettingsOperationEffects::default(),
        };
        let mut effects = SettingsOperationEffects::default();
        for result in results {
            if self.pending_operations.get(&result.scope) != Some(&result.request_id) {
                continue;
            }
            self.pending_operations.remove(&result.scope);
            effects.changed = true;
            let domain = result.scope.domain();
            match result.payload {
                SettingsOperationPayload::Config(Ok(success)) => {
                    self.server_generations
                        .bump(SettingsOperationDomain::Config);
                    self.config_items = success.items;
                    self.config_error = None;
                    if let Some(saved_draft) = success.saved_draft {
                        match saved_draft {
                            ConfigSavedDraft::Raw { key, value } => {
                                if self.field_drafts.get(&key) == Some(&value) {
                                    self.field_drafts.remove(&key);
                                }
                                self.field_errors.remove(&key);
                            }
                            ConfigSavedDraft::Schema { key, value } => {
                                let prefix = config_schema_field_prefix(&key);
                                self.field_errors
                                    .remove(&config_schema_field_key(&key, &[]));
                                if self.config_drafts.get(&key) == Some(&value) {
                                    self.config_drafts.remove(&key);
                                    self.field_drafts
                                        .retain(|draft_key, _| !draft_key.starts_with(&prefix));
                                    self.field_errors
                                        .retain(|error_key, _| !error_key.starts_with(&prefix));
                                    self.config_ui_revision =
                                        self.config_ui_revision.wrapping_add(1);
                                }
                            }
                        }
                    }
                }
                SettingsOperationPayload::Config(Err(error)) => {
                    self.apply_operation_queue_error(&result.scope, error);
                }
                SettingsOperationPayload::Providers(Ok(success)) => {
                    self.server_generations
                        .bump(SettingsOperationDomain::Config);
                    self.server_generations
                        .bump(SettingsOperationDomain::Providers);
                    self.config_items = success.items;
                    self.config_error = None;
                    self.provider_forms = success.forms;
                    self.provider_health = success.health;
                    effects.chat_providers = Some(success.chat_providers);
                    self.provider_action_error = None;
                    self.provider_remove_confirming = None;
                    match success.effect {
                        ProviderOperationEffect::Refreshed => {}
                        ProviderOperationEffect::Added {
                            provider,
                            account,
                            uses_device_login,
                        } => {
                            if self.draft_generations.providers == result.draft_generation {
                                self.close_provider_add();
                                self.provider_add_account = "default".to_string();
                                self.provider_add_api_key.clear();
                                self.provider_add_base_url.clear();
                                self.provider_add_model =
                                    provider_default_model(self.provider_add_provider());
                            }
                            self.set_provider_status(
                                provider.clone(),
                                account.clone(),
                                "Account added to lm.providers",
                            );
                            if uses_device_login {
                                effects.provider_device_login = Some((provider, account));
                            }
                        }
                        ProviderOperationEffect::Updated {
                            provider,
                            account,
                            message,
                            saved_draft,
                            invalidates_health,
                        } => {
                            if let Some((key, saved)) = saved_draft {
                                if self.field_drafts.get(&key) == Some(&saved) {
                                    self.field_drafts.remove(&key);
                                }
                                self.field_errors.remove(&key);
                            }
                            if invalidates_health {
                                self.invalidate_provider_health(&provider, &account);
                            }
                            self.set_provider_status(provider, account, message);
                        }
                        ProviderOperationEffect::Removed { provider, account } => {
                            if self.device_login.as_ref().is_some_and(|login| {
                                login.provider == provider && login.account == account
                            }) {
                                self.device_login = None;
                            }
                            self.provider_health
                                .remove(&provider_account_key(&provider, &account));
                            self.provider_action_status = None;
                        }
                    }
                }
                SettingsOperationPayload::Providers(Err(error)) => {
                    self.apply_operation_queue_error(&result.scope, error);
                }
                SettingsOperationPayload::ProviderHealth(health_result) => {
                    let key = provider_account_key(&health_result.provider, &health_result.account);
                    match health_result.result {
                        Ok(snapshot) => {
                            let healthy = snapshot.healthy;
                            let provider = snapshot.provider.clone();
                            let account = snapshot.account.clone();
                            self.provider_health
                                .insert(key, ProviderHealthState::from_snapshot(snapshot));
                            if healthy {
                                self.set_provider_status(provider, account, "Connection verified");
                            }
                        }
                        Err(error) => {
                            let health = self.provider_health.entry(key).or_default();
                            health.checking = false;
                            health.last_checked_at_unix = Some(current_settings_unix_timestamp());
                            health.error = Some(error);
                            health.stale = health.last_verified_at_unix.is_some();
                        }
                    }
                }
                SettingsOperationPayload::Agents(Ok(success)) => {
                    self.server_generations
                        .bump(SettingsOperationDomain::Agents);
                    self.agent_snapshot = success.snapshot;
                    if self.draft_generations.agents == result.draft_generation {
                        self.agent_selected_id = self
                            .agent_snapshot
                            .selected_agent
                            .as_ref()
                            .map(|agent| agent.key().to_string());
                        if let Some(definition) = self.agent_snapshot.selected_agent.clone() {
                            self.agent_form = AgentForm::from_definition(definition);
                        }
                    }
                    self.agent_action_error = None;
                    self.agent_action_status = match success.effect {
                        AgentOperationEffect::Refreshed => None,
                        AgentOperationEffect::Saved(id) => Some(format!("Saved custom agent {id}")),
                        AgentOperationEffect::Deleted(id) => {
                            self.agent_delete_confirming = None;
                            self.agent_validation_report = None;
                            Some(format!("Deleted custom agent {id}"))
                        }
                        AgentOperationEffect::Imported(id) => {
                            self.agent_import_path.clear();
                            self.agent_validation_report = None;
                            Some(format!("Imported agent package {id}"))
                        }
                        AgentOperationEffect::Exported(path) => {
                            Some(format!("Exported package to {}", path.display()))
                        }
                        AgentOperationEffect::Validated(report) => {
                            let ready = report.is_ready();
                            let summary = report.summary();
                            self.agent_validation_report = Some(report);
                            Some(if ready {
                                format!("Validation passed · {summary}")
                            } else {
                                format!("Validation needs attention · {summary}")
                            })
                        }
                        AgentOperationEffect::Tested(report) => {
                            let ready = report.is_ready();
                            let summary = report.summary();
                            self.agent_validation_report = Some(report);
                            Some(if ready {
                                format!("Readiness test passed · {summary}")
                            } else {
                                format!("Readiness test needs attention · {summary}")
                            })
                        }
                    };
                    effects.chat_agents = Some(success.chat_agents);
                }
                SettingsOperationPayload::Agents(Err(error)) => {
                    self.apply_operation_queue_error(&result.scope, error);
                }
                SettingsOperationPayload::Modules(Ok(success)) => {
                    self.server_generations
                        .bump(SettingsOperationDomain::Modules);
                    self.module_snapshot = success.snapshot;
                    if self.draft_generations.modules == result.draft_generation {
                        self.module_selected_node_id = self
                            .module_snapshot
                            .selected_node
                            .as_ref()
                            .map(|node| node.summary.node_id.clone());
                    }
                    self.module_action_error = None;
                    self.module_action_status = success.message;
                }
                SettingsOperationPayload::Modules(Err(error)) => {
                    self.apply_operation_queue_error(&result.scope, error);
                }
                SettingsOperationPayload::Skills(Ok(success)) => {
                    self.server_generations
                        .bump(SettingsOperationDomain::Skills);
                    self.skills = success.skills;
                    if self.draft_generations.skills == result.draft_generation {
                        self.skill_selected = success.selected;
                        self.skill_detail = success.detail;
                    }
                    self.skill_action_error = None;
                    self.skill_action_status = success.message;
                }
                SettingsOperationPayload::Skills(Err(error)) => {
                    self.apply_operation_queue_error(&result.scope, error);
                }
                SettingsOperationPayload::Cluster(Ok(success)) => {
                    self.server_generations
                        .bump(SettingsOperationDomain::Cluster);
                    self.clustered = success.status.clustered;
                    self.cluster_id = success.status.cluster_id;
                    self.cluster_node_id = success.status.local_node_id;
                    self.cluster_summary = success.status.summary;
                    self.cluster_action_error = success.status.error;
                    self.leave_confirming = false;
                    effects.cluster_snapshot = Some(success.snapshot);
                    match success.effect {
                        ClusterOperationEffect::Invite(invite) => {
                            self.invite_result = Some(invite);
                            self.set_cluster_action_status("Invitation generated");
                        }
                        ClusterOperationEffect::Left => {
                            self.invite_result = None;
                            self.join_confirming = false;
                            self.set_cluster_action_status("Started single-node cluster");
                        }
                        ClusterOperationEffect::Joined => {
                            if self.draft_generations.cluster == result.draft_generation {
                                self.join_invite.clear();
                                self.join_confirming = false;
                            }
                            self.invite_result = None;
                            self.set_cluster_action_status(
                                "Joined cluster. If recovery is needed, run `sinomo cluster status` and restart the daemon; failed migrations restore the source cluster automatically.",
                            );
                        }
                    }
                }
                SettingsOperationPayload::Cluster(Err(error)) => {
                    self.apply_operation_queue_error(&result.scope, error);
                }
            }
            debug_assert_eq!(domain, result.scope.domain());
        }
        effects
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

    fn prepare_agent_selection(&mut self, index: usize) -> Option<String> {
        let id = self.agent_snapshot.agents.get(index)?.id.clone();
        self.agent_selected_id = Some(id.clone());
        self.agent_action_error = None;
        self.agent_action_status = Some("Loading agent…".to_string());
        self.agent_delete_confirming = None;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        Some(id)
    }

    fn agent_save_operation(&mut self) -> Option<SettingsOperation> {
        let definition = match self.agent_form.to_definition() {
            Ok(definition) => definition,
            Err(error) => {
                self.agent_action_error = Some(error);
                self.agent_action_status = None;
                return None;
            }
        };
        Some(SettingsOperation::SaveAgent {
            definition,
            previous_id: self.agent_form.previous_id.clone(),
        })
    }

    fn create_agent(&mut self) {
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        let id = unique_agent_id(&self.agent_snapshot, "custom-agent");
        self.agent_form = AgentForm::new_custom(id.clone());
        self.agent_selected_id = Some(id);
        self.agent_action_error = None;
        self.agent_action_status = None;
        self.agent_delete_confirming = None;
    }

    fn set_agent_import_path(&mut self, value: String) {
        self.agent_import_path = value;
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_export_path(&mut self, value: String) {
        self.agent_export_path = value;
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn duplicate_selected_agent(&mut self) {
        let Some(definition) = self.agent_snapshot.selected_agent.clone() else {
            self.agent_action_error = Some("select an agent to duplicate".to_string());
            self.agent_action_status = None;
            return;
        };
        let id = unique_agent_id(
            &self.agent_snapshot,
            &format!("{}-custom", definition.key()),
        );
        self.agent_form = AgentForm::duplicate_from(definition, id.clone());
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_selected_id = Some(id);
        self.agent_action_error = None;
        self.agent_action_status = Some("Editing duplicated custom agent".to_string());
        self.agent_delete_confirming = None;
    }

    fn set_agent_id(&mut self, value: String) {
        self.agent_form.id = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_name(&mut self, value: String) {
        self.agent_form.name = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_description(&mut self, value: String) {
        self.agent_form.description = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_role_index(&mut self, index: usize) {
        self.agent_form.role_index = index.min(AGENT_ROLE_OPTIONS.len().saturating_sub(1));
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_system_prompt(&mut self, value: String) {
        self.agent_form.system_prompt = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_provider(&mut self, value: String) {
        self.agent_form.provider = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_model(&mut self, value: String) {
        self.agent_form.model = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_temperature(&mut self, value: String) {
        self.agent_form.temperature = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_max_output_tokens(&mut self, value: String) {
        self.agent_form.max_output_tokens = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_max_steps(&mut self, value: String) {
        self.agent_form.max_steps = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_max_tool_calls_per_step(&mut self, value: String) {
        self.agent_form.max_tool_calls_per_step = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_max_model_repair_attempts(&mut self, value: String) {
        self.agent_form.max_model_repair_attempts = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_sandbox_node_id(&mut self, value: String) {
        self.agent_form.sandbox_node_id = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_output_format_index(&mut self, index: usize) {
        self.agent_form.output_format_index =
            index.min(AGENT_OUTPUT_FORMAT_OPTIONS.len().saturating_sub(1));
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_skills_text(&mut self, value: String) {
        self.agent_form.skills_text = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_visible_toolsets_text(&mut self, value: String) {
        self.agent_form.visible_toolsets_text = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_allow_tools_text(&mut self, value: String) {
        self.agent_form.allow_tools_text = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn set_agent_deny_tools_text(&mut self, value: String) {
        self.agent_form.deny_tools_text = value;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn agent_form_can_save(&self) -> bool {
        self.agent_form.can_save() && agent_definition_is_editable(&self.agent_form.definition)
    }

    fn confirm_agent_delete(&mut self) {
        if let Some(id) = self.agent_selected_id.clone() {
            self.agent_delete_confirming = Some(id);
            self.agent_action_error = None;
            self.agent_action_status = None;
        }
    }

    fn cancel_agent_delete(&mut self) {
        self.agent_delete_confirming = None;
        self.agent_action_error = None;
    }

    fn selected_module_node_index(&self) -> Option<usize> {
        let selected = self.module_selected_node_id.as_ref()?;
        self.module_snapshot
            .nodes
            .iter()
            .position(|node| &node.node_id == selected)
    }

    fn set_skill_import_path(&mut self, value: String) {
        self.skill_import_path = value;
        self.mark_draft_changed(SettingsOperationDomain::Skills);
        self.skill_action_error = None;
        self.skill_action_status = None;
    }

    fn set_skill_export_path(&mut self, value: String) {
        self.skill_export_path = value;
        self.mark_draft_changed(SettingsOperationDomain::Skills);
        self.skill_action_error = None;
        self.skill_action_status = None;
    }

    fn toggle_skill_text(&mut self) {
        self.skill_text_expanded = !self.skill_text_expanded;
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
        self.mark_draft_changed(SettingsOperationDomain::Providers);
    }

    fn set_provider_add_account(&mut self, value: String) {
        self.provider_add_account = value;
        self.mark_draft_changed(SettingsOperationDomain::Providers);
        self.provider_add_error = None;
    }

    fn set_provider_add_api_key(&mut self, value: String) {
        self.provider_add_api_key = value;
        self.mark_draft_changed(SettingsOperationDomain::Providers);
        self.provider_add_error = None;
    }

    fn set_provider_add_model(&mut self, value: String) {
        self.provider_add_model = value;
        self.mark_draft_changed(SettingsOperationDomain::Providers);
        self.provider_add_error = None;
    }

    fn set_provider_add_base_url(&mut self, value: String) {
        self.provider_add_base_url = value;
        self.mark_draft_changed(SettingsOperationDomain::Providers);
        self.provider_add_error = None;
    }

    /// Return to the menu level (narrow layout).
    fn back_to_menu(&mut self) {
        self.selected = None;
    }

    fn set_join_invite(&mut self, value: String) {
        self.join_invite = value;
        self.join_confirming = false;
        self.mark_draft_changed(SettingsOperationDomain::Cluster);
        self.cluster_action_error = None;
        self.cluster_action_status = None;
    }

    fn confirm_join_cluster(&mut self) -> bool {
        match cluster_join_preview(&self.join_invite) {
            Ok(_) => {
                self.join_confirming = true;
                self.cluster_action_error = None;
                self.cluster_action_status = None;
                self.mark_draft_changed(SettingsOperationDomain::Cluster);
                true
            }
            Err(error) => {
                self.join_confirming = false;
                self.set_cluster_action_error(error);
                false
            }
        }
    }

    fn cancel_join_cluster(&mut self) {
        self.join_confirming = false;
        self.mark_draft_changed(SettingsOperationDomain::Cluster);
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
        self.mark_draft_changed(SettingsOperationDomain::Cluster);
        self.cluster_action_error = None;
        self.cluster_action_status = None;
    }

    fn cancel_leave_cluster(&mut self) {
        self.leave_confirming = false;
        self.mark_draft_changed(SettingsOperationDomain::Cluster);
    }

    fn set_config_value_draft(&mut self, index: usize, raw: String) {
        let Some(item) = self.config_items.get(index) else {
            return;
        };
        if item.edit_kind == ConfigEditKind::Bool || item.read_only {
            return;
        }
        let key = config_field_key(&item.key);
        self.field_errors.remove(&key);
        self.set_field_draft(SettingsOperationDomain::Config, key, raw);
    }

    fn config_value_operation(
        &self,
        index: usize,
    ) -> Option<(SettingsOperationScope, SettingsOperation)> {
        let item = self.config_items.get(index)?;
        if item.edit_kind == ConfigEditKind::Bool || item.read_only {
            return None;
        }
        let draft_key = config_field_key(&item.key);
        let raw = self
            .field_drafts
            .get(&draft_key)
            .cloned()
            .unwrap_or_else(|| item.edit_value.clone());
        Some((
            SettingsOperationScope::ConfigField(draft_key.clone()),
            SettingsOperation::SetConfigValue {
                key: item.key.clone(),
                kind: item.edit_kind,
                raw,
                draft_key,
            },
        ))
    }

    fn set_config_schema_text_draft(
        &mut self,
        index: usize,
        path: Vec<ConfigPathSegment>,
        schema: ConfigSchemaValue,
        raw: String,
    ) {
        let Some(item) = self.config_items.get(index) else {
            return;
        };
        if item.read_only {
            return;
        }
        let config_key = item.key.clone();
        let base = item.raw_value.clone();
        let key = config_schema_field_key(&config_key, &path);
        self.field_drafts.insert(key.clone(), raw.clone());
        match parse_schema_editor_value(&schema, &raw) {
            Ok(value) => {
                self.field_errors.remove(&key);
                let draft = self.config_drafts.entry(config_key).or_insert(base);
                if let Err(error) = set_json_path_value(draft, &path, value) {
                    self.field_errors.insert(key, error);
                }
            }
            Err(error) => {
                self.field_errors.insert(key, error);
            }
        }
        self.mark_draft_changed(SettingsOperationDomain::Config);
    }

    fn set_config_schema_value_draft(
        &mut self,
        index: usize,
        path: Vec<ConfigPathSegment>,
        value: Value,
    ) -> Result<(), String> {
        let item = self
            .config_items
            .get(index)
            .ok_or_else(|| "configuration field is no longer available".to_string())?;
        if item.read_only {
            return Err("this configuration is read-only".to_string());
        }
        let config_key = item.key.clone();
        let base = item.raw_value.clone();
        let error_key = config_schema_field_key(&config_key, &path);
        let draft = self.config_drafts.entry(config_key).or_insert(base);
        set_json_path_value(draft, &path, value)?;
        self.field_errors.remove(&error_key);
        self.mark_draft_changed(SettingsOperationDomain::Config);
        Ok(())
    }

    fn config_schema_draft_value<'a>(&'a self, item: &'a ConfigItem) -> &'a Value {
        self.config_drafts.get(&item.key).unwrap_or(&item.raw_value)
    }

    fn config_schema_is_dirty(&self, key: &str) -> bool {
        let Some(draft) = self.config_drafts.get(key) else {
            return false;
        };
        self.config_items
            .iter()
            .find(|item| item.key == key)
            .is_none_or(|item| draft != &item.raw_value)
    }

    fn config_schema_save_operation(
        &self,
        index: usize,
    ) -> Result<(SettingsOperationScope, SettingsOperation), String> {
        let item = self
            .config_items
            .get(index)
            .ok_or_else(|| "configuration definition is no longer available".to_string())?;
        if item.read_only {
            return Err("this configuration is read-only".to_string());
        }
        let schema = item
            .schema
            .as_ref()
            .ok_or_else(|| "configuration does not declare a typed schema".to_string())?;
        let prefix = config_schema_field_prefix(&item.key);
        if let Some(error) = self
            .field_errors
            .iter()
            .find_map(|(key, error)| key.starts_with(&prefix).then(|| error.clone()))
        {
            return Err(error);
        }
        let value = self.config_schema_draft_value(item).clone();
        let key = ConfigKey::new(item.key.clone()).map_err(|error| error.to_string())?;
        schema
            .validate(&key, &value)
            .map_err(|error| error.to_string())?;
        let scope_key = config_schema_field_key(&item.key, &[]);
        Ok((
            SettingsOperationScope::ConfigField(scope_key),
            SettingsOperation::SetConfigDocument {
                key: item.key.clone(),
                value: value.clone(),
                saved_draft: Some(ConfigSavedDraft::Schema {
                    key: item.key.clone(),
                    value,
                }),
            },
        ))
    }

    fn reset_config_schema_draft(&mut self, index: usize) {
        let Some(key) = self.config_items.get(index).map(|item| item.key.clone()) else {
            return;
        };
        self.config_drafts.remove(&key);
        let prefix = config_schema_field_prefix(&key);
        self.field_drafts
            .retain(|draft_key, _| !draft_key.starts_with(&prefix));
        self.field_errors
            .retain(|error_key, _| !error_key.starts_with(&prefix));
        self.config_ui_revision = self.config_ui_revision.wrapping_add(1);
        self.mark_draft_changed(SettingsOperationDomain::Config);
    }

    fn append_config_schema_list_item(
        &mut self,
        index: usize,
        path: &[ConfigPathSegment],
        item_schema: &ConfigSchemaValue,
    ) -> Result<(), String> {
        let item = self
            .config_items
            .get(index)
            .ok_or_else(|| "configuration definition is no longer available".to_string())?;
        if item.read_only {
            return Err("this configuration is read-only".to_string());
        }
        let key = item.key.clone();
        let base = item.raw_value.clone();
        let draft = self.config_drafts.entry(key).or_insert(base);
        let list = config_value_mut_at_path(draft, path, ValueContainerKind::List)?;
        let list = list
            .as_array_mut()
            .ok_or_else(|| "list setting is not an array".to_string())?;
        list.push(schema_default_value(item_schema));
        self.config_ui_revision = self.config_ui_revision.wrapping_add(1);
        self.mark_draft_changed(SettingsOperationDomain::Config);
        Ok(())
    }

    fn insert_config_schema_map_entry(
        &mut self,
        index: usize,
        path: &[ConfigPathSegment],
        entry_key: &str,
        item_schema: &ConfigSchemaValue,
    ) -> Result<(), String> {
        let entry_key = entry_key.trim();
        if entry_key.is_empty() {
            return Err("entry key cannot be empty".to_string());
        }
        let item = self
            .config_items
            .get(index)
            .ok_or_else(|| "configuration definition is no longer available".to_string())?;
        if item.read_only {
            return Err("this configuration is read-only".to_string());
        }
        let key = item.key.clone();
        let base = item.raw_value.clone();
        let draft = self.config_drafts.entry(key.clone()).or_insert(base);
        let map = config_value_mut_at_path(draft, path, ValueContainerKind::Object)?;
        let map = map
            .as_object_mut()
            .ok_or_else(|| "map setting is not an object".to_string())?;
        if map.contains_key(entry_key) {
            return Err(format!("entry `{entry_key}` already exists"));
        }
        map.insert(entry_key.to_string(), schema_default_value(item_schema));
        let add_key = config_schema_add_key(&key, path);
        self.field_drafts.remove(&add_key);
        self.field_errors.remove(&add_key);
        self.config_ui_revision = self.config_ui_revision.wrapping_add(1);
        self.mark_draft_changed(SettingsOperationDomain::Config);
        Ok(())
    }

    fn remove_config_schema_path(
        &mut self,
        index: usize,
        path: &[ConfigPathSegment],
    ) -> Result<(), String> {
        let item = self
            .config_items
            .get(index)
            .ok_or_else(|| "configuration definition is no longer available".to_string())?;
        if item.read_only {
            return Err("this configuration is read-only".to_string());
        }
        let key = item.key.clone();
        let base = item.raw_value.clone();
        let draft = self.config_drafts.entry(key.clone()).or_insert(base);
        let list_shift = match path.split_last() {
            Some((ConfigPathSegment::Index(item_index), parent_path)) => {
                config_value_at_path(draft, parent_path)
                    .and_then(Value::as_array)
                    .map(|items| (*item_index, items.len(), parent_path.to_vec()))
            }
            _ => None,
        };
        remove_json_path_value(draft, path)?;
        clear_config_schema_auxiliary_path(&mut self.field_drafts, &key, path);
        clear_config_schema_auxiliary_path(&mut self.field_errors, &key, path);
        if let Some((removed_index, old_len, parent_path)) = list_shift {
            for old_index in removed_index + 1..old_len {
                let mut old_path = parent_path.clone();
                old_path.push(ConfigPathSegment::Index(old_index));
                let mut new_path = parent_path.clone();
                new_path.push(ConfigPathSegment::Index(old_index - 1));
                move_config_schema_auxiliary_path(
                    &mut self.field_drafts,
                    &key,
                    &old_path,
                    &new_path,
                );
                move_config_schema_auxiliary_path(
                    &mut self.field_errors,
                    &key,
                    &old_path,
                    &new_path,
                );
            }
        }
        self.config_ui_revision = self.config_ui_revision.wrapping_add(1);
        self.mark_draft_changed(SettingsOperationDomain::Config);
        Ok(())
    }

    fn set_provider_field_draft(&mut self, index: usize, field: ProviderField, raw: String) {
        let Some(form) = self.provider_forms.get(index) else {
            return;
        };
        let key = provider_field_key(&form.provider, &form.account, field);
        self.field_errors.remove(&key);
        self.set_field_draft(SettingsOperationDomain::Providers, key, raw);
    }

    fn provider_field_operation(
        &self,
        index: usize,
        field: ProviderField,
    ) -> Option<SettingsOperation> {
        let form = self.provider_forms.get(index)?;
        let draft_key = provider_field_key(&form.provider, &form.account, field);
        let raw = self
            .field_drafts
            .get(&draft_key)
            .cloned()
            .unwrap_or_else(|| match field {
                ProviderField::ApiKey => String::new(),
                ProviderField::Model => form.default_model.clone(),
                ProviderField::BaseUrl => form.api_base_url.clone(),
                ProviderField::ReasoningBudget => form
                    .reasoning_budget_tokens
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                ProviderField::VendorOptions => form.vendor_options.clone(),
            });
        Some(SettingsOperation::SetProviderField {
            provider: form.provider.clone(),
            account: form.account.clone(),
            field,
            raw,
            draft_key,
        })
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

    fn provider_health(&self, provider: &str, account: &str) -> ProviderHealthState {
        self.provider_health
            .get(&provider_account_key(provider, account))
            .cloned()
            .unwrap_or_default()
    }

    fn invalidate_provider_health(&mut self, provider: &str, account: &str) {
        let health = self
            .provider_health
            .entry(provider_account_key(provider, account))
            .or_default();
        health.stale = health.last_verified_at_unix.is_some();
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
            if let Some((provider, account)) = done_account.clone() {
                self.invalidate_provider_health(&provider, &account);
                self.set_provider_status(provider, account, "Sign-in saved to lm.providers");
            }
            self.provider_refresh_needed = true;
            self.provider_refresh_health_invalidation = done_account;
            self.device_login = None;
        }
        true
    }
}

fn settings_operation_worker(
    request_rx: Receiver<SettingsOperationRequest>,
    result_tx: Sender<SettingsOperationResult>,
    waker: Option<sui::UiHandle>,
) {
    while let Ok(request) = request_rx.recv() {
        let payload = execute_settings_operation(&request.backend, request.operation);
        let result = SettingsOperationResult {
            request_id: request.request_id,
            scope: request.scope,
            draft_generation: request.draft_generation,
            payload,
        };
        if result_tx.send(result).is_err() {
            break;
        }
        if let Some(waker) = waker.as_ref() {
            waker.wake();
        }
    }
}

fn execute_settings_operation(
    backend: &Backend,
    operation: SettingsOperation,
) -> SettingsOperationPayload {
    match operation {
        SettingsOperation::SetConfigValue {
            key,
            kind,
            raw,
            draft_key,
        } => SettingsOperationPayload::Config((|| {
            set_value(backend.config.as_ref(), &key, kind, &raw)?;
            Ok(ConfigOperationSuccess {
                items: resolve_items(backend.config.as_ref()),
                saved_draft: Some(ConfigSavedDraft::Raw {
                    key: draft_key,
                    value: raw,
                }),
            })
        })()),
        SettingsOperation::SetConfigDocument {
            key,
            value,
            saved_draft,
        } => SettingsOperationPayload::Config((|| {
            backend
                .config
                .set(&key, value, ConfigWriteTarget::Auto)
                .map_err(|error| error.to_string())?;
            Ok(ConfigOperationSuccess {
                items: resolve_items(backend.config.as_ref()),
                saved_draft,
            })
        })()),
        SettingsOperation::SetConfigBool { key, value } => {
            SettingsOperationPayload::Config((|| {
                set_bool(backend.config.as_ref(), &key, value)?;
                Ok(ConfigOperationSuccess {
                    items: resolve_items(backend.config.as_ref()),
                    saved_draft: None,
                })
            })())
        }
        SettingsOperation::AddProvider {
            provider,
            account,
            api_key,
            model,
            base_url,
            uses_device_login,
        } => SettingsOperationPayload::Providers((|| {
            add_provider_account(
                backend.config.as_ref(),
                &provider,
                &account,
                &api_key,
                &model,
                &base_url,
            )?;
            let account = sinomo_lm::normalize_provider_account_name(Some(&account));
            provider_operation_success(
                backend,
                ProviderOperationEffect::Added {
                    provider,
                    account,
                    uses_device_login,
                },
            )
        })()),
        SettingsOperation::RefreshProviders { invalidate_health } => {
            SettingsOperationPayload::Providers((|| {
                if let Some((provider, account)) = invalidate_health {
                    mark_provider_health_stale(backend.config.as_ref(), &provider, &account)?;
                }
                provider_operation_success(backend, ProviderOperationEffect::Refreshed)
            })())
        }
        SettingsOperation::SetProviderEnabled {
            provider,
            account,
            enabled,
        } => SettingsOperationPayload::Providers((|| {
            set_provider_enabled(backend.config.as_ref(), &provider, &account, enabled)?;
            provider_operation_success(
                backend,
                ProviderOperationEffect::Updated {
                    provider,
                    account,
                    message: if enabled {
                        "Provider enabled in lm.providers".to_string()
                    } else {
                        "Provider disabled in lm.providers".to_string()
                    },
                    saved_draft: None,
                    invalidates_health: false,
                },
            )
        })()),
        SettingsOperation::SetProviderChatDefault { provider, account } => {
            SettingsOperationPayload::Providers((|| {
                set_provider_chat_default(backend.config.as_ref(), &provider, &account)?;
                provider_operation_success(
                    backend,
                    ProviderOperationEffect::Updated {
                        provider,
                        account,
                        message: "Chat default saved to lm.providers".to_string(),
                        saved_draft: None,
                        invalidates_health: false,
                    },
                )
            })())
        }
        SettingsOperation::RemoveProvider { provider, account } => {
            SettingsOperationPayload::Providers((|| {
                remove_provider_account(backend.config.as_ref(), &provider, &account)?;
                remove_provider_health(backend.config.as_ref(), &provider, &account)?;
                provider_operation_success(
                    backend,
                    ProviderOperationEffect::Removed { provider, account },
                )
            })())
        }
        SettingsOperation::SetProviderField {
            provider,
            account,
            field,
            raw,
            draft_key,
        } => SettingsOperationPayload::Providers((|| {
            match field {
                ProviderField::ApiKey => {
                    set_provider_api_key(backend.config.as_ref(), &provider, &account, &raw)
                }
                ProviderField::Model => {
                    set_provider_model(backend.config.as_ref(), &provider, &account, &raw)
                }
                ProviderField::BaseUrl => {
                    set_provider_base_url(backend.config.as_ref(), &provider, &account, &raw)
                }
                ProviderField::ReasoningBudget => set_provider_reasoning_budget(
                    backend.config.as_ref(),
                    &provider,
                    &account,
                    &raw,
                ),
                ProviderField::VendorOptions => {
                    set_provider_vendor_options(backend.config.as_ref(), &provider, &account, &raw)
                }
            }?;
            mark_provider_health_stale(backend.config.as_ref(), &provider, &account)?;
            provider_operation_success(
                backend,
                ProviderOperationEffect::Updated {
                    provider,
                    account,
                    message: provider_field_saved_message(field).to_string(),
                    saved_draft: Some((draft_key, raw)),
                    invalidates_health: true,
                },
            )
        })()),
        SettingsOperation::SetProviderEffort {
            provider,
            account,
            effort_index,
        } => SettingsOperationPayload::Providers((|| {
            set_provider_effort(backend.config.as_ref(), &provider, &account, effort_index)?;
            mark_provider_health_stale(backend.config.as_ref(), &provider, &account)?;
            provider_operation_success(
                backend,
                ProviderOperationEffect::Updated {
                    provider,
                    account,
                    message: "Reasoning effort saved to lm.providers".to_string(),
                    saved_draft: None,
                    invalidates_health: true,
                },
            )
        })()),
        SettingsOperation::SetProviderVerbosity {
            provider,
            account,
            verbosity_index,
        } => SettingsOperationPayload::Providers((|| {
            set_provider_text_verbosity(
                backend.config.as_ref(),
                &provider,
                &account,
                verbosity_index,
            )?;
            mark_provider_health_stale(backend.config.as_ref(), &provider, &account)?;
            provider_operation_success(
                backend,
                ProviderOperationEffect::Updated {
                    provider,
                    account,
                    message: "Text verbosity saved to lm.providers".to_string(),
                    saved_draft: None,
                    invalidates_health: true,
                },
            )
        })()),
        SettingsOperation::TestProviderConnection { provider, account } => {
            let result = test_provider_connection(
                backend.data_dir.as_ref(),
                backend.config.as_ref(),
                &provider,
                &account,
            );
            SettingsOperationPayload::ProviderHealth(ProviderHealthOperationResult {
                provider,
                account,
                result,
            })
        }
        SettingsOperation::RefreshAgents { selected_id } => {
            SettingsOperationPayload::Agents(Ok(agent_operation_success(
                backend,
                selected_id.as_deref(),
                AgentOperationEffect::Refreshed,
            )))
        }
        SettingsOperation::SaveAgent {
            definition,
            previous_id,
        } => SettingsOperationPayload::Agents((|| {
            backend.save_custom_agent_definition(&definition, previous_id.as_deref())?;
            let id = definition.key().to_string();
            Ok(agent_operation_success(
                backend,
                Some(&id),
                AgentOperationEffect::Saved(id.clone()),
            ))
        })()),
        SettingsOperation::DeleteAgent { id } => SettingsOperationPayload::Agents((|| {
            backend.delete_custom_agent_definition(&id)?;
            Ok(agent_operation_success(
                backend,
                None,
                AgentOperationEffect::Deleted(id),
            ))
        })()),
        SettingsOperation::ImportAgent { path } => SettingsOperationPayload::Agents((|| {
            let id = backend.import_agent_definition(&path)?;
            Ok(agent_operation_success(
                backend,
                Some(&id),
                AgentOperationEffect::Imported(id.clone()),
            ))
        })()),
        SettingsOperation::ExportAgent { id, output } => SettingsOperationPayload::Agents((|| {
            let path = backend.export_agent_definition(&id, &output)?;
            Ok(agent_operation_success(
                backend,
                Some(&id),
                AgentOperationEffect::Exported(path),
            ))
        })(
        )),
        SettingsOperation::ValidateAgent { id } => SettingsOperationPayload::Agents((|| {
            let report = backend.validate_agent_definition(&id)?;
            Ok(agent_operation_success(
                backend,
                Some(&id),
                AgentOperationEffect::Validated(report),
            ))
        })()),
        SettingsOperation::TestAgent { id } => SettingsOperationPayload::Agents((|| {
            let report = backend.test_agent_definition(&id)?;
            Ok(agent_operation_success(
                backend,
                Some(&id),
                AgentOperationEffect::Tested(report),
            ))
        })()),
        SettingsOperation::RefreshModules { selected_node_id } => {
            SettingsOperationPayload::Modules(Ok(ModuleOperationSuccess {
                snapshot: backend.module_config_snapshot(selected_node_id.as_deref()),
                message: None,
            }))
        }
        SettingsOperation::SetClusterModuleDefault {
            selected_node_id,
            module_id,
            enabled,
        } => SettingsOperationPayload::Modules((|| {
            backend.set_cluster_module_default(&module_id, enabled)?;
            Ok(ModuleOperationSuccess {
                snapshot: backend.module_config_snapshot(selected_node_id.as_deref()),
                message: Some(format!(
                    "Cluster default saved for {}",
                    module_display_id(&module_id)
                )),
            })
        })()),
        SettingsOperation::SetNodeModuleOverride {
            node_id,
            module_id,
            enabled,
        } => SettingsOperationPayload::Modules((|| {
            backend.set_node_module_override(&node_id, &module_id, enabled)?;
            let message = match enabled {
                Some(true) => format!(
                    "Node override enabled for {}",
                    module_display_id(&module_id)
                ),
                Some(false) => {
                    format!(
                        "Node override disabled for {}",
                        module_display_id(&module_id)
                    )
                }
                None => format!(
                    "Node override cleared for {}",
                    module_display_id(&module_id)
                ),
            };
            Ok(ModuleOperationSuccess {
                snapshot: backend.module_config_snapshot(Some(&node_id)),
                message: Some(message),
            })
        })()),
        SettingsOperation::RefreshSkills {
            include_all,
            selected,
        } => SettingsOperationPayload::Skills(load_skill_operation_success(
            backend.data_dir.as_ref(),
            include_all,
            selected,
            None,
        )),
        SettingsOperation::ImportSkill { path } => SettingsOperationPayload::Skills((|| {
            let result = sinomo_cli::app_api::import_skill(
                backend.data_dir.as_ref(),
                &path,
                None,
                None,
                true,
            )?;
            load_skill_operation_success(
                backend.data_dir.as_ref(),
                true,
                Some((result.skill.id.clone(), result.skill.version.clone())),
                Some(skill_mutation_message(&result)),
            )
        })()),
        SettingsOperation::ExportSkill {
            skill_id,
            version,
            output,
            include_all,
        } => SettingsOperationPayload::Skills((|| {
            let result = sinomo_cli::app_api::export_skill(
                backend.data_dir.as_ref(),
                &skill_id,
                Some(&version),
                Some(&output),
            )?;
            load_skill_operation_success(
                backend.data_dir.as_ref(),
                include_all,
                Some((skill_id, version)),
                Some(skill_mutation_message(&result)),
            )
        })()),
        SettingsOperation::ActivateSkill {
            skill_id,
            version,
            include_all,
        } => SettingsOperationPayload::Skills((|| {
            let result = sinomo_cli::app_api::activate_skill(
                backend.data_dir.as_ref(),
                &skill_id,
                Some(&version),
            )?;
            load_skill_operation_success(
                backend.data_dir.as_ref(),
                include_all,
                Some((result.skill.id.clone(), result.skill.version.clone())),
                Some(skill_mutation_message(&result)),
            )
        })()),
        SettingsOperation::CreateClusterInvite => SettingsOperationPayload::Cluster((|| {
            let invite = backend.create_cluster_invite()?;
            Ok(ClusterOperationSuccess {
                status: backend.app_cluster_status(),
                snapshot: backend.cluster_snapshot(),
                effect: ClusterOperationEffect::Invite(invite),
            })
        })()),
        SettingsOperation::LeaveCluster => SettingsOperationPayload::Cluster((|| {
            let status = backend.leave_cluster()?;
            Ok(ClusterOperationSuccess {
                status,
                snapshot: backend.cluster_snapshot(),
                effect: ClusterOperationEffect::Left,
            })
        })()),
        SettingsOperation::JoinCluster { invite } => SettingsOperationPayload::Cluster((|| {
            let status = backend.join_cluster(&invite)?;
            Ok(ClusterOperationSuccess {
                status,
                snapshot: backend.cluster_snapshot(),
                effect: ClusterOperationEffect::Joined,
            })
        })(
        )),
    }
}

fn provider_operation_success(
    backend: &Backend,
    effect: ProviderOperationEffect,
) -> Result<ProviderOperationSuccess, String> {
    backend
        .config
        .catalog(None)
        .map_err(|error| error.to_string())?;
    Ok(ProviderOperationSuccess {
        items: resolve_items(backend.config.as_ref()),
        forms: provider_forms(backend.config.as_ref()),
        chat_providers: cluster_provider_summaries(backend.config.as_ref()),
        health: provider_health_states(load_provider_health(backend.config.as_ref())),
        effect,
    })
}

fn agent_operation_success(
    backend: &Backend,
    selected_id: Option<&str>,
    effect: AgentOperationEffect,
) -> AgentOperationSuccess {
    AgentOperationSuccess {
        snapshot: backend.agent_config_snapshot(selected_id),
        chat_agents: agent_summaries(backend.data_dir.as_ref()),
        effect,
    }
}

fn load_skill_operation_success(
    data_dir: &SinomoDataDir,
    include_all: bool,
    selected: Option<(String, String)>,
    message: Option<String>,
) -> Result<SkillOperationSuccess, String> {
    let skills = sinomo_cli::app_api::skill_summaries(data_dir, include_all)?;
    let selected = selected
        .filter(|(id, version)| {
            skills
                .iter()
                .any(|skill| &skill.id == id && &skill.version == version)
        })
        .or_else(|| {
            skills
                .first()
                .map(|skill| (skill.id.clone(), skill.version.clone()))
        });
    let detail = match selected.as_ref() {
        Some((id, version)) => Some(sinomo_cli::app_api::skill_detail(
            data_dir,
            id,
            Some(version),
        )?),
        None => None,
    };
    Ok(SkillOperationSuccess {
        skills,
        selected,
        detail,
        message,
    })
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

fn config_schema_field_prefix(key: &str) -> String {
    format!("config-schema:{key}:")
}

fn config_schema_field_key(key: &str, path: &[ConfigPathSegment]) -> String {
    let mut output = config_schema_field_prefix(key);
    if path.is_empty() {
        output.push('$');
        return output;
    }
    for segment in path {
        match segment {
            ConfigPathSegment::Field(field) => {
                output.push('.');
                output.push_str(&field.len().to_string());
                output.push(':');
                output.push_str(field);
            }
            ConfigPathSegment::Index(index) => {
                output.push('[');
                output.push_str(&index.to_string());
                output.push(']');
            }
        }
    }
    output
}

fn config_schema_add_key(key: &str, path: &[ConfigPathSegment]) -> String {
    format!("{}:add", config_schema_field_key(key, path))
}

fn clear_config_schema_auxiliary_path<T>(
    values: &mut HashMap<String, T>,
    config_key: &str,
    path: &[ConfigPathSegment],
) {
    let prefix = config_schema_field_key(config_key, path);
    values.retain(|key, _| !key.starts_with(&prefix));
}

fn move_config_schema_auxiliary_path<T>(
    values: &mut HashMap<String, T>,
    config_key: &str,
    old_path: &[ConfigPathSegment],
    new_path: &[ConfigPathSegment],
) {
    let old_prefix = config_schema_field_key(config_key, old_path);
    let new_prefix = config_schema_field_key(config_key, new_path);
    let moves = values
        .keys()
        .filter(|key| key.starts_with(&old_prefix))
        .cloned()
        .collect::<Vec<_>>();
    for old_key in moves {
        let new_key = format!("{}{}", new_prefix, &old_key[old_prefix.len()..]);
        if let Some(value) = values.remove(&old_key) {
            values.insert(new_key, value);
        }
    }
}

fn parse_schema_editor_value(schema: &ConfigSchemaValue, raw: &str) -> Result<Value, String> {
    let trimmed = raw.trim();
    match schema {
        ConfigSchemaValue::Bool => Ok(Value::Bool(trimmed.eq_ignore_ascii_case("true"))),
        ConfigSchemaValue::I64 { range } => {
            let value = trimmed
                .parse::<i64>()
                .map_err(|error| format!("invalid i64: {error}"))?;
            validate_schema_editor_number_range(value as f64, range.as_ref())?;
            Ok(Value::Number(value.into()))
        }
        ConfigSchemaValue::U64 { range } => {
            let value = trimmed
                .parse::<u64>()
                .map_err(|error| format!("invalid u64: {error}"))?;
            validate_schema_editor_number_range(value as f64, range.as_ref())?;
            Ok(Value::Number(value.into()))
        }
        ConfigSchemaValue::F64 { range } => {
            let value = trimmed
                .parse::<f64>()
                .map_err(|error| format!("invalid number: {error}"))?;
            validate_schema_editor_number_range(value, range.as_ref())?;
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

fn validate_schema_editor_number_range(
    value: f64,
    range: Option<&sinomo_runtime::config::ConfigNumberRange>,
) -> Result<(), String> {
    let Some(range) = range else {
        return Ok(());
    };
    if let Some(minimum) = range.min
        && value < minimum
    {
        return Err(format!("value must be at least {minimum}"));
    }
    if let Some(maximum) = range.max
        && value > maximum
    {
        return Err(format!("value must be at most {maximum}"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ValueContainerKind {
    Object,
    List,
}

fn config_value_mut_at_path<'a>(
    root: &'a mut Value,
    path: &[ConfigPathSegment],
    target_kind: ValueContainerKind,
) -> Result<&'a mut Value, String> {
    let mut current = root;
    for (index, segment) in path.iter().enumerate() {
        let next_kind = path
            .get(index + 1)
            .map(|next| match next {
                ConfigPathSegment::Field(_) => ValueContainerKind::Object,
                ConfigPathSegment::Index(_) => ValueContainerKind::List,
            })
            .unwrap_or(target_kind);
        match segment {
            ConfigPathSegment::Field(field) => {
                if !current.is_object() {
                    *current = Value::Object(serde_json::Map::new());
                }
                let map = current
                    .as_object_mut()
                    .ok_or_else(|| format!("{field} is not an object field"))?;
                current = map.entry(field.clone()).or_insert(Value::Null);
            }
            ConfigPathSegment::Index(item_index) => {
                if !current.is_array() {
                    *current = Value::Array(Vec::new());
                }
                let list = current
                    .as_array_mut()
                    .ok_or_else(|| format!("item {item_index} is not in a list"))?;
                while list.len() <= *item_index {
                    list.push(Value::Null);
                }
                current = &mut list[*item_index];
            }
        }
        if current.is_null() {
            *current = match next_kind {
                ValueContainerKind::Object => Value::Object(serde_json::Map::new()),
                ValueContainerKind::List => Value::Array(Vec::new()),
            };
        }
    }
    Ok(current)
}

fn set_json_path_value(
    root: &mut Value,
    path: &[ConfigPathSegment],
    value: Value,
) -> Result<(), String> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }
    let (parent_path, tail) = path.split_at(path.len() - 1);
    let parent_kind = match tail[0] {
        ConfigPathSegment::Field(_) => ValueContainerKind::Object,
        ConfigPathSegment::Index(_) => ValueContainerKind::List,
    };
    let parent = config_value_mut_at_path(root, parent_path, parent_kind)?;
    match &tail[0] {
        ConfigPathSegment::Field(field) => {
            if !parent.is_object() {
                *parent = Value::Object(serde_json::Map::new());
            }
            parent
                .as_object_mut()
                .ok_or_else(|| "parent value is not an object".to_string())?
                .insert(field.clone(), value);
        }
        ConfigPathSegment::Index(index) => {
            if !parent.is_array() {
                *parent = Value::Array(Vec::new());
            }
            let list = parent
                .as_array_mut()
                .ok_or_else(|| "parent value is not an array".to_string())?;
            while list.len() <= *index {
                list.push(Value::Null);
            }
            list[*index] = value;
        }
    }
    Ok(())
}

fn remove_json_path_value(root: &mut Value, path: &[ConfigPathSegment]) -> Result<(), String> {
    if path.is_empty() {
        *root = Value::Null;
        return Ok(());
    }
    let (parent_path, tail) = path.split_at(path.len() - 1);
    let parent_kind = match tail[0] {
        ConfigPathSegment::Field(_) => ValueContainerKind::Object,
        ConfigPathSegment::Index(_) => ValueContainerKind::List,
    };
    let parent = config_value_mut_at_path(root, parent_path, parent_kind)?;
    match &tail[0] {
        ConfigPathSegment::Field(field) => {
            parent
                .as_object_mut()
                .ok_or_else(|| "parent value is not an object".to_string())?
                .remove(field);
        }
        ConfigPathSegment::Index(index) => {
            let list = parent
                .as_array_mut()
                .ok_or_else(|| "parent value is not an array".to_string())?;
            if *index >= list.len() {
                return Err(format!("list item {index} no longer exists"));
            }
            list.remove(*index);
        }
    }
    Ok(())
}

fn schema_default_value(schema: &ConfigSchemaValue) -> Value {
    match schema {
        ConfigSchemaValue::Bool => Value::Bool(false),
        ConfigSchemaValue::I64 { range } => {
            let value = schema_default_number(range.as_ref()).round() as i64;
            Value::Number(value.into())
        }
        ConfigSchemaValue::U64 { range } => {
            let value = schema_default_number(range.as_ref()).max(0.0).round() as u64;
            Value::Number(value.into())
        }
        ConfigSchemaValue::F64 { range } => {
            serde_json::Number::from_f64(schema_default_number(range.as_ref()))
                .map(Value::Number)
                .unwrap_or_else(|| Value::Number(0.into()))
        }
        ConfigSchemaValue::String { choices, .. } => Value::String(
            choices
                .first()
                .map(|choice| choice.value.clone())
                .unwrap_or_default(),
        ),
        ConfigSchemaValue::StringList | ConfigSchemaValue::List { .. } => Value::Array(Vec::new()),
        ConfigSchemaValue::Object { fields, .. } => Value::Object(
            fields
                .iter()
                .filter(|field| field.required || field.default_value.is_some())
                .map(|field| {
                    (
                        field.name.clone(),
                        field
                            .default_value
                            .clone()
                            .unwrap_or_else(|| schema_default_value(&field.value)),
                    )
                })
                .collect(),
        ),
        ConfigSchemaValue::Map { .. } => Value::Object(serde_json::Map::new()),
        ConfigSchemaValue::Json => Value::Null,
    }
}

fn schema_default_number(range: Option<&sinomo_runtime::config::ConfigNumberRange>) -> f64 {
    let Some(range) = range else {
        return 0.0;
    };
    let mut value: f64 = 0.0;
    if let Some(min) = range.min {
        value = value.max(min);
    }
    if let Some(max) = range.max {
        value = value.min(max);
    }
    value
}

/// Field-error map key for a provider text field (Providers section).
fn provider_field_key(provider: &str, account: &str, field: ProviderField) -> String {
    let suffix = match field {
        ProviderField::ApiKey => "api_key",
        ProviderField::Model => "model",
        ProviderField::BaseUrl => "base_url",
        ProviderField::ReasoningBudget => "reasoning_budget_tokens",
        ProviderField::VendorOptions => "vendor_options",
    };
    format!("provider:{provider}:{account}:{suffix}")
}

fn provider_field_saved_message(field: ProviderField) -> &'static str {
    match field {
        ProviderField::ApiKey => "API key saved to lm.providers",
        ProviderField::Model => "Default model saved to lm.providers",
        ProviderField::BaseUrl => "API base URL saved to lm.providers",
        ProviderField::ReasoningBudget => "Reasoning budget saved to lm.providers",
        ProviderField::VendorOptions => "Provider options saved to lm.providers",
    }
}

fn provider_account_key(provider: &str, account: &str) -> String {
    sinomo_lm::provider_health_account_key(provider, account)
}

fn provider_health_states(
    config: sinomo_lm::LanguageModelProviderHealthConfig,
) -> HashMap<String, ProviderHealthState> {
    config
        .accounts
        .into_iter()
        .map(|(key, snapshot)| (key, ProviderHealthState::from_snapshot(snapshot)))
        .collect()
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

fn role_index(role: &AgentRole) -> usize {
    match role {
        AgentRole::Main => 0,
        AgentRole::Background => 1,
        AgentRole::Deliberation => 2,
        AgentRole::Evolution => 3,
    }
}

fn role_for_index(index: usize) -> AgentRole {
    match index {
        1 => AgentRole::Background,
        2 => AgentRole::Deliberation,
        3 => AgentRole::Evolution,
        _ => AgentRole::Main,
    }
}

fn output_format_index(output_format: &AgentOutputFormat) -> usize {
    match output_format {
        AgentOutputFormat::JsonDecision => 0,
        AgentOutputFormat::Text => 1,
    }
}

fn output_format_for_index(index: usize) -> AgentOutputFormat {
    match index {
        1 => AgentOutputFormat::Text,
        _ => AgentOutputFormat::JsonDecision,
    }
}

fn optional_trimmed_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn parse_agent_list(value: &str) -> Vec<String> {
    value
        .split(|ch| ch == '\n' || ch == ',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_optional_f32(label: &str, value: &str) -> Result<Option<f32>, String> {
    let Some(value) = optional_trimmed_string(value) else {
        return Ok(None);
    };
    let parsed = value
        .parse::<f32>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err(format!(
            "{label} must be a finite value greater than or equal to 0"
        ));
    }
    Ok(Some(parsed))
}

fn parse_optional_u32(label: &str, value: &str) -> Result<Option<u32>, String> {
    let Some(value) = optional_trimmed_string(value) else {
        return Ok(None);
    };
    let parsed = value
        .parse::<u32>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{label} must be greater than 0"));
    }
    Ok(Some(parsed))
}

fn parse_optional_usize(label: &str, value: &str) -> Result<Option<usize>, String> {
    let Some(value) = optional_trimmed_string(value) else {
        return Ok(None);
    };
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{label} must be greater than 0"));
    }
    Ok(Some(parsed))
}

fn unique_agent_id(snapshot: &AgentConfigSnapshot, base: &str) -> String {
    let mut candidate = sanitize_agent_id(base);
    if candidate.is_empty() {
        candidate = "custom-agent".to_string();
    }
    if !snapshot.agents.iter().any(|agent| agent.id == candidate) {
        return candidate;
    }
    for index in 2..1000 {
        let next = format!("{candidate}-{index}");
        if !snapshot.agents.iter().any(|agent| agent.id == next) {
            return next;
        }
    }
    format!("{candidate}-{}", snapshot.agents.len() + 1)
}

fn sanitize_agent_id(value: &str) -> String {
    value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn agent_custom_count(snapshot: &AgentConfigSnapshot) -> usize {
    snapshot
        .agents
        .iter()
        .filter(|agent| agent.editable)
        .count()
}

fn agent_sidebar_preview(snapshot: &AgentConfigSnapshot) -> String {
    if snapshot.agents.is_empty() {
        return "No agents".to_string();
    }
    let custom = agent_custom_count(snapshot);
    if custom == 0 {
        format!("{} built-ins", snapshot.agents.len())
    } else {
        format!("{custom} custom · {} total", snapshot.agents.len())
    }
}

fn agent_summary_line(agent: &AgentConfigSummary) -> String {
    let mut parts = vec![
        agent.role.clone(),
        format!("{} package {}", agent.source, agent.package_version),
    ];
    if agent.tool_count == 1 {
        parts.push("1 tool".to_string());
    } else if agent.tool_count > 1 {
        parts.push(format!("{} tools", agent.tool_count));
    }
    if agent.skill_count == 1 {
        parts.push("1 skill".to_string());
    } else if agent.skill_count > 1 {
        parts.push(format!("{} skills", agent.skill_count));
    }
    if let Some(node_id) = agent.sandbox_node_id.as_ref()
        && !node_id.trim().is_empty()
    {
        parts.push(format!("sandbox {}", short(node_id, 12)));
    }
    parts.join(" · ")
}

fn agent_catalog_row(shared: &Shared, agent: &AgentConfigSummary) -> TrailingSlotRow {
    let title = if agent.name.trim().is_empty() {
        agent.id.clone()
    } else {
        agent.name.clone()
    };
    let body = Flex::vertical()
        .main_gap(2.0)
        .align_items(Alignment::Stretch)
        .with_child(primary_label(shared, title))
        .with_child(muted_meta_label(shared, agent_summary_line(agent)));
    let trailing = muted_meta_label(
        shared,
        match agent.readiness {
            AgentReadiness::Ready => "Ready",
            AgentReadiness::NeedsAttention => "Review",
        },
    );
    TrailingSlotRow::new(body, trailing)
        .trailing_width(52.0)
        .trailing_height(20.0)
        .gap(8.0)
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

fn skill_catalog_row(shared: &Shared, skill: &SkillSummary) -> TrailingSlotRow {
    let state = if skill.active {
        "active"
    } else {
        skill.status.as_str()
    };
    let body = Flex::vertical()
        .main_gap(2.0)
        .align_items(Alignment::Stretch)
        .with_child(primary_label(shared, skill_title(skill)))
        .with_child(muted_meta_label(
            shared,
            format!("{} · {}", state, skill_source_label(skill)),
        ));
    TrailingSlotRow::new(body, SizedBox::new())
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
            format!(
                "{} · {} text",
                app.theme_mode.label(),
                app.text_scale.label()
            )
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
        SettingsSection::Agents => {
            let settings = shared.settings();
            agent_sidebar_preview(&settings.agent_snapshot)
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

/// Resolve the app chrome palette for the current theme.
fn palette(shared: &Shared) -> Palette {
    Palette::from_theme(&shared.theme(), widgets::theme_is_dark(shared))
}

/// A muted `Label` whose color tracks the theme.
fn muted_label(shared: &Shared, text: impl Into<String>) -> Label {
    let token = shared.theme().text.base;
    muted_label_with_token(shared, text, token)
}

fn settings_text_style(shared: &Shared, token: ThemeTextToken) -> sui::TextStyle {
    sui::TextStyle {
        font_size: token.size,
        line_height: token.line_height,
        ..shared.theme().body_text_style()
    }
}

fn settings_label(shared: &Shared, text: impl Into<String>, token: ThemeTextToken) -> Label {
    Label::new(text).style(settings_text_style(shared, token))
}

fn muted_label_with_token(
    shared: &Shared,
    text: impl Into<String>,
    token: ThemeTextToken,
) -> Label {
    let s = shared.clone();
    settings_label(shared, text, token).color_when(move || palette(&s).text_muted)
}

fn muted_meta_label(shared: &Shared, text: impl Into<String>) -> Label {
    let token = shared.theme().text.sm;
    muted_label_with_token(shared, text, token)
}

fn primary_label(shared: &Shared, text: impl Into<String>) -> Label {
    let s = shared.clone();
    let token = shared.theme().text.base;
    settings_label(shared, text, token).color_when(move || palette(&s).text)
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

/// Feedback changes frequently while editing. Keep it in a retained switch so an update never
/// replaces sibling text controls and invalidates their focus or caret state.
fn feedback_slot<E, S>(
    shared: &Shared,
    reader: &ThemeReader,
    error_when: E,
    status_when: S,
) -> SwitchView
where
    E: Fn(&SettingsState) -> Option<String> + 'static,
    S: Fn(&SettingsState) -> Option<String> + 'static,
{
    let error: Rc<dyn Fn() -> Option<String>> = {
        let s = shared.clone();
        Rc::new(move || error_when(&s.settings()))
    };
    let status: Rc<dyn Fn() -> Option<String>> = {
        let s = shared.clone();
        Rc::new(move || status_when(&s.settings()))
    };
    let error_label = Label::dynamic("Settings error", {
        let error = Rc::clone(&error);
        move || error().unwrap_or_default()
    })
    .style(settings_text_style(shared, shared.theme().text.base))
    .color_when({
        let s = shared.clone();
        move || palette(&s).danger_text
    });
    let status_label = Label::dynamic("Settings status", {
        let status = Rc::clone(&status);
        move || status().unwrap_or_default()
    })
    .style(settings_text_style(shared, shared.theme().text.base))
    .color_when({
        let s = shared.clone();
        move || palette(&s).text_muted
    });

    SwitchView::new()
        .with_child(SizedBox::new().height(0.0))
        .with_child(
            FormRow::new("Error", error_label)
                .inline()
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new("Status", status_label)
                .inline()
                .theme_when(clone_reader(reader)),
        )
        .selected_when(move || {
            if error().is_some() {
                1
            } else if status().is_some() {
                2
            } else {
                0
            }
        })
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
    if let Some(detail) = ready.backend.connection_issue_detail() {
        issues.push(ConnectionIssue {
            title: "Daemon startup",
            detail,
            tab: Tab::Settings,
            action: "Open Settings",
        });
        return issues;
    }
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

#[cfg(test)]
#[allow(dead_code)]
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

#[cfg(test)]
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

#[cfg(test)]
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
        "clustered:{};summary:{:?};invite:{invite_key};leave:{};join:{}",
        settings.clustered,
        settings.cluster_summary,
        settings.leave_confirming,
        settings.join_confirming
    )
}

#[cfg(test)]
#[allow(dead_code)]
fn configuration_key(shared: &Shared) -> String {
    let settings = shared.settings();
    let item_key = settings
        .config_items
        .iter()
        .map(config_item_rebuild_signature)
        .collect::<Vec<_>>()
        .join("\n");
    let expanded = settings
        .advanced_detail_expanded
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{item_key}\nexpanded:{expanded};revision:{}",
        settings.config_ui_revision
    )
}

#[cfg(test)]
fn config_item_rebuild_signature(item: &ConfigItem) -> String {
    format!(
        "{}:{}:{}:{:?}:{:?}:{:?}:{}:{:?}:{:?}",
        item.key,
        item.owner,
        item.source,
        item.module_id,
        item.edit_kind,
        item.bool_value,
        item.read_only,
        item.raw_value,
        item.schema,
    )
}

#[cfg(test)]
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
        "all:{};selected:{:?};detail:{:?};text:{}\n{}",
        settings.skills_include_all,
        settings.skill_selected,
        settings
            .skill_detail
            .as_ref()
            .map(|detail| detail.summary.content_hash.as_str()),
        settings.skill_text_expanded,
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
    shared.set_active_tab(tab);
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
    let value = value.into();
    let input = selectable_read_only_input(reader, name, value.clone());
    let copy_value = value;
    let copy = sui::Button::new("Copy")
        .semantic_name(format!("Copy {label}"))
        .description(format!("Copy the full {label} to the clipboard"))
        .icon(IconGlyph::Restore)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| ctx.set_clipboard_text(&copy_value));
    FormRow::new(
        label,
        Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(8.0)
            .align_items(Alignment::Center)
            .with_item(input, FlexItem::fill().min_width(220.0))
            .with_item(copy, FlexItem::new().no_shrink()),
    )
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
    let value = value.into();
    let area = SizedBox::new()
        .height(72.0)
        .with_child(selectable_read_only_area(reader, name, value.clone()));
    let copy_value = value;
    let copy = sui::Button::new("Copy")
        .semantic_name(format!("Copy {label}"))
        .description(format!("Copy the full {label} to the clipboard"))
        .icon(IconGlyph::Restore)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| ctx.set_clipboard_text(&copy_value));
    FormRow::new(
        label,
        Stack::vertical()
            .spacing(8.0)
            .alignment(Alignment::Stretch)
            .with_child(area)
            .with_child(
                Stack::horizontal()
                    .alignment(Alignment::Start)
                    .with_child(copy),
            ),
    )
    .stacked()
    .theme_when(clone_reader(reader))
}

/// A read-only single-line field that behaves like selectable document text.
fn selectable_read_only_input(reader: &ThemeReader, name: &str, value: String) -> ContextMenu {
    let has_text = !value.is_empty();
    let selection = SelectionScope::new();
    let field = TextInput::new(name)
        .value(value)
        .read_only()
        .selection_scope(selection.clone())
        .theme_when(clone_reader(reader));
    read_only_text_menu(reader, name, field, selection, has_text)
}

/// A read-only multiline field that behaves like selectable document text.
fn selectable_read_only_area(reader: &ThemeReader, name: &str, value: String) -> ContextMenu {
    let has_text = !value.is_empty();
    let selection = SelectionScope::new();
    let field = TextArea::new(name)
        .value(value)
        .read_only()
        .selection_scope(selection.clone())
        .theme_when(clone_reader(reader));
    read_only_text_menu(reader, name, field, selection, has_text)
}

/// Wrap a SUI text editor in its standard read-only selection menu.
///
/// The editor remains authoritative for pointer selection, hotkeys, clipboard
/// contents, and accessibility actions. Menu activation only posts the same
/// typed commands understood by `TextInput` and `TextArea`.
fn read_only_text_menu<W>(
    reader: &ThemeReader,
    name: &str,
    field: W,
    selection: SelectionScope,
    has_text: bool,
) -> ContextMenu
where
    W: Widget + 'static,
{
    let menu =
        ContextMenu::new(format!("{name}-context-menu"), field).theme_when(clone_reader(reader));
    let field_id = menu.trigger_id();

    menu.items_when(move || {
        let has_selection = selection.has_owner_selection(field_id);
        let copy = MenuItem::new("Copy").shortcut("Ctrl+C");
        let select_all = MenuItem::new("Select All")
            .shortcut("Ctrl+A")
            .separator_before();
        vec![
            if has_selection { copy } else { copy.disabled() },
            if has_text {
                select_all
            } else {
                select_all.disabled()
            },
        ]
    })
    .on_activate_with_ctx(move |ctx, _index, item| {
        let command = match item.label() {
            "Copy" => Some(TextCommand::Copy),
            "Select All" => Some(TextCommand::SelectAll),
            _ => None,
        };
        if let Some(command) = command {
            ctx.post_command(field_id, sui::TEXT_COMMAND, command);
        }
    })
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

/// An editable draft plus an explicit commit button. `on_change` is local-only; `on_save` queues
/// the background write. This is the default for any field whose config client may be daemon RPC.
fn text_save_row<C, S, P>(
    reader: &ThemeReader,
    name: &str,
    label: &str,
    value: impl Into<String>,
    placeholder: &str,
    on_change: C,
    on_save: S,
    pending_when: P,
) -> FormRow
where
    C: FnMut(String) + 'static,
    S: FnMut() + 'static,
    P: Fn() -> bool + 'static,
{
    let input = TextInput::new(name)
        .value(value)
        .placeholder(placeholder)
        .theme_when(clone_reader(reader))
        .on_change(on_change);
    let save = sui::Button::new("Save")
        .icon(sui::IconGlyph::Check)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .enabled_when(move || !pending_when())
        .on_press(on_save);
    FormRow::new(
        label,
        Stack::vertical()
            .spacing(8.0)
            .alignment(Alignment::Stretch)
            .with_child(input)
            .with_child(
                Stack::horizontal()
                    .alignment(Alignment::Start)
                    .with_child(save),
            ),
    )
    .stacked()
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

fn queue_settings_operation(
    shared: &Shared,
    scope: SettingsOperationScope,
    operation: SettingsOperation,
) -> bool {
    let (backend, waker) = {
        let app = shared.borrow();
        let Some(ready) = app.ready.as_ref() else {
            drop(app);
            let mut settings = shared.settings_mut();
            settings.apply_operation_queue_error(&scope, "backend is not ready".to_string());
            return false;
        };
        (ready.backend.clone(), app.waker.clone())
    };
    shared
        .settings_mut()
        .queue_operation(backend, waker, scope, operation)
}

pub(crate) fn pump_background_operations(shared: &Shared) -> bool {
    let (refresh_providers, invalidate_health) = {
        let mut settings = shared.settings_mut_unpublished();
        (
            std::mem::take(&mut settings.provider_refresh_needed),
            settings.provider_refresh_health_invalidation.take(),
        )
    };
    if refresh_providers {
        queue_settings_operation(
            shared,
            SettingsOperationScope::Providers,
            SettingsOperation::RefreshProviders { invalidate_health },
        );
    }
    let effects = { shared.settings_mut_unpublished().pump_operations() };
    if !effects.changed {
        if refresh_providers {
            return true;
        }
        return false;
    }
    let waker = shared.borrow().waker.clone();
    {
        let mut app = shared.borrow_mut();
        let Some(ready) = app.ready.as_mut() else {
            return true;
        };
        if let Some(providers) = effects.chat_providers {
            ready.chat.refresh_providers(providers);
        }
        if let Some(agents) = effects.chat_agents {
            ready.chat.refresh_agents(agents);
        }
        if let Some(snapshot) = effects.cluster_snapshot {
            ready.cluster.snapshot = snapshot;
        }
        if let Some((provider, account)) = effects.provider_device_login {
            let index = ready
                .settings
                .provider_forms
                .iter()
                .position(|form| form.provider == provider && form.account == account);
            if let Some(index) = index {
                ready.settings.begin_device_login(index, waker);
            }
        }
    }
    shared.publish_settings_signals();
    shared.publish_chat_signals();
    shared.publish_cluster_signals();
    true
}

// ---------------------------------------------------------------------------
// Section subtrees
// ---------------------------------------------------------------------------

fn index_to_mode(i: usize) -> AppThemeMode {
    match i {
        0 => AppThemeMode::Light,
        1 => AppThemeMode::Dark,
        2 => AppThemeMode::Void,
        _ => AppThemeMode::System,
    }
}

fn mode_to_index(mode: AppThemeMode) -> usize {
    match mode {
        AppThemeMode::Light => 0,
        AppThemeMode::Dark => 1,
        AppThemeMode::Void => 2,
        AppThemeMode::System => 3,
    }
}

fn text_scale_to_index(scale: AppTextScale) -> usize {
    AppTextScale::ALL
        .iter()
        .position(|candidate| *candidate == scale)
        .unwrap_or_default()
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
    let (mode_index, text_scale) = {
        let app = shared.borrow();
        (mode_to_index(app.theme_mode), app.text_scale)
    };
    let tab_bar = {
        let s = shared.clone();
        TabBar::new("theme")
            .tabs(["Light", "Dark", "Void", "System"])
            .selected(mode_index)
            .theme_when(clone_reader(reader))
            .on_change(move |i, _| {
                s.set_theme_mode(index_to_mode(i));
            })
    };
    let text_scale_select = {
        let s = shared.clone();
        Select::new("text-scale")
            .options(AppTextScale::ALL.map(AppTextScale::label))
            .selected(text_scale_to_index(text_scale))
            .theme_when(clone_reader(reader))
            .on_change(move |index, _| {
                if let Some(scale) = AppTextScale::ALL.get(index).copied() {
                    s.set_text_scale(scale);
                }
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

    let scale_body = FieldGroup::new()
        .spacing(8.0)
        .with_child(
            FormRow::new("Text size", text_scale_select)
                .inline()
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new(
                "Preview",
                muted_label(shared, format!("{} text", text_scale.label())),
            )
            .stacked()
            .theme_when(clone_reader(reader)),
        );
    let scale_section = FormSection::new("Typography", scale_body)
        .description(
            "Text size changes typography without inflating interface controls or spacing.",
        )
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
        .with_child(scale_section)
        .with_child(render_section)
}

fn section_connection(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared
        .signals()
        .settings_structure
        .select_named("settings connection structure", |key| {
            key.0.as_deref().map(|snapshot| snapshot.connection.clone())
        });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_key| {
        WidgetPod::new(section_connection_body(&s, &r))
    })
}

fn section_connection_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
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
        settings_label(shared, "Needs attention", shared.theme().text.sm)
            .color_when(move || palette(&s).warn)
    };
    let summary = muted_meta_label(shared, connection_issue_summary(&issue));
    let recovery = muted_meta_label(shared, connection_issue_recovery_hint(&issue));
    let detail_label = {
        let s = shared.clone();
        settings_label(shared, "Low-level detail", shared.theme().text.sm)
            .color_when(move || palette(&s).text_faint)
    };
    let detail_field = {
        let name = match tab {
            Tab::Sinomo => "connection-chat-detail",
            Tab::Files => "connection-files-detail",
            _ => "connection-issue-detail",
        };
        SizedBox::new()
            .height(68.0)
            .with_child(selectable_read_only_area(reader, name, detail))
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
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press(move || s.settings_mut().toggle_connection_detail(tab))
    };
    let header = Stack::vertical()
        .spacing(3.0)
        .alignment(Alignment::Stretch)
        .with_child(status)
        .with_child(summary);
    let mut controls = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Start)
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
    let source = shared
        .signals()
        .settings_structure
        .select_named("settings node structure", |key| {
            key.0.as_deref().map(|snapshot| snapshot.node.clone())
        });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_sig| {
        WidgetPod::new(section_node_body(&s, &r))
    })
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
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
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

struct ClusterJoinPreview {
    target: String,
    expires_at: String,
    issuer: String,
}

fn cluster_join_preview(raw: &str) -> Result<ClusterJoinPreview, String> {
    let invite = sinomo_runtime::cluster::extract_invite_from_join_command(raw.trim());
    if invite.is_empty() {
        return Err("enter an invitation link before joining".to_string());
    }
    let invite = sinomo_runtime::cluster::decode_cluster_join_invite(invite)
        .map_err(|error| format!("invalid invitation: {error}"))?;
    let cluster_id = invite.cluster_id.to_string();
    let target = invite
        .cluster_name
        .filter(|name| !name.trim().is_empty())
        .map(|name| format!("{} · {}", name, short(&cluster_id, 18)))
        .unwrap_or_else(|| short(&cluster_id, 30));
    Ok(ClusterJoinPreview {
        target,
        expires_at: invite.expires_at.to_string(),
        issuer: short(&invite.issuer_node_id.to_string(), 24),
    })
}

fn section_cluster(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared
        .signals()
        .settings_structure
        .select_named("settings cluster structure", |key| {
            key.0.as_deref().map(|snapshot| snapshot.cluster.clone())
        });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_key| {
        WidgetPod::new(section_cluster_body(&s, &r))
    })
}

fn section_cluster_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (clustered, cluster_summary, invite_result, leave_confirming) = {
        let settings = shared.settings();
        (
            settings.clustered,
            settings
                .cluster_summary
                .clone()
                .unwrap_or_else(|| "connected".to_string()),
            settings.invite_result.clone(),
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
                .enabled_when({
                    let s = s.clone();
                    move || {
                        !s.settings()
                            .domain_pending(SettingsOperationDomain::Cluster)
                    }
                })
                .on_press(move || {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Cluster,
                        SettingsOperation::CreateClusterInvite,
                    );
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
                    .theme_when(clone_reader(reader))
                    .appearance(sui::ButtonAppearance::Filled)
                    .tone(SemanticTone::Danger)
                    .enabled_when({
                        let s = s.clone();
                        move || {
                            !s.settings()
                                .domain_pending(SettingsOperationDomain::Cluster)
                        }
                    })
                    .on_press(move || {
                        queue_settings_operation(
                            &s,
                            SettingsOperationScope::Cluster,
                            SettingsOperation::LeaveCluster,
                        );
                    })
            };
            let cancel = {
                let s = shared.clone();
                sui::Button::new("Cancel")
                    .icon(sui::IconGlyph::Close)
                    .theme_when(clone_reader(reader))
                    .appearance(sui::ButtonAppearance::Tonal)
                    .tone(SemanticTone::Neutral)
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
                    Flex::horizontal()
                        .wrap(FlexWrap::Wrap)
                        .gap(8.0)
                        .align_items(Alignment::Start)
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
                    .theme_when(clone_reader(reader))
                    .appearance(sui::ButtonAppearance::Filled)
                    .tone(SemanticTone::Danger)
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
    }

    body = body.with_child(cluster_join_controls(shared, reader, clustered));
    body = body.with_child(feedback_slot(
        shared,
        reader,
        |settings| settings.cluster_action_error.clone(),
        |settings| settings.cluster_action_status.clone(),
    ));

    FormSection::new("Cluster", body).theme_when(clone_reader(reader))
}

fn cluster_join_controls(shared: &Shared, reader: &ThemeReader, clustered: bool) -> FormRow {
    let (join_value, confirming, source_cluster) = {
        let settings = shared.settings();
        (
            settings.join_invite.clone(),
            settings.join_confirming,
            settings
                .cluster_id
                .as_deref()
                .map(|id| short(id, 24))
                .unwrap_or_else(|| "local bootstrap state".to_string()),
        )
    };
    let input = TextInput::new("cluster-join-link")
        .value(join_value.clone())
        .placeholder("sinomo://cluster/join/…")
        .theme_when(clone_reader(reader))
        .on_change({
            let s = shared.clone();
            move |value| s.settings_mut().set_join_invite(value)
        });
    let mut controls = Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(input);

    if confirming {
        if let Ok(preview) = cluster_join_preview(&join_value) {
            controls = controls
                .with_child(value_row(shared, reader, "Target cluster", preview.target))
                .with_child(value_row(
                    shared,
                    reader,
                    "Invitation expires",
                    preview.expires_at,
                ))
                .with_child(value_row(shared, reader, "Issued by", preview.issuer));
        }
        let impact = if clustered {
            format!(
                "This device will leave {source_cluster}, retain its node identity, quarantine the source cluster state, join the target, and import node-local configuration. Cluster-scoped data and membership do not move."
            )
        } else {
            "This device will retain its node identity and join the target cluster. Local node configuration is preserved; cluster-scoped data remains owned by its current cluster."
                .to_string()
        };
        controls = controls
            .with_child(muted_meta_label(shared, impact))
            .with_child(
                muted_meta_label(
                    shared,
                    "Recovery: if the target join fails, Sinomo restores the source cluster automatically. After an interrupted migration, restart the daemon and run `sinomo cluster status` before retrying.",
                ),
            );
        let confirm = {
            let s = shared.clone();
            sui::Button::new(if clustered {
                "Confirm migration"
            } else {
                "Confirm join"
            })
            .icon(sui::IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Filled)
            .tone(SemanticTone::Danger)
            .enabled_when({
                let s = s.clone();
                move || {
                    !s.settings()
                        .domain_pending(SettingsOperationDomain::Cluster)
                }
            })
            .on_press(move || {
                let invite = s.settings().join_invite.clone();
                if cluster_join_preview(&invite).is_err() {
                    s.settings_mut()
                        .set_cluster_action_error("the invitation changed; review it again");
                    return;
                }
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Cluster,
                    SettingsOperation::JoinCluster { invite },
                );
            })
        };
        let cancel = {
            let s = shared.clone();
            sui::Button::new("Cancel")
                .icon(sui::IconGlyph::Close)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press(move || s.settings_mut().cancel_join_cluster())
        };
        controls = controls.with_child(
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .align_items(Alignment::Start)
                .with_child(confirm)
                .with_child(cancel),
        );
    } else {
        let review = {
            let s = shared.clone();
            sui::Button::new(if clustered {
                "Review migration"
            } else {
                "Review join"
            })
            .icon(sui::IconGlyph::ChevronRight)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    !s.settings().join_invite.trim().is_empty()
                        && !s
                            .settings()
                            .domain_pending(SettingsOperationDomain::Cluster)
                }
            })
            .on_press(move || {
                s.settings_mut().confirm_join_cluster();
            })
        };
        controls = controls
            .with_child(muted_label(
                shared,
                if clustered {
                    "Join another cluster without manually leaving first. You will review the target and migration impact before anything changes."
                } else {
                    "Review the target and impact before joining."
                },
            ))
            .with_child(Stack::horizontal().with_child(review));
    }

    FormRow::new(
        if clustered {
            "Join another cluster"
        } else {
            "Join cluster"
        },
        controls,
    )
    .stacked()
    .theme_when(clone_reader(reader))
}

fn cluster_dashboard_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    sui::Button::new("Open Cluster dashboard")
        .icon(sui::IconGlyph::Storage)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
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
            settings.node_id.clone(),
            app_surface_summary(&settings.role),
            settings.cluster_node_id.clone(),
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
        .with_child(copyable_text_row(
            reader,
            "about-app-surface-id",
            "App surface ID",
            app_surface_id,
        ))
        .with_child(value_row(shared, reader, "Surface role", surface_role));
    if let Some(cluster_node_id) = cluster_node_id {
        runtime = runtime.with_child(copyable_text_row(
            reader,
            "about-daemon-node-id",
            "Daemon node ID",
            cluster_node_id,
        ));
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
    let health = shared
        .settings()
        .provider_health(&form.provider, &form.account);
    let expanded = shared
        .settings()
        .provider_detail_expanded(&form.provider, &form.account);

    // Keep the operational state and enable control visible without opening details.
    let enable_switch = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        let enabled = !form.enabled;
        Switch::new("")
            .on(form.enabled)
            .theme_when(clone_reader(reader))
            .on_toggle(move |_| {
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Providers,
                    SettingsOperation::SetProviderEnabled {
                        provider: provider.clone(),
                        account: account.clone(),
                        enabled,
                    },
                );
            })
    };
    let health_presentation = provider_health_presentation(&health);
    let health_badge = StatusBadge::new(health_presentation.label)
        .icon(health_presentation.icon)
        .tone(health_presentation.tone)
        .theme_when(clone_reader(reader));
    let header_actions = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center)
        .with_child(health_badge)
        .with_child(enable_switch);

    let mut body = FieldGroup::new().spacing(8.0);
    body = body.with_child(
        FormRow::new(
            "Authentication",
            Stack::horizontal().with_child(
                StatusBadge::new(provider_auth_summary(&form))
                    .icon(if form.signed_in {
                        IconGlyph::Lock
                    } else {
                        IconGlyph::Unlock
                    })
                    .tone(if form.signed_in {
                        SemanticTone::Info
                    } else {
                        SemanticTone::Warning
                    })
                    .theme_when(clone_reader(reader)),
            ),
        )
        .inline()
        .theme_when(clone_reader(reader)),
    );
    let test_button = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        sui::Button::new("Test connection")
            .icon(IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled(form.signed_in)
            .on_press(move || {
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Providers,
                    SettingsOperation::TestProviderConnection {
                        provider: provider.clone(),
                        account: account.clone(),
                    },
                );
            })
    };
    let test_control = SwitchView::new()
        .selected(usize::from(health.checking))
        .with_child(test_button)
        .with_child(
            Spinner::new(format!(
                "Testing {} {} connection",
                form.display_name, form.account_label
            ))
            .label("Testing connection…")
            .theme_when(clone_reader(reader)),
        );
    body = body.with_child(value_row(
        shared,
        reader,
        "Verification",
        provider_last_verified_label(&health),
    ));
    if let Some(error) = health.error.clone() {
        body = body.with_child(
            FormRow::new("Connection error", muted_label(shared, error))
                .stacked()
                .theme_when(clone_reader(reader)),
        );
    }
    let chat_default_control = if form.is_default {
        Stack::horizontal().alignment(Alignment::Center).with_child(
            StatusBadge::new("Chat default")
                .icon(IconGlyph::Chat)
                .tone(SemanticTone::Accent)
                .theme_when(clone_reader(reader)),
        )
    } else {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        let ready_for_chat = form.enabled && form.signed_in;
        let button = sui::Button::new("Use for chat")
            .icon(sui::IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled(ready_for_chat)
            .on_press(move || {
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Providers,
                    SettingsOperation::SetProviderChatDefault {
                        provider: provider.clone(),
                        account: account.clone(),
                    },
                );
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
    let status_label = {
        let status_shared = shared.clone();
        let color_shared = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        Label::dynamic("No changes yet", move || {
            status_shared
                .settings()
                .provider_action_status
                .as_ref()
                .filter(|status| status.provider == provider && status.account == account)
                .map(|status| status.message.clone())
                .unwrap_or_else(|| "No changes yet".to_string())
        })
        .color_when(move || palette(&color_shared).good)
    };
    body = body.with_child(
        FormRow::new("Last change", status_label)
            .inline()
            .theme_when(clone_reader(reader)),
    );

    let mut model_options = form.known_models.clone();
    model_options.extend(health.models.iter().cloned());
    let has_model_options = !model_options.is_empty();
    if has_model_options
        && !form.default_model.is_empty()
        && !model_options.contains(&form.default_model)
    {
        model_options.push(form.default_model.clone());
    }
    model_options.sort();
    model_options.dedup();
    if has_model_options {
        let models = model_options;
        let selected_model = form.default_model.clone();
        let model_select = {
            let s = shared.clone();
            let provider = form.provider.clone();
            let account = form.account.clone();
            let options = models.clone();
            Select::new(format!("provider-{index}-model-select"))
                .options(options.clone())
                .selected_when(move || {
                    options
                        .iter()
                        .position(|model| model == &selected_model)
                        .or((!options.is_empty()).then_some(0))
                })
                .theme_when(clone_reader(reader))
                .on_change(move |_index, model| {
                    let draft_key = provider_field_key(&provider, &account, ProviderField::Model);
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Providers,
                        SettingsOperation::SetProviderField {
                            provider: provider.clone(),
                            account: account.clone(),
                            field: ProviderField::Model,
                            raw: model.clone(),
                            draft_key,
                        },
                    );
                })
        };
        body = body.with_child(
            FormRow::new("Model", model_select)
                .inline()
                .theme_when(clone_reader(reader)),
        );
    } else {
        body = body.with_child(provider_model_text_row(shared, reader, index, "Model"));
        if let Some(error) = health.model_discovery_error.clone() {
            body = body.with_child(
                FormRow::new("Model discovery", muted_label(shared, error))
                    .stacked()
                    .theme_when(clone_reader(reader)),
            );
        }
    }

    if let Some(usage) = health.usage.as_ref() {
        body = body.with_child(provider_usage_row(shared, reader, usage));
    } else if let Some(error) = health.usage_error.clone() {
        body = body.with_child(
            FormRow::new("Usage", muted_label(shared, error))
                .stacked()
                .theme_when(clone_reader(reader)),
        );
    }

    let detail_button = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        sui::Button::new(if expanded {
            "Hide settings"
        } else {
            "Settings"
        })
        .icon(if expanded {
            sui::IconGlyph::ChevronUp
        } else {
            sui::IconGlyph::ChevronDown
        })
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press(move || {
            s.settings_mut().toggle_provider_detail(&provider, &account);
        })
    };
    body = body.with_child(
        FormRow::new(
            "Actions",
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(test_control)
                .with_child(detail_button),
        )
        .inline()
        .theme_when(clone_reader(reader)),
    );

    if expanded {
        if has_model_options {
            body = body.with_child(provider_model_text_row(
                shared,
                reader,
                index,
                "Custom model",
            ));
        }
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
                .with_child(status_reflector(shared, index, reader))
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
            let api_key = provider_field_key(&form.provider, &form.account, ProviderField::ApiKey);
            let api_value = shared.settings().field_draft_or(&api_key, String::new);
            let on_change = {
                let s = shared.clone();
                move |value: String| {
                    s.settings_mut()
                        .set_provider_field_draft(index, ProviderField::ApiKey, value);
                }
            };
            body = body.with_child(text_save_row(
                reader,
                &format!("provider-{index}-api-key"),
                "API key",
                api_value,
                placeholder,
                on_change,
                {
                    let s = shared.clone();
                    move || {
                        let operation = s
                            .settings()
                            .provider_field_operation(index, ProviderField::ApiKey);
                        if let Some(operation) = operation {
                            queue_settings_operation(
                                &s,
                                SettingsOperationScope::Providers,
                                operation,
                            );
                        }
                    }
                },
                {
                    let s = shared.clone();
                    move || {
                        s.settings()
                            .domain_pending(SettingsOperationDomain::Providers)
                    }
                },
            ));
        }

        // API base URL.
        let base_url_key =
            provider_field_key(&form.provider, &form.account, ProviderField::BaseUrl);
        let base_url_value = shared
            .settings()
            .field_draft_or(&base_url_key, || form.api_base_url.clone());
        body = body.with_child(text_save_row(
            reader,
            &format!("provider-{index}-base-url"),
            "API base URL",
            base_url_value,
            "Provider default",
            {
                let s = shared.clone();
                move |value: String| {
                    s.settings_mut()
                        .set_provider_field_draft(index, ProviderField::BaseUrl, value);
                }
            },
            {
                let s = shared.clone();
                move || {
                    let operation = s
                        .settings()
                        .provider_field_operation(index, ProviderField::BaseUrl);
                    if let Some(operation) = operation {
                        queue_settings_operation(&s, SettingsOperationScope::Providers, operation);
                    }
                }
            },
            {
                let s = shared.clone();
                move || {
                    s.settings()
                        .domain_pending(SettingsOperationDomain::Providers)
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
                let provider = form.provider.clone();
                let account = form.account.clone();
                move |idx: usize, _label: String| {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Providers,
                        SettingsOperation::SetProviderEffort {
                            provider: provider.clone(),
                            account: account.clone(),
                            effort_index: idx,
                        },
                    );
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
        if provider_supports_reasoning_budget(&form.provider) {
            let budget_key = provider_field_key(
                &form.provider,
                &form.account,
                ProviderField::ReasoningBudget,
            );
            let budget_value = shared.settings().field_draft_or(&budget_key, || {
                form.reasoning_budget_tokens
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            });
            body = body.with_child(text_save_row(
                reader,
                &format!("provider-{index}-reasoning-budget"),
                "Reasoning budget",
                budget_value,
                "Provider default",
                {
                    let s = shared.clone();
                    move |value| {
                        s.settings_mut().set_provider_field_draft(
                            index,
                            ProviderField::ReasoningBudget,
                            value,
                        );
                    }
                },
                {
                    let s = shared.clone();
                    move || {
                        if let Some(operation) = s
                            .settings()
                            .provider_field_operation(index, ProviderField::ReasoningBudget)
                        {
                            queue_settings_operation(
                                &s,
                                SettingsOperationScope::Providers,
                                operation,
                            );
                        }
                    }
                },
                {
                    let s = shared.clone();
                    move || {
                        s.settings()
                            .domain_pending(SettingsOperationDomain::Providers)
                    }
                },
            ));
        }
        if provider_supports_text_verbosity(&form.provider) {
            let selected_when = {
                let s = shared.clone();
                move || {
                    s.settings()
                        .provider_forms
                        .get(index)
                        .map(|form| text_verbosity_index(form.text_verbosity.as_deref()))
                }
            };
            let on_change = {
                let s = shared.clone();
                let provider = form.provider.clone();
                let account = form.account.clone();
                move |verbosity_index, _label| {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Providers,
                        SettingsOperation::SetProviderVerbosity {
                            provider: provider.clone(),
                            account: account.clone(),
                            verbosity_index,
                        },
                    );
                }
            };
            body = body.with_child(select_row(
                reader,
                &format!("provider-{index}-verbosity"),
                "Text verbosity",
                TEXT_VERBOSITY_OPTIONS,
                selected_when,
                on_change,
            ));
        }
        if provider_supports_vendor_options(&form.provider) {
            let options_key =
                provider_field_key(&form.provider, &form.account, ProviderField::VendorOptions);
            let options_value = shared
                .settings()
                .field_draft_or(&options_key, || form.vendor_options.clone());
            body = body.with_child(text_save_row(
                reader,
                &format!("provider-{index}-vendor-options"),
                "Provider options",
                options_value,
                "{}",
                {
                    let s = shared.clone();
                    move |value| {
                        s.settings_mut().set_provider_field_draft(
                            index,
                            ProviderField::VendorOptions,
                            value,
                        );
                    }
                },
                {
                    let s = shared.clone();
                    move || {
                        if let Some(operation) = s
                            .settings()
                            .provider_field_operation(index, ProviderField::VendorOptions)
                        {
                            queue_settings_operation(
                                &s,
                                SettingsOperationScope::Providers,
                                operation,
                            );
                        }
                    }
                },
                {
                    let s = shared.clone();
                    move || {
                        s.settings()
                            .domain_pending(SettingsOperationDomain::Providers)
                    }
                },
            ));
        }
        body = body.with_child(provider_management_row(shared, reader, index, &form));
    }

    FormSection::new(form.display_name.clone(), body)
        .header_action(header_actions)
        .theme_when(clone_reader(reader))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProviderHealthPresentation {
    label: &'static str,
    tone: SemanticTone,
    icon: IconGlyph,
}

fn provider_health_presentation(health: &ProviderHealthState) -> ProviderHealthPresentation {
    if health.checking {
        ProviderHealthPresentation {
            label: "Testing…",
            tone: SemanticTone::Accent,
            icon: IconGlyph::Hourglass,
        }
    } else if health.error.is_some() {
        ProviderHealthPresentation {
            label: "Connection failed",
            tone: SemanticTone::Danger,
            icon: IconGlyph::Alert,
        }
    } else if health.last_verified_at_unix.is_some() && health.stale {
        ProviderHealthPresentation {
            label: "Needs retest",
            tone: SemanticTone::Warning,
            icon: IconGlyph::Alert,
        }
    } else if health.last_verified_at_unix.is_some() {
        ProviderHealthPresentation {
            label: "Verified",
            tone: SemanticTone::Success,
            icon: IconGlyph::Check,
        }
    } else {
        ProviderHealthPresentation {
            label: "Not verified",
            tone: SemanticTone::Neutral,
            icon: IconGlyph::Hourglass,
        }
    }
}

fn provider_model_text_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    label: &str,
) -> FormRow {
    let form = shared.settings().provider_forms[index].clone();
    let model_key = provider_field_key(&form.provider, &form.account, ProviderField::Model);
    let model_value = shared
        .settings()
        .field_draft_or(&model_key, || form.default_model.clone());
    text_save_row(
        reader,
        &format!("provider-{index}-model"),
        label,
        model_value,
        "Provider default",
        {
            let s = shared.clone();
            move |value: String| {
                s.settings_mut()
                    .set_provider_field_draft(index, ProviderField::Model, value);
            }
        },
        {
            let s = shared.clone();
            move || {
                if let Some(operation) = s
                    .settings()
                    .provider_field_operation(index, ProviderField::Model)
                {
                    queue_settings_operation(&s, SettingsOperationScope::Providers, operation);
                }
            }
        },
        {
            let s = shared.clone();
            move || {
                s.settings()
                    .domain_pending(SettingsOperationDomain::Providers)
            }
        },
    )
}

fn provider_auth_summary(form: &ProviderForm) -> &'static str {
    if form.uses_device_login {
        if form.signed_in {
            "Credential stored"
        } else {
            "Sign-in required"
        }
    } else if form.has_api_key {
        "API key stored"
    } else {
        "API key required"
    }
}

fn provider_last_verified_label(health: &ProviderHealthState) -> String {
    let Some(timestamp) = health.last_verified_at_unix else {
        return "Never".to_string();
    };
    let value = format_relative_age(timestamp, current_settings_unix_timestamp());
    if health.stale {
        format!("{value} · configuration changed")
    } else {
        value
    }
}

fn format_relative_age(timestamp: i64, now: i64) -> String {
    if timestamp >= now {
        return "Just now".to_string();
    }
    format!("{} ago", format_compact_duration((now - timestamp) as u64))
}

fn format_compact_duration(seconds: u64) -> String {
    if seconds < 60 {
        return "less than a minute".to_string();
    }
    if seconds < 3_600 {
        return format!("{}m", seconds / 60);
    }
    if seconds < 86_400 {
        let hours = seconds / 3_600;
        let minutes = (seconds % 3_600) / 60;
        return if minutes == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {minutes}m")
        };
    }
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    if hours == 0 {
        format!("{days}d")
    } else {
        format!("{days}d {hours}h")
    }
}

fn current_settings_unix_timestamp() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

fn provider_supports_reasoning_budget(provider: &str) -> bool {
    provider == "anthropic"
}

fn provider_supports_text_verbosity(provider: &str) -> bool {
    matches!(provider, "openai" | "codex")
}

fn provider_supports_vendor_options(provider: &str) -> bool {
    matches!(provider, "anthropic" | "openai" | "codex")
}

fn provider_usage_row(
    shared: &Shared,
    reader: &ThemeReader,
    usage: &sinomo_lm::LanguageModelProviderUsage,
) -> FormRow {
    let mut stack = Stack::vertical().spacing(8.0).alignment(Alignment::Stretch);
    let mut metadata = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center);
    if let Some(plan) = usage.plan.as_deref() {
        metadata = metadata.with_child(
            StatusBadge::new(plan)
                .tone(SemanticTone::Info)
                .theme_when(clone_reader(reader)),
        );
    }
    if let Ok(captured_at) = i64::try_from(usage.captured_at_unix) {
        metadata = metadata.with_child(muted_label(
            shared,
            format!(
                "Updated {}",
                format_relative_age(captured_at, current_settings_unix_timestamp()).to_lowercase()
            ),
        ));
    }
    stack = stack.with_child(metadata);
    if usage.limits.is_empty() {
        stack = stack.with_child(muted_label(shared, "No usage limits reported"));
    } else {
        for limit in &usage.limits {
            stack = stack.with_child(provider_usage_limit_view(shared, reader, limit));
        }
    }
    if let Some(credits) = usage.credits.as_ref()
        && let Some(balance) = credits.balance.as_deref()
    {
        stack = stack.with_child(muted_label(shared, format!("Credits: {balance}")));
    }
    FormRow::new("Usage and limits", stack)
        .stacked()
        .theme_when(clone_reader(reader))
}

fn provider_usage_limit_view(
    shared: &Shared,
    reader: &ThemeReader,
    limit: &sinomo_lm::LanguageModelUsageLimit,
) -> Stack {
    let name = limit.name.as_deref().unwrap_or(&limit.id);
    if limit.unlimited == Some(true) {
        return Stack::horizontal()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(primary_label(shared, name.to_string()))
            .with_child(
                StatusBadge::new("Unlimited")
                    .icon(IconGlyph::Check)
                    .tone(SemanticTone::Success)
                    .theme_when(clone_reader(reader)),
            );
    }

    let Some(used_percent) = provider_usage_used_percent(limit) else {
        return Stack::vertical()
            .spacing(3.0)
            .alignment(Alignment::Stretch)
            .with_child(primary_label(shared, name.to_string()))
            .with_child(muted_label(
                shared,
                provider_usage_limit_detail(limit, current_settings_unix_timestamp()),
            ));
    };
    let tone = provider_usage_tone(limit, used_percent);
    Stack::vertical()
        .spacing(4.0)
        .alignment(Alignment::Stretch)
        .with_child(primary_label(
            shared,
            format!("{name} · {used_percent:.0}% used"),
        ))
        .with_child(
            SizedBox::new().height(8.0).with_child(
                ProgressBar::new(format!("{name} usage"))
                    .range(0.0, 100.0)
                    .value(used_percent)
                    .tone(tone)
                    .height(8.0)
                    .min_width(360.0)
                    .theme_when(clone_reader(reader)),
            ),
        )
        .with_child(muted_label(
            shared,
            provider_usage_limit_detail(limit, current_settings_unix_timestamp()),
        ))
}

fn provider_usage_used_percent(limit: &sinomo_lm::LanguageModelUsageLimit) -> Option<f64> {
    if limit.exhausted == Some(true) {
        return Some(100.0);
    }
    limit
        .used_percent
        .or_else(|| limit.remaining_percent.map(|remaining| 100.0 - remaining))
        .or_else(|| {
            let entitlement = limit.entitlement?;
            let remaining = limit.remaining?;
            (entitlement > 0.0).then_some((entitlement - remaining) / entitlement * 100.0)
        })
        .map(|used| used.clamp(0.0, 100.0))
}

fn provider_usage_tone(
    limit: &sinomo_lm::LanguageModelUsageLimit,
    used_percent: f64,
) -> SemanticTone {
    if limit.exhausted == Some(true) || used_percent >= 90.0 {
        SemanticTone::Danger
    } else if used_percent >= 75.0 {
        SemanticTone::Warning
    } else {
        SemanticTone::Accent
    }
}

fn provider_usage_limit_detail(limit: &sinomo_lm::LanguageModelUsageLimit, now: i64) -> String {
    let mut details = Vec::new();
    if let Some(remaining) = limit.remaining {
        if let Some(entitlement) = limit.entitlement {
            details.push(format!("{remaining:.0} of {entitlement:.0} remaining"));
        } else {
            details.push(format!("{remaining:.0} remaining"));
        }
    } else if let Some(remaining) = limit.remaining_percent {
        details.push(format!("{:.0}% remaining", remaining.clamp(0.0, 100.0)));
    }
    if limit.exhausted == Some(true) {
        details.push("Exhausted".to_string());
    }
    if let Some(reset) = limit.reset_at_unix {
        if reset > now {
            details.push(format!(
                "resets in {}",
                format_compact_duration((reset - now) as u64)
            ));
        } else {
            details.push("reset due".to_string());
        }
    }
    if details.is_empty() {
        "Limit details unavailable".to_string()
    } else {
        details.join(" · ")
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
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press(move || s.settings_mut().cancel_provider_remove())
        };
        let remove = {
            let s = shared.clone();
            let provider = form.provider.clone();
            let account = form.account.clone();
            sui::Button::new("Remove account")
                .icon(sui::IconGlyph::Trash)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Danger)
                .enabled_when({
                    let s = s.clone();
                    move || {
                        !s.settings()
                            .domain_pending(SettingsOperationDomain::Providers)
                    }
                })
                .on_press(move || {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Providers,
                        SettingsOperation::RemoveProvider {
                            provider: provider.clone(),
                            account: account.clone(),
                        },
                    );
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
            stack = stack.with_child(
                settings_label(shared, error, shared.theme().text.sm).color_when({
                    let s = shared.clone();
                    move || palette(&s).accent
                }),
            );
        }
        FormRow::new("Management", stack)
            .stacked()
            .theme_when(clone_reader(reader))
    } else {
        let remove = {
            let s = shared.clone();
            sui::Button::new("Remove account")
                .icon(sui::IconGlyph::Trash)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Danger)
                .on_press(move || s.settings_mut().confirm_provider_remove(index))
        };
        FormRow::new("Management", remove)
            .inline()
            .theme_when(clone_reader(reader))
    }
}

fn section_providers(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared
        .signals()
        .settings_structure
        .select_named("settings providers structure", |key| {
            key.0.as_deref().map(|snapshot| snapshot.providers.clone())
        });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_sig| provider_section_body(&s, &r))
}

fn provider_section_body(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    if shared.settings().provider_add_open {
        WidgetPod::new(provider_add_view(shared, reader))
    } else {
        WidgetPod::new(provider_list_view(shared, reader))
    }
}

#[cfg(test)]
fn provider_section_signature(shared: &Shared) -> String {
    let settings = shared.settings();
    let mut parts = vec![
        format!("add:{}", settings.provider_add_open),
        format!("add-provider:{}", settings.provider_add_provider_index),
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
        let health = settings.provider_health(&form.provider, &form.account);
        format!(
            "{}@{}:{}:{}:{}:{}:{}:{:?}:{:?}:{}:{}:{:?}",
            form.provider,
            form.account,
            form.enabled,
            form.signed_in,
            form.is_default,
            form.default_model,
            form.display_name,
            form.reasoning_budget_tokens,
            form.text_verbosity,
            form.vendor_options,
            health.checking,
            health
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
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
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
    let (selected_index, uses_device_login) = {
        let settings = shared.settings();
        (
            settings.provider_add_provider_index,
            settings.provider_add_uses_device_login(),
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

    body = body.with_child(feedback_slot(
        shared,
        reader,
        |settings| {
            settings
                .provider_add_error
                .clone()
                .or_else(|| settings.provider_action_error.clone())
        },
        |settings| {
            settings
                .provider_action_status
                .as_ref()
                .map(|status| status.message.clone())
        },
    ));

    let cancel = {
        let s = shared.clone();
        sui::Button::new("Cancel")
            .icon(sui::IconGlyph::ChevronLeft)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
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
                move || {
                    s.settings().provider_add_can_submit()
                        && !s
                            .settings()
                            .domain_pending(SettingsOperationDomain::Providers)
                }
            })
            .on_press(move || {
                let operation = {
                    let settings = s.settings();
                    SettingsOperation::AddProvider {
                        provider: settings.provider_add_provider().to_string(),
                        account: settings.provider_add_account.trim().to_string(),
                        api_key: settings.provider_add_api_key.clone(),
                        model: settings.provider_add_model.clone(),
                        base_url: settings.provider_add_base_url.clone(),
                        uses_device_login: settings.provider_add_uses_device_login(),
                    }
                };
                queue_settings_operation(&s, SettingsOperationScope::Providers, operation);
            })
    };
    let actions = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Center)
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

fn section_agents(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    Stack::vertical()
        .spacing(14.0)
        .alignment(Alignment::Stretch)
        .with_child(agent_catalog_section(shared, reader))
        .with_child(agent_package_management_section(shared, reader))
        .with_child(agent_details_host(shared, reader))
}

fn agent_details_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> RebuildOnChange<Option<SettingsAgentsStructure>> {
    let source = shared
        .signals()
        .settings_structure
        .select_named("settings agent details structure", |key| {
            key.0.as_deref().map(|snapshot| snapshot.agents.clone())
        });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_key| WidgetPod::new(agent_details(&s, &r)))
}

#[cfg(test)]
fn agents_section_key(shared: &Shared) -> String {
    let settings = shared.settings();
    let agents = settings
        .agent_snapshot
        .agents
        .iter()
        .map(|agent| format!("{agent:?}:{}", agent_summary_line(agent)))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "selected:{:?};delete:{:?};report:{:?};editor:{:?}\n{agents}",
        settings.agent_selected_id,
        settings.agent_delete_confirming,
        settings.agent_validation_report,
        settings.agent_form.previous_id,
    )
}

fn agent_catalog_section(shared: &Shared, reader: &ThemeReader) -> FormSection {
    let collection = shared.signals().settings_structure.select_named(
        "settings virtual agent collection",
        |key| {
            let Some(snapshot) = key.0.as_deref() else {
                return widgets::VirtualCollectionSnapshot::new(Vec::new(), None);
            };
            let agents = snapshot.agents.agents.clone();
            let selected = snapshot.agents.selected_id.clone();
            let theme = snapshot.agents.theme;
            widgets::VirtualCollectionSnapshot::new(
                agents
                    .into_iter()
                    .map(|agent| (agent.id.clone(), SettingsAgentRow { theme, agent }))
                    .collect(),
                selected,
            )
        },
    );
    let row_shared = shared.clone();
    let action_shared = shared.clone();
    let list = widgets::VirtualCollectionHost::new(
        "Settings agent collection",
        collection,
        move |model, state| {
            VirtualList::new("agent-config-list", model, move |_id, agent| {
                let shared = row_shared.clone();
                RebuildOnChange::new_observable(agent, move |row| {
                    WidgetPod::new(agent_catalog_row(&shared, &row.agent))
                })
            })
            .state(state)
            .estimated_row_height(MENU_ROW_H)
            .theme_when(clone_reader(reader))
            .row_name(|_id, row| {
                let agent = &row.agent;
                if agent.name.trim().is_empty() {
                    agent.id.clone()
                } else {
                    agent.name.clone()
                }
            })
            .row_description(|_id, row| agent_summary_line(&row.agent))
            .on_change(move |selected_id| {
                let index = {
                    let settings = action_shared.settings();
                    settings
                        .agent_snapshot
                        .agents
                        .iter()
                        .position(|agent| agent.id == selected_id)
                };
                if let Some(index) = index {
                    let selected_id = action_shared.settings_mut().prepare_agent_selection(index);
                    if let Some(selected_id) = selected_id {
                        queue_settings_operation(
                            &action_shared,
                            SettingsOperationScope::Agents,
                            SettingsOperation::RefreshAgents {
                                selected_id: Some(selected_id),
                            },
                        );
                    }
                }
            })
        },
    );
    let new_agent = {
        let s = shared.clone();
        sui::Button::new("New agent")
            .icon(sui::IconGlyph::Add)
            .theme_when(clone_reader(reader))
            .on_press(move || s.settings_mut().create_agent())
    };
    let duplicate = {
        let s = shared.clone();
        sui::Button::new("Duplicate")
            .icon(sui::IconGlyph::Restore)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled_when({
                let s = s.clone();
                move || s.settings().agent_snapshot.selected_agent.is_some()
            })
            .on_press(move || s.settings_mut().duplicate_selected_agent())
    };
    let refresh = {
        let s = shared.clone();
        sui::Button::new("Refresh")
            .icon(sui::IconGlyph::Redo)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || {
                let selected_id = s.settings().agent_selected_id.clone();
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Agents,
                    SettingsOperation::RefreshAgents { selected_id },
                );
            })
    };

    let summary_body = FieldGroup::new()
        .spacing(8.0)
        .with_child(agent_catalog_summary_host(shared, reader))
        .with_child(
            FormRow::new("Agents", SizedBox::new().height(230.0).with_child(list))
                .stacked()
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new(
                "Actions",
                Flex::horizontal()
                    .wrap(FlexWrap::Wrap)
                    .gap(8.0)
                    .align_items(Alignment::Start)
                    .with_child(new_agent)
                    .with_child(duplicate)
                    .with_child(refresh),
            )
            .inline()
            .theme_when(clone_reader(reader)),
        )
        .with_child(feedback_slot(
            shared,
            reader,
            |settings| settings.agent_action_error.clone(),
            |settings| settings.agent_action_status.clone(),
        ));

    FormSection::new("Agents", summary_body)
        .description("Create, inspect, validate, and move agent packages without leaving Settings.")
        .theme_when(clone_reader(reader))
}

fn agent_catalog_summary_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> RebuildOnChange<Option<(Vec<AgentConfigSummary>, Option<String>)>> {
    let source =
        shared
            .signals()
            .settings_structure
            .select_named("settings agent catalog summary", |key| {
                key.0.as_deref().map(|snapshot| {
                    (
                        snapshot.agents.agents.clone(),
                        snapshot.agents.error.clone(),
                    )
                })
            });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |snapshot| {
        let (agents, error) = snapshot.clone().unwrap_or_default();
        let catalog = AgentConfigSnapshot {
            agents,
            selected_agent: None,
            error,
        };
        let mut rows = FieldGroup::new()
            .spacing(8.0)
            .with_child(value_row(
                &s,
                &r,
                "Catalog",
                agent_sidebar_preview(&catalog),
            ))
            .with_child(value_row(
                &s,
                &r,
                "Custom agents",
                format!("{}", agent_custom_count(&catalog)),
            ));
        if let Some(error) = catalog.error {
            rows = rows.with_child(value_row(&s, &r, "Catalog error", error));
        }
        WidgetPod::new(rows)
    })
}

fn agent_details(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let mut stack = Stack::vertical()
        .spacing(14.0)
        .alignment(Alignment::Stretch);

    let editing_unsaved = shared.settings().agent_form.previous_id.is_none();
    let selected = shared.settings().agent_snapshot.selected_agent.clone();
    let editor_visible =
        editing_unsaved || selected.as_ref().is_some_and(agent_definition_is_editable);
    if editor_visible {
        stack = stack.with_child(agent_editor_section(shared, reader));
    } else if let Some(definition) = selected {
        stack = stack.with_child(agent_readonly_section(shared, reader, definition));
    }
    if let Some(report) = shared.settings().agent_validation_report.clone() {
        stack = stack.with_child(agent_validation_report_section(shared, reader, report));
    }
    stack
}

fn agent_package_management_section(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (import_path, export_path) = {
        let settings = shared.settings();
        (
            settings.agent_import_path.clone(),
            settings.agent_export_path.clone(),
        )
    };
    let import_input = {
        let s = shared.clone();
        TextInput::new("agent-import-path")
            .value(import_path)
            .placeholder("Path to an agent JSON package")
            .theme_when(clone_reader(reader))
            .on_change(move |value| s.settings_mut().set_agent_import_path(value))
    };
    let import_button = {
        let s = shared.clone();
        sui::Button::new("Import package")
            .icon(IconGlyph::Download)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    !s.settings().agent_import_path.trim().is_empty()
                        && !s.settings().domain_pending(SettingsOperationDomain::Agents)
                }
            })
            .on_press(move || {
                let path = s.settings().agent_import_path.trim().to_string();
                if !path.is_empty() {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Agents,
                        SettingsOperation::ImportAgent {
                            path: PathBuf::from(path),
                        },
                    );
                }
            })
    };
    let export_input = {
        let s = shared.clone();
        TextInput::new("agent-export-path")
            .value(export_path)
            .placeholder("Output file or directory")
            .theme_when(clone_reader(reader))
            .on_change(move |value| s.settings_mut().set_agent_export_path(value))
    };
    let export_button = {
        let s = shared.clone();
        sui::Button::new("Export selected")
            .icon(IconGlyph::Storage)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled_when({
                let s = s.clone();
                move || {
                    s.settings().agent_selected_id.is_some()
                        && !s.settings().agent_export_path.trim().is_empty()
                        && !s.settings().domain_pending(SettingsOperationDomain::Agents)
                }
            })
            .on_press(move || {
                let settings = s.settings();
                let request = settings.agent_selected_id.clone().zip(
                    (!settings.agent_export_path.trim().is_empty())
                        .then(|| PathBuf::from(settings.agent_export_path.trim())),
                );
                drop(settings);
                if let Some((id, output)) = request {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Agents,
                        SettingsOperation::ExportAgent { id, output },
                    );
                }
            })
    };
    let validate = agent_lifecycle_action_button(shared, reader, false);
    let test = agent_lifecycle_action_button(shared, reader, true);

    let body = FieldGroup::new()
        .spacing(8.0)
        .with_child(
            FormRow::new(
                "Import",
                Stack::vertical()
                    .spacing(8.0)
                    .alignment(Alignment::Stretch)
                    .with_child(import_input)
                    .with_child(
                        Stack::horizontal()
                            .alignment(Alignment::Start)
                            .with_child(import_button),
                    ),
            )
            .stacked()
            .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new(
                "Export",
                Stack::vertical()
                    .spacing(8.0)
                    .alignment(Alignment::Stretch)
                    .with_child(export_input)
                    .with_child(
                        Stack::horizontal()
                            .alignment(Alignment::Start)
                            .with_child(export_button),
                    ),
            )
            .stacked()
            .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new(
                "Checks",
                Flex::horizontal()
                    .wrap(FlexWrap::Wrap)
                    .gap(8.0)
                    .align_items(Alignment::Start)
                    .with_child(validate)
                    .with_child(test),
            )
            .inline()
            .theme_when(clone_reader(reader)),
        );
    FormSection::new("Package management", body)
        .description("Validation checks package structure. Test also verifies configured dependencies without making a model request.")
        .theme_when(clone_reader(reader))
}

fn agent_lifecycle_action_button(shared: &Shared, reader: &ThemeReader, test: bool) -> sui::Button {
    let s = shared.clone();
    let label = if test { "Test readiness" } else { "Validate" };
    sui::Button::new(label)
        .icon(if test {
            IconGlyph::Redo
        } else {
            IconGlyph::Check
        })
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .enabled_when({
            let s = s.clone();
            move || {
                s.settings().agent_selected_id.is_some()
                    && !s.settings().domain_pending(SettingsOperationDomain::Agents)
            }
        })
        .on_press(move || {
            if let Some(id) = s.settings().agent_selected_id.clone() {
                let operation = if test {
                    SettingsOperation::TestAgent { id }
                } else {
                    SettingsOperation::ValidateAgent { id }
                };
                queue_settings_operation(&s, SettingsOperationScope::Agents, operation);
            }
        })
}

fn agent_editor_section(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let form = shared.settings().agent_form.clone();
    let role_select = {
        let s = shared.clone();
        Select::new("agent-role")
            .options(AGENT_ROLE_OPTIONS.iter().copied())
            .selected_when({
                let s = s.clone();
                move || Some(s.settings().agent_form.role_index)
            })
            .theme_when(clone_reader(reader))
            .on_change(move |index, _label| s.settings_mut().set_agent_role_index(index))
    };
    let output_format_select = {
        let s = shared.clone();
        Select::new("agent-output-format")
            .options(AGENT_OUTPUT_FORMAT_OPTIONS.iter().copied())
            .selected_when({
                let s = s.clone();
                move || Some(s.settings().agent_form.output_format_index)
            })
            .theme_when(clone_reader(reader))
            .on_change(move |index, _label| s.settings_mut().set_agent_output_format_index(index))
    };

    let mut body = FieldGroup::new()
        .spacing(8.0)
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-id",
            "Agent ID",
            form.id.clone(),
            "custom-agent",
            |settings, value| settings.set_agent_id(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-name",
            "Display name",
            form.name.clone(),
            "Custom agent",
            |settings, value| settings.set_agent_name(value),
        ))
        .with_child(
            FormRow::new("Role", role_select)
                .inline()
                .theme_when(clone_reader(reader)),
        )
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-description",
            "Description",
            form.description.clone(),
            "What this agent is for",
            |settings, value| settings.set_agent_description(value),
        ))
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-system-prompt",
            "System prompt",
            form.system_prompt.clone(),
            "Describe the agent behavior",
            140.0,
            |settings, value| settings.set_agent_system_prompt(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-provider",
            "Provider",
            form.provider.clone(),
            "Cluster default",
            |settings, value| settings.set_agent_provider(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-model",
            "Model",
            form.model.clone(),
            "Provider default",
            |settings, value| settings.set_agent_model(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-temperature",
            "Temperature",
            form.temperature.clone(),
            "0.2",
            |settings, value| settings.set_agent_temperature(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-max-output",
            "Max output tokens",
            form.max_output_tokens.clone(),
            "Provider default",
            |settings, value| settings.set_agent_max_output_tokens(value),
        ))
        .with_child(
            FormRow::new("Output", output_format_select)
                .inline()
                .theme_when(clone_reader(reader)),
        )
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-max-steps",
            "Max steps",
            form.max_steps.clone(),
            "Runtime default",
            |settings, value| settings.set_agent_max_steps(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-max-tool-calls",
            "Max tool calls",
            form.max_tool_calls_per_step.clone(),
            "Runtime default",
            |settings, value| settings.set_agent_max_tool_calls_per_step(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-max-repairs",
            "Repair attempts",
            form.max_model_repair_attempts.clone(),
            "Runtime default",
            |settings, value| settings.set_agent_max_model_repair_attempts(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-sandbox-node",
            "Sandbox node",
            form.sandbox_node_id.clone(),
            "Local runtime",
            |settings, value| settings.set_agent_sandbox_node_id(value),
        ))
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-skills",
            "Skills",
            form.skills_text.clone(),
            "skill-id per line",
            74.0,
            |settings, value| settings.set_agent_skills_text(value),
        ))
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-visible-toolsets",
            "Visible toolsets",
            form.visible_toolsets_text.clone(),
            "toolset id per line",
            70.0,
            |settings, value| settings.set_agent_visible_toolsets_text(value),
        ))
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-allow-tools",
            "Allowed tools",
            form.allow_tools_text.clone(),
            "tool id per line",
            70.0,
            |settings, value| settings.set_agent_allow_tools_text(value),
        ))
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-deny-tools",
            "Denied tools",
            form.deny_tools_text.clone(),
            "tool id per line",
            70.0,
            |settings, value| settings.set_agent_deny_tools_text(value),
        ));

    if let Some(summary) = selected_agent_config_summary(shared) {
        body = body
            .with_child(agent_readiness_row(shared, reader, summary.readiness))
            .with_child(value_row(
                shared,
                reader,
                "Package",
                format!("{} · version {}", summary.source, summary.package_version),
            ))
            .with_child(value_row(shared, reader, "Provenance", summary.provenance));
    }
    body = body.with_child(agent_actions_row(shared, reader));
    let title = if form.previous_id.is_some() {
        format!("Edit {}", short_one_line(&form.id, 48))
    } else {
        "New custom agent".to_string()
    };
    FormSection::new(title, body)
        .description("Saved agents are stored under the local Sinomo data directory and become available in chat agent selection.")
        .theme_when(clone_reader(reader))
}

fn agent_actions_row(shared: &Shared, reader: &ThemeReader) -> FormRow {
    let save = {
        let s = shared.clone();
        sui::Button::new("Save agent")
            .icon(sui::IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    s.settings().agent_form_can_save()
                        && !s.settings().domain_pending(SettingsOperationDomain::Agents)
                }
            })
            .on_press(move || {
                let operation = s.settings_mut().agent_save_operation();
                if let Some(operation) = operation {
                    queue_settings_operation(&s, SettingsOperationScope::Agents, operation);
                }
            })
    };
    let duplicate = {
        let s = shared.clone();
        sui::Button::new("Duplicate")
            .icon(sui::IconGlyph::Restore)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || s.settings_mut().duplicate_selected_agent())
    };
    let delete_controls = agent_delete_controls(shared, reader);
    FormRow::new(
        "Actions",
        Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(8.0)
            .align_items(Alignment::Start)
            .with_child(save)
            .with_child(duplicate)
            .with_child(delete_controls),
    )
    .inline()
    .theme_when(clone_reader(reader))
}

fn agent_delete_controls(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (previous_id, confirming) = {
        let settings = shared.settings();
        let previous_id = settings.agent_form.previous_id.clone();
        let confirming = previous_id
            .as_ref()
            .is_some_and(|id| settings.agent_delete_confirming.as_ref() == Some(id));
        (previous_id, confirming)
    };
    if confirming {
        let confirm = {
            let s = shared.clone();
            sui::Button::new("Confirm delete")
                .icon(sui::IconGlyph::Trash)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Danger)
                .enabled_when({
                    let s = s.clone();
                    move || !s.settings().domain_pending(SettingsOperationDomain::Agents)
                })
                .on_press(move || {
                    let id = s.settings().agent_selected_id.clone();
                    if let Some(id) = id {
                        queue_settings_operation(
                            &s,
                            SettingsOperationScope::Agents,
                            SettingsOperation::DeleteAgent { id },
                        );
                    } else {
                        s.settings_mut().agent_action_error =
                            Some("select a custom agent first".to_string());
                    }
                })
        };
        let cancel = {
            let s = shared.clone();
            sui::Button::new("Cancel")
                .icon(sui::IconGlyph::Close)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press(move || s.settings_mut().cancel_agent_delete())
        };
        return Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(8.0)
            .align_items(Alignment::Start)
            .with_child(confirm)
            .with_child(cancel);
    }
    let s = shared.clone();
    Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Start)
        .with_child(
            sui::Button::new("Delete agent")
                .icon(sui::IconGlyph::Trash)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Danger)
                .enabled_when(move || previous_id.is_some())
                .on_press(move || s.settings_mut().confirm_agent_delete()),
        )
}

fn agent_readonly_section(
    shared: &Shared,
    reader: &ThemeReader,
    definition: AgentDefinition,
) -> impl Widget + use<> {
    let duplicate = {
        let s = shared.clone();
        sui::Button::new("Duplicate")
            .icon(sui::IconGlyph::Restore)
            .theme_when(clone_reader(reader))
            .on_press(move || s.settings_mut().duplicate_selected_agent())
    };
    let mut body = FieldGroup::new()
        .spacing(8.0)
        .with_child(agent_readiness_row(
            shared,
            reader,
            selected_agent_config_summary(shared)
                .map(|summary| summary.readiness)
                .unwrap_or(AgentReadiness::NeedsAttention),
        ))
        .with_child(copyable_text_row(
            reader,
            "agent-readonly-id",
            "Agent ID",
            definition.key(),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Package version",
            definition.package.version.clone(),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Source",
            agent_source_label(&definition.package.provenance.source),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Provenance",
            selected_agent_config_summary(shared)
                .map(|summary| summary.provenance)
                .unwrap_or_else(|| "Package provenance unavailable".to_string()),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Role",
            agent_role_label(&definition.role),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Description",
            if definition.description.trim().is_empty() {
                "No description".to_string()
            } else {
                definition.description.clone()
            },
        ))
        .with_child(value_row(
            shared,
            reader,
            "Tools",
            agent_tools_summary(&definition),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Model policy",
            agent_model_summary(&definition),
        ))
        .with_child(copyable_area_row(
            reader,
            "agent-readonly-system-prompt",
            "System prompt",
            definition.system_prompt.clone(),
        ))
        .with_child(
            FormRow::new("Actions", duplicate)
                .inline()
                .theme_when(clone_reader(reader)),
        );
    if let Some(node_id) = definition.tool_runtime.sandbox_node_id.as_ref() {
        body = body.with_child(value_row(shared, reader, "Sandbox node", node_id.clone()));
    }
    FormSection::new(definition.name.clone(), body)
        .description("This agent is supplied by the runtime. Duplicate it to create an editable custom agent.")
        .theme_when(clone_reader(reader))
}

fn selected_agent_config_summary(shared: &Shared) -> Option<AgentConfigSummary> {
    let settings = shared.settings();
    let id = settings.agent_selected_id.as_deref()?;
    settings
        .agent_snapshot
        .agents
        .iter()
        .find(|agent| agent.id == id)
        .cloned()
}

fn agent_readiness_row(
    shared: &Shared,
    reader: &ThemeReader,
    readiness: AgentReadiness,
) -> FormRow {
    let (label, detail, tone, icon) = match readiness {
        AgentReadiness::Ready => (
            "Ready",
            "The package passes its structural validation.",
            SemanticTone::Success,
            IconGlyph::Check,
        ),
        AgentReadiness::NeedsAttention => (
            "Needs attention",
            "Run validation to review package problems.",
            SemanticTone::Warning,
            IconGlyph::Alert,
        ),
    };
    FormRow::new(
        "Readiness",
        Stack::vertical()
            .spacing(4.0)
            .alignment(Alignment::Start)
            .with_child(
                Stack::horizontal().with_child(
                    StatusBadge::new(label)
                        .tone(tone)
                        .icon(icon)
                        .theme_when(clone_reader(reader)),
                ),
            )
            .with_child(muted_label(shared, detail)),
    )
    .stacked()
    .theme_when(clone_reader(reader))
}

fn agent_validation_report_section(
    shared: &Shared,
    reader: &ThemeReader,
    report: AgentValidationReport,
) -> impl Widget + use<> {
    let ready = report.is_ready();
    let title = format!("Validation report · {}", report.agent_id);
    let summary = report.summary();
    let mut body = FieldGroup::new().spacing(8.0).with_child(
        FormRow::new(
            "Result",
            Stack::horizontal().with_child(
                StatusBadge::new(if ready { "Ready" } else { "Needs attention" })
                    .icon(if ready {
                        IconGlyph::Check
                    } else {
                        IconGlyph::Alert
                    })
                    .tone(if ready {
                        SemanticTone::Success
                    } else {
                        SemanticTone::Warning
                    })
                    .theme_when(clone_reader(reader)),
            ),
        )
        .inline()
        .theme_when(clone_reader(reader)),
    );
    for check in report.checks {
        body = body.with_child(
            FormRow::new(
                check.name,
                Stack::vertical()
                    .spacing(4.0)
                    .alignment(Alignment::Start)
                    .with_child(
                        Stack::horizontal().with_child(
                            StatusBadge::new(if check.passed { "Passed" } else { "Review" })
                                .icon(if check.passed {
                                    IconGlyph::Check
                                } else {
                                    IconGlyph::Alert
                                })
                                .tone(if check.passed {
                                    SemanticTone::Success
                                } else {
                                    SemanticTone::Warning
                                })
                                .theme_when(clone_reader(reader)),
                        ),
                    )
                    .with_child(muted_label(shared, check.detail)),
            )
            .stacked()
            .theme_when(clone_reader(reader)),
        );
    }
    FormSection::new(title, body)
        .description(summary)
        .theme_when(clone_reader(reader))
}

fn agent_text_row<F>(
    shared: &Shared,
    reader: &ThemeReader,
    name: &str,
    label: &str,
    value: impl Into<String>,
    placeholder: &str,
    mut on_change: F,
) -> FormRow
where
    F: FnMut(&mut SettingsState, String) + 'static,
{
    let s = shared.clone();
    let input = TextInput::new(name)
        .value(value)
        .placeholder(placeholder)
        .theme_when(clone_reader(reader))
        .on_change(move |value| {
            let mut settings = s.settings_mut();
            on_change(&mut *settings, value);
        });
    FormRow::new(label, input)
        .inline()
        .theme_when(clone_reader(reader))
}

fn agent_area_row<F>(
    shared: &Shared,
    reader: &ThemeReader,
    name: &str,
    label: &str,
    value: impl Into<String>,
    placeholder: &str,
    height: f32,
    mut on_change: F,
) -> FormRow
where
    F: FnMut(&mut SettingsState, String) + 'static,
{
    let s = shared.clone();
    let area = SizedBox::new().height(height).with_child(
        TextArea::new(name)
            .value(value)
            .placeholder(placeholder)
            .theme_when(clone_reader(reader))
            .on_change(move |value| {
                let mut settings = s.settings_mut();
                on_change(&mut *settings, value);
            }),
    );
    FormRow::new(label, area)
        .stacked()
        .theme_when(clone_reader(reader))
}

fn agent_tools_summary(definition: &AgentDefinition) -> String {
    let tools = definition.tools.len();
    let visible = definition.tool_surface.visible_toolsets.len();
    let allow = definition.tool_surface.allow_tools.len();
    let deny = definition.tool_surface.deny_tools.len();
    let skills = definition.skills.len();
    let mut parts = Vec::new();
    parts.push(match tools {
        0 => "no packaged tools".to_string(),
        1 => "1 packaged tool".to_string(),
        _ => format!("{tools} packaged tools"),
    });
    if visible > 0 {
        parts.push(format!("{visible} visible toolsets"));
    }
    if allow > 0 {
        parts.push(format!("{allow} allowed tools"));
    }
    if deny > 0 {
        parts.push(format!("{deny} denied tools"));
    }
    if skills > 0 {
        parts.push(format!("{skills} skills"));
    }
    parts.join(" · ")
}

fn agent_model_summary(definition: &AgentDefinition) -> String {
    let provider = definition
        .model_policy
        .provider
        .as_deref()
        .unwrap_or("cluster default");
    let model = definition
        .model_policy
        .model
        .as_deref()
        .unwrap_or("provider default");
    let temperature = definition
        .model_policy
        .temperature
        .or(definition.temperature)
        .map(|value| format!("temperature {value}"))
        .unwrap_or_else(|| "default temperature".to_string());
    format!("{provider} / {model} · {temperature}")
}

const MODULE_OVERRIDE_OPTIONS: &[&str] = &["Inherit", "Enable", "Disable"];

fn section_modules(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared
        .signals()
        .settings_structure
        .select_named("settings modules structure", |key| {
            key.0.as_deref().map(|snapshot| snapshot.modules.clone())
        });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_key| {
        WidgetPod::new(section_modules_body(&s, &r))
    })
}

#[cfg(test)]
#[allow(dead_code)]
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
                "{}:{}:{}:{}:{:?}:{:?}:{}:{:?}:{}",
                module.module_id,
                module.label,
                module.backend,
                module.detail,
                module.desired_enabled,
                module.effective_source,
                module.effective_enabled,
                module.observed,
                module.observed_detail
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
    let module_settings = settings
        .config_items
        .iter()
        .filter(|item| item.module_id.is_some())
        .map(config_item_rebuild_signature)
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "selected:{:?};error:{:?};status:{:?};warnings:{:?};config-revision:{}\n{nodes}\n{modules}\n{cluster}\n{selected}\n{module_settings}",
        settings.module_selected_node_id,
        settings.module_action_error,
        settings.module_action_status,
        snapshot.warnings,
        settings.config_ui_revision,
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
                let selected = s
                    .settings()
                    .module_snapshot
                    .nodes
                    .get(index)
                    .map(|node| node.node_id.clone());
                if let Some(selected_node_id) = selected {
                    {
                        let mut settings = s.settings_mut();
                        settings.module_selected_node_id = Some(selected_node_id.clone());
                        settings.mark_draft_changed(SettingsOperationDomain::Modules);
                    }
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Modules,
                        SettingsOperation::RefreshModules {
                            selected_node_id: Some(selected_node_id),
                        },
                    );
                }
            })
    };
    let refresh = {
        let s = shared.clone();
        sui::Button::new("Refresh")
            .icon(sui::IconGlyph::Redo)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || {
                let selected_node_id = s.settings().module_selected_node_id.clone();
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Modules,
                    SettingsOperation::RefreshModules { selected_node_id },
                );
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
                Flex::horizontal()
                    .wrap(FlexWrap::Wrap)
                    .gap(8.0)
                    .align_items(Alignment::Start)
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
    let module_config_catalog = {
        let settings = shared.settings();
        settings
            .config_items
            .iter()
            .enumerate()
            .filter(|(_, item)| item.module_id.is_some())
            .map(|(index, item)| (index, item.clone()))
            .collect::<Vec<_>>()
    };
    let mut rendered_config_indices = HashSet::new();
    if snapshot.modules.is_empty() {
        let empty = FieldGroup::new().spacing(8.0).with_child(value_row(
            shared,
            reader,
            "Status",
            "No installed or configured modules were found",
        ));
        stack =
            stack.with_child(FormSection::new("Catalog", empty).theme_when(clone_reader(reader)));
    }
    for module in snapshot.modules {
        let mut associated = Vec::new();
        for (index, item) in &module_config_catalog {
            if !rendered_config_indices.contains(index)
                && item.module_id.as_deref().is_some_and(|config_module_id| {
                    module_ids_match(config_module_id, &module.module_id)
                })
            {
                rendered_config_indices.insert(*index);
                associated.push((*index, item.clone()));
            }
        }
        stack = stack.with_child(module_config_section(shared, reader, module, associated));
    }
    let unmatched = module_config_catalog
        .into_iter()
        .filter(|(index, _)| !rendered_config_indices.contains(index))
        .collect::<Vec<_>>();
    if !unmatched.is_empty() {
        let mut body = FieldGroup::new().spacing(8.0);
        for (index, item) in unmatched {
            body = body.with_child(PodHost::new(module_config_editor_row(
                shared, reader, index, &item,
            )));
        }
        stack = stack.with_child(
            FormSection::new("Additional module settings", body)
                .description(
                    "Settings declared by modules that do not currently have a lifecycle card on this node.",
                )
                .theme_when(clone_reader(reader)),
        );
    }
    stack
}

fn module_config_section(
    shared: &Shared,
    reader: &ThemeReader,
    module: crate::backend::modules::ModuleConfigModuleSummary,
    module_config_items: Vec<(usize, ConfigItem)>,
) -> impl Widget + use<> {
    let module_id = module.module_id.clone();
    let selected_node_label = {
        let settings = shared.settings();
        settings
            .module_snapshot
            .selected_node
            .as_ref()
            .map(|node| node.summary.display_name.clone())
            .unwrap_or_else(|| "No node selected".to_string())
    };
    let cluster_enabled = module.desired_enabled.unwrap_or(false);

    let cluster_switch = switch_row(reader, "Cluster default", cluster_enabled, {
        let s = shared.clone();
        let module_id = module_id.clone();
        move |enabled| {
            let selected_node_id = s.settings().module_selected_node_id.clone();
            queue_settings_operation(
                &s,
                SettingsOperationScope::Modules,
                SettingsOperation::SetClusterModuleDefault {
                    selected_node_id,
                    module_id: module_id.clone(),
                    enabled,
                },
            );
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
                let node_id = s.settings().module_selected_node_id.clone();
                if let Some(node_id) = node_id {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Modules,
                        SettingsOperation::SetNodeModuleOverride {
                            node_id,
                            module_id: selected_module_id.clone(),
                            enabled,
                        },
                    );
                } else {
                    s.settings_mut().module_action_error = Some("select a node first".to_string());
                }
            },
        )
    };

    let desired = match module.desired_enabled {
        Some(true) => module_lifecycle_value(
            shared,
            reader,
            "Enabled",
            SemanticTone::Success,
            "Cluster desired state",
        ),
        Some(false) => module_lifecycle_value(
            shared,
            reader,
            "Disabled",
            SemanticTone::Neutral,
            "Cluster desired state",
        ),
        None => module_lifecycle_value(
            shared,
            reader,
            "Not configured",
            SemanticTone::Neutral,
            "No cluster desired state is set",
        ),
    };
    let effective = module_lifecycle_value(
        shared,
        reader,
        if module.effective_enabled {
            "Enabled"
        } else {
            "Disabled"
        },
        if module.effective_enabled {
            SemanticTone::Success
        } else {
            SemanticTone::Neutral
        },
        module_effective_source_label(module.effective_source),
    );
    let observed = module_lifecycle_value(
        shared,
        reader,
        module_observed_label(module.observed),
        module_observed_tone(module.observed),
        module.observed_detail.clone(),
    );

    let mut body = FieldGroup::new()
        .spacing(8.0)
        .with_child(
            FormRow::new("Desired", desired)
                .stacked()
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new(format!("Effective on {selected_node_label}"), effective)
                .stacked()
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            FormRow::new(format!("Observed on {selected_node_label}"), observed)
                .stacked()
                .theme_when(clone_reader(reader)),
        )
        .with_child(copyable_text_row(
            reader,
            "module-technical-id",
            "Technical ID",
            module.module_id.clone(),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Backend",
            format!("{} · {}", module.backend, module.detail),
        ))
        .with_child(cluster_switch)
        .with_child(node_override);

    for (index, item) in module_config_items {
        body = body.with_child(PodHost::new(module_config_editor_row(
            shared, reader, index, &item,
        )));
    }

    if module_runtime_state_mismatch(&module) {
        body = body.with_child(
            FormRow::new(
                "Reconciliation",
                Stack::horizontal().with_child(
                    StatusBadge::new("Pending reconciliation")
                        .icon(IconGlyph::Alert)
                        .tone(SemanticTone::Warning)
                        .theme_when(clone_reader(reader)),
                ),
            )
            .inline()
            .theme_when(clone_reader(reader)),
        );
    }

    if let Some(node) = shared.settings().module_snapshot.selected_node.as_ref()
        && let Some(error) = node.error.as_ref()
    {
        body = body.with_child(value_row(shared, reader, "Node error", error.clone()));
    }

    FormSection::new(module.label.clone(), body).theme_when(clone_reader(reader))
}

fn module_config_editor_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    item: &ConfigItem,
) -> WidgetPod {
    let editor = if let Some(schema) = item.schema.as_ref() {
        WidgetPod::new(config_schema_editor(shared, reader, index, item, schema))
    } else {
        WidgetPod::new(config_detail_editor(shared, reader, index, item))
    };
    let mut settings_body = Stack::vertical()
        .spacing(5.0)
        .alignment(Alignment::Stretch)
        .with_child(muted_meta_label(shared, config_metadata_line(item)));
    if !item.description.trim().is_empty() {
        settings_body = settings_body.with_child(muted_meta_label(
            shared,
            short_one_line(&item.description, 160),
        ));
    }
    settings_body = settings_body.with_child(PodHost::new(editor));
    WidgetPod::new(
        FormRow::new(
            format!("Settings · {}", item.key.trim_start_matches('/')),
            settings_body,
        )
        .stacked()
        .theme_when(clone_reader(reader)),
    )
}

fn module_ids_match(left: &str, right: &str) -> bool {
    fn normalized(value: &str) -> Option<sinomo_runtime::core::Id> {
        sinomo_runtime::core::Id::new(value.to_string())
            .or_else(|_| sinomo_runtime::core::Id::from_legacy_named(value))
            .ok()
    }

    normalized(left)
        .zip(normalized(right))
        .is_some_and(|(left, right)| left == right)
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

fn module_effective_source_label(source: ModuleRuntimeConfigSource) -> &'static str {
    match source {
        ModuleRuntimeConfigSource::Node => "Resolved from this node's override",
        ModuleRuntimeConfigSource::Cluster => "Resolved from cluster desired state",
        ModuleRuntimeConfigSource::DisabledDefault => "No desired state; disabled by default",
    }
}

fn module_observed_label(state: ModuleObservedState) -> &'static str {
    match state {
        ModuleObservedState::Running => "Running",
        ModuleObservedState::Stopped => "Stopped",
        ModuleObservedState::Installed => "Installed",
        ModuleObservedState::NotReported => "Not reported",
        ModuleObservedState::Unavailable => "Unavailable",
    }
}

fn module_observed_tone(state: ModuleObservedState) -> SemanticTone {
    match state {
        ModuleObservedState::Running => SemanticTone::Success,
        ModuleObservedState::Stopped => SemanticTone::Neutral,
        ModuleObservedState::Installed => SemanticTone::Info,
        ModuleObservedState::NotReported | ModuleObservedState::Unavailable => {
            SemanticTone::Warning
        }
    }
}

fn module_lifecycle_value(
    shared: &Shared,
    reader: &ThemeReader,
    label: impl Into<String>,
    tone: SemanticTone,
    detail: impl Into<String>,
) -> Stack {
    Stack::vertical()
        .spacing(4.0)
        .alignment(Alignment::Start)
        .with_child(
            Stack::horizontal().with_child(
                StatusBadge::new(label)
                    .tone(tone)
                    .theme_when(clone_reader(reader)),
            ),
        )
        .with_child(muted_label(shared, detail))
}

fn module_runtime_state_mismatch(
    module: &crate::backend::modules::ModuleConfigModuleSummary,
) -> bool {
    matches!(
        (module.effective_enabled, module.observed),
        (true, ModuleObservedState::Stopped) | (false, ModuleObservedState::Running)
    )
}

fn module_display_id(module_id: &str) -> String {
    short_one_line(module_id, 72)
}

fn section_skills(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    Stack::vertical()
        .spacing(14.0)
        .alignment(Alignment::Stretch)
        .with_child(skills_summary_section(shared, reader))
        .with_child(skills_catalog_host(shared, reader))
        .with_child(skill_detail_host(shared, reader))
}

fn skills_summary_section(shared: &Shared, reader: &ThemeReader) -> FormSection {
    let (include_all, import_path) = {
        let settings = shared.settings();
        (
            settings.skills_include_all,
            settings.skill_import_path.clone(),
        )
    };

    let mut summary_body = FieldGroup::new()
        .spacing(8.0)
        .with_child(skills_summary_preview_host(shared, reader))
        .with_child(switch_row(reader, "Show all versions", include_all, {
            let s = shared.clone();
            move |on| {
                let selected = {
                    let mut settings = s.settings_mut();
                    settings.skills_include_all = on;
                    settings.mark_draft_changed(SettingsOperationDomain::Skills);
                    settings.skill_selected.clone()
                };
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Skills,
                    SettingsOperation::RefreshSkills {
                        include_all: on,
                        selected,
                    },
                );
            }
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
            .enabled_when({
                let s = s.clone();
                move || !s.settings().domain_pending(SettingsOperationDomain::Skills)
            })
            .on_press(move || {
                let path = s.settings().skill_import_path.trim().to_string();
                if path.is_empty() {
                    s.settings_mut().skill_action_error =
                        Some("enter a skill file or directory path".to_string());
                    return;
                }
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Skills,
                    SettingsOperation::ImportSkill {
                        path: PathBuf::from(path),
                    },
                );
            })
    };
    let refresh = {
        let s = shared.clone();
        sui::Button::new("Refresh")
            .icon(sui::IconGlyph::Redo)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled_when({
                let s = s.clone();
                move || !s.settings().domain_pending(SettingsOperationDomain::Skills)
            })
            .on_press(move || {
                let (include_all, selected) = {
                    let settings = s.settings();
                    (settings.skills_include_all, settings.skill_selected.clone())
                };
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Skills,
                    SettingsOperation::RefreshSkills {
                        include_all,
                        selected,
                    },
                );
            })
    };
    summary_body = summary_body.with_child(
        FormRow::new(
            "Actions",
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .align_items(Alignment::Start)
                .with_child(import)
                .with_child(refresh),
        )
        .inline()
        .theme_when(clone_reader(reader)),
    );
    summary_body = summary_body.with_child(feedback_slot(
        shared,
        reader,
        |settings| settings.skill_action_error.clone(),
        |settings| settings.skill_action_status.clone(),
    ));

    FormSection::new("Skills", summary_body)
        .description("Manage imported and generated SKILL.md files used by agents.")
        .theme_when(clone_reader(reader))
}

fn skills_summary_preview_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> RebuildOnChange<Vec<SkillSummary>> {
    let source = shared.signals().settings_structure.select_named(
        "settings skills catalog summary",
        |key| {
            key.0
                .as_deref()
                .map(|snapshot| snapshot.skills.skills.clone())
                .unwrap_or_default()
        },
    );
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |skills| {
        WidgetPod::new(value_row(&s, &r, "Catalog", skills_sidebar_preview(skills)))
    })
}

fn skills_catalog_host(shared: &Shared, reader: &ThemeReader) -> RebuildOnChange<bool> {
    let source = shared.signals().settings_structure.select_named(
        "settings skills catalog availability",
        |key| {
            key.0
                .as_deref()
                .is_none_or(|snapshot| snapshot.skills.skills.is_empty())
        },
    );
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |empty| {
        if *empty {
            let body = FieldGroup::new()
                .spacing(8.0)
                .with_child(value_row(
                    &s,
                    &r,
                    "Status",
                    "No skills in the local catalog",
                ))
                .with_child(value_row(
                    &s,
                    &r,
                    "Import",
                    "Add a SKILL.md file to make it available to agents.",
                ));
            WidgetPod::new(FormSection::new("Catalog", body).theme_when(clone_reader(&r)))
        } else {
            WidgetPod::new(skill_virtual_catalog(&s, &r))
        }
    })
}

fn skill_virtual_catalog(shared: &Shared, reader: &ThemeReader) -> FormSection {
    let collection = shared.signals().settings_structure.select_named(
        "settings virtual skill collection",
        |key| {
            let Some(snapshot) = key.0.as_deref() else {
                return widgets::VirtualCollectionSnapshot::new(Vec::new(), None);
            };
            let skills = snapshot.skills.skills.clone();
            let selected = snapshot.skills.selected.clone();
            let theme = snapshot.skills.theme;
            widgets::VirtualCollectionSnapshot::new(
                skills
                    .into_iter()
                    .map(|skill| {
                        (
                            (skill.id.clone(), skill.version.clone()),
                            SettingsSkillRow { theme, skill },
                        )
                    })
                    .collect(),
                selected,
            )
        },
    );
    let row_shared = shared.clone();
    let action_shared = shared.clone();
    let list = widgets::VirtualCollectionHost::new(
        "Settings skill collection",
        collection,
        move |model, state| {
            VirtualList::new("skill-catalog", model, move |_key, skill| {
                let shared = row_shared.clone();
                RebuildOnChange::new_observable(skill, move |row| {
                    WidgetPod::new(skill_catalog_row(&shared, &row.skill))
                })
            })
            .state(state)
            .estimated_row_height(MENU_ROW_H)
            .theme_when(clone_reader(reader))
            .row_name(|_key, row| skill_title(&row.skill))
            .row_description(|_key, row| {
                let skill = &row.skill;
                let state = if skill.active {
                    "active"
                } else {
                    skill.status.as_str()
                };
                format!("{}; {}", state, skill_source_label(skill))
            })
            .on_change(move |selected| {
                let include_all = {
                    let mut settings = action_shared.settings_mut();
                    settings.skill_selected = Some(selected.clone());
                    settings.skill_action_error = None;
                    settings.skill_text_expanded = false;
                    settings.mark_draft_changed(SettingsOperationDomain::Skills);
                    settings.skills_include_all
                };
                queue_settings_operation(
                    &action_shared,
                    SettingsOperationScope::Skills,
                    SettingsOperation::RefreshSkills {
                        include_all,
                        selected: Some(selected),
                    },
                );
            })
        },
    );
    FormSection::new("Catalog", list)
        .description("Select a skill version to inspect, activate, or export.")
        .theme_when(clone_reader(reader))
}

fn skill_detail_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> RebuildOnChange<Option<SettingsSkillsStructure>> {
    let source = shared
        .signals()
        .settings_structure
        .select_named("settings skill details structure", |key| {
            key.0.as_deref().map(|snapshot| snapshot.skills.clone())
        });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_key| {
        let (detail, export_path, text_expanded) = {
            let settings = s.settings();
            (
                settings.skill_detail.clone(),
                settings.skill_export_path.clone(),
                settings.skill_text_expanded,
            )
        };
        detail
            .map(|detail| {
                WidgetPod::new(skill_detail_section(
                    &s,
                    &r,
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
                            &s,
                            &r,
                            "Status",
                            "Select a skill to view details",
                        )),
                    )
                    .theme_when(clone_reader(&r)),
                )
            })
    })
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

    let already_active = summary.active;
    let activate = {
        let s = shared.clone();
        sui::Button::new("Activate")
            .icon(sui::IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    !already_active && !s.settings().domain_pending(SettingsOperationDomain::Skills)
                }
            })
            .on_press(move || {
                let selected = s.settings().skill_selected.clone();
                let include_all = s.settings().skills_include_all;
                if let Some((skill_id, version)) = selected {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Skills,
                        SettingsOperation::ActivateSkill {
                            skill_id,
                            version,
                            include_all,
                        },
                    );
                }
            })
    };
    let export = {
        let s = shared.clone();
        sui::Button::new("Export skill")
            .icon(sui::IconGlyph::Download)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled_when({
                let s = s.clone();
                move || !s.settings().domain_pending(SettingsOperationDomain::Skills)
            })
            .on_press(move || {
                let (selected, output, include_all) = {
                    let settings = s.settings();
                    (
                        settings.skill_selected.clone(),
                        settings.skill_export_path.trim().to_string(),
                        settings.skills_include_all,
                    )
                };
                let Some((skill_id, version)) = selected else {
                    s.settings_mut().skill_action_error =
                        Some("select a skill to export".to_string());
                    return;
                };
                if output.is_empty() {
                    s.settings_mut().skill_action_error = Some("enter an export path".to_string());
                    return;
                }
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Skills,
                    SettingsOperation::ExportSkill {
                        skill_id,
                        version,
                        output: PathBuf::from(output),
                        include_all,
                    },
                );
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
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press(move || s.settings_mut().toggle_skill_text())
    };
    body = body.with_child(
        FormRow::new(
            "Actions",
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .align_items(Alignment::Start)
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
    let source = shared.signals().settings_structure.select_named(
        "settings configuration structure",
        |key| {
            key.0
                .as_deref()
                .map(|snapshot| snapshot.configuration.clone())
        },
    );
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_key| configuration_body(&s, &r))
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
        if edit_kind == ConfigEditKind::Bool && !item.read_only {
            let on_toggle = {
                let s = shared.clone();
                let key = key.clone();
                move |next: bool| {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::ConfigField(config_field_key(&key)),
                        SettingsOperation::SetConfigBool {
                            key: key.clone(),
                            value: next,
                        },
                    );
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
        settings_label(shared, summary, shared.theme().text.sm)
            .color_when(move || palette(&s).text_muted)
    };
    let meta_label = {
        let s = shared.clone();
        let metadata = metadata.clone();
        settings_label(shared, metadata.clone(), shared.theme().text.xs)
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
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || s.settings_mut().toggle_advanced_detail(&key))
    };
    let mut details = Stack::vertical()
        .spacing(6.0)
        .alignment(Alignment::Stretch)
        .with_child(summary_label)
        .with_child(meta_label);
    if !description.is_empty() {
        details = details.with_child(muted_meta_label(shared, description));
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
    let draft_key = config_field_key(&item.key);
    let detail = shared
        .settings()
        .field_draft_or(&draft_key, || config_detail_value(item));
    let editable = !config_item_has_sensitive_detail(item) && !item.read_only;
    let note = if item.read_only {
        "This operational document is read-only. JSON is only a human-readable projection of the SOBJ value."
    } else if editable {
        "No detailed field schema is available, so this raw editor is the fallback. JSON is only the human editing projection; the config service transports the typed value as SOBJ."
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
            s.settings_mut().set_config_value_draft(index, value);
        });
    } else {
        editor = editor.read_only();
    }
    let mut stack = Stack::vertical()
        .spacing(5.0)
        .alignment(Alignment::Stretch)
        .with_child(
            Stack::horizontal().with_child(
                StatusBadge::new(if item.read_only {
                    "Read-only raw projection"
                } else {
                    "Raw editor fallback"
                })
                .icon(if item.read_only {
                    IconGlyph::Lock
                } else {
                    IconGlyph::Alert
                })
                .tone(if item.read_only {
                    SemanticTone::Neutral
                } else {
                    SemanticTone::Warning
                })
                .theme_when(clone_reader(reader)),
            ),
        )
        .with_child(muted_meta_label(shared, note))
        .with_child(SizedBox::new().height(112.0).with_child(editor));
    if editable {
        let save = {
            let s = shared.clone();
            let scope = SettingsOperationScope::ConfigField(draft_key);
            sui::Button::new("Save raw value")
                .icon(sui::IconGlyph::Check)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .enabled_when({
                    let s = s.clone();
                    let scope = scope.clone();
                    move || !s.settings().operation_pending(&scope)
                })
                .on_press(move || {
                    if let Some((scope, operation)) = s.settings().config_value_operation(index) {
                        queue_settings_operation(&s, scope, operation);
                    }
                })
        };
        stack = stack.with_child(Stack::horizontal().with_child(save));
    }
    stack
}

fn config_schema_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    item: &ConfigItem,
    schema: &ConfigSchema,
) -> impl Widget + use<> {
    let (value, read_only) = {
        let settings = shared.settings();
        (
            settings.config_schema_draft_value(item).clone(),
            item.read_only,
        )
    };
    let mut stack = Stack::vertical().spacing(8.0).alignment(Alignment::Stretch);
    if !schema.description.trim().is_empty() {
        stack = stack.with_child(muted_meta_label(shared, schema.description.clone()));
    }
    if read_only {
        stack = stack.with_child(
            Stack::horizontal().with_child(
                StatusBadge::new("Read only")
                    .icon(IconGlyph::Lock)
                    .tone(SemanticTone::Neutral)
                    .theme_when(clone_reader(reader)),
            ),
        );
    }
    stack = stack.with_child(PodHost::new(config_schema_value_editor(
        shared,
        reader,
        index,
        &value,
        &schema.value,
        Vec::new(),
        &schema.name,
        "",
        read_only,
    )));
    if !read_only {
        let root_scope =
            SettingsOperationScope::ConfigField(config_schema_field_key(&item.key, &[]));
        let save = {
            let s = shared.clone();
            let scope = root_scope.clone();
            sui::Button::new("Save settings")
                .icon(IconGlyph::Check)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .enabled_when({
                    let s = s.clone();
                    let key = item.key.clone();
                    let scope = scope.clone();
                    move || {
                        s.settings().config_schema_is_dirty(&key)
                            && !s.settings().operation_pending(&scope)
                    }
                })
                .on_press(
                    move || match s.settings().config_schema_save_operation(index) {
                        Ok((scope, operation)) => {
                            queue_settings_operation(&s, scope, operation);
                        }
                        Err(error) => {
                            if let SettingsOperationScope::ConfigField(key) = &scope {
                                s.settings_mut().field_errors.insert(key.clone(), error);
                            }
                        }
                    },
                )
        };
        let reset = {
            let s = shared.clone();
            let key = item.key.clone();
            sui::Button::new("Reset draft")
                .icon(IconGlyph::Redo)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .enabled_when({
                    let s = s.clone();
                    let key = key.clone();
                    move || s.settings().config_schema_is_dirty(&key)
                })
                .on_press(move || s.settings_mut().reset_config_schema_draft(index))
        };
        let root_error_key = config_schema_field_key(&item.key, &[]);
        stack = stack.with_child(
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .with_child(save)
                .with_child(reset),
        );
        stack = stack.with_child(config_schema_feedback_label(
            shared,
            root_error_key,
            String::new(),
        ));
    }
    stack
}

fn config_schema_feedback_label(shared: &Shared, error_key: String, fallback: String) -> Label {
    let s = shared.clone();
    let token = shared.theme().text.sm;
    settings_label(shared, fallback.clone(), token)
        .text_when({
            let s = s.clone();
            let error_key = error_key.clone();
            move || {
                s.settings()
                    .field_errors
                    .get(&error_key)
                    .cloned()
                    .unwrap_or_else(|| fallback.clone())
            }
        })
        .color_when(move || {
            let palette = palette(&s);
            if s.settings().field_errors.contains_key(&error_key) {
                palette.danger_text
            } else {
                palette.text_muted
            }
        })
}

fn config_schema_value_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    schema: &ConfigSchemaValue,
    path: Vec<ConfigPathSegment>,
    label: &str,
    description: &str,
    read_only: bool,
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
            read_only,
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
            read_only,
        ),
        ConfigSchemaValue::List { value: item_schema } => config_schema_list_editor(
            shared,
            reader,
            index,
            value,
            item_schema,
            path,
            label,
            description,
            read_only,
        ),
        ConfigSchemaValue::StringList => config_schema_list_editor(
            shared,
            reader,
            index,
            value,
            &ConfigSchemaValue::string(),
            path,
            label,
            description,
            read_only,
        ),
        ConfigSchemaValue::Json => config_schema_json_row(
            shared,
            reader,
            index,
            value,
            schema,
            path,
            label,
            description,
            read_only,
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
            read_only,
        ),
    }
}

fn config_schema_object_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    fields: &[ConfigSchemaField],
    path: Vec<ConfigPathSegment>,
    label: &str,
    description: &str,
    read_only: bool,
) -> WidgetPod {
    let mut group = FieldGroup::new().spacing(8.0);
    if !description.trim().is_empty() {
        group = group.with_child(value_row(shared, reader, label, description));
    }
    for field in fields {
        let mut field_path = path.clone();
        field_path.push(ConfigPathSegment::Field(field.name.clone()));
        let field_label = if field.required {
            format!("{} *", field.label)
        } else {
            field.label.clone()
        };
        let stored_field_value = value
            .as_object()
            .and_then(|map| map.get(&field.name))
            .filter(|value| !value.is_null());
        let field_present = stored_field_value.is_some();
        if !field.required && !field_present && field.default_value.is_none() {
            let error_key = shared
                .settings()
                .config_items
                .get(index)
                .map(|item| config_schema_field_key(&item.key, &field_path));
            let mut body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(config_schema_feedback_label(
                    shared,
                    error_key.clone().unwrap_or_default(),
                    "Not set".to_string(),
                ));
            if !field.description.trim().is_empty() {
                body = body.with_child(muted_meta_label(shared, field.description.clone()));
            }
            if !read_only {
                let s = shared.clone();
                let set_path = field_path.clone();
                let initial_value = if matches!(&field.value, ConfigSchemaValue::Json) {
                    Value::Object(serde_json::Map::new())
                } else {
                    schema_default_value(&field.value)
                };
                let field_label = field.label.clone();
                body = body.with_child(
                    Stack::horizontal().with_child(
                        sui::Button::new(format!("Set {field_label}"))
                            .icon(IconGlyph::Add)
                            .theme_when(clone_reader(reader))
                            .appearance(sui::ButtonAppearance::Tonal)
                            .tone(SemanticTone::Neutral)
                            .on_press(move || {
                                if let Err(error) = s.settings_mut().set_config_schema_value_draft(
                                    index,
                                    set_path.clone(),
                                    initial_value.clone(),
                                ) && let Some(key) = &error_key
                                {
                                    s.settings_mut().field_errors.insert(key.clone(), error);
                                }
                            }),
                    ),
                );
            }
            group = group.with_child(
                FormRow::new(field_label, body)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            );
            continue;
        }
        let field_value = stored_field_value
            .or(field.default_value.as_ref())
            .unwrap_or(&Value::Null);
        group = group.with_child(PodHost::new(config_schema_value_editor(
            shared,
            reader,
            index,
            field_value,
            &field.value,
            field_path.clone(),
            &field_label,
            &field.description,
            read_only,
        )));
        if !field.required && !read_only && field_present {
            let s = shared.clone();
            let unset_path = field_path;
            let error_key = s
                .settings()
                .config_items
                .get(index)
                .map(|item| config_schema_field_key(&item.key, &unset_path));
            group = group.with_child(
                Stack::horizontal().with_child(
                    sui::Button::new(format!("Unset {}", field.label))
                        .icon(IconGlyph::Close)
                        .theme_when(clone_reader(reader))
                        .appearance(sui::ButtonAppearance::Tonal)
                        .tone(SemanticTone::Neutral)
                        .on_press(move || {
                            if let Err(error) = s
                                .settings_mut()
                                .remove_config_schema_path(index, &unset_path)
                                && let Some(key) = &error_key
                            {
                                s.settings_mut().field_errors.insert(key.clone(), error);
                            }
                        }),
                ),
            );
        }
    }
    WidgetPod::new(group)
}

fn config_schema_map_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    item_schema: &ConfigSchemaValue,
    path: Vec<ConfigPathSegment>,
    label: &str,
    description: &str,
    read_only: bool,
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
        .with_child(muted_meta_label(
            shared,
            format!("{} entries", entries.len()),
        ));
    if !description.trim().is_empty() {
        summary_body = summary_body.with_child(muted_meta_label(shared, description.to_string()));
    }
    group = group.with_child(
        FormRow::new(label, summary_body)
            .stacked()
            .theme_when(clone_reader(reader)),
    );
    for (entry_key, entry_value) in entries {
        let mut entry_path = path.clone();
        entry_path.push(ConfigPathSegment::Field(entry_key.clone()));
        group = group.with_child(PodHost::new(config_schema_value_editor(
            shared,
            reader,
            index,
            &entry_value,
            item_schema,
            entry_path.clone(),
            &entry_key,
            "",
            read_only,
        )));
        if !read_only {
            let s = shared.clone();
            let remove_path = entry_path;
            group = group.with_child(
                Stack::horizontal().with_child(
                    sui::Button::new(format!("Remove {}", short_one_line(&entry_key, 32)))
                        .icon(IconGlyph::Close)
                        .theme_when(clone_reader(reader))
                        .appearance(sui::ButtonAppearance::Tonal)
                        .tone(SemanticTone::Neutral)
                        .on_press(move || {
                            let _ = s
                                .settings_mut()
                                .remove_config_schema_path(index, &remove_path);
                        }),
                ),
            );
        }
    }
    if !read_only {
        let add_key = shared
            .settings()
            .config_items
            .get(index)
            .map(|item| config_schema_add_key(&item.key, &path));
        if let Some(add_key) = add_key {
            let draft = shared.settings().field_draft_or(&add_key, String::new);
            let input = {
                let s = shared.clone();
                let draft_key = add_key.clone();
                TextInput::new(format!("{}-map-key", schema_control_name(index, &path)))
                    .value(draft)
                    .placeholder("New entry key")
                    .theme_when(clone_reader(reader))
                    .on_change(move |value| {
                        let mut settings = s.settings_mut();
                        settings.field_errors.remove(&draft_key);
                        settings.set_field_draft(
                            SettingsOperationDomain::Config,
                            draft_key.clone(),
                            value,
                        );
                    })
            };
            let add = {
                let s = shared.clone();
                let add_path = path.clone();
                let item_schema = item_schema.clone();
                let draft_key = add_key.clone();
                sui::Button::new("Add entry")
                    .icon(IconGlyph::Add)
                    .theme_when(clone_reader(reader))
                    .appearance(sui::ButtonAppearance::Tonal)
                    .tone(SemanticTone::Neutral)
                    .on_press(move || {
                        let entry_key = s
                            .settings()
                            .field_drafts
                            .get(&draft_key)
                            .cloned()
                            .unwrap_or_default();
                        if let Err(error) = s.settings_mut().insert_config_schema_map_entry(
                            index,
                            &add_path,
                            &entry_key,
                            &item_schema,
                        ) {
                            s.settings_mut()
                                .field_errors
                                .insert(draft_key.clone(), error);
                        }
                    })
            };
            let add_body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(input)
                .with_child(Stack::horizontal().with_child(add))
                .with_child(config_schema_feedback_label(
                    shared,
                    add_key.clone(),
                    String::new(),
                ));
            group = group.with_child(
                FormRow::new("New entry", add_body)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            );
        }
    }
    WidgetPod::new(group)
}

fn config_schema_list_editor(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    item_schema: &ConfigSchemaValue,
    path: Vec<ConfigPathSegment>,
    label: &str,
    description: &str,
    read_only: bool,
) -> WidgetPod {
    let items = value.as_array().cloned().unwrap_or_default();
    let mut summary = Stack::vertical()
        .spacing(4.0)
        .alignment(Alignment::Stretch)
        .with_child(muted_meta_label(shared, format!("{} items", items.len())));
    if !description.trim().is_empty() {
        summary = summary.with_child(muted_meta_label(shared, description.to_string()));
    }
    let mut group = FieldGroup::new().spacing(8.0).with_child(
        FormRow::new(label, summary)
            .stacked()
            .theme_when(clone_reader(reader)),
    );
    for (item_index, item_value) in items.iter().enumerate() {
        let mut item_path = path.clone();
        item_path.push(ConfigPathSegment::Index(item_index));
        group = group.with_child(PodHost::new(config_schema_value_editor(
            shared,
            reader,
            index,
            item_value,
            item_schema,
            item_path.clone(),
            &format!("Item {}", item_index + 1),
            "",
            read_only,
        )));
        if !read_only {
            let s = shared.clone();
            let remove_path = item_path;
            group = group.with_child(
                Stack::horizontal().with_child(
                    sui::Button::new(format!("Remove item {}", item_index + 1))
                        .icon(IconGlyph::Close)
                        .theme_when(clone_reader(reader))
                        .appearance(sui::ButtonAppearance::Tonal)
                        .tone(SemanticTone::Neutral)
                        .on_press(move || {
                            let _ = s
                                .settings_mut()
                                .remove_config_schema_path(index, &remove_path);
                        }),
                ),
            );
        }
    }
    if !read_only {
        let s = shared.clone();
        let add_path = path;
        let item_schema = item_schema.clone();
        group = group.with_child(
            Stack::horizontal().with_child(
                sui::Button::new("Add item")
                    .icon(IconGlyph::Add)
                    .theme_when(clone_reader(reader))
                    .appearance(sui::ButtonAppearance::Tonal)
                    .tone(SemanticTone::Neutral)
                    .on_press(move || {
                        let _ = s.settings_mut().append_config_schema_list_item(
                            index,
                            &add_path,
                            &item_schema,
                        );
                    }),
            ),
        );
    }
    WidgetPod::new(group)
}

fn config_schema_primitive_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    schema: &ConfigSchemaValue,
    path: Vec<ConfigPathSegment>,
    label: &str,
    description: &str,
    read_only: bool,
) -> WidgetPod {
    if read_only {
        let mut detail = config_schema_inline_summary(value, schema);
        if !description.trim().is_empty() {
            detail.push_str(" · ");
            detail.push_str(description);
        }
        return WidgetPod::new(value_row(shared, reader, label, detail));
    }
    match schema {
        ConfigSchemaValue::Bool => {
            let current = value.as_bool().unwrap_or(false);
            let row = switch_row(reader, label, current, {
                let s = shared.clone();
                let path = path.clone();
                move |enabled| {
                    if let Err(error) = s.settings_mut().set_config_schema_value_draft(
                        index,
                        path.clone(),
                        Value::Bool(enabled),
                    ) && let Some(item) = s.settings().config_items.get(index)
                    {
                        let key = config_schema_field_key(&item.key, &path);
                        s.settings_mut().field_errors.insert(key, error);
                    }
                }
            });
            let mut body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(row);
            if !description.trim().is_empty() {
                body = body.with_child(muted_meta_label(shared, description.to_string()));
            }
            WidgetPod::new(body)
        }
        ConfigSchemaValue::String { choices, secret } if !choices.is_empty() && !*secret => {
            let fallback_value = value.as_str().map(str::to_string);
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
                    let fallback_value = fallback_value.clone();
                    move || {
                        let settings = s.settings();
                        let item = settings.config_items.get(index)?;
                        let current =
                            config_value_at_path(settings.config_schema_draft_value(item), &path)
                                .and_then(Value::as_str)
                                .or(fallback_value.as_deref())?;
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
                            let _ = s.settings_mut().set_config_schema_value_draft(
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
                body = body.with_child(muted_meta_label(shared, description.to_string()));
            }
            WidgetPod::new(
                FormRow::new(label, body)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            )
        }
        ConfigSchemaValue::String { secret, .. } if *secret => {
            let configured = value.as_str().is_some_and(|value| !value.is_empty());
            let error_key = shared
                .settings()
                .config_items
                .get(index)
                .map(|item| config_schema_field_key(&item.key, &path));
            let replacement = error_key
                .as_ref()
                .and_then(|key| shared.settings().field_drafts.get(key).cloned())
                .unwrap_or_default();
            let input = TextInput::new(schema_control_name(index, &path))
                .value(replacement)
                .placeholder("Enter replacement secret")
                .theme_when(clone_reader(reader))
                .on_change({
                    let s = shared.clone();
                    let path = path.clone();
                    let schema = schema.clone();
                    move |raw| {
                        s.settings_mut().set_config_schema_text_draft(
                            index,
                            path.clone(),
                            schema.clone(),
                            raw,
                        );
                    }
                });
            let body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(muted_meta_label(
                    shared,
                    if configured {
                        "Current value: •••••••• · enter a replacement; the current secret is never prefilled"
                    } else {
                        "Not configured · enter a secret to configure this field"
                    },
                ))
                .with_child(input);
            WidgetPod::new(
                FormRow::new(label, body)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            )
        }
        ConfigSchemaValue::I64 { .. } | ConfigSchemaValue::U64 { .. } => {
            let error_key = {
                let settings = shared.settings();
                settings
                    .config_items
                    .get(index)
                    .map(|item| config_schema_field_key(&item.key, &path))
            };
            let value_text = error_key
                .as_ref()
                .map(|key| {
                    shared
                        .settings()
                        .field_draft_or(key, || schema_scalar_edit_value(schema, value))
                })
                .unwrap_or_else(|| schema_scalar_edit_value(schema, value));
            let input = TextInput::new(schema_control_name(index, &path))
                .value(value_text)
                .placeholder(schema_placeholder(schema))
                .theme_when(clone_reader(reader))
                .on_change({
                    let s = shared.clone();
                    let path = path.clone();
                    let schema = schema.clone();
                    move |raw| {
                        s.settings_mut().set_config_schema_text_draft(
                            index,
                            path.clone(),
                            schema.clone(),
                            raw,
                        );
                    }
                });
            let mut body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(input);
            let fallback = schema_field_detail(schema, description);
            if let Some(error_key) = error_key {
                body = body.with_child(config_schema_feedback_label(shared, error_key, fallback));
            } else if !fallback.trim().is_empty() {
                body = body.with_child(muted_meta_label(shared, fallback));
            }
            WidgetPod::new(
                FormRow::new(label, body)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            )
        }
        ConfigSchemaValue::F64 { range } => {
            let current = value
                .as_f64()
                .unwrap_or_else(|| schema_default_number(range.as_ref()));
            let type_bounds = (-f64::MAX, f64::MAX);
            let (min, max) = range.as_ref().map_or(type_bounds, |range| {
                (
                    range.min.unwrap_or(type_bounds.0),
                    range.max.unwrap_or(type_bounds.1),
                )
            });
            let number = NumberInput::new(schema_control_name(index, &path))
                .range(min, max)
                .step(0.01)
                .precision(2)
                .value(current)
                .theme_when(clone_reader(reader))
                .on_change({
                    let s = shared.clone();
                    let path = path.clone();
                    move |number| {
                        let value = serde_json::Number::from_f64(number)
                            .map(Value::Number)
                            .unwrap_or(Value::Null);
                        let _ = s.settings_mut().set_config_schema_value_draft(
                            index,
                            path.clone(),
                            value,
                        );
                    }
                });
            let mut body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(number);
            let detail = schema_field_detail(schema, description);
            if !detail.trim().is_empty() {
                body = body.with_child(muted_meta_label(shared, detail));
            }
            WidgetPod::new(
                FormRow::new(label, body)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            )
        }
        ConfigSchemaValue::String { .. } => {
            let error_key = {
                let settings = shared.settings();
                settings
                    .config_items
                    .get(index)
                    .map(|item| config_schema_field_key(&item.key, &path))
            };
            let value_text = error_key
                .as_ref()
                .map(|key| {
                    shared
                        .settings()
                        .field_draft_or(key, || schema_scalar_edit_value(schema, value))
                })
                .unwrap_or_else(|| schema_scalar_edit_value(schema, value));
            let input = TextInput::new(schema_control_name(index, &path))
                .value(value_text)
                .placeholder(schema_placeholder(schema))
                .theme_when(clone_reader(reader))
                .on_change({
                    let s = shared.clone();
                    let path = path.clone();
                    let schema = schema.clone();
                    move |raw| {
                        s.settings_mut().set_config_schema_text_draft(
                            index,
                            path.clone(),
                            schema.clone(),
                            raw,
                        );
                    }
                });
            let mut body = Stack::vertical()
                .spacing(4.0)
                .alignment(Alignment::Stretch)
                .with_child(input);
            let fallback = schema_field_detail(schema, description);
            if let Some(error_key) = error_key {
                body = body.with_child(config_schema_feedback_label(shared, error_key, fallback));
            } else if !fallback.trim().is_empty() {
                body = body.with_child(muted_meta_label(shared, fallback));
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
            false,
        ),
    }
}

fn config_schema_json_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    value: &Value,
    schema: &ConfigSchemaValue,
    path: Vec<ConfigPathSegment>,
    label: &str,
    description: &str,
    read_only: bool,
) -> WidgetPod {
    if read_only {
        return WidgetPod::new(value_row(
            shared,
            reader,
            label,
            config_json_value_summary(value),
        ));
    }
    let fallback_value = if value.is_null() {
        String::new()
    } else {
        serde_json::to_string_pretty(value).unwrap_or_default()
    };
    let error_key = shared
        .settings()
        .config_items
        .get(index)
        .map(|item| config_schema_field_key(&item.key, &path));
    let value_text = error_key
        .as_ref()
        .map(|key| {
            shared
                .settings()
                .field_draft_or(key, || fallback_value.clone())
        })
        .unwrap_or(fallback_value);
    let editor = TextArea::new(schema_control_name(index, &path))
        .value(value_text)
        .placeholder("JSON value")
        .theme_when(clone_reader(reader))
        .on_change({
            let s = shared.clone();
            let path = path.clone();
            let schema = schema.clone();
            move |raw| {
                s.settings_mut().set_config_schema_text_draft(
                    index,
                    path.clone(),
                    schema.clone(),
                    raw,
                );
            }
        });
    let mut body = Stack::vertical()
        .spacing(4.0)
        .alignment(Alignment::Stretch)
        .with_child(
            Stack::horizontal().with_child(
                StatusBadge::new("Raw JSON fallback")
                    .icon(IconGlyph::Alert)
                    .tone(SemanticTone::Warning)
                    .theme_when(clone_reader(reader)),
            ),
        )
        .with_child(muted_meta_label(
            shared,
            "This schema explicitly permits arbitrary data. JSON is only the human editing projection; the config service transports the typed value as SOBJ.",
        ))
        .with_child(SizedBox::new().height(92.0).with_child(editor));
    let fallback = schema_field_detail(schema, description);
    if let Some(error_key) = error_key {
        body = body.with_child(config_schema_feedback_label(shared, error_key, fallback));
    } else if !fallback.trim().is_empty() {
        body = body.with_child(muted_meta_label(shared, fallback));
    }
    WidgetPod::new(
        FormRow::new(label, body)
            .stacked()
            .theme_when(clone_reader(reader)),
    )
}

fn config_value_at_path<'a>(value: &'a Value, path: &[ConfigPathSegment]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = match segment {
            ConfigPathSegment::Field(field) => current.as_object()?.get(field)?,
            ConfigPathSegment::Index(index) => current.as_array()?.get(*index)?,
        };
    }
    Some(current)
}

fn schema_control_name(index: usize, path: &[ConfigPathSegment]) -> String {
    if path.is_empty() {
        return format!("config-{index}-schema-root");
    }
    let path = path
        .iter()
        .map(|segment| {
            let segment = match segment {
                ConfigPathSegment::Field(field) => field.clone(),
                ConfigPathSegment::Index(index) => format!("item-{index}"),
            };
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
                let value = map
                    .get(&field.name)
                    .or(field.default_value.as_ref())
                    .unwrap_or(&Value::Null);
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
        ConfigSchemaValue::String { secret, .. } if *secret => {
            if value.as_str().is_some_and(|value| !value.is_empty()) {
                "configured secret".to_string()
            } else {
                "unset".to_string()
            }
        }
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
    let selected =
        shared
            .signals()
            .settings_structure
            .select_named("settings detail selection", |key| {
                key.0
                    .as_deref()
                    .and_then(|snapshot| snapshot.selected)
                    .unwrap_or(SettingsSection::Appearance)
                    .index()
            });
    let switch = SwitchView::new()
        .with_child(section_appearance(shared, reader))
        .with_child(section_connection(shared, reader))
        .with_child(section_node(shared, reader))
        .with_child(section_cluster(shared, reader))
        .with_child(section_providers(shared, reader))
        .with_child(section_agents(shared, reader))
        .with_child(section_modules(shared, reader))
        .with_child(section_skills(shared, reader))
        .with_child(section_configuration(shared, reader))
        .with_child(section_about(shared, reader))
        .selected_from(selected);
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
        let mut style = settings_text_style(shared, shared.theme().text.xs);
        style.weight = FontWeight::SEMIBOLD;
        Label::new("SETTINGS")
            .style(style)
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

    let selected =
        shared
            .signals()
            .settings_structure
            .select_named("settings narrow menu selection", |key| {
                key.0
                    .as_deref()
                    .is_some_and(|snapshot| snapshot.selected.is_some()) as usize
            });
    let switch = SwitchView::new()
        .with_child(menu(shared, reader))
        .with_child(detail_with_backbar)
        .selected_from(selected);
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
// Live device-login status — composed from stock SUI text/link widgets.
// ---------------------------------------------------------------------------

fn status_reflector(shared: &Shared, index: usize, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared.signals().settings_structure.select_named(
        "settings device login status",
        move |key| {
            key.0
                .as_deref()
                .map(|snapshot| (snapshot.theme, snapshot.device_logins.get(index).cloned()))
        },
    );
    let build_shared = shared.clone();
    let build_reader = Rc::clone(reader);
    RebuildOnChange::new_observable(source, move |_| {
        let theme = build_reader();
        let pal = palette(&build_shared);
        let (primary, prompt_url) = device_login_lines(&build_shared, index);
        let is_error = device_login_is_error(&build_shared, index);
        let primary_style = sui::TextStyle {
            font_size: theme.text.sm.size,
            line_height: theme.text.sm.line_height,
            weight: FontWeight::MEDIUM,
            color: if is_error { pal.danger_text } else { pal.text },
            ..theme.body_text_style()
        };
        let mut body = Flex::vertical()
            .align_items(Alignment::Start)
            .with_child(sui::Label::new(primary).style(primary_style));
        if let Some(url) = prompt_url {
            let link_style = sui::TextStyle {
                font_size: theme.text.xs.size,
                line_height: theme.text.xs.line_height,
                ..theme.text_style(pal.accent_text)
            };
            body = body.with_child(
                sui::Link::new(url.clone(), url)
                    .semantic_name("Open login page")
                    .text_style(link_style)
                    .theme_when(clone_reader(&build_reader))
                    .on_open(open_provider_login_url),
            );
        }
        WidgetPod::new(body)
    })
}

fn device_login_lines(shared: &Shared, index: usize) -> (String, Option<String>) {
    let settings = shared.settings();
    let Some(form) = settings.provider_forms.get(index) else {
        return (String::new(), None);
    };
    match device_login_status_for(&settings, index) {
        Some(DeviceLoginStatus::Starting) => ("Requesting code…".to_string(), None),
        Some(DeviceLoginStatus::Prompt {
            verification_uri,
            user_code,
        }) => (format!("Code {user_code}"), Some(verification_uri.clone())),
        Some(DeviceLoginStatus::Done) => ("Signed in".to_string(), None),
        Some(DeviceLoginStatus::Failed(error)) => (format!("Failed: {error}"), None),
        None if form.signed_in => ("Signed in".to_string(), None),
        None => ("Not signed in".to_string(), None),
    }
}

fn device_login_is_error(shared: &Shared, index: usize) -> bool {
    let settings = shared.settings();
    matches!(
        device_login_status_for(&settings, index),
        Some(DeviceLoginStatus::Failed(_))
    )
}

fn device_login_status_for(settings: &SettingsState, index: usize) -> Option<&DeviceLoginStatus> {
    let form = settings.provider_forms.get(index)?;
    settings
        .device_login
        .as_ref()
        .filter(|d| d.provider == form.provider && d.account == form.account)
        .map(|d| &d.status)
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

// ---------------------------------------------------------------------------
// Responsive host
// ---------------------------------------------------------------------------

/// The settings tab: a thin responsive host that owns two pre-built pods (wide master/detail and
/// narrow drill-down) and delegates to the one chosen by the available width. The choice is made in
/// `measure` (constraints-driven) and cached so `arrange`/`paint`/event routing all stay in sync.
pub struct SettingsView {
    wide: WidgetPod,
    narrow: WidgetPod,
    use_wide: Cell<bool>,
}

impl SettingsView {
    pub fn new(shared: Shared) -> Self {
        let theme_reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || settings_control_theme(&s))
        };
        // One shared scroll state so position is consistent across the wide/narrow swap.
        let scroll_state = ScrollState::new();
        let wide = wide_pod(&shared, &theme_reader, &scroll_state);
        let narrow = narrow_pod(&shared, &theme_reader, &scroll_state);
        Self {
            wide,
            narrow,
            // Default to wide before the first measure (matches the old default).
            use_wide: Cell::new(true),
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
    fn event(&mut self, _ctx: &mut EventCtx, _event: &Event) {}

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let use_wide = constraints.max.width >= WIDE_THRESHOLD;
        self.use_wide.set(use_wide);
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

fn settings_control_theme(shared: &Shared) -> DefaultTheme {
    let mut theme = shared.theme().with_size(ControlSize::Small);
    theme.metrics.form_row_label_width = 132.0;
    theme.metrics.form_row_control_width = 520.0;
    theme.metrics.form_section_max_width = 860.0;
    theme
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
    shared.publish_settings_signals();
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

    fn reload_config_state(state: &mut SettingsState) {
        state.config_items = resolve_items(state.config.as_ref());
        state.provider_forms = provider_forms(state.config.as_ref());
    }

    fn apply_test_operation(
        state: &mut SettingsState,
        backend: &Backend,
        scope: SettingsOperationScope,
        operation: SettingsOperation,
    ) -> SettingsOperationEffects {
        let request_id = state.next_operation_request_id;
        state.next_operation_request_id = request_id.wrapping_add(1);
        let draft_generation = state.draft_generations.get(scope.domain());
        state.mark_operation_pending(&operation);
        state.pending_operations.insert(scope.clone(), request_id);
        let payload = execute_settings_operation(backend, operation);
        let (request_tx, _request_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        result_tx
            .send(SettingsOperationResult {
                request_id,
                scope,
                draft_generation,
                payload,
            })
            .expect("settings result");
        state.operation_lane = Some(SettingsOperationLane {
            tx: request_tx,
            rx: result_rx,
        });
        state.pump_operations()
    }

    #[test]
    fn live_text_drafts_do_not_change_retained_section_keys() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));

        shared.settings_mut().cluster_action_status = Some("Ready".to_string());
        let cluster_key = cluster_section_key(&shared);
        shared
            .settings_mut()
            .set_join_invite("sinomo://cluster/join/test".to_string());
        assert_eq!(
            cluster_section_key(&shared),
            cluster_key,
            "typing a join link must not replace the focused cluster section"
        );

        shared.settings_mut().create_agent();
        shared.settings_mut().agent_action_status = Some("Draft loaded".to_string());
        let agents_key = agents_section_key(&shared);
        {
            let mut settings = shared.settings_mut();
            settings.set_agent_name("Draft agent".to_string());
            settings.set_agent_system_prompt("Keep the editor focused.".to_string());
            settings.set_agent_import_path("agent.json".to_string());
            settings.set_agent_export_path("exported-agent.json".to_string());
        }
        assert_eq!(
            agents_section_key(&shared),
            agents_key,
            "agent form and package path drafts must not replace the focused agents section"
        );

        shared.settings_mut().skill_action_status = Some("Ready".to_string());
        let skills_key = skills_section_key(&shared);
        {
            let mut settings = shared.settings_mut();
            settings.set_skill_import_path("SKILL.md".to_string());
            settings.set_skill_export_path("exported-SKILL.md".to_string());
        }
        assert_eq!(
            skills_section_key(&shared),
            skills_key,
            "skill path drafts must not replace the focused skills section"
        );

        {
            let mut settings = shared.settings_mut();
            settings.open_provider_add();
            settings.provider_add_error = Some("API key required".to_string());
        }
        let providers_key = provider_section_signature(&shared);
        shared
            .settings_mut()
            .set_provider_add_api_key("secret".to_string());
        assert_eq!(
            provider_section_signature(&shared),
            providers_key,
            "clearing add-provider validation feedback must not replace its focused input"
        );
    }

    #[test]
    fn settings_text_input_keeps_focus_across_character_commits() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::Agents);
            settings.agent_import_path.clear();
            settings.agent_action_status = Some("Previous operation completed".to_string());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings input focus")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let field = window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-import-path");

        window
            .get_by_text("Previous operation completed")
            .expect()
            .to_be_visible()?;
        field.focus()?;
        field.fill("A")?;
        field.expect().to_be_focused()?;
        window
            .get_by_text("Previous operation completed")
            .expect()
            .to_be_hidden()?;
        field.fill("B")?;
        field.expect().to_be_focused()?;
        field.expect().to_have_value("AB")?;
        assert_eq!(shared.settings().agent_import_path, "AB");

        Ok(())
    }

    #[test]
    fn settings_snapshot_degrades_sifs_sections_instead_of_wedging() {
        let backend = Backend::local_fallback();
        // A zero budget stands in for SIFS loads wedged behind distributed
        // timeouts: both SIFS-backed sections must degrade with an error while
        // the rest of the snapshot (connection, cluster, config) stays usable.
        let state = SettingsState::new_with_sifs_timeout(&backend, Duration::ZERO);

        let skill_error = state
            .skill_action_error
            .as_deref()
            .expect("skills section should report the degraded load");
        assert!(
            skill_error.contains("skill-catalog timed out"),
            "unexpected skills error: {skill_error}"
        );

        let agent_error = state
            .agent_snapshot
            .error
            .as_deref()
            .expect("agents section should report the degraded load");
        assert!(
            agent_error.contains("agent-catalog timed out"),
            "unexpected agents error: {agent_error}"
        );
        assert!(
            !state.agent_snapshot.agents.is_empty(),
            "agents section should fall back to built-ins"
        );

        assert!(
            !state.mode_label.is_empty() && !state.node_id.is_empty(),
            "connection details must stay available in the degraded snapshot"
        );
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
    fn lightweight_settings_state_defers_expensive_snapshots() {
        let backend = Backend::local_fallback();
        let state = SettingsState::new_lightweight(&backend);

        assert!(state.config_items.is_empty());
        assert!(state.provider_forms.is_empty());
        assert!(state.agent_snapshot.agents.is_empty());
        assert!(state.module_snapshot.nodes.is_empty());
        assert!(state.skills.is_empty());
        assert_eq!(
            state.config_error.as_deref(),
            Some("Settings data is loading.")
        );
        assert!(!state.initial_load_started);
    }

    #[test]
    fn preload_keeps_agent_draft_while_merging_server_catalog() {
        let backend = Backend::local_fallback();
        let mut live = SettingsState::new_lightweight(&backend);
        let baseline_server = live.server_generations;
        let baseline_drafts = live.draft_generations;
        live.create_agent();
        live.set_agent_id("draft-agent".to_string());
        live.set_agent_name("Unsaved draft".to_string());

        let loaded = SettingsState::new(&backend);
        assert!(!loaded.agent_snapshot.agents.is_empty());
        live.merge_initial_load(SettingsInitialLoadResult {
            settings: loaded,
            server_generations: baseline_server,
            draft_generations: baseline_drafts,
        });

        assert!(!live.agent_snapshot.agents.is_empty());
        assert_eq!(live.agent_form.id, "draft-agent");
        assert_eq!(live.agent_form.name, "Unsaved draft");
    }

    #[test]
    fn stale_operation_result_is_rejected_by_request_id() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let scope = SettingsOperationScope::ConfigField("test-field".to_string());
        state.pending_operations.insert(scope.clone(), 2);
        let (request_tx, _request_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        state.operation_lane = Some(SettingsOperationLane {
            tx: request_tx,
            rx: result_rx,
        });

        result_tx
            .send(SettingsOperationResult {
                request_id: 1,
                scope: scope.clone(),
                draft_generation: 0,
                payload: SettingsOperationPayload::Config(Err("stale failure".to_string())),
            })
            .unwrap();
        assert!(!state.pump_operations().changed);
        assert_eq!(state.pending_operations.get(&scope), Some(&2));
        assert!(!state.field_errors.contains_key("test-field"));

        result_tx
            .send(SettingsOperationResult {
                request_id: 2,
                scope: scope.clone(),
                draft_generation: 0,
                payload: SettingsOperationPayload::Config(Err("recoverable failure".to_string())),
            })
            .unwrap();
        assert!(state.pump_operations().changed);
        assert!(!state.pending_operations.contains_key(&scope));
        assert_eq!(
            state.field_errors.get("test-field").map(String::as_str),
            Some("recoverable failure")
        );
    }

    #[test]
    fn duplicate_destructive_operation_is_ignored_while_scope_is_pending() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let scope = SettingsOperationScope::Agents;
        let first_request_id = 41;
        state
            .pending_operations
            .insert(scope.clone(), first_request_id);
        state.agent_action_status = Some("Deleting agent…".to_string());
        let next_request_id = state.next_operation_request_id;

        let queued = state.queue_operation(
            backend,
            None,
            scope.clone(),
            SettingsOperation::DeleteAgent {
                id: "already-pending".to_string(),
            },
        );

        assert!(!queued);
        assert_eq!(
            state.pending_operations.get(&scope),
            Some(&first_request_id)
        );
        assert_eq!(state.next_operation_request_id, next_request_id);
        assert!(state.operation_lane.is_none());
        assert_eq!(
            state.agent_action_status.as_deref(),
            Some("Deleting agent…")
        );
        assert!(state.agent_action_error.is_none());
    }

    #[test]
    fn reconnect_rebind_invalidates_old_lane_and_preserves_drafts_for_retry() {
        let old_backend = Backend::local_fallback();
        let new_backend = Backend::local_fallback();
        let mut state = SettingsState::new(&old_backend);
        let old_config = state.config.clone();
        let draft_key = "config-draft".to_string();
        state.set_field_draft(
            SettingsOperationDomain::Config,
            draft_key.clone(),
            "unsaved value".to_string(),
        );
        state.create_agent();
        state.set_agent_id("reconnect-draft".to_string());
        state.set_agent_name("Keep across reconnect".to_string());
        state
            .pending_operations
            .insert(SettingsOperationScope::ConfigField(draft_key.clone()), 7);
        state
            .pending_operations
            .insert(SettingsOperationScope::Agents, 8);
        let (request_tx, _request_rx) = mpsc::channel();
        let (_result_tx, result_rx) = mpsc::channel();
        state.operation_lane = Some(SettingsOperationLane {
            tx: request_tx,
            rx: result_rx,
        });
        state.initial_load_started = true;

        state.rebind_backend(&new_backend);

        assert!(!Arc::ptr_eq(&old_config, &state.config));
        assert!(state.operation_lane.is_none());
        assert!(state.pending_operations.is_empty());
        assert!(!state.initial_load_started);
        assert_eq!(
            state.field_drafts.get(&draft_key).map(String::as_str),
            Some("unsaved value")
        );
        assert_eq!(
            state.field_errors.get(&draft_key).map(String::as_str),
            Some("Connection changed before the action completed; retry it.")
        );
        assert_eq!(
            state.agent_action_error.as_deref(),
            Some("Connection changed before the action completed; retry it.")
        );

        let reconnect_server_generations = state.server_generations;
        let reconnect_draft_generations = state.draft_generations;
        state.merge_initial_load(SettingsInitialLoadResult {
            settings: SettingsState::new(&new_backend),
            server_generations: reconnect_server_generations,
            draft_generations: reconnect_draft_generations,
        });
        assert_eq!(state.agent_form.id, "reconnect-draft");
        assert_eq!(state.agent_form.name, "Keep across reconnect");
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

        state.set_config_value_draft(idx, "not-valid-json {{".to_string());
        let (scope, operation) = state
            .config_value_operation(idx)
            .expect("editable config operation");
        apply_test_operation(&mut state, &backend, scope, operation);
        assert!(
            state.field_errors.contains_key(&config_field_key(&key)),
            "an invalid value records a parse error against the row"
        );
        assert_eq!(
            state
                .field_drafts
                .get(&config_field_key(&key))
                .map(String::as_str),
            Some("not-valid-json {{"),
            "failed saves retain the editable draft for retry"
        );

        // The service rejected the write, so a fresh resolve keeps the prior value.
        reload_config_state(&mut state);
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
            ConfigPathSegment::Field("modules".to_string()),
            ConfigPathSegment::Field("observe".to_string()),
            ConfigPathSegment::Field("enabled".to_string()),
        ];
        state
            .set_config_schema_value_draft(idx, path.clone(), Value::Bool(true))
            .expect("schema draft");
        let (scope, operation) = state
            .config_schema_save_operation(idx)
            .expect("whole-definition schema operation");
        apply_test_operation(&mut state, &backend, scope, operation);
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
    fn schema_config_draft_composes_sibling_edits_into_one_save() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let idx = state
            .config_items
            .iter()
            .position(|item| item.key == sinomo_runtime::module::MODULE_NODE_CONFIG_KEY)
            .expect("module node config item");
        let observe = vec![
            ConfigPathSegment::Field("modules".to_string()),
            ConfigPathSegment::Field("observe".to_string()),
            ConfigPathSegment::Field("enabled".to_string()),
        ];
        let dfs = vec![
            ConfigPathSegment::Field("modules".to_string()),
            ConfigPathSegment::Field("dfs".to_string()),
            ConfigPathSegment::Field("enabled".to_string()),
        ];

        state
            .set_config_schema_value_draft(idx, observe.clone(), Value::Bool(true))
            .expect("observe draft");
        state
            .set_config_schema_value_draft(idx, dfs.clone(), Value::Bool(false))
            .expect("dfs draft");
        let draft = state.config_schema_draft_value(&state.config_items[idx]);
        assert_eq!(
            config_value_at_path(draft, &observe).and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            config_value_at_path(draft, &dfs).and_then(Value::as_bool),
            Some(false)
        );

        let (scope, operation) = state
            .config_schema_save_operation(idx)
            .expect("one validated save");
        let SettingsOperation::SetConfigDocument { value, .. } = &operation else {
            panic!("schema save must write the whole typed definition");
        };
        assert_eq!(
            config_value_at_path(value, &observe).and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            config_value_at_path(value, &dfs).and_then(Value::as_bool),
            Some(false)
        );

        apply_test_operation(&mut state, &backend, scope, operation);
        assert!(
            !state
                .config_drafts
                .contains_key(sinomo_runtime::module::MODULE_NODE_CONFIG_KEY)
        );
    }

    #[test]
    fn schema_path_helpers_edit_nested_lists_and_maps() {
        let mut value = serde_json::json!({
            "services": {
                "primary": {
                    "targets": [{"url": "one"}]
                }
            }
        });
        let second_url = vec![
            ConfigPathSegment::Field("services".to_string()),
            ConfigPathSegment::Field("primary".to_string()),
            ConfigPathSegment::Field("targets".to_string()),
            ConfigPathSegment::Index(1),
            ConfigPathSegment::Field("url".to_string()),
        ];
        set_json_path_value(&mut value, &second_url, Value::String("two".to_string()))
            .expect("set nested list value");
        assert_eq!(
            config_value_at_path(&value, &second_url).and_then(Value::as_str),
            Some("two")
        );

        let first = vec![
            ConfigPathSegment::Field("services".to_string()),
            ConfigPathSegment::Field("primary".to_string()),
            ConfigPathSegment::Field("targets".to_string()),
            ConfigPathSegment::Index(0),
        ];
        remove_json_path_value(&mut value, &first).expect("remove first list item");
        let now_first_url = vec![
            ConfigPathSegment::Field("services".to_string()),
            ConfigPathSegment::Field("primary".to_string()),
            ConfigPathSegment::Field("targets".to_string()),
            ConfigPathSegment::Index(0),
            ConfigPathSegment::Field("url".to_string()),
        ];
        assert_eq!(
            config_value_at_path(&value, &now_first_url).and_then(Value::as_str),
            Some("two")
        );

        let config_key = "test.list";
        let old_first = vec![ConfigPathSegment::Index(0)];
        let old_second = vec![ConfigPathSegment::Index(1)];
        let mut drafts = HashMap::from([
            (
                config_schema_field_key(config_key, &old_first),
                "one".to_string(),
            ),
            (
                config_schema_field_key(config_key, &old_second),
                "two".to_string(),
            ),
        ]);
        clear_config_schema_auxiliary_path(&mut drafts, config_key, &old_first);
        move_config_schema_auxiliary_path(&mut drafts, config_key, &old_second, &old_first);
        assert_eq!(
            drafts
                .get(&config_schema_field_key(config_key, &old_first))
                .map(String::as_str),
            Some("two")
        );
        assert!(!drafts.contains_key(&config_schema_field_key(config_key, &old_second)));
    }

    #[test]
    fn schema_read_only_definition_rejects_drafts() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let idx = state
            .config_items
            .iter()
            .position(|item| item.schema.is_some())
            .expect("schema-backed config item");
        state.config_items[idx].read_only = true;
        let result = state.set_config_schema_value_draft(idx, Vec::new(), Value::Null);
        assert_eq!(
            result.expect_err("read-only draft must fail"),
            "this configuration is read-only"
        );
        assert!(state.config_schema_save_operation(idx).is_err());
        assert!(state.config_drafts.is_empty());
    }

    #[test]
    fn module_id_matching_normalizes_legacy_aliases() {
        let legacy = "module.sandbox.wasix";
        let canonical = sinomo_runtime::core::Id::from_legacy_named(legacy)
            .expect("canonical module id")
            .to_string();
        assert!(module_ids_match(&canonical, legacy));
        assert!(module_ids_match(legacy, &canonical));
        assert!(!module_ids_match(&canonical, "module.other"));
        assert!(!module_ids_match("not valid / id", legacy));
    }

    #[test]
    fn toggle_config_no_ops_on_a_non_bool_key() {
        let Some((state, idx)) = editable_state() else {
            return;
        };
        let before = state.config_items[idx].edit_value.clone();
        assert!(
            state.config_items[idx].bool_value.is_none(),
            "non-boolean config is not eligible for a toggle operation"
        );
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
            state.set_config_value_draft(idx, "true".to_string());
            assert!(
                state.field_errors.is_empty(),
                "a boolean key is not text-edited (no error recorded)"
            );
            assert_eq!(
                state.config_items[idx].value, before,
                "the boolean value is untouched by the text writer"
            );
            assert!(state.config_value_operation(idx).is_none());
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
    fn provider_health_result_controls_verified_state_and_catalog() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let provider = "openai".to_string();
        let account = "default".to_string();
        state.mark_operation_pending(&SettingsOperation::TestProviderConnection {
            provider: provider.clone(),
            account: account.clone(),
        });
        assert!(state.provider_health(&provider, &account).checking);

        let (request_tx, _request_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        state.operation_lane = Some(SettingsOperationLane {
            tx: request_tx,
            rx: result_rx,
        });
        state
            .pending_operations
            .insert(SettingsOperationScope::Providers, 41);
        result_tx
            .send(SettingsOperationResult {
                request_id: 41,
                scope: SettingsOperationScope::Providers,
                draft_generation: 0,
                payload: SettingsOperationPayload::ProviderHealth(ProviderHealthOperationResult {
                    provider: provider.clone(),
                    account: account.clone(),
                    result: Ok(sinomo_lm::LanguageModelProviderHealthSnapshot {
                        provider: provider.clone(),
                        account: account.clone(),
                        checked_at_unix: 1_750_000_000,
                        last_verified_at_unix: Some(1_750_000_000),
                        healthy: true,
                        error: None,
                        models: vec!["gpt-5.5".to_string()],
                        model_discovery_error: None,
                        usage: Some(sinomo_lm::LanguageModelProviderUsage {
                            provider: provider.clone(),
                            plan: Some("pro".to_string()),
                            captured_at_unix: 1_750_000_000,
                            limits: Vec::new(),
                            credits: None,
                        }),
                        usage_error: None,
                        stale: false,
                    }),
                }),
            })
            .expect("health result");

        assert!(state.pump_operations().changed);
        let health = state.provider_health(&provider, &account);
        assert!(!health.checking);
        assert_eq!(health.last_verified_at_unix, Some(1_750_000_000));
        assert_eq!(health.models, vec!["gpt-5.5".to_string()]);
        assert_eq!(provider_health_presentation(&health).label, "Verified");
        assert_eq!(
            health.usage.and_then(|usage| usage.plan),
            Some("pro".to_string())
        );
    }

    #[test]
    fn stored_credential_is_not_described_as_verified() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        add_provider_account(
            state.config.as_ref(),
            "openai",
            "default",
            "sk-test",
            "gpt-5.2-codex",
            "",
        )
        .expect("provider account");
        state.provider_forms = provider_forms(state.config.as_ref());
        let form = state.provider_forms.first().expect("provider form");

        assert_eq!(provider_auth_summary(form), "API key stored");
        assert_eq!(
            provider_health_presentation(&state.provider_health(&form.provider, &form.account))
                .label,
            "Not verified"
        );
    }

    #[test]
    fn provider_usage_normalizes_percentages_and_threshold_tones() {
        let mut limit = sinomo_lm::LanguageModelUsageLimit {
            id: "weekly".to_string(),
            name: Some("Weekly limit".to_string()),
            scope: None,
            unlimited: None,
            allowed: Some(true),
            exhausted: None,
            remaining: None,
            entitlement: None,
            used_percent: None,
            remaining_percent: Some(36.0),
            window_seconds: None,
            reset_at_unix: None,
        };

        assert_eq!(provider_usage_used_percent(&limit), Some(64.0));
        assert_eq!(provider_usage_tone(&limit, 64.0), SemanticTone::Accent);
        assert_eq!(provider_usage_tone(&limit, 80.0), SemanticTone::Warning);
        assert_eq!(provider_usage_tone(&limit, 95.0), SemanticTone::Danger);

        limit.exhausted = Some(true);
        assert_eq!(provider_usage_used_percent(&limit), Some(100.0));
        assert_eq!(provider_usage_tone(&limit, 10.0), SemanticTone::Danger);
        assert_eq!(format_compact_duration(8_040), "2h 14m");
        assert_eq!(format_relative_age(1_000, 1_120), "2m ago");
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
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Skills,
            SettingsOperation::ImportSkill {
                path: first_path.clone(),
            },
        );
        assert!(state.skill_action_error.is_none());
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
        let selected = state.skill_selected.clone();
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Skills,
            SettingsOperation::RefreshSkills {
                include_all: true,
                selected,
            },
        );
        let v2 = state
            .skills
            .iter()
            .position(|skill| skill.id == "shortcut-skill" && skill.version == "2.0.0")
            .expect("v2 skill");
        let selected = {
            let skill = &state.skills[v2];
            (skill.id.clone(), skill.version.clone())
        };
        state.skill_selected = Some(selected.clone());
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Skills,
            SettingsOperation::RefreshSkills {
                include_all: true,
                selected: Some(selected.clone()),
            },
        );
        assert!(
            !state
                .skill_detail
                .as_ref()
                .expect("v2 detail")
                .summary
                .active
        );
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Skills,
            SettingsOperation::ActivateSkill {
                skill_id: selected.0.clone(),
                version: selected.1.clone(),
                include_all: true,
            },
        );
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
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Skills,
            SettingsOperation::ExportSkill {
                skill_id: selected.0,
                version: selected.1,
                output: export_path.clone(),
                include_all: true,
            },
        );
        assert!(state.skill_action_error.is_none());
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
    fn settings_skill_reorder_preserves_virtual_row_ids() -> sui::Result<()> {
        let (backend, root) = temp_skill_backend("skills-virtual-reorder");
        for name in ["virtual-skill-alpha", "virtual-skill-beta"] {
            let path = write_skill_file(&root, name, "Virtual collection regression fixture.");
            sinomo_cli::app_api::import_skill(backend.data_dir.as_ref(), &path, None, None, true)
                .expect("import virtual collection skill");
        }
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
                    .title("Settings virtual skills")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let row_ids = |snapshot: &sui_testing::WindowSnapshot| {
            ["virtual-skill-alpha @ 1.0.0", "virtual-skill-beta @ 1.0.0"]
                .into_iter()
                .map(|name| {
                    let id = snapshot
                        .accessibility
                        .nodes
                        .iter()
                        .find(|node| {
                            node.role == sui::SemanticsRole::ListItem
                                && node.name.as_deref() == Some(name)
                        })
                        .expect("virtual skill row")
                        .id;
                    (name, id)
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before = row_ids(&window.snapshot()?);
        {
            let mut settings = shared.settings_mut();
            let alpha = settings
                .skills
                .iter()
                .position(|skill| skill.id == "virtual-skill-alpha")
                .expect("alpha skill");
            let beta = settings
                .skills
                .iter()
                .position(|skill| skill.id == "virtual-skill-beta")
                .expect("beta skill");
            settings.skills.swap(alpha, beta);
        }
        window.run_until_idle()?;
        assert_eq!(row_ids(&window.snapshot()?), before);

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn agent_settings_state_saves_renames_and_deletes_custom_agent() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);

        state.create_agent();
        state.set_agent_id("docs-agent".to_string());
        state.set_agent_name("Docs agent".to_string());
        state.set_agent_description("Writes project documentation".to_string());
        state.set_agent_role_index(2);
        state.set_agent_system_prompt("Write concise docs for the current project.".to_string());
        state.set_agent_provider("openai".to_string());
        state.set_agent_model("gpt-5".to_string());
        state.set_agent_temperature("0.1".to_string());
        state.set_agent_max_steps("4".to_string());
        state.set_agent_skills_text("docs.skill\nreview.skill".to_string());

        let operation = state.agent_save_operation().expect("save operation");
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Agents,
            operation,
        );
        assert_eq!(
            state.agent_action_status.as_deref(),
            Some("Saved custom agent docs-agent")
        );
        assert!(
            state
                .agent_snapshot
                .agents
                .iter()
                .any(|agent| agent.id == "docs-agent" && agent.editable),
            "saved custom agent should appear in the settings catalog"
        );
        let summaries = agent_summaries(backend.data_dir.as_ref());
        assert!(
            summaries.iter().any(|agent| {
                agent.key == "docs-agent" && agent.description == "Writes project documentation"
            }),
            "saved custom agent should feed the chat agent catalog"
        );

        state.set_agent_id("docs-agent-v2".to_string());
        let operation = state.agent_save_operation().expect("rename operation");
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Agents,
            operation,
        );
        let summaries = agent_summaries(backend.data_dir.as_ref());
        assert!(summaries.iter().any(|agent| agent.key == "docs-agent-v2"));
        assert!(
            !summaries.iter().any(|agent| agent.key == "docs-agent"),
            "renaming a custom agent should remove the previous JSON definition"
        );

        state.confirm_agent_delete();
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Agents,
            SettingsOperation::DeleteAgent {
                id: "docs-agent-v2".to_string(),
            },
        );
        let summaries = agent_summaries(backend.data_dir.as_ref());
        assert!(!summaries.iter().any(|agent| agent.key == "docs-agent-v2"));
    }

    #[test]
    fn agent_settings_state_imports_exports_and_reports_readiness() {
        let backend = Backend::local_fallback();
        let package_path = backend.data_dir.root().join("settings-agent.json");
        let export_dir = backend.data_dir.root().join("agent-exports");
        let definition = AgentDefinition::custom(
            "settings-package-agent",
            AgentRole::Background,
            "Verify lifecycle actions.",
        );
        std::fs::write(
            &package_path,
            serde_json::to_vec_pretty(&definition).expect("serialize agent package"),
        )
        .expect("write agent package");
        let mut state = SettingsState::new(&backend);
        state.set_agent_import_path(package_path.display().to_string());

        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Agents,
            SettingsOperation::ImportAgent { path: package_path },
        );
        assert_eq!(
            state.agent_selected_id.as_deref(),
            Some("settings-package-agent")
        );
        assert!(state.agent_import_path.is_empty());

        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Agents,
            SettingsOperation::ValidateAgent {
                id: "settings-package-agent".to_string(),
            },
        );
        assert!(
            state
                .agent_validation_report
                .as_ref()
                .is_some_and(AgentValidationReport::is_ready)
        );
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Agents,
            SettingsOperation::TestAgent {
                id: "settings-package-agent".to_string(),
            },
        );
        assert!(
            state
                .agent_validation_report
                .as_ref()
                .is_some_and(|report| report.checks.len() >= 5 && report.is_ready())
        );

        state.set_agent_export_path(export_dir.display().to_string());
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Agents,
            SettingsOperation::ExportAgent {
                id: "settings-package-agent".to_string(),
                output: export_dir.clone(),
            },
        );
        assert!(export_dir.join("settings-package-agent.json").is_file());
    }

    #[test]
    fn settings_agents_section_renders_management_controls() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let mut definition =
            AgentDefinition::custom("render-agent", AgentRole::Main, "Render settings detail.");
        definition.description = "Rendered custom agent".to_string();
        backend
            .save_custom_agent_definition(&definition, None)
            .expect("seed custom agent");
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Agents);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings agents")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window.get_by_text("Agents").expect().to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-id")
            .expect()
            .to_have_value("render-agent")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-description")
            .expect()
            .to_have_value("Rendered custom agent")?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("New agent")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Duplicate")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Refresh")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Package management")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Import package")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Export selected")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Validate")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Test readiness")
            .expect()
            .to_be_visible()?;

        Ok(())
    }

    #[test]
    fn settings_agent_reorder_preserves_virtual_row_ids() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        for (id, name) in [
            ("virtual-agent-alpha", "Virtual agent alpha"),
            ("virtual-agent-beta", "Virtual agent beta"),
        ] {
            let mut definition = AgentDefinition::custom(id, AgentRole::Main, "Retained row test");
            definition.name = name.to_string();
            backend
                .save_custom_agent_definition(&definition, None)
                .expect("seed virtual collection agent");
        }
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Agents);
        }
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings virtual agents")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let row_ids = |snapshot: &sui_testing::WindowSnapshot| {
            ["Virtual agent alpha", "Virtual agent beta"]
                .into_iter()
                .map(|name| {
                    let id = snapshot
                        .accessibility
                        .nodes
                        .iter()
                        .find(|node| {
                            node.role == sui::SemanticsRole::ListItem
                                && node.name.as_deref() == Some(name)
                        })
                        .expect("virtual agent row")
                        .id;
                    (name, id)
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before = row_ids(&window.snapshot()?);
        {
            let mut settings = shared.settings_mut();
            let alpha = settings
                .agent_snapshot
                .agents
                .iter()
                .position(|agent| agent.id == "virtual-agent-alpha")
                .expect("alpha agent");
            let beta = settings
                .agent_snapshot
                .agents
                .iter()
                .position(|agent| agent.id == "virtual-agent-beta")
                .expect("beta agent");
            settings.agent_snapshot.agents.swap(alpha, beta);
        }
        window.run_until_idle()?;
        assert_eq!(row_ids(&window.snapshot()?), before);
        Ok(())
    }

    #[test]
    fn settings_modules_section_names_and_separates_lifecycle_states() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let associated_index;
        let unmatched_index;
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::Modules);
            let node_summary = crate::backend::modules::ModuleConfigNodeSummary {
                node_id: "node-technical-id".to_string(),
                display_name: "Studio node".to_string(),
                availability: "online".to_string(),
                local: true,
            };
            settings.module_selected_node_id = Some(node_summary.node_id.clone());
            settings.module_snapshot = ModuleConfigSnapshot {
                cluster_config: sinomo_runtime::module::ModuleRuntimeConfig::default(),
                selected_node: Some(crate::backend::modules::ModuleConfigNode {
                    summary: node_summary.clone(),
                    config: sinomo_runtime::module::ModuleRuntimeConfig::default(),
                    error: None,
                }),
                nodes: vec![node_summary],
                modules: vec![crate::backend::modules::ModuleConfigModuleSummary {
                    module_id: "module.sandbox.wasix".to_string(),
                    label: "WASIX Sandbox".to_string(),
                    backend: "wasix".to_string(),
                    detail: "WASIX sandbox runtime module".to_string(),
                    desired_enabled: Some(true),
                    effective_enabled: false,
                    effective_source: ModuleRuntimeConfigSource::Node,
                    observed: ModuleObservedState::Running,
                    observed_detail: "Runtime process is active".to_string(),
                }],
                warnings: Vec::new(),
            };
            associated_index = settings.config_items.len();
            let mut associated = test_config_item(
                "test.sandbox.settings",
                ConfigEditKind::Json,
                r#"{"enabled":true,"endpoint":"https://sandbox.test"}"#,
            );
            associated.module_id = Some(
                sinomo_runtime::core::Id::from_legacy_named("module.sandbox.wasix")
                    .expect("canonical module id")
                    .to_string(),
            );
            associated.schema = Some(ConfigSchema::object(
                "SandboxSettings",
                "Sandbox module settings",
                vec![
                    ConfigSchemaField::new(
                        "enabled",
                        "Enabled",
                        "Enable the sandbox integration.",
                        ConfigSchemaValue::bool(),
                    ),
                    ConfigSchemaField::new(
                        "endpoint",
                        "Endpoint",
                        "Sandbox service endpoint.",
                        ConfigSchemaValue::string(),
                    ),
                ],
            ));
            settings.config_items.push(associated);

            unmatched_index = settings.config_items.len();
            let mut unmatched = test_config_item(
                "test.headless.settings",
                ConfigEditKind::Json,
                r#"{"workspace":"/tmp/evolution"}"#,
            );
            unmatched.module_id = Some(
                sinomo_runtime::core::Id::from_legacy_named("module.without.lifecycle.card")
                    .expect("canonical module id")
                    .to_string(),
            );
            unmatched.schema = Some(ConfigSchema::object(
                "HeadlessModuleSettings",
                "Settings for a module without a lifecycle card",
                vec![ConfigSchemaField::new(
                    "workspace",
                    "Workspace",
                    "Module workspace.",
                    ConfigSchemaValue::string(),
                )],
            ));
            settings.config_items.push(unmatched);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings modules")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_text("WASIX Sandbox")
            .expect()
            .to_be_visible()?;
        window.get_by_text("Desired").expect().to_be_visible()?;
        window
            .get_by_text("Effective on Studio node")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Observed on Studio node")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Pending reconciliation")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Technical ID")
            .expect()
            .to_be_visible()?;
        assert!(
            window
                .get_by_text("Settings · test.sandbox.settings")
                .count()?
                >= 1,
            "a canonical config module ID must match its legacy lifecycle card"
        );
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("config-{associated_index}-schema-endpoint"))
            .expect()
            .to_have_value("https://sandbox.test")?;
        window
            .get_by_text("Additional module settings")
            .expect()
            .to_be_visible()?;
        assert!(
            window
                .get_by_text("Settings · test.headless.settings")
                .count()?
                >= 1,
            "module settings without lifecycle cards must remain visible"
        );
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("config-{unmatched_index}-schema-workspace"))
            .expect()
            .to_have_value("/tmp/evolution")?;

        Ok(())
    }

    #[test]
    fn schema_editor_displays_omitted_defaults_without_offering_unset() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let index;
        let item;
        let schema;
        {
            let mut settings = shared.settings_mut();
            index = settings.config_items.len();
            let mut config_item = test_config_item(
                "test.defaults",
                ConfigEditKind::Json,
                r#"{"present":"custom","token":"sk-stored-secret","exact_limit":18446744073709551615}"#,
            );
            config_item.schema = Some(ConfigSchema::object(
                "DefaultSettings",
                "",
                vec![
                    ConfigSchemaField::new(
                        "mode",
                        "Mode",
                        "",
                        ConfigSchemaValue::string_choices(vec![
                            sinomo_runtime::config::ConfigSchemaChoice::new("auto", "Automatic"),
                            sinomo_runtime::config::ConfigSchemaChoice::new("manual", "Manual"),
                        ]),
                    )
                    .optional()
                    .with_default_value(Value::String("auto".to_string())),
                    ConfigSchemaField::new("present", "Present", "", ConfigSchemaValue::string())
                        .optional()
                        .with_default_value(Value::String("default".to_string())),
                    ConfigSchemaField::new(
                        "limit_ms",
                        "Latency limit",
                        "Optional latency limit.",
                        ConfigSchemaValue::u64(None),
                    )
                    .optional(),
                    ConfigSchemaField::new(
                        "exact_limit",
                        "Exact integer limit",
                        "A full-width integer must not round through f64.",
                        ConfigSchemaValue::u64(None),
                    ),
                    ConfigSchemaField::new(
                        "optional_token",
                        "Optional token",
                        "Optional replacement credential.",
                        ConfigSchemaValue::secret_string(),
                    )
                    .optional(),
                    ConfigSchemaField::new(
                        "token",
                        "Token",
                        "Replacement credential.",
                        ConfigSchemaValue::secret_string(),
                    ),
                ],
            ));
            settings.config_items.push(config_item);
            item = settings.config_items[index].clone();
            schema = item.schema.clone().expect("schema");
        }
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let runtime =
            sui::Application::new()
                .window(sui::WindowBuilder::new().title("Schema defaults").root(
                    config_schema_editor(&shared, &reader, index, &item, &schema),
                ))
                .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::ComboBox)
            .with_name(format!("config-{index}-schema-mode"))
            .expect()
            .to_have_value("Automatic")?;
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Unset Mode")
                .count()?,
            0,
            "an inherited default is not a stored field and must not offer Unset"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Unset Present")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Set Latency limit")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("config-{index}-schema-exact_limit"))
            .expect()
            .to_have_value("18446744073709551615")?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Set Optional token")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("config-{index}-schema-token"))
            .expect()
            .to_have_value("")?;
        assert_eq!(
            window.get_by_text("sk-stored-secret").count()?,
            0,
            "stored schema secrets must never be copied into the rendered editor"
        );
        assert_eq!(
            config_schema_inline_summary(
                &Value::String(String::new()),
                &ConfigSchemaValue::secret_string(),
            ),
            "unset"
        );

        Ok(())
    }

    #[test]
    fn copyable_identifier_row_copies_the_full_unshortened_value() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let full_id = "node-01jz7x9m4k2p8q6r5s3t1v0w9y";
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Copy identifier")
                    .root(copyable_text_row(
                        &reader,
                        "copy-node-id",
                        "Node ID",
                        full_id,
                    )),
            )
            .build()?;
        let clipboard = runtime.clipboard();
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy Node ID")
            .click()?;
        assert_eq!(clipboard.text().as_deref(), Some(full_id));

        clipboard.set_text("context menu did not copy");
        let field = window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("copy-node-id");
        field.focus()?;
        let mut select_all = sui::KeyboardEvent::new("a", sui::KeyState::Pressed);
        select_all.modifiers.control = true;
        field.dispatch_event(sui::Event::Keyboard(select_all))?;
        let field_node = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == sui::SemanticsRole::TextInput
                    && node.name.as_deref() == Some("copy-node-id")
            })
            .expect("copyable Node ID semantics");
        let field_center = sui::Point::new(
            field_node.bounds.x() + field_node.bounds.width() * 0.5,
            field_node.bounds.y() + field_node.bounds.height() * 0.5,
        );
        let mut secondary_down = sui::PointerEvent::new(sui::PointerEventKind::Down, field_center);
        secondary_down.pointer_id = 1;
        secondary_down.button = Some(sui::PointerButton::Secondary);
        secondary_down.buttons = sui::PointerButtons::new(2);
        window
            .root()
            .dispatch_event(sui::Event::Pointer(secondary_down))?;
        window
            .get_by_role(sui::SemanticsRole::MenuItem)
            .with_name("Copy")
            .click()?;
        assert_eq!(clipboard.text().as_deref(), Some(full_id));

        Ok(())
    }

    #[test]
    fn appearance_exposes_text_scaling_without_a_global_ui_size() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Appearance);
        }
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Appearance scaling")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::ComboBox)
            .with_name("text-scale")
            .expect()
            .to_have_value("Standard")?;
        window
            .get_by_role(sui::SemanticsRole::ComboBox)
            .with_name("ui-scale")
            .expect()
            .to_have_count(0)?;
        window.get_by_text("Typography").expect().to_be_visible()?;

        Ok(())
    }

    #[test]
    fn provider_model_edit_reports_saved_status_and_persists() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend.clone()));
        {
            let config = shared.settings().config.clone();
            add_provider_account(config.as_ref(), "openai", "work", "sk-work", "gpt-5", "")
                .expect("add provider account");
        }
        reload_config_state(&mut shared.settings_mut());
        let before_signature = provider_section_signature(&shared);

        {
            let mut settings = shared.settings_mut();
            settings.set_provider_field_draft(0, ProviderField::Model, "gpt-5.1".to_string());
            let operation = settings
                .provider_field_operation(0, ProviderField::Model)
                .expect("provider field operation");
            apply_test_operation(
                &mut settings,
                &backend,
                SettingsOperationScope::Providers,
                operation,
            );
        }

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
        reload_config_state(&mut state);
        let before_signature = provider_section_signature_for_state(&state);

        let form = state.provider_forms[1].clone();
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Providers,
            SettingsOperation::SetProviderChatDefault {
                provider: form.provider,
                account: form.account,
            },
        );

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

        let node_id = state
            .module_selected_node_id
            .clone()
            .expect("selected node id");
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Modules,
            SettingsOperation::SetNodeModuleOverride {
                node_id,
                module_id: sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID.to_string(),
                enabled: Some(true),
            },
        );
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
        reload_config_state(&mut state);
        assert_eq!(state.provider_forms.len(), 1);
        assert!(!state.provider_remove_confirming("openai", "work"));

        state.confirm_provider_remove(0);
        assert!(state.provider_remove_confirming("openai", "work"));

        state.cancel_provider_remove();
        assert!(!state.provider_remove_confirming("openai", "work"));

        state.confirm_provider_remove(0);
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Providers,
            SettingsOperation::RemoveProvider {
                provider: "openai".to_string(),
                account: "work".to_string(),
            },
        );
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
        reload_config_state(&mut state);

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
        reload_config_state(&mut state);
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
            reload_config_state(settings);
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
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Test connection")
            .expect()
            .to_be_visible()?;
        assert!(
            window.get_by_text("Not verified").count()? >= 1,
            "credential presence must not be presented as verified health"
        );
        assert!(
            window.get_by_text("API key stored").count()? >= 1,
            "the authentication row should report credential presence separately"
        );

        Ok(())
    }

    #[test]
    fn provider_card_renders_health_badge_usage_meter_and_discovered_model() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let now = current_settings_unix_timestamp();
        {
            let config = shared.settings().config.clone();
            add_provider_account(config.as_ref(), "openai", "work", "sk-work", "gpt-5", "")
                .expect("add provider account");
        }
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            reload_config_state(settings);
            settings.provider_health.insert(
                provider_account_key("openai", "work"),
                ProviderHealthState {
                    checking: false,
                    last_verified_at_unix: Some(now - 720),
                    last_checked_at_unix: Some(now - 720),
                    error: None,
                    models: vec!["gpt-5".to_string(), "gpt-5-mini".to_string()],
                    model_discovery_error: None,
                    usage: Some(sinomo_lm::LanguageModelProviderUsage {
                        provider: "openai".to_string(),
                        plan: Some("Pro".to_string()),
                        captured_at_unix: now as u64,
                        limits: vec![sinomo_lm::LanguageModelUsageLimit {
                            id: "five-hour".to_string(),
                            name: Some("Five-hour limit".to_string()),
                            scope: None,
                            unlimited: None,
                            allowed: Some(true),
                            exhausted: Some(false),
                            remaining: None,
                            entitlement: None,
                            used_percent: Some(64.0),
                            remaining_percent: Some(36.0),
                            window_seconds: Some(18_000),
                            reset_at_unix: Some(now + 8_040),
                        }],
                        credits: None,
                    }),
                    usage_error: None,
                    stale: false,
                },
            );
            settings.open(SettingsSection::Providers);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings provider usage")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(window.get_by_text("Verified").count()? >= 1);
        assert!(window.get_by_text("Five-hour limit · 64% used").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::ProgressBar)
            .with_name("Five-hour limit usage")
            .expect()
            .to_have_value("64")?;
        window
            .get_by_role(sui::SemanticsRole::ComboBox)
            .with_name("provider-0-model-select")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-model")
            .expect()
            .to_be_hidden()?;

        Ok(())
    }

    #[test]
    fn provider_card_uses_dropdown_for_known_catalog_before_connection_test() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let config = shared.settings().config.clone();
            add_provider_account(
                config.as_ref(),
                "deepseek",
                "default",
                "sk-test",
                sinomo_lm::DEEPSEEK_DEFAULT_MODEL,
                "",
            )
            .expect("add provider account");
        }
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            reload_config_state(settings);
            settings.open(SettingsSection::Providers);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings known provider models")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::ComboBox)
            .with_name("provider-0-model-select")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-model")
            .expect()
            .to_be_hidden()?;

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
            reload_config_state(settings);
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

        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            if shared
                .settings()
                .provider_action_status
                .as_ref()
                .is_some_and(|status| status.message == "Chat default saved to lm.providers")
            {
                break;
            }
            if pump_background_operations(&shared) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            shared
                .settings()
                .provider_action_status
                .as_ref()
                .map(|status| status.message.as_str()),
            Some("Chat default saved to lm.providers")
        );

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
            reload_config_state(settings);
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
            .with_name("Settings")
            .click()?;

        assert_ne!(
            collapsed_signature,
            provider_section_signature(&shared),
            "provider disclosure state must invalidate the retained section"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide settings")
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
            reload_config_state(settings);
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
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("about-app-surface-id")
            .expect()
            .to_have_value("node_about_test")?;
        assert!(window.get_by_text("voter app surface").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("about-daemon-node-id")
            .expect()
            .to_have_value("daemon_node_about_test")?;
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
            module_id: None,
            description: "test setting".to_string(),
            value: raw.to_string(),
            source: "cluster",
            bool_value: None,
            edit_kind: kind,
            edit_value: raw.to_string(),
            raw_value,
            schema: None,
            read_only: false,
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
    fn schema_default_value_recurses_and_materializes_declared_field_defaults() {
        let schema =
            ConfigSchemaValue::object(vec![
                ConfigSchemaField::new("mode", "Mode", "", ConfigSchemaValue::string())
                    .optional()
                    .with_default_value(Value::String("auto".to_string())),
                ConfigSchemaField::new("enabled", "Enabled", "", ConfigSchemaValue::bool()),
                ConfigSchemaField::new(
                    "targets",
                    "Targets",
                    "",
                    ConfigSchemaValue::list(ConfigSchemaValue::object(vec![
                        ConfigSchemaField::new("url", "URL", "", ConfigSchemaValue::string()),
                    ])),
                ),
                ConfigSchemaField::new("note", "Note", "", ConfigSchemaValue::string()).optional(),
            ]);

        assert_eq!(
            schema_default_value(&schema),
            serde_json::json!({"mode": "auto", "enabled": false, "targets": []})
        );
        assert!(
            config_schema_value_summary(&serde_json::json!({}), &schema, "TestSettings")
                .contains("Mode: auto"),
            "summaries should project declared defaults for omitted fields"
        );
    }

    #[test]
    fn config_rebuild_signature_tracks_same_length_values_and_schema_metadata() {
        let mut item = test_config_item("test.signature", ConfigEditKind::Json, r#"{"v":1}"#);
        let before = config_item_rebuild_signature(&item);
        item.update_value(serde_json::json!({"v": 2}));
        let value_changed = config_item_rebuild_signature(&item);
        assert_ne!(before, value_changed);

        item.schema = Some(ConfigSchema::object(
            "SignatureSettings",
            "",
            vec![ConfigSchemaField::new(
                "v",
                "Value",
                "",
                ConfigSchemaValue::i64(None),
            )],
        ));
        assert_ne!(value_changed, config_item_rebuild_signature(&item));
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
            settings.config_items.insert(
                0,
                test_config_item(
                    "test.raw-fallback",
                    ConfigEditKind::Json,
                    r#"{"legacy":true}"#,
                ),
            );
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

        // Click the topmost raw-value disclosure at its semantics bounds so the
        // test tracks the layout instead of hardcoding coordinates.
        let snapshot = window.snapshot()?;
        let disclosure = snapshot
            .accessibility
            .nodes
            .iter()
            .filter(|node| {
                node.role == sui::SemanticsRole::Button
                    && node.name.as_deref() == Some("Edit raw value")
            })
            .min_by(|a, b| a.bounds.y().total_cmp(&b.bounds.y()))
            .expect("raw value disclosure present")
            .clone();
        let center = sui::Point::new(
            disclosure.bounds.x() + disclosure.bounds.width() / 2.0,
            disclosure.bounds.y() + disclosure.bounds.height() / 2.0,
        );
        let root = window.root();
        let mut down = sui::PointerEvent::new(sui::PointerEventKind::Down, center);
        down.button = Some(sui::PointerButton::Primary);
        root.dispatch_event(sui::Event::Pointer(down))?;
        let mut up = sui::PointerEvent::new(sui::PointerEventKind::Up, center);
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
        window
            .get_by_text("Raw editor fallback")
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
    fn clustered_settings_exposes_join_another_cluster_migration_flow() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.clustered = true;
            settings.cluster_id = Some("cluster_source".to_string());
            settings.cluster_summary = Some("single-node source cluster".to_string());
            settings.open(SettingsSection::Cluster);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings join another cluster")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(window.get_by_text("Join another cluster").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("cluster-join-link")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Review migration")
            .expect()
            .to_be_visible()?;
        Ok(())
    }

    #[test]
    fn settings_preload_merges_cluster_snapshot_without_erasing_join_draft() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.set_join_invite("sinomo://cluster/join/test".to_string());
        state.join_confirming = true;
        state.leave_confirming = true;
        let mut loaded = SettingsState::new(&backend);
        loaded.clustered = true;
        loaded.cluster_id = Some("cluster_test".to_string());
        loaded.cluster_node_id = Some("node_test".to_string());
        loaded.cluster_summary = Some("cluster cluster_test · authorized".to_string());
        let server_generations = state.server_generations;
        let baseline_drafts = SettingsGenerations::default();
        state.merge_initial_load(SettingsInitialLoadResult {
            settings: loaded,
            server_generations,
            draft_generations: baseline_drafts,
        });

        assert!(state.clustered);
        assert_eq!(
            state.cluster_summary.as_deref(),
            Some("cluster cluster_test · authorized")
        );
        assert!(
            state.join_invite == "sinomo://cluster/join/test",
            "preload must preserve the live join-link draft"
        );
        assert!(state.join_confirming);
        assert!(state.leave_confirming);
    }

    #[test]
    fn settings_preload_preserves_completed_cluster_action_state() {
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
        state.set_cluster_action_status("Invitation generated");
        state
            .server_generations
            .bump(SettingsOperationDomain::Cluster);
        let mut stale = SettingsState::new(&backend);
        stale.cluster_id = Some("cluster_stale".to_string());
        state.merge_initial_load(SettingsInitialLoadResult {
            settings: stale,
            server_generations: SettingsGenerations::default(),
            draft_generations: SettingsGenerations::default(),
        });

        assert!(state.clustered);
        assert_eq!(state.cluster_id.as_deref(), Some("cluster_old"));
        assert!(state.invite_result.is_some());
        assert_eq!(
            state.cluster_action_status.as_deref(),
            Some("Invitation generated")
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
