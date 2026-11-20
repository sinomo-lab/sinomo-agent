//! "Settings" tab — a nested, responsive settings page.
//!
//! Settings are grouped into top-level [`SettingsSection`]s. When the tab is wide
//! enough, the section menu stays pinned in a left sidebar (the "previous level")
//! next to the selected section's detail — a master/detail layout. When it's
//! narrow, it collapses to a single column: the menu, which you drill into to
//! reach a detail page with a back affordance.
//!
//! The page is a fully *composed* `sui-widgets` tree: [`view`] uses a retained
//! `ConstraintView` for its wide/narrow presentations, each assembled from stock
//! `FormSection` / `FormRow` / `TabBar` / `Switch` /
//! `TextInput` / `Select` / `Button` / `ListView` / `ScrollView` widgets. State lives
//! entirely in [`SettingsState`]; controls bind to it via reactive `*_when` readers and
//! snapshot-seed-plus-`on_change` writeback. Async provider device-login status is
//! rebuilt as ordinary selectable text / `Label` / `Link` composition when its typed state
//! changes.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;
#[cfg(not(test))]
use sinomo_client::SkillClient;
use sinomo_client::chat_contract::fallback_agent_summaries;
#[cfg(not(test))]
use sinomo_client::skill_contract::{SkillActivateRequest, SkillExportRequest, SkillImportRequest};
use sinomo_client::skill_contract::{SkillDetail, SkillMutationResult, SkillSummary};
use sui::{
    Align, Alignment, ArrangeCtx, Axis, Checkbox, Command, ConstraintQuery, ConstraintView,
    Constraints, ContextMenu, ControlSize, DefaultTheme, Event, EventCtx, EventPhase, FieldGroup,
    FixedPaneSplit, Flex, FlexItem, FlexWrap, FontWeight, IconButton, IconGlyph, KeyedStack, Label,
    ListItem, ListView, MeasureCtx, MenuItem, NumberInput, Observable, PaintCtx, PasswordInput,
    Presence, ProgressBar, RebuildOnChange, Rect, RichText, ScrollState, ScrollView, SectionLabel,
    Select, SelectionScope, SemanticRegion, SemanticTone, SemanticsCtx, SemanticsRole, Separator,
    Signal, Size, SizedBox, Spinner, Stack, StatusBadge, Surface, SurfaceRole, Switch, SwitchView,
    TextArea, TextCommand, TextDocument, TextInput, ThemeTextToken, TimerToken, Tooltip,
    TooltipPlacement, TrailingSlotRow, VirtualList, Widget, WidgetPod, WidgetPodMutVisitor,
    WidgetPodVisitor, containers::Padding as WidgetPadding,
};

use sinomo_runtime::config::{
    ConfigClient, ConfigKey, ConfigSchema, ConfigSchemaField, ConfigSchemaValue, ConfigWriteTarget,
};
#[cfg(test)]
use sinomo_runtime::dfs::HOST_FILESYSTEM_PROVIDER_INTERFACE_VERSION;
use sinomo_runtime::dfs::{
    SifsMountAttentionKind, SifsMountAttentionNextAction, SifsMountAttentionSummary,
    SifsMountConfig, SifsMountPlatform, SifsMountRuntimePhase, SifsMountWritebackMode,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::LocalNodePermissions;
#[cfg(test)]
use sinomo_runtime::vfs::VfsHandle;

use crate::backend::Backend;
use crate::backend::agents::{
    AgentConfigSnapshot, AgentConfigSummary, AgentDefinition, AgentMcpProtocolEra,
    AgentMcpServerDefinition, AgentOutputFormat, AgentPackageDefinition, AgentReadiness, AgentRole,
    AgentToolDescriptor, AgentToolInvocationMode, AgentToolSpec, AgentValidationReport,
    agent_definition_is_editable, agent_home_sifs_path_for_id, agent_role_label,
    agent_source_label, built_in_agent_config_snapshot, fallback_agent_tool_descriptors,
};
use crate::backend::chat_engine::{AgentSummary, ProviderSummary};
use crate::backend::cluster::{AppClusterInvite, AppClusterStatus, ClusterSnapshot};
use crate::backend::config::{
    CONFIGURABLE_PROVIDERS, ConfigEditKind, ConfigItem, ProviderForm, REASONING_EFFORT_OPTIONS,
    TEXT_VERBOSITY_OPTIONS, add_provider_account, cluster_provider_summaries, load_provider_health,
    mark_provider_health_stale, provider_capabilities, provider_default_model,
    provider_display_name, provider_forms, provider_requires_api_key, provider_uses_device_login,
    reasoning_effort_index, remove_provider_account, remove_provider_health, resolve_items,
    run_provider_device_login, set_bool, set_provider_api_key,
    set_provider_auto_compact_token_limit, set_provider_base_url, set_provider_chat_default,
    set_provider_effort, set_provider_enabled, set_provider_max_context_tokens, set_provider_model,
    set_provider_reasoning_budget, set_provider_text_verbosity, set_provider_vendor_options,
    set_value, test_provider_connection, text_verbosity_index,
};
use crate::backend::modules::{ModuleConfigSnapshot, ModuleObservedState, module_ids_match};
use crate::backend::mounts::{
    SifsMountSettingsSnapshot, validate_connected_host_filesystem_provider_status_at,
    validate_sifs_mount_config,
};
use crate::backend::remote_management::{
    EXTERNAL_INTEGRATIONS_ALLOW_COMMAND, EXTERNAL_INTEGRATIONS_DENY_COMMAND,
    REMOTE_MANAGEMENT_ALLOW_COMMAND, REMOTE_MANAGEMENT_DENY_COMMAND,
    RemoteManagementSettingsSnapshot,
};
use crate::rendering::AppRenderMode;
use crate::state::{Shared, Tab};
use crate::theme::{AppTextScale, AppThemeMode};
use crate::ui::widgets::{self, Restyle};
use sinomo_runtime::module::ModuleRuntimeConfigSource;
use sui::SurfacePalette;

/// Budget for each SIFS-backed section (skills, stored agents) of the full settings
/// snapshot; past this the section reports itself unavailable instead of wedging the
/// preload while the distributed file system is unhealthy.
const SETTINGS_SIFS_LOAD_TIMEOUT: Duration = Duration::from_secs(8);
const SIFS_MOUNT_LIVE_REFRESH_SECS: f64 = 1.0;
const SIFS_MOUNT_ATTENTION_DISPLAY_LIMIT: usize = 16;

/// Below this content width the tab collapses to a single drill-down column.
const WIDE_THRESHOLD: f32 = 700.0;
const SIDEBAR_W: f32 = 276.0;
const SIDEBAR_ITEM_H: f32 = 40.0;
const MENU_ROW_H: f32 = 44.0;
const CONTENT_PAD: f32 = 22.0;
const AGENT_MENU_W: f32 = 292.0;
const AGENT_LIST_ROW_H: f32 = 66.0;
const AGENT_SPLIT_THRESHOLD: f32 = 820.0;

/// The settings navigation, grouped by what people come to do. Each feature lives in exactly one
/// section; the configuration browser is the advanced catch-all for raw keys.
const SETTINGS_GROUPS: &[SettingsGroup] = &[
    SettingsGroup {
        title: "General",
        list_name: "settings-general",
        sections: &[SettingsSection::Appearance, SettingsSection::System],
    },
    SettingsGroup {
        title: "AI",
        list_name: "settings-ai",
        sections: &[
            SettingsSection::Providers,
            SettingsSection::Usage,
            SettingsSection::Agents,
            SettingsSection::Skills,
        ],
    },
    SettingsGroup {
        title: "This device",
        list_name: "settings-device",
        sections: &[SettingsSection::Storage, SettingsSection::RemoteManagement],
    },
    SettingsGroup {
        title: "Cluster",
        list_name: "settings-cluster",
        sections: &[SettingsSection::Cluster, SettingsSection::Modules],
    },
    SettingsGroup {
        title: "Advanced",
        list_name: "settings-advanced",
        sections: &[SettingsSection::Configuration],
    },
];

struct SettingsGroup {
    title: &'static str,
    list_name: &'static str,
    sections: &'static [SettingsSection],
}

mod kit;
mod node;
use kit::*;
use node::*;

/// A shared closure that resolves the current widget theme — threaded into every control so the
/// Appearance toggle restyles the whole tab live, with no rebuild.
type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

/// A top-level settings destination. Each renders its own detail page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SettingsSection {
    Appearance,
    /// Connection status, app version, and diagnostics.
    System,
    Providers,
    Usage,
    Agents,
    Skills,
    /// The SIFS mount and the NFS mount on the selected node.
    Storage,
    RemoteManagement,
    /// Cluster membership: joining, invitations, and this device's role.
    Cluster,
    Modules,
    /// Every configuration key, for the selected node or the whole cluster.
    Configuration,
}

impl SettingsSection {
    pub const ALL: [SettingsSection; 11] = [
        SettingsSection::Appearance,
        SettingsSection::System,
        SettingsSection::Providers,
        SettingsSection::Usage,
        SettingsSection::Agents,
        SettingsSection::Skills,
        SettingsSection::Storage,
        SettingsSection::RemoteManagement,
        SettingsSection::Cluster,
        SettingsSection::Modules,
        SettingsSection::Configuration,
    ];

    /// List-and-detail pages use the full content width instead of the reading column.
    fn uses_wide_page(self) -> bool {
        matches!(self, SettingsSection::Agents)
    }

    pub fn title(self) -> &'static str {
        match self {
            SettingsSection::Appearance => "Appearance",
            SettingsSection::System => "System",
            SettingsSection::Providers => "Providers",
            SettingsSection::Agents => "Agents",
            SettingsSection::Skills => "Skills",
            SettingsSection::Storage => "Storage",
            SettingsSection::RemoteManagement => "Host access",
            SettingsSection::Cluster => "Membership",
            SettingsSection::Modules => "Modules",
            SettingsSection::Usage => "Usage",
            SettingsSection::Configuration => "Configuration",
        }
    }

    fn description(self) -> &'static str {
        match self {
            SettingsSection::Appearance => "Choose how Sinomo looks, reads, and renders.",
            SettingsSection::System => {
                "Connection status, app version, and details for reporting a problem."
            }
            SettingsSection::Providers => {
                "Configure model provider accounts, defaults, and connection health."
            }
            SettingsSection::Agents => {
                "Create, validate, import, and export the agents available to Sinomo."
            }
            SettingsSection::Skills => {
                "Browse, activate, import, and export reusable agent skills."
            }
            SettingsSection::Storage => {
                "Mount SIFS on the selected node's filesystem, natively or over NFS."
            }
            SettingsSection::RemoteManagement => {
                "Inspect host-access consent and configure the selected node's remote-management module."
            }
            SettingsSection::Cluster => {
                "Join a cluster, invite devices, and see this device's role in it."
            }
            SettingsSection::Modules => {
                "Manage module lifecycle and effective configuration across nodes."
            }
            SettingsSection::Usage => {
                "Retain usage history, maintain price estimates, and set display-only budgets."
            }
            SettingsSection::Configuration => {
                "Browse and edit every setting key, for the selected node or the whole cluster."
            }
        }
    }

    fn icon(self) -> IconGlyph {
        match self {
            SettingsSection::Appearance => IconGlyph::Brush,
            SettingsSection::System => IconGlyph::Monitor,
            SettingsSection::Providers => IconGlyph::Chat,
            SettingsSection::Agents => IconGlyph::Sparkles,
            SettingsSection::Skills => IconGlyph::FileText,
            SettingsSection::Storage => IconGlyph::Storage,
            SettingsSection::RemoteManagement => IconGlyph::Lock,
            SettingsSection::Cluster => IconGlyph::Link,
            SettingsSection::Modules => IconGlyph::Blocks,
            SettingsSection::Usage => IconGlyph::FileText,
            SettingsSection::Configuration => IconGlyph::MoreHorizontal,
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
    agent_detail_open: bool,
    pub agent_action_error: Option<String>,
    pub agent_action_status: Option<String>,
    pub agent_delete_confirming: Option<String>,
    pub agent_import_path: String,
    pub agent_export_path: String,
    pub agent_validation_report: Option<AgentValidationReport>,
    pub module_snapshot: ModuleConfigSnapshot,
    /// Shared target for every node-scoped settings page.
    pub module_selected_node_id: Option<String>,
    node_selection_epoch: Arc<AtomicU64>,
    node_settings_loading: bool,
    node_settings_error: Option<String>,
    pub module_action_error: Option<String>,
    pub module_action_status: Option<String>,
    pub sifs_mount_snapshot: SifsMountSettingsSnapshot,
    sifs_mount_form: SifsMountForm,
    pub sifs_mount_action_error: Option<String>,
    pub sifs_mount_action_status: Option<String>,
    sifs_mount_ui_revision: u64,
    pub remote_management: RemoteManagementSettingsSnapshot,
    pub remote_management_permission_error: Option<String>,
    pub remote_management_permission_status: Option<String>,
    pub remote_management_module_error: Option<String>,
    pub remote_management_module_status: Option<String>,
    pub remote_management_enable_confirming: bool,
    pub external_integrations_permission_error: Option<String>,
    pub external_integrations_permission_status: Option<String>,
    pub external_integrations_enable_confirming: bool,
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
    /// Requests for [`SettingsOperation::is_slow`] operations. They share the
    /// lane's result channel but not its queue, so a live provider test or a
    /// cluster join cannot stall unrelated settings edits.
    slow_operation_tx: Option<Sender<SettingsOperationRequest>>,
    /// Invalidates queued daemon-backed requests after reconnect. The
    /// node-local permission mutation is intentionally exempt and returns its
    /// exact post-lock document through the retained result lane.
    operation_epoch: Arc<AtomicU64>,
    next_operation_request_id: u64,
    pending_operations: HashMap<SettingsOperationScope, u64>,
    provider_refresh_needed: bool,
    provider_refresh_health_invalidation: Option<(String, String)>,
    server_generations: SettingsGenerations,
    draft_generations: SettingsGenerations,
    connection_detail_expanded: Vec<Tab>,
    /// Open "Show details" disclosures, by id; see [`kit::details`].
    details_open: HashSet<String>,
    /// The configuration browser shows the selected node's settings instead of the cluster's.
    configuration_node_scope: bool,
    /// User-owned Advanced browser state. The query input itself stays outside the filtered
    /// results rebuild so typing never replaces the focused editor.
    advanced_query: String,
    advanced_overrides_only: bool,
    /// Changes only when both browser controls must be reset as a unit.
    advanced_controls_revision: u64,
    advanced_detail_expanded: Vec<String>,
    provider_detail_expanded: Vec<String>,
    provider_usage_expanded: Vec<String>,
    provider_remove_confirming: Option<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SifsMountForm {
    enabled: bool,
    mount_point: String,
    root_path: String,
    read_only: bool,
    allow_other: bool,
    sync_local_changes: bool,
    /// Carried through unchanged; this form has no control for it.
    operation_budget: Option<String>,
}

impl From<&SifsMountConfig> for SifsMountForm {
    fn from(config: &SifsMountConfig) -> Self {
        Self::for_platform(config, SifsMountPlatform::current())
    }
}

impl SifsMountForm {
    fn for_platform(config: &SifsMountConfig, platform: SifsMountPlatform) -> Self {
        let mut form = Self {
            enabled: config.enabled,
            mount_point: config.mount_point.clone(),
            root_path: config.root_path.clone(),
            read_only: config.read_only,
            allow_other: config.allow_other,
            sync_local_changes: config.sync_local_changes,
            operation_budget: config.operation_budget.clone(),
        };
        match platform {
            SifsMountPlatform::UnixFuse => form.sync_local_changes = false,
            SifsMountPlatform::ExternalProvider => {
                form.allow_other = false;
            }
            SifsMountPlatform::Unsupported => {
                form.allow_other = false;
                form.sync_local_changes = false;
            }
        }
        form
    }
}

impl SifsMountForm {
    fn to_config(&self) -> SifsMountConfig {
        SifsMountConfig {
            enabled: self.enabled,
            mount_point: self.mount_point.trim().to_string(),
            root_path: self.root_path.trim().to_string(),
            read_only: self.read_only,
            allow_other: self.allow_other,
            sync_local_changes: self.sync_local_changes,
            operation_budget: self.operation_budget.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ConfigItemStructure {
    key: String,
    owner: String,
    description: String,
    source: &'static str,
    scope: &'static str,
    module_id: Option<String>,
    edit_kind: ConfigEditKind,
    bool_value: Option<bool>,
    read_only: bool,
    node_scoped: bool,
    raw_value: Value,
    schema: Option<ConfigSchema>,
}

impl From<&ConfigItem> for ConfigItemStructure {
    fn from(item: &ConfigItem) -> Self {
        Self {
            key: item.key.clone(),
            owner: item.owner.clone(),
            description: item.description.clone(),
            source: item.source,
            scope: item.scope,
            module_id: item.module_id.clone(),
            edit_kind: item.edit_kind,
            bool_value: item.bool_value,
            read_only: item.read_only,
            node_scoped: item.node_scoped,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SettingsConnectionStructure {
    mode_label: String,
    status_detail: String,
    issues: Vec<SettingsConnectionIssueStructure>,
    expanded: Vec<Tab>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SettingsNodeStructure {
    clustered: bool,
    cluster_node_id: Option<String>,
    cluster_summary: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsSifsMountStructure {
    snapshot: SifsMountSettingsSnapshot,
    form: SifsMountForm,
    pending: bool,
    dirty: bool,
    ui_revision: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SettingsClusterStructure {
    clustered: bool,
    cluster_summary: Option<String>,
    cluster_id: Option<String>,
    invite: Option<(String, String, String, String)>,
    leave_confirming: bool,
    join_confirming: bool,
    join_invite: String,
    pending: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsRemoteManagementStructure {
    local: bool,
    external_integrations: (Option<String>, Option<String>, bool, bool),
    snapshot: RemoteManagementSettingsSnapshot,
    permission_error: Option<String>,
    permission_status: Option<String>,
    module_error: Option<String>,
    module_status: Option<String>,
    enable_confirming: bool,
    permission_pending: bool,
    module_pending: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct SettingsConfigurationStructure {
    error: Option<String>,
    items: Vec<ConfigItemStructure>,
    /// Owner groups of catalog indices that pass the search and the overrides filter.
    groups: Vec<(String, Vec<usize>)>,
    query_empty: bool,
    overrides_only: bool,
    controls_revision: u64,
    expanded: Vec<String>,
    ui_revision: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct SettingsProviderStructure {
    add_open: bool,
    add_provider_index: usize,
    add_auth: ProviderAddAuth,
    action_error: Option<String>,
    action_status: Option<ProviderActionStatus>,
    remove_confirming: Option<(String, String)>,
    usage_expanded: Vec<String>,
    expanded: Vec<String>,
    providers: Vec<(ProviderForm, ProviderHealthState)>,
    pending: bool,
}

/// How a provider being added signs in; picks the add form's authentication row.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ProviderAddAuth {
    #[default]
    DeviceLogin,
    ApiKey,
    None,
}

impl ProviderAddAuth {
    fn index(self) -> usize {
        match self {
            ProviderAddAuth::DeviceLogin => 0,
            ProviderAddAuth::ApiKey => 1,
            ProviderAddAuth::None => 2,
        }
    }
}

/// What the agent detail pane shows: the editor for a custom or unsaved agent, a read-only
/// summary of a runtime-supplied one, or a prompt to pick an agent.
#[derive(Clone, Debug, Default, PartialEq)]
enum SettingsAgentDetail {
    Editor,
    ReadOnly(Box<AgentDefinition>),
    #[default]
    Empty,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsAgentMcpServerRow {
    ui_id: u64,
    name: String,
    enabled: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct SettingsAgentsStructure {
    agents: Vec<AgentConfigSummary>,
    error: Option<String>,
    selected_id: Option<String>,
    detail_open: bool,
    delete_confirming: bool,
    validation_report: Option<AgentValidationReport>,
    editor_previous_id: Option<String>,
    detail: SettingsAgentDetail,
    editor_tools: Vec<AgentToolSpec>,
    available_tools: Vec<String>,
    editor_mcp_servers: Vec<SettingsAgentMcpServerRow>,
    pending: bool,
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
    display_name: String,
    availability: String,
    error: Option<String>,
    config: Vec<(String, bool)>,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct SettingsModulesStructure {
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SettingsSkillsStructure {
    pending: bool,
    include_all: bool,
    selected: Option<(String, String)>,
    detail_content_hash: Option<String>,
    text_expanded: bool,
    skills: Vec<SkillSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SettingsDeviceLoginStructure {
    primary: String,
    prompt_url: Option<String>,
    error: bool,
}

/// An error and a status message for one settings surface.
type SettingsFeedback = (Option<String>, Option<String>);

/// Action feedback for each surface, so feedback rows update in place.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SettingsFeedbackStructure {
    agent: SettingsFeedback,
    sifs_mount: SettingsFeedback,
    cluster: SettingsFeedback,
    provider_add: SettingsFeedback,
    skill: SettingsFeedback,
}

#[derive(Clone, Debug, PartialEq)]
struct SettingsStructureSnapshot {
    node_target: (Option<String>, bool, Option<String>),
    selected: Option<SettingsSection>,
    navigation_previews: Vec<(SettingsSection, String)>,
    connection: SettingsConnectionStructure,
    node: SettingsNodeStructure,
    sifs_mount: SettingsSifsMountStructure,
    remote_management: SettingsRemoteManagementStructure,
    cluster: SettingsClusterStructure,
    configuration: SettingsConfigurationStructure,
    providers: SettingsProviderStructure,
    agents: SettingsAgentsStructure,
    modules: SettingsModulesStructure,
    skills: SettingsSkillsStructure,
    device_logins: Vec<SettingsDeviceLoginStructure>,
    feedback: SettingsFeedbackStructure,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SettingsStructureKey(Option<Box<SettingsStructureSnapshot>>);

impl SettingsStructureKey {
    pub(crate) fn empty() -> Self {
        Self(None)
    }
}

/// Project the settings state that retained widgets bind to. Theme changes are not part of it:
/// widgets read the theme live and the shell re-lays them out when it changes.
pub(crate) fn settings_structure_key(shared: &Shared) -> SettingsStructureKey {
    let issues = connection_issues(shared)
        .into_iter()
        .map(|issue| SettingsConnectionIssueStructure {
            title: issue.title,
            detail: issue.detail,
            tab: issue.tab,
        })
        .collect();
    let mut snapshot = {
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
            error: settings.config_error.clone(),
            items: settings
                .config_items
                .iter()
                .map(ConfigItemStructure::from)
                .collect(),
            groups: visible_config_groups(&settings),
            query_empty: settings.advanced_query.trim().is_empty(),
            overrides_only: settings.advanced_overrides_only,
            controls_revision: settings.advanced_controls_revision,
            expanded: settings.advanced_detail_expanded.clone(),
            ui_revision: settings.config_ui_revision,
        };
        let providers = SettingsProviderStructure {
            add_open: settings.provider_add_open,
            add_provider_index: settings.provider_add_provider_index,
            add_auth: if settings.provider_add_uses_device_login() {
                ProviderAddAuth::DeviceLogin
            } else if settings.provider_add_requires_api_key() {
                ProviderAddAuth::ApiKey
            } else {
                ProviderAddAuth::None
            },
            action_error: settings.provider_action_error.clone(),
            action_status: settings.provider_action_status.clone(),
            remove_confirming: settings.provider_remove_confirming.clone(),
            usage_expanded: settings.provider_usage_expanded.clone(),
            expanded: settings.provider_detail_expanded.clone(),
            providers: settings
                .provider_forms
                .iter()
                .map(|form| {
                    (
                        form.clone(),
                        settings.provider_health(&form.provider, &form.account),
                    )
                })
                .collect(),
            pending: settings.domain_pending(SettingsOperationDomain::Providers),
        };
        let selected_definition = settings.agent_snapshot.selected_agent.clone();
        let agents = SettingsAgentsStructure {
            agents: settings.agent_snapshot.agents.clone(),
            error: settings.agent_snapshot.error.clone(),
            selected_id: settings.agent_selected_id.clone(),
            detail_open: settings.agent_detail_open,
            delete_confirming: settings
                .agent_form
                .previous_id
                .as_ref()
                .is_some_and(|id| settings.agent_delete_confirming.as_ref() == Some(id)),
            validation_report: settings.agent_validation_report.clone(),
            editor_previous_id: settings.agent_form.previous_id.clone(),
            detail: if settings.agent_form.previous_id.is_none()
                || selected_definition
                    .as_ref()
                    .is_some_and(agent_definition_is_editable)
            {
                SettingsAgentDetail::Editor
            } else if let Some(definition) = selected_definition {
                SettingsAgentDetail::ReadOnly(Box::new(definition))
            } else {
                SettingsAgentDetail::Empty
            },
            editor_tools: settings.agent_form.definition.tools.clone(),
            available_tools: available_agent_tool_specs(
                &settings.agent_snapshot.available_tools,
                &settings.agent_form.definition.tools,
            )
            .into_iter()
            .map(|tool| tool.name)
            .collect(),
            editor_mcp_servers: settings
                .agent_form
                .mcp_servers
                .iter()
                .map(|server| SettingsAgentMcpServerRow {
                    ui_id: server.ui_id,
                    name: server.name.clone(),
                    enabled: server.enabled,
                })
                .collect(),
            pending: settings.domain_pending(SettingsOperationDomain::Agents),
        };
        let module_snapshot = &settings.module_snapshot;
        let modules = SettingsModulesStructure {
            selected_node_id: settings.module_selected_node_id.clone(),
            action_error: settings.module_action_error.clone(),
            action_status: settings.module_action_status.clone(),
            warnings: module_snapshot.warnings.clone(),
            config_ui_revision: settings.config_ui_revision,
            nodes: module_snapshot
                .nodes
                .iter()
                .map(|node| SettingsModuleNodeStructure {
                    node_id: node.node_id.clone(),
                    display_name: node.display_name.clone(),
                    availability: node.availability.clone(),
                    local: node.local,
                })
                .collect(),
            modules: module_snapshot
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
            cluster_config: module_snapshot
                .cluster_config
                .modules
                .iter()
                .map(|(module_id, setting)| (module_id.clone(), setting.enabled))
                .collect(),
            selected_node: module_snapshot.selected_node.as_ref().map(|node| {
                SettingsSelectedModuleNodeStructure {
                    node_id: node.summary.node_id.clone(),
                    display_name: node.summary.display_name.clone(),
                    availability: node.summary.availability.clone(),
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
            pending: settings.domain_pending(SettingsOperationDomain::Skills),
            include_all: settings.skills_include_all,
            selected: settings.skill_selected.clone(),
            detail_content_hash: settings
                .skill_detail
                .as_ref()
                .map(|detail| detail.summary.content_hash.clone()),
            text_expanded: settings.skill_text_expanded,
            skills: settings.skills.clone(),
        };
        let feedback = SettingsFeedbackStructure {
            agent: (
                settings.agent_action_error.clone(),
                settings.agent_action_status.clone(),
            ),
            sifs_mount: (
                settings.sifs_mount_action_error.clone(),
                settings.sifs_mount_action_status.clone(),
            ),
            cluster: (
                settings.cluster_action_error.clone(),
                settings.cluster_action_status.clone(),
            ),
            provider_add: (
                settings
                    .provider_add_error
                    .clone()
                    .or_else(|| settings.provider_action_error.clone()),
                settings
                    .provider_action_status
                    .as_ref()
                    .map(|status| status.message.clone()),
            ),
            skill: (
                settings.skill_action_error.clone(),
                settings.skill_action_status.clone(),
            ),
        };
        SettingsStructureSnapshot {
            node_target: (
                settings.module_selected_node_id.clone(),
                settings.node_settings_loading,
                settings.node_settings_error.clone(),
            ),
            selected: settings.selected,
            navigation_previews: Vec::new(),
            connection: SettingsConnectionStructure {
                mode_label: settings.mode_label.clone(),
                status_detail: settings.status_detail.clone(),
                issues,
                expanded: settings.connection_detail_expanded.clone(),
            },
            node: SettingsNodeStructure {
                clustered: settings.clustered,
                cluster_node_id: settings.cluster_node_id.clone(),
                cluster_summary: settings.cluster_summary.clone(),
            },
            sifs_mount: SettingsSifsMountStructure {
                snapshot: settings.sifs_mount_snapshot.clone(),
                form: settings.sifs_mount_form.clone(),
                pending: settings.sifs_mount_pending(),
                dirty: settings.sifs_mount_dirty(),
                ui_revision: settings.sifs_mount_ui_revision,
            },
            remote_management: SettingsRemoteManagementStructure {
                local: settings.selected_settings_node_is_local(),
                external_integrations: (
                    settings.external_integrations_permission_error.clone(),
                    settings.external_integrations_permission_status.clone(),
                    settings.external_integrations_enable_confirming,
                    settings.external_integrations_permission_pending(),
                ),
                snapshot: settings.remote_management.clone(),
                permission_error: settings.remote_management_permission_error.clone(),
                permission_status: settings.remote_management_permission_status.clone(),
                module_error: settings.remote_management_module_error.clone(),
                module_status: settings.remote_management_module_status.clone(),
                enable_confirming: settings.remote_management_enable_confirming,
                permission_pending: settings.remote_management_permission_pending(),
                module_pending: settings.remote_management_module_pending(),
            },
            cluster: SettingsClusterStructure {
                clustered: settings.clustered,
                cluster_summary: settings.cluster_summary.clone(),
                cluster_id: settings.cluster_id.clone(),
                invite,
                leave_confirming: settings.leave_confirming,
                join_confirming: settings.join_confirming,
                join_invite: settings.join_invite.clone(),
                pending: settings.domain_pending(SettingsOperationDomain::Cluster),
            },
            configuration,
            providers,
            agents,
            modules,
            skills,
            device_logins: Vec::new(),
            feedback,
        }
    };
    snapshot.device_logins = (0..snapshot.providers.providers.len())
        .map(|index| {
            let (primary, prompt_url) = device_login_lines(shared, index);
            SettingsDeviceLoginStructure {
                primary,
                prompt_url,
                error: device_login_is_error(shared, index),
            }
        })
        .collect();
    snapshot.navigation_previews = SettingsSection::ALL
        .into_iter()
        .map(|section| (section, section_preview(shared, section)))
        .collect();
    SettingsStructureKey(Some(Box::new(snapshot)))
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
    sifs_mount: u64,
    remote_management_permission: u64,
    remote_management: u64,
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
            SettingsOperationDomain::SifsMount => self.sifs_mount,
            SettingsOperationDomain::RemoteManagement => self.remote_management,
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
            SettingsOperationDomain::SifsMount => &mut self.sifs_mount,
            SettingsOperationDomain::RemoteManagement => &mut self.remote_management,
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
    SifsMount,
    RemoteManagement,
    Skills,
    Cluster,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum SettingsOperationScope {
    ConfigField(String),
    Providers,
    Agents,
    Modules,
    SifsMount,
    RemoteManagementRefresh,
    RemoteManagementPermission,
    ExternalIntegrationsPermission,
    RemoteManagementModule,
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
            Self::SifsMount => SettingsOperationDomain::SifsMount,
            Self::RemoteManagementRefresh
            | Self::RemoteManagementPermission
            | Self::ExternalIntegrationsPermission
            | Self::RemoteManagementModule => SettingsOperationDomain::RemoteManagement,
            Self::Skills => SettingsOperationDomain::Skills,
            Self::Cluster => SettingsOperationDomain::Cluster,
        }
    }
}

struct SettingsInitialLoadResult {
    settings: SettingsState,
    chat_providers: Vec<ProviderSummary>,
    server_generations: SettingsGenerations,
    draft_generations: SettingsGenerations,
}

#[derive(Default)]
pub(crate) struct SettingsInitialLoadEffects {
    pub(crate) changed: bool,
    pub(crate) chat_providers: Option<Vec<ProviderSummary>>,
}

struct SettingsOperationLane {
    tx: Sender<SettingsOperationRequest>,
    rx: Receiver<SettingsOperationResult>,
}

struct SettingsOperationRequest {
    request_id: u64,
    scope: SettingsOperationScope,
    draft_generation: u64,
    operation_epoch: u64,
    operation_epoch_source: Arc<AtomicU64>,
    node_epoch: Option<(Arc<AtomicU64>, u64)>,
    backend: Backend,
    operation: SettingsOperation,
}

enum SettingsOperation {
    RefreshNodeSettings,
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
    SetNodeModuleOverride {
        node_id: String,
        module_id: String,
        enabled: Option<bool>,
        native_rust: bool,
    },
    RefreshSifsMount {
        update_feedback: bool,
    },
    SaveSifsMount {
        config: SifsMountConfig,
    },
    RefreshRemoteManagement,
    #[cfg(target_os = "android")]
    OpenAndroidMediaSettings,
    SetDeviceExposure {
        kind: String,
        enabled: bool,
    },
    SetRemoteManagementPermission {
        enabled: bool,
    },
    SetExternalIntegrationsPermission {
        enabled: bool,
    },
    SetRemoteManagementModuleOverride {
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
    node_epoch: Option<u64>,
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
    SifsMount(Result<SifsMountOperationSuccess, String>),
    RemoteManagement(Result<RemoteManagementOperationSuccess, String>),
    Skills(Result<SkillOperationSuccess, String>),
    Cluster(Result<ClusterOperationSuccess, String>),
}

struct ProviderHealthOperationResult {
    provider: String,
    account: String,
    result: Result<sinomo_runtime::api::lm::LanguageModelProviderHealthSnapshot, String>,
}

struct ConfigOperationSuccess {
    node_scoped: bool,
    items: Vec<ConfigItem>,
    saved_draft: Option<ConfigSavedDraft>,
}

struct SifsMountOperationSuccess {
    snapshot: SifsMountSettingsSnapshot,
    items: Vec<ConfigItem>,
    message: Option<String>,
    update_feedback: bool,
    effect: SifsMountOperationEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SifsMountOperationEffect {
    Refreshed,
    Saved,
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
    node_settings: Option<NodeSettingsSnapshot>,
    snapshot: ModuleConfigSnapshot,
    remote_management: RemoteManagementSettingsSnapshot,
    message: Option<String>,
}

enum RemoteManagementOperationSuccess {
    Refreshed {
        snapshot: RemoteManagementSettingsSnapshot,
        local_module_snapshot: ModuleConfigSnapshot,
    },
    PermissionChanged {
        permissions: LocalNodePermissions,
        target: LocalPermissionTarget,
        message: String,
    },
    ModuleChanged {
        snapshot: RemoteManagementSettingsSnapshot,
        local_module_snapshot: ModuleConfigSnapshot,
        message: String,
    },
}

#[derive(Clone, Copy)]
enum LocalPermissionTarget {
    RemoteManagement,
    ExternalIntegrations,
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
    home_server_node_id: String,
    sandbox_node_id: String,
    mcp_servers: Vec<AgentMcpServerForm>,
    next_mcp_server_ui_id: u64,
    tool_add_index: usize,
    output_format_index: usize,
}

#[derive(Clone, Debug)]
struct AgentMcpServerForm {
    ui_id: u64,
    name: String,
    url: String,
    enabled: bool,
    required: bool,
    protocol_era_index: usize,
    allowed_tools_text: String,
    bearer_token_env: String,
    headers_from_env_text: String,
    startup_timeout_ms: String,
    tool_timeout_ms: String,
}

impl AgentMcpServerForm {
    fn from_definition(ui_id: u64, server: &AgentMcpServerDefinition) -> Self {
        Self {
            ui_id,
            name: server.name.clone(),
            url: server.url.clone(),
            enabled: server.enabled,
            required: server.required,
            protocol_era_index: mcp_protocol_era_index(server.protocol_era),
            allowed_tools_text: server.allowed_tools.join("\n"),
            bearer_token_env: server.bearer_token_env.clone().unwrap_or_default(),
            headers_from_env_text: server
                .headers_from_env
                .iter()
                .map(|(header, variable)| format!("{header}: {variable}"))
                .collect::<Vec<_>>()
                .join("\n"),
            startup_timeout_ms: server.startup_timeout_ms.to_string(),
            tool_timeout_ms: server.tool_timeout_ms.to_string(),
        }
    }

    fn to_definition(&self) -> Result<AgentMcpServerDefinition, String> {
        let startup_timeout_ms = self
            .startup_timeout_ms
            .trim()
            .parse::<u64>()
            .map_err(|error| format!("invalid MCP startup timeout: {error}"))?;
        let tool_timeout_ms = self
            .tool_timeout_ms
            .trim()
            .parse::<u64>()
            .map_err(|error| format!("invalid MCP tool timeout: {error}"))?;
        Ok(AgentMcpServerDefinition {
            name: self.name.trim().to_string(),
            url: self.url.trim().to_string(),
            enabled: self.enabled,
            protocol_era: mcp_protocol_era_for_index(self.protocol_era_index),
            required: self.required,
            allowed_tools: parse_agent_list(&self.allowed_tools_text),
            bearer_token_env: optional_trimmed_string(&self.bearer_token_env),
            headers_from_env: parse_mcp_headers(&self.headers_from_env_text)?,
            startup_timeout_ms,
            tool_timeout_ms,
        })
    }
}

/// A text-editable field of a provider in the Providers section.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProviderField {
    ApiKey,
    Model,
    BaseUrl,
    ReasoningBudget,
    MaxContextTokens,
    AutoCompactTokenLimit,
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
    usage: Option<sinomo_runtime::api::lm::LanguageModelProviderUsage>,
    usage_error: Option<String>,
    stale: bool,
}

impl ProviderHealthState {
    fn from_snapshot(
        snapshot: sinomo_runtime::api::lm::LanguageModelProviderHealthSnapshot,
    ) -> Self {
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

const AGENT_MCP_PROTOCOL_ERA_OPTIONS: &[&str] = &[
    "Detect automatically",
    "Stateless (2026-07-28)",
    "Handshake (2025-11-25 and earlier)",
];
const AGENT_ROLE_OPTIONS: &[&str] = &["Main", "Background", "Deliberation", "Evolution"];
const AGENT_OUTPUT_FORMAT_OPTIONS: &[&str] =
    &["JSON decision", "Plain text", "Native tools and text"];

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
        let mcp_servers = definition
            .tool_runtime
            .mcp_servers
            .iter()
            .enumerate()
            .map(|(index, server)| AgentMcpServerForm::from_definition(index as u64, server))
            .collect::<Vec<_>>();
        let next_mcp_server_ui_id = mcp_servers.len() as u64;
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
            home_server_node_id: definition
                .placement
                .home_server_node_id
                .clone()
                .unwrap_or_default(),
            sandbox_node_id: definition
                .tool_runtime
                .sandbox_node_id
                .clone()
                .unwrap_or_default(),
            mcp_servers,
            next_mcp_server_ui_id,
            tool_add_index: 0,
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
        definition.placement.home_server_node_id =
            optional_trimmed_string(&self.home_server_node_id);
        definition.tool_runtime.sandbox_node_id = optional_trimmed_string(&self.sandbox_node_id);
        definition.tool_runtime.mcp_servers = self
            .mcp_servers
            .iter()
            .map(AgentMcpServerForm::to_definition)
            .collect::<Result<Vec<_>, _>>()?;
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
        #[cfg(not(test))]
        let skill_address = backend.connection.daemon_address().map(str::to_string);
        #[cfg(not(test))]
        let (skills, skill_selected, skill_detail, skill_action_error) = match skill_address
            .ok_or_else(|| "skill catalog requires a daemon connection".to_string())
            .and_then(|address| {
                crate::backend::run_with_timeout("skill-catalog", sifs_timeout, move || {
                    load_initial_skill_state_from_client(&address, skills_include_all)
                })
            }) {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(%error, "settings skill catalog load degraded");
                (Vec::new(), None, None, Some(error))
            }
        };
        #[cfg(test)]
        let (skills, skill_selected, skill_detail, skill_action_error) =
            match backend.catalog_read_vfs().and_then(|catalog_vfs| {
                crate::backend::run_with_timeout("skill-catalog", sifs_timeout, move || {
                    load_initial_skill_state(&catalog_vfs, skills_include_all)
                })
            }) {
                Ok(state) => state,
                Err(error) => (Vec::new(), None, None, Some(error)),
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
        let sifs_mount_snapshot = backend.sifs_mount_settings_snapshot();
        let sifs_mount_form =
            SifsMountForm::for_platform(&sifs_mount_snapshot.config, sifs_mount_snapshot.platform);
        let remote_management =
            backend.remote_management_settings_snapshot_from_modules(&module_snapshot);

        let cluster_status = backend.app_cluster_status();
        let (clustered, cluster_summary) = if cluster_status.clustered {
            (true, cluster_status.summary.clone())
        } else {
            match &backend.connection {
                crate::backend::Connection::Daemon(status)
                | crate::backend::Connection::AppOwnedDaemon(status) => {
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
            agent_detail_open: false,
            agent_action_error: None,
            agent_action_status: None,
            agent_delete_confirming: None,
            agent_import_path: String::new(),
            agent_export_path: String::new(),
            agent_validation_report: None,
            module_snapshot,
            module_selected_node_id,
            node_selection_epoch: Arc::new(AtomicU64::new(0)),
            node_settings_loading: false,
            node_settings_error: None,
            module_action_error: None,
            module_action_status: None,
            sifs_mount_snapshot,
            sifs_mount_form,
            sifs_mount_action_error: None,
            sifs_mount_action_status: None,
            sifs_mount_ui_revision: 0,
            remote_management,
            remote_management_permission_error: None,
            remote_management_permission_status: None,
            remote_management_module_error: None,
            remote_management_module_status: None,
            remote_management_enable_confirming: false,
            external_integrations_permission_error: None,
            external_integrations_permission_status: None,
            external_integrations_enable_confirming: false,
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
            slow_operation_tx: None,
            operation_epoch: Arc::new(AtomicU64::new(0)),
            next_operation_request_id: 0,
            pending_operations: HashMap::new(),
            provider_refresh_needed: false,
            provider_refresh_health_invalidation: None,
            server_generations: SettingsGenerations::default(),
            draft_generations: SettingsGenerations::default(),
            connection_detail_expanded: Vec::new(),
            details_open: HashSet::new(),
            configuration_node_scope: false,
            advanced_query: String::new(),
            advanced_overrides_only: false,
            advanced_controls_revision: 0,
            advanced_detail_expanded: Vec::new(),
            provider_detail_expanded: Vec::new(),
            provider_usage_expanded: Vec::new(),
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
                | crate::backend::Connection::AppOwnedDaemon(status) => {
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
            agent_detail_open: false,
            agent_action_error: None,
            agent_action_status: None,
            agent_delete_confirming: None,
            agent_import_path: String::new(),
            agent_export_path: String::new(),
            agent_validation_report: None,
            module_snapshot: ModuleConfigSnapshot::default(),
            module_selected_node_id: None,
            node_selection_epoch: Arc::new(AtomicU64::new(0)),
            node_settings_loading: false,
            node_settings_error: None,
            module_action_error: None,
            module_action_status: None,
            sifs_mount_snapshot: SifsMountSettingsSnapshot::default(),
            sifs_mount_form: SifsMountForm::from(&SifsMountConfig::default()),
            sifs_mount_action_error: None,
            sifs_mount_action_status: Some("Mount state is loading…".to_string()),
            sifs_mount_ui_revision: 0,
            remote_management: RemoteManagementSettingsSnapshot::default(),
            remote_management_permission_error: None,
            remote_management_permission_status: None,
            remote_management_module_error: None,
            remote_management_module_status: None,
            remote_management_enable_confirming: false,
            external_integrations_permission_error: None,
            external_integrations_permission_status: None,
            external_integrations_enable_confirming: false,
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
            slow_operation_tx: None,
            operation_epoch: Arc::new(AtomicU64::new(0)),
            next_operation_request_id: 0,
            pending_operations: HashMap::new(),
            provider_refresh_needed: false,
            provider_refresh_health_invalidation: None,
            server_generations: SettingsGenerations::default(),
            draft_generations: SettingsGenerations::default(),
            connection_detail_expanded: Vec::new(),
            details_open: HashSet::new(),
            configuration_node_scope: false,
            advanced_query: String::new(),
            advanced_overrides_only: false,
            advanced_controls_revision: 0,
            advanced_detail_expanded: Vec::new(),
            provider_detail_expanded: Vec::new(),
            provider_usage_expanded: Vec::new(),
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
                let chat_providers = cluster_provider_summaries(backend.config.as_ref());
                let _ = tx.send(SettingsInitialLoadResult {
                    settings,
                    chat_providers,
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

    pub(crate) fn pump_initial_load(&mut self) -> SettingsInitialLoadEffects {
        let result = self
            .initial_load_rx
            .as_ref()
            .and_then(|rx| rx.try_recv().ok());
        let Some(result) = result else {
            return SettingsInitialLoadEffects::default();
        };
        self.initial_load_rx = None;
        self.initial_load_started = true;
        let chat_providers = self.merge_initial_load(result);
        SettingsInitialLoadEffects {
            changed: true,
            chat_providers,
        }
    }

    /// Rebind Settings after the connection state machine installs a new daemon backend. In-flight
    /// daemon calls cannot be cancelled, so their request IDs are abandoned and late results are
    /// ignored. A node-local permission write is the exception: it is independent of the daemon
    /// and may already have committed, so its result lane is retained until the authoritative
    /// post-mutation document arrives.
    pub(crate) fn rebind_backend(&mut self, backend: &Backend) {
        let retry_message = "Connection changed while the action was in flight; verify refreshed state before retrying.";
        self.operation_epoch.fetch_add(1, Ordering::AcqRel);
        let pending_scopes = self.pending_operations.keys().cloned().collect::<Vec<_>>();
        let preserve_permission_write = self.pending_operations.keys().any(|scope| {
            matches!(
                scope,
                SettingsOperationScope::RemoteManagementPermission
                    | SettingsOperationScope::ExternalIntegrationsPermission
            )
        });
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
                SettingsOperationScope::SifsMount => {
                    self.sifs_mount_action_error = Some(retry_message.to_string());
                    self.sifs_mount_action_status = None;
                }
                SettingsOperationScope::RemoteManagementPermission => {
                    self.remote_management_permission_error = None;
                    self.remote_management_permission_status =
                        Some("Finishing the node-local permission change…".to_string());
                }
                SettingsOperationScope::ExternalIntegrationsPermission => {
                    self.external_integrations_permission_error = None;
                    self.external_integrations_permission_status =
                        Some("Finishing the node-local permission change…".to_string());
                }
                SettingsOperationScope::RemoteManagementModule => {
                    self.remote_management_module_error = Some(retry_message.to_string());
                    self.remote_management_module_status = None;
                }
                SettingsOperationScope::RemoteManagementRefresh => {
                    self.remote_management_permission_error = Some(retry_message.to_string());
                    self.remote_management_permission_status = None;
                    self.remote_management_module_error = Some(retry_message.to_string());
                    self.remote_management_module_status = None;
                    self.external_integrations_permission_error = Some(retry_message.to_string());
                    self.external_integrations_permission_status = None;
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
        if preserve_permission_write {
            self.pending_operations.retain(|scope, _| {
                matches!(
                    scope,
                    SettingsOperationScope::RemoteManagementPermission
                        | SettingsOperationScope::ExternalIntegrationsPermission
                )
            });
        } else {
            self.pending_operations.clear();
            self.operation_lane = None;
            self.slow_operation_tx = None;
        }
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
            SettingsOperationDomain::SifsMount,
            SettingsOperationDomain::RemoteManagement,
            SettingsOperationDomain::Skills,
            SettingsOperationDomain::Cluster,
        ] {
            self.server_generations.bump(domain);
        }
        self.server_generations.remote_management_permission = self
            .server_generations
            .remote_management_permission
            .wrapping_add(1);
    }

    /// Merge only server-owned fields whose domain has not advanced since the preload began.
    /// Drafts, confirmations, action feedback, expansion state, and the operation lane always stay
    /// owned by the live state. This is deliberately field-wise: replacing `self` here used to
    /// erase join links, agent edits, and completed action feedback entered while loading.
    fn merge_initial_load(
        &mut self,
        result: SettingsInitialLoadResult,
    ) -> Option<Vec<ProviderSummary>> {
        let chat_providers = (self.server_generations.providers
            == result.server_generations.providers)
            .then_some(result.chat_providers);
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
        if self.server_generations.sifs_mount == result.server_generations.sifs_mount {
            self.sifs_mount_snapshot = loaded.sifs_mount_snapshot;
            if result.draft_generations.sifs_mount == 0
                && self.draft_generations.sifs_mount == result.draft_generations.sifs_mount
            {
                self.sifs_mount_form = loaded.sifs_mount_form;
            }
            if self.sifs_mount_action_error.is_none() {
                self.sifs_mount_action_error = loaded.sifs_mount_action_error;
            }
            if self.sifs_mount_action_status.as_deref() == Some("Mount state is loading…") {
                self.sifs_mount_action_status = loaded.sifs_mount_action_status;
            }
        }
        if self.server_generations.remote_management_permission
            == result.server_generations.remote_management_permission
        {
            self.remote_management
                .replace_permission_state(&loaded.remote_management);
        }
        if self.server_generations.remote_management == result.server_generations.remote_management
        {
            self.remote_management
                .replace_module_state(&loaded.remote_management);
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
        chat_providers
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

    fn sifs_mount_pending(&self) -> bool {
        self.operation_pending(&SettingsOperationScope::SifsMount)
    }

    fn sifs_mount_dirty(&self) -> bool {
        self.sifs_mount_form.to_config() != self.sifs_mount_snapshot.config
    }

    fn sifs_mount_save_config(&self) -> Result<SifsMountConfig, String> {
        let config = self.sifs_mount_form.to_config();
        validate_sifs_mount_config(&config, self.sifs_mount_snapshot.platform)?;
        Ok(config)
    }

    fn reset_sifs_mount_form(&mut self) {
        self.sifs_mount_form = SifsMountForm::for_platform(
            &self.sifs_mount_snapshot.config,
            self.sifs_mount_snapshot.platform,
        );
        self.sifs_mount_action_error = None;
        self.sifs_mount_action_status = None;
        self.sifs_mount_ui_revision = self.sifs_mount_ui_revision.wrapping_add(1);
        self.mark_draft_changed(SettingsOperationDomain::SifsMount);
    }

    fn edit_sifs_mount(&mut self, edit: impl FnOnce(&mut SifsMountForm)) {
        edit(&mut self.sifs_mount_form);
        self.sifs_mount_action_error = None;
        self.sifs_mount_action_status = None;
        self.mark_draft_changed(SettingsOperationDomain::SifsMount);
    }

    fn remote_management_permission_pending(&self) -> bool {
        self.operation_pending(&SettingsOperationScope::RemoteManagementPermission)
            || self.operation_pending(&SettingsOperationScope::RemoteManagementRefresh)
    }

    fn external_integrations_permission_pending(&self) -> bool {
        self.operation_pending(&SettingsOperationScope::ExternalIntegrationsPermission)
            || self.operation_pending(&SettingsOperationScope::RemoteManagementRefresh)
    }

    fn remote_management_module_pending(&self) -> bool {
        self.operation_pending(&SettingsOperationScope::RemoteManagementModule)
            || self.operation_pending(&SettingsOperationScope::RemoteManagementRefresh)
    }

    fn apply_module_snapshot_if_selected(&mut self, snapshot: ModuleConfigSnapshot) {
        let Some(local_node_id) = snapshot
            .selected_node
            .as_ref()
            .map(|node| node.summary.node_id.clone())
        else {
            return;
        };
        self.server_generations
            .bump(SettingsOperationDomain::Modules);
        if self
            .module_selected_node_id
            .as_deref()
            .is_none_or(|selected| selected == local_node_id.as_str())
        {
            self.module_selected_node_id = Some(local_node_id);
            self.module_snapshot = snapshot;
        }
    }

    fn queue_operation(
        &mut self,
        backend: Backend,
        waker: Option<sui::UiHandle>,
        scope: SettingsOperationScope,
        operation: SettingsOperation,
    ) -> bool {
        if let SettingsOperation::SetConfigValue { key, .. }
        | SettingsOperation::SetConfigDocument { key, .. }
        | SettingsOperation::SetConfigBool { key, .. } = &operation
        {
            if !self.config_items.iter().any(|item| item.key == *key) {
                self.apply_operation_queue_error(&scope, "The configuration definition is no longer available; refresh the selected scope.".into());
                return false;
            }
        }
        let node_scoped = self.operation_targets_node(&operation);
        if node_scoped
            && self.node_settings_loading
            && !matches!(&operation, SettingsOperation::RefreshNodeSettings)
        {
            return false;
        }
        let backend = if node_scoped {
            let target = self
                .module_selected_node_id
                .clone()
                .or(self.cluster_node_id.clone())
                .or_else(|| Some(self.node_id.clone()));
            match backend.with_settings_node(target) {
                Ok(backend) => backend,
                Err(error) => {
                    self.apply_operation_queue_error(&scope, error);
                    return false;
                }
            }
        } else {
            backend
        };
        let node_epoch = node_scoped.then(|| {
            (
                Arc::clone(&self.node_selection_epoch),
                self.node_selection_epoch.load(Ordering::Acquire),
            )
        });
        // One in-flight request owns a scope until its matching result arrives. In particular,
        // ignore key-repeat/double-click activation of destructive actions instead of enqueueing a
        // second mutation whose eventual "not found" could mask the first request's success.
        if self.operation_pending(&scope) {
            return false;
        }
        if self.operation_lane.is_none() {
            let (request_tx, request_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let (slow_tx, slow_rx) = mpsc::channel();
            let slow_result_tx = result_tx.clone();
            let slow_waker = waker.clone();
            // Without the slow lane, slow operations fall back to the shared queue.
            self.slow_operation_tx = thread::Builder::new()
                .name("sinomo-app-settings-slow-operations".to_string())
                .spawn(move || settings_operation_worker(slow_rx, slow_result_tx, slow_waker))
                .ok()
                .map(|_| slow_tx);
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

        let slow = operation.is_slow();
        let request_id = self.next_operation_request_id;
        self.next_operation_request_id = self.next_operation_request_id.wrapping_add(1);
        let draft_generation = self.draft_generations.get(scope.domain());
        let operation_epoch = self.operation_epoch.load(Ordering::Acquire);
        self.mark_operation_pending(&operation);
        self.pending_operations.insert(scope.clone(), request_id);
        let request = SettingsOperationRequest {
            request_id,
            scope: scope.clone(),
            draft_generation,
            operation_epoch,
            operation_epoch_source: Arc::clone(&self.operation_epoch),
            node_epoch,
            backend,
            operation,
        };
        let slow_tx = self.slow_operation_tx.as_ref().filter(|_| slow);
        let sent = match slow_tx {
            Some(tx) => tx.send(request).is_ok(),
            None => self
                .operation_lane
                .as_ref()
                .is_some_and(|lane| lane.tx.send(request).is_ok()),
        };
        if !sent {
            self.pending_operations.remove(&scope);
            self.operation_lane = None;
            self.slow_operation_tx = None;
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
            SettingsOperation::RefreshNodeSettings | SettingsOperation::RefreshModules { .. } => {
                self.module_action_error = None;
                self.module_action_status = Some("Refreshing modules…".to_string());
            }
            SettingsOperation::SetNodeModuleOverride { .. } => {
                self.module_action_error = None;
                self.module_action_status = Some("Saving module configuration…".to_string());
            }
            SettingsOperation::RefreshSifsMount { update_feedback } => {
                if *update_feedback {
                    self.sifs_mount_action_error = None;
                    self.sifs_mount_action_status = Some("Refreshing mount state…".to_string());
                }
            }
            SettingsOperation::SaveSifsMount { .. } => {
                self.sifs_mount_action_error = None;
                self.sifs_mount_action_status = Some("Saving mount configuration…".to_string());
            }
            SettingsOperation::RefreshRemoteManagement => {
                self.remote_management_permission_error = None;
                self.remote_management_permission_status = None;
                self.remote_management_module_error = None;
                self.remote_management_module_status = None;
                self.external_integrations_permission_error = None;
                self.external_integrations_permission_status = None;
            }
            #[cfg(target_os = "android")]
            SettingsOperation::OpenAndroidMediaSettings => {
                self.remote_management_permission_error = None;
                self.remote_management_permission_status =
                    Some("Opening Android App Use setup…".into());
            }
            SettingsOperation::SetDeviceExposure { .. } => {
                self.remote_management_permission_error = None;
                self.remote_management_permission_status = Some("Updating device sharing…".into());
            }
            SettingsOperation::SetRemoteManagementPermission { enabled } => {
                self.remote_management_permission_error = None;
                self.remote_management_permission_status = Some(if *enabled {
                    "Allowing remote management…".to_string()
                } else {
                    "Revoking remote management…".to_string()
                });
            }
            SettingsOperation::SetExternalIntegrationsPermission { enabled } => {
                self.external_integrations_permission_error = None;
                self.external_integrations_permission_status = Some(if *enabled {
                    "Allowing external integrations…".to_string()
                } else {
                    "Revoking external integrations…".to_string()
                });
            }
            SettingsOperation::SetRemoteManagementModuleOverride { .. } => {
                self.remote_management_module_error = None;
                self.remote_management_module_status =
                    Some("Saving the local module override…".to_string());
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
            SettingsOperationDomain::SifsMount => {
                self.sifs_mount_action_error = Some(error);
                self.sifs_mount_action_status = None;
            }
            SettingsOperationDomain::RemoteManagement => match scope {
                SettingsOperationScope::RemoteManagementPermission => {
                    self.remote_management_permission_error = Some(error);
                    self.remote_management_permission_status = None;
                }
                SettingsOperationScope::ExternalIntegrationsPermission => {
                    self.external_integrations_permission_error = Some(error);
                    self.external_integrations_permission_status = None;
                }
                SettingsOperationScope::RemoteManagementModule => {
                    self.remote_management_module_error = Some(error);
                    self.remote_management_module_status = None;
                }
                SettingsOperationScope::RemoteManagementRefresh => {
                    self.remote_management_permission_error = Some(error.clone());
                    self.remote_management_permission_status = None;
                    self.remote_management_module_error = Some(error.clone());
                    self.remote_management_module_status = None;
                    self.external_integrations_permission_error = Some(error);
                    self.external_integrations_permission_status = None;
                }
                _ => unreachable!("remote-management domain has a remote-management scope"),
            },
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
            if result
                .node_epoch
                .is_some_and(|epoch| self.node_selection_epoch.load(Ordering::Acquire) != epoch)
            {
                continue;
            }
            effects.changed = true;
            let domain = result.scope.domain();
            match result.payload {
                SettingsOperationPayload::Config(Ok(success)) => {
                    self.server_generations
                        .bump(SettingsOperationDomain::Config);
                    self.merge_config_scope(success.items, success.node_scoped);
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
                    self.merge_config_scope(success.items, false);
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
                        AgentOperationEffect::Saved(id) => {
                            self.agent_detail_open = true;
                            Some(format!("Saved custom agent {id}"))
                        }
                        AgentOperationEffect::Deleted(id) => {
                            self.agent_detail_open = false;
                            self.agent_delete_confirming = None;
                            self.agent_validation_report = None;
                            Some(format!("Deleted custom agent {id}"))
                        }
                        AgentOperationEffect::Imported(id) => {
                            self.agent_detail_open = true;
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
                    if self
                        .module_selected_node_id
                        .as_deref()
                        .is_some_and(|selected| {
                            success
                                .snapshot
                                .selected_node
                                .as_ref()
                                .is_none_or(|node| node.summary.node_id != selected)
                        })
                    {
                        self.node_settings_loading = false;
                        self.node_settings_error = Some("The selected node is no longer available. Select another node or refresh.".into());
                        continue;
                    }
                    if let Some(node_settings) = success.node_settings {
                        self.merge_config_scope(node_settings.items, true);
                        self.sifs_mount_snapshot = node_settings.mount;
                        self.sifs_mount_form = SifsMountForm::for_platform(
                            &self.sifs_mount_snapshot.config,
                            self.sifs_mount_snapshot.platform,
                        );
                        self.sifs_mount_ui_revision = self.sifs_mount_ui_revision.wrapping_add(1);
                        self.remote_management = success.remote_management.clone();
                        self.node_settings_loading = false;
                        self.node_settings_error = None;
                    }
                    self.server_generations
                        .bump(SettingsOperationDomain::Modules);
                    self.server_generations
                        .bump(SettingsOperationDomain::RemoteManagement);
                    self.module_snapshot = success.snapshot;
                    self.remote_management
                        .replace_module_state(&success.remote_management);
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
                    if self.node_settings_loading {
                        self.node_settings_loading = false;
                        self.node_settings_error = Some(error.clone());
                    }
                    self.apply_operation_queue_error(&result.scope, error);
                }
                SettingsOperationPayload::SifsMount(Ok(success)) => {
                    self.server_generations
                        .bump(SettingsOperationDomain::SifsMount);
                    self.server_generations
                        .bump(SettingsOperationDomain::Config);
                    self.sifs_mount_snapshot = success.snapshot;
                    self.merge_config_scope(success.items, true);
                    self.config_error = None;
                    if success.effect == SifsMountOperationEffect::Saved
                        && self.draft_generations.sifs_mount == result.draft_generation
                    {
                        self.sifs_mount_form = SifsMountForm::for_platform(
                            &self.sifs_mount_snapshot.config,
                            self.sifs_mount_snapshot.platform,
                        );
                        self.sifs_mount_ui_revision = self.sifs_mount_ui_revision.wrapping_add(1);
                    }
                    if success.update_feedback {
                        self.sifs_mount_action_error = None;
                        self.sifs_mount_action_status = success.message;
                    }
                }
                SettingsOperationPayload::SifsMount(Err(error)) => {
                    self.apply_operation_queue_error(&result.scope, error);
                }
                SettingsOperationPayload::RemoteManagement(Ok(success)) => match success {
                    RemoteManagementOperationSuccess::Refreshed {
                        snapshot,
                        local_module_snapshot,
                    } => {
                        self.server_generations.remote_management_permission = self
                            .server_generations
                            .remote_management_permission
                            .wrapping_add(1);
                        self.server_generations
                            .bump(SettingsOperationDomain::RemoteManagement);
                        self.remote_management = snapshot;
                        self.apply_module_snapshot_if_selected(local_module_snapshot);
                        self.remote_management_permission_error = None;
                        self.remote_management_permission_status = None;
                        self.external_integrations_permission_error = None;
                        self.external_integrations_permission_status = None;
                        self.remote_management_module_error = None;
                        self.remote_management_module_status = None;
                    }
                    RemoteManagementOperationSuccess::PermissionChanged {
                        permissions,
                        target,
                        message,
                    } => {
                        self.server_generations.remote_management_permission = self
                            .server_generations
                            .remote_management_permission
                            .wrapping_add(1);
                        self.remote_management
                            .replace_permission_document(&permissions);
                        match target {
                            LocalPermissionTarget::RemoteManagement => {
                                self.remote_management_permission_error = None;
                                self.remote_management_permission_status = Some(message);
                                self.remote_management_enable_confirming = false;
                            }
                            LocalPermissionTarget::ExternalIntegrations => {
                                self.external_integrations_permission_error = None;
                                self.external_integrations_permission_status = Some(message);
                                self.external_integrations_enable_confirming = false;
                            }
                        }
                    }
                    RemoteManagementOperationSuccess::ModuleChanged {
                        snapshot,
                        local_module_snapshot,
                        message,
                    } => {
                        self.server_generations
                            .bump(SettingsOperationDomain::RemoteManagement);
                        self.remote_management.replace_module_state(&snapshot);
                        self.apply_module_snapshot_if_selected(local_module_snapshot);
                        self.remote_management_module_error = None;
                        self.remote_management_module_status = Some(message);
                    }
                },
                SettingsOperationPayload::RemoteManagement(Err(error)) => {
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

    fn confirm_remote_management_enable(&mut self) {
        self.remote_management_enable_confirming = true;
        self.remote_management_permission_error = None;
        self.remote_management_permission_status = None;
        self.mark_draft_changed(SettingsOperationDomain::RemoteManagement);
    }

    fn cancel_remote_management_enable(&mut self) {
        self.remote_management_enable_confirming = false;
        self.mark_draft_changed(SettingsOperationDomain::RemoteManagement);
    }

    fn confirm_external_integrations_enable(&mut self) {
        self.external_integrations_enable_confirming = true;
        self.external_integrations_permission_error = None;
        self.external_integrations_permission_status = None;
        self.mark_draft_changed(SettingsOperationDomain::RemoteManagement);
    }

    fn cancel_external_integrations_enable(&mut self) {
        self.external_integrations_enable_confirming = false;
        self.mark_draft_changed(SettingsOperationDomain::RemoteManagement);
    }

    fn set_advanced_query(&mut self, query: String) {
        self.advanced_query = query;
    }

    fn set_advanced_overrides_only(&mut self, overrides_only: bool) {
        self.advanced_overrides_only = overrides_only;
    }

    fn clear_advanced_filters(&mut self) {
        self.advanced_query.clear();
        self.advanced_overrides_only = false;
        self.advanced_controls_revision = self.advanced_controls_revision.wrapping_add(1);
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

    #[cfg(test)]
    fn provider_remove_confirming(&self, provider: &str, account: &str) -> bool {
        self.provider_remove_confirming
            .as_ref()
            .is_some_and(|(p, a)| p == provider && a == account)
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

    fn toggle_provider_usage(&mut self, provider: &str, account: &str) {
        let key = provider_account_key(provider, account);
        if let Some(index) = self
            .provider_usage_expanded
            .iter()
            .position(|expanded| expanded == &key)
        {
            self.provider_usage_expanded.remove(index);
        } else {
            self.provider_usage_expanded.push(key);
        }
    }

    fn prepare_agent_selection(&mut self, index: usize) -> Option<String> {
        let id = self.agent_snapshot.agents.get(index)?.id.clone();
        if self
            .agent_snapshot
            .selected_agent
            .as_ref()
            .is_some_and(|agent| agent.key() != id)
        {
            self.agent_snapshot.selected_agent = None;
        }
        self.agent_selected_id = Some(id.clone());
        self.agent_detail_open = true;
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
        self.agent_detail_open = true;
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
        self.agent_detail_open = true;
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

    fn set_agent_home_server_node_id(&mut self, value: String) {
        self.agent_form.home_server_node_id = value;
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

    fn add_agent_mcp_server(&mut self) {
        let ui_id = self.agent_form.next_mcp_server_ui_id;
        self.agent_form.next_mcp_server_ui_id = ui_id.wrapping_add(1);
        let mut suffix = self.agent_form.mcp_servers.len() + 1;
        let name = loop {
            let candidate = format!("mcp-{suffix}");
            if !self
                .agent_form
                .mcp_servers
                .iter()
                .any(|server| server.name == candidate)
            {
                break candidate;
            }
            suffix += 1;
        };
        self.agent_form
            .mcp_servers
            .push(AgentMcpServerForm::from_definition(
                ui_id,
                &AgentMcpServerDefinition::streamable_http(name, "http://127.0.0.1:3000/mcp"),
            ));
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = Some("Added MCP server draft".to_string());
    }

    fn update_agent_mcp_server(
        &mut self,
        ui_id: u64,
        update: impl FnOnce(&mut AgentMcpServerForm),
    ) {
        let Some(server) = self
            .agent_form
            .mcp_servers
            .iter_mut()
            .find(|server| server.ui_id == ui_id)
        else {
            return;
        };
        update(server);
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn remove_agent_mcp_server(&mut self, ui_id: u64) {
        let previous_len = self.agent_form.mcp_servers.len();
        self.agent_form
            .mcp_servers
            .retain(|server| server.ui_id != ui_id);
        if self.agent_form.mcp_servers.len() != previous_len {
            self.mark_draft_changed(SettingsOperationDomain::Agents);
            self.agent_action_error = None;
            self.agent_action_status = Some("Removed MCP server draft".to_string());
        }
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

    fn set_agent_tool_add_index(&mut self, index: usize) {
        self.agent_form.tool_add_index = index;
    }

    fn add_selected_agent_tool(&mut self) {
        let available = available_agent_tool_specs(
            &self.agent_snapshot.available_tools,
            &self.agent_form.definition.tools,
        );
        let Some(spec) = available.get(self.agent_form.tool_add_index).cloned() else {
            return;
        };
        self.agent_form.definition.tools.push(spec);
        self.agent_form
            .definition
            .tools
            .sort_by(|left, right| left.name.cmp(&right.name));
        self.agent_form.tool_add_index = 0;
        self.mark_draft_changed(SettingsOperationDomain::Agents);
        self.agent_action_error = None;
        self.agent_action_status = None;
    }

    fn remove_agent_tool(&mut self, name: &str) {
        let previous_len = self.agent_form.definition.tools.len();
        self.agent_form
            .definition
            .tools
            .retain(|tool| tool.name != name);
        if self.agent_form.definition.tools.len() != previous_len {
            self.agent_form.tool_add_index = 0;
            self.mark_draft_changed(SettingsOperationDomain::Agents);
            self.agent_action_error = None;
            self.agent_action_status = None;
        }
    }

    fn back_to_agent_list(&mut self) {
        self.agent_detail_open = false;
        self.agent_delete_confirming = None;
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
        provider_uses_device_login(self.provider_add_provider())
    }

    fn provider_add_requires_api_key(&self) -> bool {
        provider_requires_api_key(self.provider_add_provider())
    }

    fn provider_add_can_submit(&self) -> bool {
        !self.provider_add_requires_api_key() || !self.provider_add_api_key.trim().is_empty()
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

    /// Apply platform back navigation before the window is allowed to close.
    ///
    /// The provider form is a nested destination inside the Providers section,
    /// so it closes first. A second back returns a narrow Settings view to its
    /// section menu. Wide layouts normally have no back affordance, but clearing
    /// `selected` is harmless because the wide view always supplies a fallback.
    pub(crate) fn navigate_back(&mut self) -> bool {
        if self.provider_add_open {
            self.close_provider_add();
            return true;
        }
        if self.selected.is_some() {
            self.back_to_menu();
            return true;
        }
        false
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
                ProviderField::MaxContextTokens => form
                    .max_context_tokens
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                ProviderField::AutoCompactTokenLimit => form
                    .auto_compact_token_limit
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

impl SettingsOperation {
    /// Operations bounded by an external party (a model provider, an agent run,
    /// or cluster membership consensus) rather than by local configuration I/O.
    fn is_slow(&self) -> bool {
        matches!(
            self,
            SettingsOperation::TestProviderConnection { .. }
                | SettingsOperation::TestAgent { .. }
                | SettingsOperation::CreateClusterInvite
                | SettingsOperation::JoinCluster { .. }
                | SettingsOperation::LeaveCluster
        )
    }
}

fn settings_operation_worker(
    request_rx: Receiver<SettingsOperationRequest>,
    result_tx: Sender<SettingsOperationResult>,
    waker: Option<sui::UiHandle>,
) {
    while let Ok(request) = request_rx.recv() {
        if !settings_operation_request_is_current(&request) {
            continue;
        }
        let payload = execute_settings_operation(&request.backend, request.operation);
        let result = SettingsOperationResult {
            node_epoch: request.node_epoch.as_ref().map(|(_, epoch)| *epoch),
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

fn settings_operation_request_is_current(request: &SettingsOperationRequest) -> bool {
    if request
        .node_epoch
        .as_ref()
        .is_some_and(|(source, epoch)| source.load(Ordering::Acquire) != *epoch)
    {
        return false;
    }
    matches!(
        &request.scope,
        SettingsOperationScope::RemoteManagementPermission
            | SettingsOperationScope::ExternalIntegrationsPermission
    ) || request.operation_epoch_source.load(Ordering::Acquire) == request.operation_epoch
}

fn execute_settings_operation(
    backend: &Backend,
    operation: SettingsOperation,
) -> SettingsOperationPayload {
    match operation {
        SettingsOperation::RefreshNodeSettings => SettingsOperationPayload::Modules((|| {
            backend
                .config
                .catalog(None)
                .map_err(|error| error.to_string())?;
            let snapshot = backend.module_config_snapshot(backend.settings_node_id.as_deref());
            let selected = snapshot
                .selected_node
                .as_ref()
                .ok_or_else(|| "The selected node is no longer available".to_string())?;
            if let Some(error) = &selected.error {
                return Err(error.clone());
            }
            let mut result = module_operation_success(backend, snapshot, None);
            result.node_settings = Some(NodeSettingsSnapshot {
                items: crate::backend::config::resolve_node_items(backend.config.as_ref())?,
                mount: backend.sifs_mount_settings_snapshot(),
            });
            Ok(result)
        })()),
        SettingsOperation::SetConfigValue {
            key,
            kind,
            raw,
            draft_key,
        } => SettingsOperationPayload::Config((|| {
            set_value(backend.config.as_ref(), &key, kind, &raw)?;
            Ok(ConfigOperationSuccess {
                node_scoped: backend.settings_node_id.is_some(),
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
                node_scoped: backend.settings_node_id.is_some(),
                items: resolve_items(backend.config.as_ref()),
                saved_draft,
            })
        })()),
        SettingsOperation::SetConfigBool { key, value } => {
            SettingsOperationPayload::Config((|| {
                set_bool(backend.config.as_ref(), &key, value)?;
                Ok(ConfigOperationSuccess {
                    node_scoped: backend.settings_node_id.is_some(),
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
            let account = sinomo_runtime::api::lm::normalize_provider_account_name(Some(&account));
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
                ProviderField::MaxContextTokens => set_provider_max_context_tokens(
                    backend.config.as_ref(),
                    &provider,
                    &account,
                    &raw,
                ),
                ProviderField::AutoCompactTokenLimit => set_provider_auto_compact_token_limit(
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
            let snapshot = backend.module_config_snapshot(selected_node_id.as_deref());
            SettingsOperationPayload::Modules(Ok(module_operation_success(backend, snapshot, None)))
        }
        SettingsOperation::SetNodeModuleOverride {
            node_id,
            module_id,
            enabled,
            native_rust,
        } => SettingsOperationPayload::Modules((|| {
            backend.set_node_module_override(&node_id, &module_id, enabled, native_rust)?;
            let mut message = match enabled {
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
            if enabled.is_some() && native_rust {
                message.push_str("; restart that node's daemon to apply it");
            }
            let snapshot = backend.module_config_snapshot(Some(&node_id));
            Ok(module_operation_success(backend, snapshot, Some(message)))
        })()),
        SettingsOperation::RefreshSifsMount { update_feedback } => {
            SettingsOperationPayload::SifsMount(sifs_mount_operation_success(
                backend,
                None,
                update_feedback,
                SifsMountOperationEffect::Refreshed,
            ))
        }
        SettingsOperation::SaveSifsMount { config } => {
            SettingsOperationPayload::SifsMount((|| {
                backend.set_local_sifs_mount_config(&config)?;
                sifs_mount_operation_success(
                    backend,
                    Some(if config.enabled {
                        "Mount configuration saved; the local supervisor is reconciling it"
                            .to_string()
                    } else {
                        "Mount disabled; the local supervisor is reconciling it".to_string()
                    }),
                    true,
                    SifsMountOperationEffect::Saved,
                )
            })())
        }
        SettingsOperation::RefreshRemoteManagement => {
            let module_snapshot = backend.module_config_snapshot(None);
            SettingsOperationPayload::RemoteManagement(Ok(
                RemoteManagementOperationSuccess::Refreshed {
                    snapshot: backend
                        .remote_management_settings_snapshot_from_modules(&module_snapshot),
                    local_module_snapshot: module_snapshot,
                },
            ))
        }
        #[cfg(target_os = "android")]
        SettingsOperation::OpenAndroidMediaSettings => {
            SettingsOperationPayload::RemoteManagement((|| {
                crate::android_media::request("app-use")?;
                let permissions =
                    sinomo_runtime::node::LocalNodePermissionStore::new(backend.data_dir.root())
                        .load()
                        .map_err(|error| error.to_string())?;
                Ok(RemoteManagementOperationSuccess::PermissionChanged {
                    permissions,
                    target: LocalPermissionTarget::RemoteManagement,
                    message: "App Use setup opened on this device".into(),
                })
            })())
        }
        SettingsOperation::SetDeviceExposure { kind, enabled } => {
            SettingsOperationPayload::RemoteManagement((|| {
                #[cfg(target_os = "android")]
                if enabled && matches!(kind.as_str(), "microphone" | "camera") {
                    crate::android_media::request(&kind)?;
                }
                let mutation = backend.set_local_device_exposure(&kind, enabled)?;
                Ok(RemoteManagementOperationSuccess::PermissionChanged {
                    permissions: mutation.permissions,
                    target: LocalPermissionTarget::RemoteManagement,
                    message: format!(
                        "{kind} sharing {}",
                        if enabled { "enabled" } else { "disabled" }
                    ),
                })
            })())
        }
        SettingsOperation::SetRemoteManagementPermission { enabled } => {
            SettingsOperationPayload::RemoteManagement((|| {
                let mutation = backend.set_local_remote_management_permission(enabled)?;
                let message = match (enabled, mutation.changed) {
                    (true, true) => "Remote management is now allowed on this node",
                    (true, false) => "Remote management was already allowed on this node",
                    (false, true) => "Remote management was revoked on this node",
                    (false, false) => "Remote management was already denied on this node",
                };
                Ok(RemoteManagementOperationSuccess::PermissionChanged {
                    permissions: mutation.permissions,
                    target: LocalPermissionTarget::RemoteManagement,
                    message: message.to_string(),
                })
            })())
        }
        SettingsOperation::SetExternalIntegrationsPermission { enabled } => {
            SettingsOperationPayload::RemoteManagement((|| {
                let mutation = backend.set_local_external_integrations_permission(enabled)?;
                let message = match (enabled, mutation.changed) {
                    (true, true) => "External integrations are now allowed on this node",
                    (true, false) => "External integrations were already allowed on this node",
                    (false, true) => "External integrations were revoked on this node",
                    (false, false) => "External integrations were already denied on this node",
                };
                Ok(RemoteManagementOperationSuccess::PermissionChanged {
                    permissions: mutation.permissions,
                    target: LocalPermissionTarget::ExternalIntegrations,
                    message: message.to_string(),
                })
            })())
        }
        SettingsOperation::SetRemoteManagementModuleOverride { enabled } => {
            SettingsOperationPayload::RemoteManagement((|| {
                backend.set_local_remote_management_module_override(enabled)?;
                let message = match enabled {
                    Some(true) => "Local module override set to enabled",
                    Some(false) => "Local module override set to disabled",
                    None => "Local module override cleared; the cluster default now applies",
                };
                let module_snapshot = backend.module_config_snapshot(None);
                Ok(RemoteManagementOperationSuccess::ModuleChanged {
                    snapshot: backend
                        .remote_management_settings_snapshot_from_modules(&module_snapshot),
                    local_module_snapshot: module_snapshot,
                    message: message.to_string(),
                })
            })())
        }
        SettingsOperation::RefreshSkills {
            include_all,
            selected,
        } => SettingsOperationPayload::Skills(load_skill_operation_success(
            backend,
            include_all,
            selected,
            None,
        )),
        SettingsOperation::ImportSkill { path } => SettingsOperationPayload::Skills((|| {
            let result = import_skill_for_backend(backend, path, false)?;
            load_skill_operation_success(
                backend,
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
            let result =
                export_skill_for_backend(backend, skill_id.clone(), version.clone(), output)?;
            load_skill_operation_success(
                backend,
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
            let result = activate_skill_for_backend(backend, skill_id, version)?;
            load_skill_operation_success(
                backend,
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

fn sifs_mount_operation_success(
    backend: &Backend,
    message: Option<String>,
    update_feedback: bool,
    effect: SifsMountOperationEffect,
) -> Result<SifsMountOperationSuccess, String> {
    backend
        .config
        .catalog(None)
        .map_err(|error| error.to_string())?;
    Ok(SifsMountOperationSuccess {
        snapshot: backend.sifs_mount_settings_snapshot(),
        items: resolve_items(backend.config.as_ref()),
        message,
        update_feedback,
        effect,
    })
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
    let snapshot = backend.agent_config_snapshot(selected_id);
    let chat_agents = agent_summaries_from_snapshot(&snapshot);
    AgentOperationSuccess {
        snapshot,
        chat_agents,
        effect,
    }
}

fn agent_summaries_from_snapshot(snapshot: &AgentConfigSnapshot) -> Vec<AgentSummary> {
    let agents = snapshot
        .agents
        .iter()
        .filter(|agent| matches!(agent.role.as_str(), "main" | "deliberation"))
        .map(|agent| AgentSummary {
            key: agent.id.clone(),
            name: agent.name.clone(),
            description: agent.description.clone(),
            module_id: agent.module_id.clone(),
            module_version: agent.module_version.clone(),
            tool_runtime: sinomo_client::chat_contract::ChatToolRuntimeSelection {
                sandbox_node_id: agent.sandbox_node_id.clone(),
                workspace_root: None,
                ..Default::default()
            },
        })
        .collect::<Vec<_>>();
    if agents.is_empty() {
        fallback_agent_summaries()
    } else {
        agents
    }
}

#[cfg(test)]
fn agent_summaries_for_backend(backend: &Backend) -> Vec<AgentSummary> {
    agent_summaries_from_snapshot(&backend.agent_config_snapshot(None))
}

fn module_operation_success(
    backend: &Backend,
    snapshot: ModuleConfigSnapshot,
    message: Option<String>,
) -> ModuleOperationSuccess {
    let remote_management = backend.remote_management_settings_snapshot_from_modules(&snapshot);
    ModuleOperationSuccess {
        node_settings: None,
        snapshot,
        remote_management,
        message,
    }
}

fn load_skill_operation_success(
    backend: &Backend,
    include_all: bool,
    selected: Option<(String, String)>,
    message: Option<String>,
) -> Result<SkillOperationSuccess, String> {
    let skills = list_skills_for_backend(backend, include_all)?;
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
        Some((id, version)) => Some(skill_detail_for_backend(backend, id, version)?),
        None => None,
    };
    Ok(SkillOperationSuccess {
        skills,
        selected,
        detail,
        message,
    })
}

#[cfg(not(test))]
fn skill_client_for_backend(backend: &Backend) -> Result<SkillClient, String> {
    backend
        .connection
        .daemon_address()
        .map(SkillClient::at_address)
        .ok_or_else(|| "skill catalog requires a daemon connection".to_string())
}

fn list_skills_for_backend(
    backend: &Backend,
    include_all: bool,
) -> Result<Vec<SkillSummary>, String> {
    #[cfg(test)]
    {
        return backend
            .catalog_read_vfs()
            .and_then(|vfs| sinomo_cli::app_api::skill_summaries(&vfs, include_all));
    }
    #[cfg(not(test))]
    skill_client_for_backend(backend)?
        .list(include_all)
        .map_err(|error| error.to_string())
}

fn skill_detail_for_backend(
    backend: &Backend,
    skill_id: &str,
    version: &str,
) -> Result<SkillDetail, String> {
    #[cfg(test)]
    {
        return backend
            .catalog_read_vfs()
            .and_then(|vfs| sinomo_cli::app_api::skill_detail(&vfs, skill_id, Some(version)));
    }
    #[cfg(not(test))]
    skill_client_for_backend(backend)?
        .detail(skill_id, Some(version.to_string()))
        .map_err(|error| error.to_string())
}

fn import_skill_for_backend(
    backend: &Backend,
    path: PathBuf,
    activate: bool,
) -> Result<SkillMutationResult, String> {
    #[cfg(test)]
    {
        return sinomo_cli::app_api::import_skill(
            backend.data_dir.as_ref(),
            &path,
            None,
            None,
            activate,
        );
    }
    #[cfg(not(test))]
    skill_client_for_backend(backend)?
        .import(SkillImportRequest {
            path,
            skill_id: None,
            version: None,
            activate,
        })
        .map_err(|error| error.to_string())
}

fn export_skill_for_backend(
    backend: &Backend,
    skill_id: String,
    version: String,
    output: PathBuf,
) -> Result<SkillMutationResult, String> {
    #[cfg(test)]
    {
        return backend.catalog_read_vfs().and_then(|vfs| {
            sinomo_cli::app_api::export_skill(&vfs, &skill_id, Some(&version), Some(&output))
        });
    }
    #[cfg(not(test))]
    skill_client_for_backend(backend)?
        .export(SkillExportRequest {
            skill_id,
            version: Some(version),
            output: Some(output),
        })
        .map_err(|error| error.to_string())
}

fn activate_skill_for_backend(
    backend: &Backend,
    skill_id: String,
    version: String,
) -> Result<SkillMutationResult, String> {
    #[cfg(test)]
    {
        return sinomo_cli::app_api::activate_skill(
            backend.data_dir.as_ref(),
            &skill_id,
            Some(&version),
        );
    }
    #[cfg(not(test))]
    skill_client_for_backend(backend)?
        .activate(SkillActivateRequest {
            skill_id,
            version: Some(version),
        })
        .map_err(|error| error.to_string())
}

#[cfg(not(test))]
fn load_initial_skill_state_from_client(
    daemon_address: &str,
    include_all: bool,
) -> (
    Vec<SkillSummary>,
    Option<(String, String)>,
    Option<SkillDetail>,
    Option<String>,
) {
    let client = SkillClient::at_address(daemon_address);
    let skills = match client.list(include_all) {
        Ok(skills) => skills,
        Err(error) => return (Vec::new(), None, None, Some(error.to_string())),
    };
    let selected = skills
        .first()
        .map(|skill| (skill.id.clone(), skill.version.clone()));
    let detail = match selected.as_ref() {
        Some((skill_id, version)) => match client.detail(skill_id, Some(version.clone())) {
            Ok(detail) => Some(detail),
            Err(error) => return (skills, selected, None, Some(error.to_string())),
        },
        None => None,
    };
    (skills, selected, detail, None)
}

#[cfg(test)]
fn load_initial_skill_state(
    catalog_vfs: &VfsHandle,
    include_all: bool,
) -> (
    Vec<SkillSummary>,
    Option<(String, String)>,
    Option<SkillDetail>,
    Option<String>,
) {
    let skills = match sinomo_cli::app_api::skill_summaries(catalog_vfs, include_all) {
        Ok(skills) => skills,
        Err(error) => return (Vec::new(), None, None, Some(error)),
    };
    let selected = skills
        .first()
        .map(|skill| (skill.id.clone(), skill.version.clone()));
    let detail = match selected.as_ref() {
        Some((skill_id, version)) => {
            match sinomo_cli::app_api::skill_detail(catalog_vfs, skill_id, Some(version)) {
                Ok(detail) => Some(detail),
                Err(error) => return (skills, selected, None, Some(error)),
            }
        }
        None => None,
    };
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
        ProviderField::MaxContextTokens => "max_context_tokens",
        ProviderField::AutoCompactTokenLimit => "auto_compact_token_limit",
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
        ProviderField::MaxContextTokens => "Maximum context saved to lm.providers",
        ProviderField::AutoCompactTokenLimit => "Compaction limit saved to lm.providers",
        ProviderField::VendorOptions => "Provider options saved to lm.providers",
    }
}

fn provider_account_key(provider: &str, account: &str) -> String {
    sinomo_runtime::api::lm::provider_health_account_key(provider, account)
}

fn provider_health_states(
    config: sinomo_runtime::api::lm::LanguageModelProviderHealthConfig,
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

fn mcp_protocol_era_index(era: AgentMcpProtocolEra) -> usize {
    match era {
        AgentMcpProtocolEra::Auto => 0,
        AgentMcpProtocolEra::Modern => 1,
        AgentMcpProtocolEra::Legacy => 2,
    }
}

fn mcp_protocol_era_for_index(index: usize) -> AgentMcpProtocolEra {
    match index {
        1 => AgentMcpProtocolEra::Modern,
        2 => AgentMcpProtocolEra::Legacy,
        _ => AgentMcpProtocolEra::Auto,
    }
}

fn output_format_index(output_format: &AgentOutputFormat) -> usize {
    match output_format {
        AgentOutputFormat::JsonDecision => 0,
        AgentOutputFormat::Text => 1,
        AgentOutputFormat::NativeTools => 2,
    }
}

fn output_format_for_index(index: usize) -> AgentOutputFormat {
    match index {
        1 => AgentOutputFormat::Text,
        2 => AgentOutputFormat::NativeTools,
        _ => AgentOutputFormat::JsonDecision,
    }
}

fn optional_trimmed_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn parse_agent_list(value: &str) -> Vec<String> {
    value
        .split(['\n', ','])
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_mcp_headers(value: &str) -> Result<BTreeMap<String, String>, String> {
    let mut headers = BTreeMap::new();
    for (index, line) in value.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((header, variable)) = line.split_once(':').or_else(|| line.split_once('=')) else {
            return Err(format!(
                "invalid MCP header on line {}: use `Header: SINOMO_MCP_VARIABLE`",
                index + 1
            ));
        };
        let header = header.trim();
        let variable = variable.trim();
        if header.is_empty() || variable.is_empty() {
            return Err(format!(
                "invalid MCP header on line {}: header and environment variable are required",
                index + 1
            ));
        }
        if headers
            .insert(header.to_string(), variable.to_string())
            .is_some()
        {
            return Err(format!("duplicate MCP header `{header}`"));
        }
    }
    Ok(headers)
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
    let mut parts = vec![agent.role.clone(), agent.default_model.clone()];
    if agent.tool_count == 1 {
        parts.push("1 tool".to_string());
    } else if agent.tool_count > 1 {
        parts.push(format!("{} tools", agent.tool_count));
    }
    if agent.mcp_server_count == 1 {
        parts.push("1 MCP server".to_string());
    } else if agent.mcp_server_count > 1 {
        parts.push(format!("{} MCP servers", agent.mcp_server_count));
    }
    if agent.skill_count == 1 {
        parts.push("1 skill".to_string());
    } else if agent.skill_count > 1 {
        parts.push(format!("{} skills", agent.skill_count));
    }
    if let Some(node_id) = agent.home_server_node_id.as_ref()
        && !node_id.trim().is_empty()
    {
        parts.push(format!("home {}", short(node_id, 12)));
    }
    if let Some(node_id) = agent.sandbox_node_id.as_ref()
        && !node_id.trim().is_empty()
    {
        parts.push(format!("sandbox {}", short(node_id, 12)));
    }
    if let Some(module_id) = agent
        .module_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
    {
        parts.push(format!("provided by {module_id}"));
    } else {
        parts.push(agent.source.clone());
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

fn available_agent_tool_specs(
    descriptors: &[AgentToolDescriptor],
    selected: &[AgentToolSpec],
) -> Vec<AgentToolSpec> {
    let selected_names = selected
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<HashSet<_>>();
    let mut tools = descriptors
        .iter()
        .map(|descriptor| descriptor.spec.clone())
        .filter(|tool| !selected_names.contains(tool.name.as_str()))
        .collect::<Vec<_>>();
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    tools.dedup_by(|left, right| left.name == right.name);
    tools
}

fn module_sidebar_preview(snapshot: &ModuleConfigSnapshot, items: &[ConfigItem]) -> String {
    let actionable = items
        .iter()
        .filter(|item| item.module_id.is_some() && module_operational_error(item).is_some())
        .count();
    if actionable > 0 {
        return format!("{actionable} action required");
    }
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

/// Keep custom virtual-list content clear of the leading selection indicator, inset by the
/// theme's data row padding.
fn padded_virtual_list_row<W>(reader: &ThemeReader, child: W) -> ThemedPadding
where
    W: Widget + 'static,
{
    ThemedPadding::new(reader, |theme| theme.metrics.data_row_padding, child)
}

fn provider_model_label(form: &ProviderForm) -> String {
    let model = form.default_model.trim();
    if model.is_empty() {
        "provider default".to_string()
    } else {
        model.to_string()
    }
}

fn provider_capabilities_summary(provider: &str) -> String {
    let capabilities = provider_capabilities(provider);
    let mut labels = Vec::new();
    if capabilities.responses_http {
        labels.push("Responses API");
    }
    if capabilities.responses_websocket {
        labels.push("Responses WebSocket");
    }
    if capabilities.realtime_websocket || capabilities.realtime_webrtc {
        labels.push("Realtime voice");
    }
    if capabilities.web_search {
        labels.push("Web search");
    }
    if capabilities.image_generation {
        labels.push(if capabilities.image_editing {
            "Image generation + editing"
        } else {
            "Image generation"
        });
    }
    if capabilities.transcription {
        labels.push("Transcription");
    }
    if capabilities.local_model_management {
        labels.push("Local model discovery");
    }
    if capabilities.mcp {
        labels.push("MCP API");
    }
    if labels.is_empty() {
        "Standard chat".to_string()
    } else {
        labels.join(" · ")
    }
}

fn provider_credential_note(form: &ProviderForm) -> &'static str {
    if form.uses_device_login {
        "sign-in required"
    } else if provider_requires_api_key(&form.provider) {
        "API key required"
    } else {
        "local service unavailable"
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
        SettingsSection::System => {
            if connection_issue_count(shared) > 0 {
                "Needs attention".to_string()
            } else {
                shared.settings().mode_label.clone()
            }
        }
        SettingsSection::Storage => {
            let settings = shared.settings();
            sifs_mount_section_presentation_at(
                &settings.sifs_mount_snapshot,
                settings.sifs_mount_action_error.as_deref(),
                time::OffsetDateTime::now_utc(),
            )
            .label
        }
        SettingsSection::RemoteManagement => {
            let settings = shared.settings();
            if !settings.selected_settings_node_is_local() {
                return "Local consent".into();
            }
            let snapshot = &settings.remote_management;
            if snapshot.permission_loading || snapshot.module_loading {
                "Loading".to_string()
            } else if snapshot.permission_error.is_some()
                || snapshot.module_error.is_some()
                || settings.remote_management_permission_error.is_some()
                || settings.remote_management_module_error.is_some()
            {
                "Needs attention".to_string()
            } else if snapshot.permission_enabled
                && snapshot.module_observed == ModuleObservedState::Running
            {
                "Allowed · active".to_string()
            } else if snapshot.permission_enabled {
                "Permission allowed".to_string()
            } else {
                "Denied".to_string()
            }
        }
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
            module_sidebar_preview(&settings.module_snapshot, &settings.config_items)
        }
        SettingsSection::Skills => {
            let settings = shared.settings();
            skills_sidebar_preview(&settings.skills)
        }
        SettingsSection::Usage => "History, prices and budgets".into(),
        SettingsSection::Configuration => {
            let settings = shared.settings();
            if settings.config_error.is_some() {
                "Unavailable".to_string()
            } else {
                format!("{} keys", settings.config_items.len())
            }
        }
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
fn palette(shared: &Shared) -> SurfacePalette {
    shared.theme().surfaces
}

/// A step on the theme's type ramp. Labels resolve it whenever they lay out, so a text-size
/// change restyles them in place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TextSize {
    Xs,
    Sm,
    Base,
    Lg,
    Xl,
}

impl TextSize {
    fn token(self, theme: &DefaultTheme) -> ThemeTextToken {
        match self {
            TextSize::Xs => theme.text.xs,
            TextSize::Sm => theme.text.sm,
            TextSize::Base => theme.text.base,
            TextSize::Lg => theme.text.lg,
            TextSize::Xl => theme.text.xl,
        }
    }
}

/// The settings body text style at `size`, from the current theme.
fn settings_text_style(shared: &Shared, size: TextSize) -> sui::TextStyle {
    let theme = shared.theme();
    let token = size.token(&theme);
    sui::TextStyle {
        font_size: token.size,
        line_height: token.line_height,
        ..theme.body_text_style()
    }
}

/// A `Label` in the settings text style at `size`, in the primary text color.
fn settings_label(shared: &Shared, text: impl Into<String>, size: TextSize) -> Label {
    let style = shared.clone();
    let color = shared.clone();
    Label::new(text)
        .text_style_when(move || settings_text_style(&style, size))
        .color_when(move || palette(&color).text)
}

/// Style `label` as muted settings text at `size`.
fn muted_text(shared: &Shared, label: Label, size: TextSize) -> Label {
    let style = shared.clone();
    let color = shared.clone();
    label
        .text_style_when(move || settings_text_style(&style, size))
        .color_when(move || palette(&color).text_muted)
}

/// A muted `Label` whose style and color track the theme.
fn muted_label(shared: &Shared, text: impl Into<String>) -> Label {
    muted_label_with_token(shared, text, TextSize::Base)
}

fn muted_label_with_token(shared: &Shared, text: impl Into<String>, size: TextSize) -> Label {
    muted_text(shared, Label::new(text), size)
}

fn muted_meta_label(shared: &Shared, text: impl Into<String>) -> Label {
    muted_label_with_token(shared, text, TextSize::Sm)
}

fn primary_label(shared: &Shared, text: impl Into<String>) -> Label {
    settings_label(shared, text, TextSize::Base)
}

/// A projection of the settings structure bound to retained widgets.
type SettingsProjection<U> = sui::Selector<Signal<SettingsStructureKey>, SettingsStructureKey, U>;

/// Project one value out of the settings structure. A widget bound to the projection updates in
/// place, and only when the projected value changes.
fn settings_projection<U>(
    shared: &Shared,
    name: &'static str,
    select: impl Fn(&SettingsStructureSnapshot) -> U + Send + Sync + 'static,
) -> SettingsProjection<U>
where
    U: Clone + PartialEq + Default + Send + Sync + 'static,
{
    shared
        .signals()
        .settings_structure
        .select_named(name, move |key| {
            key.0.as_deref().map(&select).unwrap_or_default()
        })
}

/// Derive another value from a projection, under the same equality rules.
fn derived<S, I, O>(
    source: S,
    name: &'static str,
    select: impl Fn(&I) -> O + Send + Sync + 'static,
) -> sui::Selector<S, I, O>
where
    S: Observable<I>,
{
    sui::Selector::new(name, source, select)
}

/// Show `child` while `shown` holds; the space it takes grows and shrinks with it.
fn revealed<W>(child: W, shown: impl Observable<bool> + 'static) -> Presence
where
    W: Widget + 'static,
{
    Presence::new(child)
        .transition(crate::ui::widgets::reveal_transition())
        .shown_from(shown)
        .collapse(Axis::Vertical)
}

/// Keep a leaf control mounted and re-measured whenever `source` changes, so its `*_when`
/// readers pick up the new state.
fn follow<W, K>(control: W, source: impl Observable<K> + 'static) -> Restyle<W, K>
where
    W: Widget,
    K: Clone + PartialEq + 'static,
{
    Restyle::new(control, source, |control, _| control)
}

/// Fill the value column and align compact controls to the section's right edge.
fn inline_row<W: Widget + 'static>(label: impl Into<String>, control: W) -> FormRow {
    FormRow::new(
        label,
        Align::new(
            Alignment::Stretch,
            Alignment::Center,
            Align::new(Alignment::End, Alignment::Center, control),
        ),
    )
    .inline()
}

/// A read-only value row with right-aligned text, including wrapped lines.
fn value_row(
    shared: &Shared,
    reader: &ThemeReader,
    label: &str,
    value: impl Into<String>,
) -> FormRow {
    inline_row(label, value_text(shared, value)).theme_when(clone_reader(reader))
}

/// [`value_row`] whose value follows `value`.
fn value_row_from<O>(shared: &Shared, reader: &ThemeReader, label: &str, value: O) -> FormRow
where
    O: Observable<String> + Clone + 'static,
{
    inline_row(label, value_text_from(shared, value)).theme_when(clone_reader(reader))
}

/// Feedback changes frequently while editing. Keep it in a retained switch so an update never
/// replaces sibling text controls and invalidates their focus or caret state.
fn feedback_slot(
    shared: &Shared,
    reader: &ThemeReader,
    select: impl Fn(&SettingsStructureSnapshot) -> SettingsFeedback + Send + Sync + 'static,
) -> SwitchView {
    let feedback = settings_projection(shared, "settings feedback", select);
    let error = derived(feedback.clone(), "settings feedback error", |(error, _)| {
        error.clone().unwrap_or_default()
    });
    let status = derived(
        feedback.clone(),
        "settings feedback status",
        |(_, status)| status.clone().unwrap_or_default(),
    );
    let selected = derived(feedback, "settings feedback slot", |(error, status)| {
        if error.is_some() {
            1
        } else if status.is_some() {
            2
        } else {
            0
        }
    });
    let error_selection = SelectionScope::new();
    let error_document = {
        let error = error.clone();
        let s = shared.clone();
        move || {
            let mut style = settings_text_style(&s, TextSize::Base);
            style.color = palette(&s).bad_text;
            TextDocument::from_plain_text(error.get(), style)
        }
    };
    let error_text = follow(
        RichText::new(error_document())
            .document_when(error_document)
            .semantic_name("Settings error")
            .selectable(error_selection.clone())
            .copy_to_clipboard(true),
        error,
    );
    let error_text =
        read_only_text_menu(reader, "settings-error", error_text, error_selection, true);
    let status_label = muted_text(
        shared,
        Label::new(status.get()).text_from(status),
        TextSize::Base,
    );

    SwitchView::new()
        .with_child(SizedBox::new().height(0.0))
        .with_child(inline_row("Error", error_text).theme_when(clone_reader(reader)))
        .with_child(inline_row("Status", status_label).theme_when(clone_reader(reader)))
        .selected_from(selected)
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
        "{}:{}:{}:{}:{}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{}:{:?}:{:?}",
        item.key,
        item.owner,
        item.description,
        item.source,
        item.scope,
        item.module_id,
        item.module_label,
        item.module_description,
        item.operational_error,
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
            .with_item(input, FlexItem::fill())
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
        .child(selectable_read_only_area(reader, name, value.clone()));
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
            .gap(8.0)
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
        .read_only(true)
        .selection_scope(selection.clone())
        .copy_to_clipboard(true)
        .theme_when(clone_reader(reader));
    read_only_text_menu(reader, name, field, selection, has_text)
}

/// A read-only multiline field that behaves like selectable document text.
fn selectable_read_only_area(reader: &ThemeReader, name: &str, value: String) -> ContextMenu {
    let has_text = !value.is_empty();
    let selection = SelectionScope::new();
    let field = TextArea::new(name)
        .value(value)
        .read_only(true)
        .selection_scope(selection.clone())
        .copy_to_clipboard(true)
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
            .separator_before(true);
        vec![
            if has_selection {
                copy
            } else {
                copy.enabled(false)
            },
            if has_text {
                select_all
            } else {
                select_all.enabled(false)
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
        .checked(on)
        .theme_when(clone_reader(reader))
        .on_change(on_toggle);
    inline_row(label, switch).theme_when(clone_reader(reader))
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
    inline_row(label, select).theme_when(clone_reader(reader))
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
    for scope in shared.workspace_scopes() {
        if scope.workspace_kind() != Some(crate::state::WorkspaceTabKind::Chat) {
            continue;
        }
        if let Some(providers) = &effects.chat_providers {
            scope.chat_mut().refresh_providers(providers.clone());
        }
        if let Some(agents) = &effects.chat_agents {
            scope.chat_mut().refresh_agents(agents.clone());
        }
    }
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
    let theme_control = {
        let s = shared.clone();
        Select::new("theme")
            .options(["Light", "Dark", "Void", "System"])
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
    let preview = settings_label(
        shared,
        "Clear thinking starts with readable text.",
        TextSize::Lg,
    );

    let theme_and_text = SettingsCard::new("Theme and text")
        .row(
            SettingRow::new("Theme")
                .help("Void uses true black for OLED screens. System follows your OS.")
                .control(theme_control)
                .build(shared),
        )
        .row(
            SettingRow::new("Text size")
                .help("Changes text in messages, files, and settings without resizing controls.")
                .control(text_scale_select)
                .build(shared),
        )
        .row(SettingRow::new("Preview").control(preview).build(shared))
        .build(shared, reader);
    let display = SettingsCard::new("Display")
        .row(
            SettingRow::new("Color output")
                .help(render_mode_hint(shared.render_mode()))
                .control(render_select)
                .build(shared),
        )
        .build(shared, reader);

    let content = Stack::vertical()
        .gap(16.0)
        .alignment(Alignment::Stretch)
        .with_child(theme_and_text)
        .with_child(display);
    if cfg!(target_os = "android") {
        return content;
    }
    use crate::backend::app_config::{FileOpenAction, save_file_open_action};
    let files = shared.files();
    let preference = std::sync::Arc::clone(&files.open_files_externally);
    let selected = std::sync::Arc::clone(&preference);
    let data_dir = files.data_dir.clone();
    let picker = Select::new("Default file action")
        .options(["Open in Sinomo", "Open externally"])
        .selected_when(move || Some(usize::from(selected.load(Ordering::Relaxed))))
        .theme_when(clone_reader(reader))
        .on_change(move |index, _| {
            let action = if index == 1 {
                FileOpenAction::External
            } else {
                FileOpenAction::Internal
            };
            match save_file_open_action(&data_dir, action) {
                Ok(()) => preference.store(action == FileOpenAction::External, Ordering::Relaxed),
                Err(error) => tracing::warn!(%error, "could not save the default file action"),
            }
        });
    drop(files);
    content.with_child(SettingsCard::new("Files").row(SettingRow::new("Default file action")
        .help("Double-click a file or press Enter to open it. Right-click for the other option.")
        .control(picker).build(shared)).build(shared, reader))
}

/// Connection status first, since that is why people usually open this page, then the app build
/// and diagnostics. Identifiers stay behind "Show details". Connection changes update the page in
/// place; recovery issues are keyed by the surface they affect.
fn section_system(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    section_system_body(shared, reader)
}

fn section_system_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (version, app_surface_id, surface_role, data_directory) = {
        let settings = shared.settings();
        (
            settings.version.clone(),
            settings.node_id.clone(),
            app_surface_summary(&settings.role),
            settings.data_dir.root().display().to_string(),
        )
    };
    let platform = format!("{} · {}", std::env::consts::OS, std::env::consts::ARCH);
    let mode_label = settings_projection(shared, "settings system mode", |snapshot| {
        snapshot.connection.mode_label.clone()
    });
    let status_detail = settings_projection(shared, "settings system status", |snapshot| {
        snapshot.connection.status_detail.clone()
    });
    let cluster_summary = settings_projection(shared, "settings system cluster", |snapshot| {
        snapshot
            .node
            .cluster_summary
            .clone()
            .map(|summary| short_one_line(&summary, 96))
            .unwrap_or_else(|| {
                if snapshot.node.clustered {
                    "cluster status unavailable".to_string()
                } else {
                    "not clustered".to_string()
                }
            })
    });
    let issues = settings_projection(shared, "settings system issues", |snapshot| {
        snapshot.connection.issues.clone()
    });
    let issue_label = derived(
        issues.clone(),
        "settings system issue label",
        |issues: &Vec<SettingsConnectionIssueStructure>| match issues.len() {
            0 => "None".to_string(),
            1 => "1 needs attention".to_string(),
            count => format!("{count} need attention"),
        },
    );
    let issue_tone = derived(
        issues.clone(),
        "settings system issue tone",
        |issues: &Vec<SettingsConnectionIssueStructure>| {
            if issues.is_empty() {
                SemanticTone::Success
            } else {
                SemanticTone::Warning
            }
        },
    );
    let has_issues = derived(
        issues.clone(),
        "settings system has issues",
        |issues: &Vec<SettingsConnectionIssueStructure>| !issues.is_empty(),
    );

    let daemon_node_id = settings_projection(shared, "settings system daemon node", |snapshot| {
        snapshot.node.cluster_node_id.clone()
    });
    let has_daemon_node = derived(
        daemon_node_id.clone(),
        "settings system has daemon node",
        Option::is_some,
    );
    // A copyable identifier; rebuilt only if the daemon reports another node.
    let daemon_node = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::key_from(daemon_node_id, move |node_id| {
            WidgetPod::new(
                SettingRow::new("Daemon node ID")
                    .control(copy_control(
                        &r,
                        "system-daemon-node-id",
                        "Daemon node ID",
                        node_id.clone().unwrap_or_default(),
                    ))
                    .build(&s),
            )
        })
    };
    let identity = vec![
        widgets::Boxed::new(
            SettingRow::new("App surface ID")
                .control(copy_control(
                    reader,
                    "system-app-surface-id",
                    "App surface ID",
                    app_surface_id.clone(),
                ))
                .build(shared),
        ),
        widgets::Boxed::new(
            SettingRow::new("Surface role")
                .value(shared, surface_role.clone())
                .build(shared),
        ),
        widgets::Boxed::new(revealed(daemon_node, has_daemon_node)),
    ];
    let connection = SettingsCard::new("Connection")
        .row(
            SettingRow::new("Mode")
                .value_from(shared, mode_label)
                .build(shared),
        )
        .row(
            SettingRow::new("Status")
                .value_from(shared, status_detail)
                .build(shared),
        )
        .row(
            SettingRow::new("Cluster")
                .value_from(shared, cluster_summary)
                .build(shared),
        )
        .row(
            SettingRow::new("Recovery issues")
                .help("Chat and file problems that need your action.")
                .control(status_badge_from(reader, issue_label, issue_tone))
                .build(shared),
        )
        .row(details(
            shared,
            reader,
            "system-connection",
            "identifiers",
            identity,
        ))
        .build(shared, reader);

    let app = SettingsCard::new("Sinomo App")
        .row(
            SettingRow::new("Version")
                .value(shared, format!("v{version}"))
                .build(shared),
        )
        .row(
            SettingRow::new("Platform")
                .value(shared, platform.clone())
                .build(shared),
        )
        .row(details(
            shared,
            reader,
            "system-app",
            "build details",
            vec![widgets::Boxed::new(
                SettingRow::new("Toolkit")
                    .value(shared, "SUI")
                    .build(shared),
            )],
        ))
        .build(shared, reader);

    let diagnostic_summary = settings_projection(
        shared,
        "settings system diagnostics",
        move |snapshot| {
            let mode_label = &snapshot.connection.mode_label;
            let short_status = short_one_line(&snapshot.connection.status_detail, 96);
            format!(
                "Sinomo App v{version}\nPlatform: {platform}\nMode: {mode_label}\nStatus: {short_status}\nApp surface: {app_surface_id}\nSurface role: {surface_role}\nData directory: {data_directory}"
            )
        },
    );
    let copy_summary = {
        let summary = diagnostic_summary.clone();
        sui::Button::new(COPY_DIAGNOSTIC_SUMMARY_LABEL)
            .icon(IconGlyph::Restore)
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| ctx.set_clipboard_text(summary.get()))
    };
    // The selectable summary text; rebuilt only when its contents change.
    let summary_text = {
        let r = Rc::clone(reader);
        RebuildOnChange::key_from(diagnostic_summary, move |summary| {
            WidgetPod::new(
                SizedBox::new()
                    .height(112.0)
                    .child(selectable_read_only_area(
                        &r,
                        "system-diagnostic-summary",
                        summary.clone(),
                    )),
            )
        })
    };
    let data_directory = shared.settings().data_dir.root().display().to_string();
    let diagnostics = SettingsCard::new("Diagnostics")
        .row(
            SettingRow::new("Diagnostic summary")
                .help("Version, platform, connection, and data directory, for reporting a problem.")
                .control(copy_summary)
                .build(shared),
        )
        .row(
            SettingRow::new("Data directory")
                .control(copy_control(
                    reader,
                    "system-data-directory",
                    "Data directory",
                    data_directory,
                ))
                .build(shared),
        )
        .row(details(
            shared,
            reader,
            "system-diagnostics",
            "summary text",
            vec![widgets::Boxed::new(summary_text)],
        ))
        .build(shared, reader);

    let issue_rows = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        KeyedStack::vertical(
            issues,
            |issue: &SettingsConnectionIssueStructure| issue.title,
            move |_, issue| {
                let tab = issue.get().tab;
                connection_issue_row(&s, &r, tab, issue)
            },
        )
        .gap(14.0)
    };
    let attention = SettingsCard::new("Needs attention")
        .description("Each of these needs your action; the daemon itself may still be connected.")
        .row(issue_rows)
        .build(shared, reader);

    Column::new(reader, |_| 16.0)
        .with_child(connection)
        .with_child(revealed(attention, has_issues))
        .with_child(app)
        .with_child(diagnostics)
}

fn connection_issue_action(tab: Tab) -> &'static str {
    match tab {
        Tab::Sinomo => "Open Chat",
        Tab::Files => "Open Files",
        _ => "Open Settings",
    }
}

fn connection_issue_row(
    shared: &Shared,
    reader: &ThemeReader,
    tab: Tab,
    issue: Signal<SettingsConnectionIssueStructure>,
) -> FormRow {
    let as_issue = |issue: &SettingsConnectionIssueStructure| ConnectionIssue {
        title: issue.title,
        detail: issue.detail.clone(),
        tab: issue.tab,
        action: connection_issue_action(issue.tab),
    };
    let current = as_issue(&issue.get());
    let expanded = settings_projection(
        shared,
        "settings connection issue expanded",
        move |snapshot| snapshot.connection.expanded.contains(&tab),
    );
    let status = {
        let s = shared.clone();
        settings_label(shared, "Needs attention", TextSize::Sm).color_when(move || palette(&s).warn)
    };
    let summary = muted_meta_label(shared, connection_issue_summary(&current));
    let recovery = {
        let hint = issue.select_named("settings connection issue hint", move |issue| {
            connection_issue_recovery_hint(&as_issue(issue)).to_string()
        });
        muted_text(shared, Label::new(hint.get()).text_from(hint), TextSize::Sm)
    };
    let detail_label = {
        let s = shared.clone();
        settings_label(shared, "Low-level detail", TextSize::Sm)
            .color_when(move || palette(&s).text_faint)
    };
    let detail_field = {
        let name = match tab {
            Tab::Sinomo => "connection-chat-detail",
            Tab::Files => "connection-files-detail",
            _ => "connection-issue-detail",
        };
        let r = Rc::clone(reader);
        // The selectable detail text; rebuilt only when the reported detail changes.
        RebuildOnChange::key_from(
            issue.select_named("settings connection issue detail", |issue| {
                issue.detail.clone()
            }),
            move |detail| {
                WidgetPod::new(
                    SizedBox::new()
                        .height(68.0)
                        .child(selectable_read_only_area(&r, name, detail.clone())),
                )
            },
        )
    };
    let button = {
        let s = shared.clone();
        sui::Button::new(current.action)
            .icon(tab_action_icon(tab))
            .theme_when(clone_reader(reader))
            .on_press(move || open_tab(&s, tab))
    };
    let sifs_button = {
        let s = shared.clone();
        sui::Button::new("Check SIFS")
            .icon(tab_action_icon(Tab::Files))
            .theme_when(clone_reader(reader))
            .on_press(move || open_tab(&s, Tab::Files))
    };
    let detail_button = {
        let s = shared.clone();
        disclosure_restyle(
            sui::Button::new("Show detail")
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press(move || s.settings_mut().toggle_connection_detail(tab)),
            expanded.clone(),
            "Show detail",
            "Hide detail",
        )
    };
    let header = Stack::vertical()
        .gap(3.0)
        .alignment(Alignment::Stretch)
        .with_child(status)
        .with_child(summary);
    let controls = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Start)
        .with_child(button)
        .with_child(
            Presence::new(sifs_button)
                .transition(crate::ui::widgets::reveal_transition())
                .shown_from(
                    issue.select_named("settings connection issue SIFS", move |issue| {
                        connection_issue_has_sifs_recovery(&as_issue(issue))
                    }),
                )
                .collapse(Axis::Horizontal),
        )
        .with_child(detail_button);
    let action_row = Stack::vertical()
        .gap(5.0)
        .alignment(Alignment::Start)
        .with_child(controls)
        .with_child(recovery);
    let detail = Column::new(reader, |theme| theme.metrics.property_row_stacked_gap)
        .with_child(detail_label)
        .with_child(detail_field);
    let stack = Column::new(reader, |_| 8.0)
        .with_child(header)
        .with_child(action_row)
        .with_child(revealed(detail, expanded));

    FormRow::new(current.title, stack)
        .stacked()
        .theme_when(clone_reader(reader))
}

/// The selected node and its configuration. Only the node card follows the node it describes;
/// the configuration browser below stays mounted.
fn section_node(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let node = settings_projection(shared, "settings selected node", |snapshot| {
        snapshot.modules.selected_node.as_ref().map(|node| {
            (
                node.node_id.clone(),
                node.display_name.clone(),
                node.availability.clone(),
            )
        })
    });
    let s = shared.clone();
    let r = Rc::clone(reader);
    let card = RebuildOnChange::key_from(node, move |node| {
        WidgetPod::new(selected_node_card(&s, &r, node.clone()))
    });
    Stack::vertical()
        .gap(16.0)
        .alignment(Alignment::Stretch)
        .with_child(card)
        .with_child(section_configuration(shared, reader, true))
}

fn selected_node_card(
    shared: &Shared,
    reader: &ThemeReader,
    node: Option<(String, String, String)>,
) -> impl Widget + use<> {
    let mut card = SettingsCard::new("Selected node");
    if let Some((node_id, display_name, availability)) = node {
        card = card
            .row(
                SettingRow::new("Name")
                    .value(shared, display_name)
                    .build(shared),
            )
            .row(
                SettingRow::new("Availability")
                    .value(shared, availability)
                    .build(shared),
            )
            .row(details(
                shared,
                reader,
                "selected-node",
                "node ID",
                vec![widgets::Boxed::new(
                    SettingRow::new("Node ID")
                        .control(copy_control(reader, "settings-node-id", "Node ID", node_id))
                        .build(shared),
                )],
            ));
    }
    card.build(shared, reader)
}

/// The SIFS mount and the NFS mount, which both expose SIFS through the node's filesystem.
fn section_storage(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    Stack::vertical()
        .gap(16.0)
        .alignment(Alignment::Stretch)
        .with_child(section_sifs_mount(shared, reader))
        .with_child(storage_modules_host(shared, reader))
}

/// The modules behind the two mounts. The SIFS mount module shows only its lifecycle here, since
/// the mount settings above edit its configuration; the NFS module brings its own settings.
fn storage_modules_host(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let cards = settings_projection(shared, "settings storage module cards", storage_page_cards);
    let nfs_missing = settings_projection(shared, "settings storage NFS missing", |snapshot| {
        !storage_module_ids(&snapshot.configuration.items)
            .iter()
            .any(|(_, with_settings)| *with_settings)
    });
    Column::new(reader, |_| 16.0)
        .with_child(module_card_stack(shared, reader, cards))
        .with_child(revealed(
            SettingsCard::new("NFS mount")
                .row(
                    SettingRow::new("Status")
                        .value(
                            shared,
                            "The NFS mount module is not available on this node.",
                        )
                        .build(shared),
                )
                .build(shared, reader),
            nfs_missing,
        ))
}

/// Modules managed on the Storage page, each with whether Storage shows its settings.
#[cfg(test)]
fn storage_modules(settings: &SettingsState) -> Vec<(String, bool)> {
    let items = settings
        .config_items
        .iter()
        .map(ConfigItemStructure::from)
        .collect::<Vec<_>>();
    storage_module_ids(&items)
}

const SIFS_MOUNT_CONFIG_KEY: &str = "storage.sifs-mount";
const NFS_MOUNT_CONFIG_KEY: &str = "storage.sifs-nfs-mount";

/// One configuration browser with a scope switch, instead of separate node and cluster pages.
fn section_configuration_page(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let scope = {
        let s = shared.clone();
        let selected = shared.clone();
        sui::SegmentedControl::new("configuration-scope")
            .segments(CONFIGURATION_SCOPES)
            .selected_when(move || Some(usize::from(selected.settings().configuration_node_scope)))
            .theme_when(clone_reader(reader))
            .on_change_with_ctx(move |ctx, index, _| {
                s.settings_mut().configuration_node_scope = index == 1;
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_paint();
                ctx.request_semantics();
            })
    };
    let scope_card = SettingsCard::new("Scope")
        .row(
            SettingRow::new("Show settings for")
                .help("Cluster settings apply to every node; a device setting overrides them on one node.")
                .control(scope)
                .build(shared),
        )
        .build(shared, reader);
    let selected = {
        let s = shared.clone();
        move || usize::from(s.settings().configuration_node_scope)
    };
    let browser = SwitchView::new()
        .selected_when(selected)
        .with_child(section_configuration(shared, reader, false))
        .with_child(node_settings_page(
            shared,
            reader,
            section_node(shared, reader),
        ));
    Stack::vertical()
        .gap(16.0)
        .alignment(Alignment::Stretch)
        .with_child(scope_card)
        .with_child(browser)
}

const CONFIGURATION_SCOPES: [&str; 2] = ["Cluster", "This device"];

/// The supervisor's status and the mount settings. The status refreshes beside the settings, so
/// live updates never touch the form; the form is rebuilt only when the node's platform or the
/// availability of its stored configuration changes, which changes which controls apply.
fn section_sifs_mount(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let shape = settings_projection(shared, "settings SIFS mount editor shape", |snapshot| {
        Some(sifs_mount_editor_shape(&snapshot.sifs_mount))
    });
    let s = shared.clone();
    let r = Rc::clone(reader);
    let editor = RebuildOnChange::key_from(shape, move |_shape| {
        WidgetPod::new(section_sifs_mount_body(&s, &r))
    });
    Stack::vertical()
        .gap(16.0)
        .alignment(Alignment::Stretch)
        .with_child(sifs_mount_runtime_status(shared, reader))
        .with_child(editor)
}

/// What decides which mount controls apply: the node's platform, and whether its stored
/// configuration could be read.
fn sifs_mount_editor_shape(mount: &SettingsSifsMountStructure) -> (SifsMountPlatform, bool) {
    (
        mount.snapshot.platform,
        mount.snapshot.config_error.is_some(),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SifsMountRuntimePresentationKey {
    snapshot: SifsMountSettingsSnapshot,
    presentation: SifsMountPresentation,
}

/// The status card stays mounted, with its Refresh button; only the reported rows follow each
/// observation.
fn sifs_mount_runtime_status(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let refresh = {
        let s = shared.clone();
        let idle = settings_projection(shared, "settings SIFS mount idle", |snapshot| {
            !snapshot.sifs_mount.pending
        });
        sui::Button::new("Refresh status")
            .icon(IconGlyph::Redo)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled_from(idle)
            .on_press(move || {
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::SifsMount,
                    SettingsOperation::RefreshSifsMount {
                        update_feedback: true,
                    },
                );
            })
    };
    let key_shared = shared.clone();
    let build_shared = shared.clone();
    let build_reader = Rc::clone(reader);
    // Read-only observation rows. The presentation depends on the clock as well as the snapshot,
    // so the key is read on each pass rather than projected.
    let rows = RebuildOnChange::key_when(
        move || {
            let snapshot = key_shared.settings().sifs_mount_snapshot.clone();
            let presentation =
                sifs_mount_presentation_at(&snapshot, time::OffsetDateTime::now_utc());
            SifsMountRuntimePresentationKey {
                snapshot,
                presentation,
            }
        },
        move |key| {
            WidgetPod::new(build_sifs_mount_runtime_status(
                &build_shared,
                &build_reader,
                &key.snapshot,
                &key.presentation,
            ))
        },
    );
    let card = SettingsCard::new("SIFS mount")
        .description("What the mount supervisor on this node reports. A saved setting is not a mount until the supervisor confirms it.")
        .header_action(refresh)
        .row(rows)
        .build(shared, reader);
    SifsMountLiveRefresh::new(shared.clone(), card)
}

fn build_sifs_mount_runtime_status(
    shared: &Shared,
    reader: &ThemeReader,
    snapshot: &SifsMountSettingsSnapshot,
    presentation: &SifsMountPresentation,
) -> impl Widget + use<> {
    let status = &snapshot.status;
    let value = |label: &str, value: String| {
        widgets::Boxed::new(SettingRow::new(label).value(shared, value).build(shared))
    };

    let mut card = Column::new(reader, |theme| theme.metrics.form_section_body_gap)
        .with_child(
            SettingRow::new("Status")
                .help(presentation.detail.clone())
                .status(reader, presentation.label.clone(), presentation.tone)
                .build(shared),
        )
        .with_child(value(
            "Location",
            status
                .resolved_mount_point
                .clone()
                .unwrap_or_else(|| status.configured_mount_point.clone()),
        ))
        .with_child(value(
            "Access",
            if status.read_only {
                "Read only".to_string()
            } else {
                "Read and write".to_string()
            },
        ));
    if status.platform == SifsMountPlatform::ExternalProvider {
        card = card
            .with_child(value(
                "Writeback",
                sifs_mount_writeback_mode_label(status.writeback_mode).to_string(),
            ))
            .with_child(value(
                "Pending changes",
                format!(
                    "{} pending · {} retrying · {} failed · {} conflicts",
                    status.pending_operations,
                    status.retrying_operations,
                    status.failed_operations,
                    status.conflict_count
                ),
            ));
        for (index, attention) in status
            .attention
            .iter()
            .take(SIFS_MOUNT_ATTENTION_DISPLAY_LIMIT)
            .enumerate()
        {
            card = card.with_child(copyable_area_row(
                reader,
                &format!("sifs-mount-attention-{index}"),
                &format!(
                    "Attention {} · {}",
                    index + 1,
                    sifs_mount_attention_kind_label(attention.kind)
                ),
                sifs_mount_attention_detail(attention),
            ));
        }
        let locally_omitted = status
            .attention
            .len()
            .saturating_sub(SIFS_MOUNT_ATTENTION_DISPLAY_LIMIT)
            as u64;
        let attention_overflow = status.attention_overflow.saturating_add(locally_omitted);
        if attention_overflow > 0 {
            card = card.with_child(value(
                "Additional attention",
                format!(
                    "{attention_overflow} additional issues omitted from this bounded live status; inspect daemon logs and refresh after addressing the shown issues"
                ),
            ));
        }
    }
    if let Some(last_error) = status.last_error.as_ref() {
        card = card.with_child(value("Last error", last_error.clone()));
    }
    if let Some(action) = presentation.operator_action.as_ref() {
        card = card.with_child(value("Safe next action", action.clone()));
    }
    if let Some(error) = snapshot.status_error.as_ref() {
        card = card.with_child(value("Status unavailable", error.clone()));
    }
    // Supervisor internals for diagnosing a mount that does not come up.
    let mut technical = vec![
        value(
            "Platform adapter",
            sifs_mount_platform_label(status.platform).to_string(),
        ),
        value("Configured location", status.configured_mount_point.clone()),
        value("Observed SIFS root", status.root_path.clone()),
    ];
    if let Some(resolved) = status.resolved_mount_point.as_ref() {
        technical.push(widgets::Boxed::new(copyable_text_row(
            reader,
            "sifs-mount-resolved-location",
            "Resolved location",
            resolved.clone(),
        )));
    }
    if status.platform == SifsMountPlatform::ExternalProvider {
        technical.push(value(
            "Cloud connection",
            format!(
                "registered={} · connected={} · worker={} · ready={}",
                status.provider_registered,
                status.provider_connected,
                status.worker_alive,
                status.worker_ready
            ),
        ));
        if status.worker_health_generation > 0 {
            technical.push(value(
                "Worker heartbeat",
                status
                    .last_worker_heartbeat_at
                    .clone()
                    .map(|observed| {
                        format!(
                            "generation {} · {observed}",
                            status.worker_health_generation
                        )
                    })
                    .unwrap_or_else(|| format!("generation {}", status.worker_health_generation)),
            ));
        }
        if status.unregister_pending {
            technical.push(value(
                "Retired registration",
                "Cleanup pending; replacement activation remains blocked".to_string(),
            ));
        }
    }
    if let Some(pid) = status.daemon_pid {
        technical.push(value("Supervisor process", format!("daemon PID {pid}")));
    }
    if let Some(interface) = status.provider_interface_version.as_ref() {
        let provider = status
            .provider_content_hash
            .as_deref()
            .map(|hash| format!("{} · {}", interface, short(hash, 16)))
            .unwrap_or_else(|| interface.clone());
        technical.push(value("Provider module", provider));
    }
    if let Some(placeholders) = status.placeholders_created {
        technical.push(value("Placeholders created", placeholders.to_string()));
    }
    if status.restart_count > 0 {
        technical.push(value("Restarts", status.restart_count.to_string()));
    }
    if !status.updated_at.trim().is_empty() {
        technical.push(value("Last observation", status.updated_at.clone()));
    }
    card.with_child(details(
        shared,
        reader,
        "sifs-mount-runtime",
        "supervisor details",
        technical,
    ))
}

struct SifsMountLiveRefresh {
    shared: Shared,
    child: WidgetPod,
    timer: Cell<Option<(TimerToken, Instant)>>,
}

impl SifsMountLiveRefresh {
    fn new<W>(shared: Shared, child: W) -> Self
    where
        W: Widget + 'static,
    {
        Self {
            shared,
            child: WidgetPod::new(child),
            timer: Cell::new(None),
        }
    }

    fn visible(&self) -> bool {
        self.shared.active_tab() == Tab::Settings
            && self.shared.settings().selected == Some(SettingsSection::Storage)
    }

    fn schedule(&self, ctx: &mut EventCtx) {
        if !self.visible() {
            self.timer.set(None);
            return;
        }
        let now = Instant::now();
        if self.timer.get().is_some_and(|(_, deadline)| deadline > now) {
            return;
        }
        let token = ctx.schedule_timer_after(SIFS_MOUNT_LIVE_REFRESH_SECS);
        self.timer.set(Some((
            token,
            now + Duration::from_secs_f64(SIFS_MOUNT_LIVE_REFRESH_SECS),
        )));
    }

    fn queue_refresh(&self) {
        let should_refresh = {
            let settings = self.shared.settings();
            settings.selected == Some(SettingsSection::Storage) && !settings.sifs_mount_pending()
        };
        if should_refresh && self.shared.active_tab() == Tab::Settings {
            queue_settings_operation(
                &self.shared,
                SettingsOperationScope::SifsMount,
                SettingsOperation::RefreshSifsMount {
                    update_feedback: false,
                },
            );
        }
    }
}

impl Widget for SifsMountLiveRefresh {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target {
            return;
        }
        if let Event::Wake(sui::WakeEvent::Timer { token, .. }) = event
            && self
                .timer
                .get()
                .is_some_and(|(current, _)| current == *token)
        {
            self.timer.set(None);
            if self.visible() {
                self.queue_refresh();
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_paint();
                ctx.request_semantics();
                self.schedule(ctx);
            }
            return;
        }
        self.schedule(ctx);
    }

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

fn section_sifs_mount_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (platform, form) = {
        let settings = shared.settings();
        (
            settings.sifs_mount_snapshot.platform,
            settings.sifs_mount_form.clone(),
        )
    };
    let external = platform == SifsMountPlatform::ExternalProvider;
    // Reset, reload, and save replace the draft and bump this revision; the retained fields and
    // switches adopt the new values without being rebuilt.
    let revision = settings_projection(shared, "settings SIFS mount revision", |snapshot| {
        snapshot.sifs_mount.ui_revision
    });
    let form_flag = |name: &'static str, flag: fn(&SifsMountForm) -> bool| {
        settings_projection(shared, name, move |snapshot| {
            flag(&snapshot.sifs_mount.form)
        })
    };
    let input = |name: &str,
                 value: String,
                 placeholder: &str,
                 read: fn(&SifsMountForm) -> String,
                 edit: fn(&mut SifsMountForm, String)| {
        let s = shared.clone();
        let field = TextInput::new(name)
            .value(value)
            .placeholder(placeholder)
            .theme_when(clone_reader(reader))
            .on_change(move |value| s.settings_mut().edit_sifs_mount(|form| edit(form, value)));
        let s = shared.clone();
        SyncedTextInput::new(field, move || read(&s.settings().sifs_mount_form))
            .observing(revision.clone())
    };
    let mut card = SettingsCard::new("Mount settings")
        .description("Applies to the selected node. Sinomo picks the change up after you save.")
        .row(
            SettingRow::new("Enabled")
                .help("Mount SIFS into this node's filesystem.")
                .control(setting_switch_from(
                    reader,
                    "Enabled",
                    form_flag("settings SIFS mount enabled", |form| form.enabled),
                    {
                        let s = shared.clone();
                        move |enabled| {
                            s.settings_mut()
                                .edit_sifs_mount(|form| form.enabled = enabled);
                        }
                    },
                ))
                .build(shared),
        )
        .row(
            SettingRow::new(if external {
                "Sync root"
            } else {
                "Mount location"
            })
            .help(if external {
                "The local folder that shows SIFS files."
            } else {
                "Where SIFS appears in this node's filesystem."
            })
            .control(input(
                "sifs-mount-location",
                form.mount_point.clone(),
                if external {
                    r"C:\Users\you\Sinomo"
                } else {
                    "~/sinomo"
                },
                |form| form.mount_point.clone(),
                |form, value| form.mount_point = value,
            ))
            .build(shared),
        )
        .row(
            SettingRow::new("SIFS root")
                .help("The SIFS folder to expose. / shows everything.")
                .control(input(
                    "sifs-mount-root-path",
                    form.root_path.clone(),
                    "/",
                    |form| form.root_path.clone(),
                    |form, value| form.root_path = value,
                ))
                .build(shared),
        );
    match platform {
        SifsMountPlatform::UnixFuse => {
            card = card
                .row(
                    SettingRow::new("Read only")
                        .control(setting_switch_from(
                            reader,
                            "Read only",
                            form_flag("settings SIFS mount read only", |form| form.read_only),
                            {
                                let s = shared.clone();
                                move |read_only| {
                                    s.settings_mut()
                                        .edit_sifs_mount(|form| form.read_only = read_only);
                                }
                            },
                        ))
                        .build(shared),
                )
                .row(
                    SettingRow::new("Allow other local users")
                        .help("Owner-only by default; this also needs user_allow_other in /etc/fuse.conf.")
                        .control(setting_switch_from(
                            reader,
                            "Allow other local users",
                            form_flag("settings SIFS mount allow other", |form| form.allow_other),
                            {
                                let s = shared.clone();
                                move |allow_other| {
                                    s.settings_mut()
                                        .edit_sifs_mount(|form| form.allow_other = allow_other);
                                }
                            },
                        ))
                        .build(shared),
                );
        }
        SifsMountPlatform::ExternalProvider => {
            card = card.row(
                SettingRow::new("Durable writable cloud sync")
                    .help("Local changes are journaled and version-checked; conflicts stop for your review.")
                    .control(setting_switch_from(
                        reader,
                        "Durable writable cloud sync",
                        form_flag("settings SIFS mount writeback", |form| {
                            !form.read_only && form.sync_local_changes
                        }),
                        {
                            let s = shared.clone();
                            move |enabled| {
                                s.settings_mut().edit_sifs_mount(|form| {
                                    form.read_only = !enabled;
                                    form.sync_local_changes = enabled;
                                });
                            }
                        },
                    ))
                    .build(shared),
            );
        }
        SifsMountPlatform::Unsupported => {
            card = card.row(
                SettingRow::new("Platform")
                    .value(
                        shared,
                        "Host mounts are not supported on this device; enabling is rejected",
                    )
                    .build(shared),
            );
        }
    }
    if shared.settings().sifs_mount_snapshot.config_error.is_some() {
        let error = settings_projection(shared, "settings SIFS mount config error", |snapshot| {
            snapshot
                .sifs_mount
                .snapshot
                .config_error
                .clone()
                .unwrap_or_default()
        });
        card = card.row(
            SettingRow::new("Configuration unavailable")
                .value_from(shared, error)
                .build(shared),
        );
    }

    let can_save = settings_projection(shared, "settings SIFS mount can save", |snapshot| {
        snapshot.sifs_mount.dirty && !snapshot.sifs_mount.pending
    });
    let save = {
        let s = shared.clone();
        sui::Button::new("Save mount configuration")
            .icon(IconGlyph::Check)
            .primary_action()
            .theme_when(clone_reader(reader))
            .enabled_from(can_save.clone())
            .on_press(move || {
                let config = s.settings().sifs_mount_save_config();
                match config {
                    Ok(config) => {
                        queue_settings_operation(
                            &s,
                            SettingsOperationScope::SifsMount,
                            SettingsOperation::SaveSifsMount { config },
                        );
                    }
                    Err(error) => {
                        let mut settings = s.settings_mut();
                        settings.sifs_mount_action_error = Some(error);
                        settings.sifs_mount_action_status = None;
                    }
                }
            })
    };
    let reset = {
        let s = shared.clone();
        sui::Button::new("Reset draft")
            .icon(IconGlyph::Redo)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled_from(can_save)
            .on_press(move || s.settings_mut().reset_sifs_mount_form())
    };
    card.row(Align::new(
        Alignment::End,
        Alignment::Center,
        Flex::horizontal()
            .gap(8.0)
            .align_items(Alignment::Center)
            .with_child(reset)
            .with_child(save),
    ))
    .row(feedback_slot(shared, reader, |snapshot| {
        snapshot.feedback.sifs_mount.clone()
    }))
    .row(details(
        shared,
        reader,
        "sifs-mount-settings",
        "storage key",
        vec![widgets::Boxed::new(
            SettingRow::new("Stored in")
                .help(format!(
                    "One node-local {} mount, applied by the main Sinomo process.",
                    sifs_mount_platform_label(platform)
                ))
                .value(shared, "storage.sifs-mount")
                .build(shared),
        )],
    ))
    .build(shared, reader)
}

fn sifs_mount_platform_label(platform: SifsMountPlatform) -> &'static str {
    match platform {
        SifsMountPlatform::UnixFuse => "Unix FUSE",
        SifsMountPlatform::ExternalProvider => "External provider",
        SifsMountPlatform::Unsupported => "Unsupported platform",
    }
}

fn sifs_mount_phase_label(phase: SifsMountRuntimePhase) -> &'static str {
    match phase {
        SifsMountRuntimePhase::Disabled => "Disabled",
        SifsMountRuntimePhase::WaitingForDaemon => "Waiting for daemon",
        SifsMountRuntimePhase::Starting => "Starting",
        SifsMountRuntimePhase::Registered => "Registered",
        SifsMountRuntimePhase::Mounted => "Mounted",
        SifsMountRuntimePhase::Connected => "Connected",
        SifsMountRuntimePhase::Stopping => "Stopping",
        SifsMountRuntimePhase::Error => "Error",
    }
}

fn sifs_mount_writeback_mode_label(mode: SifsMountWritebackMode) -> &'static str {
    match mode {
        SifsMountWritebackMode::Disabled => "Disabled",
        SifsMountWritebackMode::Unsupported => "Unsupported",
        SifsMountWritebackMode::ReadOnlyEnforcementUnavailable => {
            "Read-only enforcement unavailable"
        }
        SifsMountWritebackMode::Starting => "Starting",
        SifsMountWritebackMode::Active => "Active",
        SifsMountWritebackMode::Blocked => "Blocked",
    }
}

fn sifs_mount_phase_tone(phase: SifsMountRuntimePhase) -> SemanticTone {
    match phase {
        SifsMountRuntimePhase::Mounted | SifsMountRuntimePhase::Connected => SemanticTone::Success,
        SifsMountRuntimePhase::WaitingForDaemon
        | SifsMountRuntimePhase::Starting
        | SifsMountRuntimePhase::Registered
        | SifsMountRuntimePhase::Stopping => SemanticTone::Warning,
        SifsMountRuntimePhase::Error => SemanticTone::Danger,
        SifsMountRuntimePhase::Disabled => SemanticTone::Neutral,
    }
}

fn sifs_mount_attention_kind_label(kind: SifsMountAttentionKind) -> &'static str {
    match kind {
        SifsMountAttentionKind::LocalOperationConflict => "Local operation conflict",
        SifsMountAttentionKind::FailedLocalOperation => "Failed local operation",
        SifsMountAttentionKind::RemoteNamespaceConflict => "Remote namespace conflict",
        SifsMountAttentionKind::StartupConflict => "Startup conflict",
        SifsMountAttentionKind::UnsupportedObjectKind => "Unsupported object",
        SifsMountAttentionKind::StaleOrAmbiguousReconciliation => "Ambiguous reconciliation",
    }
}

fn sifs_mount_attention_next_action_label(action: SifsMountAttentionNextAction) -> &'static str {
    match action {
        SifsMountAttentionNextAction::ReviewAndResolve => {
            "Review the local and remote versions, preserve both, and resolve explicitly; Sinomo will not choose a winner."
        }
        SifsMountAttentionNextAction::RepairCauseAndRetry => {
            "Preserve retained local evidence, repair the reported cause, and refresh status to retry."
        }
        SifsMountAttentionNextAction::PreserveLocalCopyAndRescan => {
            "Preserve or move the local copy outside the mount, then retry reconciliation. Sinomo will clear the issue automatically when the local path matches the authoritative delete."
        }
        SifsMountAttentionNextAction::ReviewUnsupportedEntry => {
            "Review the unsupported entry and preserve it outside the sync workflow before making any change."
        }
        SifsMountAttentionNextAction::WaitForAuthoritativeReconciliation => {
            "Keep the mount enabled and wait for authoritative reconciliation; refresh status before taking action."
        }
    }
}

fn sifs_mount_attention_detail(attention: &SifsMountAttentionSummary) -> String {
    let path = if attention.path.trim().is_empty() {
        "(mount-wide issue)"
    } else {
        attention.path.as_str()
    };
    let mut lines = vec![
        format!("Path: {path}"),
        format!(
            "Reason: {} · {}",
            attention.reason_code,
            short_one_line(&attention.reason, 512)
        ),
    ];
    if let Some(evidence) = attention.retained_local_evidence.as_ref() {
        let mut retained = Vec::new();
        if let Some(operation_id) = evidence.operation_id.as_deref() {
            retained.push(format!("operation {operation_id}"));
        }
        if let Some(evidence_id) = evidence.evidence_id.as_deref() {
            retained.push(format!("evidence {evidence_id}"));
        }
        if let Some(hash) = evidence.staged_content_hash.as_deref() {
            retained.push(format!("staged content {hash}"));
        }
        if let Some(size) = evidence.staged_content_size_bytes {
            retained.push(format!("{size} bytes"));
        }
        if let Some(local_state) = evidence.local_state.as_deref() {
            retained.push(format!("local state {}", short_one_line(local_state, 128)));
        }
        if !retained.is_empty() {
            lines.push(format!("Retained evidence: {}", retained.join(" · ")));
        }
    }
    lines.push(format!(
        "Safe next action: {}",
        sifs_mount_attention_next_action_label(attention.safe_next_action)
    ));
    lines.join("\n")
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SifsMountPresentation {
    label: String,
    tone: SemanticTone,
    detail: String,
    operator_action: Option<String>,
}

fn sifs_mount_presentation_at(
    snapshot: &SifsMountSettingsSnapshot,
    now: time::OffsetDateTime,
) -> SifsMountPresentation {
    let status = &snapshot.status;
    let needs_attention = |detail: String, operator_action: &str| SifsMountPresentation {
        label: "Needs attention".to_string(),
        tone: SemanticTone::Danger,
        detail,
        operator_action: Some(operator_action.to_string()),
    };

    if let Some(error) = snapshot.status_error.as_deref() {
        return needs_attention(
            format!("{error}; last observation: {}", status.detail),
            "Refresh status. If it remains unavailable, inspect the local daemon before changing the mount configuration.",
        );
    }
    if status.phase == SifsMountRuntimePhase::Connected
        && let Err(error) = validate_connected_host_filesystem_provider_status_at(status, now)
    {
        return needs_attention(
            format!("{error}; last observation: {}", status.detail),
            "Refresh status. Local writes must not be treated as synchronized until a new worker progress heartbeat is observed.",
        );
    }
    if status.unregister_pending {
        return needs_attention(
            format!(
                "A retired host-filesystem provider still requires cleanup; {}",
                status.detail
            ),
            "Keep the daemon running and refresh status. Replacement activation stays blocked until cleanup completes.",
        );
    }
    if status.failed_operations > 0 {
        return needs_attention(
            format!(
                "{} failed writeback operations require review; latest detail: {}",
                status.failed_operations,
                status
                    .last_error
                    .clone()
                    .unwrap_or_else(|| status.detail.clone())
            ),
            "Keep the mount enabled so journaled work remains recoverable. Preserve retained evidence, repair the reported cause, and refresh status.",
        );
    }
    if status.conflict_count > 0 || !status.attention.is_empty() || status.attention_overflow > 0 {
        let evidence = status.attention.first().map_or_else(
            || {
                status
                    .last_error
                    .clone()
                    .unwrap_or_else(|| status.detail.clone())
            },
            |attention| {
                format!(
                    "{} at {}: {} · {}",
                    sifs_mount_attention_kind_label(attention.kind),
                    attention.path,
                    attention.reason_code,
                    short_one_line(&attention.reason, 256)
                )
            },
        );
        let overflow = (status.attention_overflow > 0).then(|| {
            format!(
                "; {} additional bounded attention summaries omitted",
                status.attention_overflow
            )
        });
        let action = status.attention.first().map_or(
            "Keep the mount enabled so journaled work remains recoverable. Review the reported local path or provider error, then refresh; conflicts are never overwritten automatically.",
            |attention| sifs_mount_attention_next_action_label(attention.safe_next_action),
        );
        let connected = status.phase == SifsMountRuntimePhase::Connected && status.worker_ready;
        return SifsMountPresentation {
            label: if connected {
                "Mounted with conflicts".to_string()
            } else {
                "Needs attention".to_string()
            },
            tone: if connected {
                SemanticTone::Warning
            } else {
                SemanticTone::Danger
            },
            detail: format!(
                "{} local/remote conflicts require review; unrelated paths {} available; latest detail: {evidence}{}",
                status.conflict_count,
                if connected { "remain" } else { "are not yet" },
                overflow.as_deref().unwrap_or_default()
            ),
            operator_action: Some(action.to_string()),
        };
    }
    if let Some(error) = status.last_error.as_deref() {
        return needs_attention(
            format!("{}; latest error: {error}", status.detail),
            "Refresh status after inspecting the local daemon and provider error. Do not assume pending local writes are synchronized.",
        );
    }
    if status.pending_operations > 0 || status.retrying_operations > 0 {
        return SifsMountPresentation {
            label: if status.phase == SifsMountRuntimePhase::Connected {
                "Syncing".to_string()
            } else {
                sifs_mount_phase_label(status.phase).to_string()
            },
            tone: SemanticTone::Warning,
            detail: format!(
                "{}; {} operations pending and {} retrying",
                status.detail, status.pending_operations, status.retrying_operations
            ),
            operator_action: None,
        };
    }
    SifsMountPresentation {
        label: sifs_mount_phase_label(status.phase).to_string(),
        tone: sifs_mount_phase_tone(status.phase),
        detail: status.detail.clone(),
        operator_action: None,
    }
}

fn sifs_mount_section_presentation_at(
    snapshot: &SifsMountSettingsSnapshot,
    action_error: Option<&str>,
    now: time::OffsetDateTime,
) -> SifsMountPresentation {
    if let Some(error) = snapshot.config_error.as_deref().or(action_error) {
        return SifsMountPresentation {
            label: "Needs attention".to_string(),
            tone: SemanticTone::Danger,
            detail: error.to_string(),
            operator_action: None,
        };
    }
    sifs_mount_presentation_at(snapshot, now)
}

/// Host access for the selected node: the local consent controls when it is this device, or the
/// commands to run on it otherwise. Both stay mounted and update in place.
fn section_remote_management(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let remote = settings_projection(shared, "settings remote management scope", |snapshot| {
        usize::from(!snapshot.remote_management.local)
    });
    SwitchView::new()
        .with_child(section_remote_management_body(shared, reader))
        .with_child(remote_host_access_panel(shared, reader))
        .selected_from(remote)
}

/// The state of a node-local permission document.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PermissionState {
    #[default]
    Loading,
    Unavailable,
    Allowed,
    Denied,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PermissionView {
    state: PermissionState,
    enabled: bool,
    pending: bool,
    confirming: bool,
    error: Option<String>,
}

impl PermissionView {
    fn new(
        loading: bool,
        error: Option<String>,
        enabled: bool,
        pending: bool,
        confirming: bool,
    ) -> Self {
        let state = if loading {
            PermissionState::Loading
        } else if error.is_some() {
            PermissionState::Unavailable
        } else if enabled {
            PermissionState::Allowed
        } else {
            PermissionState::Denied
        };
        Self {
            state,
            enabled,
            pending,
            confirming,
            error,
        }
    }

    /// Which control the permission row shows: progress, a refresh, the switch, or the
    /// confirmation entry point.
    fn control(&self) -> usize {
        if self.pending {
            0
        } else {
            match self.state {
                PermissionState::Loading | PermissionState::Unavailable => 1,
                PermissionState::Allowed => 2,
                PermissionState::Denied => 3,
            }
        }
    }

    fn label_and_tone(&self) -> (&'static str, SemanticTone) {
        match self.state {
            PermissionState::Loading => ("Loading", SemanticTone::Neutral),
            PermissionState::Unavailable => ("Unavailable", SemanticTone::Danger),
            PermissionState::Allowed => ("Allowed", SemanticTone::Warning),
            PermissionState::Denied => ("Denied", SemanticTone::Neutral),
        }
    }
}

/// The wording and actions of one node-local permission card.
struct PermissionCard {
    title: &'static str,
    description: &'static str,
    /// Help for the loading, unavailable, allowed, and denied states.
    details: [&'static str; 4],
    switch_name: &'static str,
    allow_label: &'static str,
    pending_name: &'static str,
    confirm_region: &'static str,
    confirm_help: &'static str,
    confirm_description: &'static str,
    error_name: &'static str,
    scope: fn() -> SettingsOperationScope,
    set: fn(bool) -> SettingsOperation,
    confirm: fn(&mut SettingsState),
    cancel: fn(&mut SettingsState),
    view: fn(&SettingsStructureSnapshot) -> PermissionView,
    feedback: fn(&SettingsStructureSnapshot) -> SettingsFeedback,
}

/// A permission card whose badge, control, confirmation, and feedback follow the permission
/// state in place. `extra` rows sit between the confirmation and the error details.
fn permission_card(
    shared: &Shared,
    reader: &ThemeReader,
    card: PermissionCard,
    extra: Vec<widgets::Boxed>,
) -> SettingsCard {
    let view = settings_projection(shared, card.title, card.view);
    let label = derived(
        view.clone(),
        "settings permission label",
        |view: &PermissionView| view.label_and_tone().0.to_string(),
    );
    let tone = derived(
        view.clone(),
        "settings permission tone",
        |view: &PermissionView| view.label_and_tone().1,
    );
    let details = card.details;
    let detail = derived(
        view.clone(),
        "settings permission detail",
        move |view: &PermissionView| {
            let index = match view.state {
                PermissionState::Loading => 0,
                PermissionState::Unavailable => 1,
                PermissionState::Allowed => 2,
                PermissionState::Denied => 3,
            };
            details[index].to_string()
        },
    );
    let scope = card.scope;
    let set = card.set;
    let refresh = {
        let s = shared.clone();
        sui::Button::new("Refresh")
            .icon(IconGlyph::Redo)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || {
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::RemoteManagementRefresh,
                    SettingsOperation::RefreshRemoteManagement,
                );
            })
    };
    let switch = {
        let s = shared.clone();
        synced_switch(
            Switch::new("")
                .semantic_name(card.switch_name)
                .checked(true)
                .theme_when(clone_reader(reader))
                .on_change(move |enabled| {
                    if !enabled {
                        queue_settings_operation(&s, scope(), set(false));
                    }
                }),
            derived(
                view.clone(),
                "settings permission enabled",
                |view: &PermissionView| view.enabled,
            ),
        )
    };
    let allow = {
        let s = shared.clone();
        let confirm = card.confirm;
        sui::Button::new(card.allow_label)
            .icon(IconGlyph::Lock)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Warning)
            .on_press(move || confirm(&mut s.settings_mut()))
    };
    let control = SwitchView::new()
        .with_child(
            Flex::horizontal()
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(Spinner::new(card.pending_name).theme_when(clone_reader(reader)))
                .with_child(muted_label(shared, "Applying local permission…")),
        )
        .with_child(refresh)
        .with_child(switch)
        .with_child(allow)
        .selected_from(derived(
            view.clone(),
            "settings permission control",
            PermissionView::control,
        ));

    let confirm = {
        let s = shared.clone();
        sui::Button::new("Confirm allow")
            .icon(IconGlyph::Lock)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Filled)
            .tone(SemanticTone::Danger)
            .on_press(move || {
                queue_settings_operation(&s, scope(), set(true));
            })
    };
    let cancel = {
        let s = shared.clone();
        let cancel = card.cancel;
        sui::Button::new("Cancel")
            .icon(IconGlyph::Close)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || cancel(&mut s.settings_mut()))
    };
    let confirmation = SemanticRegion::new(
        card.confirm_region,
        SettingRow::new("Confirm access")
            .help(card.confirm_help)
            .control(
                Flex::horizontal()
                    .gap(8.0)
                    .align_items(Alignment::Center)
                    .with_child(cancel)
                    .with_child(confirm),
            )
            .build(shared),
    )
    .description(card.confirm_description)
    .role(SemanticsRole::Dialog);
    let confirming = derived(
        view.clone(),
        "settings permission confirming",
        |view: &PermissionView| view.confirming && !view.enabled && !view.pending,
    );

    let error = derived(
        view.clone(),
        "settings permission error",
        |view: &PermissionView| view.error.clone(),
    );
    let has_error = derived(
        error.clone(),
        "settings permission has error",
        Option::is_some,
    );
    let error_name = card.error_name;
    // The selectable error text; rebuilt only when the error itself changes.
    let error_row = {
        let r = Rc::clone(reader);
        RebuildOnChange::key_from(error, move |error| match error.clone() {
            Some(error) => {
                WidgetPod::new(copyable_area_row(&r, error_name, "Permission error", error))
            }
            None => WidgetPod::new(SizedBox::new()),
        })
    };

    let mut result = SettingsCard::new(card.title)
        .description(card.description)
        .header_action(status_badge_from(reader, label, tone))
        .row(
            SettingRow::new("Permission")
                .help_from(detail)
                .control(control)
                .build(shared),
        )
        .row(revealed(confirmation, confirming));
    for row in extra {
        result = result.row(row);
    }
    let feedback = card.feedback;
    result
        .row(revealed(error_row, has_error))
        .row(feedback_slot(shared, reader, feedback))
}

fn section_remote_management_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    // ------------------------------------------------------------------ local permission
    let exposure = settings_projection(shared, "settings remote management exposure", |snapshot| {
        let snapshot = &snapshot.remote_management.snapshot;
        if snapshot.resource_exposure_count == 0 {
            return String::new();
        }
        format!(
            "{} operation{} across {} resource{}",
            snapshot.resource_operation_count,
            if snapshot.resource_operation_count == 1 {
                ""
            } else {
                "s"
            },
            snapshot.resource_exposure_count,
            if snapshot.resource_exposure_count == 1 {
                ""
            } else {
                "s"
            },
        )
    });
    let has_exposure = derived(
        exposure.clone(),
        "settings remote management has exposure",
        |exposure: &String| !exposure.is_empty(),
    );
    let permission = permission_card(
        shared,
        reader,
        PermissionCard {
            title: "Remote management",
            description: "Stored only on this node and applied immediately. Module activation and remote requests can never change it.",
            details: [
                "Reading the node-local permission document.",
                "Permission state could not be read, so access stays blocked.",
                "Signed local or voter administrators may use unrestricted host-management routes.",
                "Remote host-management effects are blocked on this node.",
            ],
            switch_name: "Allow remote management",
            allow_label: "Allow remote management…",
            pending_name: "remote-management-permission-pending",
            confirm_region: "Allow remote management confirmation",
            confirm_help: "This lets signed local or voter administrators run shell commands, transfer files, inspect the host, and change ordinary node configuration.",
            confirm_description: "Confirmation is required before changing the local host permission",
            error_name: "remote-management-permission-error",
            scope: || SettingsOperationScope::RemoteManagementPermission,
            set: |enabled| SettingsOperation::SetRemoteManagementPermission { enabled },
            confirm: SettingsState::confirm_remote_management_enable,
            cancel: SettingsState::cancel_remote_management_enable,
            view: |snapshot| {
                let remote = &snapshot.remote_management;
                PermissionView::new(
                    remote.snapshot.permission_loading,
                    remote.snapshot.permission_error.clone(),
                    remote.snapshot.permission_enabled,
                    remote.permission_pending,
                    remote.enable_confirming,
                )
            },
            feedback: |snapshot| {
                (
                    snapshot.remote_management.permission_error.clone(),
                    snapshot.remote_management.permission_status.clone(),
                )
            },
        },
        vec![widgets::Boxed::new(revealed(
            SettingRow::new("Other device exposure")
                .help("Managed separately from remote management.")
                .value_from(shared, exposure)
                .build(shared),
            has_exposure,
        ))],
    );

    // ------------------------------------------------------------------ module on this node
    let module = settings_projection(shared, "settings remote management module", |snapshot| {
        snapshot.remote_management.snapshot.clone()
    });
    let override_control = settings_projection(
        shared,
        "settings remote management override control",
        |snapshot| {
            let remote = &snapshot.remote_management;
            if remote.module_pending {
                0
            } else if remote.snapshot.module_loading {
                1
            } else if remote.snapshot.module_node_id.is_none() {
                2
            } else {
                3
            }
        },
    );
    let module_override = {
        let refresh = {
            let s = shared.clone();
            sui::Button::new("Refresh")
                .icon(IconGlyph::Redo)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press(move || {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::RemoteManagementRefresh,
                        SettingsOperation::RefreshRemoteManagement,
                    );
                })
        };
        let s = shared.clone();
        let select = Select::new("Remote management node override")
            .options(MODULE_OVERRIDE_OPTIONS.iter().copied())
            .selected_when({
                let s = s.clone();
                move || {
                    Some(match s.settings().remote_management.module_override {
                        Some(true) => 1,
                        Some(false) => 2,
                        None => 0,
                    })
                }
            })
            .theme_when(clone_reader(reader))
            .on_change(move |index, _label| {
                let enabled = match index {
                    1 => Some(true),
                    2 => Some(false),
                    _ => None,
                };
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::RemoteManagementModule,
                    SettingsOperation::SetRemoteManagementModuleOverride { enabled },
                );
            });
        let busy = |name: &str, text: &str| {
            Flex::horizontal()
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(Spinner::new(name).theme_when(clone_reader(reader)))
                .with_child(muted_label(shared, text))
        };
        SwitchView::new()
            .with_child(busy(
                "remote-management-module-pending",
                "Saving desired state…",
            ))
            .with_child(busy(
                "remote-management-module-loading",
                "Reading local module state…",
            ))
            .with_child(
                Flex::horizontal()
                    .gap(8.0)
                    .align_items(Alignment::Center)
                    .with_child(muted_label(shared, "Local node configuration unavailable"))
                    .with_child(refresh),
            )
            .with_child(select)
            .selected_from(override_control)
    };
    let open_modules = {
        let s = shared.clone();
        sui::Button::new("Open Modules")
            .icon(IconGlyph::Blocks)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || s.settings_mut().open(SettingsSection::Modules))
    };
    let loaded = derived(
        module.clone(),
        "settings remote management module loaded",
        |module: &RemoteManagementSettingsSnapshot| !module.module_loading,
    );
    let desired = SettingRow::new("Desired state")
        .help_from(derived(
            module.clone(),
            "settings remote management desired source",
            |module: &RemoteManagementSettingsSnapshot| {
                module_effective_source_label(module.module_effective_source).to_string()
            },
        ))
        .control(status_badge_from(
            reader,
            derived(
                module.clone(),
                "settings remote management desired label",
                |module: &RemoteManagementSettingsSnapshot| {
                    if module.module_effective_enabled {
                        "Enabled"
                    } else {
                        "Disabled"
                    }
                    .to_string()
                },
            ),
            derived(
                module.clone(),
                "settings remote management desired tone",
                |module: &RemoteManagementSettingsSnapshot| {
                    if module.module_effective_enabled {
                        SemanticTone::Success
                    } else {
                        SemanticTone::Neutral
                    }
                },
            ),
        ))
        .build(shared);
    let running = SettingRow::new("Running")
        .help_from(derived(
            module.clone(),
            "settings remote management observed detail",
            |module: &RemoteManagementSettingsSnapshot| module.module_observed_detail.clone(),
        ))
        .control(status_badge_from(
            reader,
            derived(
                module.clone(),
                "settings remote management observed label",
                |module: &RemoteManagementSettingsSnapshot| {
                    remote_management_observed_label(module.module_observed).to_string()
                },
            ),
            derived(
                module.clone(),
                "settings remote management observed tone",
                |module: &RemoteManagementSettingsSnapshot| {
                    module_observed_tone(module.module_observed)
                },
            ),
        ))
        .build(shared);
    let attention = derived(
        module.clone(),
        "settings remote management attention",
        |module: &RemoteManagementSettingsSnapshot| {
            remote_management_module_attention(module)
                .filter(|_| !module.module_loading)
                .map(|(label, detail)| (label.to_string(), detail.to_string()))
        },
    );
    let needs_attention = SettingRow::new("Needs attention")
        .help_from(derived(
            attention.clone(),
            "settings remote management attention detail",
            |attention: &Option<(String, String)>| {
                attention
                    .as_ref()
                    .map(|(_, detail)| detail.clone())
                    .unwrap_or_default()
            },
        ))
        .control(status_badge_from(
            reader,
            derived(
                attention.clone(),
                "settings remote management attention label",
                |attention: &Option<(String, String)>| {
                    attention
                        .as_ref()
                        .map(|(label, _)| label.clone())
                        .unwrap_or_default()
                },
            ),
            derived(
                attention.clone(),
                "settings remote management attention tone",
                |_: &Option<(String, String)>| SemanticTone::Warning,
            ),
        ))
        .build(shared);
    let module_error = derived(
        module.clone(),
        "settings remote management module error",
        |module: &RemoteManagementSettingsSnapshot| module.module_error.clone().unwrap_or_default(),
    );
    let node_identity = derived(
        module,
        "settings remote management node identity",
        |module: &RemoteManagementSettingsSnapshot| {
            format!(
                "{} · {}",
                module.module_node_label,
                module
                    .module_node_id
                    .as_deref()
                    .map(|id| short(id, 18))
                    .unwrap_or_else(|| "identity unavailable".to_string())
            )
        },
    );
    let module = SettingsCard::new("Remote management module")
        .description("Turns the module on or off for this node. It does not grant host permission or install a missing module.")
        .row(
            SettingRow::new("Node override")
                .help("Inherit follows the cluster default.")
                .control(module_override)
                .build(shared),
        )
        .row(revealed(desired, loaded.clone()))
        .row(revealed(running, loaded.clone()))
        .row(revealed(
            needs_attention,
            derived(attention, "settings remote management has attention", Option::is_some),
        ))
        .row(revealed(
            SettingRow::new("Module error")
                .value_from(shared, module_error.clone())
                .build(shared),
            derived(module_error, "settings remote management has module error", |error: &String| {
                !error.is_empty()
            }),
        ))
        .row(feedback_slot(shared, reader, |snapshot| {
            (
                snapshot.remote_management.module_error.clone(),
                snapshot.remote_management.module_status.clone(),
            )
        }))
        .row(
            SettingRow::new("Module settings")
                .help("Configure the module itself in Modules.")
                .control(open_modules)
                .build(shared),
        )
        .row(revealed(
            details(
                shared,
                reader,
                "remote-management-module",
                "node identity",
                vec![widgets::Boxed::new(
                    SettingRow::new("Node")
                        .value_from(shared, node_identity)
                        .build(shared),
                )],
            ),
            loaded,
        ));

    // ------------------------------------------------------------------ other nodes
    let commands = SettingsCard::new("Other nodes")
        .description("Run these in a trusted shell on each node, with its normal SINOMO_DATA_DIR. Remote callers cannot grant themselves access.")
        .row(
            SettingRow::new("Allow on a node")
                .control(copy_control(
                    reader,
                    "remote-management-allow-command",
                    "Allow host permission command",
                    REMOTE_MANAGEMENT_ALLOW_COMMAND.to_string(),
                ))
                .build(shared),
        )
        .row(
            SettingRow::new("Deny on a node")
                .control(copy_control(
                    reader,
                    "remote-management-deny-command",
                    "Deny host permission command",
                    REMOTE_MANAGEMENT_DENY_COMMAND.to_string(),
                ))
                .build(shared),
        )
        .row(details(
            shared,
            reader,
            "remote-management-commands", "why these run locally",
            vec![widgets::Boxed::new(
                SettingRow::new("Why no --target")
                    .help("Permission commands deliberately take no --target or --node, so a node's grant can only be changed on that node. Module installation and desired state stay separate.")
                    .build(shared),
            )],
        ));

    let mut devices = SettingsCard::new("Device sharing")
        .description("Allow authenticated cluster peers to use this node's default devices. Android permissions and device availability still apply. Turn sharing off to revoke access.");
    for (kind, label) in [
        ("microphone", "Microphone"),
        ("speaker", "Speaker"),
        ("camera", "Camera"),
        ("motion", "Motion and environment sensors"),
    ] {
        let s = shared.clone();
        let checked = settings_projection(shared, "settings device exposure", move |snapshot| {
            snapshot
                .remote_management
                .snapshot
                .shared_devices
                .iter()
                .any(|value| value == kind)
        });
        devices = devices.row(
            SettingRow::new(label)
                .control(setting_switch_from(
                    reader,
                    &format!("Share {label}"),
                    checked,
                    move |enabled| {
                        queue_settings_operation(
                            &s,
                            SettingsOperationScope::RemoteManagementPermission,
                            SettingsOperation::SetDeviceExposure {
                                kind: kind.into(),
                                enabled,
                            },
                        );
                    },
                ))
                .build(shared),
        );
    }
    #[cfg(target_os = "android")]
    {
        let s = shared.clone();
        devices = devices.row(
            SettingRow::new("App Use")
                .help("Share the display and enable accessibility for touch and text input.")
                .control(
                    sui::Button::new("Set up App Use")
                        .theme_when(clone_reader(reader))
                        .on_press(move || {
                            queue_settings_operation(
                                &s,
                                SettingsOperationScope::RemoteManagementPermission,
                                SettingsOperation::OpenAndroidMediaSettings,
                            );
                        }),
                )
                .build(shared),
        );
    }

    Stack::vertical()
        .gap(16.0)
        .alignment(Alignment::Stretch)
        .with_child(permission.build(shared, reader))
        .with_child(devices.build(shared, reader))
        .with_child(module.build(shared, reader))
        .with_child(node_external_integrations_permission_section(
            shared, reader,
        ))
        .with_child(commands.build(shared, reader))
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

/// Membership. The page changes shape only when this device joins or leaves a cluster; the join
/// field, review, confirmations, and invitations update in place.
fn section_cluster(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = settings_projection(shared, "settings cluster membership", |snapshot| {
        snapshot.cluster.clustered
    });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::key_from(source, move |clustered| {
        WidgetPod::new(section_cluster_body(&s, &r, *clustered))
    })
}

fn section_cluster_body(
    shared: &Shared,
    reader: &ThemeReader,
    clustered: bool,
) -> impl Widget + use<> {
    let idle = settings_projection(shared, "settings cluster idle", |snapshot| {
        !snapshot.cluster.pending
    });
    let mut this_cluster =
        SettingsCard::new("This cluster").header_action(cluster_dashboard_button(shared, reader));
    this_cluster = if clustered {
        let summary = settings_projection(shared, "settings cluster summary", |snapshot| {
            snapshot
                .cluster
                .cluster_summary
                .clone()
                .unwrap_or_else(|| "connected".to_string())
        });
        this_cluster
            .row(
                SettingRow::new("Status")
                    .status(reader, "Connected", SemanticTone::Success)
                    .build(shared),
            )
            .row(
                SettingRow::new("Cluster")
                    .value_from(shared, summary)
                    .build(shared),
            )
    } else {
        this_cluster.row(
            SettingRow::new("Status")
                .help("This device runs its own single-node cluster.")
                .status(reader, "Running locally", SemanticTone::Neutral)
                .build(shared),
        )
    };
    this_cluster = this_cluster.row(feedback_slot(shared, reader, |snapshot| {
        snapshot.feedback.cluster.clone()
    }));

    let mut page = Column::new(reader, |_| 16.0).with_child(this_cluster.build(shared, reader));

    if clustered {
        let generate = {
            let s = shared.clone();
            sui::Button::new("Generate invitation")
                .icon(sui::IconGlyph::Link)
                .theme_when(clone_reader(reader))
                .enabled_from(idle.clone())
                .on_press(move || {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Cluster,
                        SettingsOperation::CreateClusterInvite,
                    );
                })
        };
        let invite = settings_projection(shared, "settings cluster invite", |snapshot| {
            snapshot.cluster.invite.clone()
        });
        let has_invite = derived(
            invite.clone(),
            "settings cluster has invite",
            Option::is_some,
        );
        // The generated link and command; rebuilt only when a new invitation is issued.
        let invite_rows = {
            let s = shared.clone();
            let r = Rc::clone(reader);
            RebuildOnChange::key_from(invite, move |invite| match invite.clone() {
                Some(invite) => WidgetPod::new(cluster_invite_rows(&s, &r, invite)),
                None => WidgetPod::new(SizedBox::new()),
            })
        };
        page = page.with_child(
            SettingsCard::new("Invite a device")
                .row(
                    SettingRow::new("Invitation")
                        .help("Creates a one-time link another device uses to join. It expires.")
                        .control(generate)
                        .build(shared),
                )
                .row(revealed(invite_rows, has_invite))
                .build(shared, reader),
        );
    }

    page = page.with_child(cluster_join_card(shared, reader, clustered));

    if clustered {
        let confirm = {
            let s = shared.clone();
            sui::Button::new("Confirm leave")
                .icon(sui::IconGlyph::Trash)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Danger)
                .enabled_from(idle)
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
        let leave = {
            let s = shared.clone();
            sui::Button::new("Leave cluster")
                .icon(sui::IconGlyph::Trash)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Danger)
                .on_press(move || {
                    s.settings_mut().confirm_leave_cluster();
                })
        };
        let confirming =
            settings_projection(shared, "settings cluster leave confirming", |snapshot| {
                usize::from(snapshot.cluster.leave_confirming)
            });
        let leave = SwitchView::new()
            .with_child(leave)
            .with_child(
                Flex::horizontal()
                    .gap(8.0)
                    .align_items(Alignment::Center)
                    .with_child(confirm)
                    .with_child(cancel),
            )
            .selected_from(confirming);
        page = page.with_child(
            SettingsCard::new("Leave cluster")
                .row(
                    SettingRow::new("Leave this cluster")
                        .help("This device starts a fresh single-node cluster of its own.")
                        .control(leave)
                        .build(shared),
                )
                .build(shared, reader),
        );
    }
    page
}

fn cluster_invite_rows(
    shared: &Shared,
    reader: &ThemeReader,
    (uri, join_command, expires_at, persisted_key): (String, String, String, String),
) -> impl Widget + use<> {
    Column::new(reader, |theme| theme.metrics.form_section_body_gap)
        .with_child(
            SettingRow::new("Join link")
                .help("Open it on the other device.")
                .control(copy_control(
                    reader,
                    "cluster-invite-link",
                    "Join link",
                    uri,
                ))
                .build(shared),
        )
        .with_child(
            SettingRow::new("Join command")
                .help("Or run it in a terminal there.")
                .control(copy_control(
                    reader,
                    "cluster-invite-command",
                    "Join command",
                    join_command,
                ))
                .build(shared),
        )
        .with_child(
            SettingRow::new("Expires")
                .value(shared, expires_at)
                .build(shared),
        )
        .with_child(details(
            shared,
            reader,
            "cluster-invite",
            "saved key",
            vec![widgets::Boxed::new(
                SettingRow::new("Saved key")
                    .value(shared, persisted_key)
                    .build(shared),
            )],
        ))
}

fn cluster_join_card(
    shared: &Shared,
    reader: &ThemeReader,
    clustered: bool,
) -> impl Widget + use<> {
    let join_value = shared.settings().join_invite.clone();
    let idle = settings_projection(shared, "settings cluster join idle", |snapshot| {
        !snapshot.cluster.pending
    });
    let confirming = settings_projection(shared, "settings cluster join confirming", |snapshot| {
        snapshot.cluster.join_confirming
    });
    let reviewing = derived(
        confirming.clone(),
        "settings cluster join reviewing",
        |confirming| !confirming,
    );
    let input = {
        let s = shared.clone();
        let field = TextInput::new("cluster-join-link")
            .value(join_value)
            .placeholder("sinomo://cluster/join/…")
            .theme_when(clone_reader(reader))
            .on_change(move |value| s.settings_mut().set_join_invite(value));
        let s = shared.clone();
        SyncedTextInput::new(field, move || s.settings().join_invite.clone()).observing(
            settings_projection(shared, "settings cluster join invite", |snapshot| {
                snapshot.cluster.join_invite.clone()
            }),
        )
    };
    let review = {
        let s = shared.clone();
        let can_review = settings_projection(shared, "settings cluster can review", |snapshot| {
            !snapshot.cluster.join_invite.trim().is_empty() && !snapshot.cluster.pending
        });
        sui::Button::new(if clustered {
            "Review migration"
        } else {
            "Review join"
        })
        .icon(sui::IconGlyph::ChevronRight)
        .theme_when(clone_reader(reader))
        .enabled_from(can_review)
        .on_press(move || {
            s.settings_mut().confirm_join_cluster();
        })
    };
    let card = SettingsCard::new(if clustered {
        "Join another cluster"
    } else {
        "Join a cluster"
    })
    .description(if clustered {
        "Move this device to another cluster without leaving first. You review the target and impact before anything changes."
    } else {
        "Paste an invitation from a device in the cluster. You review the target and impact before joining."
    })
    .row(
        SettingRow::new("Invitation link")
            .control(
                Flex::horizontal()
                    .gap(8.0)
                    .align_items(Alignment::Center)
                    .with_item(input, FlexItem::new().shrink(1.0))
                    .with_item(
                        Presence::new(review)
                            .transition(crate::ui::widgets::reveal_transition())
                            .shown_from(reviewing)
                            .collapse(Axis::Horizontal),
                        FlexItem::new().no_shrink(),
                    ),
            )
            .build(shared),
    );

    let preview = settings_projection(shared, "settings cluster join preview", |snapshot| {
        cluster_join_preview(&snapshot.cluster.join_invite)
            .ok()
            .map(|preview| (preview.target, preview.expires_at, preview.issuer))
    });
    let has_preview = derived(
        preview.clone(),
        "settings cluster join has preview",
        Option::is_some,
    );
    let preview_field = |name: &'static str, field: fn(&(String, String, String)) -> String| {
        derived(preview.clone(), name, move |preview| {
            preview.as_ref().map(field).unwrap_or_default()
        })
    };
    let preview_rows = Column::new(reader, |theme| theme.metrics.form_section_body_gap)
        .with_child(
            SettingRow::new("Target cluster")
                .value_from(
                    shared,
                    preview_field("settings cluster join target", |preview| preview.0.clone()),
                )
                .build(shared),
        )
        .with_child(
            SettingRow::new("Invitation expires")
                .value_from(
                    shared,
                    preview_field("settings cluster join expiry", |preview| preview.1.clone()),
                )
                .build(shared),
        )
        .with_child(
            SettingRow::new("Issued by")
                .value_from(
                    shared,
                    preview_field("settings cluster join issuer", |preview| preview.2.clone()),
                )
                .build(shared),
        );
    let impact = settings_projection(shared, "settings cluster join impact", move |snapshot| {
        if clustered {
            let source_cluster = snapshot
                .cluster
                .cluster_id
                .as_deref()
                .map(|id| short(id, 24))
                .unwrap_or_else(|| "local bootstrap state".to_string());
            format!(
                "This device will leave {source_cluster}, retain its node identity, quarantine the source cluster state, join the target, and import node-local configuration. Cluster-scoped data and membership do not move."
            )
        } else {
            "This device will retain its node identity and join the target cluster. Local node configuration is preserved; cluster-scoped data remains owned by its current cluster."
                .to_string()
        }
    });
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
        .enabled_from(idle)
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
    let review_block = Column::new(reader, |theme| theme.metrics.form_section_body_gap)
        .with_child(revealed(preview_rows, has_preview))
        .with_child(SettingRow::new("What changes").help_from(impact).build(shared))
        .with_child(
            SettingRow::new("If it fails")
                .help("Sinomo restores the source cluster automatically. After an interrupted migration, restart the daemon and run `sinomo cluster status` before retrying.")
                .build(shared),
        )
        .with_child(Align::new(
            Alignment::End,
            Alignment::Center,
            Flex::horizontal()
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(cancel)
                .with_child(confirm),
        ));
    card.row(revealed(review_block, confirming))
        .build(shared, reader)
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

const COPY_DIAGNOSTIC_SUMMARY_LABEL: &str = "Copy summary";

/// One provider account card's state. `index` addresses the account in the provider forms.
#[derive(Clone, Debug, PartialEq)]
struct ProviderCardStructure {
    index: usize,
    form: ProviderForm,
    health: ProviderHealthState,
    advanced: bool,
    usage: bool,
    status: Option<String>,
    remove_confirming: bool,
    action_error: Option<String>,
}

impl ProviderCardStructure {
    /// Model choices: built-in and discovered models, plus the configured one.
    fn model_options(&self) -> Vec<String> {
        let mut options = self.form.known_models.clone();
        options.extend(self.health.models.iter().cloned());
        if !options.is_empty()
            && !self.form.default_model.is_empty()
            && !options.contains(&self.form.default_model)
        {
            options.push(self.form.default_model.clone());
        }
        options.sort();
        options.dedup();
        options
    }
}

fn provider_cards(providers: &SettingsProviderStructure) -> Vec<ProviderCardStructure> {
    providers
        .providers
        .iter()
        .enumerate()
        .map(|(index, (form, health))| {
            let key = provider_account_key(&form.provider, &form.account);
            ProviderCardStructure {
                index,
                form: form.clone(),
                health: health.clone(),
                advanced: providers.expanded.contains(&key),
                usage: providers.usage_expanded.contains(&key),
                status: providers
                    .action_status
                    .as_ref()
                    .filter(|status| {
                        status.provider == form.provider && status.account == form.account
                    })
                    .map(|status| status.message.clone()),
                remove_confirming: providers.remove_confirming.as_ref().is_some_and(
                    |(provider, account)| provider == &form.provider && account == &form.account,
                ),
                action_error: providers.action_error.clone(),
            }
        })
        .collect()
}

/// One provider account: operational state and chat routing stay visible; quota detail,
/// credentials, tuning, and destructive actions are disclosed only when requested. The card stays
/// mounted while health checks, tests, and edits update it.
fn provider_card(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    card: Signal<ProviderCardStructure>,
) -> impl Widget + use<> {
    let form = shared.settings().provider_forms[index].clone();
    let watch = |name: &'static str, select: fn(&ProviderCardStructure) -> bool| {
        card.select_named(name, select)
    };
    let text = |name: &'static str, select: fn(&ProviderCardStructure) -> String| {
        card.select_named(name, select)
    };

    let enable_switch = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        synced_switch(
            Switch::new("Enabled")
                .checked(form.enabled)
                .theme_when(clone_reader(reader))
                .on_change(move |enabled| {
                    queue_settings_operation(
                        &s,
                        SettingsOperationScope::Providers,
                        SettingsOperation::SetProviderEnabled {
                            provider: provider.clone(),
                            account: account.clone(),
                            enabled,
                        },
                    );
                }),
            watch("settings provider enabled", |card| card.form.enabled),
        )
    };
    let health = card.select_named("settings provider health", |card| {
        provider_health_presentation(&card.health)
    });
    let health_label = derived(health.clone(), "settings provider health label", |health| {
        health.label.to_string()
    });
    let health_badge = Restyle::new(
        StatusBadge::new(health_label.get())
            .label_from(health_label)
            .theme_when(clone_reader(reader)),
        health,
        |badge, health| badge.tone(health.tone).icon(health.icon),
    );
    let header_actions = Stack::horizontal()
        .gap(8.0)
        .alignment(Alignment::Center)
        .with_child(health_badge)
        .with_child(enable_switch);

    let test_button = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        sui::Button::new("Test connection")
            .icon(IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled_from(watch("settings provider signed in", |card| {
                card.form.signed_in
            }))
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
    let checking = card.select_named("settings provider checking", |card| {
        usize::from(card.health.checking)
    });
    let test_control = SwitchView::new()
        .with_child(test_button)
        .with_child(
            Spinner::new(format!(
                "Testing {} {} connection",
                form.display_name, form.account_label
            ))
            .label("Testing connection…")
            .theme_when(clone_reader(reader)),
        )
        .selected_from(checking);
    let connection_help = text("settings provider connection help", |card| {
        format!(
            "{} · Last tested {}",
            provider_auth_summary(&card.form),
            provider_last_verified_label(&card.health).to_lowercase()
        )
    });
    let error = text("settings provider error", |card| {
        card.health.error.clone().unwrap_or_default()
    });
    let error_row = SettingRow::new("Last error")
        .control(
            settings_label(shared, error.get(), TextSize::Sm)
                .text_from(error)
                .color_when({
                    let s = shared.clone();
                    move || palette(&s).bad_text
                }),
        )
        .build(shared);

    let set_default = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        sui::Button::new("Set as chat default")
            .icon(sui::IconGlyph::Chat)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled_from(watch("settings provider ready for chat", |card| {
                card.form.enabled && card.form.signed_in
            }))
            .on_press(move || {
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Providers,
                    SettingsOperation::SetProviderChatDefault {
                        provider: provider.clone(),
                        account: account.clone(),
                    },
                );
            })
    };
    let chat_default_control = SwitchView::new()
        .with_child(set_default)
        .with_child(
            StatusBadge::new("Default for chat")
                .icon(IconGlyph::Chat)
                .tone(SemanticTone::Accent)
                .theme_when(clone_reader(reader)),
        )
        .selected_from(card.select_named("settings provider chat default", |card| {
            usize::from(card.form.is_default)
        }));

    // The model select's options have no reader form; only this row follows the option list.
    let model_row = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::key_from(
            card.select_named("settings provider model options", |card| {
                card.model_options()
            }),
            move |options| WidgetPod::new(provider_model_row(&s, &r, index, options.clone())),
        )
    };
    let discovery_error = text("settings provider model discovery", |card| {
        if card.model_options().is_empty() {
            card.health
                .model_discovery_error
                .clone()
                .unwrap_or_default()
        } else {
            String::new()
        }
    });
    let has_discovery_error = derived(
        discovery_error.clone(),
        "settings provider has model discovery error",
        |error: &String| !error.is_empty(),
    );

    let usage_toggle = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        disclosure_restyle(
            disclosure_button(reader, "View usage details", move || {
                s.settings_mut().toggle_provider_usage(&provider, &account)
            })
            .enabled_from(watch("settings provider usage has detail", |card| {
                card.health
                    .usage
                    .as_ref()
                    .is_some_and(|usage| !usage.limits.is_empty() || usage.credits.is_some())
            })),
            watch("settings provider usage open", |card| card.usage),
            "View usage details",
            "Hide usage details",
        )
    };
    let usage_summary = SettingRow::new("Usage")
        .help_from(text("settings provider usage summary", |card| {
            provider_usage_summary(&card.health)
        }))
        .control(revealed(
            usage_toggle,
            watch("settings provider has usage", |card| {
                card.health.usage.is_some()
            }),
        ))
        .build(shared);
    let usage_details = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        // Reported quotas, read-only; rebuilt when a test reports new usage.
        RebuildOnChange::key_from(
            card.select_named("settings provider usage", |card| card.health.usage.clone()),
            move |usage| match usage {
                Some(usage) => WidgetPod::new(provider_usage_row(&s, &r, usage)),
                None => WidgetPod::new(SizedBox::new()),
            },
        )
    };
    let status = text("settings provider status", |card| {
        card.status.clone().unwrap_or_default()
    });
    let has_status = derived(
        status.clone(),
        "settings provider has status",
        |status: &String| !status.is_empty(),
    );

    let detail_button = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        disclosure_toggle(
            reader,
            watch("settings provider advanced open", |card| card.advanced),
            PROVIDER_ADVANCED_SHOW_LABEL,
            PROVIDER_ADVANCED_HIDE_LABEL,
            move || s.settings_mut().toggle_provider_detail(&provider, &account),
        )
    };
    let advanced = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        let card = card.clone();
        // The advanced controls change shape with the configured model (its context limits and
        // whether a model list exists); edits and saves update them in place.
        RebuildOnChange::key_from(
            card.select_named("settings provider advanced shape", |card| {
                (
                    card.form.default_model.clone(),
                    !card.model_options().is_empty(),
                    card.form.uses_device_login,
                    card.form.supports_reasoning,
                )
            }),
            move |_| WidgetPod::new(provider_advanced_settings(&s, &r, index, card.clone())),
        )
    };

    SettingsCard::new(form.display_name.clone())
        .description_from(text("settings provider description", |card| {
            provider_card_description(&card.form)
        }))
        .header_action(header_actions)
        .row(
            SettingRow::new("Connection")
                .help_from(connection_help)
                .control(test_control)
                .build(shared),
        )
        .row(revealed(
            error_row,
            watch("settings provider has error", |card| {
                card.health.error.is_some()
            }),
        ))
        .row(
            SettingRow::new("Chat routing")
                .help("New chats use the default account unless you pick another.")
                .control(chat_default_control)
                .build(shared),
        )
        .row(model_row)
        .row(revealed(
            SettingRow::new("Model discovery")
                .help_from(discovery_error)
                .build(shared),
            has_discovery_error,
        ))
        .row(usage_summary)
        .row(revealed(
            usage_details,
            watch("settings provider usage shown", |card| {
                card.usage && card.health.usage.is_some()
            }),
        ))
        .row(revealed(
            SettingRow::new("Status")
                .value_from(shared, status)
                .build(shared),
            has_status,
        ))
        .row(Align::new(
            Alignment::Start,
            Alignment::Center,
            detail_button,
        ))
        .row(revealed(
            advanced,
            watch("settings provider advanced shown", |card| card.advanced),
        ))
        .build(shared, reader)
}

/// The default model: a select over the known and discovered models, or a text field.
fn provider_model_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    options: Vec<String>,
) -> widgets::Boxed {
    if options.is_empty() {
        return widgets::Boxed::new(provider_model_text_row(shared, reader, index, "Model"));
    }
    let form = shared.settings().provider_forms[index].clone();
    let model_select = {
        let s = shared.clone();
        let selected = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        let choices = options.clone();
        Select::new(format!("provider-{index}-model-select"))
            .options(options)
            .selected_when(move || {
                let current = selected
                    .settings()
                    .provider_forms
                    .get(index)
                    .map(|form| form.default_model.clone())
                    .unwrap_or_default();
                choices
                    .iter()
                    .position(|model| model == &current)
                    .or((!choices.is_empty()).then_some(0))
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
    widgets::Boxed::new(
        SettingRow::new("Model")
            .help("Used when a chat or agent does not choose one.")
            .control(model_select)
            .build(shared),
    )
}

/// Credentials, endpoint, context, response defaults, and account removal.
fn provider_advanced_settings(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    card: Signal<ProviderCardStructure>,
) -> impl Widget + use<> {
    let form = shared.settings().provider_forms[index].clone();
    let has_model_options = !card.get().model_options().is_empty();
    let mut body = FieldGroup::new().gap(10.0).fill_width(true);
    body = body.with_child(
        SettingRow::new("Capabilities")
            .value(shared, provider_capabilities_summary(&form.provider))
            .build(shared),
    );
    body = body.with_child(provider_subsection_heading(shared, reader, "Credentials"));
    if form.uses_device_login {
        let button = {
            let s = shared.clone();
            let label = shared.clone();
            sui::Button::new(if form.signed_in {
                "Re-authenticate"
            } else {
                "Sign in"
            })
            .label_when(move || {
                let signed_in = label
                    .settings()
                    .provider_forms
                    .get(index)
                    .is_some_and(|form| form.signed_in);
                if signed_in {
                    "Re-authenticate"
                } else {
                    "Sign in"
                }
                .to_string()
            })
            .theme_when(clone_reader(reader))
            .on_press(move || {
                let waker = s.borrow().waker.clone();
                s.settings_mut().begin_device_login(index, waker);
            })
        };
        let auth = Stack::vertical()
            .gap(6.0)
            .alignment(Alignment::Stretch)
            .with_child(status_reflector(shared, index, reader))
            .with_child(Stack::horizontal().with_child(button));
        body = body.with_child(
            FormRow::new("Sign-in", auth)
                .stacked()
                .theme_when(clone_reader(reader)),
        );
    } else if provider_requires_api_key(&form.provider) {
        let api_key = provider_field_key(&form.provider, &form.account, ProviderField::ApiKey);
        let api_value = shared.settings().field_draft_or(&api_key, String::new);
        let on_change = {
            let s = shared.clone();
            move |value: String| {
                s.settings_mut()
                    .set_provider_field_draft(index, ProviderField::ApiKey, value);
            }
        };
        let has_key = card.select_named("settings provider has API key", |card| {
            card.form.has_api_key
        });
        body = body.with_child(password_save_inline_row(
            shared,
            reader,
            &format!("provider-{index}-api-key"),
            "API key",
            api_key,
            api_value,
            has_key,
            on_change,
            {
                let s = shared.clone();
                move || {
                    let operation = s
                        .settings()
                        .provider_field_operation(index, ProviderField::ApiKey);
                    if let Some(operation) = operation {
                        queue_settings_operation(&s, SettingsOperationScope::Providers, operation);
                    }
                }
            },
        ));
    } else {
        body = body.with_child(value_row(
            shared,
            reader,
            "Authentication",
            "No credentials required; the app connects to the configured local service.",
        ));
    }

    body = body.with_child(provider_subsection_heading(shared, reader, "Endpoint"));
    if has_model_options {
        body = body.with_child(provider_model_text_row(
            shared,
            reader,
            index,
            "Model ID override",
        ));
    }

    let base_url_key = provider_field_key(&form.provider, &form.account, ProviderField::BaseUrl);
    let base_url_value = shared
        .settings()
        .field_draft_or(&base_url_key, || form.api_base_url.clone());
    body = body.with_child(text_save_inline_row(
        shared,
        reader,
        &format!("provider-{index}-base-url"),
        "API base URL",
        base_url_key,
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
    ));

    if provider_supports_context_controls(&form.provider) {
        body = body.with_child(provider_subsection_heading(
            shared,
            reader,
            "Context and cost",
        ));
        let configured_model = if form.default_model.trim().is_empty() {
            sinomo_runtime::api::lm::PROVIDER_DEFAULT_MODEL
        } else {
            form.default_model.as_str()
        };
        let automatic =
            sinomo_lm::inferred_model_context_window_tokens(&form.provider, configured_model);
        let supported =
            sinomo_lm::inferred_model_max_context_window_tokens(&form.provider, configured_model);
        let effective_model =
            sinomo_runtime::api::lm::effective_model_name(&form.provider, configured_model);
        let context_hint = match (automatic, supported, effective_model.contains("gpt-5.6")) {
            (Some(automatic), Some(supported), true) => format!(
                "Automatic cost-aware limit: {automatic} tokens. Known model window: {supported} tokens. OpenAI long-context requests above {} input tokens use higher rates and may consume subscription allowance faster.",
                sinomo_lm::OPENAI_LONG_CONTEXT_PRICE_THRESHOLD_TOKENS
            ),
            (Some(automatic), Some(supported), false) => format!(
                "Automatic inferred limit: {automatic} tokens. Known model window: {supported} tokens. Higher context limits can increase API cost or subscription usage."
            ),
            _ => "Leave blank to use Sinomo's inferred model limit. Higher context limits can increase API cost or subscription usage.".to_string(),
        };
        body = body.with_child(
            SettingRow::new("Context policy")
                .help(context_hint)
                .build(shared),
        );

        let max_key = provider_field_key(
            &form.provider,
            &form.account,
            ProviderField::MaxContextTokens,
        );
        let max_value = shared.settings().field_draft_or(&max_key, || {
            form.max_context_tokens
                .map(|value| value.to_string())
                .unwrap_or_default()
        });
        let max_placeholder = automatic
            .map(|value| format!("Automatic ({value})"))
            .unwrap_or_else(|| "Automatic".to_string());
        body = body.with_child(text_save_inline_row(
            shared,
            reader,
            &format!("provider-{index}-max-context-tokens"),
            "Max context tokens",
            max_key,
            max_value,
            &max_placeholder,
            {
                let s = shared.clone();
                move |value| {
                    s.settings_mut().set_provider_field_draft(
                        index,
                        ProviderField::MaxContextTokens,
                        value,
                    );
                }
            },
            {
                let s = shared.clone();
                move || {
                    if let Some(operation) = s
                        .settings()
                        .provider_field_operation(index, ProviderField::MaxContextTokens)
                    {
                        queue_settings_operation(&s, SettingsOperationScope::Providers, operation);
                    }
                }
            },
        ));

        let compact_key = provider_field_key(
            &form.provider,
            &form.account,
            ProviderField::AutoCompactTokenLimit,
        );
        let compact_value = shared.settings().field_draft_or(&compact_key, || {
            form.auto_compact_token_limit
                .map(|value| value.to_string())
                .unwrap_or_default()
        });
        body = body.with_child(text_save_inline_row(
            shared,
            reader,
            &format!("provider-{index}-auto-compact-token-limit"),
            "Compact history after",
            compact_key,
            compact_value,
            "Automatic (80% of active limit)",
            {
                let s = shared.clone();
                move |value| {
                    s.settings_mut().set_provider_field_draft(
                        index,
                        ProviderField::AutoCompactTokenLimit,
                        value,
                    );
                }
            },
            {
                let s = shared.clone();
                move || {
                    if let Some(operation) = s
                        .settings()
                        .provider_field_operation(index, ProviderField::AutoCompactTokenLimit)
                    {
                        queue_settings_operation(&s, SettingsOperationScope::Providers, operation);
                    }
                }
            },
        ));
    }

    let has_response_defaults = form.supports_reasoning
        || provider_supports_reasoning_budget(&form.provider)
        || provider_supports_text_verbosity(&form.provider)
        || provider_supports_vendor_options(&form.provider);
    if has_response_defaults {
        body = body.with_child(provider_subsection_heading(
            shared,
            reader,
            "Response defaults",
        ));
    }

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
        body = body.with_child(text_save_inline_row(
            shared,
            reader,
            &format!("provider-{index}-reasoning-budget"),
            "Reasoning budget",
            budget_key,
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
                        queue_settings_operation(&s, SettingsOperationScope::Providers, operation);
                    }
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
        body = body.with_child(text_save_inline_row(
            shared,
            reader,
            &format!("provider-{index}-vendor-options"),
            "Provider options",
            options_key,
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
                        queue_settings_operation(&s, SettingsOperationScope::Providers, operation);
                    }
                }
            },
        ));
    }
    body = body.with_child(provider_subsection_heading(shared, reader, "Account"));
    body.with_child(provider_management_row(shared, reader, index, card))
}

const PROVIDER_ADVANCED_SHOW_LABEL: &str = "Advanced settings";
const PROVIDER_ADVANCED_HIDE_LABEL: &str = "Hide advanced settings";

fn provider_card_description(form: &ProviderForm) -> String {
    let model = if form.default_model.trim().is_empty() {
        "Provider default model"
    } else {
        form.default_model.as_str()
    };
    format!("{} account - {model}", form.account_label)
}

fn provider_usage_summary(health: &ProviderHealthState) -> String {
    let Some(usage) = health.usage.as_ref() else {
        return health
            .usage_error
            .clone()
            .unwrap_or_else(|| "Test the connection to load usage and limits".to_string());
    };
    let mut parts = Vec::new();
    if let Some(plan) = usage.plan.as_deref() {
        parts.push(plan.to_string());
    }
    let highest = usage
        .limits
        .iter()
        .filter_map(provider_usage_used_percent)
        .fold(None::<f64>, |current, used| {
            Some(current.map_or(used, |value| value.max(used)))
        });
    match (usage.limits.len(), highest) {
        (0, _) => parts.push("No limits reported".to_string()),
        (1, Some(used)) => parts.push(format!("{used:.0}% used")),
        (count, Some(used)) => parts.push(format!("{count} limits - highest {used:.0}% used")),
        (count, None) => parts.push(format!("{count} limits")),
    }
    if let Some(credits) = usage.credits.as_ref()
        && let Some(balance) = credits.balance.as_deref()
    {
        parts.push(format!("Credits {balance}"));
    }
    parts.join(" - ")
}

fn provider_subsection_heading(shared: &Shared, reader: &ThemeReader, title: &str) -> Stack {
    let style = shared.clone();
    Stack::vertical()
        .gap(8.0)
        .alignment(Alignment::Stretch)
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)))
        .with_child(
            Label::new(title.to_ascii_uppercase())
                .text_style_when(move || {
                    let mut style = settings_text_style(&style, TextSize::Xs);
                    style.weight = FontWeight::SEMIBOLD;
                    style
                })
                .color_when({
                    let s = shared.clone();
                    move || palette(&s).text_faint
                }),
        )
}

fn provider_save_button<F>(
    shared: &Shared,
    reader: &ThemeReader,
    label: &str,
    draft_key: String,
    on_save: F,
) -> Tooltip
where
    F: FnMut() + 'static,
{
    let enabled_shared = shared.clone();
    let enabled_key = draft_key;
    let button = IconButton::new(IconGlyph::Check, format!("Save {label}"))
        .description(format!("Save the edited {label}"))
        .theme_when(clone_reader(reader))
        .enabled_when(move || {
            let settings = enabled_shared.settings();
            settings.field_drafts.contains_key(&enabled_key)
                && !settings.domain_pending(SettingsOperationDomain::Providers)
        })
        .on_press(on_save);
    Tooltip::new(format!("Save {label}"), button)
        .placement(TooltipPlacement::Above)
        .theme_when(clone_reader(reader))
}

fn text_save_inline_row<C, S>(
    shared: &Shared,
    reader: &ThemeReader,
    name: &str,
    label: &str,
    draft_key: String,
    value: impl Into<String>,
    placeholder: &str,
    on_change: C,
    on_save: S,
) -> FormRow
where
    C: FnMut(String) + 'static,
    S: FnMut() + 'static,
{
    let input = TextInput::new(name)
        .value(value)
        .placeholder(placeholder)
        .theme_when(clone_reader(reader))
        .on_change(on_change);
    let save = provider_save_button(shared, reader, label, draft_key, on_save);
    inline_row(label, FieldWithAction::new(input, save).gap(6.0)).theme_when(clone_reader(reader))
}

fn password_save_inline_row<C, S>(
    shared: &Shared,
    reader: &ThemeReader,
    name: &str,
    label: &str,
    draft_key: String,
    value: impl Into<String>,
    stored: impl Observable<bool> + 'static,
    on_change: C,
    on_save: S,
) -> FormRow
where
    C: FnMut(String) + 'static,
    S: FnMut() + 'static,
{
    // The placeholder says whether a key is stored; a save updates it without replacing the field.
    let input = Restyle::new(
        PasswordInput::new(name)
            .value(value)
            .theme_when(clone_reader(reader))
            .on_change(on_change),
        stored,
        |input, stored| input.placeholder(if *stored { "Configured" } else { "Not set" }),
    );
    let save = provider_save_button(shared, reader, label, draft_key, on_save);
    inline_row(label, FieldWithAction::new(input, save).gap(6.0)).theme_when(clone_reader(reader))
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
    text_save_inline_row(
        shared,
        reader,
        &format!("provider-{index}-model"),
        label,
        model_key,
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
    )
}

fn provider_auth_summary(form: &ProviderForm) -> &'static str {
    if form.uses_device_login {
        if form.signed_in {
            "Credential stored"
        } else {
            "Sign-in required"
        }
    } else if !provider_requires_api_key(&form.provider) {
        "No credentials required"
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

fn provider_supports_context_controls(provider: &str) -> bool {
    matches!(provider, "openai" | "codex")
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
    usage: &sinomo_runtime::api::lm::LanguageModelProviderUsage,
) -> FormRow {
    let mut stack = Stack::vertical().gap(8.0).alignment(Alignment::Stretch);
    let mut metadata = Stack::horizontal().gap(8.0).alignment(Alignment::Center);
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
    let action = shared.clone();
    let provider = usage.provider.clone();
    stack = stack.with_child(
        sui::Button::new("Usage history")
            .theme_when(clone_reader(reader))
            .on_press(move || {
                crate::ui::cluster::open_usage(
                    &action,
                    sinomo_client::usage_contract::UsageFilter {
                        provider: Some(provider.clone()),
                        ..Default::default()
                    },
                );
            }),
    );
    FormRow::new("Usage and limits", stack)
        .stacked()
        .theme_when(clone_reader(reader))
}

fn provider_usage_limit_view(
    shared: &Shared,
    reader: &ThemeReader,
    limit: &sinomo_runtime::api::lm::LanguageModelUsageLimit,
) -> Stack {
    let name = limit.name.as_deref().unwrap_or(&limit.id);
    if limit.unlimited == Some(true) {
        return Stack::horizontal()
            .gap(8.0)
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
            .gap(3.0)
            .alignment(Alignment::Stretch)
            .with_child(primary_label(shared, name.to_string()))
            .with_child(muted_label(
                shared,
                provider_usage_limit_detail(limit, current_settings_unix_timestamp()),
            ));
    };
    let tone = provider_usage_tone(limit, used_percent);
    Stack::vertical()
        .gap(4.0)
        .alignment(Alignment::Stretch)
        .with_child(primary_label(
            shared,
            format!("{name} · {used_percent:.0}% used"),
        ))
        .with_child(
            SizedBox::new().height(8.0).child(
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

fn provider_usage_used_percent(
    limit: &sinomo_runtime::api::lm::LanguageModelUsageLimit,
) -> Option<f64> {
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
    limit: &sinomo_runtime::api::lm::LanguageModelUsageLimit,
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

fn provider_usage_limit_detail(
    limit: &sinomo_runtime::api::lm::LanguageModelUsageLimit,
    now: i64,
) -> String {
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
    card: Signal<ProviderCardStructure>,
) -> FormRow {
    let form = shared.settings().provider_forms[index].clone();
    let cancel = {
        let s = shared.clone();
        sui::Button::new("Cancel")
            .icon(sui::IconGlyph::Close)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || s.settings_mut().cancel_provider_remove())
    };
    let confirm = {
        let s = shared.clone();
        let provider = form.provider.clone();
        let account = form.account.clone();
        sui::Button::new("Remove account")
            .icon(sui::IconGlyph::Trash)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Filled)
            .tone(SemanticTone::Danger)
            .enabled_from(settings_projection(
                shared,
                "settings provider remove enabled",
                |snapshot| !snapshot.providers.pending,
            ))
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
    let error = card.select_named("settings provider remove error", |card| {
        card.action_error.clone().unwrap_or_default()
    });
    let has_error = derived(
        error.clone(),
        "settings provider has remove error",
        |error: &String| !error.is_empty(),
    );
    let confirmation = Column::new(reader, |theme| theme.metrics.property_row_stacked_gap)
        .with_child(muted_label(
            shared,
            "This removes the provider account from lm.providers.",
        ))
        .with_child(
            Stack::horizontal()
                .gap(8.0)
                .alignment(Alignment::Start)
                .with_child(cancel)
                .with_child(confirm),
        )
        .with_child(revealed(
            settings_label(shared, error.get(), TextSize::Sm)
                .text_from(error)
                .color_when({
                    let s = shared.clone();
                    move || palette(&s).bad_text
                }),
            has_error,
        ));
    let remove = {
        let s = shared.clone();
        sui::Button::new("Remove account")
            .icon(sui::IconGlyph::Trash)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Filled)
            .tone(SemanticTone::Danger)
            .on_press(move || s.settings_mut().confirm_provider_remove(index))
    };
    let confirming = card.select_named("settings provider remove confirming", |card| {
        usize::from(card.remove_confirming)
    });
    FormRow::new(
        "Remove",
        SwitchView::new()
            .with_child(Align::new(Alignment::End, Alignment::Center, remove))
            .with_child(confirmation)
            .selected_from(confirming),
    )
    .stacked()
    .theme_when(clone_reader(reader))
}

/// The account list and the add form stay mounted side by side; opening or closing the form
/// only switches between them, so neither loses its scroll, fields, or focus.
fn section_providers(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let add_open = settings_projection(shared, "settings providers add open", |snapshot| {
        usize::from(snapshot.providers.add_open)
    });
    SwitchView::new()
        .with_child(provider_list_view(shared, reader))
        .with_child(provider_add_view(shared, reader))
        .selected_from(add_open)
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
        format!("usage-expanded:{:?}", settings.provider_usage_expanded),
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
    let add = |primary: bool| {
        let s = shared.clone();
        let button = sui::Button::new(ADD_PROVIDER_LABEL)
            .icon(sui::IconGlyph::Add)
            .theme_when(clone_reader(reader))
            .on_press(move || {
                s.settings_mut().open_provider_add();
            });
        if primary {
            button.primary_action()
        } else {
            button
        }
    };
    let error = settings_projection(shared, "settings providers error", |snapshot| {
        snapshot.providers.action_error.clone().unwrap_or_default()
    });
    let has_error = derived(
        error.clone(),
        "settings providers has error",
        |error: &String| !error.is_empty(),
    );
    let error_row = |error: SettingsProjection<String>| {
        SettingRow::new("Provider error")
            .control(
                settings_label(shared, error.get(), TextSize::Sm)
                    .text_from(error)
                    .color_when({
                        let s = shared.clone();
                        move || palette(&s).bad_text
                    }),
            )
            .build(shared)
    };

    let empty = SettingsCard::new("No provider accounts yet")
        .description(
            "Add an account to start chatting. Providers sign in with an API key or a device login.",
        )
        .row(Align::new(Alignment::Start, Alignment::Center, add(true)))
        .row(revealed(error_row(error.clone()), has_error.clone()))
        .row(details(
            shared,
            reader,
            "providers-storage",
            "where accounts are stored",
            vec![widgets::Boxed::new(
                SettingRow::new("Stored in")
                    .help("The provider selector in chat reads the same setting.")
                    .value(shared, "lm.providers")
                    .build(shared),
            )],
        ))
        .build(shared, reader);

    let forms = |name: &'static str, summary: fn(&[ProviderForm]) -> String| {
        settings_projection(shared, name, move |snapshot| {
            let forms = snapshot
                .providers
                .providers
                .iter()
                .map(|(form, _)| form.clone())
                .collect::<Vec<_>>();
            summary(&forms)
        })
    };
    let overview = SettingsCard::new("Overview")
        .header_action(add(false))
        .row(
            SettingRow::new("Accounts")
                .value_from(
                    shared,
                    forms("settings providers accounts", provider_accounts_summary),
                )
                .build(shared),
        )
        .row(
            SettingRow::new("Ready for chat")
                .value_from(
                    shared,
                    forms("settings providers ready", provider_ready_summary),
                )
                .build(shared),
        )
        .row(
            SettingRow::new("Chat default")
                .value_from(
                    shared,
                    forms("settings providers default", provider_chat_default_summary),
                )
                .build(shared),
        )
        .row(revealed(error_row(error), has_error))
        .build(shared, reader);
    // Accounts are keyed by position and identity, so a health check, test, or edit updates the
    // matching card in place.
    let cards = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        KeyedStack::vertical(
            settings_projection(shared, "settings provider cards", |snapshot| {
                provider_cards(&snapshot.providers)
            }),
            |card: &ProviderCardStructure| {
                (
                    card.index,
                    card.form.provider.clone(),
                    card.form.account.clone(),
                )
            },
            move |(index, _, _), card| provider_card(&s, &r, *index, card),
        )
        .gap(16.0)
    };
    let list = Stack::vertical()
        .gap(16.0)
        .alignment(Alignment::Stretch)
        .with_child(overview)
        .with_child(cards);
    SwitchView::new()
        .with_child(empty)
        .with_child(list)
        .selected_from(settings_projection(
            shared,
            "settings providers present",
            |snapshot| usize::from(!snapshot.providers.providers.is_empty()),
        ))
}

const ADD_PROVIDER_LABEL: &str = "Add provider";

fn provider_add_view(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
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
    // Opening the form or picking another provider resets some fields in the model; the retained
    // fields adopt those values without being rebuilt.
    let resets = settings_projection(shared, "settings provider add resets", |snapshot| {
        (
            snapshot.providers.add_open,
            snapshot.providers.add_provider_index,
        )
    });
    let field = |name: &str,
                 label: &str,
                 placeholder: &str,
                 read: fn(&SettingsState) -> String,
                 write: fn(&mut SettingsState, String)| {
        let s = shared.clone();
        let input = TextInput::new(name)
            .value(read(&shared.settings()))
            .placeholder(placeholder)
            .theme_when(clone_reader(reader))
            .on_change(move |value| write(&mut s.settings_mut(), value));
        let s = shared.clone();
        inline_row(
            label,
            SyncedTextInput::new(input, move || read(&s.settings())).observing(resets.clone()),
        )
        .theme_when(clone_reader(reader))
    };
    let api_key = {
        let s = shared.clone();
        let current = shared.clone();
        let input = PasswordInput::new("provider-add-api-key")
            .value(shared.settings().provider_add_api_key.clone())
            .placeholder("Paste API key")
            .theme_when(clone_reader(reader))
            .on_change(move |value| s.settings_mut().set_provider_add_api_key(value));
        let input = Restyle::new(input, resets.clone(), move |mut input, _| {
            let value = current.settings().provider_add_api_key.clone();
            if input.current_value() != value {
                input.set_value(value);
            }
            input
        });
        inline_row("API key", input).theme_when(clone_reader(reader))
    };
    let auth = SwitchView::new()
        .with_child(value_row(
            shared,
            reader,
            "Authentication",
            "Device login after add",
        ))
        .with_child(
            Column::new(reader, |_| 8.0)
                .with_child(api_key)
                .with_child(value_row(
                    shared,
                    reader,
                    "Requirement",
                    "API key required before adding.",
                )),
        )
        .with_child(value_row(
            shared,
            reader,
            "Authentication",
            "No credentials required; make sure the local service is running.",
        ))
        .selected_from(settings_projection(
            shared,
            "settings provider add authentication",
            |snapshot| snapshot.providers.add_auth.index(),
        ));

    let body = FieldGroup::new()
        .fill_width(true)
        .gap(8.0)
        .with_child(inline_row("Provider", provider_select).theme_when(clone_reader(reader)))
        .with_child(field(
            "provider-add-account",
            "Account",
            "default",
            |settings| settings.provider_add_account.clone(),
            |settings, value| settings.set_provider_add_account(value),
        ))
        .with_child(auth)
        .with_child(field(
            "provider-add-model",
            "Default model",
            "Provider default",
            |settings| settings.provider_add_model.clone(),
            |settings, value| settings.set_provider_add_model(value),
        ))
        .with_child(field(
            "provider-add-base-url",
            "API base URL",
            "Provider default",
            |settings| settings.provider_add_base_url.clone(),
            |settings, value| settings.set_provider_add_base_url(value),
        ))
        .with_child(feedback_slot(shared, reader, |snapshot| {
            snapshot.feedback.provider_add.clone()
        }));

    let cancel = {
        let s = shared.clone();
        sui::Button::new("Cancel")
            .icon(sui::IconGlyph::ChevronLeft)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || s.settings_mut().close_provider_add())
    };
    let add = {
        let s = shared.clone();
        let label = shared.clone();
        sui::Button::new(ADD_PROVIDER_LABEL)
            .label_when(move || {
                if label.settings().provider_add_uses_device_login() {
                    "Add and sign in"
                } else {
                    ADD_PROVIDER_LABEL
                }
                .to_string()
            })
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
    let add = follow(
        add,
        settings_projection(shared, "settings provider add state", |snapshot| {
            (snapshot.providers.add_auth, snapshot.providers.pending)
        }),
    );
    let actions = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Center)
        .with_child(cancel)
        .with_child(add);
    let body = body.with_child(inline_row("Actions", actions).theme_when(clone_reader(reader)));

    let title = settings_projection(shared, "settings provider add title", |snapshot| {
        CONFIGURABLE_PROVIDERS
            .get(snapshot.providers.add_provider_index)
            .map(|provider| format!("Add {}", provider_display_name(provider)))
            .unwrap_or_else(|| ADD_PROVIDER_LABEL.to_string())
    });
    SettingsCard::new(title.get())
        .title_from(title)
        .description(
            "Add an account, then adjust low-level defaults from the provider card when needed.",
        )
        .row(body)
        .build(shared, reader)
}

fn section_agents(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    section_agents_body(shared, reader)
}

/// The agent detail pane. It rebuilds only when another agent (or a new draft) is opened or the
/// opened agent's kind of view changes; edits, tool and server lists, delete confirmation, and
/// validation results update in place.
fn agent_details_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> RebuildOnChange<(Option<String>, Option<String>, SettingsAgentDetail)> {
    let source = settings_projection(shared, "settings agent detail", |snapshot| {
        (
            snapshot.agents.selected_id.clone(),
            snapshot.agents.editor_previous_id.clone(),
            snapshot.agents.detail.clone(),
        )
    });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::key_from(source, move |(_, _, detail)| {
        WidgetPod::new(agent_detail_body(&s, &r, detail.clone()))
    })
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
        "selected:{:?};detail:{};delete:{:?};report:{:?};editor:{:?};tools:{:?};mcp:{:?};external:{}\n{agents}",
        settings.agent_selected_id,
        settings.agent_detail_open,
        settings.agent_delete_confirming,
        settings.agent_validation_report,
        settings.agent_form.previous_id,
        settings.agent_form.definition.tools,
        settings
            .agent_form
            .mcp_servers
            .iter()
            .map(|server| (server.ui_id, server.enabled, server.required))
            .collect::<Vec<_>>(),
        settings.remote_management.external_integrations_enabled,
    )
}

fn section_agents_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let wide = FixedPaneSplit::horizontal(
        agent_catalog_section(shared, reader),
        Separator::vertical().theme_when(clone_reader(reader)),
        WidgetPadding::horizontal(18.0, agent_detail_panel(shared, reader)).fill_child(),
    )
    .fixed_first(AGENT_MENU_W)
    .divider_extent(1.0)
    .fallback_flexible_extent(560.0);

    let back = {
        let shared = shared.clone();
        sui::Button::new("Agents")
            .icon(IconGlyph::ChevronLeft)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || shared.settings_mut().back_to_agent_list())
    };
    let detail = Stack::vertical()
        .gap(10.0)
        .alignment(Alignment::Stretch)
        .with_child(Stack::horizontal().with_child(back))
        .with_child(agent_detail_panel(shared, reader));
    let open = settings_projection(shared, "settings agent detail open", |snapshot| {
        usize::from(snapshot.agents.detail_open)
    });
    let narrow = SwitchView::new()
        .with_child(agent_catalog_section(shared, reader))
        .with_child(detail)
        .selected_from(open);

    ConstraintView::new(narrow).when(
        ConstraintQuery::new().min_width(AGENT_SPLIT_THRESHOLD),
        wide,
    )
}

fn agent_catalog_section(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let collection = shared.signals().settings_structure.select_named(
        "settings virtual agent collection",
        |key| {
            let Some(snapshot) = key.0.as_deref() else {
                return widgets::VirtualCollectionSnapshot::new(Vec::new(), None);
            };
            widgets::VirtualCollectionSnapshot::new(
                snapshot
                    .agents
                    .agents
                    .iter()
                    .map(|agent| (agent.id.clone(), agent.clone()))
                    .collect(),
                snapshot.agents.selected_id.clone(),
            )
        },
    );
    let row_shared = shared.clone();
    let row_reader = Rc::clone(reader);
    let action_shared = shared.clone();
    let list = widgets::VirtualCollectionHost::new(
        "Settings agent collection",
        collection,
        move |model, state| {
            let row_reader = Rc::clone(&row_reader);
            VirtualList::new("agent-config-list", model, move |_id, agent| {
                let shared = row_shared.clone();
                let reader = Rc::clone(&row_reader);
                // One row's summary text; the list and its scroll position stay mounted.
                RebuildOnChange::key_from(agent, move |agent| {
                    WidgetPod::new(padded_virtual_list_row(
                        &reader,
                        agent_catalog_row(&shared, agent),
                    ))
                })
            })
            .state(state)
            .estimated_row_height(MENU_ROW_H)
            .theme_when(clone_reader(reader))
            .row_name(|_id, agent| {
                if agent.name.trim().is_empty() {
                    agent.id.clone()
                } else {
                    agent.name.clone()
                }
            })
            .row_description(|_id, agent| agent_summary_line(agent))
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
    let refresh = {
        let s = shared.clone();
        let button = IconButton::new(IconGlyph::Redo, "Refresh")
            .description("Reload the agent catalog")
            .theme_when(clone_reader(reader))
            .on_press(move || {
                let selected_id = s.settings().agent_selected_id.clone();
                queue_settings_operation(
                    &s,
                    SettingsOperationScope::Agents,
                    SettingsOperation::RefreshAgents { selected_id },
                );
            });
        Tooltip::new("Refresh agents", button)
            .placement(TooltipPlacement::Above)
            .theme_when(clone_reader(reader))
    };

    SettingsCard::new("Agents")
        .header_action(
            Flex::horizontal()
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(new_agent)
                .with_child(refresh),
        )
        .row(agent_catalog_summary(shared))
        .row(SizedBox::new().height(AGENT_LIST_ROW_H * 5.0).child(list))
        .build(shared, reader)
}

/// The catalog counts and any catalog error, updated in place.
fn agent_catalog_summary(shared: &Shared) -> impl Widget + use<> {
    let summary = settings_projection(shared, "settings agent catalog summary", |snapshot| {
        let catalog = AgentConfigSnapshot {
            agents: snapshot.agents.agents.clone(),
            selected_agent: None,
            available_tools: Vec::new(),
            error: snapshot.agents.error.clone(),
        };
        format!(
            "{} · {} custom",
            agent_sidebar_preview(&catalog),
            agent_custom_count(&catalog)
        )
    });
    let error = settings_projection(shared, "settings agent catalog error", |snapshot| {
        snapshot
            .agents
            .error
            .as_ref()
            .map(|error| format!("Catalog error: {error}"))
            .unwrap_or_default()
    });
    let has_error = derived(error.clone(), "settings agent catalog has error", |error| {
        !error.is_empty()
    });
    let error_label = settings_label(shared, error.get(), TextSize::Sm)
        .text_from(error)
        .color_when({
            let s = shared.clone();
            move || palette(&s).bad_text
        });
    Column::new(&shared_reader(shared), |theme| {
        theme.metrics.property_row_stacked_gap
    })
    .with_child(muted_text(
        shared,
        Label::new(summary.get()).text_from(summary),
        TextSize::Sm,
    ))
    .with_child(revealed(error_label, has_error))
}

/// Feedback, the selected agent's view, package tools, and the latest validation report.
fn agent_detail_panel(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let report = settings_projection(shared, "settings agent validation report", |snapshot| {
        snapshot.agents.validation_report.clone()
    });
    let has_report = derived(report.clone(), "settings agent has report", Option::is_some);
    let report_card = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        // Read-only results; rebuilt only when a new report arrives.
        RebuildOnChange::key_from(report, move |report| match report.clone() {
            Some(report) => WidgetPod::new(agent_validation_report_section(&s, &r, report)),
            None => WidgetPod::new(SizedBox::new()),
        })
    };
    Column::new(reader, |_| 14.0)
        .with_child(feedback_slot(shared, reader, |snapshot| {
            snapshot.feedback.agent.clone()
        }))
        .with_child(agent_details_host(shared, reader))
        .with_child(agent_package_management_section(shared, reader))
        .with_child(revealed(report_card, has_report))
}

fn agent_detail_body(
    shared: &Shared,
    reader: &ThemeReader,
    detail: SettingsAgentDetail,
) -> widgets::Boxed {
    match detail {
        SettingsAgentDetail::Editor => widgets::Boxed::new(agent_editor_section(shared, reader)),
        SettingsAgentDetail::ReadOnly(definition) => {
            widgets::Boxed::new(agent_readonly_section(shared, reader, *definition))
        }
        SettingsAgentDetail::Empty => widgets::Boxed::new(
            SettingsCard::new("Select an agent")
                .description("Choose an agent from the list, or create a custom agent.")
                .build(shared, reader),
        ),
    }
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

    let path_with_action = |input: TextInput, button: sui::Button| {
        Flex::horizontal()
            .gap(8.0)
            .align_items(Alignment::Center)
            .with_item(input, FlexItem::new().shrink(1.0))
            .with_item(button, FlexItem::new().no_shrink())
    };
    SettingsCard::new("Package management")
        .description("Move agents between Sinomo installs as JSON packages.")
        .row(
            SettingRow::new("Import")
                .help("Adds a custom agent from a package file.")
                .control(path_with_action(import_input, import_button))
                .build(shared),
        )
        .row(
            SettingRow::new("Export")
                .help("Writes the selected agent to a file or folder.")
                .control(path_with_action(export_input, export_button))
                .build(shared),
        )
        .row(
            SettingRow::new("Checks")
                .help("Validate checks the package structure. Test also checks its dependencies, without a model request.")
                .control(
                    Flex::horizontal()
                        .gap(8.0)
                        .align_items(Alignment::Center)
                        .with_child(validate)
                        .with_child(test),
                )
                .build(shared),
        )
        .build(shared, reader)
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

    let profile = FieldGroup::new()
        .fill_width(true)
        .gap(8.0)
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
        .with_child(inline_row("Role", role_select).theme_when(clone_reader(reader)))
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
            220.0,
            |settings, value| settings.set_agent_system_prompt(value),
        ))
        .with_child(agent_actions_row(shared, reader));

    let model = FieldGroup::new()
        .fill_width(true)
        .gap(8.0)
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-provider",
            "Default provider",
            form.provider.clone(),
            "Cluster default",
            |settings, value| settings.set_agent_provider(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-model",
            "Default model",
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
            "Model default",
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
        .with_child(inline_row("Output", output_format_select).theme_when(clone_reader(reader)));

    let tool_rows = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        KeyedStack::vertical(
            settings_projection(shared, "settings agent editor tools", |snapshot| {
                snapshot.agents.editor_tools.clone()
            }),
            |tool: &AgentToolSpec| tool.name.clone(),
            move |_name, tool| agent_tool_editor_row(&s, &r, tool.get()),
        )
        .gap(8.0)
    };
    let no_tools = settings_projection(shared, "settings agent editor has no tools", |snapshot| {
        snapshot.agents.editor_tools.is_empty()
    });
    let available_tools = settings_projection(shared, "settings agent addable tools", |snapshot| {
        snapshot.agents.available_tools.clone()
    });
    let can_add_tool = derived(
        available_tools.clone(),
        "settings agent can add tool",
        |tools: &Vec<String>| !tools.is_empty(),
    );
    // The option list has no reader form, so only the select itself follows the addable tools.
    let tool_select = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::key_from(available_tools, move |names| {
            let selected = s.clone();
            let count = names.len();
            let change = s.clone();
            WidgetPod::new(
                Select::new("agent-add-tool-select")
                    .options(names.clone())
                    .selected_when(move || {
                        Some(
                            selected
                                .settings()
                                .agent_form
                                .tool_add_index
                                .min(count.saturating_sub(1)),
                        )
                    })
                    .theme_when(clone_reader(&r))
                    .on_change(move |index, _| {
                        change.settings_mut().set_agent_tool_add_index(index)
                    }),
            )
        })
    };
    let add_tool = {
        let s = shared.clone();
        sui::Button::new("Add tool")
            .icon(IconGlyph::Add)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || s.settings_mut().add_selected_agent_tool())
    };
    let mut tools = Column::new(reader, |theme| theme.metrics.field_group_spacing)
        .with_child(revealed(
            value_row(
                shared,
                reader,
                "Packaged tools",
                "No packaged tools enabled",
            ),
            no_tools,
        ))
        .with_child(tool_rows)
        .with_child(revealed(
            inline_row(
                "Add packaged tool",
                FieldWithAction::new(tool_select, add_tool),
            )
            .theme_when(clone_reader(reader)),
            can_add_tool,
        ));
    tools = tools
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-skills",
            "Skills",
            form.skills_text.clone(),
            "skill-id per line",
            84.0,
            |settings, value| settings.set_agent_skills_text(value),
        ))
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-visible-toolsets",
            "Visible toolsets",
            form.visible_toolsets_text.clone(),
            "toolset id per line; empty uses the package default",
            76.0,
            |settings, value| settings.set_agent_visible_toolsets_text(value),
        ))
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-allow-tools",
            "Additional allowed tools",
            form.allow_tools_text.clone(),
            "tool id per line",
            76.0,
            |settings, value| settings.set_agent_allow_tools_text(value),
        ))
        .with_child(agent_area_row(
            shared,
            reader,
            "agent-deny-tools",
            "Denied tools",
            form.deny_tools_text.clone(),
            "tool id per line",
            76.0,
            |settings, value| settings.set_agent_deny_tools_text(value),
        ))
        .with_child(value_row(
            shared,
            reader,
            "Realtime handoff",
            "Voice sessions can delegate coding, research, and longer work to this regular agent while preserving the conversation context.",
        ));

    let runtime = FieldGroup::new()
        .fill_width(true)
        .gap(8.0)
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
            "agent-home-server",
            "Preferred home server",
            form.home_server_node_id.clone(),
            "Local session placement",
            |settings, value| settings.set_agent_home_server_node_id(value),
        ))
        .with_child(agent_text_row(
            shared,
            reader,
            "agent-sandbox-node",
            "Sandbox execution node",
            form.sandbox_node_id.clone(),
            "Local runtime",
            |settings, value| settings.set_agent_sandbox_node_id(value),
        ))
        .with_child(agent_home_folder_row(
            shared,
            reader,
            &form.id,
            form.previous_id.is_some(),
        ));

    // Read-only package facts; rebuilt only when the saved summary changes.
    let package = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::key_from(
            settings_projection(shared, "settings agent package summary", |snapshot| {
                let id = snapshot.agents.selected_id.as_deref()?;
                snapshot
                    .agents
                    .agents
                    .iter()
                    .find(|agent| agent.id == id)
                    .cloned()
            }),
            move |summary| WidgetPod::new(agent_package_rows(&s, &r, summary.clone())),
        )
    };
    Stack::vertical()
        .gap(14.0)
        .alignment(Alignment::Stretch)
        .with_child(
            SettingsCard::new(if form.previous_id.is_some() {
                    format!("Edit {}", short_one_line(&form.id, 48))
                } else {
                    "New custom agent".to_string()
                })
                .description("Identity and system instructions used for every session.")
                .row(profile)
                .build(shared, reader),
        )
        .with_child(
            SettingsCard::new("Default model")
                .description("Leave provider or model blank to inherit the cluster and provider defaults.")
                .row(model)
                .build(shared, reader),
        )
        .with_child(
            SettingsCard::new("Tools and skills")
                .description("Packaged tools define the base capability set; tool-surface rules can further expose or restrict runtime tools.")
                .row(tools)
                .build(shared, reader),
        )
        .with_child(agent_external_integrations_sections(shared, reader))
        .with_child(
            SettingsCard::new("Runtime")
                .description("Default turn limits and preferred placement for sessions and sandboxed tools.")
                .row(runtime)
                .build(shared, reader),
        )
        .with_child(
            SettingsCard::new("Package")
                .description("Saved custom definitions live in SIFS and become available to chats and channels.")
                .row(package)
                .build(shared, reader),
        )
}

fn node_external_integrations_permission_section(
    shared: &Shared,
    reader: &ThemeReader,
) -> impl Widget + use<> {
    permission_card(
        shared,
        reader,
        PermissionCard {
            title: "External integrations",
            description: "Lets agents use MCP tools that reach external servers from this node. Applies immediately to chat and voice sessions.",
            details: [
                "Reading the node-local permission document.",
                "Permission state could not be read, so MCP calls stay blocked.",
                "Configured MCP tools may send arguments to external servers.",
                "MCP tools stay visible in agent definitions but cannot run on this node.",
            ],
            switch_name: "Allow external integrations",
            allow_label: "Allow external integrations…",
            pending_name: "external-integrations-permission-pending",
            confirm_region: "Allow external integrations confirmation",
            confirm_help: "MCP servers receive model-generated tool arguments and may reach network services or take external actions. Each agent's tool rules and server allow-list still apply.",
            confirm_description: "Confirmation is required before enabling MCP execution on this node",
            error_name: "external-integrations-permission-error",
            scope: || SettingsOperationScope::ExternalIntegrationsPermission,
            set: |enabled| SettingsOperation::SetExternalIntegrationsPermission { enabled },
            confirm: SettingsState::confirm_external_integrations_enable,
            cancel: SettingsState::cancel_external_integrations_enable,
            view: |snapshot| {
                let remote = &snapshot.remote_management;
                let (_, _, confirming, pending) = remote.external_integrations;
                PermissionView::new(
                    remote.snapshot.permission_loading,
                    remote.snapshot.permission_error.clone(),
                    remote.snapshot.external_integrations_enabled,
                    pending,
                    confirming,
                )
            },
            feedback: |snapshot| {
                let (error, status, _, _) = &snapshot.remote_management.external_integrations;
                (error.clone(), status.clone())
            },
        },
        Vec::new(),
    )
    .row(details(
        shared,
        reader,
        "external-integrations-commands",
        "shell commands",
        vec![
            widgets::Boxed::new(
                SettingRow::new("Allow from a shell")
                    .control(copy_control(
                        reader,
                        "external-integrations-allow-command",
                        "Allow integrations from CLI",
                        EXTERNAL_INTEGRATIONS_ALLOW_COMMAND.to_string(),
                    ))
                    .build(shared),
            ),
            widgets::Boxed::new(
                SettingRow::new("Deny from a shell")
                    .control(copy_control(
                        reader,
                        "external-integrations-deny-command",
                        "Deny integrations from CLI",
                        EXTERNAL_INTEGRATIONS_DENY_COMMAND.to_string(),
                    ))
                    .build(shared),
            ),
        ],
    ))
    .build(shared, reader)
}

fn agent_external_integrations_sections(
    shared: &Shared,
    reader: &ThemeReader,
) -> impl Widget + use<> {
    let s = shared.clone();
    let permission_section = SettingsCard::new("Host authorization").row(FieldGroup::new().gap(8.0)
        .with_child(muted_label(shared, "MCP server definitions are shared with the cluster. Execution requires consent on the node that runs them."))
        .with_child(sui::Button::new("Host access settings").theme_when(clone_reader(reader))
            .on_press(move || s.settings_mut().open(SettingsSection::RemoteManagement)))).build(shared, reader);

    let add_server = {
        let s = shared.clone();
        sui::Button::new("Add MCP server")
            .icon(IconGlyph::Add)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || s.settings_mut().add_agent_mcp_server())
    };
    let servers = settings_projection(shared, "settings agent MCP servers", |snapshot| {
        snapshot.agents.editor_mcp_servers.clone()
    });
    let server_summary = derived(
        servers.clone(),
        "settings agent MCP server summary",
        |servers: &Vec<SettingsAgentMcpServerRow>| {
            if servers.is_empty() {
                "No MCP servers configured".to_string()
            } else {
                let enabled = servers.iter().filter(|server| server.enabled).count();
                format!(
                    "{} configured · {enabled} enabled · tools are shared by chat and voice",
                    servers.len()
                )
            }
        },
    );
    let overview = SettingsCard::new("MCP servers").description("Each server is stored in this agent package. Secrets remain node-local environment variables beginning with SINOMO_MCP_. Leave the protocol on automatic unless an endpoint mishandles the stateless probe. Save the agent, then run its readiness test to connect and discover exposed tools.",).row(FieldGroup::new()
            .gap(8.0)
            .with_child(value_row(
                shared,
                reader,
                "Transport",
                "Streamable HTTP · MCP 2026-07-28 with handshake fallback · Chat + voice",
            ))
            .with_child(value_row_from(shared, reader, "Servers", server_summary))
            .with_child(
                inline_row("Actions", Stack::horizontal().with_child(add_server))
                    .theme_when(clone_reader(reader)),
            )).build(shared, reader);

    // Servers are keyed by their draft id, so adding or removing one leaves the others' fields,
    // focus, and caret in place.
    let server_cards = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        KeyedStack::vertical(
            servers,
            |server: &SettingsAgentMcpServerRow| server.ui_id,
            move |ui_id, server| agent_mcp_server_section(&s, &r, *ui_id, server),
        )
        .gap(14.0)
    };
    Column::new(reader, |_| 14.0)
        .with_child(permission_section)
        .with_child(overview)
        .with_child(server_cards)
}

fn agent_mcp_server_section(
    shared: &Shared,
    reader: &ThemeReader,
    ui_id: u64,
    row: Signal<SettingsAgentMcpServerRow>,
) -> impl Widget + use<> {
    let server = shared
        .settings()
        .agent_form
        .mcp_servers
        .iter()
        .find(|server| server.ui_id == ui_id)
        .cloned()
        .unwrap_or_else(|| {
            AgentMcpServerForm::from_definition(
                ui_id,
                &AgentMcpServerDefinition::streamable_http(String::new(), String::new()),
            )
        });
    let input_name = |field: &str| format!("agent-mcp-{ui_id}-{field}");
    let enabled = {
        let s = shared.clone();
        switch_row(reader, "Enabled", server.enabled, move |enabled| {
            s.settings_mut()
                .update_agent_mcp_server(ui_id, |server| server.enabled = enabled);
        })
    };
    let required = {
        let s = shared.clone();
        switch_row(
            reader,
            "Required at startup",
            server.required,
            move |required| {
                s.settings_mut()
                    .update_agent_mcp_server(ui_id, |server| server.required = required);
            },
        )
    };
    let remove = {
        let s = shared.clone();
        sui::Button::new("Remove server")
            .icon(IconGlyph::Trash)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Filled)
            .tone(SemanticTone::Danger)
            .on_press(move || s.settings_mut().remove_agent_mcp_server(ui_id))
    };
    let mut body = FieldGroup::new()
        .fill_width(true)
        .gap(8.0)
        .with_child(agent_text_row(
            shared,
            reader,
            &input_name("name"),
            "Server name",
            server.name.clone(),
            "filesystem",
            move |settings, value| {
                settings.update_agent_mcp_server(ui_id, |server| server.name = value)
            },
        ));
    let protocol = {
        let selected = shared.clone();
        let s = shared.clone();
        select_row(
            reader,
            &input_name("protocol"),
            "Protocol",
            AGENT_MCP_PROTOCOL_ERA_OPTIONS,
            move || {
                Some(
                    selected
                        .settings()
                        .agent_form
                        .mcp_servers
                        .iter()
                        .find(|server| server.ui_id == ui_id)
                        .map_or(0, |server| server.protocol_era_index),
                )
            },
            move |index, _label| {
                s.settings_mut().update_agent_mcp_server(ui_id, |server| {
                    server.protocol_era_index =
                        index.min(AGENT_MCP_PROTOCOL_ERA_OPTIONS.len().saturating_sub(1));
                });
            },
        )
    };
    body = body
        .with_child(agent_text_row(
            shared,
            reader,
            &input_name("url"),
            "Endpoint",
            server.url.clone(),
            "https://example.com/mcp",
            move |settings, value| {
                settings.update_agent_mcp_server(ui_id, |server| server.url = value)
            },
        ))
        .with_child(protocol)
        .with_child(enabled)
        .with_child(required);
    body = body.with_child(agent_area_row(
        shared,
        reader,
        &input_name("allowed-tools"),
        "Allowed tools",
        server.allowed_tools_text.clone(),
        "one original MCP tool name per line; empty exposes all",
        76.0,
        move |settings, value| {
            settings.update_agent_mcp_server(ui_id, |server| server.allowed_tools_text = value)
        },
    ));
    body = body.with_child(agent_text_row(
        shared,
        reader,
        &input_name("bearer-token-env"),
        "Bearer token variable",
        server.bearer_token_env.clone(),
        "SINOMO_MCP_FILES_TOKEN",
        move |settings, value| {
            settings.update_agent_mcp_server(ui_id, |server| server.bearer_token_env = value)
        },
    ));
    body = body.with_child(agent_area_row(
        shared,
        reader,
        &input_name("headers"),
        "Headers from environment",
        server.headers_from_env_text.clone(),
        "X-Api-Key: SINOMO_MCP_FILES_KEY",
        76.0,
        move |settings, value| {
            settings.update_agent_mcp_server(ui_id, |server| server.headers_from_env_text = value)
        },
    ));
    body = body.with_child(agent_text_row(
        shared,
        reader,
        &input_name("startup-timeout"),
        "Startup timeout (ms)",
        server.startup_timeout_ms.clone(),
        "10000",
        move |settings, value| {
            settings.update_agent_mcp_server(ui_id, |server| server.startup_timeout_ms = value)
        },
    ));
    body = body
        .with_child(agent_text_row(
            shared,
            reader,
            &input_name("tool-timeout"),
            "Tool timeout (ms)",
            server.tool_timeout_ms.clone(),
            "60000",
            move |settings, value| {
                settings.update_agent_mcp_server(ui_id, |server| server.tool_timeout_ms = value)
            },
        ))
        .with_child(
            inline_row("Actions", Stack::horizontal().with_child(remove))
                .theme_when(clone_reader(reader)),
        );
    let title = row.select_named("settings agent MCP server title", |row| {
        mcp_server_card_title(&row.name)
    });
    SettingsCard::new(mcp_server_card_title(&server.name))
        .title_from(title)
        .description("The server's discovered tools use the same implementation in regular chat and realtime voice. Non-loopback endpoints must use HTTPS.",)
        .row(body)
        .build(shared, reader)
}

fn mcp_server_card_title(name: &str) -> String {
    if name.trim().is_empty() {
        "MCP server".to_string()
    } else {
        format!("MCP · {name}")
    }
}

fn agent_tool_editor_row(shared: &Shared, reader: &ThemeReader, tool: AgentToolSpec) -> FormRow {
    let mode = agent_tool_invocation_mode_label(&tool.name);
    let description = if tool.async_default {
        format!(
            "{mode} · {} Runs asynchronously by default.",
            tool.description
        )
    } else {
        format!("{mode} · {}", tool.description)
    };
    let remove = {
        let s = shared.clone();
        let name = tool.name.clone();
        let label = format!("Remove {}", tool.name);
        let button = IconButton::new(IconGlyph::Trash, label.clone())
            .description(format!("Remove {} from this agent", tool.name))
            .theme_when(clone_reader(reader))
            .on_press(move || s.settings_mut().remove_agent_tool(&name));
        Tooltip::new(label, button)
            .placement(TooltipPlacement::Above)
            .theme_when(clone_reader(reader))
    };
    inline_row(
        tool.name,
        FieldWithAction::new(muted_label(shared, description), remove),
    )
    .theme_when(clone_reader(reader))
}

fn agent_tool_invocation_mode_label(tool_name: &str) -> &'static str {
    let modes = fallback_agent_tool_descriptors()
        .iter()
        .find(|descriptor| descriptor.spec.name == tool_name)
        .map(|descriptor| descriptor.invocation_modes.clone())
        .unwrap_or_default();
    let turn_based = modes.contains(&AgentToolInvocationMode::TurnBased);
    let realtime = modes.contains(&AgentToolInvocationMode::Realtime);
    match (turn_based, realtime) {
        (true, true) => "Chat + voice",
        (false, true) => "Voice",
        _ => "Chat",
    }
}

fn agent_home_folder_row(
    shared: &Shared,
    reader: &ThemeReader,
    agent_id: &str,
    persisted: bool,
) -> FormRow {
    let path = agent_home_sifs_path_for_id(agent_id);
    let open = {
        let s = shared.clone();
        let path = path.clone();
        sui::Button::new("Open home folder")
            .icon(IconGlyph::Folder)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled(persisted)
            .on_press(move || open_agent_home_folder(&s, path.clone()))
    };
    let detail = if persisted {
        "Shared SIFS home mounted into this agent's tool runtime."
    } else {
        "The home folder becomes available after this agent is saved and first used."
    };
    FormRow::new(
        "Agent home",
        Stack::vertical()
            .gap(6.0)
            .alignment(Alignment::Stretch)
            .with_child(FieldWithAction::new(
                selectable_read_only_input(reader, "agent-home-path", path),
                open,
            ))
            .with_child(muted_label(shared, detail)),
    )
    .stacked()
    .theme_when(clone_reader(reader))
}

fn open_agent_home_folder(shared: &Shared, path: String) {
    let waker = shared.borrow().waker.clone();
    {
        let mut files = shared.files_mut();
        files.open_sifs_path(path, waker);
    }
    open_tab(shared, Tab::Files);
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
    inline_row(
        "Actions",
        Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(8.0)
            .align_items(Alignment::Start)
            .with_child(save)
            .with_child(duplicate)
            .with_child(delete_controls),
    )
    .theme_when(clone_reader(reader))
}

fn agent_delete_controls(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let persisted = shared.settings().agent_form.previous_id.is_some();
    let confirm = {
        let s = shared.clone();
        let pending = settings_projection(shared, "settings agent delete enabled", |snapshot| {
            !snapshot.agents.pending
        });
        sui::Button::new("Confirm delete")
            .icon(sui::IconGlyph::Trash)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Filled)
            .tone(SemanticTone::Danger)
            .enabled_from(pending)
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
    let delete = {
        let s = shared.clone();
        sui::Button::new("Delete agent")
            .icon(sui::IconGlyph::Trash)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Filled)
            .tone(SemanticTone::Danger)
            .enabled(persisted)
            .on_press(move || s.settings_mut().confirm_agent_delete())
    };
    let confirming = settings_projection(shared, "settings agent delete confirming", |snapshot| {
        usize::from(snapshot.agents.delete_confirming)
    });
    SwitchView::new()
        .with_child(
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .align_items(Alignment::Start)
                .with_child(delete),
        )
        .with_child(
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .align_items(Alignment::Start)
                .with_child(confirm)
                .with_child(cancel),
        )
        .selected_from(confirming)
}

/// The saved package's readiness, versions, and provenance, when the agent has been saved.
fn agent_package_rows(
    shared: &Shared,
    reader: &ThemeReader,
    summary: Option<AgentConfigSummary>,
) -> impl Widget + use<> {
    let mut package = FieldGroup::new().fill_width(true).gap(8.0);
    let Some(summary) = summary else {
        return package;
    };
    package = package
        .with_child(agent_readiness_row(shared, reader, summary.readiness))
        .with_child(value_row(
            shared,
            reader,
            "Package",
            format!("{} · version {}", summary.source, summary.package_version),
        ));
    if let Some(module_id) = summary
        .module_id
        .as_deref()
        .filter(|module_id| !module_id.trim().is_empty())
    {
        let module = summary
            .module_version
            .as_deref()
            .filter(|version| !version.trim().is_empty())
            .map(|version| format!("{module_id} · version {version}"))
            .unwrap_or_else(|| module_id.to_string());
        package = package.with_child(value_row(shared, reader, "Module", module));
    }
    package.with_child(value_row(shared, reader, "Provenance", summary.provenance))
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
    let summary = selected_agent_config_summary(shared);
    // Readiness is reported with the catalog, which refreshes without reopening the agent.
    let readiness = settings_projection(shared, "settings agent readiness", |snapshot| {
        let id = snapshot.agents.selected_id.as_deref()?;
        snapshot
            .agents
            .agents
            .iter()
            .find(|agent| agent.id == id)
            .map(|agent| agent.readiness)
    });
    let presentation = |readiness: &Option<AgentReadiness>| {
        agent_readiness_presentation(readiness.unwrap_or(AgentReadiness::NeedsAttention))
    };
    let readiness_label = derived(
        readiness.clone(),
        "settings agent readiness label",
        move |r| presentation(r).0.to_string(),
    );
    let readiness_detail = derived(
        readiness.clone(),
        "settings agent readiness detail",
        move |r| presentation(r).1.to_string(),
    );
    let readiness_badge = Restyle::new(
        StatusBadge::new(readiness_label.get())
            .label_from(readiness_label)
            .theme_when(clone_reader(reader)),
        readiness,
        move |badge, readiness| {
            let (_, _, tone, icon) = presentation(readiness);
            badge.tone(tone).icon(icon)
        },
    );
    let prompt_copy = {
        let prompt = definition.system_prompt.clone();
        sui::Button::new("Copy")
            .semantic_name("Copy System prompt")
            .icon(IconGlyph::Restore)
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| ctx.set_clipboard_text(&prompt))
    };

    let mut card = SettingsCard::new(definition.name.clone())
        .description("Supplied by the runtime. Duplicate it to make an editable custom agent.")
        .header_action(readiness_badge)
        .row(
            SettingRow::new("Role")
                .value(shared, agent_role_label(&definition.role))
                .build(shared),
        )
        .row(
            SettingRow::new("Description")
                .value(
                    shared,
                    if definition.description.trim().is_empty() {
                        "No description".to_string()
                    } else {
                        definition.description.clone()
                    },
                )
                .build(shared),
        )
        .row(
            SettingRow::new("Model policy")
                .value(shared, agent_model_summary(&definition))
                .build(shared),
        )
        .row(
            SettingRow::new("Tools")
                .value(shared, agent_tools_summary(&definition))
                .build(shared),
        )
        .row(
            SettingRow::new("MCP servers")
                .value(shared, agent_mcp_servers_summary(&definition))
                .build(shared),
        )
        .row(
            SettingRow::new("System prompt")
                .control(prompt_copy)
                .build(shared),
        )
        .row(
            SizedBox::new()
                .height(64.0)
                .child(selectable_read_only_area(
                    reader,
                    "agent-readonly-system-prompt",
                    definition.system_prompt.clone(),
                )),
        )
        .row(agent_home_setting_row(
            shared,
            reader,
            definition.key(),
            false,
        ))
        .row(SettingRow::new("Actions").control(duplicate).build(shared));

    let mut technical = vec![
        widgets::Boxed::new(
            SettingRow::new("Readiness")
                .value_from(shared, readiness_detail)
                .build(shared),
        ),
        widgets::Boxed::new(
            SettingRow::new("Agent ID")
                .control(copy_control(
                    reader,
                    "agent-readonly-id",
                    "Agent ID",
                    definition.key().to_string(),
                ))
                .build(shared),
        ),
        widgets::Boxed::new(
            SettingRow::new("Package version")
                .value(shared, definition.package.version.clone())
                .build(shared),
        ),
        widgets::Boxed::new(
            SettingRow::new("Source")
                .value(
                    shared,
                    agent_source_label(&definition.package.provenance.source),
                )
                .build(shared),
        ),
        widgets::Boxed::new(
            SettingRow::new("Provenance")
                .value(
                    shared,
                    summary
                        .map(|summary| summary.provenance)
                        .unwrap_or_else(|| "Package provenance unavailable".to_string()),
                )
                .build(shared),
        ),
    ];
    if let Some(node_id) = definition.tool_runtime.sandbox_node_id.as_ref() {
        technical.push(widgets::Boxed::new(
            SettingRow::new("Sandbox node")
                .value(shared, node_id.clone())
                .build(shared),
        ));
    }
    if let Some(node_id) = definition.placement.home_server_node_id.as_ref() {
        technical.push(widgets::Boxed::new(
            SettingRow::new("Preferred home server")
                .value(shared, node_id.clone())
                .build(shared),
        ));
    }
    card = card.row(details(
        shared,
        reader,
        "agent-technical",
        "package details",
        technical,
    ));
    card.build(shared, reader)
}

/// The agent's SIFS home folder, with a button that opens it in Files once it exists.
fn agent_home_setting_row(
    shared: &Shared,
    reader: &ThemeReader,
    agent_id: &str,
    persisted: bool,
) -> impl Widget + use<> {
    let path = agent_home_sifs_path_for_id(agent_id);
    let open = {
        let s = shared.clone();
        let path = path.clone();
        sui::Button::new("Open home folder")
            .icon(IconGlyph::Folder)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled(persisted)
            .on_press(move || open_agent_home_folder(&s, path.clone()))
    };
    let help = if persisted {
        format!("{path} · Shared SIFS home mounted into this agent's tools.")
    } else {
        format!("{path} · Available after the agent is saved and first used.")
    };
    SettingRow::new("Agent home")
        .help(help)
        .control(open)
        .build(shared)
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

fn agent_readiness_presentation(
    readiness: AgentReadiness,
) -> (&'static str, &'static str, SemanticTone, IconGlyph) {
    match readiness {
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
    }
}

fn agent_readiness_row(
    shared: &Shared,
    reader: &ThemeReader,
    readiness: AgentReadiness,
) -> FormRow {
    let (label, detail, tone, icon) = agent_readiness_presentation(readiness);
    FormRow::new(
        "Readiness",
        Stack::vertical()
            .gap(4.0)
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
    let mut body = FieldGroup::new().gap(8.0).with_child(
        inline_row(
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
        .theme_when(clone_reader(reader)),
    );
    for check in report.checks {
        body = body.with_child(
            FormRow::new(
                check.name,
                Stack::vertical()
                    .gap(4.0)
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
    SettingsCard::new(title)
        .description(summary)
        .row(body)
        .build(shared, reader)
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
            on_change(&mut settings, value);
        });
    inline_row(label, input).theme_when(clone_reader(reader))
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
    let area = SizedBox::new().height(height).child(
        TextArea::new(name)
            .value(value)
            .placeholder(placeholder)
            .theme_when(clone_reader(reader))
            .on_change(move |value| {
                let mut settings = s.settings_mut();
                on_change(&mut settings, value);
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

fn agent_mcp_servers_summary(definition: &AgentDefinition) -> String {
    if definition.tool_runtime.mcp_servers.is_empty() {
        return "None configured".to_string();
    }
    definition
        .tool_runtime
        .mcp_servers
        .iter()
        .map(|server| {
            let state = if server.enabled {
                "enabled"
            } else {
                "disabled"
            };
            let requirement = if server.required {
                "required"
            } else {
                "optional"
            };
            let mut facets = vec![state.to_string(), requirement.to_string()];
            if !server.protocol_era.is_default() {
                facets.push(format!("{} protocol", server.protocol_era.as_str()));
            }
            format!("{} ({})", server.name, facets.join(", "))
        })
        .collect::<Vec<_>>()
        .join(" · ")
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

/// One module card on the Modules or Storage page. `settings` are the node-scoped catalog
/// entries the card edits, by catalog index.
#[derive(Clone, Debug, PartialEq)]
struct ModuleCardStructure {
    module: SettingsModuleSummaryStructure,
    node_id: Option<String>,
    node_label: String,
    node_error: Option<String>,
    managed_elsewhere: bool,
    settings: Vec<(usize, ConfigItemStructure)>,
    revision: u64,
}

impl ModuleCardStructure {
    fn key(&self) -> (String, Option<String>, bool) {
        (
            self.module.module_id.clone(),
            self.node_id.clone(),
            self.managed_elsewhere,
        )
    }
}

/// The owners of the two mount settings, each with whether Storage shows its settings: the SIFS
/// mount module (its settings are the mount card) and the NFS mount module. They are found as
/// the owners of their settings because the app reaches the runtime through the client facade,
/// which does not export the storage crates.
fn storage_module_ids(items: &[ConfigItemStructure]) -> Vec<(String, bool)> {
    let owner = |key: &str| {
        items
            .iter()
            .find(|item| item.key == key)
            .and_then(|item| item.module_id.clone())
    };
    let mut modules: Vec<(String, bool)> = Vec::new();
    for (key, with_settings) in [(SIFS_MOUNT_CONFIG_KEY, false), (NFS_MOUNT_CONFIG_KEY, true)] {
        if let Some(module_id) = owner(key)
            && !modules
                .iter()
                .any(|(known, _)| module_ids_match(known, &module_id))
        {
            modules.push((module_id, with_settings));
        }
    }
    modules
}

fn module_card_structure(
    snapshot: &SettingsStructureSnapshot,
    module: &SettingsModuleSummaryStructure,
    managed_elsewhere: bool,
    settings: Vec<(usize, ConfigItemStructure)>,
) -> ModuleCardStructure {
    let node = snapshot.modules.selected_node.as_ref();
    ModuleCardStructure {
        module: module.clone(),
        node_id: node.map(|node| node.node_id.clone()),
        node_label: node
            .map(|node| node.display_name.clone())
            .unwrap_or_else(|| "No node selected".to_string()),
        node_error: node.and_then(|node| node.error.clone()),
        managed_elsewhere,
        settings,
        revision: snapshot.configuration.ui_revision,
    }
}

fn node_module_settings(snapshot: &SettingsStructureSnapshot) -> Vec<(usize, ConfigItemStructure)> {
    snapshot
        .configuration
        .items
        .iter()
        .enumerate()
        .filter(|(_, item)| item.module_id.is_some() && item.node_scoped)
        .map(|(index, item)| (index, item.clone()))
        .collect()
}

/// The Modules page cards, each with its settings, and the settings no installed module owns.
fn modules_page_cards(
    snapshot: &SettingsStructureSnapshot,
) -> (Vec<ModuleCardStructure>, Vec<(usize, ConfigItemStructure)>) {
    let catalog = node_module_settings(snapshot);
    let storage = storage_module_ids(&snapshot.configuration.items);
    let mut rendered = HashSet::new();
    let mut cards = Vec::new();
    for module in &snapshot.modules.modules {
        let mut associated = Vec::new();
        for (index, item) in &catalog {
            if !rendered.contains(index)
                && item.module_id.as_deref().is_some_and(|config_module_id| {
                    module_ids_match(config_module_id, &module.module_id)
                })
            {
                rendered.insert(*index);
                associated.push((*index, item.clone()));
            }
        }
        // The mount modules are managed in Storage, beside the mount settings.
        let managed_elsewhere = storage
            .iter()
            .any(|(id, _)| module_ids_match(id, &module.module_id));
        if managed_elsewhere {
            associated.clear();
        }
        cards.push(module_card_structure(
            snapshot,
            module,
            managed_elsewhere,
            associated,
        ));
    }
    let unmatched = catalog
        .into_iter()
        .filter(|(index, _)| !rendered.contains(index))
        .collect();
    (cards, unmatched)
}

/// The Storage page's module cards: lifecycle for the SIFS mount module, and lifecycle and
/// settings for the NFS mount module.
fn storage_page_cards(snapshot: &SettingsStructureSnapshot) -> Vec<ModuleCardStructure> {
    let catalog = node_module_settings(snapshot);
    storage_module_ids(&snapshot.configuration.items)
        .into_iter()
        .filter_map(|(module_id, with_settings)| {
            let module = snapshot
                .modules
                .modules
                .iter()
                .find(|module| module_ids_match(&module.module_id, &module_id))?;
            let settings = catalog
                .iter()
                .filter(|(_, item)| {
                    with_settings
                        && item
                            .module_id
                            .as_deref()
                            .is_some_and(|id| module_ids_match(id, &module.module_id))
                })
                .cloned()
                .collect();
            Some(module_card_structure(snapshot, module, false, settings))
        })
        .collect()
}

/// Module cards keyed by module and node, so a status refresh or a settings edit updates the
/// matching card in place.
fn module_card_stack(
    shared: &Shared,
    reader: &ThemeReader,
    cards: SettingsProjection<Vec<ModuleCardStructure>>,
) -> KeyedStack<(String, Option<String>, bool), ModuleCardStructure> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    KeyedStack::vertical(cards, ModuleCardStructure::key, move |_, card| {
        if card.get().managed_elsewhere {
            widgets::Boxed::new(managed_elsewhere_card(
                &s,
                &r,
                &card.get().module.label,
                SettingsSection::Storage,
            ))
        } else {
            widgets::Boxed::new(module_config_section(&s, &r, card))
        }
    })
    .gap(16.0)
}

fn section_modules(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    section_modules_body(shared, reader)
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
    format!(
        "selected:{:?};error:{:?};status:{:?};warnings:{:?};config-revision:{}\n{nodes}",
        settings.module_selected_node_id,
        settings.module_action_error,
        settings.module_action_status,
        snapshot.warnings,
        settings.config_ui_revision,
    )
}

/// The Modules page: a summary that updates in place, one keyed card per module, and the
/// settings of modules missing from the node.
fn section_modules_body(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
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
    let cluster_defaults =
        settings_projection(shared, "settings modules cluster defaults", |snapshot| {
            let configured = snapshot.modules.cluster_config.len();
            let enabled = snapshot
                .modules
                .cluster_config
                .iter()
                .filter(|(_, enabled)| *enabled)
                .count();
            if configured == 0 {
                "No cluster defaults".to_string()
            } else {
                format!("{enabled} enabled · {configured} configured")
            }
        });
    let node_error = settings_projection(shared, "settings modules node error", |snapshot| {
        snapshot
            .modules
            .selected_node
            .as_ref()
            .and_then(|node| node.error.clone())
            .unwrap_or_default()
    });
    let warnings = settings_projection(shared, "settings modules warnings", |snapshot| {
        snapshot.modules.warnings.join("; ")
    });
    let not_empty = |text: &String| !text.is_empty();
    let summary = SettingsCard::new("Modules")
        .description("Turn modules on or off for the selected node. Cluster-wide defaults are in Configuration.")
        .header_action(refresh)
        .row(
            SettingRow::new("Cluster defaults")
                .value_from(shared, cluster_defaults)
                .build(shared),
        )
        .row(revealed(
            SettingRow::new("Node config")
                .value_from(shared, node_error.clone())
                .build(shared),
            derived(node_error, "settings modules has node error", not_empty),
        ))
        .row(revealed(
            SettingRow::new("Warnings")
                .value_from(shared, warnings.clone())
                .build(shared),
            derived(warnings, "settings modules has warnings", not_empty),
        ))
        .row(feedback_slot(shared, reader, |snapshot| {
            (
                snapshot.modules.action_error.clone(),
                snapshot.modules.action_status.clone(),
            )
        }));

    let cards = settings_projection(shared, "settings module cards", |snapshot| {
        modules_page_cards(snapshot).0
    });
    let unmatched = settings_projection(shared, "settings unmatched module settings", |snapshot| {
        let (_, unmatched) = modules_page_cards(snapshot);
        (unmatched, snapshot.configuration.ui_revision)
    });
    let has_unmatched = derived(
        unmatched.clone(),
        "settings has unmatched module settings",
        |(unmatched, _): &(Vec<(usize, ConfigItemStructure)>, u64)| !unmatched.is_empty(),
    );
    // Editors for settings without an installed module; rebuilt for catalog or structural draft
    // changes only.
    let unmatched_card = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::key_from(unmatched, move |(unmatched, _)| {
            let mut card = SettingsCard::new("Additional module settings")
                .description("Settings declared by modules that are not installed on this node.");
            for (index, _) in unmatched {
                if let Some(item) = s.settings().config_items.get(*index).cloned() {
                    card = card.row(PodHost::new(module_config_editor_row(
                        &s, &r, *index, &item,
                    )));
                }
            }
            WidgetPod::new(card.build(&s, &r))
        })
    };
    let no_modules = settings_projection(shared, "settings no modules", |snapshot| {
        snapshot.modules.modules.is_empty()
    });
    Column::new(reader, |_| 16.0)
        .with_child(summary.build(shared, reader))
        .with_child(revealed(
            SettingsCard::new("No modules")
                .description("No installed or configured modules were found on this node.")
                .build(shared, reader),
            no_modules,
        ))
        .with_child(module_card_stack(shared, reader, cards))
        .with_child(revealed(unmatched_card, has_unmatched))
}

/// A module whose settings live on a dedicated page: name it here and link there.
fn managed_elsewhere_card(
    shared: &Shared,
    reader: &ThemeReader,
    label: &str,
    section: SettingsSection,
) -> impl Widget + use<> {
    let open = {
        let s = shared.clone();
        sui::Button::new(format!("Open {}", section.title()))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .theme_when(clone_reader(reader))
            .on_press(move || s.settings_mut().open(section))
    };
    SettingsCard::new(label.to_string())
        .row(
            SettingRow::new(format!("Managed in {}", section.title()))
                .help("Turn it on, configure it, and check its status there.")
                .control(open)
                .build(shared),
        )
        .build(shared, reader)
}

fn module_config_section(
    shared: &Shared,
    reader: &ThemeReader,
    card: Signal<ModuleCardStructure>,
) -> impl Widget + use<> {
    let current = card.get();
    let module = current.module.clone();
    let module_id = module.module_id.clone();
    let native_rust = module.backend == "native-rust";
    let node_override = {
        let s = shared.clone();
        let selected_module_id = module_id.clone();
        Select::new(format!("module-override-{module_id}"))
            .options(MODULE_OVERRIDE_OPTIONS.iter().copied())
            .selected_when({
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
            })
            .theme_when(clone_reader(reader))
            .on_change(move |index, _label| {
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
                            native_rust,
                        },
                    );
                } else {
                    s.settings_mut().module_action_error = Some("select a node first".to_string());
                }
            })
    };
    let summary = card.select_named("settings module summary", |card| card.module.clone());
    let text = |name: &'static str, select: fn(&SettingsModuleSummaryStructure) -> String| {
        derived(summary.clone(), name, select)
    };
    let tone = |name: &'static str, select: fn(&SettingsModuleSummaryStructure) -> SemanticTone| {
        derived(summary.clone(), name, select)
    };
    let node_error = card.select_named("settings module node error", |card| {
        card.node_error.clone().unwrap_or_default()
    });
    let settings = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        // Setting editors change shape only with the catalog or a structural draft change.
        RebuildOnChange::key_from(
            card.select_named("settings module settings", |card| {
                (card.settings.clone(), card.revision)
            }),
            move |(settings, _)| {
                let mut rows = Column::new(&r, |theme| theme.metrics.form_section_body_gap);
                for (index, _) in settings {
                    if let Some(item) = s.settings().config_items.get(*index).cloned() {
                        rows.push(PodHost::new(module_config_editor_row(
                            &s, &r, *index, &item,
                        )));
                    }
                }
                WidgetPod::new(rows)
            },
        )
    };

    SettingsCard::new(module.label.clone())
        .description(module.detail.clone())
        .row(
            SettingRow::new("Node override")
                .help("Inherit follows the cluster default.")
                .control(node_override)
                .build(shared),
        )
        .row(
            SettingRow::new("Cluster default")
                .help_from(text("settings module desired help", |module| {
                    module_desired_presentation(module.desired_enabled)
                        .1
                        .to_string()
                }))
                .control(status_badge_from(
                    reader,
                    text("settings module desired label", |module| {
                        module_desired_presentation(module.desired_enabled)
                            .0
                            .to_string()
                    }),
                    tone("settings module desired tone", |module| {
                        enabled_tone(module.desired_enabled == Some(true))
                    }),
                ))
                .build(shared),
        )
        .row(
            SettingRow::new(format!("On {}", current.node_label))
                .help_from(text("settings module effective source", |module| {
                    module_effective_source_label(module.effective_source).to_string()
                }))
                .control(status_badge_from(
                    reader,
                    text("settings module effective label", |module| {
                        if module.effective_enabled {
                            "Enabled"
                        } else {
                            "Disabled"
                        }
                        .to_string()
                    }),
                    tone("settings module effective tone", |module| {
                        enabled_tone(module.effective_enabled)
                    }),
                ))
                .build(shared),
        )
        .row(
            SettingRow::new(format!("Running on {}", current.node_label))
                .help_from(text("settings module observed detail", |module| {
                    module.observed_detail.clone()
                }))
                .control(status_badge_from(
                    reader,
                    text("settings module observed label", |module| {
                        module_observed_label(module.observed).to_string()
                    }),
                    tone("settings module observed tone", |module| {
                        module_observed_tone(module.observed)
                    }),
                ))
                .build(shared),
        )
        .row(revealed(
            SettingRow::new("Reconciliation")
                .help("The node has not caught up with its desired state yet.")
                .status(reader, "Pending reconciliation", SemanticTone::Warning)
                .build(shared),
            derived(
                summary,
                "settings module reconciling",
                |module: &SettingsModuleSummaryStructure| {
                    matches!(
                        (module.effective_enabled, module.observed),
                        (true, ModuleObservedState::Stopped)
                            | (false, ModuleObservedState::Running)
                    )
                },
            ),
        ))
        .row(revealed(
            SettingRow::new("Node error")
                .value_from(shared, node_error.clone())
                .build(shared),
            derived(
                node_error,
                "settings module has node error",
                |error: &String| !error.is_empty(),
            ),
        ))
        .row(settings)
        .row(details(
            shared,
            reader,
            &format!("module-{module_id}"),
            "module ID and backend",
            vec![
                widgets::Boxed::new(
                    SettingRow::new("Technical ID")
                        .control(copy_control(
                            reader,
                            "module-technical-id",
                            "Technical ID",
                            module.module_id.clone(),
                        ))
                        .build(shared),
                ),
                widgets::Boxed::new(
                    SettingRow::new("Backend")
                        .value(shared, format!("{} · {}", module.backend, module.detail))
                        .build(shared),
                ),
            ],
        ))
        .build(shared, reader)
}

fn module_desired_presentation(desired: Option<bool>) -> (&'static str, &'static str) {
    match desired {
        Some(true) => ("Enabled", "Every node runs it unless overridden."),
        Some(false) => ("Disabled", "No node runs it unless overridden."),
        None => ("Not configured", "No cluster default is set; it stays off."),
    }
}

fn enabled_tone(enabled: bool) -> SemanticTone {
    if enabled {
        SemanticTone::Success
    } else {
        SemanticTone::Neutral
    }
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
    let mut body = Stack::vertical()
        .gap(8.0)
        .alignment(Alignment::Stretch)
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)));
    if !item.description.trim().is_empty() {
        body = body.with_child(muted_label_with_token(
            shared,
            short_one_line(&item.description, 160),
            TextSize::Sm,
        ));
    }
    if let Some((error, resolution)) = module_operational_error(item) {
        body = body
            .with_child(
                Stack::horizontal().with_child(
                    StatusBadge::new("Action required")
                        .icon(IconGlyph::Alert)
                        .tone(SemanticTone::Danger)
                        .theme_when(clone_reader(reader)),
                ),
            )
            .with_child(selectable_read_only_area(
                reader,
                &format!("module-operational-error-{index}"),
                error,
            ))
            .with_child(muted_meta_label(shared, "How to resolve"))
            .with_child(selectable_read_only_area(
                reader,
                &format!("module-operational-resolution-{index}"),
                resolution,
            ));
    }
    let key = item.key.trim_start_matches('/').to_string();
    body = body.with_child(PodHost::new(editor)).with_child(details(
        shared,
        reader,
        &format!("module-setting-{key}"),
        "setting key",
        vec![
            widgets::Boxed::new(
                SettingRow::new("Setting key")
                    .value(shared, key.clone())
                    .build(shared),
            ),
            widgets::Boxed::new(
                SettingRow::new("Definition")
                    .value(shared, config_metadata_line(item))
                    .build(shared),
            ),
        ],
    ));
    WidgetPod::new(SemanticRegion::new(format!("Settings · {key}"), body))
}

fn module_operational_error(item: &ConfigItem) -> Option<(String, String)> {
    let presentation = item.operational_error.as_ref()?;
    let object = item.raw_value.as_object()?;
    let error = object.get(&presentation.error_field)?.as_str()?.trim();
    if error.is_empty() {
        return None;
    }
    let resolution = object
        .get(&presentation.resolution_field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Resolve the reported module error and refresh this page.");
    Some((error.to_string(), resolution.to_string()))
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

fn remote_management_observed_label(state: ModuleObservedState) -> &'static str {
    match state {
        ModuleObservedState::Running => "Active",
        ModuleObservedState::Stopped => "Inactive",
        ModuleObservedState::Installed => "Installed, not active",
        ModuleObservedState::NotReported => "Not reported",
        ModuleObservedState::Unavailable => "Unavailable",
    }
}

fn remote_management_module_attention(
    snapshot: &RemoteManagementSettingsSnapshot,
) -> Option<(&'static str, &'static str)> {
    match (snapshot.module_effective_enabled, snapshot.module_observed) {
        (_, ModuleObservedState::Unavailable | ModuleObservedState::NotReported) => Some((
            "Runtime state unknown",
            "The desired node setting is known, but loader activation cannot currently be verified.",
        )),
        (true, ModuleObservedState::Installed) => Some((
            "Activation required",
            "The module artifact is installed, but the loader has not activated it.",
        )),
        (true, ModuleObservedState::Stopped) => Some((
            "Desired enabled · observed inactive",
            "The node override is desired state; the loader currently reports the module inactive.",
        )),
        (false, ModuleObservedState::Running) => Some((
            "Desired disabled · observed active",
            "The loader remains active even though desired state is disabled.",
        )),
        _ => None,
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

fn module_display_id(module_id: &str) -> String {
    short_one_line(module_id, 72)
}

fn section_skills(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    Stack::vertical()
        .gap(14.0)
        .alignment(Alignment::Stretch)
        .with_child(skills_summary_section(shared, reader))
        .with_child(skills_catalog(shared, reader))
        .with_child(skill_detail_host(shared, reader))
}

fn skills_summary_section(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let import_path = shared.settings().skill_import_path.clone();
    let idle = settings_projection(shared, "settings skills idle", |snapshot| {
        !snapshot.skills.pending
    });
    let import_input = {
        let s = shared.clone();
        TextInput::new("skill-import-path")
            .value(import_path)
            .placeholder("/path/to/skill-name")
            .theme_when(clone_reader(reader))
            .on_change(move |value| s.settings_mut().set_skill_import_path(value))
    };
    let import = {
        let s = shared.clone();
        sui::Button::new("Import skill")
            .icon(sui::IconGlyph::Add)
            .primary_action()
            .theme_when(clone_reader(reader))
            .enabled_from(idle.clone())
            .on_press(move || {
                let path = s.settings().skill_import_path.trim().to_string();
                if path.is_empty() {
                    s.settings_mut().skill_action_error =
                        Some("enter an Agent Skills package directory".to_string());
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
            .enabled_from(idle)
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
    let show_all = {
        let s = shared.clone();
        setting_switch_from(
            reader,
            "Show all versions",
            settings_projection(shared, "settings skills include all", |snapshot| {
                snapshot.skills.include_all
            }),
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
            },
        )
    };
    let catalog_summary =
        settings_projection(shared, "settings skills catalog summary", |snapshot| {
            skills_sidebar_preview(&snapshot.skills.skills)
        });

    SettingsCard::new("Skills")
        .description(
            "Agent Skills packages that agents can load. Each package is a folder with a SKILL.md.",
        )
        .header_action(refresh)
        .row(
            SettingRow::new("Catalog")
                .value_from(shared, catalog_summary)
                .build(shared),
        )
        .row(
            SettingRow::new("Import a skill")
                .help("Copies the package folder into the skill catalog.")
                .control(
                    Flex::horizontal()
                        .gap(8.0)
                        .align_items(Alignment::Center)
                        .with_item(import_input, FlexItem::new().shrink(1.0))
                        .with_item(import, FlexItem::new().no_shrink()),
                )
                .build(shared),
        )
        .row(
            SettingRow::new("Show all versions")
                .help("Include older and inactive versions in the catalog.")
                .control(show_all)
                .build(shared),
        )
        .row(feedback_slot(shared, reader, |snapshot| {
            snapshot.feedback.skill.clone()
        }))
        .build(shared, reader)
}

/// The catalog list, or a prompt to import the first skill; both stay mounted.
fn skills_catalog(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let present = settings_projection(shared, "settings skills catalog present", |snapshot| {
        usize::from(!snapshot.skills.skills.is_empty())
    });
    SwitchView::new()
        .with_child(
            SettingsCard::new("No skills yet")
                .description(
                    "Import a folder containing SKILL.md above to make it available to agents.",
                )
                .build(shared, reader),
        )
        .with_child(skill_virtual_catalog(shared, reader))
        .selected_from(present)
}

fn skill_virtual_catalog(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let collection = shared.signals().settings_structure.select_named(
        "settings virtual skill collection",
        |key| {
            let Some(snapshot) = key.0.as_deref() else {
                return widgets::VirtualCollectionSnapshot::new(Vec::new(), None);
            };
            widgets::VirtualCollectionSnapshot::new(
                snapshot
                    .skills
                    .skills
                    .iter()
                    .map(|skill| ((skill.id.clone(), skill.version.clone()), skill.clone()))
                    .collect(),
                snapshot.skills.selected.clone(),
            )
        },
    );
    let row_shared = shared.clone();
    let row_reader = Rc::clone(reader);
    let action_shared = shared.clone();
    let list = widgets::VirtualCollectionHost::new(
        "Settings skill collection",
        collection,
        move |model, state| {
            let row_reader = Rc::clone(&row_reader);
            VirtualList::new("skill-catalog", model, move |_key, skill| {
                let shared = row_shared.clone();
                let reader = Rc::clone(&row_reader);
                // One row's summary text; the list and its scroll position stay mounted.
                RebuildOnChange::key_from(skill, move |skill| {
                    WidgetPod::new(padded_virtual_list_row(
                        &reader,
                        skill_catalog_row(&shared, skill),
                    ))
                })
            })
            .state(state)
            .estimated_row_height(MENU_ROW_H)
            .theme_when(clone_reader(reader))
            .row_name(|_key, skill| skill_title(skill))
            .row_description(|_key, skill| {
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
    SettingsCard::new("Catalog")
        .description("Select a skill version to inspect, activate, or export.")
        .row(list)
        .build(shared, reader)
}

/// The selected skill. It is rebuilt when another version is selected or its content changes;
/// activation state, the SKILL.md disclosure, and the export field update in place.
fn skill_detail_host(
    shared: &Shared,
    reader: &ThemeReader,
) -> RebuildOnChange<(Option<(String, String)>, Option<String>, bool)> {
    let source = settings_projection(shared, "settings skill detail", |snapshot| {
        (
            snapshot.skills.selected.clone(),
            snapshot.skills.detail_content_hash.clone(),
            snapshot.skills.skills.is_empty(),
        )
    });
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::key_from(source, move |_key| {
        let (detail, export_path) = {
            let settings = s.settings();
            (
                settings.skill_detail.clone(),
                settings.skill_export_path.clone(),
            )
        };
        detail
            .map(|detail| WidgetPod::new(skill_detail_section(&s, &r, detail, export_path)))
            .unwrap_or_else(|| {
                // With an empty catalog there is nothing to select; say so only once.
                if s.settings().skills.is_empty() {
                    WidgetPod::new(SizedBox::new())
                } else {
                    WidgetPod::new(
                        SettingsCard::new("Selected skill")
                            .description("Select a skill in the catalog to see its details.")
                            .build(&s, &r),
                    )
                }
            })
    })
}

fn skill_detail_section(
    shared: &Shared,
    reader: &ThemeReader,
    detail: SkillDetail,
    export_path: String,
) -> impl Widget + use<> {
    let summary = detail.summary.clone();
    // Activation is reported with the catalog, which refreshes without reselecting the skill.
    let state = {
        let id = summary.id.clone();
        let version = summary.version.clone();
        let fallback = (summary.active, summary.status.clone());
        settings_projection(shared, "settings skill state", move |snapshot| {
            snapshot
                .skills
                .skills
                .iter()
                .find(|skill| skill.id == id && skill.version == version)
                .map(|skill| (skill.active, skill.status.clone()))
                .unwrap_or_else(|| fallback.clone())
        })
    };
    let state_label = derived(
        state.clone(),
        "settings skill state label",
        |(active, status)| {
            if *active {
                "Active".to_string()
            } else {
                status.clone()
            }
        },
    );
    let state_tone = derived(state.clone(), "settings skill state tone", |(active, _)| {
        if *active {
            SemanticTone::Success
        } else {
            SemanticTone::Neutral
        }
    });
    let can_activate = sui::combine_named(
        "settings skill can activate",
        (
            state,
            settings_projection(shared, "settings skills busy", |snapshot| {
                snapshot.skills.pending
            }),
        ),
        |((active, _), pending)| !active && !pending,
    );
    let mut card = SettingsCard::new(skill_title(&summary))
        .description(short_one_line(&summary.description, 160))
        .header_action(status_badge_from(reader, state_label, state_tone))
        .row(
            SettingRow::new("Source")
                .value(shared, skill_source_label(&summary))
                .build(shared),
        )
        .row(
            SettingRow::new("Version")
                .value(shared, summary.version.clone())
                .build(shared),
        );
    let optional_rows = [
        ("License", detail.license.clone()),
        ("Compatibility", detail.compatibility.clone()),
        (
            "Tags",
            (!detail.activation_tags.is_empty()).then(|| detail.activation_tags.join(", ")),
        ),
        (
            "Capabilities",
            (!detail.required_capabilities.is_empty())
                .then(|| detail.required_capabilities.join(", ")),
        ),
    ];
    for (label, value) in optional_rows {
        if let Some(value) = value {
            card = card.row(SettingRow::new(label).value(shared, value).build(shared));
        }
    }
    let mut technical = vec![
        widgets::Boxed::new(
            SettingRow::new("SIFS path")
                .value(shared, short_one_line(&summary.sifs_path, 96))
                .build(shared),
        ),
        widgets::Boxed::new(
            SettingRow::new("Content hash")
                .value(shared, short(&summary.content_hash, 48))
                .build(shared),
        ),
    ];
    if let Some(base_hash) = detail.base_hash.as_ref() {
        technical.push(widgets::Boxed::new(
            SettingRow::new("Base hash")
                .value(shared, short(base_hash, 48))
                .build(shared),
        ));
    }
    if !detail.resources.is_empty() {
        technical.push(widgets::Boxed::new(
            SettingRow::new("Resources")
                .value(shared, detail.resources.join(", "))
                .build(shared),
        ));
    }
    let export_input = {
        let s = shared.clone();
        TextInput::new("skill-export-path")
            .value(export_path)
            .placeholder("/path/to/skill-name")
            .theme_when(clone_reader(reader))
            .on_change(move |value| s.settings_mut().set_skill_export_path(value))
    };

    let activate = {
        let s = shared.clone();
        sui::Button::new("Activate")
            .icon(sui::IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled_from(can_activate)
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
            .enabled_from(settings_projection(
                shared,
                "settings skills export idle",
                |snapshot| !snapshot.skills.pending,
            ))
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
    let text_expanded = settings_projection(shared, "settings skill text expanded", |snapshot| {
        snapshot.skills.text_expanded
    });
    let text = {
        let s = shared.clone();
        disclosure_toggle(
            reader,
            text_expanded.clone(),
            "Show SKILL.md",
            "Hide SKILL.md",
            move || s.settings_mut().toggle_skill_text(),
        )
    };
    card = card
        .row(
            SettingRow::new("Use this version")
                .help("Agents load the active version of each skill.")
                .control(activate)
                .build(shared),
        )
        .row(
            SettingRow::new("Export to")
                .help("Writes this version as a package folder.")
                .control(
                    Flex::horizontal()
                        .gap(8.0)
                        .align_items(Alignment::Center)
                        .with_item(export_input, FlexItem::new().shrink(1.0))
                        .with_item(export, FlexItem::new().no_shrink()),
                )
                .build(shared),
        )
        .row(Align::new(Alignment::Start, Alignment::Center, text))
        .row(revealed(
            SizedBox::new().height(220.0).child(
                TextArea::new("skill-markdown")
                    .value(detail.text)
                    .read_only(true)
                    .theme_when(clone_reader(reader)),
            ),
            text_expanded,
        ));
    card.row(details(
        shared,
        reader,
        "skill-technical",
        "package details",
        technical,
    ))
    .build(shared, reader)
}

fn configuration_controls(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    // Clear filters resets both controls in place: they adopt the model when its controls
    // revision changes, so the search field keeps focus while typing.
    let revision = settings_projection(
        shared,
        "settings configuration controls revision",
        |snapshot| snapshot.configuration.controls_revision,
    );
    let search = {
        let query = shared.settings().advanced_query.clone();
        let s = shared.clone();
        let input = TextInput::new("advanced-config-search")
            .value(query)
            .placeholder("Search keys, owners, and descriptions")
            .leading_icon(IconGlyph::Search)
            .theme_when(clone_reader(reader))
            .on_change(move |query| s.settings_mut().set_advanced_query(query));
        let s = shared.clone();
        SyncedTextInput::new(input, move || s.settings().advanced_query.clone())
            .observing(revision.clone())
    };
    let overrides = {
        let on = shared.settings().advanced_overrides_only;
        let s = shared.clone();
        let checkbox = Checkbox::new("Show only local or cluster overrides")
            .checked(on)
            .theme_when(clone_reader(reader))
            .on_change(move |on| s.settings_mut().set_advanced_overrides_only(on));
        let s = shared.clone();
        SyncedCheckbox::new(checkbox, move || s.settings().advanced_overrides_only)
            .observing(revision)
    };
    SettingsCard::new("Browse configuration").description("Search catalog metadata without exposing resolved values. Overrides are settings explicitly stored on this device or in the cluster.",).row(FieldGroup::new()
            .gap(8.0)
            .with_child(
                FormRow::new("Search", search)
                    .stacked()
                    .theme_when(clone_reader(reader)),
            )
            .with_child(
                FormRow::new("Filter", overrides).theme_when(clone_reader(reader)),
            )).build(shared, reader)
}

fn section_configuration(
    shared: &Shared,
    reader: &ThemeReader,
    node_scoped: bool,
) -> impl Widget + use<> {
    Stack::vertical()
        .gap(14.0)
        .alignment(Alignment::Stretch)
        .with_child(configuration_controls(shared, reader))
        .with_child(configuration_results(shared, reader, node_scoped))
}

/// One owner group of visible settings in a configuration browser.
#[derive(Clone, Debug, PartialEq)]
struct ConfigGroupStructure {
    owner: String,
    rows: Vec<ConfigRowStructure>,
}

/// One visible setting. `index` addresses the catalog item that saves and toggles act on.
#[derive(Clone, Debug, PartialEq)]
struct ConfigRowStructure {
    index: usize,
    item: ConfigItemStructure,
    expanded: bool,
    revision: u64,
}

impl ConfigRowStructure {
    /// Booleans that can be written directly are a switch; everything else has an editor.
    fn is_toggle(&self) -> bool {
        self.item.bool_value.is_some()
            && self.item.edit_kind == ConfigEditKind::Bool
            && !self.item.read_only
    }
}

fn config_scope_groups(
    configuration: &SettingsConfigurationStructure,
    node_scoped: bool,
) -> Vec<ConfigGroupStructure> {
    configuration
        .groups
        .iter()
        .filter_map(|(owner, indices)| {
            let rows = indices
                .iter()
                .filter_map(|index| {
                    let item = configuration.items.get(*index)?;
                    (item.node_scoped == node_scoped).then(|| ConfigRowStructure {
                        index: *index,
                        item: item.clone(),
                        expanded: configuration.expanded.contains(&item.key),
                        revision: configuration.ui_revision,
                    })
                })
                .collect::<Vec<_>>();
            (!rows.is_empty()).then(|| ConfigGroupStructure {
                owner: owner.clone(),
                rows,
            })
        })
        .collect()
}

/// The browser's results. Searching and filtering add and remove keyed rows; existing rows, their
/// open editors, and the cards around them stay mounted.
fn configuration_results(
    shared: &Shared,
    reader: &ThemeReader,
    node_scoped: bool,
) -> impl Widget + use<> {
    let total = settings_projection(shared, "settings configuration total", move |snapshot| {
        snapshot
            .configuration
            .items
            .iter()
            .filter(|item| item.node_scoped == node_scoped)
            .count()
    });
    let groups = settings_projection(shared, "settings configuration groups", move |snapshot| {
        config_scope_groups(&snapshot.configuration, node_scoped)
    });
    let visible = derived(
        groups.clone(),
        "settings configuration visible count",
        |groups: &Vec<ConfigGroupStructure>| {
            groups.iter().map(|group| group.rows.len()).sum::<usize>()
        },
    );
    let state = settings_projection(shared, "settings configuration state", move |snapshot| {
        let configuration = &snapshot.configuration;
        if configuration.error.is_some() {
            0
        } else if !configuration
            .items
            .iter()
            .any(|item| item.node_scoped == node_scoped)
        {
            1
        } else if config_scope_groups(configuration, node_scoped).is_empty() {
            2
        } else {
            3
        }
    });

    let error = settings_projection(shared, "settings configuration error", |snapshot| {
        snapshot.configuration.error.clone().unwrap_or_default()
    });
    let unavailable = SettingsCard::new("Configuration unavailable")
        .description("Sinomo could not load the resolved configuration catalog.")
        .row(value_row_from(shared, reader, "Error", error))
        .build(shared, reader);
    let empty = SettingsCard::new("Configuration values")
        .description("The active runtime did not publish any configurable settings.")
        .row(muted_label(shared, "No configuration keys are available."))
        .build(shared, reader);

    let message = settings_projection(shared, "settings configuration empty message", |snapshot| {
        let configuration = &snapshot.configuration;
        if configuration.overrides_only && configuration.query_empty {
            "No local or cluster overrides are currently stored."
        } else if configuration.overrides_only {
            "No stored overrides match this search."
        } else {
            "No configuration metadata matches this search."
        }
        .to_string()
    });
    let none_shown = derived(
        total.clone(),
        "settings configuration none shown",
        |total: &usize| format!("0 of {total} configuration values shown"),
    );
    let clear = {
        let s = shared.clone();
        sui::Button::new("Clear filters")
            .icon(IconGlyph::Close)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || {
                s.settings_mut().clear_advanced_filters();
            })
    };
    let no_matches = SettingsCard::new("No matching settings")
        .description_from(none_shown)
        .row(
            Stack::vertical()
                .gap(8.0)
                .alignment(Alignment::Stretch)
                .with_child(muted_text(
                    shared,
                    Label::new(message.get()).text_from(message),
                    TextSize::Base,
                ))
                .with_child(Stack::horizontal().with_child(clear)),
        )
        .build(shared, reader);

    let showing = sui::combine_named(
        "settings configuration showing",
        (visible, total),
        |(visible, total)| format!("{visible} of {total}"),
    );
    let summary = SettingsCard::new("Configuration values")
        .description(
            "Resolved values and drafts remain redacted where the catalog marks data as sensitive.",
        )
        .row(value_row_from(shared, reader, "Showing", showing))
        .build(shared, reader);
    let group_cards = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        KeyedStack::vertical(
            groups,
            |group: &ConfigGroupStructure| group.owner.clone(),
            move |owner, group| config_group_card(&s, &r, owner, group),
        )
        .gap(14.0)
    };
    let list = Stack::vertical()
        .gap(14.0)
        .alignment(Alignment::Stretch)
        .with_child(summary)
        .with_child(group_cards);

    SwitchView::new()
        .with_child(unavailable)
        .with_child(empty)
        .with_child(no_matches)
        .with_child(list)
        .selected_from(state)
}

fn config_group_card(
    shared: &Shared,
    reader: &ThemeReader,
    owner: &str,
    group: Signal<ConfigGroupStructure>,
) -> impl Widget + use<> {
    let count = group.select_named("settings configuration group count", |group| {
        if group.rows.len() == 1 {
            "1 setting".to_string()
        } else {
            format!("{} settings", group.rows.len())
        }
    });
    let rows = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        KeyedStack::vertical(
            group.select_named("settings configuration group rows", |group| {
                group.rows.clone()
            }),
            |row: &ConfigRowStructure| (row.item.key.clone(), row.index, row.is_toggle()),
            move |(_, index, _), row| config_row(&s, &r, *index, row),
        )
        .gap(8.0)
    };
    SettingsCard::new(owner)
        .description_from(count)
        .row(rows)
        .build(shared, reader)
}

/// One Advanced config row: a toggle for booleans, otherwise a redacted summary with an explicit
/// raw-value disclosure. Sensitive raw values remain read-only and redacted in Advanced.
fn config_row(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    row: Signal<ConfigRowStructure>,
) -> impl Widget + use<> {
    let Some(item) = shared.settings().config_items.get(index).cloned() else {
        return widgets::Boxed::new(SizedBox::new());
    };
    let key = item.key.clone();
    let display_key = key.trim_start_matches('/').to_string();
    let item_changes = row.select_named("settings configuration row item", |row| row.item.clone());

    if row.get().is_toggle() {
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
        let checked = row.select_named("settings configuration row value", |row| {
            row.item.bool_value.unwrap_or(false)
        });
        let switch = synced_switch(
            Switch::new("Applies immediately")
                .checked(checked.get())
                .theme_when(clone_reader(reader))
                .on_change(on_toggle),
            checked,
        );
        let metadata = row.select_named("settings configuration row metadata", |row| {
            config_structure_metadata_line(&row.item)
        });
        let description = row.select_named("settings configuration row description", |row| {
            short_one_line(&row.item.description, 128)
        });
        let has_description = derived(
            description.clone(),
            "settings configuration row has description",
            |description: &String| !description.trim().is_empty(),
        );
        let details = Column::new(reader, |theme| theme.metrics.property_row_stacked_gap)
            .with_child(switch)
            .with_child(muted_text(
                shared,
                Label::new(metadata.get()).text_from(metadata),
                TextSize::Xs,
            ))
            .with_child(revealed(
                muted_text(
                    shared,
                    Label::new(description.get()).text_from(description),
                    TextSize::Sm,
                ),
                has_description,
            ));
        return widgets::Boxed::new(
            FormRow::new(display_key, details)
                .stacked()
                .theme_when(clone_reader(reader)),
        );
    }

    let sensitive = config_item_has_sensitive_detail(&item);
    let row_key = config_field_key(&key);
    let summary_label = {
        let s = shared.clone();
        follow(
            settings_label(shared, config_value_summary(&item), TextSize::Sm)
                .text_when(move || {
                    s.settings()
                        .config_items
                        .get(index)
                        .map(config_value_summary)
                        .unwrap_or_default()
                })
                .color_when({
                    let s = shared.clone();
                    move || palette(&s).text_muted
                }),
            item_changes.clone(),
        )
    };
    let meta_label = {
        let s = shared.clone();
        let metadata = row.select_named("settings configuration row metadata", |row| {
            config_structure_metadata_line(&row.item)
        });
        let fallback = metadata.clone();
        follow(
            settings_label(shared, metadata.get(), TextSize::Xs)
                .text_when({
                    let s = s.clone();
                    let row_key = row_key.clone();
                    move || {
                        s.settings()
                            .field_errors
                            .get(&row_key)
                            .cloned()
                            .unwrap_or_else(|| fallback.get())
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
                }),
            metadata,
        )
    };
    let description = short_one_line(&item.description, 128);
    let expanded = row.select_named("settings configuration row expanded", |row| row.expanded);
    let disclosure = {
        let s = shared.clone();
        let key = key.clone();
        let closed_label = if sensitive {
            "Show redacted detail"
        } else if item.schema.is_some() {
            "Edit fields"
        } else {
            "Edit raw value"
        };
        disclosure_toggle(
            reader,
            expanded.clone(),
            closed_label,
            "Hide value",
            move || s.settings_mut().toggle_advanced_detail(&key),
        )
    };
    // The editor is built while open and rebuilt only for structural draft changes (a list or map
    // entry added or removed, an optional field unset, Reset) or a new saved value.
    let editor = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        let source = row.select_named("settings configuration row editor", |row| {
            row.expanded.then(|| (row.revision, row.item.clone()))
        });
        RebuildOnChange::key_from(source, move |open| {
            let item = s.settings().config_items.get(index).cloned();
            match (open, item) {
                (Some(_), Some(item)) => match &item.schema {
                    Some(schema) => {
                        WidgetPod::new(config_schema_editor(&s, &r, index, &item, schema))
                    }
                    None => WidgetPod::new(config_detail_editor(&s, &r, index, &item)),
                },
                _ => WidgetPod::new(SizedBox::new()),
            }
        })
    };
    let mut details = Column::new(reader, |theme| theme.metrics.property_row_stacked_gap)
        .with_child(summary_label)
        .with_child(meta_label);
    if !description.is_empty() {
        details = details.with_child(muted_meta_label(shared, description));
    }
    details = details
        .with_child(Stack::horizontal().with_child(disclosure))
        .with_child(revealed(editor, expanded));
    widgets::Boxed::new(
        FormRow::new(display_key, details)
            .stacked()
            .theme_when(clone_reader(reader)),
    )
}

fn visible_config_groups(settings: &SettingsState) -> Vec<(String, Vec<usize>)> {
    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for (index, item) in settings.config_items.iter().enumerate() {
        if settings.advanced_overrides_only && !config_item_is_override_or_dirty(settings, item) {
            continue;
        }
        if !config_item_matches_query(item, &settings.advanced_query) {
            continue;
        }
        groups
            .entry(config_owner_label(&item.owner))
            .or_default()
            .push(index);
    }
    groups.into_iter().collect()
}

fn config_item_is_override_or_dirty(settings: &SettingsState, item: &ConfigItem) -> bool {
    if matches!(item.source, "local" | "cluster") || settings.config_schema_is_dirty(&item.key) {
        return true;
    }
    let raw_key = config_field_key(&item.key);
    let schema_prefix = config_schema_field_prefix(&item.key);
    settings.field_drafts.contains_key(&raw_key)
        || settings
            .field_drafts
            .keys()
            .any(|key| key.starts_with(&schema_prefix))
}

fn config_item_matches_query(item: &ConfigItem, query: &str) -> bool {
    let terms = query
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>();
    if terms.is_empty() {
        return true;
    }
    let mut metadata = format!(
        "{} {} {} {} {}",
        item.key, item.owner, item.description, item.source, item.scope
    );
    if let Some(module_id) = &item.module_id {
        metadata.push(' ');
        metadata.push_str(module_id);
    }
    if let Some(schema) = &item.schema {
        metadata.push(' ');
        metadata.push_str(&schema.name);
        metadata.push(' ');
        metadata.push_str(&schema.description);
        push_config_schema_search_metadata(&schema.value, &mut metadata);
    }
    let metadata = metadata.to_lowercase();
    terms.iter().all(|term| metadata.contains(term))
}

fn push_config_schema_search_metadata(schema: &ConfigSchemaValue, metadata: &mut String) {
    match schema {
        ConfigSchemaValue::Object { fields, .. } => {
            for field in fields {
                metadata.push(' ');
                metadata.push_str(&field.name);
                metadata.push(' ');
                metadata.push_str(&field.label);
                metadata.push(' ');
                metadata.push_str(&field.description);
                push_config_schema_search_metadata(&field.value, metadata);
            }
        }
        ConfigSchemaValue::Map { value } | ConfigSchemaValue::List { value } => {
            push_config_schema_search_metadata(value, metadata);
        }
        ConfigSchemaValue::String { choices, .. } => {
            for choice in choices {
                metadata.push(' ');
                metadata.push_str(&choice.value);
                metadata.push(' ');
                metadata.push_str(&choice.label);
                metadata.push(' ');
                metadata.push_str(&choice.description);
            }
        }
        ConfigSchemaValue::Bool
        | ConfigSchemaValue::I64 { .. }
        | ConfigSchemaValue::U64 { .. }
        | ConfigSchemaValue::F64 { .. }
        | ConfigSchemaValue::StringList
        | ConfigSchemaValue::Json => {}
    }
}

fn config_owner_label(owner: &str) -> String {
    match owner.trim() {
        "sinomo-lm" => "Language models".to_string(),
        "sinomo-dfs" => "Files and storage".to_string(),
        "sinomo-module" => "Modules".to_string(),
        "sinomo-sandbox" => "Sandbox and security".to_string(),
        "sinomo-rollout" => "Rollout".to_string(),
        "sinomo-evolution" => "Evolution".to_string(),
        "" => "System".to_string(),
        other => other
            .trim_start_matches("sinomo-")
            .split(['-', '_'])
            .filter(|part| !part.is_empty())
            .enumerate()
            .map(|(index, part)| {
                if index == 0 {
                    let mut chars = part.chars();
                    chars
                        .next()
                        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                        .unwrap_or_default()
                } else {
                    part.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
    }
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
        editor = editor.read_only(true);
    }
    let mut stack = Stack::vertical()
        .gap(5.0)
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
        .with_child(SizedBox::new().height(112.0).child(editor));
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
    let mut stack = Column::new(reader, |theme| theme.metrics.field_group_spacing);
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
                .primary_action()
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
        stack = stack.with_child(Align::new(
            Alignment::End,
            Alignment::Center,
            Flex::horizontal()
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(reset)
                .with_child(save),
        ));
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
    settings_label(shared, fallback.clone(), TextSize::Sm)
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
                palette.bad_text
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
    let mut group = FieldGroup::new().theme_when(clone_reader(reader));
    if !description.trim().is_empty() {
        group = group.with_child(SettingRow::new(label).help(description).build(shared));
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
            let mut row = SettingRow::new(field_label.clone())
                .help(field.description.clone())
                .help_widget(config_schema_feedback_label(
                    shared,
                    error_key.clone().unwrap_or_default(),
                    "Not set".to_string(),
                ));
            if !read_only {
                let s = shared.clone();
                let set_path = field_path.clone();
                let initial_value = if matches!(&field.value, ConfigSchemaValue::Json) {
                    Value::Object(serde_json::Map::new())
                } else {
                    schema_default_value(&field.value)
                };
                let field_label = field.label.clone();
                row = row.control(
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
                );
            }
            group = group.with_child(row.build(shared));
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
            group = group.with_child(Align::new(
                Alignment::End,
                Alignment::Center,
                sui::Button::new(format!("Unset {}", field.label))
                    .icon(IconGlyph::Close)
                    .theme_when(clone_reader(reader))
                    .appearance(sui::ButtonAppearance::Ghost)
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
            ));
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
    let mut group = FieldGroup::new().gap(8.0);
    let entries = value
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut summary_body = Stack::vertical()
        .gap(4.0)
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
                .gap(4.0)
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
        .gap(4.0)
        .alignment(Alignment::Stretch)
        .with_child(muted_meta_label(shared, format!("{} items", items.len())));
    if !description.trim().is_empty() {
        summary = summary.with_child(muted_meta_label(shared, description.to_string()));
    }
    let mut group = FieldGroup::new().gap(8.0).with_child(
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
        return WidgetPod::new(
            SettingRow::new(label)
                .help(description)
                .value(shared, config_schema_inline_summary(value, schema))
                .build(shared),
        );
    }
    match schema {
        ConfigSchemaValue::Bool => {
            let current = value.as_bool().unwrap_or(false);
            let switch = setting_switch(reader, label, current, {
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
            WidgetPod::new(
                SettingRow::new(label)
                    .help(description)
                    .control(switch)
                    .build(shared),
            )
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
            WidgetPod::new(
                SettingRow::new(label)
                    .help(description)
                    .control(select)
                    .build(shared),
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
            let input = PasswordInput::new(schema_control_name(index, &path))
                .value(replacement)
                .placeholder(if configured {
                    "Enter a replacement"
                } else {
                    "Enter a secret"
                })
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
            let status = if configured {
                "Set. The current secret is never shown; enter a replacement to change it."
            } else {
                "Not set."
            };
            let help = if description.trim().is_empty() {
                status.to_string()
            } else {
                format!("{description} {status}")
            };
            WidgetPod::new(
                SettingRow::new(label)
                    .help(help)
                    .control(input)
                    .build(shared),
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
            let fallback = schema_field_detail(schema, description);
            let row = SettingRow::new(label).control(input);
            let row = match error_key {
                // Validation feedback replaces the help while the field has an error.
                Some(error_key) => {
                    row.help_widget(config_schema_feedback_label(shared, error_key, fallback))
                }
                None => row.help(fallback),
            };
            WidgetPod::new(row.build(shared))
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
            WidgetPod::new(
                SettingRow::new(label)
                    .help(schema_field_detail(schema, description))
                    .control(number)
                    .build(shared),
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
            let fallback = schema_field_detail(schema, description);
            let row = SettingRow::new(label).control(input);
            let row = match error_key {
                // Validation feedback replaces the help while the field has an error.
                Some(error_key) => {
                    row.help_widget(config_schema_feedback_label(shared, error_key, fallback))
                }
                None => row.help(fallback),
            };
            WidgetPod::new(row.build(shared))
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
        .gap(4.0)
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
        .with_child(SizedBox::new().height(92.0).child(editor));
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
    config_metadata_text(
        item.edit_kind,
        &item.owner,
        item.schema.as_ref(),
        item.source,
        item.scope,
    )
}

fn config_structure_metadata_line(item: &ConfigItemStructure) -> String {
    config_metadata_text(
        item.edit_kind,
        &item.owner,
        item.schema.as_ref(),
        item.source,
        item.scope,
    )
}

fn config_metadata_text(
    edit_kind: ConfigEditKind,
    owner: &str,
    schema: Option<&ConfigSchema>,
    source: &str,
    scope: &str,
) -> String {
    let kind = match edit_kind {
        ConfigEditKind::Bool => "Boolean",
        ConfigEditKind::Text => "Text",
        ConfigEditKind::Number => "Number",
        ConfigEditKind::Json => "JSON",
    };
    let owner = owner.trim();
    let schema = schema.map(|schema| schema.name.as_str()).unwrap_or(kind);
    if owner.is_empty() {
        format!("{source} · {scope} · {schema}")
    } else {
        format!("{source} · {scope} · {owner} · {schema}")
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
    if item.key.trim_matches('/') == "lm.providers"
        && let Some(accounts) = value.get("accounts").and_then(Value::as_array)
    {
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
    scroll_states: &[ScrollState; SettingsSection::ALL.len()],
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
    let mut pages = SwitchView::new();
    // Children follow `SettingsSection::ALL`, which `index()` selects by.
    for section in SettingsSection::ALL {
        let body = match section {
            SettingsSection::Appearance => widgets::Boxed::new(section_appearance(shared, reader)),
            SettingsSection::System => widgets::Boxed::new(section_system(shared, reader)),
            SettingsSection::Providers => widgets::Boxed::new(section_providers(shared, reader)),
            SettingsSection::Agents => widgets::Boxed::new(section_agents(shared, reader)),
            SettingsSection::Skills => widgets::Boxed::new(section_skills(shared, reader)),
            SettingsSection::Storage => widgets::Boxed::new(node_settings_page(
                shared,
                reader,
                section_storage(shared, reader),
            )),
            SettingsSection::RemoteManagement => widgets::Boxed::new(node_settings_page(
                shared,
                reader,
                section_remote_management(shared, reader),
            )),
            SettingsSection::Cluster => widgets::Boxed::new(section_cluster(shared, reader)),
            SettingsSection::Modules => widgets::Boxed::new(node_settings_page(
                shared,
                reader,
                section_modules(shared, reader),
            )),
            SettingsSection::Usage => widgets::Boxed::new(usage_settings::section(shared, reader)),
            SettingsSection::Configuration => {
                widgets::Boxed::new(section_configuration_page(shared, reader))
            }
        };
        pages = pages.with_child(section_page(
            shared,
            reader,
            section,
            body,
            &scroll_states[section.index()],
        ));
    }
    pages.selected_from(selected)
}

fn section_page<W>(
    shared: &Shared,
    reader: &ThemeReader,
    section: SettingsSection,
    body: W,
    scroll_state: &ScrollState,
) -> impl Widget + use<W>
where
    W: Widget + 'static,
{
    // The header and the cards share one left-aligned column, so titles and card edges line up.
    let column = Stack::vertical()
        .gap(0.0)
        .alignment(Alignment::Stretch)
        .with_child(page_header(shared, section))
        .with_child(SizedBox::new().height(20.0))
        .with_child(body);
    let column = if section.uses_wide_page() {
        widgets::Boxed::new(column)
    } else {
        widgets::Boxed::new(PageColumn::new(reader, column))
    };
    let inset = Stack::vertical()
        .gap(0.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(CONTENT_PAD))
        .with_child(horizontal_inset(column, CONTENT_PAD))
        .with_child(SizedBox::new().height(CONTENT_PAD));
    ScrollView::vertical(inset).state(scroll_state.clone())
}

/// Build one group's rows for the desktop sidebar or narrow drill-in menu from the section
/// previews.
fn section_list_items(
    sections: &'static [SettingsSection],
    previews: &[(SettingsSection, String)],
    with_preview: bool,
) -> Vec<ListItem> {
    sections
        .iter()
        .map(|section| {
            let preview = previews
                .iter()
                .find(|(candidate, _)| candidate == section)
                .map(|(_, preview)| preview.as_str())
                .unwrap_or_default();
            let mut item = ListItem::new(section.title())
                .leading_icon(section.icon())
                .semantic_name(section.title())
                .description(format!("{} {preview}", section.description()));
            if with_preview {
                item = item.detail(preview);
            }
            item
        })
        .collect()
}

/// One group's list. Previews reach the rows' descriptions in place, so the list keeps its
/// selection, hover, and focus while settings load and change.
fn section_group_list(
    shared: &Shared,
    reader: &ThemeReader,
    name: &'static str,
    sections: &'static [SettingsSection],
    with_preview: bool,
    show_selection: bool,
    row_height: f32,
) -> impl Widget + use<> {
    let previews = settings_projection(shared, name, move |snapshot| {
        snapshot
            .navigation_previews
            .iter()
            .filter(|(section, _)| sections.contains(section))
            .cloned()
            .collect::<Vec<_>>()
    });
    let list = {
        let s = shared.clone();
        let selected_sections = sections;
        let action_sections = sections;
        ListView::new(name)
            .items(section_list_items(sections, &previews.get(), with_preview))
            .row_height(row_height)
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || {
                    if !show_selection {
                        return None;
                    }
                    let selected = s.settings().selected.unwrap_or(SettingsSection::Appearance);
                    selected_sections
                        .iter()
                        .position(|section| *section == selected)
                }
            })
            .on_change(move |i, _| {
                if let Some(section) = action_sections.get(i) {
                    s.settings_mut().open(*section);
                }
            })
    };
    let list = Restyle::new(list, previews, move |list, previews| {
        list.items(section_list_items(sections, previews, with_preview))
    });
    let rows = sections.len() as f32;
    ThemedHeight::new(
        reader,
        move |theme| {
            let padding = theme.metrics.data_viewport_padding;
            row_height * rows + padding.top + padding.bottom
        },
        list,
    )
}

fn section_group(
    shared: &Shared,
    reader: &ThemeReader,
    label: &'static str,
    list_name: &'static str,
    sections: &'static [SettingsSection],
    with_preview: bool,
    show_selection: bool,
    row_height: f32,
) -> impl Widget + use<> {
    Stack::vertical()
        .gap(5.0)
        .alignment(Alignment::Stretch)
        .with_child(horizontal_inset(
            SectionLabel::new(label).theme_when(clone_reader(reader)),
            10.0,
        ))
        .with_child(section_group_list(
            shared,
            reader,
            list_name,
            sections,
            with_preview,
            show_selection,
            row_height,
        ))
}

fn primary_section_groups_host(
    shared: &Shared,
    reader: &ThemeReader,
    with_preview: bool,
    show_selection: bool,
    row_height: f32,
) -> impl Widget + use<> {
    let mut groups = Stack::vertical().gap(12.0).alignment(Alignment::Stretch);
    for group in SETTINGS_GROUPS {
        groups = groups.with_child(section_group(
            shared,
            reader,
            group.title,
            group.list_name,
            group.sections,
            with_preview,
            show_selection,
            row_height,
        ));
    }
    groups
}

fn sidebar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let primary = Stack::vertical()
        .gap(6.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(14.0))
        .with_child(horizontal_inset(
            primary_section_groups_host(shared, reader, false, true, SIDEBAR_ITEM_H),
            8.0,
        ))
        .with_child(SizedBox::new().height(14.0))
        .with_child(SizedBox::new().height(14.0));
    ScrollView::vertical(primary).state(ScrollState::new())
}

fn menu(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let groups = Stack::vertical()
        .gap(0.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(CONTENT_PAD))
        .with_child(horizontal_inset(
            primary_section_groups_host(shared, reader, false, false, MENU_ROW_H),
            CONTENT_PAD,
        ))
        .with_child(SizedBox::new().height(14.0))
        .with_child(SizedBox::new().height(CONTENT_PAD));
    ScrollView::vertical(groups).state(ScrollState::new())
}

fn wide_pod(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_states: &[ScrollState; SettingsSection::ALL.len()],
) -> impl Widget + use<> {
    let detail = section_body(shared, reader, scroll_states);
    Stack::horizontal()
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new().width(SIDEBAR_W).child(
                Surface::new(SurfaceRole::Sidebar, sidebar(shared, reader))
                    .fill()
                    .theme_when(clone_reader(reader)),
            ),
        )
        .with_child(Separator::vertical().theme_when(clone_reader(reader)))
        .with_child(SizedBox::new().child(detail))
}

fn narrow_pod(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_states: &[ScrollState; SettingsSection::ALL.len()],
) -> impl Widget + use<> {
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
    let current_section = {
        let s = shared.clone();
        let color = shared.clone();
        Label::new("Current settings section")
            .text_when(move || {
                s.settings()
                    .selected
                    .unwrap_or(SettingsSection::Appearance)
                    .title()
                    .to_string()
            })
            .text_style_when({
                let s = shared.clone();
                move || settings_text_style(&s, TextSize::Base)
            })
            .color_when(move || palette(&color).text)
    };
    let backbar = TrailingSlotRow::new(
        Align::new(Alignment::Start, Alignment::Center, back_button),
        current_section,
    )
    .trailing_width(190.0)
    .trailing_height(32.0)
    .gap(12.0);
    let detail_with_backbar = Stack::vertical()
        .gap(0.0)
        .alignment(Alignment::Stretch)
        .with_child(horizontal_inset(
            Stack::vertical()
                .with_child(SizedBox::new().height(8.0))
                .with_child(SizedBox::new().height(40.0).child(backbar))
                .with_child(SizedBox::new().height(8.0)),
            12.0,
        ))
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)))
        .with_child(SizedBox::new().child(section_body(shared, reader, scroll_states)));

    let selected =
        shared
            .signals()
            .settings_structure
            .select_named("settings narrow menu selection", |key| {
                key.0
                    .as_deref()
                    .is_some_and(|snapshot| snapshot.selected.is_some()) as usize
            });
    SwitchView::new()
        .with_child(menu(shared, reader))
        .with_child(detail_with_backbar)
        .selected_from(selected)
}

// ---------------------------------------------------------------------------
// Tiny layout helpers
// ---------------------------------------------------------------------------

/// Retains the editor (and therefore focus/caret state) while still accepting model-side resets.
/// The model is updated synchronously by the input callback, so a differing value here always
/// represents an external change such as Clear filters or activating the other responsive tree.
struct SyncedTextInput {
    inner: TextInput,
    value_reader: Box<dyn Fn() -> String>,
    observe: Option<Box<dyn Fn(&MeasureCtx)>>,
}

impl SyncedTextInput {
    fn new<F>(inner: TextInput, value_reader: F) -> Self
    where
        F: Fn() -> String + 'static,
    {
        Self {
            inner,
            value_reader: Box::new(value_reader),
            observe: None,
        }
    }

    /// Re-check the model whenever `source` changes, such as a reset revision, so an external
    /// change reaches the retained editor without a rebuild.
    fn observing<K: 'static>(mut self, source: impl Observable<K> + 'static) -> Self {
        self.observe = Some(Box::new(move |ctx| {
            let _: K = ctx.observe(&source);
        }));
        self
    }

    fn sync_value(&mut self) {
        let value = (self.value_reader)();
        if self.inner.current_value() != value {
            self.inner.set_value(value);
        }
    }
}

impl Widget for SyncedTextInput {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        self.inner.event(ctx, event);
    }

    fn command(&mut self, ctx: &mut EventCtx, command: &Command<'_>) {
        self.inner.command(ctx, command);
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        if let Some(observe) = &self.observe {
            observe(ctx);
        }
        self.sync_value();
        self.inner.measure(ctx, constraints)
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.inner.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.inner.semantics(ctx);
    }

    fn accepts_focus(&self) -> bool {
        self.inner.accepts_focus()
    }

    fn focus_changed(&mut self, ctx: &mut EventCtx, focused: bool) {
        self.inner.focus_changed(ctx, focused);
    }
}

/// Checkbox counterpart to [`SyncedTextInput`], used where wide and narrow trees share one model.
struct SyncedCheckbox {
    inner: Checkbox,
    checked_reader: Box<dyn Fn() -> bool>,
    observe: Option<Box<dyn Fn(&MeasureCtx)>>,
}

impl SyncedCheckbox {
    fn new<F>(inner: Checkbox, checked_reader: F) -> Self
    where
        F: Fn() -> bool + 'static,
    {
        Self {
            inner,
            checked_reader: Box::new(checked_reader),
            observe: None,
        }
    }

    /// See [`SyncedTextInput::observing`].
    fn observing<K: 'static>(mut self, source: impl Observable<K> + 'static) -> Self {
        self.observe = Some(Box::new(move |ctx| {
            let _: K = ctx.observe(&source);
        }));
        self
    }

    fn sync_checked(&mut self) {
        let checked = (self.checked_reader)();
        if self.inner.is_checked() != checked {
            self.inner.set_checked(checked);
        }
    }
}

impl Widget for SyncedCheckbox {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        self.inner.event(ctx, event);
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        if let Some(observe) = &self.observe {
            observe(ctx);
        }
        self.sync_checked();
        self.inner.measure(ctx, constraints)
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.inner.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.inner.semantics(ctx);
    }

    fn accepts_focus(&self) -> bool {
        self.inner.accepts_focus()
    }

    fn focus_changed(&mut self, ctx: &mut EventCtx, focused: bool) {
        self.inner.focus_changed(ctx, focused);
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

/// Keeps the horizontal-inset call sites concise while delegating sizing and
/// retained-child behavior to SUI's stock padding container.
fn horizontal_inset<W>(child: W, pad: f32) -> WidgetPadding
where
    W: Widget + 'static,
{
    WidgetPadding::horizontal(pad, child).fill_child_width(true)
}

// ---------------------------------------------------------------------------
// Live device-login status — composed from stock SUI text/link widgets.
// ---------------------------------------------------------------------------

fn status_reflector(shared: &Shared, index: usize, reader: &ThemeReader) -> impl Widget + use<> {
    let source = settings_projection(shared, "settings device login status", move |snapshot| {
        snapshot.device_logins.get(index).cloned()
    });
    let build_shared = shared.clone();
    let build_reader = Rc::clone(reader);
    // A small status block, rebuilt when the sign-in flow moves to its next step.
    RebuildOnChange::key_from(source, move |_| {
        let (primary, prompt_url) = device_login_lines(&build_shared, index);
        let prompt_code = device_login_code(&build_shared, index);
        let is_error = device_login_is_error(&build_shared, index);
        let style = build_shared.clone();
        let mut body = Flex::vertical().align_items(Alignment::Start).with_child(
            sui::Label::new(if prompt_code.is_some() {
                "Enter this code:"
            } else {
                primary.as_str()
            })
            .text_style_when(move || {
                let mut text = settings_text_style(&style, TextSize::Sm);
                let pal = palette(&style);
                text.weight = FontWeight::MEDIUM;
                text.color = if is_error { pal.bad_text } else { pal.text };
                text
            }),
        );
        if let Some(code) = prompt_code {
            body = body.with_child(device_login_code_control(
                &build_shared,
                &build_reader,
                code,
            ));
        }
        if let Some(url) = prompt_url {
            // Links have no style reader; the retained link is restyled when the theme changes.
            let link = sui::Link::new(url.clone(), url)
                .semantic_name("Open login page")
                .theme_when(clone_reader(&build_reader))
                .on_open(open_provider_login_url);
            body = body.with_child(Restyle::new(
                link,
                build_shared.signals().theme.clone(),
                |link, theme| {
                    link.text_style(sui::TextStyle {
                        font_size: theme.text.xs.size,
                        line_height: theme.text.xs.line_height,
                        ..theme.text_style(theme.surfaces.accent_text)
                    })
                },
            ));
        }
        WidgetPod::new(body)
    })
}

fn device_login_code_control(
    shared: &Shared,
    reader: &ThemeReader,
    code: String,
) -> impl Widget + use<> {
    let selection = SelectionScope::new();
    let field = TextInput::new("provider-device-login-code")
        .value(code.clone())
        .read_only(true)
        .selection_scope(selection.clone())
        .copy_to_clipboard(true)
        .padding(sui::Insets {
            left: 14.0,
            top: 6.0,
            right: 14.0,
            bottom: 6.0,
        })
        .min_width(240.0)
        .min_height(54.0)
        .theme_when(clone_reader(reader));
    // Text inputs have no style reader; the retained field is restyled when the theme changes.
    let field = Restyle::new(field, shared.signals().theme.clone(), |field, theme| {
        field.text_style(device_login_code_text_style(theme, &theme.surfaces))
    });
    let field = read_only_text_menu(reader, "provider-device-login-code", field, selection, true);
    let copy_code = code;
    let copy = sui::Button::new("Copy code")
        .semantic_name("Copy pairing code")
        .description("Copy the device-login pairing code to the clipboard")
        .icon(IconGlyph::Restore)
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| ctx.set_clipboard_text(&copy_code));

    Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Center)
        .with_item(field, FlexItem::new().min_width(240.0))
        .with_item(copy, FlexItem::new().no_shrink())
}

fn device_login_code_text_style(theme: &DefaultTheme, pal: &SurfacePalette) -> sui::TextStyle {
    let mut style = theme.mono_text_style(pal.text);
    style.font_size = theme.text._3xl.size;
    style.line_height = theme.text._3xl.line_height;
    style.weight = FontWeight::SEMIBOLD;
    style
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

fn device_login_code(shared: &Shared, index: usize) -> Option<String> {
    let settings = shared.settings();
    match device_login_status_for(&settings, index) {
        Some(DeviceLoginStatus::Prompt { user_code, .. }) => Some(user_code.clone()),
        _ => None,
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
    let url = validate_external_http_url(url)?;
    open_url_with_system_browser(url)
}

fn validate_external_http_url(url: &str) -> Result<&str, String> {
    let url = url.trim();
    if url.is_empty() {
        return Err("URL is empty".to_string());
    }
    if url
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err("URL contains whitespace or control characters".to_string());
    }
    let (scheme, location) = url
        .split_once("://")
        .ok_or_else(|| "URL must be an absolute http or https URL".to_string())?;
    if !scheme.eq_ignore_ascii_case("https") && !scheme.eq_ignore_ascii_case("http") {
        return Err("only http and https URLs can be opened".to_string());
    }
    let authority = location.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() {
        return Err("URL is missing a host".to_string());
    }
    Ok(url)
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

#[cfg(all(target_os = "android", feature = "mobile"))]
fn open_url_with_system_browser(url: &str) -> Result<(), String> {
    crate::open_android_external_url(url)
}

#[cfg(all(target_os = "android", not(feature = "mobile")))]
fn open_url_with_system_browser(_url: &str) -> Result<(), String> {
    Err("opening URLs requires the Android mobile app surface".to_string())
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "android"))))]
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
// Responsive layout
// ---------------------------------------------------------------------------

/// Builds both settings presentations once and lets SUI retain, focus-scope, and switch the
/// active branch from the layout's own incoming constraints.
fn responsive_settings_view(shared: Shared) -> impl Widget {
    let wide_theme_reader: ThemeReader = {
        let shared = shared.clone();
        Rc::new(move || settings_control_theme(&shared, ControlSize::Small))
    };
    let narrow_theme_reader: ThemeReader = {
        let shared = shared.clone();
        Rc::new(move || settings_control_theme(&shared, ControlSize::Medium))
    };
    // Preserve an independent reading position for each destination. Both retained branches share
    // each destination's state, so crossing the breakpoint keeps the right position without
    // carrying Appearance's offset into Providers or System configuration.
    let scroll_states: [ScrollState; SettingsSection::ALL.len()] =
        std::array::from_fn(|_| ScrollState::new());
    let narrow = narrow_pod(&shared, &narrow_theme_reader, &scroll_states);
    let wide = wide_pod(&shared, &wide_theme_reader, &scroll_states);

    ConstraintView::new(narrow).when(ConstraintQuery::new().min_width(WIDE_THRESHOLD), wide)
}

fn settings_control_theme(shared: &Shared, size: ControlSize) -> DefaultTheme {
    shared.theme_sized(size)
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
    responsive_settings_view(shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::state::{AppState, Shared};
    use crate::ui::chat::{ActivityTone, ChatActivityEntry};

    fn backend_with_mounted_agent_catalog() -> Backend {
        let mut backend = Backend::local_fallback();
        backend
            .use_mounted_catalog_reads_for_tests()
            .expect("test agent catalog VFS");
        backend
    }

    fn scroll_agent_rows_into_view(
        window: &sui_testing::TestWindow,
        names: &[&str],
    ) -> sui::Result<()> {
        window
            .get_by_role(sui::SemanticsRole::List)
            .with_name("agent-config-list")
            .scroll_pixels(sui::Vector::new(0.0, -2000.0))?;
        assert_agent_rows_in_view(window, names)
    }

    fn assert_agent_rows_in_view(
        window: &sui_testing::TestWindow,
        names: &[&str],
    ) -> sui::Result<()> {
        window.run_until_idle()?;
        let snapshot = window.snapshot()?;
        let list = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::List
                    && node.name.as_deref() == Some("agent-config-list")
            })
            .expect("agent list viewport");
        let viewport = snapshot
            .scene_summary
            .as_ref()
            .expect("rendered agent settings viewport")
            .viewport;
        for name in names {
            let row = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| {
                    node.parent == Some(list.id)
                        && node.role == sui::SemanticsRole::ListItem
                        && node.name.as_deref() == Some(*name)
                })
                .unwrap_or_else(|| panic!("agent row {name:?} should be realized after scrolling"));
            let center_x = row.bounds.x() + row.bounds.width() * 0.5;
            let center_y = row.bounds.y() + row.bounds.height() * 0.5;
            assert!(
                center_x >= list.bounds.x()
                    && center_x < list.bounds.max_x()
                    && center_y >= list.bounds.y()
                    && center_y < list.bounds.max_y()
                    && center_x >= 0.0
                    && center_x < viewport.width
                    && center_y >= 0.0
                    && center_y < viewport.height,
                "agent row {name:?} center should lie inside the list and window viewports: row={:?}, list={:?}, viewport={viewport:?}",
                row.bounds,
                list.bounds,
            );
        }
        Ok(())
    }

    #[test]
    fn agent_summary_identifies_the_providing_module() {
        let summary = AgentConfigSummary {
            role: "evolution".to_string(),
            source: "module".to_string(),
            default_model: "Cluster default".to_string(),
            module_id: Some("module.evolution".to_string()),
            module_version: Some("0.1.0".to_string()),
            ..AgentConfigSummary::default()
        };

        assert_eq!(
            agent_summary_line(&summary),
            "evolution · Cluster default · provided by module.evolution"
        );
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

    fn reload_config_state(state: &mut SettingsState) {
        state.config_items = resolve_items(state.config.as_ref());
        state.provider_forms = provider_forms(state.config.as_ref());
    }

    fn connected_sifs_mount_snapshot_at(
        observed_at: time::OffsetDateTime,
    ) -> SifsMountSettingsSnapshot {
        let timestamp = observed_at
            .format(&time::format_description::well_known::Rfc3339)
            .expect("connected fixture timestamp");
        let config = SifsMountConfig {
            enabled: true,
            mount_point: r"C:\SinomoSettingsMount".to_string(),
            root_path: "/".to_string(),
            read_only: false,
            allow_other: false,
            sync_local_changes: true,
            operation_budget: None,
        };
        let status = sinomo_runtime::dfs::SifsMountRuntimeStatus {
            phase: SifsMountRuntimePhase::Connected,
            platform: SifsMountPlatform::ExternalProvider,
            configured_mount_point: config.mount_point.clone(),
            resolved_mount_point: Some(config.mount_point.clone()),
            root_path: config.root_path.clone(),
            read_only: config.read_only,
            sync_local_changes: config.sync_local_changes,
            writeback_mode: SifsMountWritebackMode::Active,
            provider_registered: true,
            provider_connected: true,
            worker_alive: true,
            worker_ready: true,
            worker_health_generation: 7,
            last_worker_heartbeat_at: Some(timestamp.clone()),
            daemon_pid: Some(4242),
            provider_content_hash: Some("abcdef0123456789".to_string()),
            provider_interface_version: Some(
                HOST_FILESYSTEM_PROVIDER_INTERFACE_VERSION.to_string(),
            ),
            placeholders_created: Some(3),
            detail: "callbacks connected after a complete worker pass".to_string(),
            updated_at: timestamp,
            ..sinomo_runtime::dfs::SifsMountRuntimeStatus::default()
        };
        SifsMountSettingsSnapshot {
            platform: SifsMountPlatform::current(),
            config,
            status,
            config_error: None,
            status_error: None,
        }
    }

    #[test]
    fn sifs_mount_text_drafts_preserve_the_retained_form_structure() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let shape = |shared: &Shared| {
            let key = settings_structure_key(shared);
            sifs_mount_editor_shape(&key.0.as_deref().expect("settings snapshot").sifs_mount)
        };
        let before = shape(&shared);

        shared.settings_mut().edit_sifs_mount(|form| {
            form.mount_point = "/tmp/sinomo-live".to_string();
        });
        assert_eq!(
            shape(&shared),
            before,
            "typing must not rebuild the form and discard TextInput focus or caret state"
        );

        let read_only = !shared.settings().sifs_mount_form.read_only;
        shared.settings_mut().edit_sifs_mount(|form| {
            form.read_only = read_only;
        });
        assert_eq!(
            shape(&shared),
            before,
            "switches update in place rather than rebuilding the form"
        );
        let key = settings_structure_key(&shared);
        assert_eq!(
            key.0
                .as_deref()
                .expect("settings snapshot")
                .sifs_mount
                .form
                .read_only,
            read_only,
            "the retained switches follow the projected form"
        );
    }

    #[test]
    fn sifs_mount_live_observations_do_not_rebuild_the_editor() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let before = {
            let key = settings_structure_key(&shared);
            sifs_mount_editor_shape(&key.0.as_deref().expect("settings snapshot").sifs_mount)
        };

        {
            let mut settings = shared.settings_mut();
            settings.sifs_mount_snapshot.status.worker_health_generation = 9;
            settings.sifs_mount_snapshot.status.updated_at = "2026-08-10T21:00:00Z".to_string();
            settings
                .pending_operations
                .insert(SettingsOperationScope::SifsMount, 99);
        }

        let after = {
            let key = settings_structure_key(&shared);
            sifs_mount_editor_shape(&key.0.as_deref().expect("settings snapshot").sifs_mount)
        };
        assert_eq!(
            after, before,
            "heartbeat refreshes and silent pending state must not replace focused mount inputs"
        );
    }

    #[test]
    fn sifs_mount_silent_refresh_result_preserves_the_editor_and_unsaved_draft() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend.clone()));
        let unsaved_mount_point = "/tmp/sinomo-unsaved-mount-draft".to_string();
        {
            let mut settings = shared.settings_mut();
            settings.sifs_mount_snapshot.status.worker_health_generation = 91;
            settings.sifs_mount_snapshot.status.detail = "older live observation".to_string();
            settings.edit_sifs_mount(|form| {
                form.mount_point = unsaved_mount_point.clone();
            });
        }
        let before_status = shared.settings().sifs_mount_snapshot.status.clone();
        let before_editor = {
            let key = settings_structure_key(&shared);
            sifs_mount_editor_shape(&key.0.as_deref().expect("settings snapshot").sifs_mount)
        };

        let effects = {
            let mut settings = shared.settings_mut();
            apply_test_operation(
                &mut settings,
                &backend,
                SettingsOperationScope::SifsMount,
                SettingsOperation::RefreshSifsMount {
                    update_feedback: false,
                },
            )
        };

        assert!(effects.changed);
        assert_ne!(shared.settings().sifs_mount_snapshot.status, before_status);
        assert_eq!(
            shared.settings().sifs_mount_form.mount_point,
            unsaved_mount_point,
            "a silent observation refresh must never discard an unsaved mount draft"
        );
        let after_editor = {
            let key = settings_structure_key(&shared);
            sifs_mount_editor_shape(&key.0.as_deref().expect("settings snapshot").sifs_mount)
        };
        assert_eq!(
            after_editor, before_editor,
            "the 1s live-status result must update only the runtime subtree"
        );
    }

    #[test]
    fn sifs_mount_save_draft_fails_closed_before_queueing() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.edit_sifs_mount(|form| {
            form.enabled = true;
            form.mount_point = "relative/path".to_string();
            form.root_path = "/teams/../private".to_string();
        });

        assert_eq!(
            state.sifs_mount_save_config(),
            Err("mount location must be absolute or begin with ~/".to_string())
        );
        assert!(!state.sifs_mount_pending());
    }

    #[test]
    fn sifs_mount_structure_carries_observed_runtime_evidence() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let snapshot = connected_sifs_mount_snapshot_at(time::OffsetDateTime::now_utc());
        {
            let mut settings = shared.settings_mut();
            settings.sifs_mount_snapshot = snapshot;
        }

        let key = settings_structure_key(&shared);
        let mount = &key.0.as_deref().expect("settings snapshot").sifs_mount;
        assert_eq!(
            mount.snapshot.status.phase,
            SifsMountRuntimePhase::Connected
        );
        assert_eq!(mount.snapshot.status.daemon_pid, Some(4242));
        assert_eq!(
            mount.snapshot.status.provider_interface_version.as_deref(),
            Some(HOST_FILESYSTEM_PROVIDER_INTERFACE_VERSION)
        );
        assert_eq!(
            section_preview(&shared, SettingsSection::Storage),
            "Connected"
        );
    }

    #[test]
    fn sifs_mount_connected_presentation_expires_without_a_new_rpc() {
        let observed_at = time::OffsetDateTime::parse(
            "2026-08-10T21:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("fixed observation time");
        let snapshot = connected_sifs_mount_snapshot_at(observed_at);

        let fresh = sifs_mount_presentation_at(&snapshot, observed_at);
        assert_eq!(fresh.label, "Connected");
        assert_eq!(fresh.tone, SemanticTone::Success);

        let expired =
            sifs_mount_presentation_at(&snapshot, observed_at + time::Duration::seconds(6));
        assert_eq!(expired.label, "Needs attention");
        assert_eq!(expired.tone, SemanticTone::Danger);
        assert!(expired.detail.contains("freshness window"));
    }

    #[test]
    fn sifs_mount_failed_operations_are_never_green() {
        let observed_at = time::OffsetDateTime::parse(
            "2026-08-10T21:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("fixed observation time");
        let mut snapshot = connected_sifs_mount_snapshot_at(observed_at);
        snapshot.status.failed_operations = 1;

        let presentation = sifs_mount_presentation_at(&snapshot, observed_at);
        assert_eq!(presentation.label, "Needs attention");
        assert_eq!(presentation.tone, SemanticTone::Danger);
        assert!(presentation.operator_action.is_some());
    }

    #[test]
    fn sifs_mount_path_conflict_is_warning_while_mount_remains_functional() {
        let observed_at = time::OffsetDateTime::parse(
            "2026-08-10T21:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("fixed observation time");
        let mut snapshot = connected_sifs_mount_snapshot_at(observed_at);
        snapshot.status.conflict_count = 1;

        let presentation = sifs_mount_presentation_at(&snapshot, observed_at);
        assert_eq!(presentation.label, "Mounted with conflicts");
        assert_eq!(presentation.tone, SemanticTone::Warning);
        assert!(
            presentation
                .detail
                .contains("unrelated paths remain available")
        );
        assert!(presentation.operator_action.is_some());
    }

    #[test]
    fn sifs_mount_bounded_attention_exposes_path_evidence_and_safe_action() {
        let observed_at = time::OffsetDateTime::parse(
            "2026-08-10T21:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("fixed observation time");
        let mut snapshot = connected_sifs_mount_snapshot_at(observed_at);
        let attention = SifsMountAttentionSummary {
            kind: SifsMountAttentionKind::UnsupportedObjectKind,
            path: "/teams/design/socket".to_string(),
            reason_code: "unsupported-object-kind".to_string(),
            reason: "external provider cannot expose a Unix socket".to_string(),
            retained_local_evidence: Some(sinomo_runtime::dfs::SifsMountRetainedLocalEvidence {
                operation_id: Some("operation-7".to_string()),
                evidence_id: Some("evidence-9".to_string()),
                staged_content_hash: Some("blake3:abcdef".to_string()),
                staged_content_size_bytes: Some(41),
                local_state: Some("preserved".to_string()),
            }),
            safe_next_action: SifsMountAttentionNextAction::ReviewUnsupportedEntry,
        };
        snapshot.status.attention = vec![attention.clone()];
        snapshot.status.attention_overflow = 3;

        let presentation = sifs_mount_presentation_at(&snapshot, observed_at);
        assert_eq!(presentation.label, "Mounted with conflicts");
        assert_eq!(presentation.tone, SemanticTone::Warning);
        assert!(presentation.detail.contains("/teams/design/socket"));
        assert!(presentation.detail.contains("3 additional"));

        let detail = sifs_mount_attention_detail(&attention);
        assert!(detail.contains("unsupported-object-kind"));
        assert!(detail.contains("operation-7"));
        assert!(detail.contains("evidence-9"));
        assert!(detail.contains("blake3:abcdef"));
        assert!(detail.contains("preserve it outside the sync workflow"));
    }

    #[test]
    fn sifs_mount_pending_or_retrying_work_is_warning_not_success() {
        let observed_at = time::OffsetDateTime::parse(
            "2026-08-10T21:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("fixed observation time");
        for (pending_operations, retrying_operations) in [(1, 0), (0, 1)] {
            let mut snapshot = connected_sifs_mount_snapshot_at(observed_at);
            snapshot.status.pending_operations = pending_operations;
            snapshot.status.retrying_operations = retrying_operations;

            let presentation = sifs_mount_presentation_at(&snapshot, observed_at);
            assert_eq!(presentation.label, "Syncing");
            assert_eq!(presentation.tone, SemanticTone::Warning);
        }
    }

    #[test]
    fn provider_login_url_validation_accepts_absolute_http_urls() {
        assert_eq!(
            validate_external_http_url(" https://example.test/device?code=ABCD "),
            Ok("https://example.test/device?code=ABCD")
        );
        assert_eq!(
            validate_external_http_url("HTTP://localhost:8080/login"),
            Ok("HTTP://localhost:8080/login")
        );
    }

    #[test]
    fn provider_login_url_validation_rejects_unsafe_or_incomplete_urls() {
        for url in [
            "",
            "example.test/device",
            "https:example.test/device",
            "https:///device",
            "file:///tmp/device",
            "mailto:user@example.test",
            "https://example.test/device code",
            "https://example.test/device\0code",
        ] {
            assert!(
                validate_external_http_url(url).is_err(),
                "{url:?} should be rejected"
            );
        }
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
        let targeted = if state.operation_targets_node(&operation) {
            backend
                .with_settings_node(
                    state
                        .module_selected_node_id
                        .clone()
                        .or_else(|| Some(backend.node.node_id.to_string())),
                )
                .unwrap()
        } else {
            backend.clone()
        };
        let payload = execute_settings_operation(&targeted, operation);
        let (request_tx, _request_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        result_tx
            .send(SettingsOperationResult {
                node_epoch: None,
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
            settings.set_skill_import_path("skill-name".to_string());
            settings.set_skill_export_path("exported-skill".to_string());
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
            settings.create_agent();
            settings.agent_form.name.clear();
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
            .with_name("agent-name");

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
        assert_eq!(shared.settings().agent_form.name, "AB");

        Ok(())
    }

    #[test]
    fn settings_error_text_can_be_selected_and_copied() -> sui::Result<()> {
        let fixture =
            sinomo_testdir::TestDir::new("app-ui-settings-error-text-can-be-selected-and-copied");
        let backend = Backend::local_fallback_for_data_dir(
            sinomo_runtime::local::SinomoDataDir::new(fixture.path()),
        );
        let shared = Shared::new(AppState::ready(backend));
        let error_message =
            "agent definition store I/O failed at /services/sinomo/agents: metadata unavailable";
        shared.settings_mut().agent_action_error = Some(error_message.to_string());
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Selectable settings error")
                    .root(feedback_slot(&shared, &reader, |snapshot| {
                        snapshot.feedback.agent.clone()
                    })),
            )
            .build()?;
        let clipboard = runtime.clipboard();
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let error = window
            .get_by_role(sui::SemanticsRole::Text)
            .with_name("Settings error");

        error.expect().to_have_value(error_message)?;
        error.focus()?;
        let mut select_all = sui::KeyboardEvent::new("a", sui::KeyState::Pressed);
        select_all.modifiers.control = true;
        error.dispatch_event(sui::Event::Keyboard(select_all))?;
        let mut copy = sui::KeyboardEvent::new("c", sui::KeyState::Pressed);
        copy.modifiers.control = true;
        error.dispatch_event(sui::Event::Keyboard(copy))?;
        assert_eq!(clipboard.text().as_deref(), Some(error_message));

        clipboard.set_text("context menu did not copy");
        let error_node = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == sui::SemanticsRole::Text
                    && node.name.as_deref() == Some("Settings error")
            })
            .expect("settings error semantics");
        let center = sui::Point::new(
            error_node.bounds.x() + error_node.bounds.width() * 0.5,
            error_node.bounds.y() + error_node.bounds.height() * 0.5,
        );
        let mut secondary_down = sui::PointerEvent::new(sui::PointerEventKind::Down, center);
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
        assert_eq!(clipboard.text().as_deref(), Some(error_message));

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

    #[test]
    fn initial_skill_state_reports_unavailable_package_content() {
        use sinomo_agent::{
            SkillCatalogSifsStore, SkillCatalogSource, SkillCatalogStore, SkillCatalogWrite,
        };

        let vfs = VfsHandle::memory();
        let write = SkillCatalogWrite::from_markdown_skill(
            None,
            "1.0.0",
            SkillCatalogSource::user("settings-test"),
            "---\nname: unavailable-skill\ndescription: Exercise unavailable skill content.\n---\n\n# Unavailable skill\n",
        )
        .expect("skill write");
        let skill_path = write.entry.sifs_path.clone();
        SkillCatalogSifsStore::from_vfs(vfs.clone())
            .write_skill_version(write)
            .expect("persist skill fixture");
        vfs.remove(&skill_path).expect("remove package content");

        let (skills, selected, detail, error) = load_initial_skill_state(&vfs, true);

        assert_eq!(skills.len(), 1, "catalog descriptor remains visible");
        assert!(selected.is_some());
        assert!(detail.is_none());
        assert!(
            error.is_some(),
            "package read failure must not masquerade as an absent detail"
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

    fn temp_skill_backend(_label: &str) -> (Backend, PathBuf) {
        let backend = Backend::local_fallback();
        let root = backend.data_dir.root();
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
            chat_providers: Vec::new(),
            server_generations: baseline_server,
            draft_generations: baseline_drafts,
        });

        assert!(!live.agent_snapshot.agents.is_empty());
        assert_eq!(live.agent_form.id, "draft-agent");
        assert_eq!(live.agent_form.name, "Unsaved draft");
    }

    #[test]
    fn provider_preload_exports_chat_snapshot_without_overwriting_newer_edits() {
        let backend = Backend::local_fallback();
        let provider = ProviderSummary {
            id: "codex".to_string(),
            account: sinomo_runtime::api::lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "Codex".to_string(),
            models: vec!["gpt-5.4".to_string()],
            capabilities: provider_capabilities("codex"),
        };

        let mut current = SettingsState::new_lightweight(&backend);
        let current_server_generations = current.server_generations;
        let current_draft_generations = current.draft_generations;
        let accepted = current.merge_initial_load(SettingsInitialLoadResult {
            settings: SettingsState::new(&backend),
            chat_providers: vec![provider.clone()],
            server_generations: current_server_generations,
            draft_generations: current_draft_generations,
        });
        assert_eq!(accepted, Some(vec![provider.clone()]));

        let mut edited = SettingsState::new_lightweight(&backend);
        let stale_generation = edited.server_generations;
        edited
            .server_generations
            .bump(SettingsOperationDomain::Providers);
        let edited_draft_generations = edited.draft_generations;
        let rejected = edited.merge_initial_load(SettingsInitialLoadResult {
            settings: SettingsState::new(&backend),
            chat_providers: vec![provider],
            server_generations: stale_generation,
            draft_generations: edited_draft_generations,
        });
        assert_eq!(rejected, None);
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
                node_epoch: None,
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
                node_epoch: None,
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
            Some(
                "Connection changed while the action was in flight; verify refreshed state before retrying."
            )
        );
        assert_eq!(
            state.agent_action_error.as_deref(),
            Some(
                "Connection changed while the action was in flight; verify refreshed state before retrying."
            )
        );

        let reconnect_server_generations = state.server_generations;
        let reconnect_draft_generations = state.draft_generations;
        state.merge_initial_load(SettingsInitialLoadResult {
            settings: SettingsState::new(&new_backend),
            chat_providers: Vec::new(),
            server_generations: reconnect_server_generations,
            draft_generations: reconnect_draft_generations,
        });
        assert_eq!(state.agent_form.id, "reconnect-draft");
        assert_eq!(state.agent_form.name, "Keep across reconnect");
    }

    #[test]
    fn reconnect_rebind_preserves_an_in_flight_local_permission_result() {
        let old_backend = Backend::local_fallback();
        let new_backend =
            Backend::local_fallback_for_data_dir(SinomoDataDir::new(old_backend.data_dir.root()));
        let mut state = SettingsState::new(&old_backend);
        let (request_tx, _request_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        state.operation_lane = Some(SettingsOperationLane {
            tx: request_tx,
            rx: result_rx,
        });
        state
            .pending_operations
            .insert(SettingsOperationScope::RemoteManagementPermission, 19);

        state.rebind_backend(&new_backend);

        assert!(state.operation_lane.is_some());
        assert_eq!(state.pending_operations.len(), 1);
        assert!(state.remote_management_permission_pending());
        assert_eq!(
            state.remote_management_permission_status.as_deref(),
            Some("Finishing the node-local permission change…")
        );
        let reconnect_server_generations = state.server_generations;
        let reconnect_draft_generations = state.draft_generations;

        let mutation = old_backend
            .set_local_remote_management_permission(true)
            .expect("complete local permission write");
        result_tx
            .send(SettingsOperationResult {
                node_epoch: None,
                request_id: 19,
                scope: SettingsOperationScope::RemoteManagementPermission,
                draft_generation: 0,
                payload: SettingsOperationPayload::RemoteManagement(Ok(
                    RemoteManagementOperationSuccess::PermissionChanged {
                        permissions: mutation.permissions,
                        target: LocalPermissionTarget::RemoteManagement,
                        message: "Remote management is now allowed on this node".to_string(),
                    },
                )),
            })
            .expect("send retained local result");

        assert!(state.pump_operations().changed);
        assert!(state.remote_management.permission_enabled);
        assert!(!state.remote_management_permission_pending());

        let mut refreshed = SettingsState::new(&new_backend);
        refreshed.remote_management.module_observed = ModuleObservedState::Stopped;
        state.merge_initial_load(SettingsInitialLoadResult {
            settings: refreshed,
            chat_providers: Vec::new(),
            server_generations: reconnect_server_generations,
            draft_generations: reconnect_draft_generations,
        });
        assert!(state.remote_management.permission_enabled);
        assert_eq!(
            state.remote_management.module_observed,
            ModuleObservedState::Stopped
        );
    }

    #[test]
    fn reconnect_epoch_skips_queued_daemon_work_but_keeps_local_permission_work() {
        let backend = Backend::local_fallback();
        let epoch_source = Arc::new(AtomicU64::new(4));
        let daemon_request = SettingsOperationRequest {
            request_id: 1,
            scope: SettingsOperationScope::RemoteManagementRefresh,
            draft_generation: 0,
            operation_epoch: 4,
            operation_epoch_source: Arc::clone(&epoch_source),
            node_epoch: None,
            backend: backend.clone(),
            operation: SettingsOperation::RefreshRemoteManagement,
        };
        assert!(settings_operation_request_is_current(&daemon_request));

        epoch_source.store(5, Ordering::Release);
        assert!(!settings_operation_request_is_current(&daemon_request));

        let permission_request = SettingsOperationRequest {
            request_id: 2,
            scope: SettingsOperationScope::RemoteManagementPermission,
            draft_generation: 0,
            operation_epoch: 4,
            operation_epoch_source: epoch_source,
            node_epoch: None,
            backend,
            operation: SettingsOperation::SetRemoteManagementPermission { enabled: true },
        };
        assert!(settings_operation_request_is_current(&permission_request));
    }

    #[test]
    fn local_module_refresh_does_not_restore_an_abandoned_local_selection() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.module_selected_node_id = Some("remote-node".to_string());

        state.apply_module_snapshot_if_selected(backend.module_config_snapshot(None));

        assert_eq!(
            state.module_selected_node_id.as_deref(),
            Some("remote-node")
        );
    }

    #[test]
    fn set_value_with_invalid_input_errors_and_preserves_prior_value() {
        // A typed (Number/Json) key: writing garbage returns Err and the stored value is unchanged.
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        let Some(idx) = state.config_items.iter().position(|item| {
            !item.read_only
                && matches!(
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
        let legacy = "module.external-executor";
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
                node_epoch: None,
                request_id: 41,
                scope: SettingsOperationScope::Providers,
                draft_generation: 0,
                payload: SettingsOperationPayload::ProviderHealth(ProviderHealthOperationResult {
                    provider: provider.clone(),
                    account: account.clone(),
                    result: Ok(
                        sinomo_runtime::api::lm::LanguageModelProviderHealthSnapshot {
                            provider: provider.clone(),
                            account: account.clone(),
                            checked_at_unix: 1_750_000_000,
                            last_verified_at_unix: Some(1_750_000_000),
                            healthy: true,
                            error: None,
                            models: vec!["gpt-5.5".to_string()],
                            model_discovery_error: None,
                            usage: Some(sinomo_runtime::api::lm::LanguageModelProviderUsage {
                                provider: provider.clone(),
                                plan: Some("pro".to_string()),
                                captured_at_unix: 1_750_000_000,
                                limits: Vec::new(),
                                credits: None,
                            }),
                            usage_error: None,
                            stale: false,
                        },
                    ),
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
        let mut limit = sinomo_runtime::api::lm::LanguageModelUsageLimit {
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

        let second_path = write_skill_file(&root, "shortcut-skill", "The shortcut is beta-lane.");
        let second_reference = second_path
            .parent()
            .expect("second skill directory")
            .join("references")
            .join("details.md");
        std::fs::create_dir_all(second_reference.parent().expect("reference parent"))
            .expect("reference directory");
        std::fs::write(&second_reference, "Beta lane reference.").expect("reference");
        sinomo_cli::app_api::import_skill(
            backend.data_dir.as_ref(),
            &second_path,
            None,
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

        let export_path = root.join("exports").join("shortcut-skill");
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
        let exported =
            std::fs::read_to_string(export_path.join("SKILL.md")).expect("exported skill");
        assert!(exported.contains("name: shortcut-skill"));
        assert!(exported.contains("beta-lane"));
        assert_eq!(
            std::fs::read_to_string(export_path.join("references/details.md"))
                .expect("exported reference"),
            "Beta lane reference."
        );
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
        // The control sits below the default 720px test viewport; off-viewport clicks miss.
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 2400.0,
            ))))?;
        app.run_until_idle()?;

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
        let snapshot = window.snapshot()?;
        let row = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::ListItem
                    && node.name.as_deref() == Some("render-skill @ 1.0.0")
            })
            .expect("render skill row semantics");
        let title = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::Text
                    && node.name.as_deref() == Some("render-skill @ 1.0.0")
                    && node.bounds.y() >= row.bounds.y()
                    && node.bounds.max_y() <= row.bounds.max_y()
            })
            .expect("render skill title semantics");
        let expected_inset = settings_control_theme(&shared, ControlSize::Small)
            .metrics
            .data_row_padding
            .left;
        assert!(
            title.bounds.x() >= row.bounds.x() + expected_inset - 0.5,
            "skill title must clear the leading selection indicator: row={:?}, title={:?}",
            row.bounds,
            title.bounds
        );

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

        Ok(())
    }

    #[test]
    fn agent_settings_state_saves_renames_and_deletes_custom_agent() {
        let backend = backend_with_mounted_agent_catalog();
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
        state.set_agent_home_server_node_id("home-node".to_string());
        state.set_agent_skills_text("docs.skill\nreview.skill".to_string());
        let default_tool_count = state.agent_form.definition.tools.len();
        let added_tool = available_agent_tool_specs(
            &state.agent_snapshot.available_tools,
            &state.agent_form.definition.tools,
        )
        .get(state.agent_form.tool_add_index)
        .expect("an additional agent tool")
        .name
        .clone();
        state.add_selected_agent_tool();

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
        let saved = state
            .agent_snapshot
            .selected_agent
            .as_ref()
            .expect("saved agent definition");
        assert_eq!(
            saved.placement.home_server_node_id.as_deref(),
            Some("home-node")
        );
        assert_eq!(saved.tools.len(), default_tool_count + 1);
        assert!(saved.tools.iter().any(|tool| tool.name == added_tool));
        let summaries = agent_summaries_for_backend(&backend);
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
        let summaries = agent_summaries_for_backend(&backend);
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
        let summaries = agent_summaries_for_backend(&backend);
        assert!(!summaries.iter().any(|agent| agent.key == "docs-agent-v2"));
    }

    #[test]
    fn agent_form_round_trips_mcp_server_configuration() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.create_agent();
        state.add_agent_mcp_server();
        let ui_id = state.agent_form.mcp_servers[0].ui_id;
        assert_eq!(
            state.agent_form.mcp_servers[0].protocol_era_index,
            mcp_protocol_era_index(AgentMcpProtocolEra::Auto),
            "new servers probe for the stateless protocol by default"
        );
        state.update_agent_mcp_server(ui_id, |server| {
            server.name = "workspace".to_string();
            server.url = "https://mcp.example.test/tools".to_string();
            server.required = true;
            server.protocol_era_index = mcp_protocol_era_index(AgentMcpProtocolEra::Legacy);
            server.allowed_tools_text = "read_file\nedit_file".to_string();
            server.bearer_token_env = "SINOMO_MCP_WORKSPACE_TOKEN".to_string();
            server.headers_from_env_text = "X-Tenant: SINOMO_MCP_WORKSPACE_TENANT".to_string();
            server.startup_timeout_ms = "2500".to_string();
            server.tool_timeout_ms = "45000".to_string();
        });

        let definition = state.agent_form.to_definition().expect("MCP definition");
        let server = definition
            .tool_runtime
            .mcp_servers
            .first()
            .expect("MCP server");
        assert_eq!(server.name, "workspace");
        assert_eq!(server.url, "https://mcp.example.test/tools");
        assert!(server.enabled);
        assert!(server.required);
        assert_eq!(server.allowed_tools, ["read_file", "edit_file"]);
        assert_eq!(
            server.bearer_token_env.as_deref(),
            Some("SINOMO_MCP_WORKSPACE_TOKEN")
        );
        assert_eq!(
            server.headers_from_env.get("X-Tenant").map(String::as_str),
            Some("SINOMO_MCP_WORKSPACE_TENANT")
        );
        assert_eq!(server.startup_timeout_ms, 2500);
        assert_eq!(server.tool_timeout_ms, 45000);
        assert_eq!(server.protocol_era, AgentMcpProtocolEra::Legacy);
        assert!(
            agent_mcp_servers_summary(&definition).contains("legacy protocol"),
            "a pinned protocol is visible without opening the editor"
        );

        let reloaded = AgentMcpServerForm::from_definition(ui_id, server);
        assert_eq!(
            reloaded.protocol_era_index,
            mcp_protocol_era_index(AgentMcpProtocolEra::Legacy)
        );

        state.remove_agent_mcp_server(ui_id);
        assert!(state.agent_form.mcp_servers.is_empty());
    }

    #[test]
    fn tool_mode_labels_expose_shared_chat_and_voice_implementations() {
        assert_eq!(
            agent_tool_invocation_mode_label("read_file"),
            "Chat + voice"
        );
        assert_eq!(
            agent_tool_invocation_mode_label("request_user_input"),
            "Chat"
        );
        for hosted_tool in ["web_search", "image_generation"] {
            assert_eq!(
                agent_tool_invocation_mode_label(hosted_tool),
                "Chat + voice"
            );
        }
        let available = available_agent_tool_specs(&fallback_agent_tool_descriptors(), &[]);
        assert!(available.iter().any(|tool| tool.name == "web_search"));
        assert!(available.iter().any(|tool| tool.name == "image_generation"));
    }

    #[test]
    fn agent_settings_state_imports_exports_and_reports_readiness() {
        let backend = backend_with_mounted_agent_catalog();
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

    /// Renders `section`, applies each edit to the model, and checks that the named controls
    /// keep their widget identity: edits update retained widgets instead of remounting them.
    fn assert_edits_keep_controls_mounted(
        section: SettingsSection,
        controls: &[&str],
        prepare: impl FnOnce(&Shared),
        edits: &[(&str, &dyn Fn(&Shared))],
    ) -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(section);
        }
        prepare(&shared);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Retained settings controls")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(sui::Event::Window(sui::WindowEvent::Resized(Size::new(
                1440.0, 1600.0,
            ))))?;
        app.run_until_idle()?;
        let ids = || -> sui::Result<Vec<_>> {
            let snapshot = window.snapshot()?;
            Ok(controls
                .iter()
                .map(|name| {
                    snapshot
                        .accessibility
                        .nodes
                        .iter()
                        .find(|node| node.name.as_deref() == Some(*name))
                        .unwrap_or_else(|| panic!("{name:?} should be present"))
                        .id
                })
                .collect())
        };
        let before = ids()?;
        for (label, edit) in edits {
            edit(&shared);
            app.run_until_idle()?;
            window.advance_time(1.0)?;
            assert_eq!(ids()?, before, "{label} must not remount {controls:?}");
        }
        Ok(())
    }

    #[test]
    fn agent_editor_edits_keep_its_fields_mounted() -> sui::Result<()> {
        assert_edits_keep_controls_mounted(
            SettingsSection::Agents,
            &["agent-name", "agent-system-prompt", "agent-import-path"],
            |shared| shared.settings_mut().create_agent(),
            &[
                ("typing a name", &|shared: &Shared| {
                    shared.settings_mut().set_agent_name("Renamed".to_string())
                }),
                ("adding an MCP server", &|shared: &Shared| {
                    shared.settings_mut().add_agent_mcp_server()
                }),
                ("adding a packaged tool", &|shared: &Shared| {
                    shared.settings_mut().add_selected_agent_tool()
                }),
                ("an action error", &|shared: &Shared| {
                    shared.settings_mut().agent_action_error = Some("check failed".to_string())
                }),
            ],
        )
    }

    #[test]
    fn provider_add_form_stays_mounted_while_choosing_a_provider() -> sui::Result<()> {
        assert_edits_keep_controls_mounted(
            SettingsSection::Providers,
            &[
                "provider-add-account",
                "provider-add-model",
                "provider-add-provider",
            ],
            |shared| shared.settings_mut().open_provider_add(),
            &[
                ("picking another provider", &|shared: &Shared| {
                    shared.settings_mut().set_provider_add_provider(1)
                }),
                ("typing an account", &|shared: &Shared| {
                    shared
                        .settings_mut()
                        .set_provider_add_account("work".to_string())
                }),
            ],
        )
    }

    #[test]
    fn configuration_search_and_filters_keep_the_browser_mounted() -> sui::Result<()> {
        assert_edits_keep_controls_mounted(
            SettingsSection::Configuration,
            &["advanced-config-search", "configuration-scope"],
            |_| {},
            &[
                ("searching", &|shared: &Shared| {
                    shared.settings_mut().set_advanced_query("lm".to_string())
                }),
                ("filtering overrides", &|shared: &Shared| {
                    shared.settings_mut().set_advanced_overrides_only(true)
                }),
                ("clearing filters", &|shared: &Shared| {
                    shared.settings_mut().clear_advanced_filters()
                }),
            ],
        )
    }

    #[test]
    fn settings_agents_section_renders_management_controls() -> sui::Result<()> {
        let backend = backend_with_mounted_agent_catalog();
        let mut definition =
            AgentDefinition::custom("render-agent", AgentRole::Main, "Render settings detail.");
        definition.description = "Rendered custom agent".to_string();
        let mut mcp = AgentMcpServerDefinition::streamable_http(
            "workspace",
            "https://mcp.example.test/tools",
        );
        mcp.required = true;
        mcp.allowed_tools = vec!["read_file".to_string(), "edit_file".to_string()];
        definition.tool_runtime.mcp_servers.push(mcp);
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
        assert_agent_rows_in_view(&window, &["render-agent"])?;

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
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-temperature")
            .expect()
            .to_have_value("")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-home-server")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-home-path")
            .expect()
            .to_have_value("/sinomo/agents/render-agent/home")?;
        let render_summary = shared
            .settings()
            .agent_snapshot
            .agents
            .iter()
            .find(|agent| agent.id == "render-agent")
            .cloned()
            .expect("render agent summary");
        assert_eq!(
            agent_summary_line(&render_summary),
            "main · Cluster default · 4 tools · 1 MCP server · custom",
            "the agent menu detail should expose role, model policy, and source"
        );
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
        let snapshot = window.snapshot()?;
        let agent_list = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::List
                    && node.name.as_deref() == Some("agent-config-list")
            })
            .expect("agent list semantics");
        let agent_editor = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::TextInput
                    && node.name.as_deref() == Some("agent-id")
            })
            .expect("agent editor semantics");
        let agent_row = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::ListItem
                    && node.name.as_deref() == Some("render-agent")
            })
            .expect("render agent row semantics");
        let agent_title = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::Text
                    && node.name.as_deref() == Some("render-agent")
                    && node.bounds.y() >= agent_row.bounds.y()
                    && node.bounds.max_y() <= agent_row.bounds.max_y()
            })
            .expect("render agent title semantics");
        let expected_inset = settings_control_theme(&shared, ControlSize::Small)
            .metrics
            .data_row_padding
            .left;
        assert!(
            agent_title.bounds.x() >= agent_row.bounds.x() + expected_inset - 0.5,
            "agent title must clear the leading selection indicator: row={:?}, title={:?}",
            agent_row.bounds,
            agent_title.bounds
        );
        assert!(
            agent_list.bounds.max_x() < agent_editor.bounds.x(),
            "wide agent settings should place the second-level agent menu left of the editor"
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Add tool")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-mcp-0-name")
            .expect()
            .to_have_value("workspace")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-mcp-0-url")
            .expect()
            .to_have_value("https://mcp.example.test/tools")?;
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Add MCP server")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Host access settings")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Allow external integrations…")
                .count()?,
            0
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open home folder")
            .expect()
            .to_be_visible()?;
        open_agent_home_folder(&shared, "/sinomo/agents/render-agent/home".to_string());
        assert_eq!(shared.borrow().active_tab, Tab::Files);
        assert_eq!(
            shared.files().source,
            crate::ui::files::FilesSourceKind::Sifs
        );
        assert_eq!(shared.files().path, "/sinomo/agents/render-agent/home");

        Ok(())
    }

    #[test]
    fn node_external_integrations_control_requires_explicit_confirmation() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        shared.settings_mut().create_agent();
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Agent external integrations")
                    .root(ScrollView::vertical(section_remote_management(
                        &shared, &reader,
                    ))),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Allow external integrations…")
            .expect()
            .to_have_count(1)?;
        shared.settings_mut().confirm_external_integrations_enable();
        window.run_until_idle()?;
        window
            .get_by_role(sui::SemanticsRole::Dialog)
            .with_name("Allow external integrations confirmation")
            .expect()
            .to_have_count(1)?;
        assert!(shared.settings().external_integrations_enable_confirming);
        assert!(
            !shared
                .settings()
                .remote_management
                .external_integrations_enabled
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .expect()
            .to_have_count(1)?;
        shared.settings_mut().cancel_external_integrations_enable();
        window.run_until_idle()?;
        assert!(!shared.settings().external_integrations_enable_confirming);
        assert!(
            !shared
                .settings()
                .remote_management
                .external_integrations_enabled
        );

        Ok(())
    }

    #[test]
    fn settings_agents_section_drills_between_list_and_detail_when_compact() -> sui::Result<()> {
        let backend = backend_with_mounted_agent_catalog();
        let mut alpha =
            AgentDefinition::custom("alpha-agent", AgentRole::Main, "Alpha system prompt.");
        alpha.name = "Alpha Agent".to_string();
        backend
            .save_custom_agent_definition(&alpha, None)
            .expect("seed alpha agent");
        let mut zulu =
            AgentDefinition::custom("zulu-agent", AgentRole::Background, "Zulu system prompt.");
        zulu.name = "Zulu Agent".to_string();
        backend
            .save_custom_agent_definition(&zulu, None)
            .expect("seed zulu agent");
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
                    .title("Compact settings agents")
                    .root(
                        SizedBox::new()
                            .size(Size::new(760.0, 720.0))
                            .child(view(shared.clone())),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::List)
            .with_name("agent-config-list")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-id")
            .expect()
            .to_be_hidden()?;

        scroll_agent_rows_into_view(&window, &["Zulu Agent"])?;
        window
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("Zulu Agent")
            .click()?;
        assert_eq!(
            shared.settings().agent_selected_id.as_deref(),
            Some("zulu-agent")
        );
        assert!(shared.settings().agent_detail_open);
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Agents")
            .click()?;
        window
            .get_by_role(sui::SemanticsRole::List)
            .with_name("agent-config-list")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("agent-id")
            .expect()
            .to_be_hidden()?;

        Ok(())
    }

    #[test]
    fn settings_agent_reorder_preserves_virtual_row_ids() -> sui::Result<()> {
        let backend = backend_with_mounted_agent_catalog();
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
        scroll_agent_rows_into_view(&window, &["Virtual agent alpha", "Virtual agent beta"])?;
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
        let external_provider_index;
        let nfs_config_index;
        let nfs_status_index;
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
                modules: vec![
                    crate::backend::modules::ModuleConfigModuleSummary {
                        module_id: "module.external-executor".to_string(),
                        label: "External Executor".to_string(),
                        backend: "native-dylib".to_string(),
                        detail: "External sandbox executor".to_string(),
                        desired_enabled: Some(true),
                        effective_enabled: false,
                        effective_source: ModuleRuntimeConfigSource::Node,
                        observed: ModuleObservedState::Running,
                        observed_detail: "Runtime process is active".to_string(),
                    },
                    crate::backend::modules::ModuleConfigModuleSummary {
                        module_id: "module.external-filesystem".to_string(),
                        label: "External Filesystem".to_string(),
                        backend: "native-dylib".to_string(),
                        detail: "Windows SIFS filesystem projection".to_string(),
                        desired_enabled: Some(true),
                        effective_enabled: true,
                        effective_source: ModuleRuntimeConfigSource::Node,
                        observed: ModuleObservedState::Running,
                        observed_detail: "Native provider is active".to_string(),
                    },
                    crate::backend::modules::ModuleConfigModuleSummary {
                        module_id: sinomo_runtime::dfs_nfs::SINOMO_DFS_NFS_MODULE_LEGACY_ID
                            .to_string(),
                        label: sinomo_runtime::dfs_nfs::NFS_MODULE_LABEL.to_string(),
                        backend: "native-rust".to_string(),
                        detail: sinomo_runtime::dfs_nfs::NFS_MODULE_DESCRIPTION.to_string(),
                        desired_enabled: Some(true),
                        effective_enabled: true,
                        effective_source: ModuleRuntimeConfigSource::Node,
                        observed: ModuleObservedState::Running,
                        observed_detail: "NFS supervisor is active".to_string(),
                    },
                ],
                warnings: Vec::new(),
            };
            associated_index = settings.config_items.len();
            let mut associated = test_config_item(
                "test.sandbox.settings",
                ConfigEditKind::Json,
                r#"{"enabled":true,"endpoint":"https://sandbox.test"}"#,
            );
            associated.module_id = Some(
                sinomo_runtime::core::Id::from_legacy_named("module.external-executor")
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

            external_provider_index = settings.config_items.len();
            let definition = sinomo_runtime::dfs::sifs_mount_config_definition_for_module(
                "module.external-filesystem",
            );
            let default_value = definition.default_value.expect("mount mapping default");
            let default_json = serde_json::to_string(&default_value).expect("mount mapping JSON");
            let mut external_provider = test_config_item(
                sinomo_runtime::dfs::SIFS_MOUNT_CONFIG_KEY,
                ConfigEditKind::Json,
                &default_json,
            );
            external_provider.owner = definition.owner;
            external_provider.module_id = definition.module_id;
            external_provider.description = definition.description;
            external_provider.schema = definition.schema;
            settings.config_items.push(external_provider);

            nfs_config_index = settings.config_items.len();
            let definition = sinomo_runtime::dfs_nfs::nfs_mount_config_definition();
            let default_value = definition
                .default_value
                .clone()
                .expect("NFS config default");
            let mut nfs_config = test_config_item(
                sinomo_runtime::dfs_nfs::NFS_MOUNT_CONFIG_KEY,
                ConfigEditKind::Json,
                &serde_json::to_string(&default_value).expect("NFS config text"),
            );
            nfs_config.owner = definition.owner;
            nfs_config.module_id = definition.module_id;
            nfs_config.module_label = definition.module_label;
            nfs_config.module_description = definition.module_description;
            nfs_config.description = definition.description;
            nfs_config.schema = definition.schema;
            nfs_config.raw_value = default_value;
            settings.config_items.push(nfs_config);

            nfs_status_index = settings.config_items.len();
            let definition = sinomo_runtime::dfs_nfs::nfs_mount_status_config_definition();
            let status = sinomo_runtime::dfs_nfs::NfsMountRuntimeStatus::error(
                &sinomo_runtime::dfs_nfs::NfsMountConfig::default(),
                Some("/mnt/sinomo".to_string()),
                "mount-permission-required",
                "mount.nfs: Operation not permitted",
                "Grant the daemon mount authority and retry.",
            );
            let raw_value = serde_json::to_value(status).expect("NFS status JSON");
            let mut nfs_status = test_config_item(
                sinomo_runtime::dfs_nfs::NFS_MOUNT_STATUS_CONFIG_KEY,
                ConfigEditKind::Json,
                &serde_json::to_string(&raw_value).expect("NFS status text"),
            );
            nfs_status.owner = definition.owner;
            nfs_status.module_id = definition.module_id;
            nfs_status.module_label = definition.module_label;
            nfs_status.module_description = definition.module_description;
            nfs_status.operational_error = definition.operational_error;
            nfs_status.description = definition.description;
            nfs_status.schema = definition.schema;
            nfs_status.read_only = true;
            nfs_status.raw_value = raw_value;
            settings.config_items.push(nfs_status);

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
            for item in &mut settings.config_items {
                item.node_scoped = true;
            }
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings modules")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_text("External Executor")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Cluster default")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("On Studio node")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Running on Studio node")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Pending reconciliation")
            .expect()
            .to_be_visible()?;
        assert_eq!(
            window.get_by_text("Technical ID").count()?,
            0,
            "the technical ID stays behind Show details"
        );
        // Off-viewport clicks miss; make the long Modules page fit before clicking into it.
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 4000.0,
            ))))?;
        app.run_until_idle()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_description(details_toggle_description(
                "module-module.external-executor",
            ))
            .click()?;
        window
            .get_by_text("Technical ID")
            .expect()
            .to_be_visible()?;
        assert!(
            window
                .snapshot()?
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Settings · test.sandbox.settings")),
            "a canonical config module ID must match its legacy lifecycle card"
        );
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("config-{associated_index}-schema-endpoint"))
            .expect()
            .to_have_value("https://sandbox.test")?;
        window
            .get_by_text("External Filesystem")
            .expect()
            .to_be_visible()?;
        assert!(
            window
                .snapshot()?
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Settings · storage.sifs-mount")),
            "the external provider descriptor must render the shared mount mapping on its module card"
        );
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!(
                "config-{external_provider_index}-schema-mount_point"
            ))
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("config-{external_provider_index}-schema-root_path"))
            .expect()
            .to_have_value("/")?;
        window
            .get_by_text("Additional module settings")
            .expect()
            .to_be_visible()?;
        assert!(
            window
                .snapshot()?
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Settings · test.headless.settings")),
            "module settings without lifecycle cards must remain visible"
        );
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("config-{unmatched_index}-schema-workspace"))
            .expect()
            .to_have_value("/tmp/evolution")?;

        // The NFS mount is managed in Storage: Modules names it and links there, and its
        // settings are not duplicated as "additional" settings.
        window
            .get_by_text("Managed in Storage")
            .expect()
            .to_be_visible()?;
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::TextInput)
                .with_name(format!("config-{nfs_config_index}-schema-target"))
                .count()?,
            0
        );
        // Off-viewport clicks miss; make the long Modules page fit before clicking its last card.
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 4000.0,
            ))))?;
        app.run_until_idle()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open Storage")
            .click()?;
        assert_eq!(shared.settings().selected, Some(SettingsSection::Storage));
        window.get_by_text("NFS Mount").expect().to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("config-{nfs_config_index}-schema-target"))
            .expect()
            .to_have_value(&sinomo_runtime::dfs_nfs::NfsMountConfig::default().target)?;
        window
            .get_by_text("Action required")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("module-operational-error-{nfs_status_index}"))
            .expect()
            .to_have_value("mount.nfs: Operation not permitted")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(format!("module-operational-resolution-{nfs_status_index}"))
            .expect()
            .to_have_value("Grant the daemon mount authority and retry.")?;

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
        let secret_name = format!("config-{index}-schema-token");
        let secret = window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name(secret_name.clone());
        secret.expect().to_have_value("")?;
        secret.fill("sk-typed-replacement")?;
        let secret_node = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == sui::SemanticsRole::TextInput
                    && node.name.as_deref() == Some(secret_name.as_str())
            })
            .expect("schema secret input semantics");
        assert!(
            secret_node
                .editable_text
                .as_ref()
                .is_some_and(|editable| editable.password),
            "typed schema secrets must be rendered through a password input"
        );
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
    fn schema_boolean_labels_stack_clear_of_switches() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let index = {
            let mut settings = shared.settings_mut();
            let index = settings.config_items.len();
            settings.config_items.push(test_config_item(
                "test.schema-toggle",
                ConfigEditKind::Json,
                r#"{"allow_remote_local_owned_mounts":false}"#,
            ));
            index
        };
        let reader: ThemeReader = {
            let shared = shared.clone();
            Rc::new(move || shared.theme())
        };
        let label = "Allow remote local-owned mounts";
        let row = config_schema_primitive_row(
            &shared,
            &reader,
            index,
            &Value::Bool(false),
            &ConfigSchemaValue::Bool,
            vec![ConfigPathSegment::Field(
                "allow_remote_local_owned_mounts".to_string(),
            )],
            label,
            "Permit placement away from the owner of a local-owned mount.",
            false,
        );
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Schema boolean layout")
                    .root(
                        SizedBox::new()
                            .size(Size::new(320.0, 120.0))
                            .child(PodHost::new(row)),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let snapshot = window.snapshot()?;
        let form_row = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::GenericContainer
                    && node.name.as_deref() == Some(label)
            })
            .expect("boolean form row semantics");
        let toggle = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| node.role == sui::SemanticsRole::Switch)
            .expect("boolean switch semantics");
        assert!(
            toggle.bounds.y() > form_row.bounds.y() + 1.0,
            "switch must be arranged below the complete label: row={:?}, switch={:?}",
            form_row.bounds,
            toggle.bounds
        );
        assert!(
            toggle.bounds.x() >= form_row.bounds.x() - 0.5
                && toggle.bounds.max_x() <= form_row.bounds.max_x() + 0.5,
            "a narrow row stacks its switch inside the row, under the label: row={:?}, switch={:?}",
            form_row.bounds,
            toggle.bounds
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
    fn remote_management_section_exposes_copyable_local_commands() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Remote management settings")
                    .root(
                        SizedBox::new().size(Size::new(1000.0, 720.0)).child(
                            ScrollView::vertical(section_remote_management_body(&shared, &reader))
                                .name("remote-management-settings-scroll")
                                .state(ScrollState::new())
                                .theme(reader()),
                        ),
                    ),
            )
            .build()?;
        let clipboard = runtime.clipboard();
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(window.get_by_text("Remote management").count()? >= 1);
        assert!(window.get_by_text("Remote management module").count()? >= 1);
        assert!(window.get_by_text("Other nodes").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::ScrollView)
            .with_name("remote-management-settings-scroll")
            .scroll_pixels(sui::Vector::new(0.0, -800.0))?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy Allow host permission command")
            .click()?;
        assert_eq!(
            clipboard.text().as_deref(),
            Some(REMOTE_MANAGEMENT_ALLOW_COMMAND)
        );

        Ok(())
    }

    #[test]
    fn remote_management_allow_control_opens_an_explicit_confirmation() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Remote management confirmation")
                    .root(section_remote_management(&shared, &reader)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Allow remote management…")
            .click()?;
        // The confirmation grows into place; let the reveal finish before pressing its buttons.
        window.advance_time(1.0)?;
        window
            .get_by_role(sui::SemanticsRole::Dialog)
            .with_name("Allow remote management confirmation")
            .expect()
            .to_have_count(1)?;
        assert!(
            !shared.settings().remote_management.permission_enabled,
            "opening confirmation must not grant permission"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;
        assert!(!shared.settings().remote_management_enable_confirming);

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
        window.get_by_text("Text size").expect().to_be_visible()?;

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
                .position(|provider| provider_requires_api_key(provider))
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
                .position(|provider| provider_uses_device_login(provider))
                .expect("device-login provider"),
        );
        assert!(
            state.provider_add_can_submit(),
            "device-login providers collect credentials after the account is added"
        );

        state.set_provider_add_provider(
            CONFIGURABLE_PROVIDERS
                .iter()
                .position(|provider| *provider == sinomo_runtime::api::lm::OLLAMA_PROVIDER_ID)
                .expect("Ollama provider"),
        );
        assert!(
            state.provider_add_can_submit(),
            "local Responses providers should not require an API key"
        );
    }

    #[test]
    fn provider_capabilities_surface_hosted_realtime_and_local_features() {
        let openai = provider_capabilities_summary("openai");
        assert!(openai.contains("Responses API"));
        assert!(openai.contains("Realtime voice"));
        assert!(openai.contains("Web search"));
        assert!(openai.contains("Image generation + editing"));
        assert!(openai.contains("MCP API"));

        assert_eq!(
            provider_capabilities_summary(sinomo_runtime::api::lm::OLLAMA_PROVIDER_ID),
            "Responses API · Local model discovery"
        );
    }

    #[test]
    fn platform_back_closes_provider_form_before_settings_section() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.open_provider_add();

        assert!(state.navigate_back());
        assert!(!state.provider_add_open);
        assert_eq!(state.selected, Some(SettingsSection::Providers));

        assert!(state.navigate_back());
        assert_eq!(state.selected, None);
        assert!(!state.navigate_back());
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
                .any(|module| module_ids_match(
                    &module.module_id,
                    sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID,
                )),
            "module settings should include the built-in DFS target"
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
                module_id: sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID.to_string(),
                enabled: Some(false),
                native_rust: true,
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
                .setting(sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID)
                .map(|setting| setting.enabled),
            Some(false)
        );
        assert!(
            !sinomo_runtime::dfs::dfs_module_enabled(backend.data_dir.root())
                .expect("read DFS module state")
        );
        assert!(
            state
                .module_action_status
                .as_deref()
                .is_some_and(|status| status.contains("restart that node's daemon"))
        );
    }

    #[test]
    fn device_sharing_switches_follow_refreshed_state_without_remounting() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        shared
            .settings_mut()
            .remote_management
            .shared_devices
            .clear();
        let reader = shared_reader(&shared);
        let runtime = sui::Application::new()
            .window(sui::WindowBuilder::new().root(ScrollView::vertical(
                section_remote_management_body(&shared, &reader),
            )))
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let switches = || -> sui::Result<Vec<_>> {
            Ok(window
                .snapshot()?
                .accessibility
                .nodes
                .into_iter()
                .filter(|node| {
                    node.name
                        .as_deref()
                        .is_some_and(|name| name.starts_with("Share "))
                })
                .map(|node| (node.id, node.name.unwrap(), node.state.checked))
                .collect())
        };
        let initial = switches()?;
        assert_eq!(initial.len(), 4);
        assert!(
            initial
                .iter()
                .all(|(_, _, state)| *state == Some(sui::ToggleState::Unchecked))
        );
        for (devices, expected) in [
            (vec!["microphone".to_string()], vec!["Share Microphone"]),
            (
                vec!["camera".to_string(), "speaker".to_string()],
                vec!["Share Camera", "Share Speaker"],
            ),
            (Vec::new(), Vec::new()),
        ] {
            shared.settings_mut().remote_management.shared_devices = devices;
            let current = switches()?;
            assert_eq!(current.len(), initial.len());
            for ((id, name, state), (original_id, original_name, _)) in current.iter().zip(&initial)
            {
                assert_eq!((id, name), (original_id, original_name));
                assert_eq!(
                    *state,
                    Some(if expected.contains(&name.as_str()) {
                        sui::ToggleState::Checked
                    } else {
                        sui::ToggleState::Unchecked
                    })
                );
            }
        }
        assert!(!shared.settings().remote_management.permission_enabled);
        Ok(())
    }

    #[test]
    fn remote_management_permission_requires_confirmation_and_revokes_immediately() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        assert!(!state.remote_management.permission_enabled);

        state.confirm_remote_management_enable();
        assert!(state.remote_management_enable_confirming);
        state.cancel_remote_management_enable();
        assert!(!state.remote_management_enable_confirming);
        assert!(
            !backend
                .remote_management_settings_snapshot()
                .permission_enabled,
            "cancelling confirmation must not write permission state"
        );

        state.confirm_remote_management_enable();
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::RemoteManagementPermission,
            SettingsOperation::SetRemoteManagementPermission { enabled: true },
        );
        assert!(state.remote_management.permission_enabled);
        assert!(!state.remote_management_enable_confirming);

        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::RemoteManagementPermission,
            SettingsOperation::SetRemoteManagementPermission { enabled: false },
        );
        assert!(!state.remote_management.permission_enabled);
    }

    #[test]
    fn external_integrations_permission_requires_confirmation_and_revokes_immediately() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        assert!(!state.remote_management.external_integrations_enabled);

        state.confirm_external_integrations_enable();
        assert!(state.external_integrations_enable_confirming);
        state.cancel_external_integrations_enable();
        assert!(!state.external_integrations_enable_confirming);
        assert!(
            !backend
                .remote_management_settings_snapshot()
                .external_integrations_enabled,
            "cancelling confirmation must not grant MCP authority"
        );

        state.confirm_external_integrations_enable();
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::ExternalIntegrationsPermission,
            SettingsOperation::SetExternalIntegrationsPermission { enabled: true },
        );
        assert!(state.remote_management.external_integrations_enabled);
        assert!(!state.external_integrations_enable_confirming);
        assert!(!state.remote_management.permission_enabled);

        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::ExternalIntegrationsPermission,
            SettingsOperation::SetExternalIntegrationsPermission { enabled: false },
        );
        assert!(!state.remote_management.external_integrations_enabled);
    }

    #[test]
    fn remote_management_module_shortcut_sets_local_node_override() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);

        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::RemoteManagementModule,
            SettingsOperation::SetRemoteManagementModuleOverride {
                enabled: Some(true),
            },
        );

        assert_eq!(state.remote_management.module_override, Some(true));
        assert!(state.remote_management.module_effective_enabled);
        assert_eq!(
            state.remote_management.module_effective_source,
            ModuleRuntimeConfigSource::Node
        );
        assert_eq!(
            state
                .module_snapshot
                .selected_node
                .as_ref()
                .and_then(|node| {
                    node.config
                        .setting(sinomo_runtime::remote_management::REMOTE_MANAGEMENT_MODULE_ID)
                        .map(|setting| setting.enabled)
                }),
            Some(true),
            "the full Modules page must receive the shortcut's local override"
        );

        let node_id = state
            .module_selected_node_id
            .clone()
            .expect("local module node");
        apply_test_operation(
            &mut state,
            &backend,
            SettingsOperationScope::Modules,
            SettingsOperation::SetNodeModuleOverride {
                node_id,
                module_id: sinomo_runtime::remote_management::REMOTE_MANAGEMENT_MODULE_ID
                    .to_string(),
                enabled: Some(false),
                native_rust: false,
            },
        );
        assert_eq!(
            state.remote_management.module_override,
            Some(false),
            "the shortcut must receive local changes made from the full Modules page"
        );
    }

    #[test]
    fn remote_management_preview_requires_observed_activation_before_saying_active() {
        let backend = Backend::local_fallback();
        backend
            .set_local_remote_management_permission(true)
            .expect("allow local permission");
        backend
            .set_local_remote_management_module_override(Some(true))
            .expect("request local module enablement");
        let shared = Shared::new(AppState::ready(backend));

        assert_eq!(
            section_preview(&shared, SettingsSection::RemoteManagement),
            "Permission allowed"
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
            window
                .snapshot()?
                .accessibility
                .nodes
                .iter()
                .any(|node| node
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("API key stored"))),
            "the connection row should report credential presence separately"
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
                    usage: Some(sinomo_runtime::api::lm::LanguageModelProviderUsage {
                        provider: "openai".to_string(),
                        plan: Some("Pro".to_string()),
                        captured_at_unix: now as u64,
                        limits: vec![sinomo_runtime::api::lm::LanguageModelUsageLimit {
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
        assert!(window.get_by_text("Pro - 64% used").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::ProgressBar)
            .with_name("Five-hour limit usage")
            .expect()
            .to_be_hidden()?;
        let collapsed_signature = provider_section_signature(&shared);
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("View usage details")
            .click()?;
        assert_ne!(
            collapsed_signature,
            provider_section_signature(&shared),
            "usage disclosure state must invalidate the retained section"
        );
        // The details grow into place; let the reveal finish before measuring them.
        window.advance_time(1.0)?;
        assert!(window.get_by_text("Five-hour limit · 64% used").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::ProgressBar)
            .with_name("Five-hour limit usage")
            .expect()
            .to_have_value("64")?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide usage details")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::ComboBox)
            .with_name("provider-0-model-select")
            .expect()
            .to_be_visible()?;
        let snapshot = window.snapshot()?;
        let card = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::GenericContainer
                    && node.name.as_deref() == Some("OpenAI (work)")
            })
            .expect("provider card semantics should exist");
        let model = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::ComboBox
                    && node.name.as_deref() == Some("provider-0-model-select")
            })
            .expect("provider model selector semantics should exist");
        let model_row = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::GenericContainer
                    && node.name.as_deref() == Some("Model")
            })
            .expect("provider model row semantics should exist");
        assert!(
            model_row.bounds.width() > card.bounds.width() * 0.9
                && (model.bounds.max_x() - model_row.bounds.max_x()).abs() < 0.01,
            "provider settings controls should use the full card content width"
        );
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-model")
            .expect()
            .to_be_hidden()?;

        Ok(())
    }

    #[test]
    fn provider_card_uses_dropdown_for_known_catalog_before_connection_test() -> sui::Result<()> {
        let fixture = sinomo_testdir::TestDir::new(
            "app-ui-provider-card-uses-dropdown-for-known-catalog-before-connection-test",
        );
        let backend = Backend::local_fallback_for_data_dir(
            sinomo_runtime::local::SinomoDataDir::new(fixture.path()),
        );
        let shared = Shared::new(AppState::ready(backend));
        {
            let config = shared.settings().config.clone();
            add_provider_account(
                config.as_ref(),
                "deepseek",
                "default",
                "sk-test",
                sinomo_runtime::api::lm::DEEPSEEK_DEFAULT_MODEL,
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
        // The control sits below the default 720px test viewport; off-viewport clicks miss.
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 2400.0,
            ))))?;
        app.run_until_idle()?;

        assert!(
            window.get_by_text("Default for chat").count()? >= 1,
            "provider card should identify the current chat default"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Set as chat default")
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
            .get_by_role(sui::SemanticsRole::ComboBox)
            .with_name("provider-0-model-select")
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
            .with_name("Advanced settings")
            .click()?;

        assert_ne!(
            collapsed_signature,
            provider_section_signature(&shared),
            "provider disclosure state must invalidate the retained section"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide advanced settings")
            .expect()
            .to_be_visible()?;
        assert!(window.get_by_text("CREDENTIALS").count()? >= 1);
        assert!(window.get_by_text("ENDPOINT").count()? >= 1);
        assert!(window.get_by_text("ACCOUNT").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-model")
            .expect()
            .to_have_value("gpt-5")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-base-url")
            .expect()
            .to_have_value("")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-max-context-tokens")
            .expect()
            .to_have_value("")?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-auto-compact-token-limit")
            .expect()
            .to_have_value("")?;
        assert!(window.get_by_text("CONTEXT AND COST").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Remove account")
            .expect()
            .to_be_visible()?;

        Ok(())
    }

    #[test]
    fn provider_card_saves_advanced_text_fields_inline() -> sui::Result<()> {
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
                    .title("Settings provider inline save")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        // The control sits below the default 720px test viewport; off-viewport clicks miss.
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 2400.0,
            ))))?;
        app.run_until_idle()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Advanced settings")
            .click()?;
        let save = window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Save API base URL");
        save.expect().to_be_visible()?;
        assert!(
            save.click().is_err(),
            "an unchanged inline field must expose a disabled save action"
        );
        assert!(
            shared.settings().provider_action_status.is_none(),
            "an unchanged inline field must not queue a save"
        );

        let field = window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("provider-0-base-url");
        field.fill("https://example.test/v1")?;
        field.expect().to_be_focused()?;
        save.click()?;

        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            if shared
                .settings()
                .provider_action_status
                .as_ref()
                .is_some_and(|status| status.message == "API base URL saved to lm.providers")
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
            Some("API base URL saved to lm.providers")
        );

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
            window.get_by_text("No provider accounts yet").count()? >= 1,
            "empty provider state should say what is missing"
        );
        assert!(
            window
                .get_by_text(
                    "Add an account to start chatting. Providers sign in with an API key or a device login."
                )
                .count()?
                >= 1,
            "empty provider state should explain the user-facing outcome"
        );
        assert_eq!(
            window.get_by_text("lm.providers").count()?,
            0,
            "low-level storage detail should stay collapsed by default"
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_description(details_toggle_description("providers-storage"))
            .click()?;
        assert!(
            window.get_by_text("lm.providers").count()? >= 1,
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
            settings.open(SettingsSection::System);
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
            window.get_by_text("Connection").count()? >= 1,
            "System should lead with the connection"
        );
        assert!(window.get_by_text("Cluster").count()? >= 1);
        assert!(
            window
                .get_by_text("Connected to daemon at 127.0.0.1:21353")
                .count()?
                >= 1
        );
        assert!(window.get_by_text("cluster test · voter").count()? >= 1);
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::TextInput)
                .with_name("system-app-surface-id")
                .count()?,
            0,
            "identifiers stay behind Show details"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_description(details_toggle_description("system-connection"))
            .click()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("system-app-surface-id")
            .expect()
            .to_have_value("node_about_test")?;
        assert!(window.get_by_text("voter app surface").count()? >= 1);
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("system-daemon-node-id")
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

        assert!(
            window.get_by_text("Display").count()? >= 1,
            "Appearance should expose render settings as a first-class card"
        );
        window
            .get_by_role(sui::SemanticsRole::ComboBox)
            .with_name("render-color-output")
            .expect()
            .to_have_value("Force SDR")?;
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
            module_label: None,
            module_description: None,
            operational_error: None,
            description: "test setting".to_string(),
            value: raw.to_string(),
            source: "cluster",
            node_scoped: false,
            scope: "cluster",
            bool_value: None,
            edit_kind: kind,
            edit_value: raw.to_string(),
            raw_value,
            schema: None,
            read_only: false,
        }
    }

    #[test]
    fn module_operational_error_uses_registered_field_paths() {
        let mut item = test_config_item(
            "storage.test-status",
            ConfigEditKind::Json,
            r#"{"failure":"mount.nfs: Operation not permitted","fix":"Grant mount authority and retry."}"#,
        );
        item.read_only = true;
        item.operational_error = Some(sinomo_runtime::config::ConfigOperationalErrorPresentation {
            error_field: "failure".to_string(),
            resolution_field: "fix".to_string(),
        });
        assert_eq!(
            module_operational_error(&item),
            Some((
                "mount.nfs: Operation not permitted".to_string(),
                "Grant mount authority and retry.".to_string(),
            ))
        );
    }

    #[test]
    fn configuration_scope_switches_between_cluster_and_device_settings() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        shared.settings_mut().open(SettingsSection::Configuration);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings configuration scope")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let node_picker = || {
            window
                .get_by_role(SemanticsRole::ComboBox)
                .with_name("settings-node-selector")
                .count()
        };
        assert_eq!(node_picker()?, 0, "cluster scope has no node to pick");

        window
            .get_by_role(SemanticsRole::RadioButton)
            .with_name(CONFIGURATION_SCOPES[1])
            .click()?;
        app.run_until_idle()?;
        assert!(shared.settings().configuration_node_scope);
        assert_eq!(
            node_picker()?,
            1,
            "device scope picks the node to configure"
        );

        window
            .get_by_role(SemanticsRole::RadioButton)
            .with_name(CONFIGURATION_SCOPES[0])
            .click()?;
        app.run_until_idle()?;
        assert!(!shared.settings().configuration_node_scope);
        assert_eq!(node_picker()?, 0);
        Ok(())
    }

    #[test]
    fn storage_takes_over_the_mount_modules_and_edits_only_nfs_settings_itself() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.config_items.clear();
        let mut sifs = test_config_item(SIFS_MOUNT_CONFIG_KEY, ConfigEditKind::Json, "{}");
        sifs.module_id = Some("module.external-filesystem".to_string());
        let mut nfs = test_config_item(NFS_MOUNT_CONFIG_KEY, ConfigEditKind::Json, "{}");
        nfs.module_id = Some("module.dfs-nfs".to_string());
        let mut other = test_config_item("test.other", ConfigEditKind::Json, "{}");
        other.module_id = Some("module.other".to_string());
        state.config_items = vec![sifs, nfs, other];

        assert_eq!(
            storage_modules(&state),
            vec![
                ("module.external-filesystem".to_string(), false),
                ("module.dfs-nfs".to_string(), true),
            ],
            "the SIFS mount module's settings are edited by the mount card; NFS brings its own"
        );
    }

    #[test]
    fn settings_navigation_groups_cover_each_section_once() {
        let grouped = SETTINGS_GROUPS
            .iter()
            .flat_map(|group| group.sections.iter().copied())
            .collect::<Vec<_>>();

        assert_eq!(grouped.len(), SettingsSection::ALL.len());
        let unique = grouped.iter().copied().collect::<HashSet<_>>();
        assert_eq!(unique.len(), SettingsSection::ALL.len());
        assert!(
            SETTINGS_GROUPS[0]
                .sections
                .contains(&SettingsSection::System)
        );
        for section in grouped {
            assert!(!section.title().trim().is_empty());
            assert!(!section.description().trim().is_empty());
        }
    }

    #[test]
    fn advanced_search_matches_catalog_metadata_but_not_resolved_values() {
        let mut item = test_config_item(
            "dfs.placement",
            ConfigEditKind::Json,
            r#"{"api_key":"sk-search-must-not-leak"}"#,
        );
        item.owner = "sinomo-dfs".to_string();
        item.description = "Controls durable file placement".to_string();
        item.schema = Some(ConfigSchema::object(
            "PlacementSettings",
            "Placement policy metadata",
            vec![ConfigSchemaField::new(
                "replicas",
                "Replica count",
                "Number of durable copies",
                ConfigSchemaValue::u64(None),
            )],
        ));

        assert!(config_item_matches_query(&item, "DFS durable"));
        assert!(config_item_matches_query(&item, "replica copies"));
        assert!(config_item_matches_query(&item, "placement policy"));
        assert!(
            !config_item_matches_query(&item, "sk-search-must-not-leak"),
            "resolved values must never become a search oracle for secrets"
        );
    }

    #[test]
    fn advanced_overrides_filter_preserves_original_catalog_indices_and_dirty_drafts() {
        let backend = Backend::local_fallback();
        let mut state = SettingsState::new(&backend);
        state.config_items = vec![
            test_config_item("test.default", ConfigEditKind::Text, "default"),
            test_config_item("test.unset", ConfigEditKind::Text, "unset"),
            test_config_item("test.local", ConfigEditKind::Text, "local"),
            test_config_item("test.cluster", ConfigEditKind::Text, "cluster"),
            test_config_item("test.dirty", ConfigEditKind::Text, "before"),
        ];
        state.config_items[0].source = "default";
        state.config_items[1].source = "unset";
        state.config_items[2].source = "local";
        state.config_items[3].source = "cluster";
        state.config_items[4].source = "default";
        state.set_config_value_draft(4, "after".to_string());
        state.set_advanced_overrides_only(true);

        let visible = visible_config_groups(&state)
            .into_iter()
            .flat_map(|(_, indices)| indices)
            .collect::<Vec<_>>();
        assert_eq!(visible, vec![2, 3, 4]);

        state.set_advanced_query("test.cluster".to_string());
        let searched = visible_config_groups(&state)
            .into_iter()
            .flat_map(|(_, indices)| indices)
            .collect::<Vec<_>>();
        assert_eq!(searched, vec![3]);
    }

    #[test]
    fn configuration_structure_tracks_browser_state_and_catalog_copy() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut settings = shared.settings_mut();
            settings.set_advanced_query("placement".to_string());
            settings.set_advanced_overrides_only(true);
            let mut item = test_config_item("test.catalog", ConfigEditKind::Text, "value");
            item.description = "Updated catalog description".to_string();
            settings.config_items = vec![item];
        }

        let key = settings_structure_key(&shared);
        let configuration = &key.0.as_deref().expect("settings snapshot").configuration;
        assert!(!configuration.query_empty);
        assert!(configuration.overrides_only);
        assert!(
            configuration
                .items
                .iter()
                .any(|item| item.description == "Updated catalog description")
        );
    }

    #[test]
    fn advanced_search_keeps_focus_while_filtering_results() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::Configuration);
            settings.config_error = None;
            settings.config_items = vec![
                test_config_item("alpha.visible", ConfigEditKind::Text, "one"),
                test_config_item("beta.hidden", ConfigEditKind::Text, "two"),
            ];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings configuration search")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let search = window
            .get_by_role(SemanticsRole::TextInput)
            .with_name("advanced-config-search");

        search.focus()?;
        search.fill("alpha")?;
        search.expect().to_be_focused()?;
        search.expect().to_have_value("alpha")?;
        window
            .get_by_text("alpha.visible")
            .expect()
            .to_be_visible()?;
        window.get_by_text("beta.hidden").expect().to_be_hidden()?;
        assert_eq!(shared.settings().advanced_query, "alpha");

        Ok(())
    }

    #[test]
    fn advanced_filter_controls_sync_after_clear_and_breakpoint_changes() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.open(SettingsSection::Configuration);
            settings.config_error = None;
            settings.config_items = vec![
                test_config_item("alpha.visible", ConfigEditKind::Text, "one"),
                test_config_item("beta.visible", ConfigEditKind::Text, "two"),
            ];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings synchronized filters")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let search = || {
            window
                .get_by_role(SemanticsRole::TextInput)
                .with_name("advanced-config-search")
        };
        let overrides = || {
            window
                .get_by_role(SemanticsRole::CheckBox)
                .with_name("Show only local or cluster overrides")
        };

        search().fill("no-match")?;
        overrides().click()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Clear filters")
            .click()?;
        search().expect().to_have_value("")?;
        let checkbox_state = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == SemanticsRole::CheckBox
                    && node.name.as_deref() == Some("Show only local or cluster overrides")
            })
            .and_then(|node| node.state.checked);
        assert_eq!(checkbox_state, Some(sui::ToggleState::Unchecked));
        assert!(shared.settings().advanced_query.is_empty());
        assert!(!shared.settings().advanced_overrides_only);

        search().fill("alpha")?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                520.0, 720.0,
            ))))?;
        app.run_until_idle()?;
        search().expect().to_have_value("alpha")?;
        search().focus()?;
        let compact_search_id = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == SemanticsRole::TextInput
                    && node.name.as_deref() == Some("advanced-config-search")
            })
            .expect("compact advanced search semantics")
            .id;

        shared.settings_mut().set_advanced_query("beta".to_string());
        shared.publish_settings_signals();
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 720.0,
            ))))?;
        app.run_until_idle()?;
        search().expect().to_have_value("beta")?;

        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                520.0, 720.0,
            ))))?;
        app.run_until_idle()?;
        search().expect().to_have_value("beta")?;
        search().expect().to_be_focused()?;
        let restored_search_id = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == SemanticsRole::TextInput
                    && node.name.as_deref() == Some("advanced-config-search")
            })
            .expect("restored compact advanced search semantics")
            .id;
        assert_eq!(
            restored_search_id, compact_search_id,
            "the retained compact settings branch should keep its widget identity"
        );

        Ok(())
    }

    #[test]
    fn settings_navigation_remains_single_line_when_status_changes() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            let settings = &mut app.ready.as_mut().expect("ready state").settings;
            settings.provider_forms.clear();
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings live navigation previews")
                    .root(
                        SizedBox::new()
                            .size(Size::new(1280.0, 420.0))
                            .child(view(shared.clone())),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let provider_row = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == SemanticsRole::ListItem && node.name.as_deref() == Some("Providers")
            })
            .expect("Providers navigation row");
        assert!(provider_row.bounds.height() <= SIDEBAR_ITEM_H + 0.1);

        let snapshot = window.snapshot()?;
        let app_group = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::List && node.name.as_deref() == Some("settings-general")
            })
            .expect("General group");
        let about = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::ListItem && node.name.as_deref() == Some("System")
            })
            .expect("System navigation row");
        assert!(
            about.bounds.y() >= app_group.bounds.y()
                && about.bounds.max_y() <= app_group.bounds.max_y(),
            "System belongs inside the General group"
        );
        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("System")
            .click()?;
        let about_list = window
            .get_by_role(SemanticsRole::List)
            .with_name("settings-general");
        about_list.expect().to_be_focused()?;

        let config = shared.settings().config.clone();
        add_provider_account(
            config.as_ref(),
            "openai",
            "work",
            "sk-navigation-preview",
            "gpt-5",
            "",
        )
        .expect("add provider for live preview");
        shared.settings_mut().provider_forms = provider_forms(config.as_ref());
        shared.publish_settings_signals();
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::RedrawRequested))?;
        app.run_until_idle()?;

        let refreshed_provider_row = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == SemanticsRole::ListItem && node.name.as_deref() == Some("Providers")
            })
            .expect("refreshed Providers navigation row");
        assert!(refreshed_provider_row.bounds.height() <= SIDEBAR_ITEM_H + 0.1);
        about_list.expect().to_be_focused()?;
        Ok(())
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
            "sandbox.policy",
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
            settings.config_items = vec![test_config_item(
                "test.raw-fallback",
                ConfigEditKind::Json,
                r#"{"legacy":true}"#,
            )];
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
        // The disclosure sits below the default 720px viewport; off-viewport clicks miss.
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 1600.0,
            ))))?;
        app.run_until_idle()?;

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
    fn node_settings_summary_keeps_selected_node_ids_in_details() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let node_id = {
            let mut settings = shared.settings_mut();
            settings.open(SettingsSection::Configuration);
            settings.configuration_node_scope = true;
            settings
                .module_snapshot
                .selected_node
                .as_ref()
                .unwrap()
                .summary
                .node_id
                .clone()
        };
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Selected node identity")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("settings-node-id")
            .expect()
            .to_be_hidden()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_description(details_toggle_description("selected-node"))
            .click()?;
        window
            .get_by_role(sui::SemanticsRole::TextInput)
            .with_name("settings-node-id")
            .expect()
            .to_have_value(&node_id)?;
        Ok(())
    }

    #[test]
    fn connection_preview_surfaces_attention_state() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        shared.chat_mut().notice =
            Some("chat list failed: device authorization was denied".to_string());

        assert_eq!(
            section_preview(&shared, SettingsSection::System),
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
                .open(SettingsSection::System);
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
                .open(SettingsSection::System);
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
                .open(SettingsSection::System);
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
            chat_providers: Vec::new(),
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
            chat_providers: Vec::new(),
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
    fn device_login_pairing_code_is_large_selectable_and_copyable() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        add_provider_account(
            backend.config.as_ref(),
            "codex",
            sinomo_runtime::api::lm::DEFAULT_PROVIDER_ACCOUNT,
            "",
            &provider_default_model("codex"),
            "",
        )
        .expect("add Codex provider account");
        let shared = Shared::new(AppState::ready(backend));
        let (_tx, rx) = std::sync::mpsc::channel();
        shared.settings_mut().device_login = Some(DeviceLogin {
            provider: "codex".to_string(),
            account: sinomo_runtime::api::lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            status: DeviceLoginStatus::Prompt {
                verification_uri: "https://example.test/device".to_string(),
                user_code: "ABCD-1234".to_string(),
            },
            rx,
        });
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let theme = reader();
        let code_style = device_login_code_text_style(&theme, &palette(&shared));
        assert_eq!(code_style.font_size, theme.text._3xl.size);
        assert!(code_style.font_size > theme.body_text_style().font_size);

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Provider pairing code")
                    .root(
                        SizedBox::new()
                            .width(520.0)
                            .height(160.0)
                            .child(status_reflector(&shared, 0, &reader)),
                    ),
            )
            .build()?;
        let clipboard = runtime.clipboard();
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let field = window
            .get_by_role(SemanticsRole::TextInput)
            .with_name("provider-device-login-code");
        field.expect().to_have_value("ABCD-1234")?;
        let field_bounds = window
            .snapshot()?
            .accessibility
            .nodes
            .into_iter()
            .find(|node| {
                node.role == SemanticsRole::TextInput
                    && node.name.as_deref() == Some("provider-device-login-code")
            })
            .expect("pairing code field semantics")
            .bounds;
        assert!(field_bounds.height() >= 54.0);

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Copy pairing code")
            .click()?;
        assert_eq!(clipboard.text().as_deref(), Some("ABCD-1234"));

        clipboard.set_text("selection copy did not run");
        field.focus()?;
        let mut select_all = sui::KeyboardEvent::new("a", sui::KeyState::Pressed);
        select_all.modifiers.control = true;
        field.dispatch_event(sui::Event::Keyboard(select_all))?;
        let mut copy = sui::KeyboardEvent::new("c", sui::KeyState::Pressed);
        copy.modifiers.control = true;
        field.dispatch_event(sui::Event::Keyboard(copy))?;
        assert_eq!(clipboard.text().as_deref(), Some("ABCD-1234"));

        Ok(())
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

mod usage_settings;
pub(crate) fn open_usage_skill(shared: &Shared, id: &str, version: &str) {
    let selected = (id.to_string(), version.to_string());
    {
        let mut settings = shared.settings_mut();
        settings.open(SettingsSection::Skills);
        settings.skill_selected = Some(selected.clone());
        settings.skills_include_all = true;
    }
    shared.set_active_tab(Tab::Settings);
    queue_settings_operation(
        shared,
        SettingsOperationScope::Skills,
        SettingsOperation::RefreshSkills {
            include_all: true,
            selected: Some(selected),
        },
    );
}
