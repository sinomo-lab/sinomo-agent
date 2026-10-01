//! Bounded interactive controller skeleton.

pub mod cli;

use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelCancellation, LanguageModelError, LanguageModelMessage, LanguageModelProvider,
    LanguageModelRequest, LanguageModelResponse, LanguageModelResponseFormat, LanguageModelRole,
    LanguageModelStreamEvent, LanguageModelUsage,
};
use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventKind, Id, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source,
};
use sinomo_runtime::events::{EventId, EventStore, ReplayDirection, ReplayQuery, SessionId};
use sinomo_runtime::node::NodeId;
use sinomo_runtime::observe::{ObserveError, ObserveMetricRecord};
use sinomo_runtime::vfs::{VfsHandle, VfsObjectKind};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use thiserror::Error;

mod agent;
mod channel;
mod compaction;
mod process;
mod readiness;
mod service;
mod session_store;
mod skill_catalog;
mod time_serde;
pub mod tools;

pub use agent::*;
pub use channel::*;
pub use compaction::*;
pub use process::*;
pub use readiness::*;
pub use service::*;
pub use session_store::*;
pub use skill_catalog::*;
pub use tools::*;

pub type TaskId = Id;
pub const AGENT_DEFINITION_SIFS_ROOT: &str = "/services/sinomo/agents";

#[derive(Clone, Debug, Default)]
pub struct AgentCancellationToken {
    cancelled: Arc<AtomicBool>,
    deadline: Option<std::time::Instant>,
    parent: Option<Arc<AgentCancellationToken>>,
}

impl AgentCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        if self
            .parent
            .as_ref()
            .is_some_and(|parent| parent.is_cancelled())
            || self
                .deadline
                .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            self.cancel();
        }
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Cancellation flows from parent to child; cancelling the child never
    /// cancels its parent or siblings.
    pub fn child_token(&self) -> Self {
        Self {
            parent: Some(Arc::new(self.clone())),
            ..Self::default()
        }
    }

    pub(crate) fn with_timeout_ms(mut self, timeout_ms: Option<u64>) -> Self {
        if let Some(deadline) = timeout_ms.and_then(|timeout| {
            std::time::Instant::now().checked_add(std::time::Duration::from_millis(timeout))
        }) {
            self.deadline = Some(
                self.deadline
                    .map_or(deadline, |previous| previous.min(deadline)),
            );
        }
        self
    }

    fn language_model_cancellation(&self) -> LanguageModelCancellation {
        let token = self.clone();
        LanguageModelCancellation::new(move || token.is_cancelled())
    }
}

pub fn register_storage_paths(
    layout: &mut sinomo_runtime::storage::StorageLayout,
) -> Result<(), sinomo_runtime::storage::StorageContractError> {
    sinomo_memory::register_storage_paths(layout)?;
    layout.register_escape_path(
        "sinomo-agent",
        sinomo_runtime::storage::StorageRing::R2,
        "events/agent",
        "agent sessions, channels, traces, context assembly reports, model events, and tool evidence artifacts",
    )?;
    layout.register_escape_path(
        "sinomo-agent",
        sinomo_runtime::storage::StorageRing::R2,
        "agents",
        "local agent workspace homes; cluster-wide agent definitions live in SIFS",
    )?;
    layout.register_escape_path(
        "sinomo-agent",
        sinomo_runtime::storage::StorageRing::R2,
        "skills",
        "local mirror and import/export staging for SIFS-backed skill catalog artifacts",
    )
}

pub fn register_observe_metric_exporters() {
    use sinomo_runtime::observe::{
        ObserveMetricAggregation, ObserveMetricDescriptor, ObserveMetricKind,
        ObserveMetricSemantics, ObserveMetricUnit,
    };

    for (name, help) in [
        ("sinomo.agent.sessions", "Persisted agent sessions."),
        (
            "sinomo.agent.invalid_session_dirs",
            "Invalid persisted agent session directories.",
        ),
        (
            "sinomo.agent.session_snapshots",
            "Persisted agent session snapshots.",
        ),
        (
            "sinomo.agent.control_snapshots",
            "Persisted agent control snapshots.",
        ),
        ("sinomo.agent.active_turns", "Agent turns currently active."),
        (
            "sinomo.agent.buffered_events",
            "Agent events currently buffered.",
        ),
        (
            "sinomo.agent.tasks",
            "Agent tasks by bounded lifecycle status.",
        ),
        (
            "sinomo.agent.control_snapshot_read_errors",
            "Agent control snapshots that could not be read.",
        ),
    ] {
        let labels = if name == "sinomo.agent.tasks" {
            vec!["status"]
        } else {
            Vec::new()
        };
        sinomo_runtime::observe::register_observe_metric_descriptor(
            ObserveMetricDescriptor::new(
                name,
                ObserveMetricKind::Gauge,
                Some(ObserveMetricUnit::Count),
                help,
            )
            .with_semantics(ObserveMetricSemantics::new(
                "agent",
                ObserveMetricAggregation::None,
                labels.iter().copied(),
            )),
        );
    }
    sinomo_runtime::observe::register_observe_metric_exporter_with_generation(
        "sinomo-agent",
        ["sinomo.agent."],
        agent_observe_metric_generation,
        agent_observe_metrics,
    );
}

struct AgentObserveMetricWatcher {
    generation: Arc<AtomicU64>,
    _watcher: notify::RecommendedWatcher,
}

static AGENT_OBSERVE_METRIC_WATCHERS: OnceLock<
    Mutex<BTreeMap<PathBuf, AgentObserveMetricWatcher>>,
> = OnceLock::new();

fn agent_observe_metric_generation(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<u64, ObserveError> {
    use notify::Watcher;

    let paths = AgentStoragePaths::from_data_dir(data_dir);
    let watch_root = paths.root;
    let mut watchers = AGENT_OBSERVE_METRIC_WATCHERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| ObserveError::Runtime {
            detail: "agent observe metric watcher registry is poisoned".to_string(),
        })?;
    if let Some(watcher) = watchers.get(&watch_root) {
        return Ok(watcher.generation.load(Ordering::Acquire));
    }
    std::fs::create_dir_all(&watch_root).map_err(|source| ObserveError::Runtime {
        detail: format!(
            "agent observe metric watcher could not create {}: {source}",
            watch_root.display()
        ),
    })?;
    let generation = Arc::new(AtomicU64::new(1));
    let callback_generation = Arc::clone(&generation);
    let mut watcher = notify::recommended_watcher(
        move |event: Result<notify::Event, notify::Error>| match event {
            Ok(event) if !matches!(event.kind, notify::EventKind::Access(_)) => {
                callback_generation.fetch_add(1, Ordering::AcqRel);
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(
                target: "sinomo.agent.observe",
                "agent metric invalidation watcher failed: {error}"
            ),
        },
    )
    .map_err(|source| ObserveError::Runtime {
        detail: format!("agent observe metric watcher could not initialize: {source}"),
    })?;
    watcher
        .watch(&watch_root, notify::RecursiveMode::Recursive)
        .map_err(|source| ObserveError::Runtime {
            detail: format!(
                "agent observe metric watcher could not watch {}: {source}",
                watch_root.display()
            ),
        })?;
    let current = generation.load(Ordering::Acquire);
    watchers.insert(
        watch_root,
        AgentObserveMetricWatcher {
            generation,
            _watcher: watcher,
        },
    );
    Ok(current)
}

fn agent_observe_metrics(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<Vec<ObserveMetricRecord>, ObserveError> {
    let paths = AgentStoragePaths::from_data_dir(data_dir);
    let store = FileAgentSessionStore::new(paths.clone());
    let vfs = VfsHandle::local(paths.root.clone()).map_err(|source| ObserveError::Runtime {
        detail: format!(
            "agent snapshot metric export failed to open VFS root {}: {source}",
            paths.root.display()
        ),
    })?;
    let mut session_dirs = 0_usize;
    let mut session_snapshots = 0_usize;
    let mut control_snapshots = 0_usize;
    let mut active_turns = 0_usize;
    let mut buffered_events = 0_usize;
    let mut invalid_session_dirs = 0_usize;
    let mut control_snapshot_read_errors = 0_usize;
    let mut pending_tasks = 0_usize;
    let mut running_tasks = 0_usize;
    let mut completed_tasks = 0_usize;
    let mut failed_tasks = 0_usize;
    let mut cancelled_tasks = 0_usize;
    let mut timed_out_tasks = 0_usize;

    let entries = vfs
        .list_if_exists("sessions")
        .map_err(|source| ObserveError::Runtime {
            detail: format!(
                "agent snapshot metric export failed to read {}: {source}",
                paths.sessions_dir.display()
            ),
        })?;

    for entry in entries {
        if entry.kind != VfsObjectKind::Directory {
            continue;
        }
        session_dirs = session_dirs.saturating_add(1);
        let Ok(session_id) = entry.name.parse::<SessionId>() else {
            invalid_session_dirs = invalid_session_dirs.saturating_add(1);
            continue;
        };
        let snapshot_path = format!("sessions/{}/snapshot.json", session_id.as_str());
        if vfs.stat(&snapshot_path).is_ok() {
            session_snapshots = session_snapshots.saturating_add(1);
        }
        match store.load_control_snapshot(&session_id) {
            Ok(Some(snapshot)) => {
                control_snapshots = control_snapshots.saturating_add(1);
                if snapshot.active_turn.is_some() {
                    active_turns = active_turns.saturating_add(1);
                }
                buffered_events = buffered_events.saturating_add(snapshot.buffered_event_count);
                for task in snapshot.tasks {
                    match task.status {
                        AgentTaskLifecycleStatus::Pending => {
                            pending_tasks = pending_tasks.saturating_add(1)
                        }
                        AgentTaskLifecycleStatus::Running => {
                            running_tasks = running_tasks.saturating_add(1)
                        }
                        AgentTaskLifecycleStatus::Completed => {
                            completed_tasks = completed_tasks.saturating_add(1)
                        }
                        AgentTaskLifecycleStatus::Failed => {
                            failed_tasks = failed_tasks.saturating_add(1)
                        }
                        AgentTaskLifecycleStatus::Cancelled => {
                            cancelled_tasks = cancelled_tasks.saturating_add(1)
                        }
                        AgentTaskLifecycleStatus::TimedOut => {
                            timed_out_tasks = timed_out_tasks.saturating_add(1)
                        }
                    }
                }
            }
            Ok(None) => {}
            Err(_) => {
                control_snapshot_read_errors = control_snapshot_read_errors.saturating_add(1);
            }
        }
    }

    Ok(agent_metric_records(AgentMetricSnapshot {
        session_dirs,
        session_snapshots,
        control_snapshots,
        active_turns,
        buffered_events,
        invalid_session_dirs,
        control_snapshot_read_errors,
        pending_tasks,
        running_tasks,
        completed_tasks,
        failed_tasks,
        cancelled_tasks,
        timed_out_tasks,
    }))
}

#[derive(Default)]
struct AgentMetricSnapshot {
    session_dirs: usize,
    session_snapshots: usize,
    control_snapshots: usize,
    active_turns: usize,
    buffered_events: usize,
    invalid_session_dirs: usize,
    control_snapshot_read_errors: usize,
    pending_tasks: usize,
    running_tasks: usize,
    completed_tasks: usize,
    failed_tasks: usize,
    cancelled_tasks: usize,
    timed_out_tasks: usize,
}

fn agent_metric_records(snapshot: AgentMetricSnapshot) -> Vec<ObserveMetricRecord> {
    let mut records = vec![
        agent_gauge("sinomo.agent.sessions", snapshot.session_dirs),
        agent_gauge("sinomo.agent.session_snapshots", snapshot.session_snapshots),
        agent_gauge("sinomo.agent.control_snapshots", snapshot.control_snapshots),
        agent_gauge("sinomo.agent.active_turns", snapshot.active_turns),
        agent_gauge("sinomo.agent.buffered_events", snapshot.buffered_events),
        agent_gauge(
            "sinomo.agent.invalid_session_dirs",
            snapshot.invalid_session_dirs,
        ),
        agent_gauge(
            "sinomo.agent.control_snapshot_read_errors",
            snapshot.control_snapshot_read_errors,
        ),
    ];
    for (status, count) in [
        ("pending", snapshot.pending_tasks),
        ("running", snapshot.running_tasks),
        ("completed", snapshot.completed_tasks),
        ("failed", snapshot.failed_tasks),
        ("cancelled", snapshot.cancelled_tasks),
        ("timed_out", snapshot.timed_out_tasks),
    ] {
        let mut labels = BTreeMap::new();
        labels.insert("status".to_string(), status.to_string());
        records.push(ObserveMetricRecord::gauge(
            "sinomo.agent.tasks",
            labels,
            count as f64,
        ));
    }
    records
}

fn agent_gauge(name: impl Into<String>, value: usize) -> ObserveMetricRecord {
    ObserveMetricRecord::gauge(name, BTreeMap::new(), value as f64)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolSpec {
    pub kind: AgentToolKind,
    pub name: String,
    pub description: String,
    pub async_default: bool,
}

impl AgentToolSpec {
    pub fn new(
        kind: AgentToolKind,
        name: impl Into<String>,
        description: impl Into<String>,
        async_default: bool,
    ) -> Self {
        Self {
            kind,
            name: name.into(),
            description: description.into(),
            async_default,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolKind {
    Subagents,
    StartAsyncTask,
    InspectAsyncTask,
    CancelAsyncTask,
    RunShellCommand,
    ReadFile,
    ListFiles,
    EditFile,
    WriteFile,
    SearchFiles,
    ApplyPatch,
    ChangedFiles,
    GitDiff,
    ConfigSearch,
    ConfigGet,
    ConfigSet,
    SessionHistorySearch,
    MemorySearch,
    MemoryRead,
    MemoryWrite,
    RequestUserInput,
    ToolSearch,
    ToolDescribe,
    ToolCall,
    SkillSearch,
    SkillDescribe,
    SkillActivate,
    SkillUpdate,
    ModuleObjectRpc,
    McpCall,
    WebSearch,
    ImageGeneration,
    ReadSifs,
    WriteSifs,
    ListSifs,
    ClusterStatus,
    HandoffToBackground,
    ObserveVision,
}

impl AgentToolKind {
    pub fn from_identifier(value: &str) -> Option<Self> {
        serde_json::from_value::<Self>(serde_json::Value::String(value.to_string())).ok()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum AgentOutputFormat {
    #[default]
    JsonDecision,
    Text,
}

impl AgentOutputFormat {
    pub fn language_model_response_format(&self) -> LanguageModelResponseFormat {
        match self {
            Self::JsonDecision => LanguageModelResponseFormat::JsonObject,
            Self::Text => LanguageModelResponseFormat::Text,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentDefinition {
    #[serde(default = "default_agent_definition_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub package: AgentPackageDefinition,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub role: AgentRole,
    pub system_prompt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<AgentToolSpec>,
    #[serde(
        default,
        skip_serializing_if = "AgentToolSurfaceDefinition::is_default"
    )]
    pub tool_surface: AgentToolSurfaceDefinition,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
    #[serde(default)]
    pub context: AgentContextDefinition,
    #[serde(default)]
    pub compaction: AgentCompactionDefinition,
    #[serde(default, skip_serializing_if = "AgentModelPolicy::is_default")]
    pub model_policy: AgentModelPolicy,
    #[serde(
        default,
        skip_serializing_if = "AgentRuntimeLimitsDefinition::is_default"
    )]
    pub runtime_limits: AgentRuntimeLimitsDefinition,
    #[serde(default, skip_serializing_if = "AgentPlacementDefinition::is_default")]
    pub placement: AgentPlacementDefinition,
    #[serde(
        default,
        skip_serializing_if = "AgentToolRuntimeDefinition::is_default"
    )]
    pub tool_runtime: AgentToolRuntimeDefinition,
    #[serde(default)]
    pub output_format: AgentOutputFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
}

pub const AGENT_DEFINITION_SCHEMA_VERSION: u32 = 1;
pub const AGENT_PACKAGE_VERSION: &str = "1.0.0";
pub const DEFAULT_AGENT_CONTEXT_PIPELINE_ID: &str = "context.pipeline.default";
pub const DEFAULT_AGENT_COMPACTION_STRATEGY_ID: &str = "compaction.strategy.default";

fn default_agent_definition_schema_version() -> u32 {
    AGENT_DEFINITION_SCHEMA_VERSION
}

fn default_agent_package_version() -> String {
    AGENT_PACKAGE_VERSION.to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentPackageDefinition {
    #[serde(default = "default_agent_package_version")]
    pub version: String,
    #[serde(default)]
    pub provenance: AgentPackageProvenance,
}

impl Default for AgentPackageDefinition {
    fn default() -> Self {
        Self::local()
    }
}

impl AgentPackageDefinition {
    pub fn built_in() -> Self {
        Self {
            version: AGENT_PACKAGE_VERSION.to_string(),
            provenance: AgentPackageProvenance::built_in(),
        }
    }

    pub fn local() -> Self {
        Self {
            version: AGENT_PACKAGE_VERSION.to_string(),
            provenance: AgentPackageProvenance::local(),
        }
    }

    pub fn from_module(module_id: impl Into<String>, module_version: impl Into<String>) -> Self {
        let module_version = module_version.into();
        Self {
            version: module_version.clone(),
            provenance: AgentPackageProvenance::module(module_id, module_version),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentPackageProvenance {
    #[serde(default)]
    pub source: AgentPackageSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

impl AgentPackageProvenance {
    pub fn built_in() -> Self {
        Self {
            source: AgentPackageSource::BuiltIn,
            ..Self::default()
        }
    }

    pub fn local() -> Self {
        Self {
            source: AgentPackageSource::Local,
            ..Self::default()
        }
    }

    pub fn module(module_id: impl Into<String>, module_version: impl Into<String>) -> Self {
        Self {
            source: AgentPackageSource::Module,
            module_id: Some(module_id.into()),
            module_version: Some(module_version.into()),
            ..Self::default()
        }
    }

    pub fn imported(path: impl Into<String>) -> Self {
        Self {
            source: AgentPackageSource::Imported,
            path: Some(path.into()),
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentPackageSource {
    BuiltIn,
    Local,
    Module,
    Imported,
    #[default]
    Unknown,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolSurfaceDefinition {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_toolsets: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deny_tools: Vec<String>,
}

impl AgentToolSurfaceDefinition {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentContextDefinition {
    pub pipeline_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<String>,
}

impl Default for AgentContextDefinition {
    fn default() -> Self {
        Self {
            pipeline_id: DEFAULT_AGENT_CONTEXT_PIPELINE_ID.to_string(),
            providers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentCompactionDefinition {
    pub strategy_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_tail_items: Option<usize>,
}

impl Default for AgentCompactionDefinition {
    fn default() -> Self {
        Self {
            strategy_id: DEFAULT_AGENT_COMPACTION_STRATEGY_ID.to_string(),
            retained_tail_items: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentModelPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub vendor_options: serde_json::Value,
}

impl AgentModelPolicy {
    pub fn is_default(&self) -> bool {
        self.provider.is_none()
            && self.model.is_none()
            && self.temperature.is_none()
            && self.max_output_tokens.is_none()
            && self.vendor_options.is_null()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentRuntimeLimitsDefinition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls_per_step: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_model_repair_attempts: Option<usize>,
}

impl AgentRuntimeLimitsDefinition {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

/// Preferred placement for new sessions of this agent.
///
/// The home server is a starting preference, not an ownership boundary. The
/// active session host is recorded on [`AgentSession`] and can change only via
/// explicit migration.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentPlacementDefinition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_server_node_id: Option<String>,
}

impl AgentPlacementDefinition {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub fn with_home_server_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.home_server_node_id = Some(node_id.into());
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolRuntimeDefinition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_node_id: Option<String>,
    /// Streamable HTTP MCP servers whose tools are projected through the
    /// regular Sinomo tool registry. Authentication values remain node-local:
    /// definitions contain environment-variable names, never secret values.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp_servers: Vec<AgentMcpServerDefinition>,
}

impl AgentToolRuntimeDefinition {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub fn with_sandbox_node_id(mut self, sandbox_node_id: impl Into<String>) -> Self {
        self.sandbox_node_id = Some(sandbox_node_id.into());
        self
    }

    pub fn with_mcp_server(mut self, server: AgentMcpServerDefinition) -> Self {
        self.mcp_servers.push(server);
        self
    }
}

/// Which generation of the MCP wire protocol a server is expected to speak.
///
/// Revision `2026-07-28` removed the `initialize` handshake and protocol-level
/// sessions: version, identity, and capabilities travel in every request's
/// `_meta`. Earlier revisions negotiate once and keep session state. A single
/// endpoint may implement either, or both.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentMcpProtocolEra {
    /// Probe for the stateless protocol and fall back to the handshake when the
    /// endpoint does not answer `server/discover`.
    #[default]
    Auto,
    /// Require the stateless per-request protocol (`2026-07-28` or later).
    Modern,
    /// Use the `initialize` handshake without probing (`2025-11-25` or earlier).
    Legacy,
}

impl AgentMcpProtocolEra {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Modern => "modern",
            Self::Legacy => "legacy",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentMcpServerDefinition {
    pub name: String,
    pub url: String,
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub enabled: bool,
    /// Wire-protocol generation to use for this endpoint. Defaults to probing.
    #[serde(default, skip_serializing_if = "AgentMcpProtocolEra::is_default")]
    pub protocol_era: AgentMcpProtocolEra,
    /// When true, an unavailable server prevents the agent tool registry from
    /// being configured. Optional servers are skipped with an operator warning.
    #[serde(default)]
    pub required: bool,
    /// Optional allow-list over the server's original MCP tool names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    /// Node-local secret reference. To prevent distributed definitions from
    /// reading unrelated process secrets, the name must start with
    /// `SINOMO_MCP_`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_token_env: Option<String>,
    /// HTTP header name to environment-variable name. Header values are read
    /// only on the node that opens the MCP transport.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers_from_env: BTreeMap<String, String>,
    #[serde(default = "default_mcp_startup_timeout_ms")]
    pub startup_timeout_ms: u64,
    #[serde(default = "default_mcp_tool_timeout_ms")]
    pub tool_timeout_ms: u64,
}

impl AgentMcpServerDefinition {
    pub fn streamable_http(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            enabled: true,
            protocol_era: AgentMcpProtocolEra::default(),
            required: false,
            allowed_tools: Vec::new(),
            bearer_token_env: None,
            headers_from_env: BTreeMap::new(),
            startup_timeout_ms: default_mcp_startup_timeout_ms(),
            tool_timeout_ms: default_mcp_tool_timeout_ms(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty()
            || self.name.len() > 64
            || !self.name.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            return Err("name must contain only ASCII letters, numbers, '-' or '_'".to_string());
        }
        let url =
            reqwest::Url::parse(&self.url).map_err(|source| format!("URL is invalid: {source}"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err("URL must be an absolute http or https endpoint".to_string());
        }
        if url.scheme() == "http"
            && !url.host_str().is_some_and(|host| {
                host.eq_ignore_ascii_case("localhost")
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            })
        {
            return Err("non-loopback MCP endpoints must use https".to_string());
        }
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err("URL must not contain credentials or a fragment".to_string());
        }
        if self.startup_timeout_ms == 0 || self.startup_timeout_ms > 600_000 {
            return Err("startup_timeout_ms must be between 1 and 600000".to_string());
        }
        if self.tool_timeout_ms == 0 || self.tool_timeout_ms > 600_000 {
            return Err("tool_timeout_ms must be between 1 and 600000".to_string());
        }
        if let Some(variable) = self.bearer_token_env.as_deref() {
            validate_mcp_env_name(variable)?;
        }
        let mut normalized_headers = BTreeSet::new();
        for (header, variable) in &self.headers_from_env {
            let header_name = reqwest::header::HeaderName::from_bytes(header.as_bytes())
                .map_err(|source| format!("header `{header}` is invalid: {source}"))?;
            if mcp_header_name_is_transport_controlled(header_name.as_str()) {
                return Err(format!(
                    "header `{header}` is controlled by the MCP transport"
                ));
            }
            if !normalized_headers.insert(header_name.as_str().to_string()) {
                return Err(format!("header `{header}` duplicates another header name"));
            }
            validate_mcp_env_name(variable)?;
        }
        let mut allowed = BTreeSet::new();
        for tool in &self.allowed_tools {
            if tool.trim().is_empty() || tool.len() > 128 || tool.chars().any(char::is_control) {
                return Err(format!(
                    "allowed tool name `{tool}` is empty, too long, or contains control characters"
                ));
            }
            if !allowed.insert(tool) {
                return Err(format!("allowed tool name `{tool}` is duplicated"));
            }
        }
        Ok(())
    }
}

fn default_true() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

fn default_mcp_startup_timeout_ms() -> u64 {
    10_000
}

fn default_mcp_tool_timeout_ms() -> u64 {
    60_000
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentContextStrategyDescriptor {
    pub id: String,
    pub description: String,
    pub built_in: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentCompactionStrategyDescriptor {
    pub id: String,
    pub description: String,
    pub built_in: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AgentContextStrategyRegistry {
    strategies: BTreeMap<String, AgentContextStrategyDescriptor>,
}

impl AgentContextStrategyRegistry {
    pub fn built_in() -> Self {
        let mut registry = Self::default();
        registry.register(AgentContextStrategyDescriptor {
            id: DEFAULT_AGENT_CONTEXT_PIPELINE_ID.to_string(),
            description:
                "assemble the standard session, workspace, tools, and prompt-reference context"
                    .to_string(),
            built_in: true,
        });
        registry
    }

    pub fn register(&mut self, descriptor: AgentContextStrategyDescriptor) {
        self.strategies.insert(descriptor.id.clone(), descriptor);
    }

    pub fn contains(&self, id: &str) -> bool {
        self.strategies.contains_key(id)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AgentCompactionStrategyRegistry {
    strategies: BTreeMap<String, AgentCompactionStrategyDescriptor>,
}

impl AgentCompactionStrategyRegistry {
    pub fn built_in() -> Self {
        let mut registry = Self::default();
        registry.register(AgentCompactionStrategyDescriptor {
            id: DEFAULT_AGENT_COMPACTION_STRATEGY_ID.to_string(),
            description:
                "retain the current transcript unless the runtime budget forces summary compaction"
                    .to_string(),
            built_in: true,
        });
        registry
    }

    pub fn register(&mut self, descriptor: AgentCompactionStrategyDescriptor) {
        self.strategies.insert(descriptor.id.clone(), descriptor);
    }

    pub fn contains(&self, id: &str) -> bool {
        self.strategies.contains_key(id)
    }
}

impl Default for AgentDefinition {
    fn default() -> Self {
        Self::main()
    }
}

/// Host-effect authority held by an agent principal. Hosts configure tool
/// executors from this value; it is never read from package JSON.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentPrincipalAuthority {
    /// May mutate attached workspaces and run generic workspace commands.
    pub workspace_mutation: bool,
    /// May change node-local or cluster configuration through `config_set`.
    pub config_writes: bool,
}

impl AgentDefinition {
    pub fn built_ins() -> Vec<Self> {
        vec![Self::main(), Self::background(), Self::coding()]
    }

    pub fn built_in(name: &str) -> Option<Self> {
        match name {
            "main" | "main-agent" => Some(Self::main()),
            "background" | "background-agent" => Some(Self::background()),
            "coding" | "coding-agent" | "deliberation" | "deliberation-agent" => {
                Some(Self::coding())
            }
            _ => None,
        }
    }

    /// Whether this definition is backed by the user-facing chat runtime.
    /// Background and evolution roles have dedicated execution paths and must
    /// not be presented as ordinary chat participants.
    pub fn supports_interactive_chat(&self) -> bool {
        matches!(self.role, AgentRole::Main | AgentRole::Deliberation)
    }

    /// Host-effect authority this package's principal holds.
    ///
    /// Authority comes only from the reserved built-in identities. Catalog
    /// validation guarantees no stored, imported, or module package can occupy
    /// a reserved id unless it is the canonical built-in, so a custom package
    /// cannot gain authority by declaring a role, copying tool names, or
    /// serializing built-in provenance under another id.
    pub fn principal_authority(&self) -> AgentPrincipalAuthority {
        if self.package.provenance.source != AgentPackageSource::BuiltIn {
            return AgentPrincipalAuthority::default();
        }
        match self.key() {
            "main-agent" => AgentPrincipalAuthority {
                workspace_mutation: true,
                config_writes: true,
            },
            "coding-agent" => AgentPrincipalAuthority {
                workspace_mutation: true,
                config_writes: false,
            },
            "background-agent" => AgentPrincipalAuthority {
                workspace_mutation: false,
                config_writes: true,
            },
            _ => AgentPrincipalAuthority::default(),
        }
    }

    /// Validate that a catalog entry cannot replace one of the built-in
    /// principals. Built-in ids also key durable recall state, so accepting a
    /// different package under the same id would cross an identity boundary.
    pub fn validate_catalog_identity(&self) -> Result<(), AgentDefinitionValidationError> {
        let Some(canonical) = Self::built_in(self.key()) else {
            return Ok(());
        };
        if self == &canonical {
            Ok(())
        } else {
            Err(AgentDefinitionValidationError::ReservedBuiltInId {
                id: self.key().to_string(),
            })
        }
    }

    pub fn custom(
        name: impl Into<String>,
        role: AgentRole,
        system_prompt: impl Into<String>,
    ) -> Self {
        let name = name.into();
        Self {
            schema_version: AGENT_DEFINITION_SCHEMA_VERSION,
            package: AgentPackageDefinition::local(),
            id: name.clone(),
            name,
            description: String::new(),
            role,
            system_prompt: system_prompt.into(),
            tools: default_agent_recall_tools(),
            tool_surface: AgentToolSurfaceDefinition::default(),
            skills: Vec::new(),
            context: AgentContextDefinition::default(),
            compaction: AgentCompactionDefinition::default(),
            model_policy: AgentModelPolicy::default(),
            runtime_limits: AgentRuntimeLimitsDefinition::default(),
            placement: AgentPlacementDefinition::default(),
            tool_runtime: AgentToolRuntimeDefinition::default(),
            output_format: AgentOutputFormat::JsonDecision,
            temperature: None,
            max_output_tokens: None,
        }
    }

    pub fn main() -> Self {
        let max_output_tokens = Some(1600);
        Self {
            schema_version: AGENT_DEFINITION_SCHEMA_VERSION,
            package: AgentPackageDefinition::built_in(),
            id: "main-agent".to_string(),
            name: "main-agent".to_string(),
            description: "user-facing interactive agent".to_string(),
            role: AgentRole::Main,
            system_prompt: main_agent_base_prompt().to_string(),
            tools: default_main_agent_tools(),
            tool_surface: AgentToolSurfaceDefinition::default(),
            skills: Vec::new(),
            context: AgentContextDefinition::default(),
            compaction: AgentCompactionDefinition::default(),
            model_policy: AgentModelPolicy {
                max_output_tokens,
                ..AgentModelPolicy::default()
            },
            runtime_limits: AgentRuntimeLimitsDefinition::default(),
            placement: AgentPlacementDefinition::default(),
            tool_runtime: AgentToolRuntimeDefinition::default(),
            output_format: AgentOutputFormat::JsonDecision,
            temperature: None,
            max_output_tokens,
        }
    }

    pub fn background() -> Self {
        let max_output_tokens = Some(700);
        Self {
            schema_version: AGENT_DEFINITION_SCHEMA_VERSION,
            package: AgentPackageDefinition::built_in(),
            id: "background-agent".to_string(),
            name: "background-agent".to_string(),
            description: "silent maintenance and cluster-support agent".to_string(),
            role: AgentRole::Background,
            system_prompt: background_agent_base_prompt().to_string(),
            tools: default_background_agent_tools(),
            tool_surface: AgentToolSurfaceDefinition::default(),
            skills: Vec::new(),
            context: AgentContextDefinition::default(),
            compaction: AgentCompactionDefinition::default(),
            model_policy: AgentModelPolicy {
                max_output_tokens,
                ..AgentModelPolicy::default()
            },
            runtime_limits: AgentRuntimeLimitsDefinition::default(),
            placement: AgentPlacementDefinition::default(),
            tool_runtime: AgentToolRuntimeDefinition::default(),
            output_format: AgentOutputFormat::JsonDecision,
            temperature: None,
            max_output_tokens,
        }
    }

    pub fn coding() -> Self {
        Self {
            schema_version: AGENT_DEFINITION_SCHEMA_VERSION,
            package: AgentPackageDefinition::built_in(),
            id: "coding-agent".to_string(),
            name: "coding-agent".to_string(),
            description: "general workspace task and verification agent".to_string(),
            role: AgentRole::Deliberation,
            system_prompt: coding_agent_base_prompt().to_string(),
            tools: default_coding_agent_tools(),
            tool_surface: AgentToolSurfaceDefinition::default(),
            skills: Vec::new(),
            context: AgentContextDefinition::default(),
            compaction: AgentCompactionDefinition::default(),
            model_policy: AgentModelPolicy::default(),
            runtime_limits: AgentRuntimeLimitsDefinition::default(),
            placement: AgentPlacementDefinition::default(),
            tool_runtime: AgentToolRuntimeDefinition::default(),
            output_format: AgentOutputFormat::JsonDecision,
            temperature: None,
            max_output_tokens: None,
        }
    }

    pub fn key(&self) -> &str {
        if self.id.is_empty() {
            &self.name
        } else {
            &self.id
        }
    }

    pub fn validate(&self) -> Result<(), AgentDefinitionValidationError> {
        AgentDefinitionValidator::built_in().validate(self)
    }

    pub fn with_tools(mut self, tools: impl IntoIterator<Item = AgentToolSpec>) -> Self {
        self.tools = tools.into_iter().collect();
        self
    }

    pub fn with_tool(mut self, tool: AgentToolSpec) -> Self {
        self.tools.push(tool);
        self
    }

    pub fn with_skill(mut self, skill: impl Into<String>) -> Self {
        self.skills.push(skill.into());
        self
    }

    pub fn with_package_version(mut self, version: impl Into<String>) -> Self {
        self.package.version = version.into();
        self
    }

    pub fn with_package_provenance(mut self, provenance: AgentPackageProvenance) -> Self {
        self.package.provenance = provenance;
        self
    }

    pub fn with_tool_runtime(mut self, tool_runtime: AgentToolRuntimeDefinition) -> Self {
        self.tool_runtime = tool_runtime;
        self
    }

    pub fn with_home_server_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.placement.home_server_node_id = Some(node_id.into());
        self
    }

    pub fn with_sandbox_runtime_node_id(mut self, sandbox_node_id: impl Into<String>) -> Self {
        self.tool_runtime.sandbox_node_id = Some(sandbox_node_id.into());
        self
    }

    pub fn with_output_format(mut self, output_format: AgentOutputFormat) -> Self {
        self.output_format = output_format;
        self
    }

    pub fn with_temperature(mut self, temperature: Option<f32>) -> Self {
        self.temperature = temperature;
        self.model_policy.temperature = temperature;
        self
    }

    pub fn with_max_output_tokens(mut self, max_output_tokens: Option<u32>) -> Self {
        self.max_output_tokens = max_output_tokens;
        self.model_policy.max_output_tokens = max_output_tokens;
        self
    }

    pub fn resolve_tool_specs<'a>(
        &'a self,
        turn_tools: &'a [AgentToolSpec],
    ) -> &'a [AgentToolSpec] {
        if turn_tools.is_empty() {
            &self.tools
        } else {
            turn_tools
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum AgentDefinitionValidationError {
    #[error("agent definition id is required")]
    MissingId,
    #[error("agent definition id `{id}` contains unsupported characters")]
    InvalidId { id: String },
    #[error("agent definition id `{id}` is reserved for an immutable built-in principal")]
    ReservedBuiltInId { id: String },
    #[error("agent definition `{id}` has an empty name")]
    MissingName { id: String },
    #[error("agent definition `{id}` has an empty system prompt")]
    MissingSystemPrompt { id: String },
    #[error("agent definition `{id}` has an empty package version")]
    MissingPackageVersion { id: String },
    #[error("agent definition `{id}` package version `{version}` contains unsupported characters")]
    InvalidPackageVersion { id: String, version: String },
    #[error("agent definition `{id}` references invalid tool name `{tool}`")]
    InvalidToolName { id: String, tool: String },
    #[error("agent definition `{id}` declares duplicate tool name `{tool}`")]
    DuplicateToolName { id: String, tool: String },
    #[error("agent definition `{id}` references invalid tool surface entry `{value}` in `{field}`")]
    InvalidToolSurfaceEntry {
        id: String,
        field: String,
        value: String,
    },
    #[error("agent definition `{id}` references invalid skill `{skill}`")]
    InvalidSkill { id: String, skill: String },
    #[error("agent definition `{id}` references invalid context provider `{provider}`")]
    InvalidContextProvider { id: String, provider: String },
    #[error(
        "agent definition `{id}` declares context providers, but pluggable context providers are not supported by this runtime"
    )]
    UnsupportedContextProviders { id: String },
    #[error("agent definition `{id}` references invalid model preference `{value}` in `{field}`")]
    InvalidModelPreference {
        id: String,
        field: String,
        value: String,
    },
    #[error("agent definition `{id}` has invalid temperature `{value}` in `{field}`")]
    InvalidTemperature {
        id: String,
        field: String,
        value: String,
    },
    #[error(
        "agent definition `{id}` has conflicting `{field}` values in legacy fields and model_policy"
    )]
    ConflictingModelPolicyField { id: String, field: String },
    #[error(
        "agent definition `{id}` declares model vendor options, but package vendor options are not supported by this runtime"
    )]
    UnsupportedModelVendorOptions { id: String },
    #[error("agent definition `{id}` has invalid runtime limit `{field}`")]
    InvalidRuntimeLimit { id: String, field: String },
    #[error(
        "agent definition `{id}` reserves {max_output_tokens} output tokens, leaving no input capacity in the resolved {context_window_tokens}-token model window"
    )]
    OutputReserveExhaustsContextWindow {
        id: String,
        max_output_tokens: u32,
        context_window_tokens: usize,
    },
    #[error("agent definition `{id}` has invalid sandbox runtime node id `{node_id}`")]
    InvalidSandboxRuntimeNodeId { id: String, node_id: String },
    #[error("agent definition `{id}` has invalid MCP server `{server}`: {detail}")]
    InvalidMcpServer {
        id: String,
        server: String,
        detail: String,
    },
    #[error("agent definition `{id}` declares a colliding MCP server namespace for `{server}`")]
    DuplicateMcpServer { id: String, server: String },
    #[error("agent definition `{id}` has invalid home server node id `{node_id}`")]
    InvalidHomeServerNodeId { id: String, node_id: String },
    #[error("agent definition `{id}` module provenance is missing module id")]
    MissingProvenanceModuleId { id: String },
    #[error(
        "agent definition `{id}` module provenance id `{module_id}` contains unsupported characters"
    )]
    InvalidProvenanceModuleId { id: String, module_id: String },
    #[error("agent definition `{id}` references unknown context pipeline `{pipeline_id}`")]
    UnknownContextPipeline { id: String, pipeline_id: String },
    #[error("agent definition `{id}` references unknown compaction strategy `{strategy_id}`")]
    UnknownCompactionStrategy { id: String, strategy_id: String },
    #[error("module agent export `{id}` references unknown role `{role}`")]
    UnknownRole { id: String, role: String },
    #[error("module agent export `{id}` references unknown tool kind `{kind}`")]
    UnknownToolKind { id: String, kind: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentDefinitionValidator {
    context_strategies: AgentContextStrategyRegistry,
    compaction_strategies: AgentCompactionStrategyRegistry,
}

impl AgentDefinitionValidator {
    pub fn built_in() -> Self {
        Self {
            context_strategies: AgentContextStrategyRegistry::built_in(),
            compaction_strategies: AgentCompactionStrategyRegistry::built_in(),
        }
    }

    pub fn validate(
        &self,
        definition: &AgentDefinition,
    ) -> Result<(), AgentDefinitionValidationError> {
        let id = definition.key();
        if id.trim().is_empty() {
            return Err(AgentDefinitionValidationError::MissingId);
        }
        if !is_valid_agent_definition_id(id) {
            return Err(AgentDefinitionValidationError::InvalidId { id: id.to_string() });
        }
        if definition.name.trim().is_empty() {
            return Err(AgentDefinitionValidationError::MissingName { id: id.to_string() });
        }
        if definition.system_prompt.trim().is_empty() {
            return Err(AgentDefinitionValidationError::MissingSystemPrompt { id: id.to_string() });
        }
        if definition.package.version.trim().is_empty() {
            return Err(AgentDefinitionValidationError::MissingPackageVersion {
                id: id.to_string(),
            });
        }
        if !is_valid_agent_package_version(&definition.package.version) {
            return Err(AgentDefinitionValidationError::InvalidPackageVersion {
                id: id.to_string(),
                version: definition.package.version.clone(),
            });
        }
        if definition.package.provenance.source == AgentPackageSource::Module {
            let module_id = definition
                .package
                .provenance
                .module_id
                .as_deref()
                .ok_or_else(
                    || AgentDefinitionValidationError::MissingProvenanceModuleId {
                        id: id.to_string(),
                    },
                )?;
            if !is_valid_agent_definition_id(module_id) {
                return Err(AgentDefinitionValidationError::InvalidProvenanceModuleId {
                    id: id.to_string(),
                    module_id: module_id.to_string(),
                });
            }
        }
        let mut tool_names = BTreeSet::new();
        for tool in &definition.tools {
            if !is_valid_agent_package_identifier(&tool.name) {
                return Err(AgentDefinitionValidationError::InvalidToolName {
                    id: id.to_string(),
                    tool: tool.name.clone(),
                });
            }
            if !tool_names.insert(tool.name.clone()) {
                return Err(AgentDefinitionValidationError::DuplicateToolName {
                    id: id.to_string(),
                    tool: tool.name.clone(),
                });
            }
        }
        for value in &definition.tool_surface.visible_toolsets {
            validate_tool_surface_entry(id, "visible_toolsets", value)?;
        }
        for value in &definition.tool_surface.allow_tools {
            validate_tool_surface_entry(id, "allow_tools", value)?;
        }
        for value in &definition.tool_surface.deny_tools {
            validate_tool_surface_entry(id, "deny_tools", value)?;
        }
        for skill in &definition.skills {
            if !is_valid_agent_package_identifier(skill) {
                return Err(AgentDefinitionValidationError::InvalidSkill {
                    id: id.to_string(),
                    skill: skill.clone(),
                });
            }
        }
        for provider in &definition.context.providers {
            if !is_valid_agent_package_identifier(provider) {
                return Err(AgentDefinitionValidationError::InvalidContextProvider {
                    id: id.to_string(),
                    provider: provider.clone(),
                });
            }
        }
        if !definition.context.providers.is_empty() {
            return Err(
                AgentDefinitionValidationError::UnsupportedContextProviders { id: id.to_string() },
            );
        }
        if let Some(provider) = definition.model_policy.provider.as_deref() {
            validate_model_preference(id, "provider", provider)?;
        }
        if let Some(model) = definition.model_policy.model.as_deref() {
            validate_model_preference(id, "model", model)?;
        }
        if let Some(temperature) = definition.temperature {
            validate_temperature(id, "temperature", temperature)?;
        }
        if let Some(temperature) = definition.model_policy.temperature {
            validate_temperature(id, "model_policy.temperature", temperature)?;
        }
        if definition.temperature.is_some()
            && definition.model_policy.temperature.is_some()
            && definition.temperature != definition.model_policy.temperature
        {
            return Err(
                AgentDefinitionValidationError::ConflictingModelPolicyField {
                    id: id.to_string(),
                    field: "temperature".to_string(),
                },
            );
        }
        if !definition.model_policy.vendor_options.is_null() {
            return Err(
                AgentDefinitionValidationError::UnsupportedModelVendorOptions {
                    id: id.to_string(),
                },
            );
        }
        if let Some(max_output_tokens) = definition.max_output_tokens
            && max_output_tokens == 0
        {
            return Err(AgentDefinitionValidationError::InvalidRuntimeLimit {
                id: id.to_string(),
                field: "max_output_tokens".to_string(),
            });
        }
        if let Some(max_output_tokens) = definition.model_policy.max_output_tokens
            && max_output_tokens == 0
        {
            return Err(AgentDefinitionValidationError::InvalidRuntimeLimit {
                id: id.to_string(),
                field: "model_policy.max_output_tokens".to_string(),
            });
        }
        if definition.max_output_tokens.is_some()
            && definition.model_policy.max_output_tokens.is_some()
            && definition.max_output_tokens != definition.model_policy.max_output_tokens
        {
            return Err(
                AgentDefinitionValidationError::ConflictingModelPolicyField {
                    id: id.to_string(),
                    field: "max_output_tokens".to_string(),
                },
            );
        }
        if let Some(max_output_tokens) = definition
            .max_output_tokens
            .or(definition.model_policy.max_output_tokens)
        {
            let provider = definition.model_policy.provider.as_deref().unwrap_or("");
            let model = definition.model_policy.model.as_deref().unwrap_or("");
            let context_window_tokens =
                agent::resolved_model_context_window_tokens(provider, model);
            if max_output_tokens as usize >= context_window_tokens {
                return Err(
                    AgentDefinitionValidationError::OutputReserveExhaustsContextWindow {
                        id: id.to_string(),
                        max_output_tokens,
                        context_window_tokens,
                    },
                );
            }
        }
        if let Some(max_steps) = definition.runtime_limits.max_steps
            && max_steps == 0
        {
            return Err(AgentDefinitionValidationError::InvalidRuntimeLimit {
                id: id.to_string(),
                field: "runtime_limits.max_steps".to_string(),
            });
        }
        if let Some(max_tool_calls_per_step) = definition.runtime_limits.max_tool_calls_per_step
            && max_tool_calls_per_step == 0
        {
            return Err(AgentDefinitionValidationError::InvalidRuntimeLimit {
                id: id.to_string(),
                field: "runtime_limits.max_tool_calls_per_step".to_string(),
            });
        }
        if let Some(node_id) = definition.tool_runtime.sandbox_node_id.as_deref()
            && !is_valid_agent_sandbox_runtime_node_id(node_id)
        {
            return Err(
                AgentDefinitionValidationError::InvalidSandboxRuntimeNodeId {
                    id: id.to_string(),
                    node_id: node_id.to_string(),
                },
            );
        }
        let mut mcp_server_namespaces = BTreeSet::new();
        for server in &definition.tool_runtime.mcp_servers {
            validate_agent_mcp_server(id, server)?;
            let namespace = server
                .name
                .chars()
                .map(|character| match character {
                    '-' | '_' => '_',
                    character => character.to_ascii_lowercase(),
                })
                .collect::<String>();
            if !mcp_server_namespaces.insert(namespace) {
                return Err(AgentDefinitionValidationError::DuplicateMcpServer {
                    id: id.to_string(),
                    server: server.name.clone(),
                });
            }
        }
        if let Some(node_id) = definition.placement.home_server_node_id.as_deref()
            && !is_valid_agent_sandbox_runtime_node_id(node_id)
        {
            return Err(AgentDefinitionValidationError::InvalidHomeServerNodeId {
                id: id.to_string(),
                node_id: node_id.to_string(),
            });
        }
        if !self
            .context_strategies
            .contains(&definition.context.pipeline_id)
        {
            return Err(AgentDefinitionValidationError::UnknownContextPipeline {
                id: id.to_string(),
                pipeline_id: definition.context.pipeline_id.clone(),
            });
        }
        if !self
            .compaction_strategies
            .contains(&definition.compaction.strategy_id)
        {
            return Err(AgentDefinitionValidationError::UnknownCompactionStrategy {
                id: id.to_string(),
                strategy_id: definition.compaction.strategy_id.clone(),
            });
        }
        Ok(())
    }
}

fn validate_agent_mcp_server(
    agent_id: &str,
    server: &AgentMcpServerDefinition,
) -> Result<(), AgentDefinitionValidationError> {
    server
        .validate()
        .map_err(|detail| AgentDefinitionValidationError::InvalidMcpServer {
            id: agent_id.to_string(),
            server: server.name.clone(),
            detail,
        })
}

pub(crate) fn mcp_header_name_is_transport_controlled(value: &str) -> bool {
    matches!(
        value,
        "accept"
            | "authorization"
            | "connection"
            | "content-length"
            | "content-type"
            | "host"
            | "mcp-method"
            | "mcp-name"
            | "mcp-protocol-version"
            | "mcp-session-id"
            | "transfer-encoding"
            | "user-agent"
    ) || value.starts_with("mcp-param-")
}

pub(crate) fn validate_mcp_env_name(value: &str) -> Result<(), String> {
    let mut characters = value.chars();
    let Some(first) = characters.next() else {
        return Err("environment-variable name must not be empty".to_string());
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(format!(
            "environment-variable name `{value}` contains unsupported characters"
        ));
    }
    if !value.starts_with("SINOMO_MCP_") {
        return Err(format!(
            "environment-variable name `{value}` must use the dedicated `SINOMO_MCP_` prefix"
        ));
    }
    Ok(())
}

fn validate_tool_surface_entry(
    id: &str,
    field: &str,
    value: &str,
) -> Result<(), AgentDefinitionValidationError> {
    if is_valid_agent_package_identifier(value) {
        return Ok(());
    }
    Err(AgentDefinitionValidationError::InvalidToolSurfaceEntry {
        id: id.to_string(),
        field: field.to_string(),
        value: value.to_string(),
    })
}

fn validate_model_preference(
    id: &str,
    field: &str,
    value: &str,
) -> Result<(), AgentDefinitionValidationError> {
    if is_valid_agent_model_preference(value) {
        return Ok(());
    }
    Err(AgentDefinitionValidationError::InvalidModelPreference {
        id: id.to_string(),
        field: field.to_string(),
        value: value.to_string(),
    })
}

fn is_valid_agent_sandbox_runtime_node_id(value: &str) -> bool {
    NodeId::new(value.to_string()).is_ok() || NodeId::from_legacy_named(value.to_string()).is_ok()
}

fn validate_temperature(
    id: &str,
    field: &str,
    value: f32,
) -> Result<(), AgentDefinitionValidationError> {
    if value.is_finite() && value >= 0.0 {
        return Ok(());
    }
    Err(AgentDefinitionValidationError::InvalidTemperature {
        id: id.to_string(),
        field: field.to_string(),
        value: value.to_string(),
    })
}

fn is_valid_agent_definition_id(id: &str) -> bool {
    !id.is_empty() && id.bytes().all(is_valid_agent_package_identifier_byte)
}

fn is_valid_agent_package_identifier(value: &str) -> bool {
    !value.is_empty()
        && value == value.trim()
        && value.bytes().all(is_valid_agent_package_identifier_byte)
}

fn is_valid_agent_package_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
}

fn is_valid_agent_model_preference(value: &str) -> bool {
    !value.is_empty()
        && value == value.trim()
        && value
            .bytes()
            .all(|byte| !byte.is_ascii_control() && !byte.is_ascii_whitespace())
}

fn is_valid_agent_package_version(version: &str) -> bool {
    !version.is_empty()
        && version == version.trim()
        && version.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'+' | b':')
        })
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AgentCatalog {
    definitions: BTreeMap<String, AgentDefinition>,
}

impl AgentCatalog {
    pub fn built_ins() -> Self {
        let mut catalog = Self::default();
        for definition in AgentDefinition::built_ins() {
            catalog
                .upsert(definition)
                .expect("built-in agent definition should validate");
        }
        catalog
    }

    pub fn load_from_data_dir(
        data_dir: &sinomo_runtime::local::SinomoDataDir,
    ) -> Result<Self, AgentCatalogError> {
        let mut catalog =
            Self::load_from_store(AgentDefinitionSifsStore::open(Some(data_dir.root()))?)?;
        catalog.merge_product_modules(data_dir)?;
        Ok(catalog)
    }

    /// Load the catalog through a caller-selected SIFS transport.
    ///
    /// Callers that already have a resident daemon read transport use this
    /// path so opening a catalog does not construct another DFS service.
    /// [`Self::load_from_data_dir`] remains an explicit mounted read path;
    /// mutation callers construct [`AgentDefinitionSifsStore::open`] directly.
    pub fn load_from_vfs(vfs: sinomo_runtime::vfs::VfsHandle) -> Result<Self, AgentCatalogError> {
        let mut catalog = Self::load_from_store(AgentDefinitionSifsStore::from_vfs(vfs)?)?;
        if let Ok(data_dir) = sinomo_runtime::local::SinomoDataDir::from_env_or_home() {
            catalog.merge_product_modules(&data_dir)?;
        }
        Ok(catalog)
    }

    fn load_from_store(store: AgentDefinitionSifsStore) -> Result<Self, AgentCatalogError> {
        let mut catalog = Self::built_ins();
        for definition in store.load_definitions()? {
            catalog.upsert(definition)?;
        }
        Ok(catalog)
    }

    fn merge_product_modules(
        &mut self,
        data_dir: &sinomo_runtime::local::SinomoDataDir,
    ) -> Result<(), AgentCatalogError> {
        let cluster_initialized = sinomo_runtime::network::ClusterPaths::new(data_dir)
            .state
            .exists();
        if let Ok(components) = sinomo_product_profile::base_component_catalog(
            data_dir,
            sinomo_product_profile::ProductComponentProfile::main(cluster_initialized),
        ) {
            self.merge_module_registry(components.modules())?;
        }
        Ok(())
    }

    pub fn upsert(
        &mut self,
        mut definition: AgentDefinition,
    ) -> Result<Option<AgentDefinition>, AgentDefinitionValidationError> {
        if definition.id.is_empty() {
            definition.id = definition.name.clone();
        }
        definition.validate()?;
        definition.validate_catalog_identity()?;
        Ok(self
            .definitions
            .insert(definition.key().to_string(), definition))
    }

    pub fn get(&self, id: &str) -> Option<&AgentDefinition> {
        self.definitions.get(id)
    }

    pub fn list(&self) -> impl Iterator<Item = &AgentDefinition> {
        self.definitions.values()
    }

    pub fn merge_module_registry(
        &mut self,
        registry: &sinomo_runtime::module::ModuleRegistry,
    ) -> Result<(), AgentCatalogError> {
        for registration in registry.registrations() {
            for export in &registration.surfaces.agent_definitions {
                let mut definition = AgentDefinition::try_from_module_export(export)?;
                definition.package = AgentPackageDefinition::from_module(
                    registration.module_id().as_str(),
                    registration.manifest.version.clone(),
                );
                if let Some(existing) = self.get(definition.key()) {
                    return Err(AgentCatalogError::AgentIdCollision {
                        id: definition.key().to_string(),
                        existing_source: existing.package.provenance.source.clone(),
                        module_id: registration.module_id().as_str().to_string(),
                    });
                }
                self.upsert(definition)?;
            }
        }
        Ok(())
    }
}

/// Resolve independently callable module routes for agent-facing projection.
///
/// Product composition remains the authority for which registrations exist;
/// this function does not discover modules or grant route capabilities.
pub fn product_module_tool_projection(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> Result<AgentModuleToolProjection, String> {
    let cluster_initialized = sinomo_runtime::network::ClusterPaths::new(data_dir)
        .state
        .exists();
    let components = sinomo_product_profile::base_component_catalog(
        data_dir,
        sinomo_product_profile::ProductComponentProfile::main(cluster_initialized),
    )
    .map_err(|error| error.to_string())?;
    Ok(AgentModuleToolProjection::from_registry(
        components.modules(),
    ))
}

#[derive(Clone, Debug, Error)]
pub enum AgentCatalogError {
    #[error(transparent)]
    Store(#[from] AgentDefinitionStoreError),
    #[error(transparent)]
    Validation(#[from] AgentDefinitionValidationError),
    #[error(
        "module `{module_id}` agent `{id}` collides with an existing {existing_source:?} agent package"
    )]
    AgentIdCollision {
        id: String,
        existing_source: AgentPackageSource,
        module_id: String,
    },
}

pub trait AgentDefinitionStore {
    fn load_definitions(&self) -> Result<Vec<AgentDefinition>, AgentDefinitionStoreError>;
    fn save_definition(
        &self,
        definition: &AgentDefinition,
    ) -> Result<String, AgentDefinitionStoreError>;
    fn delete_definition(&self, id: &str) -> Result<bool, AgentDefinitionStoreError>;
}

#[derive(Debug)]
pub struct AgentDefinitionSifsStore {
    root: String,
    objects: sinomo_runtime::vfs::DirectoryObjectStore<AgentDefinition>,
}

impl AgentDefinitionSifsStore {
    pub fn open(data_dir: Option<PathBuf>) -> Result<Self, AgentDefinitionStoreError> {
        Self::open_with_handle(sinomo_runtime::vfs::mounted_sifs(data_dir))
    }

    /// Open the definition store over an already selected VFS transport.
    ///
    /// This lets read-only callers use the daemon-resident SIFS service while
    /// retaining the mounted authority path for catalog mutations.
    pub fn from_vfs(
        vfs: sinomo_runtime::vfs::VfsHandle,
    ) -> Result<Self, AgentDefinitionStoreError> {
        Self::open_with_handle(Ok(vfs))
    }

    fn open_with_handle(
        handle: sinomo_runtime::vfs::Result<sinomo_runtime::vfs::VfsHandle>,
    ) -> Result<Self, AgentDefinitionStoreError> {
        let handle = handle
            .and_then(|handle| handle.scoped(AGENT_DEFINITION_SIFS_ROOT))
            .map_err(agent_definition_vfs_error)?;
        let objects = sinomo_runtime::vfs::DirectoryObjectStore::json(handle, "json");
        Ok(Self {
            root: AGENT_DEFINITION_SIFS_ROOT.to_string(),
            objects,
        })
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    fn validate_definition_id(&self, id: &str) -> Result<(), AgentDefinitionStoreError> {
        if !is_valid_agent_definition_id(id) {
            return Err(AgentDefinitionStoreError::Validation(
                AgentDefinitionValidationError::InvalidId { id: id.to_string() },
            ));
        }
        Ok(())
    }
}

impl AgentDefinitionStore for AgentDefinitionSifsStore {
    fn load_definitions(&self) -> Result<Vec<AgentDefinition>, AgentDefinitionStoreError> {
        let mut definitions = self
            .objects
            .load_all()
            .map_err(agent_definition_vfs_error)?;
        for definition in &mut definitions {
            if definition.id.is_empty() {
                definition.id = definition.name.clone();
            }
            definition.validate()?;
        }
        Ok(definitions)
    }

    fn save_definition(
        &self,
        definition: &AgentDefinition,
    ) -> Result<String, AgentDefinitionStoreError> {
        let mut definition = definition.clone();
        if definition.id.is_empty() {
            definition.id = definition.name.clone();
        }
        definition.validate()?;
        if AgentDefinition::built_in(definition.key()).is_some() {
            return Err(AgentDefinitionStoreError::Validation(
                AgentDefinitionValidationError::ReservedBuiltInId {
                    id: definition.key().to_string(),
                },
            ));
        }
        self.validate_definition_id(definition.key())?;
        self.objects
            .save(definition.key(), &definition)
            .map_err(agent_definition_vfs_error)
    }

    fn delete_definition(&self, id: &str) -> Result<bool, AgentDefinitionStoreError> {
        if AgentDefinition::built_in(id).is_some() {
            return Err(AgentDefinitionStoreError::Validation(
                AgentDefinitionValidationError::ReservedBuiltInId { id: id.to_string() },
            ));
        }
        self.validate_definition_id(id)?;
        self.objects.delete(id).map_err(agent_definition_vfs_error)
    }
}

fn agent_definition_vfs_error(source: sinomo_runtime::vfs::VfsError) -> AgentDefinitionStoreError {
    let path = source
        .path()
        .unwrap_or(AGENT_DEFINITION_SIFS_ROOT)
        .to_string();
    match source {
        sinomo_runtime::vfs::VfsError::Json { detail, .. }
        | sinomo_runtime::vfs::VfsError::Sobj { detail, .. } => {
            AgentDefinitionStoreError::Json { path, detail }
        }
        other => AgentDefinitionStoreError::Io {
            path,
            detail: other.to_string(),
        },
    }
}

#[derive(Clone, Debug, Error)]
pub enum AgentDefinitionStoreError {
    #[error("agent definition store I/O failed at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("agent definition JSON failed at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error(transparent)]
    Validation(#[from] AgentDefinitionValidationError),
}

impl AgentDefinition {
    pub fn try_from_module_export(
        export: &sinomo_runtime::module::ModuleAgentDefinitionExport,
    ) -> Result<Self, AgentDefinitionValidationError> {
        let role = parse_agent_role(&export.role).ok_or_else(|| {
            AgentDefinitionValidationError::UnknownRole {
                id: export.id.clone(),
                role: export.role.clone(),
            }
        })?;
        let mut definition = AgentDefinition::custom(&export.name, role, &export.system_prompt);
        definition.id = export.id.clone();
        definition.description = export.description.clone();
        definition.skills = export.skills.clone();
        definition.tool_surface.visible_toolsets = export.visible_toolsets.clone();
        definition.context.pipeline_id = export
            .context_pipeline_id
            .clone()
            .unwrap_or_else(|| DEFAULT_AGENT_CONTEXT_PIPELINE_ID.to_string());
        definition.compaction.strategy_id = export
            .compaction_strategy_id
            .clone()
            .unwrap_or_else(|| DEFAULT_AGENT_COMPACTION_STRATEGY_ID.to_string());
        definition.tools = export
            .tools
            .iter()
            .map(|tool| {
                let kind = AgentToolKind::from_identifier(&tool.kind).ok_or_else(|| {
                    AgentDefinitionValidationError::UnknownToolKind {
                        id: export.id.clone(),
                        kind: tool.kind.clone(),
                    }
                })?;
                Ok(AgentToolSpec::new(
                    kind,
                    tool.name.clone(),
                    tool.description.clone(),
                    tool.async_default,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        definition.validate()?;
        Ok(definition)
    }
}

fn parse_agent_role(value: &str) -> Option<AgentRole> {
    match value {
        "main" | "main-agent" => Some(AgentRole::Main),
        "background" | "background-agent" => Some(AgentRole::Background),
        "deliberation" | "coding" | "coding-agent" | "deliberation-agent" => {
            Some(AgentRole::Deliberation)
        }
        "evolution" | "evolution-agent" => Some(AgentRole::Evolution),
        _ => None,
    }
}

pub fn default_main_agent_tools() -> Vec<AgentToolSpec> {
    let mut tools = default_coding_agent_tools();
    tools.extend(default_sinomo_system_tools());
    tools
}

fn default_sinomo_system_tools() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::ConfigSearch,
            "config_search",
            "Search registered Sinomo configuration keys by key, owner, or description.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::ConfigGet,
            "config_get",
            "Read a registered Sinomo configuration value through the shared config resolver.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::ConfigSet,
            "config_set",
            "Update a registered Sinomo configuration value through the shared config resolver.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::ClusterStatus,
            "cluster_status",
            "Read a live cluster-wide health summary from the local control plane.",
            false,
        ),
    ]
}

pub fn default_background_agent_tools() -> Vec<AgentToolSpec> {
    let mut tools = default_agent_recall_tools();
    tools.extend(default_workspace_read_tools());
    tools.extend(default_sinomo_system_tools());
    tools
}

pub fn default_coding_agent_tools() -> Vec<AgentToolSpec> {
    let mut tools = default_agent_recall_tools();
    tools.extend(default_workspace_read_tools());
    tools.extend([
        AgentToolSpec::new(
            AgentToolKind::EditFile,
            "edit_file",
            "Edit one workspace text file by replacing exact text. Use old_text/new_text for targeted changes; old_text must be unique unless replace_all is true.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::WriteFile,
            "write_file",
            "Create a workspace text file or intentionally replace a whole file. Existing files require overwrite=true.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::RunShellCommand,
            "run_command",
            "Run a generic host command through simple-sandbox. Commands start in the workspace root and may access any host path admitted by runtime policy; structured workspace file permissions do not make shell commands read-only. Omit cwd unless using a known workspace subdirectory.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::RequestUserInput,
            "request_user_input",
            "Ask the user for a short clarification through the interactive runtime.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::ToolSearch,
            "tool_search",
            "Search all planned agent tools, including hidden module/plugin tools, without executing them.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::ToolDescribe,
            "tool_describe",
            "Describe one planned agent tool, including schema, source, visibility, and execution availability.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::ToolCall,
            "tool_call",
            "Call a planned tool discovered through tool_search/tool_describe while preserving tool-surface availability and evidence handling.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::SkillSearch,
            "skill_search",
            "Search the available Agent Skills catalog by name or description.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::SkillDescribe,
            "skill_describe",
            "Inspect Agent Skills metadata and provenance without activating its instructions.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::SkillActivate,
            "skill_activate",
            "Load the selected Agent Skills SKILL.md instructions and list bundled resources.",
            false,
        ),
    ]);
    tools
}

fn default_workspace_read_tools() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "Read a workspace file or one-based line range into model-visible text with a bounded byte budget; prefer line ranges for focused snippets.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::ListFiles,
            "list_files",
            "List workspace files and directories with bounded recursion; use this before broad shell commands when discovering workspace structure.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::SearchFiles,
            "search",
            "Search workspace text files for a literal query.",
            false,
        ),
    ]
}

fn default_agent_recall_tools() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::SessionHistorySearch,
            "session_history_search",
            "Search chats, channels, and background activities this agent engaged in without exposing raw model-session records.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::MemorySearch,
            "memory_search",
            "Search this agent's Markdown memory notes in its SIFS home.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::MemoryRead,
            "memory_read",
            "Read a bounded versioned Markdown memory note from this agent's SIFS home.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::MemoryWrite,
            "memory_write",
            "Create or compare-and-replace a Markdown memory note in this agent's SIFS home.",
            true,
        ),
    ]
}

fn main_agent_base_prompt() -> &'static str {
    r#"You are Sinomo, the user-facing main agent.

Operate as a responsive, event-driven agent. Answer directly when the request
does not need a tool. The main agent has the same workspace inspection,
editing, command, interactive, discovery, and skill tools as the coding agent
whenever the conversation has an explicit workspace grant. Use them to
complete coding and other workspace tasks directly. Commands start at the
granted workspace root and execute through simple-sandbox; respect its runtime
policy and do not claim stronger containment than it provides.

Use config_search, config_get, config_set, and cluster_status for Sinomo system
inspection and configuration. Use tool_search, tool_describe, and tool_call to
discover and invoke available module, plugin, remote-node, and integration tools
without bypassing their runtime availability or node-local authority checks.
Keep tool work bounded; if the available tools cannot complete the request,
explain the concrete limitation instead of promising unavailable background or
asynchronous work.

Coding advice, document drafting, image/file reasoning from provided context,
critique, and role-play should return the actual user-facing result in `text`,
not a promise that you will produce it. Use a short progress note only while
invoking an available tool in the current turn."#
}

fn background_agent_base_prompt() -> &'static str {
    "You are Sinomo's background agent. Stay silent unless escalation is needed. Coalesce maintenance events, inspect evidence through files and tools, prefer minimal operational fixes, and report concise findings back to the main agent."
}

fn coding_agent_base_prompt() -> &'static str {
    r#"You are Sinomo's general workspace agent model.

You operate inside Sinomo's coding-agent loop. The runtime owns tool execution,
tool-surface availability, policy admission, sandbox lifecycle, event emission,
and session state. Use tools when evidence or workspace changes are needed,
then continue after tool_result messages arrive.

The workspace may contain source code, documents, datasets, media, arbitrary
files, or nothing yet. Do not assume it is a Git repository, a Cargo project,
or even a software project, and do not assume Git, Cargo, or any particular
toolchain is installed. Infer the task and available tools from workspace
evidence. Commands start in the configured workspace root. Do not cd to guessed
paths such as /workspace or /home/user/project; pass a relative cwd only for a
known workspace subdirectory. Prefer read_file, list_files, and search for
focused inspection. Use rg or rg --files for shell search only when rg is
actually available.

run_command is a generic host-command tool executed through simple-sandbox. It
is not development-specific and is not made read-only by the structured
file-tool permission flag. The runtime still performs policy admission, limits,
cancellation, leases, and audit, but simple-sandbox is a best-effort permission
boundary hint rather than a hard anti-escape security sandbox. Use ambient host
access intentionally and do not claim stronger containment than the runtime
provides.

SIFS/DFS paths exposed by the runtime are live POSIX mounts. Reads and writes
synchronize through the filesystem continuously, independently of checkpoint
state. Checkpoints track and restore work; they are not command commits, flush
boundaries, copy-back steps, or synchronization gates.

Command stdout and stderr are returned in the next tool_result. Treat that
output as the diagnostic and adjust from it instead of repeating the same
command blindly. If validation is blocked by missing dependencies or host setup,
inspect the current state and finish with the blocker rather than looping on
environment setup unless the task is dependency setup.

Use edit_file for targeted text edits and write_file only for intentional
new-file creation or whole-file replacement. Keep edits focused. Do not revert
or overwrite user changes you did not make. edit_file requires exact
old_text/new_text; when old_text matches multiple places, provide more
surrounding context or explicitly set replace_all. write_file refuses to replace
existing files unless overwrite=true. Formatting commands and generated output
may use suitable host tools through run_command when those tools exist. If
apply_patch is visible in your tool list, you may use it for scoped patch edits;
otherwise discover provider-specific editing tools through tool_search only
when the selected model/provider has a reliable native patch capability.

When the next step is clear and no more tools are needed, return an empty
tool_calls array. If a tool fails or is blocked, recover from the model-visible
tool_result instead of repeating the same call blindly."#
}

pub fn main_agent_system_prompt(tools: &[AgentToolSpec]) -> String {
    let definition = AgentDefinition::main().with_tools(tools.to_vec());
    render_agent_system_prompt(&definition, tools)
}

pub fn background_agent_system_prompt() -> String {
    background_agent_base_prompt().to_string()
}

pub fn render_agent_package_system_prompt(definition: &AgentDefinition) -> String {
    definition.system_prompt.trim().to_string()
}

pub fn render_agent_system_prompt(definition: &AgentDefinition, tools: &[AgentToolSpec]) -> String {
    let tools_json = serde_json::to_string_pretty(tools).expect("tool specs serialize");
    render_agent_system_prompt_with_tools_json(definition, "Available tools", &tools_json)
}

pub fn render_agent_system_prompt_with_descriptors(
    definition: &AgentDefinition,
    tools: &[AgentToolDescriptor],
) -> String {
    let tools_json = serde_json::to_string_pretty(tools).expect("tool descriptors serialize");
    render_agent_system_prompt_with_tools_json(definition, "Available agent tools", &tools_json)
}

fn render_agent_system_prompt_with_tools_json(
    definition: &AgentDefinition,
    tool_label: &str,
    tools_json: &str,
) -> String {
    let mut prompt = definition.system_prompt.trim().to_string();
    match definition.output_format {
        AgentOutputFormat::JsonDecision => {
            prompt.push_str("\n\n");
            prompt.push_str(agent_decision_output_contract());
        }
        AgentOutputFormat::Text => {
            prompt.push_str("\n\nRespond directly in plain text.");
        }
    }
    prompt.push_str("\n\n");
    prompt.push_str(tool_label);
    prompt.push_str(":\n```json\n");
    prompt.push_str(tools_json);
    prompt.push_str("\n```");
    prompt
}

pub fn agent_output_contract(output_format: &AgentOutputFormat) -> &'static str {
    match output_format {
        AgentOutputFormat::JsonDecision => agent_decision_output_contract(),
        AgentOutputFormat::Text => "Respond directly in plain text.",
    }
}

pub fn agent_decision_output_contract() -> &'static str {
    r#"Return only JSON shaped as:
{
  "response_mode": "answer|acknowledge|clarify|defer|silent",
  "text": "complete user-facing answer or requested artifact; use a concise working note only when tool_calls or background_handoff are present",
  "tool_calls": [
    {
      "tool": "tool name",
      "input": {},
      "async": false,
      "summary": "why this tool is being used"
    }
  ],
  "background_handoff": null,
  "next_state": "speaking|listening|waiting|delegating|recovering|idle"
}"#
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AsyncTaskUpdate {
    pub kind: String,
    pub task_id: TaskId,
    pub status: AsyncTaskStatus,
    pub summary: String,
    pub needs_user_attention: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    pub next_action_hint: AsyncTaskNextAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AsyncTaskStatus {
    Running,
    Progress,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AsyncTaskNextAction {
    ContinueSilently,
    TellUser,
    AskUser,
    HandoffToBackground,
    InspectResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentLoopInput {
    pub session: InteractionSession,
    pub latest_event: EventEnvelope,
    /// Per-turn control context that is visible to the model as system
    /// instructions but is not appended to the durable session transcript.
    ///
    /// This is intended for surface-owned orchestration such as channel
    /// participation policy. User-visible content belongs in `latest_event`
    /// (or an already reconciled session item), not in this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transient_system_context: Option<String>,
    #[serde(default)]
    pub async_update: Option<AsyncTaskUpdate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_attachments: Vec<AgentContextAttachment>,
    #[serde(default)]
    pub tool_specs: Vec<AgentToolSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentLoopDecision {
    pub response_mode: AgentLoopResponseMode,
    pub text: String,
    #[serde(default)]
    pub tool_calls: Vec<AgentToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_handoff: Option<BackgroundHandoffRequest>,
    pub next_state: AgentInteractionState,
}

impl AgentLoopDecision {
    pub fn silent() -> Self {
        Self {
            response_mode: AgentLoopResponseMode::Silent,
            text: String::new(),
            tool_calls: Vec::new(),
            background_handoff: None,
            next_state: AgentInteractionState::Idle,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentLoopResponseMode {
    Answer,
    Acknowledge,
    Clarify,
    Defer,
    Silent,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentInteractionState {
    Speaking,
    Listening,
    Waiting,
    Delegating,
    Recovering,
    Idle,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    pub tool: String,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub r#async: bool,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BackgroundHandoffRequest {
    pub reason: String,
    pub evidence_ref: String,
    pub priority: BackgroundHandoffPriority,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum BackgroundHandoffPriority {
    Routine,
    Important,
    Emergency,
}

pub fn parse_agent_loop_response(
    output_format: &AgentOutputFormat,
    raw: &str,
) -> Result<AgentLoopDecision, AgentError> {
    match output_format {
        AgentOutputFormat::JsonDecision => parse_agent_loop_decision(raw),
        AgentOutputFormat::Text => Ok(AgentLoopDecision {
            response_mode: AgentLoopResponseMode::Answer,
            text: raw.trim().to_string(),
            tool_calls: Vec::new(),
            background_handoff: None,
            next_state: AgentInteractionState::Listening,
        }),
    }
}

pub fn parse_agent_loop_decision(raw: &str) -> Result<AgentLoopDecision, AgentError> {
    let trimmed = raw.trim();
    let json = trimmed
        .strip_prefix("```json")
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    match serde_json::from_str(json) {
        Ok(decision) => Ok(decision),
        Err(source) => {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(json)
                && let Some(text) = value.get("text").and_then(serde_json::Value::as_str)
            {
                return Ok(AgentLoopDecision {
                    response_mode: value
                        .get("response_mode")
                        .and_then(serde_json::Value::as_str)
                        .and_then(parse_agent_loop_response_mode)
                        .unwrap_or(AgentLoopResponseMode::Answer),
                    text: text.to_string(),
                    tool_calls: value
                        .get("tool_calls")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .unwrap_or_default(),
                    background_handoff: value
                        .get("background_handoff")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok()),
                    next_state: value
                        .get("next_state")
                        .and_then(serde_json::Value::as_str)
                        .and_then(parse_agent_interaction_state)
                        .unwrap_or(AgentInteractionState::Listening),
                });
            }
            if trimmed.is_empty() {
                Err(AgentError::ModelResponse {
                    detail: format!("{source}: {raw}"),
                })
            } else {
                Ok(AgentLoopDecision {
                    response_mode: AgentLoopResponseMode::Answer,
                    text: trimmed.to_string(),
                    tool_calls: Vec::new(),
                    background_handoff: None,
                    next_state: AgentInteractionState::Listening,
                })
            }
        }
    }
}

fn parse_agent_loop_response_mode(value: &str) -> Option<AgentLoopResponseMode> {
    match value {
        "answer" => Some(AgentLoopResponseMode::Answer),
        "acknowledge" => Some(AgentLoopResponseMode::Acknowledge),
        "clarify" => Some(AgentLoopResponseMode::Clarify),
        "defer" => Some(AgentLoopResponseMode::Defer),
        "silent" => Some(AgentLoopResponseMode::Silent),
        _ => None,
    }
}

fn parse_agent_interaction_state(value: &str) -> Option<AgentInteractionState> {
    match value {
        "speaking" => Some(AgentInteractionState::Speaking),
        "listening" => Some(AgentInteractionState::Listening),
        "waiting" => Some(AgentInteractionState::Waiting),
        "delegating" => Some(AgentInteractionState::Delegating),
        "recovering" => Some(AgentInteractionState::Recovering),
        "idle" => Some(AgentInteractionState::Idle),
        _ => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractionSession {
    pub session_id: SessionId,
    pub node_id: NodeId,
    pub surface: InteractionSurface,
}

impl InteractionSession {
    pub fn new(session_id: SessionId, node_id: NodeId, surface: InteractionSurface) -> Self {
        Self {
            session_id,
            node_id,
            surface,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractionSurface {
    pub surface_id: String,
    pub kind: InteractionSurfaceKind,
}

impl InteractionSurface {
    pub fn new(surface_id: impl Into<String>, kind: InteractionSurfaceKind) -> InteractionSurface {
        InteractionSurface {
            surface_id: surface_id.into(),
            kind,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractionSurfaceKind {
    CommandLine,
    App,
    Api,
    Voice,
    Remote,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractionInputKind {
    Message,
    Command,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InteractionOutput {
    Chunk(String),
    Line(String),
    ModelUsage {
        provider: String,
        model: String,
        usage: LanguageModelUsage,
    },
    Exit,
}

pub trait InteractionModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError>;

    fn complete_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation_token: &AgentCancellationToken,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if cancellation_token.is_cancelled() {
            return Err(cancelled_language_model_error(request, "before request"));
        }
        let response = self.complete(request)?;
        if cancellation_token.is_cancelled() {
            return Err(cancelled_language_model_error(request, "after response"));
        }
        Ok(response)
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let _ = on_delta;
        self.complete(request)
    }

    fn complete_streaming_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation_token: &AgentCancellationToken,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if cancellation_token.is_cancelled() {
            return Err(cancelled_language_model_error(request, "before request"));
        }
        let response = self.complete_streaming(request, &mut |delta| {
            if !cancellation_token.is_cancelled() {
                on_delta(delta);
            }
        })?;
        if cancellation_token.is_cancelled() {
            return Err(cancelled_language_model_error(request, "during response"));
        }
        Ok(response)
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        on_event(LanguageModelStreamEvent::Started {
            provider: request.provider.clone(),
            model: request.model.clone(),
        });
        let mut emitted_delta = false;
        let response = self.complete_streaming(request, &mut |delta| {
            emitted_delta = true;
            on_event(LanguageModelStreamEvent::TextDelta {
                delta: delta.to_string(),
            });
        })?;
        if !emitted_delta && !response.content.is_empty() {
            on_event(LanguageModelStreamEvent::TextDelta {
                delta: response.content.clone(),
            });
        }
        if let Some(usage) = &response.usage {
            on_event(LanguageModelStreamEvent::Usage {
                usage: usage.clone(),
            });
        }
        on_event(LanguageModelStreamEvent::Finished {
            finish_reason: None,
        });
        Ok(response)
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation_token: &AgentCancellationToken,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if cancellation_token.is_cancelled() {
            return Err(cancelled_language_model_error(request, "before request"));
        }
        let response = self.complete_streaming_events(request, &mut |event| {
            if !cancellation_token.is_cancelled() {
                on_event(event);
            }
        })?;
        if cancellation_token.is_cancelled() {
            return Err(cancelled_language_model_error(request, "during response"));
        }
        Ok(response)
    }
}

fn cancelled_language_model_error(
    request: &LanguageModelRequest,
    phase: &'static str,
) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: request.provider.clone(),
        model: request.model.clone(),
        detail: format!("model invocation cancelled {phase}"),
    }
}

impl<T: LanguageModelProvider> InteractionModel for T {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete(self, request)
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming(self, request, on_delta)
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming_events(self, request, on_event)
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation_token: &AgentCancellationToken,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let cancellation = cancellation_token.language_model_cancellation();
        LanguageModelProvider::complete_streaming_events_with_cancellation(
            self,
            request,
            &cancellation,
            on_event,
        )
    }
}

pub trait EventEnvelopeExt {
    fn caused_by(self, event_id: &EventId) -> Self;
}

impl EventEnvelopeExt for EventEnvelope {
    fn caused_by(mut self, event_id: &EventId) -> Self {
        self.links.caused_by.push(event_id.clone());
        self
    }
}

pub fn user_interaction_input_event(session: &InteractionSession, text: &str) -> EventEnvelope {
    let input_kind = if text.trim_start().starts_with('/') {
        InteractionInputKind::Command
    } else {
        InteractionInputKind::Message
    };
    interaction_event(
        session,
        "interaction.input.received",
        serde_json::json!({
            "text": text,
            "input_kind": input_kind,
            "surface": session.surface,
        }),
    )
}

pub fn interaction_event(
    session: &InteractionSession,
    kind: &str,
    payload: serde_json::Value,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        time::OffsetDateTime::now_utc(),
        session.node_id.clone(),
        Source::new("interaction.surface").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.62).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session.session_id.clone())
    .with_payload(payload)
}

pub fn interaction_model_request<S: EventStore>(
    store: &S,
    session: &InteractionSession,
    message_event_id: &EventId,
    text: &str,
    provider: &str,
    model: &str,
) -> Result<LanguageModelRequest, AgentError> {
    let recent = store.replay(ReplayQuery {
        session_id: Some(session.session_id.clone()),
        kind_prefix: Some("interaction.model.responded".to_string()),
        origin_node_id: None,
        after_sequence: None,
        start_time: None,
        end_time: None,
        direction: ReplayDirection::Reverse,
        limit: Some(6),
    })?;
    let mut recent_turns: Vec<String> = recent
        .into_iter()
        .rev()
        .filter_map(|stored| {
            stored
                .event
                .payload
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect();
    if recent_turns.is_empty() {
        recent_turns.push("No prior assistant turns in this session.".to_string());
    }

    Ok(LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        messages: vec![
            LanguageModelMessage {
                role: LanguageModelRole::System,
                content: INTERACTION_SYSTEM_PROMPT.to_string(),
            },
            LanguageModelMessage {
                role: LanguageModelRole::User,
                content: serde_json::to_string_pretty(&serde_json::json!({
                    "session_id": session.session_id,
                    "surface": session.surface,
                    "input_event_id": message_event_id,
                    "recent_assistant_turns": recent_turns,
                    "user_input": text
                }))
                .expect("interaction request serializes"),
            },
        ],
        items: Vec::new(),
        tools: Vec::new(),
        tool_choice: None,
        parallel_tool_calls: None,
        temperature: Some(0.4),
        max_output_tokens: Some(800),
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::json!({
            "loop": "interactive",
            "node_id": session.node_id,
            "session_id": session.session_id,
            "input_event_id": message_event_id
        }),
    })
}

const INTERACTION_SYSTEM_PROMPT: &str = r#"You are Sinomo's command-line conversation loop.
Respond directly to the user in plain text.
Use the supplied event ids and session metadata as context, but do not expose internal event JSON unless asked.
Slash commands are handled by Sinomo outside the model; ordinary user input reaches you only after being recorded as an interaction event."#;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error(transparent)]
    EventStore(#[from] sinomo_runtime::events::EventStoreError),
    #[error(transparent)]
    Model(#[from] LanguageModelError),
    #[error("failed to build model request: {detail}")]
    ModelRequest { detail: String },
    #[error("failed to parse model response: {detail}")]
    ModelResponse { detail: String },
    #[error("agent trace store failed: {detail}")]
    TraceStore { detail: String },
    #[error("agent session store failed: {detail}")]
    SessionStore { detail: String },
    #[error("agent runtime failed: {detail}")]
    Runtime { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::events::{EventId, EventStore, InMemoryEventStore};
    use sinomo_runtime::node::NodeId;
    use sinomo_runtime::vfs::{
        VfsAppendOptions, VfsBackend, VfsDirEntry, VfsError, VfsFileRead, VfsRemoveOptions,
        VfsWriteOptions,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[test]
    fn agent_metric_generation_advances_from_filesystem_events() {
        let root = sinomo_testdir::TestDir::new("agent-observe-generation");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(root.path());
        let initial = agent_observe_metric_generation(&data_dir).expect("install watcher");
        let paths = AgentStoragePaths::from_data_dir(&data_dir);
        std::fs::write(paths.root.join("observe-change"), b"changed")
            .expect("write watched change");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let current = agent_observe_metric_generation(&data_dir).expect("read generation");
            if current > initial {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "agent metric watcher did not invalidate the cached generation"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    struct FakeInteractionModel {
        response: &'static str,
    }

    #[derive(Debug)]
    struct ContentUnavailableCatalogVfsBackend {
        list_unavailable: bool,
    }

    impl ContentUnavailableCatalogVfsBackend {
        fn unavailable(&self, path: &str) -> VfsError {
            VfsError::Backend {
                backend: self.backend_name().to_string(),
                path: path.to_string(),
                detail: "authoritative metadata exists but catalog content is unavailable"
                    .to_string(),
            }
        }

        fn unsupported(&self, detail: &str) -> VfsError {
            VfsError::Unsupported {
                backend: self.backend_name().to_string(),
                detail: detail.to_string(),
            }
        }
    }

    impl VfsBackend for ContentUnavailableCatalogVfsBackend {
        fn backend_name(&self) -> &'static str {
            "content-unavailable-catalog"
        }

        fn stat(
            &self,
            path: &str,
        ) -> sinomo_runtime::vfs::Result<sinomo_runtime::vfs::VfsMetadata> {
            Err(VfsError::NotFound {
                path: path.to_string(),
            })
        }

        fn list(&self, path: &str) -> sinomo_runtime::vfs::Result<Vec<VfsDirEntry>> {
            if path == AGENT_DEFINITION_SIFS_ROOT {
                if self.list_unavailable {
                    return Err(VfsError::Backend {
                        backend: self.backend_name().to_string(),
                        path: path.to_string(),
                        detail: "daemon route not found while catalog metadata is unavailable"
                            .to_string(),
                    });
                }
                return Ok(vec![VfsDirEntry {
                    name: "unavailable-agent.json".to_string(),
                    path: format!("{AGENT_DEFINITION_SIFS_ROOT}/unavailable-agent.json"),
                    kind: VfsObjectKind::File,
                    size_bytes: Some(1),
                }]);
            }
            Err(VfsError::NotFound {
                path: path.to_string(),
            })
        }

        fn read(&self, path: &str) -> sinomo_runtime::vfs::Result<Vec<u8>> {
            Err(self.unavailable(path))
        }

        fn read_prefix(
            &self,
            path: &str,
            _max_bytes: u64,
        ) -> sinomo_runtime::vfs::Result<VfsFileRead> {
            Err(self.unavailable(path))
        }

        fn write(
            &self,
            _path: &str,
            _bytes: &[u8],
            _options: &VfsWriteOptions,
        ) -> sinomo_runtime::vfs::Result<()> {
            Err(self.unsupported("write"))
        }

        fn append(
            &self,
            _path: &str,
            _bytes: &[u8],
            _options: &VfsAppendOptions,
        ) -> sinomo_runtime::vfs::Result<()> {
            Err(self.unsupported("append"))
        }

        fn create_dir_all(&self, _path: &str) -> sinomo_runtime::vfs::Result<()> {
            Err(self.unsupported("create directory"))
        }

        fn rename(&self, _from: &str, _to: &str) -> sinomo_runtime::vfs::Result<()> {
            Err(self.unsupported("rename"))
        }

        fn remove(
            &self,
            _path: &str,
            _options: &VfsRemoveOptions,
        ) -> sinomo_runtime::vfs::Result<()> {
            Err(self.unsupported("remove"))
        }
    }

    impl InteractionModel for FakeInteractionModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            assert_eq!(
                request.response_format,
                LanguageModelResponseFormat::JsonObject
            );
            assert!(request.messages[0].content.contains("Available tools"));
            Ok(LanguageModelResponse {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                content: self.response.to_string(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }
    }

    struct CapturingInteractionModel {
        response: &'static str,
        requests: Arc<Mutex<Vec<LanguageModelRequest>>>,
    }

    impl CapturingInteractionModel {
        fn new(response: &'static str) -> Self {
            Self {
                response,
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl InteractionModel for CapturingInteractionModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            Ok(LanguageModelResponse {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                content: self.response.to_string(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }
    }

    struct CancellationProbingProvider {
        observed: Arc<AtomicBool>,
    }

    impl LanguageModelProvider for CancellationProbingProvider {
        fn info(&self) -> sinomo_lm::LanguageModelProviderInfo {
            sinomo_lm::LanguageModelProviderInfo {
                provider: "probe".to_string(),
                default_model: "probe".to_string(),
                invocation: sinomo_lm::LanguageModelInvocation::InProcess,
                host_access: sinomo_lm::LanguageModelHostAccess::Bounded,
                capabilities: sinomo_lm::LanguageModelProviderCapabilities::default(),
                supports_json_object: false,
                supports_tool_permissions: false,
            }
        }

        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            Ok(LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content: String::new(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }

        fn complete_streaming_events_with_cancellation(
            &self,
            request: &LanguageModelRequest,
            cancellation: &LanguageModelCancellation,
            _on_event: &mut dyn FnMut(LanguageModelStreamEvent),
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            self.observed.store(true, Ordering::SeqCst);
            while !cancellation.is_cancelled() {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(LanguageModelError::ProviderInvocationFailed {
                provider: request.provider.clone(),
                model: request.model.clone(),
                detail: "cancelled by provider probe".to_string(),
            })
        }
    }

    #[test]
    fn agent_cancellation_token_reaches_language_model_provider() {
        let observed = Arc::new(AtomicBool::new(false));
        let provider = CancellationProbingProvider {
            observed: Arc::clone(&observed),
        };
        let request = LanguageModelRequest {
            provider: "probe".to_string(),
            model: "probe".to_string(),
            messages: Vec::new(),
            items: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        };
        let cancellation_token = AgentCancellationToken::new();
        let cancel_worker = {
            let cancellation_token = cancellation_token.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(20));
                cancellation_token.cancel();
            })
        };

        let error = InteractionModel::complete_streaming_events_with_cancellation(
            &provider,
            &request,
            &cancellation_token,
            &mut |_| {},
        )
        .expect_err("provider invocation is cancelled");

        cancel_worker.join().expect("cancellation worker");
        assert!(observed.load(Ordering::SeqCst));
        assert!(error.to_string().contains("cancelled by provider probe"));
    }

    #[test]
    fn interaction_input_event_tags_slash_commands() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_interaction").expect("session"),
            NodeId::from_legacy_named("node_interaction").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );

        let event = user_interaction_input_event(&session, " /status");

        assert_eq!(
            event
                .payload
                .get("input_kind")
                .and_then(serde_json::Value::as_str),
            Some("command")
        );
        assert_eq!(event.session_id, Some(session.session_id));
    }

    #[test]
    fn interaction_events_can_record_causality() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_interaction").expect("session"),
            NodeId::from_legacy_named("node_interaction").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        let cause = EventId::from_legacy_named("evt_cause").expect("event");

        let event = interaction_event(
            &session,
            "interaction.message.received",
            serde_json::json!({ "text": "hello" }),
        )
        .caused_by(&cause);

        assert_eq!(event.links.caused_by, vec![cause]);
    }

    #[test]
    fn interaction_model_request_includes_recent_assistant_turns() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_interaction").expect("session"),
            NodeId::from_legacy_named("node_interaction").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        let mut store = InMemoryEventStore::new();
        store
            .append(interaction_event(
                &session,
                "interaction.model.responded",
                serde_json::json!({ "text": "previous answer" }),
            ))
            .expect("append");
        let message_event_id = EventId::from_legacy_named("evt_message").expect("event");

        let request = interaction_model_request(
            &store,
            &session,
            &message_event_id,
            "what next?",
            "provider",
            "model",
        )
        .expect("request");

        assert_eq!(request.provider, "provider");
        assert_eq!(request.model, "model");
        assert!(request.messages[1].content.contains("previous answer"));
        assert!(request.messages[1].content.contains("what next?"));
    }

    #[test]
    fn parse_agent_loop_response_accepts_json_tool_decision() {
        let decision = parse_agent_loop_response(
            &AgentOutputFormat::JsonDecision,
            r#"{
                "response_mode": "acknowledge",
                "text": "I will check that in the background.",
                "tool_calls": [{
                    "tool": "handoff_to_background",
                    "input": {"reason": "cluster check"},
                    "async": true,
                    "summary": "Ask background to inspect cluster health."
                }],
                "background_handoff": {
                    "reason": "cluster health check",
                    "evidence_ref": "sifs:/services/tasks/task_1/input.json",
                    "priority": "routine"
                },
                "next_state": "delegating"
            }"#,
        )
        .expect("decision");

        assert_eq!(decision.response_mode, AgentLoopResponseMode::Acknowledge);
        assert_eq!(decision.tool_calls[0].tool, "handoff_to_background");
        assert_eq!(decision.next_state, AgentInteractionState::Delegating);
    }

    #[test]
    fn main_and_background_packages_declare_production_inspection_tools() {
        let expected = [
            "read_file",
            "list_files",
            "search",
            "config_search",
            "config_get",
            "config_set",
        ];
        let removed = [
            "start_async_task",
            "inspect_async_task",
            "cancel_async_task",
            "run_shell_command",
            "handoff_to_background",
            "observe_vision",
            "read_sifs",
            "write_sifs",
            "list_sifs",
        ];

        for definition in [AgentDefinition::main(), AgentDefinition::background()] {
            let names = definition
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>();
            for expected in expected {
                assert!(
                    names.contains(&expected),
                    "{} is missing {expected}",
                    definition.key()
                );
            }
            for removed in removed {
                assert!(
                    !names.contains(&removed),
                    "{} still declares unavailable tool {removed}",
                    definition.key()
                );
            }
        }

        let main_names = AgentDefinition::main()
            .tools
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert!(main_names.iter().any(|name| name == "cluster_status"));

        let background_names = AgentDefinition::background()
            .tools
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert!(background_names.iter().any(|name| name == "cluster_status"));
    }

    #[test]
    fn main_agent_prompt_keeps_direct_interaction_deliverables_in_text() {
        let prompt = main_agent_system_prompt(&[]);

        assert!(prompt.contains("actual user-facing result in `text`"));
        assert!(prompt.contains("not a promise that you will produce it"));
        assert!(
            prompt.contains("complete user-facing answer or requested artifact"),
            "JSON contract should allow complete chat artifacts, not only short status notes"
        );
    }

    #[test]
    fn main_agent_prompt_uses_available_tools_without_fictional_handoffs() {
        let prompt = AgentDefinition::main().system_prompt;

        for expected in [
            "tool_search",
            "tool_describe",
            "tool_call",
            "inspection and configuration",
        ] {
            assert!(
                prompt.contains(expected),
                "missing prompt guidance for {expected}"
            );
        }
        for removed in [
            "start async tasks",
            "hand cluster maintenance",
            "background handoff",
            "delegate deliberate work",
        ] {
            assert!(
                !prompt.contains(removed),
                "main prompt still promises unavailable behavior: {removed}"
            );
        }
    }

    #[test]
    fn coding_agent_prompt_treats_workspace_and_shell_as_generic() {
        let definition = AgentDefinition::coding();
        let prompt = definition.system_prompt;
        let normalized_prompt = prompt.split_whitespace().collect::<Vec<_>>().join(" ");

        for expected in [
            "general workspace agent",
            "Do not assume it is a Git repository, a Cargo project",
            "run_command is a generic host-command tool",
            "structured file-tool permission flag",
            "SIFS/DFS paths exposed by the runtime are live POSIX mounts",
            "independently of checkpoint state",
        ] {
            assert!(
                normalized_prompt.contains(expected),
                "missing generic guidance: {expected}"
            );
        }
        assert_eq!(
            definition.description,
            "general workspace task and verification agent"
        );
        let run_command = definition
            .tools
            .iter()
            .find(|tool| tool.name == "run_command")
            .expect("run command tool");
        assert!(run_command.description.contains("generic host command"));
        assert!(!run_command.description.contains("Cargo"));
        assert!(!run_command.description.contains("Git"));
    }

    #[test]
    fn principal_authority_comes_only_from_reserved_built_in_identity() {
        assert_eq!(
            AgentDefinition::main().principal_authority(),
            AgentPrincipalAuthority {
                workspace_mutation: true,
                config_writes: true,
            }
        );
        assert!(
            AgentDefinition::coding()
                .principal_authority()
                .workspace_mutation
        );
        assert!(
            !AgentDefinition::coding()
                .principal_authority()
                .config_writes
        );
        assert!(
            !AgentDefinition::background()
                .principal_authority()
                .workspace_mutation
        );

        let custom_main = AgentDefinition::custom("my-main", AgentRole::Main, "Help.");
        assert_eq!(
            custom_main.principal_authority(),
            AgentPrincipalAuthority::default()
        );

        let mut spoofed = AgentDefinition::custom("imposter", AgentRole::Main, "Help.");
        spoofed.package.provenance.source = AgentPackageSource::BuiltIn;
        assert_eq!(
            spoofed.principal_authority(),
            AgentPrincipalAuthority::default()
        );

        let mut demoted = AgentDefinition::main();
        demoted.package.provenance.source = AgentPackageSource::Imported;
        assert_eq!(
            demoted.principal_authority(),
            AgentPrincipalAuthority::default()
        );
    }

    #[test]
    fn built_in_agent_definitions_validate() {
        let catalog = AgentCatalog::built_ins();
        let ids: Vec<_> = catalog.list().map(AgentDefinition::key).collect();

        assert!(ids.contains(&"main-agent"));
        assert!(ids.contains(&"background-agent"));
        assert!(ids.contains(&"coding-agent"));
        for definition in catalog.list() {
            definition
                .validate()
                .expect("built-in definition validates");
            assert_eq!(definition.package.version, AGENT_PACKAGE_VERSION);
            assert_eq!(
                definition.package.provenance.source,
                AgentPackageSource::BuiltIn
            );
            assert_eq!(
                definition.context.pipeline_id,
                DEFAULT_AGENT_CONTEXT_PIPELINE_ID
            );
            assert_eq!(
                definition.compaction.strategy_id,
                DEFAULT_AGENT_COMPACTION_STRATEGY_ID
            );
        }
    }

    #[test]
    fn built_in_ids_are_reserved_catalog_principals() {
        let mut catalog = AgentCatalog::built_ins();
        let forged = AgentDefinition::custom("main-agent", AgentRole::Main, "Steal recall.");

        let error = catalog.upsert(forged).expect_err("reserved id must fail");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::ReservedBuiltInId { ref id }
                if id == "main-agent"
        ));
        assert_eq!(catalog.get("main-agent"), Some(&AgentDefinition::main()));
    }

    #[test]
    fn persistent_store_rejects_reserved_built_in_ids() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-reserved-definition-store");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let store = AgentDefinitionSifsStore::open(Some(data_dir.root())).expect("open store");

        let save_error = store
            .save_definition(&AgentDefinition::main())
            .expect_err("built-ins must not be persisted");
        assert!(matches!(
            save_error,
            AgentDefinitionStoreError::Validation(
                AgentDefinitionValidationError::ReservedBuiltInId { .. }
            )
        ));
        let delete_error = store
            .delete_definition("main-agent")
            .expect_err("built-ins must not be deleted");
        assert!(matches!(
            delete_error,
            AgentDefinitionStoreError::Validation(
                AgentDefinitionValidationError::ReservedBuiltInId { .. }
            )
        ));
    }

    #[test]
    fn only_main_and_deliberation_roles_support_interactive_chat() {
        assert!(AgentDefinition::main().supports_interactive_chat());
        assert!(AgentDefinition::coding().supports_interactive_chat());
        assert!(!AgentDefinition::background().supports_interactive_chat());
        assert!(
            !AgentDefinition::custom("evolution-agent", AgentRole::Evolution, "Evolve.")
                .supports_interactive_chat()
        );
    }

    #[test]
    fn built_in_agents_leave_temperature_to_the_model_default() {
        for definition in [
            AgentDefinition::main(),
            AgentDefinition::background(),
            AgentDefinition::coding(),
        ] {
            assert_eq!(
                definition.temperature,
                None,
                "{} must not set a legacy temperature override",
                definition.key()
            );
            assert_eq!(
                definition.model_policy.temperature,
                None,
                "{} must not set a model-policy temperature override",
                definition.key()
            );
        }
    }

    #[test]
    fn agent_definition_validation_rejects_unknown_strategies() {
        let mut definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.");
        definition.context.pipeline_id = "context.pipeline.missing".to_string();

        let error = definition.validate().expect_err("invalid context");

        assert!(matches!(
            error,
            AgentDefinitionValidationError::UnknownContextPipeline { .. }
        ));
    }

    #[test]
    fn agent_definition_validation_rejects_invalid_package_metadata() {
        let definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.")
            .with_package_version("1.0 bad")
            .with_skill("bad skill");

        let error = definition.validate().expect_err("invalid package version");

        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidPackageVersion { .. }
        ));

        let mut definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.")
            .with_package_provenance(AgentPackageProvenance {
                source: AgentPackageSource::Module,
                ..AgentPackageProvenance::default()
            });

        let error = definition.validate().expect_err("missing module id");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::MissingProvenanceModuleId { .. }
        ));

        definition.package.provenance = AgentPackageProvenance::local();
        definition.skills = vec!["bad skill".to_string()];
        let error = definition.validate().expect_err("invalid skill");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidSkill { .. }
        ));
    }

    #[test]
    fn agent_definition_validation_rejects_invalid_package_references() {
        let definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.")
            .with_tool(AgentToolSpec::new(
                AgentToolKind::RunShellCommand,
                "bad tool",
                "Run a command.",
                true,
            ));

        let error = definition.validate().expect_err("invalid tool name");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidToolName { .. }
        ));

        let definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.")
            .with_tool(AgentToolSpec::new(
                AgentToolKind::RunShellCommand,
                "run_command",
                "Run a command.",
                true,
            ))
            .with_tool(AgentToolSpec::new(
                AgentToolKind::ReadFile,
                "run_command",
                "Read a file.",
                false,
            ));

        let error = definition.validate().expect_err("duplicate tool name");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::DuplicateToolName { .. }
        ));

        let mut definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.");
        definition.tool_surface.visible_toolsets = vec!["bad toolset".to_string()];

        let error = definition.validate().expect_err("invalid toolset");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidToolSurfaceEntry { .. }
        ));

        let mut definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.");
        definition.context.providers = vec!["bad provider".to_string()];

        let error = definition.validate().expect_err("invalid context provider");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidContextProvider { .. }
        ));
    }

    #[test]
    fn agent_definition_validation_rejects_invalid_model_and_runtime_limits() {
        let mut definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.");
        definition.model_policy.model = Some("bad model".to_string());

        let error = definition.validate().expect_err("invalid model preference");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidModelPreference { .. }
        ));

        let mut definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.");
        definition.temperature = Some(-0.1);

        let error = definition.validate().expect_err("invalid temperature");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidTemperature { .. }
        ));

        let mut definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.");
        definition.temperature = Some(0.1);
        definition.model_policy.temperature = Some(0.2);

        let error = definition.validate().expect_err("conflicting temperature");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::ConflictingModelPolicyField { .. }
        ));

        let mut definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.");
        definition.runtime_limits.max_steps = Some(0);

        let error = definition.validate().expect_err("invalid runtime limit");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidRuntimeLimit { .. }
        ));

        let definition =
            AgentDefinition::custom("large-output-agent", AgentRole::Main, "Be helpful.")
                .with_max_output_tokens(Some(30_000));
        definition
            .validate()
            .expect("output reserve below the conservative unknown-model window validates");

        let definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.")
            .with_max_output_tokens(Some(32 * 1024));
        let error = definition
            .validate()
            .expect_err("output reserve must leave input capacity");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::OutputReserveExhaustsContextWindow {
                max_output_tokens: 32_768,
                context_window_tokens: 32_768,
                ..
            }
        ));

        let definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.")
            .with_sandbox_runtime_node_id("bad node");

        let error = definition
            .validate()
            .expect_err("invalid sandbox runtime node");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidSandboxRuntimeNodeId { .. }
        ));

        let definition = AgentDefinition::custom("bad-agent", AgentRole::Main, "Be helpful.")
            .with_home_server_node_id("bad node");
        let error = definition.validate().expect_err("invalid home server node");
        assert!(matches!(
            error,
            AgentDefinitionValidationError::InvalidHomeServerNodeId { .. }
        ));
    }

    #[test]
    fn agent_definition_rejects_declared_policy_fields_without_runtime_enforcement() {
        let mut context = AgentDefinition::custom("context-agent", AgentRole::Main, "Be helpful.");
        context.context.providers = vec!["context.provider.valid".to_string()];
        assert!(matches!(
            context.validate(),
            Err(AgentDefinitionValidationError::UnsupportedContextProviders { .. })
        ));

        let mut vendor = AgentDefinition::custom("vendor-agent", AgentRole::Main, "Be helpful.");
        vendor.model_policy.vendor_options = serde_json::json!({"reasoning": "high"});
        assert!(matches!(
            vendor.validate(),
            Err(AgentDefinitionValidationError::UnsupportedModelVendorOptions { .. })
        ));
    }

    #[test]
    fn agent_definition_serializes_tool_runtime_default() {
        let definition = AgentDefinition::custom("worker-agent", AgentRole::Main, "Be helpful.")
            .with_sandbox_runtime_node_id("worker-node")
            .with_home_server_node_id("home-node");

        definition.validate().expect("definition validates");
        let value = serde_json::to_value(&definition).expect("serialize definition");
        assert_eq!(
            value
                .pointer("/tool_runtime/sandbox_node_id")
                .and_then(serde_json::Value::as_str),
            Some("worker-node")
        );
        assert_eq!(
            value
                .pointer("/placement/home_server_node_id")
                .and_then(serde_json::Value::as_str),
            Some("home-node")
        );

        let round_trip: AgentDefinition =
            serde_json::from_value(value).expect("deserialize definition");
        assert_eq!(
            round_trip.tool_runtime.sandbox_node_id.as_deref(),
            Some("worker-node")
        );
        assert_eq!(
            round_trip.placement.home_server_node_id.as_deref(),
            Some("home-node")
        );
    }

    #[test]
    fn agent_definition_round_trips_validated_mcp_server_references() {
        let mut server = AgentMcpServerDefinition::streamable_http(
            "project_docs",
            "https://mcp.example.test/v1",
        );
        server.required = true;
        server.allowed_tools = vec!["search".to_string(), "fetch".to_string()];
        server.bearer_token_env = Some("SINOMO_MCP_TEST_TOKEN".to_string());
        server.headers_from_env.insert(
            "x-tenant-id".to_string(),
            "SINOMO_MCP_TEST_TENANT".to_string(),
        );
        let definition = AgentDefinition::custom("mcp-agent", AgentRole::Main, "Use project docs.")
            .with_tool_runtime(AgentToolRuntimeDefinition::default().with_mcp_server(server));

        definition.validate().expect("MCP definition validates");
        let value = serde_json::to_value(&definition).expect("serialize definition");
        assert_eq!(
            value["tool_runtime"]["mcp_servers"][0]["bearer_token_env"],
            "SINOMO_MCP_TEST_TOKEN"
        );
        assert!(
            value["tool_runtime"]["mcp_servers"][0]
                .get("bearer_token")
                .is_none()
        );
        let round_trip: AgentDefinition =
            serde_json::from_value(value).expect("deserialize definition");
        assert_eq!(round_trip, definition);
    }

    #[test]
    fn agent_definition_rejects_invalid_and_duplicate_mcp_servers() {
        let invalid_url = AgentDefinition::custom("mcp-agent", AgentRole::Main, "Use tools.")
            .with_tool_runtime(AgentToolRuntimeDefinition::default().with_mcp_server(
                AgentMcpServerDefinition::streamable_http("docs", "file:///tmp/mcp.sock"),
            ));
        assert!(matches!(
            invalid_url.validate(),
            Err(AgentDefinitionValidationError::InvalidMcpServer { .. })
        ));

        let mut invalid_env =
            AgentMcpServerDefinition::streamable_http("docs", "https://mcp.example.test");
        invalid_env.bearer_token_env = Some("AWS_SECRET_ACCESS_KEY".to_string());
        let invalid_env = AgentDefinition::custom("mcp-agent", AgentRole::Main, "Use tools.")
            .with_tool_runtime(AgentToolRuntimeDefinition::default().with_mcp_server(invalid_env));
        assert!(matches!(
            invalid_env.validate(),
            Err(AgentDefinitionValidationError::InvalidMcpServer { .. })
        ));

        let mut invalid_header =
            AgentMcpServerDefinition::streamable_http("docs", "https://mcp.example.test");
        invalid_header.headers_from_env.insert(
            "mcp-session-id".to_string(),
            "SINOMO_MCP_SESSION".to_string(),
        );
        let invalid_header = AgentDefinition::custom("mcp-agent", AgentRole::Main, "Use tools.")
            .with_tool_runtime(
                AgentToolRuntimeDefinition::default().with_mcp_server(invalid_header),
            );
        assert!(matches!(
            invalid_header.validate(),
            Err(AgentDefinitionValidationError::InvalidMcpServer { .. })
        ));

        let server = AgentMcpServerDefinition::streamable_http("docs", "https://mcp.example.test");
        let duplicate = AgentDefinition::custom("mcp-agent", AgentRole::Main, "Use tools.")
            .with_tool_runtime(
                AgentToolRuntimeDefinition::default()
                    .with_mcp_server(server.clone())
                    .with_mcp_server(server),
            );
        assert!(matches!(
            duplicate.validate(),
            Err(AgentDefinitionValidationError::DuplicateMcpServer { .. })
        ));

        let colliding = AgentDefinition::custom("mcp-agent", AgentRole::Main, "Use tools.")
            .with_tool_runtime(
                AgentToolRuntimeDefinition::default()
                    .with_mcp_server(AgentMcpServerDefinition::streamable_http(
                        "project-docs",
                        "https://one.example.test",
                    ))
                    .with_mcp_server(AgentMcpServerDefinition::streamable_http(
                        "project_docs",
                        "https://two.example.test",
                    )),
            );
        assert!(matches!(
            colliding.validate(),
            Err(AgentDefinitionValidationError::DuplicateMcpServer { .. })
        ));
    }

    #[test]
    fn sifs_agent_definition_store_round_trips_custom_definition() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-definition-store");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let store = AgentDefinitionSifsStore::open(Some(data_dir.root())).expect("open store");
        let definition =
            AgentDefinition::custom("docs-agent", AgentRole::Deliberation, "Write docs.")
                .with_package_version("2.3.4")
                .with_package_provenance(AgentPackageProvenance {
                    source: AgentPackageSource::Imported,
                    path: Some("agent-packages/docs-agent.json".to_string()),
                    revision: Some("rev_docs_agent".to_string()),
                    ..AgentPackageProvenance::default()
                })
                .with_skill("docs.write")
                .with_temperature(Some(0.3))
                .with_max_output_tokens(Some(456));

        let path = store.save_definition(&definition).expect("save definition");
        let loaded = store.load_definitions().expect("load definitions");

        assert_eq!(
            path,
            format!("{AGENT_DEFINITION_SIFS_ROOT}/docs-agent.json")
        );
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].key(), "docs-agent");
        assert_eq!(loaded[0].package.version, "2.3.4");
        assert_eq!(
            loaded[0].package.provenance.source,
            AgentPackageSource::Imported
        );
        assert_eq!(
            loaded[0].package.provenance.path.as_deref(),
            Some("agent-packages/docs-agent.json")
        );
        assert_eq!(loaded[0].skills, vec!["docs.write"]);
        assert_eq!(loaded[0].temperature, Some(0.3));
        assert_eq!(loaded[0].model_policy.max_output_tokens, Some(456));
        assert!(store.delete_definition("docs-agent").expect("delete"));
        assert!(store.load_definitions().expect("reload").is_empty());
    }

    #[test]
    fn agent_catalog_loads_definitions_from_mounted_sifs() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-definition-store-catalog");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let writer = AgentDefinitionSifsStore::open(Some(data_dir.root())).expect("open writer");
        let definition =
            AgentDefinition::custom("catalog-agent", AgentRole::Deliberation, "Stay consistent.");
        writer.save_definition(&definition).expect("save");

        let reader =
            AgentDefinitionSifsStore::open(Some(data_dir.root())).expect("open reader store");
        let loaded = reader.load_definitions().expect("load definitions");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].key(), "catalog-agent");

        let catalog = AgentCatalog::load_from_data_dir(&data_dir).expect("catalog load");
        assert!(
            catalog.get("catalog-agent").is_some(),
            "catalog load should surface the mounted-SIFS definition"
        );
    }

    #[test]
    fn agent_catalog_loads_from_an_injected_read_transport() {
        let vfs = sinomo_runtime::vfs::VfsHandle::memory();
        let writer =
            AgentDefinitionSifsStore::from_vfs(vfs.clone()).expect("in-memory definition store");
        let definition = AgentDefinition::custom(
            "resident-read-agent",
            AgentRole::Deliberation,
            "Use the caller-selected SIFS transport.",
        );
        writer
            .save_definition(&definition)
            .expect("save definition");

        let catalog = AgentCatalog::load_from_vfs(vfs).expect("catalog through injected VFS");

        assert!(catalog.get("resident-read-agent").is_some());
    }

    #[test]
    fn catalog_reads_distinguish_confirmed_absence_from_content_unavailability() {
        let absent = sinomo_runtime::vfs::VfsHandle::memory();
        let skill_catalog = SkillCatalogSifsStore::from_vfs(absent.clone())
            .load_catalog()
            .expect("confirmed missing skill catalog is empty");
        assert_eq!(skill_catalog, SkillCatalog::default());
        // Exercise namespace absence independently of the operator's active
        // product modules, which the host merges after loading stored agents.
        let agent_catalog = AgentCatalog::load_from_store(
            AgentDefinitionSifsStore::from_vfs(absent).expect("memory agent store"),
        )
        .expect("confirmed missing agent directory");
        assert_eq!(
            agent_catalog.list().count(),
            AgentDefinition::built_ins().len(),
            "confirmed namespace absence retains the built-in catalog"
        );

        let unavailable = sinomo_runtime::vfs::VfsHandle::new(
            Arc::new(ContentUnavailableCatalogVfsBackend {
                list_unavailable: false,
            }),
            "/",
        )
        .expect("unavailable catalog VFS");
        let skill_error = SkillCatalogSifsStore::from_vfs(unavailable.clone())
            .load_catalog()
            .expect_err("skill content unavailability must not become an empty catalog");
        assert!(matches!(skill_error, SkillCatalogError::Store { .. }));

        let agent_error = AgentCatalog::load_from_vfs(unavailable)
            .expect_err("agent content unavailability must not become built-ins only");
        assert!(matches!(
            agent_error,
            AgentCatalogError::Store(AgentDefinitionStoreError::Io { .. })
        ));

        let listing_unavailable = sinomo_runtime::vfs::VfsHandle::new(
            Arc::new(ContentUnavailableCatalogVfsBackend {
                list_unavailable: true,
            }),
            "/",
        )
        .expect("unavailable catalog-list VFS");
        let agent_error = AgentCatalog::load_from_vfs(listing_unavailable)
            .expect_err("opaque listing failures must not become built-ins only");
        assert!(matches!(
            agent_error,
            AgentCatalogError::Store(AgentDefinitionStoreError::Io { ref detail, .. })
                if detail.contains("route not found")
        ));
    }

    #[test]
    fn agent_catalog_imports_module_agent_exports() {
        let export = sinomo_runtime::module::ModuleAgentDefinitionExport::new(
            "module.example.agent",
            "module-example-agent",
            "deliberation",
            "Use module-specific tools.",
        )
        .with_tool(sinomo_runtime::module::ModuleAgentToolExport::new(
            "read_file",
            "read_file",
            "Read a workspace file.",
            false,
        ))
        .with_skill("skill.module-example")
        .with_visible_toolset("coding");
        let module_id = sinomo_runtime::module::ModuleId::from_legacy_named("module.example")
            .expect("module id");
        let module_id_text = module_id.as_str().to_string();
        let registration = sinomo_runtime::module::ModuleRegistration::native_rust(
            module_id,
            "0.1.0",
            "module-example",
            "module-example",
            "module-example-v1",
            sinomo_runtime::core::TrustRing::R2TrustedService,
            "module-example",
        )
        .with_surfaces(
            sinomo_runtime::module::ModuleSurfaces::default().with_agent_definition(export),
        );
        let registry = sinomo_runtime::module::ModuleRegistry::builder()
            .register(registration)
            .expect("register")
            .build();
        let mut catalog = AgentCatalog::built_ins();

        catalog
            .merge_module_registry(&registry)
            .expect("merge module exports");

        let definition = catalog
            .get("module.example.agent")
            .expect("module agent definition");
        assert_eq!(definition.name, "module-example-agent");
        assert_eq!(definition.role, AgentRole::Deliberation);
        assert_eq!(definition.tools[0].kind, AgentToolKind::ReadFile);
        assert_eq!(definition.skills, ["skill.module-example"]);
        assert_eq!(definition.tool_surface.visible_toolsets, vec!["coding"]);
        assert_eq!(definition.package.version, "0.1.0");
        assert_eq!(
            definition.package.provenance.source,
            AgentPackageSource::Module
        );
        assert_eq!(
            definition.package.provenance.module_id.as_deref(),
            Some(module_id_text.as_str())
        );
        assert_eq!(
            definition.package.provenance.module_version.as_deref(),
            Some("0.1.0")
        );
    }

    #[test]
    fn module_agent_exports_cannot_shadow_existing_catalog_principals() {
        let export = sinomo_runtime::module::ModuleAgentDefinitionExport::new(
            "main-agent",
            "module-main-agent",
            "main",
            "Replace the main agent.",
        );
        let module_id = sinomo_runtime::module::ModuleId::from_legacy_named("module.shadow")
            .expect("module id");
        let registration = sinomo_runtime::module::ModuleRegistration::native_rust(
            module_id.clone(),
            "0.1.0",
            "module-shadow",
            "module-shadow",
            "module-shadow-v1",
            sinomo_runtime::core::TrustRing::R2TrustedService,
            "module-shadow",
        )
        .with_surfaces(
            sinomo_runtime::module::ModuleSurfaces::default().with_agent_definition(export),
        );
        let registry = sinomo_runtime::module::ModuleRegistry::builder()
            .register(registration)
            .expect("register")
            .build();
        let mut catalog = AgentCatalog::built_ins();

        let error = catalog
            .merge_module_registry(&registry)
            .expect_err("module agent must not shadow a built-in principal");

        assert!(matches!(
            error,
            AgentCatalogError::AgentIdCollision {
                ref id,
                existing_source: AgentPackageSource::BuiltIn,
                module_id: ref observed_module_id,
            } if id == "main-agent" && observed_module_id == module_id.as_str()
        ));
        assert_eq!(catalog.get("main-agent"), Some(&AgentDefinition::main()));
    }

    #[test]
    fn custom_agent_definition_controls_agent_request() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_custom_loop").expect("session"),
            NodeId::from_legacy_named("node_custom_loop").expect("node"),
            InteractionSurface::new("surface.custom", InteractionSurfaceKind::CommandLine),
        );
        let latest_event = user_interaction_input_event(&session, "status");
        let loop_input = AgentLoopInput {
            session,
            latest_event,
            transient_system_context: None,
            async_update: None,
            context_attachments: Vec::new(),
            tool_specs: Vec::new(),
        };
        let definition = AgentDefinition::custom(
            "operator-agent",
            AgentRole::Background,
            "Use the operator playbook.",
        )
        .with_output_format(AgentOutputFormat::Text)
        .with_temperature(Some(0.1))
        .with_max_output_tokens(Some(123))
        .with_tool(AgentToolSpec::new(
            AgentToolKind::ClusterStatus,
            "custom_status",
            "Read custom operator status.",
            false,
        ));
        let model = CapturingInteractionModel::new("plain operator answer");
        let requests = model.requests.clone();
        let agent = Agent::new(model, "fake", "fake").with_agent_definition(definition);

        let run = agent.run(&loop_input).expect("run");
        let request = requests.lock().expect("requests")[0].clone();

        assert_eq!(
            run.final_decision.response_mode,
            AgentLoopResponseMode::Answer
        );
        assert_eq!(run.final_decision.text, "plain operator answer");
        assert_eq!(request.response_format, LanguageModelResponseFormat::Text);
        assert_eq!(request.temperature, Some(0.1));
        assert_eq!(request.max_output_tokens, Some(123));
        assert!(
            request.messages[0]
                .content
                .contains("Use the operator playbook.")
        );
        assert!(
            request.messages[0]
                .content
                .contains("Respond directly in plain text")
        );
        assert_eq!(
            request
                .metadata
                .get("agent_name")
                .and_then(serde_json::Value::as_str),
            Some("operator-agent")
        );
        assert_eq!(
            request
                .metadata
                .get("agent_role")
                .and_then(serde_json::Value::as_str),
            Some("background")
        );
    }

    #[test]
    fn non_json_model_response_becomes_plain_answer() {
        let decision = parse_agent_loop_decision("A short plain response.").expect("decision");

        assert_eq!(decision.response_mode, AgentLoopResponseMode::Answer);
        assert_eq!(decision.text, "A short plain response.");
        assert!(decision.tool_calls.is_empty());
        assert_eq!(decision.next_state, AgentInteractionState::Listening);
    }

    #[test]
    fn partial_json_model_response_gets_agent_defaults() {
        let decision =
            parse_agent_loop_decision(r#"{"response_mode":"answer","text":"Hello from Sinomo."}"#)
                .expect("decision");

        assert_eq!(decision.response_mode, AgentLoopResponseMode::Answer);
        assert_eq!(decision.text, "Hello from Sinomo.");
        assert!(decision.tool_calls.is_empty());
        assert_eq!(decision.background_handoff, None);
        assert_eq!(decision.next_state, AgentInteractionState::Listening);
    }

    #[test]
    fn quiet_async_update_does_not_call_model() {
        let session = InteractionSession::new(
            SessionId::from_legacy_named("sess_loop").expect("session"),
            NodeId::from_legacy_named("node_loop").expect("node"),
            InteractionSurface::new("surface.loop", InteractionSurfaceKind::CommandLine),
        );
        let latest_event = user_interaction_input_event(&session, "hello");
        let loop_input = AgentLoopInput {
            session,
            latest_event,
            transient_system_context: None,
            async_update: Some(AsyncTaskUpdate {
                kind: "async_task_update".to_string(),
                task_id: TaskId::from_legacy_named("task_background").expect("task"),
                status: AsyncTaskStatus::Progress,
                summary: "still running".to_string(),
                needs_user_attention: false,
                result_ref: None,
                next_action_hint: AsyncTaskNextAction::ContinueSilently,
            }),
            context_attachments: Vec::new(),
            tool_specs: default_main_agent_tools(),
        };
        let agent = Agent::new(
            FakeInteractionModel {
                response: "this should not be used",
            },
            "fake",
            "fake",
        );

        let run = agent.run(&loop_input).expect("run");
        let decision = run.final_decision;

        assert_eq!(decision.response_mode, AgentLoopResponseMode::Silent);
        assert!(decision.tool_calls.is_empty());
    }
}
