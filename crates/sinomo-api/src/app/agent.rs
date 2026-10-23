//! Stable app-facing agent definition and management contracts.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const AGENT_MANAGEMENT_RPC_CATEGORY: &str = "agent.app";
pub const AGENT_MANAGEMENT_ROUTE_AUTH_SCOPE: &str = "local-command";
pub const AGENT_SNAPSHOT_ROUTE: &str = "/agent/app/snapshot";
pub const AGENT_SAVE_ROUTE: &str = "/agent/app/save";
pub const AGENT_DELETE_ROUTE: &str = "/agent/app/delete";
pub const AGENT_IMPORT_ROUTE: &str = "/agent/app/import";
pub const AGENT_EXPORT_ROUTE: &str = "/agent/app/export";
pub const AGENT_VALIDATE_ROUTE: &str = "/agent/app/validate";
pub const AGENT_TEST_ROUTE: &str = "/agent/app/test";
pub const AGENT_MANAGEMENT_RPC_ROUTES: &[&str] = &[
    AGENT_SNAPSHOT_ROUTE,
    AGENT_SAVE_ROUTE,
    AGENT_DELETE_ROUTE,
    AGENT_IMPORT_ROUTE,
    AGENT_EXPORT_ROUTE,
    AGENT_VALIDATE_ROUTE,
    AGENT_TEST_ROUTE,
];
pub const AGENT_MANAGEMENT_READ_CAPABILITY: &str = "agent.app.read";
pub const AGENT_MANAGEMENT_WRITE_CAPABILITY: &str = "agent.app.write";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentDefinition {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub package: AgentPackageDefinition,
    #[serde(default)]
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub role: AgentRole,
    pub system_prompt: String,
    #[serde(default)]
    pub tools: Vec<AgentToolSpec>,
    #[serde(default)]
    pub tool_surface: AgentToolSurfaceDefinition,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub context: AgentContextDefinition,
    #[serde(default)]
    pub compaction: AgentCompactionDefinition,
    #[serde(default)]
    pub model_policy: AgentModelPolicy,
    #[serde(default)]
    pub runtime_limits: AgentRuntimeLimitsDefinition,
    #[serde(default)]
    pub placement: AgentPlacementDefinition,
    #[serde(default)]
    pub tool_runtime: AgentToolRuntimeDefinition,
    #[serde(default)]
    pub output_format: AgentOutputFormat,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
}

impl AgentDefinition {
    pub fn key(&self) -> &str {
        self.id.as_str()
    }

    pub fn custom(id: impl Into<String>, role: AgentRole, prompt: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            schema_version: 1,
            package: AgentPackageDefinition::local(),
            name: id.clone(),
            id,
            description: String::new(),
            role,
            system_prompt: prompt.into(),
            tools: default_agent_recall_tools(),
            tool_surface: AgentToolSurfaceDefinition::default(),
            skills: Vec::new(),
            context: AgentContextDefinition::default(),
            compaction: AgentCompactionDefinition::default(),
            model_policy: AgentModelPolicy::default(),
            runtime_limits: AgentRuntimeLimitsDefinition::default(),
            placement: AgentPlacementDefinition::default(),
            tool_runtime: AgentToolRuntimeDefinition::default(),
            output_format: AgentOutputFormat::default(),
            temperature: None,
            max_output_tokens: None,
        }
    }

    pub fn main() -> Self {
        let mut definition = Self::custom("main-agent", AgentRole::Main, "You are Sinomo.");
        definition.package = AgentPackageDefinition::built_in();
        definition.description = "user-facing interactive agent".to_string();
        definition
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.id.trim().is_empty() {
            return Err("agent id is required".to_string());
        }
        if self.name.trim().is_empty() {
            return Err("agent name is required".to_string());
        }
        if self.system_prompt.trim().is_empty() {
            return Err("agent system prompt is required".to_string());
        }
        for server in &self.tool_runtime.mcp_servers {
            server.validate()?;
        }
        Ok(())
    }
}

fn default_schema_version() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRole {
    Main,
    Background,
    Deliberation,
    Evolution,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentPackageDefinition {
    #[serde(default = "default_package_version")]
    pub version: String,
    #[serde(default)]
    pub provenance: AgentPackageProvenance,
}

impl AgentPackageDefinition {
    pub fn local() -> Self {
        Self {
            version: default_package_version(),
            provenance: AgentPackageProvenance {
                source: AgentPackageSource::Local,
                ..AgentPackageProvenance::default()
            },
        }
    }

    pub fn built_in() -> Self {
        Self {
            version: default_package_version(),
            provenance: AgentPackageProvenance {
                source: AgentPackageSource::BuiltIn,
                ..AgentPackageProvenance::default()
            },
        }
    }
}

fn default_package_version() -> String {
    "1.0.0".to_string()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentPackageProvenance {
    #[serde(default)]
    pub source: AgentPackageSource,
    #[serde(default)]
    pub module_id: Option<String>,
    #[serde(default)]
    pub module_version: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub revision: Option<String>,
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolSpec {
    pub kind: serde_json::Value,
    pub name: String,
    pub description: String,
    pub async_default: bool,
}

impl AgentToolSpec {
    fn new(kind: &str, name: &str, description: &str, async_default: bool) -> Self {
        Self {
            kind: serde_json::Value::String(kind.to_string()),
            name: name.to_string(),
            description: description.to_string(),
            async_default,
        }
    }
}

fn default_agent_recall_tools() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            "session_history_search",
            "session_history_search",
            "Search chats, channels, and background activities this agent engaged in without exposing raw model-session records.",
            false,
        ),
        AgentToolSpec::new(
            "memory_search",
            "memory_search",
            "Search this agent's Markdown memory notes in its SIFS home.",
            false,
        ),
        AgentToolSpec::new(
            "memory_read",
            "memory_read",
            "Read a bounded versioned Markdown memory note from this agent's SIFS home.",
            false,
        ),
        AgentToolSpec::new(
            "memory_write",
            "memory_write",
            "Create or compare-and-replace a Markdown memory note in this agent's SIFS home.",
            true,
        ),
    ]
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolSurfaceDefinition {
    pub visible_toolsets: Vec<String>,
    pub allow_tools: Vec<String>,
    pub deny_tools: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentContextDefinition {
    pub pipeline_id: String,
    #[serde(default)]
    pub providers: Vec<String>,
}

impl Default for AgentContextDefinition {
    fn default() -> Self {
        Self {
            pipeline_id: "context.pipeline.default".to_string(),
            providers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentCompactionDefinition {
    pub strategy_id: String,
    #[serde(default)]
    pub retained_tail_items: Option<usize>,
}

impl Default for AgentCompactionDefinition {
    fn default() -> Self {
        Self {
            strategy_id: "compaction.strategy.default".to_string(),
            retained_tail_items: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentModelPolicy {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub vendor_options: serde_json::Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentRuntimeLimitsDefinition {
    #[serde(default)]
    pub max_steps: Option<usize>,
    #[serde(default)]
    pub max_tool_calls_per_step: Option<usize>,
    #[serde(default)]
    pub max_model_repair_attempts: Option<usize>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentPlacementDefinition {
    #[serde(default)]
    pub home_server_node_id: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolRuntimeDefinition {
    #[serde(default)]
    pub sandbox_node_id: Option<String>,
    #[serde(default)]
    pub mcp_servers: Vec<AgentMcpServerDefinition>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentMcpProtocolEra {
    #[default]
    Auto,
    Modern,
    Legacy,
}

impl AgentMcpProtocolEra {
    pub fn is_default(&self) -> bool {
        *self == Self::Auto
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
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub protocol_era: AgentMcpProtocolEra,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    #[serde(default)]
    pub headers_from_env: BTreeMap<String, String>,
    pub startup_timeout_ms: u64,
    pub tool_timeout_ms: u64,
}

fn default_true() -> bool {
    true
}

impl AgentMcpServerDefinition {
    pub fn streamable_http(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            enabled: true,
            protocol_era: AgentMcpProtocolEra::Auto,
            required: false,
            allowed_tools: Vec::new(),
            bearer_token_env: None,
            headers_from_env: BTreeMap::new(),
            startup_timeout_ms: 10_000,
            tool_timeout_ms: 60_000,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty()
            || !self
                .name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        {
            return Err("MCP server name is invalid".to_string());
        }
        if !(self.url.starts_with("https://")
            || self.url.starts_with("http://127.0.0.1")
            || self.url.starts_with("http://localhost"))
        {
            return Err("MCP URL must use HTTPS or a loopback HTTP endpoint".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolInvocationMode {
    TurnBased,
    Realtime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolDescriptor {
    pub spec: AgentToolSpec,
    #[serde(default)]
    pub invocation_modes: Vec<AgentToolInvocationMode>,
}

impl AgentToolDescriptor {
    pub fn supports_invocation_mode(&self, mode: AgentToolInvocationMode) -> bool {
        self.invocation_modes.contains(&mode)
    }
}

pub fn fallback_agent_tool_descriptors() -> Vec<AgentToolDescriptor> {
    [
        ("read_file", "Read a workspace file.", true),
        ("request_user_input", "Request a user choice.", false),
        ("web_search", "Search the web.", true),
        ("image_generation", "Generate or edit an image.", true),
    ]
    .into_iter()
    .map(|(name, description, realtime)| AgentToolDescriptor {
        spec: AgentToolSpec {
            kind: serde_json::Value::String(name.to_string()),
            name: name.to_string(),
            description: description.to_string(),
            async_default: false,
        },
        invocation_modes: if realtime {
            vec![
                AgentToolInvocationMode::TurnBased,
                AgentToolInvocationMode::Realtime,
            ]
        } else {
            vec![AgentToolInvocationMode::TurnBased]
        },
    })
    .collect()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentOutputFormat {
    #[default]
    JsonDecision,
    Text,
    NativeTools,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentConfigSnapshot {
    pub agents: Vec<AgentConfigSummary>,
    pub selected_agent: Option<AgentDefinition>,
    pub available_tools: Vec<AgentToolDescriptor>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentConfigSummary {
    pub id: String,
    pub name: String,
    pub description: String,
    pub role: String,
    pub source: String,
    pub package_version: String,
    pub provenance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_version: Option<String>,
    pub readiness: AgentReadiness,
    pub editable: bool,
    pub tool_count: usize,
    pub mcp_server_count: usize,
    pub skill_count: usize,
    pub default_model: String,
    pub home_server_node_id: Option<String>,
    pub sandbox_node_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentReadiness {
    #[default]
    Ready,
    NeedsAttention,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentValidationCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentValidationReport {
    pub agent_id: String,
    pub checks: Vec<AgentValidationCheck>,
}

impl AgentValidationReport {
    pub fn is_ready(&self) -> bool {
        self.checks.iter().all(|check| check.passed)
    }

    pub fn summary(&self) -> String {
        let passed = self.checks.iter().filter(|check| check.passed).count();
        format!("{passed} of {} checks passed", self.checks.len())
    }
}

pub fn agent_definition_is_editable(definition: &AgentDefinition) -> bool {
    matches!(
        definition.package.provenance.source,
        AgentPackageSource::Local | AgentPackageSource::Imported
    )
}

pub fn agent_role_label(role: &AgentRole) -> &'static str {
    match role {
        AgentRole::Main => "main",
        AgentRole::Background => "background",
        AgentRole::Deliberation => "deliberation",
        AgentRole::Evolution => "evolution",
    }
}

pub fn agent_source_label(source: &AgentPackageSource) -> &'static str {
    match source {
        AgentPackageSource::BuiltIn => "built-in",
        AgentPackageSource::Local => "custom",
        AgentPackageSource::Imported => "imported",
        AgentPackageSource::Module => "module",
        AgentPackageSource::Unknown => "unknown",
    }
}

pub fn agent_home_sifs_path_for_id(agent_id: &str) -> String {
    let component = agent_id
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("/sinomo/agents/{component}/home")
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSnapshotRequest {
    pub selected_agent_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSnapshotResponse {
    pub snapshot: AgentConfigSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSaveRequest {
    pub definition: AgentDefinition,
    pub previous_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentDeleteRequest {
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentDeleteResponse {
    pub deleted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentImportRequest {
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentImportResponse {
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentExportRequest {
    pub id: String,
    pub output: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentExportResponse {
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentInspectRequest {
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentInspectResponse {
    pub report: AgentValidationReport,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentMutationResponse {
    pub accepted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_agents_keep_the_contract_owned_recall_surface() {
        let definition = AgentDefinition::custom("helper", AgentRole::Main, "Help.");
        assert_eq!(
            definition
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "session_history_search",
                "memory_search",
                "memory_read",
                "memory_write",
            ]
        );
        assert_eq!(
            agent_home_sifs_path_for_id("helper/unsafe"),
            "/sinomo/agents/helper-unsafe/home"
        );
    }
}
