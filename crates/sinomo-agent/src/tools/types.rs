use crate::{
    AgentCancellationToken, AgentProcessSnapshot, AgentToolCall, AgentToolKind, AgentToolSpec,
    AgentVerificationState, InteractionSession, SkillCatalogEntry, TaskId,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_runtime::core::{HotSwapState, ModuleLoaderState};
use sinomo_runtime::events::EventId;
use sinomo_runtime::module::{
    ModuleDeclarativeResourceExport, ModuleDeclarativeResourceKind, ModuleRegistration,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolDescriptor {
    pub spec: AgentToolSpec,
    #[serde(default)]
    pub input_schema: serde_json::Value,
    #[serde(default)]
    pub output_schema: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub toolsets: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<AgentToolCapability>,
    /// Model interaction modes through which this implementation may be
    /// exposed. Tool behavior, authority checks, and execution stay shared;
    /// this only controls which model-facing adapters may advertise it.
    #[serde(
        default = "default_agent_tool_invocation_modes",
        skip_serializing_if = "agent_tool_invocation_modes_are_default"
    )]
    pub invocation_modes: Vec<AgentToolInvocationMode>,
    /// Host-local authority grants required at the final tool execution
    /// boundary. These opaque names are parsed strictly by the registry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_local_permissions: Vec<String>,
    pub supports_parallel: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_specs: Vec<AgentToolProviderSpec>,
}

impl AgentToolDescriptor {
    pub fn new(spec: AgentToolSpec) -> Self {
        let toolsets = default_toolsets_for_kind(&spec.kind)
            .into_iter()
            .map(str::to_string)
            .collect();
        Self {
            spec,
            input_schema: serde_json::Value::Null,
            output_schema: serde_json::Value::Null,
            toolsets,
            capabilities: Vec::new(),
            invocation_modes: default_agent_tool_invocation_modes(),
            required_local_permissions: Vec::new(),
            supports_parallel: false,
            provider_specs: Vec::new(),
        }
    }

    pub fn with_input_schema(mut self, input_schema: serde_json::Value) -> Self {
        self.input_schema = input_schema;
        self
    }

    pub fn with_output_schema(mut self, output_schema: serde_json::Value) -> Self {
        self.output_schema = output_schema;
        self
    }

    pub fn with_toolsets(mut self, toolsets: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.toolsets = toolsets.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = AgentToolCapability>,
    ) -> Self {
        self.capabilities = capabilities.into_iter().collect();
        self
    }

    pub fn with_invocation_modes(
        mut self,
        invocation_modes: impl IntoIterator<Item = AgentToolInvocationMode>,
    ) -> Self {
        self.invocation_modes = invocation_modes.into_iter().collect();
        self
    }

    pub fn supports_invocation_mode(&self, invocation_mode: AgentToolInvocationMode) -> bool {
        self.invocation_modes.contains(&invocation_mode)
    }

    pub fn with_required_local_permissions(
        mut self,
        permissions: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.required_local_permissions = permissions.into_iter().map(Into::into).collect();
        self
    }

    pub fn requiring_local_permission(mut self, permission: impl Into<String>) -> Self {
        self.required_local_permissions.push(permission.into());
        self
    }

    pub fn supporting_parallel(mut self, supports_parallel: bool) -> Self {
        self.supports_parallel = supports_parallel;
        self
    }

    pub fn with_provider_specs(
        mut self,
        provider_specs: impl IntoIterator<Item = AgentToolProviderSpec>,
    ) -> Self {
        self.provider_specs = provider_specs.into_iter().collect();
        self
    }

    pub(crate) fn from_spec(spec: AgentToolSpec) -> Self {
        Self::new(spec)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolInvocationMode {
    TurnBased,
    Realtime,
}

impl AgentToolInvocationMode {
    pub const fn all() -> [Self; 2] {
        [Self::TurnBased, Self::Realtime]
    }
}

fn default_agent_tool_invocation_modes() -> Vec<AgentToolInvocationMode> {
    vec![AgentToolInvocationMode::TurnBased]
}

fn agent_tool_invocation_modes_are_default(modes: &[AgentToolInvocationMode]) -> bool {
    modes == [AgentToolInvocationMode::TurnBased]
}

fn default_toolsets_for_kind(kind: &crate::AgentToolKind) -> Vec<&'static str> {
    use crate::AgentToolKind;
    match kind {
        AgentToolKind::ReadFile
        | AgentToolKind::ListFiles
        | AgentToolKind::SearchFiles
        | AgentToolKind::EditFile
        | AgentToolKind::WriteFile
        | AgentToolKind::ApplyPatch
        | AgentToolKind::RunShellCommand => vec!["core", "coding", "local"],
        AgentToolKind::ChangedFiles | AgentToolKind::GitDiff => vec!["version_control"],
        AgentToolKind::ModuleObjectRpc => vec!["module", "object_rpc"],
        AgentToolKind::McpCall => vec!["core", "mcp", "network"],
        AgentToolKind::WebSearch => vec!["core", "research", "network"],
        AgentToolKind::ImageGeneration => vec!["core", "media", "creative"],
        AgentToolKind::RequestUserInput => vec!["core", "interactive"],
        AgentToolKind::ToolSearch
        | AgentToolKind::ToolDescribe
        | AgentToolKind::ToolCall
        | AgentToolKind::SkillSearch
        | AgentToolKind::SkillDescribe
        | AgentToolKind::SkillActivate
        | AgentToolKind::SkillUpdate => vec!["core", "discovery", "skill"],
        AgentToolKind::ConfigSearch | AgentToolKind::ConfigGet | AgentToolKind::ConfigSet => {
            vec!["config", "local"]
        }
        AgentToolKind::SessionHistorySearch => vec!["core", "memory"],
        AgentToolKind::MemorySearch | AgentToolKind::MemoryRead | AgentToolKind::MemoryWrite => {
            vec!["core", "memory"]
        }
        AgentToolKind::ReadSifs | AgentToolKind::WriteSifs | AgentToolKind::ListSifs => {
            vec!["sifs", "cluster"]
        }
        AgentToolKind::ClusterStatus => vec!["cluster"],
        AgentToolKind::StartAsyncTask
        | AgentToolKind::InspectAsyncTask
        | AgentToolKind::CancelAsyncTask => vec!["async_task"],
        AgentToolKind::HandoffToBackground => vec!["delegation"],
        AgentToolKind::ObserveVision => vec!["perception"],
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolProviderSpec {
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom: Option<AgentToolCustomSpec>,
}

impl AgentToolProviderSpec {
    pub fn custom_grammar(
        provider: impl Into<String>,
        syntax: impl Into<String>,
        definition: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            custom: Some(AgentToolCustomSpec {
                format: AgentToolCustomFormat::Grammar {
                    syntax: syntax.into(),
                    definition: definition.into(),
                },
                raw_input_argument: None,
            }),
        }
    }

    pub fn with_raw_input_argument(mut self, argument: impl Into<String>) -> Self {
        if let Some(custom) = &mut self.custom {
            custom.raw_input_argument = Some(argument.into());
        }
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolCustomSpec {
    pub format: AgentToolCustomFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_input_argument: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentToolCustomFormat {
    Grammar { syntax: String, definition: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolPlanSource {
    Runtime,
    Module,
    Session,
    Default,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolPlanEntry {
    pub descriptor: AgentToolDescriptor,
    pub source: AgentToolPlanSource,
    pub executable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolPlanReadinessIssueKind {
    VisibleToolMissingExecutor,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolPlanReadinessIssue {
    pub kind: AgentToolPlanReadinessIssueKind,
    pub tool: String,
    pub source: AgentToolPlanSource,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub toolsets: Vec<String>,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentToolPlan {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<AgentToolPlanEntry>,
    #[serde(default)]
    pub registry_generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_toolsets: Vec<String>,
}

impl AgentToolPlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn descriptors(&self) -> Vec<AgentToolDescriptor> {
        self.entries
            .iter()
            .map(|entry| entry.descriptor.clone())
            .collect()
    }

    pub fn entry(&self, tool_name: &str) -> Option<&AgentToolPlanEntry> {
        self.entries
            .iter()
            .find(|entry| entry.descriptor.spec.name == tool_name)
    }

    pub fn descriptor(&self, tool_name: &str) -> Option<&AgentToolDescriptor> {
        self.entry(tool_name).map(|entry| &entry.descriptor)
    }

    pub fn descriptors_matching(
        &self,
        query: Option<&str>,
        include_bridge_tools: bool,
    ) -> Vec<&AgentToolPlanEntry> {
        let query = query.map(|query| query.trim().to_ascii_lowercase());
        self.entries
            .iter()
            .filter(|entry| {
                include_bridge_tools || !is_tool_disclosure_bridge_name(&entry.descriptor.spec.name)
            })
            .filter(|entry| {
                let Some(query) = query.as_deref().filter(|query| !query.is_empty()) else {
                    return true;
                };
                descriptor_matches_query(&entry.descriptor, query)
            })
            .collect()
    }

    pub fn is_executable(&self, tool_name: &str) -> bool {
        self.entry(tool_name)
            .map(|entry| entry.executable)
            .unwrap_or(false)
    }

    pub fn is_model_visible(&self, tool_name: &str) -> bool {
        self.entry(tool_name)
            .map(|entry| descriptor_matches_toolsets(&entry.descriptor, &self.visible_toolsets))
            .unwrap_or(false)
    }

    pub fn push_or_replace(
        &mut self,
        descriptor: AgentToolDescriptor,
        source: AgentToolPlanSource,
        executable: bool,
    ) {
        let name = descriptor.spec.name.clone();
        let entry = AgentToolPlanEntry {
            descriptor,
            source,
            executable,
        };
        if let Some(index) = self
            .entries
            .iter()
            .position(|existing| existing.descriptor.spec.name == name)
        {
            self.entries[index] = entry;
        } else {
            self.entries.push(entry);
        }
    }

    pub fn model_visible_descriptors(&self) -> Vec<AgentToolDescriptor> {
        self.entries
            .iter()
            .filter(|entry| descriptor_matches_toolsets(&entry.descriptor, &self.visible_toolsets))
            .map(|entry| entry.descriptor.clone())
            .collect()
    }

    pub fn readiness_issues(&self) -> Vec<AgentToolPlanReadinessIssue> {
        self.entries
            .iter()
            .filter(|entry| {
                !entry.executable
                    && descriptor_matches_toolsets(&entry.descriptor, &self.visible_toolsets)
            })
            .map(|entry| AgentToolPlanReadinessIssue {
                kind: AgentToolPlanReadinessIssueKind::VisibleToolMissingExecutor,
                tool: entry.descriptor.spec.name.clone(),
                source: entry.source.clone(),
                toolsets: entry.descriptor.toolsets.clone(),
                reason: format!(
                    "tool `{}` is visible to the model but has no runtime executor",
                    entry.descriptor.spec.name
                ),
            })
            .collect()
    }

    pub fn for_invocation_mode(mut self, invocation_mode: AgentToolInvocationMode) -> Self {
        self.entries
            .retain(|entry| entry.descriptor.supports_invocation_mode(invocation_mode));
        self
    }
}

pub fn tool_disclosure_descriptors() -> Vec<AgentToolDescriptor> {
    vec![
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ToolSearch,
            "tool_search",
            "Search planned tools by name, description, toolset, or capability. Results include hidden tools so large module/plugin/MCP surfaces can stay out of the prompt until needed.",
            false,
        ))
        .with_capabilities([AgentToolCapability::ObserveEnvironment])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Optional case-insensitive query over tool names, descriptions, toolsets, and capabilities."
                },
                "max_results": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum results to return. Defaults to 20."
                },
                "include_bridge_tools": {
                    "type": "boolean",
                    "description": "Whether to include tool_search/tool_describe/tool_call in results. Defaults to false."
                }
            }
        })),
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ToolDescribe,
            "tool_describe",
            "Return the full planned descriptor and availability metadata for one tool.",
            false,
        ))
        .with_capabilities([AgentToolCapability::ObserveEnvironment])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["tool"],
            "properties": {
                "tool": {
                    "type": "string",
                    "description": "Tool name to describe."
                }
            }
        })),
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ToolCall,
            "tool_call",
            "Execute a planned tool by name with input discovered through tool_search/tool_describe. The target call is checked by the same tool-surface availability and registry path.",
            true,
        ))
        .with_capabilities([AgentToolCapability::ObserveEnvironment])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["tool", "input"],
            "properties": {
                "tool": {
                    "type": "string",
                    "description": "Planned target tool name to execute."
                },
                "input": {
                    "type": "object",
                    "description": "Target tool input object."
                },
                "summary": {
                    "type": "string",
                    "description": "Short reason for executing the target tool."
                },
                "async": {
                    "type": "boolean",
                    "description": "Whether the target call should be treated as async by higher-level runtimes."
                }
            }
        })),
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::SkillSearch,
            "skill_search",
            "Search the available Agent Skills catalog by name or description without loading instructions.",
            false,
        ))
        .with_capabilities([AgentToolCapability::ObserveEnvironment])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Optional case-insensitive query over skill ids, descriptions, source modules, paths, target agents, model families, tags, and capabilities."
                },
                "max_results": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum results to return. Defaults to 20."
                }
            }
        })),
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::SkillDescribe,
            "skill_describe",
            "Inspect Agent Skills metadata and provenance without activating its instructions.",
            false,
        ))
        .with_capabilities([AgentToolCapability::ObserveEnvironment])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["skill"],
            "properties": {
                "skill": {
                    "type": "string",
                    "description": "Skill id to describe."
                }
            }
        })),
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::SkillActivate,
            "skill_activate",
            "Load the selected Agent Skills SKILL.md instructions and list bundled resources. Call this when the task matches an available skill description.",
            false,
        ))
        .with_capabilities([AgentToolCapability::ObserveEnvironment])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["skill"],
            "properties": {
                "skill": {
                    "type": "string",
                    "description": "Skill id to activate."
                },
                "resource": {
                    "type": "string",
                    "description": "Optional relative resource path from a prior activation result. When set, returns only that bundled resource."
                },
                "max_bytes": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum resource bytes to return. Defaults to 256 KiB and is capped at 1 MiB."
                },
                "reason": {
                    "type": "string",
                    "description": "Short reason why this skill should be activated."
                }
            }
        })),
    ]
}

pub fn is_tool_disclosure_bridge_name(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "tool_search"
            | "tool_describe"
            | "tool_call"
            | "skill_search"
            | "skill_describe"
            | "skill_activate"
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSkillDescriptor {
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_paths: Vec<String>,
    #[serde(skip)]
    pub resource_contents: BTreeMap<String, Vec<u8>>,
    pub source: AgentSkillSource,
    #[serde(default)]
    pub package_declared: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Legacy discovery metadata retained for wire compatibility. Skill
    /// eligibility and activation are independent of agent identity.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_families: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activation_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_base_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_manifest_path: Option<String>,
}

impl AgentSkillDescriptor {
    pub fn package(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            description: String::new(),
            license: None,
            compatibility: None,
            allowed_tools: None,
            metadata: BTreeMap::new(),
            resource_paths: Vec::new(),
            resource_contents: BTreeMap::new(),
            source: AgentSkillSource::Package,
            package_declared: true,
            module_id: None,
            module_version: None,
            path: None,
            target_agents: Vec::new(),
            model_families: Vec::new(),
            activation_tags: Vec::new(),
            required_capabilities: Vec::new(),
            embedded_text: None,
            catalog_version: None,
            catalog_content_hash: None,
            catalog_base_hash: None,
            catalog_status: None,
            catalog_manifest_path: None,
        }
    }

    fn from_module_resource(
        registration: &ModuleRegistration,
        resource: &ModuleDeclarativeResourceExport,
        package_declared: bool,
    ) -> Self {
        Self {
            id: resource.id.clone(),
            description: resource.description.clone(),
            license: None,
            compatibility: None,
            allowed_tools: None,
            metadata: BTreeMap::new(),
            resource_paths: Vec::new(),
            resource_contents: BTreeMap::new(),
            source: AgentSkillSource::Module,
            package_declared,
            module_id: Some(registration.module_id().as_str().to_string()),
            module_version: Some(registration.manifest.version.clone()),
            path: Some(resource.path.clone()),
            target_agents: resource.target_agents.clone(),
            model_families: resource.model_families.clone(),
            activation_tags: resource.activation_tags.clone(),
            required_capabilities: resource
                .required_capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect(),
            embedded_text: resource.embedded_text.clone(),
            catalog_version: None,
            catalog_content_hash: None,
            catalog_base_hash: None,
            catalog_status: None,
            catalog_manifest_path: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentSkillSource {
    Package,
    Module,
    Catalog,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSkillPlan {
    pub skills: Vec<AgentSkillDescriptor>,
}

impl AgentSkillPlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_package_and_modules(
        package_skills: impl IntoIterator<Item = impl Into<String>>,
        module_projection: &AgentModuleToolProjection,
        agent_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::from_package_modules_and_catalog(
            package_skills,
            module_projection,
            agent_keys,
            std::iter::empty::<AgentSkillDescriptor>(),
        )
    }

    pub fn from_package_modules_and_catalog(
        package_skills: impl IntoIterator<Item = impl Into<String>>,
        module_projection: &AgentModuleToolProjection,
        _agent_keys: impl IntoIterator<Item = impl Into<String>>,
        catalog_skills: impl IntoIterator<Item = AgentSkillDescriptor>,
    ) -> Self {
        let mut descriptors = BTreeMap::<String, AgentSkillDescriptor>::new();
        let package_skill_ids = package_skills
            .into_iter()
            .map(Into::into)
            .filter(|skill: &String| !skill.trim().is_empty())
            .collect::<BTreeSet<_>>();
        for skill in &package_skill_ids {
            descriptors.insert(skill.clone(), AgentSkillDescriptor::package(skill.clone()));
        }
        for registration in &module_projection.registrations {
            for resource in &registration.surfaces.declarative_resources {
                if resource.kind != ModuleDeclarativeResourceKind::Skill {
                    continue;
                }
                descriptors.insert(
                    resource.id.clone(),
                    AgentSkillDescriptor::from_module_resource(
                        registration,
                        resource,
                        package_skill_ids.contains(&resource.id),
                    ),
                );
            }
        }
        for skill in catalog_skills {
            descriptors.insert(skill.id.clone(), skill);
        }
        Self {
            skills: descriptors.into_values().collect(),
        }
    }

    pub fn from_package_modules_and_catalog_entries<'a>(
        package_skills: impl IntoIterator<Item = impl Into<String>>,
        module_projection: &AgentModuleToolProjection,
        agent_keys: impl IntoIterator<Item = impl Into<String>>,
        catalog_entries: impl IntoIterator<Item = &'a SkillCatalogEntry>,
    ) -> Self {
        Self::from_package_modules_and_catalog(
            package_skills,
            module_projection,
            agent_keys,
            catalog_entries
                .into_iter()
                .map(|entry| entry.to_agent_descriptor(None)),
        )
    }

    pub fn get(&self, skill_id: &str) -> Option<&AgentSkillDescriptor> {
        self.skills.iter().find(|skill| skill.id == skill_id)
    }

    pub fn matching(&self, query: Option<&str>) -> Vec<&AgentSkillDescriptor> {
        let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) else {
            return self.skills.iter().collect();
        };
        let query = query.to_ascii_lowercase();
        self.skills
            .iter()
            .filter(|skill| skill_matches_query(skill, &query))
            .collect()
    }
}

fn skill_matches_query(skill: &AgentSkillDescriptor, query: &str) -> bool {
    skill.id.to_ascii_lowercase().contains(query)
        || skill.description.to_ascii_lowercase().contains(query)
        || skill
            .module_id
            .as_deref()
            .is_some_and(|module_id| module_id.to_ascii_lowercase().contains(query))
        || skill
            .path
            .as_deref()
            .is_some_and(|path| path.to_ascii_lowercase().contains(query))
        || skill
            .catalog_version
            .as_deref()
            .is_some_and(|version| version.to_ascii_lowercase().contains(query))
        || skill
            .catalog_status
            .as_deref()
            .is_some_and(|status| status.to_ascii_lowercase().contains(query))
        || skill
            .catalog_content_hash
            .as_deref()
            .is_some_and(|hash| hash.to_ascii_lowercase().contains(query))
        || skill
            .target_agents
            .iter()
            .any(|value| value.to_ascii_lowercase().contains(query))
        || skill
            .model_families
            .iter()
            .any(|value| value.to_ascii_lowercase().contains(query))
        || skill
            .activation_tags
            .iter()
            .any(|value| value.to_ascii_lowercase().contains(query))
        || skill
            .required_capabilities
            .iter()
            .any(|value| value.to_ascii_lowercase().contains(query))
}

fn descriptor_matches_query(descriptor: &AgentToolDescriptor, query: &str) -> bool {
    descriptor.spec.name.to_ascii_lowercase().contains(query)
        || descriptor
            .spec
            .description
            .to_ascii_lowercase()
            .contains(query)
        || descriptor
            .toolsets
            .iter()
            .any(|toolset| toolset.to_ascii_lowercase().contains(query))
        || descriptor
            .capabilities
            .iter()
            .any(|capability| capability.availability_label().contains(query))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AgentToolPlanner;

impl AgentToolPlanner {
    pub fn plan(
        runtime_descriptors: impl IntoIterator<Item = AgentToolDescriptor>,
        module_projection: &AgentModuleToolProjection,
        session_specs: impl IntoIterator<Item = AgentToolSpec>,
        default_specs: impl IntoIterator<Item = AgentToolSpec>,
        use_default_specs: bool,
        visible_toolsets: impl IntoIterator<Item = impl Into<String>>,
        registry_generation: u64,
    ) -> AgentToolPlan {
        let mut plan = AgentToolPlan::new();
        plan.visible_toolsets = visible_toolsets.into_iter().map(Into::into).collect();
        plan.registry_generation = registry_generation;
        if use_default_specs {
            for spec in default_specs {
                plan.push_or_replace(
                    AgentToolDescriptor::from_spec(spec),
                    AgentToolPlanSource::Default,
                    false,
                );
            }
        }
        for descriptor in module_projection.project_descriptors() {
            plan.push_or_replace(descriptor, AgentToolPlanSource::Module, false);
        }
        for spec in session_specs {
            plan.push_or_replace(
                AgentToolDescriptor::from_spec(spec),
                AgentToolPlanSource::Session,
                false,
            );
        }
        for descriptor in runtime_descriptors {
            plan.push_or_replace(descriptor, AgentToolPlanSource::Runtime, true);
        }
        for descriptor in tool_disclosure_descriptors() {
            plan.push_or_replace(descriptor, AgentToolPlanSource::Runtime, true);
        }
        plan
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentModuleToolProjection {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registrations: Vec<ModuleRegistration>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub loader_states: Vec<ModuleLoaderState>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentModuleToolBinding {
    pub descriptor: AgentToolDescriptor,
    pub route: String,
    pub category: String,
}

impl AgentModuleToolProjection {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.registrations.is_empty() && self.loader_states.is_empty()
    }

    pub fn with_registration(mut self, registration: ModuleRegistration) -> Self {
        self.registrations.push(registration);
        self
    }

    pub fn with_loader_state(mut self, state: ModuleLoaderState) -> Self {
        self.loader_states.push(state);
        self
    }

    pub fn from_registry(registry: &sinomo_runtime::module::ModuleRegistry) -> Self {
        Self {
            registrations: registry.registrations().cloned().collect(),
            loader_states: Vec::new(),
        }
    }

    pub fn project_bindings(&self) -> Vec<AgentModuleToolBinding> {
        let loader_states = self
            .loader_states
            .iter()
            .map(|state| (state.module_id.as_str(), state))
            .collect::<BTreeMap<_, _>>();
        let mut bindings = Vec::new();
        let mut names = BTreeSet::new();
        for registration in &self.registrations {
            if loader_states
                .get(registration.manifest.module_id.as_str())
                .is_some_and(|state| !module_loader_state_is_routable(state))
            {
                continue;
            }
            for route in &registration.surfaces.object_rpc_routes {
                let base_name = module_object_rpc_tool_name(&route.category, &route.operation);
                let name = unique_module_tool_name(base_name, &mut names);
                let descriptor = AgentToolDescriptor::new(AgentToolSpec::new(
                    AgentToolKind::ModuleObjectRpc,
                    name,
                    format!(
                        "Call active module object-RPC route {} for module {} ({}/{})",
                        route.route,
                        registration.manifest.module_id,
                        registration.manifest.owner,
                        registration.manifest.version
                    ),
                    true,
                ))
                .with_toolsets([
                    "module".to_string(),
                    "object_rpc".to_string(),
                    registration.manifest.owner.clone(),
                ])
                .with_capabilities([AgentToolCapability::CallModule])
                .with_required_local_permissions(route.required_local_permissions.clone())
                .with_input_schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "route_key": {
                            "type": "string",
                            "description": "Optional routing key for the module invocation."
                        },
                        "input": {
                            "description": "Route-specific object-RPC request payload."
                        }
                    }
                }))
                .with_output_schema(serde_json::json!({
                    "type": "object",
                    "description": "Route-specific object-RPC response payload."
                }));
                bindings.push(AgentModuleToolBinding {
                    descriptor,
                    route: route.route.clone(),
                    category: route.category.clone(),
                });
            }
        }
        bindings
    }

    pub fn project_descriptors(&self) -> Vec<AgentToolDescriptor> {
        self.project_bindings()
            .into_iter()
            .map(|binding| binding.descriptor)
            .collect()
    }
}

fn module_loader_state_is_routable(state: &ModuleLoaderState) -> bool {
    !matches!(state.hot_swap_state, HotSwapState::Quarantined)
}

fn module_object_rpc_tool_name(category: &str, operation: &str) -> String {
    let category = sanitize_tool_name_segment(category);
    let operation = sanitize_tool_name_segment(operation);
    if category.is_empty() {
        format!("module_rpc_{operation}")
    } else if operation.is_empty() {
        format!("module_rpc_{category}")
    } else {
        format!("module_rpc_{category}_{operation}")
    }
}

fn sanitize_tool_name_segment(value: &str) -> String {
    let mut sanitized = String::new();
    let mut previous_was_underscore = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
            previous_was_underscore = false;
        } else if !previous_was_underscore {
            sanitized.push('_');
            previous_was_underscore = true;
        }
    }
    sanitized.trim_matches('_').to_string()
}

fn unique_module_tool_name(base: String, names: &mut BTreeSet<String>) -> String {
    let base = if base == "module_rpc_" || base.is_empty() {
        "module_rpc_route".to_string()
    } else {
        base
    };
    if names.insert(base.clone()) {
        return base;
    }
    for index in 2usize.. {
        let candidate = format!("{base}_{index}");
        if names.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("unbounded numeric suffix loop always returns")
}

fn descriptor_matches_toolsets(
    descriptor: &AgentToolDescriptor,
    visible_toolsets: &[String],
) -> bool {
    visible_toolsets.is_empty()
        || descriptor
            .toolsets
            .iter()
            .any(|toolset| visible_toolsets.iter().any(|visible| visible == toolset))
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolCapability {
    ReadWorkspace,
    WriteWorkspace,
    ExecuteProcess,
    InspectVersionControl,
    AccessNetwork,
    PromptUser,
    Delegate,
    ObserveEnvironment,
    CallModel,
    CallModule,
    CallMcp,
    ManageBackgroundTask,
    InspectClusterState,
    ManageConfig,
    ManageSkills,
    ReadSessionHistory,
    ReadMemory,
    WriteMemory,
}

impl AgentToolCapability {
    pub(crate) fn availability_label(&self) -> &'static str {
        match self {
            AgentToolCapability::ReadWorkspace => "read_workspace",
            AgentToolCapability::WriteWorkspace => "write_workspace",
            AgentToolCapability::ExecuteProcess => "execute_process",
            AgentToolCapability::InspectVersionControl => "inspect_version_control",
            AgentToolCapability::AccessNetwork => "access_network",
            AgentToolCapability::PromptUser => "prompt_user",
            AgentToolCapability::Delegate => "delegate",
            AgentToolCapability::ObserveEnvironment => "observe_environment",
            AgentToolCapability::CallModel => "call_model",
            AgentToolCapability::CallModule => "call_module",
            AgentToolCapability::CallMcp => "call_mcp",
            AgentToolCapability::ManageBackgroundTask => "manage_background_task",
            AgentToolCapability::InspectClusterState => "inspect_cluster_state",
            AgentToolCapability::ManageConfig => "manage_config",
            AgentToolCapability::ManageSkills => "manage_skills",
            AgentToolCapability::ReadSessionHistory => "read_session_history",
            AgentToolCapability::ReadMemory => "read_memory",
            AgentToolCapability::WriteMemory => "write_memory",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolOutcome {
    pub call_id: TaskId,
    pub step_index: usize,
    pub call_index: usize,
    pub call: AgentToolCall,
    pub status: AgentToolStatus,
    pub summary: String,
    #[serde(default)]
    pub output: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<EventId>,
    pub evidence: AgentToolEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolEvidence {
    #[serde(default, alias = "policy")]
    pub availability: AgentToolAvailabilityDecision,
    #[serde(default)]
    pub output: AgentToolOutputEvidence,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<AgentToolArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<AgentToolExecutionEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<AgentToolEvidenceRecord>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolOutputEvidence {
    #[serde(default)]
    pub model_visible_bytes: usize,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolArtifactRef {
    pub kind: String,
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

impl AgentToolArtifactRef {
    pub fn new(kind: impl Into<String>, uri: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            uri: uri.into(),
            label: None,
            media_type: None,
        }
    }

    pub fn sifs(kind: impl Into<String>, path: impl AsRef<str>) -> Self {
        let path = path.as_ref().trim();
        let uri = if path.starts_with("sifs://") {
            path.to_string()
        } else {
            format!("sifs://{}", path.trim_start_matches('/'))
        };
        Self::new(kind, uri)
    }

    pub fn is_sifs(&self) -> bool {
        self.uri.starts_with("sifs://")
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn with_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.media_type = Some(media_type.into());
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentCommandOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_ref: Option<AgentToolArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_ref: Option<AgentToolArtifactRef>,
    #[serde(default)]
    pub aggregated_output: String,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub stdout_truncated: bool,
    #[serde(default)]
    pub stderr_truncated: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolExecutionEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

pub const AGENT_CODING_EVIDENCE_KIND: &str = "coding";
pub const AGENT_SKILL_USAGE_EVIDENCE_KIND: &str = "skill_usage";
pub const AGENT_SKILL_EVOLUTION_EVIDENCE_KIND: &str = "skill_evolution";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolEvidenceRecord {
    pub kind: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

impl AgentToolEvidenceRecord {
    pub fn new(kind: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            kind: kind.into(),
            data,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentCodingToolEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
    pub verification_state: AgentVerificationState,
}

impl Default for AgentCodingToolEvidence {
    fn default() -> Self {
        Self {
            patch_ref: None,
            diff_ref: None,
            diff_summary: None,
            changed_files: Vec::new(),
            verification_state: AgentVerificationState::NotRun,
        }
    }
}

impl AgentToolEvidence {
    pub fn availability_only(allowed: bool, reason: impl Into<String>) -> Self {
        Self {
            availability: AgentToolAvailabilityDecision {
                allowed,
                reason: reason.into(),
            },
            output: AgentToolOutputEvidence::default(),
            artifacts: Vec::new(),
            execution: None,
            records: Vec::new(),
            metadata: serde_json::Value::Null,
        }
    }

    #[deprecated(note = "use availability_only")]
    pub fn policy_only(allowed: bool, reason: impl Into<String>) -> Self {
        Self::availability_only(allowed, reason)
    }

    pub fn with_output(mut self, visible_bytes: usize, truncated: bool) -> Self {
        self.output.model_visible_bytes = visible_bytes;
        self.output.truncated = truncated;
        self
    }

    pub fn with_output_value(mut self, value: &serde_json::Value, truncated: bool) -> Self {
        self.output.model_visible_bytes = value.to_string().len();
        self.output.truncated = truncated;
        self
    }

    pub fn with_command(mut self, cwd: impl Into<String>, exit_code: Option<i32>) -> Self {
        self.execution = Some(AgentToolExecutionEvidence {
            environment: Some("local_workspace".to_string()),
            cwd: Some(cwd.into()),
            exit_code,
        });
        self
    }

    pub fn with_artifact(mut self, artifact: AgentToolArtifactRef) -> Self {
        self.artifacts.push(artifact);
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_record_value(mut self, kind: impl Into<String>, data: serde_json::Value) -> Self {
        let kind = kind.into();
        if let Some(record) = self.records.iter_mut().find(|record| record.kind == kind) {
            record.data = data;
        } else {
            self.records.push(AgentToolEvidenceRecord::new(kind, data));
        }
        self
    }

    pub fn with_record<T: Serialize>(mut self, kind: impl Into<String>, record: &T) -> Self {
        let data = serde_json::to_value(record).expect("agent tool evidence record serializes");
        self = self.with_record_value(kind, data);
        self
    }

    pub fn record_value(&self, kind: &str) -> Option<&serde_json::Value> {
        self.records
            .iter()
            .find(|record| record.kind == kind)
            .map(|record| &record.data)
    }

    pub fn record<T: DeserializeOwned>(&self, kind: &str) -> Option<T> {
        self.record_value(kind)
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
    }

    pub fn full_output_ref(&self) -> Option<&str> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.kind == "full_output")
            .map(|artifact| artifact.uri.as_str())
    }
}

impl Default for AgentToolEvidence {
    fn default() -> Self {
        Self::availability_only(true, "available through registered agent tool surface")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolAvailabilityDecision {
    pub allowed: bool,
    pub reason: String,
}

impl Default for AgentToolAvailabilityDecision {
    fn default() -> Self {
        Self {
            allowed: true,
            reason: "available through registered agent tool surface".to_string(),
        }
    }
}

/// Backward-compatible name for a tool availability decision.
#[deprecated(note = "use AgentToolAvailabilityDecision")]
pub type AgentToolPolicyDecision = AgentToolAvailabilityDecision;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentToolStatus {
    Succeeded,
    Failed,
    Blocked,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolResult {
    pub status: AgentToolStatus,
    pub summary: String,
    #[serde(default)]
    pub output: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<AgentToolEvidence>,
}

impl AgentToolResult {
    pub fn succeeded(summary: impl Into<String>, output: serde_json::Value) -> Self {
        Self {
            status: AgentToolStatus::Succeeded,
            summary: summary.into(),
            output,
            evidence: None,
        }
    }

    pub fn failed(summary: impl Into<String>, output: serde_json::Value) -> Self {
        Self {
            status: AgentToolStatus::Failed,
            summary: summary.into(),
            output,
            evidence: None,
        }
    }

    pub fn blocked(summary: impl Into<String>, output: serde_json::Value) -> Self {
        Self {
            status: AgentToolStatus::Blocked,
            summary: summary.into(),
            output,
            evidence: None,
        }
    }

    pub fn cancelled(summary: impl Into<String>, output: serde_json::Value) -> Self {
        Self {
            status: AgentToolStatus::Cancelled,
            summary: summary.into(),
            output,
            evidence: None,
        }
    }

    pub fn with_evidence(mut self, evidence: AgentToolEvidence) -> Self {
        self.evidence = Some(evidence);
        self
    }
}

pub struct AgentToolRequest<'a> {
    pub call_id: &'a TaskId,
    pub session: &'a InteractionSession,
    pub step_index: usize,
    pub call_index: usize,
    pub call: &'a AgentToolCall,
    pub cancellation_token: AgentCancellationToken,
    pub progress_sink: AgentToolProgressSink,
}

pub trait AgentTool: Send + Sync {
    fn descriptor(&self) -> AgentToolDescriptor;

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult;
}

#[derive(Clone, Default)]
pub struct AgentToolProgressSink {
    callback: Option<Arc<dyn Fn(AgentToolProgressEvent) + Send + Sync>>,
}

impl AgentToolProgressSink {
    pub fn new(callback: impl Fn(AgentToolProgressEvent) + Send + Sync + 'static) -> Self {
        Self {
            callback: Some(Arc::new(callback)),
        }
    }

    pub fn emit(&self, event: AgentToolProgressEvent) {
        if let Some(callback) = &self.callback {
            callback(event);
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.callback.is_some()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentToolProgressEvent {
    ProcessStarted {
        process: AgentProcessSnapshot,
    },
    ProcessOutput {
        process: AgentProcessSnapshot,
    },
    ProcessCompleted {
        process: AgentProcessSnapshot,
    },
    Message {
        summary: String,
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        payload: serde_json::Value,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_descriptor(name: &str) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            name,
            name,
            false,
        ))
    }

    #[test]
    fn tool_descriptors_default_to_turn_based_and_round_trip_legacy_json() {
        let descriptor = test_descriptor("legacy_tool");
        assert!(descriptor.supports_invocation_mode(AgentToolInvocationMode::TurnBased));
        assert!(!descriptor.supports_invocation_mode(AgentToolInvocationMode::Realtime));

        let value = serde_json::to_value(&descriptor).expect("serialize descriptor");
        assert!(value.get("invocation_modes").is_none());
        let restored: AgentToolDescriptor =
            serde_json::from_value(value).expect("deserialize legacy descriptor");
        assert_eq!(
            restored.invocation_modes,
            vec![AgentToolInvocationMode::TurnBased]
        );
    }

    #[test]
    fn tool_plan_filters_exposure_without_replacing_implementations() {
        let plan = AgentToolPlan {
            entries: vec![
                AgentToolPlanEntry {
                    descriptor: test_descriptor("turn_only"),
                    source: AgentToolPlanSource::Runtime,
                    executable: true,
                },
                AgentToolPlanEntry {
                    descriptor: test_descriptor("shared")
                        .with_invocation_modes(AgentToolInvocationMode::all()),
                    source: AgentToolPlanSource::Runtime,
                    executable: true,
                },
            ],
            ..AgentToolPlan::default()
        };

        let realtime = plan.for_invocation_mode(AgentToolInvocationMode::Realtime);
        assert_eq!(realtime.entries.len(), 1);
        assert_eq!(realtime.entries[0].descriptor.spec.name, "shared");
        assert!(realtime.entries[0].executable);
    }

    #[test]
    fn module_tool_projection_preserves_route_local_permissions() {
        let module_id = TaskId::generate();
        let registration = sinomo_runtime::module::ModuleRegistration::native_rust(
            module_id.clone(),
            "0.1.0",
            "permission-test",
            "sinomo-permission-test",
            "permission-test-v1",
            sinomo_runtime::core::TrustRing::R1TrustedRuntime,
            "permission.test",
        )
        .with_surfaces(
            sinomo_runtime::module::ModuleSurfaces::default().with_object_rpc_route(
                sinomo_runtime::module::ModuleObjectRpcRoute::new(
                    "/permission/test/status",
                    "permission-test",
                    "status",
                    sinomo_runtime::module::ModuleRouteAuth::LocalOrClusterMember,
                )
                .require_local_permission("remote-management"),
            ),
        );
        let projection = AgentModuleToolProjection::new().with_registration(registration);

        let descriptors = projection.project_descriptors();
        let bindings = projection.project_bindings();

        assert_eq!(descriptors.len(), 1);
        assert_eq!(bindings[0].route, "/permission/test/status");
        assert_eq!(bindings[0].category, "permission-test");
        assert_eq!(
            descriptors[0].required_local_permissions,
            vec!["remote-management".to_string()]
        );
    }
}
