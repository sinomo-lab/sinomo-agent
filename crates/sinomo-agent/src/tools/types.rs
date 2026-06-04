use crate::{AgentToolCall, AgentToolSpec, AgentVerificationState, InteractionSession, TaskId};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_runtime::events::EventId;

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

fn default_toolsets_for_kind(kind: &crate::AgentToolKind) -> Vec<&'static str> {
    use crate::AgentToolKind;
    match kind {
        AgentToolKind::ReadFile
        | AgentToolKind::SearchFiles
        | AgentToolKind::WriteFile
        | AgentToolKind::ApplyPatch
        | AgentToolKind::ChangedFiles
        | AgentToolKind::GitDiff
        | AgentToolKind::ReplaceExact
        | AgentToolKind::ReplaceRange
        | AgentToolKind::RunShellCommand
        | AgentToolKind::StartCommandSession
        | AgentToolKind::InspectCommandSession
        | AgentToolKind::WriteCommandStdin
        | AgentToolKind::CancelCommandSession => vec!["core", "coding", "local"],
        AgentToolKind::RequestUserInput => vec!["core", "interactive"],
        AgentToolKind::ConfigSearch | AgentToolKind::ConfigGet | AgentToolKind::ConfigSet => {
            vec!["config", "local"]
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
    Session,
    Default,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolPlanEntry {
    pub descriptor: AgentToolDescriptor,
    pub source: AgentToolPlanSource,
    pub executable: bool,
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
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AgentToolPlanner;

impl AgentToolPlanner {
    pub fn plan(
        runtime_descriptors: impl IntoIterator<Item = AgentToolDescriptor>,
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
        plan
    }
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
    ManageBackgroundTask,
    InspectClusterState,
    ManageConfig,
}

impl AgentToolCapability {
    pub(crate) fn policy_label(&self) -> &'static str {
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
            AgentToolCapability::ManageBackgroundTask => "manage_background_task",
            AgentToolCapability::InspectClusterState => "inspect_cluster_state",
            AgentToolCapability::ManageConfig => "manage_config",
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
    pub policy: AgentToolPolicyDecision,
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
    pub fn policy_only(allowed: bool, reason: impl Into<String>) -> Self {
        Self {
            policy: AgentToolPolicyDecision {
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
        Self::policy_only(true, "allowed by registered agent tool policy")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolPolicyDecision {
    pub allowed: bool,
    pub reason: String,
}

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
}

pub trait AgentTool: Send + Sync {
    fn descriptor(&self) -> AgentToolDescriptor;

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult;
}
