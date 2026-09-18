//! Agent construction and the public facade for session, turn, and tool execution.

mod coding;
mod compaction;
mod context;
mod control;
mod model;
mod session;
mod tool_execution;
mod tool_output;
mod trace;
mod turn;

pub use coding::*;
pub use compaction::*;
pub use context::*;
pub use control::*;
pub use model::*;
pub use session::*;
pub use tool_execution::*;
use tool_output::*;
pub use trace::*;
use turn::*;

use crate::session_store::{
    AgentSessionStore, AgentSessionStoreError, AgentSessionStoreRecord,
    AgentSessionStoreRecordKind, FileAgentSessionStore, NoopAgentSessionStore,
};
use crate::tools::{
    AGENT_CODING_EVIDENCE_KIND, AGENT_SKILL_USAGE_EVIDENCE_KIND, AgentCodingToolEvidence,
    AgentModuleToolProjection, AgentSkillDescriptor, AgentSkillPlan, AgentTool,
    AgentToolAvailabilityDecision, AgentToolCapability, AgentToolCustomFormat, AgentToolCustomSpec,
    AgentToolDescriptor, AgentToolEvidence, AgentToolInvocationMode, AgentToolOutcome,
    AgentToolPlan, AgentToolPlanner, AgentToolProgressEvent, AgentToolProgressSink,
    AgentToolProviderSpec, AgentToolRegistry, AgentToolStatus, is_tool_disclosure_bridge_name,
};
use crate::{
    AgentCancellationToken, AgentCompactionEngine, AgentCompactionRequest, AgentDefinition,
    AgentError, AgentLoopDecision, AgentLoopInput, AgentOutputFormat, AgentToolCall,
    AsyncTaskNextAction, AsyncTaskUpdate, CodexStyleAgentCompactionEngine, InteractionModel,
    InteractionSession, SkillCatalog, TaskId, agent_output_contract, parse_agent_loop_response,
    render_agent_package_system_prompt, render_agent_system_prompt_with_descriptors,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelCustomToolFormat, LanguageModelCustomToolSpec, LanguageModelErrorCategory,
    LanguageModelMessage, LanguageModelProviderToolSpec, LanguageModelRequest,
    LanguageModelRequestItem, LanguageModelResponse, LanguageModelResponseFormat,
    LanguageModelRole, LanguageModelStreamEvent, LanguageModelToolAdapterSource,
    LanguageModelToolCall, LanguageModelToolChoice, LanguageModelToolSpec,
    classify_language_model_error_detail, collect_language_model_tool_calls,
    inferred_model_context_window_tokens, language_model_system_instructions,
};
use sinomo_memory::{
    EvidenceCoverage, IndexedFileMemoryStore, MemoryAccessContext, MemoryEvidence,
    MemoryEvidenceBundle, MemoryQuery, MemoryRetriever,
};
use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventKind, LatencyClass, MemoryScope, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source,
};
use sinomo_runtime::events::{EventId, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{VfsAppendOptions, VfsHandle};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

const DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO: f32 = 0.80;
const CONTEXT_ESTIMATE_BYTES_PER_TOKEN: usize = 4;
const DEFAULT_UNKNOWN_MODEL_CONTEXT_WINDOW_TOKENS: usize = 32 * 1024;
const DEFAULT_RETRIEVED_MEMORY_RECORDS: usize = 8;
const DEFAULT_RETRIEVED_MEMORY_BYTES: usize = 16 * 1024;
const DEFAULT_SKILL_RESOURCE_BYTES: usize = 256 * 1024;
const MAX_SKILL_RESOURCE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRole {
    Main,
    Background,
    Deliberation,
    Evolution,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct AgentContextBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_threshold_ratio: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_tail_items: Option<usize>,
}

impl AgentContextBudget {
    pub fn for_loaded_model(provider: &str, model: &str, max_output_tokens: Option<u32>) -> Self {
        Self::for_loaded_model_with_limits(provider, model, max_output_tokens, None, None)
    }

    pub fn for_loaded_model_with_limits(
        provider: &str,
        model: &str,
        max_output_tokens: Option<u32>,
        max_context_tokens: Option<u32>,
        auto_compact_token_limit: Option<u32>,
    ) -> Self {
        let inferred_window = resolved_model_context_window_tokens(provider, model);
        let supported_max = sinomo_lm::inferred_model_max_context_window_tokens(provider, model);
        let window_tokens = max_context_tokens
            .map(|value| value as usize)
            .map(|value| supported_max.map_or(value, |max| value.min(max)))
            .unwrap_or(inferred_window);
        let max_context_bytes = context_max_bytes_for_window(window_tokens, max_output_tokens);
        let compaction_threshold_ratio =
            auto_compact_token_limit.map_or(DEFAULT_CONTEXT_COMPACTION_THRESHOLD_RATIO, |limit| {
                if max_context_bytes == 0 {
                    0.0
                } else {
                    ((limit as usize).saturating_mul(CONTEXT_ESTIMATE_BYTES_PER_TOKEN) as f32
                        / max_context_bytes as f32)
                        .clamp(0.0, 1.0)
                }
            });
        Self {
            max_context_bytes: Some(max_context_bytes),
            compaction_threshold_ratio: Some(compaction_threshold_ratio),
            retained_tail_items: None,
        }
    }

    pub fn compaction_threshold_bytes(&self) -> Option<usize> {
        let max_context_bytes = self.max_context_bytes?;
        let ratio = self.compaction_threshold_ratio?;
        let ratio = if ratio.is_finite() {
            ratio.clamp(0.0, 1.0)
        } else {
            return None;
        };
        Some((max_context_bytes as f32 * ratio).round() as usize)
    }
}

fn context_max_bytes_for_window(window_tokens: usize, max_output_tokens: Option<u32>) -> usize {
    let output_reserve = max_output_tokens.unwrap_or_default() as usize;
    let input_tokens = window_tokens.saturating_sub(output_reserve);
    input_tokens.saturating_mul(CONTEXT_ESTIMATE_BYTES_PER_TOKEN)
}

pub(crate) fn resolved_model_context_window_tokens(provider: &str, model: &str) -> usize {
    inferred_model_context_window_tokens(provider, model)
        .unwrap_or(DEFAULT_UNKNOWN_MODEL_CONTEXT_WINDOW_TOKENS)
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentItem {
    UserMessage {
        item_id: TaskId,
        step_index: usize,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
    },
    AssistantText {
        item_id: TaskId,
        step_index: usize,
        content: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        native_items: Vec<LanguageModelRequestItem>,
    },
    AssistantReasoningSummary {
        item_id: TaskId,
        step_index: usize,
        summary: String,
    },
    ToolCall {
        item_id: TaskId,
        step_index: usize,
        call_index: usize,
        call: AgentToolCall,
    },
    ToolResult {
        item_id: TaskId,
        step_index: usize,
        call_index: usize,
        call_id: TaskId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_call_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        status: AgentToolStatus,
        summary: String,
        #[serde(default)]
        output: serde_json::Value,
        evidence: AgentToolEvidence,
    },
    AsyncTaskUpdate {
        item_id: TaskId,
        update: crate::AsyncTaskUpdate,
    },
    ContextInjection {
        item_id: TaskId,
        report: ContextAssemblyReport,
    },
    CompactionSummary {
        item_id: TaskId,
        summary: AgentCompactionSummary,
    },
    ModelUsage {
        item_id: TaskId,
        step_index: usize,
        provider: String,
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<sinomo_lm::LanguageModelUsage>,
    },
    Error {
        item_id: TaskId,
        step_index: usize,
        detail: String,
    },
    PatchSnapshot {
        item_id: TaskId,
        step_index: usize,
        call_id: TaskId,
        status: AgentToolStatus,
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        patch_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changed_files: Vec<String>,
    },
    VerificationResult {
        item_id: TaskId,
        step_index: usize,
        call_id: TaskId,
        command: String,
        status: AgentVerificationState,
        summary: String,
    },
    CodingRunState {
        item_id: TaskId,
        step_index: usize,
        state: CodingRunState,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentContextAttachment {
    pub kind: String,
    pub summary: String,
    #[serde(default)]
    pub payload_json: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_original_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub payload_model_visible_truncated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentCandidateEvidence {
    Patch {
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        patch_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changed_files: Vec<String>,
    },
    Verification {
        command: String,
        status: AgentVerificationState,
        summary: String,
    },
    ToolResult {
        tool: String,
        status: AgentToolStatus,
        summary: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentCandidateEvidenceBundle {
    pub bundle_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_summary: Option<String>,
    pub final_response: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<AgentCandidateEvidence>,
}

pub(crate) fn agent_item_id(item: &AgentItem) -> TaskId {
    match item {
        AgentItem::UserMessage { item_id, .. }
        | AgentItem::AssistantText { item_id, .. }
        | AgentItem::AssistantReasoningSummary { item_id, .. }
        | AgentItem::ToolCall { item_id, .. }
        | AgentItem::ToolResult { item_id, .. }
        | AgentItem::AsyncTaskUpdate { item_id, .. }
        | AgentItem::ContextInjection { item_id, .. }
        | AgentItem::CompactionSummary { item_id, .. }
        | AgentItem::ModelUsage { item_id, .. }
        | AgentItem::Error { item_id, .. }
        | AgentItem::PatchSnapshot { item_id, .. }
        | AgentItem::VerificationResult { item_id, .. }
        | AgentItem::CodingRunState { item_id, .. } => item_id.clone(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentConfig {
    pub provider: String,
    pub model: String,
    #[serde(default = "default_agent_config_definition")]
    pub definition: AgentDefinition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    max_steps_explicit_override: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls_per_step: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    max_tool_calls_per_step_explicit_override: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_model_repair_attempts: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    max_model_repair_attempts_explicit_override: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
    pub run_context: AgentRunContext,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_toolsets: Vec<String>,
    #[serde(default, skip_serializing_if = "AgentModuleToolProjection::is_empty")]
    pub module_tool_projection: AgentModuleToolProjection,
    #[serde(default, skip_serializing_if = "SkillCatalog::is_empty")]
    pub skill_catalog: SkillCatalog,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub skill_catalog_texts: BTreeMap<String, String>,
    #[serde(skip)]
    pub skill_catalog_resources: BTreeMap<String, Vec<crate::AgentSkillPackageFile>>,
    #[serde(default = "default_workspace_instruction_files")]
    pub workspace_instruction_files: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_file_max_bytes: Option<usize>,
    #[serde(default = "default_expand_prompt_references")]
    pub expand_prompt_references: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_reference_max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_reference_max_entries: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_attachment_max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_update_max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub context_budget: AgentContextBudget,
}

impl AgentConfig {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        let provider = provider.into();
        let model = model.into();
        let definition = AgentDefinition::main();
        let visible_toolsets =
            default_visible_toolsets_for_role_and_provider(&definition.role, &provider);
        let context_budget =
            AgentContextBudget::for_loaded_model(&provider, &model, definition.max_output_tokens);
        Self {
            provider,
            model,
            temperature: definition.temperature,
            max_output_tokens: definition.max_output_tokens,
            definition,
            max_steps: None,
            max_steps_explicit_override: false,
            max_tool_calls_per_step: None,
            max_tool_calls_per_step_explicit_override: false,
            max_model_repair_attempts: None,
            max_model_repair_attempts_explicit_override: false,
            workspace_root: None,
            run_context: AgentRunContext::default(),
            visible_toolsets,
            module_tool_projection: AgentModuleToolProjection::default(),
            skill_catalog: SkillCatalog::default(),
            skill_catalog_texts: BTreeMap::new(),
            skill_catalog_resources: BTreeMap::new(),
            workspace_instruction_files: default_workspace_instruction_files(),
            instruction_file_max_bytes: Some(DEFAULT_WORKSPACE_INSTRUCTION_FILE_BYTES),
            expand_prompt_references: default_expand_prompt_references(),
            prompt_reference_max_bytes: Some(DEFAULT_PROMPT_REFERENCE_FILE_BYTES),
            prompt_reference_max_entries: Some(DEFAULT_PROMPT_REFERENCE_DIRECTORY_ENTRIES),
            context_attachment_max_bytes: Some(DEFAULT_CONTEXT_ATTACHMENT_PAYLOAD_BYTES),
            async_update_max_bytes: Some(DEFAULT_ASYNC_UPDATE_BYTES),
            context_budget,
        }
    }

    pub fn with_agent_definition(mut self, definition: AgentDefinition) -> Self {
        let previous_default_toolsets = default_visible_toolsets_for_role(&self.definition.role);
        let previous_provider_default_toolsets =
            default_visible_toolsets_for_role_and_provider(&self.definition.role, &self.provider);
        let previous_derived_context_budget = AgentContextBudget::for_loaded_model(
            &self.provider,
            &self.model,
            self.max_output_tokens,
        );
        self.temperature = definition
            .temperature
            .or(definition.model_policy.temperature);
        self.max_output_tokens = definition
            .max_output_tokens
            .or(definition.model_policy.max_output_tokens);
        if self.context_budget == previous_derived_context_budget
            || self.context_budget == AgentContextBudget::default()
        {
            self.context_budget = AgentContextBudget::for_loaded_model(
                &self.provider,
                &self.model,
                self.max_output_tokens,
            );
        }
        if !definition.tool_surface.visible_toolsets.is_empty() {
            self.visible_toolsets = definition.tool_surface.visible_toolsets.clone();
        } else if self.visible_toolsets == previous_default_toolsets
            || self.visible_toolsets == previous_provider_default_toolsets
        {
            self.visible_toolsets =
                default_visible_toolsets_for_role_and_provider(&definition.role, &self.provider);
        }
        if !self.max_steps_explicit_override {
            self.max_steps = definition.runtime_limits.max_steps;
        }
        if !self.max_tool_calls_per_step_explicit_override {
            self.max_tool_calls_per_step = definition.runtime_limits.max_tool_calls_per_step;
        }
        if !self.max_model_repair_attempts_explicit_override {
            self.max_model_repair_attempts = definition.runtime_limits.max_model_repair_attempts;
        }
        self.definition = definition;
        self
    }

    pub fn with_temperature(mut self, temperature: Option<f32>) -> Self {
        self.temperature = temperature;
        self
    }

    pub fn with_max_output_tokens(mut self, max_output_tokens: Option<u32>) -> Self {
        self.max_output_tokens = max_output_tokens;
        self
    }

    pub fn with_max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = Some(max_steps.max(1));
        self.max_steps_explicit_override = true;
        self
    }

    pub fn with_max_tool_calls_per_step(mut self, max_tool_calls_per_step: usize) -> Self {
        self.max_tool_calls_per_step = Some(max_tool_calls_per_step.max(1));
        self.max_tool_calls_per_step_explicit_override = true;
        self
    }

    pub fn with_max_model_repair_attempts(mut self, max_model_repair_attempts: usize) -> Self {
        self.max_model_repair_attempts = Some(max_model_repair_attempts);
        self.max_model_repair_attempts_explicit_override = true;
        self
    }

    pub fn with_workspace_root(mut self, workspace_root: impl Into<PathBuf>) -> Self {
        let workspace_root = workspace_root.into();
        if self.run_context.workspace.is_none() {
            self.run_context.workspace = Some(WorkspaceRef::local_path(workspace_root.clone()));
        }
        self.workspace_root = Some(workspace_root);
        self
    }

    pub fn with_run_context(mut self, run_context: AgentRunContext) -> Self {
        self.run_context = run_context;
        self
    }

    pub fn with_task_ref(mut self, task: AgentTaskRef) -> Self {
        self.run_context.task = Some(task);
        self
    }

    pub fn with_workspace_ref(mut self, workspace: WorkspaceRef) -> Self {
        self.run_context.workspace = Some(workspace);
        self
    }

    pub fn with_prompt_reference_expansion(mut self, enabled: bool) -> Self {
        self.expand_prompt_references = enabled;
        self
    }

    pub fn with_prompt_reference_limits(
        mut self,
        max_bytes: Option<usize>,
        max_entries: Option<usize>,
    ) -> Self {
        self.prompt_reference_max_bytes = max_bytes;
        self.prompt_reference_max_entries = max_entries;
        self
    }

    pub fn with_context_attachment_max_bytes(mut self, max_bytes: Option<usize>) -> Self {
        self.context_attachment_max_bytes = max_bytes;
        self
    }

    pub fn with_async_update_max_bytes(mut self, max_bytes: Option<usize>) -> Self {
        self.async_update_max_bytes = max_bytes;
        self
    }

    pub fn with_visible_toolsets(
        mut self,
        toolsets: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.visible_toolsets = toolsets.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_module_tool_projection(mut self, projection: AgentModuleToolProjection) -> Self {
        self.module_tool_projection = projection;
        self
    }

    pub fn with_skill_catalog(mut self, catalog: SkillCatalog) -> Self {
        self.skill_catalog = catalog;
        self
    }

    pub fn with_skill_catalog_texts(
        mut self,
        texts: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        self.skill_catalog_texts = texts.into_iter().collect();
        self
    }

    pub fn with_skill_catalog_resources(
        mut self,
        resources: impl IntoIterator<Item = (String, Vec<crate::AgentSkillPackageFile>)>,
    ) -> Self {
        self.skill_catalog_resources = resources.into_iter().collect();
        self
    }

    pub fn with_placement(mut self, placement: AgentPlacementMetadata) -> Self {
        self.run_context.placement = Some(placement);
        self
    }

    pub fn with_workspace_instruction_files(
        mut self,
        files: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) -> Self {
        self.workspace_instruction_files = files.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_instruction_file_max_bytes(mut self, max_bytes: Option<usize>) -> Self {
        self.instruction_file_max_bytes = max_bytes;
        self
    }

    pub fn with_context_budget(mut self, context_budget: AgentContextBudget) -> Self {
        self.context_budget = context_budget;
        self
    }

    pub fn with_model_context_limits(
        mut self,
        max_context_tokens: Option<u32>,
        auto_compact_token_limit: Option<u32>,
    ) -> Self {
        self.context_budget = AgentContextBudget::for_loaded_model_with_limits(
            &self.provider,
            &self.model,
            self.max_output_tokens,
            max_context_tokens,
            auto_compact_token_limit,
        );
        self
    }
}

fn default_agent_config_definition() -> AgentDefinition {
    AgentDefinition::main()
}

fn default_visible_toolsets_for_role(role: &crate::AgentRole) -> Vec<String> {
    let toolsets: &[&str] = match role {
        crate::AgentRole::Main => &[
            "core",
            "coding",
            "local",
            "interactive",
            "discovery",
            "version_control",
            "config",
            "delegation",
            "perception",
            "cluster",
            "remote",
        ],
        crate::AgentRole::Background => &[
            "core",
            "interactive",
            "config",
            "delegation",
            "perception",
            "cluster",
            "sifs",
            "remote",
        ],
        crate::AgentRole::Deliberation | crate::AgentRole::Evolution => &[
            "core",
            "coding",
            "local",
            "interactive",
            "discovery",
            "version_control",
        ],
    };
    toolsets.iter().copied().map(str::to_string).collect()
}

fn default_visible_toolsets_for_role_and_provider(
    role: &crate::AgentRole,
    provider: &str,
) -> Vec<String> {
    let mut toolsets = default_visible_toolsets_for_role(role);
    if is_openai_provider(provider)
        && matches!(
            role,
            crate::AgentRole::Main | crate::AgentRole::Deliberation | crate::AgentRole::Evolution
        )
    {
        push_unique(&mut toolsets, "provider_specific".to_string());
    }
    toolsets
}

fn is_openai_provider(provider: &str) -> bool {
    matches!(
        provider.to_ascii_lowercase().as_str(),
        "openai" | "openai-api"
    )
}

fn default_workspace_instruction_files() -> Vec<PathBuf> {
    vec![PathBuf::from("AGENTS.md")]
}

fn default_expand_prompt_references() -> bool {
    true
}

const DEFAULT_WORKSPACE_INSTRUCTION_FILE_BYTES: usize = 64 * 1024;
const DEFAULT_PROMPT_REFERENCE_FILE_BYTES: usize = 64 * 1024;
const DEFAULT_PROMPT_REFERENCE_DIRECTORY_ENTRIES: usize = 200;
const DEFAULT_CONTEXT_ATTACHMENT_PAYLOAD_BYTES: usize = 64 * 1024;
const DEFAULT_ASYNC_UPDATE_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentRun {
    pub final_decision: AgentLoopDecision,
    pub stop_reason: AgentStopReason,
    #[serde(default)]
    pub model_metrics: AgentRunModelMetrics,
    #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
    pub run_context: AgentRunContext,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript: Vec<AgentTranscriptItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_turns: Vec<AgentModelTurn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<AgentToolOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<AgentItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_reports: Vec<ContextAssemblyReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_events: Vec<AgentModelEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compactions: Vec<AgentCompactionSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EventEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coding_state: Option<CodingRunState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_evidence_bundle: Option<AgentCandidateEvidenceBundle>,
}

impl AgentRun {
    pub fn tool_pair_integrity(&self) -> AgentToolPairIntegrity {
        AgentToolPairIntegrity::from_items(&self.items)
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentToolPairIntegrity {
    pub tool_calls: usize,
    pub tool_results: usize,
    pub orphan_tool_calls: usize,
    pub orphan_tool_results: usize,
}

impl AgentToolPairIntegrity {
    pub fn from_run(run: &AgentRun) -> Self {
        run.tool_pair_integrity()
    }

    pub fn from_runs(runs: &[AgentRun]) -> Self {
        let mut metrics = Self::default();
        for run in runs {
            metrics.add(run.tool_pair_integrity());
        }
        metrics
    }

    pub fn from_items(items: &[AgentItem]) -> Self {
        let mut call_pairs = BTreeSet::new();
        let mut result_pairs = BTreeSet::new();
        let mut tool_calls = 0usize;
        let mut tool_results = 0usize;
        for item in items {
            match item {
                AgentItem::ToolCall {
                    step_index,
                    call_index,
                    ..
                } => {
                    tool_calls = tool_calls.saturating_add(1);
                    call_pairs.insert((*step_index, *call_index));
                }
                AgentItem::ToolResult {
                    step_index,
                    call_index,
                    ..
                } => {
                    tool_results = tool_results.saturating_add(1);
                    result_pairs.insert((*step_index, *call_index));
                }
                _ => {}
            }
        }
        Self {
            tool_calls,
            tool_results,
            orphan_tool_calls: call_pairs.difference(&result_pairs).count(),
            orphan_tool_results: result_pairs.difference(&call_pairs).count(),
        }
    }

    pub fn add(&mut self, other: Self) {
        self.tool_calls = self.tool_calls.saturating_add(other.tool_calls);
        self.tool_results = self.tool_results.saturating_add(other.tool_results);
        self.orphan_tool_calls = self
            .orphan_tool_calls
            .saturating_add(other.orphan_tool_calls);
        self.orphan_tool_results = self
            .orphan_tool_results
            .saturating_add(other.orphan_tool_results);
    }

    pub fn is_balanced(&self) -> bool {
        self.orphan_tool_calls == 0 && self.orphan_tool_results == 0
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentRunModelMetrics {
    #[serde(default)]
    pub output_format: AgentOutputFormat,
    #[serde(default)]
    pub model_turn_count: usize,
    #[serde(default)]
    pub model_response_count: usize,
    #[serde(default)]
    pub model_response_parse_failure_count: usize,
    #[serde(default)]
    pub strict_json_decision_turn_count: usize,
    #[serde(default)]
    pub text_fallback_turn_count: usize,
    #[serde(default)]
    pub native_tool_call_turn_count: usize,
    #[serde(default)]
    pub native_tool_call_count: usize,
    #[serde(default)]
    pub vendor_fallback_tool_call_turn_count: usize,
    #[serde(default)]
    pub vendor_fallback_tool_call_count: usize,
    #[serde(default)]
    pub repair_attempt_count: usize,
    #[serde(default)]
    pub repair_request_model_visible_bytes: usize,
    #[serde(default)]
    pub reported_usage: AgentRunModelTokenUsage,
    #[serde(default)]
    pub repair_reported_usage: AgentRunModelTokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_decision_source: Option<AgentModelDecisionSource>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRunModelTokenUsage {
    #[serde(default)]
    pub response_count: usize,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum AgentModelDecisionSource {
    #[default]
    StrictJsonDecision,
    TextFallback,
    NativeToolCalls,
    VendorFallbackToolCalls,
}

impl AgentRunModelMetrics {
    fn from_turns(
        output_format: AgentOutputFormat,
        model_turns: &[AgentModelTurn],
        model_events: &[AgentModelEvent],
        repair_request_model_visible_bytes: usize,
    ) -> Self {
        let model_response_parse_failure_count = model_events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    AgentModelEvent::Error { detail, .. }
                        if detail.starts_with("model response parse failed")
                )
            })
            .count();
        let mut metrics = Self {
            output_format,
            model_turn_count: model_turns.len(),
            model_response_count: model_turns.len() + model_response_parse_failure_count,
            model_response_parse_failure_count,
            repair_request_model_visible_bytes,
            reported_usage: Self::reported_usage_from_events(model_events),
            repair_reported_usage: Self::repair_reported_usage_from_turns(model_turns),
            final_decision_source: model_turns.last().map(|turn| turn.decision_source.clone()),
            ..Self::default()
        };
        for turn in model_turns {
            match &turn.decision_source {
                AgentModelDecisionSource::StrictJsonDecision => {
                    metrics.strict_json_decision_turn_count += 1;
                }
                AgentModelDecisionSource::TextFallback => {
                    metrics.text_fallback_turn_count += 1;
                }
                AgentModelDecisionSource::NativeToolCalls => {
                    metrics.native_tool_call_turn_count += 1;
                    metrics.native_tool_call_count += turn.decision.tool_calls.len();
                }
                AgentModelDecisionSource::VendorFallbackToolCalls => {
                    metrics.vendor_fallback_tool_call_turn_count += 1;
                    metrics.vendor_fallback_tool_call_count += turn.decision.tool_calls.len();
                }
            }
        }
        metrics.repair_attempt_count = model_response_parse_failure_count;
        metrics
    }

    fn reported_usage_from_events(model_events: &[AgentModelEvent]) -> AgentRunModelTokenUsage {
        let mut usage = AgentRunModelTokenUsage::default();
        for event in model_events {
            if let AgentModelEvent::Usage {
                usage: Some(model_usage),
                ..
            } = event
            {
                usage.add(model_usage);
            }
        }
        usage
    }

    fn repair_reported_usage_from_turns(model_turns: &[AgentModelTurn]) -> AgentRunModelTokenUsage {
        let mut usage = AgentRunModelTokenUsage::default();
        for turn in model_turns {
            if turn
                .request
                .metadata
                .get("model_response_repair_attempt")
                .is_some()
                && let Some(model_usage) = &turn.response.usage
            {
                usage.add(model_usage);
            }
        }
        usage
    }
}

impl AgentRunModelTokenUsage {
    fn add(&mut self, usage: &sinomo_lm::LanguageModelUsage) {
        self.response_count += 1;
        self.input_tokens = self
            .input_tokens
            .saturating_add(usage.input_tokens.unwrap_or_default());
        self.output_tokens = self
            .output_tokens
            .saturating_add(usage.output_tokens.unwrap_or_default());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentStopReason {
    FinalDecision,
    SilentAsyncUpdate,
    CodingStopRecommended,
    MaxStepsReached,
    Aborted,
    Cancelled,
}

pub struct Agent<M> {
    model: M,
    config: AgentConfig,
    tool_surface: AgentToolSurface,
    tools: AgentToolRegistry,
    context_assembler: ContextAssembler,
    memory_retriever: Option<Arc<dyn MemoryRetriever>>,
    compactor: AgentCompactor,
    compaction_engine: Arc<RwLock<Box<dyn AgentCompactionEngine<M>>>>,
    trace_store: Box<dyn AgentTraceStore>,
    session_store: Box<dyn AgentSessionStore>,
    trace_store_configured: bool,
    session_store_configured: bool,
}

impl<M: InteractionModel> Agent<M> {
    pub fn new(model: M, provider: impl Into<String>, model_id: impl Into<String>) -> Self {
        let provider = provider.into();
        let model_id = model_id.into();
        let config = AgentConfig::new(provider, model_id);
        let tool_surface = AgentToolSurface::for_definition(&config.definition);
        Self {
            model,
            config,
            tool_surface,
            tools: AgentToolRegistry::new(),
            context_assembler: ContextAssembler,
            memory_retriever: None,
            compactor: AgentCompactor::default(),
            compaction_engine: Arc::new(RwLock::new(Box::new(
                CodexStyleAgentCompactionEngine::default(),
            ))),
            trace_store: Box::new(NoopAgentTraceStore),
            session_store: Box::new(NoopAgentSessionStore),
            trace_store_configured: false,
            session_store_configured: false,
        }
    }

    pub fn with_config(mut self, mut config: AgentConfig) -> Self {
        if config.context_budget == AgentContextBudget::default() {
            config.context_budget = AgentContextBudget::for_loaded_model(
                &config.provider,
                &config.model,
                config.max_output_tokens,
            );
        }
        let previous_default_surface = AgentToolSurface::for_definition(&self.config.definition);
        if self.tool_surface == previous_default_surface {
            self.tool_surface = AgentToolSurface::for_definition(&config.definition);
        }
        if self.compactor == AgentCompactor::default() {
            self.compactor.retained_tail_count = config.definition.compaction.retained_tail_items;
        }
        self.config = config;
        self
    }

    pub fn with_agent_definition(mut self, definition: AgentDefinition) -> Self {
        let previous_default_surface = AgentToolSurface::for_definition(&self.config.definition);
        if self.tool_surface == previous_default_surface {
            self.tool_surface = AgentToolSurface::for_definition(&definition);
        }
        if self.compactor == AgentCompactor::default() {
            self.compactor.retained_tail_count = definition.compaction.retained_tail_items;
        }
        self.config = self.config.with_agent_definition(definition);
        self
    }

    pub fn with_tool_registry(mut self, tools: AgentToolRegistry) -> Self {
        self.tools = tools;
        self
    }

    pub fn tool_registry(&self) -> AgentToolRegistry {
        self.tools.clone()
    }

    pub fn load_explicit_skill_context(
        &self,
        skill_id: &str,
    ) -> Result<ExplicitAgentSkillContext, AgentError> {
        let skill_plan = self.skill_plan();
        let skill = skill_plan
            .get(skill_id)
            .ok_or_else(|| AgentError::Runtime {
                detail: format!(
                    "skill `{skill_id}` is not available to agent `{}`",
                    self.config.definition.key()
                ),
            })?;
        let document = standard_skill_document(skill).ok_or_else(|| AgentError::Runtime {
            detail: format!(
                "skill `{skill_id}` does not include a valid readable SKILL.md matching its descriptor"
            ),
        })?;
        Ok(ExplicitAgentSkillContext {
            name: skill.id.clone(),
            description: skill.description.clone(),
            instructions: document.body,
            resources: skill.resource_paths.clone(),
        })
    }

    pub fn register_tool<T: AgentTool + 'static>(&self, tool: T) -> Option<AgentToolDescriptor> {
        self.tools.register(tool)
    }

    pub fn remove_tool(&self, tool_name: &str) -> Option<AgentToolDescriptor> {
        self.tools.remove(tool_name)
    }

    pub fn with_tool_surface(mut self, tool_surface: AgentToolSurface) -> Self {
        self.tool_surface = tool_surface;
        self
    }

    #[deprecated(note = "use with_tool_surface")]
    pub fn with_policy(self, tool_surface: AgentToolSurface) -> Self {
        self.with_tool_surface(tool_surface)
    }

    pub fn with_context_assembler(mut self, context_assembler: ContextAssembler) -> Self {
        self.context_assembler = context_assembler;
        self
    }

    pub fn with_memory_retriever<T>(mut self, retriever: T) -> Self
    where
        T: MemoryRetriever + 'static,
    {
        self.memory_retriever = Some(Arc::new(retriever));
        self
    }

    pub fn with_shared_memory_retriever(mut self, retriever: Arc<dyn MemoryRetriever>) -> Self {
        self.memory_retriever = Some(retriever);
        self
    }

    pub fn with_compactor(mut self, compactor: AgentCompactor) -> Self {
        self.compactor = compactor;
        self
    }

    pub fn with_compaction_engine<E>(mut self, engine: E) -> Self
    where
        E: AgentCompactionEngine<M> + 'static,
    {
        self.compaction_engine = Arc::new(RwLock::new(Box::new(engine)));
        self
    }

    pub fn replace_compaction_engine<E>(&self, engine: E) -> Result<(), AgentError>
    where
        E: AgentCompactionEngine<M> + 'static,
    {
        let mut slot = self
            .compaction_engine
            .write()
            .map_err(|_| AgentError::Runtime {
                detail: "compaction engine lock poisoned".to_string(),
            })?;
        *slot = Box::new(engine);
        Ok(())
    }

    pub fn with_trace_store<T: AgentTraceStore + 'static>(mut self, trace_store: T) -> Self {
        self.trace_store = Box::new(trace_store);
        self.trace_store_configured = true;
        self
    }

    pub fn with_session_store<T: AgentSessionStore + 'static>(mut self, session_store: T) -> Self {
        self.session_store = Box::new(session_store);
        self.session_store_configured = true;
        self
    }
}

fn agent_session_event(
    op_id: &TaskId,
    session_id: &SessionId,
    payload: AgentEventPayload,
) -> AgentEvent {
    AgentEvent::new(op_id.clone(), session_id.clone(), payload)
}

fn turn_completed_agent_event(
    op_id: &TaskId,
    session_id: &SessionId,
    run: &AgentRun,
) -> AgentEvent {
    agent_session_event(
        op_id,
        session_id,
        AgentEventPayload::TurnCompleted {
            stop_reason: run.stop_reason.clone(),
            item_count: run.items.len(),
            event_count: run.events.len(),
            model_turn_count: run.model_turns.len(),
            tool_result_count: run.tool_results.len(),
            run_context: run.run_context.clone(),
            emitted_event_ids: run
                .events
                .iter()
                .map(|event| event.event_id.clone())
                .collect(),
            candidate_evidence_bundle_id: run
                .candidate_evidence_bundle
                .as_ref()
                .map(|bundle| bundle.bundle_id.clone()),
        },
    )
}

fn agent_event(
    session: &InteractionSession,
    kind: &str,
    salience: f32,
    payload: serde_json::Value,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        time::OffsetDateTime::now_utc(),
        session.node_id.clone(),
        Source::new("agent.loop").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(salience).expect("valid salience"),
        Confidence::new(0.95).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session.session_id.clone())
    .with_payload(payload)
}

fn caused_by_latest_event(mut event: EventEnvelope, input: &AgentLoopInput) -> EventEnvelope {
    event
        .links
        .caused_by
        .push(input.latest_event.event_id.clone());
    event
}

struct TruncatedText {
    text: String,
    truncated: bool,
}

fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> TruncatedText {
    let truncated = bytes.len() > max_bytes;
    TruncatedText {
        text: String::from_utf8_lossy(&bytes[..bytes.len().min(max_bytes)]).to_string(),
        truncated,
    }
}

#[cfg(test)]
mod tests;
