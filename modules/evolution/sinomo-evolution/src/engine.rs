//! Core autonomous self-evolution planning and orchestration.
//!
//! This module is the public facade for the current engine API. Domain modules
//! own cohesive policy boundaries: `opportunity` turns telemetry into scoped
//! jobs, while `background` owns cluster/background scheduling and intake.

mod background;
mod opportunity;

pub use background::*;
pub use opportunity::*;

use crate::{
    CandidateApprovalPreflightCheck, CandidateApprovalPreflightReport, CandidateBundleAttestation,
    CandidateBundleContentManifest, CandidateBundleFileDigest, CandidateBundleManifest,
    CandidateBundlePredicate, CandidateBundleSubject, CandidateDecisionOutcome, CommandLogRef,
    EditScope, EvolutionJob, EvolutionJobId, EvolutionJobStatus, EvolutionPaths,
    EvolutionPersistenceError, EvolutionScope, FileExcerpt, ModuleReplayReportRef,
    MutationOutputContract, MutationPacket, RecordedCandidateDecision,
    RecordedCandidateVerification, RecoveryPlan, StoredEvolutionJob, VerificationCommand,
    VerificationPlan, record_candidate_decision_with_preflight, record_candidate_verification,
    write_bundle_manifest, write_stored_job,
};
use serde::{Deserialize, Deserializer, Serialize};
use sinomo_agent::{
    Agent, AgentConfig, AgentContextAttachment, AgentContextBudget, AgentItem, AgentLoopInput,
    AgentModelEvent, AgentOp, AgentRole, AgentSession, AgentSessionRuntime, AgentStoragePaths,
    AgentToolKind, AgentToolPairIntegrity, AgentToolRegistry, AgentToolSpec, AgentToolSurface,
    FileAgentTraceStore, InteractionModel, InteractionSession, InteractionSurface,
    InteractionSurfaceKind, LocalWorkspaceAgentConfig, default_coding_agent_tools,
    user_interaction_input_event,
};
#[cfg(test)]
use sinomo_lm::LanguageModelResponse;
use sinomo_lm::{
    AnthropicConfig, AnthropicProvider, CLAUDE_CODE_DEFAULT_MODEL, CODEX_CLI_DEFAULT_MODEL,
    CODEX_PROVIDER_ID, ClaudeCodeConfig, ClaudeCodeProvider, CodexCliConfig, CodexCliProvider,
    GeminiConfig, GeminiProvider, GitHubCopilotCliConfig, GitHubCopilotCliProvider,
    GitHubCopilotConfig, GitHubCopilotProvider, LanguageModelError, LanguageModelInvocation,
    LanguageModelMessage, LanguageModelProvider, LanguageModelProviderInfo, LanguageModelRequest,
    LanguageModelResponseFormat, LanguageModelRole, NvidiaConfig, NvidiaProvider, OpenAiConfig,
    OpenAiProvider, PROVIDER_DEFAULT_MODEL, codex_provider_for_model, model_or_default,
    normalize_model_id_for_provider, normalize_model_provider,
};
use sinomo_runtime::core::{
    EvaluationGate, EvaluationMetric, EvaluationOutcome, EvaluationScorecard, MetricValue,
    RollbackPolicy, TrustRing,
};
use sinomo_runtime::eval::{CommandRunResult, CommandRunner, ExecutingEvalHarness};
use sinomo_runtime::events::{EventId, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::{LocalNodePermission, LocalNodePermissionStore, NodeId};
use sinomo_runtime::registry::{
    ArtifactEvidenceManifest, ArtifactEvidenceRecord, ArtifactRegistry,
    RELEASE_MATERIAL_RELATIVE_PATH, VerifiedReleaseMaterialRecord, read_release_material_from_path,
    read_trusted_artifact_signers, validate_verified_release_material,
    verify_and_write_verified_release_material, verify_artifact_evidence_integrity,
};
use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use thiserror::Error;
use time::OffsetDateTime;

pub const CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH: &str = "regression-evidence.json";
pub const CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH: &str = "evolution-lesson.json";
pub const CANDIDATE_EVIDENCE_SUMMARY_RELATIVE_PATH: &str = "candidate-evidence-summary.md";
pub const AGENT_EVENTS_RELATIVE_PATH: &str = "agent-events.jsonl";
pub const CODING_RUN_STATE_RELATIVE_PATH: &str = "coding-run-state.json";
pub const PATCH_SNAPSHOTS_RELATIVE_PATH: &str = "patch-snapshots.json";
pub const VALIDATION_RECORDS_RELATIVE_PATH: &str = "validation-records.json";
const SINOMO_BENCHMARK_MODULE_ROOT_ENV: &str = "SINOMO_BENCHMARK_MODULE_ROOT";
const DEFAULT_BENCHMARK_MODULE_ROOT: &str = "modules/benchmark/sinomo-benchmarks";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionRequest {
    pub objective: String,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intended_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_commands: Vec<VerificationCommand>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub benchmark_gates: Vec<BenchmarkGateRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub module_replay_reports: Vec<ModuleReplayReportRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkGateRequest {
    pub name: String,
    #[serde(default)]
    pub expected_exit_code: i32,
    pub kind: BenchmarkGateKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum BenchmarkGateKind {
    SweBench {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        instance_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        provider: String,
        model: String,
        max_steps: usize,
        #[serde(default)]
        evaluate: bool,
    },
    Command {
        command: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary_path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        results_dir: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        progress_path: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkGateReport {
    pub name: String,
    pub command: String,
    pub expected_exit_code: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    pub outcome: EvaluationOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub results_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<EvaluationMetric>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_event_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_last_event: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_verdict: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BenchmarkSummaryVerdict {
    outcome: EvaluationOutcome,
    summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ResourceUsageConfidence {
    Exact,
    ProviderReported,
    Estimated,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateResourceUsageRecord {
    pub resource: String,
    pub units: u64,
    pub unit: String,
    pub confidence: ResourceUsageConfidence,
    pub source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateResourceUsageSummary {
    pub job_id: EvolutionJobId,
    pub generated_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<CandidateResourceUsageRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scheduler_estimates: Vec<ResourceUsageEstimate>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateRegressionEvidenceSummary {
    pub job_id: EvolutionJobId,
    pub objective: String,
    pub target_ring: TrustRing,
    pub outcome: EvaluationOutcome,
    pub bundle_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_gates: Vec<RegressionGateEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_commands: Vec<RegressionCommandFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_benchmarks: Vec<RegressionBenchmarkFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trace_fixtures: Vec<TraceDerivedRegressionFixture>,
    pub replay_minimization: ReplayMinimizationPlan,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reusable_gate_suggestions: Vec<String>,
    pub generated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RegressionGateEvidence {
    pub gate: EvaluationGate,
    pub outcome: EvaluationOutcome,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RegressionCommandFailure {
    pub name: String,
    pub command: String,
    pub expected_exit_code: i32,
    pub exit_code: i32,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stdout_excerpt: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stderr_excerpt: String,
    pub rerun_command: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RegressionBenchmarkFailure {
    pub name: String,
    pub outcome: EvaluationOutcome,
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub results_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_verdict: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TraceDerivedRegressionFixture {
    pub name: String,
    pub source: String,
    pub report_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<String>,
    pub minimization_hint: String,
    pub suggested_gate: EvaluationGate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReplayMinimizationPlan {
    pub status: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub minimization_steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_gate_commands: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateEvolutionLessonRecord {
    pub lesson_id: String,
    pub job_id: EvolutionJobId,
    pub objective: String,
    pub outcome: EvaluationOutcome,
    pub failure_class: EvolutionFailureClass,
    pub root_cause_summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_assumptions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rollback_evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggested_follow_up_tasks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reusable_evidence_refs: Vec<String>,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CampaignProposalRequest {
    pub objective: String,
    pub horizon: String,
    pub budget_policy: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserve_floors: Vec<ReserveFloor>,
    pub approval_policy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_time: Option<String>,
    #[serde(default)]
    pub pause_after_proposal: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub approved: bool,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_repo: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CampaignProposalInput {
    pub objective: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub horizon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserve_floor_specs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_time: Option<String>,
    #[serde(default)]
    pub pause_after_proposal: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub approved: bool,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_repo: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionRequestInput {
    pub objective: String,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intended_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_commands: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub benchmark_specs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub module_replay_report_specs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PreparedEvolutionRequest {
    pub request: EvolutionRequest,
    pub provider: String,
    pub model: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionCampaign {
    pub campaign_id: String,
    pub status: EvolutionCampaignStatus,
    pub objective_card: ObjectiveCard,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<EvolutionStage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub near_term_tasks: Vec<NearTermEvolutionTask>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub iteration_results: Vec<EvolutionIterationResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decisions: Vec<CampaignDecision>,
    pub budget_policy: CampaignBudgetPolicy,
    pub resource_schedule_preview: ResourceSchedulePreview,
    pub approval_checkpoint: ApprovalCheckpoint,
    pub target_ring: TrustRing,
    pub dry_run: bool,
    pub capability_registry_required: Option<bool>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_repo: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum EvolutionCampaignStatus {
    Draft,
    AwaitingApproval,
    Approved,
    Scheduled,
    Running,
    PausedBudget,
    PausedPolicy,
    PausedHuman,
    StageComplete,
    Complete,
    Cancelled,
    Superseded,
    FailedExhausted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectiveCard {
    pub objective_id: String,
    pub human_text: String,
    pub interpreted_goal: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_goals: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assumptions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub success_metrics: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failure_conditions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub safety_notes: Vec<String>,
    pub resource_policy: String,
    pub approval_policy: String,
    pub initial_horizon: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assumptions_needing_human_review: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionStage {
    pub stage_id: String,
    pub name: String,
    pub purpose: String,
    pub baseline: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acceptance_metrics: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regression_gates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_artifacts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_task_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eval_suite: Vec<String>,
    pub estimated_budget: String,
    pub promotion_policy: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failure_conditions: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NearTermEvolutionTask {
    pub task_id: String,
    pub stage_id: String,
    pub title: String,
    pub objective: String,
    pub rationale: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_commands: Vec<String>,
    pub estimated_budget: String,
    pub approval_required: bool,
    pub expected_result_kind: EvolutionResultKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum EvolutionResultKind {
    Patch,
    Artifact,
    Evaluator,
    Instrumentation,
    FoundationRepair,
    TaskReplacementProposal,
    Documentation,
    RolloutGuard,
    Other(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionIterationResult {
    pub iteration_id: String,
    pub task_id: String,
    pub result_kind: EvolutionResultKind,
    pub status: EvolutionIterationStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_usage: Vec<ResourceUsageEstimate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum EvolutionIterationStatus {
    Proposed,
    ResourceLeaseRequested,
    ResourceLeaseGranted,
    AgentRunning,
    CandidateProduced,
    Evaluating,
    AwaitingRolloutApproval,
    RollingOut,
    RolledOut,
    Rejected,
    RolledBack,
    Adapted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CampaignDecision {
    pub decision_id: String,
    pub kind: CampaignDecisionKind,
    pub summary: String,
    pub rationale: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub decided_by: String,
    pub decided_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum CampaignDecisionKind {
    ProposalCreated,
    Approved,
    Rejected,
    Edited,
    TaskSelected,
    ResourcePolicySelected,
    Paused,
    Resumed,
    StagePromoted,
    DirectionChanged,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CampaignBudgetPolicy {
    pub policy_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserve_floors: Vec<ReserveFloor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scheduling_notes: Vec<String>,
    pub confidence: String,
    pub debt_note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReserveFloor {
    pub resource: String,
    pub floor_percent: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceSchedulePreview {
    pub horizon: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<ResourceScheduleEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceScheduleEntry {
    pub resource: String,
    pub window: String,
    pub planned_use: String,
    pub reserve_floor: String,
    pub notes: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ApprovalCheckpoint {
    pub policy: String,
    pub status: ApprovalCheckpointStatus,
    pub required_before: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub editable_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub possible_actions: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ApprovalCheckpointStatus {
    AwaitingHuman,
    Approved,
    Rejected,
    Edited,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceSchedulerDryRunRequest {
    pub campaign: EvolutionCampaign,
    pub preview_horizon: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ResourceClassDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_estimates: Vec<ResourceUsageEstimate>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceClassDefinition {
    pub resource_id: String,
    pub unit: String,
    pub total_units: u64,
    pub used_units: u64,
    pub reserve_floor_percent: u8,
    pub reset_window: String,
    pub notes: String,
}

pub fn resource_class_from_reserve_floor(
    floor: &ReserveFloor,
    notes: &str,
) -> ResourceClassDefinition {
    ResourceClassDefinition {
        resource_id: floor.resource.clone(),
        unit: "percent".to_string(),
        total_units: 100,
        used_units: 0,
        reserve_floor_percent: floor.floor_percent,
        reset_window: "current reset window".to_string(),
        notes: notes.to_string(),
    }
}

pub fn resource_classes_from_reserve_floors(
    floors: &[ReserveFloor],
    notes: &str,
) -> Vec<ResourceClassDefinition> {
    floors
        .iter()
        .map(|floor| resource_class_from_reserve_floor(floor, notes))
        .collect()
}

pub fn prepare_campaign_proposal_request(
    input: CampaignProposalInput,
) -> Result<CampaignProposalRequest, EvolutionInputParseError> {
    Ok(CampaignProposalRequest {
        objective: input.objective,
        horizon: input.horizon.unwrap_or_else(|| "30 days".to_string()),
        budget_policy: input
            .budget_policy
            .unwrap_or_else(|| "free-and-prepaid".to_string()),
        reserve_floors: parse_reserve_floors(&input.reserve_floor_specs)?,
        approval_policy: input
            .approval_policy
            .unwrap_or_else(|| "stage-rollout".to_string()),
        max_wall_time: input.max_wall_time,
        pause_after_proposal: input.pause_after_proposal,
        dry_run: input.dry_run,
        approved: input.approved,
        target_ring: input.target_ring,
        source_repo: input.source_repo,
    })
}

pub fn prepare_evolution_request(
    input: EvolutionRequestInput,
) -> Result<PreparedEvolutionRequest, EvolutionInputParseError> {
    let provider = input
        .provider
        .or(input.default_provider)
        .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
    let provider =
        normalize_model_provider(&provider).ok_or_else(|| EvolutionInputParseError::Invalid {
            detail: format!("unknown provider `{provider}`"),
        })?;
    let model = input
        .model
        .or(input.default_model)
        .map(|model| normalize_model_id_for_provider(&provider, &model));
    let verification_commands = input
        .verification_commands
        .into_iter()
        .enumerate()
        .map(|(index, command)| VerificationCommand {
            name: format!("verify-{}", index + 1),
            command,
            expected_exit_code: 0,
            gate: EvaluationGate::StaticChecks,
        })
        .collect();
    let benchmark_gates = parse_benchmark_gate_specs(
        &input.benchmark_specs,
        &provider,
        model.as_deref().unwrap_or(PROVIDER_DEFAULT_MODEL),
    )?;
    let module_replay_reports =
        parse_module_replay_report_specs(&input.module_replay_report_specs)?;
    let request = EvolutionRequest {
        objective: input.objective,
        target_ring: input.target_ring,
        intended_paths: input.intended_paths,
        verification_commands,
        benchmark_gates,
        module_replay_reports,
        model_provider: Some(provider.clone()),
        model: model.clone(),
        dry_run: input.dry_run,
    };

    Ok(PreparedEvolutionRequest {
        request,
        provider,
        model: model.unwrap_or_else(|| PROVIDER_DEFAULT_MODEL.to_string()),
    })
}

pub fn parse_module_replay_report_specs(
    specs: &[String],
) -> Result<Vec<ModuleReplayReportRef>, EvolutionInputParseError> {
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| parse_module_replay_report_spec(index + 1, spec))
        .collect()
}

fn parse_module_replay_report_spec(
    index: usize,
    spec: &str,
) -> Result<ModuleReplayReportRef, EvolutionInputParseError> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err(EvolutionInputParseError::Invalid {
            detail: "module replay report spec cannot be empty".to_string(),
        });
    }
    let (name, report_path) = if let Some((name, report_path)) = trimmed.split_once('=') {
        let name = name.trim();
        let report_path = report_path.trim();
        if name.is_empty() || report_path.is_empty() {
            return Err(EvolutionInputParseError::Invalid {
                detail: format!(
                    "module replay report spec `{spec}` must look like `name=path` or `path`"
                ),
            });
        }
        (name.to_string(), report_path.to_string())
    } else {
        (format!("module-replay-{index}"), trimmed.to_string())
    };
    Ok(ModuleReplayReportRef {
        name,
        report_path,
        baseline_artifact_id: None,
        candidate_artifact_id: None,
    })
}

pub fn parse_benchmark_gate_specs(
    specs: &[String],
    provider: &str,
    model: &str,
) -> Result<Vec<BenchmarkGateRequest>, EvolutionInputParseError> {
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| parse_benchmark_gate_spec(index + 1, spec, provider, model))
        .collect()
}

fn parse_benchmark_gate_spec(
    index: usize,
    spec: &str,
    provider: &str,
    model: &str,
) -> Result<BenchmarkGateRequest, EvolutionInputParseError> {
    if spec == "swebench" {
        return Ok(BenchmarkGateRequest {
            name: format!("benchmark-swebench-{index}"),
            expected_exit_code: 0,
            kind: BenchmarkGateKind::SweBench {
                instance_id: None,
                limit: Some(1),
                provider: provider.to_string(),
                model: model.to_string(),
                max_steps: 20,
                evaluate: true,
            },
        });
    }
    if let Some(value) = spec.strip_prefix("swebench:") {
        let (instance_id, limit) = if let Some(limit) = value.strip_prefix("limit=") {
            let limit =
                limit
                    .parse::<usize>()
                    .map_err(|source| EvolutionInputParseError::Invalid {
                        detail: format!("invalid SWE-bench limit `{limit}`: {source}"),
                    })?;
            if limit == 0 {
                return Err(EvolutionInputParseError::Invalid {
                    detail: "SWE-bench limit must be at least 1".to_string(),
                });
            }
            (None, Some(limit))
        } else if value.trim().is_empty() {
            (None, Some(1))
        } else {
            (Some(value.to_string()), None)
        };
        return Ok(BenchmarkGateRequest {
            name: format!("benchmark-swebench-{index}"),
            expected_exit_code: 0,
            kind: BenchmarkGateKind::SweBench {
                instance_id,
                limit,
                provider: provider.to_string(),
                model: model.to_string(),
                max_steps: 20,
                evaluate: true,
            },
        });
    }
    if let Some(command) = spec.strip_prefix("command:") {
        let command = command.trim();
        if command.is_empty() {
            return Err(EvolutionInputParseError::Invalid {
                detail: "benchmark command spec cannot be empty".to_string(),
            });
        }
        return Ok(BenchmarkGateRequest {
            name: format!("benchmark-command-{index}"),
            expected_exit_code: 0,
            kind: BenchmarkGateKind::Command {
                command: command.to_string(),
                summary_path: None,
                results_dir: None,
                progress_path: None,
            },
        });
    }
    Err(EvolutionInputParseError::Invalid {
        detail: format!(
            "unsupported benchmark spec `{spec}`; expected `swebench`, `swebench:<instance_id>`, `swebench:limit=N`, or `command:<shell command>`"
        ),
    })
}

pub fn parse_reserve_floors(
    values: &[String],
) -> Result<Vec<ReserveFloor>, EvolutionInputParseError> {
    values
        .iter()
        .map(|value| parse_reserve_floor(value))
        .collect()
}

pub fn parse_reserve_floor(value: &str) -> Result<ReserveFloor, EvolutionInputParseError> {
    let (resource, floor) =
        value
            .rsplit_once(':')
            .ok_or_else(|| EvolutionInputParseError::Invalid {
                detail: format!("reserve floor `{value}` must look like resource:30%"),
            })?;
    let floor = floor
        .trim_end_matches('%')
        .parse::<u8>()
        .map_err(|source| EvolutionInputParseError::Invalid {
            detail: format!("invalid reserve floor `{value}`: {source}"),
        })?;
    if floor > 100 || resource.trim().is_empty() {
        return Err(EvolutionInputParseError::Invalid {
            detail: format!(
                "reserve floor `{value}` must have a non-empty resource and 0-100 percent"
            ),
        });
    }
    Ok(ReserveFloor {
        resource: resource.to_string(),
        floor_percent: floor,
    })
}

pub fn parse_failure_class(value: &str) -> Result<EvolutionFailureClass, EvolutionInputParseError> {
    match value {
        "build-test-failure" => Ok(EvolutionFailureClass::BuildTestFailure),
        "eval-regression" => Ok(EvolutionFailureClass::EvalRegression),
        "artifact-quality-failure" => Ok(EvolutionFailureClass::ArtifactQualityFailure),
        "rollout-failure" => Ok(EvolutionFailureClass::RolloutFailure),
        "resource-exhaustion" => Ok(EvolutionFailureClass::ResourceExhaustion),
        "repeated-implementation-failure" => {
            Ok(EvolutionFailureClass::RepeatedImplementationFailure)
        }
        "foundation-blocker" => Ok(EvolutionFailureClass::FoundationBlocker),
        "ambiguous-metric" => Ok(EvolutionFailureClass::AmbiguousMetric),
        "wrong-task-selection" => Ok(EvolutionFailureClass::WrongTaskSelection),
        "inconclusive-evidence" => Ok(EvolutionFailureClass::InconclusiveEvidence),
        _ => Err(EvolutionInputParseError::Invalid {
            detail: format!("unknown failure class `{value}`"),
        }),
    }
}

pub fn parse_stage_resource_usage(
    value: &str,
    default_task_id: Option<&str>,
) -> Result<ResourceUsageEstimate, EvolutionInputParseError> {
    let parts: Vec<&str> = value.split(':').collect();
    let (task_id, resource, estimated, actual) = match parts.as_slice() {
        [resource, estimated] => (
            default_task_id.ok_or_else(|| EvolutionInputParseError::Invalid {
                detail: format!(
                    "usage `{value}` needs --task first or task:resource:estimated[:actual]"
                ),
            })?,
            resource.trim(),
            estimated.trim(),
            None,
        ),
        [resource, estimated, actual] => (
            default_task_id.ok_or_else(|| EvolutionInputParseError::Invalid {
                detail: format!(
                    "usage `{value}` needs --task first or task:resource:estimated[:actual]"
                ),
            })?,
            resource.trim(),
            estimated.trim(),
            Some(actual.trim()),
        ),
        [task_id, resource, estimated, actual] => (
            task_id.trim(),
            resource.trim(),
            estimated.trim(),
            Some(actual.trim()),
        ),
        _ => {
            return Err(EvolutionInputParseError::Invalid {
                detail: format!(
                    "usage `{value}` must look like resource:estimated[:actual] or task:resource:estimated:actual"
                ),
            });
        }
    };
    let estimated_units =
        estimated
            .parse::<u64>()
            .map_err(|source| EvolutionInputParseError::Invalid {
                detail: format!("invalid estimated usage units in `{value}`: {source}"),
            })?;
    let actual_units = actual
        .map(|actual| {
            actual
                .parse::<u64>()
                .map_err(|source| EvolutionInputParseError::Invalid {
                    detail: format!("invalid actual usage units in `{value}`: {source}"),
                })
        })
        .transpose()?;
    Ok(ResourceUsageEstimate {
        task_id: task_id.to_string(),
        resource: resource.to_string(),
        estimated_units,
        actual_units,
    })
}

pub fn parse_schedule_resources(
    values: &[String],
    campaign: &EvolutionCampaign,
) -> Result<Vec<ResourceClassDefinition>, EvolutionInputParseError> {
    if values.is_empty() {
        return Ok(resource_classes_from_reserve_floors(
            &campaign.budget_policy.reserve_floors,
            "derived from campaign reserve-floor config",
        ));
    }

    values
        .iter()
        .map(|value| parse_schedule_resource(value, campaign))
        .collect()
}

pub fn parse_schedule_resource(
    value: &str,
    campaign: &EvolutionCampaign,
) -> Result<ResourceClassDefinition, EvolutionInputParseError> {
    let mut parts = value.split(':');
    let resource_id = parts.next().unwrap_or_default().trim();
    let usage = parts
        .next()
        .ok_or_else(|| EvolutionInputParseError::Invalid {
            detail: format!("resource `{value}` must look like resource:used/total[:reset-window]"),
        })?;
    let reset_window = parts.next().unwrap_or("current reset window");
    if parts.next().is_some() || resource_id.is_empty() {
        return Err(EvolutionInputParseError::Invalid {
            detail: format!("resource `{value}` must look like resource:used/total[:reset-window]"),
        });
    }
    let (used, total) = usage
        .split_once('/')
        .ok_or_else(|| EvolutionInputParseError::Invalid {
            detail: format!("resource `{value}` must include used/total units"),
        })?;
    let used_units = used
        .parse::<u64>()
        .map_err(|source| EvolutionInputParseError::Invalid {
            detail: format!("invalid used units in resource `{value}`: {source}"),
        })?;
    let total_units = total
        .parse::<u64>()
        .map_err(|source| EvolutionInputParseError::Invalid {
            detail: format!("invalid total units in resource `{value}`: {source}"),
        })?;
    if total_units == 0 || used_units > total_units {
        return Err(EvolutionInputParseError::Invalid {
            detail: format!("resource `{value}` must have 0 <= used <= total and total > 0"),
        });
    }
    let reserve_floor_percent = campaign
        .budget_policy
        .reserve_floors
        .iter()
        .find(|floor| floor.resource == resource_id)
        .map(|floor| floor.floor_percent)
        .unwrap_or(0);
    Ok(ResourceClassDefinition {
        resource_id: resource_id.to_string(),
        unit: "percent".to_string(),
        total_units,
        used_units,
        reserve_floor_percent,
        reset_window: reset_window.to_string(),
        notes: "provided by schedule dry-run command".to_string(),
    })
}

pub fn parse_schedule_estimates(
    values: &[String],
    campaign: &EvolutionCampaign,
    resources: &[ResourceClassDefinition],
) -> Result<Vec<ResourceUsageEstimate>, EvolutionInputParseError> {
    if values.is_empty() {
        return Ok(default_resource_usage_estimates(campaign, resources));
    }

    values
        .iter()
        .map(|value| parse_schedule_estimate(value, campaign))
        .collect()
}

pub fn parse_schedule_estimate(
    value: &str,
    campaign: &EvolutionCampaign,
) -> Result<ResourceUsageEstimate, EvolutionInputParseError> {
    let parts: Vec<&str> = value.split(':').collect();
    let (task_id, resource, units) = match parts.as_slice() {
        [resource, units] => {
            let Some(task) = campaign.near_term_tasks.first() else {
                return Err(EvolutionInputParseError::Invalid {
                    detail: "cannot infer task id because campaign has no near-term tasks"
                        .to_string(),
                });
            };
            (task.task_id.clone(), resource.trim(), units.trim())
        }
        [task_id, resource, units] => (task_id.trim().to_string(), resource.trim(), units.trim()),
        _ => {
            return Err(EvolutionInputParseError::Invalid {
                detail: format!(
                    "estimate `{value}` must look like resource:units or task:resource:units"
                ),
            });
        }
    };
    if resource.is_empty() || units.is_empty() {
        return Err(EvolutionInputParseError::Invalid {
            detail: format!("estimate `{value}` must include resource and units"),
        });
    }
    let estimated_units =
        units
            .parse::<u64>()
            .map_err(|source| EvolutionInputParseError::Invalid {
                detail: format!("invalid estimated units in `{value}`: {source}"),
            })?;
    Ok(ResourceUsageEstimate {
        task_id,
        resource: resource.to_string(),
        estimated_units,
        actual_units: None,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceUsageEstimate {
    pub task_id: String,
    pub resource: String,
    pub estimated_units: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_units: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceSchedulerDryRun {
    pub campaign_id: String,
    pub aggregate_decision: CampaignScheduleDecision,
    pub preview_horizon: String,
    pub current_reset_window: String,
    pub resource_ledger: ResourceLedger,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lease_requests: Vec<ResourceLeaseRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lease_decisions: Vec<ResourceLeaseDecision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_decisions: Vec<TaskScheduleDecision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub next_24h_preview: Vec<Next24hScheduleEntry>,
    pub generated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ScheduledIterationRequest {
    pub campaign: EvolutionCampaign,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intended_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_verification_commands: Vec<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ScheduledIterationPlan {
    pub campaign_id: String,
    pub iteration_id: String,
    pub task: NearTermEvolutionTask,
    pub evolution_request: EvolutionRequest,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memory_context: Vec<EvolutionIterationMemoryContext>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operator_notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionIterationMemoryContext {
    pub memory_id: String,
    pub source: EvolutionIterationMemorySource,
    pub campaign_id: String,
    pub stage_id: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<EvaluationGate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_evidence_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum EvolutionIterationMemorySource {
    Lesson,
    ProposedRegressionGate,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StageEvaluationRequest {
    pub campaign: EvolutionCampaign,
    pub task_id: String,
    pub report: EvolutionReport,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_usage: Vec<ResourceUsageEstimate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<EvolutionFailureClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_cause_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub what_was_learned: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StageEvaluationUpdate {
    pub campaign: EvolutionCampaign,
    pub stage_scorecard: CampaignStageScorecard,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adaptation: Option<CampaignAdaptationRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CampaignStageScorecard {
    pub scorecard_id: String,
    pub campaign_id: String,
    pub stage_id: String,
    pub task_id: String,
    pub iteration_id: String,
    pub outcome: EvaluationOutcome,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declared_metrics: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub met_metrics: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_metrics: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regression_gates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub promotion: StagePromotionDecision,
    pub summary: String,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum StagePromotionDecision {
    Promote,
    HoldFailed,
    HoldInconclusive,
    HoldHumanCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum EvolutionFailureClass {
    BuildTestFailure,
    EvalRegression,
    ArtifactQualityFailure,
    RolloutFailure,
    ResourceExhaustion,
    RepeatedImplementationFailure,
    FoundationBlocker,
    AmbiguousMetric,
    WrongTaskSelection,
    InconclusiveEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CampaignAdaptationRecord {
    pub adaptation_id: String,
    pub campaign_id: String,
    pub stage_id: String,
    pub failed_iteration_id: String,
    pub failed_task_id: String,
    pub failure_class: EvolutionFailureClass,
    pub root_cause_summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub what_was_learned: Vec<String>,
    pub next_task_simplification: String,
    pub new_eval_or_diagnostic: String,
    pub foundation_issue_detected: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proposed_regression_gates: Vec<CampaignProposedRegressionGate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_task_candidate: Option<NearTermEvolutionTask>,
    pub budget_adjustment: String,
    pub human_attention_needed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CampaignProposedRegressionGate {
    pub gate_id: String,
    pub gate: EvaluationGate,
    pub source: String,
    pub evidence_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    pub rationale: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionLessonMemoryRecord {
    pub memory_id: String,
    pub campaign_id: String,
    pub adaptation_id: String,
    pub stage_id: String,
    pub failed_iteration_id: String,
    pub failed_task_id: String,
    pub failure_class: EvolutionFailureClass,
    pub root_cause_summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub what_was_learned: Vec<String>,
    pub next_task_simplification: String,
    pub new_eval_or_diagnostic: String,
    pub foundation_issue_detected: bool,
    pub budget_adjustment: String,
    pub human_attention_needed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_task_candidate: Option<NearTermEvolutionTask>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proposed_regression_gate_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub campaign_bundle_path: String,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionRegressionGateMemoryRecord {
    pub memory_id: String,
    pub campaign_id: String,
    pub adaptation_id: String,
    pub stage_id: String,
    pub failed_task_id: String,
    pub status: String,
    pub proposed_gate: CampaignProposedRegressionGate,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub campaign_bundle_path: String,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CampaignTaskReplacementProposal {
    pub title: String,
    pub objective: String,
    pub rationale: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_commands: Vec<String>,
    pub expected_result_kind: EvolutionResultKind,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CampaignIterationError {
    #[error("campaign `{campaign_id}` is not approved for scheduled iteration")]
    CampaignNotApproved { campaign_id: String },
    #[error("campaign `{campaign_id}` has no near-term tasks")]
    NoNearTermTasks { campaign_id: String },
    #[error("task `{task_id}` was not found in campaign `{campaign_id}`")]
    TaskNotFound {
        campaign_id: String,
        task_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum CampaignScheduleDecision {
    Scheduled,
    Delayed,
    PausedBudget,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum CampaignCheckpointDecision {
    Approve,
    Reject,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceLedger {
    pub ledger_id: String,
    pub campaign_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ResourceClassDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub usage_records: Vec<ResourceUsageRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lease_records: Vec<ResourceLeaseDecision>,
    pub generated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceUsageRecord {
    pub task_id: String,
    pub resource: String,
    pub estimated_units: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_units: Option<u64>,
    pub reset_window: String,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceLeaseRequest {
    pub request_id: String,
    pub campaign_id: String,
    pub task_id: String,
    pub resource: String,
    pub estimated_units: u64,
    pub requested_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResourceLeaseDecision {
    pub decision_id: String,
    pub request_id: String,
    pub resource: String,
    pub outcome: ResourceLeaseOutcome,
    pub reason: String,
    pub reserve_floor_percent: u8,
    pub used_before_units: u64,
    pub estimated_units: u64,
    pub used_after_units: u64,
    pub total_units: u64,
    pub decided_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ResourceLeaseOutcome {
    Granted,
    Denied,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TaskScheduleDecision {
    pub task_id: String,
    pub decision: TaskScheduleDecisionKind,
    pub resource: String,
    pub explanation: String,
    pub lease_decision_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum TaskScheduleDecisionKind {
    RunNow,
    Delayed,
    PausedBudget,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Next24hScheduleEntry {
    pub task_id: String,
    pub resource: String,
    pub reset_window: String,
    pub planned_start: String,
    pub decision: TaskScheduleDecisionKind,
    pub explanation: String,
}

pub fn dry_run_resource_scheduler(
    request: ResourceSchedulerDryRunRequest,
) -> ResourceSchedulerDryRun {
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let resources = if request.resources.is_empty() {
        default_resource_classes(&request.campaign)
    } else {
        request.resources
    };
    let task_estimates = if request.task_estimates.is_empty() {
        default_resource_usage_estimates(&request.campaign, &resources)
    } else {
        request.task_estimates
    };
    let current_reset_window = resources
        .first()
        .map(|resource| resource.reset_window.clone())
        .unwrap_or_else(|| "no reset window configured".to_string());
    let mut planned_usage: Vec<(String, u64)> = resources
        .iter()
        .map(|resource| (resource.resource_id.clone(), resource.used_units))
        .collect();
    let mut lease_requests = Vec::new();
    let mut lease_decisions = Vec::new();
    let mut task_decisions = Vec::new();
    let mut next_24h_preview = Vec::new();
    let mut usage_records = Vec::new();

    for estimate in task_estimates {
        let resource = resource_for_estimate(&resources, &estimate.resource);
        let used_before = planned_usage
            .iter()
            .find(|(resource_id, _)| resource_id == &resource.resource_id)
            .map(|(_, used)| *used)
            .unwrap_or(resource.used_units);
        let used_after = used_before.saturating_add(estimate.estimated_units);
        let reserve_floor_units =
            reserve_floor_units(resource.total_units, resource.reserve_floor_percent);
        let spendable_ceiling = resource.total_units.saturating_sub(reserve_floor_units);
        let request_id = generated_id("lease_req");
        let lease_request = ResourceLeaseRequest {
            request_id: request_id.clone(),
            campaign_id: request.campaign.campaign_id.clone(),
            task_id: estimate.task_id.clone(),
            resource: resource.resource_id.clone(),
            estimated_units: estimate.estimated_units,
            requested_at: now.clone(),
        };
        let (outcome, task_decision, reason) = if used_after <= spendable_ceiling {
            if let Some((_, used)) = planned_usage
                .iter_mut()
                .find(|(resource_id, _)| resource_id == &resource.resource_id)
            {
                *used = used_after;
            }
            (
                ResourceLeaseOutcome::Granted,
                TaskScheduleDecisionKind::RunNow,
                format!(
                    "grant: estimated use {} {} keeps {} at {}/{}; reserve floor remains protected at {}%",
                    estimate.estimated_units,
                    resource.unit,
                    resource.resource_id,
                    used_after,
                    resource.total_units,
                    resource.reserve_floor_percent
                ),
            )
        } else {
            (
                ResourceLeaseOutcome::Denied,
                TaskScheduleDecisionKind::PausedBudget,
                format!(
                    "deny: estimated use {} {} would move {} from {}/{} to {}/{} and violate reserve floor {}%",
                    estimate.estimated_units,
                    resource.unit,
                    resource.resource_id,
                    used_before,
                    resource.total_units,
                    used_after,
                    resource.total_units,
                    resource.reserve_floor_percent
                ),
            )
        };
        let decision_id = generated_id("lease_decision");
        let lease_decision = ResourceLeaseDecision {
            decision_id: decision_id.clone(),
            request_id,
            resource: resource.resource_id.clone(),
            outcome,
            reason: reason.clone(),
            reserve_floor_percent: resource.reserve_floor_percent,
            used_before_units: used_before,
            estimated_units: estimate.estimated_units,
            used_after_units: used_after,
            total_units: resource.total_units,
            decided_at: now.clone(),
        };
        usage_records.push(ResourceUsageRecord {
            task_id: estimate.task_id.clone(),
            resource: resource.resource_id.clone(),
            estimated_units: estimate.estimated_units,
            actual_units: estimate.actual_units,
            reset_window: resource.reset_window.clone(),
            recorded_at: now.clone(),
        });
        task_decisions.push(TaskScheduleDecision {
            task_id: estimate.task_id.clone(),
            decision: task_decision.clone(),
            resource: resource.resource_id.clone(),
            explanation: reason.clone(),
            lease_decision_id: decision_id,
        });
        next_24h_preview.push(Next24hScheduleEntry {
            task_id: estimate.task_id,
            resource: resource.resource_id.clone(),
            reset_window: resource.reset_window.clone(),
            planned_start: if task_decision == TaskScheduleDecisionKind::RunNow {
                "now".to_string()
            } else {
                "after reset or human budget review".to_string()
            },
            decision: task_decision,
            explanation: reason,
        });
        lease_requests.push(lease_request);
        lease_decisions.push(lease_decision);
    }

    let aggregate_decision = if lease_decisions
        .iter()
        .any(|decision| decision.outcome == ResourceLeaseOutcome::Denied)
    {
        CampaignScheduleDecision::PausedBudget
    } else if lease_decisions.is_empty() {
        CampaignScheduleDecision::Delayed
    } else {
        CampaignScheduleDecision::Scheduled
    };
    let resource_ledger = ResourceLedger {
        ledger_id: generated_id("ledger"),
        campaign_id: request.campaign.campaign_id.clone(),
        resources,
        usage_records,
        lease_records: lease_decisions.clone(),
        generated_at: now.clone(),
    };

    ResourceSchedulerDryRun {
        campaign_id: request.campaign.campaign_id,
        aggregate_decision,
        preview_horizon: request.preview_horizon,
        current_reset_window,
        resource_ledger,
        lease_requests,
        lease_decisions,
        task_decisions,
        next_24h_preview,
        generated_at: now,
    }
}

pub fn candidate_resource_usage_estimates_for_report(
    campaign: &EvolutionCampaign,
    task_id: &str,
    report: &EvolutionReport,
) -> Result<Vec<ResourceUsageEstimate>, EvolutionInputParseError> {
    let Some(path) = &report.bundle.resource_usage_path else {
        return Ok(Vec::new());
    };
    let content = fs::read_to_string(path).map_err(|source| EvolutionInputParseError::Invalid {
        detail: format!("could not read candidate resource usage `{path}`: {source}"),
    })?;
    let summary =
        serde_json::from_str::<CandidateResourceUsageSummary>(&content).map_err(|source| {
            EvolutionInputParseError::Invalid {
                detail: format!("could not parse candidate resource usage `{path}`: {source}"),
            }
        })?;
    Ok(candidate_resource_usage_estimates(
        campaign, task_id, &summary,
    ))
}

pub fn candidate_resource_usage_estimates(
    campaign: &EvolutionCampaign,
    task_id: &str,
    summary: &CandidateResourceUsageSummary,
) -> Vec<ResourceUsageEstimate> {
    if !summary.scheduler_estimates.is_empty() {
        return summary
            .scheduler_estimates
            .iter()
            .cloned()
            .map(|mut estimate| {
                estimate.task_id = task_id.to_string();
                estimate
            })
            .collect();
    }
    if summary.records.is_empty() {
        return Vec::new();
    }
    let resource = campaign
        .budget_policy
        .reserve_floors
        .first()
        .map(|floor| floor.resource.clone())
        .unwrap_or_else(|| "local-agent-wall-time".to_string());
    let estimated_units = campaign
        .near_term_tasks
        .iter()
        .find(|task| task.task_id == task_id)
        .map(|_| 10)
        .unwrap_or(10);
    vec![ResourceUsageEstimate {
        task_id: task_id.to_string(),
        resource,
        estimated_units,
        actual_units: Some(candidate_scheduler_usage_units(summary)),
    }]
}

fn candidate_scheduler_usage_units(summary: &CandidateResourceUsageSummary) -> u64 {
    let input_tokens = usage_record_units(summary, "agent-input-tokens");
    let output_tokens = usage_record_units(summary, "agent-output-tokens");
    let token_units = input_tokens
        .saturating_add(output_tokens)
        .saturating_add(999)
        / 1000;
    let model_turn_units = usage_record_units(summary, "agent-model-turns");
    let local_minutes =
        usage_record_units(summary, "local-compute-ms").saturating_add(59_999) / 60_000;
    token_units
        .saturating_add(model_turn_units)
        .saturating_add(local_minutes)
        .max(1)
}

fn usage_record_units(summary: &CandidateResourceUsageSummary, resource: &str) -> u64 {
    summary
        .records
        .iter()
        .filter(|record| record.resource == resource)
        .map(|record| record.units)
        .sum()
}

#[derive(Debug, Error)]
pub enum EvolutionInputParseError {
    #[error("{detail}")]
    Invalid { detail: String },
}

fn default_resource_classes(campaign: &EvolutionCampaign) -> Vec<ResourceClassDefinition> {
    if campaign.budget_policy.reserve_floors.is_empty() {
        return vec![ResourceClassDefinition {
            resource_id: "local-agent-wall-time".to_string(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 0,
            reserve_floor_percent: 0,
            reset_window: "current 24h preview window".to_string(),
            notes:
                "default dry-run resource; replace with configured resource classes before dispatch"
                    .to_string(),
        }];
    }
    resource_classes_from_reserve_floors(
        &campaign.budget_policy.reserve_floors,
        "derived from campaign reserve-floor config",
    )
}

pub fn default_resource_usage_estimates(
    campaign: &EvolutionCampaign,
    resources: &[ResourceClassDefinition],
) -> Vec<ResourceUsageEstimate> {
    let Some(task) = campaign.near_term_tasks.first() else {
        return Vec::new();
    };
    let resource = resources
        .first()
        .map(|resource| resource.resource_id.clone())
        .unwrap_or_else(|| "local-agent-wall-time".to_string());
    vec![ResourceUsageEstimate {
        task_id: task.task_id.clone(),
        resource,
        estimated_units: 10,
        actual_units: None,
    }]
}

fn resource_for_estimate<'a>(
    resources: &'a [ResourceClassDefinition],
    resource_id: &str,
) -> &'a ResourceClassDefinition {
    resources
        .iter()
        .find(|resource| resource.resource_id == resource_id)
        .or_else(|| resources.first())
        .expect("resource scheduler dry run requires at least one resource")
}

fn reserve_floor_units(total_units: u64, reserve_floor_percent: u8) -> u64 {
    total_units
        .saturating_mul(reserve_floor_percent as u64)
        .div_ceil(100)
}

pub fn prepare_scheduled_iteration(
    request: ScheduledIterationRequest,
) -> Result<ScheduledIterationPlan, CampaignIterationError> {
    let campaign = request.campaign;
    if !matches!(
        campaign.status,
        EvolutionCampaignStatus::Approved
            | EvolutionCampaignStatus::Scheduled
            | EvolutionCampaignStatus::Running
    ) {
        return Err(CampaignIterationError::CampaignNotApproved {
            campaign_id: campaign.campaign_id,
        });
    }
    let task = match request.task_id {
        Some(task_id) => campaign
            .near_term_tasks
            .iter()
            .find(|task| task.task_id == task_id)
            .cloned()
            .ok_or_else(|| CampaignIterationError::TaskNotFound {
                campaign_id: campaign.campaign_id.clone(),
                task_id,
            })?,
        None => campaign.near_term_tasks.first().cloned().ok_or_else(|| {
            CampaignIterationError::NoNearTermTasks {
                campaign_id: campaign.campaign_id.clone(),
            }
        })?,
    };
    let mut command_texts = task.verification_commands.clone();
    command_texts.extend(request.extra_verification_commands);
    if command_texts.is_empty() {
        let project = campaign
            .source_repo
            .as_deref()
            .map(|repo| project_profile_for_root(Path::new(repo)))
            .unwrap_or_else(|| EvolutionProjectProfile {
                kind: EvolutionProjectKind::Sinomo,
                name: "sinomo".to_string(),
            });
        command_texts.extend(default_campaign_task_verification_commands(&project));
    }
    let verification_commands = command_texts
        .into_iter()
        .enumerate()
        .map(|(index, command)| VerificationCommand {
            name: format!("campaign-task-verify-{}", index + 1),
            command,
            expected_exit_code: 0,
            gate: EvaluationGate::StaticChecks,
        })
        .collect();
    let intended_paths = if request.intended_paths.is_empty() {
        default_iteration_intended_paths(&task.expected_result_kind)
    } else {
        request.intended_paths
    };
    let iteration_id = generated_id("iter");
    let evolution_request = EvolutionRequest {
        objective: scheduled_iteration_objective(&campaign.campaign_id, &iteration_id, &task),
        target_ring: campaign.target_ring.clone(),
        intended_paths,
        verification_commands,
        benchmark_gates: Vec::new(),
        module_replay_reports: Vec::new(),
        model_provider: None,
        model: None,
        dry_run: request.dry_run,
    };
    let mut operator_notes = vec![
        "resource scheduling must have granted a lease before model or agent dispatch".to_string(),
        "candidate work happens in an isolated worktree; the active checkout is not mutated"
            .to_string(),
    ];
    if task.expected_result_kind == EvolutionResultKind::RolloutGuard {
        operator_notes.push(
            "rollout-guard iterations must produce a minimized replay, regression gate, or explicit no-change diagnosis before proposing another rollout"
                .to_string(),
        );
        operator_notes
            .push("runtime rollout remains a separate explicit approval/apply step".to_string());
    }
    Ok(ScheduledIterationPlan {
        campaign_id: campaign.campaign_id,
        iteration_id,
        task,
        evolution_request,
        memory_context: Vec::new(),
        operator_notes,
    })
}

fn scheduled_iteration_objective(
    campaign_id: &str,
    iteration_id: &str,
    task: &NearTermEvolutionTask,
) -> String {
    let required_evidence = if task.required_evidence.is_empty() {
        "none declared".to_string()
    } else {
        task.required_evidence.join("; ")
    };
    let mut objective = format!(
        "Campaign {campaign_id} scheduled iteration {iteration_id}\nTask {}: {}\n\nTask objective: {}\nRequired evidence: {required_evidence}",
        task.task_id, task.title, task.objective
    );
    if task.expected_result_kind == EvolutionResultKind::RolloutGuard {
        let rollback_evidence_refs = task
            .required_evidence
            .iter()
            .filter(|evidence| {
                evidence.starts_with("rollout-telemetry://")
                    || evidence.starts_with("event://")
                    || evidence.contains("rollout-rollback")
            })
            .cloned()
            .collect::<Vec<_>>();
        objective.push_str(
            "\n\nRollout guard handoff:\n- Treat this as post-rollback regression mining, not a normal feature patch.\n- Preserve or produce a minimized replay, private regression gate, or explicit no-change diagnosis.\n- Do not request runtime rollout or promotion from this iteration.",
        );
        if !rollback_evidence_refs.is_empty() {
            objective.push_str("\n- Rollback evidence refs: ");
            objective.push_str(&rollback_evidence_refs.join("; "));
        }
    }
    objective
}

pub fn attach_evolution_memory_to_scheduled_iteration(
    mut plan: ScheduledIterationPlan,
    lessons: &[EvolutionLessonMemoryRecord],
    gates: &[EvolutionRegressionGateMemoryRecord],
    limit: usize,
) -> ScheduledIterationPlan {
    let contexts = evolution_iteration_memory_contexts(&plan.task.stage_id, lessons, gates, limit);
    if contexts.is_empty() {
        return plan;
    }
    let rendered = render_iteration_memory_context(&contexts);
    plan.evolution_request.objective = format!(
        "{}\n\nPrior self-evolution memory:\n{}",
        plan.evolution_request.objective, rendered
    );
    let applied_gates = apply_memory_regression_gates_to_iteration_plan(&mut plan, &contexts);
    plan.operator_notes.push(format!(
        "attached {} prior self-evolution lesson/gate memory item(s) to the model handoff",
        contexts.len()
    ));
    if applied_gates.command_gates > 0 || applied_gates.replay_fixture_gates > 0 {
        plan.operator_notes.push(format!(
            "materialized {} memory command gate(s) and {} replay fixture gate(s) into scheduled verification",
            applied_gates.command_gates, applied_gates.replay_fixture_gates
        ));
    }
    if applied_gates.rollout_telemetry_window_gates > 0 {
        plan.operator_notes.push(format!(
            "attached {} rollout telemetry window gate(s) as replay-minimization evidence; convert them to concrete replay fixtures before treating them as executable gates",
            applied_gates.rollout_telemetry_window_gates
        ));
    }
    if applied_gates.telemetry_trace_evidence_gates > 0 {
        plan.operator_notes.push(format!(
            "attached {} telemetry/event trace gate(s) as replay-minimization evidence; convert them to concrete replay fixtures before treating them as executable gates",
            applied_gates.telemetry_trace_evidence_gates
        ));
    }
    plan.memory_context = contexts;
    plan
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AppliedMemoryRegressionGates {
    command_gates: usize,
    replay_fixture_gates: usize,
    rollout_telemetry_window_gates: usize,
    telemetry_trace_evidence_gates: usize,
}

fn apply_memory_regression_gates_to_iteration_plan(
    plan: &mut ScheduledIterationPlan,
    contexts: &[EvolutionIterationMemoryContext],
) -> AppliedMemoryRegressionGates {
    let mut applied = AppliedMemoryRegressionGates::default();
    let mut existing_commands = plan
        .evolution_request
        .verification_commands
        .iter()
        .map(|command| command.command.clone())
        .collect::<BTreeSet<_>>();
    let mut existing_replay_reports = plan
        .evolution_request
        .module_replay_reports
        .iter()
        .map(|report| report.report_path.clone())
        .collect::<BTreeSet<_>>();
    let mut rollout_telemetry_windows = BTreeSet::new();
    let mut telemetry_trace_evidence_refs = BTreeSet::new();

    for context in contexts
        .iter()
        .filter(|context| context.source == EvolutionIterationMemorySource::ProposedRegressionGate)
    {
        if let Some(command) = &context.command
            && existing_commands.insert(command.clone())
        {
            plan.evolution_request
                .verification_commands
                .push(VerificationCommand {
                    name: memory_regression_gate_name("memory-gate", context),
                    command: command.clone(),
                    expected_exit_code: 0,
                    gate: context
                        .gate
                        .clone()
                        .unwrap_or(EvaluationGate::PrivateRegressionEvals),
                });
            applied.command_gates += 1;
        }

        if context.gate.as_ref() == Some(&EvaluationGate::ReplayTests)
            && context.gate_source.as_deref() == Some("module-replay-report")
            && let Some(report_path) = &context.primary_evidence_ref
            && existing_replay_reports.insert(report_path.clone())
        {
            plan.evolution_request
                .module_replay_reports
                .push(ModuleReplayReportRef {
                    name: memory_regression_gate_name("memory-replay", context),
                    report_path: report_path.clone(),
                    baseline_artifact_id: None,
                    candidate_artifact_id: None,
                });
            applied.replay_fixture_gates += 1;
        }

        if context.gate.as_ref() == Some(&EvaluationGate::ReplayTests)
            && context.gate_source.as_deref() == Some("rollout-telemetry-window")
            && let Some(evidence_ref) = &context.primary_evidence_ref
            && evidence_ref.starts_with("rollout-telemetry://window?")
            && rollout_telemetry_windows.insert(evidence_ref.clone())
        {
            applied.rollout_telemetry_window_gates += 1;
        }

        if context.gate.as_ref() == Some(&EvaluationGate::ReplayTests)
            && matches!(
                context.gate_source.as_deref(),
                Some("module-telemetry-window") | Some("event-log-signal")
            )
            && let Some(evidence_ref) = &context.primary_evidence_ref
            && telemetry_trace_evidence_refs.insert(evidence_ref.clone())
        {
            applied.telemetry_trace_evidence_gates += 1;
        }
    }

    if !rollout_telemetry_windows.is_empty() {
        let rendered_windows = rollout_telemetry_windows
            .iter()
            .map(|evidence_ref| {
                format!(
                    "- {evidence_ref}: convert this telemetry window into a minimized replay fixture or private ReplayTests gate before relying on another rollout attempt."
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        plan.evolution_request.objective = format!(
            "{}\n\nPrior rollout telemetry regression windows:\n{}",
            plan.evolution_request.objective, rendered_windows
        );
    }
    if !telemetry_trace_evidence_refs.is_empty() {
        let rendered_refs = telemetry_trace_evidence_refs
            .iter()
            .map(|evidence_ref| {
                format!(
                    "- {evidence_ref}: convert this telemetry/event evidence into a minimized replay fixture or private ReplayTests gate before relying on another attempt."
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        plan.evolution_request.objective = format!(
            "{}\n\nPrior telemetry regression evidence:\n{}",
            plan.evolution_request.objective, rendered_refs
        );
    }

    applied
}

fn memory_regression_gate_name(prefix: &str, context: &EvolutionIterationMemoryContext) -> String {
    let mut name = String::with_capacity(prefix.len() + 1 + context.memory_id.len());
    name.push_str(prefix);
    name.push('-');
    name.extend(context.memory_id.chars().map(|ch| {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            ch
        } else {
            '-'
        }
    }));
    name
}

fn evolution_iteration_memory_contexts(
    stage_id: &str,
    lessons: &[EvolutionLessonMemoryRecord],
    gates: &[EvolutionRegressionGateMemoryRecord],
    limit: usize,
) -> Vec<EvolutionIterationMemoryContext> {
    let limit = limit.max(1);
    let mut contexts = Vec::new();
    let selected_lessons = lessons
        .iter()
        .rev()
        .filter(|lesson| lesson.stage_id == stage_id || lesson.human_attention_needed)
        .collect::<Vec<_>>();
    let selected_lesson_adaptations = selected_lessons
        .iter()
        .map(|lesson| lesson.adaptation_id.clone())
        .collect::<BTreeSet<_>>();
    contexts.extend(
        selected_lessons
            .iter()
            .map(|lesson| EvolutionIterationMemoryContext {
                memory_id: lesson.memory_id.clone(),
                source: EvolutionIterationMemorySource::Lesson,
                campaign_id: lesson.campaign_id.clone(),
                stage_id: lesson.stage_id.clone(),
                summary: format!("{:?}: {}", lesson.failure_class, lesson.root_cause_summary),
                gate: None,
                gate_source: None,
                primary_evidence_ref: None,
                command: None,
                evidence_refs: lesson.evidence_refs.clone(),
            }),
    );
    contexts.extend(
        gates
            .iter()
            .rev()
            .filter(|gate| {
                gate.stage_id == stage_id
                    || selected_lesson_adaptations.contains(&gate.adaptation_id)
            })
            .map(|gate| EvolutionIterationMemoryContext {
                memory_id: gate.memory_id.clone(),
                source: EvolutionIterationMemorySource::ProposedRegressionGate,
                campaign_id: gate.campaign_id.clone(),
                stage_id: gate.stage_id.clone(),
                summary: gate.proposed_gate.rationale.clone(),
                gate: Some(gate.proposed_gate.gate.clone()),
                gate_source: Some(gate.proposed_gate.source.clone()),
                primary_evidence_ref: Some(gate.proposed_gate.evidence_ref.clone()),
                command: gate.proposed_gate.command.clone(),
                evidence_refs: {
                    let mut evidence_refs = gate.evidence_refs.clone();
                    evidence_refs.push(gate.proposed_gate.evidence_ref.clone());
                    evidence_refs.sort();
                    evidence_refs.dedup();
                    evidence_refs
                },
            }),
    );
    let mut seen = BTreeSet::new();
    contexts.retain(|context| seen.insert(context.memory_id.clone()));
    contexts.truncate(limit);
    contexts
}

fn render_iteration_memory_context(contexts: &[EvolutionIterationMemoryContext]) -> String {
    contexts
        .iter()
        .map(|context| {
            let mut line = format!(
                "- {:?} {} [{}]: {}",
                context.source, context.memory_id, context.stage_id, context.summary
            );
            if let Some(gate) = &context.gate {
                line.push_str(&format!("; gate={gate:?}"));
            }
            if let Some(command) = &context.command {
                line.push_str(&format!("; command=`{command}`"));
            }
            if !context.evidence_refs.is_empty() {
                line.push_str(&format!("; evidence={}", context.evidence_refs.join(", ")));
            }
            line
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn record_scheduled_iteration_result(
    mut campaign: EvolutionCampaign,
    task_id: &str,
    report: &EvolutionReport,
    resource_usage: Vec<ResourceUsageEstimate>,
) -> EvolutionCampaign {
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let task_kind = campaign
        .near_term_tasks
        .iter()
        .find(|task| task.task_id == task_id)
        .map(|task| task.expected_result_kind.clone())
        .unwrap_or(EvolutionResultKind::Other("unknown-task".to_string()));
    let outcome = report
        .scorecard
        .as_ref()
        .map(|scorecard| &scorecard.outcome);
    let status = if matches!(outcome, Some(EvaluationOutcome::Failed)) {
        EvolutionIterationStatus::Adapted
    } else if matches!(outcome, Some(EvaluationOutcome::Passed)) {
        EvolutionIterationStatus::AwaitingRolloutApproval
    } else {
        EvolutionIterationStatus::Evaluating
    };
    let evidence = campaign_iteration_evidence_refs(report, &resource_usage);
    let summary = match outcome {
        Some(EvaluationOutcome::Passed) => Some(
            "candidate produced and verification passed; waiting for rollout approval".to_string(),
        ),
        Some(EvaluationOutcome::Failed) => Some(
            "candidate verification failed; recorded as adaptation evidence instead of retrying silently"
                .to_string(),
        ),
        Some(EvaluationOutcome::Inconclusive) | None => Some(
            "candidate evidence is incomplete or inconclusive; keep under human review".to_string(),
        ),
    };
    campaign.iteration_results.push(EvolutionIterationResult {
        iteration_id: generated_id("iter_result"),
        task_id: task_id.to_string(),
        result_kind: task_kind,
        status: status.clone(),
        resource_usage: resource_usage.clone(),
        evidence: evidence.clone(),
        summary,
    });
    let (decision_kind, decision_summary, decision_rationale, campaign_status) = match status {
        EvolutionIterationStatus::Adapted => (
            CampaignDecisionKind::DirectionChanged,
            "Recorded failed iteration as adaptation evidence".to_string(),
            "Phase 2 must not silently retry failed implementation work; the next backlog change needs explicit review."
                .to_string(),
            EvolutionCampaignStatus::Scheduled,
        ),
        EvolutionIterationStatus::AwaitingRolloutApproval => (
            CampaignDecisionKind::TaskSelected,
            "Attached one scheduled candidate to campaign".to_string(),
            "Verification passed in the candidate bundle; rollout still requires existing approval gates."
                .to_string(),
            EvolutionCampaignStatus::PausedHuman,
        ),
        _ => (
            CampaignDecisionKind::TaskSelected,
            "Attached inconclusive scheduled candidate to campaign".to_string(),
            "The candidate bundle needs human inspection before further scheduling.".to_string(),
            EvolutionCampaignStatus::PausedHuman,
        ),
    };
    campaign.decisions.push(CampaignDecision {
        decision_id: generated_id("decision"),
        kind: decision_kind,
        summary: decision_summary,
        rationale: decision_rationale,
        evidence_refs: evidence,
        decided_by: "campaign-iteration-runner".to_string(),
        decided_at: now.clone(),
    });
    campaign.status = campaign_status;
    campaign.updated_at = now;
    campaign
}

pub fn evaluate_campaign_stage(request: StageEvaluationRequest) -> StageEvaluationUpdate {
    let StageEvaluationRequest {
        campaign,
        task_id,
        report,
        resource_usage,
        failure_class,
        root_cause_summary,
        what_was_learned,
    } = request;
    let task = campaign
        .near_term_tasks
        .iter()
        .find(|task| task.task_id == task_id)
        .cloned();
    let stage = task
        .as_ref()
        .and_then(|task| {
            campaign
                .stages
                .iter()
                .find(|stage| stage.stage_id == task.stage_id)
                .cloned()
        })
        .or_else(|| campaign.stages.first().cloned());
    let stage_id = stage
        .as_ref()
        .map(|stage| stage.stage_id.clone())
        .unwrap_or_else(|| "stage_unknown".to_string());
    let outcome = report
        .scorecard
        .as_ref()
        .map(|scorecard| scorecard.outcome.clone())
        .unwrap_or(EvaluationOutcome::Inconclusive);
    let evidence_refs = campaign_iteration_evidence_refs(&report, &resource_usage);
    let declared_metrics = stage
        .as_ref()
        .map(|stage| stage.acceptance_metrics.clone())
        .unwrap_or_default();
    let met_metrics = if outcome == EvaluationOutcome::Passed {
        declared_metrics.clone()
    } else {
        Vec::new()
    };
    let missing_metrics = declared_metrics
        .iter()
        .filter(|metric| !met_metrics.contains(metric))
        .cloned()
        .collect::<Vec<_>>();
    let promotion = match outcome {
        EvaluationOutcome::Passed if missing_metrics.is_empty() => StagePromotionDecision::Promote,
        EvaluationOutcome::Passed => StagePromotionDecision::HoldHumanCheckpoint,
        EvaluationOutcome::Failed => StagePromotionDecision::HoldFailed,
        EvaluationOutcome::Inconclusive => StagePromotionDecision::HoldInconclusive,
    };
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let stage_scorecard = CampaignStageScorecard {
        scorecard_id: generated_id("stage_score"),
        campaign_id: campaign.campaign_id.clone(),
        stage_id: stage_id.clone(),
        task_id: task_id.clone(),
        iteration_id: generated_id("stage_eval"),
        outcome: outcome.clone(),
        declared_metrics,
        met_metrics,
        missing_metrics,
        regression_gates: stage
            .as_ref()
            .map(|stage| stage.regression_gates.clone())
            .unwrap_or_default(),
        evidence_refs: evidence_refs.clone(),
        promotion: promotion.clone(),
        summary: match promotion {
            StagePromotionDecision::Promote => {
                "stage acceptance metrics passed; ready for human rollout checkpoint".to_string()
            }
            StagePromotionDecision::HoldFailed => {
                "stage metrics did not pass; adapt before retrying same task family".to_string()
            }
            StagePromotionDecision::HoldInconclusive => {
                "stage evidence is inconclusive; human metric checkpoint required".to_string()
            }
            StagePromotionDecision::HoldHumanCheckpoint => {
                "stage evidence needs human review before promotion".to_string()
            }
        },
        recorded_at: now.clone(),
    };

    let mut campaign =
        record_scheduled_iteration_result(campaign, &task_id, &report, resource_usage);
    let candidate_lesson = candidate_evolution_lesson_from_bundle(&report.bundle);
    let candidate_regression_evidence = candidate_regression_evidence_from_bundle(&report.bundle);
    let adaptation = if outcome == EvaluationOutcome::Failed {
        let inferred_failure_class = failure_class
            .or_else(|| {
                candidate_lesson
                    .as_ref()
                    .map(|lesson| lesson.failure_class.clone())
            })
            .unwrap_or_else(|| classify_failure_from_report(&report));
        let inferred_root_cause = root_cause_summary.or_else(|| {
            candidate_lesson
                .as_ref()
                .map(|lesson| lesson.root_cause_summary.clone())
        });
        let learned =
            learned_items_from_candidate_lesson(what_was_learned, candidate_lesson.as_ref());
        let learned = learned_items_from_candidate_regression_evidence(
            learned,
            candidate_regression_evidence.as_ref(),
        );
        Some(build_campaign_adaptation_record(
            &mut campaign,
            task.as_ref(),
            &stage_id,
            &task_id,
            inferred_failure_class,
            inferred_root_cause,
            learned,
            candidate_regression_evidence.as_ref(),
            &evidence_refs,
            &now,
        ))
    } else {
        None
    };

    match stage_scorecard.promotion {
        StagePromotionDecision::Promote => {
            campaign.status = EvolutionCampaignStatus::StageComplete;
            campaign.approval_checkpoint.status = ApprovalCheckpointStatus::AwaitingHuman;
            campaign.approval_checkpoint.required_before =
                format!("first rollout or next-stage scheduling for {stage_id}");
            campaign.decisions.push(CampaignDecision {
                decision_id: generated_id("decision"),
                kind: CampaignDecisionKind::StagePromoted,
                summary: format!("Stage metrics met for `{stage_id}`"),
                rationale:
                    "Phase 3 promotion requires declared stage metrics to pass before stage completion."
                        .to_string(),
                evidence_refs: vec!["stage-scorecards.jsonl".to_string()],
                decided_by: "campaign-stage-evaluator".to_string(),
                decided_at: now.clone(),
            });
        }
        StagePromotionDecision::HoldInconclusive | StagePromotionDecision::HoldHumanCheckpoint => {
            campaign.status = EvolutionCampaignStatus::PausedHuman;
            campaign.approval_checkpoint.status = ApprovalCheckpointStatus::AwaitingHuman;
            campaign.approval_checkpoint.required_before =
                format!("clarify ambiguous metrics for {stage_id}");
        }
        StagePromotionDecision::HoldFailed => {
            if matches!(
                adaptation
                    .as_ref()
                    .map(|adaptation| &adaptation.failure_class),
                Some(
                    EvolutionFailureClass::FoundationBlocker
                        | EvolutionFailureClass::AmbiguousMetric
                )
            ) {
                campaign.status = EvolutionCampaignStatus::PausedHuman;
                campaign.approval_checkpoint.status = ApprovalCheckpointStatus::AwaitingHuman;
                campaign.approval_checkpoint.required_before =
                    format!("review adaptation before retrying {stage_id}");
            } else {
                campaign.status = EvolutionCampaignStatus::Scheduled;
            }
        }
    }
    campaign.updated_at = now;

    StageEvaluationUpdate {
        campaign,
        stage_scorecard,
        adaptation,
    }
}

fn campaign_iteration_evidence_refs(
    report: &EvolutionReport,
    resource_usage: &[ResourceUsageEstimate],
) -> Vec<String> {
    let mut evidence = vec![
        format!("job:{}", report.job.job_id),
        format!(
            "candidate-worktree:{}",
            report.candidate_workspace.path.display()
        ),
        format!("bundle:{}", report.bundle.bundle_path),
    ];
    if let Some(scorecard_path) = &report.bundle.scorecard_path {
        evidence.push(format!("scorecard:{scorecard_path}"));
    }
    if let Some(changed_files_path) = &report.bundle.changed_files_path {
        evidence.push(format!("changed-files:{changed_files_path}"));
    }
    if let Some(resource_usage_path) = &report.bundle.resource_usage_path {
        evidence.push(format!("resource-usage:{resource_usage_path}"));
    }
    for (label, relative_path) in [
        (
            "regression-evidence",
            CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH,
        ),
        ("evolution-lesson", CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH),
        ("evidence-summary", CANDIDATE_EVIDENCE_SUMMARY_RELATIVE_PATH),
    ] {
        let path = PathBuf::from(&report.bundle.bundle_path).join(relative_path);
        if path.exists() {
            evidence.push(format!("{label}:{}", path.display()));
        }
    }
    for usage in resource_usage {
        evidence.push(format!(
            "resource-usage:{} estimated={} actual={}",
            usage.resource,
            usage.estimated_units,
            usage
                .actual_units
                .map(|units| units.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        ));
    }
    evidence
}

fn candidate_evolution_lesson_from_bundle(
    bundle: &CandidateBundleManifest,
) -> Option<CandidateEvolutionLessonRecord> {
    let path = PathBuf::from(&bundle.bundle_path).join(CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH);
    let content = fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

fn candidate_regression_evidence_from_bundle(
    bundle: &CandidateBundleManifest,
) -> Option<CandidateRegressionEvidenceSummary> {
    let path = PathBuf::from(&bundle.bundle_path).join(CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH);
    let content = fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

fn learned_items_from_candidate_lesson(
    mut learned: Vec<String>,
    lesson: Option<&CandidateEvolutionLessonRecord>,
) -> Vec<String> {
    let Some(lesson) = lesson else {
        return learned;
    };
    learned.extend(
        lesson
            .failed_assumptions
            .iter()
            .map(|assumption| format!("failed assumption: {assumption}")),
    );
    learned.extend(
        lesson
            .suggested_follow_up_tasks
            .iter()
            .map(|task| format!("candidate follow-up: {task}")),
    );
    learned.extend(
        lesson
            .reusable_evidence_refs
            .iter()
            .map(|evidence| format!("reusable evidence: {evidence}")),
    );
    learned.sort();
    learned.dedup();
    learned
}

fn learned_items_from_candidate_regression_evidence(
    mut learned: Vec<String>,
    evidence: Option<&CandidateRegressionEvidenceSummary>,
) -> Vec<String> {
    let Some(evidence) = evidence else {
        return learned;
    };
    learned.extend(
        evidence
            .reusable_gate_suggestions
            .iter()
            .map(|suggestion| format!("candidate gate suggestion: {suggestion}")),
    );
    learned.extend(
        evidence
            .replay_minimization
            .minimization_steps
            .iter()
            .map(|step| format!("replay minimization: {step}")),
    );
    learned.extend(
        evidence
            .replay_minimization
            .retained_evidence_refs
            .iter()
            .map(|evidence_ref| format!("retained regression evidence: {evidence_ref}")),
    );
    learned.sort();
    learned.dedup();
    learned
}

fn classify_failure_from_report(report: &EvolutionReport) -> EvolutionFailureClass {
    if report.changed_files.is_empty() {
        EvolutionFailureClass::WrongTaskSelection
    } else if report
        .job
        .required_gates
        .contains(&EvaluationGate::StaticChecks)
    {
        EvolutionFailureClass::BuildTestFailure
    } else {
        EvolutionFailureClass::EvalRegression
    }
}

fn build_campaign_adaptation_record(
    campaign: &mut EvolutionCampaign,
    task: Option<&NearTermEvolutionTask>,
    stage_id: &str,
    task_id: &str,
    failure_class: EvolutionFailureClass,
    root_cause_summary: Option<String>,
    what_was_learned: Vec<String>,
    regression_evidence: Option<&CandidateRegressionEvidenceSummary>,
    evidence_refs: &[String],
    now: &str,
) -> CampaignAdaptationRecord {
    let repeated_failures = campaign
        .iteration_results
        .iter()
        .filter(|result| {
            result.task_id == task_id && result.status == EvolutionIterationStatus::Adapted
        })
        .count();
    let foundation_issue_detected = failure_class == EvolutionFailureClass::FoundationBlocker;
    let human_attention_needed = matches!(
        failure_class,
        EvolutionFailureClass::FoundationBlocker | EvolutionFailureClass::AmbiguousMetric
    );
    let proposed_regression_gates =
        proposed_regression_gates_from_candidate_evidence(task_id, regression_evidence);
    attach_proposed_regression_gates_to_stage(campaign, stage_id, &proposed_regression_gates);
    let should_propose_replacement = foundation_issue_detected || repeated_failures >= 2;
    let replacement_task_candidate = if should_propose_replacement {
        Some(adapted_task_for_failure(
            task,
            stage_id,
            task_id,
            &failure_class,
            root_cause_summary.as_deref(),
            &proposed_regression_gates,
        ))
    } else {
        None
    };
    if let Some(replacement) = replacement_task_candidate.clone() {
        campaign.near_term_tasks.insert(0, replacement.clone());
        campaign.decisions.push(CampaignDecision {
            decision_id: generated_id("decision"),
            kind: CampaignDecisionKind::DirectionChanged,
            summary: format!("Proposed adapted task `{}`", replacement.task_id),
            rationale: format!(
                "Failed task `{task_id}` classified as {:?}; next work must be easier, diagnostic, or foundational.",
                failure_class
            ),
            evidence_refs: evidence_refs.to_vec(),
            decided_by: "campaign-stage-evaluator".to_string(),
            decided_at: now.to_string(),
        });
    }
    CampaignAdaptationRecord {
        adaptation_id: generated_id("adapt"),
        campaign_id: campaign.campaign_id.clone(),
        stage_id: stage_id.to_string(),
        failed_iteration_id: campaign
            .iteration_results
            .last()
            .map(|result| result.iteration_id.clone())
            .unwrap_or_else(|| "unknown-iteration".to_string()),
        failed_task_id: task_id.to_string(),
        failure_class: failure_class.clone(),
        root_cause_summary: root_cause_summary.unwrap_or_else(|| {
            format!(
                "candidate failed with class {:?}; inspect scorecard and logs",
                failure_class
            )
        }),
        what_was_learned,
        next_task_simplification: adaptation_simplification(&failure_class),
        new_eval_or_diagnostic: adaptation_diagnostic_with_gates(
            &failure_class,
            &proposed_regression_gates,
        ),
        foundation_issue_detected,
        proposed_regression_gates,
        replacement_task_candidate,
        budget_adjustment: adaptation_budget_adjustment(&failure_class),
        human_attention_needed,
        evidence_refs: evidence_refs.to_vec(),
        recorded_at: now.to_string(),
    }
}

fn adapted_task_for_failure(
    task: Option<&NearTermEvolutionTask>,
    stage_id: &str,
    task_id: &str,
    failure_class: &EvolutionFailureClass,
    root_cause: Option<&str>,
    proposed_regression_gates: &[CampaignProposedRegressionGate],
) -> NearTermEvolutionTask {
    let base_title = task
        .map(|task| task.title.clone())
        .unwrap_or_else(|| task_id.to_string());
    let (title, objective, expected_result_kind) = match failure_class {
        EvolutionFailureClass::FoundationBlocker => (
            format!("Repair foundation blocker for {base_title}"),
            format!(
                "Reproduce and repair the foundation blocker before retrying `{task_id}`. Root cause: {}",
                root_cause.unwrap_or("foundation blocker needs diagnosis")
            ),
            EvolutionResultKind::FoundationRepair,
        ),
        EvolutionFailureClass::AmbiguousMetric => (
            format!("Clarify metrics for {base_title}"),
            format!(
                "Add a clearer stage metric or diagnostic scorecard before retrying `{task_id}`."
            ),
            EvolutionResultKind::Evaluator,
        ),
        _ => (
            format!("Add diagnostic repro for {base_title}"),
            format!(
                "Make the failure for `{task_id}` reproducible and narrower before another implementation attempt."
            ),
            EvolutionResultKind::Instrumentation,
        ),
    };
    let mut required_evidence = vec![
        "failed iteration scorecard".to_string(),
        "focused diagnostic or blocker repro".to_string(),
    ];
    required_evidence.extend(proposed_regression_gates.iter().map(|gate| {
        format!(
            "proposed {:?} regression gate from {}",
            gate.gate, gate.evidence_ref
        )
    }));
    required_evidence.sort();
    required_evidence.dedup();

    let mut verification_commands = task
        .map(|task| task.verification_commands.clone())
        .unwrap_or_default();
    verification_commands.extend(
        proposed_regression_gates
            .iter()
            .filter_map(|gate| gate.command.clone()),
    );
    verification_commands.sort();
    verification_commands.dedup();

    NearTermEvolutionTask {
        task_id: generated_id("task"),
        stage_id: stage_id.to_string(),
        title,
        objective,
        rationale: "Phase 3 adaptation requires a failed task family to become easier, more diagnostic, or more foundational before retry.".to_string(),
        required_evidence,
        verification_commands,
        estimated_budget: "smaller diagnostic lease before full implementation retry".to_string(),
        approval_required: true,
        expected_result_kind,
    }
}

pub(crate) fn proposed_regression_gates_from_candidate_evidence(
    task_id: &str,
    evidence: Option<&CandidateRegressionEvidenceSummary>,
) -> Vec<CampaignProposedRegressionGate> {
    let Some(evidence) = evidence else {
        return Vec::new();
    };
    let mut gates = Vec::new();

    for failure in &evidence.failed_commands {
        gates.push(CampaignProposedRegressionGate {
            gate_id: format!("gate_{}_{}", task_id, gates.len() + 1),
            gate: EvaluationGate::StaticChecks,
            source: "failed-command".to_string(),
            evidence_ref: failure
                .command_log_path
                .clone()
                .or_else(|| evidence.scorecard_path.clone())
                .unwrap_or_else(|| evidence.bundle_path.clone()),
            command: Some(failure.rerun_command.clone()),
            rationale: format!(
                "Rerun `{}` until the diagnostic task makes the failure reproducible or fixed.",
                failure.command
            ),
        });
    }

    for failure in &evidence.failed_benchmarks {
        gates.push(CampaignProposedRegressionGate {
            gate_id: format!("gate_{}_{}", task_id, gates.len() + 1),
            gate: EvaluationGate::PrivateRegressionEvals,
            source: "failed-benchmark".to_string(),
            evidence_ref: failure
                .summary_path
                .clone()
                .or_else(|| failure.results_dir.clone())
                .or_else(|| failure.command_log_path.clone())
                .unwrap_or_else(|| evidence.bundle_path.clone()),
            command: Some(failure.command.clone()),
            rationale: format!(
                "Minimize benchmark `{}` into a private regression gate before retrying the original task.",
                failure.name
            ),
        });
    }

    for fixture in &evidence.trace_fixtures {
        gates.push(CampaignProposedRegressionGate {
            gate_id: format!("gate_{}_{}", task_id, gates.len() + 1),
            gate: fixture.suggested_gate.clone(),
            source: fixture.source.clone(),
            evidence_ref: fixture.report_path.clone(),
            command: None,
            rationale: fixture.minimization_hint.clone(),
        });
    }

    for failed_gate in &evidence.failed_gates {
        let evidence_ref = evidence
            .scorecard_path
            .clone()
            .or_else(|| evidence.diff_path.clone())
            .unwrap_or_else(|| evidence.bundle_path.clone());
        let already_represented = gates
            .iter()
            .any(|gate| gate.gate == failed_gate.gate && gate.evidence_ref == evidence_ref);
        if !already_represented {
            gates.push(CampaignProposedRegressionGate {
                gate_id: format!("gate_{}_{}", task_id, gates.len() + 1),
                gate: failed_gate.gate.clone(),
                source: "failed-scorecard-gate".to_string(),
                evidence_ref,
                command: None,
                rationale: format!(
                    "Preserve failed {:?} scorecard evidence: {}",
                    failed_gate.gate, failed_gate.summary
                ),
            });
        }
    }

    let mut seen = BTreeSet::new();
    gates.retain(|gate| {
        seen.insert((
            format!("{:?}", gate.gate),
            gate.source.clone(),
            gate.evidence_ref.clone(),
            gate.command.clone(),
        ))
    });
    gates.truncate(8);
    gates
}

fn attach_proposed_regression_gates_to_stage(
    campaign: &mut EvolutionCampaign,
    stage_id: &str,
    proposed_regression_gates: &[CampaignProposedRegressionGate],
) {
    if proposed_regression_gates.is_empty() {
        return;
    }
    let Some(stage) = campaign
        .stages
        .iter_mut()
        .find(|stage| stage.stage_id == stage_id)
    else {
        return;
    };
    stage
        .regression_gates
        .extend(proposed_regression_gates.iter().map(|gate| {
            format!(
                "{:?} gate from {} ({})",
                gate.gate, gate.evidence_ref, gate.source
            )
        }));
    stage.regression_gates.sort();
    stage.regression_gates.dedup();
}

fn adaptation_diagnostic_with_gates(
    failure_class: &EvolutionFailureClass,
    proposed_regression_gates: &[CampaignProposedRegressionGate],
) -> String {
    let base = adaptation_diagnostic(failure_class);
    if proposed_regression_gates.is_empty() {
        return base;
    }
    let gates = proposed_regression_gates
        .iter()
        .map(|gate| format!("{:?} from {}", gate.gate, gate.evidence_ref))
        .collect::<Vec<_>>()
        .join("; ");
    format!("{base}; proposed reusable gates: {gates}")
}

fn adaptation_simplification(failure_class: &EvolutionFailureClass) -> String {
    match failure_class {
        EvolutionFailureClass::FoundationBlocker => {
            "switch to a prerequisite foundation repair before retrying the original stage task"
                .to_string()
        }
        EvolutionFailureClass::AmbiguousMetric => {
            "replace the ambiguous metric with a narrower diagnostic scorecard".to_string()
        }
        EvolutionFailureClass::ResourceExhaustion => {
            "defer expensive work and schedule an analysis-only or cheaper scouting task"
                .to_string()
        }
        _ => "split the failed task into a smaller diagnostic or instrumentation step".to_string(),
    }
}

fn adaptation_diagnostic(failure_class: &EvolutionFailureClass) -> String {
    match failure_class {
        EvolutionFailureClass::BuildTestFailure => {
            "focused repro test or build log triage".to_string()
        }
        EvolutionFailureClass::EvalRegression => "metric-specific regression fixture".to_string(),
        EvolutionFailureClass::ArtifactQualityFailure => {
            "labeled artifact inspection examples".to_string()
        }
        EvolutionFailureClass::RolloutFailure => "shadow-only rollout health check".to_string(),
        EvolutionFailureClass::FoundationBlocker => {
            "foundation blocker reproduction and post-fix gate".to_string()
        }
        EvolutionFailureClass::AmbiguousMetric => "human-reviewed metric clarification".to_string(),
        _ => "diagnostic evidence bundle before retry".to_string(),
    }
}

fn adaptation_budget_adjustment(failure_class: &EvolutionFailureClass) -> String {
    match failure_class {
        EvolutionFailureClass::ResourceExhaustion => {
            "lower next lease estimate and wait for reset or cheaper resource class".to_string()
        }
        EvolutionFailureClass::RepeatedImplementationFailure => {
            "cap retries until diagnostic task passes".to_string()
        }
        _ => "keep existing budget policy; require scheduling before adapted dispatch".to_string(),
    }
}

pub fn accept_task_replacement_proposal(
    mut campaign: EvolutionCampaign,
    proposal: CampaignTaskReplacementProposal,
) -> EvolutionCampaign {
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let stage_id = campaign
        .near_term_tasks
        .first()
        .map(|task| task.stage_id.clone())
        .or_else(|| campaign.stages.first().map(|stage| stage.stage_id.clone()))
        .unwrap_or_else(|| "stage_replacement".to_string());
    let task = NearTermEvolutionTask {
        task_id: generated_id("task"),
        stage_id,
        title: proposal.title.clone(),
        objective: proposal.objective.clone(),
        rationale: proposal.rationale.clone(),
        required_evidence: proposal.required_evidence.clone(),
        verification_commands: proposal.verification_commands.clone(),
        estimated_budget: "inherits campaign resource policy; schedule before dispatch".to_string(),
        approval_required: true,
        expected_result_kind: proposal.expected_result_kind,
    };
    campaign.near_term_tasks.insert(0, task.clone());
    campaign.decisions.push(CampaignDecision {
        decision_id: generated_id("decision"),
        kind: CampaignDecisionKind::DirectionChanged,
        summary: format!("Accepted replacement task `{}`", task.task_id),
        rationale: proposal.rationale,
        evidence_refs: vec![format!("replacement-task:{}", task.task_id)],
        decided_by: "campaign-iteration-runner".to_string(),
        decided_at: now.clone(),
    });
    campaign.status = EvolutionCampaignStatus::Scheduled;
    campaign.updated_at = now;
    campaign
}

pub fn decide_campaign_checkpoint(
    mut campaign: EvolutionCampaign,
    decision: CampaignCheckpointDecision,
    note: Option<String>,
    decided_by: impl Into<String>,
) -> EvolutionCampaign {
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let decided_by = decided_by.into();
    let note = note
        .map(|note| note.trim().to_string())
        .filter(|note| !note.is_empty());

    match decision {
        CampaignCheckpointDecision::Approve => {
            campaign.status = EvolutionCampaignStatus::Scheduled;
            campaign.approval_checkpoint.status = ApprovalCheckpointStatus::Approved;
            campaign.approval_checkpoint.required_before =
                "background scheduler may request a resource lease before dispatch".to_string();
            campaign.approval_checkpoint.possible_actions = vec![
                "run background scheduler tick with --dispatch-approved".to_string(),
                "run explicit iteration with sinomo evolve iterate".to_string(),
            ];
            campaign.decisions.push(CampaignDecision {
                decision_id: generated_id("decision"),
                kind: CampaignDecisionKind::Approved,
                summary: "Approved campaign checkpoint for scheduled iteration".to_string(),
                rationale: note.unwrap_or_else(|| {
                    "human checkpoint approved; resource scheduler still gates dispatch".to_string()
                }),
                evidence_refs: vec![
                    "campaign.json".to_string(),
                    "approval-checkpoint.json".to_string(),
                ],
                decided_by,
                decided_at: now.clone(),
            });
        }
        CampaignCheckpointDecision::Reject => {
            campaign.status = EvolutionCampaignStatus::Cancelled;
            campaign.approval_checkpoint.status = ApprovalCheckpointStatus::Rejected;
            campaign.approval_checkpoint.required_before =
                "campaign was rejected at human checkpoint".to_string();
            campaign.approval_checkpoint.possible_actions = vec![
                "inspect campaign bundle".to_string(),
                "create a narrower replacement campaign".to_string(),
            ];
            campaign.decisions.push(CampaignDecision {
                decision_id: generated_id("decision"),
                kind: CampaignDecisionKind::Rejected,
                summary: "Rejected campaign checkpoint".to_string(),
                rationale: note
                    .unwrap_or_else(|| "human checkpoint rejected this campaign".to_string()),
                evidence_refs: vec![
                    "campaign.json".to_string(),
                    "approval-checkpoint.json".to_string(),
                ],
                decided_by,
                decided_at: now.clone(),
            });
        }
    }

    campaign.updated_at = now;
    campaign
}

fn default_iteration_intended_paths(kind: &EvolutionResultKind) -> Vec<String> {
    match kind {
        EvolutionResultKind::Documentation => vec!["docs/**".to_string()],
        EvolutionResultKind::Evaluator => vec!["crates/sinomo-eval/**".to_string()],
        EvolutionResultKind::TaskReplacementProposal => vec!["docs/plans/**".to_string()],
        EvolutionResultKind::Artifact => vec!["docs/**".to_string(), "crates/**".to_string()],
        EvolutionResultKind::RolloutGuard => vec![
            "crates/sinomo-rollout/**".to_string(),
            "crates/sinomo-module/**".to_string(),
            "modules/evolution/sinomo-evolution/**".to_string(),
            "crates/sinomo-cli/**".to_string(),
            "docs/**".to_string(),
        ],
        EvolutionResultKind::Instrumentation
        | EvolutionResultKind::FoundationRepair
        | EvolutionResultKind::Patch
        | EvolutionResultKind::Other(_) => vec![
            "modules/evolution/sinomo-evolution/**".to_string(),
            "crates/sinomo-cli/**".to_string(),
            "docs/**".to_string(),
        ],
    }
}

pub fn propose_evolution_campaign(request: CampaignProposalRequest) -> EvolutionCampaign {
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let campaign_id = generated_id("camp");
    let objective_id = generated_id("objcard");
    let objective_kind = classify_objective(&request.objective);
    let project = request
        .source_repo
        .as_deref()
        .map(|repo| project_profile_for_root(Path::new(repo)))
        .unwrap_or_else(|| EvolutionProjectProfile {
            kind: EvolutionProjectKind::Sinomo,
            name: "sinomo".to_string(),
        });
    let stages = stages_for_objective(objective_kind);
    let near_term_tasks = tasks_for_objective(objective_kind, &stages, &project);
    let budget_policy = CampaignBudgetPolicy {
        policy_id: request.budget_policy.clone(),
        reserve_floors: request.reserve_floors.clone(),
        scheduling_notes: budget_notes(&request),
        confidence: "estimated: no live resource ledger in phase 0".to_string(),
        debt_note:
            "Phase 1 must replace this preview with lease accounting and reset-window state."
                .to_string(),
    };
    let status = if request.approved {
        EvolutionCampaignStatus::Approved
    } else {
        EvolutionCampaignStatus::AwaitingApproval
    };
    let approval_status = if request.approved {
        ApprovalCheckpointStatus::Approved
    } else {
        ApprovalCheckpointStatus::AwaitingHuman
    };

    EvolutionCampaign {
        campaign_id: campaign_id.clone(),
        status,
        objective_card: ObjectiveCard {
            objective_id,
            human_text: request.objective.clone(),
            interpreted_goal: interpreted_goal(objective_kind, &request.objective),
            non_goals: non_goals(objective_kind),
            assumptions: assumptions(objective_kind),
            constraints: proposal_constraints(&request),
            success_metrics: success_metrics(objective_kind),
            failure_conditions: failure_conditions(objective_kind, &request.reserve_floors),
            safety_notes: safety_notes(objective_kind),
            resource_policy: request.budget_policy.clone(),
            approval_policy: request.approval_policy.clone(),
            initial_horizon: request.horizon.clone(),
            assumptions_needing_human_review: vec![
                "Confirm objective interpretation, target environment, spending boundaries, and approval policy before scheduled execution.".to_string(),
            ],
        },
        stages,
        near_term_tasks,
        iteration_results: Vec::new(),
        decisions: vec![CampaignDecision {
            decision_id: generated_id("decision"),
            kind: CampaignDecisionKind::ProposalCreated,
            summary: "Created phase-0 long-horizon campaign proposal without agent execution.".to_string(),
            rationale: "Phase 0 is proposal-only: persist an inspectable campaign bundle with stages, tasks, budgets, and approval state before dispatching implementation agents.".to_string(),
            evidence_refs: vec!["docs/self-evolution-architecture.md#campaign-proposals".to_string()],
            decided_by: "sinomo.evolution.campaign-planner.phase0".to_string(),
            decided_at: now.clone(),
        }],
        resource_schedule_preview: resource_schedule_preview(&request),
        budget_policy,
        approval_checkpoint: ApprovalCheckpoint {
            policy: request.approval_policy.clone(),
            status: approval_status,
            required_before: "scheduling the first implementation iteration or spending scarce resources".to_string(),
            editable_fields: vec![
                "objective_card".to_string(),
                "stages".to_string(),
                "near_term_tasks".to_string(),
                "budget_policy".to_string(),
                "approval_policy".to_string(),
            ],
            possible_actions: vec!["approve".to_string(), "reject".to_string(), "edit".to_string()],
        },
        target_ring: request.target_ring,
        dry_run: request.dry_run,
        capability_registry_required: Some(false),
        created_at: now.clone(),
        updated_at: now,
        source_repo: request.source_repo,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectiveKind {
    Scheduler,
    Minecraft,
    Video,
    Generic,
}

fn classify_objective(objective: &str) -> ObjectiveKind {
    let lower = objective.to_ascii_lowercase();
    if lower.contains("scheduler")
        || lower.contains("pause")
        || lower.contains("resume")
        || lower.contains("budget")
    {
        ObjectiveKind::Scheduler
    } else if lower.contains("minecraft") || lower.contains("speedrun") {
        ObjectiveKind::Minecraft
    } else if lower.contains("video") || lower.contains("clip") {
        ObjectiveKind::Video
    } else {
        ObjectiveKind::Generic
    }
}

fn generated_id(prefix: &str) -> String {
    format!("{prefix}_{}", EventId::generate().as_str())
}

fn proposal_constraints(request: &CampaignProposalRequest) -> Vec<String> {
    let mut constraints = vec![
        format!("horizon: {}", request.horizon),
        format!("budget policy: {}", request.budget_policy),
        "no formal capability registry is required for phase 0".to_string(),
    ];
    if let Some(source_repo) = &request.source_repo {
        let project = project_profile_for_root(Path::new(source_repo));
        constraints.push(format!("target project: {}", project.summary()));
        constraints.push(format!("source repo: {source_repo}"));
    }
    if let Some(max_wall_time) = &request.max_wall_time {
        constraints.push(format!("max wall time: {max_wall_time}"));
    }
    if request.pause_after_proposal {
        constraints
            .push("pause after proposal before scheduling implementation agents".to_string());
    }
    constraints
}

fn interpreted_goal(kind: ObjectiveKind, objective: &str) -> String {
    match kind {
        ObjectiveKind::Scheduler => "Improve Sinomo's self-evolution scheduling foundation so campaigns can plan, pause, and resume around resource budgets with durable evidence.".to_string(),
        ObjectiveKind::Minecraft => "Build and improve Sinomo until it can make measurable progress toward Minecraft speedrunning objectives in a controlled local environment.".to_string(),
        ObjectiveKind::Video => "Build and improve a repeatable workflow that can plan, generate, inspect, revise, and assemble generated video segments into a longer draft.".to_string(),
        ObjectiveKind::Generic => format!("Pursue the human objective as a durable, staged self-evolution campaign: {objective}"),
    }
}

fn non_goals(kind: ObjectiveKind) -> Vec<String> {
    let mut goals = vec![
        "No unapproved paid API overage.".to_string(),
        "No broad live rollout without evidence gates.".to_string(),
        "No direct mutation of the active checkout.".to_string(),
    ];
    match kind {
        ObjectiveKind::Minecraft => {
            goals.push(
                "No public-server automation, anti-cheat bypass, or account-risk behavior."
                    .to_string(),
            );
        }
        ObjectiveKind::Video => {
            goals.push("No unreviewed public publishing or use of copyrighted assets unless explicitly approved.".to_string());
        }
        _ => {}
    }
    goals
}

fn assumptions(kind: ObjectiveKind) -> Vec<String> {
    match kind {
        ObjectiveKind::Scheduler => vec![
            "Current scheduler/resource controls are incomplete and should be improved through small verified changes.".to_string(),
            "The first run is allowed to work on Sinomo's own foundation when that removes campaign blockers.".to_string(),
        ],
        ObjectiveKind::Minecraft => vec![
            "The target environment is a controlled local Minecraft setup or equivalent fixture.".to_string(),
            "Early progress means observation/action evidence, not a full speedrun.".to_string(),
        ],
        ObjectiveKind::Video => vec![
            "Available video APIs or local generators may have quota, duration, or quality limits.".to_string(),
            "Early progress means creating and inspecting short clips before scaling to 15 minutes.".to_string(),
        ],
        ObjectiveKind::Generic => vec![
            "The objective should be refined with evidence before implementation agents run.".to_string(),
            "Near-term tasks may include foundation repair if the current system blocks progress.".to_string(),
        ],
    }
}

fn success_metrics(kind: ObjectiveKind) -> Vec<String> {
    match kind {
        ObjectiveKind::Scheduler => vec![
            "A persisted campaign bundle includes objective, stages, tasks, resource preview, and approval checkpoint.".to_string(),
            "A scheduler dry run can explain run/pause/resume decisions without spending scarce resources.".to_string(),
            "Tests cover resource lease boundaries and resume behavior before rollout.".to_string(),
        ],
        ObjectiveKind::Minecraft => vec![
            "Controlled setup launches or is described with reproducible metadata.".to_string(),
            "Observation/action evidence is saved for at least one bounded interaction.".to_string(),
            "Stage scorecards improve over repeated controlled attempts without unsafe public automation.".to_string(),
        ],
        ObjectiveKind::Video => vec![
            "At least one short generated clip is saved with prompt, provider, and cost metadata.".to_string(),
            "Generated clips can be inspected and scored against scene intent.".to_string(),
            "A composition path assembles selected clips into a longer draft with QA artifacts.".to_string(),
        ],
        ObjectiveKind::Generic => vec![
            "Each stage has falsifiable metrics and required artifacts.".to_string(),
            "Near-term tasks are small enough that failure teaches something useful.".to_string(),
            "Every candidate has evidence before approval or rollout.".to_string(),
        ],
    }
}

fn failure_conditions(kind: ObjectiveKind, reserves: &[ReserveFloor]) -> Vec<String> {
    let mut conditions = vec![
        "A proposed stage lacks explicit acceptance metrics or required evidence.".to_string(),
        "The campaign dispatches agents or spends scarce resources before approval.".to_string(),
        "The proposal cannot be inspected or resumed from disk.".to_string(),
    ];
    if reserves.is_empty() {
        conditions.push(
            "Resource reserve floors are not declared or reviewed before scheduled execution."
                .to_string(),
        );
    } else {
        conditions.push(format!(
            "A run would violate reserve floors: {}.",
            reserves
                .iter()
                .map(|reserve| format!("{}:{}%", reserve.resource, reserve.floor_percent))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    match kind {
        ObjectiveKind::Minecraft => conditions
            .push("The campaign attempts public-server or account-risk behavior.".to_string()),
        ObjectiveKind::Video => conditions.push(
            "The campaign uses paid video API capacity or publishes artifacts without approval."
                .to_string(),
        ),
        _ => {}
    }
    conditions
}

fn safety_notes(kind: ObjectiveKind) -> Vec<String> {
    let mut notes = vec![
        "Human approval is required before scheduled execution.".to_string(),
        "Implementation agents may propose better next tasks but must attach evidence.".to_string(),
    ];
    if kind == ObjectiveKind::Minecraft {
        notes.push(
            "Keep game automation in controlled local fixtures with an emergency stop path."
                .to_string(),
        );
    }
    notes
}

fn stages_for_objective(kind: ObjectiveKind) -> Vec<EvolutionStage> {
    let stage_specs: Vec<(&str, &str, &str)> = match kind {
        ObjectiveKind::Scheduler => vec![
            (
                "proposal-completeness",
                "Proposal completeness",
                "Persist an inspectable campaign bundle with objective, stages, tasks, budget preview, decisions, and approval state.",
            ),
            (
                "resource-ledger-dry-run",
                "Resource ledger dry run",
                "Preview leases, reserve floors, reset windows, and pause/resume decisions without spending scarce resources.",
            ),
            (
                "resume-loop",
                "Pause/resume loop",
                "Demonstrate that a campaign can pause for budget, resume after a reset, and explain the transition.",
            ),
            (
                "rollout-readiness",
                "Rollout readiness",
                "Attach verification and rollout evidence before any scheduler change is promoted.",
            ),
        ],
        ObjectiveKind::Minecraft => vec![
            (
                "controlled-setup",
                "Controlled setup",
                "Establish a reproducible local environment for attempts.",
            ),
            (
                "baseline-observation-action",
                "Baseline observation/action",
                "Demonstrate observation and one bounded action or equivalent controlled interaction.",
            ),
            (
                "interaction-trace",
                "Interaction trace",
                "Record enough evidence to diagnose why attempts succeed or fail.",
            ),
            (
                "primitive-tasks",
                "Primitive tasks",
                "Complete small tasks in controlled fixtures before route-level objectives.",
            ),
        ],
        ObjectiveKind::Video => vec![
            (
                "single-clip",
                "Single clip generation",
                "Generate and save one short clip with metadata.",
            ),
            (
                "clip-inspection",
                "Clip inspection",
                "Inspect generated clips through frames, metadata, or another practical method.",
            ),
            (
                "scene-batch",
                "Scene batch",
                "Generate several clips from a scene plan and select or retry weak outputs.",
            ),
            (
                "composition",
                "Composition",
                "Assemble selected clips into a draft artifact with QA evidence.",
            ),
        ],
        ObjectiveKind::Generic => vec![
            (
                "objective-refinement",
                "Objective refinement",
                "Turn the raw objective into falsifiable stages and metrics.",
            ),
            (
                "baseline",
                "Baseline evidence",
                "Measure or estimate the current state before implementation.",
            ),
            (
                "first-candidate",
                "First candidate",
                "Produce one bounded candidate or task-replacement proposal with evidence.",
            ),
            (
                "reliability",
                "Reliability",
                "Repeat the workflow with acceptable cost, quality, runtime, and no regression.",
            ),
        ],
    };

    stage_specs
        .into_iter()
        .enumerate()
        .map(|(index, (id, name, purpose))| EvolutionStage {
            stage_id: format!("stage-{:02}-{id}", index + 1),
            name: name.to_string(),
            purpose: purpose.to_string(),
            baseline: "not measured in phase 0; first scheduled iteration must capture or justify baseline".to_string(),
            acceptance_metrics: vec![
                "Required artifacts are persisted in the campaign bundle.".to_string(),
                "A human can inspect pass/fail evidence for this stage.".to_string(),
                "Regression gates from earlier stages remain green or explicitly waived by policy.".to_string(),
            ],
            regression_gates: vec!["campaign bundle remains readable".to_string(), "resource policy remains satisfied".to_string()],
            required_artifacts: vec!["stage scorecard".to_string(), "evidence summary".to_string()],
            allowed_task_types: vec!["patch".to_string(), "evaluator".to_string(), "instrumentation".to_string(), "foundation-repair".to_string(), "task-replacement-proposal".to_string()],
            eval_suite: vec!["phase-0 manual proposal inspection".to_string()],
            estimated_budget: "small planning/model budget; no scarce execution until approved".to_string(),
            promotion_policy: "Promote only after all acceptance metrics pass and required artifacts exist.".to_string(),
            failure_conditions: vec![
                "No measurable acceptance metric is attached.".to_string(),
                "Required artifacts are missing or not linked.".to_string(),
            ],
        })
        .collect()
}

fn tasks_for_objective(
    kind: ObjectiveKind,
    stages: &[EvolutionStage],
    project: &EvolutionProjectProfile,
) -> Vec<NearTermEvolutionTask> {
    stages
        .iter()
        .take(3)
        .enumerate()
        .map(|(index, stage)| NearTermEvolutionTask {
            task_id: format!("task-{:02}-{}", index + 1, stage.stage_id.trim_start_matches("stage-")),
            stage_id: stage.stage_id.clone(),
            title: match (kind, index) {
                (ObjectiveKind::Scheduler, 0) => "Persist phase-0 campaign proposals".to_string(),
                (ObjectiveKind::Scheduler, 1) => "Design resource ledger dry-run scorecard".to_string(),
                (ObjectiveKind::Scheduler, _) => "Add pause/resume state transition tests".to_string(),
                _ => format!("Advance {}", stage.name),
            },
            objective: format!("Make progress on stage `{}` while preserving evidence and approval gates.", stage.name),
            rationale: "Selected because it is small, inspectable, and failure would produce useful adaptation evidence.".to_string(),
            required_evidence: vec!["changed artifacts or scorecard".to_string(), "decision rationale".to_string(), "verification output".to_string()],
            verification_commands: default_campaign_task_verification_commands(project),
            estimated_budget: "one focused implementation iteration after approval".to_string(),
            approval_required: true,
            expected_result_kind: if kind == ObjectiveKind::Scheduler && index == 1 {
                EvolutionResultKind::Evaluator
            } else if kind == ObjectiveKind::Scheduler && index == 2 {
                EvolutionResultKind::FoundationRepair
            } else {
                EvolutionResultKind::Patch
            },
        })
        .collect()
}

fn default_campaign_task_verification_commands(project: &EvolutionProjectProfile) -> Vec<String> {
    match project.kind {
        EvolutionProjectKind::Sinomo => vec!["cargo test --workspace".to_string()],
        EvolutionProjectKind::Rust => vec!["cargo check".to_string()],
        EvolutionProjectKind::Node
        | EvolutionProjectKind::Python
        | EvolutionProjectKind::Generic => vec!["git diff --check".to_string()],
    }
}

fn budget_notes(request: &CampaignProposalRequest) -> Vec<String> {
    let mut notes = vec![
        "Phase 0 only previews budget policy; Phase 1 must enforce leases.".to_string(),
        "Free and prepaid resources should be spread across reset windows instead of spent immediately.".to_string(),
    ];
    if request.reserve_floors.is_empty() {
        notes.push(
            "No reserve floors were provided; require human review before scheduled execution."
                .to_string(),
        );
    }
    if let Some(max_wall_time) = &request.max_wall_time {
        notes.push(format!("max wall time: {max_wall_time}"));
    }
    if request.pause_after_proposal {
        notes.push("pause after proposal before scheduling implementation agents".to_string());
    }
    notes
}

fn resource_schedule_preview(request: &CampaignProposalRequest) -> ResourceSchedulePreview {
    let entries = if request.reserve_floors.is_empty() {
        vec![ResourceScheduleEntry {
            resource: request.budget_policy.clone(),
            window: request.horizon.clone(),
            planned_use: "proposal only; no agent dispatch".to_string(),
            reserve_floor: "unconfigured".to_string(),
            notes: "Add reserve floors before scheduled execution.".to_string(),
        }]
    } else {
        request
            .reserve_floors
            .iter()
            .map(|reserve| ResourceScheduleEntry {
                resource: reserve.resource.clone(),
                window: request.horizon.clone(),
                planned_use: "proposal only now; later iterations must request leases before use"
                    .to_string(),
                reserve_floor: format!("{}%", reserve.floor_percent),
                notes: "Do not schedule work that would cross this reserve floor.".to_string(),
            })
            .collect()
    };
    ResourceSchedulePreview {
        horizon: request.horizon.clone(),
        entries,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionReport {
    pub job: EvolutionJob,
    pub candidate_workspace: CandidateWorkspace,
    pub bundle: CandidateBundleManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard: Option<EvaluationScorecard>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateWorkspace {
    pub job_id: EvolutionJobId,
    pub path: PathBuf,
    pub branch_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

pub trait WorkspaceAdapter {
    fn root(&self) -> &Path;
    fn prepare_candidate(&self, job: &EvolutionJob) -> Result<CandidateWorkspace, WorkspaceError>;
    fn changed_files(&self, workspace: &CandidateWorkspace) -> Result<Vec<String>, WorkspaceError>;
    fn diff(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError>;
    fn status(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError>;
}

#[derive(Clone, Debug)]
pub struct GitWorkspaceAdapter {
    source_root: PathBuf,
    worktrees_root: PathBuf,
}

impl GitWorkspaceAdapter {
    pub fn new(source_root: impl Into<PathBuf>, worktrees_root: impl Into<PathBuf>) -> Self {
        Self {
            source_root: source_root.into(),
            worktrees_root: worktrees_root.into(),
        }
    }

    fn run_git(&self, args: &[String], cwd: &Path) -> Result<String, WorkspaceError> {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .map_err(|source| WorkspaceError::Git {
                command: format!("git {}", args.join(" ")),
                detail: source.to_string(),
            })?;
        if !output.status.success() {
            return Err(WorkspaceError::Git {
                command: format!("git {}", args.join(" ")),
                detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

fn absolute_workspace_path(path: &Path) -> Result<PathBuf, WorkspaceError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|source| WorkspaceError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

impl WorkspaceAdapter for GitWorkspaceAdapter {
    fn root(&self) -> &Path {
        &self.source_root
    }

    fn prepare_candidate(&self, job: &EvolutionJob) -> Result<CandidateWorkspace, WorkspaceError> {
        let source_root = absolute_workspace_path(&self.source_root)?;
        let worktrees_root = absolute_workspace_path(&self.worktrees_root)?;
        fs::create_dir_all(&worktrees_root).map_err(|source| WorkspaceError::Io {
            path: worktrees_root.display().to_string(),
            detail: source.to_string(),
        })?;
        let source_revision =
            self.run_git(&["rev-parse".to_string(), "HEAD".to_string()], &source_root)?;
        let branch_name = format!("codex/evolve/{}", job.job_id.as_str());
        let path = worktrees_root.join(job.job_id.as_str()).join("worktree");
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|source| WorkspaceError::Io {
                    path: parent.display().to_string(),
                    detail: source.to_string(),
                })?;
            }
            self.run_git(
                &[
                    "worktree".to_string(),
                    "add".to_string(),
                    "-B".to_string(),
                    branch_name.clone(),
                    path.display().to_string(),
                    source_revision.clone(),
                ],
                &source_root,
            )?;
        }

        Ok(CandidateWorkspace {
            job_id: job.job_id.clone(),
            path,
            branch_name,
            source_revision: Some(source_revision),
        })
    }

    fn changed_files(&self, workspace: &CandidateWorkspace) -> Result<Vec<String>, WorkspaceError> {
        parse_git_status_paths(&self.status(workspace)?).map_err(WorkspaceError::StatusParse)
    }

    fn diff(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
        let tracked = self.run_git(
            &[
                "diff".to_string(),
                "--binary".to_string(),
                "HEAD".to_string(),
                "--".to_string(),
            ],
            &workspace.path,
        )?;
        let status = self.status(workspace)?;
        let mut diff = tracked;
        for path in parse_git_status_paths(&status).map_err(WorkspaceError::StatusParse)? {
            if is_untracked_status_path(&status, &path) {
                let file_path = workspace.path.join(&path);
                if file_path.is_file() {
                    let content =
                        fs::read_to_string(&file_path).map_err(|source| WorkspaceError::Io {
                            path: file_path.display().to_string(),
                            detail: source.to_string(),
                        })?;
                    if !diff.is_empty() {
                        diff.push_str("\n\n");
                    }
                    diff.push_str(&format!(
                        "diff --git a/{path} b/{path}\nnew file mode 100644\n--- /dev/null\n+++ b/{path}\n"
                    ));
                    for line in content.lines() {
                        diff.push('+');
                        diff.push_str(line);
                        diff.push('\n');
                    }
                }
            }
        }
        Ok(diff)
    }

    fn status(&self, workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
        self.run_git(
            &["status".to_string(), "--porcelain".to_string()],
            &workspace.path,
        )
    }
}

pub fn commit_and_fast_forward_approved_candidate(
    record: &StoredEvolutionJob,
) -> Result<(), WorkspaceError> {
    let worktree = &record.candidate_workspace.path;
    run_git_command(worktree, &["add", "-A"])?;
    let status = run_git_command(worktree, &["status", "--porcelain"])?;
    if !status.trim().is_empty() {
        run_git_command(
            worktree,
            &[
                "commit",
                "-m",
                &format!("Apply evolution job {}", record.job.job_id),
            ],
        )?;
    }
    let source_root = Path::new(&record.job.edit_scope.workspace_root);
    run_git_command(
        source_root,
        &[
            "merge",
            "--ff-only",
            &record.candidate_workspace.branch_name,
        ],
    )?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvolutionCleanupSummary {
    pub job_id: EvolutionJobId,
    pub worktree_path: PathBuf,
    pub job_dir: PathBuf,
    pub bundle_path: Option<PathBuf>,
}

pub fn cleanup_stored_evolution_job(
    paths: &EvolutionPaths,
    record: &StoredEvolutionJob,
) -> Result<EvolutionCleanupSummary, WorkspaceError> {
    let worktree_path = record.candidate_workspace.path.clone();
    if worktree_path.exists() {
        let output = Command::new("git")
            .args(["worktree", "remove", "--force"])
            .arg(&worktree_path)
            .current_dir(Path::new(&record.job.edit_scope.workspace_root))
            .output();
        match output {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                fs::remove_dir_all(&worktree_path).map_err(|source| WorkspaceError::Io {
                    path: worktree_path.display().to_string(),
                    detail: format!(
                        "git worktree remove failed: {}; remove_dir_all failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim(),
                        source
                    ),
                })?;
            }
            Err(error) => {
                fs::remove_dir_all(&worktree_path).map_err(|source| WorkspaceError::Io {
                    path: worktree_path.display().to_string(),
                    detail: format!(
                        "git worktree remove could not start: {}; remove_dir_all failed: {}",
                        error, source
                    ),
                })?;
            }
        }
    }

    let job_dir = paths.job_dir(&record.job.job_id);
    if job_dir.exists() {
        fs::remove_dir_all(&job_dir).map_err(|source| WorkspaceError::Io {
            path: job_dir.display().to_string(),
            detail: source.to_string(),
        })?;
    }

    let bundle_path = PathBuf::from(&record.bundle.bundle_path);
    let removed_bundle_path = if bundle_path.exists() && bundle_path != job_dir {
        fs::remove_dir_all(&bundle_path).map_err(|source| WorkspaceError::Io {
            path: bundle_path.display().to_string(),
            detail: source.to_string(),
        })?;
        Some(bundle_path)
    } else {
        None
    };

    Ok(EvolutionCleanupSummary {
        job_id: record.job.job_id.clone(),
        worktree_path,
        job_dir,
        bundle_path: removed_bundle_path,
    })
}

fn run_git_command(cwd: &Path, args: &[&str]) -> Result<String, WorkspaceError> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|source| WorkspaceError::Git {
            command: format!("git {}", args.join(" ")),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(WorkspaceError::Git {
            command: format!("git {}", args.join(" ")),
            detail: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn parse_git_status_paths(status: &str) -> Result<Vec<String>, String> {
    let mut paths = Vec::new();
    for line in status.lines().filter(|line| !line.trim().is_empty()) {
        let bytes = line.as_bytes();
        let path_start = if bytes.get(1).is_some_and(|byte| byte.is_ascii_whitespace()) {
            2
        } else {
            3
        };
        if line.len() < path_start {
            return Err(format!("invalid porcelain status line: {line}"));
        }
        let path = line[path_start..].trim();
        let path = path
            .rsplit_once(" -> ")
            .map(|(_, new_path)| new_path)
            .unwrap_or(path)
            .trim_matches('"')
            .to_string();
        if !path.is_empty() {
            paths.push(path);
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn is_untracked_status_path(status: &str, path: &str) -> bool {
    status
        .lines()
        .any(|line| line.starts_with("?? ") && line.get(3..).map(str::trim) == Some(path))
}

pub trait CandidateBundleStore {
    fn create(&self, job: &EvolutionJob) -> Result<CandidateBundleManifest, BundleError>;
    fn write_json<T: Serialize>(
        &self,
        manifest: &CandidateBundleManifest,
        name: &str,
        value: &T,
    ) -> Result<(), BundleError>;
    fn write_text(
        &self,
        manifest: &CandidateBundleManifest,
        name: &str,
        value: &str,
    ) -> Result<(), BundleError>;
}

#[derive(Clone, Debug)]
pub struct LocalCandidateBundleStore {
    candidates_root: PathBuf,
}

impl LocalCandidateBundleStore {
    pub fn new(candidates_root: impl Into<PathBuf>) -> Self {
        Self {
            candidates_root: candidates_root.into(),
        }
    }

    pub fn path_for(&self, manifest: &CandidateBundleManifest, name: &str) -> PathBuf {
        PathBuf::from(&manifest.bundle_path).join(name)
    }

    pub fn write_manifest(&self, manifest: &CandidateBundleManifest) -> Result<(), BundleError> {
        self.write_json(manifest, "manifest.json", manifest)
    }

    pub fn write_command_log(
        &self,
        manifest: &CandidateBundleManifest,
        result: &CommandRunResult,
    ) -> Result<CommandLogRef, BundleError> {
        let safe_name = bundle_safe_file_stem(&result.name);
        let path = format!("command-logs/{safe_name}.txt");
        let text = format!(
            "name: {}\ncommand: {}\nexpected_exit_code: {}\nexit_code: {}\nduration_ms: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
            result.name,
            result.command,
            result.expected_exit_code,
            result.exit_code,
            result.duration_ms,
            result.stdout,
            result.stderr
        );
        self.write_text(manifest, &path, &text)?;
        Ok(CommandLogRef {
            name: result.name.clone(),
            command: result.command.clone(),
            log_path: self.path_for(manifest, &path).display().to_string(),
            exit_code: Some(result.exit_code),
        })
    }
}

impl CandidateBundleStore for LocalCandidateBundleStore {
    fn create(&self, job: &EvolutionJob) -> Result<CandidateBundleManifest, BundleError> {
        let bundle_path = self.candidates_root.join(job.job_id.as_str());
        fs::create_dir_all(bundle_path.join("command-logs")).map_err(|source| BundleError::Io {
            path: bundle_path.display().to_string(),
            detail: source.to_string(),
        })?;
        let manifest = CandidateBundleManifest {
            job_id: job.job_id.clone(),
            bundle_path: bundle_path.display().to_string(),
            created_at: OffsetDateTime::now_utc(),
            mutation_packet_path: bundle_path
                .join("mutation-packet.json")
                .display()
                .to_string(),
            job_path: Some(bundle_path.join("job.json").display().to_string()),
            model_output_path: None,
            agent_run_path: None,
            agent_summary_path: None,
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: None,
            attestation_path: None,
            registry_record_path: None,
            bundle_content_hash: None,
            changed_files_path: None,
            diff_path: None,
            scorecard_path: None,
            candidate_summary_path: None,
            decision_path: None,
            artifact_refs: Vec::new(),
            command_logs: Vec::new(),
        };
        self.write_manifest(&manifest)?;
        Ok(manifest)
    }

    fn write_json<T: Serialize>(
        &self,
        manifest: &CandidateBundleManifest,
        name: &str,
        value: &T,
    ) -> Result<(), BundleError> {
        let path = self.path_for(manifest, name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| BundleError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        let content = serde_json::to_string_pretty(value)
            .map_err(|source| BundleError::Serialize(source.to_string()))?;
        fs::write(&path, content).map_err(|source| BundleError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    }

    fn write_text(
        &self,
        manifest: &CandidateBundleManifest,
        name: &str,
        value: &str,
    ) -> Result<(), BundleError> {
        let path = self.path_for(manifest, name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| BundleError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&path, value).map_err(|source| BundleError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    }
}

pub trait MutationModel {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
        bundle: Option<&CandidateBundleManifest>,
    ) -> Result<ModelMutationResult, MutationModelError>;
}

use crate::tools::{
    EvolutionToolCall, EvolutionToolDescriptor, EvolutionToolRegistry, EvolutionToolResult,
    ToolCallStatus,
};
#[cfg(test)]
use crate::tools::{EvolutionToolEventContext, ToolEvidence};

#[cfg(windows)]
pub(crate) fn shell_command(command: &str) -> Command {
    let mut shell = Command::new("powershell");
    shell.args(["-NoProfile", "-NonInteractive", "-Command", command]);
    shell
}

#[cfg(not(windows))]
pub(crate) fn shell_command(command: &str) -> Command {
    let mut shell = Command::new("sh");
    shell.args(["-c", command]);
    shell
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModelMutationResult {
    pub provider: String,
    pub model: String,
    pub raw_output: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parsed_patch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<StructuredMutationOutput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<EvolutionToolResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_turns: Vec<MutationModelTurn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_summary: Option<AgentMutationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_resource_usage: Option<CandidateResourceUsageSummary>,
    #[serde(skip)]
    pub agent_run: Option<serde_json::Value>,
    #[serde(skip)]
    pub agent_events: Vec<serde_json::Value>,
    #[serde(skip)]
    pub coding_run_state: Option<serde_json::Value>,
    #[serde(skip)]
    pub patch_snapshots: Vec<serde_json::Value>,
    #[serde(skip)]
    pub validation_records: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentMutationSummary {
    pub stop_reason: String,
    pub model_turns: usize,
    pub tool_results: usize,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_metrics: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_pair_integrity: Option<AgentToolPairIntegrity>,
    pub final_text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MutationModelTurn {
    pub turn: usize,
    pub raw_output: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<StructuredMutationOutput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<EvolutionToolResult>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct StructuredMutationOutput {
    #[serde(default, alias = "changedFiles")]
    pub changed_files: Vec<String>,
    #[serde(default, alias = "toolCalls")]
    pub tool_calls: Vec<EvolutionToolCall>,
    #[serde(default, alias = "unified_diff")]
    pub patch: Option<String>,
    #[serde(default, alias = "patchSummary")]
    pub patch_summary: Option<String>,
    #[serde(
        default,
        alias = "verificationNotes",
        deserialize_with = "deserialize_optional_note"
    )]
    pub verification_notes: Option<String>,
    #[serde(
        default,
        alias = "experimentNotes",
        deserialize_with = "deserialize_optional_note"
    )]
    pub experiment_notes: Option<String>,
    #[serde(
        default,
        alias = "recoveryNotes",
        deserialize_with = "deserialize_optional_note"
    )]
    pub recovery_notes: Option<String>,
}

fn deserialize_optional_note<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(note) => Ok(Some(note)),
        serde_json::Value::Array(items) => {
            let notes: Vec<String> = items
                .into_iter()
                .filter_map(|item| match item {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(note) => Some(note),
                    other => Some(other.to_string()),
                })
                .collect();
            if notes.is_empty() {
                Ok(None)
            } else {
                Ok(Some(notes.join("\n")))
            }
        }
        other => Ok(Some(other.to_string())),
    }
}

pub struct LanguageModelMutationBridge<P> {
    provider: P,
    model: String,
    tool_registry: EvolutionToolRegistry,
    local_permission_data_dir: Option<SinomoDataDir>,
    max_tool_rounds: usize,
}

impl<P> LanguageModelMutationBridge<P> {
    pub fn new(provider: P, model: impl Into<String>) -> Self {
        Self {
            provider,
            model: model.into(),
            tool_registry: EvolutionToolRegistry::bootstrap(),
            local_permission_data_dir: None,
            max_tool_rounds: 4,
        }
    }

    pub fn with_tool_registry(mut self, tool_registry: EvolutionToolRegistry) -> Self {
        self.tool_registry = tool_registry;
        self
    }

    /// Constructs the bridge with node-local consent available to effectful
    /// tools. The ordinary constructor remains fail-closed for callers that do
    /// not have an authoritative local data directory.
    pub fn new_for_data_dir(
        provider: P,
        model: impl Into<String>,
        data_dir: &SinomoDataDir,
    ) -> Self {
        let mut bridge = Self::new(provider, model);
        bridge.tool_registry = EvolutionToolRegistry::bootstrap_for_data_dir(data_dir.clone());
        bridge.local_permission_data_dir = Some(data_dir.clone());
        bridge
    }

    pub fn with_max_tool_rounds(mut self, max_tool_rounds: usize) -> Self {
        self.max_tool_rounds = max_tool_rounds;
        self
    }

    fn authorize_provider_host_access(
        &self,
        info: &LanguageModelProviderInfo,
    ) -> Result<(), MutationModelError> {
        if !info.host_access.requires_remote_management_permission() {
            return Ok(());
        }

        let Some(data_dir) = self.local_permission_data_dir.as_ref() else {
            return Err(self.provider_permission_denied(
                info,
                "no authoritative node-local permission context is configured",
            ));
        };
        let store = LocalNodePermissionStore::new(data_dir.root());
        let granted = store
            .try_permission_granted(LocalNodePermission::RemoteManagement)
            .map_err(|error| {
                self.provider_permission_denied(
                    info,
                    format!("could not verify node-local permission: {error}"),
                )
            })?;
        if !granted {
            return Err(self.provider_permission_denied(
                info,
                "grant `remote-management` locally on this node",
            ));
        }
        Ok(())
    }

    fn provider_permission_denied(
        &self,
        info: &LanguageModelProviderInfo,
        detail: impl std::fmt::Display,
    ) -> MutationModelError {
        MutationModelError::LanguageModel(LanguageModelError::ProviderInvocationFailed {
            provider: info.provider.clone(),
            model: self.model.clone(),
            detail: format!(
                "provider requires node-local permission `remote-management` for unrestricted host access; {detail}"
            ),
        })
    }
}

impl<P: LanguageModelProvider> MutationModel for LanguageModelMutationBridge<P> {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
        _bundle: Option<&CandidateBundleManifest>,
    ) -> Result<ModelMutationResult, MutationModelError> {
        let info = self.provider.info();
        let direct_edit =
            info.supports_tool_permissions && info.invocation == LanguageModelInvocation::Process;
        let structured_response = !direct_edit;
        let response_format = if structured_response && info.supports_json_object {
            LanguageModelResponseFormat::JsonObject
        } else {
            LanguageModelResponseFormat::Text
        };
        let descriptors = self.tool_registry.descriptors();
        let mut messages = vec![
            LanguageModelMessage {
                role: LanguageModelRole::System,
                content: render_mutation_system_prompt(structured_response, direct_edit),
            },
            LanguageModelMessage {
                role: LanguageModelRole::User,
                content: render_mutation_bridge_prompt(
                    packet,
                    workspace,
                    &descriptors,
                    structured_response,
                    direct_edit,
                ),
            },
        ];
        let mut model_turns = Vec::new();
        let mut all_tool_results = Vec::new();
        let mut final_response = None;

        for turn in 1..=self.max_tool_rounds.max(1) {
            let request = LanguageModelRequest {
                provider: info.provider.clone(),
                model: self.model.clone(),
                messages: messages.clone(),
                items: Vec::new(),
                tools: Vec::new(),
                tool_choice: None,
                parallel_tool_calls: None,
                temperature: Some(0.2),
                max_output_tokens: None,
                response_format: response_format.clone(),
                metadata: serde_json::json!({
                    "usage_attribution": sinomo_runtime::api::app::usage::UsageAttribution{origin:"evolution".into(),purpose:"mutation".into(),task:Some(packet.job_id.to_string()),operation:Some(packet.job_id.to_string()),..Default::default()},
                    "job_id": packet.job_id,
                    "candidate_workspace": workspace.path,
                    "direct_edit": direct_edit,
                    "target_ring_policy": "ignored-during-bootstrap",
                    "turn": turn,
                    "max_tool_rounds": self.max_tool_rounds,
                }),
            };
            // This check belongs immediately at the provider effect boundary,
            // not bridge construction: a local operator can revoke consent
            // between model/tool rounds and the next host process must stop.
            self.authorize_provider_host_access(&info)?;
            let response = sinomo_lm::usage::complete_provider(
                self.local_permission_data_dir.as_ref(),
                &self.provider,
                &request,
            )?;
            let structured_output = parse_structured_mutation_output(&response.content);
            let tool_calls = if direct_edit {
                Vec::new()
            } else {
                structured_output
                    .as_ref()
                    .map(|output| output.tool_calls.clone())
                    .unwrap_or_default()
            };
            let mut turn_tool_results = Vec::new();
            for call in tool_calls {
                let result = self.tool_registry.execute(workspace, call);
                turn_tool_results.push(result.clone());
                all_tool_results.push(result);
            }
            model_turns.push(MutationModelTurn {
                turn,
                raw_output: response.content.clone(),
                structured_output: structured_output.clone(),
                tool_results: turn_tool_results.clone(),
            });

            let has_final_patch = structured_output
                .as_ref()
                .and_then(|output| output.patch.as_ref())
                .is_some_and(|patch| !patch.trim().is_empty());
            let has_text_patch = extract_patch_from_text(&response.content).is_some();
            if !direct_edit && !turn_tool_results.is_empty() && !has_final_patch && !has_text_patch
            {
                if turn >= self.max_tool_rounds.max(1) {
                    return Err(MutationModelError::ToolRoundLimitExceeded {
                        max_rounds: self.max_tool_rounds,
                    });
                }
                messages.push(LanguageModelMessage {
                    role: LanguageModelRole::Assistant,
                    content: response.content,
                });
                messages.push(LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: render_tool_results_followup(&turn_tool_results),
                });
                continue;
            } else {
                final_response = Some(response);
                break;
            }
        }

        let response = final_response.ok_or(MutationModelError::ToolRoundLimitExceeded {
            max_rounds: self.max_tool_rounds,
        })?;
        let structured_output = parse_structured_mutation_output(&response.content);
        let parsed_patch = structured_output
            .as_ref()
            .and_then(|output| output.patch.clone())
            .or_else(|| extract_patch_from_text(&response.content));
        if !direct_edit
            && let Some(patch) = parsed_patch
                .as_ref()
                .filter(|patch| !patch.trim().is_empty())
        {
            let result = self.tool_registry.execute(
                workspace,
                EvolutionToolCall {
                    tool: "apply_patch".to_string(),
                    input: serde_json::json!({ "patch": patch }),
                },
            );
            if result.status != ToolCallStatus::Succeeded {
                return Err(MutationModelError::ToolExecution {
                    tool: result.tool,
                    detail: result.evidence.summary,
                });
            }
            all_tool_results.push(result);
        }
        let summary = structured_output
            .as_ref()
            .and_then(|output| output.patch_summary.clone())
            .or_else(|| first_non_empty_line(&response.content));
        Ok(ModelMutationResult {
            provider: response.provider,
            model: response.model,
            raw_output: response.content,
            parsed_patch,
            summary,
            structured_output,
            tool_results: all_tool_results,
            model_turns,
            agent_summary: None,
            agent_resource_usage: None,
            agent_run: None,
            agent_events: Vec::new(),
            coding_run_state: None,
            patch_snapshots: Vec::new(),
            validation_records: Vec::new(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct EvolutionLanguageModelMutationModel {
    pub provider: String,
    pub model: String,
    pub data_dir: SinomoDataDir,
}

impl MutationModel for EvolutionLanguageModelMutationModel {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
        _bundle: Option<&CandidateBundleManifest>,
    ) -> Result<ModelMutationResult, MutationModelError> {
        match self.provider.as_str() {
            GitHubCopilotProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
                let default_config = GitHubCopilotConfig::default();
                let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
                    options: Default::default(),
                    client_id: default_config.client_id,
                    github_host: default_config.github_host,
                    api_base_url: default_config.api_base_url,
                    default_model: model_or_default(
                        &self.model,
                        GitHubCopilotConfig::default().default_model,
                    ),
                    token_store_path: Some(credentials_path),
                });
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            GitHubCopilotCliProvider::PROVIDER_ID => {
                let provider = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
                    model: model_or_default(&self.model, GitHubCopilotCliConfig::default().model),
                    ..GitHubCopilotCliConfig::default()
                });
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            CodexCliProvider::PROVIDER_ID => {
                let provider = CodexCliProvider::new(CodexCliConfig {
                    model: model_or_default(&self.model, CODEX_CLI_DEFAULT_MODEL.to_string()),
                    working_dir: Some(workspace.path.clone()),
                    ..CodexCliConfig::default()
                });
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            CODEX_PROVIDER_ID => {
                let provider = codex_provider_for_model(&self.model);
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            ClaudeCodeProvider::PROVIDER_ID | "anthropic-claude-code" => {
                let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
                    model: model_or_default(&self.model, CLAUDE_CODE_DEFAULT_MODEL.to_string()),
                    working_dir: Some(workspace.path.clone()),
                    ..ClaudeCodeConfig::default()
                });
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            AnthropicProvider::PROVIDER_ID | "claude" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(AnthropicProvider::PROVIDER_ID);
                let provider = AnthropicProvider::new(AnthropicConfig {
                    default_model: model_or_default(
                        &self.model,
                        AnthropicConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..AnthropicConfig::default()
                });
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            OpenAiProvider::PROVIDER_ID | "openai-api" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(OpenAiProvider::PROVIDER_ID);
                let provider = OpenAiProvider::new(OpenAiConfig {
                    default_model: model_or_default(
                        &self.model,
                        OpenAiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..OpenAiConfig::default()
                });
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            GeminiProvider::PROVIDER_ID | "google-gemini" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GeminiProvider::PROVIDER_ID);
                let provider = GeminiProvider::new(GeminiConfig {
                    default_model: model_or_default(
                        &self.model,
                        GeminiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..GeminiConfig::default()
                });
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            NvidiaProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(NvidiaProvider::PROVIDER_ID);
                let provider = NvidiaProvider::new(NvidiaConfig {
                    options: Default::default(),
                    api_base_url: NvidiaConfig::default().api_base_url,
                    default_model: model_or_default(
                        &self.model,
                        NvidiaConfig::default().default_model,
                    ),
                    api_key: None,
                    credentials_path: Some(credentials_path),
                });
                LanguageModelMutationBridge::new_for_data_dir(
                    provider,
                    self.model.clone(),
                    &self.data_dir,
                )
                .mutate(packet, workspace, None)
            }
            _ => Err(MutationModelError::LanguageModel(
                LanguageModelError::ProviderInvocationFailed {
                    provider: self.provider.clone(),
                    model: self.model.clone(),
                    detail: "unknown evolution model provider".to_string(),
                },
            )),
        }
    }
}

#[derive(Clone)]
pub struct EvolutionAgentMutationModel<'a> {
    pub provider: String,
    pub model: String,
    pub max_steps: Option<usize>,
    pub sessions: crate::SessionServices<'a>,
}

impl MutationModel for EvolutionAgentMutationModel<'_> {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
        bundle: Option<&CandidateBundleManifest>,
    ) -> Result<ModelMutationResult, MutationModelError> {
        use sinomo_module_api::sessions::*;
        let sessions = self.sessions.ok_or_else(|| MutationModelError::Agent {
            detail: "host session service is required for candidate execution".into(),
        })?;
        let call = |request| {
            sessions
                .request(&request)
                .map_err(|detail| MutationModelError::Agent { detail })
        };
        let started = call(SessionRequest::Start {
            worker: SessionWorkerRequest {
                agent_id: crate::EVOLUTION_IMPLEMENTER_AGENT_ID.into(),
                provider: self.provider.clone(),
                model: self.model.clone(),
                workspace: workspace.path.to_string_lossy().into_owned(),
                instructions: render_agent_mutation_prompt(packet, workspace),
                context: vec![
                    serde_json::to_value(evolution_agent_context_attachment(packet, bundle))
                        .map_err(|e| MutationModelError::Serialization {
                            detail: e.to_string(),
                        })?,
                ],
                max_steps: self.max_steps,
            },
        })?;
        let id = started["session_id"]
            .as_str()
            .ok_or_else(|| MutationModelError::Agent {
                detail: "host omitted session identity".into(),
            })?
            .to_string();
        loop {
            let state = call(SessionRequest::Command {
                command: SessionCommand::Wait {
                    id: id.clone(),
                    timeout_ms: 100,
                },
            })?;
            if state["subagent"]["status"] != "running" {
                break;
            }
        }
        let result = call(SessionRequest::Command {
            command: SessionCommand::Result { id },
        })?;
        let outcome: Option<sinomo_agent::AgentOpOutcome> = serde_json::from_value(result)
            .map_err(|e| MutationModelError::Serialization {
                detail: e.to_string(),
            })?;
        let run = outcome
            .and_then(|o| o.run)
            .ok_or_else(|| MutationModelError::Agent {
                detail: "host session finished without a run".into(),
            })?;
        agent_mutation_result(packet, &self.provider, &self.model, run)
    }
}

#[derive(Clone, Debug)]
pub struct AgentMutationWorker<M> {
    interaction_model: M,
    provider: String,
    model: String,
    local_permission_data_dir: Option<SinomoDataDir>,
    max_steps: Option<usize>,
}

impl<M> AgentMutationWorker<M> {
    pub fn new(
        interaction_model: M,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            interaction_model,
            provider: provider.into(),
            model: model.into(),
            local_permission_data_dir: None,
            max_steps: None,
        }
    }

    pub fn with_local_permission_data_dir(mut self, data_dir: SinomoDataDir) -> Self {
        self.local_permission_data_dir = Some(data_dir);
        self
    }

    pub fn with_max_steps(mut self, max_steps: Option<usize>) -> Self {
        self.max_steps = max_steps;
        self
    }
}

impl<M> MutationModel for AgentMutationWorker<M>
where
    M: InteractionModel + Clone + Send + Sync + 'static,
{
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
        bundle: Option<&CandidateBundleManifest>,
    ) -> Result<ModelMutationResult, MutationModelError> {
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_evolution_agent").expect("valid evolution node id"),
            InteractionSurface::new("evolution.mutation", InteractionSurfaceKind::CommandLine),
        );
        let storage_paths = AgentStoragePaths::new(agent_storage_root(workspace, bundle));
        let mut workspace_config = LocalWorkspaceAgentConfig::new(&workspace.path)
            .allowing_shell_commands(true)
            .allowing_file_writes(true)
            .with_agent_storage(&storage_paths, &session.session_id);
        workspace_config.max_read_bytes = 256 * 1024;
        workspace_config.max_command_output_bytes = 128 * 1024;
        workspace_config.max_search_matches = 300;
        // Candidate workspaces own an isolated command-sandbox store rather
        // than instances in the daemon's node-local sandbox store.
        let sandboxes = sinomo_runtime::sandbox::SandboxLifecycleClient::in_process_at_runtime_root(
            &workspace_config.sandbox_runtime_root,
        );
        sinomo_agent::local_workspace::provision_default_agent_session_sandbox(
            &sandboxes,
            &workspace_config,
            Vec::new(),
        )
        .map_err(|detail| MutationModelError::Agent {
            detail: format!("provision evolution-agent command sandbox: {detail}"),
        })?;

        let mut config = AgentConfig::new(self.provider.clone(), self.model.clone())
            .with_agent_definition(crate::evolution_implementer_agent_definition())
            .with_workspace_root(&workspace.path)
            .with_context_budget(AgentContextBudget::for_loaded_model(
                &self.provider,
                &self.model,
                None,
            ))
            .with_max_model_repair_attempts(2);
        if !config
            .visible_toolsets
            .iter()
            .any(|toolset| toolset == "coding_advanced")
        {
            config.visible_toolsets.push("coding_advanced".to_string());
        }
        if let Some(max_steps) = self.max_steps {
            config = config.with_max_steps(max_steps);
        }

        let tool_registry = AgentToolRegistry::local_workspace_with_config(workspace_config);
        if let Some(data_dir) = &self.local_permission_data_dir {
            tool_registry.set_local_permission_data_dir(data_dir.root());
            tool_registry.set_usage_data_dir(data_dir.clone());
            let memory_backend = sinomo_agent::SifsMemoryNoteBackend::new(
                data_dir,
                crate::EVOLUTION_IMPLEMENTER_AGENT_ID,
            )
            .map_err(|source| MutationModelError::Agent {
                detail: format!("configure evolution-agent Markdown memory notes: {source}"),
            })?;
            sinomo_agent::memory_notes::register_tools(
                &tool_registry,
                sinomo_agent::MemoryNoteAgentToolConfig::from_backend(memory_backend),
            );
        }

        let agent = Agent::new(
            self.interaction_model.clone(),
            self.provider.clone(),
            self.model.clone(),
        )
        .with_config(config)
        .with_tool_surface(AgentToolSurface::default_for_role(AgentRole::Deliberation))
        .with_tool_registry(tool_registry)
        .with_trace_store(FileAgentTraceStore::new(
            storage_paths.trace_path_for_session(&session.session_id),
        ));
        let prompt = render_agent_mutation_prompt(packet, workspace);
        let latest_event = user_interaction_input_event(&session, &prompt);
        let input = AgentLoopInput {
            session,
            latest_event,
            transient_system_context: None,
            async_update: None,
            context_attachments: vec![evolution_agent_context_attachment(packet, bundle)],
            tool_specs: evolution_agent_mutation_tools(self.local_permission_data_dir.is_some()),
        };
        let runtime = AgentSessionRuntime::new(
            agent,
            AgentSession::new(input.session.session_id.clone(), AgentRole::Deliberation),
        );
        let op = AgentOp::start_turn(input);
        let op_id = op.op_id.clone();
        let submit = runtime
            .submit_op(op)
            .map_err(|source| MutationModelError::Agent {
                detail: source.to_string(),
            })?;
        if !submit.accepted {
            let detail = submit
                .events
                .iter()
                .find_map(|event| match &event.payload {
                    sinomo_agent::AgentEventPayload::OperationRejected { reason, .. } => {
                        Some(reason.clone())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| "evolution agent turn was rejected".to_string());
            return Err(MutationModelError::Agent { detail });
        }
        runtime
            .join_active_turn()
            .map_err(|source| MutationModelError::Agent {
                detail: source.to_string(),
            })?;
        let run = runtime
            .turn_result(&op_id)
            .map_err(|source| MutationModelError::Agent {
                detail: source.to_string(),
            })?
            .and_then(|outcome| outcome.run)
            .ok_or_else(|| MutationModelError::Agent {
                detail: format!("agent runtime turn `{op_id}` completed without a run"),
            })?;
        agent_mutation_result(packet, &self.provider, &self.model, run)
    }
}

fn agent_mutation_result(
    packet: &MutationPacket,
    provider: &str,
    model: &str,
    run: sinomo_agent::AgentRun,
) -> Result<ModelMutationResult, MutationModelError> {
    let final_text = run.final_decision.text.clone();
    let (input_tokens, output_tokens) = agent_model_usage_totals(&run.model_events);
    let agent_summary = AgentMutationSummary {
        stop_reason: agent_stop_reason_label(&run.stop_reason),
        model_turns: run.model_turns.len(),
        tool_results: run.tool_results.len(),
        input_tokens,
        output_tokens,
        model_metrics: Some(serde_json::to_value(&run.model_metrics).map_err(|source| {
            MutationModelError::Serialization {
                detail: source.to_string(),
            }
        })?),
        tool_pair_integrity: Some(run.tool_pair_integrity()),
        final_text: final_text.clone(),
    };
    let agent_resource_usage = agent_resource_usage_summary(
        packet.job_id.clone(),
        input_tokens,
        output_tokens,
        run.model_turns.len(),
        run.tool_results.len(),
    );
    let agent_run =
        serde_json::to_value(&run).map_err(|source| MutationModelError::Serialization {
            detail: source.to_string(),
        })?;
    let agent_events = run
        .events
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| MutationModelError::Serialization {
            detail: source.to_string(),
        })?;
    let coding_run_state = run
        .coding_state
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|source| MutationModelError::Serialization {
            detail: source.to_string(),
        })?;
    let patch_snapshots = run
        .items
        .iter()
        .filter(|item| matches!(item, AgentItem::PatchSnapshot { .. }))
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| MutationModelError::Serialization {
            detail: source.to_string(),
        })?;
    let validation_records = run
        .coding_state
        .as_ref()
        .map(|state| {
            state
                .validation_attempts
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
        .map_err(|source| MutationModelError::Serialization {
            detail: source.to_string(),
        })?
        .unwrap_or_default();
    Ok(ModelMutationResult {
        provider: provider.to_string(),
        model: model.to_string(),
        raw_output: final_text.clone(),
        parsed_patch: None,
        summary: non_empty_string(final_text),
        structured_output: None,
        tool_results: Vec::new(),
        model_turns: Vec::new(),
        agent_summary: Some(agent_summary),
        agent_resource_usage: Some(agent_resource_usage),
        agent_run: Some(agent_run),
        agent_events,
        coding_run_state,
        patch_snapshots,
        validation_records,
    })
}

fn agent_storage_root(
    workspace: &CandidateWorkspace,
    bundle: Option<&CandidateBundleManifest>,
) -> PathBuf {
    if let Some(bundle) = bundle {
        return PathBuf::from(&bundle.bundle_path).join("agent");
    }
    workspace
        .path
        .parent()
        .unwrap_or(&workspace.path)
        .join("agent-artifacts")
        .join(workspace.job_id.as_str())
}

fn agent_stop_reason_label(reason: &sinomo_agent::AgentStopReason) -> String {
    serde_json::to_value(reason)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| format!("{reason:?}"))
}

fn evolution_agent_context_attachment(
    packet: &MutationPacket,
    bundle: Option<&CandidateBundleManifest>,
) -> AgentContextAttachment {
    AgentContextAttachment {
        kind: "sinomo.evolution.mutation".to_string(),
        summary: format!("mutation packet for evolution job {}", packet.job_id),
        payload_json: serde_json::json!({
            "job_id": &packet.job_id,
            "objective": &packet.objective,
            "target_ring": &packet.target_ring,
            "edit_scope": &packet.edit_scope,
            "verification": &packet.verification,
            "candidate_bundle": bundle,
        }),
        payload_original_bytes: None,
        payload_model_visible_truncated: false,
    }
}

fn agent_model_usage_totals(events: &[AgentModelEvent]) -> (u64, u64) {
    events
        .iter()
        .filter_map(|event| match event {
            AgentModelEvent::Usage {
                usage: Some(usage), ..
            } => Some(usage),
            _ => None,
        })
        .fold((0u64, 0u64), |(input, output), usage| {
            (
                input.saturating_add(usage.input_tokens.unwrap_or(0)),
                output.saturating_add(usage.output_tokens.unwrap_or(0)),
            )
        })
}

fn agent_resource_usage_summary(
    job_id: EvolutionJobId,
    input_tokens: u64,
    output_tokens: u64,
    model_turns: usize,
    tool_results: usize,
) -> CandidateResourceUsageSummary {
    let mut records = Vec::new();
    if input_tokens > 0 {
        records.push(resource_usage_record(
            "agent-input-tokens",
            input_tokens,
            "tokens",
            ResourceUsageConfidence::ProviderReported,
            "AgentRun.model_events usage input_tokens",
        ));
    }
    if output_tokens > 0 {
        records.push(resource_usage_record(
            "agent-output-tokens",
            output_tokens,
            "tokens",
            ResourceUsageConfidence::ProviderReported,
            "AgentRun.model_events usage output_tokens",
        ));
    }
    if model_turns > 0 {
        records.push(resource_usage_record(
            "agent-model-turns",
            model_turns as u64,
            "turns",
            ResourceUsageConfidence::Exact,
            "AgentRun.model_turns",
        ));
    }
    if tool_results > 0 {
        records.push(resource_usage_record(
            "agent-tool-results",
            tool_results as u64,
            "tool-results",
            ResourceUsageConfidence::Exact,
            "AgentRun.tool_results",
        ));
    }
    CandidateResourceUsageSummary {
        job_id,
        generated_at: now_rfc3339(),
        records,
        scheduler_estimates: Vec::new(),
    }
}

fn resource_usage_record(
    resource: impl Into<String>,
    units: u64,
    unit: impl Into<String>,
    confidence: ResourceUsageConfidence,
    source: impl Into<String>,
) -> CandidateResourceUsageRecord {
    CandidateResourceUsageRecord {
        resource: resource.into(),
        units,
        unit: unit.into(),
        confidence,
        source: source.into(),
    }
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string())
}

fn non_empty_string(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn evolution_agent_mutation_tools(markdown_memory_available: bool) -> Vec<AgentToolSpec> {
    // The internal mutation harness has no host-injected stable activity
    // backend. Do not advertise session_history_search through its per-turn
    // tool projection: doing so would create a visible descriptor with no
    // executor (or tempt this layer to read raw model-session storage). The
    // three SIFS Markdown-note tools are registered above whenever the worker
    // has an authoritative data directory.
    let mut tools = default_coding_agent_tools()
        .into_iter()
        .filter(|tool| {
            tool.kind != AgentToolKind::SessionHistorySearch
                && (markdown_memory_available
                    || !matches!(
                        tool.kind,
                        AgentToolKind::MemorySearch
                            | AgentToolKind::MemoryRead
                            | AgentToolKind::MemoryWrite
                    ))
        })
        .collect::<Vec<_>>();
    tools.push(AgentToolSpec::new(
        AgentToolKind::ApplyPatch,
        "apply_patch",
        "Edit workspace files with a scoped provider-specific patch.",
        true,
    ));
    tools
}

pub fn render_agent_mutation_prompt(
    packet: &MutationPacket,
    workspace: &CandidateWorkspace,
) -> String {
    let mut prompt = render_mutation_prompt(packet);
    prompt.push_str("\n## Candidate Workspace\n");
    prompt.push_str(&format!("path: {}\n", workspace.path.display()));
    prompt.push_str(&format!("branch: {}\n", workspace.branch_name));
    if let Some(revision) = &workspace.source_revision {
        prompt.push_str(&format!("source_revision: {revision}\n"));
    }
    prompt.push_str(
        "Treat this workspace as the only mutation target. The source checkout or owning runtime may be using the source workspace separately.\n",
    );
    prompt.push_str("\n## Agent Harness Instructions\n");
    prompt.push_str(
        "Use the available workspace tools to inspect and edit files directly. Do not ask for approval; any visible tool or resource is already available under the active sandbox.\n",
    );
    prompt.push_str(
        "Keep changes scoped to the intended edit scope unless repository evidence shows a supporting file must also change. Run the requested verification commands when practical; the orchestrator will run them again after you finish.\n",
    );
    prompt.push_str(
        "When the candidate is ready, return a concise final summary with changed files and verification status. Leave all source changes in the candidate workspace.\n",
    );
    prompt
}

pub fn render_mutation_prompt(packet: &MutationPacket) -> String {
    let project = project_profile_for_root(Path::new(&packet.edit_scope.workspace_root));
    let mut prompt = String::new();
    prompt.push_str("# Evolution Mutation Packet\n\n");
    prompt.push_str(&format!("job_id: {}\n", packet.job_id));
    prompt.push_str(&format!("target_project: {}\n", project.summary()));
    prompt
        .push_str("target_ring: used as a risk/evidence hint during the current bootstrap pass\n");
    prompt.push_str(&format!("objective: {}\n\n", packet.objective));
    prompt.push_str("## Intended Edit Scope\n");
    prompt.push_str(&format!(
        "workspace_root: {}\n",
        packet.edit_scope.workspace_root
    ));
    for path in &packet.edit_scope.intended_paths {
        prompt.push_str(&format!("- {path}\n"));
    }
    if !packet.edit_scope.notes.is_empty() {
        prompt.push_str(&format!("notes: {}\n", packet.edit_scope.notes));
    }
    prompt.push_str("\n## Verification Commands\n");
    for command in &packet.verification.commands {
        prompt.push_str(&format!(
            "- {}: `{}` expects exit {} ({:?})\n",
            command.name, command.command, command.expected_exit_code, command.gate
        ));
    }
    if !packet.verification.module_replay_reports.is_empty() {
        prompt.push_str("\n## Module Replay Reports\n");
        for report in &packet.verification.module_replay_reports {
            prompt.push_str(&format!(
                "- {}: `{}` contributes replay/shadow gates\n",
                report.name, report.report_path
            ));
        }
    }
    prompt.push_str("\n## Relevant Files\n");
    if packet.relevant_files.is_empty() {
        prompt.push_str("- no excerpts were selected; inspect the candidate worktree as needed\n");
    } else {
        for file in &packet.relevant_files {
            prompt.push_str(&format!(
                "\n### {}\n```text\n{}\n```\n",
                file.path, file.content
            ));
        }
    }
    prompt.push_str("\n## Output Contract\n");
    for section in &packet.output_contract.expected_sections {
        prompt.push_str(&format!("- {section}\n"));
    }
    prompt.push_str(&format!(
        "prefer_patch: {}\nnotes: {}\n",
        packet.output_contract.prefer_patch, packet.output_contract.notes
    ));
    prompt.push_str("\n## Recovery\n");
    prompt.push_str(&format!(
        "strategy: {:?}\nnote: {}\n",
        packet.recovery.strategy, packet.recovery.note
    ));
    prompt
}

pub fn render_mutation_bridge_prompt(
    packet: &MutationPacket,
    workspace: &CandidateWorkspace,
    tool_descriptors: &[EvolutionToolDescriptor],
    structured_json: bool,
    direct_edit: bool,
) -> String {
    let mut prompt = render_mutation_prompt(packet);
    prompt.push_str("\n## Candidate Workspace\n");
    prompt.push_str(&format!("path: {}\n", workspace.path.display()));
    prompt.push_str(&format!("branch: {}\n", workspace.branch_name));
    if let Some(revision) = &workspace.source_revision {
        prompt.push_str(&format!("source_revision: {revision}\n"));
    }
    prompt.push_str(
        "Treat this workspace as the only mutation target. The source checkout or owning runtime may be using the source workspace separately.\n",
    );

    prompt.push_str("\n## Tool Substrate\n");
    if tool_descriptors.is_empty() {
        prompt.push_str("- no registered tools were provided\n");
    } else {
        for descriptor in tool_descriptors {
            prompt.push_str(&format!(
                "- {} effects={:?} input_schema={}\n",
                descriptor.name, descriptor.effects, descriptor.input_schema_json
            ));
        }
    }

    prompt.push_str("\nLong-running command tools emit `run_id` values. If a command produces no further stdout/stderr, the runtime notifies the model after 1 minute, after 5 minutes, and then every 10 minutes of continued inactivity; use `interrupt_command` to send SIGINT or `kill_command` to terminate by run_id when appropriate.\n");

    prompt.push_str("\n## Mutation Mode\n");
    if direct_edit {
        prompt.push_str(
            "Edit files directly in the candidate workspace. Return concise evidence notes after the edits.\n",
        );
    } else {
        prompt.push_str(
            "Return a unified diff in the `patch` field. Sinomo will apply it to the candidate workspace through the tool registry.\n",
        );
    }

    if structured_json {
        prompt.push_str("\n## Required JSON Response\n");
        prompt.push_str(
            "Return only a json object with keys: tool_calls, changed_files, patch, patch_summary, verification_notes, experiment_notes, recovery_notes.\n",
        );
        prompt.push_str(
            "`tool_calls` is an array of objects shaped like `{ \"tool\": \"read_file\", \"input\": { ... } }`. Use it when you need more context or want Sinomo to run a command before finalizing. When ready, leave `tool_calls` empty and provide `patch` or final notes.\n",
        );
        if direct_edit {
            prompt.push_str("When files were edited directly, set patch to null unless you also have a useful evidence diff.\n");
        }
    }
    prompt
}

fn render_mutation_system_prompt(structured_json: bool, direct_edit: bool) -> String {
    let mut prompt = String::from(
        "You are Sinomo's supervised evolution mutation worker. Work only on the requested candidate workspace and produce durable evidence.",
    );
    if direct_edit {
        prompt.push_str(
            " You may use the coding-agent harness to edit the candidate workspace directly.",
        );
    } else {
        prompt.push_str(
            " You cannot edit files directly; produce an inspectable patch that Sinomo can apply as a candidate.",
        );
    }
    if structured_json {
        prompt.push_str(" Return valid json only.");
    }
    prompt
}

pub fn parse_structured_mutation_output(raw_output: &str) -> Option<StructuredMutationOutput> {
    let json = extract_json_object(raw_output)?;
    serde_json::from_str::<StructuredMutationOutput>(json).ok()
}

fn extract_json_object(raw_output: &str) -> Option<&str> {
    let trimmed = raw_output.trim();
    for fence in ["```json", "```"] {
        if let Some(start) = trimmed.find(fence) {
            let body_start = start + fence.len();
            let body = trimmed[body_start..].trim_start_matches(['\r', '\n']);
            if let Some(end) = body.find("```") {
                let candidate = body[..end].trim();
                if candidate.starts_with('{') && candidate.ends_with('}') {
                    return Some(candidate);
                }
            }
        }
    }
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        return Some(trimmed);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if start < end {
        Some(&trimmed[start..=end])
    } else {
        None
    }
}

fn extract_patch_from_text(raw_output: &str) -> Option<String> {
    let trimmed = raw_output.trim();
    for fence in ["```diff", "```patch", "```"] {
        if let Some(start) = trimmed.find(fence) {
            let body_start = start + fence.len();
            let body = trimmed[body_start..].trim_start_matches(['\r', '\n']);
            if let Some(end) = body.find("```") {
                let patch = body[..end].trim();
                if patch.contains("diff --git") || patch.contains("--- ") {
                    return Some(patch.to_string());
                }
            }
        }
    }
    trimmed
        .find("diff --git ")
        .map(|start| trimmed[start..].trim().to_string())
}

fn first_non_empty_line(raw_output: &str) -> Option<String> {
    raw_output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('{') && !line.starts_with('}'))
        .map(ToOwned::to_owned)
}

fn render_tool_results_followup(results: &[EvolutionToolResult]) -> String {
    let tool_results_json =
        serde_json::to_string_pretty(results).unwrap_or_else(|_| "[]".to_string());
    format!(
        "Tool results from the previous turn:\n```json\n{tool_results_json}\n```\nUse these results as evidence. Return the next JSON response. If more context is required, request more `tool_calls`; if ready, return an empty `tool_calls` array and provide the final patch or notes."
    )
}

pub struct EvolutionOrchestrator<W, R, B> {
    pub workspace: W,
    pub evaluator: ExecutingEvalHarness<R>,
    pub bundles: B,
}

#[derive(Clone, Debug)]
pub struct EvolutionLifecycleUpdate {
    pub job: EvolutionJob,
    pub candidate_workspace: CandidateWorkspace,
    pub bundle: CandidateBundleManifest,
    pub verification_outcome: Option<EvaluationOutcome>,
    pub changed_files: Vec<String>,
    pub summary: String,
}

pub trait EvolutionLifecycleObserver {
    fn on_update(&self, update: &EvolutionLifecycleUpdate) -> Result<(), String>;
}

impl<W, R, B> EvolutionOrchestrator<W, R, B> {
    pub fn new(workspace: W, evaluator: ExecutingEvalHarness<R>, bundles: B) -> Self {
        Self {
            workspace,
            evaluator,
            bundles,
        }
    }
}

impl<W, R, B> EvolutionOrchestrator<W, R, B>
where
    W: WorkspaceAdapter,
    R: CommandRunner,
    B: CandidateBundleStore,
{
    pub fn propose(
        &self,
        request: EvolutionRequest,
        model: Option<&dyn MutationModel>,
    ) -> Result<EvolutionReport, EvolutionError> {
        self.propose_with_lifecycle(request, model, None)
    }

    pub fn propose_with_lifecycle(
        &self,
        request: EvolutionRequest,
        model: Option<&dyn MutationModel>,
        observer: Option<&dyn EvolutionLifecycleObserver>,
    ) -> Result<EvolutionReport, EvolutionError> {
        let mut job = create_bootstrap_job(&request, self.workspace.root());
        let mut bundle = self.bundles.create(&job)?;
        let candidate_workspace = self.workspace.prepare_candidate(&job)?;
        notify_lifecycle(
            observer,
            &job,
            &candidate_workspace,
            &bundle,
            None,
            &[],
            "candidate job created",
        )?;
        let packet = mutation_packet_for_job(&job);

        self.bundles.write_json(&bundle, "job.json", &job)?;
        self.bundles
            .write_json(&bundle, "mutation-packet.json", &packet)?;
        job.status = EvolutionJobStatus::PacketWritten;
        notify_lifecycle(
            observer,
            &job,
            &candidate_workspace,
            &bundle,
            None,
            &[],
            "mutation packet written",
        )?;

        let mut mutation_result = None;
        let run_result = (|| -> Result<EvolutionReport, EvolutionError> {
            if request.dry_run {
                // Dry-run proposals only prepare candidate artifacts and verification evidence.
                job.status = EvolutionJobStatus::MutationFinished;
                notify_lifecycle(
                    observer,
                    &job,
                    &candidate_workspace,
                    &bundle,
                    None,
                    &[],
                    "dry-run mutation skipped",
                )?;
            } else if let Some(model) = model {
                job.status = EvolutionJobStatus::MutationStarted;
                notify_lifecycle(
                    observer,
                    &job,
                    &candidate_workspace,
                    &bundle,
                    None,
                    &[],
                    "mutation agent started",
                )?;
                let result = model.mutate(&packet, &candidate_workspace, Some(&bundle))?;
                self.bundles
                    .write_text(&bundle, "model-output.txt", &result.raw_output)?;
                self.bundles
                    .write_json(&bundle, "model-result.json", &result)?;
                if let Some(agent_run) = &result.agent_run {
                    self.bundles
                        .write_json(&bundle, "agent-run.json", agent_run)?;
                    bundle.agent_run_path = Some(
                        PathBuf::from(&bundle.bundle_path)
                            .join("agent-run.json")
                            .display()
                            .to_string(),
                    );
                }
                if let Some(agent_summary) = &result.agent_summary {
                    self.bundles
                        .write_json(&bundle, "agent-summary.json", agent_summary)?;
                    bundle.agent_summary_path = Some(
                        PathBuf::from(&bundle.bundle_path)
                            .join("agent-summary.json")
                            .display()
                            .to_string(),
                    );
                }
                if !result.agent_events.is_empty() {
                    self.bundles.write_text(
                        &bundle,
                        AGENT_EVENTS_RELATIVE_PATH,
                        &render_jsonl(&result.agent_events)?,
                    )?;
                    bundle.artifact_refs.push(
                        PathBuf::from(&bundle.bundle_path)
                            .join(AGENT_EVENTS_RELATIVE_PATH)
                            .display()
                            .to_string(),
                    );
                }
                if let Some(coding_run_state) = &result.coding_run_state {
                    self.bundles.write_json(
                        &bundle,
                        CODING_RUN_STATE_RELATIVE_PATH,
                        coding_run_state,
                    )?;
                    bundle.artifact_refs.push(
                        PathBuf::from(&bundle.bundle_path)
                            .join(CODING_RUN_STATE_RELATIVE_PATH)
                            .display()
                            .to_string(),
                    );
                }
                if !result.patch_snapshots.is_empty() {
                    self.bundles.write_json(
                        &bundle,
                        PATCH_SNAPSHOTS_RELATIVE_PATH,
                        &result.patch_snapshots,
                    )?;
                    bundle.artifact_refs.push(
                        PathBuf::from(&bundle.bundle_path)
                            .join(PATCH_SNAPSHOTS_RELATIVE_PATH)
                            .display()
                            .to_string(),
                    );
                }
                if !result.validation_records.is_empty() {
                    self.bundles.write_json(
                        &bundle,
                        VALIDATION_RECORDS_RELATIVE_PATH,
                        &result.validation_records,
                    )?;
                    bundle.artifact_refs.push(
                        PathBuf::from(&bundle.bundle_path)
                            .join(VALIDATION_RECORDS_RELATIVE_PATH)
                            .display()
                            .to_string(),
                    );
                }
                if let Some(patch) = &result.parsed_patch {
                    self.bundles.write_text(&bundle, "model.patch", patch)?;
                }
                if !result.tool_results.is_empty() {
                    self.bundles.write_json(
                        &bundle,
                        "model-tool-results.json",
                        &result.tool_results,
                    )?;
                }
                if !result.model_turns.is_empty() {
                    self.bundles
                        .write_json(&bundle, "model-turns.json", &result.model_turns)?;
                }
                bundle.model_output_path = Some(
                    PathBuf::from(&bundle.bundle_path)
                        .join("model-output.txt")
                        .display()
                        .to_string(),
                );
                mutation_result = Some(result);
                job.status = EvolutionJobStatus::MutationFinished;
                notify_lifecycle(
                    observer,
                    &job,
                    &candidate_workspace,
                    &bundle,
                    None,
                    &[],
                    "mutation agent finished",
                )?;
            }

            let changed_files = self.workspace.changed_files(&candidate_workspace)?;
            let diff = self.workspace.diff(&candidate_workspace)?;
            self.bundles
                .write_json(&bundle, "changed-files.json", &changed_files)?;
            self.bundles.write_text(&bundle, "diff.patch", &diff)?;
            bundle.changed_files_path = Some(
                PathBuf::from(&bundle.bundle_path)
                    .join("changed-files.json")
                    .display()
                    .to_string(),
            );
            bundle.diff_path = Some(
                PathBuf::from(&bundle.bundle_path)
                    .join("diff.patch")
                    .display()
                    .to_string(),
            );
            job.status = EvolutionJobStatus::Implemented;
            notify_lifecycle(
                observer,
                &job,
                &candidate_workspace,
                &bundle,
                None,
                &changed_files,
                "candidate diff captured",
            )?;

            let benchmark_executions = prepare_benchmark_gate_executions(&request, &bundle);
            let mut verification_plan = job.verification.clone();
            verification_plan.commands.extend(
                benchmark_executions
                    .iter()
                    .map(|execution| execution.command.clone()),
            );
            job.status = EvolutionJobStatus::Built;
            notify_lifecycle(
                observer,
                &job,
                &candidate_workspace,
                &bundle,
                None,
                &changed_files,
                "verification plan prepared",
            )?;
            let (mut scorecard, command_results) = self
                .evaluator
                .run_verification_plan(verification_plan.into(), &candidate_workspace.path)?;
            for result in &command_results {
                let log_ref = write_command_log_via_store(&self.bundles, &bundle, result)?;
                bundle.command_logs.push(log_ref);
            }
            let benchmark_reports =
                finalize_benchmark_gate_reports(benchmark_executions, &command_results, &bundle);
            apply_benchmark_reports_to_scorecard(&mut scorecard, &benchmark_reports);
            if !benchmark_reports.is_empty() {
                self.bundles
                    .write_json(&bundle, "benchmark-results.json", &benchmark_reports)?;
                bundle.benchmark_results_path = Some(
                    PathBuf::from(&bundle.bundle_path)
                        .join("benchmark-results.json")
                        .display()
                        .to_string(),
                );
            }
            let resource_usage_summary = candidate_resource_usage_summary(
                &job,
                mutation_result.as_ref(),
                &command_results,
                &benchmark_reports,
            );
            self.bundles
                .write_json(&bundle, "resource-usage.json", &resource_usage_summary)?;
            bundle.resource_usage_path = Some(
                PathBuf::from(&bundle.bundle_path)
                    .join("resource-usage.json")
                    .display()
                    .to_string(),
            );
            self.bundles
                .write_json(&bundle, "scorecard.json", &scorecard)?;
            bundle.scorecard_path = Some(
                PathBuf::from(&bundle.bundle_path)
                    .join("scorecard.json")
                    .display()
                    .to_string(),
            );
            let regression_evidence = candidate_regression_evidence_summary(
                &job,
                &bundle,
                &request,
                &scorecard,
                &changed_files,
                &command_results,
                &benchmark_reports,
            );
            self.bundles.write_json(
                &bundle,
                CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH,
                &regression_evidence,
            )?;
            self.bundles.write_text(
                &bundle,
                CANDIDATE_EVIDENCE_SUMMARY_RELATIVE_PATH,
                &render_candidate_evidence_summary(&regression_evidence),
            )?;
            if scorecard.outcome != EvaluationOutcome::Passed {
                let lesson = candidate_evolution_lesson_record(&job, &regression_evidence);
                self.bundles.write_json(
                    &bundle,
                    CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH,
                    &lesson,
                )?;
            }
            job.status = EvolutionJobStatus::Evaluated;
            notify_lifecycle(
                observer,
                &job,
                &candidate_workspace,
                &bundle,
                Some(&scorecard.outcome),
                &changed_files,
                "evaluation scorecard written",
            )?;

            finalize_candidate_bundle_integrity(
                &mut bundle,
                &job,
                &candidate_workspace,
                mutation_result.as_ref(),
                scorecard.outcome.clone(),
                &changed_files,
            )?;
            self.bundles.write_json(&bundle, "manifest.json", &bundle)?;

            job.status = EvolutionJobStatus::AwaitingApproval;
            notify_lifecycle(
                observer,
                &job,
                &candidate_workspace,
                &bundle,
                Some(&scorecard.outcome),
                &changed_files,
                "candidate awaiting approval",
            )?;

            Ok(EvolutionReport {
                job: job.clone(),
                candidate_workspace: candidate_workspace.clone(),
                bundle: bundle.clone(),
                scorecard: Some(scorecard),
                changed_files,
            })
        })();
        if let Err(error) = &run_result {
            job.status = EvolutionJobStatus::Failed;
            let _ = notify_lifecycle(
                observer,
                &job,
                &candidate_workspace,
                &bundle,
                None,
                &[],
                &format!("candidate failed: {error}"),
            );
        }
        run_result
    }
}

fn notify_lifecycle(
    observer: Option<&dyn EvolutionLifecycleObserver>,
    job: &EvolutionJob,
    candidate_workspace: &CandidateWorkspace,
    bundle: &CandidateBundleManifest,
    verification_outcome: Option<&EvaluationOutcome>,
    changed_files: &[String],
    summary: &str,
) -> Result<(), EvolutionError> {
    if let Some(observer) = observer {
        observer
            .on_update(&EvolutionLifecycleUpdate {
                job: job.clone(),
                candidate_workspace: candidate_workspace.clone(),
                bundle: bundle.clone(),
                verification_outcome: verification_outcome.cloned(),
                changed_files: changed_files.to_vec(),
                summary: summary.to_string(),
            })
            .map_err(|detail| EvolutionError::LifecycleObserver { detail })?;
    }
    Ok(())
}

fn finalize_candidate_bundle_integrity(
    bundle: &mut CandidateBundleManifest,
    job: &EvolutionJob,
    workspace: &CandidateWorkspace,
    mutation_result: Option<&ModelMutationResult>,
    verification_outcome: EvaluationOutcome,
    changed_files: &[String],
) -> Result<(), BundleError> {
    let bundle_root = PathBuf::from(&bundle.bundle_path);
    let content_manifest = build_candidate_bundle_content_manifest(&bundle_root)?;
    let content_manifest_path = bundle_root.join("bundle-content-manifest.json");
    write_json_file_for_bundle(&content_manifest_path, &content_manifest)?;
    bundle.content_manifest_path = Some(content_manifest_path.display().to_string());
    bundle.bundle_content_hash = Some(content_manifest.bundle_content_hash.clone());

    let attestation = build_candidate_bundle_attestation(
        bundle,
        job,
        workspace,
        mutation_result,
        verification_outcome,
        changed_files,
        &content_manifest,
    );
    let attestation_path = bundle_root.join("bundle-attestation.json");
    write_json_file_for_bundle(&attestation_path, &attestation)?;
    bundle.attestation_path = Some(attestation_path.display().to_string());
    Ok(())
}

fn build_candidate_bundle_content_manifest(
    bundle_root: &Path,
) -> Result<CandidateBundleContentManifest, BundleError> {
    let mut files = Vec::new();
    collect_candidate_bundle_file_digests(bundle_root, bundle_root, &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let bundle_content_hash = bundle_content_hash(&files);
    Ok(CandidateBundleContentManifest {
        bundle_path: bundle_root.display().to_string(),
        generated_at: now_rfc3339(),
        bundle_content_hash,
        hash_algorithm: "blake3".to_string(),
        files,
    })
}

fn collect_candidate_bundle_file_digests(
    bundle_root: &Path,
    path: &Path,
    files: &mut Vec<CandidateBundleFileDigest>,
) -> Result<(), BundleError> {
    let entries = fs::read_dir(path).map_err(|source| BundleError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| BundleError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_candidate_bundle_file_digests(bundle_root, &entry_path, files)?;
        } else if entry_path.is_file() {
            let relative = bundle_relative_path(bundle_root, &entry_path);
            if is_bundle_integrity_control_file(&relative) {
                continue;
            }
            files.push(file_digest(&entry_path, relative)?);
        }
    }
    Ok(())
}

fn bundle_relative_path(bundle_root: &Path, path: &Path) -> String {
    path.strip_prefix(bundle_root)
        .unwrap_or(path)
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn is_bundle_integrity_control_file(path: &str) -> bool {
    matches!(
        path,
        "manifest.json"
            | "job.json"
            | "record.json"
            | "bundle-content-manifest.json"
            | "bundle-attestation.json"
            | "artifact-evidence-record.json"
            | "candidate-summary.md"
            | "approval-decision.json"
            | RELEASE_MATERIAL_RELATIVE_PATH
    )
}

fn file_digest(path: &Path, relative: String) -> Result<CandidateBundleFileDigest, BundleError> {
    let mut file = fs::File::open(path).map_err(|source| BundleError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 8192];
    loop {
        let read = file.read(&mut buffer).map_err(|source| BundleError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        if read == 0 {
            break;
        }
        bytes = bytes.saturating_add(read as u64);
        hasher.update(&buffer[..read]);
    }
    Ok(CandidateBundleFileDigest {
        path: relative,
        bytes,
        digest: format!("blake3:{}", hasher.finalize().to_hex()),
    })
}

fn bundle_content_hash(files: &[CandidateBundleFileDigest]) -> String {
    let mut hasher = blake3::Hasher::new();
    for file in files {
        hasher.update(file.path.as_bytes());
        hasher.update(&[0]);
        hasher.update(file.digest.as_bytes());
        hasher.update(&[0]);
        hasher.update(file.bytes.to_string().as_bytes());
        hasher.update(&[0]);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn build_candidate_bundle_attestation(
    bundle: &CandidateBundleManifest,
    job: &EvolutionJob,
    workspace: &CandidateWorkspace,
    mutation_result: Option<&ModelMutationResult>,
    verification_outcome: EvaluationOutcome,
    changed_files: &[String],
    content_manifest: &CandidateBundleContentManifest,
) -> CandidateBundleAttestation {
    let mutation_worker = mutation_result
        .map(|result| {
            if result.agent_run.is_some() {
                "agent-loop"
            } else {
                "legacy-bridge"
            }
        })
        .unwrap_or("dry-run")
        .to_string();
    let artifact_refs = candidate_bundle_artifact_refs(bundle);
    CandidateBundleAttestation {
        statement_type: "https://in-toto.io/Statement/v1".to_string(),
        predicate_type: "https://sinomo.net/attestations/evolution-candidate-bundle/v1".to_string(),
        subject: CandidateBundleSubject {
            name: format!("candidate-bundle:{}", job.job_id),
            digest: content_manifest.bundle_content_hash.clone(),
        },
        predicate: CandidateBundlePredicate {
            job_id: job.job_id.clone(),
            objective: job.objective.clone(),
            target_ring: job.target_ring.clone(),
            produced_by: "sinomo-evolution".to_string(),
            mutation_worker,
            provider: mutation_result.map(|result| result.provider.clone()),
            model: mutation_result.map(|result| result.model.clone()),
            source_workspace: job.edit_scope.workspace_root.clone(),
            candidate_worktree: workspace.path.display().to_string(),
            candidate_branch: workspace.branch_name.clone(),
            source_revision: workspace.source_revision.clone(),
            verification_outcome: Some(verification_outcome.clone()),
            changed_files: changed_files.to_vec(),
            artifact_refs,
            generated_at: now_rfc3339(),
        },
    }
}

fn candidate_bundle_artifact_refs(bundle: &CandidateBundleManifest) -> Vec<String> {
    let mut refs = vec![bundle.mutation_packet_path.clone()];
    refs.extend(bundle.artifact_refs.iter().cloned());
    refs.extend(bundle.job_path.iter().cloned());
    refs.extend(bundle.model_output_path.iter().cloned());
    refs.extend(bundle.agent_run_path.iter().cloned());
    refs.extend(bundle.agent_summary_path.iter().cloned());
    refs.extend(bundle.benchmark_results_path.iter().cloned());
    refs.extend(bundle.resource_usage_path.iter().cloned());
    refs.extend(bundle.changed_files_path.iter().cloned());
    refs.extend(bundle.diff_path.iter().cloned());
    refs.extend(bundle.scorecard_path.iter().cloned());
    refs.extend(bundle.command_logs.iter().map(|log| log.log_path.clone()));
    for relative in [
        CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH,
        CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH,
        CANDIDATE_EVIDENCE_SUMMARY_RELATIVE_PATH,
        AGENT_EVENTS_RELATIVE_PATH,
        CODING_RUN_STATE_RELATIVE_PATH,
        PATCH_SNAPSHOTS_RELATIVE_PATH,
        VALIDATION_RECORDS_RELATIVE_PATH,
    ] {
        let path = PathBuf::from(&bundle.bundle_path).join(relative);
        if path.exists() {
            refs.push(path.display().to_string());
        }
    }
    let rollout_material_path = candidate_bundle_release_material_path(bundle);
    if rollout_material_path.exists() {
        refs.push(rollout_material_path.display().to_string());
    }
    refs.sort();
    refs.dedup();
    refs
}

fn candidate_bundle_release_material_path(bundle: &CandidateBundleManifest) -> PathBuf {
    PathBuf::from(&bundle.bundle_path).join(RELEASE_MATERIAL_RELATIVE_PATH)
}

fn candidate_bundle_artifact_evidence_manifest(
    bundle: &CandidateBundleManifest,
) -> ArtifactEvidenceManifest {
    ArtifactEvidenceManifest {
        evidence_id: bundle.job_id.clone(),
        evidence_kind: "evolution-candidate-bundle".to_string(),
        evidence_path: bundle.bundle_path.clone(),
        declared_content_hash: bundle.bundle_content_hash.clone(),
        content_manifest_path: bundle.content_manifest_path.clone(),
        attestation_path: bundle.attestation_path.clone(),
        evidence_refs: candidate_bundle_artifact_refs(bundle),
    }
}

fn write_json_file_for_bundle<T: Serialize>(path: &Path, value: &T) -> Result<(), BundleError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| BundleError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_json::to_string_pretty(value)
        .map_err(|source| BundleError::Serialize(source.to_string()))?;
    fs::write(path, content).map_err(|source| BundleError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn render_jsonl(values: &[serde_json::Value]) -> Result<String, BundleError> {
    let mut content = String::new();
    for value in values {
        content.push_str(
            &serde_json::to_string(value)
                .map_err(|source| BundleError::Serialize(source.to_string()))?,
        );
        content.push('\n');
    }
    Ok(content)
}

fn candidate_resource_usage_summary(
    job: &EvolutionJob,
    mutation_result: Option<&ModelMutationResult>,
    command_results: &[CommandRunResult],
    benchmark_reports: &[BenchmarkGateReport],
) -> CandidateResourceUsageSummary {
    let mut records = mutation_result
        .and_then(|result| result.agent_resource_usage.as_ref())
        .map(|usage| usage.records.clone())
        .unwrap_or_default();

    let benchmark_names: BTreeSet<&str> = benchmark_reports
        .iter()
        .map(|report| report.name.as_str())
        .collect();
    let benchmark_wall_ms = benchmark_reports
        .iter()
        .filter_map(|report| report.duration_ms)
        .sum::<u64>();
    let verification_wall_ms = command_results
        .iter()
        .filter(|result| !benchmark_names.contains(result.name.as_str()))
        .map(|result| result.duration_ms)
        .sum::<u64>();
    if verification_wall_ms > 0 {
        records.push(resource_usage_record(
            "verification-command-wall-ms",
            verification_wall_ms,
            "ms",
            ResourceUsageConfidence::Exact,
            "verification command wall time",
        ));
    }
    if benchmark_wall_ms > 0 {
        records.push(resource_usage_record(
            "benchmark-wall-ms",
            benchmark_wall_ms,
            "ms",
            ResourceUsageConfidence::Exact,
            "benchmark command wall time",
        ));
    }
    let local_compute_ms = verification_wall_ms.saturating_add(benchmark_wall_ms);
    if local_compute_ms > 0 {
        records.push(resource_usage_record(
            "local-compute-ms",
            local_compute_ms,
            "ms",
            ResourceUsageConfidence::Estimated,
            "wall time proxy for local compute cost",
        ));
    }
    CandidateResourceUsageSummary {
        job_id: job.job_id.clone(),
        generated_at: now_rfc3339(),
        scheduler_estimates: Vec::new(),
        records,
    }
}

fn candidate_regression_evidence_summary(
    job: &EvolutionJob,
    bundle: &CandidateBundleManifest,
    request: &EvolutionRequest,
    scorecard: &EvaluationScorecard,
    changed_files: &[String],
    command_results: &[CommandRunResult],
    benchmark_reports: &[BenchmarkGateReport],
) -> CandidateRegressionEvidenceSummary {
    let benchmark_result_names: BTreeSet<&str> = benchmark_reports
        .iter()
        .map(|report| report.name.as_str())
        .collect();
    let failed_gates = scorecard
        .gates
        .iter()
        .filter(|gate| gate.outcome != EvaluationOutcome::Passed)
        .map(|gate| RegressionGateEvidence {
            gate: gate.gate.clone(),
            outcome: gate.outcome.clone(),
            summary: gate.summary.clone(),
        })
        .collect::<Vec<_>>();
    let failed_commands = command_results
        .iter()
        .filter(|result| !result.passed())
        .filter(|result| !benchmark_result_names.contains(result.name.as_str()))
        .map(|result| {
            let command_log_path = bundle
                .command_logs
                .iter()
                .find(|log| log.name == result.name)
                .map(|log| log.log_path.clone());
            RegressionCommandFailure {
                name: result.name.clone(),
                command: result.command.clone(),
                expected_exit_code: result.expected_exit_code,
                exit_code: result.exit_code,
                duration_ms: result.duration_ms,
                command_log_path,
                stdout_excerpt: tail_excerpt(&result.stdout, 4096),
                stderr_excerpt: tail_excerpt(&result.stderr, 4096),
                rerun_command: result.command.clone(),
            }
        })
        .collect::<Vec<_>>();
    let failed_benchmarks = benchmark_reports
        .iter()
        .filter(|report| report.outcome != EvaluationOutcome::Passed)
        .map(|report| RegressionBenchmarkFailure {
            name: report.name.clone(),
            outcome: report.outcome.clone(),
            command: report.command.clone(),
            summary_path: report.summary_path.clone(),
            results_dir: report.results_dir.clone(),
            command_log_path: report.command_log_path.clone(),
            summary_verdict: report.summary_verdict.clone(),
            summary_error: report.summary_error.clone(),
        })
        .collect::<Vec<_>>();
    let mut trace_fixtures = request
        .module_replay_reports
        .iter()
        .map(|report| TraceDerivedRegressionFixture {
            name: report.name.clone(),
            source: "module-replay-report".to_string(),
            report_path: report.report_path.clone(),
            baseline_artifact_id: report
                .baseline_artifact_id
                .as_ref()
                .map(ToString::to_string),
            candidate_artifact_id: report
                .candidate_artifact_id
                .as_ref()
                .map(ToString::to_string),
            minimization_hint: format!(
                "minimize `{}` to the smallest replay trace that still reproduces the failed gate",
                report.report_path
            ),
            suggested_gate: EvaluationGate::ReplayTests,
        })
        .collect::<Vec<_>>();
    trace_fixtures.extend(rollout_telemetry_trace_fixtures_from_request(request));
    trace_fixtures.extend(module_telemetry_trace_fixtures_from_request(request));
    trace_fixtures.extend(event_log_trace_fixtures_from_request(request));
    trace_fixtures.sort_by(|left, right| {
        left.report_path
            .cmp(&right.report_path)
            .then_with(|| left.name.cmp(&right.name))
    });
    trace_fixtures.dedup_by(|left, right| left.report_path == right.report_path);
    let reusable_gate_suggestions =
        reusable_gate_suggestions(&failed_commands, &failed_benchmarks, &trace_fixtures);
    let replay_minimization = replay_minimization_plan(
        bundle,
        &failed_commands,
        &failed_benchmarks,
        &trace_fixtures,
        &reusable_gate_suggestions,
    );

    CandidateRegressionEvidenceSummary {
        job_id: job.job_id.clone(),
        objective: job.objective.clone(),
        target_ring: job.target_ring.clone(),
        outcome: scorecard.outcome.clone(),
        bundle_path: bundle.bundle_path.clone(),
        scorecard_path: bundle.scorecard_path.clone(),
        diff_path: bundle.diff_path.clone(),
        changed_files: changed_files.to_vec(),
        failed_gates,
        failed_commands,
        failed_benchmarks,
        trace_fixtures,
        replay_minimization,
        reusable_gate_suggestions,
        generated_at: now_rfc3339(),
    }
}

fn rollout_telemetry_trace_fixtures_from_request(
    request: &EvolutionRequest,
) -> Vec<TraceDerivedRegressionFixture> {
    rollout_telemetry_window_refs_from_text(&request.objective)
        .into_iter()
        .enumerate()
        .map(|(index, evidence_ref)| TraceDerivedRegressionFixture {
            name: format!("rollout-telemetry-window-{}", index + 1),
            source: "rollout-telemetry-window".to_string(),
            report_path: evidence_ref.clone(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
            minimization_hint: format!(
                "convert `{evidence_ref}` into the smallest replay or telemetry fixture that still reproduces the rollback trigger"
            ),
            suggested_gate: EvaluationGate::ReplayTests,
        })
        .collect()
}

fn module_telemetry_trace_fixtures_from_request(
    request: &EvolutionRequest,
) -> Vec<TraceDerivedRegressionFixture> {
    trace_evidence_refs_from_text(&request.objective, "module-telemetry://")
        .into_iter()
        .enumerate()
        .map(|(index, evidence_ref)| TraceDerivedRegressionFixture {
            name: format!("module-telemetry-window-{}", index + 1),
            source: "module-telemetry-window".to_string(),
            report_path: evidence_ref.clone(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
            minimization_hint: format!(
                "convert `{evidence_ref}` into the smallest module replay or telemetry fixture that still reproduces the observed module failure"
            ),
            suggested_gate: EvaluationGate::ReplayTests,
        })
        .collect()
}

fn event_log_trace_fixtures_from_request(
    request: &EvolutionRequest,
) -> Vec<TraceDerivedRegressionFixture> {
    trace_evidence_refs_from_text(&request.objective, "event://")
        .into_iter()
        .enumerate()
        .map(|(index, evidence_ref)| TraceDerivedRegressionFixture {
            name: format!("event-log-signal-{}", index + 1),
            source: "event-log-signal".to_string(),
            report_path: evidence_ref.clone(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
            minimization_hint: format!(
                "use `{evidence_ref}` to locate the smallest event-log window or replay trace that still reproduces the observed failure"
            ),
            suggested_gate: EvaluationGate::ReplayTests,
        })
        .collect()
}

fn rollout_telemetry_window_refs_from_text(text: &str) -> Vec<String> {
    trace_evidence_refs_from_text(text, "rollout-telemetry://window?")
}

fn trace_evidence_refs_from_text(text: &str, prefix: &str) -> Vec<String> {
    let mut refs = text
        .split(|ch: char| ch.is_whitespace() || ch == ';' || ch == ',' || ch == '"' || ch == '\'')
        .filter_map(|token| {
            let token = token.trim_matches(|ch: char| {
                matches!(ch, '.' | ')' | ']' | '}' | '(' | '[' | '{' | '`')
            });
            token.starts_with(prefix).then(|| token.to_string())
        })
        .collect::<Vec<_>>();
    refs.sort();
    refs.dedup();
    refs
}

fn reusable_gate_suggestions(
    failed_commands: &[RegressionCommandFailure],
    failed_benchmarks: &[RegressionBenchmarkFailure],
    trace_fixtures: &[TraceDerivedRegressionFixture],
) -> Vec<String> {
    let mut suggestions = Vec::new();
    suggestions.extend(failed_commands.iter().map(|failure| {
        format!(
            "add verification command `{}` for failure `{}`",
            failure.command, failure.name
        )
    }));
    suggestions.extend(failed_benchmarks.iter().map(|failure| {
        format!(
            "promote benchmark `{}` results to a private regression gate",
            failure.name
        )
    }));
    suggestions.extend(trace_fixtures.iter().map(|fixture| {
        format!(
            "keep replay fixture `{}` at `{}` as a ReplayTests gate",
            fixture.name, fixture.report_path
        )
    }));
    suggestions.sort();
    suggestions.dedup();
    suggestions
}

fn replay_minimization_plan(
    bundle: &CandidateBundleManifest,
    failed_commands: &[RegressionCommandFailure],
    failed_benchmarks: &[RegressionBenchmarkFailure],
    trace_fixtures: &[TraceDerivedRegressionFixture],
    reusable_gate_suggestions: &[String],
) -> ReplayMinimizationPlan {
    let mut retained_evidence_refs = Vec::new();
    retained_evidence_refs.extend(bundle.scorecard_path.iter().cloned());
    retained_evidence_refs.extend(bundle.diff_path.iter().cloned());
    retained_evidence_refs.extend(bundle.changed_files_path.iter().cloned());
    retained_evidence_refs.extend(
        failed_commands
            .iter()
            .filter_map(|failure| failure.command_log_path.clone()),
    );
    retained_evidence_refs.extend(
        failed_benchmarks
            .iter()
            .filter_map(|failure| failure.summary_path.clone()),
    );
    retained_evidence_refs.extend(
        trace_fixtures
            .iter()
            .map(|fixture| fixture.report_path.clone()),
    );
    retained_evidence_refs.sort();
    retained_evidence_refs.dedup();

    let mut minimization_steps = Vec::new();
    if failed_commands.is_empty() && failed_benchmarks.is_empty() && trace_fixtures.is_empty() {
        minimization_steps.push(
            "inspect the scorecard and diff before creating a reusable regression fixture"
                .to_string(),
        );
    }
    minimization_steps.extend(failed_commands.iter().map(|failure| {
        format!(
            "rerun `{}` and reduce inputs until exit {} still reproduces",
            failure.command, failure.exit_code
        )
    }));
    minimization_steps.extend(failed_benchmarks.iter().map(|failure| {
        format!(
            "extract the smallest benchmark case from `{}` that preserves the verdict `{}`",
            failure.name,
            failure
                .summary_verdict
                .as_deref()
                .or(failure.summary_error.as_deref())
                .unwrap_or("failed")
        )
    }));
    minimization_steps.extend(trace_fixtures.iter().map(|fixture| {
        format!(
            "reduce `{}` to the shortest replay trace that still fails",
            fixture.report_path
        )
    }));

    let mut candidate_gate_commands = failed_commands
        .iter()
        .map(|failure| failure.rerun_command.clone())
        .collect::<Vec<_>>();
    candidate_gate_commands.extend(reusable_gate_suggestions.iter().cloned());
    candidate_gate_commands.sort();
    candidate_gate_commands.dedup();

    ReplayMinimizationPlan {
        status: if retained_evidence_refs.is_empty() {
            "needs-human-triage".to_string()
        } else {
            "ready-for-minimization".to_string()
        },
        retained_evidence_refs,
        minimization_steps,
        candidate_gate_commands,
    }
}

fn candidate_evolution_lesson_record(
    job: &EvolutionJob,
    evidence: &CandidateRegressionEvidenceSummary,
) -> CandidateEvolutionLessonRecord {
    let failure_class = classify_failure_from_regression_evidence(evidence);
    CandidateEvolutionLessonRecord {
        lesson_id: generated_id("lesson"),
        job_id: job.job_id.clone(),
        objective: job.objective.clone(),
        outcome: evidence.outcome.clone(),
        failure_class: failure_class.clone(),
        root_cause_summary: root_cause_summary_from_regression_evidence(evidence),
        failed_assumptions: failed_assumptions_from_regression_evidence(evidence),
        rollback_evidence: vec![
            "no runtime rollout is attempted by candidate generation".to_string(),
            format!("recovery strategy: {:?}", job.recovery.strategy),
            job.recovery.note.clone(),
        ],
        suggested_follow_up_tasks: suggested_follow_up_tasks_from_regression_evidence(
            evidence,
            &failure_class,
        ),
        reusable_evidence_refs: evidence.replay_minimization.retained_evidence_refs.clone(),
        recorded_at: now_rfc3339(),
    }
}

fn classify_failure_from_regression_evidence(
    evidence: &CandidateRegressionEvidenceSummary,
) -> EvolutionFailureClass {
    if !evidence.failed_benchmarks.is_empty()
        || evidence
            .failed_gates
            .iter()
            .any(|gate| matches!(gate.gate, EvaluationGate::PrivateRegressionEvals))
    {
        return EvolutionFailureClass::EvalRegression;
    }
    if evidence
        .failed_gates
        .iter()
        .any(|gate| matches!(gate.gate, EvaluationGate::ReplayTests))
    {
        return EvolutionFailureClass::EvalRegression;
    }
    if evidence.failed_gates.iter().any(|gate| {
        matches!(
            gate.gate,
            EvaluationGate::StaticChecks
                | EvaluationGate::UnitTests
                | EvaluationGate::IntegrationTests
        )
    }) {
        return EvolutionFailureClass::BuildTestFailure;
    }
    if evidence.changed_files.is_empty() {
        return EvolutionFailureClass::WrongTaskSelection;
    }
    EvolutionFailureClass::InconclusiveEvidence
}

fn root_cause_summary_from_regression_evidence(
    evidence: &CandidateRegressionEvidenceSummary,
) -> String {
    if let Some(command) = evidence.failed_commands.first() {
        return format!(
            "verification command `{}` exited {}, expected {}",
            command.command, command.exit_code, command.expected_exit_code
        );
    }
    if let Some(benchmark) = evidence.failed_benchmarks.first() {
        return format!(
            "benchmark `{}` failed: {}",
            benchmark.name,
            benchmark
                .summary_verdict
                .as_deref()
                .or(benchmark.summary_error.as_deref())
                .unwrap_or("no benchmark summary")
        );
    }
    if let Some(gate) = evidence.failed_gates.first() {
        return format!("{:?} gate failed: {}", gate.gate, gate.summary);
    }
    if evidence.changed_files.is_empty() {
        return "candidate produced no source diff for a mutation attempt".to_string();
    }
    "candidate evidence was inconclusive; inspect scorecard and command logs".to_string()
}

fn failed_assumptions_from_regression_evidence(
    evidence: &CandidateRegressionEvidenceSummary,
) -> Vec<String> {
    let mut assumptions = Vec::new();
    if evidence.changed_files.is_empty() {
        assumptions
            .push("the selected scope and model run would produce a candidate diff".to_string());
    }
    assumptions.extend(evidence.failed_commands.iter().map(|failure| {
        format!(
            "verification command `{}` would exit {}",
            failure.command, failure.expected_exit_code
        )
    }));
    assumptions.extend(
        evidence
            .failed_benchmarks
            .iter()
            .map(|failure| format!("benchmark `{}` would satisfy its gate", failure.name)),
    );
    assumptions.extend(
        evidence
            .failed_gates
            .iter()
            .map(|gate| format!("{:?} gate would pass: {}", gate.gate, gate.summary)),
    );
    assumptions.sort();
    assumptions.dedup();
    assumptions
}

fn suggested_follow_up_tasks_from_regression_evidence(
    evidence: &CandidateRegressionEvidenceSummary,
    failure_class: &EvolutionFailureClass,
) -> Vec<String> {
    let mut tasks = Vec::new();
    match failure_class {
        EvolutionFailureClass::WrongTaskSelection => tasks.push(
            "create a smaller diagnostic task that first proves the intended edit scope can change"
                .to_string(),
        ),
        EvolutionFailureClass::BuildTestFailure => tasks.push(
            "add a focused build or unit-test reproduction before retrying the implementation"
                .to_string(),
        ),
        EvolutionFailureClass::EvalRegression => tasks.push(
            "minimize the replay or benchmark failure into a private regression gate".to_string(),
        ),
        _ => tasks.push(
            "add an analysis-only task that explains the inconclusive evidence before retrying"
                .to_string(),
        ),
    }
    tasks.extend(
        evidence
            .replay_minimization
            .minimization_steps
            .iter()
            .cloned(),
    );
    tasks.sort();
    tasks.dedup();
    tasks
}

fn render_candidate_evidence_summary(evidence: &CandidateRegressionEvidenceSummary) -> String {
    let changed_files = if evidence.changed_files.is_empty() {
        "none".to_string()
    } else {
        evidence.changed_files.join("\n")
    };
    let failed_gates = if evidence.failed_gates.is_empty() {
        "none".to_string()
    } else {
        evidence
            .failed_gates
            .iter()
            .map(|gate| format!("- {:?}: {}", gate.gate, gate.summary))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let replay_fixtures = if evidence.trace_fixtures.is_empty() {
        "none".to_string()
    } else {
        evidence
            .trace_fixtures
            .iter()
            .map(|fixture| format!("- {}: {}", fixture.name, fixture.report_path))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let minimization_steps = if evidence.replay_minimization.minimization_steps.is_empty() {
        "none".to_string()
    } else {
        evidence
            .replay_minimization
            .minimization_steps
            .iter()
            .map(|step| format!("- {step}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let reusable_gate_suggestions = if evidence.reusable_gate_suggestions.is_empty() {
        "none".to_string()
    } else {
        evidence
            .reusable_gate_suggestions
            .iter()
            .map(|suggestion| format!("- {suggestion}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    format!(
        "# Candidate Evidence Summary\n\njob: {}\noutcome: {:?}\nobjective: {}\nbundle: {}\nscorecard: {}\ndiff: {}\n\n## Changed Files\n{}\n\n## Failed Gates\n{}\n\n## Trace-Derived Fixtures\n{}\n\n## Replay Minimization\nstatus: {}\n{}\n\n## Reusable Gate Suggestions\n{}\n",
        evidence.job_id,
        evidence.outcome,
        evidence.objective,
        evidence.bundle_path,
        evidence.scorecard_path.as_deref().unwrap_or("none"),
        evidence.diff_path.as_deref().unwrap_or("none"),
        changed_files,
        failed_gates,
        replay_fixtures,
        evidence.replay_minimization.status,
        minimization_steps,
        reusable_gate_suggestions
    )
}

fn tail_excerpt(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let tail = value
        .chars()
        .rev()
        .take(max_chars)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("[truncated]\n{tail}")
}

#[derive(Clone, Debug)]
struct BenchmarkGateExecution {
    command: VerificationCommand,
    report: BenchmarkGateReport,
}

fn prepare_benchmark_gate_executions(
    request: &EvolutionRequest,
    bundle: &CandidateBundleManifest,
) -> Vec<BenchmarkGateExecution> {
    request
        .benchmark_gates
        .iter()
        .enumerate()
        .map(|(index, gate)| prepare_benchmark_gate_execution(index + 1, gate, bundle))
        .collect()
}

fn prepare_benchmark_gate_execution(
    index: usize,
    gate: &BenchmarkGateRequest,
    bundle: &CandidateBundleManifest,
) -> BenchmarkGateExecution {
    let name = if gate.name.trim().is_empty() {
        format!("benchmark-{index}")
    } else {
        gate.name.clone()
    };
    let safe_name = bundle_safe_file_stem(&name);
    let (command, summary_path, results_dir, progress_path) = match &gate.kind {
        BenchmarkGateKind::SweBench {
            instance_id,
            limit,
            provider,
            model,
            max_steps,
            evaluate,
        } => {
            let results_dir = PathBuf::from(&bundle.bundle_path)
                .join("benchmarks")
                .join(&safe_name);
            let summary_path = results_dir.join("summary.json");
            let progress_path = results_dir.join("progress.jsonl");
            let mut args = vec![
                swebench_python_command(),
                benchmark_module_path("swebench/run_swebench_verified.py"),
                "--mode".to_string(),
                "sinomo".to_string(),
                "--provider".to_string(),
                provider.clone(),
                "--model".to_string(),
                model.clone(),
                "--max-steps".to_string(),
                max_steps.to_string(),
                "--results-dir".to_string(),
                results_dir.display().to_string(),
                "--summary-out".to_string(),
                summary_path.display().to_string(),
                "--progress-out".to_string(),
                progress_path.display().to_string(),
                "--run-id".to_string(),
                safe_name.clone(),
            ];
            if let Some(instance_id) = instance_id {
                args.extend(["--instance-id".to_string(), instance_id.clone()]);
            }
            if let Some(limit) = limit {
                args.extend(["--limit".to_string(), limit.to_string()]);
            }
            if *evaluate {
                args.push("--evaluate".to_string());
            }
            (
                shell_join(&args),
                Some(summary_path.display().to_string()),
                Some(results_dir.display().to_string()),
                Some(progress_path.display().to_string()),
            )
        }
        BenchmarkGateKind::Command {
            command,
            summary_path,
            results_dir,
            progress_path,
        } => (
            command.clone(),
            summary_path.clone(),
            results_dir.clone(),
            progress_path.clone(),
        ),
    };
    BenchmarkGateExecution {
        command: VerificationCommand {
            name: name.clone(),
            command: command.clone(),
            expected_exit_code: gate.expected_exit_code,
            gate: EvaluationGate::PrivateRegressionEvals,
        },
        report: BenchmarkGateReport {
            name,
            command,
            expected_exit_code: gate.expected_exit_code,
            exit_code: None,
            duration_ms: None,
            outcome: EvaluationOutcome::Inconclusive,
            summary_path,
            results_dir,
            command_log_path: None,
            metrics: Vec::new(),
            progress_path,
            progress_event_count: None,
            progress_last_event: None,
            progress_error: None,
            summary: None,
            summary_error: None,
            summary_verdict: None,
        },
    }
}

fn swebench_python_command() -> String {
    std::env::var("SINOMO_SWEBENCH_PYTHON")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            let repo_local = Path::new("target/tmp/swebench-venv/bin/python");
            if repo_local.exists() {
                repo_local.display().to_string()
            } else {
                "python3".to_string()
            }
        })
}

fn benchmark_module_path(relative_path: &str) -> String {
    let root = std::env::var(SINOMO_BENCHMARK_MODULE_ROOT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_BENCHMARK_MODULE_ROOT.to_string());
    format!(
        "{}/{}",
        root.trim_end_matches('/'),
        relative_path.trim_start_matches('/')
    )
}

fn finalize_benchmark_gate_reports(
    executions: Vec<BenchmarkGateExecution>,
    command_results: &[CommandRunResult],
    bundle: &CandidateBundleManifest,
) -> Vec<BenchmarkGateReport> {
    executions
        .into_iter()
        .map(|execution| {
            let mut report = execution.report;
            if let Some(result) = command_results
                .iter()
                .find(|result| result.name == execution.command.name)
            {
                report.exit_code = Some(result.exit_code);
                report.duration_ms = Some(result.duration_ms);
                report.outcome = if result.passed() {
                    EvaluationOutcome::Passed
                } else {
                    EvaluationOutcome::Failed
                };
                report.command_log_path = bundle
                    .command_logs
                    .iter()
                    .find(|log| log.name == result.name)
                    .map(|log| log.log_path.clone());
            }
            enrich_benchmark_report_from_summary(&mut report);
            enrich_benchmark_report_from_progress(&mut report);
            report
        })
        .collect()
}

fn enrich_benchmark_report_from_summary(report: &mut BenchmarkGateReport) {
    let Some(summary_path) = report.summary_path.clone() else {
        return;
    };
    let path = PathBuf::from(&summary_path);
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(source) => {
            report.summary_error = Some(format!(
                "could not read benchmark summary `{}`: {source}",
                path.display()
            ));
            if report.outcome == EvaluationOutcome::Passed {
                report.outcome = EvaluationOutcome::Inconclusive;
            }
            return;
        }
    };
    let summary = match serde_json::from_str::<serde_json::Value>(&content) {
        Ok(summary) => summary,
        Err(source) => {
            report.summary_error = Some(format!(
                "could not parse benchmark summary `{}`: {source}",
                path.display()
            ));
            if report.outcome == EvaluationOutcome::Passed {
                report.outcome = EvaluationOutcome::Inconclusive;
            }
            return;
        }
    };
    report.metrics = benchmark_metrics_from_summary(&summary);
    if let Some(verdict) = benchmark_summary_verdict(&summary) {
        let verdict_outcome = verdict.outcome.clone();
        report.summary_verdict = Some(verdict.summary);
        match verdict_outcome {
            EvaluationOutcome::Failed => report.outcome = EvaluationOutcome::Failed,
            EvaluationOutcome::Inconclusive => {
                if report.outcome == EvaluationOutcome::Passed
                    || summary_host_dependency_blocking_issue_count(&summary) > 0
                {
                    report.outcome = EvaluationOutcome::Inconclusive;
                }
            }
            EvaluationOutcome::Passed => {}
        }
    }
    report.summary = Some(summary);
}

fn enrich_benchmark_report_from_progress(report: &mut BenchmarkGateReport) {
    let Some(progress_path) = report.progress_path.clone() else {
        return;
    };
    let path = PathBuf::from(&progress_path);
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(source) => {
            report.progress_error = Some(format!(
                "could not read benchmark progress `{}`: {source}",
                path.display()
            ));
            if report.outcome == EvaluationOutcome::Passed {
                report.outcome = EvaluationOutcome::Inconclusive;
            }
            return;
        }
    };
    let mut event_count = 0usize;
    let mut last_event = None;
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(event) => event,
            Err(source) => {
                report.progress_error = Some(format!(
                    "could not parse benchmark progress `{}` line {}: {source}",
                    path.display(),
                    index + 1
                ));
                if report.outcome == EvaluationOutcome::Passed {
                    report.outcome = EvaluationOutcome::Inconclusive;
                }
                return;
            }
        };
        event_count += 1;
        last_event = Some(event);
    }
    if event_count == 0 {
        report.progress_error = Some(format!(
            "benchmark progress `{}` did not contain any events",
            path.display()
        ));
        if report.outcome == EvaluationOutcome::Passed {
            report.outcome = EvaluationOutcome::Inconclusive;
        }
        return;
    }
    report.progress_event_count = Some(event_count);
    report.progress_last_event = last_event;
}

fn benchmark_metrics_from_summary(summary: &serde_json::Value) -> Vec<EvaluationMetric> {
    let mut metrics = Vec::new();
    push_summary_integer_metric(
        &mut metrics,
        "swebench_total_instances",
        summary,
        &[&["total_instances"], &["suite", "total_instances"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_resolved_instances",
        summary,
        &[
            &["resolved_instances"],
            &["official_evaluation", "resolved_instances"],
            &["sinomo", "resolved_instances"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_error_instances",
        summary,
        &[
            &["error_instances"],
            &["official_evaluation", "error_instances"],
            &["sinomo", "error_instances"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_harness_blocked_instances",
        summary,
        &[
            &["harness_blocked_instances"],
            &["sinomo", "harness_blocked_instances"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_missing_report_instances",
        summary,
        &[
            &["missing_report_instances"],
            &["sinomo", "missing_report_instances"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_unresolved_instances",
        summary,
        &[
            &["unresolved_instances"],
            &["sinomo", "unresolved_instances"],
            &["official_evaluation", "unresolved_instances"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_official_completed_instances",
        summary,
        &[
            &["official_completed_instances"],
            &["sinomo", "official_completed_instances"],
            &["official_evaluation", "completed_instances"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_official_empty_patch_instances",
        summary,
        &[
            &["official_empty_patch_instances"],
            &["sinomo", "official_empty_patch_instances"],
            &["official_evaluation", "empty_patch_instances"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_model_turns",
        summary,
        &[&["agent_model_turns"], &["sinomo", "agent_model_turns"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_tool_results",
        summary,
        &[&["agent_tool_results"], &["sinomo", "agent_tool_results"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_event_count",
        summary,
        &[&["agent_event_count"], &["sinomo", "agent_event_count"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_item_count",
        summary,
        &[&["agent_item_count"], &["sinomo", "agent_item_count"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_tool_call_items",
        summary,
        &[
            &["agent_tool_call_items"],
            &["sinomo", "agent_tool_call_items"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_tool_result_items",
        summary,
        &[
            &["agent_tool_result_items"],
            &["sinomo", "agent_tool_result_items"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_orphan_tool_calls",
        summary,
        &[&["orphan_tool_calls"], &["sinomo", "orphan_tool_calls"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_orphan_tool_results",
        summary,
        &[&["orphan_tool_results"], &["sinomo", "orphan_tool_results"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_input_tokens",
        summary,
        &[&["agent_input_tokens"], &["sinomo", "agent_input_tokens"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_output_tokens",
        summary,
        &[&["agent_output_tokens"], &["sinomo", "agent_output_tokens"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_model_response_count",
        summary,
        &[
            &["agent_model_response_count"],
            &["sinomo", "agent_model_response_count"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_model_response_parse_failures",
        summary,
        &[
            &["agent_model_response_parse_failures"],
            &["sinomo", "agent_model_response_parse_failures"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_strict_json_decision_turns",
        summary,
        &[
            &["agent_strict_json_decision_turns"],
            &["sinomo", "agent_strict_json_decision_turns"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_text_fallback_turns",
        summary,
        &[
            &["agent_text_fallback_turns"],
            &["sinomo", "agent_text_fallback_turns"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_native_tool_call_turns",
        summary,
        &[
            &["agent_native_tool_call_turns"],
            &["sinomo", "agent_native_tool_call_turns"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_native_tool_calls",
        summary,
        &[
            &["agent_native_tool_calls"],
            &["sinomo", "agent_native_tool_calls"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_vendor_fallback_tool_call_turns",
        summary,
        &[
            &["agent_vendor_fallback_tool_call_turns"],
            &["sinomo", "agent_vendor_fallback_tool_call_turns"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_vendor_fallback_tool_calls",
        summary,
        &[
            &["agent_vendor_fallback_tool_calls"],
            &["sinomo", "agent_vendor_fallback_tool_calls"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_model_repair_attempts",
        summary,
        &[
            &["agent_model_repair_attempts"],
            &["sinomo", "agent_model_repair_attempts"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "agent_repair_request_model_visible_bytes",
        summary,
        &[
            &["agent_repair_request_model_visible_bytes"],
            &["sinomo", "agent_repair_request_model_visible_bytes"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_instances_with_successful_patch",
        summary,
        &[
            &["instances_with_successful_patch"],
            &["sinomo", "instances_with_successful_patch"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_first_successful_patch_turn_sum",
        summary,
        &[
            &["first_successful_patch_turn_sum"],
            &["sinomo", "first_successful_patch_turn_sum"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_first_successful_patch_turn_min",
        summary,
        &[
            &["first_successful_patch_turn_min"],
            &["sinomo", "first_successful_patch_turn_min"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_first_successful_patch_turn_max",
        summary,
        &[
            &["first_successful_patch_turn_max"],
            &["sinomo", "first_successful_patch_turn_max"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_coding_validation_attempts",
        summary,
        &[
            &["coding_validation_attempts"],
            &["sinomo", "coding_validation_attempts"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_coding_finish_with_patch_instances",
        summary,
        &[
            &["coding_stop_recommendation_counts", "finish_with_patch"],
            &[
                "sinomo",
                "coding_stop_recommendation_counts",
                "finish_with_patch",
            ],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_coding_finish_blocked_instances",
        summary,
        &[
            &["coding_stop_recommendation_counts", "finish_blocked"],
            &[
                "sinomo",
                "coding_stop_recommendation_counts",
                "finish_blocked",
            ],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_coding_failed_environment_instances",
        summary,
        &[
            &["coding_validation_status_counts", "failed_environment"],
            &[
                "sinomo",
                "coding_validation_status_counts",
                "failed_environment",
            ],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_coding_failed_patch_relevant_instances",
        summary,
        &[
            &["coding_validation_status_counts", "failed_patch_relevant"],
            &[
                "sinomo",
                "coding_validation_status_counts",
                "failed_patch_relevant",
            ],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_coding_missing_command_failures",
        summary,
        &[
            &["coding_validation_failure_kind_counts", "missing_command"],
            &[
                "sinomo",
                "coding_validation_failure_kind_counts",
                "missing_command",
            ],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_coding_missing_dependency_failures",
        summary,
        &[
            &[
                "coding_validation_failure_kind_counts",
                "missing_dependency",
            ],
            &[
                "sinomo",
                "coding_validation_failure_kind_counts",
                "missing_dependency",
            ],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_coding_patch_relevant_failures",
        summary,
        &[
            &["coding_validation_failure_kind_counts", "patch_relevant"],
            &[
                "sinomo",
                "coding_validation_failure_kind_counts",
                "patch_relevant",
            ],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_command_observation_turns",
        summary,
        &[
            &["command_observation_turns"],
            &["sinomo", "command_observation_turns"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_command_tool_results",
        summary,
        &[
            &["command_tool_results"],
            &["sinomo", "command_tool_results"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_cancelled_tool_results",
        summary,
        &[
            &["cancelled_tool_results"],
            &["sinomo", "cancelled_tool_results"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_cancelled_runs",
        summary,
        &[&["cancelled_runs"], &["sinomo", "cancelled_runs"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_extra_turns_after_patch",
        summary,
        &[
            &["extra_turns_after_patch"],
            &["sinomo", "extra_turns_after_patch"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_host_dependency_blocking_issues",
        summary,
        &[&["host_dependencies", "blocking_issue_count"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_patch_bytes",
        summary,
        &[&["patch_bytes"], &["sinomo", "patch_bytes"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_changed_files",
        summary,
        &[&["changed_files"], &["sinomo", "changed_files"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_post_patch_check_failures",
        summary,
        &[
            &["post_patch_check_failures"],
            &["sinomo", "post_patch_check_failures"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_initial_post_patch_check_failures",
        summary,
        &[
            &["initial_post_patch_check_failures"],
            &["sinomo", "initial_post_patch_check_failures"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_repair_attempts",
        summary,
        &[&["repair_attempts"], &["sinomo", "repair_attempts"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_post_patch_repair_attempts",
        summary,
        &[
            &["post_patch_repair_attempts"],
            &["sinomo", "post_patch_repair_attempts"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_empty_patch_repair_attempts",
        summary,
        &[
            &["empty_patch_repair_attempts"],
            &["sinomo", "empty_patch_repair_attempts"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_repair_model_turns",
        summary,
        &[&["repair_model_turns"], &["sinomo", "repair_model_turns"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_repair_tool_results",
        summary,
        &[&["repair_tool_results"], &["sinomo", "repair_tool_results"]],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_deterministic_repair_attempts",
        summary,
        &[
            &["deterministic_repair_attempts"],
            &["sinomo", "deterministic_repair_attempts"],
        ],
    );
    push_summary_integer_metric(
        &mut metrics,
        "swebench_deterministic_repair_changed_files",
        summary,
        &[
            &["deterministic_repair_changed_files"],
            &["sinomo", "deterministic_repair_changed_files"],
        ],
    );
    metrics
}

fn benchmark_summary_verdict(summary: &serde_json::Value) -> Option<BenchmarkSummaryVerdict> {
    let benchmark = summary.get("benchmark").and_then(|value| value.as_str());
    if benchmark != Some("swebench") {
        return None;
    }

    let host_dependency_blockers = summary_host_dependency_blocking_issue_count(summary);
    if host_dependency_blockers > 0 {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Inconclusive,
            summary: format!(
                "SWE-bench host dependencies blocked {host_dependency_blockers} issue(s)"
            ),
        });
    }

    let (harness_blocked, missing_reports) = summary_harness_incomplete_counts(summary);
    if harness_blocked > 0 || missing_reports > 0 {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Inconclusive,
            summary: format!(
                "SWE-bench harness evidence incomplete: {harness_blocked} harness-blocked instance(s), {missing_reports} missing report instance(s)"
            ),
        });
    }

    let (orphan_tool_calls, orphan_tool_results) = summary_orphan_tool_pair_counts(summary);
    if orphan_tool_calls > 0 || orphan_tool_results > 0 {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Failed,
            summary: format!(
                "SWE-bench reported {orphan_tool_calls} orphan tool call(s) and {orphan_tool_results} orphan tool result(s)"
            ),
        });
    }
    if let Some((tool_call_items, tool_result_items)) =
        summary_tool_pair_item_count_mismatch(summary)
    {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Failed,
            summary: format!(
                "SWE-bench reported tool call/result item mismatch: {tool_call_items} call item(s), {tool_result_items} result item(s)"
            ),
        });
    }

    let total = summary_integer_at(summary, &["total_instances"])
        .or_else(|| summary_integer_at(summary, &["suite", "total_instances"]));
    let resolved = summary_integer_at(summary, &["resolved_instances"])
        .or_else(|| summary_integer_at(summary, &["official_evaluation", "resolved_instances"]))
        .or_else(|| summary_integer_at(summary, &["sinomo", "resolved_instances"]));
    let errors = [
        summary_integer_at(summary, &["error_instances"]),
        summary_integer_at(summary, &["official_evaluation", "error_instances"]),
        summary_integer_at(summary, &["sinomo", "error_instances"]),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(0);
    let post_patch_check_failures =
        summary_integer_at(summary, &["sinomo", "post_patch_check_failures"])
            .or_else(|| summary_integer_at(summary, &["post_patch_check_failures"]))
            .unwrap_or(0);

    let Some(total) = total else {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Inconclusive,
            summary: "SWE-bench summary did not report total_instances".to_string(),
        });
    };
    let Some(resolved) = resolved else {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Inconclusive,
            summary: "SWE-bench summary did not report resolved_instances".to_string(),
        });
    };
    if total <= 0 {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Inconclusive,
            summary: format!("SWE-bench summary reported invalid total_instances={total}"),
        });
    }
    if errors > 0 {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Failed,
            summary: format!("SWE-bench reported {errors} error instance(s)"),
        });
    }
    if post_patch_check_failures > 0 {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Failed,
            summary: format!(
                "SWE-bench reported {post_patch_check_failures} post-patch check failure(s)"
            ),
        });
    }
    if resolved < total {
        return Some(BenchmarkSummaryVerdict {
            outcome: EvaluationOutcome::Failed,
            summary: format!("SWE-bench resolved {resolved}/{total} instance(s)"),
        });
    }
    Some(BenchmarkSummaryVerdict {
        outcome: EvaluationOutcome::Passed,
        summary: format!("SWE-bench resolved {resolved}/{total} instance(s)"),
    })
}

fn push_summary_integer_metric(
    metrics: &mut Vec<EvaluationMetric>,
    name: &str,
    summary: &serde_json::Value,
    paths: &[&[&str]],
) {
    if let Some(value) = paths
        .iter()
        .find_map(|path| summary_integer_at(summary, path))
    {
        metrics.push(EvaluationMetric {
            name: name.to_string(),
            value: MetricValue::Integer(value),
            sample_count: None,
        });
    }
}

fn summary_integer_at(summary: &serde_json::Value, path: &[&str]) -> Option<i64> {
    let mut value = summary;
    for segment in path {
        value = value.get(*segment)?;
    }
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn summary_host_dependency_blocking_issue_count(summary: &serde_json::Value) -> i64 {
    summary_integer_at(summary, &["host_dependencies", "blocking_issue_count"]).unwrap_or(0)
}

fn summary_harness_incomplete_counts(summary: &serde_json::Value) -> (i64, i64) {
    (
        summary_integer_at(summary, &["harness_blocked_instances"])
            .or_else(|| summary_integer_at(summary, &["sinomo", "harness_blocked_instances"]))
            .unwrap_or(0),
        summary_integer_at(summary, &["missing_report_instances"])
            .or_else(|| summary_integer_at(summary, &["sinomo", "missing_report_instances"]))
            .unwrap_or(0),
    )
}

fn summary_orphan_tool_pair_counts(summary: &serde_json::Value) -> (i64, i64) {
    (
        summary_integer_at(summary, &["orphan_tool_calls"])
            .or_else(|| summary_integer_at(summary, &["sinomo", "orphan_tool_calls"]))
            .unwrap_or(0),
        summary_integer_at(summary, &["orphan_tool_results"])
            .or_else(|| summary_integer_at(summary, &["sinomo", "orphan_tool_results"]))
            .unwrap_or(0),
    )
}

fn summary_tool_pair_item_count_mismatch(summary: &serde_json::Value) -> Option<(i64, i64)> {
    let tool_call_items = summary_integer_at(summary, &["agent_tool_call_items"])
        .or_else(|| summary_integer_at(summary, &["sinomo", "agent_tool_call_items"]))?;
    let tool_result_items = summary_integer_at(summary, &["agent_tool_result_items"])
        .or_else(|| summary_integer_at(summary, &["sinomo", "agent_tool_result_items"]))?;
    (tool_call_items != tool_result_items).then_some((tool_call_items, tool_result_items))
}

fn apply_benchmark_reports_to_scorecard(
    scorecard: &mut EvaluationScorecard,
    reports: &[BenchmarkGateReport],
) {
    if reports.is_empty() {
        return;
    }
    remove_raw_benchmark_gates(scorecard, reports);
    scorecard.metrics.push(EvaluationMetric {
        name: "benchmark_count".to_string(),
        value: MetricValue::Integer(reports.len() as i64),
        sample_count: Some(reports.len() as u64),
    });
    for report in reports {
        scorecard.metrics.extend(report.metrics.clone());
        if let Some(summary_error) = &report.summary_error {
            scorecard
                .gates
                .push(sinomo_runtime::core::EvaluationGateResult {
                    gate: EvaluationGate::PrivateRegressionEvals,
                    outcome: EvaluationOutcome::Inconclusive,
                    summary: format!(
                        "benchmark `{}` summary evidence is incomplete: {summary_error}",
                        report.name
                    ),
                });
            if scorecard.outcome == EvaluationOutcome::Passed {
                scorecard.outcome = EvaluationOutcome::Inconclusive;
            }
        }
        if let Some(progress_error) = &report.progress_error {
            scorecard
                .gates
                .push(sinomo_runtime::core::EvaluationGateResult {
                    gate: EvaluationGate::PrivateRegressionEvals,
                    outcome: EvaluationOutcome::Inconclusive,
                    summary: format!(
                        "benchmark `{}` progress evidence is incomplete: {progress_error}",
                        report.name
                    ),
                });
            if scorecard.outcome == EvaluationOutcome::Passed {
                scorecard.outcome = EvaluationOutcome::Inconclusive;
            }
        }
        if let Some(summary_verdict) = &report.summary_verdict {
            scorecard
                .gates
                .push(sinomo_runtime::core::EvaluationGateResult {
                    gate: EvaluationGate::PrivateRegressionEvals,
                    outcome: report.outcome.clone(),
                    summary: format!("benchmark `{}` verdict: {summary_verdict}", report.name),
                });
        }
        if report.outcome == EvaluationOutcome::Failed {
            scorecard.outcome = EvaluationOutcome::Failed;
        } else if report.outcome == EvaluationOutcome::Inconclusive
            && scorecard.outcome == EvaluationOutcome::Passed
        {
            scorecard.outcome = EvaluationOutcome::Inconclusive;
        }
    }
}

fn remove_raw_benchmark_gates(
    scorecard: &mut EvaluationScorecard,
    reports: &[BenchmarkGateReport],
) {
    let benchmark_gate_prefixes: Vec<String> = reports
        .iter()
        .map(|report| format!("`{}` exited", report.command))
        .collect();
    scorecard.gates.retain(|gate| {
        gate.gate != EvaluationGate::PrivateRegressionEvals
            || !benchmark_gate_prefixes
                .iter()
                .any(|prefix| gate.summary.starts_with(prefix))
    });
    scorecard.outcome = scorecard_outcome_from_gates(&scorecard.gates);
}

fn scorecard_outcome_from_gates(
    gates: &[sinomo_runtime::core::EvaluationGateResult],
) -> EvaluationOutcome {
    if gates
        .iter()
        .any(|gate| gate.outcome == EvaluationOutcome::Failed)
    {
        return EvaluationOutcome::Failed;
    }
    if gates
        .iter()
        .any(|gate| gate.outcome == EvaluationOutcome::Inconclusive)
    {
        return EvaluationOutcome::Inconclusive;
    }
    EvaluationOutcome::Passed
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvolutionProjectKind {
    Sinomo,
    Rust,
    Node,
    Python,
    Generic,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EvolutionProjectProfile {
    kind: EvolutionProjectKind,
    name: String,
}

impl EvolutionProjectProfile {
    fn summary(&self) -> String {
        format!("{} ({})", self.name, self.kind.label())
    }

    fn default_verification_commands(&self) -> Vec<VerificationCommand> {
        match self.kind {
            EvolutionProjectKind::Sinomo | EvolutionProjectKind::Rust => {
                vec![VerificationCommand {
                    name: "cargo check".to_string(),
                    command: "cargo check".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }]
            }
            EvolutionProjectKind::Node
            | EvolutionProjectKind::Python
            | EvolutionProjectKind::Generic => {
                vec![VerificationCommand {
                    name: "git diff check".to_string(),
                    command: "git diff --check".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }]
            }
        }
    }

    fn edit_notes(&self) -> String {
        match self.kind {
            EvolutionProjectKind::Sinomo => {
                "bootstrap supervised Sinomo self-evolution iteration".to_string()
            }
            EvolutionProjectKind::Rust => {
                "supervised evolution iteration for an external Rust project".to_string()
            }
            EvolutionProjectKind::Node => {
                "supervised evolution iteration for an external Node/JavaScript project; provide explicit --verify commands for project-specific tests".to_string()
            }
            EvolutionProjectKind::Python => {
                "supervised evolution iteration for an external Python project; provide explicit --verify commands for project-specific tests".to_string()
            }
            EvolutionProjectKind::Generic => {
                "supervised evolution iteration for an external project; provide explicit --verify commands for project-specific tests".to_string()
            }
        }
    }
}

impl EvolutionProjectKind {
    fn label(self) -> &'static str {
        match self {
            EvolutionProjectKind::Sinomo => "sinomo",
            EvolutionProjectKind::Rust => "rust",
            EvolutionProjectKind::Node => "node",
            EvolutionProjectKind::Python => "python",
            EvolutionProjectKind::Generic => "generic",
        }
    }
}

fn project_profile_for_root(source_root: &Path) -> EvolutionProjectProfile {
    let kind = if source_root
        .join("modules")
        .join("evolution")
        .join("sinomo-evolution")
        .exists()
        && source_root
            .join("docs")
            .join("self-evolution-architecture.md")
            .exists()
    {
        EvolutionProjectKind::Sinomo
    } else if source_root.join("Cargo.toml").exists() {
        EvolutionProjectKind::Rust
    } else if source_root.join("package.json").exists() {
        EvolutionProjectKind::Node
    } else if source_root.join("pyproject.toml").exists()
        || source_root.join("setup.py").exists()
        || source_root.join("requirements.txt").exists()
    {
        EvolutionProjectKind::Python
    } else {
        EvolutionProjectKind::Generic
    };
    let name = source_root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("project")
        .to_string();
    EvolutionProjectProfile { kind, name }
}

fn create_bootstrap_job(request: &EvolutionRequest, source_root: &Path) -> EvolutionJob {
    let job_id = EvolutionJobId::generate();
    let project = project_profile_for_root(source_root);
    let commands = if request.verification_commands.is_empty() {
        project.default_verification_commands()
    } else {
        request.verification_commands.clone()
    };
    let mut required_gates: Vec<EvaluationGate> = commands
        .iter()
        .map(|command| command.gate.clone())
        .collect();
    if !request.module_replay_reports.is_empty() {
        if !required_gates.contains(&EvaluationGate::ReplayTests) {
            required_gates.push(EvaluationGate::ReplayTests);
        }
        if !required_gates.contains(&EvaluationGate::ShadowMode) {
            required_gates.push(EvaluationGate::ShadowMode);
        }
    }
    EvolutionJob {
        job_id,
        objective: request.objective.clone(),
        target_ring: request.target_ring.clone(),
        target_module_id: None,
        edit_scope: EditScope {
            workspace_root: source_root.display().to_string(),
            intended_paths: request.intended_paths.clone(),
            notes: project.edit_notes(),
        },
        verification: VerificationPlan {
            commands,
            module_replay_reports: request.module_replay_reports.clone(),
        },
        recovery: RecoveryPlan::default(),
        scope: EvolutionScope {
            allowed_paths: request.intended_paths.clone(),
            forbidden_paths: Vec::new(),
            workspace_id: None,
        },
        status: EvolutionJobStatus::Created,
        required_gates,
        rollback_policy: RollbackPolicy::default(),
    }
}

fn mutation_packet_for_job(job: &EvolutionJob) -> MutationPacket {
    MutationPacket {
        job_id: job.job_id.clone(),
        objective: job.objective.clone(),
        target_ring: job.target_ring.clone(),
        edit_scope: job.edit_scope.clone(),
        relevant_files: select_relevant_files(
            Path::new(&job.edit_scope.workspace_root),
            &job.edit_scope.intended_paths,
        ),
        verification: job.verification.clone(),
        output_contract: MutationOutputContract::default(),
        recovery: job.recovery.clone(),
    }
}

fn select_relevant_files(workspace_root: &Path, intended_paths: &[String]) -> Vec<FileExcerpt> {
    const MAX_FILES: usize = 12;
    const MAX_BYTES_PER_FILE: usize = 16 * 1024;

    let mut candidates = Vec::new();
    for intended_path in intended_paths {
        let root = workspace_root.join(glob_base_path(intended_path));
        if root.is_file() {
            candidates.push(root);
        } else if root.is_dir() {
            collect_relevant_files(&root, &mut candidates, MAX_FILES);
        }
        if candidates.len() >= MAX_FILES {
            break;
        }
    }
    candidates.sort();
    candidates.dedup();
    candidates.truncate(MAX_FILES);

    candidates
        .into_iter()
        .filter_map(|path| {
            let bytes = fs::read(&path).ok()?;
            let content =
                String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_BYTES_PER_FILE)]).into_owned();
            let relative = path
                .strip_prefix(workspace_root)
                .unwrap_or(&path)
                .display()
                .to_string()
                .replace('\\', "/");
            Some(FileExcerpt {
                path: relative,
                content,
            })
        })
        .collect()
}

fn glob_base_path(pattern: &str) -> &str {
    pattern
        .split(['*', '?', '['])
        .next()
        .unwrap_or(pattern)
        .trim_end_matches(['/', '\\'])
}

fn collect_relevant_files(dir: &Path, candidates: &mut Vec<PathBuf>, max_files: usize) {
    if candidates.len() >= max_files || is_ignored_dir(dir) {
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        if candidates.len() >= max_files {
            break;
        }
        let path = entry.path();
        if path.is_dir() {
            collect_relevant_files(&path, candidates, max_files);
        } else if is_relevant_source_file(&path) {
            candidates.push(path);
        }
    }
}

fn is_ignored_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| matches!(name, ".git" | "target" | "node_modules" | ".evolution"))
}

fn is_relevant_source_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if matches!(
        name,
        "Cargo.toml"
            | "README.md"
            | "package.json"
            | "pyproject.toml"
            | "requirements.txt"
            | "setup.py"
    ) {
        return true;
    }
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("rs" | "toml" | "md" | "json" | "js" | "jsx" | "ts" | "tsx" | "py" | "yaml" | "yml")
    )
}

fn write_command_log_via_store<B: CandidateBundleStore>(
    store: &B,
    manifest: &CandidateBundleManifest,
    result: &CommandRunResult,
) -> Result<CommandLogRef, BundleError> {
    let safe_name = bundle_safe_file_stem(&result.name);
    let path = format!("command-logs/{safe_name}.txt");
    let text = format!(
        "name: {}\ncommand: {}\nexpected_exit_code: {}\nexit_code: {}\nduration_ms: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        result.name,
        result.command,
        result.expected_exit_code,
        result.exit_code,
        result.duration_ms,
        result.stdout,
        result.stderr
    );
    store.write_text(manifest, &path, &text)?;
    Ok(CommandLogRef {
        name: result.name.clone(),
        command: result.command.clone(),
        log_path: PathBuf::from(&manifest.bundle_path)
            .join(&path)
            .display()
            .to_string(),
        exit_code: Some(result.exit_code),
    })
}

fn bundle_safe_file_stem(value: &str) -> String {
    let stem: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    stem.trim_matches('-').to_string().if_empty("command")
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(not(windows))]
fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':' | '='))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(windows)]
fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':' | '='))
    {
        return value.to_string();
    }
    format!("\"{}\"", value.replace('"', "`\""))
}

trait IfEmpty {
    fn if_empty(self, fallback: &str) -> String;
}

impl IfEmpty for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

pub fn verify_stored_evolution_job<R: CommandRunner>(
    paths: &EvolutionPaths,
    record: StoredEvolutionJob,
    harness: &ExecutingEvalHarness<R>,
) -> Result<RecordedCandidateVerification, EvolutionError> {
    let (scorecard, results) = harness.run_verification_plan(
        record.job.verification.clone().into(),
        &record.candidate_workspace.path,
    )?;
    record_candidate_verification(paths, record, &scorecard, &results).map_err(Into::into)
}

pub fn refresh_stored_candidate_bundle_integrity(
    paths: &EvolutionPaths,
    record: &mut StoredEvolutionJob,
) -> Result<(), EvolutionError> {
    let verification_outcome = record
        .verification_outcome
        .clone()
        .unwrap_or(EvaluationOutcome::Inconclusive);
    finalize_candidate_bundle_integrity(
        &mut record.bundle,
        &record.job,
        &record.candidate_workspace,
        None,
        verification_outcome,
        &record.changed_files,
    )?;
    write_bundle_manifest(record)?;
    write_stored_job(paths, record)?;
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CandidateDecisionOptions {
    pub override_preflight: bool,
}

pub fn decide_stored_evolution_job(
    paths: &EvolutionPaths,
    record: StoredEvolutionJob,
    outcome: CandidateDecisionOutcome,
    decided_by: impl Into<String>,
    note: impl Into<String>,
) -> Result<RecordedCandidateDecision, EvolutionError> {
    decide_stored_evolution_job_with_options(
        paths,
        record,
        outcome,
        decided_by,
        note,
        CandidateDecisionOptions::default(),
    )
}

pub fn decide_stored_evolution_job_with_options(
    paths: &EvolutionPaths,
    mut record: StoredEvolutionJob,
    outcome: CandidateDecisionOutcome,
    decided_by: impl Into<String>,
    note: impl Into<String>,
    options: CandidateDecisionOptions,
) -> Result<RecordedCandidateDecision, EvolutionError> {
    let note = note.into();
    let approval_preflight = if outcome == CandidateDecisionOutcome::Approved {
        let report = approval_preflight_report(paths, &record, options.override_preflight)?;
        if !report.passed && !options.override_preflight {
            return Err(EvolutionError::ApprovalPreflight {
                detail: failed_preflight_summary(&report),
            });
        }
        let mut rollout_material_path = None;
        if preflight_check_passed(&report, "candidate-bundle-integrity") {
            let registry_record = write_candidate_bundle_registry_record(paths, &mut record)?;
            if preflight_check_passed(&report, "candidate-verified-rollout-material") {
                rollout_material_path = write_candidate_verified_rollout_material_if_present(
                    paths,
                    &record,
                    &registry_record,
                )?;
            }
        }
        let candidate_summary_path =
            write_candidate_summary(&record, &report, rollout_material_path.as_deref())?;
        record.bundle.candidate_summary_path = Some(candidate_summary_path.display().to_string());
        Some(report)
    } else {
        None
    };
    let recorded = record_candidate_decision_with_preflight(
        paths,
        record,
        outcome.clone(),
        decided_by,
        note,
        approval_preflight,
    )?;
    if outcome == CandidateDecisionOutcome::Approved {
        commit_and_fast_forward_approved_candidate(&recorded.record)?;
    }
    Ok(recorded)
}

fn preflight_check_passed(report: &CandidateApprovalPreflightReport, name: &str) -> bool {
    report
        .checks
        .iter()
        .any(|check| check.name == name && check.outcome == EvaluationOutcome::Passed)
}

fn approval_preflight_report(
    paths: &EvolutionPaths,
    record: &StoredEvolutionJob,
    override_used: bool,
) -> Result<CandidateApprovalPreflightReport, EvolutionError> {
    let diff_summary = candidate_diff_summary(record);
    let benchmark_outcome = stored_benchmark_outcome(record);
    let mut checks = Vec::new();
    checks.push(CandidateApprovalPreflightCheck {
        name: "verification-passed".to_string(),
        outcome: if record.verification_outcome == Some(EvaluationOutcome::Passed) {
            EvaluationOutcome::Passed
        } else {
            EvaluationOutcome::Failed
        },
        summary: match &record.verification_outcome {
            Some(outcome) => format!("candidate verification outcome is {outcome:?}"),
            None => "candidate has no recorded verification outcome".to_string(),
        },
    });
    checks.push(candidate_diff_preflight_check(record));
    checks.push(candidate_bundle_integrity_preflight_check(record));
    checks.push(candidate_verified_rollout_material_preflight_check(
        paths, record,
    ));
    checks.push(source_clean_preflight_check(record));
    checks.push(source_fast_forward_preflight_check(record));
    let passed = checks
        .iter()
        .all(|check| check.outcome == EvaluationOutcome::Passed);
    Ok(CandidateApprovalPreflightReport {
        objective: record.job.objective.clone(),
        verification_outcome: record.verification_outcome.clone(),
        benchmark_outcome,
        diff_summary,
        override_used,
        passed,
        checks,
        recovery_note: record.job.recovery.note.clone(),
    })
}

fn candidate_bundle_integrity_preflight_check(
    record: &StoredEvolutionJob,
) -> CandidateApprovalPreflightCheck {
    let evidence_manifest = candidate_bundle_artifact_evidence_manifest(&record.bundle);
    match verify_artifact_evidence_integrity(&evidence_manifest) {
        Ok(report) if report.is_valid() => CandidateApprovalPreflightCheck {
            name: "candidate-bundle-integrity".to_string(),
            outcome: EvaluationOutcome::Passed,
            summary: format!(
                "bundle integrity verified with {} checks; content hash {}",
                report.checks.len(),
                report
                    .recomputed_content_hash
                    .as_deref()
                    .unwrap_or("unavailable")
            ),
        },
        Ok(report) => CandidateApprovalPreflightCheck {
            name: "candidate-bundle-integrity".to_string(),
            outcome: EvaluationOutcome::Failed,
            summary: format!(
                "bundle integrity verification failed: {}",
                report
                    .failed_checks()
                    .join(", ")
                    .if_empty("unknown failure")
            ),
        },
        Err(error) => CandidateApprovalPreflightCheck {
            name: "candidate-bundle-integrity".to_string(),
            outcome: EvaluationOutcome::Failed,
            summary: format!("bundle integrity verification could not run: {error}"),
        },
    }
}

fn candidate_verified_rollout_material_preflight_check(
    paths: &EvolutionPaths,
    record: &StoredEvolutionJob,
) -> CandidateApprovalPreflightCheck {
    let material = match read_release_material_from_path(candidate_bundle_release_material_path(
        &record.bundle,
    )) {
        Ok(Some(material)) => material,
        Ok(None) => {
            return CandidateApprovalPreflightCheck {
                name: "candidate-verified-rollout-material".to_string(),
                outcome: EvaluationOutcome::Passed,
                summary: format!(
                    "candidate bundle does not contain `{}`",
                    RELEASE_MATERIAL_RELATIVE_PATH
                ),
            };
        }
        Err(error) => {
            return CandidateApprovalPreflightCheck {
                name: "candidate-verified-rollout-material".to_string(),
                outcome: EvaluationOutcome::Failed,
                summary: format!("candidate rollout material could not be read: {error}"),
            };
        }
    };

    let mut registry = ArtifactRegistry::new();
    let evidence_manifest = candidate_bundle_artifact_evidence_manifest(&record.bundle);
    let artifact_evidence = match registry.register_verified_artifact_evidence(&evidence_manifest) {
        Ok(artifact_evidence) => artifact_evidence.clone(),
        Err(error) => {
            return CandidateApprovalPreflightCheck {
                name: "candidate-verified-rollout-material".to_string(),
                outcome: EvaluationOutcome::Failed,
                summary: format!(
                    "candidate rollout material cannot be validated until bundle integrity passes: {error}"
                ),
            };
        }
    };
    let trusted_signers = match read_trusted_artifact_signers(&paths.data_dir) {
        Ok(signers) => signers,
        Err(error) => {
            return CandidateApprovalPreflightCheck {
                name: "candidate-verified-rollout-material".to_string(),
                outcome: EvaluationOutcome::Failed,
                summary: format!("trusted artifact signers could not be read: {error}"),
            };
        }
    };
    let material = materialize_candidate_verified_rollout_material(
        material,
        &record.bundle,
        &artifact_evidence,
    );
    let report =
        validate_verified_release_material(&material, &artifact_evidence, &trusted_signers);
    CandidateApprovalPreflightCheck {
        name: "candidate-verified-rollout-material".to_string(),
        outcome: if report.is_valid() {
            EvaluationOutcome::Passed
        } else {
            EvaluationOutcome::Failed
        },
        summary: if report.is_valid() {
            format!(
                "verified rollout material is valid for artifact {} and rollout plan {}",
                report.artifact_id, report.rollout_plan_id
            )
        } else {
            format!(
                "verified rollout material failed validation: {}",
                report.failed_checks().join(", ")
            )
        },
    }
}

fn candidate_diff_preflight_check(record: &StoredEvolutionJob) -> CandidateApprovalPreflightCheck {
    let diff_has_content = record
        .bundle
        .diff_path
        .as_ref()
        .and_then(|path| fs::read_to_string(path).ok())
        .is_some_and(|diff| !diff.trim().is_empty());
    let has_diff = diff_has_content || !record.changed_files.is_empty();
    CandidateApprovalPreflightCheck {
        name: "candidate-diff-present".to_string(),
        outcome: if has_diff {
            EvaluationOutcome::Passed
        } else {
            EvaluationOutcome::Failed
        },
        summary: if has_diff {
            candidate_diff_summary(record)
        } else {
            "candidate has no recorded diff or changed files".to_string()
        },
    }
}

fn source_clean_preflight_check(record: &StoredEvolutionJob) -> CandidateApprovalPreflightCheck {
    let source_root = Path::new(&record.job.edit_scope.workspace_root);
    match run_git_command(source_root, &["status", "--porcelain"]) {
        Ok(status) if status.trim().is_empty() => CandidateApprovalPreflightCheck {
            name: "source-worktree-clean".to_string(),
            outcome: EvaluationOutcome::Passed,
            summary: "source worktree is clean".to_string(),
        },
        Ok(status) => CandidateApprovalPreflightCheck {
            name: "source-worktree-clean".to_string(),
            outcome: EvaluationOutcome::Failed,
            summary: format!("source worktree has uncommitted changes: {}", status.trim()),
        },
        Err(error) => CandidateApprovalPreflightCheck {
            name: "source-worktree-clean".to_string(),
            outcome: EvaluationOutcome::Failed,
            summary: error.to_string(),
        },
    }
}

fn source_fast_forward_preflight_check(
    record: &StoredEvolutionJob,
) -> CandidateApprovalPreflightCheck {
    let source_root = Path::new(&record.job.edit_scope.workspace_root);
    let branch = record.candidate_workspace.branch_name.as_str();
    match run_git_command(
        source_root,
        &["merge-base", "--is-ancestor", "HEAD", branch],
    ) {
        Ok(_) => CandidateApprovalPreflightCheck {
            name: "source-fast-forward".to_string(),
            outcome: EvaluationOutcome::Passed,
            summary: format!("source HEAD can fast-forward to `{branch}`"),
        },
        Err(error) => CandidateApprovalPreflightCheck {
            name: "source-fast-forward".to_string(),
            outcome: EvaluationOutcome::Failed,
            summary: format!("source HEAD cannot fast-forward to `{branch}`: {error}"),
        },
    }
}

fn candidate_diff_summary(record: &StoredEvolutionJob) -> String {
    if record.changed_files.is_empty() {
        return "0 changed files".to_string();
    }
    let mut paths = record.changed_files.clone();
    paths.sort();
    let shown = paths.iter().take(8).cloned().collect::<Vec<_>>();
    let suffix = if paths.len() > shown.len() {
        format!(" and {} more", paths.len() - shown.len())
    } else {
        String::new()
    };
    format!(
        "{} changed file(s): {}{}",
        paths.len(),
        shown.join(", "),
        suffix
    )
}

fn stored_benchmark_outcome(record: &StoredEvolutionJob) -> Option<EvaluationOutcome> {
    let scorecard_path = record.bundle.scorecard_path.as_ref()?;
    let scorecard = fs::read_to_string(scorecard_path).ok()?;
    let scorecard: EvaluationScorecard = serde_json::from_str(&scorecard).ok()?;
    let benchmark_outcomes = scorecard
        .gates
        .iter()
        .filter(|gate| gate.gate == EvaluationGate::PrivateRegressionEvals)
        .map(|gate| gate.outcome.clone())
        .collect::<Vec<_>>();
    if benchmark_outcomes.is_empty() {
        None
    } else if benchmark_outcomes
        .iter()
        .any(|outcome| outcome == &EvaluationOutcome::Failed)
    {
        Some(EvaluationOutcome::Failed)
    } else if benchmark_outcomes
        .iter()
        .any(|outcome| outcome == &EvaluationOutcome::Inconclusive)
    {
        Some(EvaluationOutcome::Inconclusive)
    } else {
        Some(EvaluationOutcome::Passed)
    }
}

fn failed_preflight_summary(report: &CandidateApprovalPreflightReport) -> String {
    report
        .checks
        .iter()
        .filter(|check| check.outcome != EvaluationOutcome::Passed)
        .map(|check| format!("{}: {}", check.name, check.summary))
        .collect::<Vec<_>>()
        .join("; ")
        .if_empty("approval preflight failed")
}

fn write_candidate_bundle_registry_record(
    paths: &EvolutionPaths,
    record: &mut StoredEvolutionJob,
) -> Result<ArtifactEvidenceRecord, EvolutionError> {
    let mut registry = ArtifactRegistry::new();
    let evidence_manifest = candidate_bundle_artifact_evidence_manifest(&record.bundle);
    let registry_record = registry
        .register_verified_artifact_evidence(&evidence_manifest)?
        .clone();
    let path = PathBuf::from(&record.bundle.bundle_path).join("artifact-evidence-record.json");
    write_json_file_for_bundle(&path, &registry_record)?;
    sinomo_runtime::registry::write_artifact_evidence_record(&paths.data_dir, &registry_record)?;
    record.bundle.registry_record_path = Some(path.display().to_string());
    Ok(registry_record)
}

fn write_candidate_verified_rollout_material_if_present(
    paths: &EvolutionPaths,
    record: &StoredEvolutionJob,
    artifact_evidence: &ArtifactEvidenceRecord,
) -> Result<Option<PathBuf>, EvolutionError> {
    let Some(material) =
        read_release_material_from_path(candidate_bundle_release_material_path(&record.bundle))?
    else {
        return Ok(None);
    };
    let material = materialize_candidate_verified_rollout_material(
        material,
        &record.bundle,
        artifact_evidence,
    );
    let trusted_signers = read_trusted_artifact_signers(&paths.data_dir)?;
    let (path, _report) = verify_and_write_verified_release_material(
        &paths.data_dir,
        &material,
        artifact_evidence,
        &trusted_signers,
    )?;
    Ok(Some(path))
}

fn materialize_candidate_verified_rollout_material(
    mut material: VerifiedReleaseMaterialRecord,
    bundle: &CandidateBundleManifest,
    artifact_evidence: &ArtifactEvidenceRecord,
) -> VerifiedReleaseMaterialRecord {
    material.registered_at = OffsetDateTime::now_utc();
    material.release_id = artifact_evidence.evidence_id.clone();
    material.release_material_hash = artifact_evidence.content_hash.clone();
    material.evidence_refs = candidate_verified_rollout_material_evidence_refs(
        material.evidence_refs,
        bundle,
        artifact_evidence,
    );
    material
}

fn candidate_verified_rollout_material_evidence_refs(
    mut refs: Vec<String>,
    bundle: &CandidateBundleManifest,
    artifact_evidence: &ArtifactEvidenceRecord,
) -> Vec<String> {
    refs.push(format!(
        "registry://artifact-evidence/{}",
        artifact_evidence.evidence_id
    ));
    refs.push(
        candidate_bundle_release_material_path(bundle)
            .display()
            .to_string(),
    );
    refs.extend(artifact_evidence.evidence_refs.iter().cloned());
    refs.extend(bundle.registry_record_path.iter().cloned());
    refs.sort();
    refs.dedup();
    refs
}

fn write_candidate_summary(
    record: &StoredEvolutionJob,
    report: &CandidateApprovalPreflightReport,
    rollout_material_path: Option<&Path>,
) -> Result<PathBuf, EvolutionError> {
    let path = PathBuf::from(&record.bundle.bundle_path).join("candidate-summary.md");
    let changed_files = if record.changed_files.is_empty() {
        "none recorded".to_string()
    } else {
        record.changed_files.join("\n")
    };
    let checks = report
        .checks
        .iter()
        .map(|check| format!("- {}: {:?} - {}", check.name, check.outcome, check.summary))
        .collect::<Vec<_>>()
        .join("\n");
    let registry_record = record
        .bundle
        .registry_record_path
        .as_deref()
        .unwrap_or("not recorded");
    let rollout_material = rollout_material_path
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "not recorded".to_string());
    let agent_run_summary = render_candidate_agent_run_summary(record)?;
    let summary = format!(
        "# Candidate Summary\n\njob: {}\nstatus: awaiting approval\nobjective: {}\nverification: {:?}\nbenchmark: {:?}\noverride_used: {}\nregistry_record: {}\nverified_rollout_material: {}\n\n## Agent Run\n{}\n\n## Diff\n{}\n\n## Changed Files\n{}\n\n## Approval Preflight\n{}\n\n## Recovery\n{}\n",
        record.job.job_id,
        record.job.objective,
        record.verification_outcome,
        report.benchmark_outcome,
        report.override_used,
        registry_record,
        rollout_material,
        agent_run_summary,
        report.diff_summary,
        changed_files,
        checks,
        report.recovery_note
    );
    fs::write(&path, summary).map_err(|source| {
        EvolutionError::Bundle(BundleError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    })?;
    Ok(path)
}

fn render_candidate_agent_run_summary(
    record: &StoredEvolutionJob,
) -> Result<String, EvolutionError> {
    let Some(path) = record.bundle.agent_summary_path.as_deref() else {
        return Ok("not recorded".to_string());
    };
    let path = PathBuf::from(path);
    let content = fs::read_to_string(&path).map_err(|source| {
        EvolutionError::Bundle(BundleError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    })?;
    let summary: AgentMutationSummary = serde_json::from_str(&content).map_err(|source| {
        EvolutionError::Bundle(BundleError::Deserialize {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
    })?;
    Ok(render_agent_mutation_summary(&summary, &path))
}

fn render_agent_mutation_summary(summary: &AgentMutationSummary, path: &Path) -> String {
    let final_decision_source = summary
        .model_metrics
        .as_ref()
        .and_then(|metrics| metrics.get("final_decision_source"))
        .and_then(|value| value.as_str())
        .unwrap_or("not recorded");
    let tool_pair_integrity = summary
        .tool_pair_integrity
        .map(|integrity| {
            format!(
                "calls={}, results={}, orphan_calls={}, orphan_results={}",
                integrity.tool_calls,
                integrity.tool_results,
                integrity.orphan_tool_calls,
                integrity.orphan_tool_results
            )
        })
        .unwrap_or_else(|| "not recorded".to_string());
    format!(
        "artifact: {}\n- stop_reason: {}\n- model_turns: {}\n- tool_results: {}\n- tokens: input={}, output={}\n- final_decision_source: {}\n- tool_pair_integrity: {}\n",
        path.display(),
        summary.stop_reason,
        summary.model_turns,
        summary.tool_results,
        summary.input_tokens,
        summary.output_tokens,
        final_decision_source,
        tool_pair_integrity
    )
}

#[derive(Debug, Error)]
pub enum EvolutionError {
    #[error(transparent)]
    Persistence(#[from] EvolutionPersistenceError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error(transparent)]
    Eval(#[from] sinomo_runtime::eval::EvalError),
    #[error(transparent)]
    MutationModel(#[from] MutationModelError),
    #[error("lifecycle observer failed: {detail}")]
    LifecycleObserver { detail: String },
    #[error("approval preflight failed: {detail}")]
    ApprovalPreflight { detail: String },
    #[error(transparent)]
    Registry(#[from] sinomo_runtime::registry::RegistryError),
}

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("git command `{command}` failed: {detail}")]
    Git { command: String, detail: String },
    #[error("filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("failed to parse git status: {0}")]
    StatusParse(String),
}

#[derive(Debug, Error)]
pub enum BundleError {
    #[error("bundle filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("failed to serialize bundle artifact: {0}")]
    Serialize(String),
    #[error("failed to deserialize bundle artifact at {path}: {detail}")]
    Deserialize { path: String, detail: String },
}

#[derive(Debug, Error)]
pub enum MutationModelError {
    #[error(transparent)]
    LanguageModel(#[from] LanguageModelError),
    #[error("agent mutation failed: {detail}")]
    Agent { detail: String },
    #[error("failed to serialize mutation artifact: {detail}")]
    Serialization { detail: String },
    #[error("mutation tool `{tool}` failed: {detail}")]
    ToolExecution { tool: String, detail: String },
    #[error("mutation bridge exceeded max tool rounds ({max_rounds})")]
    ToolRoundLimitExceeded { max_rounds: usize },
}

#[cfg(test)]
mod tests;
