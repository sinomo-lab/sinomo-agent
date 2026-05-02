//! Autonomous self-evolution planning skeleton.

use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelError, LanguageModelMessage, LanguageModelProvider, LanguageModelRequest,
    LanguageModelResponseFormat, LanguageModelRole,
};
use sinomo_runtime::core::{
    CandidateBundleManifest, CommandLogRef, Confidence, EditScope, EvaluationGate,
    EvaluationScorecard, EventEnvelope, EventId, EventKind, EvolutionJob, EvolutionJobId,
    EvolutionJobStatus, EvolutionScope, FileExcerpt, LatencyClass, ModuleId,
    MutationOutputContract, MutationPacket, NodeId, PrivacyClass, RecoveryPlan, RollbackPolicy,
    RoutingHint, RoutingMetadata, RoutingScope, Salience, Source, TrustRing, VerificationCommand,
    VerificationPlan, WorkspaceId,
};
use sinomo_runtime::eval::{CommandRunResult, CommandRunner, ExecutingEvalHarness};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionOpportunity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_event_id: Option<EventId>,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_module_id: Option<ModuleId>,
    pub objective: String,
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_paths: Vec<String>,
    pub priority: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateWorkspacePlan {
    pub job_id: EvolutionJobId,
    pub workspace_id: WorkspaceId,
    pub branch_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionRequest {
    pub objective: String,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intended_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_commands: Vec<VerificationCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum CampaignScheduleDecision {
    Scheduled,
    Delayed,
    PausedBudget,
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
    campaign
        .budget_policy
        .reserve_floors
        .iter()
        .map(|floor| ResourceClassDefinition {
            resource_id: floor.resource.clone(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 0,
            reserve_floor_percent: floor.floor_percent,
            reset_window: "current reset window".to_string(),
            notes: "derived from campaign reserve-floor config".to_string(),
        })
        .collect()
}

fn default_resource_usage_estimates(
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
    (total_units.saturating_mul(reserve_floor_percent as u64) + 99) / 100
}

pub fn propose_evolution_campaign(request: CampaignProposalRequest) -> EvolutionCampaign {
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let campaign_id = generated_id("camp");
    let objective_id = generated_id("objcard");
    let objective_kind = classify_objective(&request.objective);
    let stages = stages_for_objective(objective_kind);
    let near_term_tasks = tasks_for_objective(objective_kind, &stages);
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
            evidence_refs: vec!["docs/plans/human-guided-long-horizon-evolution.md#phase-0-campaign-proposal-only".to_string()],
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
    format!(
        "{prefix}_{}",
        EventId::generate()
            .as_str()
            .trim_start_matches(EventId::PREFIX)
    )
}

fn proposal_constraints(request: &CampaignProposalRequest) -> Vec<String> {
    let mut constraints = vec![
        format!("horizon: {}", request.horizon),
        format!("budget policy: {}", request.budget_policy),
        "no formal capability registry is required for phase 0".to_string(),
    ];
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
            verification_commands: vec!["cargo test --workspace".to_string()],
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

impl WorkspaceAdapter for GitWorkspaceAdapter {
    fn root(&self) -> &Path {
        &self.source_root
    }

    fn prepare_candidate(&self, job: &EvolutionJob) -> Result<CandidateWorkspace, WorkspaceError> {
        fs::create_dir_all(&self.worktrees_root).map_err(|source| WorkspaceError::Io {
            path: self.worktrees_root.display().to_string(),
            detail: source.to_string(),
        })?;
        let source_revision = self.run_git(
            &["rev-parse".to_string(), "HEAD".to_string()],
            &self.source_root,
        )?;
        let branch_name = format!("codex/evolve/{}", job.job_id.as_str());
        let path = self
            .worktrees_root
            .join(job.job_id.as_str())
            .join("worktree");
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
                &self.source_root,
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

pub fn parse_git_status_paths(status: &str) -> Result<Vec<String>, String> {
    let mut paths = Vec::new();
    for line in status.lines().filter(|line| !line.trim().is_empty()) {
        if line.len() < 4 {
            return Err(format!("invalid porcelain status line: {line}"));
        }
        let path = line[3..].trim();
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
        let safe_name = safe_file_stem(&result.name);
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
            changed_files_path: None,
            diff_path: None,
            scorecard_path: None,
            decision_path: None,
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
    ) -> Result<ModelMutationResult, MutationModelError>;
}

mod tools;

pub use tools::{
    EvolutionTool, EvolutionToolCall, EvolutionToolDescriptor, EvolutionToolEventContext,
    EvolutionToolRegistry, EvolutionToolResult, ToolCallStatus, ToolEffect, ToolEvidence,
};

#[cfg(windows)]
fn shell_command(command: &str) -> Command {
    let mut shell = Command::new("powershell");
    shell.args(["-NoProfile", "-NonInteractive", "-Command", command]);
    shell
}

#[cfg(not(windows))]
fn shell_command(command: &str) -> Command {
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
    #[serde(default, alias = "verificationNotes")]
    pub verification_notes: Option<String>,
    #[serde(default, alias = "experimentNotes")]
    pub experiment_notes: Option<String>,
    #[serde(default, alias = "recoveryNotes")]
    pub recovery_notes: Option<String>,
}

pub struct LanguageModelMutationBridge<P> {
    provider: P,
    model: String,
    tool_registry: EvolutionToolRegistry,
    max_tool_rounds: usize,
}

impl<P> LanguageModelMutationBridge<P> {
    pub fn new(provider: P, model: impl Into<String>) -> Self {
        Self {
            provider,
            model: model.into(),
            tool_registry: EvolutionToolRegistry::bootstrap(),
            max_tool_rounds: 4,
        }
    }

    pub fn with_tool_registry(mut self, tool_registry: EvolutionToolRegistry) -> Self {
        self.tool_registry = tool_registry;
        self
    }

    pub fn with_max_tool_rounds(mut self, max_tool_rounds: usize) -> Self {
        self.max_tool_rounds = max_tool_rounds;
        self
    }
}

impl<P: LanguageModelProvider> MutationModel for LanguageModelMutationBridge<P> {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
    ) -> Result<ModelMutationResult, MutationModelError> {
        let info = self.provider.info();
        let response_format = if info.supports_json_object {
            LanguageModelResponseFormat::JsonObject
        } else {
            LanguageModelResponseFormat::Text
        };
        let direct_edit = info.supports_tool_permissions;
        let structured_response = !direct_edit;
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
                temperature: Some(0.2),
                max_output_tokens: None,
                response_format: response_format.clone(),
                metadata: serde_json::json!({
                    "job_id": packet.job_id,
                    "candidate_workspace": workspace.path,
                    "direct_edit": direct_edit,
                    "target_ring_policy": "ignored-during-bootstrap",
                    "turn": turn,
                    "max_tool_rounds": self.max_tool_rounds,
                }),
            };
            let response = self.provider.complete(&request)?;
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
        if !direct_edit {
            if let Some(patch) = parsed_patch
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
        })
    }
}

pub fn render_mutation_prompt(packet: &MutationPacket) -> String {
    let mut prompt = String::new();
    prompt.push_str("# Sinomo Mutation Packet\n\n");
    prompt.push_str(&format!("job_id: {}\n", packet.job_id));
    prompt.push_str("target_ring: ignored during current bootstrap pass\n");
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
        "Treat this workspace as the only mutation target. The running Sinomo instance may be using the source workspace separately.\n",
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
            "Return only a JSON object with keys: tool_calls, changed_files, patch, patch_summary, verification_notes, experiment_notes, recovery_notes.\n",
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
        "You are Sinomo's supervised bootstrap mutation worker. Work only on the requested R0/R1 candidate and produce durable evidence.",
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
        prompt.push_str(" Return valid JSON only.");
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
        let job = create_bootstrap_job(&request, self.workspace.root());
        let mut bundle = self.bundles.create(&job)?;
        let candidate_workspace = self.workspace.prepare_candidate(&job)?;
        let packet = mutation_packet_for_job(&job);

        self.bundles.write_json(&bundle, "job.json", &job)?;
        self.bundles
            .write_json(&bundle, "mutation-packet.json", &packet)?;

        if request.dry_run {
            // Dry-run proposals only prepare candidate artifacts and verification evidence.
        } else if let Some(model) = model {
            let result = model.mutate(&packet, &candidate_workspace)?;
            self.bundles
                .write_text(&bundle, "model-output.txt", &result.raw_output)?;
            self.bundles
                .write_json(&bundle, "model-result.json", &result)?;
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

        let (scorecard, command_results) = self
            .evaluator
            .run_verification_plan(job.verification.clone(), &candidate_workspace.path)?;
        for result in &command_results {
            let log_ref = write_command_log_via_store(&self.bundles, &bundle, result)?;
            bundle.command_logs.push(log_ref);
        }
        self.bundles
            .write_json(&bundle, "scorecard.json", &scorecard)?;
        bundle.scorecard_path = Some(
            PathBuf::from(&bundle.bundle_path)
                .join("scorecard.json")
                .display()
                .to_string(),
        );

        self.bundles.write_json(&bundle, "manifest.json", &bundle)?;

        Ok(EvolutionReport {
            job: EvolutionJob {
                status: EvolutionJobStatus::AwaitingApproval,
                ..job
            },
            candidate_workspace,
            bundle,
            scorecard: Some(scorecard),
            changed_files,
        })
    }
}

fn create_bootstrap_job(request: &EvolutionRequest, source_root: &Path) -> EvolutionJob {
    let job_id = EvolutionJobId::generate();
    let commands = if request.verification_commands.is_empty() {
        vec![VerificationCommand {
            name: "cargo check".to_string(),
            command: "cargo check".to_string(),
            expected_exit_code: 0,
            gate: EvaluationGate::StaticChecks,
        }]
    } else {
        request.verification_commands.clone()
    };
    let required_gates = commands
        .iter()
        .map(|command| command.gate.clone())
        .collect();
    EvolutionJob {
        job_id,
        objective: request.objective.clone(),
        target_ring: request.target_ring.clone(),
        target_module_id: None,
        edit_scope: EditScope {
            workspace_root: source_root.display().to_string(),
            intended_paths: request.intended_paths.clone(),
            notes: "bootstrap supervised self-evolution iteration".to_string(),
        },
        verification: VerificationPlan { commands },
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
    if matches!(name, "Cargo.toml" | "README.md") {
        return true;
    }
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("rs" | "toml" | "md" | "json")
    )
}

fn write_command_log_via_store<B: CandidateBundleStore>(
    store: &B,
    manifest: &CandidateBundleManifest,
    result: &CommandRunResult,
) -> Result<CommandLogRef, BundleError> {
    let safe_name = safe_file_stem(&result.name);
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

fn safe_file_stem(value: &str) -> String {
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

#[derive(Debug, Error)]
pub enum EvolutionError {
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error(transparent)]
    Eval(#[from] sinomo_runtime::eval::EvalError),
    #[error(transparent)]
    MutationModel(#[from] MutationModelError),
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
}

#[derive(Debug, Error)]
pub enum MutationModelError {
    #[error(transparent)]
    LanguageModel(#[from] LanguageModelError),
    #[error("mutation tool `{tool}` failed: {detail}")]
    ToolExecution { tool: String, detail: String },
    #[error("mutation bridge exceeded max tool rounds ({max_rounds})")]
    ToolRoundLimitExceeded { max_rounds: usize },
}

#[derive(Clone, Debug)]
pub struct EvolutionPlanner {
    node_id: NodeId,
}

impl EvolutionPlanner {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn scope_job(
        &self,
        opportunity: EvolutionOpportunity,
    ) -> (EvolutionJob, CandidateWorkspacePlan) {
        let job_id = EvolutionJobId::generate();
        let required_gates = required_gates_for(&opportunity.target_ring);
        let job = EvolutionJob {
            job_id: job_id.clone(),
            objective: opportunity.objective,
            target_ring: opportunity.target_ring,
            target_module_id: opportunity.target_module_id,
            edit_scope: sinomo_runtime::core::EditScope {
                workspace_root: opportunity.workspace_id.to_string(),
                intended_paths: opportunity.allowed_paths.clone(),
                notes: "planner-scoped evolution opportunity".to_string(),
            },
            verification: sinomo_runtime::core::VerificationPlan {
                commands: required_gates
                    .iter()
                    .map(|gate| sinomo_runtime::core::VerificationCommand {
                        name: format!("{gate:?}"),
                        command: "cargo check".to_string(),
                        expected_exit_code: 0,
                        gate: gate.clone(),
                    })
                    .collect(),
            },
            recovery: sinomo_runtime::core::RecoveryPlan::default(),
            scope: EvolutionScope {
                allowed_paths: opportunity.allowed_paths.clone(),
                forbidden_paths: opportunity.forbidden_paths.clone(),
                workspace_id: Some(opportunity.workspace_id.clone()),
            },
            status: EvolutionJobStatus::Scoped,
            required_gates,
            rollback_policy: RollbackPolicy::default(),
        };
        let branch_name = format!("evolution/{}", job_id.as_str());
        let workspace = CandidateWorkspacePlan {
            job_id,
            workspace_id: opportunity.workspace_id,
            branch_name,
            allowed_paths: opportunity.allowed_paths,
            forbidden_paths: opportunity.forbidden_paths,
        };
        (job, workspace)
    }

    pub fn job_event(
        &self,
        job: &EvolutionJob,
        workspace: &CandidateWorkspacePlan,
    ) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("evolution.planner").expect("valid source"),
            EventKind::new("evolution.job.scoped").expect("valid event kind"),
            Salience::new(0.86).expect("valid salience"),
            Confidence::new(0.97).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            )
            .with_hint(
                RoutingHint::new(format!("target:ring:{}", trust_ring_hint(&job.target_ring)))
                    .expect("hint"),
            ),
        )
        .with_payload(serde_json::json!({
            "job": job,
            "candidate_workspace": workspace,
        }));
        if let Some(workspace_id) = &job.scope.workspace_id {
            event.evidence.refs.push(sinomo_runtime::core::EvidenceRef {
                uri: format!("dfs://{workspace_id}"),
                kind: sinomo_runtime::core::EvidenceKind::Other,
                content_hash: None,
            });
        }
        event
    }
}

pub fn required_gates_for(target_ring: &TrustRing) -> Vec<EvaluationGate> {
    if target_ring.requires_continuity_gates() {
        vec![
            EvaluationGate::ManifestValidation,
            EvaluationGate::StaticChecks,
            EvaluationGate::IntegrationTests,
            EvaluationGate::ReplayTests,
            EvaluationGate::ContinuityChecks,
        ]
    } else {
        vec![
            EvaluationGate::ManifestValidation,
            EvaluationGate::ReplayTests,
        ]
    }
}

fn trust_ring_hint(target_ring: &TrustRing) -> &'static str {
    match target_ring {
        TrustRing::R0ContinuityRoot => "r0",
        TrustRing::R1TrustedRuntime => "r1",
        TrustRing::R2TrustedService => "r2",
        TrustRing::R3SandboxedModule => "r3",
        TrustRing::R4DisposableWorkspace => "r4",
        TrustRing::R5ModelProvider => "r5",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_lm::{
        LanguageModelProviderInfo, LanguageModelResponse, LanguageModelUsage,
        PROVIDER_DEFAULT_MODEL,
    };
    use sinomo_runtime::core::EvaluationOutcome;
    use sinomo_runtime::eval::LocalCommandRunner;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn campaign_proposal_contains_inspectable_phase_zero_plan() {
        let proposal = propose_evolution_campaign(CampaignProposalRequest {
            objective: "improve the self-evolution scheduler so it can pause and resume around resource budgets".to_string(),
            horizon: "30 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: vec![ReserveFloor {
                resource: "coding-plan-weekly".to_string(),
                floor_percent: 30,
            }],
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: true,
            dry_run: true,
            approved: false,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some("/workspace/sinomo".to_string()),
        });

        assert!(proposal.campaign_id.starts_with("camp_"));
        assert_eq!(proposal.status, EvolutionCampaignStatus::AwaitingApproval);
        assert_eq!(proposal.objective_card.initial_horizon, "30 days");
        assert!(
            proposal
                .objective_card
                .failure_conditions
                .iter()
                .any(|condition| condition.contains("reserve"))
        );
        assert!(proposal.stages.len() >= 3);
        assert!(
            proposal
                .stages
                .iter()
                .all(|stage| !stage.acceptance_metrics.is_empty())
        );
        assert!(
            proposal
                .stages
                .iter()
                .all(|stage| !stage.failure_conditions.is_empty())
        );
        assert!(proposal.near_term_tasks.len() >= 2);
        assert_eq!(proposal.budget_policy.reserve_floors[0].floor_percent, 30);
        assert!(
            proposal
                .budget_policy
                .scheduling_notes
                .iter()
                .any(|note| note.contains("max wall time: 10m"))
        );
        assert!(
            proposal
                .budget_policy
                .scheduling_notes
                .iter()
                .any(|note| note.contains("pause after proposal"))
        );
        assert_eq!(
            proposal.iteration_results,
            Vec::<EvolutionIterationResult>::new()
        );
        assert!(
            proposal
                .decisions
                .iter()
                .any(|decision| decision.kind == CampaignDecisionKind::ProposalCreated)
        );
        assert_eq!(
            proposal.approval_checkpoint.status,
            ApprovalCheckpointStatus::AwaitingHuman
        );
        assert!(proposal.capability_registry_required == Some(false));
    }

    #[test]
    fn resource_scheduler_dry_run_denies_leases_that_violate_reserve_floor() {
        let campaign = propose_evolution_campaign(CampaignProposalRequest {
            objective: "improve the self-evolution scheduler so it can pause and resume around resource budgets".to_string(),
            horizon: "30 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: vec![ReserveFloor {
                resource: "coding-plan-weekly".to_string(),
                floor_percent: 30,
            }],
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: false,
            dry_run: true,
            approved: true,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some("/workspace/sinomo".to_string()),
        });
        let task_id = campaign.near_term_tasks[0].task_id.clone();

        let dry_run = dry_run_resource_scheduler(ResourceSchedulerDryRunRequest {
            campaign: campaign.clone(),
            preview_horizon: "24h".to_string(),
            resources: vec![ResourceClassDefinition {
                resource_id: "coding-plan-weekly".to_string(),
                unit: "percent".to_string(),
                total_units: 100,
                used_units: 65,
                reserve_floor_percent: 30,
                reset_window: "weekly reset window".to_string(),
                notes: "free coding plan budget".to_string(),
            }],
            task_estimates: vec![ResourceUsageEstimate {
                task_id: task_id.clone(),
                resource: "coding-plan-weekly".to_string(),
                estimated_units: 10,
                actual_units: Some(65),
            }],
        });

        assert_eq!(
            dry_run.aggregate_decision,
            CampaignScheduleDecision::PausedBudget
        );
        assert_eq!(dry_run.resource_ledger.resources[0].used_units, 65);
        assert_eq!(dry_run.resource_ledger.usage_records[0].estimated_units, 10);
        assert_eq!(
            dry_run.resource_ledger.usage_records[0].actual_units,
            Some(65)
        );
        assert_eq!(
            dry_run.lease_decisions[0].outcome,
            ResourceLeaseOutcome::Denied
        );
        assert!(dry_run.lease_decisions[0].reason.contains("reserve floor"));
        assert_eq!(dry_run.task_decisions[0].task_id, task_id);
        assert_eq!(
            dry_run.task_decisions[0].decision,
            TaskScheduleDecisionKind::PausedBudget
        );
    }

    #[test]
    fn resource_scheduler_dry_run_grants_leases_when_reserve_floor_remains_safe() {
        let campaign = propose_evolution_campaign(CampaignProposalRequest {
            objective: "improve the self-evolution scheduler so it can pause and resume around resource budgets".to_string(),
            horizon: "30 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: vec![ReserveFloor {
                resource: "coding-plan-weekly".to_string(),
                floor_percent: 30,
            }],
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: false,
            dry_run: true,
            approved: true,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some("/workspace/sinomo".to_string()),
        });
        let task_id = campaign.near_term_tasks[0].task_id.clone();

        let dry_run = dry_run_resource_scheduler(ResourceSchedulerDryRunRequest {
            campaign,
            preview_horizon: "24h".to_string(),
            resources: vec![ResourceClassDefinition {
                resource_id: "coding-plan-weekly".to_string(),
                unit: "percent".to_string(),
                total_units: 100,
                used_units: 40,
                reserve_floor_percent: 30,
                reset_window: "weekly reset window".to_string(),
                notes: "free coding plan budget".to_string(),
            }],
            task_estimates: vec![ResourceUsageEstimate {
                task_id: task_id.clone(),
                resource: "coding-plan-weekly".to_string(),
                estimated_units: 10,
                actual_units: Some(40),
            }],
        });

        assert_eq!(
            dry_run.aggregate_decision,
            CampaignScheduleDecision::Scheduled
        );
        assert_eq!(
            dry_run.lease_decisions[0].outcome,
            ResourceLeaseOutcome::Granted
        );
        assert_eq!(
            dry_run.task_decisions[0].decision,
            TaskScheduleDecisionKind::RunNow
        );
        assert!(dry_run.next_24h_preview.iter().any(|entry| {
            entry.task_id == task_id
                && entry
                    .explanation
                    .contains("reserve floor remains protected")
        }));
    }

    #[test]
    fn r1_job_receives_continuity_gates_and_workspace_scope() {
        let planner = EvolutionPlanner::new(NodeId::new("node_desktop").expect("node"));
        let (job, workspace) = planner.scope_job(EvolutionOpportunity {
            observed_event_id: None,
            target_ring: TrustRing::R1TrustedRuntime,
            target_module_id: None,
            objective: "harden event store".to_string(),
            workspace_id: WorkspaceId::new("workspace_runtime").expect("workspace"),
            allowed_paths: vec!["crates/sinomo-events/**".to_string()],
            forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
            priority: 0.9,
        });

        assert!(
            job.required_gates
                .contains(&EvaluationGate::ContinuityChecks)
        );
        assert_eq!(job.scope.workspace_id, Some(workspace.workspace_id));
        assert!(workspace.branch_name.starts_with("evolution/evojob_"));
    }

    #[test]
    fn git_status_parser_handles_modified_untracked_and_renamed_paths() {
        let paths = parse_git_status_paths(" M crates/a.rs\n?? docs/new.md\nR  old.rs -> new.rs\n")
            .expect("status paths");

        assert_eq!(
            paths,
            vec![
                "crates/a.rs".to_string(),
                "docs/new.md".to_string(),
                "new.rs".to_string()
            ]
        );
    }

    #[test]
    fn mutation_prompt_contains_objective_verification_and_recovery() {
        let packet = MutationPacket {
            job_id: EvolutionJobId::new("evojob_prompt").expect("job"),
            objective: "improve bundle summaries".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            edit_scope: EditScope {
                workspace_root: "candidate".to_string(),
                intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                notes: "keep it focused".to_string(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: vec![VerificationCommand {
                    name: "check".to_string(),
                    command: "cargo check".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let prompt = render_mutation_prompt(&packet);

        assert!(prompt.contains("improve bundle summaries"));
        assert!(prompt.contains("cargo check"));
        assert!(prompt.contains("recovery notes"));
        assert!(prompt.contains("restore the disposable VM snapshot"));
    }

    #[test]
    fn bridge_prompt_exposes_candidate_workspace_tools_and_json_contract() {
        let root = temp_dir("bridge-prompt");
        fs::create_dir_all(&root).expect("root");
        let workspace = candidate_workspace(&root);
        let packet = MutationPacket {
            job_id: workspace.job_id.clone(),
            objective: "add mutation evidence".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            edit_scope: EditScope {
                workspace_root: root.display().to_string(),
                intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                notes: "keep it focused".to_string(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: Vec::new(),
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let prompt = render_mutation_bridge_prompt(
            &packet,
            &workspace,
            &EvolutionToolRegistry::bootstrap().descriptors(),
            true,
            false,
        );

        assert!(prompt.contains("Candidate Workspace"));
        assert!(prompt.contains(&root.display().to_string()));
        assert!(prompt.contains("apply_patch"));
        assert!(prompt.contains("interrupt_command"));
        assert!(prompt.contains("kill_command"));
        assert!(prompt.contains("no further stdout/stderr"));
        assert!(prompt.contains("Required JSON Response"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn structured_mutation_output_parses_json_and_patch_fences() {
        let structured = parse_structured_mutation_output(
            r#"{"changed_files":["src/lib.rs"],"patch":"diff --git a/src/lib.rs b/src/lib.rs","patch_summary":"updated bridge"}"#,
        )
        .expect("structured output");

        assert_eq!(structured.changed_files, vec!["src/lib.rs".to_string()]);
        assert_eq!(structured.patch_summary.as_deref(), Some("updated bridge"));

        let patch = extract_patch_from_text(
            "notes\n```diff\ndiff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n```\n",
        )
        .expect("patch");
        assert!(patch.starts_with("diff --git"));

        let fenced = parse_structured_mutation_output(
            "```json\n{\"tool_calls\":[{\"tool\":\"read_file\",\"input\":{\"path\":\"Cargo.toml\"}}],\"changed_files\":[]}\n```",
        )
        .expect("fenced json");
        assert_eq!(fenced.tool_calls.len(), 1);
        assert_eq!(fenced.tool_calls[0].tool, "read_file");
    }

    #[test]
    fn mutation_packet_collects_relevant_source_excerpts_from_intended_paths() {
        let root = temp_dir("packet-excerpts");
        fs::create_dir_all(root.join("crates/example/src")).expect("dirs");
        fs::write(
            root.join("crates/example/Cargo.toml"),
            "[package]\nname = \"example\"\n",
        )
        .expect("manifest");
        fs::write(
            root.join("crates/example/src/lib.rs"),
            "pub fn example() {}\n",
        )
        .expect("lib");
        let job = create_bootstrap_job(
            &EvolutionRequest {
                objective: "collect context".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["crates/example/**".to_string()],
                verification_commands: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            &root,
        );

        let packet = mutation_packet_for_job(&job);

        assert!(
            packet
                .relevant_files
                .iter()
                .any(|file| file.path == "crates/example/Cargo.toml")
        );
        assert!(
            packet
                .relevant_files
                .iter()
                .any(|file| file.path == "crates/example/src/lib.rs")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn language_model_bridge_requests_json_and_applies_returned_patch() {
        let root = git_repo("bridge-applies-patch");
        fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
        run_in(&root, "git add patched.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        let workspace = candidate_workspace(&root);
        let provider = FakeLanguageModelProvider::new(
            LanguageModelProviderInfo {
                provider: "fake-json".to_string(),
                default_model: "fake-model".to_string(),
                invocation: sinomo_lm::LanguageModelInvocation::Http,
                supports_json_object: true,
                supports_tool_permissions: false,
            },
            serde_json::json!({
                "changed_files": ["patched.txt"],
                "patch": "diff --git a/patched.txt b/patched.txt\n--- a/patched.txt\n+++ b/patched.txt\n@@ -1 +1 @@\n-old\n+new\n",
                "patch_summary": "updated patched.txt",
                "verification_notes": "not run",
                "experiment_notes": "unit test fixture",
                "recovery_notes": "restore temp repo"
            })
            .to_string(),
        );
        let requests = provider.requests.clone();
        let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
        let packet = MutationPacket {
            job_id: workspace.job_id.clone(),
            objective: "apply patch from json provider".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            edit_scope: EditScope {
                workspace_root: root.display().to_string(),
                intended_paths: vec!["patched.txt".to_string()],
                notes: String::new(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: Vec::new(),
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let result = bridge.mutate(&packet, &workspace).expect("mutate");

        assert_eq!(
            requests.borrow()[0].response_format,
            LanguageModelResponseFormat::JsonObject
        );
        assert_eq!(result.summary.as_deref(), Some("updated patched.txt"));
        assert!(result.parsed_patch.as_deref().is_some());
        assert_eq!(result.tool_results[0].status, ToolCallStatus::Succeeded);
        let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
        assert_eq!(patched.replace("\r\n", "\n"), "new\n");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn language_model_bridge_runs_tool_rounds_before_final_patch() {
        let root = git_repo("bridge-tool-rounds");
        fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
        run_in(&root, "git add patched.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        let workspace = candidate_workspace(&root);
        let provider = FakeLanguageModelProvider::with_responses(
            LanguageModelProviderInfo {
                provider: "fake-tool-loop".to_string(),
                default_model: "fake-model".to_string(),
                invocation: sinomo_lm::LanguageModelInvocation::Http,
                supports_json_object: true,
                supports_tool_permissions: false,
            },
            vec![
                serde_json::json!({
                    "tool_calls": [
                        { "tool": "read_file", "input": { "path": "patched.txt" } }
                    ],
                    "changed_files": []
                })
                .to_string(),
                serde_json::json!({
                    "tool_calls": [],
                    "changed_files": ["patched.txt"],
                    "patch": "diff --git a/patched.txt b/patched.txt\n--- a/patched.txt\n+++ b/patched.txt\n@@ -1 +1 @@\n-old\n+new\n",
                    "patch_summary": "updated after reading file"
                })
                .to_string(),
            ],
        );
        let requests = provider.requests.clone();
        let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
        let packet = MutationPacket {
            job_id: workspace.job_id.clone(),
            objective: "use tools before patch".to_string(),
            target_ring: TrustRing::R3SandboxedModule,
            edit_scope: EditScope {
                workspace_root: root.display().to_string(),
                intended_paths: vec!["patched.txt".to_string()],
                notes: String::new(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: Vec::new(),
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let result = bridge.mutate(&packet, &workspace).expect("mutate");

        assert_eq!(requests.borrow().len(), 2);
        assert!(requests.borrow()[0].metadata["target_ring"].is_null());
        assert_eq!(
            requests.borrow()[0].metadata["target_ring_policy"],
            "ignored-during-bootstrap"
        );
        assert!(
            requests.borrow()[1]
                .messages
                .last()
                .unwrap()
                .content
                .contains("Tool results")
        );
        assert_eq!(result.model_turns.len(), 2);
        assert_eq!(result.tool_results[0].tool, "read_file");
        assert_eq!(result.tool_results[1].tool, "apply_patch");
        let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
        assert_eq!(patched.replace("\r\n", "\n"), "new\n");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn language_model_bridge_errors_when_final_patch_fails_to_apply() {
        let root = git_repo("bridge-bad-patch");
        fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
        run_in(&root, "git add patched.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        let workspace = candidate_workspace(&root);
        let provider = FakeLanguageModelProvider::new(
            LanguageModelProviderInfo {
                provider: "fake-json".to_string(),
                default_model: "fake-model".to_string(),
                invocation: sinomo_lm::LanguageModelInvocation::Http,
                supports_json_object: true,
                supports_tool_permissions: false,
            },
            serde_json::json!({
                "tool_calls": [],
                "changed_files": ["missing.txt"],
                "patch": "diff --git a/missing.txt b/missing.txt\n--- a/missing.txt\n+++ b/missing.txt\n@@ -1 +1 @@\n-old\n+new\n"
            })
            .to_string(),
        );
        let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
        let packet = MutationPacket {
            job_id: workspace.job_id.clone(),
            objective: "bad patch".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            edit_scope: EditScope {
                workspace_root: root.display().to_string(),
                intended_paths: vec!["missing.txt".to_string()],
                notes: String::new(),
            },
            relevant_files: Vec::new(),
            verification: VerificationPlan {
                commands: Vec::new(),
            },
            output_contract: MutationOutputContract::default(),
            recovery: RecoveryPlan::default(),
        };

        let error = bridge.mutate(&packet, &workspace).expect_err("bad patch");

        assert!(matches!(error, MutationModelError::ToolExecution { .. }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn local_bundle_store_writes_packet_and_manifest_paths() {
        let root = temp_dir("bundle-store");
        let store = LocalCandidateBundleStore::new(root.join("candidates"));
        let job = create_bootstrap_job(
            &EvolutionRequest {
                objective: "write bundle".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: Vec::new(),
                verification_commands: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            &root,
        );
        let manifest = store.create(&job).expect("bundle create");
        let packet = mutation_packet_for_job(&job);

        store
            .write_json(&manifest, "mutation-packet.json", &packet)
            .expect("packet write");

        assert!(PathBuf::from(&manifest.bundle_path).exists());
        assert!(PathBuf::from(&manifest.mutation_packet_path).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn orchestrator_dry_run_writes_bundle_and_runs_verification() {
        let root = temp_dir("orchestrator");
        let candidate = root.join("candidate");
        fs::create_dir_all(&candidate).expect("candidate dir");
        let orchestrator = EvolutionOrchestrator::new(
            FakeWorkspace {
                root: root.clone(),
                candidate: candidate.clone(),
            },
            ExecutingEvalHarness::new(LocalCommandRunner::new()),
            LocalCandidateBundleStore::new(root.join("candidates")),
        );

        let report = orchestrator
            .propose(
                EvolutionRequest {
                    objective: "dry run".to_string(),
                    target_ring: TrustRing::R1TrustedRuntime,
                    intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                    verification_commands: vec![VerificationCommand {
                        name: "success".to_string(),
                        command: "exit 0".to_string(),
                        expected_exit_code: 0,
                        gate: EvaluationGate::StaticChecks,
                    }],
                    model_provider: None,
                    model: None,
                    dry_run: true,
                },
                None,
            )
            .expect("dry run propose");

        assert_eq!(report.job.status, EvolutionJobStatus::AwaitingApproval);
        assert_eq!(
            report.scorecard.as_ref().expect("scorecard").outcome,
            EvaluationOutcome::Passed
        );
        assert!(PathBuf::from(&report.bundle.mutation_packet_path).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn orchestrator_writes_model_turn_and_tool_evidence_artifacts() {
        let root = temp_dir("orchestrator-model-evidence");
        let candidate = root.join("candidate");
        fs::create_dir_all(&candidate).expect("candidate dir");
        let orchestrator = EvolutionOrchestrator::new(
            FakeWorkspace {
                root: root.clone(),
                candidate: candidate.clone(),
            },
            ExecutingEvalHarness::new(LocalCommandRunner::new()),
            LocalCandidateBundleStore::new(root.join("jobs")),
        );
        let model = FakeMutationModel;

        let report = orchestrator
            .propose(
                EvolutionRequest {
                    objective: "write model evidence".to_string(),
                    target_ring: TrustRing::R1TrustedRuntime,
                    intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
                    verification_commands: vec![VerificationCommand {
                        name: "success".to_string(),
                        command: "exit 0".to_string(),
                        expected_exit_code: 0,
                        gate: EvaluationGate::StaticChecks,
                    }],
                    model_provider: None,
                    model: None,
                    dry_run: false,
                },
                Some(&model),
            )
            .expect("propose");
        let bundle = PathBuf::from(&report.bundle.bundle_path);

        assert!(bundle.join("model-output.txt").exists());
        assert!(bundle.join("model-result.json").exists());
        assert!(bundle.join("model-turns.json").exists());
        assert!(bundle.join("model-tool-results.json").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn git_workspace_adapter_places_worktree_inside_job_folder() {
        let source = git_repo("git-workspace-layout-source");
        fs::write(source.join("README.md"), "# source\n").expect("readme");
        run_in(&source, "git add README.md").expect("git add");
        run_in(&source, "git commit -m init").expect("git commit");
        let jobs_root = temp_dir("git-workspace-layout-jobs");
        let job = create_bootstrap_job(
            &EvolutionRequest {
                objective: "layout".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["README.md".to_string()],
                verification_commands: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            &source,
        );
        let adapter = GitWorkspaceAdapter::new(&source, &jobs_root);

        let workspace = adapter.prepare_candidate(&job).expect("candidate");

        assert_eq!(
            workspace.path,
            jobs_root.join(job.job_id.as_str()).join("worktree")
        );
        assert!(workspace.path.exists());
        let _ = run_in(
            &source,
            &format!("git worktree remove --force {}", workspace.path.display()),
        );
        let _ = fs::remove_dir_all(source);
        let _ = fs::remove_dir_all(jobs_root);
    }

    #[test]
    fn bootstrap_tool_registry_exposes_minimal_phase_three_tools() {
        let registry = EvolutionToolRegistry::bootstrap();
        let names = registry
            .descriptors()
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect::<Vec<_>>();

        for expected in [
            "read_file",
            "list_files",
            "search",
            "apply_patch",
            "run_command",
            "interrupt_command",
            "kill_command",
            "git_diff",
            "git_status",
            "format",
            "test",
        ] {
            assert!(names.iter().any(|name| name == expected), "{expected}");
        }
    }

    #[test]
    fn read_file_tool_returns_content_and_path_evidence() {
        let root = temp_dir("read-file-tool");
        fs::create_dir_all(&root).expect("root");
        fs::write(root.join("notes.txt"), "hello tools").expect("write note");
        let workspace = candidate_workspace(&root);
        let result = EvolutionToolRegistry::bootstrap().execute(
            &workspace,
            EvolutionToolCall {
                tool: "read_file".to_string(),
                input: serde_json::json!({ "path": "notes.txt" }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Succeeded);
        assert_eq!(result.output["content"], "hello tools");
        assert_eq!(result.evidence.paths, vec!["notes.txt".to_string()]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tool_registry_can_emit_execution_event_with_result_and_changed_files() {
        let root = git_repo("tool-event");
        fs::write(root.join("tracked.txt"), "before\n").expect("write tracked");
        run_in(&root, "git add tracked.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        fs::write(root.join("tracked.txt"), "after\n").expect("modify tracked");
        let workspace = candidate_workspace(&root);
        let context = EvolutionToolEventContext::new(
            NodeId::new("node_tool_test").expect("node"),
            workspace.job_id.clone(),
        );

        let (result, event) = EvolutionToolRegistry::bootstrap().execute_with_event(
            &workspace,
            &context,
            EvolutionToolCall {
                tool: "read_file".to_string(),
                input: serde_json::json!({ "path": "tracked.txt" }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Succeeded);
        assert_eq!(event.kind.as_str(), "evolution.tool.executed");
        assert_eq!(event.payload["tool"], "read_file");
        assert_eq!(event.payload["result"]["status"], "succeeded");
        assert_eq!(
            event.payload["changed_files"],
            serde_json::json!(["tracked.txt"])
        );
        assert_eq!(event.evidence.refs.len(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_command_tool_captures_exit_code_stdout_and_stderr() {
        let root = temp_dir("run-command-tool");
        fs::create_dir_all(&root).expect("root");
        let workspace = candidate_workspace(&root);
        let result = EvolutionToolRegistry::bootstrap().execute(
            &workspace,
            EvolutionToolCall {
                tool: "run_command".to_string(),
                input: serde_json::json!({ "command": output_command() }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Succeeded);
        assert_eq!(result.output["exit_code"], 0);
        assert!(result.output["run_id"].as_str().is_some());
        assert!(
            result.output["stdout"]
                .as_str()
                .unwrap()
                .contains("sinomo-stdout")
        );
        assert!(
            result.output["stderr"]
                .as_str()
                .unwrap()
                .contains("sinomo-stderr")
        );
        assert!(result.evidence.command.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn command_control_tool_reports_unknown_run_id() {
        let root = temp_dir("command-control-tool");
        fs::create_dir_all(&root).expect("root");
        let workspace = candidate_workspace(&root);
        let result = EvolutionToolRegistry::bootstrap().execute(
            &workspace,
            EvolutionToolCall {
                tool: "interrupt_command".to_string(),
                input: serde_json::json!({ "run_id": "cmd_missing" }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Failed);
        assert!(
            result.evidence.summary.contains("cmd_missing"),
            "{}",
            result.evidence.summary
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn kill_command_tool_can_terminate_active_run_command() {
        use std::thread;

        let root = temp_dir("kill-command-tool");
        fs::create_dir_all(&root).expect("root");
        let workspace = candidate_workspace(&root);
        let registry = std::sync::Arc::new(EvolutionToolRegistry::bootstrap());
        let running_registry = registry.clone();
        let running_workspace = workspace.clone();
        let handle = thread::spawn(move || {
            running_registry.execute(
                &running_workspace,
                EvolutionToolCall {
                    tool: "run_command".to_string(),
                    input: serde_json::json!({ "command": "sleep 5" }),
                },
            )
        });
        let run_id = wait_for_active_command(&registry);

        let control = registry.execute(
            &workspace,
            EvolutionToolCall {
                tool: "kill_command".to_string(),
                input: serde_json::json!({ "run_id": run_id }),
            },
        );
        let result = handle.join().expect("run command thread");

        assert_eq!(control.status, ToolCallStatus::Succeeded);
        assert_ne!(result.output["exit_code"], 0);
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    fn wait_for_active_command(registry: &EvolutionToolRegistry) -> String {
        for _ in 0..100 {
            if let Some(run_id) = registry.active_command_run_ids().into_iter().next() {
                return run_id;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("expected an active command run");
    }

    #[test]
    fn git_tools_report_status_and_diff_from_candidate_workspace() {
        let root = git_repo("git-tools");
        fs::write(root.join("tracked.txt"), "before\n").expect("write tracked");
        run_in(&root, "git add tracked.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        fs::write(root.join("tracked.txt"), "after\n").expect("modify tracked");
        let workspace = candidate_workspace(&root);
        let registry = EvolutionToolRegistry::bootstrap();

        let status = registry.execute(
            &workspace,
            EvolutionToolCall {
                tool: "git_status".to_string(),
                input: serde_json::json!({}),
            },
        );
        let diff = registry.execute(
            &workspace,
            EvolutionToolCall {
                tool: "git_diff".to_string(),
                input: serde_json::json!({}),
            },
        );

        assert_eq!(status.status, ToolCallStatus::Succeeded);
        assert!(
            status.output["stdout"]
                .as_str()
                .unwrap()
                .contains("tracked.txt")
        );
        assert_eq!(diff.status, ToolCallStatus::Succeeded);
        assert!(diff.output["stdout"].as_str().unwrap().contains("+after"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_patch_tool_applies_unified_diff_in_candidate_workspace() {
        let root = git_repo("apply-patch-tool");
        fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
        run_in(&root, "git add patched.txt").expect("git add");
        run_in(&root, "git commit -m init").expect("git commit");
        let workspace = candidate_workspace(&root);
        let patch = "\
diff --git a/patched.txt b/patched.txt
index 3b18e51..6320cd2 100644
--- a/patched.txt
+++ b/patched.txt
@@ -1 +1 @@
-old
+new
";
        let result = EvolutionToolRegistry::bootstrap().execute(
            &workspace,
            EvolutionToolCall {
                tool: "apply_patch".to_string(),
                input: serde_json::json!({ "patch": patch }),
            },
        );

        assert_eq!(result.status, ToolCallStatus::Succeeded);
        let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
        assert_eq!(patched.replace("\r\n", "\n"), "new\n");
        let _ = fs::remove_dir_all(root);
    }

    #[derive(Clone, Debug)]
    struct FakeLanguageModelProvider {
        info: LanguageModelProviderInfo,
        responses: Vec<String>,
        requests: Rc<RefCell<Vec<LanguageModelRequest>>>,
    }

    impl FakeLanguageModelProvider {
        fn new(info: LanguageModelProviderInfo, response: String) -> Self {
            Self::with_responses(info, vec![response])
        }

        fn with_responses(info: LanguageModelProviderInfo, responses: Vec<String>) -> Self {
            Self {
                info,
                responses,
                requests: Rc::new(RefCell::new(Vec::new())),
            }
        }
    }

    impl LanguageModelProvider for FakeLanguageModelProvider {
        fn info(&self) -> LanguageModelProviderInfo {
            self.info.clone()
        }

        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            let response_index = self.requests.borrow().len();
            self.requests.borrow_mut().push(request.clone());
            Ok(LanguageModelResponse {
                provider: self.info.provider.clone(),
                model: self.info.default_model.clone(),
                content: self
                    .responses
                    .get(response_index)
                    .or_else(|| self.responses.last())
                    .cloned()
                    .unwrap_or_default(),
                usage: Some(LanguageModelUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(20),
                }),
            })
        }
    }

    #[derive(Clone, Debug)]
    struct FakeMutationModel;

    impl MutationModel for FakeMutationModel {
        fn mutate(
            &self,
            _packet: &MutationPacket,
            _workspace: &CandidateWorkspace,
        ) -> Result<ModelMutationResult, MutationModelError> {
            let tool_result = EvolutionToolResult {
                tool: "read_file".to_string(),
                status: ToolCallStatus::Succeeded,
                output: serde_json::json!({ "content": "evidence" }),
                evidence: ToolEvidence {
                    summary: "read evidence".to_string(),
                    command: None,
                    exit_code: None,
                    duration_ms: None,
                    paths: vec!["evidence.txt".to_string()],
                },
            };
            Ok(ModelMutationResult {
                provider: "fake".to_string(),
                model: "fake-model".to_string(),
                raw_output: "{\"patch_summary\":\"fake mutation\"}".to_string(),
                parsed_patch: None,
                summary: Some("fake mutation".to_string()),
                structured_output: Some(StructuredMutationOutput {
                    patch_summary: Some("fake mutation".to_string()),
                    ..StructuredMutationOutput::default()
                }),
                tool_results: vec![tool_result.clone()],
                model_turns: vec![MutationModelTurn {
                    turn: 1,
                    raw_output: "{\"patch_summary\":\"fake mutation\"}".to_string(),
                    structured_output: Some(StructuredMutationOutput {
                        patch_summary: Some("fake mutation".to_string()),
                        ..StructuredMutationOutput::default()
                    }),
                    tool_results: vec![tool_result],
                }],
            })
        }
    }

    #[derive(Clone, Debug)]
    struct FakeWorkspace {
        root: PathBuf,
        candidate: PathBuf,
    }

    impl WorkspaceAdapter for FakeWorkspace {
        fn root(&self) -> &Path {
            &self.root
        }

        fn prepare_candidate(
            &self,
            job: &EvolutionJob,
        ) -> Result<CandidateWorkspace, WorkspaceError> {
            Ok(CandidateWorkspace {
                job_id: job.job_id.clone(),
                path: self.candidate.clone(),
                branch_name: "codex/evolve/test".to_string(),
                source_revision: Some("test".to_string()),
            })
        }

        fn changed_files(
            &self,
            _workspace: &CandidateWorkspace,
        ) -> Result<Vec<String>, WorkspaceError> {
            Ok(Vec::new())
        }

        fn diff(&self, _workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
            Ok(String::new())
        }

        fn status(&self, _workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
            Ok(String::new())
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-evolution-{name}-{stamp}"))
    }

    fn candidate_workspace(root: &Path) -> CandidateWorkspace {
        CandidateWorkspace {
            job_id: EvolutionJobId::new("evojob_tools").expect("job"),
            path: root.to_path_buf(),
            branch_name: "codex/evolve/tools".to_string(),
            source_revision: Some("test".to_string()),
        }
    }

    fn git_repo(name: &str) -> PathBuf {
        let root = temp_dir(name);
        fs::create_dir_all(&root).expect("root");
        run_in(&root, "git init").expect("git init");
        run_in(&root, "git config user.email sinomo@example.test").expect("git email");
        run_in(&root, "git config user.name Sinomo").expect("git name");
        root
    }

    fn run_in(cwd: &Path, command: &str) -> Result<(), String> {
        let output = super::shell_command(command)
            .current_dir(cwd)
            .output()
            .map_err(|error| error.to_string())?;
        if output.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        }
    }

    #[cfg(windows)]
    fn output_command() -> &'static str {
        "Write-Output sinomo-stdout; [Console]::Error.WriteLine('sinomo-stderr')"
    }

    #[cfg(not(windows))]
    fn output_command() -> &'static str {
        "printf sinomo-stdout; printf sinomo-stderr >&2"
    }
}
