//! Background scheduling and telemetry-intake policy.
//!
//! This module owns the background tick contract: cluster resource budgeting,
//! long-horizon workload scheduling, and conversion of detected opportunities
//! into human-gated campaigns.

use super::{
    ApprovalCheckpoint, ApprovalCheckpointStatus, CampaignBudgetPolicy, CampaignDecision,
    CampaignDecisionKind, CampaignProposalRequest, CampaignScheduleDecision,
    CandidateWorkspacePlan, DetectedEvolutionOpportunity, EvolutionCampaign,
    EvolutionCampaignStatus, EvolutionInputParseError, EvolutionJob,
    EvolutionOpportunityDetectionPolicy, EvolutionOpportunitySignalKind, EvolutionPlanner,
    EvolutionResultKind, ObjectiveCard, ResourceClassDefinition, ResourceLeaseDecision,
    ResourceLeaseOutcome, ResourceSchedulePreview, ResourceSchedulerDryRunRequest,
    ResourceUsageEstimate, ScheduledIterationPlan, ScheduledIterationRequest,
    detect_evolution_opportunities_from_events, dry_run_resource_scheduler, generated_id,
    parse_schedule_estimate, parse_schedule_resource, prepare_scheduled_iteration,
    propose_evolution_campaign, reserve_floor_units, resource_class_from_reserve_floor,
    resource_for_estimate,
};
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{EventEnvelope, TrustRing};
use sinomo_runtime::node::NodeId;
use std::collections::BTreeSet;
use time::OffsetDateTime;

pub fn parse_background_tick_resources(
    values: &[String],
    campaigns: &[EvolutionCampaign],
) -> Result<Vec<ResourceClassDefinition>, EvolutionInputParseError> {
    if values.is_empty() {
        return Ok(background_resource_classes_from_campaign_reserve_floors(
            campaigns,
            "derived from background campaign reserve floors",
        ));
    }
    let fallback = campaigns
        .first()
        .cloned()
        .unwrap_or_else(background_tick_fallback_campaign);
    values
        .iter()
        .map(|value| parse_schedule_resource(value, &fallback))
        .collect()
}

pub fn parse_background_tick_estimates(
    values: &[String],
    campaigns: &[EvolutionCampaign],
) -> Result<Vec<ResourceUsageEstimate>, EvolutionInputParseError> {
    if values.is_empty() {
        return Ok(Vec::new());
    }
    let Some(first_campaign) = campaigns.first() else {
        return Err(EvolutionInputParseError::Invalid {
            detail: "background estimates need at least one campaign".to_string(),
        });
    };
    values
        .iter()
        .map(|value| parse_schedule_estimate(value, first_campaign))
        .collect()
}

pub fn background_resource_classes_from_campaign_reserve_floors(
    campaigns: &[EvolutionCampaign],
    notes: &str,
) -> Vec<ResourceClassDefinition> {
    let mut resources: Vec<ResourceClassDefinition> = Vec::new();
    for campaign in campaigns {
        for floor in &campaign.budget_policy.reserve_floors {
            if let Some(resource) = resources
                .iter_mut()
                .find(|resource| resource.resource_id == floor.resource)
            {
                resource.reserve_floor_percent =
                    resource.reserve_floor_percent.max(floor.floor_percent);
            } else {
                resources.push(resource_class_from_reserve_floor(floor, notes));
            }
        }
    }
    resources
}

pub fn background_tick_fallback_campaign() -> EvolutionCampaign {
    EvolutionCampaign {
        campaign_id: "background_tick".to_string(),
        status: EvolutionCampaignStatus::Scheduled,
        objective_card: ObjectiveCard {
            objective_id: "objective_background_tick".to_string(),
            human_text: "background scheduler tick".to_string(),
            interpreted_goal: "background scheduler tick".to_string(),
            non_goals: Vec::new(),
            assumptions: Vec::new(),
            constraints: Vec::new(),
            success_metrics: Vec::new(),
            failure_conditions: Vec::new(),
            safety_notes: Vec::new(),
            resource_policy: "background".to_string(),
            approval_policy: "manual-only".to_string(),
            initial_horizon: "24h".to_string(),
            assumptions_needing_human_review: Vec::new(),
        },
        stages: Vec::new(),
        near_term_tasks: Vec::new(),
        iteration_results: Vec::new(),
        decisions: Vec::new(),
        budget_policy: CampaignBudgetPolicy {
            policy_id: "background".to_string(),
            reserve_floors: Vec::new(),
            scheduling_notes: Vec::new(),
            confidence: "unknown".to_string(),
            debt_note: "unknown".to_string(),
        },
        resource_schedule_preview: ResourceSchedulePreview {
            horizon: "24h".to_string(),
            entries: Vec::new(),
        },
        approval_checkpoint: ApprovalCheckpoint {
            policy: "manual-only".to_string(),
            status: ApprovalCheckpointStatus::Approved,
            required_before: "background tick".to_string(),
            editable_fields: Vec::new(),
            possible_actions: Vec::new(),
        },
        target_ring: TrustRing::R1TrustedRuntime,
        dry_run: true,
        capability_registry_required: Some(false),
        created_at: "unknown-time".to_string(),
        updated_at: "unknown-time".to_string(),
        source_repo: None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BackgroundSchedulingRequest {
    pub scheduler_service_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub campaigns: Vec<EvolutionCampaign>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub opportunity_events: Vec<EventEnvelope>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub previously_intaken_opportunity_event_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opportunity_detection_policy: Option<EvolutionOpportunityDetectionPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opportunity_planner_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_workloads: Vec<ClusterLongHorizonWorkload>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ResourceClassDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_estimates: Vec<ResourceUsageEstimate>,
    pub max_concurrent_campaigns: usize,
    pub max_concurrent_workloads: usize,
    #[serde(default)]
    pub idle_window_open: bool,
    #[serde(default)]
    pub reset_window_open: bool,
    #[serde(default)]
    pub dry_run: bool,
    pub now: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterLongHorizonWorkload {
    pub workload_id: String,
    pub system: String,
    pub title: String,
    pub status: ClusterLongHorizonWorkloadStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_estimates: Vec<ResourceUsageEstimate>,
    #[serde(default)]
    pub human_checkpoint_required: bool,
    #[serde(default)]
    pub terminal: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ClusterLongHorizonWorkloadStatus {
    Ready,
    Running,
    PausedBudget,
    PausedHuman,
    Complete,
    Cancelled,
}

pub fn new_cluster_long_horizon_workload(
    system: impl Into<String>,
    workload_id: impl Into<String>,
    title: impl Into<String>,
    status: ClusterLongHorizonWorkloadStatus,
    evidence_refs: Vec<String>,
) -> ClusterLongHorizonWorkload {
    let terminal = cluster_long_horizon_workload_status_is_terminal(&status);
    ClusterLongHorizonWorkload {
        workload_id: workload_id.into(),
        system: system.into(),
        title: title.into(),
        status,
        resource_estimates: Vec::new(),
        human_checkpoint_required: false,
        terminal,
        evidence_refs,
    }
}

pub fn cluster_long_horizon_workload_status_is_terminal(
    status: &ClusterLongHorizonWorkloadStatus,
) -> bool {
    matches!(
        status,
        ClusterLongHorizonWorkloadStatus::Complete | ClusterLongHorizonWorkloadStatus::Cancelled
    )
}

pub fn ensure_cluster_long_horizon_workload<'a>(
    workloads: &'a mut Vec<ClusterLongHorizonWorkload>,
    system: &str,
    workload_id: &str,
) -> &'a mut ClusterLongHorizonWorkload {
    if let Some(index) = workloads
        .iter()
        .position(|workload| workload.system == system && workload.workload_id == workload_id)
    {
        return &mut workloads[index];
    }
    workloads.push(new_cluster_long_horizon_workload(
        system,
        workload_id,
        workload_id,
        ClusterLongHorizonWorkloadStatus::Ready,
        Vec::new(),
    ));
    workloads
        .last_mut()
        .expect("just inserted external workload")
}

pub fn require_cluster_long_horizon_workload_human_checkpoint(
    workload: &mut ClusterLongHorizonWorkload,
) {
    workload.human_checkpoint_required = true;
    if workload.status == ClusterLongHorizonWorkloadStatus::Ready {
        workload.status = ClusterLongHorizonWorkloadStatus::PausedHuman;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterWorkloadUpdate {
    pub workload: ClusterLongHorizonWorkload,
    pub previous_status: ClusterLongHorizonWorkloadStatus,
    pub action: BackgroundSchedulingAction,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lease_decisions: Vec<ResourceLeaseDecision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BackgroundSchedulingPlan {
    pub tick_id: String,
    pub scheduler_service_id: String,
    pub generated_at: String,
    pub concurrency_cap: usize,
    pub workload_concurrency_cap: usize,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cluster_resources_after_tick: Vec<ResourceClassDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub campaigns: Vec<BackgroundCampaignUpdate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_workloads: Vec<ClusterWorkloadUpdate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub opportunity_intake: Vec<BackgroundOpportunityIntake>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub opportunity_events_skipped: Vec<BackgroundOpportunityEventSkip>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_campaigns: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub held_campaigns: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_workloads: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub held_workloads: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub human_checkpoints: Vec<HumanCheckpointSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timer_events: Vec<BackgroundTimerEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BackgroundCampaignUpdate {
    pub campaign: EvolutionCampaign,
    pub previous_status: EvolutionCampaignStatus,
    pub action: BackgroundSchedulingAction,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_decision: Option<CampaignScheduleDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_iteration: Option<ScheduledIterationPlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BackgroundOpportunityIntake {
    pub detection: DetectedEvolutionOpportunity,
    pub risk_gate: BackgroundOpportunityRiskGate,
    pub scoped_job: EvolutionJob,
    pub workspace_plan: CandidateWorkspacePlan,
    pub campaign_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BackgroundOpportunityEventSkip {
    pub event_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BackgroundOpportunityRiskGate {
    pub accepted: bool,
    pub human_checkpoint_required: bool,
    #[serde(default)]
    pub dispatch_allowed: bool,
    pub evidence_gate: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub budget_gate: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub risk_filter: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum BackgroundSchedulingAction {
    DispatchIteration,
    DispatchIterationDryRun,
    DispatchExternalWorkloadDryRun,
    ResumeAfterReset,
    PauseForBudget,
    HoldHumanCheckpoint,
    HoldConcurrencyCap,
    WaitForIdleWindow,
    NoNearTermTask,
    IgnoreTerminal,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct HumanCheckpointSummary {
    pub campaign_id: String,
    pub status: EvolutionCampaignStatus,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_actions: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BackgroundTimerEvent {
    pub event_id: String,
    pub kind: BackgroundTimerEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<String>,
    pub summary: String,
    pub due_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum BackgroundTimerEventKind {
    ResetWindowOpened,
    IdleWindowOpened,
    NextTickSuggested,
    HumanCheckpointRequested,
}

pub fn plan_background_scheduling_tick(
    request: BackgroundSchedulingRequest,
) -> BackgroundSchedulingPlan {
    let now = if request.now.trim().is_empty() {
        OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "unknown-time".to_string())
    } else {
        request.now.clone()
    };
    let scheduler_service_id = if request.scheduler_service_id.trim().is_empty() {
        "sinomo-cluster-scheduler".to_string()
    } else {
        request.scheduler_service_id.clone()
    };
    let concurrency_cap = request.max_concurrent_campaigns.max(1);
    let workload_concurrency_cap = request.max_concurrent_workloads.max(concurrency_cap).max(1);
    let base_resources = if request.resources.is_empty() {
        background_resource_classes_from_campaign_reserve_floors(
            &request.campaigns,
            "derived from campaign reserve floors for background scheduler",
        )
    } else {
        request.resources.clone()
    };
    let mut shared_resources = if request.reset_window_open {
        reset_resources_for_background_tick(&base_resources)
    } else {
        base_resources
    };
    if !request.reset_window_open {
        apply_campaign_iteration_usage_to_resources(&mut shared_resources, &request.campaigns);
    }
    let mut campaigns = Vec::new();
    let mut external_workloads = Vec::new();
    let mut active_campaigns = Vec::new();
    let mut held_campaigns = Vec::new();
    let mut active_workloads = Vec::new();
    let mut held_workloads = Vec::new();
    let mut human_checkpoints = Vec::new();
    let mut timer_events = Vec::new();
    let previously_intaken_opportunity_event_ids = request
        .previously_intaken_opportunity_event_ids
        .into_iter()
        .filter(|event_id| !event_id.trim().is_empty())
        .collect::<BTreeSet<_>>();
    let mut opportunity_events = Vec::new();
    let mut opportunity_events_skipped = Vec::new();
    for event in request.opportunity_events {
        if previously_intaken_opportunity_event_ids.contains(event.event_id.as_str()) {
            opportunity_events_skipped.push(BackgroundOpportunityEventSkip {
                event_id: event.event_id.to_string(),
                reason: "event already recorded in background opportunity intake history"
                    .to_string(),
            });
        } else {
            opportunity_events.push(event);
        }
    }

    let (opportunity_intake, opportunity_campaigns) = background_opportunity_intake(
        &opportunity_events,
        request.opportunity_detection_policy.as_ref(),
        request.opportunity_planner_node_id.as_ref(),
        &now,
    );

    if request.reset_window_open {
        timer_events.push(BackgroundTimerEvent {
            event_id: generated_id("timer"),
            kind: BackgroundTimerEventKind::ResetWindowOpened,
            campaign_id: None,
            summary: "reset window opened; renewable current-cluster resource usage was refreshed for this scheduling tick".to_string(),
            due_at: now.clone(),
        });
    }
    if request.idle_window_open {
        timer_events.push(BackgroundTimerEvent {
            event_id: generated_id("timer"),
            kind: BackgroundTimerEventKind::IdleWindowOpened,
            campaign_id: None,
            summary:
                "idle window opened; current-cluster background work may run if policy gates pass"
                    .to_string(),
            due_at: now.clone(),
        });
    }

    for workload in request.external_workloads {
        let previous_status = workload.status.clone();
        let workload_key = cluster_workload_key(&workload);
        if workload.terminal
            || matches!(
                workload.status,
                ClusterLongHorizonWorkloadStatus::Complete
                    | ClusterLongHorizonWorkloadStatus::Cancelled
            )
        {
            held_workloads.push(workload_key);
            external_workloads.push(ClusterWorkloadUpdate {
                workload,
                previous_status,
                action: BackgroundSchedulingAction::IgnoreTerminal,
                reason: "external workload is terminal and is ignored by the cluster scheduler"
                    .to_string(),
                lease_decisions: Vec::new(),
                evidence_refs: Vec::new(),
            });
            continue;
        }
        if workload.human_checkpoint_required
            || matches!(
                workload.status,
                ClusterLongHorizonWorkloadStatus::PausedHuman
            )
        {
            let reason = format!(
                "external workload {} requires a human checkpoint before cluster scheduling can continue",
                workload_key
            );
            held_workloads.push(workload_key.clone());
            human_checkpoints.push(HumanCheckpointSummary {
                campaign_id: workload_key.clone(),
                status: EvolutionCampaignStatus::PausedHuman,
                reason: reason.clone(),
                requested_actions: vec!["review external workload budget and policy".to_string()],
            });
            timer_events.push(BackgroundTimerEvent {
                event_id: generated_id("timer"),
                kind: BackgroundTimerEventKind::HumanCheckpointRequested,
                campaign_id: Some(workload_key),
                summary: reason.clone(),
                due_at: now.clone(),
            });
            external_workloads.push(ClusterWorkloadUpdate {
                workload,
                previous_status,
                action: BackgroundSchedulingAction::HoldHumanCheckpoint,
                reason,
                lease_decisions: Vec::new(),
                evidence_refs: Vec::new(),
            });
            continue;
        }
        if active_workloads.len() >= workload_concurrency_cap {
            held_workloads.push(workload_key.clone());
            external_workloads.push(ClusterWorkloadUpdate {
                workload,
                previous_status,
                action: BackgroundSchedulingAction::HoldConcurrencyCap,
                reason: format!(
                    "cluster workload concurrency cap {workload_concurrency_cap} is already occupied"
                ),
                lease_decisions: Vec::new(),
                evidence_refs: vec![format!("active workloads: {}", active_workloads.join(", "))],
            });
            continue;
        }
        if matches!(
            workload.status,
            ClusterLongHorizonWorkloadStatus::PausedBudget
        ) && !(request.reset_window_open || request.idle_window_open)
        {
            held_workloads.push(workload_key.clone());
            external_workloads.push(ClusterWorkloadUpdate {
                workload,
                previous_status,
                action: BackgroundSchedulingAction::WaitForIdleWindow,
                reason: "external workload is paused for budget until a reset or idle window opens"
                    .to_string(),
                lease_decisions: Vec::new(),
                evidence_refs: Vec::new(),
            });
            continue;
        }
        if workload.resource_estimates.is_empty() {
            held_workloads.push(workload_key.clone());
            external_workloads.push(ClusterWorkloadUpdate {
                workload,
                previous_status,
                action: BackgroundSchedulingAction::NoNearTermTask,
                reason: "external workload has no resource estimates to schedule".to_string(),
                lease_decisions: Vec::new(),
                evidence_refs: Vec::new(),
            });
            continue;
        }
        let lease_decisions = lease_estimates_against_cluster_resources(
            &workload_key,
            &workload.resource_estimates,
            &shared_resources,
            &now,
        );
        if lease_decisions
            .iter()
            .any(|decision| decision.outcome == ResourceLeaseOutcome::Denied)
        {
            let mut paused = workload;
            paused.status = ClusterLongHorizonWorkloadStatus::PausedBudget;
            held_workloads.push(workload_key);
            external_workloads.push(ClusterWorkloadUpdate {
                workload: paused,
                previous_status,
                action: BackgroundSchedulingAction::PauseForBudget,
                reason: "cluster resource scheduler denied at least one external workload lease"
                    .to_string(),
                evidence_refs: lease_decisions
                    .iter()
                    .map(|decision| format!("{}: {}", decision.decision_id, decision.reason))
                    .collect(),
                lease_decisions,
            });
            continue;
        }
        apply_granted_usage_to_resources(&mut shared_resources, &lease_decisions);
        let mut running = workload;
        running.status = ClusterLongHorizonWorkloadStatus::Running;
        active_workloads.push(workload_key);
        let mut evidence_refs = running.evidence_refs.clone();
        evidence_refs.extend(
            lease_decisions
                .iter()
                .map(|decision| format!("{}: {}", decision.decision_id, decision.reason)),
        );
        external_workloads.push(ClusterWorkloadUpdate {
            workload: running,
            previous_status,
            action: BackgroundSchedulingAction::DispatchExternalWorkloadDryRun,
            reason:
                "cluster scheduler reserved shared budget for an external long-horizon workload"
                    .to_string(),
            lease_decisions,
            evidence_refs,
        });
    }

    for campaign in request.campaigns.into_iter().chain(opportunity_campaigns) {
        let campaign_context_evidence_refs = background_campaign_context_evidence_refs(&campaign);
        let previous_status = campaign.status.clone();
        if is_terminal_campaign_status(&campaign.status) {
            held_campaigns.push(campaign.campaign_id.clone());
            campaigns.push(BackgroundCampaignUpdate {
                campaign,
                previous_status,
                action: BackgroundSchedulingAction::IgnoreTerminal,
                reason: "campaign is terminal and is ignored by the background scheduler"
                    .to_string(),
                schedule_decision: None,
                scheduled_iteration: None,
                evidence_refs: campaign_context_evidence_refs,
            });
            continue;
        }

        if background_status_requires_human(&campaign.status) {
            let reason = format!(
                "human checkpoint required before background scheduling can continue from {:?}",
                campaign.status
            );
            held_campaigns.push(campaign.campaign_id.clone());
            human_checkpoints.push(HumanCheckpointSummary {
                campaign_id: campaign.campaign_id.clone(),
                status: campaign.status.clone(),
                reason: reason.clone(),
                requested_actions: campaign.approval_checkpoint.possible_actions.clone(),
            });
            timer_events.push(BackgroundTimerEvent {
                event_id: generated_id("timer"),
                kind: BackgroundTimerEventKind::HumanCheckpointRequested,
                campaign_id: Some(campaign.campaign_id.clone()),
                summary: reason.clone(),
                due_at: now.clone(),
            });
            campaigns.push(BackgroundCampaignUpdate {
                campaign,
                previous_status,
                action: BackgroundSchedulingAction::HoldHumanCheckpoint,
                reason,
                schedule_decision: None,
                scheduled_iteration: None,
                evidence_refs: campaign_context_evidence_refs,
            });
            continue;
        }

        if active_campaigns.len() >= concurrency_cap
            || active_workloads.len() >= workload_concurrency_cap
        {
            let evidence_refs = background_campaign_evidence_refs_with(
                &campaign,
                vec![
                    format!("active campaigns: {}", active_campaigns.join(", ")),
                    format!("active workloads: {}", active_workloads.join(", ")),
                ],
            );
            held_campaigns.push(campaign.campaign_id.clone());
            campaigns.push(BackgroundCampaignUpdate {
                campaign,
                previous_status,
                action: BackgroundSchedulingAction::HoldConcurrencyCap,
                reason: format!("cluster scheduler concurrency cap is already occupied; campaign cap {concurrency_cap}, workload cap {workload_concurrency_cap}"),
                schedule_decision: None,
                scheduled_iteration: None,
                evidence_refs,
            });
            continue;
        }

        let mut schedulable_campaign = campaign.clone();
        if schedulable_campaign.status == EvolutionCampaignStatus::PausedBudget
            && (request.reset_window_open || request.idle_window_open)
        {
            schedulable_campaign.status = EvolutionCampaignStatus::Scheduled;
        }
        if !matches!(
            schedulable_campaign.status,
            EvolutionCampaignStatus::Approved
                | EvolutionCampaignStatus::Scheduled
                | EvolutionCampaignStatus::Running
        ) {
            let evidence_refs = background_campaign_context_evidence_refs(&schedulable_campaign);
            held_campaigns.push(schedulable_campaign.campaign_id.clone());
            campaigns.push(BackgroundCampaignUpdate {
                campaign: schedulable_campaign,
                previous_status,
                action: BackgroundSchedulingAction::WaitForIdleWindow,
                reason:
                    "campaign is not runnable until a reset, idle window, or approval event opens"
                        .to_string(),
                schedule_decision: None,
                scheduled_iteration: None,
                evidence_refs,
            });
            continue;
        }

        if schedulable_campaign.near_term_tasks.is_empty() {
            let evidence_refs = background_campaign_context_evidence_refs(&schedulable_campaign);
            held_campaigns.push(schedulable_campaign.campaign_id.clone());
            campaigns.push(BackgroundCampaignUpdate {
                campaign: schedulable_campaign,
                previous_status,
                action: BackgroundSchedulingAction::NoNearTermTask,
                reason: "campaign has no near-term tasks to dispatch".to_string(),
                schedule_decision: None,
                scheduled_iteration: None,
                evidence_refs,
            });
            continue;
        }

        let task_estimates = estimates_for_campaign(&request.task_estimates, &schedulable_campaign);
        let dry_run = dry_run_resource_scheduler(ResourceSchedulerDryRunRequest {
            campaign: schedulable_campaign.clone(),
            preview_horizon: "current-cluster background scheduler tick".to_string(),
            resources: shared_resources.clone(),
            task_estimates,
        });
        if dry_run.aggregate_decision != CampaignScheduleDecision::Scheduled {
            let mut paused = schedulable_campaign;
            paused.status = EvolutionCampaignStatus::PausedBudget;
            let evidence_refs = background_campaign_evidence_refs_with(
                &paused,
                dry_run
                    .lease_decisions
                    .iter()
                    .map(|decision| format!("{}: {}", decision.decision_id, decision.reason))
                    .collect(),
            );
            held_campaigns.push(paused.campaign_id.clone());
            campaigns.push(BackgroundCampaignUpdate {
                campaign: paused,
                previous_status,
                action: BackgroundSchedulingAction::PauseForBudget,
                reason:
                    "cluster resource scheduler denied at least one lease; campaign stays paused for budget"
                        .to_string(),
                schedule_decision: Some(dry_run.aggregate_decision),
                scheduled_iteration: None,
                evidence_refs,
            });
            continue;
        }

        apply_granted_usage_to_resources(&mut shared_resources, &dry_run.lease_decisions);
        let scheduled_iteration = prepare_scheduled_iteration(ScheduledIterationRequest {
            campaign: schedulable_campaign.clone(),
            task_id: None,
            intended_paths: Vec::new(),
            extra_verification_commands: Vec::new(),
            dry_run: request.dry_run,
        })
        .ok();
        if scheduled_iteration.is_some() {
            active_campaigns.push(schedulable_campaign.campaign_id.clone());
            active_workloads.push(format!(
                "sinomo-evolution:{}",
                schedulable_campaign.campaign_id
            ));
            let action = if request.dry_run {
                BackgroundSchedulingAction::DispatchIterationDryRun
            } else {
                BackgroundSchedulingAction::DispatchIteration
            };
            let reason = if request.dry_run {
                "cluster scheduler prepared one dry-run evolution iteration handoff; no agent was launched"
            } else {
                "cluster scheduler reserved budget for one approved evolution iteration dispatch; rollout still requires explicit approval"
            };
            let evidence_refs = background_campaign_evidence_refs_with(
                &schedulable_campaign,
                dry_run
                    .lease_decisions
                    .iter()
                    .map(|decision| format!("{}: {}", decision.decision_id, decision.reason))
                    .collect(),
            );
            campaigns.push(BackgroundCampaignUpdate {
                campaign: schedulable_campaign,
                previous_status,
                action,
                reason: reason.to_string(),
                schedule_decision: Some(dry_run.aggregate_decision),
                scheduled_iteration,
                evidence_refs,
            });
        } else {
            let evidence_refs = background_campaign_context_evidence_refs(&schedulable_campaign);
            held_campaigns.push(schedulable_campaign.campaign_id.clone());
            campaigns.push(BackgroundCampaignUpdate {
                campaign: schedulable_campaign,
                previous_status,
                action: BackgroundSchedulingAction::NoNearTermTask,
                reason: "campaign could not prepare an iteration handoff".to_string(),
                schedule_decision: Some(dry_run.aggregate_decision),
                scheduled_iteration: None,
                evidence_refs,
            });
        }
    }

    timer_events.push(BackgroundTimerEvent {
        event_id: generated_id("timer"),
        kind: BackgroundTimerEventKind::NextTickSuggested,
        campaign_id: None,
        summary: "next cron-compatible current-cluster scheduler tick should reload all long-horizon work queues and campaign bundles from disk"
            .to_string(),
        due_at: "next reset, idle window, or operator-scheduled interval".to_string(),
    });

    BackgroundSchedulingPlan {
        tick_id: generated_id("tick"),
        scheduler_service_id,
        generated_at: now,
        concurrency_cap,
        workload_concurrency_cap,
        dry_run: request.dry_run,
        cluster_resources_after_tick: shared_resources,
        campaigns,
        external_workloads,
        opportunity_intake,
        opportunity_events_skipped,
        active_campaigns,
        held_campaigns,
        active_workloads,
        held_workloads,
        human_checkpoints,
        timer_events,
    }
}

fn background_campaign_evidence_refs_with(
    campaign: &EvolutionCampaign,
    extra: Vec<String>,
) -> Vec<String> {
    let mut evidence_refs = background_campaign_context_evidence_refs(campaign);
    evidence_refs.extend(extra);
    evidence_refs.sort();
    evidence_refs.dedup();
    evidence_refs
}

fn background_campaign_context_evidence_refs(campaign: &EvolutionCampaign) -> Vec<String> {
    let mut evidence_refs = Vec::new();
    evidence_refs.extend(
        campaign
            .decisions
            .iter()
            .flat_map(|decision| decision.evidence_refs.iter().cloned()),
    );
    evidence_refs.extend(
        campaign
            .near_term_tasks
            .iter()
            .flat_map(|task| task.required_evidence.iter().cloned()),
    );
    evidence_refs.extend(
        campaign
            .stages
            .iter()
            .flat_map(|stage| stage.regression_gates.iter().cloned()),
    );
    evidence_refs.extend(
        campaign
            .objective_card
            .constraints
            .iter()
            .filter(|constraint| constraint.starts_with("background-"))
            .cloned(),
    );
    evidence_refs.sort();
    evidence_refs.dedup();
    evidence_refs
}

fn background_opportunity_intake(
    events: &[EventEnvelope],
    policy: Option<&EvolutionOpportunityDetectionPolicy>,
    planner_node_id: Option<&NodeId>,
    now: &str,
) -> (Vec<BackgroundOpportunityIntake>, Vec<EvolutionCampaign>) {
    let Some(policy) = policy else {
        return (Vec::new(), Vec::new());
    };
    if events.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let planner =
        EvolutionPlanner::new(planner_node_id.cloned().unwrap_or_else(|| {
            NodeId::from_legacy_named("node_background_evolution").expect("node")
        }));
    let mut intake = Vec::new();
    let mut campaigns = Vec::new();
    for detection in detect_evolution_opportunities_from_events(events, policy) {
        let risk_gate = background_opportunity_risk_gate(&detection);
        let (scoped_job, workspace_plan) = planner.scope_job(detection.opportunity.clone());
        let campaign =
            background_campaign_for_detected_opportunity(&detection, &risk_gate, &scoped_job, now);
        let campaign_id = campaign.campaign_id.clone();
        let evidence_refs = detection.evidence_refs.clone();
        if risk_gate.accepted {
            campaigns.push(campaign);
        }
        intake.push(BackgroundOpportunityIntake {
            detection,
            risk_gate,
            scoped_job,
            workspace_plan,
            campaign_id,
            evidence_refs,
        });
    }
    (intake, campaigns)
}

fn background_opportunity_risk_gate(
    detection: &DetectedEvolutionOpportunity,
) -> BackgroundOpportunityRiskGate {
    let accepted = !detection.evidence_refs.is_empty();
    let trusted_runtime = matches!(
        detection.opportunity.target_ring,
        TrustRing::R0ContinuityRoot | TrustRing::R1TrustedRuntime | TrustRing::R2TrustedService
    );
    BackgroundOpportunityRiskGate {
        accepted,
        human_checkpoint_required: true,
        dispatch_allowed: false,
        evidence_gate: if accepted {
            "detected opportunity carries event or evidence refs".to_string()
        } else {
            "detected opportunity has no reusable evidence refs".to_string()
        },
        budget_gate:
            "intake is bounded to a dry-run campaign with max wall time 30m; scheduler leases are still required after human approval"
                .to_string(),
        risk_filter: if trusted_runtime {
            "trusted-runtime target; intake only, no background dispatch".to_string()
        } else {
            "sandbox/module target; human checkpoint remains required before dispatch".to_string()
        },
        reason: if trusted_runtime {
            "trusted-runtime opportunity is intake-only until human approval and stronger gates"
                .to_string()
        } else {
            "opportunity is converted to a human-gated campaign; background tick must not promote"
                .to_string()
        },
    }
}

fn background_campaign_for_detected_opportunity(
    detection: &DetectedEvolutionOpportunity,
    risk_gate: &BackgroundOpportunityRiskGate,
    scoped_job: &EvolutionJob,
    now: &str,
) -> EvolutionCampaign {
    let mut campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: format!(
            "Telemetry opportunity {:?}: {}",
            detection.signal_kind, detection.opportunity.objective
        ),
        horizon: "14 days".to_string(),
        budget_policy: "background-opportunity".to_string(),
        reserve_floors: Vec::new(),
        approval_policy: "human-gated-background-opportunity".to_string(),
        max_wall_time: Some("30m".to_string()),
        pause_after_proposal: true,
        dry_run: true,
        approved: false,
        target_ring: detection.opportunity.target_ring.clone(),
        source_repo: None,
    });
    campaign.status = EvolutionCampaignStatus::AwaitingApproval;
    campaign.updated_at = now.to_string();
    campaign.approval_checkpoint.required_before =
        "background opportunity dispatch from telemetry".to_string();
    campaign.approval_checkpoint.possible_actions = vec![
        "approve campaign for dry-run scheduling".to_string(),
        "reject noisy opportunity".to_string(),
        "edit allowed paths or verification gates".to_string(),
    ];
    let risk_evidence = background_opportunity_risk_evidence(risk_gate);
    let context_evidence = background_opportunity_context_evidence(detection);
    campaign
        .objective_card
        .constraints
        .extend(risk_evidence.clone());
    campaign
        .objective_card
        .constraints
        .extend(context_evidence.clone());
    campaign.objective_card.safety_notes.push(
        "background telemetry intake may create scoped work, but it does not promote or roll out runtime changes"
            .to_string(),
    );
    if detection.signal_kind == EvolutionOpportunitySignalKind::RolloutRollback {
        campaign.objective_card.success_metrics.push(
            "Rollback follow-up produces a minimized replay, regression gate, or explicit no-change diagnosis before any new rollout attempt."
                .to_string(),
        );
        campaign.objective_card.failure_conditions.push(
            "Rollback evidence is treated as a generic patch request without replay or guard evidence."
                .to_string(),
        );
    }
    for task in &mut campaign.near_term_tasks {
        task.required_evidence
            .extend(detection.evidence_refs.clone());
        task.required_evidence.extend(risk_evidence.clone());
        task.required_evidence.extend(context_evidence.clone());
        task.required_evidence.push(format!(
            "scoped job {} must remain inspectable before agent dispatch",
            scoped_job.job_id
        ));
        task.required_evidence.sort();
        task.required_evidence.dedup();
        task.approval_required = true;
        if detection.signal_kind == EvolutionOpportunitySignalKind::RolloutRollback {
            task.expected_result_kind = EvolutionResultKind::RolloutGuard;
            task.objective = format!(
                "{} Convert the rollback transition evidence into a minimized replay, regression gate, or explicit no-change diagnosis before proposing another rollout.",
                task.objective
            );
        }
    }
    campaign.decisions.push(CampaignDecision {
        decision_id: generated_id("decision"),
        kind: CampaignDecisionKind::ProposalCreated,
        summary: format!(
            "Background intake created human-gated campaign from {:?}",
            detection.signal_kind
        ),
        rationale: risk_gate.reason.clone(),
        evidence_refs: detection.evidence_refs.clone(),
        decided_by: "background-opportunity-intake".to_string(),
        decided_at: now.to_string(),
    });
    campaign
}

fn background_opportunity_context_evidence(
    detection: &DetectedEvolutionOpportunity,
) -> Vec<String> {
    let mut evidence = match detection.signal_kind {
        EvolutionOpportunitySignalKind::RolloutRollback => vec![
            "background-context-signal=rollout-rollback".to_string(),
            "background-context-required-output=rollout-guard-or-minimized-regression-gate"
                .to_string(),
            "background-context-replay-minimization-required=true".to_string(),
        ],
        _ => Vec::new(),
    };
    evidence.sort();
    evidence.dedup();
    evidence
}

fn background_opportunity_risk_evidence(risk_gate: &BackgroundOpportunityRiskGate) -> Vec<String> {
    let mut evidence = vec![
        format!("background-risk-accepted={}", risk_gate.accepted),
        format!(
            "background-risk-human-checkpoint-required={}",
            risk_gate.human_checkpoint_required
        ),
        format!(
            "background-risk-dispatch-allowed-at-intake={}",
            risk_gate.dispatch_allowed
        ),
        format!("background-risk-evidence-gate={}", risk_gate.evidence_gate),
        format!("background-risk-reason={}", risk_gate.reason),
    ];
    if !risk_gate.budget_gate.is_empty() {
        evidence.push(format!(
            "background-risk-budget-gate={}",
            risk_gate.budget_gate
        ));
    }
    if !risk_gate.risk_filter.is_empty() {
        evidence.push(format!("background-risk-filter={}", risk_gate.risk_filter));
    }
    evidence.sort();
    evidence.dedup();
    evidence
}

pub fn cluster_workload_key(workload: &ClusterLongHorizonWorkload) -> String {
    format!("{}:{}", workload.system, workload.workload_id)
}

pub fn parse_background_tick_external_workloads(
    workload_specs: &[String],
    estimate_specs: &[String],
    checkpoint_specs: &[String],
) -> Result<Vec<ClusterLongHorizonWorkload>, EvolutionInputParseError> {
    let mut workloads = Vec::new();
    for spec in workload_specs {
        let workload = parse_external_workload_spec(spec)?;
        if workloads
            .iter()
            .any(|existing: &ClusterLongHorizonWorkload| {
                existing.system == workload.system && existing.workload_id == workload.workload_id
            })
        {
            return Err(EvolutionInputParseError::Invalid {
                detail: format!(
                    "duplicate external workload `{}`",
                    cluster_workload_key(&workload)
                ),
            });
        }
        workloads.push(workload);
    }
    for spec in estimate_specs {
        let (system, workload_id, estimate) = parse_external_workload_estimate(spec)?;
        let workload = ensure_cluster_long_horizon_workload(&mut workloads, &system, &workload_id);
        workload.resource_estimates.push(estimate);
    }
    for spec in checkpoint_specs {
        let (system, workload_id) = parse_external_workload_key(spec)?;
        let workload = ensure_cluster_long_horizon_workload(&mut workloads, &system, &workload_id);
        require_cluster_long_horizon_workload_human_checkpoint(workload);
    }
    Ok(workloads)
}

pub fn parse_external_workload_spec(
    value: &str,
) -> Result<ClusterLongHorizonWorkload, EvolutionInputParseError> {
    let parts: Vec<&str> = value.splitn(4, ':').collect();
    let [system, workload_id, status, title] = match parts.as_slice() {
        [system, workload_id, status] => [*system, *workload_id, *status, *workload_id],
        [system, workload_id, status, title] => [*system, *workload_id, *status, *title],
        _ => {
            return Err(EvolutionInputParseError::Invalid {
                detail: format!(
                    "external workload `{value}` must look like system:workload_id:status[:title]"
                ),
            });
        }
    };
    if system.trim().is_empty() || workload_id.trim().is_empty() {
        return Err(EvolutionInputParseError::Invalid {
            detail: format!("external workload `{value}` needs non-empty system and workload id"),
        });
    }
    let status = parse_external_workload_status(status)?;
    Ok(new_cluster_long_horizon_workload(
        system.trim(),
        workload_id.trim(),
        title.trim(),
        status,
        vec!["sinomo evolve tick external workload input".to_string()],
    ))
}

pub fn parse_external_workload_estimate(
    value: &str,
) -> Result<(String, String, ResourceUsageEstimate), EvolutionInputParseError> {
    let parts: Vec<&str> = value.split(':').collect();
    let [system, workload_id, resource, units] = match parts.as_slice() {
        [system, workload_id, resource, units] => [*system, *workload_id, *resource, *units],
        _ => {
            return Err(EvolutionInputParseError::Invalid {
                detail: format!(
                    "external workload estimate `{value}` must look like system:workload_id:resource:units"
                ),
            });
        }
    };
    if system.trim().is_empty() || workload_id.trim().is_empty() || resource.trim().is_empty() {
        return Err(EvolutionInputParseError::Invalid {
            detail: format!(
                "external workload estimate `{value}` needs non-empty system, workload id, and resource"
            ),
        });
    }
    let estimated_units =
        units
            .trim()
            .parse::<u64>()
            .map_err(|source| EvolutionInputParseError::Invalid {
                detail: format!("invalid external workload estimate `{value}`: {source}"),
            })?;
    Ok((
        system.trim().to_string(),
        workload_id.trim().to_string(),
        ResourceUsageEstimate {
            task_id: workload_id.trim().to_string(),
            resource: resource.trim().to_string(),
            estimated_units,
            actual_units: None,
        },
    ))
}

pub fn parse_external_workload_key(
    value: &str,
) -> Result<(String, String), EvolutionInputParseError> {
    let parts: Vec<&str> = value.split(':').collect();
    let [system, workload_id] = match parts.as_slice() {
        [system, workload_id] => [*system, *workload_id],
        _ => {
            return Err(EvolutionInputParseError::Invalid {
                detail: format!(
                    "external workload key `{value}` must look like system:workload_id"
                ),
            });
        }
    };
    if system.trim().is_empty() || workload_id.trim().is_empty() {
        return Err(EvolutionInputParseError::Invalid {
            detail: format!(
                "external workload key `{value}` needs non-empty system and workload id"
            ),
        });
    }
    Ok((system.trim().to_string(), workload_id.trim().to_string()))
}

pub fn parse_external_workload_status(
    value: &str,
) -> Result<ClusterLongHorizonWorkloadStatus, EvolutionInputParseError> {
    match value {
        "ready" => Ok(ClusterLongHorizonWorkloadStatus::Ready),
        "running" => Ok(ClusterLongHorizonWorkloadStatus::Running),
        "paused-budget" => Ok(ClusterLongHorizonWorkloadStatus::PausedBudget),
        "paused-human" => Ok(ClusterLongHorizonWorkloadStatus::PausedHuman),
        "complete" => Ok(ClusterLongHorizonWorkloadStatus::Complete),
        "cancelled" | "canceled" => Ok(ClusterLongHorizonWorkloadStatus::Cancelled),
        _ => Err(EvolutionInputParseError::Invalid {
            detail: format!(
                "unknown external workload status `{value}`; expected ready, running, paused-budget, paused-human, complete, or cancelled"
            ),
        }),
    }
}

fn lease_estimates_against_cluster_resources(
    owner_id: &str,
    estimates: &[ResourceUsageEstimate],
    resources: &[ResourceClassDefinition],
    now: &str,
) -> Vec<ResourceLeaseDecision> {
    let mut planned_usage: Vec<(String, u64)> = resources
        .iter()
        .map(|resource| (resource.resource_id.clone(), resource.used_units))
        .collect();
    let mut decisions = Vec::new();
    for estimate in estimates {
        let resource = resource_for_estimate(resources, &estimate.resource);
        let used_before = planned_usage
            .iter()
            .find(|(resource_id, _)| resource_id == &resource.resource_id)
            .map(|(_, used)| *used)
            .unwrap_or(resource.used_units);
        let used_after = used_before.saturating_add(estimate.estimated_units);
        let reserve_floor_units =
            reserve_floor_units(resource.total_units, resource.reserve_floor_percent);
        let spendable_ceiling = resource.total_units.saturating_sub(reserve_floor_units);
        let (outcome, reason) = if used_after <= spendable_ceiling {
            if let Some((_, used)) = planned_usage
                .iter_mut()
                .find(|(resource_id, _)| resource_id == &resource.resource_id)
            {
                *used = used_after;
            }
            (
                ResourceLeaseOutcome::Granted,
                format!(
                    "grant: cluster workload {} estimated use {} {} keeps {} at {}/{}; reserve floor remains protected at {}%",
                    owner_id,
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
                format!(
                    "deny: cluster workload {} estimated use {} {} would move {} from {}/{} to {}/{} and violate reserve floor {}%",
                    owner_id,
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
        decisions.push(ResourceLeaseDecision {
            decision_id: generated_id("lease_decision"),
            request_id: generated_id("lease_req"),
            resource: resource.resource_id.clone(),
            outcome,
            reason,
            reserve_floor_percent: resource.reserve_floor_percent,
            used_before_units: used_before,
            estimated_units: estimate.estimated_units,
            used_after_units: used_after,
            total_units: resource.total_units,
            decided_at: now.to_string(),
        });
    }
    decisions
}

fn reset_resources_for_background_tick(
    resources: &[ResourceClassDefinition],
) -> Vec<ResourceClassDefinition> {
    resources
        .iter()
        .cloned()
        .map(|mut resource| {
            resource.used_units = 0;
            resource.notes = format!("{}; reset by background scheduling tick", resource.notes);
            resource
        })
        .collect()
}

fn is_terminal_campaign_status(status: &EvolutionCampaignStatus) -> bool {
    matches!(
        status,
        EvolutionCampaignStatus::Complete
            | EvolutionCampaignStatus::Cancelled
            | EvolutionCampaignStatus::Superseded
            | EvolutionCampaignStatus::FailedExhausted
    )
}

fn background_status_requires_human(status: &EvolutionCampaignStatus) -> bool {
    matches!(
        status,
        EvolutionCampaignStatus::AwaitingApproval
            | EvolutionCampaignStatus::PausedHuman
            | EvolutionCampaignStatus::PausedPolicy
            | EvolutionCampaignStatus::StageComplete
    )
}

fn estimates_for_campaign(
    estimates: &[ResourceUsageEstimate],
    campaign: &EvolutionCampaign,
) -> Vec<ResourceUsageEstimate> {
    let task_ids: Vec<&str> = campaign
        .near_term_tasks
        .iter()
        .map(|task| task.task_id.as_str())
        .collect();
    let matching: Vec<ResourceUsageEstimate> = estimates
        .iter()
        .filter(|estimate| task_ids.iter().any(|task_id| *task_id == estimate.task_id))
        .cloned()
        .collect();
    if matching.is_empty() {
        Vec::new()
    } else {
        matching
    }
}

fn apply_granted_usage_to_resources(
    resources: &mut [ResourceClassDefinition],
    decisions: &[ResourceLeaseDecision],
) {
    for decision in decisions
        .iter()
        .filter(|decision| decision.outcome == ResourceLeaseOutcome::Granted)
    {
        if let Some(resource) = resources
            .iter_mut()
            .find(|resource| resource.resource_id == decision.resource)
        {
            resource.used_units = decision.used_after_units;
        }
    }
}

fn apply_campaign_iteration_usage_to_resources(
    resources: &mut [ResourceClassDefinition],
    campaigns: &[EvolutionCampaign],
) {
    for usage in campaigns
        .iter()
        .flat_map(|campaign| &campaign.iteration_results)
        .flat_map(|result| &result.resource_usage)
    {
        let used_units = usage.actual_units.unwrap_or(usage.estimated_units);
        if used_units == 0 {
            continue;
        }
        if let Some(resource) = resources
            .iter_mut()
            .find(|resource| resource.resource_id == usage.resource)
        {
            resource.used_units = resource
                .used_units
                .saturating_add(used_units)
                .min(resource.total_units);
            if !resource.notes.contains("iteration usage") {
                resource.notes = format!(
                    "{}; includes previous campaign iteration usage in current window",
                    resource.notes
                );
            }
        }
    }
}
