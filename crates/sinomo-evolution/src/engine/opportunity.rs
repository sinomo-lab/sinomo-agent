//! Opportunity detection and planner-owned job scoping.
//!
//! This module owns the translation from runtime telemetry into bounded
//! candidate work, including required gates and candidate workspace plans.

use crate::{
    EditScope, EvolutionJob, EvolutionJobId, EvolutionJobStatus, EvolutionScope, RecoveryPlan,
    VerificationCommand, VerificationPlan, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{
    Confidence, EvaluationGate, EventEnvelope, EventKind, LatencyClass, PrivacyClass,
    RollbackPolicy, RoutingHint, RoutingMetadata, RoutingScope, Salience, Source, TrustRing,
};
use sinomo_runtime::events::EventId;
use sinomo_runtime::module::{
    MODULE_CRASH_EVENT_KIND, MODULE_INVOCATION_DECISION_EVENT_KIND,
    MODULE_POLICY_BLOCKED_EVENT_KIND, MODULE_TIMEOUT_EVENT_KIND, MODULE_USER_CORRECTION_EVENT_KIND,
    ModuleId,
};
use sinomo_runtime::node::NodeId;
use std::collections::BTreeSet;
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
pub enum EvolutionOpportunitySignalKind {
    ModuleTimeout,
    ModuleCrash,
    ModulePolicyBlocked,
    ModuleUserCorrection,
    RolloutRollback,
    MetricRegression,
    ReplayFailure,
    StaleScorecard,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvolutionOpportunityDetectionPolicy {
    pub workspace_id: WorkspaceId,
    pub default_target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_forbidden_paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DetectedEvolutionOpportunity {
    pub signal_kind: EvolutionOpportunitySignalKind,
    pub opportunity: EvolutionOpportunity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
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

#[derive(Clone, Debug)]
pub struct EvolutionPlanner {
    node_id: NodeId,
}

pub fn detect_evolution_opportunities_from_events(
    events: &[EventEnvelope],
    policy: &EvolutionOpportunityDetectionPolicy,
) -> Vec<DetectedEvolutionOpportunity> {
    let mut seen_events = BTreeSet::new();
    let mut opportunities = Vec::new();
    for event in events {
        if !seen_events.insert(event.event_id.clone()) {
            continue;
        }
        let Some(signal_kind) = opportunity_signal_kind(event) else {
            continue;
        };
        opportunities.push(DetectedEvolutionOpportunity {
            evidence_refs: opportunity_evidence_refs(event),
            opportunity: EvolutionOpportunity {
                observed_event_id: Some(event.event_id.clone()),
                target_ring: opportunity_target_ring(event, policy),
                target_module_id: event_module_id(event),
                objective: opportunity_objective(event, &signal_kind),
                workspace_id: opportunity_workspace_id(event, policy),
                allowed_paths: opportunity_allowed_paths(event, policy),
                forbidden_paths: opportunity_forbidden_paths(event, policy),
                priority: event.salience.get().max(0.5),
            },
            signal_kind,
        });
    }
    opportunities
}

fn opportunity_signal_kind(event: &EventEnvelope) -> Option<EvolutionOpportunitySignalKind> {
    match event.kind.as_str() {
        MODULE_TIMEOUT_EVENT_KIND => Some(EvolutionOpportunitySignalKind::ModuleTimeout),
        MODULE_CRASH_EVENT_KIND => Some(EvolutionOpportunitySignalKind::ModuleCrash),
        MODULE_POLICY_BLOCKED_EVENT_KIND => {
            Some(EvolutionOpportunitySignalKind::ModulePolicyBlocked)
        }
        MODULE_USER_CORRECTION_EVENT_KIND => {
            Some(EvolutionOpportunitySignalKind::ModuleUserCorrection)
        }
        MODULE_INVOCATION_DECISION_EVENT_KIND => match event
            .payload
            .get("primary_error_kind")
            .and_then(|value| value.as_str())
        {
            Some("timeout") => Some(EvolutionOpportunitySignalKind::ModuleTimeout),
            Some("policy-blocked") => Some(EvolutionOpportunitySignalKind::ModulePolicyBlocked),
            Some("runtime-error") => Some(EvolutionOpportunitySignalKind::ModuleCrash),
            _ => None,
        },
        "rollout.transition.completed" if rollout_transition_event_is_rollback(event) => {
            Some(EvolutionOpportunitySignalKind::RolloutRollback)
        }
        "evolution.metric.regression" => Some(EvolutionOpportunitySignalKind::MetricRegression),
        "evolution.replay.failure" => Some(EvolutionOpportunitySignalKind::ReplayFailure),
        "evolution.scorecard.stale" => Some(EvolutionOpportunitySignalKind::StaleScorecard),
        _ => None,
    }
}

fn rollout_transition_event_is_rollback(event: &EventEnvelope) -> bool {
    matches!(
        event.payload.get("action").and_then(|value| value.as_str()),
        Some("roll-back") | Some("rollback")
    ) || matches!(
        event.payload.get("stage").and_then(|value| value.as_str()),
        Some("rolled-back")
    )
}

fn opportunity_objective(
    event: &EventEnvelope,
    signal_kind: &EvolutionOpportunitySignalKind,
) -> String {
    if let Some(objective) = event
        .payload
        .get("objective")
        .and_then(|value| value.as_str())
    {
        if !objective.trim().is_empty() {
            return objective.trim().to_string();
        }
    }
    let target = event
        .payload
        .get("module_id")
        .and_then(|value| value.as_str())
        .map(|module_id| format!("module `{module_id}`"))
        .unwrap_or_else(|| "the affected component".to_string());
    match signal_kind {
        EvolutionOpportunitySignalKind::ModuleTimeout => {
            format!("Reduce timeout failures for {target} without broadening capabilities")
        }
        EvolutionOpportunitySignalKind::ModuleCrash => {
            format!("Harden {target} after runtime failure telemetry")
        }
        EvolutionOpportunitySignalKind::ModulePolicyBlocked => {
            format!("Reduce policy-blocked behavior for {target} without weakening sandbox policy")
        }
        EvolutionOpportunitySignalKind::ModuleUserCorrection => {
            format!("Reduce user corrections for {target} using replay-backed evidence")
        }
        EvolutionOpportunitySignalKind::RolloutRollback => {
            format!("Investigate rollback of {target} and preserve reusable regression evidence")
        }
        EvolutionOpportunitySignalKind::MetricRegression => {
            "Investigate and improve the regressed metric with scoped verification".to_string()
        }
        EvolutionOpportunitySignalKind::ReplayFailure => {
            "Fix replay failure with a bounded candidate and preserved reproduction evidence"
                .to_string()
        }
        EvolutionOpportunitySignalKind::StaleScorecard => {
            "Refresh stale scorecard evidence and improve the failing target if needed".to_string()
        }
    }
}

fn event_module_id(event: &EventEnvelope) -> Option<ModuleId> {
    event
        .payload
        .get("module_id")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<ModuleId>().ok())
}

fn opportunity_target_ring(
    event: &EventEnvelope,
    policy: &EvolutionOpportunityDetectionPolicy,
) -> TrustRing {
    event
        .payload
        .get("target_ring")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_else(|| {
            if event_module_id(event).is_some() {
                TrustRing::R3SandboxedModule
            } else {
                policy.default_target_ring.clone()
            }
        })
}

fn opportunity_workspace_id(
    event: &EventEnvelope,
    policy: &EvolutionOpportunityDetectionPolicy,
) -> WorkspaceId {
    event
        .payload
        .get("workspace_id")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<WorkspaceId>().ok())
        .unwrap_or_else(|| policy.workspace_id.clone())
}

fn opportunity_allowed_paths(
    event: &EventEnvelope,
    policy: &EvolutionOpportunityDetectionPolicy,
) -> Vec<String> {
    payload_string_array(event, "allowed_paths")
        .or_else(|| payload_string_array(event, "suggested_paths"))
        .unwrap_or_else(|| policy.default_allowed_paths.clone())
}

fn opportunity_forbidden_paths(
    event: &EventEnvelope,
    policy: &EvolutionOpportunityDetectionPolicy,
) -> Vec<String> {
    payload_string_array(event, "forbidden_paths")
        .unwrap_or_else(|| policy.default_forbidden_paths.clone())
}

fn opportunity_evidence_refs(event: &EventEnvelope) -> Vec<String> {
    let mut refs = vec![format!("event://{}", event.event_id)];
    refs.extend(payload_string_array(event, "evidence_refs").unwrap_or_default());
    refs.extend(payload_string_array(event, "triggering_evidence_refs").unwrap_or_default());
    refs.extend(
        event
            .evidence
            .refs
            .iter()
            .map(|evidence| evidence.uri.clone()),
    );
    refs.sort();
    refs.dedup();
    refs
}

fn payload_string_array(event: &EventEnvelope, key: &str) -> Option<Vec<String>> {
    let array = event.payload.get(key)?.as_array()?;
    let values = array
        .iter()
        .filter_map(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
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
            edit_scope: EditScope {
                workspace_root: opportunity.workspace_id.to_string(),
                intended_paths: opportunity.allowed_paths.clone(),
                notes: "planner-scoped evolution opportunity".to_string(),
            },
            verification: VerificationPlan {
                commands: required_gates
                    .iter()
                    .map(|gate| VerificationCommand {
                        name: format!("{gate:?}"),
                        command: "cargo check".to_string(),
                        expected_exit_code: 0,
                        gate: gate.clone(),
                    })
                    .collect(),
                module_replay_reports: Vec::new(),
            },
            recovery: RecoveryPlan::default(),
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
