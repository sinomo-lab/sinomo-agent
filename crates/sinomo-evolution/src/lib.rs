//! Autonomous self-evolution planning skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    Confidence, EvaluationGate, EventEnvelope, EventId, EventKind, EvolutionJob, EvolutionJobId,
    EvolutionJobStatus, EvolutionScope, LatencyClass, ModuleId, NodeId, PrivacyClass,
    RollbackPolicy, RoutingHint, RoutingMetadata, RoutingScope, Salience, Source, TrustRing,
    WorkspaceId,
};
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
            target_ring: opportunity.target_ring,
            target_module_id: opportunity.target_module_id,
            objective: opportunity.objective,
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
            event.evidence.refs.push(sinomo_core::EvidenceRef {
                uri: format!("dfs://{workspace_id}"),
                kind: sinomo_core::EvidenceKind::Other,
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
}
