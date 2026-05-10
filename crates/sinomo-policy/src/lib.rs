//! Trusted policy decision skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    CapabilityDiff, EvaluationGate, EvaluationOutcome, EvaluationScorecard, PolicyAction,
    PolicyDecision, RolloutPlan, TrustRing,
};
use sinomo_module::ArtifactId;

pub type PolicyDecisionId = sinomo_core::Id;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PolicyInput {
    pub target_ring: TrustRing,
    pub target_artifact_id: ArtifactId,
    pub capability_diff: CapabilityDiff,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard: Option<EvaluationScorecard>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout_plan: Option<RolloutPlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_incidents: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct PolicyEngine;

impl PolicyEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn decide(&self, input: PolicyInput) -> PolicyDecision {
        let mut required_additional_evals = Vec::new();
        let action = if input.target_ring.requires_continuity_gates() {
            required_additional_evals.push(EvaluationGate::ContinuityChecks);
            required_additional_evals.push(EvaluationGate::ReplayTests);
            PolicyAction::RequireMoreEvidence
        } else if input.capability_diff.requests_new_power() {
            required_additional_evals.push(EvaluationGate::ManifestValidation);
            required_additional_evals.push(EvaluationGate::ReplayTests);
            PolicyAction::RequireMoreEvidence
        } else if input
            .scorecard
            .as_ref()
            .is_some_and(|scorecard| scorecard.outcome == EvaluationOutcome::Failed)
        {
            PolicyAction::Deny
        } else {
            PolicyAction::Allow
        };

        let reason = match action {
            PolicyAction::Allow => {
                "scorecard and capability diff satisfy phase-2 policy".to_string()
            }
            PolicyAction::Deny => "candidate scorecard failed a blocking gate".to_string(),
            PolicyAction::RequireMoreEvidence => {
                "target ring or capability diff requires additional autonomous evidence".to_string()
            }
            PolicyAction::Defer => "policy deferred".to_string(),
            PolicyAction::Quarantine => "policy quarantined artifact".to_string(),
            PolicyAction::EmergencyStop => "policy emergency stop".to_string(),
        };

        PolicyDecision {
            decision_id: PolicyDecisionId::generate(),
            target_ring: input.target_ring,
            target_artifact_id: input.target_artifact_id,
            action,
            capability_diff: input.capability_diff,
            required_additional_evals,
            max_rollout_weight_bps: Some(500),
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{CapabilityName, TrustRing};

    #[test]
    fn new_capability_requires_more_evidence() {
        let decision = PolicyEngine::new().decide(PolicyInput {
            target_ring: TrustRing::R3SandboxedModule,
            target_artifact_id: ArtifactId::from_legacy_named("artifact_candidate")
                .expect("artifact"),
            capability_diff: CapabilityDiff {
                added_imports: vec![CapabilityName::new("event.read.session").expect("cap")],
                ..CapabilityDiff::default()
            },
            scorecard: None,
            rollout_plan: None,
            recent_incidents: Vec::new(),
        });

        assert_eq!(decision.action, PolicyAction::RequireMoreEvidence);
    }
}
