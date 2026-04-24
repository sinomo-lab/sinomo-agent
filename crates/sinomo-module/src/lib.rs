//! Module manifest and node-local loader state skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    ArtifactManifest, HotSwapState, ModuleId, ModuleLoaderState, NodeId, PolicyAction,
    PolicyDecision, RolloutPlan, TrustRing,
};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LoadRequest {
    pub node_id: NodeId,
    pub artifact: ArtifactManifest,
    pub policy_decision: PolicyDecision,
    pub rollout_plan: RolloutPlan,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LoadDecision {
    pub node_id: NodeId,
    pub module_id: ModuleId,
    pub artifact_id: sinomo_core::ArtifactId,
    pub action: LoaderAction,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LoaderAction {
    AcceptShadow,
    AcceptCandidate,
    Reject,
    Quarantine,
    RollBack,
}

#[derive(Clone, Debug)]
pub struct ModuleLoader {
    node_id: NodeId,
}

impl ModuleLoader {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn evaluate_load(&self, request: LoadRequest) -> Result<LoadDecision, ModuleLoadError> {
        if request.node_id != self.node_id {
            return Err(ModuleLoadError::WrongNode {
                expected: self.node_id.clone(),
                actual: request.node_id,
            });
        }

        if request.artifact.target_ring != request.rollout_plan.target_ring {
            return Ok(LoadDecision {
                node_id: self.node_id.clone(),
                module_id: request.artifact.module_id,
                artifact_id: request.artifact.artifact_id,
                action: LoaderAction::Reject,
                reason: "artifact and rollout target rings differ".to_string(),
            });
        }

        let action = match request.policy_decision.action {
            PolicyAction::Allow if request.artifact.target_ring == TrustRing::R3SandboxedModule => {
                LoaderAction::AcceptShadow
            }
            PolicyAction::Allow => LoaderAction::AcceptCandidate,
            PolicyAction::Quarantine => LoaderAction::Quarantine,
            PolicyAction::EmergencyStop => LoaderAction::RollBack,
            _ => LoaderAction::Reject,
        };

        Ok(LoadDecision {
            node_id: self.node_id.clone(),
            module_id: request.artifact.module_id,
            artifact_id: request.artifact.artifact_id,
            action,
            reason: "phase-2 loader performed manifest/policy/ring checks".to_string(),
        })
    }

    pub fn shadow_state(
        &self,
        request: &LoadRequest,
        active_artifact_id: sinomo_core::ArtifactId,
    ) -> ModuleLoaderState {
        ModuleLoaderState {
            module_id: request.artifact.module_id.clone(),
            active_artifact_id,
            previous_artifact_id: None,
            shadow_artifact_id: Some(request.artifact.artifact_id.clone()),
            candidate_artifact_id: Some(request.artifact.artifact_id.clone()),
            hot_swap_state: HotSwapState::Shadowing,
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ModuleLoadError {
    #[error("load request targets {actual}, but loader belongs to {expected}")]
    WrongNode { expected: NodeId, actual: NodeId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        ActorKind, ActorRef, ArtifactId, ArtifactManifest, ArtifactProvenance, CapabilityDiff,
        ModuleManifest, PolicyDecisionId, RollbackPolicy, RolloutPlanId, RolloutStrategy,
    };

    #[test]
    fn r3_allowed_artifact_loads_in_shadow() {
        let node_id = NodeId::new("node_desktop").expect("node");
        let module_id = ModuleId::new("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::new("artifact_candidate").expect("artifact");
        let artifact = ArtifactManifest {
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            content_hash: "sha256:abc".to_string(),
            signature: None,
            source_workspace: None,
            source_revision: None,
            module_manifest: ModuleManifest::r3_wasm(module_id.clone(), "0.1.0", "trigger"),
            provenance: ArtifactProvenance {
                produced_by: ActorRef {
                    kind: ActorKind::EvolutionOrchestrator,
                    id: "phase2-test".to_string(),
                },
                source_event_id: None,
                build_log_artifact_id: None,
                attestations: Vec::new(),
            },
        };
        let request = LoadRequest {
            node_id: node_id.clone(),
            artifact,
            policy_decision: PolicyDecision {
                decision_id: PolicyDecisionId::new("policy_ok").expect("policy"),
                target_ring: TrustRing::R3SandboxedModule,
                target_artifact_id: artifact_id.clone(),
                action: PolicyAction::Allow,
                capability_diff: CapabilityDiff::default(),
                required_additional_evals: Vec::new(),
                max_rollout_weight_bps: Some(500),
                reason: "ok".to_string(),
            },
            rollout_plan: RolloutPlan {
                rollout_plan_id: RolloutPlanId::new("rollout_ok").expect("rollout"),
                artifact_id,
                module_id,
                target_ring: TrustRing::R3SandboxedModule,
                strategy: RolloutStrategy::Shadow,
                target_nodes: vec![node_id.clone()],
                initial_weight_bps: 0,
                promotion_steps_bps: Vec::new(),
                rollback_policy: RollbackPolicy::default(),
            },
        };

        let decision = ModuleLoader::new(node_id)
            .evaluate_load(request)
            .expect("load");
        assert_eq!(decision.action, LoaderAction::AcceptShadow);
    }
}
