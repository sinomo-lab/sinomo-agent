use serde::{Deserialize, Serialize};
use sinomo_core::{
    ArtifactId, Confidence, EvaluationOutcome, EvaluationScorecard, EventEnvelope, EventId,
    EventKind, HotSwapState, LatencyClass, ModuleLoaderState, NodeId, PrivacyClass, RolloutPlan,
    RolloutStrategy, RoutingMetadata, RoutingScope, Salience, Source, TrustRing,
};
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RolloutRuntimeState {
    pub plan: RolloutPlan,
    pub loader_state: ModuleLoaderState,
    pub stage: RolloutStage,
    pub current_weight_bps: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RolloutStage {
    Shadow,
    Canary,
    GuardedR1,
    Promoted,
    RolledBack,
    Quarantined,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RolloutObservation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard: Option<EvaluationScorecard>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub incident_event_ids: Vec<EventId>,
    pub user_correction_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RolloutDecision {
    pub action: RolloutAction,
    pub state: RolloutRuntimeState,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RolloutAction {
    StayShadow,
    AdvanceCanary,
    Promote,
    GuardR1Activation,
    RollBack,
    Quarantine,
}

#[derive(Clone, Debug)]
pub struct RolloutController {
    node_id: NodeId,
}

impl RolloutController {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn initial_state(
        &self,
        plan: RolloutPlan,
        active_artifact_id: ArtifactId,
    ) -> RolloutRuntimeState {
        RolloutRuntimeState {
            loader_state: ModuleLoaderState {
                module_id: plan.module_id.clone(),
                active_artifact_id,
                previous_artifact_id: None,
                shadow_artifact_id: Some(plan.artifact_id.clone()),
                candidate_artifact_id: Some(plan.artifact_id.clone()),
                hot_swap_state: HotSwapState::Shadowing,
            },
            current_weight_bps: plan.initial_weight_bps,
            stage: match plan.strategy {
                RolloutStrategy::Shadow => RolloutStage::Shadow,
                RolloutStrategy::Canary | RolloutStrategy::Rolling | RolloutStrategy::BlueGreen => {
                    RolloutStage::Canary
                }
                RolloutStrategy::Immediate => RolloutStage::Promoted,
            },
            plan,
        }
    }

    pub fn advance(
        &self,
        mut state: RolloutRuntimeState,
        observation: RolloutObservation,
    ) -> RolloutDecision {
        if !observation.incident_event_ids.is_empty()
            || observation.user_correction_count > 0
            || observation
                .scorecard
                .as_ref()
                .is_some_and(|scorecard| scorecard.outcome == EvaluationOutcome::Failed)
        {
            state.stage = RolloutStage::RolledBack;
            state.current_weight_bps = 0;
            state.loader_state.hot_swap_state = HotSwapState::RollingBack;
            state.loader_state.candidate_artifact_id = None;
            return RolloutDecision {
                action: RolloutAction::RollBack,
                state,
                reason: "incident, user correction, or failed scorecard triggered rollback"
                    .to_string(),
            };
        }

        if state.plan.target_ring == TrustRing::R1TrustedRuntime {
            state.stage = RolloutStage::GuardedR1;
            state.loader_state.hot_swap_state = HotSwapState::ActivationCheckpoint;
            return RolloutDecision {
                action: RolloutAction::GuardR1Activation,
                state,
                reason: "R1 candidate requires guarded activation checkpoint".to_string(),
            };
        }

        if observation
            .scorecard
            .as_ref()
            .is_some_and(|scorecard| scorecard.outcome == EvaluationOutcome::Passed)
        {
            let next_weight =
                next_weight(state.current_weight_bps, &state.plan.promotion_steps_bps);
            if next_weight >= 10_000 || state.stage == RolloutStage::Promoted {
                state.stage = RolloutStage::Promoted;
                state.current_weight_bps = 10_000;
                state.loader_state.previous_artifact_id =
                    Some(state.loader_state.active_artifact_id.clone());
                state.loader_state.active_artifact_id = state.plan.artifact_id.clone();
                state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
                return RolloutDecision {
                    action: RolloutAction::Promote,
                    state,
                    reason: "passed evidence reached full rollout".to_string(),
                };
            }

            state.stage = RolloutStage::Canary;
            state.current_weight_bps = next_weight;
            state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
            return RolloutDecision {
                action: RolloutAction::AdvanceCanary,
                state,
                reason: "passed evidence advanced canary weight".to_string(),
            };
        }

        RolloutDecision {
            action: RolloutAction::StayShadow,
            state,
            reason: "waiting for passing scorecard evidence".to_string(),
        }
    }

    pub fn decision_event(&self, decision: &RolloutDecision) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("rollout.controller").expect("valid source"),
            EventKind::new("rollout.transition.completed").expect("valid event kind"),
            Salience::new(if decision.action == RolloutAction::RollBack {
                0.9
            } else {
                0.76
            })
            .expect("valid salience"),
            Confidence::new(0.95).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(self.node_id.clone())
        .with_payload(serde_json::json!({
            "rollout_plan_id": decision.state.plan.rollout_plan_id,
            "artifact_id": decision.state.plan.artifact_id,
            "module_id": decision.state.plan.module_id,
            "action": decision.action,
            "stage": decision.state.stage,
            "current_weight_bps": decision.state.current_weight_bps,
            "reason": decision.reason,
        }))
    }
}

fn next_weight(current: u16, steps: &[u16]) -> u16 {
    steps
        .iter()
        .copied()
        .find(|step| *step > current)
        .unwrap_or(10_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        EvaluationGate, EvaluationGateResult, ModuleId, RollbackPolicy, RolloutPlanId, ScorecardId,
    };

    #[test]
    fn passed_shadow_scorecard_advances_r3_canary() {
        let node = NodeId::new("node_desktop").expect("node");
        let plan = RolloutPlan {
            rollout_plan_id: RolloutPlanId::new("rollout_candidate").expect("rollout"),
            artifact_id: ArtifactId::new("artifact_candidate").expect("artifact"),
            module_id: ModuleId::new("module.trigger.silence_policy").expect("module"),
            target_ring: TrustRing::R3SandboxedModule,
            strategy: RolloutStrategy::Shadow,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500, 2_500],
            rollback_policy: RollbackPolicy::default(),
        };
        let controller = RolloutController::new(node);
        let state =
            controller.initial_state(plan, ArtifactId::new("artifact_active").expect("artifact"));
        let decision = controller.advance(
            state,
            RolloutObservation {
                scorecard: Some(EvaluationScorecard {
                    scorecard_id: ScorecardId::new("score_candidate").expect("score"),
                    baseline_artifact_id: None,
                    candidate_artifact_id: Some(
                        ArtifactId::new("artifact_candidate").expect("artifact"),
                    ),
                    outcome: EvaluationOutcome::Passed,
                    gates: vec![EvaluationGateResult {
                        gate: EvaluationGate::ReplayTests,
                        outcome: EvaluationOutcome::Passed,
                        summary: "ok".to_string(),
                    }],
                    metrics: Vec::new(),
                }),
                incident_event_ids: Vec::new(),
                user_correction_count: 0,
            },
        );

        assert_eq!(decision.action, RolloutAction::AdvanceCanary);
        assert_eq!(decision.state.current_weight_bps, 500);
    }
}
