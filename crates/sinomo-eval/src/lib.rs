//! Local command and replay evaluation skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    ArtifactId, Confidence, EvaluationGate, EvaluationGateResult, EvaluationMetric,
    EvaluationOutcome, EvaluationScorecard, EventEnvelope, EventId, EventKind, EvolutionJobId,
    LatencyClass, MetricValue, NodeId, PrivacyClass, RoutingMetadata, RoutingScope, Salience,
    ScorecardId, Source,
};
use sinomo_events::ReplayQuery;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommandEvaluationSpec {
    pub gate: EvaluationGate,
    pub command: String,
    pub expected_exit_code: i32,
    pub observed_exit_code: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replay_fixtures: Vec<ReplayFixtureRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_excerpt: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplayFixtureRef {
    pub name: String,
    pub fixture_uri: String,
    pub query: ReplayQuery,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommandEvaluationSuite {
    pub candidate_artifact_id: ArtifactId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_artifact_id: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub specs: Vec<CommandEvaluationSpec>,
}

#[derive(Clone, Debug)]
pub struct LocalEvalHarness {
    evaluator_node_id: NodeId,
}

impl LocalEvalHarness {
    pub fn new(evaluator_node_id: NodeId) -> Self {
        Self { evaluator_node_id }
    }

    pub fn run(&self, suite: CommandEvaluationSuite) -> Result<EvaluationScorecard, EvalError> {
        if suite.specs.is_empty() {
            return Err(EvalError::EmptySuite);
        }

        let mut gates = Vec::with_capacity(suite.specs.len());
        let mut passed = 0;
        let mut replay_fixture_count = 0;

        for spec in &suite.specs {
            let outcome = if spec.observed_exit_code == spec.expected_exit_code {
                passed += 1;
                EvaluationOutcome::Passed
            } else {
                EvaluationOutcome::Failed
            };
            replay_fixture_count += spec.replay_fixtures.len();
            gates.push(EvaluationGateResult {
                gate: spec.gate.clone(),
                outcome,
                summary: format!(
                    "`{}` exited {}, expected {}",
                    spec.command, spec.observed_exit_code, spec.expected_exit_code
                ),
            });
        }

        let outcome = if passed == suite.specs.len() {
            EvaluationOutcome::Passed
        } else {
            EvaluationOutcome::Failed
        };

        Ok(EvaluationScorecard {
            scorecard_id: ScorecardId::generate(),
            baseline_artifact_id: suite.baseline_artifact_id,
            candidate_artifact_id: Some(suite.candidate_artifact_id),
            outcome,
            gates,
            metrics: vec![
                EvaluationMetric {
                    name: "command_count".to_string(),
                    value: MetricValue::Integer(suite.specs.len() as i64),
                    sample_count: Some(suite.specs.len() as u64),
                },
                EvaluationMetric {
                    name: "replay_fixture_count".to_string(),
                    value: MetricValue::Integer(replay_fixture_count as i64),
                    sample_count: Some(replay_fixture_count as u64),
                },
            ],
        })
    }

    pub fn scorecard_event(
        &self,
        job_id: Option<EvolutionJobId>,
        scorecard: &EvaluationScorecard,
    ) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.evaluator_node_id.clone(),
            Source::new("eval.local").expect("valid source"),
            EventKind::new("eval.scorecard.completed").expect("valid event kind"),
            Salience::new(0.78).expect("valid salience"),
            Confidence::new(0.96).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "job_id": job_id,
            "scorecard": scorecard,
            "outcome": scorecard.outcome,
            "candidate_artifact_id": scorecard.candidate_artifact_id,
        }))
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum EvalError {
    #[error("evaluation suite must contain at least one command spec")]
    EmptySuite,
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::SessionId;

    #[test]
    fn command_suite_produces_passing_scorecard_with_replay_fixture_count() {
        let harness = LocalEvalHarness::new(NodeId::new("node_server").expect("node"));
        let scorecard = harness
            .run(CommandEvaluationSuite {
                candidate_artifact_id: ArtifactId::new("artifact_candidate").expect("artifact"),
                baseline_artifact_id: None,
                specs: vec![CommandEvaluationSpec {
                    gate: EvaluationGate::ReplayTests,
                    command: "cargo test -p sinomo-trigger".to_string(),
                    expected_exit_code: 0,
                    observed_exit_code: 0,
                    replay_fixtures: vec![ReplayFixtureRef {
                        name: "urgent alert replay".to_string(),
                        fixture_uri: "fixtures/replay/urgent-alert.jsonl".to_string(),
                        query: ReplayQuery::by_session(
                            SessionId::new("sess_eval").expect("session"),
                        ),
                    }],
                    log_excerpt: Some("ok".to_string()),
                }],
            })
            .expect("scorecard");

        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        assert_eq!(scorecard.metrics[1].value, MetricValue::Integer(1));
    }
}
