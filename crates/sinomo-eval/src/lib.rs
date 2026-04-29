//! Local command and replay evaluation skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    ArtifactId, Confidence, EvaluationGate, EvaluationGateResult, EvaluationMetric,
    EvaluationOutcome, EvaluationScorecard, EventEnvelope, EventId, EventKind, EvolutionJobId,
    LatencyClass, MetricValue, NodeId, PrivacyClass, RoutingMetadata, RoutingScope, Salience,
    ScorecardId, Source, VerificationCommand, VerificationPlan,
};
use sinomo_events::ReplayQuery;
use std::{
    path::Path,
    process::Command,
    time::{Duration, Instant},
};
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

pub trait CommandRunner {
    fn run(&self, command: &VerificationCommand, cwd: &Path)
    -> Result<CommandRunResult, EvalError>;
}

#[derive(Clone, Debug, Default)]
pub struct LocalCommandRunner;

impl LocalCommandRunner {
    pub fn new() -> Self {
        Self
    }
}

impl CommandRunner for LocalCommandRunner {
    fn run(
        &self,
        command: &VerificationCommand,
        cwd: &Path,
    ) -> Result<CommandRunResult, EvalError> {
        let start = Instant::now();
        let output = shell_command(&command.command)
            .current_dir(cwd)
            .output()
            .map_err(|error| EvalError::CommandSpawnFailed {
                command: command.command.clone(),
                message: error.to_string(),
            })?;
        let duration = start.elapsed();

        Ok(CommandRunResult {
            name: command.name.clone(),
            command: command.command.clone(),
            expected_exit_code: command.expected_exit_code,
            exit_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            duration_ms: duration_millis(duration),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommandRunResult {
    pub name: String,
    pub command: String,
    pub expected_exit_code: i32,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
}

impl CommandRunResult {
    pub fn passed(&self) -> bool {
        self.exit_code == self.expected_exit_code
    }
}

#[derive(Clone, Debug)]
pub struct ExecutingEvalHarness<R> {
    pub runner: R,
}

impl<R> ExecutingEvalHarness<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }
}

impl<R: CommandRunner> ExecutingEvalHarness<R> {
    pub fn run_verification_plan(
        &self,
        plan: VerificationPlan,
        cwd: &Path,
    ) -> Result<(EvaluationScorecard, Vec<CommandRunResult>), EvalError> {
        if plan.commands.is_empty() {
            return Err(EvalError::EmptyVerificationPlan);
        }

        let mut gates = Vec::with_capacity(plan.commands.len());
        let mut results = Vec::with_capacity(plan.commands.len());
        let mut passed = 0;

        for command in &plan.commands {
            let result = self.runner.run(command, cwd)?;
            let outcome = if result.passed() {
                passed += 1;
                EvaluationOutcome::Passed
            } else {
                EvaluationOutcome::Failed
            };

            gates.push(EvaluationGateResult {
                gate: command.gate.clone(),
                outcome,
                summary: format!(
                    "`{}` exited {}, expected {} in {}ms",
                    command.command,
                    result.exit_code,
                    command.expected_exit_code,
                    result.duration_ms
                ),
            });
            results.push(result);
        }

        let outcome = if passed == results.len() {
            EvaluationOutcome::Passed
        } else {
            EvaluationOutcome::Failed
        };
        let total_duration_ms = results.iter().map(|result| result.duration_ms).sum::<u64>();

        Ok((
            EvaluationScorecard {
                scorecard_id: ScorecardId::generate(),
                baseline_artifact_id: None,
                candidate_artifact_id: None,
                outcome,
                gates,
                metrics: vec![
                    EvaluationMetric {
                        name: "command_count".to_string(),
                        value: MetricValue::Integer(results.len() as i64),
                        sample_count: Some(results.len() as u64),
                    },
                    EvaluationMetric {
                        name: "command_duration_ms".to_string(),
                        value: MetricValue::Integer(total_duration_ms as i64),
                        sample_count: Some(results.len() as u64),
                    },
                ],
            },
            results,
        ))
    }
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
    #[error("verification plan must contain at least one command")]
    EmptyVerificationPlan,
    #[error("failed to spawn command `{command}`: {message}")]
    CommandSpawnFailed { command: String, message: String },
}

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

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::SessionId;
    use std::env;

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

    #[test]
    fn executing_harness_passes_successful_command() {
        let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());
        let (scorecard, results) = harness
            .run_verification_plan(
                VerificationPlan {
                    commands: vec![verification_command("success", success_command(), 0)],
                },
                &env::current_dir().expect("cwd"),
            )
            .expect("verification plan");

        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        assert_eq!(scorecard.gates[0].outcome, EvaluationOutcome::Passed);
        assert_eq!(results[0].exit_code, 0);
    }

    #[test]
    fn executing_harness_fails_nonzero_command() {
        let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());
        let (scorecard, results) = harness
            .run_verification_plan(
                VerificationPlan {
                    commands: vec![verification_command("failure", failure_command(), 0)],
                },
                &env::current_dir().expect("cwd"),
            )
            .expect("verification plan");

        assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
        assert_eq!(scorecard.gates[0].outcome, EvaluationOutcome::Failed);
        assert_ne!(results[0].exit_code, 0);
    }

    #[test]
    fn local_command_runner_captures_stdout_and_stderr() {
        let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());
        let (_scorecard, results) = harness
            .run_verification_plan(
                VerificationPlan {
                    commands: vec![verification_command("output", output_command(), 0)],
                },
                &env::current_dir().expect("cwd"),
            )
            .expect("verification plan");

        assert!(results[0].stdout.contains("sinomo-stdout"));
        assert!(results[0].stderr.contains("sinomo-stderr"));
    }

    fn verification_command(
        name: impl Into<String>,
        command: impl Into<String>,
        expected_exit_code: i32,
    ) -> VerificationCommand {
        VerificationCommand {
            name: name.into(),
            command: command.into(),
            expected_exit_code,
            gate: EvaluationGate::UnitTests,
        }
    }

    #[cfg(windows)]
    fn success_command() -> String {
        "exit 0".to_string()
    }

    #[cfg(not(windows))]
    fn success_command() -> String {
        "exit 0".to_string()
    }

    #[cfg(windows)]
    fn failure_command() -> String {
        "exit 7".to_string()
    }

    #[cfg(not(windows))]
    fn failure_command() -> String {
        "exit 7".to_string()
    }

    #[cfg(windows)]
    fn output_command() -> String {
        "Write-Output 'sinomo-stdout'; Write-Error 'sinomo-stderr'; exit 0".to_string()
    }

    #[cfg(not(windows))]
    fn output_command() -> String {
        "printf 'sinomo-stdout\\n'; printf 'sinomo-stderr\\n' >&2".to_string()
    }
}
