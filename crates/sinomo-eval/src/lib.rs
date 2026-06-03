//! Local command and replay evaluation skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    Confidence, EvaluationGate, EvaluationGateResult, EvaluationMetric, EvaluationOutcome,
    EvaluationScorecard, EventEnvelope, EventKind, Id, LatencyClass, MetricValue, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_events::{EventId, ReplayQuery};
use sinomo_module::{ArtifactId, ModuleReplayReport};
use sinomo_node::NodeId;
use sinomo_rollout::ScorecardId;
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct VerificationPlan {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<VerificationCommand>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub module_replay_reports: Vec<ModuleReplayReportRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct VerificationCommand {
    pub name: String,
    pub command: String,
    pub expected_exit_code: i32,
    pub gate: EvaluationGate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleReplayReportRef {
    pub name: String,
    pub report_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_artifact_id: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<ArtifactId>,
}

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

pub type CommandRunId = String;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandInactivityPolicy {
    pub initial_notifications: Vec<Duration>,
    pub repeat_every: Duration,
    pub poll_interval: Duration,
}

impl Default for CommandInactivityPolicy {
    fn default() -> Self {
        Self {
            initial_notifications: vec![Duration::from_secs(60), Duration::from_secs(300)],
            repeat_every: Duration::from_secs(600),
            poll_interval: Duration::from_millis(250),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommandInactivityNotification {
    pub run_id: CommandRunId,
    pub command_name: String,
    pub idle_for_ms: u64,
    pub elapsed_ms: u64,
    pub notification_index: usize,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum CommandRunEvent {
    Started {
        run_id: CommandRunId,
        command_name: String,
    },
    Output {
        run_id: CommandRunId,
        stream: CommandOutputStream,
        byte_count: usize,
    },
    Inactivity(CommandInactivityNotification),
    Interrupted {
        run_id: CommandRunId,
    },
    Killed {
        run_id: CommandRunId,
    },
    Exited {
        run_id: CommandRunId,
        exit_code: i32,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum CommandOutputStream {
    Stdout,
    Stderr,
}

pub trait CommandRunObserver: Send + Sync {
    fn on_command_event(&self, event: CommandRunEvent);
}

#[derive(Clone, Debug, Default)]
pub struct NoopCommandRunObserver;

impl CommandRunObserver for NoopCommandRunObserver {
    fn on_command_event(&self, _event: CommandRunEvent) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandControlAction {
    Interrupt,
    Kill,
}

#[derive(Default, Debug)]
pub struct CommandControlRegistry {
    active: Mutex<BTreeMap<CommandRunId, ActiveCommand>>,
}

#[derive(Clone, Debug)]
struct ActiveCommand {
    pid: u32,
    requested_action: Arc<Mutex<Option<CommandControlAction>>>,
}

impl CommandControlRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn interrupt(&self, run_id: &str) -> Result<(), EvalError> {
        self.control(run_id, CommandControlAction::Interrupt)
    }

    pub fn kill(&self, run_id: &str) -> Result<(), EvalError> {
        self.control(run_id, CommandControlAction::Kill)
    }

    pub fn active_run_ids(&self) -> Vec<CommandRunId> {
        self.active
            .lock()
            .expect("command registry lock")
            .keys()
            .cloned()
            .collect()
    }

    fn register(
        &self,
        run_id: CommandRunId,
        pid: u32,
        requested_action: Arc<Mutex<Option<CommandControlAction>>>,
    ) {
        self.active.lock().expect("command registry lock").insert(
            run_id,
            ActiveCommand {
                pid,
                requested_action,
            },
        );
    }

    fn unregister(&self, run_id: &str) {
        self.active
            .lock()
            .expect("command registry lock")
            .remove(run_id);
    }

    fn control(&self, run_id: &str, action: CommandControlAction) -> Result<(), EvalError> {
        let command = self
            .active
            .lock()
            .expect("command registry lock")
            .get(run_id)
            .cloned()
            .ok_or_else(|| EvalError::UnknownCommandRun {
                run_id: run_id.to_string(),
            })?;
        send_control_signal(run_id, command.pid, action)?;
        *command
            .requested_action
            .lock()
            .expect("command control action lock") = Some(action);
        Ok(())
    }
}

#[derive(Clone)]
pub struct LocalCommandRunner {
    inactivity_policy: CommandInactivityPolicy,
    observer: Arc<dyn CommandRunObserver>,
    control: Arc<CommandControlRegistry>,
}

impl std::fmt::Debug for LocalCommandRunner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalCommandRunner")
            .field("inactivity_policy", &self.inactivity_policy)
            .finish_non_exhaustive()
    }
}

impl Default for LocalCommandRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalCommandRunner {
    pub fn new() -> Self {
        Self::with_observer(
            CommandInactivityPolicy::default(),
            NoopCommandRunObserver,
            CommandControlRegistry::new_shared(),
        )
    }

    pub fn with_observer<O>(
        inactivity_policy: CommandInactivityPolicy,
        observer: O,
        control: Arc<CommandControlRegistry>,
    ) -> Self
    where
        O: CommandRunObserver + 'static,
    {
        Self {
            inactivity_policy,
            observer: Arc::new(observer),
            control,
        }
    }

    pub fn control_registry(&self) -> Arc<CommandControlRegistry> {
        self.control.clone()
    }
}

impl CommandRunner for LocalCommandRunner {
    fn run(
        &self,
        command: &VerificationCommand,
        cwd: &Path,
    ) -> Result<CommandRunResult, EvalError> {
        let run_id = next_command_run_id();
        let start = Instant::now();
        let mut child_command = shell_command(&command.command);
        child_command
            .current_dir(cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_command_process_group(&mut child_command);
        let mut child = child_command
            .spawn()
            .map_err(|error| EvalError::CommandSpawnFailed {
                command: command.command.clone(),
                message: error.to_string(),
            })?;
        let requested_action = Arc::new(Mutex::new(None));
        self.control
            .register(run_id.clone(), child.id(), requested_action.clone());
        self.observer.on_command_event(CommandRunEvent::Started {
            run_id: run_id.clone(),
            command_name: command.name.clone(),
        });

        let (output_tx, output_rx) = mpsc::channel();
        if let Some(stdout) = child.stdout.take() {
            spawn_output_reader(stdout, CommandOutputStream::Stdout, output_tx.clone());
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_output_reader(stderr, CommandOutputStream::Stderr, output_tx.clone());
        }
        drop(output_tx);

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut closed_streams = 0usize;
        let mut status = None;
        let mut last_output = Instant::now();
        let mut next_notification = next_inactivity_threshold(&self.inactivity_policy, 0);
        let mut notification_index = 0usize;

        while status.is_none() || closed_streams < 2 {
            match output_rx.recv_timeout(self.inactivity_policy.poll_interval) {
                Ok(OutputMessage::Chunk { stream, bytes }) => {
                    if !bytes.is_empty() {
                        match stream {
                            CommandOutputStream::Stdout => stdout.extend_from_slice(&bytes),
                            CommandOutputStream::Stderr => stderr.extend_from_slice(&bytes),
                        }
                        last_output = Instant::now();
                        notification_index = 0;
                        next_notification = next_inactivity_threshold(&self.inactivity_policy, 0);
                        self.observer.on_command_event(CommandRunEvent::Output {
                            run_id: run_id.clone(),
                            stream,
                            byte_count: bytes.len(),
                        });
                    }
                }
                Ok(OutputMessage::Closed) => {
                    closed_streams += 1;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    closed_streams = 2;
                }
            }

            if status.is_none() {
                status = child
                    .try_wait()
                    .map_err(|error| EvalError::CommandWaitFailed {
                        command: command.command.clone(),
                        message: error.to_string(),
                    })?;
            }

            match (status.is_none(), next_notification) {
                (true, Some(threshold)) if last_output.elapsed() >= threshold => {
                    self.observer.on_command_event(CommandRunEvent::Inactivity(
                        CommandInactivityNotification {
                            run_id: run_id.clone(),
                            command_name: command.name.clone(),
                            idle_for_ms: duration_millis(last_output.elapsed()),
                            elapsed_ms: duration_millis(start.elapsed()),
                            notification_index,
                            message: format!(
                                "Command `{}` has produced no output for {}ms. The model may wait, interrupt, or kill it.",
                                command.name,
                                duration_millis(last_output.elapsed())
                            ),
                        },
                    ));
                    notification_index += 1;
                    next_notification =
                        next_inactivity_threshold(&self.inactivity_policy, notification_index);
                }
                _ => {}
            }
        }

        let status = status.expect("command status after loop");
        let exit_code = status.code().unwrap_or(-1);
        let requested_action = *requested_action
            .lock()
            .expect("command control action lock");
        match requested_action {
            Some(CommandControlAction::Interrupt) => {
                self.observer
                    .on_command_event(CommandRunEvent::Interrupted {
                        run_id: run_id.clone(),
                    })
            }
            Some(CommandControlAction::Kill) => {
                self.observer.on_command_event(CommandRunEvent::Killed {
                    run_id: run_id.clone(),
                })
            }
            None => {}
        }
        self.observer.on_command_event(CommandRunEvent::Exited {
            run_id: run_id.clone(),
            exit_code,
        });
        self.control.unregister(&run_id);

        Ok(CommandRunResult {
            run_id: Some(run_id),
            name: command.name.clone(),
            command: command.command.clone(),
            expected_exit_code: command.expected_exit_code,
            exit_code,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
            duration_ms: duration_millis(start.elapsed()),
        })
    }
}

#[derive(Debug)]
enum OutputMessage {
    Chunk {
        stream: CommandOutputStream,
        bytes: Vec<u8>,
    },
    Closed,
}

static NEXT_COMMAND_RUN_ID: AtomicU64 = AtomicU64::new(1);

fn next_command_run_id() -> CommandRunId {
    let id = NEXT_COMMAND_RUN_ID.fetch_add(1, Ordering::Relaxed);
    format!("cmd_{id}")
}

fn next_inactivity_threshold(
    policy: &CommandInactivityPolicy,
    notification_index: usize,
) -> Option<Duration> {
    if notification_index < policy.initial_notifications.len() {
        policy
            .initial_notifications
            .get(notification_index)
            .copied()
    } else if policy.repeat_every.is_zero() {
        None
    } else {
        policy
            .initial_notifications
            .last()
            .copied()
            .unwrap_or_default()
            .checked_add(
                policy.repeat_every
                    * (notification_index - policy.initial_notifications.len() + 1) as u32,
            )
    }
}

fn spawn_output_reader<R>(
    mut reader: R,
    stream: CommandOutputStream,
    sender: mpsc::Sender<OutputMessage>,
) where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0u8; 8192];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    if sender
                        .send(OutputMessage::Chunk {
                            stream,
                            bytes: buffer[..read].to_vec(),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = sender.send(OutputMessage::Closed);
    });
}

#[cfg(unix)]
fn configure_command_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    unsafe {
        command.pre_exec(|| {
            if set_process_group(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_command_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn send_control_signal(
    run_id: &str,
    pid: u32,
    action: CommandControlAction,
) -> Result<(), EvalError> {
    let signal = match action {
        CommandControlAction::Interrupt => 2,
        CommandControlAction::Kill => 9,
    };
    let target_process_group = -(pid as i32);
    if send_signal(target_process_group, signal) == -1 {
        return Err(EvalError::CommandControlFailed {
            run_id: run_id.to_string(),
            message: std::io::Error::last_os_error().to_string(),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn send_control_signal(
    run_id: &str,
    pid: u32,
    action: CommandControlAction,
) -> Result<(), EvalError> {
    if matches!(action, CommandControlAction::Kill) {
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()
            .map_err(|error| EvalError::CommandControlFailed {
                run_id: run_id.to_string(),
                message: error.to_string(),
            })?;
        if status.success() {
            return Ok(());
        }
    }
    Err(EvalError::CommandControlFailed {
        run_id: run_id.to_string(),
        message: format!("{action:?} is not supported on this platform"),
    })
}

#[cfg(unix)]
fn send_signal(pid: i32, signal: i32) -> i32 {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe { kill(pid, signal) }
}

#[cfg(unix)]
fn set_process_group(pid: i32, pgid: i32) -> i32 {
    unsafe extern "C" {
        fn setpgid(pid: i32, pgid: i32) -> i32;
    }
    unsafe { setpgid(pid, pgid) }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommandRunResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<CommandRunId>,
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
        if plan.commands.is_empty() && plan.module_replay_reports.is_empty() {
            return Err(EvalError::EmptyVerificationPlan);
        }

        let mut gates =
            Vec::with_capacity(plan.commands.len() + plan.module_replay_reports.len() * 2);
        let mut results = Vec::with_capacity(plan.commands.len());
        let mut outcome = EvaluationOutcome::Passed;

        for command in &plan.commands {
            let result = self.runner.run(command, cwd)?;
            let command_outcome = if result.passed() {
                EvaluationOutcome::Passed
            } else {
                EvaluationOutcome::Failed
            };
            merge_outcome(&mut outcome, &command_outcome);

            gates.push(EvaluationGateResult {
                gate: command.gate.clone(),
                outcome: command_outcome,
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

        let mut module_replay_metrics = Vec::new();
        for report_ref in &plan.module_replay_reports {
            let report = read_module_replay_report(cwd, report_ref)?;
            let replay_scorecard = module_replay_report_scorecard(
                &report,
                report_ref.baseline_artifact_id.clone(),
                report_ref.candidate_artifact_id.clone(),
            );
            merge_outcome(&mut outcome, &replay_scorecard.outcome);
            gates.extend(
                replay_scorecard
                    .gates
                    .into_iter()
                    .map(|gate| EvaluationGateResult {
                        gate: gate.gate,
                        outcome: gate.outcome,
                        summary: format!(
                            "module replay `{}` from `{}`: {}",
                            report_ref.name, report_ref.report_path, gate.summary
                        ),
                    }),
            );
            module_replay_metrics.extend(replay_scorecard.metrics);
        }
        let total_duration_ms = results.iter().map(|result| result.duration_ms).sum::<u64>();
        let mut metrics = vec![
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
        ];
        if !plan.module_replay_reports.is_empty() {
            metrics.push(EvaluationMetric {
                name: "module_replay_report_count".to_string(),
                value: MetricValue::Integer(plan.module_replay_reports.len() as i64),
                sample_count: Some(plan.module_replay_reports.len() as u64),
            });
            metrics.extend(module_replay_metrics);
        }

        Ok((
            EvaluationScorecard {
                scorecard_id: ScorecardId::generate(),
                baseline_artifact_id: None,
                candidate_artifact_id: None,
                outcome,
                gates,
                metrics,
            },
            results,
        ))
    }
}

fn read_module_replay_report(
    cwd: &Path,
    report_ref: &ModuleReplayReportRef,
) -> Result<ModuleReplayReport, EvalError> {
    let path = resolve_verification_path(cwd, &report_ref.report_path);
    let content =
        fs::read_to_string(&path).map_err(|source| EvalError::ModuleReplayReportRead {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    serde_json::from_str(&content).map_err(|source| EvalError::ModuleReplayReportParse {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn module_replay_report_scorecard(
    report: &ModuleReplayReport,
    baseline_artifact_id: Option<ArtifactId>,
    candidate_artifact_id: Option<ArtifactId>,
) -> EvaluationScorecard {
    let outcome = module_replay_report_outcome(report);
    EvaluationScorecard {
        scorecard_id: ScorecardId::generate(),
        baseline_artifact_id,
        candidate_artifact_id,
        outcome: outcome.clone(),
        gates: vec![
            EvaluationGateResult {
                gate: EvaluationGate::ReplayTests,
                outcome: if report.missing_route_count == 0 && !report.cases.is_empty() {
                    EvaluationOutcome::Passed
                } else {
                    EvaluationOutcome::Failed
                },
                summary: format!(
                    "{} replay cases, {} missing routes",
                    report.cases.len(),
                    report.missing_route_count
                ),
            },
            EvaluationGateResult {
                gate: EvaluationGate::ShadowMode,
                outcome,
                summary: format!(
                    "{} primary errors, {} shadow errors, {} shadow mismatches",
                    report.primary_error_count,
                    report.shadow_error_count,
                    report.shadow_mismatch_count
                ),
            },
        ],
        metrics: vec![
            eval_metric("module_replay_case_count", report.cases.len() as i64),
            eval_metric(
                "module_replay_event_count",
                report.replay_event_count as i64,
            ),
            eval_metric(
                "module_replay_missing_route_count",
                report.missing_route_count as i64,
            ),
            eval_metric(
                "module_replay_primary_error_count",
                report.primary_error_count as i64,
            ),
            eval_metric(
                "module_replay_shadow_error_count",
                report.shadow_error_count as i64,
            ),
            eval_metric(
                "module_replay_shadow_mismatch_count",
                report.shadow_mismatch_count as i64,
            ),
        ],
    }
}

fn module_replay_report_outcome(report: &ModuleReplayReport) -> EvaluationOutcome {
    if report.cases.is_empty() {
        return EvaluationOutcome::Inconclusive;
    }
    if report.missing_route_count == 0
        && report.primary_error_count == 0
        && report.shadow_error_count == 0
        && report.shadow_mismatch_count == 0
    {
        EvaluationOutcome::Passed
    } else {
        EvaluationOutcome::Failed
    }
}

fn eval_metric(name: &str, value: i64) -> EvaluationMetric {
    EvaluationMetric {
        name: name.to_string(),
        value: MetricValue::Integer(value),
        sample_count: None,
    }
}

fn resolve_verification_path(cwd: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

fn merge_outcome(current: &mut EvaluationOutcome, next: &EvaluationOutcome) {
    match (&*current, next) {
        (EvaluationOutcome::Failed, _) | (_, EvaluationOutcome::Passed) => {}
        (_, EvaluationOutcome::Failed) => *current = EvaluationOutcome::Failed,
        (EvaluationOutcome::Passed, EvaluationOutcome::Inconclusive) => {
            *current = EvaluationOutcome::Inconclusive;
        }
        (EvaluationOutcome::Inconclusive, EvaluationOutcome::Inconclusive) => {}
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
        subject_id: Option<Id>,
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
            "subject_id": subject_id,
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
    #[error("verification plan must contain at least one command or module replay report")]
    EmptyVerificationPlan,
    #[error("failed to read module replay report `{path}`: {detail}")]
    ModuleReplayReportRead { path: String, detail: String },
    #[error("failed to parse module replay report `{path}`: {detail}")]
    ModuleReplayReportParse { path: String, detail: String },
    #[error("failed to spawn command `{command}`: {message}")]
    CommandSpawnFailed { command: String, message: String },
    #[error("failed to wait for command `{command}`: {message}")]
    CommandWaitFailed { command: String, message: String },
    #[error("unknown active command run `{run_id}`")]
    UnknownCommandRun { run_id: String },
    #[error("failed to control command run `{run_id}`: {message}")]
    CommandControlFailed { run_id: String, message: String },
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
    use sinomo_events::SessionId;
    use std::env;

    #[test]
    fn command_suite_produces_passing_scorecard_with_replay_fixture_count() {
        let harness =
            LocalEvalHarness::new(NodeId::from_legacy_named("node_server").expect("node"));
        let scorecard = harness
            .run(CommandEvaluationSuite {
                candidate_artifact_id: ArtifactId::from_legacy_named("artifact_candidate")
                    .expect("artifact"),
                baseline_artifact_id: None,
                specs: vec![CommandEvaluationSpec {
                    gate: EvaluationGate::ReplayTests,
                    command: "cargo test -p sinomo-agent".to_string(),
                    expected_exit_code: 0,
                    observed_exit_code: 0,
                    replay_fixtures: vec![ReplayFixtureRef {
                        name: "urgent alert replay".to_string(),
                        fixture_uri: "fixtures/replay/urgent-alert.jsonl".to_string(),
                        query: ReplayQuery::by_session(
                            SessionId::from_legacy_named("sess_eval").expect("session"),
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
                    module_replay_reports: Vec::new(),
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
                    module_replay_reports: Vec::new(),
                },
                &env::current_dir().expect("cwd"),
            )
            .expect("verification plan");

        assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
        assert_eq!(scorecard.gates[0].outcome, EvaluationOutcome::Failed);
        assert_ne!(results[0].exit_code, 0);
    }

    #[test]
    fn executing_harness_ingests_module_replay_report() {
        let root = temp_dir("module-replay-report");
        fs::create_dir_all(&root).expect("temp dir");
        let report_path = root.join("module-replay-report.json");
        let report = ModuleReplayReport {
            fixture_id: Id::from_legacy_named("fixture_module_replay").expect("fixture"),
            module_id: Id::from_legacy_named("module_trigger").expect("module"),
            cases: vec![sinomo_module::ModuleReplayCaseOutcome {
                case_id: "case-1".to_string(),
                route_key: "case-1".to_string(),
                source_event_id: None,
                outcome: None,
                shadow_matched: Some(true),
            }],
            replay_event_count: 1,
            missing_route_count: 0,
            primary_error_count: 0,
            shadow_error_count: 0,
            shadow_mismatch_count: 0,
            events: Vec::new(),
        };
        fs::write(
            &report_path,
            serde_json::to_string_pretty(&report).expect("report json"),
        )
        .expect("write report");

        let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());
        let (scorecard, results) = harness
            .run_verification_plan(
                VerificationPlan {
                    commands: Vec::new(),
                    module_replay_reports: vec![ModuleReplayReportRef {
                        name: "trigger replay".to_string(),
                        report_path: "module-replay-report.json".to_string(),
                        baseline_artifact_id: None,
                        candidate_artifact_id: None,
                    }],
                },
                &root,
            )
            .expect("verification plan");

        assert!(results.is_empty());
        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        assert!(scorecard.gates.iter().any(|gate| {
            gate.gate == EvaluationGate::ReplayTests
                && gate.summary.contains("module replay `trigger replay`")
        }));
        assert!(scorecard.metrics.iter().any(|metric| {
            metric.name == "module_replay_report_count" && metric.value == MetricValue::Integer(1)
        }));
    }

    #[test]
    fn local_command_runner_captures_stdout_and_stderr() {
        let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());
        let (_scorecard, results) = harness
            .run_verification_plan(
                VerificationPlan {
                    commands: vec![verification_command("output", output_command(), 0)],
                    module_replay_reports: Vec::new(),
                },
                &env::current_dir().expect("cwd"),
            )
            .expect("verification plan");

        assert!(results[0].stdout.contains("sinomo-stdout"));
        assert!(results[0].stderr.contains("sinomo-stderr"));
    }

    #[test]
    #[cfg(not(windows))]
    fn local_command_runner_notifies_when_command_has_no_output() {
        let observer = RecordingCommandObserver::default();
        let runner = LocalCommandRunner::with_observer(
            CommandInactivityPolicy {
                initial_notifications: vec![Duration::from_millis(20), Duration::from_millis(50)],
                repeat_every: Duration::from_millis(40),
                poll_interval: Duration::from_millis(5),
            },
            observer.clone(),
            CommandControlRegistry::new_shared(),
        );

        let result = runner
            .run(
                &verification_command("quiet", "sleep 0.12", 0),
                &env::current_dir().expect("cwd"),
            )
            .expect("command result");

        assert_eq!(result.exit_code, 0);
        let inactivity_events = observer.inactivity_events();
        assert!(
            inactivity_events.len() >= 3,
            "expected at least 3 inactivity notifications, got {inactivity_events:?}"
        );
        assert!(inactivity_events[0].idle_for_ms >= 20);
        assert!(inactivity_events[1].idle_for_ms >= 50);
        assert!(inactivity_events[2].idle_for_ms >= 90);
    }

    #[test]
    #[cfg(unix)]
    fn command_control_registry_interrupts_running_command() {
        use std::sync::mpsc;
        use std::thread;

        let observer = RecordingCommandObserver::default();
        let control = CommandControlRegistry::new_shared();
        let runner = LocalCommandRunner::with_observer(
            CommandInactivityPolicy {
                initial_notifications: vec![Duration::from_secs(60)],
                repeat_every: Duration::from_secs(60),
                poll_interval: Duration::from_millis(5),
            },
            observer.clone(),
            control.clone(),
        );
        let (started_tx, started_rx) = mpsc::channel();
        observer.forward_started_to(started_tx);

        let handle = thread::spawn(move || {
            runner.run(
                &verification_command("interruptible", "trap 'exit 42' INT; sleep 5", 42),
                &env::current_dir().expect("cwd"),
            )
        });
        let run_id = started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("started");
        thread::sleep(Duration::from_millis(50));

        control.interrupt(&run_id).expect("interrupt sent");
        let result = handle
            .join()
            .expect("runner thread")
            .expect("command result");

        assert_eq!(result.exit_code, 42);
        assert_eq!(result.run_id.as_deref(), Some(run_id.as_str()));
        assert!(observer.has_interrupted_event(&run_id));
    }

    #[derive(Clone, Default)]
    struct RecordingCommandObserver {
        events: Arc<Mutex<Vec<CommandRunEvent>>>,
        started_sender: Arc<Mutex<Option<mpsc::Sender<CommandRunId>>>>,
    }

    impl RecordingCommandObserver {
        fn forward_started_to(&self, sender: mpsc::Sender<CommandRunId>) {
            *self.started_sender.lock().expect("started sender lock") = Some(sender);
        }

        fn inactivity_events(&self) -> Vec<CommandInactivityNotification> {
            self.events
                .lock()
                .expect("events lock")
                .iter()
                .filter_map(|event| match event {
                    CommandRunEvent::Inactivity(notification) => Some(notification.clone()),
                    _ => None,
                })
                .collect()
        }

        fn has_interrupted_event(&self, expected_run_id: &str) -> bool {
            self.events
                .lock()
                .expect("events lock")
                .iter()
                .any(|event| matches!(event, CommandRunEvent::Interrupted { run_id } if run_id == expected_run_id))
        }
    }

    impl CommandRunObserver for RecordingCommandObserver {
        fn on_command_event(&self, event: CommandRunEvent) {
            let started_run_id = match &event {
                CommandRunEvent::Started { run_id, .. } => Some(run_id.clone()),
                _ => None,
            };
            if let Some(run_id) = started_run_id {
                let sender = self
                    .started_sender
                    .lock()
                    .expect("started sender lock")
                    .clone();
                if let Some(sender) = sender {
                    let _ = sender.send(run_id);
                }
            }
            self.events.lock().expect("events lock").push(event);
        }
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

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = OffsetDateTime::now_utc()
            .unix_timestamp_nanos()
            .abs()
            .to_string();
        env::temp_dir().join(format!("sinomo-eval-{name}-{stamp}"))
    }
}
