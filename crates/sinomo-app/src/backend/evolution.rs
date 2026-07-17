//! App-facing launcher and view over local self-evolution job persistence.

use std::sync::mpsc::{Receiver, channel};
use std::thread;

use sinomo_evolution::EvolutionJobStatus;
use sinomo_evolution::persistence::{EvolutionPaths, StoredEvolutionJob, list_job_records};
use sinomo_runtime::local::SinomoDataDir;

use crate::backend::Backend;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EvolutionDashboard {
    pub source: String,
    pub jobs: Vec<EvolutionJobSummary>,
    pub error: Option<String>,
}

impl EvolutionDashboard {
    pub fn load(backend: &Backend) -> Self {
        load_evolution_dashboard(&backend.data_dir)
    }

    pub fn active_count(&self) -> usize {
        self.jobs
            .iter()
            .filter(|job| matches!(job.phase, EvolutionPhase::Active))
            .count()
    }

    pub fn awaiting_decision_count(&self) -> usize {
        self.jobs
            .iter()
            .filter(|job| job.status_key == "awaiting-approval")
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.jobs
            .iter()
            .filter(|job| matches!(job.phase, EvolutionPhase::Failed))
            .count()
    }

    pub fn promoted_count(&self) -> usize {
        self.jobs
            .iter()
            .filter(|job| job.status_key == "promoted")
            .count()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvolutionPhase {
    Active,
    AwaitingDecision,
    Passed,
    Failed,
    Recovered,
}

impl Default for EvolutionPhase {
    fn default() -> Self {
        Self::Active
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EvolutionJobSummary {
    pub id: String,
    pub short_id: String,
    pub objective: String,
    pub status_key: String,
    pub status_label: String,
    pub phase: EvolutionPhase,
    pub target_ring: String,
    pub target_module: Option<String>,
    pub created_at: String,
    pub workspace_root: String,
    pub candidate_workspace: String,
    pub candidate_branch: String,
    pub source_revision: Option<String>,
    pub verification_outcome: Option<String>,
    pub required_gate_count: usize,
    pub verification_command_count: usize,
    pub module_replay_count: usize,
    pub changed_files: Vec<String>,
    pub allowed_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub recovery_strategy: String,
    pub recovery_note: String,
    pub rollback_policy: String,
    pub bundle_path: String,
    pub mutation_packet_path: String,
    pub diff_path: Option<String>,
    pub scorecard_path: Option<String>,
    pub candidate_summary_path: Option<String>,
    pub decision_path: Option<String>,
    pub artifact_ref_count: usize,
    pub command_log_count: usize,
    pub decision: Option<EvolutionDecisionSummary>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EvolutionDecisionSummary {
    pub outcome: String,
    pub decided_at: String,
    pub decided_by: String,
    pub note: String,
    pub preflight_passed: Option<bool>,
}

#[derive(Clone, Debug, Default)]
pub struct EvolutionStartRequest {
    pub objective: String,
    pub target_ring: String,
    pub intended_paths: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum EvolutionStartEvent {
    Started,
    Finished(Result<(), String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvolutionDecisionAction {
    Approve,
    Reject,
}

impl EvolutionDecisionAction {
    pub fn command(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Reject => "reject",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Approve => "Approve",
            Self::Reject => "Reject",
        }
    }

    pub fn running_label(self) -> &'static str {
        match self {
            Self::Approve => "approving candidate",
            Self::Reject => "rejecting candidate",
        }
    }

    pub fn recorded_label(self) -> &'static str {
        match self {
            Self::Approve => "approval recorded",
            Self::Reject => "rejection recorded",
        }
    }
}

#[derive(Clone, Debug)]
pub struct EvolutionDecisionRequest {
    pub job_id: String,
    pub action: EvolutionDecisionAction,
}

#[derive(Clone, Debug)]
pub enum EvolutionDecisionEvent {
    Started,
    Finished(Result<(), String>),
}

pub fn spawn_evolution_job(request: EvolutionStartRequest) -> Receiver<EvolutionStartEvent> {
    let (tx, rx) = channel();
    let failure_tx = tx.clone();
    if let Err(error) = thread::Builder::new()
        .name("sinomo-app-evolution-start".to_string())
        .spawn(move || {
            let _ = tx.send(EvolutionStartEvent::Started);
            let result = run_evolution_job(request).map_err(|error| error.to_string());
            let _ = tx.send(EvolutionStartEvent::Finished(result));
        })
    {
        let _ = failure_tx.send(EvolutionStartEvent::Finished(Err(format!(
            "failed to start evolution worker: {error}"
        ))));
    }
    rx
}

pub fn spawn_evolution_decision(
    request: EvolutionDecisionRequest,
) -> Receiver<EvolutionDecisionEvent> {
    let (tx, rx) = channel();
    let failure_tx = tx.clone();
    if let Err(error) = thread::Builder::new()
        .name("sinomo-app-evolution-decision".to_string())
        .spawn(move || {
            let _ = tx.send(EvolutionDecisionEvent::Started);
            let result = run_evolution_decision(request).map_err(|error| error.to_string());
            let _ = tx.send(EvolutionDecisionEvent::Finished(result));
        })
    {
        let _ = failure_tx.send(EvolutionDecisionEvent::Finished(Err(format!(
            "failed to start evolution decision worker: {error}"
        ))));
    }
    rx
}

pub fn load_evolution_dashboard(data_dir: &SinomoDataDir) -> EvolutionDashboard {
    let paths = EvolutionPaths::new(data_dir);
    let source = paths.root.display().to_string();
    let mut records = match list_job_records(&paths) {
        Ok(records) => records,
        Err(error) => {
            return EvolutionDashboard {
                source,
                jobs: Vec::new(),
                error: Some(error.to_string()),
            };
        }
    };
    records.sort_by(|a, b| b.bundle.created_at.cmp(&a.bundle.created_at));
    let jobs: Vec<_> = records.into_iter().map(summarize_record).collect();
    EvolutionDashboard {
        source,
        jobs,
        error: None,
    }
}

fn run_evolution_job(request: EvolutionStartRequest) -> Result<(), sinomo_evolution::CliError> {
    let mut args = vec![
        "propose".to_string(),
        request.objective,
        "--target-ring".to_string(),
        request.target_ring,
    ];
    for path in request.intended_paths {
        args.push("--allow".to_string());
        args.push(path);
    }
    sinomo_evolution::run_evolve_command(&args)
}

fn run_evolution_decision(
    request: EvolutionDecisionRequest,
) -> Result<(), sinomo_evolution::CliError> {
    let args = vec![
        request.action.command().to_string(),
        "--job".to_string(),
        request.job_id,
    ];
    sinomo_evolution::run_evolve_command(&args)
}

fn summarize_record(record: StoredEvolutionJob) -> EvolutionJobSummary {
    let StoredEvolutionJob {
        job,
        candidate_workspace,
        bundle,
        verification_outcome,
        changed_files,
        decision,
    } = record;

    let status_key = status_key(&job.status).to_string();
    let status_label = humanize_key(&status_key);
    let phase = phase_for_status(&job.status);
    let id = job.job_id.to_string();
    let short_id = short_id(&id, 12);
    let created_at = bundle
        .created_at
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| bundle.created_at.to_string());

    EvolutionJobSummary {
        id,
        short_id,
        objective: job.objective,
        status_key,
        status_label,
        phase,
        target_ring: format!("{:?}", job.target_ring),
        target_module: job.target_module_id.map(|id| id.to_string()),
        created_at,
        workspace_root: job.edit_scope.workspace_root,
        candidate_workspace: candidate_workspace.path.display().to_string(),
        candidate_branch: candidate_workspace.branch_name,
        source_revision: candidate_workspace.source_revision,
        verification_outcome: verification_outcome
            .as_ref()
            .map(|outcome| humanize_key(&format!("{outcome:?}"))),
        required_gate_count: job.required_gates.len(),
        verification_command_count: job.verification.commands.len(),
        module_replay_count: job.verification.module_replay_reports.len(),
        changed_files,
        allowed_paths: job.scope.allowed_paths,
        forbidden_paths: job.scope.forbidden_paths,
        recovery_strategy: humanize_key(&format!("{:?}", job.recovery.strategy)),
        recovery_note: job.recovery.note,
        rollback_policy: format!("{:?}", job.rollback_policy),
        bundle_path: bundle.bundle_path,
        mutation_packet_path: bundle.mutation_packet_path,
        diff_path: bundle.diff_path,
        scorecard_path: bundle.scorecard_path,
        candidate_summary_path: bundle.candidate_summary_path,
        decision_path: bundle.decision_path,
        artifact_ref_count: bundle.artifact_refs.len(),
        command_log_count: bundle.command_logs.len(),
        decision: decision.map(|decision| EvolutionDecisionSummary {
            outcome: humanize_key(&format!("{:?}", decision.outcome)),
            decided_at: decision
                .decided_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| decision.decided_at.to_string()),
            decided_by: decision.decided_by,
            note: decision.note,
            preflight_passed: decision
                .approval_preflight
                .map(|preflight| preflight.passed),
        }),
    }
}

fn status_key(status: &EvolutionJobStatus) -> &'static str {
    match status {
        EvolutionJobStatus::Created => "created",
        EvolutionJobStatus::PacketWritten => "packet-written",
        EvolutionJobStatus::MutationStarted => "mutation-started",
        EvolutionJobStatus::MutationFinished => "mutation-finished",
        EvolutionJobStatus::Verified => "verified",
        EvolutionJobStatus::AwaitingApproval => "awaiting-approval",
        EvolutionJobStatus::Approved => "approved",
        EvolutionJobStatus::Revised => "revised",
        EvolutionJobStatus::Restored => "restored",
        EvolutionJobStatus::Failed => "failed",
        EvolutionJobStatus::Observed => "observed",
        EvolutionJobStatus::Scoped => "scoped",
        EvolutionJobStatus::Proposed => "proposed",
        EvolutionJobStatus::Implemented => "implemented",
        EvolutionJobStatus::Built => "built",
        EvolutionJobStatus::Evaluated => "evaluated",
        EvolutionJobStatus::PolicyApproved => "policy-approved",
        EvolutionJobStatus::Staged => "staged",
        EvolutionJobStatus::Shadowed => "shadowed",
        EvolutionJobStatus::Canary => "canary",
        EvolutionJobStatus::Promoted => "promoted",
        EvolutionJobStatus::Rejected => "rejected",
        EvolutionJobStatus::BuildFailed => "build-failed",
        EvolutionJobStatus::EvalFailed => "eval-failed",
        EvolutionJobStatus::Denied => "denied",
        EvolutionJobStatus::RolledBack => "rolled-back",
        EvolutionJobStatus::Reverted => "reverted",
    }
}

fn phase_for_status(status: &EvolutionJobStatus) -> EvolutionPhase {
    match status {
        EvolutionJobStatus::AwaitingApproval => EvolutionPhase::AwaitingDecision,
        EvolutionJobStatus::Approved
        | EvolutionJobStatus::PolicyApproved
        | EvolutionJobStatus::Staged
        | EvolutionJobStatus::Shadowed
        | EvolutionJobStatus::Canary
        | EvolutionJobStatus::Promoted => EvolutionPhase::Passed,
        EvolutionJobStatus::Failed
        | EvolutionJobStatus::BuildFailed
        | EvolutionJobStatus::EvalFailed
        | EvolutionJobStatus::Denied
        | EvolutionJobStatus::Rejected => EvolutionPhase::Failed,
        EvolutionJobStatus::Restored
        | EvolutionJobStatus::RolledBack
        | EvolutionJobStatus::Reverted => EvolutionPhase::Recovered,
        _ => EvolutionPhase::Active,
    }
}

fn humanize_key(value: &str) -> String {
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch == '-' || ch == '_' {
            out.push(' ');
        } else if ch.is_uppercase() {
            if index > 0 {
                out.push(' ');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn short_id(id: &str, max: usize) -> String {
    if id.chars().count() <= max {
        id.to_string()
    } else {
        id.chars().take(max).collect()
    }
}
