//! Daemon-owned application projection over evolution persistence and commands.

use sinomo_app_contract::evolution as contract;
use sinomo_runtime::local::SinomoDataDir;

use crate::persistence::{EvolutionPaths, StoredEvolutionJob, list_job_records};
use crate::{CliError, EvolutionJobStatus, run_evolve_command_captured};

pub fn app_evolution_dashboard(data_dir: &SinomoDataDir) -> contract::EvolutionDashboard {
    let paths = EvolutionPaths::new(data_dir);
    let source = paths.root.display().to_string();
    let mut records = match list_job_records(&paths) {
        Ok(records) => records,
        Err(error) => {
            return contract::EvolutionDashboard {
                source,
                jobs: Vec::new(),
                error: Some(error.to_string()),
            };
        }
    };
    records.sort_by_key(|record| std::cmp::Reverse(record.bundle.created_at));
    contract::EvolutionDashboard {
        source,
        jobs: records.into_iter().map(summarize_record).collect(),
        error: None,
    }
}

pub fn run_app_evolution_job(request: contract::EvolutionStartRequest) -> Result<(), CliError> {
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
    run_evolve_command_captured(&args).map(|_| ())
}

pub fn run_app_evolution_decision(
    request: contract::EvolutionDecisionRequest,
) -> Result<(), CliError> {
    run_evolve_command_captured(&[
        request.action.command().to_string(),
        "--job".to_string(),
        request.job_id,
    ])
    .map(|_| ())
}

fn summarize_record(record: StoredEvolutionJob) -> contract::EvolutionJobSummary {
    let StoredEvolutionJob {
        job,
        candidate_workspace,
        bundle,
        verification_outcome,
        changed_files,
        decision,
    } = record;
    let status_key = status_key(&job.status).to_string();
    let id = job.job_id.to_string();
    contract::EvolutionJobSummary {
        short_id: short_id(&id, 12),
        id,
        objective: job.objective,
        status_label: humanize_key(&status_key),
        status_key,
        phase: phase_for_status(&job.status),
        target_ring: format!("{:?}", job.target_ring),
        target_module: job.target_module_id.map(|id| id.to_string()),
        created_at: bundle
            .created_at
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| bundle.created_at.to_string()),
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
        decision: decision.map(|decision| contract::EvolutionDecisionSummary {
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

fn phase_for_status(status: &EvolutionJobStatus) -> contract::EvolutionPhase {
    match status {
        EvolutionJobStatus::AwaitingApproval => contract::EvolutionPhase::AwaitingDecision,
        EvolutionJobStatus::Approved
        | EvolutionJobStatus::PolicyApproved
        | EvolutionJobStatus::Staged
        | EvolutionJobStatus::Shadowed
        | EvolutionJobStatus::Canary
        | EvolutionJobStatus::Promoted => contract::EvolutionPhase::Passed,
        EvolutionJobStatus::Failed
        | EvolutionJobStatus::BuildFailed
        | EvolutionJobStatus::EvalFailed
        | EvolutionJobStatus::Denied
        | EvolutionJobStatus::Rejected => contract::EvolutionPhase::Failed,
        EvolutionJobStatus::Restored
        | EvolutionJobStatus::RolledBack
        | EvolutionJobStatus::Reverted => contract::EvolutionPhase::Recovered,
        _ => contract::EvolutionPhase::Active,
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
