use crate::{
    BackgroundCampaignUpdate, BackgroundSchedulingAction, BackgroundSchedulingPlan,
    CampaignDecision, CampaignDecisionKind, CampaignScheduleDecision,
    CandidateApprovalPreflightReport, CandidateBundleManifest, CandidateDecision,
    CandidateDecisionOutcome, EvolutionCampaign, EvolutionCampaignStatus, EvolutionJob,
    EvolutionJobId, EvolutionJobStatus, EvolutionReport, ResourceSchedulerDryRun,
    ScheduledIterationPlan, StageEvaluationUpdate,
};
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{EvaluationOutcome, EvaluationScorecard};
use sinomo_runtime::eval::CommandRunResult;
use sinomo_runtime::local::SinomoDataDir;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionConfig {
    pub repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoredEvolutionJob {
    pub job: EvolutionJob,
    pub candidate_workspace: crate::CandidateWorkspace,
    pub bundle: CandidateBundleManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_outcome: Option<EvaluationOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<CandidateDecision>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordedCandidateDecision {
    pub record: StoredEvolutionJob,
    pub decision_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordedCandidateVerification {
    pub record: StoredEvolutionJob,
    pub scorecard_path: PathBuf,
    pub command_log_paths: Vec<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvolutionJobSelectionMode {
    Active,
    AwaitingApproval,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoredJobArtifactKind {
    Diff,
    Scorecard,
    Decision,
    MutationPacket,
}

#[derive(Clone, Debug)]
pub struct EvolutionPaths {
    pub data_dir: PathBuf,
    pub root: PathBuf,
    pub jobs: PathBuf,
    pub campaigns: PathBuf,
    pub logs: PathBuf,
    pub config: PathBuf,
}

impl EvolutionPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        let data_dir_root = data_dir.root().to_path_buf();
        let root = data_dir_root.join("evolution");
        Self {
            data_dir: data_dir_root,
            jobs: root.join("jobs"),
            campaigns: root.join("campaigns"),
            logs: root.join("logs"),
            config: root.join("config.json"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), EvolutionPersistenceError> {
        for path in [&self.root, &self.jobs, &self.campaigns, &self.logs] {
            fs::create_dir_all(path).map_err(|source| EvolutionPersistenceError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(())
    }

    pub fn campaign_dir(&self, campaign_id: &str) -> PathBuf {
        self.campaigns.join(campaign_id)
    }

    pub fn campaign_path(&self, campaign_id: &str) -> PathBuf {
        self.campaign_dir(campaign_id).join("campaign.json")
    }

    pub fn job_dir(&self, job_id: &EvolutionJobId) -> PathBuf {
        self.jobs.join(job_id.as_str())
    }

    pub fn job_path(&self, job_id: &EvolutionJobId) -> PathBuf {
        self.job_dir(job_id).join("record.json")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackgroundSchedulerPaths {
    pub root: PathBuf,
    pub plan_json: PathBuf,
    pub status_json: PathBuf,
    pub events_jsonl: PathBuf,
}

impl BackgroundSchedulerPaths {
    pub fn new(paths: &EvolutionPaths) -> Self {
        let root = paths.root.join("background-scheduler");
        Self {
            plan_json: root.join("scheduler-plan.json"),
            status_json: root.join("background-status.json"),
            events_jsonl: root.join("scheduler-events.jsonl"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), EvolutionPersistenceError> {
        fs::create_dir_all(&self.root).map_err(|source| EvolutionPersistenceError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CampaignBundlePaths {
    pub root: PathBuf,
    pub campaign_json: PathBuf,
    pub objective_card_json: PathBuf,
    pub stages_json: PathBuf,
    pub near_term_tasks_json: PathBuf,
    pub resource_schedule_preview_json: PathBuf,
    pub resource_ledger_json: PathBuf,
    pub scheduler_dry_run_json: PathBuf,
    pub lease_requests_jsonl: PathBuf,
    pub lease_decisions_jsonl: PathBuf,
    pub iteration_plan_json: PathBuf,
    pub iteration_request_json: PathBuf,
    pub iteration_result_json: PathBuf,
    pub iteration_results_jsonl: PathBuf,
    pub iteration_dispatches_jsonl: PathBuf,
    pub stage_scorecard_json: PathBuf,
    pub stage_scorecards_jsonl: PathBuf,
    pub adaptation_json: PathBuf,
    pub adaptations_jsonl: PathBuf,
    pub approval_checkpoint_json: PathBuf,
    pub decisions_jsonl: PathBuf,
}

impl CampaignBundlePaths {
    pub fn new(paths: &EvolutionPaths, campaign_id: &str) -> Self {
        let root = paths.campaign_dir(campaign_id);
        Self {
            campaign_json: paths.campaign_path(campaign_id),
            objective_card_json: root.join("objective-card.json"),
            stages_json: root.join("stages.json"),
            near_term_tasks_json: root.join("near-term-tasks.json"),
            resource_schedule_preview_json: root.join("resource-schedule-preview.json"),
            resource_ledger_json: root.join("resource-ledger.json"),
            scheduler_dry_run_json: root.join("scheduler-dry-run.json"),
            lease_requests_jsonl: root.join("lease-requests.jsonl"),
            lease_decisions_jsonl: root.join("lease-decisions.jsonl"),
            iteration_plan_json: root.join("iteration-plan.json"),
            iteration_request_json: root.join("iteration-request.json"),
            iteration_result_json: root.join("iteration-result.json"),
            iteration_results_jsonl: root.join("iteration-results.jsonl"),
            iteration_dispatches_jsonl: root.join("iteration-dispatches.jsonl"),
            stage_scorecard_json: root.join("stage-scorecard.json"),
            stage_scorecards_jsonl: root.join("stage-scorecards.jsonl"),
            adaptation_json: root.join("adaptation.json"),
            adaptations_jsonl: root.join("adaptations.jsonl"),
            approval_checkpoint_json: root.join("approval-checkpoint.json"),
            decisions_jsonl: root.join("decisions.jsonl"),
            root,
        }
    }
}

pub fn write_report_record(
    paths: &EvolutionPaths,
    report: &EvolutionReport,
) -> Result<(), EvolutionPersistenceError> {
    let record = StoredEvolutionJob {
        job: report.job.clone(),
        candidate_workspace: report.candidate_workspace.clone(),
        bundle: report.bundle.clone(),
        verification_outcome: report
            .scorecard
            .as_ref()
            .map(|scorecard| scorecard.outcome.clone()),
        changed_files: report.changed_files.clone(),
        decision: None,
    };
    write_stored_job(paths, &record)?;
    if let Some(job_path) = &record.bundle.job_path {
        write_json(Path::new(job_path), &record.job)?;
    }
    Ok(())
}

pub fn write_bundle_manifest(record: &StoredEvolutionJob) -> Result<(), EvolutionPersistenceError> {
    write_json(
        &PathBuf::from(&record.bundle.bundle_path).join("manifest.json"),
        &record.bundle,
    )
}

pub fn write_stored_job(
    paths: &EvolutionPaths,
    record: &StoredEvolutionJob,
) -> Result<(), EvolutionPersistenceError> {
    write_json(&paths.job_path(&record.job.job_id), record)
}

pub fn record_candidate_decision(
    paths: &EvolutionPaths,
    record: StoredEvolutionJob,
    outcome: CandidateDecisionOutcome,
    decided_by: impl Into<String>,
    note: impl Into<String>,
) -> Result<RecordedCandidateDecision, EvolutionPersistenceError> {
    record_candidate_decision_with_preflight(paths, record, outcome, decided_by, note, None)
}

pub fn record_candidate_decision_with_preflight(
    paths: &EvolutionPaths,
    mut record: StoredEvolutionJob,
    outcome: CandidateDecisionOutcome,
    decided_by: impl Into<String>,
    note: impl Into<String>,
    approval_preflight: Option<CandidateApprovalPreflightReport>,
) -> Result<RecordedCandidateDecision, EvolutionPersistenceError> {
    let mut decision = CandidateDecision::new(
        &record.job,
        outcome,
        decided_by,
        record.candidate_workspace.path.display().to_string(),
        record.bundle.bundle_path.clone(),
        record.verification_outcome.clone(),
        record.changed_files.clone(),
        note,
    );
    decision.approval_preflight = approval_preflight;
    let decision_path = PathBuf::from(&record.bundle.bundle_path).join("approval-decision.json");
    write_json(&decision_path, &decision)?;
    record.job.status = decision.resulting_status.clone();
    record.bundle.decision_path = Some(decision_path.display().to_string());
    record.decision = Some(decision);
    write_bundle_manifest(&record)?;
    if let Some(job_path) = &record.bundle.job_path {
        write_json(Path::new(job_path), &record.job)?;
    }
    write_stored_job(paths, &record)?;
    Ok(RecordedCandidateDecision {
        record,
        decision_path,
    })
}

pub fn record_candidate_verification(
    paths: &EvolutionPaths,
    mut record: StoredEvolutionJob,
    scorecard: &EvaluationScorecard,
    command_results: &[CommandRunResult],
) -> Result<RecordedCandidateVerification, EvolutionPersistenceError> {
    let scorecard_path = PathBuf::from(&record.bundle.bundle_path).join("scorecard.json");
    write_json(&scorecard_path, scorecard)?;
    let mut command_log_paths = Vec::new();
    for result in command_results {
        let safe_name = safe_file_stem(&result.name);
        let path = PathBuf::from(&record.bundle.bundle_path)
            .join("command-logs")
            .join(format!("{safe_name}.txt"));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| EvolutionPersistenceError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(
            &path,
            format!(
                "name: {}\ncommand: {}\nexpected_exit_code: {}\nexit_code: {}\nduration_ms: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
                result.name,
                result.command,
                result.expected_exit_code,
                result.exit_code,
                result.duration_ms,
                result.stdout,
                result.stderr
            ),
        )
        .map_err(|source| EvolutionPersistenceError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        command_log_paths.push(path);
    }
    record.verification_outcome = Some(scorecard.outcome.clone());
    record.bundle.scorecard_path = Some(scorecard_path.display().to_string());
    write_stored_job(paths, &record)?;
    Ok(RecordedCandidateVerification {
        record,
        scorecard_path,
        command_log_paths,
    })
}

pub fn read_stored_job(path: &Path) -> Result<StoredEvolutionJob, EvolutionPersistenceError> {
    read_json_file(path)
}

pub fn select_stored_job(
    paths: &EvolutionPaths,
    job_id: Option<&EvolutionJobId>,
    mode: EvolutionJobSelectionMode,
) -> Result<StoredEvolutionJob, EvolutionJobSelectionError> {
    if let Some(job_id) = job_id {
        return read_stored_job(&paths.job_path(job_id)).map_err(Into::into);
    }

    let records = list_job_records(paths)?;
    let candidates: Vec<_> = records
        .into_iter()
        .filter(|record| match mode {
            EvolutionJobSelectionMode::Active => !is_terminal_status(&record.job.status),
            EvolutionJobSelectionMode::AwaitingApproval => {
                record.job.status == EvolutionJobStatus::AwaitingApproval
            }
        })
        .collect();
    match candidates.as_slice() {
        [record] => Ok(record.clone()),
        [] => Err(EvolutionJobSelectionError::NoActiveJob),
        _ => Err(EvolutionJobSelectionError::AmbiguousJob {
            job_ids: candidates
                .iter()
                .map(|record| record.job.job_id.to_string())
                .collect(),
        }),
    }
}

pub fn ensure_stored_job_awaiting_approval(
    record: &StoredEvolutionJob,
) -> Result<(), EvolutionJobStateError> {
    if record.job.status == EvolutionJobStatus::AwaitingApproval {
        return Ok(());
    }
    Err(EvolutionJobStateError::UnexpectedStatus {
        job_id: record.job.job_id.to_string(),
        status: job_status_label(&record.job.status).to_string(),
        expected: job_status_label(&EvolutionJobStatus::AwaitingApproval).to_string(),
    })
}

pub fn job_status_label(status: &EvolutionJobStatus) -> &'static str {
    match status {
        EvolutionJobStatus::AwaitingApproval => "awaiting-approval",
        EvolutionJobStatus::Approved => "approved",
        EvolutionJobStatus::Rejected => "rejected",
        EvolutionJobStatus::Created => "created",
        EvolutionJobStatus::Failed => "failed",
        _ => "in-progress",
    }
}

pub fn evolution_report_from_record(
    record: StoredEvolutionJob,
) -> Result<EvolutionReport, EvolutionPersistenceError> {
    let scorecard = if let Some(scorecard_path) = &record.bundle.scorecard_path {
        let path = PathBuf::from(scorecard_path);
        if path.exists() {
            Some(read_json_file::<EvaluationScorecard>(&path)?)
        } else {
            None
        }
    } else {
        None
    };
    Ok(EvolutionReport {
        job: record.job,
        candidate_workspace: record.candidate_workspace,
        bundle: record.bundle,
        scorecard,
        changed_files: record.changed_files,
    })
}

pub fn stored_job_artifact_path(
    record: &StoredEvolutionJob,
    artifact: StoredJobArtifactKind,
) -> Result<PathBuf, EvolutionPersistenceError> {
    let path = match artifact {
        StoredJobArtifactKind::Diff => record.bundle.diff_path.clone(),
        StoredJobArtifactKind::Scorecard => record.bundle.scorecard_path.clone(),
        StoredJobArtifactKind::Decision => record.bundle.decision_path.clone(),
        StoredJobArtifactKind::MutationPacket => Some(record.bundle.mutation_packet_path.clone()),
    }
    .ok_or_else(|| EvolutionPersistenceError::MissingArtifact {
        artifact: format!("{artifact:?}"),
    })?;
    Ok(PathBuf::from(path))
}

pub fn read_stored_job_artifact(
    record: &StoredEvolutionJob,
    artifact: StoredJobArtifactKind,
) -> Result<String, EvolutionPersistenceError> {
    let path = stored_job_artifact_path(record, artifact)?;
    fs::read_to_string(&path).map_err(|source| EvolutionPersistenceError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn read_json_file<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<T, EvolutionPersistenceError> {
    let content = fs::read_to_string(path).map_err(|source| EvolutionPersistenceError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| EvolutionPersistenceError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn list_job_records(
    paths: &EvolutionPaths,
) -> Result<Vec<StoredEvolutionJob>, EvolutionPersistenceError> {
    if !paths.jobs.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in fs::read_dir(&paths.jobs).map_err(|source| EvolutionPersistenceError::Io {
        path: paths.jobs.display().to_string(),
        detail: source.to_string(),
    })? {
        let entry = entry.map_err(|source| EvolutionPersistenceError::Io {
            path: paths.jobs.display().to_string(),
            detail: source.to_string(),
        })?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            let record_path = entry_path.join("record.json");
            if record_path.exists() {
                records.push(read_stored_job(&record_path)?);
            }
        } else if entry_path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            records.push(read_stored_job(&entry_path)?);
        }
    }
    Ok(records)
}

pub fn read_config(paths: &EvolutionPaths) -> Result<EvolutionConfig, EvolutionPersistenceError> {
    read_json_file(&paths.config)
}

pub fn write_config(
    paths: &EvolutionPaths,
    config: &EvolutionConfig,
) -> Result<(), EvolutionPersistenceError> {
    write_json(&paths.config, config)
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), EvolutionPersistenceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| EvolutionPersistenceError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content =
        serde_json::to_string_pretty(value).map_err(|source| EvolutionPersistenceError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    fs::write(path, content).map_err(|source| EvolutionPersistenceError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn write_campaign_bundle(
    paths: &EvolutionPaths,
    campaign: &EvolutionCampaign,
) -> Result<CampaignBundlePaths, EvolutionPersistenceError> {
    let bundle = CampaignBundlePaths::new(paths, &campaign.campaign_id);
    fs::create_dir_all(&bundle.root).map_err(|source| EvolutionPersistenceError::Io {
        path: bundle.root.display().to_string(),
        detail: source.to_string(),
    })?;
    write_json(&bundle.campaign_json, campaign)?;
    write_json(&bundle.objective_card_json, &campaign.objective_card)?;
    write_json(&bundle.stages_json, &campaign.stages)?;
    write_json(&bundle.near_term_tasks_json, &campaign.near_term_tasks)?;
    write_json(
        &bundle.resource_schedule_preview_json,
        &campaign.resource_schedule_preview,
    )?;
    write_json(
        &bundle.approval_checkpoint_json,
        &campaign.approval_checkpoint,
    )?;
    write_jsonl(&bundle.decisions_jsonl, &campaign.decisions)?;
    Ok(bundle)
}

pub fn read_campaign(
    paths: &EvolutionPaths,
    campaign_id: &str,
) -> Result<EvolutionCampaign, EvolutionPersistenceError> {
    read_json_file(&paths.campaign_path(campaign_id))
}

pub fn write_campaign_schedule_dry_run(
    paths: &EvolutionPaths,
    campaign: &mut EvolutionCampaign,
    dry_run: &ResourceSchedulerDryRun,
) -> Result<CampaignBundlePaths, EvolutionPersistenceError> {
    let previous_status = campaign.status.clone();
    campaign.status = match dry_run.aggregate_decision {
        CampaignScheduleDecision::PausedBudget => EvolutionCampaignStatus::PausedBudget,
        CampaignScheduleDecision::Delayed => EvolutionCampaignStatus::Scheduled,
        CampaignScheduleDecision::Scheduled => EvolutionCampaignStatus::Scheduled,
    };
    campaign.updated_at = dry_run.generated_at.clone();
    let decision_kind = if dry_run.aggregate_decision == CampaignScheduleDecision::PausedBudget {
        CampaignDecisionKind::Paused
    } else if matches!(previous_status, EvolutionCampaignStatus::PausedBudget) {
        CampaignDecisionKind::Resumed
    } else {
        CampaignDecisionKind::ResourcePolicySelected
    };
    campaign.decisions.push(CampaignDecision {
        decision_id: format!("decision_{}", campaign.decisions.len() + 1),
        kind: decision_kind,
        summary: match dry_run.aggregate_decision {
            CampaignScheduleDecision::PausedBudget => {
                "Paused because one or more resource leases would violate reserve floors"
                    .to_string()
            }
            CampaignScheduleDecision::Delayed => {
                "Delayed because the scheduler had no runnable lease requests".to_string()
            }
            CampaignScheduleDecision::Scheduled => {
                "Scheduled because all previewed resource leases preserve reserve floors"
                    .to_string()
            }
        },
        rationale:
            "Phase 1 resource scheduler dry run gates dispatch before any agent work starts."
                .to_string(),
        evidence_refs: vec![
            "scheduler-dry-run.json".to_string(),
            "resource-ledger.json".to_string(),
            "lease-decisions.jsonl".to_string(),
        ],
        decided_by: "sinomo evolve schedule --dry-run".to_string(),
        decided_at: dry_run.generated_at.clone(),
    });

    let bundle = write_campaign_bundle(paths, campaign)?;
    write_json(&bundle.resource_ledger_json, &dry_run.resource_ledger)?;
    write_json(&bundle.scheduler_dry_run_json, dry_run)?;
    write_jsonl(&bundle.lease_requests_jsonl, &dry_run.lease_requests)?;
    write_jsonl(&bundle.lease_decisions_jsonl, &dry_run.lease_decisions)?;
    Ok(bundle)
}

pub fn write_campaign_iteration_dry_run(
    paths: &EvolutionPaths,
    campaign: &mut EvolutionCampaign,
    plan: &ScheduledIterationPlan,
) -> Result<CampaignBundlePaths, EvolutionPersistenceError> {
    let now = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    campaign.status = EvolutionCampaignStatus::Scheduled;
    campaign.updated_at = now.clone();
    campaign.decisions.push(CampaignDecision {
        decision_id: format!("decision_{}", campaign.decisions.len() + 1),
        kind: CampaignDecisionKind::TaskSelected,
        summary: format!(
            "Prepared dry-run scheduled iteration {} for task {}",
            plan.iteration_id, plan.task.task_id
        ),
        rationale:
            "Phase 2 strategy-plan execution writes an inspectable handoff before any agent work starts."
                .to_string(),
        evidence_refs: vec![
            "iteration-plan.json".to_string(),
            "iteration-request.json".to_string(),
            "iteration-dispatches.jsonl".to_string(),
        ],
        decided_by: "sinomo evolve iterate --dry-run".to_string(),
        decided_at: now,
    });

    let bundle = write_campaign_bundle(paths, campaign)?;
    write_json(&bundle.iteration_plan_json, plan)?;
    write_json(&bundle.iteration_request_json, &plan.evolution_request)?;
    write_jsonl(
        &bundle.iteration_dispatches_jsonl,
        std::slice::from_ref(plan),
    )?;
    Ok(bundle)
}

pub fn write_campaign_iteration_evaluation(
    paths: &EvolutionPaths,
    plan: &ScheduledIterationPlan,
    update: &StageEvaluationUpdate,
) -> Result<CampaignBundlePaths, EvolutionPersistenceError> {
    let bundle = write_campaign_stage_evaluation(paths, update)?;
    write_json(&bundle.iteration_plan_json, plan)?;
    write_json(&bundle.iteration_request_json, &plan.evolution_request)?;
    write_jsonl(
        &bundle.iteration_dispatches_jsonl,
        std::slice::from_ref(plan),
    )?;
    if let Some(result) = update.campaign.iteration_results.last() {
        write_json(&bundle.iteration_result_json, result)?;
        append_jsonl(&bundle.iteration_results_jsonl, result)?;
    }
    Ok(bundle)
}

pub fn write_campaign_stage_evaluation(
    paths: &EvolutionPaths,
    update: &StageEvaluationUpdate,
) -> Result<CampaignBundlePaths, EvolutionPersistenceError> {
    let bundle = write_campaign_bundle(paths, &update.campaign)?;
    write_json(&bundle.stage_scorecard_json, &update.stage_scorecard)?;
    append_jsonl(&bundle.stage_scorecards_jsonl, &update.stage_scorecard)?;
    if let Some(adaptation) = &update.adaptation {
        write_json(&bundle.adaptation_json, adaptation)?;
        append_jsonl(&bundle.adaptations_jsonl, adaptation)?;
    }
    Ok(bundle)
}

pub fn write_background_scheduling_plan(
    paths: &EvolutionPaths,
    plan: &BackgroundSchedulingPlan,
) -> Result<BackgroundSchedulerPaths, EvolutionPersistenceError> {
    let background_paths = BackgroundSchedulerPaths::new(paths);
    background_paths.ensure()?;
    write_json(&background_paths.plan_json, plan)?;
    write_json(&background_paths.status_json, plan)?;
    for event in &plan.timer_events {
        append_jsonl(&background_paths.events_jsonl, event)?;
    }
    for update in &plan.campaigns {
        let mut campaign = update.campaign.clone();
        campaign.updated_at = plan.generated_at.clone();
        campaign
            .decisions
            .push(background_campaign_decision(plan, update));
        let bundle = write_campaign_bundle(paths, &campaign)?;
        if let Some(iteration) = &update.scheduled_iteration {
            write_json(&bundle.iteration_plan_json, iteration)?;
            write_json(&bundle.iteration_request_json, &iteration.evolution_request)?;
            append_jsonl(&bundle.iteration_dispatches_jsonl, iteration)?;
        }
    }
    Ok(background_paths)
}

pub fn background_campaign_decision(
    plan: &BackgroundSchedulingPlan,
    update: &BackgroundCampaignUpdate,
) -> CampaignDecision {
    let kind = match update.action {
        BackgroundSchedulingAction::PauseForBudget => CampaignDecisionKind::Paused,
        BackgroundSchedulingAction::DispatchIterationDryRun
            if matches!(
                update.previous_status,
                EvolutionCampaignStatus::PausedBudget
            ) =>
        {
            CampaignDecisionKind::Resumed
        }
        BackgroundSchedulingAction::DispatchIterationDryRun => CampaignDecisionKind::TaskSelected,
        BackgroundSchedulingAction::HoldHumanCheckpoint => CampaignDecisionKind::Paused,
        _ => CampaignDecisionKind::ResourcePolicySelected,
    };
    CampaignDecision {
        decision_id: format!("decision_{}", update.campaign.decisions.len() + 1),
        kind,
        summary: format!("Background scheduler action: {:?}", update.action),
        rationale: update.reason.clone(),
        evidence_refs: vec![
            "background-scheduler/scheduler-plan.json".to_string(),
            "background-scheduler/scheduler-events.jsonl".to_string(),
        ],
        decided_by: "sinomo evolve tick --dry-run".to_string(),
        decided_at: plan.generated_at.clone(),
    }
}

pub fn read_background_tick_campaigns(
    paths: &EvolutionPaths,
    campaign_ids: &[String],
) -> Result<Vec<EvolutionCampaign>, EvolutionPersistenceError> {
    if !campaign_ids.is_empty() {
        return campaign_ids
            .iter()
            .map(|campaign_id| read_campaign(paths, campaign_id))
            .collect();
    }
    let mut campaigns = Vec::new();
    if !paths.campaigns.exists() {
        return Ok(campaigns);
    }
    let entries =
        fs::read_dir(&paths.campaigns).map_err(|source| EvolutionPersistenceError::Io {
            path: paths.campaigns.display().to_string(),
            detail: source.to_string(),
        })?;
    for entry in entries {
        let entry = entry.map_err(|source| EvolutionPersistenceError::Io {
            path: paths.campaigns.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = entry.path().join("campaign.json");
        if path.exists() {
            campaigns.push(read_json_file(&path)?);
        }
    }
    campaigns.sort_by(|left: &EvolutionCampaign, right: &EvolutionCampaign| {
        left.campaign_id.cmp(&right.campaign_id)
    });
    Ok(campaigns)
}

pub fn append_jsonl<T: Serialize>(path: &Path, value: &T) -> Result<(), EvolutionPersistenceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| EvolutionPersistenceError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| EvolutionPersistenceError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    let line = serde_json::to_string(value).map_err(|source| EvolutionPersistenceError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    writeln!(file, "{line}").map_err(|source| EvolutionPersistenceError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn write_jsonl<T: Serialize>(
    path: &Path,
    values: &[T],
) -> Result<(), EvolutionPersistenceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| EvolutionPersistenceError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut lines = String::new();
    for value in values {
        lines.push_str(&serde_json::to_string(value).map_err(|source| {
            EvolutionPersistenceError::Json {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?);
        lines.push('\n');
    }
    fs::write(path, lines).map_err(|source| EvolutionPersistenceError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn is_terminal_status(status: &EvolutionJobStatus) -> bool {
    matches!(
        status,
        EvolutionJobStatus::Approved
            | EvolutionJobStatus::Rejected
            | EvolutionJobStatus::Restored
            | EvolutionJobStatus::Failed
    )
}

pub fn decision_outcome_label(outcome: &CandidateDecisionOutcome) -> &'static str {
    match outcome {
        CandidateDecisionOutcome::Approved => "approved",
        CandidateDecisionOutcome::Rejected => "rejected",
        CandidateDecisionOutcome::Restored => "restored",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CandidateWorkspace, EditScope, EvolutionScope, RecoveryPlan, VerificationPlan, WorkspaceId,
    };
    use sinomo_runtime::core::{EvaluationGate, Id, RollbackPolicy, TrustRing};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    type ScorecardId = Id;

    #[test]
    fn record_candidate_decision_persists_decision_manifest_job_and_record() {
        let root = temp_dir("record-decision");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let job_id = EvolutionJobId::generate();
        let bundle_dir = root.join("bundle");
        let job_artifact = bundle_dir.join("job.json");
        let record = StoredEvolutionJob {
            job: EvolutionJob {
                job_id: job_id.clone(),
                objective: "record a candidate decision".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                target_module_id: None,
                edit_scope: EditScope {
                    workspace_root: root.display().to_string(),
                    intended_paths: vec!["README.md".to_string()],
                    notes: String::new(),
                },
                verification: VerificationPlan {
                    commands: Vec::new(),
                    module_replay_reports: Vec::new(),
                },
                recovery: RecoveryPlan::default(),
                scope: EvolutionScope {
                    allowed_paths: vec!["README.md".to_string()],
                    forbidden_paths: Vec::new(),
                    workspace_id: Some(
                        WorkspaceId::from_legacy_named("workspace_record_decision")
                            .expect("workspace"),
                    ),
                },
                status: EvolutionJobStatus::AwaitingApproval,
                required_gates: vec![EvaluationGate::StaticChecks],
                rollback_policy: RollbackPolicy::default(),
            },
            candidate_workspace: CandidateWorkspace {
                job_id: job_id.clone(),
                path: root.join("worktree"),
                branch_name: "codex/evolve/record-decision".to_string(),
                source_revision: Some("abc123".to_string()),
            },
            bundle: CandidateBundleManifest {
                job_id: job_id.clone(),
                bundle_path: bundle_dir.display().to_string(),
                created_at: OffsetDateTime::now_utc(),
                mutation_packet_path: bundle_dir
                    .join("mutation-packet.json")
                    .display()
                    .to_string(),
                job_path: Some(job_artifact.display().to_string()),
                model_output_path: None,
                agent_run_path: None,
                agent_summary_path: None,
                benchmark_results_path: None,
                resource_usage_path: None,
                content_manifest_path: None,
                attestation_path: None,
                registry_record_path: None,
                bundle_content_hash: None,
                changed_files_path: None,
                diff_path: None,
                scorecard_path: None,
                candidate_summary_path: None,
                decision_path: None,
                command_logs: Vec::new(),
            },
            verification_outcome: Some(EvaluationOutcome::Passed),
            changed_files: vec!["README.md".to_string()],
            decision: None,
        };

        let recorded = record_candidate_decision(
            &paths,
            record,
            CandidateDecisionOutcome::Rejected,
            "test-reviewer",
            "needs another pass",
        )
        .expect("decision");

        assert_eq!(recorded.record.job.status, EvolutionJobStatus::Rejected);
        assert_eq!(
            recorded.record.decision.as_ref().unwrap().note,
            "needs another pass"
        );
        assert!(recorded.decision_path.exists());
        let manifest: CandidateBundleManifest =
            read_json_file(&bundle_dir.join("manifest.json")).expect("manifest");
        assert_eq!(
            manifest.decision_path,
            Some(recorded.decision_path.display().to_string())
        );
        let job_artifact: EvolutionJob = read_json_file(&job_artifact).expect("job artifact");
        assert_eq!(job_artifact.status, EvolutionJobStatus::Rejected);
        let stored = read_stored_job(&paths.job_path(&job_id)).expect("stored job");
        assert_eq!(stored.job.status, EvolutionJobStatus::Rejected);
        assert_eq!(stored.decision.unwrap().decided_by, "test-reviewer");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn record_candidate_verification_persists_scorecard_logs_and_record() {
        let root = temp_dir("record-verification");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let job_id = EvolutionJobId::generate();
        let bundle_dir = root.join("bundle");
        let record = StoredEvolutionJob {
            job: EvolutionJob {
                job_id: job_id.clone(),
                objective: "record verification".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                target_module_id: None,
                edit_scope: EditScope {
                    workspace_root: root.display().to_string(),
                    intended_paths: vec!["README.md".to_string()],
                    notes: String::new(),
                },
                verification: VerificationPlan {
                    commands: Vec::new(),
                    module_replay_reports: Vec::new(),
                },
                recovery: RecoveryPlan::default(),
                scope: EvolutionScope {
                    allowed_paths: vec!["README.md".to_string()],
                    forbidden_paths: Vec::new(),
                    workspace_id: Some(
                        WorkspaceId::from_legacy_named("workspace_record_verification")
                            .expect("workspace"),
                    ),
                },
                status: EvolutionJobStatus::AwaitingApproval,
                required_gates: vec![EvaluationGate::StaticChecks],
                rollback_policy: RollbackPolicy::default(),
            },
            candidate_workspace: CandidateWorkspace {
                job_id: job_id.clone(),
                path: root.join("worktree"),
                branch_name: "codex/evolve/record-verification".to_string(),
                source_revision: Some("abc123".to_string()),
            },
            bundle: CandidateBundleManifest {
                job_id: job_id.clone(),
                bundle_path: bundle_dir.display().to_string(),
                created_at: OffsetDateTime::now_utc(),
                mutation_packet_path: bundle_dir
                    .join("mutation-packet.json")
                    .display()
                    .to_string(),
                job_path: None,
                model_output_path: None,
                agent_run_path: None,
                agent_summary_path: None,
                benchmark_results_path: None,
                resource_usage_path: None,
                content_manifest_path: None,
                attestation_path: None,
                registry_record_path: None,
                bundle_content_hash: None,
                changed_files_path: None,
                diff_path: None,
                scorecard_path: None,
                candidate_summary_path: None,
                decision_path: None,
                command_logs: Vec::new(),
            },
            verification_outcome: None,
            changed_files: Vec::new(),
            decision: None,
        };
        let scorecard = EvaluationScorecard {
            scorecard_id: ScorecardId::generate(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
            outcome: EvaluationOutcome::Passed,
            gates: Vec::new(),
            metrics: Vec::new(),
        };
        let results = vec![CommandRunResult {
            run_id: None,
            name: "cargo test".to_string(),
            command: "cargo test -p sinomo-evolution".to_string(),
            expected_exit_code: 0,
            exit_code: 0,
            stdout: "ok".to_string(),
            stderr: String::new(),
            duration_ms: 12,
        }];

        let recorded =
            record_candidate_verification(&paths, record, &scorecard, &results).expect("record");

        assert_eq!(
            recorded.record.verification_outcome,
            Some(EvaluationOutcome::Passed)
        );
        assert_eq!(recorded.scorecard_path, bundle_dir.join("scorecard.json"));
        assert!(recorded.scorecard_path.exists());
        assert_eq!(recorded.command_log_paths.len(), 1);
        let log = fs::read_to_string(&recorded.command_log_paths[0]).expect("command log");
        assert!(log.contains("stdout:\nok"));
        let stored = read_stored_job(&paths.job_path(&job_id)).expect("stored job");
        assert_eq!(
            stored.bundle.scorecard_path,
            Some(recorded.scorecard_path.display().to_string())
        );
        assert_eq!(stored.verification_outcome, Some(EvaluationOutcome::Passed));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn read_stored_job_artifact_reads_selected_bundle_artifact() {
        let root = temp_dir("read-artifact");
        let job_id = EvolutionJobId::generate();
        let bundle_dir = root.join("bundle");
        fs::create_dir_all(&bundle_dir).expect("bundle");
        let diff = bundle_dir.join("diff.patch");
        let packet = bundle_dir.join("mutation-packet.json");
        fs::write(&diff, "diff-content").expect("diff");
        fs::write(&packet, "packet-content").expect("packet");
        let record = StoredEvolutionJob {
            job: EvolutionJob {
                job_id: job_id.clone(),
                objective: "read artifact".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                target_module_id: None,
                edit_scope: EditScope {
                    workspace_root: root.display().to_string(),
                    intended_paths: vec!["README.md".to_string()],
                    notes: String::new(),
                },
                verification: VerificationPlan {
                    commands: Vec::new(),
                    module_replay_reports: Vec::new(),
                },
                recovery: RecoveryPlan::default(),
                scope: EvolutionScope {
                    allowed_paths: vec!["README.md".to_string()],
                    forbidden_paths: Vec::new(),
                    workspace_id: None,
                },
                status: EvolutionJobStatus::AwaitingApproval,
                required_gates: vec![EvaluationGate::StaticChecks],
                rollback_policy: RollbackPolicy::default(),
            },
            candidate_workspace: CandidateWorkspace {
                job_id: job_id.clone(),
                path: root.join("worktree"),
                branch_name: "codex/evolve/read-artifact".to_string(),
                source_revision: None,
            },
            bundle: CandidateBundleManifest {
                job_id,
                bundle_path: bundle_dir.display().to_string(),
                created_at: OffsetDateTime::now_utc(),
                mutation_packet_path: packet.display().to_string(),
                job_path: None,
                model_output_path: None,
                agent_run_path: None,
                agent_summary_path: None,
                benchmark_results_path: None,
                resource_usage_path: None,
                content_manifest_path: None,
                attestation_path: None,
                registry_record_path: None,
                bundle_content_hash: None,
                changed_files_path: None,
                diff_path: Some(diff.display().to_string()),
                scorecard_path: None,
                candidate_summary_path: None,
                decision_path: None,
                command_logs: Vec::new(),
            },
            verification_outcome: None,
            changed_files: Vec::new(),
            decision: None,
        };

        assert_eq!(
            read_stored_job_artifact(&record, StoredJobArtifactKind::Diff).expect("diff"),
            "diff-content"
        );
        assert_eq!(
            read_stored_job_artifact(&record, StoredJobArtifactKind::MutationPacket)
                .expect("packet"),
            "packet-content"
        );
        assert!(matches!(
            stored_job_artifact_path(&record, StoredJobArtifactKind::Scorecard),
            Err(EvolutionPersistenceError::MissingArtifact { .. })
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn select_stored_job_filters_active_jobs_and_honors_explicit_job_id() {
        let root = temp_dir("select-job");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let active = stored_record_fixture(&root, EvolutionJobStatus::AwaitingApproval);
        let terminal = stored_record_fixture(&root, EvolutionJobStatus::Approved);
        write_stored_job(&paths, &active).expect("active");
        write_stored_job(&paths, &terminal).expect("terminal");

        let selected =
            select_stored_job(&paths, None, EvolutionJobSelectionMode::Active).expect("selected");
        assert_eq!(selected.job.job_id, active.job.job_id);

        let explicit = select_stored_job(
            &paths,
            Some(&terminal.job.job_id),
            EvolutionJobSelectionMode::Active,
        )
        .expect("explicit");
        assert_eq!(explicit.job.job_id, terminal.job.job_id);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_stored_job_awaiting_approval_reports_status_mismatch() {
        let root = temp_dir("job-state");
        let approved = stored_record_fixture(&root, EvolutionJobStatus::Approved);
        assert!(
            ensure_stored_job_awaiting_approval(&stored_record_fixture(
                &root,
                EvolutionJobStatus::AwaitingApproval
            ))
            .is_ok()
        );
        let error = ensure_stored_job_awaiting_approval(&approved).expect_err("status mismatch");
        assert!(matches!(
            error,
            EvolutionJobStateError::UnexpectedStatus {
                status,
                expected,
                ..
            } if status == "approved" && expected == "awaiting-approval"
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_campaign_iteration_evaluation_persists_iteration_and_stage_artifacts() {
        let root = temp_dir("campaign-iteration-evaluation");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let mut campaign = crate::propose_evolution_campaign(crate::CampaignProposalRequest {
            objective: "connect campaign iteration to candidate evaluation".to_string(),
            horizon: "14 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: vec![crate::ReserveFloor {
                resource: "coding-plan-weekly".to_string(),
                floor_percent: 30,
            }],
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: false,
            dry_run: true,
            approved: true,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some(root.display().to_string()),
        });
        campaign.status = crate::EvolutionCampaignStatus::Scheduled;
        let task_id = campaign.near_term_tasks[0].task_id.clone();
        let plan = crate::prepare_scheduled_iteration(crate::ScheduledIterationRequest {
            campaign: campaign.clone(),
            task_id: Some(task_id.clone()),
            intended_paths: vec!["crates/sinomo-evolution/**".to_string()],
            extra_verification_commands: vec!["cargo test -p sinomo-evolution".to_string()],
            dry_run: false,
        })
        .expect("iteration plan");
        let mut record = stored_record_fixture(&root, EvolutionJobStatus::AwaitingApproval);
        record.job.objective = plan.evolution_request.objective.clone();
        record.job.target_ring = campaign.target_ring.clone();
        record.changed_files = vec!["crates/sinomo-evolution/src/engine.rs".to_string()];
        record.bundle.scorecard_path = Some(
            root.join("bundle")
                .join("scorecard.json")
                .display()
                .to_string(),
        );
        record.bundle.changed_files_path = Some(
            root.join("bundle")
                .join("changed-files.json")
                .display()
                .to_string(),
        );
        let report = EvolutionReport {
            job: record.job,
            candidate_workspace: record.candidate_workspace,
            bundle: record.bundle,
            scorecard: Some(EvaluationScorecard {
                scorecard_id: ScorecardId::generate(),
                baseline_artifact_id: None,
                candidate_artifact_id: None,
                outcome: EvaluationOutcome::Passed,
                gates: Vec::new(),
                metrics: Vec::new(),
            }),
            changed_files: record.changed_files,
        };
        let update = crate::evaluate_campaign_stage(crate::StageEvaluationRequest {
            campaign,
            task_id: task_id.clone(),
            report,
            resource_usage: Vec::new(),
            failure_class: None,
            root_cause_summary: None,
            what_was_learned: Vec::new(),
        });

        let bundle = write_campaign_iteration_evaluation(&paths, &plan, &update)
            .expect("iteration evaluation artifacts");

        assert!(bundle.iteration_plan_json.exists());
        assert!(bundle.iteration_request_json.exists());
        assert!(bundle.iteration_result_json.exists());
        assert!(bundle.iteration_results_jsonl.exists());
        assert!(bundle.stage_scorecard_json.exists());
        let result: crate::EvolutionIterationResult =
            read_json_file(&bundle.iteration_result_json).expect("iteration result");
        assert_eq!(result.task_id, task_id);
        assert_eq!(
            fs::read_to_string(&bundle.iteration_results_jsonl)
                .expect("iteration results jsonl")
                .lines()
                .count(),
            1
        );
        let stored: EvolutionCampaign =
            read_json_file(&bundle.campaign_json).expect("campaign json");
        assert_eq!(stored.iteration_results.len(), 1);
        assert!(
            fs::read_to_string(&bundle.stage_scorecards_jsonl)
                .expect("stage scorecards jsonl")
                .contains(&stored.campaign_id)
        );
        let _ = fs::remove_dir_all(root);
    }

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-evolution-persistence-{name}-{stamp}"))
    }

    fn stored_record_fixture(root: &Path, status: EvolutionJobStatus) -> StoredEvolutionJob {
        let job_id = EvolutionJobId::generate();
        StoredEvolutionJob {
            job: EvolutionJob {
                job_id: job_id.clone(),
                objective: format!("fixture {}", job_id),
                target_ring: TrustRing::R1TrustedRuntime,
                target_module_id: None,
                edit_scope: EditScope {
                    workspace_root: root.display().to_string(),
                    intended_paths: vec!["README.md".to_string()],
                    notes: String::new(),
                },
                verification: VerificationPlan {
                    commands: Vec::new(),
                    module_replay_reports: Vec::new(),
                },
                recovery: RecoveryPlan::default(),
                scope: EvolutionScope {
                    allowed_paths: vec!["README.md".to_string()],
                    forbidden_paths: Vec::new(),
                    workspace_id: Some(
                        WorkspaceId::from_legacy_named(format!("workspace_{}", job_id))
                            .expect("workspace"),
                    ),
                },
                status,
                required_gates: vec![EvaluationGate::StaticChecks],
                rollback_policy: RollbackPolicy::default(),
            },
            candidate_workspace: CandidateWorkspace {
                job_id: job_id.clone(),
                path: root.join(job_id.as_str()).join("worktree"),
                branch_name: format!("codex/evolve/{}", job_id),
                source_revision: Some("abc123".to_string()),
            },
            bundle: CandidateBundleManifest {
                job_id: job_id.clone(),
                bundle_path: root
                    .join(job_id.as_str())
                    .join("bundle")
                    .display()
                    .to_string(),
                created_at: OffsetDateTime::now_utc(),
                mutation_packet_path: root
                    .join(job_id.as_str())
                    .join("bundle")
                    .join("mutation-packet.json")
                    .display()
                    .to_string(),
                job_path: None,
                model_output_path: None,
                agent_run_path: None,
                agent_summary_path: None,
                benchmark_results_path: None,
                resource_usage_path: None,
                content_manifest_path: None,
                attestation_path: None,
                registry_record_path: None,
                bundle_content_hash: None,
                changed_files_path: None,
                diff_path: None,
                scorecard_path: None,
                candidate_summary_path: None,
                decision_path: None,
                command_logs: Vec::new(),
            },
            verification_outcome: None,
            changed_files: Vec::new(),
            decision: None,
        }
    }
}

pub fn safe_file_stem(value: &str) -> String {
    let stem: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let stem = stem.trim_matches('-');
    if stem.is_empty() {
        "command".to_string()
    } else {
        stem.to_string()
    }
}

#[derive(Debug, Error)]
pub enum EvolutionPersistenceError {
    #[error("evolution storage I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("evolution JSON error at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error("missing evolution artifact: {artifact}")]
    MissingArtifact { artifact: String },
}

#[derive(Debug, Error)]
pub enum EvolutionJobSelectionError {
    #[error("no active evolution job")]
    NoActiveJob,
    #[error("multiple active evolution jobs: {}", job_ids.join(", "))]
    AmbiguousJob { job_ids: Vec<String> },
    #[error(transparent)]
    Persistence(#[from] EvolutionPersistenceError),
}

#[derive(Debug, Error)]
pub enum EvolutionJobStateError {
    #[error("evolution job {job_id} is {status}; expected {expected}")]
    UnexpectedStatus {
        job_id: String,
        status: String,
        expected: String,
    },
}
