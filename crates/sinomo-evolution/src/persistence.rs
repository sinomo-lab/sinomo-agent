use crate::{
    BackgroundCampaignUpdate, BackgroundSchedulingAction, BackgroundSchedulingPlan,
    CampaignDecision, CampaignDecisionKind, CampaignScheduleDecision, EvolutionCampaign,
    EvolutionCampaignStatus, EvolutionReport, ResourceSchedulerDryRun, ScheduledIterationPlan,
    StageEvaluationUpdate,
};
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{
    CandidateBundleManifest, CandidateDecision, CandidateDecisionOutcome, EvaluationOutcome,
    EvaluationScorecard, EvolutionJob, EvolutionJobId, EvolutionJobStatus,
};
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

#[derive(Clone, Debug)]
pub struct EvolutionPaths {
    pub root: PathBuf,
    pub jobs: PathBuf,
    pub campaigns: PathBuf,
    pub logs: PathBuf,
    pub config: PathBuf,
}

impl EvolutionPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("evolution");
        Self {
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

pub fn read_stored_job(path: &Path) -> Result<StoredEvolutionJob, EvolutionPersistenceError> {
    read_json_file(path)
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
}
