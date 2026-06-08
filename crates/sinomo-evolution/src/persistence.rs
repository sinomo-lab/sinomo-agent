use crate::engine::proposed_regression_gates_from_candidate_evidence;
use crate::{
    BackgroundCampaignUpdate, BackgroundOpportunityIntake, BackgroundSchedulingAction,
    BackgroundSchedulingPlan, CANDIDATE_EVIDENCE_SUMMARY_RELATIVE_PATH,
    CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH, CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH,
    CampaignDecision, CampaignDecisionKind, CampaignScheduleDecision,
    CandidateApprovalPreflightReport, CandidateBundleManifest, CandidateBundleSifsMirrorFile,
    CandidateBundleSifsMirrorReport, CandidateDecision, CandidateDecisionOutcome,
    CandidateEvolutionLessonRecord, CandidateRegressionEvidenceSummary, EvolutionCampaign,
    EvolutionCampaignStatus, EvolutionJob, EvolutionJobId, EvolutionJobStatus,
    EvolutionLessonMemoryRecord, EvolutionRegressionGateMemoryRecord, EvolutionReport,
    ResourceSchedulerDryRun, ScheduledIterationPlan, StageEvaluationUpdate,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_runtime::config::{
    ConfigClusterMode, ConfigDefinition, ConfigError, ConfigRegistry, ConfigRegistryBuilder,
    ConfigStore, ConfigValueType, ConfigWriteTarget,
};
use sinomo_runtime::core::ClusterEndpointRecord;
use sinomo_runtime::core::{EvaluationOutcome, EvaluationScorecard};
use sinomo_runtime::dfs::{
    DfsEndpointTransport, DfsMetadataJournalBatchSender, DfsMetadataJournalReplicationOptions,
    DfsMetadataPeerTransportError, DfsMetadataShardConfig, DfsMetadataSnapshotInstallSender,
    DfsPeerTransportError, DfsProtocolFrameCommand, DfsProtocolFrameResponse,
    DfsProtocolFrameSender, DfsWriteOptions, LocalContentStore, NetworkedDfsService,
    default_dfs_metadata_root, default_dfs_metadata_shards_root, fallback_dfs_node_id,
    metadata_shards_prefix, open_local_sharded_metadata_backend, open_root_local_metadata_backend,
    publish_cluster_dfs_metadata_journal, sifs_file_system_id,
};
use sinomo_runtime::eval::CommandRunResult;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const EVOLUTION_CONFIG_KEY: &str = "evolution.config";
pub const DEFAULT_EVOLUTION_SIFS_ARTIFACT_ROOT: &str = "/services/evolution/artifacts";
pub const CANDIDATE_SIFS_MIRROR_RELATIVE_PATH: &str = "sifs-mirror.json";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionConfig {
    pub repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default = "default_evolution_sifs_artifact_root")]
    pub sifs_artifact_root: String,
}

pub fn default_evolution_sifs_artifact_root() -> String {
    DEFAULT_EVOLUTION_SIFS_ARTIFACT_ROOT.to_string()
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
    RegressionEvidence,
    EvolutionLesson,
    EvidenceSummary,
}

#[derive(Clone, Debug)]
pub struct EvolutionPaths {
    pub data_dir: PathBuf,
    pub root: PathBuf,
    pub jobs: PathBuf,
    pub campaigns: PathBuf,
    pub memory: PathBuf,
    pub lesson_memory_dir: PathBuf,
    pub lesson_memory_jsonl: PathBuf,
    pub proposed_regression_gate_memory_dir: PathBuf,
    pub proposed_regression_gate_memory_jsonl: PathBuf,
    pub logs: PathBuf,
    pub config: PathBuf,
    pub legacy_config: PathBuf,
}

impl EvolutionPaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        let data_dir_root = data_dir.root().to_path_buf();
        let root = data_dir_root.join("evolution");
        Self {
            data_dir: data_dir_root.clone(),
            jobs: root.join("jobs"),
            campaigns: root.join("campaigns"),
            memory: root.join("memory"),
            lesson_memory_dir: root.join("memory").join("lessons"),
            lesson_memory_jsonl: root.join("memory").join("lessons.jsonl"),
            proposed_regression_gate_memory_dir: root
                .join("memory")
                .join("proposed-regression-gates"),
            proposed_regression_gate_memory_jsonl: root
                .join("memory")
                .join("proposed-regression-gates.jsonl"),
            logs: root.join("logs"),
            config: data_dir_root
                .join("configs")
                .join("evolution")
                .join("config.json"),
            legacy_config: root.join("config.json"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), EvolutionPersistenceError> {
        for path in [
            &self.root,
            &self.jobs,
            &self.campaigns,
            &self.memory,
            &self.lesson_memory_dir,
            &self.proposed_regression_gate_memory_dir,
            &self.logs,
        ] {
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
    pub opportunity_intake_json: PathBuf,
    pub opportunity_intakes_jsonl: PathBuf,
}

impl BackgroundSchedulerPaths {
    pub fn new(paths: &EvolutionPaths) -> Self {
        let root = paths.root.join("background-scheduler");
        Self {
            plan_json: root.join("scheduler-plan.json"),
            status_json: root.join("background-status.json"),
            events_jsonl: root.join("scheduler-events.jsonl"),
            opportunity_intake_json: root.join("opportunity-intake.json"),
            opportunity_intakes_jsonl: root.join("opportunity-intakes.jsonl"),
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
    pub background_opportunity_intake_json: PathBuf,
    pub background_scoped_job_json: PathBuf,
    pub background_workspace_plan_json: PathBuf,
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
            background_opportunity_intake_json: root.join("background-opportunity-intake.json"),
            background_scoped_job_json: root.join("background-scoped-job.json"),
            background_workspace_plan_json: root.join("background-workspace-plan.json"),
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
    write_candidate_evolution_memory(paths, report)?;
    Ok(())
}

pub fn configured_sifs_artifact_root(config: Option<&EvolutionConfig>) -> Option<String> {
    let root = config
        .map(|config| config.sifs_artifact_root.clone())
        .unwrap_or_else(default_evolution_sifs_artifact_root);
    let root = root.trim();
    if root.is_empty() {
        None
    } else {
        Some(root.to_string())
    }
}

pub fn mirror_evolution_report_bundle_to_sifs(
    data_dir: &SinomoDataDir,
    report: &mut EvolutionReport,
    sifs_root: &str,
) -> Result<CandidateBundleSifsMirrorReport, EvolutionPersistenceError> {
    mirror_candidate_bundle_to_sifs(data_dir, &mut report.bundle, sifs_root)
}

pub fn mirror_candidate_bundle_to_sifs(
    data_dir: &SinomoDataDir,
    bundle: &mut CandidateBundleManifest,
    sifs_root: &str,
) -> Result<CandidateBundleSifsMirrorReport, EvolutionPersistenceError> {
    let bundle_root = PathBuf::from(&bundle.bundle_path);
    let sifs_root = normalize_sifs_path(sifs_root)?;
    let sifs_bundle_path = join_sifs_path(&sifs_root, bundle.job_id.as_str())?;
    let mut service = open_evolution_sifs_service(data_dir)?;
    service
        .posix_mkdir_all(&sifs_bundle_path)
        .map_err(|source| EvolutionPersistenceError::Sifs {
            path: sifs_bundle_path.clone(),
            detail: source.to_string(),
        })?;

    let mut local_files = Vec::new();
    let mut skipped_paths = Vec::new();
    collect_bundle_sifs_mirror_files(
        &bundle_root,
        &bundle_root,
        &mut local_files,
        &mut skipped_paths,
    )?;

    let mut mirrored_files = Vec::new();
    for local_path in local_files {
        let relative = bundle_relative_path(&bundle_root, &local_path);
        let sifs_path = join_sifs_path(&sifs_bundle_path, &relative)?;
        let file = fs::File::open(&local_path).map_err(|source| EvolutionPersistenceError::Io {
            path: local_path.display().to_string(),
            detail: source.to_string(),
        })?;
        let mirrored = write_reader_to_sifs(&mut service, &sifs_path, file)?;
        mirrored_files.push(CandidateBundleSifsMirrorFile {
            relative_path: relative,
            sifs_path,
            bytes: mirrored.bytes,
            digest: mirrored.digest,
        });
    }

    let sifs_report_path = join_sifs_path(&sifs_bundle_path, CANDIDATE_SIFS_MIRROR_RELATIVE_PATH)?;
    let mut report = CandidateBundleSifsMirrorReport {
        bundle_path: bundle.bundle_path.clone(),
        sifs_bundle_path: sifs_bundle_path.clone(),
        sifs_report_path: sifs_report_path.clone(),
        generated_at: current_rfc3339(),
        mirrored_files,
        skipped_paths,
        metadata_publish_error: None,
    };

    match publish_evolution_sifs_metadata_journal(data_dir) {
        Ok(_) => {}
        Err(error) => report.metadata_publish_error = Some(error.to_string()),
    }
    let local_report_path = bundle_root.join(CANDIDATE_SIFS_MIRROR_RELATIVE_PATH);
    write_json(&local_report_path, &report)?;
    let report_bytes =
        serde_json::to_vec_pretty(&report).map_err(|source| EvolutionPersistenceError::Json {
            path: local_report_path.display().to_string(),
            detail: source.to_string(),
        })?;
    write_bytes_to_sifs(&mut service, &sifs_report_path, &report_bytes)?;

    bundle
        .artifact_refs
        .push(format!("sifs://{}", report.sifs_bundle_path));
    bundle
        .artifact_refs
        .push(format!("sifs://{}", report.sifs_report_path));
    bundle.artifact_refs.extend(
        report
            .mirrored_files
            .iter()
            .map(|file| format!("sifs://{}", file.sifs_path)),
    );
    bundle.artifact_refs.sort();
    bundle.artifact_refs.dedup();
    write_json(&bundle_root.join("manifest.json"), bundle)?;

    Ok(report)
}

pub fn write_candidate_evolution_memory(
    paths: &EvolutionPaths,
    report: &EvolutionReport,
) -> Result<(), EvolutionPersistenceError> {
    let bundle_root = PathBuf::from(&report.bundle.bundle_path);
    let lesson_path = bundle_root.join(CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH);
    if !lesson_path.exists() {
        return Ok(());
    }
    let lesson: CandidateEvolutionLessonRecord = read_json_file(&lesson_path)?;
    let regression_evidence_path = bundle_root.join(CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH);
    let regression_evidence: Option<CandidateRegressionEvidenceSummary> =
        if regression_evidence_path.exists() {
            Some(read_json_file(&regression_evidence_path)?)
        } else {
            None
        };
    let proposed_regression_gates = proposed_regression_gates_from_candidate_evidence(
        report.job.job_id.as_str(),
        regression_evidence.as_ref(),
    );
    let proposed_regression_gate_ids = proposed_regression_gates
        .iter()
        .map(|gate| gate.gate_id.clone())
        .collect::<Vec<_>>();
    let mut evidence_refs = vec![
        format!("bundle:{}", report.bundle.bundle_path),
        format!("evolution-lesson:{}", lesson_path.display()),
    ];
    if regression_evidence_path.exists() {
        evidence_refs.push(format!(
            "regression-evidence:{}",
            regression_evidence_path.display()
        ));
    }
    if let Some(scorecard_path) = &report.bundle.scorecard_path {
        evidence_refs.push(format!("scorecard:{scorecard_path}"));
    }
    if let Some(diff_path) = &report.bundle.diff_path {
        evidence_refs.push(format!("diff:{diff_path}"));
    }
    evidence_refs.extend(
        lesson
            .reusable_evidence_refs
            .iter()
            .map(|evidence| format!("reusable-evidence:{evidence}")),
    );
    evidence_refs.sort();
    evidence_refs.dedup();

    let memory_id = format!("candidate_lesson_memory_{}", report.job.job_id.as_str());
    let lesson_record = EvolutionLessonMemoryRecord {
        memory_id: memory_id.clone(),
        campaign_id: "standalone-candidate".to_string(),
        adaptation_id: format!("candidate_{}", report.job.job_id.as_str()),
        stage_id: "standalone-candidate".to_string(),
        failed_iteration_id: report.job.job_id.to_string(),
        failed_task_id: report.job.job_id.to_string(),
        failure_class: lesson.failure_class.clone(),
        root_cause_summary: lesson.root_cause_summary.clone(),
        what_was_learned: candidate_lesson_memory_items(&lesson),
        next_task_simplification: lesson
            .suggested_follow_up_tasks
            .first()
            .cloned()
            .unwrap_or_else(|| {
                "create a smaller diagnostic task before retrying the candidate".to_string()
            }),
        new_eval_or_diagnostic: regression_evidence
            .as_ref()
            .map(|evidence| {
                if evidence
                    .replay_minimization
                    .candidate_gate_commands
                    .is_empty()
                {
                    "candidate regression evidence needs a minimized private gate".to_string()
                } else {
                    "candidate regression evidence includes reusable gate commands".to_string()
                }
            })
            .unwrap_or_else(|| "candidate lesson did not include regression evidence".to_string()),
        foundation_issue_detected: false,
        budget_adjustment:
            "no scheduler authority change; future dispatch still requires normal leases"
                .to_string(),
        human_attention_needed: true,
        replacement_task_candidate: None,
        proposed_regression_gate_ids,
        evidence_refs: evidence_refs.clone(),
        campaign_bundle_path: report.bundle.bundle_path.clone(),
        recorded_at: lesson.recorded_at.clone(),
    };
    let lesson_memory_path = paths
        .lesson_memory_dir
        .join(format!("{}.json", safe_memory_path_component(&memory_id)));
    let new_lesson_record = !lesson_memory_path.exists();
    write_json(&lesson_memory_path, &lesson_record)?;
    if new_lesson_record {
        append_jsonl(&paths.lesson_memory_jsonl, &lesson_record)?;
    }

    for gate in proposed_regression_gates {
        let gate_record = EvolutionRegressionGateMemoryRecord {
            memory_id: format!(
                "candidate_gate_memory_{}_{}",
                report.job.job_id.as_str(),
                gate.gate_id
            ),
            campaign_id: "standalone-candidate".to_string(),
            adaptation_id: format!("candidate_{}", report.job.job_id.as_str()),
            stage_id: "standalone-candidate".to_string(),
            failed_task_id: report.job.job_id.to_string(),
            status: "proposed".to_string(),
            proposed_gate: gate,
            evidence_refs: evidence_refs.clone(),
            campaign_bundle_path: report.bundle.bundle_path.clone(),
            recorded_at: lesson.recorded_at.clone(),
        };
        let gate_memory_path = paths.proposed_regression_gate_memory_dir.join(format!(
            "{}.json",
            safe_memory_path_component(&gate_record.memory_id)
        ));
        let new_gate_record = !gate_memory_path.exists();
        write_json(&gate_memory_path, &gate_record)?;
        if new_gate_record {
            append_jsonl(&paths.proposed_regression_gate_memory_jsonl, &gate_record)?;
        }
    }
    Ok(())
}

fn candidate_lesson_memory_items(lesson: &CandidateEvolutionLessonRecord) -> Vec<String> {
    let mut learned = Vec::new();
    learned.extend(
        lesson
            .failed_assumptions
            .iter()
            .map(|assumption| format!("failed assumption: {assumption}")),
    );
    learned.extend(
        lesson
            .suggested_follow_up_tasks
            .iter()
            .map(|task| format!("candidate follow-up: {task}")),
    );
    learned.extend(
        lesson
            .rollback_evidence
            .iter()
            .map(|evidence| format!("rollback evidence: {evidence}")),
    );
    learned.sort();
    learned.dedup();
    learned
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
        StoredJobArtifactKind::RegressionEvidence => Some(
            PathBuf::from(&record.bundle.bundle_path)
                .join(CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH)
                .display()
                .to_string(),
        ),
        StoredJobArtifactKind::EvolutionLesson => Some(
            PathBuf::from(&record.bundle.bundle_path)
                .join(CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH)
                .display()
                .to_string(),
        ),
        StoredJobArtifactKind::EvidenceSummary => Some(
            PathBuf::from(&record.bundle.bundle_path)
                .join(CANDIDATE_EVIDENCE_SUMMARY_RELATIVE_PATH)
                .display()
                .to_string(),
        ),
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

pub fn evolution_config_definition() -> Result<ConfigDefinition, EvolutionPersistenceError> {
    ConfigDefinition::new(
        EVOLUTION_CONFIG_KEY,
        "sinomo-evolution",
        ConfigValueType::Object {
            schema_name: "EvolutionConfig".to_string(),
        },
        "Local self-evolution defaults such as source repo and preferred model.",
        ConfigClusterMode::LocalOnly,
    )
    .map_err(map_evolution_config_error)
}

pub fn register_config_definitions(
    builder: ConfigRegistryBuilder,
) -> Result<ConfigRegistryBuilder, EvolutionPersistenceError> {
    builder
        .register(evolution_config_definition()?)
        .map_err(map_evolution_config_error)
}

pub fn read_config(paths: &EvolutionPaths) -> Result<EvolutionConfig, EvolutionPersistenceError> {
    let data_dir = SinomoDataDir::new(paths.data_dir.clone());
    if paths.config.exists() {
        return evolution_config_store()?
            .get_typed(&data_dir, None, EVOLUTION_CONFIG_KEY)
            .map_err(map_evolution_config_error);
    }
    if paths.legacy_config.exists() {
        let config = read_json_file(&paths.legacy_config)?;
        write_config(paths, &config)?;
        return Ok(config);
    }
    evolution_config_store()?
        .get_typed(&data_dir, None, EVOLUTION_CONFIG_KEY)
        .map_err(map_evolution_config_error)
}

pub fn write_config(
    paths: &EvolutionPaths,
    config: &EvolutionConfig,
) -> Result<(), EvolutionPersistenceError> {
    let data_dir = SinomoDataDir::new(paths.data_dir.clone());
    evolution_config_store()?
        .set_typed(
            &data_dir,
            None,
            EVOLUTION_CONFIG_KEY,
            config,
            ConfigWriteTarget::Local,
        )
        .map(|_| ())
        .map_err(map_evolution_config_error)
}

fn evolution_config_store() -> Result<ConfigStore, EvolutionPersistenceError> {
    let registry = register_config_definitions(ConfigRegistry::builder())?.build();
    Ok(ConfigStore::new(registry))
}

fn map_evolution_config_error(source: ConfigError) -> EvolutionPersistenceError {
    EvolutionPersistenceError::Config {
        key: EVOLUTION_CONFIG_KEY.to_string(),
        detail: source.to_string(),
    }
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
        write_evolution_lesson_memory(paths, update, &bundle)?;
    }
    Ok(bundle)
}

pub fn write_evolution_lesson_memory(
    paths: &EvolutionPaths,
    update: &StageEvaluationUpdate,
    bundle: &CampaignBundlePaths,
) -> Result<(), EvolutionPersistenceError> {
    let Some(adaptation) = &update.adaptation else {
        return Ok(());
    };
    let proposed_regression_gate_ids = adaptation
        .proposed_regression_gates
        .iter()
        .map(|gate| gate.gate_id.clone())
        .collect::<Vec<_>>();
    let lesson_record = EvolutionLessonMemoryRecord {
        memory_id: format!("lesson_memory_{}", adaptation.adaptation_id),
        campaign_id: adaptation.campaign_id.clone(),
        adaptation_id: adaptation.adaptation_id.clone(),
        stage_id: adaptation.stage_id.clone(),
        failed_iteration_id: adaptation.failed_iteration_id.clone(),
        failed_task_id: adaptation.failed_task_id.clone(),
        failure_class: adaptation.failure_class.clone(),
        root_cause_summary: adaptation.root_cause_summary.clone(),
        what_was_learned: adaptation.what_was_learned.clone(),
        next_task_simplification: adaptation.next_task_simplification.clone(),
        new_eval_or_diagnostic: adaptation.new_eval_or_diagnostic.clone(),
        foundation_issue_detected: adaptation.foundation_issue_detected,
        budget_adjustment: adaptation.budget_adjustment.clone(),
        human_attention_needed: adaptation.human_attention_needed,
        replacement_task_candidate: adaptation.replacement_task_candidate.clone(),
        proposed_regression_gate_ids,
        evidence_refs: adaptation.evidence_refs.clone(),
        campaign_bundle_path: bundle.root.display().to_string(),
        recorded_at: adaptation.recorded_at.clone(),
    };
    let lesson_path = paths.lesson_memory_dir.join(format!(
        "{}.json",
        safe_memory_path_component(&lesson_record.memory_id)
    ));
    let new_lesson_record = !lesson_path.exists();
    write_json(&lesson_path, &lesson_record)?;
    if new_lesson_record {
        append_jsonl(&paths.lesson_memory_jsonl, &lesson_record)?;
    }

    for gate in &adaptation.proposed_regression_gates {
        let gate_record = EvolutionRegressionGateMemoryRecord {
            memory_id: format!("gate_memory_{}_{}", adaptation.adaptation_id, gate.gate_id),
            campaign_id: adaptation.campaign_id.clone(),
            adaptation_id: adaptation.adaptation_id.clone(),
            stage_id: adaptation.stage_id.clone(),
            failed_task_id: adaptation.failed_task_id.clone(),
            status: "proposed".to_string(),
            proposed_gate: gate.clone(),
            evidence_refs: adaptation.evidence_refs.clone(),
            campaign_bundle_path: bundle.root.display().to_string(),
            recorded_at: adaptation.recorded_at.clone(),
        };
        let gate_path = paths.proposed_regression_gate_memory_dir.join(format!(
            "{}.json",
            safe_memory_path_component(&gate_record.memory_id)
        ));
        let new_gate_record = !gate_path.exists();
        write_json(&gate_path, &gate_record)?;
        if new_gate_record {
            append_jsonl(&paths.proposed_regression_gate_memory_jsonl, &gate_record)?;
        }
    }
    Ok(())
}

pub fn read_recent_evolution_lesson_memory(
    paths: &EvolutionPaths,
    limit: usize,
) -> Result<Vec<EvolutionLessonMemoryRecord>, EvolutionPersistenceError> {
    read_recent_jsonl(&paths.lesson_memory_jsonl, limit)
}

pub fn read_recent_proposed_regression_gate_memory(
    paths: &EvolutionPaths,
    limit: usize,
) -> Result<Vec<EvolutionRegressionGateMemoryRecord>, EvolutionPersistenceError> {
    read_recent_jsonl(&paths.proposed_regression_gate_memory_jsonl, limit)
}

fn read_recent_jsonl<T: DeserializeOwned>(
    path: &Path,
    limit: usize,
) -> Result<Vec<T>, EvolutionPersistenceError> {
    if limit == 0 || !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(path).map_err(|source| EvolutionPersistenceError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut values = content
        .lines()
        .rev()
        .filter(|line| !line.trim().is_empty())
        .take(limit)
        .map(|line| {
            serde_json::from_str(line).map_err(|source| EvolutionPersistenceError::Json {
                path: path.display().to_string(),
                detail: source.to_string(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    values.reverse();
    Ok(values)
}

fn safe_memory_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
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
    if !plan.opportunity_intake.is_empty() {
        write_json(
            &background_paths.opportunity_intake_json,
            &plan.opportunity_intake,
        )?;
        for intake in &plan.opportunity_intake {
            append_jsonl(&background_paths.opportunity_intakes_jsonl, intake)?;
        }
    }
    for update in &plan.campaigns {
        let mut campaign = update.campaign.clone();
        campaign.updated_at = plan.generated_at.clone();
        let intake = plan
            .opportunity_intake
            .iter()
            .find(|intake| intake.campaign_id == update.campaign.campaign_id);
        campaign
            .decisions
            .push(background_campaign_decision(plan, update, intake));
        let bundle = write_campaign_bundle(paths, &campaign)?;
        if let Some(intake) = intake {
            write_json(&bundle.background_opportunity_intake_json, intake)?;
            write_json(&bundle.background_scoped_job_json, &intake.scoped_job)?;
            write_json(
                &bundle.background_workspace_plan_json,
                &intake.workspace_plan,
            )?;
        }
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
    intake: Option<&BackgroundOpportunityIntake>,
) -> CampaignDecision {
    let kind = match update.action {
        BackgroundSchedulingAction::PauseForBudget => CampaignDecisionKind::Paused,
        BackgroundSchedulingAction::DispatchIteration
            if matches!(
                update.previous_status,
                EvolutionCampaignStatus::PausedBudget
            ) =>
        {
            CampaignDecisionKind::Resumed
        }
        BackgroundSchedulingAction::DispatchIteration => CampaignDecisionKind::TaskSelected,
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
    let mut evidence_refs = vec![
        "background-scheduler/scheduler-plan.json".to_string(),
        "background-scheduler/scheduler-events.jsonl".to_string(),
    ];
    if intake.is_some() {
        evidence_refs.extend([
            "background-scheduler/opportunity-intake.json".to_string(),
            "background-scheduler/opportunity-intakes.jsonl".to_string(),
            format!(
                "campaigns/{}/background-opportunity-intake.json",
                update.campaign.campaign_id
            ),
            format!(
                "campaigns/{}/background-scoped-job.json",
                update.campaign.campaign_id
            ),
            format!(
                "campaigns/{}/background-workspace-plan.json",
                update.campaign.campaign_id
            ),
        ]);
    }
    evidence_refs.extend(update.evidence_refs.clone());
    evidence_refs.sort();
    evidence_refs.dedup();

    CampaignDecision {
        decision_id: format!("decision_{}", update.campaign.decisions.len() + 1),
        kind,
        summary: format!("Background scheduler action: {:?}", update.action),
        rationale: update.reason.clone(),
        evidence_refs,
        decided_by: if plan.dry_run {
            "sinomo evolve tick --dry-run".to_string()
        } else {
            "sinomo evolve tick --dispatch-approved".to_string()
        },
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

pub fn read_background_opportunity_intake_event_ids(
    paths: &EvolutionPaths,
) -> Result<BTreeSet<String>, EvolutionPersistenceError> {
    let background_paths = BackgroundSchedulerPaths::new(paths);
    let mut event_ids = BTreeSet::new();
    if background_paths.opportunity_intake_json.exists() {
        let intakes: Vec<BackgroundOpportunityIntake> =
            read_json_file(&background_paths.opportunity_intake_json)?;
        for intake in &intakes {
            collect_background_opportunity_intake_event_id(&mut event_ids, intake);
        }
    }
    if background_paths.opportunity_intakes_jsonl.exists() {
        let file =
            fs::File::open(&background_paths.opportunity_intakes_jsonl).map_err(|source| {
                EvolutionPersistenceError::Io {
                    path: background_paths
                        .opportunity_intakes_jsonl
                        .display()
                        .to_string(),
                    detail: source.to_string(),
                }
            })?;
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line = line.map_err(|source| EvolutionPersistenceError::Io {
                path: format!(
                    "{}:{}",
                    background_paths.opportunity_intakes_jsonl.display(),
                    index + 1
                ),
                detail: source.to_string(),
            })?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let intake: BackgroundOpportunityIntake =
                serde_json::from_str(line).map_err(|source| EvolutionPersistenceError::Json {
                    path: format!(
                        "{}:{}",
                        background_paths.opportunity_intakes_jsonl.display(),
                        index + 1
                    ),
                    detail: source.to_string(),
                })?;
            collect_background_opportunity_intake_event_id(&mut event_ids, &intake);
        }
    }
    Ok(event_ids)
}

fn collect_background_opportunity_intake_event_id(
    event_ids: &mut BTreeSet<String>,
    intake: &BackgroundOpportunityIntake,
) {
    if let Some(event_id) = &intake.detection.opportunity.observed_event_id {
        event_ids.insert(event_id.to_string());
    }
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
    use sinomo_runtime::core::{
        Confidence, EvaluationGate, EventEnvelope, EventKind, Id, LatencyClass, PrivacyClass,
        RollbackPolicy, RoutingMetadata, RoutingScope, Salience, Source, TrustRing,
    };
    use sinomo_runtime::events::EventId;
    use sinomo_runtime::node::NodeId;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    type ScorecardId = Id;

    #[test]
    fn evolution_config_writes_shared_config_document() {
        let root = temp_dir("evolution-config-document");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        let config = EvolutionConfig {
            repo: root.join("source").display().to_string(),
            default_provider: Some("openai".to_string()),
            default_model: Some("gpt-5.5".to_string()),
            sifs_artifact_root: default_evolution_sifs_artifact_root(),
        };

        write_config(&paths, &config).expect("write config");

        assert_eq!(
            paths.config,
            root.join("configs").join("evolution").join("config.json")
        );
        let document: sinomo_runtime::config::ConfigDocument =
            serde_json::from_str(&fs::read_to_string(&paths.config).expect("config document"))
                .expect("config document");
        assert_eq!(document.key.as_str(), EVOLUTION_CONFIG_KEY);
        assert_eq!(
            serde_json::from_value::<EvolutionConfig>(document.value).expect("stored config"),
            config
        );
        assert_eq!(read_config(&paths).expect("read config"), config);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn evolution_config_migrates_legacy_config_json() {
        let root = temp_dir("evolution-config-migrate");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        let config = EvolutionConfig {
            repo: root.join("source").display().to_string(),
            default_provider: Some("codex".to_string()),
            default_model: Some("gpt-5.5".to_string()),
            sifs_artifact_root: default_evolution_sifs_artifact_root(),
        };
        write_json(&paths.legacy_config, &config).expect("legacy config");

        let migrated = read_config(&paths).expect("read config");

        assert_eq!(migrated, config);
        let document: sinomo_runtime::config::ConfigDocument = serde_json::from_str(
            &fs::read_to_string(&paths.config).expect("migrated config document"),
        )
        .expect("config document");
        assert_eq!(document.key.as_str(), EVOLUTION_CONFIG_KEY);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn mirror_candidate_bundle_to_sifs_persists_local_artifacts_and_skips_workspaces() {
        let root = temp_dir("sifs-mirror");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let mut record = stored_record_fixture(&root, EvolutionJobStatus::AwaitingApproval);
        let bundle_dir = PathBuf::from(&record.bundle.bundle_path);
        fs::create_dir_all(&bundle_dir).expect("bundle dir");
        fs::write(&record.bundle.mutation_packet_path, r#"{"task":"smoke"}"#)
            .expect("mutation packet");
        let benchmark_summary = bundle_dir
            .join("benchmarks")
            .join("swebench")
            .join("summary.json");
        fs::create_dir_all(benchmark_summary.parent().expect("summary parent"))
            .expect("benchmark dir");
        fs::write(&benchmark_summary, r#"{"resolved":0}"#).expect("summary");
        let workspace_file = bundle_dir
            .join("benchmarks")
            .join("swebench")
            .join("sinomo")
            .join("workspaces")
            .join("instance")
            .join("README.md");
        fs::create_dir_all(workspace_file.parent().expect("workspace parent"))
            .expect("workspace dir");
        fs::write(&workspace_file, "large checkout").expect("workspace file");
        let worktree_file = bundle_dir.join("worktree").join("README.md");
        fs::create_dir_all(worktree_file.parent().expect("worktree parent")).expect("worktree dir");
        fs::write(&worktree_file, "candidate checkout").expect("worktree file");

        let report = mirror_candidate_bundle_to_sifs(
            &data_dir,
            &mut record.bundle,
            DEFAULT_EVOLUTION_SIFS_ARTIFACT_ROOT,
        )
        .expect("mirror bundle");

        assert_eq!(
            report.sifs_bundle_path,
            format!(
                "{}/{}",
                DEFAULT_EVOLUTION_SIFS_ARTIFACT_ROOT,
                record.bundle.job_id.as_str()
            )
        );
        assert_eq!(
            report.sifs_report_path,
            format!(
                "{}/{}",
                report.sifs_bundle_path, CANDIDATE_SIFS_MIRROR_RELATIVE_PATH
            )
        );
        assert!(
            report
                .mirrored_files
                .iter()
                .any(|file| file.relative_path == "mutation-packet.json")
        );
        assert!(
            report
                .mirrored_files
                .iter()
                .any(|file| file.relative_path == "benchmarks/swebench/summary.json")
        );
        assert!(
            report
                .skipped_paths
                .iter()
                .any(|path| path == "benchmarks/swebench/sinomo/workspaces")
        );
        assert!(report.skipped_paths.iter().any(|path| path == "worktree"));
        assert!(
            !report
                .mirrored_files
                .iter()
                .any(|file| file.relative_path.contains("/workspaces/"))
        );
        assert!(
            !report
                .mirrored_files
                .iter()
                .any(|file| file.relative_path.starts_with("worktree/"))
        );
        assert!(
            record
                .bundle
                .artifact_refs
                .iter()
                .any(|artifact| artifact == &format!("sifs://{}", report.sifs_report_path))
        );
        let stored_manifest: CandidateBundleManifest =
            read_json_file(&bundle_dir.join("manifest.json")).expect("stored manifest");
        assert_eq!(stored_manifest.artifact_refs, record.bundle.artifact_refs);

        let service = open_evolution_sifs_service(&data_dir).expect("open sifs");
        assert_eq!(
            service
                .posix_read_file(&format!("{}/mutation-packet.json", report.sifs_bundle_path))
                .expect("read mirrored packet"),
            br#"{"task":"smoke"}"#
        );
        let local_report: CandidateBundleSifsMirrorReport =
            read_json_file(&bundle_dir.join(CANDIDATE_SIFS_MIRROR_RELATIVE_PATH))
                .expect("local mirror report");
        assert_eq!(local_report, report);
        fs::remove_dir_all(root).ok();
    }

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
                artifact_refs: Vec::new(),
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
                artifact_refs: Vec::new(),
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
                artifact_refs: Vec::new(),
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

    #[test]
    fn write_campaign_stage_evaluation_indexes_lessons_and_proposed_gates_as_memory() {
        let root = temp_dir("campaign-lesson-memory");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let campaign = crate::propose_evolution_campaign(crate::CampaignProposalRequest {
            objective: "make failed campaign attempts reusable".to_string(),
            horizon: "14 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: Vec::new(),
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: Some("10m".to_string()),
            pause_after_proposal: false,
            dry_run: true,
            approved: true,
            target_ring: TrustRing::R2TrustedService,
            source_repo: Some(root.display().to_string()),
        });
        let stage_id = campaign.stages[0].stage_id.clone();
        let task_id = campaign.near_term_tasks[0].task_id.clone();
        let update = crate::StageEvaluationUpdate {
            campaign: campaign.clone(),
            stage_scorecard: crate::CampaignStageScorecard {
                scorecard_id: ScorecardId::generate().to_string(),
                campaign_id: campaign.campaign_id.clone(),
                stage_id: stage_id.clone(),
                task_id: task_id.clone(),
                iteration_id: "stage_eval_failed".to_string(),
                outcome: EvaluationOutcome::Failed,
                declared_metrics: Vec::new(),
                met_metrics: Vec::new(),
                missing_metrics: Vec::new(),
                regression_gates: Vec::new(),
                evidence_refs: vec!["scorecard:/tmp/scorecard.json".to_string()],
                promotion: crate::StagePromotionDecision::HoldFailed,
                summary: "stage failed and should create memory".to_string(),
                recorded_at: "2026-05-02T03:00:00Z".to_string(),
            },
            adaptation: Some(crate::CampaignAdaptationRecord {
                adaptation_id: "adapt_memory_1".to_string(),
                campaign_id: campaign.campaign_id.clone(),
                stage_id: stage_id.clone(),
                failed_iteration_id: "iter_failed_1".to_string(),
                failed_task_id: task_id.clone(),
                failure_class: crate::EvolutionFailureClass::EvalRegression,
                root_cause_summary: "timeout replay still reproduces".to_string(),
                what_was_learned: vec!["replay needs minimization".to_string()],
                next_task_simplification: "create smaller replay".to_string(),
                new_eval_or_diagnostic: "metric-specific regression fixture".to_string(),
                foundation_issue_detected: false,
                proposed_regression_gates: vec![crate::CampaignProposedRegressionGate {
                    gate_id: "gate_timeout_replay".to_string(),
                    gate: EvaluationGate::ReplayTests,
                    source: "module-replay-report".to_string(),
                    evidence_ref: "replays/timeout-replay.json".to_string(),
                    command: Some(
                        "cargo test -p sinomo-evolution timeout_replay_fixture".to_string(),
                    ),
                    rationale: "keep timeout replay as a private gate".to_string(),
                }],
                replacement_task_candidate: None,
                budget_adjustment: "keep scheduler gate".to_string(),
                human_attention_needed: false,
                evidence_refs: vec![
                    "regression-evidence:/tmp/bundle/regression-evidence.json".to_string(),
                ],
                recorded_at: "2026-05-02T03:00:00Z".to_string(),
            }),
        };

        let bundle = write_campaign_stage_evaluation(&paths, &update).expect("stage evaluation");
        write_campaign_stage_evaluation(&paths, &update).expect("idempotent memory rewrite");

        assert!(bundle.adaptation_json.exists());
        assert!(paths.lesson_memory_jsonl.exists());
        assert!(paths.proposed_regression_gate_memory_jsonl.exists());
        let lesson_lines = fs::read_to_string(&paths.lesson_memory_jsonl)
            .expect("lesson memory jsonl")
            .lines()
            .count();
        assert_eq!(lesson_lines, 1);
        let gate_lines = fs::read_to_string(&paths.proposed_regression_gate_memory_jsonl)
            .expect("gate memory jsonl")
            .lines()
            .count();
        assert_eq!(gate_lines, 1);
        let lesson_path = paths
            .lesson_memory_dir
            .join("lesson_memory_adapt_memory_1.json");
        assert!(lesson_path.exists());
        let lesson: crate::EvolutionLessonMemoryRecord =
            read_json_file(&lesson_path).expect("lesson memory");
        assert_eq!(lesson.adaptation_id, "adapt_memory_1");
        assert_eq!(
            lesson.proposed_regression_gate_ids,
            vec!["gate_timeout_replay".to_string()]
        );
        let gate_path = paths
            .proposed_regression_gate_memory_dir
            .join("gate_memory_adapt_memory_1_gate_timeout_replay.json");
        assert!(gate_path.exists());
        let gate: crate::EvolutionRegressionGateMemoryRecord =
            read_json_file(&gate_path).expect("gate memory");
        assert_eq!(gate.status, "proposed");
        assert_eq!(gate.proposed_gate.gate, EvaluationGate::ReplayTests);
        assert_eq!(
            gate.proposed_gate.evidence_ref,
            "replays/timeout-replay.json"
        );
        let recent_lessons =
            read_recent_evolution_lesson_memory(&paths, 8).expect("recent lessons");
        let recent_gates =
            read_recent_proposed_regression_gate_memory(&paths, 8).expect("recent gates");
        assert_eq!(recent_lessons.len(), 1);
        assert_eq!(recent_gates.len(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_report_record_indexes_failed_candidate_lesson_as_memory() {
        let root = temp_dir("candidate-lesson-memory");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let mut record = stored_record_fixture(&root, EvolutionJobStatus::AwaitingApproval);
        let bundle_dir = PathBuf::from(&record.bundle.bundle_path);
        fs::create_dir_all(&bundle_dir).expect("bundle dir");
        let command_log_path = bundle_dir.join("command-logs/focused-replay.log");
        fs::create_dir_all(command_log_path.parent().expect("parent")).expect("log dir");
        fs::write(&command_log_path, "timeout replay still fails").expect("command log");
        let scorecard_path = bundle_dir.join("scorecard.json");
        record.bundle.scorecard_path = Some(scorecard_path.display().to_string());
        record.changed_files = vec!["modules/trigger/silence_policy.wat".to_string()];
        let evidence = crate::CandidateRegressionEvidenceSummary {
            job_id: record.job.job_id.clone(),
            objective: record.job.objective.clone(),
            target_ring: record.job.target_ring.clone(),
            outcome: EvaluationOutcome::Failed,
            bundle_path: record.bundle.bundle_path.clone(),
            scorecard_path: record.bundle.scorecard_path.clone(),
            diff_path: None,
            changed_files: record.changed_files.clone(),
            failed_gates: vec![crate::RegressionGateEvidence {
                gate: EvaluationGate::ReplayTests,
                outcome: EvaluationOutcome::Failed,
                summary: "timeout replay still fails".to_string(),
            }],
            failed_commands: vec![crate::RegressionCommandFailure {
                name: "focused-replay".to_string(),
                command: "cargo test -p sinomo-evolution timeout_replay_fixture".to_string(),
                expected_exit_code: 0,
                exit_code: 1,
                duration_ms: 420,
                command_log_path: Some(command_log_path.display().to_string()),
                stdout_excerpt: String::new(),
                stderr_excerpt: "timeout replay still fails".to_string(),
                rerun_command: "cargo test -p sinomo-evolution timeout_replay_fixture"
                    .to_string(),
            }],
            failed_benchmarks: Vec::new(),
            trace_fixtures: vec![crate::TraceDerivedRegressionFixture {
                name: "timeout-replay".to_string(),
                source: "module-replay-report".to_string(),
                report_path: "replays/timeout-replay.json".to_string(),
                baseline_artifact_id: None,
                candidate_artifact_id: None,
                minimization_hint:
                    "reduce timeout replay to the shortest trace that still fails".to_string(),
                suggested_gate: EvaluationGate::ReplayTests,
            }],
            replay_minimization: crate::ReplayMinimizationPlan {
                status: "ready-for-minimization".to_string(),
                retained_evidence_refs: vec!["replays/timeout-replay.json".to_string()],
                minimization_steps: vec![
                    "reduce replays/timeout-replay.json before retry".to_string()
                ],
                candidate_gate_commands: vec![
                    "cargo test -p sinomo-evolution timeout_replay_fixture".to_string()
                ],
            },
            reusable_gate_suggestions: vec![
                "keep replay fixture `timeout-replay` at `replays/timeout-replay.json` as a ReplayTests gate"
                    .to_string(),
            ],
            generated_at: "2026-05-02T04:00:00Z".to_string(),
        };
        let lesson = crate::CandidateEvolutionLessonRecord {
            lesson_id: "lesson_candidate_timeout".to_string(),
            job_id: record.job.job_id.clone(),
            objective: record.job.objective.clone(),
            outcome: EvaluationOutcome::Failed,
            failure_class: crate::EvolutionFailureClass::EvalRegression,
            root_cause_summary: "timeout replay still reproduces".to_string(),
            failed_assumptions: vec!["candidate would satisfy replay gate".to_string()],
            rollback_evidence: vec!["no runtime rollout attempted".to_string()],
            suggested_follow_up_tasks: vec![
                "minimize timeout replay before another implementation attempt".to_string(),
            ],
            reusable_evidence_refs: vec!["replays/timeout-replay.json".to_string()],
            recorded_at: "2026-05-02T04:00:00Z".to_string(),
        };
        write_json(
            &bundle_dir.join(CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH),
            &evidence,
        )
        .expect("regression evidence");
        write_json(
            &bundle_dir.join(CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH),
            &lesson,
        )
        .expect("lesson");
        let scorecard = EvaluationScorecard {
            scorecard_id: ScorecardId::generate(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
            outcome: EvaluationOutcome::Failed,
            gates: Vec::new(),
            metrics: Vec::new(),
        };
        let report = EvolutionReport {
            job: record.job.clone(),
            candidate_workspace: record.candidate_workspace.clone(),
            bundle: record.bundle.clone(),
            scorecard: Some(scorecard),
            changed_files: record.changed_files.clone(),
        };

        write_report_record(&paths, &report).expect("write report");
        write_report_record(&paths, &report).expect("idempotent memory rewrite");

        let recent_lessons =
            read_recent_evolution_lesson_memory(&paths, 8).expect("recent candidate lessons");
        let recent_gates =
            read_recent_proposed_regression_gate_memory(&paths, 8).expect("recent candidate gates");
        assert_eq!(recent_lessons.len(), 1);
        let lesson_memory = &recent_lessons[0];
        assert_eq!(lesson_memory.campaign_id, "standalone-candidate");
        assert!(lesson_memory.human_attention_needed);
        assert_eq!(
            lesson_memory.root_cause_summary,
            "timeout replay still reproduces"
        );
        assert!(
            lesson_memory
                .evidence_refs
                .iter()
                .any(|evidence| evidence.contains("evolution-lesson:"))
        );
        assert!(recent_gates.iter().any(|gate| {
            gate.proposed_gate.command.as_deref()
                == Some("cargo test -p sinomo-evolution timeout_replay_fixture")
        }));
        assert!(recent_gates.iter().any(|gate| {
            gate.proposed_gate.gate == EvaluationGate::ReplayTests
                && gate.proposed_gate.evidence_ref == "replays/timeout-replay.json"
        }));
        assert_eq!(
            fs::read_to_string(&paths.lesson_memory_jsonl)
                .expect("lesson jsonl")
                .lines()
                .count(),
            1
        );
        let gate_jsonl_count = fs::read_to_string(&paths.proposed_regression_gate_memory_jsonl)
            .expect("gate jsonl")
            .lines()
            .count();
        assert_eq!(gate_jsonl_count, recent_gates.len());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_background_scheduling_plan_persists_opportunity_intake_artifacts() {
        let root = temp_dir("background-opportunity-intake");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let event = EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            NodeId::from_legacy_named("node_desktop").expect("node"),
            Source::new("module.live").expect("source"),
            EventKind::new("module.timeout").expect("kind"),
            Salience::new(0.83).expect("salience"),
            Confidence::new(0.96).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "module_id": "module.trigger.silence_policy",
            "suggested_paths": ["modules/trigger/**"],
            "evidence_refs": ["module-telemetry://timeout-window"]
        }));
        let event_id = event.event_id.clone();
        let plan = crate::plan_background_scheduling_tick(crate::BackgroundSchedulingRequest {
            scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
            campaigns: Vec::new(),
            opportunity_events: vec![event],
            previously_intaken_opportunity_event_ids: Vec::new(),
            opportunity_detection_policy: Some(crate::EvolutionOpportunityDetectionPolicy {
                workspace_id: WorkspaceId::from_legacy_named("workspace_self_evolution")
                    .expect("workspace"),
                default_target_ring: TrustRing::R2TrustedService,
                default_allowed_paths: vec!["crates/sinomo-module/**".to_string()],
                default_forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
            }),
            opportunity_planner_node_id: Some(
                NodeId::from_legacy_named("node_desktop").expect("node"),
            ),
            external_workloads: Vec::new(),
            resources: Vec::new(),
            task_estimates: Vec::new(),
            max_concurrent_campaigns: 1,
            max_concurrent_workloads: 1,
            idle_window_open: true,
            reset_window_open: false,
            dry_run: true,
            now: "2026-05-02T01:30:00Z".to_string(),
        });
        assert_eq!(plan.opportunity_intake.len(), 1);
        let intake = plan.opportunity_intake[0].clone();
        let background_paths =
            write_background_scheduling_plan(&paths, &plan).expect("background plan");

        assert!(background_paths.opportunity_intake_json.exists());
        assert!(background_paths.opportunity_intakes_jsonl.exists());
        let persisted_intake: Vec<crate::BackgroundOpportunityIntake> =
            read_json_file(&background_paths.opportunity_intake_json).expect("intake json");
        assert_eq!(persisted_intake[0].campaign_id, intake.campaign_id);
        assert!(
            fs::read_to_string(&background_paths.opportunity_intakes_jsonl)
                .expect("intake jsonl")
                .contains("ModuleTimeout")
        );
        let intaken_event_ids =
            read_background_opportunity_intake_event_ids(&paths).expect("intake event ids");
        assert!(intaken_event_ids.contains(event_id.as_str()));

        let bundle = CampaignBundlePaths::new(&paths, &intake.campaign_id);
        assert!(bundle.background_opportunity_intake_json.exists());
        assert!(bundle.background_scoped_job_json.exists());
        assert!(bundle.background_workspace_plan_json.exists());
        let scoped_job: EvolutionJob =
            read_json_file(&bundle.background_scoped_job_json).expect("scoped job");
        assert_eq!(scoped_job.job_id, intake.scoped_job.job_id);
        let stored_campaign: EvolutionCampaign =
            read_json_file(&bundle.campaign_json).expect("campaign");
        assert!(stored_campaign.decisions.iter().any(|decision| {
            decision
                .evidence_refs
                .iter()
                .any(|evidence| evidence.ends_with("background-scoped-job.json"))
        }));
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
                artifact_refs: Vec::new(),
                command_logs: Vec::new(),
            },
            verification_outcome: None,
            changed_files: Vec::new(),
            decision: None,
        }
    }
}

#[derive(Clone, Debug)]
struct SifsWriteDigest {
    bytes: u64,
    digest: String,
}

fn open_evolution_sifs_service(
    data_dir: &SinomoDataDir,
) -> Result<NetworkedDfsService<DfsEndpointTransport>, EvolutionPersistenceError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| EvolutionPersistenceError::Io {
            path: data_dir.root().display().to_string(),
            detail: source.to_string(),
        })?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let (local_node_id, endpoints) = if cluster_paths.state.exists() {
        let state = network::read_cluster_state_without_kv(&cluster_paths).map_err(|source| {
            EvolutionPersistenceError::Sifs {
                path: cluster_paths.state.display().to_string(),
                detail: source.to_string(),
            }
        })?;
        let endpoints = network::cluster_dfs_protocol_targets(&cluster_paths, &state);
        (state.local_node_id, endpoints)
    } else {
        (fallback_dfs_node_id(), Vec::new())
    };
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content")).map_err(|source| {
        EvolutionPersistenceError::Sifs {
            path: data_dir.dfs_dir().join("content").display().to_string(),
            detail: source.to_string(),
        }
    })?;
    let service = NetworkedDfsService::new(
        DfsEndpointTransport::new(
            local_node_id,
            endpoints,
            evolution_sifs_protocol_frame_sender(),
        ),
        content,
    );
    let service = if let Some(metadata) =
        open_evolution_sifs_sharded_metadata_backend(data_dir, &cluster_paths)?
    {
        service.with_metadata_backend(metadata).map_err(|source| {
            EvolutionPersistenceError::Sifs {
                path: default_dfs_metadata_shards_root(data_dir)
                    .display()
                    .to_string(),
                detail: source.to_string(),
            }
        })?
    } else {
        let metadata = open_root_local_metadata_backend(default_dfs_metadata_root(data_dir))
            .map_err(|source| EvolutionPersistenceError::Sifs {
                path: default_dfs_metadata_root(data_dir).display().to_string(),
                detail: source.to_string(),
            })?;
        service.with_metadata_backend(metadata).map_err(|source| {
            EvolutionPersistenceError::Sifs {
                path: default_dfs_metadata_root(data_dir).display().to_string(),
                detail: source.to_string(),
            }
        })?
    };
    let mut service = service;
    service
        .bootstrap_distributed_sifs()
        .map_err(|source| EvolutionPersistenceError::Sifs {
            path: "/".to_string(),
            detail: source.to_string(),
        })?;
    Ok(service)
}

fn evolution_sifs_protocol_frame_sender() -> DfsProtocolFrameSender {
    Arc::new(evolution_send_dfs_protocol_frame)
}

fn evolution_send_dfs_protocol_frame(
    endpoint: &ClusterEndpointRecord,
    command: &DfsProtocolFrameCommand,
    timeout: std::time::Duration,
) -> Result<DfsProtocolFrameResponse, DfsPeerTransportError> {
    let request = sinomo_runtime::dfs::dfs_protocol_frame_rpc_request(command)
        .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
    let response = sinomo_runtime::daemon::send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
        .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
    sinomo_runtime::dfs::decode_dfs_protocol_frame_rpc_response(&response)
        .map_err(|error| DfsPeerTransportError::new(error.to_string()))
}

fn publish_evolution_sifs_metadata_journal(
    data_dir: &SinomoDataDir,
) -> Result<(), sinomo_runtime::dfs::DfsError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let sender: DfsMetadataJournalBatchSender = Arc::new(|endpoint, command, timeout| {
        let request = sinomo_runtime::dfs::dfs_metadata_journal_batch_rpc_request(command)
            .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))?;
        let response =
            sinomo_runtime::daemon::send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
                .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))?;
        sinomo_runtime::dfs::decode_dfs_metadata_journal_batch_rpc_response(&response)
            .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))
    });
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(|endpoint, command, timeout| {
            let request = sinomo_runtime::dfs::dfs_metadata_snapshot_install_rpc_request(command)
                .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))?;
            let response =
                sinomo_runtime::daemon::send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
                    .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))?;
            sinomo_runtime::dfs::decode_dfs_metadata_snapshot_install_rpc_response(&response)
                .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))
        });
    publish_cluster_dfs_metadata_journal(
        data_dir,
        &cluster_paths,
        sender,
        snapshot_sender,
        DfsMetadataJournalReplicationOptions::default(),
    )?;
    Ok(())
}

fn open_evolution_sifs_sharded_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Option<sinomo_runtime::dfs::LocalShardedMetadataBackend>, EvolutionPersistenceError> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let file_system_id = sifs_file_system_id();
    let entries = network::read_cluster_kv_prefix_entries(
        cluster_paths,
        &format!("{}/", metadata_shards_prefix(&file_system_id)),
    )
    .map_err(|source| EvolutionPersistenceError::Sifs {
        path: metadata_shards_prefix(&file_system_id),
        detail: source.to_string(),
    })?;
    let mut shards: Vec<DfsMetadataShardConfig> = Vec::new();
    for entry in entries {
        shards.push(
            entry
                .decode()
                .map_err(|source| EvolutionPersistenceError::Sifs {
                    path: entry.key.clone(),
                    detail: source.to_string(),
                })?,
        );
    }
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok(None);
    }
    open_local_sharded_metadata_backend(default_dfs_metadata_shards_root(data_dir), shards)
        .map(Some)
        .map_err(|source| EvolutionPersistenceError::Sifs {
            path: default_dfs_metadata_shards_root(data_dir)
                .display()
                .to_string(),
            detail: source.to_string(),
        })
}

fn collect_bundle_sifs_mirror_files(
    bundle_root: &Path,
    path: &Path,
    files: &mut Vec<PathBuf>,
    skipped_paths: &mut Vec<String>,
) -> Result<(), EvolutionPersistenceError> {
    let entries = fs::read_dir(path).map_err(|source| EvolutionPersistenceError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| EvolutionPersistenceError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let entry_path = entry.path();
        let relative = bundle_relative_path(bundle_root, &entry_path);
        if should_skip_sifs_bundle_mirror_path(&relative) {
            skipped_paths.push(relative);
            continue;
        }
        if entry_path.is_dir() {
            collect_bundle_sifs_mirror_files(bundle_root, &entry_path, files, skipped_paths)?;
        } else if entry_path.is_file() {
            files.push(entry_path);
        }
    }
    Ok(())
}

fn should_skip_sifs_bundle_mirror_path(relative: &str) -> bool {
    relative == CANDIDATE_SIFS_MIRROR_RELATIVE_PATH
        || relative == "worktree"
        || relative.starts_with("worktree/")
        || relative.split('/').any(|part| part == ".git")
        || relative
            .split('/')
            .collect::<Vec<_>>()
            .windows(2)
            .any(|window| {
                window == ["sinomo", "workspaces"] || window == ["mini-swe-agent", "workspaces"]
            })
}

fn write_reader_to_sifs<R: Read>(
    service: &mut NetworkedDfsService<DfsEndpointTransport>,
    sifs_path: &str,
    mut reader: R,
) -> Result<SifsWriteDigest, EvolutionPersistenceError> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|source| EvolutionPersistenceError::Io {
            path: sifs_path.to_string(),
            detail: source.to_string(),
        })?;
    write_bytes_to_sifs(service, sifs_path, &bytes)
}

fn write_bytes_to_sifs(
    service: &mut NetworkedDfsService<DfsEndpointTransport>,
    sifs_path: &str,
    bytes: &[u8],
) -> Result<SifsWriteDigest, EvolutionPersistenceError> {
    let parent = parent_sifs_path(sifs_path)?;
    service
        .posix_mkdir_all(&parent)
        .map_err(|source| EvolutionPersistenceError::Sifs {
            path: parent.clone(),
            detail: source.to_string(),
        })?;
    let options = DfsWriteOptions::replicated_eventual(vec![service.node_id().clone()]);
    service
        .posix_write_file(sifs_path, bytes, options)
        .map_err(|source| EvolutionPersistenceError::Sifs {
            path: sifs_path.to_string(),
            detail: source.to_string(),
        })?;
    Ok(SifsWriteDigest {
        bytes: bytes.len() as u64,
        digest: format!("blake3:{}", blake3::hash(bytes).to_hex()),
    })
}

fn normalize_sifs_path(path: &str) -> Result<String, EvolutionPersistenceError> {
    let path = path.trim();
    if path.bytes().any(|byte| byte == 0) {
        return Err(EvolutionPersistenceError::Sifs {
            path: path.to_string(),
            detail: "path contains an embedded NUL byte".to_string(),
        });
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    Ok(format!("/{}", parts.join("/")))
}

fn join_sifs_path(parent: &str, child: &str) -> Result<String, EvolutionPersistenceError> {
    let mut path = normalize_sifs_path(parent)?;
    for part in child.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                return Err(EvolutionPersistenceError::Sifs {
                    path: child.to_string(),
                    detail: "relative SIFS child path cannot contain `..`".to_string(),
                });
            }
            value => {
                if path != "/" {
                    path.push('/');
                }
                path.push_str(value);
            }
        }
    }
    normalize_sifs_path(&path)
}

fn parent_sifs_path(path: &str) -> Result<String, EvolutionPersistenceError> {
    let path = normalize_sifs_path(path)?;
    let Some((parent, _)) = path.rsplit_once('/') else {
        return Ok("/".to_string());
    };
    if parent.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(parent.to_string())
    }
}

fn bundle_relative_path(bundle_root: &Path, path: &Path) -> String {
    path.strip_prefix(bundle_root)
        .unwrap_or(path)
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn current_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string())
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
    #[error("evolution config error for {key}: {detail}")]
    Config { key: String, detail: String },
    #[error("evolution SIFS artifact mirror error at {path}: {detail}")]
    Sifs { path: String, detail: String },
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
