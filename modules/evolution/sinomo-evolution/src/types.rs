use crate::EvolutionJobId;
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{EvaluationGate, EvaluationOutcome, RollbackPolicy, TrustRing};
use sinomo_runtime::module::ArtifactId;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionJob {
    pub job_id: EvolutionJobId,
    pub objective: String,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_module_id: Option<sinomo_runtime::core::Id>,
    pub edit_scope: EditScope,
    pub verification: VerificationPlan,
    pub recovery: RecoveryPlan,
    pub scope: EvolutionScope,
    pub status: EvolutionJobStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_gates: Vec<EvaluationGate>,
    pub rollback_policy: RollbackPolicy,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EditScope {
    pub workspace_root: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intended_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub notes: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionScope {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<sinomo_runtime::core::Id>,
}

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

impl From<VerificationPlan> for sinomo_runtime::eval::VerificationPlan {
    fn from(plan: VerificationPlan) -> Self {
        Self {
            commands: plan.commands.into_iter().map(Into::into).collect(),
            module_replay_reports: plan
                .module_replay_reports
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<VerificationCommand> for sinomo_runtime::eval::VerificationCommand {
    fn from(command: VerificationCommand) -> Self {
        Self {
            name: command.name,
            command: command.command,
            expected_exit_code: command.expected_exit_code,
            gate: command.gate,
        }
    }
}

impl From<ModuleReplayReportRef> for sinomo_runtime::eval::ModuleReplayReportRef {
    fn from(report: ModuleReplayReportRef) -> Self {
        Self {
            name: report.name,
            report_path: report.report_path,
            baseline_artifact_id: report.baseline_artifact_id,
            candidate_artifact_id: report.candidate_artifact_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RecoveryPlan {
    pub strategy: RecoveryStrategy,
    pub note: String,
}

impl Default for RecoveryPlan {
    fn default() -> Self {
        Self {
            strategy: RecoveryStrategy::VmSnapshotRestore,
            note: "restore the disposable VM snapshot or manually repair the candidate worktree"
                .to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RecoveryStrategy {
    GitRevert,
    GitBranchReset,
    VmSnapshotRestore,
    ManualRepair,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MutationPacket {
    pub job_id: EvolutionJobId,
    pub objective: String,
    pub target_ring: TrustRing,
    pub edit_scope: EditScope,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relevant_files: Vec<FileExcerpt>,
    pub verification: VerificationPlan,
    pub output_contract: MutationOutputContract,
    pub recovery: RecoveryPlan,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct FileExcerpt {
    pub path: String,
    pub content: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MutationOutputContract {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_sections: Vec<String>,
    pub prefer_patch: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub notes: String,
}

impl Default for MutationOutputContract {
    fn default() -> Self {
        Self {
            expected_sections: vec![
                "changed files".to_string(),
                "patch summary".to_string(),
                "verification notes".to_string(),
                "experiment notes".to_string(),
                "recovery notes".to_string(),
            ],
            prefer_patch: false,
            notes: "write directly in the candidate workspace when using a coding-agent provider"
                .to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateBundleManifest {
    pub job_id: EvolutionJobId,
    pub bundle_path: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub mutation_packet_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_output_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<sinomo_module_api::sessions::SessionExecutionRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_summary_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub benchmark_results_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_usage_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_record_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changed_files_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_summary_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_logs: Vec<CommandLogRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateBundleContentManifest {
    pub bundle_path: String,
    pub generated_at: String,
    pub bundle_content_hash: String,
    pub hash_algorithm: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<CandidateBundleFileDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateBundleFileDigest {
    pub path: String,
    pub bytes: u64,
    pub digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CandidateBundleAttestation {
    pub statement_type: String,
    pub predicate_type: String,
    pub subject: CandidateBundleSubject,
    pub predicate: CandidateBundlePredicate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateBundleSubject {
    pub name: String,
    pub digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CandidateBundlePredicate {
    pub job_id: EvolutionJobId,
    pub objective: String,
    pub target_ring: TrustRing,
    pub produced_by: String,
    pub mutation_worker: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub source_workspace: String,
    pub candidate_worktree: String,
    pub candidate_branch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_outcome: Option<EvaluationOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<String>,
    pub generated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateBundleSifsMirrorReport {
    pub bundle_path: String,
    pub sifs_bundle_path: String,
    pub sifs_report_path: String,
    pub generated_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mirrored_files: Vec<CandidateBundleSifsMirrorFile>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_publish_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateBundleSifsMirrorFile {
    pub relative_path: String,
    pub sifs_path: String,
    pub bytes: u64,
    pub digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CommandLogRef {
    pub name: String,
    pub command: String,
    pub log_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionJobStatus {
    Created,
    PacketWritten,
    MutationStarted,
    MutationFinished,
    Verified,
    AwaitingApproval,
    Approved,
    Revised,
    Restored,
    Failed,
    Observed,
    Scoped,
    Proposed,
    Implemented,
    Built,
    Evaluated,
    PolicyApproved,
    Staged,
    Shadowed,
    Canary,
    Promoted,
    Rejected,
    BuildFailed,
    EvalFailed,
    Denied,
    RolledBack,
    Reverted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateDecision {
    pub job_id: EvolutionJobId,
    pub outcome: CandidateDecisionOutcome,
    #[serde(with = "time::serde::rfc3339")]
    pub decided_at: OffsetDateTime,
    pub decided_by: String,
    pub resulting_status: EvolutionJobStatus,
    pub objective: String,
    pub worktree_path: String,
    pub bundle_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_outcome: Option<EvaluationOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
    pub recovery: RecoveryPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_preflight: Option<CandidateApprovalPreflightReport>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
}

impl CandidateDecision {
    pub fn new(
        job: &EvolutionJob,
        outcome: CandidateDecisionOutcome,
        decided_by: impl Into<String>,
        worktree_path: impl Into<String>,
        bundle_path: impl Into<String>,
        verification_outcome: Option<EvaluationOutcome>,
        changed_files: Vec<String>,
        note: impl Into<String>,
    ) -> Self {
        Self {
            job_id: job.job_id.clone(),
            outcome: outcome.clone(),
            decided_at: OffsetDateTime::now_utc(),
            decided_by: decided_by.into(),
            resulting_status: outcome.resulting_status(),
            objective: job.objective.clone(),
            worktree_path: worktree_path.into(),
            bundle_path: bundle_path.into(),
            verification_outcome,
            changed_files,
            recovery: job.recovery.clone(),
            approval_preflight: None,
            note: note.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateApprovalPreflightReport {
    pub objective: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_outcome: Option<EvaluationOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub benchmark_outcome: Option<EvaluationOutcome>,
    pub diff_summary: String,
    pub override_used: bool,
    pub passed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<CandidateApprovalPreflightCheck>,
    pub recovery_note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateApprovalPreflightCheck {
    pub name: String,
    pub outcome: EvaluationOutcome,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CandidateDecisionOutcome {
    Approved,
    Rejected,
    Restored,
}

impl CandidateDecisionOutcome {
    pub fn resulting_status(&self) -> EvolutionJobStatus {
        match self {
            Self::Approved => EvolutionJobStatus::Approved,
            Self::Rejected => EvolutionJobStatus::Rejected,
            Self::Restored => EvolutionJobStatus::Restored,
        }
    }
}
