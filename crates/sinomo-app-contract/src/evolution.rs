//! Stable self-evolution module commands and app-facing projections.

use serde::{Deserialize, Serialize};

pub const EVOLUTION_MODULE_ID: &str = "module.evolution";
pub const EVOLUTION_MODULE_OWNER: &str = "sinomo-evolution";
pub const EVOLUTION_INTERFACE_ID: &str = "sinomo.evolution";
pub const EVOLUTION_INTERFACE_VERSION: &str = "1.0.0";
pub const EVOLUTION_MODULE_INTERFACE_VERSION: &str = "sinomo.evolution.v1";
pub const EVOLUTION_SERVICE_ID: &str = "evolution";
pub const EVOLUTION_RPC_CATEGORY: &str = "evolution.app";
pub const EVOLUTION_ROUTE_AUTH_SCOPE: &str = "local-command";
pub const EVOLUTION_DASHBOARD_ROUTE: &str = "/evolution/app/dashboard";
pub const EVOLUTION_START_ROUTE: &str = "/evolution/app/start";
pub const EVOLUTION_DECISION_ROUTE: &str = "/evolution/app/decision";
pub const EVOLUTION_GOAL_CREATE_ROUTE: &str = "/evolution/app/goals/create";
pub const EVOLUTION_REVISION_RECORD_ROUTE: &str = "/evolution/app/revisions/record";
pub const EVOLUTION_GOAL_CONTROL_ROUTE: &str = "/evolution/app/goals/control";
pub const EVOLUTION_WORKFLOW_UPDATE_ROUTE: &str = "/evolution/app/workflow/update";
pub const EVOLUTION_CLI_ROUTE: &str = "/evolution/cli/run";
pub const EVOLUTION_RPC_ROUTES: &[&str] = &[
    EVOLUTION_DASHBOARD_ROUTE,
    EVOLUTION_START_ROUTE,
    EVOLUTION_DECISION_ROUTE,
    EVOLUTION_GOAL_CREATE_ROUTE,
    EVOLUTION_REVISION_RECORD_ROUTE,
    EVOLUTION_GOAL_CONTROL_ROUTE,
    EVOLUTION_WORKFLOW_UPDATE_ROUTE,
    EVOLUTION_CLI_ROUTE,
];
pub const EVOLUTION_READ_CAPABILITY: &str = "evolution.app.read";
pub const EVOLUTION_WRITE_CAPABILITY: &str = "evolution.app.write";
pub const EVOLUTION_DASHBOARD_OPERATION: &str = "evolution.dashboard.read";
pub const EVOLUTION_START_OPERATION: &str = "evolution.job.start";
pub const EVOLUTION_DECISION_OPERATION: &str = "evolution.job.decision";
pub const EVOLUTION_GOAL_CREATE_OPERATION: &str = "evolution.goal.create";
pub const EVOLUTION_REVISION_RECORD_OPERATION: &str = "evolution.revision.record";
pub const EVOLUTION_GOAL_CONTROL_OPERATION: &str = "evolution.goal.control";
pub const EVOLUTION_WORKFLOW_UPDATE_OPERATION: &str = "evolution.workflow.update";
pub const EVOLUTION_CLI_OPERATION: &str = "evolution.cli.run";
pub const EVOLUTION_OPERATIONS: &[&str] = &[
    EVOLUTION_DASHBOARD_OPERATION,
    EVOLUTION_START_OPERATION,
    EVOLUTION_DECISION_OPERATION,
    EVOLUTION_GOAL_CREATE_OPERATION,
    EVOLUTION_REVISION_RECORD_OPERATION,
    EVOLUTION_GOAL_CONTROL_OPERATION,
    EVOLUTION_WORKFLOW_UPDATE_OPERATION,
    EVOLUTION_CLI_OPERATION,
];

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionDashboard {
    pub source: String,
    /// Goal-centric projection. Each goal owns its complete revision DAG and
    /// orchestration/resource state. `jobs` remains for v1 clients.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub goals: Vec<EvolutionGoalSummary>,
    pub jobs: Vec<EvolutionJobSummary>,
    pub error: Option<String>,
}

impl EvolutionDashboard {
    pub fn active_goal_count(&self) -> usize {
        self.goals
            .iter()
            .filter(|goal| matches!(goal.status.as_str(), "planning" | "running"))
            .count()
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionGoalSummary {
    pub id: String,
    pub objective: String,
    pub status: String,
    pub evolution_agent_id: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metric_definitions: Vec<EvolutionMetricDefinitionSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revisions: Vec<EvolutionRevisionSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edges: Vec<EvolutionRevisionEdgeSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub head_revision_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tasks: Vec<EvolutionTaskSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_runs: Vec<EvolutionAgentRunSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<EvolutionResourceSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_events: Vec<EvolutionEventSummary>,
    pub event_count: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionEventSummary {
    pub id: String,
    pub kind: String,
    pub occurred_at: String,
    pub revision_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_run_id: Option<String>,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionMetricDefinitionSummary {
    pub id: String,
    pub label: String,
    pub direction: String,
    pub unit: String,
    pub target: Option<String>,
    pub required: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionRevisionSummary {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_ids: Vec<String>,
    pub status: String,
    pub summary: String,
    pub content_kind: String,
    pub content_identity: String,
    pub content_hash: String,
    pub created_at: String,
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<EvolutionArtifactSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<EvolutionMetricObservationSummary>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionRevisionEdgeSummary {
    pub parent_id: String,
    pub child_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionArtifactSummary {
    pub id: String,
    pub kind: String,
    pub uri: String,
    pub content_hash: String,
    pub role: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionMetricObservationSummary {
    pub metric_id: String,
    pub value: String,
    pub sample_count: Option<u64>,
    pub outcome: Option<String>,
    pub evaluator: String,
    pub artifact_id: String,
    pub measured_at: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionTaskSummary {
    pub id: String,
    pub title: String,
    pub status: String,
    pub revision_id: Option<String>,
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependency_task_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subgoals: Vec<String>,
    pub assigned_agent_run_id: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionAgentRunSummary {
    pub id: String,
    pub agent_id: String,
    pub role: String,
    pub status: String,
    pub parent_agent_run_id: Option<String>,
    pub task_id: Option<String>,
    pub session_ref: Option<String>,
    pub summary: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionResourceSummary {
    pub id: String,
    pub revision_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_run_id: Option<String>,
    pub resource: String,
    pub quantity: u64,
    pub unit: String,
    pub monetary_microunits: Option<u64>,
    pub currency: Option<String>,
    pub confidence: String,
    pub evidence_ref: String,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionPhase {
    #[default]
    Active,
    AwaitingDecision,
    Passed,
    Failed,
    Recovered,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionDecisionSummary {
    pub outcome: String,
    pub decided_at: String,
    pub decided_by: String,
    pub note: String,
    pub preflight_passed: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionStartRequest {
    pub objective: String,
    pub target_ring: String,
    pub intended_paths: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionGoalCreateRequest {
    pub objective: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evolution_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<EvolutionMetricDefinitionInput>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionGoalCreateResponse {
    pub accepted: bool,
    pub goal_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionMetricDefinitionInput {
    pub id: String,
    pub label: String,
    pub direction: String,
    pub unit: String,
    pub target: Option<EvolutionMetricValueInput>,
    #[serde(default)]
    pub required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionRevisionRecordRequest {
    pub goal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_revision_ids: Vec<String>,
    pub content: EvolutionRevisionContentInput,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<EvolutionArtifactInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<EvolutionMetricObservationInput>,
    pub status: String,
    pub summary: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionRevisionRecordResponse {
    pub accepted: bool,
    pub revision_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum EvolutionRevisionContentInput {
    GitCommit {
        repository: String,
        commit: String,
    },
    DirectorySnapshot {
        path: String,
        digest: String,
        manifest_ref: Option<String>,
    },
    InlineContent {
        media_type: String,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        digest: Option<String>,
    },
    Composite {
        base_revision_id: String,
        patch_artifact_id: String,
        digest: String,
    },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionArtifactInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub kind: String,
    pub uri: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub media_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema: String,
    pub role: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionMetricObservationInput {
    pub metric_id: String,
    pub value: EvolutionMetricValueInput,
    pub sample_count: Option<u64>,
    pub outcome: Option<String>,
    pub evaluator: String,
    pub artifact_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "kebab-case")]
pub enum EvolutionMetricValueInput {
    Boolean(bool),
    Integer(i64),
    Decimal(String),
    Text(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionGoalControlRequest {
    pub goal_id: String,
    pub action: EvolutionGoalControlAction,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionGoalControlAction {
    Pause,
    Resume,
    Complete,
    Cancel,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionWorkflowUpdateRequest {
    pub goal_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tasks: Vec<EvolutionTaskInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_runs: Vec<EvolutionAgentRunInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<EvolutionResourceInput>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionTaskInput {
    pub id: String,
    pub title: String,
    pub status: String,
    pub revision_id: Option<String>,
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependency_task_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subgoals: Vec<String>,
    pub assigned_agent_run_id: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionAgentRunInput {
    pub id: String,
    pub agent_id: String,
    pub role: String,
    pub status: String,
    pub parent_agent_run_id: Option<String>,
    pub task_id: Option<String>,
    pub session_ref: Option<String>,
    pub summary: String,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionResourceInput {
    pub id: String,
    pub revision_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_run_id: Option<String>,
    pub resource: String,
    pub quantity: u64,
    pub unit: String,
    pub monetary_microunits: Option<u64>,
    pub currency: Option<String>,
    pub confidence: String,
    pub evidence_ref: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionDecisionRequest {
    pub job_id: String,
    pub action: EvolutionDecisionAction,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionDashboardRequest;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionDashboardResponse {
    pub dashboard: EvolutionDashboard,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionMutationResponse {
    pub accepted: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionCliRequest {
    pub args: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionCliResponse {
    pub stdout: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_and_commands_round_trip() {
        let request = EvolutionDecisionRequest {
            job_id: "job-1".to_string(),
            action: EvolutionDecisionAction::Approve,
        };
        let encoded = serde_json::to_vec(&request).expect("encode decision");
        let decoded: EvolutionDecisionRequest =
            serde_json::from_slice(&encoded).expect("decode decision");
        assert_eq!(decoded, request);
    }
}
