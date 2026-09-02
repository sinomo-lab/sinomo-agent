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
pub const EVOLUTION_CLI_ROUTE: &str = "/evolution/cli/run";
pub const EVOLUTION_RPC_ROUTES: &[&str] = &[
    EVOLUTION_DASHBOARD_ROUTE,
    EVOLUTION_START_ROUTE,
    EVOLUTION_DECISION_ROUTE,
    EVOLUTION_CLI_ROUTE,
];
pub const EVOLUTION_READ_CAPABILITY: &str = "evolution.app.read";
pub const EVOLUTION_WRITE_CAPABILITY: &str = "evolution.app.write";
pub const EVOLUTION_DASHBOARD_OPERATION: &str = "evolution.dashboard.read";
pub const EVOLUTION_START_OPERATION: &str = "evolution.job.start";
pub const EVOLUTION_DECISION_OPERATION: &str = "evolution.job.decision";
pub const EVOLUTION_CLI_OPERATION: &str = "evolution.cli.run";
pub const EVOLUTION_OPERATIONS: &[&str] = &[
    EVOLUTION_DASHBOARD_OPERATION,
    EVOLUTION_START_OPERATION,
    EVOLUTION_DECISION_OPERATION,
    EVOLUTION_CLI_OPERATION,
];

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionDashboard {
    pub source: String,
    pub jobs: Vec<EvolutionJobSummary>,
    pub error: Option<String>,
}

impl EvolutionDashboard {
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
