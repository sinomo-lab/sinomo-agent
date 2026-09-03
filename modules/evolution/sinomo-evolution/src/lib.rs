//! Independent native-dylib implementation of Sinomo self-evolution.

mod agent_package;
mod app_api;
pub mod cli;
mod cli_runtime;
mod engine;
mod module_abi;
pub mod module_evidence;
pub mod persistence;
#[cfg(test)]
mod test_cases;
pub mod tools;
mod types;

pub use agent_package::{EVOLUTION_AGENT_ID, evolution_agent_definition};
pub use app_api::{app_evolution_dashboard, run_app_evolution_decision, run_app_evolution_job};
pub use cli::*;
pub use cli_runtime::*;
pub use engine::*;
pub use module_abi::evolution_native_module_descriptor;
pub use module_evidence::*;
pub use persistence::*;
pub use tools::*;
pub use types::*;

pub type EvolutionJobId = sinomo_runtime::core::Id;
pub type TaskId = sinomo_runtime::core::Id;
pub type WorkspaceId = sinomo_runtime::core::Id;

pub const EVOLUTION_JOB_SCOPED_EVENT_KIND: &str = "evolution.job.scoped";
pub const EVOLUTION_TOOL_EXECUTED_EVENT_KIND: &str = "evolution.tool.executed";
pub const EVOLUTION_METRIC_REGRESSION_EVENT_KIND: &str = "evolution.metric.regression";
pub const EVOLUTION_REPLAY_FAILURE_EVENT_KIND: &str = "evolution.replay.failure";
pub const EVOLUTION_SCORECARD_STALE_EVENT_KIND: &str = "evolution.scorecard.stale";

pub fn evolution_module_id() -> sinomo_runtime::module::ModuleId {
    sinomo_runtime::module::ModuleId::from_legacy_named(
        sinomo_app_contract::evolution::EVOLUTION_MODULE_ID,
    )
    .expect("valid evolution module id")
}
