//! Rollout paths, source management, staged binary activation, and rollout policy.

mod artifact;
mod controller;
mod error;
mod paths;
mod process;
mod protocol;
mod source;
mod state;

pub use artifact::{
    activate_staged_rollout_binary, install_binary_atomically, restore_rollout_binary_from_backup,
    rollout_previous_binary_path, same_path_if_exists, stage_rollout_bytes, stage_rollout_file,
};
pub use controller::{
    RolloutAction, RolloutController, RolloutDecision, RolloutObservation, RolloutRuntimeState,
    RolloutStage,
};
pub use error::RolloutError;
pub use paths::{SelfRolloutPaths, SelfRolloutPlan, ensure_rollout_layout, executable_name};
pub use process::{MainExitKind, MainExitObservation, classify_main_exit};
pub use protocol::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
};
pub use source::{
    ManagedSourceStatus, create_source_repo_bundle, deploy_managed_source_bundle,
    deploy_managed_source_repo, discover_current_git_repo, managed_source_status,
    remove_dir_if_exists, sync_external_repo_from_managed,
};
pub use state::{
    PendingShadowRollout, PendingShadowRolloutContext, RolloutFailureDiagnostics,
    RolloutMainActivation, RolloutMainExitRecord, read_pending_shadow_rollout,
    read_rollout_main_activation, read_rollout_main_exit, read_rollout_status,
    record_activation_failure, record_main_activation_staged, record_main_rollout_failure,
    record_main_rollout_failure_with_diagnostics, record_rollout_main_exit,
    record_shadow_rollout_finalized, remove_main_activation, remove_shadow_finalize,
    rollout_main_activation_path, rollout_main_exit_path, rollout_shadow_finalize_path,
    rollout_staged_main_binary, rollout_staged_shadow_binary, rollout_staging_dir,
    stage_pending_shadow_rollout, write_json, write_main_activation, write_rollout_status,
};
