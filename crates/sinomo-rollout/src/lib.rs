//! Rollout paths, source management, staged binary activation, and rollout policy.

mod artifact;
mod controller;
mod error;
mod paths;
mod process;
mod protocol;
mod source;
mod state;

pub type RolloutPlanId = sinomo_core::Id;
pub type ScorecardId = sinomo_core::Id;

pub use artifact::{
    activate_staged_rollout_binary, binary_should_update, current_executable_path,
    current_executable_should_update, install_binary_atomically, install_current_executable,
    restore_rollout_binary_from_backup, rollout_previous_binary_path, same_path_if_exists,
    stage_rollout_bytes, stage_rollout_file,
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
    cluster_rollout_targets_from_state, prepare_cluster_rollout_binary_request,
    prepare_cluster_rollout_build_request, prepare_rollout_cluster_targets, unique_rollout_targets,
};
pub use source::{
    LocalRolloutArtifacts, ManagedSourceStatus, RolloutStatusView, create_source_repo_bundle,
    create_source_repo_bundle_base64, deploy_managed_source_bundle, deploy_managed_source_repo,
    deploy_managed_source_repo_or_current, discover_current_git_repo, ensure_rollout_source_repo,
    managed_source_status, prepare_local_rollout_artifacts, prepare_rollout_dev_binary,
    prepare_rollout_source_bundle_base64, remove_dir_if_exists, rollout_artifact_suffix,
    rollout_build_dev_binary, rollout_remote_repo_path, rollout_source_head, rollout_status_view,
    source_heads_match, stage_local_rollout_artifacts, sync_external_repo_from_managed,
};
pub use state::{
    PendingShadowRollout, PendingShadowRolloutContext, ROLLOUT_DIAGNOSTIC_TAIL_BYTES,
    RolloutFailureDiagnostics, RolloutMainActivation, RolloutMainExitRecord,
    read_pending_shadow_rollout, read_rollout_main_activation, read_rollout_main_exit,
    read_rollout_status, read_tail_lossy, record_activation_failure, record_main_activation_staged,
    record_main_rollout_failure, record_main_rollout_failure_with_diagnostics,
    record_rollout_main_exit, record_shadow_rollout_finalized, remove_main_activation,
    remove_shadow_finalize, rollout_diagnostics_report, rollout_main_activation_path,
    rollout_main_exit_path, rollout_shadow_finalize_path, rollout_staged_main_binary,
    rollout_staged_shadow_binary, rollout_staging_dir, stage_pending_shadow_rollout, write_json,
    write_main_activation, write_rollout_status,
};

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    layout.register_sinomo_object(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/state.sobj",
        ObjectSchema::new("ClusterNodeRolloutStatus", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("status", "String")
            .field("mode", "Option<String>")
            .field("detail", "Option<String>"),
    )?;
    layout.register_sinomo_object(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/last-main-exit.sobj",
        ObjectSchema::new("RolloutMainExitRecord", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("exit_code", "Option<i32>")
            .field("signal", "Option<i32>")
            .field("requested_shutdown", "bool")
            .field("kind", "MainExitKind")
            .field("observed_at", "String"),
    )?;
    layout.register_human_text("sinomo-rollout", StorageRing::R1, "rollout/build.log")?;
    layout.register_human_text(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/last-main-exit.json",
    )?;
    layout.register_sinomo_object(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/pending-main-rollout.sobj",
        ObjectSchema::new("RolloutMainActivation", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_sinomo_object(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/pending-shadow-rollout.sobj",
        ObjectSchema::new("PendingShadowRollout", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    for (relative_path, reason) in [
        ("source", "managed or imported source repositories"),
        ("bin", "installed runtime binaries"),
        ("rollout/staged", "staged rollout build artifacts"),
        (
            "rollout/logs",
            "operator-readable rollout logs and diagnostics",
        ),
    ] {
        layout.register_escape_path("sinomo-rollout", StorageRing::R1, relative_path, reason)?;
    }
    Ok(())
}
