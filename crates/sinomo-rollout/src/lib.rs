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
    stage_rollout_bytes, stage_rollout_file, stage_rollout_file_with_retry,
};
pub use controller::{
    AppliedRolloutTransition, DEFAULT_SHADOW_TELEMETRY_LIMIT, RolloutAction, RolloutController,
    RolloutDecision, RolloutObservation, RolloutRuntimeState, RolloutStage,
    RolloutTelemetryAdvanceOptions, RolloutTelemetryAdvanceResult, RolloutTelemetryAdvanceStatus,
    VerifiedArtifactRolloutPreparation, VerifiedArtifactRolloutRequest,
    canary_telemetry_scorecard_from_event_store, canary_telemetry_scorecard_from_events,
    shadow_telemetry_scorecard_from_event_store, shadow_telemetry_scorecard_from_events,
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
    create_source_repo_bundle_bytes, deploy_managed_source_bundle, deploy_managed_source_repo,
    deploy_managed_source_repo_or_current, discover_current_git_repo, ensure_rollout_source_repo,
    managed_source_status, prepare_local_rollout_artifacts, prepare_rollout_dev_binary,
    prepare_rollout_source_bundle, remove_dir_if_exists, rollout_artifact_suffix,
    rollout_build_dev_binary, rollout_remote_repo_path, rollout_source_head, rollout_status_view,
    source_heads_match, stage_local_rollout_artifacts, sync_external_repo_from_managed,
};
pub use state::{
    PendingShadowRollout, PendingShadowRolloutContext, ROLLOUT_DIAGNOSTIC_TAIL_BYTES,
    RecordedRolloutTransition, RolloutFailureDiagnostics, RolloutMainActivation,
    RolloutMainExitRecord, RolloutTelemetryAdvanceState, RolloutTelemetryGateConfig,
    RolloutTransitionRecord, read_pending_shadow_rollout, read_rollout_main_activation,
    read_rollout_main_exit, read_rollout_runtime_state, read_rollout_status,
    read_rollout_telemetry_advance_state, read_rollout_telemetry_gate_config, read_tail_lossy,
    record_activation_failure, record_main_activation_staged, record_main_rollout_failure,
    record_main_rollout_failure_with_diagnostics, record_rollout_main_exit,
    record_rollout_transition, record_shadow_rollout_finalized, remove_main_activation,
    remove_shadow_finalize, rollout_diagnostics_report, rollout_main_activation_path,
    rollout_main_exit_path, rollout_runtime_state_path, rollout_shadow_finalize_path,
    rollout_staged_main_binary, rollout_staged_shadow_binary, rollout_staging_dir,
    rollout_telemetry_advance_state_path, rollout_telemetry_gate_config_path,
    rollout_transition_history_dir, rollout_transition_record, stage_pending_shadow_rollout,
    write_json, write_main_activation, write_rollout_runtime_state, write_rollout_status,
    write_rollout_telemetry_advance_state, write_rollout_telemetry_gate_config,
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
    layout.register_sinomo_object(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/runtime-state.sobj",
        ObjectSchema::new("RolloutRuntimeState", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("plan", "RolloutPlan")
            .field("loader_state", "ModuleLoaderState")
            .field("stage", "RolloutStage")
            .field("current_weight_bps", "u16"),
    )?;
    layout.register_sinomo_object(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/telemetry-advance-state.sobj",
        ObjectSchema::new(
            "RolloutTelemetryAdvanceState",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("rollout_plan_id", "Id")
        .field("module_id", "Id")
        .field("candidate_artifact_id", "Id")
        .field("telemetry_kind", "String")
        .field("last_consumed_sequence", "Option<EventSequence>")
        .field("last_transition_record_id", "Option<Id>")
        .field("updated_at", "String"),
    )?;
    layout.register_sinomo_object(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/telemetry-gates.sobj",
        ObjectSchema::new(
            "RolloutTelemetryGateConfig",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("automatic_promotion_enabled", "bool")
        .field("event_limit", "usize")
        .field("minimum_event_count", "u64")
        .field("maximum_automatic_weight_bps", "Option<u16>")
        .field("maximum_candidate_latency_p95_ms", "Option<u64>"),
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
        (
            "rollout/history",
            "durable rollout promotion and rollback decision records",
        ),
    ] {
        layout.register_escape_path("sinomo-rollout", StorageRing::R1, relative_path, reason)?;
    }
    Ok(())
}
