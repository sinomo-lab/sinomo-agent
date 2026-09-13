//! Rollout paths, source management, staged binary activation, and rollout policy.

mod activation_network;
mod artifact;
mod binary_ingress;
mod client_protocol;
mod cluster_kv;
mod controller;
mod coordinator;
mod desired_reactor;
mod error;
mod module_load;
mod native_install;
mod paths;
mod process;
mod profile;
mod progress;
mod protocol;
mod service;
mod source;
mod state;
mod supervision;
mod target;
mod telemetry;

pub type RolloutPlanId = sinomo_core::Id;
pub type ScorecardId = sinomo_core::Id;

pub const ROLLOUT_PROTOCOL: &str = "sinomo.rollout.v1";
pub const SINOMO_ROLLOUT_MODULE_LEGACY_ID: &str = "module.rollout";
pub const SINOMO_ROLLOUT_MODULE_OWNER: &str = "sinomo-rollout";
pub const SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY: &str = "sinomo-rollout";
pub const ROLLOUT_TRANSITION_COMPLETED_EVENT_KIND: &str = "rollout.transition.completed";
pub const ROLLOUT_RPC_CATEGORY: &str = "rollout";
pub const ROLLOUT_DIAGNOSTICS_ROUTE: &str = "/rollout/diagnostics";
pub const ROLLOUT_UPLOAD_STATUS_ROUTE: &str = "/rollout/upload/status";
pub const ROLLOUT_BINARY_ROUTE: &str = "/rollout/binary";
pub const ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE: &str = "/rollout/native-dylib-module";
pub const ROLLOUT_BUILD_ROUTE: &str = "/rollout/build";
pub const ROLLOUT_FINALIZE_SHADOW_ROUTE: &str = "/rollout/finalize-shadow";
pub const ROLLOUT_CLEAR_STATUS_ROUTE: &str = "/rollout/clear-status";
pub const SHADOW_MAIN_HOST_ENV: &str = "SINOMO_SHADOW_MAIN_HOST";
pub const SHADOW_MAIN_PORT_ENV: &str = "SINOMO_SHADOW_MAIN_PORT";
pub const SHADOW_MAIN_BINARY_ENV: &str = "SINOMO_SHADOW_MAIN_BINARY";
pub const SHADOW_START_DELAY_MS_ENV: &str = "SINOMO_SHADOW_START_DELAY_MS";
pub const ROLLOUT_OBJECT_RPC_ROUTES: &[&str] = &[
    ROLLOUT_DIAGNOSTICS_ROUTE,
    ROLLOUT_UPLOAD_STATUS_ROUTE,
    ROLLOUT_BINARY_ROUTE,
    ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
    ROLLOUT_BUILD_ROUTE,
    ROLLOUT_FINALIZE_SHADOW_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RolloutActivationStrategy {
    MainOnlyShadowBootstrap,
    ShadowSupervised,
}

pub use activation_network::{
    rollout_bind_covers_manual_tcp_endpoint, select_rollout_main_host, select_rollout_shadow_host,
};
pub use artifact::{
    ROLLOUT_RECOVERY_BACKUP_RETENTION, activate_staged_rollout_binary, binary_should_update,
    current_executable_path, current_executable_should_update, install_binary_atomically,
    install_current_executable, prune_rollout_binary_recovery_backups,
    restore_rollout_binary_from_backup, rollout_previous_binary_path, same_path_if_exists,
    stage_rollout_bytes, stage_rollout_bytes_pair, stage_rollout_file, stage_rollout_file_pair,
    stage_rollout_file_with_retry,
};
pub use binary_ingress::{
    PreparedRolloutBinaryActivation, RolloutBinaryIngress, RolloutUploadStatus,
    complete_rollout_binary_activation, read_rollout_upload_status, receive_rollout_binary_ingress,
    rollout_upload_artifact_path, rollout_upload_manifest_path, validate_rollout_binary_manifest,
    validate_rollout_upload_component, verify_rollout_uploaded_artifact,
    verify_rollout_uploaded_module,
};
pub use client_protocol::{rollout_binary_rpc_call, rollout_build_rpc_call};
pub use cluster_kv::{
    build_cluster_module_rollout_desired_put_op,
    build_cluster_module_rollout_reconciliation_put_op, build_cluster_module_rollout_status_put_op,
    build_cluster_rollout_desired_put_op, project_cluster_module_rollout_desired_from_snapshot,
    project_cluster_module_rollout_reconciliations_from_snapshot,
    project_cluster_module_rollout_statuses_from_snapshot,
    project_cluster_rollout_desired_from_snapshot,
};
pub use controller::{
    AppliedRolloutTransition, DEFAULT_SHADOW_TELEMETRY_LIMIT, RolloutAction, RolloutController,
    RolloutDecision, RolloutObservation, RolloutRuntimeState, RolloutStage,
    RolloutTelemetryAdvanceOptions, RolloutTelemetryAdvanceResult, RolloutTelemetryAdvanceStatus,
    VerifiedArtifactRolloutPreparation, VerifiedArtifactRolloutRequest,
    canary_telemetry_scorecard_from_event_store, canary_telemetry_scorecard_from_events,
    rollout_telemetry_event_kind_is_relevant, shadow_telemetry_scorecard_from_event_store,
    shadow_telemetry_scorecard_from_events,
};
pub use coordinator::{
    RolloutArtifactSource, RolloutCoordinatorPlan, RolloutCoordinatorTarget,
    cache_rollout_coordinator_artifact, load_rollout_coordinator_plan, lock_rollout_coordinator,
    rollout_coordinator_plan_path, save_rollout_coordinator_plan,
    unfinished_rollout_coordinator_plan_ids,
};
pub use desired_reactor::{
    DESIRED_ROLLOUT_RETRY_MAX, DESIRED_ROLLOUT_RETRY_MIN,
    DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS, DESIRED_ROLLOUT_WATCH_CANCEL_TIMEOUT,
    DESIRED_ROLLOUT_WATCH_DRAIN_TIMEOUT, DESIRED_ROLLOUT_WATCH_DRAIN_WAIT,
    DESIRED_ROLLOUT_WATCH_READ_TIMEOUT, DESIRED_ROLLOUT_WATCH_RECREATE_AFTER_FAILURES,
    DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_ATTEMPT, DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_BUDGET,
    DESIRED_ROLLOUT_WATCH_START_TIMEOUT, DesiredRolloutReactor, DesiredRolloutReactorFuture,
    DesiredRolloutReactorHost, DesiredRolloutReconcileDecision, DesiredRolloutReconcileWork,
    desired_rollout_failure_is_hard, desired_rollout_reconcile_decision,
    desired_rollout_stale_handoff_failure_status, desired_rollout_stale_handoff_should_be_recorded,
    record_desired_rollout_stale_handoff_failure_if_needed, rollout_progress_status_is_stale,
};
pub use error::RolloutError;
pub use module_load::{
    LoadDecision, LoadRequest, LoaderAction, ModuleLoadError, ModuleLoader, ModulePolicyViolation,
};
pub use native_install::{
    install_cluster_rollout_native_dylib_modules, install_local_rollout_native_dylib_modules,
    native_dylib_module_install_request,
};
pub use paths::{
    NativeDylibModulePlatformPolicy, NativeDylibPlatformReconciliationReport, SelfRolloutPaths,
    SelfRolloutPlan, discover_native_dylib_module_platform_policies, ensure_rollout_layout,
    executable_name, native_dylib_library_name, native_dylib_loader_state_is_host_compatible,
    reconcile_rollout_native_dylib_platforms,
};
pub use process::{MainExitKind, MainExitObservation, classify_main_exit};
pub use profile::*;
pub use progress::{
    CLUSTER_ROLLOUT_BUILD_HEARTBEAT_INTERVAL, CLUSTER_ROLLOUT_PROGRESS_TOTAL,
    ClusterRolloutProgressUpdate, cluster_rollout_progress_status, rollout_timestamp_now,
};
pub use protocol::{
    ClusterRolloutBinaryManifest, ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse,
    ClusterRolloutBuildRequest, ClusterRolloutNativeDylibModuleArtifact,
    ClusterRolloutNativeDylibModuleRequest, ClusterRolloutNativeDylibModuleResponse,
    ClusterRolloutNativeDylibModuleUploadSpec, ClusterRolloutUploadArtifactSpec,
    prepare_cluster_rollout_binary_manifest, prepare_cluster_rollout_binary_manifest_for_target,
    prepare_cluster_rollout_build_request, prepare_cluster_rollout_native_dylib_module_request,
    unique_rollout_targets,
};
pub use service::{
    HostedRolloutService, RolloutHostFuture, RolloutService, RolloutServiceHost, component_factory,
    component_factory_with_telemetry, register_host_services, register_services,
};
pub use source::{
    LocalRolloutArtifacts, LocalRolloutNativeDylibModuleArtifact, ManagedSourceDeployOptions,
    ManagedSourceStatus, RolloutBuiltArtifacts, RolloutStatusView, create_source_repo_bundle,
    create_source_repo_bundle_bytes, deploy_managed_source_bundle,
    deploy_managed_source_bundle_with_options, deploy_managed_source_repo,
    deploy_managed_source_repo_or_current, deploy_managed_source_repo_or_current_with_options,
    deploy_managed_source_repo_with_options, discover_current_git_repo, ensure_rollout_source_repo,
    ensure_rollout_source_repo_with_options, managed_source_status,
    prepare_local_rollout_artifacts, prepare_local_rollout_artifacts_with_options,
    prepare_rollout_artifacts, prepare_rollout_binary, prepare_rollout_source_bundle,
    remove_dir_if_exists, resolve_rollout_source_commit, rollout_artifact_suffix,
    rollout_build_artifacts, rollout_build_binary, rollout_build_native_dylib_module,
    rollout_remote_repo_path, rollout_source_head, rollout_status_view, source_heads_match,
    stage_local_rollout_artifacts, sync_external_repo_from_managed,
};
pub use state::{
    NativeDylibLoaderRollbackLifecycle, NativeDylibLoaderStateRollback, NativeDylibRolloutGuard,
    PendingShadowRollout, PendingShadowRolloutContext, ROLLOUT_DIAGNOSTIC_TAIL_BYTES,
    ROLLOUT_TELEMETRY_GATE_CONFIG_KEY, RecordedRolloutTransition, RolloutActivationGuard,
    RolloutFailureDiagnostics, RolloutMainActivation, RolloutMainExitRecord,
    RolloutTelemetryAdvanceState, RolloutTelemetryGateConfig, RolloutTransitionRecord,
    acquire_native_dylib_rollout_guard, acquire_rollout_activation_guard, clear_rollout_status,
    native_dylib_rollout_lock_path, read_object, read_pending_shadow_rollout,
    read_rollout_main_activation, read_rollout_main_exit, read_rollout_runtime_state,
    read_rollout_status, read_rollout_telemetry_advance_state, read_rollout_telemetry_gate_config,
    read_tail_lossy, record_activation_failure, record_main_activation_staged,
    record_main_rollout_failure, record_main_rollout_failure_with_diagnostics,
    record_rollout_main_exit, record_rollout_transition, record_shadow_rollout_finalized,
    recover_unresolved_native_dylib_loader_rollback_intents, register_config_definitions,
    remove_main_activation, remove_native_dylib_loader_state_rollback, remove_shadow_finalize,
    restore_native_dylib_loader_state_for_activation, restore_native_dylib_loader_state_rollback,
    rollout_activation_lock_path, rollout_diagnostics_report, rollout_main_activation_path,
    rollout_main_exit_path, rollout_native_dylib_loader_rollback_dir,
    rollout_native_dylib_loader_rollback_path, rollout_runtime_state_path,
    rollout_shadow_finalize_path, rollout_staged_main_binary, rollout_staged_shadow_binary,
    rollout_staging_dir, rollout_telemetry_advance_state_path,
    rollout_telemetry_gate_config_definition, rollout_telemetry_gate_config_path,
    rollout_telemetry_gate_legacy_config_path, rollout_transition_history_dir,
    rollout_transition_record, stage_native_dylib_loader_state_rollback,
    stage_pending_shadow_rollout, unresolved_native_dylib_loader_rollback_artifacts, write_json,
    write_main_activation, write_object, write_rollout_runtime_state, write_rollout_status,
    write_rollout_telemetry_advance_state, write_rollout_telemetry_gate_config,
};
pub use supervision::{
    PendingShadowCandidateExitAction, SHADOW_MAIN_RESTART_INITIAL_BACKOFF,
    SHADOW_MAIN_RESTART_MAX_BACKOFF, SHADOW_MAIN_RESTART_STABLE_RESET_AFTER,
    ShadowMainRestartBackoff, SupervisedMainExitDecision, classify_supervised_main_exit,
    pending_rollout_probe_matches_candidate, pending_shadow_candidate_exit_action,
    shadow_main_should_start,
};
pub use target::{
    cluster_rollout_should_use_remote_build, ensure_rollout_target_platform_matches_local,
    rollout_target_platform_matches_local,
};
pub use telemetry::{
    RolloutTelemetryHost, RolloutTelemetryReactor, RolloutTelemetryReconcileOutcome,
};

pub fn rollout_module_id() -> sinomo_module::ModuleId {
    sinomo_module::ModuleId::from_legacy_named(SINOMO_ROLLOUT_MODULE_LEGACY_ID)
        .expect("valid rollout module id")
}

pub fn rollout_module_registration() -> sinomo_module::ModuleRegistration {
    let mut registration = sinomo_module::ModuleRegistration::native_rust(
        rollout_module_id(),
        env!("CARGO_PKG_VERSION"),
        SINOMO_ROLLOUT_MODULE_OWNER,
        env!("CARGO_PKG_NAME"),
        ROLLOUT_PROTOCOL,
        sinomo_core::TrustRing::R1TrustedRuntime,
        SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY,
    );
    registration.manifest.capabilities = rollout_module_capabilities();
    registration.manifest.limits = sinomo_core::ModuleResourceLimits {
        cpu_ms: 30_000,
        memory_mb: 512,
        wall_time_ms: 30_000,
        network: sinomo_core::NetworkAccess::Allowlisted,
        filesystem: sinomo_core::FileSystemAccess::Scoped,
    };
    registration.surfaces = rollout_module_surfaces();
    registration
}

pub fn rollout_module_surfaces() -> sinomo_module::ModuleSurfaces {
    use sinomo_module::{ModuleObjectRpcRoute, ModuleRouteAuth, ModuleSurfaces};

    let mut surfaces = ModuleSurfaces::default()
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            ROLLOUT_UPLOAD_STATUS_ROUTE,
            ROLLOUT_RPC_CATEGORY,
            "rollout.diagnostics.read",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            ROLLOUT_DIAGNOSTICS_ROUTE,
            ROLLOUT_RPC_CATEGORY,
            "rollout.diagnostics.read",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            ROLLOUT_BINARY_ROUTE,
            ROLLOUT_RPC_CATEGORY,
            "rollout.binary.apply",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
            ROLLOUT_RPC_CATEGORY,
            "rollout.module.native-dylib.apply",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            ROLLOUT_BUILD_ROUTE,
            ROLLOUT_RPC_CATEGORY,
            "rollout.build.apply",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            ROLLOUT_FINALIZE_SHADOW_ROUTE,
            ROLLOUT_RPC_CATEGORY,
            "rollout.shadow.finalize",
            ModuleRouteAuth::LocalCommand,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            ROLLOUT_CLEAR_STATUS_ROUTE,
            ROLLOUT_RPC_CATEGORY,
            "rollout.status.clear",
            ModuleRouteAuth::Admin,
        ))
        .with_event_kind_name(ROLLOUT_TRANSITION_COMPLETED_EVENT_KIND)
        .with_storage_owner(SINOMO_ROLLOUT_MODULE_OWNER);
    surfaces = surfaces
        .with_config_definition(
            rollout_telemetry_gate_config_definition()
                .expect("rollout telemetry config definition must be valid"),
        )
        .expect("rollout settings surface must be valid");
    surfaces.capabilities = rollout_module_capabilities();
    surfaces
}

pub fn rollout_module_capabilities() -> sinomo_core::ModuleCapabilities {
    sinomo_core::ModuleCapabilities {
        imports: vec![
            rollout_capability("storage.local.read"),
            rollout_capability("storage.local.write"),
            rollout_capability("network.object_rpc"),
            rollout_capability("cluster.kv.read"),
            rollout_capability("process.spawn"),
        ],
        exports: vec![
            rollout_capability("rollout.diagnostics.read"),
            rollout_capability("rollout.binary.apply"),
            rollout_capability("rollout.module.native-dylib.apply"),
            rollout_capability("rollout.build.apply"),
            rollout_capability("rollout.shadow.finalize"),
            rollout_capability("rollout.status.clear"),
            rollout_capability("rollout.transition.record"),
        ],
    }
}

fn rollout_capability(name: &str) -> sinomo_core::CapabilityName {
    sinomo_core::CapabilityName::new(name).expect("valid rollout module capability")
}

pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    use sinomo_core::storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    layout.register_sinomo_object(
        "sinomo-rollout",
        StorageRing::R1,
        "rollout/state.sobj",
        ObjectSchema::new("ClusterNodeRolloutStatus", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("status", "String")
            .field("mode", "Option<String>")
            .field("detail", "Option<String>")
            .field("phase", "Option<String>")
            .field("progress_current", "Option<u32>")
            .field("progress_total", "Option<u32>")
            .field("progress_message", "Option<String>"),
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
        ("bin", "installed runtime binaries"),
        ("rollout/staged", "staged rollout build artifacts"),
        (
            "rollout/coordinator",
            "durable rollout plans and coordinator admission lock",
        ),
        (
            "rollout/logs",
            "operator-readable rollout logs and diagnostics",
        ),
        (
            "rollout/history",
            "durable rollout promotion and rollback decision records",
        ),
        (
            "rollout/native-loader-rollback",
            "generation-coupled native loader rollback records",
        ),
        (
            "rollout/activation.lock",
            "durable per-node rollout activation and module mutation admission lock",
        ),
    ] {
        layout.register_escape_path("sinomo-rollout", StorageRing::R1, relative_path, reason)?;
    }
    Ok(())
}

#[cfg(test)]
mod module_registration_tests {
    use super::*;
    use sinomo_core::{FileSystemAccess, ModuleKind, NetworkAccess, TrustRing};
    use sinomo_module::{
        ModuleBackendRegistration, ModuleRouteAuth, NativeRustBackendRegistration,
    };

    #[test]
    fn rollout_registers_as_r1_native_rust_module() {
        let registration = rollout_module_registration();

        assert_eq!(registration.module_id(), &rollout_module_id());
        assert_eq!(registration.manifest.kind, ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.target_ring,
            TrustRing::R1TrustedRuntime
        );
        assert_eq!(registration.manifest.api_version, ROLLOUT_PROTOCOL);
        assert_eq!(
            registration.manifest.limits.network,
            NetworkAccess::Allowlisted
        );
        assert_eq!(
            registration.manifest.limits.filesystem,
            FileSystemAccess::Scoped
        );
        assert!(
            registration
                .manifest
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == "rollout.build.apply")
        );
        assert_eq!(
            registration.backend,
            ModuleBackendRegistration::NativeRust {
                registration: NativeRustBackendRegistration::new(
                    SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY,
                    SINOMO_ROLLOUT_MODULE_OWNER
                )
            }
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            vec![SINOMO_ROLLOUT_MODULE_OWNER.to_string()]
        );
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            ROLLOUT_OBJECT_RPC_ROUTES.len()
        );
        assert!(
            registration
                .surfaces
                .event_kinds
                .iter()
                .any(|event_kind| event_kind.as_str() == ROLLOUT_TRANSITION_COMPLETED_EVENT_KIND)
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == ROLLOUT_BUILD_ROUTE
                    && route.category == ROLLOUT_RPC_CATEGORY
                    && route.auth == ModuleRouteAuth::Admin)
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == ROLLOUT_FINALIZE_SHADOW_ROUTE
                    && route.auth == ModuleRouteAuth::LocalCommand)
        );
    }
}
