//! Rollout paths, source management, staged binary activation, and rollout policy.

mod artifact;
mod controller;
mod error;
mod module_load;
mod native_install;
mod paths;
mod process;
mod progress;
mod protocol;
mod service;
mod source;
mod state;
mod target;

pub type RolloutPlanId = sinomo_core::Id;
pub type ScorecardId = sinomo_core::Id;

pub const ROLLOUT_PROTOCOL: &str = "sinomo.rollout.v1";
pub const SINOMO_ROLLOUT_MODULE_LEGACY_ID: &str = "module.rollout";
pub const SINOMO_ROLLOUT_MODULE_OWNER: &str = "sinomo-rollout";
pub const SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY: &str = "sinomo-rollout";
pub const ROLLOUT_TRANSITION_COMPLETED_EVENT_KIND: &str = "rollout.transition.completed";
pub const ROLLOUT_RPC_CATEGORY: &str = "rollout";
pub const ROLLOUT_DIAGNOSTICS_ROUTE: &str = "/rollout/diagnostics";
pub const ROLLOUT_BINARY_ROUTE: &str = "/rollout/binary";
pub const ROLLOUT_BUILD_ROUTE: &str = "/rollout/build";
pub const ROLLOUT_FINALIZE_SHADOW_ROUTE: &str = "/rollout/finalize-shadow";
pub const ROLLOUT_CLEAR_STATUS_ROUTE: &str = "/rollout/clear-status";
pub const SHADOW_MAIN_HOST_ENV: &str = "SINOMO_SHADOW_MAIN_HOST";
pub const SHADOW_MAIN_PORT_ENV: &str = "SINOMO_SHADOW_MAIN_PORT";
pub const SHADOW_MAIN_BINARY_ENV: &str = "SINOMO_SHADOW_MAIN_BINARY";
pub const SHADOW_START_DELAY_MS_ENV: &str = "SINOMO_SHADOW_START_DELAY_MS";
pub const ROLLOUT_OBJECT_RPC_ROUTES: &[&str] = &[
    ROLLOUT_DIAGNOSTICS_ROUTE,
    ROLLOUT_BINARY_ROUTE,
    ROLLOUT_BUILD_ROUTE,
    ROLLOUT_FINALIZE_SHADOW_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE,
];

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
pub use module_load::{
    LoadDecision, LoadRequest, LoaderAction, ModuleLoadError, ModuleLoader, ModulePolicyViolation,
};
pub use native_install::{
    install_cluster_rollout_native_dylib_modules, install_local_rollout_native_dylib_modules,
    native_dylib_module_install_request,
};
pub use paths::{
    SANDBOX_WASIX_MODULE_PACKAGE, SANDBOX_WASIX_MODULE_RUNTIME_FEATURE,
    SANDBOX_WASIX_NATIVE_MODULE_CACHE_KEY, SANDBOX_WASIX_NATIVE_MODULE_SOURCE,
    SelfRolloutNativeDylibModuleBuild, SelfRolloutPaths, SelfRolloutPlan,
    append_native_dylib_module_build_args, default_native_dylib_module_builds,
    ensure_rollout_layout, executable_name, native_dylib_library_name,
    sandbox_wasix_native_module_library_name,
};
pub use process::{MainExitKind, MainExitObservation, classify_main_exit};
pub use progress::{
    CLUSTER_ROLLOUT_BUILD_HEARTBEAT_INTERVAL, CLUSTER_ROLLOUT_PROGRESS_TOTAL,
    ClusterRolloutProgressUpdate, cluster_rollout_progress_status, rollout_timestamp_now,
};
pub use protocol::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleArtifact, cluster_rollout_targets_from_state,
    prepare_cluster_rollout_binary_request, prepare_cluster_rollout_build_request,
    prepare_rollout_cluster_targets, unique_rollout_targets,
};
pub use service::{RolloutService, register_services};
pub use source::{
    LocalRolloutArtifacts, LocalRolloutNativeDylibModuleArtifact, ManagedSourceStatus,
    RolloutBuiltArtifacts, RolloutStatusView, create_source_repo_bundle,
    create_source_repo_bundle_bytes, deploy_managed_source_bundle, deploy_managed_source_repo,
    deploy_managed_source_repo_or_current, discover_current_git_repo, ensure_rollout_source_repo,
    managed_source_status, prepare_local_rollout_artifacts, prepare_rollout_dev_artifacts,
    prepare_rollout_dev_binary, prepare_rollout_source_bundle, remove_dir_if_exists,
    rollout_artifact_suffix, rollout_build_dev_artifacts, rollout_build_dev_binary,
    rollout_remote_repo_path, rollout_source_head, rollout_status_view, source_heads_match,
    stage_local_rollout_artifacts, sync_external_repo_from_managed,
};
pub use state::{
    PendingShadowRollout, PendingShadowRolloutContext, ROLLOUT_DIAGNOSTIC_TAIL_BYTES,
    RecordedRolloutTransition, RolloutFailureDiagnostics, RolloutMainActivation,
    RolloutMainExitRecord, RolloutTelemetryAdvanceState, RolloutTelemetryGateConfig,
    RolloutTransitionRecord, clear_rollout_status, read_pending_shadow_rollout,
    read_rollout_main_activation, read_rollout_main_exit, read_rollout_runtime_state,
    read_rollout_status, read_rollout_telemetry_advance_state, read_rollout_telemetry_gate_config,
    read_tail_lossy, record_activation_failure, record_main_activation_staged,
    record_main_rollout_failure, record_main_rollout_failure_with_diagnostics,
    record_rollout_main_exit, record_rollout_transition, record_shadow_rollout_finalized,
    register_config_definitions, remove_main_activation, remove_shadow_finalize,
    rollout_diagnostics_report, rollout_main_activation_path, rollout_main_exit_path,
    rollout_runtime_state_path, rollout_shadow_finalize_path, rollout_staged_main_binary,
    rollout_staged_shadow_binary, rollout_staging_dir, rollout_telemetry_advance_state_path,
    rollout_telemetry_gate_config_definition, rollout_telemetry_gate_config_path,
    rollout_telemetry_gate_legacy_config_path, rollout_transition_history_dir,
    rollout_transition_record, stage_pending_shadow_rollout, write_json, write_main_activation,
    write_rollout_runtime_state, write_rollout_status, write_rollout_telemetry_advance_state,
    write_rollout_telemetry_gate_config,
};
pub use target::{
    cluster_rollout_should_use_remote_build, ensure_rollout_target_platform_matches_local,
    rollout_target_platform_matches_local,
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

    #[test]
    fn rollout_route_constants_match_network_rpc_mirrors() {
        assert_eq!(ROLLOUT_RPC_CATEGORY, sinomo_network::ROLLOUT_RPC_CATEGORY);
        assert_eq!(
            ROLLOUT_DIAGNOSTICS_ROUTE,
            sinomo_network::ROLLOUT_DIAGNOSTICS_ROUTE
        );
        assert_eq!(ROLLOUT_BINARY_ROUTE, sinomo_network::ROLLOUT_BINARY_ROUTE);
        assert_eq!(ROLLOUT_BUILD_ROUTE, sinomo_network::ROLLOUT_BUILD_ROUTE);
        assert_eq!(
            ROLLOUT_FINALIZE_SHADOW_ROUTE,
            sinomo_network::ROLLOUT_FINALIZE_SHADOW_ROUTE
        );
        assert_eq!(
            ROLLOUT_CLEAR_STATUS_ROUTE,
            sinomo_network::ROLLOUT_CLEAR_STATUS_ROUTE
        );
    }
}
