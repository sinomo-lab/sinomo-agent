//! Explicit R1 runtime boundary facade.
//!
//! R2 service crates should treat this crate as the public foundation surface
//! for trusted runtime contracts. The facade aggregates the R0 boundary
//! (`sinomo-core`) plus the R1 crates; it must never depend on R2 service or
//! application crates, and it reaches R0 exclusively through `sinomo-core`.
//!
//! The crate does not add behavior. It makes the trust-ring cut visible in the
//! Cargo graph and gives architecture tests one concrete boundary to enforce.

/// R0 cluster foundation, re-exported from the `sinomo-core` boundary facade.
///
/// These are the smallest durable contracts that must remain understandable
/// across standby handoff, rollback, recovery, and high-level automation: the
/// minimal set of crates that defines the cluster.
pub mod r0 {
    pub use sinomo_core as core;
    pub use sinomo_core::*;
    pub use sinomo_core::{
        base, cluster, events, kv, local, network, node, object, service, storage,
    };
}

/// R1 trusted-runtime services and contracts.
///
/// R1 owns the local runtime substrate above the cluster foundation: DFS
/// metadata, module loading, policy, registry, rollout, sandboxing,
/// supervision, and command evaluation. R2 services may build on these crates,
/// but R1 crates must not depend back on R2 service crates.
pub mod r1 {
    pub use sinomo_api as api;
    pub use sinomo_build as build;
    pub use sinomo_cli_registry as cli_registry;
    pub use sinomo_config as config;
    pub use sinomo_daemon as daemon;
    pub use sinomo_device as device;
    pub use sinomo_dfs as dfs;
    pub use sinomo_dfs_cloud_files_api as dfs_cloud_files_api;
    pub use sinomo_eval as eval;
    pub use sinomo_memory_extension_api as memory_extension_api;
    pub use sinomo_module as module;
    pub use sinomo_module_api as module_api;
    pub mod network_module {
        pub use sinomo_daemon::registry::{
            network_module_capabilities, network_module_id, network_module_registration,
            network_module_surfaces,
        };
    }
    pub use sinomo_observe as observe;
    pub use sinomo_policy as policy;
    pub use sinomo_registry as registry;
    pub use sinomo_remote_management as remote_management;
    pub use sinomo_rollout as rollout;
    // The `sandbox` facade is always the lightweight contracts crate. Runtime
    // execution is provided by `sandbox_wasix_host`, which loads an active
    // native-dylib module from content-addressed module cache.
    pub use sinomo_sandbox as sandbox;
    pub use sinomo_sandbox_simple as sandbox_simple;
    pub use sinomo_sandbox_wasix_api as sandbox_wasix_api;
    #[cfg(feature = "sandbox")]
    pub use sinomo_sandbox_wasix_host as sandbox_wasix_host;
    pub use sinomo_supervision as supervision;
    pub use sinomo_test as test;
    pub mod vfs {
        pub use sinomo_vfs::*;
        pub use sinomo_vfs_sifs::{
            DaemonSifsVfsBackend, DfsCancellationFlag, MountedSifsOpenOptions, MountedSifsReadMode,
            MountedSifsVfsBackend, daemon_sifs, daemon_sifs_with_timeout, mounted_sifs,
            mounted_sifs_local_reads, mounted_sifs_with_cancellation,
        };
    }

    pub use sinomo_core::{cluster, events, kv, local, network, node, service};

    pub mod storage {
        pub use sinomo_core::storage::*;
        pub use sinomo_daemon::registry::{
            ensure_sinomo_data_dir_is_registered, sinomo_data_dir_layout, storage_registry,
            verify_sinomo_data_dir,
        };
    }

    pub mod modules {
        pub use sinomo_daemon::registry::{
            InternalModuleTable, ModuleRegistration, ModuleRegistry, ModuleRegistryError,
            ModuleRegistrySnapshot, ModuleRegistrySnapshotStore, NativeRustModuleBinding,
            internal_module_registry, internal_module_table, internal_native_rust_module_bindings,
            module_object_rpc_route_descriptor_to_service,
            module_registration_object_rpc_route_descriptors,
            module_registry_object_rpc_route_descriptors,
            module_registry_snapshot_object_rpc_route_descriptors,
            native_rust_binding_for_registration, native_rust_module_binding,
            register_internal_module_storage_paths,
        };
    }
}

pub use r0::cluster;
pub use r0::{core, object};
#[cfg(feature = "sandbox")]
pub use r1::sandbox_wasix_host;
pub use r1::{
    api, build, cli_registry, config, daemon, device, dfs, dfs_cloud_files_api, eval, events, kv,
    local, memory_extension_api, module, module_api, modules, network, network_module, node,
    observe, policy, registry, remote_management, rollout, sandbox, sandbox_simple,
    sandbox_wasix_api, service, storage, supervision, test, vfs,
};

#[cfg(test)]
mod tests {
    use super::modules::{
        internal_module_registry, internal_module_table,
        module_registry_object_rpc_route_descriptors, native_rust_binding_for_registration,
    };
    use super::storage::{StorageCoverage, StoragePathOwner, sinomo_data_dir_layout};
    use super::{
        build, core, daemon, dfs, memory_extension_api, module, network, network_module, observe,
        rollout, service,
    };

    #[test]
    fn runtime_facade_exports_memory_extension_contracts() {
        let _ = memory_extension_api::MemoryExtensionDescriptor::new(
            "memory.test",
            "1.0.0",
            memory_extension_api::MEMORY_CONTENT_EXTRACTOR_INTERFACE_ID_V1,
        );
    }

    #[test]
    fn runtime_storage_layout_is_composed_from_owner_crates() {
        let layout = sinomo_data_dir_layout().expect("layout");

        let Some(StoragePathOwner::File(cluster_state)) = layout.owner_for("cluster/state.sobj")
        else {
            panic!("expected cluster state contract");
        };
        assert_eq!(cluster_state.module, "sinomo-cluster");

        let Some(StoragePathOwner::Escape(kv_raft_node)) =
            layout.owner_for("cluster/kv-raft/01kr3d54d45zkm0p0xgps8k7mt/wal.sobjlog")
        else {
            panic!("expected dynamic raft node escape");
        };
        assert_eq!(kv_raft_node.module, "sinomo-kv");

        let Some(StoragePathOwner::File(kv_watch_events)) =
            layout.owner_for("cluster/watch-events.sobj")
        else {
            panic!("expected cluster kv watch events contract");
        };
        assert_eq!(kv_watch_events.module, "sinomo-kv");

        let Some(StoragePathOwner::Escape(kv_watch_registration)) =
            layout.owner_for("cluster/watches/watch-7.sobj")
        else {
            panic!("expected dynamic cluster kv watch registration escape");
        };
        assert_eq!(kv_watch_registration.module, "sinomo-kv");

        let report = layout
            .verify_relative_paths([
                "daemon/status.sobj",
                "dfs/metadata/root/records/filesystems/root/object.sobj",
                "dfs/metadata-replication-cursors/01kr4w089vqgdxpb84t6bq1qxn/root.json",
                "sandbox/runtime/active.sobj",
                "sandbox/module-artifacts/artifact_candidate.wasm",
                "sandbox/module-wasm-artifacts.sobj",
                "module/loader-state/01kr3d54d45zkm0p0xgps8k7mt.sobj",
                "module/loader-state/native-rust/01kr3d54d45zkm0p0xgps8k7mt.sobj",
                "module/lifecycle/unload-records/01kr3d54d45zkm0p0xgps8k7mt.sobj",
                "module/local-command-artifacts.sobj",
                "events/ops/operational-events.jsonl",
                "events/ops/logs/operational-log.jsonl.lock",
                "configs/rollout/telemetry/gates.json",
                "rollout/runtime-state.sobj",
                "rollout/telemetry-advance-state.sobj",
                "rollout/telemetry-gates.sobj",
                "registry/trusted-artifact-signers.json",
                "registry/artifact-evidence/release_verified.json",
                "registry/verified-release-material/release_verified.json",
                "bin/sinomo",
                "startup/sinomo-daemon.service",
            ])
            .expect("verify");
        assert!(report.unregistered.is_empty());
        assert!(
            report
                .covered
                .iter()
                .any(|path| path.coverage == StorageCoverage::Escape)
        );
        assert!(report.covered.iter().any(|path| {
            path.relative_path == "sandbox/runtime/active.sobj" && path.module == "sinomo-sandbox"
        }));
    }

    #[test]
    fn runtime_daemon_facade_exports_async_rpc_clients() {
        let _ = daemon::send_daemon_rpc_async;
        let _ = daemon::send_daemon_rpc_to_endpoint_async;
        let _ = daemon::fetch_daemon_status_from_endpoint_async;
        let _ = daemon::send_object_rpc_to_target_async;
        let _ = daemon::send_dfs_metadata_journal_batch_to_endpoint_async;
        let _ = daemon::send_dfs_metadata_snapshot_install_to_endpoint_async;
        let request =
            network::ObjectRpcRequest::encode_body("/test/stream", "test", &()).expect("request");
        let _future = daemon::send_daemon_rpc_stream_async(
            "127.0.0.1:0",
            &request,
            network::DaemonRpcTimeouts::connect_and_idle(std::time::Duration::from_millis(1)),
            |_response| Ok(false),
        );
    }

    #[test]
    fn runtime_internal_module_registry_includes_dfs() {
        let registry = internal_module_registry().expect("module registry");
        let module_id = dfs::dfs_module_id();
        let registration = registry.get(&module_id).expect("dfs module registration");

        assert_eq!(registration.manifest.kind, core::ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.target_ring,
            core::TrustRing::R1TrustedRuntime
        );
        assert_eq!(registration.manifest.api_version, dfs::DFS_PROTOCOL);
        assert!(
            registration
                .manifest
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == "dfs.metadata.replicate")
        );
        assert_eq!(
            registration.backend,
            module::ModuleBackendRegistration::NativeRust {
                registration: module::NativeRustBackendRegistration::new(
                    dfs::SINOMO_DFS_NATIVE_REGISTRATION_KEY,
                    dfs::SINOMO_DFS_MODULE_OWNER
                )
            }
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            vec![dfs::SINOMO_DFS_MODULE_OWNER.to_string()]
        );
        assert!(
            registration
                .surfaces
                .event_kinds
                .iter()
                .any(|event_kind| event_kind.as_str() == dfs::DFS_POLICY_VIOLATION_EVENT_KIND)
        );
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            dfs::DFS_OBJECT_RPC_ROUTES.len()
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == dfs::DFS_STATUS_ROUTE
                    && route.category == dfs::DFS_RPC_CATEGORY
                    && route.auth == module::ModuleRouteAuth::LocalOrClusterMember)
        );
    }

    #[test]
    fn runtime_internal_module_registry_includes_build() {
        let registry = internal_module_registry().expect("module registry");
        let registration = registry
            .get(&build::build_module_id())
            .expect("build module registration");

        assert_eq!(registration.manifest.kind, core::ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.owner,
            build::SINOMO_BUILD_MODULE_OWNER
        );
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            build::BUILD_OBJECT_RPC_ROUTES.len()
        );
    }

    #[test]
    fn runtime_internal_module_registry_includes_network() {
        let registry = internal_module_registry().expect("module registry");
        let module_id = network_module::network_module_id();
        let registration = registry
            .get(&module_id)
            .expect("network module registration");

        assert_eq!(registration.manifest.kind, core::ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.target_ring,
            core::TrustRing::R1TrustedRuntime
        );
        assert_eq!(registration.manifest.api_version, network::NETWORK_PROTOCOL);
        assert!(
            registration
                .manifest
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == network::CLUSTER_HEALTH_READ_CAPABILITY)
        );
        assert_eq!(
            registration.backend,
            module::ModuleBackendRegistration::NativeRust {
                registration: module::NativeRustBackendRegistration::new(
                    network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
                    network::SINOMO_NETWORK_MODULE_OWNER
                )
            }
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            vec![network::SINOMO_NETWORK_MODULE_OWNER.to_string()]
        );
        assert_eq!(registration.surfaces.object_rpc_routes.len(), 3);
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == network::CLUSTER_HEALTH_ROUTE
                    && route.category == network::CLUSTER_HEALTH_RPC_CATEGORY
                    && route.auth == module::ModuleRouteAuth::LocalOrClusterMember)
        );
        assert!(
            registration
                .surfaces
                .declarative_resources
                .iter()
                .any(
                    |resource| resource.id == network::CLUSTER_NETWORK_INSPECTION_SKILL_ID
                        && resource.kind == module::ModuleDeclarativeResourceKind::Skill
                )
        );
        let binding =
            native_rust_binding_for_registration(registration).expect("network native binding");
        assert_eq!(
            binding.registration_key,
            network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY
        );
        assert!(binding.register_services.is_none());
        assert!(binding.register_storage_paths.is_none());
    }

    #[test]
    fn runtime_internal_module_registry_includes_rollout() {
        let registry = internal_module_registry().expect("module registry");
        let module_id = rollout::rollout_module_id();
        let registration = registry
            .get(&module_id)
            .expect("rollout module registration");

        assert_eq!(registration.manifest.kind, core::ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.target_ring,
            core::TrustRing::R1TrustedRuntime
        );
        assert_eq!(registration.manifest.api_version, rollout::ROLLOUT_PROTOCOL);
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
            module::ModuleBackendRegistration::NativeRust {
                registration: module::NativeRustBackendRegistration::new(
                    rollout::SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY,
                    rollout::SINOMO_ROLLOUT_MODULE_OWNER
                )
            }
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            vec![rollout::SINOMO_ROLLOUT_MODULE_OWNER.to_string()]
        );
        assert!(
            registration
                .surfaces
                .event_kinds
                .iter()
                .any(|event_kind| event_kind.as_str()
                    == rollout::ROLLOUT_TRANSITION_COMPLETED_EVENT_KIND)
        );
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            rollout::ROLLOUT_OBJECT_RPC_ROUTES.len()
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == rollout::ROLLOUT_BUILD_ROUTE
                    && route.category == rollout::ROLLOUT_RPC_CATEGORY
                    && route.auth == module::ModuleRouteAuth::Admin)
        );
    }

    #[test]
    fn runtime_internal_module_registry_includes_observe() {
        let registry = internal_module_registry().expect("module registry");
        let module_id = observe::observe_module_id();
        let registration = registry
            .get(&module_id)
            .expect("observe module registration");

        assert_eq!(registration.manifest.kind, core::ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.target_ring,
            core::TrustRing::R1TrustedRuntime
        );
        assert_eq!(registration.manifest.api_version, observe::OBSERVE_PROTOCOL);
        assert!(
            registration
                .manifest
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == "observe.events.read")
        );
        assert_eq!(
            registration.backend,
            module::ModuleBackendRegistration::NativeRust {
                registration: module::NativeRustBackendRegistration::new(
                    observe::SINOMO_OBSERVE_NATIVE_REGISTRATION_KEY,
                    observe::SINOMO_OBSERVE_MODULE_OWNER
                )
            }
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            vec![observe::SINOMO_OBSERVE_MODULE_OWNER.to_string()]
        );
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            observe::OBSERVE_OBJECT_RPC_ROUTES.len()
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == observe::OBSERVE_EVENTS_ROUTE
                    && route.category == observe::OBSERVE_RPC_CATEGORY
                    && route.auth == module::ModuleRouteAuth::LocalOrClusterMember)
        );
    }

    #[test]
    fn runtime_internal_module_rpc_surfaces_convert_to_network_descriptors() {
        let registry = internal_module_registry().expect("module registry");
        let descriptors =
            module_registry_object_rpc_route_descriptors(&registry).expect("route descriptors");
        assert!(descriptors.iter().any(|descriptor| {
            descriptor.route == dfs::DFS_STATUS_ROUTE
                && descriptor.category == dfs::DFS_RPC_CATEGORY
                && descriptor.auth_scope.as_deref() == Some("local-or-cluster-member")
                && descriptor
                    .required_capabilities
                    .iter()
                    .any(|capability| capability == "dfs.status.read")
        }));
        assert!(descriptors.iter().any(|descriptor| {
            descriptor.route == network::CLUSTER_HEALTH_ROUTE
                && descriptor.category == network::CLUSTER_HEALTH_RPC_CATEGORY
                && descriptor.auth_scope.as_deref() == Some("local-or-cluster-member")
                && descriptor
                    .required_capabilities
                    .iter()
                    .any(|capability| capability == network::CLUSTER_HEALTH_READ_CAPABILITY)
        }));
        assert!(descriptors.iter().any(|descriptor| {
            descriptor.route == rollout::ROLLOUT_BUILD_ROUTE
                && descriptor.category == rollout::ROLLOUT_RPC_CATEGORY
                && descriptor.auth_scope.as_deref() == Some("admin")
                && descriptor
                    .required_capabilities
                    .iter()
                    .any(|capability| capability == "rollout.build.apply")
        }));
    }

    #[test]
    fn runtime_internal_module_table_binds_dfs_hooks() {
        let table = internal_module_table().expect("module table");
        let module_id = dfs::dfs_module_id();
        let registration = table
            .registry
            .get(&module_id)
            .expect("dfs module registration");
        let binding = native_rust_binding_for_registration(registration).expect("dfs binding");

        assert_eq!(
            binding.registration_key,
            dfs::SINOMO_DFS_NATIVE_REGISTRATION_KEY
        );
        assert_eq!(table.native_rust_bindings.len(), 5);
        assert!(binding.register_services.is_some());
        assert!(binding.register_storage_paths.is_some());

        let registry =
            binding.register_services.expect("dfs services")(service::ServiceRegistry::builder())
                .build();
        assert!(registry.service_names().contains(&"dfs"));
        assert!(registry.owns(dfs::DFS_STATUS_ROUTE));

        let layout = sinomo_data_dir_layout().expect("layout");
        let Some(StoragePathOwner::Escape(dfs_blobs)) =
            layout.owner_for("dfs/content/blobs/object-chunk")
        else {
            panic!("expected DFS blob storage contract");
        };
        assert_eq!(dfs_blobs.module, dfs::SINOMO_DFS_MODULE_OWNER);
    }

    #[test]
    fn runtime_internal_module_table_binds_rollout_storage_hook() {
        let table = internal_module_table().expect("module table");
        let module_id = rollout::rollout_module_id();
        let registration = table
            .registry
            .get(&module_id)
            .expect("rollout module registration");
        let binding = native_rust_binding_for_registration(registration).expect("rollout binding");

        assert_eq!(
            binding.registration_key,
            rollout::SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY
        );
        assert!(binding.register_services.is_some());
        assert!(binding.register_storage_paths.is_some());

        let registry = binding.register_services.expect("rollout services")(
            service::ServiceRegistry::builder(),
        )
        .build();
        assert!(registry.owns(rollout::ROLLOUT_DIAGNOSTICS_ROUTE));

        let layout = sinomo_data_dir_layout().expect("layout");
        let Some(StoragePathOwner::File(rollout_state)) =
            layout.owner_for("rollout/runtime-state.sobj")
        else {
            panic!("expected rollout runtime state storage contract");
        };
        assert_eq!(rollout_state.module, rollout::SINOMO_ROLLOUT_MODULE_OWNER);
    }

    #[test]
    fn runtime_internal_module_table_binds_observe_hooks() {
        let table = internal_module_table().expect("module table");
        let module_id = observe::observe_module_id();
        let registration = table
            .registry
            .get(&module_id)
            .expect("observe module registration");
        let binding = native_rust_binding_for_registration(registration).expect("observe binding");

        assert_eq!(
            binding.registration_key,
            observe::SINOMO_OBSERVE_NATIVE_REGISTRATION_KEY
        );
        assert!(binding.register_services.is_some());
        assert!(binding.register_storage_paths.is_some());

        let registry = binding.register_services.expect("observe services")(
            service::ServiceRegistry::builder(),
        )
        .build();
        assert!(registry.owns(observe::OBSERVE_EVENTS_ROUTE));
        assert!(registry.owns(observe::OBSERVE_LOGS_ROUTE));

        let layout = sinomo_data_dir_layout().expect("layout");
        let Some(StoragePathOwner::File(observe_events)) =
            layout.owner_for(observe::OPERATIONAL_EVENTS_RELATIVE_PATH)
        else {
            panic!("expected observe event log storage contract");
        };
        assert_eq!(observe_events.module, observe::SINOMO_OBSERVE_MODULE_OWNER);
    }
}
