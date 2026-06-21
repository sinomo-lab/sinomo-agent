//! Explicit R0/R1 runtime boundary facade.
//!
//! R2 service crates should treat this crate as the public foundation surface
//! for trusted runtime contracts. The facade intentionally aggregates only R0
//! and R1 crates; it must never depend on R2 service or application crates.
//!
//! The crate does not add behavior. It makes the trust-ring cut visible in the
//! Cargo graph and gives architecture tests one concrete boundary to enforce.

/// R0 continuity-root contracts.
///
/// These are the smallest durable contracts that must remain understandable
/// across standby handoff, rollback, recovery, and high-level automation.
pub mod r0 {
    pub use sinomo_core as core;
    pub use sinomo_core::*;
    pub use sinomo_object as object;
}

/// R1 trusted-runtime services and contracts.
///
/// R1 owns the local runtime substrate: events, nodes, DFS metadata, module
/// loading, policy, registry, rollout, sandboxing, supervision, local paths,
/// and command evaluation. R2 services may build on these crates, but R1 crates
/// must not depend back on R2 service crates.
pub mod r1 {
    pub use sinomo_cli_registry as cli_registry;
    pub use sinomo_config as config;
    pub use sinomo_daemon as daemon;
    pub use sinomo_device as device;
    pub use sinomo_dfs as dfs;
    pub use sinomo_dfs_cloud_files_api as dfs_cloud_files_api;
    pub use sinomo_eval as eval;
    pub use sinomo_events as events;
    pub use sinomo_kv as kv;
    pub use sinomo_local as local;
    pub use sinomo_module as module;
    pub use sinomo_module_api as module_api;
    pub use sinomo_network as network;
    pub use sinomo_network_module as network_module;
    pub use sinomo_node as node;
    pub use sinomo_observe as observe;
    pub use sinomo_policy as policy;
    pub use sinomo_registry as registry;
    pub use sinomo_rollout as rollout;
    // The `sandbox` facade is always the lightweight contracts crate. Runtime
    // execution is provided by `sandbox_wasix_host`, which loads an active
    // native-dylib module from content-addressed module cache.
    pub use sinomo_sandbox as sandbox;
    pub use sinomo_sandbox_simple as sandbox_simple;
    pub use sinomo_sandbox_wasix_api as sandbox_wasix_api;
    #[cfg(feature = "sandbox")]
    pub use sinomo_sandbox_wasix_host as sandbox_wasix_host;
    pub use sinomo_service as service;
    pub use sinomo_supervision as supervision;
    pub use sinomo_test as test;

    pub mod storage {
        pub use sinomo_storage::*;

        pub fn sinomo_data_dir_layout() -> Result<StorageLayout, StorageContractError> {
            let mut layout = StorageLayout::new();
            sinomo_network::register_storage_paths(&mut layout)?;
            sinomo_kv::register_storage_paths(&mut layout)?;
            sinomo_daemon::register_storage_paths(&mut layout)?;
            sinomo_config::register_storage_paths(&mut layout)?;
            sinomo_module::register_storage_paths(&mut layout)?;
            sinomo_registry::register_storage_paths(&mut layout)?;
            sinomo_node::register_storage_paths(&mut layout)?;
            super::modules::register_internal_module_storage_paths(&mut layout)?;
            sinomo_sandbox::register_storage_paths(&mut layout)?;
            sinomo_local::register_storage_paths(&mut layout)?;
            sinomo_events::register_storage_paths(&mut layout)?;
            Ok(layout)
        }

        pub fn storage_registry() -> Result<StorageRegistry, StorageContractError> {
            sinomo_data_dir_layout().map(StorageRegistry::from_layout)
        }

        pub fn verify_sinomo_data_dir(
            data_dir: impl AsRef<std::path::Path>,
        ) -> Result<StorageVerificationReport, StorageContractError> {
            sinomo_data_dir_layout()?.verify_data_dir(data_dir)
        }

        pub fn ensure_sinomo_data_dir_is_registered(
            data_dir: impl AsRef<std::path::Path>,
        ) -> Result<(), StorageContractError> {
            let report = verify_sinomo_data_dir(data_dir)?;
            if report.is_complete() {
                Ok(())
            } else {
                Err(StorageContractError::UnregisteredPaths {
                    paths: report.unregistered,
                })
            }
        }
    }

    pub mod modules {
        pub use sinomo_module::{
            ModuleRegistration, ModuleRegistry, ModuleRegistryError, ModuleRegistrySnapshot,
            ModuleRegistrySnapshotStore,
        };
        use std::collections::BTreeSet;
        use std::future::Future;
        use std::sync::Arc;

        pub struct NativeRustModuleBinding {
            pub registration_key: &'static str,
            pub register_services: Option<
                fn(
                    sinomo_service::ServiceRegistryBuilder,
                ) -> sinomo_service::ServiceRegistryBuilder,
            >,
            pub register_storage_paths: Option<
                fn(
                    &mut sinomo_storage::StorageLayout,
                ) -> Result<(), sinomo_storage::StorageContractError>,
            >,
        }

        pub struct InternalModuleTable {
            pub registry: ModuleRegistry,
            pub native_rust_bindings: &'static [NativeRustModuleBinding],
        }

        const INTERNAL_NATIVE_RUST_MODULE_BINDINGS: &[NativeRustModuleBinding] = &[
            NativeRustModuleBinding {
                registration_key: sinomo_dfs::SINOMO_DFS_NATIVE_REGISTRATION_KEY,
                register_services: Some(sinomo_dfs::register_services),
                register_storage_paths: Some(sinomo_dfs::register_storage_paths),
            },
            NativeRustModuleBinding {
                registration_key: sinomo_rollout::SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY,
                register_services: Some(sinomo_rollout::register_services),
                register_storage_paths: Some(sinomo_rollout::register_storage_paths),
            },
            NativeRustModuleBinding {
                registration_key: sinomo_network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
                register_services: None,
                register_storage_paths: None,
            },
            NativeRustModuleBinding {
                registration_key: sinomo_observe::SINOMO_OBSERVE_NATIVE_REGISTRATION_KEY,
                register_services: Some(sinomo_observe::register_services),
                register_storage_paths: Some(sinomo_observe::register_storage_paths),
            },
        ];

        pub fn internal_module_table() -> Result<InternalModuleTable, ModuleRegistryError> {
            let registry = internal_module_registry()?;
            Ok(InternalModuleTable {
                registry,
                native_rust_bindings: internal_native_rust_module_bindings(),
            })
        }

        pub fn internal_module_registry() -> Result<ModuleRegistry, ModuleRegistryError> {
            let registry = sinomo_module::ModuleRegistry::builder()
                .register(sinomo_dfs::dfs_module_registration())?
                .register(sinomo_network_module::network_module_registration())?
                .register(sinomo_rollout::rollout_module_registration())?
                .register(sinomo_observe::observe_module_registration())?
                .try_build()?;
            Ok(registry)
        }

        pub fn module_object_rpc_route_descriptor_to_network(
            descriptor: &sinomo_module::ModuleObjectRpcRouteDescriptor,
        ) -> sinomo_network::ObjectRouteDescriptor {
            let mut route = sinomo_network::ObjectRouteDescriptor::new(
                descriptor.route.clone(),
                descriptor.category.clone(),
                format!(
                    "module object RPC operation {}",
                    descriptor.operation.as_str()
                ),
                "sinomo_network::ObjectRpcRequest",
                "sinomo_network::ObjectRpcResponse",
            )
            .with_auth_scope(descriptor.auth_scope.clone())
            .observable("module.operation", descriptor.operation.as_str());
            for capability in &descriptor.required_capabilities {
                route = route.require_capability(capability.as_str());
            }
            route
        }

        pub fn module_registration_object_rpc_route_descriptors(
            registration: &ModuleRegistration,
        ) -> Result<Vec<sinomo_network::ObjectRouteDescriptor>, sinomo_module::ModuleSurfaceError>
        {
            Ok(registration
                .surfaces
                .object_rpc_route_descriptors()?
                .iter()
                .map(module_object_rpc_route_descriptor_to_network)
                .collect::<Vec<_>>())
        }

        pub fn module_registry_object_rpc_route_descriptors(
            registry: &ModuleRegistry,
        ) -> Result<Vec<sinomo_network::ObjectRouteDescriptor>, sinomo_module::ModuleSurfaceError>
        {
            let mut seen_routes = BTreeSet::new();
            let mut descriptors = Vec::new();
            for registration in registry.registrations() {
                for descriptor in module_registration_object_rpc_route_descriptors(registration)? {
                    if !seen_routes.insert(descriptor.route.clone()) {
                        return Err(sinomo_module::ModuleSurfaceError::DuplicateObjectRpcRoute {
                            route: descriptor.route,
                        });
                    }
                    descriptors.push(descriptor);
                }
            }
            Ok(descriptors)
        }

        pub fn module_registry_snapshot_object_rpc_route_descriptors(
            snapshot: &ModuleRegistrySnapshot,
        ) -> Result<Vec<sinomo_network::ObjectRouteDescriptor>, sinomo_module::ModuleSurfaceError>
        {
            module_registry_object_rpc_route_descriptors(snapshot.registry())
        }

        pub const MODULE_REGISTRY_SNAPSHOT_VERSION_OBSERVABILITY_KEY: &str =
            "module.registry.snapshot_version";

        pub fn install_module_registry_snapshot_routes<F, Fut>(
            router: &sinomo_network::ObjectRpcRouter,
            snapshot: &ModuleRegistrySnapshot,
            handler: F,
        ) -> Result<Arc<sinomo_network::ObjectRpcRouteRegistry>, sinomo_network::NetworkError>
        where
            F: Fn(sinomo_network::ObjectRpcRequest) -> Fut + Clone + Send + Sync + 'static,
            Fut: Future<
                    Output = Result<
                        sinomo_network::ObjectRpcResponse,
                        sinomo_network::NetworkError,
                    >,
                > + Send
                + 'static,
        {
            let snapshot_version = snapshot.version().to_string();
            let descriptors = module_registry_snapshot_object_rpc_route_descriptors(snapshot)
                .map_err(module_surface_error_to_network_error)?
                .into_iter()
                .map(|descriptor| {
                    descriptor.observable(
                        MODULE_REGISTRY_SNAPSHOT_VERSION_OBSERVABILITY_KEY,
                        snapshot_version.clone(),
                    )
                })
                .collect::<Vec<_>>();
            router.replace_route_set(
                |descriptor| {
                    descriptor
                        .observability
                        .contains_key(MODULE_REGISTRY_SNAPSHOT_VERSION_OBSERVABILITY_KEY)
                },
                descriptors,
                handler,
            )
        }

        fn module_surface_error_to_network_error(
            error: sinomo_module::ModuleSurfaceError,
        ) -> sinomo_network::NetworkError {
            sinomo_network::NetworkError::Serialization {
                detail: error.to_string(),
            }
        }

        pub fn internal_native_rust_module_bindings() -> &'static [NativeRustModuleBinding] {
            INTERNAL_NATIVE_RUST_MODULE_BINDINGS
        }

        pub fn native_rust_module_binding(
            registration_key: &str,
        ) -> Option<&'static NativeRustModuleBinding> {
            internal_native_rust_module_bindings()
                .iter()
                .find(|binding| binding.registration_key == registration_key)
        }

        pub fn native_rust_binding_for_registration(
            registration: &ModuleRegistration,
        ) -> Option<&'static NativeRustModuleBinding> {
            let sinomo_module::ModuleBackendRegistration::NativeRust {
                registration: native_registration,
            } = &registration.backend
            else {
                return None;
            };
            native_rust_module_binding(&native_registration.registration_key)
        }

        pub fn register_internal_module_storage_paths(
            layout: &mut sinomo_storage::StorageLayout,
        ) -> Result<(), sinomo_storage::StorageContractError> {
            for binding in internal_native_rust_module_bindings() {
                if let Some(register_storage_paths) = binding.register_storage_paths {
                    register_storage_paths(layout)?;
                }
            }
            Ok(())
        }
    }
}

pub use r0::{core, object};
#[cfg(feature = "sandbox")]
pub use r1::sandbox_wasix_host;
pub use r1::{
    cli_registry, config, daemon, device, dfs, dfs_cloud_files_api, eval, events, kv, local,
    module, module_api, modules, network, network_module, node, observe, policy, registry, rollout,
    sandbox, sandbox_simple, sandbox_wasix_api, service, storage, supervision, test,
};

#[cfg(test)]
mod tests {
    use super::modules::{
        MODULE_REGISTRY_SNAPSHOT_VERSION_OBSERVABILITY_KEY,
        install_module_registry_snapshot_routes, internal_module_registry, internal_module_table,
        module_registry_object_rpc_route_descriptors, native_rust_binding_for_registration,
    };
    use super::storage::{StorageCoverage, StoragePathOwner, sinomo_data_dir_layout};
    use super::{core, daemon, dfs, module, network, network_module, observe, rollout, service};

    #[test]
    fn runtime_storage_layout_is_composed_from_owner_crates() {
        let layout = sinomo_data_dir_layout().expect("layout");

        let Some(StoragePathOwner::File(cluster_state)) = layout.owner_for("cluster/state.sobj")
        else {
            panic!("expected cluster state contract");
        };
        assert_eq!(cluster_state.module, "sinomo-network");

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
        let _ = daemon::send_dfs_object_rpc_to_target_async;
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
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            network::NETWORK_OBJECT_RPC_ROUTES.len()
        );
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
                    && route.auth == module::ModuleRouteAuth::LocalCommand)
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

        let mut builder = network::ObjectRpcRouteRegistry::builder();
        for descriptor in descriptors {
            builder = builder
                .try_register_route(descriptor, |request| async move {
                    network::ObjectRpcResponse::encode_body(request.route, &())
                })
                .expect("register module descriptor in network registry");
        }
        let snapshot = builder
            .try_build()
            .expect("module descriptors should validate as a network route snapshot");
        assert!(snapshot.version() >= 1);
        assert!(
            snapshot
                .catalog()
                .routes
                .contains_key(network::CLUSTER_HEALTH_ROUTE)
        );
        assert!(
            snapshot
                .catalog()
                .routes
                .contains_key(rollout::ROLLOUT_BUILD_ROUTE)
        );
    }

    #[test]
    fn runtime_installs_module_snapshot_routes_into_network_router_snapshot() {
        let module_id =
            module::ModuleId::from_legacy_named("module.runtime.snapshot").expect("module");
        let route = "/module/runtime/snapshot";
        let registration = module::ModuleRegistration::native_rust(
            module_id.clone(),
            "0.1.0",
            "runtime-snapshot-module",
            "runtime-snapshot-module",
            "runtime-snapshot-module-v1",
            core::TrustRing::R1TrustedRuntime,
            "runtime-snapshot-module",
        )
        .with_surfaces(
            module::ModuleSurfaces::default()
                .with_object_rpc_route(module::ModuleObjectRpcRoute::new(
                    route,
                    "runtime-snapshot",
                    "runtime.snapshot.read",
                    module::ModuleRouteAuth::LocalOrClusterMember,
                ))
                .with_capability_export(
                    core::CapabilityName::new("runtime.snapshot.read").expect("capability"),
                ),
        );
        let registry = module::ModuleRegistry::builder()
            .register(registration)
            .expect("register")
            .try_build()
            .expect("registry");
        let store = module::ModuleRegistrySnapshotStore::new(registry).expect("snapshot store");
        let module_snapshot = store.active_snapshot().expect("module snapshot");
        let router = network::ObjectRpcRouter::new();
        let old_network_snapshot = router.route_registry_snapshot();

        let installed = install_module_registry_snapshot_routes(
            &router,
            &module_snapshot,
            |request| async move { network::ObjectRpcResponse::encode_body(request.route, &()) },
        )
        .expect("install module routes");

        assert!(installed.version() > old_network_snapshot.version());
        assert!(!old_network_snapshot.catalog().routes.contains_key(route));
        let active = router.route_registry_snapshot();
        let descriptor = active
            .catalog()
            .routes
            .get(route)
            .expect("installed module route");
        assert_eq!(descriptor.category, "runtime-snapshot");
        let expected_snapshot_version = module_snapshot.version().to_string();
        assert_eq!(
            descriptor
                .observability
                .get("module.registry.snapshot_version")
                .map(String::as_str),
            Some(expected_snapshot_version.as_str())
        );
        assert!(
            descriptor
                .required_capabilities
                .contains(&"runtime.snapshot.read".to_string())
        );
    }

    #[test]
    fn runtime_module_snapshot_route_install_replaces_previous_module_routes() {
        fn registration(
            module_name: &str,
            route: &str,
            capability: &str,
        ) -> module::ModuleRegistration {
            let module_id = module::ModuleId::from_legacy_named(module_name).expect("module");
            module::ModuleRegistration::native_rust(
                module_id,
                "0.1.0",
                module_name,
                module_name,
                "runtime-snapshot-module-v1",
                core::TrustRing::R1TrustedRuntime,
                module_name,
            )
            .with_surfaces(
                module::ModuleSurfaces::default()
                    .with_object_rpc_route(module::ModuleObjectRpcRoute::new(
                        route,
                        "runtime-snapshot",
                        capability,
                        module::ModuleRouteAuth::LocalOrClusterMember,
                    ))
                    .with_capability_export(
                        core::CapabilityName::new(capability).expect("capability"),
                    ),
            )
        }

        let first_route = "/module/runtime/snapshot/first";
        let second_route = "/module/runtime/snapshot/second";
        let first_registry = module::ModuleRegistry::builder()
            .register(registration(
                "module.runtime.snapshot.first",
                first_route,
                "runtime.snapshot.first.read",
            ))
            .expect("register first")
            .try_build()
            .expect("first registry");
        let store =
            module::ModuleRegistrySnapshotStore::new(first_registry).expect("snapshot store");
        let router = network::ObjectRpcRouter::new();
        let first_snapshot = store.active_snapshot().expect("first snapshot");

        install_module_registry_snapshot_routes(&router, &first_snapshot, |request| async move {
            network::ObjectRpcResponse::encode_body(request.route.clone(), &request.route)
        })
        .expect("install first snapshot");
        assert!(router.catalog().routes.contains_key(first_route));

        let second_registry = module::ModuleRegistry::builder()
            .register(registration(
                "module.runtime.snapshot.second",
                second_route,
                "runtime.snapshot.second.read",
            ))
            .expect("register second")
            .try_build()
            .expect("second registry");
        let second_snapshot = store
            .replace_registry(second_registry)
            .expect("replace module registry");
        install_module_registry_snapshot_routes(&router, &second_snapshot, |request| async move {
            network::ObjectRpcResponse::encode_body(request.route.clone(), &request.route)
        })
        .expect("install second snapshot");

        let active = router.route_registry_snapshot();
        assert!(!active.catalog().routes.contains_key(first_route));
        let second_descriptor = active
            .catalog()
            .routes
            .get(second_route)
            .expect("second route installed");
        let expected_snapshot_version = second_snapshot.version().to_string();
        assert_eq!(
            second_descriptor
                .observability
                .get(MODULE_REGISTRY_SNAPSHOT_VERSION_OBSERVABILITY_KEY)
                .map(String::as_str),
            Some(expected_snapshot_version.as_str())
        );
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
        assert_eq!(table.native_rust_bindings.len(), 4);
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
