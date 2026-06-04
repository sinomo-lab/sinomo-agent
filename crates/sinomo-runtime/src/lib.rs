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
    pub use sinomo_api as api;
    pub use sinomo_config as config;
    pub use sinomo_daemon as daemon;
    pub use sinomo_device as device;
    pub use sinomo_dfs as dfs;
    pub use sinomo_eval as eval;
    pub use sinomo_events as events;
    pub use sinomo_kv as kv;
    pub use sinomo_local as local;
    pub use sinomo_module as module;
    pub use sinomo_module_api as module_api;
    pub use sinomo_network as network;
    pub use sinomo_node as node;
    pub use sinomo_policy as policy;
    pub use sinomo_registry as registry;
    pub use sinomo_rollout as rollout;
    // The `sandbox` facade is always the lightweight contracts crate. Runtime
    // execution is provided by `sandbox_wasix_host`, which loads an active
    // native-dylib module from content-addressed module cache.
    pub use sinomo_sandbox as sandbox;
    pub use sinomo_sandbox_wasix_api as sandbox_wasix_api;
    #[cfg(feature = "sandbox")]
    pub use sinomo_sandbox_wasix_host as sandbox_wasix_host;
    pub use sinomo_service as service;
    pub use sinomo_supervision as supervision;

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
        pub use sinomo_module::{ModuleRegistration, ModuleRegistry, ModuleRegistryError};

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
                .register(sinomo_rollout::rollout_module_registration())?
                .build();
            Ok(registry)
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
    api, config, daemon, device, dfs, eval, events, kv, local, module, module_api, modules,
    network, node, policy, registry, rollout, sandbox, sandbox_wasix_api, service, storage,
    supervision,
};

#[cfg(test)]
mod tests {
    use super::modules::{
        internal_module_registry, internal_module_table, native_rust_binding_for_registration,
    };
    use super::storage::{StorageCoverage, StoragePathOwner, sinomo_data_dir_layout};
    use super::{core, dfs, module, rollout, service};

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

        let report = layout
            .verify_relative_paths([
                "daemon/status.sobj",
                "dfs/metadata/root/records/filesystems/root/object.sobj",
                "dfs/metadata-replication-cursors/01kr4w089vqgdxpb84t6bq1qxn/root.json",
                "sandbox/runtime/active.sobj",
                "sandbox/module-artifacts/artifact_candidate.wasm",
                "sandbox/module-wasm-artifacts.sobj",
                "module/loader-state/01kr3d54d45zkm0p0xgps8k7mt.sobj",
                "module/lifecycle/unload-records/01kr3d54d45zkm0p0xgps8k7mt.sobj",
                "module/local-command-artifacts.sobj",
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
        assert_eq!(table.native_rust_bindings.len(), 2);
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
}
