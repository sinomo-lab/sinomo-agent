//! Daemon runtime registry: composition for internal module registration and
//! the durable storage layout assembled from owner crates.
//!
//! This machinery previously lived in the `sinomo-runtime` facade; the facade
//! now re-exports it from here so it stays behavior-free while the daemon owns
//! process composition (stage E of
//! docs/plans/r0-r1-crate-boundary-refactor.md).

pub use sinomo_core::storage::*;

pub fn sinomo_data_dir_layout() -> Result<StorageLayout, StorageContractError> {
    let mut layout = StorageLayout::new();
    sinomo_core::network::register_storage_paths(&mut layout)?;
    sinomo_core::cluster::register_storage_paths(&mut layout)?;
    sinomo_core::kv::register_storage_paths(&mut layout)?;
    crate::register_storage_paths(&mut layout)?;
    sinomo_config::register_storage_paths(&mut layout)?;
    sinomo_module::register_storage_paths(&mut layout)?;
    sinomo_registry::register_storage_paths(&mut layout)?;
    sinomo_core::node::register_storage_paths(&mut layout)?;
    register_internal_module_storage_paths(&mut layout)?;
    sinomo_sandbox::register_storage_paths(&mut layout)?;
    sinomo_core::local::register_storage_paths(&mut layout)?;
    sinomo_core::events::register_storage_paths(&mut layout)?;
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

pub use sinomo_module::{
    ModuleRegistration, ModuleRegistry, ModuleRegistryError, ModuleRegistrySnapshot,
    ModuleRegistrySnapshotStore,
};
pub struct NativeRustModuleBinding {
    pub registration_key: &'static str,
    pub register_services: Option<
        fn(
            sinomo_core::service::ServiceRegistryBuilder,
        ) -> sinomo_core::service::ServiceRegistryBuilder,
    >,
    pub register_services_for_data_dir: Option<
        fn(
            sinomo_core::service::ServiceRegistryBuilder,
            &std::path::Path,
        ) -> sinomo_core::service::ServiceRegistryBuilder,
    >,
    pub register_storage_paths: Option<
        fn(
            &mut sinomo_core::storage::StorageLayout,
        ) -> Result<(), sinomo_core::storage::StorageContractError>,
    >,
}

pub struct InternalModuleTable {
    pub registry: ModuleRegistry,
    pub native_rust_bindings: &'static [NativeRustModuleBinding],
}

const INTERNAL_NATIVE_RUST_MODULE_BINDINGS: &[NativeRustModuleBinding] = &[
    NativeRustModuleBinding {
        registration_key: sinomo_build::SINOMO_BUILD_NATIVE_REGISTRATION_KEY,
        register_services: Some(sinomo_build::register_services),
        register_services_for_data_dir: Some(sinomo_build::register_services_for_data_dir),
        register_storage_paths: Some(sinomo_build::register_storage_paths),
    },
    NativeRustModuleBinding {
        registration_key: sinomo_dfs::SINOMO_DFS_NATIVE_REGISTRATION_KEY,
        register_services: Some(sinomo_dfs::register_services),
        register_services_for_data_dir: None,
        register_storage_paths: Some(sinomo_dfs::register_storage_paths),
    },
    NativeRustModuleBinding {
        registration_key: sinomo_dfs_nfs::SINOMO_DFS_NFS_NATIVE_REGISTRATION_KEY,
        register_services: None,
        register_services_for_data_dir: None,
        register_storage_paths: None,
    },
    NativeRustModuleBinding {
        registration_key: sinomo_rollout::SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY,
        register_services: Some(sinomo_rollout::register_services),
        register_services_for_data_dir: None,
        register_storage_paths: Some(sinomo_rollout::register_storage_paths),
    },
    NativeRustModuleBinding {
        registration_key: sinomo_core::network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
        register_services: None,
        register_services_for_data_dir: None,
        register_storage_paths: None,
    },
    NativeRustModuleBinding {
        registration_key: sinomo_observe::SINOMO_OBSERVE_NATIVE_REGISTRATION_KEY,
        register_services: Some(sinomo_observe::register_services),
        register_services_for_data_dir: None,
        register_storage_paths: Some(sinomo_observe::register_storage_paths),
    },
];

type StartNativeRustModuleRuntimeTask =
    fn(
        &crate::DaemonPaths,
        &tokio::runtime::Handle,
        tokio::sync::watch::Receiver<bool>,
        &str,
        bool,
    ) -> Result<Option<NativeRustModuleRuntimeTask>, crate::DaemonError>;

struct NativeRustModuleLifecycleBinding {
    registration_key: &'static str,
    start_runtime_task: StartNativeRustModuleRuntimeTask,
}

const INTERNAL_NATIVE_RUST_MODULE_LIFECYCLE_BINDINGS: &[NativeRustModuleLifecycleBinding] = &[
    NativeRustModuleLifecycleBinding {
        registration_key: sinomo_rollout::SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY,
        start_runtime_task: crate::control::start_daemon_rollout_telemetry_runtime_task,
    },
    NativeRustModuleLifecycleBinding {
        registration_key: sinomo_dfs_nfs::SINOMO_DFS_NFS_NATIVE_REGISTRATION_KEY,
        start_runtime_task: start_nfs_mount_runtime_task,
    },
];

pub(crate) struct NativeRustModuleRuntimeTask {
    label: &'static str,
    task: tokio::task::JoinHandle<()>,
}

impl NativeRustModuleRuntimeTask {
    pub(crate) fn new(label: &'static str, task: tokio::task::JoinHandle<()>) -> Self {
        Self { label, task }
    }

    pub(crate) fn into_parts(self) -> (&'static str, tokio::task::JoinHandle<()>) {
        (self.label, self.task)
    }
}

pub fn internal_module_table() -> Result<InternalModuleTable, ModuleRegistryError> {
    let registry = internal_module_registry()?;
    Ok(InternalModuleTable {
        registry,
        native_rust_bindings: internal_native_rust_module_bindings(),
    })
}

pub fn internal_module_registry() -> Result<ModuleRegistry, ModuleRegistryError> {
    let registry = sinomo_module::ModuleRegistry::builder()
        .register(sinomo_build::build_module_registration())?
        .register(sinomo_dfs::dfs_module_registration())?
        .register(sinomo_dfs_nfs::nfs_module_registration())?
        .register(network_module_registration())?
        .register(sinomo_rollout::rollout_module_registration())?
        .register(sinomo_observe::observe_module_registration())?
        .try_build()?;
    Ok(registry)
}

/// Compose settings from every built-in module registration without requiring
/// clients to name individual modules. Definitions already present in the
/// host catalog may repeat only when byte-for-byte equal.
pub fn register_internal_module_config_definitions(
    mut builder: sinomo_config::ConfigRegistryBuilder,
) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
    let registry = internal_module_registry().map_err(|error| error.to_string())?;
    for registration in registry.registrations() {
        for definition in registration
            .surfaces
            .config_definitions()
            .map_err(|error| error.to_string())?
        {
            if let Ok(existing) = builder.get(definition.key.as_str()) {
                if existing == &definition {
                    continue;
                }
                return Err(format!(
                    "module {} settings key {} conflicts with an existing host definition",
                    registration.module_id(),
                    definition.key
                ));
            }
            builder = builder
                .register(definition)
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(builder)
}

fn start_nfs_mount_runtime_task(
    paths: &crate::DaemonPaths,
    runtime: &tokio::runtime::Handle,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    _label: &str,
    maintain_cluster_runtime: bool,
) -> Result<Option<NativeRustModuleRuntimeTask>, crate::DaemonError> {
    if !maintain_cluster_runtime {
        return Ok(None);
    }
    let data_root = paths
        .root
        .parent()
        .ok_or_else(|| crate::DaemonError::Parse {
            detail: format!(
                "daemon path {} has no Sinomo data-directory parent",
                paths.root.display()
            ),
        })?;
    let data_dir = sinomo_core::local::SinomoDataDir::new(data_root);
    Ok(Some(NativeRustModuleRuntimeTask::new(
        "NFS mount supervisor",
        sinomo_dfs_nfs::spawn_nfs_mount_supervisor(data_dir, runtime, shutdown_rx),
    )))
}

pub use sinomo_module::{
    module_object_rpc_route_descriptor_to_service,
    module_registration_object_rpc_route_descriptors, module_registry_object_rpc_route_descriptors,
    module_registry_snapshot_object_rpc_route_descriptors,
};

pub fn internal_native_rust_module_bindings() -> &'static [NativeRustModuleBinding] {
    INTERNAL_NATIVE_RUST_MODULE_BINDINGS
}

/// Starts lifecycle workers contributed by the selected native-Rust modules.
///
/// The daemon control loop supplies process resources and supervises returned
/// tasks; module-specific activation and worker construction stay behind this
/// registration table beside each module's service binding.
pub(crate) fn start_internal_native_rust_module_runtime_tasks(
    registration_keys: impl IntoIterator<Item = &'static str>,
    paths: &crate::DaemonPaths,
    runtime: &tokio::runtime::Handle,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    label: &str,
    maintain_cluster_runtime: bool,
) -> Result<Vec<NativeRustModuleRuntimeTask>, crate::DaemonError> {
    let mut tasks = Vec::new();
    for registration_key in registration_keys {
        let Some(binding) = INTERNAL_NATIVE_RUST_MODULE_LIFECYCLE_BINDINGS
            .iter()
            .find(|binding| binding.registration_key == registration_key)
        else {
            continue;
        };
        if let Some(task) = (binding.start_runtime_task)(
            paths,
            runtime,
            shutdown_rx.clone(),
            label,
            maintain_cluster_runtime,
        )? {
            tasks.push(task);
        }
    }
    Ok(tasks)
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

/// Applies the service registrar declared by an internal native-Rust module.
///
/// The daemon composition root selects active modules; the binding table owns
/// how each selected module contributes services. This keeps the daemon from
/// constructing domain service implementations directly.
pub fn register_internal_native_rust_module_services(
    builder: sinomo_core::service::ServiceRegistryBuilder,
    registration_keys: impl IntoIterator<Item = &'static str>,
    data_dir_root: &std::path::Path,
) -> sinomo_core::service::ServiceRegistryBuilder {
    registration_keys.into_iter().fold(builder, |builder, key| {
        let binding = native_rust_module_binding(key)
            .unwrap_or_else(|| panic!("missing internal native-Rust module binding for {key}"));
        match binding.register_services_for_data_dir {
            Some(register) => register(builder, data_dir_root),
            None => {
                let register = binding.register_services.unwrap_or_else(|| {
                    panic!("internal native-Rust module binding {key} has no service registrar")
                });
                register(builder)
            }
        }
    })
}

pub fn register_internal_module_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    for binding in internal_native_rust_module_bindings() {
        if let Some(register_storage_paths) = binding.register_storage_paths {
            register_storage_paths(layout)?;
        }
    }
    Ok(())
}

pub use sinomo_module::{
    ProcessNetworkModules as DaemonNetworkModules,
    load_process_network_module_registry as load_daemon_network_module_registry,
    network_module_capabilities, network_module_enabled_state, network_module_id,
    network_module_registration, network_module_surfaces, persist_network_module_enabled_state,
    process_network_modules as daemon_network_modules, start_enabled_network_modules_with_context,
};

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{FileSystemAccess, ModuleKind, NetworkAccess, TrustRing};

    #[test]
    fn daemon_sandbox_lifecycle_recovery_state_is_registered() {
        let layout = sinomo_data_dir_layout().expect("data-dir layout");
        let report = layout
            .verify_relative_paths([
                "daemon/sandbox-lifecycle-recovery/last-safety-sweep",
                "daemon/sandbox-lifecycle-recovery/pending/sandbox_marker",
                "shadow-daemon/sandbox-lifecycle-recovery/last-safety-sweep",
                "shadow-daemon/sandbox-lifecycle-recovery/pending/sandbox_marker",
            ])
            .expect("verify lifecycle recovery paths");

        assert!(report.is_complete(), "{:#?}", report.unregistered);
        assert_eq!(report.covered.len(), 4);
    }

    #[test]
    fn module_route_local_permissions_propagate_to_network_descriptor() {
        let descriptor = sinomo_module::ModuleObjectRpcRoute::new(
            "/remote/status",
            "remote.management",
            "remote.management.status.read",
            sinomo_module::ModuleRouteAuth::Admin,
        )
        .with_request_body_mode(sinomo_module_api::NativeModuleBodyMode::Optional)
        .with_response_body_mode(sinomo_module_api::NativeModuleBodyMode::Required)
        .require_local_permissions(["remote-management", "device:display"])
        .descriptor()
        .expect("module route descriptor");

        let network = module_object_rpc_route_descriptor_to_service(&descriptor);

        assert_eq!(
            network.required_local_permissions,
            vec!["remote-management", "device:display"]
        );
        assert_eq!(
            network.request_body_mode,
            sinomo_core::service::ServiceBodyMode::Optional
        );
        assert_eq!(
            network.response_body_mode,
            sinomo_core::service::ServiceBodyMode::Required
        );
    }

    #[test]
    fn build_module_is_registered_with_daemon_storage_binding() {
        let table = internal_module_table().expect("internal module table");
        let registration = table
            .registry
            .get(&sinomo_build::build_module_id())
            .expect("build module registration");
        let binding =
            native_rust_binding_for_registration(registration).expect("build native-rust binding");

        assert_eq!(
            binding.registration_key,
            sinomo_build::SINOMO_BUILD_NATIVE_REGISTRATION_KEY
        );
        assert!(binding.register_services.is_some());
        assert!(binding.register_services_for_data_dir.is_some());
        assert!(binding.register_storage_paths.is_some());
    }

    #[test]
    fn native_runtime_lifecycle_hooks_are_attached_to_registered_modules() {
        assert_eq!(INTERNAL_NATIVE_RUST_MODULE_LIFECYCLE_BINDINGS.len(), 2);
        for lifecycle in INTERNAL_NATIVE_RUST_MODULE_LIFECYCLE_BINDINGS {
            assert!(
                native_rust_module_binding(lifecycle.registration_key).is_some(),
                "lifecycle hook {} must share a native module registration",
                lifecycle.registration_key
            );
        }
        assert_eq!(
            INTERNAL_NATIVE_RUST_MODULE_LIFECYCLE_BINDINGS[0].registration_key,
            sinomo_rollout::SINOMO_ROLLOUT_NATIVE_REGISTRATION_KEY
        );
        assert_eq!(
            INTERNAL_NATIVE_RUST_MODULE_LIFECYCLE_BINDINGS[1].registration_key,
            sinomo_dfs_nfs::SINOMO_DFS_NFS_NATIVE_REGISTRATION_KEY
        );
    }

    #[test]
    fn nfs_module_registers_its_own_settings_and_lifecycle_binding() {
        let table = internal_module_table().expect("internal module table");
        let registration = table
            .registry
            .get(&sinomo_dfs_nfs::nfs_module_id())
            .expect("NFS module registration");
        let binding =
            native_rust_binding_for_registration(registration).expect("NFS native binding");
        assert_eq!(
            binding.registration_key,
            sinomo_dfs_nfs::SINOMO_DFS_NFS_NATIVE_REGISTRATION_KEY
        );
        let definitions = registration
            .surfaces
            .config_definitions()
            .expect("NFS settings surfaces");
        assert!(
            definitions.iter().any(|definition| {
                definition.key.as_str() == sinomo_dfs_nfs::NFS_MOUNT_CONFIG_KEY
            })
        );
        assert!(definitions.iter().any(|definition| {
            definition.key.as_str() == sinomo_dfs_nfs::NFS_MOUNT_STATUS_CONFIG_KEY
                && definition.read_only
        }));
        let catalog =
            register_internal_module_config_definitions(sinomo_config::ConfigRegistry::builder())
                .expect("compose built-in module config")
                .build();
        assert!(catalog.get(sinomo_dfs_nfs::NFS_MOUNT_CONFIG_KEY).is_ok());
    }

    #[test]
    fn network_registers_as_r1_native_rust_module() {
        let registration = network_module_registration();

        assert_eq!(registration.module_id(), &network_module_id());
        assert_eq!(registration.manifest.kind, ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.target_ring,
            TrustRing::R1TrustedRuntime
        );
        assert_eq!(
            registration.manifest.api_version,
            sinomo_core::network::NETWORK_PROTOCOL
        );
        assert_eq!(
            registration.manifest.limits.network,
            NetworkAccess::Allowlisted
        );
        assert_eq!(
            registration.manifest.limits.filesystem,
            FileSystemAccess::Scoped
        );
        assert_eq!(
            registration.backend,
            sinomo_module::ModuleBackendRegistration::NativeRust {
                registration: sinomo_module::NativeRustBackendRegistration::new(
                    sinomo_core::network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
                    sinomo_core::network::SINOMO_NETWORK_MODULE_OWNER,
                )
            }
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            vec![sinomo_core::network::SINOMO_NETWORK_MODULE_OWNER.to_string()]
        );
        assert_eq!(registration.surfaces.object_rpc_routes.len(), 3);
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(
                    |route| route.route == sinomo_core::network::CLUSTER_HEALTH_ROUTE
                        && route.category == sinomo_core::network::CLUSTER_HEALTH_RPC_CATEGORY
                        && route.operation == sinomo_core::network::CLUSTER_HEALTH_READ_CAPABILITY
                        && route.auth == sinomo_module::ModuleRouteAuth::LocalOrClusterMember
                )
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(
                    |route| route.route == sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE
                        && route.category == sinomo_core::network::CLUSTER_NETWORK_RPC_CATEGORY
                        && route.operation == sinomo_core::network::CLUSTER_NETWORK_PING_CAPABILITY
                        && route.auth == sinomo_module::ModuleRouteAuth::LocalOrClusterMember
                        && route.request_body_mode
                            == sinomo_module_api::NativeModuleBodyMode::Optional
                        && route.response_body_mode
                            == sinomo_module_api::NativeModuleBodyMode::Optional
                )
        );

        let descriptor = registration
            .surfaces
            .object_rpc_route_descriptor(sinomo_core::network::CLUSTER_HEALTH_ROUTE)
            .expect("network surfaces are valid")
            .expect("health route descriptor");
        assert_eq!(
            descriptor.auth_scope,
            sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope()
        );
        assert_eq!(
            descriptor.required_capabilities,
            vec![
                sinomo_core::CapabilityName::new(
                    sinomo_core::network::CLUSTER_HEALTH_READ_CAPABILITY
                )
                .expect("capability")
            ]
        );
        let ping_descriptor = registration
            .surfaces
            .object_rpc_route_descriptor(sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE)
            .expect("network surfaces are valid")
            .expect("network ping route descriptor");
        assert_eq!(
            ping_descriptor.request_body_mode,
            sinomo_module_api::NativeModuleBodyMode::Optional
        );
        assert_eq!(
            ping_descriptor.response_body_mode,
            sinomo_module_api::NativeModuleBodyMode::Optional
        );
        assert!(
            registration
                .surfaces
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str()
                    == sinomo_core::network::CLUSTER_HEALTH_READ_CAPABILITY)
        );
        assert!(
            registration
                .surfaces
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str()
                    == sinomo_core::network::CLUSTER_NETWORK_PING_CAPABILITY)
        );

        let skill = registration
            .surfaces
            .declarative_resources
            .iter()
            .find(|resource| {
                resource.id == sinomo_core::network::CLUSTER_NETWORK_INSPECTION_SKILL_ID
            })
            .expect("cluster network inspection skill");
        assert_eq!(
            skill.kind,
            sinomo_module::ModuleDeclarativeResourceKind::Skill
        );
        assert_eq!(
            skill.path,
            sinomo_core::network::CLUSTER_NETWORK_INSPECTION_SKILL_PATH
        );
        assert!(skill.model_families.iter().any(|family| family == "gpt"));
        assert!(skill.activation_tags.iter().any(|tag| tag == "cluster"));
        assert!(
            skill
                .required_capabilities
                .iter()
                .any(|capability| capability.as_str()
                    == sinomo_core::network::CLUSTER_HEALTH_READ_CAPABILITY)
        );

        let prompt = registration
            .surfaces
            .declarative_resources
            .iter()
            .find(|resource| {
                resource.id == sinomo_core::network::CLUSTER_NETWORK_INSPECTION_PROMPT_ID
            })
            .expect("cluster network inspection prompt");
        assert_eq!(
            prompt.kind,
            sinomo_module::ModuleDeclarativeResourceKind::Prompt
        );
        assert_eq!(
            prompt.path,
            sinomo_core::network::CLUSTER_NETWORK_INSPECTION_PROMPT_PATH
        );
    }
}
