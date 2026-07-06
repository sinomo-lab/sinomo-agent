//! Daemon runtime registry: the single owner of internal module registration,
//! module->route conversion, network-module surface adaptation, and the
//! composed durable storage layout.
//!
//! This machinery previously lived in the `sinomo-runtime` facade; the facade
//! now re-exports it from here so it stays behavior-free while the daemon owns
//! module and service loading (stage E of
//! docs/plans/r0-r1-crate-boundary-refactor.md).

#[allow(unused_imports)]
use sinomo_core::storage::{
    StorageContractError, StorageLayout, StorageRegistry, StorageVerificationReport,
};

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
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

pub struct NativeRustModuleBinding {
    pub registration_key: &'static str,
    pub register_services: Option<
        fn(
            sinomo_core::service::ServiceRegistryBuilder,
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
        registration_key: sinomo_core::network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
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
        .register(network_module_registration())?
        .register(sinomo_rollout::rollout_module_registration())?
        .register(sinomo_observe::observe_module_registration())?
        .try_build()?;
    Ok(registry)
}

pub fn module_object_rpc_route_descriptor_to_network(
    descriptor: &sinomo_module::ModuleObjectRpcRouteDescriptor,
) -> sinomo_core::network::ObjectRouteDescriptor {
    let mut route = sinomo_core::network::ObjectRouteDescriptor::new(
        descriptor.route.clone(),
        descriptor.category.clone(),
        format!(
            "module object RPC operation {}",
            descriptor.operation.as_str()
        ),
        "sinomo_core::network::ObjectRpcRequest",
        "sinomo_core::network::ObjectRpcResponse",
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
) -> Result<Vec<sinomo_core::network::ObjectRouteDescriptor>, sinomo_module::ModuleSurfaceError> {
    Ok(registration
        .surfaces
        .object_rpc_route_descriptors()?
        .iter()
        .map(module_object_rpc_route_descriptor_to_network)
        .collect::<Vec<_>>())
}

pub fn module_registry_object_rpc_route_descriptors(
    registry: &ModuleRegistry,
) -> Result<Vec<sinomo_core::network::ObjectRouteDescriptor>, sinomo_module::ModuleSurfaceError> {
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
) -> Result<Vec<sinomo_core::network::ObjectRouteDescriptor>, sinomo_module::ModuleSurfaceError> {
    module_registry_object_rpc_route_descriptors(snapshot.registry())
}

pub const MODULE_REGISTRY_SNAPSHOT_VERSION_OBSERVABILITY_KEY: &str =
    "module.registry.snapshot_version";

pub fn install_module_registry_snapshot_routes<F, Fut>(
    router: &sinomo_core::network::ObjectRpcRouter,
    snapshot: &ModuleRegistrySnapshot,
    handler: F,
) -> Result<Arc<sinomo_core::network::ObjectRpcRouteRegistry>, sinomo_core::network::NetworkError>
where
    F: Fn(sinomo_core::network::ObjectRpcRequest) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<
            Output = Result<
                sinomo_core::network::ObjectRpcResponse,
                sinomo_core::network::NetworkError,
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
) -> sinomo_core::network::NetworkError {
    sinomo_core::network::NetworkError::Serialization {
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
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    for binding in internal_native_rust_module_bindings() {
        if let Some(register_storage_paths) = binding.register_storage_paths {
            register_storage_paths(layout)?;
        }
    }
    Ok(())
}

// --- network module surface adaptation (absorbed from sinomo-network-module) ---

pub fn network_module_id() -> sinomo_module::ModuleId {
    sinomo_module::ModuleId::from_legacy_named(
        sinomo_core::network::SINOMO_NETWORK_MODULE_LEGACY_ID,
    )
    .expect("valid network module id")
}

pub fn network_module_surfaces() -> sinomo_module::ModuleSurfaces {
    let mut surfaces = sinomo_module::ModuleSurfaces::default()
        .with_storage_owner(sinomo_core::network::SINOMO_NETWORK_MODULE_OWNER);

    for tool in sinomo_core::network::NETWORK_OBJECT_RPC_TOOL_SURFACES {
        surfaces = surfaces.with_object_rpc_route(sinomo_module::ModuleObjectRpcRoute::new(
            tool.route,
            tool.category,
            tool.operation,
            module_route_auth(tool.auth),
        ));
    }

    for resource in sinomo_core::network::NETWORK_DECLARATIVE_RESOURCE_SURFACES {
        let mut export = sinomo_module::ModuleDeclarativeResourceExport::new(
            resource.id,
            module_declarative_resource_kind(resource.kind),
            resource.path,
        )
        .with_description(resource.description);
        for family in resource.model_families {
            export = export.with_model_family(*family);
        }
        for tag in resource.activation_tags {
            export = export.with_activation_tag(*tag);
        }
        for capability in resource.required_capabilities {
            export = export.with_required_capability(network_capability(capability));
        }
        surfaces = surfaces.with_declarative_resource(export);
    }

    surfaces.capabilities = network_module_capabilities();
    surfaces
}

pub fn network_module_registration() -> sinomo_module::ModuleRegistration {
    let mut registration = sinomo_module::ModuleRegistration::native_rust(
        network_module_id(),
        env!("CARGO_PKG_VERSION"),
        sinomo_core::network::SINOMO_NETWORK_MODULE_OWNER,
        sinomo_core::network::SINOMO_NETWORK_MODULE_OWNER,
        sinomo_core::network::NETWORK_PROTOCOL,
        sinomo_core::TrustRing::R1TrustedRuntime,
        sinomo_core::network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
    );
    registration.manifest.capabilities = network_module_capabilities();
    registration.manifest.limits = sinomo_core::ModuleResourceLimits {
        network: sinomo_core::NetworkAccess::Allowlisted,
        filesystem: sinomo_core::FileSystemAccess::Scoped,
        ..sinomo_core::ModuleResourceLimits::default()
    };
    registration.surfaces = network_module_surfaces();
    registration
}

pub fn network_module_capabilities() -> sinomo_core::ModuleCapabilities {
    sinomo_core::ModuleCapabilities {
        imports: sinomo_core::network::NETWORK_CAPABILITY_IMPORTS
            .iter()
            .map(|capability| network_capability(capability))
            .collect(),
        exports: sinomo_core::network::NETWORK_CAPABILITY_EXPORTS
            .iter()
            .map(|capability| network_capability(capability))
            .collect(),
    }
}

fn module_route_auth(
    auth: sinomo_core::network::NetworkModuleRouteAuth,
) -> sinomo_module::ModuleRouteAuth {
    match auth {
        sinomo_core::network::NetworkModuleRouteAuth::LocalCommand => {
            sinomo_module::ModuleRouteAuth::LocalCommand
        }
        sinomo_core::network::NetworkModuleRouteAuth::LocalOrClusterMember => {
            sinomo_module::ModuleRouteAuth::LocalOrClusterMember
        }
        sinomo_core::network::NetworkModuleRouteAuth::ClusterMember => {
            sinomo_module::ModuleRouteAuth::ClusterMember
        }
        sinomo_core::network::NetworkModuleRouteAuth::Admin => {
            sinomo_module::ModuleRouteAuth::Admin
        }
    }
}

fn module_declarative_resource_kind(
    kind: sinomo_core::network::NetworkDeclarativeResourceKind,
) -> sinomo_module::ModuleDeclarativeResourceKind {
    match kind {
        sinomo_core::network::NetworkDeclarativeResourceKind::Skill => {
            sinomo_module::ModuleDeclarativeResourceKind::Skill
        }
        sinomo_core::network::NetworkDeclarativeResourceKind::Prompt => {
            sinomo_module::ModuleDeclarativeResourceKind::Prompt
        }
    }
}

fn network_capability(name: &str) -> sinomo_core::CapabilityName {
    sinomo_core::CapabilityName::new(name).expect("valid network module capability")
}

// --- runtime network-module loading (stage C of
// docs/plans/r0-r1-crate-boundary-refactor.md) ---
//
// Built-in transports (Loopback/LAN-TCP) keep their in-crate dial paths
// inside `sinomo-network`; they are NOT wrapped as registry modules. The
// registry carries native-dylib network modules, which routing consults
// before the built-in paths.

/// The daemon's loaded network modules: the registry routing dispatches
/// through plus the loader-state module id behind each network-module id
/// (used to persist the enabled flag).
pub struct DaemonNetworkModules {
    pub registry: Arc<sinomo_core::network::NetworkModuleRegistry>,
    /// network-module id (`NetworkModuleDescriptor::id`) -> dylib module id.
    pub loader_module_ids: std::collections::BTreeMap<String, sinomo_module::ModuleId>,
}

static DAEMON_NETWORK_MODULES: std::sync::Mutex<Option<Arc<DaemonNetworkModules>>> =
    std::sync::Mutex::new(None);

/// The most recently loaded daemon network modules (process-wide, like the
/// `sinomo_network` registry handle they mirror).
pub fn daemon_network_modules() -> Option<Arc<DaemonNetworkModules>> {
    DAEMON_NETWORK_MODULES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Persisted enabled flag for a network module's dylib. Network modules reuse
/// the `NativeRustModuleRuntimeState`-style enablement store (keyed by module
/// id under `module/loader-state/native-rust`) rather than growing a parallel
/// mechanism; native-dylib module ids cannot collide with native-rust
/// registration keys.
pub fn network_module_enabled_state(
    data_dir_root: &std::path::Path,
    module_id: &sinomo_module::ModuleId,
) -> bool {
    sinomo_module::read_native_rust_module_runtime_state(data_dir_root, module_id)
        .ok()
        .flatten()
        .map(|state| state.enabled)
        .unwrap_or(true)
}

/// Persists a network module's enabled flag (see
/// [`network_module_enabled_state`] for the store choice).
pub fn persist_network_module_enabled_state(
    data_dir_root: &std::path::Path,
    module_id: &sinomo_module::ModuleId,
    enabled: bool,
    reason: impl Into<String>,
) -> Result<(), sinomo_module::ModuleLoaderStoreError> {
    sinomo_module::set_native_rust_module_enabled(
        data_dir_root,
        module_id,
        enabled,
        "daemon-network-modules",
        reason,
    )
    .map(|_| ())
}

/// Provides `context` to every registered network module and starts the
/// enabled ones whose listener is not yet running — the recovery path for
/// modules registered before the node had a cluster identity (pre-cluster
/// daemon that later runs `cluster init`/`join`). Idempotent per module.
///
/// Returns `true` when at least one module started.
pub fn start_enabled_network_modules_with_context(
    context: &sinomo_core::network::NetworkModuleContext,
) -> bool {
    let Some(modules) = daemon_network_modules() else {
        return false;
    };
    let mut any_started = false;
    for (id, result) in modules.registry.provide_context_and_start_enabled(context) {
        match result {
            Ok(()) => {
                any_started = true;
                tracing::info!(
                    "sinomo daemon network module {id} started (cluster context now available)"
                );
            }
            Err(error) => {
                tracing::warn!("sinomo daemon network module {id} start failed: {error}");
            }
        }
    }
    any_started
}

/// Builds the daemon's [`sinomo_core::network::NetworkModuleRegistry`] from the
/// installed native-dylib loader states, starts enabled modules, and installs
/// the registry process-wide via
/// [`sinomo_core::network::set_network_module_registry`].
///
/// Per-module failures (bad hash, missing entry symbol, start errors) are
/// logged and skipped so one broken module cannot take the daemon down.
/// `context` is `None` before the node has a cluster identity; modules are
/// still registered (dial-only) and start once toggled with a context later.
pub fn load_daemon_network_module_registry(
    data_dir_root: &std::path::Path,
    context: Option<sinomo_core::network::NetworkModuleContext>,
) -> Arc<DaemonNetworkModules> {
    let registry = Arc::new(sinomo_core::network::NetworkModuleRegistry::new());
    let mut loader_module_ids = std::collections::BTreeMap::new();

    let states = match sinomo_module::read_native_dylib_module_loader_states(data_dir_root) {
        Ok(states) => states,
        Err(error) => {
            tracing::warn!("sinomo daemon network module loader states unreadable: {error}");
            Vec::new()
        }
    };
    for state in states {
        match load_daemon_network_module_from_loader_state(data_dir_root, &state, context.as_ref())
        {
            Ok(Some((network_module_id, module, enabled, started))) => {
                registry.register(module, context.clone(), enabled, started);
                loader_module_ids.insert(network_module_id, state.module_id.clone());
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    "sinomo daemon network module {} load failed: {error}",
                    state.module_id
                );
            }
        }
    }

    // The registry is process-wide; in the unit-test binary a lifecycle test
    // booting a daemon would replace it mid-run and perturb concurrently
    // running endpoint-ranking tests. Tests exercise the loader through the
    // returned handle instead.
    #[cfg(not(test))]
    sinomo_core::network::set_network_module_registry(registry.clone());
    let modules = Arc::new(DaemonNetworkModules {
        registry,
        loader_module_ids,
    });
    *DAEMON_NETWORK_MODULES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(modules.clone());
    modules
}

/// Loads one loader state's dylib as a network module when its descriptor
/// surfaces declare `network_transports`. Returns `Ok(None)` for modules
/// without a network surface.
fn load_daemon_network_module_from_loader_state(
    data_dir_root: &std::path::Path,
    state: &sinomo_module::NativeDylibModuleLoaderState,
    context: Option<&sinomo_core::network::NetworkModuleContext>,
) -> Result<
    Option<(
        String,
        Arc<dyn sinomo_core::network::NetworkModule>,
        bool,
        bool,
    )>,
    sinomo_core::network::NetworkError,
> {
    let network_error = |detail: String| sinomo_core::network::NetworkError::Io {
        operation: format!("load network module {}", state.module_id),
        detail,
    };
    // The registration carries the descriptor surfaces (network_transports)
    // validated against the loader state.
    let registration =
        sinomo_module::native_dylib_module_registration_from_loader_state(data_dir_root, state)
            .map_err(|error| network_error(error.to_string()))?;
    if registration.surfaces.network_transports.is_empty() {
        return Ok(None);
    }
    let library_path = sinomo_module::native_module_cache_path(
        data_dir_root,
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    // SAFETY: trusted native-dylib module cache, blake3-verified against the
    // loader state's content hash before any code from it runs.
    let library = unsafe {
        sinomo_module::NativeLibrary::load_from_content_addressed_path(
            &library_path,
            &state.content_hash,
        )
    }
    .map_err(|error| network_error(error.to_string()))?;
    // SAFETY: symbol type matches the pinned v1 entry signature.
    let entry = unsafe {
        library.symbol::<sinomo_core::network::network_module_abi::SinomoNetworkModuleCreateV1Fn>(
            sinomo_core::network::network_module_abi::SINOMO_NETWORK_MODULE_ENTRY_SYMBOL,
        )
    }
    .map_err(|error| network_error(error.to_string()))?;
    let mut instance: *mut std::ffi::c_void = std::ptr::null_mut();
    let mut vtable = std::mem::MaybeUninit::<
        sinomo_core::network::network_module_abi::SinomoNetworkModuleVtableV1,
    >::uninit();
    // SAFETY: calling the module's exported entry with valid out pointers.
    let status = unsafe { entry(&mut instance, vtable.as_mut_ptr()) };
    if status != sinomo_core::network::network_module_abi::SINOMO_NETWORK_MODULE_STATUS_OK {
        return Err(network_error(format!(
            "network module entry returned status {status}"
        )));
    }
    // SAFETY: entry returned OK, so the vtable was fully initialized.
    let vtable = unsafe { vtable.assume_init() };
    // SAFETY: instance/vtable come from the entry call above; the
    // NativeLibrary keep-alive token keeps the dylib mapped for the adapter's
    // lifetime (the registry never drops registered modules).
    let module = unsafe {
        sinomo_core::network::FfiNetworkModule::from_entry_parts(
            instance,
            vtable,
            Box::new(library),
        )
    }?;
    let descriptor = sinomo_core::network::NetworkModule::descriptor(&module);
    let enabled = network_module_enabled_state(data_dir_root, &state.module_id);
    let module: Arc<dyn sinomo_core::network::NetworkModule> = Arc::new(module);
    let mut started = false;
    if enabled && let Some(context) = context {
        if let Err(error) = module.start(context) {
            // Registered but not listening; dials may still work and the
            // module can be toggled once the failure cause is fixed.
            tracing::warn!(
                "sinomo daemon network module {} start failed: {error}",
                descriptor.id
            );
        } else {
            started = true;
            tracing::info!(
                "sinomo daemon network module {} started (transports {:?})",
                descriptor.id,
                descriptor.transports
            );
        }
    }
    Ok(Some((descriptor.id, module, enabled, started)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{FileSystemAccess, ModuleKind, NetworkAccess, TrustRing};

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
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            sinomo_core::network::NETWORK_OBJECT_RPC_ROUTES.len()
        );
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
