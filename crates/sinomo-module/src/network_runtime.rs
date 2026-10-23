//! Module route adaptation and process-resident network-module loading.

use crate::{
    ModuleRegistration, ModuleRegistry, ModuleRegistrySnapshot, NATIVE_DYLIB_HOST_ABI_VERSION,
    NativeDylibModuleLoaderState,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub fn module_object_rpc_route_descriptor_to_service(
    descriptor: &crate::ModuleObjectRpcRouteDescriptor,
) -> sinomo_core::service::ServiceRoute {
    let mut route = sinomo_core::service::ServiceRoute::describe(
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
    for permission in &descriptor.required_local_permissions {
        route = route.require_local_permission(permission);
    }
    if descriptor.exact_member_identity {
        route = route.require_exact_member_identity();
    }
    route.request_body_mode = native_module_body_mode_to_service(descriptor.request_body_mode);
    route.response_body_mode = native_module_body_mode_to_service(descriptor.response_body_mode);
    route.concurrency_limit = descriptor.concurrency_limit;
    route.timeout_ms = descriptor.timeout_ms;
    route
}

fn native_module_body_mode_to_service(
    mode: sinomo_module_api::NativeModuleBodyMode,
) -> sinomo_core::service::ServiceBodyMode {
    match mode {
        sinomo_module_api::NativeModuleBodyMode::None => {
            sinomo_core::service::ServiceBodyMode::None
        }
        sinomo_module_api::NativeModuleBodyMode::Optional => {
            sinomo_core::service::ServiceBodyMode::Optional
        }
        sinomo_module_api::NativeModuleBodyMode::Required => {
            sinomo_core::service::ServiceBodyMode::Required
        }
    }
}

pub fn module_registration_object_rpc_route_descriptors(
    registration: &ModuleRegistration,
) -> Result<Vec<sinomo_core::service::ServiceRoute>, crate::ModuleSurfaceError> {
    Ok(registration
        .surfaces
        .object_rpc_route_descriptors()?
        .iter()
        .map(module_object_rpc_route_descriptor_to_service)
        .collect::<Vec<_>>())
}

pub fn module_registry_object_rpc_route_descriptors(
    registry: &ModuleRegistry,
) -> Result<Vec<sinomo_core::service::ServiceRoute>, crate::ModuleSurfaceError> {
    let mut seen_routes = BTreeSet::new();
    let mut descriptors = Vec::new();
    for registration in registry.registrations() {
        for descriptor in module_registration_object_rpc_route_descriptors(registration)? {
            if !seen_routes.insert(descriptor.route.clone()) {
                return Err(crate::ModuleSurfaceError::DuplicateObjectRpcRoute {
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
) -> Result<Vec<sinomo_core::service::ServiceRoute>, crate::ModuleSurfaceError> {
    module_registry_object_rpc_route_descriptors(snapshot.registry())
}

/// Native-dylib dispatch information projected from one immutable module
/// registry generation. Hosts attach process-specific authorization and
/// cancellation effects without rebuilding module-owned route semantics.
#[derive(Clone, Debug)]
pub struct NativeDylibModuleRouteTarget {
    pub module_id: crate::ModuleId,
    pub operation: String,
    pub request_body_mode: sinomo_core::service::ServiceBodyMode,
    pub response_body_mode: sinomo_core::service::ServiceBodyMode,
}

/// Service-facing projection of an immutable module registry snapshot.
#[derive(Debug)]
pub struct ModuleServiceSnapshot {
    pub snapshot: Arc<ModuleRegistrySnapshot>,
    pub routes: Vec<sinomo_core::service::ServiceRoute>,
    pub native_dylib_routes: BTreeMap<String, NativeDylibModuleRouteTarget>,
}

pub fn module_service_snapshot(
    snapshots: &crate::ModuleRegistrySnapshotStore,
) -> Result<ModuleServiceSnapshot, sinomo_core::network::NetworkError> {
    let snapshot = snapshots.active_snapshot().map_err(module_network_error)?;
    module_service_snapshot_from_snapshot(snapshot)
}

pub fn module_service_snapshot_from_snapshot(
    snapshot: Arc<ModuleRegistrySnapshot>,
) -> Result<ModuleServiceSnapshot, sinomo_core::network::NetworkError> {
    let mut seen_routes = BTreeSet::new();
    let mut routes = Vec::new();
    let mut native_dylib_routes = BTreeMap::new();
    for registration in snapshot.registry().registrations() {
        let component = crate::component_descriptor_from_registration(registration)
            .map_err(module_network_error)?;
        for descriptor in registration
            .surfaces
            .object_rpc_route_descriptors()
            .map_err(module_network_error)?
        {
            let operation = descriptor.operation.as_str().to_string();
            let request_body_mode =
                native_module_body_mode_to_service(descriptor.request_body_mode);
            let response_body_mode =
                native_module_body_mode_to_service(descriptor.response_body_mode);
            let route = crate::service_route_with_component_identity(
                module_object_rpc_route_descriptor_to_service(&descriptor),
                &component,
            );
            if !seen_routes.insert(route.route.clone()) {
                return Err(sinomo_core::network::NetworkError::Serialization {
                    detail: format!("duplicate module object rpc route {}", route.route),
                });
            }
            if matches!(
                &registration.backend,
                crate::ModuleBackendRegistration::NativeDylib { .. }
            ) {
                native_dylib_routes.insert(
                    route.route.clone(),
                    NativeDylibModuleRouteTarget {
                        module_id: registration.manifest.module_id.clone(),
                        operation,
                        request_body_mode,
                        response_body_mode,
                    },
                );
            }
            routes.push(route);
        }
    }
    Ok(ModuleServiceSnapshot {
        snapshot,
        routes,
        native_dylib_routes,
    })
}

/// Verify that a host service registered for a module-owned route preserves
/// the module descriptor's authorization contract.
pub fn validate_module_service_route_contract(
    descriptor: &sinomo_core::service::ServiceRoute,
    route: &sinomo_core::service::ServiceRoute,
) -> Result<(), sinomo_core::network::NetworkError> {
    if descriptor.category != route.category {
        return Err(sinomo_core::network::NetworkError::Serialization {
            detail: format!(
                "module route {} category {} does not match service category {}",
                descriptor.route, descriptor.category, route.category
            ),
        });
    }
    if descriptor.auth_scope != route.auth_scope {
        return Err(sinomo_core::network::NetworkError::Serialization {
            detail: format!(
                "module route {} auth scope {:?} does not match service auth scope {:?}",
                descriptor.route, descriptor.auth_scope, route.auth_scope
            ),
        });
    }
    if descriptor.required_capabilities != route.required_capabilities {
        return Err(sinomo_core::network::NetworkError::Serialization {
            detail: format!(
                "module route {} capabilities {:?} do not match service capabilities {:?}",
                descriptor.route, descriptor.required_capabilities, route.required_capabilities
            ),
        });
    }
    if descriptor.required_local_permissions != route.required_local_permissions {
        return Err(sinomo_core::network::NetworkError::Serialization {
            detail: format!(
                "module route {} local permissions {:?} do not match service local permissions {:?}",
                descriptor.route,
                descriptor.required_local_permissions,
                route.required_local_permissions
            ),
        });
    }
    Ok(())
}

fn module_network_error(error: impl std::fmt::Display) -> sinomo_core::network::NetworkError {
    sinomo_core::network::NetworkError::Serialization {
        detail: error.to_string(),
    }
}

pub fn network_module_id() -> crate::ModuleId {
    crate::ModuleId::from_legacy_named(sinomo_core::network::SINOMO_NETWORK_MODULE_ID)
        .expect("valid network module id")
}

pub fn network_module_surfaces() -> crate::ModuleSurfaces {
    let mut surfaces = crate::ModuleSurfaces::default()
        .with_storage_owner(sinomo_core::network::SINOMO_NETWORK_MODULE_OWNER);

    for (route, category, operation, request_body_mode, response_body_mode) in [
        (
            sinomo_core::network::CLUSTER_HEALTH_ROUTE,
            sinomo_core::network::CLUSTER_HEALTH_RPC_CATEGORY,
            sinomo_core::network::CLUSTER_HEALTH_READ_CAPABILITY,
            sinomo_module_api::NativeModuleBodyMode::None,
            sinomo_module_api::NativeModuleBodyMode::None,
        ),
        (
            sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
            sinomo_core::network::CLUSTER_NETWORK_RPC_CATEGORY,
            sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_READ_CAPABILITY,
            sinomo_module_api::NativeModuleBodyMode::None,
            sinomo_module_api::NativeModuleBodyMode::None,
        ),
        (
            sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE,
            sinomo_core::network::CLUSTER_NETWORK_RPC_CATEGORY,
            sinomo_core::network::CLUSTER_NETWORK_PING_CAPABILITY,
            sinomo_module_api::NativeModuleBodyMode::Optional,
            sinomo_module_api::NativeModuleBodyMode::Optional,
        ),
    ] {
        surfaces = surfaces.with_object_rpc_route(
            crate::ModuleObjectRpcRoute::new(
                route,
                category,
                operation,
                crate::ModuleRouteAuth::LocalOrClusterMember,
            )
            .with_request_body_mode(request_body_mode)
            .with_response_body_mode(response_body_mode),
        );
    }

    for resource in sinomo_core::network::NETWORK_DECLARATIVE_RESOURCE_SURFACES {
        let mut export = crate::ModuleDeclarativeResourceExport::new(
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

pub fn network_module_registration() -> crate::ModuleRegistration {
    let mut registration = crate::ModuleRegistration::native_rust(
        network_module_id(),
        env!("CARGO_PKG_VERSION"),
        sinomo_core::network::SINOMO_NETWORK_MODULE_OWNER,
        sinomo_core::network::SINOMO_NETWORK_MODULE_OWNER,
        sinomo_core::network::NETWORK_PROTOCOL,
        sinomo_core::TrustRing::R1TrustedRuntime,
        sinomo_core::network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
    )
    .with_display_name("Networking");
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

fn module_declarative_resource_kind(
    kind: sinomo_core::network::NetworkDeclarativeResourceKind,
) -> crate::ModuleDeclarativeResourceKind {
    match kind {
        sinomo_core::network::NetworkDeclarativeResourceKind::Skill => {
            crate::ModuleDeclarativeResourceKind::Skill
        }
        sinomo_core::network::NetworkDeclarativeResourceKind::Prompt => {
            crate::ModuleDeclarativeResourceKind::Prompt
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
pub struct ProcessNetworkModules {
    pub registry: Arc<sinomo_core::network::NetworkModuleRegistry>,
    /// network-module id (`NetworkModuleDescriptor::id`) -> dylib module id.
    pub loader_module_ids: std::collections::BTreeMap<String, crate::ModuleId>,
}

impl ProcessNetworkModules {
    pub fn unload_loader_module(
        &self,
        module_id: &crate::ModuleId,
        timeout: std::time::Duration,
    ) -> Result<
        Vec<sinomo_core::network::NetworkModuleUnloadReport>,
        sinomo_core::network::NetworkError,
    > {
        let network_module_ids = self
            .loader_module_ids
            .iter()
            .filter(|(_, loader_module_id)| *loader_module_id == module_id)
            .map(|(network_module_id, _)| network_module_id.clone())
            .collect::<Vec<_>>();
        let started = std::time::Instant::now();
        let mut reports = Vec::with_capacity(network_module_ids.len());
        for network_module_id in network_module_ids {
            let remaining = timeout
                .checked_sub(started.elapsed())
                .unwrap_or(std::time::Duration::ZERO);
            reports.push(self.registry.unregister(&network_module_id, remaining)?);
        }
        Ok(reports)
    }
}

static PROCESS_NETWORK_MODULES: std::sync::Mutex<Option<Arc<ProcessNetworkModules>>> =
    std::sync::Mutex::new(None);

/// The most recently loaded daemon network modules (process-wide, like the
/// `sinomo_network` registry handle they mirror).
pub fn process_network_modules() -> Option<Arc<ProcessNetworkModules>> {
    PROCESS_NETWORK_MODULES
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
    module_id: &crate::ModuleId,
) -> bool {
    crate::read_native_rust_module_runtime_state(data_dir_root, module_id)
        .ok()
        .flatten()
        .map(|state| state.enabled)
        .unwrap_or(true)
}

/// Persists a network module's enabled flag (see
/// [`network_module_enabled_state`] for the store choice).
pub fn persist_network_module_enabled_state(
    data_dir_root: &std::path::Path,
    module_id: &crate::ModuleId,
    enabled: bool,
    reason: impl Into<String>,
) -> Result<(), crate::ModuleLoaderStoreError> {
    crate::set_native_rust_module_enabled(
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
    let Some(modules) = process_network_modules() else {
        return false;
    };
    let mut any_started = false;
    for (id, result) in modules.registry.provide_context_and_start_enabled(context) {
        match result {
            Ok(()) => {
                any_started = true;
                tracing::info!(
                    "sinomo network module {id} started (cluster context now available)"
                );
            }
            Err(error) => {
                tracing::warn!("sinomo network module {id} start failed: {error}");
            }
        }
    }
    any_started
}

/// Whether a persisted native module matches this host platform and ABI.
pub fn native_dylib_loader_state_is_host_compatible(state: &NativeDylibModuleLoaderState) -> bool {
    native_dylib_loader_state_is_host_compatible_for(
        state,
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY,
    )
}

/// Deterministic platform variant used by rollout validation tests.
#[doc(hidden)]
pub fn native_dylib_loader_state_is_host_compatible_for(
    state: &NativeDylibModuleLoaderState,
    current_arch: &str,
    current_os: &str,
    current_family: &str,
) -> bool {
    let current_platform_triple = format!("{current_arch}-{current_os}-{current_family}");
    state.platform_triple == current_platform_triple
        && state.native_abi_version == NATIVE_DYLIB_HOST_ABI_VERSION
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
pub fn load_process_network_module_registry(
    data_dir_root: &std::path::Path,
    context: Option<sinomo_core::network::NetworkModuleContext>,
) -> Arc<ProcessNetworkModules> {
    let registry = Arc::new(sinomo_core::network::NetworkModuleRegistry::new());
    let mut loader_module_ids = std::collections::BTreeMap::new();

    let states = match crate::read_native_dylib_module_loader_states(data_dir_root) {
        Ok(states) => states,
        Err(error) => {
            tracing::warn!("sinomo network module loader states unreadable: {error}");
            Vec::new()
        }
    };
    for state in states {
        if !native_dylib_loader_state_is_host_compatible(&state) {
            tracing::warn!(
                "sinomo native module {} skipped because its build platform or native ABI does not match this host",
                state.module_id
            );
            continue;
        }
        match load_process_network_module_from_loader_state(data_dir_root, &state, context.as_ref())
        {
            Ok(Some((network_module_id, module, enabled, started))) => {
                registry.register(module, context.clone(), enabled, started);
                loader_module_ids.insert(network_module_id, state.module_id.clone());
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    "sinomo network module {} load failed: {error}",
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
    let modules = Arc::new(ProcessNetworkModules {
        registry,
        loader_module_ids,
    });
    *PROCESS_NETWORK_MODULES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(modules.clone());
    modules
}

/// Loads one loader state's dylib as a network module when its descriptor
/// surfaces declare `network_transports`. Returns `Ok(None)` for modules
/// without a network surface.
fn load_process_network_module_from_loader_state(
    data_dir_root: &std::path::Path,
    state: &crate::NativeDylibModuleLoaderState,
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
        crate::native_dylib_module_registration_from_loader_state(data_dir_root, state)
            .map_err(|error| network_error(error.to_string()))?;
    if registration.surfaces.network_transports.is_empty() {
        return Ok(None);
    }
    let library_path = crate::native_module_cache_path(
        data_dir_root,
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    // SAFETY: trusted native-dylib module cache, blake3-verified against the
    // loader state's content hash before any code from it runs.
    let library = unsafe {
        crate::NativeLibrary::load_from_content_addressed_path(&library_path, &state.content_hash)
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
    let mut vtable = sinomo_core::network::network_module_abi::SinomoNetworkModuleVtableV1::empty();
    // SAFETY: calling the module's exported entry with valid out pointers.
    let status = unsafe { entry(&mut instance, &mut vtable) };
    if status != sinomo_core::network::network_module_abi::SINOMO_NETWORK_MODULE_STATUS_OK {
        return Err(network_error(format!(
            "network module entry returned status {status}"
        )));
    }
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
                "sinomo network module {} start failed: {error}",
                descriptor.id
            );
        } else {
            started = true;
            tracing::info!(
                "sinomo network module {} started (transports {:?})",
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

    fn loader_state(platform_triple: impl Into<String>) -> NativeDylibModuleLoaderState {
        NativeDylibModuleLoaderState {
            module_id: crate::ModuleId::from_legacy_named("module.test.network")
                .expect("module id"),
            artifact_id: None,
            content_hash: "blake3:test".to_string(),
            replaces_content_hash: None,
            cache_key: "module/test/network".to_string(),
            library_name: "libsinomo_test_network.so".to_string(),
            platform_triple: platform_triple.into(),
            native_abi_version: NATIVE_DYLIB_HOST_ABI_VERSION,
            interface_version: "sinomo.test.network.v1".to_string(),
            activated_at: "2026-08-20T00:00:00Z".to_string(),
            source: "test:network".to_string(),
        }
    }

    #[test]
    fn route_descriptor_conversion_preserves_module_policy() {
        let descriptor = crate::ModuleObjectRpcRoute {
            route: "/module/test/network/push".to_string(),
            category: "module.test.network".to_string(),
            operation: "network.push".to_string(),
            auth: crate::ModuleRouteAuth::Admin,
            request_body_mode: sinomo_module_api::NativeModuleBodyMode::Required,
            response_body_mode: sinomo_module_api::NativeModuleBodyMode::Optional,
            required_local_permissions: vec!["network.manage".to_string()],
            exact_member_identity: true,
            concurrency_limit: Some(3),
            timeout_ms: Some(60_000),
            request_schema: None,
        }
        .descriptor()
        .expect("valid descriptor");

        let route = module_object_rpc_route_descriptor_to_service(&descriptor);

        assert_eq!(route.route, descriptor.route);
        assert_eq!(route.auth_scope.as_deref(), Some("admin"));
        assert_eq!(route.required_capabilities, vec!["network.push"]);
        assert_eq!(route.required_local_permissions, vec!["network.manage"]);
        assert!(route.exact_member_identity);
        assert_eq!(route.concurrency_limit, Some(3));
        assert_eq!(route.timeout_ms, Some(60_000));
        assert_eq!(
            route.request_body_mode,
            sinomo_core::service::ServiceBodyMode::Required
        );
        assert_eq!(
            route.response_body_mode,
            sinomo_core::service::ServiceBodyMode::Optional
        );
    }

    #[test]
    fn native_loader_compatibility_requires_platform_and_abi() {
        let mut state = loader_state("x86_64-linux-unix");
        assert!(native_dylib_loader_state_is_host_compatible_for(
            &state, "x86_64", "linux", "unix"
        ));

        state.native_abi_version = NATIVE_DYLIB_HOST_ABI_VERSION.saturating_add(1);
        assert!(!native_dylib_loader_state_is_host_compatible_for(
            &state, "x86_64", "linux", "unix"
        ));
        assert!(!native_dylib_loader_state_is_host_compatible_for(
            &loader_state("aarch64-linux-unix"),
            "x86_64",
            "linux",
            "unix"
        ));
    }
}
