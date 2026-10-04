use crate::{
    SANDBOX_CHECKPOINT_ROUTE, SANDBOX_CREATE_ROUTE, SANDBOX_DESTROY_ROUTE,
    SANDBOX_HTTP_FETCH_ROUTE, SANDBOX_HTTP_FETCH_TO_FILE_ROUTE, SANDBOX_IMPORT_BUSYBOX_ROUTE,
    SANDBOX_LIST_ROUTE, SANDBOX_READ_ROUTE, SANDBOX_REBOOT_ROUTE, SANDBOX_RESUME_ROUTE,
    SANDBOX_RPC_CATEGORY, SANDBOX_RUN_CANCEL_ROUTE, SANDBOX_RUN_PROCESS_PLAN_ROUTE,
    SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE, SANDBOX_RUN_TOOLCHAIN_ROUTE,
    SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE, SANDBOX_STATUS_ROUTE, SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
    SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE, SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
    SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE, SANDBOX_UPDATE_ROUTE, SANDBOX_WRITE_ROUTE,
};
use sinomo_core::{
    CapabilityName, FileSystemAccess, ModuleCapabilities, ModuleResourceLimits, NetworkAccess,
    TrustRing,
};
use sinomo_module::{
    ModuleId, ModuleObjectRpcRoute, ModuleRegistration, ModuleRouteAuth, ModuleSurfaces,
};

pub const SINOMO_SANDBOX_MODULE_LEGACY_ID: &str = "module.sandbox";
pub const SINOMO_SANDBOX_MODULE_OWNER: &str = "sinomo-sandbox";
pub const SINOMO_SANDBOX_NATIVE_REGISTRATION_KEY: &str = "sinomo-sandbox";
pub const SANDBOX_PROTOCOL: &str = "sinomo.sandbox.v1";

pub const SANDBOX_OBJECT_RPC_ROUTES: &[&str] = &[
    SANDBOX_CREATE_ROUTE,
    SANDBOX_STATUS_ROUTE,
    SANDBOX_UPDATE_ROUTE,
    SANDBOX_CHECKPOINT_ROUTE,
    SANDBOX_RESUME_ROUTE,
    SANDBOX_REBOOT_ROUTE,
    SANDBOX_DESTROY_ROUTE,
    SANDBOX_LIST_ROUTE,
    SANDBOX_READ_ROUTE,
    SANDBOX_WRITE_ROUTE,
    SANDBOX_HTTP_FETCH_ROUTE,
    SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
    SANDBOX_IMPORT_BUSYBOX_ROUTE,
    SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE,
    SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
    SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE,
    SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
    SANDBOX_RUN_TOOLCHAIN_ROUTE,
    SANDBOX_RUN_PROCESS_PLAN_ROUTE,
    SANDBOX_RUN_CANCEL_ROUTE,
    SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
    SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
];

pub fn sandbox_module_id() -> ModuleId {
    ModuleId::from_legacy_named(SINOMO_SANDBOX_MODULE_LEGACY_ID).expect("valid sandbox module id")
}

pub fn sandbox_module_capabilities() -> ModuleCapabilities {
    ModuleCapabilities {
        imports: vec![
            sandbox_capability("storage.local.read"),
            sandbox_capability("storage.local.write"),
            sandbox_capability("cluster.kv.read"),
            sandbox_capability("network.object_rpc"),
            sandbox_capability("network.http"),
            sandbox_capability("dfs.filesystem"),
        ],
        exports: SANDBOX_OBJECT_RPC_ROUTES
            .iter()
            .map(|route| sandbox_capability(&sandbox_operation_name(route)))
            .collect(),
    }
}

pub fn sandbox_module_surfaces() -> ModuleSurfaces {
    let mut surfaces = ModuleSurfaces::default().with_storage_owner(SINOMO_SANDBOX_MODULE_OWNER);
    for route in SANDBOX_OBJECT_RPC_ROUTES {
        let mut descriptor = ModuleObjectRpcRoute::new(
            *route,
            SANDBOX_RPC_CATEGORY,
            sandbox_operation_name(route),
            sandbox_route_auth(route),
        );
        if matches!(
            *route,
            SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE | SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE
        ) {
            descriptor = descriptor
                .with_response_body_mode(sinomo_module_api::NativeModuleBodyMode::Required);
        }
        surfaces = surfaces.with_object_rpc_route(descriptor);
    }
    surfaces = surfaces
        .with_config_definition(
            crate::sandbox_policy_config_definition()
                .expect("sandbox policy config definition must be valid")
                .with_module_id(sandbox_module_id().to_string())
                .expect("sandbox module id must be valid for its config definition"),
        )
        .expect("sandbox config surface must be valid");
    surfaces.capabilities = sandbox_module_capabilities();
    surfaces
}

pub fn sandbox_module_registration() -> ModuleRegistration {
    let mut registration = ModuleRegistration::native_rust(
        sandbox_module_id(),
        env!("CARGO_PKG_VERSION"),
        SINOMO_SANDBOX_MODULE_OWNER,
        env!("CARGO_PKG_NAME"),
        SANDBOX_PROTOCOL,
        TrustRing::R1TrustedRuntime,
        SINOMO_SANDBOX_NATIVE_REGISTRATION_KEY,
    )
    .with_display_name("Sandbox");
    registration.manifest.capabilities = sandbox_module_capabilities();
    registration.manifest.limits = ModuleResourceLimits {
        cpu_ms: 4 * 60 * 60 * 1_000,
        memory_mb: 32 * 1_024,
        wall_time_ms: 4 * 60 * 60 * 1_000,
        network: NetworkAccess::Allowlisted,
        filesystem: FileSystemAccess::Scoped,
    };
    registration.surfaces = sandbox_module_surfaces();
    registration
}

/// Lifecycle management is one interface for every node: a cluster member
/// may create, inspect, update, or destroy a sandbox hosted here, the same way
/// the local node does. Execution, file, toolchain, and network routes remain
/// local commands.
pub fn sandbox_route_auth(route: &str) -> ModuleRouteAuth {
    match route {
        SANDBOX_CREATE_ROUTE
        | SANDBOX_STATUS_ROUTE
        | SANDBOX_UPDATE_ROUTE
        | SANDBOX_DESTROY_ROUTE => ModuleRouteAuth::LocalOrClusterMember,
        _ => ModuleRouteAuth::LocalCommand,
    }
}

pub fn sandbox_operation_name(route: &str) -> String {
    let suffix = route.strip_prefix("/sandbox/").unwrap_or(route);
    format!("sandbox.{}", suffix.replace('/', "."))
}

fn sandbox_capability(name: &str) -> CapabilityName {
    CapabilityName::new(name).expect("valid sandbox module capability")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_module_registration_owns_every_service_route() {
        let registration = sandbox_module_registration();
        let routes = registration
            .surfaces
            .object_rpc_routes
            .iter()
            .map(|route| route.route.as_str())
            .collect::<Vec<_>>();
        assert_eq!(routes, SANDBOX_OBJECT_RPC_ROUTES);
        assert_eq!(
            registration.manifest.target_ring,
            TrustRing::R1TrustedRuntime
        );
    }
}
