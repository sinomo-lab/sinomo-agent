use crate::{
    BUILD_ARTIFACT_ROUTE, BUILD_CAPABILITIES_ROUTE, BUILD_PROTOCOL, BUILD_RPC_CATEGORY,
    BUILD_STATUS_ROUTE, BUILD_SUBMIT_ROUTE,
};
use sinomo_core::{
    CapabilityName, FileSystemAccess, ModuleCapabilities, ModuleResourceLimits, NetworkAccess,
    TrustRing,
};
use sinomo_module::{
    ModuleId, ModuleObjectRpcRoute, ModuleRegistration, ModuleRouteAuth, ModuleSurfaces,
};

pub const SINOMO_BUILD_MODULE_LEGACY_ID: &str = "module.build";
pub const SINOMO_BUILD_MODULE_OWNER: &str = "sinomo-build";
pub const SINOMO_BUILD_NATIVE_REGISTRATION_KEY: &str = "sinomo-build";
pub const BUILD_JOB_QUEUED_EVENT_KIND: &str = "build.job.queued";
pub const BUILD_JOB_STARTED_EVENT_KIND: &str = "build.job.started";
pub const BUILD_JOB_SUCCEEDED_EVENT_KIND: &str = "build.job.succeeded";
pub const BUILD_JOB_FAILED_EVENT_KIND: &str = "build.job.failed";

pub fn build_module_id() -> ModuleId {
    ModuleId::from_legacy_named(SINOMO_BUILD_MODULE_LEGACY_ID).expect("valid build module id")
}

pub fn build_module_capabilities() -> ModuleCapabilities {
    ModuleCapabilities {
        imports: vec![
            build_capability("storage.local.read"),
            build_capability("storage.local.write"),
            build_capability("network.object_rpc"),
            build_capability("process.spawn"),
        ],
        exports: vec![
            build_capability("build.capabilities.read"),
            build_capability("build.job.submit"),
            build_capability("build.job.status.read"),
            build_capability("build.job.log.read"),
            build_capability("build.artifact.read"),
            build_capability("build.job.record"),
        ],
    }
}

pub fn build_module_surfaces() -> ModuleSurfaces {
    let mut surfaces = ModuleSurfaces::default()
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            BUILD_CAPABILITIES_ROUTE,
            BUILD_RPC_CATEGORY,
            "build.capabilities.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            BUILD_SUBMIT_ROUTE,
            BUILD_RPC_CATEGORY,
            "build.job.submit",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            BUILD_STATUS_ROUTE,
            BUILD_RPC_CATEGORY,
            "build.job.status.read",
            ModuleRouteAuth::ClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            BUILD_ARTIFACT_ROUTE,
            BUILD_RPC_CATEGORY,
            "build.artifact.read",
            ModuleRouteAuth::ClusterMember,
        ))
        .with_storage_owner(SINOMO_BUILD_MODULE_OWNER)
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            crate::BUILD_LOG_ROUTE,
            BUILD_RPC_CATEGORY,
            "build.job.log.read",
            ModuleRouteAuth::Admin,
        ))
        .with_event_kind_name(BUILD_JOB_QUEUED_EVENT_KIND)
        .with_event_kind_name(BUILD_JOB_STARTED_EVENT_KIND)
        .with_event_kind_name(BUILD_JOB_SUCCEEDED_EVENT_KIND)
        .with_event_kind_name(BUILD_JOB_FAILED_EVENT_KIND);
    surfaces.capabilities = build_module_capabilities();
    surfaces
}

pub fn build_module_registration() -> ModuleRegistration {
    let mut registration = ModuleRegistration::native_rust(
        build_module_id(),
        env!("CARGO_PKG_VERSION"),
        SINOMO_BUILD_MODULE_OWNER,
        env!("CARGO_PKG_NAME"),
        BUILD_PROTOCOL,
        TrustRing::R1TrustedRuntime,
        SINOMO_BUILD_NATIVE_REGISTRATION_KEY,
    );
    registration.manifest.capabilities = build_module_capabilities();
    registration.manifest.limits = ModuleResourceLimits {
        cpu_ms: 4 * 60 * 60 * 1_000,
        memory_mb: 32 * 1_024,
        wall_time_ms: 4 * 60 * 60 * 1_000,
        network: NetworkAccess::Allowlisted,
        filesystem: FileSystemAccess::Scoped,
    };
    registration.surfaces = build_module_surfaces();
    registration
}

fn build_capability(name: &str) -> CapabilityName {
    CapabilityName::new(name).expect("valid build module capability")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BUILD_OBJECT_RPC_ROUTES;
    use sinomo_module::ModuleRegistry;

    #[test]
    fn build_module_registration_is_surface_valid() {
        let registration = build_module_registration();
        let routes = registration
            .surfaces
            .object_rpc_routes
            .iter()
            .map(|route| route.route.as_str())
            .collect::<Vec<_>>();

        assert_eq!(routes, BUILD_OBJECT_RPC_ROUTES);
        assert_eq!(
            registration.manifest.target_ring,
            TrustRing::R1TrustedRuntime
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            [SINOMO_BUILD_MODULE_OWNER]
        );
        ModuleRegistry::builder()
            .register(registration)
            .expect("registration")
            .try_build()
            .expect("registry");
    }

    #[test]
    fn submit_and_logs_require_admin_while_cluster_members_can_fetch_results() {
        let surfaces = build_module_surfaces();
        let auth = |route: &str| {
            surfaces
                .object_rpc_routes
                .iter()
                .find(|candidate| candidate.route == route)
                .map(|candidate| candidate.auth)
        };
        assert_eq!(auth(BUILD_SUBMIT_ROUTE), Some(ModuleRouteAuth::Admin));
        assert_eq!(auth(crate::BUILD_LOG_ROUTE), Some(ModuleRouteAuth::Admin));
        assert_eq!(
            auth(BUILD_ARTIFACT_ROUTE),
            Some(ModuleRouteAuth::ClusterMember)
        );
    }
}
