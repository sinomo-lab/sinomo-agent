//! Converts `sinomo-network` owned route and skill specs into module surfaces.
//!
//! `sinomo-network` cannot depend on `sinomo-module` directly because
//! `sinomo-config` depends on network RPC types and `sinomo-module` depends on
//! configuration bindings. This adapter keeps the catalog data owned by
//! `sinomo-network` while providing the shared module-registration API.

pub fn network_module_id() -> sinomo_module::ModuleId {
    sinomo_module::ModuleId::from_legacy_named(sinomo_network::SINOMO_NETWORK_MODULE_LEGACY_ID)
        .expect("valid network module id")
}

pub fn network_module_surfaces() -> sinomo_module::ModuleSurfaces {
    let mut surfaces = sinomo_module::ModuleSurfaces::default()
        .with_storage_owner(sinomo_network::SINOMO_NETWORK_MODULE_OWNER);

    for tool in sinomo_network::NETWORK_OBJECT_RPC_TOOL_SURFACES {
        surfaces = surfaces.with_object_rpc_route(sinomo_module::ModuleObjectRpcRoute::new(
            tool.route,
            tool.category,
            tool.operation,
            module_route_auth(tool.auth),
        ));
    }

    for resource in sinomo_network::NETWORK_DECLARATIVE_RESOURCE_SURFACES {
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
        sinomo_network::SINOMO_NETWORK_MODULE_OWNER,
        sinomo_network::SINOMO_NETWORK_MODULE_OWNER,
        sinomo_network::NETWORK_PROTOCOL,
        sinomo_core::TrustRing::R1TrustedRuntime,
        sinomo_network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
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
        imports: sinomo_network::NETWORK_CAPABILITY_IMPORTS
            .iter()
            .map(|capability| network_capability(capability))
            .collect(),
        exports: sinomo_network::NETWORK_CAPABILITY_EXPORTS
            .iter()
            .map(|capability| network_capability(capability))
            .collect(),
    }
}

fn module_route_auth(
    auth: sinomo_network::NetworkModuleRouteAuth,
) -> sinomo_module::ModuleRouteAuth {
    match auth {
        sinomo_network::NetworkModuleRouteAuth::LocalCommand => {
            sinomo_module::ModuleRouteAuth::LocalCommand
        }
        sinomo_network::NetworkModuleRouteAuth::LocalOrClusterMember => {
            sinomo_module::ModuleRouteAuth::LocalOrClusterMember
        }
        sinomo_network::NetworkModuleRouteAuth::ClusterMember => {
            sinomo_module::ModuleRouteAuth::ClusterMember
        }
        sinomo_network::NetworkModuleRouteAuth::Admin => sinomo_module::ModuleRouteAuth::Admin,
    }
}

fn module_declarative_resource_kind(
    kind: sinomo_network::NetworkDeclarativeResourceKind,
) -> sinomo_module::ModuleDeclarativeResourceKind {
    match kind {
        sinomo_network::NetworkDeclarativeResourceKind::Skill => {
            sinomo_module::ModuleDeclarativeResourceKind::Skill
        }
        sinomo_network::NetworkDeclarativeResourceKind::Prompt => {
            sinomo_module::ModuleDeclarativeResourceKind::Prompt
        }
    }
}

fn network_capability(name: &str) -> sinomo_core::CapabilityName {
    sinomo_core::CapabilityName::new(name).expect("valid network module capability")
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
            sinomo_network::NETWORK_PROTOCOL
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
                    sinomo_network::SINOMO_NETWORK_NATIVE_REGISTRATION_KEY,
                    sinomo_network::SINOMO_NETWORK_MODULE_OWNER,
                )
            }
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            vec![sinomo_network::SINOMO_NETWORK_MODULE_OWNER.to_string()]
        );
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            sinomo_network::NETWORK_OBJECT_RPC_ROUTES.len()
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == sinomo_network::CLUSTER_HEALTH_ROUTE
                    && route.category == sinomo_network::CLUSTER_HEALTH_RPC_CATEGORY
                    && route.operation == sinomo_network::CLUSTER_HEALTH_READ_CAPABILITY
                    && route.auth == sinomo_module::ModuleRouteAuth::LocalOrClusterMember)
        );

        let descriptor = registration
            .surfaces
            .object_rpc_route_descriptor(sinomo_network::CLUSTER_HEALTH_ROUTE)
            .expect("network surfaces are valid")
            .expect("health route descriptor");
        assert_eq!(
            descriptor.auth_scope,
            sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope()
        );
        assert_eq!(
            descriptor.required_capabilities,
            vec![
                sinomo_core::CapabilityName::new(sinomo_network::CLUSTER_HEALTH_READ_CAPABILITY)
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
                    == sinomo_network::CLUSTER_HEALTH_READ_CAPABILITY)
        );

        let skill = registration
            .surfaces
            .declarative_resources
            .iter()
            .find(|resource| resource.id == sinomo_network::CLUSTER_NETWORK_INSPECTION_SKILL_ID)
            .expect("cluster network inspection skill");
        assert_eq!(
            skill.kind,
            sinomo_module::ModuleDeclarativeResourceKind::Skill
        );
        assert_eq!(
            skill.path,
            sinomo_network::CLUSTER_NETWORK_INSPECTION_SKILL_PATH
        );
        assert!(skill.model_families.iter().any(|family| family == "gpt"));
        assert!(skill.activation_tags.iter().any(|tag| tag == "cluster"));
        assert!(skill.required_capabilities.iter().any(
            |capability| capability.as_str() == sinomo_network::CLUSTER_HEALTH_READ_CAPABILITY
        ));

        let prompt = registration
            .surfaces
            .declarative_resources
            .iter()
            .find(|resource| resource.id == sinomo_network::CLUSTER_NETWORK_INSPECTION_PROMPT_ID)
            .expect("cluster network inspection prompt");
        assert_eq!(
            prompt.kind,
            sinomo_module::ModuleDeclarativeResourceKind::Prompt
        );
        assert_eq!(
            prompt.path,
            sinomo_network::CLUSTER_NETWORK_INSPECTION_PROMPT_PATH
        );
    }
}
