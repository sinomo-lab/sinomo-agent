//! Daemon-owned registries and foundation storage contracts.
//!
//! Product modules and their storage declarations arrive through an explicit
//! [`sinomo_module::ComponentCatalog`]; this crate never assembles a hidden
//! built-in module list.

pub use sinomo_core::storage::*;

pub fn sinomo_data_dir_layout() -> Result<StorageLayout, StorageContractError> {
    sinomo_data_dir_layout_with_catalog(&sinomo_module::ComponentCatalog::default())
}

pub fn sinomo_data_dir_layout_with_catalog(
    catalog: &sinomo_module::ComponentCatalog,
) -> Result<StorageLayout, StorageContractError> {
    let mut layout = StorageLayout::new();
    sinomo_core::network::register_storage_paths(&mut layout)?;
    sinomo_core::cluster::register_storage_paths(&mut layout)?;
    sinomo_core::kv::register_storage_paths(&mut layout)?;
    crate::register_storage_paths(&mut layout)?;
    sinomo_config::register_storage_paths(&mut layout)?;
    sinomo_module::register_storage_paths(&mut layout)?;
    sinomo_core::node::register_storage_paths(&mut layout)?;
    sinomo_core::local::register_storage_paths(&mut layout)?;
    sinomo_core::events::register_storage_paths(&mut layout)?;
    catalog.register_storage_paths(&mut layout)?;
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

pub fn verify_sinomo_data_dir_with_catalog(
    data_dir: impl AsRef<std::path::Path>,
    catalog: &sinomo_module::ComponentCatalog,
) -> Result<StorageVerificationReport, StorageContractError> {
    sinomo_data_dir_layout_with_catalog(catalog)?.verify_data_dir(data_dir)
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
    module_object_rpc_route_descriptor_to_service,
    module_registration_object_rpc_route_descriptors, module_registry_object_rpc_route_descriptors,
    module_registry_snapshot_object_rpc_route_descriptors,
};

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
    fn daemon_mount_lease_is_registered() {
        let layout = sinomo_data_dir_layout().expect("data-dir layout");
        let report = layout
            .verify_relative_paths(["daemon/sifs-mount.lock"])
            .expect("verify mount lease path");

        assert!(report.is_complete(), "{:#?}", report.unregistered);
        assert_eq!(report.covered.len(), 1);
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
