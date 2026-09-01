//! Shared product-level component selection.
//!
//! The daemon hosts a catalog but does not define the product. Standalone CLI
//! and embedded app hosts start from this same base profile and add only their
//! process-specific components.

use std::sync::Arc;

use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::module::{
    ComponentCatalog, ComponentCatalogBuilder, ComponentCatalogError, ComponentFactory,
    ComponentHostRole,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProductComponentProfile {
    pub host_role: ComponentHostRole,
    pub cluster_initialized: bool,
    pub resource_class: ProductResourceClass,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProductResourceClass {
    #[default]
    Standard,
    Mobile,
}

impl ProductComponentProfile {
    pub fn main(cluster_initialized: bool) -> Self {
        Self {
            host_role: ComponentHostRole::Main,
            cluster_initialized,
            resource_class: ProductResourceClass::Standard,
        }
    }

    pub fn shadow(cluster_initialized: bool) -> Self {
        Self {
            host_role: ComponentHostRole::Shadow,
            cluster_initialized,
            resource_class: ProductResourceClass::Standard,
        }
    }

    pub fn embedded(cluster_initialized: bool) -> Self {
        Self {
            host_role: ComponentHostRole::Embedded,
            cluster_initialized,
            resource_class: ProductResourceClass::Standard,
        }
    }

    pub fn mobile_embedded(cluster_initialized: bool) -> Self {
        Self {
            host_role: ComponentHostRole::Embedded,
            cluster_initialized,
            resource_class: ProductResourceClass::Mobile,
        }
    }

    fn hosts_native_build_worker(self) -> bool {
        self.resource_class == ProductResourceClass::Standard
            && matches!(
                self.host_role,
                ComponentHostRole::Main | ComponentHostRole::Embedded
            )
    }

    pub fn hosts_native_filesystem_projection(self) -> bool {
        self.resource_class == ProductResourceClass::Standard
            && matches!(
                self.host_role,
                ComponentHostRole::Main | ComponentHostRole::Embedded
            )
    }
}

pub fn base_component_catalog(
    data_dir: &SinomoDataDir,
    profile: ProductComponentProfile,
) -> Result<ComponentCatalog, ComponentCatalogError> {
    let mut builder = ComponentCatalog::builder()
        .register_native_request_preparer(Arc::new(RemoteManagementRequestPreparer))?;
    builder =
        builder.register_module(sinomo_runtime::network_module::network_module_registration())?;
    builder = builder.register_module(sinomo_runtime::rollout::rollout_module_registration())?;

    if matches!(
        profile.host_role,
        ComponentHostRole::Main | ComponentHostRole::Embedded
    ) {
        let dfs_registration = sinomo_runtime::dfs::dfs_module_registration();
        let dfs_active = sinomo_runtime::dfs::dfs_module_enabled(data_dir.root()).unwrap_or(true);
        if dfs_active {
            builder = register_module_factory(
                builder,
                dfs_registration,
                sinomo_runtime::dfs::component_factory()
                    .expect("DFS product component descriptor must be valid"),
            )?;
        } else {
            builder = builder.register_module(dfs_registration)?;
        }
        builder = builder.register_module(sinomo_runtime::dfs_nfs::nfs_module_registration())?;
        builder = builder.register_native_rust_control_target(
            sinomo_runtime::module::NativeRustModuleServiceTarget::new(
                sinomo_runtime::dfs::SINOMO_DFS_MODULE_LEGACY_ID,
                dfs_active,
                true,
                "distributed filesystem provider module",
            ),
        )?;
        builder = builder.register_native_rust_control_target(
            sinomo_runtime::module::NativeRustModuleServiceTarget::new(
                sinomo_runtime::dfs_nfs::SINOMO_DFS_NFS_MODULE_LEGACY_ID,
                profile.hosts_native_filesystem_projection()
                    && profile.cluster_initialized
                    && sinomo_runtime::dfs_nfs::nfs_module_enabled(data_dir).unwrap_or(false),
                true,
                "direct SIFS NFS mount module",
            ),
        )?;
    }

    let observe_active =
        sinomo_runtime::observe::observe_module_enabled(data_dir.root()).unwrap_or(true);
    if observe_active {
        builder = register_module_factory(
            builder,
            sinomo_runtime::observe::observe_module_registration(),
            sinomo_runtime::observe::component_factory()
                .expect("observe product component descriptor must be valid"),
        )?;
    }
    builder = builder.register_native_rust_control_target(
        sinomo_runtime::module::NativeRustModuleServiceTarget::new(
            sinomo_runtime::observe::SINOMO_OBSERVE_MODULE_LEGACY_ID,
            observe_active,
            true,
            "operational observation module",
        ),
    )?;

    if profile.hosts_native_build_worker() {
        #[cfg(feature = "native-workers")]
        {
            builder = register_module_factory(
                builder,
                sinomo_runtime::build::build_module_registration(),
                sinomo_runtime::build::component_factory()
                    .expect("build product component descriptor must be valid"),
            )?;
        }
    }
    #[cfg(feature = "native-workers")]
    {
        if profile.hosts_native_filesystem_projection()
            && profile.cluster_initialized
            && sinomo_runtime::dfs_nfs::nfs_module_enabled(data_dir).unwrap_or(false)
        {
            builder = builder.register_factory(Arc::new(
                sinomo_runtime::dfs_nfs::component_factory()
                    .expect("NFS product component descriptor must be valid"),
            ))?;
        }
    }

    builder.build()
}

#[derive(Clone, Debug)]
pub struct RemoteManagementRequestPreparer;

impl sinomo_runtime::module::NativeModuleRequestPreparer for RemoteManagementRequestPreparer {
    fn id(&self) -> &'static str {
        "sinomo.remote-management.authority"
    }

    fn prepare(
        &self,
        data_dir_root: &std::path::Path,
        target: &sinomo_runtime::module::NativeDylibModuleRouteTarget,
        request: &mut sinomo_runtime::network::ObjectRpcRequest,
    ) -> Result<(), sinomo_runtime::network::NetworkError> {
        if target.operation
            != sinomo_runtime::remote_management::REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION
        {
            return Ok(());
        }
        let mut observation: sinomo_runtime::remote_management::RemoteManagementSystemObserveRequest =
            request.decode_body()?;
        observation.host_authorized_resource_exposures.clear();
        if observation.include_resource_exposures {
            let store = sinomo_runtime::node::LocalNodePermissionStore::new(data_dir_root);
            if let Ok(permissions) = store.load() {
                observation.host_authorized_resource_exposures = permissions
                    .resource_exposures()
                    .iter()
                    .take(sinomo_runtime::remote_management::MAX_OBSERVED_RESOURCE_EXPOSURES)
                    .map(|(resource_id, exposure)| {
                        sinomo_runtime::remote_management::RemoteManagementResourceExposurePolicy {
                            resource_id: resource_id.clone(),
                            allowed_operations: exposure.operations().iter().cloned().collect(),
                        }
                    })
                    .collect();
            }
        }
        request.body = sinomo_runtime::module_api::encode_sobj(&observation).map_err(|error| {
            sinomo_runtime::network::NetworkError::Serialization {
                detail: error.to_string(),
            }
        })?;
        Ok(())
    }
}

pub fn with_component(
    catalog: &ComponentCatalog,
    factory: Arc<dyn ComponentFactory>,
) -> Result<ComponentCatalog, ComponentCatalogError> {
    catalog.to_builder().register_factory(factory)?.build()
}

pub fn with_components(
    catalog: &ComponentCatalog,
    factories: impl IntoIterator<Item = Arc<dyn ComponentFactory>>,
) -> Result<ComponentCatalog, ComponentCatalogError> {
    let mut builder = catalog.to_builder();
    for factory in factories {
        builder = builder.register_factory(factory)?;
    }
    builder.build()
}

fn register_module_factory(
    builder: ComponentCatalogBuilder,
    registration: sinomo_runtime::module::ModuleRegistration,
    factory: impl ComponentFactory + 'static,
) -> Result<ComponentCatalogBuilder, ComponentCatalogError> {
    builder.register_module_factory(registration, Arc::new(factory))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main_profile_owns_dfs_factory_and_shadow_profile_does_not() {
        let data_dir = SinomoDataDir::new(std::env::temp_dir().join(format!(
            "sinomo-product-dfs-profile-{}",
            sinomo_runtime::core::Id::generate()
        )));
        let main = base_component_catalog(&data_dir, ProductComponentProfile::main(true))
            .expect("main profile");
        let shadow = base_component_catalog(&data_dir, ProductComponentProfile::shadow(true))
            .expect("shadow profile");
        let dfs_id = sinomo_runtime::dfs::dfs_module_id();

        assert!(main.modules().get(&dfs_id).is_some());
        assert!(
            main.factories()
                .iter()
                .any(|factory| factory.descriptor().component_id == dfs_id.as_str())
        );
        assert!(shadow.modules().get(&dfs_id).is_none());
        assert!(
            !shadow
                .factories()
                .iter()
                .any(|factory| factory.descriptor().component_id == dfs_id.as_str())
        );
    }

    #[cfg(feature = "native-workers")]
    #[test]
    fn main_and_shadow_profiles_share_base_selection_but_not_main_workers() {
        let data_dir = SinomoDataDir::new(
            std::env::temp_dir().join("sinomo-product-component-profile-missing"),
        );
        let main = base_component_catalog(&data_dir, ProductComponentProfile::main(false))
            .expect("main profile");
        let shadow = base_component_catalog(&data_dir, ProductComponentProfile::shadow(false))
            .expect("shadow profile");

        assert!(
            main.factories()
                .iter()
                .any(|factory| factory.descriptor().component_id
                    == sinomo_runtime::build::build_module_id().as_str())
        );
        assert!(!shadow.factories().iter().any(|factory| {
            factory.descriptor().component_id == sinomo_runtime::build::build_module_id().as_str()
        }));
        assert!(
            main.modules()
                .get(&sinomo_runtime::rollout::rollout_module_id())
                .is_some()
        );
        assert!(
            shadow
                .modules()
                .get(&sinomo_runtime::rollout::rollout_module_id())
                .is_some()
        );
    }

    #[cfg(feature = "native-workers")]
    #[test]
    fn mobile_embedded_profile_omits_native_build_worker() {
        let data_dir =
            SinomoDataDir::new(std::env::temp_dir().join("sinomo-product-mobile-profile-missing"));
        let standard = base_component_catalog(&data_dir, ProductComponentProfile::embedded(false))
            .expect("standard embedded profile");
        let mobile =
            base_component_catalog(&data_dir, ProductComponentProfile::mobile_embedded(false))
                .expect("mobile embedded profile");
        let build_component_id = sinomo_runtime::build::build_module_id();

        assert!(standard.modules().get(&build_component_id).is_some());
        assert!(
            standard.factories().iter().any(|factory| {
                factory.descriptor().component_id == build_component_id.as_str()
            })
        );
        assert!(mobile.modules().get(&build_component_id).is_none());
        assert!(
            !mobile.factories().iter().any(|factory| {
                factory.descriptor().component_id == build_component_id.as_str()
            })
        );
        assert!(ProductComponentProfile::embedded(false).hosts_native_filesystem_projection());
        assert!(
            !ProductComponentProfile::mobile_embedded(false).hosts_native_filesystem_projection()
        );
    }

    #[cfg(not(feature = "native-workers"))]
    #[test]
    fn native_worker_free_build_omits_build_component_for_every_profile() {
        let data_dir = SinomoDataDir::new(
            std::env::temp_dir().join("sinomo-product-no-native-workers-missing"),
        );
        let catalog = base_component_catalog(&data_dir, ProductComponentProfile::main(false))
            .expect("worker-free main profile");
        let build_component_id = sinomo_runtime::build::build_module_id();

        assert!(catalog.modules().get(&build_component_id).is_none());
        assert!(
            !catalog.factories().iter().any(|factory| {
                factory.descriptor().component_id == build_component_id.as_str()
            })
        );
    }

    #[test]
    fn remote_management_preparer_overwrites_caller_policy_from_local_consent() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-product-profile-authority-{}",
            sinomo_runtime::core::Id::generate()
        ));
        let mut permissions = sinomo_runtime::node::LocalNodePermissions::new();
        permissions
            .expose_resource_operation("camera.front", "observe")
            .expect("expose camera");
        sinomo_runtime::node::LocalNodePermissionStore::new(&root)
            .save(&permissions)
            .expect("save permissions");
        let payload =
            sinomo_runtime::remote_management::RemoteManagementSystemObserveRequest::new()
                .including_resource_exposures(true)
                .with_host_authorized_resource_exposures([
                    sinomo_runtime::remote_management::RemoteManagementResourceExposurePolicy {
                        resource_id: "caller.spoofed".to_string(),
                        allowed_operations: vec!["everything".to_string()],
                    },
                ]);
        let mut request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            sinomo_runtime::remote_management::REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
            sinomo_runtime::remote_management::REMOTE_MANAGEMENT_RPC_CATEGORY,
            &payload,
        )
        .expect("request");
        let target = sinomo_runtime::module::NativeDylibModuleRouteTarget {
            module_id: sinomo_runtime::module::ModuleId::from_legacy_named("remote.management")
                .expect("module id"),
            operation:
                sinomo_runtime::remote_management::REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION
                    .to_string(),
            request_body_mode: sinomo_runtime::service::ServiceBodyMode::None,
            response_body_mode: sinomo_runtime::service::ServiceBodyMode::None,
        };
        let catalog = ComponentCatalog::builder()
            .register_native_request_preparer(Arc::new(RemoteManagementRequestPreparer))
            .expect("register preparer")
            .build()
            .expect("catalog");
        sinomo_runtime::module::NativeModuleRequestPreparer::prepare(
            &catalog,
            &root,
            &target,
            &mut request,
        )
        .expect("prepare request");
        let injected: sinomo_runtime::remote_management::RemoteManagementSystemObserveRequest =
            request.decode_body().expect("decode request");
        assert_eq!(
            injected.host_authorized_resource_exposures[0].resource_id,
            "camera.front"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
