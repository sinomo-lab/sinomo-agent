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
    // Catalog-owned lifecycle contributions use the same canonical component
    // identity as factories projected from module registrations. Legacy names
    // remain a CLI/config compatibility surface; using them as catalog keys
    // prevents factory-presence checks and hot-unload cleanup from matching the
    // owning component.
    let build_component_id = sinomo_runtime::build::build_module_id().to_string();
    let dfs_component_id = sinomo_runtime::dfs::dfs_module_id().to_string();
    let observe_component_id = sinomo_runtime::observe::observe_module_id().to_string();
    let rollout_component_id = sinomo_runtime::rollout::rollout_module_id().to_string();
    let sandbox_component_id = sinomo_runtime::sandbox::sandbox_module_id().to_string();
    let mut builder = ComponentCatalog::builder()
        .register_native_request_preparer(Arc::new(RemoteManagementRequestPreparer))?
        .register_storage_contributor(
            build_component_id,
            sinomo_runtime::build::register_storage_paths,
        )?
        .register_storage_contributor(
            dfs_component_id.clone(),
            sinomo_runtime::dfs::register_storage_paths,
        )?
        .register_storage_contributor(
            observe_component_id,
            sinomo_runtime::observe::register_storage_paths,
        )?
        .register_storage_contributor(
            rollout_component_id.clone(),
            sinomo_runtime::rollout::register_storage_paths,
        )?
        .register_storage_contributor(
            sandbox_component_id,
            sinomo_runtime::sandbox::register_storage_paths,
        )?
        .register_storage_contributor(
            "sinomo-registry",
            sinomo_runtime::registry::register_storage_paths,
        )?
        .register_status_contributor(
            rollout_component_id.clone(),
            sinomo_runtime::rollout_host::daemon_status_projection,
        )?
        .register_startup_contributor(
            rollout_component_id.clone(),
            sinomo_runtime::rollout_host::recover_startup_state,
        )?
        .register_maintenance_inhibitor(
            rollout_component_id,
            sinomo_runtime::rollout_host::maintenance_inhibited,
        )?
        .register_migration_contributor(dfs_component_id, dfs_migration_contribution)?;
    builder =
        builder.register_module(sinomo_runtime::network_module::network_module_registration())?;
    builder = register_module_factory(
        builder,
        sinomo_runtime::rollout::rollout_module_registration(),
        sinomo_runtime::rollout_host::component_factory(data_dir, profile.host_role)
            .expect("rollout host component descriptor must be valid"),
    )?;
    if profile.cluster_initialized
        && matches!(
            profile.host_role,
            ComponentHostRole::Main | ComponentHostRole::Embedded
        )
    {
        builder = builder.register_factory(Arc::new(
            sinomo_runtime::module::cluster_service_catalog_publisher_component_factory()
                .expect("service catalog publisher descriptor must be valid"),
        ))?;
    }

    if matches!(
        profile.host_role,
        ComponentHostRole::Main | ComponentHostRole::Embedded
    ) {
        builder = register_module_factory(
            builder,
            sinomo_runtime::sandbox::sandbox_module_registration(),
            sinomo_runtime::sandbox::component_factory()
                .expect("Sandbox product component descriptor must be valid"),
        )?;
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

    builder = register_installed_native_dylib_modules(builder, &data_dir.root())?;

    builder.build()
}

fn dfs_migration_contribution(
    data_dir_root: &std::path::Path,
    source_cluster_id: &sinomo_runtime::node::ClusterId,
) -> Result<sinomo_runtime::module::ComponentMigrationContribution, String> {
    let data_dir = SinomoDataDir::new(data_dir_root);
    let plan = sinomo_runtime::dfs::dfs_migration_quarantine_plan(&data_dir, source_cluster_id);
    Ok(sinomo_runtime::module::ComponentMigrationContribution {
        moves: plan.moves,
        evidence_refs: vec![format!("dfs-import-namespace:{}", plan.import_namespace)],
    })
}

/// Materialize installed native modules into the product catalog. Persisted
/// loader state is product composition input, so the daemon must not rediscover
/// or append these registrations behind the caller's catalog.
fn register_installed_native_dylib_modules(
    mut builder: ComponentCatalogBuilder,
    data_dir_root: &std::path::Path,
) -> Result<ComponentCatalogBuilder, ComponentCatalogError> {
    let states = match sinomo_runtime::module::read_native_dylib_module_loader_states(data_dir_root)
    {
        Ok(states) => states,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "native-dylib module loader states are unreadable; installed modules are omitted"
            );
            return Ok(builder);
        }
    };
    for state in states {
        if !sinomo_runtime::module::native_dylib_loader_state_is_host_compatible(&state) {
            tracing::warn!(
                module_id = %state.module_id,
                "installed native-dylib module is incompatible with this host and was omitted"
            );
            continue;
        }
        let registration =
            match sinomo_runtime::module::native_dylib_module_registration_from_loader_state(
                data_dir_root,
                &state,
            ) {
                Ok(registration) => registration,
                Err(error) => {
                    tracing::warn!(
                        module_id = %state.module_id,
                        error = %error,
                        "installed native-dylib module could not be materialized"
                    );
                    continue;
                }
            };
        builder = builder.register_module(registration)?;
    }
    Ok(builder)
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
    fn main_profile_owns_dfs_and_sandbox_factories_while_shadow_owns_neither() {
        let data_dir = SinomoDataDir::new(std::env::temp_dir().join(format!(
            "sinomo-product-dfs-profile-{}",
            sinomo_runtime::core::Id::generate()
        )));
        let main = base_component_catalog(&data_dir, ProductComponentProfile::main(true))
            .expect("main profile");
        let shadow = base_component_catalog(&data_dir, ProductComponentProfile::shadow(true))
            .expect("shadow profile");
        let dfs_id = sinomo_runtime::dfs::dfs_module_id();
        let sandbox_id = sinomo_runtime::sandbox::sandbox_module_id();

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
        assert!(main.modules().get(&sandbox_id).is_some());
        assert!(
            main.factories()
                .iter()
                .any(|factory| factory.descriptor().component_id == sandbox_id.as_str())
        );
        assert!(shadow.modules().get(&sandbox_id).is_none());
        assert!(
            !shadow
                .factories()
                .iter()
                .any(|factory| factory.descriptor().component_id == sandbox_id.as_str())
        );
        assert_eq!(main.status_contributors().len(), 1);
        assert_eq!(main.maintenance_inhibitors().len(), 1);
        assert_eq!(main.migration_contributors().len(), 1);
    }

    #[test]
    fn main_profile_storage_layout_registers_each_component_path_once() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-product-storage-profile-{}",
            sinomo_runtime::core::Id::generate()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let catalog = base_component_catalog(&data_dir, ProductComponentProfile::main(true))
            .expect("main profile");

        let layout = sinomo_runtime::storage::sinomo_data_dir_layout_with_catalog(&catalog)
            .expect("compose product storage layout");

        assert!(layout.owner_for("build/jobs/example/status.sobj").is_some());
        assert!(layout.owner_for("dfs/metadata").is_some());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn observe_enablement_is_resolved_by_the_product_profile() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-product-observe-profile-{}",
            sinomo_runtime::core::Id::generate()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let observe_id = sinomo_runtime::observe::observe_module_id();

        let enabled = base_component_catalog(&data_dir, ProductComponentProfile::main(false))
            .expect("enabled profile");
        assert!(enabled.modules().get(&observe_id).is_some());
        assert!(
            enabled
                .factories()
                .iter()
                .any(|factory| { factory.descriptor().component_id == observe_id.as_str() })
        );

        sinomo_runtime::observe::set_observe_module_enabled(
            &root,
            false,
            "test",
            "disable observe",
        )
        .expect("disable observe");
        let disabled = base_component_catalog(&data_dir, ProductComponentProfile::main(false))
            .expect("disabled profile");
        assert!(disabled.modules().get(&observe_id).is_none());
        assert!(
            !disabled
                .factories()
                .iter()
                .any(|factory| { factory.descriptor().component_id == observe_id.as_str() })
        );
        let _ = std::fs::remove_dir_all(root);
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
        let clustered_main = base_component_catalog(&data_dir, ProductComponentProfile::main(true))
            .expect("clustered main profile");
        let clustered_shadow =
            base_component_catalog(&data_dir, ProductComponentProfile::shadow(true))
                .expect("clustered shadow profile");
        assert!(
            clustered_main
                .factory(sinomo_runtime::module::SERVICE_CATALOG_PUBLISHER_COMPONENT_ID)
                .is_some()
        );
        assert!(
            clustered_shadow
                .factory(sinomo_runtime::module::SERVICE_CATALOG_PUBLISHER_COMPONENT_ID)
                .is_none()
        );
        assert!(
            main.factory(sinomo_runtime::module::SERVICE_CATALOG_PUBLISHER_COMPONENT_ID)
                .is_none()
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
