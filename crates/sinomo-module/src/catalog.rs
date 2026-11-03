//! Unified component composition catalog.
//!
//! A host consumes one catalog for runtime factories, module metadata, config
//! definitions, and storage declarations. Product profiles build catalogs;
//! daemon code should not maintain parallel per-surface module lists.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use thiserror::Error;

use crate::{ComponentFactory, ModuleId, ModuleRegistration, ModuleRegistry, ModuleRegistryError};

#[derive(Clone, Default)]
pub struct ComponentCatalog {
    factories: BTreeMap<String, Arc<dyn ComponentFactory>>,
    factory_order: Vec<String>,
    modules: ModuleRegistry,
    storage_contributors: BTreeMap<String, ComponentStorageContributor>,
    status_contributors: BTreeMap<String, ComponentStatusContributor>,
    startup_contributors: BTreeMap<String, ComponentStartupContributor>,
    maintenance_inhibitors: BTreeMap<String, ComponentMaintenanceInhibitor>,
    migration_contributors: BTreeMap<String, ComponentMigrationContributor>,
    native_request_preparers: BTreeMap<String, Arc<dyn crate::NativeModuleRequestPreparer>>,
    native_rust_control_targets: BTreeMap<String, crate::NativeRustModuleServiceTarget>,
}

impl std::fmt::Debug for ComponentCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ComponentCatalog")
            .field("component_ids", &self.factory_order)
            .field(
                "module_ids",
                &self
                    .modules
                    .registrations()
                    .map(|registration| registration.module_id().as_str())
                    .collect::<Vec<_>>(),
            )
            .field(
                "storage_contributors",
                &self.storage_contributors.keys().collect::<Vec<_>>(),
            )
            .field(
                "status_contributors",
                &self.status_contributors.keys().collect::<Vec<_>>(),
            )
            .field(
                "startup_contributors",
                &self.startup_contributors.keys().collect::<Vec<_>>(),
            )
            .field(
                "maintenance_inhibitors",
                &self.maintenance_inhibitors.keys().collect::<Vec<_>>(),
            )
            .field(
                "migration_contributors",
                &self.migration_contributors.keys().collect::<Vec<_>>(),
            )
            .field(
                "native_request_preparers",
                &self.native_request_preparers.keys().collect::<Vec<_>>(),
            )
            .field(
                "native_rust_control_targets",
                &self.native_rust_control_targets.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ComponentCatalog {
    pub fn builder() -> ComponentCatalogBuilder {
        ComponentCatalogBuilder::default()
    }

    pub fn to_builder(&self) -> ComponentCatalogBuilder {
        ComponentCatalogBuilder {
            factories: self.factories.clone(),
            factory_order: self.factory_order.clone(),
            modules: self.modules.to_builder(),
            module_ids: self
                .modules
                .registrations()
                .map(|registration| registration.module_id().clone())
                .collect(),
            storage_contributors: self.storage_contributors.clone(),
            status_contributors: self.status_contributors.clone(),
            startup_contributors: self.startup_contributors.clone(),
            maintenance_inhibitors: self.maintenance_inhibitors.clone(),
            migration_contributors: self.migration_contributors.clone(),
            native_request_preparers: self.native_request_preparers.clone(),
            native_rust_control_targets: self.native_rust_control_targets.clone(),
        }
    }

    pub fn factories(&self) -> Vec<Arc<dyn ComponentFactory>> {
        self.factory_order
            .iter()
            .filter_map(|component_id| self.factories.get(component_id).cloned())
            .collect()
    }

    pub fn factory(&self, component_id: &str) -> Option<Arc<dyn ComponentFactory>> {
        self.factories.get(component_id).cloned()
    }

    pub fn modules(&self) -> &ModuleRegistry {
        &self.modules
    }

    /// Remove one module's runtime registration and matching component-owned
    /// effects while retaining storage declarations and generic host adapters
    /// needed for recovery or a later reactivation.
    pub fn without_module(&self, module_id: &ModuleId) -> Result<Self, ComponentCatalogError> {
        let registration = self.modules.get(module_id).ok_or_else(|| {
            ComponentCatalogError::ModuleRegistry(ModuleRegistryError::UnknownModule {
                module_id: module_id.clone(),
            })
        })?;
        let component_id = crate::component_descriptor_from_registration(registration)
            .map_err(|error| ComponentCatalogError::InvalidComponent {
                component_id: module_id.to_string(),
                detail: error.to_string(),
            })?
            .component_id;
        let mut catalog = self.clone();
        catalog.modules = self.modules.without(module_id)?;
        catalog.factories.remove(&component_id);
        catalog
            .factory_order
            .retain(|candidate| candidate != &component_id);
        catalog.status_contributors.remove(&component_id);
        catalog.startup_contributors.remove(&component_id);
        catalog.maintenance_inhibitors.remove(&component_id);
        catalog.migration_contributors.remove(&component_id);
        Ok(catalog)
    }

    pub fn has_native_request_preparer(&self, id: &str) -> bool {
        self.native_request_preparers.contains_key(id)
    }

    pub fn native_rust_control_targets(&self) -> Vec<crate::NativeRustModuleServiceTarget> {
        self.native_rust_control_targets.values().cloned().collect()
    }

    pub fn status_contributors(&self) -> Vec<ComponentStatusContributor> {
        self.status_contributors.values().copied().collect()
    }

    pub fn run_startup_contributors(&self, data_dir_root: &std::path::Path) -> Result<(), String> {
        for contributor in self.startup_contributors.values() {
            contributor(data_dir_root)?;
        }
        Ok(())
    }

    pub fn maintenance_inhibitors(&self) -> Vec<ComponentMaintenanceInhibitor> {
        self.maintenance_inhibitors.values().copied().collect()
    }

    pub fn migration_contributors(&self) -> Vec<ComponentMigrationContributor> {
        self.migration_contributors.values().copied().collect()
    }

    pub fn register_config_definitions(
        &self,
        mut builder: sinomo_config::ConfigRegistryBuilder,
    ) -> Result<sinomo_config::ConfigRegistryBuilder, ComponentCatalogError> {
        for factory in self.factories() {
            builder = factory
                .register_config_definitions(builder)
                .map_err(|detail| ComponentCatalogError::Config {
                    component_id: factory.descriptor().component_id.clone(),
                    detail,
                })?;
        }
        for registration in self.modules.registrations() {
            for definition in registration
                .surfaces
                .config_definitions()
                .map_err(|error| ComponentCatalogError::Config {
                    component_id: registration.module_id().to_string(),
                    detail: error.to_string(),
                })?
            {
                if let Ok(existing) = builder.get(definition.key.as_str()) {
                    if existing == &definition {
                        continue;
                    }
                    tracing::warn!(
                        component_id = %registration.module_id(),
                        key = %definition.key,
                        "module config contribution skipped because the host catalog already owns a different definition"
                    );
                    continue;
                }
                builder = builder.register(definition).map_err(|error| {
                    ComponentCatalogError::Config {
                        component_id: registration.module_id().to_string(),
                        detail: error.to_string(),
                    }
                })?;
            }
        }
        Ok(builder)
    }

    pub fn register_storage_paths(
        &self,
        layout: &mut sinomo_core::storage::StorageLayout,
    ) -> Result<(), sinomo_core::storage::StorageContractError> {
        for factory in self.factories() {
            factory.register_storage_paths(layout)?;
        }
        for (component_id, contributor) in &self.storage_contributors {
            // A contributor is the storage-only fallback for a component that
            // is disabled or not mounted in this product profile. When its
            // factory is present, that factory remains the single authority so
            // the same paths are never registered twice.
            if !self.factories.contains_key(component_id) {
                contributor(layout)?;
            }
        }
        Ok(())
    }
}

impl crate::NativeModuleRequestPreparer for ComponentCatalog {
    fn host_services(
        &self,
        data_dir_root: &std::path::Path,
        descriptor: &sinomo_module_api::NativeModuleDescriptor,
        cancellation: &crate::NativeModuleOperationCancellationToken,
    ) -> Result<
        BTreeMap<String, Arc<dyn crate::NativeModuleHostCallbacks>>,
        sinomo_core::network::NetworkError,
    > {
        let mut services = BTreeMap::new();
        for preparer in self.native_request_preparers.values() {
            for (interface, service) in
                preparer.host_services(data_dir_root, descriptor, cancellation)?
            {
                if services.insert(interface.clone(), service).is_some() {
                    return Err(sinomo_core::network::NetworkError::Serialization {
                        detail: format!("duplicate host service {interface}"),
                    });
                }
            }
        }
        Ok(services)
    }

    fn id(&self) -> &'static str {
        "component-catalog"
    }

    fn prepare(
        &self,
        data_dir_root: &std::path::Path,
        target: &crate::NativeDylibModuleRouteTarget,
        request: &mut sinomo_core::network::ObjectRpcRequest,
    ) -> Result<(), sinomo_core::network::NetworkError> {
        for preparer in self.native_request_preparers.values() {
            preparer.prepare(data_dir_root, target, request)?;
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct ComponentCatalogBuilder {
    factories: BTreeMap<String, Arc<dyn ComponentFactory>>,
    factory_order: Vec<String>,
    modules: crate::ModuleRegistryBuilder,
    module_ids: BTreeSet<ModuleId>,
    storage_contributors: BTreeMap<String, ComponentStorageContributor>,
    status_contributors: BTreeMap<String, ComponentStatusContributor>,
    startup_contributors: BTreeMap<String, ComponentStartupContributor>,
    maintenance_inhibitors: BTreeMap<String, ComponentMaintenanceInhibitor>,
    migration_contributors: BTreeMap<String, ComponentMigrationContributor>,
    native_request_preparers: BTreeMap<String, Arc<dyn crate::NativeModuleRequestPreparer>>,
    native_rust_control_targets: BTreeMap<String, crate::NativeRustModuleServiceTarget>,
}

pub type ComponentStorageContributor = fn(
    &mut sinomo_core::storage::StorageLayout,
)
    -> Result<(), sinomo_core::storage::StorageContractError>;

#[derive(Clone, Debug, PartialEq)]
pub struct ComponentStatusProjection {
    pub key: String,
    pub value: serde_json::Value,
}

impl ComponentStatusProjection {
    pub fn new(key: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

pub type ComponentStatusContributor =
    fn(&std::path::Path) -> Result<Option<ComponentStatusProjection>, String>;
pub type ComponentStartupContributor = fn(&std::path::Path) -> Result<(), String>;
pub type ComponentMaintenanceInhibitor = fn(&std::path::Path) -> bool;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ComponentMigrationContribution {
    pub moves: Vec<(std::path::PathBuf, std::path::PathBuf)>,
    pub evidence_refs: Vec<String>,
}

pub type ComponentMigrationContributor = fn(
    &std::path::Path,
    &sinomo_core::node::ClusterId,
) -> Result<ComponentMigrationContribution, String>;

impl ComponentCatalogBuilder {
    pub fn register_factory(
        mut self,
        factory: Arc<dyn ComponentFactory>,
    ) -> Result<Self, ComponentCatalogError> {
        let descriptor = factory.descriptor();
        descriptor
            .validate()
            .map_err(|error| ComponentCatalogError::InvalidComponent {
                component_id: descriptor.component_id.clone(),
                detail: error.to_string(),
            })?;
        if self.factories.contains_key(&descriptor.component_id) {
            return Err(ComponentCatalogError::DuplicateComponent {
                component_id: descriptor.component_id.clone(),
            });
        }
        self.factory_order.push(descriptor.component_id.clone());
        self.factories
            .insert(descriptor.component_id.clone(), factory);
        Ok(self)
    }

    pub fn register_module(
        mut self,
        registration: ModuleRegistration,
    ) -> Result<Self, ComponentCatalogError> {
        let module_id = registration.module_id().clone();
        if !self.module_ids.insert(module_id.clone()) {
            return Err(ComponentCatalogError::DuplicateModule { module_id });
        }
        self.modules = self.modules.register(registration)?;
        Ok(self)
    }

    pub fn register_module_factory(
        self,
        registration: ModuleRegistration,
        factory: Arc<dyn ComponentFactory>,
    ) -> Result<Self, ComponentCatalogError> {
        let projected =
            crate::component_descriptor_from_registration(&registration).map_err(|error| {
                ComponentCatalogError::InvalidComponent {
                    component_id: factory.descriptor().component_id.clone(),
                    detail: error.to_string(),
                }
            })?;
        if projected.component_id != factory.descriptor().component_id
            || projected.component_version != factory.descriptor().component_version
        {
            return Err(ComponentCatalogError::RegistrationFactoryMismatch {
                module_id: registration.module_id().clone(),
                component_id: factory.descriptor().component_id.clone(),
            });
        }
        self.register_module(registration)?
            .register_factory(factory)
    }

    pub fn register_storage_contributor(
        mut self,
        component_id: impl Into<String>,
        contributor: ComponentStorageContributor,
    ) -> Result<Self, ComponentCatalogError> {
        let component_id = component_id.into();
        if self
            .storage_contributors
            .insert(component_id.clone(), contributor)
            .is_some()
        {
            return Err(ComponentCatalogError::DuplicateStorageContributor { component_id });
        }
        Ok(self)
    }

    pub fn register_status_contributor(
        mut self,
        component_id: impl Into<String>,
        contributor: ComponentStatusContributor,
    ) -> Result<Self, ComponentCatalogError> {
        let component_id = component_id.into();
        if self
            .status_contributors
            .insert(component_id.clone(), contributor)
            .is_some()
        {
            return Err(ComponentCatalogError::DuplicateStatusContributor { component_id });
        }
        Ok(self)
    }

    pub fn register_startup_contributor(
        mut self,
        component_id: impl Into<String>,
        contributor: ComponentStartupContributor,
    ) -> Result<Self, ComponentCatalogError> {
        let component_id = component_id.into();
        if self
            .startup_contributors
            .insert(component_id.clone(), contributor)
            .is_some()
        {
            return Err(ComponentCatalogError::DuplicateStartupContributor { component_id });
        }
        Ok(self)
    }

    pub fn register_maintenance_inhibitor(
        mut self,
        component_id: impl Into<String>,
        inhibitor: ComponentMaintenanceInhibitor,
    ) -> Result<Self, ComponentCatalogError> {
        let component_id = component_id.into();
        if self
            .maintenance_inhibitors
            .insert(component_id.clone(), inhibitor)
            .is_some()
        {
            return Err(ComponentCatalogError::DuplicateMaintenanceInhibitor { component_id });
        }
        Ok(self)
    }

    pub fn register_migration_contributor(
        mut self,
        component_id: impl Into<String>,
        contributor: ComponentMigrationContributor,
    ) -> Result<Self, ComponentCatalogError> {
        let component_id = component_id.into();
        if self
            .migration_contributors
            .insert(component_id.clone(), contributor)
            .is_some()
        {
            return Err(ComponentCatalogError::DuplicateMigrationContributor { component_id });
        }
        Ok(self)
    }

    pub fn register_native_request_preparer(
        mut self,
        preparer: Arc<dyn crate::NativeModuleRequestPreparer>,
    ) -> Result<Self, ComponentCatalogError> {
        let id = preparer.id().to_string();
        if self
            .native_request_preparers
            .insert(id.clone(), preparer)
            .is_some()
        {
            return Err(ComponentCatalogError::DuplicateNativeRequestPreparer { id });
        }
        Ok(self)
    }

    pub fn register_native_rust_control_target(
        mut self,
        target: crate::NativeRustModuleServiceTarget,
    ) -> Result<Self, ComponentCatalogError> {
        let legacy_id = target.legacy_id.clone();
        if self
            .native_rust_control_targets
            .insert(legacy_id.clone(), target)
            .is_some()
        {
            return Err(ComponentCatalogError::DuplicateNativeRustControlTarget { legacy_id });
        }
        Ok(self)
    }

    pub fn extend(mut self, catalog: &ComponentCatalog) -> Result<Self, ComponentCatalogError> {
        for registration in catalog.modules.registrations() {
            self = self.register_module(registration.clone())?;
        }
        for factory in catalog.factories() {
            self = self.register_factory(factory)?;
        }
        for (component_id, contributor) in &catalog.storage_contributors {
            self = self.register_storage_contributor(component_id, *contributor)?;
        }
        for (component_id, contributor) in &catalog.status_contributors {
            self = self.register_status_contributor(component_id, *contributor)?;
        }
        for (component_id, contributor) in &catalog.startup_contributors {
            self = self.register_startup_contributor(component_id, *contributor)?;
        }
        for (component_id, inhibitor) in &catalog.maintenance_inhibitors {
            self = self.register_maintenance_inhibitor(component_id, *inhibitor)?;
        }
        for (component_id, contributor) in &catalog.migration_contributors {
            self = self.register_migration_contributor(component_id, *contributor)?;
        }
        for preparer in catalog.native_request_preparers.values() {
            self = self.register_native_request_preparer(Arc::clone(preparer))?;
        }
        for target in catalog.native_rust_control_targets.values() {
            self = self.register_native_rust_control_target(target.clone())?;
        }
        Ok(self)
    }

    pub fn build(self) -> Result<ComponentCatalog, ComponentCatalogError> {
        let modules = self.modules.try_build()?;
        Ok(ComponentCatalog {
            factories: self.factories,
            factory_order: self.factory_order,
            modules,
            storage_contributors: self.storage_contributors,
            status_contributors: self.status_contributors,
            startup_contributors: self.startup_contributors,
            maintenance_inhibitors: self.maintenance_inhibitors,
            migration_contributors: self.migration_contributors,
            native_request_preparers: self.native_request_preparers,
            native_rust_control_targets: self.native_rust_control_targets,
        })
    }
}

#[derive(Debug, Error)]
pub enum ComponentCatalogError {
    #[error("component catalog repeats component `{component_id}`")]
    DuplicateComponent { component_id: String },
    #[error("component catalog repeats module `{module_id}`")]
    DuplicateModule { module_id: ModuleId },
    #[error("component catalog repeats storage contributor `{component_id}`")]
    DuplicateStorageContributor { component_id: String },
    #[error("component catalog repeats status contributor `{component_id}`")]
    DuplicateStatusContributor { component_id: String },
    #[error("component catalog repeats startup contributor `{component_id}`")]
    DuplicateStartupContributor { component_id: String },
    #[error("component catalog repeats maintenance inhibitor `{component_id}`")]
    DuplicateMaintenanceInhibitor { component_id: String },
    #[error("component catalog repeats migration contributor `{component_id}`")]
    DuplicateMigrationContributor { component_id: String },
    #[error("component catalog repeats native request preparer `{id}`")]
    DuplicateNativeRequestPreparer { id: String },
    #[error("component catalog repeats native Rust control target `{legacy_id}`")]
    DuplicateNativeRustControlTarget { legacy_id: String },
    #[error("invalid component `{component_id}`: {detail}")]
    InvalidComponent {
        component_id: String,
        detail: String,
    },
    #[error("module `{module_id}` registration does not match component factory `{component_id}`")]
    RegistrationFactoryMismatch {
        module_id: ModuleId,
        component_id: String,
    },
    #[error("component `{component_id}` config contribution failed: {detail}")]
    Config {
        component_id: String,
        detail: String,
    },
    #[error(transparent)]
    ModuleRegistry(#[from] ModuleRegistryError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ComponentDescriptor, StaticComponentFactory};

    fn factory(component_id: &str) -> Arc<dyn ComponentFactory> {
        Arc::new(
            StaticComponentFactory::new(ComponentDescriptor::new(component_id, "1.0.0"), |_, _| {
                Ok(())
            })
            .expect("factory"),
        )
    }

    #[test]
    fn catalog_preserves_factory_order_and_rejects_duplicates() {
        let first = factory("fixture.first");
        let second = factory("fixture.second");
        let catalog = ComponentCatalog::builder()
            .register_factory(Arc::clone(&first))
            .expect("first")
            .register_factory(Arc::clone(&second))
            .expect("second")
            .build()
            .expect("catalog");
        assert_eq!(
            catalog
                .factories()
                .iter()
                .map(|factory| factory.descriptor().component_id.as_str())
                .collect::<Vec<_>>(),
            ["fixture.first", "fixture.second"]
        );
        assert!(matches!(
            catalog.to_builder().register_factory(first),
            Err(ComponentCatalogError::DuplicateComponent { .. })
        ));
    }
}
