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

    pub fn has_native_request_preparer(&self, id: &str) -> bool {
        self.native_request_preparers.contains_key(id)
    }

    pub fn native_rust_control_targets(&self) -> Vec<crate::NativeRustModuleServiceTarget> {
        self.native_rust_control_targets.values().cloned().collect()
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
        for contributor in self.storage_contributors.values() {
            contributor(layout)?;
        }
        Ok(())
    }
}

impl crate::NativeModuleRequestPreparer for ComponentCatalog {
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
    native_request_preparers: BTreeMap<String, Arc<dyn crate::NativeModuleRequestPreparer>>,
    native_rust_control_targets: BTreeMap<String, crate::NativeRustModuleServiceTarget>,
}

pub type ComponentStorageContributor = fn(
    &mut sinomo_core::storage::StorageLayout,
)
    -> Result<(), sinomo_core::storage::StorageContractError>;

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
