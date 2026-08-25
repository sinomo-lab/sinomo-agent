//! Backend-neutral component activation and lifecycle-owned runtime effects.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use sinomo_core::local::SinomoDataDir;
use sinomo_core::service::{ServiceRegistry, ServiceRegistrySnapshotStore};
pub use sinomo_module_api::{
    ComponentDescriptor, ServiceLocality, ServiceOffer, ServiceRequirement,
    ServiceRequirementLocality,
};
use thiserror::Error;

use crate::{ModuleBackendRegistration, ModuleRegistration};

/// Project an existing module registration into the component dependency
/// model. Explicit typed interfaces remain explicit; legacy object-RPC module
/// surfaces receive a deterministic interface identity during migration.
pub fn component_descriptor_from_registration(
    registration: &ModuleRegistration,
) -> Result<ComponentDescriptor, ComponentError> {
    let mut descriptor = if registration.component.is_empty() {
        ComponentDescriptor::new(
            registration.module_id().as_str(),
            registration.manifest.version.clone(),
        )
    } else {
        registration.component.clone()
    };
    if let ModuleBackendRegistration::NativeDylib {
        artifact_id,
        content_hash: _,
        library_name: _,
        platform_triple: _,
        interface_version: _,
    } = &registration.backend
    {
        if descriptor.artifact_id.is_none() {
            descriptor = descriptor.with_artifact(artifact_id.as_str(), 1);
        }
    }

    for export in &registration.surfaces.interface_exports {
        if descriptor
            .offers
            .iter()
            .any(|offer| offer.service_id == export.export_id)
        {
            continue;
        }
        let mut offer = ServiceOffer::new(
            export.export_id.clone(),
            export.interface_id.clone(),
            export.interface_version.clone(),
        );
        for operation in &export.operations {
            offer = offer.with_provider_capability(operation.logical_operation.clone());
            if let sinomo_module_api::ModuleInterfaceOperationTarget::ObjectRpc { route, .. } =
                &operation.target
            {
                offer = offer.with_route(route.clone());
            }
        }
        descriptor.offers.push(offer);
    }

    let mut object_rpc_offers = BTreeMap::<String, ServiceOffer>::new();
    for route in &registration.surfaces.object_rpc_routes {
        let offer = object_rpc_offers
            .entry(route.category.clone())
            .or_insert_with(|| {
                ServiceOffer::new(
                    route.category.clone(),
                    format!("sinomo.object-rpc/{}", route.category),
                    registration.manifest.api_version.clone(),
                )
                .cluster_visible()
            });
        if !offer.provider_capabilities.contains(&route.operation) {
            offer.provider_capabilities.push(route.operation.clone());
        }
        if !offer.caller_capabilities.contains(&route.operation) {
            offer.caller_capabilities.push(route.operation.clone());
        }
        if !offer.routes.contains(&route.route) {
            offer.routes.push(route.route.clone());
        }
    }
    descriptor.offers.extend(object_rpc_offers.into_values());
    descriptor.validate().map_err(ComponentError::contract)?;
    Ok(descriptor)
}

/// Process resources made available to a component factory.
///
/// The context contains host mechanism only. Domain-specific host adapters are
/// themselves services or factory configuration and do not become fields here.
#[derive(Clone)]
pub struct ComponentContext {
    data_dir: Arc<SinomoDataDir>,
    runtime: Option<tokio::runtime::Handle>,
    host_role: ComponentHostRole,
}

impl ComponentContext {
    pub fn new(data_dir: Arc<SinomoDataDir>) -> Self {
        Self {
            data_dir,
            runtime: None,
            host_role: ComponentHostRole::Main,
        }
    }

    pub fn with_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn with_host_role(mut self, host_role: ComponentHostRole) -> Self {
        self.host_role = host_role;
        self
    }

    pub fn data_dir(&self) -> &SinomoDataDir {
        &self.data_dir
    }

    pub fn data_dir_root(&self) -> PathBuf {
        self.data_dir.root()
    }

    pub fn runtime(&self) -> Option<&tokio::runtime::Handle> {
        self.runtime.as_ref()
    }

    pub fn host_role(&self) -> ComponentHostRole {
        self.host_role
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ComponentHostRole {
    #[default]
    Main,
    Shadow,
    Embedded,
}

/// One mounted component generation and every runtime effect it owns.
///
/// Dropping the scope first signals cancellation, then aborts tasks that did
/// not cooperate, and finally runs registered disposers in reverse order. A
/// durable external effect must register a disposer that records or resumes its
/// compensating operation rather than assuming process-local cleanup is enough.
pub struct ComponentScope {
    descriptor: ComponentDescriptor,
    services: ServiceRegistry,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    tasks: Vec<ScopedComponentTask>,
    disposers: Vec<Box<dyn FnOnce() + Send + 'static>>,
    host_start: Vec<Arc<dyn Fn() + Send + Sync + 'static>>,
    host_ready: Vec<Arc<dyn Fn() + Send + Sync + 'static>>,
}

impl std::fmt::Debug for ComponentScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ComponentScope")
            .field("component_id", &self.descriptor.component_id)
            .field("component_version", &self.descriptor.component_version)
            .field("service_routes", &self.services.route_count())
            .field("tasks", &self.tasks.len())
            .field("disposers", &self.disposers.len())
            .finish()
    }
}

impl ComponentScope {
    pub fn new(descriptor: ComponentDescriptor) -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            descriptor,
            services: ServiceRegistry::default(),
            shutdown_tx,
            tasks: Vec::new(),
            disposers: Vec::new(),
            host_start: Vec::new(),
            host_ready: Vec::new(),
        }
    }

    pub fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }

    pub fn services(&self) -> &ServiceRegistry {
        &self.services
    }

    pub fn set_services(&mut self, services: ServiceRegistry) {
        self.services = services;
    }

    pub fn shutdown_receiver(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub fn own_task(&mut self, label: impl Into<String>, task: tokio::task::JoinHandle<()>) {
        self.tasks.push(ScopedComponentTask {
            label: label.into(),
            task,
        });
    }

    pub fn defer(&mut self, dispose: impl FnOnce() + Send + 'static) {
        self.disposers.push(Box::new(dispose));
    }

    pub fn on_host_start(&mut self, callback: impl Fn() + Send + Sync + 'static) {
        self.host_start.push(Arc::new(callback));
    }

    pub fn on_host_ready(&mut self, callback: impl Fn() + Send + Sync + 'static) {
        self.host_ready.push(Arc::new(callback));
    }

    fn notify_host_start(&self) {
        for callback in &self.host_start {
            callback();
        }
    }

    fn notify_host_ready(&self) {
        for callback in &self.host_ready {
            callback();
        }
    }
}

impl Drop for ComponentScope {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        for task in self.tasks.drain(..).rev() {
            tracing::debug!(
                component_id = self.descriptor.component_id,
                task = task.label,
                "stopping component-owned task"
            );
            task.task.abort();
        }
        for dispose in self.disposers.drain(..).rev() {
            dispose();
        }
    }
}

struct ScopedComponentTask {
    label: String,
    task: tokio::task::JoinHandle<()>,
}

/// A backend-specific materializer for one component descriptor.
pub trait ComponentFactory: Send + Sync {
    fn descriptor(&self) -> &ComponentDescriptor;

    fn mount(&self, context: &ComponentContext) -> Result<ComponentScope, ComponentError>;

    fn register_storage_paths(
        &self,
        _layout: &mut sinomo_core::storage::StorageLayout,
    ) -> Result<(), sinomo_core::storage::StorageContractError> {
        Ok(())
    }

    fn register_config_definitions(
        &self,
        builder: sinomo_config::ConfigRegistryBuilder,
    ) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
        Ok(builder)
    }
}

type StaticComponentMount = dyn Fn(&ComponentContext, &mut ComponentScope) -> Result<(), ComponentError>
    + Send
    + Sync
    + 'static;

/// Closure adapter used by built-in Rust modules and focused tests.
pub struct StaticComponentFactory {
    descriptor: ComponentDescriptor,
    mount: Arc<StaticComponentMount>,
    register_storage_paths: Option<
        fn(
            &mut sinomo_core::storage::StorageLayout,
        ) -> Result<(), sinomo_core::storage::StorageContractError>,
    >,
    register_config_definitions: Option<
        fn(
            sinomo_config::ConfigRegistryBuilder,
        ) -> Result<sinomo_config::ConfigRegistryBuilder, String>,
    >,
}

impl StaticComponentFactory {
    pub fn new(
        descriptor: ComponentDescriptor,
        mount: impl Fn(&ComponentContext, &mut ComponentScope) -> Result<(), ComponentError>
        + Send
        + Sync
        + 'static,
    ) -> Result<Self, ComponentError> {
        descriptor.validate().map_err(ComponentError::contract)?;
        Ok(Self {
            descriptor,
            mount: Arc::new(mount),
            register_storage_paths: None,
            register_config_definitions: None,
        })
    }

    pub fn with_storage_paths(
        mut self,
        register_storage_paths: fn(
            &mut sinomo_core::storage::StorageLayout,
        )
            -> Result<(), sinomo_core::storage::StorageContractError>,
    ) -> Self {
        self.register_storage_paths = Some(register_storage_paths);
        self
    }

    pub fn with_config_definitions(
        mut self,
        register_config_definitions: fn(
            sinomo_config::ConfigRegistryBuilder,
        )
            -> Result<sinomo_config::ConfigRegistryBuilder, String>,
    ) -> Self {
        self.register_config_definitions = Some(register_config_definitions);
        self
    }
}

impl ComponentFactory for StaticComponentFactory {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }

    fn mount(&self, context: &ComponentContext) -> Result<ComponentScope, ComponentError> {
        let mut scope = ComponentScope::new(self.descriptor.clone());
        (self.mount)(context, &mut scope)?;
        Ok(scope)
    }

    fn register_storage_paths(
        &self,
        layout: &mut sinomo_core::storage::StorageLayout,
    ) -> Result<(), sinomo_core::storage::StorageContractError> {
        match self.register_storage_paths {
            Some(register) => register(layout),
            None => Ok(()),
        }
    }

    fn register_config_definitions(
        &self,
        builder: sinomo_config::ConfigRegistryBuilder,
    ) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
        match self.register_config_definitions {
            Some(register) => register(builder),
            None => Ok(builder),
        }
    }
}

/// Reconciles desired component factories into one live service registry.
///
/// Existing scopes whose descriptors are unchanged survive registry swaps, so
/// service-owned state is not reconstructed merely because another component
/// was added or removed.
pub struct ComponentReconciler {
    context: ComponentContext,
    base_registry: ServiceRegistry,
    service_registry: ServiceRegistrySnapshotStore,
    external_offers: Vec<ServiceOffer>,
    active: BTreeMap<String, ComponentScope>,
}

impl ComponentReconciler {
    pub fn new(context: ComponentContext, base_registry: ServiceRegistry) -> Self {
        Self {
            context,
            service_registry: ServiceRegistrySnapshotStore::new(base_registry.clone()),
            base_registry,
            external_offers: Vec::new(),
            active: BTreeMap::new(),
        }
    }

    pub fn with_service_registry(
        context: ComponentContext,
        base_registry: ServiceRegistry,
        service_registry: ServiceRegistrySnapshotStore,
    ) -> Self {
        Self {
            context,
            base_registry,
            service_registry,
            external_offers: Vec::new(),
            active: BTreeMap::new(),
        }
    }

    pub fn service_registry(&self) -> &ServiceRegistrySnapshotStore {
        &self.service_registry
    }

    pub fn set_external_offers(&mut self, offers: Vec<ServiceOffer>) {
        self.external_offers = offers;
    }

    pub fn active_component_ids(&self) -> Vec<String> {
        self.active.keys().cloned().collect()
    }

    pub fn notify_host_start(&self) {
        for scope in self.active.values() {
            scope.notify_host_start();
        }
    }

    pub fn notify_host_ready(&self) {
        for scope in self.active.values() {
            scope.notify_host_ready();
        }
    }

    pub fn reconcile(
        &mut self,
        factories: &[Arc<dyn ComponentFactory>],
    ) -> Result<ComponentReconcileReport, ComponentError> {
        let factories = validated_factories(factories)?;
        let (activation_order, pending) =
            resolve_component_activation(&factories, &self.external_offers);

        let desired_ids = activation_order
            .iter()
            .map(|factory| factory.descriptor().component_id.clone())
            .collect::<BTreeSet<_>>();
        let mut mounted = BTreeMap::new();
        let mut started = Vec::new();
        let mut reused = Vec::new();
        for factory in &activation_order {
            let descriptor = factory.descriptor();
            if self
                .active
                .get(&descriptor.component_id)
                .is_some_and(|scope| scope.descriptor() == descriptor)
            {
                reused.push(descriptor.component_id.clone());
                continue;
            }
            let mut scope =
                factory
                    .mount(&self.context)
                    .map_err(|error| ComponentError::Mount {
                        component_id: descriptor.component_id.clone(),
                        detail: error.to_string(),
                    })?;
            if scope.descriptor() != descriptor {
                return Err(ComponentError::DescriptorDrift {
                    component_id: descriptor.component_id.clone(),
                });
            }
            let services = services_with_component_identity(scope.services(), descriptor);
            scope.set_services(services);
            started.push(descriptor.component_id.clone());
            mounted.insert(descriptor.component_id.clone(), scope);
        }

        validate_route_ownership(&self.base_registry, &self.active, &mounted, &desired_ids)?;
        let mut builder = self.base_registry.to_builder();
        for factory in &activation_order {
            let component_id = &factory.descriptor().component_id;
            let scope = mounted
                .get(component_id)
                .or_else(|| self.active.get(component_id))
                .expect("resolved component must have a mounted or reusable scope");
            builder = builder.merge_registry(scope.services());
        }
        let snapshot = self
            .service_registry
            .replace_registry(builder.build())
            .map_err(|error| ComponentError::Registry {
                detail: error.to_string(),
            })?;

        let stopped = self
            .active
            .keys()
            .filter(|component_id| {
                !desired_ids.contains(*component_id) || mounted.contains_key(*component_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        self.active.retain(|component_id, _| {
            desired_ids.contains(component_id) && !mounted.contains_key(component_id)
        });
        self.active.extend(mounted);

        Ok(ComponentReconcileReport {
            active: activation_order
                .iter()
                .map(|factory| factory.descriptor().component_id.clone())
                .collect(),
            pending,
            started,
            reused,
            stopped,
            service_registry_version: snapshot.version(),
        })
    }
}

fn services_with_component_identity(
    services: &ServiceRegistry,
    descriptor: &ComponentDescriptor,
) -> ServiceRegistry {
    let mut builder = services.to_builder();
    for route in services.route_categories() {
        builder = builder
            .replace_route_metadata(service_route_with_component_identity(route, descriptor));
    }
    builder.build()
}

pub fn service_route_with_component_identity(
    route: sinomo_core::service::ServiceRoute,
    descriptor: &ComponentDescriptor,
) -> sinomo_core::service::ServiceRoute {
    let route_category = route.category.clone();
    let offer = descriptor.offers.iter().find(|offer| {
        offer.routes.iter().any(|offered| offered == &route.route)
            || offer.service_id == route.category
    });
    let provider_capabilities = offer
        .map(|offer| offer.provider_capabilities.clone())
        .unwrap_or_default();
    let mut caller_requirements = offer
        .map(|offer| offer.caller_capabilities.clone())
        .unwrap_or_default();
    for capability in &route.required_capabilities {
        if !caller_requirements.contains(capability) {
            caller_requirements.push(capability.clone());
        }
    }
    let identity = sinomo_core::service::ServiceDiscoveryIdentity {
        service_id: offer
            .map(|offer| offer.service_id.clone())
            .unwrap_or_else(|| route_category.clone()),
        interface_id: offer
            .map(|offer| offer.interface_id.clone())
            .unwrap_or_else(|| format!("sinomo.object-rpc/{route_category}")),
        interface_version: offer
            .map(|offer| offer.interface_version.clone())
            .unwrap_or_else(|| descriptor.component_version.clone()),
        provider_capabilities,
        caller_requirements,
        component_id: descriptor.component_id.clone(),
        component_version: descriptor.component_version.clone(),
        artifact_id: descriptor.artifact_id.clone(),
        component_generation: descriptor.artifact_generation.max(1),
        health: sinomo_core::service::ServiceDiscoveryHealth::Ready,
        locality: match offer.map(|offer| offer.locality).unwrap_or_default() {
            ServiceLocality::Local => sinomo_core::service::ServiceDiscoveryLocality::Local,
            ServiceLocality::Cluster => sinomo_core::service::ServiceDiscoveryLocality::Cluster,
        },
    };
    route.with_discovery_identity(identity)
}

pub fn register_component_config_definitions(
    mut builder: sinomo_config::ConfigRegistryBuilder,
    factories: &[Arc<dyn ComponentFactory>],
) -> Result<sinomo_config::ConfigRegistryBuilder, String> {
    for factory in factories {
        builder = factory.register_config_definitions(builder)?;
    }
    Ok(builder)
}

pub fn register_component_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
    factories: &[Arc<dyn ComponentFactory>],
) -> Result<(), sinomo_core::storage::StorageContractError> {
    for factory in factories {
        factory.register_storage_paths(layout)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentReconcileReport {
    pub active: Vec<String>,
    pub pending: Vec<PendingComponent>,
    pub started: Vec<String>,
    pub reused: Vec<String>,
    pub stopped: Vec<String>,
    pub service_registry_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingComponent {
    pub component_id: String,
    pub missing_requirements: Vec<ServiceRequirement>,
}

#[derive(Debug, Error)]
pub enum ComponentError {
    #[error("invalid component contract: {detail}")]
    Contract { detail: String },
    #[error("duplicate component id `{component_id}`")]
    DuplicateComponent { component_id: String },
    #[error("mount component `{component_id}`: {detail}")]
    Mount {
        component_id: String,
        detail: String,
    },
    #[error("component `{component_id}` mounted with a different descriptor")]
    DescriptorDrift { component_id: String },
    #[error(
        "service route `{route}` is claimed by both component `{first_component_id}` and `{second_component_id}`"
    )]
    RouteCollision {
        route: String,
        first_component_id: String,
        second_component_id: String,
    },
    #[error("replace component service registry: {detail}")]
    Registry { detail: String },
    #[error("component `{component_id}` requires a runtime handle")]
    RuntimeUnavailable { component_id: String },
    #[error("component `{component_id}` requires a data-directory path")]
    DataDirUnavailable { component_id: String },
    #[error("component `{component_id}` I/O at {path}: {detail}")]
    Io {
        component_id: String,
        path: PathBuf,
        detail: String,
    },
    #[error("component `{component_id}`: {detail}")]
    Component {
        component_id: String,
        detail: String,
    },
}

impl ComponentError {
    pub fn contract(error: impl std::fmt::Display) -> Self {
        Self::Contract {
            detail: error.to_string(),
        }
    }

    pub fn component(component_id: impl Into<String>, error: impl std::fmt::Display) -> Self {
        Self::Component {
            component_id: component_id.into(),
            detail: error.to_string(),
        }
    }
}

fn validated_factories(
    factories: &[Arc<dyn ComponentFactory>],
) -> Result<BTreeMap<String, Arc<dyn ComponentFactory>>, ComponentError> {
    let mut by_id = BTreeMap::new();
    for factory in factories {
        let descriptor = factory.descriptor();
        descriptor.validate().map_err(ComponentError::contract)?;
        if by_id
            .insert(descriptor.component_id.clone(), Arc::clone(factory))
            .is_some()
        {
            return Err(ComponentError::DuplicateComponent {
                component_id: descriptor.component_id.clone(),
            });
        }
    }
    Ok(by_id)
}

fn resolve_component_activation(
    factories: &BTreeMap<String, Arc<dyn ComponentFactory>>,
    external_offers: &[ServiceOffer],
) -> (Vec<Arc<dyn ComponentFactory>>, Vec<PendingComponent>) {
    let mut available = external_offers.to_vec();
    let mut remaining = factories.clone();
    let mut active = Vec::new();
    loop {
        let ready = remaining
            .iter()
            .filter(|(_, factory)| {
                factory
                    .descriptor()
                    .requirements
                    .iter()
                    .filter(|requirement| !requirement.optional)
                    .all(|requirement| {
                        available
                            .iter()
                            .any(|offer| requirement.is_satisfied_by(offer))
                    })
            })
            .map(|(component_id, _)| component_id.clone())
            .collect::<Vec<_>>();
        if ready.is_empty() {
            break;
        }
        for component_id in ready {
            let factory = remaining
                .remove(&component_id)
                .expect("ready component remains registered");
            available.extend(factory.descriptor().offers.clone());
            active.push(factory);
        }
    }
    let pending = remaining
        .into_values()
        .map(|factory| PendingComponent {
            component_id: factory.descriptor().component_id.clone(),
            missing_requirements: factory
                .descriptor()
                .requirements
                .iter()
                .filter(|requirement| {
                    !requirement.optional
                        && !available
                            .iter()
                            .any(|offer| requirement.is_satisfied_by(offer))
                })
                .cloned()
                .collect(),
        })
        .collect();
    (active, pending)
}

fn validate_route_ownership(
    base_registry: &ServiceRegistry,
    active: &BTreeMap<String, ComponentScope>,
    mounted: &BTreeMap<String, ComponentScope>,
    desired_ids: &BTreeSet<String>,
) -> Result<(), ComponentError> {
    let mut routes = base_registry
        .route_categories()
        .into_iter()
        .map(|route| (route.route, "host".to_string()))
        .collect::<BTreeMap<_, _>>();
    let scopes = desired_ids.iter().filter_map(|component_id| {
        mounted
            .get(component_id)
            .or_else(|| active.get(component_id))
            .map(|scope| (component_id, scope))
    });
    for (component_id, scope) in scopes {
        for route in scope.services().route_categories() {
            if let Some(first_component_id) =
                routes.insert(route.route.clone(), component_id.clone())
            {
                return Err(ComponentError::RouteCollision {
                    route: route.route,
                    first_component_id,
                    second_component_id: component_id.clone(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sinomo_core::network::ObjectRpcRequest;
    use sinomo_core::service::{
        Service, ServiceContext, ServiceDisposition, ServiceFuture, ServiceRegistryBuilder,
    };

    use super::*;

    struct NamedService {
        name: &'static str,
        routes: &'static [&'static str],
    }

    impl Service for NamedService {
        fn name(&self) -> &'static str {
            self.name
        }

        fn routes(&self) -> &'static [&'static str] {
            self.routes
        }

        fn handle(&self, _ctx: ServiceContext, _request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async { Ok(ServiceDisposition::Continue) })
        }
    }

    fn context() -> ComponentContext {
        ComponentContext::new(Arc::new(SinomoDataDir::new("/tmp/component-test")))
    }

    fn factory(
        descriptor: ComponentDescriptor,
        service_name: &'static str,
        routes: &'static [&'static str],
        mounts: Arc<AtomicUsize>,
    ) -> Arc<dyn ComponentFactory> {
        Arc::new(
            StaticComponentFactory::new(descriptor, move |_, scope| {
                mounts.fetch_add(1, Ordering::SeqCst);
                scope.set_services(
                    ServiceRegistryBuilder::default()
                        .register(NamedService {
                            name: service_name,
                            routes,
                        })
                        .build(),
                );
                Ok(())
            })
            .expect("component factory"),
        )
    }

    #[test]
    fn dependencies_activate_reactively_and_unchanged_scopes_survive_registry_swaps() {
        let provider_mounts = Arc::new(AtomicUsize::new(0));
        let consumer_mounts = Arc::new(AtomicUsize::new(0));
        let offer =
            ServiceOffer::new("echo", "sinomo.fixture.echo", "1.0.0").with_route("/fixture/echo");
        let provider = factory(
            ComponentDescriptor::new("fixture.provider", "1.0.0").with_offer(offer),
            "fixture-provider",
            &["/fixture/echo"],
            Arc::clone(&provider_mounts),
        );
        let consumer = factory(
            ComponentDescriptor::new("fixture.consumer", "1.0.0").require(
                ServiceRequirement::new("sinomo.fixture.echo", "1.0.0").from_service("echo"),
            ),
            "fixture-consumer",
            &["/fixture/consumer"],
            Arc::clone(&consumer_mounts),
        );
        let mut reconciler = ComponentReconciler::new(context(), ServiceRegistry::default());

        let first = reconciler
            .reconcile(&[Arc::clone(&consumer)])
            .expect("pending consumer");
        assert!(first.active.is_empty());
        assert_eq!(first.pending.len(), 1);

        let second = reconciler
            .reconcile(&[Arc::clone(&consumer), Arc::clone(&provider)])
            .expect("activate graph");
        assert_eq!(second.active, ["fixture.provider", "fixture.consumer"]);
        assert_eq!(provider_mounts.load(Ordering::SeqCst), 1);
        assert_eq!(consumer_mounts.load(Ordering::SeqCst), 1);
        let active = reconciler.service_registry().active_snapshot();
        assert!(active.registry().owns("/fixture/echo"));
        assert!(active.registry().owns("/fixture/consumer"));

        let third = reconciler
            .reconcile(&[consumer, provider])
            .expect("reuse graph");
        assert_eq!(third.reused.len(), 2);
        assert_eq!(provider_mounts.load(Ordering::SeqCst), 1);
        assert_eq!(consumer_mounts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn route_collision_is_rejected_before_registry_publication() {
        let mounts = Arc::new(AtomicUsize::new(0));
        let first = factory(
            ComponentDescriptor::new("fixture.first", "1.0.0"),
            "first",
            &["/collision"],
            Arc::clone(&mounts),
        );
        let second = factory(
            ComponentDescriptor::new("fixture.second", "1.0.0"),
            "second",
            &["/collision"],
            mounts,
        );
        let mut reconciler = ComponentReconciler::new(context(), ServiceRegistry::default());
        let error = reconciler
            .reconcile(&[first, second])
            .expect_err("route collision");
        assert!(matches!(error, ComponentError::RouteCollision { .. }));
        assert_eq!(
            reconciler
                .service_registry()
                .active_snapshot()
                .registry()
                .route_count(),
            0
        );
    }

    #[test]
    fn component_scope_unwinds_compensations_in_reverse_registration_order() {
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let mut scope = ComponentScope::new(ComponentDescriptor::new("fixture.scope", "1.0.0"));
        for value in [1, 2, 3] {
            let observed = Arc::clone(&observed);
            scope.defer(move || observed.lock().expect("observations").push(value));
        }
        drop(scope);
        assert_eq!(*observed.lock().expect("observations"), [3, 2, 1]);
    }
}
