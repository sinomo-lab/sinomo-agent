//! Backend-neutral component activation and lifecycle-owned runtime effects.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{ClusterObjectRpcOptions, NetworkError, ObjectRpcCall, ObjectRpcResult};
use sinomo_core::node::NodeId;
use sinomo_core::service::ClusterAccess;
use sinomo_core::service::{ServiceRegistry, ServiceRegistrySnapshotStore};
pub use sinomo_module_api::{
    ComponentDescriptor, ComponentEventDurability, ComponentEventLocality,
    ComponentEventPublication, ComponentEventSubscription, ServiceLocality, ServiceOffer,
    ServiceOperationOffer, ServiceRequirement, ServiceRequirementLocality,
};
use thiserror::Error;

use crate::{ModuleBackendRegistration, ModuleRegistration};

/// Host-owned, target-addressed object-RPC transport available to components.
///
/// The interface exposes only the generic cluster transport mechanism. Domain
/// modules retain ownership of route selection, request and response types,
/// retry policy, authority validation, and delivery-state interpretation.
pub trait ComponentObjectRpcTransport: Send + Sync {
    /// Retained routing capability of this SAME carrier, never health,
    /// membership, storage readiness, effect permission or an RPC result.
    /// Hosts may validate their resident route snapshot without constructing
    /// cold carriers. Missing evidence preserves the existing network projection.
    fn availability(&self, _target_node_id: &NodeId) -> sinomo_core::network::RoutedAvailability {
        sinomo_core::network::RoutedAvailability::Unknown
    }

    fn call(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, NetworkError>;

    fn call_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> Pin<Box<dyn Future<Output = Result<ObjectRpcResult, NetworkError>> + Send + 'a>>;
}

/// Optional process-level activity counters supplied by the embedding host.
///
/// Counter names are stable contract strings. Components may also publish
/// ordinary `metrics` records; this observer exists for the daemon's compact
/// idle-I/O status projection without coupling modules to `sinomo-observe`.
pub trait ComponentActivityObserver: Send + Sync {
    fn increment(&self, counter: &'static str, amount: u64);
}

pub const COMPONENT_ACTIVITY_DISK_READ_DFS_METADATA: &str = "disk-read.dfs-metadata";
pub const COMPONENT_ACTIVITY_DFS_METADATA_REPLICATION_SENT: &str = "dfs.metadata-replication-sent";

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
        && descriptor.artifact_id.is_none()
    {
        descriptor = descriptor.with_artifact(artifact_id.as_str(), 1);
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
            let mut operation_offer = ServiceOperationOffer::new(&operation.logical_operation);
            if let sinomo_module_api::ModuleInterfaceOperationTarget::ObjectRpc { route, .. } =
                &operation.target
            {
                operation_offer = operation_offer.at_route(route);
            }
            offer = offer.with_operation(operation_offer);
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
        if !offer
            .operations
            .iter()
            .any(|operation| operation.operation == route.operation)
        {
            let operation = ServiceOperationOffer::new(&route.operation)
                .at_route(&route.route)
                .require_caller_capability(&route.operation);
            if !offer.provider_capabilities.contains(&route.operation) {
                offer.provider_capabilities.push(route.operation.clone());
            }
            if !offer.routes.contains(&route.route) {
                offer.routes.push(route.route.clone());
            }
            offer.operations.push(operation);
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
    cluster: Option<Arc<dyn ClusterAccess>>,
    object_rpc: Option<Arc<dyn ComponentObjectRpcTransport>>,
    node_resources: Option<sinomo_device::NodeResourceMonitor>,
    activity_observer: Option<Arc<dyn ComponentActivityObserver>>,
    service_registry: Option<ServiceRegistrySnapshotStore>,
    module_runtime: Option<Arc<crate::ModuleRuntimeHost>>,
    event_bus: Option<crate::ComponentEventBus>,
}

impl ComponentContext {
    pub fn new(data_dir: Arc<SinomoDataDir>) -> Self {
        Self {
            data_dir,
            runtime: None,
            host_role: ComponentHostRole::Main,
            cluster: None,
            object_rpc: None,
            node_resources: None,
            activity_observer: None,
            service_registry: None,
            module_runtime: None,
            event_bus: None,
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

    pub fn with_cluster_access(mut self, cluster: Arc<dyn ClusterAccess>) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn with_object_rpc_transport(
        mut self,
        object_rpc: Arc<dyn ComponentObjectRpcTransport>,
    ) -> Self {
        self.object_rpc = Some(object_rpc);
        self
    }

    pub fn with_node_resources(
        mut self,
        node_resources: sinomo_device::NodeResourceMonitor,
    ) -> Self {
        self.node_resources = Some(node_resources);
        self
    }

    pub fn with_activity_observer(
        mut self,
        activity_observer: Arc<dyn ComponentActivityObserver>,
    ) -> Self {
        self.activity_observer = Some(activity_observer);
        self
    }

    fn with_service_registry(mut self, service_registry: ServiceRegistrySnapshotStore) -> Self {
        self.service_registry = Some(service_registry);
        self
    }

    pub fn with_module_runtime(mut self, module_runtime: crate::ModuleRuntimeHost) -> Self {
        self.module_runtime = Some(crate::runtime_generation::bind_process_module_host(
            &self.data_dir.root(),
            module_runtime,
        ));
        self
    }

    pub fn with_event_bus(mut self, event_bus: crate::ComponentEventBus) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    pub fn service_client(
        &self,
        caller: &ComponentDescriptor,
    ) -> Result<crate::ComponentServiceClient, ComponentError> {
        let service_registry =
            self.service_registry
                .clone()
                .ok_or_else(|| ComponentError::Component {
                    component_id: caller.component_id.clone(),
                    detail: "component service registry is unavailable".to_string(),
                })?;
        Ok(crate::ComponentServiceClient::new(
            caller.clone(),
            Arc::clone(&self.data_dir),
            service_registry,
        ))
    }

    pub fn service_registry_snapshot_store(
        &self,
    ) -> Result<ServiceRegistrySnapshotStore, ComponentError> {
        self.service_registry
            .clone()
            .ok_or_else(|| ComponentError::Component {
                component_id: "component-host".to_string(),
                detail: "component service registry is unavailable".to_string(),
            })
    }

    pub fn extension_registry_snapshot(
        &self,
    ) -> Result<Arc<crate::ExtensionRegistrySnapshot>, ComponentError> {
        self.module_runtime
            .as_ref()
            .map(|runtime| {
                runtime
                    .generations
                    .active_snapshot()
                    .extension_registry
                    .clone()
            })
            .ok_or_else(|| ComponentError::Component {
                component_id: "component-host".to_string(),
                detail: "component extension registry is unavailable".to_string(),
            })
    }

    /// Build an executor over the exact extension and service generations
    /// exposed to this component. Reducers, pipeline validators, and final
    /// state application remain trusted code owned by the extension point.
    pub fn extension_executor(
        &self,
        owner: &ComponentDescriptor,
    ) -> Result<crate::ExtensionExecutor, ComponentError> {
        Ok(crate::ExtensionExecutor::new(
            owner.clone(),
            self.extension_registry_snapshot()?,
            Arc::clone(&self.data_dir),
            self.service_registry_snapshot_store()?,
        )
        .with_invocation_leases(
            self.module_runtime
                .as_ref()
                .expect("extension registry availability checked above")
                .invocations
                .clone(),
        ))
    }

    pub fn event_client(
        &self,
        caller: &ComponentDescriptor,
    ) -> Result<crate::ComponentEventClient, ComponentError> {
        let event_bus = self
            .event_bus
            .clone()
            .ok_or_else(|| ComponentError::Component {
                component_id: caller.component_id.clone(),
                detail: "component event bus is unavailable".to_string(),
            })?;
        Ok(crate::ComponentEventClient::new(caller.clone(), event_bus))
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

    pub fn cluster_access(&self) -> Option<Arc<dyn ClusterAccess>> {
        self.cluster.clone()
    }

    pub fn object_rpc_transport(&self) -> Option<Arc<dyn ComponentObjectRpcTransport>> {
        self.object_rpc.clone()
    }

    pub fn node_resources(&self) -> Option<sinomo_device::NodeResourceMonitor> {
        self.node_resources.clone()
    }

    pub fn activity_observer(&self) -> Option<Arc<dyn ComponentActivityObserver>> {
        self.activity_observer.clone()
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
    host_prepare: Vec<ComponentHostPrepare>,
    task_drains: Vec<ComponentTaskDrain>,
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
            host_prepare: Vec::new(),
            task_drains: Vec::new(),
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

    /// Register asynchronous preparation that must complete before the host
    /// begins serving requests. Components use this for authoritative startup
    /// recovery that cannot safely race their first request.
    pub fn on_host_prepare<F, Fut>(&mut self, prepare: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        self.host_prepare
            .push(Arc::new(move || Box::pin(prepare())));
    }

    /// Join component-owned child registries after orchestration tasks stop,
    /// before disposers can release storage. Cancellation/failure retains this
    /// callback and the retiring scope for a bounded retry.
    pub fn on_task_drain<F, Fut>(&mut self, label: impl Into<String>, drain: F)
    where
        F: Fn(tokio::time::Instant) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        self.task_drains.push(ComponentTaskDrain {
            label: label.into(),
            drain: Arc::new(move |deadline| Box::pin(drain(deadline))),
        });
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

    async fn prepare_host(&self) -> Result<(), ComponentError> {
        for prepare in &self.host_prepare {
            prepare()
                .await
                .map_err(|detail| ComponentError::Component {
                    component_id: self.descriptor.component_id.clone(),
                    detail,
                })?;
        }
        Ok(())
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

struct ComponentTaskDrain {
    label: String,
    drain: Arc<
        dyn Fn(
                tokio::time::Instant,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>
            + Send
            + Sync
            + 'static,
    >,
}

type ComponentHostPrepare = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

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
    event_bus: crate::ComponentEventBus,
    external_offers: Vec<ServiceOffer>,
    active: BTreeMap<String, ComponentScope>,
    retiring: BTreeMap<String, Vec<ComponentScope>>,
}

impl ComponentReconciler {
    pub fn new(context: ComponentContext, base_registry: ServiceRegistry) -> Self {
        let service_registry = ServiceRegistrySnapshotStore::new(base_registry.clone());
        let event_bus = context.event_bus.clone().unwrap_or_default();
        Self {
            context: context
                .with_service_registry(service_registry.clone())
                .with_event_bus(event_bus.clone()),
            service_registry,
            event_bus,
            base_registry,
            external_offers: Vec::new(),
            active: BTreeMap::new(),
            retiring: BTreeMap::new(),
        }
    }

    pub fn with_service_registry(
        context: ComponentContext,
        base_registry: ServiceRegistry,
        service_registry: ServiceRegistrySnapshotStore,
    ) -> Self {
        let event_bus = context.event_bus.clone().unwrap_or_default();
        Self {
            context: context
                .with_service_registry(service_registry.clone())
                .with_event_bus(event_bus.clone()),
            base_registry,
            service_registry,
            event_bus,
            external_offers: Vec::new(),
            active: BTreeMap::new(),
            retiring: BTreeMap::new(),
        }
    }

    /// Borrow the initialized host context for composition with the same mechanisms.
    pub fn context(&self) -> &ComponentContext {
        &self.context
    }

    pub fn service_registry(&self) -> &ServiceRegistrySnapshotStore {
        &self.service_registry
    }

    pub fn event_bus(&self) -> &crate::ComponentEventBus {
        &self.event_bus
    }

    pub fn set_external_offers(&mut self, offers: Vec<ServiceOffer>) {
        self.external_offers = offers;
    }

    pub fn active_component_ids(&self) -> Vec<String> {
        self.active.keys().cloned().collect()
    }

    pub fn planned_active_component_ids(
        &self,
        factories: &[Arc<dyn ComponentFactory>],
    ) -> Result<Vec<String>, ComponentError> {
        let factories = validated_factories(factories)?;
        let (activation_order, _) = resolve_component_activation(&factories, &self.external_offers);
        Ok(activation_order
            .iter()
            .map(|factory| factory.descriptor().component_id.clone())
            .collect())
    }

    pub fn notify_host_start(&self) {
        for scope in self.active.values() {
            scope.notify_host_start();
        }
    }

    pub async fn prepare_host(&self) -> Result<(), ComponentError> {
        for scope in self.active.values() {
            scope.prepare_host().await?;
        }
        Ok(())
    }

    pub fn shutdown(&mut self) -> Result<(), ComponentError> {
        self.service_registry
            .withdraw_for_host_stop(self.base_registry.clone())
            .map_err(|error| ComponentError::Registry {
                detail: error.to_string(),
            })?;
        for (component_id, scope) in std::mem::take(&mut self.active) {
            self.retiring.entry(component_id).or_default().push(scope);
        }
        for scopes in self.retiring.values() {
            for scope in scopes {
                let _ = scope.shutdown_tx.send(true);
                for task in &scope.tasks {
                    task.task.abort();
                }
            }
        }
        let ids = self.retiring.keys().cloned().collect::<Vec<_>>();
        self.finish_retiring(&ids);
        if !self.retiring.is_empty() {
            return Err(ComponentError::Component {
                component_id: "component-host".into(),
                detail: "component work remains owned; bounded asynchronous shutdown is required"
                    .into(),
            });
        }
        Ok(())
    }

    /// Stop admission, signal and abort owned tasks, then join before disposal.
    /// On timeout the retiring scopes remain owned by this reconciler; callers
    /// must retain their storage until the live task has finished.
    pub async fn stop_tasks_and_wait(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<(), ComponentError> {
        self.service_registry
            .withdraw_for_host_stop(self.base_registry.clone())
            .map_err(|error| ComponentError::Registry {
                detail: error.to_string(),
            })?;
        for (component_id, scope) in std::mem::take(&mut self.active) {
            self.retiring.entry(component_id).or_default().push(scope);
        }
        let ids = self.retiring.keys().cloned().collect::<Vec<_>>();
        self.drain_retiring_until(&ids, tokio::time::Instant::now() + timeout)
            .await
    }

    /// Stop and join only the already-withdrawn generations in this transition.
    /// Other active component generations retain their tasks and services.
    pub async fn finish_retiring_and_wait(
        &mut self,
        component_ids: &[String],
        deadline: tokio::time::Instant,
    ) -> Result<(), ComponentError> {
        self.drain_retiring_until(component_ids, deadline).await?;
        self.finish_retiring(component_ids);
        Ok(())
    }

    async fn drain_retiring_until(
        &mut self,
        component_ids: &[String],
        deadline: tokio::time::Instant,
    ) -> Result<(), ComponentError> {
        for (id, scopes) in &mut self.retiring {
            if !component_ids.contains(id) {
                continue;
            }
            for scope in scopes {
                let _ = scope.shutdown_tx.send(true);
                for task in &scope.tasks {
                    task.task.abort();
                }
            }
        }
        for (component_id, scopes) in &mut self.retiring {
            if !component_ids.contains(component_id) {
                continue;
            }
            for scope in scopes {
                while let Some(task) = scope.tasks.last_mut() {
                    let label = task.label.clone();
                    match tokio::time::timeout_at(deadline, &mut task.task).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) if error.is_cancelled() => {}
                        Ok(Err(error)) => {
                            scope.tasks.pop();
                            return Err(ComponentError::Component {
                                component_id: component_id.clone(),
                                detail: format!("join {label}: {error}"),
                            });
                        }
                        Err(_) => {
                            return Err(ComponentError::Component {
                                component_id: component_id.clone(),
                                detail: format!("timed out joining {label}"),
                            });
                        }
                    }
                    scope.tasks.pop();
                }
            }
        }
        // Every orchestration handle is joined before child draining starts.
        // A started worker can have registered its child during that final join.
        for (component_id, scopes) in &mut self.retiring {
            if !component_ids.contains(component_id) {
                continue;
            }
            for scope in scopes {
                while let Some(drain) = scope.task_drains.last() {
                    let label = drain.label.clone();
                    match tokio::time::timeout_at(deadline, (drain.drain)(deadline)).await {
                        Ok(Ok(())) => {
                            scope.task_drains.pop();
                        }
                        Ok(Err(detail)) => {
                            return Err(ComponentError::Component {
                                component_id: component_id.clone(),
                                detail: format!("drain {label}: {detail}"),
                            });
                        }
                        Err(_) => {
                            return Err(ComponentError::Component {
                                component_id: component_id.clone(),
                                detail: format!("timed out draining {label}"),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Join owned component tasks and then dispose the stopped scopes.
    pub async fn shutdown_and_wait(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<(), ComponentError> {
        self.stop_tasks_and_wait(timeout).await?;
        self.retiring.clear();
        Ok(())
    }

    /// Synchronous host bridge for embedders whose registry assembly API is
    /// synchronous. No runtime is needed when no component registered a
    /// preparation barrier.
    pub fn prepare_host_on_runtime(
        &self,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> Result<(), ComponentError> {
        if self
            .active
            .values()
            .all(|scope| scope.host_prepare.is_empty())
        {
            return Ok(());
        }
        let runtime = runtime.ok_or_else(|| ComponentError::RuntimeUnavailable {
            component_id: "component-host".to_string(),
        })?;
        let future = self.prepare_host();
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            if matches!(
                current.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                return tokio::task::block_in_place(|| runtime.block_on(future));
            }
            return Err(ComponentError::Component {
                component_id: "component-host".to_string(),
                detail: "component preparation cannot synchronously bridge a current-thread Tokio runtime"
                    .to_string(),
            });
        }
        runtime.block_on(future)
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
        let report = self.reconcile_staged(factories)?;
        self.finish_retiring(&report.stopped);
        Ok(report)
    }

    /// Publish a new base/service generation while retaining removed scopes.
    /// Hot-unload callers wait for invocation leases before calling
    /// [`Self::finish_retiring`], so no new request can enter a removed route
    /// and existing handlers keep their component-owned state until drained.
    pub fn reconcile_staged_with_base_registry(
        &mut self,
        base_registry: ServiceRegistry,
        factories: &[Arc<dyn ComponentFactory>],
    ) -> Result<ComponentReconcileReport, ComponentError> {
        let previous = std::mem::replace(&mut self.base_registry, base_registry);
        match self.reconcile_staged(factories) {
            Ok(report) => Ok(report),
            Err(error) => {
                self.base_registry = previous;
                Err(error)
            }
        }
    }

    pub fn finish_retiring(&mut self, component_ids: &[String]) {
        for component_id in component_ids {
            let stopped = self.retiring.get(component_id).is_some_and(|scopes| {
                scopes.iter().all(|scope| {
                    scope.task_drains.is_empty()
                        && scope.tasks.iter().all(|task| task.task.is_finished())
                })
            });
            if stopped {
                self.retiring.remove(component_id);
            }
        }
    }

    fn reconcile_staged(
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
        for component_id in &stopped {
            if let Some(scope) = self.active.remove(component_id) {
                self.retiring
                    .entry(component_id.clone())
                    .or_default()
                    .push(scope);
            }
        }
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
    // A component may expose a broad service whose id matches the transport
    // category alongside a narrower logical service on one of that category's
    // routes. Resolve every exact route offer before falling back to the
    // category identity; otherwise declaration order can hide the narrower
    // service and its provider capabilities from cluster discovery.
    let offer = descriptor
        .offers
        .iter()
        .find(|offer| offer.routes.iter().any(|offered| offered == &route.route))
        .or_else(|| {
            descriptor
                .offers
                .iter()
                .find(|offer| offer.service_id == route.category)
        });
    let operation_offer = offer.and_then(|offer| {
        offer
            .operations
            .iter()
            .find(|operation| operation.route.as_deref() == Some(route.route.as_str()))
    });
    let provider_capabilities = operation_offer
        .map(|operation| vec![operation.operation.clone()])
        .or_else(|| offer.map(|offer| offer.provider_capabilities.clone()))
        .unwrap_or_default();
    let mut caller_requirements = offer
        .map(|offer| offer.caller_capabilities.clone())
        .unwrap_or_default();
    if let Some(operation) = operation_offer {
        for capability in &operation.caller_capabilities {
            if !caller_requirements.contains(capability) {
                caller_requirements.push(capability.clone());
            }
        }
    }
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
    fn exact_route_offer_precedes_transport_category_fallback() {
        let descriptor = ComponentDescriptor::new("fixture.provider", "1.0.0")
            .with_offer(
                ServiceOffer::new("daemon", "sinomo.daemon", "1.0.0")
                    .cluster_visible()
                    .with_route("/daemon/general")
                    .with_provider_capability("daemon.general"),
            )
            .with_offer(
                ServiceOffer::new("path-access", "sinomo.path-access", "1.0.0")
                    .cluster_visible()
                    .with_route("/daemon/path/read")
                    .with_provider_capability("path.read"),
            );
        let route = sinomo_core::service::ServiceRoute::new("/daemon/path/read", "daemon");

        let decorated = service_route_with_component_identity(route, &descriptor);
        let discovery = decorated.discovery.expect("discovery identity");

        assert_eq!(discovery.service_id, "path-access");
        assert_eq!(discovery.interface_id, "sinomo.path-access");
        assert_eq!(discovery.provider_capabilities, ["path.read"]);
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

    struct BlockingComponentFixture {
        reconciler: ComponentReconciler,
        release: std::sync::mpsc::Sender<()>,
        started: std::sync::mpsc::Receiver<()>,
        disposed: Arc<AtomicUsize>,
        resource: std::sync::Weak<()>,
    }

    fn blocking_component_fixture(runtime: &tokio::runtime::Handle) -> BlockingComponentFixture {
        let (release, release_rx) = std::sync::mpsc::channel();
        let (started_tx, started) = std::sync::mpsc::channel();
        let disposed = Arc::new(AtomicUsize::new(0));
        let resource = Arc::new(());
        let weak_resource = Arc::downgrade(&resource);
        let factory: Arc<dyn ComponentFactory> = Arc::new(
            StaticComponentFactory::new(
                ComponentDescriptor::new("fixture.blocking-stop", "1.0.0"),
                {
                    let runtime = runtime.clone();
                    let release_rx = std::sync::Mutex::new(Some(release_rx));
                    let disposed = Arc::clone(&disposed);
                    move |_, scope| {
                        let release_rx = release_rx
                            .lock()
                            .expect("release receiver")
                            .take()
                            .expect("component mounts once");
                        let started_tx = started_tx.clone();
                        scope.own_task(
                            "started-blocking-worker",
                            runtime.spawn_blocking(move || {
                                started_tx.send(()).expect("report started worker");
                                // Dropping the sender also releases the worker if an assertion fails.
                                let _ = release_rx.recv();
                            }),
                        );
                        scope.set_services(
                            ServiceRegistryBuilder::default()
                                .register(NamedService {
                                    name: "fixture-blocking-stop",
                                    routes: &["/fixture/blocking-stop"],
                                })
                                .build(),
                        );
                        scope.defer({
                            let disposed = Arc::clone(&disposed);
                            let resource = Arc::clone(&resource);
                            move || {
                                disposed.fetch_add(1, Ordering::SeqCst);
                                drop(resource);
                            }
                        });
                        Ok(())
                    }
                },
            )
            .expect("blocking component factory"),
        );
        let mut reconciler = ComponentReconciler::new(context(), ServiceRegistry::default());
        reconciler
            .reconcile(&[factory])
            .expect("mount blocking component");
        BlockingComponentFixture {
            reconciler,
            release,
            started,
            disposed,
            resource: weak_resource,
        }
    }

    #[test]
    fn stop_tasks_timeout_retains_scope_until_shutdown_disposes_it() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("component test runtime");
        let mut fixture = blocking_component_fixture(runtime.handle());
        fixture
            .started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("spawn_blocking worker actually started");
        runtime.block_on(async {
            let error = fixture
                .reconciler
                .stop_tasks_and_wait(std::time::Duration::ZERO)
                .await
                .expect_err("abort cannot stop a started blocking worker");
            assert!(
                matches!(error, ComponentError::Component { component_id, detail }
                if component_id == "fixture.blocking-stop" && detail.contains("timed out joining"))
            );
            assert!(fixture.reconciler.active.is_empty());
            assert_eq!(
                fixture.reconciler.retiring["fixture.blocking-stop"][0]
                    .tasks
                    .len(),
                1
            );
            assert_eq!(fixture.disposed.load(Ordering::SeqCst), 0);
            assert!(fixture.resource.upgrade().is_some());
            assert!(
                !fixture
                    .reconciler
                    .service_registry()
                    .active_snapshot()
                    .registry()
                    .owns("/fixture/blocking-stop")
            );
            assert!(
                fixture
                    .reconciler
                    .service_registry()
                    .active_snapshot()
                    .host_stopping(),
                "host stop withdraws admission without changing the published offer"
            );

            fixture.release.send(()).expect("release blocking worker");
            fixture
                .reconciler
                .stop_tasks_and_wait(std::time::Duration::from_secs(5))
                .await
                .expect("retry joins the same retained handle");
            assert!(
                fixture.reconciler.retiring["fixture.blocking-stop"][0]
                    .tasks
                    .is_empty()
            );
            assert_eq!(fixture.disposed.load(Ordering::SeqCst), 0);
            assert!(fixture.resource.upgrade().is_some());

            fixture
                .reconciler
                .shutdown_and_wait(std::time::Duration::from_secs(5))
                .await
                .expect("dispose after owned task join");
            assert!(fixture.reconciler.retiring.is_empty());
            assert_eq!(fixture.disposed.load(Ordering::SeqCst), 1);
            assert!(fixture.resource.upgrade().is_none());
            fixture
                .reconciler
                .shutdown_and_wait(std::time::Duration::ZERO)
                .await
                .expect("shutdown is idempotent");
            assert_eq!(fixture.disposed.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn cancelling_stop_join_retains_the_scope_and_original_task_handle() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("component test runtime");
        let mut fixture = blocking_component_fixture(runtime.handle());
        fixture
            .started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("spawn_blocking worker actually started");
        runtime.block_on(async {
            let mut join = Box::pin(
                fixture
                    .reconciler
                    .stop_tasks_and_wait(std::time::Duration::from_secs(60)),
            );
            let first_poll = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(join.as_mut(), cx))
            })
            .await;
            assert!(
                first_poll.is_pending(),
                "started blocking task is still waiting for release"
            );
            drop(join); // External cancellation after the actual join has been polled.
            assert!(fixture.reconciler.active.is_empty());
            assert_eq!(
                fixture.reconciler.retiring["fixture.blocking-stop"][0]
                    .tasks
                    .len(),
                1
            );
            assert_eq!(fixture.disposed.load(Ordering::SeqCst), 0);
            assert!(fixture.resource.upgrade().is_some());

            fixture.release.send(()).expect("release blocking worker");
            fixture
                .reconciler
                .shutdown_and_wait(std::time::Duration::from_secs(5))
                .await
                .expect("join retained task after cancellation and dispose");
            assert_eq!(fixture.disposed.load(Ordering::SeqCst), 1);
            assert!(fixture.resource.upgrade().is_none());
        });
    }

    #[test]
    fn child_drain_timeout_and_cancellation_retain_the_same_scope_until_retry() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let resource = Arc::new(());
        let retained = Arc::downgrade(&resource);
        let disposed = Arc::new(AtomicUsize::new(0));
        let (release, released) = tokio::sync::watch::channel(false);
        let for_factory = Arc::clone(&resource);
        let for_dispose = Arc::clone(&disposed);
        let factory: Arc<dyn ComponentFactory> = Arc::new(
            StaticComponentFactory::new(
                ComponentDescriptor::new("fixture.child-drain", "1.0.0"),
                move |_, scope| {
                    let released = released.clone();
                    scope.on_task_drain("actual child completion", move |_deadline| {
                        let mut released = released.clone();
                        async move {
                            while !*released.borrow_and_update() {
                                released
                                    .changed()
                                    .await
                                    .map_err(|error| error.to_string())?;
                            }
                            Ok(())
                        }
                    });
                    let resource = Arc::clone(&for_factory);
                    let disposed = Arc::clone(&for_dispose);
                    scope.defer(move || {
                        drop(resource);
                        disposed.fetch_add(1, Ordering::SeqCst);
                    });
                    Ok(())
                },
            )
            .unwrap(),
        );
        let mut reconciler = ComponentReconciler::new(context(), ServiceRegistry::default());
        reconciler.reconcile(&[Arc::clone(&factory)]).unwrap();
        drop(factory);
        drop(resource);
        runtime.block_on(async {
            assert!(
                reconciler
                    .stop_tasks_and_wait(std::time::Duration::ZERO)
                    .await
                    .is_err()
            );
            assert_eq!(
                reconciler.retiring["fixture.child-drain"][0]
                    .task_drains
                    .len(),
                1
            );
            assert_eq!(disposed.load(Ordering::SeqCst), 0);
            assert!(retained.upgrade().is_some());
            let mut pending =
                Box::pin(reconciler.stop_tasks_and_wait(std::time::Duration::from_secs(60)));
            let first = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(pending.as_mut(), cx))
            })
            .await;
            assert!(first.is_pending());
            drop(pending);
            assert_eq!(
                reconciler.retiring["fixture.child-drain"][0]
                    .task_drains
                    .len(),
                1
            );
            assert_eq!(disposed.load(Ordering::SeqCst), 0);
            assert!(retained.upgrade().is_some());
            release.send(true).unwrap();
            reconciler
                .shutdown_and_wait(std::time::Duration::from_secs(5))
                .await
                .unwrap();
            assert_eq!(disposed.load(Ordering::SeqCst), 1);
            assert!(retained.upgrade().is_none());
        });
    }

    #[test]
    fn staged_reconcile_withdraws_routes_before_retiring_component_scope() {
        let disposed = Arc::new(AtomicUsize::new(0));
        let factory: Arc<dyn ComponentFactory> = Arc::new(
            StaticComponentFactory::new(ComponentDescriptor::new("fixture.retiring", "1.0.0"), {
                let disposed = Arc::clone(&disposed);
                move |_, scope| {
                    scope.set_services(
                        ServiceRegistryBuilder::default()
                            .register(NamedService {
                                name: "fixture-retiring",
                                routes: &["/fixture/retiring"],
                            })
                            .build(),
                    );
                    scope.defer({
                        let disposed = Arc::clone(&disposed);
                        move || {
                            disposed.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                    Ok(())
                }
            })
            .expect("factory"),
        );
        let mut reconciler = ComponentReconciler::new(context(), ServiceRegistry::default());
        reconciler
            .reconcile(&[factory])
            .expect("activate component");

        let report = reconciler
            .reconcile_staged_with_base_registry(ServiceRegistry::default(), &[])
            .expect("stage retirement");
        assert_eq!(report.stopped, ["fixture.retiring"]);
        assert!(
            !reconciler
                .service_registry()
                .active_snapshot()
                .registry()
                .owns("/fixture/retiring")
        );
        assert_eq!(disposed.load(Ordering::SeqCst), 0);

        reconciler.finish_retiring(&report.stopped);
        assert_eq!(disposed.load(Ordering::SeqCst), 1);
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
