//! Host-agnostic, async service seam for Sinomo's object-RPC control plane.
//!
//! A [`Service`] owns a set of object-RPC routes and serves requests **asynchronously**
//! against a [`ServiceContext`] that carries only neutral, owned inputs — a local data
//! directory, an optional cluster handle, the request signer, and a response sink.
//! Nothing here depends on the daemon, so the same service can be driven two ways:
//!
//! - **In-process / local** (no daemon, no cluster): a caller builds a [`ServiceContext`]
//!   with `cluster: None` and awaits the registry directly. Local-capable services (DFS
//!   local-owned mode, sandbox) run in this mode with no daemon process.
//! - **Over the network**: the daemon authenticates a request, builds a clustered
//!   [`ServiceContext`], and dispatches through the same [`ServiceRegistry`].
//!
//! [`Service::handle`] returns an owned, `Send`, `'static`-capable future and
//! [`ServiceContext`] is owned, so the host can run each request as its own task — a
//! slow or blocking service never stalls another. (Heavy blocking work inside a handler
//! should still use `tokio::task::spawn_blocking` so it doesn't occupy a worker thread.)
//! Routes that need consensus call [`ServiceContext::require_cluster`] and degrade to
//! [`ServiceError::NotClustered`] in local-only mode.

pub mod discovery;
pub use discovery::{
    CLUSTER_SERVICES_KV_PREFIX, ServiceInstanceRecord, ServiceTargetSelector,
    TargetSelectionPolicy, cluster_service_instance_key, cluster_service_prefix,
    discover_service_instances, publish_local_service_instances,
    service_instance_records_from_registry, service_instances_from_entries,
};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use sinomo_kv::{KvEntry, KvError, KvTxn, KvTxnResponse};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    NetworkError, ObjectRpcBodyMode, ObjectRpcBodySource, ObjectRpcIncomingBody, ObjectRpcRequest,
    ObjectRpcResponse, ObjectRpcRouteReadiness, ObjectRpcSessionResponse,
};

/// Whether the host should keep serving after a request (`Stop` == daemon shutdown).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceDisposition {
    Continue,
    Stop,
}

/// Origin of a request: `Local` for in-process callers, `Remote` for an authenticated
/// peer reached over the network.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceSigner {
    Local,
    LocalNode { node_id: String, public_key: String },
    Remote { node_id: String, public_key: String },
}

impl ServiceSigner {
    pub fn is_local(&self) -> bool {
        matches!(self, ServiceSigner::Local | ServiceSigner::LocalNode { .. })
    }

    pub fn cluster_identity(&self) -> Option<(&str, &str)> {
        match self {
            ServiceSigner::Local => None,
            ServiceSigner::LocalNode {
                node_id,
                public_key,
            }
            | ServiceSigner::Remote {
                node_id,
                public_key,
            } => Some((node_id.as_str(), public_key.as_str())),
        }
    }
}

/// Error surface for service handlers — independent of any host (daemon) error type.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// The route requires an initialized cluster, which isn't available here.
    #[error("service route `{0}` requires an initialized cluster")]
    NotClustered(String),
    /// No registered service owns the requested route.
    #[error("unknown service route: {0}")]
    UnknownRoute(String),
    #[error(transparent)]
    Network(#[from] NetworkError),
    #[error(transparent)]
    Kv(#[from] KvError),
    /// A service-specific failure carried as a message (services map domain errors here).
    #[error("{0}")]
    Service(String),
}

/// Read/commit access to cluster state. Shared (`Arc`) across concurrent requests, so
/// methods take `&self`; the implementation serializes writes internally. Present only
/// when clustered — [`ServiceContext::cluster`] is `None` in local-only mode.
pub trait ClusterAccess: Send + Sync {
    /// Read a single cluster-KV entry.
    fn kv_get(&self, key: &str) -> Result<Option<KvEntry>, ServiceError>;
    /// Read all cluster-KV entries under a key prefix.
    fn kv_range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, ServiceError>;
    /// Commit a cluster-KV transaction through the host-owned async runtime.
    fn kv_commit_async<'a>(
        &'a self,
        txn: KvTxn,
    ) -> Pin<Box<dyn Future<Output = Result<KvTxnResponse, ServiceError>> + Send + 'a>>;
}

/// Where the response to one service request goes.
///
/// The sink is consumed by [`ResponseSink::send`], making response delivery exactly
/// once. The host adapter decides the destination (a network session, stdout, an
/// in-memory collector, …), while `sinomo-network` owns body framing and transfer.
pub trait ResponseSink: Send {
    fn send(self: Box<Self>, response: ObjectRpcSessionResponse) -> Result<(), ServiceError>;
}

/// Everything a service needs to serve one request — owned and `'static`, with no daemon
/// or cluster baked in, so a handler future can be spawned as its own task.
pub struct ServiceContext {
    /// Local data directory — always present; local-capable services use only this.
    pub data_dir: Arc<SinomoDataDir>,
    /// Cluster access when clustered; `None` ⇒ local-only mode (limited functionality).
    pub cluster: Option<Arc<dyn ClusterAccess>>,
    /// Who originated the request.
    pub signer: ServiceSigner,
    request_body: ObjectRpcIncomingBody,
    response_sink: Option<Box<dyn ResponseSink>>,
}

impl ServiceContext {
    pub fn new(
        data_dir: Arc<SinomoDataDir>,
        cluster: Option<Arc<dyn ClusterAccess>>,
        signer: ServiceSigner,
        request_body: ObjectRpcIncomingBody,
        response_sink: Box<dyn ResponseSink>,
    ) -> Self {
        Self {
            data_dir,
            cluster,
            signer,
            request_body,
            response_sink: Some(response_sink),
        }
    }

    /// Cluster handle for a route that requires consensus, or
    /// [`ServiceError::NotClustered`] when running without a cluster.
    pub fn require_cluster(&self, route: &str) -> Result<Arc<dyn ClusterAccess>, ServiceError> {
        self.cluster
            .clone()
            .ok_or_else(|| ServiceError::NotClustered(route.to_string()))
    }

    /// Takes ownership of the optional request body attachment.
    ///
    /// A second call returns an empty body. Services choose a high-level sink or
    /// reader; transport framing and chunk sizes stay inside `sinomo-network`.
    pub fn take_request_body(&mut self) -> ObjectRpcIncomingBody {
        std::mem::take(&mut self.request_body)
    }

    /// Send a response without a body attachment.
    pub fn respond(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
        self.respond_with_body(response, ObjectRpcBodySource::default())
    }

    /// Send a response with an optional body source managed by `sinomo-network`.
    pub fn respond_with_body(
        &mut self,
        response: ObjectRpcResponse,
        body: ObjectRpcBodySource,
    ) -> Result<(), ServiceError> {
        let sink = self.response_sink.take().ok_or_else(|| {
            ServiceError::Service("service response was already sent".to_string())
        })?;
        sink.send(ObjectRpcSessionResponse { response, body })
    }
}

/// The future returned by [`Service::handle`] and [`ServiceRegistry::dispatch`].
pub type ServiceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ServiceDisposition, ServiceError>> + Send + 'a>>;

/// A host-agnostic control-plane service: owns object-RPC routes and serves requests
/// asynchronously. Implementors return `Box::pin(async move { … })`.
pub trait Service: Send + Sync {
    /// Stable service name (diagnostics / catalog).
    fn name(&self) -> &'static str;
    /// The object-RPC routes this service owns (matched against `ObjectRpcRequest::route`).
    fn routes(&self) -> &'static [&'static str];
    /// The object-RPC route/category pairs this service owns.
    ///
    /// The default maps every route to [`Service::name`]. Services whose diagnostic name
    /// differs from their wire category should override this.
    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| ServiceRoute::new(route, self.name()))
            .collect()
    }
    /// Serve a request whose route this service owns, writing the reply via `ctx.respond*`,
    /// and report whether the host should keep running.
    fn handle(&self, ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_>;
}

pub type ServiceBodyMode = ObjectRpcBodyMode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceRoute {
    pub route: String,
    pub category: String,
    pub auth_scope: Option<String>,
    pub required_capabilities: Vec<String>,
    pub concurrency_limit: Option<usize>,
    pub timeout_ms: Option<u64>,
    pub request_body_mode: ServiceBodyMode,
    pub response_body_mode: ServiceBodyMode,
}

impl ServiceRoute {
    pub fn new(route: impl Into<String>, category: impl Into<String>) -> Self {
        Self {
            route: route.into(),
            category: category.into(),
            auth_scope: None,
            required_capabilities: Vec::new(),
            concurrency_limit: None,
            timeout_ms: None,
            request_body_mode: ServiceBodyMode::None,
            response_body_mode: ServiceBodyMode::None,
        }
    }

    pub fn from_route_metadata<I, S>(
        route: impl Into<String>,
        category: impl Into<String>,
        auth_scope: impl Into<String>,
        required_capabilities: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(route, category)
            .with_auth_scope(auth_scope)
            .require_capabilities(required_capabilities)
    }

    pub fn with_auth_scope(mut self, scope: impl Into<String>) -> Self {
        self.auth_scope = Some(scope.into());
        self
    }

    pub fn require_capability(mut self, capability: impl Into<String>) -> Self {
        self.required_capabilities.push(capability.into());
        self
    }

    pub fn require_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_capabilities
            .extend(capabilities.into_iter().map(Into::into));
        self
    }

    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = Some(limit);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout_ms = Some(timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        self
    }

    pub fn with_request_body_mode(mut self, mode: ServiceBodyMode) -> Self {
        self.request_body_mode = mode;
        self
    }

    pub fn with_response_body_mode(mut self, mode: ServiceBodyMode) -> Self {
        self.response_body_mode = mode;
        self
    }
}

#[derive(Clone)]
struct RouteOwner {
    service_index: usize,
    category: String,
    auth_scope: Option<String>,
    required_capabilities: Vec<String>,
    concurrency_limit: Option<usize>,
    timeout_ms: Option<u64>,
    request_body_mode: ServiceBodyMode,
    response_body_mode: ServiceBodyMode,
}

/// Dispatches object-RPC requests to the service that owns the request's route and
/// category. The returned future is `'static`, so the host can spawn each request
/// independently and services never block one another.
#[derive(Default, Clone)]
pub struct ServiceRegistry {
    services: Vec<Arc<dyn Service>>,
    by_route: BTreeMap<String, RouteOwner>,
}

impl ServiceRegistry {
    pub fn builder() -> ServiceRegistryBuilder {
        ServiceRegistryBuilder::default()
    }

    /// Route `request` to its owning service, returning a spawnable `'static` future.
    /// Unknown routes resolve to [`ServiceError::UnknownRoute`]; known routes with a
    /// mismatched category fail before invoking the handler. Generic route policy
    /// such as timeout and concurrency is metadata for the host object-RPC Tower
    /// layer; direct registry dispatch invokes the handler without owning that
    /// policy itself.
    pub fn dispatch(
        &self,
        ctx: ServiceContext,
        request: ObjectRpcRequest,
    ) -> ServiceFuture<'static> {
        self.dispatch_direct(ctx, request)
    }

    /// Route `request` to its owning service without adding another policy layer.
    /// Network-owned object-RPC Tower middleware is the single owner for generic
    /// route timeout and concurrency policy.
    pub fn dispatch_direct(
        &self,
        ctx: ServiceContext,
        request: ObjectRpcRequest,
    ) -> ServiceFuture<'static> {
        match self.by_route.get(request.route.as_str()) {
            Some(owner) if request.category == owner.category => {
                let service = Arc::clone(&self.services[owner.service_index]);
                Box::pin(async move { service.handle(ctx, request).await })
            }
            Some(owner) => {
                let route = request.route.clone();
                let expected = owner.category.to_string();
                let actual = request.category;
                Box::pin(async move {
                    Err(ServiceError::Network(NetworkError::Serialization {
                        detail: format!(
                            "service route {route} expected category {expected}, got {actual}"
                        ),
                    }))
                })
            }
            None => Box::pin(async move { Err(ServiceError::UnknownRoute(request.route)) }),
        }
    }

    /// Names of the registered services, in registration order.
    pub fn service_names(&self) -> Vec<&'static str> {
        self.services.iter().map(|service| service.name()).collect()
    }

    /// Number of distinct routes served.
    pub fn route_count(&self) -> usize {
        self.by_route.len()
    }

    /// Route/category pairs owned by this registry, sorted by route.
    pub fn route_categories(&self) -> Vec<ServiceRoute> {
        self.by_route
            .iter()
            .map(|(route, owner)| ServiceRoute {
                route: route.clone(),
                category: owner.category.clone(),
                auth_scope: owner.auth_scope.clone(),
                required_capabilities: owner.required_capabilities.clone(),
                concurrency_limit: owner.concurrency_limit,
                timeout_ms: owner.timeout_ms,
                request_body_mode: owner.request_body_mode,
                response_body_mode: owner.response_body_mode,
            })
            .collect()
    }

    pub fn route_readiness(&self, route: &str) -> Option<ObjectRpcRouteReadiness> {
        let owner = self.by_route.get(route)?;
        Some(ObjectRpcRouteReadiness {
            route: route.to_string(),
            category: owner.category.clone(),
            concurrency_limit: owner.concurrency_limit,
            available_concurrency: None,
            timeout_ms: owner.timeout_ms,
        })
    }

    pub fn category_for_route(&self, route: &str) -> Option<&str> {
        self.by_route
            .get(route)
            .map(|owner| owner.category.as_str())
    }

    /// Whether any registered service owns `route`. Lets a host dispatch registry-owned
    /// routes and fall through to its own handling for the rest.
    pub fn owns(&self, route: &str) -> bool {
        self.by_route.contains_key(route)
    }
}

#[derive(Clone)]
pub struct ServiceRegistrySnapshot {
    version: u64,
    registry: Arc<ServiceRegistry>,
}

impl std::fmt::Debug for ServiceRegistrySnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceRegistrySnapshot")
            .field("version", &self.version)
            .field("service_names", &self.registry.service_names())
            .field("route_count", &self.registry.route_count())
            .finish()
    }
}

impl ServiceRegistrySnapshot {
    fn new(version: u64, registry: ServiceRegistry) -> Self {
        Self {
            version,
            registry: Arc::new(registry),
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn registry(&self) -> &ServiceRegistry {
        &self.registry
    }

    pub fn registry_arc(&self) -> Arc<ServiceRegistry> {
        Arc::clone(&self.registry)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceRegistrySnapshotError {
    #[error("service registry snapshot lock failed: {detail}")]
    SnapshotLock { detail: String },
}

#[derive(Clone)]
pub struct ServiceRegistrySnapshotStore {
    active: Arc<ArcSwap<ServiceRegistrySnapshot>>,
    update_lock: Arc<Mutex<()>>,
}

impl std::fmt::Debug for ServiceRegistrySnapshotStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceRegistrySnapshotStore")
            .field("active_version", &self.active.load().version())
            .finish_non_exhaustive()
    }
}

impl Default for ServiceRegistrySnapshotStore {
    fn default() -> Self {
        Self::new(ServiceRegistry::default())
    }
}

impl ServiceRegistrySnapshotStore {
    pub fn new(registry: ServiceRegistry) -> Self {
        let snapshot = Arc::new(ServiceRegistrySnapshot::new(1, registry));
        Self {
            active: Arc::new(ArcSwap::from(snapshot)),
            update_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn active_snapshot(&self) -> Arc<ServiceRegistrySnapshot> {
        self.active.load_full()
    }

    pub fn replace_registry(
        &self,
        registry: ServiceRegistry,
    ) -> Result<Arc<ServiceRegistrySnapshot>, ServiceRegistrySnapshotError> {
        let _guard = self.update_lock.lock().map_err(|source| {
            ServiceRegistrySnapshotError::SnapshotLock {
                detail: source.to_string(),
            }
        })?;
        let active = self.active.load();
        let snapshot = Arc::new(ServiceRegistrySnapshot::new(
            active.version().saturating_add(1),
            registry,
        ));
        self.active.store(Arc::clone(&snapshot));
        Ok(snapshot)
    }
}

/// Builds a [`ServiceRegistry`], rejecting duplicate route ownership.
#[derive(Default)]
pub struct ServiceRegistryBuilder {
    services: Vec<Arc<dyn Service>>,
    by_route: BTreeMap<String, RouteOwner>,
}

impl ServiceRegistryBuilder {
    /// Register a service, mapping each of its route/category pairs to it.
    ///
    /// # Panics
    /// Panics if two services claim the same route — a construction-time wiring bug.
    pub fn register(mut self, service: impl Service + 'static) -> Self {
        let index = self.services.len();
        let service_routes = validate_service_route_categories(&service);
        for route in service_routes {
            let concurrency_limit = route.concurrency_limit;
            if let Some(previous) = self.by_route.insert(
                route.route.clone(),
                RouteOwner {
                    service_index: index,
                    category: route.category,
                    auth_scope: route.auth_scope,
                    required_capabilities: route.required_capabilities,
                    concurrency_limit,
                    timeout_ms: route.timeout_ms,
                    request_body_mode: route.request_body_mode,
                    response_body_mode: route.response_body_mode,
                },
            ) {
                panic!(
                    "service route `{}` is claimed by both `{}` and `{}`",
                    route.route,
                    self.services[previous.service_index].name(),
                    service.name()
                );
            }
        }
        self.services.push(Arc::new(service));
        self
    }

    pub fn build(self) -> ServiceRegistry {
        ServiceRegistry {
            services: self.services,
            by_route: self.by_route,
        }
    }
}

fn validate_service_route_categories(service: &impl Service) -> Vec<ServiceRoute> {
    let mut declared_routes: BTreeSet<&'static str> = service.routes().iter().copied().collect();
    let mut seen_routes = BTreeSet::new();
    let service_routes = service.route_categories();
    for route in &service_routes {
        assert!(
            !route.route.trim().is_empty(),
            "service `{}` declared an empty route",
            service.name()
        );
        assert!(
            !route.category.trim().is_empty(),
            "service `{}` route `{}` declared an empty category",
            service.name(),
            route.route
        );
        assert!(
            seen_routes.insert(route.route.as_str()),
            "service `{}` declared route `{}` more than once",
            service.name(),
            route.route
        );
        assert!(
            !matches!(route.concurrency_limit, Some(0)),
            "service `{}` route `{}` declared a zero concurrency limit",
            service.name(),
            route.route
        );
        assert!(
            !matches!(route.timeout_ms, Some(0)),
            "service `{}` route `{}` declared a zero timeout",
            service.name(),
            route.route
        );
        assert!(
            declared_routes.remove(route.route.as_str()),
            "service `{}` route category for `{}` is not listed in routes()",
            service.name(),
            route.route
        );
    }
    assert!(
        declared_routes.is_empty(),
        "service `{}` routes() entries missing route categories: {:?}",
        service.name(),
        declared_routes
    );
    service_routes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EchoService;

    impl Service for EchoService {
        fn name(&self) -> &'static str {
            "echo"
        }

        fn routes(&self) -> &'static [&'static str] {
            &["/test/echo"]
        }

        fn handle(&self, mut ctx: ServiceContext, _request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async move {
                let response = ObjectRpcResponse::encode_body("/test/echo", &"ok".to_string())?;
                ctx.respond(response)?;
                Ok(ServiceDisposition::Continue)
            })
        }
    }

    #[derive(Clone, Default)]
    struct CollectingSink {
        frames: Arc<Mutex<Vec<ObjectRpcSessionResponse>>>,
    }

    impl ResponseSink for CollectingSink {
        fn send(self: Box<Self>, response: ObjectRpcSessionResponse) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    fn test_data_dir() -> Arc<SinomoDataDir> {
        let root = sinomo_testdir::TestDir::new("sinomo-service-seam-test");
        Arc::new(SinomoDataDir::new(&root))
    }

    fn local_context(sink: CollectingSink) -> ServiceContext {
        ServiceContext::new(
            test_data_dir(),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::default(),
            Box::new(sink),
        )
    }

    #[tokio::test]
    async fn registry_dispatches_known_route_and_rejects_unknown() {
        let registry = ServiceRegistry::builder().register(EchoService).build();
        assert_eq!(registry.route_count(), 1);
        assert_eq!(registry.service_names(), vec!["echo"]);
        assert_eq!(registry.category_for_route("/test/echo"), Some("echo"));
        assert_eq!(
            registry.route_categories(),
            vec![ServiceRoute::new("/test/echo", "echo")]
        );

        let sink = CollectingSink::default();
        let frames = Arc::clone(&sink.frames);
        let request = ObjectRpcRequest::encode_body("/test/echo", "echo", &()).expect("encode");
        let disposition = registry
            .dispatch(local_context(sink), request)
            .await
            .expect("dispatch");
        assert_eq!(disposition, ServiceDisposition::Continue);
        assert_eq!(frames.lock().expect("frames").len(), 1);

        let unknown =
            ObjectRpcRequest::encode_body("/test/missing", "test.category", &()).expect("encode");
        let error = registry
            .dispatch(local_context(CollectingSink::default()), unknown)
            .await
            .expect_err("unknown route");
        assert!(matches!(error, ServiceError::UnknownRoute(route) if route == "/test/missing"));
    }

    #[tokio::test]
    async fn registry_rejects_category_mismatch_before_handler() {
        let registry = ServiceRegistry::builder().register(EchoService).build();
        let sink = CollectingSink::default();
        let frames = Arc::clone(&sink.frames);
        let request =
            ObjectRpcRequest::encode_body("/test/echo", "wrong.category", &()).expect("encode");

        let error = registry
            .dispatch(local_context(sink), request)
            .await
            .expect_err("category mismatch");

        assert!(
            matches!(error, ServiceError::Network(NetworkError::Serialization { ref detail })
                if detail.contains("expected category echo")),
            "unexpected error: {error}"
        );
        assert!(
            frames.lock().expect("frames").is_empty(),
            "handler should not emit a response when route category validation fails"
        );
    }

    #[tokio::test]
    async fn registry_dispatches_response_body_through_the_same_sink() {
        struct BodyService;

        impl Service for BodyService {
            fn name(&self) -> &'static str {
                "body"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/body"]
            }

            fn route_categories(&self) -> Vec<ServiceRoute> {
                vec![
                    ServiceRoute::new("/test/body", "body")
                        .with_response_body_mode(ServiceBodyMode::Optional),
                ]
            }

            fn handle(
                &self,
                mut ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                Box::pin(async move {
                    ctx.respond_with_body(
                        ObjectRpcResponse::encode_body("/test/body", &"metadata".to_string())?,
                        ObjectRpcBodySource::bytes(b"stream-body".to_vec()),
                    )?;
                    Ok(ServiceDisposition::Continue)
                })
            }
        }

        let registry = ServiceRegistry::builder().register(BodyService).build();
        let sink = CollectingSink::default();
        let frames = Arc::clone(&sink.frames);
        let request = ObjectRpcRequest::encode_body("/test/body", "body", &()).expect("encode");
        registry
            .dispatch(local_context(sink), request)
            .await
            .expect("dispatch");
        let session_response = frames.lock().expect("frames").pop().expect("response");

        assert_eq!(
            session_response
                .response
                .decode_body::<String>()
                .expect("metadata"),
            "metadata"
        );
        assert!(!session_response.body.is_empty());
        assert_eq!(session_response.body.total_bytes(), Some(11));
        assert_eq!(
            registry.route_categories(),
            vec![
                ServiceRoute::new("/test/body", "body")
                    .with_response_body_mode(ServiceBodyMode::Optional)
            ]
        );
    }

    #[tokio::test]
    async fn service_context_sends_at_most_one_response() {
        struct DoubleResponseService;

        impl Service for DoubleResponseService {
            fn name(&self) -> &'static str {
                "double-response"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/double-response"]
            }

            fn handle(
                &self,
                mut ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                Box::pin(async move {
                    ctx.respond(ObjectRpcResponse::encode_body(
                        "/test/double-response",
                        &"first",
                    )?)?;
                    let error = ctx
                        .respond(ObjectRpcResponse::encode_body(
                            "/test/double-response",
                            &"second",
                        )?)
                        .expect_err("second response should be rejected");
                    assert!(error.to_string().contains("already sent"));
                    Ok(ServiceDisposition::Continue)
                })
            }
        }

        let registry = ServiceRegistry::builder()
            .register(DoubleResponseService)
            .build();
        let sink = CollectingSink::default();
        let frames = Arc::clone(&sink.frames);
        let request =
            ObjectRpcRequest::encode_body("/test/double-response", "double-response", &())
                .expect("encode");

        registry
            .dispatch(local_context(sink), request)
            .await
            .expect("dispatch");

        assert_eq!(frames.lock().expect("frames").len(), 1);
    }

    #[tokio::test]
    async fn service_context_request_body_is_taken_once() {
        let mut ctx = local_context(CollectingSink::default());

        assert!(
            ctx.take_request_body()
                .collect_to_vec(1)
                .await
                .expect("first request body")
                .is_empty()
        );
        assert!(
            ctx.take_request_body()
                .collect_to_vec(1)
                .await
                .expect("replacement request body")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn registry_accepts_service_route_category_override() {
        struct DiagnosticNameService;

        impl Service for DiagnosticNameService {
            fn name(&self) -> &'static str {
                "diagnostic-name"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/wire-category"]
            }

            fn route_categories(&self) -> Vec<ServiceRoute> {
                vec![ServiceRoute::new("/test/wire-category", "wire.category")]
            }

            fn handle(
                &self,
                mut ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                Box::pin(async move {
                    let response =
                        ObjectRpcResponse::encode_body("/test/wire-category", &"ok".to_string())?;
                    ctx.respond(response)?;
                    Ok(ServiceDisposition::Continue)
                })
            }
        }

        let registry = ServiceRegistry::builder()
            .register(DiagnosticNameService)
            .build();
        assert_eq!(
            registry.category_for_route("/test/wire-category"),
            Some("wire.category")
        );

        let sink = CollectingSink::default();
        let frames = Arc::clone(&sink.frames);
        let request = ObjectRpcRequest::encode_body("/test/wire-category", "wire.category", &())
            .expect("encode");
        registry
            .dispatch(local_context(sink), request)
            .await
            .expect("dispatch");

        assert_eq!(frames.lock().expect("frames").len(), 1);
    }

    #[test]
    fn service_route_accepts_bulk_required_capabilities() {
        assert_eq!(
            ServiceRoute::from_route_metadata(
                "/test/module",
                "module",
                "admin",
                ["module.read", "module.write"],
            ),
            ServiceRoute::new("/test/module", "module")
                .with_auth_scope("admin")
                .require_capability("module.read")
                .require_capability("module.write")
        );
    }

    #[tokio::test]
    async fn registry_preserves_route_policy_metadata() {
        struct PolicyService;

        impl Service for PolicyService {
            fn name(&self) -> &'static str {
                "policy"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/policy"]
            }

            fn route_categories(&self) -> Vec<ServiceRoute> {
                vec![
                    ServiceRoute::new("/test/policy", "policy")
                        .with_auth_scope("admin")
                        .require_capability("policy.write")
                        .with_concurrency_limit(4)
                        .with_timeout(Duration::from_millis(250))
                        .with_request_body_mode(ServiceBodyMode::Required)
                        .with_response_body_mode(ServiceBodyMode::Optional),
                ]
            }

            fn handle(
                &self,
                mut ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                Box::pin(async move {
                    let response =
                        ObjectRpcResponse::encode_body("/test/policy", &"ok".to_string())?;
                    ctx.respond(response)?;
                    Ok(ServiceDisposition::Continue)
                })
            }
        }

        let registry = ServiceRegistry::builder().register(PolicyService).build();

        assert_eq!(
            registry.route_categories(),
            vec![
                ServiceRoute::new("/test/policy", "policy")
                    .with_auth_scope("admin")
                    .require_capability("policy.write")
                    .with_concurrency_limit(4)
                    .with_timeout(Duration::from_millis(250))
                    .with_request_body_mode(ServiceBodyMode::Required)
                    .with_response_body_mode(ServiceBodyMode::Optional)
            ]
        );
    }

    #[tokio::test]
    async fn registry_snapshot_store_swaps_active_registry_without_invalidating_old_snapshots() {
        struct ReplacementService;

        impl Service for ReplacementService {
            fn name(&self) -> &'static str {
                "replacement"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/replacement"]
            }

            fn handle(
                &self,
                mut ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                Box::pin(async move {
                    let response = ObjectRpcResponse::encode_body(
                        "/test/replacement",
                        &"replacement-ok".to_string(),
                    )?;
                    ctx.respond(response)?;
                    Ok(ServiceDisposition::Continue)
                })
            }
        }

        let store = ServiceRegistrySnapshotStore::new(
            ServiceRegistry::builder().register(EchoService).build(),
        );
        let old_snapshot = store.active_snapshot();
        assert_eq!(old_snapshot.version(), 1);
        assert_eq!(
            old_snapshot.registry().category_for_route("/test/echo"),
            Some("echo")
        );

        let new_snapshot = store
            .replace_registry(
                ServiceRegistry::builder()
                    .register(ReplacementService)
                    .build(),
            )
            .expect("replace registry");
        assert_eq!(new_snapshot.version(), 2);
        assert_eq!(store.active_snapshot().version(), 2);
        assert_eq!(
            store
                .active_snapshot()
                .registry()
                .category_for_route("/test/replacement"),
            Some("replacement")
        );

        let old_sink = CollectingSink::default();
        let old_frames = Arc::clone(&old_sink.frames);
        let old_request = ObjectRpcRequest::encode_body("/test/echo", "echo", &()).expect("old");
        old_snapshot
            .registry_arc()
            .dispatch(local_context(old_sink), old_request)
            .await
            .expect("old snapshot dispatch");
        assert_eq!(old_frames.lock().expect("old frames").len(), 1);

        let active_sink = CollectingSink::default();
        let active_frames = Arc::clone(&active_sink.frames);
        let active_request =
            ObjectRpcRequest::encode_body("/test/replacement", "replacement", &()).expect("active");
        store
            .active_snapshot()
            .registry_arc()
            .dispatch(local_context(active_sink), active_request)
            .await
            .expect("active snapshot dispatch");
        assert_eq!(active_frames.lock().expect("active frames").len(), 1);
    }

    #[tokio::test]
    async fn registry_dispatch_preserves_policy_metadata_without_owning_policy() {
        struct LimitedService {
            active: Arc<AtomicUsize>,
            max_active: Arc<AtomicUsize>,
            both_entered: Arc<tokio::sync::Notify>,
            release: tokio::sync::watch::Receiver<bool>,
        }

        impl Service for LimitedService {
            fn name(&self) -> &'static str {
                "limited"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/limited"]
            }

            fn route_categories(&self) -> Vec<ServiceRoute> {
                vec![
                    ServiceRoute::new("/test/limited", "limited")
                        .with_concurrency_limit(1)
                        .with_timeout(Duration::from_millis(250)),
                ]
            }

            fn handle(
                &self,
                mut ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                let active = Arc::clone(&self.active);
                let max_active = Arc::clone(&self.max_active);
                let both_entered = Arc::clone(&self.both_entered);
                let mut release = self.release.clone();
                Box::pin(async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    if current >= 2 {
                        both_entered.notify_one();
                    }
                    while !*release.borrow() {
                        release.changed().await.map_err(|_| {
                            ServiceError::Service("release signal closed".to_string())
                        })?;
                    }
                    active.fetch_sub(1, Ordering::SeqCst);
                    let response = ObjectRpcResponse::encode_body("/test/limited", &"ok")?;
                    ctx.respond(response)?;
                    Ok(ServiceDisposition::Continue)
                })
            }
        }

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let both_entered = Arc::new(tokio::sync::Notify::new());
        let (release_tx, release_rx) = tokio::sync::watch::channel(false);
        let registry = Arc::new(
            ServiceRegistry::builder()
                .register(LimitedService {
                    active: Arc::clone(&active),
                    max_active: Arc::clone(&max_active),
                    both_entered: Arc::clone(&both_entered),
                    release: release_rx,
                })
                .build(),
        );
        let initial = registry
            .route_readiness("/test/limited")
            .expect("initial readiness");
        assert_eq!(initial.concurrency_limit, Some(1));
        assert_eq!(initial.available_concurrency, None);
        assert_eq!(initial.timeout_ms, Some(250));
        assert!(initial.is_ready());

        let first_request =
            ObjectRpcRequest::encode_body("/test/limited", "limited", &()).expect("first request");
        let first = tokio::spawn({
            let registry = Arc::clone(&registry);
            async move {
                registry
                    .dispatch(local_context(CollectingSink::default()), first_request)
                    .await
            }
        });

        let second_request =
            ObjectRpcRequest::encode_body("/test/limited", "limited", &()).expect("second request");
        let second = tokio::spawn({
            let registry = Arc::clone(&registry);
            async move {
                registry
                    .dispatch(local_context(CollectingSink::default()), second_request)
                    .await
            }
        });

        both_entered.notified().await;
        let in_flight = registry
            .route_readiness("/test/limited")
            .expect("in-flight readiness");
        assert_eq!(in_flight.available_concurrency, None);
        assert!(in_flight.is_ready());

        release_tx.send(true).expect("release handlers");
        first.await.expect("first task").expect("first dispatch");
        second.await.expect("second task").expect("second dispatch");

        assert_eq!(max_active.load(Ordering::SeqCst), 2);
    }

    #[test]
    #[should_panic(expected = "declared a zero concurrency limit")]
    fn registry_rejects_zero_service_route_concurrency_limit() {
        struct ZeroLimitService;

        impl Service for ZeroLimitService {
            fn name(&self) -> &'static str {
                "zero-limit"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/zero-limit"]
            }

            fn route_categories(&self) -> Vec<ServiceRoute> {
                vec![ServiceRoute::new("/test/zero-limit", "zero-limit").with_concurrency_limit(0)]
            }

            fn handle(
                &self,
                _ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                Box::pin(async { Ok(ServiceDisposition::Continue) })
            }
        }

        let _ = ServiceRegistry::builder()
            .register(ZeroLimitService)
            .build();
    }

    #[test]
    #[should_panic(expected = "declared a zero timeout")]
    fn registry_rejects_zero_service_route_timeout() {
        struct ZeroTimeoutService;

        impl Service for ZeroTimeoutService {
            fn name(&self) -> &'static str {
                "zero-timeout"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/zero-timeout"]
            }

            fn route_categories(&self) -> Vec<ServiceRoute> {
                vec![
                    ServiceRoute::new("/test/zero-timeout", "zero-timeout")
                        .with_timeout(Duration::ZERO),
                ]
            }

            fn handle(
                &self,
                _ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                Box::pin(async { Ok(ServiceDisposition::Continue) })
            }
        }

        let _ = ServiceRegistry::builder()
            .register(ZeroTimeoutService)
            .build();
    }

    #[tokio::test]
    async fn require_cluster_errors_in_local_only_mode() {
        // `require_cluster`'s Ok type is a trait object (not Debug), so match it.
        let ctx = local_context(CollectingSink::default());
        let error = match ctx.require_cluster("/cluster/kv/txn") {
            Ok(_) => panic!("expected NotClustered without a cluster"),
            Err(error) => error,
        };
        assert!(matches!(error, ServiceError::NotClustered(route) if route == "/cluster/kv/txn"));
    }

    /// A slow service must not delay a fast one — they dispatch as independent futures.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn services_do_not_block_each_other() {
        use std::time::{Duration, Instant};

        struct SlowService;
        impl Service for SlowService {
            fn name(&self) -> &'static str {
                "slow"
            }
            fn routes(&self) -> &'static [&'static str] {
                &["/test/slow"]
            }
            fn handle(
                &self,
                mut ctx: ServiceContext,
                _request: ObjectRpcRequest,
            ) -> ServiceFuture<'_> {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let response =
                        ObjectRpcResponse::encode_body("/test/slow", &"slow".to_string())?;
                    ctx.respond(response)?;
                    Ok(ServiceDisposition::Continue)
                })
            }
        }

        let registry = Arc::new(
            ServiceRegistry::builder()
                .register(EchoService)
                .register(SlowService)
                .build(),
        );

        let slow_request =
            ObjectRpcRequest::encode_body("/test/slow", "slow", &()).expect("encode");
        let slow = tokio::spawn({
            let registry = Arc::clone(&registry);
            async move {
                registry
                    .dispatch(local_context(CollectingSink::default()), slow_request)
                    .await
            }
        });

        let fast_request =
            ObjectRpcRequest::encode_body("/test/echo", "echo", &()).expect("encode");
        let started = Instant::now();
        registry
            .dispatch(local_context(CollectingSink::default()), fast_request)
            .await
            .expect("fast dispatch");
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "fast service was blocked by the slow one ({:?})",
            started.elapsed()
        );

        slow.await.expect("join slow").expect("slow dispatch");
    }
}
