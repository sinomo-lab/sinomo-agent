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

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use sinomo_kv::{KvEntry, KvError, KvTxn, KvTxnResponse};
use sinomo_local::SinomoDataDir;
use sinomo_network::{NetworkError, ObjectRpcRequest, ObjectRpcResponse};

mod dfs;
mod rollout;
mod sandbox;
pub use dfs::DfsService;
pub use rollout::RolloutService;
pub use sandbox::{LocalSandboxBackendFactory, SandboxBackendFactory, SandboxService};

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
    Remote { node_id: String, public_key: String },
}

impl ServiceSigner {
    pub fn is_local(&self) -> bool {
        matches!(self, ServiceSigner::Local)
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
    /// Commit a cluster-KV transaction (quorum-replicated by the host).
    fn kv_commit(&self, txn: KvTxn) -> Result<KvTxnResponse, ServiceError>;
}

/// Where a service's response frame(s) go: one [`ResponseSink::emit`] for a unary reply,
/// several for a streamed reply. Owned per request, so the host adapter decides the
/// destination (network stream, stdout, an in-memory buffer, …).
pub trait ResponseSink: Send {
    fn emit(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError>;
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
    /// Where response frames are written.
    pub sink: Box<dyn ResponseSink>,
}

impl ServiceContext {
    /// Cluster handle for a route that requires consensus, or
    /// [`ServiceError::NotClustered`] when running without a cluster.
    pub fn require_cluster(&self, route: &str) -> Result<Arc<dyn ClusterAccess>, ServiceError> {
        self.cluster
            .clone()
            .ok_or_else(|| ServiceError::NotClustered(route.to_string()))
    }

    /// Emit a single unary response frame.
    pub fn respond(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
        self.sink.emit(response)
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
    /// Serve a request whose route this service owns, writing the reply via `ctx.sink`,
    /// and report whether the host should keep running.
    fn handle(&self, ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_>;
}

/// Dispatches object-RPC requests to the service that owns the request's route. The
/// returned future is `'static`, so the host can spawn each request independently and
/// services never block one another.
#[derive(Default, Clone)]
pub struct ServiceRegistry {
    services: Vec<Arc<dyn Service>>,
    by_route: BTreeMap<&'static str, usize>,
}

impl ServiceRegistry {
    pub fn builder() -> ServiceRegistryBuilder {
        ServiceRegistryBuilder::default()
    }

    /// Route `request` to its owning service, returning a spawnable `'static` future.
    /// Unknown routes resolve to [`ServiceError::UnknownRoute`].
    pub fn dispatch(
        &self,
        ctx: ServiceContext,
        request: ObjectRpcRequest,
    ) -> ServiceFuture<'static> {
        match self.by_route.get(request.route.as_str()) {
            Some(&index) => {
                let service = Arc::clone(&self.services[index]);
                Box::pin(async move { service.handle(ctx, request).await })
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

    /// Whether any registered service owns `route`. Lets a host dispatch registry-owned
    /// routes and fall through to its own handling for the rest.
    pub fn owns(&self, route: &str) -> bool {
        self.by_route.contains_key(route)
    }
}

/// Builds a [`ServiceRegistry`], rejecting duplicate route ownership.
#[derive(Default)]
pub struct ServiceRegistryBuilder {
    services: Vec<Arc<dyn Service>>,
    by_route: BTreeMap<&'static str, usize>,
}

impl ServiceRegistryBuilder {
    /// Register a service, mapping each of its routes to it.
    ///
    /// # Panics
    /// Panics if two services claim the same route — a construction-time wiring bug.
    pub fn register(mut self, service: impl Service + 'static) -> Self {
        let index = self.services.len();
        for &route in service.routes() {
            if let Some(previous) = self.by_route.insert(route, index) {
                panic!(
                    "service route `{route}` is claimed by both `{}` and `{}`",
                    self.services[previous].name(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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
        frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
    }

    impl ResponseSink for CollectingSink {
        fn emit(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
            self.frames.lock().expect("sink lock").push(response);
            Ok(())
        }
    }

    fn test_data_dir() -> Arc<SinomoDataDir> {
        Arc::new(SinomoDataDir::new(
            std::env::temp_dir().join("sinomo-service-seam-test"),
        ))
    }

    fn local_context(sink: CollectingSink) -> ServiceContext {
        ServiceContext {
            data_dir: test_data_dir(),
            cluster: None,
            signer: ServiceSigner::Local,
            sink: Box::new(sink),
        }
    }

    #[tokio::test]
    async fn registry_dispatches_known_route_and_rejects_unknown() {
        let registry = ServiceRegistry::builder().register(EchoService).build();
        assert_eq!(registry.route_count(), 1);
        assert_eq!(registry.service_names(), vec!["echo"]);

        let sink = CollectingSink::default();
        let frames = Arc::clone(&sink.frames);
        let request =
            ObjectRpcRequest::encode_body("/test/echo", "test.category", &()).expect("encode");
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
            ObjectRpcRequest::encode_body("/test/slow", "test.category", &()).expect("encode");
        let slow = tokio::spawn({
            let registry = Arc::clone(&registry);
            async move {
                registry
                    .dispatch(local_context(CollectingSink::default()), slow_request)
                    .await
            }
        });

        let fast_request =
            ObjectRpcRequest::encode_body("/test/echo", "test.category", &()).expect("encode");
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
