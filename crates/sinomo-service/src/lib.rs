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

mod observed_task;
pub use observed_task::observed_blocking_task;

pub mod discovery;
pub use discovery::{
    CLUSTER_SERVICES_KV_PREFIX, ServiceInstanceRecord, ServiceTargetSelector,
    TargetSelectionPolicy, cluster_service_instance_key, cluster_service_prefix,
    discover_service_instances, publish_local_service_instances,
    publish_local_service_instances_with_authoritative_recheck,
    service_instance_records_from_registry, service_instances_from_entries,
    service_instances_providing_capability,
};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use sinomo_kv::{
    ClusterKvRequestOutcomeRead, ClusterKvTxnCommitResult, ClusterKvTxnCompactCommitResult,
    KvAuthoritativeRead, KvEntry, KvError, KvLocalRead, KvRequestId, KvTxnRequest,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    NetworkError, ObjectRpcBodySource, ObjectRpcIncomingBody, ObjectRpcOutgoingResponse,
    ObjectRpcRequest, ObjectRpcResponse,
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
    /// Read a single entry from the best local view. The metadata is
    /// observational and does not establish cluster freshness.
    fn kv_get_local(&self, key: &str) -> Result<KvLocalRead<Option<KvEntry>>, ServiceError>;
    /// Read a prefix from the best local view, with explicit local-read
    /// metadata.
    fn kv_range_prefix_local(
        &self,
        prefix: &str,
    ) -> Result<KvLocalRead<Vec<KvEntry>>, ServiceError>;
    /// Read one entry at or after a current-term quorum-confirmed barrier.
    /// This must fail rather than degrade to the local view when authority
    /// cannot be established.
    fn kv_get_authoritative_async<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<KvAuthoritativeRead<Option<KvEntry>>, ServiceError>>
                + Send
                + 'a,
        >,
    >;
    /// Read one ordered, exact key set from a single current-term
    /// quorum-confirmed KV revision.
    ///
    /// The one returned authority proof binds every value in the result. Hosts
    /// must not implement this as a sequence of point reads: doing so can mix
    /// independently authoritative revisions when related control records are
    /// changing. The compatibility default fails closed so embedders must opt
    /// into a native exact-set authority path explicitly.
    fn kv_get_authoritative_exact_set_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<KvAuthoritativeRead<Vec<Option<KvEntry>>>, ServiceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            Err(ServiceError::Service(format!(
                "authoritative exact-key-set reads are unavailable for this service host ({} keys requested)",
                keys.len()
            )))
        })
    }
    /// Read one prefix at a single current-term quorum-confirmed revision.
    ///
    /// Prefix consumers such as placement and topology modules must not
    /// reconstruct this from point reads. The compatibility default fails
    /// closed so hosts explicitly opt into a native bounded ReadIndex path.
    fn kv_range_prefix_authoritative_async<'a>(
        &'a self,
        prefix: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<KvAuthoritativeRead<Vec<KvEntry>>, ServiceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            Err(ServiceError::Service(format!(
                "authoritative prefix reads are unavailable for this service host ({prefix:?})"
            )))
        })
    }
    /// Read retained request history through the SAME normal current-term
    /// authoritative boundary. This never executes the supplied request ID.
    /// Received request DATA cannot become an effect/receipt on an unsupported host.
    fn kv_request_outcome_authoritative_async<'a>(
        &'a self,
        _request_id: KvRequestId,
    ) -> Pin<Box<dyn Future<Output = Result<ClusterKvRequestOutcomeRead, ServiceError>> + Send + 'a>>
    {
        Box::pin(async {
            Err(ServiceError::Kv(KvError::AuthorityUnavailable {
                detail: "host has no native authoritative request-outcome reader".into(),
            }))
        })
    }

    /// Commit a stable-ID cluster-KV request through the host-owned async
    /// runtime. The ID must be preserved through forwarding and retries.
    fn kv_commit_async<'a>(
        &'a self,
        request: KvTxnRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ClusterKvTxnCommitResult, ServiceError>> + Send + 'a>>;

    /// Commits without forcing the host to materialize the complete post-commit
    /// keyspace. Hosts with a resident KV backend should override this method;
    /// the compatibility default keeps existing embedders source compatible.
    fn kv_commit_compact_async<'a>(
        &'a self,
        request: KvTxnRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<ClusterKvTxnCompactCommitResult, ServiceError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.kv_commit_async(request)
                .await
                .map(|result| ClusterKvTxnCompactCommitResult {
                    request_id: result.request_id,
                    submitted_command_digest: Some(result.command_digest),
                    command_digest: result.command_digest,
                    txn_response: result.txn_response,
                    authority: result.authority,
                    replayed_committed_request: result.replayed_committed_request,
                })
        })
    }
}

/// Where the response to one service request goes.
///
/// The sink is consumed by [`ResponseSink::send`], making response delivery exactly
/// once. The host adapter decides the destination (a network session, stdout, an
/// in-memory collector, …), while `sinomo-network` owns body framing and transfer.
pub trait ResponseSink: Send {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError>;
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
    /// Host-assigned identity of a checked in-process component invocation.
    /// Never populated from wire metadata or a domain's producer label.
    component_caller: Option<String>,
    request_body: ObjectRpcIncomingBody,
    response_sink: Option<Box<dyn ResponseSink>>,
    response_body_mode: ServiceBodyMode,
    response_lifetime_guards: Vec<Box<dyn Any + Send>>,
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
            component_caller: None,
            request_body,
            response_sink: Some(response_sink),
            response_body_mode: ServiceBodyMode::None,
            response_lifetime_guards: Vec::new(),
        }
    }

    /// Used by the checked component dispatcher after resolving the caller's
    /// declared requirements. Network service adapters must leave this unset.
    pub fn with_component_caller(mut self, component_id: String) -> Self {
        self.component_caller = Some(component_id);
        self
    }

    pub fn component_caller(&self) -> Option<&str> {
        self.component_caller.as_deref()
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

    /// Whether this call carries a semantic attachment in addition to its
    /// ordinary typed request value.
    pub fn has_request_attachment(&self) -> bool {
        self.request_body.is_attachment()
    }

    /// Encoded byte length of the ordinary request value, before it is decoded.
    ///
    /// A stream-native value reports its declared length; decoding validates the
    /// stream against that declaration. Semantic attachment bytes are separate.
    /// Call before taking or decoding the body. Unknown stream lengths stay None.
    pub fn request_value_encoded_bytes(&self, request: &ObjectRpcRequest) -> Option<u64> {
        if self.request_body.is_streamed_value() {
            self.request_body.total_bytes()
        } else {
            u64::try_from(request.body.len()).ok()
        }
    }

    /// Decodes the ordinary typed request value directly from transport-owned
    /// chunks when it arrived through the stream-native value path.
    ///
    /// A semantic attachment is independent of the typed request head. In that
    /// case this decodes `request.body` and leaves the attachment available to
    /// [`Self::take_request_body`].
    pub async fn decode_request_body<T>(
        &mut self,
        request: &ObjectRpcRequest,
    ) -> Result<T, ServiceError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        if self.request_body.is_streamed_value() {
            return Ok(std::mem::take(&mut self.request_body).decode_sobj().await?);
        }
        Ok(request.decode_body()?)
    }

    /// Decodes an ordinary typed request only when its encoded value is within
    /// `max_bytes`.
    ///
    /// Inline values are rejected from the request head before deserialization.
    /// Stream-native values must declare their total length, and that declaration
    /// is checked before the incremental decoder reads or allocates. A semantic
    /// attachment is independent of the typed request head and remains available
    /// through [`Self::take_request_body`].
    pub async fn decode_request_body_bounded<T>(
        &mut self,
        request: &ObjectRpcRequest,
        max_bytes: usize,
    ) -> Result<T, ServiceError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let encoded_bytes = self.request_value_encoded_bytes(request).ok_or_else(|| {
            ServiceError::Network(NetworkError::Serialization {
                detail: "bounded incremental SOBJ decoding requires a declared body length"
                    .to_string(),
            })
        })?;
        if encoded_bytes > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
            return Err(ServiceError::Network(NetworkError::FrameTooLarge {
                actual_bytes: usize::try_from(encoded_bytes).unwrap_or(usize::MAX),
                max_bytes,
            }));
        }
        self.decode_request_body(request).await
    }

    async fn materialize_request_value(
        &mut self,
        request: &mut ObjectRpcRequest,
    ) -> Result<(), ServiceError> {
        if self.request_body.is_streamed_value() {
            request.body = std::mem::take(&mut self.request_body)
                .collect_to_vec(usize::MAX)
                .await?;
        }
        Ok(())
    }

    fn request_attachment_is_empty(&self) -> bool {
        !self.has_request_attachment()
    }

    fn set_response_body_mode(&mut self, mode: ServiceBodyMode) {
        self.response_body_mode = mode;
    }

    fn hold_response_lifetime_guard<T>(&mut self, guard: T)
    where
        T: Any + Send + 'static,
    {
        self.response_lifetime_guards.push(Box::new(guard));
    }

    /// Send a response without a body attachment.
    pub fn respond(&mut self, response: ObjectRpcResponse) -> Result<(), ServiceError> {
        self.respond_with_body(response, ObjectRpcBodySource::default())
    }

    /// Encodes an ordinary typed response directly into transport-shaped
    /// segments that the owned path adopts without payload copies.
    pub fn respond_borrowed_value<T: serde::Serialize + ?Sized>(
        &mut self,
        route: impl Into<String>,
        value: &T,
    ) -> Result<(), ServiceError> {
        let sink = self.response_sink.take().ok_or_else(|| {
            ServiceError::Service("service response was already sent".to_string())
        })?;
        sink.send(ObjectRpcOutgoingResponse::encode_borrowed_value(
            route, value,
        )?)
    }

    /// Sends an owned ordinary response value. Values larger than one network
    /// record remain as their Rust representation until the transport grants
    /// slab credit, then serialize directly into those owned slabs.
    pub fn respond_value<T>(
        &mut self,
        route: impl Into<String>,
        value: T,
    ) -> Result<(), ServiceError>
    where
        T: serde::Serialize + Send + 'static,
    {
        let sink = self.response_sink.take().ok_or_else(|| {
            ServiceError::Service("service response was already sent".to_string())
        })?;
        sink.send(ObjectRpcOutgoingResponse::encode_value(route, value)?)
    }

    #[doc(hidden)]
    pub fn respond_owned_value<T>(
        &mut self,
        route: impl Into<String>,
        value: T,
    ) -> Result<(), ServiceError>
    where
        T: serde::Serialize + Send + 'static,
    {
        self.respond_value(route, value)
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
        validate_service_body_mode(
            response.route.as_str(),
            "response",
            self.response_body_mode,
            body.is_empty(),
        )?;
        let response = if body.is_empty() {
            ObjectRpcOutgoingResponse::value(response)
        } else {
            let mut body = body;
            for guard in self.response_lifetime_guards.drain(..) {
                body.hold_lifetime_guard(guard)?;
            }
            ObjectRpcOutgoingResponse::new(response, body)
        };
        sink.send(response)
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

/// Whether a service method accepts an attached streaming body.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ServiceBodyMode {
    #[default]
    None,
    Optional,
    Required,
}

/// How an ordinary typed request value crosses the final service boundary.
///
/// Stream-native incremental decoding is the default. A compatibility route
/// must opt in explicitly when its handler still requires one contiguous
/// encoded request buffer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ServiceRequestValueMode {
    BufferedCompatibility,
    #[default]
    Incremental,
}

/// Current capacity of one method in the active service registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceRouteReadiness {
    pub route: String,
    pub category: String,
    pub concurrency_limit: Option<usize>,
    pub available_concurrency: Option<usize>,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceDiscoveryLocality {
    #[default]
    Local,
    Cluster,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceDiscoveryHealth {
    Starting,
    #[default]
    Ready,
    Degraded,
    Unavailable,
}

/// Provider identity attached to a route and projected into cluster discovery.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ServiceDiscoveryIdentity {
    pub service_id: String,
    pub interface_id: String,
    pub interface_version: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caller_requirements: Vec<String>,
    pub component_id: String,
    pub component_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub component_generation: u64,
    #[serde(default)]
    pub health: ServiceDiscoveryHealth,
    #[serde(default)]
    pub locality: ServiceDiscoveryLocality,
}

impl ServiceRouteReadiness {
    pub fn is_ready(&self) -> bool {
        self.available_concurrency
            .is_none_or(|available| available > 0)
    }

    pub fn is_saturated(&self) -> bool {
        !self.is_ready()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceRoute {
    pub route: String,
    pub category: String,
    pub summary: String,
    pub input_type: String,
    pub output_type: String,
    pub observability: BTreeMap<String, String>,
    pub auth_scope: Option<String>,
    pub required_capabilities: Vec<String>,
    pub required_local_permissions: Vec<String>,
    pub exact_member_identity: bool,
    pub concurrency_limit: Option<usize>,
    pub timeout_ms: Option<u64>,
    pub request_body_mode: ServiceBodyMode,
    pub response_body_mode: ServiceBodyMode,
    pub request_value_mode: ServiceRequestValueMode,
    pub discovery: Option<ServiceDiscoveryIdentity>,
}

impl ServiceRoute {
    pub fn new(route: impl Into<String>, category: impl Into<String>) -> Self {
        Self {
            route: route.into(),
            category: category.into(),
            summary: String::new(),
            input_type: String::new(),
            output_type: String::new(),
            observability: BTreeMap::new(),
            auth_scope: None,
            required_capabilities: Vec::new(),
            required_local_permissions: Vec::new(),
            exact_member_identity: false,
            concurrency_limit: None,
            timeout_ms: None,
            request_body_mode: ServiceBodyMode::None,
            response_body_mode: ServiceBodyMode::None,
            request_value_mode: ServiceRequestValueMode::Incremental,
            discovery: None,
        }
    }

    pub fn describe(
        route: impl Into<String>,
        category: impl Into<String>,
        summary: impl Into<String>,
        input_type: impl Into<String>,
        output_type: impl Into<String>,
    ) -> Self {
        Self {
            summary: summary.into(),
            input_type: input_type.into(),
            output_type: output_type.into(),
            ..Self::new(route, category)
        }
    }

    pub fn observable(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.observability.insert(key.into(), value.into());
        self
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

    pub fn require_local_permission(mut self, permission: impl Into<String>) -> Self {
        self.required_local_permissions.push(permission.into());
        self
    }

    pub fn require_local_permissions<I, S>(mut self, permissions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_local_permissions
            .extend(permissions.into_iter().map(Into::into));
        self
    }

    pub fn require_exact_member_identity(mut self) -> Self {
        self.exact_member_identity = true;
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

    pub fn with_incremental_request_value(mut self) -> Self {
        self.request_value_mode = ServiceRequestValueMode::Incremental;
        self
    }

    /// Explicit compatibility boundary for a handler that still decodes the
    /// complete serialized request from [`ObjectRpcRequest::body`].
    pub fn with_buffered_request_value(mut self) -> Self {
        self.request_value_mode = ServiceRequestValueMode::BufferedCompatibility;
        self
    }

    pub fn with_discovery_identity(mut self, identity: ServiceDiscoveryIdentity) -> Self {
        self.discovery = Some(identity);
        self
    }
}

type ServiceHandler =
    Arc<dyn Fn(ServiceContext, ObjectRpcRequest) -> ServiceFuture<'static> + Send + Sync + 'static>;

/// Stable identity supplied to a process-wide invocation gate before a
/// service handler starts. The gate is host mechanism: it can fence a draining
/// component generation without teaching the service registry about modules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceInvocationMetadata {
    pub route: String,
    pub category: String,
    pub component_id: Option<String>,
    pub component_version: Option<String>,
    pub artifact_id: Option<String>,
    pub component_generation: Option<u64>,
}

pub trait ServiceInvocationLease: Send + Sync {}

impl<T: Send + Sync> ServiceInvocationLease for T {}

pub trait ServiceInvocationGate: Send + Sync {
    fn enter(
        &self,
        invocation: &ServiceInvocationMetadata,
    ) -> Result<Arc<dyn ServiceInvocationLease>, ServiceError>;
}

#[derive(Clone)]
struct RouteOwner {
    service_name: &'static str,
    handler: ServiceHandler,
    category: String,
    summary: String,
    input_type: String,
    output_type: String,
    observability: BTreeMap<String, String>,
    auth_scope: Option<String>,
    required_capabilities: Vec<String>,
    required_local_permissions: Vec<String>,
    exact_member_identity: bool,
    concurrency_limit: Option<usize>,
    timeout_ms: Option<u64>,
    request_body_mode: ServiceBodyMode,
    response_body_mode: ServiceBodyMode,
    request_value_mode: ServiceRequestValueMode,
    discovery: Option<ServiceDiscoveryIdentity>,
    semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

/// Dispatches object-RPC requests to the service that owns the request's route and
/// category. The returned future is `'static`, so the host can spawn each request
/// independently and services never block one another.
#[derive(Default, Clone)]
pub struct ServiceRegistry {
    service_names: Vec<&'static str>,
    by_route: BTreeMap<String, RouteOwner>,
    invocation_gate: Option<Arc<dyn ServiceInvocationGate>>,
}

impl ServiceRegistry {
    pub fn builder() -> ServiceRegistryBuilder {
        ServiceRegistryBuilder::default()
    }

    /// Route `request` to its owning service, returning a spawnable `'static` future.
    /// The registry is the sole owner of method lookup and generic method policy.
    pub fn dispatch(
        &self,
        ctx: ServiceContext,
        request: ObjectRpcRequest,
    ) -> ServiceFuture<'static> {
        match self.by_route.get(request.route.as_str()) {
            Some(owner) if request.category == owner.category => {
                if let Err(error) = validate_service_body_mode(
                    request.route.as_str(),
                    "request",
                    owner.request_body_mode,
                    ctx.request_attachment_is_empty(),
                ) {
                    return Box::pin(async move { Err(error) });
                }
                let handler = Arc::clone(&owner.handler);
                let route = request.route.clone();
                let concurrency_limit = owner.concurrency_limit;
                let semaphore = owner.semaphore.clone();
                let timeout = owner.timeout_ms.map(Duration::from_millis);
                let response_body_mode = owner.response_body_mode;
                let request_value_mode = owner.request_value_mode;
                let invocation_gate = self.invocation_gate.clone();
                let invocation = ServiceInvocationMetadata {
                    route: request.route.clone(),
                    category: owner.category.clone(),
                    component_id: owner
                        .discovery
                        .as_ref()
                        .map(|identity| identity.component_id.clone()),
                    component_version: owner
                        .discovery
                        .as_ref()
                        .map(|identity| identity.component_version.clone()),
                    artifact_id: owner
                        .discovery
                        .as_ref()
                        .and_then(|identity| identity.artifact_id.clone()),
                    component_generation: owner
                        .discovery
                        .as_ref()
                        .map(|identity| identity.component_generation),
                };
                Box::pin(async move {
                    let invocation_lease = invocation_gate
                        .as_ref()
                        .map(|gate| gate.enter(&invocation))
                        .transpose()?;
                    let mut ctx = ctx;
                    if let Some(invocation_lease) = invocation_lease.as_ref() {
                        ctx.hold_response_lifetime_guard(Arc::clone(invocation_lease));
                    }
                    let _handler_invocation_lease = invocation_lease;
                    let mut request = request;
                    ctx.set_response_body_mode(response_body_mode);
                    if request_value_mode == ServiceRequestValueMode::BufferedCompatibility {
                        ctx.materialize_request_value(&mut request).await?;
                    }
                    if let Some(semaphore) = semaphore {
                        let permit =
                            semaphore.try_acquire_owned().map_err(|error| match error {
                                tokio::sync::TryAcquireError::NoPermits => {
                                    ServiceError::Network(NetworkError::ObjectRpcRouteSaturated {
                                        route: route.clone(),
                                        concurrency_limit: concurrency_limit.unwrap_or(0),
                                    })
                                }
                                tokio::sync::TryAcquireError::Closed => {
                                    ServiceError::Network(NetworkError::Io {
                                        operation: format!("service route {route}"),
                                        detail: "route limiter closed".to_string(),
                                    })
                                }
                            })?;
                        ctx.hold_response_lifetime_guard(permit);
                    }
                    let call = handler(ctx, request);
                    if let Some(timeout) = timeout {
                        tokio::time::timeout(timeout, call).await.map_err(|_| {
                            ServiceError::Network(NetworkError::Io {
                                operation: format!("service route {route}"),
                                detail: format!("timed out after {}ms", timeout.as_millis()),
                            })
                        })?
                    } else {
                        call.await
                    }
                })
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
        self.service_names.clone()
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
                summary: owner.summary.clone(),
                input_type: owner.input_type.clone(),
                output_type: owner.output_type.clone(),
                observability: owner.observability.clone(),
                auth_scope: owner.auth_scope.clone(),
                required_capabilities: owner.required_capabilities.clone(),
                required_local_permissions: owner.required_local_permissions.clone(),
                exact_member_identity: owner.exact_member_identity,
                concurrency_limit: owner.concurrency_limit,
                timeout_ms: owner.timeout_ms,
                request_body_mode: owner.request_body_mode,
                response_body_mode: owner.response_body_mode,
                request_value_mode: owner.request_value_mode,
                discovery: owner.discovery.clone(),
            })
            .collect()
    }

    pub fn route(&self, route: &str) -> Option<ServiceRoute> {
        let owner = self.by_route.get(route)?;
        Some(ServiceRoute {
            route: route.to_string(),
            category: owner.category.clone(),
            summary: owner.summary.clone(),
            input_type: owner.input_type.clone(),
            output_type: owner.output_type.clone(),
            observability: owner.observability.clone(),
            auth_scope: owner.auth_scope.clone(),
            required_capabilities: owner.required_capabilities.clone(),
            required_local_permissions: owner.required_local_permissions.clone(),
            exact_member_identity: owner.exact_member_identity,
            concurrency_limit: owner.concurrency_limit,
            timeout_ms: owner.timeout_ms,
            request_body_mode: owner.request_body_mode,
            response_body_mode: owner.response_body_mode,
            request_value_mode: owner.request_value_mode,
            discovery: owner.discovery.clone(),
        })
    }

    pub fn route_readiness(&self, route: &str) -> Option<ServiceRouteReadiness> {
        let owner = self.by_route.get(route)?;
        Some(ServiceRouteReadiness {
            route: route.to_string(),
            category: owner.category.clone(),
            concurrency_limit: owner.concurrency_limit,
            available_concurrency: owner
                .semaphore
                .as_ref()
                .map(|semaphore| semaphore.available_permits()),
            timeout_ms: owner.timeout_ms,
        })
    }

    pub fn category_for_route(&self, route: &str) -> Option<&str> {
        self.by_route
            .get(route)
            .map(|owner| owner.category.as_str())
    }

    pub fn service_name_for_route(&self, route: &str) -> Option<&'static str> {
        self.by_route.get(route).map(|owner| owner.service_name)
    }

    /// Whether any registered service owns `route`. Lets a host dispatch registry-owned
    /// routes and fall through to its own handling for the rest.
    pub fn owns(&self, route: &str) -> bool {
        self.by_route.contains_key(route)
    }

    /// Reopens this immutable snapshot for composing host-provided dynamic services.
    pub fn to_builder(&self) -> ServiceRegistryBuilder {
        ServiceRegistryBuilder {
            service_names: self.service_names.clone(),
            invocation_gate: self.invocation_gate.clone(),
            by_route: self
                .by_route
                .iter()
                .map(|(route, owner)| {
                    (
                        route.clone(),
                        PendingRouteOwner {
                            service_name: owner.service_name,
                            handler: Arc::clone(&owner.handler),
                            route: ServiceRoute {
                                route: route.clone(),
                                category: owner.category.clone(),
                                summary: owner.summary.clone(),
                                input_type: owner.input_type.clone(),
                                output_type: owner.output_type.clone(),
                                observability: owner.observability.clone(),
                                auth_scope: owner.auth_scope.clone(),
                                required_capabilities: owner.required_capabilities.clone(),
                                required_local_permissions: owner
                                    .required_local_permissions
                                    .clone(),
                                exact_member_identity: owner.exact_member_identity,
                                concurrency_limit: owner.concurrency_limit,
                                timeout_ms: owner.timeout_ms,
                                request_body_mode: owner.request_body_mode,
                                response_body_mode: owner.response_body_mode,
                                request_value_mode: owner.request_value_mode,
                                discovery: owner.discovery.clone(),
                            },
                        },
                    )
                })
                .collect(),
        }
    }
}

fn validate_service_body_mode(
    route: &str,
    direction: &str,
    mode: ServiceBodyMode,
    body_is_empty: bool,
) -> Result<(), ServiceError> {
    match (mode, body_is_empty) {
        (ServiceBodyMode::None, false) => Err(ServiceError::Network(NetworkError::Serialization {
            detail: format!("service route {route} does not accept a {direction} body"),
        })),
        (ServiceBodyMode::Required, true) => {
            Err(ServiceError::Network(NetworkError::Serialization {
                detail: format!("service route {route} requires a {direction} body"),
            }))
        }
        _ => Ok(()),
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
    version_tx: tokio::sync::watch::Sender<u64>,
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
        let (version_tx, _) = tokio::sync::watch::channel(snapshot.version());
        Self {
            active: Arc::new(ArcSwap::from(snapshot)),
            update_lock: Arc::new(Mutex::new(())),
            version_tx,
        }
    }

    pub fn active_snapshot(&self) -> Arc<ServiceRegistrySnapshot> {
        self.active.load_full()
    }

    /// Subscribe to exact active-generation changes. Consumers use this edge
    /// to publish discovery state immediately rather than waiting for a generic
    /// maintenance cadence.
    pub fn subscribe_versions(&self) -> tokio::sync::watch::Receiver<u64> {
        self.version_tx.subscribe()
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
        self.version_tx.send_replace(snapshot.version());
        Ok(snapshot)
    }
}

/// Builds a [`ServiceRegistry`], rejecting duplicate route ownership.
#[derive(Default)]
pub struct ServiceRegistryBuilder {
    service_names: Vec<&'static str>,
    by_route: BTreeMap<String, PendingRouteOwner>,
    invocation_gate: Option<Arc<dyn ServiceInvocationGate>>,
}

#[derive(Clone)]
struct PendingRouteOwner {
    service_name: &'static str,
    handler: ServiceHandler,
    route: ServiceRoute,
}

impl ServiceRegistryBuilder {
    pub fn with_invocation_gate(mut self, gate: Arc<dyn ServiceInvocationGate>) -> Self {
        self.invocation_gate = Some(gate);
        self
    }

    /// Merge one immutable registry fragment into this registry generation.
    ///
    /// Component scopes build private fragments once at mount time. The host
    /// can then compose a new generation by cloning their handlers and route
    /// metadata without reconstructing service-owned state.
    ///
    /// # Panics
    /// Panics if a route in `registry` is already owned by this builder.
    pub fn merge_registry(mut self, registry: &ServiceRegistry) -> Self {
        for (route, owner) in &registry.by_route {
            if let Some(previous) = self.by_route.insert(
                route.clone(),
                PendingRouteOwner {
                    service_name: owner.service_name,
                    handler: Arc::clone(&owner.handler),
                    route: ServiceRoute {
                        route: route.clone(),
                        category: owner.category.clone(),
                        summary: owner.summary.clone(),
                        input_type: owner.input_type.clone(),
                        output_type: owner.output_type.clone(),
                        observability: owner.observability.clone(),
                        auth_scope: owner.auth_scope.clone(),
                        required_capabilities: owner.required_capabilities.clone(),
                        required_local_permissions: owner.required_local_permissions.clone(),
                        exact_member_identity: owner.exact_member_identity,
                        concurrency_limit: owner.concurrency_limit,
                        timeout_ms: owner.timeout_ms,
                        request_body_mode: owner.request_body_mode,
                        response_body_mode: owner.response_body_mode,
                        request_value_mode: owner.request_value_mode,
                        discovery: owner.discovery.clone(),
                    },
                },
            ) {
                panic!(
                    "service route `{route}` is claimed by both `{}` and `{}`",
                    previous.service_name, owner.service_name
                );
            }
        }
        for service_name in &registry.service_names {
            if !self.service_names.contains(service_name) {
                self.service_names.push(service_name);
            }
        }
        self
    }

    /// Register a service, mapping each of its route/category pairs to it.
    ///
    /// # Panics
    /// Panics if two services claim the same route — a construction-time wiring bug.
    pub fn register(mut self, service: impl Service + 'static) -> Self {
        let service_routes = validate_service_route_categories(&service);
        let service_name = service.name();
        let service = Arc::new(service);
        for route in service_routes {
            let handler_service = Arc::clone(&service);
            let handler: ServiceHandler = Arc::new(move |ctx, request| {
                let service = Arc::clone(&handler_service);
                Box::pin(async move { service.handle(ctx, request).await })
            });
            if let Some(previous) = self.by_route.insert(
                route.route.clone(),
                PendingRouteOwner {
                    service_name,
                    handler,
                    route: route.clone(),
                },
            ) {
                panic!(
                    "service route `{}` is claimed by both `{}` and `{}`",
                    route.route, previous.service_name, service_name
                );
            }
        }
        self.service_names.push(service_name);
        self
    }

    /// Register one host-provided method in the same registry as typed services.
    pub fn register_dynamic<F, Fut>(
        mut self,
        service_name: &'static str,
        route: ServiceRoute,
        handler: F,
    ) -> Self
    where
        F: Fn(ServiceContext, ObjectRpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ServiceDisposition, ServiceError>> + Send + 'static,
    {
        validate_service_route(service_name, &route);
        let route_name = route.route.clone();
        let handler: ServiceHandler = Arc::new(move |ctx, request| Box::pin(handler(ctx, request)));
        if let Some(previous) = self.by_route.insert(
            route_name.clone(),
            PendingRouteOwner {
                service_name,
                handler,
                route,
            },
        ) {
            panic!(
                "service route `{route_name}` is claimed by both `{}` and `{service_name}`",
                previous.service_name,
            );
        }
        if !self.service_names.contains(&service_name) {
            self.service_names.push(service_name);
        }
        self
    }

    /// Replace metadata for an already registered method without changing its handler.
    pub fn replace_route_metadata(mut self, route: ServiceRoute) -> Self {
        let route_name = route.route.clone();
        let owner = self.by_route.get_mut(&route_name).unwrap_or_else(|| {
            panic!("cannot replace metadata for unknown service route `{route_name}`")
        });
        validate_service_route(owner.service_name, &route);
        owner.route = route;
        self
    }

    pub fn build(self) -> ServiceRegistry {
        ServiceRegistry {
            service_names: self.service_names,
            invocation_gate: self.invocation_gate,
            by_route: self
                .by_route
                .into_iter()
                .map(|(route, owner)| {
                    let metadata = owner.route;
                    (
                        route,
                        RouteOwner {
                            service_name: owner.service_name,
                            handler: owner.handler,
                            category: metadata.category,
                            summary: metadata.summary,
                            input_type: metadata.input_type,
                            output_type: metadata.output_type,
                            observability: metadata.observability,
                            auth_scope: metadata.auth_scope,
                            required_capabilities: metadata.required_capabilities,
                            required_local_permissions: metadata.required_local_permissions,
                            exact_member_identity: metadata.exact_member_identity,
                            concurrency_limit: metadata.concurrency_limit,
                            timeout_ms: metadata.timeout_ms,
                            request_body_mode: metadata.request_body_mode,
                            response_body_mode: metadata.response_body_mode,
                            request_value_mode: metadata.request_value_mode,
                            discovery: metadata.discovery,
                            semaphore: metadata
                                .concurrency_limit
                                .map(|limit| Arc::new(tokio::sync::Semaphore::new(limit))),
                        },
                    )
                })
                .collect(),
        }
    }
}

fn validate_service_route(service_name: &str, route: &ServiceRoute) {
    assert!(
        !route.route.trim().is_empty(),
        "service `{service_name}` declared an empty route"
    );
    assert!(
        !route.category.trim().is_empty(),
        "service `{service_name}` route `{}` declared an empty category",
        route.route
    );
    assert!(
        !matches!(route.concurrency_limit, Some(0)),
        "service `{service_name}` route `{}` declared a zero concurrency limit",
        route.route
    );
    assert!(
        !matches!(route.timeout_ms, Some(0)),
        "service `{service_name}` route `{}` declared a zero timeout",
        route.route
    );
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
        let mut local_permissions = BTreeSet::new();
        for permission in &route.required_local_permissions {
            assert!(
                !permission.trim().is_empty(),
                "service `{}` route `{}` declared an empty local permission",
                service.name(),
                route.route
            );
            assert!(
                permission == permission.trim(),
                "service `{}` route `{}` local permission `{permission}` has leading or trailing whitespace",
                service.name(),
                route.route
            );
            assert!(
                permission
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':')),
                "service `{}` route `{}` local permission `{permission}` contains invalid characters",
                service.name(),
                route.route
            );
            assert!(
                local_permissions.insert(permission.as_str()),
                "service `{}` route `{}` declares local permission `{permission}` more than once",
                service.name(),
                route.route
            );
        }
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
        frames: Arc<Mutex<Vec<ObjectRpcOutgoingResponse>>>,
    }

    impl ResponseSink for CollectingSink {
        fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
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

    static BOUNDED_DECODE_ATTEMPTS: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];

    #[derive(Debug, Eq, PartialEq)]
    struct CountedDecodeValue<const COUNTER: usize>(Vec<u8>);

    impl<const COUNTER: usize> serde::Serialize for CountedDecodeValue<COUNTER> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            self.0.serialize(serializer)
        }
    }

    impl<'de, const COUNTER: usize> serde::Deserialize<'de> for CountedDecodeValue<COUNTER> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            BOUNDED_DECODE_ATTEMPTS[COUNTER].fetch_add(1, Ordering::SeqCst);
            Vec::<u8>::deserialize(deserializer).map(Self)
        }
    }

    struct BoundedDecodeService {
        max_bytes: usize,
        mutation_count: Arc<AtomicUsize>,
    }

    impl Service for BoundedDecodeService {
        fn name(&self) -> &'static str {
            "bounded-decode"
        }

        fn routes(&self) -> &'static [&'static str] {
            &["/test/bounded-decode"]
        }

        fn route_categories(&self) -> Vec<ServiceRoute> {
            vec![
                ServiceRoute::new("/test/bounded-decode", "bounded-decode")
                    .with_request_body_mode(ServiceBodyMode::Optional),
            ]
        }

        fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
            let max_bytes = self.max_bytes;
            let mutation_count = Arc::clone(&self.mutation_count);
            Box::pin(async move {
                let decoded: CountedDecodeValue<0> =
                    ctx.decode_request_body_bounded(&request, max_bytes).await?;
                mutation_count.fetch_add(1, Ordering::SeqCst);
                ctx.respond_value("/test/bounded-decode", decoded.0.len())?;
                Ok(ServiceDisposition::Continue)
            })
        }
    }

    async fn dispatch_bounded_decode(
        request: ObjectRpcRequest,
        request_body: ObjectRpcIncomingBody,
        max_bytes: usize,
        mutation_count: Arc<AtomicUsize>,
    ) -> Result<ServiceDisposition, ServiceError> {
        let registry = ServiceRegistry::builder()
            .register(BoundedDecodeService {
                max_bytes,
                mutation_count,
            })
            .build();
        let context = ServiceContext::new(
            test_data_dir(),
            None,
            ServiceSigner::Local,
            request_body,
            Box::new(CollectingSink::default()),
        );
        registry.dispatch(context, request).await
    }

    #[tokio::test]
    async fn bounded_request_decode_checks_inline_and_streamed_lengths_before_handler_mutation() {
        let value = CountedDecodeValue::<0>(vec![0x5a; 257]);
        let inline =
            ObjectRpcRequest::encode_body("/test/bounded-decode", "bounded-decode", &value)
                .expect("encode bounded request");
        let exact_bytes = inline.body.len();
        assert!(exact_bytes > 1);

        let mutations = Arc::new(AtomicUsize::new(0));
        BOUNDED_DECODE_ATTEMPTS[0].store(0, Ordering::SeqCst);
        assert_eq!(
            dispatch_bounded_decode(
                inline.clone(),
                ObjectRpcIncomingBody::default(),
                exact_bytes,
                Arc::clone(&mutations),
            )
            .await
            .expect("decode exact-limit inline value"),
            ServiceDisposition::Continue,
        );
        assert_eq!(BOUNDED_DECODE_ATTEMPTS[0].load(Ordering::SeqCst), 1);
        assert_eq!(mutations.load(Ordering::SeqCst), 1);

        assert_eq!(
            dispatch_bounded_decode(
                inline.clone(),
                ObjectRpcIncomingBody::default(),
                exact_bytes + 1,
                Arc::clone(&mutations),
            )
            .await
            .expect("decode ordinary under-limit inline value"),
            ServiceDisposition::Continue,
        );
        assert_eq!(BOUNDED_DECODE_ATTEMPTS[0].load(Ordering::SeqCst), 2);
        assert_eq!(mutations.load(Ordering::SeqCst), 2);

        mutations.store(0, Ordering::SeqCst);
        BOUNDED_DECODE_ATTEMPTS[0].store(0, Ordering::SeqCst);
        let error = dispatch_bounded_decode(
            inline.clone(),
            ObjectRpcIncomingBody::default(),
            exact_bytes - 1,
            Arc::clone(&mutations),
        )
        .await
        .expect_err("reject oversized inline value");
        assert!(matches!(
            error,
            ServiceError::Network(NetworkError::FrameTooLarge {
                actual_bytes,
                max_bytes,
            }) if actual_bytes == exact_bytes && max_bytes == exact_bytes - 1
        ));
        assert_eq!(BOUNDED_DECODE_ATTEMPTS[0].load(Ordering::SeqCst), 0);
        assert_eq!(mutations.load(Ordering::SeqCst), 0);

        BOUNDED_DECODE_ATTEMPTS[0].store(0, Ordering::SeqCst);
        let streamed_head = ObjectRpcRequest::from_encoded_body(
            "/test/bounded-decode",
            "bounded-decode",
            Vec::new(),
        );
        assert_eq!(
            dispatch_bounded_decode(
                streamed_head.clone(),
                ObjectRpcIncomingBody::bytes(inline.body.clone()).with_test_streamed_value_role(),
                exact_bytes,
                Arc::clone(&mutations),
            )
            .await
            .expect("decode exact-limit streamed value"),
            ServiceDisposition::Continue,
        );
        assert_eq!(BOUNDED_DECODE_ATTEMPTS[0].load(Ordering::SeqCst), 1);
        assert_eq!(mutations.load(Ordering::SeqCst), 1);

        mutations.store(0, Ordering::SeqCst);
        BOUNDED_DECODE_ATTEMPTS[0].store(0, Ordering::SeqCst);
        let (_sender, declared_oversized) =
            ObjectRpcIncomingBody::test_channel(Some((exact_bytes + 1) as u64), 1);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            dispatch_bounded_decode(
                streamed_head,
                declared_oversized.with_test_streamed_value_role(),
                exact_bytes,
                Arc::clone(&mutations),
            ),
        )
        .await
        .expect("declared-length rejection must not await a streamed chunk")
        .expect_err("reject oversized streamed declaration");
        assert!(matches!(
            error,
            ServiceError::Network(NetworkError::FrameTooLarge {
                actual_bytes,
                max_bytes,
            }) if actual_bytes == exact_bytes + 1 && max_bytes == exact_bytes
        ));
        assert_eq!(BOUNDED_DECODE_ATTEMPTS[0].load(Ordering::SeqCst), 0);
        assert_eq!(mutations.load(Ordering::SeqCst), 0);

        let (_sender, undeclared) = ObjectRpcIncomingBody::test_channel(None, 1);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            dispatch_bounded_decode(
                ObjectRpcRequest::from_encoded_body(
                    "/test/bounded-decode",
                    "bounded-decode",
                    Vec::new(),
                ),
                undeclared.with_test_streamed_value_role(),
                exact_bytes,
                Arc::clone(&mutations),
            ),
        )
        .await
        .expect("missing-length rejection must not await a streamed chunk")
        .expect_err("reject streamed value without a declared length");
        assert!(matches!(
            error,
            ServiceError::Network(NetworkError::Serialization { detail })
                if detail.contains("requires a declared body length")
        ));
        assert_eq!(BOUNDED_DECODE_ATTEMPTS[0].load(Ordering::SeqCst), 0);
        assert_eq!(mutations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn bounded_request_decode_leaves_semantic_attachment_available() {
        let expected = CountedDecodeValue::<1>(vec![0x2c; 33]);
        let request = ObjectRpcRequest::encode_body(
            "/test/bounded-attachment",
            "bounded-attachment",
            &expected,
        )
        .expect("encode bounded attachment head");
        let max_bytes = request.body.len();
        let attachment = vec![0x7d; max_bytes + 17];
        let mut context = ServiceContext::new(
            test_data_dir(),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::bytes(attachment.clone()),
            Box::new(CollectingSink::default()),
        );

        BOUNDED_DECODE_ATTEMPTS[1].store(0, Ordering::SeqCst);
        assert_eq!(
            context
                .decode_request_body_bounded::<CountedDecodeValue<1>>(&request, max_bytes)
                .await
                .expect("decode typed head beside attachment"),
            expected,
        );
        assert_eq!(BOUNDED_DECODE_ATTEMPTS[1].load(Ordering::SeqCst), 1);
        assert!(context.has_request_attachment());
        assert_eq!(
            context
                .take_request_body()
                .collect_to_vec(attachment.len())
                .await
                .expect("collect preserved attachment"),
            attachment,
        );
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

        struct CountingGate(Arc<AtomicUsize>);
        struct CountingLease(Arc<AtomicUsize>);
        impl Drop for CountingLease {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        impl ServiceInvocationGate for CountingGate {
            fn enter(
                &self,
                _invocation: &ServiceInvocationMetadata,
            ) -> Result<Arc<dyn ServiceInvocationLease>, ServiceError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(CountingLease(Arc::clone(&self.0))))
            }
        }

        let active = Arc::new(AtomicUsize::new(0));
        let registry = ServiceRegistry::builder()
            .with_invocation_gate(Arc::new(CountingGate(Arc::clone(&active))))
            .register(BodyService)
            .build();
        let sink = CollectingSink::default();
        let frames = Arc::clone(&sink.frames);
        let request = ObjectRpcRequest::encode_body("/test/body", "body", &()).expect("encode");
        registry
            .dispatch(local_context(sink), request)
            .await
            .expect("dispatch");
        let body_response = frames.lock().expect("frames").pop().expect("response");
        assert_eq!(
            active.load(Ordering::SeqCst),
            1,
            "streaming response must retain its invocation lease"
        );

        assert_eq!(
            body_response
                .response
                .decode_body::<String>()
                .expect("metadata"),
            "metadata"
        );
        assert!(!body_response.body.is_empty());
        assert_eq!(body_response.body.total_bytes(), Some(11));
        assert_eq!(
            registry.route_categories(),
            vec![
                ServiceRoute::new("/test/body", "body")
                    .with_response_body_mode(ServiceBodyMode::Optional)
            ]
        );
        drop(body_response);
        assert_eq!(active.load(Ordering::SeqCst), 0);
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
            )
            .require_local_permissions(["remote-management", "device:display"]),
            ServiceRoute::new("/test/module", "module")
                .with_auth_scope("admin")
                .require_capability("module.read")
                .require_capability("module.write")
                .require_local_permission("remote-management")
                .require_local_permission("device:display")
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
                        .require_local_permission("remote-management")
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
                    .require_local_permission("remote-management")
                    .with_concurrency_limit(4)
                    .with_timeout(Duration::from_millis(250))
                    .with_request_body_mode(ServiceBodyMode::Required)
                    .with_response_body_mode(ServiceBodyMode::Optional)
            ]
        );
    }

    #[test]
    #[should_panic(expected = "has leading or trailing whitespace")]
    fn registry_rejects_non_normalized_local_permission() {
        struct InvalidPermissionService;

        impl Service for InvalidPermissionService {
            fn name(&self) -> &'static str {
                "invalid-permission"
            }

            fn routes(&self) -> &'static [&'static str] {
                &["/test/invalid-permission"]
            }

            fn route_categories(&self) -> Vec<ServiceRoute> {
                vec![
                    ServiceRoute::new("/test/invalid-permission", "test")
                        .require_local_permission(" remote-management"),
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

        let _ = ServiceRegistry::builder().register(InvalidPermissionService);
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
        let mut versions = store.subscribe_versions();
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
        versions.changed().await.expect("registry generation edge");
        assert_eq!(*versions.borrow_and_update(), 2);
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
    async fn registry_dispatch_owns_concurrency_and_readiness_policy() {
        struct LimitedService {
            active: Arc<AtomicUsize>,
            max_active: Arc<AtomicUsize>,
            entered: Arc<tokio::sync::Notify>,
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
                let entered = Arc::clone(&self.entered);
                let mut release = self.release.clone();
                Box::pin(async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    entered.notify_one();
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
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release_tx, release_rx) = tokio::sync::watch::channel(false);
        let registry = Arc::new(
            ServiceRegistry::builder()
                .register(LimitedService {
                    active: Arc::clone(&active),
                    max_active: Arc::clone(&max_active),
                    entered: Arc::clone(&entered),
                    release: release_rx,
                })
                .build(),
        );
        let initial = registry
            .route_readiness("/test/limited")
            .expect("initial readiness");
        assert_eq!(initial.concurrency_limit, Some(1));
        assert_eq!(initial.available_concurrency, Some(1));
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

        entered.notified().await;
        let in_flight = registry
            .route_readiness("/test/limited")
            .expect("in-flight readiness");
        assert_eq!(in_flight.available_concurrency, Some(0));
        assert!(in_flight.is_saturated());

        let second_request =
            ObjectRpcRequest::encode_body("/test/limited", "limited", &()).expect("second request");
        let error = registry
            .dispatch(local_context(CollectingSink::default()), second_request)
            .await
            .expect_err("second request must fail while the route is saturated");
        assert!(matches!(
            error,
            ServiceError::Network(NetworkError::ObjectRpcRouteSaturated {
                ref route,
                concurrency_limit: 1,
            }) if route == "/test/limited"
        ));

        release_tx.send(true).expect("release handlers");
        first.await.expect("first task").expect("first dispatch");

        assert_eq!(max_active.load(Ordering::SeqCst), 1);
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
