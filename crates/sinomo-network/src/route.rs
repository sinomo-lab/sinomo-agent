//! Object RPC over the stream-native node-routing executor.

use crate::prelude::*;
use crate::{
    ClusterConnectivityGraph, ClusterPaths, DaemonRpcTimeouts, NetworkCallPolicy,
    NetworkDeliveryState, NetworkDuplexStream, NetworkError, NetworkHealthPolicy,
    NetworkTimeoutPhase, ObjectRpcCall, ObjectRpcRequest, ObjectRpcResult,
};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

const PREPARED_ROUTE_CACHE_TTL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PreparedRouteCacheKey {
    cluster_root: PathBuf,
    local_node_id: NodeId,
    directory_generation: u64,
    connect_timeout: Duration,
    no_progress_timeout: Duration,
}

struct PreparedRouteCacheEntry {
    connector: Arc<crate::MultiHopRoutedConnector>,
    valid_until: Instant,
}

static PREPARED_ROUTE_CACHE: LazyLock<
    Mutex<BTreeMap<PreparedRouteCacheKey, PreparedRouteCacheEntry>>,
> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn prepared_route_cache_get(
    key: &PreparedRouteCacheKey,
) -> Option<Arc<crate::MultiHopRoutedConnector>> {
    let now = Instant::now();
    let mut cache = PREPARED_ROUTE_CACHE.lock().ok()?;
    cache.retain(|_, entry| entry.valid_until > now);
    cache.get(key).map(|entry| Arc::clone(&entry.connector))
}

fn prepared_route_cache_insert(
    key: PreparedRouteCacheKey,
    connector: Arc<crate::MultiHopRoutedConnector>,
) {
    let Ok(mut cache) = PREPARED_ROUTE_CACHE.lock() else {
        return;
    };
    let now = Instant::now();
    cache.retain(|candidate, entry| {
        entry.valid_until > now
            && (candidate.cluster_root != key.cluster_root
                || candidate.local_node_id != key.local_node_id
                || candidate.directory_generation >= key.directory_generation)
    });
    cache.insert(
        key,
        PreparedRouteCacheEntry {
            connector,
            valid_until: now + PREPARED_ROUTE_CACHE_TTL,
        },
    );
}

fn record_routed_rpc_timeline_stage(
    trace: Option<crate::NetworkTimelineTraceContext>,
    stage: u16,
    started_ns: u64,
    success: bool,
) {
    let Some(trace) = trace else {
        return;
    };
    crate::record_network_timeline_event_now(crate::NetworkTimelineEvent::new(
        trace,
        stage,
        crate::network_timeline_flag::REQUEST
            | if success {
                crate::network_timeline_flag::SUCCESS
            } else {
                0
            },
        0,
        0,
        0,
        0,
        0,
        crate::network_timeline_monotonic_ns().saturating_sub(started_ns),
    ));
}

pub(crate) fn network_perf_trace_elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

fn network_rpc_outcome(result: &Result<ObjectRpcResult, NetworkError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(NetworkError::Timeout { .. }) => "timeout",
        Err(
            NetworkError::AuthDenied
            | NetworkError::AuthExpired
            | NetworkError::UnauthorizedNode { .. }
            | NetworkError::CertificateExpired { .. }
            | NetworkError::CertificateKeyMismatch { .. }
            | NetworkError::CertificateSignatureMissing { .. },
        ) => "auth",
        Err(
            NetworkError::ClusterOrphan
            | NetworkError::UnknownNode { .. }
            | NetworkError::UnknownRoute { .. },
        ) => "no_route",
        Err(
            NetworkError::MailboxFull { .. }
            | NetworkError::ObjectFrameMemoryUnavailable { .. }
            | NetworkError::ObjectRpcRouteSaturated { .. }
            | NetworkError::FrameTooLarge { .. }
            | NetworkError::ResourceExhausted { .. },
        ) => "saturated",
        Err(
            NetworkError::OutcomeUnknown { .. }
            | NetworkError::RetryExhausted { .. }
            | NetworkError::DeliveryFailed { .. },
        ) => "delivery",
        Err(NetworkError::Io { .. } | NetworkError::StorageIo { .. }) => "io",
        Err(_) => "protocol",
    }
}

fn record_network_rpc_metrics(
    path: &'static str,
    started: Instant,
    request_head_bytes: usize,
    result: &Result<ObjectRpcResult, NetworkError>,
) {
    let outcome = network_rpc_outcome(result);
    metrics::counter!(
        "sinomo.network.rpc.requests",
        "path" => path,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.network.rpc.total.us",
        "path" => path,
        "outcome" => outcome
    )
    .record(network_perf_trace_elapsed_us(started) as f64);
    metrics::counter!("sinomo.network.rpc.request_head.bytes", "path" => path)
        .increment(request_head_bytes as u64);
    if let Ok(result) = result {
        metrics::counter!("sinomo.network.rpc.response_head.bytes", "path" => path)
            .increment(result.response.body.len() as u64);
    }
}

fn stream_native_service_route_constraints() -> crate::RouteConstraints {
    crate::RouteConstraints {
        // The routed-session layer encrypts and authenticates the complete
        // endpoint stream. Requiring the adjacent link to be confidential
        // as well would incorrectly exclude the built-in LAN TCP link while
        // adding no service-RPC confidentiality.
        require_confidentiality: false,
        ..crate::RouteConstraints::default()
    }
}

/// Makes a bounded, explicitly active timeline capture observe locally
/// originated RPCs without requiring every domain caller to know about the
/// diagnostic metadata contract.
///
/// The metadata is attached before the request is signed and only when the
/// request is still unsigned. An explicit sampled value remains authoritative,
/// so callers can suppress a particularly noisy operation even during a
/// capture. Ordinary traffic still pays only the timeline's relaxed atomic
/// active check.
fn prepare_call_for_active_network_timeline(call: &mut ObjectRpcCall) {
    if !crate::network_timeline_capture_is_active() || call.request.auth.is_some() {
        return;
    }
    call.request.ensure_trace_context();
    call.request
        .metadata
        .entry(crate::OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY.to_string())
        .or_insert_with(|| "true".to_string());
}

/// A discovered route to a target node: the immediate next hop to send to, and the full
/// node path (`local` … `target`, inclusive) for diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRoute {
    pub next_hop: NodeId,
    pub path: Vec<NodeId>,
}

fn try_clone_route_node_id(node_id: &NodeId, operation: &str) -> Result<NodeId, NetworkError> {
    crate::object::try_clone_id(node_id, operation)
}

#[derive(Clone, Debug, Default)]
struct RouteNodeSet {
    nodes: Vec<NodeId>,
}

impl RouteNodeSet {
    fn try_insert_clone(
        &mut self,
        node_id: &NodeId,
        operation: &str,
    ) -> Result<bool, NetworkError> {
        match self.position(node_id) {
            Ok(_) => Ok(false),
            Err(index) => {
                let node_id = try_clone_route_node_id(node_id, operation)?;
                crate::object::try_reserve_vec(&mut self.nodes, 1, operation)?;
                self.nodes.insert(index, node_id);
                Ok(true)
            }
        }
    }

    fn position(&self, node_id: &NodeId) -> Result<usize, usize> {
        self.nodes.binary_search(node_id)
    }
}

#[derive(Clone, Debug, Default)]
struct RoutePredecessorIndex {
    entries: Vec<(NodeId, NodeId)>,
}

impl RoutePredecessorIndex {
    fn get(&self, node_id: &NodeId) -> Option<&NodeId> {
        self.position(node_id)
            .ok()
            .and_then(|index| self.entries.get(index).map(|(_, previous)| previous))
    }

    fn try_insert_clone(
        &mut self,
        node_id: &NodeId,
        previous: &NodeId,
        operation: &str,
    ) -> Result<(), NetworkError> {
        match self.position(node_id) {
            Ok(index) => {
                self.entries[index].1 = try_clone_route_node_id(previous, operation)?;
            }
            Err(index) => {
                let node_id = try_clone_route_node_id(node_id, operation)?;
                let previous = try_clone_route_node_id(previous, operation)?;
                crate::object::try_reserve_vec(&mut self.entries, 1, operation)?;
                self.entries.insert(index, (node_id, previous));
            }
        }
        Ok(())
    }

    fn position(&self, node_id: &NodeId) -> Result<usize, usize> {
        self.entries
            .binary_search_by(|(stored, _)| stored.cmp(node_id))
    }
}

/// Breadth-first search the connectivity graph for a shortest path from `local` to
/// `target` over reachable edges (records whose state is not `Unreachable`), returning the
/// first hop along it. Returns `None` when `target == local` or no reachable path exists.
pub fn cluster_route_next_hop(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Option<ClusterRoute> {
    cluster_route_next_hop_fallible(graph, local, target)
        .ok()
        .flatten()
}

pub fn cluster_healthy_route_next_hop(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Option<ClusterRoute> {
    cluster_route_next_hop_excluding(graph, local, target, &[], ClusterRouteAdjacency::Healthy)
        .ok()
        .flatten()
}

pub(crate) fn cluster_route_next_hop_fallible(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Result<Option<ClusterRoute>, NetworkError> {
    cluster_route_next_hop_excluding(graph, local, target, &[], ClusterRouteAdjacency::Reachable)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterRouteAdjacency {
    Reachable,
    Healthy,
}

fn cluster_route_next_hop_excluding(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &[NodeId],
    adjacency: ClusterRouteAdjacency,
) -> Result<Option<ClusterRoute>, NetworkError> {
    if local == target {
        return Ok(None);
    }
    let mut seen = RouteNodeSet::default();
    for excluded_node in excluded {
        seen.try_insert_clone(excluded_node, "cluster route excluded nodes")?;
    }
    seen.try_insert_clone(local, "cluster route seen nodes")?;
    let mut predecessor = RoutePredecessorIndex::default();
    let mut queue = VecDeque::new();
    queue
        .try_reserve(1)
        .map_err(|_| crate::object::network_allocation_error("cluster route queue", 1))?;
    queue.push_back(try_clone_route_node_id(local, "cluster route queue")?);
    while let Some(current) = queue.pop_front() {
        match adjacency {
            ClusterRouteAdjacency::Reachable => {
                for next in graph.reachable_target_refs(&current) {
                    if let Some(route) = cluster_route_visit_next(
                        local,
                        target,
                        &current,
                        next,
                        &mut seen,
                        &mut predecessor,
                        &mut queue,
                    )? {
                        return Ok(Some(route));
                    }
                }
            }
            ClusterRouteAdjacency::Healthy => {
                for next in graph.healthy_target_refs(&current) {
                    if let Some(route) = cluster_route_visit_next(
                        local,
                        target,
                        &current,
                        next,
                        &mut seen,
                        &mut predecessor,
                        &mut queue,
                    )? {
                        return Ok(Some(route));
                    }
                }
            }
        }
    }
    Ok(None)
}

fn cluster_route_visit_next(
    local: &NodeId,
    target: &NodeId,
    current: &NodeId,
    next: &NodeId,
    seen: &mut RouteNodeSet,
    predecessor: &mut RoutePredecessorIndex,
    queue: &mut VecDeque<NodeId>,
) -> Result<Option<ClusterRoute>, NetworkError> {
    if !seen.try_insert_clone(next, "cluster route seen nodes")? {
        return Ok(None);
    }
    predecessor.try_insert_clone(next, current, "cluster route predecessors")?;
    if next == target {
        return route_from_predecessors(local, target, predecessor);
    }
    queue
        .try_reserve(1)
        .map_err(|_| crate::object::network_allocation_error("cluster route queue", 1))?;
    queue.push_back(try_clone_route_node_id(next, "cluster route queue")?);
    Ok(None)
}

fn route_from_predecessors(
    local: &NodeId,
    target: &NodeId,
    predecessor: &RoutePredecessorIndex,
) -> Result<Option<ClusterRoute>, NetworkError> {
    let mut reversed = Vec::new();
    crate::object::try_reserve_vec(&mut reversed, 1, "cluster route path")?;
    reversed.push(try_clone_route_node_id(target, "cluster route path")?);
    let mut current = target;
    while current != local {
        let Some(previous) = predecessor.get(current) else {
            return Ok(None);
        };
        crate::object::try_reserve_vec(&mut reversed, 1, "cluster route path")?;
        reversed.push(try_clone_route_node_id(previous, "cluster route path")?);
        current = previous;
    }
    reversed.reverse();
    let Some(next_hop) = reversed.get(1) else {
        return Ok(None);
    };
    let next_hop = try_clone_route_node_id(next_hop, "cluster route next hop")?;
    Ok(Some(ClusterRoute {
        next_hop,
        path: reversed,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterObjectRpcOptions {
    timeouts: DaemonRpcTimeouts,
    overall_timeout: Option<Duration>,
}

impl ClusterObjectRpcOptions {
    /// Override transport phases without creating a whole-call deadline.
    /// Connect and idle limits are not reinterpreted as operation deadlines;
    /// callers with a domain budget must use `with_overall_timeout`.
    pub fn new(timeouts: DaemonRpcTimeouts) -> Self {
        Self {
            timeouts,
            overall_timeout: None,
        }
    }

    /// Builds routed-call options from the shared health/deadline policy.
    pub fn from_call_policy(policy: NetworkCallPolicy) -> Self {
        let options = Self::new(policy.health.into());
        match policy.overall_timeout {
            Some(timeout) => options.with_overall_timeout(timeout),
            None => options,
        }
    }

    /// Override the whole-call delivery deadline.
    ///
    /// The deadline covers route planning, signing, connecting, writing, and
    /// receiving the response.
    pub fn with_overall_timeout(mut self, timeout: Duration) -> Self {
        self.overall_timeout = Some(timeout);
        self
    }

    fn requested_overall_timeout(self) -> Option<Duration> {
        self.overall_timeout
    }

    fn call_policy(self) -> NetworkCallPolicy {
        let defaults = NetworkHealthPolicy::default();
        NetworkCallPolicy {
            health: NetworkHealthPolicy::new(
                self.timeouts
                    .connect_timeout
                    .unwrap_or(defaults.connect_timeout),
                self.timeouts
                    .idle_timeout
                    .unwrap_or(defaults.no_progress_timeout),
            ),
            overall_timeout: self.overall_timeout,
        }
    }
}

impl From<NetworkCallPolicy> for ClusterObjectRpcOptions {
    fn from(policy: NetworkCallPolicy) -> Self {
        Self::from_call_policy(policy)
    }
}

impl Default for ClusterObjectRpcOptions {
    fn default() -> Self {
        Self {
            timeouts: DaemonRpcTimeouts::default(),
            overall_timeout: None,
        }
    }
}

pub type ClusterSignedObjectRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterSignedObjectRpcObservationKind {
    Success,
    IoFailure,
    HardIoFailure,
    AuthDenied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterSignedObjectRpcObservation {
    kind: ClusterSignedObjectRpcObservationKind,
    latency_ms: Option<u64>,
}

impl ClusterSignedObjectRpcObservation {
    pub fn success(latency_ms: Option<u64>) -> Self {
        Self {
            kind: ClusterSignedObjectRpcObservationKind::Success,
            latency_ms,
        }
    }

    pub fn io_failure() -> Self {
        Self {
            kind: ClusterSignedObjectRpcObservationKind::IoFailure,
            latency_ms: None,
        }
    }

    /// A failed command attempt that consumed its bounded response budget.
    /// Unlike a background probe miss, this should not retain a Healthy route
    /// through the single-failure grace period.
    pub fn hard_io_failure() -> Self {
        Self {
            kind: ClusterSignedObjectRpcObservationKind::HardIoFailure,
            latency_ms: None,
        }
    }

    pub fn auth_denied() -> Self {
        Self {
            kind: ClusterSignedObjectRpcObservationKind::AuthDenied,
            latency_ms: None,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self.kind, ClusterSignedObjectRpcObservationKind::Success)
    }

    pub fn is_io_failure(&self) -> bool {
        matches!(
            self.kind,
            ClusterSignedObjectRpcObservationKind::IoFailure
                | ClusterSignedObjectRpcObservationKind::HardIoFailure
        )
    }

    pub fn is_hard_io_failure(&self) -> bool {
        matches!(
            self.kind,
            ClusterSignedObjectRpcObservationKind::HardIoFailure
        )
    }

    pub fn is_auth_denied(&self) -> bool {
        matches!(self.kind, ClusterSignedObjectRpcObservationKind::AuthDenied)
    }

    pub fn latency_ms(&self) -> Option<u64> {
        self.latency_ms
    }
}

pub trait ClusterSignedObjectRpcAsyncHost: Send {
    fn sign_object_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> ClusterSignedObjectRpcFuture<'a, Vec<u8>>;

    fn observe_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _observation: ClusterSignedObjectRpcObservation,
    ) -> ClusterSignedObjectRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Opens the transport stream for the stream-native body-aware call. Hosts
    /// may override synthetic/local transports; ordinary LAN and runtime
    /// modules use the transport-neutral registry path.
    fn open_object_rpc_stream_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        health: NetworkHealthPolicy,
    ) -> ClusterSignedObjectRpcFuture<'a, Box<dyn NetworkDuplexStream>> {
        Box::pin(async move {
            crate::roaming::open_daemon_rpc_stream_to_cluster_endpoint_async(endpoint, health).await
        })
    }
}

fn cluster_signed_endpoint_rpc_observation(
    result: Result<&ObjectRpcResult, &NetworkError>,
    started: Instant,
) -> Option<ClusterSignedObjectRpcObservation> {
    match result {
        // Completing an authenticated stream proves that the immediate endpoint is
        // reachable even when the application response denies the requested operation.
        // Session/link authentication failures arrive through the `Err` branch below.
        Ok(_) => Some(ClusterSignedObjectRpcObservation::success(Some(
            cluster_signed_endpoint_rpc_elapsed_latency_ms(started),
        ))),
        Err(error) => cluster_signed_endpoint_rpc_error_observation(error),
    }
}

fn cluster_paths_data_dir(
    paths: Option<&ClusterPaths>,
) -> Result<sinomo_local::SinomoDataDir, NetworkError> {
    if let Some(paths) = paths {
        let root = paths.root.parent().ok_or_else(|| NetworkError::Io {
            operation: "resolve cluster data directory".to_string(),
            detail: format!(
                "cluster path {} has no parent data directory",
                paths.root.display()
            ),
        })?;
        return Ok(sinomo_local::SinomoDataDir::new(root));
    }
    sinomo_local::SinomoDataDir::from_env_or_home().map_err(|error| NetworkError::Io {
        operation: "resolve object RPC call data directory".to_string(),
        detail: error.to_string(),
    })
}

fn cluster_signed_endpoint_rpc_error_observation(
    error: &NetworkError,
) -> Option<ClusterSignedObjectRpcObservation> {
    match error {
        NetworkError::Io { .. }
        | NetworkError::Timeout { .. }
        | NetworkError::DeliveryFailed { .. } => {
            Some(ClusterSignedObjectRpcObservation::io_failure())
        }
        NetworkError::AuthDenied => Some(ClusterSignedObjectRpcObservation::auth_denied()),
        _ => None,
    }
}

fn cluster_signed_endpoint_rpc_elapsed_latency_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(Clone, Copy)]
struct ClusterObjectRpcRouteContext<'a> {
    paths: &'a ClusterPaths,
}

/// Cluster object-RPC client facade.
///
/// `call_target` is the routed API: callers name a target node and
/// provide one size-independent call while the network layer chooses the next
/// hop and manages the full-duplex transfer.
pub struct ClusterObjectRpcClient<'a, H> {
    host: &'a mut H,
    route_context: Option<ClusterObjectRpcRouteContext<'a>>,
}

impl<'a, H: ClusterSignedObjectRpcAsyncHost> ClusterObjectRpcClient<'a, H> {
    /// Build a client for a caller that intentionally has only a concrete endpoint.
    pub fn endpoint_only(host: &'a mut H) -> Self {
        Self {
            host,
            route_context: None,
        }
    }

    /// Build the route-aware cluster object-RPC client.
    pub fn routed(host: &'a mut H, paths: &'a ClusterPaths) -> Self {
        Self {
            host,
            route_context: Some(ClusterObjectRpcRouteContext { paths }),
        }
    }
}

impl<'a, H> ClusterObjectRpcClient<'a, H>
where
    H: ClusterSignedObjectRpcAsyncHost + Send,
{
    /// Runs one body-aware RPC call against a concrete endpoint.
    ///
    /// The request head is signed before stream setup. The transport owns
    /// chunking, flow control, health checks, and
    /// response-body delivery after this method opens the duplex stream.
    pub async fn call_endpoint(
        &mut self,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let started = Instant::now();
        let request_head_bytes = call.request.body.len();
        let deadline = ClusterEndpointRpcDeadline::new(options.requested_overall_timeout());
        let timeout_error = deadline.timeout_error(
            "cluster endpoint object RPC",
            NetworkTimeoutPhase::Overall,
            NetworkDeliveryState::PossiblyApplied,
        );
        let result = cluster_object_rpc_call_with_deadline(
            self.call_endpoint_inner(state, endpoint, call, options, deadline),
            deadline,
            timeout_error,
        )
        .await;
        record_network_rpc_metrics("endpoint", started, request_head_bytes, &result);
        result
    }

    async fn call_endpoint_inner(
        &mut self,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        mut call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        deadline: ClusterEndpointRpcDeadline,
    ) -> Result<ObjectRpcResult, NetworkError> {
        prepare_call_for_active_network_timeline(&mut call);
        let data_dir = cluster_paths_data_dir(self.route_context.map(|context| context.paths))?;
        let policy = options.call_policy();
        let request_auth = cluster_object_rpc_sign_with_deadline(
            self.host.sign_object_rpc_request_async(&call.request),
            deadline,
        )
        .await?;
        let directory = crate::DirectorySnapshot::try_from_records(
            crate::DirectoryVersion(0),
            state.members.clone(),
            std::iter::once(endpoint.clone()),
        )?;
        let identity: Arc<dyn crate::RoutedSessionIdentity> = Arc::new(
            crate::DaemonRoutedSessionIdentity::new(state.local_node_id.clone(), data_dir)?,
        );
        let provider = crate::ClusterLinkProvider::new(
            state.local_node_id.clone(),
            Arc::clone(&identity),
            policy.health,
        )?;
        let started = Instant::now();
        let connect_timeout = match deadline.checked_remaining() {
            Some(Some(remaining)) => remaining.min(policy.health.connect_timeout),
            Some(None) => policy.health.connect_timeout,
            None => {
                return Err(deadline.timeout_error(
                    "establish direct routed endpoint session",
                    NetworkTimeoutPhase::Overall,
                    NetworkDeliveryState::DefinitelyNotSent,
                ));
            }
        };
        let constraints = stream_native_service_route_constraints();
        let target_public_key = &directory
            .node(&endpoint.node_id)
            .ok_or_else(|| NetworkError::UnauthorizedNode {
                node_id: endpoint.node_id.clone(),
            })?
            .public_key;
        let mut endpoint_pool_identity = blake3::Hasher::new();
        for field in [
            endpoint.endpoint_id.as_str(),
            endpoint.address.as_str(),
            target_public_key.as_str(),
        ] {
            endpoint_pool_identity.update(&(field.len() as u64).to_be_bytes());
            endpoint_pool_identity.update(field.as_bytes());
        }
        endpoint_pool_identity.update(format!("{:?}", endpoint.transport).as_bytes());
        let stream = crate::open_pooled_routed_stream(
            format!(
                "{}:direct-endpoint:{}",
                state.cluster_id,
                endpoint_pool_identity.finalize().to_hex()
            ),
            directory.version().0,
            constraints,
            state.local_node_id.clone(),
            endpoint.node_id.clone(),
            connect_timeout,
            crate::connect_direct_routed_endpoint(
                &directory,
                endpoint,
                &provider,
                identity.as_ref(),
                crate::ConnectOptions {
                    constraints,
                    timeout: connect_timeout,
                },
            ),
        )
        .await;
        let mut authenticated_stream_established = false;
        let result = match stream {
            Ok(stream) => {
                authenticated_stream_established = true;
                let context = crate::object_rpc_stream::RpcPeerContext::authenticated(
                    stream.local_node_id.clone(),
                    stream.peer_node_id.clone(),
                    stream.session_id.clone(),
                    stream.channel_binding,
                )?;
                crate::service_rpc_stream::call_service_rpc_stream(
                    stream,
                    &context,
                    call,
                    request_auth,
                    crate::ObjectFrameIoOptions::idle_timeout(policy.health.no_progress_timeout),
                )
                .await
            }
            Err(error) => Err(error),
        };
        let observation = if authenticated_stream_established
            && matches!(&result, Err(NetworkError::AuthDenied))
        {
            // A final service rejection is sent only after both routed
            // endpoints have authenticated. It proves endpoint reachability
            // even though the requested operation was denied.
            Some(ClusterSignedObjectRpcObservation::success(Some(
                cluster_signed_endpoint_rpc_elapsed_latency_ms(started),
            )))
        } else {
            cluster_signed_endpoint_rpc_observation(result.as_ref(), started)
        };
        if let Some(observation) = observation {
            let _ = self
                .host
                .observe_signed_endpoint_rpc_async(endpoint, observation)
                .await;
        }
        let result = result?;
        result.response.error_detail()?;
        Ok(result)
    }

    /// Runs stream-native object RPC against this process's loopback daemon before
    /// cluster bootstrap state exists.
    ///
    /// This path is deliberately limited to loopback addresses. It pins the
    /// responder to the local main-daemon identity on disk; non-loopback calls
    /// must use [`Self::call_endpoint`] and an authorized cluster member.
    pub async fn call_loopback(
        &mut self,
        data_dir: &sinomo_local::SinomoDataDir,
        address: &str,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let started = Instant::now();
        let request_head_bytes = call.request.body.len();
        let deadline = ClusterEndpointRpcDeadline::new(options.requested_overall_timeout());
        let timeout_error = deadline.timeout_error(
            "local object RPC",
            NetworkTimeoutPhase::Overall,
            NetworkDeliveryState::PossiblyApplied,
        );
        let result = cluster_object_rpc_call_with_deadline(
            self.call_loopback_inner(data_dir, address, call, options, deadline),
            deadline,
            timeout_error,
        )
        .await;
        record_network_rpc_metrics("loopback", started, request_head_bytes, &result);
        result
    }

    async fn call_loopback_inner(
        &mut self,
        data_dir: &sinomo_local::SinomoDataDir,
        address: &str,
        mut call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        deadline: ClusterEndpointRpcDeadline,
    ) -> Result<ObjectRpcResult, NetworkError> {
        if !crate::is_loopback_connect_address(address) {
            return Err(NetworkError::Io {
                operation: "open local object RPC call".to_string(),
                detail: format!("non-loopback address `{address}` requires cluster authorization"),
            });
        }
        let expected_public_key = crate::daemon_auth::cached_daemon_signing_public_key(data_dir)?;
        let endpoint_node_id =
            NodeId::derived("sinomo.network.local-object-rpc-call", &expected_public_key);
        let endpoint = crate::endpoint_advertisement::daemon_loopback_endpoint(
            &endpoint_node_id,
            address.to_string(),
            0,
        );
        prepare_call_for_active_network_timeline(&mut call);
        let policy = options.call_policy();
        let request_auth = cluster_object_rpc_sign_with_deadline(
            self.host.sign_object_rpc_request_async(&call.request),
            deadline,
        )
        .await?;
        let connect_timeout = match deadline.checked_remaining() {
            Some(Some(remaining)) => remaining.min(policy.health.connect_timeout),
            Some(None) => policy.health.connect_timeout,
            None => {
                return Err(deadline.timeout_error(
                    "establish local object RPC carrier",
                    NetworkTimeoutPhase::Overall,
                    NetworkDeliveryState::DefinitelyNotSent,
                ));
            }
        };
        let establish = async {
            let mut stream = self
                .host
                .open_object_rpc_stream_async(&endpoint, policy.health)
                .await?;
            let context =
                crate::routed_stream_pool::write_local_object_rpc_carrier_prelude(&mut *stream)
                    .await?;
            Ok((stream, context))
        };
        let stream = crate::open_pooled_local_object_rpc_stream(
            format!("{address}:{endpoint_node_id}"),
            connect_timeout,
            establish,
        )
        .await?;
        let context = crate::object_rpc_stream::RpcPeerContext::unauthenticated_local(
            stream.session_id.clone(),
            stream.channel_binding,
        );
        let result = crate::service_rpc_stream::call_service_rpc_stream(
            stream,
            &context,
            call,
            request_auth,
            crate::ObjectFrameIoOptions::idle_timeout(policy.health.no_progress_timeout),
        )
        .await?;
        result.response.error_detail()?;
        Ok(result)
    }

    pub async fn call_target(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let started = Instant::now();
        let request_head_bytes = call.request.body.len();
        let result = self
            .call_target_inner(state, target_node_id, call, options)
            .await;
        record_network_rpc_metrics("routed", started, request_head_bytes, &result);
        result
    }

    async fn call_target_inner(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        mut call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let Some(context) = self.route_context else {
            return Err(NetworkError::Io {
                operation: "cluster target object RPC call".to_string(),
                detail: "cluster target RPC call requires a route context".to_string(),
            });
        };
        let route_timeout = options.requested_overall_timeout();
        let deadline = ClusterEndpointRpcDeadline::new(route_timeout);
        let timeout_error = cluster_target_rpc_timeout_error(
            target_node_id,
            route_timeout,
            NetworkDeliveryState::PossiblyApplied,
            &[],
        );
        prepare_call_for_active_network_timeline(&mut call);
        cluster_object_rpc_call_with_deadline(
            self.call_target_stream_native(
                context.paths,
                state,
                target_node_id,
                call,
                options,
                deadline,
            ),
            deadline,
            timeout_error,
        )
        .await
    }

    async fn call_target_stream_native(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        deadline: ClusterEndpointRpcDeadline,
    ) -> Result<ObjectRpcResult, NetworkError> {
        if target_node_id == &state.local_node_id {
            return Err(NetworkError::Io {
                operation: "establish routed service RPC stream".to_string(),
                detail: "local-node calls require the explicit local service path".to_string(),
            });
        }
        let policy = options.call_policy();
        let trace = crate::network_timeline_trace_context_for_request(&call.request);
        let sign_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let request_auth_result = cluster_object_rpc_sign_with_deadline(
            self.host.sign_object_rpc_request_async(&call.request),
            deadline,
        )
        .await;
        record_routed_rpc_timeline_stage(
            trace,
            crate::network_timeline_stage::REQUEST_SIGN,
            sign_started_ns,
            request_auth_result.is_ok(),
        );
        let request_auth = request_auth_result?;
        let directory = crate::cluster_directory_with_bootstrap_state(
            crate::cluster_directory_for(paths),
            state,
        );
        let snapshot_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let routing_version_result = directory.routing_version();
        record_routed_rpc_timeline_stage(
            trace,
            crate::network_timeline_stage::ROUTE_SNAPSHOT,
            snapshot_started_ns,
            routing_version_result.is_ok(),
        );
        let routing_version = routing_version_result?;
        let initial_cache_key = PreparedRouteCacheKey {
            cluster_root: paths.root.clone(),
            local_node_id: state.local_node_id.clone(),
            directory_generation: routing_version.0,
            connect_timeout: policy.health.connect_timeout,
            no_progress_timeout: policy.health.no_progress_timeout,
        };
        let prepare_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let prepared = if let Some(connector) = prepared_route_cache_get(&initial_cache_key) {
            Ok((connector, routing_version.0))
        } else {
            (|| {
                let (directory_version, members, endpoints, directory_connectivity) =
                    directory.routing_records_snapshot()?.into_parts();
                let directory_snapshot = crate::DirectorySnapshot::try_from_records(
                    directory_version,
                    members,
                    endpoints,
                )?;
                if !directory_snapshot.is_authorized(&state.local_node_id) {
                    return Err(NetworkError::UnauthorizedNode {
                        node_id: state.local_node_id.clone(),
                    });
                }
                if !directory_snapshot.is_authorized(target_node_id) {
                    return Err(NetworkError::UnauthorizedNode {
                        node_id: target_node_id.clone(),
                    });
                }
                let connectivity = crate::connectivity_snapshot_from_directory_records(
                    &directory_snapshot,
                    crate::route_graph_read_model::read_cluster_connectivity_graph_for_routing_snapshot(
                        paths,
                        state,
                        &directory_snapshot,
                        directory_connectivity,
                    )?,
                    OffsetDateTime::now_utc(),
                )?;
                let connectivity = Arc::new(crate::ConnectivityTracker::new(connectivity));
                let data_dir = cluster_paths_data_dir(Some(paths))?;
                let identity: Arc<dyn crate::RoutedSessionIdentity> = Arc::new(
                    crate::DaemonRoutedSessionIdentity::new(state.local_node_id.clone(), data_dir)?,
                );
                let provider: Arc<dyn crate::LinkProvider> =
                    Arc::new(crate::ClusterLinkProvider::new(
                        state.local_node_id.clone(),
                        Arc::clone(&identity),
                        policy.health,
                    )?);
                let connectivity_view: Arc<dyn crate::ConnectivityView> = connectivity.clone();
                let failure_sink: Arc<dyn crate::ConnectivityFailureSink> = connectivity;
                let connector = Arc::new(crate::MultiHopRoutedConnector::from_snapshot(
                    state.local_node_id.clone(),
                    directory_snapshot,
                    connectivity_view,
                    failure_sink,
                    provider,
                    identity,
                )?);
                let cache_key = PreparedRouteCacheKey {
                    directory_generation: directory_version.0,
                    ..initial_cache_key
                };
                prepared_route_cache_insert(cache_key, Arc::clone(&connector));
                Ok((connector, directory_version.0))
            })()
        };
        record_routed_rpc_timeline_stage(
            trace,
            crate::network_timeline_stage::ROUTE_PREPARE,
            prepare_started_ns,
            prepared.is_ok(),
        );
        let (connector, directory_generation) = prepared?;
        let connect_timeout = match deadline.checked_remaining() {
            Some(Some(remaining)) => remaining.min(policy.health.connect_timeout),
            Some(None) => policy.health.connect_timeout,
            None => {
                return Err(deadline.timeout_error(
                    "establish routed service RPC stream",
                    NetworkTimeoutPhase::Overall,
                    NetworkDeliveryState::DefinitelyNotSent,
                ));
            }
        };
        let constraints = stream_native_service_route_constraints();
        let open_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let stream_result = crate::open_pooled_routed_stream(
            state.cluster_id.to_string(),
            directory_generation,
            constraints,
            state.local_node_id.clone(),
            target_node_id.clone(),
            connect_timeout,
            crate::RoutedConnector::connect(
                connector.as_ref(),
                target_node_id.clone(),
                crate::ConnectOptions {
                    constraints,
                    timeout: connect_timeout,
                },
            ),
        )
        .await;
        record_routed_rpc_timeline_stage(
            trace,
            crate::network_timeline_stage::ENDPOINT_OPEN,
            open_started_ns,
            stream_result.is_ok(),
        );
        let stream = stream_result?;
        let context = crate::object_rpc_stream::RpcPeerContext::authenticated(
            stream.local_node_id.clone(),
            stream.peer_node_id.clone(),
            stream.session_id.clone(),
            stream.channel_binding,
        )?;
        let exchange_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let result = crate::service_rpc_stream::call_service_rpc_stream(
            stream,
            &context,
            call,
            request_auth,
            crate::ObjectFrameIoOptions::idle_timeout(policy.health.no_progress_timeout),
        )
        .await;
        record_routed_rpc_timeline_stage(
            trace,
            crate::network_timeline_stage::RPC_EXCHANGE,
            exchange_started_ns,
            result.is_ok(),
        );
        let result = result?;
        result.response.error_detail()?;
        Ok(result)
    }
}

#[derive(Clone, Copy, Debug)]
struct ClusterEndpointRpcDeadline {
    deadline_at: Option<tokio::time::Instant>,
    timeout: Option<Duration>,
}

impl ClusterEndpointRpcDeadline {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            deadline_at: timeout.map(|timeout| tokio::time::Instant::now() + timeout),
            timeout,
        }
    }

    fn checked_remaining(&self) -> Option<Option<Duration>> {
        match self.deadline_at {
            Some(deadline_at) => {
                let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
                (!remaining.is_zero()).then_some(Some(remaining))
            }
            None => Some(None),
        }
    }

    fn timeout_error(
        &self,
        operation: &str,
        phase: NetworkTimeoutPhase,
        delivery_state: NetworkDeliveryState,
    ) -> NetworkError {
        NetworkError::timeout(
            operation,
            phase,
            self.timeout.unwrap_or_default(),
            delivery_state,
        )
    }
}

async fn cluster_object_rpc_call_with_deadline<T, F>(
    call: F,
    deadline: ClusterEndpointRpcDeadline,
    timeout_error: NetworkError,
) -> Result<T, NetworkError>
where
    F: Future<Output = Result<T, NetworkError>>,
{
    let Some(deadline_at) = deadline.deadline_at else {
        return call.await;
    };
    match tokio::time::timeout_at(deadline_at, call).await {
        Ok(result) => result,
        Err(_) => Err(timeout_error),
    }
}

async fn cluster_object_rpc_sign_with_deadline<T, F>(
    sign: F,
    deadline: ClusterEndpointRpcDeadline,
) -> Result<T, NetworkError>
where
    F: Future<Output = Result<T, NetworkError>>,
{
    let Some(deadline_at) = deadline.deadline_at else {
        return sign.await;
    };
    match tokio::time::timeout_at(deadline_at, sign).await {
        Ok(result) => result,
        Err(_) => Err(deadline.timeout_error(
            "cluster object rpc signing",
            NetworkTimeoutPhase::Signing,
            NetworkDeliveryState::DefinitelyNotSent,
        )),
    }
}

fn cluster_target_rpc_timeout_error(
    target_node_id: &NodeId,
    timeout: Option<Duration>,
    delivery_state: NetworkDeliveryState,
    failures: &[String],
) -> NetworkError {
    let mut detail = format!(
        "timed out routing to {target_node_id} after {}ms",
        timeout.unwrap_or_default().as_millis()
    );
    if !failures.is_empty() {
        detail.push_str(": ");
        detail.push_str(&failures.join("; "));
    }
    NetworkError::Timeout {
        operation: format!("cluster target rpc to {target_node_id}: {detail}"),
        phase: NetworkTimeoutPhase::Overall,
        timeout: timeout.unwrap_or_default(),
        delivery_state,
    }
}
