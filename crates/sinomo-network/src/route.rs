//! Object RPC over the stream-native node-routing executor.

use crate::prelude::*;
use crate::{
    ClusterConnectivityGraph, ClusterPaths, DaemonRpcTimeouts, NetworkCallPolicy,
    NetworkDeliveryState, NetworkDuplexStream, NetworkError, NetworkHealthPolicy,
    NetworkTimeoutPhase, ObjectRpcCall, ObjectRpcResult,
};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

const PREPARED_ROUTE_CACHE_TTL: Duration = Duration::from_secs(1);
const MAX_STABLE_ROUTE_CAPTURE_ATTEMPTS: usize = 3;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PreparedRouteCacheKey {
    cluster_root: PathBuf,
    local_node_id: NodeId,
    directory_identity: RouteDirectoryCacheIdentity,
    directory_generation: u64,
    module_generation: u64,
    connect_timeout: Duration,
    no_progress_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RouteDirectoryCacheIdentity {
    Dynamic,
    FencedBootstrap([u8; 32]),
}

impl RouteDirectoryCacheIdentity {
    fn effective_generation(self, dynamic_generation: u64) -> u64 {
        match self {
            Self::Dynamic => dynamic_generation,
            // The full fingerprint already namespaces both prepared connectors
            // and pooled carriers. Keep the numeric generation stable across
            // unrelated Raft terms/revisions so those hot objects are not
            // needlessly churned.
            Self::FencedBootstrap(_) => 0,
        }
    }

    fn routed_stream_scope(self, cluster_id: &sinomo_node::ClusterId) -> String {
        match self {
            Self::Dynamic => cluster_id.to_string(),
            Self::FencedBootstrap(fingerprint) => format!(
                "{}:fenced-bootstrap:{}",
                cluster_id,
                blake3::Hash::from_bytes(fingerprint).to_hex()
            ),
        }
    }
}

struct PreparedRouteCacheEntry {
    connector: Arc<crate::MultiHopRoutedConnector>,
    valid_until: Instant,
}

static PREPARED_ROUTE_CACHE: LazyLock<
    Mutex<BTreeMap<PreparedRouteCacheKey, PreparedRouteCacheEntry>>,
> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn prepared_route_cache_entry<'a, T>(
    cache: &'a BTreeMap<PreparedRouteCacheKey, T>,
    key: &PreparedRouteCacheKey,
) -> Option<&'a T> {
    cache.get(key)
}

fn prepared_route_cache_get(
    key: &PreparedRouteCacheKey,
) -> Option<Arc<crate::MultiHopRoutedConnector>> {
    let now = Instant::now();
    let mut cache = PREPARED_ROUTE_CACHE.lock().ok()?;
    cache.retain(|_, entry| entry.valid_until > now);
    prepared_route_cache_entry(&cache, key).map(|entry| Arc::clone(&entry.connector))
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
                || candidate.directory_identity != key.directory_identity
                || (candidate.module_generation, candidate.directory_generation)
                    >= (key.module_generation, key.directory_generation))
    });
    cache.insert(
        key,
        PreparedRouteCacheEntry {
            connector,
            valid_until: now + PREPARED_ROUTE_CACHE_TTL,
        },
    );
}

#[cfg(any(test, feature = "test-support"))]
fn prepared_dynamic_route_cache_key_for_test(
    cluster_root: PathBuf,
    local_node_id: NodeId,
    directory_generation: u64,
    module_generation: u64,
) -> PreparedRouteCacheKey {
    PreparedRouteCacheKey {
        cluster_root,
        local_node_id,
        directory_identity: RouteDirectoryCacheIdentity::Dynamic,
        directory_generation,
        module_generation,
        connect_timeout: Duration::from_millis(251),
        no_progress_timeout: Duration::from_millis(997),
    }
}

/// Test-support seam for proving that a changed dynamic directory or local
/// module generation cannot reuse a connector already resident in the real
/// prepared-route cache.
#[cfg(any(test, feature = "test-support"))]
pub fn prepared_dynamic_route_cache_insert_for_test(
    cluster_root: PathBuf,
    local_node_id: NodeId,
    directory_generation: u64,
    module_generation: u64,
    connector: Arc<crate::MultiHopRoutedConnector>,
) {
    prepared_route_cache_insert(
        prepared_dynamic_route_cache_key_for_test(
            cluster_root,
            local_node_id,
            directory_generation,
            module_generation,
        ),
        connector,
    );
}

#[cfg(any(test, feature = "test-support"))]
pub fn prepared_dynamic_route_cache_contains_for_test(
    cluster_root: PathBuf,
    local_node_id: NodeId,
    directory_generation: u64,
    module_generation: u64,
) -> bool {
    prepared_route_cache_get(&prepared_dynamic_route_cache_key_for_test(
        cluster_root,
        local_node_id,
        directory_generation,
        module_generation,
    ))
    .is_some()
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

pub fn stream_native_service_route_constraints() -> crate::RouteConstraints {
    crate::RouteConstraints {
        // Ordinary trusted-cluster RPC is plaintext and does not require a
        // confidential adjacent link. Explicit routed-session AEAD, when
        // selected by the caller, protects the complete endpoint stream
        // without changing which reliable links may carry it.
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

/// Whether two nodes belong to the same component of established healthy,
/// full-duplex carrier relationships.
///
/// Connectivity observations remain directed because each node owns its probe
/// evidence. A healthy carrier itself is symmetric, so component consumers
/// such as convergence scheduling and diagnostics must walk either direction
/// without inventing a reverse dialable endpoint.
pub fn cluster_nodes_share_healthy_component(
    graph: &ClusterConnectivityGraph,
    left: &NodeId,
    right: &NodeId,
) -> bool {
    cluster_nodes_share_healthy_component_fallible(graph, left, right).unwrap_or(false)
}

fn cluster_nodes_share_healthy_component_fallible(
    graph: &ClusterConnectivityGraph,
    left: &NodeId,
    right: &NodeId,
) -> Result<bool, NetworkError> {
    if left == right {
        return Ok(true);
    }
    let mut seen = RouteNodeSet::default();
    seen.try_insert_clone(left, "healthy component seen nodes")?;
    let mut queue = VecDeque::new();
    queue
        .try_reserve(1)
        .map_err(|_| crate::object::network_allocation_error("healthy component queue", 1))?;
    queue.push_back(try_clone_route_node_id(left, "healthy component queue")?);
    while let Some(current) = queue.pop_front() {
        for next in graph.healthy_relationship_neighbor_refs(&current) {
            if next == right {
                return Ok(true);
            }
            if !seen.try_insert_clone(next, "healthy component seen nodes")? {
                continue;
            }
            queue.try_reserve(1).map_err(|_| {
                crate::object::network_allocation_error("healthy component queue", 1)
            })?;
            queue.push_back(try_clone_route_node_id(next, "healthy component queue")?);
        }
    }
    Ok(false)
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
    protection: crate::RoutedSessionProtection,
    destination_process_affinity: crate::DestinationProcessAffinity,
}

impl ClusterObjectRpcOptions {
    /// Override transport phases without creating a whole-call deadline.
    /// Connect and idle limits are not reinterpreted as operation deadlines;
    /// callers with a domain budget must use `with_overall_timeout`.
    pub fn new(timeouts: DaemonRpcTimeouts) -> Self {
        Self {
            timeouts,
            overall_timeout: None,
            protection: crate::RoutedSessionProtection::Plaintext,
            destination_process_affinity: crate::DestinationProcessAffinity::Any,
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
    /// The deadline covers route planning, connecting, writing, and
    /// receiving the response.
    pub fn with_overall_timeout(mut self, timeout: Duration) -> Self {
        self.overall_timeout = Some(timeout);
        self
    }

    /// Returns the caller's whole-call budget, when one was requested.
    ///
    /// Stream-native callers consume a returned body under their own domain
    /// lifetime. Compatibility callers that fold that body back into one
    /// response must use this budget around both the call and materialization.
    pub const fn overall_timeout(self) -> Option<Duration> {
        self.overall_timeout
    }

    /// Selects end-to-end routed-session protection for this call's reusable
    /// carrier. Plaintext is the default; AEAD must be requested explicitly.
    pub fn with_routed_session_protection(
        mut self,
        protection: crate::RoutedSessionProtection,
    ) -> Self {
        self.protection = protection;
        self
    }

    /// Restricts node-targeted routing to a compatible destination process.
    ///
    /// This affects only [`ClusterObjectRpcClient::call_target`]. Concrete
    /// endpoint calls are already pinned by endpoint identity.
    pub fn with_destination_process_affinity(
        mut self,
        affinity: crate::DestinationProcessAffinity,
    ) -> Self {
        self.destination_process_affinity = affinity;
        self
    }

    /// Returns the destination-process requirement for node-targeted calls.
    pub const fn destination_process_affinity(self) -> crate::DestinationProcessAffinity {
        self.destination_process_affinity
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
            protection: crate::RoutedSessionProtection::Plaintext,
            destination_process_affinity: crate::DestinationProcessAffinity::Any,
        }
    }
}

pub type ClusterObjectRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterObjectRpcObservationKind {
    Success,
    IoFailure,
    HardIoFailure,
    AuthDenied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterObjectRpcObservation {
    kind: ClusterObjectRpcObservationKind,
    latency_ms: Option<u64>,
}

impl ClusterObjectRpcObservation {
    pub fn success(latency_ms: Option<u64>) -> Self {
        Self {
            kind: ClusterObjectRpcObservationKind::Success,
            latency_ms,
        }
    }

    pub fn io_failure() -> Self {
        Self {
            kind: ClusterObjectRpcObservationKind::IoFailure,
            latency_ms: None,
        }
    }

    /// A failed command attempt that consumed its bounded response budget.
    /// Unlike a background probe miss, this should not retain a Healthy route
    /// through the single-failure grace period.
    pub fn hard_io_failure() -> Self {
        Self {
            kind: ClusterObjectRpcObservationKind::HardIoFailure,
            latency_ms: None,
        }
    }

    pub fn auth_denied() -> Self {
        Self {
            kind: ClusterObjectRpcObservationKind::AuthDenied,
            latency_ms: None,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self.kind, ClusterObjectRpcObservationKind::Success)
    }

    pub fn is_io_failure(&self) -> bool {
        matches!(
            self.kind,
            ClusterObjectRpcObservationKind::IoFailure
                | ClusterObjectRpcObservationKind::HardIoFailure
        )
    }

    pub fn is_hard_io_failure(&self) -> bool {
        matches!(self.kind, ClusterObjectRpcObservationKind::HardIoFailure)
    }

    pub fn is_auth_denied(&self) -> bool {
        matches!(self.kind, ClusterObjectRpcObservationKind::AuthDenied)
    }

    pub fn latency_ms(&self) -> Option<u64> {
        self.latency_ms
    }
}

pub trait ClusterObjectRpcAsyncHost: Send {
    fn observe_endpoint_rpc_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _observation: ClusterObjectRpcObservation,
    ) -> ClusterObjectRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Opens the transport stream for the stream-native body-aware call. Hosts
    /// may override synthetic/local transports; ordinary LAN and runtime
    /// modules use the transport-neutral registry path.
    fn open_object_rpc_stream_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        health: NetworkHealthPolicy,
    ) -> ClusterObjectRpcFuture<'a, Box<dyn NetworkDuplexStream>> {
        Box::pin(async move {
            crate::roaming::open_daemon_rpc_stream_to_cluster_endpoint_async(endpoint, health).await
        })
    }

    /// Dispatches one node-addressed call through the host's service boundary.
    ///
    /// The default remains the fenced routed carrier. Daemon hosts override
    /// this seam for the one node that a routed client deliberately cannot
    /// address: the local node, whose exact main-daemon listener must be
    /// selected explicitly rather than inferred from cluster identity.
    fn call_node_object_rpc_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
    ) -> ClusterObjectRpcFuture<'a, ObjectRpcResult>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let mut client =
                ClusterObjectRpcClient::routed_from_fenced_bootstrap_state(self, paths, state)?;
            client
                .call_target(state, target_node_id, call, options)
                .await
        })
    }
}

fn cluster_endpoint_rpc_observation(
    result: Result<&ObjectRpcResult, &NetworkError>,
    started: Instant,
) -> Option<ClusterObjectRpcObservation> {
    match result {
        // Completing an authenticated stream proves that the immediate endpoint is
        // reachable even when the application response denies the requested operation.
        // Session/link authentication failures arrive through the `Err` branch below.
        Ok(_) => Some(ClusterObjectRpcObservation::success(Some(
            cluster_endpoint_rpc_elapsed_latency_ms(started),
        ))),
        Err(error) => cluster_endpoint_rpc_error_observation(error),
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

fn cluster_endpoint_rpc_error_observation(
    error: &NetworkError,
) -> Option<ClusterObjectRpcObservation> {
    match error {
        NetworkError::Io { .. }
        | NetworkError::Timeout { .. }
        | NetworkError::DeliveryFailed { .. } => Some(ClusterObjectRpcObservation::io_failure()),
        NetworkError::AuthDenied => Some(ClusterObjectRpcObservation::auth_denied()),
        _ => None,
    }
}

fn cluster_endpoint_rpc_elapsed_latency_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(Clone)]
struct ClusterObjectRpcRouteContext<'a> {
    paths: &'a ClusterPaths,
    directory: ClusterObjectRpcRouteDirectory,
}

#[derive(Clone)]
enum ClusterObjectRpcRouteDirectory {
    Dynamic,
    FencedBootstrap {
        directory: Arc<dyn crate::ClusterDirectory>,
        cluster_id: sinomo_node::ClusterId,
        local_node_id: NodeId,
        fingerprint: [u8; 32],
    },
}

fn update_fenced_route_fingerprint_field(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn update_fenced_route_fingerprint_value<T: serde::Serialize>(
    hasher: &mut blake3::Hasher,
    value: &T,
) -> Result<(), NetworkError> {
    let encoded = serde_json::to_vec(value).map_err(|source| NetworkError::Serialization {
        detail: format!("serialize fenced bootstrap routing field: {source}"),
    })?;
    update_fenced_route_fingerprint_field(hasher, &encoded);
    Ok(())
}

fn fenced_bootstrap_endpoint_fingerprint(
    endpoint: &ClusterEndpointRecord,
) -> Result<[u8; 32], NetworkError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.fenced-bootstrap.endpoint.v1");
    for value in [
        endpoint.endpoint_id.as_str(),
        endpoint.node_id.as_str(),
        endpoint.address.as_str(),
    ] {
        update_fenced_route_fingerprint_field(&mut hasher, value.as_bytes());
    }
    update_fenced_route_fingerprint_value(&mut hasher, &endpoint.transport)?;
    update_fenced_route_fingerprint_value(&mut hasher, &endpoint.source)?;
    update_fenced_route_fingerprint_value(&mut hasher, &endpoint.privacy)?;
    hasher.update(&endpoint.priority.to_le_bytes());
    // `last_observed_latency_ms` is transient observation data and is not
    // consumed by the immutable directory route planner. It must not rotate a
    // fenced carrier scope.
    Ok(*hasher.finalize().as_bytes())
}

fn fenced_bootstrap_member_fingerprint(
    member: &sinomo_base::ClusterMemberRecord,
) -> Result<[u8; 32], NetworkError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.fenced-bootstrap.member.v1");
    for value in [
        member.cluster_id.as_str(),
        member.node_id.as_str(),
        member.node_public_key.as_str(),
    ] {
        update_fenced_route_fingerprint_field(&mut hasher, value.as_bytes());
    }
    update_fenced_route_fingerprint_value(&mut hasher, &member.node_role)?;
    update_fenced_route_fingerprint_value(&mut hasher, &member.role)?;
    update_fenced_route_fingerprint_value(&mut hasher, &member.state)?;

    let mut capabilities = member
        .capabilities
        .iter()
        .map(|capability| capability.as_str())
        .collect::<Vec<_>>();
    capabilities.sort_unstable();
    hasher.update(&(capabilities.len() as u64).to_le_bytes());
    for capability in capabilities {
        update_fenced_route_fingerprint_field(&mut hasher, capability.as_bytes());
    }
    let mut allowed_capabilities = member
        .allowed_capabilities
        .iter()
        .map(|capability| capability.as_str())
        .collect::<Vec<_>>();
    allowed_capabilities.sort_unstable();
    hasher.update(&(allowed_capabilities.len() as u64).to_le_bytes());
    for capability in allowed_capabilities {
        update_fenced_route_fingerprint_field(&mut hasher, capability.as_bytes());
    }

    let mut endpoints = member
        .endpoints
        .iter()
        .map(fenced_bootstrap_endpoint_fingerprint)
        .collect::<Result<Vec<_>, _>>()?;
    endpoints.sort_unstable();
    hasher.update(&(endpoints.len() as u64).to_le_bytes());
    for endpoint in endpoints {
        hasher.update(&endpoint);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn fenced_bootstrap_route_fingerprint(
    state: &ClusterBootstrapState,
) -> Result<[u8; 32], NetworkError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.fenced-bootstrap.route.v1");
    for value in [state.cluster_id.as_str(), state.local_node_id.as_str()] {
        update_fenced_route_fingerprint_field(&mut hasher, value.as_bytes());
    }
    let mut members = state
        .members
        .iter()
        .filter(|member| member.state.is_authorized())
        .map(fenced_bootstrap_member_fingerprint)
        .collect::<Result<Vec<_>, _>>()?;
    members.sort_unstable();
    hasher.update(&(members.len() as u64).to_le_bytes());
    for member in members {
        hasher.update(&member);
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Returns the exact pool scopes under which this process may reuse one
/// accepted full-duplex cluster carrier.
///
/// The dynamic scope serves ordinary cluster routing. The second scope is
/// fenced to the caller's authoritative bootstrap routing inputs, including
/// the local node identity, and is the only alias consensus/KV traffic may
/// use. Callers must not synthesize a broader prefix match from these values.
pub fn cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(
    state: &ClusterBootstrapState,
) -> Result<[String; 2], NetworkError> {
    Ok([
        RouteDirectoryCacheIdentity::Dynamic.routed_stream_scope(&state.cluster_id),
        RouteDirectoryCacheIdentity::FencedBootstrap(fenced_bootstrap_route_fingerprint(state)?)
            .routed_stream_scope(&state.cluster_id),
    ])
}

impl ClusterObjectRpcRouteContext<'_> {
    fn directory(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<
        (
            Arc<dyn crate::ClusterDirectory>,
            RouteDirectoryCacheIdentity,
        ),
        NetworkError,
    > {
        match &self.directory {
            ClusterObjectRpcRouteDirectory::Dynamic => Ok((
                crate::cluster_directory_with_bootstrap_state(
                    crate::cluster_directory_for(self.paths),
                    state,
                ),
                RouteDirectoryCacheIdentity::Dynamic,
            )),
            ClusterObjectRpcRouteDirectory::FencedBootstrap {
                directory,
                cluster_id,
                local_node_id,
                fingerprint,
            } => {
                if cluster_id != &state.cluster_id
                    || local_node_id != &state.local_node_id
                    || *fingerprint != fenced_bootstrap_route_fingerprint(state)?
                {
                    return Err(NetworkError::Directory {
                        detail:
                            "fenced bootstrap route context does not match the call's cluster state"
                                .to_string(),
                    });
                }
                Ok((
                    Arc::clone(directory),
                    RouteDirectoryCacheIdentity::FencedBootstrap(*fingerprint),
                ))
            }
        }
    }
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

impl<'a, H: ClusterObjectRpcAsyncHost> ClusterObjectRpcClient<'a, H> {
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
            route_context: Some(ClusterObjectRpcRouteContext {
                paths,
                directory: ClusterObjectRpcRouteDirectory::Dynamic,
            }),
        }
    }

    /// Build a routed client from one already-authoritative bootstrap-state
    /// fence, without consulting the process-wide cluster directory.
    ///
    /// This path is for consensus and state-authority operations whose caller
    /// already carries an accepted voter/member view (for example Raft control
    /// messages, ReadIndex probes, and authoritative KV forwarding). Ordinary
    /// member/application routing without such a fence should use
    /// [`Self::routed`] so it sees the reactive KV-backed projection.
    pub fn routed_from_fenced_bootstrap_state(
        host: &'a mut H,
        paths: &'a ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Self, NetworkError> {
        Self::routed_from_fenced_bootstrap_state_inner(host, paths, state, None)
    }

    /// Build a routed client whose authorization and endpoints remain fenced
    /// to `state`, while route planning may use connectivity evidence from a
    /// locally observed directory projection.
    ///
    /// The evidence provider is queried only while capturing a cold prepared
    /// connector. Its members and endpoints are ignored, so a newer or stale
    /// projection cannot widen the accepted Raft/KV membership fence.
    pub fn routed_from_fenced_bootstrap_state_with_connectivity(
        host: &'a mut H,
        paths: &'a ClusterPaths,
        state: &ClusterBootstrapState,
        connectivity: Arc<dyn crate::ClusterDirectory>,
    ) -> Result<Self, NetworkError> {
        Self::routed_from_fenced_bootstrap_state_inner(host, paths, state, Some(connectivity))
    }

    fn routed_from_fenced_bootstrap_state_inner(
        host: &'a mut H,
        paths: &'a ClusterPaths,
        state: &ClusterBootstrapState,
        connectivity: Option<Arc<dyn crate::ClusterDirectory>>,
    ) -> Result<Self, NetworkError> {
        let fingerprint = fenced_bootstrap_route_fingerprint(state)?;
        let directory = match connectivity {
            Some(connectivity) => {
                crate::directory::cluster_directory_from_fenced_bootstrap_state_with_connectivity(
                    state,
                    connectivity,
                )
            }
            None => crate::directory::cluster_directory_from_fenced_bootstrap_state(state),
        };
        Ok(Self {
            host,
            route_context: Some(ClusterObjectRpcRouteContext {
                paths,
                directory: ClusterObjectRpcRouteDirectory::FencedBootstrap {
                    directory,
                    cluster_id: state.cluster_id.clone(),
                    local_node_id: state.local_node_id.clone(),
                    fingerprint,
                },
            }),
        })
    }
}

impl<'a, H> ClusterObjectRpcClient<'a, H>
where
    H: ClusterObjectRpcAsyncHost + Send,
{
    /// Runs one body-aware RPC call against a concrete endpoint.
    ///
    /// The transport authenticates the endpoint while opening the reusable
    /// carrier. It then owns chunking, flow control, health checks, and
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
        let deadline = ClusterEndpointRpcDeadline::new(options.overall_timeout());
        let timeout_error = deadline.timeout_error(
            "cluster endpoint object RPC",
            NetworkTimeoutPhase::Overall,
            NetworkDeliveryState::PossiblyApplied,
        );
        let result = cluster_object_rpc_call_with_deadline(
            Box::pin(self.call_endpoint_inner(state, endpoint, call, options, deadline)),
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
        let data_dir =
            cluster_paths_data_dir(self.route_context.as_ref().map(|context| context.paths))?;
        let policy = options.call_policy();
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
        let stream = crate::open_pooled_routed_stream_with_protection(
            format!(
                "{}:direct-endpoint:{}",
                state.cluster_id,
                endpoint_pool_identity.finalize().to_hex()
            ),
            directory.version().0,
            constraints,
            state.local_node_id.clone(),
            endpoint.node_id.clone(),
            options.protection,
            connect_timeout,
            crate::connect_direct_routed_endpoint(
                &directory,
                endpoint,
                &provider,
                identity.as_ref(),
                crate::ConnectOptions {
                    constraints,
                    timeout: connect_timeout,
                    protection: options.protection,
                },
            ),
        )
        .await;
        let mut authenticated_stream_established = false;
        let result = match stream {
            Ok(stream) => {
                authenticated_stream_established = true;
                crate::service_rpc_stream::call_service_rpc_peer_stream(
                    stream,
                    call,
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
            Some(ClusterObjectRpcObservation::success(Some(
                cluster_endpoint_rpc_elapsed_latency_ms(started),
            )))
        } else {
            cluster_endpoint_rpc_observation(result.as_ref(), started)
        };
        if let Some(observation) = observation {
            let _ = self
                .host
                .observe_endpoint_rpc_async(endpoint, observation)
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
        let deadline = ClusterEndpointRpcDeadline::new(options.overall_timeout());
        let timeout_error = deadline.timeout_error(
            "local object RPC",
            NetworkTimeoutPhase::Overall,
            NetworkDeliveryState::PossiblyApplied,
        );
        let result = cluster_object_rpc_call_with_deadline(
            Box::pin(self.call_loopback_inner(data_dir, address, call, options, deadline)),
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
        let result = crate::service_rpc_stream::call_service_rpc_peer_stream(
            stream,
            call,
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
        let Some(context) = self.route_context.clone() else {
            return Err(NetworkError::Io {
                operation: "cluster target object RPC call".to_string(),
                detail: "cluster target RPC call requires a route context".to_string(),
            });
        };
        let (directory, directory_identity) = context.directory(state)?;
        let route_timeout = options.overall_timeout();
        let deadline = ClusterEndpointRpcDeadline::new(route_timeout);
        let timeout_error = cluster_target_rpc_timeout_error(
            target_node_id,
            route_timeout,
            NetworkDeliveryState::PossiblyApplied,
            &[],
        );
        prepare_call_for_active_network_timeline(&mut call);
        cluster_object_rpc_call_with_deadline(
            Box::pin(self.call_target_stream_native(
                context.paths,
                directory,
                directory_identity,
                state,
                target_node_id,
                call,
                options,
                deadline,
            )),
            deadline,
            timeout_error,
        )
        .await
    }

    async fn call_target_stream_native(
        &mut self,
        paths: &ClusterPaths,
        directory: Arc<dyn crate::ClusterDirectory>,
        directory_identity: RouteDirectoryCacheIdentity,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        deadline: ClusterEndpointRpcDeadline,
    ) -> Result<ObjectRpcResult, NetworkError> {
        if target_node_id == &state.local_node_id {
            return Err(NetworkError::Io {
                operation: "establish routed service RPC stream".to_string(),
                detail: format!(
                    "local-node calls require the explicit local service path for route {} in category {}",
                    call.request.route, call.request.category,
                ),
            });
        }
        let policy = options.call_policy();
        let trace = crate::network_timeline_trace_context_for_request(&call.request);
        let snapshot_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let prepare_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let mut route_snapshot_succeeded = false;
        let prepared = (|| {
            for _ in 0..MAX_STABLE_ROUTE_CAPTURE_ATTEMPTS {
                // The module registry is a process-local route-realization
                // input, independent of the distributed directory revision.
                // Fence it around both cache lookup and snapshot construction
                // so a connector can never combine a pre-registry directory
                // capture with post-registry planning.
                let module_generation = crate::network_module_routing_generation();
                let routing_version = directory.routing_version()?;
                route_snapshot_succeeded = true;
                if crate::network_module_routing_generation() != module_generation {
                    continue;
                }
                let cache_generation = directory_identity.effective_generation(routing_version.0);
                let initial_cache_key = PreparedRouteCacheKey {
                    cluster_root: paths.root.clone(),
                    local_node_id: state.local_node_id.clone(),
                    directory_identity,
                    directory_generation: cache_generation,
                    module_generation,
                    connect_timeout: policy.health.connect_timeout,
                    no_progress_timeout: policy.health.no_progress_timeout,
                };
                if let Some(connector) = prepared_route_cache_get(&initial_cache_key) {
                    if crate::network_module_routing_generation() == module_generation {
                        return Ok((connector, cache_generation, module_generation));
                    }
                    continue;
                }

                let (directory_version, members, endpoints, directory_connectivity) =
                    directory.routing_records_snapshot()?.into_parts();
                let captured_directory_generation =
                    directory_identity.effective_generation(directory_version.0);
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
                if crate::network_module_routing_generation() != module_generation {
                    continue;
                }
                let cache_key = PreparedRouteCacheKey {
                    directory_generation: captured_directory_generation,
                    ..initial_cache_key
                };
                prepared_route_cache_insert(cache_key, Arc::clone(&connector));
                if crate::network_module_routing_generation() == module_generation {
                    return Ok((connector, captured_directory_generation, module_generation));
                }
            }
            Err(NetworkError::Directory {
                detail:
                    "network module registry changed repeatedly while capturing a routed connector"
                        .to_string(),
            })
        })();
        record_routed_rpc_timeline_stage(
            trace,
            crate::network_timeline_stage::ROUTE_SNAPSHOT,
            snapshot_started_ns,
            route_snapshot_succeeded,
        );
        record_routed_rpc_timeline_stage(
            trace,
            crate::network_timeline_stage::ROUTE_PREPARE,
            prepare_started_ns,
            prepared.is_ok(),
        );
        let (connector, directory_generation, module_generation) = prepared?;
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
        let destination_process_affinity = options.destination_process_affinity;
        let open_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        if crate::network_module_routing_generation() != module_generation {
            return Err(NetworkError::Directory {
                detail: "network module registry changed after capturing a routed connector"
                    .to_string(),
            });
        }
        let stream_result = crate::routed_stream_pool::open_pooled_routed_stream_with_protection_destination_affinity_and_route_compatibility(
            directory_identity.routed_stream_scope(&state.cluster_id),
            module_generation,
            directory_generation,
            constraints,
            destination_process_affinity,
            state.local_node_id.clone(),
            target_node_id.clone(),
            options.protection,
            connect_timeout,
            connector.connect_with_destination_process_affinity(
                target_node_id.clone(),
                crate::ConnectOptions {
                    constraints,
                    timeout: connect_timeout,
                    protection: options.protection,
                },
                destination_process_affinity,
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
        let exchange_started_ns = trace
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let result = crate::service_rpc_stream::call_service_rpc_peer_stream(
            stream,
            call,
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

// Keep the concrete routed-call future behind a pointer before composing it
// with Tokio's timeout future. In unoptimized builds the stream-native RPC
// future is large; a generic wrapper would embed that state in its own future
// and amplify the synchronous caller's poll stack.
fn cluster_object_rpc_call_with_deadline<'a, T: 'a>(
    call: ClusterObjectRpcFuture<'a, T>,
    deadline: ClusterEndpointRpcDeadline,
    timeout_error: NetworkError,
) -> ClusterObjectRpcFuture<'a, T> {
    let Some(deadline_at) = deadline.deadline_at else {
        return call;
    };
    Box::pin(async move {
        match tokio::time::timeout_at(deadline_at, call).await {
            Ok(result) => result,
            Err(_) => Err(timeout_error),
        }
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectRpcRequest;

    #[test]
    fn deadline_wrapper_erases_oversized_concrete_call_state() {
        const OVERSIZED_STATE_BYTES: usize = 128 * 1024;
        let oversized_state = [0_u8; OVERSIZED_STATE_BYTES];
        let concrete_call = async move {
            std::hint::black_box(&oversized_state);
            Ok::<usize, NetworkError>(oversized_state.len())
        };
        assert!(std::mem::size_of_val(&concrete_call) >= OVERSIZED_STATE_BYTES);

        let deadline = ClusterEndpointRpcDeadline::new(Some(Duration::from_secs(1)));
        let wrapped = cluster_object_rpc_call_with_deadline(
            Box::pin(concrete_call),
            deadline,
            deadline.timeout_error(
                "oversized call fixture",
                NetworkTimeoutPhase::Overall,
                NetworkDeliveryState::PossiblyApplied,
            ),
        );

        assert_eq!(
            std::mem::size_of_val(&wrapped),
            std::mem::size_of::<ClusterObjectRpcFuture<'static, usize>>()
        );
        assert_eq!(
            std::mem::size_of_val(&wrapped),
            2 * std::mem::size_of::<usize>()
        );
    }

    struct NoopObjectRpcHost;

    impl ClusterObjectRpcAsyncHost for NoopObjectRpcHost {}

    struct PanickingRouteDirectory;

    impl crate::ClusterDirectory for PanickingRouteDirectory {
        fn members(&self) -> Result<Vec<sinomo_base::ClusterMemberRecord>, NetworkError> {
            panic!("registered dynamic directory must not serve a fenced route")
        }

        fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            panic!("registered dynamic directory must not serve a fenced route")
        }

        fn node_statuses(
            &self,
        ) -> Result<BTreeMap<NodeId, sinomo_base::ClusterNodeStatusRecord>, NetworkError> {
            panic!("registered dynamic directory must not serve a fenced route")
        }

        fn connectivity_records(
            &self,
        ) -> Result<Vec<crate::ClusterConnectivityRecord>, NetworkError> {
            panic!("registered dynamic directory must not serve a fenced route")
        }

        fn version(&self) -> Result<crate::DirectoryVersion, NetworkError> {
            panic!("registered dynamic directory must not serve a fenced route")
        }
    }

    #[derive(Clone)]
    struct ConnectivityOnlyRouteDirectory(crate::DirectoryRoutingRecordsSnapshot);

    impl crate::ClusterDirectory for ConnectivityOnlyRouteDirectory {
        fn members(&self) -> Result<Vec<sinomo_base::ClusterMemberRecord>, NetworkError> {
            panic!("fenced connectivity evidence must not supply members")
        }

        fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            panic!("fenced connectivity evidence must not supply endpoints")
        }

        fn node_statuses(
            &self,
        ) -> Result<BTreeMap<NodeId, sinomo_base::ClusterNodeStatusRecord>, NetworkError> {
            panic!("fenced connectivity evidence must not supply statuses")
        }

        fn connectivity_records(
            &self,
        ) -> Result<Vec<crate::ClusterConnectivityRecord>, NetworkError> {
            panic!("fenced connectivity evidence must use one routing snapshot")
        }

        fn version(&self) -> Result<crate::DirectoryVersion, NetworkError> {
            panic!("fenced connectivity evidence must not supply a generation")
        }

        fn routing_records_snapshot(
            &self,
        ) -> Result<crate::DirectoryRoutingRecordsSnapshot, NetworkError> {
            Ok(self.0.clone())
        }
    }

    struct RegisteredDirectoryActivation(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for RegisteredDirectoryActivation {
        fn drop(&mut self) {
            self.0.store(false, std::sync::atomic::Ordering::Release);
        }
    }

    #[test]
    fn fenced_bootstrap_route_never_consults_registered_directory() {
        let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let _activation = RegisteredDirectoryActivation(Arc::clone(&active));
        let owner_thread = std::thread::current().id();
        crate::set_cluster_directory_provider(Arc::new(move |_| {
            if active.load(std::sync::atomic::Ordering::Acquire)
                && std::thread::current().id() == owner_thread
            {
                Arc::new(PanickingRouteDirectory)
            } else {
                Arc::new(crate::EmptyClusterDirectory)
            }
        }));

        let cluster_id = sinomo_base::Id::from_legacy_named("fenced-route-provider-cluster")
            .expect("cluster id");
        let local_node_id = sinomo_base::Id::from_legacy_named("fenced-route-provider-local")
            .expect("local node id");
        let target_node_id = sinomo_base::Id::from_legacy_named("fenced-route-provider-target")
            .expect("target node id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "fenced-route-provider",
            local_node_id,
            "local",
            "local-public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        let mut target = sinomo_base::ClusterMemberRecord::local_voter(
            cluster_id,
            target_node_id.clone(),
            "target",
            "target-public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        let target_endpoint =
            ClusterEndpointRecord::loopback(target_node_id.clone(), "127.0.0.1:45679");
        target.endpoints.push(target_endpoint.clone());
        state.members.push(target);
        state.kv_revision = 41;

        let root = PathBuf::from("/test/fenced-route-provider");
        let paths = ClusterPaths {
            state: root.join("state.sobj"),
            connectivity_local: root.join("connectivity-local.sobj"),
            root,
        };
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::cluster_directory_for(&paths)
                    .routing_version()
                    .expect("registered dynamic directory version")
            }))
            .is_err(),
            "the fixture must install a provider whose directory methods panic"
        );

        let mut host = NoopObjectRpcHost;
        let client =
            ClusterObjectRpcClient::routed_from_fenced_bootstrap_state(&mut host, &paths, &state)
                .expect("fenced routed client");
        let context = client.route_context.expect("fenced route context");
        let (directory, identity) = context.directory(&state).expect("fenced route directory");
        assert!(matches!(
            identity,
            RouteDirectoryCacheIdentity::FencedBootstrap(_)
        ));
        let (version, members, endpoints, connectivity) = directory
            .routing_records_snapshot()
            .expect("fenced routing snapshot must bypass registered provider")
            .into_parts();
        assert!(connectivity.is_empty());
        let snapshot = crate::DirectorySnapshot::try_from_records(version, members, endpoints)
            .expect("fenced directory snapshot");
        assert_eq!(
            snapshot
                .node(&target_node_id)
                .expect("fenced target member")
                .endpoints,
            vec![target_endpoint]
        );
    }

    #[test]
    fn fenced_bootstrap_route_imports_only_connectivity_from_observed_directory() {
        let cluster_id =
            sinomo_base::Id::from_legacy_named("fenced-connectivity-cluster").expect("cluster id");
        let local_node_id =
            sinomo_base::Id::from_legacy_named("fenced-connectivity-local").expect("local node");
        let target_node_id =
            sinomo_base::Id::from_legacy_named("fenced-connectivity-target").expect("target node");
        let unaccepted_node_id =
            sinomo_base::Id::from_legacy_named("fenced-connectivity-unaccepted")
                .expect("unaccepted node");
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "fenced-connectivity",
            local_node_id.clone(),
            "local",
            "local-public-key",
            now,
        );
        let mut target = sinomo_base::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            target_node_id.clone(),
            "target",
            "target-public-key",
            now,
        );
        let target_endpoint =
            ClusterEndpointRecord::loopback(target_node_id.clone(), "127.0.0.1:45680");
        target.endpoints.push(target_endpoint.clone());
        state.members.push(target);
        state.kv_revision = 53;

        let mut unaccepted = sinomo_base::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            unaccepted_node_id.clone(),
            "unaccepted",
            "unaccepted-public-key",
            now,
        );
        let unaccepted_endpoint =
            ClusterEndpointRecord::loopback(unaccepted_node_id.clone(), "127.0.0.1:45681");
        unaccepted.endpoints.push(unaccepted_endpoint.clone());
        let connectivity = crate::ClusterConnectivityRecord {
            cluster_id,
            source_node_id: local_node_id,
            target_node_id: target_node_id.clone(),
            endpoint_id: target_endpoint.endpoint_id.clone(),
            transport: target_endpoint.transport.clone(),
            address: target_endpoint.address.clone(),
            state: crate::ClusterConnectivityState::Healthy,
            latency_ms: Some(2),
            consecutive_failures: 0,
            last_success_at: None,
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::minutes(15),
        };
        let observed = Arc::new(ConnectivityOnlyRouteDirectory(
            crate::DirectoryRoutingRecordsSnapshot::new(
                crate::DirectoryVersion(91),
                vec![unaccepted],
                vec![unaccepted_endpoint],
                vec![connectivity.clone()],
            ),
        ));

        let root = PathBuf::from("/test/fenced-connectivity-provider");
        let paths = ClusterPaths {
            state: root.join("state.sobj"),
            connectivity_local: root.join("connectivity-local.sobj"),
            root,
        };
        let mut host = NoopObjectRpcHost;
        let client = ClusterObjectRpcClient::routed_from_fenced_bootstrap_state_with_connectivity(
            &mut host, &paths, &state, observed,
        )
        .expect("fenced routed client with connectivity");
        let context = client.route_context.expect("fenced route context");
        let (directory, _) = context.directory(&state).expect("fenced route directory");
        assert_eq!(directory.routing_version(), Ok(crate::DirectoryVersion(53)));
        let (version, members, endpoints, connectivity_records) = directory
            .routing_records_snapshot()
            .expect("fenced routing snapshot")
            .into_parts();
        let snapshot = crate::DirectorySnapshot::try_from_records(version, members, endpoints)
            .expect("exact fenced directory snapshot");
        assert!(snapshot.node(&target_node_id).is_some());
        assert!(
            snapshot.node(&unaccepted_node_id).is_none(),
            "connectivity evidence must not widen the accepted member fence"
        );
        assert_eq!(connectivity_records, vec![connectivity]);
    }

    #[test]
    fn fenced_route_cache_rejects_colliding_dynamic_generation() {
        let common = |directory_identity| PreparedRouteCacheKey {
            cluster_root: PathBuf::from("/test/fenced-route-cache"),
            local_node_id: NodeId::from_legacy_named("fenced-route-cache-local")
                .expect("local node id"),
            directory_identity,
            directory_generation: 41,
            module_generation: 7,
            connect_timeout: Duration::from_millis(250),
            no_progress_timeout: Duration::from_secs(1),
        };
        let dynamic_key = common(RouteDirectoryCacheIdentity::Dynamic);
        let fenced_key = common(RouteDirectoryCacheIdentity::FencedBootstrap([0x5a; 32]));
        let mut cache = BTreeMap::new();
        cache.insert(dynamic_key, "dynamic connector");

        assert!(prepared_route_cache_entry(&cache, &fenced_key).is_none());
        assert_ne!(
            RouteDirectoryCacheIdentity::Dynamic.routed_stream_scope(
                &sinomo_node::ClusterId::from_legacy_named("fenced-route-cache-cluster")
                    .expect("cluster id")
            ),
            RouteDirectoryCacheIdentity::FencedBootstrap([0x5a; 32]).routed_stream_scope(
                &sinomo_node::ClusterId::from_legacy_named("fenced-route-cache-cluster")
                    .expect("cluster id")
            ),
            "a fenced call must not reuse a dynamic pooled carrier when numeric generations collide"
        );
    }

    #[test]
    fn prepared_route_cache_rejects_a_module_transition_at_the_same_directory_generation() {
        let key = |module_generation| PreparedRouteCacheKey {
            cluster_root: PathBuf::from("/test/module-route-cache"),
            local_node_id: NodeId::from_legacy_named("module-route-cache-local")
                .expect("local node id"),
            directory_identity: RouteDirectoryCacheIdentity::Dynamic,
            directory_generation: 41,
            module_generation,
            connect_timeout: Duration::from_millis(250),
            no_progress_timeout: Duration::from_secs(1),
        };
        let mut cache = BTreeMap::new();
        cache.insert(key(7), "pre-transition connector");

        assert!(
            prepared_route_cache_entry(&cache, &key(8)).is_none(),
            "a registry change between routing_version and cache lookup must not hit the old connector"
        );
    }

    #[test]
    fn fenced_route_identity_tracks_only_canonical_routing_projection() {
        let cluster_id = sinomo_base::Id::from_legacy_named("fenced-route-identity-cluster")
            .expect("cluster id");
        let local_node_id = sinomo_base::Id::from_legacy_named("fenced-route-identity-local")
            .expect("local node id");
        let target_node_id = sinomo_base::Id::from_legacy_named("fenced-route-identity-target")
            .expect("target node id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "fenced-route-identity",
            local_node_id.clone(),
            "local",
            "local-public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        let mut target = sinomo_base::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            target_node_id.clone(),
            "target",
            "target-public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        target.capabilities = vec![
            sinomo_base::CapabilityName::new("service.write").expect("write capability"),
            sinomo_base::CapabilityName::new("service.read").expect("read capability"),
        ];
        target.allowed_capabilities = vec![
            sinomo_base::CapabilityName::new("route.relay").expect("relay capability"),
            sinomo_base::CapabilityName::new("route.direct").expect("direct capability"),
        ];
        target.endpoints = vec![
            ClusterEndpointRecord::loopback(target_node_id.clone(), "127.0.0.1:45681"),
            ClusterEndpointRecord::loopback(target_node_id.clone(), "127.0.0.1:45680"),
        ];
        state.members.push(target);
        state.kv_revision = 7;
        let stable_fingerprint = fenced_bootstrap_route_fingerprint(&state).expect("fingerprint");

        let mut non_routing_update = state.clone();
        non_routing_update.cluster_name = "renamed-display-only".to_string();
        non_routing_update.local_node_display_name = "renamed-local".to_string();
        non_routing_update.term = 99;
        non_routing_update.kv_revision = 10_000;
        non_routing_update.initialized_at += time::Duration::days(1);
        non_routing_update.updated_at += time::Duration::days(2);
        for member in &mut non_routing_update.members {
            member.display_name.push_str("-renamed");
            member.joined_at += time::Duration::hours(1);
            member.updated_at += time::Duration::hours(2);
            member.capabilities.reverse();
            member.allowed_capabilities.reverse();
            member.endpoints.reverse();
            for endpoint in &mut member.endpoints {
                endpoint.last_observed_latency_ms = Some(42);
            }
        }
        non_routing_update.members.reverse();
        let pending = sinomo_base::ClusterMemberRecord::pending_learner(
            cluster_id.clone(),
            sinomo_base::Id::from_legacy_named("fenced-route-identity-pending")
                .expect("pending node id"),
            "pending",
            "pending-public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        non_routing_update.members.push(pending);

        let stable_after_non_routing = fenced_bootstrap_route_fingerprint(&non_routing_update)
            .expect("non-routing fingerprint");
        assert_eq!(stable_after_non_routing, stable_fingerprint);
        let original_identity = RouteDirectoryCacheIdentity::FencedBootstrap(stable_fingerprint);
        let updated_identity =
            RouteDirectoryCacheIdentity::FencedBootstrap(stable_after_non_routing);
        let [dynamic_scope, fenced_scope] =
            cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(&state)
                .expect("accepted carrier scopes");
        assert_eq!(
            dynamic_scope,
            RouteDirectoryCacheIdentity::Dynamic.routed_stream_scope(&cluster_id)
        );
        assert_eq!(
            fenced_scope,
            original_identity.routed_stream_scope(&cluster_id),
            "the accepted-carrier alias must exactly match the fenced caller scope"
        );
        let mut different_receiver = state.clone();
        different_receiver.local_node_id = target_node_id.clone();
        let [_, different_receiver_scope] =
            cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(&different_receiver)
                .expect("different receiver scopes");
        assert_ne!(
            different_receiver_scope, fenced_scope,
            "a receiver-local fence must not be reusable by another node identity"
        );
        assert_eq!(
            original_identity.routed_stream_scope(&cluster_id),
            updated_identity.routed_stream_scope(&cluster_id)
        );
        assert_eq!(
            original_identity.effective_generation(state.kv_revision),
            updated_identity.effective_generation(non_routing_update.kv_revision)
        );
        let cache_key = |state: &ClusterBootstrapState,
                         directory_identity: RouteDirectoryCacheIdentity| {
            PreparedRouteCacheKey {
                cluster_root: PathBuf::from("/test/fenced-route-stable-cardinality"),
                local_node_id: state.local_node_id.clone(),
                directory_identity,
                directory_generation: directory_identity.effective_generation(state.kv_revision),
                module_generation: 7,
                connect_timeout: Duration::from_millis(250),
                no_progress_timeout: Duration::from_secs(1),
            }
        };
        let mut cache = BTreeMap::new();
        cache.insert(cache_key(&state, original_identity), "original connector");
        cache.insert(
            cache_key(&non_routing_update, updated_identity),
            "reused connector",
        );
        assert_eq!(
            cache.len(),
            1,
            "non-routing churn must retain one cache key"
        );

        let mut endpoint_changed = state.clone();
        endpoint_changed
            .members
            .iter_mut()
            .find(|member| member.node_id == target_node_id)
            .expect("target member")
            .endpoints[0]
            .address = "127.0.0.1:55681".to_string();
        assert_ne!(
            fenced_bootstrap_route_fingerprint(&endpoint_changed).expect("endpoint fingerprint"),
            stable_fingerprint
        );

        let mut key_changed = state.clone();
        key_changed
            .members
            .iter_mut()
            .find(|member| member.node_id == target_node_id)
            .expect("target member")
            .node_public_key = "rotated-target-public-key".to_string();
        assert_ne!(
            fenced_bootstrap_route_fingerprint(&key_changed).expect("key fingerprint"),
            stable_fingerprint
        );

        let mut config_changed = state.clone();
        config_changed
            .members
            .iter_mut()
            .find(|member| member.node_id == target_node_id)
            .expect("target member")
            .role = sinomo_base::ClusterMemberRole::Learner;
        assert_ne!(
            fenced_bootstrap_route_fingerprint(&config_changed).expect("config fingerprint"),
            stable_fingerprint
        );
    }

    #[test]
    fn cluster_rpc_protection_is_plaintext_unless_explicitly_enabled() {
        let defaults = ClusterObjectRpcOptions::default();
        assert_eq!(
            defaults.protection,
            crate::RoutedSessionProtection::Plaintext
        );
        assert_eq!(
            defaults
                .with_routed_session_protection(crate::RoutedSessionProtection::Aead)
                .protection,
            crate::RoutedSessionProtection::Aead
        );
    }

    #[test]
    fn cluster_rpc_destination_process_affinity_is_explicit() {
        let defaults = ClusterObjectRpcOptions::default();
        assert_eq!(
            defaults.destination_process_affinity,
            crate::DestinationProcessAffinity::Any
        );
        assert_eq!(
            defaults
                .with_destination_process_affinity(crate::DestinationProcessAffinity::MainDaemon,)
                .destination_process_affinity,
            crate::DestinationProcessAffinity::MainDaemon
        );
    }

    #[test]
    fn active_timeline_can_annotate_forwarded_streamed_value_metadata() {
        let _capture_guard = crate::timeline::network_timeline_test_capture_guard();
        if crate::network_timeline_capture_is_active() {
            let _ = crate::stop_network_timeline_capture();
        }
        crate::start_network_timeline_capture(8).expect("start timeline capture");

        let mut request = ObjectRpcRequest::encode_body(
            "/test/timeline-forwarded",
            "test.timeline-forwarded",
            &vec![0x5a_u8; crate::owned_chunk::BODY_CHUNK_MAX + 17],
        )
        .expect("encode forwarded request");
        let encoded = std::mem::take(&mut request.body);
        let body = crate::ObjectRpcIncomingBody::bytes(encoded);
        let mut call = ObjectRpcCall::from_forwarded_streamed_value(request, body)
            .expect("forward streamed value");

        prepare_call_for_active_network_timeline(&mut call);

        assert!(call.request.traceparent().is_some());
        assert!(call.request.observe_sampled());
        crate::stop_network_timeline_capture().expect("stop timeline capture");
    }

    #[test]
    fn active_timeline_still_samples_mutable_calls() {
        let _capture_guard = crate::timeline::network_timeline_test_capture_guard();
        if crate::network_timeline_capture_is_active() {
            let _ = crate::stop_network_timeline_capture();
        }
        crate::start_network_timeline_capture(8).expect("start timeline capture");
        let mut call = ObjectRpcCall::value(ObjectRpcRequest::from_encoded_body(
            "/test/timeline-mutable",
            "test.timeline-mutable",
            Vec::new(),
        ));

        prepare_call_for_active_network_timeline(&mut call);

        assert!(call.request.traceparent().is_some());
        assert!(call.request.observe_sampled());
        crate::stop_network_timeline_capture().expect("stop timeline capture");
    }
}
