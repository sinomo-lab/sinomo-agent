use crate::{AuthenticatedDaemonCommand, DaemonError, DaemonPaths, daemon_rpc_body_sha256};
use sinomo_core::cluster::read_cluster_state_without_kv;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    AsyncDaemonControlStream, CLUSTER_TCP_TUNNEL_OPEN_ROUTE, CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
    CLUSTER_TCP_TUNNEL_RPC_CATEGORY, ClusterEndpointSelectionPurpose, ClusterTcpTunnelFrame,
    ClusterTcpTunnelOpenRequest, ClusterTcpTunnelOpenResponse, DaemonRpcTimeouts,
    NetworkDeliveryState, NetworkError, NetworkTimeoutPhase, ObjectFrameIoOptions,
    ObjectRpcRequest, ObjectRpcResponse, read_object_frame_async, write_object_frame_async,
};
use sinomo_core::node::NodeId;
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterTransportKind,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};

const TCP_TUNNEL_CHANNEL_CAPACITY: usize = 128;
const TCP_TUNNEL_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(2);
// Tunnel setup is an opportunistic fast path. Fail quickly so the route layer
// can use the ordinary direct endpoint within its five-second network budget.
const TCP_TUNNEL_CONNECT_TIMEOUT: Duration = Duration::from_millis(1500);
// Real endpoints reserve most of their attempt for direct fallback when a
// cached tunnel is stale; synthetic tunnel-only endpoints do not use this cap.
const TCP_TUNNEL_REAL_ENDPOINT_RPC_TIMEOUT: Duration = Duration::from_millis(750);
const TCP_TUNNEL_REAL_ENDPOINT_MIN_DIRECT_FALLBACK_BUDGET: Duration = Duration::from_millis(500);
const TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX: &str =
    sinomo_core::network::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TcpTunnelDirection {
    Inbound,
    Outbound,
}

type PendingTcpTunnelMap = Arc<tokio::sync::Mutex<HashMap<u64, PendingTcpTunnelRequest>>>;

struct PendingTcpTunnelRequest {
    tx: oneshot::Sender<Result<ObjectRpcResponse, NetworkError>>,
}

static TCP_TUNNELS: LazyLock<TcpTunnelRegistry> = LazyLock::new(TcpTunnelRegistry::default);

#[derive(Default)]
struct TcpTunnelRegistry {
    peers: Mutex<BTreeMap<NodeId, Arc<TcpTunnelHandle>>>,
    connect_locks: Mutex<BTreeMap<NodeId, Arc<tokio::sync::Mutex<()>>>>,
}

impl TcpTunnelRegistry {
    fn get(&self, node_id: &NodeId) -> Option<Arc<TcpTunnelHandle>> {
        self.peers
            .lock()
            .ok()
            .and_then(|peers| peers.get(node_id).cloned())
    }

    fn contains(&self, node_id: &NodeId) -> bool {
        self.peers
            .lock()
            .map(|peers| peers.contains_key(node_id))
            .unwrap_or(false)
    }

    fn connect_lock(&self, node_id: &NodeId) -> Arc<tokio::sync::Mutex<()>> {
        let Ok(mut connect_locks) = self.connect_locks.lock() else {
            return Arc::new(tokio::sync::Mutex::new(()));
        };
        connect_locks
            .entry(node_id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn insert_or_existing(&self, handle: Arc<TcpTunnelHandle>) -> Result<(), Arc<TcpTunnelHandle>> {
        let Ok(mut peers) = self.peers.lock() else {
            return Ok(());
        };
        if let Some(existing) = peers.get(&handle.peer_node_id).cloned() {
            if existing.active_requests.load(Ordering::Relaxed) > 0 {
                return Err(existing);
            }
            // Both peers are allowed to dial so asymmetric/NAT reachability
            // still works. When both directions connect at once, however,
            // replacing every idle duplicate makes the two sides close each
            // other's physical connection forever. Prefer the connection
            // initiated by the lower node id on both ends; retain a lone
            // reverse-direction connection until that canonical one arrives.
            let replace = existing.clean_shutdown.load(Ordering::Relaxed)
                || (!existing.canonical_direction && handle.canonical_direction);
            if !replace {
                return Err(existing);
            }
            peers.insert(handle.peer_node_id.clone(), handle);
            existing.request_close("replaced by canonical tcp tunnel".to_string());
            return Ok(());
        }
        peers.insert(handle.peer_node_id.clone(), handle);
        Ok(())
    }

    #[cfg(test)]
    fn insert(&self, handle: Arc<TcpTunnelHandle>) {
        let _ = self.insert_or_existing(handle);
    }

    fn remove_if_same(&self, handle: &Arc<TcpTunnelHandle>) {
        let Ok(mut peers) = self.peers.lock() else {
            return;
        };
        if peers
            .get(&handle.peer_node_id)
            .is_some_and(|current| Arc::ptr_eq(current, handle))
        {
            peers.remove(&handle.peer_node_id);
        }
    }

    fn remove(&self, node_id: &NodeId) -> Option<Arc<TcpTunnelHandle>> {
        self.peers
            .lock()
            .ok()
            .and_then(|mut peers| peers.remove(node_id))
    }

    fn peer_ids(&self) -> Vec<NodeId> {
        self.peers
            .lock()
            .map(|peers| peers.keys().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
pub(crate) fn insert_test_tcp_tunnel(node_id: NodeId, tx: mpsc::Sender<ClusterTcpTunnelFrame>) {
    let handle = Arc::new(TcpTunnelHandle {
        peer_node_id: node_id,
        tx,
        pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        next_request_id: AtomicU64::new(1),
        active_requests: AtomicUsize::new(0),
        clean_shutdown: AtomicBool::new(false),
        canonical_direction: true,
    });
    TCP_TUNNELS.insert(handle);
}

#[cfg(test)]
pub(crate) fn remove_test_tcp_tunnel(node_id: &NodeId) {
    if let Some(handle) = TCP_TUNNELS.remove(node_id) {
        handle.request_close("test cleanup".to_string());
    }
}

struct TcpTunnelHandle {
    peer_node_id: NodeId,
    tx: mpsc::Sender<ClusterTcpTunnelFrame>,
    pending: PendingTcpTunnelMap,
    next_request_id: AtomicU64,
    active_requests: AtomicUsize,
    clean_shutdown: AtomicBool,
    canonical_direction: bool,
}

struct TcpTunnelActiveRequestGuard<'a>(&'a AtomicUsize);

impl Drop for TcpTunnelActiveRequestGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl TcpTunnelHandle {
    fn request_close(&self, detail: String) {
        self.clean_shutdown.store(true, Ordering::Relaxed);
        let _ = self.tx.try_send(ClusterTcpTunnelFrame::Close { detail });
    }

    async fn send(
        &self,
        request: ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.active_requests.fetch_add(1, Ordering::Relaxed);
        let _active_request = TcpTunnelActiveRequestGuard(&self.active_requests);
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .await
            .insert(id, PendingTcpTunnelRequest { tx });
        let frame = ClusterTcpTunnelFrame::Request { id, request };
        let send = self.tx.send(frame);
        let send_result = if let Some(timeout) = timeouts.connect_timeout {
            match tokio::time::timeout(timeout, send).await {
                Ok(result) => result,
                Err(_) => {
                    self.pending.lock().await.remove(&id);
                    return Err(tcp_tunnel_timeout_error(
                        "send tcp tunnel request",
                        timeout,
                        NetworkTimeoutPhase::Write,
                        NetworkDeliveryState::DefinitelyNotSent,
                    ));
                }
            }
        } else {
            send.await
        };
        if send_result.is_err() {
            self.pending.lock().await.remove(&id);
            return Err(NetworkError::delivery_failed(
                "send tcp tunnel request",
                NetworkDeliveryState::DefinitelyNotSent,
                "tunnel writer is closed",
            ));
        }
        let wait = rx;
        let result = if let Some(timeout) = timeouts.idle_timeout.or(timeouts.connect_timeout) {
            match tokio::time::timeout(timeout, wait).await {
                Ok(result) => result,
                Err(_) => {
                    self.pending.lock().await.remove(&id);
                    return Err(tcp_tunnel_timeout_error(
                        "read tcp tunnel response",
                        timeout,
                        NetworkTimeoutPhase::FirstByte,
                        NetworkDeliveryState::PossiblyApplied,
                    ));
                }
            }
        } else {
            wait.await
        };
        match result {
            Ok(response) => response,
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(NetworkError::delivery_failed(
                    "read tcp tunnel response",
                    NetworkDeliveryState::PossiblyApplied,
                    "tunnel reader is closed",
                ))
            }
        }
    }

    async fn fail_pending(&self, detail: String) {
        let pending = {
            let mut guard = self.pending.lock().await;
            std::mem::take(&mut *guard)
        };
        for sender in pending.into_values() {
            let error = NetworkError::Io {
                operation: "tcp tunnel request".to_string(),
                detail: detail.clone(),
            };
            let _ = sender.tx.send(Err(error));
        }
    }
}

pub(crate) fn start_tcp_tunnel_maintenance_task(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    label: String,
) -> tokio::task::JoinHandle<()> {
    runtime.clone().spawn(async move {
        loop {
            if *shutdown_rx.borrow() {
                return;
            }
            if let Err(error) = maintain_tcp_tunnels_once(&paths, &runtime).await {
                tracing::debug!("sinomo {label} tcp tunnel maintenance skipped: {error}");
            }
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                }
                () = tokio::time::sleep(TCP_TUNNEL_MAINTENANCE_INTERVAL) => {}
            }
        }
    })
}

pub(crate) async fn accept_tcp_tunnel_connection_async(
    mut stream: AsyncDaemonControlStream,
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    stream_address: String,
    authenticated: AuthenticatedDaemonCommand,
    request: ObjectRpcRequest,
) -> Result<(), DaemonError> {
    let open: ClusterTcpTunnelOpenRequest = request.decode_body().map_err(DaemonError::from)?;
    if open.protocol_version != CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION {
        let response = ObjectRpcResponse::encode_error(
            CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
            format!("unsupported tcp tunnel protocol {}", open.protocol_version),
        )
        .map_err(DaemonError::from)?;
        stream
            .write_response(&stream_address, &response, ObjectFrameIoOptions::default())
            .await
            .map_err(DaemonError::from)?;
        return Ok(());
    }
    let peer_node_id =
        NodeId::from_str(&authenticated.signer_node_id).map_err(|source| DaemonError::Network {
            address: authenticated.signer_node_id.clone(),
            detail: format!("invalid tcp tunnel peer node id: {source}"),
        })?;
    let mut response = ObjectRpcResponse::encode_body(
        CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
        &ClusterTcpTunnelOpenResponse::accepted(),
    )
    .map_err(DaemonError::from)?;
    crate::service_adapter::mark_daemon_rpc_response_responder(&paths, &mut response);
    stream
        .write_response(&stream_address, &response, ObjectFrameIoOptions::default())
        .await
        .map_err(DaemonError::from)?;
    let connectivity_endpoint = tcp_tunnel_connectivity_context(&paths, &peer_node_id);
    let tunnel_record_endpoint = connectivity_endpoint
        .as_ref()
        .map(|(_, _, endpoint)| endpoint.clone());
    spawn_tcp_tunnel(
        peer_node_id.clone(),
        stream.into_inner(),
        paths.clone(),
        service_runtime.clone(),
        tunnel_record_endpoint,
        TcpTunnelDirection::Inbound,
    );
    if let Some((cluster_paths, cluster_state, endpoint)) = connectivity_endpoint.as_ref() {
        crate::cluster::remember_cluster_tunnel_connectivity(
            cluster_paths,
            cluster_state,
            endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(&service_runtime),
        );
    }
    Ok(())
}

pub(crate) async fn send_signed_endpoint_rpc_via_tcp_tunnel(
    endpoint: &ClusterEndpointRecord,
    auth: &[u8],
    request: &ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
) -> Option<Result<ObjectRpcResponse, NetworkError>> {
    if endpoint.transport != ClusterTransportKind::LanTcp {
        return None;
    }
    let mut signed = request.clone();
    signed.auth = Some(auth.to_vec());
    if !tcp_tunnel_is_synthetic_endpoint(endpoint) {
        let tunnel_timeouts = tcp_tunnel_real_endpoint_rpc_timeouts(timeouts);
        if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
            return match send_over_tcp_tunnel_handle(handle, signed, tunnel_timeouts).await {
                Ok(response) => Some(Ok(response)),
                Err(error)
                    if error.delivery_state() == Some(NetworkDeliveryState::DefinitelyNotSent) =>
                {
                    tracing::debug!(
                        "tcp tunnel send to {} ({}) failed on a real endpoint; falling back to direct tcp: {error}",
                        endpoint.node_id,
                        endpoint.address
                    );
                    None
                }
                Err(error) => Some(Err(error)),
            };
        }
        match connect_tcp_tunnel_to_endpoint_for_rpc(endpoint, runtime, paths, tunnel_timeouts)
            .await
        {
            Ok(handle) => {
                return match send_over_tcp_tunnel_handle(handle, signed, tunnel_timeouts).await {
                    Ok(response) => Some(Ok(response)),
                    Err(error)
                        if error.delivery_state()
                            == Some(NetworkDeliveryState::DefinitelyNotSent) =>
                    {
                        tracing::debug!(
                            "tcp tunnel send to {} ({}) failed after open on a real endpoint; falling back to direct tcp: {error}",
                            endpoint.node_id,
                            endpoint.address
                        );
                        None
                    }
                    Err(error) => Some(Err(error)),
                };
            }
            Err(error) => {
                tracing::debug!(
                    "tcp tunnel open to {} ({}) failed before rpc; falling back to direct tcp: {error}",
                    endpoint.node_id,
                    endpoint.address
                );
                return None;
            }
        }
    }
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return match send_over_tcp_tunnel_handle(handle, signed, timeouts).await {
            Ok(response) => Some(Ok(response)),
            Err(error) => {
                tracing::debug!(
                    "tcp tunnel send to {} ({}) failed after request acceptance: {error}",
                    endpoint.node_id,
                    endpoint.address
                );
                Some(Err(error))
            }
        };
    }
    if let Some(paths) = paths {
        remember_tcp_tunnel_connectivity(
            paths,
            endpoint,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            Some(runtime),
        )
        .await;
    }
    Some(Err(NetworkError::delivery_failed(
        "send tcp tunnel request",
        NetworkDeliveryState::DefinitelyNotSent,
        format!(
            "no persistent tcp tunnel to {} ({})",
            endpoint.node_id, endpoint.address
        ),
    )))
}

fn tcp_tunnel_real_endpoint_rpc_timeouts(timeouts: DaemonRpcTimeouts) -> DaemonRpcTimeouts {
    let total_timeout = match (timeouts.connect_timeout, timeouts.idle_timeout) {
        (Some(connect), Some(idle)) => Some(connect.min(idle)),
        (Some(timeout), None) | (None, Some(timeout)) => Some(timeout),
        (None, None) => None,
    };
    let tunnel_budget = tcp_tunnel_real_endpoint_rpc_budget(total_timeout);
    let connect_timeout = Some(
        timeouts
            .connect_timeout
            .map(|timeout| timeout.min(tunnel_budget))
            .unwrap_or(tunnel_budget),
    );
    let idle_timeout = Some(
        timeouts
            .idle_timeout
            .or(timeouts.connect_timeout)
            .map(|timeout| timeout.min(tunnel_budget))
            .unwrap_or(tunnel_budget),
    );
    timeouts
        .with_connect_timeout(connect_timeout)
        .with_idle_timeout(idle_timeout)
}

fn tcp_tunnel_real_endpoint_rpc_budget(total_timeout: Option<Duration>) -> Duration {
    let Some(total_timeout) = total_timeout else {
        return TCP_TUNNEL_REAL_ENDPOINT_RPC_TIMEOUT;
    };
    if total_timeout <= TCP_TUNNEL_REAL_ENDPOINT_MIN_DIRECT_FALLBACK_BUDGET {
        return total_timeout;
    }
    total_timeout
        .saturating_sub(TCP_TUNNEL_REAL_ENDPOINT_MIN_DIRECT_FALLBACK_BUDGET)
        .min(TCP_TUNNEL_REAL_ENDPOINT_RPC_TIMEOUT)
}

fn tcp_tunnel_connectivity_context(
    paths: &DaemonPaths,
    peer_node_id: &NodeId,
) -> Option<(
    sinomo_core::network::ClusterPaths,
    sinomo_core::ClusterBootstrapState,
    ClusterEndpointRecord,
)> {
    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(paths).ok()?;
    let state = read_cluster_state_without_kv(&cluster_paths).ok()?;
    let endpoint = tcp_tunnel_synthetic_endpoint(&state.local_node_id, peer_node_id);
    Some((cluster_paths, state, endpoint))
}

fn tcp_tunnel_synthetic_endpoint(
    local_node_id: &NodeId,
    peer_node_id: &NodeId,
) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
            "sinomo.daemon.tcp-tunnel.endpoint",
            format!("{}\0{}", local_node_id.as_str(), peer_node_id.as_str()),
        ),
        node_id: peer_node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address: format!("{TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX}{peer_node_id}"),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 0,
        last_observed_latency_ms: None,
    }
}

fn tcp_tunnel_is_synthetic_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    endpoint.transport == ClusterTransportKind::LanTcp
        && endpoint
            .address
            .starts_with(TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX)
}

fn tcp_tunnel_connectivity_record_is_synthetic(
    record: &sinomo_core::network::ClusterConnectivityRecord,
) -> bool {
    record.transport == ClusterTransportKind::LanTcp
        && record
            .address
            .starts_with(TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX)
}

async fn ping_tcp_tunnel_peer(
    local_node_id: &NodeId,
    peer_node_id: &NodeId,
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<(), NetworkError> {
    let endpoint = tcp_tunnel_synthetic_endpoint(local_node_id, peer_node_id);
    let request = sinomo_core::cluster::cluster_network_ping_rpc_request(
        format!("tcp-tunnel-maintenance-{peer_node_id}"),
        0,
    )?;
    let auth = sign_object_rpc_request_for_local_node(&request, runtime, Some(paths)).await?;
    let response = send_signed_endpoint_rpc_via_tcp_tunnel(
        &endpoint,
        &auth,
        &request,
        DaemonRpcTimeouts::connect_and_idle(TCP_TUNNEL_CONNECT_TIMEOUT),
        runtime,
        Some(paths),
    )
    .await
    .unwrap_or_else(|| {
        Err(NetworkError::Io {
            operation: "tcp tunnel maintenance ping".to_string(),
            detail: "tcp tunnel send was not handled by tunnel transport".to_string(),
        })
    })?;
    response.error_detail()?;
    let ping = sinomo_core::cluster::decode_cluster_network_ping_rpc_response(&response)?;
    if ping.responder_node_id != *peer_node_id {
        return Err(NetworkError::Io {
            operation: "tcp tunnel maintenance ping".to_string(),
            detail: format!(
                "responder mismatch: expected {}, got {}",
                peer_node_id, ping.responder_node_id
            ),
        });
    }
    Ok(())
}

async fn evict_tcp_tunnel_peer(peer_node_id: &NodeId, detail: String) {
    if let Some(handle) = TCP_TUNNELS.remove(peer_node_id) {
        handle.request_close(detail.clone());
        handle.fail_pending(detail).await;
    }
}

async fn send_over_tcp_tunnel_handle(
    handle: Arc<TcpTunnelHandle>,
    request: ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<ObjectRpcResponse, NetworkError> {
    match send_over_tcp_tunnel_handle_once(handle.clone(), request.clone(), timeouts).await {
        Ok(response) => Ok(response),
        Err(error) => {
            if error.delivery_state() == Some(NetworkDeliveryState::DefinitelyNotSent)
                && let Some(current) = TCP_TUNNELS
                    .get(&handle.peer_node_id)
                    .filter(|current| !Arc::ptr_eq(current, &handle))
            {
                tracing::debug!(
                    "retrying tcp tunnel request to {} on replacement tunnel after stale handle failed: {error}",
                    handle.peer_node_id
                );
                return send_over_tcp_tunnel_handle_once(current, request, timeouts).await;
            }
            Err(error)
        }
    }
}

async fn send_over_tcp_tunnel_handle_once(
    handle: Arc<TcpTunnelHandle>,
    request: ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<ObjectRpcResponse, NetworkError> {
    let result = handle.send(request, timeouts).await;
    if let Err(error) = result.as_ref()
        && tcp_tunnel_request_error_should_evict(error)
    {
        TCP_TUNNELS.remove_if_same(&handle);
        let detail = format!("tcp tunnel evicted after request failure: {error}");
        handle.request_close(detail.clone());
        handle.fail_pending(detail).await;
    }
    result
}

fn tcp_tunnel_request_error_should_evict(error: &NetworkError) -> bool {
    match error {
        NetworkError::Io { operation, detail } => match operation.as_str() {
            "send tcp tunnel request" => true,
            "read tcp tunnel response" => !detail.starts_with("timed out after "),
            _ => true,
        },
        NetworkError::Timeout {
            phase: NetworkTimeoutPhase::FirstByte | NetworkTimeoutPhase::Idle,
            ..
        } => false,
        _ => true,
    }
}

fn tcp_tunnel_should_maintain_outbound(local_node_id: &NodeId, peer_node_id: &NodeId) -> bool {
    local_node_id != peer_node_id
}

fn tcp_tunnel_direction_is_canonical(
    local_node_id: &NodeId,
    peer_node_id: &NodeId,
    direction: TcpTunnelDirection,
) -> bool {
    match direction {
        TcpTunnelDirection::Outbound => local_node_id < peer_node_id,
        TcpTunnelDirection::Inbound => peer_node_id < local_node_id,
    }
}

fn tcp_tunnel_direction_is_canonical_for_paths(
    paths: &DaemonPaths,
    peer_node_id: &NodeId,
    direction: TcpTunnelDirection,
) -> bool {
    crate::cluster::cluster_paths_from_daemon_paths(paths)
        .ok()
        .and_then(|cluster_paths| read_cluster_state_without_kv(&cluster_paths).ok())
        .is_some_and(|state| {
            tcp_tunnel_direction_is_canonical(&state.local_node_id, peer_node_id, direction)
        })
}

async fn maintain_tcp_tunnels_once(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(paths)?;
    let state = read_cluster_state_without_kv(&cluster_paths).map_err(DaemonError::from)?;
    prune_stale_tcp_tunnel_connectivity(paths, runtime).await?;
    let selector = sinomo_core::network::ClusterEndpointSelector::new(&cluster_paths, &state);
    let mut endpoints =
        selector.select(ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates);
    endpoints.extend(selector.select(ClusterEndpointSelectionPurpose::NetworkProbe));
    let mut live_peers = BTreeSet::new();
    for peer_node_id in TCP_TUNNELS.peer_ids() {
        if peer_node_id == state.local_node_id {
            continue;
        }
        match ping_tcp_tunnel_peer(&state.local_node_id, &peer_node_id, paths, runtime).await {
            Ok(()) => {
                let synthetic = tcp_tunnel_synthetic_endpoint(&state.local_node_id, &peer_node_id);
                remember_tcp_tunnel_connectivity(
                    paths,
                    &synthetic,
                    sinomo_core::network::ClusterConnectivityState::Healthy,
                    Some(runtime),
                )
                .await;
                live_peers.insert(peer_node_id);
            }
            Err(error) => {
                evict_tcp_tunnel_peer(&peer_node_id, format!("maintenance ping failed: {error}"))
                    .await;
                let synthetic = tcp_tunnel_synthetic_endpoint(&state.local_node_id, &peer_node_id);
                remember_tcp_tunnel_connectivity(
                    paths,
                    &synthetic,
                    sinomo_core::network::ClusterConnectivityState::Unreachable,
                    Some(runtime),
                )
                .await;
            }
        }
    }
    for endpoint in tcp_tunnel_maintenance_candidate_endpoints(&state.local_node_id, endpoints) {
        if live_peers.contains(&endpoint.node_id) {
            continue;
        }
        if connect_tcp_tunnel_to_endpoint(&endpoint, runtime, Some(paths))
            .await
            .is_ok()
        {
            live_peers.insert(endpoint.node_id.clone());
        }
    }
    prune_stale_tcp_tunnel_connectivity(paths, runtime).await?;
    Ok(())
}

async fn prune_stale_tcp_tunnel_connectivity(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(paths)?;
    let state = read_cluster_state_without_kv(&cluster_paths).map_err(DaemonError::from)?;
    let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
        &cluster_paths,
        &state.local_node_id,
    )?;
    let retained = records
        .iter()
        .filter(|record| {
            !tcp_tunnel_connectivity_record_is_synthetic(record)
                || TCP_TUNNELS.contains(&record.target_node_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    if retained.len() != records.len() {
        sinomo_core::network::write_local_cluster_connectivity_record_set_async(
            runtime,
            &cluster_paths,
            &state.local_node_id,
            &retained,
        )
        .await?;
    }
    Ok(())
}

fn tcp_tunnel_maintenance_candidate_endpoints(
    local_node_id: &NodeId,
    endpoints: Vec<ClusterEndpointRecord>,
) -> Vec<ClusterEndpointRecord> {
    endpoints
        .into_iter()
        .filter(|endpoint| {
            endpoint.node_id != *local_node_id
                && endpoint.transport == ClusterTransportKind::LanTcp
                && !tcp_tunnel_is_synthetic_endpoint(endpoint)
                && sinomo_core::network::cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
                && tcp_tunnel_should_maintain_outbound(local_node_id, &endpoint.node_id)
        })
        .collect()
}

async fn connect_tcp_tunnel_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
) -> Result<Arc<TcpTunnelHandle>, NetworkError> {
    connect_tcp_tunnel_to_endpoint_with_timeout(
        endpoint,
        runtime,
        paths,
        TCP_TUNNEL_CONNECT_TIMEOUT,
    )
    .await
}

async fn connect_tcp_tunnel_to_endpoint_for_rpc(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
    rpc_timeouts: DaemonRpcTimeouts,
) -> Result<Arc<TcpTunnelHandle>, NetworkError> {
    let connect_timeout = rpc_timeouts
        .connect_timeout
        .map(|timeout| timeout.min(TCP_TUNNEL_CONNECT_TIMEOUT))
        .unwrap_or(TCP_TUNNEL_CONNECT_TIMEOUT);
    connect_tcp_tunnel_to_endpoint_with_timeout(endpoint, runtime, paths, connect_timeout).await
}

async fn connect_tcp_tunnel_to_endpoint_with_timeout(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
    connect_timeout: Duration,
) -> Result<Arc<TcpTunnelHandle>, NetworkError> {
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return Ok(handle);
    }
    let connect_lock = TCP_TUNNELS.connect_lock(&endpoint.node_id);
    let _connect_guard = connect_lock.lock().await;
    connect_tcp_tunnel_to_endpoint_after_lock(endpoint, runtime, paths, connect_timeout).await
}

async fn connect_tcp_tunnel_to_endpoint_after_lock(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
    connect_timeout: Duration,
) -> Result<Arc<TcpTunnelHandle>, NetworkError> {
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return Ok(handle);
    }
    let mut stream = AsyncDaemonControlStream::connect_with_options(
        &endpoint.address,
        DaemonRpcTimeouts::connect_and_idle(connect_timeout),
    )
    .await?;
    let request = signed_tcp_tunnel_open_request(runtime, paths).await?;
    stream
        .write_request(&endpoint.address, &request, ObjectFrameIoOptions::default())
        .await?;
    let response = stream
        .read_response(
            &endpoint.address,
            sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
            ObjectFrameIoOptions::default(),
        )
        .await?;
    response.error_detail()?;
    let open: ClusterTcpTunnelOpenResponse = response.decode_body()?;
    if !open.accepted || open.protocol_version != CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION {
        return Err(NetworkError::Io {
            operation: "open tcp tunnel".to_string(),
            detail: "peer rejected tcp tunnel protocol".to_string(),
        });
    }
    let paths = paths
        .cloned()
        .or_else(daemon_paths_from_env)
        .ok_or_else(|| NetworkError::Io {
            operation: "open tcp tunnel".to_string(),
            detail: "local daemon paths unavailable".to_string(),
        })?;
    let handle = spawn_tcp_tunnel(
        endpoint.node_id.clone(),
        stream.into_inner(),
        paths.clone(),
        runtime.clone(),
        Some(endpoint.clone()),
        TcpTunnelDirection::Outbound,
    );
    remember_tcp_tunnel_connectivity(
        &paths,
        endpoint,
        sinomo_core::network::ClusterConnectivityState::Healthy,
        Some(runtime),
    )
    .await;
    let synthetic =
        tcp_tunnel_connectivity_context(&paths, &endpoint.node_id).map(|(_, _, endpoint)| endpoint);
    if let Some(synthetic) = synthetic.as_ref() {
        remember_tcp_tunnel_connectivity(
            &paths,
            synthetic,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(runtime),
        )
        .await;
    }
    Ok(handle)
}

async fn signed_tcp_tunnel_open_request(
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
) -> Result<ObjectRpcRequest, NetworkError> {
    let mut request = ObjectRpcRequest::encode_body(
        CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
        CLUSTER_TCP_TUNNEL_RPC_CATEGORY,
        &ClusterTcpTunnelOpenRequest::default(),
    )?;
    request.auth = Some(sign_object_rpc_request_for_local_node(&request, runtime, paths).await?);
    Ok(request)
}

async fn sign_object_rpc_request_for_local_node(
    request: &ObjectRpcRequest,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
) -> Result<Vec<u8>, NetworkError> {
    let data_dir = tcp_tunnel_signing_data_dir(paths)?;
    let route = request.route.clone();
    let category = request.category.clone();
    let body_sha256 = daemon_rpc_body_sha256(&request);
    Ok(runtime
        .spawn_blocking(move || {
            sinomo_core::network::sign_daemon_rpc_request_digest_from_data_dir(
                &data_dir,
                &route,
                &category,
                body_sha256,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "sign tcp tunnel request".to_string(),
            detail: format!("signing task failed: {source}"),
        })?
        .map_err(|error| NetworkError::Io {
            operation: "sign tcp tunnel request".to_string(),
            detail: error.to_string(),
        })?)
}

fn tcp_tunnel_signing_data_dir(paths: Option<&DaemonPaths>) -> Result<SinomoDataDir, NetworkError> {
    if let Some(paths) = paths {
        let Some(data_dir) = paths.root.parent() else {
            return Err(NetworkError::Io {
                operation: "tcp tunnel signing data directory".to_string(),
                detail: format!(
                    "daemon path {} has no parent data directory",
                    paths.root.display()
                ),
            });
        };
        return Ok(SinomoDataDir::new(data_dir));
    }
    SinomoDataDir::from_env_or_home().map_err(|source| NetworkError::Io {
        operation: "tcp tunnel signing data directory".to_string(),
        detail: source.to_string(),
    })
}

fn spawn_tcp_tunnel(
    peer_node_id: NodeId,
    stream: tokio::net::TcpStream,
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    connectivity_endpoint: Option<ClusterEndpointRecord>,
    direction: TcpTunnelDirection,
) -> Arc<TcpTunnelHandle> {
    let canonical_direction =
        tcp_tunnel_direction_is_canonical_for_paths(&paths, &peer_node_id, direction);
    let (read_half, write_half) = stream.into_split();
    let (tx, rx) = mpsc::channel(TCP_TUNNEL_CHANNEL_CAPACITY);
    let handle = Arc::new(TcpTunnelHandle {
        peer_node_id,
        tx,
        pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        next_request_id: AtomicU64::new(1),
        active_requests: AtomicUsize::new(0),
        clean_shutdown: AtomicBool::new(false),
        canonical_direction,
    });
    if let Err(existing) = TCP_TUNNELS.insert_or_existing(handle.clone()) {
        drop(read_half);
        tokio::spawn(async move {
            let mut write_half = write_half;
            let _ = write_object_frame_async(
                &mut write_half,
                &ClusterTcpTunnelFrame::Close {
                    detail: "duplicate tunnel ignored; existing tunnel is active".to_string(),
                },
                ObjectFrameIoOptions::default(),
            )
            .await;
        });
        return existing;
    }
    tokio::spawn(tcp_tunnel_writer_loop(handle.clone(), write_half, rx));
    tokio::spawn(tcp_tunnel_reader_loop(
        handle.clone(),
        read_half,
        paths,
        service_runtime,
        connectivity_endpoint,
    ));
    handle
}

async fn tcp_tunnel_writer_loop(
    handle: Arc<TcpTunnelHandle>,
    mut write_half: OwnedWriteHalf,
    mut rx: mpsc::Receiver<ClusterTcpTunnelFrame>,
) {
    while let Some(frame) = rx.recv().await {
        let close = matches!(frame, ClusterTcpTunnelFrame::Close { .. });
        if write_object_frame_async(&mut write_half, &frame, ObjectFrameIoOptions::default())
            .await
            .is_err()
        {
            break;
        }
        if close {
            break;
        }
    }
    handle
        .fail_pending("tcp tunnel writer stopped".to_string())
        .await;
}

async fn tcp_tunnel_reader_loop(
    handle: Arc<TcpTunnelHandle>,
    mut read_half: OwnedReadHalf,
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    connectivity_endpoint: Option<ClusterEndpointRecord>,
) {
    let mut mark_unreachable_on_exit = false;
    loop {
        // This reader waits between independent multiplexed requests, so an
        // idle connection is healthy. Each active request carries its own
        // 5-second connect/no-progress policy and operation deadline.
        let frame = read_object_frame_async::<_, ClusterTcpTunnelFrame>(
            &mut read_half,
            sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
            ObjectFrameIoOptions::none(),
        )
        .await;
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                handle.fail_pending(error.to_string()).await;
                mark_unreachable_on_exit = !handle.clean_shutdown.load(Ordering::Relaxed);
                break;
            }
        };
        match frame {
            ClusterTcpTunnelFrame::Request { id, request } => {
                let tx = handle.tx.clone();
                let paths = paths.clone();
                let service_runtime = service_runtime.clone();
                tokio::spawn(async move {
                    let mut response =
                        match crate::control::dispatch_daemon_control_request_in_process_async(
                            paths.clone(),
                            service_runtime,
                            request.clone(),
                        )
                        .await
                        {
                            Ok(response) => response,
                            Err(error) => ObjectRpcResponse::encode_error(
                                request.route.clone(),
                                error.to_string(),
                            )
                            .unwrap_or_else(|_| ObjectRpcResponse {
                                route: request.route,
                                body: Vec::new(),
                                metadata: BTreeMap::new(),
                            }),
                        };
                    crate::service_adapter::mark_daemon_rpc_response_responder(
                        &paths,
                        &mut response,
                    );
                    let _ = tx
                        .send(ClusterTcpTunnelFrame::Response { id, response })
                        .await;
                });
            }
            ClusterTcpTunnelFrame::Response { id, response } => {
                if let Some(pending) = handle.pending.lock().await.remove(&id) {
                    let _ = pending.tx.send(Ok(response));
                }
            }
            ClusterTcpTunnelFrame::Close { detail } => {
                handle.clean_shutdown.store(true, Ordering::Relaxed);
                handle.fail_pending(detail).await;
                break;
            }
        }
    }
    TCP_TUNNELS.remove_if_same(&handle);
    if mark_unreachable_on_exit && let Some(endpoint) = connectivity_endpoint.as_ref() {
        remember_tcp_tunnel_connectivity(
            &paths,
            endpoint,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            Some(&service_runtime),
        )
        .await;
    }
}

#[cfg(test)]
fn tcp_tunnel_connectivity_endpoint_for_peer(
    paths: &DaemonPaths,
    peer_node_id: &NodeId,
) -> Option<ClusterEndpointRecord> {
    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(paths).ok()?;
    let state = read_cluster_state_without_kv(&cluster_paths).ok()?;
    let selector = sinomo_core::network::ClusterEndpointSelector::new(&cluster_paths, &state);
    selector
        .select(ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates)
        .into_iter()
        .chain(selector.select(ClusterEndpointSelectionPurpose::NetworkProbe))
        .find(|endpoint| {
            endpoint.node_id == *peer_node_id
                && endpoint.transport == ClusterTransportKind::LanTcp
                && sinomo_core::network::cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
        })
}

async fn remember_tcp_tunnel_connectivity(
    paths: &DaemonPaths,
    endpoint: &ClusterEndpointRecord,
    state: sinomo_core::network::ClusterConnectivityState,
    runtime: Option<&tokio::runtime::Handle>,
) {
    let Ok(cluster_paths) = crate::cluster::cluster_paths_from_daemon_paths(paths) else {
        return;
    };
    let Ok(cluster_state) = read_cluster_state_without_kv(&cluster_paths) else {
        return;
    };
    let paths = cluster_paths.clone();
    let endpoint = endpoint.clone();
    let work = move || {
        crate::cluster::remember_cluster_tunnel_connectivity(
            &paths,
            &cluster_state,
            &endpoint,
            state,
            None,
        );
    };
    if let Some(runtime) = runtime {
        let _ = runtime.spawn_blocking(work).await;
    } else {
        work();
    }
}

fn daemon_paths_from_env() -> Option<DaemonPaths> {
    let data_dir = SinomoDataDir::from_env_or_home().ok()?;
    Some(DaemonPaths::new(&data_dir))
}

fn tcp_tunnel_timeout_error(
    operation: &str,
    timeout: Duration,
    phase: NetworkTimeoutPhase,
    delivery_state: NetworkDeliveryState,
) -> NetworkError {
    NetworkError::timeout(operation, phase, timeout, delivery_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{ClusterEndpointPrivacy, ClusterEndpointSource};

    fn test_tunnel_handle(
        node_name: &str,
        tx: mpsc::Sender<ClusterTcpTunnelFrame>,
    ) -> Arc<TcpTunnelHandle> {
        test_tunnel_handle_with_direction(node_name, tx, true)
    }

    fn test_tunnel_handle_with_direction(
        node_name: &str,
        tx: mpsc::Sender<ClusterTcpTunnelFrame>,
        canonical_direction: bool,
    ) -> Arc<TcpTunnelHandle> {
        Arc::new(TcpTunnelHandle {
            peer_node_id: NodeId::from_legacy_named(node_name).expect("test node id"),
            tx,
            pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            next_request_id: AtomicU64::new(1),
            active_requests: AtomicUsize::new(0),
            clean_shutdown: AtomicBool::new(false),
            canonical_direction,
        })
    }

    fn test_rpc_request() -> ObjectRpcRequest {
        ObjectRpcRequest::encode_body("/test/tcp-tunnel", "test", &()).expect("test request")
    }

    #[test]
    fn tcp_tunnel_synthetic_endpoint_is_stable_for_node_pair() {
        let local = NodeId::from_legacy_named("tcp_tunnel_synthetic_local").expect("local node id");
        let peer = NodeId::from_legacy_named("tcp_tunnel_synthetic_peer").expect("peer node id");

        let first = tcp_tunnel_synthetic_endpoint(&local, &peer);
        let second = tcp_tunnel_synthetic_endpoint(&local, &peer);

        assert_eq!(first.endpoint_id, second.endpoint_id);
        assert_eq!(first.node_id, peer);
        assert_eq!(first.transport, ClusterTransportKind::LanTcp);
        assert!(tcp_tunnel_is_synthetic_endpoint(&first));
    }

    #[tokio::test]
    async fn tcp_tunnel_open_request_signs_with_daemon_paths_cluster_node() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-tcp-tunnel-path-signing");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local =
            NodeId::from_legacy_named("node_tcp_tunnel_path_signing_local").expect("local node id");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_tcp_tunnel_path_signing")
                .expect("cluster id");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "tcp tunnel path signing",
            local.clone(),
            "local",
            "pk-local",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");

        let request =
            signed_tcp_tunnel_open_request(&tokio::runtime::Handle::current(), Some(&daemon_paths))
                .await
                .expect("signed request");
        let envelope: crate::SignedDaemonCommand =
            sinomo_core::network::SinomoObjectCodec::default()
                .decode(request.auth.as_deref().expect("auth envelope"))
                .expect("decode signed envelope");

        assert_eq!(envelope.node_id, local.to_string());
    }

    #[tokio::test]
    async fn tcp_tunnel_synthetic_endpoint_without_handle_does_not_direct_dial() {
        let local =
            NodeId::from_legacy_named("tcp_tunnel_synthetic_send_local").expect("local node id");
        let peer =
            NodeId::from_legacy_named("tcp_tunnel_synthetic_send_peer").expect("peer node id");
        let endpoint = tcp_tunnel_synthetic_endpoint(&local, &peer);

        let result = send_signed_endpoint_rpc_via_tcp_tunnel(
            &endpoint,
            b"auth",
            &test_rpc_request(),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(60)),
            &tokio::runtime::Handle::current(),
            None,
        )
        .await;

        match result {
            Some(Err(NetworkError::DeliveryFailed {
                operation,
                delivery_state,
                detail,
            })) => {
                assert_eq!(operation, "send tcp tunnel request");
                assert_eq!(delivery_state, NetworkDeliveryState::DefinitelyNotSent);
                assert!(detail.contains("no persistent tcp tunnel"));
            }
            other => panic!("expected synthetic tunnel endpoint error, got {other:?}"),
        }
    }

    #[test]
    fn tcp_tunnel_unreachable_record_is_not_identity_mismatch() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-tcp-tunnel-unreachable-record");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = time::OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_tcp_tunnel_unreachable")
                .expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_tcp_tunnel_unreachable_local").expect("local node id");
        let peer =
            NodeId::from_legacy_named("node_tcp_tunnel_unreachable_peer").expect("peer node id");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "tcp tunnel unreachable",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = tcp_tunnel_synthetic_endpoint(&local, &peer);

        crate::cluster::remember_cluster_tunnel_connectivity(
            &cluster_paths,
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            None,
        );

        let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            &cluster_paths,
            &local,
        )
        .expect("connectivity records");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].state,
            sinomo_core::network::ClusterConnectivityState::Unreachable
        );
        assert_eq!(records[0].failure_reason, None);
    }

    fn test_endpoint(
        endpoint_name: &str,
        node_id: &NodeId,
        address: &str,
        transport: ClusterTransportKind,
    ) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::from_legacy_named(endpoint_name)
                .expect("endpoint id"),
            node_id: node_id.clone(),
            transport,
            address: address.to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        }
    }

    #[tokio::test]
    async fn tcp_tunnel_send_timeout_removes_pending_request() {
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(ClusterTcpTunnelFrame::Close {
            detail: "fill channel".to_string(),
        })
        .expect("fill tunnel channel");
        let handle = test_tunnel_handle("tcp_tunnel_send_timeout_cleanup", tx);

        let result = handle
            .send(
                test_rpc_request(),
                DaemonRpcTimeouts::connect_only(Duration::from_millis(1)),
            )
            .await;

        assert!(matches!(
            result,
            Err(NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Write,
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            })
        ));
        assert!(handle.pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn tcp_tunnel_response_timeout_removes_pending_request() {
        let (tx, _rx) = mpsc::channel(1);
        let handle = test_tunnel_handle("tcp_tunnel_response_timeout_cleanup", tx);

        let result = handle
            .send(
                test_rpc_request(),
                DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(1)),
            )
            .await;

        assert!(matches!(
            result,
            Err(NetworkError::Timeout {
                phase: NetworkTimeoutPhase::FirstByte,
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            })
        ));
        assert!(handle.pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn failed_tcp_tunnel_send_evicts_stale_handle() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let node_id =
            NodeId::from_legacy_named("tcp_tunnel_failed_send_eviction").expect("test node id");
        let handle = test_tunnel_handle(node_id.as_str(), tx);
        TCP_TUNNELS.insert(handle.clone());

        let result = send_over_tcp_tunnel_handle(
            handle.clone(),
            test_rpc_request(),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(5)),
        )
        .await;

        assert!(result.is_err());
        assert!(TCP_TUNNELS.get(&node_id).is_none());
        assert!(handle.pending.lock().await.is_empty());
        assert!(handle.clean_shutdown.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn tcp_tunnel_response_timeout_retains_handle() {
        let (tx, _rx) = mpsc::channel(1);
        let node_name = "tcp_tunnel_response_timeout_retains_handle";
        let node_id = NodeId::from_legacy_named(node_name).expect("test node id");
        let handle = test_tunnel_handle(node_name, tx);
        TCP_TUNNELS.insert(handle.clone());

        let result = send_over_tcp_tunnel_handle(
            handle.clone(),
            test_rpc_request(),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(1)),
        )
        .await;

        assert!(matches!(
            result,
            Err(NetworkError::Timeout {
                phase: NetworkTimeoutPhase::FirstByte,
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            })
        ));
        assert!(Arc::ptr_eq(
            &TCP_TUNNELS.get(&node_id).expect("tunnel retained"),
            &handle
        ));
        assert!(handle.pending.lock().await.is_empty());
        assert!(!handle.clean_shutdown.load(Ordering::Relaxed));

        TCP_TUNNELS.remove(&node_id);
    }

    #[tokio::test]
    async fn tcp_tunnel_send_retries_current_handle_after_idle_replacement() {
        let (old_tx, old_rx) = mpsc::channel(1);
        drop(old_rx);
        let node_name = "tcp_tunnel_retry_after_idle_replacement";
        let node_id = NodeId::from_legacy_named(node_name).expect("test node id");
        let old = test_tunnel_handle_with_direction(node_name, old_tx, false);
        TCP_TUNNELS.insert(old.clone());

        let (new_tx, mut new_rx) = mpsc::channel(1);
        let current = test_tunnel_handle_with_direction(node_name, new_tx, true);
        TCP_TUNNELS.insert(current.clone());
        assert!(old.clean_shutdown.load(Ordering::Relaxed));
        assert!(Arc::ptr_eq(
            &TCP_TUNNELS.get(&node_id).expect("current tunnel"),
            &current
        ));

        let request = test_rpc_request();
        let send_task = tokio::spawn(async move {
            send_over_tcp_tunnel_handle(
                old,
                request,
                DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            )
            .await
        });
        let id = match new_rx.recv().await.expect("retried tunnel request") {
            ClusterTcpTunnelFrame::Request { id, request } => {
                assert_eq!(request.route, "/test/tcp-tunnel");
                id
            }
            other => panic!("expected retried tunnel request, got {other:?}"),
        };
        let pending = current
            .pending
            .lock()
            .await
            .remove(&id)
            .expect("pending retried request");
        pending
            .tx
            .send(Ok(ObjectRpcResponse::from_encoded_body(
                "/test/tcp-tunnel",
                Vec::new(),
            )))
            .expect("send retried tunnel response");

        let response = send_task.await.expect("join send task");
        assert!(response.is_ok());
        assert!(Arc::ptr_eq(
            &TCP_TUNNELS.get(&node_id).expect("current tunnel retained"),
            &current
        ));
        assert!(current.pending.lock().await.is_empty());
        assert!(!current.clean_shutdown.load(Ordering::Relaxed));
    }

    #[test]
    fn tcp_tunnel_registry_replaces_idle_reverse_tunnel_with_canonical_open() {
        let (existing_tx, _existing_rx) = mpsc::channel(1);
        let (duplicate_tx, _duplicate_rx) = mpsc::channel(1);
        let node_name = "tcp_tunnel_duplicate_refreshes_idle";
        let existing = test_tunnel_handle_with_direction(node_name, existing_tx, false);
        let duplicate = test_tunnel_handle_with_direction(node_name, duplicate_tx, true);
        let registry = TcpTunnelRegistry::default();

        assert!(registry.insert_or_existing(existing.clone()).is_ok());
        assert!(registry.insert_or_existing(duplicate.clone()).is_ok());

        assert!(Arc::ptr_eq(
            &registry
                .get(&existing.peer_node_id)
                .expect("registered duplicate tunnel"),
            &duplicate
        ));
        assert!(existing.clean_shutdown.load(Ordering::Relaxed));
        assert!(!duplicate.clean_shutdown.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn tcp_tunnel_existing_request_failure_is_route_error() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let node_name = "tcp_tunnel_route_error_on_existing_failure";
        let node_id = NodeId::from_legacy_named(node_name).expect("test node id");
        let handle = test_tunnel_handle(node_name, tx);
        TCP_TUNNELS.insert(handle.clone());
        let local = NodeId::from_legacy_named("tcp_tunnel_route_error_local").expect("local");
        let endpoint = tcp_tunnel_synthetic_endpoint(&local, &node_id);

        let result = send_signed_endpoint_rpc_via_tcp_tunnel(
            &endpoint,
            b"auth",
            &test_rpc_request(),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(5)),
            &tokio::runtime::Handle::current(),
            None,
        )
        .await;

        assert!(matches!(result, Some(Err(_))));
        assert!(TCP_TUNNELS.get(&node_id).is_none());
        assert!(handle.pending.lock().await.is_empty());
        assert!(handle.clean_shutdown.load(Ordering::Relaxed));
    }

    #[test]
    fn tcp_tunnel_real_endpoint_timeouts_leave_direct_fallback_budget() {
        let shaped = tcp_tunnel_real_endpoint_rpc_timeouts(DaemonRpcTimeouts::connect_and_idle(
            Duration::from_secs(5),
        ));

        assert_eq!(
            shaped.connect_timeout,
            Some(TCP_TUNNEL_REAL_ENDPOINT_RPC_TIMEOUT)
        );
        assert_eq!(
            shaped.idle_timeout,
            Some(TCP_TUNNEL_REAL_ENDPOINT_RPC_TIMEOUT)
        );
    }

    #[test]
    fn tcp_tunnel_real_endpoint_timeouts_preserve_tiny_deadlines() {
        let timeout = Duration::from_millis(25);
        let shaped =
            tcp_tunnel_real_endpoint_rpc_timeouts(DaemonRpcTimeouts::connect_and_idle(timeout));

        assert_eq!(shaped.connect_timeout, Some(timeout));
        assert_eq!(shaped.idle_timeout, Some(timeout));
    }

    #[tokio::test]
    async fn tcp_tunnel_real_lan_endpoint_reuses_existing_tunnel() {
        let (tx, mut rx) = mpsc::channel(1);
        let node_name = "tcp_tunnel_real_lan_reuses_existing";
        let node_id = NodeId::from_legacy_named(node_name).expect("test node id");
        let handle = test_tunnel_handle(node_name, tx);
        TCP_TUNNELS.insert(handle.clone());
        let endpoint = test_endpoint(
            "tcp_tunnel_real_lan_reuses_existing_endpoint",
            &node_id,
            "10.0.0.2:21353",
            ClusterTransportKind::LanTcp,
        );
        let endpoint_for_send = endpoint.clone();
        let request = test_rpc_request();

        let send_task = tokio::spawn(async move {
            send_signed_endpoint_rpc_via_tcp_tunnel(
                &endpoint_for_send,
                b"auth",
                &request,
                DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
                &tokio::runtime::Handle::current(),
                None,
            )
            .await
        });
        let id = match rx.recv().await.expect("tunnel request") {
            ClusterTcpTunnelFrame::Request { id, request } => {
                assert_eq!(request.route, "/test/tcp-tunnel");
                assert_eq!(request.auth.as_deref(), Some(b"auth".as_slice()));
                id
            }
            other => panic!("expected tunnel request, got {other:?}"),
        };
        let pending = handle
            .pending
            .lock()
            .await
            .remove(&id)
            .expect("pending tunnel request");
        pending
            .tx
            .send(Ok(ObjectRpcResponse::from_encoded_body(
                "/test/tcp-tunnel",
                Vec::new(),
            )))
            .expect("send tunnel response");

        let result = send_task.await.expect("join send task");
        assert!(matches!(result, Some(Ok(_))));
        assert!(TCP_TUNNELS.get(&node_id).is_some());
        assert!(handle.pending.lock().await.is_empty());
        assert!(!handle.clean_shutdown.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn tcp_tunnel_real_lan_endpoint_returns_signed_ambiguous_retry_to_route_layer() {
        let (tx, mut rx) = mpsc::channel(1);
        let node_name = "tcp_tunnel_real_lan_existing_timeout_fallback";
        let node_id = NodeId::from_legacy_named(node_name).expect("test node id");
        let handle = test_tunnel_handle(node_name, tx);
        TCP_TUNNELS.insert(handle.clone());
        let endpoint = test_endpoint(
            "tcp_tunnel_real_lan_existing_timeout_fallback_endpoint",
            &node_id,
            "10.0.0.2:21353",
            ClusterTransportKind::LanTcp,
        );

        let result = send_signed_endpoint_rpc_via_tcp_tunnel(
            &endpoint,
            b"auth",
            &test_rpc_request(),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(1)),
            &tokio::runtime::Handle::current(),
            None,
        )
        .await;

        assert!(matches!(
            result,
            Some(Err(NetworkError::Timeout {
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            }))
        ));
        assert!(Arc::ptr_eq(
            &TCP_TUNNELS.get(&node_id).expect("tunnel retained"),
            &handle
        ));
        assert!(handle.pending.lock().await.is_empty());
        assert!(!handle.clean_shutdown.load(Ordering::Relaxed));
        let _ = rx.recv().await.expect("first ambiguous tunnel request");

        let retry_safe_result = send_signed_endpoint_rpc_via_tcp_tunnel(
            &endpoint,
            b"auth",
            &test_rpc_request().with_response_retry_safe(),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(1)),
            &tokio::runtime::Handle::current(),
            None,
        )
        .await;
        assert!(matches!(
            retry_safe_result,
            Some(Err(NetworkError::Timeout {
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            }))
        ));
        let _ = rx
            .recv()
            .await
            .expect("retry-safe ambiguous tunnel request");

        TCP_TUNNELS.remove(&node_id);
    }

    #[tokio::test]
    async fn tcp_tunnel_real_lan_endpoint_falls_back_when_open_fails() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed endpoint");
        let address = listener.local_addr().expect("listener address").to_string();
        drop(listener);
        let node_id =
            NodeId::from_legacy_named("tcp_tunnel_real_lan_open_fallback").expect("test node id");
        let endpoint = test_endpoint(
            "tcp_tunnel_real_lan_open_fallback_endpoint",
            &node_id,
            &address,
            ClusterTransportKind::LanTcp,
        );

        let result = send_signed_endpoint_rpc_via_tcp_tunnel(
            &endpoint,
            b"auth",
            &test_rpc_request(),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(20)),
            &tokio::runtime::Handle::current(),
            None,
        )
        .await;

        assert!(result.is_none());
        assert!(TCP_TUNNELS.get(&node_id).is_none());
    }

    #[test]
    fn tcp_tunnel_maintenance_allows_either_peer_to_dial() {
        let low = NodeId::from_legacy_named("tcp_tunnel_maintenance_low").expect("low node id");
        let high = NodeId::from_legacy_named("tcp_tunnel_maintenance_high").expect("high node id");
        let (first, second) = if low < high { (low, high) } else { (high, low) };

        assert!(tcp_tunnel_should_maintain_outbound(&first, &second));
        assert!(tcp_tunnel_should_maintain_outbound(&second, &first));
        assert!(!tcp_tunnel_should_maintain_outbound(&first, &first));
    }

    #[test]
    fn tcp_tunnel_canonical_direction_is_lower_node_outbound() {
        let low = NodeId::from_legacy_named("tcp_tunnel_direction_low").expect("low node id");
        let high = NodeId::from_legacy_named("tcp_tunnel_direction_high").expect("high node id");
        let (low, high) = if low < high { (low, high) } else { (high, low) };

        assert!(tcp_tunnel_direction_is_canonical(
            &low,
            &high,
            TcpTunnelDirection::Outbound
        ));
        assert!(tcp_tunnel_direction_is_canonical(
            &high,
            &low,
            TcpTunnelDirection::Inbound
        ));
        assert!(!tcp_tunnel_direction_is_canonical(
            &low,
            &high,
            TcpTunnelDirection::Inbound
        ));
        assert!(!tcp_tunnel_direction_is_canonical(
            &high,
            &low,
            TcpTunnelDirection::Outbound
        ));
    }

    #[test]
    fn tcp_tunnel_maintenance_keeps_all_peer_endpoints_until_connected() {
        let low =
            NodeId::from_legacy_named("tcp_tunnel_maintenance_multi_low").expect("low node id");
        let high =
            NodeId::from_legacy_named("tcp_tunnel_maintenance_multi_high").expect("high node id");
        let (local, peer) = if low < high { (low, high) } else { (high, low) };
        let endpoints = vec![
            test_endpoint(
                "tcp_tunnel_maintenance_dead_private",
                &peer,
                "10.0.0.41:21353",
                ClusterTransportKind::LanTcp,
            ),
            test_endpoint(
                "tcp_tunnel_maintenance_public",
                &peer,
                "198.51.100.76:21353",
                ClusterTransportKind::LanTcp,
            ),
            tcp_tunnel_synthetic_endpoint(&local, &peer),
            test_endpoint(
                "tcp_tunnel_maintenance_loopback",
                &peer,
                "127.0.0.1:21353",
                ClusterTransportKind::Loopback,
            ),
            test_endpoint(
                "tcp_tunnel_maintenance_shadow",
                &peer,
                "10.0.0.41:21354",
                ClusterTransportKind::LanTcp,
            ),
            test_endpoint(
                "tcp_tunnel_maintenance_self",
                &local,
                "10.14.1.11:21353",
                ClusterTransportKind::LanTcp,
            ),
        ];

        let candidates = tcp_tunnel_maintenance_candidate_endpoints(&local, endpoints);

        assert_eq!(
            candidates
                .iter()
                .map(|endpoint| endpoint.address.as_str())
                .collect::<Vec<_>>(),
            vec!["10.0.0.41:21353", "198.51.100.76:21353"]
        );
    }

    #[tokio::test]
    async fn tcp_tunnel_maintenance_prunes_synthetic_record_without_handle() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-tcp-tunnel-prune-stale");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = time::OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_tcp_tunnel_prune")
                .expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_tcp_tunnel_prune_local").expect("local node id");
        let peer = NodeId::from_legacy_named("node_tcp_tunnel_prune_peer").expect("peer node id");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "tcp tunnel prune",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        let synthetic = tcp_tunnel_synthetic_endpoint(&local, &peer);
        let direct = test_endpoint(
            "tcp_tunnel_prune_direct",
            &peer,
            "10.14.1.11:21353",
            ClusterTransportKind::LanTcp,
        );
        let synthetic_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &synthetic,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(0),
            0,
            Some(now.to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let direct_record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &direct,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some(now.to_string()),
            None,
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &cluster_paths,
            &local,
            &[synthetic_record, direct_record.clone()],
        )
        .expect("write connectivity");

        prune_stale_tcp_tunnel_connectivity(&daemon_paths, &tokio::runtime::Handle::current())
            .await
            .expect("prune stale synthetic tunnel record");

        let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            &cluster_paths,
            &local,
        )
        .expect("connectivity records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].endpoint_id, direct_record.endpoint_id);
        assert_eq!(records[0].address, direct_record.address);
    }

    #[test]
    fn tcp_tunnel_peer_endpoint_falls_back_to_probe_candidate() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-tcp-tunnel-probe-fallback");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let daemon_paths = DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = time::OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_tcp_tunnel_probe_fallback")
                .expect("cluster id");
        let local = NodeId::from_legacy_named("node_tcp_tunnel_probe_fallback_local")
            .expect("local node id");
        let peer =
            NodeId::from_legacy_named("node_tcp_tunnel_probe_fallback_peer").expect("peer node id");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "tcp tunnel probe fallback",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut peer_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id,
            peer.clone(),
            "peer",
            "pk-peer",
            now,
        );
        peer_member.capabilities = sinomo_core::node::main_daemon_capabilities(&peer_member.role);
        peer_member.allowed_capabilities = peer_member.capabilities.clone();
        let endpoint = test_endpoint(
            "tcp_tunnel_probe_fallback_endpoint",
            &peer,
            "10.14.1.11:21353",
            ClusterTransportKind::LanTcp,
        );
        peer_member.endpoints = vec![endpoint.clone()];
        state.members.push(peer_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        let record = sinomo_core::network::cluster_connectivity_record(
            &state,
            &endpoint,
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            None,
            sinomo_core::network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
            Some(now.to_string()),
            now,
            sinomo_core::network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        );
        sinomo_core::network::write_local_cluster_connectivity_record_set(
            &cluster_paths,
            &local,
            &[record],
        )
        .expect("write connectivity");
        let command_candidates =
            sinomo_core::network::ClusterEndpointSelector::new(&cluster_paths, &state)
                .select(ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates);
        assert!(
            command_candidates
                .iter()
                .all(|candidate| candidate.endpoint_id != endpoint.endpoint_id),
            "the setup should model a peer endpoint suppressed by command selection"
        );

        let selected = tcp_tunnel_connectivity_endpoint_for_peer(&daemon_paths, &peer)
            .expect("probe fallback endpoint");

        assert_eq!(selected.endpoint_id, endpoint.endpoint_id);
        assert_eq!(selected.address, endpoint.address);
    }

    #[test]
    fn tcp_tunnel_connect_lock_is_stable_per_peer() {
        let registry = TcpTunnelRegistry::default();
        let peer = NodeId::from_legacy_named("tcp_tunnel_connect_lock_peer").expect("test node id");

        let first = registry.connect_lock(&peer);
        let second = registry.connect_lock(&peer);

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn tcp_tunnel_duplicate_insert_keeps_active_existing_handle() {
        let registry = TcpTunnelRegistry::default();
        let peer_name = "tcp_tunnel_duplicate_insert_peer";
        let peer = NodeId::from_legacy_named(peer_name).expect("test node id");
        let (first_tx, _first_rx) = mpsc::channel(1);
        let (second_tx, _second_rx) = mpsc::channel(1);
        let first = test_tunnel_handle(peer_name, first_tx);
        let second = test_tunnel_handle(peer_name, second_tx);

        assert!(registry.insert_or_existing(first.clone()).is_ok());
        first.active_requests.fetch_add(1, Ordering::Relaxed);
        let duplicate = registry.insert_or_existing(second.clone());

        assert!(duplicate.is_err());
        let existing = duplicate.err().expect("existing handle");
        assert!(Arc::ptr_eq(&existing, &first));
        assert!(Arc::ptr_eq(
            &registry.get(&peer).expect("registered"),
            &first
        ));
        assert!(!first.clean_shutdown.load(Ordering::Relaxed));
    }

    #[test]
    fn tcp_tunnel_duplicate_insert_replaces_idle_reverse_handle_with_canonical() {
        let registry = TcpTunnelRegistry::default();
        let peer_name = "tcp_tunnel_duplicate_idle_peer";
        let peer = NodeId::from_legacy_named(peer_name).expect("test node id");
        let (first_tx, _first_rx) = mpsc::channel(1);
        let (second_tx, _second_rx) = mpsc::channel(1);
        let first = test_tunnel_handle_with_direction(peer_name, first_tx, false);
        let second = test_tunnel_handle_with_direction(peer_name, second_tx, true);

        assert!(registry.insert_or_existing(first.clone()).is_ok());
        assert!(registry.insert_or_existing(second.clone()).is_ok());

        assert!(Arc::ptr_eq(
            &registry.get(&peer).expect("registered"),
            &second
        ));
        assert!(first.clean_shutdown.load(Ordering::Relaxed));
        assert!(!second.clean_shutdown.load(Ordering::Relaxed));
    }

    #[test]
    fn tcp_tunnel_simultaneous_open_converges_on_canonical_direction() {
        let peer_name = "tcp_tunnel_simultaneous_open_peer";
        let lower_registry = TcpTunnelRegistry::default();
        let higher_registry = TcpTunnelRegistry::default();
        let (lower_outbound_tx, _lower_outbound_rx) = mpsc::channel(1);
        let (higher_inbound_tx, _higher_inbound_rx) = mpsc::channel(1);
        let (higher_outbound_tx, _higher_outbound_rx) = mpsc::channel(1);
        let (lower_inbound_tx, _lower_inbound_rx) = mpsc::channel(1);
        let lower_outbound = test_tunnel_handle_with_direction(peer_name, lower_outbound_tx, true);
        let higher_inbound = test_tunnel_handle_with_direction(peer_name, higher_inbound_tx, true);
        let higher_outbound =
            test_tunnel_handle_with_direction(peer_name, higher_outbound_tx, false);
        let lower_inbound = test_tunnel_handle_with_direction(peer_name, lower_inbound_tx, false);

        assert!(
            lower_registry
                .insert_or_existing(lower_outbound.clone())
                .is_ok()
        );
        assert!(
            higher_registry
                .insert_or_existing(higher_outbound.clone())
                .is_ok()
        );

        let rejected_reverse = lower_registry.insert_or_existing(lower_inbound);
        assert!(rejected_reverse.is_err());
        assert!(
            higher_registry
                .insert_or_existing(higher_inbound.clone())
                .is_ok()
        );

        assert!(Arc::ptr_eq(
            &lower_registry
                .get(&lower_outbound.peer_node_id)
                .expect("lower canonical tunnel"),
            &lower_outbound
        ));
        assert!(Arc::ptr_eq(
            &higher_registry
                .get(&higher_inbound.peer_node_id)
                .expect("higher canonical tunnel"),
            &higher_inbound
        ));
        assert!(higher_outbound.clean_shutdown.load(Ordering::Relaxed));
    }
}
