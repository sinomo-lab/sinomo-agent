use crate::{
    AuthenticatedDaemonCommand, DaemonError, DaemonPaths, daemon_rpc_body_sha256,
    sign_daemon_rpc_request_digest_for_local_node,
};
use sinomo_core::cluster::read_cluster_state_without_kv;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    AsyncDaemonControlStream, CLUSTER_TCP_TUNNEL_OPEN_ROUTE, CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
    CLUSTER_TCP_TUNNEL_RPC_CATEGORY, ClusterEndpointSelectionPurpose, ClusterTcpTunnelFrame,
    ClusterTcpTunnelOpenRequest, ClusterTcpTunnelOpenResponse, DaemonRpcTimeouts, NetworkError,
    ObjectFrameIoOptions, ObjectRpcRequest, ObjectRpcResponse, read_object_frame_async,
    write_object_frame_async,
};
use sinomo_core::node::NodeId;
use sinomo_core::{ClusterEndpointRecord, ClusterTransportKind};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};

const TCP_TUNNEL_CHANNEL_CAPACITY: usize = 128;
const TCP_TUNNEL_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(15);
const TCP_TUNNEL_CONNECT_TIMEOUT: Duration = Duration::from_millis(1500);

type PendingTcpTunnelMap =
    Arc<tokio::sync::Mutex<HashMap<u64, oneshot::Sender<Result<ObjectRpcResponse, NetworkError>>>>>;

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

    fn insert(&self, handle: Arc<TcpTunnelHandle>) {
        let Ok(mut peers) = self.peers.lock() else {
            return;
        };
        if let Some(previous) = peers.insert(handle.peer_node_id.clone(), handle) {
            previous.request_close("replaced by newer tunnel".to_string());
        }
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
}

struct TcpTunnelHandle {
    peer_node_id: NodeId,
    tx: mpsc::Sender<ClusterTcpTunnelFrame>,
    pending: PendingTcpTunnelMap,
    next_request_id: AtomicU64,
    clean_shutdown: AtomicBool,
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
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        let frame = ClusterTcpTunnelFrame::Request { id, request };
        let send = self.tx.send(frame);
        let send_result = if let Some(timeout) = timeouts.connect_timeout {
            match tokio::time::timeout(timeout, send).await {
                Ok(result) => result,
                Err(_) => {
                    self.pending.lock().await.remove(&id);
                    return Err(tcp_tunnel_timeout_error("send tcp tunnel request", timeout));
                }
            }
        } else {
            send.await
        };
        if send_result.is_err() {
            self.pending.lock().await.remove(&id);
            return Err(NetworkError::Io {
                operation: "send tcp tunnel request".to_string(),
                detail: "tunnel writer is closed".to_string(),
            });
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
                Err(NetworkError::Io {
                    operation: "read tcp tunnel response".to_string(),
                    detail: "tunnel reader is closed".to_string(),
                })
            }
        }
    }

    async fn fail_pending(&self, detail: String) {
        let pending = {
            let mut guard = self.pending.lock().await;
            std::mem::take(&mut *guard)
        };
        for sender in pending.into_values() {
            let _ = sender.send(Err(NetworkError::Io {
                operation: "tcp tunnel request".to_string(),
                detail: detail.clone(),
            }));
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
            .write_response(&stream_address, &response, ObjectFrameIoOptions::none())
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
        .write_response(&stream_address, &response, ObjectFrameIoOptions::none())
        .await
        .map_err(DaemonError::from)?;
    let endpoint = tcp_tunnel_connectivity_endpoint_for_peer(&paths, &peer_node_id);
    if let Some(endpoint) = endpoint.as_ref() {
        remember_tcp_tunnel_connectivity(
            &paths,
            endpoint,
            sinomo_core::network::ClusterConnectivityState::Healthy,
            Some(&service_runtime),
        )
        .await;
    }
    spawn_tcp_tunnel(
        peer_node_id,
        stream.into_inner(),
        paths,
        service_runtime,
        endpoint,
    );
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
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return match send_over_tcp_tunnel_handle(handle, signed, timeouts).await {
            Ok(response) => Some(Ok(response)),
            Err(error) => {
                tracing::debug!(
                    "tcp tunnel send to {} ({}) failed, falling back to direct endpoint rpc: {error}",
                    endpoint.node_id,
                    endpoint.address
                );
                None
            }
        };
    }
    match connect_tcp_tunnel_to_endpoint(endpoint, runtime, paths).await {
        Ok(handle) => match send_over_tcp_tunnel_handle(handle, signed, timeouts).await {
            Ok(response) => Some(Ok(response)),
            Err(error) => {
                tracing::debug!(
                    "tcp tunnel send to {} ({}) failed after open, falling back to direct endpoint rpc: {error}",
                    endpoint.node_id,
                    endpoint.address
                );
                None
            }
        },
        Err(error) => {
            tracing::debug!(
                "tcp tunnel open to {} ({}) failed: {error}",
                endpoint.node_id,
                endpoint.address
            );
            None
        }
    }
}

async fn send_over_tcp_tunnel_handle(
    handle: Arc<TcpTunnelHandle>,
    request: ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
) -> Result<ObjectRpcResponse, NetworkError> {
    let result = handle.send(request, timeouts).await;
    if let Err(error) = result.as_ref() {
        TCP_TUNNELS.remove_if_same(&handle);
        let detail = format!("tcp tunnel evicted after request failure: {error}");
        handle.request_close(detail.clone());
        handle.fail_pending(detail).await;
    }
    result
}

fn tcp_tunnel_should_maintain_outbound(local_node_id: &NodeId, peer_node_id: &NodeId) -> bool {
    local_node_id < peer_node_id
}

async fn maintain_tcp_tunnels_once(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(paths)?;
    let state = read_cluster_state_without_kv(&cluster_paths).map_err(DaemonError::from)?;
    let endpoints = sinomo_core::network::ClusterEndpointSelector::new(&cluster_paths, &state)
        .select(ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates);
    let mut attempted = BTreeMap::<NodeId, ClusterEndpointRecord>::new();
    for endpoint in endpoints {
        if endpoint.node_id == state.local_node_id
            || endpoint.transport != ClusterTransportKind::LanTcp
            || !tcp_tunnel_should_maintain_outbound(&state.local_node_id, &endpoint.node_id)
            || TCP_TUNNELS.contains(&endpoint.node_id)
            || attempted.contains_key(&endpoint.node_id)
        {
            continue;
        }
        attempted.insert(endpoint.node_id.clone(), endpoint);
    }
    for endpoint in attempted.into_values() {
        let _ = connect_tcp_tunnel_to_endpoint(&endpoint, runtime, Some(paths)).await;
    }
    Ok(())
}

async fn connect_tcp_tunnel_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
) -> Result<Arc<TcpTunnelHandle>, NetworkError> {
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return Ok(handle);
    }
    let connect_lock = TCP_TUNNELS.connect_lock(&endpoint.node_id);
    let _connect_guard = connect_lock.lock().await;
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return Ok(handle);
    }
    let mut stream = AsyncDaemonControlStream::connect_with_options(
        &endpoint.address,
        DaemonRpcTimeouts::connect_and_idle(TCP_TUNNEL_CONNECT_TIMEOUT),
    )
    .await?;
    let request = signed_tcp_tunnel_open_request(runtime).await?;
    stream
        .write_request(&endpoint.address, &request, ObjectFrameIoOptions::none())
        .await?;
    let response = stream
        .read_response(
            &endpoint.address,
            sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
            ObjectFrameIoOptions::none(),
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
    );
    remember_tcp_tunnel_connectivity(
        &paths,
        endpoint,
        sinomo_core::network::ClusterConnectivityState::Healthy,
        Some(runtime),
    )
    .await;
    Ok(handle)
}

async fn signed_tcp_tunnel_open_request(
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcRequest, NetworkError> {
    let mut request = ObjectRpcRequest::encode_body(
        CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
        CLUSTER_TCP_TUNNEL_RPC_CATEGORY,
        &ClusterTcpTunnelOpenRequest::default(),
    )?;
    let route = request.route.clone();
    let category = request.category.clone();
    let body_sha256 = daemon_rpc_body_sha256(&request);
    let signed = runtime
        .spawn_blocking(move || {
            sign_daemon_rpc_request_digest_for_local_node(&route, &category, body_sha256)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "sign tcp tunnel open request".to_string(),
            detail: format!("signing task failed: {source}"),
        })?
        .map_err(|error| NetworkError::Io {
            operation: "sign tcp tunnel open request".to_string(),
            detail: error.to_string(),
        })?;
    request.auth = Some(signed);
    Ok(request)
}

fn spawn_tcp_tunnel(
    peer_node_id: NodeId,
    stream: tokio::net::TcpStream,
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    connectivity_endpoint: Option<ClusterEndpointRecord>,
) -> Arc<TcpTunnelHandle> {
    let (read_half, write_half) = stream.into_split();
    let (tx, rx) = mpsc::channel(TCP_TUNNEL_CHANNEL_CAPACITY);
    let handle = Arc::new(TcpTunnelHandle {
        peer_node_id,
        tx,
        pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        next_request_id: AtomicU64::new(1),
        clean_shutdown: AtomicBool::new(false),
    });
    TCP_TUNNELS.insert(handle.clone());
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
        if write_object_frame_async(&mut write_half, &frame, ObjectFrameIoOptions::none())
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
                if let Some(sender) = handle.pending.lock().await.remove(&id) {
                    let _ = sender.send(Ok(response));
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

fn tcp_tunnel_connectivity_endpoint_for_peer(
    paths: &DaemonPaths,
    peer_node_id: &NodeId,
) -> Option<ClusterEndpointRecord> {
    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(paths).ok()?;
    let state = read_cluster_state_without_kv(&cluster_paths).ok()?;
    sinomo_core::network::ClusterEndpointSelector::new(&cluster_paths, &state)
        .select(ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates)
        .into_iter()
        .find(|endpoint| {
            endpoint.node_id == *peer_node_id && endpoint.transport == ClusterTransportKind::LanTcp
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
        crate::cluster::remember_cluster_endpoint_command_connectivity_without_failure_grace(
            &paths,
            &cluster_state,
            &endpoint,
            state,
            None,
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

fn tcp_tunnel_timeout_error(operation: &str, timeout: Duration) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: format!("timed out after {}ms", timeout.as_millis()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tunnel_handle(
        node_name: &str,
        tx: mpsc::Sender<ClusterTcpTunnelFrame>,
    ) -> Arc<TcpTunnelHandle> {
        Arc::new(TcpTunnelHandle {
            peer_node_id: NodeId::from_legacy_named(node_name).expect("test node id"),
            tx,
            pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            next_request_id: AtomicU64::new(1),
            clean_shutdown: AtomicBool::new(false),
        })
    }

    fn test_rpc_request() -> ObjectRpcRequest {
        ObjectRpcRequest::encode_body("/test/tcp-tunnel", "test", &()).expect("test request")
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
            Err(NetworkError::Io { operation, .. }) if operation == "send tcp tunnel request"
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
            Err(NetworkError::Io { operation, .. }) if operation == "read tcp tunnel response"
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

    #[test]
    fn tcp_tunnel_maintenance_uses_one_canonical_outbound_dialer() {
        let low = NodeId::from_legacy_named("tcp_tunnel_maintenance_low").expect("low node id");
        let high = NodeId::from_legacy_named("tcp_tunnel_maintenance_high").expect("high node id");
        let (first, second) = if low < high { (low, high) } else { (high, low) };

        assert!(tcp_tunnel_should_maintain_outbound(&first, &second));
        assert!(!tcp_tunnel_should_maintain_outbound(&second, &first));
    }

    #[test]
    fn tcp_tunnel_connect_lock_is_stable_per_peer() {
        let registry = TcpTunnelRegistry::default();
        let peer = NodeId::from_legacy_named("tcp_tunnel_connect_lock_peer").expect("test node id");

        let first = registry.connect_lock(&peer);
        let second = registry.connect_lock(&peer);

        assert!(Arc::ptr_eq(&first, &second));
    }
}
