use crate::{
    AuthenticatedDaemonCommand, DaemonError, DaemonPaths, daemon_rpc_body_sha256,
    sign_daemon_rpc_request_digest_for_local_node,
};
use sinomo_core::cluster::read_cluster_state_without_kv;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    AsyncDaemonControlStream, CLUSTER_TCP_TUNNEL_OPEN_ROUTE, CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
    CLUSTER_TCP_TUNNEL_RPC_CATEGORY, ClusterEndpointSelectionPurpose,
    ClusterRelayStreamSendOutcome, ClusterTcpTunnelFrame, ClusterTcpTunnelOpenRequest,
    ClusterTcpTunnelOpenResponse, DaemonRpcTimeouts, NetworkError, ObjectFrameIoOptions,
    ObjectRpcRequest, ObjectRpcResponse, read_object_frame_async, write_object_frame_async,
};
use sinomo_core::node::NodeId;
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterTransportKind,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};

const TCP_TUNNEL_CHANNEL_CAPACITY: usize = 128;
const TCP_TUNNEL_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(2);
const TCP_TUNNEL_CONNECT_TIMEOUT: Duration = Duration::from_millis(1500);
const TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX: &str = "tcp-tunnel://";

type PendingTcpTunnelMap = Arc<tokio::sync::Mutex<HashMap<u64, PendingTcpTunnelRequest>>>;

struct PendingTcpTunnelRequest {
    kind: PendingTcpTunnelRequestKind,
}

enum PendingTcpTunnelRequestKind {
    Collect {
        tx: oneshot::Sender<Result<ObjectRpcResponse, NetworkError>>,
        stream: Option<PendingTcpTunnelStreamResponse>,
    },
    Forward {
        tx: mpsc::Sender<Result<TcpTunnelStreamForwardEvent, NetworkError>>,
    },
}

enum TcpTunnelStreamForwardEvent {
    Response(ObjectRpcResponse),
    Chunk(Vec<u8>),
    End,
}

struct PendingTcpTunnelStreamResponse {
    response: ObjectRpcResponse,
    body: Vec<u8>,
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
            return Err(existing);
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

struct TcpTunnelHandle {
    peer_node_id: NodeId,
    tx: mpsc::Sender<ClusterTcpTunnelFrame>,
    pending: PendingTcpTunnelMap,
    next_request_id: AtomicU64,
    active_requests: AtomicUsize,
    clean_shutdown: AtomicBool,
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
        self.pending.lock().await.insert(
            id,
            PendingTcpTunnelRequest {
                kind: PendingTcpTunnelRequestKind::Collect { tx, stream: None },
            },
        );
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

    async fn forward_stream(
        &self,
        request: ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
        upstream: &mut AsyncDaemonControlStream,
        upstream_address: &str,
        max_response_bytes: usize,
    ) -> Result<ClusterRelayStreamSendOutcome, NetworkError> {
        self.active_requests.fetch_add(1, Ordering::Relaxed);
        let _active_request = TcpTunnelActiveRequestGuard(&self.active_requests);
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel(TCP_TUNNEL_CHANNEL_CAPACITY);
        self.pending.lock().await.insert(
            id,
            PendingTcpTunnelRequest {
                kind: PendingTcpTunnelRequestKind::Forward { tx },
            },
        );
        let send = self.tx.send(ClusterTcpTunnelFrame::Request { id, request });
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

        let frame_options = ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
        let mut response_started = false;
        let mut streamed_body = false;
        let mut total_body_bytes: usize = 0;
        loop {
            let next = if let Some(timeout) = timeouts.idle_timeout.or(timeouts.connect_timeout) {
                match tokio::time::timeout(timeout, rx.recv()).await {
                    Ok(event) => event,
                    Err(_) => {
                        self.pending.lock().await.remove(&id);
                        let error = tcp_tunnel_timeout_error("read tcp tunnel stream", timeout);
                        return if response_started {
                            Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                                error,
                            ))
                        } else {
                            Err(error)
                        };
                    }
                }
            } else {
                rx.recv().await
            };
            let Some(next) = next else {
                self.pending.lock().await.remove(&id);
                let error = NetworkError::Io {
                    operation: "read tcp tunnel stream".to_string(),
                    detail: "tunnel reader is closed".to_string(),
                };
                return if response_started {
                    Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                        error,
                    ))
                } else {
                    Err(error)
                };
            };
            let event = match next {
                Ok(event) => event,
                Err(error) => {
                    self.pending.lock().await.remove(&id);
                    return if response_started {
                        Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                            error,
                        ))
                    } else {
                        Err(error)
                    };
                }
            };
            match event {
                TcpTunnelStreamForwardEvent::Response(response) => {
                    if sinomo_core::network::relay_response_is_routing_error(&response) {
                        self.pending.lock().await.remove(&id);
                        return Ok(ClusterRelayStreamSendOutcome::RoutingError(response));
                    }
                    streamed_body = sinomo_core::network::object_rpc_response_stream_mode(
                        &response,
                    )
                        == sinomo_core::network::ObjectRpcResponseStreamMode::RawStreamedDaemonBody;
                    if let Err(error) = upstream
                        .write_rpc_response(upstream_address, &response, frame_options)
                        .await
                    {
                        self.pending.lock().await.remove(&id);
                        return Err(error);
                    }
                    response_started = true;
                    if !streamed_body {
                        self.pending.lock().await.remove(&id);
                        return Ok(ClusterRelayStreamSendOutcome::Forwarded);
                    }
                }
                TcpTunnelStreamForwardEvent::Chunk(bytes) => {
                    if !response_started || !streamed_body {
                        self.pending.lock().await.remove(&id);
                        let error = NetworkError::Io {
                            operation: "read tcp tunnel stream".to_string(),
                            detail: "tcp tunnel received stream chunk before streamed response"
                                .to_string(),
                        };
                        return if response_started {
                            Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                                error,
                            ))
                        } else {
                            Err(error)
                        };
                    }
                    let Some(next_total_body_bytes) = total_body_bytes.checked_add(bytes.len())
                    else {
                        return Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                            NetworkError::Io {
                                operation: "read tcp tunnel stream".to_string(),
                                detail: "tcp tunnel streamed response size overflow".to_string(),
                            },
                        ));
                    };
                    total_body_bytes = next_total_body_bytes;
                    if total_body_bytes > max_response_bytes {
                        self.pending.lock().await.remove(&id);
                        return Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                            NetworkError::Io {
                                operation: "read tcp tunnel stream".to_string(),
                                detail: format!(
                                    "tcp tunnel streamed response exceeded {max_response_bytes} bytes"
                                ),
                            },
                        ));
                    }
                    if let Err(error) = upstream
                        .write_raw_byte_stream_chunk(upstream_address, &bytes, frame_options)
                        .await
                    {
                        self.pending.lock().await.remove(&id);
                        return Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                            error,
                        ));
                    }
                }
                TcpTunnelStreamForwardEvent::End => {
                    self.pending.lock().await.remove(&id);
                    if streamed_body {
                        if let Err(error) = upstream
                            .write_raw_byte_stream_chunk(upstream_address, &[], frame_options)
                            .await
                        {
                            return Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
                                error,
                            ));
                        }
                    }
                    return Ok(ClusterRelayStreamSendOutcome::Forwarded);
                }
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
            match sender.kind {
                PendingTcpTunnelRequestKind::Collect { tx, .. } => {
                    let _ = tx.send(Err(error));
                }
                PendingTcpTunnelRequestKind::Forward { tx } => {
                    let _ = tx.send(Err(error)).await;
                }
            }
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
    if !tcp_tunnel_is_synthetic_endpoint(endpoint) {
        spawn_tcp_tunnel_connect_attempt(endpoint, runtime, paths);
        return None;
    }
    let mut signed = request.clone();
    signed.auth = Some(auth.to_vec());
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
    Some(Err(NetworkError::Io {
        operation: "send tcp tunnel request".to_string(),
        detail: format!(
            "no persistent tcp tunnel to {} ({})",
            endpoint.node_id, endpoint.address
        ),
    }))
}

pub(crate) async fn forward_signed_endpoint_rpc_via_tcp_tunnel(
    endpoint: &ClusterEndpointRecord,
    auth: &[u8],
    request: &ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
    upstream: &mut AsyncDaemonControlStream,
    upstream_address: &str,
    max_response_bytes: usize,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
) -> Option<Result<ClusterRelayStreamSendOutcome, NetworkError>> {
    if endpoint.transport != ClusterTransportKind::LanTcp {
        return None;
    }
    if !tcp_tunnel_is_synthetic_endpoint(endpoint) {
        spawn_tcp_tunnel_connect_attempt(endpoint, runtime, paths);
        return None;
    }
    let mut signed = request.clone();
    signed.auth = Some(auth.to_vec());
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return match forward_stream_over_tcp_tunnel_handle(
            handle,
            signed,
            timeouts,
            upstream,
            upstream_address,
            max_response_bytes,
        )
        .await
        {
            Ok(response) => Some(Ok(response)),
            Err(error) => {
                tracing::debug!(
                    "tcp tunnel stream forward to {} ({}) failed after request acceptance: {error}",
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
    Some(Err(NetworkError::Io {
        operation: "send tcp tunnel stream request".to_string(),
        detail: format!(
            "no persistent tcp tunnel to {} ({})",
            endpoint.node_id, endpoint.address
        ),
    }))
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
    let auth = sign_object_rpc_request_for_local_node(&request, runtime).await?;
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
    let result = handle.send(request, timeouts).await;
    if let Err(error) = result.as_ref() {
        TCP_TUNNELS.remove_if_same(&handle);
        let detail = format!("tcp tunnel evicted after request failure: {error}");
        handle.request_close(detail.clone());
        handle.fail_pending(detail).await;
    }
    result
}

async fn forward_stream_over_tcp_tunnel_handle(
    handle: Arc<TcpTunnelHandle>,
    request: ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
    upstream: &mut AsyncDaemonControlStream,
    upstream_address: &str,
    max_response_bytes: usize,
) -> Result<ClusterRelayStreamSendOutcome, NetworkError> {
    let result = handle
        .forward_stream(
            request,
            timeouts,
            upstream,
            upstream_address,
            max_response_bytes,
        )
        .await;
    if result.is_err() {
        TCP_TUNNELS.remove_if_same(&handle);
        let detail = result
            .as_ref()
            .err()
            .map(|error| format!("tcp tunnel evicted after streamed request failure: {error}"))
            .unwrap_or_else(|| "tcp tunnel evicted after streamed request failure".to_string());
        handle.request_close(detail.clone());
        handle.fail_pending(detail).await;
    }
    result
}

async fn send_tcp_tunnel_stream_response(
    id: u64,
    stream_response: sinomo_core::service::ServiceStreamResponse,
    tx: mpsc::Sender<ClusterTcpTunnelFrame>,
) {
    let _ = tx
        .send(ClusterTcpTunnelFrame::Response {
            id,
            response: stream_response.response,
        })
        .await;
    if let Some(reader) = stream_response.reader {
        let chunk_bytes = stream_response
            .chunk_bytes
            .clamp(1, sinomo_core::network::OBJECT_STREAM_MAX_CHUNK_BYTES);
        let tx_for_reader = tx.clone();
        let read = tokio::task::spawn_blocking(move || {
            send_tcp_tunnel_stream_chunks_blocking(id, reader, chunk_bytes, tx_for_reader)
        })
        .await;
        match read {
            Ok(Ok(())) => {}
            Ok(Err(detail)) => {
                let _ = tx
                    .send(ClusterTcpTunnelFrame::StreamError { id, detail })
                    .await;
                return;
            }
            Err(source) => {
                let _ = tx
                    .send(ClusterTcpTunnelFrame::StreamError {
                        id,
                        detail: format!("tcp tunnel stream reader task failed: {source}"),
                    })
                    .await;
                return;
            }
        }
    }
    let _ = tx.send(ClusterTcpTunnelFrame::StreamEnd { id }).await;
}

fn send_tcp_tunnel_stream_chunks_blocking(
    id: u64,
    mut reader: Box<dyn Read + Send>,
    chunk_bytes: usize,
    tx: mpsc::Sender<ClusterTcpTunnelFrame>,
) -> Result<(), String> {
    let mut buffer = vec![0_u8; chunk_bytes];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| source.to_string())?;
        if read == 0 {
            return Ok(());
        }
        tx.blocking_send(ClusterTcpTunnelFrame::StreamChunk {
            id,
            bytes: buffer[..read].to_vec(),
        })
        .map_err(|source| source.to_string())?;
    }
}

fn tcp_tunnel_should_maintain_outbound(local_node_id: &NodeId, peer_node_id: &NodeId) -> bool {
    local_node_id != peer_node_id
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
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return Ok(handle);
    }
    let connect_lock = TCP_TUNNELS.connect_lock(&endpoint.node_id);
    let _connect_guard = connect_lock.lock().await;
    connect_tcp_tunnel_to_endpoint_after_lock(endpoint, runtime, paths).await
}

async fn connect_tcp_tunnel_to_endpoint_after_lock(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
) -> Result<Arc<TcpTunnelHandle>, NetworkError> {
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

fn spawn_tcp_tunnel_connect_attempt(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
) {
    let connect_lock = TCP_TUNNELS.connect_lock(&endpoint.node_id);
    let Ok(connect_guard) = connect_lock.try_lock_owned() else {
        return;
    };
    let endpoint = endpoint.clone();
    let runtime_for_task = runtime.clone();
    let paths = paths.cloned().or_else(daemon_paths_from_env);
    runtime.spawn(async move {
        let _connect_guard = connect_guard;
        if let Err(error) =
            connect_tcp_tunnel_to_endpoint_after_lock(&endpoint, &runtime_for_task, paths.as_ref())
                .await
        {
            tracing::debug!(
                "background tcp tunnel open to {} ({}) failed: {error}",
                endpoint.node_id,
                endpoint.address
            );
        }
    });
}

async fn signed_tcp_tunnel_open_request(
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcRequest, NetworkError> {
    let mut request = ObjectRpcRequest::encode_body(
        CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
        CLUSTER_TCP_TUNNEL_RPC_CATEGORY,
        &ClusterTcpTunnelOpenRequest::default(),
    )?;
    request.auth = Some(sign_object_rpc_request_for_local_node(&request, runtime).await?);
    Ok(request)
}

async fn sign_object_rpc_request_for_local_node(
    request: &ObjectRpcRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<Vec<u8>, NetworkError> {
    let route = request.route.clone();
    let category = request.category.clone();
    let body_sha256 = daemon_rpc_body_sha256(&request);
    Ok(runtime
        .spawn_blocking(move || {
            sign_daemon_rpc_request_digest_for_local_node(&route, &category, body_sha256)
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
        active_requests: AtomicUsize::new(0),
        clean_shutdown: AtomicBool::new(false),
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
                ObjectFrameIoOptions::none(),
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
                    if sinomo_core::network::object_rpc_request_stream_mode(&request)
                        .expects_raw_streamed_response()
                    {
                        let stream_result = if request.route
                            == sinomo_core::network::CLUSTER_RELAY_ROUTE
                        {
                            crate::cluster::handle_daemon_cluster_relay_stream_collect_on_runtime(
                                paths.clone(),
                                request.clone(),
                                service_runtime,
                            )
                            .await
                        } else {
                            crate::service_adapter::dispatch_stream_rpc_in_process_async(
                                paths.clone(),
                                service_runtime,
                                request.clone(),
                            )
                            .await
                        };
                        match stream_result {
                            Ok(stream_response) => {
                                send_tcp_tunnel_stream_response(id, stream_response, tx).await;
                            }
                            Err(error) => {
                                let response = ObjectRpcResponse::encode_error(
                                    request.route.clone(),
                                    error.to_string(),
                                )
                                .unwrap_or_else(|_| ObjectRpcResponse {
                                    route: request.route,
                                    body: Vec::new(),
                                    metadata: BTreeMap::new(),
                                });
                                let _ = tx
                                    .send(ClusterTcpTunnelFrame::Response { id, response })
                                    .await;
                            }
                        }
                        return;
                    }
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
                if sinomo_core::network::object_rpc_response_stream_mode(&response)
                    == sinomo_core::network::ObjectRpcResponseStreamMode::RawStreamedDaemonBody
                {
                    let mut response = Some(response);
                    let forward_tx = {
                        let mut pending = handle.pending.lock().await;
                        match pending.get_mut(&id).map(|pending| &mut pending.kind) {
                            Some(PendingTcpTunnelRequestKind::Collect { stream, .. }) => {
                                *stream = Some(PendingTcpTunnelStreamResponse {
                                    response: response
                                        .take()
                                        .expect("streamed response retained for collect"),
                                    body: Vec::new(),
                                });
                                None
                            }
                            Some(PendingTcpTunnelRequestKind::Forward { tx }) => Some(tx.clone()),
                            None => None,
                        }
                    };
                    if let Some(tx) = forward_tx {
                        let _ = tx
                            .send(Ok(TcpTunnelStreamForwardEvent::Response(
                                response.expect("streamed response retained for forward"),
                            )))
                            .await;
                    }
                } else if let Some(pending) = handle.pending.lock().await.remove(&id) {
                    match pending.kind {
                        PendingTcpTunnelRequestKind::Collect { tx, .. } => {
                            let _ = tx.send(Ok(response));
                        }
                        PendingTcpTunnelRequestKind::Forward { tx } => {
                            let _ = tx
                                .send(Ok(TcpTunnelStreamForwardEvent::Response(response)))
                                .await;
                        }
                    }
                }
            }
            ClusterTcpTunnelFrame::StreamChunk { id, bytes } => {
                let mut forward_tx = None;
                let mut failure = None;
                {
                    let mut pending = handle.pending.lock().await;
                    if let Some(request) = pending.get_mut(&id) {
                        match &mut request.kind {
                            PendingTcpTunnelRequestKind::Collect { stream, .. } => {
                                if let Some(stream) = stream.as_mut() {
                                    if stream.body.try_reserve(bytes.len()).is_ok() {
                                        stream.body.extend_from_slice(&bytes);
                                    } else {
                                        failure = Some(
                                            "tcp tunnel streamed response exhausted memory"
                                                .to_string(),
                                        );
                                    }
                                } else {
                                    failure = Some(
                                        "tcp tunnel received stream chunk before streamed response frame"
                                            .to_string(),
                                    );
                                }
                            }
                            PendingTcpTunnelRequestKind::Forward { tx } => {
                                forward_tx = Some(tx.clone());
                            }
                        }
                    }
                }
                if let Some(detail) = failure {
                    if let Some(request) = handle.pending.lock().await.remove(&id) {
                        let error = NetworkError::Io {
                            operation: "read tcp tunnel stream".to_string(),
                            detail,
                        };
                        match request.kind {
                            PendingTcpTunnelRequestKind::Collect { tx, .. } => {
                                let _ = tx.send(Err(error));
                            }
                            PendingTcpTunnelRequestKind::Forward { tx } => {
                                let _ = tx.send(Err(error)).await;
                            }
                        }
                    }
                } else if let Some(tx) = forward_tx
                    && tx
                        .send(Ok(TcpTunnelStreamForwardEvent::Chunk(bytes)))
                        .await
                        .is_err()
                {
                    handle.pending.lock().await.remove(&id);
                }
            }
            ClusterTcpTunnelFrame::StreamEnd { id } => {
                if let Some(pending) = handle.pending.lock().await.remove(&id) {
                    match pending.kind {
                        PendingTcpTunnelRequestKind::Collect { tx, mut stream } => {
                            let result = match stream.take() {
                                Some(mut stream) => {
                                    stream.response.body = stream.body;
                                    Ok(stream.response)
                                }
                                None => Err(NetworkError::Io {
                                    operation: "read tcp tunnel stream".to_string(),
                                    detail:
                                        "tcp tunnel received stream end before streamed response frame"
                                            .to_string(),
                                }),
                            };
                            let _ = tx.send(result);
                        }
                        PendingTcpTunnelRequestKind::Forward { tx } => {
                            let _ = tx.send(Ok(TcpTunnelStreamForwardEvent::End)).await;
                        }
                    }
                }
            }
            ClusterTcpTunnelFrame::StreamError { id, detail } => {
                if let Some(pending) = handle.pending.lock().await.remove(&id) {
                    let error = NetworkError::Io {
                        operation: "read tcp tunnel stream".to_string(),
                        detail,
                    };
                    match pending.kind {
                        PendingTcpTunnelRequestKind::Collect { tx, .. } => {
                            let _ = tx.send(Err(error));
                        }
                        PendingTcpTunnelRequestKind::Forward { tx } => {
                            let _ = tx.send(Err(error)).await;
                        }
                    }
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

fn tcp_tunnel_timeout_error(operation: &str, timeout: Duration) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: format!("timed out after {}ms", timeout.as_millis()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{ClusterEndpointPrivacy, ClusterEndpointSource};

    fn test_tunnel_handle(
        node_name: &str,
        tx: mpsc::Sender<ClusterTcpTunnelFrame>,
    ) -> Arc<TcpTunnelHandle> {
        Arc::new(TcpTunnelHandle {
            peer_node_id: NodeId::from_legacy_named(node_name).expect("test node id"),
            tx,
            pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            next_request_id: AtomicU64::new(1),
            active_requests: AtomicUsize::new(0),
            clean_shutdown: AtomicBool::new(false),
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
            Some(Err(NetworkError::Io { operation, detail })) => {
                assert_eq!(operation, "send tcp tunnel request");
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
    fn tcp_tunnel_registry_keeps_existing_tunnel_on_duplicate_open() {
        let (existing_tx, _existing_rx) = mpsc::channel(1);
        let (duplicate_tx, _duplicate_rx) = mpsc::channel(1);
        let node_name = "tcp_tunnel_duplicate_retains_existing";
        let existing = test_tunnel_handle(node_name, existing_tx);
        let duplicate = test_tunnel_handle(node_name, duplicate_tx);
        let registry = TcpTunnelRegistry::default();

        assert!(registry.insert_or_existing(existing.clone()).is_ok());
        let returned = registry
            .insert_or_existing(duplicate.clone())
            .expect_err("duplicate should return existing handle");

        assert!(Arc::ptr_eq(&returned, &existing));
        assert!(Arc::ptr_eq(
            &registry
                .get(&existing.peer_node_id)
                .expect("registered existing tunnel"),
            &existing
        ));
        assert!(!existing.clean_shutdown.load(Ordering::Relaxed));
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

    #[tokio::test]
    async fn tcp_tunnel_does_not_hijack_real_lan_endpoint() {
        let (tx, _rx) = mpsc::channel(1);
        let node_name = "tcp_tunnel_real_lan_not_hijacked";
        let node_id = NodeId::from_legacy_named(node_name).expect("test node id");
        let handle = test_tunnel_handle(node_name, tx);
        TCP_TUNNELS.insert(handle.clone());
        let endpoint = test_endpoint(
            "tcp_tunnel_real_lan_not_hijacked_endpoint",
            &node_id,
            "10.0.0.2:21353",
            ClusterTransportKind::LanTcp,
        );

        let result = send_signed_endpoint_rpc_via_tcp_tunnel(
            &endpoint,
            b"auth",
            &test_rpc_request(),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(5)),
            &tokio::runtime::Handle::current(),
            None,
        )
        .await;

        assert!(result.is_none());
        assert!(TCP_TUNNELS.get(&node_id).is_some());
        assert!(handle.pending.lock().await.is_empty());
        assert!(!handle.clean_shutdown.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn tcp_tunnel_stream_forward_writes_chunks_to_upstream() {
        let (tx, mut rx) = mpsc::channel(4);
        let handle = test_tunnel_handle("tcp_tunnel_stream_forward_peer", tx);
        let upstream_listener =
            sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                .await
                .expect("bind upstream listener");
        let upstream_address = upstream_listener
            .local_addr_string()
            .expect("upstream address");
        let upstream_client = tokio::spawn({
            let upstream_address = upstream_address.clone();
            async move {
                let mut stream =
                    sinomo_core::network::AsyncDaemonControlStream::connect(&upstream_address)
                        .await
                        .expect("connect upstream");
                let frame_options = ObjectFrameIoOptions::none();
                let response = stream
                    .read_rpc_response("upstream", 1024, frame_options)
                    .await
                    .expect("read upstream response");
                assert_eq!(
                    sinomo_core::network::object_rpc_response_stream_mode(&response),
                    sinomo_core::network::ObjectRpcResponseStreamMode::RawStreamedDaemonBody
                );
                let mut raw = stream.into_inner();
                let mut bytes = Vec::new();
                sinomo_core::network::read_raw_byte_stream_chunks_to_vec_async(
                    &mut raw,
                    &mut bytes,
                    sinomo_core::network::RawByteStreamChunkOptions {
                        chunk_bytes: 1024,
                        max_total_bytes: Some(1024),
                        idle_timeout: Some(Duration::from_secs(1)),
                    },
                )
                .await
                .expect("read upstream chunks");
                bytes
            }
        });
        let mut upstream = upstream_listener.accept().await.expect("accept upstream");
        let forward = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle
                    .forward_stream(
                        test_rpc_request(),
                        DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
                        &mut upstream,
                        "upstream",
                        1024,
                    )
                    .await
                    .expect("forward stream")
            }
        });
        let id = match rx.recv().await.expect("tunnel request") {
            ClusterTcpTunnelFrame::Request { id, request } => {
                assert_eq!(request.route, "/test/tcp-tunnel");
                id
            }
            other => panic!("expected tunnel request, got {other:?}"),
        };
        let forward_tx = {
            let pending = handle.pending.lock().await;
            match pending.get(&id).map(|pending| &pending.kind) {
                Some(PendingTcpTunnelRequestKind::Forward { tx }) => tx.clone(),
                _ => panic!("expected pending forward request"),
            }
        };
        let mut response = ObjectRpcResponse::from_encoded_body("/test/tcp-tunnel", Vec::new());
        sinomo_core::network::set_object_rpc_response_stream_mode(
            &mut response,
            sinomo_core::network::ObjectRpcResponseStreamMode::RawStreamedDaemonBody,
        );
        forward_tx
            .send(Ok(TcpTunnelStreamForwardEvent::Response(response)))
            .await
            .expect("send response event");
        forward_tx
            .send(Ok(TcpTunnelStreamForwardEvent::Chunk(b"hello ".to_vec())))
            .await
            .expect("send chunk");
        forward_tx
            .send(Ok(TcpTunnelStreamForwardEvent::Chunk(b"stream".to_vec())))
            .await
            .expect("send chunk");
        forward_tx
            .send(Ok(TcpTunnelStreamForwardEvent::End))
            .await
            .expect("send end");

        assert!(matches!(
            forward.await.expect("forward task"),
            ClusterRelayStreamSendOutcome::Forwarded
        ));
        assert_eq!(
            upstream_client.await.expect("upstream client"),
            b"hello stream"
        );
        assert!(handle.pending.lock().await.is_empty());
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
    fn tcp_tunnel_duplicate_insert_keeps_idle_existing_handle() {
        let registry = TcpTunnelRegistry::default();
        let peer_name = "tcp_tunnel_duplicate_idle_peer";
        let peer = NodeId::from_legacy_named(peer_name).expect("test node id");
        let (first_tx, _first_rx) = mpsc::channel(1);
        let (second_tx, _second_rx) = mpsc::channel(1);
        let first = test_tunnel_handle(peer_name, first_tx);
        let second = test_tunnel_handle(peer_name, second_tx);

        assert!(registry.insert_or_existing(first.clone()).is_ok());
        let duplicate = registry.insert_or_existing(second.clone());

        assert!(duplicate.is_err());
        let existing = duplicate.err().expect("existing handle");
        assert!(Arc::ptr_eq(&existing, &first));
        assert!(Arc::ptr_eq(
            &registry.get(&peer).expect("registered"),
            &first
        ));
        assert!(!first.clean_shutdown.load(Ordering::Relaxed));
        assert!(!second.clean_shutdown.load(Ordering::Relaxed));
    }
}
