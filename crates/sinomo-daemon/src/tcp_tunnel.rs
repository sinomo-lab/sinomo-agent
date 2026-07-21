use crate::{AuthenticatedDaemonCommand, DaemonError, DaemonPaths, daemon_rpc_body_sha256};
use sinomo_core::cluster::{read_cluster_state_cached_shared, read_cluster_state_without_kv};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::__channel_carrier::{
    OBJECT_RPC_CHANNEL_DATA_MAGIC, ObjectRpcChannelDataFrame, ObjectRpcChannelReceiveBufferPool,
    ObjectRpcChannelTransportFrame, read_object_rpc_channel_transport_frame_async_timed,
    write_object_rpc_channel_transport_frame_async,
    write_object_rpc_channel_transport_frame_async_timed,
};
#[cfg(test)]
use sinomo_core::network::ObjectRpcChannelWireFrame;
use sinomo_core::network::{
    AsyncDaemonControlStream, CLUSTER_TCP_TUNNEL_HANDSHAKE_PROTOCOL, CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
    CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION, CLUSTER_TCP_TUNNEL_RPC_CATEGORY,
    ClusterEndpointSelectionPurpose, ClusterTcpTunnelFrame, ClusterTcpTunnelHandshakeProof,
    ClusterTcpTunnelHandshakeTranscript, ClusterTcpTunnelOpenRequest, ClusterTcpTunnelOpenResponse,
    DaemonRpcTimeouts, MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES, NetworkError, NetworkHealthPolicy,
    ObjectFrameIoOptions, ObjectFrameReadTiming, ObjectFrameReadinessProbe, ObjectRpcChannelClient,
    ObjectRpcChannelConfig, ObjectRpcChannelEgress, ObjectRpcChannelEndpoint,
    ObjectRpcChannelIngress, ObjectRpcChannelRole, ObjectRpcChannelServer, ObjectRpcRequest,
    ObjectRpcResponse, SinomoObjectCodec, cached_daemon_signing_public_key,
    daemon_auth_encode_public_key, negotiate_cluster_tcp_tunnel_protocol, network_timeline_flag,
    read_object_frame_async, read_object_frame_async_with_max_bytes_after_first_byte_timed,
    sign_cluster_tcp_tunnel_handshake_from_data_dir, verify_cluster_tcp_tunnel_handshake,
    write_object_frame_async, write_object_frame_async_timed,
};
use sinomo_core::node::{ClusterId, NodeId};
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterNodeRole,
    ClusterTransportKind,
};
#[cfg(test)]
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::{self, Cursor};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
#[cfg(test)]
use std::sync::{RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};
#[cfg(test)]
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{Notify, watch};
#[cfg(test)]
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
const TCP_TUNNEL_CHANNEL_CAPACITY: usize = 128;
const TCP_TUNNEL_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(2);
// Tunnel setup is an opportunistic fast path. Fail quickly so the route layer
// can use the ordinary direct endpoint within its five-second network budget.
const TCP_TUNNEL_CONNECT_TIMEOUT: Duration = Duration::from_millis(1500);
const TCP_TUNNEL_FRAME_NO_PROGRESS_TIMEOUT: Duration =
    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
// Real endpoints reserve most of their attempt for direct fallback when a
// cached tunnel is stale; synthetic tunnel-only endpoints do not use this cap.
const TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX: &str =
    sinomo_core::network::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX;
const TCP_TUNNEL_HANDSHAKE_UNAVAILABLE_DETAIL: &str =
    "peer did not provide the authenticated tcp tunnel v1 handshake";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TcpTunnelDirection {
    Inbound,
    Outbound,
}

static TCP_TUNNELS: LazyLock<TcpTunnelRegistry> = LazyLock::new(TcpTunnelRegistry::default);
// A few cross-layer tests need an authenticated logical channel without a
// process-global physical tunnel. Keep those explicit clients outside the
// production registry so concurrent maintenance tests cannot retire another
// fixture's channel based on their unrelated cluster snapshot.
#[cfg(test)]
static ISOLATED_TEST_OBJECT_RPC_CHANNELS: LazyLock<Mutex<HashMap<NodeId, ObjectRpcChannelClient>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static TCP_TUNNEL_MAINTENANCE_SOURCES: LazyLock<TcpTunnelMaintenanceSources> =
    LazyLock::new(TcpTunnelMaintenanceSources::default);
#[cfg(test)]
static TEST_TCP_TUNNEL_REGISTRY_LIFECYCLE: LazyLock<RwLock<()>> = LazyLock::new(|| RwLock::new(()));

#[cfg(test)]
fn lock_test_tcp_tunnel_registry_lifecycle() -> RwLockWriteGuard<'static, ()> {
    TEST_TCP_TUNNEL_REGISTRY_LIFECYCLE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Identifies the source generation that invalidated the tunnel topology.
///
/// The maintenance owner consumes these independently so a wake cannot erase
/// another source's change or reset an unrelated retry class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TcpTunnelMaintenanceReason {
    IdentityChanged,
    MembershipChanged,
    RoleChanged,
    NetworkEnvironmentChanged,
    SessionDisconnected,
    OperatorRefresh,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TcpTunnelMaintenanceSourceGenerations {
    identity: u64,
    membership: u64,
    role: u64,
    network_environment: u64,
    session_disconnect: u64,
    operator: u64,
}

#[derive(Default)]
struct TcpTunnelMaintenanceSources {
    identity: AtomicU64,
    membership: AtomicU64,
    role: AtomicU64,
    network_environment: AtomicU64,
    session_disconnect: AtomicU64,
    operator: AtomicU64,
    notify: Notify,
}

impl TcpTunnelMaintenanceSources {
    fn request(&self, reason: TcpTunnelMaintenanceReason) {
        let generation = match reason {
            TcpTunnelMaintenanceReason::IdentityChanged => &self.identity,
            TcpTunnelMaintenanceReason::MembershipChanged => &self.membership,
            TcpTunnelMaintenanceReason::RoleChanged => &self.role,
            TcpTunnelMaintenanceReason::NetworkEnvironmentChanged => &self.network_environment,
            TcpTunnelMaintenanceReason::SessionDisconnected => &self.session_disconnect,
            TcpTunnelMaintenanceReason::OperatorRefresh => &self.operator,
        };
        generation.fetch_add(1, Ordering::Release);
        // The generation is authoritative; Notify is only the edge that keeps
        // a quiet owner asleep. A retained permit closes the check/sleep race.
        self.notify.notify_one();
    }

    fn generations(&self) -> TcpTunnelMaintenanceSourceGenerations {
        TcpTunnelMaintenanceSourceGenerations {
            identity: self.identity.load(Ordering::Acquire),
            membership: self.membership.load(Ordering::Acquire),
            role: self.role.load(Ordering::Acquire),
            network_environment: self.network_environment.load(Ordering::Acquire),
            session_disconnect: self.session_disconnect.load(Ordering::Acquire),
            operator: self.operator.load(Ordering::Acquire),
        }
    }
}

pub(crate) fn request_tcp_tunnel_maintenance_refresh(reason: TcpTunnelMaintenanceReason) {
    TCP_TUNNEL_MAINTENANCE_SOURCES.request(reason);
}

fn tcp_tunnel_close_signal() -> watch::Sender<Option<String>> {
    let (close, _) = watch::channel(None);
    close
}

pub(crate) fn network_perf_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("SINOMO_NETWORK_PERF_TRACE").as_deref() == Ok("1"))
}

pub(crate) fn network_perf_trace_elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

#[derive(Default)]
struct TcpTunnelRegistry {
    peers: Mutex<BTreeMap<NodeId, Arc<TcpTunnelHandle>>>,
    connect_locks: Mutex<BTreeMap<NodeId, Arc<tokio::sync::Mutex<()>>>>,
}

struct TcpTunnelConnectLockLease<'a> {
    registry: &'a TcpTunnelRegistry,
    node_id: NodeId,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl Drop for TcpTunnelConnectLockLease<'_> {
    fn drop(&mut self) {
        self.registry
            .remove_connect_lock_if_unused(&self.node_id, &self.lock);
    }
}

impl TcpTunnelRegistry {
    fn get(&self, node_id: &NodeId) -> Option<Arc<TcpTunnelHandle>> {
        let mut peers = self.peers.lock().ok()?;
        let handle = peers.get(node_id).cloned()?;
        if handle.closing.load(Ordering::Acquire) {
            let removed = if peers
                .get(node_id)
                .is_some_and(|current| Arc::ptr_eq(current, &handle))
            {
                peers.remove(node_id);
                true
            } else {
                false
            };
            drop(peers);
            if removed {
                request_tcp_tunnel_maintenance_refresh(
                    TcpTunnelMaintenanceReason::SessionDisconnected,
                );
            }
            return None;
        }
        let closed_channel_detail = handle
            .object_rpc_channel_client
            .as_ref()
            .and_then(|client| {
                client.is_closed().then(|| {
                    client
                        .close_detail()
                        .unwrap_or_else(|| "logical object RPC channel closed".to_string())
                })
            });
        if let Some(channel_detail) = closed_channel_detail {
            if peers
                .get(node_id)
                .is_some_and(|current| Arc::ptr_eq(current, &handle))
            {
                peers.remove(node_id);
            }
            drop(peers);
            let detail = format!("retiring tcp tunnel after {channel_detail}");
            tracing::info!(peer_node_id = %node_id, %detail, "sinomo tcp tunnel logical channel retired");
            handle.request_close(detail);
            request_tcp_tunnel_maintenance_refresh(TcpTunnelMaintenanceReason::SessionDisconnected);
            return None;
        }
        Some(handle)
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

    fn connect_lock_lease(&self, node_id: &NodeId) -> TcpTunnelConnectLockLease<'_> {
        TcpTunnelConnectLockLease {
            registry: self,
            node_id: node_id.clone(),
            lock: self.connect_lock(node_id),
        }
    }

    fn remove_connect_lock_if_unused(&self, node_id: &NodeId, lock: &Arc<tokio::sync::Mutex<()>>) {
        let Ok(mut connect_locks) = self.connect_locks.lock() else {
            return;
        };
        let removable = connect_locks.get(node_id).is_some_and(|current| {
            Arc::ptr_eq(current, lock)
                // One reference is retained by the map and one by this lease.
                // Any holder or waiter owns another lease and keeps the
                // singleflight lock registered until it has finished.
                && Arc::strong_count(current) == 2
        });
        if removable {
            connect_locks.remove(node_id);
        }
    }

    fn insert_or_existing(&self, handle: Arc<TcpTunnelHandle>) -> Result<(), Arc<TcpTunnelHandle>> {
        let Ok(mut peers) = self.peers.lock() else {
            return Ok(());
        };
        if let Some(existing) = peers.get(&handle.peer_node_id).cloned() {
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
            let claimed_close = existing
                .closing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok();
            if !claimed_close && !existing.clean_shutdown.load(Ordering::Acquire) {
                return Err(existing);
            }
            // A canonical connection must win even while the reverse carrier
            // has logical activity. The peer may
            // already have replaced its half of that reverse connection;
            // rejecting the canonical socket here would then close both
            // retained halves and make maintenance repeat the same
            // unavailable/reconnect cycle. Work on the retired carrier can
            // fail through the normal route semantics, while installing the
            // canonical carrier makes later calls stable.
            peers.insert(handle.peer_node_id.clone(), handle);
            drop(peers);
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
        #[cfg(test)]
        let _lifecycle = lock_test_tcp_tunnel_registry_lifecycle();
        let Ok(mut peers) = self.peers.lock() else {
            return;
        };
        if peers
            .get(&handle.peer_node_id)
            .is_some_and(|current| Arc::ptr_eq(current, handle))
        {
            peers.remove(&handle.peer_node_id);
            drop(peers);
            request_tcp_tunnel_maintenance_refresh(TcpTunnelMaintenanceReason::SessionDisconnected);
        }
    }

    #[cfg(test)]
    fn remove(&self, node_id: &NodeId) -> Option<Arc<TcpTunnelHandle>> {
        self.remove_inner(node_id, true)
    }

    fn remove_reconciled(&self, node_id: &NodeId) -> Option<Arc<TcpTunnelHandle>> {
        self.remove_inner(node_id, false)
    }

    fn remove_inner(
        &self,
        node_id: &NodeId,
        notify_disconnect: bool,
    ) -> Option<Arc<TcpTunnelHandle>> {
        #[cfg(test)]
        let _lifecycle = lock_test_tcp_tunnel_registry_lifecycle();
        let removed = self
            .peers
            .lock()
            .ok()
            .and_then(|mut peers| peers.remove(node_id));
        if notify_disconnect && removed.is_some() {
            request_tcp_tunnel_maintenance_refresh(TcpTunnelMaintenanceReason::SessionDisconnected);
        }
        removed
    }

    fn close_all(&self, detail: &str) -> usize {
        #[cfg(test)]
        let _lifecycle = lock_test_tcp_tunnel_registry_lifecycle();
        let handles = self
            .peers
            .lock()
            .map(|mut peers| {
                std::mem::take(&mut *peers)
                    .into_values()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let closed = handles.len();
        for handle in handles {
            handle.request_close(detail.to_string());
        }
        closed
    }

    fn peer_ids(&self) -> Vec<NodeId> {
        self.peers
            .lock()
            .map(|peers| peers.keys().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
pub(crate) fn insert_test_tcp_tunnel_object_rpc_channel(node_id: NodeId) -> ObjectRpcChannelClient {
    let endpoint = ObjectRpcChannelEndpoint::new(
        ObjectRpcChannelRole::Dialer,
        ObjectRpcChannelConfig::default(),
    )
    .expect("test tcp tunnel object RPC channel");
    let ObjectRpcChannelEndpoint {
        client,
        server,
        ingress,
        egress: _egress,
    } = endpoint;
    let handle = Arc::new(TcpTunnelHandle {
        peer_node_id: node_id,
        closing: AtomicBool::new(false),
        clean_shutdown: AtomicBool::new(false),
        close_signal: tcp_tunnel_close_signal(),
        canonical_direction: true,
        object_rpc_channel_client: Some(client.clone()),
        object_rpc_channel_ingress: Some(ingress),
        object_rpc_channel_server: Mutex::new(Some(server)),
    });
    TCP_TUNNELS.insert(handle);
    client
}

#[cfg(test)]
pub(crate) fn remove_test_tcp_tunnel(node_id: &NodeId) {
    if let Some(handle) = TCP_TUNNELS.remove(node_id) {
        handle.request_close("test cleanup".to_string());
    }
}

#[cfg(test)]
pub(crate) fn insert_isolated_test_object_rpc_channel_client(
    node_id: NodeId,
    client: ObjectRpcChannelClient,
) {
    ISOLATED_TEST_OBJECT_RPC_CHANNELS
        .lock()
        .expect("isolated test object RPC channel registry")
        .insert(node_id, client);
}

#[cfg(test)]
pub(crate) fn remove_isolated_test_object_rpc_channel_client(node_id: &NodeId) {
    ISOLATED_TEST_OBJECT_RPC_CHANNELS
        .lock()
        .expect("isolated test object RPC channel registry")
        .remove(node_id);
}

/// Returns the cloneable logical channel carried by the peer's v1 tunnel.
pub(crate) fn tcp_tunnel_object_rpc_channel_client(
    peer_node_id: &NodeId,
) -> Option<ObjectRpcChannelClient> {
    #[cfg(test)]
    if let Some(client) = ISOLATED_TEST_OBJECT_RPC_CHANNELS
        .lock()
        .ok()
        .and_then(|channels| channels.get(peer_node_id).cloned())
        .filter(|client| !client.is_closed())
    {
        return Some(client);
    }
    TCP_TUNNELS.get(peer_node_id).and_then(|handle| {
        (!handle.closing.load(Ordering::Acquire))
            .then(|| handle.object_rpc_channel_client.clone())
            .flatten()
            .filter(|client| !client.is_closed())
    })
}

/// Returns the peer's healthy logical object-RPC channel, rebuilding the
/// authenticated physical tunnel on a cold lookup miss. The registry's
/// singleflight lock bounds concurrent reconnects; steady-state calls still
/// complete through the memory-only lookup above.
pub(crate) async fn ensure_tcp_tunnel_object_rpc_channel_client(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
    health: NetworkHealthPolicy,
) -> Result<Option<ObjectRpcChannelClient>, NetworkError> {
    if let Some(client) = tcp_tunnel_object_rpc_channel_client(&endpoint.node_id) {
        return Ok(Some(client));
    }
    let handle = connect_tcp_tunnel_to_endpoint_with_timeout(
        endpoint,
        runtime,
        paths,
        health.connect_timeout.min(TCP_TUNNEL_CONNECT_TIMEOUT),
    )
    .await?;
    Ok(handle
        .object_rpc_channel_client
        .clone()
        .filter(|client| !client.is_closed()))
}

/// Retires every physical tunnel authenticated under the current cluster
/// identity. Callers invoke this only from a cluster identity transition or
/// after observing an unsettled/orphaned cluster state; ordinary per-RPC
/// lookups remain memory-only.
pub(crate) fn close_all_tcp_tunnels_for_cluster_transition(detail: &str) -> usize {
    TCP_TUNNELS.close_all(detail)
}

struct TcpTunnelHandle {
    peer_node_id: NodeId,
    closing: AtomicBool,
    clean_shutdown: AtomicBool,
    close_signal: watch::Sender<Option<String>>,
    canonical_direction: bool,
    object_rpc_channel_client: Option<ObjectRpcChannelClient>,
    object_rpc_channel_ingress: Option<ObjectRpcChannelIngress>,
    object_rpc_channel_server: Mutex<Option<ObjectRpcChannelServer>>,
}

impl TcpTunnelHandle {
    fn signal_physical_close(&self, detail: String) {
        self.closing.store(true, Ordering::Release);
        if let Some(ingress) = self.object_rpc_channel_ingress.as_ref() {
            ingress.close(detail.clone());
        }
        self.close_signal.send_if_modified(|current| {
            if current.is_some() {
                false
            } else {
                *current = Some(detail.clone());
                true
            }
        });
    }

    fn request_close(self: &Arc<Self>, detail: String) {
        self.clean_shutdown.store(true, Ordering::Relaxed);
        // Registry reads reject the closing handle immediately, while the
        // writer observes this signal independently of the bounded control
        // lane. Do not lock the registry here: canonical replacement calls
        // this method while holding its registry lock.
        self.signal_physical_close(detail);
    }
}

pub(crate) fn start_tcp_tunnel_maintenance_task(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    label: String,
    startup_cluster_id: ClusterId,
    startup_node_id: NodeId,
) -> Result<tokio::task::JoinHandle<()>, DaemonError> {
    // Every live quorum commit and replicated import is materialized through
    // `observe_cluster_kv_snapshot_write` in this daemon process, which invokes
    // this post-commit observer. Offline writers are covered by the dirty
    // startup pass below. This keeps topology wakeups source-driven without a
    // second file poll or a second durable-watch lifecycle owner.
    let topology_subscription = subscribe_tcp_tunnel_topology_sources(&paths, &startup_node_id)?;
    Ok(runtime.clone().spawn(async move {
        // Retain the path-scoped callback for exactly the maintenance owner's
        // lifetime.
        let _topology_subscription = topology_subscription;
        let sources = &*TCP_TUNNEL_MAINTENANCE_SOURCES;
        let mut completed_generations = sources.generations();
        let mut startup_pending = true;
        let mut consecutive_retry_rounds = 0_u32;
        let mut retry_at = None;
        let mut local_node_role = ClusterNodeRole::default();
        loop {
            if *shutdown_rx.borrow() {
                return;
            }
            if !startup_pending {
                match wait_for_tcp_tunnel_maintenance_wake(
                    sources,
                    completed_generations,
                    retry_at,
                    &mut shutdown_rx,
                )
                .await
                {
                    TcpTunnelMaintenanceWake::SourceChanged
                    | TcpTunnelMaintenanceWake::RetryDeadline => {}
                    TcpTunnelMaintenanceWake::Shutdown => return,
                }
            }
            let claimed_generations = sources.generations();
            let Some(maintenance_result) = wait_for_tcp_tunnel_maintenance_or_shutdown(
                maintain_tcp_tunnels_once(
                    &paths,
                    &runtime,
                    &startup_cluster_id,
                    &startup_node_id,
                ),
                &mut shutdown_rx,
            )
            .await
            else {
                return;
            };
            // Consume only the generations claimed before this pass. A source
            // event racing the pass remains greater and forces a follow-up.
            completed_generations = claimed_generations;
            startup_pending = false;
            match maintenance_result {
                Ok(outcome) => {
                    if outcome.state == TcpTunnelMaintenanceState::IdentityChanged {
                        tracing::debug!(
                            "sinomo {label} tcp tunnel maintenance stopped after daemon identity changed"
                        );
                        return;
                    }
                    local_node_role = outcome.local_node_role;
                    if outcome.state == TcpTunnelMaintenanceState::RetryNeeded {
                        consecutive_retry_rounds = consecutive_retry_rounds.saturating_add(1);
                        retry_at = tcp_tunnel_maintenance_next_deadline(
                            Instant::now(),
                            &local_node_role,
                            outcome.state,
                            consecutive_retry_rounds,
                        );
                    } else {
                        consecutive_retry_rounds = 0;
                        retry_at = None;
                    }
                }
                Err(error) => {
                    consecutive_retry_rounds = consecutive_retry_rounds.saturating_add(1);
                    retry_at = tcp_tunnel_maintenance_retry_at(
                        Instant::now(),
                        &local_node_role,
                        consecutive_retry_rounds,
                    );
                    tracing::debug!("sinomo {label} tcp tunnel maintenance skipped: {error}");
                }
            }
        }
    }))
}

fn subscribe_tcp_tunnel_topology_sources(
    paths: &DaemonPaths,
    startup_node_id: &NodeId,
) -> Result<sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription, DaemonError> {
    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(paths)?;
    let projection = Arc::new(Mutex::new(TcpTunnelTopologyProjection::default()));
    let callback_projection = Arc::clone(&projection);
    let local_member_key = sinomo_core::node::cluster_member_key(startup_node_id);
    let callback_local_member_key = local_member_key.clone();
    let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_snapshots(
        &cluster_paths,
        move |snapshot| {
            let changes = callback_projection
                .lock()
                .map(|mut projection| projection.observe(snapshot, &callback_local_member_key))
                .unwrap_or(TcpTunnelTopologyChanges {
                    membership: true,
                    role: true,
                });
            if changes.membership {
                request_tcp_tunnel_maintenance_refresh(
                    TcpTunnelMaintenanceReason::MembershipChanged,
                );
            }
            if changes.role {
                request_tcp_tunnel_maintenance_refresh(TcpTunnelMaintenanceReason::RoleChanged);
            }
        },
    );
    // Close the register/read race by seeding only when this snapshot is newer
    // than one already delivered to the callback. The unconditional startup
    // pass reconciles the same durable source before the owner first sleeps.
    match sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
        &cluster_paths,
    ) {
        Ok(Some(snapshot)) => {
            if let Ok(mut projection) = projection.lock() {
                projection.seed_if_newer(&snapshot, &local_member_key);
            }
        }
        Ok(None) => {}
        Err(error) => tracing::warn!(
            "tcp tunnel topology observation started without an initial KV projection: {error}"
        ),
    }
    Ok(subscription)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TcpTunnelEndpointProjection {
    endpoint_id: sinomo_core::Id,
    node_id: NodeId,
    transport: ClusterTransportKind,
    address: String,
    source: ClusterEndpointSource,
    privacy: ClusterEndpointPrivacy,
    priority: u16,
}

impl From<&ClusterEndpointRecord> for TcpTunnelEndpointProjection {
    fn from(endpoint: &ClusterEndpointRecord) -> Self {
        Self {
            endpoint_id: endpoint.endpoint_id.clone(),
            node_id: endpoint.node_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            source: endpoint.source.clone(),
            privacy: endpoint.privacy.clone(),
            priority: endpoint.priority,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TcpTunnelMemberProjection {
    cluster_id: ClusterId,
    node_id: NodeId,
    raft_role: sinomo_core::ClusterMemberRole,
    state: sinomo_core::ClusterMemberState,
    trust_domain: sinomo_core::TrustDomain,
    node_public_key: String,
    certificate_id: Option<sinomo_core::Id>,
    capabilities: Vec<sinomo_core::CapabilityName>,
    allowed_capabilities: Vec<sinomo_core::CapabilityName>,
    endpoints: Vec<TcpTunnelEndpointProjection>,
    local_role: Option<ClusterNodeRole>,
}

impl TcpTunnelMemberProjection {
    fn new(member: &sinomo_core::ClusterMemberRecord, is_local: bool) -> Self {
        let mut capabilities = member.capabilities.clone();
        capabilities.sort();
        let mut allowed_capabilities = member.allowed_capabilities.clone();
        allowed_capabilities.sort();
        let mut endpoints = member
            .endpoints
            .iter()
            .map(TcpTunnelEndpointProjection::from)
            .collect::<Vec<_>>();
        endpoints.sort_by(|left, right| {
            (&left.node_id, &left.endpoint_id, &left.address).cmp(&(
                &right.node_id,
                &right.endpoint_id,
                &right.address,
            ))
        });
        Self {
            cluster_id: member.cluster_id.clone(),
            node_id: member.node_id.clone(),
            raft_role: member.role.clone(),
            state: member.state.clone(),
            trust_domain: member.trust_domain.clone(),
            node_public_key: member.node_public_key.clone(),
            certificate_id: member.certificate_id.clone(),
            capabilities,
            allowed_capabilities,
            endpoints,
            local_role: is_local.then(|| member.node_role.clone()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TcpTunnelCapabilityProjection {
    cluster_id: ClusterId,
    node_id: NodeId,
    capabilities: Vec<sinomo_core::CapabilityName>,
    allowed_capabilities: Vec<sinomo_core::CapabilityName>,
}

impl From<&sinomo_core::node::ClusterCapabilityPolicyRecord> for TcpTunnelCapabilityProjection {
    fn from(policy: &sinomo_core::node::ClusterCapabilityPolicyRecord) -> Self {
        let mut capabilities = policy.capabilities.clone();
        capabilities.sort();
        let mut allowed_capabilities = policy.allowed_capabilities.clone();
        allowed_capabilities.sort();
        Self {
            cluster_id: policy.cluster_id.clone(),
            node_id: policy.node_id.clone(),
            capabilities,
            allowed_capabilities,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TcpTunnelTopologyValue {
    Member(TcpTunnelMemberProjection),
    Endpoint(TcpTunnelEndpointProjection),
    Capability(TcpTunnelCapabilityProjection),
    Malformed(Vec<u8>),
}

impl TcpTunnelTopologyValue {
    fn local_role(&self) -> Option<&ClusterNodeRole> {
        match self {
            Self::Member(member) => member.local_role.as_ref(),
            Self::Endpoint(_) | Self::Capability(_) | Self::Malformed(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TcpTunnelTopologyChanges {
    membership: bool,
    role: bool,
}

#[derive(Debug, Default)]
struct TcpTunnelTopologyProjection {
    revision: u64,
    values: BTreeMap<String, TcpTunnelTopologyValue>,
}

impl TcpTunnelTopologyProjection {
    fn seed_if_newer(&mut self, snapshot: &sinomo_core::kv::KvSnapshot, local_member_key: &str) {
        if snapshot.revision.0 <= self.revision {
            return;
        }
        self.values = tcp_tunnel_topology_projection_from_snapshot(snapshot, local_member_key);
        self.revision = snapshot.revision.0;
    }

    fn observe(
        &mut self,
        snapshot: &sinomo_core::kv::KvSnapshot,
        local_member_key: &str,
    ) -> TcpTunnelTopologyChanges {
        if snapshot.revision.0 <= self.revision {
            return TcpTunnelTopologyChanges::default();
        }
        if self.revision < snapshot.compacted_revision.0 {
            let next = tcp_tunnel_topology_projection_from_snapshot(snapshot, local_member_key);
            let changes = TcpTunnelTopologyChanges {
                membership: self.values != next,
                role: tcp_tunnel_projection_local_role(&self.values, local_member_key)
                    != tcp_tunnel_projection_local_role(&next, local_member_key),
            };
            self.values = next;
            self.revision = snapshot.revision.0;
            return changes;
        }

        let first_new = snapshot
            .events
            .partition_point(|event| event.revision.0 <= self.revision);
        let mut before = BTreeMap::new();
        for event in &snapshot.events[first_new..] {
            if !tcp_tunnel_topology_key(&event.key) {
                continue;
            }
            before
                .entry(event.key.clone())
                .or_insert_with(|| self.values.get(&event.key).cloned());
            match &event.kind {
                sinomo_core::kv::KvEventKind::Put { value } => {
                    self.values.insert(
                        event.key.clone(),
                        tcp_tunnel_topology_value(&event.key, value, local_member_key),
                    );
                }
                sinomo_core::kv::KvEventKind::Delete => {
                    self.values.remove(&event.key);
                }
            }
        }
        self.revision = snapshot.revision.0;
        let mut changes = TcpTunnelTopologyChanges::default();
        for (key, previous) in before {
            let current = self.values.get(&key).cloned();
            if previous != current {
                changes.membership = true;
                if key == local_member_key
                    && previous
                        .as_ref()
                        .and_then(TcpTunnelTopologyValue::local_role)
                        != current
                            .as_ref()
                            .and_then(TcpTunnelTopologyValue::local_role)
                {
                    changes.role = true;
                }
            }
        }
        changes
    }
}

fn tcp_tunnel_projection_local_role<'a>(
    values: &'a BTreeMap<String, TcpTunnelTopologyValue>,
    local_member_key: &str,
) -> Option<&'a ClusterNodeRole> {
    values
        .get(local_member_key)
        .and_then(TcpTunnelTopologyValue::local_role)
}

fn tcp_tunnel_topology_projection_from_snapshot(
    snapshot: &sinomo_core::kv::KvSnapshot,
    local_member_key: &str,
) -> BTreeMap<String, TcpTunnelTopologyValue> {
    let mut values = BTreeMap::new();
    for entry in &snapshot.entries {
        if tcp_tunnel_topology_key(&entry.key) {
            values.insert(
                entry.key.clone(),
                tcp_tunnel_topology_value(&entry.key, &entry.value, local_member_key),
            );
        }
    }
    values
}

fn tcp_tunnel_topology_value(
    key: &str,
    value: &[u8],
    local_member_key: &str,
) -> TcpTunnelTopologyValue {
    if key.starts_with("/cluster/members/") {
        return sinomo_core::kv::decode_kv_value::<sinomo_core::ClusterMemberRecord>(value)
            .map(|member| {
                TcpTunnelTopologyValue::Member(TcpTunnelMemberProjection::new(
                    &member,
                    key == local_member_key,
                ))
            })
            .unwrap_or_else(|_| TcpTunnelTopologyValue::Malformed(value.to_vec()));
    }
    if key.starts_with("/cluster/endpoints/") {
        return sinomo_core::kv::decode_kv_value::<ClusterEndpointRecord>(value)
            .map(|endpoint| {
                TcpTunnelTopologyValue::Endpoint(TcpTunnelEndpointProjection::from(&endpoint))
            })
            .unwrap_or_else(|_| TcpTunnelTopologyValue::Malformed(value.to_vec()));
    }
    sinomo_core::kv::decode_kv_value::<sinomo_core::node::ClusterCapabilityPolicyRecord>(value)
        .map(|policy| {
            TcpTunnelTopologyValue::Capability(TcpTunnelCapabilityProjection::from(&policy))
        })
        .unwrap_or_else(|_| TcpTunnelTopologyValue::Malformed(value.to_vec()))
}

fn tcp_tunnel_topology_key(key: &str) -> bool {
    key.starts_with("/cluster/members/")
        || key.starts_with("/cluster/endpoints/")
        || key.starts_with("/cluster/policy/capabilities/")
}

async fn wait_for_tcp_tunnel_maintenance_or_shutdown<F>(
    maintenance: F,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Option<F::Output>
where
    F: Future,
{
    tokio::pin!(maintenance);
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return None;
                }
            }
            result = &mut maintenance => return Some(result),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TcpTunnelMaintenanceWake {
    SourceChanged,
    RetryDeadline,
    Shutdown,
}

async fn wait_for_tcp_tunnel_maintenance_wake(
    sources: &TcpTunnelMaintenanceSources,
    completed_generations: TcpTunnelMaintenanceSourceGenerations,
    retry_at: Option<Instant>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> TcpTunnelMaintenanceWake {
    loop {
        if *shutdown_rx.borrow() {
            return TcpTunnelMaintenanceWake::Shutdown;
        }
        // Register the waiter before sampling generations. Notify retains one
        // permit, but this ordering also makes the no-lost-wake proof explicit.
        let notified = sources.notify.notified();
        tokio::pin!(notified);
        if sources.generations() != completed_generations {
            return TcpTunnelMaintenanceWake::SourceChanged;
        }
        match retry_at {
            Some(retry_at) => tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return TcpTunnelMaintenanceWake::Shutdown;
                    }
                }
                () = &mut notified => return TcpTunnelMaintenanceWake::SourceChanged,
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(retry_at)) => {
                    return TcpTunnelMaintenanceWake::RetryDeadline;
                }
            },
            None => tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return TcpTunnelMaintenanceWake::Shutdown;
                    }
                }
                () = &mut notified => return TcpTunnelMaintenanceWake::SourceChanged,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TcpTunnelMaintenanceState {
    /// The state file no longer describes the identity captured by this
    /// process. The replacement daemon owns all target-identity tunnels.
    IdentityChanged,
    /// At least one selected peer could not be connected during this round.
    RetryNeeded,
    /// Every selected peer has a tunnel.
    IdleConnected,
    /// There is neither a tunnel nor a selected peer to connect.
    Quiescent,
}

#[derive(Debug, Eq, PartialEq)]
struct TcpTunnelMaintenanceOutcome {
    local_node_role: ClusterNodeRole,
    state: TcpTunnelMaintenanceState,
}

fn tcp_tunnel_maintenance_retry_delay(
    role: &ClusterNodeRole,
    consecutive_retry_rounds: u32,
) -> Duration {
    // Only unresolved connection work owns a timer. Cluster connectivity
    // probing owns endpoint liveness; tunnel readers own physical disconnects.
    let shift = consecutive_retry_rounds.saturating_sub(1).min(10);
    let multiplier = 1_u32.checked_shl(shift).unwrap_or(1024);
    TCP_TUNNEL_RETRY_INITIAL_DELAY
        .saturating_mul(multiplier)
        .min(sinomo_core::network::cluster_network_probe_interval_for_role(role))
}

fn tcp_tunnel_maintenance_retry_at(
    now: Instant,
    role: &ClusterNodeRole,
    consecutive_retry_rounds: u32,
) -> Option<Instant> {
    now.checked_add(tcp_tunnel_maintenance_retry_delay(
        role,
        consecutive_retry_rounds,
    ))
}

fn tcp_tunnel_maintenance_next_deadline(
    now: Instant,
    role: &ClusterNodeRole,
    state: TcpTunnelMaintenanceState,
    consecutive_retry_rounds: u32,
) -> Option<Instant> {
    (state == TcpTunnelMaintenanceState::RetryNeeded)
        .then(|| tcp_tunnel_maintenance_retry_at(now, role, consecutive_retry_rounds))
        .flatten()
}

async fn sign_tcp_tunnel_handshake_proof_async(
    runtime: &tokio::runtime::Handle,
    paths: &DaemonPaths,
    authenticated: &AuthenticatedDaemonCommand,
    request: &ObjectRpcRequest,
    requested_protocol_version: u16,
    negotiated_protocol_version: u16,
    cluster_state: &sinomo_core::ClusterBootstrapState,
) -> Result<ClusterTcpTunnelHandshakeProof, NetworkError> {
    let auth = request.auth.as_deref().ok_or(NetworkError::AuthDenied)?;
    let signed_open: crate::SignedDaemonCommand = SinomoObjectCodec::default().decode(auth)?;
    let requester_public_key = daemon_auth_encode_public_key(&signed_open.public_key);
    if signed_open.node_id != authenticated.signer_node_id
        || requester_public_key != authenticated.signer_public_key
    {
        return Err(NetworkError::AuthDenied);
    }
    let data_dir = tcp_tunnel_signing_data_dir(Some(paths))?;
    let requester_node_id = signed_open.node_id;
    let dialer_nonce = signed_open.nonce;
    let responder_node_id = cluster_state.local_node_id.to_string();
    let cluster_context = Some(cluster_state.cluster_id.to_string());
    runtime
        .spawn_blocking(move || {
            let responder_public_key =
                daemon_auth_encode_public_key(&cached_daemon_signing_public_key(&data_dir)?);
            sign_cluster_tcp_tunnel_handshake_from_data_dir(
                &data_dir,
                ClusterTcpTunnelHandshakeTranscript {
                    protocol: CLUSTER_TCP_TUNNEL_HANDSHAKE_PROTOCOL.to_string(),
                    requester_node_id,
                    requester_public_key,
                    responder_node_id,
                    responder_public_key,
                    requested_protocol_version,
                    negotiated_protocol_version,
                    dialer_nonce,
                    cluster_context,
                },
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "sign tcp tunnel handshake".to_string(),
            detail: format!("signing task failed: {source}"),
        })?
}

pub(crate) async fn accept_tcp_tunnel_connection_async(
    mut stream: tokio::net::TcpStream,
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    authenticated: AuthenticatedDaemonCommand,
    request: ObjectRpcRequest,
) -> Result<(), DaemonError> {
    let open: ClusterTcpTunnelOpenRequest = request.decode_body().map_err(DaemonError::from)?;
    let Some(protocol_version) = negotiate_cluster_tcp_tunnel_protocol(open.protocol_version)
    else {
        let response = ObjectRpcResponse::encode_error(
            CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
            format!("unsupported tcp tunnel protocol {}", open.protocol_version),
        )
        .map_err(DaemonError::from)?;
        write_object_frame_async(&mut stream, &response, ObjectFrameIoOptions::default())
            .await
            .map_err(DaemonError::from)?;
        return Ok(());
    };
    let peer_node_id =
        NodeId::from_str(&authenticated.signer_node_id).map_err(|source| DaemonError::Network {
            address: authenticated.signer_node_id.clone(),
            detail: format!("invalid tcp tunnel peer node id: {source}"),
        })?;
    let connectivity_endpoint = tcp_tunnel_connectivity_context(&paths, &peer_node_id);
    let Some((_, cluster_state, _)) = connectivity_endpoint.as_ref() else {
        let response = ObjectRpcResponse::encode_error(
            CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
            "tcp tunnel v1 requires an initialized cluster identity",
        )
        .map_err(DaemonError::from)?;
        write_object_frame_async(&mut stream, &response, ObjectFrameIoOptions::default())
            .await
            .map_err(DaemonError::from)?;
        return Ok(());
    };
    let handshake_proof = sign_tcp_tunnel_handshake_proof_async(
        &service_runtime,
        &paths,
        &authenticated,
        &request,
        open.protocol_version,
        protocol_version,
        cluster_state,
    )
    .await
    .map_err(DaemonError::from)?;
    let open_response = ClusterTcpTunnelOpenResponse::accepted_version_with_handshake(
        protocol_version,
        handshake_proof,
    );
    let mut response =
        ObjectRpcResponse::encode_body(CLUSTER_TCP_TUNNEL_OPEN_ROUTE, &open_response)
            .map_err(DaemonError::from)?;
    crate::service_adapter::mark_daemon_rpc_response_responder(&paths, &mut response);
    write_object_frame_async(&mut stream, &response, ObjectFrameIoOptions::default())
        .await
        .map_err(DaemonError::from)?;
    let tunnel_record_endpoint = connectivity_endpoint
        .as_ref()
        .map(|(_, _, endpoint)| endpoint.clone());
    spawn_tcp_tunnel(
        peer_node_id.clone(),
        stream,
        paths.clone(),
        service_runtime.clone(),
        tunnel_record_endpoint,
        TcpTunnelDirection::Inbound,
        protocol_version,
    )?;
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

async fn evict_tcp_tunnel_peer(peer_node_id: &NodeId, detail: String) {
    // This removal is the maintenance pass's output, not a new source edge.
    // The claimed membership generation already owns its follow-through.
    if let Some(handle) = TCP_TUNNELS.remove_reconciled(peer_node_id) {
        handle.request_close(detail);
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

fn tcp_tunnel_cluster_state_is_settled(state: &sinomo_core::ClusterBootstrapState) -> bool {
    state.cluster_id.as_str() != sinomo_core::cluster::PENDING_INVITE_CLUSTER_ID
        && !sinomo_core::network::cluster_state_needs_join_registration(state)
}

fn tcp_tunnel_state_matches_startup_identity(
    state: &sinomo_core::ClusterBootstrapState,
    startup_cluster_id: &ClusterId,
    startup_node_id: &NodeId,
) -> bool {
    state.cluster_id == *startup_cluster_id && state.local_node_id == *startup_node_id
}

fn tcp_tunnel_registry_accepts_cluster_state(
    registry: &TcpTunnelRegistry,
    state: &sinomo_core::ClusterBootstrapState,
) -> bool {
    if tcp_tunnel_cluster_state_is_settled(state) {
        return true;
    }
    registry.close_all("local cluster state became pending; retiring authenticated tcp tunnels");
    false
}

fn tcp_tunnel_peer_is_authorized(
    state: &sinomo_core::ClusterBootstrapState,
    peer_node_id: &NodeId,
) -> bool {
    state.members.iter().any(|member| {
        member.cluster_id == state.cluster_id
            && member.node_id == *peer_node_id
            && member.state.is_authorized()
    })
}

async fn maintain_tcp_tunnels_once(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    startup_cluster_id: &ClusterId,
    startup_node_id: &NodeId,
) -> Result<TcpTunnelMaintenanceOutcome, DaemonError> {
    let cluster_paths = crate::cluster::cluster_paths_from_daemon_paths(paths)?;
    match read_cluster_state_without_kv(&cluster_paths) {
        Ok(state)
            if !tcp_tunnel_state_matches_startup_identity(
                &state,
                startup_cluster_id,
                startup_node_id,
            ) =>
        {
            close_all_tcp_tunnels_for_cluster_transition(
                "daemon identity changed; retiring prior-identity authenticated tcp tunnels",
            );
            return Ok(TcpTunnelMaintenanceOutcome {
                local_node_role: sinomo_core::network::cluster_local_node_role(&state),
                state: TcpTunnelMaintenanceState::IdentityChanged,
            });
        }
        Ok(state) if tcp_tunnel_registry_accepts_cluster_state(&TCP_TUNNELS, &state) => {}
        Ok(state) => {
            return Ok(TcpTunnelMaintenanceOutcome {
                local_node_role: sinomo_core::network::cluster_local_node_role(&state),
                state: TcpTunnelMaintenanceState::Quiescent,
            });
        }
        Err(NetworkError::ClusterOrphan) => {
            close_all_tcp_tunnels_for_cluster_transition(
                "local cluster state became uninitialized; retiring authenticated tcp tunnels",
            );
            return Ok(TcpTunnelMaintenanceOutcome {
                local_node_role: ClusterNodeRole::default(),
                state: TcpTunnelMaintenanceState::Quiescent,
            });
        }
        Err(error) => {
            close_all_tcp_tunnels_for_cluster_transition(
                "local cluster state became unreadable; retiring authenticated tcp tunnels",
            );
            return Err(DaemonError::from(error));
        }
    }
    // Membership authority is committed in cluster KV and can intentionally
    // diverge from the bootstrap state file. Use the merged resident snapshot
    // for every peer authorization and route choice in this maintenance pass.
    let state = read_cluster_state_cached_shared(&cluster_paths).map_err(DaemonError::from)?;
    if !tcp_tunnel_state_matches_startup_identity(&state, startup_cluster_id, startup_node_id) {
        close_all_tcp_tunnels_for_cluster_transition(
            "daemon identity changed; retiring prior-identity authenticated tcp tunnels",
        );
        return Ok(TcpTunnelMaintenanceOutcome {
            local_node_role: sinomo_core::network::cluster_local_node_role(&state),
            state: TcpTunnelMaintenanceState::IdentityChanged,
        });
    }
    let local_node_role = sinomo_core::network::cluster_local_node_role(&state);
    let selector = sinomo_core::network::ClusterEndpointSelector::new(&cluster_paths, &state);
    let mut endpoints =
        selector.select(ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates);
    endpoints.extend(selector.select(ClusterEndpointSelectionPurpose::NetworkProbe));
    let candidate_endpoints =
        tcp_tunnel_maintenance_candidate_endpoints(&state.local_node_id, endpoints);
    let candidate_peers = candidate_endpoints
        .iter()
        .map(|endpoint| endpoint.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut live_peers = BTreeSet::new();
    for peer_node_id in TCP_TUNNELS.peer_ids() {
        if peer_node_id == state.local_node_id {
            continue;
        }
        if !tcp_tunnel_peer_is_authorized(&state, &peer_node_id) {
            evict_tcp_tunnel_peer(
                &peer_node_id,
                "peer is no longer an authorized cluster member".to_string(),
            )
            .await;
            continue;
        }
        if TCP_TUNNELS.get(&peer_node_id).is_some() {
            // The physical reader owns immediate disconnect detection and each
            // logical channel owns its progress deadline. Cluster connectivity
            // probing owns endpoint liveness, so a healthy retained carrier
            // needs no second tunnel ping.
            live_peers.insert(peer_node_id);
        }
    }
    for endpoint in candidate_endpoints {
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
    let has_tunnels = !TCP_TUNNELS.peer_ids().is_empty();
    let has_unresolved_candidates = candidate_peers
        .iter()
        .any(|peer_node_id| !live_peers.contains(peer_node_id));
    let maintenance_state = if has_unresolved_candidates {
        TcpTunnelMaintenanceState::RetryNeeded
    } else if has_tunnels {
        TcpTunnelMaintenanceState::IdleConnected
    } else {
        TcpTunnelMaintenanceState::Quiescent
    };
    Ok(TcpTunnelMaintenanceOutcome {
        local_node_role,
        state: maintenance_state,
    })
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

async fn connect_tcp_tunnel_to_endpoint_with_timeout(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
    connect_timeout: Duration,
) -> Result<Arc<TcpTunnelHandle>, NetworkError> {
    if let Some(handle) = TCP_TUNNELS.get(&endpoint.node_id) {
        return Ok(handle);
    }
    let connect_lock = TCP_TUNNELS.connect_lock_lease(&endpoint.node_id);
    let _connect_guard = connect_lock.lock.lock().await;
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
    let (stream, protocol_version) = open_tcp_tunnel_physical_connection(
        endpoint,
        runtime,
        paths,
        connect_timeout,
        CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
    )
    .await?;
    let paths = paths
        .cloned()
        .or_else(daemon_paths_from_env)
        .ok_or_else(|| NetworkError::Io {
            operation: "open tcp tunnel".to_string(),
            detail: "local daemon paths unavailable".to_string(),
        })?;
    let handle = spawn_tcp_tunnel(
        endpoint.node_id.clone(),
        stream,
        paths.clone(),
        runtime.clone(),
        Some(endpoint.clone()),
        TcpTunnelDirection::Outbound,
        protocol_version,
    )?;
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

async fn open_tcp_tunnel_physical_connection(
    endpoint: &ClusterEndpointRecord,
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
    connect_timeout: Duration,
    requested_protocol_version: u16,
) -> Result<(tokio::net::TcpStream, u16), NetworkError> {
    let mut stream = AsyncDaemonControlStream::connect_with_options(
        &endpoint.address,
        DaemonRpcTimeouts::connect_and_idle(connect_timeout),
    )
    .await?
    .into_inner();
    let request =
        signed_tcp_tunnel_open_request(runtime, paths, requested_protocol_version).await?;
    let signed_open: crate::SignedDaemonCommand = SinomoObjectCodec::default()
        .decode(request.auth.as_deref().ok_or(NetworkError::AuthDenied)?)?;
    write_object_frame_async(&mut stream, &request, ObjectFrameIoOptions::default()).await?;
    let response: ObjectRpcResponse =
        read_object_frame_async(&mut stream, ObjectFrameIoOptions::default()).await?;
    response.error_detail()?;
    let open: ClusterTcpTunnelOpenResponse = response.decode_body()?;
    if !open.accepted
        || open.protocol_version != requested_protocol_version
        || negotiate_cluster_tcp_tunnel_protocol(open.protocol_version)
            != Some(open.protocol_version)
    {
        return Err(NetworkError::Io {
            operation: "open tcp tunnel".to_string(),
            detail: format!(
                "peer returned invalid tcp tunnel protocol {} for requested maximum {}",
                open.protocol_version, requested_protocol_version
            ),
        });
    }
    let proof = open
        .handshake_proof
        .as_ref()
        .ok_or_else(|| NetworkError::Io {
            operation: "authenticate tcp tunnel".to_string(),
            detail: TCP_TUNNEL_HANDSHAKE_UNAVAILABLE_DETAIL.to_string(),
        })?;
    let expected = expected_tcp_tunnel_handshake_transcript(
        endpoint,
        paths,
        &signed_open,
        requested_protocol_version,
        open.protocol_version,
    )?;
    verify_cluster_tcp_tunnel_handshake(proof, &expected)?;
    Ok((stream, open.protocol_version))
}

fn expected_tcp_tunnel_handshake_transcript(
    endpoint: &ClusterEndpointRecord,
    paths: Option<&DaemonPaths>,
    signed_open: &crate::SignedDaemonCommand,
    requested_protocol_version: u16,
    negotiated_protocol_version: u16,
) -> Result<ClusterTcpTunnelHandshakeTranscript, NetworkError> {
    let data_dir = tcp_tunnel_signing_data_dir(paths)?;
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = read_cluster_state_cached_shared(&cluster_paths)?;
    let requester_public_key = daemon_auth_encode_public_key(&signed_open.public_key);
    let requester_is_current_local_member = state.members.iter().any(|member| {
        member.node_id == state.local_node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && member.node_public_key == requester_public_key
    });
    if signed_open.node_id != state.local_node_id.to_string() || !requester_is_current_local_member
    {
        return Err(NetworkError::AuthDenied);
    }
    let responder = state
        .members
        .iter()
        .find(|member| {
            member.node_id == endpoint.node_id
                && member.cluster_id == state.cluster_id
                && member.state.is_authorized()
                && !member.node_public_key.trim().is_empty()
        })
        .ok_or(NetworkError::AuthDenied)?;
    Ok(ClusterTcpTunnelHandshakeTranscript {
        protocol: CLUSTER_TCP_TUNNEL_HANDSHAKE_PROTOCOL.to_string(),
        requester_node_id: signed_open.node_id.clone(),
        requester_public_key,
        responder_node_id: endpoint.node_id.to_string(),
        responder_public_key: responder.node_public_key.clone(),
        requested_protocol_version,
        negotiated_protocol_version,
        dialer_nonce: signed_open.nonce.clone(),
        cluster_context: Some(state.cluster_id.to_string()),
    })
}

async fn signed_tcp_tunnel_open_request(
    runtime: &tokio::runtime::Handle,
    paths: Option<&DaemonPaths>,
    protocol_version: u16,
) -> Result<ObjectRpcRequest, NetworkError> {
    let mut request = ObjectRpcRequest::encode_body(
        CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
        CLUSTER_TCP_TUNNEL_RPC_CATEGORY,
        &ClusterTcpTunnelOpenRequest { protocol_version },
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
    _protocol_version: u16,
) -> Result<Arc<TcpTunnelHandle>, NetworkError> {
    let canonical_direction =
        tcp_tunnel_direction_is_canonical_for_paths(&paths, &peer_node_id, direction);
    let channel_role = match direction {
        TcpTunnelDirection::Outbound => ObjectRpcChannelRole::Dialer,
        TcpTunnelDirection::Inbound => ObjectRpcChannelRole::Acceptor,
    };
    let ObjectRpcChannelEndpoint {
        client: object_rpc_channel_client,
        server: object_rpc_channel_server,
        ingress: object_rpc_channel_ingress,
        egress,
    } = ObjectRpcChannelEndpoint::new(channel_role, ObjectRpcChannelConfig::default())?;
    let (read_half, write_half) = stream.into_split();
    let handle = Arc::new(TcpTunnelHandle {
        peer_node_id,
        closing: AtomicBool::new(false),
        clean_shutdown: AtomicBool::new(false),
        close_signal: tcp_tunnel_close_signal(),
        canonical_direction,
        object_rpc_channel_client: Some(object_rpc_channel_client),
        object_rpc_channel_ingress: Some(object_rpc_channel_ingress),
        object_rpc_channel_server: Mutex::new(Some(object_rpc_channel_server)),
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
        return Ok(existing);
    }
    tokio::spawn(tcp_tunnel_writer_loop(handle.clone(), write_half, egress));
    tokio::spawn(tcp_tunnel_reader_loop(
        handle.clone(),
        read_half,
        paths,
        service_runtime,
        connectivity_endpoint,
    ));
    Ok(handle)
}

#[derive(Debug)]
enum TcpTunnelOutboundFrame {
    /// Tunnel control or object-RPC channel control, encoded as SOBJ.
    Tunnel(ClusterTcpTunnelFrame),
    /// Channel body data, encoded as fixed SCDF plus its raw payload.
    FixedChannelData(ObjectRpcChannelDataFrame),
}

async fn tcp_tunnel_writer_loop<W>(
    handle: Arc<TcpTunnelHandle>,
    mut write_half: W,
    mut object_rpc_egress: ObjectRpcChannelEgress,
) where
    W: AsyncWrite + Unpin,
{
    let mut close_signal = handle.close_signal.subscribe();
    let close_requested = async move {
        loop {
            if let Some(detail) = close_signal.borrow_and_update().clone() {
                return detail;
            }
            if close_signal.changed().await.is_err() {
                return "tcp tunnel close signal stopped".to_string();
            }
        }
    };
    tokio::pin!(close_requested);
    loop {
        let (frame, object_rpc_frame) = tokio::select! {
            biased;
            detail = &mut close_requested => (
                Some(TcpTunnelOutboundFrame::Tunnel(ClusterTcpTunnelFrame::Close { detail })),
                false,
            ),
            frame = next_tcp_tunnel_object_rpc_frame(&mut object_rpc_egress) => (frame, true),
        };
        let Some(frame) = frame else {
            break;
        };
        let close = matches!(
            frame,
            TcpTunnelOutboundFrame::Tunnel(ClusterTcpTunnelFrame::Close { .. })
        );
        let mut object_rpc_timeline = object_rpc_frame
            .then(|| object_rpc_egress.pending_write_timeline())
            .flatten();
        if let Some(timeline) = object_rpc_timeline.as_mut() {
            timeline.flags |= network_timeline_flag::TUNNEL_CARRIER;
        }
        let (write_result, write_timing) = match frame {
            TcpTunnelOutboundFrame::Tunnel(frame) if object_rpc_timeline.is_some() => {
                write_object_frame_async_timed(
                    &mut write_half,
                    &frame,
                    ObjectFrameIoOptions::default(),
                )
                .await
            }
            TcpTunnelOutboundFrame::Tunnel(frame) => (
                write_object_frame_async(&mut write_half, &frame, ObjectFrameIoOptions::default())
                    .await,
                None,
            ),
            TcpTunnelOutboundFrame::FixedChannelData(frame) if object_rpc_timeline.is_some() => {
                write_object_rpc_channel_transport_frame_async_timed(
                    &mut write_half,
                    &ObjectRpcChannelTransportFrame::Data(frame),
                    ObjectFrameIoOptions::default(),
                )
                .await
            }
            TcpTunnelOutboundFrame::FixedChannelData(frame) => (
                write_object_rpc_channel_transport_frame_async(
                    &mut write_half,
                    &ObjectRpcChannelTransportFrame::Data(frame),
                    ObjectFrameIoOptions::default(),
                )
                .await,
                None,
            ),
        };
        if let Some(timeline) = object_rpc_timeline {
            timeline.record_frame_write_timing(write_timing, write_result.is_ok());
        }
        if object_rpc_frame {
            object_rpc_egress.complete_frame_write(write_result.is_ok());
        }
        if write_result.is_err() {
            break;
        }
        if close {
            break;
        }
    }
    handle.closing.store(true, Ordering::Release);
    TCP_TUNNELS.remove_if_same(&handle);
    if let Some(ingress) = handle.object_rpc_channel_ingress.as_ref() {
        ingress.close(format!(
            "tcp tunnel writer to {} stopped",
            handle.peer_node_id
        ));
    }
}

async fn next_tcp_tunnel_object_rpc_frame(
    egress: &mut ObjectRpcChannelEgress,
) -> Option<TcpTunnelOutboundFrame> {
    egress
        .next_transport_frame()
        .await
        .map(tcp_tunnel_channel_transport_frame)
}

fn tcp_tunnel_channel_transport_frame(
    frame: ObjectRpcChannelTransportFrame,
) -> TcpTunnelOutboundFrame {
    match frame {
        ObjectRpcChannelTransportFrame::Control(frame) => {
            TcpTunnelOutboundFrame::Tunnel(ClusterTcpTunnelFrame::ObjectRpcChannel { frame })
        }
        ObjectRpcChannelTransportFrame::Data(frame) => {
            TcpTunnelOutboundFrame::FixedChannelData(frame)
        }
    }
}

#[cfg(test)]
async fn read_tcp_tunnel_frame<R>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
) -> Result<ClusterTcpTunnelFrame, NetworkError>
where
    R: AsyncRead + Unpin,
{
    sinomo_core::network::read_object_frame_async_with_max_bytes_after_first_byte(
        reader,
        options,
        MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES,
    )
    .await
}

enum TcpTunnelInboundFrame {
    Tunnel(ClusterTcpTunnelFrame),
    FixedChannelData(ObjectRpcChannelDataFrame),
}

async fn read_tcp_tunnel_frame_timed<R>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    readiness_probe: &ObjectFrameReadinessProbe,
    channel_ingress: Option<&ObjectRpcChannelIngress>,
    receive_pool: &Arc<ObjectRpcChannelReceiveBufferPool>,
) -> Result<(TcpTunnelInboundFrame, Option<ObjectFrameReadTiming>), NetworkError>
where
    R: AsyncRead + Unpin,
{
    // Tunnel v1 has two physical frame magics. The first read remains
    // indefinitely idle-safe; after any progress, the carrier no-progress
    // timeout applies while the rest of the discriminant arrives.
    let magic = read_tcp_tunnel_frame_magic(reader, options.idle_timeout).await?;
    let prefix = Cursor::new(magic);
    let mut replay = prefix.chain(reader);
    if magic == OBJECT_RPC_CHANNEL_DATA_MAGIC {
        let ingress = channel_ingress.ok_or_else(|| NetworkError::Serialization {
            detail: "peer sent fixed channel Data without an active object RPC channel".to_string(),
        })?;
        let (frame, timing) = read_object_rpc_channel_transport_frame_async_timed(
            &mut replay,
            options,
            receive_pool,
            |header| ingress.validate_data_header(header),
        )
        .await?;
        return match frame {
            ObjectRpcChannelTransportFrame::Data(frame) => {
                Ok((TcpTunnelInboundFrame::FixedChannelData(frame), timing))
            }
            ObjectRpcChannelTransportFrame::Control(_) => Err(NetworkError::Serialization {
                detail: "SCDF tunnel reader decoded an unexpected control frame".to_string(),
            }),
        };
    }

    let (frame, timing) = read_object_frame_async_with_max_bytes_after_first_byte_timed(
        &mut replay,
        options,
        MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES,
        readiness_probe,
    )
    .await?;
    Ok((TcpTunnelInboundFrame::Tunnel(frame), timing))
}

async fn read_tcp_tunnel_frame_magic<R>(
    reader: &mut R,
    idle_timeout: Option<Duration>,
) -> Result<[u8; 4], NetworkError>
where
    R: AsyncRead + Unpin,
{
    let mut magic = [0_u8; 4];
    let first = loop {
        match reader.read(&mut magic).await {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: "read tcp tunnel frame magic".to_string(),
                    detail: "peer closed before the next tunnel frame".to_string(),
                });
            }
            Ok(bytes) => break bytes,
            Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: "read tcp tunnel frame magic".to_string(),
                    detail: source.to_string(),
                });
            }
        }
    };
    let mut filled = first;
    while filled < magic.len() {
        let read = async { reader.read(&mut magic[filled..]).await };
        let result = match idle_timeout {
            Some(timeout) => {
                tokio::time::timeout(timeout, read)
                    .await
                    .map_err(|_| NetworkError::Io {
                        operation: "read tcp tunnel frame magic".to_string(),
                        detail: format!(
                            "timed out after {}ms without tunnel magic read progress",
                            timeout.as_millis()
                        ),
                    })?
            }
            None => read.await,
        };
        match result {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: "read tcp tunnel frame magic".to_string(),
                    detail: format!(
                        "peer closed during tunnel magic: read {filled} of {} bytes",
                        magic.len()
                    ),
                });
            }
            Ok(bytes) => filled = filled.saturating_add(bytes),
            Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: "read tcp tunnel frame magic".to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(magic)
}

async fn tcp_tunnel_reader_loop(
    handle: Arc<TcpTunnelHandle>,
    mut read_half: OwnedReadHalf,
    paths: DaemonPaths,
    service_runtime: tokio::runtime::Handle,
    connectivity_endpoint: Option<ClusterEndpointRecord>,
) {
    // Resolve the cloneable registry host once for the physical tunnel's
    // object-RPC channel server.
    let service_host = match crate::service_adapter::DaemonServiceRegistryHost::new_on_runtime(
        paths.clone(),
        service_runtime.clone(),
    )
    .await
    {
        Ok(service_host) => service_host,
        Err(error) => {
            let detail = format!("tcp tunnel service registry unavailable: {error}");
            tracing::warn!("{detail}");
            handle.signal_physical_close(detail.clone());
            TCP_TUNNELS.remove_if_same(&handle);
            if let Some(endpoint) = connectivity_endpoint.as_ref() {
                remember_tcp_tunnel_connectivity(
                    &paths,
                    endpoint,
                    sinomo_core::network::ClusterConnectivityState::Unreachable,
                    Some(&service_runtime),
                )
                .await;
            }
            return;
        }
    };
    let channel_server = handle
        .object_rpc_channel_server
        .lock()
        .ok()
        .and_then(|mut server| server.take());
    if let Some(server) = channel_server {
        let channel_paths = paths.clone();
        let channel_service_host = service_host.clone();
        let channel_runtime = service_runtime.clone();
        let channel_peer = handle.peer_node_id.clone();
        let channel_ingress = handle.object_rpc_channel_ingress.clone();
        let channel_handle = Arc::clone(&handle);
        tokio::spawn(async move {
            let result = crate::control::handle_daemon_object_rpc_channel_async(
                server,
                channel_paths,
                channel_service_host,
                channel_runtime,
                crate::control::DaemonObjectRpcChannelPeer::ClusterNode(channel_peer.clone()),
            )
            .await;
            match result {
                Ok(false) => {
                    let detail =
                        format!("tcp tunnel object RPC channel server for {channel_peer} retired");
                    if let Some(ingress) = channel_ingress.as_ref() {
                        ingress.close(detail.clone());
                    }
                    // The channel principal is authenticated once per
                    // physical tunnel. Retiring only the logical endpoint
                    // would leave this registry entry permanently unable to
                    // capture an updated membership role or public key.
                    channel_handle.request_close(detail);
                }
                Ok(true) => {
                    let detail =
                        format!("tcp tunnel object RPC channel server for {channel_peer} stopped");
                    if let Some(ingress) = channel_ingress.as_ref() {
                        ingress.close(detail.clone());
                    }
                    channel_handle.request_close(detail);
                }
                Err(error) => {
                    tracing::debug!(
                        "sinomo tcp tunnel object RPC channel for {channel_peer} stopped: {error}"
                    );
                    if let Some(ingress) = channel_ingress.as_ref() {
                        ingress.close(error.to_string());
                    }
                    channel_handle.request_close(error.to_string());
                }
            }
        });
    }
    let mut mark_unreachable_on_exit = false;
    let readiness_probe = ObjectFrameReadinessProbe::new();
    let channel_receive_pool = ObjectRpcChannelReceiveBufferPool::default_for_carrier();
    loop {
        // This reader waits between independent multiplexed requests, so an
        // idle connection is healthy. Each active request carries its own
        // 5-second connect/no-progress policy and operation deadline.
        let frame = read_tcp_tunnel_frame_timed(
            &mut read_half,
            ObjectFrameIoOptions::idle_timeout(TCP_TUNNEL_FRAME_NO_PROGRESS_TIMEOUT),
            &readiness_probe,
            handle.object_rpc_channel_ingress.as_ref(),
            &channel_receive_pool,
        )
        .await;
        let (frame, read_timing) = match frame {
            Ok(frame) => frame,
            Err(error) => {
                // Capture the physical fault before closing the logical
                // channel. Its dispatcher may concurrently request a
                // clean carrier retirement; that must not reclassify this
                // read timeout/EOF as an intentional shutdown.
                mark_unreachable_on_exit = !handle.clean_shutdown.load(Ordering::Acquire);
                handle.signal_physical_close(error.to_string());
                break;
            }
        };
        match frame {
            TcpTunnelInboundFrame::FixedChannelData(frame) => {
                if let Some(ingress) = handle.object_rpc_channel_ingress.as_ref() {
                    ingress.record_data_frame_read_timeline(
                        &frame.header,
                        read_timing,
                        network_timeline_flag::TUNNEL_CARRIER,
                    );
                }
                let receive = handle
                    .object_rpc_channel_ingress
                    .as_ref()
                    .ok_or_else(|| NetworkError::Serialization {
                        detail: "peer sent channel Data without an active object RPC channel"
                            .to_string(),
                    })
                    .and_then(|ingress| ingress.receive_data(frame));
                if let Err(error) = receive {
                    let detail = format!("invalid fixed tcp tunnel channel Data: {error}");
                    mark_unreachable_on_exit = !handle.clean_shutdown.load(Ordering::Acquire);
                    handle.signal_physical_close(detail.clone());
                    break;
                }
            }
            TcpTunnelInboundFrame::Tunnel(ClusterTcpTunnelFrame::ObjectRpcChannel { frame }) => {
                if let Some(ingress) = handle.object_rpc_channel_ingress.as_ref() {
                    ingress.record_frame_read_timeline(
                        &frame,
                        read_timing,
                        network_timeline_flag::TUNNEL_CARRIER,
                    );
                }
                let receive = handle
                    .object_rpc_channel_ingress
                    .as_ref()
                    .ok_or_else(|| NetworkError::Serialization {
                        detail: "peer sent channel control without an active object RPC channel"
                            .to_string(),
                    })
                    .and_then(|ingress| ingress.receive_control(frame));
                if let Err(error) = receive {
                    let detail = format!("invalid tcp tunnel object RPC channel frame: {error}");
                    mark_unreachable_on_exit = !handle.clean_shutdown.load(Ordering::Acquire);
                    handle.signal_physical_close(detail.clone());
                    break;
                }
            }
            TcpTunnelInboundFrame::Tunnel(ClusterTcpTunnelFrame::Close { detail }) => {
                handle.closing.store(true, Ordering::Release);
                handle.clean_shutdown.store(true, Ordering::Relaxed);
                tracing::debug!("peer closed tcp tunnel: {detail}");
                break;
            }
        }
    }
    handle.signal_physical_close("tcp tunnel reader stopped".to_string());
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{ClusterEndpointPrivacy, ClusterEndpointSource};

    static TIMELINE_CAPTURE_TEST: Mutex<()> = Mutex::new(());

    fn test_tunnel_handle(
        node_name: &str,
        _tx: mpsc::Sender<ClusterTcpTunnelFrame>,
    ) -> Arc<TcpTunnelHandle> {
        test_tunnel_handle_with_direction(node_name, _tx, true)
    }

    fn test_tunnel_handle_with_direction(
        node_name: &str,
        _tx: mpsc::Sender<ClusterTcpTunnelFrame>,
        canonical_direction: bool,
    ) -> Arc<TcpTunnelHandle> {
        test_tunnel_handle_with_direction_and_channel(node_name, _tx, canonical_direction, None)
    }

    fn test_tunnel_handle_with_direction_and_channel(
        node_name: &str,
        _tx: mpsc::Sender<ClusterTcpTunnelFrame>,
        canonical_direction: bool,
        object_rpc_channel_client: Option<ObjectRpcChannelClient>,
    ) -> Arc<TcpTunnelHandle> {
        Arc::new(TcpTunnelHandle {
            peer_node_id: NodeId::from_legacy_named(node_name).expect("test node id"),
            closing: AtomicBool::new(false),
            clean_shutdown: AtomicBool::new(false),
            close_signal: tcp_tunnel_close_signal(),
            canonical_direction,
            object_rpc_channel_client,
            object_rpc_channel_ingress: None,
            object_rpc_channel_server: Mutex::new(None),
        })
    }

    #[test]
    fn tunnel_v1_keeps_channel_data_out_of_sobj() {
        let payload = vec![0x5a; 64 * 1024];
        let data = ObjectRpcChannelDataFrame::new(
            sinomo_core::network::ObjectRpcChannelCallId(1),
            sinomo_core::network::ObjectRpcChannelBodyDirection::Request,
            128,
            payload.clone().into(),
        )
        .expect("fixed channel Data");

        match tcp_tunnel_channel_transport_frame(ObjectRpcChannelTransportFrame::Data(data.clone()))
        {
            TcpTunnelOutboundFrame::FixedChannelData(frame) => {
                assert_eq!(frame.header, data.header);
                assert_eq!(frame.bytes.as_ref(), payload.as_slice());
            }
            TcpTunnelOutboundFrame::Tunnel(_) => panic!("v1 Data must stay out of SOBJ"),
        }
    }

    #[tokio::test]
    async fn tunnel_v1_mixed_reader_validates_and_delivers_channel_data() {
        let _capture_guard = TIMELINE_CAPTURE_TEST
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if sinomo_core::network::network_timeline_capture_is_active() {
            let _ = sinomo_core::network::stop_network_timeline_capture();
        }
        sinomo_core::network::start_network_timeline_capture(64).expect("start capture");
        let ObjectRpcChannelEndpoint {
            client: _client,
            mut server,
            ingress,
            egress: _egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Acceptor,
            ObjectRpcChannelConfig::default(),
        )
        .expect("object RPC channel");
        let call_id = sinomo_core::network::ObjectRpcChannelCallId(1);
        let payload = vec![0x73; 64 * 1024];
        let request = ObjectRpcRequest::encode_body("/test/tunnel-v1/data", "test", &"head")
            .expect("request head")
            .with_traceparent("00-51515151515151515151515151515151-5252525252525252-01")
            .with_observe_sampled(true);
        ingress
            .receive_control(ObjectRpcChannelWireFrame::Request {
                call_id,
                request,
                body: Some(sinomo_core::network::ObjectRpcChannelBodyDescriptor {
                    kind: sinomo_core::network::ObjectRpcChannelBodyKind::Attachment,
                    total_bytes: Some(payload.len() as u64),
                }),
            })
            .expect("request control");

        let frame = ObjectRpcChannelDataFrame::new(
            call_id,
            sinomo_core::network::ObjectRpcChannelBodyDirection::Request,
            0,
            payload.clone().into(),
        )
        .expect("fixed channel Data");
        let (mut reader, mut writer) = tokio::io::duplex(128 * 1024);
        write_object_rpc_channel_transport_frame_async(
            &mut writer,
            &ObjectRpcChannelTransportFrame::Data(frame),
            ObjectFrameIoOptions::none(),
        )
        .await
        .expect("write fixed Data");

        let readiness_probe = ObjectFrameReadinessProbe::new();
        let receive_pool = ObjectRpcChannelReceiveBufferPool::default_for_carrier();
        let (frame, timing) = read_tcp_tunnel_frame_timed(
            &mut reader,
            ObjectFrameIoOptions::none(),
            &readiness_probe,
            Some(&ingress),
            &receive_pool,
        )
        .await
        .expect("read mixed tunnel frame");
        let timing = timing.expect("fixed Data read timing");
        assert_ne!(timing.capture_generation, 0);
        assert!(timing.body_read_completed_monotonic_ns >= timing.first_read_monotonic_ns);
        assert!(timing.decoded_monotonic_ns >= timing.decode_started_monotonic_ns);
        let TcpTunnelInboundFrame::FixedChannelData(frame) = frame else {
            panic!("expected fixed tunnel Data");
        };
        ingress.record_data_frame_read_timeline(
            &frame.header,
            Some(timing),
            network_timeline_flag::TUNNEL_CARRIER,
        );
        ingress.receive_data(frame).expect("deliver fixed Data");
        ingress
            .receive_control(ObjectRpcChannelWireFrame::BodyEnd {
                call_id,
                direction: sinomo_core::network::ObjectRpcChannelBodyDirection::Request,
                total_bytes: payload.len() as u64,
            })
            .expect("body end");

        let inbound = server.accept().await.expect("inbound request");
        let received = inbound
            .request
            .body
            .collect_to_vec(128 * 1024)
            .await
            .expect("collect fixed Data");
        assert_eq!(received, payload);
        let snapshot = sinomo_core::network::stop_network_timeline_capture().expect("stop capture");
        for stage in [
            sinomo_core::network::network_timeline_stage::FRAME_FIRST_READ,
            sinomo_core::network::network_timeline_stage::FRAME_READ,
            sinomo_core::network::network_timeline_stage::FRAME_DECODE,
        ] {
            assert!(snapshot.events.iter().any(|event| {
                event.call_id == call_id.0
                    && event.stage == stage
                    && event.flags & network_timeline_flag::DATA_FRAME != 0
                    && event.flags & network_timeline_flag::TUNNEL_CARRIER != 0
            }));
        }
    }

    #[test]
    fn registry_retires_v1_tunnel_when_its_logical_rpc_channel_closes() {
        let peer = NodeId::from_legacy_named("tcp_tunnel_closed_logical_channel_peer")
            .expect("peer node id");
        let ObjectRpcChannelEndpoint {
            client,
            server: _server,
            ingress,
            egress: _egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("object RPC channel");
        let (tx, _rx) = mpsc::channel(TCP_TUNNEL_CHANNEL_CAPACITY);
        let handle = test_tunnel_handle_with_direction_and_channel(
            "tcp_tunnel_closed_logical_channel_peer",
            tx,
            true,
            Some(client.clone()),
        );
        TCP_TUNNELS.insert(handle.clone());

        ingress.close("injected logical channel failure");
        assert!(client.is_closed());
        assert!(TCP_TUNNELS.get(&peer).is_none());
        assert!(!TCP_TUNNELS.contains(&peer));
        assert!(handle.closing.load(Ordering::Acquire));
        assert!(handle.clean_shutdown.load(Ordering::Acquire));
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

    #[test]
    fn pending_cluster_state_retires_authenticated_channel_handles() {
        let registry = TcpTunnelRegistry::default();
        let peer_name = "tcp_tunnel_pending_transition_peer";
        let peer = NodeId::from_legacy_named(peer_name).expect("peer node id");
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("object RPC channel");
        let (tx, _rx) = mpsc::channel(1);
        let handle = test_tunnel_handle_with_direction_and_channel(
            peer_name,
            tx,
            true,
            Some(endpoint.client),
        );
        registry.insert(handle.clone());
        let pending_cluster_id = sinomo_core::node::ClusterId::new(
            sinomo_core::cluster::PENDING_INVITE_CLUSTER_ID.to_string(),
        )
        .expect("pending cluster id");
        let local_node =
            NodeId::from_legacy_named("tcp_tunnel_pending_transition_local").expect("local node");
        let pending = sinomo_core::ClusterBootstrapState::initialized_locally(
            pending_cluster_id,
            "pending transition",
            local_node,
            "local",
            "pk-local",
            time::OffsetDateTime::now_utc(),
        );

        assert!(registry.get(&peer).is_some(), "precondition: live handle");
        assert!(!tcp_tunnel_registry_accepts_cluster_state(
            &registry, &pending
        ));

        assert!(
            registry.get(&peer).is_none(),
            "pending state must remove the old authenticated handle"
        );
        assert!(handle.closing.load(Ordering::Acquire));
        assert!(handle.clean_shutdown.load(Ordering::Acquire));
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

        let request = signed_tcp_tunnel_open_request(
            &tokio::runtime::Handle::current(),
            Some(&daemon_paths),
            CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
        )
        .await
        .expect("signed request");
        let envelope: crate::SignedDaemonCommand =
            sinomo_core::network::SinomoObjectCodec::default()
                .decode(request.auth.as_deref().expect("auth envelope"))
                .expect("decode signed envelope");

        assert_eq!(envelope.node_id, local.to_string());
    }

    #[tokio::test]
    async fn tcp_tunnel_dialer_exposes_v1_channel_only_after_valid_target_proof() {
        let client_root = sinomo_testdir::TestDir::new("sinomo-tcp-tunnel-v1-proof-client");
        let server_root = sinomo_testdir::TestDir::new("sinomo-tcp-tunnel-v1-proof-server");
        let client_data_dir = SinomoDataDir::new(client_root.path());
        let server_data_dir = SinomoDataDir::new(server_root.path());
        client_data_dir
            .ensure_root_layout()
            .expect("client data dir");
        server_data_dir
            .ensure_root_layout()
            .expect("server data dir");
        let client_paths = DaemonPaths::new(&client_data_dir);
        client_paths.ensure().expect("client daemon paths");
        let client_identity =
            sinomo_core::node::load_or_create_main_node_identity(&client_data_dir.root())
                .expect("client identity");
        let server_identity =
            sinomo_core::node::load_or_create_main_node_identity(&server_data_dir.root())
                .expect("server identity");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_tcp_tunnel_v1_proof")
                .expect("cluster id");
        let client_node =
            NodeId::from_legacy_named("node_tcp_tunnel_v1_proof_client").expect("client node");
        let server_node =
            NodeId::from_legacy_named("node_tcp_tunnel_v1_proof_server").expect("server node");
        let now = time::OffsetDateTime::now_utc();
        let mut client_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "tcp tunnel v1 proof",
            client_node,
            "client",
            client_identity.public_key,
            now,
        );
        client_state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                server_node.clone(),
                "server",
                server_identity.public_key.clone(),
                now,
            ));
        sinomo_core::cluster::write_cluster_state(
            &sinomo_core::network::ClusterPaths::new(&client_data_dir),
            &client_state,
        )
        .expect("client cluster state");
        let server_state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "tcp tunnel v1 proof",
            server_node.clone(),
            "server",
            server_identity.public_key.clone(),
            now,
        );
        sinomo_core::cluster::write_cluster_state(
            &sinomo_core::network::ClusterPaths::new(&server_data_dir),
            &server_state,
        )
        .expect("server cluster state");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("v1 proof listener");
        let address = listener.local_addr().expect("listener address");
        let (release_tx, release_rx) = oneshot::channel();
        let responder_node_id = server_node.to_string();
        let responder_public_key = server_identity.public_key;
        let cluster_context = cluster_id.to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("tunnel attempt");
            let request: ObjectRpcRequest =
                read_object_frame_async(&mut stream, ObjectFrameIoOptions::default())
                    .await
                    .expect("tunnel open request");
            let open: ClusterTcpTunnelOpenRequest =
                request.decode_body().expect("decode tunnel open request");
            assert_eq!(open.protocol_version, CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION);
            let signed_open: crate::SignedDaemonCommand = SinomoObjectCodec::default()
                .decode(request.auth.as_deref().expect("signed open"))
                .expect("decode signed open");
            let proof = sign_cluster_tcp_tunnel_handshake_from_data_dir(
                &server_data_dir,
                ClusterTcpTunnelHandshakeTranscript {
                    protocol: CLUSTER_TCP_TUNNEL_HANDSHAKE_PROTOCOL.to_string(),
                    requester_node_id: signed_open.node_id,
                    requester_public_key: daemon_auth_encode_public_key(&signed_open.public_key),
                    responder_node_id,
                    responder_public_key,
                    requested_protocol_version: open.protocol_version,
                    negotiated_protocol_version: CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
                    dialer_nonce: signed_open.nonce,
                    cluster_context: Some(cluster_context),
                },
            )
            .expect("sign target handshake proof");
            let response = ObjectRpcResponse::encode_body(
                CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
                &ClusterTcpTunnelOpenResponse::accepted_version_with_handshake(
                    CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
                    proof,
                ),
            )
            .expect("v1 proof acceptance");
            write_object_frame_async(&mut stream, &response, ObjectFrameIoOptions::default())
                .await
                .expect("write v1 proof acceptance");
            let _ = release_rx.await;
        });
        let endpoint = test_endpoint(
            "tcp_tunnel_v1_proof_server_endpoint",
            &server_node,
            &address.to_string(),
            ClusterTransportKind::LanTcp,
        );

        let handle = connect_tcp_tunnel_to_endpoint_with_timeout(
            &endpoint,
            &tokio::runtime::Handle::current(),
            Some(&client_paths),
            Duration::from_secs(2),
        )
        .await
        .expect("valid target proof opens v1 tunnel");
        assert!(handle.object_rpc_channel_client.is_some());
        assert!(handle.object_rpc_channel_ingress.is_some());
        assert!(
            handle
                .object_rpc_channel_server
                .lock()
                .expect("channel server lock")
                .is_some()
        );
        assert!(tcp_tunnel_object_rpc_channel_client(&server_node).is_some());
        handle.request_close("test complete".to_string());
        let _ = release_tx.send(());
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("v1 proof server stops")
            .expect("v1 proof server task");
        TCP_TUNNELS.remove(&server_node);
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

    #[test]
    fn tcp_tunnel_maintenance_allows_either_peer_to_dial() {
        let low = NodeId::from_legacy_named("tcp_tunnel_maintenance_low").expect("low node id");
        let high = NodeId::from_legacy_named("tcp_tunnel_maintenance_high").expect("high node id");
        let (first, second) = if low < high { (low, high) } else { (high, low) };

        assert!(tcp_tunnel_should_maintain_outbound(&first, &second));
        assert!(tcp_tunnel_should_maintain_outbound(&second, &first));
        assert!(!tcp_tunnel_should_maintain_outbound(&first, &first));
    }

    #[tokio::test]
    async fn tcp_tunnel_maintenance_consumes_exact_source_generation_before_sleep() {
        let sources = TcpTunnelMaintenanceSources::default();
        let completed = sources.generations();
        sources.request(TcpTunnelMaintenanceReason::MembershipChanged);
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let wake = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_tcp_tunnel_maintenance_wake(&sources, completed, None, &mut shutdown_rx),
        )
        .await
        .expect("pre-issued source generation should be retained");

        assert_eq!(wake, TcpTunnelMaintenanceWake::SourceChanged);
        assert_eq!(sources.generations().membership, completed.membership + 1);
        assert_eq!(sources.generations().role, completed.role);
    }

    #[tokio::test]
    async fn tcp_tunnel_healthy_idle_has_no_maintenance_deadline() {
        let sources = TcpTunnelMaintenanceSources::default();
        let completed = sources.generations();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                wait_for_tcp_tunnel_maintenance_wake(&sources, completed, None, &mut shutdown_rx,)
            )
            .await
            .is_err(),
            "a healthy tunnel owner must remain asleep without a source edge"
        );
    }

    #[tokio::test]
    async fn tcp_tunnel_maintenance_shutdown_wins_over_pending_refresh() {
        let sources = TcpTunnelMaintenanceSources::default();
        let completed = sources.generations();
        sources.request(TcpTunnelMaintenanceReason::SessionDisconnected);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        shutdown_tx.send_replace(true);

        let wake =
            wait_for_tcp_tunnel_maintenance_wake(&sources, completed, None, &mut shutdown_rx).await;

        assert_eq!(wake, TcpTunnelMaintenanceWake::Shutdown);
    }

    #[tokio::test]
    async fn tcp_tunnel_shutdown_interrupts_inflight_maintenance() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let maintenance = tokio::spawn(async move {
            wait_for_tcp_tunnel_maintenance_or_shutdown(
                std::future::pending::<()>(),
                &mut shutdown_rx,
            )
            .await
        });

        tokio::task::yield_now().await;
        shutdown_tx.send_replace(true);
        let result = tokio::time::timeout(Duration::from_secs(1), maintenance)
            .await
            .expect("shutdown must interrupt tunnel maintenance")
            .expect("maintenance task");

        assert_eq!(result, None);
    }

    #[test]
    fn tcp_tunnel_maintenance_is_fenced_to_startup_identity() {
        let cluster_id = ClusterId::from_legacy_named("tcp_tunnel_startup_identity_cluster")
            .expect("cluster id");
        let node_id =
            NodeId::from_legacy_named("tcp_tunnel_startup_identity_node").expect("node id");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "tcp tunnel startup identity",
            node_id.clone(),
            "node",
            "pk-node",
            time::OffsetDateTime::now_utc(),
        );

        assert!(tcp_tunnel_state_matches_startup_identity(
            &state,
            &cluster_id,
            &node_id,
        ));
        assert!(!tcp_tunnel_state_matches_startup_identity(
            &state,
            &ClusterId::from_legacy_named("tcp_tunnel_other_cluster").expect("other cluster"),
            &node_id,
        ));
        assert!(!tcp_tunnel_state_matches_startup_identity(
            &state,
            &cluster_id,
            &NodeId::from_legacy_named("tcp_tunnel_other_node").expect("other node"),
        ));
    }

    #[test]
    fn tcp_tunnel_topology_subscription_setup_failure_is_returned() {
        let paths = DaemonPaths::with_root(std::path::PathBuf::from("/"));
        let node_id =
            NodeId::from_legacy_named("tcp_tunnel_subscription_failure_node").expect("node id");

        let error = subscribe_tcp_tunnel_topology_sources(&paths, &node_id)
            .expect_err("a daemon root without a data-directory parent must fail setup");

        assert!(matches!(
            error,
            DaemonError::Io { detail, .. }
                if detail == "daemon paths have no data directory parent"
        ));
    }

    #[test]
    fn tcp_tunnel_healthy_states_never_own_a_timer() {
        let now = Instant::now();
        for role in [
            ClusterNodeRole::Workstation,
            ClusterNodeRole::Mobile,
            ClusterNodeRole::Server,
            ClusterNodeRole::Sensor,
            ClusterNodeRole::Test,
        ] {
            for state in [
                TcpTunnelMaintenanceState::IdleConnected,
                TcpTunnelMaintenanceState::Quiescent,
            ] {
                assert_eq!(
                    tcp_tunnel_maintenance_next_deadline(now, &role, state, 20),
                    None,
                    "healthy {role:?}/{state:?} unexpectedly owns a deadline"
                );
            }
        }
    }

    #[test]
    fn tcp_tunnel_retry_backoff_is_bounded_by_role_network_cadence() {
        assert_eq!(
            tcp_tunnel_maintenance_retry_delay(&ClusterNodeRole::Workstation, 1),
            Duration::from_secs(2)
        );
        assert_eq!(
            tcp_tunnel_maintenance_retry_delay(&ClusterNodeRole::Workstation, 2),
            Duration::from_secs(4)
        );
        assert_eq!(
            tcp_tunnel_maintenance_retry_delay(&ClusterNodeRole::Mobile, 8),
            Duration::from_secs(256)
        );
        assert_eq!(
            tcp_tunnel_maintenance_retry_delay(&ClusterNodeRole::Workstation, 20),
            sinomo_core::network::cluster_network_probe_interval_for_role(
                &ClusterNodeRole::Workstation
            )
        );
        assert_eq!(
            tcp_tunnel_maintenance_retry_delay(&ClusterNodeRole::Sensor, 20),
            sinomo_core::network::cluster_network_probe_interval_for_role(&ClusterNodeRole::Sensor)
        );
    }

    #[test]
    fn tcp_tunnel_topology_projection_ignores_volatile_member_refreshes() {
        let cluster_id =
            ClusterId::from_legacy_named("tcp_tunnel_projection_cluster").expect("cluster id");
        let local =
            NodeId::from_legacy_named("tcp_tunnel_projection_local").expect("local node id");
        let peer = NodeId::from_legacy_named("tcp_tunnel_projection_peer").expect("peer node id");
        let member_key = sinomo_core::node::cluster_member_key(&local);
        let now = time::OffsetDateTime::now_utc();
        let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id,
            local,
            "local",
            "public-key-a",
            now,
        );
        member.endpoints.push(ClusterEndpointRecord {
            endpoint_id: sinomo_core::Id::from_legacy_named("tcp_tunnel_projection_endpoint")
                .expect("endpoint id"),
            node_id: peer,
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.2:21353".to_string(),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: Some(1),
        });
        let mut store = sinomo_core::kv::InProcessKvStore::new();
        store
            .put_serde(member_key.clone(), &member, None)
            .expect("seed member");
        let mut projection = TcpTunnelTopologyProjection::default();
        projection.seed_if_newer(&store.snapshot().expect("seed snapshot"), &member_key);

        let mut volatile = member.clone();
        volatile.display_name = "renamed but same tunnel topology".to_string();
        volatile.updated_at = now + time::Duration::seconds(30);
        volatile.endpoints[0].last_observed_latency_ms = Some(999);
        store
            .put_serde(member_key.clone(), &volatile, None)
            .expect("refresh volatile member fields");
        assert_eq!(
            projection.observe(&store.snapshot().expect("volatile snapshot"), &member_key,),
            TcpTunnelTopologyChanges::default(),
            "timestamps, display metadata, and endpoint latency are not tunnel topology"
        );

        let mut auth_changed = volatile.clone();
        auth_changed.node_public_key = "public-key-b".to_string();
        store
            .put_serde(member_key.clone(), &auth_changed, None)
            .expect("change member auth material");
        assert_eq!(
            projection.observe(&store.snapshot().expect("auth snapshot"), &member_key),
            TcpTunnelTopologyChanges {
                membership: true,
                role: false,
            }
        );

        let mut role_changed = auth_changed;
        role_changed.node_role = ClusterNodeRole::Server;
        store
            .put_serde(member_key.clone(), &role_changed, None)
            .expect("change local role");
        assert_eq!(
            projection.observe(&store.snapshot().expect("role snapshot"), &member_key),
            TcpTunnelTopologyChanges {
                membership: true,
                role: true,
            }
        );
    }

    #[test]
    fn tcp_tunnel_maintenance_retires_removed_members() {
        let root = sinomo_testdir::TestDir::new("sinomo-tcp-tunnel-member-prune");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("tcp_tunnel_member_prune_cluster")
                .expect("cluster id");
        let local = NodeId::from_legacy_named("tcp_tunnel_member_prune_local").expect("local node");
        let peer = NodeId::from_legacy_named("tcp_tunnel_member_prune_peer").expect("peer node");
        let now = time::OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "tcp tunnel member prune",
            local,
            "local",
            "local-public-key",
            now,
        );
        let peer_member = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id,
            peer.clone(),
            "peer",
            "peer-public-key",
            now,
        );
        let peer_key = match sinomo_core::cluster::cluster_member_put_command(&peer_member)
            .expect("peer KV command")
        {
            sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put { key, .. }) => key,
            other => panic!("unexpected peer KV command: {other:?}"),
        };
        state.members.push(peer_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write bootstrap state");
        let kv_state = state.clone();
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &kv_state)
            .expect("sync initial KV membership");
        let merged = read_cluster_state_cached_shared(&cluster_paths).expect("merged membership");
        assert!(tcp_tunnel_peer_is_authorized(&merged, &peer));

        sinomo_core::cluster::append_cluster_raft_command(
            &cluster_paths,
            &kv_state.local_node_id,
            sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Delete { key: peer_key }),
        )
        .expect("commit removed KV membership");
        let stale_bootstrap = read_cluster_state_without_kv(&cluster_paths)
            .expect("stale bootstrap membership remains readable");
        assert!(
            tcp_tunnel_peer_is_authorized(&stale_bootstrap, &peer),
            "the regression requires bootstrap state to remain stale"
        );
        let merged = read_cluster_state_cached_shared(&cluster_paths)
            .expect("refreshed authoritative membership");
        assert!(!tcp_tunnel_peer_is_authorized(&merged, &peer));
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

    #[tokio::test]
    async fn tcp_tunnel_connect_lock_leases_do_not_retain_peer_churn() {
        let registry = TcpTunnelRegistry::default();
        for index in 0..512 {
            let peer = NodeId::from_legacy_named(format!("tcp_tunnel_connect_lock_churn_{index}"))
                .expect("test node id");
            let lease = registry.connect_lock_lease(&peer);
            let guard = lease.lock.lock().await;
            drop(guard);
            drop(lease);
        }
        assert!(
            registry
                .connect_locks
                .lock()
                .expect("connect locks")
                .is_empty(),
            "completed or cancelled connection attempts must not retain historical peer ids"
        );
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

    #[tokio::test]
    async fn tcp_tunnel_canonical_v1_replacement_converges_during_active_reverse_call() {
        let registry = TcpTunnelRegistry::default();
        let peer_name = "tcp_tunnel_duplicate_active_channel_peer";
        let peer = NodeId::from_legacy_named(peer_name).expect("test node id");
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("object RPC channel");
        let request =
            ObjectRpcRequest::encode_body("/test/tunnel-active-channel", "test", &"request")
                .expect("request");
        let pending = endpoint
            .client
            .call(sinomo_core::network::ObjectRpcSessionCall::value(request))
            .await
            .expect("pending channel call");
        assert_eq!(endpoint.client.queue_snapshot().active_calls, 1);

        let (first_tx, _first_rx) = mpsc::channel(1);
        let (second_tx, _second_rx) = mpsc::channel(1);
        let first = test_tunnel_handle_with_direction_and_channel(
            peer_name,
            first_tx,
            false,
            Some(endpoint.client.clone()),
        );
        let replacement_endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("replacement object RPC channel");
        let second = test_tunnel_handle_with_direction_and_channel(
            peer_name,
            second_tx,
            true,
            Some(replacement_endpoint.client.clone()),
        );

        assert!(registry.insert_or_existing(first.clone()).is_ok());
        assert!(registry.insert_or_existing(second.clone()).is_ok());

        assert!(Arc::ptr_eq(
            &registry.get(&peer).expect("registered"),
            &second
        ));
        assert!(first.closing.load(Ordering::Acquire));
        assert!(first.clean_shutdown.load(Ordering::Acquire));
        assert!(!second.closing.load(Ordering::Acquire));

        // Repeated reverse-direction maintenance opens must not dislodge the
        // canonical v1 carrier, including while that carrier has a call in
        // flight.
        let replacement_request = ObjectRpcRequest::encode_body(
            "/test/tunnel-stable-canonical-channel",
            "test",
            &"request",
        )
        .expect("replacement request");
        let replacement_pending = replacement_endpoint
            .client
            .call(sinomo_core::network::ObjectRpcSessionCall::value(
                replacement_request,
            ))
            .await
            .expect("pending replacement channel call");
        for _ in 0..8 {
            let (duplicate_tx, _duplicate_rx) = mpsc::channel(1);
            let duplicate = test_tunnel_handle_with_direction(peer_name, duplicate_tx, false);
            assert!(registry.insert_or_existing(duplicate).is_err());
            assert!(Arc::ptr_eq(
                &registry
                    .get(&peer)
                    .expect("stable canonical registry entry"),
                &second
            ));
            assert!(!second.closing.load(Ordering::Acquire));
            assert!(!replacement_endpoint.client.is_closed());
        }
        drop(replacement_pending);
        drop(pending);
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

    #[tokio::test]
    async fn tcp_tunnel_reader_rejects_oversized_frame_from_length_prefix() {
        let declared_bytes = MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES + 1;
        let mut encoded = b"SOF2".to_vec();
        encoded.extend_from_slice(&(declared_bytes as u64).to_be_bytes());
        let mut reader = std::io::Cursor::new(encoded);

        let error = read_tcp_tunnel_frame(&mut reader, ObjectFrameIoOptions::none())
            .await
            .expect_err("oversized tunnel frame");

        assert_eq!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes: declared_bytes,
                max_bytes: MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES,
            }
        );
        assert_eq!(
            reader.position(),
            12,
            "tunnel limit must reject immediately after the SOF2 length prefix"
        );
    }

    #[tokio::test]
    async fn tcp_tunnel_reader_bounds_partial_frame_stalls_but_not_inter_frame_idle() {
        const NO_PROGRESS_TIMEOUT: Duration = Duration::from_millis(20);
        let mut partial_body = b"SOF2".to_vec();
        partial_body.extend_from_slice(&64_u64.to_be_bytes());
        partial_body.extend_from_slice(b"partial");

        for (index, (label, stalled_bytes)) in [
            ("partial prefix", b"S".as_slice()),
            ("partial body", partial_body.as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            let (mut writer, mut reader) = tokio::io::duplex(128);
            let read = tokio::spawn(async move {
                read_tcp_tunnel_frame(
                    &mut reader,
                    ObjectFrameIoOptions::idle_timeout(NO_PROGRESS_TIMEOUT),
                )
                .await
            });
            if index == 0 {
                tokio::time::sleep(Duration::from_millis(60)).await;
                assert!(
                    !read.is_finished(),
                    "an idle tunnel must wait indefinitely before the next frame starts"
                );
            }
            writer
                .write_all(stalled_bytes)
                .await
                .unwrap_or_else(|error| panic!("write {label}: {error}"));
            let error = tokio::time::timeout(Duration::from_secs(1), read)
                .await
                .unwrap_or_else(|_| panic!("{label} reader did not retire"))
                .expect("reader task")
                .expect_err("partial frame must time out");
            assert!(
                error.to_string().contains("timed out"),
                "unexpected {label} error: {error}"
            );
        }
    }

    #[tokio::test]
    async fn v1_tunnel_channel_carrier_runs_full_duplex_calls() {
        let left = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("left channel");
        let right = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Acceptor,
            ObjectRpcChannelConfig::default(),
        )
        .expect("right channel");
        let ObjectRpcChannelEndpoint {
            client: left_client,
            server: mut left_server,
            ingress: left_ingress,
            egress: left_egress,
        } = left;
        let ObjectRpcChannelEndpoint {
            client: right_client,
            server: mut right_server,
            ingress: right_ingress,
            egress: right_egress,
        } = right;
        let (left_tx, _left_rx) = mpsc::channel(1);
        let (right_tx, _right_rx) = mpsc::channel(1);
        let left_handle = test_tunnel_handle("tunnel_channel_right", left_tx);
        let right_handle = test_tunnel_handle("tunnel_channel_left", right_tx);
        let (left_wire, right_wire) = tokio::io::duplex(128 * 1024);
        let (left_read, left_write) = tokio::io::split(left_wire);
        let (right_read, right_write) = tokio::io::split(right_wire);
        let left_writer = tokio::spawn(tcp_tunnel_writer_loop(
            Arc::clone(&left_handle),
            left_write,
            left_egress,
        ));
        let right_writer = tokio::spawn(tcp_tunnel_writer_loop(
            Arc::clone(&right_handle),
            right_write,
            right_egress,
        ));
        let right_wire_ingress = right_ingress.clone();
        let left_to_right = tokio::spawn(async move {
            let mut reader = right_read;
            loop {
                match read_object_frame_async::<_, ClusterTcpTunnelFrame>(
                    &mut reader,
                    ObjectFrameIoOptions::none(),
                )
                .await
                .expect("left-to-right tunnel frame")
                {
                    ClusterTcpTunnelFrame::ObjectRpcChannel { frame } => right_wire_ingress
                        .receive_control(frame)
                        .expect("right channel ingress"),
                    ClusterTcpTunnelFrame::Close { .. } => break,
                }
            }
        });
        let left_wire_ingress = left_ingress.clone();
        let right_to_left = tokio::spawn(async move {
            let mut reader = left_read;
            loop {
                match read_object_frame_async::<_, ClusterTcpTunnelFrame>(
                    &mut reader,
                    ObjectFrameIoOptions::none(),
                )
                .await
                .expect("right-to-left tunnel frame")
                {
                    ClusterTcpTunnelFrame::ObjectRpcChannel { frame } => left_wire_ingress
                        .receive_control(frame)
                        .expect("left channel ingress"),
                    ClusterTcpTunnelFrame::Close { .. } => break,
                }
            }
        });

        let left_request =
            ObjectRpcRequest::encode_body("/test/tunnel-channel/left", "test", &"from-left")
                .expect("left request");
        let right_request =
            ObjectRpcRequest::encode_body("/test/tunnel-channel/right", "test", &"from-right")
                .expect("right request");
        let left_pending = left_client
            .call(sinomo_core::network::ObjectRpcSessionCall::value(
                left_request,
            ))
            .await
            .expect("left call");
        let right_pending = right_client
            .call(sinomo_core::network::ObjectRpcSessionCall::value(
                right_request,
            ))
            .await
            .expect("right call");
        let (from_left, from_right) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(right_server.accept(), left_server.accept())
        })
        .await
        .expect("duplex request timeout");
        let from_left = from_left.expect("right accepted left call");
        let from_right = from_right.expect("left accepted right call");
        assert_eq!(
            from_left
                .request
                .request
                .decode_body::<String>()
                .expect("left body"),
            "from-left"
        );
        assert_eq!(
            from_right
                .request
                .request
                .decode_body::<String>()
                .expect("right body"),
            "from-right"
        );
        let left_response =
            ObjectRpcResponse::encode_body("/test/tunnel-channel/left", &"from-right-server")
                .expect("left response");
        let right_response =
            ObjectRpcResponse::encode_body("/test/tunnel-channel/right", &"from-left-server")
                .expect("right response");
        let (left_responded, right_responded) = tokio::join!(
            from_left.respond(sinomo_core::network::ObjectRpcSessionResponse::value(
                left_response,
            )),
            from_right.respond(sinomo_core::network::ObjectRpcSessionResponse::value(
                right_response,
            )),
        );
        left_responded.expect("respond to left");
        right_responded.expect("respond to right");
        let (left_result, right_result) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(left_pending.result(), right_pending.result())
        })
        .await
        .expect("duplex response timeout");
        assert_eq!(
            left_result
                .expect("left result")
                .response
                .decode_body::<String>()
                .expect("left response body"),
            "from-right-server"
        );
        assert_eq!(
            right_result
                .expect("right result")
                .response
                .decode_body::<String>()
                .expect("right response body"),
            "from-left-server"
        );

        left_ingress.close("test complete");
        right_ingress.close("test complete");
        left_handle.request_close("test complete".to_string());
        right_handle.request_close("test complete".to_string());
        for task in [left_writer, right_writer, left_to_right, right_to_left] {
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("carrier task stops")
                .expect("carrier task");
        }
    }
}
