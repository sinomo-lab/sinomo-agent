//! Mounted SIFS adapter over local cluster state and DFS peer transports.
//!
//! This module owns the low-level glue required to open SIFS as a mounted
//! networked DFS service from a local data directory.

use sinomo_core::ClusterBootstrapState;
use sinomo_core::cluster::ClusterKvPathsExt;
#[cfg(test)]
use sinomo_core::kv::KvSnapshot;
use sinomo_core::kv::{KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterPaths, NetworkError, ObjectRpcCall, ObjectRpcResponse, ObjectRpcResult,
};
use sinomo_core::node::NodeId;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::node_transport::collect_dfs_incoming_body_sync;
use crate::{
    AsyncDfsPeerTransport, DfsCommittedWork, DfsConfigurationPublication, DfsControlPlaneKvClient,
    DfsError,
    DfsMessageTransport, DfsNamespaceChangeBatch, DfsNamespaceChangeCursor, DfsNodeAdvertisement,
    DfsNodeId, DfsNodeTransport, DfsObjectRpcSendOptions, DfsPeerTransport, DfsPeerTransportError,
    DfsPeerTransportFuture, DfsPosixError, NetworkedDfsService,
    cluster_dfs_peer_node_ids, content_services_prefix,
    open_cluster_dfs_service_with_peer_transports_for_nodes_lazy,
};

pub type MountedSifsService = NetworkedDfsService<DfsNodeTransport>;

thread_local! {
    static MOUNTED_CALL_DEADLINE: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

/// Deadline of one synchronous adapter callback. Async adapters carry its absolute
/// value across scheduling and enter it only inside the blocking worker.
#[must_use]
pub struct MountedSifsCall {
    previous: Option<Instant>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl MountedSifsCall {
    pub fn enter(deadline: Option<Instant>) -> Self {
        let deadline = Self::earliest(deadline);
        let previous = MOUNTED_CALL_DEADLINE.replace(deadline);
        Self {
            previous,
            _thread: std::marker::PhantomData,
        }
    }

    pub fn deadline() -> Option<Instant> {
        MOUNTED_CALL_DEADLINE.get()
    }

    pub fn earliest(other: Option<Instant>) -> Option<Instant> {
        Self::deadline().into_iter().chain(other).min()
    }

    /// Take `lock` within this callback's deadline. A callback without a
    /// deadline waits for the lock's release; one whose deadline has passed
    /// does not enter.
    pub fn lock<T>(lock: &MountedSifsLock<T>) -> std::io::Result<MountedSifsLockGuard<'_, T>> {
        match Self::deadline() {
            Some(deadline) if deadline <= Instant::now() => {
                Err(std::io::ErrorKind::TimedOut.into())
            }
            deadline => lock.lock_until(deadline),
        }
    }

    pub fn lock_service(
        service: &MountedSifsLock<MountedSifsService>,
    ) -> std::io::Result<MountedSifsServiceGuard<'_>> {
        let mut service = Self::lock(service)?;
        let deadline = Self::earliest(service.transport.operation_deadline());
        let previous = service.set_transport_operation_deadline(deadline);
        Ok(MountedSifsServiceGuard { service, previous })
    }
}

impl Drop for MountedSifsCall {
    fn drop(&mut self) {
        MOUNTED_CALL_DEADLINE.set(self.previous);
    }
}

/// A mutex of a mounted adapter. A waiter sleeps until the holder releases
/// the lock or the waiter's own deadline passes; nothing polls.
///
/// A holder that panics poisons the lock, as [`std::sync::Mutex`] does, and
/// every later attempt reports it.
#[derive(Debug)]
pub struct MountedSifsLock<T> {
    state: Mutex<MountedSifsLockState<T>>,
    released: Condvar,
}

#[derive(Debug)]
struct MountedSifsLockState<T> {
    /// The protected value while no guard holds it.
    value: Option<Box<T>>,
    poisoned: bool,
}

impl<T: Default> Default for MountedSifsLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> MountedSifsLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            state: Mutex::new(MountedSifsLockState {
                value: Some(Box::new(value)),
                poisoned: false,
            }),
            released: Condvar::new(),
        }
    }

    /// Wait for the lock, however long its holder keeps it.
    pub fn lock(&self) -> std::io::Result<MountedSifsLockGuard<'_, T>> {
        self.lock_until(None)
    }

    /// Take the lock only when no guard holds it now.
    pub fn try_lock(&self) -> std::io::Result<MountedSifsLockGuard<'_, T>> {
        let mut state = self.state.lock().map_err(|_| Self::poisoned())?;
        if state.poisoned {
            return Err(Self::poisoned());
        }
        match state.value.take() {
            Some(value) => Ok(MountedSifsLockGuard {
                lock: self,
                value: Some(value),
            }),
            None => Err(std::io::ErrorKind::WouldBlock.into()),
        }
    }

    /// Wait for the lock until `deadline`; `None` waits for its release.
    pub fn lock_until(
        &self,
        deadline: Option<Instant>,
    ) -> std::io::Result<MountedSifsLockGuard<'_, T>> {
        let mut state = self.state.lock().map_err(|_| Self::poisoned())?;
        loop {
            if state.poisoned {
                return Err(Self::poisoned());
            }
            if let Some(value) = state.value.take() {
                return Ok(MountedSifsLockGuard {
                    lock: self,
                    value: Some(value),
                });
            }
            state = match deadline {
                None => self.released.wait(state).map_err(|_| Self::poisoned())?,
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(std::io::ErrorKind::TimedOut.into());
                    }
                    self.released
                        .wait_timeout(state, remaining)
                        .map_err(|_| Self::poisoned())?
                        .0
                }
            };
        }
    }

    fn poisoned() -> std::io::Error {
        std::io::Error::other("mounted filesystem lock poisoned")
    }
}

/// Exclusive access to the value of a [`MountedSifsLock`]. Dropping the guard
/// releases the lock and wakes one waiter.
#[derive(Debug)]
pub struct MountedSifsLockGuard<'a, T> {
    lock: &'a MountedSifsLock<T>,
    value: Option<Box<T>>,
}

impl<T> std::ops::Deref for MountedSifsLockGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value
            .as_deref()
            .expect("a live guard holds the locked value")
    }
}

impl<T> std::ops::DerefMut for MountedSifsLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.value
            .as_deref_mut()
            .expect("a live guard holds the locked value")
    }
}

impl<T> Drop for MountedSifsLockGuard<'_, T> {
    fn drop(&mut self) {
        let mut state = self
            .lock
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.value = self.value.take();
        state.poisoned |= std::thread::panicking();
        drop(state);
        // Every waiter of a poisoned lock must learn it; otherwise one
        // release admits one waiter.
        if std::thread::panicking() {
            self.lock.released.notify_all();
        } else {
            self.lock.released.notify_one();
        }
    }
}

pub struct MountedSifsServiceGuard<'a> {
    service: MountedSifsLockGuard<'a, MountedSifsService>,
    previous: Option<Instant>,
}

impl std::ops::Deref for MountedSifsServiceGuard<'_> {
    type Target = MountedSifsService;
    fn deref(&self) -> &Self::Target {
        &self.service
    }
}

impl std::ops::DerefMut for MountedSifsServiceGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.service
    }
}

impl Drop for MountedSifsServiceGuard<'_> {
    fn drop(&mut self) {
        self.service.set_transport_operation_deadline(self.previous);
    }
}

/// Outcome plus the exact durable work produced by one mounted mutation.
///
/// Mounted adapters use this public seam instead of reaching into the
/// served-message implementation. The outcome is deliberately generic so an
/// adapter always receives the committed-work receipt even when its operation
/// returns an error after a durable metadata edge.
#[derive(Debug)]
pub struct MountedSifsMutationReport<R> {
    pub outcome: R,
    pub committed_work: DfsCommittedWork,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Capture every durable edge produced by one direct mounted mutation.
    ///
    /// An already-installed [`crate::DfsCommittedWorkTracker`] remains in
    /// place and continues to receive the same edges. This method consumes
    /// only the operation-local receipt and repair-job delta that it begins.
    pub fn capture_mounted_mutation<R>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> MountedSifsMutationReport<R> {
        self.begin_message_committed_work();
        let outcome = operation(self);
        let committed_work = self.take_message_committed_work();
        MountedSifsMutationReport {
            outcome,
            committed_work,
        }
    }
}

fn mounted_sifs_authority_deadline_error() -> KvError {
    KvError::AuthorityUnavailable {
        detail: "metadata shard authority read deadline elapsed".to_string(),
    }
}

/// Read-only content capability detached from mounted DFS authority state.
///
/// Open file descriptors already carry an exact immutable object/version/hash
/// identity. Reading that identity needs the local content store and peer
/// routes, but it must not hold (or clone) the mutable mounted service while
/// disk or network I/O is in progress. Each call below creates request-local
/// retry/response state over these thread-safe shared capabilities.
#[derive(Clone, Debug)]
pub struct MountedSifsContentReadContext {
    content: crate::LocalContentStore,
    transport: DfsNodeTransport,
    cancellation: Option<crate::DfsCancellationFlag>,
}

impl MountedSifsContentReadContext {
    /// Read a bounded range from the exact content identity captured at open.
    ///
    /// This deliberately has no pathname metadata lookup and no control-plane
    /// handle. Local reads retain manifest/hash validation; remote reads retain
    /// the normal exact-version range request, response validation, retry, and
    /// timeout behavior.
    pub fn read_opened_file_range(
        &self,
        path: &str,
        opened: &crate::DfsStat,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>, crate::DfsPosixError> {
        let mut request = NetworkedDfsService::new(
            self.transport.fork_read_only_request(),
            self.content.clone(),
        );
        request.set_cancellation(self.cancellation.clone());
        request.set_transport_operation_deadline(MountedSifsCall::earliest(
            request.transport.operation_deadline(),
        ));
        // A queued FUSE read can begin after unmount cancellation. Observe it
        // before the exact-content helper has any opportunity to send its
        // first remote request.
        request.check_cancelled("mounted opened-file read before content transfer")?;
        request.posix_read_file_range_for_opened_stat(path, opened, offset, max_bytes)
    }
}

impl NetworkedDfsService<DfsNodeTransport> {
    /// Snapshot the thread-safe capabilities needed by opened-descriptor reads.
    ///
    /// Metadata, coordinators, pending writes, namespace state, and the control
    /// plane remain exclusively owned by the mounted service.
    pub fn mounted_content_read_context(&self) -> MountedSifsContentReadContext {
        MountedSifsContentReadContext {
            content: self.content.clone(),
            transport: self.transport.fork_read_only_request(),
            cancellation: self.cancellation.clone(),
        }
    }

    /// Poll mounted SIFS namespace changes as independent shard-local batches.
    ///
    /// This is a named mounted-service seam for platform modules; it preserves
    /// the generic service contract that the returned vector has no implied
    /// ordering across shards.
    pub fn poll_mounted_namespace_changes_for_subtree(
        &mut self,
        file_system_id: crate::DfsFileSystemId,
        root_path: &str,
        cursors: &[DfsNamespaceChangeCursor],
    ) -> Result<Vec<DfsNamespaceChangeBatch>, DfsError> {
        self.poll_namespace_changes_for_subtree(file_system_id, root_path, cursors)
    }
}

#[derive(Debug)]
pub struct OpenedMountedSifsService {
    pub service: MountedSifsService,
    pub replica_candidates: Vec<DfsNodeId>,
}

pub fn open_mounted_sifs_service(
    data_dir: Option<PathBuf>,
) -> Result<OpenedMountedSifsService, DfsError> {
    open_mounted_sifs_service_with_cancellation(data_dir, None)
}

/// Like [`open_mounted_sifs_service`], with cooperative cancellation for
/// namespace operations. Configuration reads retain their transport deadlines.
/// Budgeted callers (app catalog loads) flip the flag to abandon subsequent
/// namespace work promptly when the cluster link is degraded.
pub fn open_mounted_sifs_service_with_cancellation(
    data_dir: Option<PathBuf>,
    cancellation: Option<crate::DfsCancellationFlag>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let data_dir = match data_dir {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home().map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?,
    };
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    let peer_transport = Arc::new(MountedSifsPeerTransport {
        cluster_paths: cluster_paths.clone(),
    });
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer_transport;
    let control_plane = if cluster_state.is_some() {
        Some(Box::new(MountedSifsControlPlaneKvClient::new_authoritative(
            &data_dir,
            Arc::clone(&sync_peer_transport),
            Arc::clone(&async_peer_transport),
        )) as Box<dyn DfsControlPlaneKvClient>)
    } else {
        None
    };
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        cancellation,
        None,
    )
}

/// Opens mounted SIFS with caller-owned peer transports and routes clustered
/// control-plane commits through the same transport.
///
/// Application mounts use this entry point with a local-daemon-backed peer so
/// both content/metadata traffic and cluster-KV transactions share the
/// daemon's canonical routes and tunnels. Reads continue to use the local KV
/// snapshot; no clustered write falls back to a standalone router.
pub fn open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> Result<OpenedMountedSifsService, DfsError> {
    open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
        data_dir,
        sync_peer_transport,
        async_peer_transport,
        None,
    )
}

/// Like [`open_mounted_sifs_service_with_peer_transports_and_default_control_plane`],
/// with cooperative cancellation for namespace operations.
///
/// Budgeted application mounts use this entry point so abandoning catalog
/// hydration also cancels the daemon-backed distributed work it started.
pub fn open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    cancellation: Option<crate::DfsCancellationFlag>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    let control_plane = if cluster_state.is_some() {
        Some(Box::new(MountedSifsControlPlaneKvClient::new_authoritative(
            &data_dir,
            Arc::clone(&sync_peer_transport),
            Arc::clone(&async_peer_transport),
        )) as Box<dyn DfsControlPlaneKvClient>)
    } else {
        None
    };
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        cancellation,
        None,
    )
}

/// Opens mounted SIFS with caller-owned DFS peer transports.
///
/// Daemon hosts use this entry point so browser operations share the daemon's
/// established cluster router instead of constructing a second router inside
/// the daemon process. Any authorized cluster node remains a DFS client; shard
/// ownership only selects the target node for the request.
pub fn open_mounted_sifs_service_with_peer_transports(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        None,
        None,
    )
}

/// Core browser read input captured from its actual Current configuration.
/// This supplies only candidate DATA; normal mounted routing, bootstrap and
/// all subsequent native publication/admission checks remain unchanged.
pub(crate) fn open_mounted_sifs_service_with_peer_transports_and_configuration(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    configuration: Arc<crate::DfsWriteConfigurationProjection>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    let candidates = configuration.content_replica_candidates()?;
    open_mounted_sifs_service_from_context(
        &data_dir, &cluster_paths, cluster_state.as_ref(), local_node_id,
        peer_node_ids, sync_peer_transport, async_peer_transport, control_plane,
        None, Some(candidates),
    )
}

/// Build the hosted browser as a requester of its registered Main. Metadata
/// queries and mutations use that owner; the browser retains no native image.
pub(crate) fn open_hosted_browser_sifs_service(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    configuration: Option<Arc<crate::DfsWriteConfigurationProjection>>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (_, cluster_state, local_node_id, peer_node_ids) = mounted_sifs_cluster_context(&data_dir)?;
    let control_plane = control_plane.ok_or_else(|| DfsError::Metadata {
        message: "hosted SIFS browser requires its Main control-plane client".into(),
    })?;
    if cluster_state.is_none() {
        return Err(DfsError::Metadata {
            message: "hosted SIFS browser requires an initialized cluster".into(),
        });
    }
    let replica_candidates = match configuration {
        Some(configuration) => configuration.content_replica_candidates()?,
        None => mounted_sifs_replica_candidates_from_context(
            cluster_state.as_ref(),
            local_node_id.clone(),
            Some(control_plane.as_ref()),
        )?,
    };
    let transport = DfsNodeTransport::with_peer_transports_for_nodes(
        local_node_id,
        peer_node_ids,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        sync_peer_transport,
        async_peer_transport,
    )
    .with_registered_main_namespace_dispatch();
    let content = crate::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let mut service =
        NetworkedDfsService::new(transport, content).with_boxed_control_plane(control_plane);
    service
        .bootstrap_distributed_sifs()
        .map_err(|source| match source {
            DfsPosixError::Dfs(error) => error,
            other => DfsError::Metadata {
                message: other.to_string(),
            },
        })?;
    Ok(OpenedMountedSifsService {
        service,
        replica_candidates,
    })
}

fn mounted_sifs_cluster_context(
    data_dir: &SinomoDataDir,
) -> Result<
    (
        ClusterPaths,
        Option<ClusterBootstrapState>,
        NodeId,
        Vec<NodeId>,
    ),
    DfsError,
> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let cluster_state = if cluster_paths.state.exists() {
        Some(
            sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
                .map_err(DfsError::from_network)?,
        )
    } else {
        None
    };
    let local_node_id = cluster_state
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .ok_or_else(|| DfsError::Metadata {
            message: "SIFS requires an initialized node".into(),
        })?;
    let peer_node_ids = cluster_state
        .as_ref()
        .map(cluster_dfs_peer_node_ids)
        .unwrap_or_default();
    Ok((cluster_paths, cluster_state, local_node_id, peer_node_ids))
}

#[allow(clippy::too_many_arguments)]
fn open_mounted_sifs_service_from_context(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    cluster_state: Option<&ClusterBootstrapState>,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    cancellation: Option<crate::DfsCancellationFlag>,
    captured_replica_candidates: Option<Vec<DfsNodeId>>,
) -> Result<OpenedMountedSifsService, DfsError> {
    // A mounted client does not own maintenance or recovery. Hydrate only the
    // filesystem records needed to route canonical operations; path/object
    // metadata remains authoritative in the backend and is loaded on demand.
    // Full metadata hydration here made every short-lived app/CLI catalog load
    // scan the entire local SIFS replica before its first distributed read.
    let replica_candidates = match captured_replica_candidates {
        Some(candidates) => candidates,
        None => mounted_sifs_replica_candidates_from_context(
            cluster_state, local_node_id.clone(), control_plane.as_deref(),
        )?,
    };
    let mut service = open_cluster_dfs_service_with_peer_transports_for_nodes_lazy(
        data_dir,
        cluster_paths,
        local_node_id.clone(),
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        cluster_state.is_some(),
    )?;
    service.set_cancellation(cancellation);
    if cluster_state.is_none() {
        service
            .bootstrap_distributed_sifs()
            .map_err(|source| match source {
                DfsPosixError::Dfs(error) => error,
                other => DfsError::Metadata {
                    message: other.to_string(),
                },
            })?;
    }
    Ok(OpenedMountedSifsService {
        service,
        replica_candidates,
    })
}

fn mounted_sifs_replica_candidates_from_context(
    cluster_state: Option<&ClusterBootstrapState>,
    local_node_id: NodeId,
    control_plane: Option<&dyn DfsControlPlaneKvClient>,
) -> Result<Vec<DfsNodeId>, DfsError> {
    if cluster_state.is_none() {
        return Ok(vec![local_node_id]);
    }
    let control_plane = control_plane.ok_or_else(|| {
        DfsError::from(KvError::AuthorityUnavailable {
            detail: "clustered mounted SIFS requires an authoritative DFS control-plane client"
                .to_string(),
        })
    })?;
    discover_mounted_sifs_content_replica_candidates_with_control_plane(control_plane)
}

/// Returns the current content-service nodes using a caller-owned control-plane client.
///
/// Daemon-hosted adapters use this seam so refreshes stay on the daemon's retained
/// cluster transports. In particular, this function never constructs a standalone
/// router, which would be unable to dispatch a same-node authority call through the
/// daemon's explicit local service path.
/// sinomo:kv-configuration: Load configured DFS provider eligibility; existing placement and network snapshots rank candidates.
pub fn discover_mounted_sifs_content_replica_candidates_with_control_plane(
    control_plane: &dyn DfsControlPlaneKvClient,
) -> Result<Vec<DfsNodeId>, DfsError> {
    if let Some(configuration) = control_plane.write_configuration_projection()? {
        return configuration.content_replica_candidates();
    }
    let content_entries = control_plane.range_prefix(content_services_prefix())?;
    let service_entries =
        control_plane.range_prefix(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)?;
    content_replica_candidates_from_configuration_entries(&content_entries, &service_entries)
}

pub(crate) fn content_replica_candidates_from_configuration_entries(
    content_entries: &[sinomo_core::kv::KvEntry],
    service_entries: &[sinomo_core::kv::KvEntry],
) -> Result<Vec<DfsNodeId>, DfsError> {
    let mut candidates = mounted_sifs_content_replica_candidates(
        content_entries
            .iter()
            .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok()),
    );
    if !service_entries.is_empty() {
        let instances = sinomo_core::service::service_instances_from_entries(service_entries)
            .map_err(|source| DfsError::Metadata {
                message: format!("cannot decode filesystem provider records: {source}"),
            })?;
        let providers = sinomo_core::service::service_instances_providing_capability(
            instances,
            "dfs.object.transfer",
        )
        .into_iter()
        .map(|instance| instance.node_id)
        .collect::<std::collections::BTreeSet<_>>();
        if !providers.is_empty() {
            candidates.retain(|node_id| providers.contains(node_id));
        }
    }
    Ok(candidates)
}

/// Returns the current content-service nodes eligible to store SIFS replicas.
///
/// Cluster membership grants client access but does not make a node a storage
/// target. In an initialized cluster this therefore returns only nodes with a
/// current `accepts_content` service advertisement. An unclustered standalone
/// mount keeps its local-only behavior.
pub fn discover_mounted_sifs_content_replica_candidates(
    data_dir: &SinomoDataDir,
) -> Result<Vec<DfsNodeId>, DfsError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    if !cluster_paths.state.exists() {
        return Err(DfsError::Metadata {
            message: "SIFS requires an initialized node".into(),
        });
    }
    let _state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DfsError::from_network)?;
    let peer_transport = Arc::new(MountedSifsPeerTransport { cluster_paths });
    let control_plane = MountedSifsControlPlaneKvClient::new_authoritative(
        data_dir,
        peer_transport.clone(),
        peer_transport,
    );
    discover_mounted_sifs_content_replica_candidates_with_control_plane(&control_plane)
}

#[cfg(test)]
fn mounted_sifs_materialized_snapshot(
    cluster_paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, KvError> {
    cluster_paths
        .kv_paths()
        .local_cache()
        .cached_snapshot_arc_if_present()
}

fn mounted_sifs_content_replica_candidates(
    advertisements: impl IntoIterator<Item = DfsNodeAdvertisement>,
) -> Vec<DfsNodeId> {
    let mut candidates = Vec::new();
    for advertisement in advertisements {
        if advertisement.accepts_content && !candidates.contains(&advertisement.node_id) {
            candidates.push(advertisement.node_id);
        }
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn mounted_mutation_capture_returns_error_receipt_and_preserves_outer_tracker() {
        let local_node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
            "native-mounted-mutation-capture",
            1,
        )
        .remove(0);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(
            "native-mounted-mutation-capture",
            local_node.clone(),
        )
        .unwrap();
        runtime.start_nodes().unwrap();
        let mut service = runtime.client(&local_node).unwrap();
        let tracker = crate::DfsCommittedWorkTracker::new();
        service.set_committed_work_tracker(tracker.clone());

        let report = service.capture_mounted_mutation(|service| {
            service
                .posix_mkdir("/services/actual-mounted-capture")
                .expect("actual guarded namespace commit");
            Err::<(), _>("after-commit")
        });

        assert_eq!(report.outcome, Err("after-commit"));
        assert!(report.committed_work.publish_metadata);
        assert!(report.committed_work.semantic_changes);
        let outer = tracker.take();
        assert!(outer.publish_metadata);
        assert_eq!(
            outer, report.committed_work,
            "all actual committed edges reach the outer tracker"
        );
        assert!(
            service
                .posix_stat_distributed("/services/actual-mounted-capture")
                .is_ok()
        );
        drop(service);
        runtime.close().unwrap();
    }


    #[test]
    fn a_mounted_lock_waiter_enters_on_release_and_leaves_at_its_deadline() {
        let lock = MountedSifsLock::new(0usize);
        let held = lock.lock().unwrap();
        // A waiter with a deadline leaves at it, and takes nothing.
        let started = Instant::now();
        let timed_out = lock
            .lock_until(Some(started + Duration::from_millis(40)))
            .expect_err("the lock is held past the deadline");
        assert_eq!(timed_out.kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() >= Duration::from_millis(40));
        assert_eq!(
            lock.try_lock().expect_err("held").kind(),
            std::io::ErrorKind::WouldBlock
        );

        // A waiter without one sleeps until the release wakes it.
        let (entered_tx, entered_rx) = mpsc::channel();
        thread::scope(|scope| {
            scope.spawn(|| {
                let mut guard = lock.lock().expect("the release admits the waiter");
                *guard += 1;
                entered_tx.send(Instant::now()).unwrap();
            });
            assert!(
                entered_rx.recv_timeout(Duration::from_millis(150)).is_err(),
                "the waiter cannot enter while the lock is held"
            );
            let released = Instant::now();
            drop(held);
            let entered = entered_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the waiter entered after the release");
            assert!(entered >= released);
        });
        assert_eq!(*lock.lock().unwrap(), 1);
    }

    #[test]
    fn a_callback_past_its_deadline_does_not_take_a_free_mounted_lock() {
        let lock = MountedSifsLock::new(());
        {
            let _call = MountedSifsCall::enter(Some(Instant::now()));
            assert_eq!(
                MountedSifsCall::lock(&lock)
                    .expect_err("the deadline passed")
                    .kind(),
                std::io::ErrorKind::TimedOut
            );
        }
        // Without a deadline the same lock is taken at once.
        assert!(MountedSifsCall::lock(&lock).is_ok());
    }

    #[test]
    fn a_panicking_holder_poisons_a_mounted_lock_for_every_waiter() {
        let lock = MountedSifsLock::new(());
        let panicked = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _guard = lock.lock().unwrap();
                    panic!("holder failed");
                })
                .join()
        });
        assert!(panicked.is_err());
        assert!(lock.lock().is_err());
        assert!(lock.try_lock().is_err());
        assert!(
            lock.lock_until(Some(Instant::now() + Duration::from_secs(5)))
                .is_err()
        );
    }

    /// Records the budget each request carries, as the serving node reads it.
    #[derive(Debug, Default)]
    struct BudgetRecordingPeerTransport {
        budgets: Mutex<Vec<Option<Duration>>>,
    }

    impl DfsPeerTransport for BudgetRecordingPeerTransport {
        fn call_object_rpc(
            &self,
            _target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.budgets
                .lock()
                .unwrap()
                .push(crate::routes::dfs_request_budget(&call.request));
            Err(DfsPeerTransportError::new("the budget was recorded"))
        }
    }

    impl AsyncDfsPeerTransport for BudgetRecordingPeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            let result = self.call_object_rpc(target_node_id, call, options);
            Box::pin(async move { result })
        }
    }

    #[test]
    fn a_configured_operation_budget_travels_with_each_request_a_callback_sends() {
        let root = sinomo_testdir::TestDir::new("mounted-operation-budget-request-key");
        let local = NodeId::from_legacy_named("node_mounted_budget_local").unwrap();
        let remote = NodeId::from_legacy_named("node_mounted_budget_remote").unwrap();
        let recorder = Arc::new(BudgetRecordingPeerTransport::default());
        let transport = DfsNodeTransport::with_peer_transports_for_nodes(
            local.clone(),
            vec![remote.clone()],
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            recorder.clone(),
            recorder.clone(),
        );
        let content = crate::LocalContentStore::new(root.path().join("content")).unwrap();
        let service = MountedSifsLock::new(NetworkedDfsService::new(transport, content));
        let verification = || {
            crate::DfsNetworkMessage::PrimaryVerificationRequest(
                crate::DfsPrimaryVerificationRequest {
                    file_system_id: crate::sifs_file_system_id(),
                    shard_id: "root".into(),
                    epoch: 1,
                    primary_node_id: remote.clone(),
                },
            )
        };

        // The text a mount's `--operation-budget` flag and its
        // `operation_budget` configuration field both carry.
        let budget = crate::parse_mount_operation_budget("30s")
            .unwrap()
            .expect("a non-empty budget");
        assert_eq!(budget, Duration::from_secs(30));
        {
            let _call = MountedSifsCall::enter(Instant::now().checked_add(budget));
            let mut service = MountedSifsCall::lock_service(&service).unwrap();
            let _ = service.send_dfs_message_if_routable(&remote, verification());
        }
        // A mount without a budget stamps none: its request has no deadline.
        assert_eq!(crate::parse_mount_operation_budget("").unwrap(), None);
        {
            let _call = MountedSifsCall::enter(None);
            let mut service = MountedSifsCall::lock_service(&service).unwrap();
            let _ = service.send_dfs_message_if_routable(&remote, verification());
        }

        let budgets = recorder.budgets.lock().unwrap().clone();
        assert_eq!(budgets.len(), 2, "each callback sent its request");
        let stamped = budgets[0].expect("the budgeted callback's request carries its budget");
        assert!(
            stamped <= budget && stamped > budget - Duration::from_secs(5),
            "the request carries what is left of the mount's budget: {stamped:?}"
        );
        assert_eq!(budgets[1], None, "no budget, no key");
    }

    #[derive(Debug)]
    struct RecordingMountedPeerTransport {
        targets: std::sync::Mutex<Vec<NodeId>>,
    }

    impl DfsPeerTransport for RecordingMountedPeerTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.targets
                .lock()
                .expect("targets")
                .push(target_node_id.clone());
            assert_eq!(call.request.route, sinomo_core::kv::CLUSTER_KV_TXN_ROUTE);
            let request = crate::node_transport::materialize_dfs_value_call_sync(call, usize::MAX)
                .expect("materialize transaction request");
            let submitted = sinomo_core::kv::decode_cluster_kv_txn_rpc_request_body(&request.body)
                .expect("transaction request");
            let payload = sinomo_core::kv::cluster_kv_txn_response_rpc_payload(
                &sinomo_core::kv::ClusterKvTxnCommitResult {
                    request_id: submitted.request_id.clone(),
                    command_digest: submitted
                        .into_command_request()
                        .digest()
                        .expect("transaction digest"),
                    snapshot: sinomo_core::kv::KvSnapshot {
                        revision: KvRevision(7),
                        compacted_revision: KvRevision::ZERO,
                        entries: Vec::new(),
                        leases: Vec::new(),
                        events: Vec::new(),
                        committed_requests: Vec::new(),
                        expired_requests: Vec::new(),
                    },
                    txn_response: Some(KvTxnResponse {
                        succeeded: true,
                        revision: KvRevision(7),
                        responses: Vec::new(),
                    }),
                    authority: sinomo_core::kv::KvAuthorityProof {
                        term: 1,
                        commit_index: 1,
                        revision: KvRevision(7),
                        basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                    replayed_committed_request: false,
                },
            )
            .expect("txn response payload");
            Ok(ObjectRpcResult::value(
                ObjectRpcResponse::from_encoded_body(payload.route, payload.body),
            ))
        }
    }

    impl AsyncDfsPeerTransport for RecordingMountedPeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            let result = self.call_object_rpc(target_node_id, call, options);
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum MountedTxnResponseFault {
        None,
        MismatchedRequestId,
        MismatchedDigest,
        MissingSubmittedDigest,
        InvalidAuthority,
        StaleAuthorityRevision,
        MissingAuthoritativeEntry,
        WrongAuthoritativeEntryKey,
        OutOfPrefixAuthoritativeEntry,
        UnknownExactSetRoute,
        FailedExactSetRoute,
    }

    /// One scripted reply of the local daemon's configuration route.
    #[derive(Clone, Debug)]
    enum MountedConfigurationReply {
        Response(crate::DfsConfigurationSnapshotResponse),
        UnknownRoute,
    }

    #[derive(Debug)]
    struct ScriptedMountedKvTransport {
        exact_receipt: bool,
        local_node_id: NodeId,
        succeeded: bool,
        fault: MountedTxnResponseFault,
        routes: Mutex<Vec<String>>,
        options: Mutex<Vec<DfsObjectRpcSendOptions>>,
        authoritative_entry_calls: AtomicUsize,
        /// Replies in order; the last one repeats.
        configuration: Mutex<std::collections::VecDeque<MountedConfigurationReply>>,
        configuration_calls: AtomicUsize,
    }

    impl ScriptedMountedKvTransport {
        fn new(local_node_id: NodeId, succeeded: bool, fault: MountedTxnResponseFault) -> Self {
            Self {
                exact_receipt: false,
                local_node_id,
                succeeded,
                fault,
                routes: Mutex::new(Vec::new()),
                options: Mutex::new(Vec::new()),
                authoritative_entry_calls: AtomicUsize::new(0),
                configuration: Mutex::new(std::collections::VecDeque::new()),
                configuration_calls: AtomicUsize::new(0),
            }
        }

        fn with_configuration(
            self,
            replies: impl IntoIterator<Item = MountedConfigurationReply>,
        ) -> Self {
            *self.configuration.lock().expect("configuration replies") =
                replies.into_iter().collect();
            self
        }

        fn configuration_reply(&self) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.configuration_calls.fetch_add(1, Ordering::SeqCst);
            let reply = {
                let mut replies = self.configuration.lock().expect("configuration replies");
                if replies.len() > 1 {
                    replies.pop_front()
                } else {
                    replies.front().cloned()
                }
            };
            match reply.expect("scripted configuration reply") {
                MountedConfigurationReply::Response(response) => Ok(ObjectRpcResult::value(
                    ObjectRpcResponse::encode_body(crate::DFS_CONFIGURATION_SNAPSHOT_ROUTE, &response)
                        .expect("encode configuration reply"),
                )),
                MountedConfigurationReply::UnknownRoute => Err(DfsPeerTransportError::from_network(
                    NetworkError::UnknownRoute {
                        route: crate::DFS_CONFIGURATION_SNAPSHOT_ROUTE.to_string(),
                    },
                )),
            }
        }

        fn authority(&self, revision: KvRevision) -> sinomo_core::kv::KvAuthorityProof {
            let (term, voter_acks, required_quorum) =
                if matches!(self.fault, MountedTxnResponseFault::InvalidAuthority) {
                    (0, 0, 1)
                } else {
                    (3, 2, 2)
                };
            sinomo_core::kv::KvAuthorityProof {
                term,
                commit_index: revision.0.saturating_add(10),
                revision,
                basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks,
                    required_quorum,
                },
            }
        }

        fn routes(&self) -> Vec<String> {
            self.routes.lock().expect("routes").clone()
        }

        fn options(&self) -> Vec<DfsObjectRpcSendOptions> {
            self.options.lock().expect("options").clone()
        }
    }

    impl DfsPeerTransport for ScriptedMountedKvTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            assert_eq!(target_node_id, &self.local_node_id);
            if call.request.route == crate::DFS_CONFIGURATION_SNAPSHOT_ROUTE {
                return self.configuration_reply();
            }
            self.options.lock().expect("options").push(options);
            let route = call.request.route.clone();
            self.routes.lock().expect("routes").push(route.clone());
            let request = crate::node_transport::materialize_dfs_value_call_sync(
                call,
                sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES
                    .saturating_add(1024 * 1024),
            )
            .expect("materialize mounted KV request");
            let mut payload = match route.as_str() {
                sinomo_core::kv::CLUSTER_KV_TXN_ROUTE => {
                    let submitted =
                        sinomo_core::kv::decode_cluster_kv_txn_rpc_request_body(&request.body)
                            .expect("decode mounted transaction");
                    let submitted_digest = submitted
                        .clone()
                        .into_command_request()
                        .digest()
                        .expect("mounted transaction digest");
                    let request_id =
                        if matches!(self.fault, MountedTxnResponseFault::MismatchedRequestId) {
                            sinomo_core::kv::KvRequestId::generate()
                        } else {
                            submitted.request_id
                        };
                    let command_digest =
                        if matches!(self.fault, MountedTxnResponseFault::MismatchedDigest) {
                            sinomo_core::kv::KvCommandDigest([0xA5; 32])
                        } else {
                            submitted_digest
                        };
                    let result_revision = KvRevision(9);
                    let authority_revision =
                        if matches!(self.fault, MountedTxnResponseFault::StaleAuthorityRevision) {
                            KvRevision(8)
                        } else {
                            result_revision
                        };
                    sinomo_core::kv::cluster_kv_txn_response_rpc_payload(
                        &sinomo_core::kv::ClusterKvTxnCommitResult {
                            request_id,
                            command_digest,
                            snapshot: empty_mounted_sifs_kv_snapshot(result_revision),
                            txn_response: Some(KvTxnResponse {
                                succeeded: self.succeeded,
                                revision: result_revision,
                                responses: Vec::new(),
                            }),
                            authority: sinomo_core::kv::KvAuthorityProof {
                                basis: if self.exact_receipt {
                                    sinomo_core::kv::KvAuthorityBasis::ExactCommittedRequest
                                } else {
                                    self.authority(authority_revision).basis
                                },
                                ..self.authority(authority_revision)
                            },
                            replayed_committed_request: self.exact_receipt,
                        },
                    )
                    .expect("encode mounted transaction response")
                }
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE => {
                    let request =
                        sinomo_core::kv::decode_cluster_kv_authoritative_entry_rpc_request_body(
                            &request.body,
                        )
                        .expect("decode mounted exact-entry read");
                    self.authoritative_entry_calls
                        .fetch_add(1, Ordering::SeqCst);
                    let read_revision = KvRevision(9);
                    let response_key = if matches!(
                        self.fault,
                        MountedTxnResponseFault::WrongAuthoritativeEntryKey
                    ) {
                        "/dfs/foreign/entry".to_string()
                    } else {
                        request.key
                    };
                    sinomo_core::kv::cluster_kv_authoritative_entry_response_rpc_payload(
                        &sinomo_core::kv::KvAuthoritativeRead {
                            value: if matches!(
                                self.fault,
                                MountedTxnResponseFault::MissingAuthoritativeEntry
                            ) {
                                None
                            } else {
                                Some(KvEntry {
                                    key: response_key,
                                    value: b"authoritative".to_vec(),
                                    create_revision: read_revision,
                                    modify_revision: read_revision,
                                    version: sinomo_core::kv::KvVersion(1),
                                    lease_id: None,
                                })
                            },
                            authority: self.authority(read_revision),
                        },
                    )
                    .expect("encode mounted exact-entry response")
                }
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE => {
                    let request =
                        sinomo_core::kv::decode_cluster_kv_authoritative_entries_rpc_request_body(
                            &request.body,
                        )
                        .expect("decode mounted exact-key-set read");
                    if matches!(self.fault, MountedTxnResponseFault::UnknownExactSetRoute) {
                        return Err(DfsPeerTransportError::from_network(
                            sinomo_core::network::NetworkError::UnknownRoute {
                                route: route.clone(),
                            },
                        ));
                    }
                    if matches!(self.fault, MountedTxnResponseFault::FailedExactSetRoute) {
                        return Err(DfsPeerTransportError::new(
                            "injected exact-key-set transport failure",
                        ));
                    }
                    sinomo_core::kv::cluster_kv_authoritative_entries_response_rpc_payload(
                        &request.keys,
                        &sinomo_core::kv::KvAuthoritativeRead {
                            value: request
                                .keys
                                .iter()
                                .map(|key| {
                                    Some(KvEntry {
                                        key: key.clone(),
                                        value: format!("authoritative:{key}").into_bytes(),
                                        create_revision: KvRevision(9),
                                        modify_revision: KvRevision(9),
                                        version: sinomo_core::kv::KvVersion(1),
                                        lease_id: None,
                                    })
                                })
                                .collect(),
                            authority: self.authority(KvRevision(9)),
                        },
                    )
                    .expect("encode mounted exact-key-set response")
                }

                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE => {
                    let request = sinomo_core::kv::decode_cluster_kv_authoritative_range_prefix_rpc_request_body(
                        &request.body,
                    )
                    .expect("decode mounted exact-prefix read");
                    sinomo_core::kv::cluster_kv_authoritative_range_prefix_response_rpc_payload(
                        &sinomo_core::kv::KvAuthoritativeRead {
                            value: if matches!(
                                self.fault,
                                MountedTxnResponseFault::OutOfPrefixAuthoritativeEntry
                            ) {
                                vec![KvEntry {
                                    key: format!(
                                        "{}-sibling",
                                        request.prefix.trim_end_matches('/')
                                    ),
                                    value: b"foreign".to_vec(),
                                    create_revision: KvRevision(9),
                                    modify_revision: KvRevision(9),
                                    version: sinomo_core::kv::KvVersion(1),
                                    lease_id: None,
                                }]
                            } else {
                                Vec::new()
                            },
                            authority: self.authority(KvRevision(9)),
                        },
                    )
                    .expect("encode mounted exact-prefix response")
                }
                other => panic!("unexpected mounted KV route {other}"),
            };
            if route == sinomo_core::kv::CLUSTER_KV_TXN_ROUTE
                && matches!(self.fault, MountedTxnResponseFault::MissingSubmittedDigest)
            {
                let mut response = sinomo_core::kv::decode_cluster_kv_txn_rpc_body(&payload.body)
                    .expect("decode legacy fixture response");
                response.submitted_command_digest = None;
                payload.body = ObjectRpcResponse::encode_body(route.clone(), &response)
                    .expect("encode legacy fixture response")
                    .body;
            }
            assert!(
                payload.body.len()
                    <= sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES
                        .saturating_add(1024 * 1024),
                "mounted control-plane fixture must remain bounded"
            );
            Ok(ObjectRpcResult::value(
                ObjectRpcResponse::from_encoded_body(payload.route, payload.body),
            ))
        }
    }

    impl AsyncDfsPeerTransport for ScriptedMountedKvTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            let result = self.call_object_rpc(target_node_id, call, options);
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct TransitioningMountedKvTransport {
        local_node_id: NodeId,
        shard_key: String,
        handoff_key: String,
        current_shard_value: Vec<u8>,
        calls: AtomicUsize,
        requested_keys: Mutex<Vec<String>>,
    }

    impl TransitioningMountedKvTransport {
        fn authority(revision: KvRevision) -> sinomo_core::kv::KvAuthorityProof {
            sinomo_core::kv::KvAuthorityProof {
                term: 5,
                commit_index: revision.0.saturating_add(10),
                revision,
                basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            }
        }
    }

    impl DfsPeerTransport for TransitioningMountedKvTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            assert_eq!(target_node_id, &self.local_node_id);
            assert_eq!(
                call.request.route,
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE
            );
            let request = crate::node_transport::materialize_dfs_value_call_sync(
                call,
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES
                    .saturating_add(1024 * 1024),
            )
            .expect("materialize transition authority request");
            let request =
                sinomo_core::kv::decode_cluster_kv_authoritative_entries_rpc_request_body(
                    &request.body,
                )
                .expect("decode transition authority request");
            self.requested_keys
                .lock()
                .expect("transition requested keys")
                .extend(request.keys.iter().cloned());
            assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
            let revision = KvRevision(51);
            let values = request
                .keys
                .iter()
                .map(|key| match key.as_str() {
                    key if key == self.shard_key => Some(KvEntry {
                        key: key.to_string(),
                        value: self.current_shard_value.clone(),
                        create_revision: revision,
                        modify_revision: revision,
                        version: sinomo_core::kv::KvVersion(1),
                        lease_id: None,
                    }),
                    key if key == self.handoff_key => None,
                    key => panic!("unexpected transition exact key {key}"),
                })
                .collect();
            let payload = sinomo_core::kv::cluster_kv_authoritative_entries_response_rpc_payload(
                &request.keys,
                &sinomo_core::kv::KvAuthoritativeRead {
                    value: values,
                    authority: Self::authority(revision),
                },
            )
            .expect("encode transition authority response");
            Ok(ObjectRpcResult::value(
                ObjectRpcResponse::from_encoded_body(payload.route, payload.body),
            ))
        }
    }

    fn mounted_test_cluster(name: &str) -> (sinomo_testdir::TestDir, ClusterPaths, NodeId) {
        let root = sinomo_testdir::TestDir::new(&format!("sinomo-mounted-sifs-{name}"));
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node = NodeId::from_legacy_named(format!("node_mounted_sifs_{name}"))
            .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(format!("cluster_mounted_sifs_{name}"))
                .expect("cluster id"),
            "mounted SIFS test cluster",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        (root, cluster_paths, local_node)
    }

    /// A client whose feed has no subscription: every read is ReadIndex.
    fn detached_client(
        cluster_paths: ClusterPaths,
        transport: Arc<dyn DfsPeerTransport>,
    ) -> MountedSifsControlPlaneKvClient {
        let shared = Arc::new(MountedSifsConfigurationShared::new());
        shared.set(MountedSifsConfiguration::Disconnected);
        let (stop, _) = tokio::sync::watch::channel(false);
        MountedSifsControlPlaneKvClient::with_configuration(
            cluster_paths,
            transport,
            Arc::new(MountedSifsConfigurationFeed { shared, stop }),
        )
    }

    /// A client whose feed subscribes to `sessions` scripted streams in order.
    fn scripted_client(
        cluster_paths: ClusterPaths,
        transport: Arc<ScriptedMountedKvTransport>,
        sessions: usize,
    ) -> (
        MountedSifsControlPlaneKvClient,
        Vec<tokio::sync::mpsc::UnboundedSender<MountedSifsConfigurationItem>>,
    ) {
        let (senders, receivers): (Vec<_>, std::collections::VecDeque<_>) = (0..sessions)
            .map(|_| tokio::sync::mpsc::unbounded_channel())
            .unzip();
        let feed = MountedSifsConfigurationFeed::subscribe(
            MountedSifsConfigurationEvents::Scripted(Arc::new(Mutex::new(receivers))),
            MountedSifsConfigurationFetch {
                cluster_paths: cluster_paths.clone(),
                transport: transport.clone(),
            },
        );
        (
            MountedSifsControlPlaneKvClient::with_configuration(cluster_paths, transport, feed),
            senders,
        )
    }

    const MOUNTED_TEST_CONFIGURATION_KEY: &str = "/dfs/filesystems/mounted-test/config";

    fn mounted_test_publication(generation: u64, revision: u64) -> DfsConfigurationPublication {
        DfsConfigurationPublication {
            generation,
            projection_generation: generation,
            storage_generation: 1,
            revision: Some(KvRevision(revision)),
        }
    }

    fn mounted_test_published(
        generation: u64,
        revision: u64,
        value: &str,
    ) -> MountedConfigurationReply {
        MountedConfigurationReply::Response(crate::DfsConfigurationSnapshotResponse::Published {
            snapshot: sinomo_api::dfs::DfsConfigurationSnapshotWire {
                publication: mounted_test_publication(generation, revision),
                filesystem_entries: vec![KvEntry {
                    key: MOUNTED_TEST_CONFIGURATION_KEY.to_string(),
                    value: value.as_bytes().to_vec(),
                    create_revision: KvRevision(revision),
                    modify_revision: KvRevision(revision),
                    version: sinomo_core::kv::KvVersion(1),
                    lease_id: None,
                }],
                write: None,
            },
        })
    }

    fn mounted_test_value(client: &MountedSifsControlPlaneKvClient) -> Vec<u8> {
        client
            .get(MOUNTED_TEST_CONFIGURATION_KEY)
            .expect("published-scope read")
            .expect("published-scope entry")
            .value
    }

    #[test]
    fn mounted_configuration_feed_serves_published_scopes_and_fetches_only_on_announcements() {
        let (_root, cluster_paths, local_node) = mounted_test_cluster("configuration-feed");
        let transport = Arc::new(
            ScriptedMountedKvTransport::new(local_node, true, MountedTxnResponseFault::None)
                .with_configuration([
                    MountedConfigurationReply::Response(
                        crate::DfsConfigurationSnapshotResponse::Pending {
                            publication: DfsConfigurationPublication {
                                revision: None,
                                ..mounted_test_publication(1, 0)
                            },
                        },
                    ),
                    mounted_test_published(2, 8, "first"),
                    mounted_test_published(3, 10, "second"),
                ]),
        );
        let (client, sessions) = scripted_client(cluster_paths, transport.clone(), 1);
        let shared = Arc::clone(&client.configuration.shared);

        // Ready: one fetch. A pending daemon leaves every read on ReadIndex.
        sessions[0].send(MountedSifsConfigurationItem::Ready).unwrap();
        assert!(matches!(
            shared.wait_for_transitions(1),
            MountedSifsConfiguration::Pending(_)
        ));
        assert!(client.configuration_snapshot().unwrap().is_none());
        assert_eq!(mounted_test_value(&client), b"authoritative");
        assert_eq!(transport.authoritative_entry_calls.load(Ordering::SeqCst), 1);

        // The announced publication is fetched once and then answers its
        // scopes without a KV request.
        sessions[0]
            .send(MountedSifsConfigurationItem::Published(mounted_test_publication(2, 8)))
            .unwrap();
        assert!(matches!(
            shared.wait_for_transitions(2),
            MountedSifsConfiguration::Published(_)
        ));
        let routes = transport.routes().len();
        assert_eq!(mounted_test_value(&client), b"first");
        assert_eq!(client.range_prefix("/dfs/filesystems/").unwrap().len(), 1);
        assert_eq!(
            client
                .configuration_snapshot()
                .unwrap()
                .expect("held snapshot")
                .generation(),
            2
        );
        assert_eq!(transport.routes().len(), routes);

        // An announcement of the held publication is not fetched; a newer one is.
        sessions[0]
            .send(MountedSifsConfigurationItem::Published(mounted_test_publication(2, 8)))
            .unwrap();
        sessions[0]
            .send(MountedSifsConfigurationItem::Published(mounted_test_publication(3, 10)))
            .unwrap();
        shared.wait_for_transitions(3);
        assert_eq!(mounted_test_value(&client), b"second");
        assert_eq!(transport.configuration_calls.load(Ordering::SeqCst), 3);

        // A lagged subscriber may have missed announcements: fetch once.
        sessions[0].send(MountedSifsConfigurationItem::Lagged).unwrap();
        shared.wait_for_transitions(4);
        assert_eq!(transport.configuration_calls.load(Ordering::SeqCst), 4);

        // Keys outside the published scopes cross ReadIndex on every read.
        for _ in 0..2 {
            client.get("/config/outside-published-scopes").unwrap();
        }
        assert_eq!(transport.authoritative_entry_calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn mounted_configuration_feed_withdraws_on_stream_loss_and_refetches_after_resubscribing() {
        let (_root, cluster_paths, local_node) = mounted_test_cluster("configuration-reconnect");
        let transport = Arc::new(
            ScriptedMountedKvTransport::new(local_node, true, MountedTxnResponseFault::None)
                .with_configuration([mounted_test_published(2, 8, "held")]),
        );
        let (client, mut sessions) = scripted_client(cluster_paths, transport.clone(), 2);
        let shared = Arc::clone(&client.configuration.shared);
        sessions[0].send(MountedSifsConfigurationItem::Ready).unwrap();
        shared.wait_for_transitions(1);
        assert_eq!(mounted_test_value(&client), b"held");
        assert_eq!(transport.authoritative_entry_calls.load(Ordering::SeqCst), 0);

        // Losing the stream withdraws the held snapshot at once.
        drop(sessions.remove(0));
        assert!(matches!(
            shared.wait_for_transitions(2),
            MountedSifsConfiguration::Disconnected
        ));
        assert_eq!(mounted_test_value(&client), b"authoritative");
        assert_eq!(transport.authoritative_entry_calls.load(Ordering::SeqCst), 1);

        // The next subscription fetches once before it is used again.
        sessions[0].send(MountedSifsConfigurationItem::Ready).unwrap();
        assert!(matches!(
            shared.wait_for_transitions(3),
            MountedSifsConfiguration::Published(_)
        ));
        assert_eq!(transport.configuration_calls.load(Ordering::SeqCst), 2);
        assert_eq!(mounted_test_value(&client), b"held");
        assert_eq!(transport.authoritative_entry_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mounted_configuration_feed_reports_an_older_daemon_as_a_typed_error() {
        let (_root, cluster_paths, local_node) = mounted_test_cluster("configuration-unsupported");
        let transport = Arc::new(
            ScriptedMountedKvTransport::new(local_node, true, MountedTxnResponseFault::None)
                .with_configuration([
                    MountedConfigurationReply::UnknownRoute,
                    mounted_test_published(2, 8, "upgraded"),
                ]),
        );
        let (client, sessions) = scripted_client(cluster_paths, transport.clone(), 1);
        let shared = Arc::clone(&client.configuration.shared);
        sessions[0].send(MountedSifsConfigurationItem::Ready).unwrap();
        assert!(matches!(
            shared.wait_for_transitions(1),
            MountedSifsConfiguration::Unsupported
        ));
        let error = client
            .get(MOUNTED_TEST_CONFIGURATION_KEY)
            .expect_err("an older daemon cannot answer a published scope");
        assert!(crate::is_dfs_configuration_snapshot_unsupported(&error));
        assert!(
            crate::is_dfs_configuration_snapshot_unsupported(
                &client.configuration_snapshot().expect_err("typed error")
            )
        );
        assert_eq!(
            transport.authoritative_entry_calls.load(Ordering::SeqCst),
            0,
            "no fallback configuration read replaces the missing route"
        );
        client.get("/config/outside-published-scopes").unwrap();
        assert_eq!(transport.configuration_calls.load(Ordering::SeqCst), 1);

        // An upgraded daemon announces its publication; that is fetched.
        sessions[0]
            .send(MountedSifsConfigurationItem::Published(mounted_test_publication(2, 8)))
            .unwrap();
        shared.wait_for_transitions(2);
        assert_eq!(mounted_test_value(&client), b"upgraded");
    }

    #[test]
    fn mounted_published_scope_write_reads_its_own_commit_until_a_newer_publication() {
        let (_root, cluster_paths, local_node) = mounted_test_cluster("configuration-own-write");
        let transport = Arc::new(
            ScriptedMountedKvTransport::new(local_node, true, MountedTxnResponseFault::None)
                .with_configuration([
                    mounted_test_published(2, 8, "before"),
                    mounted_test_published(3, 9, "committed"),
                ]),
        );
        let (mut client, sessions) = scripted_client(cluster_paths, transport.clone(), 1);
        let shared = Arc::clone(&client.configuration.shared);
        sessions[0].send(MountedSifsConfigurationItem::Ready).unwrap();
        shared.wait_for_transitions(1);
        assert_eq!(mounted_test_value(&client), b"before");

        client
            .put(MOUNTED_TEST_CONFIGURATION_KEY.to_string(), b"committed".to_vec())
            .expect("commit inside a published scope");
        assert_eq!(
            mounted_test_value(&client),
            b"authoritative",
            "a held snapshot older than this client's own commit does not answer it"
        );
        assert!(client.configuration_snapshot().unwrap().is_none());
        assert_eq!(transport.authoritative_entry_calls.load(Ordering::SeqCst), 1);

        sessions[0]
            .send(MountedSifsConfigurationItem::Published(mounted_test_publication(3, 9)))
            .unwrap();
        shared.wait_for_transitions(2);
        assert_eq!(mounted_test_value(&client), b"committed");
        assert_eq!(transport.authoritative_entry_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mounted_client_follows_component_configuration_announcements() {
        let name = "native-mounted-configuration-announcements";
        let (runtime, nodes) = crate::test_support::DfsTestRuntime::start_cluster(name, 1).unwrap();
        let local_node = nodes[0].clone();
        let data_dir = runtime.data_dir(&local_node).clone();
        let (bus, _) = mounted_sifs_test_event_bus(&data_dir).expect("fixture component bus");
        let mut announcements = bus.subscribe_host();
        let (sync, asynchronous) = runtime.peer_transports(&local_node).unwrap();
        let control = MountedSifsControlPlaneKvClient::new_authoritative(&data_dir, sync, asynchronous);
        let before = control
            .configuration_snapshot()
            .unwrap()
            .expect("the route serves the floor-checked publication");

        let added = runtime
            .bootstrap_empty_metadata_prefix("announced", "/announced")
            .unwrap();
        let shard_key = crate::metadata_shard_key(&added.file_system_id, &added.shard_id);

        // The component announces the change on its event bus...
        let announced = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
                    loop {
                        let event = announcements.recv().await.expect("component bus");
                        if event.envelope.kind.as_str()
                            != crate::DFS_CONFIGURATION_PUBLISHED_EVENT_KIND
                        {
                            continue;
                        }
                        let publication: DfsConfigurationPublication =
                            serde_json::from_value(event.envelope.payload.clone()).unwrap();
                        if publication.advances_past(&before.publication()) {
                            return event;
                        }
                    }
                })
                .await
                .expect("configuration announcement")
            });
        assert_eq!(
            announced.envelope.routing.scope,
            sinomo_core::RoutingScope::Local
        );
        assert_eq!(announced.schema_id, crate::DFS_CONFIGURATION_EVENT_SCHEMA_ID);
        assert!(
            !announced.envelope.links.supersedes.is_empty(),
            "each announcement supersedes the previous one"
        );

        // ...and the mounted client fetches it and answers from it.
        control.configuration.shared.wait_until(|state| {
            matches!(state, MountedSifsConfiguration::Published(snapshot)
                if snapshot.entry(&shard_key).is_some_and(|entry| entry.is_some()))
        });
        let shards_prefix = format!(
            "{}/",
            crate::control_plane::metadata_shards_prefix(&added.file_system_id)
        );
        assert!(
            control
                .range_prefix(&shards_prefix)
                .unwrap()
                .iter()
                .any(|entry| entry.key == shard_key)
        );
        drop(control);
        runtime.close().unwrap();
    }

    #[test]
    fn content_replica_candidates_exclude_client_only_nodes() {
        let client = NodeId::from_legacy_named("node_mounted_sifs_client").expect("client");
        let first_server =
            NodeId::from_legacy_named("node_mounted_sifs_content_a").expect("first server");
        let second_server =
            NodeId::from_legacy_named("node_mounted_sifs_content_b").expect("second server");
        let mut client_advertisement = DfsNodeAdvertisement::metadata(client);
        client_advertisement.accepts_content = false;

        assert_eq!(
            mounted_sifs_content_replica_candidates([
                client_advertisement,
                DfsNodeAdvertisement::content(first_server.clone()),
                DfsNodeAdvertisement::content(second_server.clone()),
            ]),
            vec![first_server, second_server]
        );
    }

    #[derive(Debug)]
    struct RecordingReplicaDiscoveryControlPlane {
        content_entries: Vec<KvEntry>,
        service_entries: Vec<KvEntry>,
        prefixes: Mutex<Vec<String>>,
    }

    impl DfsControlPlaneKvClient for RecordingReplicaDiscoveryControlPlane {
        fn get(&self, _key: &str) -> Result<Option<KvEntry>, KvError> {
            Ok(None)
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.prefixes
                .lock()
                .expect("replica discovery prefixes")
                .push(prefix.to_string());
            if prefix == content_services_prefix() {
                Ok(self.content_entries.clone())
            } else if prefix == sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX {
                Ok(self.service_entries.clone())
            } else {
                Ok(Vec::new())
            }
        }

        fn put(&mut self, _key: String, _value: Vec<u8>) -> Result<KvRevision, KvError> {
            panic!("replica discovery must not write the control plane")
        }

        fn txn(&mut self, _txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            panic!("replica discovery must not transact against the control plane")
        }
    }

    #[test]
    fn replica_discovery_uses_the_supplied_control_plane() {
        let client = NodeId::from_legacy_named("node_replica_discovery_client").expect("client");
        let server = NodeId::from_legacy_named("node_replica_discovery_server").expect("server");
        let mut client_advertisement = DfsNodeAdvertisement::metadata(client.clone());
        client_advertisement.accepts_content = false;
        let revision = KvRevision(7);
        let entry = |advertisement: &DfsNodeAdvertisement| KvEntry {
            key: crate::content_service_key(&advertisement.node_id),
            value: sinomo_core::kv::encode_kv_value(advertisement)
                .expect("encode DFS advertisement"),
            create_revision: revision,
            modify_revision: revision,
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        };
        let control_plane = RecordingReplicaDiscoveryControlPlane {
            content_entries: vec![
                entry(&client_advertisement),
                entry(&DfsNodeAdvertisement::content(server.clone())),
            ],
            service_entries: Vec::new(),
            prefixes: Mutex::new(Vec::new()),
        };

        assert_eq!(
            discover_mounted_sifs_content_replica_candidates_with_control_plane(&control_plane)
                .expect("discover content replicas"),
            vec![server]
        );
        assert_eq!(
            control_plane
                .prefixes
                .lock()
                .expect("replica discovery prefixes")
                .as_slice(),
            [
                content_services_prefix(),
                sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX,
            ]
        );
    }

    #[test]
    fn replica_discovery_excludes_stale_content_advertisements_for_disabled_dfs_nodes() {
        let active = NodeId::from_legacy_named("node_replica_discovery_active").expect("active");
        let disabled =
            NodeId::from_legacy_named("node_replica_discovery_disabled").expect("disabled");
        let revision = KvRevision(9);
        let advertisement_entry = |node_id: &NodeId| KvEntry {
            key: crate::content_service_key(node_id),
            value: sinomo_core::kv::encode_kv_value(&DfsNodeAdvertisement::content(
                node_id.clone(),
            ))
            .expect("encode DFS advertisement"),
            create_revision: revision,
            modify_revision: revision,
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        };
        let mut service_record =
            sinomo_core::service::ServiceInstanceRecord::new("dfs", active.clone());
        service_record.routes = vec![crate::DFS_STATUS_ROUTE.to_string()];
        service_record.provider_capabilities = vec!["dfs.object.transfer".to_string()];
        service_record.updated_at = time::OffsetDateTime::now_utc();
        let service_entry = KvEntry {
            key: sinomo_core::service::cluster_service_instance_key("dfs", &active),
            value: sinomo_core::kv::encode_kv_value(&service_record).expect("encode service"),
            create_revision: revision,
            modify_revision: revision,
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        };
        let control_plane = RecordingReplicaDiscoveryControlPlane {
            content_entries: vec![advertisement_entry(&active), advertisement_entry(&disabled)],
            service_entries: vec![service_entry],
            prefixes: Mutex::new(Vec::new()),
        };

        assert_eq!(
            discover_mounted_sifs_content_replica_candidates_with_control_plane(&control_plane)
                .expect("discover active content replicas"),
            vec![active]
        );
    }

    #[test]
    fn out_of_scope_reads_cross_authority_and_never_trust_the_materialized_projection() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-critical-cache-fence");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_critical_cache_fence").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_critical_cache_fence",
            )
            .expect("cluster id"),
            "mounted SIFS critical cache fence",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let key = "/config/mounted-sifs-critical";
        let mut stale = empty_mounted_sifs_kv_snapshot(KvRevision(1));
        stale.entries.push(KvEntry {
            key: key.to_string(),
            value: b"stale".to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: sinomo_core::kv::KvVersion(1),
            lease_id: None,
        });
        stale.canonicalize();
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &stale)
            .expect("write stale materialized projection");

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::None,
        ));
        let client = detached_client(cluster_paths, transport.clone());

        for _ in 0..2 {
            assert_eq!(
                client
                    .get(key)
                    .expect("critical authoritative read")
                    .expect("authoritative entry")
                    .value,
                b"authoritative",
            );
        }
        assert_eq!(
            transport.authoritative_entry_calls.load(Ordering::SeqCst),
            2,
            "a key outside the published scopes crosses authority on every read"
        );
    }

    #[test]
    fn mounted_control_plane_commits_through_the_supplied_peer_transport() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-control-plane-peer");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_control_plane").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_mounted_sifs_control_plane")
                .expect("cluster id"),
            "mounted SIFS control plane",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let peer = Arc::new(RecordingMountedPeerTransport {
            targets: std::sync::Mutex::new(Vec::new()),
        });
        let mut client = detached_client(cluster_paths, peer.clone());

        let response = client
            .txn(KvTxn {
                compares: Vec::new(),
                success: Vec::new(),
                failure: Vec::new(),
            })
            .expect("txn");

        assert_eq!(response.revision, KvRevision(7));
        assert_eq!(
            peer.targets.lock().expect("targets").as_slice(),
            &[local_node]
        );
    }

    #[test]
    fn mounted_control_plane_txn_fences_only_the_executed_branch() {
        let (_root, cluster_paths, local_node) = mounted_test_cluster("txn-branch");
        for succeeded in [true, false] {
            let transport = Arc::new(ScriptedMountedKvTransport::new(
                local_node.clone(),
                succeeded,
                MountedTxnResponseFault::None,
            ));
            let mut client = detached_client(cluster_paths.clone(), transport.clone());
            let response = client
                .txn(KvTxn {
                    compares: vec![sinomo_core::kv::KvCompare::Missing {
                        key: "/dfs/test/compare".to_string(),
                    }],
                    success: vec![
                        KvOp::Put {
                            key: "/dfs/filesystems/mounted-test/success".to_string(),
                            value: b"success".to_vec(),
                            lease_id: None,
                        },
                        KvOp::DeletePrefix {
                            prefix: "/dfs/test/deleted/".to_string(),
                        },
                    ],
                    failure: vec![KvOp::Put {
                        key: "/dfs/test/failure".to_string(),
                        value: b"failure".to_vec(),
                        lease_id: None,
                    }],
                })
                .expect("mounted transaction");

            assert_eq!(response.succeeded, succeeded);
            assert_eq!(response.revision, KvRevision(9));
            assert_eq!(
                transport.routes(),
                vec![sinomo_core::kv::CLUSTER_KV_TXN_ROUTE.to_string()],
                "a commit is not followed by a revalidation round"
            );
            assert_eq!(
                client.scope_write_revision.load(AtomicOrdering::Acquire),
                if succeeded { 9 } else { 0 },
                "only the executed branch's published-scope write fences the held snapshot"
            );
            assert_eq!(client.observed_revision.load(AtomicOrdering::Acquire), 9);
        }
    }

    #[test]
    fn repeated_mounted_path_reads_use_the_published_shard_catalog() {
        struct ObservedPeer {
            inner: Arc<dyn DfsPeerTransport>,
            prefixes: Mutex<Vec<String>>,
        }
        impl DfsPeerTransport for ObservedPeer {
            fn response_delivery_owner(&self) -> Option<Arc<crate::DfsRoutedResponseDelivery>> {
                self.inner.response_delivery_owner()
            }
            fn call_object_rpc(
                &self,
                target: &NodeId,
                call: ObjectRpcCall,
                options: DfsObjectRpcSendOptions,
            ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
                if call.request.route
                    == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
                {
                    let request =
                        sinomo_core::kv::decode_cluster_kv_authoritative_range_prefix_rpc_request_body(
                            &call.request.body,
                        )
                        .expect("decode prefix request");
                    self.prefixes.lock().unwrap().push(request.prefix);
                }
                // Same actual owned native transport/request/result, no
                // manufactured floor, response or route-specific behavior.
                self.inner.call_object_rpc(target, call, options)
            }
        }
        let name = "native-mounted-path-catalog-certificate";
        let (mut runtime, nodes) =
            crate::test_support::DfsTestRuntime::start_cluster(name, 1).unwrap();
        let local_node = nodes[0].clone();
        let path = "/services/ordinary-read.txt";
        let mut writer = runtime.client(&local_node).unwrap();
        writer
            .posix_write_file(
                path,
                b"ordinary-read",
                crate::DfsWriteOptions::replicated_eventual(vec![local_node.clone()]),
            )
            .unwrap();
        drop(writer);
        let data_dir = runtime.data_dir(&local_node).clone();
        let (sync, asynchronous) = runtime.peer_transports(&local_node).unwrap();
        let observed = Arc::new(ObservedPeer {
            inner: sync,
            prefixes: Mutex::new(Vec::new()),
        });
        let control = MountedSifsControlPlaneKvClient::new_authoritative(
            &data_dir,
            observed.clone(),
            asynchronous.clone(),
        );
        let mut opened = open_mounted_sifs_service_with_peer_transports(
            data_dir,
            observed.clone(),
            asynchronous,
            Some(Box::new(control.clone())),
        )
        .unwrap();
        assert_eq!(
            opened
                .service
                .posix_read_file_range_distributed(path, 0, 8)
                .unwrap(),
            b"ordinary"
        );
        let prefix = format!(
            "{}/",
            crate::control_plane::metadata_shards_prefix(&crate::sifs_file_system_id())
        );
        let published = control
            .configuration_snapshot()
            .expect("published configuration")
            .expect("the mount holds the component's published snapshot");
        assert!(
            published.entries(&prefix).is_some(),
            "the published snapshot covers the metadata-shard catalog"
        );
        for _ in 0..8 {
            assert_eq!(
                opened
                    .service
                    .posix_read_file_range_distributed(path, 0, 8)
                    .expect("ordinary mounted path read"),
                b"ordinary"
            );
        }
        assert!(
            !observed
                .prefixes
                .lock()
                .unwrap()
                .iter()
                .any(|requested| requested.starts_with(&prefix)),
            "ordinary reads route from the published catalog instead of a ReadIndex per read"
        );
        drop(opened);
        drop(control);
        drop(observed);
        runtime.close().unwrap();
    }

    #[test]
    fn metadata_shard_authority_never_accepts_a_mixed_revision_transition() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-shard-transition");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_shard_transition").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_shard_transition",
            )
            .expect("cluster id"),
            "mounted SIFS shard transition",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let expected = crate::DfsMetadataShardConfig {
            file_system_id: sinomo_core::Id::from_legacy_named(
                "filesystem_mounted_sifs_shard_transition",
            )
            .expect("filesystem id"),
            shard_id: "root".to_string(),
            primary_node_id: local_node.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: crate::DfsMetadataShardAssignment::Root,

            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut current = expected.clone();
        current.epoch = 2;
        let shard_key = crate::metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let handoff_key = crate::metadata_shard_membership_handoff_key(
            &expected.file_system_id,
            &expected.shard_id,
        );
        let transport = Arc::new(TransitioningMountedKvTransport {
            local_node_id: local_node,
            shard_key: shard_key.clone(),
            handoff_key: handoff_key.clone(),
            current_shard_value: sinomo_core::kv::encode_kv_value(&current)
                .expect("encode current shard"),
            calls: AtomicUsize::new(0),
            requested_keys: Mutex::new(Vec::new()),
        });
        let mut client = detached_client(cluster_paths, transport.clone());

        assert!(
            !crate::DfsControlPlaneStore::new(&mut client)
                .metadata_shard_authority_is_current(&expected)
                .expect("revision-pinned shard authority check"),
            "the retired r50 assignment must not be combined with r51 handoff absence"
        );
        assert_eq!(
            transport
                .requested_keys
                .lock()
                .expect("transition requested keys")
                .as_slice(),
            &[shard_key.clone(), handoff_key.clone(),],
            "one exact-key-set request must observe the transition coherently"
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert_eq!(client.observed_revision.load(AtomicOrdering::Acquire), 51);
    }

    #[test]
    fn mounted_exact_set_requires_the_current_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-legacy-exact-set");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_legacy_exact_set").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_legacy_exact_set",
            )
            .expect("cluster id"),
            "mounted SIFS legacy exact-set fallback",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let keys = vec![
            "/dfs/test/legacy-a".to_string(),
            "/dfs/test/legacy-b".to_string(),
        ];

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::UnknownExactSetRoute,
        ));
        let client = detached_client(cluster_paths.clone(), transport.clone());
        client
            .get_consistent(&keys)
            .expect_err("an incompatible daemon must be upgraded");
        assert_eq!(
            transport.routes(),
            vec![sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string()],
            "missing current authority route must fail closed"
        );

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::FailedExactSetRoute,
        ));
        let client = detached_client(cluster_paths, transport.clone());
        let error = client
            .get_consistent(&keys)
            .expect_err("a transport failure must fail closed");
        assert!(
            error
                .to_string()
                .contains("exact-key-set transport failure")
        );
        assert_eq!(
            transport.routes(),
            vec![sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string()],
            "transport failure must not request a full snapshot"
        );
    }

    #[test]
    fn the_first_configuration_publication_wait_observes_the_mount_deadline() {
        let shared = MountedSifsConfigurationShared::new();
        let start = Instant::now();
        let _call = MountedSifsCall::enter(Some(start + Duration::from_millis(25)));
        assert!(matches!(
            shared.current(),
            MountedSifsConfiguration::Disconnected
        ));
        assert!(start.elapsed() < sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    }

    #[test]
    fn mounted_authority_absolute_deadline_bounds_dispatch() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-authority-deadline");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_authority_deadline").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_authority_deadline",
            )
            .expect("cluster id"),
            "mounted SIFS authority deadline",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::None,
        ));
        let mut client = detached_client(cluster_paths, transport.clone());
        let keys = vec!["/dfs/deadline/key".to_string()];

        let error = client
            .get_authoritative_consistent_before(
                &keys,
                Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .expect("past instant"),
            )
            .expect_err("expired deadline must reject before dispatch");
        assert!(error.to_string().contains("deadline elapsed"));
        assert!(transport.routes().is_empty());

        let budget = Duration::from_secs(2);
        client
            .get_authoritative_consistent_before(&keys, Instant::now() + budget)
            .expect("deadline-bounded current exact read");
        assert_eq!(
            transport.routes(),
            vec![sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string(),]
        );
        let options = transport.options();
        assert_eq!(options.len(), 1);
        assert!(options.iter().all(|options| {
            options
                .overall_timeout
                .is_some_and(|remaining| !remaining.is_zero() && remaining <= budget)
        }));
    }

    #[test]
    fn mounted_authority_rejects_entries_outside_the_requested_scope() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-authority-containment");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node = NodeId::from_legacy_named("node_mounted_sifs_authority_containment")
            .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_authority_containment",
            )
            .expect("cluster id"),
            "mounted SIFS authority reply containment",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::WrongAuthoritativeEntryKey,
        ));
        let exact_client = detached_client(cluster_paths.clone(), transport);
        let error = exact_client
            .get("/dfs/requested/entry")
            .expect_err("a mismatched exact-key reply must be rejected");
        assert!(error.to_string().contains("returned entry"));
        assert_eq!(
            exact_client.observed_revision.load(AtomicOrdering::Acquire),
            0,
            "a rejected reply is not observed"
        );

        let transport = Arc::new(ScriptedMountedKvTransport::new(
            local_node,
            true,
            MountedTxnResponseFault::OutOfPrefixAuthoritativeEntry,
        ));
        let prefix_client = detached_client(cluster_paths, transport);
        let error = prefix_client
            .range_prefix("/dfs/requested/")
            .expect_err("an out-of-prefix reply must be rejected");
        assert!(error.to_string().contains("returned entry"));
        assert_eq!(prefix_client.observed_revision.load(AtomicOrdering::Acquire), 0);
    }

    #[test]
    fn mounted_control_plane_rejects_unbound_or_stale_txn_proofs() {
        mounted_txn_rejected_proof_case(false);
    }

    #[test]
    fn mounted_control_plane_rejects_unbound_or_stale_exact_receipts() {
        mounted_txn_rejected_proof_case(true);
    }

    fn mounted_txn_rejected_proof_case(exact_receipt: bool) {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-control-plane-proof");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_control_plane_proof").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_mounted_sifs_control_plane_proof",
            )
            .expect("cluster id"),
            "mounted SIFS control plane proof",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        for fault in [
            MountedTxnResponseFault::MismatchedRequestId,
            MountedTxnResponseFault::MismatchedDigest,
            MountedTxnResponseFault::MissingSubmittedDigest,
            MountedTxnResponseFault::InvalidAuthority,
            MountedTxnResponseFault::StaleAuthorityRevision,
        ] {
            let mut transport = ScriptedMountedKvTransport::new(local_node.clone(), true, fault);
            transport.exact_receipt = exact_receipt;
            let transport = Arc::new(transport);
            let mut client = detached_client(cluster_paths.clone(), transport);
            let result = client.txn(KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/dfs/test/rejected".to_string(),
                    value: Vec::new(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            });
            if matches!(fault, MountedTxnResponseFault::MissingSubmittedDigest) && !exact_receipt {
                result.expect("retain legacy fresh-quorum compatibility");
                continue;
            }
            let error = result.expect_err("unbound or stale mounted transaction proof");
            assert!(
                matches!(
                    error,
                    KvError::Serialization { .. } | KvError::AuthorityUnavailable { .. }
                ),
                "unexpected rejection for {fault:?}: {error}"
            );
        }
    }

    #[test]
    fn clustered_mounted_open_uses_daemon_authority_without_a_materialized_projection() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-no-projection");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_no_projection").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_mounted_sifs_no_projection")
                .expect("cluster id"),
            "mounted SIFS no projection",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        assert!(!metadata_root.exists());

        let peer = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::MissingAuthoritativeEntry,
        ));
        let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let opened = open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
            data_dir.clone(),
            sync_peer_transport,
            async_peer_transport,
        )
        .expect("clustered mount from daemon authority");
        assert_eq!(opened.service.node_id(), &local_node);
        // The mount delegates namespace work to its node's Main, which alone
        // owns the node's metadata store; opening it authors no local store.
        assert!(opened.service.metadata_backend().is_none());
        assert!(
            !metadata_root.exists(),
            "a delegating mount must not author a local metadata store"
        );
        assert!(peer.routes().iter().any(|route| {
            route == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
        }));

        fs::write(&cluster_paths.kv_paths().snapshot, b"not a SOBJ snapshot")
            .expect("corrupt materialized projection");
        let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let reopened = open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
            data_dir,
            sync_peer_transport,
            async_peer_transport,
        )
        .expect("corrupt observational projection must not affect authoritative open");
        assert_eq!(reopened.service.node_id(), &local_node);
    }

    #[test]
    fn mounted_opener_never_replays_daemon_owned_raft() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-open-no-raft-replay");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_open_no_raft").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_mounted_sifs_open_no_raft")
                .expect("cluster id"),
            "mounted SIFS open without Raft replay",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("materialized cluster KV state");
        assert!(
            mounted_sifs_materialized_snapshot(&cluster_paths)
                .expect("materialized projection")
                .is_some()
        );

        // A durable-state marker makes any current-KV helper attempt cold
        // replay. The directory sentinel turns opening node.lock into a
        // deterministic failure even without a second process.
        let wal_paths = cluster_paths.kv_paths().raft_wal_paths(&local_node);
        fs::create_dir_all(&wal_paths.root).expect("Raft root");
        fs::write(&wal_paths.wal, b"").expect("durable-state marker");
        // Fixture initialization may leave the unlocked lock-file inode in
        // place. Replace that test-owned file with a directory so any later
        // attempt to open the daemon's Raft node fails deterministically.
        if wal_paths.lock.exists() {
            fs::remove_file(&wal_paths.lock).expect("remove fixture lock file");
        }
        fs::create_dir(&wal_paths.lock).expect("inaccessible node lock sentinel");

        let peer = Arc::new(ScriptedMountedKvTransport::new(
            local_node.clone(),
            true,
            MountedTxnResponseFault::MissingAuthoritativeEntry,
        ));
        let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let opened = open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
            data_dir,
            sync_peer_transport,
            async_peer_transport,
        )
        .expect("open mounted SIFS from daemon authority");

        assert_eq!(opened.service.node_id(), &local_node);
        let routes = peer.routes();
        assert!(!routes.is_empty(), "clustered open must consult authority");
        assert!(routes.iter().all(|route| {
            route == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE
                || route == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE
                || route == sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
        }));
    }

    #[test]
    fn mounted_content_read_context_overlaps_remote_opened_range_reads() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-content-read-context");
        let local = NodeId::from_legacy_named("node_mounted_read_local").expect("local node");
        let remote = NodeId::from_legacy_named("node_mounted_read_remote").expect("remote node");
        let object_id =
            crate::DfsObjectId::from_legacy_named("object_mounted_read").expect("object id");
        let version =
            crate::DfsVersion::from_legacy_named("version_mounted_read").expect("version");
        let bytes = b"request-local-read-context".to_vec();
        let content_hash = crate::content_hash(&bytes);
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_for_sender = Arc::clone(&release);
        let bytes_for_sender = bytes.clone();
        let sender: crate::DfsMessageSender = Arc::new(move |target_node_id, command, _options| {
            let request = match &command.routed_message.message {
                crate::DfsNetworkMessage::ReadRequest(request) => request,
                other => {
                    return Err(DfsPeerTransportError::new(format!(
                        "unexpected mounted read command: {other:?}"
                    )));
                }
            };
            entered_tx
                .send(())
                .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
            let (released, wake) = &*release_for_sender;
            let mut released = released.lock().expect("release lock");
            while !*released {
                released = wake.wait(released).expect("release wait");
            }
            let offset = request.range_offset.expect("range offset");
            let max_bytes = request.range_max_bytes.expect("range max bytes");
            let start = usize::try_from(offset).expect("range offset fits");
            let end = start.saturating_add(max_bytes).min(bytes_for_sender.len());
            let response = crate::DfsReadResponse {
                request_id: request.request_id.clone(),
                object_id: request.object_id.clone(),
                result: crate::DfsReadResult::RangeFound(crate::DfsReadRangeData {
                    object_id: request.object_id.clone(),
                    version: request.version.clone().expect("exact version"),
                    content_hash: request.content_hash.clone(),
                    offset,
                    total_size_bytes: request.content_size_bytes.expect("exact size"),
                    bytes: bytes_for_sender[start..end].to_vec(),
                }),
            };
            Ok(crate::DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: vec![crate::DfsRoutedMessage {
                    source_node_id: target_node_id.clone(),
                    target_node_id: command.routed_message.source_node_id.clone(),
                    frame_id: command.routed_message.frame_id,
                    message_id: command.routed_message.message_id,
                    message: crate::DfsNetworkMessage::ReadResponse(response),
                }],
            })
        });
        let transport = DfsNodeTransport::with_message_sender_for_nodes(
            local,
            vec![remote.clone()],
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            sender,
        );
        let service = NetworkedDfsService::new(
            transport,
            crate::LocalContentStore::new(root.join("content")).expect("content store"),
        );
        let context = service.mounted_content_read_context();
        // The context must not borrow or clone the mutable service. Dropping it
        // before the workers also exercises that ownership boundary.
        drop(service);
        let opened = crate::DfsStat {
            file_system_id: crate::sifs_file_system_id(),
            object_id,
            path: "/opened.txt".to_string(),
            kind: crate::DfsObjectKind::File,
            version,
            content_hash: Some(content_hash),
            size_bytes: Some(bytes.len() as u64),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec![remote],
        };
        let first_context = context.clone();
        let first_opened = opened.clone();
        let first = thread::spawn(move || {
            first_context.read_opened_file_range("/opened.txt", &first_opened, 0, 7)
        });
        let second =
            thread::spawn(move || context.read_opened_file_range("/opened.txt", &opened, 7, 6));

        let first_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        let second_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }
        let first = first.join().expect("first read thread");
        let second = second.join().expect("second read thread");

        assert!(
            first_entered && second_entered,
            "remote reads did not overlap"
        );
        assert_eq!(first.expect("first opened range"), &bytes[..7]);
        assert_eq!(second.expect("second opened range"), &bytes[7..13]);
    }

    fn cancellation_test_opened_stat(remote: NodeId) -> crate::DfsStat {
        let bytes = b"cancelled-opened-read";
        crate::DfsStat {
            file_system_id: crate::sifs_file_system_id(),
            object_id: crate::DfsObjectId::from_legacy_named("object_cancelled_opened_read")
                .expect("object id"),
            path: "/cancelled.txt".to_string(),
            kind: crate::DfsObjectKind::File,
            version: crate::DfsVersion::from_legacy_named("version_cancelled_opened_read")
                .expect("version"),
            content_hash: Some(crate::content_hash(bytes)),
            size_bytes: Some(bytes.len() as u64),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec![remote],
        }
    }

    fn cancellation_test_read_context(
        root: &std::path::Path,
        local: NodeId,
        remote: NodeId,
        sender: crate::DfsMessageSender,
        cancellation: crate::DfsCancellationFlag,
    ) -> MountedSifsContentReadContext {
        let transport = DfsNodeTransport::with_message_sender_for_nodes(
            local,
            [remote],
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            sender,
        );
        let mut service = NetworkedDfsService::new(
            transport,
            crate::LocalContentStore::new(root.join("content")).expect("content store"),
        );
        service.add_cancellation(cancellation);
        service.mounted_content_read_context()
    }

    #[test]
    fn cancelled_queued_opened_read_never_sends_its_first_remote_request() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-read-cancel-before-send");
        let local = NodeId::from_legacy_named("node_cancel_before_send_local").expect("local");
        let remote = NodeId::from_legacy_named("node_cancel_before_send_remote").expect("remote");
        let sends = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let sends_for_transport = Arc::clone(&sends);
        let sender: crate::DfsMessageSender = Arc::new(move |_target, _command, _options| {
            sends_for_transport.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(DfsPeerTransportError::new(
                "cancelled read must not reach transport",
            ))
        });
        let cancellation = crate::DfsCancellationFlag::new();
        let context = cancellation_test_read_context(
            &root,
            local,
            remote.clone(),
            sender,
            cancellation.clone(),
        );
        let opened = cancellation_test_opened_stat(remote);
        cancellation.cancel();

        let error = context
            .read_opened_file_range("/cancelled.txt", &opened, 0, 4)
            .expect_err("cancelled queued read");

        assert!(matches!(
            error,
            crate::DfsPosixError::Dfs(crate::DfsError::Cancelled {
                operation: "mounted opened-file read before content transfer"
            })
        ));
        assert_eq!(sends.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn cancellation_during_first_remote_send_stops_read_before_response_processing() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-read-cancel-in-flight");
        let local = NodeId::from_legacy_named("node_cancel_in_flight_local").expect("local");
        let remote = NodeId::from_legacy_named("node_cancel_in_flight_remote").expect("remote");
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_for_sender = Arc::clone(&release);
        let sender: crate::DfsMessageSender = Arc::new(move |target, command, _options| {
            entered_tx
                .send(())
                .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
            let (released, wake) = &*release_for_sender;
            let mut released = released.lock().expect("release lock");
            while !*released {
                released = wake.wait(released).expect("release wait");
            }
            let request = match &command.routed_message.message {
                crate::DfsNetworkMessage::ReadRequest(request) => request,
                other => {
                    return Err(DfsPeerTransportError::new(format!(
                        "unexpected cancellation command: {other:?}"
                    )));
                }
            };
            Ok(crate::DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: vec![crate::DfsRoutedMessage {
                    source_node_id: target.clone(),
                    target_node_id: command.routed_message.source_node_id.clone(),
                    frame_id: command.routed_message.frame_id,
                    message_id: command.routed_message.message_id,
                    message: crate::DfsNetworkMessage::ReadResponse(crate::DfsReadResponse {
                        request_id: request.request_id.clone(),
                        object_id: request.object_id.clone(),
                        result: crate::DfsReadResult::NotFound,
                    }),
                }],
            })
        });
        let cancellation = crate::DfsCancellationFlag::new();
        let context = cancellation_test_read_context(
            &root,
            local,
            remote.clone(),
            sender,
            cancellation.clone(),
        );
        let opened = cancellation_test_opened_stat(remote);
        let read =
            thread::spawn(move || context.read_opened_file_range("/cancelled.txt", &opened, 0, 4));
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first remote send entered");
        cancellation.cancel();
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }

        let error = read
            .join()
            .expect("read thread")
            .expect_err("in-flight read cancelled");
        assert!(matches!(
            error,
            crate::DfsPosixError::Dfs(crate::DfsError::Cancelled {
                operation: "remote metadata range read wait"
            })
        ));
    }

    #[test]
    fn linked_cancellation_preserves_every_source() {
        let external = crate::DfsCancellationFlag::new();
        let mount = crate::DfsCancellationFlag::new();
        let linked = crate::DfsCancellationFlag::linked([external.clone(), mount.clone()]);
        assert!(!linked.is_cancelled());
        external.cancel();
        assert!(linked.is_cancelled());

        let external = crate::DfsCancellationFlag::new();
        let linked = crate::DfsCancellationFlag::linked([external, mount.clone()]);
        assert!(!linked.is_cancelled());
        mount.cancel();
        assert!(linked.is_cancelled());
    }
}

/// First delay before re-subscribing after the daemon event stream ended;
/// it doubles up to [`MOUNTED_SIFS_CONFIGURATION_RESUBSCRIBE_MAX_DELAY`].
/// This is a re-send timer, never a network deadline.
const MOUNTED_SIFS_CONFIGURATION_RESUBSCRIBE_DELAY: Duration = Duration::from_millis(250);
const MOUNTED_SIFS_CONFIGURATION_RESUBSCRIBE_MAX_DELAY: Duration = Duration::from_secs(5);
/// Connect budget for the daemon event stream; the stream itself is
/// long-lived and kept alive by session keepalives.
const MOUNTED_SIFS_CONFIGURATION_STREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// What a mounted client currently knows about the local daemon's published
/// DFS configuration.
#[derive(Clone, Debug)]
enum MountedSifsConfiguration {
    /// The first subscription has not settled yet.
    Connecting,
    /// No live event subscription: a held snapshot could miss a change, so
    /// none is used until the next subscription has fetched again.
    Disconnected,
    /// Subscribed; the daemon has no coherent publication yet.
    Pending(DfsConfigurationPublication),
    /// Subscribed, and fetched after the newest announcement seen.
    Published(Arc<crate::DfsConfigurationSnapshot>),
    /// The local daemon predates the snapshot route.
    Unsupported,
}

#[derive(Debug)]
struct MountedSifsConfigurationShared {
    state: Mutex<(MountedSifsConfiguration, u64)>,
    settled: Condvar,
}

impl MountedSifsConfigurationShared {
    fn new() -> Self {
        Self {
            state: Mutex::new((MountedSifsConfiguration::Connecting, 0)),
            settled: Condvar::new(),
        }
    }

    fn set(&self, configuration: MountedSifsConfiguration) {
        if let Ok(mut state) = self.state.lock() {
            state.0 = configuration;
            state.1 = state.1.wrapping_add(1);
        }
        self.settled.notify_all();
    }

    fn held_publication(&self) -> Option<DfsConfigurationPublication> {
        match &self.state.lock().ok()?.0 {
            MountedSifsConfiguration::Published(snapshot) => Some(snapshot.publication()),
            MountedSifsConfiguration::Pending(publication) => Some(*publication),
            _ => None,
        }
    }

    fn is_unsupported(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| matches!(state.0, MountedSifsConfiguration::Unsupported))
    }

    /// The current state. Only a use before the first subscription settled
    /// waits, and only under one network deadline.
    fn current(&self) -> MountedSifsConfiguration {
        // The state is never held across a wait: the wait below releases it.
        if MountedSifsCall::deadline().is_some_and(|at| at <= Instant::now()) {
            return MountedSifsConfiguration::Disconnected;
        }
        let Ok(mut state) = self.state.lock() else {
            return MountedSifsConfiguration::Disconnected;
        };
        let network_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let deadline =
            MountedSifsCall::deadline().map_or(network_deadline, |call| call.min(network_deadline));
        while matches!(state.0, MountedSifsConfiguration::Connecting) {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return MountedSifsConfiguration::Disconnected;
            };
            state = match self.settled.wait_timeout(state, remaining) {
                Ok((state, _)) => state,
                Err(_) => return MountedSifsConfiguration::Disconnected,
            };
        }
        state.0.clone()
    }

    #[cfg(test)]
    fn wait_until(
        &self,
        settled: impl Fn(&MountedSifsConfiguration) -> bool,
    ) -> MountedSifsConfiguration {
        let mut state = self.state.lock().expect("mounted configuration state");
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        while !settled(&state.0) {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("mounted configuration deadline");
            state = self
                .settled
                .wait_timeout(state, remaining)
                .expect("mounted configuration state")
                .0;
        }
        state.0.clone()
    }

    #[cfg(test)]
    fn wait_for_transitions(&self, transitions: u64) -> MountedSifsConfiguration {
        let mut state = self.state.lock().expect("mounted configuration state");
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        while state.1 < transitions {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("mounted configuration transition deadline");
            state = self
                .settled
                .wait_timeout(state, remaining)
                .expect("mounted configuration state")
                .0;
        }
        state.0.clone()
    }
}

/// One stream item, already decoded from the daemon event stream.
#[derive(Debug)]
enum MountedSifsConfigurationItem {
    Ready,
    Published(DfsConfigurationPublication),
    Lagged,
}

#[cfg(test)]
type MountedSifsConfigurationItems =
    tokio::sync::mpsc::UnboundedReceiver<MountedSifsConfigurationItem>;

/// Where announcements come from: the local daemon's event stream, or, in
/// fixtures that host the DFS component in process, that component's bus.
#[derive(Clone)]
enum MountedSifsConfigurationEvents {
    Daemon(SinomoDataDir),
    #[cfg(any(test, feature = "test-support"))]
    Bus(SinomoDataDir),
    /// Each subscription takes the next scripted session.
    #[cfg(test)]
    Scripted(Arc<Mutex<std::collections::VecDeque<MountedSifsConfigurationItems>>>),
}

impl MountedSifsConfigurationEvents {
    fn for_data_dir(data_dir: &SinomoDataDir) -> Self {
        #[cfg(any(test, feature = "test-support"))]
        if mounted_sifs_test_event_bus(data_dir).is_some() {
            return Self::Bus(data_dir.clone());
        }
        Self::Daemon(data_dir.clone())
    }

    /// Deliver items until the subscription ends.
    async fn subscribe(
        &self,
        items: tokio::sync::mpsc::UnboundedSender<MountedSifsConfigurationItem>,
    ) -> Result<(), String> {
        match self {
            Self::Daemon(data_dir) => {
                mounted_sifs_daemon_configuration_events(data_dir, items).await
            }
            #[cfg(any(test, feature = "test-support"))]
            Self::Bus(data_dir) => {
                // A stopped component ends the subscription, as a daemon
                // restart ends its event stream.
                let (bus, mut stopped) = mounted_sifs_test_event_bus(data_dir)
                    .ok_or_else(|| "DFS component is not running".to_string())?;
                let mut receiver = bus.subscribe_host();
                if items.send(MountedSifsConfigurationItem::Ready).is_err() {
                    return Ok(());
                }
                loop {
                    let received = tokio::select! {
                        received = receiver.recv() => received,
                        _ = stopped.wait_for(|stopped| *stopped) => {
                            return Err("DFS component stopped".into());
                        }
                    };
                    let item = match received {
                        Ok(event)
                            if event.envelope.kind.as_str()
                                == crate::DFS_CONFIGURATION_PUBLISHED_EVENT_KIND =>
                        {
                            mounted_sifs_configuration_item(event.envelope.payload.clone())
                        }
                        Ok(_) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            MountedSifsConfigurationItem::Lagged
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return Err("DFS component event bus closed".into());
                        }
                    };
                    if items.send(item).is_err() {
                        return Ok(());
                    }
                }
            }
            #[cfg(test)]
            Self::Scripted(sessions) => {
                let next = sessions.lock().expect("scripted sessions").pop_front();
                let Some(mut session) = next else {
                    return std::future::pending().await;
                };
                while let Some(item) = session.recv().await {
                    if items.send(item).is_err() {
                        return Ok(());
                    }
                }
                Err("scripted configuration stream ended".into())
            }
        }
    }
}

/// An undecodable announcement still means "changed".
fn mounted_sifs_configuration_item(payload: serde_json::Value) -> MountedSifsConfigurationItem {
    match serde_json::from_value(payload) {
        Ok(publication) => MountedSifsConfigurationItem::Published(publication),
        Err(_) => MountedSifsConfigurationItem::Lagged,
    }
}

/// Subscribe to `dfs.configuration.published` on the local daemon's existing
/// event stream. The authenticated local Main is resolved for each session.
async fn mounted_sifs_daemon_configuration_events(
    data_dir: &SinomoDataDir,
    items: tokio::sync::mpsc::UnboundedSender<MountedSifsConfigurationItem>,
) -> Result<(), String> {
    let runtime = tokio::runtime::Handle::current();
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data_dir))
        .map_err(|error| error.to_string())?;
    let address = match sinomo_api::probe_daemon_status_async(
        &sinomo_api::DaemonPaths::new(data_dir),
        sinomo_api::DAEMON_API_PROBE_TIMEOUT,
        &runtime,
    )
    .await
    {
        sinomo_api::DaemonProbeStatus::Running(status)
            if status.cluster.as_ref().is_some_and(|cluster| {
                cluster.cluster_id == state.cluster_id
                    && cluster.node_id == state.local_node_id
                    && cluster.process == sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE
            }) =>
        {
            status.address
        }
        _ => return Err("the local Main daemon is unavailable".into()),
    };
    let request = sinomo_core::network::ObjectRpcRequest::encode_body(
        sinomo_api::DAEMON_EVENT_SUBSCRIBE_ROUTE,
        sinomo_api::DAEMON_RPC_CATEGORY,
        &sinomo_api::DaemonEventSubscribeRequest::for_dfs_configuration(),
    )
    .map_err(|error| error.to_string())?;
    sinomo_api::send_daemon_rpc_stream_from_data_dir_async(
        data_dir.clone(),
        &address,
        &request,
        sinomo_core::network::DaemonRpcTimeouts::default()
            .with_connect_timeout(Some(MOUNTED_SIFS_CONFIGURATION_STREAM_CONNECT_TIMEOUT)),
        |response| {
            response.error_detail()?;
            let item = match response
                .decode_body::<sinomo_api::DaemonEventStreamItem>()
                .map_err(sinomo_api::DaemonApiError::from)?
            {
                sinomo_api::DaemonEventStreamItem::Ready { .. } => {
                    MountedSifsConfigurationItem::Ready
                }
                sinomo_api::DaemonEventStreamItem::Event { event }
                    if event.kind.as_str() == crate::DFS_CONFIGURATION_PUBLISHED_EVENT_KIND =>
                {
                    mounted_sifs_configuration_item(event.payload)
                }
                sinomo_api::DaemonEventStreamItem::Event { .. } => return Ok(true),
                sinomo_api::DaemonEventStreamItem::Lagged { .. } => {
                    MountedSifsConfigurationItem::Lagged
                }
            };
            Ok(items.send(item).is_ok())
        },
    )
    .await
    .map_err(|error| error.to_string())
}

/// One local-node read of the current publication.
#[derive(Clone)]
struct MountedSifsConfigurationFetch {
    cluster_paths: ClusterPaths,
    transport: Arc<dyn AsyncDfsPeerTransport>,
}

impl MountedSifsConfigurationFetch {
    async fn fetch(&self) -> Result<crate::DfsFetchedConfiguration, KvError> {
        let unavailable = |detail: String| KvError::AuthorityUnavailable { detail };
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)
            .map_err(|error| unavailable(error.to_string()))?;
        let call = crate::dfs_configuration_snapshot_call()?;
        let result = self
            .transport
            .call_object_rpc_async(
                &state.local_node_id,
                call,
                DfsObjectRpcSendOptions::default(),
            )
            .await
            .map_err(|error| {
                if error.is_unknown_route(crate::DFS_CONFIGURATION_SNAPSHOT_ROUTE) {
                    crate::dfs_configuration_snapshot_unsupported()
                } else {
                    unavailable(error.to_string())
                }
            })?;
        let response = result
            .into_materialized_value_response(
                crate::dfs_configuration_snapshot_max_response_bytes(),
            )
            .await
            .map_err(|error| match error {
                NetworkError::UnknownRoute { .. } => {
                    crate::dfs_configuration_snapshot_unsupported()
                }
                error => unavailable(error.to_string()),
            })?;
        crate::decode_dfs_configuration_snapshot_response(&response)
    }
}

/// Re-subscribe for as long as the feed lives. A lost stream withdraws the
/// held snapshot immediately; every new subscription fetches once after its
/// `Ready` before announcements are acted on.
async fn run_mounted_sifs_configuration_feed(
    shared: Arc<MountedSifsConfigurationShared>,
    events: MountedSifsConfigurationEvents,
    fetch: MountedSifsConfigurationFetch,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut delay = MOUNTED_SIFS_CONFIGURATION_RESUBSCRIBE_DELAY;
    loop {
        let subscribed = tokio::select! {
            subscribed = run_mounted_sifs_configuration_session(&shared, &events, &fetch) => subscribed,
            _ = stop.wait_for(|stopped| *stopped) => return,
        };
        shared.set(MountedSifsConfiguration::Disconnected);
        if subscribed {
            delay = MOUNTED_SIFS_CONFIGURATION_RESUBSCRIBE_DELAY;
        }
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            _ = stop.wait_for(|stopped| *stopped) => return,
        }
        delay = delay
            .saturating_mul(2)
            .min(MOUNTED_SIFS_CONFIGURATION_RESUBSCRIBE_MAX_DELAY);
    }
}

/// One subscription. Returns whether it became ready.
async fn run_mounted_sifs_configuration_session(
    shared: &MountedSifsConfigurationShared,
    events: &MountedSifsConfigurationEvents,
    fetch: &MountedSifsConfigurationFetch,
) -> bool {
    let (sender, mut items) = tokio::sync::mpsc::unbounded_channel();
    let stream = events.subscribe(sender);
    tokio::pin!(stream);
    let mut ready = false;
    loop {
        let first = tokio::select! {
            biased;
            item = items.recv() => match item {
                Some(item) => item,
                None => return ready,
            },
            ended = &mut stream => {
                if let Err(error) = ended {
                    tracing::debug!(target: "sinomo.dfs.mounted", %error,
                        "mounted SIFS configuration subscription ended");
                }
                return ready;
            }
        };
        // Coalesce: one fetch answers every announcement already queued.
        let mut refetch = false;
        let mut newest = None;
        for item in std::iter::once(first).chain(std::iter::from_fn(|| items.try_recv().ok())) {
            match item {
                MountedSifsConfigurationItem::Ready => {
                    ready = true;
                    refetch = true;
                }
                MountedSifsConfigurationItem::Lagged => refetch = true,
                MountedSifsConfigurationItem::Published(publication) => {
                    newest = Some(publication);
                }
            }
        }
        if !ready {
            continue;
        }
        let advanced = newest.is_some_and(|publication| {
            // An old daemon without the route announces nothing; a newer
            // announcement means the route is served now.
            shared.is_unsupported()
                || shared
                    .held_publication()
                    .is_none_or(|held| publication.advances_past(&held))
        });
        if !refetch && !advanced {
            continue;
        }
        match fetch.fetch().await {
            Ok(crate::DfsFetchedConfiguration::Published(snapshot)) => {
                shared.set(MountedSifsConfiguration::Published(snapshot));
            }
            Ok(crate::DfsFetchedConfiguration::Pending(publication)) => {
                shared.set(MountedSifsConfiguration::Pending(publication));
            }
            Err(error) if crate::is_dfs_configuration_snapshot_unsupported(&error) => {
                shared.set(MountedSifsConfiguration::Unsupported);
            }
            Err(error) => {
                // End this session; the re-subscription fetches again.
                tracing::debug!(target: "sinomo.dfs.mounted", %error,
                    "mounted SIFS configuration fetch failed");
                return ready;
            }
        }
    }
}

/// Event-driven holder of the published configuration for one mounted
/// client and its clones. Dropping the last clone ends its subscription.
struct MountedSifsConfigurationFeed {
    shared: Arc<MountedSifsConfigurationShared>,
    stop: tokio::sync::watch::Sender<bool>,
}

impl std::fmt::Debug for MountedSifsConfigurationFeed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountedSifsConfigurationFeed")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl Drop for MountedSifsConfigurationFeed {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

impl MountedSifsConfigurationFeed {
    fn subscribe(
        events: MountedSifsConfigurationEvents,
        fetch: MountedSifsConfigurationFetch,
    ) -> Arc<Self> {
        let shared = Arc::new(MountedSifsConfigurationShared::new());
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let task_shared = Arc::clone(&shared);
        let spawned = std::thread::Builder::new()
            .name("sinomo-sifs-configuration".into())
            .spawn(move || {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime.block_on(run_mounted_sifs_configuration_feed(
                        task_shared,
                        events,
                        fetch,
                        stopped,
                    )),
                    Err(error) => {
                        tracing::warn!(%error, "mounted SIFS configuration feed has no runtime");
                        task_shared.set(MountedSifsConfiguration::Disconnected);
                    }
                }
            });
        if let Err(error) = spawned {
            tracing::warn!(%error, "mounted SIFS configuration feed could not start");
            shared.set(MountedSifsConfiguration::Disconnected);
        }
        Arc::new(Self { shared, stop })
    }
}

#[cfg(any(test, feature = "test-support"))]
type MountedSifsTestEventBuses = std::collections::BTreeMap<
    PathBuf,
    (
        sinomo_module::ComponentEventBus,
        tokio::sync::watch::Sender<bool>,
    ),
>;

#[cfg(any(test, feature = "test-support"))]
fn mounted_sifs_test_event_buses() -> &'static Mutex<MountedSifsTestEventBuses> {
    static BUSES: std::sync::OnceLock<Mutex<MountedSifsTestEventBuses>> =
        std::sync::OnceLock::new();
    BUSES.get_or_init(Default::default)
}

/// In-process fixtures host the DFS component without a daemon event
/// stream; their mounted clients subscribe to that component's bus instead.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn register_mounted_sifs_test_event_bus(
    data_dir: &SinomoDataDir,
    bus: Option<sinomo_module::ComponentEventBus>,
) {
    if let Ok(mut buses) = mounted_sifs_test_event_buses().lock() {
        let previous = match bus {
            Some(bus) => buses.insert(
                data_dir.root().to_path_buf(),
                (bus, tokio::sync::watch::channel(false).0),
            ),
            None => buses.remove(&data_dir.root().to_path_buf()),
        };
        if let Some((_, stopped)) = previous {
            let _ = stopped.send(true);
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
fn mounted_sifs_test_event_bus(
    data_dir: &SinomoDataDir,
) -> Option<(
    sinomo_module::ComponentEventBus,
    tokio::sync::watch::Receiver<bool>,
)> {
    mounted_sifs_test_event_buses()
        .lock()
        .ok()?
        .get(&data_dir.root().to_path_buf())
        .map(|(bus, stopped)| (bus.clone(), stopped.subscribe()))
}

/// Out-of-process DFS control-plane client for mounted SIFS.
///
/// Reads inside the published configuration scopes use the snapshot the
/// local daemon published, held by an event-driven feed. Every other read is
/// one bounded ReadIndex request through the local daemon and is not
/// retained; operation-bound fence reads always cross ReadIndex.
#[derive(Clone)]
pub struct MountedSifsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    configuration: Arc<MountedSifsConfigurationFeed>,
    /// Highest revision this client or its clones read or committed through
    /// the daemon. A ReadIndex reply below it is rejected.
    observed_revision: Arc<AtomicU64>,
    /// Highest revision at which this client committed a change inside a
    /// published scope. An older held snapshot does not answer that scope,
    /// so the client reads its own configuration writes.
    scope_write_revision: Arc<AtomicU64>,
}

impl std::fmt::Debug for MountedSifsControlPlaneKvClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountedSifsControlPlaneKvClient")
            .field("cluster_paths", &self.cluster_paths)
            .field("peer_transport", &"dyn DfsPeerTransport")
            .field("configuration", &self.configuration)
            .finish()
    }
}

impl MountedSifsControlPlaneKvClient {
    /// Subscribes to the local daemon's configuration announcements and
    /// sends every request through `peer_transport` to the local node.
    pub fn new_authoritative(
        data_dir: &SinomoDataDir,
        peer_transport: Arc<dyn DfsPeerTransport>,
        async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    ) -> Self {
        let cluster_paths = ClusterPaths::new(data_dir);
        let configuration = MountedSifsConfigurationFeed::subscribe(
            MountedSifsConfigurationEvents::for_data_dir(data_dir),
            MountedSifsConfigurationFetch {
                cluster_paths: cluster_paths.clone(),
                transport: async_peer_transport,
            },
        );
        Self::with_configuration(cluster_paths, peer_transport, configuration)
    }

    fn with_configuration(
        cluster_paths: ClusterPaths,
        peer_transport: Arc<dyn DfsPeerTransport>,
        configuration: Arc<MountedSifsConfigurationFeed>,
    ) -> Self {
        Self {
            cluster_paths,
            peer_transport,
            configuration,
            observed_revision: Arc::new(AtomicU64::new(0)),
            scope_write_revision: Arc::new(AtomicU64::new(0)),
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        mounted_sifs_kv_io_error(&self.cluster_paths, source)
    }

    fn observe_revision(&self, revision: KvRevision) {
        self.observed_revision
            .fetch_max(revision.0, AtomicOrdering::AcqRel);
    }

    /// The held publication, if it is current for this client's own writes.
    /// An older daemon is a typed error; no other read path replaces it.
    fn published_configuration(
        &self,
    ) -> Result<Option<Arc<crate::DfsConfigurationSnapshot>>, KvError> {
        let snapshot = match self.configuration.shared.current() {
            MountedSifsConfiguration::Published(snapshot) => snapshot,
            MountedSifsConfiguration::Unsupported => {
                return Err(crate::dfs_configuration_snapshot_unsupported());
            }
            MountedSifsConfiguration::Connecting
            | MountedSifsConfiguration::Disconnected
            | MountedSifsConfiguration::Pending(_) => return Ok(None),
        };
        let floor = self.scope_write_revision.load(AtomicOrdering::Acquire);
        let current = snapshot.revision().is_some_and(|revision| revision.0 >= floor)
            && snapshot
                .write_configuration()
                .is_none_or(|write| write.revision().0 >= floor);
        Ok(current.then_some(snapshot))
    }

    /// The publication answering a read inside its scopes.
    fn covering_snapshot(
        &self,
        key_or_prefix: &str,
    ) -> Result<Option<Arc<crate::DfsConfigurationSnapshot>>, KvError> {
        if !crate::dfs_configuration_scope_contains(key_or_prefix) {
            return Ok(None);
        }
        Ok(self
            .published_configuration()?
            .filter(|snapshot| snapshot.covering_revision(key_or_prefix).is_some()))
    }

    fn call_local_kv_rpc(
        &self,
        call: ObjectRpcCall,
        expected_route: &str,
        max_response_bytes: usize,
    ) -> Result<ObjectRpcResponse, KvError> {
        self.call_local_kv_rpc_raw(call, expected_route, max_response_bytes, None)
            .map_err(|source| self.kv_error(source))
    }

    fn call_local_kv_rpc_raw(
        &self,
        call: ObjectRpcCall,
        expected_route: &str,
        max_response_bytes: usize,
        deadline: Option<Instant>,
    ) -> Result<ObjectRpcResponse, DfsPeerTransportError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        // Compute the remaining budget immediately before transport dispatch;
        // reading local cluster state consumes the same operation deadline.
        let options = match MountedSifsCall::earliest(deadline) {
            Some(deadline) => {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return Err(DfsPeerTransportError::new(
                        "metadata shard authority read deadline elapsed",
                    ));
                };
                if remaining.is_zero() {
                    return Err(DfsPeerTransportError::new(
                        "metadata shard authority read deadline elapsed",
                    ));
                }
                DfsObjectRpcSendOptions::default().with_overall_timeout(remaining)
            }
            None => DfsObjectRpcSendOptions::default(),
        };
        let result = self
            .peer_transport
            .call_object_rpc(&state.local_node_id, call, options)?;
        let response = mounted_sifs_value_response_from_rpc_result(result, max_response_bytes)?;
        if response.route != expected_route {
            return Err(DfsPeerTransportError::new(format!(
                "mounted SIFS cluster KV route {expected_route} returned mismatched route {}",
                response.route
            )));
        }
        response
            .error_detail()
            .map_err(DfsPeerTransportError::from_network)?;
        Ok(response)
    }

    /// Accept a ReadIndex reply only at or after every revision this client
    /// already observed.
    fn accept_authoritative_revision(
        &self,
        observed: KvRevision,
        scope: &str,
    ) -> Result<(), KvError> {
        let minimum = self.observed_revision.load(AtomicOrdering::Acquire);
        if observed.0 < minimum {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "authoritative mounted SIFS read for {scope:?} returned revision {}, below observed revision {minimum}",
                    observed.0,
                ),
            });
        }
        self.observe_revision(observed);
        Ok(())
    }

    fn fetch_authoritative_entry(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        let payload = sinomo_core::kv::cluster_kv_authoritative_entry_rpc_payload(key)?;
        let route = payload.route;
        let call = ObjectRpcCall::value(sinomo_core::network::ObjectRpcRequest::from_encoded_body(
            payload.route,
            payload.category,
            payload.body,
        ));
        let response = self.call_local_kv_rpc(call, route, 1024 * 1024)?;
        let read = sinomo_core::kv::decode_cluster_kv_authoritative_entry_rpc_body(&response.body)?;
        if read.value.as_ref().is_some_and(|entry| entry.key != key) {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative mounted SIFS read for {key:?} returned entry for {:?}",
                    read.value.as_ref().expect("checked entry").key,
                ),
            });
        }
        self.accept_authoritative_revision(read.authority.revision, key)?;
        Ok(read.value)
    }

    fn fetch_authoritative_entries(
        &self,
        keys: &[String],
        deadline: Option<Instant>,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(mounted_sifs_authority_deadline_error());
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        sinomo_core::kv::validate_cluster_kv_authoritative_entries_request_keys(keys)?;
        let route = sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE;
        let call = ObjectRpcCall::encode_value(
            route,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            sinomo_core::kv::ClusterKvAuthoritativeEntriesRequest {
                keys: keys.to_vec(),
            },
        )
        .map_err(|source| self.kv_error(source))?;
        let response = self
            .call_local_kv_rpc_raw(
                call,
                route,
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES
                    .saturating_add(1024 * 1024),
                deadline,
            )
            .map_err(|error| self.kv_error(error))?;
        let read = sinomo_core::kv::decode_cluster_kv_authoritative_entries_rpc_body(
            &response.body,
            keys,
        )?;
        if keys.len() != read.value.len() {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative mounted SIFS exact-key set returned {} values for {} keys",
                    read.value.len(),
                    keys.len(),
                ),
            });
        }
        for (key, value) in keys.iter().zip(&read.value) {
            if value.as_ref().is_some_and(|entry| entry.key != *key) {
                return Err(KvError::Serialization {
                    detail: format!(
                        "authoritative mounted SIFS read for {key:?} returned entry for {:?}",
                        value.as_ref().expect("checked entry").key,
                    ),
                });
            }
        }
        self.accept_authoritative_revision(read.authority.revision, "exact-key set")?;
        Ok(read.value)
    }

    fn fetch_authoritative_prefix(
        &self,
        prefix: &str,
    ) -> Result<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>, KvError> {
        let payload = sinomo_core::kv::cluster_kv_authoritative_range_prefix_rpc_payload(prefix)?;
        let route = payload.route;
        let call = ObjectRpcCall::value(sinomo_core::network::ObjectRpcRequest::from_encoded_body(
            payload.route,
            payload.category,
            payload.body,
        ));
        let response = self.call_local_kv_rpc(
            call,
            route,
            sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES.saturating_add(1024 * 1024),
        )?;
        let read =
            sinomo_core::kv::decode_cluster_kv_authoritative_range_prefix_rpc_body(&response.body)?;
        if let Some(entry) = read
            .value
            .iter()
            .find(|entry| !entry.key.starts_with(prefix))
        {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative mounted SIFS prefix read for {prefix:?} returned entry for {:?}",
                    entry.key,
                ),
            });
        }
        self.accept_authoritative_revision(read.authority.revision, prefix)?;
        Ok(read)
    }
}

/// A mounted requester's wait on its configuration feed. It holds the feed,
/// so the subscription outlives the client's other clones for the wait.
struct MountedSifsConfigurationChangeWatch {
    configuration: Arc<MountedSifsConfigurationFeed>,
    seen_transitions: u64,
    woken: Arc<std::sync::atomic::AtomicBool>,
}

impl crate::DfsConfigurationChangeWatch for MountedSifsConfigurationChangeWatch {
    fn wait_for_change(&mut self, wait: Duration) -> Result<bool, KvError> {
        let shared = &self.configuration.shared;
        let deadline = Instant::now().checked_add(wait);
        let poisoned = || KvError::AuthorityUnavailable {
            detail: "mounted SIFS configuration state is poisoned".into(),
        };
        let mut state = shared.state.lock().map_err(|_| poisoned())?;
        loop {
            if state.1 != self.seen_transitions {
                self.seen_transitions = state.1;
                return Ok(true);
            }
            if self.woken.swap(false, AtomicOrdering::AcqRel) {
                return Ok(false);
            }
            state = match deadline {
                Some(deadline) => {
                    let Some(remaining) = deadline
                        .checked_duration_since(Instant::now())
                        .filter(|remaining| !remaining.is_zero())
                    else {
                        return Ok(false);
                    };
                    shared
                        .settled
                        .wait_timeout(state, remaining)
                        .map_err(|_| poisoned())?
                        .0
                }
                None => shared.settled.wait(state).map_err(|_| poisoned())?,
            };
        }
    }

    fn waker(&self) -> Arc<dyn Fn() + Send + Sync> {
        let configuration = Arc::clone(&self.configuration);
        let woken = Arc::clone(&self.woken);
        Arc::new(move || {
            woken.store(true, AtomicOrdering::Release);
            // Taking the state lock orders this wake after a waiter's check.
            drop(configuration.shared.state.lock());
            configuration.shared.settled.notify_all();
        })
    }
}

impl DfsControlPlaneKvClient for MountedSifsControlPlaneKvClient {
    /// sinomo:kv-configuration: Lend the snapshot the local daemon floor-checked and published, as last announced on its event stream.
    fn configuration_snapshot(
        &self,
    ) -> Result<Option<Arc<crate::DfsConfigurationSnapshot>>, KvError> {
        self.published_configuration()
    }

    /// sinomo:kv-configuration: Observe replacements of the snapshot held from the local daemon's event stream.
    fn watch_configuration_changes(&self) -> Option<Box<dyn crate::DfsConfigurationChangeWatch>> {
        let seen_transitions = self.configuration.shared.state.lock().ok()?.1;
        Some(Box::new(MountedSifsConfigurationChangeWatch {
            configuration: Arc::clone(&self.configuration),
            seen_transitions,
            woken: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }))
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        if let Some(snapshot) = self.covering_snapshot(key)? {
            return Ok(snapshot.entry(key).flatten().cloned());
        }
        self.fetch_authoritative_entry(key)
    }

    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        // One publication answers the set only when it covers every key;
        // otherwise one exact-set ReadIndex pins every key to one revision.
        if !keys.is_empty()
            && keys
                .iter()
                .all(|key| crate::dfs_configuration_scope_contains(key))
            && let Some(snapshot) = self.published_configuration()?
            && keys
                .iter()
                .all(|key| snapshot.covering_revision(key).is_some())
        {
            return Ok(keys
                .iter()
                .map(|key| snapshot.entry(key).flatten().cloned())
                .collect());
        }
        self.fetch_authoritative_entries(keys, None)
    }

    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        // Operation-bound fences never inherit a publication.
        self.fetch_authoritative_entries(keys, None)
    }

    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        if Instant::now() >= deadline {
            return Err(mounted_sifs_authority_deadline_error());
        }
        self.fetch_authoritative_entries(keys, Some(deadline))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        if let Some(snapshot) = self.covering_snapshot(prefix)? {
            return Ok(snapshot.entries(prefix).unwrap_or_default().to_vec());
        }
        Ok(self.fetch_authoritative_prefix(prefix)?.value)
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<crate::DfsAuthoritativePrefixProjection, KvError> {
        let read = self.fetch_authoritative_prefix(prefix)?;
        crate::DfsAuthoritativePrefixProjection::complete(prefix, read.value, read.authority.revision)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let request = sinomo_core::kv::KvTxnRequest::generated(txn);
        let submitted_digest = sinomo_core::kv::kv_command_digest(
            &sinomo_core::kv::KvCommand::Txn(request.txn.clone()),
        )?;
        let call = ObjectRpcCall::encode_borrowed_value(
            sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &request,
        )
        .map_err(|source| self.kv_error(source))?;
        let response =
            self.call_local_kv_rpc(call, sinomo_core::kv::CLUSTER_KV_TXN_ROUTE, 1024 * 1024)?;
        let result = sinomo_core::kv::decode_cluster_kv_txn_rpc_body(&response.body)?;
        if result.request_id != request.request_id {
            return Err(KvError::Serialization {
                detail: format!(
                    "mounted SIFS cluster KV transaction response request id {} does not match {}",
                    result.request_id, request.request_id,
                ),
            });
        }
        if result
            .submitted_command_digest
            .is_some_and(|digest| digest != submitted_digest)
            || (matches!(
                result.authority.basis,
                sinomo_core::kv::KvAuthorityBasis::ExactCommittedRequest
            ) && result.submitted_command_digest != Some(submitted_digest))
        {
            return Err(KvError::Serialization {
                detail:
                    "mounted SIFS cluster KV transaction response does not match the submitted command"
                        .to_string(),
            });
        }
        let response = result.txn_response.ok_or_else(|| KvError::Serialization {
            detail: "cluster KV returned a barrier result for a mounted SIFS transaction"
                .to_string(),
        })?;
        sinomo_core::kv::validate_cluster_kv_txn_authority_proof(
            &result.authority,
            result.replayed_committed_request,
            response.revision,
        )?;
        self.observe_revision(response.revision);
        let executed = if response.succeeded {
            &request.txn.success
        } else {
            &request.txn.failure
        };
        let touches_published_scope = executed.iter().any(|operation| match operation {
            KvOp::Put { key, .. } | KvOp::Delete { key } => {
                crate::dfs_configuration_scope_contains(key)
            }
            KvOp::DeletePrefix { prefix } => crate::dfs_configuration_scopes_overlap(prefix),
            // A revoked lease can delete leased keys inside any scope.
            KvOp::LeaseRevoke { .. } => true,
            KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } => false,
        });
        if touches_published_scope {
            self.scope_write_revision
                .fetch_max(response.revision.0, AtomicOrdering::AcqRel);
        }
        Ok(response)
    }
}

#[cfg(test)]
fn empty_mounted_sifs_kv_snapshot(revision: KvRevision) -> KvSnapshot {
    KvSnapshot {
        revision,
        compacted_revision: KvRevision::ZERO,
        entries: Vec::new(),
        leases: Vec::new(),
        events: Vec::new(),
        committed_requests: Vec::new(),
        expired_requests: Vec::new(),
    }
}

fn mounted_sifs_value_response_from_rpc_result(
    result: ObjectRpcResult,
    max_bytes: usize,
) -> Result<ObjectRpcResponse, DfsPeerTransportError> {
    let streamed_value = result.has_streamed_value();
    let (mut response, body) = result.into_parts();
    if streamed_value {
        response
            .error_detail()
            .map_err(DfsPeerTransportError::from_network)?;
        response.body = collect_dfs_incoming_body_sync(body, max_bytes)?;
        return Ok(response);
    }
    if !body.is_empty() {
        return Err(DfsPeerTransportError::new(format!(
            "mounted SIFS KV route {} returned an unexpected attachment",
            response.route
        )));
    }
    if response.body.len() > max_bytes {
        return Err(DfsPeerTransportError::new(format!(
            "mounted SIFS KV route {} returned {} bytes, exceeding the {max_bytes} byte limit",
            response.route,
            response.body.len(),
        )));
    }
    Ok(response)
}

fn mounted_sifs_kv_io_error(
    cluster_paths: &ClusterPaths,
    source: impl std::fmt::Display,
) -> KvError {
    KvError::Io {
        path: cluster_paths.kv_paths().snapshot.display().to_string(),
        detail: source.to_string(),
    }
}

#[derive(Clone, Debug)]
struct MountedSifsPeerTransport {
    cluster_paths: ClusterPaths,
}

impl MountedSifsPeerTransport {
    async fn call_object_rpc_async_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
        _runtime: &tokio::runtime::Handle,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)?;
        let mut host = MountedSifsObjectRpcHost;
        let mut client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &self.cluster_paths);
        let result = client
            .call_target(&state, target_node_id, call, options.cluster_rpc_options())
            .await?;
        result.response.error_detail()?;
        Ok(result)
    }
}

impl DfsPeerTransport for MountedSifsPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return if matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                let result = tokio::task::block_in_place(|| {
                    handle.block_on(self.call_object_rpc_async_inner(
                        target_node_id,
                        call,
                        options,
                        &handle,
                    ))
                });
                result.map_err(DfsPeerTransportError::from_network)
            } else {
                Err(DfsPeerTransportError::new(
                    "mounted SIFS sync peer transport cannot run inside a current-thread Tokio runtime; use the async transport",
                ))
            };
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let result = runtime.block_on(self.call_object_rpc_async_inner(
            target_node_id,
            call,
            options,
            runtime.handle(),
        ));
        runtime.shutdown_timeout(Duration::from_millis(250));
        result.map_err(DfsPeerTransportError::from_network)
    }
}

impl AsyncDfsPeerTransport for MountedSifsPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            self.call_object_rpc_async_inner(target_node_id, call, options, &runtime)
                .await
                .map_err(DfsPeerTransportError::from_network)
        })
    }
}

struct MountedSifsObjectRpcHost;

impl sinomo_core::network::ClusterObjectRpcAsyncHost for MountedSifsObjectRpcHost {}
