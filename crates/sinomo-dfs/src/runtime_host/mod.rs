#[cfg(test)]
use crate::dfs_metadata_prefix_probe_is_verification_only as daemon_dfs_metadata_prefix_probe_is_verification_only;
use crate::{
    DfsNodeAdvertisement, LocalContentStore,
    MountedSifsMutationHandoffRequest as DaemonDfsMountedMutationHandoffRequest,
    MountedSifsMutationHandoffResponse as DaemonDfsMountedMutationHandoffResponse,
    content_services_prefix,
};
use fs2::FileExt as _;
use sinomo_core::cluster::ClusterKvPathsExt as _;
use sinomo_module::{
    ComponentActivityObserver, ComponentEventClient, ComponentHostRole, ComponentObjectRpcTransport,
};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsRuntimePaths {
    pub root: PathBuf,
    pub data_owner_lock: PathBuf,
    host_role: ComponentHostRole,
}

impl DfsRuntimePaths {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        Self::for_host_role(data_dir, ComponentHostRole::Main)
    }

    pub fn for_host_role(data_dir: &SinomoDataDir, host_role: ComponentHostRole) -> Self {
        let root = match host_role {
            ComponentHostRole::Shadow => data_dir.root().join("shadow-daemon"),
            ComponentHostRole::Main | ComponentHostRole::Embedded => data_dir.root().join("daemon"),
        };
        Self {
            data_owner_lock: root.join("data-owner.lock"),
            root,
            host_role,
        }
    }

    #[cfg(test)]
    pub fn with_root(root: PathBuf) -> Self {
        let host_role = if root.file_name().is_some_and(|name| name == "shadow-daemon") {
            ComponentHostRole::Shadow
        } else {
            ComponentHostRole::Main
        };
        Self {
            data_owner_lock: root.join("data-owner.lock"),
            root,
            host_role,
        }
    }

    pub fn data_dir(&self) -> Result<SinomoDataDir, DfsRuntimeError> {
        self.root
            .parent()
            .map(SinomoDataDir::new)
            .ok_or_else(|| DfsRuntimeError::Io {
                path: self.root.display().to_string(),
                detail: "DFS runtime root has no data-directory parent".to_string(),
            })
    }

    pub fn host_role(&self) -> ComponentHostRole {
        self.host_role
    }

    pub fn is_shadow(&self) -> bool {
        self.host_role == ComponentHostRole::Shadow
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DfsRuntimeOptions;

#[derive(Debug, Error)]
pub enum DfsRuntimeError {
    #[error("filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("invalid DFS runtime operation: {detail}")]
    Parse { detail: String },
    #[error(
        "metadata storage preparation at {root:?} remains pending after {processed_records} records"
    )]
    StoragePreparationPending {
        root: PathBuf,
        processed_records: usize,
    },
    #[error("DFS runtime network error at {address}: {detail}")]
    Network { address: String, detail: String },
    #[error("DFS data root is already owned at {lock_file}: {detail}")]
    AlreadyRunning { lock_file: String, detail: String },
    #[error("this DFS runtime has no cluster identity")]
    ClusterOrphan,
    #[error("cluster authority is unavailable: {detail}")]
    ClusterReadOnly { detail: String },
}

impl From<sinomo_core::network::NetworkError> for DfsRuntimeError {
    fn from(error: sinomo_core::network::NetworkError) -> Self {
        match error {
            sinomo_core::network::NetworkError::ClusterOrphan => Self::ClusterOrphan,
            sinomo_core::network::NetworkError::ClusterReadOnly { detail }
            | sinomo_core::network::NetworkError::VerifiedClusterNonLeader { detail, .. } => {
                Self::ClusterReadOnly { detail }
            }
            sinomo_core::network::NetworkError::StorageIo { path, detail }
            | sinomo_core::network::NetworkError::Json { path, detail } => {
                Self::Io { path, detail }
            }
            error => Self::Network {
                address: "component object RPC".to_string(),
                detail: error.to_string(),
            },
        }
    }
}

impl From<sinomo_core::service::ServiceError> for DfsRuntimeError {
    fn from(error: sinomo_core::service::ServiceError) -> Self {
        match error {
            sinomo_core::service::ServiceError::Network(error) => error.into(),
            sinomo_core::service::ServiceError::Kv(error) => error.into(),
            sinomo_core::service::ServiceError::NotClustered(_) => Self::ClusterOrphan,
            error => Self::Parse {
                detail: error.to_string(),
            },
        }
    }
}

impl From<sinomo_core::kv::KvError> for DfsRuntimeError {
    fn from(error: sinomo_core::kv::KvError) -> Self {
        match error {
            sinomo_core::kv::KvError::AuthorityUnavailable { detail } => {
                Self::ClusterReadOnly { detail }
            }
            error => Self::Parse {
                detail: error.to_string(),
            },
        }
    }
}

impl From<crate::DfsError> for DfsRuntimeError {
    fn from(error: crate::DfsError) -> Self {
        match error {
            crate::DfsError::StoragePreparationPending {
                root,
                processed_records,
            } => Self::StoragePreparationPending {
                root,
                processed_records,
            },
            error => Self::Parse {
                detail: error.to_string(),
            },
        }
    }
}

impl From<crate::DfsDistributedError> for DfsRuntimeError {
    fn from(error: crate::DfsDistributedError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<crate::DfsPosixError> for DfsRuntimeError {
    fn from(error: crate::DfsPosixError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<crate::MetadataBackendError> for DfsRuntimeError {
    fn from(error: crate::MetadataBackendError) -> Self {
        match error {
            crate::MetadataBackendError::StoragePreparationPending {
                root,
                processed_records,
            } => Self::StoragePreparationPending {
                root,
                processed_records,
            },
            crate::MetadataBackendError::Io { path, source } => Self::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            },
            error => Self::Parse {
                detail: error.to_string(),
            },
        }
    }
}

impl From<crate::ContentStoreError> for DfsRuntimeError {
    fn from(error: crate::ContentStoreError) -> Self {
        match error {
            crate::ContentStoreError::StoragePreparationPending {
                root,
                processed_records,
            } => Self::StoragePreparationPending {
                root,
                processed_records,
            },
            crate::ContentStoreError::Io { path, source } => Self::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            },
            error => Self::Parse {
                detail: error.to_string(),
            },
        }
    }
}

#[derive(Debug)]
pub struct DfsDataOwnerLock {
    pub(crate) metadata_sync_flights: Arc<crate::service_sync_flights::DfsMetadataSyncFlights>,
    file: File,
    physical_data_root: PathBuf,
    selected_original_data_layout: Option<Arc<crate::DfsSelectedOriginalDataLayout>>,
}

/// Explicit per-native-fixture expectation retained across component reopen.
/// It owns representation identities only, never an OS owner or authority.
#[derive(Clone, Debug, Default)]
pub(crate) struct DfsSelectedOriginalDataFixture {
    #[cfg(any(test, feature = "test-support"))]
    layout: Arc<Mutex<Option<Arc<crate::DfsSelectedOriginalDataLayout>>>>,
}

#[cfg(any(test, feature = "test-support"))]
impl DfsSelectedOriginalDataFixture {
    pub(crate) fn admit(&self, owner: &mut DfsDataOwnerLock) -> Result<(), DfsRuntimeError> {
        let mut expected = self.layout.lock().map_err(|_| DfsRuntimeError::Parse {
            detail: "selected native fixture expectation poisoned".into(),
        })?;
        let actual = owner.admit_selected_original_fixture(expected.clone())?;
        *expected = Some(actual);
        Ok(())
    }

    pub(crate) fn layout(&self) -> Result<Arc<crate::DfsSelectedOriginalDataLayout>, DfsRuntimeError> {
        self.layout.lock().map_err(|_| DfsRuntimeError::Parse {
            detail: "selected native fixture expectation poisoned".into(),
        })?.clone().ok_or_else(|| DfsRuntimeError::Parse {
            detail: "selected native fixture has no admitted actual owner yet".into(),
        })
    }

    pub(crate) fn validate_retained(&self, owner: &DfsDataOwnerLock) -> Result<(), DfsRuntimeError> {
        let expected = self.layout()?;
        let actual = owner.selected_original_data_layout()?.ok_or_else(|| DfsRuntimeError::Parse {
            detail: "selected native fixture cannot adopt an ordinary retained owner".into(),
        })?;
        if !Arc::ptr_eq(&expected, &actual) {
            return Err(DfsRuntimeError::Parse {
                detail: "selected native fixture retained another layout expectation".into(),
            });
        }
        Ok(())
    }
}

/// Read-only platform identity DATA for one actually held file handle.
/// This acquires no owner, opens no path, and grants no storage permission.
pub fn storage_file_identity(file: &File) -> std::io::Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        Ok((metadata.dev(), metadata.ino()))
    }
    #[cfg(windows)]
    {
        // Use the same stable held-handle identity as native journal storage.
        // An unavailable identity stays fail-closed, never a path fallback.
        let (volume, index) = crate::metadata_io::windows_file_identity(file)
            .ok_or_else(|| std::io::Error::other("storage volume/file identity is unavailable"))?;
        Ok((u64::from(volume), index))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        Err(std::io::Error::other(
            "storage file identity is unavailable on this platform",
        ))
    }
}

impl DfsDataOwnerLock {

    pub(crate) fn open_native_content(&self) -> Result<LocalContentStore, DfsRuntimeError> {
        LocalContentStore::new(self.physical_data_root.join("dfs/content")).map_err(Into::into)
    }

    /// The actual retained owner may carry an explicit fixture reader. The
    /// context owns no DATA lock and therefore creates no owner-lifetime cycle.
    pub(crate) fn selected_original_data_layout(
        &self,
    ) -> Result<Option<Arc<crate::DfsSelectedOriginalDataLayout>>, DfsRuntimeError> {
        if let Some(layout) = &self.selected_original_data_layout {
            layout.check_owner(self).map_err(|error| DfsRuntimeError::Parse {
                detail: error.to_string(),
            })?;
        }
        Ok(self.selected_original_data_layout.clone())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn admit_selected_original_fixture(
        &mut self,
        expected: Option<Arc<crate::DfsSelectedOriginalDataLayout>>,
    ) -> Result<Arc<crate::DfsSelectedOriginalDataLayout>, DfsRuntimeError> {
        if self.selected_original_data_layout.is_some() {
            return Err(DfsRuntimeError::Parse {
                detail: "selected native representation was already admitted".into(),
            });
        }
        let layout = match expected {
            Some(layout) => {
                layout.reopen(self).map_err(|error| DfsRuntimeError::Parse {
                    detail: error.to_string(),
                })?;
                layout
            }
            None => crate::DfsSelectedOriginalDataLayout::fresh(self)
                .map_err(|error| DfsRuntimeError::Parse { detail: error.to_string() })?,
        };
        self.selected_original_data_layout = Some(Arc::clone(&layout));
        Ok(layout)
    }

    pub(crate) fn physical_data_root(&self) -> &Path {
        &self.physical_data_root
    }
    /// Identity of the actual retained OS owner lock, used by source-owner
    /// capabilities. It grants no filesystem or cluster authorization.
    pub fn storage_owner_file_identity(&self) -> std::io::Result<(u64, u64)> {
        storage_file_identity(&self.file)
    }

    pub fn acquire(paths: &DfsRuntimePaths) -> Result<Self, DfsRuntimeError> {
        std::fs::create_dir_all(&paths.root).map_err(|source| DfsRuntimeError::Io {
            path: paths.root.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = paths.data_owner_lock.clone();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| DfsRuntimeError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.try_lock_exclusive()
            .map_err(|source| DfsRuntimeError::AlreadyRunning {
                lock_file: path.display().to_string(),
                detail: format!("DFS data directory is already owned: {source}"),
            })?;
        file.set_len(0).map_err(|source| DfsRuntimeError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(format!("pid={}\n", std::process::id()).as_bytes()))
            .and_then(|_| file.flush())
            .map_err(|source| DfsRuntimeError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        let physical_runtime_root =
            paths
                .root
                .canonicalize()
                .map_err(|source| DfsRuntimeError::Io {
                    path: paths.root.display().to_string(),
                    detail: source.to_string(),
                })?;
        let physical_lock =
            physical_runtime_root.join(path.file_name().ok_or_else(|| DfsRuntimeError::Parse {
                detail: "DFS owner lock has no filename".into(),
            })?);
        let held_identity = storage_file_identity(&file).map_err(|source| DfsRuntimeError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        // Probe the separately opened physical name against the SAME retained
        // OS-locked handle; replacement/alias mismatches cannot change owner.
        let named_file = File::open(&physical_lock).map_err(|source| DfsRuntimeError::Io {
            path: physical_lock.display().to_string(),
            detail: source.to_string(),
        })?;
        let named_identity =
            storage_file_identity(&named_file).map_err(|source| DfsRuntimeError::Io {
                path: physical_lock.display().to_string(),
                detail: source.to_string(),
            })?;
        if held_identity != named_identity {
            return Err(DfsRuntimeError::Parse {
                detail: "DFS owner physical path differs from its actual held lock".into(),
            });
        }
        drop(named_file);
        let physical_data_root = physical_runtime_root
            .parent()
            .ok_or_else(|| DfsRuntimeError::Parse {
                detail: "DFS owner runtime root has no physical data-directory parent".into(),
            })?
            .to_path_buf();
        Ok(Self {
            file,
            metadata_sync_flights: Default::default(),
            physical_data_root,
            selected_original_data_layout: None,
        })
    }

    pub(crate) fn owns_backend_root(&self, canonical_backend_root: &Path) -> bool {
        canonical_backend_root.starts_with(&self.physical_data_root)
    }
}

impl Drop for DfsDataOwnerLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub struct DfsRuntimeHost {
    pub cluster: Arc<dyn sinomo_core::service::ClusterAccess>,
    pub object_rpc: Arc<dyn ComponentObjectRpcTransport>,
    pub activity: Option<Arc<dyn ComponentActivityObserver>>,
    pub(crate) response_delivery: Arc<crate::DfsRoutedResponseDelivery>,
    maintenance_notify: Arc<tokio::sync::Notify>,
    event_client: Option<ComponentEventClient>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    shutting_down: crate::DfsCancellationFlag,
    shutdown_notify: tokio::sync::Notify,
    /// Per-mounted-host configuration input for native unit fixtures. This
    /// changes what the real advertisement producers read, never their results.
    #[cfg(test)]
    fixture_local_zone: Mutex<Option<String>>,
}

impl DfsRuntimeHost {
    pub(crate) fn new(
        cluster: Arc<dyn sinomo_core::service::ClusterAccess>,
        object_rpc: Arc<dyn ComponentObjectRpcTransport>,
        activity: Option<Arc<dyn ComponentActivityObserver>>,
    ) -> Self {
        Self {
            cluster,
            object_rpc,
            activity,
            response_delivery: Arc::new(crate::DfsRoutedResponseDelivery::new()),
            maintenance_notify: Arc::new(tokio::sync::Notify::new()),
            event_client: None,
            tasks: Mutex::new(Vec::new()),
            shutting_down: crate::DfsCancellationFlag::new(),
            shutdown_notify: tokio::sync::Notify::new(),
            #[cfg(test)]
            fixture_local_zone: Mutex::new(None),
        }
    }

    pub(crate) fn with_event_client(mut self, events: ComponentEventClient) -> Self {
        self.event_client = Some(events);
        self
    }

    pub(crate) fn own_task(&self, task: tokio::task::JoinHandle<()>) {
        let mut tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        if self.shutting_down.is_cancelled() {
            // Abort does not stop an already-running blocking task. Retain its
            // handle so the same lifecycle drain can join its actual owner.
            task.abort();
        }
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    pub(crate) fn shutdown(&self) {
        self.shutting_down.cancel();
        self.shutdown_notify.notify_waiters();
        self.response_delivery.shutdown();
        if let Ok(tasks) = self.tasks.lock() {
            for task in tasks.iter() {
                task.abort();
            }
        }
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutting_down.is_cancelled()
    }

    pub(crate) async fn wait_for_shutdown(&self) {
        // Register before reading the flag so shutdown cannot be lost between
        // that read and the first poll. The flag also covers children started
        // after shutdown; notification alone is not retained for those children.
        let notified = self.shutdown_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.shutting_down.is_cancelled() {
            notified.await;
        }
    }
}

static DFS_RUNTIME_HOSTS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<DfsRuntimeHost>>>> =
    OnceLock::new();

pub fn register_dfs_runtime_host(
    data_dir: &SinomoDataDir,
    host: &Arc<DfsRuntimeHost>,
) -> Result<(), DfsRuntimeError> {
    DFS_RUNTIME_HOSTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DfsRuntimeError::Network {
            address: "DFS runtime host registry".to_string(),
            detail: "runtime host registry is poisoned".to_string(),
        })?
        .insert(data_dir.root(), Arc::downgrade(host));
    Ok(())
}

pub fn unregister_dfs_runtime_host(data_dir: &SinomoDataDir) {
    if let Ok(mut hosts) = DFS_RUNTIME_HOSTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        hosts.remove(&data_dir.root());
    }
}

// Source reconciliation retains only the actual registered normal task owner;
// it must never select the standalone/test fallback host on a missing registry.
pub(crate) fn registered_dfs_runtime_host(
    data_dir: &SinomoDataDir,
) -> Result<Option<Arc<DfsRuntimeHost>>, DfsRuntimeError> {
    Ok(DFS_RUNTIME_HOSTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DfsRuntimeError::Network {
            address: "DFS runtime host registry".to_string(),
            detail: "runtime host registry is poisoned".to_string(),
        })?
        .get(&data_dir.root())
        .and_then(Weak::upgrade))
}

/// Inspect the actual component host, without constructing a local facade.
/// Absence carries no storage or publication permission.
pub fn registered_component_cluster(
    context: &sinomo_module::ComponentContext,
) -> Result<Option<Arc<dyn sinomo_core::service::ClusterAccess>>, DfsRuntimeError> {
    Ok(registered_dfs_runtime_host(context.data_dir())?.map(|host| Arc::clone(&host.cluster)))
}

fn dfs_runtime_host(data_dir: &SinomoDataDir) -> Result<Arc<DfsRuntimeHost>, DfsRuntimeError> {
    let registered = registered_dfs_runtime_host(data_dir)?;
    if let Some(host) = registered {
        return Ok(host);
    }
    #[cfg(test)]
    {
        Ok(test_dfs_runtime_host(data_dir))
    }
    #[cfg(not(test))]
    {
        Err(DfsRuntimeError::Network {
            address: "DFS runtime host registry".to_string(),
            detail: format!(
                "no live component host is registered for {}",
                data_dir.root().display()
            ),
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
struct TestDfsClusterAccess {
    local_data_dir: SinomoDataDir,
    authority_data_dir: SinomoDataDir,
    peers: Option<Arc<DfsTestClusterPeerBridge>>,
    object_rpc: Option<Arc<dyn ComponentObjectRpcTransport>>,
    /// A voter reads on its own consensus state and forwards a read it cannot
    /// serve over the fixture carrier, as a daemon does. A single-voter
    /// fixture's learners instead read on the sole voter's directory.
    forward_reads: bool,
    full_commit_calls: AtomicUsize,
    compact_commit_calls: AtomicUsize,
}

#[cfg(any(test, feature = "test-support"))]
impl TestDfsClusterAccess {
    fn cluster_paths(&self) -> ClusterPaths {
        ClusterPaths::new(&self.authority_data_dir)
    }
    fn read_host(&self) -> TestDfsClusterQuorumHost {
        TestDfsClusterQuorumHost {
            peers: self.peers.clone(),
            object_rpc: self.object_rpc.clone().filter(|_| self.forward_reads),
        }
    }
    fn local_cluster_paths(&self) -> ClusterPaths {
        ClusterPaths::new(&self.local_data_dir)
    }

    fn state(
        &self,
    ) -> Result<sinomo_core::ClusterBootstrapState, sinomo_core::service::ServiceError> {
        sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths())
            .map_err(sinomo_core::service::ServiceError::Network)
    }
}

#[cfg(any(test, feature = "test-support"))]
struct TestDfsClusterQuorumHost {
    peers: Option<Arc<DfsTestClusterPeerBridge>>,
    object_rpc: Option<Arc<dyn ComponentObjectRpcTransport>>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
pub(crate) struct DfsTestClusterPeerBridge {
    peers: Mutex<BTreeMap<NodeId, (SinomoDataDir, Arc<std::sync::atomic::AtomicBool>)>>,
    /// Nodes a partition cuts off from every other node's KV traffic.
    kv_partitioned: Mutex<std::collections::BTreeSet<NodeId>>,
    fanout: Arc<tokio::sync::Mutex<()>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<Result<(), sinomo_core::network::NetworkError>>>>,
    native_effects:
        Mutex<Vec<tokio::task::JoinHandle<Result<(), sinomo_core::network::NetworkError>>>>,
}

#[cfg(any(test, feature = "test-support"))]
impl DfsTestClusterPeerBridge {
    pub(crate) fn register(
        &self,
        node: NodeId,
        data: SinomoDataDir,
        available: Arc<std::sync::atomic::AtomicBool>,
    ) {
        self.peers
            .lock()
            .expect("native fixture peer registry")
            .insert(node, (data, available));
    }

    /// Cut `node` off from every other node's KV traffic, both ways, or
    /// restore it.
    pub(crate) fn set_kv_partitioned(&self, node: &NodeId, partitioned: bool) {
        let mut cut = self
            .kv_partitioned
            .lock()
            .expect("native fixture KV partitions");
        if partitioned {
            cut.insert(node.clone());
        } else {
            cut.remove(node);
        }
    }

    /// The live KV store of `node`, as seen by the caller with `state`. A
    /// configured member that is not running fails as a routed call to a
    /// dead peer does; only a node outside the membership is unknown. A
    /// partition between the two fails the same way.
    fn paths(
        &self,
        state: &sinomo_core::ClusterBootstrapState,
        node: &NodeId,
    ) -> Result<ClusterPaths, sinomo_core::network::NetworkError> {
        if state.local_node_id != *node {
            let cut = self
                .kv_partitioned
                .lock()
                .expect("native fixture KV partitions");
            if cut.contains(node) || cut.contains(&state.local_node_id) {
                return Err(partitioned_test_peer(node));
            }
        }
        let peers = self.peers.lock().expect("native fixture peer registry");
        match peers.get(node) {
            Some((data, available)) if available.load(Ordering::Acquire) => {
                Ok(ClusterPaths::new(data))
            }
            _ => Err(unavailable_test_kv_peer(state, node)),
        }
    }

    /// Record one target-bound KV call from the caller with `state`, as the
    /// routed client does after every call (`None` is an authenticated
    /// response), so liveness evidence can unblock a request-owned election.
    fn record_target_outcome(
        &self,
        state: &sinomo_core::ClusterBootstrapState,
        target: &NodeId,
        error: Option<&sinomo_core::network::NetworkError>,
    ) {
        let source = self
            .peers
            .lock()
            .expect("native fixture peer registry")
            .get(&state.local_node_id)
            .map(|(data, _)| ClusterPaths::new(data));
        if let Some(source) = source {
            sinomo_core::network::record_cluster_target_rpc_outcome_for_test_carrier(
                &source, state, target, error,
            );
        }
    }

    /// One Raft peer RPC from the caller with `state` to `target`: `receive`
    /// runs as the target's authenticated handler. The caller's bounded wait
    /// never drops admitted native work; case lifecycle owns and joins the
    /// receiver task independently. Any handler reply, a refusal included,
    /// is an authenticated response from the target.
    async fn call_peer<T, F, R>(
        &self,
        state: &sinomo_core::ClusterBootstrapState,
        target: &NodeId,
        operation: &'static str,
        timeout: Duration,
        receive: F,
    ) -> Result<T, sinomo_core::network::NetworkError>
    where
        T: Send + 'static,
        F: FnOnce(ClusterPaths, sinomo_core::cluster::ClusterAuthenticatedPeer) -> R,
        R: std::future::Future<Output = Result<T, sinomo_core::network::NetworkError>>
            + Send
            + 'static,
    {
        let reply = self
            .deliver_to_peer(state, target, operation, timeout, receive)
            .await;
        self.record_target_outcome(state, target, reply.as_ref().err());
        reply?
    }

    async fn deliver_to_peer<T, F, R>(
        &self,
        state: &sinomo_core::ClusterBootstrapState,
        target: &NodeId,
        operation: &'static str,
        timeout: Duration,
        receive: F,
    ) -> Result<Result<T, sinomo_core::network::NetworkError>, sinomo_core::network::NetworkError>
    where
        T: Send + 'static,
        F: FnOnce(ClusterPaths, sinomo_core::cluster::ClusterAuthenticatedPeer) -> R,
        R: std::future::Future<Output = Result<T, sinomo_core::network::NetworkError>>
            + Send
            + 'static,
    {
        let paths = self.paths(state, target)?;
        let source = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
            .ok_or_else(|| sinomo_core::network::NetworkError::UnknownNode {
                node_id: state.local_node_id.clone(),
            })?;
        let authenticated = sinomo_core::cluster::ClusterAuthenticatedPeer {
            node_id: source.node_id.to_string(),
            public_key: source.node_public_key.clone(),
        };
        let deadline = tokio::time::Instant::now() + timeout;
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let received = receive(paths, authenticated);
        let task = tokio::spawn(async move {
            let _ = response_tx.send(received.await);
            Ok(())
        });
        self.native_effects
            .lock()
            .expect("owned fixture native peer tasks")
            .push(task);
        tokio::time::timeout_at(deadline, response_rx)
            .await
            .map_err(|_| {
                sinomo_core::network::NetworkError::timeout(
                    operation,
                    sinomo_core::network::NetworkTimeoutPhase::Overall,
                    timeout,
                    sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                )
            })?
            .map_err(|error| {
                sinomo_core::network::NetworkError::delivery_failed(
                    format!("{operation} receiver"),
                    sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    error.to_string(),
                )
            })
    }

    pub(crate) async fn join(self: &Arc<Self>, timeout: Duration) -> Result<(), String> {
        type Handles = Vec<tokio::task::JoinHandle<Result<(), sinomo_core::network::NetworkError>>>;
        struct Owned<'a> {
            pool: &'a Mutex<Handles>,
            handles: Handles,
        }
        impl Drop for Owned<'_> {
            fn drop(&mut self) {
                self.pool
                    .lock()
                    .expect("owned fixture KV tasks")
                    .append(&mut self.handles);
            }
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for pool in [&self.tasks, &self.native_effects] {
                let mut owned = Owned {
                    pool,
                    handles: std::mem::take(&mut *pool.lock().expect("owned fixture KV tasks")),
                };
                while let Some(handle) = owned.handles.last_mut() {
                    let result = tokio::time::timeout_at(deadline, handle)
                        .await
                        .map_err(|_| {
                            "native fixture KV work did not drain before lifecycle deadline"
                                .to_string()
                        })?;
                    owned.handles.pop();
                    result
                        .map_err(|error| error.to_string())?
                        .map_err(|error| error.to_string())?;
                }
            }
            if self
                .tasks
                .lock()
                .expect("owned fixture fanout tasks")
                .is_empty()
                && self
                    .native_effects
                    .lock()
                    .expect("owned fixture native effects")
                    .is_empty()
            {
                return Ok(());
            }
        }
    }

    pub(crate) fn retain_after_failed_shutdown(self: &Arc<Self>) {
        // Producers may be cancelled. Already admitted native effects retain
        // their futures/handles and their case files until they finish.
        for task in self
            .tasks
            .lock()
            .expect("owned fixture fanout tasks")
            .drain(..)
        {
            task.abort();
        }
        for (_, available) in self
            .peers
            .lock()
            .expect("native fixture peer registry")
            .values()
        {
            available.store(false, Ordering::Release);
        }
        static RETAINED: OnceLock<Mutex<Vec<Arc<DfsTestClusterPeerBridge>>>> = OnceLock::new();
        RETAINED
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .expect("retained failed native fixture work")
            .push(Arc::clone(self));
    }
}

/// What a caller with `state` sees for a KV peer that is not running. A
/// configured member fails as a routed call to a dead peer does: every path
/// is exhausted and nothing was sent. Only a node outside the membership is
/// unknown.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn unavailable_test_kv_peer(
    state: &sinomo_core::ClusterBootstrapState,
    node: &NodeId,
) -> sinomo_core::network::NetworkError {
    if state.members.iter().any(|member| member.node_id == *node) {
        sinomo_core::network::NetworkError::RetryExhausted {
            operation: format!("establish routed session to {node}"),
            attempts: 1,
            delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            detail: "native fixture member is not running".to_string(),
        }
    } else {
        sinomo_core::network::NetworkError::UnknownNode {
            node_id: node.clone(),
        }
    }
}

/// What a caller sees for a peer a partition cuts it off from: the routed
/// session cannot be established and nothing is sent, as for a dead peer.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn partitioned_test_peer(node: &NodeId) -> sinomo_core::network::NetworkError {
    sinomo_core::network::NetworkError::RetryExhausted {
        operation: format!("establish routed session to {node}"),
        attempts: 1,
        delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
        detail: "native fixture partition".to_string(),
    }
}

#[cfg(any(test, feature = "test-support"))]
impl sinomo_core::network::ClusterObjectRpcAsyncHost for TestDfsClusterQuorumHost {
    fn call_node_object_rpc_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a sinomo_core::ClusterBootstrapState,
        target_node_id: &'a NodeId,
        call: sinomo_core::network::ObjectRpcCall,
        options: sinomo_core::network::ClusterObjectRpcOptions,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<'a, sinomo_core::network::ObjectRpcResult>
    {
        Box::pin(async move {
            if let Some(transport) = &self.object_rpc {
                return transport
                    .call_async(target_node_id, call, options, true)
                    .await;
            }
            let mut client =
                sinomo_core::network::ClusterObjectRpcClient::routed_from_fenced_bootstrap_state(
                    self, paths, state,
                )?;
            client
                .call_target(state, target_node_id, call, options)
                .await
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
struct DfsTestHostedClusterKvServiceHost {
    paths: ClusterPaths,
    peers: Arc<DfsTestClusterPeerBridge>,
    object_rpc: Arc<dyn ComponentObjectRpcTransport>,
}

#[cfg(any(test, feature = "test-support"))]
impl DfsTestHostedClusterKvServiceHost {
    fn native_host(&self) -> TestDfsClusterQuorumHost {
        TestDfsClusterQuorumHost {
            peers: Some(Arc::clone(&self.peers)),
            object_rpc: Some(Arc::clone(&self.object_rpc)),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl sinomo_core::cluster::ClusterKvServiceHost for DfsTestHostedClusterKvServiceHost {
    fn authoritative_snapshot<'a>(
        &'a self,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<sinomo_core::kv::KvSnapshot>,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::current();
            let mut state =
                sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &self.paths)
                    .await?;
            let mut host = self.native_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_snapshot_async_with_host(
                &runtime,
                &self.paths,
                &mut state,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(allow_forwarding),
                &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::from)
        })
    }
    fn authoritative_entry<'a>(
        &'a self,
        key: String,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Option<sinomo_core::kv::KvEntry>>,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::current();
            let mut state =
                sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &self.paths)
                    .await?;
            let mut host = self.native_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_entry_async_with_host(
                &runtime,
                &self.paths,
                &mut state,
                key,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(allow_forwarding),
                &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::from)
        })
    }
    fn authoritative_entries<'a>(
        &'a self,
        keys: Vec<String>,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Vec<Option<sinomo_core::kv::KvEntry>>>,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::current();
            let mut state =
                sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &self.paths)
                    .await?;
            let mut host = self.native_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_entries_async_with_host(
                &runtime,
                &self.paths,
                &mut state,
                keys,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(allow_forwarding),
                &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::from)
        })
    }
    fn authoritative_range_prefix<'a>(
        &'a self,
        prefix: String,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Vec<sinomo_core::kv::KvEntry>>,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::current();
            let mut state =
                sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &self.paths)
                    .await?;
            let mut host = self.native_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_range_prefix_async_with_host(
                &runtime,
                &self.paths,
                &mut state,
                prefix,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(allow_forwarding),
                &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::from)
        })
    }
    fn authoritative_complete_range_prefix<'a>(
        &'a self,
        prefix: String,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Vec<sinomo_core::kv::KvEntry>>,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::current();
            let mut state =
                sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &self.paths)
                    .await?;
            let mut host = self.native_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_complete_range_prefix_async_with_host(
                &runtime, &self.paths, &mut state, prefix, sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport().kv_txn_quorum_options(allow_forwarding), &mut host,
            ).await.map_err(sinomo_core::service::ServiceError::from)
        })
    }
    fn request_outcome<'a>(
        &'a self,
        request: sinomo_core::kv::ClusterKvRequestOutcomeRequest,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<
        'a,
        sinomo_core::kv::ClusterKvRequestOutcomeRead,
    > {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::current();
            let mut state =
                sinomo_core::cluster::read_cluster_state_without_kv_async(&runtime, &self.paths)
                    .await?;
            let mut host = self.native_host();
            sinomo_core::cluster::read_cluster_kv_request_outcome_async_with_host(
                &runtime,
                &self.paths,
                &mut state,
                request.request_id,
                false,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(allow_forwarding),
                &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::from)
        })
    }
    fn commit_transaction<'a>(
        &'a self,
        request: sinomo_core::kv::KvTxnRequest,
        allow_forwarding: bool,
    ) -> sinomo_core::cluster::ClusterKvServiceFuture<'a, sinomo_core::kv::ClusterKvTxnRpcResponse>
    {
        Box::pin(async move {
            let mut host = self.native_host();
            sinomo_core::cluster::handle_forwarded_cluster_kv_txn_rpc_request_compact_async_with_host(
                &tokio::runtime::Handle::current(), &self.paths, request,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport().kv_txn_quorum_options(allow_forwarding), &mut host,
            ).await.map_err(sinomo_core::service::ServiceError::from)
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn hosted_cluster_kv_test_factory(
    peers: Arc<DfsTestClusterPeerBridge>,
) -> Result<sinomo_module::StaticComponentFactory, sinomo_module::ComponentError> {
    let mut offer = sinomo_module::ServiceOffer::new(
        "cluster-kv",
        "sinomo.cluster.kv",
        env!("CARGO_PKG_VERSION"),
    );
    for route in sinomo_core::kv::CLUSTER_KV_RPC_ROUTES {
        offer = offer.with_route(*route);
    }
    sinomo_module::StaticComponentFactory::new(
        sinomo_module::ComponentDescriptor::new(
            "native-test-hosted-cluster-kv",
            env!("CARGO_PKG_VERSION"),
        )
        .with_offer(offer),
        move |context, scope| {
            let object_rpc = context.object_rpc_transport().ok_or_else(|| {
                sinomo_module::ComponentError::component(
                    "native-test-hosted-cluster-kv",
                    "actual component carrier is missing",
                )
            })?;
            let host = Arc::new(DfsTestHostedClusterKvServiceHost {
                paths: ClusterPaths::new(context.data_dir()),
                peers: Arc::clone(&peers),
                object_rpc,
            });
            scope.set_services(
                sinomo_core::cluster::register_cluster_kv_service(
                    sinomo_core::service::ServiceRegistryBuilder::default(),
                    host,
                )
                .build(),
            );
            Ok(())
        },
    )
}

#[cfg(any(test, feature = "test-support"))]
impl sinomo_core::cluster::ClusterKvQuorumAsyncHost for TestDfsClusterQuorumHost {
    fn last_local_kv_replication_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a sinomo_core::ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, u64> {
        Box::pin(async move {
            paths
                .kv_paths()
                .locked_last_raft_log_index_async(&state.local_node_id)
                .await
                .map_err(sinomo_core::network::NetworkError::directory)
        })
    }

    fn build_kv_replication_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a sinomo_core::ClusterBootstrapState,
        next_index: u64,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterRaftAppendRequest,
    > {
        Box::pin(async move {
            paths
                .kv_paths()
                .locked_build_raft_append_request_from_next_index_async(state, next_index)
                .await
                .map_err(sinomo_core::network::NetworkError::directory)
        })
    }

    fn send_cluster_kv_replication_to_node_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        state: &'a sinomo_core::ClusterBootstrapState,
        target: &'a NodeId,
        request: &'a sinomo_core::kv::ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterRaftAppendResponse,
    > {
        Box::pin(async move {
            let peers = self.peers.as_ref().ok_or_else(|| {
                sinomo_core::network::NetworkError::UnknownNode {
                    node_id: target.clone(),
                }
            })?;
            let request = request.clone();
            peers
                .call_peer(
                    state,
                    target,
                    "native fixture learner append",
                    timeout,
                    |paths, authenticated| async move {
                        sinomo_core::cluster::handle_cluster_raft_append_request_async(
                            &tokio::runtime::Handle::current(),
                            &paths,
                            request,
                            &authenticated,
                        )
                        .await
                    },
                )
                .await
        })
    }

    fn fan_out_committed_cluster_kv_command_async<'a>(
        &'a mut self,
        paths: ClusterPaths,
        state: sinomo_core::ClusterBootstrapState,
        events: Arc<Vec<sinomo_core::kv::KvEvent>>,
        _targets: Vec<NodeId>,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            let Some(peers) = self.peers.as_ref().cloned() else {
                return Ok(());
            };
            let worker = Arc::clone(&peers);
            let task = tokio::spawn(async move {
                let _wave = worker.fanout.lock().await;
                let mut host = TestDfsClusterQuorumHost {
                    peers: Some(Arc::clone(&worker)),
                    object_rpc: None,
                };
                sinomo_core::cluster::fan_out_committed_cluster_raft_command_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &state,
                    &events,
                    sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                        .committed_fanout_options(),
                    &mut host,
                )
                .await
                .map(|_| ())
            });
            peers
                .tasks
                .lock()
                .expect("owned fixture fanout tasks")
                .push(task);
            Ok(())
        })
    }

    fn catch_up_cluster_kv_peers(
        &mut self,
        paths: &ClusterPaths,
        state: &sinomo_core::ClusterBootstrapState,
        targets: Vec<NodeId>,
    ) {
        let Some(peers) = self.peers.as_ref().cloned() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let worker = Arc::clone(&peers);
        let attempts = sinomo_core::cluster::catch_up_cluster_raft_peers_async_with_factory(
            paths.clone(),
            state.clone(),
            targets,
            runtime.clone(),
            sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport(),
            move || TestDfsClusterQuorumHost {
                peers: Some(Arc::clone(&worker)),
                object_rpc: None,
            },
        );
        let task = runtime.spawn(async move {
            attempts.await;
            Ok(())
        });
        peers
            .tasks
            .lock()
            .expect("owned fixture catch-up tasks")
            .push(task);
    }
}

#[cfg(any(test, feature = "test-support"))]
impl sinomo_core::cluster::ClusterKvCommittedFanoutAsyncHost for TestDfsClusterQuorumHost {
    fn replicate_cluster_kv_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a sinomo_core::ClusterBootstrapState,
        target: &'a NodeId,
        events: &'a [sinomo_core::kv::KvEvent],
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterKvRaftAppendReplicationOutcome,
    > {
        Box::pin(async move {
            sinomo_core::cluster::replicate_cluster_kv_to_node_async_with_host(
                paths,
                state,
                target,
                events,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport(),
                timeout,
                self,
            )
            .await
        })
    }

    fn send_cluster_kv_watch_events_to_node_async<'a>(
        &'a mut self,
        state: &'a sinomo_core::ClusterBootstrapState,
        target: &'a NodeId,
        request: &'a sinomo_core::kv::ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterKvWatchEventsResponse,
    > {
        Box::pin(async move {
            let peers = self.peers.as_ref().ok_or_else(|| {
                sinomo_core::network::NetworkError::UnknownNode {
                    node_id: target.clone(),
                }
            })?;
            let paths = peers.paths(state, target).inspect_err(|error| {
                peers.record_target_outcome(state, target, Some(error));
            })?;
            let started = Instant::now();
            let result = tokio::task::block_in_place(|| {
                let state = sinomo_core::cluster::read_cluster_state(&paths)?;
                sinomo_core::kv::handle_cluster_kv_watch_events(
                    &paths.kv_paths(),
                    &state,
                    request.clone(),
                )
                .map_err(sinomo_core::network::NetworkError::directory)
            });
            if started.elapsed() >= timeout {
                let error = sinomo_core::network::NetworkError::timeout(
                    "native fixture watch-event delivery",
                    sinomo_core::network::NetworkTimeoutPhase::Overall,
                    timeout,
                    sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                );
                peers.record_target_outcome(state, target, Some(&error));
                return Err(error);
            }
            peers.record_target_outcome(state, target, None);
            result
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
impl sinomo_core::cluster::ClusterStateAuthorityReachabilityAsyncHost for TestDfsClusterQuorumHost {
    fn probe_cluster_state_authority_node_async<'a>(
        &'a mut self,
        _state: &'a sinomo_core::ClusterBootstrapState,
        _target_node_id: &'a NodeId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<bool, sinomo_core::network::NetworkError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async { Ok(false) })
    }
}

#[cfg(any(test, feature = "test-support"))]
impl sinomo_core::cluster::ClusterKvTxnQuorumAsyncHost for TestDfsClusterQuorumHost {
    fn request_kv_state_authority_vote_async<'a>(
        &'a mut self,
        state: &'a sinomo_core::ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a sinomo_core::kv::ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterKvStateAuthorityVoteResponse,
    > {
        Box::pin(async move {
            let peers = self.peers.as_ref().ok_or_else(|| {
                sinomo_core::network::NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                }
            })?;
            test_cluster_kv_vote(peers, state, target_node_id, request, timeout).await
        })
    }

    /// Every vote is sent at once, as the daemon does; the round stops
    /// waiting once `required_grants` voters granted this term.
    fn request_kv_state_authority_votes_async<'a>(
        &'a mut self,
        state: &'a sinomo_core::ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &'a sinomo_core::kv::ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
        required_grants: usize,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        Vec<(
            NodeId,
            Result<
                sinomo_core::kv::ClusterKvStateAuthorityVoteResponse,
                sinomo_core::network::NetworkError,
            >,
        )>,
    > {
        use futures_util::StreamExt as _;
        Box::pin(async move {
            let peers =
                self.peers
                    .as_ref()
                    .ok_or_else(|| sinomo_core::network::NetworkError::Io {
                        operation: "concurrent native fixture Raft votes".to_string(),
                        detail: "no native fixture peer bridge".to_string(),
                    })?;
            let mut votes = targets
                .into_iter()
                .map(|target| async move {
                    let result =
                        test_cluster_kv_vote(peers, state, &target, request, timeout).await;
                    (target, result)
                })
                .collect::<futures_util::stream::FuturesUnordered<_>>();
            let mut granted = 0_usize;
            let mut outcomes = Vec::new();
            while let Some((target, result)) = votes.next().await {
                if result.as_ref().is_ok_and(|response| {
                    response.accepted
                        && response.vote.vote_granted
                        && response.vote.term == request.vote.term
                }) {
                    granted = granted.saturating_add(1);
                }
                outcomes.push((target, result));
                if granted >= required_grants {
                    break;
                }
            }
            Ok(outcomes)
        })
    }
}

/// One RequestVote to `target`, handled by its own Raft vote handler.
#[cfg(any(test, feature = "test-support"))]
async fn test_cluster_kv_vote(
    peers: &DfsTestClusterPeerBridge,
    state: &sinomo_core::ClusterBootstrapState,
    target: &NodeId,
    request: &sinomo_core::kv::ClusterKvStateAuthorityVoteRequest,
    timeout: Duration,
) -> Result<sinomo_core::kv::ClusterKvStateAuthorityVoteResponse, sinomo_core::network::NetworkError>
{
    let request = request.clone();
    peers
        .call_peer(
            state,
            target,
            "native fixture vote",
            timeout,
            |paths, authenticated| async move {
                sinomo_core::cluster::handle_cluster_raft_vote_request_async(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    request,
                    &authenticated,
                )
                .await
            },
        )
        .await
}

/// Promote `node` to a cluster-KV Raft voter through the production
/// state-authority lifecycle, run on the KV leader at `leader` over the
/// native peer bridge: the learner proves catch-up, then the quorum-backed
/// configuration change commits. Returns whether anything changed.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn promote_test_cluster_kv_voter(
    runtime: &tokio::runtime::Handle,
    leader: &SinomoDataDir,
    peers: &Arc<DfsTestClusterPeerBridge>,
    node: &NodeId,
) -> Result<bool, sinomo_core::network::NetworkError> {
    let paths = ClusterPaths::new(leader);
    let state = sinomo_core::cluster::read_cluster_state(&paths)?;
    let mut host = TestDfsClusterMemberMutationHost {
        runtime: runtime.clone(),
        peers: Arc::clone(peers),
    };
    sinomo_core::cluster::update_cluster_state_authority_with_host(
        &paths,
        state,
        node.clone(),
        sinomo_core::ClusterMemberRole::Voter,
        time::OffsetDateTime::now_utc(),
        &mut host,
    )
    .map(|update| update.changed)
}

/// Membership mutations on the KV leader, carried by the native peer bridge.
#[cfg(any(test, feature = "test-support"))]
struct TestDfsClusterMemberMutationHost {
    runtime: tokio::runtime::Handle,
    peers: Arc<DfsTestClusterPeerBridge>,
}

#[cfg(any(test, feature = "test-support"))]
impl TestDfsClusterMemberMutationHost {
    fn quorum_host(&self) -> TestDfsClusterQuorumHost {
        TestDfsClusterQuorumHost {
            peers: Some(Arc::clone(&self.peers)),
            object_rpc: None,
        }
    }

    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl sinomo_core::cluster::ClusterMemberMutationHost for TestDfsClusterMemberMutationHost {
    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut sinomo_core::ClusterBootstrapState,
    ) -> Result<
        sinomo_core::kv::KvAuthoritativeRead<sinomo_core::kv::KvSnapshot>,
        sinomo_core::network::NetworkError,
    > {
        let mut host = self.quorum_host();
        let runtime = self.runtime.clone();
        self.block_on(
            sinomo_core::cluster::read_cluster_kv_authoritative_snapshot_async_with_host(
                &runtime,
                paths,
                state,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ),
        )
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut sinomo_core::ClusterBootstrapState,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> Result<sinomo_core::kv::ClusterKvTxnCommitResult, sinomo_core::network::NetworkError> {
        let mut host = self.quorum_host();
        let runtime = self.runtime.clone();
        self.block_on(
            sinomo_core::cluster::commit_cluster_kv_txn_with_quorum_response_async_with_host(
                &runtime,
                paths,
                state,
                request,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ),
        )
    }

    fn prove_cluster_raft_member_caught_up(
        &mut self,
        paths: &ClusterPaths,
        state: &sinomo_core::ClusterBootstrapState,
        node_id: &NodeId,
        required_log_index: u64,
        now: time::OffsetDateTime,
    ) -> Result<sinomo_core::kv::ClusterKvRaftCatchUpProof, sinomo_core::network::NetworkError>
    {
        let mut host = self.quorum_host();
        let timeout = sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true)
            .raft_append_timeout;
        let response = self.block_on(async {
            let request = paths
                .kv_paths()
                .locked_build_raft_append_request_from_next_index_async(state, 1)
                .await
                .map_err(sinomo_core::network::NetworkError::directory)?;
            sinomo_core::cluster::ClusterKvQuorumAsyncHost::send_cluster_kv_replication_to_node_async(
                &mut host, paths, state, node_id, &request, timeout,
            )
            .await
        })?;
        if !response.accepted || response.append.match_index < required_log_index {
            return Err(sinomo_core::network::NetworkError::ClusterReadOnly {
                detail: format!(
                    "learner `{node_id}` caught up only to Raft index {} ({} required): {}",
                    response.append.match_index, required_log_index, response.detail
                ),
            });
        }
        Ok(sinomo_core::kv::ClusterKvRaftCatchUpProof {
            node_id: node_id.clone(),
            term: response.append.term,
            required_log_index,
            observed_match_index: response.append.match_index,
            observed_at: now,
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
impl sinomo_core::service::ClusterAccess for TestDfsClusterAccess {
    fn kv_get_local(
        &self,
        key: &str,
    ) -> Result<sinomo_core::kv::KvLocalRead<Option<KvEntry>>, sinomo_core::service::ServiceError>
    {
        let paths = self.local_cluster_paths();
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .map_err(sinomo_core::service::ServiceError::Network)?;
        sinomo_core::kv::read_cluster_kv_local_entry(&paths.kv_paths(), &state.local_node_id, key)
            .map_err(sinomo_core::service::ServiceError::from)
    }
    fn kv_range_prefix_local(
        &self,
        prefix: &str,
    ) -> Result<sinomo_core::kv::KvLocalRead<Vec<KvEntry>>, sinomo_core::service::ServiceError>
    {
        let paths = self.local_cluster_paths();
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .map_err(sinomo_core::service::ServiceError::Network)?;
        sinomo_core::kv::read_cluster_kv_local_prefix(
            &paths.kv_paths(),
            &state.local_node_id,
            prefix,
        )
        .map_err(sinomo_core::service::ServiceError::from)
    }

    fn kv_get_authoritative_async<'a>(
        &'a self,
        key: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Option<KvEntry>>,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let options =
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true);
            let paths = self.cluster_paths();
            let mut state = self.state()?;
            let runtime = tokio::runtime::Handle::current();
            let mut host = self.read_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_entry_async_with_host(
                &runtime,
                &paths,
                &mut state,
                key.to_string(),
                options,
                &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::Network)
        })
    }

    fn kv_get_authoritative_exact_set_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Vec<Option<KvEntry>>>,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let options =
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true);
            let paths = self.cluster_paths();
            let mut state = self.state()?;
            let runtime = tokio::runtime::Handle::current();
            let mut host = self.read_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_entries_async_with_host(
                &runtime, &paths, &mut state, keys, options, &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::Network)
        })
    }

    fn kv_range_prefix_authoritative_async<'a>(
        &'a self,
        prefix: String,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let options =
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true);
            let paths = self.cluster_paths();
            let mut state = self.state()?;
            let runtime = tokio::runtime::Handle::current();
            let mut host = self.read_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_range_prefix_async_with_host(
                &runtime, &paths, &mut state, prefix, options, &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::Network)
        })
    }

    fn kv_range_prefix_complete_authoritative_async<'a>(
        &'a self,
        prefix: String,
    ) -> sinomo_core::cluster::ClusterMembershipServiceFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>,
    > {
        Box::pin(async move {
            let options =
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true);
            let paths = self.cluster_paths();
            let mut state = self.state()?;
            let runtime = tokio::runtime::Handle::current();
            let mut host = self.read_host();
            sinomo_core::cluster::read_cluster_kv_authoritative_complete_range_prefix_async_with_host(
                &runtime, &paths, &mut state, prefix, options, &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::Network)
        })
    }

    fn kv_request_outcome_authoritative_async<'a>(
        &'a self,
        request_id: sinomo_core::kv::KvRequestId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::ClusterKvRequestOutcomeRead,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let options =
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true);
            let paths = self.cluster_paths();
            let mut state = self.state()?;
            let runtime = tokio::runtime::Handle::current();
            let mut host = self.read_host();
            sinomo_core::cluster::read_cluster_kv_request_outcome_async_with_host(
                &runtime, &paths, &mut state, request_id, true, options, &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::Network)
        })
    }

    fn kv_commit_async<'a>(
        &'a self,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::ClusterKvTxnCommitResult,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        self.full_commit_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            crate::test_runtime::observe_native_kv_request(&self.local_data_dir.root(), &request)?;
            let options =
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true);
            let paths = self.local_cluster_paths();
            let runtime = tokio::runtime::Handle::current();
            let mut host = TestDfsClusterQuorumHost {
                peers: self.peers.clone(),
                object_rpc: self.object_rpc.clone(),
            };
            sinomo_core::cluster::handle_cluster_kv_txn_request_async_with_host(
                &runtime, &paths, request, options, &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::Network)
        })
    }

    fn kv_commit_compact_async<'a>(
        &'a self,
        request: sinomo_core::kv::KvTxnRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::ClusterKvTxnCompactCommitResult,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        self.compact_commit_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let root = self.local_data_dir.root();
            crate::test_runtime::observe_native_kv_request(&root, &request)?;
            let options =
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true);
            let paths = self.local_cluster_paths();
            let runtime = tokio::runtime::Handle::current();
            let mut host = TestDfsClusterQuorumHost {
                peers: self.peers.clone(),
                object_rpc: self.object_rpc.clone(),
            };
            let submitted = request.clone();
            let committed = sinomo_core::cluster::handle_cluster_kv_txn_request_compact_async_with_host(
                &runtime, &paths, request, options, &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::Network)?;
            // A reply hook can lose the reply of a request that committed.
            crate::test_runtime::observe_native_kv_reply(&root, &submitted)?;
            Ok(committed)
        })
    }
}

#[cfg(test)]
struct TestDfsObjectRpcTransport;

#[cfg(test)]
impl ComponentObjectRpcTransport for TestDfsObjectRpcTransport {
    fn call(
        &self,
        _target_node_id: &NodeId,
        _call: ObjectRpcCall,
        _options: sinomo_core::network::ClusterObjectRpcOptions,
        _observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, sinomo_core::network::NetworkError> {
        Err(sinomo_core::network::NetworkError::delivery_failed(
            "DFS test object RPC",
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            "no test peer transport installed",
        ))
    }

    fn call_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: sinomo_core::network::ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ObjectRpcResult, sinomo_core::network::NetworkError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move { self.call(target_node_id, call, options, observe_connectivity) })
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_cluster_access_with_peers_for_runtime(
    data_dir: &SinomoDataDir,
    peers: Option<Arc<DfsTestClusterPeerBridge>>,
) -> Arc<dyn sinomo_core::service::ClusterAccess> {
    test_cluster_access_for_node_on_voter(data_dir, data_dir, peers)
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_cluster_access_for_node_on_voter(
    local_data_dir: &SinomoDataDir,
    authority_data_dir: &SinomoDataDir,
    peers: Option<Arc<DfsTestClusterPeerBridge>>,
) -> Arc<dyn sinomo_core::service::ClusterAccess> {
    test_cluster_access_for_node_on_voter_with_rpc(local_data_dir, authority_data_dir, peers, None)
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_cluster_access_for_node_on_voter_with_rpc(
    local_data_dir: &SinomoDataDir,
    authority_data_dir: &SinomoDataDir,
    peers: Option<Arc<DfsTestClusterPeerBridge>>,
    object_rpc: Option<Arc<dyn ComponentObjectRpcTransport>>,
) -> Arc<dyn sinomo_core::service::ClusterAccess> {
    Arc::new(TestDfsClusterAccess {
        local_data_dir: local_data_dir.clone(),
        authority_data_dir: authority_data_dir.clone(),
        peers,
        object_rpc,
        forward_reads: false,
        full_commit_calls: AtomicUsize::new(0),
        compact_commit_calls: AtomicUsize::new(0),
    })
}

/// A Raft voter's own access: it commits and reads on its own consensus
/// state and forwards over `object_rpc` what it cannot serve itself.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_cluster_access_for_voter_with_rpc(
    data_dir: &SinomoDataDir,
    peers: Option<Arc<DfsTestClusterPeerBridge>>,
    object_rpc: Option<Arc<dyn ComponentObjectRpcTransport>>,
) -> Arc<dyn sinomo_core::service::ClusterAccess> {
    Arc::new(TestDfsClusterAccess {
        local_data_dir: data_dir.clone(),
        authority_data_dir: data_dir.clone(),
        peers,
        object_rpc,
        forward_reads: true,
        full_commit_calls: AtomicUsize::new(0),
        compact_commit_calls: AtomicUsize::new(0),
    })
}

struct DfsRuntimeTaskJoinOwner {
    host: Arc<DfsRuntimeHost>,
    remaining: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for DfsRuntimeTaskJoinOwner {
    fn drop(&mut self) {
        if self.remaining.is_empty() {
            return;
        }
        // Cancellation and every early return restore ownership. Even if the
        // registry was poisoned, do not detach its unfinished task handles.
        let mut tasks = match self.host.tasks.lock() {
            Ok(tasks) => tasks,
            Err(poisoned) => poisoned.into_inner(),
        };
        tasks.append(&mut self.remaining);
    }
}

impl DfsRuntimeHost {
    /// Retain and join the same started tasks before exposing component storage.
    /// The component caller supplies its original absolute close deadline.
    pub(crate) async fn shutdown_and_wait(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        self.shutdown();
        let remaining = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .map_err(|_| "DFS runtime task registry poisoned")?,
        );
        let mut owner = DfsRuntimeTaskJoinOwner {
            host: Arc::clone(self),
            remaining,
        };
        for task in &owner.remaining {
            task.abort();
        }
        loop {
            while let Some(task) = owner.remaining.last_mut() {
                match tokio::time::timeout_at(deadline, task).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if error.is_cancelled() => {}
                    Ok(Err(error)) => {
                        owner.remaining.pop();
                        return Err(format!("DFS runtime task failed: {error}"));
                    }
                    Err(_) => return Err("DFS runtime task did not stop".into()),
                }
                owner.remaining.pop();
            }
            // A stopped task may have registered its already-started blocking
            // child while this wave was being joined. Producers are stopped before
            // this drain, so join every retained wave under the original deadline.
            owner.remaining =
                std::mem::take(&mut *self.tasks.lock().unwrap_or_else(|error| error.into_inner()));
            if owner.remaining.is_empty() {
                break;
            }
            for task in &owner.remaining {
                task.abort();
            }
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn join_registered_runtime_for_test(
    data_dir: &SinomoDataDir,
    timeout: Duration,
) -> Result<(), String> {
    let host = registered_dfs_runtime_host(data_dir)
        .map_err(|error| error.to_string())?
        .ok_or("missing live embedded DFS host")?;
    host.shutdown_and_wait(tokio::time::Instant::now() + timeout)
        .await
}

#[cfg(test)]
fn test_dfs_runtime_host(data_dir: &SinomoDataDir) -> Arc<DfsRuntimeHost> {
    static HOSTS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<DfsRuntimeHost>>>> = OnceLock::new();

    let root = data_dir.root();
    let mut hosts = HOSTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("DFS test runtime host registry must remain available");
    Arc::clone(hosts.entry(root).or_insert_with(|| {
        Arc::new(DfsRuntimeHost::new(
            Arc::new(TestDfsClusterAccess {
                local_data_dir: data_dir.clone(),
                authority_data_dir: data_dir.clone(),
                peers: None,
                object_rpc: None,
                forward_reads: false,
                full_commit_calls: AtomicUsize::new(0),
                compact_commit_calls: AtomicUsize::new(0),
            }),
            Arc::new(TestDfsObjectRpcTransport),
            None,
        ))
    }))
}

fn dfs_runtime_host_for_cluster_paths(
    cluster_paths: &ClusterPaths,
) -> Result<Arc<DfsRuntimeHost>, DfsRuntimeError> {
    let data_root = cluster_paths
        .root
        .parent()
        .ok_or_else(|| DfsRuntimeError::Io {
            path: cluster_paths.root.display().to_string(),
            detail: "cluster root has no data-directory parent".to_string(),
        })?;
    dfs_runtime_host(&SinomoDataDir::new(data_root))
}

fn dfs_cluster_service_error(error: sinomo_core::service::ServiceError) -> KvError {
    match error {
        sinomo_core::service::ServiceError::Kv(error) => error,
        sinomo_core::service::ServiceError::Network(
            sinomo_core::network::NetworkError::ClusterReadOnly { detail }
            | sinomo_core::network::NetworkError::VerifiedClusterNonLeader { detail, .. },
        ) => KvError::AuthorityUnavailable { detail },
        sinomo_core::service::ServiceError::NotClustered(route) => KvError::AuthorityUnavailable {
            detail: format!("service route {route} requires cluster authority"),
        },
        error => KvError::Io {
            path: "component cluster access".to_string(),
            detail: error.to_string(),
        },
    }
}

fn block_on_dfs_cluster_future<T>(
    runtime: &tokio::runtime::Handle,
    future: impl std::future::Future<Output = Result<T, sinomo_core::service::ServiceError>> + Send,
) -> Result<T, KvError>
where
    T: Send,
{
    // This wrapper only boxes the caller's borrowed future. The normal Tokio
    // bridge below specializes on its result type, not each concrete future.
    block_on_dfs_cluster_erased(runtime, Box::pin(future))
}

fn block_on_dfs_cluster_erased<T: Send>(
    runtime: &tokio::runtime::Handle,
    future: std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<T, sinomo_core::service::ServiceError>>
                + Send
                + '_,
        >,
    >,
) -> Result<T, KvError> {
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| runtime.block_on(future))
                .map_err(dfs_cluster_service_error);
        }
        return Err(KvError::AuthorityUnavailable {
            detail:
                "synchronous DFS cluster access cannot run inside a current-thread Tokio runtime"
                    .to_string(),
        });
    }
    runtime.block_on(future).map_err(dfs_cluster_service_error)
}

pub(crate) fn dfs_namespace_event_publication() -> sinomo_module::ComponentEventPublication {
    sinomo_module::ComponentEventPublication::new(
        crate::DFS_NAMESPACE_CHANGED_EVENT_KIND,
        crate::DFS_EVENT_SCHEMA_ID,
        crate::DFS_EVENT_SCHEMA_VERSION,
    )
}

pub(crate) fn dfs_placement_event_publication() -> sinomo_module::ComponentEventPublication {
    sinomo_module::ComponentEventPublication::new(
        crate::DFS_PLACEMENT_CHANGED_EVENT_KIND,
        crate::DFS_EVENT_SCHEMA_ID,
        crate::DFS_EVENT_SCHEMA_VERSION,
    )
}

pub(crate) fn dfs_write_answer_event_publication() -> sinomo_module::ComponentEventPublication {
    sinomo_module::ComponentEventPublication::new(
        crate::DFS_WRITE_ANSWER_UNCLAIMED_EVENT_KIND,
        crate::DFS_WRITE_ANSWER_EVENT_SCHEMA_ID,
        crate::DFS_WRITE_ANSWER_EVENT_SCHEMA_VERSION,
    )
}

/// Announce that an answer to a routed write sent from this node arrived
/// with no registered response owner. A requester outside Main, which
/// registers none, holds that request and sends it again under its id; Main
/// keeps nothing for it. Provenance: the primary that answered, and the
/// route the answer arrived on.
pub(crate) async fn publish_dfs_write_answer_unclaimed(
    client: &ComponentEventClient,
    origin_node_id: &sinomo_core::Id,
    answering_node_id: &sinomo_core::Id,
    request_id: &str,
    object_id: &crate::DfsObjectId,
) {
    let publication = dfs_write_answer_event_publication();
    let payload = match serde_json::to_value(crate::DfsWriteAnswerUnclaimed {
        request_id: request_id.to_owned(),
        object_id: object_id.clone(),
    }) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::warn!(%error, "DFS unclaimed write answer payload could not be encoded");
            return;
        }
    };
    let mut envelope = sinomo_core::EventEnvelope::new(
        sinomo_core::Id::generate(),
        time::OffsetDateTime::now_utc(),
        origin_node_id.clone(),
        sinomo_core::Source::new("component/sinomo-dfs").expect("static DFS event source"),
        sinomo_core::EventKind::new(publication.kind.clone()).expect("static DFS event kind"),
        sinomo_core::Salience::new(0.2).expect("static DFS event salience"),
        sinomo_core::Confidence::new(1.0).expect("static DFS event confidence"),
        sinomo_core::RoutingMetadata::new(
            sinomo_core::RoutingScope::Local,
            sinomo_core::LatencyClass::Reactive,
            sinomo_core::PrivacyClass::Derived,
        ),
    )
    .with_payload(payload);
    envelope.evidence.refs.push(sinomo_core::EvidenceRef::new(
        format!("sinomo-node:{answering_node_id}"),
        sinomo_core::EvidenceKind::Other,
    ));
    envelope
        .links
        .related_to
        .push(crate::DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE.to_string());
    if let Err(error) = client.publish(&publication, envelope).await {
        tracing::warn!(%error, "DFS unclaimed write answer event publication failed");
    }
}

pub(crate) fn dfs_configuration_event_publication() -> sinomo_module::ComponentEventPublication {
    sinomo_module::ComponentEventPublication::new(
        crate::DFS_CONFIGURATION_PUBLISHED_EVENT_KIND,
        crate::DFS_CONFIGURATION_EVENT_SCHEMA_ID,
        crate::DFS_CONFIGURATION_EVENT_SCHEMA_VERSION,
    )
}

/// Announce each change of the published configuration on the component
/// event bus, which the daemon bridges to `/daemon/events/subscribe`. A
/// replacement that only replays unrelated later revisions is not announced.
/// The current publication is announced once at start, so a subscriber that
/// fetched before this task ran still learns about it.
pub(crate) async fn publish_dfs_configuration_events(
    client: ComponentEventClient,
    authority: Arc<DaemonDfsMetadataAuthorityGeneration>,
    origin_node_id: sinomo_core::Id,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let publication = dfs_configuration_event_publication();
    let mut published = authority.subscribe_configuration_snapshot();
    let mut announced: Option<(Arc<sinomo_dfs::DfsConfigurationSnapshot>, sinomo_core::Id)> = None;
    loop {
        let current = Arc::clone(&published.borrow_and_update());
        if announced
            .as_ref()
            .is_none_or(|(previous, _)| !previous.same_configuration_content(&current))
        {
            let event_id = sinomo_core::Id::generate();
            let previous = announced.as_ref().map(|(_, event_id)| event_id.clone());
            publish_dfs_configuration_event(
                &client,
                &publication,
                &origin_node_id,
                &current,
                &event_id,
                previous,
            )
            .await;
            announced = Some((current, event_id));
        }
        tokio::select! {
            changed = published.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

/// Wake the node's repair executor whenever the published configuration
/// changes which shards this node is primary for. A successor serves its new
/// shard from its own replicated index at once; the executor's pass drops the
/// changed shards' timing when it observes the same assignment.
pub(crate) async fn watch_dfs_repair_primary_assignments(
    data_dir: SinomoDataDir,
    authority: Arc<DaemonDfsMetadataAuthorityGeneration>,
    local_node_id: sinomo_core::Id,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let data_root = data_dir.root();
    let mut published = authority.subscribe_configuration_snapshot();
    let mut observed = None;
    loop {
        let current = Arc::clone(&published.borrow_and_update());
        if let Ok(Some(shards)) = current.metadata_shards(&crate::sifs_file_system_id()) {
            let primaries = crate::repair_executor::local_primary_shards(&shards, &local_node_id);
            if crate::repair_executor::repair_primary_assignments_changed(&mut observed, primaries)
            {
                crate::repair_executor::wake_repair_executor(&data_root, "primary_assignment");
            }
        }
        // Any published configuration can bring a lane's authority view up to
        // an assignment a refused dispatch was waiting for.
        crate::repair_executor::repair_executor_authority_changed(&data_root);
        tokio::select! {
            changed = published.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn publish_dfs_configuration_event(
    client: &ComponentEventClient,
    publication: &sinomo_module::ComponentEventPublication,
    origin_node_id: &sinomo_core::Id,
    snapshot: &sinomo_dfs::DfsConfigurationSnapshot,
    event_id: &sinomo_core::Id,
    supersedes: Option<sinomo_core::Id>,
) {
    let stamp = snapshot.publication();
    let payload = match serde_json::to_value(stamp) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::warn!(%error, "DFS configuration event payload could not be encoded");
            return;
        }
    };
    let mut envelope = sinomo_core::EventEnvelope::new(
        event_id.clone(),
        time::OffsetDateTime::now_utc(),
        origin_node_id.clone(),
        sinomo_core::Source::new("component/sinomo-dfs").expect("static DFS event source"),
        sinomo_core::EventKind::new(publication.kind.clone()).expect("static DFS event kind"),
        sinomo_core::Salience::new(0.1).expect("static DFS event salience"),
        sinomo_core::Confidence::new(1.0).expect("static DFS event confidence"),
        sinomo_core::RoutingMetadata::new(
            sinomo_core::RoutingScope::Local,
            sinomo_core::LatencyClass::Reactive,
            sinomo_core::PrivacyClass::Derived,
        ),
    )
    .with_payload(payload);
    // Provenance: the committed revision this publication reflects, and the
    // route that serves its content.
    if let Some(revision) = stamp.revision {
        envelope.evidence.refs.push(sinomo_core::EvidenceRef::new(
            format!("sinomo-kv:revision/{}", revision.0),
            sinomo_core::EvidenceKind::Other,
        ));
    }
    envelope
        .links
        .related_to
        .push(crate::DFS_CONFIGURATION_SNAPSHOT_ROUTE.to_string());
    envelope.links.supersedes.extend(supersedes);
    if let Err(error) = client.publish(publication, envelope).await {
        tracing::warn!(%error, "DFS configuration event publication failed");
    }
}

pub(crate) fn host_cluster_control_subscription() -> sinomo_module::ComponentEventSubscription {
    sinomo_module::ComponentEventSubscription::new(
        sinomo_module::COMPONENT_HOST_CLUSTER_CONTROL_CHANGED_EVENT_KIND,
        sinomo_module::COMPONENT_HOST_EVENT_SCHEMA_ID,
        sinomo_module::COMPONENT_HOST_EVENT_SCHEMA_VERSION,
    )
}

pub(crate) fn host_connectivity_subscription() -> sinomo_module::ComponentEventSubscription {
    sinomo_module::ComponentEventSubscription::new(
        sinomo_module::COMPONENT_HOST_CONNECTIVITY_CHANGED_EVENT_KIND,
        sinomo_module::COMPONENT_HOST_EVENT_SCHEMA_ID,
        sinomo_module::COMPONENT_HOST_EVENT_SCHEMA_VERSION,
    )
}

async fn publish_dfs_change_event(
    client: &ComponentEventClient,
    publication: sinomo_module::ComponentEventPublication,
    origin_node_id: sinomo_core::Id,
    operation: &str,
    paths: Vec<String>,
) {
    let envelope = sinomo_core::EventEnvelope::new(
        sinomo_core::Id::generate(),
        time::OffsetDateTime::now_utc(),
        origin_node_id,
        sinomo_core::Source::new("component/sinomo-dfs").expect("static DFS event source"),
        sinomo_core::EventKind::new(publication.kind.clone()).expect("static DFS event kind"),
        sinomo_core::Salience::new(0.35).expect("static DFS event salience"),
        sinomo_core::Confidence::new(1.0).expect("static DFS event confidence"),
        sinomo_core::RoutingMetadata::new(
            sinomo_core::RoutingScope::Local,
            sinomo_core::LatencyClass::Reactive,
            sinomo_core::PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "operation": operation,
        "paths": paths,
    }));
    if let Err(error) = client.publish(&publication, envelope).await {
        tracing::warn!(%error, "DFS component event publication failed");
    }
}

async fn publish_dfs_namespace_change(
    client: &ComponentEventClient,
    origin_node_id: sinomo_core::Id,
    operation: &str,
    paths: Vec<String>,
) {
    publish_dfs_change_event(
        client,
        dfs_namespace_event_publication(),
        origin_node_id,
        operation,
        paths,
    )
    .await;
}

async fn publish_dfs_placement_change(
    client: &ComponentEventClient,
    origin_node_id: sinomo_core::Id,
    operation: &str,
    paths: Vec<String>,
) {
    publish_dfs_change_event(
        client,
        dfs_placement_event_publication(),
        origin_node_id,
        operation,
        paths,
    )
    .await;
}

type DaemonPaths = DfsRuntimePaths;
type DaemonRunOptions = DfsRuntimeOptions;
type DaemonError = DfsRuntimeError;
type DaemonDataOwnerLock = DfsDataOwnerLock;

#[cfg(test)]
fn shadow_daemon_paths_from_data_dir(data_dir: &SinomoDataDir) -> DfsRuntimePaths {
    DfsRuntimePaths::for_host_role(data_dir, ComponentHostRole::Shadow)
}

#[cfg(test)]
fn build_daemon_async_runtime(label: &str) -> Result<tokio::runtime::Runtime, DfsRuntimeError> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(format!("sinomo-{label}-dfs-test"))
        .enable_all()
        .build()
        .map_err(|source| DfsRuntimeError::Io {
            path: format!("build DFS test runtime {label}"),
            detail: source.to_string(),
        })
}

pub(crate) fn handle_dfs_mounted_mutation_handoff(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    request: crate::MountedSifsMutationHandoffRequest,
) -> Result<crate::MountedSifsMutationHandoffResponse, DfsRuntimeError> {
    handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
        &DfsRuntimePaths::new(data_dir),
        runtime,
        traceparent,
        request,
    )
}

pub fn open_dfs_runtime_service(
    data_dir: &SinomoDataDir,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsRuntimeError> {
    let content = open_dfs_runtime_content_store(data_dir)?;
    open_dfs_runtime_service_with_content(data_dir, runtime, content)
}

pub fn open_dfs_runtime_content_store(
    data_dir: &SinomoDataDir,
) -> Result<LocalContentStore, DfsRuntimeError> {
    // A missing managed live root may be an interrupted owned publication.
    // Only the actual mounted owner can select fresh initialization or ask its
    // preparation provider to recover it. Unmanaged DATA uses LocalContentStore.
    require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(data_dir))?
        .open_native_content()
}

/// sinomo:kv-configuration: Discover configured membership and capability offers through the active native owner's bounded configuration client.
pub(crate) fn registered_dfs_runtime_peer_node_ids(
    control_plane: &dyn crate::DfsControlPlaneKvClient,
    state: &sinomo_core::ClusterBootstrapState,
) -> Result<Vec<NodeId>, DfsRuntimeError> {
    let members = registered_dfs_runtime_members(control_plane)?;
    let services = control_plane.range_prefix(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)?;
    dfs_runtime_peer_node_ids_from_entries(state, &members, &services)
}

/// sinomo:kv-configuration: Read registered membership through the actual native owner's bounded configuration client for service discovery.
fn registered_dfs_runtime_members(
    control_plane: &dyn crate::DfsControlPlaneKvClient,
) -> Result<Vec<sinomo_core::ClusterMemberRecord>, DfsRuntimeError> {
    control_plane
        .range_prefix("/cluster/members/")?
        .into_iter()
        .map(|entry| entry.decode::<sinomo_core::ClusterMemberRecord>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn dfs_runtime_peer_node_ids_from_entries(
    state: &sinomo_core::ClusterBootstrapState,
    members: &[sinomo_core::ClusterMemberRecord],
    services: &[KvEntry],
) -> Result<Vec<NodeId>, DfsRuntimeError> {
    let mut peers = crate::cluster_service::dfs_provider_node_ids_from_current_entries(
        state, members, services,
    )?;
    if !peers.contains(&state.local_node_id) {
        peers.push(state.local_node_id.clone());
        peers.sort();
    }
    Ok(peers)
}

/// Qualify bootstrap with the same registered host and already-held execution
/// owners. Configuration readiness precedes the native publication writer.
pub(crate) async fn bootstrap_publication_execution_provider(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    execution: &Arc<crate::DfsPublicationExecutionOwners>,
    runtime: &tokio::runtime::Handle,
) -> Result<Arc<crate::DfsPublicationExecutionProvider>, crate::DfsError> {
    let error = |message: String| crate::DfsError::Metadata { message };
    if execution.data_root() != data_dir.root()
        || cluster_paths.root != ClusterPaths::new(data_dir).root
    {
        return Err(error(
            "bootstrap publication belongs to another data root".into(),
        ));
    }
    let host = registered_dfs_runtime_host(data_dir)
        .map_err(|source| error(source.to_string()))?
        .ok_or_else(|| {
            error("bootstrap publication has no actual registered runtime host".into())
        })?;
    if host.shutting_down.is_cancelled() {
        return Err(error(
            "bootstrap publication runtime host is stopping".into(),
        ));
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(|source| error(source.to_string()))?;
    let authority = message_host::daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &state.cluster_id,
        &state.local_node_id,
        Some(runtime),
    )?;
    // Bootstrap has written its assignment before this shared configuration
    // join. It retains the existing global admission; no nested permit is taken.
    // The wait ends with the component or the network deadline.
    tokio::select! {
        ready = tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            authority.ensure_write_configuration_ready_async(),
        ) => ready.map_err(|_| error(
            "bootstrap publication configuration deadline elapsed".into(),
        ))??,
        () = host.wait_for_shutdown() => {}
    }
    if host.shutting_down.is_cancelled() {
        return Err(error(
            "bootstrap publication runtime host is stopping".into(),
        ));
    }
    crate::DfsPublicationExecutionProvider::from_execution_owners(
        execution,
        authority,
        runtime.clone(),
    )
    .map_err(crate::DfsError::from)
}

pub fn open_dfs_runtime_service_with_content(
    data_dir: &SinomoDataDir,
    runtime: Option<&tokio::runtime::Handle>,
    content: LocalContentStore,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsRuntimeError> {
    open_dfs_runtime_service_for_use(
        data_dir,
        runtime,
        content,
        DfsRuntimeServiceScope::Caller,
    )
}

/// Open only the exact current owning physical shard for a bounded effect.
/// Uses the same native owner/projection/provider as an ordinary client shell.
pub fn open_dfs_runtime_service_on_shard(
    data_dir: &SinomoDataDir,
    runtime: Option<&tokio::runtime::Handle>,
    expected: &crate::DfsMetadataShardConfig,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsRuntimeError> {
    let content = open_dfs_runtime_content_store(data_dir)?;
    open_dfs_runtime_service_for_use(
        data_dir,
        runtime,
        content,
        DfsRuntimeServiceScope::OwnedShard(expected),
    )
}

/// Run `operation` on this node's own primary copy of `shard`, under the
/// shard's exact mutation admission: the admission a commit on the shard
/// holds. No prepare or commit lands on the shard while it runs, and work on
/// every other shard, a member's answer to another primary's probe included,
/// is admitted beside it. Write-set work that waits on a member runs here, so
/// two primaries probing each other's members never wait on each other.
pub(crate) fn with_daemon_dfs_exact_shard_service<R>(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    shard: &crate::DfsMetadataShardConfig,
    purpose: &str,
    operation: impl FnOnce(&mut NetworkedDfsService<DfsNodeTransport>) -> R,
) -> Result<R, crate::DfsError> {
    let mut service = open_dfs_runtime_service_on_shard(data_dir, Some(runtime), shard)
        .map_err(|error| crate::DfsError::Metadata {
            message: error.to_string(),
        })?;
    let execution = service
        .acquire_prepared_execution(&crate::DfsPreparedOperationScope {
            file_system_id: shard.file_system_id.clone(),
            shard_id: crate::DfsMetadataShardId::new(shard.shard_id.clone()),
            owner_epoch: shard.epoch,
            operation_id: purpose.to_string(),
        })?
        .ok_or_else(|| crate::DfsError::Metadata {
            message: format!(
                "metadata shard {} has no exact mutation admission here",
                shard.shard_id
            ),
        })?;
    let result = operation(&mut service);
    drop(execution);
    Ok(result)
}

/// Open this node's own copy of `shard` for selected local recovery. The node
/// holds the shard as a replica and need not be its primary; the service
/// writes only that copy and does not delegate to Main.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn open_dfs_runtime_service_on_local_shard_copy(
    data_dir: &SinomoDataDir,
    runtime: Option<&tokio::runtime::Handle>,
    shard: &crate::DfsMetadataShardConfig,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsRuntimeError> {
    let content = open_dfs_runtime_content_store(data_dir)?;
    open_dfs_runtime_service_for_use(
        data_dir,
        runtime,
        content,
        DfsRuntimeServiceScope::LocalShardCopy(shard),
    )
}

/// The metadata a runtime service opens.
#[derive(Clone, Copy)]
enum DfsRuntimeServiceScope<'a> {
    /// A caller delegating to its Main keeps no local metadata store.
    Caller,
    /// A bounded Core effect on the exact locally owned primary shard.
    OwnedShard(&'a crate::DfsMetadataShardConfig),
    /// Selected local recovery of this node's own copy of a shard.
    #[cfg(any(test, feature = "test-support"))]
    LocalShardCopy(&'a crate::DfsMetadataShardConfig),
}

fn open_dfs_runtime_service_for_use(
    data_dir: &SinomoDataDir,
    runtime: Option<&tokio::runtime::Handle>,
    content: LocalContentStore,
    scope: DfsRuntimeServiceScope<'_>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsRuntimeError> {
    let effect = match scope {
        DfsRuntimeServiceScope::Caller => None,
        DfsRuntimeServiceScope::OwnedShard(shard) => Some(shard),
        #[cfg(any(test, feature = "test-support"))]
        DfsRuntimeServiceScope::LocalShardCopy(shard) => Some(shard),
    };
    let cluster_paths = ClusterPaths::new(data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let provider_runtime = runtime
        .cloned()
        .or_else(|| tokio::runtime::Handle::try_current().ok())
        .ok_or_else(|| DfsRuntimeError::Parse {
            detail: "native DFS service requires its actual runtime".into(),
        })?;
    let paths = DfsRuntimePaths::new(data_dir);
    let owner = require_daemon_dfs_data_owner_lock(&paths)?;
    let host = registered_dfs_runtime_host(data_dir)?.ok_or_else(|| DfsRuntimeError::Parse {
        detail: "native DFS service has no actual registered runtime host".into(),
    })?;
    let authority = message_host::daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &state.cluster_id,
        &state.local_node_id,
        Some(&provider_runtime),
    )
    .map_err(|error| DfsRuntimeError::Parse {
        detail: error.to_string(),
    })?;
    // Join configuration before any service/backend mutation or effect
    // admission. The wait ends with the component or the network deadline.
    let configuration = block_on_dfs_cluster_future(&provider_runtime, async {
        tokio::select! {
            ready = tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                authority.ready_write_configuration_projection_async(),
            ) => ready
                .map_err(|_| sinomo_core::service::ServiceError::Service(
                    "native DFS service configuration deadline elapsed".into(),
                ))?
                .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string())),
            () = host.wait_for_shutdown() => Err(sinomo_core::service::ServiceError::Service(
                "native DFS service runtime host is stopping".into(),
            )),
        }
    })
    .map_err(|error| DfsRuntimeError::Parse {
        detail: error.to_string(),
    })?
    .ok_or_else(|| DfsRuntimeError::Parse {
        detail: "native DFS service has no current certified metadata topology".into(),
    })?;
    let control_plane =
        daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(&provider_runtime))
            .ok_or_else(|| DfsRuntimeError::Parse {
                detail: "native DFS service has no actual registered control-plane client".into(),
            })?;
    // Use the existing owner client's native configuration projection. Opening
    // another KV-path reader here competes with the live Raft owner and cannot
    // substitute for that component's retained configuration.
    // Peer offers and topology come from the SAME floor-checked configuration
    // returned by readiness. A later refresh cannot erase this read input;
    // actual effects still reacquire and validate their current authority.
    let members = registered_dfs_runtime_members(control_plane.as_ref())?;
    let services = configuration
        .entries(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
        .ok_or_else(|| DfsRuntimeError::Parse {
            detail: "native DFS configuration has no complete service-offer scope".into(),
        })?;
    let peer_node_ids = dfs_runtime_peer_node_ids_from_entries(&state, &members, services)?;
    let file_system_id = effect
        .map(|scope| scope.file_system_id.clone())
        .unwrap_or_else(crate::sifs_file_system_id);
    let prefix = format!("{}/", crate::metadata_shards_prefix(&file_system_id));
    let entries = configuration
        .entries(&prefix)
        .filter(|entries| !entries.is_empty())
        .ok_or_else(|| DfsRuntimeError::Parse {
            detail: "native DFS service has no current certified metadata topology".into(),
        })?;
    let mut shards = entries
        .iter()
        .map(|entry| entry.decode::<crate::DfsMetadataShardConfig>())
        .collect::<Result<Vec<_>, _>>()?;
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    // A shell delegating to its Main keeps no local metadata store; only an
    // exact Core effect or a selected local recovery opens its shard.
    let selected_layout = owner.selected_original_data_layout()?;
    let metadata: Option<Box<dyn crate::DfsMetadataBackend>> = match effect {
        Some(expected) => {
            let held = match scope {
                #[cfg(any(test, feature = "test-support"))]
                DfsRuntimeServiceScope::LocalShardCopy(_) => {
                    crate::service_routed_journal::shard_lists_node(expected, &state.local_node_id)
                }
                _ => expected.primary_node_id == state.local_node_id,
            };
            if !held || !shards.contains(expected) {
                return Err(DfsRuntimeError::Parse {
                    detail: "effect opener has no exact current locally held shard".into(),
                });
            }
            let root = crate::default_dfs_metadata_shards_root(data_dir)
                .join(crate::metadata_keys::key_segment(&expected.shard_id));
            Some(Box::new(
                (if let Some(layout) = &selected_layout {
                    crate::LocalSobjMetadataBackend::new_with_selected_layout(
                        expected.file_system_id.clone(),
                        crate::DfsMetadataShardId::new(expected.shard_id.clone()),
                        root,
                        Arc::clone(layout),
                    )
                } else {
                    crate::LocalSobjMetadataBackend::new(
                        crate::DfsMetadataShardId::new(expected.shard_id.clone()),
                        root,
                    )
                })
                .map_err(|error| DfsRuntimeError::Parse {
                    detail: error.to_string(),
                })?,
            ))
        }
        None => None,
    };
    let publication_provider = crate::DfsPublicationExecutionProvider::from_runtime(
        data_dir.root(),
        owner,
        daemon_dfs_mutation_scheduler(data_dir)?,
        authority,
        provider_runtime.clone(),
    );
    let cancellation = host.shutting_down.clone();
    let transport = if effect.is_none() {
        data_plane::daemon_dfs_client_node_transport(
            state.local_node_id.clone(),
            peer_node_ids,
            host,
            data_dir,
            &provider_runtime,
        )
    } else {
        // Exact Core effects already own their admitted shard. Delegating a
        // nested same-node namespace call here could reenter GLOBAL ownership.
        daemon_dfs_node_transport_on_runtime(
            state.local_node_id.clone(),
            peer_node_ids,
            data_dir,
            &provider_runtime,
        )
    };
    // The same normal host owns eligibility for this Core/client shell. Borrow
    // its shared reactive cache; opening a service neither samples storage nor
    // rebuilds an advisory placement snapshot. Publication permission remains
    // exclusively with the native provider/intent/receipt/kernel guards.
    let service = NetworkedDfsService::new(transport, content)
        .with_cancellation(cancellation)
        .with_node_data_dir(data_dir.clone())
        .with_placement_snapshot_cache(cached_daemon_dfs_placement_snapshot_cache(
            data_dir,
            &state.local_node_id,
        ));
    service
        .publication_provider
        .retain_runtime_provider(publication_provider);
    let service = service.with_boxed_control_plane(control_plane);
    let (Some(metadata), Some(expected)) = (metadata, effect) else {
        // Main alone owns, recovers and rebinds the node's shards.
        return service.into_main_sifs_client().map_err(DfsRuntimeError::from);
    };
    let mut service = service
        .with_boxed_metadata_backend_lazy(metadata)
        .map_err(DfsRuntimeError::from)?;
    #[cfg(any(test, feature = "test-support"))]
    if matches!(scope, DfsRuntimeServiceScope::LocalShardCopy(_)) {
        // Recovery fences this replica copy; it binds no primary writer.
        return Ok(service);
    }
    // A fresh physical shard instance does not inherit another lane's
    // completed membership-handoff binding. Reuse normal mutation admission's
    // exact active-assignment, completion-proof and durable-prefix checks
    // before this bounded effect can acquire its authoritative writer.
    // This newly created lazy shell has no operational projection to
    // rehydrate. Clear/bind only its exact certified physical instance;
    // ordinary mutation/hydration retains all participant recovery checks.
    service.rebind_completed_local_metadata_shard_instance_if_frozen(expected)?;
    // No request shell or maintenance owner drains this effect's committed
    // work, so each edge is emitted as it is recorded, as a served commit's
    // edges are after its request: the publication that carries a commit
    // record to the followers is scheduled for the exact shard.
    let (edge_data_dir, edge_cluster_paths) = (data_dir.clone(), cluster_paths.clone());
    let edge_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: expected.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(expected.shard_id.clone()),
    };
    let edge_runtime = provider_runtime.clone();
    service.set_committed_work_tracker(crate::DfsCommittedWorkTracker::emitting(Arc::new(
        move |work| {
            message_host::emit_daemon_dfs_committed_edges(
                &edge_data_dir,
                &edge_cluster_paths,
                &edge_scope,
                &edge_runtime,
                None,
                work,
                &[],
            );
        },
    )));
    Ok(service)
}

pub fn dfs_runtime_replica_candidates(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
) -> Vec<NodeId> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let advertisements = sinomo_core::kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        content_services_prefix(),
    )
    .map(|entries| {
        entries
            .into_iter()
            .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok())
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    sinomo_sandbox_replica_candidates(local_node_id, advertisements)
}

fn sinomo_sandbox_replica_candidates(
    local_node_id: &NodeId,
    advertisements: Vec<DfsNodeAdvertisement>,
) -> Vec<NodeId> {
    let mut candidates = BTreeSet::from([local_node_id.clone()]);
    candidates.extend(
        advertisements
            .into_iter()
            .filter(|advertisement| advertisement.accepts_content)
            .map(|advertisement| advertisement.node_id),
    );
    candidates.into_iter().collect()
}




#[cfg(test)]
use sinomo_core::ClusterNodeAvailability;
use sinomo_core::kv::{KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse};
#[cfg(test)]
use sinomo_core::kv::{KvSnapshot, KvVersion};
use sinomo_core::local::SinomoDataDir;
#[cfg(test)]
use sinomo_core::network::ClusterConnectivityGraph;
use sinomo_core::network::{ClusterPaths, ObjectRpcCall, ObjectRpcResult};
use sinomo_core::node::NodeId;
use sinomo_dfs::DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL as DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL;
#[cfg(test)]
use sinomo_dfs::DfsFileSystemConfig;
#[cfg(any(test, feature = "test-support"))]
use sinomo_dfs::DfsMetadataBackend;
#[cfg(test)]
#[cfg(test)]
use sinomo_dfs::{
    ContentStoreError, ContentStoreGcOptions, ContentStoreVersionGcOptions,
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse,
    DfsContentRootCatalogCollectionValidation, DfsContentRootCatalogIncompleteReason,
    DfsMaintenanceRunOptions, DfsMessageResponse, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataJournalBatchSender,
    DfsMetadataShardAssignment, DfsMetadataShardStatus, DfsMetadataSnapshotInstallResult,
    DfsMetadataSummary, DfsObjectId, DfsObjectMetadata, DfsObjectVersionRecord,
    DfsOperatorInspector, DfsOperatorRepairView, DfsReplicaReceiptRecord,
    DfsVersion, DfsWriteIntentRecord, DfsWriteIntentStatus, LocalSobjMetadataBackend,
    cluster_dfs_all_file_system_configs, cluster_dfs_all_metadata_shard_head_watermarks,
    cluster_dfs_all_metadata_shard_membership_handoffs, cluster_dfs_all_metadata_shards,
    default_dfs_content_root_catalog_store, default_dfs_metadata_root, distributed_sifs,
    file_system_config_key, metadata_shard_key, sifs_node_files_path,
    };
#[cfg(test)]
use sinomo_dfs::{
    DEFAULT_DFS_CONNECTIVITY_PREFIX as DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX,
    DEFAULT_DFS_ENDPOINT_PREFIX as DAEMON_DEFAULT_DFS_ENDPOINT_PREFIX,
    observe_default_dfs_reconciliation,
};
use sinomo_dfs::{
    DfsCommittedEffects as DaemonDfsMessageCommittedEdges, DfsCommittedWorkTracker,
    DfsCrossShardNamespaceLockRegistry, DfsDataPlaneHost, DfsDataPlaneHostError,
    DfsDataPlaneHostFuture, DfsDataPlaneResponse, DfsDefaultClusterBootstrapOptions,
    DfsFileSystemId, DfsInboundAttachment, DfsMaintenanceExecutionExclusions,
    DfsMaintenanceForegroundPreemption as DaemonDfsMaintenanceForegroundPreemption,
    DfsMaintenanceTickOptions, DfsMaintenanceTickReport, DfsMessageCommand,
    DfsMessageCommittedWork, DfsMessageExecutionScope, DfsMessageProcessResult,
    DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationReport,
    DfsMetadataLaneKey as DaemonDfsMetadataPublishLaneKey, DfsMetadataPublishScheduleOptions,
    DfsMetadataPublishScheduleStart, DfsMetadataSchedulerRegistry, DfsMetadataShardId,
    DfsMetadataShardMembershipHandoff, DfsMutationAdmission as DaemonDfsMutationAdmission,
    DfsMutationLaneKey as DaemonDfsMutationLaneKey,
    DfsMutationScheduler as DaemonDfsMutationScheduler, DfsMutationSchedulerRegistry,
    DfsNodeDetachedSendWorker, DfsNodeTransport, DfsObjectRpcSendOptions, DfsPeerTransport,
    DfsPeerTransportError, DfsResidentMetadataScope as DaemonDfsResidentMetadataScope,
    DfsRuntimeMessageResult as DaemonDfsMessageCommandProcessResult,
    DfsRuntimeMessageServiceLane as DaemonDfsMessageServiceLane, NetworkedDfsService,
    ResidentLocalMetadataShardHandle, ResidentLocalShardedMetadataBackend,
    dfs_message_execution_scope,
    dfs_message_is_read_only_request,
    dfs_metadata_journal_rejection as daemon_dfs_metadata_journal_rejection,
    dfs_metadata_journal_rejection_for as daemon_dfs_metadata_journal_rejection_for,
    dfs_metadata_replication_can_wait_for_maintenance,
    dfs_metadata_snapshot_rejection as daemon_dfs_metadata_snapshot_rejection,
    dfs_metadata_snapshot_rejection_for as daemon_dfs_metadata_snapshot_rejection_for,
    dfs_network_message_kind as daemon_dfs_message_kind,
    dfs_response_committed_visible_mutation as daemon_dfs_response_committed_visible_mutation,
    dfs_visible_mutation_event as daemon_dfs_visible_mutation_event,
    metadata_replica_context_from_authority_projection as daemon_dfs_metadata_replica_context_from_authority_projection,
    metadata_shards_from_authority_projection as daemon_dfs_metadata_shards_from_authority_projection,
    };
#[cfg(test)]
use sinomo_dfs::{
    DefaultDfsCommittedProjectionObserver as DaemonDefaultDfsCommittedProjectionObserver,
    DefaultDfsReconciliationProjection as DaemonDefaultDfsReconciliationProjection,
    ingest_dfs_message_attachment,
};
use sinomo_dfs::{
    DefaultDfsExactRetryGeneration as DaemonDefaultDfsExactRetryGeneration,
    DefaultDfsParkedHandoff as DaemonDefaultDfsParkedHandoff,
    DefaultDfsReconciliationWork as DaemonDefaultDfsReconciliationWork,
    default_dfs_locally_owned_pending_handoff as daemon_default_dfs_locally_owned_pending_handoff,
    default_dfs_reconciliation_local_owns_metadata_primary as daemon_default_dfs_reconciliation_local_owns_metadata_primary,
};
#[cfg(test)]
use sinomo_dfs::{
    DfsMetadataShardConfig, DfsMetadataShardMembershipHandoffProof, DfsPlacementReconciliationJob,
    DfsPlacementReconciliationState,
    dfs_metadata_shard_config_key_is_exact as daemon_dfs_metadata_shard_config_key_is_exact,
    read_local_dfs_metadata_shard_write_freeze,
};
#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::AtomicUsize;
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use time::OffsetDateTime;

// Data-plane object-RPC messages keep the cached service hot by applying
// inbound messages and routed journal batches. A full backend hydrate is a
// defensive catch-up for out-of-band metadata writes, and doing it every few
// seconds makes multi-node DFS writes pay repeated disk scans while a request is
// still in flight.
const DAEMON_DFS_PROMOTION_RETRY_DELAY: Duration = Duration::from_secs(5);
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_MAX_AGE: Duration = Duration::from_secs(14 * 24 * 60 * 60);
#[cfg(test)]
const DAEMON_DFS_REPAIR_MAX_JOBS_PER_TICK: usize =
    sinomo_dfs::DFS_RESIDENT_REPAIR_MAX_JOBS_PER_TICK;
#[cfg(test)]
const DAEMON_DFS_TOMBSTONE_COMPACTION_MIN_AGE: Duration =
    sinomo_dfs::DFS_RESIDENT_TOMBSTONE_COMPACTION_MIN_AGE;
#[cfg(test)]
const DAEMON_DFS_TOMBSTONE_COMPACTION_MAX_RECORDS: u64 =
    sinomo_dfs::DFS_RESIDENT_TOMBSTONE_COMPACTION_MAX_RECORDS;
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_MIN_AGE: Duration =
    sinomo_dfs::DFS_RESIDENT_METADATA_HISTORY_GC_MIN_AGE;
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS: u64 =
    sinomo_dfs::DFS_RESIDENT_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS;
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_MAX_RECORDS: u64 =
    sinomo_dfs::DFS_RESIDENT_METADATA_HISTORY_GC_MAX_RECORDS;
#[cfg(test)]
const DAEMON_DFS_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS: u64 =
    sinomo_dfs::DFS_RESIDENT_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS;
#[cfg(test)]
const DAEMON_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize =
    sinomo_dfs::DFS_RESIDENT_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET;
#[cfg(test)]
const DAEMON_DFS_METADATA_REPLICATION_ATTEMPT_TIMEOUT: Duration =
    sinomo_dfs::DFS_RESIDENT_METADATA_REPLICATION_ATTEMPT_TIMEOUT;
#[cfg(test)]
const DAEMON_DFS_METADATA_REPLICATION_TARGET_TIMEOUT: Duration =
    sinomo_dfs::DFS_RESIDENT_METADATA_REPLICATION_TARGET_TIMEOUT;
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_RECORD_TRIGGER: u64 = 10_000;
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_BYTES_TRIGGER: u64 = 64 * 1024 * 1024;
const DAEMON_DFS_METADATA_AUTHORITY_PREFIX: &str = "/dfs/filesystems/";
const DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS: usize = 3;
// A dirty durable journal lane can outlive the route/authority generation
// which first rejected it. Source-change notifications are the fast path, but
// a missed projection edge must not park replica catch-up for the complete
// five-minute placement-handoff window. Complete lanes remain fully idle;
// only a retained failed lane performs this sparse safety reprobe.
const DAEMON_DFS_METADATA_PUBLISH_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
const DAEMON_DFS_METADATA_DISCOVERY_RETRY_INITIAL: Duration = Duration::from_secs(1);
const DAEMON_DFS_METADATA_DISCOVERY_RETRY_MAX: Duration = Duration::from_secs(60);
const DAEMON_DFS_SMOKE_TIMEOUT: Duration = Duration::from_millis(9_500);
// Returning pages from glibc is process-global allocator work. Defer it until
// DFS mutation traffic is quiet, then hold only the global mutation fence for
// the brief trim instead of waking a complete maintenance generation.
#[cfg(not(test))]
const DAEMON_DFS_ALLOCATOR_CLEANUP_QUIET_PERIOD: Duration = Duration::from_secs(5);
#[cfg(test)]
const DAEMON_DFS_ALLOCATOR_CLEANUP_QUIET_PERIOD: Duration = Duration::from_millis(10);
// A bounded default-layout verification pass is durable control-plane work,
// but it can still scan many settled metadata records after an upgrade. If a
// foreground request arrives during one pass, let the complete burst drain
// before the reactor attempts another global generation.
#[cfg(not(test))]
const DAEMON_DEFAULT_DFS_RECONCILIATION_FOREGROUND_QUIET_PERIOD: Duration = Duration::from_secs(30);
#[cfg(test)]
const DAEMON_DEFAULT_DFS_RECONCILIATION_FOREGROUND_QUIET_PERIOD: Duration =
    Duration::from_millis(10);
// DFS provider discovery is a routing hint, not an authority decision. Keep a
// short process-local lease so every routed metadata lookup does not stat and
// range the materialized cluster KV snapshot. The retained cluster-state Arc
// still invalidates membership changes immediately; service-only changes are
// observed within this bounded lease.
const SINOMO_DFS_ZONE_ENV: &str = "SINOMO_DFS_ZONE";
const DAEMON_DEFAULT_DFS_RECONCILIATION_RETRY_INITIAL: Duration = Duration::from_secs(1);
const DAEMON_DEFAULT_DFS_RECONCILIATION_RETRY_MAX: Duration = Duration::from_secs(60);
#[cfg(test)]
fn daemon_kv_events_cover_revision_range(
    previous_revision: KvRevision,
    current_revision: KvRevision,
    events: &[sinomo_core::kv::KvEvent],
) -> bool {
    current_revision <= previous_revision
        || (events.first().map(|event| event.revision) == Some(previous_revision.next())
            && events.last().map(|event| event.revision) == Some(current_revision)
            && events.windows(2).all(|events| {
                let previous = events[0].revision;
                let current = events[1].revision;
                current == previous || current == previous.next()
            }))
}

fn daemon_dfs_local_zone(_data_dir: &SinomoDataDir) -> Option<String> {
    #[cfg(test)]
    if let Ok(Some(host)) = registered_dfs_runtime_host(_data_dir)
        && let Some(zone) = host
            .fixture_local_zone
            .lock()
            .expect("native fixture zone input")
            .clone()
    {
        return Some(zone);
    }
    std::env::var(SINOMO_DFS_ZONE_ENV)
        .ok()
        .map(|zone| zone.trim().to_string())
        .filter(|zone| !zone.is_empty())
}

/// Bootstrap reconciliation and full maintenance both open and recover the
/// local metadata tables. Keep those mandatory lifecycle paths single-flight
/// so startup cannot retain multiple decoded table images at once.
static DAEMON_DEFAULT_DFS_LIFECYCLE_GATES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>,
> = OnceLock::new();

type DaemonDfsMessageServiceSignature = sinomo_dfs::DfsRuntimeMessageServiceSignature;
type DaemonDfsMessagePeerSnapshot = sinomo_dfs::DfsRuntimeMessagePeerSnapshot;

type DaemonDfsMetadataAuthorityGeneration = sinomo_dfs::DfsMetadataAuthorityRuntime;

#[derive(Clone, Debug)]
pub(crate) struct DaemonDfsMetadataAuthorityPreflight {
    authority: Arc<DaemonDfsMetadataAuthorityGeneration>,
    generation: u64,
}

#[derive(Clone)]
struct DaemonDfsMetadataAuthorityHost {
    data_dir: SinomoDataDir,
    /// The node whose parked namespace work an activation resumes.
    local_node_id: Option<NodeId>,
    cluster_paths: ClusterPaths,
    cluster: Arc<dyn sinomo_core::service::ClusterAccess>,
    task_owner: Weak<DfsRuntimeHost>,
    runtime: tokio::runtime::Handle,
}

impl DaemonDfsMetadataAuthorityHost {
    async fn prepare_metadata_authority_representation(&self) -> Result<(), sinomo_dfs::DfsError> {
        loop {
            let owner =
                self.task_owner
                    .upgrade()
                    .ok_or_else(|| sinomo_dfs::DfsError::Metadata {
                        message: "authority preparation has no registered runtime host".into(),
                    })?;
            let root = self.data_dir.dfs_dir().join("metadata");
            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
            let child = self.runtime.spawn_blocking(move || {
                // The registered provider retains the actual component data
                // owner throughout native representation preparation.
                let result = crate::storage_preparation::prepare_storage_root_before_open(
                    &root,
                    &crate::DfsStoragePreparationScope::MetadataRouting,
                );
                let _ = result_tx.send(result);
            });
            // Cancellation cannot detach a native preparation effect from the
            // component's normal child-task drain.
            owner.own_task(child);
            let result = result_rx
                .await
                .map_err(|source| sinomo_dfs::DfsError::Metadata {
                    message: format!(
                        "authority preparation task stopped before returning: {source}"
                    ),
                })?;
            match result {
                Ok(()) => return Ok(()),
                Err(crate::MetadataBackendError::StoragePreparationPending {
                    processed_records,
                    ..
                }) if processed_records > 0 => {
                    // Positive bounded progress keeps this same cold-read
                    // owner unfinished. The next cut is read only after Ready.
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

impl sinomo_dfs::DfsMetadataAuthorityHost for DaemonDfsMetadataAuthorityHost {
    fn supports_write_configuration_projection(&self) -> bool {
        true
    }

    /// The native Raft bootstrap explicitly permits one unresolved local voter
    /// before its first committed config. This is not a cold/unknown fallback.
    fn allows_unconfigured_single_node_write_configuration(
        &self,
        authority: &sinomo_core::kv::KvAuthorityProof,
        floor: &sinomo_core::kv::KvLocalRead<Option<KvEntry>>,
    ) -> bool {
        let Ok(state) =
            sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&self.cluster_paths)
        else {
            return false;
        };
        matches!(
            authority.basis,
            sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 1,
                required_quorum: 1,
            }
        ) && authority.revision == KvRevision::ZERO
            && sinomo_core::kv::validate_cluster_kv_current_term_authority_proof(authority).is_ok()
            && floor.value.is_none()
            && floor.observed_term == authority.term
            && floor.applied_index >= authority.commit_index
            && floor.revision >= authority.revision
            && floor.known_leader.as_ref() == Some(&state.local_node_id)
    }

    /// sinomo:kv-configuration: Bootstrap or recover the existing shared configuration projection through native bounded current-term ReadIndex.
    fn read_authoritative_configuration_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> sinomo_dfs::DfsMetadataAuthorityReadFuture<'a> {
        #[cfg(test)]
        record_daemon_dfs_test_authoritative_prefix_call(&self.cluster_paths);
        Box::pin(async move {
            if prefix == DAEMON_DFS_METADATA_AUTHORITY_PREFIX {
                self.prepare_metadata_authority_representation().await?;
            }
            self.cluster
                .kv_range_prefix_complete_authoritative_async(prefix.to_string())
                .await
                .map_err(|error| sinomo_dfs::DfsError::Network {
                    message: error.to_string(),
                })
        })
    }

    /// sinomo:kv-configuration: Check the exact resident committed Raft configuration and pending higher-term fence without hydration, durable replay or ReadIndex.
    fn write_configuration_floor(
        &self,
    ) -> Result<sinomo_core::kv::KvLocalRead<Option<KvEntry>>, sinomo_dfs::DfsError> {
        let state =
            sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&self.cluster_paths)
                .map_err(daemon_dfs_network_error)?;
        let floor = sinomo_core::kv::read_cluster_kv_resident_configuration_floor(
            &self.cluster_paths.kv_paths(),
            &state.local_node_id,
        )
        .map_err(sinomo_dfs::DfsError::from)?;
        if let Some(entry) = &floor.value {
            let config: sinomo_core::kv::ClusterKvRaftConfigRecord =
                entry.decode().map_err(sinomo_dfs::DfsError::from)?;
            if config.cluster_id != state.cluster_id {
                return Err(sinomo_dfs::DfsError::Network {
                    message: "resident configuration belongs to a different cluster".into(),
                });
            }
        }
        Ok(floor)
    }

    /// sinomo:kv-configuration: Load the retained DFS configuration projection during bootstrap or committed-delta gap recovery.
    fn read_authoritative_prefix(&self) -> sinomo_dfs::DfsMetadataAuthorityReadFuture<'_> {
        #[cfg(test)]
        record_daemon_dfs_test_authoritative_prefix_call(&self.cluster_paths);
        Box::pin(async move {
            self.prepare_metadata_authority_representation().await?;
            self.cluster
                .kv_range_prefix_complete_authoritative_async(
                    DAEMON_DFS_METADATA_AUTHORITY_PREFIX.to_string(),
                )
                .await
                .map_err(|error| sinomo_dfs::DfsError::Network {
                    message: error.to_string(),
                })
        })
    }

    fn spawn(&self, task: sinomo_dfs::DfsMetadataAuthorityTask) {
        let task = self.runtime.spawn(task);
        match self.task_owner.upgrade() {
            Some(host) => host.own_task(task),
            None => task.abort(),
        }
    }

    fn authority_changed(&self) {
        schedule_daemon_dfs_metadata_publish_discovery_on_runtime(
            self.data_dir.clone(),
            self.cluster_paths.clone(),
            &self.runtime,
            None,
        );
    }

    /// Namespace work parked on an activated shard (a move whose carried
    /// alias targets it) resumes in a maintenance pass: wake one now rather
    /// than leave it to the routine retry of a pass that still has work. A
    /// node with no work parked on these shards runs no pass. A primary copy
    /// the repair executor left out while closed is gained by its next pass.
    fn shard_storage_activated(
        &self,
        activated: &BTreeSet<sinomo_dfs::DfsMetadataShardStorageKey>,
    ) {
        if self.local_node_id.as_ref().is_some_and(|node| {
            crate::maintenance::storage_waits::take_parked_on_activated_shard_storage(
                node, activated,
            )
        }) {
            notify_daemon_dfs_maintenance_work(&self.data_dir);
        }
        crate::repair_executor::repair_executor_shard_storage_activated(
            &self.data_dir.root(),
            activated,
        );
    }
}

fn daemon_dfs_metadata_authority_host(
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Option<Arc<dyn sinomo_dfs::DfsMetadataAuthorityHost>> {
    let runtime = runtime?;
    let data_root = cluster_paths.root.parent()?;
    let data_dir = SinomoDataDir::new(data_root);
    let host = dfs_runtime_host(&data_dir).ok()?;
    let local_node_id = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .ok()
        .map(|state| state.local_node_id);
    Some(Arc::new(DaemonDfsMetadataAuthorityHost {
        data_dir,
        local_node_id,
        cluster_paths: cluster_paths.clone(),
        cluster: Arc::clone(&host.cluster),
        task_owner: Arc::downgrade(&host),
        runtime: runtime.clone(),
    }))
}

type DaemonDfsMessageServiceLaneHandle = sinomo_dfs::DfsRuntimeMessageServiceLaneHandle;

type DaemonDfsDetachedSendRuntime = sinomo_dfs::DfsDetachedSendRuntime;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DaemonDfsMetadataAuthorityGenerationKey {
    cluster_root: PathBuf,
    cluster_id: String,
    local_node_id: String,
}

type DaemonDfsMessageServiceCacheEntry = sinomo_dfs::DfsRuntimeMessageCacheEntry;
type DaemonDfsMaintenanceServiceLane = sinomo_dfs::DfsRuntimeMaintenanceServiceLane;

/// One coalesced replica-materialization generation for an exact shard.
///
/// The active scheduler retains a shard identity and the daemon data-owner
/// capability through every pending pass. Quiet waits own no resident-generation
/// fence. Each materialization reacquires the current canonical resident shard
/// under mutation admission, so topology changes cannot leave this worker using
/// a detached backend. Coalesced ACKs remain owned by this task until replay.
#[derive(Debug)]
struct DaemonDfsReplicaMaterializationLane {
    data_dir: SinomoDataDir,
    runtime: tokio::runtime::Handle,
    lane: DaemonDfsMutationLaneKey,
    mutation_scheduler: Arc<DaemonDfsMutationScheduler>,
    _data_owner_lock: Arc<DaemonDataOwnerLock>,
    allocator_cleanup_needed: AtomicBool,
}

impl DaemonDfsReplicaMaterializationLane {
    fn materialize(
        &self,
        mut shard: sinomo_dfs::ResidentLocalMetadataShardLease,
    ) -> Result<usize, sinomo_dfs::MetadataBackendError> {
        // Even a failed decode/apply can leave a glibc arena at its temporary
        // high-water mark. The lane's final drop transfers one cleanup edge to
        // the scoped allocator lane after every attempted pass.
        self.allocator_cleanup_needed.store(true, Ordering::Release);
        #[cfg(any(test, feature = "test-support"))]
        let _blocking_owner_observation = materialization_test::observe_blocking_owner(
            &self.data_dir,
            &self.lane.shard_id,
            &self._data_owner_lock,
        );
        #[cfg(test)]
        wait_at_daemon_dfs_message_committed_edge_test_hook(&format!(
            "replica-materialization:{}",
            self.data_dir.root().display()
        ));
        #[cfg(any(test, feature = "test-support"))]
        let mut native_boundary = crate::native_shard_trace::current_boundary_interest();
        #[cfg(any(test, feature = "test-support"))]
        let native_before = native_boundary.as_ref().and_then(|_| {
            DfsMetadataBackend::native_held_snapshot_for_scope(
                &*shard,
                &self.lane.file_system_id,
                &self.lane.shard_id,
            )
        });
        let result = shard
            .try_materialize_resident_journal()
            .and_then(|materialized| {
                materialized.ok_or_else(|| sinomo_dfs::MetadataBackendError::Io {
                    path: shard.shard_root().join("journal/write.lock"),
                    source: std::io::Error::from(std::io::ErrorKind::WouldBlock),
                })
            });
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(trace), Ok(materialized)) = (native_boundary.as_mut(), &result) {
            trace.observe(
                crate::native_shard_trace::NativeBoundaryFactsV1::ReplicaMaterializationCompleted {
                    file_system_id: self.lane.file_system_id.clone(),
                    shard_id: self.lane.shard_id.clone(),
                    actual_materialized_entries: *materialized,
                },
                native_before,
                DfsMetadataBackend::native_held_snapshot_for_scope(
                    &*shard,
                    &self.lane.file_system_id,
                    &self.lane.shard_id,
                ),
            );
        }
        #[cfg(any(test, feature = "test-support"))]
        if result.is_err() {
            if let Some(trace) = native_boundary.as_ref() {
                trace.uncertain();
            }
        }
        // The ordinary resident checkout ends before formatting copied facts.
        drop(shard);
        result
    }
}

impl Drop for DaemonDfsReplicaMaterializationLane {
    fn drop(&mut self) {
        if !self.allocator_cleanup_needed.load(Ordering::Acquire) {
            return;
        }
        #[cfg(test)]
        record_daemon_dfs_allocator_cleanup_request(&self.data_dir);
        // This task begins with an async quiet period, so this Drop finishes and
        // releases the lane's data-owner capability before it can acquire
        // global mutation admission. The bounded materialization already
        // released its resident lease and generation admission.
        schedule_daemon_dfs_allocator_cleanup_on_runtime(
            self.data_dir.clone(),
            &self.runtime,
            Arc::clone(&self.mutation_scheduler),
        );
    }
}

#[cfg(test)]
static DAEMON_DFS_TEST_MESSAGE_SERVICE_LANE_OPENS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_AUTHORITATIVE_EXACT_SET_CALLS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_AUTHORITATIVE_PREFIX_CALLS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_ALLOCATOR_CLEANUP_REQUESTS: OnceLock<Mutex<BTreeMap<PathBuf, u64>>> =
    OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_ALLOCATOR_TRIM_COMPLETIONS: OnceLock<Mutex<BTreeMap<PathBuf, u64>>> =
    OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_METADATA_PUBLISH_DISCOVERY_ATTEMPTS: OnceLock<
    Mutex<BTreeMap<PathBuf, (usize, usize)>>,
> = OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_METADATA_PUBLISH_LANE_REQUESTS: OnceLock<
    Mutex<BTreeMap<DaemonDfsMetadataPublishLaneKey, usize>>,
> = OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_REPAIR_MAINTENANCE_EDGE_REQUESTS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_MESSAGE_COMMITTED_EDGE_HOOKS: OnceLock<
    Mutex<BTreeMap<String, Arc<DaemonDfsMessageCommittedEdgeTestHook>>>,
> = OnceLock::new();
static DAEMON_DFS_MUTATION_SCHEDULERS: OnceLock<DfsMutationSchedulerRegistry> = OnceLock::new();
static DAEMON_DFS_METADATA_PUBLISH_SCHEDULERS: OnceLock<
    DfsMetadataSchedulerRegistry<DaemonDfsMetadataPublishLaneKey>,
> = OnceLock::new();
static DAEMON_DFS_METADATA_MATERIALIZATION_SCHEDULERS: OnceLock<
    DfsMetadataSchedulerRegistry<DaemonDfsMetadataPublishLaneKey>,
> = OnceLock::new();
static DAEMON_DFS_ALLOCATOR_CLEANUP_SCHEDULERS: OnceLock<DfsMetadataSchedulerRegistry<PathBuf>> =
    OnceLock::new();
static DAEMON_DFS_METADATA_PUBLISH_DISCOVERY_SCHEDULERS: OnceLock<
    DfsMetadataSchedulerRegistry<PathBuf>,
> = OnceLock::new();
static DAEMON_DFS_METADATA_AUTHORITY_GENERATIONS: OnceLock<
    sinomo_dfs::DfsMetadataAuthorityRuntimeRegistry<DaemonDfsMetadataAuthorityGenerationKey>,
> = OnceLock::new();
static DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS: OnceLock<
    Mutex<BTreeMap<PathBuf, maintenance_host::DaemonDfsReconciliationRequest>>,
> = OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_MAINTENANCE_WAKE_COUNT: AtomicU64 = AtomicU64::new(0);
static DAEMON_DFS_PLACEMENT_AUTHORITY_SESSION_ID: OnceLock<String> = OnceLock::new();
static DAEMON_DFS_DATA_OWNER_LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<DaemonDataOwnerLock>>>> =
    OnceLock::new();
static DAEMON_DFS_SMOKE_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
#[derive(Debug)]
struct DaemonDfsMessageCommittedEdgeTestHook {
    started_tx: std::sync::mpsc::Sender<()>,
    release_rx: Mutex<std::sync::mpsc::Receiver<()>>,
}

/// Publish the process-lifetime main-daemon data lock to DFS blocking work.
/// The registry stores only a weak reference; the control loop remains the
/// owner, while each backend-bearing blocking task upgrades and retains its
/// own `Arc` until the actual synchronous work stops.
pub(crate) fn register_daemon_dfs_data_owner_lock(
    paths: &DaemonPaths,
    lock: &Arc<DaemonDataOwnerLock>,
) -> Result<(), DaemonError> {
    DAEMON_DFS_DATA_OWNER_LOCKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "DFS data-owner lock registry".to_string(),
            detail: "data-owner lock registry is poisoned".to_string(),
        })?
        .insert(paths.root.clone(), Arc::downgrade(lock));
    Ok(())
}

/// Observe the registered weak owner without acquiring or retaining its lease.
#[cfg(test)]
pub(crate) fn test_data_owner_weak_reference(
    paths: &DfsRuntimePaths,
) -> Option<Weak<DaemonDataOwnerLock>> {
    // A Weak observation survives registry removal without retaining the OS
    // owner or granting permission to reopen its DATA.
    DAEMON_DFS_DATA_OWNER_LOCKS
        .get()?
        .lock()
        .ok()?
        .get(&paths.root)
        .cloned()
}

/// Observe the registered weak owner without acquiring or retaining its lease.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn trace_dfs_data_owner_references(paths: &DfsRuntimePaths, checkpoint: &str) {
    if !tracing::enabled!(target: "sinomo.dfs.reconciliation", tracing::Level::TRACE) {
        return;
    }
    let (registry_state, strong_count) = match DAEMON_DFS_DATA_OWNER_LOCKS.get() {
        None => ("uninitialized", None),
        Some(registry) => match registry.lock() {
            Err(_) => ("poisoned", None),
            Ok(owners) => match owners.get(&paths.root) {
                None => ("missing", None),
                Some(owner) => ("present", Some(owner.strong_count())),
            },
        },
    };
    tracing::trace!(target: "sinomo.dfs.reconciliation",
        phase = "data-owner-reference-count", checkpoint,
        data_root = %paths.root.display(), registry_state,
        strong_count = ?strong_count);
}

fn daemon_dfs_data_owner_lock(
    paths: &DaemonPaths,
) -> Result<Option<Arc<DaemonDataOwnerLock>>, DaemonError> {
    Ok(DAEMON_DFS_DATA_OWNER_LOCKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "DFS data-owner lock registry".to_string(),
            detail: "data-owner lock registry is poisoned".to_string(),
        })?
        .get(&paths.root)
        .and_then(Weak::upgrade))
}

/// Cache serialization follows the physical root pinned by the actual held
/// OS owner. Resolving an already-registered lexical/physical alias here uses
/// only that existing weak owner registry, never a fresh filesystem probe or
/// an inferred publication capability. Ownerless DATA slots keep their input
/// key; ordinary runtime access still requires its existing native owner.
pub(crate) fn dfs_runtime_message_cache_root(
    data_root: &Path,
    nonblocking: bool,
) -> Result<PathBuf, DfsRuntimeError> {
    let Some(owners) = DAEMON_DFS_DATA_OWNER_LOCKS.get() else {
        return Ok(data_root.to_path_buf());
    };
    let owners = if nonblocking {
        match owners.try_lock() {
            Ok(owners) => owners,
            Err(std::sync::TryLockError::WouldBlock) => {
                tracing::trace!(target: "sinomo.dfs.cache_diagnostic",
                    data_root = %data_root.display(), branch = "physical-owner-registry",
                    "bounded status cache busy");
                return Err(DfsRuntimeError::Network {
                    address: "DFS message cache root identity".into(),
                    detail: "bounded DFS status unavailable while the resident generation cache is being updated".into(),
                });
            }
            Err(std::sync::TryLockError::Poisoned(_)) => return Err(DfsRuntimeError::Parse {
                detail: "DFS data-owner lock registry is poisoned".into(),
            }),
        }
    } else {
        owners.lock().map_err(|_| DfsRuntimeError::Parse {
            detail: "DFS data-owner lock registry is poisoned".into(),
        })?
    };
    let logical_owner = owners
        .get(&data_root.join("daemon"))
        .and_then(Weak::upgrade);
    let owner = logical_owner.or_else(|| {
        owners
            .values()
            .filter_map(Weak::upgrade)
            .find(|owner| owner.physical_data_root == data_root)
    });
    Ok(owner
        .map(|owner| owner.physical_data_root.clone())
        .unwrap_or_else(|| data_root.to_path_buf()))
}

/// Return the actual retained OS owner for a backend in this runtime. The
/// pathname is an ownership index; only an upgraded live flock is returned.
pub(crate) fn publication_runtime_owner(
    backend_root: &Path,
) -> Result<Option<Arc<DfsDataOwnerLock>>, DfsRuntimeError> {
    let Some(owners) = DAEMON_DFS_DATA_OWNER_LOCKS.get() else {
        return Ok(None);
    };
    let owners = owners.lock().map_err(|_| DfsRuntimeError::Parse {
        detail: "DFS data-owner lock registry is poisoned".into(),
    })?;
    Ok(owners
        .iter()
        .filter_map(|(_, owner)| owner.upgrade())
        .find(|owner| owner.owns_backend_root(backend_root)))
}

pub(crate) fn require_daemon_dfs_data_owner_lock(
    paths: &DaemonPaths,
) -> Result<Arc<DaemonDataOwnerLock>, DaemonError> {
    daemon_dfs_data_owner_lock(paths)?.ok_or_else(|| DaemonError::Parse {
        detail:
            "DFS data-plane access requires the live exclusive main-daemon data-owner capability"
                .to_string(),
    })
}

#[derive(Debug)]
struct DaemonDfsSmokeInFlightGuard;

impl DaemonDfsSmokeInFlightGuard {
    fn acquire() -> Result<Self, DaemonError> {
        DAEMON_DFS_SMOKE_IN_FLIGHT
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| DaemonError::Network {
                address: "DFS smoke".to_string(),
                detail: "DFS smoke is already running".to_string(),
            })
    }
}

impl Drop for DaemonDfsSmokeInFlightGuard {
    fn drop(&mut self) {
        DAEMON_DFS_SMOKE_IN_FLIGHT.store(false, Ordering::Release);
    }
}

pub(crate) type DaemonDfsPendingMaintenanceWork = sinomo_dfs::DfsPendingMaintenanceWork;

pub(crate) fn daemon_dfs_now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
fn daemon_dfs_duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DaemonDfsMetadataHistoryGcSchedule {
    due_record_count: usize,
    next_deadline_unix_nanos: Option<u64>,
}

#[cfg(test)]
fn daemon_dfs_metadata_history_gc_schedule(
    projection: &sinomo_dfs::DfsMetadataHistoryGcReport,
    now_unix_nanos: u64,
    record_trigger: u64,
    bytes_trigger: u64,
    max_history_age: Duration,
) -> DaemonDfsMetadataHistoryGcSchedule {
    let eligible_record_count = projection.eligible_record_count();
    let retained_pressure = projection
        .shards
        .iter()
        .filter(|shard| shard.collection_authorized)
        .any(|shard| {
            let largest_retained_representation = shard
                .estimated_serialized_bytes
                .max(shard.snapshot_size_bytes.unwrap_or(0))
                .max(shard.journal_size_bytes);
            shard.total_record_count >= record_trigger
                || largest_retained_representation >= bytes_trigger
        });
    // Clamp the lifecycle age trigger to the planner's configured recovery
    // window. Count and byte pressure are likewise gated on an age-eligible
    // candidate, so neither path can shorten the retention contract.
    let trigger_age_nanos =
        daemon_dfs_duration_nanos(max_history_age).max(projection.min_history_age_nanos);
    let oldest_age_deadline = projection
        .oldest_eligible_record_unix_nanos
        .map(|retained_at| retained_at.saturating_add(trigger_age_nanos));
    let age_due = eligible_record_count > 0
        && oldest_age_deadline.is_none_or(|deadline| deadline <= now_unix_nanos);
    let pressure_due = eligible_record_count > 0 && retained_pressure;
    if age_due || pressure_due {
        return DaemonDfsMetadataHistoryGcSchedule {
            due_record_count: usize::try_from(eligible_record_count).unwrap_or(usize::MAX),
            next_deadline_unix_nanos: None,
        };
    }

    // A footprint made only of young or recovery-rooted records cannot advance
    // GC. Keep its first eligibility deadline observable without advertising
    // immediately actionable work; otherwise a static high-water condition
    // would launch a no-op lifecycle pass every short maintenance interval.
    let next_deadline_unix_nanos = if eligible_record_count == 0 {
        projection.next_eligible_at_unix_nanos
    } else {
        oldest_age_deadline.filter(|deadline| *deadline > now_unix_nanos)
    };
    DaemonDfsMetadataHistoryGcSchedule {
        due_record_count: 0,
        next_deadline_unix_nanos,
    }
}

#[derive(Debug)]
struct DaemonDfsMaintenanceAdmission {
    _lifecycle_guard: tokio::sync::OwnedMutexGuard<()>,
    _mutation_admission: DaemonDfsMutationAdmission,
}

async fn acquire_daemon_dfs_maintenance_admission(
    data_dir: &SinomoDataDir,
) -> Result<DaemonDfsMaintenanceAdmission, DaemonError> {
    // DFS metadata journals and repair queues are node-local. Cross-node
    // serialization prevents a primary on another daemon from advancing its
    // own shard, so admission is intentionally limited to this data root.
    let lifecycle_guard = daemon_default_dfs_lifecycle_gate(&data_dir.root())?
        .lock_owned()
        .await;
    let mutation_admission = daemon_dfs_mutation_scheduler(data_dir)?
        .admit(&DfsMessageExecutionScope::GlobalExclusive)
        .await?;
    Ok(DaemonDfsMaintenanceAdmission {
        _lifecycle_guard: lifecycle_guard,
        _mutation_admission: mutation_admission,
    })
}

mod browser_host;
mod component_runtime;
mod control_plane;
pub(crate) use browser_host::RuntimeDfsBrowserServiceHost;
pub(crate) use component_runtime::mount_dfs_runtime_tasks;
use control_plane::recover_prior_process_default_dfs_placement_authority_from_observation;
#[cfg(test)]
use control_plane::*;
#[cfg(test)]
pub(crate) use control_plane::{
    test_retained_client_configuration_projection, test_retained_client_configuration_sync,
};
pub(crate) use control_plane::{
    DaemonDfsControlPlaneKvClient, break_glass_recover_dfs_placement_authority_permit,
    inspect_dfs_placement_authority_permit, recover_removed_node_dfs_placement_authority_permit,
};

mod data_plane;
mod native_prefix_return;
pub use native_prefix_return::{
    NativeMetadataPrefixPhysicalOwner, OwnedNativeMetadataPrefixReturn,
};
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod materialization_test;
#[cfg(any(test, feature = "test-support"))]
pub use materialization_test::{DfsTestMaterializationCapture, DfsTestMaterializationPause};
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod deferred_work_test;
#[cfg(any(test, feature = "test-support"))]
pub use deferred_work_test::{DfsTestDeferredResponseCapture, DfsTestDeferredResponsePause};
#[cfg(any(test, feature = "test-support"))]
pub(crate) use data_plane::daemon_dfs_node_transport;
#[cfg(test)]
use data_plane::*;
pub(crate) use data_plane::{
    DaemonDfsDataPlaneHost, daemon_dfs_control_plane_client_on_runtime,
    daemon_dfs_node_transport_on_runtime, daemon_dfs_peer_transports_on_runtime,
    with_daemon_dfs_control_plane_on_runtime,
};

#[cfg(test)]
mod test_support;
#[cfg(test)]
use test_support::*;
mod maintenance_host;
#[cfg(test)]
pub(crate) use maintenance_host::daemon_dfs_maintenance_wake_count;
#[cfg(test)]
use maintenance_host::*;
use maintenance_host::{
    complete_daemon_dfs_maintenance_reconciliation, daemon_dfs_maintenance_execution_exclusions,
    daemon_dfs_offline_node_ids, };
pub(crate) use maintenance_host::{
    daemon_dfs_maintenance_notify, daemon_dfs_maintenance_reconciliation_generation,
    daemon_dfs_pending_reactive_maintenance_work_on_runtime, handle_daemon_dfs_mounted_mutation_handoff_on_runtime, notify_daemon_dfs_maintenance_work,
    notify_daemon_dfs_maintenance_work_for_paths, request_daemon_dfs_maintenance_reconciliation,
    request_daemon_dfs_maintenance_reconciliation_with_namespace_jobs,
    schedule_daemon_dfs_startup_reconciliation,
};

mod placement_environment;
use placement_environment::{
    cached_daemon_dfs_local_content_advertisement, refresh_daemon_dfs_placement_snapshot,
};
pub(crate) use placement_environment::{
    cached_daemon_dfs_placement_snapshot_cache, daemon_dfs_storage_environment_dirty_generation,
    daemon_dfs_storage_environment_notify, notify_daemon_dfs_storage_environment_dirty,
    refresh_daemon_dfs_remote_placement_observation,
    refresh_daemon_dfs_storage_environment_with_resources_on_runtime,
    subscribe_daemon_dfs_remote_placement_observation,
};

mod message_host;
pub(crate) use message_host::*;

mod reconciliation_host;
use reconciliation_host::notify_daemon_default_dfs_reconciliation_replica_progress;
pub(crate) use reconciliation_host::{
    DaemonDefaultDfsReconciliationReactor, run_daemon_default_dfs_reconciliation_reactor,
};
#[cfg(test)]
use reconciliation_host::{
    observe_daemon_default_dfs_reconciliation,
    recover_prior_process_default_dfs_placement_authority_async_on_runtime,
};
mod maintenance_runtime;
#[cfg(test)]
use maintenance_runtime::*;
pub(crate) use maintenance_runtime::{
    DaemonDfsMaintenanceTickOutcome, daemon_dfs_background_network_work_deferred,
    replay_daemon_dfs_mounted_mutation_committed_work_on_runtime,
    run_daemon_dfs_maintenance_tick_in_blocking_task,
    run_daemon_dfs_repair_executor_turn_in_blocking_task,
};
mod peer_verdicts;
#[cfg(any(test, feature = "test-support"))]
pub use peer_verdicts::DfsStandingVerdictGate;
pub(crate) use peer_verdicts::{DfsPeerPathBackWatch, watch_peer_path_back};
mod write_set_keeping;
mod write_set_takeover;
pub(crate) use write_set_takeover::{
    DFS_TAKEOVER_EVENT_KINDS, answer_dfs_takeover_corroboration, dfs_takeover_event_publication,
};
#[cfg(any(test, feature = "test-support"))]
pub(crate) use write_set_takeover::{
    hold_takeover_activation_for_test, stop_takeover_after_promise_for_test,
    takeover_events_for_test, takeover_trace_for_test,
};
#[cfg(test)]
pub(crate) use write_set_takeover::ask_dfs_takeover_corroboration_for_test;
#[cfg(test)]
pub(crate) use write_set_keeping::{JoinRendezvousGuard, hold_joins_until_all_admitted_for_test};
#[cfg(any(test, feature = "test-support"))]
pub(crate) use write_set_keeping::{
    end_test_peer_verification_unconcluded, keeping_events_for_test, keeping_failures_for_test,
    keeping_reachability_interest_for_test, set_test_peer_verdict, set_test_probe_owner,
    settle_test_peer_verification,
    set_write_set_keeping_suspended, suspend_write_set_keeping_unless_enabled,
    test_verification_requests,
};
pub(crate) use write_set_keeping::{
    DFS_WRITE_SET_EVENT_KINDS, dfs_write_set_event_publication, run_dfs_write_set_keeping,
};
mod publish_host;
#[cfg(test)]
use publish_host::{
    daemon_dfs_metadata_publish_discovery_lanes_boxed_async,
    daemon_dfs_metadata_publish_schedule_options, daemon_dfs_metadata_replication_options,
};
pub(crate) use publish_host::{
    notify_daemon_dfs_metadata_publish_route_generation_on_runtime,
    schedule_daemon_dfs_metadata_publish_on_runtime,
};
use publish_host::{
    run_daemon_dfs_smoke_async, schedule_daemon_dfs_allocator_cleanup_on_runtime,
    schedule_daemon_dfs_metadata_publish_discovery_on_runtime,
    schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime,
    schedule_daemon_dfs_replica_materialization_on_runtime,
};
fn daemon_data_dir(paths: &DaemonPaths) -> Result<&std::path::Path, DaemonError> {
    paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon root has no data directory parent".to_string(),
    })
}

#[cfg(test)]
mod tests;

/// Retain the actual component-owned OS lease for a storage preparation service.
/// The registry contains live ownership handles; absence never grants permission.
pub fn retain_component_data_owner(
    context: &sinomo_module::ComponentContext,
) -> Result<Arc<DfsDataOwnerLock>, DfsRuntimeError> {
    let paths = DfsRuntimePaths::for_host_role(context.data_dir(), context.host_role());
    require_daemon_dfs_data_owner_lock(&paths)
}

pub use maintenance_host::queue_namespace_reconciliation_work;
pub(crate) use maintenance_host::release_idle_namespace_reconciliation_work;

#[cfg(test)]
pub(crate) mod native_request_outcome_test;

#[cfg(test)]
mod storage_preparation_error_tests {
    use super::DfsRuntimeError;

    #[test]
    fn metadata_preparation_pending_preserves_the_exact_typed_provider_result() {
        let root = std::path::PathBuf::from("actual-owner/dfs/metadata/root");
        let original = crate::MetadataBackendError::StoragePreparationPending {
            root: root.clone(),
            processed_records: 17,
        };
        let detail = original.to_string();
        let error = DfsRuntimeError::from(crate::DfsError::from(original));
        assert!(matches!(error, DfsRuntimeError::StoragePreparationPending {
            root: actual, processed_records: 17,
        } if actual == root));
        assert!(
            matches!(DfsRuntimeError::from(crate::MetadataBackendError::StoragePreparationPending {
            root: root.clone(), processed_records: 0,
        }), DfsRuntimeError::StoragePreparationPending { root: actual, processed_records: 0 } if actual == root)
        );
        assert!(
            matches!(
                DfsRuntimeError::from(crate::DfsError::Metadata { message: detail }),
                DfsRuntimeError::Parse { .. }
            ),
            "diagnostic text cannot select the Pending lifecycle path"
        );
        assert!(
            matches!(
                DfsRuntimeError::from(crate::MetadataBackendError::Io {
                    path: root,
                    source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
                }),
                DfsRuntimeError::Io { .. }
            ),
            "permanent physical-owner errors must remain errors"
        );
    }
}

#[cfg(test)]
mod content_preparation_error_tests {
    use super::DfsRuntimeError;
    #[test]
    fn content_preparation_pending_retains_its_typed_root_and_count() {
        let root = std::path::PathBuf::from("actual-owner/dfs/content");
        let original = crate::ContentStoreError::StoragePreparationPending {
            root: root.clone(),
            processed_records: 256,
        };
        let detail = original.to_string();
        assert!(
            matches!(DfsRuntimeError::from(crate::DfsError::from(original)),
            DfsRuntimeError::StoragePreparationPending { root: actual, processed_records: 256 } if actual == root)
        );
        assert!(
            matches!(DfsRuntimeError::from(crate::ContentStoreError::StoragePreparationPending {
            root: root.clone(), processed_records: 0,
        }), DfsRuntimeError::StoragePreparationPending { root: actual, processed_records: 0 } if actual == root)
        );
        assert!(
            matches!(
                DfsRuntimeError::from(crate::DfsError::ContentStore { message: detail }),
                DfsRuntimeError::Parse { .. }
            ),
            "text cannot defer host preparation"
        );
        assert!(
            matches!(
                DfsRuntimeError::from(crate::ContentStoreError::Io {
                    path: root,
                    source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
                }),
                DfsRuntimeError::Io { .. }
            ),
            "permanent content IO still aborts preparation"
        );
    }
}
