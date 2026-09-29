use crate::{
    DfsNodeAdvertisement, LocalContentStore,
    MountedSifsMutationHandoffRequest as DaemonDfsMountedMutationHandoffRequest,
    MountedSifsMutationHandoffResponse as DaemonDfsMountedMutationHandoffResponse,
    content_services_prefix, discovered_cluster_dfs_peer_node_ids,
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
        Self::Parse {
            detail: error.to_string(),
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
    file: File,
    cold_content_root: Option<PathBuf>,
}

/// Borrowing the unique, still-unpublished owner prevents concurrent route/task
/// ownership while the content format is activated. Only this module can mint
/// the permit after acquiring the real main data-owner flock.
pub(crate) struct DfsColdContentActivation<'a> {
    content_root: PathBuf,
    owner: &'a mut DfsDataOwnerLock,
}

impl DfsColdContentActivation<'_> {
    pub(crate) fn content_root(&self) -> &Path {
        &self.content_root
    }
}

impl DfsDataOwnerLock {
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
        let cold_content_root = if paths.host_role == ComponentHostRole::Shadow {
            None
        } else {
            Some(paths.data_dir()?.dfs_dir().join("content"))
        };
        Ok(Self {
            file,
            cold_content_root,
        })
    }

    pub(crate) fn activate_content_bindings_before_routes(
        &mut self,
    ) -> Result<(), DfsRuntimeError> {
        let root = self.cold_content_root.clone().ok_or_else(|| DfsRuntimeError::Parse {
            detail: "cold content activation requires an unused exclusive main data-owner capability".into(),
        })?;
        let mut content = LocalContentStore::new(&root)?;
        let mut permit = DfsColdContentActivation {
            content_root: root,
            owner: self,
        };
        content.activate_inline_bindings(&mut permit)?;
        permit.owner.cold_content_root = None;
        Ok(())
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
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    shutting_down: AtomicBool,
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
            tasks: Mutex::new(Vec::new()),
            shutting_down: AtomicBool::new(false),
        }
    }

    pub(crate) fn own_task(&self, task: tokio::task::JoinHandle<()>) {
        if self.shutting_down.load(Ordering::Acquire) {
            task.abort();
            return;
        }
        match self.tasks.lock() {
            Ok(mut tasks) if !self.shutting_down.load(Ordering::Acquire) => {
                tasks.retain(|task| !task.is_finished());
                tasks.push(task);
            }
            _ => task.abort(),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        if let Ok(mut tasks) = self.tasks.lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
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

fn dfs_runtime_host(data_dir: &SinomoDataDir) -> Result<Arc<DfsRuntimeHost>, DfsRuntimeError> {
    let registered = DFS_RUNTIME_HOSTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DfsRuntimeError::Network {
            address: "DFS runtime host registry".to_string(),
            detail: "runtime host registry is poisoned".to_string(),
        })?
        .get(&data_dir.root())
        .and_then(Weak::upgrade);
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

#[cfg(test)]
struct TestDfsClusterAccess {
    data_dir: SinomoDataDir,
    full_commit_calls: AtomicUsize,
    compact_commit_calls: AtomicUsize,
}

#[cfg(test)]
impl TestDfsClusterAccess {
    fn cluster_paths(&self) -> ClusterPaths {
        ClusterPaths::new(&self.data_dir)
    }

    fn state(
        &self,
    ) -> Result<sinomo_core::ClusterBootstrapState, sinomo_core::service::ServiceError> {
        sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths())
            .map_err(sinomo_core::service::ServiceError::Network)
    }

    fn snapshot(&self) -> Result<KvSnapshot, sinomo_core::service::ServiceError> {
        let paths = self.cluster_paths();
        match sinomo_core::cluster::read_cluster_kv_current_snapshot(&paths) {
            Ok(snapshot) => Ok(snapshot),
            Err(sinomo_core::network::NetworkError::ClusterOrphan) => {
                Err(sinomo_core::service::ServiceError::NotClustered(
                    "DFS test cluster access".to_string(),
                ))
            }
            Err(_) => Ok(KvSnapshot {
                revision: KvRevision::default(),
                compacted_revision: KvRevision::default(),
                entries: Vec::new(),
                leases: Vec::new(),
                events: Vec::new(),
                committed_requests: Vec::new(),
                expired_requests: Vec::new(),
            }),
        }
    }
}

#[cfg(test)]
struct TestDfsClusterQuorumHost;

#[cfg(test)]
fn test_dfs_cluster_transport_unavailable<'a, T: 'a>(
    operation: &'static str,
) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, T> {
    Box::pin(async move {
        Err(sinomo_core::network::NetworkError::ClusterReadOnly {
            detail: format!("DFS single-voter test unexpectedly attempted {operation}"),
        })
    })
}

#[cfg(test)]
impl sinomo_core::network::ClusterObjectRpcAsyncHost for TestDfsClusterQuorumHost {}

#[cfg(test)]
impl sinomo_core::cluster::ClusterKvQuorumAsyncHost for TestDfsClusterQuorumHost {
    fn last_local_kv_replication_index_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a sinomo_core::ClusterBootstrapState,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<'a, u64> {
        test_dfs_cluster_transport_unavailable("a remote replication-index read")
    }

    fn build_kv_replication_request_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a sinomo_core::ClusterBootstrapState,
        _next_index: u64,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterRaftAppendRequest,
    > {
        test_dfs_cluster_transport_unavailable("a remote replication request")
    }

    fn send_cluster_kv_replication_to_node_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a sinomo_core::ClusterBootstrapState,
        _target_node_id: &'a NodeId,
        _request: &'a sinomo_core::kv::ClusterRaftAppendRequest,
        _timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterRaftAppendResponse,
    > {
        test_dfs_cluster_transport_unavailable("remote KV replication")
    }
}

#[cfg(test)]
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

#[cfg(test)]
impl sinomo_core::cluster::ClusterKvTxnQuorumAsyncHost for TestDfsClusterQuorumHost {
    fn request_kv_state_authority_vote_async<'a>(
        &'a mut self,
        _state: &'a sinomo_core::ClusterBootstrapState,
        _target_node_id: &'a NodeId,
        _request: &'a sinomo_core::kv::ClusterKvStateAuthorityVoteRequest,
        _timeout: Duration,
    ) -> sinomo_core::network::ClusterMembershipRpcFuture<
        'a,
        sinomo_core::kv::ClusterKvStateAuthorityVoteResponse,
    > {
        test_dfs_cluster_transport_unavailable("a remote state-authority vote")
    }
}

#[cfg(test)]
impl sinomo_core::service::ClusterAccess for TestDfsClusterAccess {
    fn kv_get_local(
        &self,
        key: &str,
    ) -> Result<sinomo_core::kv::KvLocalRead<Option<KvEntry>>, sinomo_core::service::ServiceError>
    {
        let snapshot = self.snapshot()?;
        let state = self.state()?;
        Ok(sinomo_core::kv::KvLocalRead {
            value: snapshot.get(key).cloned(),
            revision: snapshot.revision,
            observed_term: 1,
            applied_index: snapshot.revision.0,
            known_leader: Some(state.local_node_id),
        })
    }

    fn kv_range_prefix_local(
        &self,
        prefix: &str,
    ) -> Result<sinomo_core::kv::KvLocalRead<Vec<KvEntry>>, sinomo_core::service::ServiceError>
    {
        let snapshot = self.snapshot()?;
        let state = self.state()?;
        let value = snapshot
            .range_prefix_limited(prefix, sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
            .map_err(sinomo_core::service::ServiceError::Kv)?;
        Ok(sinomo_core::kv::KvLocalRead {
            value,
            revision: snapshot.revision,
            observed_term: 1,
            applied_index: snapshot.revision.0,
            known_leader: Some(state.local_node_id),
        })
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
            let paths = self.cluster_paths();
            let mut state = self.state()?;
            let runtime = tokio::runtime::Handle::current();
            let mut host = TestDfsClusterQuorumHost;
            sinomo_core::cluster::read_cluster_kv_authoritative_entry_async_with_host(
                &runtime,
                &paths,
                &mut state,
                key.to_string(),
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
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
            let paths = self.cluster_paths();
            let mut state = self.state()?;
            let runtime = tokio::runtime::Handle::current();
            let mut host = TestDfsClusterQuorumHost;
            sinomo_core::cluster::read_cluster_kv_authoritative_entries_async_with_host(
                &runtime,
                &paths,
                &mut state,
                keys,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
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
            let paths = self.cluster_paths();
            let mut state = self.state()?;
            let runtime = tokio::runtime::Handle::current();
            let mut host = TestDfsClusterQuorumHost;
            sinomo_core::cluster::read_cluster_kv_authoritative_range_prefix_async_with_host(
                &runtime,
                &paths,
                &mut state,
                prefix,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
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
            let paths = self.cluster_paths();
            let runtime = tokio::runtime::Handle::current();
            let mut host = TestDfsClusterQuorumHost;
            sinomo_core::cluster::handle_cluster_kv_txn_request_async_with_host(
                &runtime,
                &paths,
                request,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
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
            let paths = self.cluster_paths();
            let runtime = tokio::runtime::Handle::current();
            let mut host = TestDfsClusterQuorumHost;
            sinomo_core::cluster::handle_cluster_kv_txn_request_compact_async_with_host(
                &runtime,
                &paths,
                request,
                sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            )
            .await
            .map_err(sinomo_core::service::ServiceError::Network)
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
                data_dir: data_dir.clone(),
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
    let future = Box::pin(future);
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
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    open_dfs_runtime_service_with_content(data_dir, runtime, content)
}

pub fn open_dfs_runtime_content_store(
    data_dir: &SinomoDataDir,
) -> Result<LocalContentStore, DfsRuntimeError> {
    LocalContentStore::new(data_dir.dfs_dir().join("content")).map_err(Into::into)
}

pub fn open_dfs_runtime_service_with_content(
    data_dir: &SinomoDataDir,
    runtime: Option<&tokio::runtime::Handle>,
    content: LocalContentStore,
) -> Result<NetworkedDfsService<DfsNodeTransport>, DfsRuntimeError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let peer_node_ids = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)?;
    let transport = match runtime {
        Some(runtime) => daemon_dfs_node_transport_on_runtime(
            state.local_node_id.clone(),
            peer_node_ids,
            data_dir,
            runtime,
        ),
        None => daemon_dfs_node_transport(state.local_node_id.clone(), peer_node_ids),
    };
    let service = NetworkedDfsService::new(transport, content);
    let service = with_daemon_owned_dfs_control_plane_on_runtime(service, &cluster_paths, runtime);
    let metadata = open_daemon_dfs_metadata_backend(data_dir, &cluster_paths)?;
    let mut service = service
        .with_boxed_metadata_backend_lazy(metadata)
        .map_err(DfsRuntimeError::from)?;
    service.bootstrap_distributed_sifs()?;
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
#[cfg(test)]
use sinomo_dfs::LocalDfsRepairQueueStore;
#[cfg(test)]
use sinomo_dfs::{
    ContentStoreError, ContentStoreGcOptions, ContentStoreVersionGcOptions,
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsConflictRecord, DfsConflictStatus,
    DfsContentRootCatalogCollectionValidation, DfsContentRootCatalogIncompleteReason,
    DfsMaintenanceRunOptions, DfsMessageResponse, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataJournalBatchSender,
    DfsMetadataPeerTransportError, DfsMetadataShardAssignment, DfsMetadataShardStatus,
    DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult,
    DfsMetadataSnapshotInstallSender, DfsMetadataSummary, DfsObjectId, DfsObjectMetadata,
    DfsObjectVersionRecord, DfsOperatorInspector, DfsOperatorRepairView, DfsRepairAttemptSnapshot,
    DfsReplicaReceiptRecord, DfsVersion, DfsWriteIntentRecord, DfsWriteIntentStatus,
    LocalSobjMetadataBackend, cluster_dfs_all_file_system_configs,
    cluster_dfs_all_metadata_shard_head_watermarks,
    cluster_dfs_all_metadata_shard_membership_handoffs, cluster_dfs_all_metadata_shards,
    default_dfs_content_root_catalog_store, default_dfs_metadata_root, distributed_sifs,
    distributed_sifs_bootstrap_objects, file_system_config_key, metadata_shard_key,
    sifs_node_files_path, write_dfs_repair_attempt_snapshot,
};
#[cfg(test)]
use sinomo_dfs::{
    DEFAULT_DFS_CONNECTIVITY_PREFIX as DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX,
    DEFAULT_DFS_ENDPOINT_PREFIX as DAEMON_DEFAULT_DFS_ENDPOINT_PREFIX,
    observe_default_dfs_reconciliation,
};
use sinomo_dfs::{
    DFS_AUTHORITY_PROJECTION_MAX_BYTES,
    DfsAuthoritativeProjectionCache as DaemonDfsAuthoritativeProjection,
    DfsCommittedEffects as DaemonDfsMessageCommittedEdges, DfsCommittedWorkTracker,
    DfsCrossShardNamespaceLockRegistry, DfsDataPlaneHost, DfsDataPlaneHostError,
    DfsDataPlaneHostFuture, DfsDataPlaneResponse, DfsDefaultClusterBootstrapOptions,
    DfsDefaultClusterSmokeOptions, DfsFileSystemId, DfsInboundAttachment,
    DfsMaintenanceExecutionExclusions,
    DfsMaintenanceForegroundPreemption as DaemonDfsMaintenanceForegroundPreemption,
    DfsMaintenanceTickOptions, DfsMaintenanceTickReport, DfsMessageCommand,
    DfsMessageCommittedWork, DfsMessageExecutionScope, DfsMessageProcessResult, DfsMetadataBackend,
    DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationReport,
    DfsMetadataLaneKey as DaemonDfsMetadataPublishLaneKey, DfsMetadataPublishScheduleOptions,
    DfsMetadataPublishScheduleStart, DfsMetadataSchedulerRegistry, DfsMetadataShardId,
    DfsMetadataShardMembershipHandoff, DfsMutationAdmission as DaemonDfsMutationAdmission,
    DfsMutationGenerationFence as DaemonDfsMutationGenerationFence,
    DfsMutationLaneKey as DaemonDfsMutationLaneKey,
    DfsMutationScheduler as DaemonDfsMutationScheduler, DfsMutationSchedulerRegistry,
    DfsNodeDetachedSendWorker, DfsNodeServedMessageTransport, DfsNodeTransport,
    DfsObjectRpcSendOptions, DfsPeerTransport, DfsPeerTransportError, DfsRepairAttemptRecord,
    DfsResidentMetadataScope as DaemonDfsResidentMetadataScope,
    DfsRuntimeMessageResult as DaemonDfsMessageCommandProcessResult,
    DfsRuntimeMessageServiceLane as DaemonDfsMessageServiceLane, NetworkedDfsService,
    ResidentLocalMetadataShardHandle, ResidentLocalShardedMetadataBackend,
    default_dfs_repair_attempt_snapshot_path, dfs_message_execution_scope,
    dfs_message_is_read_only_request,
    dfs_metadata_journal_rejection as daemon_dfs_metadata_journal_rejection,
    dfs_metadata_journal_rejection_for as daemon_dfs_metadata_journal_rejection_for,
    dfs_metadata_prefix_probe_is_verification_only as daemon_dfs_metadata_prefix_probe_is_verification_only,
    dfs_metadata_snapshot_rejection as daemon_dfs_metadata_snapshot_rejection,
    dfs_metadata_snapshot_rejection_for as daemon_dfs_metadata_snapshot_rejection_for,
    dfs_network_message_kind as daemon_dfs_message_kind,
    dfs_response_committed_visible_mutation as daemon_dfs_response_committed_visible_mutation,
    dfs_visible_mutation_event as daemon_dfs_visible_mutation_event, ingest_dfs_message_attachment,
    metadata_replica_context_from_authority_projection as daemon_dfs_metadata_replica_context_from_authority_projection,
    metadata_shards_from_authority_projection as daemon_dfs_metadata_shards_from_authority_projection,
    read_dfs_repair_attempt_snapshot,
};
#[cfg(test)]
use sinomo_dfs::{
    DefaultDfsCommittedProjectionObserver as DaemonDefaultDfsCommittedProjectionObserver,
    DefaultDfsReconciliationProjection as DaemonDefaultDfsReconciliationProjection,
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
    metadata_shard_membership_handoff_proof_key, read_local_dfs_metadata_shard_write_freeze,
};
#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
#[cfg(test)]
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
// Replica table expansion is rebuildable and deliberately sits behind the
// durable journal ACK. Let a short foreground burst finish before checking out
// the same resident shard; each later ACK resets this quiet period through the
// retained per-shard scheduler. This keeps full-shard authority latency on the
// append-only durability path while still materializing promptly when quiet.
#[cfg(not(test))]
const DAEMON_DFS_REPLICA_MATERIALIZATION_QUIET_PERIOD: Duration = Duration::from_millis(750);
#[cfg(test)]
const DAEMON_DFS_REPLICA_MATERIALIZATION_QUIET_PERIOD: Duration = Duration::from_millis(10);
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
const DAEMON_DFS_PEER_DISCOVERY_VALIDATION_LEASE: Duration = Duration::from_millis(250);
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

fn daemon_dfs_local_zone() -> Option<String> {
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
    cluster_paths: ClusterPaths,
    cluster: Arc<dyn sinomo_core::service::ClusterAccess>,
    task_owner: Weak<DfsRuntimeHost>,
    runtime: tokio::runtime::Handle,
}

impl sinomo_dfs::DfsMetadataAuthorityHost for DaemonDfsMetadataAuthorityHost {
    /// sinomo:kv-configuration: Load the retained DFS configuration projection during bootstrap or committed-delta gap recovery.
    fn read_authoritative_prefix(&self) -> sinomo_dfs::DfsMetadataAuthorityReadFuture<'_> {
        #[cfg(test)]
        record_daemon_dfs_test_authoritative_prefix_call(&self.cluster_paths);
        Box::pin(async move {
            self.cluster
                .kv_range_prefix_authoritative_async(
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
}

fn daemon_dfs_metadata_authority_host(
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Option<Arc<dyn sinomo_dfs::DfsMetadataAuthorityHost>> {
    let runtime = runtime?;
    let data_root = cluster_paths.root.parent()?;
    let data_dir = SinomoDataDir::new(data_root);
    let host = dfs_runtime_host(&data_dir).ok()?;
    Some(Arc::new(DaemonDfsMetadataAuthorityHost {
        data_dir,
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
    fn begin_foreground_preemption(&self) -> DaemonDfsMaintenanceForegroundPreemption {
        self.mutation_scheduler.begin_maintenance_interest()
    }

    fn materialize(
        &self,
        mut shard: sinomo_dfs::ResidentLocalMetadataShardLease,
    ) -> Result<usize, sinomo_dfs::MetadataBackendError> {
        // Even a failed decode/apply can leave a glibc arena at its temporary
        // high-water mark. The lane's final drop transfers one cleanup edge to
        // the scoped allocator lane after every attempted pass.
        self.allocator_cleanup_needed.store(true, Ordering::Release);
        #[cfg(test)]
        wait_at_daemon_dfs_message_committed_edge_test_hook(&format!(
            "replica-materialization:{}",
            self.data_dir.root().display()
        ));
        DfsMetadataBackend::materialize_pending_replica_journal(&mut *shard)
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
#[cfg(test)]
static DAEMON_DFS_TEST_MOUNTED_MUTATION_FAIL_AFTER_REPAIR_PERSIST: OnceLock<
    Mutex<BTreeSet<PathBuf>>,
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
static DAEMON_DFS_MAINTENANCE_NOTIFY: OnceLock<Arc<tokio::sync::Notify>> = OnceLock::new();
static DAEMON_DFS_REPAIR_QUEUE_NONEMPTY: OnceLock<Mutex<BTreeMap<PathBuf, bool>>> = OnceLock::new();
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

fn require_daemon_dfs_data_owner_lock(
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
pub(crate) use control_plane::{
    DaemonDfsControlPlaneKvClient, break_glass_recover_dfs_placement_authority_permit,
    inspect_dfs_placement_authority_permit, recover_removed_node_dfs_placement_authority_permit,
};

mod data_plane;
use data_plane::with_daemon_dfs_control_plane_seeded_on_runtime;
#[cfg(test)]
use data_plane::*;
pub(crate) use data_plane::{
    DaemonDfsDataPlaneHost, daemon_dfs_control_plane_client_on_runtime, daemon_dfs_node_transport,
    daemon_dfs_node_transport_on_runtime, daemon_dfs_peer_transports_on_runtime,
    open_daemon_dfs_metadata_backend, with_daemon_dfs_control_plane_on_runtime,
    with_daemon_owned_dfs_control_plane_on_runtime,
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
    daemon_dfs_offline_node_ids, persist_daemon_dfs_repair_queue_records,
    record_daemon_dfs_repair_queue_nonempty, refresh_daemon_dfs_repair_queue_nonempty,
};
pub(crate) use maintenance_host::{
    daemon_dfs_maintenance_notify, daemon_dfs_maintenance_reconciliation_generation,
    daemon_dfs_pending_reactive_maintenance_work_on_runtime, daemon_dfs_repair_queue_work_is_known,
    handle_daemon_dfs_mounted_mutation_handoff_on_runtime, notify_daemon_dfs_maintenance_work,
    request_daemon_dfs_maintenance_reconciliation,
    schedule_daemon_dfs_startup_repair_reconciliation,
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
#[cfg(test)]
pub(crate) use reconciliation_host::ensure_default_distributed_dfs_async_on_runtime;
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
};
mod publish_host;
#[cfg(test)]
use publish_host::{
    daemon_dfs_metadata_publish_discovery_lanes_boxed_async,
    daemon_dfs_metadata_publish_schedule_options, daemon_dfs_metadata_replication_options,
    publish_default_dfs_metadata_journal_with_sender,
    publish_default_dfs_metadata_journal_with_senders,
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
