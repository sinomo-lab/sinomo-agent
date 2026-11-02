//! Opt-in adapters for a real, mounted native DFS test owner. No synthetic VFS,
//! local writer, authority proof or successful RPC response is installed.
use super::*;
use sinomo_dfs::{AsyncDfsPeerTransport, DfsPeerTransport, test_support::DfsTestRuntime};
use std::{collections::BTreeMap, sync::OnceLock};

#[derive(Clone)]
struct Registration {
    node: NodeId,
    cluster: sinomo_core::Id,
    sync: Arc<dyn DfsPeerTransport>,
    asynchronous: Arc<dyn AsyncDfsPeerTransport>,
}
fn registrations() -> &'static Mutex<BTreeMap<PathBuf, Registration>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<PathBuf, Registration>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

pub struct NativeMountedFixture {
    path: PathBuf,
}
impl NativeMountedFixture {
    /// The caller must keep this actual native owner alive and drain it after
    /// dropping the registration and all VFS clients. DATA must be owner-matched.
    pub fn register(native: &DfsTestRuntime, node: &NodeId) -> Self {
        let data = native.data_dir(node);
        native
            .component_context(node)
            .expect("actual mounted native owner");
        let state =
            sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data)).unwrap();
        assert_eq!(&state.local_node_id, node);
        let path = std::fs::canonicalize(data.root()).unwrap();
        let (sync, asynchronous) = native.peer_transports(node).unwrap();
        {
            let mut registered = registrations().lock().unwrap();
            assert!(
                !registered.contains_key(&path),
                "duplicate native mounted fixture"
            );
            registered.insert(
                path.clone(),
                Registration {
                    node: node.clone(),
                    cluster: state.cluster_id,
                    sync,
                    asynchronous,
                },
            );
        }
        Self { path }
    }
}
impl Drop for NativeMountedFixture {
    fn drop(&mut self) {
        registrations().lock().unwrap().remove(&self.path);
    }
}

/// Own the real single-node runtime and its mounted-client registration.
/// Callers must drop filesystem clients before dropping this owner.
pub struct NativeMountedOwner {
    native: Mutex<Option<DfsTestRuntime>>,
    registration: Option<NativeMountedFixture>,
    node: NodeId,
    data_dir: SinomoDataDir,
}

impl std::fmt::Debug for NativeMountedOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeMountedOwner")
            .field("node", &self.node)
            .field("data_dir", &self.data_dir)
            .finish_non_exhaustive()
    }
}

impl NativeMountedOwner {
    pub fn start(name: &str) -> std::result::Result<Self, String> {
        let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let (native, client) = DfsTestRuntime::start_single_node(name, node.clone())?;
        drop(client);
        Ok(Self::register(native, node))
    }

    pub fn start_in_data_dir(
        name: &str,
        data_dir: SinomoDataDir,
    ) -> std::result::Result<Self, String> {
        let (native, node) = DfsTestRuntime::start_single_node_in_data_dir(name, data_dir)?;
        Ok(Self::register(native, node))
    }

    fn register(native: DfsTestRuntime, node: NodeId) -> Self {
        let registration = NativeMountedFixture::register(&native, &node);
        let data_dir = native.data_dir(&node).clone();
        Self {
            native: Mutex::new(Some(native)),
            registration: Some(registration),
            node,
            data_dir,
        }
    }

    pub fn data_dir(&self) -> &SinomoDataDir {
        &self.data_dir
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node
    }

    pub fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.native
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .runtime_handle()
    }

    /// Seal a completed phase through the native runtime's verified observation checkpoint.
    pub fn checkpoint_rpc_observation_window(&self) -> std::result::Result<usize, String> {
        self.native
            .lock()
            .map_err(|_| "native mounted owner mutex poisoned".to_string())?
            .as_ref()
            .ok_or_else(|| "native mounted owner already closed".to_string())?
            .checkpoint_rpc_observation_window()
    }

    /// Retain this owner until every clone of the actual mounted VFS is dropped.
    pub fn mounted_vfs(self: &Arc<Self>) -> Result<VfsHandle> {
        let mut backend = MountedSifsVfsBackend::open(Some(self.data_dir.root()))?;
        backend.native_owner = Some(Arc::clone(self));
        VfsHandle::new(Arc::new(backend), "/")
    }
}

impl Drop for NativeMountedOwner {
    fn drop(&mut self) {
        drop(self.registration.take());
        if let Some(native) = self.native.get_mut().unwrap().take()
            && let Err(error) = native.close()
        {
            if std::thread::panicking() {
                eprintln!("native mounted owner shutdown failed: {error}");
            } else {
                panic!("native mounted owner shutdown failed: {error}");
            }
        }
    }
}

/// Open a client through the existing registration for this actual native owner.
/// An unregistered path returns `None`, preserving the caller's ordinary guard.
pub fn open_registered(
    data: &SinomoDataDir,
    cancellation: Option<DfsCancellationFlag>,
) -> Option<Result<OpenedMountedSifsService>> {
    let path = std::fs::canonicalize(data.root()).ok()?;
    let registration = registrations().lock().unwrap().get(&path)?.clone();
    let result = (|| {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data))
            .map_err(|e| e.to_string())?;
        if state.local_node_id != registration.node || state.cluster_id != registration.cluster {
            return Err("native mounted fixture identity changed".to_string());
        }
        sinomo_dfs::open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
            data.clone(), registration.sync, registration.asynchronous, cancellation,
        ).map_err(|e| e.to_string())
    })();
    Some(result.map_err(|detail| VfsError::Backend {
        backend: "mounted-sifs".into(),
        path: "/".into(),
        detail,
    }))
}
