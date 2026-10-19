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

pub(super) fn open_registered(
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
