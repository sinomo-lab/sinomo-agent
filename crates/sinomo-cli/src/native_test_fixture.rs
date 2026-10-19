//! Explicit positive-test ownership. No production initialization or RPC fallback.
//!
//! The fixture mounts the supported native Embedded/Main host at a fresh DATA
//! path. Browser calls use that host's authenticated registry; only transport is
//! adapted. Weak registration neither prolongs ownership nor supplies replies.
use crate::app_api::RemoteSifsBrowser;
use sinomo_runtime::{
    api::DaemonApiError, dfs::test_support::DfsTestRuntime, local::SinomoDataDir, node::NodeId,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, Weak},
};

type Registration = (NodeId, Weak<Mutex<DfsTestRuntime>>);
fn owners() -> &'static Mutex<BTreeMap<PathBuf, Registration>> {
    static OWNERS: OnceLock<Mutex<BTreeMap<PathBuf, Registration>>> = OnceLock::new();
    OWNERS.get_or_init(Mutex::default)
}

/// Declare before clients so their drops precede the native owner's drain.
pub(crate) struct NativeCliFixture {
    path: PathBuf,
    native: Option<Arc<Mutex<DfsTestRuntime>>>,
    mounted: Option<sinomo_runtime::vfs::native_test_support::NativeMountedFixture>,
}

impl NativeCliFixture {
    pub(crate) fn start(data: &SinomoDataDir) -> Self {
        let name = format!("cli-native-{}", sinomo_runtime::core::Id::generate());
        let (native, node) = DfsTestRuntime::start_single_node_in_data_dir(&name, data.clone())
            .expect("initialize and mount actual native CLI fixture");
        let path = std::fs::canonicalize(data.root()).unwrap();
        let mounted = sinomo_runtime::vfs::native_test_support::NativeMountedFixture::register(
            &native, &node,
        );
        native.checkpoint_rpc_observation_window().unwrap();
        let native = Arc::new(Mutex::new(native));
        {
            let mut registered = owners().lock().unwrap();
            assert!(
                !registered.contains_key(&path),
                "duplicate native CLI owner"
            );
            registered.insert(path.clone(), (node, Arc::downgrade(&native)));
        }
        Self {
            path,
            native: Some(native),
            mounted: Some(mounted),
        }
    }
}

pub(crate) fn peer_transports(
    data: &SinomoDataDir,
) -> Option<(
    Arc<dyn sinomo_runtime::dfs::DfsPeerTransport>,
    Arc<dyn sinomo_runtime::dfs::AsyncDfsPeerTransport>,
)> {
    let path = std::fs::canonicalize(data.root()).ok()?;
    let (node, native) = owners().lock().unwrap().get(&path)?.clone();
    let native = native.upgrade()?;
    let native = native.lock().unwrap();
    assert_eq!(
        std::fs::canonicalize(native.data_dir(&node).root()).unwrap(),
        path
    );
    Some(
        native
            .peer_transports(&node)
            .expect("actual registered Main transport"),
    )
}

impl Drop for NativeCliFixture {
    fn drop(&mut self) {
        owners().lock().unwrap().remove(&self.path);
        drop(self.mounted.take());
        let native = self.native.take().unwrap();
        // Clients carry Weak references. A live call at this boundary is an
        // ownership error, not permission to clear a data-owner lock.
        let result = Arc::try_unwrap(native)
            .map_err(|_| "native CLI fixture still has an active RPC at teardown".to_string())
            .and_then(|native| {
                native
                    .into_inner()
                    .map_err(|_| "native fixture mutex poisoned".to_string())
            })
            .and_then(DfsTestRuntime::close);
        if let Err(error) = result {
            if std::thread::panicking() {
                eprintln!("native CLI fixture drain failed: {error}");
            } else {
                panic!("native CLI fixture drain failed: {error}");
            }
        }
    }
}

pub(crate) fn browser(
    data: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> Option<RemoteSifsBrowser> {
    let path = std::fs::canonicalize(data.root()).ok()?;
    let (node, native) = owners().lock().unwrap().get(&path)?.clone();
    Some(
        RemoteSifsBrowser::for_main_owner(
            data.clone(),
            "actual-native-fixture".into(),
            node.clone(),
            runtime,
        )
        .with_test_rpc(Arc::new(move |request, remaining| {
            let native = native.upgrade().ok_or_else(|| DaemonApiError::NotRunning {
                detail: "native fixture owner drained".into(),
            })?;
            let deadline = std::time::Instant::now() + remaining;
            let (handle, future) = {
                let native = native.lock().unwrap();
                native
                    .checkpoint_rpc_observation_window()
                    .expect("complete native observation checkpoint before CLI request");
                assert_eq!(
                    std::fs::canonicalize(native.data_dir(&node).root()).unwrap(),
                    path
                );
                native
                    .component_context(&node)
                    .map_err(|detail| DaemonApiError::NotRunning { detail })?;
                (
                    native.runtime_handle(),
                    native.rpc_result_future(
                        &node,
                        &node,
                        sinomo_runtime::network::ObjectRpcCall::value(request),
                        deadline.saturating_duration_since(std::time::Instant::now()),
                    ),
                )
            };
            // The lifecycle mutex is released BEFORE dispatch. Parallel writers
            // retain the native runtime's ordinary concurrency and ordering.
            let call = async {
                let result = future.await?;
                result
                    .into_materialized_value_response(16 * 1024 * 1024)
                    .await
                    .map_err(|e| e.to_string())
            };
            let run = || {
                handle.block_on(async {
                    tokio::time::timeout(
                        deadline.saturating_duration_since(std::time::Instant::now()),
                        call,
                    )
                    .await
                    .map_err(|_| {
                        "native fixture RPC deadline expired; PossiblyApplied".to_string()
                    })?
                })
            };
            let result = if tokio::runtime::Handle::try_current().is_ok() {
                tokio::task::block_in_place(run)
            } else {
                run()
            };
            native
                .lock()
                .unwrap()
                .checkpoint_rpc_observation_window()
                .expect("complete native observation checkpoint after CLI request");
            result.map_err(|detail| DaemonApiError::Network {
                address: "actual native Main registry".into(),
                detail,
            })
        })),
    )
}

#[test]
fn explicit_native_fixture_preserves_path_isolation_and_drains_with_live_frontend() {
    use crate::sifs_file_manager::SifsFileManagerBackend;
    let root = sinomo_testdir::TestDir::new("cli-native-fixture-lifetime");
    let data = SinomoDataDir::new(root.join("owned"));
    let other = SinomoDataDir::new(root.join("uninitialized"));
    let owner = NativeCliFixture::start(&data);
    assert!(
        SifsFileManagerBackend::open(&other)
            .unwrap_err()
            .contains("initialized node")
    );
    let backend = SifsFileManagerBackend::open(&data).unwrap();
    let mounted = sinomo_runtime::vfs::mounted_sifs(Some(data.root())).unwrap();
    {
        assert!(
            mounted.stat("/services").unwrap().kind
                == sinomo_runtime::vfs::VfsObjectKind::Directory
        );
    }
    backend
        .write_file("/services/fixture-lifetime", b"actual native publication")
        .unwrap();
    assert_eq!(
        backend.read_file("/services/fixture-lifetime").unwrap(),
        b"actual native publication"
    );
    drop(owner);
    assert!(
        mounted.stat("/services").is_err(),
        "a live frontend must not retain stopped native authority"
    );
    assert!(backend.read_file("/services/fixture-lifetime").is_err());
    assert!(
        backend
            .write_file("/services/after-drain", b"not authorized")
            .is_err()
    );
    assert!(
        owners()
            .lock()
            .unwrap()
            .get(&std::fs::canonicalize(data.root()).unwrap())
            .is_none()
    );
}
