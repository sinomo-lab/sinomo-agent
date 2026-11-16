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
        Self::from_native(native, node)
    }

    /// Adapt another genuine mounted native owner to the SAME CLI test boundary.
    pub(crate) fn from_native(native: DfsTestRuntime, node: NodeId) -> Self {
        let data = native.data_dir(&node);
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

    /// Borrow the actual fixture lifecycle owner for setup/inspection only.
    /// Callers return owned futures/handles and release this mutex BEFORE RPC.
    pub(crate) fn with_native<R>(&self, work: impl FnOnce(&mut DfsTestRuntime, &NodeId) -> R) -> R {
        let node = owners().lock().unwrap().get(&self.path).unwrap().0.clone();
        let mut native = self.native.as_ref().unwrap().lock().unwrap();
        work(&mut native, &node)
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

#[test]
fn registered_native_control_read_keeps_local_provenance_and_remote_contention_fails_closed() {
    use sinomo_runtime::{
        cluster::ClusterKvPathsExt,
        dfs::DfsObjectRpcSendOptions,
        network::{ClusterPaths, ObjectRpcCall, ObjectRpcRequest},
    };
    let root = sinomo_testdir::TestDir::new("cli-native-local-read-provenance");
    let data = SinomoDataDir::new(&root);
    let owner = NativeCliFixture::start(&data);
    let paths = ClusterPaths::new(&data);
    let state = sinomo_runtime::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let node = state.local_node_id.clone();
    let budget = sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT;
    let prefix = "/dfs/filesystems/";
    let request = || {
        let payload =
            sinomo_runtime::kv::cluster_kv_authoritative_range_prefix_rpc_payload(prefix).unwrap();
        ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
    };
    let (peer, _) = peer_transports(&data).unwrap();
    let response = peer
        .call_object_rpc(
            &node,
            ObjectRpcCall::value(request()),
            DfsObjectRpcSendOptions::default().with_overall_timeout(budget),
        )
        .unwrap();
    let native = owner.native.as_ref().unwrap().lock().unwrap();
    let response = native
        .runtime_handle()
        .block_on(response.into_materialized_value_response(16 * 1024 * 1024))
        .unwrap();
    let read =
        sinomo_runtime::kv::decode_cluster_kv_authoritative_range_prefix_rpc_body(&response.body)
            .unwrap();
    assert!(
        !read.value.is_empty(),
        "actual committed native configuration"
    );
    let completed = native
        .rpc_observations()
        .into_iter()
        .rev()
        .find(|r| {
            r.source == node
                && r.target == node
                && r.route == sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
                && r.phase == sinomo_runtime::dfs::test_support::DfsTestRpcPhase::Completed
        })
        .unwrap();
    drop(native);
    let signer = completed.signer.expect("actual receiver signer recorded");
    assert!(
        signer.is_local(),
        "registered same-host request was mislabeled: {signer:?}"
    );
    let (signer_node, signer_key) = signer.cluster_identity().unwrap();
    let member = state.members.iter().find(|m| m.node_id == node).unwrap();
    assert_eq!(signer_node, node.as_str());
    assert_eq!(signer_key, member.node_public_key);
    owner.with_native(|native, _| native.checkpoint_rpc_observation_window().unwrap());
    // A raw protocol frame remains a remote/forwarded call even when its
    // source ID equals its destination. It cannot borrow local-origin policy.
    let held =
        sinomo_runtime::kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths()).unwrap();
    let resident = paths.kv_paths().raft_store().open_node(&node).unwrap();
    assert!(
        sinomo_runtime::kv::with_cluster_kv_resident_read_projection(
            &paths.kv_paths().raft_wal_paths(&node),
            &node,
            |_| (),
        )
        .is_none(),
        "hold the actual resident read projection as well as mutation admission"
    );
    let started = std::time::Instant::now();
    let (rejected, refused) = {
        let native = owner.native.as_ref().unwrap().lock().unwrap();
        let rejected = native
            .rpc_request(&node, &node, request(), budget)
            .unwrap_err();
        let observations = native.rpc_observations();
        let constructed = observations
            .iter()
            .find(|r| {
                r.source == node
                    && r.target == node
                    && r.route == sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
                    && r.origin
                        == sinomo_runtime::dfs::test_support::DfsTestRpcOrigin::ExplicitFixtureRpc
                    && r.phase == sinomo_runtime::dfs::test_support::DfsTestRpcPhase::Constructed
            })
            .expect("actual explicit raw request");
        // The receiver's busy KV handler answers with an error response, as
        // the daemon sends a handler's refusal, so the same call completes
        // with that refusal instead of ending without a response.
        let refused = observations
            .iter()
            .find(|r| {
                r.call_id == constructed.call_id
                    && r.phase == sinomo_runtime::dfs::test_support::DfsTestRpcPhase::Completed
            })
            .expect("same explicit raw request completed with the receiver's refusal")
            .clone();
        (rejected, refused)
    };
    assert!(
        rejected.contains("local Raft admission is busy"),
        "{rejected}"
    );
    assert!(started.elapsed() < budget);
    assert!(!refused.signer.unwrap().is_local());
    drop(resident);
    drop(held);
    owner
        .native
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .checkpoint_rpc_observation_window()
        .unwrap();
    drop(owner);
}

#[test]
fn registered_native_transport_cannot_borrow_restarted_host_generation() {
    use sinomo_runtime::network::{
        ClusterObjectRpcOptions, ClusterPaths, NetworkDeliveryState, ObjectRpcCall,
        ObjectRpcRequest,
    };
    let root = sinomo_testdir::TestDir::new("cli-native-host-generation");
    let data = SinomoDataDir::new(&root);
    let owner = NativeCliFixture::start(&data);
    let node = sinomo_runtime::cluster::read_cluster_state_without_kv(&ClusterPaths::new(&data))
        .unwrap()
        .local_node_id;
    let host = owner
        .native
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .component_context(&node)
        .unwrap()
        .object_rpc_transport()
        .unwrap();
    let request = || {
        ObjectRpcCall::value(
            ObjectRpcRequest::encode_body(
                sinomo_runtime::api::DAEMON_DFS_BROWSER_STATUS_ROUTE,
                sinomo_runtime::api::DAEMON_RPC_CATEGORY,
                &sinomo_runtime::api::DaemonDfsBrowserStatusRequest::node_only(),
            )
            .unwrap(),
        )
    };
    let options = ClusterObjectRpcOptions::default()
        .with_overall_timeout(sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT);
    host.call(&node, request(), options, true).unwrap();
    {
        let mut native = owner.native.as_ref().unwrap().lock().unwrap();
        native.stop_node(&node).unwrap();
        assert!(host.call(&node, request(), options, true).is_err());
        native.reopen_node(node.clone()).unwrap();
    }
    let rejected = host.call(&node, request(), options, true).unwrap_err();
    assert!(
        rejected
            .to_string()
            .contains("retired native host generation"),
        "{rejected}"
    );
    assert_eq!(
        rejected.delivery_state(),
        Some(NetworkDeliveryState::DefinitelyNotSent)
    );
    let current = owner
        .native
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .component_context(&node)
        .unwrap()
        .object_rpc_transport()
        .unwrap();
    current.call(&node, request(), options, true).unwrap();
    owner
        .native
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .checkpoint_rpc_observation_window()
        .unwrap();
    drop(current);
    drop(host);
    drop(owner);
}
