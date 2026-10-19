//! Observational construction against actual isolated native ownership.
use super::*;

fn dfs_bytes(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        current: &std::path::Path,
        out: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in fs::read_dir(current).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    visit(root, root, &mut out);
    out
}

#[test]
fn offline_backend_transitions_through_actual_native_main_without_private_state() {
    let root = temp_sifs_data_dir("cold-contract-native-transition");
    let data = SinomoDataDir::new(&root);
    let (mut native, node) =
        sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "cold-contract-native-transition",
            data.clone(),
        )
        .unwrap();
    let mut source = native.client(&node).unwrap();
    let original = source
        .posix_write_file(
            "/services/previous",
            b"retained actual bytes",
            sinomo_runtime::dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    drop(source);
    let owner = sinomo_runtime::dfs::runtime_host::retain_component_data_owner(
        &native.component_context(&node).unwrap(),
    )
    .unwrap();
    let weak = Arc::downgrade(&owner);
    let physical = owner.storage_owner_file_identity().unwrap();
    drop(owner);
    native.stop_node(&node).unwrap();
    assert!(
        weak.upgrade().is_none(),
        "offline assertions require a genuinely stopped native owner"
    );
    let before = dfs_bytes(&data.dfs_dir());
    let mut backend =
        SifsFileManagerBackend::open_with_deadline(&data, Instant::now() + Duration::from_secs(1))
            .unwrap();
    assert!(
        backend.service.get().is_none(),
        "construction must not create a caller metadata replica"
    );
    assert!(
        backend.dfs_stat("/services/previous").is_err(),
        "old local metadata is not a current stat guarantee"
    );
    assert!(
        backend.read_file("/services/previous").is_err(),
        "cached bytes do not authorize a current read"
    );
    let error = backend
        .create_dir_all("/services/offline-attempt")
        .unwrap_err();
    assert!(
        error.contains("cannot prepare SIFS mutation authority")
            && error.contains("daemon is not running"),
        "{error}"
    );
    assert!(
        backend
            .write_file_with_deadline("/services/expired", b"never published", Instant::now())
            .is_err()
    );
    assert_eq!(*backend.mutation_bootstrapped.lock().unwrap(), Some(false));
    assert_eq!(
        dfs_bytes(&data.dfs_dir()),
        before,
        "offline construction/operations publish no effects"
    );

    // Adapt only the transport to the real Embedded/Main service registry.
    // No status, receipt, capability or successful application reply is mocked.
    let native = Arc::new(Mutex::new(native));
    let actual = Arc::clone(&native);
    let target = node.clone();
    let actual_data = data.clone();
    let runtime = backend.rpc_runtime.clone();
    backend.native_test_main = Some(
        crate::app_api::RemoteSifsBrowser::for_main_owner(
            data.clone(),
            "native-registry-fixture".into(),
            node.clone(),
            runtime.runtime.handle(),
        )
        .with_test_rpc(Arc::new(move |request, remaining| {
            let native = actual.lock().unwrap();
            assert_eq!(native.data_dir(&target).root(), actual_data.root());
            native
                .component_context(&target)
                .map_err(|detail| sinomo_runtime::api::DaemonApiError::NotRunning { detail })?;
            native
                .rpc_request(&target, &target, request, remaining)
                .map_err(|detail| sinomo_runtime::api::DaemonApiError::Network {
                    address: "actual native Main registry".into(),
                    detail,
                })
        })),
    );
    assert!(
        backend.dfs_stat("/services/previous").is_err(),
        "native transport also rejects its stopped owner"
    );
    assert_eq!(dfs_bytes(&data.dfs_dir()), before);
    native.lock().unwrap().reopen_node(node.clone()).unwrap();
    {
        let current = sinomo_runtime::dfs::runtime_host::retain_component_data_owner(
            &native.lock().unwrap().component_context(&node).unwrap(),
        )
        .unwrap();
        assert_eq!(current.storage_owner_file_identity().unwrap(), physical);
        assert!(
            weak.upgrade().is_none(),
            "new actual owner, same physical storage"
        );
    }
    backend.create_dir_all("/services/offline-attempt").unwrap();
    backend
        .write_file(
            "/services/offline-attempt/online",
            b"authorized after Main starts",
        )
        .unwrap();
    assert_eq!(
        backend
            .read_file("/services/offline-attempt/online")
            .unwrap(),
        b"authorized after Main starts"
    );
    assert_eq!(
        backend.dfs_stat("/services/previous").unwrap().version,
        original.version
    );
    assert_eq!(
        backend.read_file("/services/previous").unwrap(),
        b"retained actual bytes"
    );
    assert!(
        backend.service.get().is_none(),
        "all operations used genuine Main, never caller replay or a local writer"
    );
    let calls = native.lock().unwrap().rpc_observations();
    for route in [
        DAEMON_DFS_BROWSER_STATUS_ROUTE,
        DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
        DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
        DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
    ] {
        assert!(
            calls.iter().any(|call| call.source == node
                && call.target == node
                && call.route == route
                && call.phase == sinomo_runtime::dfs::test_support::DfsTestRpcPhase::Completed),
            "real authenticated dispatch for {route}"
        );
    }
    native.lock().unwrap().stop_node(&node).unwrap();
    assert!(
        backend.dfs_stat("/services/previous").is_err(),
        "no successful operation makes future offline reads current"
    );
    drop(backend);
    Arc::try_unwrap(native)
        .ok()
        .unwrap()
        .into_inner()
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn cold_backend_rejects_missing_corrupt_and_changed_identity() {
    let root = temp_sifs_data_dir("cold-contract-no-identity");
    let data = SinomoDataDir::new(&root);
    data.ensure_root_layout().unwrap();
    assert!(
        SifsFileManagerBackend::open(&data)
            .unwrap_err()
            .contains("initialized node")
    );
    seed_observation_cluster(&data);
    let paths = ClusterPaths::new(&data);
    let original = fs::read(&paths.state).unwrap();
    let backend = SifsFileManagerBackend::open(&data).unwrap();
    // Negative DATA only; never installed in a live host or accepted as authority.
    fs::write(&paths.state, b"malformed cluster state").unwrap();
    assert!(SifsFileManagerBackend::open(&data).is_err());
    assert!(backend.create_dir_all("/services/no-authority").is_err());
    fs::write(&paths.state, &original).unwrap();
    let mut changed = cluster::read_cluster_state_without_kv(&paths).unwrap();
    changed.cluster_id = sinomo_runtime::core::Id::generate();
    cluster::write_cluster_state(&paths, &changed).unwrap();
    assert!(
        backend
            .create_dir_all("/services/no-authority")
            .unwrap_err()
            .contains("identity changed")
    );
    fs::write(&paths.state, original).unwrap();
    assert!(
        backend
            .create_dir_all("/services/no-authority")
            .unwrap_err()
            .contains("daemon is not running")
    );
}
