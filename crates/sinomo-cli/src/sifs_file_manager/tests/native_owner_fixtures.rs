//! Actual native owner setup for CLI tests; no decoded DATA grants effects.
use super::*;
use sinomo_runtime::dfs::test_support::DfsTestRuntime;

pub(super) fn publish_actual_native_catalog(
    native: &DfsTestRuntime,
    node: &NodeId,
) -> sinomo_runtime::kv::KvAuthoritativeRead<KvSnapshot> {
    let payload = sinomo_runtime::kv::cluster_kv_authoritative_snapshot_rpc_payload().unwrap();
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let (peer, _) = native.peer_transports(node).unwrap();
    let result = peer
        .call_object_rpc(
            node,
            network::ObjectRpcCall::value(request),
            DfsObjectRpcSendOptions::default()
                .with_overall_timeout(sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT),
        )
        .unwrap();
    let response = native
        .runtime_handle()
        .block_on(async move {
            result
                .into_materialized_value_response(64 * 1024 * 1024)
                .await
        })
        .unwrap();
    let read =
        sinomo_runtime::kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
            .unwrap();
    let paths = ClusterPaths::new(native.data_dir(node));
    // The hosted snapshot route returns the native read without publishing it.
    // Explicitly materialize that exact DATA through the existing derived-view
    // writer for this fixture; this grants no Raft adoption or effect authority.
    cluster::write_imported_cluster_kv_snapshot_if_changed(&paths, &read.value).unwrap();
    let checkpoint =
        read_sifs_materialized_cluster_kv_snapshot(&paths)
            .unwrap()
            .unwrap();
    assert!(
        checkpoint.revision >= read.authority.revision,
        "normal snapshot publication reaches actual captured generation"
    );
    assert_eq!(
        snapshot_metadata_shards(checkpoint.as_ref()),
        snapshot_metadata_shards(&read.value),
        "the actual published metadata assignments match the captured native cut"
    );
    read
}

pub(super) fn snapshot_metadata_shards(snapshot: &KvSnapshot) -> Vec<DfsMetadataShardConfig> {
    let mut parsed = sinomo_runtime::kv::InProcessKvStore::restore(snapshot.clone()).unwrap();
    DfsControlPlaneStore::new(&mut parsed)
        .list_metadata_shards(&sifs_file_system_id())
        .unwrap()
}

pub(super) fn actual_stat(
    fixture: &crate::native_test_fixture::NativeCliFixture,
    path: &str,
) -> Result<DfsStat, DfsPosixError> {
    fixture.with_native(|native, _| {
        let mut owner = native.metadata_primary_service(path).unwrap();
        owner.posix_stat(path)
    })
}

pub(super) struct NativeCatalogCuts {
    pub state: sinomo_runtime::core::ClusterBootstrapState,
    pub local: NodeId,
    pub replica_only: sinomo_runtime::kv::KvAuthoritativeRead<KvSnapshot>,
    pub local_primary: sinomo_runtime::kv::KvAuthoritativeRead<KvSnapshot>,
    pub replica_shards: Vec<DfsMetadataShardConfig>,
    pub primary_shards: Vec<DfsMetadataShardConfig>,
}

/// Produce real membership/capabilities and both catalogs before closing the
/// owners. The returned DTOs test an OFFLINE observational/cache contract only.
pub(super) fn actual_replica_and_primary_catalogs(name: &str) -> NativeCatalogCuts {
    actual_replica_and_primary_catalogs_for(name, "root")
}

pub(super) fn actual_replica_and_primary_catalogs_for(
    name: &str,
    changed_shard: &str,
) -> NativeCatalogCuts {
    let (native, nodes) = DfsTestRuntime::start_cluster(name, 3).unwrap();
    let remote = nodes[0].clone();
    let local = nodes[2].clone();
    let fs = sifs_file_system_id();
    for shard in native.metadata_shards().unwrap() {
        native
            .reassign_metadata_shard(
                &fs,
                &shard.shard_id,
                remote.clone(),
                nodes
                    .iter()
                    .filter(|node| **node != remote)
                    .cloned()
                    .collect(),
            )
            .unwrap();
    }
    let replica_only = publish_actual_native_catalog(&native, &local);
    let replica_shards = snapshot_metadata_shards(&replica_only.value);
    assert_eq!(replica_shards.len(), 3);
    assert!(
        replica_shards
            .iter()
            .all(|shard| shard.primary_node_id != local)
    );
    native
        .reassign_metadata_shard(
            &fs,
            changed_shard,
            local.clone(),
            nodes
                .iter()
                .filter(|node| **node != local)
                .cloned()
                .collect(),
        )
        .unwrap();
    let local_primary = publish_actual_native_catalog(&native, &local);
    let primary_shards = snapshot_metadata_shards(&local_primary.value);
    assert!(
        primary_shards
            .iter()
            .any(|shard| shard.primary_node_id == local)
    );
    let state = cluster::read_cluster_state_without_kv(&ClusterPaths::new(native.data_dir(&local)))
        .unwrap();
    native.checkpoint_rpc_observation_window().unwrap();
    native.close().unwrap();
    NativeCatalogCuts {
        state,
        local,
        replica_only,
        local_primary,
        replica_shards,
        primary_shards,
    }
}

pub(super) fn install_actual_offline_catalog(
    data: &SinomoDataDir,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    snapshot: &KvSnapshot,
) {
    data.ensure_root_layout().unwrap();
    let paths = ClusterPaths::new(data);
    // Exact DATA from the real stopped producer; not a fake daemon, member,
    // capability record, Native commit, or permission to execute any mutation.
    cluster::write_cluster_state(&paths, state).unwrap();
    sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), snapshot).unwrap();
    assert_eq!(
        read_sifs_metadata_shard_configs(&paths).unwrap(),
        snapshot_metadata_shards(snapshot)
    );
}

pub(super) fn evidence(data: &SinomoDataDir, name: &str, value: serde_json::Value) {
    fs::write(
        data.root().join(name),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
}

/// Actual live metadata/capability owners for an inspection/routing test.
pub(super) fn live_replica_cli_fixture(
    name: &str,
) -> (
    crate::native_test_fixture::NativeCliFixture,
    SinomoDataDir,
    NodeId,
    sinomo_runtime::kv::KvAuthoritativeRead<KvSnapshot>,
    Vec<DfsMetadataShardConfig>,
) {
    let (native, nodes) = DfsTestRuntime::start_cluster(name, 3).unwrap();
    let remote = nodes[0].clone();
    let caller = nodes[2].clone();
    for shard in native.metadata_shards().unwrap() {
        native
            .reassign_metadata_shard(
                &sifs_file_system_id(),
                &shard.shard_id,
                remote.clone(),
                nodes
                    .iter()
                    .filter(|node| **node != remote)
                    .cloned()
                    .collect(),
            )
            .unwrap();
    }
    let old = publish_actual_native_catalog(&native, &caller);
    let shards = snapshot_metadata_shards(&old.value);
    let data = native.data_dir(&caller).clone();
    let fixture = crate::native_test_fixture::NativeCliFixture::from_native(native, caller.clone());
    (fixture, data, caller, old, shards)
}
