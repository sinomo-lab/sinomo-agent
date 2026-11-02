use super::*;

#[test]
fn busy_exact_cache_keeps_two_worker_executor_responsive() {
    let (mut native, _) =
        crate::test_runtime::DfsTestRuntime::start_cluster("mobile-cache-eligibility", 2).unwrap();
    let config = native.root_shard().unwrap();
    let node = config.replica_node_ids[0].clone();
    let mut source = native.client(&config.primary_node_id).unwrap();
    source
        .posix_mkdir("/mobile-cache-positive-control")
        .unwrap();
    source.posix_sync_directory("/").unwrap();
    drop(source);
    let data = native.data_dir(&node).clone();
    let paths = ClusterPaths::new(&data);
    let owner_runtime = native.runtime_handle();
    let peer = read_daemon_dfs_message_peer_snapshot(&paths)
        .unwrap()
        .unwrap();
    let lane = DaemonDfsMutationLaneKey {
        file_system_id: config.file_system_id,
        shard_id: DfsMetadataShardId::new(config.shard_id),
    };
    assert!(
        daemon_dfs_message_cache_exact_admission_handle(
            &data,
            &paths,
            &peer,
            &lane,
            &owner_runtime,
        )
        .unwrap()
        .is_some(),
        "positive control must use a real resident shard"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let slot = daemon_dfs_message_service_cache()
        .existing_root(&data.root())
        .unwrap()
        .unwrap();
    let held = slot.lock().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    for _ in 0..2 {
        let (data, paths, peer, lane, handle, send) = (
            data.clone(),
            paths.clone(),
            peer.clone(),
            lane.clone(),
            owner_runtime.clone(),
            send.clone(),
        );
        runtime.spawn(async move {
            let eligible = daemon_dfs_message_cache_exact_admission_is_current(
                &data, &paths, &peer, &lane, &handle, None,
            );
            let _ = send.send(eligible);
        });
    }
    let first = receive.recv_timeout(Duration::from_secs(2));
    let second = receive.recv_timeout(Duration::from_secs(2));
    // Release before asserting so the old blocking implementation can drain
    // after a failed control, rather than hanging the test's runtime teardown.
    drop(held);
    assert!(
        matches!(first, Ok(Ok(false))),
        "busy cache blocked a worker: {first:?}"
    );
    assert!(
        matches!(second, Ok(Ok(false))),
        "busy cache blocked both workers: {second:?}"
    );
    assert!(
        daemon_dfs_message_cache_exact_admission_handle(
            &data,
            &paths,
            &peer,
            &lane,
            &owner_runtime,
        )
        .unwrap()
        .is_some(),
        "contention must not retire the resident generation"
    );
    drop(runtime);
    native.close().unwrap();
}
