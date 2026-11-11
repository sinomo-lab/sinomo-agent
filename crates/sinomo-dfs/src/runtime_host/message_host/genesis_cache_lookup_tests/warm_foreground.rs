use super::*;
use std::fs;

fn warm_negative(name: &str, kind: u8) {
    let (native, nodes) = crate::test_support::DfsTestRuntime::start_cluster(name, 2).unwrap();
    let local = native.root_shard().unwrap().primary_node_id;
    let data = native.data_dir(&local).clone();
    let mut client = native.client(&local).unwrap();
    let object = client.posix_mkdir("/genesis-cache-ready").unwrap();
    drop(client);
    let job = if kind == 0 {
        Some(
            native
                .apply_placement_policy(
                    &local,
                    "/genesis-cache-ready",
                    crate::DfsPlacementScope::Subtree,
                    crate::DfsPlacementPolicy::new(
                        sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                        1,
                        1,
                    )
                    .unwrap(),
                )
                .unwrap(),
        )
    } else {
        None
    };
    let paths = ClusterPaths::new(&data);
    let runtime = native.runtime_handle();
    let peer = read_daemon_dfs_message_peer_snapshot(&paths)
        .unwrap()
        .unwrap();
    let mut preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
            &paths, &peer, &runtime,
        ))
        .unwrap();
    let mut command = mutation_for_genesis_check(&native, &local);
    if let Some(job) = job {
        let crate::DfsNetworkMessage::NamespaceRequest(request) =
            &mut command.routed_message.message
        else {
            unreachable!()
        };
        request.operation = crate::DfsNamespaceOperation::ReconcilePlacement {
            job_id: job.job_id,
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            expected_version: object.version.clone(),
            object_policy: object.policy.clone(),
            replication_policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
            desired_replica_node_ids: vec![local.clone()],
        };
        assert!(!daemon_dfs_message_preempts_maintenance(
            &command.routed_message.message
        ));
    }
    let owner = require_daemon_dfs_data_owner_lock(&DaemonPaths::new(&data)).unwrap();
    runtime
        .block_on(prepare_daemon_dfs_genesis_ingress(
            &data,
            &paths,
            &peer,
            &command,
            Some(&preflight),
            &runtime,
            &owner,
        ))
        .unwrap();
    let scheduler = daemon_dfs_mutation_scheduler(&data).unwrap();
    let interest = scheduler.begin_maintenance_interest();
    let admission = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    let slot = daemon_dfs_message_service_cache()
        .existing_root(&data.root())
        .unwrap()
        .unwrap();
    let resident = slot
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .resident_metadata_backend
        .clone()
        .unwrap();
    let shard = native.root_shard().unwrap();
    let handle = resident
        .scope_shard(
            &shard.file_system_id,
            &crate::DfsMetadataShardId::new(&shard.shard_id),
        )
        .unwrap();
    let lease = handle.checkout().unwrap();
    let result;
    if kind == 0 {
        let worker_data = data.clone();
        let worker_paths = paths.clone();
        let worker_peer = peer.clone();
        let worker_preflight = preflight.clone();
        let worker_runtime = runtime.clone();
        let worker_owner = Arc::clone(&owner);
        let (send, receive) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            send.send(
                worker_runtime
                    .block_on(prepare_daemon_dfs_genesis_ingress(
                        &worker_data,
                        &worker_paths,
                        &worker_peer,
                        &command,
                        Some(&worker_preflight),
                        &worker_runtime,
                        &worker_owner,
                    ))
                    .map_err(|error| error.into_dfs_error().to_string()),
            )
            .unwrap()
        });
        let early = receive.recv_timeout(Duration::from_millis(100));
        let requested = interest.is_requested();
        drop(lease);
        drop(admission);
        result = match early {
            Ok(value) => value,
            Err(_) => receive
                .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                .unwrap(),
        };
        worker.join().unwrap();
        assert!(
            !requested,
            "a real placement child must not cancel the owner it needs"
        );
        assert!(result.is_ok());
    } else if kind == 1 {
        preflight.generation = preflight.generation.checked_sub(1).unwrap();
        let stale = runtime.block_on(prepare_daemon_dfs_genesis_ingress(
            &data,
            &paths,
            &peer,
            &command,
            Some(&preflight),
            &runtime,
            &owner,
        ));
        let typed = matches!(
            stale,
            Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(_))
        );
        let requested = interest.is_requested();
        drop(lease);
        drop(admission);
        result = Ok(());
        assert!(
            typed && !requested,
            "stale original authority cannot gain scheduling or publication readiness"
        );
    } else {
        command.routed_message.target_node_id =
            nodes.iter().find(|node| **node != local).unwrap().clone();
        let invalid = native.rpc_request(
            &local,
            &local,
            crate::dfs_message_rpc_request(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        );
        let requested = interest.is_requested();
        drop(lease);
        drop(admission);
        result = Ok(());
        assert!(
            invalid.is_err() && !requested,
            "wrong target must fail before warm owner wait"
        );
    }
    fs::write(data.root().join("warm-genesis-negative.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "kind":kind,"preempted":interest.is_requested(),"result":result,"unchanged_object":format!("{object:?}")
    })).unwrap()).unwrap();
    drop(interest);
    drop(scheduler);
    drop(handle);
    drop(resident);
    drop(slot);
    drop(owner);
    drop(preflight);
    let mut check = native.client(&local).unwrap();
    assert_eq!(
        check
            .posix_stat_distributed("/genesis-cache-ready")
            .unwrap()
            .object_id,
        object.object_id
    );
    assert!(matches!(
        check.posix_stat_distributed("/genesis-cache-renamed"),
        Err(crate::DfsPosixError::NotFound { .. })
    ));
    drop(check);
    native.close().unwrap();
}

#[test]
fn warm_genesis_background_placement_wait_does_not_preempt() {
    warm_negative("native-warm-genesis-background", 0);
}
#[test]
fn warm_genesis_stale_authority_does_not_bypass_owner() {
    warm_negative("native-warm-genesis-stale", 1);
}
#[test]
fn warm_genesis_wrong_target_does_not_touch_owner() {
    warm_negative("native-warm-genesis-wrong-target", 2);
}
