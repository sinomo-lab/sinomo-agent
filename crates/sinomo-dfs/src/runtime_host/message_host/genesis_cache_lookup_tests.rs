use super::*;
use std::sync::mpsc;

// These controls deliberately hold the process-wide physical-owner registry.
// Keep their own healthy-status setup outside another control's forced pause.
static CONTENTION_CONTROL: Mutex<()> = Mutex::new(());

fn required_genesis_lookup_waits_for_cache_ownership(contention: u8) {
    // This gate carries no state. A failed assertion must not poison the
    // other independently checked contention controls.
    let _control = CONTENTION_CONTROL.lock().unwrap_or_else(|e| e.into_inner());
    let name = match contention {
        0 => "native-genesis-physical-registry-contention",
        1 => "native-genesis-root-slot-contention",
        2 => "native-genesis-inspection-fence-contention",
        _ => "native-genesis-shard-checkout-contention",
    };
    let (native, nodes) = crate::test_support::DfsTestRuntime::start_cluster(name, 1).unwrap();
    let local = nodes[0].clone();
    let data = native.data_dir(&local).clone();
    let mut client = native.client(&local).unwrap();
    client.posix_mkdir("/genesis-cache-ready").unwrap();
    drop(client);
    let paths = ClusterPaths::new(&data);
    let runtime = native.runtime_handle();
    let peer = read_daemon_dfs_message_peer_snapshot(&paths)
        .unwrap()
        .unwrap();
    let preflight = runtime
        .block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
            &paths, &peer, &runtime,
        ))
        .unwrap();
    let command = mutation_for_genesis_check(&native, &local);
    // Match normal dispatch: prepare the actual resident scope, then acquire
    // its existing mutation admission before the final pre-effect check.
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
    let admission = runtime.block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive),
        )
        .await
        .unwrap()
        .unwrap()
    });
    let preflight = preflight.for_admission(&admission);
    reject_daemon_dfs_incomplete_genesis_before_effects(&data, &peer, &command, Some(&preflight), None)
        .unwrap();
    // Setup borrows the resident through normal ownership below. An
    // opportunistic status peek may legitimately be unavailable while another
    // native fixture updates the shared cache; the controlled status assertion
    // belongs after the specific contention has been installed.
    let slot = daemon_dfs_message_service_cache()
        .existing_root(&data.root())
        .unwrap()
        .unwrap();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let (status_tx, status_rx) = mpsc::sync_channel(1);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
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
    let release: Box<dyn FnOnce() + '_> = if contention == 0 {
        let held = DAEMON_DFS_DATA_OWNER_LOCKS.get().unwrap().lock().unwrap();
        Box::new(move || drop(held))
    } else if contention == 1 {
        let held = slot.lock().unwrap();
        Box::new(move || drop(held))
    } else if contention == 2 {
        let root = crate::local_dfs_metadata_shard_path(
            &data,
            &crate::DfsMetadataShardId::new(&shard.shard_id),
        );
        let held = crate::metadata_lock::metadata_write_lock(
            root.clone(),
            crate::metadata_cursor::journal_write_lock_path(&root),
        )
        .unwrap();
        Box::new(move || drop(held))
    } else {
        let held = handle.checkout().unwrap();
        Box::new(move || drop(held))
    };
    let worker_data = data.clone();
    let worker_peer = peer.clone();
    let worker_command = command.clone();
    let worker_preflight = preflight.clone();
    let worker_runtime = runtime.clone();
    let worker = std::thread::spawn(move || {
        let _entered = worker_runtime.enter();
        entered_tx.send(()).unwrap();
        let result = reject_daemon_dfs_incomplete_genesis_before_effects(
            &worker_data,
            &worker_peer,
            &worker_command,
            Some(&worker_preflight),
            None,
        )
        .map_err(|error| error.into_dfs_error().to_string());
        result_tx.send(result).unwrap();
    });
    let status_data = data.clone();
    let status_peer = peer.clone();
    // Cache lookup has its own busy controls above. For physical-fence and
    // checkout contention, pin the same already-published owner during setup
    // and read its actual maintained status while that contention is held.
    // This isolates the intended boundary from unrelated fixture registration
    // and tests a real snapshot read, not only a successful cache lookup.
    let status_resident = (contention >= 2).then(|| resident.clone());
    let observer = std::thread::spawn(move || {
        let result = match status_resident {
            Some(resident) => resident
                .bounded_status_snapshot()
                .map(|_| ())
                .map_err(|error| error.to_string()),
            None => daemon_dfs_bounded_status_resident(&status_data, &status_peer)
                .map(|_| ())
                .map_err(|error| error.to_string()),
        };
        status_tx.send(result).unwrap();
    });
    // Hold the actual lock, observe both paths, then release it before any
    // assertion/unwind. This adds no retry or sleep to production dispatch.
    let entered = entered_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    let status = status_rx.recv_timeout(Duration::from_millis(200));
    let early = result_rx.recv_timeout(Duration::from_millis(100));
    let waited = matches!(early, Err(mpsc::RecvTimeoutError::Timeout));
    release();
    let result = match early {
        Ok(result) => result,
        Err(_) => result_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("required check completes after the real lock releases"),
    };
    worker.join().unwrap();
    observer.join().unwrap();
    drop(preflight);
    drop(admission);
    drop(owner);
    let mut client = native.client(&local).unwrap();
    let actual_write = client.posix_mkdir("/genesis-cache-after-release");
    drop(client);
    std::fs::write(data.root().join("boundary-diagnostic-genesis-cache-lookup.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "contention":contention,"request":command,"entered":entered.is_ok(),
            "bounded_status":format!("{status:?}"),"required_waited":waited,
            "required_result":result,"actual_after_release_mutation":format!("{actual_write:?}"),
            "qualification":"Actual initialized native owner/preflight and held existing lock. Guard observation performs no effect; normal native mutation is checked after release."
        })).unwrap()).unwrap();
    drop(slot);
    native.close().unwrap();
    assert!(entered.is_ok());
    assert!(
        if contention <= 1 {
            matches!(status, Ok(Err(ref error)) if error.contains("bounded DFS status unavailable"))
        } else {
            matches!(status, Ok(Ok(())))
        },
        "status must remain bounded while the cache is held: {status:?}"
    );
    assert!(
        waited,
        "required genesis validation must use normal cache ownership, not an opportunistic status peek: {result:?}"
    );
    assert!(
        result.is_ok(),
        "initialized native genesis remains usable after contention: {result:?}"
    );
    assert!(
        actual_write.is_ok(),
        "normal native write must remain usable: {actual_write:?}"
    );
}

#[test]
fn required_genesis_check_waits_for_physical_registry_without_blocking_status() {
    required_genesis_lookup_waits_for_cache_ownership(0);
}

#[test]
fn required_genesis_check_waits_for_root_slot_without_blocking_status() {
    required_genesis_lookup_waits_for_cache_ownership(1);
}

fn mutation_for_genesis_check(
    native: &crate::test_support::DfsTestRuntime,
    local: &NodeId,
) -> DfsMessageCommand {
    let shard = native.root_shard().unwrap();
    DfsMessageCommand {
        request_id: format!("required-genesis-{}", sinomo_core::Id::generate()),
        routed_message: crate::build_dfs_routed_message(
            &local,
            &local,
            1,
            1,
            crate::DfsNetworkMessage::NamespaceRequest(crate::DfsNamespaceCommand {
                storage_revision: None,
                request_id: format!("genesis-check-{}", sinomo_core::Id::generate()),
                file_system_id: shard.file_system_id.clone(),
                requesting_node_id: local.clone(),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: vec![shard],
                operation: crate::DfsNamespaceOperation::Rename {
                    from: "/genesis-cache-ready".into(),
                    to: "/genesis-cache-renamed".into(),
                },
            }),
        ),
    }
}

#[test]
fn genesis_preparation_and_final_fence_preserve_typed_stale_authority() {
    let (native, nodes) =
        crate::test_support::DfsTestRuntime::start_cluster("native-genesis-typed-authority", 1)
            .unwrap();
    let data = native.data_dir(&nodes[0]).clone();
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
    // Negative input only: retain the genuine owner but present an earlier
    // expected generation. Neither gate may turn this into a terminal storage
    // error, create an owner, or confer readiness on an absent preflight.
    preflight.generation = preflight.generation.checked_sub(1).unwrap();
    let command = mutation_for_genesis_check(&native, &nodes[0]);
    let owner = require_daemon_dfs_data_owner_lock(&DaemonPaths::new(&data)).unwrap();
    assert!(matches!(
        runtime.block_on(prepare_daemon_dfs_genesis_ingress(
            &data,
            &paths,
            &peer,
            &command,
            Some(&preflight),
            &runtime,
            &owner,
        )),
        Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(_))
    ));
    assert!(matches!(
        reject_daemon_dfs_incomplete_genesis_before_effects(
            &data,
            &peer,
            &command,
            Some(&preflight),
            None,
        ),
        Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(_))
    ));
    assert!(matches!(
        reject_daemon_dfs_incomplete_genesis_before_effects(&data, &peer, &command, None, None),
        Err(DaemonDfsAuthorityPreExecutionError::Failed(_))
    ));
    drop(owner);
    drop(preflight);
    native.close().unwrap();
}

#[test]
fn required_genesis_check_waits_for_inspection_fence_without_blocking_status() {
    required_genesis_lookup_waits_for_cache_ownership(2);
}
#[test]
fn required_genesis_check_waits_for_shard_checkout_without_blocking_status() {
    required_genesis_lookup_waits_for_cache_ownership(3);
}

#[test]
fn prepared_genesis_cut_waits_for_inspection_fence_after_actual_preparation() {
    let _control = CONTENTION_CONTROL.lock().unwrap();
    let (native, nodes) =
        crate::test_support::DfsTestRuntime::start_cluster("genesis-post-preparation-fence", 1)
            .unwrap();
    let data = native.data_dir(&nodes[0]).clone();
    let mut client = native.client(&nodes[0]).unwrap();
    client.posix_mkdir("/prepared-genesis-fence").unwrap();
    drop(client);
    let shard = native.root_shard().unwrap();
    let runtime = native.runtime_handle();
    let scheduler = daemon_dfs_mutation_scheduler(&data).unwrap();
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
    let id = crate::DfsMetadataShardId::new(&shard.shard_id);
    let handle = resident.scope_shard(&shard.file_system_id, &id).unwrap();
    let root = crate::local_dfs_metadata_shard_path(&data, &id);
    let lock = || {
        crate::metadata_lock::metadata_write_lock(
            root.clone(),
            crate::metadata_cursor::journal_write_lock_path(&root),
        )
        .unwrap()
    };
    let initial = lock();
    let (phase_tx, phase_rx) = mpsc::sync_channel(2);
    let (continue_tx, continue_rx) = mpsc::sync_channel(1);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let worker = std::thread::spawn(move || {
        let _entered = runtime.enter();
        crate::metadata_sharded_backend::GENESIS_PREPARATION_OBSERVER.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |phase| {
                phase_tx.send(phase).unwrap();
                if phase == 1 {
                    continue_rx
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                        .unwrap();
                }
            }));
        });
        let operation = crate::default_cluster::genesis::genesis_operation_id(&shard);
        let result = handle
            .try_prepare_genesis_record_for_ingress(&shard, &operation)
            .map_err(|e| e.to_string());
        crate::metadata_sharded_backend::GENESIS_PREPARATION_OBSERVER.with(|hook| {
            hook.borrow_mut().take();
        });
        result_tx.send(result).unwrap();
    });
    // Force a real initial inspection miss, then let real preparation finish.
    let first = phase_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    drop(initial);
    let prepared = phase_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    let held = lock();
    let released_observer = continue_tx.send(());
    let early = result_rx.recv_timeout(Duration::from_millis(100));
    let waited = matches!(early, Err(mpsc::RecvTimeoutError::Timeout));
    drop(held); // Always release actual fences before assertion/unwind.
    let result = match early {
        Ok(r) => r,
        Err(_) => result_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap(),
    };
    worker.join().unwrap();
    std::fs::write(data.root().join("boundary-diagnostic-genesis-post-preparation.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"first":format!("{first:?}"),"prepared":format!("{prepared:?}"),"waited":waited,"result":format!("{result:?}"),"original_deadline_met":Instant::now()<deadline})).unwrap()).unwrap();
    drop(resident);
    drop(slot);
    drop(admission);
    native.close().unwrap();
    assert_eq!(first.unwrap(), 0);
    assert_eq!(prepared.unwrap(), 1);
    released_observer.unwrap();
    assert!(
        waited,
        "required post-preparation observation must wait for its actual fence: {result:?}"
    );
    assert!(
        matches!(result, Ok(Some(_))),
        "actual prepared cut must remain available: {result:?}"
    );
}

#[path = "genesis_cache_lookup_tests/warm_foreground.rs"]
mod warm_foreground;
