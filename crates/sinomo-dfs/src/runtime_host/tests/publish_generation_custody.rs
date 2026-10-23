use super::super::message_host::{
    DaemonDfsPreDispatchTestPoint, install_daemon_dfs_pre_dispatch_test_hook,
};
use super::*;

fn generation_child_is_owned(kind: &'static str) {
    let name = format!("native-publish-{kind}-generation-custody");
    let (mut native, _) = crate::test_runtime::DfsTestRuntime::start_cluster(&name, 2).unwrap();
    let shard = native.root_shard().unwrap();
    let node = if kind == "lane" {
        shard.primary_node_id.clone()
    } else {
        shard.replica_node_ids[0].clone()
    };
    let data = native.data_dir(&node).clone();
    let paths = DfsRuntimePaths::new(&data);
    let cluster_paths = ClusterPaths::new(&data);
    let runtime = native.runtime_handle();
    let host = registered_dfs_runtime_host(&data).unwrap().unwrap();
    let scheduler = daemon_dfs_mutation_scheduler(&data).unwrap();
    let owner = require_daemon_dfs_data_owner_lock(&paths).unwrap();
    let owner_identity = owner.storage_owner_file_identity().unwrap();
    let owner_token = Arc::as_ptr(&owner) as usize;
    let weak_owner = Arc::downgrade(&owner);
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let idle_preflight = if kind == "idle" {
        Some(runtime.block_on(async {
            let peer =
                read_daemon_dfs_message_peer_snapshot_before(&cluster_paths, &runtime, deadline)
                    .await
                    .unwrap()
                    .unwrap();
            let preflight = warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
                &cluster_paths,
                &peer,
                &runtime,
                deadline,
                None,
            )
            .await
            .unwrap();
            (peer, preflight)
        }))
    } else {
        None
    };
    let armed = Arc::new(AtomicBool::new(false));
    let used = Arc::new(AtomicBool::new(false));
    let (requested_tx, requested_rx) = std::sync::mpsc::sync_channel(1);
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let hook_armed = Arc::clone(&armed);
    let hook_used = Arc::clone(&used);
    let controlled_task = Arc::new(Mutex::new(None));
    let hook_controlled_task = Arc::clone(&controlled_task);
    let hook = install_daemon_dfs_pre_dispatch_test_hook(
        &data,
        Arc::new(move |point, _, preflight| {
            if !hook_armed.load(Ordering::Acquire) {
                return Ok(());
            }
            match point {
                DaemonDfsPreDispatchTestPoint::PublishGenerationAdmissionRequested {
                    owner,
                    caller_task,
                } if owner == kind && caller_task == *hook_controlled_task.lock().unwrap() => {
                    let _ = requested_tx.try_send(());
                }
                DaemonDfsPreDispatchTestPoint::PublishGenerationWorkerStarted {
                    owner,
                    caller_task,
                    global_admission,
                    data_owner_token,
                } if owner == kind
                    && caller_task == *hook_controlled_task.lock().unwrap()
                    && !hook_used.swap(true, Ordering::AcqRel) =>
                {
                    assert!(preflight.is_some(), "actual native authority preflight");
                    let task =
                        tokio::task::try_id().expect("actual spawned blocking task identity");
                    if entered_tx
                        .send((
                            task,
                            caller_task,
                            global_admission,
                            data_owner_token,
                            preflight.unwrap().generation,
                        ))
                        .is_ok()
                    {
                        // Dropping the test's sender also releases this negative
                        // pause on unwind. No unrelated work or owner is stopped.
                        let _ = release_rx.lock().unwrap().recv();
                    }
                }
                _ => {}
            }
            Ok(())
        }),
    );
    let global = runtime
        .block_on(scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
        .unwrap();
    // Evict only the derived entry under its actual global fence. Preserve the
    // same root slot, native authority, DATA, journal and OS owner.
    {
        let slot = daemon_dfs_message_service_cache()
            .root(&data.root())
            .unwrap();
        assert!(slot.lock().unwrap().take().is_some());
    }
    armed.store(true, Ordering::Release);
    let task_runtime = runtime.clone();
    let task_data = data.clone();
    let task_paths = cluster_paths.clone();
    let task_scheduler = Arc::clone(&scheduler);
    let lane = DaemonDfsMutationLaneKey {
        file_system_id: shard.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
    };
    let supplied_owner = if kind == "lane" {
        Some(owner)
    } else {
        drop(owner);
        None
    };
    let (result_tx, mut result_rx) = tokio::sync::oneshot::channel();
    let (begin_tx, begin_rx) = tokio::sync::oneshot::channel();
    let outer = runtime.spawn(async move {
        begin_rx.await.unwrap();
        let result = if let Some((peer, preflight)) = idle_preflight {
            super::super::publish_host::ensure_daemon_dfs_idle_resident_generation_after_preflight(
                task_data,
                task_paths,
                peer,
                preflight,
                task_runtime,
            )
            .await
        } else {
            super::super::publish_host::ensure_daemon_dfs_metadata_publish_resident_generation(
                task_scheduler,
                task_data,
                task_paths,
                lane,
                task_runtime,
                supplied_owner.unwrap(),
            )
            .await
            .map(|_| ())
        };
        let _ = result_tx.send(result);
    });
    let outer_id = outer.id();
    *controlled_task.lock().unwrap() = Some(outer_id);
    let cancel = outer.abort_handle();
    // The ordinary host owns the scheduler as well as the child under test.
    host.own_task(outer);
    begin_tx.send(()).unwrap();
    requested_rx
        .recv_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()))
        .expect("actual cold publisher branch selected while global admission is held");
    drop(global);
    let (child_id, child_caller, child_global, child_owner, child_generation) = entered_rx
        .recv_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()))
        .expect("actual native generation child reached its held-owner boundary");
    let registered = runtime.block_on(async {
        let until = deadline.min(tokio::time::Instant::now() + Duration::from_secs(1));
        loop {
            if host
                .tasks
                .lock()
                .unwrap()
                .iter()
                .any(|task| task.id() == child_id)
            {
                break true;
            }
            if tokio::time::Instant::now() >= until {
                break false;
            }
            tokio::task::yield_now().await;
        }
    });
    cancel.abort();
    let mut draining = Box::pin(host.shutdown_and_wait(deadline));
    let early = runtime.block_on(std::future::poll_fn(|cx| {
        use std::future::Future;
        std::task::Poll::Ready(match draining.as_mut().poll(cx) {
            std::task::Poll::Ready(result) => Some(result),
            std::task::Poll::Pending => None,
        })
    }));
    let returned_while_held = early.is_some();
    release_tx.send(()).unwrap();
    if let Some(result) = early {
        result.unwrap();
    } else {
        runtime.block_on(draining.as_mut()).unwrap();
    }
    drop(draining);
    // On the unrepaired control, join the same physical admission after
    // releasing the child so even failed evidence drains normally.
    let final_global = runtime.block_on(async {
        tokio::time::timeout_at(
            deadline,
            scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive),
        )
        .await
        .unwrap()
        .unwrap()
    });
    drop(final_global);
    std::fs::write(data.root().join("boundary-diagnostic-publish-generation-custody.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "owner_kind":kind,"root":data.root(),"node":node,"actual_child_task":child_id.to_string(),
            "controlled_call_task":outer_id.to_string(),"controlled_call_result":format!("{:?}",result_rx.try_recv()),"child_held_global_admission":child_global,
            "actual_child_caller_task":child_caller.map(|id|id.to_string()),"actual_child_authority_generation":child_generation,
            "actual_data_owner_token":child_owner,"expected_data_owner_token":owner_token,
            "physical_owner_file":format!("{owner_identity:?}"),"actual_child_registered":registered,
            "drain_returned_while_child_held":returned_while_held,
            "qualification":"Actual normal native owner, derived cold-cache entry, and production generation worker. No replacement writer, supplied reply, or cleared ownership lock."
        })).unwrap()).unwrap();
    drop(hook);
    drop(host);
    native.stop_node(&node).unwrap();
    let old_owner_released = weak_owner.upgrade().is_none();
    native.reopen_node(node).unwrap();
    let reopened = require_daemon_dfs_data_owner_lock(&paths).unwrap();
    assert_eq!(
        reopened.storage_owner_file_identity().unwrap(),
        owner_identity
    );
    drop(reopened);
    native.close().unwrap();
    assert_ne!(child_id, outer_id);
    assert_eq!(child_caller, Some(outer_id));
    assert!(child_global);
    assert_eq!(child_owner, owner_token);
    assert!(
        old_owner_released,
        "normal node stop releases the actual old owner"
    );
    assert!(
        registered,
        "actual generation child must be registered with the native host before its caller can be cancelled"
    );
    assert!(
        !returned_while_held,
        "native drain must join that same held child"
    );
}

#[test]
fn native_publish_lane_generation_child_belongs_to_actual_host_drain() {
    generation_child_is_owned("lane");
}

#[test]
fn native_publish_idle_generation_child_belongs_to_actual_host_drain() {
    generation_child_is_owned("idle");
}
