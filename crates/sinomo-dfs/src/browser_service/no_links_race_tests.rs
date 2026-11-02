//! Native races after caller validation and before real receiver admission.
use super::write_options_tests::{native_browser_policy, native_browser_runtime};
use super::*;
use crate::test_runtime::{FaultAction, FaultRule, RpcOrigin, RpcPhase};
use std::sync::mpsc;
use tokio::sync::Semaphore;

const ROOT: &str = "/no-links-race";
const PARENT: &str = "/no-links-race/parent";
const CHILD: &str = "/no-links-race/parent/note.md";
const OUTSIDE: &str = "/no-links-race/outside";
const INITIAL: &[u8] = b"original base";
const COMPETING: &[u8] = b"independent native actor committed this version";
const ORIGINAL: &[u8] = b"the paused original caller must not overwrite the competing state";

#[derive(Clone, Copy, Debug)]
enum Race {
    ParentRecreated,
    ParentLink,
    ConcurrentCas,
}

// Scope cleanup closes the existing negative dispatch gate on every unwind.
// Scoped threads then join the original caller before its fixture can drop.
struct DispatchGateCleanup(Arc<Semaphore>);

impl Drop for DispatchGateCleanup {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn request(
    operation: crate::DaemonDfsBrowserNoLinksOperation,
) -> crate::DaemonDfsBrowserNoLinksRequest {
    crate::DaemonDfsBrowserNoLinksRequest {
        expected_cluster_id: None,
        root: ROOT.into(),
        path: CHILD.into(),
        operation,
    }
}

fn run_native_race(name: &str, race: Race) {
    let (native, nodes) = native_browser_runtime(name, 3);
    let primary = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap()
        .primary_node_id;
    let caller = nodes.iter().find(|node| **node != primary).unwrap().clone();
    native_browser_policy(
        &native.shared_control_plane().unwrap(),
        ROOT,
        crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 2, 3)
            .unwrap(),
    );
    let data = Arc::new(native.data_dir(&caller).clone());
    let backend = crate::DfsBrowserBackend::for_component_data_dir(
        Arc::clone(&data),
        &native.runtime_handle(),
    )
    .unwrap();
    let host = dfs_browser_component_host(&data).unwrap();
    native.runtime_handle().block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            host.ensure_write_configuration_ready(&data, &native.runtime_handle()),
        )
        .await
        .unwrap()
        .unwrap();
    });
    drop(host);
    // Normal independent native effect opener: no rows, permissions, receipts
    // or response DTOs are seeded. The original caller remains a remote client.
    let mut actor = native.metadata_primary_service(PARENT).unwrap();
    assert_eq!(actor.node_id(), &primary);
    actor
        .posix_mkdir_all_beneath_no_links(ROOT, PARENT)
        .unwrap();
    let options = crate::DfsWriteOptions {
        policy: crate::DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    drop(actor);
    let actor_backend = if matches!(race, Race::ConcurrentCas) {
        let actor_node = nodes
            .iter()
            .find(|node| **node != primary && **node != caller)
            .unwrap();
        let actor_data = Arc::new(native.data_dir(actor_node).clone());
        let actor_backend = crate::DfsBrowserBackend::for_component_data_dir(
            Arc::clone(&actor_data),
            &native.runtime_handle(),
        )
        .unwrap();
        let actor_host = dfs_browser_component_host(&actor_data).unwrap();
        native.runtime_handle().block_on(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                actor_host.ensure_write_configuration_ready(&actor_data, &native.runtime_handle()),
            )
            .await
            .unwrap()
            .unwrap();
        });
        Some(actor_backend)
    } else {
        None
    };
    let initial = if matches!(race, Race::ConcurrentCas) {
        // Seed through the actual configured browser/Main route. A raw Core
        // fresh write cannot supply its missing native execution admission.
        let seed = backend
            .no_links(request(
                crate::DaemonDfsBrowserNoLinksOperation::CreateNew {
                    bytes: INITIAL.to_vec(),
                },
            ))
            .unwrap();
        let crate::DaemonDfsBrowserNoLinksResponse::Metadata { metadata } = seed else {
            panic!("normal browser seed failed: {seed:?}")
        };
        let mut inspection = native.metadata_primary_service(CHILD).unwrap();
        let stat = inspection.posix_stat_distributed(CHILD).unwrap();
        assert_eq!(stat.object_id.as_str(), metadata.object_id);
        assert_eq!(stat.version.as_str(), metadata.version);
        assert_eq!(
            inspection.posix_read_file_distributed(CHILD).unwrap(),
            INITIAL
        );
        Some(stat)
    } else {
        None
    };
    let parent = backend
        .lock_service()
        .unwrap()
        .posix_open_beneath_no_links_distributed(ROOT, PARENT)
        .unwrap();
    let operation = match initial.as_ref() {
        Some(stat) => crate::DaemonDfsBrowserNoLinksOperation::ReplaceIfVersion {
            expected_object_id: stat.object_id.to_string(),
            expected_version: stat.version.to_string(),
            bytes: ORIGINAL.to_vec(),
        },
        None => crate::DaemonDfsBrowserNoLinksOperation::CreateNew {
            bytes: ORIGINAL.to_vec(),
        },
    };
    let gate = Arc::new(Semaphore::new(0));
    native.install_fault(FaultRule {
        source: Some(caller.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
    });
    let start = native.rpc_observations().len();
    let operation_started = Instant::now();
    let deadline = operation_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    backend
        .lock_service()
        .unwrap()
        .set_transport_operation_deadline(Some(deadline));
    let task_backend = Arc::clone(&backend);
    let selected = |event: &crate::test_runtime::RpcObservation| {
        event.source == caller
            && event.target == primary
            && event.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
            && event.origin == RpcOrigin::RegisteredHost
    };
    let (preparation, result, original_completed_at) = std::thread::scope(|scope| {
        let (finished_tx, finished_rx) = mpsc::channel();
        let _cleanup = DispatchGateCleanup(Arc::clone(&gate));
        let worker = scope.spawn(move || {
            let result = task_backend.no_links(request(operation));
            // Record the original call's return before controller scheduling or
            // any independent forensic reads can add elapsed time.
            let completed_at = Instant::now();
            let _ = finished_tx.send((completed_at, result));
        });
        let preparation = (|| -> Result<(u64, crate::DfsStat), String> {
            let call_id = loop {
                let observations = native.rpc_observations();
                if let Some(call) = observations
                    .iter()
                    .skip(start)
                    .find(|event| selected(event) && event.phase == RpcPhase::BeforeDispatch)
                {
                    break call.call_id;
                }
                if Instant::now() >= deadline {
                    return Err(
                        "original call never reached the actual before-dispatch boundary".into(),
                    );
                }
                std::thread::yield_now();
            };
            // The existing carrier fault waits before into_test_incoming and before
            // native authentication/admission/cache locks. The browser holds only
            // its private caller mutex and lazy publication provider here. Native
            // replica callbacks use their separate Main lanes, so the other actor
            // can commit without a global/ACK cycle.
            assert!(
                !native.rpc_observations().iter().any(|event| selected(event)
                    && event.call_id == call_id
                    && event.phase == RpcPhase::AfterDispatch)
            );
            let mut actor = if matches!(race, Race::ConcurrentCas) {
                None
            } else {
                Some(
                    native
                        .metadata_primary_service(PARENT)
                        .map_err(|e| e.to_string())?,
                )
            };
            if let Some(actor) = actor.as_mut() {
                actor.set_transport_operation_deadline(Some(deadline));
            }
            let changed = match race {
                Race::ParentRecreated => {
                    let actor = actor.as_mut().unwrap();
                    actor
                        .posix_rename(PARENT, "/no-links-race/retired")
                        .map_err(|e| e.to_string())?;
                    let changed = actor.posix_mkdir(PARENT).map_err(|e| e.to_string())?;
                    assert_ne!(changed.object_id, parent.object_id);
                    changed
                }
                Race::ParentLink => {
                    let actor = actor.as_mut().unwrap();
                    actor
                        .posix_rename(PARENT, "/no-links-race/retired")
                        .map_err(|e| e.to_string())?;
                    actor.posix_mkdir(OUTSIDE).map_err(|e| e.to_string())?;
                    actor
                        .posix_symlink(OUTSIDE, PARENT, options.clone())
                        .map_err(|e| e.to_string())?;
                    actor
                        .posix_namespace_lstat_distributed(PARENT)
                        .map_err(|e| e.to_string())?
                }
                Race::ConcurrentCas => {
                    let actor = actor_backend.as_ref().unwrap();
                    actor
                        .lock_service()
                        .unwrap()
                        .set_transport_operation_deadline(Some(deadline));
                    let base = initial.as_ref().unwrap();
                    let response = actor
                        .no_links(request(
                            crate::DaemonDfsBrowserNoLinksOperation::ReplaceIfVersion {
                                expected_object_id: base.object_id.to_string(),
                                expected_version: base.version.to_string(),
                                bytes: COMPETING.to_vec(),
                            },
                        ))
                        .map_err(|e| e.to_string())?;
                    let crate::DaemonDfsBrowserNoLinksResponse::Metadata { metadata } = response
                    else {
                        return Err(format!(
                            "independent configured browser update failed: {response:?}"
                        ));
                    };
                    let mut inspection = native
                        .metadata_primary_service(CHILD)
                        .map_err(|e| e.to_string())?;
                    let changed = inspection
                        .posix_stat_distributed(CHILD)
                        .map_err(|e| e.to_string())?;
                    assert_eq!(changed.object_id.as_str(), metadata.object_id);
                    assert_eq!(changed.version.as_str(), metadata.version);
                    assert_eq!(
                        inspection.posix_read_file_distributed(CHILD).unwrap(),
                        COMPETING
                    );
                    assert_ne!(changed.version, base.version);
                    changed
                }
            };
            if Instant::now() >= deadline {
                return Err(
                    "independent native mutation exceeded the original five-second budget".into(),
                );
            }
            Ok((call_id, changed))
        })();
        // Release the one original after the real competing effect. Any
        // preparation panic instead closes the gate via RAII and scope joins.
        gate.add_permits(1);
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (completed_at, result) = match finished_rx.recv_timeout(remaining) {
            Ok(completed) => completed,
            Err(error) => {
                gate.close();
                if let Err(panic) = worker.join() {
                    std::panic::resume_unwind(panic);
                }
                panic!(
                    "original bounded native caller failed to return: {error}; setup={preparation:?}"
                );
            }
        };
        if let Err(panic) = worker.join() {
            std::panic::resume_unwind(panic);
        }
        (preparation, result, completed_at)
    });
    native.clear_faults();
    let (call_id, changed) =
        preparation.expect("actual paused call and independent native mutation");
    let calls = native
        .rpc_observations()
        .into_iter()
        .skip(start)
        .filter(&selected)
        .collect::<Vec<_>>();
    let attempts = calls
        .iter()
        .filter(|event| event.phase == RpcPhase::BeforeDispatch)
        .count();
    let completed = calls
        .iter()
        .find(|event| event.call_id == call_id && event.phase == RpcPhase::Completed)
        .cloned();
    let mut inspection = native.metadata_primary_service(CHILD).unwrap();
    let visible = inspection.posix_namespace_lstat_distributed(CHILD);
    let visible_bytes = inspection.posix_read_file_distributed(CHILD);
    let outside = inspection.posix_namespace_lstat_distributed("/no-links-race/outside/note.md");
    let hinted_object = calls.iter().find_map(|event| event.dfs_object_id.clone());
    let hinted_request = calls.iter().find_map(|event| event.dfs_request_id.clone());
    // Exclusive-create carrier observations may omit these optional hints.
    // Preserve the actual reply/current native object, with every matching
    // intent, rather than inventing a unique original-intent association.
    let observed_object = hinted_object.clone().or_else(|| match &result {
        Ok(crate::DaemonDfsBrowserNoLinksResponse::Metadata { metadata }) => {
            Some(crate::DfsObjectId::new(&metadata.object_id).unwrap())
        }
        _ => visible
            .as_ref()
            .ok()
            .map(|stat| stat.object_id.clone())
            .or_else(|| initial.as_ref().map(|stat| stat.object_id.clone())),
    });
    let native_intents = observed_object.as_ref().map(|object| {
        inspection
            .metadata_backend()
            .unwrap()
            .intents_for_object(object)
            .unwrap()
    });
    let hint_matched_intents = hinted_request.as_ref().and_then(|request| {
        native_intents.as_ref().map(|intents| {
            intents
                .iter()
                .filter(|intent| intent.operation_id.as_ref() == Some(request))
                .collect::<Vec<_>>()
        })
    });
    let native_content = native_intents.as_ref().map(|intents| {
        intents
            .iter()
            .map(|intent| {
                serde_json::json!({
                    "object_id":intent.object_id,"version":intent.target_version,
                    "content_hash":intent.content_hash,
                    "actual_read":format!("{:?}", inspection.content().read_object_version(
                        intent.object_id.as_str(), intent.target_version.as_str()))
                })
            })
            .collect::<Vec<_>>()
    });
    let expected_cas_version = initial.as_ref().map(|stat| stat.version.to_string());
    let outcome = serde_json::json!({
        "race":format!("{race:?}"),"original_parent":format!("{parent:?}"),
        "original_cas_version":expected_cas_version,"changed":format!("{changed:?}"),
        "original_result":format!("{result:?}"),"selected_call_id":call_id,
        "carrier_object_hint":hinted_object,"carrier_request_hint":hinted_request,
        "observed_native_object":observed_object,"all_native_object_intents":native_intents,
        "hint_matched_intents":hint_matched_intents,"actual_native_content":native_content,
        "original_elapsed_us":original_completed_at.saturating_duration_since(operation_started).as_micros(),
        "identity_join_qualification":"without a carrier request hint, native object history is retained in full; no unique original-intent join is asserted",
        "actual_native_calls":format!("{calls:#?}"),"semantic_sends":attempts,
        "visible":format!("{visible:?}"),"bytes":format!("{visible_bytes:?}"),
        "outside":format!("{outside:?}"),
        "qualification":"real native effect after caller validation; expected conflict/no effect is the oracle, not a manufactured response"
    });
    std::fs::write(
        native
            .data_dir(&caller)
            .root()
            .join("no-links-race-result.json"),
        serde_json::to_vec_pretty(&outcome).unwrap(),
    )
    .unwrap();
    // Preserve actual outcome and state before asserting the missing contract.
    assert_eq!(
        attempts, 1,
        "only the original semantic operation is dispatched"
    );
    let completed =
        completed.expect("the real authenticated native owner must return the original call");
    assert!(completed.signer.is_some());

    assert!(
        original_completed_at < deadline,
        "original five-second deadline is unchanged"
    );
    let conflict = matches!(
        &result,
        Ok(crate::DaemonDfsBrowserNoLinksResponse::Failed {
            failure: crate::DaemonDfsBrowserNoLinksFailure::Conflict { .. }
        })
    );
    let no_effect = match race {
        Race::ConcurrentCas => {
            matches!(&visible, Ok(stat) if stat.object_id == changed.object_id
            && stat.version == changed.version)
                && visible_bytes.as_ref().is_ok_and(|bytes| bytes == COMPETING)
        }
        Race::ParentRecreated => matches!(&visible, Err(crate::DfsPosixError::NotFound { .. })),
        Race::ParentLink => {
            matches!(&outside, Err(crate::DfsPosixError::NotFound { .. }))
                && matches!(&visible, Err(crate::DfsPosixError::NotFound { .. }))
        }
    };
    drop(inspection);
    drop(actor_backend);
    drop(backend);
    native.close().unwrap();
    assert!(
        conflict && no_effect,
        "cross-RPC no-links precondition was not enforced: {outcome}"
    );
}

#[test]
fn no_links_remote_parent_recreated_after_validation_conflicts_without_effect() {
    run_native_race(
        "native-no-links-parent-dispatch-race",
        Race::ParentRecreated,
    );
}

#[test]
fn no_links_remote_parent_linked_after_validation_conflicts_without_outside_effect() {
    run_native_race("native-no-links-parent-link-race", Race::ParentLink);
}

#[test]
fn no_links_remote_cas_changed_after_open_conflicts_without_overwrite() {
    run_native_race("native-no-links-cas-dispatch-race", Race::ConcurrentCas);
}

#[test]
fn no_links_same_main_committed_reply_does_not_follow_a_replacement_symlink() {
    use crate::DaemonDfsBrowserNoLinksOperation as O;
    use crate::DaemonDfsBrowserNoLinksResponse as R;
    const RETIRED: &str = "/no-links-race/parent/committed.md";
    const OTHER: &str = "/postcommit-outside.md";
    const OTHER_BYTES: &[u8] = b"outside-root content must never become the original receipt";
    let (native, nodes) = native_browser_runtime("native-no-links-postcommit-symlink", 1);
    let primary = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap()
        .primary_node_id;
    native_browser_policy(
        &native.shared_control_plane().unwrap(),
        ROOT,
        crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1)
            .unwrap(),
    );
    let data = Arc::new(native.data_dir(&primary).clone());
    let backend =
        DfsBrowserBackend::for_component_data_dir(Arc::clone(&data), &native.runtime_handle())
            .unwrap();
    let host = dfs_browser_component_host(&data).unwrap();
    native.runtime_handle().block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            host.ensure_write_configuration_ready(&data, &native.runtime_handle()),
        )
        .await
        .unwrap()
        .unwrap();
    });
    backend
        .no_links(crate::DaemonDfsBrowserNoLinksRequest {
            expected_cluster_id: None,
            root: "/".into(),
            path: PARENT.into(),
            operation: O::CreateDirAll,
        })
        .unwrap();
    let outside_reply = backend
        .no_links(crate::DaemonDfsBrowserNoLinksRequest {
            expected_cluster_id: None,
            root: "/".into(),
            path: OTHER.into(),
            operation: O::CreateNew {
                bytes: OTHER_BYTES.to_vec(),
            },
        })
        .unwrap();
    let R::Metadata { metadata: other } = outside_reply else {
        panic!("outside setup: {outside_reply:?}")
    };
    let gate = Arc::new(Semaphore::new(0));
    native.install_fault(FaultRule {
        source: Some(primary.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
    });
    let capture = native.capture_served_work(&primary, 32).unwrap();
    let start = native.rpc_observations().len();
    let started = Instant::now();
    let deadline = started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    backend
        .lock_service()
        .unwrap()
        .set_transport_operation_deadline(Some(deadline));
    let worker_backend = Arc::clone(&backend);
    let selected = |event: &crate::test_runtime::RpcObservation| {
        event.source == primary
            && event.target == primary
            && event.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
            && event.origin == RpcOrigin::RegisteredHost
    };
    let (preparation, result, completed_at) = std::thread::scope(|scope| {
        let _cleanup = DispatchGateCleanup(Arc::clone(&gate));
        let gate_owner_baseline = Arc::strong_count(&gate);
        let (send, receive) = mpsc::channel();
        let worker = scope.spawn(move || {
            let result = worker_backend.no_links(request(O::CreateNew {
                bytes: ORIGINAL.to_vec(),
            }));
            let _ = send.send((Instant::now(), result));
        });
        let preparation = (|| -> Result<_, String> {
            let call_id = loop {
                if let Some(call) = native
                    .rpc_observations()
                    .iter()
                    .skip(start)
                    .find(|event| selected(event) && event.phase == RpcPhase::AfterDispatch)
                {
                    break call.call_id;
                }
                if Instant::now() >= deadline {
                    return Err("original never completed native dispatch".into());
                }
                std::thread::yield_now();
            };
            native.assert_fault_budgets()?;
            assert!(
                Arc::strong_count(&gate) > gate_owner_baseline,
                "the actual carrier must retain its selected response gate"
            );
            assert!(
                matches!(receive.try_recv(), Err(mpsc::TryRecvError::Empty)),
                "the real completed response must remain held"
            );
            assert!(
                !native
                    .rpc_observations()
                    .iter()
                    .skip(start)
                    .any(|event| selected(event)
                        && event.call_id == call_id
                        && event.phase == RpcPhase::Completed)
            );
            let mut owner = native
                .metadata_primary_service(CHILD)
                .map_err(|e| e.to_string())?;
            owner.set_transport_operation_deadline(Some(deadline));
            let original = owner
                .posix_namespace_lstat_distributed(CHILD)
                .map_err(|e| e.to_string())?;
            assert_eq!(original.content_hash, Some(crate::content_hash(ORIGINAL)));
            let intents = owner
                .metadata_backend()
                .unwrap()
                .intents_for_object(&original.object_id)
                .map_err(|e| e.to_string())?
                .into_iter()
                .filter(|intent| {
                    intent.status == DfsWriteIntentStatus::Committed
                        && intent.target_version == original.version
                })
                .collect::<Vec<_>>();
            assert_eq!(
                intents.len(),
                1,
                "genuine committed original before the competing effect"
            );
            assert_eq!(
                (
                    intents[0].policy.min_commit_replicas,
                    intents[0].policy.desired_replicas
                ),
                (1, 1)
            );
            let prepared = owner
                .metadata_backend()
                .unwrap()
                .prepared_operation_record(&format!("write:{}", intents[0].intent_id))
                .map_err(|e| e.to_string())?
                .ok_or("committed original omitted its native body")?;
            prepared.validate().map_err(|e| e.to_string())?;
            assert_eq!(prepared.status, DfsPreparedOperationStatus::Committed);
            owner
                .posix_rename(CHILD, RETIRED)
                .map_err(|e| e.to_string())?;
            let retired = owner
                .posix_namespace_lstat_distributed(RETIRED)
                .map_err(|e| e.to_string())?;
            owner
                .posix_symlink(
                    OTHER,
                    CHILD,
                    crate::DfsWriteOptions::replicated_eventual(nodes.clone()),
                )
                .map_err(|e| e.to_string())?;
            let link = owner
                .posix_namespace_lstat_distributed(CHILD)
                .map_err(|e| e.to_string())?;
            assert_eq!(link.kind, DfsObjectKind::Symlink);
            assert_ne!(link.object_id, original.object_id);
            assert_eq!(
                owner
                    .posix_read_file_distributed(OTHER)
                    .map_err(|e| e.to_string())?,
                OTHER_BYTES
            );
            assert!(
                Instant::now() < deadline,
                "competing native effects retain the original five-second budget"
            );
            assert!(
                !native
                    .rpc_observations()
                    .iter()
                    .skip(start)
                    .any(|event| selected(event)
                        && event.call_id == call_id
                        && event.phase == RpcPhase::Completed),
                "the original reply stays held through both genuine competing effects"
            );
            Ok((call_id, original, prepared, retired, link))
        })();
        gate.add_permits(1);
        let (completed_at, result) = match receive
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            Ok(result) => result,
            Err(error) => {
                gate.close();
                if let Err(panic) = worker.join() {
                    std::panic::resume_unwind(panic);
                }
                panic!("original bounded caller failed to return: {error}; setup={preparation:?}");
            }
        };
        if let Err(panic) = worker.join() {
            std::panic::resume_unwind(panic);
        }
        (preparation, result, completed_at)
    });
    native.clear_faults();
    let (call_id, original, prepared, retired, link) =
        preparation.expect("actual committed original and replacement symlink");
    let calls = native
        .rpc_observations()
        .into_iter()
        .skip(start)
        .filter(selected)
        .collect::<Vec<_>>();
    let reports = capture.records();
    drop(capture);
    let original_reports = reports.iter().filter(|work| {
        work.succeeded && matches!(&work.command.routed_message.message,
            DfsNetworkMessage::ExclusiveCreateRequest(command)
                if command.request.object_id == original.object_id && command.request.target_version == original.version)
    }).collect::<Vec<_>>();
    let facts = serde_json::json!({"original":format!("{original:?}"),"prepared":prepared,"retired":format!("{retired:?}"),"replacement_link":format!("{link:?}"),
        "outside":other,"result":format!("{result:?}"),"calls":format!("{calls:?}"),
        "original_work":original_reports.iter().map(|work| &work.command).collect::<Vec<_>>(),
        "elapsed_us":completed_at.saturating_duration_since(started).as_micros(),
        "qualification":"actual committed Main reply held by existing carrier gate; no replay, synthetic reply or authority; source-only helper author made no runtime claim"});
    let export = std::fs::write(
        data.root().join("no-links-postcommit-symlink.json"),
        serde_json::to_vec_pretty(&facts).unwrap(),
    );
    if let Err(error) = &export {
        eprintln!("postcommit race evidence export failed: {error}");
    }
    assert_eq!(original_reports.len(), 1);
    let frame = &original_reports[0].command.routed_message;
    let DfsNetworkMessage::ExclusiveCreateRequest(command) = &frame.message else {
        unreachable!("the successful original report was matched above")
    };
    assert!(calls.iter().any(|call| {
        call.call_id == call_id
            && call.phase == RpcPhase::Decoded
            && call.dfs_frame_id == Some(frame.frame_id)
            && call.dfs_request_id.as_ref() == Some(&command.request_id)
    }));
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.phase == RpcPhase::BeforeDispatch)
            .count(),
        1,
        "a postcommit observation must not reauthor the original"
    );
    assert!(calls.iter().any(|call| call.call_id == call_id
        && call.phase == RpcPhase::Completed
        && call.outcome_error.is_none()));
    assert!(completed_at < deadline);
    let R::Metadata { metadata } = result.unwrap() else {
        panic!("committed original must retain its current object receipt: {facts}")
    };
    assert_eq!(metadata.object_id, original.object_id.as_str());
    assert_ne!(metadata.object_id, other.object_id);
    assert_eq!(metadata.content_hash, Some(crate::content_hash(ORIGINAL)));
    assert_ne!(metadata.content_hash, other.content_hash);
    let mut expected = daemon_dfs_browser_stat_response(retired, Vec::new(), None, None, None);
    expected.path = CHILD.into();
    assert_eq!(
        serde_json::to_value(&metadata).unwrap(),
        serde_json::to_value(&expected).unwrap()
    );
    export.expect("postcommit race evidence retained after business assertions");
    drop(backend);
    drop(host);
    native.close().unwrap();
}
