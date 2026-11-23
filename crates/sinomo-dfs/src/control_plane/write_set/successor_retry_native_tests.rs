//! A namespace request retried under its operation id after a takeover, on
//! the native test runtime: the member that took over answers it once, from
//! the prepares and rows the lost primary replicated to the write set.

use super::commit_native_tests::{
    catch_up, directory, hold_replies, primary, release, shard_root, switched_with_kv_voters,
};
use super::takeover_driver_native_tests::{keep_with_paths, lose};
use super::takeover_loss_native_tests::{record_from, wait_until};
use super::takeover_native_tests::shard_id_of;
use super::*;
use crate::test_runtime::{FaultAction, FaultRule, Partition, executor};
use crate::test_support::DfsTestRuntime;
use crate::{
    DfsNamespaceOperation, DfsNamespaceRequestRecord, DfsNamespaceRequestStatus,
    DfsNamespaceResult, DfsPosixCoordinationResult, DfsPreparedMetadataEffect,
    DfsPreparedOperationRecord, DfsPreparedOperationStatus,
};
use sinomo_core::network::PeerPathVerdict;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(90);

/// Every prepared record in `node`'s copy of the shard.
fn prepared_records(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> Vec<DfsPreparedOperationRecord> {
    use crate::DfsMetadataBackend as _;
    crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(config),
        shard_root(runtime, node, config),
    )
    .and_then(|backend| backend.list_prepared_operations())
    .unwrap_or_default()
}

/// `operation`'s prepared record in `node`'s copy of the shard, whatever its
/// status.
fn prepared_record(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    operation: &str,
) -> Option<DfsPreparedOperationRecord> {
    use crate::DfsMetadataBackend as _;
    crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(config),
        shard_root(runtime, node, config),
    )
    .ok()?
    .prepared_operation_record(operation)
    .ok()
    .flatten()
}

/// The calls of the current observation window `from` made, by target,
/// route, last phase, request id and error, for a failure message.
fn calls_from(runtime: &DfsTestRuntime, from: &NodeId) -> Vec<String> {
    let mut latest = std::collections::BTreeMap::new();
    for call in runtime.rpc_observations() {
        if call.source == *from && call.route.starts_with("/dfs") {
            latest.insert(call.call_id, call);
        }
    }
    latest
        .into_values()
        .map(|call| {
            format!(
                "{} {} {:?} {:?} {:?}",
                call.target, call.route, call.phase, call.dfs_request_id, call.outcome_error
            )
        })
        .collect()
}

/// The request's row prepares in `node`'s copy, the bodies whose only
/// effect puts its request row.
fn row_prepares(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    requester: &NodeId,
    request_id: &str,
) -> Vec<DfsPreparedOperationRecord> {
    prepared_records(runtime, node, config)
        .into_iter()
        .filter(|record| {
            matches!(record.metadata_effects.as_slice(),
                [DfsPreparedMetadataEffect::PutNamespaceRequest { request }]
                    if request.requester_node_id == *requester && request.request_id == request_id)
        })
        .collect()
}

/// The request's row and receipt in `node`'s copy, as `(status, result)`
/// of whichever records its final state.
fn request_outcome(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
    requester: &NodeId,
    request_id: &str,
) -> Option<(DfsNamespaceRequestStatus, Option<DfsNamespaceResult>)> {
    use crate::DfsMetadataBackend as _;
    let mut backend = crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(config),
        shard_root(runtime, node, config),
    )
    .ok()?;
    let snapshot = backend
        .namespace_request_admission_snapshot(&shard_id_of(config), requester, request_id)
        .ok()?;
    if let Some(receipt) = snapshot.receipt {
        return Some((DfsNamespaceRequestStatus::Completed, Some(receipt.result)));
    }
    snapshot
        .request
        .map(|request| (request.status, request.result))
}

/// One mkdir from `node`'s client under `operation`, on its own thread,
/// bounded only by `budget` when one is given.
fn mkdir_from(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    path: &'static str,
    operation: &'static str,
    budget: Option<Duration>,
) -> mpsc::Receiver<Result<crate::DfsStat, crate::DfsPosixError>> {
    let data_dir = runtime.data_dir(node).clone();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client =
            crate::runtime_host::open_dfs_runtime_service(&data_dir, Some(executor().handle()))
                .expect("the requester opens its client");
        if let Some(budget) = budget {
            client.set_transport_operation_deadline(Some(Instant::now() + budget));
        }
        let _ = sender.send(client.posix_mkdir_with_operation_id(path, operation));
    });
    receiver
}

/// Leave one mkdir's admission prepared on both members: the primary
/// publishes its row prepare, the members store it and their answers are
/// held, so the requester's budget ends the call with its outcome unknown.
/// Returns the row prepare's operation id.
fn admission_left_prepared(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    requester: &NodeId,
    path: &'static str,
    operation: &'static str,
) -> (String, [std::sync::Arc<tokio::sync::Semaphore>; 2]) {
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let gates = [&b, &c].map(|member| hold_replies(runtime, config, member));
    let first = mkdir_from(
        runtime,
        requester,
        path,
        operation,
        Some(Duration::from_secs(3)),
    )
    .recv_timeout(EVENT_BOUND)
    .expect("the budget ends the first attempt");
    assert!(
        first.is_err(),
        "a mkdir its members cannot acknowledge is not answered: {first:?}"
    );
    let mut row = None;
    wait_until(
        "both members hold the request's row prepare",
        runtime,
        &[&b, &c],
        || {
            let held = [&b, &c].map(|member| {
                row_prepares(runtime, member, config, requester, operation)
                    .into_iter()
                    .find(|record| record.status == DfsPreparedOperationStatus::Prepared)
            });
            match held {
                [Some(at_b), Some(at_c)] if at_b.digest == at_c.digest => {
                    row = Some(at_b.digest.operation_id);
                    true
                }
                _ => false,
            }
        },
    );
    (row.expect("the row prepare"), gates)
}

/// A mkdir's row prepare reached both members, but the primary was lost
/// before it committed. The member that took over recovers the prepare at
/// its epoch, and that recovery's commit is held, so the requester's retry
/// arrives while the request's admission exists only as a prepare. The
/// retry is not admitted afresh: its answer waits for that admission. Once
/// the recovery commits, the request runs once, and the same operation id
/// is answered with its one result.
#[test]
fn native_retry_at_a_successor_is_admitted_by_the_row_prepare_it_holds() {
    let (mut runtime, config) = switched_with_kv_voters("native-successor-row-retry", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/admitted/made";
    let operation = "successor-row-retry";
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/admitted");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    let (row, gates) = admission_left_prepared(&runtime, &config, &c, path, operation);
    // B takes over with C cut off from it, and B's recovery of the row
    // prepare stays prepared while the commit is held.
    let recovery = crate::metadata_records::recovery_operation_id(&row, config.epoch + 1);
    let hold = crate::metadata_backend::prepared_operation::hold_prepared_commit(
        &[
            shard_root(&runtime, &b, &config),
            shard_root(&runtime, &c, &config),
        ],
        &recovery,
    );
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.stop_node(&p).unwrap();
    for gate in &gates {
        release(gate);
    }
    runtime.clear_faults();
    runtime.partition_node(
        &c,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::NoPath, false);
    lose(&runtime, &[&b], &p);
    wait_until("B takes over", &runtime, &[&b], || {
        record_from(&runtime, &config, &b).shard.primary_node_id == b
    });
    wait_until(
        "B prepares its recovery of the row",
        &runtime,
        &[&b],
        || {
            prepared_records(&runtime, &b, &config)
                .iter()
                .any(|record| {
                    record.digest.operation_id == recovery
                        && record.status == DfsPreparedOperationStatus::Prepared
                })
        },
    );
    runtime.partition_node(&c, Partition::default());
    runtime.set_peer_verdict(&b, &c, PeerPathVerdict::Path, false);
    runtime.wait_published_configuration_current(&c).unwrap();

    // The retry reaches B while the admission is only a prepare there.
    runtime.checkpoint_rpc_observation_window().unwrap();
    let retry = mkdir_from(&runtime, &c, path, operation, None);
    let started = Instant::now();
    while !runtime.rpc_observations().iter().any(|call| {
        call.source == c && call.target == b && call.dfs_request_id.as_deref() == Some(operation)
    }) {
        assert!(
            started.elapsed() < EVENT_BOUND,
            "the retry reaches B; calls {:#?}",
            calls_from(&runtime, &c)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let early = retry.recv_timeout(Duration::from_secs(3));
    assert!(
        !matches!(early, Ok(Ok(_))),
        "the retry is not admitted beside the request's prepared admission"
    );
    assert!(
        row_prepares(&runtime, &b, &config, &c, operation)
            .iter()
            .all(|record| record.digest.operation_id == row
                || record.digest.operation_id == recovery),
        "the retry wrote no second admission"
    );

    // The recovery commits; the request runs once.
    drop(hold);
    runtime
        .with_main_maintenance_service(&b, |service| -> Result<(), crate::DfsError> {
            service.recover_durable_namespace_prepares_until_cancelled(&|| false)?;
            service.recover_pending_namespace_requests_bounded(16)?;
            Ok(())
        })
        .unwrap()
        .unwrap();
    if early.is_err() {
        // Still following the record: its re-send is answered now.
        let _ = retry.recv_timeout(EVENT_BOUND);
    }
    let again = mkdir_from(&runtime, &c, path, operation, None)
        .recv_timeout(EVENT_BOUND)
        .expect("the same operation id is answered")
        .expect("the same operation id answers the mkdir's one result");
    assert_eq!(again.path, path);
    assert_eq!(
        request_outcome(&runtime, &b, &config, &c, operation),
        Some((
            DfsNamespaceRequestStatus::Completed,
            Some(DfsNamespaceResult::Applied)
        )),
        "the request completed once, as applied"
    );
    runtime.close().unwrap();
}

/// A mkdir's row prepare stayed prepared at its primary, and the requester
/// retried there before anything recovered it. The retry recovers that
/// admission and runs the request under it; nothing is left for a later
/// recovery. The primary is then lost, and the member that took over
/// recovers what it holds: the request's result survives, and the same
/// operation id is answered with it.
#[test]
fn native_successor_recovery_keeps_a_result_its_lost_primary_answered() {
    let (mut runtime, config) = switched_with_kv_voters("native-successor-keeps-result", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/kept/made";
    let operation = "successor-keeps-result";
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/kept");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    let (row, gates) = admission_left_prepared(&runtime, &config, &c, path, operation);
    // Nothing at the primary recovers the row prepare before the retry.
    let hold = crate::metadata_backend::prepared_operation::hold_prepared_commit(
        &[shard_root(&runtime, &p, &config)],
        &row,
    );
    for gate in &gates {
        release(gate);
    }
    runtime.clear_faults();
    drop(hold);
    let answered = mkdir_from(&runtime, &c, path, operation, None)
        .recv_timeout(EVENT_BOUND)
        .expect("the retry is answered")
        .expect("the retry runs the mkdir");
    assert_eq!(answered.path, path);
    // Every commit record the primary wrote reaches both members.
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/kept/after");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.stop_node(&p).unwrap();
    lose(&runtime, &[&b, &c], &p);
    wait_until("a member takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    let s = record_from(&runtime, &config, &b).shard.primary_node_id;
    runtime
        .with_main_maintenance_service(&s, |service| -> Result<(), crate::DfsError> {
            service.recover_durable_namespace_prepares_until_cancelled(&|| false)?;
            service.recover_pending_namespace_requests_bounded(16)?;
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert!(
        row_prepares(&runtime, &s, &config, &c, operation)
            .iter()
            .all(|record| record.status != DfsPreparedOperationStatus::Prepared),
        "nothing of the request is left prepared"
    );
    assert_eq!(
        request_outcome(&runtime, &s, &config, &c, operation),
        Some((
            DfsNamespaceRequestStatus::Completed,
            Some(DfsNamespaceResult::Applied)
        )),
        "the successor keeps the request's one result"
    );
    let again = mkdir_from(&runtime, &c, path, operation, None)
        .recv_timeout(EVENT_BOUND)
        .expect("the same operation id is answered")
        .expect("the same operation id answers the mkdir's one result");
    assert_eq!(again.object_id, answered.object_id);
    runtime.close().unwrap();
}

/// A request's row prepare that its completed row supersedes can never
/// commit: a request completes once. Recovery aborts that prepare, and the
/// completed row stays.
#[test]
fn native_recovery_aborts_a_row_prepare_its_completed_request_superseded() {
    let (runtime, config) = switched_with_kv_voters("native-obsolete-row-prepare", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/obsolete/made";
    let operation = "obsolete-row-prepare";
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/obsolete");
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    let (row, gates) = admission_left_prepared(&runtime, &config, &c, path, operation);
    // Nothing at the primary recovers the row prepare until this test does.
    let hold = crate::metadata_backend::prepared_operation::hold_prepared_commit(
        &[shard_root(&runtime, &p, &config)],
        &row,
    );
    for gate in &gates {
        release(gate);
    }
    runtime.clear_faults();
    // The request's completed row lands without that prepare, as a pre-fix
    // admission beside it left it on disk.
    let pending = row_prepares(&runtime, &p, &config, &c, operation)
        .into_iter()
        .find(|record| record.digest.operation_id == row)
        .and_then(|record| match record.metadata_effects.as_slice() {
            [DfsPreparedMetadataEffect::PutNamespaceRequest { request }] => {
                Some(request.as_ref().clone())
            }
            _ => None,
        })
        .expect("the row prepare's request");
    {
        let mut service = primary(&runtime);
        directory(&mut service, path);
        let completed = DfsNamespaceRequestRecord {
            status: DfsNamespaceRequestStatus::Completed,
            result: Some(DfsNamespaceResult::Applied),
            updated_at: time::OffsetDateTime::now_utc(),
            ..pending.clone()
        };
        service
            .publish_namespace_request_record(&completed)
            .unwrap();
        // The pending row can no longer replace it.
        assert!(
            service.publish_namespace_request_record(&pending).is_err(),
            "a completed request's row is final"
        );
        drop(hold);
        assert!(
            service
                .recover_durable_namespace_prepares_until_cancelled(&|| false)
                .unwrap()
        );
    }
    let statuses = [row.clone(), format!("retire-admission:{row}")]
        .into_iter()
        .filter_map(|operation| {
            prepared_record(&runtime, &p, &config, &operation)
                .map(|record| (operation, record.status))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        statuses.get(&row),
        Some(&DfsPreparedOperationStatus::Aborted),
        "{statuses:?}"
    );
    assert_eq!(
        statuses.get(&format!("retire-admission:{row}")),
        Some(&DfsPreparedOperationStatus::Committed),
        "{statuses:?}"
    );
    assert_eq!(
        request_outcome(&runtime, &p, &config, &c, operation),
        Some((
            DfsNamespaceRequestStatus::Completed,
            Some(DfsNamespaceResult::Applied)
        ))
    );
    runtime.close().unwrap();
}

/// One durable kind's first attempt, its retry and what the retry must
/// leave.
struct Kind {
    name: &'static str,
    operation: DfsNamespaceOperation,
}

/// A namespace call from a node that took over: its first attempt applied
/// at the old primary and lost its reply; the old primary, alive but cut
/// off, lost the shard; the call's same operation id then finds the record
/// naming its own node. Each kind is answered there from the request's
/// recovered rows: nothing runs a second time, and a typed result returns.
#[test]
fn native_requester_that_took_over_answers_its_own_retry_once() {
    let (runtime, config) = switched_with_kv_voters("native-requester-took-over", None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let file_options = crate::DfsWriteOptions {
        policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![p.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    {
        let mut service = primary(&runtime);
        directory(&mut service, "/services/own");
        directory(&mut service, "/services/own/rmdir-me");
        directory(&mut service, "/services/own/chmod-me");
        directory(&mut service, "/services/own/move-from/child");
        for file in [
            "/services/own/rename-from",
            "/services/own/unlink-me",
            "/services/own/link-target",
        ] {
            service
                .posix_write_file(file, file.as_bytes(), file_options.clone())
                .unwrap();
        }
    }
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    let kinds = [
        Kind {
            name: "own-rename",
            operation: DfsNamespaceOperation::Rename {
                from: "/services/own/rename-from".into(),
                to: "/services/own/rename-to".into(),
            },
        },
        Kind {
            name: "own-unlink",
            operation: DfsNamespaceOperation::Unlink {
                path: "/services/own/unlink-me".into(),
            },
        },
        Kind {
            name: "own-rmdir",
            operation: DfsNamespaceOperation::Rmdir {
                path: "/services/own/rmdir-me".into(),
            },
        },
        Kind {
            name: "own-hardlink",
            operation: DfsNamespaceOperation::Hardlink {
                existing_path: "/services/own/link-target".into(),
                link_path: "/services/own/link".into(),
            },
        },
        Kind {
            name: "own-chmod",
            operation: DfsNamespaceOperation::SetMode {
                path: "/services/own/chmod-me".into(),
                mode: 0o700,
            },
        },
        Kind {
            name: "own-move-dir",
            operation: DfsNamespaceOperation::RenameDirectoryBounded {
                from: "/services/own/move-from".into(),
                to: "/services/own/move-to".into(),
                max_steps: 64,
            },
        },
    ];
    // Each first attempt applies at the old primary; its reply is lost.
    {
        let mut requester = runtime.local_shard_copy_service(&c, &config).unwrap();
        for kind in &kinds {
            runtime.install_fault(FaultRule {
                source: Some(c.clone()),
                target: Some(p.clone()),
                route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
                call_id: None,
                origin: None,
                remaining_hits: 1,
                action: FaultAction::LoseReplyAfterDispatch,
            });
            let lost = requester.route_namespace_operation_with_request_id_and_result(
                kind.name.into(),
                p.clone(),
                kind.operation.clone(),
            );
            assert!(lost.is_err(), "{}: the reply is lost: {lost:?}", kind.name);
            runtime.clear_faults();
        }
    }
    let applied_at_p = {
        let service = primary(&runtime);
        assert!(
            service
                .lookup_sifs_path("/services/own/rename-to")
                .unwrap()
                .is_some()
        );
        service
            .lookup_sifs_path("/services/own/chmod-me")
            .unwrap()
            .expect("the chmod target")
    };
    assert_eq!(applied_at_p.mode & 0o7777, 0o700, "the chmod applied at P");

    // P, alive, is cut off; C takes over with B cut off from it.
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.partition_node(
        &p,
        Partition {
            kv: true,
            dfs: true,
        },
    );
    runtime.partition_node(
        &b,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    runtime.set_peer_verdict(&c, &b, PeerPathVerdict::NoPath, false);
    lose(&runtime, &[&c], &p);
    wait_until("C takes over", &runtime, &[&c], || {
        record_from(&runtime, &config, &c).shard.primary_node_id == c
    });
    let successor = record_from(&runtime, &config, &c).shard;
    wait_until("C recovers every prepare it holds", &runtime, &[&c], || {
        prepared_records(&runtime, &c, &config).is_empty()
            && kinds.iter().all(|kind| {
                request_outcome(&runtime, &c, &config, &c, kind.name)
                    .is_some_and(|(status, _)| status == DfsNamespaceRequestStatus::Completed)
            })
    });
    let chmod_before = {
        let service = crate::runtime_host::open_dfs_runtime_service_on_shard(
            runtime.data_dir(&c),
            Some(executor().handle()),
            &successor,
        )
        .unwrap();
        service
            .lookup_sifs_path("/services/own/chmod-me")
            .unwrap()
            .expect("the chmod target")
    };
    assert_eq!(chmod_before.version, applied_at_p.version);

    // Each call again, under its operation id, from the node that is now
    // primary: it names the old primary, as its routing did.
    let mut requester = crate::runtime_host::open_dfs_runtime_service_on_shard(
        runtime.data_dir(&c),
        Some(executor().handle()),
        &successor,
    )
    .unwrap();
    for kind in &kinds {
        let answered = requester
            .route_namespace_operation_with_request_id_and_result(
                kind.name.into(),
                p.clone(),
                kind.operation.clone(),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "{}: the retry is answered from its request: {error}",
                    kind.name
                )
            });
        if let DfsNamespaceOperation::RenameDirectoryBounded { .. } = &kind.operation {
            match answered {
                Some(DfsPosixCoordinationResult::NamespaceJob(job)) => assert_eq!(
                    job.status,
                    crate::DfsNamespaceJobStatus::Completed,
                    "{}",
                    kind.name
                ),
                other => panic!("{}: the job's receipt is returned: {other:?}", kind.name),
            }
        }
    }
    for (path, present) in [
        ("/services/own/rename-from", false),
        ("/services/own/rename-to", true),
        ("/services/own/unlink-me", false),
        ("/services/own/rmdir-me", false),
        ("/services/own/link", true),
        ("/services/own/move-from", false),
        ("/services/own/move-to/child", true),
    ] {
        assert_eq!(
            requester
                .lookup_sifs_path(path)
                .unwrap()
                .is_some_and(|object| { object.kind != crate::DfsObjectKind::Tombstone }),
            present,
            "{path}"
        );
    }
    let chmod_after = requester
        .lookup_sifs_path("/services/own/chmod-me")
        .unwrap()
        .expect("the chmod target");
    assert_eq!(
        chmod_after.version, chmod_before.version,
        "the chmod ran once"
    );
    drop(requester);
    runtime.partition_node(&b, Partition::default());
    runtime.set_peer_verdict(&c, &b, PeerPathVerdict::Path, false);
    runtime.partition_node(&p, Partition::default());
    runtime.close().unwrap();
}
