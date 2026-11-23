//! A cross-shard adoption's outcome across a takeover of its target shard,
//! on the native test runtime: every node is a cluster-KV voter, so stopping
//! the target shard's primary leaves a voter quorum that elects and commits.
//!
//! A target that took over holds no witness of the source's claim, so its
//! answer comes from the shard's rows: the outcome of the adoption's whole
//! prepare chain, the original prepare and every `recover:` replacement.

use super::commit_native_tests::{
    catch_up, directory, hold_replies, primary, release, shard_root, switch_services,
};
use super::takeover_driver_native_tests::{keep_with_paths, lose};
use super::takeover_loss_native_tests::{record_from, shard_events, wait_until};
use super::takeover_native_tests::{prepare_unknown, shard_id_of};
use super::*;
use crate::DfsMetadataTakeoverEvent;
use crate::source_adoption::{DfsSourceAdoptionOutcome as Outcome, actor_for_root};
use crate::test_runtime::executor;
use crate::test_support::DfsTestRuntime;
use crate::{
    DfsPreparedMetadataEffect, DfsPreparedOperationDigest, DfsPreparedOperationPayload,
    DfsPreparedOperationRecord, DfsPreparedOperationScope, DfsPreparedOperationStatus,
};
use std::time::{Duration, Instant};

/// `operation`'s prepared record in `node`'s copy of the shard.
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

/// Every prepared operation's id and status in `node`'s copy of the shard.
fn prepared_operations(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> Vec<(String, DfsPreparedOperationStatus)> {
    use crate::DfsMetadataBackend as _;
    crate::LocalSobjMetadataBackend::open_for_inspection(
        shard_id_of(config),
        shard_root(runtime, node, config),
    )
    .and_then(|backend| backend.list_prepared_operations())
    .map(|records| {
        records
            .into_iter()
            .map(|record| (record.digest.operation_id.clone(), record.status))
            .collect()
    })
    .unwrap_or_default()
}

/// The first outcome `read` gives once the answering node's inspection cut
/// is certified: a busy writer or a cut still being published reads as
/// unavailable, which settles nothing.
fn settled_outcome(
    what: &str,
    runtime: &DfsTestRuntime,
    nodes: &[&NodeId],
    mut read: impl FnMut() -> Result<Option<Outcome>, String>,
) -> Outcome {
    let mut settled = None;
    wait_until(what, runtime, nodes, || match read() {
        Ok(Some(Outcome::Unavailable)) | Ok(None) | Err(_) => false,
        Ok(Some(outcome)) => {
            settled = Some(outcome);
            true
        }
    });
    settled.expect("an outcome settled")
}

/// Abort the prepare `digest` on `shard` through `node`'s own service at the
/// record's current epoch, with one body retried while the write set cannot
/// acknowledge yet. The shard has no producer that aborts an adoption's
/// prepare; this is the guarded kernel's own abort of a prepared body.
fn abort_prepare(
    runtime: &DfsTestRuntime,
    shard: &DfsMetadataShardConfig,
    node: &NodeId,
    digest: &DfsPreparedOperationDigest,
    operation: &str,
) {
    let payload = DfsPreparedOperationPayload::for_metadata_effects(
        DfsPreparedOperationScope {
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard_id_of(shard),
            owner_epoch: shard.epoch,
            operation_id: operation.into(),
        },
        Vec::new(),
        Vec::new(),
        vec![DfsPreparedMetadataEffect::ResolvePreparedOperation {
            digest: digest.clone(),
            status: DfsPreparedOperationStatus::Aborted,
        }],
        time::OffsetDateTime::now_utc(),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let attempt = (|| -> Result<(), String> {
            let mut service = crate::runtime_host::open_dfs_runtime_service_on_shard(
                runtime.data_dir(node),
                Some(executor().handle()),
                shard,
            )
            .map_err(|error| error.to_string())?;
            service
                .transport
                .set_operation_deadline(Some(Instant::now() + Duration::from_secs(5)));
            let published = service.publish_prepared_operation(payload.clone());
            service.transport.set_operation_deadline(None);
            published.map(drop).map_err(|error| format!("{error:?}"))
        })();
        match attempt {
            Ok(()) => return,
            Err(error) => assert!(
                Instant::now() < deadline,
                "{operation} never commits: {error}"
            ),
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// A prepared change X to a services directory, durable on B and C while P
/// never hears their acknowledgements, and then P stopped and the surviving
/// members taking over. Returns the runtime, the shard as configured before
/// the takeover, X's digest and the shard record after it.
fn prepared_across_a_takeover(
    name: &str,
    operation: &str,
    before_stop: impl FnOnce(&DfsTestRuntime, &DfsMetadataShardConfig),
) -> (
    DfsTestRuntime,
    DfsMetadataShardConfig,
    DfsPreparedOperationDigest,
    DfsMetadataShardConfig,
) {
    let (mut runtime, config) = super::commit_native_tests::switched_with_kv_voters(name, None);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let current = {
        let mut service = primary(&runtime);
        directory(&mut service, &format!("/services/{operation}"))
    };
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    let gates = [&b, &c].map(|member| hold_replies(&runtime, &config, member));
    prepare_unknown(&runtime, &config, &current, operation);
    wait_until("both members hold X", &runtime, &[&b, &c], || {
        [&b, &c]
            .into_iter()
            .all(|member| prepared_record(&runtime, member, &config, operation).is_some())
    });
    let x = prepared_record(&runtime, &b, &config, operation)
        .unwrap()
        .digest;
    before_stop(&runtime, &config);
    keep_with_paths(&runtime, &config, &[&b, &c]);
    runtime.stop_node(&p).unwrap();
    for gate in &gates {
        release(gate);
    }
    lose(&runtime, &[&b, &c], &p);
    wait_until("a member takes over", &runtime, &[&b, &c], || {
        record_from(&runtime, &config, &b).shard.primary_node_id != p
    });
    let successor = record_from(&runtime, &config, &b).shard;
    assert_eq!(successor.epoch, config.epoch + 1);
    (runtime, config, x, successor)
}

/// X's outcome as the successor's rows give it, with no witness.
fn successor_outcome(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    successor: &NodeId,
    x: &DfsPreparedOperationDigest,
) -> Result<Option<Outcome>, String> {
    let scope = DfsPreparedOperationScope {
        file_system_id: x.file_system_id.clone(),
        shard_id: x.shard_id.clone(),
        owner_epoch: config.epoch,
        operation_id: x.operation_id.clone(),
    };
    actor_for_root(&runtime.data_dir(successor).root())
        .and_then(|actor| {
            actor.retained_target_outcome_for_test(&scope, &x.payload_digest, Some(x))
        })
        .map(Some)
        .map_err(|error| error.to_string())
}

/// The target shard's primary stops after the target prepared an adoption
/// and before it commits: the prepare is durable on both other members, and
/// a member takes over. The successor holds no witness of the source's
/// claim, so it answers from its rows. The prepare can still commit, so the
/// adoption is active there: the source keeps its custody and the source
/// family stays fenced, through reconciliation as well. Once the chain is
/// aborted, the outcome follows it: the source releases its custody and the
/// fence ends with it.
#[test]
fn native_adoption_prepared_at_a_lost_target_stays_active_at_its_successor() {
    let name = "native-adopt-target-loss";
    let (runtime, _service, config) =
        crate::test_support::current_native_metadata_service_with_kv_voters(name, 3, "/services")
            .unwrap();
    drop(_service);
    let p = config.primary_node_id.clone();
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    // The move's coordinator and its source shard live on B, the target
    // shard on P, so the target's loss takes neither with it.
    for shard_id in ["root", "users"] {
        let shard = runtime
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|shard| shard.shard_id == shard_id)
            .unwrap();
        let replicas = std::iter::once(shard.primary_node_id.clone())
            .chain(shard.replica_node_ids.iter().cloned())
            .filter(|node| *node != b)
            .collect();
        runtime
            .reassign_metadata_shard(&shard.file_system_id, shard_id, b.clone(), replicas)
            .unwrap();
    }
    let (mut runtime, config) = switch_services(runtime, config, None);
    assert_eq!(config.primary_node_id, p);
    let from = "/users/adopt-target-loss.txt";
    let to = "/services/adopt-target-loss.txt";
    let moved = runtime
        .client(&b)
        .unwrap()
        .posix_write_file(
            from,
            b"bytes a lost target prepared",
            crate::DfsWriteOptions::replicated_eventual(vec![b.clone()]),
        )
        .unwrap();
    for member in [&b, &c] {
        catch_up(&runtime, &config, member);
    }
    let source = actor_for_root(&runtime.data_dir(&b).root()).unwrap();
    // The target holds its claim until the test holds every acknowledgement
    // of what it prepares next: its request row commits first, unheld.
    let (claimed, claim_seen) = std::sync::mpsc::channel();
    let (proceed, proceed_seen) = std::sync::mpsc::channel::<()>();
    crate::source_adoption::observe_next_claim(&runtime.data_dir(&p).root(), move |_, offer| {
        claimed.send(offer.clone()).unwrap();
        // Ends on the go-ahead, or when the test drops it after a failure.
        let _ = proceed_seen.recv();
    });
    let mut requester = runtime.client(&b).unwrap();
    // A bound for the test only: the move itself ends when its target stops.
    requester.set_transport_operation_deadline(Some(Instant::now() + Duration::from_secs(90)));
    let (offer, claim, moved_result) = std::thread::scope(|scope| {
        let rename = scope.spawn(move || requester.posix_rename(from, to));
        let offer = claim_seen
            .recv_timeout(Duration::from_secs(60))
            .expect("the target claims the source");
        assert_eq!(offer.source_node_id, b);
        assert_eq!(offer.target_config, config);
        // The target's adoption prepare reaches B and C; P hears neither
        // acknowledgement, so it never commits.
        let gates = [&b, &c].map(|member| hold_replies(&runtime, &config, member));
        proceed.send(()).unwrap();
        drop(proceed);
        let deadline = Instant::now() + Duration::from_secs(60);
        let claim = loop {
            let claim = source
                .held_source_custody(&offer)
                .and_then(|held| held.claim)
                .filter(|(_, _, bound)| bound.is_some());
            if let Some(claim) = claim.filter(|(scope, _, bound)| {
                [&b, &c].into_iter().all(|member| {
                    prepared_record(&runtime, member, &config, &scope.operation_id)
                        .is_some_and(|record| Some(&record.digest) == bound.as_ref())
                })
            }) {
                break claim;
            }
            assert!(
                Instant::now() < deadline,
                "both members never hold the target's adoption prepare: custody {:?}; prepares {:?}",
                source.held_source_custody(&offer),
                [&p, &b, &c].map(|node| prepared_operations(&runtime, node, &config)),
            );
            std::thread::sleep(Duration::from_millis(100));
        };
        keep_with_paths(&runtime, &config, &[&b, &c]);
        runtime.stop_node(&p).unwrap();
        for gate in &gates {
            release(gate);
        }
        lose(&runtime, &[&b, &c], &p);
        wait_until("a member takes over", &runtime, &[&b, &c], || {
            record_from(&runtime, &config, &b).shard.primary_node_id != p
        });
        (offer, claim, rename.join().unwrap())
    });
    assert!(
        moved_result.is_err(),
        "the move cannot finish without its target: {moved_result:?}"
    );
    let (scope, hash, bound) = claim;
    let bound = bound.unwrap();
    let successor = record_from(&runtime, &config, &b).shard;
    let s = successor.primary_node_id.clone();
    assert_eq!(successor.epoch, config.epoch + 1);
    wait_until("the successor activates", &runtime, &[&b, &c], || {
        !shard_events(&runtime, &s, &config, DfsMetadataTakeoverEvent::ACTIVATED).is_empty()
    });

    let outcome = settled_outcome(
        "the successor answers the source",
        &runtime,
        &[&b, &c],
        || {
            source
                .query_target_outcome(&s, &offer, &scope, &hash, Some(&bound))
                .map_err(|error| error.to_string())
        },
    );
    // Once the successor answers, reconciliation, like every other wake of
    // it, keeps the custody.
    assert_eq!(source.reconcile_pending_once().unwrap(), 0);
    let held = source
        .held_source_custody(&offer)
        .expect("the source keeps its custody");
    assert_eq!(
        held.claim.as_ref().map(|(scope, _, bound)| (scope, bound)),
        Some((&scope, &Some(bound.clone())))
    );
    assert_eq!(held.object_id, moved.object_id);
    assert!(held.fenced(), "the source family stays fenced");
    assert!(
        matches!(outcome, Outcome::Active),
        "a prepare that can still commit is active: {outcome:?}"
    );
    assert_eq!(
        prepared_record(&runtime, &s, &config, &scope.operation_id).map(|record| record.status),
        Some(DfsPreparedOperationStatus::Prepared)
    );

    // The chain's only prepare aborts: the adoption has no effect, and the
    // source learns it at its next reconciliation.
    abort_prepare(
        &runtime,
        &successor,
        &s,
        &bound,
        "abort-adoption-target-loss",
    );
    let outcome = settled_outcome(
        "the successor answers the aborted adoption",
        &runtime,
        &[&b, &c],
        || {
            source
                .query_target_outcome(&s, &offer, &scope, &hash, Some(&bound))
                .map_err(|error| error.to_string())
        },
    );
    assert!(
        matches!(&outcome, Outcome::Inactive { digest: Some(digest), status: Some(DfsPreparedOperationStatus::Aborted) } if *digest == bound),
        "{outcome:?}"
    );
    wait_until(
        "the source releases its custody",
        &runtime,
        &[&b, &c],
        || source.reconcile_pending_once().is_ok() && source.held_source_custody(&offer).is_none(),
    );
    assert!(!held.fenced(), "the fence ends with the custody");
    drop(source);
    runtime.close().unwrap();
}

/// After a takeover the successor recovers a prepared change under its
/// `recover:` id, and that replacement commits. The original's outcome is
/// the chain's: committed, with the replacement's own commit, which the
/// source accepts as the commit of the prepare it bound.
#[test]
fn native_recovery_replacement_commit_is_the_adoption_outcome() {
    let (runtime, config, x, successor) =
        prepared_across_a_takeover("native-adopt-chain-commit", "chain-commit-x", |_, _| {});
    let s = successor.primary_node_id.clone();
    let members = [&config.replica_node_ids[0], &config.replica_node_ids[1]];
    let recovery = crate::metadata_records::recovery_operation_id(&x.operation_id, successor.epoch);
    wait_until(
        "the successor commits the recovery",
        &runtime,
        &members,
        || {
            prepared_record(&runtime, &s, &config, &recovery).map(|record| record.status)
                == Some(DfsPreparedOperationStatus::Committed)
        },
    );
    assert_eq!(
        prepared_record(&runtime, &s, &config, &x.operation_id).map(|record| record.status),
        Some(DfsPreparedOperationStatus::Superseded)
    );
    let outcome = settled_outcome("the successor answers", &runtime, &members, || {
        successor_outcome(&runtime, &config, &s, &x)
    });
    let Outcome::Committed { digest, commit } = outcome else {
        panic!("a committed replacement commits the chain: {outcome:?}");
    };
    assert_eq!(digest.operation_id, recovery);
    commit.validate(&digest).unwrap();
    assert!(crate::source_adoption::commits_adoption_for_test(
        &x, &digest
    ));
    runtime.close().unwrap();
}

/// The successor's replacement of a prepared change stays prepared: its
/// commit is held. The chain is active while either prepare is: after the
/// original alone aborts, its replacement keeps it active, and only once
/// that replacement aborts too is the outcome inactive.
#[test]
fn native_adoption_chain_is_inactive_only_once_every_prepare_aborted() {
    let operation = "chain-abort-x";
    let mut hold = None;
    let (runtime, config, x, successor) =
        prepared_across_a_takeover("native-adopt-chain-abort", operation, |runtime, config| {
            let roots = config
                .replica_node_ids
                .iter()
                .map(|member| shard_root(runtime, member, config))
                .collect::<Vec<_>>();
            hold = Some(
                crate::metadata_backend::prepared_operation::hold_prepared_commit(
                    &roots,
                    &crate::metadata_records::recovery_operation_id(operation, config.epoch + 1),
                ),
            );
        });
    let s = successor.primary_node_id.clone();
    let members = [&config.replica_node_ids[0], &config.replica_node_ids[1]];
    let recovery = crate::metadata_records::recovery_operation_id(&x.operation_id, successor.epoch);
    let mut replacement = None;
    wait_until(
        "the successor prepares the recovery",
        &runtime,
        &members,
        || {
            replacement = prepared_record(&runtime, &s, &config, &recovery);
            replacement.is_some()
        },
    );
    let replacement = replacement.unwrap();
    assert_eq!(replacement.status, DfsPreparedOperationStatus::Prepared);
    let active = |what: &str| {
        let outcome = settled_outcome(what, &runtime, &members, || {
            successor_outcome(&runtime, &config, &s, &x)
        });
        assert!(matches!(outcome, Outcome::Active), "{what}: {outcome:?}");
    };
    active("both prepares are prepared");

    abort_prepare(&runtime, &successor, &s, &x, "abort-chain-original");
    assert_eq!(
        prepared_record(&runtime, &s, &config, &x.operation_id).map(|record| record.status),
        Some(DfsPreparedOperationStatus::Aborted)
    );
    active("the replacement is still prepared");

    abort_prepare(
        &runtime,
        &successor,
        &s,
        &replacement.digest,
        "abort-chain-replacement",
    );
    let outcome = settled_outcome("every prepare aborted", &runtime, &members, || {
        successor_outcome(&runtime, &config, &s, &x)
    });
    assert!(
        matches!(&outcome, Outcome::Inactive { digest: Some(digest), status: Some(DfsPreparedOperationStatus::Aborted) } if *digest == x),
        "{outcome:?}"
    );
    drop(hold);
    runtime.close().unwrap();
}
