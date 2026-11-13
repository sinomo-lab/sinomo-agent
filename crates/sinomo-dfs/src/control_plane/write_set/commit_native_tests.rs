//! The write-set commit rule on the native test runtime: real Raft-backed
//! cluster KV, real shard stores and the real acknowledgement path.

use super::*;
use crate::test_support::{DfsTestRuntime, current_native_metadata_service};
use crate::{
    DfsMetadataShardId, DfsNamespaceObjectVersionExpectation, DfsNamespacePathOccupantExpectation,
    DfsNamespaceTransaction, DfsObjectMetadata, DfsVersion, LocalSobjMetadataBackend,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

type NativeService = crate::NetworkedDfsService<crate::DfsNodeTransport>;

fn members(config: &DfsMetadataShardConfig) -> Vec<NodeId> {
    metadata_shard_configured_node_ids(config)
        .into_iter()
        .collect()
}

fn cut(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) -> DfsMetadataShardRecordCut {
    let mut client = runtime.configuration_publisher(&config.primary_node_id);
    DfsControlPlaneStore::new(client.as_mut())
        .read_metadata_shard_record_cut(&config.file_system_id, &config.shard_id, &members(config))
        .unwrap()
        .expect("the shard exists")
}

fn submit(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    change: &DfsMetadataWriteSetChangeRequest,
) {
    let mut client = runtime.configuration_publisher(&config.primary_node_id);
    assert_eq!(
        DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(change),
        DfsMetadataWriteSetTxnOutcome::Applied
    );
    for node in members(config) {
        runtime.wait_published_configuration_current(&node).unwrap();
    }
}

fn write_set_of(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
) -> DfsMetadataShardWriteSet {
    cut(runtime, config)
        .write_set_state()
        .switched()
        .cloned()
        .expect("the shard is switched")
}

/// A three-member shard on the write-set protocol, optionally under a
/// subtree rule that sets its W floor.
fn switched(name: &str, rule: Option<(usize, usize)>) -> (DfsTestRuntime, DfsMetadataShardConfig) {
    let (runtime, service, config) = current_native_metadata_service(name, 3, "/services").unwrap();
    drop(service);
    if let Some((w, n)) = rule {
        let policy =
            crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, w, n)
                .unwrap();
        runtime
            .apply_placement_policy(
                &config.primary_node_id,
                "/services",
                crate::DfsPlacementScope::Subtree,
                policy,
            )
            .unwrap();
    }
    for node in members(&config) {
        runtime
            .advertise_metadata_write_set_capability(&node)
            .unwrap();
    }
    let all = members(&config).into_iter().collect();
    let switch = build_metadata_write_set_switch(&cut(&runtime, &config), &all).unwrap();
    submit(&runtime, &config, &switch);
    (runtime, config)
}

fn shrink(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    member: &NodeId,
    w_floor: usize,
) {
    let change = build_metadata_write_set_shrink(
        &cut(runtime, config),
        member,
        &DfsMetadataShardBounds { w_floor, n: 3 },
    )
    .unwrap();
    submit(runtime, config, &change);
}

fn primary(runtime: &DfsTestRuntime) -> NativeService {
    runtime.metadata_primary_service("/services").unwrap()
}

fn directory(service: &mut NativeService, path: &str) -> DfsObjectMetadata {
    service.posix_mkdir_all(path).unwrap();
    service.lookup_sifs_path(path).unwrap().expect("directory")
}

/// One namespace change to `path`'s mode as a complete prepared body.
fn mode_change(
    config: &DfsMetadataShardConfig,
    current: &DfsObjectMetadata,
    mode: u32,
    operation: &str,
) -> crate::DfsPreparedOperationPayload {
    let mut changed = current.clone();
    changed.version = DfsVersion::generate();
    changed.mode = mode;
    crate::DfsPreparedOperationPayload::for_namespace_transaction(
        crate::DfsPreparedOperationScope {
            file_system_id: current.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
            owner_epoch: config.epoch,
            operation_id: operation.into(),
        },
        vec![DfsNamespaceObjectVersionExpectation {
            object_id: current.object_id.clone(),
            expected_version: Some(current.version.clone()),
        }],
        vec![DfsNamespacePathOccupantExpectation {
            path: current.path.clone(),
            expected_object_id: Some(current.object_id.clone()),
        }],
        DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            transaction_id: format!("transaction:{operation}"),
            file_system_id: current.file_system_id.clone(),
            objects: vec![changed],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        },
        Vec::new(),
        None,
        None,
        Vec::new(),
        None,
        None,
    )
    .unwrap()
}

fn fail_journal_calls(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig, member: &NodeId) {
    for route in [
        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    ] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(config.primary_node_id.clone()),
            target: Some(member.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    }
}

fn hold_replies(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    member: &NodeId,
) -> Arc<tokio::sync::Semaphore> {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    for (source, target, route) in [
        (
            config.primary_node_id.clone(),
            member.clone(),
            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        ),
        (
            config.primary_node_id.clone(),
            member.clone(),
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
        ),
        (
            member.clone(),
            config.primary_node_id.clone(),
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE,
        ),
    ] {
        runtime.install_fault(crate::test_runtime::FaultRule {
            source: Some(source),
            target: Some(target),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
        });
    }
    gate
}

fn release(gate: &tokio::sync::Semaphore) {
    gate.add_permits(tokio::sync::Semaphore::MAX_PERMITS / 2);
}

fn shard_root(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> std::path::PathBuf {
    crate::default_dfs_metadata_shards_root(runtime.data_dir(node))
        .join(crate::metadata_keys::key_segment(&config.shard_id))
}

fn durable_prefix(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> Option<crate::DfsMetadataPrefixProof> {
    LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(config.shard_id.clone()),
        shard_root(runtime, node, config),
    )
    .ok()?
    .metadata_prefix_proof()
    .ok()
}

/// Replicate the primary's whole journal to `member` through the ordinary
/// durability barrier; the member then holds exactly the primary's head.
fn catch_up(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    member: &NodeId,
) -> crate::DfsMetadataPrefixProof {
    let mut service = primary(runtime);
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let head = service
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard_id)
        .unwrap()
        .expect("the primary has a prefix");
    let current = cut(runtime, config).shard;
    let mut replica_node_ids = current.replica_node_ids.clone();
    replica_node_ids.sort();
    let target = crate::DfsMetadataQuorumSyncTarget {
        file_system_id: current.file_system_id.clone(),
        shard_id,
        primary_node_id: current.primary_node_id.clone(),
        authority_epoch: Some(current.epoch),
        replica_node_ids,
        expected_shard: Some(current.clone()),
        journal_sequence: head.journal_sequence,
        barrier_id: sinomo_core::Id::generate().to_string(),
        state_fingerprint: head.digest.clone(),
        checkpoint_snapshot: None,
    };
    service
        .sync_metadata_shard_to_quorum_intersecting(
            &target,
            std::slice::from_ref(member),
            Duration::from_secs(30),
        )
        .expect("the member catches up");
    drop(service);
    assert_eq!(
        durable_prefix(runtime, member, config).as_ref(),
        Some(&head),
        "{member} holds the primary's head"
    );
    head
}

fn unknown_cause(
    result: Result<crate::publication_guard::DfsPreparedPublication, crate::DfsError>,
) -> crate::DfsCommitOutcomeUnknownCause {
    match result {
        Err(crate::DfsError::MetadataCommitOutcomeUnknown { cause, .. }) => cause,
        Err(other) => panic!("expected an unknown outcome, got {other:?}"),
        Ok(publication) => panic!(
            "expected an unknown outcome, got success ({})",
            publication.published
        ),
    }
}

/// C1: at a W floor of 2 the write set {P, B} commits without the stopped
/// out-of-set member C, and the shrink to below the floor is refused.
#[test]
fn native_write_set_commit_at_the_floor() {
    let (mut runtime, config) = switched("native-write-set-commit-floor", Some((2, 3)));
    let c = config.replica_node_ids[1].clone();
    let b = config.replica_node_ids[0].clone();
    shrink(&runtime, &config, &c, 2);
    runtime.stop_node(&c).unwrap();
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/commit-at-floor");
    let committed = service
        .publish_prepared_operation(mode_change(&config, &current, 0o640, "commit-at-floor"))
        .expect("the write set at the floor commits without an out-of-set member");
    assert!(committed.published);
    let below = build_metadata_write_set_shrink(
        &cut(&runtime, &config),
        &b,
        &DfsMetadataShardBounds { w_floor: 2, n: 3 },
    );
    assert!(below.is_err(), "a shrink below the W floor is refused");
    assert_eq!(write_set_of(&runtime, &config).write_set.len(), 2);
    drop(service);
    runtime.close().unwrap();
}

/// C3: a durable pending add is required before the record shows it, across
/// a primary restart, and is released once the record moves past its cut.
#[test]
fn native_write_set_pending_add_is_required_then_released() {
    let (mut runtime, config) = switched("native-write-set-pending-add", None);
    let c = config.replica_node_ids[1].clone();
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/pending-add");
    drop(service);
    shrink(&runtime, &config, &c, 1);
    let head = catch_up(&runtime, &config, &c);
    let record = cut(&runtime, &config);
    let generation = record.write_set_state().switched().unwrap().generation;
    let evidence = crate::DfsMetadataMemberAckView {
        epoch: config.epoch,
        write_set_generation: generation,
        durable_prefix: head,
    };
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let mut service = primary(&runtime);
    service
        .metadata_backend_mut()
        .unwrap()
        .record_pending_write_set_add(&shard_id, &record, &c, &evidence)
        .expect("evidence covers the primary's whole journal");

    // IgnorePendingAdd: the record's write set is {P, B}, yet C is required.
    fail_journal_calls(&runtime, &config, &c);
    let payload = mode_change(
        &config,
        &directory(&mut service, "/services/pending-add"),
        0o640,
        "pending-add-required",
    );
    match unknown_cause(service.publish_prepared_operation(payload.clone())) {
        crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing { missing, .. } => {
            assert_eq!(missing, vec![c.clone()])
        }
        other => panic!("{other:?}"),
    }
    let _ = current;

    // VolatilePendingAdd: the requirement survives a primary restart.
    drop(service);
    runtime.stop_node(&config.primary_node_id).unwrap();
    runtime.reopen_node(config.primary_node_id.clone()).unwrap();
    let mut service = primary(&runtime);
    assert!(matches!(
        unknown_cause(service.publish_prepared_operation(payload.clone())),
        crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing { .. }
    ));

    runtime.clear_faults();
    let committed = service
        .publish_prepared_operation(payload)
        .expect("with C's acknowledgement the same body commits");
    assert!(committed.published);

    // The add applies; the next commit's fence drops the superseded add and C
    // is required through the write set itself.
    let add = build_metadata_write_set_add(&record, &c).unwrap();
    submit(&runtime, &config, &add);
    drop(service);
    let mut service = primary(&runtime);
    let next = directory(&mut service, "/services/pending-add");
    service
        .publish_prepared_operation(mode_change(&config, &next, 0o650, "pending-add-applied"))
        .unwrap();
    let state = service
        .metadata_backend()
        .unwrap()
        .write_set_member_state(&shard_id)
        .unwrap();
    assert!(
        state.is_none_or(|state| state.pending_adds.is_empty()),
        "the applied add is released"
    );
    assert!(write_set_of(&runtime, &config).contains(&c));
    drop(service);
    runtime.close().unwrap();
}

/// C3(e): an add that can never apply stops being required once the record
/// moves past its cut.
#[test]
fn native_write_set_pending_add_released_when_it_cannot_apply() {
    let (runtime, config) = switched("native-write-set-pending-release", None);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/pending-release");
    drop(service);
    shrink(&runtime, &config, &c, 1);
    let head = catch_up(&runtime, &config, &c);
    let record = cut(&runtime, &config);
    let generation = record.write_set_state().switched().unwrap().generation;
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let mut service = primary(&runtime);
    service
        .metadata_backend_mut()
        .unwrap()
        .record_pending_write_set_add(
            &shard_id,
            &record,
            &c,
            &crate::DfsMetadataMemberAckView {
                epoch: config.epoch,
                write_set_generation: generation,
                durable_prefix: head,
            },
        )
        .unwrap();
    drop(service);
    // Another change moves the record first, so the add's compare fails.
    shrink(&runtime, &config, &b, 1);
    let mut client = runtime.configuration_publisher(&config.primary_node_id);
    assert_eq!(
        DfsControlPlaneStore::new(client.as_mut())
            .submit_metadata_write_set_change(&build_metadata_write_set_add(&record, &c).unwrap()),
        DfsMetadataWriteSetTxnOutcome::Rejected
    );
    fail_journal_calls(&runtime, &config, &c);
    let mut service = primary(&runtime);
    let committed = service
        .publish_prepared_operation(mode_change(&config, &current, 0o640, "pending-release"))
        .expect("a superseded add no longer requires its member");
    assert!(committed.published);
    drop(service);
    runtime.close().unwrap();
}

/// C4(a): an add that applies between acknowledgement collection and the
/// fence is caught at the fence, which runs another round for the member.
#[test]
fn native_write_set_fence_rechecks_a_moved_record() {
    let (runtime, config) = switched("native-write-set-fence-recheck", None);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/fence-recheck");
    shrink(&runtime, &config, &c, 1);
    let gate = hold_replies(&runtime, &config, &b);
    let record = cut(&runtime, &config);
    let payload = mode_change(&config, &current, 0o640, "fence-recheck");
    let published = std::thread::scope(|scope| {
        let commit = scope.spawn(|| {
            service
                .publish_prepared_operation(payload)
                .map(|publication| publication.published)
        });
        // While B's answer is held the record gains C; then B answers.
        std::thread::sleep(Duration::from_millis(500));
        submit(
            &runtime,
            &config,
            &build_metadata_write_set_add(&record, &c).unwrap(),
        );
        release(&gate);
        commit.join().unwrap()
    })
    .expect("the fence's second round reaches the added member");
    assert!(published);
    assert!(write_set_of(&runtime, &config).contains(&c));
    runtime.clear_faults();
    drop(service);
    runtime.close().unwrap();
}

/// C4(b): a takeover that applies while the old primary collects
/// acknowledgements leaves its change prepared, uncommitted and unknown.
#[test]
fn native_write_set_takeover_during_collection_leaves_the_outcome_unknown() {
    let (runtime, config) = switched("native-write-set-fence-takeover", None);
    let b = config.replica_node_ids[0].clone();
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/fence-takeover");
    let gate = hold_replies(&runtime, &config, &b);
    let record = cut(&runtime, &config);
    let request_id = sinomo_core::kv::KvRequestId::generate();
    let payload = mode_change(&config, &current, 0o640, "fence-takeover");
    let outcome = std::thread::scope(|scope| {
        let commit = scope.spawn(|| {
            service
                .publish_prepared_operation(payload.clone())
                .map(|publication| publication.published)
        });
        std::thread::sleep(Duration::from_millis(500));
        let mut candidate = runtime.local_shard_copy_service(&b, &config).unwrap();
        let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
        let start = candidate
            .metadata_backend_mut()
            .unwrap()
            .promise_metadata_takeover(&shard_id, &record, &request_id)
            .unwrap();
        drop(candidate);
        submit(
            &runtime,
            &config,
            &build_metadata_takeover_swap(&record, &b, &start, &request_id).unwrap(),
        );
        release(&gate);
        commit.join().unwrap()
    });
    let outcome = match outcome {
        Ok(published) => panic!("a deposed primary cannot commit (published {published})"),
        Err(error) => Err(error),
    };
    assert!(matches!(
        unknown_cause(outcome),
        crate::DfsCommitOutcomeUnknownCause::RecordChanged { .. }
            | crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing { .. }
    ));
    let prepared = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_record("fence-takeover")
        .unwrap()
        .expect("the body stays durable");
    assert_eq!(prepared.status, crate::DfsPreparedOperationStatus::Prepared);
    runtime.clear_faults();
    drop(service);
    runtime.close().unwrap();
}

/// C5(a): a missing member's acknowledgement is an unknown outcome; once it
/// answers, the same operation commits exactly once.
#[test]
fn native_write_set_unknown_outcome_resolved_by_retry() {
    let (runtime, config) = switched("native-write-set-unknown-retry", None);
    let c = config.replica_node_ids[1].clone();
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/unknown-retry");
    fail_journal_calls(&runtime, &config, &c);
    let payload = mode_change(&config, &current, 0o640, "unknown-retry");
    service
        .transport
        .set_operation_deadline(Some(Instant::now() + Duration::from_secs(2)));
    let outcome = service.publish_prepared_operation(payload.clone());
    service.transport.set_operation_deadline(None);
    assert!(matches!(
        unknown_cause(outcome),
        crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing { .. }
            | crate::DfsCommitOutcomeUnknownCause::Deadline
    ));
    runtime.clear_faults();
    let committed = service.publish_prepared_operation(payload.clone()).unwrap();
    assert!(committed.published);
    let again = service.publish_prepared_operation(payload).unwrap();
    assert!(
        !again.published,
        "a committed body answers its retry without new work"
    );
    drop(service);
    runtime.close().unwrap();
}

/// C6: only the caller's deadline bounds the wait for a slow member that
/// still answers.
#[test]
fn native_write_set_slow_member_and_the_caller_deadline() {
    let (runtime, config) = switched("native-write-set-slow-member", None);
    let b = config.replica_node_ids[0].clone();
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/slow-member");
    // (b) A 2 s caller deadline ends the wait as unknown.
    let gate = hold_replies(&runtime, &config, &b);
    let payload = mode_change(&config, &current, 0o640, "slow-member");
    service
        .transport
        .set_operation_deadline(Some(Instant::now() + Duration::from_secs(2)));
    let outcome = service.publish_prepared_operation(payload.clone());
    service.transport.set_operation_deadline(None);
    assert_eq!(
        unknown_cause(outcome),
        crate::DfsCommitOutcomeUnknownCause::Deadline
    );
    // (a) Without a deadline the same body waits for B's late answers.
    let held = Duration::from_secs(7);
    let started = Instant::now();
    let committed = std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(held);
            release(&gate);
        });
        service.publish_prepared_operation(payload)
    })
    .expect("a slow member that answers never fails a commit");
    assert!(committed.published);
    assert!(started.elapsed() >= held - Duration::from_millis(500));
    runtime.clear_faults();
    drop(service);
    runtime.close().unwrap();
}

/// C7(c): one switched and one unswitched shard in one cluster. The switched
/// shard writes without its out-of-set member; the other still needs every
/// configured member.
#[test]
fn native_write_set_rule_is_per_shard() {
    let (runtime, config) = switched("native-write-set-per-shard", None);
    let c = config.replica_node_ids[1].clone();
    shrink(&runtime, &config, &c, 1);
    fail_journal_calls(&runtime, &config, &c);
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/per-shard");
    service
        .publish_prepared_operation(mode_change(&config, &current, 0o640, "per-shard-switched"))
        .expect("the switched shard does not need C");
    drop(service);
    let root = runtime.root_shard().unwrap();
    assert!(cut(&runtime, &root).write_set_state() == DfsMetadataShardWriteSetState::Unswitched);
    if root.primary_node_id == config.primary_node_id && root.replica_node_ids.contains(&c) {
        let mut root_service = runtime.metadata_primary_service("/").unwrap();
        let root_dir = root_service.lookup_sifs_path("/").unwrap().expect("root");
        let _ = root_dir;
        let outcome = root_service.posix_mkdir_all("/per-shard-unswitched");
        assert!(
            outcome.is_err(),
            "the unswitched shard still needs every member"
        );
    }
    runtime.clear_faults();
    runtime.close().unwrap();
}

/// A content write through the primary node's runtime client, the path a
/// mounted writer takes.
fn content_write(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    path: &str,
    w: usize,
    operation: &str,
) -> Result<crate::DfsStat, crate::DfsPosixError> {
    runtime
        .client(&config.primary_node_id)
        .unwrap()
        .posix_write_file_with_operation_id(
            path,
            format!("{operation} bytes").as_bytes(),
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(w, 3).unwrap(),
                replica_candidates: members(config),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
            operation,
        )
}

/// C2 and C9: a write whose own content W exceeds the live write set stays
/// prepared while W meets the shard floor; a recovery pass still commits an
/// ordinary body beside it; once the write set grows, the same operation
/// commits.
#[test]
fn native_write_set_intent_w_above_the_write_set_waits_for_it() {
    let (runtime, config) = switched("native-write-set-intent-w", None);
    let c = config.replica_node_ids[1].clone();
    shrink(&runtime, &config, &c, 1);
    let mut service = primary(&runtime);
    directory(&mut service, "/services/intent-w");
    drop(service);
    // W = 2 fits the write set {P, B}.
    content_write(
        &runtime,
        &config,
        "/services/intent-w/two.bin",
        2,
        "intent-w-two",
    )
    .expect("a write whose W the write set meets commits");
    // W = 3 does not: the change stays prepared, durable on {P, B}.
    match content_write(
        &runtime,
        &config,
        "/services/intent-w/three.bin",
        3,
        "intent-w-three",
    ) {
        Err(crate::DfsPosixError::Dfs(crate::DfsError::MetadataCommitOutcomeUnknown {
            cause: crate::DfsCommitOutcomeUnknownCause::BelowWriteSetFloor { write_set, floor },
            ..
        })) => assert_eq!((write_set, floor), (2, 3)),
        other => panic!("a write above the live write set stays pending: {other:?}"),
    }
    let mut service = primary(&runtime);
    assert!(
        service
            .lookup_sifs_path("/services/intent-w/three.bin")
            .unwrap()
            .is_none(),
        "the pending write is not visible"
    );
    // C9: recovery attempts every body; the ordinary one commits beside the
    // pending one, and the pass reports itself incomplete. The ordinary body
    // changes a directory the pending write does not touch.
    let current = directory(&mut service, "/services/intent-w-ordinary");
    let ordinary = mode_change(&config, &current, 0o640, "intent-w-ordinary");
    service
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(ordinary, None)
        .unwrap();
    assert!(
        !service
            .recover_durable_namespace_prepares_until_cancelled(&|| false)
            .unwrap()
    );
    let backend = service.metadata_backend().unwrap();
    assert_eq!(
        backend
            .prepared_operation_record("intent-w-ordinary")
            .unwrap()
            .unwrap()
            .status,
        crate::DfsPreparedOperationStatus::Committed
    );
    drop(service);

    // The write set grows to three; the same operation now commits.
    let head = catch_up(&runtime, &config, &c);
    let record = cut(&runtime, &config);
    let generation = record.write_set_state().switched().unwrap().generation;
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let mut service = primary(&runtime);
    service
        .metadata_backend_mut()
        .unwrap()
        .record_pending_write_set_add(
            &shard_id,
            &record,
            &c,
            &crate::DfsMetadataMemberAckView {
                epoch: config.epoch,
                write_set_generation: generation,
                durable_prefix: head,
            },
        )
        .unwrap();
    drop(service);
    submit(
        &runtime,
        &config,
        &build_metadata_write_set_add(&record, &c).unwrap(),
    );
    // The retained body commits on recovery; a retry of the same operation
    // then answers from it instead of writing a second change.
    let mut service = primary(&runtime);
    assert!(
        service
            .recover_durable_namespace_prepares_until_cancelled(&|| false)
            .unwrap()
    );
    assert!(
        service
            .lookup_sifs_path("/services/intent-w/three.bin")
            .unwrap()
            .is_some()
    );
    drop(service);
    let retried = content_write(
        &runtime,
        &config,
        "/services/intent-w/three.bin",
        3,
        "intent-w-three",
    );
    assert!(
        matches!(
            &retried,
            Ok(_) | Err(crate::DfsPosixError::PendingWrite { .. })
        ),
        "the same operation answers from its committed body: {retried:?}"
    );
    let service = primary(&runtime);
    drop(service);
    runtime.close().unwrap();
}

/// C7(a): a shard with no write-set record commits under a `FullShard`
/// certificate whose acknowledgements are exactly its configured members.
#[test]
fn native_unswitched_shard_commits_under_a_full_shard_certificate() {
    let (runtime, service, config) =
        current_native_metadata_service("native-unswitched-certificate", 3, "/services").unwrap();
    drop(service);
    assert!(cut(&runtime, &config).write_set_state() == DfsMetadataShardWriteSetState::Unswitched);
    let trace = crate::native_shard_trace::capture_native_shard_trace(
        &runtime.data_dir(&config.primary_node_id).root(),
        DfsMetadataShardId::new(config.shard_id.clone()),
    )
    .unwrap();
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/unswitched-certificate");
    let committed = service
        .publish_prepared_operation(mode_change(
            &config,
            &current,
            0o640,
            "unswitched-certificate",
        ))
        .unwrap();
    assert!(committed.published);
    drop(service);
    let mut bytes = Vec::new();
    trace.export_raw(&mut bytes).unwrap();
    trace.close();
    let facts: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let admitted = facts["guard_events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| {
            event["phase"] == "admitted"
                && event["digest"]["operation_id"] == "unswitched-certificate"
        })
        .collect::<Vec<_>>();
    assert_eq!(admitted.len(), 1, "one guard minted for the commit");
    let authority = &admitted[0]["facts"]["authority"];
    assert_eq!(authority["mode"], "full-shard", "{authority}");
    let acknowledged = authority["authenticated_acknowledged_node_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| node.as_str().unwrap().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    let configured = members(&config)
        .iter()
        .map(ToString::to_string)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(acknowledged, configured);
    drop(trace);
    runtime.close().unwrap();
}

/// Every file of `node`'s copy of the shard, with its length and
/// modification time; the member-state file is one of them.
fn shard_files(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    config: &DfsMetadataShardConfig,
) -> std::collections::BTreeMap<std::path::PathBuf, (u64, std::time::SystemTime)> {
    let root = shard_root(runtime, node, config);
    let mut pending = vec![root.clone()];
    let mut files = std::collections::BTreeMap::new();
    while let Some(directory) = pending.pop() {
        let Ok(listing) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in listing.flatten() {
            let path = entry.path();
            let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if metadata.is_dir() {
                pending.push(path);
            } else {
                files.insert(
                    path.strip_prefix(&root).unwrap().to_owned(),
                    (metadata.len(), metadata.modified().unwrap()),
                );
            }
            assert!(files.len() < 4096, "bounded shard inventory");
        }
    }
    files
}

/// C8: once an attempt has ended with its body prepared and its outcome
/// unknown, an idle cluster does nothing. No member rewrites its copy of the
/// shard or its member-state file, nothing is re-sent, and the body stays
/// prepared until a retry commits it.
#[test]
fn native_write_set_unknown_prepared_body_leaves_an_idle_cluster_quiet() {
    let (runtime, config) = switched("native-write-set-quiet", None);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    shrink(&runtime, &config, &c, 1);
    let mut service = primary(&runtime);
    let current = directory(&mut service, "/services/quiet");
    let gate = hold_replies(&runtime, &config, &b);
    let payload = mode_change(&config, &current, 0o640, "quiet-unknown");
    service
        .transport
        .set_operation_deadline(Some(Instant::now() + Duration::from_secs(2)));
    let outcome = service.publish_prepared_operation(payload.clone());
    service.transport.set_operation_deadline(None);
    assert_eq!(
        unknown_cause(outcome),
        crate::DfsCommitOutcomeUnknownCause::Deadline
    );
    drop(service);
    // The held answers now arrive after their attempt ended; let them land.
    release(&gate);
    runtime.clear_faults();
    std::thread::sleep(Duration::from_secs(2));
    let nodes = members(&config);
    let before = nodes
        .iter()
        .map(|node| shard_files(&runtime, node, &config))
        .collect::<Vec<_>>();
    let observed = runtime.rpc_observations().len();
    std::thread::sleep(Duration::from_secs(3));
    let after = nodes
        .iter()
        .map(|node| shard_files(&runtime, node, &config))
        .collect::<Vec<_>>();
    assert_eq!(runtime.observation_dropped_count(), 0);
    let calls = runtime
        .rpc_observations()
        .get(observed..)
        .unwrap_or_default()
        .iter()
        .filter(|call| call.route.starts_with("/dfs/"))
        .map(|call| {
            format!(
                "{} -> {} {} {:?}",
                call.source, call.target, call.route, call.phase
            )
        })
        .collect::<Vec<_>>();
    assert!(calls.is_empty(), "an idle cluster sends nothing: {calls:?}");
    for (node, (before, after)) in nodes.iter().zip(before.iter().zip(&after)) {
        assert_eq!(
            before, after,
            "{node} wrote its copy of the shard while idle"
        );
    }
    let mut service = primary(&runtime);
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .prepared_operation_record("quiet-unknown")
            .unwrap()
            .expect("the durable body")
            .status,
        crate::DfsPreparedOperationStatus::Prepared
    );
    assert!(
        service
            .publish_prepared_operation(payload)
            .expect("a retry commits the body")
            .published
    );
    drop(service);
    runtime.close().unwrap();
}
