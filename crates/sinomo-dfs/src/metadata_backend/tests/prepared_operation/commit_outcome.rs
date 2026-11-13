//! Unknown commit outcomes at the requester boundary, on a shard that keeps
//! the all-member rule: a routed write, a routed conditional rename under a
//! caller-owned id, and a reply lost after the commit.
use super::*;
use crate::{
    DfsCommitOutcomeUnknownCause, DfsError, DfsNamespaceDestinationPrecondition,
    DfsNamespaceObjectPrecondition, DfsPosixError, DfsReplicationPolicy, DfsWriteOptions,
};
use sinomo_core::network::NetworkDeliveryState;
use sinomo_core::node::NodeId;

fn fail_member_journal_calls(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    member: &NodeId,
) {
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

fn write_options(config: &DfsMetadataShardConfig) -> DfsWriteOptions {
    DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: std::iter::once(config.primary_node_id.clone())
            .chain(config.replica_node_ids.iter().cloned())
            .collect(),
        wait_for_commit: true,
        foreground_replica_repair: false,
    }
}

/// Commit records for this exact body in the primary's journal.
fn commit_records(
    runtime: &DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    digest: &crate::DfsPreparedOperationDigest,
) -> usize {
    let owner = runtime.metadata_primary_service("/services").unwrap();
    entries(&owner, &DfsMetadataShardId::new(config.shard_id.clone()))
        .iter()
        .filter(|entry| {
            matches!(
                &entry.mutation,
                DfsMetadataMutation::CommitPreparedOperationV7 { digest: committed, .. }
                    if committed == digest
            )
        })
        .count()
}

fn write_record(
    runtime: &DfsTestRuntime,
    requester: &NodeId,
    object_id: &crate::DfsObjectId,
    operation: &str,
) -> crate::DfsPreparedOperationRecord {
    let intent = crate::intent::stable_write_intent_id(requester, object_id, operation);
    let owner = runtime.metadata_primary_service("/services").unwrap();
    owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&format!("write:{intent}"))
        .unwrap()
        .expect("the write's durable body")
}

fn missing_acknowledgements(error: &DfsPosixError) -> Vec<NodeId> {
    match error {
        DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
            cause: DfsCommitOutcomeUnknownCause::AcknowledgementsMissing { missing, .. },
            ..
        }) => missing.clone(),
        other => panic!("expected an unknown outcome with missing acknowledgements: {other:?}"),
    }
}

/// C5(b): a routed write whose commit cannot collect C's acknowledgement
/// reaches its requester as the typed unknown outcome, not a handler error.
/// The requester keeps its pending write, and its same-id retry commits the
/// original body once.
#[test]
fn native_routed_write_unknown_outcome_is_typed_and_its_retry_commits_once() {
    let (runtime, service, config) = native_case("native-routed-unknown-write", 3);
    drop(service);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let path = "/services/routed-unknown-write.bin";
    let operation = "routed-unknown-write";
    let mut requester = runtime.client(&b).unwrap();
    fail_member_journal_calls(&runtime, &config, &c);
    let error = requester
        .posix_write_file_with_operation_id(
            path,
            b"routed unknown",
            write_options(&config),
            operation,
        )
        .expect_err("C cannot acknowledge the prepare");
    assert_eq!(missing_acknowledgements(&error), vec![c.clone()]);
    assert!(
        requester
            .writes
            .pending_routed_writes
            .values()
            .any(|pending| matches!(
                &pending.result,
                Some(Err(DfsError::MetadataCommitOutcomeUnknown { .. }))
            )),
        "the requester keeps its pending write with the unknown outcome"
    );
    runtime.clear_faults();
    let stat = requester
        .posix_write_file_with_operation_id(
            path,
            b"routed unknown",
            write_options(&config),
            operation,
        )
        .expect("the same-id retry commits the original body");
    assert_eq!(
        requester.posix_read_file_distributed(path).unwrap(),
        b"routed unknown"
    );
    let record = write_record(&runtime, &b, &stat.object_id, operation);
    assert_eq!(record.status, crate::DfsPreparedOperationStatus::Committed);
    assert_eq!(commit_records(&runtime, &config, &record.digest), 1);
    drop(requester);
    runtime.close().unwrap();
}

/// C5(c): a routed conditional rename under a caller-owned id reaches its
/// requester as a typed unknown outcome when its admission cannot commit.
/// The request then stays `Pending` at the primary; the retry completes the
/// original rename and a further retry replays `Applied`.
#[test]
fn native_routed_conditional_rename_stays_pending_and_its_retry_replays_applied() {
    let (runtime, service, config) = native_case("native-routed-unknown-rename", 3);
    drop(service);
    let b = config.replica_node_ids[0].clone();
    let c = config.replica_node_ids[1].clone();
    let from = "/services/unknown-rename-source";
    let to = "/services/unknown-rename-target";
    let operation = "routed-unknown-rename";
    let shard = DfsMetadataShardId::new(config.shard_id.clone());
    let mut requester = runtime.client(&b).unwrap();
    let source = requester
        .posix_write_file(from, b"rename me", write_options(&config))
        .unwrap();
    let rename = |requester: &mut NativeService| {
        requester.posix_rename_if_generation(
            from,
            to,
            operation.to_string(),
            DfsNamespaceObjectPrecondition {
                object_id: source.object_id.clone(),
                version: source.version.clone(),
            },
            DfsNamespaceDestinationPrecondition::Absent,
        )
    };
    fail_member_journal_calls(&runtime, &config, &c);
    let error = rename(&mut requester).expect_err("C cannot acknowledge the prepare");
    assert_eq!(missing_acknowledgements(&error), vec![c.clone()]);
    runtime.clear_faults();
    let request_operation = {
        // The unknown body is the request's admission. Once it commits the
        // request is visible and still `Pending`: nothing executed or
        // rejected it while its outcome was unknown.
        let mut owner = runtime.metadata_primary_service("/services").unwrap();
        assert!(
            owner
                .recover_durable_namespace_prepares_until_cancelled(&|| false)
                .unwrap()
        );
        let admission = owner
            .metadata_backend_mut()
            .unwrap()
            .namespace_request_admission_snapshot(&shard, &b, operation)
            .unwrap();
        assert!(admission.receipt.is_none(), "no terminal result yet");
        let request = admission.request.expect("the admitted request");
        assert_eq!(request.status, crate::DfsNamespaceRequestStatus::Pending);
        request.operation_id
    };
    let renamed = rename(&mut requester).expect("the retry completes the original rename");
    assert_eq!(renamed.object_id, source.object_id);
    let replayed = rename(&mut requester).expect("a further retry replays the result");
    assert_eq!(replayed.object_id, source.object_id);
    assert_eq!(replayed.version, renamed.version);
    assert!(matches!(
        requester.posix_stat_distributed(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    let mut owner = runtime.metadata_primary_service("/services").unwrap();
    let completed = owner
        .metadata_backend_mut()
        .unwrap()
        .namespace_request_admission_snapshot(&shard, &b, operation)
        .unwrap();
    let request = completed.request.expect("the completed request");
    assert_eq!(request.status, crate::DfsNamespaceRequestStatus::Completed);
    assert_eq!(request.result, Some(crate::DfsNamespaceResult::Applied));
    // The rename itself committed exactly once.
    let committed = entries(&owner, &shard)
        .into_iter()
        .filter_map(|entry| match entry.mutation {
            DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } => {
                Some(digest.operation_id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let prefix = format!("namespace-request:{request_operation}:");
    assert_eq!(
        committed
            .iter()
            .filter(|operation_id| operation_id.starts_with(&prefix))
            .count(),
        1,
        "one commit of the rename body among {committed:?}"
    );
    drop(owner);
    drop(requester);
    runtime.close().unwrap();
}

/// C5(d): a write reply lost after the primary committed reaches the
/// requester as possibly applied; its same-id retry answers from the commit,
/// which the journal records exactly once.
#[test]
fn native_lost_write_reply_after_commit_is_possibly_applied_and_commits_once() {
    let (runtime, service, config) = native_case("native-lost-write-reply", 3);
    drop(service);
    let b = config.replica_node_ids[0].clone();
    let path = "/services/lost-write-reply.bin";
    let operation = "lost-write-reply";
    let mut requester = runtime.client(&b).unwrap();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(b.clone()),
        target: Some(config.primary_node_id.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::LoseReplyAfterDispatch,
    });
    let lost = requester.posix_write_file_with_operation_id(
        path,
        b"lost reply",
        write_options(&config),
        operation,
    );
    assert!(
        matches!(
            &lost,
            Err(DfsPosixError::Dfs(DfsError::NetworkDelivery {
                delivery_state: NetworkDeliveryState::PossiblyApplied,
                ..
            }))
        ),
        "a reply lost after dispatch is possibly applied: {lost:?}"
    );
    runtime.clear_faults();
    let stat = requester
        .posix_write_file_with_operation_id(path, b"lost reply", write_options(&config), operation)
        .expect("the same-id retry answers from the commit");
    let record = write_record(&runtime, &b, &stat.object_id, operation);
    assert_eq!(record.status, crate::DfsPreparedOperationStatus::Committed);
    let again = requester
        .posix_write_file_with_operation_id(path, b"lost reply", write_options(&config), operation)
        .expect("a further retry answers the same way");
    assert_eq!(again.version, stat.version);
    assert_eq!(commit_records(&runtime, &config, &record.digest), 1);
    assert_eq!(
        requester.posix_read_file_distributed(path).unwrap(),
        b"lost reply"
    );
    drop(requester);
    runtime.close().unwrap();
}
