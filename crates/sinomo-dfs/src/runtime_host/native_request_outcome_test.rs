//! Real retained native outcome observations; no handcrafted success receipt.
use super::*;

#[test]
fn authoritative_outcome_does_not_execute_unknown_data_and_matches_actual_commit() {
    let (mut native, nodes) = crate::test_runtime::DfsTestRuntime::start_cluster(
        "native-request-outcome-replay-boundary",
        1,
    )
    .unwrap();
    let context = native.component_context(&nodes[0]).unwrap();
    let cluster = context.cluster_access().unwrap();
    let runtime = native.runtime_handle();
    let key = "/test/native-request-outcome/exact-effect".to_owned();
    let request = sinomo_core::kv::KvTxnRequest::generated(sinomo_core::kv::KvTxn {
        compares: vec![sinomo_core::kv::KvCompare::Missing { key: key.clone() }],
        success: vec![sinomo_core::kv::KvOp::Put {
            key: key.clone(),
            value: b"actual native effect".to_vec(),
            lease_id: None,
        }],
        failure: Vec::new(),
    });
    let expected =
        sinomo_core::kv::kv_command_digest(&sinomo_core::kv::KvCommand::Txn(request.txn.clone()))
            .unwrap();
    let before = runtime
        .block_on(cluster.kv_request_outcome_authoritative_async(request.request_id.clone()))
        .unwrap();
    sinomo_core::kv::validate_cluster_kv_request_outcome_read(&before).unwrap();
    assert!(!matches!(
        before.outcome,
        sinomo_core::kv::KvRequestOutcome::Committed(_)
    ));
    assert!(
        runtime
            .block_on(cluster.kv_get_authoritative_async(&key))
            .unwrap()
            .value
            .is_none(),
        "authoritative outcome read never executes received transaction DATA"
    );
    let committed = runtime
        .block_on(cluster.kv_commit_compact_async(request.clone()))
        .unwrap();
    assert!(committed.txn_response.as_ref().unwrap().succeeded);
    let read = runtime
        .block_on(cluster.kv_request_outcome_authoritative_async(request.request_id.clone()))
        .unwrap();
    sinomo_core::kv::validate_cluster_kv_request_outcome_read(&read).unwrap();
    let sinomo_core::kv::KvRequestOutcome::Committed(actual) = read.outcome else {
        panic!("real committed history missing")
    };
    assert_eq!(actual.request_id, request.request_id);
    assert_eq!(actual.command_digest, expected);
    assert_eq!(
        actual.txn_response.as_ref(),
        committed.txn_response.as_ref()
    );
    assert!(actual.term > 0 && actual.log_index > 0);
    let wrong = sinomo_core::kv::kv_command_digest(&sinomo_core::kv::KvCommand::Txn(
        sinomo_core::kv::KvTxn {
            success: vec![sinomo_core::kv::KvOp::Delete { key: key.clone() }],
            ..request.txn.clone()
        },
    ))
    .unwrap();
    assert_ne!(
        actual.command_digest, wrong,
        "different request body cannot borrow observed committed digest"
    );
    let replay = runtime
        .block_on(cluster.kv_commit_compact_async(request.clone()))
        .unwrap();
    assert!(replay.replayed_committed_request);
    assert_eq!(replay.command_digest, expected);
    assert_eq!(
        runtime
            .block_on(cluster.kv_get_authoritative_async(&key))
            .unwrap()
            .value
            .unwrap()
            .value,
        b"actual native effect"
    );
    drop(cluster);
    drop(context);
    native.close().unwrap();
}
