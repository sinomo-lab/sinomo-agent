//! Real retained native outcome observations; no handcrafted success receipt.

// This proof test needs an admitted authoritative observation, not a claim
// about a concurrent local Raft lock. Retry only the explicit fail-closed busy
// result under the SAME original daemon read deadline. Never execute or replay
// transaction DATA to obtain an outcome, and never retry other authority errors.
pub(crate) async fn admitted_native_request_outcome(
    cluster: &dyn sinomo_core::service::ClusterAccess,
    request_id: &sinomo_core::kv::KvRequestId,
) -> Result<sinomo_core::kv::ClusterKvRequestOutcomeRead, sinomo_core::service::ServiceError> {
    admitted_native_request_outcome_observing(cluster, request_id, || {}).await
}

/// Observe only a real native busy rejection; this supplies no authority or
/// receipt and changes neither the original deadline nor production admission.
pub(crate) async fn admitted_native_request_outcome_observing(
    cluster: &dyn sinomo_core::service::ClusterAccess,
    request_id: &sinomo_core::kv::KvRequestId,
    mut on_busy: impl FnMut(),
) -> Result<sinomo_core::kv::ClusterKvRequestOutcomeRead, sinomo_core::service::ServiceError> {
    let budget = sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
        .kv_txn_quorum_options(true)
        .raft_append_timeout;
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let outcome = tokio::time::timeout_at(
            deadline,
            cluster.kv_request_outcome_authoritative_async(request_id.clone()),
        )
        .await
        .map_err(|_| {
            sinomo_core::service::ServiceError::Service(
                "native request-outcome observation exhausted its original read deadline".into(),
            )
        })?;
        match outcome {
            Err(sinomo_core::service::ServiceError::Network(
                sinomo_core::network::NetworkError::ClusterReadOnly { ref detail },
            )) if detail
                == "local Raft admission is busy and no authority accepted the forwarded request-outcome read; local election was skipped" =>
            {
                on_busy();
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
}

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
        .block_on(admitted_native_request_outcome(
            cluster.as_ref(),
            &request.request_id,
        ))
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
        .block_on(admitted_native_request_outcome(
            cluster.as_ref(),
            &request.request_id,
        ))
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
