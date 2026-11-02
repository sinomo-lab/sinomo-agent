//! Prospective child of posix::directory_identity_native_tests.
//! It reuses that module's genuine native fixture and observation helpers.
use super::*;

struct OwnedNegative {
    runtime: Rc<RefCell<DfsTestRuntime>>,
    source: NodeId,
    target: NodeId,
}
impl Drop for OwnedNegative {
    fn drop(&mut self) {
        self.runtime.borrow().remove_faults_matching(|rule| {
            rule.source.as_ref() == Some(&self.source)
                && rule.target.as_ref() == Some(&self.target)
                && rule.route.is_none()
                && rule.call_id.is_none()
                && rule.origin == Some(RpcOrigin::RegisteredHost)
                && matches!(rule.action, FaultAction::BudgetExhausted)
        });
    }
}

fn requests_for_path(
    owner: &NetworkedDfsService<crate::DfsNodeTransport>,
    shard: &DfsMetadataShardConfig,
    path: &str,
) -> Vec<crate::DfsNamespaceRequestRecord> {
    owner
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&crate::DfsMetadataShardId::new(&shard.shard_id))
        .unwrap()
        .unwrap()
        .tables
        .namespace_requests
        .into_iter()
        .filter(|request| {
            matches!(&request.operation,
            crate::DfsNamespaceOperation::MkdirWithAttrs { path: actual, .. }
                if crate::namespace::metadata_path(actual) == crate::namespace::metadata_path(path))
        })
        .collect()
}

#[test]
fn prepared_namespace_original_keeps_its_body_across_retention_policy_change() {
    let (runtime, nodes, shard) = fixture("native-retention-prepared-namespace");
    let mut owner = runtime.metadata_primary_service(BASE).unwrap();
    owner.posix_mkdir_all(BASE).unwrap();
    let path = format!("{BASE}/rule-change-original");
    let attrs = DfsPosixCreateAttrs::new(0o710, 811, 812);
    let operation_id = "retention-prepared-namespace-original";
    let missing = shard.replica_node_ids.first().unwrap().clone();
    assert_ne!(missing, shard.primary_node_id);
    let native_definition_before = owner
        .metadata_backend()
        .unwrap()
        .get_file_system(&sifs_file_system_id())
        .unwrap()
        .unwrap();
    let runtime = Rc::new(RefCell::new(runtime));
    let owned_negative = OwnedNegative {
        runtime: Rc::clone(&runtime),
        source: shard.primary_node_id.clone(),
        target: missing.clone(),
    };
    let in_hook = Rc::clone(&runtime);
    let source = shard.primary_node_id.clone();
    let reached = Arc::new(AtomicBool::new(false));
    let reached_hook = Arc::clone(&reached);
    let hook = crate::service_metadata::install_namespace_prepare_after_expectations_test_hook(
        move |_| {
            reached_hook.store(true, Ordering::Release);
            in_hook.borrow().install_fault(FaultRule {
                source: Some(source),
                target: Some(missing),
                route: None,
                call_id: None,
                origin: Some(RpcOrigin::RegisteredHost),
                remaining_hits: 32,
                action: FaultAction::BudgetExhausted,
            });
        },
    );
    let previous = owner.set_transport_operation_deadline(Some(
        std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    ));
    let first = owner.posix_mkdir_with_create_attrs_and_operation_id(&path, attrs, operation_id);
    drop(hook);
    owner.set_transport_operation_deadline(previous);
    let originals: Vec<_> = all_preparations(&owner, &shard)
        .into_iter()
        .filter(|record| {
            record
                .transaction
                .objects
                .iter()
                .any(|object| object.path == path)
        })
        .collect();
    checkpoint(
        &runtime.borrow(),
        &shard.primary_node_id,
        "retention-prepared-before-policy",
        serde_json::json!({"first":format!("{first:?}"),"hook":reached.load(Ordering::Acquire),
            "records":originals,"native_definition":native_definition_before}),
    );
    assert!(reached.load(Ordering::Acquire));
    assert!(first.is_err());
    assert_eq!(originals.len(), 1);
    let original = originals.into_iter().next().unwrap();
    original.validate().unwrap();
    assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
    let original_request = owner
        .metadata_backend()
        .unwrap()
        .get_namespace_request(&shard.primary_node_id, operation_id)
        .unwrap()
        .unwrap();
    let request_path_cut = requests_for_path(&owner, &shard, &path);
    checkpoint(
        &runtime.borrow(),
        &shard.primary_node_id,
        "retention-original-native-identity",
        serde_json::json!({"actual_request":original_request,"actual_path_requests":request_path_cut,
            "actual_original":original,"caller_request_id":operation_id}),
    );
    assert_eq!(original_request.request_id, operation_id);
    assert_eq!(original_request.requester_node_id, shard.primary_node_id);
    assert_eq!(
        original_request.status,
        crate::DfsNamespaceRequestStatus::Pending
    );
    assert!(matches!(&original_request.operation,
        crate::DfsNamespaceOperation::MkdirWithAttrs { path: actual, attrs: actual_attrs }
            if actual == &path && *actual_attrs == attrs));
    assert_eq!(request_path_cut.len(), 1);
    assert!(request_path_cut[0].matches_immutable_payload(&original_request));
    assert!(original.digest.operation_id.starts_with(&format!(
        "namespace-request:{}:",
        original_request.operation_id
    )));
    let completed_identity_matches = original
        .completed_request
        .as_ref()
        .is_some_and(|request| request.matches_immutable_payload(&original_request));
    assert!(
        completed_identity_matches,
        "the genuine mkdir prepare must carry its actual admitted command identity"
    );

    // Publish the actual namespaced filesystem setting through its normal CAS.
    // This does not publish or patch any native metadata rows.
    let mut publisher = runtime.borrow().configuration_publisher(&nodes[0]);
    let mut store = crate::DfsControlPlaneStore::new(publisher.as_mut());
    let before = store
        .get_file_system_config(&sifs_file_system_id())
        .unwrap()
        .unwrap();
    let mut desired = before.clone();
    desired.retention_policies.insert(
        "module.dfs.history".into(),
        crate::DfsRetentionPolicy {
            rules: vec![crate::DfsRetentionRule {
                path: BASE.into(),
                minimum: crate::DfsRetentionMinimum {
                    keep_last: 1,
                    keep_for_seconds: 0,
                },
            }],
        },
    );
    assert_ne!(desired, before);
    let policy_result = store.compare_and_put_file_system_config(Some(&before), &desired);
    let actual_policy = store.get_file_system_config(&sifs_file_system_id());
    drop(store);
    drop(publisher);
    let changed_attrs = DfsPosixCreateAttrs::new(0o700, 811, 812);
    let changed =
        owner.posix_mkdir_with_create_attrs_and_operation_id(&path, changed_attrs, operation_id);
    let after_changed_request = owner
        .metadata_backend()
        .unwrap()
        .get_namespace_request(&original_request.requester_node_id, operation_id)
        .unwrap()
        .unwrap();
    let after_changed_requests = requests_for_path(&owner, &shard, &path);
    let after_changed = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&original.digest.operation_id)
        .unwrap()
        .unwrap();
    let before_replay_occupant = owner
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), &path)
        .unwrap();
    checkpoint(
        &runtime.borrow(),
        &shard.primary_node_id,
        "retention-prepared-policy-changed",
        serde_json::json!({"policy_result":format!("{policy_result:?}"),"actual_policy":match &actual_policy { Ok(value)=>serde_json::json!({"Ok":value}),Err(error)=>serde_json::json!({"Err":error.to_string()}) },
            "before":before,"desired":desired,"changed_command":format!("{changed:?}"),
            "original":original,"after_changed":after_changed,"occupant":before_replay_occupant,
            "original_request":original_request,"after_changed_request":after_changed_request,
            "after_changed_path_requests":after_changed_requests,
            "changed_caller_operation_id":operation_id,"changed_attrs":changed_attrs,
            "identity_provenance":"real stable mkdir passes exact caller operation_id as command.request_id; native operation_id uses requester+request_id, attrs alter only command_digest; no synthetic request is dispatched"}),
    );
    assert_eq!(policy_result.unwrap(), true);
    assert_eq!(actual_policy.unwrap(), Some(desired));
    assert!(matches!(changed, Err(DfsPosixError::Conflict { .. })));
    assert!(after_changed_request.matches_immutable_payload(&original_request));
    assert_eq!(
        after_changed_request.operation_id,
        original_request.operation_id
    );
    assert_eq!(
        after_changed_request.command_digest,
        original_request.command_digest
    );
    assert_eq!(
        after_changed_requests.len(),
        1,
        "changed attrs cannot derive or admit a second request identity"
    );
    assert!(after_changed_requests[0].matches_immutable_payload(&original_request));
    assert_eq!(after_changed, original);
    assert!(before_replay_occupant.is_none());
    drop(owned_negative);
    let runtime = Rc::try_unwrap(runtime)
        .ok()
        .expect("all owned fault guards released")
        .into_inner();

    let previous = owner.set_transport_operation_deadline(Some(
        std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    ));
    let recovered =
        owner.posix_mkdir_with_create_attrs_and_operation_id(&path, attrs, operation_id);
    owner.set_transport_operation_deadline(previous);
    let retained = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&original.digest.operation_id)
        .unwrap()
        .unwrap();
    let actual_object = owner
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), &path)
        .unwrap();
    let matching: Vec<_> = all_preparations(&owner, &shard)
        .into_iter()
        .filter(|record| {
            record
                .transaction
                .objects
                .iter()
                .any(|object| object.path == path)
        })
        .collect();
    checkpoint(
        &runtime,
        &shard.primary_node_id,
        "retention-prepared-recovered",
        serde_json::json!({"recovered":format!("{recovered:?}"),"original":original,
            "retained":retained,"actual_object":actual_object,"all_matching":matching}),
    );
    let recovered = recovered.unwrap();
    let expected = original
        .transaction
        .objects
        .iter()
        .find(|object| object.path == path)
        .unwrap();
    assert_eq!(
        (recovered.object_id, recovered.version),
        (expected.object_id.clone(), expected.version.clone())
    );
    assert_eq!(
        (recovered.mode, recovered.uid, recovered.gid),
        (0o710, 811, 812)
    );
    assert_eq!(retained.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(retained.digest, original.digest);
    assert_eq!(retained.payload, original.payload);
    assert_eq!(matching, vec![retained]);
    assert_eq!(actual_object.unwrap().object_id, expected.object_id);
    drop(owner);
    runtime.close().unwrap();
}
