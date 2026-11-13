//! Directory creation identities through actual namespace owners and recovery.
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin};
use crate::{DfsPreparedOperationRecord, DfsPreparedOperationStatus};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const BASE: &str = "/services/directory-identity-native";

fn fixture(name: &str) -> (DfsTestRuntime, Vec<NodeId>, DfsMetadataShardConfig) {
    let (runtime, service, shard) =
        crate::test_support::current_native_metadata_service(name, 3, BASE).unwrap();
    drop(service);
    (runtime, DfsTestRuntime::ordered_node_ids(name, 3), shard)
}

fn checkpoint(runtime: &DfsTestRuntime, node: &NodeId, label: &str, facts: serde_json::Value) {
    let count = runtime.checkpoint_rpc_observation_window().unwrap();
    if count > 0 {
        runtime
            .export_latest_rpc_observation_checkpoint(
                &runtime
                    .data_dir(node)
                    .root()
                    .join(format!("{label}-rpc.json")),
            )
            .unwrap();
    }
    std::fs::write(
        runtime.data_dir(node).root().join(format!("{label}.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "checkpointed_rpc_records":count,"facts":facts,
            "qualification":"actual initialized native owners; no handwritten metadata, receipts or replay identity"
        }))
        .unwrap(),
    )
    .unwrap();
}

fn all_preparations(
    service: &NetworkedDfsService<crate::DfsNodeTransport>,
    shard: &DfsMetadataShardConfig,
) -> Vec<DfsPreparedOperationRecord> {
    service
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&crate::DfsMetadataShardId::new(&shard.shard_id))
        .unwrap()
        .unwrap()
        .tables
        .prepared_operations_v7
}

fn preparation_for(
    runtime: &DfsTestRuntime,
    service: &NetworkedDfsService<crate::DfsNodeTransport>,
    shard: &DfsMetadataShardConfig,
    object_id: &DfsObjectId,
) -> DfsPreparedOperationRecord {
    let matching: Vec<_> = all_preparations(service, shard)
        .into_iter()
        .filter(|record| {
            record.transaction.objects.iter().any(|object| {
                &object.object_id == object_id && object.kind == DfsObjectKind::Directory
            })
        })
        .collect();
    checkpoint(
        runtime,
        service.node_id(),
        &format!("directory-creation-body-{object_id}"),
        serde_json::json!({"object_id":object_id,"actual_creation_bodies":matching}),
    );
    assert_eq!(matching.len(), 1, "one actual directory creation body");
    matching.into_iter().next().unwrap()
}

#[test]
fn remote_mkdir_all_recreates_a_renamed_tree_without_reusing_moved_identities() {
    let (runtime, nodes, shard) = fixture("native-directory-identity-remote");
    let caller = nodes
        .iter()
        .find(|node| **node != shard.primary_node_id)
        .unwrap()
        .clone();
    let mut client = runtime.client(&caller).unwrap();
    let source = format!("{BASE}/source");
    let leaf = format!("{source}/level/deep");
    let retired = format!("{BASE}/retired");
    client.posix_mkdir_all(&leaf).unwrap();
    let original_root = client.posix_stat_distributed(&source).unwrap();
    let original_leaf = client.posix_stat_distributed(&leaf).unwrap();
    checkpoint(
        &runtime,
        &caller,
        "directory-remote-before-rename",
        serde_json::json!({"root":format!("{original_root:?}"),"leaf":format!("{original_leaf:?}")}),
    );
    client.posix_rename(&source, &retired).unwrap();
    client.posix_mkdir_all(&leaf).unwrap();
    let new_root = client.posix_stat_distributed(&source).unwrap();
    let new_leaf = client.posix_stat_distributed(&leaf).unwrap();
    let moved_root = client.posix_stat_distributed(&retired).unwrap();
    let moved_leaf = client
        .posix_stat_distributed(&format!("{retired}/level/deep"))
        .unwrap();
    let owner = runtime.metadata_primary_service(&leaf).unwrap();
    let prepared = preparation_for(&runtime, &owner, &shard, &new_leaf.object_id);
    checkpoint(
        &runtime,
        &caller,
        "directory-remote-recreated",
        serde_json::json!({
            "old_root":format!("{original_root:?}"),"old_leaf":format!("{original_leaf:?}"),
            "new_root":format!("{new_root:?}"),"new_leaf":format!("{new_leaf:?}"),
            "moved_root":format!("{moved_root:?}"),"moved_leaf":format!("{moved_leaf:?}"),
            "actual_new_preparation":prepared
        }),
    );
    assert_ne!(new_root.object_id, original_root.object_id);
    assert_ne!(new_leaf.object_id, original_leaf.object_id);
    assert_eq!(moved_root.object_id, original_root.object_id);
    assert_eq!(moved_leaf.object_id, original_leaf.object_id);
    assert_eq!(moved_leaf.mode, original_leaf.mode);
    assert_eq!(
        (moved_leaf.uid, moved_leaf.gid),
        (original_leaf.uid, original_leaf.gid)
    );
    assert_eq!(prepared.status, DfsPreparedOperationStatus::Committed);
    assert!(prepared.committed_entry.is_some());
    drop(owner);
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn direct_native_mkdir_and_opened_parent_creation_use_new_object_identities() {
    let (runtime, _, shard) = fixture("native-directory-identity-direct");
    let mut direct = runtime.metadata_primary_service(BASE).unwrap();
    assert_eq!(direct.node_id(), &shard.primary_node_id);
    assert!(!direct.transport.delegates_local_namespace_authority());
    assert!(direct.namespace_execution.request().is_none());
    direct.posix_mkdir_all(BASE).unwrap();
    let plain = format!("{BASE}/plain");
    let old = direct
        .posix_mkdir_with_create_attrs(&plain, DfsPosixCreateAttrs::new(0o700, 123, 456))
        .unwrap();
    direct
        .posix_rename(&plain, &format!("{BASE}/retired-plain"))
        .unwrap();
    let new = direct.posix_mkdir(&plain).unwrap();
    let moved = direct
        .posix_stat_distributed(&format!("{BASE}/retired-plain"))
        .unwrap();
    assert_ne!(new.object_id, old.object_id);
    assert_eq!(moved.object_id, old.object_id);
    assert_eq!((moved.mode, moved.uid, moved.gid), (0o700, 123, 456));
    // The exact Core owns Services, while the normal mounted requester owns
    // absolute namespace resolution through Main, including the Root anchor.
    let mut requester = runtime.client(&shard.primary_node_id).unwrap();
    assert!(requester.transport.delegates_local_namespace_authority());
    requester.bootstrap_distributed_sifs().unwrap();
    assert_eq!(
        requester
            .posix_namespace_lstat_distributed("/")
            .unwrap()
            .kind,
        DfsObjectKind::Directory
    );
    let opened = requester
        .posix_open_beneath_no_links_distributed("/", BASE)
        .unwrap();
    let guarded = direct
        .posix_mkdir_at_opened_directory_no_links(&opened, "guarded")
        .unwrap();
    direct
        .posix_rename(
            &format!("{BASE}/guarded"),
            &format!("{BASE}/retired-guarded"),
        )
        .unwrap();
    let reopened = requester
        .posix_open_beneath_no_links_distributed("/", BASE)
        .unwrap();
    let recreated = direct
        .posix_mkdir_at_opened_directory_no_links(&reopened, "guarded")
        .unwrap();
    assert_ne!(recreated.object_id, guarded.object_id);
    assert_eq!(
        direct
            .posix_stat_distributed(&format!("{BASE}/retired-guarded"))
            .unwrap()
            .object_id,
        guarded.object_id
    );
    checkpoint(
        &runtime,
        &shard.primary_node_id,
        "directory-direct-created",
        serde_json::json!({"old":format!("{old:?}"),"new":format!("{new:?}"),"moved":format!("{moved:?}"),
            "guarded":format!("{guarded:?}"),"recreated":format!("{recreated:?}"),
            "new_preparation":preparation_for(&runtime,&direct,&shard,&new.object_id),
            "guarded_preparation":preparation_for(&runtime,&direct,&shard,&recreated.object_id)}),
    );
    drop(requester);
    drop(direct);
    runtime.close().unwrap();
}

#[test]
fn stable_mkdir_original_replays_after_rename_and_restart_without_recreation() {
    let (mut runtime, nodes, shard) = fixture("native-directory-identity-stable");
    let caller = nodes
        .iter()
        .find(|node| **node != shard.primary_node_id)
        .unwrap()
        .clone();
    let mut client = runtime.client(&caller).unwrap();
    client.posix_mkdir_all(BASE).unwrap();
    let path = format!("{BASE}/stable");
    let retired = format!("{BASE}/retired-stable");
    let attrs = DfsPosixCreateAttrs::new(0o750, 731, 732);
    let operation_id = "native-directory-stable-original";
    let first = client
        .posix_mkdir_with_create_attrs_and_operation_id(&path, attrs, operation_id)
        .unwrap();
    let owner = runtime.metadata_primary_service(&path).unwrap();
    let original = preparation_for(&runtime, &owner, &shard, &first.object_id);
    assert_eq!(original.status, DfsPreparedOperationStatus::Committed);
    drop(owner);
    client.posix_rename(&path, &retired).unwrap();
    let owner = runtime.metadata_primary_service(&path).unwrap();
    let postrename_occupant = owner
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), &path)
        .unwrap()
        .unwrap();
    assert_eq!(postrename_occupant.kind, DfsObjectKind::Tombstone);
    drop(owner);
    checkpoint(
        &runtime,
        &caller,
        "directory-stable-before-restart",
        serde_json::json!({"first":format!("{first:?}"),"original":original,"postrename_occupant":postrename_occupant}),
    );
    drop(client);
    runtime.stop_node(&shard.primary_node_id).unwrap();
    runtime.reopen_node(shard.primary_node_id.clone()).unwrap();
    let mut client = runtime.client(&caller).unwrap();
    let replay = client.posix_mkdir_with_create_attrs_and_operation_id(&path, attrs, operation_id);
    let operation = DfsNamespaceOperation::MkdirWithAttrs {
        path: path.clone(),
        attrs,
    };
    let reconciled = client
        .reconcile_stable_namespace_operation(operation_id, &operation)
        .unwrap();
    let changed = client.posix_mkdir_with_create_attrs_and_operation_id(
        &path,
        DfsPosixCreateAttrs::new(0o700, 731, 732),
        operation_id,
    );
    let mut owner = runtime.metadata_primary_service(&path).unwrap();
    let retained = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&original.digest.operation_id)
        .unwrap()
        .unwrap();
    let occupant = owner
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), &path)
        .unwrap();
    let moved = owner.posix_stat_distributed(&retired).unwrap();
    checkpoint(
        &runtime,
        &caller,
        "directory-stable-replayed",
        serde_json::json!({
            "replay_result":format!("{replay:?}"),"reconciled":reconciled,"changed_attrs":format!("{changed:?}"),
            "occupant":occupant,"postrename_occupant":postrename_occupant,
            "moved":format!("{moved:?}"),"original":original,"retained":retained
        }),
    );
    assert!(matches!(replay, Err(DfsPosixError::NotFound { .. })));
    assert!(matches!(
        reconciled,
        crate::DfsStableNamespaceOperationReconciliation::Applied { .. }
    ));
    assert!(matches!(changed, Err(DfsPosixError::Conflict { .. })));
    assert_eq!(occupant, Some(postrename_occupant));
    assert_eq!(moved.object_id, first.object_id);
    assert_eq!((moved.mode, moved.uid, moved.gid), (0o750, 731, 732));
    assert_eq!(retained, original);
    drop(owner);
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn hidden_original_mkdir_reserves_its_path_and_recovers_the_same_creation_body() {
    let (runtime, _, shard) = fixture("native-directory-identity-hidden");
    let mut original_owner = runtime.metadata_primary_service(BASE).unwrap();
    original_owner.posix_mkdir_all(BASE).unwrap();
    let path = format!("{BASE}/hidden");
    let attrs = DfsPosixCreateAttrs::new(0o710, 811, 812);
    let operation_id = "native-directory-hidden-original";
    let missing = shard.replica_node_ids.first().unwrap().clone();
    assert_ne!(missing, shard.primary_node_id);
    let negative = FaultRule {
        source: Some(shard.primary_node_id.clone()),
        target: Some(missing),
        route: None,
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: FaultAction::BudgetExhausted,
    };
    let runtime = Rc::new(RefCell::new(runtime));
    let in_hook = Rc::clone(&runtime);
    let reached = Arc::new(AtomicBool::new(false));
    let reached_hook = Arc::clone(&reached);
    let hook = crate::service_metadata::install_namespace_prepare_after_expectations_test_hook(
        move |_| {
            reached_hook.store(true, Ordering::Release);
            in_hook.borrow().install_fault(negative);
        },
    );
    original_owner.set_transport_operation_deadline(Some(
        std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    ));
    let first =
        original_owner.posix_mkdir_with_create_attrs_and_operation_id(&path, attrs, operation_id);
    drop(hook);
    original_owner.set_transport_operation_deadline(None);
    let mut runtime = Rc::try_unwrap(runtime)
        .ok()
        .expect("hook released fixture")
        .into_inner();
    let records = all_preparations(&original_owner, &shard);
    let originals: Vec<_> = records
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
        "directory-hidden-original",
        serde_json::json!({
            "hook_reached":reached.load(Ordering::Acquire),"first_result":format!("{first:?}"),"actual_preparations":originals
        }),
    );
    assert!(reached.load(Ordering::Acquire));
    // The namespace result adapter intentionally carries internal DFS errors
    // as a rejection reason. Check the typed native failure at its real owner.
    assert!(first.is_err());
    assert_eq!(originals.len(), 1);
    let original = originals.into_iter().next().unwrap();
    assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
    let expected = original
        .transaction
        .objects
        .iter()
        .find(|object| object.path == path)
        .unwrap()
        .clone();
    let request = original_owner
        .metadata_backend()
        .unwrap()
        .get_namespace_request(&shard.primary_node_id, operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(request.status, crate::DfsNamespaceRequestStatus::Pending);
    assert_eq!(
        expected.object_id,
        stable_create_object_id(&request.requester_node_id, &path, &request.operation_id)
    );
    assert!(
        original_owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&sifs_file_system_id(), &path)
            .unwrap()
            .is_none()
    );
    original_owner.set_transport_operation_deadline(Some(
        std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    ));
    let native_attempt = original_owner
        .commit_stored_prepared_operation(&original.digest)
        .map(|publication| publication.published);
    let native_recovery_detail = native_attempt.as_ref().err().map(ToString::to_string);
    original_owner.set_transport_operation_deadline(None);
    let after_native_attempt = original_owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&original.digest.operation_id)
        .unwrap()
        .unwrap();
    let after_native_occupant = original_owner
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), &path)
        .unwrap();
    checkpoint(
        &runtime,
        &shard.primary_node_id,
        "directory-hidden-native-attempt",
        serde_json::json!({
            "outer_api_result":format!("{first:?}"),"native_same_digest_result":format!("{native_attempt:?}"),
            "original":original,"after_native_attempt":after_native_attempt,"after_native_occupant":after_native_occupant
        }),
    );
    assert!(matches!(
        native_attempt,
        Err(DfsError::MetadataCommitOutcomeUnknown {
            cause: crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing {
                required: 3,
                acknowledged: 2,
                ..
            },
            ..
        })
    ));
    assert_eq!(after_native_attempt, original);
    assert!(after_native_occupant.is_none());
    let mut competing = runtime.metadata_primary_service(&path).unwrap();
    assert!(!competing.transport.delegates_local_namespace_authority());
    assert!(competing.namespace_execution.request().is_none());
    let competitor_reached = Arc::new(AtomicBool::new(false));
    let competitor_in_hook = Arc::clone(&competitor_reached);
    let competitor_hook =
        crate::service_metadata::install_namespace_prepare_after_expectations_test_hook(
            move |_| {
                competitor_in_hook.store(true, Ordering::Release);
            },
        );
    competing.set_transport_operation_deadline(Some(
        std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    ));
    // A direct new create reaches the actual namespace footprint check; a
    // second routed request's admission could instead fail on the denied ACK.
    let rejected = competing.posix_mkdir_with_create_attrs(&path, attrs);
    drop(competitor_hook);
    competing.set_transport_operation_deadline(None);
    let after = all_preparations(&competing, &shard);
    let same_path: Vec<_> = after
        .into_iter()
        .filter(|record| {
            record
                .transaction
                .objects
                .iter()
                .any(|object| object.path == path)
        })
        .collect();
    let competitor_occupant = competing
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), &path)
        .unwrap();
    let observed_overlap = match &rejected {
        Err(DfsPosixError::Dfs(DfsError::Metadata { message })) => message
            .strip_prefix("metadata serialization error: namespace prepare ")
            .and_then(|message| message.split_once(" overlaps active prepare ")),
        _ => None,
    };
    checkpoint(
        &runtime,
        &shard.primary_node_id,
        "directory-hidden-reserved",
        serde_json::json!({
            "competitor_result":format!("{rejected:?}"),"competitor_planned":competitor_reached.load(Ordering::Acquire),
            "same_path_preparations":same_path,"original":original,"competitor_occupant":competitor_occupant,
            "observed_competitor_operation_id":observed_overlap.map(|(operation,_)|operation),
            "native_recovery_detail":native_recovery_detail
        }),
    );
    assert!(competitor_reached.load(Ordering::Acquire));
    let (competitor_operation_id, original_overlap) =
        observed_overlap.expect("the owning preflight must refuse this exact Original overlap");
    assert!(competitor_operation_id.starts_with("namespace:namespace_mkdir-create_"));
    assert_ne!(competitor_operation_id, original.digest.operation_id);
    assert_eq!(
        original_overlap,
        format!(
            "{}: original recovery failed: {}",
            original.digest.operation_id,
            native_recovery_detail.as_deref().unwrap()
        )
    );
    assert_eq!(same_path, vec![original.clone()]);
    assert!(competitor_occupant.is_none());
    drop(competing);
    drop(original_owner);
    runtime.clear_faults();
    let mut recovered = runtime.metadata_primary_service(&path).unwrap();
    let actual = recovered
        .posix_mkdir_with_create_attrs_and_operation_id(&path, attrs, operation_id)
        .unwrap();
    let retained = recovered
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&original.digest.operation_id)
        .unwrap()
        .unwrap();
    checkpoint(
        &runtime,
        &shard.primary_node_id,
        "directory-hidden-recovered",
        serde_json::json!({
            "actual":format!("{actual:?}"),"original":original,"retained":retained
        }),
    );
    assert_eq!(actual.object_id, expected.object_id);
    assert_eq!(actual.version, expected.version);
    assert_eq!((actual.mode, actual.uid, actual.gid), (0o710, 811, 812));
    assert_eq!(retained.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(retained.digest, original.digest);
    assert_eq!(retained.payload, original.payload);
    drop(recovered);
    runtime.close().unwrap();
}

#[path = "directory_identity_native_tests/retention_configuration_native_tests.rs"]
mod retention_configuration_native_tests;
