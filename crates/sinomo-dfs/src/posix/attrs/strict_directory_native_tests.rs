use super::*;
use crate::test_runtime::DfsTestRuntime;
use crate::{DfsMetadataShardId, DfsObjectMetadata, DfsPreparedOperationStatus};

const BASE: &str = "/services/strict-directory-native";
const PRIVATE: DfsPosixCreateAttrs = DfsPosixCreateAttrs::new(0o700, 1001, 1002);

fn fixture(name: &str) -> (DfsTestRuntime, crate::MountedSifsService) {
    let (runtime, owner, shard) =
        crate::test_support::current_native_metadata_service(name, 3, BASE).unwrap();
    drop(owner);
    let caller = DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .find(|node| node != &shard.primary_node_id)
        .unwrap();
    let mut client = runtime.client(&caller).unwrap();
    client.bootstrap_distributed_sifs().unwrap();
    (runtime, client)
}

fn object(runtime: &DfsTestRuntime, path: &str) -> Option<DfsObjectMetadata> {
    runtime
        .metadata_primary_service(path)
        .unwrap()
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&crate::sifs_file_system_id(), path)
        .unwrap()
}

fn evidence(
    runtime: &DfsTestRuntime,
    client: &crate::MountedSifsService,
    label: &str,
    facts: serde_json::Value,
) {
    let count = runtime.checkpoint_rpc_observation_window().unwrap();
    let root = runtime.data_dir(client.node_id()).root();
    if count > 0 {
        runtime
            .export_latest_rpc_observation_checkpoint(&root.join(format!("{label}-rpc.json")))
            .unwrap();
    }
    std::fs::write(
        root.join(format!("{label}.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"checkpointed_rpc_records":count,"facts":facts}),
        )
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn strict_directory_create_is_attributed_and_idempotent() {
    let (runtime, mut client) = fixture("native-strict-directory-create");
    let path = format!("{BASE}/home");
    let first = client
        .posix_ensure_directory_beneath_no_links_with_attrs(&path, PRIVATE)
        .unwrap();
    let before = object(&runtime, &path).unwrap();
    let second = client
        .posix_ensure_directory_beneath_no_links_with_attrs(&path, PRIVATE)
        .unwrap();
    let after = object(&runtime, &path).unwrap();
    let owner = runtime.metadata_primary_service(&path).unwrap();
    let snapshot = owner
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&DfsMetadataShardId::new("services"))
        .unwrap()
        .unwrap();
    let creates: Vec<_> = snapshot
        .tables
        .prepared_operations_v7
        .into_iter()
        .filter(|record| {
            record.transaction.objects.iter().any(|entry| {
                entry.object_id == first.object_id && entry.kind == DfsObjectKind::Directory
            })
        })
        .collect();
    evidence(
        &runtime,
        &client,
        "strict-directory-create",
        serde_json::json!({
            "before":before,"after":after,"actual_creation_bodies":creates
        }),
    );
    assert_eq!((first.mode, first.uid, first.gid), (0o700, 1001, 1002));
    assert_eq!(first, second);
    assert_eq!(before, after);
    assert_eq!(creates.len(), 1);
    assert_eq!(creates[0].status, DfsPreparedOperationStatus::Committed);
    let created = creates[0]
        .transaction
        .objects
        .iter()
        .find(|entry| entry.object_id == first.object_id)
        .unwrap();
    assert_eq!(
        (created.mode, created.uid, created.gid),
        (0o700, 1001, 1002)
    );
    drop(owner);
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn strict_existing_owner_mode_and_insecure_directory_remain_unchanged() {
    let (runtime, mut client) = fixture("native-strict-directory-existing");
    let path = format!("{BASE}/valid");
    client
        .posix_ensure_directory_beneath_no_links_with_attrs(&path, PRIVATE)
        .unwrap();
    let before = object(&runtime, &path).unwrap();
    let mut results = Vec::new();
    for wrong in [
        DfsPosixCreateAttrs::new(0o755, 1001, 1002),
        DfsPosixCreateAttrs::new(0o700, 1003, 1002),
        DfsPosixCreateAttrs::new(0o700, 1001, 1003),
    ] {
        let result = client.posix_ensure_directory_beneath_no_links_with_attrs(&path, wrong);
        results.push(format!("{result:?}"));
        assert!(matches!(result, Err(DfsPosixError::Conflict { .. })));
        assert_eq!(object(&runtime, &path).unwrap(), before);
    }
    let insecure = format!("{BASE}/original-0755");
    client
        .posix_mkdir_with_create_attrs(&insecure, DfsPosixCreateAttrs::new(0o755, 1001, 1002))
        .unwrap();
    let old_insecure = object(&runtime, &insecure).unwrap();
    let refused = client.posix_ensure_directory_beneath_no_links_with_attrs(&insecure, PRIVATE);
    let new_insecure = object(&runtime, &insecure).unwrap();
    evidence(
        &runtime,
        &client,
        "strict-directory-refusals",
        serde_json::json!({
            "original":before,"refusals":results,"old_insecure":old_insecure,
            "new_insecure":new_insecure,"insecure_result":format!("{refused:?}")
        }),
    );
    assert!(matches!(refused, Err(DfsPosixError::Conflict { .. })));
    assert_eq!(old_insecure, new_insecure);
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn strict_inspection_is_noncreating_and_links_are_refused() {
    let (runtime, mut client) = fixture("native-strict-directory-links");
    let missing = format!("{BASE}/missing/home");
    assert!(
        client
            .posix_inspect_directory_beneath_no_links_with_attrs(&missing, PRIVATE)
            .unwrap()
            .is_none()
    );
    assert!(object(&runtime, &missing).is_none());
    assert!(object(&runtime, &format!("{BASE}/missing")).is_none());
    let target = format!("{BASE}/target");
    client
        .posix_ensure_directory_beneath_no_links_with_attrs(&target, PRIVATE)
        .unwrap();
    let target_before = object(&runtime, &target).unwrap();
    let link = format!("{BASE}/link");
    let nodes = client
        .placement_content_candidates()
        .unwrap()
        .into_iter()
        .map(|entry| entry.node_id)
        .collect();
    client
        .posix_symlink(
            &target,
            &link,
            crate::DfsWriteOptions::replicated_eventual(nodes),
        )
        .unwrap();
    // The public W1 write can return while its normal N2 receipt settles.
    // Join that actual native input before measuring refusal side effects.
    let initial_link = object(&runtime, &link).unwrap();
    let observer = runtime.metadata_primary_service(&link).unwrap();
    let durability = observer
        .metadata_backend()
        .unwrap()
        .get_version(&initial_link.object_id, &initial_link.version)
        .unwrap()
        .unwrap()
        .content_durability
        .unwrap();
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut poll_error = None;
    let receipts = loop {
        let receipts = observer
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&initial_link.object_id, &durability.source_version)
            .unwrap();
        let complete = durability.desired_replica_node_ids.iter().all(|node| {
            receipts.iter().any(|receipt| {
                &receipt.replica_node_id == node
                    && receipt.content_hash == initial_link.content_hash
            })
        });
        if complete || std::time::Instant::now() >= deadline {
            break receipts;
        }
        if let Err(error) = client.process_incoming() {
            poll_error = Some(error.to_string());
            break receipts;
        }
        std::thread::yield_now();
    };
    let physical = durability
        .desired_replica_node_ids
        .iter()
        .map(|node| {
            let result = runtime.client(node).and_then(|reader| {
                reader
                    .content()
                    .read_object_version(
                        initial_link.object_id.as_str(),
                        durability.source_version.as_str(),
                    )
                    .map(|(manifest, bytes)| (manifest.content_hash, manifest.size_bytes, bytes))
                    .map_err(|error| error.to_string())
            });
            (node.clone(), result)
        })
        .collect::<Vec<_>>();
    evidence(
        &runtime,
        &client,
        "strict-directory-link-input",
        serde_json::json!({
            "actual_initial_link":initial_link,"actual_durability":durability,"actual_receipts":receipts,
            "physical_desired_holders":physical,"poll_error":poll_error
        }),
    );
    assert_eq!(durability.policy.min_commit_replicas, 1);
    assert_eq!(durability.policy.desired_replicas, 2);
    assert_eq!(durability.desired_replica_node_ids.len(), 2);
    assert!(poll_error.is_none());
    assert!(durability.desired_replica_node_ids.iter().all(|node| {
        receipts.iter().any(|receipt| {
            &receipt.replica_node_id == node && receipt.content_hash == initial_link.content_hash
        })
    }));
    for (_, result) in physical {
        let (hash, size, bytes) =
            result.expect("actual symlink desired holder must contain its bytes");
        assert_eq!(Some(hash), initial_link.content_hash);
        assert_eq!(size, target.len() as u64);
        assert_eq!(bytes, target.as_bytes());
    }
    drop(observer);
    let link_before = object(&runtime, &link).unwrap();
    let final_result = client.posix_ensure_directory_beneath_no_links_with_attrs(&link, PRIVATE);
    let child_result = client
        .posix_ensure_directory_beneath_no_links_with_attrs(&format!("{link}/escape"), PRIVATE);
    evidence(
        &runtime,
        &client,
        "strict-directory-links",
        serde_json::json!({
            "final_result":format!("{final_result:?}"),"child_result":format!("{child_result:?}"),
            "link_before":link_before,"link_after":object(&runtime,&link),"target_before":target_before,
            "target_after":object(&runtime,&target),"outside_child":object(&runtime,&format!("{target}/escape"))
        }),
    );
    assert!(matches!(
        final_result,
        Err(DfsPosixError::InvalidPath { .. })
    ));
    assert!(matches!(
        child_result,
        Err(DfsPosixError::InvalidPath { .. })
    ));
    assert_eq!(object(&runtime, &link).unwrap(), link_before);
    assert_eq!(object(&runtime, &target).unwrap(), target_before);
    assert!(object(&runtime, &format!("{target}/escape")).is_none());
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn strict_private_creation_refuses_incompatible_sgid_before_effect() {
    let (runtime, mut client) = fixture("native-strict-directory-sgid");
    client.posix_mkdir_all(BASE).unwrap();
    let parent = format!("{BASE}/group");
    client
        .posix_mkdir_with_create_attrs(&parent, DfsPosixCreateAttrs::new(0o2755, 1001, 4242))
        .unwrap();
    let before = object(&runtime, &parent).unwrap();
    let path = format!("{parent}/private");
    let refused = client.posix_ensure_directory_beneath_no_links_with_attrs(&path, PRIVATE);
    let after = object(&runtime, &parent).unwrap();
    let absent = object(&runtime, &path);
    evidence(
        &runtime,
        &client,
        "strict-directory-sgid",
        serde_json::json!({
            "before":before,"after":after,"private_leaf":absent,"refusal":format!("{refused:?}")
        }),
    );
    assert!(matches!(refused, Err(DfsPosixError::Conflict { .. })));
    assert_eq!(before, after);
    assert!(absent.is_none());
    let opened = client
        .posix_open_beneath_no_links_distributed("/", &parent)
        .unwrap();
    let inherited = client
        .posix_mkdir_at_opened_directory_no_links_with_attrs(&opened, "normal-inherited", PRIVATE)
        .unwrap();
    assert_eq!(
        (inherited.mode, inherited.uid, inherited.gid),
        (0o2700, 1001, 4242)
    );
    drop(client);
    runtime.close().unwrap();
}
