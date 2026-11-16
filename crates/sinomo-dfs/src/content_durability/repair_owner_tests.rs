//! Native producers supply these selector inputs. These controls do not grant
//! authority, publish synthetic rows, or model a distributed failure quorum.
use super::*;
use crate::test_runtime::DfsTestRuntime;
use crate::*;

const BYTES: &[u8] = b"same bytes do not identify the same write";

fn fixture(
    name: &str,
) -> (
    DfsTestRuntime,
    sinomo_core::node::NodeId,
    DfsMetadataShardConfig,
    NetworkedDfsService<DfsNodeTransport>,
) {
    let (native, nodes) = DfsTestRuntime::start_cluster(name, 1).unwrap();
    let node = nodes[0].clone();
    let shard = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap();
    assert_eq!(shard.primary_node_id, node);
    let client = native.client(&node).unwrap();
    (native, node, shard, client)
}

fn actual_cut(
    native: &DfsTestRuntime,
    node: &sinomo_core::node::NodeId,
    shard: &DfsMetadataShardConfig,
) -> DfsMetadataTableSnapshot {
    // Read the actual selected physical owner afresh, not a request shell's
    // potentially older metadata mirror or a handcrafted fixture image.
    let root = default_dfs_metadata_shards_root(native.data_dir(node))
        .join(crate::metadata_keys::key_segment(&shard.shard_id));
    let mut reader = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(&shard.shard_id),
        root,
    )
    .unwrap();
    reader.begin_inspection_snapshot().unwrap();
    let cut = reader.export_table_snapshot().unwrap();
    reader.end_inspection_snapshot();
    assert!(cut.prefix_proof.is_well_formed());
    cut
}

fn object_at(cut: &DfsMetadataTableSnapshot, object: &DfsObjectId) -> DfsObjectMetadata {
    cut.tables
        .objects
        .iter()
        .find(|row| &row.object_id == object)
        .cloned()
        .expect("actual native current object")
}

fn version_at(
    cut: &DfsMetadataTableSnapshot,
    object: &DfsObjectId,
    version: &DfsVersion,
) -> DfsObjectVersionRecord {
    cut.tables
        .versions
        .iter()
        .find(|row| &row.object_id == object && &row.version == version)
        .cloned()
        .expect("actual native immutable version")
}

fn committed_intent_at(
    cut: &DfsMetadataTableSnapshot,
    object: &DfsObjectId,
    version: &DfsVersion,
) -> DfsWriteIntentRecord {
    let mut originals = std::collections::BTreeMap::new();
    for record in &cut.tables.prepared_operations_v7 {
        if record.status != DfsPreparedOperationStatus::Committed {
            continue;
        }
        let Some(intent) = record.terminal_intent.as_ref() else {
            continue;
        };
        if &intent.object_id == object && &intent.target_version == version {
            record.validate().unwrap();
            record
                .committed_entry
                .as_ref()
                .expect("actual native commit reference")
                .validate(&record.digest)
                .unwrap();
            originals.insert(intent.intent_id.clone(), intent.clone());
        }
    }
    assert_eq!(originals.len(), 1, "one exact actual write original");
    let intent = originals.into_values().next().unwrap();
    assert!(intent.publication.is_current());
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    intent
}

#[test]
fn native_metadata_only_chmod_keeps_the_actual_current_repair_lineage() {
    let (native, node, shard, mut client) = fixture("repair-owner-native-chmod");
    let path = "/repair-owner-chmod.bin";
    let original = client
        .posix_create_file_with_operation_id(
            path,
            BYTES,
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            &format!("repair-owner-chmod-{}", sinomo_core::Id::generate()),
        )
        .unwrap();
    let before = actual_cut(&native, &node, &shard);
    let intent = committed_intent_at(&before, &original.object_id, &original.version);
    let source = version_at(&before, &original.object_id, &original.version);
    let initial = object_at(&before, &original.object_id);
    assert!(source.created_by_event_id.is_some());
    assert!(current_repair_content_matches(
        &initial,
        &intent,
        Some(&source),
        Some(&source)
    ));

    client.posix_set_mode(path, 0o600).unwrap();
    let after = actual_cut(&native, &node, &shard);
    let object = object_at(&after, &original.object_id);
    let current = version_at(&after, &object.object_id, &object.version);
    assert_eq!(object.mode, 0o600);
    assert_ne!(current.version, source.version);
    assert_eq!(current.content_hash, source.content_hash);
    assert_eq!(current.created_by_event_id, source.created_by_event_id);
    assert!(same_content_lineage(&current, &source));
    assert!(current_repair_content_matches(
        &object,
        &intent,
        Some(&source),
        Some(&current)
    ));
    // Selector-only omission: this proves the existing recorded data-generation
    // fallback, not that native GC has removed the real source row.
    assert!(current_repair_content_matches(
        &object,
        &intent,
        None,
        Some(&current)
    ));
    assert_eq!(client.posix_read_file_distributed(path).unwrap(), BYTES);
    drop(client);
    native.close().unwrap();
}

#[test]
fn native_fresh_equal_bytes_do_not_reactivate_the_previous_current_owner() {
    let (native, node, shard, mut client) = fixture("repair-owner-native-equal-bytes");
    let path = "/repair-owner-equal.bin";
    let original = client
        .posix_write_file(
            path,
            BYTES,
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let before = actual_cut(&native, &node, &shard);
    let old_intent = committed_intent_at(&before, &original.object_id, &original.version);
    let source = version_at(&before, &original.object_id, &original.version);
    let fresh = client
        .posix_write_file(
            path,
            BYTES,
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let after = actual_cut(&native, &node, &shard);
    let object = object_at(&after, &original.object_id);
    let current = version_at(&after, &object.object_id, &object.version);
    let current_intent = committed_intent_at(&after, &object.object_id, &current.version);
    assert_eq!(fresh.object_id, original.object_id);
    assert_ne!(fresh.version, original.version);
    assert_eq!(current.content_hash, source.content_hash);
    assert!(source.created_by_event_id.is_some() && current.created_by_event_id.is_some());
    assert_ne!(current.created_by_event_id, source.created_by_event_id);
    assert_ne!(current_intent.intent_id, old_intent.intent_id);
    assert!(!same_content_lineage(&current, &source));
    assert!(!current_repair_content_matches(
        &object,
        &old_intent,
        Some(&source),
        Some(&current)
    ));
    assert!(current_repair_content_matches(
        &object,
        &current_intent,
        Some(&current),
        Some(&current)
    ));
    assert_eq!(client.posix_read_file_distributed(path).unwrap(), BYTES);
    drop(client);
    native.close().unwrap();
}

#[test]
fn native_hardlink_promotion_and_rename_keep_lineage_until_the_actual_last_unlink() {
    let (native, node, shard, mut client) = fixture("repair-owner-native-hardlink");
    let original_path = "/repair-owner-original.bin";
    let alias_path = "/repair-owner-alias.bin";
    let renamed_path = "/repair-owner-renamed.bin";
    let original = client
        .posix_write_file(
            original_path,
            BYTES,
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let before = actual_cut(&native, &node, &shard);
    let intent = committed_intent_at(&before, &original.object_id, &original.version);
    let source = version_at(&before, &original.object_id, &original.version);
    client.posix_hardlink(original_path, alias_path).unwrap();
    client.posix_unlink(original_path).unwrap();
    let promoted_cut = actual_cut(&native, &node, &shard);
    let promoted = object_at(&promoted_cut, &original.object_id);
    let promoted_version = version_at(&promoted_cut, &promoted.object_id, &promoted.version);
    assert_eq!(promoted.path, alias_path);
    assert_eq!(promoted.kind, DfsObjectKind::File);
    assert!(same_content_lineage(&promoted_version, &source));
    assert!(current_repair_content_matches(
        &promoted,
        &intent,
        Some(&source),
        Some(&promoted_version)
    ));
    let renamed = client.posix_rename(alias_path, renamed_path).unwrap();
    assert_eq!(renamed.object_id, original.object_id);
    let renamed_cut = actual_cut(&native, &node, &shard);
    let object = object_at(&renamed_cut, &original.object_id);
    let current = version_at(&renamed_cut, &object.object_id, &object.version);
    assert_eq!(object.path, renamed_path);
    assert!(same_content_lineage(&current, &source));
    assert!(current_repair_content_matches(
        &object,
        &intent,
        Some(&source),
        Some(&current)
    ));
    assert_eq!(
        client.posix_read_file_distributed(renamed_path).unwrap(),
        BYTES
    );
    client.posix_unlink(renamed_path).unwrap();
    let deleted_cut = actual_cut(&native, &node, &shard);
    let deleted = object_at(&deleted_cut, &original.object_id);
    assert_eq!(deleted.kind, DfsObjectKind::Tombstone);
    assert!(!current_repair_content_matches(
        &deleted,
        &intent,
        Some(&source),
        Some(&current)
    ));
    drop(client);
    native.close().unwrap();
}
