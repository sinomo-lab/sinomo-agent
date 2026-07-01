//! Metadata key, cursor, and path derivation helpers.

use time::OffsetDateTime;

use crate::metadata_backend::{
    DfsConflictPageCursor, DfsConflictRecord, DfsConflictScanCursor, DfsDentryPageCursor,
    DfsDirFragmentId, DfsDirectoryPageCursor, DfsInodeId, DfsMetadataShardId, DfsObjectPageCursor,
    DfsObjectVersionPageCursor, DfsObjectVersionRecord, DfsObjectVersionScanCursor, DfsPathRecord,
    DfsReplicaReceiptPageCursor, DfsReplicaReceiptRecord, DfsReplicaReceiptScanCursor,
    DfsWriteIntentPageCursor, DfsWriteIntentRecord, DfsWriteIntentScanCursor,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsObjectMetadata, DfsVersion};
use sinomo_node::NodeId;

const DIRFRAG_BUCKET_COUNT: u64 = 256;

pub(crate) fn path_record_for_object(object: &DfsObjectMetadata) -> DfsPathRecord {
    let normalized = normalize_path(&object.path);
    let name = normalized
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("/")
        .to_string();
    DfsPathRecord {
        file_system_id: object.file_system_id.clone(),
        path: normalized.clone(),
        object_id: object.object_id.clone(),
        inode_id: object_inode_id(&object.object_id),
        parent_inode_id: parent_inode_id(&object.file_system_id, &normalized),
        dirfrag_id: DfsDirFragmentId::new("root"),
        name_hash: stable_u64_hash(&name),
        name,
        kind: object.kind.clone(),
    }
}

pub(crate) fn bucket_dirfrag_id(name_hash: u64) -> DfsDirFragmentId {
    let bucket = dirfrag_bucket(name_hash);
    DfsDirFragmentId::new(format!("bucket-{bucket:02x}"))
}

pub(crate) fn id_lower_bound() -> sinomo_core::Id {
    sinomo_core::Id::new("00000000000000000000000000").expect("static zero ULID")
}

fn dirfrag_bucket(name_hash: u64) -> u64 {
    name_hash
        .checked_div((u64::MAX / DIRFRAG_BUCKET_COUNT).saturating_add(1))
        .unwrap_or(0)
        .min(DIRFRAG_BUCKET_COUNT - 1)
}

pub(crate) fn dirfrag_hash_range(
    dirfrag_id: &DfsDirFragmentId,
    name_hash: Option<u64>,
) -> (u64, u64) {
    if dirfrag_id.0 == "root" {
        return (0, u64::MAX);
    }
    let bucket = name_hash
        .map(dirfrag_bucket)
        .or_else(|| {
            dirfrag_id
                .0
                .strip_prefix("bucket-")
                .and_then(|bucket| u64::from_str_radix(bucket, 16).ok())
        })
        .unwrap_or(0)
        .min(DIRFRAG_BUCKET_COUNT - 1);
    let width = (u64::MAX / DIRFRAG_BUCKET_COUNT).saturating_add(1);
    let start = bucket.saturating_mul(width);
    let end = if bucket + 1 >= DIRFRAG_BUCKET_COUNT {
        u64::MAX
    } else {
        (bucket + 1).saturating_mul(width)
    };
    (start, end)
}

pub(crate) fn object_page_key(object: &DfsObjectMetadata) -> (&String, &DfsObjectId) {
    (&object.path, &object.object_id)
}

pub(crate) fn conflict_page_cursor(conflict: &DfsConflictRecord) -> DfsConflictPageCursor {
    DfsConflictPageCursor {
        conflict_id: conflict.conflict_id.clone(),
    }
}

pub(crate) fn object_version_page_cursor(
    version: &DfsObjectVersionRecord,
) -> DfsObjectVersionPageCursor {
    DfsObjectVersionPageCursor {
        version: version.version.clone(),
        shard_id: None,
    }
}

pub(crate) fn object_version_scan_cursor(
    version: &DfsObjectVersionRecord,
) -> DfsObjectVersionScanCursor {
    DfsObjectVersionScanCursor {
        object_id: version.object_id.clone(),
        version: version.version.clone(),
    }
}

pub(crate) fn object_version_scan_key(
    version: &DfsObjectVersionRecord,
) -> (DfsObjectId, DfsVersion) {
    (version.object_id.clone(), version.version.clone())
}

pub(crate) fn object_version_page_cursor_with_shard(
    mut cursor: DfsObjectVersionPageCursor,
    shard_id: &DfsMetadataShardId,
) -> DfsObjectVersionPageCursor {
    cursor.shard_id = Some(shard_id.clone());
    cursor
}

pub(crate) fn local_object_version_page_cursor(
    cursor: &DfsObjectVersionPageCursor,
) -> DfsObjectVersionPageCursor {
    DfsObjectVersionPageCursor {
        version: cursor.version.clone(),
        shard_id: None,
    }
}

pub(crate) fn replica_receipt_page_cursor(
    receipt: &DfsReplicaReceiptRecord,
) -> DfsReplicaReceiptPageCursor {
    DfsReplicaReceiptPageCursor {
        replica_node_id: receipt.replica_node_id.clone(),
        shard_id: None,
    }
}

pub(crate) fn replica_receipt_scan_cursor(
    receipt: &DfsReplicaReceiptRecord,
) -> DfsReplicaReceiptScanCursor {
    DfsReplicaReceiptScanCursor {
        object_id: receipt.receipt.object_id.clone(),
        version: receipt.receipt.committed_version.clone(),
        replica_node_id: receipt.replica_node_id.clone(),
    }
}

pub(crate) fn replica_receipt_scan_key(
    receipt: &DfsReplicaReceiptRecord,
) -> (DfsObjectId, DfsVersion, NodeId) {
    (
        receipt.receipt.object_id.clone(),
        receipt.receipt.committed_version.clone(),
        receipt.replica_node_id.clone(),
    )
}

pub(crate) fn replica_receipt_page_cursor_with_shard(
    mut cursor: DfsReplicaReceiptPageCursor,
    shard_id: &DfsMetadataShardId,
) -> DfsReplicaReceiptPageCursor {
    cursor.shard_id = Some(shard_id.clone());
    cursor
}

pub(crate) fn local_replica_receipt_page_cursor(
    cursor: &DfsReplicaReceiptPageCursor,
) -> DfsReplicaReceiptPageCursor {
    DfsReplicaReceiptPageCursor {
        replica_node_id: cursor.replica_node_id.clone(),
        shard_id: None,
    }
}

pub(crate) fn write_intent_page_cursor(intent: &DfsWriteIntentRecord) -> DfsWriteIntentPageCursor {
    DfsWriteIntentPageCursor {
        intent_id: intent.intent_id.clone(),
        shard_id: None,
    }
}

pub(crate) fn conflict_scan_cursor(conflict: &DfsConflictRecord) -> DfsConflictScanCursor {
    DfsConflictScanCursor {
        object_id: conflict.object_id.clone(),
        conflict_id: conflict.conflict_id.clone(),
    }
}

pub(crate) fn conflict_scan_key(conflict: &DfsConflictRecord) -> (DfsObjectId, String) {
    (conflict.object_id.clone(), conflict.conflict_id.clone())
}

pub(crate) fn write_intent_scan_key(
    intent: &DfsWriteIntentRecord,
) -> (OffsetDateTime, String, DfsObjectId) {
    (
        intent.created_at,
        intent.intent_id.clone(),
        intent.object_id.clone(),
    )
}

pub(crate) fn intent_key(intent: &DfsWriteIntentRecord) -> (DfsObjectId, String) {
    (intent.object_id.clone(), intent.intent_id.clone())
}

pub(crate) fn write_intent_target_key(
    intent: &DfsWriteIntentRecord,
) -> (DfsObjectId, DfsVersion, String) {
    (
        intent.object_id.clone(),
        intent.target_version.clone(),
        intent.intent_id.clone(),
    )
}

pub(crate) fn write_intent_scan_cursor_key(
    cursor: &DfsWriteIntentScanCursor,
) -> (OffsetDateTime, String, DfsObjectId) {
    (
        cursor.created_at,
        cursor.intent_id.clone(),
        cursor.object_id.clone(),
    )
}

pub(crate) fn write_intent_scan_order(
    left: &DfsWriteIntentRecord,
    right: &DfsWriteIntentRecord,
) -> std::cmp::Ordering {
    write_intent_scan_key(left).cmp(&write_intent_scan_key(right))
}

pub(crate) fn write_intent_scan_cursor(intent: &DfsWriteIntentRecord) -> DfsWriteIntentScanCursor {
    DfsWriteIntentScanCursor {
        created_at: intent.created_at,
        intent_id: intent.intent_id.clone(),
        object_id: intent.object_id.clone(),
    }
}

pub(crate) fn write_intent_page_cursor_with_shard(
    mut cursor: DfsWriteIntentPageCursor,
    shard_id: &DfsMetadataShardId,
) -> DfsWriteIntentPageCursor {
    cursor.shard_id = Some(shard_id.clone());
    cursor
}

pub(crate) fn local_write_intent_page_cursor(
    cursor: &DfsWriteIntentPageCursor,
) -> DfsWriteIntentPageCursor {
    DfsWriteIntentPageCursor {
        intent_id: cursor.intent_id.clone(),
        shard_id: None,
    }
}

pub(crate) fn object_page_cursor(object: &DfsObjectMetadata) -> DfsObjectPageCursor {
    DfsObjectPageCursor {
        path: object.path.clone(),
        object_id: object.object_id.clone(),
        shard_id: None,
    }
}

pub(crate) fn object_page_cursor_with_shard(
    object: &DfsObjectMetadata,
    shard_id: &DfsMetadataShardId,
) -> DfsObjectPageCursor {
    DfsObjectPageCursor {
        path: object.path.clone(),
        object_id: object.object_id.clone(),
        shard_id: Some(shard_id.clone()),
    }
}

pub(crate) fn local_object_page_cursor(cursor: &DfsObjectPageCursor) -> DfsObjectPageCursor {
    DfsObjectPageCursor {
        path: cursor.path.clone(),
        object_id: cursor.object_id.clone(),
        shard_id: None,
    }
}

pub(crate) fn directory_page_key(object: &DfsObjectMetadata) -> (&String, &DfsObjectId) {
    (&object.path, &object.object_id)
}

pub(crate) fn directory_page_cursor_with_dentry(
    object: &DfsObjectMetadata,
    dirfrag_id: DfsDirFragmentId,
    name_hash: u64,
    name: String,
) -> DfsDirectoryPageCursor {
    DfsDirectoryPageCursor {
        path: object.path.clone(),
        object_id: object.object_id.clone(),
        dentry: Some(DfsDentryPageCursor {
            dirfrag_id,
            name_hash,
            name,
        }),
        shard_id: None,
    }
}

pub(crate) fn directory_page_cursor_with_shard(
    mut cursor: DfsDirectoryPageCursor,
    shard_id: &DfsMetadataShardId,
) -> DfsDirectoryPageCursor {
    cursor.shard_id = Some(shard_id.clone());
    cursor
}

pub(crate) fn local_directory_page_cursor(
    cursor: &DfsDirectoryPageCursor,
) -> DfsDirectoryPageCursor {
    DfsDirectoryPageCursor {
        path: cursor.path.clone(),
        object_id: cursor.object_id.clone(),
        dentry: cursor.dentry.clone(),
        shard_id: None,
    }
}

pub(crate) fn normalized_node_location_list(nodes: &[NodeId]) -> Vec<NodeId> {
    let mut nodes = nodes.to_vec();
    normalize_node_locations(&mut nodes);
    nodes
}

pub(crate) fn normalize_node_locations(nodes: &mut Vec<NodeId>) {
    let mut seen = std::collections::BTreeSet::new();
    nodes.retain(|node_id| seen.insert(node_id.clone()));
}

pub(crate) fn object_inode_id(object_id: &DfsObjectId) -> DfsInodeId {
    DfsInodeId::new(format!("obj:{}", object_id))
}

pub(crate) fn parent_inode_id(
    file_system_id: &DfsFileSystemId,
    normalized_path: &str,
) -> DfsInodeId {
    let parent = normalized_path
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or("");
    if parent.is_empty() {
        DfsInodeId::new(format!("fs:{}:root", file_system_id))
    } else {
        DfsInodeId::new(format!("path:{}:{}", file_system_id, path_hash(parent)))
    }
}

pub(crate) fn directory_inode_id(
    file_system_id: &DfsFileSystemId,
    directory_path: &str,
) -> DfsInodeId {
    let normalized = normalize_path(directory_path);
    if normalized.is_empty() {
        DfsInodeId::new(format!("fs:{}:root", file_system_id))
    } else {
        DfsInodeId::new(format!(
            "path:{}:{}",
            file_system_id,
            path_hash(&normalized)
        ))
    }
}

pub(crate) fn normalize_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

pub(crate) fn normalize_posix_path(path: &str) -> String {
    let normalized = normalize_path(path);
    if normalized.is_empty() {
        "/".to_string()
    } else {
        format!("/{normalized}")
    }
}

pub(crate) fn posix_path_is_at_or_under_prefix(path: &str, prefix: &str) -> bool {
    let path = normalize_posix_path(path);
    let prefix = normalize_posix_path(prefix);
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(&prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn path_hash(path: &str) -> String {
    blake3::hash(normalize_path(path).as_bytes())
        .to_hex()
        .to_string()
}

pub(crate) fn stable_u64_hash(value: &str) -> u64 {
    let hash = blake3::hash(value.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_be_bytes(bytes)
}

pub(crate) fn key_segment(value: &impl ToString) -> String {
    value
        .to_string()
        .trim_matches('/')
        .replace(['/', '\\', ':'], "_")
}

pub(crate) fn dentry_key(
    path_record: &DfsPathRecord,
) -> (DfsInodeId, DfsDirFragmentId, u64, String) {
    (
        path_record.parent_inode_id.clone(),
        path_record.dirfrag_id.clone(),
        path_record.name_hash,
        path_record.name.clone(),
    )
}
