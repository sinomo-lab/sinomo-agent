//! POSIX object metadata construction and stat normalization helpers.

use crate::namespace::{initial_version, object_id_for_path};
use crate::{
    DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsPosixCreateAttrs, dfs_now_unix_nanos,
    sifs_file_system_id,
};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;

pub(crate) fn new_posix_metadata_object(
    node_id: &NodeId,
    path: &str,
    kind: DfsObjectKind,
) -> DfsObjectMetadata {
    let mode = default_mode_for_kind(&kind);
    let now = dfs_now_unix_nanos();
    DfsObjectMetadata {
        object_id: object_id_for_path(path),
        file_system_id: sifs_file_system_id(),
        path: path.to_string(),
        kind,
        tombstone_provenance: None,
        owner_node_id: node_id.clone(),
        current_locations: vec![node_id.clone()],
        content_hash: None,
        content_size_bytes: None,
        link_target_object_id: None,
        mode,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: now,
        modified_at_unix_nanos: now,
        created_at_unix_nanos: now,
        changed_at_unix_nanos: now,
        version: initial_version(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    }
}

pub(crate) fn recreated_posix_metadata_object(
    node_id: &NodeId,
    path: &str,
    kind: DfsObjectKind,
) -> DfsObjectMetadata {
    let mut object = new_posix_metadata_object(node_id, path, kind);
    object.object_id = DfsObjectId::generate();
    object
}

pub(crate) fn apply_posix_create_attrs(object: &mut DfsObjectMetadata, attrs: DfsPosixCreateAttrs) {
    object.mode = attrs.mode & 0o7777;
    object.uid = attrs.uid;
    object.gid = attrs.gid;
}

pub(crate) fn inherit_posix_create_attrs(
    attrs: DfsPosixCreateAttrs,
    directory: bool,
    parent_mode: u32,
    parent_gid: u32,
) -> DfsPosixCreateAttrs {
    if parent_mode & 0o2000 == 0 {
        return attrs;
    }
    DfsPosixCreateAttrs::new(
        if directory {
            attrs.mode | 0o2000
        } else {
            attrs.mode
        },
        attrs.uid,
        parent_gid,
    )
}

pub(crate) fn set_object_create_timestamps(object: &mut DfsObjectMetadata) {
    let now = dfs_now_unix_nanos();
    object.accessed_at_unix_nanos = now;
    object.modified_at_unix_nanos = now;
    object.created_at_unix_nanos = now;
    object.changed_at_unix_nanos = now;
}

pub(crate) fn object_timestamp_or_default(value: u64) -> u64 {
    if value == 0 { 1_000_000_000 } else { value }
}

pub(crate) fn object_mode_or_default(object: &DfsObjectMetadata) -> u32 {
    object.mode & 0o7777
}

fn default_mode_for_kind(kind: &DfsObjectKind) -> u32 {
    match kind {
        DfsObjectKind::Directory => 0o755,
        DfsObjectKind::Symlink => 0o777,
        DfsObjectKind::Tombstone => 0,
        _ => 0o644,
    }
}
