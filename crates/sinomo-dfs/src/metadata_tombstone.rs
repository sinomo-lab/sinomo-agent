//! Tombstone retention helpers for DFS metadata compaction.
//!
//! Backends own the actual compaction writes. This module owns the shared
//! read-only decisions: which timestamp represents tombstone age, current
//! wall-clock nanoseconds, and whether a tombstone still has live metadata
//! references that make it unsafe to compact.

use std::ops::Bound::{Included, Unbounded};

use time::OffsetDateTime;

use crate::metadata_keys::id_lower_bound;
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsObjectId, DfsObjectMetadata};

pub(crate) fn metadata_now_unix_nanos() -> u64 {
    OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(0)
}

pub(crate) fn object_timestamp_for_retention(object: &DfsObjectMetadata) -> u64 {
    [
        object.changed_at_unix_nanos,
        object.modified_at_unix_nanos,
        object.created_at_unix_nanos,
        object.accessed_at_unix_nanos,
    ]
    .into_iter()
    .find(|value| *value > 0)
    .unwrap_or(0)
}

pub(crate) fn tombstone_has_live_references(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
) -> bool {
    tables
        .versions
        .range((Included((object_id.clone(), id_lower_bound())), Unbounded))
        .take_while(|((record_object_id, _), _)| record_object_id == object_id)
        .next()
        .is_some()
        || tables
            .receipts
            .range((
                Included((object_id.clone(), id_lower_bound(), id_lower_bound())),
                Unbounded,
            ))
            .take_while(|((record_object_id, _, _), _)| record_object_id == object_id)
            .next()
            .is_some()
        || tables
            .intents
            .range((Included((object_id.clone(), String::new())), Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
            .next()
            .is_some()
        || tables
            .conflicts
            .range((Included((object_id.clone(), String::new())), Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
            .next()
            .is_some()
        || tables
            .visible_commits
            .range((Included((object_id.clone(), id_lower_bound())), Unbounded))
            .take_while(|((record_object_id, _), _)| record_object_id == object_id)
            .next()
            .is_some()
        || tables
            .hardlinks
            .iter()
            .any(|((target_object_id, hardlink_object_id), path_record)| {
                target_object_id == object_id
                    || hardlink_object_id == object_id
                    || &path_record.object_id == object_id
            })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_records::{DfsMetadataTableOp, DfsObjectVersionRecord};
    use crate::{DfsFileSystemId, DfsObjectKind, DfsVersion};
    use sinomo_core::DfsObjectPolicy;
    use sinomo_node::NodeId;
    use time::OffsetDateTime;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("fs_tombstone").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_tombstone").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn object() -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "/docs/deleted.md".to_string(),
            kind: DfsObjectKind::Tombstone,
            owner_node_id: node("node_owner"),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 2,
            created_at_unix_nanos: 3,
            changed_at_unix_nanos: 4,
            version: version("version_tombstone_1"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    #[test]
    fn retention_timestamp_uses_first_available_mutation_clock() {
        let mut object = object();
        assert_eq!(object_timestamp_for_retention(&object), 4);
        object.changed_at_unix_nanos = 0;
        assert_eq!(object_timestamp_for_retention(&object), 2);
        object.modified_at_unix_nanos = 0;
        assert_eq!(object_timestamp_for_retention(&object), 3);
        object.created_at_unix_nanos = 0;
        assert_eq!(object_timestamp_for_retention(&object), 1);
        object.accessed_at_unix_nanos = 0;
        assert_eq!(object_timestamp_for_retention(&object), 0);
    }

    #[test]
    fn tombstone_reference_check_follows_version_index() {
        let object = object();
        let mut tables = DfsMetadataTables::default();
        assert!(!tombstone_has_live_references(&tables, &object.object_id));

        tables.apply(DfsMetadataTableOp::PutVersion {
            version: DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: None,
                size_bytes: None,
                locations: Vec::new(),
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            },
        });

        assert!(tombstone_has_live_references(&tables, &object.object_id));
    }
}
