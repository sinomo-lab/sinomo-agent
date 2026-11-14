//! Tombstone retention helpers for DFS metadata compaction.
//!
//! Backends own the actual compaction writes. This module owns the shared
//! read-only decisions: which timestamp represents tombstone age, current
//! wall-clock nanoseconds, and whether a tombstone still has live metadata
//! references that make it unsafe to compact.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Included, Unbounded};

use time::OffsetDateTime;

use crate::metadata_keys::{
    dentry_key, id_lower_bound, normalize_path, object_inode_id, path_record_for_object,
};
use crate::metadata_records::{
    DfsMetadataTableOp, DfsTombstoneCompactionOptions, DfsTombstoneCompactionReport,
};
use crate::metadata_tables::DfsMetadataTables;
use crate::{DfsObjectId, DfsObjectKind, DfsObjectMetadata};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsTombstoneCompactionPlan {
    pub(crate) report: DfsTombstoneCompactionReport,
    pub(crate) compacted_object_ids: Vec<DfsObjectId>,
    pub(crate) table_ops: Vec<DfsMetadataTableOp>,
}

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
            .hardlinks
            .iter()
            .any(|((target_object_id, hardlink_object_id), path_record)| {
                target_object_id == object_id
                    || hardlink_object_id == object_id
                    || &path_record.object_id == object_id
            })
}

pub(crate) fn plan_tombstone_compaction(
    tables: &DfsMetadataTables,
    options: &DfsTombstoneCompactionOptions,
    now: u64,
) -> DfsTombstoneCompactionPlan {
    let mut report = DfsTombstoneCompactionReport::for_options(options);
    let mut compacted_object_ids = Vec::new();
    let mut table_ops = Vec::new();
    let mut removed_dentries = BTreeSet::new();
    let mut adjusted_dirfrags = BTreeMap::new();

    for object in tables
        .objects
        .values()
        .filter(|object| object.kind == DfsObjectKind::Tombstone)
    {
        report.scanned_tombstone_count = report.scanned_tombstone_count.saturating_add(1);
        let age_nanos = now.saturating_sub(object_timestamp_for_retention(object));
        if age_nanos < options.min_tombstone_age_nanos {
            report.retained_young_tombstone_count =
                report.retained_young_tombstone_count.saturating_add(1);
            let eligible_at = object_timestamp_for_retention(object)
                .saturating_add(options.min_tombstone_age_nanos);
            report.next_eligible_at_unix_nanos = Some(
                report
                    .next_eligible_at_unix_nanos
                    .map(|current| current.min(eligible_at))
                    .unwrap_or(eligible_at),
            );
            continue;
        }
        if tombstone_has_live_references(tables, &object.object_id) {
            report.retained_referenced_tombstone_count =
                report.retained_referenced_tombstone_count.saturating_add(1);
            continue;
        }

        report.eligible_tombstone_count = report.eligible_tombstone_count.saturating_add(1);
        if !options.dry_run
            && options
                .max_compactions
                .is_some_and(|max_compactions| report.compacted_tombstone_count >= max_compactions)
        {
            report.max_compactions_reached = true;
            report.retained_limit_tombstone_count =
                report.retained_limit_tombstone_count.saturating_add(1);
            continue;
        }
        if options.dry_run {
            continue;
        }

        let path_record = path_record_for_object(object);
        let dentry_key = dentry_key(&path_record);
        if tables.dentries.contains_key(&dentry_key) && removed_dentries.insert(dentry_key.clone())
        {
            table_ops.push(DfsMetadataTableOp::RemoveDentry {
                parent_inode_id: path_record.parent_inode_id.clone(),
                dirfrag_id: path_record.dirfrag_id.clone(),
                name_hash: path_record.name_hash,
                name: path_record.name.clone(),
            });
            let dirfrag_key = (
                path_record.parent_inode_id.clone(),
                path_record.dirfrag_id.clone(),
            );
            if let Some(dirfrag) = adjusted_dirfrags
                .entry(dirfrag_key.clone())
                .or_insert_with(|| tables.dirfrags.get(&dirfrag_key).cloned())
                .as_mut()
            {
                dirfrag.entry_count = dirfrag.entry_count.saturating_sub(1);
                table_ops.push(DfsMetadataTableOp::PutDirFragment {
                    dirfrag: dirfrag.clone(),
                });
            }
        }
        if tables
            .file_system_objects
            .contains_key(&(object.file_system_id.clone(), object.object_id.clone()))
        {
            report.removed_file_system_object_record_count = report
                .removed_file_system_object_record_count
                .saturating_add(1);
            let op = DfsMetadataTableOp::RemoveFileSystemObject {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
            };
            table_ops.push(op);
        }
        if tables.objects.contains_key(&object.object_id) {
            report.removed_object_record_count =
                report.removed_object_record_count.saturating_add(1);
            let op = DfsMetadataTableOp::RemoveObject {
                object_id: object.object_id.clone(),
            };
            table_ops.push(op);
        }
        let inode_id = object_inode_id(&object.object_id);
        if tables.inodes.contains_key(&inode_id) {
            report.removed_inode_record_count = report.removed_inode_record_count.saturating_add(1);
            let op = DfsMetadataTableOp::RemoveInode { inode_id };
            table_ops.push(op);
        }
        if tables
            .paths
            .contains_key(&(object.file_system_id.clone(), normalize_path(&object.path)))
        {
            report.preserved_path_marker_count =
                report.preserved_path_marker_count.saturating_add(1);
        }
        let op = DfsMetadataTableOp::PutCompactedTombstone {
            object: object.clone(),
        };
        table_ops.push(op);
        report.compacted_marker_count = report.compacted_marker_count.saturating_add(1);
        report.compacted_tombstone_count = report.compacted_tombstone_count.saturating_add(1);
        compacted_object_ids.push(object.object_id.clone());
    }

    DfsTombstoneCompactionPlan {
        report,
        compacted_object_ids,
        table_ops,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_keys::{dentry_key, object_inode_id, path_record_for_object};
    use crate::metadata_records::{
        DfsDirFragmentRecord, DfsInodeRecord, DfsMetadataTableOp, DfsObjectVersionRecord,
        DfsTombstoneCompactionOptions,
    };
    use crate::{DfsFileSystemId, DfsObjectKind, DfsVersion};
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::node::NodeId;
    use std::time::Duration;
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
            namespace_link_count: (DfsObjectKind::Tombstone).initial_namespace_link_count(),
            kind: DfsObjectKind::Tombstone,
            tombstone_provenance: None,
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
                content_durability: None,
                retention: Default::default(),
                module_metadata: Default::default(),
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

    #[test]
    fn young_tombstone_projects_exact_retention_deadline() {
        let mut object = object();
        object.changed_at_unix_nanos = 80;
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutObject { object });

        let plan = plan_tombstone_compaction(
            &tables,
            &DfsTombstoneCompactionOptions::dry_run(Duration::from_nanos(50)),
            100,
        );

        assert_eq!(plan.report.eligible_tombstone_count, 0);
        assert_eq!(plan.report.retained_young_tombstone_count, 1);
        assert_eq!(plan.report.next_eligible_at_unix_nanos, Some(130));
    }

    #[test]
    fn compaction_plan_removes_tombstone_indexes_and_preserves_path_marker() {
        let object = object();
        let path_record = path_record_for_object(&object);
        let inode_id = object_inode_id(&object.object_id);
        let dentry_key = dentry_key(&path_record);
        let mut tables = DfsMetadataTables::default();
        tables.apply(DfsMetadataTableOp::PutObject {
            object: object.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutFileSystemObject {
            path_record: path_record.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutPath {
            path_record: path_record.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutInode {
            inode: DfsInodeRecord::from(&object),
        });
        tables.apply(DfsMetadataTableOp::PutDentry {
            path_record: path_record.clone(),
        });

        let options = DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50));
        let plan = plan_tombstone_compaction(&tables, &options, 100);

        assert_eq!(plan.report.scanned_tombstone_count, 1);
        assert_eq!(plan.report.eligible_tombstone_count, 1);
        assert_eq!(plan.report.compacted_tombstone_count, 1);
        assert_eq!(plan.report.removed_object_record_count, 1);
        assert_eq!(plan.report.removed_inode_record_count, 1);
        assert_eq!(plan.report.removed_file_system_object_record_count, 1);
        assert_eq!(plan.report.preserved_path_marker_count, 1);
        assert_eq!(plan.report.compacted_marker_count, 1);
        assert_eq!(plan.compacted_object_ids, vec![object.object_id.clone()]);
        assert_eq!(plan.table_ops.len(), 5);

        let mut compacted = tables;
        for op in plan.table_ops {
            compacted.apply(op);
        }
        assert!(!compacted.objects.contains_key(&object.object_id));
        assert!(!compacted.inodes.contains_key(&inode_id));
        assert!(
            !compacted
                .file_system_objects
                .contains_key(&(object.file_system_id.clone(), object.object_id.clone()))
        );
        assert!(!compacted.dentries.contains_key(&dentry_key));
        assert!(
            compacted
                .paths
                .contains_key(&(object.file_system_id.clone(), path_record.path.clone()))
        );
        assert!(compacted.compacted_tombstone_matches(&object));
    }

    #[test]
    fn compaction_plan_accumulates_dirfrag_counts_without_cloning_tables() {
        let first = object();
        let mut second = object();
        second.object_id =
            DfsObjectId::from_legacy_named("obj_tombstone_second").expect("object id");
        second.path = "/docs/deleted-second.md".to_string();
        let first_path = path_record_for_object(&first);
        let second_path = path_record_for_object(&second);
        assert_eq!(first_path.parent_inode_id, second_path.parent_inode_id);
        assert_eq!(first_path.dirfrag_id, second_path.dirfrag_id);

        let mut tables = DfsMetadataTables::default();
        for (object, path_record) in [(&first, &first_path), (&second, &second_path)] {
            tables.apply(DfsMetadataTableOp::PutObject {
                object: object.clone(),
            });
            tables.apply(DfsMetadataTableOp::PutFileSystemObject {
                path_record: path_record.clone(),
            });
            tables.apply(DfsMetadataTableOp::PutPath {
                path_record: path_record.clone(),
            });
            tables.apply(DfsMetadataTableOp::PutInode {
                inode: DfsInodeRecord::from(object),
            });
            tables.apply(DfsMetadataTableOp::PutDentry {
                path_record: path_record.clone(),
            });
        }
        tables.apply(DfsMetadataTableOp::PutDirFragment {
            dirfrag: DfsDirFragmentRecord {
                file_system_id: first.file_system_id.clone(),
                dir_inode_id: first_path.parent_inode_id.clone(),
                dirfrag_id: first_path.dirfrag_id.clone(),
                hash_start_inclusive: 0,
                hash_end_exclusive: u64::MAX,
                entry_count: 2,
                split_after_count: 1024,
            },
        });

        let plan = plan_tombstone_compaction(
            &tables,
            &DfsTombstoneCompactionOptions::compact_eligible(Duration::from_nanos(50)),
            100,
        );
        let mut compacted = tables;
        for op in plan.table_ops {
            compacted.apply(op);
        }

        assert_eq!(plan.report.compacted_tombstone_count, 2);
        assert!(compacted.dentries.is_empty());
        assert_eq!(
            compacted
                .dirfrags
                .get(&(
                    first_path.parent_inode_id.clone(),
                    first_path.dirfrag_id.clone(),
                ))
                .expect("dirfrag")
                .entry_count,
            0
        );
    }
}
