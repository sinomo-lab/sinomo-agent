//! Metadata backend API surface.
//!
//! This module owns the public metadata backend contract and default scan
//! helpers. Concrete local storage lives in `metadata_backend`.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_node::NodeId;

use crate::intent::write_intent_status_is_active;
use crate::metadata_error::Result;
use crate::metadata_hardlink::{object_is_hardlink_to_target, sort_hardlinks_by_path};
use crate::metadata_journal::bounded_metadata_journal_batch;
use crate::metadata_keys::{
    normalize_posix_path, posix_path_is_at_or_under_prefix, write_intent_page_cursor,
};
use crate::metadata_limits::OBJECT_LOCAL_METADATA_PAGE_SIZE;
use crate::metadata_records::*;
use crate::metadata_summary::count_pending_repairs_for_object;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};

/// Durable DFS metadata store interface.
///
/// Implementations own authoritative filesystem/object metadata, object
/// versions, write intents, replica receipts, conflict records, namespace
/// transactions, and metadata journals. The trait intentionally exposes paged
/// scans as primitives so service and operator code can walk large namespaces
/// without materializing every record class at once.
pub trait DfsMetadataBackend: std::fmt::Debug + Send {
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry>;
    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>>;
    fn ensure_file_system(
        &mut self,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        if self.get_file_system(&file_system.file_system_id)?.as_ref() == Some(file_system) {
            return Ok(None);
        }
        self.put_file_system(file_system).map(Some)
    }
    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>>;
    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry>;
    fn put_objects(
        &mut self,
        objects: &[DfsObjectMetadata],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::with_capacity(objects.len());
        for object in objects {
            entries.push(self.put_object(object)?);
        }
        Ok(entries)
    }
    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>>;
    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>>;
    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut objects = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            objects.extend(page.objects);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(objects)
    }

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage>;
    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        const COUNT_PAGE_SIZE: usize = 4096;
        let mut count = 0usize;
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                COUNT_PAGE_SIZE,
            )?;
            count = count.saturating_add(page.objects.len());
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(count)
    }
    fn object_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectPageCursor> {
        Ok(DfsObjectPageCursor {
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            shard_id: None,
        })
    }
    fn child_objects_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let mut children = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    continue;
                }
                let rest = object_path.trim_start_matches(&prefix);
                if !rest.is_empty() && !rest.contains('/') {
                    children.push(object);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        children.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(children)
    }
    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        if max_entries == 0 {
            return Ok(DfsDirectoryPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let mut object_cursor = after.map(|cursor| DfsObjectPageCursor {
            path: cursor.path.clone(),
            object_id: cursor.object_id.clone(),
            shard_id: cursor.shard_id.clone(),
        });
        let mut objects = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        'pages: loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    continue;
                }
                let rest = object_path.trim_start_matches(&prefix);
                if rest.is_empty() || rest.contains('/') {
                    continue;
                }
                if objects.len() >= max_entries {
                    has_more = true;
                    break 'pages;
                }
                let object_cursor = self.object_page_cursor_for_object(&object)?;
                last_cursor = Some(DfsDirectoryPageCursor {
                    path: object_cursor.path,
                    object_id: object_cursor.object_id,
                    dentry: None,
                    shard_id: object_cursor.shard_id,
                });
                objects.push(object);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        Ok(DfsDirectoryPage {
            objects,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }
    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                if object_is_hardlink_to_target(&object, file_system_id, target_object_id) {
                    hardlinks.push(object);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        sort_hardlinks_by_path(&mut hardlinks);
        Ok(hardlinks)
    }
    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let mut object_cursor = after.cloned();
        let mut hardlinks = Vec::new();
        let mut last_cursor = None;
        let mut has_more = false;
        'pages: loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                if !object_is_hardlink_to_target(&object, file_system_id, target_object_id) {
                    continue;
                }
                if hardlinks.len() >= max_entries {
                    has_more = true;
                    break 'pages;
                }
                last_cursor = Some(self.object_page_cursor_for_object(&object)?);
                hardlinks.push(object);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        Ok(DfsObjectPage {
            objects: hardlinks,
            next_cursor: has_more.then_some(last_cursor).flatten(),
        })
    }
    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let mut count = 0usize;
        let mut cursor = None;
        loop {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor.as_ref(),
                1024,
            )?;
            count = count.saturating_add(page.objects.len());
            let Some(next_cursor) = page.next_cursor else {
                return Ok(count);
            };
            cursor = Some(next_cursor);
        }
    }
    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        Ok(self
            .hardlinks_to_object_page(file_system_id, target_object_id, None, 1)?
            .objects
            .into_iter()
            .next())
    }
    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        let mut cursor = None;
        loop {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor.as_ref(),
                1024,
            )?;
            for object in page.objects {
                if !posix_path_is_at_or_under_prefix(&object.path, &prefix) {
                    return Ok(Some(object));
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next_cursor);
        }
    }
    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry>;
    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object_entry = self.put_object(object)?;
        let version_entry = self.put_version(version)?;
        Ok(vec![object_entry, version_entry])
    }
    fn put_object_version_intent_and_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = self.put_object_and_version(object, version)?;
        entries.extend(self.put_intent_and_receipts_for_object(Some(object), intent, receipts)?);
        Ok(entries)
    }
    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>>;
    fn get_object_version_pairs(
        &self,
        keys: &[(DfsObjectId, DfsVersion)],
    ) -> Result<Vec<(Option<DfsObjectMetadata>, Option<DfsObjectVersionRecord>)>> {
        keys.iter()
            .map(|(object_id, version)| {
                Ok((
                    self.get_object(object_id)?,
                    self.get_version(object_id, version)?,
                ))
            })
            .collect()
    }
    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        let mut versions = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.versions_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            versions.extend(page.versions);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(versions)
    }

    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage>;
    fn put_receipt(&mut self, receipt: &DfsReplicaReceiptRecord)
    -> Result<DfsMetadataJournalEntry>;
    fn remove_receipt(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsMetadataJournalEntry>>;
    fn put_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = object;
        self.put_receipt(receipt)
    }
    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        let mut receipts = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            receipts.extend(page.receipts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(receipts)
    }

    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage>;
    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry>;
    fn put_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = object;
        self.put_intent(intent)
    }
    fn put_intent_and_receipts_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::with_capacity(1 + receipts.len());
        entries.push(self.put_intent_for_object(object, intent)?);
        for receipt in receipts {
            entries.push(self.put_receipt_for_object(object, receipt)?);
        }
        Ok(entries)
    }
    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>>;
    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        let mut intents = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.intents_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            intents.extend(page.intents);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(intents)
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage>;
    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        let mut intents = Vec::new();
        let mut cursor = after.cloned();
        loop {
            let page = self.intents_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for intent in page.intents {
                if intent.target_version != *target_version {
                    continue;
                }
                if intents.len() >= max_entries {
                    return Ok(DfsWriteIntentPage {
                        next_cursor: intents.last().map(write_intent_page_cursor),
                        intents,
                    });
                }
                intents.push(intent);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor: None,
        })
    }
    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage>;
    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage>;
    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage>;
    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage>;
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_versions(&self) -> Result<Vec<DfsObjectVersionRecord>> {
        let mut versions = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.version_records_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            versions.extend(page.versions);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(versions)
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_receipts(&self) -> Result<Vec<DfsReplicaReceiptRecord>> {
        let mut receipts = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.receipt_records_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            receipts.extend(page.receipts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(receipts)
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        let mut intents = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.write_intents_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            intents.extend(page.intents);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(intents)
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_conflicts(&self) -> Result<Vec<DfsConflictRecord>> {
        let mut conflicts = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.conflict_records_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            conflicts.extend(page.conflicts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(conflicts)
    }
    fn accumulate_metadata_summary_for_object(
        &self,
        summary: &mut DfsMetadataSummary,
        object: &DfsObjectMetadata,
    ) -> Result<()> {
        summary.object_count = summary.object_count.saturating_add(1);

        let mut object_intents = Vec::new();
        let mut intent_cursor = None;
        loop {
            let intent_page = self.intents_for_object_page(
                &object.object_id,
                intent_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.intent_count = summary
                .intent_count
                .saturating_add(intent_page.intents.len());
            summary.pending_write_count = summary.pending_write_count.saturating_add(
                intent_page
                    .intents
                    .iter()
                    .filter(|intent| write_intent_status_is_active(&intent.status))
                    .count(),
            );
            object_intents.extend(intent_page.intents);
            let Some(next_cursor) = intent_page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }

        let committed_targets = object_intents
            .iter()
            .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
            .map(|intent| intent.target_version.clone())
            .collect::<BTreeSet<_>>();
        let mut receipts_by_version = BTreeMap::<DfsVersion, Vec<DfsReplicaReceiptRecord>>::new();
        let mut version_cursor = None;
        loop {
            let version_page = self.versions_for_object_page(
                &object.object_id,
                version_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.version_count = summary
                .version_count
                .saturating_add(version_page.versions.len());
            for version in version_page.versions {
                let collect_for_repair = committed_targets.contains(&version.version);
                let mut receipt_cursor = None;
                loop {
                    let receipt_page = self.receipts_for_version_page(
                        &object.object_id,
                        &version.version,
                        receipt_cursor.as_ref(),
                        OBJECT_LOCAL_METADATA_PAGE_SIZE,
                    )?;
                    summary.receipt_count = summary
                        .receipt_count
                        .saturating_add(receipt_page.receipts.len());
                    if collect_for_repair {
                        receipts_by_version
                            .entry(version.version.clone())
                            .or_default()
                            .extend(receipt_page.receipts);
                    }
                    let Some(next_cursor) = receipt_page.next_cursor else {
                        break;
                    };
                    receipt_cursor = Some(next_cursor);
                }
            }
            let Some(next_cursor) = version_page.next_cursor else {
                break;
            };
            version_cursor = Some(next_cursor);
        }

        let mut conflict_cursor = None;
        loop {
            let conflict_page = self.conflicts_for_object_page(
                &object.object_id,
                conflict_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.open_conflict_count = summary.open_conflict_count.saturating_add(
                conflict_page
                    .conflicts
                    .iter()
                    .filter(|conflict| conflict.status == DfsConflictStatus::Open)
                    .count(),
            );
            let Some(next_cursor) = conflict_page.next_cursor else {
                break;
            };
            conflict_cursor = Some(next_cursor);
        }

        summary.pending_repair_count =
            summary
                .pending_repair_count
                .saturating_add(count_pending_repairs_for_object(
                    object,
                    &object_intents,
                    &receipts_by_version,
                ));
        Ok(())
    }

    fn accumulate_metadata_summary_for_file_system(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_id: &DfsFileSystemId,
    ) -> Result<()> {
        let mut object_cursor = None;
        loop {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                self.accumulate_metadata_summary_for_object(summary, &object)?;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            object_cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn accumulate_namespace_job_summary(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_scope: Option<&DfsFileSystemId>,
    ) -> Result<()> {
        let mut cursor = None;
        loop {
            let page =
                self.list_namespace_jobs_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            for job in page.jobs {
                if file_system_scope
                    .is_some_and(|file_system_id| job.file_system_id != *file_system_id)
                {
                    continue;
                }
                match job.status {
                    DfsNamespaceJobStatus::Running => {
                        summary.pending_namespace_job_count =
                            summary.pending_namespace_job_count.saturating_add(1);
                    }
                    DfsNamespaceJobStatus::Failed => {
                        summary.failed_namespace_job_count =
                            summary.failed_namespace_job_count.saturating_add(1);
                    }
                    DfsNamespaceJobStatus::Completed => {}
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let filesystems = self.list_file_systems()?;
        let mut summary = DfsMetadataSummary {
            file_system_count: filesystems.len(),
            ..DfsMetadataSummary::default()
        };
        for file_system in &filesystems {
            self.accumulate_metadata_summary_for_file_system(
                &mut summary,
                &file_system.file_system_id,
            )?;
        }
        self.accumulate_namespace_job_summary(&mut summary, None)?;
        Ok(summary)
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let Some(_) = self.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let mut summary = DfsMetadataSummary {
            file_system_count: 1,
            ..DfsMetadataSummary::default()
        };
        self.accumulate_metadata_summary_for_file_system(&mut summary, file_system_id)?;
        self.accumulate_namespace_job_summary(&mut summary, Some(file_system_id))?;
        Ok(Some(summary))
    }
    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry>;
    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry>;
    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        let _ = transaction_id;
        Ok(None)
    }
    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let _ = options;
        Ok(DfsTombstoneCompactionReport::default())
    }
    /// Records that an object moved to a metadata shard owned by another
    /// node: journals a same-object-id tombstone in the shard that held the
    /// record (so that shard's replicas observe the departure) and repoints
    /// the local object→shard index at the destination shard. Single-shard
    /// backends never split authority, so the default is a no-op.
    fn evict_object_record_for_foreign_shard_move(
        &mut self,
        moved_object: &DfsObjectMetadata,
    ) -> Result<()> {
        let _ = moved_object;
        Ok(())
    }
    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let _ = job;
        Ok(())
    }
    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let _ = job_id;
        Ok(None)
    }
    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        let mut jobs = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                self.list_namespace_jobs_page(cursor.as_ref(), OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            jobs.extend(page.jobs);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(jobs)
    }
    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        let _ = after;
        let _ = max_entries;
        Ok(DfsNamespaceJobPage {
            jobs: Vec::new(),
            next_cursor: None,
        })
    }
    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry>;
    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>>;
    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let mut conflicts = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.conflicts_for_object_page(
                object_id,
                cursor.as_ref(),
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            conflicts.extend(page.conflicts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(conflicts)
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage>;
    fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>>;
    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>>;
    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>>;
    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(self
            .metadata_shard_statuses()?
            .into_iter()
            .map(|status| status.journal_last_sequence)
            .max()
            .unwrap_or(0))
    }
    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot>;
    /// Snapshot of one shard's tables, for replica snapshot repair. Backends
    /// that cannot scope an export to a single shard return `None` and the
    /// repair is skipped.
    fn export_shard_table_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataTableSnapshot>> {
        let _ = shard_id;
        Ok(None)
    }
    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = self.journal_since(after_sequence)?;
        entries.truncate(max_entries);
        Ok(entries)
    }
    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        let entries = self.journal_since_limited(after_sequence, max_entries)?;
        bounded_metadata_journal_batch(
            DfsMetadataShardId::new("default"),
            after_sequence,
            entries,
            max_entries,
            max_encoded_bytes,
        )
    }
    fn journal_batch_for_shard_since_limited(
        &self,
        _shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        self.journal_batch_since_limited(after_sequence, max_entries, max_encoded_bytes)
    }
    fn metadata_replication_cursor(
        &self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        Ok(0)
    }
    fn put_metadata_replication_cursor(
        &mut self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
        _next_after_sequence: u64,
    ) -> Result<()> {
        Ok(())
    }
    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>>;
    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize>;
    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool>;
    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription;
    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch>;
}
