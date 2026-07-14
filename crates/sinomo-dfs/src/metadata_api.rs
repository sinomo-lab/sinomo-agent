//! Metadata backend API surface.
//!
//! This module owns the public metadata backend contract and default scan
//! helpers. Concrete local storage lives in `metadata_backend`.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_core::node::NodeId;

use crate::advisory_lock::{
    DfsAdvisoryLockApplyResult, DfsAdvisoryLockCommand, DfsAdvisoryLockStateRecord,
    DfsAdvisoryLockTransitionContext,
};
use crate::intent::write_intent_status_is_active;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_hardlink::{object_is_hardlink_to_target, sort_hardlinks_by_path};
use crate::metadata_journal::bounded_metadata_journal_batch;
use crate::metadata_keys::{normalize_posix_path, write_intent_page_cursor};
use crate::metadata_limits::OBJECT_LOCAL_METADATA_PAGE_SIZE;
use crate::metadata_pages::{
    collect_filtered_cursor_page, collect_filtered_metadata_pages, collect_metadata_pages,
    walk_metadata_pages,
};
use crate::metadata_records::*;
use crate::metadata_summary::count_pending_repairs_for_object;
use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};

/// Durable shard-local freeze installed while a same-primary membership
/// handoff carries the retiring authority's exact state.
///
/// This bounded marker is local coordination state, not file metadata. A
/// pending marker rejects every authoritative mutation. After activation, only
/// the backend instance explicitly rebound to the marker's exact active epoch
/// may mutate; independently opened stale instances remain fenced. Read,
/// snapshot, prefix-proof, and replication-cursor operations remain usable.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardWriteFreeze {
    pub handoff_id: String,
    pub retiring_epoch: u64,
    /// Active authority generation installed after the handoff commits.
    ///
    /// `None` is a pending write freeze. `Some(epoch)` remains durable after
    /// activation so already-open backend instances bound to the retiring
    /// generation cannot resume writes merely because another process
    /// completed the handoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activated_epoch: Option<u64>,
    pub journal_sequence: u64,
    pub state_fingerprint: String,
}

/// Durable DFS metadata store interface.
///
/// Implementations own authoritative filesystem/object metadata, object
/// versions, write intents, replica receipts, conflict records, namespace
/// transactions, and metadata journals. The trait intentionally exposes paged
/// scans as primitives so service and operator code can walk large namespaces
/// without materializing every record class at once.
pub trait DfsMetadataBackend: std::fmt::Debug + Send {
    /// Create or replace a filesystem record and journal the mutation.
    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry>;

    /// Fetch a filesystem record by id.
    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>>;

    /// Ensure a filesystem record exists, returning a journal entry only when
    /// storage changed.
    fn ensure_file_system(
        &mut self,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        if self.get_file_system(&file_system.file_system_id)?.as_ref() == Some(file_system) {
            return Ok(None);
        }
        self.put_file_system(file_system).map(Some)
    }

    /// Ensure a filesystem record exists on one exact metadata shard.
    ///
    /// Single-shard backends delegate to their ordinary ensure. Sharded
    /// routers override this so a primary never authors the same filesystem
    /// record on sibling shards for which it is only a replica.
    fn ensure_file_system_on_shard(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        self.ensure_file_system(file_system)
    }

    /// List all filesystem records known to this backend.
    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>>;

    /// Create or replace an object metadata record and journal the mutation.
    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry>;

    /// Add a replica location only while the durable object still names the
    /// expected visible version and content. Implementations must fence the
    /// check and update as one metadata write so a concurrent rename or unlink
    /// cannot be followed by a stale location update that resurrects the old
    /// path. Returns the updated object, or `None` when the durable object has
    /// moved, advanced, disappeared, or become a tombstone.
    fn add_object_location_if_current(
        &mut self,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        expected_content_hash: &str,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Store several object records using the backend's default single-record
    /// path unless the implementation overrides it with an atomic batch.
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

    /// Fetch an object record by object id.
    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>>;

    /// Fetch an object record from one exact metadata shard.
    ///
    /// Namespace prepares use this point read after selecting their immutable
    /// request-scoped shard. A sharded router must not resolve the object
    /// through its global object index here: during a cross-shard adoption the
    /// same object id is intentionally still visible on the source shard, but
    /// destination preconditions describe only destination-shard state.
    fn get_object_on_shard(
        &self,
        _shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.get_object(object_id)
    }

    /// Fetch the durable whole-file advisory-lock aggregate for an object.
    fn get_advisory_lock_state(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsAdvisoryLockStateRecord>>;

    /// Apply one advisory-lock command as a single journaled state transition.
    ///
    /// Synchronous distributed callers must replicate the result's shard
    /// through `durable_through_sequence` before acknowledging success.
    fn apply_advisory_lock_command(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        context: DfsAdvisoryLockTransitionContext,
        command: &DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockApplyResult>;

    /// Install an exact aggregate as part of a serialized object-shard move.
    ///
    /// This is not a lock-operation API. The destination must install the
    /// object first, reject a conflicting aggregate, and epoch-fence/warm the
    /// copied state before issuing any new grant.
    fn install_advisory_lock_state_for_migration(
        &mut self,
        state: &DfsAdvisoryLockStateRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>>;

    /// Fetch the visible object at a normalized filesystem path.
    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Fetch the visible object at a path from one exact metadata shard.
    ///
    /// Single-shard backends delegate to their normal lookup. Sharded routers
    /// override this so prepare expectations cannot accidentally span shard
    /// authorities while a namespace saga is moving an object.
    fn get_object_by_path_on_shard(
        &self,
        _shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.get_object_by_path(file_system_id, path)
    }

    /// Compatibility helper that drains all object pages for a filesystem.
    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of objects for a filesystem.
    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage>;

    /// List durable direct-child tombstones below a directory. Backends with a
    /// path index should override this so targeted namespace synchronization
    /// does not scan the entire filesystem.
    fn child_tombstones_for_directory(
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
        collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                let rest = object_path.strip_prefix(&prefix)?;
                (object.kind == DfsObjectKind::Tombstone && !rest.is_empty() && !rest.contains('/'))
                    .then_some(object)
            },
        )
    }

    /// Count visible objects in a filesystem without requiring callers to
    /// materialize every record.
    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        const COUNT_PAGE_SIZE: usize = 4096;
        let mut count = 0usize;
        walk_metadata_pages(|cursor| {
            let page =
                self.objects_for_file_system_page(file_system_id, cursor, COUNT_PAGE_SIZE)?;
            count = count.saturating_add(page.objects.len());
            Ok(page.next_cursor)
        })?;
        Ok(count)
    }

    /// Build the page cursor that resumes after `object`.
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

    /// Compatibility helper that lists direct children below a directory path.
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
        let mut children = collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    return None;
                }
                let rest = object_path.trim_start_matches(&prefix);
                (!rest.is_empty() && !rest.contains('/')).then_some(object)
            },
        )?;
        children.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(children)
    }

    /// Return one cursor-bounded page of direct directory children.
    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let object_cursor = after.map(|cursor| DfsObjectPageCursor {
            path: cursor.path.clone(),
            object_id: cursor.object_id.clone(),
            shard_id: cursor.shard_id.clone(),
        });
        let (objects, next_cursor) = collect_filtered_cursor_page(
            object_cursor,
            max_entries,
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    return Ok(None);
                }
                let rest = object_path.trim_start_matches(&prefix);
                if rest.is_empty() || rest.contains('/') {
                    return Ok(None);
                }
                let object_cursor = self.object_page_cursor_for_object(&object)?;
                let cursor = DfsDirectoryPageCursor {
                    path: object_cursor.path,
                    object_id: object_cursor.object_id,
                    dentry: None,
                    shard_id: object_cursor.shard_id,
                };
                Ok(Some((object, cursor)))
            },
        )?;
        Ok(DfsDirectoryPage {
            objects,
            next_cursor,
        })
    }

    /// Compatibility helper that lists all hardlink records targeting an object.
    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                object_is_hardlink_to_target(&object, file_system_id, target_object_id)
                    .then_some(object)
            },
        )?;
        sort_hardlinks_by_path(&mut hardlinks);
        Ok(hardlinks)
    }

    /// Return one cursor-bounded page of hardlink records for a target object.
    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let (hardlinks, next_cursor) = collect_filtered_cursor_page(
            after.cloned(),
            max_entries,
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                if !object_is_hardlink_to_target(&object, file_system_id, target_object_id) {
                    return Ok(None);
                }
                let cursor = self.object_page_cursor_for_object(&object)?;
                Ok(Some((object, cursor)))
            },
        )?;
        Ok(DfsObjectPage {
            objects: hardlinks,
            next_cursor,
        })
    }

    /// Count hardlink records targeting an object.
    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let mut count = 0usize;
        walk_metadata_pages(|cursor| {
            let page =
                self.hardlinks_to_object_page(file_system_id, target_object_id, cursor, 1024)?;
            count = count.saturating_add(page.objects.len());
            Ok(page.next_cursor)
        })?;
        Ok(count)
    }

    /// Return the first hardlink record targeting an object, if one exists.
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

    /// Return the first hardlink target outside `prefix`, used by namespace
    /// cleanup to avoid removing still-referenced content.
    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        let mut found = None;
        walk_metadata_pages(|cursor| {
            let page =
                self.hardlinks_to_object_page(file_system_id, target_object_id, cursor, 1024)?;
            for object in page.objects {
                if !posix_path_is_at_or_under_prefix(&object.path, &prefix) {
                    found = Some(object);
                    return Ok(None);
                }
            }
            Ok(page.next_cursor)
        })?;
        Ok(found)
    }

    /// Create or replace an object-version record and journal the mutation.
    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry>;

    /// Store an object record and its version. Implementations may override
    /// this to persist both records in one durable metadata batch.
    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object_entry = self.put_object(object)?;
        let version_entry = self.put_version(version)?;
        Ok(vec![object_entry, version_entry])
    }

    /// Atomically commit an existing object's next visible content version
    /// together with its write intent and durable receipts.
    ///
    /// Implementations must validate the intent base version and receipt
    /// threshold while holding the same metadata write lock used to publish the
    /// visible-version transition. New-object publication remains a namespace
    /// transaction and must not use this method.
    fn put_object_version_intent_and_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>>;

    /// Atomically publish a new object and its parent-directory touch together
    /// with the write intent and durable receipts that authorize the first
    /// visible version.
    ///
    /// The namespace transaction must contain the child and its first visible
    /// version. Implementations must preserve replay-prefix safety: the child
    /// remains invisible before the transaction entry, and any visible prefix
    /// already contains the commit-ready intent and receipt threshold.
    fn commit_create_intent_receipts_and_namespace(
        &mut self,
        pending_intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
        transaction: &DfsNamespaceTransaction,
        committed_intent: &DfsWriteIntentRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>>;

    /// Fetch one object-version record by object id and version id.
    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>>;

    /// Fetch object/version pairs in caller-provided order.
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

    /// Compatibility helper that drains all versions for one object.
    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.versions_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of versions for one object.
    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage>;

    /// Create or replace a replica receipt and journal the mutation.
    fn put_receipt(&mut self, receipt: &DfsReplicaReceiptRecord)
    -> Result<DfsMetadataJournalEntry>;

    /// Remove a replica receipt and journal the mutation when one existed.
    fn remove_receipt(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsMetadataJournalEntry>>;

    /// Store a receipt with optional object context. Sharded backends use the
    /// object to avoid an extra lookup when routing the receipt.
    fn put_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = object;
        self.put_receipt(receipt)
    }

    /// Compatibility helper that drains all receipts for one object version.
    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of receipts for one object version.
    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage>;

    /// Create or replace a write-intent record and journal the mutation.
    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry>;

    /// Store a write intent with optional object context for sharded routing.
    fn put_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _ = object;
        self.put_intent(intent)
    }

    /// Store a write intent and its receipts. Implementations may override
    /// this to persist them in one durable metadata batch.
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

    /// Fetch a write intent by object id and intent id.
    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>>;

    /// Compatibility helper that drains all intents for one object.
    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.intents_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of intents for one object.
    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage>;

    /// Return one cursor-bounded page of intents for one object and target
    /// version.
    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let (intents, next_cursor) = collect_filtered_cursor_page(
            after.cloned(),
            max_entries,
            |cursor| {
                let page = self.intents_for_object_page(
                    object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.intents, page.next_cursor))
            },
            |intent| {
                if intent.target_version != *target_version {
                    return Ok(None);
                }
                let cursor = write_intent_page_cursor(&intent);
                Ok(Some((intent, cursor)))
            },
        )?;
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor,
        })
    }

    /// Return one cursor-bounded global page of write intents.
    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage>;

    /// Return one cursor-bounded global page of object-version records.
    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage>;

    /// Return one cursor-bounded global page of replica receipts.
    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage>;

    /// Return one cursor-bounded global page of conflict records.
    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage>;
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_versions(&self) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.version_records_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_receipts(&self) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipt_records_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.receipts, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.write_intents_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_conflicts(&self) -> Result<Vec<DfsConflictRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.conflict_records_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.conflicts, page.next_cursor))
        })
    }

    /// Add one object's versions, receipts, intents, conflicts, and repair
    /// count into an in-progress summary.
    fn accumulate_metadata_summary_for_object(
        &self,
        summary: &mut DfsMetadataSummary,
        object: &DfsObjectMetadata,
    ) -> Result<()> {
        summary.object_count = summary.object_count.saturating_add(1);

        let mut object_intents = Vec::new();
        walk_metadata_pages(|intent_cursor| {
            let intent_page = self.intents_for_object_page(
                &object.object_id,
                intent_cursor,
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
            Ok(intent_page.next_cursor)
        })?;

        let committed_targets = object_intents
            .iter()
            .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
            .map(|intent| intent.target_version.clone())
            .collect::<BTreeSet<_>>();
        let mut receipts_by_version = BTreeMap::<DfsVersion, Vec<DfsReplicaReceiptRecord>>::new();
        walk_metadata_pages(|version_cursor| {
            let version_page = self.versions_for_object_page(
                &object.object_id,
                version_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.version_count = summary
                .version_count
                .saturating_add(version_page.versions.len());
            for version in version_page.versions {
                let collect_for_repair = committed_targets.contains(&version.version);
                walk_metadata_pages(|receipt_cursor| {
                    let receipt_page = self.receipts_for_version_page(
                        &object.object_id,
                        &version.version,
                        receipt_cursor,
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
                    Ok(receipt_page.next_cursor)
                })?;
            }
            Ok(version_page.next_cursor)
        })?;

        walk_metadata_pages(|conflict_cursor| {
            let conflict_page = self.conflicts_for_object_page(
                &object.object_id,
                conflict_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.open_conflict_count = summary.open_conflict_count.saturating_add(
                conflict_page
                    .conflicts
                    .iter()
                    .filter(|conflict| conflict.status == DfsConflictStatus::Open)
                    .count(),
            );
            Ok(conflict_page.next_cursor)
        })?;

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

    /// Add all object-level counts for one filesystem into an in-progress
    /// summary.
    fn accumulate_metadata_summary_for_file_system(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_id: &DfsFileSystemId,
    ) -> Result<()> {
        walk_metadata_pages(|object_cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                self.accumulate_metadata_summary_for_object(summary, &object)?;
            }
            Ok(page.next_cursor)
        })?;
        Ok(())
    }

    /// Add namespace-job counts into an in-progress summary, optionally scoped
    /// to one filesystem.
    fn accumulate_namespace_job_summary(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_scope: Option<&DfsFileSystemId>,
    ) -> Result<()> {
        walk_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
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
            Ok(page.next_cursor)
        })?;
        Ok(())
    }

    /// Compute a metadata summary across every filesystem visible to the
    /// backend.
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

    /// Compute a metadata summary for one filesystem, returning `None` when
    /// the filesystem is unknown.
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

    /// Publish a version as the visible version of an object after validating
    /// expected-version and receipt requirements.
    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry>;

    /// Atomically apply a namespace transaction and journal the mutation.
    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Durably reserve one shard-local namespace transaction without making
    /// any of its object or path changes visible.
    ///
    /// Implementations must validate all object-version and path-occupant
    /// expectations, reject overlap with another active prepare, and persist
    /// only the hidden prepare record while holding the metadata write lock.
    fn prepare_namespace_transaction(
        &mut self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Move an exact durable namespace prepare to a newer shard-owner epoch.
    ///
    /// Implementations must compare `expected_operation_digest` and
    /// `expected_owner_epoch` while holding the metadata write lock, require
    /// the current and rebound records to remain `Prepared`, and preserve the
    /// operation id, filesystem/shard, expectations, and transaction exactly.
    /// Only the owner epoch and its derived operation digest may change.
    fn rebind_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
        rebound_prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Revalidate and atomically publish an existing hidden namespace prepare.
    ///
    /// The digest and owner epoch fence operation-id reuse. Implementations
    /// must apply the prepared namespace transaction and mark the record
    /// committed in one durable metadata table batch.
    fn commit_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Fetch a durable hidden namespace prepare by operation id.
    fn get_namespace_prepare(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let _ = operation_id;
        Ok(None)
    }

    /// Move an exact non-terminal write intent to a newer shard-owner epoch.
    ///
    /// The compare and replacement must be atomic. Implementations must reject
    /// any payload/status change and may change only `owner_epoch` plus the
    /// bookkeeping `updated_at` timestamp.
    fn rebind_prepared_write_intent(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        expected_intent: &DfsWriteIntentRecord,
        rebound_intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry>;

    /// Fetch a recorded namespace transaction by id when the backend supports
    /// transaction recovery.
    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        let _ = transaction_id;
        Ok(None)
    }

    /// Compact eligible tombstone records according to backend policy.
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
        source_path: &str,
    ) -> Result<()> {
        let _ = (moved_object, source_path);
        Ok(())
    }

    /// Create or replace a namespace maintenance job.
    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let _ = job;
        Ok(())
    }

    /// Fetch a namespace maintenance job by id.
    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let _ = job_id;
        Ok(None)
    }

    /// Compatibility helper that drains all namespace jobs.
    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.jobs, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of namespace jobs.
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

    /// Create or replace a conflict record and journal the mutation.
    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry>;

    /// Fetch one conflict record by object id and conflict id.
    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>>;

    /// Compatibility helper that drains all conflicts for one object.
    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.conflicts_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.conflicts, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of conflicts for one object.
    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage>;

    /// Remove one conflict record and journal the mutation when one existed.
    fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>>;

    /// Update one conflict status and journal the mutation when the record
    /// changed.
    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>>;

    /// Read all journal entries after `after_sequence`.
    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>>;

    /// Highest journal sequence known across this backend.
    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(self
            .metadata_shard_statuses()?
            .into_iter()
            .map(|status| status.journal_last_sequence)
            .max()
            .unwrap_or(0))
    }

    /// Export a table snapshot for replication or repair.
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

    /// Return the O(1), durable identity of one shard's exact materialized
    /// journal prefix. Implementations must return `None` instead of deriving
    /// a partial or sequence-only identity.
    fn metadata_prefix_proof(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        Ok(None)
    }

    /// Return the durable membership-handoff write freeze for one shard.
    fn metadata_shard_write_freeze(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        Ok(None)
    }

    /// Atomically freeze authoritative mutations and capture the exact current
    /// shard state under the backend's cross-process write lock.
    fn freeze_metadata_shard_for_membership_handoff(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _retiring_epoch: u64,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        Ok(None)
    }

    /// Advance the exact matching freeze to a durable active generation after
    /// the desired assignment is active. The calling backend instance is
    /// rebound to `activated_epoch`; independently opened stale instances stay
    /// fenced until they separately prove and observe that generation.
    fn clear_metadata_shard_membership_handoff_freeze(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _activated_epoch: u64,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Read at most `max_entries` journal entries after `after_sequence`.
    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = self.journal_since(after_sequence)?;
        entries.truncate(max_entries);
        Ok(entries)
    }

    /// Build a bounded journal batch after `after_sequence`.
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

    /// Build a bounded journal batch for one shard. Single-shard backends may
    /// delegate to `journal_batch_since_limited`.
    fn journal_batch_for_shard_since_limited(
        &self,
        _shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        self.journal_batch_since_limited(after_sequence, max_entries, max_encoded_bytes)
    }

    /// Return the next journal sequence that should be sent to a target node
    /// for a shard.
    fn metadata_replication_cursor(
        &self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        Ok(0)
    }

    /// Persist the next journal sequence that should be sent to a target node
    /// for a shard.
    fn put_metadata_replication_cursor(
        &mut self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
        _next_after_sequence: u64,
    ) -> Result<()> {
        Ok(())
    }

    /// Return the replica-local exact-checkpoint floor persisted before a
    /// successful prefix acknowledgement.
    fn metadata_prefix_ack_floor(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixAckFloor>> {
        Ok(None)
    }

    /// Persist a monotonic exact-checkpoint floor before acknowledging the
    /// corresponding prefix probe.
    fn put_metadata_prefix_ack_floor(&mut self, _floor: &DfsMetadataPrefixAckFloor) -> Result<()> {
        Ok(())
    }

    /// Return the replica-local floor persisted before an exact ordinary-write
    /// authority acknowledgement.
    fn metadata_authority_ack_floor(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataAuthorityAckFloor>> {
        Ok(None)
    }

    /// Persist a monotonic ordinary-write authority floor before returning the
    /// corresponding request nonce.
    fn put_metadata_authority_ack_floor(
        &mut self,
        floor: &DfsMetadataAuthorityAckFloor,
    ) -> Result<()> {
        Err(MetadataBackendError::Serde(format!(
            "metadata backend cannot durably fence authority acknowledgement for shard {} sequence {}",
            floor.shard_id.0, floor.journal_sequence
        )))
    }

    /// Return the durable replay cursor for journal entries originating from
    /// `source_shard_id`.
    fn metadata_replay_cursor(&self, source_shard_id: &DfsMetadataShardId) -> Result<u64> {
        Ok(self
            .metadata_shard_statuses()?
            .into_iter()
            .flat_map(|status| status.replay_cursors)
            .find(|cursor| cursor.source_shard_id == *source_shard_id)
            .map(|cursor| cursor.replayed_through_sequence)
            .unwrap_or(0))
    }

    /// Return per-shard metadata journal and replay status.
    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>>;

    /// Persist and materialize a replicated journal batch.
    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize>;

    /// Replace local metadata tables with a replicated snapshot.
    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool>;

    /// Create a subscription cursor for journal polling.
    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription;

    /// Poll journal entries for a subscription.
    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch>;
}
