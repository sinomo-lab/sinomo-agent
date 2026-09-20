//! Cross-shard namespace saga helpers for the POSIX facade.
//!
//! This module owns coordinated SIFS metadata-shard moves: cross-primary
//! rename, hardlink adoption, resumable job driving, and destination-side
//! adoption. Local namespace operations and generic namespace-job storage stay
//! in the parent `posix` module.

use super::{
    POSIX_LIST_DIR_COLLECT_PAGE_SIZE, POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE,
    POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS, POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE, namespace_job_id,
    now_unix_nanos,
};
use crate::metadata_pages::{try_walk_metadata_pages, walk_metadata_pages};
use crate::namespace::{
    initial_version, namespace_transaction_id_for_batch,
    namespace_transaction_id_for_versioned_path, next_version,
};
use crate::posix_model::{DfsPosixError, Result};
use crate::posix_path::{
    normalize_posix_path, normalize_posix_path_with_trailing_directory,
    posix_path_is_at_or_under_prefix, subtree_prefix,
};
use crate::{
    DfsDirectoryPageCursor, DfsError, DfsFileSystemId, DfsMessageTransport,
    DfsMetadataVersionPinRecord, DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobOrigin,
    DfsNamespaceJobRecord, DfsNamespaceJobStatus, DfsNamespaceObjectPrecondition,
    DfsNamespaceOperation, DfsNamespaceTransaction, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsObjectVersionRecord, DfsPosixCoordinationResult, DfsReplicaReceiptRecord,
    DfsWriteIntentReceipt, DfsWriteIntentReceiptRetentionState, DfsWriteIntentState,
    DfsWriteIntentStatus, NetworkedDfsService, object_version_record, sifs_file_system_id,
};
use sinomo_core::node::NodeId;

#[cfg(test)]
type HardlinkAliasAfterCommitTestHook = Box<dyn FnOnce() -> Result<()> + Send>;

#[cfg(test)]
static HARDLINK_ALIAS_AFTER_COMMIT_TEST_HOOK: std::sync::Mutex<
    Option<(String, HardlinkAliasAfterCommitTestHook)>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) struct HardlinkAliasAfterCommitTestHookGuard;

#[cfg(test)]
impl Drop for HardlinkAliasAfterCommitTestHookGuard {
    fn drop(&mut self) {
        HARDLINK_ALIAS_AFTER_COMMIT_TEST_HOOK
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }
}

#[cfg(test)]
pub(crate) fn install_hardlink_alias_after_commit_test_hook(
    source_path: impl Into<String>,
    hook: impl FnOnce() -> Result<()> + Send + 'static,
) -> HardlinkAliasAfterCommitTestHookGuard {
    let previous = HARDLINK_ALIAS_AFTER_COMMIT_TEST_HOOK
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .replace((source_path.into(), Box::new(hook)));
    assert!(
        previous.is_none(),
        "hardlink-alias after-commit test hook is already installed"
    );
    HardlinkAliasAfterCommitTestHookGuard
}

#[cfg(test)]
fn run_hardlink_alias_after_commit_test_hook(source_path: &str) -> Result<()> {
    let hook = {
        let mut slot = HARDLINK_ALIAS_AFTER_COMMIT_TEST_HOOK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if slot
            .as_ref()
            .is_some_and(|(expected_path, _)| expected_path == source_path)
        {
            slot.take().map(|(_, hook)| hook)
        } else {
            None
        }
    };
    match hook {
        Some(hook) => hook(),
        None => Ok(()),
    }
}

pub(crate) fn cross_shard_object_version_record(
    object: &DfsObjectMetadata,
) -> DfsObjectVersionRecord {
    let mut version = object_version_record(object, object.content_size_bytes);
    // Cross-shard adoption can reconstruct this record from a durable job
    // frame after a crash. Bind its timestamp to the frame generation instead
    // of wall-clock retry time so the immutable record is byte-for-byte
    // idempotent on every destination attempt.
    version.created_at =
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(object.changed_at_unix_nanos))
            .expect("u64 nanoseconds since the Unix epoch fit OffsetDateTime");
    version
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn hidden_alias_tombstone_path_on_destination_shard(
        &mut self,
        destination_path: &str,
        alias_object_id: &DfsObjectId,
    ) -> Result<String> {
        let Some(shards) = self.refresh_sifs_metadata_shards()? else {
            return Ok(crate::posix_path::hidden_alias_tombstone_path(
                alias_object_id,
            ));
        };
        let destination = self
            .select_sifs_metadata_shard_for_path_from_snapshot(destination_path, &shards)?
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: format!(
                        "destination {destination_path} has no metadata shard for hardlink promotion"
                    ),
                })
            })?;
        for nonce in 0..64u8 {
            let candidate = format!(
                "{destination_path}.sinomo-hardlink-tombstone-{}-{nonce}",
                alias_object_id.as_str()
            );
            if self
                .select_sifs_metadata_shard_for_path_from_snapshot(&candidate, &shards)?
                .is_some_and(|shard| shard.shard_id == destination.shard_id)
            {
                return Ok(candidate);
            }
        }
        Err(DfsPosixError::Dfs(DfsError::Metadata {
            message: format!(
                "could not place hidden alias tombstone for {alias_object_id} on destination shard {}",
                destination.shard_id
            ),
        }))
    }

    /// Coordinates a rename whose destination lives in a metadata shard owned
    /// by another primary. The move is a durable two-step saga: the
    /// destination primary atomically adopts the object record (with its
    /// version and receipt history) at the new path, then this node — the
    /// source-shard authority — atomically tombstones the old path and evicts
    /// the moved record from the source shard. A persisted namespace job and
    /// an in-flight path lock make the window crash-recoverable and serialize
    /// concurrent mutations; after the adoption commits, recovery is forward
    /// only, so the move either fully applies or cleanly rejects.
    pub(crate) fn coordinate_cross_shard_rename(
        &mut self,
        from: &str,
        to: &str,
        destination_primary: NodeId,
    ) -> Result<()> {
        let (from, from_requires_directory) = normalize_posix_path_with_trailing_directory(from)?;
        let (to, to_requires_directory) = normalize_posix_path_with_trailing_directory(to)?;
        let from = self.resolve_posix_path_distributed(&from, false)?;
        if from_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&from)?;
        }
        let to = self.resolve_posix_path_distributed(&to, false)?;
        if from == "/" || to == "/" {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{from} -> {to}"),
            });
        }
        if from == to {
            return Ok(());
        }
        let source = self
            .lookup_sifs_path(&from)?
            .ok_or_else(|| DfsPosixError::NotFound { path: from.clone() })?;
        if source.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: from });
        }
        if source.kind == DfsObjectKind::Directory {
            // Directory renames migrate the whole subtree between shard
            // authorities through a paged, resumable saga.
            let child_prefix = subtree_prefix(&from);
            if to.starts_with(&child_prefix) {
                return Err(DfsPosixError::InvalidPath {
                    path: format!("{from} -> {to}"),
                });
            }
            let job_id = namespace_job_id(
                "rename-dir-cross-shard",
                &source.file_system_id,
                &format!("{from}->{to}"),
                &source.object_id,
                &source.version,
            );
            if let Some(job) = self.namespace_job(&job_id)? {
                return self.drive_cross_shard_namespace_job(job);
            }
            let file_system_id = sifs_file_system_id();
            self.ensure_path_not_in_cross_shard_flight(&file_system_id, &from)?;
            self.ensure_path_not_in_cross_shard_flight(&file_system_id, &to)?;
            return self.coordinate_cross_shard_directory_rename(&from, &to, source);
        }
        if to_requires_directory {
            return Err(DfsPosixError::NotDirectory { path: to });
        }
        let job_id = namespace_job_id(
            "rename-cross-shard",
            &source.file_system_id,
            &format!("{from}->{to}"),
            &source.object_id,
            &source.version,
        );
        if let Some(job) = self.namespace_job(&job_id)? {
            return self.drive_cross_shard_namespace_job(job);
        }
        let file_system_id = sifs_file_system_id();
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &from)?;
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &to)?;
        // Reject up front while writes are still settling so the caller sees
        // a clean retriable error with no job created.
        self.ensure_no_open_write_state_for_object(&source, &from)?;

        let mut moved = source.clone();
        moved.path = to.clone();
        moved.changed_at_unix_nanos =
            now_unix_nanos().max(source.changed_at_unix_nanos.saturating_add(1));
        moved.version = next_version(&source.version);
        let job = match self.namespace_job(&job_id)? {
            Some(job) => job,
            None => {
                let now = time::OffsetDateTime::now_utc();
                DfsNamespaceJobRecord {
                    job_id,
                    file_system_id: source.file_system_id.clone(),
                    origin: self
                        .namespace_execution
                        .request()
                        .map(DfsNamespaceJobOrigin::from_request),
                    kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                    status: DfsNamespaceJobStatus::Running,
                    root_path: from.clone(),
                    target_path: Some(to.clone()),
                    pending_frames: vec![
                        DfsNamespaceJobFrame::CrossShardComplete {
                            moved_object: moved.clone(),
                            old_path: from.clone(),
                        },
                        DfsNamespaceJobFrame::CrossShardAdopt {
                            moved_object: moved,
                            old_path: from.clone(),
                            destination_primary_node_id: destination_primary,
                        },
                    ],
                    processed_object_count: 0,
                    created_at: now,
                    updated_at: now,
                    completed_at: None,
                    failure_reason: None,
                }
            }
        };
        self.persist_namespace_job(&job)?;
        self.drive_cross_shard_namespace_job(job)
    }

    /// Coordinates a directory rename whose destination lives in a metadata
    /// shard owned by another primary: the root record moves first (the
    /// destination primary validates and adopts it atomically, like a local
    /// rename over an empty destination), then the subtree is walked in
    /// paged, resumable frames, adopting every child at its target path and
    /// evicting the source record. The source root path is subtree-locked
    /// for the whole saga, so no write can fork a half-moved object.
    fn coordinate_cross_shard_directory_rename(
        &mut self,
        from: &str,
        to: &str,
        source: DfsObjectMetadata,
    ) -> Result<()> {
        let job_id = namespace_job_id(
            "rename-dir-cross-shard",
            &source.file_system_id,
            &format!("{from}->{to}"),
            &source.object_id,
            &source.version,
        );
        let job = match self.namespace_job(&job_id)? {
            Some(job) => job,
            None => {
                let now = time::OffsetDateTime::now_utc();
                DfsNamespaceJobRecord {
                    job_id,
                    file_system_id: source.file_system_id.clone(),
                    origin: self
                        .namespace_execution
                        .request()
                        .map(DfsNamespaceJobOrigin::from_request),
                    kind: DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
                    status: DfsNamespaceJobStatus::Running,
                    root_path: from.to_string(),
                    target_path: Some(to.to_string()),
                    pending_frames: vec![
                        DfsNamespaceJobFrame::CrossShardDirFinish {
                            source_root: from.to_string(),
                        },
                        DfsNamespaceJobFrame::CrossShardDirScan {
                            source_path: from.to_string(),
                            target_path: to.to_string(),
                            cursor: None,
                        },
                        DfsNamespaceJobFrame::CrossShardDirStart {
                            source_object_id: source.object_id.clone(),
                            source_root: from.to_string(),
                            target_root: to.to_string(),
                        },
                    ],
                    processed_object_count: 0,
                    created_at: now,
                    updated_at: now,
                    completed_at: None,
                    failure_reason: None,
                }
            }
        };
        self.persist_namespace_job(&job)?;
        self.drive_cross_shard_namespace_job(job)
    }

    /// Moves the directory-rename root record across primaries: the
    /// destination primary adopts it, then the old root path is tombstoned
    /// (hiding the not-yet-moved children under it) and the record evicted
    /// from the source shard. Idempotent against resumes.
    fn run_cross_shard_dir_start(
        &mut self,
        job: &DfsNamespaceJobRecord,
        source_object_id: &DfsObjectId,
        source_root: &str,
        target_root: &str,
    ) -> Result<()> {
        let Some(current) = self.object_by_id(source_object_id)? else {
            return Err(DfsPosixError::NotFound {
                path: source_root.to_string(),
            });
        };
        if current.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: source_root.to_string(),
            });
        }
        if current.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: source_root.to_string(),
            });
        }
        let current_path = normalize_posix_path(&current.path)?;
        let mut moved = current.clone();
        if current_path == target_root {
            // The target became authoritative before source cleanup (for
            // example, a crash immediately after adoption). Reconcile from
            // the physical old-path shard and finish its fenced cleanup.
            self.adopt_moved_object_on_destination_primary(
                &moved,
                source_root,
                None,
                Some(&current),
                Some(job),
            )?;
            return self.complete_cross_shard_move_locally(&moved, source_root, false);
        }
        if current_path != source_root {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{current_path} -> {target_root}"),
            });
        }
        self.ensure_no_open_write_state_for_object(&current, source_root)?;
        moved.path = target_root.to_string();
        moved.changed_at_unix_nanos =
            now_unix_nanos().max(current.changed_at_unix_nanos.saturating_add(1));
        moved.version = next_version(&current.version);
        self.adopt_moved_object_on_destination_primary(
            &moved,
            source_root,
            Some(&current),
            None,
            Some(job),
        )?;
        self.complete_cross_shard_move_locally(&moved, source_root, false)
    }

    /// Pages one directory of a cross-primary subtree move: every child file
    /// is adopted at its target path and evicted from the source shard;
    /// child directories are adopted, evicted, and pushed as their own scan
    /// frames (depth first). Already-moved children are skipped, so resumes
    /// and re-sent adoptions stay idempotent.
    fn run_cross_shard_dir_scan(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        source_path: &str,
        target_path: &str,
        cursor: Option<&DfsDirectoryPageCursor>,
    ) -> Result<()> {
        let source_prefix = subtree_prefix(source_path);
        let target_prefix = subtree_prefix(target_path);
        let page = self.child_objects_for_sifs_dir_page(
            source_path,
            cursor,
            POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE,
        )?;
        let mut child_dirs = Vec::new();
        for object in page.objects {
            let object_path = normalize_posix_path(&object.path)?;
            if !object_path.starts_with(&source_prefix) {
                continue;
            }
            let suffix = object_path.trim_start_matches(&source_prefix);
            let target_child_path = format!("{target_prefix}{suffix}");
            let Some(current) = self.object_by_id(&object.object_id)? else {
                continue;
            };
            if current.kind == DfsObjectKind::Tombstone {
                continue;
            }
            let current_path = normalize_posix_path(&current.path)?;
            if current_path == target_child_path {
                // Already adopted; recover an interrupted pin-aware source
                // cleanup from the explicit old-path shard before applying
                // the legacy idempotent eviction below.
                self.adopt_moved_object_on_destination_primary(
                    &current,
                    &object_path,
                    None,
                    Some(&current),
                    Some(job),
                )?;
                if let Some(backend) = self.metadata_backend.as_mut() {
                    backend.evict_object_record_for_foreign_shard_move(&current, &object_path)?;
                }
                if current.kind == DfsObjectKind::Directory {
                    child_dirs.push((object_path, target_child_path));
                }
                continue;
            }
            if current_path != object_path {
                continue;
            }
            self.ensure_no_open_write_state_for_object(&current, &object_path)?;
            let mut moved = current.clone();
            moved.path = target_child_path.clone();
            moved.changed_at_unix_nanos =
                now_unix_nanos().max(current.changed_at_unix_nanos.saturating_add(1));
            moved.version = next_version(&current.version);
            self.adopt_moved_object_on_destination_primary(
                &moved,
                &object_path,
                Some(&current),
                None,
                Some(job),
            )?;
            if let Some(backend) = self.metadata_backend.as_mut() {
                backend.evict_object_record_for_foreign_shard_move(&moved, &object_path)?;
            }
            job.processed_object_count = job.processed_object_count.saturating_add(1);
            if moved.kind == DfsObjectKind::Directory {
                child_dirs.push((object_path, target_child_path));
            }
        }
        if let Some(next_cursor) = page.next_cursor {
            job.pending_frames
                .push(DfsNamespaceJobFrame::CrossShardDirScan {
                    source_path: source_path.to_string(),
                    target_path: target_path.to_string(),
                    cursor: Some(next_cursor),
                });
        }
        for (child_source_path, child_target_path) in child_dirs.into_iter().rev() {
            job.pending_frames
                .push(DfsNamespaceJobFrame::CrossShardDirScan {
                    source_path: child_source_path,
                    target_path: child_target_path,
                    cursor: None,
                });
        }
        Ok(())
    }

    /// Cross-shard moves must not race in-flight distributed write state: a
    /// write or repair that settles after the record changes shard authority
    /// would journal its metadata into a shard this node no longer owns.
    /// Surfaces `Busy` (retriable) so coordinators reject up front and parked
    /// sagas wait for the write to drain.
    fn ensure_no_open_write_state_for_object(
        &self,
        object: &DfsObjectMetadata,
        path: &str,
    ) -> Result<()> {
        let busy = || {
            Err(DfsPosixError::Dfs(DfsError::Busy {
                path: path.to_string(),
            }))
        };
        for (intent_id, pending) in &self.writes.pending_writes {
            if pending.object_id != object.object_id {
                continue;
            }
            let committed_visible = self.coordinator.intent(intent_id).is_some_and(|intent| {
                matches!(intent.state, DfsWriteIntentState::Committed)
                    && intent.target_version == object.version
            });
            if !committed_visible {
                return busy();
            }
        }
        if self
            .coordinator
            .has_open_intent_for_object(&object.object_id)
        {
            return busy();
        }
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut has_blocking_intent = false;
            walk_metadata_pages(|cursor| {
                let page = backend.intents_for_object_page(
                    &object.object_id,
                    cursor,
                    POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
                )?;
                if page.intents.iter().any(|intent| {
                    matches!(intent.status, DfsWriteIntentStatus::Staging)
                        || (matches!(intent.status, DfsWriteIntentStatus::ReceiptsPending)
                            && intent.target_version != object.version)
                }) {
                    has_blocking_intent = true;
                    return Ok(None);
                }
                Ok(page.next_cursor)
            })?;
            if has_blocking_intent {
                return busy();
            }
        }
        Ok(())
    }

    /// Executes a cross-shard adopt frame against the object's *current*
    /// state: a saga that parked (transport failure) may resume after
    /// pre-existing writes settled, so the snapshot recorded at job creation
    /// cannot be trusted. Idempotent against already-applied adoptions.
    #[cfg(test)]
    pub(crate) fn run_cross_shard_adopt(
        &mut self,
        frame_object: &DfsObjectMetadata,
        old_path: &str,
    ) -> Result<()> {
        self.run_cross_shard_adopt_for_job(frame_object, old_path, None)
    }

    fn run_cross_shard_adopt_for_job(
        &mut self,
        frame_object: &DfsObjectMetadata,
        old_path: &str,
        job: Option<&DfsNamespaceJobRecord>,
    ) -> Result<()> {
        let old_path = normalize_posix_path(old_path)?;
        let target_path = normalize_posix_path(&frame_object.path)?;
        let mut current = self.object_by_id(&frame_object.object_id)?;
        if current
            .as_ref()
            .is_none_or(|current| current.kind == DfsObjectKind::Tombstone)
        {
            // A duplicate saga runner can retain the adoption frame after
            // another runner has already adopted and evicted the source-side
            // object. Refresh the destination authority before classifying
            // that exact replay as a missing source.
            self.sync_remote_namespace_path(&target_path)?;
            current = self.object_by_id(&frame_object.object_id)?;
        }
        let Some(current) = current else {
            return Err(DfsPosixError::NotFound { path: old_path });
        };
        if current.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: old_path });
        }
        let current_path = normalize_posix_path(&current.path)?;
        if current_path == target_path {
            // The target may have become authoritative just before a crash,
            // while the physical source family and pins were still retained.
            // Recover from the explicit old-path shard; the destination-side
            // reconciliation preserves target-only pins.
            return self.adopt_moved_object_on_destination_primary(
                &current,
                &old_path,
                None,
                Some(frame_object),
                job,
            );
        }
        if current_path != old_path {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{current_path} -> {target_path}"),
            });
        }
        self.ensure_no_open_write_state_for_object(&current, &old_path)?;
        let mut moved = current.clone();
        moved.path = target_path;
        moved.changed_at_unix_nanos = frame_object
            .changed_at_unix_nanos
            .max(moved.changed_at_unix_nanos.saturating_add(1));
        // The durable job frame owns the target generation. Re-generating a
        // version here would assign a different ULID on every retry, breaking
        // both destination idempotency and the pin-transfer receipt identity.
        moved.version = frame_object.version.clone();
        self.adopt_moved_object_on_destination_primary(&moved, &old_path, Some(&current), None, job)
    }

    /// Routes the adoption of a moved record to the destination path's
    /// current primary (re-resolved from the control plane on every attempt),
    /// or adopts locally when a shard transfer made this node the authority.
    fn adopt_moved_object_on_destination_primary(
        &mut self,
        moved: &DfsObjectMetadata,
        source_path: &str,
        expected_source: Option<&DfsObjectMetadata>,
        finalized_proof_object: Option<&DfsObjectMetadata>,
        job: Option<&DfsNamespaceJobRecord>,
    ) -> Result<()> {
        let source_family = match self.metadata_backend.as_mut() {
            Some(backend) => {
                match backend.capture_cross_shard_source_family(&moved.object_id, source_path) {
                    Ok(source_family) => Some(source_family),
                    Err(crate::MetadataBackendError::UnknownObject { object_id })
                        if object_id == moved.object_id && expected_source.is_none() =>
                    {
                        None
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            None => None,
        };
        if let (Some(expected_source), Some(source_family)) =
            (expected_source, source_family.as_ref())
            && &source_family.source_object != expected_source
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "source object {} changed while capturing its cross-shard metadata family; retry before destination publication",
                    moved.object_id
                ),
            }));
        }
        if source_family.is_none() && expected_source.is_none() {
            // A stale runner with only a hydrated target has no authority to
            // reconcile an empty pin set: doing so could erase the transfer
            // witness while the real source still owns durable pins. Ask the
            // fresh destination primary for a read-only exact-generation
            // proof instead. Any active transfer, mismatch, or old peer fails
            // without mutating destination provenance.
            return self.verify_cross_shard_adoption_finalized(
                finalized_proof_object.unwrap_or(moved),
                source_path,
                job,
            );
        }
        let mut transfer_identity_object = moved.clone();
        if let Some(source_family) = source_family.as_ref()
            && matches!(
                source_family.source_object.tombstone_provenance,
                Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
            )
        {
            transfer_identity_object.version = source_family.source_object.version.clone();
        }
        let pin_transfer_id =
            crate::protocol::cross_shard_pin_transfer_id(&transfer_identity_object, source_path)
                .map_err(DfsPosixError::from)?;
        if let Some(crate::DfsTombstoneProvenance::SourceEviction {
            pin_transfer_id: finalize_transfer_id,
            ..
        }) = source_family
            .as_ref()
            .and_then(|family| family.source_object.tombstone_provenance.as_ref())
        {
            let Some(finalize_transfer_id) = finalize_transfer_id.as_deref() else {
                // An empty-pin rolling handoff to a legacy peer installed no
                // target provenance and therefore has nothing to finalize.
                return Ok(());
            };
            if finalize_transfer_id != pin_transfer_id {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: format!(
                        "source-eviction witness for {} names pin transfer {}, expected {}",
                        moved.object_id, finalize_transfer_id, pin_transfer_id
                    ),
                }));
            }
            return self.finalize_cross_shard_pin_transfer(
                &transfer_identity_object,
                source_path,
                finalize_transfer_id,
                job,
            );
        }
        let (
            mut versions,
            version_pins,
            receipts,
            intents,
            intent_receipts,
            intent_receipt_retention,
            conflicts,
            advisory_lock_state,
        ) = source_family
            .as_ref()
            .map(|family| {
                (
                    family.versions.clone(),
                    family.version_pins.clone(),
                    family.receipts.clone(),
                    family.intents.clone(),
                    family.intent_receipts.clone(),
                    family.intent_receipt_retention.clone(),
                    family.conflicts.clone(),
                    family.advisory_lock_state.clone(),
                )
            })
            .unwrap_or_else(|| {
                (
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    DfsWriteIntentReceiptRetentionState::default(),
                    Vec::new(),
                    None,
                )
            });
        if versions.iter().any(|version| {
            version.retained.is_some()
                || version.history_order != 0
                || version.content_durability.is_some()
        }) {
            self.require_file_history_shard_support(&moved.file_system_id, &moved.path)
                .map_err(DfsPosixError::from)?;
        }
        let mut moved_version = cross_shard_object_version_record(moved);
        if let Some(source_version) = source_family.as_ref().and_then(|family| {
            family
                .versions
                .iter()
                .find(|version| version.version == family.source_object.version)
        }) && source_version.content_durability.is_some()
        {
            if source_version.file_system_id != moved.file_system_id
                || source_version.object_id != moved.object_id
                || source_version.content_hash != moved.content_hash
                || source_version.size_bytes != moved.content_size_bytes
            {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message:
                        "cross-shard metadata move cannot change the recorded content identity"
                            .into(),
                }));
            }
            moved_version.content_durability = source_version.content_durability.clone();
        }
        moved_version.history_order = versions
            .iter()
            .map(|version| version.history_order)
            .max()
            .unwrap_or(0);
        if let Some(existing) = versions.iter_mut().find(|version| {
            version.object_id == moved_version.object_id && version.version == moved_version.version
        }) {
            if existing.content_durability.is_none() {
                existing.content_durability = moved_version.content_durability;
            }
        } else {
            versions.push(moved_version);
        }
        let version_pin_set_fingerprint =
            crate::protocol::cross_shard_version_pin_set_fingerprint(moved, &version_pins)
                .map_err(DfsPosixError::from)?;
        let pin_transfer_installed = match self.routed_namespace_primary_for_path(&moved.path)? {
            Some(primary) => {
                if let Some(source_state) = advisory_lock_state.as_ref() {
                    let (_, state_fingerprint) =
                        crate::advisory_lock::prepare_advisory_lock_state_for_migration(
                            source_state,
                        )
                        .map_err(|source| {
                            DfsPosixError::Dfs(DfsError::Serialization {
                                message: format!(
                                    "encode advisory-lock migration preparation for {}: {source}",
                                    moved.object_id
                                ),
                            })
                        })?;
                    self.route_cross_shard_job_phase(
                        job,
                        &format!(
                            "prepare-advisory-lock-migration\0{}\0{source_path}",
                            moved.object_id
                        ),
                        primary.clone(),
                        DfsNamespaceOperation::AdvisoryLockMigrationPrepare {
                            path: moved.path.clone(),
                            source_path: source_path.to_string(),
                            object_id: moved.object_id.clone(),
                            state_fingerprint,
                        },
                    )?;
                }
                let result = self.route_cross_shard_job_phase_with_result(
                    job,
                    &format!("adopt-object-family\0{}\0{source_path}", moved.object_id),
                    primary,
                    DfsNamespaceOperation::AdoptObjectAtPath {
                        object: moved.clone(),
                        source_path: source_path.to_string(),
                        pin_transfer_id: Some(pin_transfer_id.clone()),
                        versions,
                        version_pins,
                        version_pin_set_fingerprint: Some(version_pin_set_fingerprint),
                        receipts,
                        intents,
                        intent_receipts,
                        intent_receipt_retention,
                        conflicts,
                        advisory_lock_state,
                    },
                )?;
                match result {
                    Some(DfsPosixCoordinationResult::ObjectFamilyAdoption {
                        pin_transfer_id: acknowledged_transfer_id,
                        pin_transfer_active,
                        ..
                    }) if acknowledged_transfer_id == pin_transfer_id => pin_transfer_active,
                    _ => false,
                }
            }
            None => {
                let pin_transfer_active = self.adopt_object_at_path_local(
                    moved,
                    source_path,
                    Some(&pin_transfer_id),
                    &versions,
                    &version_pins,
                    Some(&version_pin_set_fingerprint),
                    &receipts,
                    &intents,
                    &intent_receipts,
                    &intent_receipt_retention,
                    &conflicts,
                    advisory_lock_state.as_ref(),
                )?;
                let metadata_target = self.capture_metadata_quorum_sync_target(moved)?;
                self.sync_metadata_shard_to_quorum(
                    &metadata_target,
                    crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
                )?;
                pin_transfer_active
            }
        };
        if let (Some(backend), Some(source_family)) =
            (self.metadata_backend.as_mut(), source_family.as_ref())
        {
            backend.evict_object_record_for_foreign_shard_move_with_pin_fence(
                moved,
                source_path,
                &source_family.source_object,
                source_family.source_journal_sequence,
                &source_family.version_pins,
                pin_transfer_installed.then_some(pin_transfer_id.as_str()),
            )?;
        }
        if pin_transfer_installed {
            self.finalize_cross_shard_pin_transfer(
                &transfer_identity_object,
                source_path,
                &pin_transfer_id,
                job,
            )?;
        }
        Ok(())
    }

    fn verify_cross_shard_adoption_finalized(
        &mut self,
        moved: &DfsObjectMetadata,
        source_path: &str,
        job: Option<&DfsNamespaceJobRecord>,
    ) -> Result<()> {
        let pin_transfer_id = crate::protocol::cross_shard_pin_transfer_id(moved, source_path)
            .map_err(DfsPosixError::from)?;
        let operation = DfsNamespaceOperation::VerifyObjectFamilyAdoption {
            object: moved.clone(),
            source_path: source_path.to_string(),
            pin_transfer_id: pin_transfer_id.clone(),
        };
        let result = match self.routed_namespace_primary_for_path(&moved.path)? {
            Some(primary) => self.route_cross_shard_job_phase_with_result(
                job,
                &format!(
                    "verify-object-family-adoption\0{}\0{source_path}",
                    moved.object_id
                ),
                primary,
                operation,
            )?,
            None => self.execute_posix_coordination_operation_local(&operation)?,
        };
        match result {
            Some(DfsPosixCoordinationResult::ObjectFamilyAdoptionVerified {
                object_id,
                target_version,
                pin_transfer_id: acknowledged_transfer_id,
                ..
            }) if object_id == moved.object_id
                && target_version == moved.version
                && acknowledged_transfer_id == pin_transfer_id =>
            {
                Ok(())
            }
            _ => Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: format!(
                    "destination authority did not prove finalized cross-shard adoption of {} at {} version {}",
                    moved.object_id, moved.path, moved.version
                ),
            })),
        }
    }

    fn finalize_cross_shard_pin_transfer(
        &mut self,
        transfer_object: &DfsObjectMetadata,
        source_path: &str,
        pin_transfer_id: &str,
        job: Option<&DfsNamespaceJobRecord>,
    ) -> Result<()> {
        match self.routed_namespace_primary_for_path(&transfer_object.path)? {
            Some(primary) => self.route_cross_shard_job_phase(
                job,
                &format!(
                    "finalize-object-family-adoption\0{}\0{source_path}",
                    transfer_object.object_id
                ),
                primary,
                DfsNamespaceOperation::FinalizeObjectFamilyAdoption {
                    path: transfer_object.path.clone(),
                    source_path: source_path.to_string(),
                    object_id: transfer_object.object_id.clone(),
                    target_version: transfer_object.version.clone(),
                    pin_transfer_id: pin_transfer_id.to_string(),
                },
            ),
            None => {
                self.metadata_backend
                    .as_mut()
                    .ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "pin-transfer finalization for {} requires a durable metadata backend",
                                transfer_object.object_id
                            ),
                        })
                    })?
                    .finalize_cross_shard_pin_transfer(
                        pin_transfer_id,
                        transfer_object,
                        source_path,
                    )?;
                let metadata_target = self.capture_metadata_quorum_sync_target(transfer_object)?;
                self.sync_metadata_shard_to_quorum(
                    &metadata_target,
                    crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
                )
                .map(drop)
                .map_err(DfsPosixError::from)
            }
        }
    }

    /// Coordinates a hardlink whose alias path lives in a metadata shard
    /// owned by another primary: the link-path primary atomically adopts the
    /// alias record, then this node — the target's shard authority — records
    /// the link-count change on the target.
    pub(crate) fn coordinate_cross_shard_hardlink(
        &mut self,
        existing_path: &str,
        link_path: &str,
        destination_primary: NodeId,
    ) -> Result<()> {
        let (existing_path, existing_requires_directory) =
            normalize_posix_path_with_trailing_directory(existing_path)?;
        let existing_path = self.resolve_posix_path_distributed(&existing_path, false)?;
        if existing_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&existing_path)?;
        }
        let (link_path, link_requires_directory) =
            normalize_posix_path_with_trailing_directory(link_path)?;
        let link_path = self.resolve_posix_path_distributed(&link_path, false)?;
        if link_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&link_path)?;
            return Err(DfsPosixError::AlreadyExists { path: link_path });
        }
        if link_path == "/" {
            return Err(DfsPosixError::InvalidPath { path: link_path });
        }
        let existing =
            self.lookup_sifs_path(&existing_path)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: existing_path.clone(),
                })?;
        if existing.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: existing_path,
            });
        }
        let target = self.resolve_hardlink_target(&existing)?;
        match target.kind {
            DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory {
                    path: existing_path,
                });
            }
            DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::NotFound {
                    path: existing_path,
                });
            }
            _ => {}
        }
        // The durable job retains this id across retries and restarts.
        let alias_object_id = DfsObjectId::generate();
        let alias = DfsObjectMetadata {
            object_id: alias_object_id,
            file_system_id: target.file_system_id.clone(),
            path: link_path.clone(),
            kind: DfsObjectKind::Hardlink,
            tombstone_provenance: None,
            owner_node_id: self.node_id.clone(),
            current_locations: target.current_locations.clone(),
            content_hash: target.content_hash.clone(),
            content_size_bytes: target.content_size_bytes,
            link_target_object_id: Some(target.object_id.clone()),
            mode: target.mode,
            uid: target.uid,
            gid: target.gid,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: initial_version(),
            policy: target.policy.clone(),
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let job_id = namespace_job_id(
            "hardlink-cross-shard",
            &target.file_system_id,
            &format!("{existing_path}->{link_path}"),
            &target.object_id,
            &target.version,
        );
        if let Some(job) = self.namespace_job(&job_id)? {
            return self.drive_cross_shard_namespace_job(job);
        }
        let file_system_id = sifs_file_system_id();
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &existing_path)?;
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &link_path)?;
        let job = match self.namespace_job(&job_id)? {
            Some(job) => job,
            None => {
                let now = time::OffsetDateTime::now_utc();
                DfsNamespaceJobRecord {
                    job_id,
                    file_system_id: target.file_system_id.clone(),
                    origin: self
                        .namespace_execution
                        .request()
                        .map(DfsNamespaceJobOrigin::from_request),
                    kind: DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
                    status: DfsNamespaceJobStatus::Running,
                    root_path: existing_path.clone(),
                    target_path: Some(link_path.clone()),
                    pending_frames: vec![
                        DfsNamespaceJobFrame::CrossShardHardlinkComplete {
                            target_object_id: target.object_id.clone(),
                        },
                        DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                            alias,
                            target_object_id: target.object_id.clone(),
                            destination_primary_node_id: destination_primary,
                        },
                    ],
                    processed_object_count: 0,
                    created_at: now,
                    updated_at: now,
                    completed_at: None,
                    failure_reason: None,
                }
            }
        };
        self.persist_namespace_job(&job)?;
        self.drive_cross_shard_namespace_job(job)
    }

    fn cross_shard_job_phase_operation_id(job: &DfsNamespaceJobRecord, phase: &str) -> String {
        let identity = format!("{}\0{}", job.job_id, phase);
        let digest = blake3::hash(identity.as_bytes()).to_hex();
        let order_token = job.origin.as_ref().and_then(|origin| {
            crate::intent::stable_operation_order_token(&origin.operation_id)
                .or_else(|| crate::intent::stable_operation_order_token(&origin.request_id))
        });
        match order_token {
            Some(order_token) => format!("xsj_{}_{}", &digest.as_str()[..24], order_token),
            None => format!("xsj_{}", &digest.as_str()[..32]),
        }
    }

    fn hardlink_alias_job_phase_operation_id(job: &DfsNamespaceJobRecord, phase: &str) -> String {
        let identity = format!("{}\0{}", job.job_id, phase);
        let digest = blake3::hash(identity.as_bytes()).to_hex();
        let order_token = job.origin.as_ref().and_then(|origin| {
            crate::intent::stable_operation_order_token(&origin.operation_id)
                .or_else(|| crate::intent::stable_operation_order_token(&origin.request_id))
        });
        match order_token {
            Some(order_token) => format!("hlj_{}_{}", &digest.as_str()[..24], order_token),
            None => format!("hlj_{}", &digest.as_str()[..32]),
        }
    }

    fn route_cross_shard_job_phase(
        &mut self,
        job: Option<&DfsNamespaceJobRecord>,
        phase: &str,
        primary: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<()> {
        self.route_cross_shard_job_phase_with_result(job, phase, primary, operation)
            .map(drop)
    }

    fn route_cross_shard_job_phase_with_result(
        &mut self,
        job: Option<&DfsNamespaceJobRecord>,
        phase: &str,
        primary: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<Option<DfsPosixCoordinationResult>> {
        match job {
            Some(job) => self.route_namespace_operation_with_request_id_and_result(
                Self::cross_shard_job_phase_operation_id(job, phase),
                primary,
                operation,
            ),
            None => self.route_namespace_operation_with_result(primary, operation),
        }
    }

    /// Coordinate one exact alias-shard transaction with the canonical target
    /// authority. Both alias paths and the canonical path remain hidden behind
    /// durable Busy fences until the two visible generations have committed.
    pub(crate) fn coordinate_cross_primary_hardlink_alias_mutation(
        &mut self,
        alias: &DfsObjectMetadata,
        result_path: &str,
        transaction: DfsNamespaceTransaction,
        reason: &str,
    ) -> Result<()> {
        let source_path = normalize_posix_path(&alias.path)?;
        let result_path = normalize_posix_path(result_path)?;
        let job_id = namespace_job_id(
            "hardlink-alias-mutation-cross-primary",
            &alias.file_system_id,
            &format!("{reason}:{source_path}->{result_path}"),
            &alias.object_id,
            &alias.version,
        );
        let active_request = self.namespace_execution.request().cloned().ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "cross-primary hardlink alias mutation for {source_path} has no durable outer namespace admission"
                ),
            })
        })?;
        // Unlike a single-shard namespace prepare, this job cannot fuse the
        // outer terminal receipt into its first visible transaction: the
        // canonical target generation still has to commit on another
        // authority. Persist Pending admission before either durable fence so
        // a process crash always leaves enough command identity to resume and
        // eventually complete the exact caller operation.
        self.metadata_backend
            .as_mut()
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: format!(
                        "cross-primary hardlink alias mutation for {source_path} requires a durable metadata backend"
                    ),
                })
            })?
            .put_namespace_request_on_shard(
                &active_request.authority_shard_id,
                &active_request,
            )?;
        if let Some(job) = self.namespace_job(&job_id)? {
            return self.drive_cross_shard_namespace_job(job);
        }
        let target_object_id =
            alias
                .link_target_object_id
                .as_ref()
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: source_path.clone(),
                })?;
        let target = self.object_by_id(target_object_id)?.ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "hardlink alias {source_path} cannot locate canonical target {target_object_id}"
                ),
            })
        })?;
        if target.kind == DfsObjectKind::Tombstone
            || target.kind == DfsObjectKind::Hardlink
            || target.object_id != *target_object_id
        {
            return Err(DfsPosixError::Conflict {
                path: source_path,
                reason: format!(
                    "canonical hardlink target generation changed; expected object {target_object_id}"
                ),
            });
        }
        let target_path = normalize_posix_path(&target.path)?;
        let target_primary = self.routed_namespace_primary_for_path(&target_path)?;
        if target_primary.is_none() {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "hardlink alias coordinator for {source_path} was invoked without a foreign canonical authority at {target_path}"
                ),
            }));
        }
        let file_system_id = alias.file_system_id.clone();
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &source_path)?;
        self.ensure_path_not_in_cross_shard_flight(&file_system_id, &result_path)?;

        let expected_target = DfsNamespaceObjectPrecondition {
            object_id: target.object_id.clone(),
            version: target.version.clone(),
        };
        let mut committed_target = target.clone();
        committed_target.changed_at_unix_nanos =
            now_unix_nanos().max(target.changed_at_unix_nanos.saturating_add(1));
        committed_target.version = next_version(&target.version);
        let now = time::OffsetDateTime::now_utc();
        let origin = Some(DfsNamespaceJobOrigin::from_request(&active_request));
        let fence_job_id = namespace_job_id(
            "hardlink-target-fence",
            &target.file_system_id,
            &format!("{job_id}:{source_path}->{target_path}"),
            &target.object_id,
            &target.version,
        );
        let fence = DfsNamespaceJobRecord {
            job_id: fence_job_id.clone(),
            file_system_id: target.file_system_id.clone(),
            origin: origin.clone(),
            kind: DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary,
            status: DfsNamespaceJobStatus::Running,
            root_path: target_path,
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path: source_path.clone(),
                expected_target: expected_target.clone(),
                committed_target: committed_target.clone(),
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        };
        let job = DfsNamespaceJobRecord {
            job_id,
            file_system_id,
            origin,
            kind: DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: source_path.clone(),
            target_path: Some(result_path),
            pending_frames: vec![
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                    fence_job_id,
                    source_path: source_path.clone(),
                    expected_target,
                    committed_target,
                },
                DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { transaction },
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall {
                    fence: Box::new(fence),
                    source_path,
                },
            ],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        };
        self.persist_namespace_job(&job)?;
        self.drive_cross_shard_namespace_job(job)
    }

    fn commit_hardlink_alias_transaction(
        &mut self,
        job: &DfsNamespaceJobRecord,
        transaction: DfsNamespaceTransaction,
    ) -> Result<()> {
        let origin = job.origin.as_ref().ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Metadata {
                message: format!(
                    "cross-primary hardlink alias job {} has no durable caller provenance",
                    job.job_id
                ),
            })
        })?;
        let previous_active_request = self.namespace_execution.take_request();
        if previous_active_request
            .as_ref()
            .is_some_and(|request| !origin.matches_request(request))
        {
            self.namespace_execution
                .restore_request(previous_active_request);
            return Err(DfsPosixError::Dfs(DfsError::Busy {
                path: job.root_path.clone(),
            }));
        }
        // Keep the exact caller operation in the visible journal/feed, but do
        // not attach a Completed request record to this intermediate alias
        // generation. The outer request becomes terminal only after the
        // canonical target frame commits and the complete job returns.
        let outcome = self
            .apply_namespace_transaction_with_feed_provenance_only(transaction, &origin.request_id);
        self.namespace_execution
            .restore_request(previous_active_request);
        outcome
    }

    fn apply_namespace_transaction_with_feed_provenance_only(
        &mut self,
        transaction: DfsNamespaceTransaction,
        operation_id: &str,
    ) -> Result<()> {
        let saved_execution = self.namespace_execution.suspend_request_prepare();
        let outcome =
            self.apply_namespace_transaction_with_journal_operation_id(transaction, operation_id);
        self.namespace_execution
            .restore_request_prepare(saved_execution);
        outcome.map_err(DfsPosixError::from)
    }

    fn install_hardlink_target_fence(
        &mut self,
        source_job: &DfsNamespaceJobRecord,
        fence: &DfsNamespaceJobRecord,
        source_path: &str,
    ) -> Result<()> {
        let operation = DfsNamespaceOperation::InstallHardlinkTargetFence {
            fence: Box::new(fence.clone()),
            source_path: source_path.to_string(),
        };
        match self.routed_namespace_primary_for_path(&fence.root_path)? {
            Some(primary) => self.route_namespace_operation_with_request_id(
                Self::hardlink_alias_job_phase_operation_id(source_job, "install-target-fence"),
                primary,
                operation,
            ),
            None => self.install_hardlink_target_fence_local(fence, source_path),
        }
    }

    fn commit_hardlink_target_fence(
        &mut self,
        source_job: &DfsNamespaceJobRecord,
        fence_job_id: &str,
        source_path: &str,
        expected_target: &DfsNamespaceObjectPrecondition,
        committed_target: &DfsObjectMetadata,
    ) -> Result<()> {
        let operation = DfsNamespaceOperation::CommitHardlinkTargetFence {
            fence_job_id: fence_job_id.to_string(),
            source_path: source_path.to_string(),
            target_path: committed_target.path.clone(),
            expected_target: expected_target.clone(),
            committed_target: committed_target.clone(),
        };
        match self.routed_namespace_primary_for_path(&committed_target.path)? {
            Some(primary) => self.route_namespace_operation_with_request_id(
                Self::hardlink_alias_job_phase_operation_id(source_job, "commit-target-fence"),
                primary,
                operation,
            ),
            None => self.commit_hardlink_target_fence_local(
                fence_job_id,
                source_path,
                &committed_target.path,
                expected_target,
                committed_target,
            ),
        }
    }

    pub(crate) fn install_hardlink_target_fence_local(
        &mut self,
        fence: &DfsNamespaceJobRecord,
        source_path: &str,
    ) -> Result<()> {
        let target_path = normalize_posix_path(&fence.root_path)?;
        if fence.kind != DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
            || fence.status != DfsNamespaceJobStatus::Running
            || fence.root_path != target_path
            || fence.target_path.is_some()
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!("invalid hardlink target fence {}", fence.job_id),
            }));
        }
        let Some(DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
            source_path: reserved_source_path,
            expected_target,
            committed_target,
        }) = fence.pending_frames.as_slice().first()
        else {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "hardlink target fence {} has no exact reservation",
                    fence.job_id
                ),
            }));
        };
        if fence.pending_frames.len() != 1
            || normalize_posix_path(reserved_source_path)? != normalize_posix_path(source_path)?
            || normalize_posix_path(&committed_target.path)? != target_path
            || committed_target.object_id != expected_target.object_id
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "hardlink target fence {} reservation does not match its command",
                    fence.job_id
                ),
            }));
        }
        if let Some(existing) = self.namespace_job(&fence.job_id)? {
            if existing == *fence {
                self.apply_cross_shard_namespace_job_guard(&existing)?;
                return Ok(());
            }
            if existing.status == DfsNamespaceJobStatus::Completed
                && existing.job_id == fence.job_id
                && existing.file_system_id == fence.file_system_id
                && existing.root_path == fence.root_path
                && existing.origin == fence.origin
                && existing.pending_frames == fence.pending_frames
                && existing.created_at == fence.created_at
            {
                let current = self.lookup_sifs_path(&target_path)?;
                if current.as_ref() == Some(committed_target) {
                    return Ok(());
                }
            }
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: format!(
                    "hardlink target fence {} conflicts with an existing durable record",
                    fence.job_id
                ),
            }));
        }
        let current = self.lookup_sifs_path(&target_path)?;
        if current.as_ref() == Some(committed_target) {
            // The target-side terminal job/receipt may have been compacted
            // after the exact pre-minted generation committed. Full metadata
            // equality is an immutable completion proof for the source frame.
            return Ok(());
        }
        if current.as_ref().is_none_or(|target| {
            target.kind == DfsObjectKind::Tombstone
                || target.kind == DfsObjectKind::Hardlink
                || target.object_id != expected_target.object_id
                || target.version != expected_target.version
        }) {
            return Err(DfsPosixError::Conflict {
                path: target_path,
                reason: format!(
                    "canonical hardlink target generation changed before fence installation; expected {}@{}",
                    expected_target.object_id, expected_target.version
                ),
            });
        }
        self.persist_namespace_job(fence)
    }

    pub(crate) fn commit_hardlink_target_fence_local(
        &mut self,
        fence_job_id: &str,
        source_path: &str,
        target_path: &str,
        expected_target: &DfsNamespaceObjectPrecondition,
        committed_target: &DfsObjectMetadata,
    ) -> Result<()> {
        let target_path = normalize_posix_path(target_path)?;
        let fence = self.namespace_job(fence_job_id)?;
        if fence.is_none()
            && self.lookup_sifs_path(&target_path)?.as_ref() == Some(committed_target)
        {
            return Ok(());
        }
        let mut fence = fence.ok_or_else(|| {
            DfsPosixError::Dfs(DfsError::Metadata {
                message: format!("hardlink target fence {fence_job_id} is missing"),
            })
        })?;
        let reservation_matches = matches!(
            fence.pending_frames.as_slice(),
            [DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path: reserved_source_path,
                expected_target: reserved_expected,
                committed_target: reserved_committed,
            }] if normalize_posix_path(reserved_source_path).ok().as_deref()
                    == normalize_posix_path(source_path).ok().as_deref()
                && reserved_expected == expected_target
                && reserved_committed == committed_target
        );
        if fence.kind != DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
            || fence.root_path != target_path
            || !reservation_matches
        {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: format!("hardlink target fence {fence_job_id} does not match its commit"),
            }));
        }
        let current = self.lookup_sifs_path(&target_path)?;
        if current.as_ref() != Some(committed_target) {
            if current.as_ref().is_none_or(|target| {
                target.kind == DfsObjectKind::Tombstone
                    || target.kind == DfsObjectKind::Hardlink
                    || target.object_id != expected_target.object_id
                    || target.version != expected_target.version
            }) {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: format!(
                        "canonical hardlink target changed while fence {fence_job_id} was held"
                    ),
                }));
            }
            let journal_operation_id = fence
                .origin
                .as_ref()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "hardlink target fence {fence_job_id} has no durable caller provenance"
                        ),
                    })
                })?
                .request_id
                .clone();
            self.apply_namespace_transaction_with_feed_provenance_only(
                DfsNamespaceTransaction {
                    file_version_pins: None,
                    transaction_id: namespace_transaction_id_for_batch(
                        "hardlink-target-fence-commit",
                        &committed_target.file_system_id,
                        std::slice::from_ref(committed_target),
                        &[],
                    ),
                    file_system_id: committed_target.file_system_id.clone(),
                    objects: vec![committed_target.clone()],
                    versions: Vec::new(),
                    advisory_lock_states: Vec::new(),
                    created_at: fence.created_at,
                },
                &journal_operation_id,
            )?;
        }
        if fence.status != DfsNamespaceJobStatus::Completed {
            let now = time::OffsetDateTime::now_utc();
            fence.status = DfsNamespaceJobStatus::Completed;
            fence.processed_object_count = 1;
            fence.updated_at = now;
            fence.completed_at = Some(now);
            self.persist_namespace_job(&fence)?;
        }
        Ok(())
    }

    /// Drives a cross-shard saga job to completion: the foreign adoption is
    /// idempotent and may be re-sent after restarts or timeouts; the local
    /// completion is idempotent against current state. Structured rejections
    /// from the destination abort the job before anything mutated locally;
    /// transport failures leave the job running for maintenance to resume.
    /// Runs a cross-shard saga inline to completion, in bounded slices.
    fn drive_cross_shard_namespace_job(&mut self, mut job: DfsNamespaceJobRecord) -> Result<()> {
        loop {
            job = self.run_cross_shard_namespace_job(job, POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS)?;
            match job.status {
                DfsNamespaceJobStatus::Completed => return Ok(()),
                DfsNamespaceJobStatus::Running => continue,
                DfsNamespaceJobStatus::Failed => {
                    let reason = job
                        .failure_reason
                        .unwrap_or_else(|| format!("cross-shard job {} failed", job.job_id));
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: reason,
                    }));
                }
            }
        }
    }

    pub(crate) fn run_cross_shard_namespace_job(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        // The caller may retain an older Running snapshot while the reactive
        // maintenance owner advances the same durable job. Always begin from
        // the authoritative row, and never reinterpret a terminal saga as a
        // fresh source-path mutation after its object was already evicted.
        if let Some(current) = self.namespace_job(&job.job_id)? {
            job = current;
        }
        if job.status != DfsNamespaceJobStatus::Running {
            return Ok(job);
        }
        let file_system_id = job.file_system_id.clone();
        // Snapshot-copied or stale jobs must not run on a node that is no
        // longer the source-side authority.
        let source_shards =
            self.namespace_shards_for_exact_paths(&file_system_id, [job.root_path.as_str()])?;
        if let Some(shard) = source_shards
            .iter()
            .find(|shard| shard.primary_node_id != self.node_id)
        {
            // Replicas retain the running job as a failover reservation, but
            // must never append a local failure or otherwise advance its
            // journal. Only the current source-shard primary may drive the
            // saga; after a certified primary transfer this same copy becomes
            // executable under the new authority epoch.
            return Err(DfsPosixError::Dfs(DfsError::StaleOwnerEpoch {
                file_system_id,
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            }));
        }
        self.apply_cross_shard_namespace_job_guard(&job)?;
        let mut steps = 0usize;
        while let Some(frame) = job.pending_frames.pop() {
            if steps >= max_steps {
                // Budget exhausted: persist progress and yield. The job stays
                // running (and the path locked) for the caller or maintenance
                // to continue.
                job.pending_frames.push(frame);
                job.updated_at = time::OffsetDateTime::now_utc();
                self.persist_namespace_job(&job)?;
                return Ok(job);
            }
            steps = steps.saturating_add(1);
            let hardlink_alias_coordination_frame = matches!(
                &frame,
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall { .. }
                    | DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { .. }
                    | DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit { .. }
            );
            #[cfg(test)]
            let hardlink_alias_commit_frame = matches!(
                &frame,
                DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { .. }
            );
            let outcome = match frame.clone() {
                DfsNamespaceJobFrame::CrossShardAdopt {
                    moved_object,
                    old_path,
                    destination_primary_node_id,
                } => {
                    let _ = destination_primary_node_id;
                    self.run_cross_shard_adopt_for_job(&moved_object, &old_path, Some(&job))
                }
                DfsNamespaceJobFrame::CrossShardComplete {
                    moved_object,
                    old_path,
                } => self.complete_cross_shard_move_locally(&moved_object, &old_path, true),
                DfsNamespaceJobFrame::CrossShardDirStart {
                    source_object_id,
                    source_root,
                    target_root,
                } => self.run_cross_shard_dir_start(
                    &job,
                    &source_object_id,
                    &source_root,
                    &target_root,
                ),
                DfsNamespaceJobFrame::CrossShardDirScan {
                    source_path,
                    target_path,
                    cursor,
                } => self.run_cross_shard_dir_scan(
                    &mut job,
                    &source_path,
                    &target_path,
                    cursor.as_ref(),
                ),
                DfsNamespaceJobFrame::CrossShardDirFinish { source_root } => {
                    self.touch_parent_directory_if_local_primary(&source_root)
                }
                DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                    alias,
                    target_object_id,
                    destination_primary_node_id,
                } => {
                    let _ = (target_object_id, destination_primary_node_id);
                    let source_path = job.root_path.clone();
                    match self.routed_namespace_primary_for_path(&alias.path)? {
                        Some(primary) => self.route_namespace_operation(
                            primary,
                            DfsNamespaceOperation::AdoptHardlinkAlias { alias, source_path },
                        ),
                        None => self.adopt_hardlink_alias_local(&alias),
                    }
                }
                DfsNamespaceJobFrame::CrossShardHardlinkComplete { target_object_id } => {
                    self.complete_cross_shard_hardlink_locally(&target_object_id)
                }
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall {
                    fence,
                    source_path,
                } => self.install_hardlink_target_fence(&job, &fence, &source_path),
                DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { transaction } => {
                    self.commit_hardlink_alias_transaction(&job, transaction)
                }
                DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                    fence_job_id,
                    source_path,
                    expected_target,
                    committed_target,
                } => self.commit_hardlink_target_fence(
                    &job,
                    &fence_job_id,
                    &source_path,
                    &expected_target,
                    &committed_target,
                ),
                DfsNamespaceJobFrame::HardlinkTargetFenceReservation { .. } => {
                    Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "target-side hardlink fence {} cannot be run as a source saga",
                            job.job_id
                        ),
                    }))
                }
                other => Err(DfsPosixError::InvalidPath {
                    path: format!("{}: unexpected frame {other:?}", job.job_id),
                }),
            };
            if let Some(current) = self.namespace_job(&job.job_id)?
                && current.status != DfsNamespaceJobStatus::Running
            {
                return Ok(current);
            }
            match outcome {
                Ok(()) => {
                    job.processed_object_count = job.processed_object_count.saturating_add(1);
                    job.updated_at = time::OffsetDateTime::now_utc();
                    self.persist_namespace_job(&job)?;
                    #[cfg(test)]
                    if hardlink_alias_commit_frame {
                        run_hardlink_alias_after_commit_test_hook(&job.root_path)?;
                    }
                }
                Err(error @ (DfsPosixError::Dfs(_) | DfsPosixError::Metadata(_))) => {
                    // Transport or internal failure: the saga stays running
                    // (and the path stays locked) so maintenance resumes it.
                    job.pending_frames.push(frame);
                    job.updated_at = time::OffsetDateTime::now_utc();
                    self.persist_namespace_job(&job)?;
                    return Err(error);
                }
                Err(error) if hardlink_alias_coordination_frame => {
                    // Once a target fence may exist, failure is forward-only:
                    // retain both durable reservations and let exact retry or
                    // authority recovery settle the same generation.
                    job.pending_frames.push(frame);
                    job.updated_at = time::OffsetDateTime::now_utc();
                    self.persist_namespace_job(&job)?;
                    return Err(error);
                }
                Err(error) => {
                    // Structured POSIX rejection from validation: abort before
                    // any local mutation happened.
                    self.fail_namespace_job(job, error.to_string())?;
                    return Err(error);
                }
            }
        }
        if let Some(current) = self.namespace_job(&job.job_id)?
            && current.status != DfsNamespaceJobStatus::Running
        {
            return Ok(current);
        }
        let now = time::OffsetDateTime::now_utc();
        job.status = DfsNamespaceJobStatus::Completed;
        job.updated_at = now;
        job.completed_at = Some(now);
        self.persist_namespace_job(&job)?;
        Ok(job)
    }

    /// The source-side half of a cross-shard move, applied after the
    /// destination primary adopted the record: tombstone the old path, evict
    /// the moved record from the source shard, and (for top-level moves)
    /// touch the source parent. Idempotent so crash recovery can re-run it.
    fn complete_cross_shard_move_locally(
        &mut self,
        moved_object: &DfsObjectMetadata,
        old_path: &str,
        touch_parent: bool,
    ) -> Result<()> {
        let old_path = normalize_posix_path(old_path)?;
        // A parked saga may resume after the adoption advanced the record
        // beyond the frame snapshot; complete against the adopted state when
        // it is visible.
        let target_path = normalize_posix_path(&moved_object.path)?;
        let refreshed = self
            .lookup_metadata_path(&moved_object.file_system_id, &target_path)?
            .filter(|record| {
                record.object_id == moved_object.object_id
                    && record.kind != DfsObjectKind::Tombstone
            })
            .unwrap_or_else(|| moved_object.clone());
        let moved_object = &refreshed;
        let needs_tombstone = !matches!(
            self.lookup_metadata_path_raw(&moved_object.file_system_id, &old_path)?,
            Some(existing) if existing.kind == DfsObjectKind::Tombstone
        );
        if needs_tombstone {
            let mut old_path_tombstone = moved_object.clone();
            old_path_tombstone.object_id = DfsObjectId::generate();
            let old_path_tombstone =
                self.tombstone_path_object_at(old_path_tombstone, old_path.clone());
            self.apply_namespace_transaction(DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: namespace_transaction_id_for_versioned_path(
                    "rename-cross-shard",
                    &moved_object.object_id,
                    &moved_object.version,
                    &old_path,
                ),
                file_system_id: moved_object.file_system_id.clone(),
                objects: vec![old_path_tombstone],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: time::OffsetDateTime::now_utc(),
            })?;
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.evict_object_record_for_foreign_shard_move(moved_object, &old_path)?;
        }
        self.clear_metadata_lookup_cache();
        self.apply_metadata_object_to_service_state(moved_object.clone())?;
        self.clear_metadata_lookup_cache();
        if touch_parent {
            self.touch_parent_directory_if_local_primary(&old_path)?;
        }
        Ok(())
    }

    /// The target-side half of a cross-shard hardlink: record the link-count
    /// change as a ctime bump on the target object.
    fn complete_cross_shard_hardlink_locally(
        &mut self,
        target_object_id: &DfsObjectId,
    ) -> Result<()> {
        let Some(mut target) = self.object_by_id(target_object_id)? else {
            return Ok(());
        };
        if target.kind == DfsObjectKind::Tombstone {
            return Ok(());
        }
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target)?;
        Ok(())
    }

    /// Atomically installs an object record moved here from another primary's
    /// shard, validating destination preconditions in the same step.
    /// Idempotent against re-sent adoptions.
    pub(crate) fn adopt_object_at_path_local(
        &mut self,
        object: &DfsObjectMetadata,
        source_path: &str,
        pin_transfer_id: Option<&str>,
        versions: &[DfsObjectVersionRecord],
        version_pins: &[DfsMetadataVersionPinRecord],
        version_pin_set_fingerprint: Option<&str>,
        receipts: &[DfsReplicaReceiptRecord],
        intents: &[crate::DfsWriteIntentRecord],
        intent_receipts: &[DfsWriteIntentReceipt],
        intent_receipt_retention: &DfsWriteIntentReceiptRetentionState,
        conflicts: &[crate::DfsConflictRecord],
        advisory_lock_state: Option<&crate::DfsAdvisoryLockStateRecord>,
    ) -> Result<bool> {
        let path = normalize_posix_path(&object.path)?;
        if versions.iter().any(|version| {
            version.retained.is_some()
                || version.history_order != 0
                || version.content_durability.is_some()
        }) {
            self.require_file_history_shard_support(&object.file_system_id, &path)
                .map_err(DfsPosixError::from)?;
        }
        let supplied_version_pin_set_fingerprint = version_pin_set_fingerprint.map(str::to_string);
        let canonical_version_pin_set_fingerprint =
            crate::protocol::cross_shard_version_pin_set_fingerprint(object, version_pins)
                .map_err(DfsPosixError::from)?;
        if supplied_version_pin_set_fingerprint
            .as_ref()
            .is_some_and(|supplied| supplied != &canonical_version_pin_set_fingerprint)
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "cross-shard adoption of {} supplied a mismatched durable version-pin fingerprint",
                    object.object_id
                ),
            }));
        }
        let pin_transfer_active = if supplied_version_pin_set_fingerprint.is_some() {
            let pin_transfer_id = pin_transfer_id
                .filter(|transfer_id| !transfer_id.is_empty())
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Distributed {
                        message: format!(
                            "pin-aware cross-shard adoption of {} omitted its stable transfer identity",
                            object.object_id
                        ),
                    })
                })?;
            self.metadata_backend
                .as_mut()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "pin-aware cross-shard adoption of {} requires a durable metadata backend",
                            object.object_id
                        ),
                    })
                })?
                .reconcile_cross_shard_pin_transfer(
                    pin_transfer_id,
                    object,
                    source_path,
                    versions,
                    version_pins,
                )?
        } else {
            false
        };
        let mut transaction_objects = Vec::new();
        let mut already_adopted = false;
        if let Some(destination) = self.lookup_sifs_path(&path)? {
            if destination.object_id == object.object_id && destination.kind == object.kind {
                let adopted_version_is_ancestor = if destination.version == object.version {
                    true
                } else if let Some(backend) = self.metadata_backend.as_ref() {
                    backend
                        .get_version(&object.object_id, &object.version)?
                        .is_some()
                } else {
                    false
                };
                if adopted_version_is_ancestor {
                    // A reply can be lost after adoption and before the
                    // source observes it. If the destination subsequently
                    // advances the same object, the migrated version remains
                    // in its history and proves that this is an exact retry;
                    // never roll the newer visible object back to the frame.
                    already_adopted = true;
                } else {
                    return Err(DfsPosixError::Conflict {
                        path: path.clone(),
                        reason: format!(
                            "destination object {} at {path} has unrelated version {} while adopting {}",
                            destination.object_id, destination.version, object.version
                        ),
                    });
                }
            } else if destination.kind != DfsObjectKind::Tombstone {
                match (&object.kind, &destination.kind) {
                    // A moved directory may replace an empty directory, like
                    // a local rename over an empty destination directory.
                    (DfsObjectKind::Directory, DfsObjectKind::Directory) => {
                        if destination.object_id != object.object_id
                            && self.sifs_dir_has_children(&path)?
                        {
                            return Err(DfsPosixError::DirectoryNotEmpty { path });
                        }
                    }
                    (DfsObjectKind::Directory, _) => {
                        return Err(DfsPosixError::NotDirectory { path });
                    }
                    (_, DfsObjectKind::Directory) => {
                        return Err(DfsPosixError::IsDirectory { path });
                    }
                    (_, DfsObjectKind::Hardlink)
                        if destination.link_target_object_id.as_ref()
                            == Some(&object.object_id) =>
                    {
                        // Hardlink promotion: the moved object replaces its
                        // own alias at this path.
                    }
                    (_, DfsObjectKind::Hardlink) => {
                        return Err(DfsPosixError::InvalidPath {
                            path: format!(
                                "{path} is a hardlink to another object; cross-shard replace is not supported"
                            ),
                        });
                    }
                    _ => {
                        if self.hardlink_count_to_object(&destination.object_id)? > 0 {
                            return Err(DfsPosixError::InvalidPath {
                                path: format!(
                                    "{path} has hardlink aliases; cross-shard replace is not supported"
                                ),
                            });
                        }
                    }
                }
                let destination_tombstone = if destination.kind == DfsObjectKind::Hardlink
                    && destination.link_target_object_id.as_ref() == Some(&object.object_id)
                {
                    // Promotion replaces the alias at this path with its
                    // canonical record. Park the alias under a hidden
                    // tombstone key so it cannot remain a second live link at
                    // the newly canonical path.
                    let hidden_path = self.hidden_alias_tombstone_path_on_destination_shard(
                        &path,
                        &destination.object_id,
                    )?;
                    self.tombstone_path_object_at(destination.clone(), hidden_path)
                } else {
                    self.tombstone_path_object(destination)
                };
                transaction_objects.push(destination_tombstone);
            }
        }
        let rebound_advisory_lock_state = advisory_lock_state
            .map(crate::advisory_lock::prepare_advisory_lock_state_for_migration)
            .transpose()
            .map_err(|source| {
                DfsPosixError::Dfs(DfsError::Serialization {
                    message: format!(
                        "encode migrated advisory-lock state for {}: {source}",
                        object.object_id
                    ),
                })
            })?
            .map(|(state, _)| state);
        if !already_adopted {
            self.ensure_parent_directory(&path)?;
            transaction_objects.push(object.clone());
            let mut visible_version = cross_shard_object_version_record(object);
            visible_version.content_durability = versions
                .iter()
                .find(|version| {
                    version.object_id == object.object_id && version.version == object.version
                })
                .and_then(|version| version.content_durability.clone());
            self.apply_namespace_transaction(DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: namespace_transaction_id_for_versioned_path(
                    "adopt-cross-shard",
                    &object.object_id,
                    &object.version,
                    &path,
                ),
                file_system_id: object.file_system_id.clone(),
                objects: transaction_objects,
                versions: vec![visible_version],
                advisory_lock_states: rebound_advisory_lock_state.iter().cloned().collect(),
                created_at: time::OffsetDateTime::now_utc(),
            })?;
        } else if let Some(state) = advisory_lock_state {
            // A rolling-upgrade peer may have adopted the object before it
            // understood the optional lock aggregate. An exact retry must be
            // able to repair that partial legacy result before acknowledging
            // the new capability receipt.
            self.metadata_backend
                .as_mut()
                .ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message:
                            "lock-bearing cross-shard adoption requires a durable metadata backend"
                                .to_string(),
                    })
                })?
                .install_advisory_lock_state_for_migration(state)?;
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            for version in versions {
                if backend
                    .get_version(&version.object_id, &version.version)?
                    .is_none()
                {
                    backend.put_version(version)?;
                }
            }
            if supplied_version_pin_set_fingerprint.is_some() {
                let installed_pins = backend.version_pins_for_object(&object.object_id)?;
                if !version_pins
                    .iter()
                    .all(|expected| installed_pins.iter().any(|installed| installed == expected))
                {
                    return Err(DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "destination durable version-pin set for {} is missing part of the acknowledged cross-shard family",
                            object.object_id
                        ),
                    }));
                }
            } else {
                let existing_pins = backend.version_pins_for_object(&object.object_id)?;
                let (pins_to_put, _) = plan_cross_shard_version_pin_reconciliation(
                    &existing_pins,
                    version_pins,
                    false,
                )?;
                for pin in &pins_to_put {
                    backend.put_version_pin(pin)?;
                }
            }
        }
        for receipt in receipts {
            let exists = self
                .backend_receipt_for_replica(
                    &receipt.receipt.object_id,
                    &receipt.receipt.committed_version,
                    &receipt.replica_node_id,
                )?
                .is_some();
            if !exists && let Some(backend) = self.metadata_backend.as_mut() {
                backend.put_receipt(receipt)?;
            }
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            for intent in intents {
                if let Some(receipt) =
                    backend.get_intent_receipt(&intent.object_id, &intent.intent_id)?
                {
                    if !receipt.matches_intent(intent) {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "destination has conflicting compact intent receipt {}/{} during cross-shard adoption",
                                intent.object_id, intent.intent_id
                            ),
                        }));
                    }
                    continue;
                }
                match backend.get_intent(&intent.object_id, &intent.intent_id)? {
                    Some(existing) if existing != *intent => {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "destination has conflicting full intent {}/{} during cross-shard adoption",
                                intent.object_id, intent.intent_id
                            ),
                        }));
                    }
                    Some(_) => {}
                    None => {
                        backend.put_migrated_intent_for_object(Some(object), intent)?;
                    }
                }
            }
            for receipt in intent_receipts {
                match backend.get_intent_receipt(&receipt.object_id, &receipt.intent_id)? {
                    Some(existing) if existing != *receipt => {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message: format!(
                                "destination has conflicting compact intent receipt {}/{} during cross-shard adoption",
                                receipt.object_id, receipt.intent_id
                            ),
                        }));
                    }
                    Some(_) => {}
                    None => {
                        if let Some(existing) =
                            backend.get_intent(&receipt.object_id, &receipt.intent_id)?
                            && !receipt.matches_intent(&existing)
                        {
                            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                                message: format!(
                                    "destination has conflicting full intent {}/{} during cross-shard adoption",
                                    receipt.object_id, receipt.intent_id
                                ),
                            }));
                        }
                        backend.put_intent_receipt_for_object(Some(object), receipt)?;
                    }
                }
            }
            if !intent_receipt_retention.is_empty() {
                backend.merge_intent_receipt_retention_for_object(
                    Some(object),
                    &object.file_system_id,
                    &object.object_id,
                    intent_receipt_retention,
                )?;
            }
            for conflict in conflicts {
                if backend
                    .get_conflict(&conflict.object_id, &conflict.conflict_id)?
                    .is_none()
                {
                    backend.put_conflict(conflict)?;
                }
            }
        }
        if !already_adopted {
            self.touch_parent_directory_if_local_primary(&path)?;
        }
        Ok(pin_transfer_active)
    }

    /// Atomically installs a hardlink alias created by another primary's
    /// coordinator. Idempotent against re-sent adoptions.
    pub(crate) fn adopt_hardlink_alias_local(&mut self, alias: &DfsObjectMetadata) -> Result<()> {
        let path = normalize_posix_path(&alias.path)?;
        if let Some(existing) = self.lookup_sifs_path(&path)? {
            if existing.object_id == alias.object_id && existing.kind == DfsObjectKind::Hardlink {
                return Ok(());
            }
            if existing.kind != DfsObjectKind::Tombstone {
                return Err(DfsPosixError::AlreadyExists { path });
            }
        }
        self.ensure_parent_directory(&path)?;
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: namespace_transaction_id_for_versioned_path(
                "adopt-hardlink-cross-shard",
                &alias.object_id,
                &alias.version,
                &path,
            ),
            file_system_id: alias.file_system_id.clone(),
            objects: vec![alias.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_if_local_primary(&path)?;
        Ok(())
    }

    pub(crate) fn resume_running_cross_shard_namespace_job_for_paths(
        &mut self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
    ) -> Result<bool> {
        let Some(job) = self.running_cross_shard_namespace_job_for_paths(
            file_system_id,
            root_path,
            target_path,
            kinds,
        )?
        else {
            return Ok(false);
        };
        self.drive_cross_shard_namespace_job(job)?;
        Ok(true)
    }

    /// Resume, or prove terminal completion of, the exact hardlink-alias job
    /// owned by the active durable outer request. This check runs before CAS
    /// validation because a crash can leave the alias transaction visible
    /// while the outer request is still Pending.
    pub(crate) fn resume_or_replay_hardlink_alias_mutation_for_active_request(
        &mut self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
    ) -> Result<bool> {
        let kinds = [DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries];
        if self.resume_running_cross_shard_namespace_job_for_paths(
            file_system_id,
            root_path,
            target_path,
            &kinds,
        )? {
            return Ok(true);
        }
        let Some(request) = self.namespace_execution.request().cloned() else {
            return Ok(false);
        };
        self.completed_cross_shard_namespace_job_for_paths_since(
            file_system_id,
            root_path,
            target_path,
            &kinds,
            request.created_at,
            &DfsNamespaceJobOrigin::from_request(&request),
        )
    }

    fn running_cross_shard_namespace_job_for_paths(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
    ) -> Result<Option<DfsNamespaceJobRecord>> {
        self.cross_shard_namespace_job_for_paths(
            file_system_id,
            root_path,
            target_path,
            kinds,
            DfsNamespaceJobStatus::Running,
            None,
            None,
        )
    }

    pub(crate) fn completed_cross_shard_namespace_job_for_paths_since(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        created_at: time::OffsetDateTime,
        origin: &DfsNamespaceJobOrigin,
    ) -> Result<bool> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        let root_path = match normalize_posix_path(root_path) {
            Ok(path) => path,
            Err(_) => return Ok(false),
        };
        let target_path = match normalize_posix_path(target_path) {
            Ok(path) => path,
            Err(_) => return Ok(false),
        };
        if backend
            .completed_namespace_job_receipt_for_paths_since(
                file_system_id,
                &root_path,
                &target_path,
                kinds,
                created_at,
                origin,
            )?
            .is_some()
        {
            return Ok(true);
        }
        self.cross_shard_namespace_job_for_paths(
            file_system_id,
            &root_path,
            &target_path,
            kinds,
            DfsNamespaceJobStatus::Completed,
            Some(created_at),
            Some(origin),
        )
        .map(|job| job.is_some())
    }

    fn cross_shard_namespace_job_for_paths(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        status: DfsNamespaceJobStatus,
        created_at_not_before: Option<time::OffsetDateTime>,
        origin: Option<&DfsNamespaceJobOrigin>,
    ) -> Result<Option<DfsNamespaceJobRecord>> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let root_path = match normalize_posix_path(root_path) {
            Ok(path) => path,
            Err(_) => return Ok(None),
        };
        let target_path = match normalize_posix_path(target_path) {
            Ok(path) => path,
            Err(_) => return Ok(None),
        };
        let mut existing_job = None;
        try_walk_metadata_pages(|cursor| {
            let page = backend
                .list_namespace_jobs_page(cursor, POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE)?;
            for job in page.jobs {
                if job.file_system_id != *file_system_id
                    || job.status != status
                    || !kinds.contains(&job.kind)
                    || created_at_not_before.is_some_and(|created_at| job.created_at < created_at)
                    || origin.is_some_and(|expected| {
                        job.origin.as_ref().is_some_and(|actual| actual != expected)
                    })
                {
                    continue;
                }
                let Ok(job_root_path) = normalize_posix_path(&job.root_path) else {
                    continue;
                };
                if job_root_path != root_path {
                    continue;
                }
                let Some(job_target_path) = job.target_path.as_ref() else {
                    continue;
                };
                let Ok(job_target_path) = normalize_posix_path(job_target_path) else {
                    continue;
                };
                if job_target_path == target_path {
                    existing_job = Some(job);
                    return Ok(None);
                }
            }
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        Ok(existing_job)
    }

    /// Reject object-scoped coordination while a durable cross-shard move can
    /// still copy or evict that object's metadata family. Unlike the in-memory
    /// path guard, this survives a coordinator restart before job recovery has
    /// rebuilt its transient lock set.
    pub(super) fn ensure_no_running_cross_shard_move_for_path(&self, path: &str) -> Result<()> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let path = self.resolve_posix_dot_components(path)?;
        let mut blocked = false;
        try_walk_metadata_pages(|cursor| {
            let page = backend
                .list_namespace_jobs_page(cursor, POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE)?;
            for job in &page.jobs {
                if job.file_system_id != sifs_file_system_id()
                    || job.status != DfsNamespaceJobStatus::Running
                    || !matches!(
                        job.kind,
                        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                    )
                {
                    continue;
                }
                let Ok(root_path) = normalize_posix_path(&job.root_path) else {
                    continue;
                };
                let target_path = job
                    .target_path
                    .as_deref()
                    .and_then(|target| normalize_posix_path(target).ok());
                if posix_path_is_at_or_under_prefix(&path, &root_path)
                    || posix_path_is_at_or_under_prefix(&root_path, &path)
                    || target_path.as_ref().is_some_and(|target_path| {
                        posix_path_is_at_or_under_prefix(&path, target_path)
                            || posix_path_is_at_or_under_prefix(target_path, &path)
                    })
                {
                    blocked = true;
                    return Ok(None);
                }
            }
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        if blocked {
            return Err(DfsPosixError::Dfs(DfsError::Busy { path }));
        }
        Ok(())
    }
}

fn plan_cross_shard_version_pin_reconciliation(
    existing: &[DfsMetadataVersionPinRecord],
    desired: &[DfsMetadataVersionPinRecord],
    exact: bool,
) -> Result<(
    Vec<DfsMetadataVersionPinRecord>,
    Vec<DfsMetadataVersionPinRecord>,
)> {
    let same_logical_pin = |left: &DfsMetadataVersionPinRecord,
                            right: &DfsMetadataVersionPinRecord| {
        left.object_id == right.object_id
            && left.version == right.version
            && left.pin_id == right.pin_id
    };
    if desired.iter().enumerate().any(|(index, pin)| {
        desired
            .iter()
            .skip(index + 1)
            .any(|other| same_logical_pin(pin, other))
    }) {
        return Err(DfsPosixError::Dfs(DfsError::Metadata {
            message: "cross-shard adoption requested duplicate durable version-pin identities"
                .to_string(),
        }));
    }
    if existing.iter().enumerate().any(|(index, pin)| {
        existing
            .iter()
            .skip(index + 1)
            .any(|other| same_logical_pin(pin, other))
    }) {
        return Err(DfsPosixError::Dfs(DfsError::Metadata {
            message: "destination contains duplicate durable version-pin identities".to_string(),
        }));
    }

    let mut pins_to_put = Vec::new();
    let mut pins_to_remove = Vec::new();
    for desired_pin in desired {
        match existing
            .iter()
            .find(|existing_pin| same_logical_pin(existing_pin, desired_pin))
        {
            Some(existing_pin) if existing_pin == desired_pin => {}
            Some(existing_pin) if exact => {
                pins_to_remove.push(existing_pin.clone());
                pins_to_put.push(desired_pin.clone());
            }
            Some(_) => {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: format!(
                        "destination has conflicting durable version pin {}/{}/{} during cross-shard adoption",
                        desired_pin.object_id, desired_pin.version, desired_pin.pin_id
                    ),
                }));
            }
            None => pins_to_put.push(desired_pin.clone()),
        }
    }
    if exact {
        for existing_pin in existing {
            let retained_exactly = desired
                .iter()
                .any(|desired_pin| desired_pin == existing_pin);
            if !retained_exactly && !pins_to_remove.iter().any(|pin| pin == existing_pin) {
                pins_to_remove.push(existing_pin.clone());
            }
        }
    }
    Ok((pins_to_put, pins_to_remove))
}

#[cfg(test)]
mod version_pin_reconciliation_tests {
    use super::*;
    use sinomo_core::node::NodeId;
    use time::OffsetDateTime;

    fn pin(version: &str, pin_id: &str, created_at: OffsetDateTime) -> DfsMetadataVersionPinRecord {
        DfsMetadataVersionPinRecord {
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            object_id: DfsObjectId::from_legacy_named("object_cross_shard_pin").expect("object"),
            version: crate::DfsVersion::from_legacy_named(version).expect("version"),
            pin_id: pin_id.to_string(),
            created_at,
        }
    }

    #[test]
    fn exact_pin_reconciliation_installs_the_first_copy() {
        let desired = pin("version_one", "checkpoint", OffsetDateTime::UNIX_EPOCH);
        let (puts, removals) =
            plan_cross_shard_version_pin_reconciliation(&[], std::slice::from_ref(&desired), true)
                .expect("reconciliation");
        assert_eq!(puts, vec![desired]);
        assert!(removals.is_empty());
    }

    #[test]
    fn exact_pin_reconciliation_removes_a_pin_unpinned_after_the_first_copy() {
        let copied = pin("version_one", "checkpoint", OffsetDateTime::UNIX_EPOCH);
        let (puts, removals) =
            plan_cross_shard_version_pin_reconciliation(std::slice::from_ref(&copied), &[], true)
                .expect("reconciliation");
        assert!(puts.is_empty());
        assert_eq!(removals, vec![copied]);
    }

    #[test]
    fn legacy_empty_pin_adoption_does_not_remove_destination_pins() {
        let copied = pin("version_one", "checkpoint", OffsetDateTime::UNIX_EPOCH);
        let (puts, removals) = plan_cross_shard_version_pin_reconciliation(&[copied], &[], false)
            .expect("reconciliation");
        assert!(puts.is_empty());
        assert!(removals.is_empty());
    }

    #[test]
    fn exact_pin_reconciliation_replaces_a_conflicting_logical_record() {
        let existing = pin("version_one", "checkpoint", OffsetDateTime::UNIX_EPOCH);
        let desired = pin(
            "version_one",
            "checkpoint",
            OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND,
        );
        let (puts, removals) = plan_cross_shard_version_pin_reconciliation(
            std::slice::from_ref(&existing),
            std::slice::from_ref(&desired),
            true,
        )
        .expect("reconciliation");
        assert_eq!(puts, vec![desired]);
        assert_eq!(removals, vec![existing]);
    }

    #[test]
    fn cross_shard_version_record_is_exactly_stable_across_retry() {
        let mut object = crate::namespace::directory_object(
            "/services/stable-version",
            NodeId::from_legacy_named("node_stable_version").expect("node"),
        );
        object.changed_at_unix_nanos = 42;

        let first = cross_shard_object_version_record(&object);
        let retry = cross_shard_object_version_record(&object);

        assert_eq!(retry, first);
        assert_eq!(first.object_id, object.object_id);
        assert_eq!(first.version, object.version);
        assert_eq!(first.content_hash, object.content_hash);
        assert_eq!(first.size_bytes, object.content_size_bytes);
        assert_eq!(first.locations, object.current_locations);
        assert_eq!(first.created_at.unix_timestamp_nanos(), 42);
    }

    #[test]
    fn cross_shard_job_phase_ids_are_stable_and_object_scoped() {
        let job = DfsNamespaceJobRecord {
            job_id: "rename-job".to_string(),
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            origin: None,
            kind: DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/users/source".to_string(),
            target_path: Some("/services/target".to_string()),
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        };
        type Service = NetworkedDfsService<sinomo_core::network::InProcessTransport>;

        let first = Service::cross_shard_job_phase_operation_id(
            &job,
            "adopt-object-family\0object-a\0/users/source/a",
        );
        let retry = Service::cross_shard_job_phase_operation_id(
            &job,
            "adopt-object-family\0object-a\0/users/source/a",
        );
        let sibling = Service::cross_shard_job_phase_operation_id(
            &job,
            "adopt-object-family\0object-b\0/users/source/b",
        );

        assert_eq!(retry, first);
        assert_ne!(sibling, first);
        assert!(first.starts_with("xsj_"));
        assert!(
            first.len() <= 256,
            "phase id must fit the namespace request bound"
        );
    }
}
