//! Cross-shard namespace saga helpers for the POSIX facade.
//!
//! This module owns coordinated SIFS metadata-shard moves: cross-primary
//! rename, hardlink adoption, resumable job driving, and destination-side
//! adoption. Local namespace operations and generic namespace-job storage stay
//! in the parent `posix` module.

use super::{
    CROSS_SHARD_MOVE_VERSION_HISTORY_LIMIT, POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
    POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE, POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
    POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE, namespace_job_id, now_unix_nanos,
};
use crate::metadata_pages::{try_walk_metadata_pages, walk_metadata_pages};
use crate::metadata_shard_migration::collect_object_metadata_family;
use crate::namespace::{
    initial_version, namespace_transaction_id_for_path, next_version, object_id_for_path,
};
use crate::posix_model::{DfsPosixError, Result};
use crate::posix_path::{
    normalize_posix_path, normalize_posix_path_with_trailing_directory,
    posix_path_is_at_or_under_prefix, subtree_prefix,
};
use crate::{
    DfsDirectoryPageCursor, DfsError, DfsFileSystemId, DfsMessageTransport, DfsNamespaceJobFrame,
    DfsNamespaceJobKind, DfsNamespaceJobRecord, DfsNamespaceJobStatus, DfsNamespaceOperation,
    DfsNamespaceTransaction, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsObjectVersionRecord,
    DfsReplicaReceiptRecord, DfsWriteIntentState, DfsWriteIntentStatus, NetworkedDfsService,
    object_version_record, sifs_file_system_id,
};
use sinomo_core::node::NodeId;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
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
            // The adoption already applied; only the source half remains.
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
        self.adopt_moved_object_on_destination_primary(&moved, source_root)?;
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
                // Already adopted; ensure the source half is evicted.
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
            self.adopt_moved_object_on_destination_primary(&moved, &object_path)?;
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
        for (intent_id, pending) in &self.pending_writes {
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
    fn run_cross_shard_adopt(
        &mut self,
        frame_object: &DfsObjectMetadata,
        old_path: &str,
    ) -> Result<()> {
        let old_path = normalize_posix_path(old_path)?;
        let target_path = normalize_posix_path(&frame_object.path)?;
        let Some(current) = self.object_by_id(&frame_object.object_id)? else {
            return Err(DfsPosixError::NotFound { path: old_path });
        };
        if current.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: old_path });
        }
        let current_path = normalize_posix_path(&current.path)?;
        if current_path == target_path {
            // The adoption (and eviction) already applied on a previous
            // attempt; nothing to re-send.
            return Ok(());
        }
        if current_path != old_path {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{current_path} -> {target_path}"),
            });
        }
        self.ensure_no_open_write_state_for_object(&current, &old_path)?;
        let mut moved = current;
        moved.path = target_path;
        moved.changed_at_unix_nanos =
            now_unix_nanos().max(moved.changed_at_unix_nanos.saturating_add(1));
        moved.version = next_version(&moved.version);
        self.adopt_moved_object_on_destination_primary(&moved, &old_path)
    }

    /// Routes the adoption of a moved record to the destination path's
    /// current primary (re-resolved from the control plane on every attempt),
    /// or adopts locally when a shard transfer made this node the authority.
    fn adopt_moved_object_on_destination_primary(
        &mut self,
        moved: &DfsObjectMetadata,
        source_path: &str,
    ) -> Result<()> {
        let (versions, receipts, intents, conflicts, advisory_lock_state) =
            self.cross_shard_move_bundle(&moved.object_id)?;
        match self.routed_namespace_primary_for_path(&moved.path)? {
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
                    self.route_namespace_operation(
                        primary.clone(),
                        DfsNamespaceOperation::AdvisoryLockMigrationPrepare {
                            path: moved.path.clone(),
                            source_path: source_path.to_string(),
                            object_id: moved.object_id.clone(),
                            state_fingerprint,
                        },
                    )?;
                }
                self.route_namespace_operation(
                    primary,
                    DfsNamespaceOperation::AdoptObjectAtPath {
                        object: moved.clone(),
                        source_path: source_path.to_string(),
                        versions,
                        receipts,
                        intents,
                        conflicts,
                        advisory_lock_state,
                    },
                )
            }
            None => {
                self.adopt_object_at_path_local(
                    moved,
                    &versions,
                    &receipts,
                    &intents,
                    &conflicts,
                    advisory_lock_state.as_ref(),
                )?;
                if advisory_lock_state.is_some() {
                    let metadata_target = self.capture_metadata_quorum_sync_target(moved)?;
                    self.sync_metadata_shard_to_quorum(
                        &metadata_target,
                        crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
                    )?;
                }
                Ok(())
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
        let alias_object_id = {
            let candidate = object_id_for_path(&link_path);
            if candidate == target.object_id {
                DfsObjectId::generate()
            } else {
                candidate
            }
        };
        let alias = DfsObjectMetadata {
            object_id: alias_object_id,
            file_system_id: target.file_system_id.clone(),
            path: link_path.clone(),
            kind: DfsObjectKind::Hardlink,
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
            self.unlock_cross_shard_namespace_path(&file_system_id, &job.root_path);
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
        self.lock_cross_shard_namespace_path(&file_system_id, &job.root_path);
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
            let outcome = match frame.clone() {
                DfsNamespaceJobFrame::CrossShardAdopt {
                    moved_object,
                    old_path,
                    destination_primary_node_id,
                } => {
                    let _ = destination_primary_node_id;
                    self.run_cross_shard_adopt(&moved_object, &old_path)
                }
                DfsNamespaceJobFrame::CrossShardComplete {
                    moved_object,
                    old_path,
                } => self.complete_cross_shard_move_locally(&moved_object, &old_path, true),
                DfsNamespaceJobFrame::CrossShardDirStart {
                    source_object_id,
                    source_root,
                    target_root,
                } => self.run_cross_shard_dir_start(&source_object_id, &source_root, &target_root),
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
                other => Err(DfsPosixError::InvalidPath {
                    path: format!("{}: unexpected frame {other:?}", job.job_id),
                }),
            };
            match outcome {
                Ok(()) => {
                    job.processed_object_count = job.processed_object_count.saturating_add(1);
                    job.updated_at = time::OffsetDateTime::now_utc();
                    self.persist_namespace_job(&job)?;
                }
                Err(error @ (DfsPosixError::Dfs(_) | DfsPosixError::Metadata(_))) => {
                    // Transport or internal failure: the saga stays running
                    // (and the path stays locked) so maintenance resumes it.
                    job.pending_frames.push(frame);
                    job.updated_at = time::OffsetDateTime::now_utc();
                    self.persist_namespace_job(&job)?;
                    return Err(error);
                }
                Err(error) => {
                    // Structured POSIX rejection from validation: abort before
                    // any local mutation happened.
                    self.unlock_cross_shard_namespace_path(&file_system_id, &job.root_path);
                    self.fail_namespace_job(job, error.to_string())?;
                    return Err(error);
                }
            }
        }
        let now = time::OffsetDateTime::now_utc();
        job.status = DfsNamespaceJobStatus::Completed;
        job.updated_at = now;
        job.completed_at = Some(now);
        self.persist_namespace_job(&job)?;
        self.unlock_cross_shard_namespace_path(&file_system_id, &job.root_path);
        Ok(job)
    }

    /// The moved object's version, receipt, intent, and conflict history,
    /// shipped with the adoption so the destination shard owns the complete
    /// record set for the object (mirroring same-node shard migration): its
    /// replicas can serve and repair the content at the new home, and
    /// historical intents stay resolvable for journal-replay tolerance.
    #[allow(clippy::type_complexity)]
    fn cross_shard_move_bundle(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<(
        Vec<DfsObjectVersionRecord>,
        Vec<DfsReplicaReceiptRecord>,
        Vec<crate::DfsWriteIntentRecord>,
        Vec<crate::DfsConflictRecord>,
        Option<crate::DfsAdvisoryLockStateRecord>,
    )> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new(), None));
        };
        let family = collect_object_metadata_family(
            backend.as_ref(),
            object_id,
            POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
            Some(CROSS_SHARD_MOVE_VERSION_HISTORY_LIMIT),
        )?;
        Ok((
            family.versions,
            family.receipts,
            family.intents,
            family.conflicts,
            family.advisory_lock_state,
        ))
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
                transaction_id: namespace_transaction_id_for_path(
                    "rename-cross-shard",
                    &moved_object.object_id,
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
        versions: &[DfsObjectVersionRecord],
        receipts: &[DfsReplicaReceiptRecord],
        intents: &[crate::DfsWriteIntentRecord],
        conflicts: &[crate::DfsConflictRecord],
        advisory_lock_state: Option<&crate::DfsAdvisoryLockStateRecord>,
    ) -> Result<()> {
        let path = normalize_posix_path(&object.path)?;
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
                transaction_objects.push(self.tombstone_path_object(destination));
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
            let visible_version = object_version_record(object, object.content_size_bytes);
            self.apply_namespace_transaction(DfsNamespaceTransaction {
                transaction_id: namespace_transaction_id_for_path(
                    "adopt-cross-shard",
                    &object.object_id,
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
                if backend
                    .get_intent(&intent.object_id, &intent.intent_id)?
                    .is_none()
                {
                    backend.put_intent(intent)?;
                }
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
        Ok(())
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
            transaction_id: namespace_transaction_id_for_path(
                "adopt-hardlink-cross-shard",
                &alias.object_id,
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

    fn running_cross_shard_namespace_job_for_paths(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
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
                    || job.status != DfsNamespaceJobStatus::Running
                    || !kinds.contains(&job.kind)
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
        let path = normalize_posix_path(path)?;
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
