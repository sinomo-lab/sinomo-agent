//! Namespace job helpers for the POSIX facade.
//!
//! This module owns resumable POSIX namespace jobs that are not specific to
//! metadata-shard routing: recursive directory clear, same-shard directory
//! rename, maintenance resume scans, and durable job status updates.

use super::{
    POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE, POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
    POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE, namespace_job_id, now_unix_nanos,
};
use crate::namespace::{namespace_transaction_id_for_path, next_version};
use crate::posix_model::{
    DfsNamespaceJobFailure, DfsNamespaceJobMaintenanceReport, DfsPosixError, Result,
};
use crate::posix_path::{
    hidden_alias_tombstone_path, normalize_posix_path,
    normalize_posix_path_with_trailing_directory, parent_posix_path, subtree_prefix,
};
use crate::{
    DfsMessageTransport, DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobRecord,
    DfsNamespaceJobStatus, DfsNamespaceOperation, DfsNamespaceTransaction, DfsObjectId,
    DfsObjectKind, DfsObjectMetadata, NetworkedDfsService,
};

fn namespace_job_kind_is_cross_shard(kind: &DfsNamespaceJobKind) -> bool {
    matches!(
        kind,
        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
            | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
    )
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn posix_clear_dir_recursive(&mut self, path: &str) -> Result<()> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            return self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::ClearDirRecursive {
                    path: path.to_string(),
                },
            );
        }
        self.posix_clear_dir_recursive_local(path)
    }

    pub(crate) fn posix_clear_dir_recursive_local(&mut self, path: &str) -> Result<()> {
        let mut job =
            self.posix_clear_dir_recursive_bounded(path, POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS)?;
        while job.status != DfsNamespaceJobStatus::Completed {
            job = self.posix_resume_namespace_job_bounded(
                &job.job_id,
                POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
            )?;
        }
        Ok(())
    }

    pub fn posix_clear_dir_recursive_bounded(
        &mut self,
        path: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        let job = self.clear_dir_recursive_job_for_path(&path)?;
        self.persist_namespace_job(&job)?;
        self.run_clear_dir_recursive_job_batch(job, max_steps)
    }

    pub fn posix_resume_namespace_job_bounded(
        &mut self,
        job_id: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        let Some(job) = self.namespace_job(job_id)? else {
            return Err(DfsPosixError::NotFound {
                path: job_id.to_string(),
            });
        };
        if job.status != DfsNamespaceJobStatus::Running {
            return Ok(job);
        }
        match job.kind {
            DfsNamespaceJobKind::ClearDirectoryRecursive => {
                self.run_clear_dir_recursive_job_batch(job, max_steps)
            }
            DfsNamespaceJobKind::RenameDirectory => {
                self.run_rename_directory_job_batch(job, max_steps)
            }
            DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
            | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries => {
                self.run_cross_shard_namespace_job(job, max_steps)
            }
        }
    }

    pub fn posix_resume_namespace_jobs_bounded(
        &mut self,
        max_jobs: usize,
        max_steps_per_job: usize,
    ) -> Result<DfsNamespaceJobMaintenanceReport> {
        if self.metadata_backend.is_none() {
            return Ok(DfsNamespaceJobMaintenanceReport::default());
        }
        let mut cursor = None;
        let mut report = DfsNamespaceJobMaintenanceReport::default();
        loop {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .list_namespace_jobs_page(
                    cursor.as_ref(),
                    POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE,
                )?;
            report.scanned_job_count = report.scanned_job_count.saturating_add(page.jobs.len());
            for job in page.jobs {
                if job.status != DfsNamespaceJobStatus::Running {
                    continue;
                }
                if report.resumed_job_count >= max_jobs {
                    report.remaining_running_job_count =
                        report.remaining_running_job_count.saturating_add(1);
                    continue;
                }
                report.resumed_job_count = report.resumed_job_count.saturating_add(1);
                match self.posix_resume_namespace_job_bounded(&job.job_id, max_steps_per_job) {
                    Ok(updated) => match updated.status {
                        DfsNamespaceJobStatus::Completed => {
                            report.completed_job_count =
                                report.completed_job_count.saturating_add(1);
                        }
                        DfsNamespaceJobStatus::Running => {
                            report.remaining_running_job_count =
                                report.remaining_running_job_count.saturating_add(1);
                        }
                        DfsNamespaceJobStatus::Failed => {
                            report.failed_job_count = report.failed_job_count.saturating_add(1);
                        }
                    },
                    Err(error) => {
                        let reason = error.to_string();
                        let latest = self.namespace_job(&job.job_id)?.unwrap_or(job);
                        match latest.status {
                            DfsNamespaceJobStatus::Completed => {
                                report.completed_job_count =
                                    report.completed_job_count.saturating_add(1);
                            }
                            DfsNamespaceJobStatus::Running
                                if namespace_job_kind_is_cross_shard(&latest.kind) =>
                            {
                                report.remaining_running_job_count =
                                    report.remaining_running_job_count.saturating_add(1);
                            }
                            DfsNamespaceJobStatus::Running => {
                                let failed = self.fail_namespace_job(latest, reason.clone())?;
                                report.failed_job_count = report.failed_job_count.saturating_add(1);
                                report.failures.push(DfsNamespaceJobFailure {
                                    job_id: failed.job_id,
                                    kind: failed.kind,
                                    reason,
                                });
                            }
                            DfsNamespaceJobStatus::Failed => {
                                report.failed_job_count = report.failed_job_count.saturating_add(1);
                                report.failures.push(DfsNamespaceJobFailure {
                                    job_id: latest.job_id,
                                    kind: latest.kind,
                                    reason: latest.failure_reason.unwrap_or(reason),
                                });
                            }
                        }
                    }
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(report)
    }

    pub fn posix_rename_directory_bounded(
        &mut self,
        from: &str,
        to: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
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
        // This bounded maintenance API runs the local rename job only; a
        // destination owned by another metadata primary must go through the
        // routed `posix_rename`, whose coordinator journals each half on its
        // own authority.
        if self.routed_namespace_primary_for_path(&from)?.is_some()
            || self.routed_namespace_primary_for_path(&to)?.is_some()
        {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{from} -> {to} crosses metadata shard primaries; use posix_rename"),
            });
        }
        if from == to {
            let source = self
                .lookup_sifs_path(&from)?
                .ok_or_else(|| DfsPosixError::NotFound { path: from.clone() })?;
            if source.kind != DfsObjectKind::Directory {
                return Err(DfsPosixError::NotDirectory { path: from });
            }
            return self.rename_directory_job_for_paths(&from, &to, &source);
        }
        let source = self
            .lookup_sifs_path(&from)?
            .ok_or_else(|| DfsPosixError::NotFound { path: from.clone() })?;
        if source.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: from });
        }
        if source.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path: from });
        }
        let _ = to_requires_directory;
        self.posix_rename_directory_bounded_resolved(&from, &to, source, max_steps)
    }

    fn clear_dir_recursive_job_for_path(&self, path: &str) -> Result<DfsNamespaceJobRecord> {
        let directory = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if directory.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        if directory.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            });
        }
        let job_id = namespace_job_id(
            "clear-dir-recursive",
            &directory.file_system_id,
            path,
            &directory.object_id,
            &directory.version,
        );
        if let Some(job) = self.namespace_job(&job_id)? {
            return Ok(job);
        }
        let now = time::OffsetDateTime::now_utc();
        Ok(DfsNamespaceJobRecord {
            job_id,
            file_system_id: directory.file_system_id,
            kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
            status: DfsNamespaceJobStatus::Running,
            root_path: path.to_string(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::ScanDirectory {
                path: path.to_string(),
                cursor: None,
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        })
    }

    pub(super) fn namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        Ok(backend.get_namespace_job(job_id)?)
    }

    pub(super) fn persist_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_namespace_job(job)?;
        }
        Ok(())
    }

    pub(super) fn fail_namespace_job(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        reason: String,
    ) -> Result<DfsNamespaceJobRecord> {
        job.status = DfsNamespaceJobStatus::Failed;
        job.failure_reason = Some(reason);
        job.updated_at = time::OffsetDateTime::now_utc();
        self.persist_namespace_job(&job)?;
        Ok(job)
    }

    fn run_clear_dir_recursive_job_batch(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        if job.status == DfsNamespaceJobStatus::Completed {
            return Ok(job);
        }
        if max_steps == 0 {
            job.updated_at = time::OffsetDateTime::now_utc();
            self.persist_namespace_job(&job)?;
            return Ok(job);
        }
        let root_path = normalize_posix_path(&job.root_path)?;
        let prefix = subtree_prefix(&root_path);
        let mut steps = 0usize;
        while steps < max_steps {
            let Some(frame) = job.pending_frames.pop() else {
                self.touch_directory_modified(&root_path)?;
                let now = time::OffsetDateTime::now_utc();
                job.status = DfsNamespaceJobStatus::Completed;
                job.updated_at = now;
                job.completed_at = Some(now);
                self.persist_namespace_job(&job)?;
                return Ok(job);
            };
            match frame {
                DfsNamespaceJobFrame::ScanDirectory { path, cursor } => {
                    let page_size = (max_steps - steps)
                        .max(1)
                        .min(POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE);
                    let page =
                        self.child_objects_for_sifs_dir_page(&path, cursor.as_ref(), page_size)?;
                    let mut child_dirs = Vec::new();
                    for object in page.objects {
                        steps = steps.saturating_add(1);
                        let object_path = normalize_posix_path(&object.path)?;
                        let listed_object_id = object.object_id.clone();
                        let Some(visible_object) = self.lookup_sifs_path(&object_path)? else {
                            continue;
                        };
                        if visible_object.kind == DfsObjectKind::Tombstone
                            || visible_object.object_id != listed_object_id
                        {
                            continue;
                        }
                        if visible_object.kind == DfsObjectKind::Directory {
                            child_dirs.push(object_path);
                            continue;
                        }
                        if self.clear_visible_object_for_recursive_job(&object_path, &prefix)? {
                            job.processed_object_count =
                                job.processed_object_count.saturating_add(1);
                        }
                    }
                    if let Some(next_cursor) = page.next_cursor {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::ScanDirectory {
                                path: path.clone(),
                                cursor: Some(next_cursor),
                            });
                    } else if path != root_path {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::CompleteDirectory { path });
                    }
                    for child_path in child_dirs.into_iter().rev() {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::ScanDirectory {
                                path: child_path,
                                cursor: None,
                            });
                    }
                }
                DfsNamespaceJobFrame::CompleteDirectory { path } => {
                    steps = steps.saturating_add(1);
                    if path != root_path
                        && self.clear_visible_object_for_recursive_job(&path, &prefix)?
                    {
                        job.processed_object_count = job.processed_object_count.saturating_add(1);
                    }
                }
                DfsNamespaceJobFrame::RenameDirectoryStart { .. }
                | DfsNamespaceJobFrame::RenameDirectoryScan { .. }
                | DfsNamespaceJobFrame::RenameDirectoryComplete { .. }
                | DfsNamespaceJobFrame::CrossShardAdopt { .. }
                | DfsNamespaceJobFrame::CrossShardComplete { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkAdopt { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkComplete { .. }
                | DfsNamespaceJobFrame::CrossShardDirStart { .. }
                | DfsNamespaceJobFrame::CrossShardDirScan { .. }
                | DfsNamespaceJobFrame::CrossShardDirFinish { .. } => {
                    return Err(DfsPosixError::InvalidPath {
                        path: job.job_id.clone(),
                    });
                }
            }
        }
        job.updated_at = time::OffsetDateTime::now_utc();
        self.persist_namespace_job(&job)?;
        Ok(job)
    }

    fn clear_visible_object_for_recursive_job(
        &mut self,
        object_path: &str,
        prefix: &str,
    ) -> Result<bool> {
        for _ in 0..64 {
            let Some(object) = self.lookup_sifs_path(object_path)? else {
                return Ok(false);
            };
            if object.kind == DfsObjectKind::Tombstone {
                return Ok(false);
            }
            if !matches!(
                object.kind,
                DfsObjectKind::Hardlink | DfsObjectKind::Directory | DfsObjectKind::Tombstone
            ) && self.link_count_for_object(&object)? > 1
                && let Some(alias) = self.first_hardlink_alias_outside_prefix(&object, prefix)?
            {
                // Promoting onto an alias in another primary's shard runs the
                // cross-shard move saga instead of a local transaction.
                let alias_path = normalize_posix_path(&alias.path)?;
                if let Some(destination_primary) =
                    self.routed_namespace_primary_for_path(&alias_path)?
                {
                    self.coordinate_cross_shard_rename(
                        object_path,
                        &alias_path,
                        destination_primary,
                    )?;
                    return Ok(true);
                }
                let mut alias_tombstone = alias.clone();
                alias_tombstone.path = hidden_alias_tombstone_path(&alias.object_id);
                alias_tombstone.kind = DfsObjectKind::Tombstone;
                alias_tombstone.version = next_version(&alias.version);
                alias_tombstone.content_hash = None;
                alias_tombstone.content_size_bytes = None;
                alias_tombstone.link_target_object_id = None;
                alias_tombstone.current_locations = vec![self.node_id.clone()];

                let mut old_path_tombstone = object.clone();
                old_path_tombstone.object_id = DfsObjectId::generate();
                old_path_tombstone.kind = DfsObjectKind::Tombstone;
                old_path_tombstone.version = next_version(&object.version);
                old_path_tombstone.content_hash = None;
                old_path_tombstone.content_size_bytes = None;
                old_path_tombstone.link_target_object_id = None;
                old_path_tombstone.current_locations = vec![self.node_id.clone()];

                let mut promoted = object.clone();
                promoted.path = alias.path.clone();
                promoted.changed_at_unix_nanos = now_unix_nanos();
                promoted.version = next_version(&promoted.version);
                self.apply_namespace_transaction(DfsNamespaceTransaction {
                    transaction_id: namespace_transaction_id_for_path(
                        "clear-hardlink",
                        &promoted.object_id,
                        &promoted.path,
                    ),
                    file_system_id: promoted.file_system_id.clone(),
                    objects: vec![old_path_tombstone, alias_tombstone, promoted],
                    versions: Vec::new(),
                    created_at: time::OffsetDateTime::now_utc(),
                })?;
                continue;
            }
            let tombstone = DfsObjectMetadata {
                object_id: DfsObjectId::generate(),
                file_system_id: object.file_system_id.clone(),
                path: object.path.clone(),
                kind: DfsObjectKind::Tombstone,
                owner_node_id: self.node_id.clone(),
                current_locations: vec![self.node_id.clone()],
                content_hash: None,
                content_size_bytes: None,
                link_target_object_id: None,
                mode: 0,
                uid: 0,
                gid: 0,
                accessed_at_unix_nanos: 0,
                modified_at_unix_nanos: 0,
                created_at_unix_nanos: 0,
                changed_at_unix_nanos: 0,
                version: next_version(&object.version),
                policy: object.policy.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                last_modified_by_event_id: object.last_modified_by_event_id.clone(),
            };
            self.add_object_metadata(tombstone)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) fn posix_rename_directory_bounded_resolved(
        &mut self,
        from: &str,
        to: &str,
        source: DfsObjectMetadata,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        self.ensure_parent_directory(to)?;
        let child_prefix = subtree_prefix(from);
        if to.starts_with(&child_prefix) {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{from} -> {to}"),
            });
        }
        self.validate_directory_rename_destination(from, to, &source)?;
        let job = self.rename_directory_job_for_paths(from, to, &source)?;
        self.persist_namespace_job(&job)?;
        self.run_rename_directory_job_batch(job, max_steps)
    }

    fn validate_directory_rename_destination(
        &self,
        from: &str,
        to: &str,
        source: &DfsObjectMetadata,
    ) -> Result<()> {
        if source.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: from.to_string(),
            });
        }
        if let Some(destination) = self.lookup_sifs_path(to)?
            && destination.kind != DfsObjectKind::Tombstone
        {
            if destination.object_id == source.object_id {
                return Ok(());
            }
            match destination.kind {
                DfsObjectKind::Directory => {
                    if self.sifs_dir_has_children(to)? {
                        return Err(DfsPosixError::DirectoryNotEmpty {
                            path: to.to_string(),
                        });
                    }
                }
                _ => {
                    return Err(DfsPosixError::NotDirectory {
                        path: to.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    fn rename_directory_job_for_paths(
        &self,
        from: &str,
        to: &str,
        source: &DfsObjectMetadata,
    ) -> Result<DfsNamespaceJobRecord> {
        let job_id = namespace_job_id(
            "rename-directory",
            &source.file_system_id,
            &format!("{from}->{to}"),
            &source.object_id,
            &source.version,
        );
        if let Some(job) = self.namespace_job(&job_id)? {
            return Ok(job);
        }
        let now = time::OffsetDateTime::now_utc();
        Ok(DfsNamespaceJobRecord {
            job_id,
            file_system_id: source.file_system_id.clone(),
            kind: DfsNamespaceJobKind::RenameDirectory,
            status: DfsNamespaceJobStatus::Running,
            root_path: from.to_string(),
            target_path: Some(to.to_string()),
            pending_frames: vec![DfsNamespaceJobFrame::RenameDirectoryStart {
                source_object_id: source.object_id.clone(),
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        })
    }

    fn run_rename_directory_job_batch(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        if job.status == DfsNamespaceJobStatus::Completed {
            return Ok(job);
        }
        if max_steps == 0 {
            job.updated_at = time::OffsetDateTime::now_utc();
            self.persist_namespace_job(&job)?;
            return Ok(job);
        }
        let source_root = normalize_posix_path(&job.root_path)?;
        let target_root = job
            .target_path
            .as_ref()
            .ok_or_else(|| DfsPosixError::InvalidPath {
                path: job.job_id.clone(),
            })
            .and_then(|path| normalize_posix_path(path))?;
        let source_prefix = subtree_prefix(&source_root);
        let target_prefix = subtree_prefix(&target_root);
        let mut steps = 0usize;
        while steps < max_steps {
            let Some(frame) = job.pending_frames.pop() else {
                self.touch_parent_directory_modified(&source_root)?;
                if parent_posix_path(&source_root) != parent_posix_path(&target_root) {
                    self.touch_parent_directory_modified(&target_root)?;
                }
                let now = time::OffsetDateTime::now_utc();
                job.status = DfsNamespaceJobStatus::Completed;
                job.updated_at = now;
                job.completed_at = Some(now);
                self.persist_namespace_job(&job)?;
                return Ok(job);
            };
            match frame {
                DfsNamespaceJobFrame::RenameDirectoryStart { source_object_id } => {
                    steps = steps.saturating_add(1);
                    self.start_rename_directory_job(&source_object_id, &source_root, &target_root)?;
                    job.pending_frames
                        .push(DfsNamespaceJobFrame::RenameDirectoryScan {
                            source_object_id,
                            source_path: source_root.clone(),
                            target_path: target_root.clone(),
                            cursor: None,
                        });
                    job.processed_object_count = job.processed_object_count.saturating_add(1);
                }
                DfsNamespaceJobFrame::RenameDirectoryScan {
                    source_object_id,
                    source_path,
                    target_path,
                    cursor,
                } => {
                    let page_size = (max_steps - steps)
                        .max(1)
                        .min(POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE);
                    let page = self.child_objects_for_sifs_dir_page(
                        &source_path,
                        cursor.as_ref(),
                        page_size,
                    )?;
                    let mut child_dirs = Vec::new();
                    for object in page.objects {
                        steps = steps.saturating_add(1);
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
                        if current.kind == DfsObjectKind::Directory {
                            child_dirs.push((
                                current.object_id.clone(),
                                object_path.clone(),
                                target_child_path.clone(),
                            ));
                            continue;
                        }
                        if current_path == target_child_path {
                            continue;
                        }
                        if current_path != object_path {
                            continue;
                        }
                        if self.move_object_for_rename_job(&current, &target_child_path)? {
                            job.processed_object_count =
                                job.processed_object_count.saturating_add(1);
                        }
                    }
                    if let Some(next_cursor) = page.next_cursor {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::RenameDirectoryScan {
                                source_object_id,
                                source_path: source_path.clone(),
                                target_path: target_path.clone(),
                                cursor: Some(next_cursor),
                            });
                    } else if source_path != source_root {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::RenameDirectoryComplete {
                                source_object_id,
                                source_path: source_path.clone(),
                                target_path: target_path.clone(),
                            });
                    }
                    for (child_object_id, child_source_path, child_target_path) in
                        child_dirs.into_iter().rev()
                    {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::RenameDirectoryScan {
                                source_object_id: child_object_id,
                                source_path: child_source_path,
                                target_path: child_target_path,
                                cursor: None,
                            });
                    }
                }
                DfsNamespaceJobFrame::RenameDirectoryComplete {
                    source_object_id,
                    source_path,
                    target_path,
                } => {
                    steps = steps.saturating_add(1);
                    if self.complete_rename_directory_job(
                        &source_object_id,
                        &source_path,
                        &target_path,
                    )? {
                        job.processed_object_count = job.processed_object_count.saturating_add(1);
                    }
                }
                DfsNamespaceJobFrame::ScanDirectory { .. }
                | DfsNamespaceJobFrame::CompleteDirectory { .. }
                | DfsNamespaceJobFrame::CrossShardAdopt { .. }
                | DfsNamespaceJobFrame::CrossShardComplete { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkAdopt { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkComplete { .. }
                | DfsNamespaceJobFrame::CrossShardDirStart { .. }
                | DfsNamespaceJobFrame::CrossShardDirScan { .. }
                | DfsNamespaceJobFrame::CrossShardDirFinish { .. } => {
                    return Err(DfsPosixError::InvalidPath {
                        path: job.job_id.clone(),
                    });
                }
            }
        }
        job.updated_at = time::OffsetDateTime::now_utc();
        self.persist_namespace_job(&job)?;
        Ok(job)
    }

    fn start_rename_directory_job(
        &mut self,
        source_object_id: &DfsObjectId,
        source_root: &str,
        target_root: &str,
    ) -> Result<()> {
        let Some(mut source) = self.object_by_id(source_object_id)? else {
            return Err(DfsPosixError::NotFound {
                path: source_root.to_string(),
            });
        };
        if source.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: source_root.to_string(),
            });
        }
        if source.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: source_root.to_string(),
            });
        }
        let source_path = normalize_posix_path(&source.path)?;
        if source_path == target_root {
            return Ok(());
        }
        if source_path != source_root {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{source_path} -> {target_root}"),
            });
        }

        let mut transaction_objects = Vec::new();
        if let Some(mut destination) = self.lookup_sifs_path(target_root)?
            && destination.kind != DfsObjectKind::Tombstone
            && destination.object_id != source.object_id
        {
            if destination.kind != DfsObjectKind::Directory {
                return Err(DfsPosixError::NotDirectory {
                    path: target_root.to_string(),
                });
            }
            if self.sifs_dir_has_children(target_root)? {
                return Err(DfsPosixError::DirectoryNotEmpty {
                    path: target_root.to_string(),
                });
            }
            destination.kind = DfsObjectKind::Tombstone;
            destination.version = next_version(&destination.version);
            destination.content_hash = None;
            destination.content_size_bytes = None;
            destination.link_target_object_id = None;
            destination.current_locations = vec![self.node_id.clone()];
            transaction_objects.push(destination);
        }

        let mut old_path_tombstone = source.clone();
        old_path_tombstone.object_id = DfsObjectId::generate();
        old_path_tombstone.path = source_root.to_string();
        old_path_tombstone.kind = DfsObjectKind::Tombstone;
        old_path_tombstone.version = next_version(&source.version);
        old_path_tombstone.content_hash = None;
        old_path_tombstone.content_size_bytes = None;
        old_path_tombstone.link_target_object_id = None;
        old_path_tombstone.current_locations = vec![self.node_id.clone()];

        source.path = target_root.to_string();
        source.changed_at_unix_nanos = now_unix_nanos();
        source.version = next_version(&source.version);
        transaction_objects.insert(0, old_path_tombstone);
        transaction_objects.insert(1, source.clone());
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_path(
                "rename",
                &source.object_id,
                &source.path,
            ),
            file_system_id: source.file_system_id.clone(),
            objects: transaction_objects,
            versions: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        Ok(())
    }

    fn complete_rename_directory_job(
        &mut self,
        source_object_id: &DfsObjectId,
        source_path: &str,
        target_path: &str,
    ) -> Result<bool> {
        let Some(current) = self.object_by_id(source_object_id)? else {
            return Ok(false);
        };
        if current.kind == DfsObjectKind::Tombstone {
            return Ok(false);
        }
        if current.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: source_path.to_string(),
            });
        }
        self.move_object_for_rename_job(&current, target_path)
    }

    fn move_object_for_rename_job(
        &mut self,
        object: &DfsObjectMetadata,
        target_path: &str,
    ) -> Result<bool> {
        let current_path = normalize_posix_path(&object.path)?;
        if current_path == target_path {
            return Ok(false);
        }
        let mut moved = object.clone();
        moved.path = target_path.to_string();
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_path(
                "rename-child",
                &moved.object_id,
                &moved.path,
            ),
            file_system_id: moved.file_system_id.clone(),
            objects: vec![moved],
            versions: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        Ok(true)
    }
}
