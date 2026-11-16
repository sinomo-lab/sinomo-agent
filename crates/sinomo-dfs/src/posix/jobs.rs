//! Namespace job helpers for the POSIX facade.
//!
//! This module owns resumable POSIX namespace jobs that are not specific to
//! metadata-shard routing: recursive directory clear, same-shard directory
//! rename, maintenance resume scans, and durable job status updates.

use super::{
    POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE, POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
    POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE, POSIX_RENAME_MOVE_TRANSACTION_MAX_OBJECTS,
    POSIX_RENAME_MOVE_TRANSACTION_MAX_PATH_BYTES, namespace_job_id, now_unix_nanos,
};
use crate::namespace::{
    namespace_transaction_id_for_batch, namespace_transaction_id_for_versioned_path, next_version,
};
use crate::posix_model::{
    DfsNamespaceJobFailure, DfsNamespaceJobMaintenanceReport, DfsPosixError, Result,
};
use crate::posix_path::{
    normalize_posix_path, normalize_posix_path_with_trailing_directory, parent_posix_path,
    subtree_prefix,
};
use crate::{
    DfsError, DfsMessageCommittedWork, DfsMessageTransport, DfsMetadataQueryResult,
    DfsMetadataSelection, DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobOrigin,
    DfsNamespaceJobRecord, DfsNamespaceJobStatus, DfsNamespaceOperation, DfsNamespaceTransaction,
    DfsObjectId, DfsObjectKind, DfsObjectMetadata, NetworkedDfsService, sifs_file_system_id,
};

pub(super) fn namespace_job_kind_is_cross_shard(kind: &DfsNamespaceJobKind) -> bool {
    matches!(
        kind,
        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
            | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
            | DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
            | DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
    )
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn posix_clear_dir_recursive(&mut self, path: &str) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_posix_dot_components_distributed(path)?;
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
        self.check_cancelled("namespace job resume")?;
        let job = if self.metadata_backend.is_none()
            && self.transport.delegates_local_namespace_authority()
            && self.control_plane.is_some()
        {
            // Main owns the durable job. A requester can have no local tables,
            // including immediately after Main accepted its original begin.
            let file_system_id = sifs_file_system_id();
            let mut current = None;
            for shard in self.metadata_shard_authorities(&file_system_id)? {
                let DfsMetadataQueryResult::NamespaceJob { job, .. } = self
                    .query_metadata_authority_impl(
                        &shard,
                        DfsMetadataSelection::NamespaceJob {
                            job_id: job_id.to_owned(),
                        },
                    )?
                else {
                    return Err(DfsError::Metadata {
                        message: "namespace job authority returned the wrong query kind".into(),
                    }
                    .into());
                };
                if let Some(job) = job {
                    if job.job_id != job_id || job.file_system_id != file_system_id {
                        return Err(DfsError::Metadata {
                            message: "namespace job authority returned a foreign job".into(),
                        }
                        .into());
                    }
                    current = Some(*job);
                    break;
                }
            }
            current
        } else {
            self.namespace_job(job_id)?
        };
        let Some(mut job) = job else {
            return Err(DfsPosixError::NotFound {
                path: job_id.to_string(),
            });
        };
        if job.status != DfsNamespaceJobStatus::Running {
            return Ok(job);
        }
        if self.transport.delegates_local_namespace_authority()
            && let Some(primary) = self.routed_namespace_primary_for_path(&job.root_path)?
        {
            let _namespace_plan = self.namespace_execution.begin_planning();
            return self.route_namespace_job_operation(
                primary,
                DfsNamespaceOperation::ResumeNamespaceJobBounded {
                    job_id: job.job_id.clone(),
                    path: job.root_path.clone(),
                    max_steps: u64::try_from(max_steps).map_err(|_| {
                        DfsPosixError::InvalidPath {
                            path: "namespace job step budget exceeds u64".into(),
                        }
                    })?,
                },
            );
        }
        if matches!(&job.kind, DfsNamespaceJobKind::RenameDirectory) {
            // A preempted batch may have sealed moves without updating its job
            // cursor. Recover those exact native bodies before observing the
            // next children, then reload the job from the recovered generation.
            let cancellation = self.cancellation.clone();
            let pass = self.recover_durable_namespace_prepares_pass(&|| {
                cancellation
                    .as_ref()
                    .is_some_and(crate::DfsCancellationFlag::is_cancelled)
            })?;
            // A sealed move that has not committed must not be planned over;
            // the job stays running and resumes once it commits.
            if let Some(unknown) = pass.first_unknown {
                return Err(unknown.into());
            }
            if !pass.completed {
                return Err(crate::DfsError::Cancelled {
                    operation: "namespace job resume",
                }
                .into());
            }
            self.check_cancelled("namespace job resume")?;
            job = self.namespace_job(job_id)?.ok_or_else(|| DfsPosixError::NotFound {
                path: job_id.to_string(),
            })?;
            if job.status != DfsNamespaceJobStatus::Running {
                return Ok(job);
            }
        }
        let _namespace_plan = self.namespace_execution.begin_planning();
        match job.kind {
            DfsNamespaceJobKind::ContentCopy => self
                .run_content_copy_job(job, max_steps)
                .map_err(DfsPosixError::from),
            DfsNamespaceJobKind::ClearDirectoryRecursive => {
                self.run_clear_dir_recursive_job_batch(job, max_steps)
            }
            DfsNamespaceJobKind::RenameDirectory => {
                self.run_rename_directory_job_batch(job, max_steps)
            }
            DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
            | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
            | DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
            | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries => {
                self.run_cross_shard_namespace_job(job, max_steps)
            }
            // This target-side reservation is released only by the
            // authenticated source coordinator after its alias generation is
            // durable. Background maintenance must not expose it early.
            DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary
            | DfsNamespaceJobKind::ParentCountParticipant => Ok(job),
        }
    }

    pub fn posix_resume_namespace_jobs_bounded(
        &mut self,
        max_jobs: usize,
        max_steps_per_job: usize,
    ) -> Result<DfsNamespaceJobMaintenanceReport> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if self.metadata_backend.is_none() {
            return Ok(DfsNamespaceJobMaintenanceReport::default());
        }
        // A shard's primary resumes its own jobs; a local copy of a shard this
        // node does not replicate is not kept current.
        let replicated = self.locally_replicated_sifs_metadata_shards();
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
                if job.status != DfsNamespaceJobStatus::Running
                    || self.path_in_unreplicated_sifs_shard(
                        replicated.as_ref(),
                        &job.file_system_id,
                        &job.root_path,
                    )?
                {
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
                        if self.remote_namespace_phase.is_some() {
                            // Stop at this actual phase; a later job must not
                            // replace its owned source custody. Join its durable
                            // origin using the existing exact request predicate.
                            let origin = job.origin.as_ref().ok_or_else(|| {
                                DfsPosixError::Dfs(crate::DfsError::Metadata {
                                    message: "remote job recovery has no durable request origin"
                                        .into(),
                                })
                            })?;
                            let request = self
                                .metadata_backend
                                .as_ref()
                                .expect("metadata backend checked above")
                                .get_namespace_request(
                                    &origin.requester_node_id,
                                    &origin.request_id,
                                )?
                                .ok_or_else(|| {
                                    DfsPosixError::Dfs(crate::DfsError::Metadata {
                                        message: "remote job recovery lost its durable request"
                                            .into(),
                                    })
                                })?;
                            if request.file_system_id != job.file_system_id
                                || !origin.matches_request(&request)
                                || request.status != crate::DfsNamespaceRequestStatus::Pending
                            {
                                return Err(DfsPosixError::Dfs(crate::DfsError::Metadata {
                                    message: "remote job recovery differs from its durable request"
                                        .into(),
                                }));
                            }
                            self.pending_namespace_request_recovery = Some(Box::new(request));
                            return Err(error);
                        }
                        let reason = error.to_string();
                        let latest = self.namespace_job(&job.job_id)?.unwrap_or(job);
                        match latest.status {
                            DfsNamespaceJobStatus::Completed => {
                                report.completed_job_count =
                                    report.completed_job_count.saturating_add(1);
                            }
                            // A durably prepared step whose commit outcome is
                            // unknown may still commit; the job keeps running.
                            DfsNamespaceJobStatus::Running
                                if namespace_job_kind_is_cross_shard(&latest.kind)
                                    || matches!(
                                        &error,
                                        DfsPosixError::Dfs(
                                            crate::DfsError::Cancelled { .. }
                                                | crate::DfsError::MetadataCommitOutcomeUnknown { .. }
                                        )
                                    ) =>
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let primaries =
            self.routed_namespace_primaries_for_paths_fresh([from.as_str(), to.as_str()])?;
        if primaries.into_iter().any(|primary| primary.is_some()) {
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

    pub fn posix_rename_directory_bounded_routed(
        &mut self,
        from: &str,
        to: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized_from = self.resolve_posix_dot_components_distributed(from)?;
        let normalized_to = self.resolve_posix_dot_components_distributed(to)?;
        let mut primaries = self
            .routed_namespace_primaries_for_paths_fresh([
                normalized_from.as_str(),
                normalized_to.as_str(),
            ])?
            .into_iter();
        let from_primary = primaries.next().unwrap_or(None);
        let to_primary = primaries.next().unwrap_or(None);
        match (from_primary, to_primary) {
            (None, None) => {
                self.posix_rename_directory_bounded(&normalized_from, &normalized_to, max_steps)
            }
            (Some(from_primary), Some(to_primary)) if from_primary == to_primary => self
                .route_namespace_job_operation(
                    from_primary,
                    DfsNamespaceOperation::RenameDirectoryBounded {
                        from: normalized_from,
                        to: normalized_to,
                        max_steps: u64::try_from(max_steps).unwrap_or(u64::MAX),
                    },
                ),
            _ => Err(DfsPosixError::InvalidPath {
                path: format!(
                    "{normalized_from} -> {normalized_to} crosses metadata shard primaries"
                ),
            }),
        }
    }

    pub fn posix_resume_namespace_job_bounded_routed(
        &mut self,
        job_id: &str,
        routing_path: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let routing_path = normalize_posix_path(routing_path)?;
        match self.routed_namespace_primary_for_path(&routing_path)? {
            Some(primary) => self.route_namespace_job_operation(
                primary,
                DfsNamespaceOperation::ResumeNamespaceJobBounded {
                    job_id: job_id.to_string(),
                    path: routing_path,
                    max_steps: u64::try_from(max_steps).unwrap_or(u64::MAX),
                },
            ),
            None => self.posix_resume_namespace_job_bounded(job_id, max_steps),
        }
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
            origin: self
                .namespace_execution
                .request()
                .map(DfsNamespaceJobOrigin::from_request),
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

    pub fn namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        Ok(backend.get_namespace_job(job_id)?)
    }

    /// Publish complete current job DATA through the ordinary prepared kernel.
    /// Callers must establish their import/application authority separately;
    /// this method still acquires the actual current owning-shard guard.
    pub fn publish_namespace_job_data(
        &mut self,
        job: &DfsNamespaceJobRecord,
    ) -> Result<crate::DfsPreparedOperationDigest> {
        if job.status != DfsNamespaceJobStatus::Running {
            return Err(DfsPosixError::Dfs(crate::DfsError::Metadata {
                message: "namespace job DATA publication requires a running current job".into(),
            }));
        }
        let binding = blake3::hash(&serde_json::to_vec(job).map_err(|error| {
            DfsPosixError::Dfs(crate::DfsError::Metadata {
                message: error.to_string(),
            })
        })?)
        .to_hex()
        .to_string();
        let operation_id = format!("namespace-job-data-v7:{binding}");
        let mut route = crate::posix_object::new_posix_metadata_object(
            &self.node_id,
            &job.root_path,
            DfsObjectKind::Directory,
        );
        route.file_system_id = job.file_system_id.clone();
        let scope = self.metadata_effect_scope_for_object(&route, operation_id.clone())?;
        let _execution = self.acquire_prepared_execution(&scope)?;
        let backend = self.metadata_backend.as_ref().ok_or_else(|| {
            DfsPosixError::Dfs(crate::DfsError::Metadata {
                message: "job publication requires a durable current backend".into(),
            })
        })?;
        let stored = backend.prepared_operation_record(&operation_id)?;
        if let Some(stored) = stored.as_ref() {
            if stored.file_system_id != job.file_system_id
                || stored.shard_id != scope.shard_id
                || stored
                    .metadata_effects
                    .iter()
                    .filter_map(|effect| effect.namespace_jobs())
                    .collect::<Vec<_>>()
                    != vec![job]
            {
                return Err(DfsPosixError::Dfs(crate::DfsError::Metadata {
                    message: "job DATA retry changes its actual stored body or scope".into(),
                }));
            }
            if stored.status == crate::DfsPreparedOperationStatus::Committed {
                return Ok(stored.digest.clone());
            }
        } else if backend
            .get_namespace_job(&job.job_id)?
            .is_some_and(|existing| existing != *job)
        {
            return Err(DfsPosixError::Dfs(crate::DfsError::Metadata {
                message: "job DATA collides with an unproven current job generation".into(),
            }));
        }
        let payload = if let Some(stored) = stored {
            (*stored.payload).clone()
        } else {
            crate::DfsPreparedOperationPayload::for_metadata_effects(
                scope,
                Vec::new(),
                Vec::new(),
                vec![crate::DfsPreparedMetadataEffect::PutNamespaceJob { job: job.clone() }],
                job.updated_at,
            )?
        };
        let digest = self.prepare_payload_for_publication(payload)?;
        self.commit_stored_prepared_operation(&digest)?;
        self.record_message_committed_work(DfsMessageCommittedWork {
            publish_metadata: true,
            namespace_maintenance: true,
            repair_maintenance: false,
            metadata_replica_progress: false,
            metadata_materialization: false,
            semantic_changes: false,
        });
        Ok(digest)
    }

    pub(crate) fn persist_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let existing = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| {
                DfsPosixError::Dfs(crate::DfsError::Metadata {
                    message: "namespace jobs require current durable metadata".into(),
                })
            })?
            .get_namespace_job(&job.job_id)?;
        if existing.as_ref() != Some(job) {
            let mut route = crate::posix_object::new_posix_metadata_object(
                &self.node_id,
                &job.root_path,
                DfsObjectKind::Directory,
            );
            route.file_system_id = job.file_system_id.clone();
            let scope = self.metadata_effect_scope_for_object(
                &route,
                format!(
                    "namespace-job:{}:{}",
                    job.job_id,
                    sinomo_core::Id::generate()
                ),
            )?;
            let _execution = self.acquire_prepared_execution(&scope)?;
            self.publish_metadata_effects_on_shard(
                scope,
                Vec::new(),
                Vec::new(),
                vec![crate::DfsPreparedMetadataEffect::PutNamespaceJob { job: job.clone() }],
            )?;
        }
        let persisted = true;
        if persisted {
            // The backend row is the durable owner for a resumable namespace
            // saga. Retain its publication and one reactive maintenance edge
            // before quorum checkpointing or destination RPCs can fail.
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                repair_maintenance: false,
                namespace_maintenance: job.status == DfsNamespaceJobStatus::Running,
                metadata_replica_progress: false,
                metadata_materialization: false,
                semantic_changes: false,
            });
            if job.status != DfsNamespaceJobStatus::Failed
                && job.origin.as_ref().is_some_and(|origin| {
                    self.namespace_execution
                        .request()
                        .is_some_and(|request| origin.matches_request(request))
                })
            {
                self.namespace_execution.record_job();
            }
        }
        if namespace_job_kind_is_cross_shard(&job.kind) {
            // The locally durable running record is already an authority
            // reservation. Install its transient fast-path guard before the
            // quorum checkpoint: if the checkpoint or the first destination
            // RPC fails, concurrent writes must still observe the parked saga.
            self.apply_cross_shard_namespace_job_guard(job)?;
        }
        if namespace_job_kind_is_cross_shard(&job.kind)
            || (job.kind == DfsNamespaceJobKind::ContentCopy && self.control_plane.is_some())
        {
            // The durable job outlives source eviction. Its filesystem/root
            // selects the same real source shard and prefix without assuming
            // this node replicates the destination object.
            let target = self
                .capture_metadata_quorum_sync_target_for_namespace_scope(
                    &job.file_system_id, &job.root_path, &job.job_id,
                )
                .map_err(DfsPosixError::from)?;
            self.sync_metadata_shard_to_quorum(&target, crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT)
                .map_err(DfsPosixError::from)?;
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
                    let page_size =
                        (max_steps - steps).clamp(1, POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE);
                    let page =
                        self.child_objects_for_sifs_dir_page(&path, cursor.as_ref(), page_size)?;
                    let mut child_dirs = Vec::new();
                    let mut revisit_directory = false;
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
                        let (removed, promoted_inside) = self.clear_visible_object_for_recursive_job(&object_path, &prefix)?;
                        revisit_directory |= promoted_inside;
                        if removed {
                            job.processed_object_count = job.processed_object_count.saturating_add(1);
                        }
                    }
                    if revisit_directory {
                        // Promotion consumes one namespace entry but changes
                        // the chosen alias identity. Revisit this actual page
                        // within the unchanged step budget rather than skip a
                        // still-live canonical entry under its old alias id.
                        job.pending_frames.push(DfsNamespaceJobFrame::ScanDirectory {
                            path: path.clone(), cursor: None,
                        });
                    } else if let Some(next_cursor) = page.next_cursor {
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
                        && self.clear_visible_object_for_recursive_job(&path, &prefix)?.0
                    {
                        job.processed_object_count = job.processed_object_count.saturating_add(1);
                    }
                }
                DfsNamespaceJobFrame::ContentCopy { .. }
                | DfsNamespaceJobFrame::RenameDirectoryStart { .. }
                | DfsNamespaceJobFrame::RenameDirectoryScan { .. }
                | DfsNamespaceJobFrame::RenameDirectoryComplete { .. }
                | DfsNamespaceJobFrame::CrossShardAdopt { .. }
                | DfsNamespaceJobFrame::CrossShardComplete { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkAdopt { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkComplete { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit { .. }
                | DfsNamespaceJobFrame::HardlinkTargetFenceReservation { .. }
                | DfsNamespaceJobFrame::ParentCountParticipant { .. }
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
    ) -> Result<(bool, bool)> {
        let Some(object) = self.lookup_sifs_path(object_path)? else { return Ok((false, false)); };
        if object.kind == DfsObjectKind::Tombstone { return Ok((false, false)); }
        if object.kind == DfsObjectKind::Directory {
            self.posix_rmdir_local(object_path)?;
            return Ok((true, false));
        }
        // The ordinary owner reports the alias it actually promoted; a second
        // discovery could choose another identity and misdirect the revisit.
        let (_, promoted) = self.posix_unlink_resolved_object_with_promotion(object_path.to_owned(), object)?;
        let promoted_inside = promoted.is_some_and(|path| path.starts_with(prefix));
        Ok((true, promoted_inside))
    }

    pub(super) fn posix_rename_directory_bounded_resolved(
        &mut self,
        from: &str,
        to: &str,
        source: DfsObjectMetadata,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
            origin: self
                .namespace_execution
                .request()
                .map(DfsNamespaceJobOrigin::from_request),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
            .and_then(|path| self.resolve_posix_dot_components_distributed(path))?;
        let source_prefix = subtree_prefix(&source_root);
        let target_prefix = subtree_prefix(&target_root);
        let mut steps = 0usize;
        // Child moves are grouped into fixed-size durable transactions within
        // the bounded job batch. Each move commits before the updated frames
        // are persisted, so a crash in between safely replays the old frame:
        // already-moved objects no longer appear below the source path and are
        // skipped by id/path checks.
        let mut pending_moves = Vec::new();
        while steps < max_steps {
            let Some(frame) = job.pending_frames.pop() else {
                self.flush_rename_directory_job_move_batches(&mut job, &mut pending_moves, true)?;
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
                    let page_size =
                        (max_steps - steps).clamp(1, POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE);
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
                        if let Some(moved) =
                            self.prepare_object_for_rename_job(&current, &target_child_path)?
                        {
                            pending_moves.push(moved);
                            self.flush_rename_directory_job_move_batches(
                                &mut job,
                                &mut pending_moves,
                                false,
                            )?;
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
                    if let Some(moved) = self.prepare_completed_rename_directory_job(
                        &source_object_id,
                        &source_path,
                        &target_path,
                    )? {
                        pending_moves.push(moved);
                        self.flush_rename_directory_job_move_batches(
                            &mut job,
                            &mut pending_moves,
                            false,
                        )?;
                    }
                }
                DfsNamespaceJobFrame::ContentCopy { .. }
                | DfsNamespaceJobFrame::ScanDirectory { .. }
                | DfsNamespaceJobFrame::CompleteDirectory { .. }
                | DfsNamespaceJobFrame::CrossShardAdopt { .. }
                | DfsNamespaceJobFrame::CrossShardComplete { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkAdopt { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkComplete { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { .. }
                | DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit { .. }
                | DfsNamespaceJobFrame::HardlinkTargetFenceReservation { .. }
                | DfsNamespaceJobFrame::ParentCountParticipant { .. }
                | DfsNamespaceJobFrame::CrossShardDirStart { .. }
                | DfsNamespaceJobFrame::CrossShardDirScan { .. }
                | DfsNamespaceJobFrame::CrossShardDirFinish { .. } => {
                    return Err(DfsPosixError::InvalidPath {
                        path: job.job_id.clone(),
                    });
                }
            }
        }
        self.flush_rename_directory_job_move_batches(&mut job, &mut pending_moves, true)?;
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let mut replaces_directory = false;
        if let Some(destination) = self.lookup_sifs_path(target_root)?
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
            replaces_directory = true;
            transaction_objects.push(self.tombstone_path_object(destination));
        }

        let mut old_path_tombstone = source.clone();
        old_path_tombstone.object_id = DfsObjectId::generate();
        let old_path_tombstone =
            self.tombstone_path_object_at(old_path_tombstone, source_root.to_string());

        source.path = target_root.to_string();
        source.changed_at_unix_nanos = now_unix_nanos();
        source.version = next_version(&source.version);
        transaction_objects.insert(0, old_path_tombstone);
        transaction_objects.insert(1, source.clone());
        // Count changes share the exact root-entry move. Its unchanged replay
        // exits above once the original inode is already at target_root.
        let old_parent = parent_posix_path(source_root);
        let new_parent = parent_posix_path(target_root);
        if old_parent == new_parent {
            if replaces_directory && let Some(parent_path) = old_parent.as_deref() {
                let mut parent = self.directory_modified_metadata(parent_path)?;
                parent.namespace_link_count = Some(super::metadata::updated_namespace_link_count(&parent, -1)?);
                transaction_objects.push(parent);
            }
        } else {
            if let Some(parent_path) = old_parent.as_deref() {
                let mut parent = self.directory_modified_metadata(parent_path)?;
                parent.namespace_link_count = Some(super::metadata::updated_namespace_link_count(&parent, -1)?);
                transaction_objects.push(parent);
            }
            if let Some(parent_path) = new_parent.as_deref() {
                let mut parent = self.directory_modified_metadata(parent_path)?;
                parent.namespace_link_count = Some(super::metadata::updated_namespace_link_count(&parent, if replaces_directory { 0 } else { 1 })?);
                transaction_objects.push(parent);
            }
        }
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            content_reference_identity: None,
            extension_expectations: Vec::new(),
            contribution_digest: None,
            file_version_pins: None,
            transaction_id: namespace_transaction_id_for_versioned_path(
                "rename",
                &source.object_id,
                &source.version,
                &source.path,
            ),
            file_system_id: source.file_system_id.clone(),
            objects: transaction_objects,
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        Ok(())
    }

    fn prepare_completed_rename_directory_job(
        &self,
        source_object_id: &DfsObjectId,
        source_path: &str,
        target_path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let Some(current) = self.object_by_id(source_object_id)? else {
            return Ok(None);
        };
        if current.kind == DfsObjectKind::Tombstone {
            return Ok(None);
        }
        if current.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: source_path.to_string(),
            });
        }
        self.prepare_object_for_rename_job(&current, target_path)
    }

    fn prepare_object_for_rename_job(
        &self,
        object: &DfsObjectMetadata,
        target_path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let current_path = normalize_posix_path(&object.path)?;
        if current_path == target_path {
            return Ok(None);
        }
        let mut moved = object.clone();
        moved.path = target_path.to_string();
        Ok(Some(moved))
    }

    fn apply_rename_directory_job_move_batch(
        &mut self,
        job: &DfsNamespaceJobRecord,
        moved_objects: Vec<DfsObjectMetadata>,
    ) -> Result<u64> {
        if moved_objects.is_empty() {
            return Ok(0);
        }
        let moved_count = moved_objects.len() as u64;
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            content_reference_identity: None,
            extension_expectations: Vec::new(),
            contribution_digest: None,
            file_version_pins: None,
            // Descendants retain their inode generation when an ancestor is
            // renamed. Visiting the same destination in another job therefore
            // needs a distinct id even when the child payload is identical.
            transaction_id: format!(
                "{}:{}",
                namespace_transaction_id_for_batch(
                    "rename-child-batch",
                    &job.file_system_id,
                    &moved_objects,
                    &[],
                ),
                job.job_id
            ),
            file_system_id: job.file_system_id.clone(),
            objects: moved_objects,
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: job.created_at,
        })?;
        Ok(moved_count)
    }

    fn flush_rename_directory_job_move_batches(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        pending_moves: &mut Vec<DfsObjectMetadata>,
        flush_partial: bool,
    ) -> Result<()> {
        loop {
            let batch_len = rename_directory_job_move_batch_len(
                pending_moves.iter().map(|object| object.path.as_str()),
            );
            if batch_len == 0
                || (!flush_partial
                    && batch_len == pending_moves.len()
                    && batch_len < POSIX_RENAME_MOVE_TRANSACTION_MAX_OBJECTS)
            {
                break;
            }
            let remaining = pending_moves.split_off(batch_len);
            let moved = self.apply_rename_directory_job_move_batch(
                job,
                std::mem::replace(pending_moves, remaining),
            )?;
            job.processed_object_count = job.processed_object_count.saturating_add(moved);
        }
        Ok(())
    }
}

fn rename_directory_job_move_batch_len<'a>(paths: impl IntoIterator<Item = &'a str>) -> usize {
    let mut count = 0usize;
    let mut path_bytes = 0usize;
    for path in paths
        .into_iter()
        .take(POSIX_RENAME_MOVE_TRANSACTION_MAX_OBJECTS)
    {
        let next_path_bytes = path.len();
        if count > 0
            && path_bytes.saturating_add(next_path_bytes)
                > POSIX_RENAME_MOVE_TRANSACTION_MAX_PATH_BYTES
        {
            break;
        }
        count = count.saturating_add(1);
        path_bytes = path_bytes.saturating_add(next_path_bytes);
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rename_move_batch_len_caps_aggregate_path_bytes_but_allows_one_oversized_path() {
        let more_than_half = POSIX_RENAME_MOVE_TRANSACTION_MAX_PATH_BYTES / 2 + 1;
        let first = "a".repeat(more_than_half);
        let second = "b".repeat(more_than_half);
        assert_eq!(
            rename_directory_job_move_batch_len([first.as_str(), second.as_str()]),
            1
        );

        let oversized = "x".repeat(POSIX_RENAME_MOVE_TRANSACTION_MAX_PATH_BYTES + 1);
        assert_eq!(
            rename_directory_job_move_batch_len([oversized.as_str(), "/next"]),
            1,
            "an oversized first object remains a valid one-object batch"
        );
        assert_eq!(rename_directory_job_move_batch_len([oversized.as_str()]), 1);
    }
}
