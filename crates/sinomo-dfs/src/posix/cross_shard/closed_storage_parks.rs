//! Steps of a cross-shard namespace job whose effect lands on another shard
//! while that shard's storage is closed.
//!
//! A step addressed to a shard whose storage record names a conversion is
//! not run, and a step that shard's primary refuses because its storage is
//! closed (its storage record names a conversion, or its root awaits storage
//! preparation) took no effect there. Either way the job stays Running at
//! that step and parks on the shard, as a directory scan does when a carried
//! alias's target shard is closed: the refusal records the shard for this
//! node in `maintenance::storage_waits` and announces the park, so the
//! coordinator reports the job waiting on that shard and the shard's
//! activation wakes it. The shard's conversion then reads the job as parked
//! on it instead of waiting for it.
//!
//! The steps covered, each with the shard it addresses:
//!
//! - the adoption of a moved object (`CrossShardAdopt`) and of a hardlink
//!   alias (`CrossShardHardlinkAdopt`): the destination path's shard;
//! - the install and the commit of a hardlink target fence: the canonical
//!   target's shard;
//! - an alias commit: the alias shard it names;
//! - the release of a directory move's destination reservation in its finish
//!   and abort frames: the target root's shard;
//! - every phase (reserve, apply, abort) of a parent-count participant: the
//!   participant's parent shard.
//!
//! A step on the job's own coordinator shard is never parked here: that
//! shard's primary runs the job only while it serves.
use super::*;

/// Whether `error` is a shard's refusal because its storage is closed (see
/// [`crate::error::dfs_error_is_closed_shard_storage`]).
pub(super) fn posix_error_is_closed_shard_storage(error: &DfsPosixError) -> bool {
    match error {
        DfsPosixError::Dfs(error) => crate::error::dfs_error_is_closed_shard_storage(error),
        DfsPosixError::Metadata(error) => {
            crate::metadata_sharded_backend::metadata_shard_storage_is_closed(error)
                || crate::error::dfs_error_is_closed_shard_storage(&DfsError::Metadata {
                    message: error.to_string(),
                })
        }
        _ => false,
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// The shard, other than `job`'s coordinator shard, that `frame`'s step
    /// addresses, with the path it addresses there.
    pub(super) fn namespace_step_shard(
        &mut self,
        job: &DfsNamespaceJobRecord,
        frame: &DfsNamespaceJobFrame,
    ) -> Result<Option<(crate::DfsMetadataShardConfig, String)>> {
        let path = match frame {
            DfsNamespaceJobFrame::CrossShardAdopt { moved_object, .. } => {
                moved_object.path.clone()
            }
            DfsNamespaceJobFrame::CrossShardHardlinkAdopt { alias, .. } => alias.path.clone(),
            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceInstall { fence, .. } => {
                fence.root_path.clone()
            }
            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                committed_target,
                ..
            } => committed_target.path.clone(),
            DfsNamespaceJobFrame::CrossShardDirFinish { .. }
            | DfsNamespaceJobFrame::CrossShardDirAbort { .. }
                if job.kind == DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries =>
            {
                let Some(target) = job.target_path.clone() else {
                    return Ok(None);
                };
                target
            }
            DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { shard_id, .. } => {
                let shard = self
                    .metadata_shard_authorities(&job.file_system_id)?
                    .into_iter()
                    .find(|shard| shard.shard_id == shard_id.0);
                return Ok(shard
                    .filter(|shard| !self.is_namespace_job_coordinator_shard(job, shard))
                    .map(|shard| (shard, job.root_path.clone())));
            }
            _ => return Ok(None),
        };
        self.namespace_path_shard_beside_coordinator(job, &path)
    }

    /// The shard that owns `path`, unless it is `job`'s coordinator shard.
    pub(super) fn namespace_path_shard_beside_coordinator(
        &mut self,
        job: &DfsNamespaceJobRecord,
        path: &str,
    ) -> Result<Option<(crate::DfsMetadataShardConfig, String)>> {
        let shard = self
            .namespace_shards_for_exact_paths(&job.file_system_id, [path])?
            .into_iter()
            .next();
        Ok(shard
            .filter(|shard| !self.is_namespace_job_coordinator_shard(job, shard))
            .map(|shard| (shard, path.to_owned())))
    }

    pub(super) fn is_namespace_job_coordinator_shard(
        &mut self,
        job: &DfsNamespaceJobRecord,
        shard: &crate::DfsMetadataShardConfig,
    ) -> bool {
        self.namespace_shards_for_exact_paths(&job.file_system_id, [job.root_path.as_str()])
            .ok()
            .and_then(|shards| shards.into_iter().next())
            .is_some_and(|coordinator| coordinator.shard_id == shard.shard_id)
    }

    /// The shard `frame`'s step addresses, when that shard's storage is
    /// closed: the step must not run until that shard activates.
    pub(super) fn closed_namespace_step_shard(
        &mut self,
        job: &DfsNamespaceJobRecord,
        frame: &DfsNamespaceJobFrame,
    ) -> Result<Option<(crate::DfsMetadataShardConfig, String)>> {
        let Some((shard, path)) = self.namespace_step_shard(job, frame)? else {
            return Ok(None);
        };
        Ok(self
            .shard_storage_record_is_closed(&shard)?
            .then_some((shard, path)))
    }

    /// Before `frame`'s step runs, which `run_cross_shard_namespace_job` has
    /// popped from `job`: when the shard it addresses converts, keep the
    /// frame next in the durable job and park there.
    pub(super) fn park_step_on_converting_shard(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        frame: &DfsNamespaceJobFrame,
    ) -> Result<()> {
        let Some((shard, path)) = self.closed_namespace_step_shard(job, frame)? else {
            return Ok(());
        };
        job.pending_frames.push(frame.clone());
        job.updated_at = time::OffsetDateTime::now_utc();
        self.persist_namespace_job(job)?;
        Err(self.park_namespace_step_on_shard_storage(job, frame, vec![shard], path))
    }

    /// Park `job` at its next frame `frame` on `shards`, whose storage is
    /// closed: record the shards for this node, so their activation wakes
    /// its maintenance, record and announce the park, and refuse with `Busy`
    /// naming `path`.
    pub(super) fn park_namespace_step_on_shard_storage(
        &mut self,
        job: &DfsNamespaceJobRecord,
        frame: &DfsNamespaceJobFrame,
        shards: Vec<crate::DfsMetadataShardConfig>,
        path: String,
    ) -> DfsPosixError {
        self.note_namespace_job_parked(job, frame, &shards);
        crate::maintenance::storage_waits::record_parked_on_shard_storage(
            &self.node_id,
            shards.into_iter().map(|shard| {
                (
                    shard.file_system_id,
                    crate::DfsMetadataShardId::new(shard.shard_id),
                )
            }),
        );
        DfsPosixError::Dfs(DfsError::Busy { path })
    }

    /// The park of `job` at its next frame `frame` when `error` is the
    /// closed-storage refusal of the shard `frame`'s step addresses.
    pub(super) fn park_refused_namespace_step(
        &mut self,
        job: &DfsNamespaceJobRecord,
        frame: &DfsNamespaceJobFrame,
        error: &DfsPosixError,
    ) -> Result<Option<DfsPosixError>> {
        if !posix_error_is_closed_shard_storage(error) {
            return Ok(None);
        }
        let Some((shard, path)) = self.namespace_step_shard(job, frame)? else {
            return Ok(None);
        };
        Ok(Some(self.park_namespace_step_on_shard_storage(
            job,
            frame,
            vec![shard],
            path,
        )))
    }
}
