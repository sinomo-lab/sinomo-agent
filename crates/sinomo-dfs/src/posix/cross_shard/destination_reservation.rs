//! Destination reservation of a cross-shard directory move.
//!
//! The shard that owns a move's target root holds a reservation row from the
//! root adoption, which installs it in the same transaction, until the
//! coordinator releases it: in the finish frame once the whole subtree has
//! moved, or in the abort frame when the move fails after its root moved.
//! While the row is Running, namespace mutations and content writes at or
//! under the target root on that shard return `Busy`; reads are never
//! checked. The move's own child adoptions are admitted by the coordinator
//! job their source offer names, which the destination's prepare then claims
//! from the live source custody.
//!
//! The row lives on the destination shard itself, so its primary enforces it
//! whether or not it replicates the source shard, and the check reads only
//! that local row.
use super::*;

fn dfs_error(error: DfsPosixError) -> DfsError {
    match error {
        DfsPosixError::Dfs(error) => error,
        DfsPosixError::Metadata(error) => error.into(),
        other => DfsError::Metadata {
            message: other.to_string(),
        },
    }
}

/// The paths a routed namespace operation would create, change or remove
/// under a reserved destination. Reads, durability barriers, lock commands,
/// parent touches and the coordination phases of other jobs return none.
fn reserved_mutation_paths(operation: &DfsNamespaceOperation) -> Vec<&str> {
    use DfsNamespaceOperation as Op;
    match operation {
        Op::Mkdir { .. }
        | Op::MkdirWithAttrs { .. }
        | Op::MkdirAtNoLinks { .. }
        | Op::MkdirAtNoLinksWithAttrs(_)
        | Op::MkdirAll { .. }
        | Op::Rmdir { .. }
        | Op::ConditionalRmdir { .. }
        | Op::Unlink { .. }
        | Op::ConditionalUnlink { .. }
        | Op::Rename { .. }
        | Op::ConditionalRename { .. }
        | Op::RenameSingleNamespaceAtomic { .. }
        | Op::RenameDirectoryBounded { .. }
        | Op::Hardlink { .. }
        | Op::SetMode { .. }
        | Op::SetOwner { .. }
        | Op::SetSymlinkOwner { .. }
        | Op::SetTimes { .. }
        | Op::ClearDirRecursive { .. }
        | Op::CommitLocalEventualBatch { .. }
        | Op::AdoptHardlinkAlias { .. }
        | Op::BeginContentCopy { .. }
        // Another job's first effect under the destination waits; its later,
        // forward-only phases can only follow an effect admitted before.
        | Op::InstallHardlinkTargetFence { .. }
        | Op::ParentCountParticipant {
            action: crate::DfsParentCountAction::Reserve,
            ..
        } => operation.involved_paths(),
        // The move's own adoptions are admitted by their bound job inside
        // the adoption itself.
        Op::AdoptObjectAtPath { .. }
        | Op::FinalizeObjectFamilyAdoption { .. }
        | Op::VerifyObjectFamilyAdoption { .. }
        | Op::CommitHardlinkTargetFence { .. }
        | Op::ParentCountParticipant { .. }
        | Op::ReleaseDirectoryMoveDestination { .. }
        | Op::MetadataQuery { .. }
        | Op::ContentReference { .. }
        | Op::PollNamespaceChanges { .. }
        | Op::ResumeNamespaceJobBounded { .. }
        | Op::RecordAccess { .. }
        | Op::TouchDirectoryModified { .. }
        | Op::LookupPath { .. }
        | Op::LookupPaths { .. }
        | Op::ReconcilePlacement { .. }
        | Op::AbortPendingWrite { .. }
        | Op::AdvisoryLock { .. }
        | Op::FileSync { .. }
        | Op::DirectorySync { .. } => Vec::new(),
    }
}

#[cfg(test)]
static SCAN_FAILURE_TEST_HOOK: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Fail the scan of the moved directory at `source_path` with a structured
/// conflict, which aborts the move after its root moved.
#[cfg(test)]
pub(crate) fn fail_directory_move_scan_of(source_path: impl Into<String>) {
    let previous = SCAN_FAILURE_TEST_HOOK
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .replace(source_path.into());
    assert!(previous.is_none(), "scan failure hook already installed");
}

/// The structured failure armed for the scan of `source_path`, once.
#[cfg(test)]
pub(super) fn directory_move_scan_test_failure(source_path: &str) -> Result<()> {
    let mut slot = SCAN_FAILURE_TEST_HOOK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if slot.as_deref() == Some(source_path) {
        slot.take();
        return Err(DfsPosixError::Conflict {
            path: source_path.to_owned(),
            reason: "injected directory-move scan conflict".into(),
        });
    }
    Ok(())
}

/// Which reservation, if any, an effect belongs to.
#[derive(Clone, Copy)]
enum ReservationAdmission<'a> {
    /// An ordinary mutation: every Running reservation refuses it.
    None,
    /// An adoption whose source offer names this coordinator job.
    Job(&'a str),
    /// A rename naming exactly a move's source and target roots, which only
    /// resumes or replays that move.
    Move {
        source_root: &'a str,
        target_root: &'a str,
    },
}

impl ReservationAdmission<'_> {
    fn admits(self, reservation: &DfsNamespaceJobRecord) -> bool {
        let Some(recorded) = reservation.directory_move_destination_reservation() else {
            return true;
        };
        match self {
            Self::None => false,
            Self::Job(job_id) => job_id == recorded.coordinator_job_id,
            Self::Move {
                source_root,
                target_root,
            } => {
                crate::posix_path::normalized_posix_path_key(recorded.source_root).as_deref()
                    == Some(source_root)
                    && crate::posix_path::normalized_posix_path_key(recorded.target_root).as_deref()
                        == Some(target_root)
            }
        }
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Admit an adoption at `path` on its destination shard: the move whose
    /// job the installed source offer names passes its own reservation.
    pub(super) fn admit_directory_move_adoption(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<()> {
        let admitted_job = self.source_adoption.offer().map(|offer| offer.job_id);
        let admission = admitted_job
            .as_deref()
            .map_or(ReservationAdmission::None, ReservationAdmission::Job);
        Ok(self.admit_under_directory_move_reservations(file_system_id, path, admission)?)
    }

    /// `Busy` while a running cross-shard directory move reserves `path` on
    /// the shard that owns it, when this node is that shard's primary.
    pub(crate) fn ensure_path_not_reserved_by_directory_move(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> std::result::Result<(), DfsError> {
        self.admit_under_directory_move_reservations(
            file_system_id,
            path,
            ReservationAdmission::None,
        )
    }

    /// [`Self::ensure_path_not_reserved_by_directory_move`] for every path a
    /// routed namespace mutation names. The rename that owns a move resumes
    /// it through its own reservation.
    pub(crate) fn ensure_namespace_operation_not_reserved_by_directory_move(
        &mut self,
        operation: &DfsNamespaceOperation,
    ) -> Result<()> {
        let roots = match operation {
            DfsNamespaceOperation::Rename { from, to }
            | DfsNamespaceOperation::ConditionalRename { from, to, .. }
            | DfsNamespaceOperation::RenameDirectoryBounded { from, to, .. } => {
                crate::posix_path::normalized_posix_path_key(from)
                    .zip(crate::posix_path::normalized_posix_path_key(to))
            }
            _ => None,
        };
        let admission =
            roots
                .as_ref()
                .map_or(ReservationAdmission::None, |(source_root, target_root)| {
                    ReservationAdmission::Move {
                        source_root,
                        target_root,
                    }
                });
        let file_system_id = sifs_file_system_id();
        for path in reserved_mutation_paths(operation) {
            self.admit_under_directory_move_reservations(&file_system_id, path, admission)?;
        }
        Ok(())
    }

    fn admit_under_directory_move_reservations(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        admission: ReservationAdmission<'_>,
    ) -> std::result::Result<(), DfsError> {
        if self.metadata_backend.is_none() {
            return Ok(());
        }
        let Some(path) = crate::posix_path::normalized_posix_path_key(path) else {
            return Ok(());
        };
        // Only the shard's primary admits mutations; it holds the row. A
        // path whose assignment cannot be read fails in its own routing.
        let Ok(shards) = self.namespace_shards_for_exact_paths(file_system_id, [path.as_str()])
        else {
            return Ok(());
        };
        let Some(shard) = shards
            .into_iter()
            .next()
            .filter(|shard| shard.primary_node_id == self.node_id)
        else {
            return Ok(());
        };
        self.admit_on_reserving_shard(
            &DfsMetadataShardId::new(shard.shard_id),
            file_system_id,
            &path,
            admission,
        )
    }

    /// [`Self::ensure_path_not_reserved_by_directory_move`] on `shard_id`,
    /// which the caller has established this node is the primary of.
    pub(crate) fn ensure_path_not_reserved_on_primary_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> std::result::Result<(), DfsError> {
        if self.metadata_backend.is_none() {
            return Ok(());
        }
        let Some(path) = crate::posix_path::normalized_posix_path_key(path) else {
            return Ok(());
        };
        self.admit_on_reserving_shard(shard_id, file_system_id, &path, ReservationAdmission::None)
    }

    /// Refuse an effect at `path` that a Running reservation on `shard_id`
    /// does not admit. A lane whose backend serves another physical shard
    /// cannot execute the effect and leaves it to that shard's own lane.
    fn admit_on_reserving_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        path: &str,
        admission: ReservationAdmission<'_>,
    ) -> std::result::Result<(), DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "destination reservation check has no metadata backend".into(),
            })?;
        if !matches!(
            backend.metadata_shard_id_for_path(file_system_id, path),
            Ok(Some(local)) if local == *shard_id
        ) {
            return Ok(());
        }
        let covering =
            backend.directory_move_destination_reservations(shard_id, file_system_id, path)?;
        if covering
            .iter()
            .any(|reservation| !admission.admits(reservation))
        {
            return Err(DfsError::Busy {
                path: path.to_owned(),
            });
        }
        Ok(())
    }

    /// Complete the exact Running reservation on this node, the primary of
    /// its shard. Releasing an absent or already completed row succeeds.
    pub(crate) fn release_directory_move_destination_local(
        &mut self,
        reservation: &DfsNamespaceJobRecord,
    ) -> std::result::Result<(), DfsError> {
        if reservation
            .directory_move_destination_reservation()
            .is_none()
            || reservation.status != DfsNamespaceJobStatus::Running
        {
            return Err(DfsError::Metadata {
                message: "destination release does not carry a running reservation".into(),
            });
        }
        let current = self.namespace_job(&reservation.job_id).map_err(dfs_error)?;
        let Some(current) = current else {
            return Ok(());
        };
        if current.status != DfsNamespaceJobStatus::Running {
            return Ok(());
        }
        if current != *reservation {
            return Err(DfsError::Metadata {
                message: "destination release names a different reservation".into(),
            });
        }
        let released = current
            .released_directory_move_destination_reservation(time::OffsetDateTime::now_utc());
        let _release = self.namespace_execution.begin_independent_planning();
        self.persist_namespace_job(&released).map_err(dfs_error)
    }

    /// The reservation a root adoption installs with the moved root, when
    /// `path` is the target root of the bound coordinator job.
    pub(super) fn directory_move_destination_reservation_for_root(
        coordinator: &DfsNamespaceJobRecord,
        path: &str,
    ) -> Result<Option<DfsNamespaceJobRecord>> {
        let is_root = coordinator.target_path.as_deref().is_some_and(|target| {
            crate::posix_path::normalized_posix_path_key(target).as_deref() == Some(path)
        });
        if !is_root {
            return Ok(None);
        }
        Ok(Some(
            coordinator.directory_move_destination_reservation_job()?,
        ))
    }

    /// Turn a structured failure of `failed`, a frame of a running cross-shard
    /// directory move, into its durable abort frame: the job keeps only its
    /// finish frame, which carries the original count plan, under the abort
    /// frame that releases the destination and then fails the job. The
    /// caller re-runs the job.
    pub(super) fn begin_directory_move_abort(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        failed: DfsNamespaceJobFrame,
        failure: &DfsPosixError,
    ) -> Result<()> {
        job.pending_frames.push(failed);
        job.pending_frames
            .retain(|frame| matches!(frame, DfsNamespaceJobFrame::CrossShardDirFinish { .. }));
        job.pending_frames
            .push(DfsNamespaceJobFrame::CrossShardDirAbort {
                failure_reason: failure.to_string(),
            });
        job.updated_at = time::OffsetDateTime::now_utc();
        self.persist_namespace_job(job)
    }

    /// Run the abort frame popped from `job`: release the destination, then
    /// fail the job with the recorded reason. A failed or yielded release
    /// keeps the frame for the next run.
    pub(super) fn run_directory_move_abort(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        frame: DfsNamespaceJobFrame,
    ) -> Result<DfsNamespaceJobRecord> {
        let DfsNamespaceJobFrame::CrossShardDirAbort { failure_reason } = &frame else {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: "directory-move abort ran another frame".into(),
            }));
        };
        let failure_reason = failure_reason.clone();
        match self.release_directory_move_destination(&job) {
            Ok(()) => self.fail_namespace_job(job, failure_reason),
            // The yielded phase owns the durable row, which still holds this
            // frame; it re-runs once the phase completes.
            Err(error) if self.remote_namespace_phase.is_some() => Err(error),
            Err(error) => {
                job.pending_frames.push(frame);
                job.updated_at = time::OffsetDateTime::now_utc();
                self.persist_namespace_job(&job)?;
                Err(error)
            }
        }
    }

    /// Release the coordinator's destination reservation: through the
    /// current primary of the target root's shard, or locally when this node
    /// is that primary.
    pub(super) fn release_directory_move_destination(
        &mut self,
        job: &DfsNamespaceJobRecord,
    ) -> Result<()> {
        let reservation = job.directory_move_destination_reservation_job()?;
        let destination = self
            .namespace_shards_for_exact_paths(
                &reservation.file_system_id,
                [reservation.root_path.as_str()],
            )?
            .into_iter()
            .next();
        match destination {
            Some(destination) if destination.primary_node_id != self.node_id => self
                .route_cross_shard_job_phase(
                    Some(job),
                    "release-directory-move-destination",
                    destination.primary_node_id,
                    DfsNamespaceOperation::ReleaseDirectoryMoveDestination {
                        reservation: Box::new(reservation),
                    },
                ),
            _ => Ok(self.release_directory_move_destination_local(&reservation)?),
        }
    }
}
