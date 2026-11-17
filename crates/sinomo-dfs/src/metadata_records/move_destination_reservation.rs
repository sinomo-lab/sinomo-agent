//! Destination reservation held by the shard that owns a cross-shard
//! directory move's target root.
//!
//! The record is an ordinary namespace job row on the destination shard. Its
//! identity and timestamps derive only from the coordinator job, so the
//! destination's install (atomic with the root adoption) and the
//! coordinator's release name the same row.
use super::*;

/// The coordinator job and roots one destination reservation records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DfsDirectoryMoveDestinationReservation<'a> {
    pub(crate) coordinator_job_id: &'a str,
    pub(crate) source_root: &'a str,
    pub(crate) target_root: &'a str,
}

impl DfsNamespaceJobRecord {
    /// The Running destination reservation this cross-shard directory move
    /// owns on the shard of its target root.
    pub(crate) fn directory_move_destination_reservation_job(
        &self,
    ) -> crate::metadata_error::Result<DfsNamespaceJobRecord> {
        let target_root = match (&self.kind, self.target_path.as_ref()) {
            (DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries, Some(target)) => target,
            _ => {
                return Err(crate::MetadataBackendError::Serde(
                    "only a cross-shard directory move reserves a destination".into(),
                ));
            }
        };
        let identity = format!("{}\0{}\0{}", self.file_system_id, self.job_id, target_root);
        Ok(DfsNamespaceJobRecord {
            job_id: format!(
                "namespace_job_dir-move-destination-reservation_{}",
                blake3::hash(identity.as_bytes()).to_hex()
            ),
            file_system_id: self.file_system_id.clone(),
            origin: None,
            kind: DfsNamespaceJobKind::DirectoryMoveDestinationReservation,
            status: DfsNamespaceJobStatus::Running,
            root_path: target_root.clone(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::DirectoryMoveDestinationReservation {
                coordinator_job_id: self.job_id.clone(),
                source_root: self.root_path.clone(),
                target_root: target_root.clone(),
            }],
            processed_object_count: 0,
            created_at: self.created_at,
            updated_at: self.created_at,
            completed_at: None,
            failure_reason: None,
        })
    }

    /// The reservation this row records, when it is a well-formed
    /// destination reservation of any status.
    pub(crate) fn directory_move_destination_reservation(
        &self,
    ) -> Option<DfsDirectoryMoveDestinationReservation<'_>> {
        let [
            DfsNamespaceJobFrame::DirectoryMoveDestinationReservation {
                coordinator_job_id,
                source_root,
                target_root,
            },
        ] = self.pending_frames.as_slice()
        else {
            return None;
        };
        (self.kind == DfsNamespaceJobKind::DirectoryMoveDestinationReservation
            && self.target_path.is_none()
            && &self.root_path == target_root)
            .then_some(DfsDirectoryMoveDestinationReservation {
                coordinator_job_id,
                source_root,
                target_root,
            })
    }

    /// Whether this cross-shard directory move has completed the frames of
    /// its start and root adoption, so its destination may be reserved.
    pub(crate) fn directory_move_root_adopted(&self) -> bool {
        self.kind == DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
            && self.processed_object_count >= 2
    }

    /// The same reservation, released: Completed at `now`.
    pub(crate) fn released_directory_move_destination_reservation(
        &self,
        now: OffsetDateTime,
    ) -> DfsNamespaceJobRecord {
        let mut released = self.clone();
        released.status = DfsNamespaceJobStatus::Completed;
        released.updated_at = now.max(self.updated_at);
        released.completed_at = Some(released.updated_at);
        released
    }
}
