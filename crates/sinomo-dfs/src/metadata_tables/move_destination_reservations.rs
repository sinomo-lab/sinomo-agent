//! Derived index of running directory-move destination reservations.
//!
//! The index is rebuilt from durable namespace job rows on every apply,
//! snapshot load and rollback, like the parent-count reservation index. It
//! grants nothing: the service checks it before admitting a mutation.
use super::*;

fn reservation_key(job: &DfsNamespaceJobRecord) -> Option<(DfsFileSystemId, String)> {
    let reservation = job.directory_move_destination_reservation()?;
    let root = crate::posix_path::normalized_posix_path_key(reservation.target_root)?;
    Some((job.file_system_id.clone(), root))
}

impl DfsMetadataTables {
    pub(super) fn remove_move_destination_reservation_index(&mut self, job_id: &str) {
        let Some(key) = self.namespace_jobs.get(job_id).and_then(reservation_key) else {
            return;
        };
        if let Some(ids) = self.move_destination_reservations.get_mut(&key) {
            ids.remove(job_id);
            if ids.is_empty() {
                self.move_destination_reservations.remove(&key);
            }
        }
    }

    pub(super) fn insert_move_destination_reservation_index(&mut self, job_id: &str) {
        let Some(job) = self.namespace_jobs.get(job_id) else {
            return;
        };
        if job.status != DfsNamespaceJobStatus::Running {
            return;
        }
        let Some(key) = reservation_key(job) else {
            return;
        };
        self.move_destination_reservations
            .entry(key)
            .or_default()
            .insert(job_id.to_owned());
    }

    pub(super) fn rebuild_move_destination_reservation_index(&mut self) {
        self.move_destination_reservations.clear();
        let ids = self.namespace_jobs.keys().cloned().collect::<Vec<_>>();
        for id in ids {
            self.insert_move_destination_reservation_index(&id);
        }
    }

    /// A reservation row must keep its exact single-marker shape, so the
    /// index never silently drops a malformed one.
    pub(super) fn validate_move_destination_reservation(job: &DfsNamespaceJobRecord) -> Result<()> {
        if job.directory_move_destination_reservation().is_none()
            || job.processed_object_count != 0
            || job.status == DfsNamespaceJobStatus::Failed
        {
            return Err(MetadataBackendError::Serde(
                "directory-move destination reservation changes its declared shape".into(),
            ));
        }
        Ok(())
    }

    /// Running reservations whose reserved subtree contains `path`.
    pub(crate) fn move_destination_reservations_covering(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Vec<DfsNamespaceJobRecord>> {
        if self.move_destination_reservations.is_empty() {
            return Ok(Vec::new());
        }
        let mut covering = Vec::new();
        let mut current = crate::posix_path::normalized_posix_path_key(path);
        while let Some(candidate) = current {
            if let Some(ids) = self
                .move_destination_reservations
                .get(&(file_system_id.clone(), candidate.clone()))
            {
                for id in ids {
                    covering.push(self.namespace_jobs.get(id).cloned().ok_or_else(|| {
                        MetadataBackendError::Serde(
                            "destination reservation index lost its original job".into(),
                        )
                    })?);
                }
            }
            current = crate::posix_path::parent_posix_path(&candidate);
        }
        Ok(covering)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    fn coordinator() -> DfsNamespaceJobRecord {
        let now = time::OffsetDateTime::now_utc();
        DfsNamespaceJobRecord {
            job_id: "dir-move-coordinator".into(),
            file_system_id: sifs_file_system_id(),
            origin: None,
            kind: DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Running,
            root_path: "/users/source".into(),
            target_path: Some("/services/target".into()),
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        }
    }

    #[test]
    fn reservation_index_covers_the_subtree_and_follows_release_and_rollback() {
        let fs = sifs_file_system_id();
        let reservation = coordinator()
            .directory_move_destination_reservation_job()
            .unwrap();
        let mut tables = DfsMetadataTables::default();
        tables
            .apply_checked(DfsMetadataTableOp::PutNamespaceJob {
                job: reservation.clone(),
            })
            .unwrap();
        for covered in ["/services/target", "/services/target/a/b"] {
            assert_eq!(
                tables
                    .move_destination_reservations_covering(&fs, covered)
                    .unwrap(),
                vec![reservation.clone()]
            );
        }
        for outside in ["/services", "/services/target-sibling", "/users/source"] {
            assert!(
                tables
                    .move_destination_reservations_covering(&fs, outside)
                    .unwrap()
                    .is_empty()
            );
        }
        let mut rollback = DfsMetadataTableRollback::default();
        tables
            .apply_checked_with_rollback(
                DfsMetadataTableOp::PutNamespaceJob {
                    job: reservation.released_directory_move_destination_reservation(
                        time::OffsetDateTime::now_utc(),
                    ),
                },
                &mut rollback,
            )
            .unwrap();
        assert!(
            tables
                .move_destination_reservations_covering(&fs, "/services/target/a")
                .unwrap()
                .is_empty()
        );
        rollback.rollback(&mut tables);
        assert_eq!(
            tables
                .move_destination_reservations_covering(&fs, "/services/target/a")
                .unwrap(),
            vec![reservation.clone()]
        );
        let mut malformed = reservation;
        malformed.root_path = "/services/elsewhere".into();
        assert!(
            tables
                .apply_checked(DfsMetadataTableOp::PutNamespaceJob { job: malformed })
                .is_err()
        );
    }
}
