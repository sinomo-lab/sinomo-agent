//! Derived index of the namespace jobs that are still Running.
//!
//! The index is rebuilt from durable namespace job rows on every apply,
//! snapshot load and rollback, like the parent-count reservation index.
//! Maintenance pages a shard's Running jobs from it, so its passes never read
//! the terminal rows a shard keeps until they are collected.
use super::*;

impl DfsMetadataTables {
    pub(super) fn index_running_namespace_job(&mut self, job_id: &str) {
        if self
            .namespace_jobs
            .get(job_id)
            .is_some_and(|job| job.status == DfsNamespaceJobStatus::Running)
        {
            self.running_namespace_jobs.insert(job_id.to_owned());
        } else {
            self.running_namespace_jobs.remove(job_id);
        }
    }

    pub(super) fn rebuild_running_namespace_job_index(&mut self) {
        self.running_namespace_jobs = self
            .namespace_jobs
            .iter()
            .filter(|(_, job)| job.status == DfsNamespaceJobStatus::Running)
            .map(|(job_id, _)| job_id.clone())
            .collect();
    }

    /// The Running jobs after `after`, in job id order.
    pub(crate) fn running_namespace_jobs_after<'a>(
        &'a self,
        after: Option<&str>,
    ) -> impl Iterator<Item = &'a DfsNamespaceJobRecord> + 'a {
        let start = match after {
            Some(after) => std::ops::Bound::Excluded(after.to_owned()),
            None => std::ops::Bound::Unbounded,
        };
        self.running_namespace_jobs
            .range((start, std::ops::Bound::Unbounded))
            .filter_map(|job_id| self.namespace_jobs.get(job_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: &str, status: DfsNamespaceJobStatus) -> DfsNamespaceJobRecord {
        DfsNamespaceJobRecord {
            job_id: id.into(),
            file_system_id: crate::sifs_file_system_id(),
            origin: None,
            kind: crate::DfsNamespaceJobKind::ClearDirectoryRecursive,
            status,
            root_path: format!("/{id}"),
            target_path: None,
            pending_frames: Vec::new(),
            processed_object_count: 0,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        }
    }

    fn running(tables: &DfsMetadataTables, after: Option<&str>) -> Vec<String> {
        tables
            .running_namespace_jobs_after(after)
            .map(|job| job.job_id.clone())
            .collect()
    }

    #[test]
    fn only_running_rows_are_indexed_through_every_transition() {
        let mut tables = DfsMetadataTables::default();
        for (id, status) in [
            ("a", DfsNamespaceJobStatus::Running),
            ("b", DfsNamespaceJobStatus::Failed),
            ("c", DfsNamespaceJobStatus::Running),
            ("d", DfsNamespaceJobStatus::Completed),
        ] {
            tables
                .apply_checked(DfsMetadataTableOp::PutNamespaceJob {
                    job: job(id, status),
                })
                .unwrap();
        }
        assert_eq!(running(&tables, None), ["a", "c"]);
        assert_eq!(running(&tables, Some("a")), ["c"]);
        // A job that ends leaves the index.
        tables
            .apply_checked(DfsMetadataTableOp::PutNamespaceJob {
                job: job("a", DfsNamespaceJobStatus::Failed),
            })
            .unwrap();
        assert_eq!(running(&tables, None), ["c"]);
        let mut rebuilt = tables.clone();
        rebuilt.running_namespace_jobs.clear();
        rebuilt.rebuild_running_namespace_job_index();
        assert_eq!(rebuilt, tables);
    }
}
