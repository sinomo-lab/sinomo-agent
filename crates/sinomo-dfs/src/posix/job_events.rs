//! The coordinator's announcements of a namespace job that work on other
//! shards may wait on: a step parked on closed shard storage, and the job
//! leaving Running while a shard it touched converts
//! ([`crate::namespace_job_events`]). An announcement never fails the job:
//! what it cannot resolve is left unannounced.

use std::collections::BTreeSet;

use crate::maintenance::storage_waits;
use crate::namespace_job_events::{
    DfsNamespaceJobEvent, DfsNamespaceJobEventKind, announce_namespace_job_event,
};
use crate::posix_path::parent_posix_path;
use crate::{
    DfsMessageTransport, DfsMetadataShardConfig, DfsMetadataShardId, DfsNamespaceJobFrame,
    DfsNamespaceJobRecord, DfsNamespaceJobStatus, NetworkedDfsService,
};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// The current catalog of `job`'s filesystem and the shard that
    /// coordinates it, the owner of its root path.
    fn namespace_job_coordinator(
        &mut self,
        job: &DfsNamespaceJobRecord,
    ) -> Option<(Vec<DfsMetadataShardConfig>, DfsMetadataShardConfig)> {
        let catalog = self
            .metadata_shard_config_snapshot(&job.file_system_id)
            .ok()
            .flatten()?;
        let coordinator = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &job.file_system_id,
            &catalog,
            &job.root_path,
        )
        .ok()
        .flatten()?;
        Some((catalog, coordinator))
    }

    /// `job`'s step at `frame`, its next frame as its durable record keeps
    /// it, was refused until each of `shards` has Active storage. Record the
    /// park for the job and announce it.
    pub(super) fn note_namespace_job_parked(
        &mut self,
        job: &DfsNamespaceJobRecord,
        frame: &DfsNamespaceJobFrame,
        shards: &[DfsMetadataShardConfig],
    ) {
        let touched = shards
            .iter()
            .filter(|shard| shard.file_system_id == job.file_system_id)
            .map(|shard| DfsMetadataShardId::new(&shard.shard_id))
            .collect::<BTreeSet<_>>();
        storage_waits::record_job_parked_on_shard_storage(
            &self.node_id,
            job,
            frame,
            touched.iter().cloned(),
        );
        let Some((_, coordinator)) = self.namespace_job_coordinator(job) else {
            tracing::debug!(target: "sinomo.dfs", job_id = %job.job_id,
                "a parked namespace job has no coordinator shard to announce");
            return;
        };
        announce_namespace_job_event(
            &self.node_id,
            DfsNamespaceJobEvent {
                kind: DfsNamespaceJobEventKind::Parked,
                file_system_id: job.file_system_id.clone(),
                coordinator_shard_id: DfsMetadataShardId::new(coordinator.shard_id),
                job_id: job.job_id.clone(),
                status: DfsNamespaceJobStatus::Running,
                touched_shard_ids: touched.into_iter().collect(),
            },
        );
    }

    /// `job` was Running and its durable record now is not. Announce the end
    /// to the converting shards it touched: the owners of its root and target
    /// paths and of their parents, and the shards it last parked on. No
    /// event is published while none of them converts.
    pub(super) fn note_namespace_job_ended(&mut self, job: &DfsNamespaceJobRecord) {
        let parked =
            storage_waits::take_job_storage_waits(&self.node_id, &job.file_system_id, &job.job_id);
        if self.control_plane.is_none() {
            return;
        }
        let Some((catalog, coordinator)) = self.namespace_job_coordinator(job) else {
            return;
        };
        let mut candidates = parked
            .into_iter()
            .map(|shard| shard.0)
            .collect::<BTreeSet<_>>();
        let paths = std::iter::once(job.root_path.clone())
            .chain(job.target_path.clone())
            .collect::<Vec<_>>();
        for path in paths
            .iter()
            .cloned()
            .chain(paths.iter().filter_map(|path| parent_posix_path(path)))
        {
            if let Ok(Some(owner)) =
                crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &job.file_system_id,
                    &catalog,
                    &path,
                )
            {
                candidates.insert(owner.shard_id);
            }
        }
        candidates.remove(&coordinator.shard_id);
        let mut touched = Vec::new();
        for shard in catalog
            .iter()
            .filter(|shard| candidates.contains(&shard.shard_id))
        {
            if self
                .metadata_shard_storage_is_converting(shard)
                .unwrap_or(false)
            {
                touched.push(DfsMetadataShardId::new(&shard.shard_id));
            }
        }
        if touched.is_empty() {
            return;
        }
        announce_namespace_job_event(
            &self.node_id,
            DfsNamespaceJobEvent {
                kind: DfsNamespaceJobEventKind::Completed,
                file_system_id: job.file_system_id.clone(),
                coordinator_shard_id: DfsMetadataShardId::new(coordinator.shard_id),
                job_id: job.job_id.clone(),
                status: job.status.clone(),
                touched_shard_ids: touched,
            },
        );
    }
}
