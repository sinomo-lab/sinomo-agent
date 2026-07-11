//! Failover-recoverable metadata durability barriers.
//!
//! File-content receipt validation lives in `posix::sync`; this module owns the
//! lower metadata primitive shared by file and directory sync. A caller first
//! freezes a shard journal head while it is the current primary, then waits
//! until that exact prefix is durably acknowledged by a configured majority.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::{
    DfsControlPlaneStore, DfsError, DfsFileSystemId, DfsMessageTransport, DfsMetadataBackend,
    DfsMetadataJournalBatch, DfsMetadataJournalBatchCommand, DfsMetadataPrefixProbe,
    DfsMetadataShardConfig, DfsMetadataShardHeadWatermark, DfsMetadataShardId,
    DfsMetadataSnapshotInstallCommand, DfsNetworkMessage, DfsObjectMetadata, NetworkedDfsService,
    NetworkedDfsServiceEvent, PendingMetadataReplication,
};

/// Default time budget for a foreground POSIX durability barrier.
pub const DEFAULT_DFS_POSIX_SYNC_TIMEOUT: Duration = Duration::from_secs(45);

const DFS_POSIX_SYNC_IDLE_WAIT: Duration = Duration::from_millis(10);

/// Immutable metadata prefix captured by the current shard primary.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataQuorumSyncTarget {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub primary_node_id: NodeId,
    /// `None` is reserved for a local backend without a configured cluster
    /// shard. Such a target is necessarily single-node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_node_ids: Vec<NodeId>,
    pub journal_sequence: u64,
    /// Restart-unique identity for this durability attempt. It prevents a
    /// delayed response from an earlier service lifetime from satisfying a
    /// newly captured target whose numeric request counter was reused.
    #[serde(default)]
    pub barrier_id: String,
    /// Canonical digest of the complete materialized shard snapshot at
    /// `journal_sequence`.
    #[serde(default)]
    pub state_fingerprint: String,
    /// Frozen checkpoint used only by the authority executing this barrier.
    /// It is deliberately excluded from certificates/wire serialization.
    #[serde(skip)]
    pub checkpoint_snapshot: Option<crate::DfsMetadataTableSnapshot>,
}

/// Current acknowledgement state for a frozen metadata prefix.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataQuorumSyncProgress {
    pub quorum_size: usize,
    pub acknowledged_node_ids: Vec<NodeId>,
}

impl DfsMetadataQuorumSyncProgress {
    pub fn quorum_reached(&self) -> bool {
        self.acknowledged_node_ids.len() >= self.quorum_size
    }
}

/// Successful metadata certificate returned by a durability barrier.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataQuorumSyncCertificate {
    pub target: DfsMetadataQuorumSyncTarget,
    pub quorum_size: usize,
    pub acknowledged_node_ids: Vec<NodeId>,
}

/// Majority size for one primary plus `replica_count` configured replicas.
pub const fn dfs_metadata_replica_quorum_size(replica_count: usize) -> usize {
    (replica_count + 1) / 2 + 1
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Freeze the local authoritative journal head that contains `object`'s
    /// already-journaled state.
    ///
    /// This method never routes. A non-primary caller receives
    /// [`DfsError::StaleOwnerEpoch`] and must send the higher-level sync request
    /// to the returned current primary.
    pub fn capture_metadata_quorum_sync_target(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsMetadataQuorumSyncTarget, DfsError> {
        let configured = self.select_metadata_shard_for_object(object)?;
        if configured.is_none() && self.control_plane.is_some() {
            return Err(DfsError::Distributed {
                message: format!(
                    "filesystem {} has a control-plane client but no configured metadata authority for POSIX sync of {}",
                    object.file_system_id, object.object_id
                ),
            });
        }
        if let Some(shard) = configured.as_ref()
            && shard.primary_node_id != self.node_id
        {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            });
        }

        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "POSIX sync requires a durable metadata backend".to_string(),
            })?;
        let statuses = backend.metadata_shard_statuses()?;
        let status = match configured.as_ref() {
            Some(shard) => {
                let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
                statuses
                    .into_iter()
                    .find(|status| status.shard_id == shard_id)
                    .ok_or_else(|| DfsError::Metadata {
                        message: format!(
                            "local primary {} has no durable metadata shard {}",
                            self.node_id, shard.shard_id
                        ),
                    })?
            }
            None => {
                let mut matching = statuses
                    .into_iter()
                    .filter(|status| {
                        status
                            .file_system_id
                            .as_ref()
                            .is_none_or(|file_system_id| file_system_id == &object.file_system_id)
                    })
                    .collect::<Vec<_>>();
                if matching.len() != 1 {
                    return Err(DfsError::Metadata {
                        message: format!(
                            "cannot select a single local metadata shard for POSIX sync of {}",
                            object.object_id
                        ),
                    });
                }
                matching.pop().expect("one matching status")
            }
        };

        // A sharded backend reports the authority metadata it was opened
        // with. After a certified primary transfer that storage status is
        // intentionally stale; the current control-plane shard config above
        // is the authority source for clustered syncs. Local/unconfigured
        // backends have no control-plane certification, so retain the storage
        // authority guard for that case.
        if configured.is_none()
            && let Some(primary_node_id) = status.primary_node_id.as_ref()
            && primary_node_id != &self.node_id
        {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: primary_node_id.clone(),
                epoch: status.epoch.unwrap_or_default(),
            });
        }
        if let Some(status_file_system_id) = status.file_system_id.as_ref()
            && status_file_system_id != &object.file_system_id
        {
            return Err(DfsError::Metadata {
                message: format!(
                    "local metadata shard {} belongs to filesystem {}, not POSIX sync filesystem {}",
                    status.shard_id.0, status_file_system_id, object.file_system_id
                ),
            });
        }
        let checkpoint_snapshot = metadata_shard_snapshot_at(
            backend.as_ref(),
            &status.shard_id,
            status.journal_last_sequence,
        )?;
        let state_fingerprint =
            crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(
                &checkpoint_snapshot,
            )
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "encode metadata shard {} durability checkpoint: {source}",
                    status.shard_id.0
                ),
            })?;
        let barrier_id = sinomo_core::Id::generate().to_string();
        let (primary_node_id, authority_epoch, replica_node_ids) = match configured {
            Some(shard) => (
                shard.primary_node_id,
                Some(shard.epoch),
                normalized_replica_node_ids(&self.node_id, shard.replica_node_ids),
            ),
            None => (self.node_id.clone(), None, Vec::new()),
        };
        Ok(DfsMetadataQuorumSyncTarget {
            file_system_id: object.file_system_id.clone(),
            shard_id: status.shard_id,
            primary_node_id,
            authority_epoch,
            replica_node_ids,
            journal_sequence: status.journal_last_sequence,
            barrier_id,
            state_fingerprint,
            checkpoint_snapshot: Some(checkpoint_snapshot),
        })
    }

    /// Inspect durable acknowledgement cursors for a frozen journal prefix.
    pub fn metadata_quorum_sync_progress(
        &self,
        target: &DfsMetadataQuorumSyncTarget,
    ) -> Result<DfsMetadataQuorumSyncProgress, DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "POSIX sync requires a durable metadata backend".to_string(),
            })?;
        let status = backend
            .metadata_shard_statuses()?
            .into_iter()
            .find(|status| status.shard_id == target.shard_id)
            .ok_or_else(|| DfsError::Metadata {
                message: format!(
                    "local metadata shard {} disappeared during POSIX sync",
                    target.shard_id.0
                ),
            })?;
        if status
            .file_system_id
            .as_ref()
            .is_some_and(|file_system_id| file_system_id != &target.file_system_id)
        {
            return Err(DfsError::Metadata {
                message: format!(
                    "local metadata shard {} changed filesystem during POSIX sync",
                    target.shard_id.0
                ),
            });
        }
        // Clustered callers revalidate and certify the current control-plane
        // authority immediately before inspecting progress. Backend status
        // retains open-time primary/epoch metadata across a primary transfer,
        // so it is authoritative here only for local/unconfigured targets.
        if target.authority_epoch.is_none()
            && (status
                .primary_node_id
                .as_ref()
                .is_some_and(|primary_node_id| primary_node_id != &target.primary_node_id)
                || status.epoch.is_some())
        {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: target.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: status
                    .primary_node_id
                    .unwrap_or_else(|| target.primary_node_id.clone()),
                epoch: status.epoch.unwrap_or_default(),
            });
        }
        let local_sequence = status.journal_last_sequence;
        if local_sequence < target.journal_sequence {
            return Err(DfsError::Metadata {
                message: format!(
                    "local metadata shard {} regressed below frozen POSIX sync prefix: {local_sequence} < {}",
                    target.shard_id.0, target.journal_sequence
                ),
            });
        }

        let mut acknowledged = vec![self.node_id.clone()];
        for replica_node_id in &target.replica_node_ids {
            if backend.metadata_replication_cursor(replica_node_id, &target.shard_id)?
                >= target.journal_sequence
            {
                acknowledged.push(replica_node_id.clone());
            }
        }
        acknowledged.sort();
        acknowledged.dedup();
        Ok(DfsMetadataQuorumSyncProgress {
            quorum_size: dfs_metadata_replica_quorum_size(target.replica_node_ids.len()),
            acknowledged_node_ids: acknowledged,
        })
    }

    /// Replicate a frozen local metadata prefix until a configured majority has
    /// durably acknowledged it, or fail without making a durability claim.
    pub fn sync_metadata_shard_to_quorum(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        timeout: Duration,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        self.sync_metadata_shard_to_quorum_with_required_intersection(
            target,
            &BTreeSet::new(),
            timeout,
        )
    }

    /// Replicate a frozen metadata prefix to quorum while also requiring at
    /// least one acknowledgement from the supplied recovery set.
    ///
    /// File sync uses the exact-hash content receipt holders as this set. A
    /// bare metadata majority that cannot reach any durable copy of the
    /// frozen bytes is not a failover-recoverable file certificate.
    pub fn sync_metadata_shard_to_quorum_intersecting(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        recovery_node_ids: &[NodeId],
        timeout: Duration,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        let recovery_node_ids = recovery_node_ids.iter().cloned().collect::<BTreeSet<_>>();
        if recovery_node_ids.is_empty() {
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} cannot form a file-sync recovery certificate without content receipt holders",
                    target.shard_id.0
                ),
            });
        }
        self.sync_metadata_shard_to_quorum_with_required_intersection(
            target,
            &recovery_node_ids,
            timeout,
        )
    }

    fn sync_metadata_shard_to_quorum_with_required_intersection(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        required_intersection: &BTreeSet<NodeId>,
        timeout: Duration,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        let result = self.sync_metadata_shard_to_quorum_with_required_intersection_inner(
            target,
            required_intersection,
            timeout,
        );
        self.clear_pending_metadata_sync_replications(target);
        result
    }

    fn sync_metadata_shard_to_quorum_with_required_intersection_inner(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        required_intersection: &BTreeSet<NodeId>,
        timeout: Duration,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        if target.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: target.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: target.primary_node_id.clone(),
                epoch: target.authority_epoch.unwrap_or_default(),
            });
        }

        let started_at = Instant::now();
        // Persisted replication cursors are scheduling hints, not a proof for
        // this barrier. Each non-primary acknowledgement below comes from a
        // fresh exact-checkpoint probe sent and answered during this call.
        // Include a per-invocation nonce even when a caller reuses a serialized
        // target across a service restart, where the numeric request counter
        // begins again at one.
        let probe_barrier_id = format!("{}-{}", target.barrier_id, sinomo_core::Id::generate());
        let mut freshly_acknowledged = BTreeSet::from([self.node_id.clone()]);
        let mut pending_prefix_probes = BTreeMap::<String, NodeId>::new();
        let mut pending_checkpoint_snapshots = BTreeMap::<String, NodeId>::new();
        loop {
            self.check_cancelled("metadata quorum sync")?;
            self.revalidate_metadata_quorum_sync_authority(target)?;
            let progress =
                metadata_sync_progress_from_fresh_acknowledgements(target, &freshly_acknowledged);
            if metadata_sync_progress_satisfies(&progress, required_intersection) {
                self.clear_pending_metadata_sync_replications(target);
                return self.finalize_metadata_quorum_sync(target, progress);
            }

            let cursor_ready = self
                .metadata_quorum_sync_progress(target)?
                .acknowledged_node_ids
                .into_iter()
                .collect::<BTreeSet<_>>();
            let behind_nodes = target
                .replica_node_ids
                .iter()
                .filter(|node_id| !cursor_ready.contains(*node_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            self.send_metadata_sync_checkpoint_snapshots(
                target,
                &probe_barrier_id,
                &behind_nodes,
                &mut pending_checkpoint_snapshots,
            )?;

            let events = self.process_incoming()?;
            let rejected_nodes = record_metadata_sync_prefix_probe_events(
                events,
                target,
                &mut pending_prefix_probes,
                &mut pending_checkpoint_snapshots,
                &mut freshly_acknowledged,
            );
            self.send_metadata_sync_checkpoint_snapshots(
                target,
                &probe_barrier_id,
                &rejected_nodes,
                &mut pending_checkpoint_snapshots,
            )?;
            self.send_metadata_sync_prefix_probes(
                target,
                &probe_barrier_id,
                &freshly_acknowledged,
                &mut pending_prefix_probes,
            )?;
            let events = self.process_incoming()?;
            let rejected_nodes = record_metadata_sync_prefix_probe_events(
                events,
                target,
                &mut pending_prefix_probes,
                &mut pending_checkpoint_snapshots,
                &mut freshly_acknowledged,
            );
            self.send_metadata_sync_checkpoint_snapshots(
                target,
                &probe_barrier_id,
                &rejected_nodes,
                &mut pending_checkpoint_snapshots,
            )?;

            let progress =
                metadata_sync_progress_from_fresh_acknowledgements(target, &freshly_acknowledged);
            if metadata_sync_progress_satisfies(&progress, required_intersection) {
                self.clear_pending_metadata_sync_replications(target);
                return self.finalize_metadata_quorum_sync(target, progress);
            }

            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                self.clear_pending_metadata_sync_replications(target);
                let recovery_intersection = required_intersection.is_empty()
                    || progress
                        .acknowledged_node_ids
                        .iter()
                        .any(|node_id| required_intersection.contains(node_id));
                return Err(DfsError::Network {
                    message: format!(
                        "metadata shard {} did not reach a POSIX sync certificate before {}ms: {}/{} acknowledgements through sequence {}; recovery-set intersection={recovery_intersection}",
                        target.shard_id.0,
                        timeout.as_millis(),
                        progress.acknowledged_node_ids.len(),
                        progress.quorum_size,
                        target.journal_sequence
                    ),
                });
            }
            let wait = DFS_POSIX_SYNC_IDLE_WAIT.min(timeout.saturating_sub(elapsed));
            self.transport
                .wait_for_incoming(wait)
                .map_err(DfsError::from_network)?;
        }
    }

    fn send_metadata_sync_prefix_probes(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        probe_barrier_id: &str,
        freshly_acknowledged: &BTreeSet<NodeId>,
        pending_prefix_probes: &mut BTreeMap<String, NodeId>,
    ) -> Result<(), DfsError> {
        if target.replica_node_ids.is_empty() {
            return Ok(());
        }
        if target.barrier_id.is_empty() || target.state_fingerprint.is_empty() {
            return Err(DfsError::Metadata {
                message: format!(
                    "clustered metadata shard {} has an incomplete POSIX sync checkpoint identity",
                    target.shard_id.0
                ),
            });
        }

        let cursor_progress = self.metadata_quorum_sync_progress(target)?;
        let eligible = cursor_progress
            .acknowledged_node_ids
            .into_iter()
            .filter(|node_id| node_id != &self.node_id)
            .collect::<BTreeSet<_>>();
        let prefix_probe = DfsMetadataPrefixProbe {
            barrier_id: probe_barrier_id.to_string(),
            file_system_id: Some(target.file_system_id.clone()),
            authority_epoch: target.authority_epoch,
            shard_id: target.shard_id.clone(),
            journal_sequence: target.journal_sequence,
            state_fingerprint: target.state_fingerprint.clone(),
        };
        let probe = DfsMetadataJournalBatch {
            shard_id: target.shard_id.clone(),
            // This must remain empty for rolling-upgrade safety: a legacy peer
            // ignores the new probe fields and processes the command as a
            // harmless no-op instead of jumping its replay cursor.
            entries: Vec::new(),
            next_after_sequence: target.journal_sequence,
        };

        for target_node_id in &target.replica_node_ids {
            if freshly_acknowledged.contains(target_node_id)
                || !eligible.contains(target_node_id)
                || pending_prefix_probes
                    .values()
                    .any(|pending_node_id| pending_node_id == target_node_id)
            {
                continue;
            }
            let request_id = format!(
                "dfs_sync_probe_{}_{}",
                probe_barrier_id,
                self.next_request_id()
            );
            self.pending_metadata_replications.insert(
                request_id.clone(),
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: target.shard_id.clone(),
                    next_after_sequence: target.journal_sequence,
                    verify_prefix_only: true,
                    created_at: Instant::now(),
                },
            );
            let sent = self.send_dfs_message_if_routable(
                target_node_id,
                DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                    request_id: request_id.clone(),
                    source_node_id: Some(self.node_id.clone()),
                    verify_prefix_only: true,
                    prefix_probe: Some(prefix_probe.clone()),
                    batch: probe.clone(),
                }),
            )?;
            if sent {
                pending_prefix_probes.insert(request_id, target_node_id.clone());
            } else {
                self.pending_metadata_replications.remove(&request_id);
            }
        }
        Ok(())
    }

    fn send_metadata_sync_checkpoint_snapshots(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        probe_barrier_id: &str,
        rejected_node_ids: &BTreeSet<NodeId>,
        pending_checkpoint_snapshots: &mut BTreeMap<String, NodeId>,
    ) -> Result<(), DfsError> {
        if rejected_node_ids.is_empty() {
            return Ok(());
        }
        let checkpoint_snapshot = match target.checkpoint_snapshot.as_ref() {
            Some(snapshot) => snapshot.clone(),
            None => metadata_shard_snapshot_at(
                self.metadata_backend
                    .as_deref()
                    .ok_or_else(|| DfsError::Metadata {
                        message: "metadata sync checkpoint repair requires a durable backend"
                            .to_string(),
                    })?,
                &target.shard_id,
                target.journal_sequence,
            )?,
        };
        for target_node_id in rejected_node_ids {
            if pending_checkpoint_snapshots
                .values()
                .any(|pending_node_id| pending_node_id == target_node_id)
            {
                continue;
            }
            let request_id = format!(
                "dfs_sync_checkpoint_{}_{}",
                probe_barrier_id,
                self.next_request_id()
            );
            let command = DfsMetadataSnapshotInstallCommand {
                request_id: request_id.clone(),
                source_node_id: self.node_id.clone(),
                file_system_id: Some(target.file_system_id.clone()),
                authority_epoch: target.authority_epoch,
                shard_id: target.shard_id.clone(),
                next_after_sequence: target.journal_sequence,
                snapshot: checkpoint_snapshot.clone(),
                reason: format!(
                    "restore frozen POSIX sync checkpoint {}",
                    target.state_fingerprint
                ),
            };
            self.pending_metadata_replications.insert(
                request_id.clone(),
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: target.shard_id.clone(),
                    next_after_sequence: target.journal_sequence,
                    verify_prefix_only: false,
                    created_at: Instant::now(),
                },
            );
            let sent = self.send_dfs_message_if_routable(
                target_node_id,
                DfsNetworkMessage::MetadataSnapshotInstall(command),
            )?;
            if sent {
                pending_checkpoint_snapshots.insert(request_id, target_node_id.clone());
            } else {
                self.pending_metadata_replications.remove(&request_id);
            }
        }
        Ok(())
    }

    fn finalize_metadata_quorum_sync(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        progress: DfsMetadataQuorumSyncProgress,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        self.revalidate_metadata_quorum_sync_authority(target)?;
        if let Some(epoch) = target.authority_epoch {
            let shard = self
                .current_metadata_sync_shard_config(target)?
                .ok_or_else(|| DfsError::Distributed {
                    message: format!(
                        "metadata shard {} configuration disappeared before POSIX sync publication",
                        target.shard_id.0
                    ),
                })?;
            let watermark = DfsMetadataShardHeadWatermark {
                file_system_id: target.file_system_id.clone(),
                shard_id: target.shard_id.0.clone(),
                primary_node_id: target.primary_node_id.clone(),
                epoch,
                journal_sequence: target.journal_sequence,
                state_fingerprint: target.state_fingerprint.clone(),
                acknowledged_node_ids: progress.acknowledged_node_ids.clone(),
            };
            let published = self
                .control_plane
                .as_mut()
                .map(|control_plane| {
                    DfsControlPlaneStore::new(control_plane.as_mut())
                        .publish_metadata_shard_head_watermark(&shard, &watermark)
                })
                .transpose()?
                .unwrap_or(false);
            if !published {
                self.refresh_control_plane_latest()?;
                let current = self
                    .current_metadata_sync_shard_config(target)?
                    .unwrap_or(shard);
                return Err(DfsError::StaleOwnerEpoch {
                    file_system_id: target.file_system_id.clone(),
                    local_node_id: self.node_id.clone(),
                    primary_node_id: current.primary_node_id,
                    epoch: current.epoch,
                });
            }
        }
        let mut certified_target = target.clone();
        certified_target.checkpoint_snapshot = None;
        Ok(DfsMetadataQuorumSyncCertificate {
            target: certified_target,
            quorum_size: progress.quorum_size,
            acknowledged_node_ids: progress.acknowledged_node_ids,
        })
    }

    fn revalidate_metadata_quorum_sync_authority(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
    ) -> Result<(), DfsError> {
        self.refresh_control_plane_latest()?;
        let Some(expected_epoch) = target.authority_epoch else {
            if self.control_plane.is_none() {
                return Ok(());
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} became cluster-connected during a local POSIX sync",
                    target.shard_id.0
                ),
            });
        };

        let shard = self
            .current_metadata_sync_shard_config(target)?
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "metadata shard {} configuration disappeared during POSIX sync",
                    target.shard_id.0
                ),
            })?;
        if shard.primary_node_id != self.node_id || shard.epoch != expected_epoch {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: target.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        let current_replicas =
            normalized_replica_node_ids(&self.node_id, shard.replica_node_ids.clone());
        if current_replicas != target.replica_node_ids {
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} replica set changed without the frozen POSIX sync epoch changing",
                    target.shard_id.0
                ),
            });
        }

        let Some(control_plane) = self.control_plane.as_mut() else {
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} lost its control-plane client during POSIX sync",
                    target.shard_id.0
                ),
            });
        };
        if !DfsControlPlaneStore::new(control_plane.as_mut())
            .certify_metadata_shard_authority(&shard)?
        {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: target.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        Ok(())
    }

    fn current_metadata_sync_shard_config(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(&target.file_system_id, &target.shard_id.0)
            .map_err(DfsError::from)
    }

    fn clear_pending_metadata_sync_replications(&mut self, target: &DfsMetadataQuorumSyncTarget) {
        let replicas = target
            .replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        self.pending_metadata_replications.retain(|_, pending| {
            pending.shard_id != target.shard_id || !replicas.contains(&pending.target_node_id)
        });
    }
}

pub(crate) fn metadata_shard_state_fingerprint_at(
    backend: &dyn DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    expected_journal_sequence: u64,
) -> Result<String, DfsError> {
    let snapshot = metadata_shard_snapshot_at(backend, shard_id, expected_journal_sequence)?;
    crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(&snapshot).map_err(
        |source| DfsError::Serialization {
            message: format!(
                "encode metadata shard {} durability checkpoint: {source}",
                shard_id.0
            ),
        },
    )
}

fn metadata_shard_snapshot_at(
    backend: &dyn DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    expected_journal_sequence: u64,
) -> Result<crate::DfsMetadataTableSnapshot, DfsError> {
    let snapshot = backend
        .export_shard_table_snapshot(shard_id)?
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "metadata shard {} cannot export a materialized snapshot for a durability proof",
                shard_id.0
            ),
        })?;
    let materialized_sequence = snapshot.materialized_through_sequence.unwrap_or_default();
    if materialized_sequence != expected_journal_sequence {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata shard {} materialized sequence {} does not match durability head {}",
                shard_id.0, materialized_sequence, expected_journal_sequence
            ),
        });
    }
    Ok(snapshot)
}

fn normalized_replica_node_ids(
    primary_node_id: &NodeId,
    replica_node_ids: Vec<NodeId>,
) -> Vec<NodeId> {
    let mut replicas = replica_node_ids
        .into_iter()
        .filter(|node_id| node_id != primary_node_id)
        .collect::<Vec<_>>();
    replicas.sort();
    replicas.dedup();
    replicas
}

fn metadata_sync_progress_satisfies(
    progress: &DfsMetadataQuorumSyncProgress,
    required_intersection: &BTreeSet<NodeId>,
) -> bool {
    progress.quorum_reached()
        && (required_intersection.is_empty()
            || progress
                .acknowledged_node_ids
                .iter()
                .any(|node_id| required_intersection.contains(node_id)))
}

fn metadata_sync_progress_from_fresh_acknowledgements(
    target: &DfsMetadataQuorumSyncTarget,
    acknowledged: &BTreeSet<NodeId>,
) -> DfsMetadataQuorumSyncProgress {
    DfsMetadataQuorumSyncProgress {
        quorum_size: dfs_metadata_replica_quorum_size(target.replica_node_ids.len()),
        acknowledged_node_ids: acknowledged.iter().cloned().collect(),
    }
}

fn record_metadata_sync_prefix_probe_events(
    events: Vec<NetworkedDfsServiceEvent>,
    target: &DfsMetadataQuorumSyncTarget,
    pending_prefix_probes: &mut BTreeMap<String, NodeId>,
    pending_checkpoint_snapshots: &mut BTreeMap<String, NodeId>,
    freshly_acknowledged: &mut BTreeSet<NodeId>,
) -> BTreeSet<NodeId> {
    let mut rejected_node_ids = BTreeSet::new();
    for event in events {
        match event {
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                request_id,
                source_node_id,
                shard_id,
                next_after_sequence,
                verified_prefix_only,
                ..
            } => {
                let Some(expected_node_id) = pending_prefix_probes.remove(&request_id) else {
                    continue;
                };
                if expected_node_id == source_node_id
                    && shard_id == target.shard_id
                    && next_after_sequence == target.journal_sequence
                    && verified_prefix_only
                {
                    freshly_acknowledged.insert(source_node_id);
                }
            }
            NetworkedDfsServiceEvent::MetadataSnapshotInstalled { request_id, .. } => {
                pending_checkpoint_snapshots.remove(&request_id);
            }
            NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id,
                source_node_id,
                ..
            } => {
                if pending_prefix_probes.remove(&request_id).is_some() {
                    rejected_node_ids.insert(source_node_id);
                }
                pending_checkpoint_snapshots.remove(&request_id);
            }
            _ => {}
        }
    }
    rejected_node_ids
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};

    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::kv::InProcessKvStore;
    use sinomo_core::network::NetworkError;

    use super::*;
    use crate::{
        DfsFileSystem, DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
        DfsMetadataShardAssignment, DfsMetadataSnapshotInstallResponse,
        DfsMetadataSnapshotInstallResult, DfsNetworkMessage, DfsRoutedMessage,
        DfsTransportIdentity, LocalContentStore, LocalShardedMetadataBackend,
        LocalSobjMetadataBackend, SharedDfsControlPlaneKvClient, build_dfs_routed_message,
        directory_object, sifs_file_system_id,
    };

    #[derive(Debug)]
    struct MetadataAckTransport {
        local_node_id: NodeId,
        acknowledged_nodes: BTreeSet<NodeId>,
        echoes_prefix_verification: bool,
        response_sequence_override: Option<u64>,
        incoming: VecDeque<DfsRoutedMessage>,
        next_frame_id: u64,
    }

    impl MetadataAckTransport {
        fn new(
            local_node_id: NodeId,
            acknowledged_nodes: impl IntoIterator<Item = NodeId>,
        ) -> Self {
            Self {
                local_node_id,
                acknowledged_nodes: acknowledged_nodes.into_iter().collect(),
                echoes_prefix_verification: true,
                response_sequence_override: None,
                incoming: VecDeque::new(),
                next_frame_id: 1,
            }
        }
    }

    impl DfsTransportIdentity for MetadataAckTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for MetadataAckTransport {
        fn send_routed_message(
            &mut self,
            target_node_id: &NodeId,
            message: DfsRoutedMessage,
        ) -> Result<(), NetworkError> {
            if !self.acknowledged_nodes.contains(target_node_id) {
                return Ok(());
            }
            let frame_id = self.next_frame_id;
            self.next_frame_id += 1;
            let response = match message.message {
                DfsNetworkMessage::MetadataJournalBatch(command) => {
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id,
                            shard_id: command.batch.shard_id,
                            next_after_sequence: self
                                .response_sequence_override
                                .unwrap_or(command.batch.next_after_sequence),
                            applied_entries: command.batch.entries.len(),
                            verified_prefix_only: command.verify_prefix_only
                                && self.echoes_prefix_verification,
                            result: DfsMetadataJournalBatchResult::Applied,
                        },
                    )
                }
                DfsNetworkMessage::MetadataSnapshotInstall(command) => {
                    DfsNetworkMessage::MetadataSnapshotInstallResponse(
                        DfsMetadataSnapshotInstallResponse {
                            request_id: command.request_id,
                            shard_id: command.shard_id,
                            next_after_sequence: command.next_after_sequence,
                            result: DfsMetadataSnapshotInstallResult::Installed {
                                archived_existing: false,
                            },
                        },
                    )
                }
                _ => return Ok(()),
            };
            self.incoming.push_back(build_dfs_routed_message(
                target_node_id,
                &self.local_node_id,
                frame_id,
                frame_id,
                response,
            ));
            Ok(())
        }

        fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(self.incoming.drain(..).collect())
        }

        fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
            Ok(Some(self.incoming.len()))
        }
    }

    struct ConfiguredSyncFixture {
        root: sinomo_testdir::TestDir,
        control_plane: SharedDfsControlPlaneKvClient<InProcessKvStore>,
        service: NetworkedDfsService<MetadataAckTransport>,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
    }

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("test node id")
    }

    fn configured_sync_fixture(
        name: &str,
        replica_node_ids: Vec<NodeId>,
        acknowledged_nodes: Vec<NodeId>,
    ) -> ConfiguredSyncFixture {
        let root = sinomo_testdir::TestDir::new(&format!("sinomo-dfs-sync-{name}"));
        let owner = node(&format!("node_{name}_owner"));
        let file_system_id = sifs_file_system_id();
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids,
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&shard)
                .expect("metadata shard config");
        });
        let backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        let mut service = NetworkedDfsService::new(
            MetadataAckTransport::new(owner.clone(), acknowledged_nodes),
            LocalContentStore::new(root.path().join("content")).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(backend)
        .expect("metadata-backed service");
        service
            .add_file_system(DfsFileSystem::sifs(owner.clone()))
            .expect("filesystem");
        let object = directory_object("/", owner);
        service
            .add_object_metadata(object.clone())
            .expect("root metadata");
        ConfiguredSyncFixture {
            root,
            control_plane,
            service,
            object,
            shard,
        }
    }

    #[test]
    fn metadata_quorum_size_counts_primary_and_configured_replicas() {
        assert_eq!(dfs_metadata_replica_quorum_size(0), 1);
        assert_eq!(dfs_metadata_replica_quorum_size(1), 2);
        assert_eq!(dfs_metadata_replica_quorum_size(2), 2);
        assert_eq!(dfs_metadata_replica_quorum_size(3), 3);
    }

    #[test]
    fn unconfigured_single_node_metadata_sync_succeeds_locally() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-sync-single-node");
        let owner = node("node_sync_single_owner");
        let backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        let mut service = NetworkedDfsService::new(
            MetadataAckTransport::new(owner.clone(), []),
            LocalContentStore::new(root.path().join("content")).expect("content store"),
        )
        .with_metadata_backend(backend)
        .expect("metadata-backed service");
        service
            .add_file_system(DfsFileSystem::sifs(owner.clone()))
            .expect("filesystem");
        let object = directory_object("/", owner.clone());
        service
            .add_object_metadata(object.clone())
            .expect("root metadata");

        let target = service
            .capture_metadata_quorum_sync_target(&object)
            .expect("capture target");
        assert_eq!(target.authority_epoch, None);
        let certificate = service
            .sync_metadata_shard_to_quorum(&target, Duration::ZERO)
            .expect("single-node certificate");

        assert_eq!(certificate.quorum_size, 1);
        assert_eq!(certificate.acknowledged_node_ids, vec![owner]);
    }

    #[test]
    fn configured_metadata_sync_uses_transferred_control_plane_authority() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-sync-transferred-primary");
        let former_primary = node("node_sync_transferred_former");
        let current_primary = node("node_sync_transferred_current");
        let file_system_id = sifs_file_system_id();
        let opened_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: former_primary,
            replica_node_ids: vec![current_primary.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let current_shard = DfsMetadataShardConfig {
            primary_node_id: current_primary.clone(),
            replica_node_ids: Vec::new(),
            epoch: 2,
            ..opened_shard.clone()
        };
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&current_shard)
                .expect("transferred metadata shard config");
        });
        let backend =
            LocalShardedMetadataBackend::new(root.path().join("metadata"), vec![opened_shard])
                .expect("backend opened under former authority");
        let mut service = NetworkedDfsService::new(
            MetadataAckTransport::new(current_primary.clone(), []),
            LocalContentStore::new(root.path().join("content")).expect("content store"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(backend)
        .expect("metadata-backed service");
        service
            .add_file_system(DfsFileSystem::sifs(current_primary.clone()))
            .expect("filesystem");
        let object = directory_object("/", current_primary.clone());
        service
            .add_object_metadata(object.clone())
            .expect("root metadata");

        let target = service
            .capture_metadata_quorum_sync_target(&object)
            .expect("current certified primary captures stale-opened backend");
        assert_eq!(target.primary_node_id, current_primary);
        assert_eq!(target.authority_epoch, Some(2));
        let progress = service
            .metadata_quorum_sync_progress(&target)
            .expect("stale backend authority fields do not reject clustered progress");
        assert!(progress.quorum_reached());
        service
            .sync_metadata_shard_to_quorum(&target, Duration::ZERO)
            .expect("current certified primary syncs after transfer");
    }

    #[test]
    fn configured_metadata_sync_waits_for_and_records_a_majority_ack() {
        let replica_a = node("node_sync_quorum_replica_a");
        let replica_b = node("node_sync_quorum_replica_b");
        let mut fixture = configured_sync_fixture(
            "quorum",
            vec![replica_a.clone(), replica_b],
            vec![replica_a.clone()],
        );
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture target");
        let second_target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture second target");
        assert_ne!(target.barrier_id, second_target.barrier_id);
        assert_eq!(target.state_fingerprint, second_target.state_fingerprint);

        let certificate = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::from_millis(50))
            .expect("metadata quorum certificate");

        assert_eq!(certificate.quorum_size, 2);
        assert!(
            certificate
                .acknowledged_node_ids
                .contains(&fixture.shard.primary_node_id)
        );
        assert!(certificate.acknowledged_node_ids.contains(&replica_a));
        let watermark = fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .get_metadata_shard_head_watermark(&target.file_system_id, &target.shard_id.0)
                .expect("read sync watermark")
                .expect("published sync watermark")
        });
        assert_eq!(watermark.journal_sequence, target.journal_sequence);
        assert_eq!(
            watermark.acknowledged_node_ids,
            certificate.acknowledged_node_ids
        );
        assert!(fixture.root.path().join("metadata").exists());
    }

    #[test]
    fn configured_metadata_sync_rejects_a_legacy_sequence_only_ack() {
        let replica_a = node("node_sync_legacy_replica_a");
        let replica_b = node("node_sync_legacy_replica_b");
        let mut fixture = configured_sync_fixture(
            "legacy_ack",
            vec![replica_a.clone(), replica_b],
            vec![replica_a],
        );
        fixture.service.transport.echoes_prefix_verification = false;
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture target");

        let error = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::ZERO)
            .expect_err("a peer that cannot echo exact-prefix verification is not a fresh ack");
        assert!(matches!(error, DfsError::Network { .. }));
    }

    #[test]
    fn configured_metadata_sync_rejects_a_replayed_response_for_another_head() {
        let replica_a = node("node_sync_replayed_replica_a");
        let replica_b = node("node_sync_replayed_replica_b");
        let mut fixture = configured_sync_fixture(
            "replayed_ack",
            vec![replica_a.clone(), replica_b],
            vec![replica_a],
        );
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture target");
        fixture.service.transport.response_sequence_override =
            Some(target.journal_sequence.saturating_sub(1));

        let error = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::ZERO)
            .expect_err("a delayed acknowledgement for another head must not be promoted");
        assert!(matches!(error, DfsError::Network { .. }));
    }

    #[test]
    fn metadata_sync_rejects_an_authority_epoch_change() {
        let replica = node("node_sync_epoch_replica");
        let mut fixture =
            configured_sync_fixture("epoch_change", vec![replica.clone()], vec![replica.clone()]);
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture target");
        let mut transferred = fixture.shard.clone();
        transferred.primary_node_id = replica;
        transferred.replica_node_ids = vec![fixture.shard.primary_node_id.clone()];
        transferred.epoch += 1;
        fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&transferred)
                .expect("transfer metadata authority");
        });

        let error = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::ZERO)
            .expect_err("stale target must be fenced");
        assert!(matches!(error, DfsError::StaleOwnerEpoch { epoch: 8, .. }));
    }

    #[test]
    fn metadata_sync_times_out_without_a_replica_ack() {
        let replica = node("node_sync_timeout_replica");
        let mut fixture = configured_sync_fixture("timeout", vec![replica], Vec::new());
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture target");

        let error = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::ZERO)
            .expect_err("missing acknowledgement must time out");
        assert!(
            matches!(error, DfsError::Network { message } if message.contains("1/2 acknowledgements"))
        );
    }

    #[test]
    fn file_metadata_sync_does_not_accept_a_disjoint_majority() {
        let content_holder = node("node_sync_intersection_content");
        let metadata_only = node("node_sync_intersection_metadata");
        let mut fixture = configured_sync_fixture(
            "intersection",
            vec![content_holder.clone(), metadata_only.clone()],
            vec![metadata_only],
        );
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture target");

        let error = fixture
            .service
            .sync_metadata_shard_to_quorum_intersecting(&target, &[content_holder], Duration::ZERO)
            .expect_err("disjoint metadata quorum is not a file-sync certificate");
        assert!(matches!(error, DfsError::Network { .. }));
    }
}
