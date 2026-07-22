//! Failover-recoverable metadata durability barriers.
//!
//! File-content receipt validation lives in `posix::sync`; this module owns the
//! lower metadata primitive shared by file and directory sync. A caller first
//! freezes a shard journal head while it is the current primary. The healthy
//! path obtains a fresh exact-prefix acknowledgement from every configured
//! shard member without touching Raft. If a member is unavailable, the
//! existing recovery-quorum requirements still apply and a compact,
//! operation-bound cluster-quorum head watermark fences membership movement
//! before the prefix is certified.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::metadata_records::{
    DFS_METADATA_PREFIX_PROOF_VERSION, DfsMetadataPrefixProof, dfs_metadata_prefix_proof_after,
};
use crate::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
    DfsControlPlaneStore, DfsError, DfsFileSystemId, DfsMessageTransport, DfsMetadataBackend,
    DfsMetadataJournalBatch, DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResult,
    DfsMetadataPrefixProbe, DfsMetadataShardConfig, DfsMetadataShardHeadWatermark,
    DfsMetadataShardId, DfsMetadataShardMembershipHandoff, DfsMetadataShardMembershipHandoffFence,
    DfsMetadataShardMembershipHandoffProof, DfsMetadataSnapshotInstallCommand, DfsNetworkMessage,
    DfsObjectMetadata, DfsRoutedCall, DfsRoutedSend, MetadataBackendError, NetworkedDfsService,
    NetworkedDfsServiceEvent, PendingMetadataReplication, sifs_file_system_id,
};

/// Default time budget for a foreground POSIX durability barrier.
pub const DEFAULT_DFS_POSIX_SYNC_TIMEOUT: Duration = Duration::from_secs(45);

const DFS_POSIX_SYNC_IDLE_WAIT: Duration = Duration::from_millis(10);
/// Give healthy WAN replicas one normal round trip to answer before taking the
/// Raft-backed partition fallback. A caller's shorter timeout remains the
/// upper bound.
const DFS_POSIX_FULL_SHARD_ACK_GRACE: Duration = Duration::from_millis(250);
/// Two canonical routed probes may run at once so one unavailable shard member
/// cannot prevent every later reachable quorum member from being attempted.
const DFS_METADATA_SYNC_ROUTED_FANOUT_CAP: usize = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
enum MetadataQuorumSyncPurpose {
    OrdinaryDurability,
    MembershipHandoff {
        /// Only the replica becoming primary receives a proof-bound marker
        /// request. Every other union member returns an ordinary exact-prefix
        /// acknowledgement.
        primary_handoff: Option<MetadataPrimaryHandoffProbe>,
        /// Exact assignments whose majorities must both acknowledge the
        /// retiring primary's fenced prefix before activation.
        retiring_shard: DfsMetadataShardConfig,
        desired_shard: DfsMetadataShardConfig,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetadataPrimaryHandoffProbe {
    handoff_id: String,
    promoted_node_id: NodeId,
}

/// One full-snapshot repair attempt for an exact target in one sync invocation.
///
/// The enclosing request map intentionally retains settled and unroutable
/// attempts until the target advances (or the invocation ends). Rebuilding an
/// identical deep snapshot cannot make an immediate rejection or missing route
/// succeed, and doing so on every 10 ms pump iteration amplifies memory use.
#[derive(Clone, Debug, Eq, PartialEq)]
struct MetadataCheckpointSnapshotAttempt {
    target_node_id: NodeId,
    shard_id: DfsMetadataShardId,
    journal_sequence: u64,
    state_fingerprint: String,
}

impl MetadataCheckpointSnapshotAttempt {
    fn for_target(target_node_id: NodeId, target: &DfsMetadataQuorumSyncTarget) -> Self {
        Self {
            target_node_id,
            shard_id: target.shard_id.clone(),
            journal_sequence: target.journal_sequence,
            state_fingerprint: target.state_fingerprint.clone(),
        }
    }
}

impl MetadataQuorumSyncPurpose {
    fn primary_handoff_id_for(&self, node_id: &NodeId) -> Option<&str> {
        match self {
            Self::MembershipHandoff {
                primary_handoff: Some(primary_handoff),
                ..
            } if &primary_handoff.promoted_node_id == node_id => {
                Some(primary_handoff.handoff_id.as_str())
            }
            _ => None,
        }
    }
}

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
    /// Exact committed assignment observed when the prefix was frozen. This
    /// is carried through routed sync requests so the authority cannot silently
    /// reinterpret an acknowledgement under another assignment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_shard: Option<DfsMetadataShardConfig>,
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

pub(crate) fn rebind_completed_metadata_shard_membership_handoff(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    active: &DfsMetadataShardConfig,
    pending: Option<&DfsMetadataShardMembershipHandoff>,
    proof: Option<&DfsMetadataShardMembershipHandoffProof>,
) -> Result<bool, DfsError> {
    if active.primary_node_id != *local_node_id {
        return Ok(false);
    }
    let shard_id = DfsMetadataShardId::new(active.shard_id.clone());
    if backend.metadata_shard_write_freeze(&shard_id)?.is_none() {
        return Ok(false);
    }
    // The completion proof and marker are durable, but this lane may have
    // opened before canonical replication caught up a different backend
    // handle. Refresh only the marker-bearing cold path before comparing the
    // exact completion prefix.
    backend.refresh_metadata_shard_durable_view(&shard_id)?;
    let Some(freeze) = backend.metadata_shard_write_freeze(&shard_id)? else {
        return Ok(false);
    };
    if pending.is_some_and(|pending| pending.handoff_id == freeze.handoff_id) {
        return Ok(false);
    }
    let Some(proof) = proof else {
        return Ok(false);
    };
    let exact_successor_epoch = freeze.retiring_epoch.checked_add(1);
    let marker_matches_active_epoch = freeze
        .activated_epoch
        .is_none_or(|activated_epoch| activated_epoch == active.epoch);
    let proof_matches_marker_and_active = proof.handoff_id == freeze.handoff_id
        && proof.retiring_shard.file_system_id == active.file_system_id
        && proof.retiring_shard.shard_id == active.shard_id
        && proof.retiring_shard.epoch == freeze.retiring_epoch
        && proof.desired_shard == *active
        && proof.journal_sequence == freeze.journal_sequence
        && proof.state_fingerprint == freeze.state_fingerprint
        && proof.acknowledged_node_ids.contains(local_node_id);
    if exact_successor_epoch != Some(active.epoch)
        || !marker_matches_active_epoch
        || !proof_matches_marker_and_active
        || !metadata_shard_prefix_matches_or_extends_marker(
            backend,
            &shard_id,
            freeze.journal_sequence,
            &freeze.state_fingerprint,
        )?
    {
        return Ok(false);
    }
    Ok(backend.clear_metadata_shard_membership_handoff_freeze(
        &shard_id,
        &freeze.handoff_id,
        active.epoch,
    )?)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Rebinds only completed, exact-proof SIFS metadata handoffs for a newly
    /// opened service lane.
    ///
    /// This is a cold-path availability guard for hosts that cache service
    /// handles. It does not advance handoffs or retry mutations. A positive
    /// result means the service was fully rehydrated after clearing its exact
    /// durable marker and is ready to serve authoritative SIFS mutations.
    pub fn rebind_completed_sifs_metadata_shard_membership_handoffs(
        &mut self,
    ) -> Result<usize, DfsError> {
        self.rebind_completed_local_metadata_shard_membership_handoffs(&sifs_file_system_id())
    }

    /// Rebind durable local membership markers whose exact handoff is already
    /// complete in the freshly observed control plane.
    ///
    /// This is an availability-only open/restart path. It never advances a
    /// pending handoff, contacts metadata peers, or writes cluster KV. A local
    /// backend is rebound only when the current assignment, last completion
    /// proof, and durable marker all identify the same exact successor epoch
    /// and this node is that assignment's primary. Backend activation may run
    /// deferred append-producing recovery, so successful rebinds are followed
    /// by a full in-memory hydration before the service can serve mutations.
    pub(crate) fn rebind_completed_local_metadata_shard_membership_handoffs(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<usize, DfsError> {
        let Some(backend) = self.metadata_backend.as_deref() else {
            return Ok(0);
        };
        if !backend.has_any_metadata_shard_write_freeze()? {
            return Ok(0);
        }
        self.refresh_control_plane_latest()?;
        let observed = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(0);
            };
            let store = DfsControlPlaneStore::new(control_plane.as_mut());
            store
                .list_metadata_shards(file_system_id)?
                .into_iter()
                .map(|active| {
                    let pending = store.get_metadata_shard_membership_handoff(
                        &active.file_system_id,
                        &active.shard_id,
                    )?;
                    let proof = store.get_metadata_shard_membership_handoff_proof(
                        &active.file_system_id,
                        &active.shard_id,
                    )?;
                    Ok((active, pending, proof))
                })
                .collect::<Result<Vec<_>, sinomo_core::kv::KvError>>()?
        };

        let Some(backend) = self.metadata_backend.as_deref_mut() else {
            return Ok(0);
        };
        let mut rebound = 0usize;
        for (active, pending, proof) in observed {
            if rebind_completed_metadata_shard_membership_handoff(
                backend,
                &self.node_id,
                &active,
                pending.as_ref(),
                proof.as_ref(),
            )? {
                rebound = rebound.saturating_add(1);
            }
        }
        if rebound > 0 {
            // `clear_*` runs the append-producing recovery deliberately
            // deferred by backend open while a marker is present. Reflect any
            // recovered objects, transactions, and coordinator state before
            // mounted bootstrap performs its first namespace mutation.
            self.hydrate_from_metadata_backend()?;
        }
        Ok(rebound)
    }

    /// Complete every pending membership handoff for which this service is the
    /// retiring primary.
    ///
    /// A quorum-side pending record does not by itself fence a partitioned
    /// retiring primary. This method refreshes that primary's control plane
    /// under its local publication guard, durably acknowledges the exact head
    /// observed after all earlier commits, repairs and freshly probes a joint
    /// quorum of the retiring and desired assignments, and only then activates
    /// it.
    pub fn reconcile_pending_metadata_shard_membership_handoffs(
        &mut self,
        metadata_shards: &[DfsMetadataShardConfig],
        timeout: Duration,
    ) -> Result<usize, DfsError> {
        // Activation does not mutate a different process or service handle's
        // local authority binding. Before looking for work owned by a retiring
        // primary, let this lane prove and bind any completed generation for
        // which it is now the active primary.
        let file_system_ids = metadata_shards
            .iter()
            .map(|shard| shard.file_system_id.clone())
            .collect::<BTreeSet<_>>();
        for file_system_id in file_system_ids {
            self.rebind_completed_local_metadata_shard_membership_handoffs(&file_system_id)?;
        }
        self.refresh_control_plane_latest()?;
        let observed = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(0);
            };
            let store = DfsControlPlaneStore::new(control_plane.as_mut());
            metadata_shards
                .iter()
                .map(|shard| {
                    let active =
                        store.get_metadata_shard(&shard.file_system_id, &shard.shard_id)?;
                    let pending = store.get_metadata_shard_membership_handoff(
                        &shard.file_system_id,
                        &shard.shard_id,
                    )?;
                    Ok((active, pending))
                })
                .collect::<Result<Vec<_>, sinomo_core::kv::KvError>>()?
        };
        let pending = observed
            .iter()
            .filter_map(|(_, handoff)| handoff.clone())
            .filter(|handoff| handoff.retiring_shard.primary_node_id == self.node_id)
            .collect::<Vec<_>>();

        let mut completed = 0;
        for handoff in pending {
            if self.reconcile_metadata_shard_membership_handoff(&handoff, timeout)? {
                completed += 1;
            }
        }
        Ok(completed)
    }

    fn reconcile_metadata_shard_membership_handoff(
        &mut self,
        requested_handoff: &DfsMetadataShardMembershipHandoff,
        timeout: Duration,
    ) -> Result<bool, DfsError> {
        let requested_retiring = &requested_handoff.retiring_shard;
        if requested_retiring.primary_node_id != self.node_id
            || requested_handoff.desired_shard.file_system_id != requested_retiring.file_system_id
            || requested_handoff.desired_shard.shard_id != requested_retiring.shard_id
            || requested_handoff.desired_shard.epoch != requested_retiring.epoch.saturating_add(1)
        {
            return Ok(false);
        }

        let shard_id = DfsMetadataShardId::new(requested_retiring.shard_id.clone());
        let (handoff, fence) = {
            // This is the retiring-primary half of the distributed fence. A
            // canonical commit that already passed validation holds this same
            // guard through its backend publication and therefore finishes
            // before the captured head. A later commit observes the refreshed
            // durable pending handoff and fails validation.
            let _publication_guard = crate::service_replica::metadata_shard_publication_guard(
                &requested_retiring.file_system_id,
                &requested_retiring.shard_id,
            );
            self.refresh_control_plane_latest()?;
            let observed_handoff = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Ok(false);
                };
                let store = DfsControlPlaneStore::new(control_plane.as_mut());
                if store.get_metadata_shard(
                    &requested_retiring.file_system_id,
                    &requested_retiring.shard_id,
                )? != Some(requested_retiring.clone())
                {
                    return Ok(false);
                }
                let Some(observed) = store.get_metadata_shard_membership_handoff(
                    &requested_retiring.file_system_id,
                    &requested_retiring.shard_id,
                )?
                else {
                    return Ok(false);
                };
                if observed.handoff_id != requested_handoff.handoff_id
                    || observed.retiring_shard != requested_handoff.retiring_shard
                    || observed.desired_shard != requested_handoff.desired_shard
                    || observed.required_prepared_sequence
                        != requested_handoff.required_prepared_sequence
                    || observed.required_head_watermark != requested_handoff.required_head_watermark
                {
                    return Ok(false);
                }
                observed
            };

            let backend =
                self.metadata_backend
                    .as_deref_mut()
                    .ok_or_else(|| DfsError::Metadata {
                        message: format!(
                            "metadata shard {} membership handoff requires a durable local backend",
                            requested_retiring.shard_id
                        ),
                    })?;
            let Some(write_freeze) = backend.freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                &observed_handoff.handoff_id,
                requested_retiring.epoch,
            )?
            else {
                return Ok(false);
            };
            if write_freeze.journal_sequence < observed_handoff.required_prepared_sequence {
                return Ok(false);
            }
            let proposed_fence = DfsMetadataShardMembershipHandoffFence {
                handoff_id: observed_handoff.handoff_id.clone(),
                primary_node_id: self.node_id.clone(),
                journal_sequence: write_freeze.journal_sequence,
                state_fingerprint: write_freeze.state_fingerprint,
            };
            if observed_handoff
                .retiring_primary_fence
                .as_ref()
                .is_some_and(|existing| existing != &proposed_fence)
            {
                return Ok(false);
            }
            let acknowledged_handoff = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Ok(false);
                };
                DfsControlPlaneStore::new(control_plane.as_mut())
                    .acknowledge_metadata_shard_membership_handoff_fence(
                        &observed_handoff,
                        &proposed_fence,
                    )?
            };
            let Some(acknowledged_handoff) = acknowledged_handoff else {
                return Ok(false);
            };
            let fence = acknowledged_handoff
                .retiring_primary_fence
                .clone()
                .expect("successful handoff acknowledgement stores the fence");
            (acknowledged_handoff, fence)
        };

        let retiring = &handoff.retiring_shard;
        let journal_sequence = fence.journal_sequence;
        let state_fingerprint = fence.state_fingerprint.clone();
        let retiring_node_ids = std::iter::once(retiring.primary_node_id.clone())
            .chain(retiring.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        let desired_node_ids = std::iter::once(handoff.desired_shard.primary_node_id.clone())
            .chain(handoff.desired_shard.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        let handoff_node_ids = retiring_node_ids
            .union(&desired_node_ids)
            .cloned()
            .collect::<BTreeSet<_>>();
        let replica_node_ids = handoff_node_ids
            .iter()
            .filter(|node_id| *node_id != &self.node_id)
            .cloned()
            .collect::<Vec<_>>();
        let target = DfsMetadataQuorumSyncTarget {
            file_system_id: retiring.file_system_id.clone(),
            shard_id: shard_id.clone(),
            primary_node_id: self.node_id.clone(),
            authority_epoch: Some(retiring.epoch),
            replica_node_ids,
            expected_shard: Some(retiring.clone()),
            journal_sequence,
            barrier_id: format!("membership-handoff-{}", handoff.handoff_id),
            state_fingerprint: state_fingerprint.clone(),
            checkpoint_snapshot: None,
        };
        let primary_handoff = (handoff.desired_shard.primary_node_id != retiring.primary_node_id)
            .then(|| MetadataPrimaryHandoffProbe {
                handoff_id: handoff.handoff_id.clone(),
                promoted_node_id: handoff.desired_shard.primary_node_id.clone(),
            });
        let certificate = self.sync_metadata_shard_to_quorum_with_required_intersection(
            &target,
            &BTreeSet::new(),
            MetadataQuorumSyncPurpose::MembershipHandoff {
                primary_handoff,
                retiring_shard: retiring.clone(),
                desired_shard: handoff.desired_shard.clone(),
            },
            timeout,
        )?;
        let acknowledged_node_ids = certificate
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if !crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
            retiring,
            &handoff.desired_shard,
            &acknowledged_node_ids,
        ) {
            return Ok(false);
        }

        // Hold the retiring-primary guard across the final exact local-state
        // check and activation CAS. The control-plane transaction also
        // compares the immutable fence acknowledgement and operation floor.
        let publication_guard = crate::service_replica::metadata_shard_publication_guard(
            &retiring.file_system_id,
            &retiring.shard_id,
        );
        self.refresh_control_plane_latest()?;
        let unchanged = {
            let backend = self
                .metadata_backend
                .as_deref()
                .expect("membership handoff backend checked above");
            let current_sequence = backend
                .metadata_shard_statuses()?
                .into_iter()
                .find(|status| status.shard_id == shard_id)
                .map(|status| status.journal_last_sequence);
            current_sequence == Some(journal_sequence)
                && metadata_shard_state_fingerprint_at(backend, &shard_id, journal_sequence)?
                    == state_fingerprint
        };
        if !unchanged {
            return Ok(false);
        }

        let proof = DfsMetadataShardMembershipHandoffProof {
            handoff_id: handoff.handoff_id.clone(),
            retiring_shard: retiring.clone(),
            desired_shard: handoff.desired_shard.clone(),
            journal_sequence,
            state_fingerprint,
            acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
        };
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(false);
        };
        let completed = DfsControlPlaneStore::new(control_plane.as_mut())
            .complete_metadata_shard_membership_handoff(retiring, &handoff.desired_shard, &proof)
            .map_err(DfsError::from)?;
        if completed {
            let backend = self
                .metadata_backend
                .as_deref_mut()
                .expect("membership handoff backend checked above");
            if handoff.desired_shard.primary_node_id == self.node_id {
                backend.clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    &handoff.handoff_id,
                    handoff.desired_shard.epoch,
                )?;
            } else {
                // Record completion durably but never bind the retiring
                // instance to the promoted epoch. This lane remains fenced;
                // only the promoted node can consume the exact completion
                // proof and its own handoff-bound local marker.
                backend.mark_metadata_shard_membership_handoff_activated(
                    &shard_id,
                    &handoff.handoff_id,
                    handoff.desired_shard.epoch,
                )?;
            }
        }
        drop(publication_guard);
        Ok(completed)
    }

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
        let state_fingerprint = metadata_shard_state_fingerprint_at(
            backend.as_ref(),
            &status.shard_id,
            status.journal_last_sequence,
        )?;
        let barrier_id = sinomo_core::Id::generate().to_string();
        let expected_shard = configured.clone();
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
            expected_shard,
            journal_sequence: status.journal_last_sequence,
            barrier_id,
            state_fingerprint,
            checkpoint_snapshot: None,
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
            MetadataQuorumSyncPurpose::OrdinaryDurability,
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
            MetadataQuorumSyncPurpose::OrdinaryDurability,
            timeout,
        )
    }

    fn sync_metadata_shard_to_quorum_with_required_intersection(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        required_intersection: &BTreeSet<NodeId>,
        purpose: MetadataQuorumSyncPurpose,
        timeout: Duration,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        // A routed peer may have accepted the request but still be waiting for
        // its local DFS service lane. Session keepalives are transport progress,
        // not application progress, so the network's idle timer deliberately
        // cannot bound that wait. Carry this barrier's semantic deadline into
        // every routed/direct send instead. This also breaks the cross-node
        // hold-and-wait cycle when several maintenance passes own their local
        // backend lane at the same time.
        let requested_deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let previous_deadline = self
            .transport
            .set_operation_deadline(Some(requested_deadline));
        let effective_deadline = previous_deadline
            .filter(|existing| *existing < requested_deadline)
            .unwrap_or(requested_deadline);
        if effective_deadline != requested_deadline {
            self.transport
                .set_operation_deadline(Some(effective_deadline));
        }
        let result = self.sync_metadata_shard_to_quorum_with_required_intersection_inner(
            target,
            required_intersection,
            &purpose,
            timeout,
        );
        self.transport.set_operation_deadline(previous_deadline);
        self.clear_pending_metadata_sync_replications(target);
        result
    }

    fn try_finalize_metadata_sync_progress(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        freshly_acknowledged: &BTreeSet<NodeId>,
        required_intersection: &BTreeSet<NodeId>,
        purpose: &MetadataQuorumSyncPurpose,
        started_at: &Instant,
        full_shard_grace: Duration,
    ) -> Result<Option<DfsMetadataQuorumSyncCertificate>, DfsError> {
        let progress =
            metadata_sync_progress_from_fresh_acknowledgements(target, freshly_acknowledged);
        if metadata_sync_progress_is_full_shard(target, &progress)
            && metadata_sync_progress_satisfies(&progress, required_intersection)
        {
            self.clear_pending_metadata_sync_replications(target);
            return self
                .finalize_metadata_full_shard_sync(target, progress)
                .map(Some);
        }
        if metadata_sync_progress_satisfies(&progress, required_intersection)
            && started_at.elapsed() >= full_shard_grace
            && let Some(certificate) =
                self.try_finalize_metadata_quorum_sync_fallback(target, progress, purpose)?
        {
            self.clear_pending_metadata_sync_replications(target);
            return Ok(Some(certificate));
        }
        Ok(None)
    }

    fn sync_metadata_shard_to_quorum_with_required_intersection_inner(
        &mut self,
        requested_target: &DfsMetadataQuorumSyncTarget,
        required_intersection: &BTreeSet<NodeId>,
        purpose: &MetadataQuorumSyncPurpose,
        timeout: Duration,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        let mut target = requested_target.clone();
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
        let full_shard_grace = DFS_POSIX_FULL_SHARD_ACK_GRACE.min(timeout);
        let mut freshly_acknowledged = BTreeSet::from([self.node_id.clone()]);
        let mut pending_prefix_probes = BTreeMap::<String, NodeId>::new();
        let mut pending_checkpoint_snapshots =
            BTreeMap::<String, MetadataCheckpointSnapshotAttempt>::new();
        // A caller can capture a target and then wait behind another DFS
        // operation before entering the barrier. Upgrade once to the current
        // exact contiguous head before the first probe. Subsequent local
        // appends do not force this barrier to chase an unbounded moving head;
        // a later prefix remains a valid stronger target only when a receiver
        // rejection proves the requested historical head is no longer usable.
        self.refresh_metadata_sync_target_to_current_head(&mut target)?;
        loop {
            self.check_cancelled("metadata quorum sync")?;
            self.revalidate_metadata_quorum_sync_authority(&target)?;
            if let Some(certificate) = self.try_finalize_metadata_sync_progress(
                &target,
                &freshly_acknowledged,
                required_intersection,
                purpose,
                &started_at,
                full_shard_grace,
            )? {
                return Ok(certificate);
            }

            // A membership handoff has already frozen authoritative writes, so
            // probe replicas known at that exact head before spending the
            // barrier's deadline catching up a lagging member. A reachable
            // joint quorum can therefore complete around one offline replica.
            // Ordinary durability barriers retain catch-up-first ordering:
            // unrelated incoming work may otherwise advance their live head
            // before the requested historical prefix is delivered.
            if matches!(purpose, MetadataQuorumSyncPurpose::OrdinaryDurability) {
                self.send_metadata_sync_catchup_batches(&target, &probe_barrier_id)?;
            }
            let mut rejected_nodes = self.send_metadata_sync_prefix_probes(
                &target,
                &probe_barrier_id,
                &mut freshly_acknowledged,
                &mut pending_prefix_probes,
                purpose,
            )?;

            let events = self.process_incoming()?;
            rejected_nodes.extend(record_metadata_sync_prefix_probe_events(
                events,
                &target,
                &mut pending_prefix_probes,
                &mut freshly_acknowledged,
            ));
            if !rejected_nodes.is_empty()
                && self.refresh_metadata_sync_target_to_current_head(&mut target)?
            {
                self.clear_superseded_metadata_sync_requests(
                    &mut pending_prefix_probes,
                    &mut pending_checkpoint_snapshots,
                );
                freshly_acknowledged.clear();
                freshly_acknowledged.insert(self.node_id.clone());
            }
            if let Some(certificate) = self.try_finalize_metadata_sync_progress(
                &target,
                &freshly_acknowledged,
                required_intersection,
                purpose,
                &started_at,
                full_shard_grace,
            )? {
                return Ok(certificate);
            }
            self.send_metadata_sync_checkpoint_snapshots(
                &target,
                &probe_barrier_id,
                &rejected_nodes,
                &mut pending_checkpoint_snapshots,
            )?;
            self.send_metadata_sync_catchup_batches(&target, &probe_barrier_id)?;
            let mut rejected_nodes = self.send_metadata_sync_prefix_probes(
                &target,
                &probe_barrier_id,
                &mut freshly_acknowledged,
                &mut pending_prefix_probes,
                purpose,
            )?;
            let events = self.process_incoming()?;
            rejected_nodes.extend(record_metadata_sync_prefix_probe_events(
                events,
                &target,
                &mut pending_prefix_probes,
                &mut freshly_acknowledged,
            ));
            if !rejected_nodes.is_empty()
                && self.refresh_metadata_sync_target_to_current_head(&mut target)?
            {
                self.clear_superseded_metadata_sync_requests(
                    &mut pending_prefix_probes,
                    &mut pending_checkpoint_snapshots,
                );
                freshly_acknowledged.clear();
                freshly_acknowledged.insert(self.node_id.clone());
            }
            if let Some(certificate) = self.try_finalize_metadata_sync_progress(
                &target,
                &freshly_acknowledged,
                required_intersection,
                purpose,
                &started_at,
                full_shard_grace,
            )? {
                return Ok(certificate);
            }
            self.send_metadata_sync_checkpoint_snapshots(
                &target,
                &probe_barrier_id,
                &rejected_nodes,
                &mut pending_checkpoint_snapshots,
            )?;
            // A loopback/in-process transport can acknowledge the catch-up
            // batch during the process pass above. Drive the newly eligible
            // exact probe immediately so a zero-wait caller does not need an
            // artificial extra loop; real network transports simply leave it
            // pending for the next wait/pump round.
            self.send_metadata_sync_catchup_batches(&target, &probe_barrier_id)?;
            let mut rejected_nodes = self.send_metadata_sync_prefix_probes(
                &target,
                &probe_barrier_id,
                &mut freshly_acknowledged,
                &mut pending_prefix_probes,
                purpose,
            )?;
            let events = self.process_incoming()?;
            rejected_nodes.extend(record_metadata_sync_prefix_probe_events(
                events,
                &target,
                &mut pending_prefix_probes,
                &mut freshly_acknowledged,
            ));
            if !rejected_nodes.is_empty()
                && self.refresh_metadata_sync_target_to_current_head(&mut target)?
            {
                self.clear_superseded_metadata_sync_requests(
                    &mut pending_prefix_probes,
                    &mut pending_checkpoint_snapshots,
                );
                freshly_acknowledged.clear();
                freshly_acknowledged.insert(self.node_id.clone());
            }
            if let Some(certificate) = self.try_finalize_metadata_sync_progress(
                &target,
                &freshly_acknowledged,
                required_intersection,
                purpose,
                &started_at,
                full_shard_grace,
            )? {
                return Ok(certificate);
            }
            self.send_metadata_sync_checkpoint_snapshots(
                &target,
                &probe_barrier_id,
                &rejected_nodes,
                &mut pending_checkpoint_snapshots,
            )?;

            let progress =
                metadata_sync_progress_from_fresh_acknowledgements(&target, &freshly_acknowledged);
            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                if metadata_sync_progress_satisfies(&progress, required_intersection)
                    && let Some(certificate) = self.try_finalize_metadata_quorum_sync_fallback(
                        &target,
                        progress.clone(),
                        purpose,
                    )?
                {
                    self.clear_pending_metadata_sync_replications(&target);
                    return Ok(certificate);
                }
                self.clear_pending_metadata_sync_replications(&target);
                let recovery_intersection = required_intersection.is_empty()
                    || progress
                        .acknowledged_node_ids
                        .iter()
                        .any(|node_id| required_intersection.contains(node_id));
                let required_node_ids = std::iter::once(target.primary_node_id.clone())
                    .chain(target.replica_node_ids.iter().cloned())
                    .collect::<BTreeSet<_>>();
                let acknowledged_node_ids = progress
                    .acknowledged_node_ids
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let missing_node_ids = required_node_ids
                    .difference(&acknowledged_node_ids)
                    .cloned()
                    .collect::<Vec<_>>();
                return Err(DfsError::Network {
                    message: format!(
                        "metadata shard {} did not reach a POSIX sync certificate before {}ms: {}/{} acknowledgements through sequence {}; missing={missing_node_ids:?}; recovery-set intersection={recovery_intersection}",
                        target.shard_id.0,
                        timeout.as_millis(),
                        progress.acknowledged_node_ids.len(),
                        required_node_ids.len(),
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

    fn refresh_metadata_sync_target_to_current_head(
        &mut self,
        target: &mut DfsMetadataQuorumSyncTarget,
    ) -> Result<bool, DfsError> {
        self.revalidate_metadata_quorum_sync_authority(target)?;
        let backend = self
            .metadata_backend
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata sync target refresh requires a durable backend".to_string(),
            })?;
        let current_sequence = backend
            .metadata_shard_statuses()?
            .into_iter()
            .find(|status| status.shard_id == target.shard_id)
            .map(|status| status.journal_last_sequence)
            .ok_or_else(|| DfsError::Metadata {
                message: format!(
                    "metadata shard {} disappeared while refreshing a POSIX sync target",
                    target.shard_id.0
                ),
            })?;
        if current_sequence < target.journal_sequence {
            return Err(DfsError::Metadata {
                message: format!(
                    "metadata shard {} regressed below POSIX sync target {} to {}",
                    target.shard_id.0, target.journal_sequence, current_sequence
                ),
            });
        }
        if current_sequence == target.journal_sequence {
            return Ok(false);
        }
        let state_fingerprint =
            metadata_shard_state_fingerprint_at(backend, &target.shard_id, current_sequence)?;
        target.journal_sequence = current_sequence;
        target.state_fingerprint = state_fingerprint;
        target.checkpoint_snapshot = None;
        Ok(true)
    }

    fn clear_superseded_metadata_sync_requests(
        &mut self,
        pending_prefix_probes: &mut BTreeMap<String, NodeId>,
        pending_checkpoint_snapshots: &mut BTreeMap<String, MetadataCheckpointSnapshotAttempt>,
    ) {
        for request_id in pending_prefix_probes
            .keys()
            .chain(pending_checkpoint_snapshots.keys())
        {
            self.pending_metadata_replications.remove(request_id);
        }
        pending_prefix_probes.clear();
        pending_checkpoint_snapshots.clear();
    }

    /// Close an ordinary replica journal lag with contiguous DFS metadata
    /// batches before asking that replica to attest the frozen prefix.
    ///
    /// The fresh prefix probe remains empty for rolling-upgrade safety. Sending
    /// it to a replica whose sender cursor is known to be behind would only
    /// manufacture a rejection and force an expensive full-snapshot repair.
    /// Sender cursors are still scheduling hints rather than proof: once they
    /// reach the target, the operation-bound probe below is what certifies the
    /// receiver's exact hash-chain head.
    fn send_metadata_sync_catchup_batches(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        probe_barrier_id: &str,
    ) -> Result<(), DfsError> {
        self.pending_metadata_replications
            .retain(|_, pending| pending.created_at.elapsed() < Duration::from_secs(60));
        let Some(backend) = self.metadata_backend.as_deref() else {
            return Ok(());
        };
        let replica_cursors = target
            .replica_node_ids
            .iter()
            .map(|replica_node_id| {
                Ok((
                    replica_node_id.clone(),
                    backend.metadata_replication_cursor(replica_node_id, &target.shard_id)?,
                ))
            })
            .collect::<Result<Vec<_>, MetadataBackendError>>()?;

        // Replicas at the same cursor need the same frozen journal prefix.
        // Read it once, then clone only the prepared wire value per target
        // instead of repeating backend scans before every fan-out wave.
        let mut prepared_batches_by_cursor =
            BTreeMap::<u64, Option<DfsMetadataJournalBatch>>::new();
        let mut prepared_catchups = Vec::new();
        for (target_node_id, after_sequence) in replica_cursors {
            if target_node_id == self.node_id || after_sequence >= target.journal_sequence {
                continue;
            }
            if self.pending_metadata_replications.values().any(|pending| {
                pending.target_node_id == target_node_id
                    && pending.shard_id == target.shard_id
                    && !pending.verify_prefix_only
            }) {
                continue;
            }
            let prepared_batch =
                if let Some(cached) = prepared_batches_by_cursor.get(&after_sequence) {
                    cached.clone()
                } else {
                    let prepared = self
                        .metadata_journal_batch_for_shard_since_limited(
                            &target.shard_id,
                            after_sequence,
                            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
                        )?
                        .and_then(|mut batch| {
                            // The local shard can advance after the barrier
                            // captured its target. Never push that successor here
                            // and then demand a proof for the older head: the
                            // receiver retains only its current O(1) prefix proof.
                            batch
                                .entries
                                .retain(|entry| entry.sequence <= target.journal_sequence);
                            let last_sequence = batch.entries.last().map(|entry| entry.sequence)?;
                            batch.next_after_sequence = last_sequence;
                            Some(batch)
                        });
                    prepared_batches_by_cursor.insert(after_sequence, prepared.clone());
                    prepared
                };
            let Some(batch) = prepared_batch else {
                continue;
            };
            let next_after_sequence = batch.next_after_sequence;
            let request_id = format!(
                "dfs_sync_catchup_{}_{}",
                probe_barrier_id,
                self.next_request_id()
            );
            let command = DfsMetadataJournalBatchCommand {
                request_id: request_id.clone(),
                source_node_id: Some(self.node_id.clone()),
                file_system_id: Some(target.file_system_id.clone()),
                authority_epoch: target.authority_epoch,
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch,
            };
            prepared_catchups.push((
                target_node_id.clone(),
                command,
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: target.shard_id.clone(),
                    next_after_sequence,
                    verify_prefix_only: false,
                    required_authority_confirmation_id: None,
                    created_at: Instant::now(),
                },
            ));
        }

        let direct_requests = prepared_catchups
            .iter()
            .map(|(node_id, command, _)| (node_id.clone(), command.clone()))
            .collect::<Vec<_>>();
        let directly_completed_catchups = if direct_requests.is_empty() {
            BTreeSet::new()
        } else {
            match self
                .transport
                .send_metadata_journal_batches_direct(&direct_requests)
                .map_err(DfsError::from_network)?
            {
                Some(responses) => {
                    let mut pending_by_response = prepared_catchups
                        .iter()
                        .map(|(node_id, command, pending)| {
                            (
                                (node_id.clone(), command.request_id.clone()),
                                pending.clone(),
                            )
                        })
                        .collect::<BTreeMap<_, _>>();
                    let mut completed = BTreeSet::new();
                    for (source_node_id, response) in responses {
                        let response_key = (source_node_id.clone(), response.request_id.clone());
                        let Some(pending) = pending_by_response.remove(&response_key) else {
                            continue;
                        };
                        completed.insert(response_key);
                        self.pending_metadata_replications
                            .insert(response.request_id.clone(), pending);
                        let _ =
                            self.handle_metadata_journal_batch_response(source_node_id, response)?;
                    }
                    completed
                }
                None => BTreeSet::new(),
            }
        };

        for (target_node_id, command, pending) in prepared_catchups {
            let request_id = command.request_id.clone();
            if directly_completed_catchups.contains(&(target_node_id.clone(), request_id.clone())) {
                continue;
            }
            // The direct path is an accelerator and may legally
            // return only the siblings that completed before its deadline.
            // Preserve the same request identity and enqueue every omitted
            // target on the canonical routed path so a slow first handshake
            // cannot livelock a durability barrier.
            self.pending_metadata_replications
                .insert(request_id.clone(), pending);
            let sent = self.send_best_effort_message_if_routable(
                &target_node_id,
                DfsNetworkMessage::MetadataJournalBatch(command),
            )?;
            if !sent {
                self.pending_metadata_replications.remove(&request_id);
            }
        }
        Ok(())
    }

    fn send_metadata_sync_prefix_probes(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        probe_barrier_id: &str,
        freshly_acknowledged: &mut BTreeSet<NodeId>,
        pending_prefix_probes: &mut BTreeMap<String, NodeId>,
        purpose: &MetadataQuorumSyncPurpose,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        if target.replica_node_ids.is_empty() {
            return Ok(BTreeSet::new());
        }
        if target.barrier_id.is_empty() || target.state_fingerprint.is_empty() {
            return Err(DfsError::Metadata {
                message: format!(
                    "clustered metadata shard {} has an incomplete POSIX sync checkpoint identity",
                    target.shard_id.0
                ),
            });
        }

        let prefix_probe = DfsMetadataPrefixProbe {
            barrier_id: probe_barrier_id.to_string(),
            file_system_id: Some(target.file_system_id.clone()),
            authority_epoch: target.authority_epoch,
            shard_id: target.shard_id.clone(),
            journal_sequence: target.journal_sequence,
            state_fingerprint: target.state_fingerprint.clone(),
            primary_handoff_id: None,
        };
        let probe = DfsMetadataJournalBatch {
            shard_id: target.shard_id.clone(),
            // This must remain empty for rolling-upgrade safety: a legacy peer
            // ignores the new probe fields and processes the command as a
            // harmless no-op instead of jumping its replay cursor.
            entries: Vec::new(),
            next_after_sequence: target.journal_sequence,
        };

        let mut candidates = Vec::new();
        for target_node_id in &target.replica_node_ids {
            if freshly_acknowledged.contains(target_node_id)
                || pending_prefix_probes
                    .values()
                    .any(|pending_node_id| pending_node_id == target_node_id)
            {
                continue;
            }
            if let Some(backend) = self.metadata_backend.as_deref()
                && backend.metadata_replication_cursor(target_node_id, &target.shard_id)?
                    < target.journal_sequence
            {
                continue;
            }
            let request_id = format!(
                "dfs_sync_probe_{}_{}",
                probe_barrier_id,
                self.next_request_id()
            );
            let required_authority_confirmation_id = purpose
                .primary_handoff_id_for(target_node_id)
                .map(str::to_string);
            let mut node_prefix_probe = prefix_probe.clone();
            node_prefix_probe.primary_handoff_id = required_authority_confirmation_id.clone();
            candidates.push((
                target_node_id.clone(),
                DfsMetadataJournalBatchCommand {
                    request_id,
                    source_node_id: Some(self.node_id.clone()),
                    file_system_id: Some(target.file_system_id.clone()),
                    authority_epoch: target.authority_epoch,
                    verify_prefix_only: true,
                    prefix_probe: Some(node_prefix_probe),
                    authority_probe: None,
                    batch: probe.clone(),
                },
                required_authority_confirmation_id,
            ));
        }

        // Ordinary exact-prefix probes must use the canonical metadata route.
        // Canonical replication mutates that route's cached backend, whereas
        // the daemon's routed DFS message lane is a separately opened,
        // long-lived instance and can hold a stale prefix after a direct
        // authority apply. Promoted-primary probes remain routed because the
        // message lane owns the proof-bound local promotion marker.
        let direct_requests = candidates
            .iter()
            .filter(|(_, _, confirmation)| confirmation.is_none())
            .map(|(node_id, command, _)| (node_id.clone(), command.clone()))
            .collect::<Vec<_>>();
        let mut rejected_node_ids = BTreeSet::new();
        let directly_completed_probes = if direct_requests.is_empty() {
            BTreeSet::new()
        } else {
            match self
                .transport
                .send_metadata_journal_batches_direct(&direct_requests)
                .map_err(DfsError::from_network)?
            {
                Some(responses) => {
                    let mut completed = BTreeSet::new();
                    for (source_node_id, response) in responses {
                        let expected = direct_requests.iter().find(|(target_node_id, command)| {
                            target_node_id == &source_node_id
                                && command.request_id == response.request_id
                        });
                        if expected.is_some() {
                            completed.insert((source_node_id.clone(), response.request_id.clone()));
                        }
                        let accepted = expected.is_some_and(|(_, command)| {
                            response.shard_id == command.batch.shard_id
                                && response.next_after_sequence == command.batch.next_after_sequence
                                && response.verified_prefix_only
                                && response.verified_authority_probe_id.is_none()
                                && matches!(response.result, DfsMetadataJournalBatchResult::Applied)
                        });
                        if accepted {
                            freshly_acknowledged.insert(source_node_id);
                        } else {
                            rejected_node_ids.insert(source_node_id);
                        }
                    }
                    completed
                }
                None => BTreeSet::new(),
            }
        };

        let mut routed_probes = Vec::new();
        for (target_node_id, command, required_authority_confirmation_id) in candidates {
            let request_id = command.request_id.clone();
            if required_authority_confirmation_id.is_none()
                && directly_completed_probes.contains(&(target_node_id.clone(), request_id.clone()))
            {
                continue;
            }
            self.pending_metadata_replications.insert(
                request_id.clone(),
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: target.shard_id.clone(),
                    next_after_sequence: target.journal_sequence,
                    verify_prefix_only: true,
                    required_authority_confirmation_id,
                    created_at: Instant::now(),
                },
            );
            let routed_message = self.next_dfs_routed_message(
                &target_node_id,
                DfsNetworkMessage::MetadataJournalBatch(command),
            );
            routed_probes.push((
                target_node_id.clone(),
                request_id,
                DfsRoutedSend::new(target_node_id, DfsRoutedCall::value(routed_message)),
            ));
        }

        // The direct metadata lane above is only a short accelerator. Route
        // every omitted probe in one bounded fan-out wave so an offline member
        // cannot consume the operation deadline before a later reachable
        // quorum member is even attempted. The production node transport
        // forks one independent send handle per in-flight request; transports
        // that cannot safely fork retain the trait's sequential fallback.
        if routed_probes.is_empty() {
            return Ok(rejected_node_ids);
        }
        let max_inflight = routed_probes.len().min(DFS_METADATA_SYNC_ROUTED_FANOUT_CAP);
        let expected = routed_probes
            .iter()
            .map(|(target_node_id, request_id, _)| (target_node_id.clone(), request_id.clone()))
            .collect::<Vec<_>>();
        let outcomes = self
            .transport
            .send_routed_calls_bounded(
                routed_probes
                    .into_iter()
                    .map(|(_, _, request)| request)
                    .collect(),
                max_inflight,
            )
            .map_err(DfsError::from_network)?;
        if outcomes.len() != expected.len() {
            return Err(DfsError::Network {
                message: format!(
                    "metadata prefix probe fan-out returned {} outcomes for {} requests",
                    outcomes.len(),
                    expected.len()
                ),
            });
        }
        for ((expected_target_node_id, request_id), outcome) in expected.into_iter().zip(outcomes) {
            if outcome.target_node_id != expected_target_node_id {
                return Err(DfsError::Network {
                    message: format!(
                        "metadata prefix probe fan-out expected target {expected_target_node_id}, got {}",
                        outcome.target_node_id
                    ),
                });
            }
            if outcome.result.is_ok() {
                pending_prefix_probes.insert(request_id, expected_target_node_id);
            } else {
                self.pending_metadata_replications.remove(&request_id);
            }
        }
        Ok(rejected_node_ids)
    }

    fn send_metadata_sync_checkpoint_snapshots(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        probe_barrier_id: &str,
        rejected_node_ids: &BTreeSet<NodeId>,
        pending_checkpoint_snapshots: &mut BTreeMap<String, MetadataCheckpointSnapshotAttempt>,
    ) -> Result<(), DfsError> {
        let attempts = rejected_node_ids
            .iter()
            .map(|target_node_id| {
                MetadataCheckpointSnapshotAttempt::for_target(target_node_id.clone(), target)
            })
            .filter(|attempt| {
                !pending_checkpoint_snapshots
                    .values()
                    .any(|pending| pending == attempt)
            })
            .collect::<Vec<_>>();
        // Filter before exporting, cloning, or validating the materialized
        // tables. A settled rejection and an unroutable attempt are terminal
        // for this exact node/head within this invocation.
        if attempts.is_empty() {
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
                &target.state_fingerprint,
            )?,
        };
        if checkpoint_snapshot.materialized_through_sequence != Some(target.journal_sequence)
            || !crate::metadata_records::dfs_metadata_table_snapshot_matches_checkpoint(
                &checkpoint_snapshot,
                target.journal_sequence,
                &target.state_fingerprint,
            )
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "encode metadata shard {} repair checkpoint: {source}",
                    target.shard_id.0
                ),
            })?
        {
            return Err(DfsError::Metadata {
                message: format!(
                    "metadata shard {} repair snapshot does not match frozen prefix {}",
                    target.shard_id.0, target.state_fingerprint
                ),
            });
        }
        for attempt in attempts {
            let target_node_id = attempt.target_node_id.clone();
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
                    required_authority_confirmation_id: None,
                    created_at: Instant::now(),
                },
            );
            let sent = self.send_best_effort_message_if_routable(
                &target_node_id,
                DfsNetworkMessage::MetadataSnapshotInstall(command),
            )?;
            // Retain the exact attempt even after a response settles and even
            // when no route exists. Only an exact target refresh clears it.
            pending_checkpoint_snapshots.insert(request_id.clone(), attempt);
            if !sent {
                self.pending_metadata_replications.remove(&request_id);
            }
        }
        Ok(())
    }

    fn finalize_metadata_full_shard_sync(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        progress: DfsMetadataQuorumSyncProgress,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        self.revalidate_metadata_quorum_sync_authority(target)?;
        if !metadata_sync_progress_is_full_shard(target, &progress) {
            return Err(DfsError::Distributed {
                message: format!(
                    "metadata shard {} full-shard certificate omitted a configured member",
                    target.shard_id.0
                ),
            });
        }
        Ok(metadata_sync_certificate(target, progress))
    }

    /// Partition fallback for an exact prefix that already satisfies its
    /// purpose-specific durability certificate. Ordinary sync publishes one
    /// bounded per-shard head watermark. Membership handoff instead requires
    /// joint majorities plus both primaries and stores that evidence only in
    /// the activation proof.
    fn try_finalize_metadata_quorum_sync_fallback(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        progress: DfsMetadataQuorumSyncProgress,
        purpose: &MetadataQuorumSyncPurpose,
    ) -> Result<Option<DfsMetadataQuorumSyncCertificate>, DfsError> {
        self.revalidate_metadata_quorum_sync_authority(target)?;
        if let MetadataQuorumSyncPurpose::MembershipHandoff {
            retiring_shard,
            desired_shard,
            ..
        } = purpose
        {
            let acknowledged_node_ids = progress
                .acknowledged_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            return Ok(
                crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
                    retiring_shard,
                    desired_shard,
                    &acknowledged_node_ids,
                )
                .then(|| metadata_sync_certificate(target, progress)),
            );
        }
        let Some(expected_shard) = target.expected_shard.as_ref() else {
            return Ok(None);
        };
        // An ordinary quorum watermark is valid only for the exact active
        // assignment it names. A wider or narrower target could publish an
        // unconfigured node, so only an exact active-assignment target may take
        // this control-plane fallback.
        let expected_node_ids = std::iter::once(expected_shard.primary_node_id.clone())
            .chain(expected_shard.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        let target_node_ids = std::iter::once(target.primary_node_id.clone())
            .chain(target.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        if target_node_ids != expected_node_ids {
            return Ok(None);
        }
        if target.journal_sequence == 0 || target.state_fingerprint.is_empty() {
            return Err(DfsError::Metadata {
                message: format!(
                    "metadata shard {} has an incomplete operation-bound POSIX sync prefix",
                    target.shard_id.0
                ),
            });
        }
        let watermark = DfsMetadataShardHeadWatermark {
            file_system_id: target.file_system_id.clone(),
            shard_id: target.shard_id.0.clone(),
            primary_node_id: target.primary_node_id.clone(),
            epoch: expected_shard.epoch,
            journal_sequence: target.journal_sequence,
            state_fingerprint: target.state_fingerprint.clone(),
            acknowledged_node_ids: progress.acknowledged_node_ids.clone(),
        };
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        let published = DfsControlPlaneStore::new(control_plane.as_mut())
            .publish_metadata_shard_head_watermark(expected_shard, &watermark)?;
        if !published {
            // Distinguish a concurrently changed assignment from a temporarily
            // unavailable cluster quorum before the caller retries.
            self.revalidate_metadata_quorum_sync_authority(target)?;
            return Ok(None);
        }
        self.revalidate_metadata_quorum_sync_authority(target)?;
        Ok(Some(metadata_sync_certificate(target, progress)))
    }

    fn revalidate_metadata_quorum_sync_authority(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
    ) -> Result<(), DfsError> {
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

        let expected_shard = target.expected_shard.as_ref().ok_or_else(|| {
            DfsError::Distributed {
                message: format!(
                    "clustered metadata shard {} POSIX sync target omitted its exact assignment",
                    target.shard_id.0
                ),
            }
        })?;
        let shard = self
            .current_metadata_sync_shard_config(target)?
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "metadata shard {} configuration disappeared during POSIX sync",
                    target.shard_id.0
                ),
            })?;
        if shard != *expected_shard
            || shard.primary_node_id != self.node_id
            || shard.epoch != expected_epoch
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

fn metadata_sync_certificate(
    target: &DfsMetadataQuorumSyncTarget,
    progress: DfsMetadataQuorumSyncProgress,
) -> DfsMetadataQuorumSyncCertificate {
    let mut certified_target = target.clone();
    certified_target.checkpoint_snapshot = None;
    DfsMetadataQuorumSyncCertificate {
        target: certified_target,
        quorum_size: progress.quorum_size,
        acknowledged_node_ids: progress.acknowledged_node_ids,
    }
}

pub(crate) fn metadata_shard_state_fingerprint_at(
    backend: &dyn DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    expected_journal_sequence: u64,
) -> Result<String, DfsError> {
    let proof = backend
        .metadata_prefix_proof(shard_id)?
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "metadata shard {} cannot produce an exact durable prefix proof",
                shard_id.0
            ),
        })?;
    if !proof.is_well_formed() || proof.journal_sequence != expected_journal_sequence {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata shard {} exact prefix proof sequence {} does not match durability head {}",
                shard_id.0, proof.journal_sequence, expected_journal_sequence
            ),
        });
    }
    Ok(proof.digest)
}

/// Prove that the current durable shard prefix is the handoff marker itself
/// or a contiguous journal extension of it.
///
/// A successfully rebound authority may append before another process opens
/// the same shard. Equality is therefore sufficient only for the first bind;
/// later fresh lanes reconstruct the hash chain from the durable marker to the
/// current exact prefix. A missing entry, sequence gap, divergent digest, or
/// concurrently moving head leaves the lane fenced for a later retry.
fn metadata_shard_prefix_matches_or_extends_marker(
    backend: &dyn DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    marker_sequence: u64,
    marker_fingerprint: &str,
) -> Result<bool, DfsError> {
    let Some(current) = backend.metadata_prefix_proof(shard_id)? else {
        return Ok(false);
    };
    let mut reconstructed = DfsMetadataPrefixProof {
        version: DFS_METADATA_PREFIX_PROOF_VERSION,
        journal_sequence: marker_sequence,
        digest: marker_fingerprint.to_string(),
    };
    if !current.is_well_formed()
        || !reconstructed.is_well_formed()
        || current.journal_sequence < marker_sequence
    {
        return Ok(false);
    }
    while reconstructed.journal_sequence < current.journal_sequence {
        let Some(batch) = backend.journal_batch_for_shard_since_limited(
            shard_id,
            reconstructed.journal_sequence,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )?
        else {
            return Ok(false);
        };
        if batch.entries.is_empty() || batch.shard_id != *shard_id {
            return Ok(false);
        }
        let previous_sequence = reconstructed.journal_sequence;
        for entry in batch.entries {
            if entry.shard_id != *shard_id
                || entry.sequence != reconstructed.journal_sequence.saturating_add(1)
                || entry.sequence > current.journal_sequence
            {
                return Ok(false);
            }
            reconstructed =
                dfs_metadata_prefix_proof_after(&reconstructed, &entry).map_err(|source| {
                    DfsError::Metadata {
                        message: format!(
                            "encode metadata shard {} handoff extension entry {}: {source}",
                            shard_id.0, entry.sequence
                        ),
                    }
                })?;
        }
        if reconstructed.journal_sequence <= previous_sequence {
            return Ok(false);
        }
    }
    Ok(reconstructed == current)
}

fn metadata_shard_snapshot_at(
    backend: &dyn DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    expected_journal_sequence: u64,
    expected_state_fingerprint: &str,
) -> Result<crate::DfsMetadataTableSnapshot, DfsError> {
    #[cfg(test)]
    DFS_METADATA_SYNC_SNAPSHOT_BUILD_CALLS.set(
        DFS_METADATA_SYNC_SNAPSHOT_BUILD_CALLS
            .get()
            .saturating_add(1),
    );
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
    let matches = crate::metadata_records::dfs_metadata_table_snapshot_matches_checkpoint(
        &snapshot,
        expected_journal_sequence,
        expected_state_fingerprint,
    )
    .map_err(|source| DfsError::Serialization {
        message: format!(
            "encode metadata shard {} repair checkpoint: {source}",
            shard_id.0
        ),
    })?;
    if !matches {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata shard {} repair snapshot does not match frozen durability checkpoint {}",
                shard_id.0, expected_state_fingerprint
            ),
        });
    }
    Ok(snapshot)
}

#[cfg(test)]
thread_local! {
    static DFS_METADATA_SYNC_SNAPSHOT_BUILD_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_metadata_sync_snapshot_build_calls() {
    DFS_METADATA_SYNC_SNAPSHOT_BUILD_CALLS.set(0);
}

#[cfg(test)]
fn metadata_sync_snapshot_build_calls() -> usize {
    DFS_METADATA_SYNC_SNAPSHOT_BUILD_CALLS.get()
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

fn metadata_sync_progress_is_full_shard(
    target: &DfsMetadataQuorumSyncTarget,
    progress: &DfsMetadataQuorumSyncProgress,
) -> bool {
    let mut required = target.replica_node_ids.clone();
    required.push(target.primary_node_id.clone());
    required.sort();
    required.dedup();
    let mut acknowledged = progress.acknowledged_node_ids.clone();
    acknowledged.sort();
    acknowledged.dedup();
    acknowledged == required
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
            NetworkedDfsServiceEvent::MetadataSnapshotInstalled { .. } => {}
            NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id,
                source_node_id,
                ..
            } => {
                if pending_prefix_probes.remove(&request_id).is_some() {
                    rejected_node_ids.insert(source_node_id);
                }
            }
            _ => {}
        }
    }
    rejected_node_ids
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::kv::{InProcessKvStore, KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};
    use sinomo_core::network::{NetworkError, ObjectRpcRequest};

    use super::*;
    use crate::{
        DfsControlPlaneKvClient, DfsFileSystem, DfsMetadataJournalBatchResponse,
        DfsMetadataJournalBatchResult, DfsMetadataShardAssignment,
        DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult, DfsNetworkMessage,
        DfsRoutedCall, DfsRoutedMessage, DfsTransportIdentity, LocalContentStore,
        LocalShardedMetadataBackend, LocalSobjMetadataBackend, SharedDfsControlPlaneKvClient,
        build_dfs_routed_message, directory_object, sifs_file_system_id,
    };

    #[derive(Debug)]
    struct MetadataAckTransport {
        local_node_id: NodeId,
        acknowledged_nodes: BTreeSet<NodeId>,
        direct_prefix_nodes: BTreeSet<NodeId>,
        direct_catchup_nodes: BTreeSet<NodeId>,
        direct_prefix_probe_count: usize,
        direct_catchup_batch_count: usize,
        direct_catchup_request_count: usize,
        omit_direct_misses: bool,
        routed_prefix_probe_count: usize,
        routed_fanout_max_inflight: Vec<usize>,
        routed_fanout_targets: Vec<Vec<NodeId>>,
        catchup_rpc_requests: Vec<(usize, ObjectRpcRequest)>,
        snapshot_install_count: usize,
        reject_prefix_probes: bool,
        reject_snapshot_installs: bool,
        unroutable_nodes: BTreeSet<NodeId>,
        network_error_nodes: BTreeSet<NodeId>,
        hold_network_errors_until_operation_deadline: bool,
        fail_direct_after_operation_deadline: bool,
        echoes_prefix_verification: bool,
        response_sequence_override: Option<u64>,
        incoming: VecDeque<DfsRoutedMessage>,
        next_frame_id: u64,
        operation_deadline: Option<Instant>,
    }

    impl MetadataAckTransport {
        fn new(
            local_node_id: NodeId,
            acknowledged_nodes: impl IntoIterator<Item = NodeId>,
        ) -> Self {
            Self {
                local_node_id,
                acknowledged_nodes: acknowledged_nodes.into_iter().collect(),
                direct_prefix_nodes: BTreeSet::new(),
                direct_catchup_nodes: BTreeSet::new(),
                direct_prefix_probe_count: 0,
                direct_catchup_batch_count: 0,
                direct_catchup_request_count: 0,
                omit_direct_misses: false,
                routed_prefix_probe_count: 0,
                routed_fanout_max_inflight: Vec::new(),
                routed_fanout_targets: Vec::new(),
                catchup_rpc_requests: Vec::new(),
                snapshot_install_count: 0,
                reject_prefix_probes: false,
                reject_snapshot_installs: false,
                unroutable_nodes: BTreeSet::new(),
                network_error_nodes: BTreeSet::new(),
                hold_network_errors_until_operation_deadline: false,
                fail_direct_after_operation_deadline: false,
                echoes_prefix_verification: true,
                response_sequence_override: None,
                incoming: VecDeque::new(),
                next_frame_id: 1,
                operation_deadline: None,
            }
        }
    }

    impl DfsTransportIdentity for MetadataAckTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for MetadataAckTransport {
        fn send_routed_call(
            &mut self,
            target_node_id: &NodeId,
            call: DfsRoutedCall,
        ) -> Result<(), NetworkError> {
            let message = crate::materialize_dfs_routed_call_sync(call)?;
            if self.unroutable_nodes.contains(target_node_id) {
                return Err(NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                });
            }
            if self.network_error_nodes.contains(target_node_id) {
                if self.hold_network_errors_until_operation_deadline
                    && let Some(deadline) = self.operation_deadline
                {
                    std::thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .saturating_add(Duration::from_millis(1)),
                    );
                }
                return Err(NetworkError::Io {
                    operation: format!("send metadata sync probe to {target_node_id}"),
                    detail: "test peer is offline after route selection".to_string(),
                });
            }
            match &message.message {
                DfsNetworkMessage::MetadataJournalBatch(command) if command.verify_prefix_only => {
                    self.routed_prefix_probe_count =
                        self.routed_prefix_probe_count.saturating_add(1);
                }
                DfsNetworkMessage::MetadataJournalBatch(command)
                    if !command.verify_prefix_only && !command.batch.entries.is_empty() =>
                {
                    let batch_bytes =
                        crate::metadata_journal::metadata_journal_batch_encoded_len(&command.batch)
                            .expect("encode captured metadata catch-up batch");
                    let request =
                        crate::routes::dfs_message_rpc_request(&crate::DfsMessageCommand {
                            request_id: format!("dfs_message_{}", message.frame_id),
                            routed_message: message.clone(),
                        })
                        .expect("encode captured routed metadata catch-up RPC");
                    self.catchup_rpc_requests.push((batch_bytes, request));
                }
                DfsNetworkMessage::MetadataSnapshotInstall(_) => {
                    self.snapshot_install_count = self.snapshot_install_count.saturating_add(1);
                }
                _ => {}
            }
            if !self.acknowledged_nodes.contains(target_node_id) {
                return Ok(());
            }
            let frame_id = self.next_frame_id;
            self.next_frame_id += 1;
            let response = match message.message {
                DfsNetworkMessage::MetadataJournalBatch(command) => {
                    let rejected = command.verify_prefix_only && self.reject_prefix_probes;
                    let verified_authority_probe_id = command
                        .prefix_probe
                        .as_ref()
                        .and_then(|probe| probe.primary_handoff_id.clone())
                        .or_else(|| {
                            command.authority_probe.as_ref().and_then(
                                crate::service_replica::metadata_authority_probe_confirmation_id,
                            )
                        });
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id,
                            shard_id: command.batch.shard_id,
                            next_after_sequence: self
                                .response_sequence_override
                                .unwrap_or(command.batch.next_after_sequence),
                            applied_entries: if rejected {
                                0
                            } else {
                                command.batch.entries.len()
                            },
                            verified_prefix_only: !rejected
                                && command.verify_prefix_only
                                && self.echoes_prefix_verification,
                            verified_authority_probe_id,
                            result: if rejected {
                                DfsMetadataJournalBatchResult::Rejected {
                                    reason: "test prefix rejection".to_string(),
                                }
                            } else {
                                DfsMetadataJournalBatchResult::Applied
                            },
                        },
                    )
                }
                DfsNetworkMessage::MetadataSnapshotInstall(command) => {
                    DfsNetworkMessage::MetadataSnapshotInstallResponse(
                        DfsMetadataSnapshotInstallResponse {
                            request_id: command.request_id,
                            shard_id: command.shard_id,
                            next_after_sequence: command.next_after_sequence,
                            result: if self.reject_snapshot_installs {
                                DfsMetadataSnapshotInstallResult::Rejected {
                                    reason: "test snapshot rejection".to_string(),
                                }
                            } else {
                                DfsMetadataSnapshotInstallResult::Installed {
                                    archived_existing: false,
                                }
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

        fn send_routed_calls_bounded(
            &mut self,
            requests: Vec<crate::DfsRoutedSend>,
            max_inflight: usize,
        ) -> Result<Vec<crate::DfsRoutedSendOutcome>, NetworkError> {
            self.routed_fanout_max_inflight.push(max_inflight);
            self.routed_fanout_targets.push(
                requests
                    .iter()
                    .map(|request| request.target_node_id.clone())
                    .collect(),
            );
            let mut pending = requests.into_iter().enumerate().collect::<Vec<_>>();
            if self.hold_network_errors_until_operation_deadline && max_inflight > 1 {
                pending.sort_by_key(|(_, request)| {
                    self.network_error_nodes.contains(&request.target_node_id)
                });
            }
            let mut outcomes = std::iter::repeat_with(|| None)
                .take(pending.len())
                .collect::<Vec<Option<crate::DfsRoutedSendOutcome>>>();
            for (index, request) in pending {
                let target_node_id = request.target_node_id;
                let result = self.send_routed_call(&target_node_id, request.call);
                outcomes[index] = Some(crate::DfsRoutedSendOutcome {
                    target_node_id,
                    result,
                });
            }
            outcomes
                .into_iter()
                .map(|outcome| {
                    outcome.ok_or_else(|| NetworkError::Serialization {
                        detail: "test routed fan-out omitted an outcome".to_string(),
                    })
                })
                .collect()
        }

        fn send_metadata_journal_batches_direct(
            &mut self,
            requests: &[(NodeId, DfsMetadataJournalBatchCommand)],
        ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
            if self.fail_direct_after_operation_deadline
                && self
                    .operation_deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(NetworkError::Io {
                    operation: "send direct test metadata journal batch".to_string(),
                    detail: "DFS operation deadline expired before routed send".to_string(),
                });
            }
            let prefix_only = requests
                .iter()
                .all(|(_, command)| command.verify_prefix_only);
            let catchup_only = requests
                .iter()
                .all(|(_, command)| !command.verify_prefix_only);
            if !prefix_only && !catchup_only {
                return Ok(None);
            }
            if prefix_only {
                if self.direct_prefix_nodes.is_empty() {
                    return Ok(None);
                }
                self.direct_prefix_probe_count = self
                    .direct_prefix_probe_count
                    .saturating_add(requests.len());
            } else {
                if self.direct_catchup_nodes.is_empty() {
                    return Ok(None);
                }
                self.direct_catchup_batch_count = self.direct_catchup_batch_count.saturating_add(1);
                self.direct_catchup_request_count = self
                    .direct_catchup_request_count
                    .saturating_add(requests.len());
            }
            Ok(Some(
                requests
                    .iter()
                    .rev()
                    .filter(|(target_node_id, _)| {
                        !self.omit_direct_misses
                            || if prefix_only {
                                self.direct_prefix_nodes.contains(target_node_id)
                            } else {
                                self.direct_catchup_nodes.contains(target_node_id)
                            }
                    })
                    .map(|(target_node_id, command)| {
                        let accepted = if prefix_only {
                            self.direct_prefix_nodes.contains(target_node_id)
                                && command
                                    .prefix_probe
                                    .as_ref()
                                    .is_some_and(|probe| probe.primary_handoff_id.is_none())
                        } else {
                            self.direct_catchup_nodes.contains(target_node_id)
                        };
                        (
                            target_node_id.clone(),
                            DfsMetadataJournalBatchResponse {
                                request_id: command.request_id.clone(),
                                shard_id: command.batch.shard_id.clone(),
                                next_after_sequence: command.batch.next_after_sequence,
                                applied_entries: if accepted {
                                    command.batch.entries.len()
                                } else {
                                    0
                                },
                                verified_prefix_only: prefix_only && accepted,
                                verified_authority_probe_id: None,
                                result: if accepted {
                                    DfsMetadataJournalBatchResult::Applied
                                } else {
                                    DfsMetadataJournalBatchResult::Rejected {
                                        reason: "direct prefix mismatch".to_string(),
                                    }
                                },
                            },
                        )
                    })
                    .collect(),
            ))
        }

        fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(self.incoming.drain(..).collect())
        }

        fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
            Ok(Some(self.incoming.len()))
        }

        fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
            std::mem::replace(&mut self.operation_deadline, deadline)
        }

        fn operation_deadline(&self) -> Option<Instant> {
            self.operation_deadline
        }
    }

    #[derive(Clone, Debug)]
    struct CountingSyncControlPlane {
        inner: InProcessKvStore,
        txn_calls: Arc<AtomicUsize>,
    }

    impl DfsControlPlaneKvClient for CountingSyncControlPlane {
        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            DfsControlPlaneKvClient::get(&self.inner, key)
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            DfsControlPlaneKvClient::range_prefix(&self.inner, prefix)
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            DfsControlPlaneKvClient::put(&mut self.inner, key, value)
        }

        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            self.txn_calls.fetch_add(1, Ordering::SeqCst);
            DfsControlPlaneKvClient::txn(&mut self.inner, txn)
        }
    }

    struct ConfiguredSyncFixture {
        root: sinomo_testdir::TestDir,
        control_plane: SharedDfsControlPlaneKvClient<CountingSyncControlPlane>,
        service: NetworkedDfsService<MetadataAckTransport>,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        txn_calls: Arc<AtomicUsize>,
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
        let txn_calls = Arc::new(AtomicUsize::new(0));
        let control_plane = SharedDfsControlPlaneKvClient::new(CountingSyncControlPlane {
            inner: InProcessKvStore::new(),
            txn_calls: Arc::clone(&txn_calls),
        });
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
        let object = directory_object("/", owner.clone());
        let mut backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        backend
            .put_file_system(&DfsFileSystem::sifs(owner.clone()))
            .expect("filesystem");
        backend.put_object(&object).expect("root metadata");
        let service = NetworkedDfsService::new(
            MetadataAckTransport::new(owner.clone(), acknowledged_nodes),
            LocalContentStore::new(root.path().join("content")).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(backend)
        .expect("metadata-backed service");
        ConfiguredSyncFixture {
            root,
            control_plane,
            service,
            object,
            shard,
            txn_calls,
        }
    }

    fn complete_local_membership_handoff(
        control_plane: &SharedDfsControlPlaneKvClient<InProcessKvStore>,
        backend: &mut LocalSobjMetadataBackend,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
    ) -> DfsMetadataShardMembershipHandoff {
        let handoff = control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(retiring, desired)
                .expect("begin local membership handoff")
                .expect("retiring assignment remains exact")
        });
        let shard_id = DfsMetadataShardId::new(retiring.shard_id.clone());
        let freeze = backend
            .freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                &handoff.handoff_id,
                retiring.epoch,
            )
            .expect("freeze local retiring generation")
            .expect("local retiring shard");
        let fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: retiring.primary_node_id.clone(),
            journal_sequence: freeze.journal_sequence,
            state_fingerprint: freeze.state_fingerprint.clone(),
        };
        let acknowledged = control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &fence)
                .expect("acknowledge local retiring fence")
                .expect("pending handoff remains exact")
        });
        let acknowledged_node_ids = std::iter::once(retiring.primary_node_id.clone())
            .chain(retiring.replica_node_ids.iter().cloned())
            .chain(desired.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let proof = DfsMetadataShardMembershipHandoffProof {
            handoff_id: acknowledged.handoff_id.clone(),
            retiring_shard: retiring.clone(),
            desired_shard: desired.clone(),
            journal_sequence: freeze.journal_sequence,
            state_fingerprint: freeze.state_fingerprint,
            acknowledged_node_ids,
        };
        assert!(control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .complete_metadata_shard_membership_handoff(retiring, desired, &proof)
                .expect("complete exact local membership handoff")
        }));
        handoff
    }

    #[test]
    fn metadata_quorum_size_counts_primary_and_configured_replicas() {
        assert_eq!(dfs_metadata_replica_quorum_size(0), 1);
        assert_eq!(dfs_metadata_replica_quorum_size(1), 2);
        assert_eq!(dfs_metadata_replica_quorum_size(2), 2);
        assert_eq!(dfs_metadata_replica_quorum_size(3), 3);
    }

    #[test]
    fn posix_catchup_uses_direct_batch_and_advances_all_replica_cursors() {
        let replicas = (0..3)
            .map(|index| node(&format!("node_sync_direct_catchup_replica_{index}")))
            .collect::<Vec<_>>();
        let mut fixture =
            configured_sync_fixture("direct_catchup", replicas.clone(), replicas.clone());
        fixture.service.transport.direct_catchup_nodes = replicas.iter().cloned().collect();
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture direct catch-up target");
        fixture
            .service
            .send_metadata_sync_catchup_batches(&target, "direct-catchup-probe")
            .expect("send concurrent direct catch-up batch");

        assert_eq!(fixture.service.transport.direct_catchup_batch_count, 1);
        assert_eq!(
            fixture.service.transport.direct_catchup_request_count,
            replicas.len()
        );
        assert!(
            fixture.service.transport.catchup_rpc_requests.is_empty(),
            "the canonical direct batch must avoid the sequential routed fallback"
        );
        assert!(fixture.service.pending_metadata_replications.is_empty());
        let backend = fixture
            .service
            .metadata_backend
            .as_deref()
            .expect("metadata backend");
        for replica in replicas {
            assert_eq!(
                backend
                    .metadata_replication_cursor(&replica, &target.shard_id)
                    .expect("replica cursor"),
                target.journal_sequence,
                "reversed direct responses must settle every replica independently"
            );
        }
    }

    #[test]
    fn partial_direct_catchup_routes_only_unresolved_replicas() {
        let replicas = (0..3)
            .map(|index| node(&format!("node_sync_partial_direct_catchup_replica_{index}")))
            .collect::<Vec<_>>();
        let mut fixture =
            configured_sync_fixture("partial_direct_catchup", replicas.clone(), replicas.clone());
        fixture
            .service
            .transport
            .direct_catchup_nodes
            .insert(replicas[0].clone());
        fixture.service.transport.omit_direct_misses = true;
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture partial direct catch-up target");
        fixture
            .service
            .send_metadata_sync_catchup_batches(&target, "partial-direct-catchup-probe")
            .expect("fall back omitted direct catch-ups to routed sends");

        assert_eq!(fixture.service.transport.direct_catchup_batch_count, 1);
        assert_eq!(
            fixture.service.transport.direct_catchup_request_count,
            replicas.len()
        );
        assert_eq!(
            fixture.service.transport.catchup_rpc_requests.len(),
            replicas.len() - 1,
            "only direct omissions should use the routed fallback"
        );
        assert_eq!(
            fixture.service.pending_metadata_replications.len(),
            replicas.len() - 1,
            "routed omissions must remain request-scoped until their replies arrive"
        );

        fixture
            .service
            .process_incoming()
            .expect("settle routed catch-up responses");
        let backend = fixture
            .service
            .metadata_backend
            .as_deref()
            .expect("metadata backend");
        for replica in replicas {
            assert_eq!(
                backend
                    .metadata_replication_cursor(&replica, &target.shard_id)
                    .expect("replica cursor"),
                target.journal_sequence,
                "direct and routed siblings must converge on the same frozen prefix"
            );
        }
        assert!(fixture.service.pending_metadata_replications.is_empty());
    }

    #[test]
    fn partial_direct_prefix_probe_routes_only_unresolved_replicas() {
        let replicas = (0..3)
            .map(|index| node(&format!("node_sync_partial_direct_prefix_replica_{index}")))
            .collect::<Vec<_>>();
        let mut fixture =
            configured_sync_fixture("partial_direct_prefix", replicas.clone(), replicas.clone());
        fixture.service.transport.direct_catchup_nodes = replicas.iter().cloned().collect();
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture partial direct prefix target");
        fixture
            .service
            .send_metadata_sync_catchup_batches(&target, "partial-direct-prefix-catchup")
            .expect("prime every replica cursor");

        fixture.service.transport.direct_prefix_nodes = BTreeSet::from([replicas[0].clone()]);
        fixture.service.transport.omit_direct_misses = true;
        let mut freshly_acknowledged = BTreeSet::from([fixture.shard.primary_node_id.clone()]);
        let mut pending_prefix_probes = BTreeMap::new();
        let rejected = fixture
            .service
            .send_metadata_sync_prefix_probes(
                &target,
                "partial-direct-prefix-probe",
                &mut freshly_acknowledged,
                &mut pending_prefix_probes,
                &MetadataQuorumSyncPurpose::OrdinaryDurability,
            )
            .expect("fall back omitted direct prefix probes to routed sends");

        assert!(rejected.is_empty());
        assert!(freshly_acknowledged.contains(&replicas[0]));
        assert_eq!(
            fixture.service.transport.routed_prefix_probe_count,
            replicas.len() - 1,
            "only direct omissions should use routed prefix probes"
        );
        assert_eq!(pending_prefix_probes.len(), replicas.len() - 1);
        assert_eq!(
            fixture.service.pending_metadata_replications.len(),
            replicas.len() - 1
        );
    }

    #[test]
    fn posix_catchup_routes_large_metadata_batch_without_transport_switch() {
        let replica = node("node_sync_large_catchup_replica");
        let mut fixture = configured_sync_fixture(
            "large_catchup",
            vec![replica.clone()],
            vec![replica.clone()],
        );
        let owner = fixture.shard.primary_node_id.clone();
        for index in 0..32 {
            let path = format!("/large-catchup-{index}-{}", "x".repeat(3_000));
            fixture
                .service
                .metadata_backend
                .as_deref_mut()
                .expect("metadata backend")
                .put_object(&directory_object(&path, owner.clone()))
                .expect("append large catch-up entry");
        }
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture target");

        fixture
            .service
            .send_metadata_sync_catchup_batches(&target, "large-catchup-probe")
            .expect("send large POSIX catch-up");
        let [(batch_bytes, request)] = fixture.service.transport.catchup_rpc_requests.as_slice()
        else {
            panic!("expected one captured POSIX catch-up RPC");
        };
        assert!(*batch_bytes > 0);
        assert!(!request.body.is_empty());
        assert_eq!(fixture.service.transport.snapshot_install_count, 0);
    }

    #[test]
    fn posix_single_large_catchup_entry_uses_same_routed_message() {
        let replica = node("node_sync_single_large_catchup_replica");
        let mut fixture = configured_sync_fixture(
            "single_large_catchup",
            vec![replica.clone()],
            vec![replica.clone()],
        );
        let initial_target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture initial target");
        fixture
            .service
            .metadata_backend
            .as_deref_mut()
            .expect("metadata backend")
            .put_metadata_replication_cursor(
                &replica,
                &initial_target.shard_id,
                initial_target.journal_sequence,
            )
            .expect("advance replica to the oversized entry");

        let large = directory_object(
            &format!(
                "/single-large-entry-{}",
                "x".repeat(DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT + 1024)
            ),
            fixture.shard.primary_node_id.clone(),
        );
        fixture
            .service
            .metadata_backend
            .as_deref_mut()
            .expect("metadata backend")
            .put_object(&large)
            .expect("append large metadata entry");
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&large)
            .expect("capture large target");

        fixture
            .service
            .send_metadata_sync_catchup_batches(&target, "single-large-catchup-probe")
            .expect("route the large entry");

        let [(batch_bytes, request)] = fixture.service.transport.catchup_rpc_requests.as_slice()
        else {
            panic!("expected one routed large-entry catch-up RPC");
        };
        assert!(*batch_bytes > DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT);
        assert!(!request.body.is_empty());
        assert_eq!(fixture.service.transport.snapshot_install_count, 0);
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
    fn configured_metadata_sync_partial_replica_set_uses_operation_bound_fallback() {
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
            .sync_metadata_shard_to_quorum(&target, Duration::ZERO)
            .expect("operation-bound metadata quorum fallback");

        assert_eq!(certificate.quorum_size, 2);
        assert!(
            certificate
                .acknowledged_node_ids
                .contains(&fixture.shard.primary_node_id)
        );
        assert!(certificate.acknowledged_node_ids.contains(&replica_a));
        assert_eq!(fixture.txn_calls.load(Ordering::SeqCst), 1);
        assert!(fixture.root.path().join("metadata").exists());
    }

    #[test]
    fn configured_metadata_sync_full_shard_uses_no_control_plane_transaction() {
        let replica_a = node("node_sync_full_shard_replica_a");
        let replica_b = node("node_sync_full_shard_replica_b");
        let mut fixture = configured_sync_fixture(
            "full_shard",
            vec![replica_a.clone(), replica_b.clone()],
            vec![replica_a, replica_b],
        );
        crate::metadata_records::reset_dfs_metadata_full_snapshot_fingerprint_calls();
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture target");
        assert!(target.checkpoint_snapshot.is_none());
        assert_eq!(
            crate::metadata_records::dfs_metadata_full_snapshot_fingerprint_calls(),
            0,
            "healthy target capture must use the durable O(1) prefix proof"
        );

        let certificate = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::ZERO)
            .expect("fresh full-shard certificate");

        assert_eq!(certificate.acknowledged_node_ids.len(), 3);
        assert_eq!(fixture.txn_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            crate::metadata_records::dfs_metadata_full_snapshot_fingerprint_calls(),
            0,
            "healthy full-shard prefix verification must not export/hash a snapshot"
        );
    }

    #[test]
    fn pending_membership_handoff_carries_exact_union_state_before_activation() {
        let old_replica = node("node_sync_membership_old_replica");
        let new_replica = node("node_sync_membership_new_replica");
        let mut fixture = configured_sync_fixture(
            "membership_handoff",
            vec![old_replica.clone()],
            vec![old_replica.clone(), new_replica.clone()],
        );
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica.clone()],
            epoch: fixture.shard.epoch + 1,
            ..fixture.shard.clone()
        };
        fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&fixture.shard, &desired)
                .expect("begin membership handoff")
                .expect("retiring assignment is exact")
        });

        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    &[fixture.shard.clone()],
                    Duration::from_secs(1),
                )
                .expect("repair and activate pending membership"),
            1
        );

        let (active, pending, proof, watermark) = fixture.control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&desired.file_system_id, &desired.shard_id)
                    .expect("active shard"),
                store
                    .get_metadata_shard_membership_handoff(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("pending handoff"),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("handoff proof")
                    .expect("durable handoff proof"),
                store
                    .get_metadata_shard_head_watermark(&desired.file_system_id, &desired.shard_id)
                    .expect("activated watermark")
                    .expect("activated watermark value"),
            )
        });
        assert_eq!(active, Some(desired.clone()));
        assert!(pending.is_none());
        let expected_union = std::iter::once(fixture.shard.primary_node_id.clone())
            .chain([old_replica, new_replica.clone()])
            .collect::<BTreeSet<_>>();
        assert_eq!(
            proof
                .acknowledged_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            expected_union
        );
        assert!(proof.journal_sequence > 0);
        assert!(!proof.state_fingerprint.is_empty());
        assert_eq!(watermark.epoch, desired.epoch);
        assert_eq!(watermark.journal_sequence, proof.journal_sequence);
        assert_eq!(watermark.state_fingerprint, proof.state_fingerprint);
        assert_eq!(
            watermark
                .acknowledged_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            std::iter::once(desired.primary_node_id)
                .chain([new_replica])
                .collect()
        );
    }

    #[test]
    fn ordinary_handoff_prefix_probes_use_the_canonical_direct_backend_lane() {
        let old_replica = node("node_sync_direct_lane_old_replica");
        let new_replica = node("node_sync_direct_lane_new_replica");
        let mut fixture = configured_sync_fixture(
            "direct_lane_membership_handoff",
            vec![old_replica.clone()],
            vec![old_replica.clone(), new_replica.clone()],
        );
        fixture.service.transport.direct_prefix_nodes =
            BTreeSet::from([old_replica.clone(), new_replica.clone()]);
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica],
            epoch: fixture.shard.epoch + 1,
            ..fixture.shard.clone()
        };
        fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&fixture.shard, &desired)
                .expect("begin membership handoff")
                .expect("retiring assignment is exact")
        });

        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    std::slice::from_ref(&fixture.shard),
                    Duration::from_secs(1),
                )
                .expect("canonical direct prefix probes complete the handoff"),
            1
        );
        assert_eq!(fixture.service.transport.direct_prefix_probe_count, 2);
        assert_eq!(
            fixture.service.transport.routed_prefix_probe_count, 0,
            "ordinary probes must not consult the stale routed message-lane backend"
        );
    }

    #[test]
    fn primary_handoff_routes_quorum_probes_concurrently_around_one_offline_member() {
        let mut candidates = vec![
            node("node_sync_handoff_fanout_a"),
            node("node_sync_handoff_fanout_b"),
            node("node_sync_handoff_fanout_c"),
        ];
        candidates.sort();
        let promoted = candidates[0].clone();
        let offline = candidates[1].clone();
        let healthy = candidates[2].clone();
        let mut fixture = configured_sync_fixture(
            "membership_handoff_fanout",
            vec![promoted.clone(), offline.clone(), healthy.clone()],
            vec![promoted.clone(), healthy.clone()],
        );
        let checkpoint = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture exact checkpoint");
        for replica_node_id in [&promoted, &offline, &healthy] {
            fixture
                .service
                .metadata_backend
                .as_deref_mut()
                .expect("metadata backend")
                .put_metadata_replication_cursor(
                    replica_node_id,
                    &checkpoint.shard_id,
                    checkpoint.journal_sequence,
                )
                .expect("seed exact replica cursor");
        }
        fixture
            .service
            .transport
            .network_error_nodes
            .insert(offline.clone());
        fixture
            .service
            .transport
            .hold_network_errors_until_operation_deadline = true;
        fixture
            .service
            .transport
            .fail_direct_after_operation_deadline = true;
        let retiring = fixture.shard.clone();
        let desired = DfsMetadataShardConfig {
            primary_node_id: promoted.clone(),
            replica_node_ids: crate::control_plane::topology_preserving_metadata_shard_replicas(
                &retiring, &promoted,
            )
            .expect("promoted replica preserves the configured node ring"),
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&retiring, &desired)
                .expect("begin primary handoff")
                .expect("retiring assignment is exact")
        });

        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    std::slice::from_ref(&retiring),
                    Duration::from_millis(80),
                )
                .expect("one offline member must not starve a joint quorum"),
            1
        );

        let expected_targets = vec![promoted.clone(), offline.clone(), healthy.clone()];
        assert!(
            fixture
                .service
                .transport
                .routed_fanout_targets
                .iter()
                .any(|targets| targets == &expected_targets),
            "all unresolved probes must enter the same bounded routed fan-out"
        );
        assert!(
            fixture
                .service
                .transport
                .routed_fanout_max_inflight
                .contains(&DFS_METADATA_SYNC_ROUTED_FANOUT_CAP),
            "the routed fallback must keep a second worker available around one offline member"
        );

        let (active, pending, proof) = fixture.control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&desired.file_system_id, &desired.shard_id)
                    .expect("active shard"),
                store
                    .get_metadata_shard_membership_handoff(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("pending handoff"),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("handoff proof")
                    .expect("durable handoff proof"),
            )
        });
        assert_eq!(active, Some(desired));
        assert!(pending.is_none());
        let acknowledged_node_ids = proof
            .acknowledged_node_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            acknowledged_node_ids,
            BTreeSet::from([retiring.primary_node_id, promoted, healthy])
        );
        assert!(!acknowledged_node_ids.contains(&offline));
    }

    #[test]
    fn primary_handoff_probes_ready_quorum_before_offline_replica_catchup() {
        let mut candidates = vec![
            node("node_sync_handoff_ready_quorum_a"),
            node("node_sync_handoff_ready_quorum_b"),
            node("node_sync_handoff_ready_quorum_c"),
        ];
        candidates.sort();
        let promoted = candidates[0].clone();
        let offline = candidates[1].clone();
        let healthy = candidates[2].clone();
        let mut fixture = configured_sync_fixture(
            "membership_handoff_ready_quorum",
            vec![promoted.clone(), offline.clone(), healthy.clone()],
            vec![promoted.clone(), healthy.clone()],
        );
        let checkpoint = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture exact checkpoint");
        for replica_node_id in [&promoted, &healthy] {
            fixture
                .service
                .metadata_backend
                .as_deref_mut()
                .expect("metadata backend")
                .put_metadata_replication_cursor(
                    replica_node_id,
                    &checkpoint.shard_id,
                    checkpoint.journal_sequence,
                )
                .expect("seed exact healthy replica cursor");
        }
        fixture
            .service
            .transport
            .network_error_nodes
            .insert(offline.clone());
        fixture
            .service
            .transport
            .hold_network_errors_until_operation_deadline = true;
        fixture
            .service
            .transport
            .fail_direct_after_operation_deadline = true;
        let retiring = fixture.shard.clone();
        let desired = DfsMetadataShardConfig {
            primary_node_id: promoted.clone(),
            replica_node_ids: crate::control_plane::topology_preserving_metadata_shard_replicas(
                &retiring, &promoted,
            )
            .expect("promoted replica preserves the configured node ring"),
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&retiring, &desired)
                .expect("begin primary handoff")
                .expect("retiring assignment is exact")
        });

        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    std::slice::from_ref(&retiring),
                    Duration::from_millis(80),
                )
                .expect("ready joint quorum must not wait for offline catch-up"),
            1
        );
        assert!(
            fixture.service.transport.catchup_rpc_requests.is_empty(),
            "the offline laggard must not consume the handoff deadline before a ready quorum probe"
        );

        let (active, pending, proof) = fixture.control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&desired.file_system_id, &desired.shard_id)
                    .expect("active shard"),
                store
                    .get_metadata_shard_membership_handoff(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("pending handoff"),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("handoff proof")
                    .expect("durable handoff proof"),
            )
        });
        assert_eq!(active, Some(desired));
        assert!(pending.is_none());
        let acknowledged_node_ids = proof
            .acknowledged_node_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            acknowledged_node_ids,
            BTreeSet::from([retiring.primary_node_id, promoted, healthy])
        );
        assert!(!acknowledged_node_ids.contains(&offline));
    }

    #[test]
    fn pending_membership_handoff_retries_until_both_assignment_quorums_ack() {
        let old_replica = node("node_sync_handoff_retry_old_replica");
        let new_replica = node("node_sync_handoff_retry_new_replica");
        let mut fixture = configured_sync_fixture(
            "membership_handoff_retry",
            vec![old_replica.clone()],
            vec![new_replica.clone()],
        );
        let retiring = fixture.shard.clone();
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica.clone()],
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        let handoff = fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&retiring, &desired)
                .expect("begin membership handoff")
                .expect("retiring assignment is exact")
        });

        let error = fixture
            .service
            .reconcile_pending_metadata_shard_membership_handoffs(
                std::slice::from_ref(&retiring),
                Duration::ZERO,
            )
            .expect_err("a desired-only quorum must remain pending");
        assert!(
            matches!(error, DfsError::Network { ref message } if message.contains("2/3 acknowledgements")),
            "the joint-quorum attempt must time out cleanly instead of publishing an invalid retiring-epoch watermark: {error}"
        );

        let (active, pending, proof, watermark) = fixture.control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&retiring.file_system_id, &retiring.shard_id)
                    .expect("active shard"),
                store
                    .get_metadata_shard_membership_handoff(
                        &retiring.file_system_id,
                        &retiring.shard_id,
                    )
                    .expect("pending handoff"),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &retiring.file_system_id,
                        &retiring.shard_id,
                    )
                    .expect("handoff proof"),
                store
                    .get_metadata_shard_head_watermark(&retiring.file_system_id, &retiring.shard_id)
                    .expect("head watermark"),
            )
        });
        assert_eq!(active, Some(retiring.clone()));
        let pending = pending.expect("handoff remains pending after partial union");
        assert_eq!(pending.handoff_id, handoff.handoff_id);
        assert_eq!(pending.retiring_shard, handoff.retiring_shard);
        assert_eq!(pending.desired_shard, handoff.desired_shard);
        assert!(
            pending.retiring_primary_fence.is_some(),
            "the exact retiring-primary fence remains durable for retry"
        );
        assert!(proof.is_none());
        assert!(watermark.is_none());
        assert_eq!(
            fixture
                .service
                .metadata_backend
                .as_deref()
                .expect("metadata backend")
                .metadata_shard_write_freeze(&DfsMetadataShardId::new(&retiring.shard_id))
                .expect("durable handoff freeze")
                .map(|freeze| freeze.handoff_id),
            Some(handoff.handoff_id.clone())
        );

        fixture
            .service
            .transport
            .acknowledged_nodes
            .insert(old_replica.clone());
        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    std::slice::from_ref(&retiring),
                    Duration::from_secs(1),
                )
                .expect("retry joint membership quorum"),
            1
        );

        let (active, pending, proof) = fixture.control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&desired.file_system_id, &desired.shard_id)
                    .expect("activated shard"),
                store
                    .get_metadata_shard_membership_handoff(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("cleared pending handoff"),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("completed handoff proof")
                    .expect("durable completed handoff proof"),
            )
        });
        assert_eq!(active, Some(desired));
        assert!(pending.is_none());
        assert_eq!(
            proof
                .acknowledged_node_ids
                .into_iter()
                .collect::<BTreeSet<_>>(),
            std::iter::once(retiring.primary_node_id)
                .chain([old_replica, new_replica])
                .collect()
        );
    }

    #[test]
    fn removal_handoff_joint_quorum_tolerates_offline_retiring_replica() {
        let retained_replica = node("node_sync_handoff_removal_retained_replica");
        let removed_replica = node("node_sync_handoff_removal_removed_replica");
        let mut fixture = configured_sync_fixture(
            "membership_handoff_removal_retry",
            vec![retained_replica.clone(), removed_replica.clone()],
            vec![retained_replica.clone()],
        );
        let retiring = fixture.shard.clone();
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![retained_replica.clone()],
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&retiring, &desired)
                .expect("begin removal-only membership handoff")
                .expect("retiring assignment is exact")
        });
        fixture
            .service
            .transport
            .network_error_nodes
            .insert(removed_replica.clone());

        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    std::slice::from_ref(&retiring),
                    Duration::ZERO,
                )
                .expect("joint quorum activates without removed offline replica"),
            1
        );
        assert_eq!(
            fixture.service.transport.operation_deadline, None,
            "the handoff deadline must not leak into later DFS operations"
        );

        let (active, pending, proof) = fixture.control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&desired.file_system_id, &desired.shard_id)
                    .expect("activated shard"),
                store
                    .get_metadata_shard_membership_handoff(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("cleared pending handoff"),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("completed handoff proof")
                    .expect("durable completed handoff proof"),
            )
        });
        assert_eq!(active, Some(desired));
        assert!(pending.is_none());
        let acknowledged_node_ids = proof
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            acknowledged_node_ids,
            std::iter::once(retiring.primary_node_id)
                .chain([retained_replica])
                .collect()
        );
        assert!(
            !acknowledged_node_ids.contains(&removed_replica),
            "an offline retiring-only replica must not be fabricated into the proof"
        );
    }

    #[test]
    fn primary_rotation_joint_quorum_leaves_retiring_lane_fenced() {
        let promoted = node("node_sync_primary_rotation_promoted");
        let retained_replica = node("node_sync_primary_rotation_retained");
        let mut fixture = configured_sync_fixture(
            "primary_rotation",
            vec![promoted.clone(), retained_replica.clone()],
            vec![promoted.clone()],
        );
        let retiring = fixture.shard.clone();
        let desired = DfsMetadataShardConfig {
            primary_node_id: promoted.clone(),
            replica_node_ids: crate::control_plane::topology_preserving_metadata_shard_replicas(
                &retiring, &promoted,
            )
            .expect("promoted node is an existing replica"),
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        let handoff = fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&retiring, &desired)
                .expect("begin proof-bound primary handoff")
                .expect("retiring assignment is exact")
        });

        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    std::slice::from_ref(&retiring),
                    Duration::from_secs(1),
                )
                .expect("complete joint-quorum primary handoff"),
            1
        );

        let (active, pending, proof) = fixture.control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&desired.file_system_id, &desired.shard_id)
                    .expect("active primary"),
                store
                    .get_metadata_shard_membership_handoff(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("pending primary handoff"),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("primary handoff proof")
                    .expect("durable primary handoff proof"),
            )
        });
        assert_eq!(active, Some(desired.clone()));
        assert!(pending.is_none());
        assert_eq!(proof.handoff_id, handoff.handoff_id);
        assert_eq!(
            proof
                .acknowledged_node_ids
                .into_iter()
                .collect::<BTreeSet<_>>(),
            std::iter::once(retiring.primary_node_id.clone())
                .chain([promoted])
                .collect()
        );

        let shard_id = DfsMetadataShardId::new(&retiring.shard_id);
        let freeze = fixture
            .service
            .metadata_backend
            .as_deref()
            .expect("retiring backend")
            .metadata_shard_write_freeze(&shard_id)
            .expect("retiring freeze")
            .expect("durable retiring marker");
        assert_eq!(freeze.handoff_id, handoff.handoff_id);
        assert_eq!(freeze.activated_epoch, Some(desired.epoch));
        let error = fixture
            .service
            .metadata_backend
            .as_deref_mut()
            .expect("retiring backend")
            .put_object(&directory_object(
                "/retiring-lane-must-stay-fenced",
                retiring.primary_node_id,
            ))
            .expect_err("retiring primary must not bind the promoted epoch");
        assert!(error.to_string().contains("frozen"));
    }

    #[test]
    fn retiring_primary_freeze_rejects_new_prepare_without_wedging_activation() {
        let old_replica = node("node_sync_frozen_handoff_old_replica");
        let new_replica = node("node_sync_frozen_handoff_new_replica");
        let mut fixture = configured_sync_fixture(
            "frozen_membership_handoff",
            vec![old_replica.clone()],
            vec![old_replica, new_replica.clone()],
        );
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica],
            epoch: fixture.shard.epoch + 1,
            ..fixture.shard.clone()
        };
        let handoff = fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&fixture.shard, &desired)
                .expect("begin membership handoff")
                .expect("retiring assignment is exact")
        });
        let shard_id = DfsMetadataShardId::new(fixture.shard.shard_id.clone());
        let freeze = fixture
            .service
            .metadata_backend
            .as_deref_mut()
            .expect("metadata backend")
            .freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                &handoff.handoff_id,
                fixture.shard.epoch,
            )
            .expect("install retiring-primary freeze")
            .expect("local retiring shard");
        let retiring_fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: fixture.shard.primary_node_id.clone(),
            journal_sequence: freeze.journal_sequence,
            state_fingerprint: freeze.state_fingerprint.clone(),
        };
        fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &retiring_fence)
                .expect("acknowledge retiring-primary fence")
                .expect("pending handoff remains exact")
        });

        let mut moved = fixture.object.clone();
        moved.path = "/must-not-append-after-fence".to_string();
        let mut prepare = crate::DfsNamespacePrepareRecord {
            operation_id: "namespace:must-not-append-after-fence".to_string(),
            file_system_id: moved.file_system_id.clone(),
            shard_id: shard_id.clone(),
            owner_epoch: fixture.shard.epoch,
            operation_digest: String::new(),
            expected_object_versions: vec![crate::DfsNamespaceObjectVersionExpectation {
                object_id: moved.object_id.clone(),
                expected_version: Some(fixture.object.version.clone()),
            }],
            expected_path_occupants: vec![crate::DfsNamespacePathOccupantExpectation {
                path: fixture.object.path.clone(),
                expected_object_id: Some(fixture.object.object_id.clone()),
            }],
            content_receipts: Vec::new(),
            transaction: crate::DfsNamespaceTransaction {
                transaction_id: "transaction:must-not-append-after-fence".to_string(),
                file_system_id: moved.file_system_id.clone(),
                objects: vec![moved],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: time::OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            status: crate::DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("prepare digest");
        let rejected = fixture
            .service
            .metadata_backend
            .as_deref_mut()
            .expect("metadata backend")
            .prepare_namespace_transaction(&prepare);
        assert!(matches!(
            rejected,
            Err(crate::MetadataBackendError::MembershipHandoffFrozen { .. })
        ));
        assert_eq!(
            fixture
                .service
                .metadata_backend
                .as_deref()
                .expect("metadata backend")
                .journal_last_sequence()
                .expect("journal head"),
            freeze.journal_sequence
        );

        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    &[fixture.shard.clone()],
                    Duration::from_secs(1),
                )
                .expect("activate the unchanged exact frozen head"),
            1
        );
        assert_eq!(
            fixture
                .service
                .metadata_backend
                .as_deref()
                .expect("metadata backend")
                .metadata_shard_write_freeze(&shard_id)
                .expect("read activated authority generation")
                .and_then(|fence| fence.activated_epoch),
            Some(desired.epoch)
        );
    }

    #[test]
    fn later_pending_handoff_reconciles_after_prior_activation_marker_survives() {
        let old_replica = node("node_sync_handoff_chain_old_replica");
        let middle_replica = node("node_sync_handoff_chain_middle_replica");
        let new_replica = node("node_sync_handoff_chain_new_replica");
        let mut fixture = configured_sync_fixture(
            "membership_handoff_chain",
            vec![old_replica.clone()],
            vec![old_replica, middle_replica.clone(), new_replica.clone()],
        );
        let middle = DfsMetadataShardConfig {
            replica_node_ids: vec![middle_replica],
            epoch: fixture.shard.epoch + 1,
            ..fixture.shard.clone()
        };
        fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&fixture.shard, &middle)
                .expect("begin first membership handoff")
                .expect("first retiring assignment is exact")
        });
        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    &[fixture.shard.clone()],
                    Duration::from_secs(1),
                )
                .expect("activate first membership handoff"),
            1
        );

        let shard_id = DfsMetadataShardId::new(middle.shard_id.clone());
        let first_marker = fixture
            .service
            .metadata_backend
            .as_deref()
            .expect("metadata backend")
            .metadata_shard_write_freeze(&shard_id)
            .expect("read first activated marker")
            .expect("first activated marker remains durable");
        assert_eq!(first_marker.activated_epoch, Some(middle.epoch));

        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica],
            epoch: middle.epoch + 1,
            ..middle.clone()
        };
        let second_handoff = fixture.control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&middle, &desired)
                .expect("begin second membership handoff")
                .expect("second retiring assignment is exact")
        });

        assert_eq!(
            fixture
                .service
                .reconcile_pending_metadata_shard_membership_handoffs(
                    &[middle.clone()],
                    Duration::from_secs(1),
                )
                .expect("advance surviving marker and activate second handoff"),
            1
        );

        let (active, pending) = fixture.control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&desired.file_system_id, &desired.shard_id)
                    .expect("read active assignment"),
                store
                    .get_metadata_shard_membership_handoff(
                        &desired.file_system_id,
                        &desired.shard_id,
                    )
                    .expect("read pending assignment"),
            )
        });
        assert_eq!(active, Some(desired.clone()));
        assert!(pending.is_none());
        let final_marker = fixture
            .service
            .metadata_backend
            .as_deref()
            .expect("metadata backend")
            .metadata_shard_write_freeze(&shard_id)
            .expect("read second activated marker")
            .expect("second activated marker remains durable");
        assert_eq!(final_marker.handoff_id, second_handoff.handoff_id);
        assert_eq!(final_marker.retiring_epoch, middle.epoch);
        assert_eq!(final_marker.activated_epoch, Some(desired.epoch));
    }

    #[test]
    fn handoff_marker_prefix_requires_equal_or_contiguous_extension() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-handoff-prefix-extension");
        let shard_id = DfsMetadataShardId::new("root");
        let local = node("node_handoff_prefix_extension");
        let mut backend =
            LocalSobjMetadataBackend::new(shard_id.clone(), root.path()).expect("metadata backend");
        backend
            .put_file_system(&DfsFileSystem::sifs(local.clone()))
            .expect("filesystem");
        let marker = backend
            .metadata_prefix_proof()
            .expect("marker prefix proof");
        backend
            .put_object(&directory_object("/after-marker", local))
            .expect("post-marker append");

        assert!(
            metadata_shard_prefix_matches_or_extends_marker(
                &backend,
                &shard_id,
                marker.journal_sequence,
                &marker.digest,
            )
            .expect("verify contiguous extension")
        );
        let divergent = format!("dfs-metadata-prefix-v1:blake3:{}", "0".repeat(64));
        assert!(
            !metadata_shard_prefix_matches_or_extends_marker(
                &backend,
                &shard_id,
                marker.journal_sequence,
                &divergent,
            )
            .expect("reject divergent extension")
        );
    }

    #[test]
    fn long_lived_mounted_lane_refreshes_before_rebinding_completed_generation() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-mounted-handoff-rebind");
        let local = node("node_mounted_handoff_local");
        let old_replica = node("node_mounted_handoff_old_replica");
        let new_replica = node("node_mounted_handoff_new_replica");
        let file_system_id = sifs_file_system_id();
        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![old_replica],
            epoch: 31,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica],
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&retiring)
                .expect("retiring shard config");
        });

        let metadata_root = root.path().join("metadata");
        let shard_id = DfsMetadataShardId::new("root");
        let mut completing = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("completing backend");
        completing
            .put_file_system(&DfsFileSystem::sifs(local.clone()))
            .expect("filesystem");
        let root_object = directory_object("/", local.clone());
        completing.put_object(&root_object).expect("root object");
        let mut stale = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("independently opened pre-activation backend");

        let mounted_backend = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("mounted-like long-lived backend");
        let mut mounted = NetworkedDfsService::new(
            MetadataAckTransport::new(local.clone(), []),
            LocalContentStore::new(root.path().join("mounted-content")).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(mounted_backend)
        .expect("mounted-like service");
        let mounted_prefix_before = mounted
            .metadata_backend()
            .expect("mounted backend")
            .metadata_prefix_proof(&shard_id)
            .expect("mounted prefix")
            .expect("mounted exact prefix");
        let caught_up_after_mount_open =
            directory_object("/caught-up-after-mount-open", local.clone());
        completing
            .put_object(&caught_up_after_mount_open)
            .expect("canonical handle advances before handoff freeze");
        assert!(
            mounted
                .metadata()
                .object(&caught_up_after_mount_open.object_id)
                .is_none(),
            "the mounted lane must begin behind the later handoff marker"
        );

        let handoff =
            complete_local_membership_handoff(&control_plane, &mut completing, &retiring, &desired);
        let handoff_marker = completing
            .metadata_shard_write_freeze(&shard_id)
            .expect("handoff marker")
            .expect("durable handoff marker");
        assert!(
            mounted_prefix_before.journal_sequence < handoff_marker.journal_sequence,
            "the regression requires a service prefix behind the completed handoff marker"
        );

        completing
            .clear_metadata_shard_membership_handoff_freeze(
                &shard_id,
                &handoff.handoff_id,
                desired.epoch,
            )
            .expect("completing process activates local marker");
        let recovered_after_mount_open = directory_object("/recovered-after-open", local.clone());
        completing
            .put_object(&recovered_after_mount_open)
            .expect("active process appends state after mounted hydration");
        assert!(
            mounted
                .metadata()
                .object(&recovered_after_mount_open.object_id)
                .is_none(),
            "mounted in-memory metadata should still reflect its pre-rebind hydration"
        );
        assert_eq!(
            mounted
                .metadata_backend()
                .expect("mounted backend")
                .metadata_shard_authoritative_writes_frozen(&shard_id)
                .expect("mounted frozen state"),
            true,
            "an activated marker still fences a long-lived instance until exact rebind"
        );

        // A process opened after that legitimate active-generation append sees
        // a current prefix beyond the completion marker. It must reconstruct
        // and prove the contiguous extension before binding.
        let fresh_after_append_backend =
            LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
                .expect("fresh backend after active append");
        let mut fresh_after_append = NetworkedDfsService::new(
            MetadataAckTransport::new(local.clone(), []),
            LocalContentStore::new(root.path().join("fresh-after-append-content"))
                .expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(fresh_after_append_backend)
        .expect("fresh service after active append");
        fresh_after_append
            .bootstrap_distributed_sifs()
            .expect("contiguous post-activation prefix rebinds");
        assert_eq!(
            fresh_after_append
                .metadata()
                .object(&recovered_after_mount_open.object_id),
            Some(&recovered_after_mount_open)
        );

        mounted
            .bootstrap_distributed_sifs()
            .expect("bootstrap rebinds the exact completed generation");
        assert_eq!(
            mounted
                .metadata()
                .object(&recovered_after_mount_open.object_id),
            Some(&recovered_after_mount_open),
            "rebind must rehydrate state refreshed or recovered by the backend"
        );
        assert_eq!(
            mounted
                .metadata()
                .object(&caught_up_after_mount_open.object_id),
            Some(&caught_up_after_mount_open),
            "rebind must first refresh a prefix that was behind the completion marker"
        );
        assert_eq!(
            mounted
                .metadata_backend()
                .expect("mounted backend")
                .metadata_shard_authoritative_writes_frozen(&shard_id)
                .expect("mounted rebound state"),
            false,
            "the convergence gate must observe the rebound current-primary lane as writable"
        );
        let after_rebind = directory_object("/after-mounted-rebind", local.clone());
        mounted
            .metadata_backend
            .as_deref_mut()
            .expect("mounted metadata backend")
            .put_object(&after_rebind)
            .expect("newly rebound mounted handle may append");
        assert!(matches!(
            stale.put_object(&directory_object("/stale-must-stay-fenced", local)),
            Err(crate::MetadataBackendError::MembershipHandoffFrozen {
                ref handoff_id,
                ..
            }) if handoff_id == &handoff.handoff_id
        ));
        assert_eq!(
            mounted
                .metadata_backend()
                .expect("mounted backend")
                .metadata_shard_write_freeze(&shard_id)
                .expect("durable active marker")
                .and_then(|marker| marker.activated_epoch),
            Some(desired.epoch),
            "the durable marker remains to fence independently opened stale handles"
        );
    }

    #[test]
    fn mounted_like_reopen_leaves_pending_membership_marker_frozen() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-mounted-pending-handoff");
        let local = node("node_mounted_pending_local");
        let old_replica = node("node_mounted_pending_old_replica");
        let new_replica = node("node_mounted_pending_new_replica");
        let file_system_id = sifs_file_system_id();
        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![old_replica],
            epoch: 41,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![new_replica],
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&retiring)
                .expect("retiring shard config");
        });
        let metadata_root = root.path().join("metadata");
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("retiring backend");
        backend
            .put_file_system(&DfsFileSystem::sifs(local.clone()))
            .expect("filesystem");
        backend
            .put_object(&directory_object("/", local.clone()))
            .expect("root object");
        let pending = control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .begin_metadata_shard_membership_handoff(&retiring, &desired)
                .expect("begin pending membership handoff")
                .expect("retiring assignment remains exact")
        });
        backend
            .freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                &pending.handoff_id,
                retiring.epoch,
            )
            .expect("freeze pending generation")
            .expect("local shard");

        let reopened = LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root)
            .expect("mounted-like pending backend");
        let mut mounted = NetworkedDfsService::new(
            MetadataAckTransport::new(local.clone(), []),
            LocalContentStore::new(root.path().join("mounted-content")).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(reopened)
        .expect("mounted-like pending service");

        assert_eq!(
            mounted
                .rebind_completed_local_metadata_shard_membership_handoffs(&file_system_id)
                .expect("pending marker is observed read-only"),
            0
        );
        assert!(matches!(
            mounted
                .metadata_backend
                .as_deref_mut()
                .expect("mounted backend")
                .put_object(&directory_object("/must-remain-frozen", local)),
            Err(crate::MetadataBackendError::MembershipHandoffFrozen {
                ref handoff_id,
                ..
            }) if handoff_id == &pending.handoff_id
        ));
        let (active, still_pending, proof) = control_plane.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            (
                store
                    .get_metadata_shard(&file_system_id, &retiring.shard_id)
                    .expect("active shard"),
                store
                    .get_metadata_shard_membership_handoff(&file_system_id, &retiring.shard_id)
                    .expect("pending handoff"),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &file_system_id,
                        &retiring.shard_id,
                    )
                    .expect("completion proof"),
            )
        });
        assert_eq!(active, Some(retiring));
        assert_eq!(still_pending, Some(pending));
        assert!(proof.is_none());
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
    fn rejected_checkpoint_snapshot_is_built_and_sent_once_per_frozen_target() {
        let replica = node("node_sync_rejected_checkpoint_replica");
        let mut fixture =
            configured_sync_fixture("rejected_checkpoint", vec![replica.clone()], vec![replica]);
        fixture.service.transport.reject_prefix_probes = true;
        fixture.service.transport.reject_snapshot_installs = true;
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture rejected checkpoint target");
        assert!(target.checkpoint_snapshot.is_none());
        reset_metadata_sync_snapshot_build_calls();

        let error = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::from_millis(35))
            .expect_err("an exact replica that rejects repair must not certify durability");

        assert!(
            matches!(error, DfsError::Network { message } if message.contains("1/2 acknowledgements")),
            "a rejected repair must remain fail closed"
        );
        assert!(
            fixture.service.transport.routed_prefix_probe_count > 3,
            "the fixture must exercise repeated rejection across sync pump iterations"
        );
        assert_eq!(
            metadata_sync_snapshot_build_calls(),
            1,
            "one frozen target must not repeatedly export and validate the same deep snapshot"
        );
        assert_eq!(
            fixture.service.transport.snapshot_install_count, 1,
            "one frozen target must attempt at most one snapshot install per replica"
        );
    }

    #[test]
    fn unroutable_checkpoint_snapshot_is_not_rebuilt_for_same_frozen_target() {
        let replica = node("node_sync_unroutable_checkpoint_replica");
        let mut fixture =
            configured_sync_fixture("unroutable_checkpoint", vec![replica.clone()], Vec::new());
        fixture
            .service
            .transport
            .unroutable_nodes
            .insert(replica.clone());
        let mut target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .expect("capture unroutable checkpoint target");
        let rejected = BTreeSet::from([replica]);
        let mut attempts = BTreeMap::new();
        reset_metadata_sync_snapshot_build_calls();

        fixture
            .service
            .send_metadata_sync_checkpoint_snapshots(
                &target,
                "unroutable-checkpoint",
                &rejected,
                &mut attempts,
            )
            .expect("first unroutable attempt is best effort");
        fixture
            .service
            .send_metadata_sync_checkpoint_snapshots(
                &target,
                "unroutable-checkpoint",
                &rejected,
                &mut attempts,
            )
            .expect("identical unroutable attempt remains settled");

        assert_eq!(metadata_sync_snapshot_build_calls(), 1);
        assert_eq!(attempts.len(), 1);
        assert_eq!(fixture.service.transport.snapshot_install_count, 0);
        assert!(fixture.service.pending_metadata_replications.is_empty());

        fixture
            .service
            .metadata_backend
            .as_deref_mut()
            .expect("metadata backend")
            .put_object(&directory_object(
                "/unroutable-checkpoint-next-head",
                fixture.shard.primary_node_id.clone(),
            ))
            .expect("advance exact metadata head");
        assert!(
            fixture
                .service
                .refresh_metadata_sync_target_to_current_head(&mut target)
                .expect("refresh exact metadata target")
        );
        let mut pending_prefix_probes = BTreeMap::new();
        fixture
            .service
            .clear_superseded_metadata_sync_requests(&mut pending_prefix_probes, &mut attempts);
        fixture
            .service
            .send_metadata_sync_checkpoint_snapshots(
                &target,
                "unroutable-checkpoint-next-head",
                &rejected,
                &mut attempts,
            )
            .expect("changed exact head permits one new repair attempt");

        assert_eq!(metadata_sync_snapshot_build_calls(), 2);
        assert_eq!(attempts.len(), 1);
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
