//! Failover-recoverable metadata durability barriers.
//!
//! File-content receipt validation lives in `posix::sync`; this module owns the
//! lower metadata primitive shared by file and directory sync. A caller first
//! freezes a shard journal head while it is the current primary. The healthy
//! path obtains a fresh exact-prefix acknowledgement from every configured
//! shard member. Ordinary sync cannot certify a partial participant set;
//! unavailable members leave the barrier pending until its existing deadline.
//! Membership handoff retains its exact joint-participant proof and separate
//! configuration transition.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::metadata_journal::metadata_prefix_proof_after_exact_frame;
use crate::metadata_records::DfsMetadataPrefixProof;
use crate::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
    DfsControlPlaneStore, DfsError, DfsFileSystemId, DfsMessageTransport, DfsMetadataBackend,
    DfsMetadataJournalBatch, DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResult,
    DfsMetadataPrefixProbe, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataShardMembershipHandoff, DfsMetadataShardMembershipHandoffFence,
    DfsMetadataShardMembershipHandoffProof, DfsMetadataSnapshotInstallCommand, DfsNetworkMessage,
    DfsObjectMetadata, DfsRoutedCall, DfsRoutedSend, MetadataBackendError, NetworkedDfsService,
    NetworkedDfsServiceEvent, PendingMetadataReplication,
    service_state::PendingMetadataReplicationResponseKind, sifs_file_system_id,
};

/// Default time budget for a foreground POSIX durability barrier.
pub const DEFAULT_DFS_POSIX_SYNC_TIMEOUT: Duration = Duration::from_secs(45);

const DFS_POSIX_SYNC_IDLE_WAIT: Duration = Duration::from_millis(10);
/// Give every member of a healthy shard one normal round trip to answer before
/// evaluating the joint participant proof for a membership handoff.
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

// Operation-local observations only. Assignment-bounded send state is included
// in a timeout so the caller's maintained artifact does not depend on daemon
// log retention. These observations never participate in acknowledgement checks.
#[derive(Default, Serialize)]
struct MetadataSyncAttemptDiagnostics {
    peers: BTreeMap<NodeId, BTreeMap<&'static str, MetadataSyncSendDiagnostic>>,
}

#[derive(Default, Serialize)]
struct MetadataSyncSendDiagnostic {
    attempts: u64,
    request_id: String,
    sequence: u64,
    status: &'static str,
}

impl MetadataSyncAttemptDiagnostics {
    fn for_target(target: &DfsMetadataQuorumSyncTarget) -> Self {
        Self {
            peers: target
                .replica_node_ids
                .iter()
                .cloned()
                .map(|node_id| (node_id, BTreeMap::new()))
                .collect(),
        }
    }

    fn record(
        &mut self,
        node_id: &NodeId,
        phase: &'static str,
        request_id: &str,
        sequence: u64,
        status: &'static str,
        attempted: bool,
    ) {
        // A mismatched peer cannot enlarge the assignment-bounded map.
        let Some(phases) = self.peers.get_mut(node_id) else {
            return;
        };
        let last = phases.entry(phase).or_default();
        last.attempts = last.attempts.saturating_add(u64::from(attempted));
        request_id.clone_into(&mut last.request_id);
        last.sequence = sequence;
        last.status = status;
    }
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
    pub checkpoint_snapshot: Option<crate::DfsMetadataSnapshotImage>,
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
    replica_count.div_ceil(2) + 1
}

#[cfg(test)]
thread_local! {
    static BEFORE_REBIND_WRITER_LOCK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn rebind_completed_metadata_shard_membership_handoff(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    active: &DfsMetadataShardConfig,
    pending: Option<&DfsMetadataShardMembershipHandoff>,
    proof: Option<&DfsMetadataShardMembershipHandoffProof>,
) -> Result<bool, DfsError> {
    // Another already-bound writer can append after the read-side lineage
    // check and before the backend acquires its write gate. Retry only that
    // typed comparison failure, re-reading the marker and revalidating the
    // complete proof/lineage each time. Never relax the under-lock comparison.
    let mut prefix_retries = 2;
    loop {
        match rebind_completed_metadata_shard_membership_handoff_once(
            backend,
            local_node_id,
            active,
            pending,
            proof,
        ) {
            Err(DfsError::MetadataRejection {
                code: crate::DfsMetadataRejectionCode::PrefixMismatch,
                ..
            }) if prefix_retries > 0 => {
                prefix_retries -= 1;
            }
            result => return result,
        }
    }
}

fn rebind_completed_metadata_shard_membership_handoff_once(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    active: &DfsMetadataShardConfig,
    pending: Option<&DfsMetadataShardMembershipHandoff>,
    proof: Option<&DfsMetadataShardMembershipHandoffProof>,
) -> Result<bool, DfsError> {
    let shard_id = DfsMetadataShardId::new(active.shard_id.clone());
    if backend.metadata_shard_write_freeze(&shard_id)?.is_none() {
        return Ok(false);
    }
    // The completion proof and marker are durable, but this lane may have
    // opened before canonical replication caught up a different backend
    // handle. Refresh only the marker-bearing cold path before comparing the
    // exact completion prefix.
    backend.refresh_metadata_shard_durable_view_if_changed(&shard_id)?;
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
    let proof_matches_target = match &proof.cause {
        crate::DfsMetadataShardHandoffCause::Membership => {
            proof.state_fingerprint == freeze.state_fingerprint
        }
        crate::DfsMetadataShardHandoffCause::StorageImport { cut } => {
            cut.target_prefix.version == 2
                && cut.target_prefix.is_well_formed()
                && !cut.converted_image_fingerprint.is_empty()
                && cut.target_prefix.journal_sequence == freeze.journal_sequence
                && cut.target_prefix.digest == freeze.state_fingerprint
                && proof.state_fingerprint == cut.converted_image_fingerprint
        }
    };
    let proof_matches_marker_and_active = proof.handoff_id == freeze.handoff_id
        && proof.retiring_shard.file_system_id == active.file_system_id
        && proof.retiring_shard.shard_id == active.shard_id
        && proof.retiring_shard.epoch == freeze.retiring_epoch
        && proof.desired_shard == *active
        && proof.journal_sequence == freeze.journal_sequence
        && proof_matches_target
        && proof.acknowledged_node_ids.contains(local_node_id);
    if exact_successor_epoch != Some(active.epoch)
        || !marker_matches_active_epoch
        || !proof_matches_marker_and_active
    {
        return Ok(false);
    }
    let Some(checked_prefix) = metadata_shard_prefix_matches_or_extends_marker(
        backend,
        &shard_id,
        freeze.journal_sequence,
        &freeze.state_fingerprint,
        &freeze.rebind_prefix_proof,
    )?
    else {
        return Ok(false);
    };
    #[cfg(any(test, feature = "test-support"))]
    let mut native_boundary = crate::native_shard_trace::current_boundary_interest();
    #[cfg(any(test, feature = "test-support"))]
    let native_before = native_boundary
        .as_ref()
        .and_then(|_| backend.native_held_snapshot_for_scope(&active.file_system_id, &shard_id));
    if active.primary_node_id == *local_node_id {
        if let crate::DfsMetadataShardHandoffCause::StorageImport { cut } = &proof.cause {
            // A new physical image has a new native journal clock. Its actual
            // import ACKs can restore the minimum delivery cursor after cold
            // open/loss of the non-durable hint; they never authorize a prepare.
            // Unacknowledged replicas still require ordinary actual catch-up.
            for peer in &active.replica_node_ids {
                if proof.acknowledged_node_ids.contains(peer)
                    && backend.metadata_replication_cursor(peer, &shard_id)?
                        < cut.target_prefix.journal_sequence
                {
                    backend.put_metadata_replication_cursor(
                        peer,
                        &shard_id,
                        cut.target_prefix.journal_sequence,
                    )?;
                }
            }
        }
        #[cfg(test)]
        if let Some(before_lock) = BEFORE_REBIND_WRITER_LOCK.with(|hook| hook.borrow_mut().take()) {
            before_lock();
        }
        let rebound = backend.clear_metadata_shard_membership_handoff_freeze(
            &shard_id,
            &freeze.handoff_id,
            active,
            &checked_prefix,
        )?;
        #[cfg(any(test, feature = "test-support"))]
        if rebound {
            if let Some(trace) = native_boundary.as_mut() {
                let prior_instance_epoch = native_before
                    .as_ref()
                    .and_then(|held| held.backend_instance_authority_epoch);
                trace.observe(
                    crate::native_shard_trace::NativeBoundaryFactsV1::SuccessorRebound {
                        active: active.clone(),
                        proof: proof.clone(),
                        original_freeze: freeze.clone(),
                        prior_instance_epoch,
                    },
                    native_before,
                    backend.native_held_snapshot_for_scope(&active.file_system_id, &shard_id),
                );
            }
        }
        return Ok(rebound);
    }

    // The completing process can crash after the control-plane CAS commits
    // but before it marks the retiring backend's local freeze as activated.
    // On restart this node is no longer the primary, so it must not bind the
    // successor epoch. It still has to advance the exact durable marker:
    // activated retired markers accept replica ingest from the successor and
    // can be superseded by a later proof-bound handoff.
    Ok(backend.mark_metadata_shard_membership_handoff_activated(
        &shard_id,
        &freeze.handoff_id,
        active.epoch,
    )?)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Rebind a cached service lane only when the stamped shard is locally
    /// fenced by a durable membership-handoff marker.
    ///
    /// A target can receive replica traffic before it becomes the committed
    /// primary, so its long-lived service handle may predate handoff
    /// completion. The local marker check keeps ordinary routed mutations on
    /// the no-refresh path; exact control-plane proof is consulted only for a
    /// lane that cannot yet perform authoritative writes.
    pub(crate) fn rebind_completed_local_metadata_shard_membership_handoff_if_frozen(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, DfsError> {
        // An empty, quiescent lane has no prior operational projection to
        // replace. Preserve its lazy state just as the fresh Core opener does;
        // the exact instance rebind still validates the full completion proof.
        let was_lazy =
            self.metadata.objects.is_empty() && self.can_release_hydrated_metadata_projection();
        let rebound = self.rebind_completed_local_metadata_shard_instance_if_frozen(expected)?;
        if rebound {
            if was_lazy {
                self.hydrate_file_systems_from_metadata_backend()?;
            } else {
                self.hydrate_from_metadata_backend()?;
            }
        }
        Ok(rebound)
    }

    /// Bind only this physical shard. A fresh lazy shell has no operational
    /// projection to replace; cached services rebuild theirs in the wrapper.
    pub(crate) fn rebind_completed_local_metadata_shard_instance_if_frozen(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, DfsError> {
        if expected.primary_node_id != self.node_id {
            return Ok(false);
        }
        let shard_id = DfsMetadataShardId::new(expected.shard_id.clone());
        let Some(backend) = self.metadata_backend.as_deref() else {
            return Ok(false);
        };
        let locally_frozen = backend.metadata_shard_instance_writes_frozen(&shard_id)?;
        if !locally_frozen {
            return Ok(false);
        }
        self.refresh_control_plane_latest()?;
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(false);
        };
        let (pending, proof) = {
            let store = DfsControlPlaneStore::new(control_plane.as_mut());
            if store
                .get_metadata_shard(&expected.file_system_id, &expected.shard_id)?
                .as_ref()
                != Some(expected)
            {
                return Ok(false);
            }
            (
                store.get_metadata_shard_membership_handoff(
                    &expected.file_system_id,
                    &expected.shard_id,
                )?,
                store.get_metadata_shard_membership_handoff_proof(
                    &expected.file_system_id,
                    &expected.shard_id,
                )?,
            )
        };
        #[cfg(any(test, feature = "test-support"))]
        let native_owner = self.native_publication_owner_for_observation();
        #[cfg(test)]
        if pending.is_none()
            && proof.is_some()
            && let Some((root, _)) = native_owner.as_ref()
            && let Some(hook) = crate::runtime_host::daemon_dfs_pre_dispatch_test_hook(
                &sinomo_core::local::SinomoDataDir::new(root),
            )
        {
            hook(
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeCompletedPrimaryRebind {
                    shard_id: &shard_id,
                },
                None,
                None,
            )?;
        }
        let rebound = {
            #[cfg(any(test, feature = "test-support"))]
            let _native_context = native_owner.as_ref().map(|(root, owner)| {
                crate::native_shard_trace::enter_boundary_context(root, owner, &shard_id)
            });
            let backend = self
                .metadata_backend
                .as_deref_mut()
                .expect("frozen backend checked");
            rebind_completed_metadata_shard_membership_handoff(
                backend,
                &self.node_id,
                expected,
                pending.as_ref(),
                proof.as_ref(),
            )?
        };
        Ok(rebound)
    }

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
        if self.metadata_backend.is_none() {
            return Ok(0);
        }

        // Retired-replica markers are intentionally durable so independently
        // opened stale handles remain fenced. They cannot be rebound by this
        // node. Checking the backend-wide marker predicate here also opens
        // every deferred replica slot before it can discover a marker on a
        // later local-primary shard. Use the daemon-projected control-plane
        // view to identify only possible local work, then probe those exact
        // shard slots. A stale negative is fail-closed: the marker remains
        // fenced until the projection catches up and a later lane retries. A
        // possible local-primary rebind still refreshes its control-plane
        // client before checking the exact proof and changing the marker.
        let projected_shards = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(0);
            };
            DfsControlPlaneStore::new(control_plane.as_mut())
                .list_metadata_shards(file_system_id)?
        };
        let Some(backend) = self.metadata_backend.as_deref() else {
            return Ok(0);
        };
        let mut has_projected_local_primary_freeze = false;
        for shard in &projected_shards {
            if shard.primary_node_id == self.node_id
                && backend.metadata_shard_instance_writes_frozen(&DfsMetadataShardId::new(
                    shard.shard_id.clone(),
                ))?
            {
                has_projected_local_primary_freeze = true;
                break;
            }
        }
        if !has_projected_local_primary_freeze {
            return Ok(0);
        }

        self.refresh_control_plane_latest()?;
        let local_node_id = self.node_id.clone();
        let observed = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(0);
            };
            let store = DfsControlPlaneStore::new(control_plane.as_mut());
            store
                .list_metadata_shards(file_system_id)?
                .into_iter()
                .filter(|active| active.primary_node_id == local_node_id)
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

        #[cfg(any(test, feature = "test-support"))]
        let native_owner = self.native_publication_owner_for_observation();
        let Some(backend) = self.metadata_backend.as_deref_mut() else {
            return Ok(0);
        };
        let mut rebound = 0usize;
        for (active, pending, proof) in observed {
            #[cfg(test)]
            if pending.is_none()
                && proof.is_some()
                && let Some((root, _)) = native_owner.as_ref()
                && let Some(hook) = crate::runtime_host::daemon_dfs_pre_dispatch_test_hook(
                    &sinomo_core::local::SinomoDataDir::new(root),
                )
            {
                hook(crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeCompletedPrimaryRebind {
                    shard_id: &DfsMetadataShardId::new(&active.shard_id),
                }, None, None)?;
            }
            #[cfg(any(test, feature = "test-support"))]
            let _native_context = native_owner.as_ref().map(|(root, owner)| {
                crate::native_shard_trace::enter_boundary_context(
                    root,
                    owner,
                    &DfsMetadataShardId::new(active.shard_id.clone()),
                )
            });
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
            // Replica ingestion deliberately retains the durable tables rather
            // than a second complete service-state mirror. Rebinding grants
            // this long-lived service authority, so rebuild its operational
            // projection unconditionally before it can admit the first
            // mutation. This also reflects append-producing recovery run by
            // `clear_*` while activating the marker.
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
        self.reconcile_pending_metadata_shard_membership_handoffs_until_cancelled(
            metadata_shards,
            timeout,
            &|| false,
        )
        .map(|completed| completed.unwrap_or_default())
    }

    pub(crate) fn reconcile_pending_metadata_shard_membership_handoffs_until_cancelled(
        &mut self,
        metadata_shards: &[DfsMetadataShardConfig],
        timeout: Duration,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<Option<usize>, DfsError> {
        if should_cancel() {
            return Ok(None);
        }
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
        if should_cancel() {
            return Ok(None);
        }
        self.refresh_control_plane_latest()?;
        let observed = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(Some(0));
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
            if should_cancel() {
                return Ok(None);
            }
            let shard_id = DfsMetadataShardId::new(handoff.retiring_shard.shard_id.clone());
            let cancelled_for_prepare_drain = if handoff.retiring_primary_fence.is_none()
                && self.has_recoverable_standalone_namespace_prepare_on_shard(&shard_id)?
            {
                // Handoff creation can race recovery of a prepare that was
                // already durable under the retiring assignment. Until the
                // retiring-primary fence exists, the assignment has not
                // changed and no successor can activate. Withdraw that exact
                // decision under the publication guard, drain the prepare
                // through normal canonical authorization, and let placement
                // propose a fresh handoff from the resulting head.
                let _publication_guard = self.metadata_shard_fence_guard(
                    &handoff.retiring_shard.file_system_id,
                    &handoff.retiring_shard.shard_id,
                )?;
                self.refresh_control_plane_latest()?;
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Ok(Some(completed));
                };
                DfsControlPlaneStore::new(control_plane.as_mut())
                    .cancel_unfenced_metadata_shard_membership_handoff(&handoff)?
            } else {
                false
            };
            if cancelled_for_prepare_drain {
                self.recover_durable_namespace_prepares()?;
                if should_cancel() {
                    return Ok(None);
                }
                continue;
            }
            if self.reconcile_metadata_shard_membership_handoff(&handoff, timeout)? {
                completed += 1;
                // An actual activation can invalidate the admitted owner cut.
                // Keep later proposals durable for normal release/readmission.
                if should_cancel() {
                    return Ok(None);
                }
            }
        }
        Ok(Some(completed))
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

        // The fresh authority read and the existing prefix exchange share the
        // caller's original window; an exact read cannot add another timeout.
        let requested_deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let deadline = self
            .transport
            .operation_deadline()
            .filter(|existing| *existing < requested_deadline)
            .unwrap_or(requested_deadline);
        let trace_operation = if crate::service_trace::dfs_perf_trace_enabled() {
            format!(
                "handoff:{}:{}:{}:{}",
                self.node_id,
                requested_retiring.file_system_id,
                requested_retiring.shard_id,
                requested_handoff.handoff_id
            )
        } else {
            String::new()
        };
        let mut timing = crate::service_trace::DfsPhaseTimer::new(&trace_operation);
        timing.mark("handoff-started");
        let shard_id = DfsMetadataShardId::new(requested_retiring.shard_id.clone());
        #[cfg(any(test, feature = "test-support"))]
        let mut native_boundary =
            self.native_publication_owner_for_observation()
                .and_then(|(root, owner)| {
                    crate::native_shard_trace::boundary_interest(&root, Some(&owner), &shard_id)
                });

        let (handoff, fence) = {
            // This is the retiring-primary half of the distributed fence. A
            // canonical commit that already passed validation holds this same
            // guard through its backend publication and therefore finishes
            // before the captured head. A later commit observes the refreshed
            // durable pending handoff and fails validation.
            let _publication_guard = self.metadata_shard_fence_guard(
                &requested_retiring.file_system_id,
                &requested_retiring.shard_id,
            )?;
            timing.mark("handoff-fence-acquired");
            let observed_handoff = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Ok(false);
                };
                // A retiring fence needs one operation-bound ReadIndex view.
                // Refreshing a private projection can reuse the host's older
                // retained snapshot after another owner completes this handoff.
                let (active, pending) = DfsControlPlaneStore::new(control_plane.as_mut())
                    .retiring_metadata_shard_handoff_view_before(
                        &requested_retiring.file_system_id,
                        &requested_retiring.shard_id,
                        deadline,
                    )?;
                timing.mark("handoff-authority-refreshed");
                if active != Some(requested_retiring.clone()) {
                    return Ok(false);
                }
                let Some(observed) = pending else {
                    return Ok(false);
                };
                if observed.handoff_id != requested_handoff.handoff_id
                    || observed.retiring_shard != requested_handoff.retiring_shard
                    || observed.desired_shard != requested_handoff.desired_shard
                {
                    return Ok(false);
                }
                observed
            };

            timing.mark("handoff-pending-validated");
            let backend =
                self.metadata_backend
                    .as_deref_mut()
                    .ok_or_else(|| DfsError::Metadata {
                        message: format!(
                            "metadata shard {} membership handoff requires a durable local backend",
                            requested_retiring.shard_id
                        ),
                    })?;
            #[cfg(any(test, feature = "test-support"))]
            let before = native_boundary.as_ref().and_then(|_| {
                backend
                    .native_held_snapshot_for_scope(&requested_retiring.file_system_id, &shard_id)
            });
            let Some(write_freeze) = backend.freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                &observed_handoff.handoff_id,
                requested_retiring.epoch,
            )?
            else {
                return Ok(false);
            };
            timing.mark("handoff-write-freeze-returned");
            #[cfg(any(test, feature = "test-support"))]
            if let Some(trace) = native_boundary.as_mut() {
                let after = backend
                    .native_held_snapshot_for_scope(&requested_retiring.file_system_id, &shard_id);
                trace.observe(
                    crate::native_shard_trace::NativeBoundaryFactsV1::RetiringFreezePersisted {
                        pending: observed_handoff.clone(),
                        freeze: write_freeze.clone(),
                    },
                    before,
                    after,
                );
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
            timing.mark("handoff-fence-cas-returned");
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
        timing.mark("handoff-prefix-proofs-started");
        let certificate = self.sync_metadata_shard_to_quorum_with_required_intersection(
            &target,
            &BTreeSet::new(),
            MetadataQuorumSyncPurpose::MembershipHandoff {
                primary_handoff,
                retiring_shard: retiring.clone(),
                desired_shard: handoff.desired_shard.clone(),
            },
            deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| DfsError::Network {
                    message: "metadata handoff authority read exhausted the caller deadline".into(),
                })?,
        )?;
        timing.mark("handoff-prefix-proofs-returned");
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
        // compares the immutable DFS fence acknowledgement.
        let publication_guard =
            self.metadata_shard_fence_guard(&retiring.file_system_id, &retiring.shard_id)?;
        timing.mark("handoff-activation-fence-acquired");
        self.refresh_control_plane_latest()?;
        timing.mark("handoff-activation-authority-refreshed");
        let checked_prefix = {
            let backend = self
                .metadata_backend
                .as_deref()
                .expect("membership handoff backend checked above");
            let current_sequence = backend
                .metadata_shard_statuses()?
                .into_iter()
                .find(|status| status.shard_id == shard_id)
                .map(|status| status.journal_last_sequence);
            backend.metadata_prefix_proof(&shard_id)?.filter(|prefix| {
                prefix.is_well_formed()
                    && current_sequence == Some(journal_sequence)
                    && prefix.journal_sequence == journal_sequence
                    && prefix.digest == state_fingerprint
            })
        };
        timing.mark("handoff-activation-prefix-read");
        let Some(checked_prefix) = checked_prefix else {
            return Ok(false);
        };

        let proof = DfsMetadataShardMembershipHandoffProof {
            cause: handoff.cause.clone(),
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
        timing.mark("handoff-activation-cas-returned");
        #[cfg(any(test, feature = "test-support"))]
        if completed {
            if let Some(trace) = native_boundary.as_mut() {
                let held = self.metadata_backend.as_ref().and_then(|backend| {
                    backend.native_held_snapshot_for_scope(&retiring.file_system_id, &shard_id)
                });
                trace.observe(
                    crate::native_shard_trace::NativeBoundaryFactsV1::HandoffActivationCompleted {
                        handoff: handoff.clone(),
                        proof: proof.clone(),
                    },
                    held.clone(),
                    held,
                );
            }
        }
        if completed {
            let backend = self
                .metadata_backend
                .as_deref_mut()
                .expect("membership handoff backend checked above");
            if handoff.desired_shard.primary_node_id == self.node_id {
                backend.clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    &handoff.handoff_id,
                    &handoff.desired_shard,
                    &checked_prefix,
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
        timing.mark("handoff-local-activation-returned");
        drop(publication_guard);
        timing.mark("handoff-activation-fence-released");
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
        let configured = if let Some(control) = self.control_plane.as_ref() {
            let projection =
                control
                    .write_configuration_projection()?
                    .ok_or_else(|| DfsError::Metadata {
                        message: "ordinary metadata sync has no retained qualified configuration"
                            .into(),
                    })?;
            let prefix = format!(
                "{}/",
                crate::control_plane::metadata_shards_prefix(&object.file_system_id)
            );
            let entries = projection
                .entries(&prefix)
                .ok_or_else(|| DfsError::Metadata {
                    message: "ordinary metadata sync configuration omits its shard scope".into(),
                })?;
            let shards = entries
                .iter()
                .map(|entry| entry.decode())
                .collect::<Result<Vec<DfsMetadataShardConfig>, _>>()?;
            crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &object.file_system_id,
                &shards,
                &object.path,
            )?
        } else {
            None
        };
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
        // is retained configuration for clustered syncs. Local/unconfigured
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

    /// Inspect sender scheduling checkpoints for a frozen journal prefix.
    /// This observation is not a fresh quorum or publication certificate.
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
        // Scope every request emitted by this invocation to one unique owner.
        // Cleanup must not discard unrelated replication work merely because
        // it happens to target the same shard and replica while this barrier
        // is in flight.
        let probe_barrier_id = format!("{}-{}", target.barrier_id, sinomo_core::Id::generate());
        let result = self.sync_metadata_shard_to_quorum_with_required_intersection_inner(
            target,
            required_intersection,
            &purpose,
            &probe_barrier_id,
            timeout,
        );
        self.transport.set_operation_deadline(previous_deadline);
        self.clear_pending_metadata_sync_replications_for_barrier(&probe_barrier_id);
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
            return self
                .finalize_metadata_full_shard_sync(target, progress, purpose)
                .map(Some);
        }
        if matches!(purpose, MetadataQuorumSyncPurpose::MembershipHandoff { .. })
            && metadata_sync_progress_satisfies(&progress, required_intersection)
            && started_at.elapsed() >= full_shard_grace
            && let Some(certificate) =
                self.try_finalize_metadata_handoff_sync(target, progress, purpose)?
        {
            return Ok(Some(certificate));
        }
        Ok(None)
    }

    fn sync_metadata_shard_to_quorum_with_required_intersection_inner(
        &mut self,
        requested_target: &DfsMetadataQuorumSyncTarget,
        required_intersection: &BTreeSet<NodeId>,
        purpose: &MetadataQuorumSyncPurpose,
        probe_barrier_id: &str,
        timeout: Duration,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        #[cfg(any(test, feature = "test-support"))]
        let _native_context =
            self.native_publication_owner_for_observation()
                .map(|(root, owner)| {
                    crate::native_shard_trace::enter_boundary_context(
                        &root,
                        &owner,
                        &requested_target.shard_id,
                    )
                });
        let mut target = requested_target.clone();
        if target.primary_node_id != self.node_id {
            let authority_epoch = target.authority_epoch.ok_or_else(|| DfsError::Metadata {
                message: format!(
                    "metadata shard {} remote sync target has no committed authority epoch",
                    target.shard_id.0
                ),
            })?;
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: target.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: target.primary_node_id.clone(),
                epoch: authority_epoch,
            });
        }

        let started_at = Instant::now();
        // Persisted replication cursors are scheduling hints, not a proof for
        // this barrier. Each non-primary acknowledgement below comes from a
        // fresh exact-checkpoint probe sent and answered during this call.
        let full_shard_grace = DFS_POSIX_FULL_SHARD_ACK_GRACE.min(timeout);
        let mut freshly_acknowledged = BTreeSet::from([self.node_id.clone()]);
        let mut send_diagnostics = MetadataSyncAttemptDiagnostics::for_target(&target);
        let mut pending_prefix_probes = BTreeMap::<String, NodeId>::new();
        let mut pending_catchup_batches = BTreeMap::<String, NodeId>::new();
        let mut pending_checkpoint_snapshots =
            BTreeMap::<String, MetadataCheckpointSnapshotAttempt>::new();
        // A caller can capture a target and then wait behind another DFS
        // operation before entering the barrier. Upgrade once to the current
        // exact contiguous head before the first probe. Subsequent local
        // appends do not force this barrier to chase an unbounded moving head;
        // a later prefix remains a valid stronger target only when a receiver
        // rejection proves the requested historical head is no longer usable.
        self.refresh_metadata_sync_target_to_current_head(&mut target, purpose)?;
        loop {
            self.check_cancelled("metadata quorum sync")?;
            self.revalidate_metadata_quorum_sync_authority(&target, purpose)?;
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
            let mut rejected_nodes = BTreeSet::new();
            let mut source_history_gap_node_ids = BTreeSet::new();
            if matches!(purpose, MetadataQuorumSyncPurpose::OrdinaryDurability) {
                rejected_nodes.extend(self.send_metadata_sync_catchup_batches(
                    &target,
                    probe_barrier_id,
                    &mut pending_catchup_batches,
                    &mut source_history_gap_node_ids,
                    &mut send_diagnostics,
                )?);
            }
            rejected_nodes.extend(self.send_metadata_sync_prefix_probes(
                &target,
                probe_barrier_id,
                &mut freshly_acknowledged,
                &mut pending_prefix_probes,
                purpose,
                &mut send_diagnostics,
            )?);

            let events = self.process_incoming()?;
            rejected_nodes.extend(record_metadata_sync_prefix_probe_events(
                events,
                &target,
                &mut pending_prefix_probes,
                &mut pending_catchup_batches,
                &mut freshly_acknowledged,
            ));
            if !rejected_nodes.is_empty()
                && self.refresh_metadata_sync_target_to_current_head(&mut target, purpose)?
            {
                self.clear_superseded_metadata_sync_requests(
                    &mut pending_prefix_probes,
                    &mut pending_catchup_batches,
                    &mut pending_checkpoint_snapshots,
                );
                source_history_gap_node_ids.clear();
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
                probe_barrier_id,
                &rejected_nodes,
                &source_history_gap_node_ids,
                &mut pending_checkpoint_snapshots,
                purpose,
            )?;
            let mut rejected_nodes = self.send_metadata_sync_catchup_batches(
                &target,
                probe_barrier_id,
                &mut pending_catchup_batches,
                &mut source_history_gap_node_ids,
                &mut send_diagnostics,
            )?;
            rejected_nodes.extend(self.send_metadata_sync_prefix_probes(
                &target,
                probe_barrier_id,
                &mut freshly_acknowledged,
                &mut pending_prefix_probes,
                purpose,
                &mut send_diagnostics,
            )?);
            let events = self.process_incoming()?;
            rejected_nodes.extend(record_metadata_sync_prefix_probe_events(
                events,
                &target,
                &mut pending_prefix_probes,
                &mut pending_catchup_batches,
                &mut freshly_acknowledged,
            ));
            if !rejected_nodes.is_empty()
                && self.refresh_metadata_sync_target_to_current_head(&mut target, purpose)?
            {
                self.clear_superseded_metadata_sync_requests(
                    &mut pending_prefix_probes,
                    &mut pending_catchup_batches,
                    &mut pending_checkpoint_snapshots,
                );
                source_history_gap_node_ids.clear();
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
                probe_barrier_id,
                &rejected_nodes,
                &source_history_gap_node_ids,
                &mut pending_checkpoint_snapshots,
                purpose,
            )?;
            // A loopback/in-process transport can acknowledge the catch-up
            // batch during the process pass above. Drive the newly eligible
            // exact probe immediately so a zero-wait caller does not need an
            // artificial extra loop; real network transports simply leave it
            // pending for the next wait/pump round.
            let mut rejected_nodes = self.send_metadata_sync_catchup_batches(
                &target,
                probe_barrier_id,
                &mut pending_catchup_batches,
                &mut source_history_gap_node_ids,
                &mut send_diagnostics,
            )?;
            rejected_nodes.extend(self.send_metadata_sync_prefix_probes(
                &target,
                probe_barrier_id,
                &mut freshly_acknowledged,
                &mut pending_prefix_probes,
                purpose,
                &mut send_diagnostics,
            )?);
            let events = self.process_incoming()?;
            rejected_nodes.extend(record_metadata_sync_prefix_probe_events(
                events,
                &target,
                &mut pending_prefix_probes,
                &mut pending_catchup_batches,
                &mut freshly_acknowledged,
            ));
            if !rejected_nodes.is_empty()
                && self.refresh_metadata_sync_target_to_current_head(&mut target, purpose)?
            {
                self.clear_superseded_metadata_sync_requests(
                    &mut pending_prefix_probes,
                    &mut pending_catchup_batches,
                    &mut pending_checkpoint_snapshots,
                );
                source_history_gap_node_ids.clear();
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
                probe_barrier_id,
                &rejected_nodes,
                &source_history_gap_node_ids,
                &mut pending_checkpoint_snapshots,
                purpose,
            )?;

            let progress =
                metadata_sync_progress_from_fresh_acknowledgements(&target, &freshly_acknowledged);
            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                if matches!(purpose, MetadataQuorumSyncPurpose::MembershipHandoff { .. })
                    && metadata_sync_progress_satisfies(&progress, required_intersection)
                    && let Some(certificate) =
                        self.try_finalize_metadata_handoff_sync(&target, progress.clone(), purpose)?
                {
                    return Ok(certificate);
                }
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
                let peer_state = target
                    .replica_node_ids
                    .iter()
                    .map(|node_id| {
                        let cursor = self.metadata_backend.as_deref().map(|backend| {
                            backend.metadata_replication_cursor(node_id, &target.shard_id)
                        });
                        let cursor = match cursor {
                            Some(Ok(sequence)) => sequence.to_string(),
                            Some(Err(_)) => "read-error".to_string(),
                            None => "no-backend".to_string(),
                        };
                        // Report at most four pre-existing requests per peer.
                        // A cap marker makes an incomplete observation explicit.
                        let pending = self.replication.pending_metadata_replications.iter()
                            .filter(|(_, pending)| pending.target_node_id == *node_id
                                && pending.shard_id == target.shard_id)
                            .take(5)
                            .collect::<Vec<_>>();
                        let capped = pending.len() > 4;
                        let pending = pending.into_iter().take(4).map(|(request_id, pending)| {
                            format!("{request_id}:{}:{:?}:prefix={}:age_ms={}",
                                pending.next_after_sequence, pending.expected_response_kind,
                                pending.verify_prefix_only, pending.created_at.elapsed().as_millis())
                        }).collect::<Vec<_>>();
                        let checkpoints = pending_checkpoint_snapshots.iter()
                            .filter(|(_, attempt)| attempt.target_node_id == *node_id)
                            .map(|(request_id, attempt)| format!("{request_id}:{}", attempt.journal_sequence))
                            .take(4).collect::<Vec<_>>();
                        format!("{node_id}:cursor={cursor}:pending={pending:?}:pending_capped={capped}:checkpoints={checkpoints:?}")
                    })
                    .collect::<Vec<_>>();
                let send_state = serde_json::to_string(&send_diagnostics)
                    .unwrap_or_else(|_| "diagnostic-serialization-error".to_string());
                return Err(DfsError::Network {
                    message: format!(
                        "timed out after {}ms before metadata shard {} reached a POSIX sync certificate: {}/{} acknowledgements through sequence {}; missing={missing_node_ids:?}; recovery-set intersection={recovery_intersection}; barrier={probe_barrier_id}; epoch={:?}; fingerprint={}; peer_state={peer_state:?}; send_state={send_state}",
                        timeout.as_millis(),
                        target.shard_id.0,
                        progress.acknowledged_node_ids.len(),
                        required_node_ids.len(),
                        target.journal_sequence,
                        target.authority_epoch,
                        target.state_fingerprint
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
        purpose: &MetadataQuorumSyncPurpose,
    ) -> Result<bool, DfsError> {
        self.revalidate_metadata_quorum_sync_authority(target, purpose)?;
        let backend = self
            .metadata_backend
            .as_deref_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata sync target refresh requires a durable backend".to_string(),
            })?;
        // Another owned writer may have advanced this shard since this handle
        // last replayed it. Attest the durable head rather than a cached prefix.
        backend.refresh_metadata_shard_attestation_view(&target.shard_id)?;
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
        pending_catchup_batches: &mut BTreeMap<String, NodeId>,
        pending_checkpoint_snapshots: &mut BTreeMap<String, MetadataCheckpointSnapshotAttempt>,
    ) {
        for request_id in pending_prefix_probes
            .keys()
            .chain(pending_catchup_batches.keys())
            .chain(pending_checkpoint_snapshots.keys())
        {
            self.replication
                .pending_metadata_replications
                .remove(request_id);
        }
        pending_prefix_probes.clear();
        pending_catchup_batches.clear();
        pending_checkpoint_snapshots.clear();
    }

    /// Close an ordinary replica journal lag with contiguous DFS metadata
    /// batches before asking that replica to attest the frozen prefix.
    ///
    /// The verification request carries no journal frames. Sending it to a
    /// replica whose sender cursor is known to be behind would only
    /// manufacture a rejection and force an expensive full-snapshot repair.
    /// Sender cursors are still scheduling hints rather than proof: once they
    /// reach the target, the operation-bound probe below is what certifies the
    /// receiver's exact hash-chain head.
    fn send_metadata_sync_catchup_batches(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        probe_barrier_id: &str,
        pending_catchup_batches: &mut BTreeMap<String, NodeId>,
        source_history_gap_node_ids: &mut BTreeSet<NodeId>,
        diagnostics: &mut MetadataSyncAttemptDiagnostics,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        self.replication
            .pending_metadata_replications
            .retain(|_, pending| pending.created_at.elapsed() < Duration::from_secs(60));
        let Some(backend) = self.metadata_backend.as_deref() else {
            return Ok(BTreeSet::new());
        };
        let authority_epoch = target.authority_epoch.ok_or_else(|| DfsError::Metadata {
            message: format!(
                "metadata shard {} catch-up has no committed authority epoch",
                target.shard_id.0
            ),
        })?;
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
        // Read its immutable exact frames once. Frame bodies use shared Bytes,
        // so cloning the prepared carrier per target does not copy the journal
        // payload before transport encoding.
        let mut prepared_batches_by_cursor = BTreeMap::<
            u64,
            Option<crate::metadata_replication::DfsPreparedMetadataJournalFrameBatch>,
        >::new();
        let mut prepared_targets = Vec::new();
        for (target_node_id, after_sequence) in replica_cursors {
            if target_node_id == self.node_id || after_sequence >= target.journal_sequence {
                continue;
            }
            if self
                .replication
                .pending_metadata_replications
                .values()
                .any(|pending| {
                    pending.target_node_id == target_node_id
                        && pending.shard_id == target.shard_id
                        && !pending.verify_prefix_only
                })
            {
                continue;
            }
            let prepared_batch =
                if let Some(cached) = prepared_batches_by_cursor.get(&after_sequence) {
                    cached.clone()
                } else {
                    let prepared = backend
                        .journal_batch_for_shard_since_limited(
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
                        })
                        .map(|batch| {
                            crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
                                backend, &batch,
                            )
                        })
                        .transpose()?;
                    prepared_batches_by_cursor.insert(after_sequence, prepared.clone());
                    prepared
                };
            let Some(batch) = prepared_batch else {
                // A retained native checkpoint can outlive its journal frames.
                // This is local source availability, not a receiver rejection
                // or an acknowledgement. Repair through the existing verified
                // snapshot attempt, then obtain the ordinary fresh prefix proof.
                source_history_gap_node_ids.insert(target_node_id);
                continue;
            };
            if after_sequence == 0
                || target.journal_sequence.saturating_sub(after_sequence)
                    >= DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT as u64
            {
                crate::catchup_diagnostics::record(
                    &self.node_id,
                    &target_node_id,
                    &target.shard_id,
                    "durability-barrier",
                    "journal-backfill",
                    Some(after_sequence),
                    Some(target.journal_sequence),
                    "durability barrier requires historical journal catch-up",
                );
            }
            prepared_targets.push((target_node_id, batch));
        }

        // End the immutable backend borrow before allocating request identities
        // or entering transport paths which need the whole service mutably.
        let mut prepared_catchups = Vec::with_capacity(prepared_targets.len());
        for (target_node_id, batch) in prepared_targets {
            let request_id = format!(
                "dfs_sync_catchup_{}_{}",
                probe_barrier_id,
                self.next_request_id()
            );
            let mut command =
                crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
                    &self.node_id,
                    &target_node_id,
                    &target.file_system_id,
                    authority_epoch,
                    batch,
                    false,
                );
            command.request_id = request_id.clone();
            let next_after_sequence = command.batch.next_after_sequence;
            let required_confirmation_id = command
                .durable_replication
                .as_ref()
                .map(|replication| replication.confirmation_id.clone())
                .ok_or_else(|| DfsError::Metadata {
                    message: format!(
                        "metadata shard {} durable catch-up command omitted its confirmation identity",
                        target.shard_id.0
                    ),
                })?;
            prepared_catchups.push((
                target_node_id.clone(),
                command,
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: target.shard_id.clone(),
                    next_after_sequence,
                    expected_response_kind: PendingMetadataReplicationResponseKind::JournalBatch,
                    verify_prefix_only: false,
                    required_authority_confirmation_id: Some(required_confirmation_id),
                    created_at: Instant::now(),
                },
            ));
        }

        let direct_requests = prepared_catchups
            .iter()
            .map(|(node_id, command, _)| (node_id.clone(), command.clone()))
            .collect::<Vec<_>>();
        for (node_id, command) in &direct_requests {
            diagnostics.record(
                node_id,
                "direct-catchup",
                &command.request_id,
                command.batch.next_after_sequence,
                "no-response",
                true,
            );
        }
        let mut directly_rejected_catchups = BTreeSet::new();
        let directly_accepted_catchups = if direct_requests.is_empty() {
            BTreeSet::new()
        } else {
            match self
                .transport
                .send_metadata_journal_batches_direct(&direct_requests)
            {
                Ok(Some(responses)) => {
                    let mut pending_by_response = prepared_catchups
                        .iter()
                        .map(|(node_id, command, pending)| {
                            (
                                (node_id.clone(), command.request_id.clone()),
                                pending.clone(),
                            )
                        })
                        .collect::<BTreeMap<_, _>>();
                    let mut accepted = BTreeSet::new();
                    for (source_node_id, response) in responses {
                        tracing::trace!(
                            target: "sinomo.dfs.perf",
                            peer = %source_node_id,
                            request_id = %response.request_id,
                            shard = %response.shard_id.0,
                            result = ?response.result,
                            "metadata-sync-native-catchup-response",
                        );
                        let response_key = (source_node_id.clone(), response.request_id.clone());
                        let Some(pending) = pending_by_response.remove(&response_key) else {
                            diagnostics.record(
                                &source_node_id,
                                "direct-catchup",
                                &response.request_id,
                                response.next_after_sequence,
                                "request-or-peer-mismatch",
                                false,
                            );
                            continue;
                        };
                        let reply_status = if response.shard_id != pending.shard_id {
                            "shard-mismatch"
                        } else if response.next_after_sequence != pending.next_after_sequence {
                            "sequence-mismatch"
                        } else if matches!(
                            response.result,
                            DfsMetadataJournalBatchResult::Rejected { .. }
                        ) {
                            "receiver-rejected"
                        } else if response.verified_prefix_only != pending.verify_prefix_only {
                            "prefix-kind-mismatch"
                        } else if response.verified_authority_probe_id.as_deref()
                            != pending.required_authority_confirmation_id.as_deref()
                        {
                            "confirmation-mismatch"
                        } else {
                            "identity-matched"
                        };
                        diagnostics.record(
                            &source_node_id,
                            "direct-catchup",
                            &response.request_id,
                            response.next_after_sequence,
                            reply_status,
                            false,
                        );
                        self.replication
                            .pending_metadata_replications
                            .insert(response.request_id.clone(), pending);
                        let event = self.handle_metadata_journal_batch_response(
                            source_node_id.clone(),
                            response,
                        )?;
                        // The direct lane is only authoritative after the
                        // common response validator accepts the exact durable
                        // sequence and confirmation identity. A completed but
                        // rejected/mismatched response must still use the
                        // routed compatibility path.
                        if matches!(
                            &event,
                            Some(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                                verified_prefix_only: false,
                                ..
                            })
                        ) {
                            accepted.insert(response_key);
                        } else if matches!(
                            event,
                            Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected { .. })
                        ) {
                            directly_rejected_catchups.insert(source_node_id);
                        }
                    }
                    accepted
                }
                outcome => {
                    if let Err(error) = &outcome {
                        tracing::trace!(target: "sinomo.dfs.perf", %error,
                            "metadata-sync-native-catchup-send-failed");
                    }
                    let status = if outcome.is_err() {
                        "transport-error"
                    } else {
                        "unsupported"
                    };
                    for (node_id, command) in &direct_requests {
                        diagnostics.record(
                            node_id,
                            "direct-catchup",
                            &command.request_id,
                            command.batch.next_after_sequence,
                            status,
                            false,
                        );
                    }
                    BTreeSet::new()
                }
            }
        };

        for (target_node_id, command, pending) in prepared_catchups {
            let request_id = command.request_id.clone();
            if directly_accepted_catchups.contains(&(target_node_id.clone(), request_id.clone())) {
                continue;
            }
            // The direct path is an accelerator and may legally
            // return only the siblings that completed before its deadline.
            // Preserve the same request identity and enqueue every omitted
            // target on the canonical routed path so a slow first handshake
            // cannot livelock a durability barrier.
            self.replication
                .pending_metadata_replications
                .insert(request_id.clone(), pending);
            pending_catchup_batches.insert(request_id.clone(), target_node_id.clone());
            let sequence = command.batch.next_after_sequence;
            let sent = self.send_best_effort_message_if_routable(
                &target_node_id,
                DfsNetworkMessage::MetadataJournalBatch(command),
            )?;
            diagnostics.record(
                &target_node_id,
                "routed-catchup",
                &request_id,
                sequence,
                if sent { "sent" } else { "not-routable" },
                true,
            );
            if !sent {
                self.replication
                    .pending_metadata_replications
                    .remove(&request_id);
                pending_catchup_batches.remove(&request_id);
            }
        }
        Ok(directly_rejected_catchups)
    }

    fn send_metadata_sync_prefix_probes(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        probe_barrier_id: &str,
        freshly_acknowledged: &mut BTreeSet<NodeId>,
        pending_prefix_probes: &mut BTreeMap<String, NodeId>,
        purpose: &MetadataQuorumSyncPurpose,
        diagnostics: &mut MetadataSyncAttemptDiagnostics,
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
        let probe = crate::DfsMetadataJournalFrameBatch {
            shard_id: target.shard_id.clone(),
            journal_frames: Vec::new(),
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
                    durable_replication: None,
                    batch: probe.clone(),
                },
                required_authority_confirmation_id,
            ));
        }

        // The canonical metadata route owns both the durable prefix floor and
        // the proof-bound promoted-primary marker. Its authoritative context
        // lookup is independent of the long-lived routed message lane, which
        // can lag a just-committed handoff generation. Routed delivery uses the
        // same current request when the direct transport is unavailable.
        let direct_requests = candidates
            .iter()
            .map(|(node_id, command, _)| (node_id.clone(), command.clone()))
            .collect::<Vec<_>>();
        for (node_id, command) in &direct_requests {
            diagnostics.record(
                node_id,
                "direct-probe",
                &command.request_id,
                command.batch.next_after_sequence,
                "no-response",
                true,
            );
        }
        let mut rejected_node_ids = BTreeSet::new();
        let mut directly_accepted_probes = BTreeSet::new();
        let directly_completed_probes = if direct_requests.is_empty() {
            BTreeSet::new()
        } else {
            match self
                .transport
                .send_metadata_journal_batches_direct(&direct_requests)
            {
                Ok(Some(responses)) => {
                    let mut completed = BTreeSet::new();
                    for (source_node_id, response) in responses {
                        let expected = candidates.iter().find(|(target_node_id, command, _)| {
                            target_node_id == &source_node_id
                                && command.request_id == response.request_id
                        });
                        if expected.is_some() {
                            completed.insert((source_node_id.clone(), response.request_id.clone()));
                        }
                        let accepted = expected.is_some_and(
                            |(_, command, required_authority_confirmation_id)| {
                                response.shard_id == command.batch.shard_id
                                    && response.next_after_sequence
                                        == command.batch.next_after_sequence
                                    && response.verified_prefix_only
                                    && response.verified_authority_probe_id.as_deref()
                                        == required_authority_confirmation_id.as_deref()
                                    && matches!(
                                        response.result,
                                        DfsMetadataJournalBatchResult::Applied
                                    )
                            },
                        );
                        let reply_status = match expected {
                            None => "request-or-peer-mismatch",
                            Some((_, command, required_confirmation)) => {
                                if response.shard_id != command.batch.shard_id {
                                    "shard-mismatch"
                                } else if response.next_after_sequence
                                    != command.batch.next_after_sequence
                                {
                                    "sequence-mismatch"
                                } else if matches!(
                                    response.result,
                                    DfsMetadataJournalBatchResult::Rejected { .. }
                                ) {
                                    "receiver-rejected"
                                } else if !response.verified_prefix_only {
                                    "prefix-kind-mismatch"
                                } else if response.verified_authority_probe_id.as_deref()
                                    != required_confirmation.as_deref()
                                {
                                    "confirmation-mismatch"
                                } else {
                                    "accepted"
                                }
                            }
                        };
                        diagnostics.record(
                            &source_node_id,
                            "direct-probe",
                            &response.request_id,
                            response.next_after_sequence,
                            reply_status,
                            false,
                        );
                        if accepted {
                            // Observe this SAME normal direct-probe acceptance.
                            // It bypasses the routed response handler, which
                            // records the equivalent consumed return there.
                            #[cfg(any(test, feature = "test-support"))]
                            if let Some((expected_target, command, required_confirmation)) =
                                expected
                                && let Some((root, owner)) =
                                    self.native_publication_owner_for_observation()
                                && let Some(mut trace) =
                                    crate::native_shard_trace::boundary_interest(
                                        &root,
                                        Some(&owner),
                                        &command.batch.shard_id,
                                    )
                            {
                                trace.observe(
                                    crate::native_shard_trace::NativeBoundaryFactsV1::SourceJournalResponseAccepted {
                                        authenticated_source_node_id: source_node_id.clone(),
                                        response: response.clone(),
                                        expected_target_node_id: expected_target.clone(),
                                        expected_next_after_sequence: command.batch.next_after_sequence,
                                        expected_prefix_only: command.verify_prefix_only,
                                        expected_confirmation_id: required_confirmation.clone(),
                                    },
                                    None, None,
                                );
                            }
                            directly_accepted_probes
                                .insert((source_node_id.clone(), response.request_id));
                            freshly_acknowledged.insert(source_node_id);
                        } else if expected.is_some_and(|(_, _, required_confirmation)| {
                            required_confirmation.is_none()
                        }) {
                            rejected_node_ids.insert(source_node_id);
                        }
                    }
                    completed
                }
                outcome => {
                    let status = if outcome.is_err() {
                        "transport-error"
                    } else {
                        "unsupported"
                    };
                    for (node_id, command) in &direct_requests {
                        diagnostics.record(
                            node_id,
                            "direct-probe",
                            &command.request_id,
                            command.batch.next_after_sequence,
                            status,
                            false,
                        );
                    }
                    BTreeSet::new()
                }
            }
        };

        let mut routed_probes = Vec::new();
        for (target_node_id, command, required_authority_confirmation_id) in candidates {
            let request_id = command.request_id.clone();
            let probe_identity = (target_node_id.clone(), request_id.clone());
            if directly_accepted_probes.contains(&probe_identity) {
                continue;
            }
            if required_authority_confirmation_id.is_none()
                && directly_completed_probes.contains(&probe_identity)
            {
                continue;
            }
            self.replication.pending_metadata_replications.insert(
                request_id.clone(),
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: target.shard_id.clone(),
                    next_after_sequence: target.journal_sequence,
                    expected_response_kind: PendingMetadataReplicationResponseKind::JournalBatch,
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
            diagnostics.record(
                &expected_target_node_id,
                "routed-probe",
                &request_id,
                target.journal_sequence,
                if outcome.result.is_ok() {
                    "sent"
                } else {
                    "transport-error"
                },
                true,
            );
            if outcome.result.is_ok() {
                pending_prefix_probes.insert(request_id, expected_target_node_id);
            } else {
                self.replication
                    .pending_metadata_replications
                    .remove(&request_id);
            }
        }
        Ok(rejected_node_ids)
    }

    fn send_metadata_sync_checkpoint_snapshots(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        probe_barrier_id: &str,
        rejected_node_ids: &BTreeSet<NodeId>,
        source_history_gap_node_ids: &BTreeSet<NodeId>,
        pending_checkpoint_snapshots: &mut BTreeMap<String, MetadataCheckpointSnapshotAttempt>,
        purpose: &MetadataQuorumSyncPurpose,
    ) -> Result<(), DfsError> {
        let attempts = rejected_node_ids
            .union(source_history_gap_node_ids)
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
        // tables. A settled repair and an unroutable attempt are terminal
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
        // Every supplied image carries the same verified native stream cut.
        checkpoint_snapshot.validate()?;
        if !checkpoint_snapshot
            .descriptor()
            .matches_checkpoint(target.journal_sequence, &target.state_fingerprint)
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
            crate::catchup_diagnostics::record(
                &self.node_id,
                &target_node_id,
                &target.shard_id,
                "durability-barrier",
                "snapshot-repair",
                None,
                Some(target.journal_sequence),
                if source_history_gap_node_ids.contains(&target_node_id) {
                    "native journal frames unavailable; rebuilding the frozen checkpoint"
                } else {
                    "incremental catch-up rejected; rebuilding the frozen checkpoint"
                },
            );
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
                primary_handoff_id: purpose
                    .primary_handoff_id_for(&target_node_id)
                    .map(str::to_string),
                shard_id: target.shard_id.clone(),
                next_after_sequence: target.journal_sequence,
                snapshot: checkpoint_snapshot.descriptor().clone(),
                image: Some(checkpoint_snapshot.clone()),
                reason: format!(
                    "restore frozen POSIX sync checkpoint {}",
                    target.state_fingerprint
                ),
            };
            self.replication.pending_metadata_replications.insert(
                request_id.clone(),
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: target.shard_id.clone(),
                    next_after_sequence: target.journal_sequence,
                    expected_response_kind: PendingMetadataReplicationResponseKind::SnapshotInstall,
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
                self.replication
                    .pending_metadata_replications
                    .remove(&request_id);
            }
        }
        Ok(())
    }

    fn finalize_metadata_full_shard_sync(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        progress: DfsMetadataQuorumSyncProgress,
        purpose: &MetadataQuorumSyncPurpose,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        self.revalidate_metadata_quorum_sync_authority(target, purpose)?;
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

    /// Membership handoff can certify an exact frozen prefix from joint
    /// majorities plus both primaries. Ordinary sync requires every assigned
    /// participant and has no global-KV durability fallback.
    fn try_finalize_metadata_handoff_sync(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        progress: DfsMetadataQuorumSyncProgress,
        purpose: &MetadataQuorumSyncPurpose,
    ) -> Result<Option<DfsMetadataQuorumSyncCertificate>, DfsError> {
        self.revalidate_metadata_quorum_sync_authority(target, purpose)?;
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
        Ok(None)
    }

    fn revalidate_metadata_quorum_sync_authority(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        purpose: &MetadataQuorumSyncPurpose,
    ) -> Result<(), DfsError> {
        if matches!(purpose, MetadataQuorumSyncPurpose::OrdinaryDurability) {
            let backend = self
                .metadata_backend
                .as_deref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "ordinary metadata sync has no actual durable backend".into(),
                })?;
            if backend.metadata_shard_instance_writes_frozen(&target.shard_id)?
                || backend
                    .metadata_shard_write_freeze(&target.shard_id)?
                    .is_some_and(|freeze| freeze.activated_epoch != target.authority_epoch)
            {
                return Err(DfsError::Metadata {
                    message:
                        "ordinary metadata sync backend is fenced by another authority generation"
                            .into(),
                });
            }
            let Some(epoch) = target.authority_epoch else {
                return if self.control_plane.is_none()
                    && target.expected_shard.is_none()
                    && target.replica_node_ids.is_empty()
                    && target.primary_node_id == self.node_id
                {
                    Ok(())
                } else {
                    Err(DfsError::Metadata {
                        message:
                            "ordinary local metadata sync has a configured participant assignment"
                                .into(),
                    })
                };
            };
            let context = self
                .retained_metadata_replica_context(&target.file_system_id, &target.shard_id)?
                .ok_or_else(|| DfsError::Metadata {
                    message: "ordinary metadata sync has no retained qualified shard context"
                        .into(),
                })?;
            if context.pending_membership_handoff.is_some()
                || target.expected_shard.as_ref() != Some(&context.shard)
                || context.shard.epoch != epoch
                || context.shard.primary_node_id != self.node_id
                || target.primary_node_id != self.node_id
                || target.replica_node_ids
                    != normalized_replica_node_ids(
                        &self.node_id,
                        context.shard.replica_node_ids.clone(),
                    )
            {
                return Err(DfsError::StaleOwnerEpoch {
                    file_system_id: target.file_system_id.clone(),
                    local_node_id: self.node_id.clone(),
                    primary_node_id: context.shard.primary_node_id,
                    epoch: context.shard.epoch,
                });
            }
            return Ok(());
        }
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
            .current_metadata_handoff_sync_shard_config(target)?
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

    // sinomo:kv-consistency: Validate the exact retiring assignment only for the existing joint handoff.
    fn current_metadata_handoff_sync_shard_config(
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

    fn clear_pending_metadata_sync_replications_for_barrier(&mut self, probe_barrier_id: &str) {
        let catchup_prefix = format!("dfs_sync_catchup_{probe_barrier_id}_");
        let probe_prefix = format!("dfs_sync_probe_{probe_barrier_id}_");
        let checkpoint_prefix = format!("dfs_sync_checkpoint_{probe_barrier_id}_");
        self.replication
            .pending_metadata_replications
            .retain(|request_id, _| {
                !request_id.starts_with(&catchup_prefix)
                    && !request_id.starts_with(&probe_prefix)
                    && !request_id.starts_with(&checkpoint_prefix)
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
/// Read-only native inspection of the exact installed anchor or its verified
/// journal descendant. This grants no mutation authority and never performs
/// writer recovery. Missing/pruned ancestry returns `None`, never a clock-only
/// acceptance. The existing bounded exact-frame validator owns all lineage.
pub fn inspect_metadata_prefix_descendant(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    anchor: &DfsMetadataPrefixProof,
) -> Result<Option<DfsMetadataPrefixProof>, DfsError> {
    let mut backend =
        crate::LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), root.to_owned())?;
    backend.begin_inspection_snapshot()?;
    let initial = DfsMetadataBackend::metadata_prefix_proof(&backend, shard_id)?;
    let through = initial.as_ref().map_or(0, |prefix| prefix.journal_sequence);
    let result = metadata_shard_prefix_matches_or_extends_marker_with_reader(
        &backend,
        shard_id,
        anchor.journal_sequence,
        &anchor.digest,
        anchor,
        |after| {
            // The current-format inspection pins the exact closed prefix.
            // A converter may already own this root's physical writer fence;
            // inspecting those durable bytes must not acquire it a second time.
            let limit = usize::try_from(through.saturating_sub(after))
                .unwrap_or(usize::MAX)
                .min(DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
            let entries =
                crate::metadata_journal::durable_journal_entries_since_limited(root, after, limit)?;
            Ok(crate::metadata_journal::bounded_metadata_journal_batch(
                shard_id.clone(),
                after,
                entries,
                limit,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )?)
        },
    );
    backend.end_inspection_snapshot();
    let proof = result?;
    if proof.is_none() {
        return Ok(None);
    }
    // DATA inspection grants nothing if its independently re-read current
    // prefix moved while frames were being checked.
    backend.begin_inspection_snapshot()?;
    let final_prefix = DfsMetadataBackend::metadata_prefix_proof(&backend, shard_id);
    backend.end_inspection_snapshot();
    if final_prefix? != initial {
        return Ok(None);
    }
    Ok(proof)
}

fn metadata_shard_prefix_matches_or_extends_marker(
    backend: &dyn DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    marker_sequence: u64,
    marker_fingerprint: &str,
    rebind_prefix_proof: &DfsMetadataPrefixProof,
) -> Result<Option<DfsMetadataPrefixProof>, DfsError> {
    metadata_shard_prefix_matches_or_extends_marker_with_reader(
        backend,
        shard_id,
        marker_sequence,
        marker_fingerprint,
        rebind_prefix_proof,
        |after| {
            Ok(backend.journal_batch_for_shard_since_limited(
                shard_id,
                after,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )?)
        },
    )
}

fn metadata_shard_prefix_matches_or_extends_marker_with_reader(
    backend: &dyn DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    marker_sequence: u64,
    marker_fingerprint: &str,
    rebind_prefix_proof: &DfsMetadataPrefixProof,
    mut read_batch: impl FnMut(u64) -> Result<Option<DfsMetadataJournalBatch>, DfsError>,
) -> Result<Option<DfsMetadataPrefixProof>, DfsError> {
    let Some(current) = backend.metadata_prefix_proof(shard_id)? else {
        return Ok(None);
    };
    let mut reconstructed = rebind_prefix_proof.clone();
    if !current.is_well_formed()
        || !reconstructed.is_well_formed()
        || reconstructed.journal_sequence < marker_sequence
        || current.journal_sequence < reconstructed.journal_sequence
    {
        return Ok(None);
    }
    if reconstructed.journal_sequence == marker_sequence
        && reconstructed.digest != marker_fingerprint
    {
        return Ok(None);
    }
    while reconstructed.journal_sequence < current.journal_sequence {
        let Some(batch) = read_batch(reconstructed.journal_sequence)? else {
            return Ok(None);
        };
        if batch.entries.is_empty() || batch.shard_id != *shard_id {
            return Ok(None);
        }
        let frames = backend.journal_frames_for_batch(&batch)?;
        if frames.len() != batch.entries.len() {
            return Ok(None);
        }
        for (entry, frame) in batch.entries.iter().zip(frames) {
            if entry.shard_id != *shard_id
                || entry.sequence != reconstructed.journal_sequence.checked_add(1).unwrap_or(0)
                || entry.sequence > current.journal_sequence
                || frame.sequence != entry.sequence
            {
                return Ok(None);
            }
            reconstructed = metadata_prefix_proof_after_exact_frame(
                &reconstructed,
                entry,
                shard_id,
                &frame.exact_bytes,
            )?;
        }
    }
    Ok((reconstructed == current).then_some(current))
}

fn metadata_shard_snapshot_at(
    backend: &dyn DfsMetadataBackend,
    shard_id: &DfsMetadataShardId,
    expected_journal_sequence: u64,
    expected_state_fingerprint: &str,
) -> Result<crate::DfsMetadataSnapshotImage, DfsError> {
    #[cfg(test)]
    DFS_METADATA_SYNC_SNAPSHOT_BUILD_CALLS.set(
        DFS_METADATA_SYNC_SNAPSHOT_BUILD_CALLS
            .get()
            .saturating_add(1),
    );
    let snapshot = backend
        .export_shard_snapshot_image(shard_id)?
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "metadata shard {} cannot export a materialized snapshot for a durability proof",
                shard_id.0
            ),
        })?;
    let materialized_sequence = snapshot.materialized_through_sequence;
    if materialized_sequence != expected_journal_sequence {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata shard {} materialized sequence {} does not match durability head {}",
                shard_id.0, materialized_sequence, expected_journal_sequence
            ),
        });
    }
    snapshot.validate()?;
    let matches = snapshot
        .descriptor()
        .matches_checkpoint(expected_journal_sequence, expected_state_fingerprint);
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
    pending_catchup_batches: &mut BTreeMap<String, NodeId>,
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
                pending_catchup_batches.remove(&request_id);
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
            } if (pending_prefix_probes.remove(&request_id).is_some()
                || pending_catchup_batches.remove(&request_id).is_some()) =>
            {
                rejected_node_ids.insert(source_node_id);
            }
            _ => {}
        }
    }
    rejected_node_ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    struct NormalSyncFixture {
        // Drop the live client/metadata lease before the owning runtime.
        service: NetworkedDfsService<crate::DfsNodeTransport>,
        runtime: crate::test_runtime::DfsTestRuntime,
        control_plane_probe: crate::test_runtime::DfsTestControlPlaneProbe,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
    }

    fn configured_normal_sync_fixture(name: &str, replicas: Vec<NodeId>) -> NormalSyncFixture {
        let owner = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, replicas.len() + 1)
            .remove(0);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(name, owner.clone())
            .expect("normal sync runtime");
        for replica in &replicas {
            runtime
                .define_node(replica.clone())
                .expect("define real replica");
        }
        runtime
            .start_nodes()
            .expect("ready real Embedded receivers");
        let shard = runtime.root_shard().expect("normal planned root shard");
        let (mut service, control_plane_probe) = runtime
            .client_with_control_plane_probe(&owner)
            .expect("normal observed primary client");
        service
            .bootstrap_distributed_sifs()
            .expect("normal filesystem bootstrap");
        let object = service
            .metadata_backend()
            .expect("normal metadata backend")
            .get_object_by_path_on_shard(
                &DfsMetadataShardId::new("root"),
                &shard.file_system_id,
                "/",
            )
            .expect("read canonical root object")
            .expect("bootstrapped root");
        // This real source-only mutation keeps the fixture's target ahead of
        // bootstrap receipts without manufacturing any receiver acknowledgement.
        let mut filesystem = runtime.file_system();
        filesystem.display_name = format!("normal-sync-{name}");
        service
            .add_file_system(filesystem)
            .expect("append real target generation");
        NormalSyncFixture {
            service,
            runtime,
            control_plane_probe,
            object,
            shard,
        }
    }

    impl NormalSyncFixture {
        fn finish(self) {
            let Self {
                service, runtime, ..
            } = self;
            drop(service);
            runtime
                .close()
                .expect("normal sync runtime closes all owned work");
        }
    }

    #[test]
    fn native_rebind_rechecks_a_prefix_advanced_after_its_read() {
        let (runtime, initial, active) = crate::test_support::current_native_metadata_service(
            "native-rebind-prefix-advance",
            2,
            "/services",
        )
        .unwrap();
        drop(initial);
        let owner = active.primary_node_id.clone();
        let shard_id = DfsMetadataShardId::new(&active.shard_id);
        let control = runtime.shared_control_plane().unwrap();
        let proof = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard_membership_handoff_proof(
                    &active.file_system_id,
                    &active.shard_id,
                )
            })
            .unwrap()
            .unwrap();
        drop(control);
        assert_eq!(proof.desired_shard, active);
        let mut writer = runtime.metadata_primary_service("/services").unwrap();
        let mut cold = crate::LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(),
            crate::default_dfs_metadata_shards_root(runtime.data_dir(&owner))
                .join(crate::metadata_keys::key_segment(&active.shard_id)),
        )
        .unwrap();
        assert!(
            cold.metadata_shard_instance_writes_frozen(&shard_id)
                .unwrap()
        );
        let initial_prefix = DfsMetadataBackend::metadata_prefix_proof(&cold, &shard_id)
            .unwrap()
            .unwrap();
        for invalidity in ["epoch", "digest", "ack"] {
            let mut invalid = proof.clone();
            match invalidity {
                "epoch" => invalid.desired_shard.epoch += 1,
                "digest" => invalid.state_fingerprint.push_str("-negative"),
                "ack" => invalid.acknowledged_node_ids.clear(),
                _ => unreachable!(),
            }
            assert!(
                !rebind_completed_metadata_shard_membership_handoff(
                    &mut cold,
                    &owner,
                    &active,
                    None,
                    Some(&invalid)
                )
                .unwrap()
            );
            assert!(
                cold.metadata_shard_instance_writes_frozen(&shard_id)
                    .unwrap()
            );
            assert_eq!(
                DfsMetadataBackend::metadata_prefix_proof(&cold, &shard_id)
                    .unwrap()
                    .unwrap(),
                initial_prefix
            );
        }
        struct ClearHook;
        impl Drop for ClearHook {
            fn drop(&mut self) {
                BEFORE_REBIND_WRITER_LOCK.with(|hook| {
                    hook.borrow_mut().take();
                });
            }
        }
        let clear_hook = ClearHook;
        BEFORE_REBIND_WRITER_LOCK.with(|hook| {
            assert!(hook.borrow().is_none());
            *hook.borrow_mut() = Some(Box::new(move || {
                // Another genuine bound writer appends after the cold handle's
                // proof read. No prefix or authority observation is fabricated.
                writer
                    .posix_mkdir_all("/services/native-rebind-concurrent-write")
                    .unwrap();
            }));
        });
        assert!(
            rebind_completed_metadata_shard_membership_handoff(
                &mut cold,
                &owner,
                &active,
                None,
                Some(&proof)
            )
            .unwrap()
        );
        assert!(
            !cold
                .metadata_shard_instance_writes_frozen(&shard_id)
                .unwrap()
        );
        let final_prefix = DfsMetadataBackend::metadata_prefix_proof(&cold, &shard_id)
            .unwrap()
            .unwrap();
        assert!(final_prefix.journal_sequence > initial_prefix.journal_sequence);
        assert!(
            cold.get_object_by_path(
                &active.file_system_id,
                "/services/native-rebind-concurrent-write"
            )
            .unwrap()
            .is_some()
        );
        drop(cold);
        // Continuous real appends cannot turn bounded retry into unchecked
        // authority or an unbounded cold-open loop.
        fn arm_append(
            writer: std::sync::Arc<std::sync::Mutex<NetworkedDfsService<crate::DfsNodeTransport>>>,
            hits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
            remaining: usize,
        ) {
            BEFORE_REBIND_WRITER_LOCK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    let index = hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    writer
                        .lock()
                        .unwrap()
                        .posix_mkdir_all(&format!("/services/native-rebind-busy-{index}"))
                        .unwrap();
                    if remaining > 1 {
                        arm_append(writer, hits, remaining - 1);
                    }
                }));
            });
        }
        let writer = std::sync::Arc::new(std::sync::Mutex::new(
            runtime.metadata_primary_service("/services").unwrap(),
        ));
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut cold = crate::LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(),
            crate::default_dfs_metadata_shards_root(runtime.data_dir(&owner))
                .join(crate::metadata_keys::key_segment(&active.shard_id)),
        )
        .unwrap();
        arm_append(
            std::sync::Arc::clone(&writer),
            std::sync::Arc::clone(&hits),
            4,
        );
        let error = rebind_completed_metadata_shard_membership_handoff(
            &mut cold,
            &owner,
            &active,
            None,
            Some(&proof),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            DfsError::MetadataRejection {
                code: crate::DfsMetadataRejectionCode::PrefixMismatch,
                ..
            }
        ));
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert!(
            cold.metadata_shard_instance_writes_frozen(&shard_id)
                .unwrap()
        );
        drop(clear_hook);
        drop(writer);
        assert!(
            rebind_completed_metadata_shard_membership_handoff(
                &mut cold,
                &owner,
                &active,
                None,
                Some(&proof)
            )
            .unwrap()
        );
        assert!(
            !cold
                .metadata_shard_instance_writes_frozen(&shard_id)
                .unwrap()
        );
        drop(cold);
        runtime.close().unwrap();
    }

    #[cfg(feature = "storage-conversion")]
    #[test]
    fn storage_import_rebind_checks_target_prefix_and_separate_image_identity() {
        let name = "native-import-target-prefix-rebind";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 1);
        let fixture = configured_normal_sync_fixture(name, Vec::new());
        let handle = fixture.runtime.runtime_handle();
        let _entered = handle.enter();
        let dir = sinomo_testdir::TestDir::new(name);
        let shard = DfsMetadataShardId::new(&fixture.shard.shard_id);
        let mut backend = LocalSobjMetadataBackend::new(shard.clone(), dir.path()).unwrap();
        let identity = inspect_metadata_candidate(dir.path()).unwrap();
        let freeze = backend
            .freeze_metadata_shard_for_membership_handoff(&shard, name, fixture.shard.epoch)
            .unwrap()
            .unwrap();
        let active = DfsMetadataShardConfig {
            epoch: fixture.shard.epoch + 1,
            ..fixture.shard.clone()
        };
        backend
            .mark_metadata_shard_membership_handoff_activated(&shard, name, active.epoch)
            .unwrap();
        // Local rebind contract test only: the production caller obtains this
        // exact completion from its fresh native configuration read.
        let proof = DfsMetadataShardMembershipHandoffProof {
            cause: DfsMetadataShardHandoffCause::StorageImport {
                cut: DfsMetadataStorageImportCut {
                    source_cut_digest: name.into(),
                    source_journal_sequence: freeze.journal_sequence,
                    source_state_fingerprint: freeze.state_fingerprint.clone(),
                    target_prefix: identity.prefix_proof.clone(),
                    converted_image_fingerprint: identity.state_fingerprint.clone(),
                    pending_mapping_digest: name.into(),
                },
            },
            handoff_id: name.into(),
            retiring_shard: fixture.shard.clone(),
            desired_shard: active.clone(),
            journal_sequence: freeze.journal_sequence,
            state_fingerprint: identity.state_fingerprint.clone(),
            acknowledged_node_ids: vec![nodes[0].clone()],
        };
        assert_ne!(proof.state_fingerprint, freeze.state_fingerprint);
        for invalidity in ["prefix", "image", "ack", "membership"] {
            let mut changed = proof.clone();
            match invalidity {
                "prefix" => {
                    if let DfsMetadataShardHandoffCause::StorageImport { cut } = &mut changed.cause
                    {
                        cut.target_prefix.digest.push('0');
                    }
                }
                "image" => changed.state_fingerprint.push('0'),
                "ack" => changed.acknowledged_node_ids.clear(),
                "membership" => changed.cause = DfsMetadataShardHandoffCause::Membership,
                _ => unreachable!(),
            }
            assert!(
                !rebind_completed_metadata_shard_membership_handoff(
                    &mut backend,
                    &nodes[0],
                    &active,
                    None,
                    Some(&changed),
                )
                .unwrap(),
                "{invalidity}"
            );
        }
        assert!(
            !rebind_completed_metadata_shard_membership_handoff(
                &mut backend,
                &nodes[0],
                &active,
                None,
                None,
            )
            .unwrap()
        );
        assert!(
            backend
                .metadata_shard_instance_writes_frozen(&shard)
                .unwrap()
        );
        assert!(
            rebind_completed_metadata_shard_membership_handoff(
                &mut backend,
                &nodes[0],
                &active,
                None,
                Some(&proof),
            )
            .unwrap()
        );
        assert!(
            !backend
                .metadata_shard_instance_writes_frozen(&shard)
                .unwrap()
        );
        drop(backend);
        let mut cold = LocalSobjMetadataBackend::new(shard.clone(), dir.path()).unwrap();
        assert!(cold.metadata_shard_instance_writes_frozen(&shard).unwrap());
        assert!(
            rebind_completed_metadata_shard_membership_handoff(
                &mut cold,
                &nodes[0],
                &active,
                None,
                Some(&proof),
            )
            .unwrap()
        );
        assert_eq!(
            cold.metadata_shard_write_freeze(&shard)
                .unwrap()
                .unwrap()
                .rebind_prefix_proof,
            identity.prefix_proof
        );
        drop(cold);
        drop(_entered);
        fixture.finish();
    }

    #[test]
    fn metadata_quorum_size_counts_primary_and_configured_replicas() {
        assert_eq!(dfs_metadata_replica_quorum_size(0), 1);
        assert_eq!(dfs_metadata_replica_quorum_size(1), 2);
        assert_eq!(dfs_metadata_replica_quorum_size(2), 2);
        assert_eq!(dfs_metadata_replica_quorum_size(3), 3);
    }

    #[test]
    fn retiring_primary_freeze_rejects_new_prepare_without_wedging_activation() {
        let name = "native-retiring-freeze-current-prepare";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut fixture = configured_normal_sync_fixture(name, nodes[1..].to_vec());
        let desired = DfsMetadataShardConfig {
            primary_node_id: nodes[1].clone(),
            replica_node_ids: crate::control_plane::topology_preserving_metadata_shard_replicas(
                &fixture.shard,
                &nodes[1],
            )
            .expect("promoted primary remains in the actual configured ring"),
            epoch: fixture.shard.epoch + 1,
            ..fixture.shard.clone()
        };
        let control = fixture.runtime.shared_control_plane().unwrap();
        let handoff = control.with_client(|client| {
            DfsControlPlaneStore::new(client.as_mut())
                .begin_metadata_shard_membership_handoff(&fixture.shard, &desired)
                .unwrap()
                .unwrap()
        });
        let shard_id = DfsMetadataShardId::new(&fixture.shard.shard_id);
        let freeze = fixture
            .service
            .metadata_backend
            .as_deref_mut()
            .unwrap()
            .freeze_metadata_shard_for_membership_handoff(
                &shard_id,
                &handoff.handoff_id,
                fixture.shard.epoch,
            )
            .unwrap()
            .unwrap();
        let retiring_fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: fixture.shard.primary_node_id.clone(),
            journal_sequence: freeze.journal_sequence,
            state_fingerprint: freeze.state_fingerprint.clone(),
        };
        control.with_client(|client| {
            DfsControlPlaneStore::new(client.as_mut())
                .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &retiring_fence)
                .unwrap();
        });
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                operation_id: "native:must-not-append-after-fence".into(),
                file_system_id: fixture.shard.file_system_id.clone(),
                shard_id: shard_id.clone(),
                owner_epoch: fixture.shard.epoch,
            },
            Vec::new(),
            Vec::new(),
            vec![crate::DfsPreparedMetadataEffect::PutFileSystem {
                file_system: fixture.runtime.file_system(),
            }],
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let rejected = fixture
            .service
            .metadata_backend
            .as_deref_mut()
            .unwrap()
            .prepare_operation(payload, None);
        assert!(matches!(
            rejected,
            Err(crate::MetadataBackendError::MembershipHandoffFrozen { .. })
        ));
        assert_eq!(
            fixture
                .service
                .metadata_backend
                .as_deref()
                .unwrap()
                .journal_last_sequence()
                .unwrap(),
            freeze.journal_sequence
        );
        let deadline = Instant::now() + crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
        let reconciled = fixture
            .service
            .reconcile_pending_metadata_shard_membership_handoffs(
                &[fixture.shard.clone()],
                deadline.saturating_duration_since(Instant::now()),
            );
        if let Err(error) = reconciled {
            // The same normal background owner can finish this handoff first.
            // A stale retiring caller is acceptable only with the exact
            // completed assignment, original proof and durable marker below.
            match error {
                DfsError::StaleOwnerEpoch {
                    file_system_id,
                    local_node_id,
                    primary_node_id,
                    epoch,
                } if file_system_id == desired.file_system_id
                    && local_node_id == fixture.shard.primary_node_id
                    && primary_node_id == desired.primary_node_id
                    && epoch == desired.epoch => {}
                other => panic!("original handoff reconciliation failed: {other}"),
            }
        }
        let keys = [
            crate::control_plane::metadata_shard_key(&desired.file_system_id, &desired.shard_id),
            crate::control_plane::metadata_shard_membership_handoff_key(
                &desired.file_system_id,
                &desired.shard_id,
            ),
            crate::control_plane::metadata_shard_membership_handoff_proof_key(
                &desired.file_system_id,
                &desired.shard_id,
            ),
        ];
        let (actual, pending, proof) = control.with_client(|client| {
            // All three records come from one actual bounded native read cut,
            // within the same original reconciliation window.
            let entries = client
                .get_authoritative_consistent_before(&keys, deadline)
                .unwrap();
            assert_eq!(entries.len(), keys.len());
            for (entry, key) in entries.iter().zip(&keys) {
                if let Some(entry) = entry {
                    assert_eq!(&entry.key, key);
                }
            }
            (
                entries[0]
                    .as_ref()
                    .expect("actual activated assignment")
                    .decode::<DfsMetadataShardConfig>()
                    .unwrap(),
                entries[1]
                    .as_ref()
                    .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>().unwrap()),
                entries[2]
                    .as_ref()
                    .expect("actual original completion proof")
                    .decode::<DfsMetadataShardMembershipHandoffProof>()
                    .unwrap(),
            )
        });
        assert!(pending.is_none());
        assert_eq!(proof.handoff_id, handoff.handoff_id);
        assert_eq!(proof.cause, handoff.cause);
        assert_eq!(proof.retiring_shard, fixture.shard);
        assert_eq!(proof.desired_shard, desired);
        assert_eq!(proof.journal_sequence, freeze.journal_sequence);
        assert_eq!(proof.state_fingerprint, freeze.state_fingerprint);
        assert!(
            crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
                &proof.retiring_shard,
                &proof.desired_shard,
                &proof.acknowledged_node_ids.iter().cloned().collect(),
            )
        );
        let activated = fixture
            .service
            .metadata_backend
            .as_deref()
            .unwrap()
            .metadata_shard_write_freeze(&shard_id)
            .unwrap()
            .expect("actual retiring backend keeps its durable fence");
        assert_eq!(activated.handoff_id, freeze.handoff_id);
        assert_eq!(activated.retiring_epoch, freeze.retiring_epoch);
        assert_eq!(activated.activated_epoch, Some(desired.epoch));
        assert_eq!(activated.journal_sequence, freeze.journal_sequence);
        assert_eq!(activated.state_fingerprint, freeze.state_fingerprint);
        assert_eq!(
            actual, desired,
            "rejected hidden prepare cannot wedge genuine joint activation"
        );
        fixture.finish();
    }

    #[test]
    fn supplied_checkpoint_snapshot_rejects_invalid_frozen_prefix_before_send() {
        for invalidity in ["sequence", "fingerprint"] {
            let name = format!("native-supplied-checkpoint-{invalidity}");
            let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(&name, 2);
            let replica = nodes[1].clone();
            let mut fixture = configured_normal_sync_fixture(&name, vec![replica.clone()]);
            let mut target = fixture
                .service
                .capture_metadata_quorum_sync_target(&fixture.object)
                .expect("capture supplied checkpoint target");
            let snapshot = metadata_shard_snapshot_at(
                fixture
                    .service
                    .metadata_backend
                    .as_deref()
                    .expect("metadata backend"),
                &target.shard_id,
                target.journal_sequence,
                &target.state_fingerprint,
            )
            .expect("build valid supplied checkpoint");
            match invalidity {
                "sequence" => {
                    target.journal_sequence = target.journal_sequence.saturating_add(1);
                }
                "fingerprint" => {
                    let last = target
                        .state_fingerprint
                        .pop()
                        .expect("nonempty prefix digest");
                    target
                        .state_fingerprint
                        .push(if last == '0' { '1' } else { '0' });
                }
                _ => unreachable!(),
            }
            target.checkpoint_snapshot = Some(snapshot);
            let before_calls = fixture
                .runtime
                .rpc_observations()
                .iter()
                .filter(|observation| {
                    observation.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                        && observation.source == fixture.shard.primary_node_id
                        && observation.target == replica
                })
                .count();
            let rejected = BTreeSet::from([replica.clone()]);
            let mut attempts = BTreeMap::new();
            reset_metadata_sync_snapshot_build_calls();

            let error = fixture
                .service
                .send_metadata_sync_checkpoint_snapshots(
                    &target,
                    "invalid-supplied-checkpoint",
                    &rejected,
                    &BTreeSet::new(),
                    &mut attempts,
                    &MetadataQuorumSyncPurpose::OrdinaryDurability,
                )
                .expect_err("invalid supplied checkpoint must fail before send");
            let expected_message = format!(
                "metadata shard {} repair snapshot does not match frozen prefix {}",
                target.shard_id.0, target.state_fingerprint
            );
            assert!(
                matches!(error, DfsError::Metadata { message } if message == expected_message),
                "invalid supplied {invalidity} must retain the metadata error"
            );
            assert_eq!(metadata_sync_snapshot_build_calls(), 0);
            assert_eq!(
                fixture
                    .runtime
                    .rpc_observations()
                    .iter()
                    .filter(|observation| {
                        observation.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                            && observation.source == fixture.shard.primary_node_id
                            && observation.target == replica
                    })
                    .count(),
                before_calls,
                "invalid supplied snapshot cannot dispatch a peer call"
            );
            assert!(attempts.is_empty());
            assert!(
                fixture
                    .service
                    .replication
                    .pending_metadata_replications
                    .is_empty()
            );
            assert_eq!(
                fixture.service.transport.pending_incoming_count().unwrap(),
                Some(0)
            );
            fixture.finish();
        }
    }

    #[test]
    fn native_full_shard_sync_uses_real_receivers_without_raft_write_fallback() {
        let name = "native-full-shard-sync";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut fixture = configured_normal_sync_fixture(name, nodes[1..].to_vec());
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .unwrap();
        let before = fixture.control_plane_probe.txn_calls();
        let progress = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::from_secs(5))
            .unwrap();
        let expected = target
            .replica_node_ids
            .iter()
            .cloned()
            .chain([fixture.shard.primary_node_id.clone()])
            .collect::<BTreeSet<_>>();
        assert_eq!(
            progress
                .acknowledged_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert!(progress.acknowledged_node_ids.len() >= progress.quorum_size);
        assert_eq!(fixture.control_plane_probe.txn_calls(), before);
        assert!(
            fixture
                .runtime
                .rpc_observations()
                .iter()
                .any(|call| target.replica_node_ids.contains(&call.target)
                    && call.phase == crate::test_runtime::RpcPhase::Completed)
        );
        fixture.finish();
    }

    #[test]
    fn native_cached_sender_cursor_cannot_replace_a_fresh_receiver_ack() {
        let name = "native-sync-cursor-not-proof";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut fixture = configured_normal_sync_fixture(name, nodes[1..].to_vec());
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .unwrap();
        fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::from_secs(5))
            .unwrap();
        fixture.runtime.stop_node(&nodes[2]).unwrap();
        // Obtain a new real attempt identity while retaining the exact prefix
        // already covered by the sender's persisted scheduling cursor.
        let partial = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .unwrap();
        assert_ne!(partial.barrier_id, target.barrier_id);
        assert_eq!(partial.expected_shard, target.expected_shard);
        assert_eq!(partial.replica_node_ids, target.replica_node_ids);
        assert_eq!(partial.journal_sequence, target.journal_sequence);
        assert_eq!(partial.state_fingerprint, target.state_fingerprint);
        let source_trace = crate::test_support::capture_native_shard_trace(
            &fixture.runtime.data_dir(&nodes[0]).root(),
            partial.shard_id.clone(),
        )
        .unwrap();
        let receiver_trace = crate::test_support::capture_native_shard_trace(
            &fixture.runtime.data_dir(&nodes[1]).root(),
            partial.shard_id.clone(),
        )
        .unwrap();
        let before = fixture.control_plane_probe.txn_calls();
        assert_eq!(fixture.runtime.observation_dropped_count(), 0);
        let observation_cursor = fixture.runtime.rpc_observations().len();
        let partial_started = Instant::now();
        let error = fixture
            .service
            .sync_metadata_shard_to_quorum(&partial, Duration::from_millis(500))
            .expect_err("a remaining quorum cannot replace a full assigned-participant proof");
        let partial_elapsed = partial_started.elapsed();
        assert!(
            partial_elapsed >= DFS_POSIX_FULL_SHARD_ACK_GRACE,
            "the actual partial attempt crosses the unchanged fallback grace"
        );
        source_trace.close();
        receiver_trace.close();
        assert_eq!(fixture.control_plane_probe.txn_calls(), before);
        assert_eq!(fixture.runtime.observation_dropped_count(), 0);
        let observations = fixture.runtime.rpc_observations();
        let window = &observations[observation_cursor..];
        assert!(
            window.iter().any(|completed| {
                completed.source == nodes[0]
                    && completed.target == nodes[1]
                    && completed.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    && completed.phase == crate::test_runtime::RpcPhase::Completed
                    && window.iter().any(|began| {
                        began.call_id == completed.call_id
                            && began.source == completed.source
                            && began.target == completed.target
                            && began.route == completed.route
                            && matches!(
                                began.phase,
                                crate::test_runtime::RpcPhase::Constructed
                                    | crate::test_runtime::RpcPhase::BeforeDispatch
                            )
                    })
            }),
            "the partial attempt completes a newly constructed actual metadata peer call"
        );
        assert!(matches!(
            error,
            DfsError::Network { .. } | DfsError::NetworkDelivery { .. }
        ));
        assert!(
            fixture
                .service
                .replication
                .pending_metadata_replications
                .is_empty(),
            "failed barrier releases only its own pending probe slots"
        );
        fixture.runtime.reopen_node(nodes[2].clone()).unwrap();
        let healed = fixture
            .service
            .sync_metadata_shard_to_quorum(&partial, Duration::from_secs(5))
            .unwrap();
        assert_eq!(healed.acknowledged_node_ids.len(), 3);
        for node in &nodes {
            assert!(healed.acknowledged_node_ids.contains(node));
        }
        assert_eq!(fixture.control_plane_probe.txn_calls(), before);
        assert_eq!(fixture.runtime.observation_dropped_count(), 0);
        let expected_shard = fixture.shard.clone();
        fixture.finish();
        // Export after ordinary owner drain. Closing observation admission
        // alone does not prove that any already-owned native attempt settled.
        let mut source_bytes = Vec::new();
        let mut receiver_bytes = Vec::new();
        source_trace.export_raw(&mut source_bytes).unwrap();
        receiver_trace.export_raw(&mut receiver_bytes).unwrap();
        let source: serde_json::Value = serde_json::from_slice(&source_bytes).unwrap();
        let receiver: serde_json::Value = serde_json::from_slice(&receiver_bytes).unwrap();
        for capture in [&source, &receiver] {
            assert_eq!(capture["closed"], true);
            assert_eq!(capture["actual_owner_stopped"], true);
            for counter in ["dropped", "incomplete", "in_flight"] {
                assert_eq!(capture[counter], "0");
            }
        }
        let source_facts = source["boundary_packets"].as_array().unwrap();
        let receiver_facts = receiver["boundary_packets"].as_array().unwrap();
        let request_prefix = format!("dfs_sync_probe_{}-", partial.barrier_id);
        let consumed = source_facts
            .iter()
            .map(|row| &row["facts"])
            .find(|fact| {
                fact["phase"] == "source-journal-response-accepted"
                    && fact["authenticated_source_node_id"] == nodes[1].as_str()
                    && fact["expected_target_node_id"] == nodes[1].as_str()
                    && fact["response"]["request_id"]
                        .as_str()
                        .is_some_and(|id| id.starts_with(&request_prefix))
                    && fact["response"]["shard_id"] == partial.shard_id.0
                    && fact["response"]["next_after_sequence"].as_u64()
                        == Some(partial.journal_sequence)
                    && fact["response"]["verified_prefix_only"] == true
                    && fact["response"]["result"] == "applied"
            })
            .expect("this invocation consumed the surviving peer's actual accepted prefix ACK");
        let start = source_facts
            .iter()
            .map(|row| &row["facts"])
            .find(|fact| {
                fact["phase"] == "direct-journal-rpc-attempt-started"
                    && fact["target_node_id"] == nodes[1].as_str()
                    && fact["route"] == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    && fact["command"]["request_id"] == consumed["response"]["request_id"]
            })
            .expect("this invocation actually polled a fresh native prefix probe");
        let command: DfsMetadataJournalBatchCommand =
            serde_json::from_value(start["command"].clone()).unwrap();
        assert_eq!(command.source_node_id.as_ref(), Some(&nodes[0]));
        assert_eq!(
            command.file_system_id.as_ref(),
            Some(&partial.file_system_id)
        );
        assert_eq!(command.authority_epoch, partial.authority_epoch);
        assert!(command.verify_prefix_only);
        assert!(command.batch.journal_frames.is_empty());
        assert_eq!(command.batch.shard_id, partial.shard_id);
        assert_eq!(command.batch.next_after_sequence, partial.journal_sequence);
        let probe = command.prefix_probe.as_ref().unwrap();
        assert!(
            probe
                .barrier_id
                .starts_with(&format!("{}-", partial.barrier_id))
        );
        assert_eq!(probe.file_system_id, command.file_system_id);
        assert_eq!(probe.authority_epoch, partial.authority_epoch);
        assert_eq!(probe.shard_id, partial.shard_id);
        assert_eq!(probe.journal_sequence, partial.journal_sequence);
        assert_eq!(probe.state_fingerprint, partial.state_fingerprint);
        assert!(probe.primary_handoff_id.is_none());
        let returned = source_facts
            .iter()
            .map(|row| &row["facts"])
            .find(|fact| {
                fact["phase"] == "direct-journal-rpc-attempt-returned"
                    && fact["attempt_id"] == start["attempt_id"]
            })
            .expect("the SAME native attempt returned an actual response");
        assert!(returned["error_detail"].is_null());
        assert!(returned["delivery_state"].is_null());
        let response: DfsMetadataJournalBatchResponse =
            serde_json::from_value(returned["response"].clone()).unwrap();
        assert_eq!(response.request_id, command.request_id);
        assert_eq!(response.shard_id, partial.shard_id);
        assert_eq!(response.next_after_sequence, partial.journal_sequence);
        assert_eq!(response.applied_entries, 0);
        assert!(response.verified_prefix_only);
        assert!(response.verified_authority_probe_id.is_none());
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
        let accepted = source_facts
            .iter()
            .map(|row| &row["facts"])
            .find(|fact| {
                fact["phase"] == "source-journal-response-accepted"
                    && fact["authenticated_source_node_id"] == nodes[1].as_str()
                    && fact["expected_target_node_id"] == nodes[1].as_str()
                    && fact["response"] == returned["response"]
            })
            .expect("the actual sync owner consumed the survivor's SAME accepted prefix ACK");
        assert_eq!(
            accepted["expected_next_after_sequence"].as_u64(),
            Some(partial.journal_sequence)
        );
        assert_eq!(accepted["expected_prefix_only"], true);
        assert!(accepted["expected_confirmation_id"].is_null());
        let received = receiver_facts
            .iter()
            .map(|row| &row["facts"])
            .find(|fact| {
                fact["phase"] == "replica-prefix-probe-accepted"
                    && fact["local_node_id"] == nodes[1].as_str()
                    && fact["command"] == start["command"]
                    && fact["response"] == returned["response"]
            })
            .expect("the actual surviving receiver verified this invocation's exact full prefix");
        assert_eq!(
            serde_json::from_value::<DfsMetadataShardConfig>(
                received["actual_source_config"].clone()
            )
            .unwrap(),
            expected_shard
        );
        assert!(received["actual_pending_handoff"].is_null());
    }

    #[test]
    fn native_primary_rotation_preserves_joint_proof_fences_and_restart_visibility() {
        let name = "native-sync-primary-handoff";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut fixture = configured_normal_sync_fixture(name, nodes[1..].to_vec());
        fixture
            .service
            .posix_mkdir("/native-before-primary-handoff")
            .unwrap();
        let stat = fixture
            .service
            .posix_stat("/native-before-primary-handoff")
            .unwrap();
        fixture
            .runtime
            .reassign_metadata_shard(
                &fixture.shard.file_system_id,
                "root",
                nodes[1].clone(),
                vec![nodes[0].clone(), nodes[2].clone()],
            )
            .unwrap();
        let control = fixture.runtime.shared_control_plane().unwrap();
        let (active, proof, pending) = control.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv.as_mut());
            (
                store
                    .get_metadata_shard(&fixture.shard.file_system_id, "root")
                    .unwrap()
                    .unwrap(),
                store
                    .get_metadata_shard_membership_handoff_proof(
                        &fixture.shard.file_system_id,
                        "root",
                    )
                    .unwrap()
                    .unwrap(),
                store
                    .get_metadata_shard_membership_handoff(&fixture.shard.file_system_id, "root")
                    .unwrap(),
            )
        });
        assert_eq!(active.primary_node_id, nodes[1]);
        assert_eq!(proof.retiring_shard, fixture.shard);
        assert_eq!(proof.desired_shard, active);
        assert!(proof.acknowledged_node_ids.contains(&nodes[0]));
        assert!(proof.acknowledged_node_ids.contains(&nodes[1]));
        assert!(pending.is_none());
        let shard_id = DfsMetadataShardId::new("root");
        assert!(
            fixture
                .service
                .metadata_backend()
                .unwrap()
                .metadata_shard_authoritative_writes_frozen(&shard_id)
                .unwrap(),
            "the retiring writer stays fenced after activation"
        );
        let mut promoted = fixture.runtime.client(&nodes[1]).unwrap();
        assert_eq!(
            promoted
                .posix_stat("/native-before-primary-handoff")
                .unwrap()
                .object_id,
            stat.object_id
        );
        promoted
            .posix_mkdir("/native-after-primary-handoff")
            .unwrap();
        drop(promoted);
        drop(fixture.service);
        fixture.runtime.stop_node(&nodes[0]).unwrap();
        fixture.runtime.reopen_node(nodes[0].clone()).unwrap();
        fixture.service = fixture.runtime.client(&nodes[0]).unwrap();
        let retired_marker = fixture
            .service
            .metadata_backend()
            .unwrap()
            .metadata_shard_write_freeze(&shard_id)
            .unwrap()
            .expect("retired replica retains its actual ingest fence");
        assert_eq!(retired_marker.handoff_id, proof.handoff_id);
        assert_eq!(retired_marker.retiring_epoch, fixture.shard.epoch);
        assert_eq!(retired_marker.activated_epoch, Some(active.epoch));
        assert_eq!(fixture.runtime.root_shard().unwrap(), active);
        assert_ne!(active.primary_node_id, *fixture.service.node_id());
        // A reopened replica's node-scoped author projection is not a frozen
        // primary. Its actual marker and current assignment still deny that role.
        assert!(
            matches!(fixture.service.capture_metadata_quorum_sync_target(&fixture.object),
            Err(DfsError::StaleOwnerEpoch { primary_node_id, epoch, .. })
            if primary_node_id == active.primary_node_id && epoch == active.epoch)
        );
        assert_eq!(
            fixture
                .service
                .posix_stat("/native-before-primary-handoff")
                .unwrap()
                .object_id,
            stat.object_id
        );
        assert!(
            fixture
                .service
                .posix_stat_distributed("/native-after-primary-handoff")
                .is_ok()
        );
        fixture.finish();
    }

    #[test]
    fn native_successive_handoffs_reconcile_the_previous_retiring_marker() {
        let name = "native-sync-successive-handoff";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut fixture = configured_normal_sync_fixture(name, nodes[1..].to_vec());
        fixture
            .runtime
            .reassign_metadata_shard(
                &fixture.shard.file_system_id,
                "root",
                nodes[1].clone(),
                vec![nodes[0].clone(), nodes[2].clone()],
            )
            .unwrap();
        fixture
            .runtime
            .reassign_metadata_shard(
                &fixture.shard.file_system_id,
                "root",
                nodes[0].clone(),
                vec![nodes[1].clone(), nodes[2].clone()],
            )
            .unwrap();
        drop(fixture.service);
        fixture.service = fixture.runtime.client(&nodes[0]).unwrap();
        let active = fixture.runtime.root_shard().unwrap();
        assert_eq!(active.primary_node_id, nodes[0]);
        assert_eq!(active.epoch, fixture.shard.epoch + 2);
        fixture
            .service
            .posix_mkdir("/native-after-two-handoffs")
            .unwrap();
        fixture.runtime.stop_node(&nodes[1]).unwrap();
        fixture.runtime.reopen_node(nodes[1].clone()).unwrap();
        let mut replica = fixture.runtime.client(&nodes[1]).unwrap();
        assert!(replica.posix_stat("/native-after-two-handoffs").is_ok());
        drop(replica);
        fixture.finish();
    }

    #[test]
    fn native_target_epoch_change_rejects_a_stale_sync_scope() {
        let name = "native-sync-stale-epoch";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
        let mut fixture = configured_normal_sync_fixture(name, nodes[1..].to_vec());
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .unwrap();
        fixture
            .runtime
            .reassign_metadata_shard(
                &fixture.shard.file_system_id,
                "root",
                nodes[1].clone(),
                vec![nodes[0].clone()],
            )
            .unwrap();
        let active = fixture.runtime.root_shard().unwrap();
        assert_eq!(active.primary_node_id, nodes[1]);
        assert_eq!(active.epoch, fixture.shard.epoch + 1);
        let marker = fixture
            .service
            .metadata_backend()
            .unwrap()
            .metadata_shard_write_freeze(&target.shard_id)
            .unwrap()
            .unwrap();
        assert_ne!(marker.activated_epoch, target.authority_epoch);
        let before = fixture
            .service
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&target.shard_id)
            .unwrap();
        let outcome = fixture
            .service
            .sync_metadata_shard_to_quorum(&target, Duration::from_millis(100));
        assert!(
            matches!(&outcome, Err(DfsError::Metadata { message })
            if message == "ordinary metadata sync backend is fenced by another authority generation"),
            "the actual native freeze must reject the original stale target: {outcome:?}"
        );
        assert_eq!(
            fixture
                .service
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&target.shard_id)
                .unwrap(),
            before
        );
        fixture.finish();
    }

    #[test]
    fn native_sync_rejects_uncorrelated_or_sequence_only_ack_events() {
        let name = "native-sync-ack-correlation";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut fixture = configured_normal_sync_fixture(name, nodes[1..].to_vec());
        let target = fixture
            .service
            .capture_metadata_quorum_sync_target(&fixture.object)
            .unwrap();
        // Healthy acknowledgements above come from actual hosted receivers.
        // These are explicitly malformed event inputs to the same correlation
        // validator; none may create positive authority or model evidence.
        for invalid in ["request", "sender", "shard", "sequence", "prefix-flag"] {
            let mut request = "expected-native-probe".to_string();
            let mut source = nodes[1].clone();
            let mut shard = target.shard_id.clone();
            let mut sequence = target.journal_sequence;
            let mut prefix = true;
            match invalid {
                "request" => request = "another-probe".into(),
                "sender" => source = nodes[2].clone(),
                "shard" => shard = DfsMetadataShardId::new("another-shard"),
                "sequence" => sequence += 1,
                "prefix-flag" => prefix = false,
                _ => unreachable!(),
            }
            let event = NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                request_id: request,
                source_node_id: source,
                shard_id: shard,
                applied_entries: 0,
                next_after_sequence: sequence,
                verified_prefix_only: prefix,
            };
            let mut probes =
                BTreeMap::from([("expected-native-probe".to_string(), nodes[1].clone())]);
            let mut catchup = BTreeMap::new();
            let mut acknowledged = BTreeSet::from([nodes[0].clone()]);
            record_metadata_sync_prefix_probe_events(
                vec![event],
                &target,
                &mut probes,
                &mut catchup,
                &mut acknowledged,
            );
            assert_eq!(
                acknowledged,
                BTreeSet::from([nodes[0].clone()]),
                "malformed {invalid} cannot acknowledge another target"
            );
        }
        fixture.finish();
    }
}
