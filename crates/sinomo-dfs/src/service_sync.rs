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

#[cfg(test)]
mod recovery_fence;
#[cfg(test)]
pub(crate) use recovery_fence::DfsSelectedLocalRecoveryFenceResult;
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
    /// Fixed source prefix, every assigned participant, no snapshot repair.
    /// Its result is live barrier DATA, not a serving or recovery capability.
    ExactPrefixFullAssignment,
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
    repair_custody: Option<crate::DfsRepairHandoffPage>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    first_batch_error: Option<MetadataSyncBatchError>,
}

#[derive(Serialize)]
struct MetadataSyncBatchError {
    request_id: String,
    sequence: u64,
    detail: String,
    detail_truncated: bool,
    delivery_state: Option<String>,
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

    fn first_batch_error(&mut self, node: &NodeId, phase: &'static str,
        request_id: &str, sequence: u64, error: &sinomo_core::network::NetworkError) {
        let Some(last) = self.peers.get_mut(node).and_then(|phases| phases.get_mut(phase)) else { return; };
        if last.first_batch_error.is_some() { return; }
        let detail = error.to_string();
        // This is the batch return, not proof each member reached the wire.
        // Retain its own original request/sequence alongside the first error.
        last.first_batch_error = Some(MetadataSyncBatchError {
            request_id: request_id.to_owned(), sequence,
            detail: detail.chars().take(2048).collect(),
            detail_truncated: detail.chars().count() > 2048,
            delivery_state: error.delivery_state().map(|state| format!("{state:?}")),
        });
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
    fn primary_handoff_for(&self, node_id: &NodeId) -> Option<&MetadataPrimaryHandoffProbe> {
        match self {
            Self::MembershipHandoff {
                primary_handoff: Some(probe),
                ..
            } if &probe.promoted_node_id == node_id => Some(probe),
            _ => None,
        }
    }
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
    static BEFORE_HANDOFF_EFFECT_ADMISSION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
static AFTER_SELECTED_HANDOFF_EXPORT: std::sync::Mutex<Option<std::sync::Arc<SelectedSourceExportPause>>> = std::sync::Mutex::new(None);

#[cfg(test)]
static BEFORE_FIRST_SELECTED_CAPTURE: std::sync::Mutex<Option<std::sync::Arc<SelectedPreCaptureStop>>> = std::sync::Mutex::new(None);

#[cfg(test)]
#[derive(Debug)]
struct SelectedPreCaptureObservation {
    root: std::path::PathBuf,
    handoff: DfsMetadataShardMembershipHandoff,
    freeze: crate::DfsMetadataShardWriteFreeze,
    queue_sequence: u64,
    birth: crate::repair_queue_store::RepairOriginalBirth,
}

#[cfg(test)]
struct SelectedPreCaptureStop {
    layout: std::sync::Arc<crate::DfsSelectedOriginalDataLayout>,
    root: std::path::PathBuf,
    handoff_id: String,
    request_id: String,
    observed: std::sync::mpsc::SyncSender<SelectedPreCaptureObservation>,
    matched_invocations: std::sync::atomic::AtomicUsize,
}

/// A negative cut, never a successful handoff or a blocking owner lifetime.
/// No Drop removal: failure cleanup must keep stopping matched owners until
/// the actual old DATA owner is gone, rather than unpausing them during close.
#[cfg(test)]
struct SelectedPreCaptureRegistration(std::sync::Arc<SelectedPreCaptureStop>);

#[cfg(test)]
impl SelectedPreCaptureRegistration {
    fn install(stop: SelectedPreCaptureStop) -> Self {
        let mut registered = BEFORE_FIRST_SELECTED_CAPTURE.lock().unwrap();
        assert!(registered.is_none(), "an existing pre-capture fault cannot be overwritten");
        let stop = std::sync::Arc::new(stop);
        *registered = Some(std::sync::Arc::clone(&stop));
        Self(stop)
    }

    fn remove_after_owner_stop(&self, owner: &std::sync::Weak<crate::runtime_host::DfsDataOwnerLock>) {
        assert!(owner.upgrade().is_none(), "pre-capture fault stays active until the old native owner is gone");
        let mut registered = BEFORE_FIRST_SELECTED_CAPTURE.lock().unwrap();
        assert!(registered.as_ref().is_some_and(|current| std::sync::Arc::ptr_eq(current, &self.0)),
            "only this exact pre-capture registration can be removed");
        registered.take();
    }
}

#[cfg(test)]
fn stop_selected_before_capture_for_test(
    root: &Path,
    layout: &std::sync::Arc<crate::DfsSelectedOriginalDataLayout>,
    backend: &dyn DfsMetadataBackend,
    handoff: &DfsMetadataShardMembershipHandoff,
    request_id: &str,
    queue_sequence: u64,
    birth: &crate::repair_queue_store::RepairOriginalBirth,
    deadline: Instant,
) -> Result<(), DfsError> {
    let stop = {
        let registered = BEFORE_FIRST_SELECTED_CAPTURE.lock().unwrap();
        registered.as_ref().filter(|stop| stop.root == root
            && stop.handoff_id == handoff.handoff_id && stop.request_id == request_id
            && std::sync::Arc::ptr_eq(&stop.layout, layout)).cloned()
    };
    let Some(stop) = stop else { return Ok(()); };
    let invocation = stop.matched_invocations.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    let invalid = |message: &str| DfsError::Metadata { message: message.into() };
    layout.check()?;
    let shard = DfsMetadataShardId::new(&handoff.retiring_shard.shard_id);
    let freeze = backend.metadata_shard_write_freeze(&shard)?
        .ok_or_else(|| invalid("pre-capture fault has no actual source freeze"))?;
    let fence = handoff.retiring_primary_fence.as_ref()
        .ok_or_else(|| invalid("pre-capture fault has no authenticated retiring fence"))?;
    let export = root.join("journal/repair-handoffs-v1")
        .join(format!("{}-source", blake3::hash(handoff.handoff_id.as_bytes()).to_hex()));
    match std::fs::symlink_metadata(&export) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(invalid("pre-capture fault cannot certify absence of its exact export directory")),
    }
    if freeze.handoff_id != handoff.handoff_id
        || freeze.retiring_epoch != handoff.retiring_shard.epoch
        || freeze.activated_epoch.is_some() || freeze.repair_custody.is_some()
        || freeze.journal_sequence != fence.journal_sequence
        || freeze.state_fingerprint != fence.state_fingerprint
        || birth.root.subject.request_id != request_id
        || birth.original.source_node_id != handoff.retiring_shard.primary_node_id
        || Instant::now() >= deadline
    {
        return Err(invalid("pre-capture fault differs from its exact original/freeze/deadline"));
    }
    if invocation == 0 {
        stop.observed.try_send(SelectedPreCaptureObservation {
            root: root.to_path_buf(), handoff: handoff.clone(), freeze,
            queue_sequence, birth: birth.clone(),
        }).map_err(|_| invalid("pre-capture observation owner unavailable"))?;
    }
    // Every matched invocation returns here while registered, including a
    // different native source owner. No page or success response is supplied.
    Err(DfsError::Cancelled { operation: "test stop after selected freeze before first capture" })
}

#[cfg(test)]
#[derive(Debug)]
struct SelectedSourceExportObservation {
    root: std::path::PathBuf,
    cut: crate::DfsRepairHandoffCut,
    page: crate::DfsRepairHandoffPage,
    freeze: crate::DfsMetadataShardWriteFreeze,
}

#[cfg(test)]
struct SelectedSourceExportPause {
    layout: std::sync::Arc<crate::DfsSelectedOriginalDataLayout>,
    root: std::path::PathBuf,
    handoff_id: String,
    deadline: Instant,
    observed: std::sync::mpsc::SyncSender<SelectedSourceExportObservation>,
    observation_sent: std::sync::atomic::AtomicBool,
    released: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
struct SelectedSourceExportRegistration(std::sync::Arc<SelectedSourceExportPause>);

#[cfg(test)]
impl SelectedSourceExportRegistration {
    fn install(pause: SelectedSourceExportPause) -> Self {
        let mut registered = AFTER_SELECTED_HANDOFF_EXPORT.lock().unwrap();
        assert!(registered.is_none(), "an existing source gate cannot be overwritten");
        let pause = std::sync::Arc::new(pause);
        *registered = Some(std::sync::Arc::clone(&pause));
        Self(pause)
    }

    fn release(&self) {
        *self.0.released.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}

#[cfg(test)]
impl Drop for SelectedSourceExportRegistration {
    fn drop(&mut self) {
        self.release();
        let mut registered = AFTER_SELECTED_HANDOFF_EXPORT.lock().unwrap();
        if registered.as_ref().is_some_and(|pause| std::sync::Arc::ptr_eq(pause, &self.0)) {
            registered.take();
        }
    }
}

/// Observe one genuine frozen source export before its first network attempt.
/// This test-only gate holds the existing source admission/drain lifetime; it
/// neither certifies another owner nor writes a marker, ACK or replacement cut.
#[cfg(test)]
fn pause_selected_source_export_for_test(
    root: &Path,
    layout: &std::sync::Arc<crate::DfsSelectedOriginalDataLayout>,
    backend: &dyn DfsMetadataBackend,
    export: &crate::repair_handoff::RepairHandoffExport,
    source_deadline: Instant,
) -> Result<(), DfsError> {
    let pause = {
        let registered = AFTER_SELECTED_HANDOFF_EXPORT.lock().unwrap();
        registered.as_ref().filter(|pause| export.cut.version == 2 && {
            pause.root == root && pause.handoff_id == export.cut.handoff.handoff_id
                && std::sync::Arc::ptr_eq(&pause.layout, layout)
        }).cloned()
    };
    let Some(pause) = pause else { return Ok(()); };
    let invalid = |message: &str| DfsError::Metadata { message: message.into() };
    let deadline = source_deadline.min(pause.deadline);
    layout.check()?;
    export.cut.validate_selected_original_data()?;
    let page = export.page(0)?;
    let shard = DfsMetadataShardId::new(&export.cut.handoff.retiring_shard.shard_id);
    let freeze = backend.metadata_shard_write_freeze(&shard)?
        .ok_or_else(|| invalid("selected source pause has no actual durable freeze"))?;
    let fence = export.cut.handoff.retiring_primary_fence.as_ref()
        .ok_or_else(|| invalid("selected source pause has no authenticated retiring fence"))?;
    if page.cut != export.cut || page.ordinal != 0
        || freeze.handoff_id != export.cut.handoff.handoff_id
        || freeze.retiring_epoch != export.cut.handoff.retiring_shard.epoch
        || freeze.activated_epoch.is_some()
        || freeze.journal_sequence != fence.journal_sequence
        || freeze.state_fingerprint != fence.state_fingerprint
        || freeze.repair_custody.as_ref().is_some_and(|cut| cut != &export.cut)
    {
        return Err(invalid("selected source pause differs from its exact frozen cut"));
    }
    // The source cut already binds the actual native fence. Do not introduce a
    // source marker mutation merely to observe a boundary before first send.
    if Instant::now() >= deadline {
        return Err(invalid("selected source pause reached its original handoff deadline before observation"));
    }
    if !pause.observation_sent.swap(true, std::sync::atomic::Ordering::AcqRel) {
        pause.observed.try_send(SelectedSourceExportObservation {
            root: root.to_path_buf(), cut: export.cut.clone(), page, freeze,
        }).map_err(|_| invalid("selected source pause observation owner unavailable"))?;
    }
    // Keep the registration until explicit release. A later actual owner that
    // reaches this exact cut after another attempt times out cannot bypass it.
    // Each invocation retains its own original native deadline.
    let mut released = pause.released.lock().unwrap();
    while !*released {
        let remaining = deadline.checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| invalid("selected source pause reached its original handoff deadline"))?;
        let (current, _) = pause.changed.wait_timeout(released, remaining).unwrap();
        released = current;
    }
    drop(released);
    if Instant::now() >= deadline {
        return Err(invalid("selected source pause released after its original handoff deadline"));
    }
    layout.check()?;
    Ok(())
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
            // The accepted transfer is itself the durable desired assignment.
            // Preserve it while freezing the complete prefix, including any
            // unfinished prepares. The successor's normal epoch recovery
            // re-certifies those exact bodies and retires their old reservations;
            // deleting this proposal would lose an explicit operator request.
            #[cfg(test)]
            if let Some((root, _)) = self.native_publication_owner_for_observation() {
                crate::default_cluster::bootstrap_observation::record(&root, &self.node_id, "handoff-executor-start", ||
                    serde_json::json!({"handoff":handoff,"timeout_millis":timeout.as_millis()}));
            }
            let outcome = self.reconcile_metadata_shard_membership_handoff(&handoff, timeout);
            #[cfg(test)]
            if let Some((root, _)) = self.native_publication_owner_for_observation() {
                crate::default_cluster::bootstrap_observation::record(&root, &self.node_id, "handoff-executor-result", ||
                    serde_json::json!({"handoff_id":handoff.handoff_id,"result":outcome.as_ref().map(|v|*v).map_err(|e|e.to_string())}));
            }
            if outcome? {
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
        // Reuse actual native admission so every prior producer has published
        // its queue checkpoint before capture. Never wait for the detached
        // carrier while holding admission its response handler may require.
        let rotates_primary =
            requested_retiring.primary_node_id != requested_handoff.desired_shard.primary_node_id;
        // Storage import already has its separately certified source/queue
        // conversion contract. Only ordinary role rotation uses these pages.
        let transfers_repair_custody = rotates_primary
            && matches!(
                requested_handoff.cause,
                crate::DfsMetadataShardHandoffCause::Membership
            );
        #[cfg(test)]
        if let Some(before) = BEFORE_HANDOFF_EFFECT_ADMISSION.with(|hook| hook.borrow_mut().take())
        {
            before();
        }
        let _execution = if transfers_repair_custody {
            match self.acquire_prepared_execution(&crate::DfsPreparedOperationScope {
                file_system_id: requested_retiring.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(&requested_retiring.shard_id),
                owner_epoch: requested_retiring.epoch,
                operation_id: requested_handoff.handoff_id.clone(),
            }) {
                Ok(execution) => execution,
                Err(error) => {
                    // Main may finish this exact proposal while a caller
                    // queues for effect admission. Reuse the existing fresh
                    // pending/assignment read solely to decline old work;
                    // this grants no effect or successful completion claim.
                    if let Some(control) = self.control_plane.as_mut()
                        && let Ok((active, pending)) = DfsControlPlaneStore::new(control.as_mut())
                            .retiring_metadata_shard_handoff_view_before(
                                &requested_retiring.file_system_id,
                                &requested_retiring.shard_id,
                                deadline,
                            )
                        && active.as_ref() == Some(&requested_handoff.desired_shard)
                        && pending.is_none()
                    {
                        return Ok(false);
                    }
                    return Err(error);
                }
            }
        } else {
            None
        };
        let drain_gate = if transfers_repair_custody {
            let backend = self
                .metadata_backend
                .as_deref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "repair handoff has no original physical metadata backend".into(),
                })?;
            let shard_id = DfsMetadataShardId::new(&requested_retiring.shard_id);
            let scope =
                backend.publication_owner_scope(&requested_retiring.file_system_id, &shard_id)?;
            let execution =
                self.publication_execution
                    .current()
                    .ok_or_else(|| DfsError::Metadata {
                        message: "repair handoff requires actual native mutation admission".into(),
                    })?;
            if !execution.admits(&scope, &requested_retiring.file_system_id, &shard_id) {
                return Err(DfsError::Metadata {
                    message: "repair handoff admission belongs to another physical scope".into(),
                });
            }
            let owner = crate::runtime_host::publication_runtime_owner(scope.root())
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?
                .ok_or_else(|| DfsError::Metadata {
                    message: "repair handoff lost its actual DATA owner".into(),
                })?;
            Some(
                crate::runtime_message::registered_repair_handoff_drain_gate(
                    &owner,
                    &self.node_id,
                )?,
            )
        } else {
            None
        };
        let _drain = match drain_gate.as_ref().map(|gate| gate.try_lock()) {
            Some(Ok(guard)) => Some(guard),
            Some(Err(std::sync::TryLockError::WouldBlock)) => return Ok(false),
            Some(Err(std::sync::TryLockError::Poisoned(_))) => {
                return Err(DfsError::Metadata {
                    message: "repair handoff carrier drain exclusion is poisoned".into(),
                });
            }
            None => None,
        };
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
        let export = if transfers_repair_custody {
            let store = self
                .owned_repair_queue_store()?
                .ok_or_else(|| DfsError::Metadata {
                    message: "primary handoff requires its original native repair queue owner"
                        .into(),
                })?;
            if drain_gate.is_none() {
                return Err(DfsError::Metadata {
                    message: "primary handoff has no native carrier drain owner".into(),
                });
            }
            let backend = self
                .metadata_backend
                .as_deref()
                .expect("handoff backend checked");
            let scope = backend.publication_owner_scope(&retiring.file_system_id, &shard_id)?;
            let belongs = |record: &crate::DfsRepairAttemptRecord| {
                Ok(backend.get_object_on_shard(&shard_id, &record.object_id)?
                    .is_some_and(|object| object.file_system_id == retiring.file_system_id))
            };
            Some(if let Some(layout) = backend.selected_original_data_layout(&retiring.file_system_id, &shard_id)? {
                let owner = crate::runtime_host::publication_runtime_owner(scope.root())
                    .map_err(|error| DfsError::Metadata { message: error.to_string() })?
                    .ok_or_else(|| DfsError::Metadata { message: "selected source lost its native DATA owner".into() })?;
                layout.check_owner(&owner)?;
                store.validate_selected_original_fixture(layout.queue_layout())?;
                if let Some(request) = layout.selected_request(&handoff)? {
                    let (_selected_sequence, birth) = store.selected_original_handoff_data(&self.node_id, &request)?;
                    let crate::DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
                        return Err(DfsError::Metadata { message: "selected source has no genuine Repair".into() });
                    };
                    if command.file_system_id != retiring.file_system_id
                        || backend.get_object_on_shard(&shard_id, &command.object_id)?
                            .is_none_or(|object| object.file_system_id != retiring.file_system_id)
                    {
                        return Err(DfsError::Metadata { message: "selected original is outside the exact frozen source shard".into() });
                    }
                    #[cfg(test)]
                    stop_selected_before_capture_for_test(scope.root(), &layout, backend, &handoff,
                        &request, _selected_sequence, &birth, deadline)?;
                }
                let export = crate::repair_handoff::RepairHandoffExport::capture_native_layout(
                    scope.root(), &layout, &handoff, belongs, deadline,
                )?;
                #[cfg(test)]
                pause_selected_source_export_for_test(scope.root(), &layout, backend, &export, deadline)?;
                export
            } else {
                crate::repair_handoff::RepairHandoffExport::capture(
                    scope.root(), &handoff, &store, belongs, deadline,
                )?
            })
        } else {
            None
        };
        timing.mark("handoff-prefix-proofs-started");
        let mut last_certificate = None;
        for ordinal in 0..export.as_ref().map_or(1, |export| export.cut.page_count) {
            let primary_handoff = if rotates_primary {
                Some(MetadataPrimaryHandoffProbe {
                    handoff_id: handoff.handoff_id.clone(),
                    promoted_node_id: handoff.desired_shard.primary_node_id.clone(),
                    repair_custody: export
                        .as_ref()
                        .map(|export| export.page(ordinal))
                        .transpose()?,
                })
            } else {
                None
            };
            last_certificate = Some(
                self.sync_metadata_shard_to_quorum_with_required_intersection(
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
                            message:
                                "metadata handoff authority read exhausted the caller deadline"
                                    .into(),
                        })?,
                )?,
            );
        }
        let certificate = last_certificate.ok_or_else(|| DfsError::Metadata {
            message: "handoff omitted its complete bounded custody cut".into(),
        })?;
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

    #[cfg(test)]
    pub(crate) fn retain_native_prefix_history(
        &mut self,
        round: &crate::node_transport::OwnedNativeMetadataPrefixRound,
        deadline: Instant,
    ) -> Result<crate::metadata_backend::native_prefix_history::NativePrefixHistoryIntake, DfsError>
    {
        self.check_cancelled("native prefix historical intake")?;
        if !round.is_complete()
            || Instant::now() >= deadline
            || self.source_adoption.has_retained_custody_for_prefix_history()
        {
            return Err(DfsError::Metadata {message:"historical intake requires a complete private round, live intake budget and no retained source writer".into()});
        }
        let assignment =
            round
                .target()
                .expected_shard
                .as_ref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "historical intake has no exact assignment".into(),
                })?;
        let provider = self
            .publication_provider
            .current()
            .ok_or_else(|| DfsError::Metadata {
                message: "historical intake has no actual native writer provider".into(),
            })?;
        // Admission precedes any backend borrow or writer. Reuse an installed
        // owner instead of waiting recursively for the same scheduler scope.
        let _restore = if self.publication_execution.current().is_none() {
            Some(
                self.publication_execution
                    .install(provider.acquire_prefix_history_execution(
                        assignment,
                        &self.node_id,
                        round.round_id(),
                        deadline,
                    )?),
            )
        } else {
            None
        };
        let execution = self
            .publication_execution
            .current()
            .ok_or_else(|| DfsError::Metadata {
                message: "historical intake has no actual execution owner".into(),
            })?;
        self.check_cancelled("native prefix historical intake")?;
        let result = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "historical intake has no metadata writer".into(),
            })?
            .retain_native_prefix_history(round, &provider, &execution, &self.node_id, deadline)
            .map_err(DfsError::from)?;
        self.check_cancelled("native prefix historical intake").map_err(|error| DfsError::Metadata {
            message: format!("historical intake completion is uncertain after local processing; retained DATA is not rolled back: {error}"),
        })?;
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn read_native_prefix_history(
        &self,
        shard: &DfsMetadataShardId,
    ) -> Result<
        Option<crate::metadata_backend::native_prefix_history::NativePrefixHistoryRecord>,
        DfsError,
    > {
        self.metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "historical reader has no actual backend".into(),
            })?
            .read_native_prefix_history(shard)
            .map_err(Into::into)
    }

    /// Gather fresh full-assignment proof for exactly this locally captured
    /// target. Refuse source advancement/history loss instead of upgrading it.
    /// The returned existing certificate is DATA, never new serving authority.
    pub(crate) fn sync_metadata_shard_exact_prefix_full_assignment(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
        timeout: Duration,
    ) -> Result<DfsMetadataQuorumSyncCertificate, DfsError> {
        if timeout.is_zero()
            || target.authority_epoch.is_none()
            || target.expected_shard.is_none()
            || target.barrier_id.is_empty()
            || target.state_fingerprint.is_empty()
            || target.checkpoint_snapshot.is_some()
        {
            return Err(DfsError::Metadata {
                message: "exact-prefix full-assignment sync requires a bounded current assignment and native prefix target without a snapshot".into(),
            });
        }
        self.sync_metadata_shard_to_quorum_with_required_intersection(
            target,
            &BTreeSet::new(),
            MetadataQuorumSyncPurpose::ExactPrefixFullAssignment,
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
        #[cfg(test)]
        if let Some((root, _)) = self.native_publication_owner_for_observation() {
            crate::default_cluster::bootstrap_observation::record(&root, &self.node_id, "metadata-sync-result", ||
                serde_json::json!({"target":target,"barrier":probe_barrier_id,"purpose":format!("{purpose:?}"),
                    "required_intersection":required_intersection,"result":result.as_ref().map(|v|v).map_err(|e|e.to_string())}));
        }
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
            if matches!(purpose, MetadataQuorumSyncPurpose::OrdinaryDurability | MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
                rejected_nodes.extend(self.send_metadata_sync_catchup_batches(
                    &target,
                    probe_barrier_id,
                    &mut pending_catchup_batches,
                    &mut source_history_gap_node_ids,
                    purpose,
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
            if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment)
                && !rejected_nodes.is_empty()
            {
                return Err(DfsError::Metadata { message: format!(
                    "exact-prefix sync participant rejected the fixed prefix: shard {} sequence {}; rejected={rejected_nodes:?}; no snapshot repair or target advance",
                    target.shard_id.0, target.journal_sequence,
                ) });
            }
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
                purpose,
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
            if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment)
                && !rejected_nodes.is_empty()
            {
                return Err(DfsError::Metadata { message: format!(
                    "exact-prefix sync participant rejected the fixed prefix: shard {} sequence {}; rejected={rejected_nodes:?}; no snapshot repair or target advance",
                    target.shard_id.0, target.journal_sequence,
                ) });
            }
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
                purpose,
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
            if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment)
                && !rejected_nodes.is_empty()
            {
                return Err(DfsError::Metadata { message: format!(
                    "exact-prefix sync participant rejected the fixed prefix: shard {} sequence {}; rejected={rejected_nodes:?}; no snapshot repair or target advance",
                    target.shard_id.0, target.journal_sequence,
                ) });
            }
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

    fn revalidate_exact_metadata_sync_prefix(
        &mut self,
        target: &DfsMetadataQuorumSyncTarget,
    ) -> Result<(), DfsError> {
        self.check_cancelled("exact-prefix full-assignment sync")?;
        let deadline = self
            .transport
            .operation_deadline()
            .ok_or_else(|| DfsError::Metadata {
                message: "exact-prefix sync requires its existing bounded transport owner".into(),
            })?;
        if Instant::now() >= deadline {
            return Err(DfsError::Network {
                message: "exact-prefix sync reached its original operation deadline".into(),
            });
        }
        let backend = self
            .metadata_backend
            .as_deref_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "exact-prefix sync has no actual durable source backend".into(),
            })?;
        backend.refresh_metadata_shard_attestation_view(&target.shard_id)?;
        let current_sequence = backend
            .metadata_shard_statuses()?
            .into_iter()
            .find(|status| status.shard_id == target.shard_id)
            .map(|status| status.journal_last_sequence)
            .ok_or_else(|| DfsError::Metadata {
                message: "exact-prefix sync source shard disappeared".into(),
            })?;
        let fingerprint =
            metadata_shard_state_fingerprint_at(backend, &target.shard_id, current_sequence)?;
        if current_sequence != target.journal_sequence || fingerprint != target.state_fingerprint {
            return Err(DfsError::Metadata {
                message: format!(
                    "exact-prefix sync source changed: shard {} requested sequence {} fingerprint {}, current sequence {} fingerprint {}; target remains unchanged",
                    target.shard_id.0,
                    target.journal_sequence,
                    target.state_fingerprint,
                    current_sequence,
                    fingerprint,
                ),
            });
        }
        if Instant::now() >= deadline {
            return Err(DfsError::Network {
                message:
                    "exact-prefix sync source validation exceeded its original operation deadline"
                        .into(),
            });
        }
        Ok(())
    }

    fn refresh_metadata_sync_target_to_current_head(
        &mut self,
        target: &mut DfsMetadataQuorumSyncTarget,
        purpose: &MetadataQuorumSyncPurpose,
    ) -> Result<bool, DfsError> {
        self.revalidate_metadata_quorum_sync_authority(target, purpose)?;
        if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
            return Ok(false); // revalidation checked the immutable head/digest
        }
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
        purpose: &MetadataQuorumSyncPurpose,
        diagnostics: &mut MetadataSyncAttemptDiagnostics,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
            self.revalidate_metadata_quorum_sync_authority(target, purpose)?;
        } else {
            self.replication
                .pending_metadata_replications
                .retain(|_, pending| pending.created_at.elapsed() < Duration::from_secs(60));
        }
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
                if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
                    return Err(DfsError::Metadata { message: format!(
                        "exact-prefix sync source history unavailable for shard {} target {} peer {}; snapshot repair is forbidden",
                        target.shard_id.0, target.journal_sequence, target_node_id,
                    ) });
                }
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

        if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
            self.revalidate_metadata_quorum_sync_authority(target, purpose)?;
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
                        if let Err(error) = &outcome {
                            diagnostics.first_batch_error(node_id, "direct-catchup", &command.request_id,
                                command.batch.next_after_sequence, error);
                        }
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
        if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
            self.revalidate_metadata_quorum_sync_authority(target, purpose)?;
        }
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
            repair_custody: None,
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
                .primary_handoff_for(target_node_id)
                .map(|handoff| {
                    handoff.repair_custody.as_ref().map_or_else(
                        || Ok(handoff.handoff_id.clone()),
                        |page| page.confirmation(),
                    )
                })
                .transpose()?;
            let mut node_prefix_probe = prefix_probe.clone();
            node_prefix_probe.primary_handoff_id = purpose
                .primary_handoff_id_for(target_node_id)
                .map(str::to_string);
            node_prefix_probe.repair_custody = purpose
                .primary_handoff_for(target_node_id)
                .and_then(|handoff| handoff.repair_custody.clone());
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
        if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
            self.revalidate_metadata_quorum_sync_authority(target, purpose)?;
        }
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
        if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
            return if rejected_node_ids.is_empty() && source_history_gap_node_ids.is_empty() {
                Ok(())
            } else {
                Err(DfsError::Metadata { message: format!(
                    "exact-prefix sync cannot repair incompatible or unavailable prefix for shard {} at {}; rejected={rejected_node_ids:?}; missing_history={source_history_gap_node_ids:?}; snapshot repair is forbidden",
                    target.shard_id.0, target.journal_sequence,
                ) })
            };
        }
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
        if matches!(purpose, MetadataQuorumSyncPurpose::OrdinaryDurability | MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
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
            if matches!(purpose, MetadataQuorumSyncPurpose::ExactPrefixFullAssignment) {
                self.revalidate_exact_metadata_sync_prefix(target)?;
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

    #[test]
    fn native_completed_handoff_is_not_reclaimed_after_effect_admission_changes() {
        use std::sync::{Arc, Mutex};
        let peer =
            NodeId::from_legacy_named("native-handoff-completed-before-admission-peer").unwrap();
        let fixture =
            configured_normal_sync_fixture("native-handoff-completed-before-admission", vec![peer]);
        let NormalSyncFixture {
            mut service,
            runtime,
            shard,
            control_plane_probe,
            ..
        } = fixture;
        let runtime = Arc::new(runtime);
        let control = runtime.shared_control_plane().unwrap();
        let promoted = shard.replica_node_ids[0].clone();
        // Stop only the existing real publication boundary, before publishing
        // the proposal. The other actual Main remains free to win on release.
        let fence = service
            .metadata_shard_fence_guard(&shard.file_system_id, &shard.shard_id)
            .unwrap();
        let handoff = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .request_metadata_shard_primary_handoff(&shard, &promoted)
            })
            .unwrap()
            .unwrap();
        let observed = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&observed);
        let actual = Arc::clone(&runtime);
        let original = handoff.clone();
        let source = shard.primary_node_id.clone();
        let control_for_hook = control.clone();
        BEFORE_HANDOFF_EFFECT_ADMISSION.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                drop(fence);
                let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                actual.request_repair_reconciliation(&source).unwrap();
                loop {
                    if let Some(completed) = actual
                        .observe_completed_native_handoff(&control_for_hook, &original, deadline)
                        .unwrap()
                    {
                        *captured.lock().unwrap() = Some(completed);
                        break;
                    }
                    let remaining = deadline.checked_duration_since(Instant::now()).expect(
                        "normal Main completes the original handoff within its existing budget",
                    );
                    actual
                        .maintenance_tick(
                            &source,
                            remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                        )
                        .unwrap();
                }
            }))
        });
        let result = service.reconcile_pending_metadata_shard_membership_handoffs(
            std::slice::from_ref(&shard),
            DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
        );
        let completed = observed.lock().unwrap().clone();
        std::fs::write(runtime.data_dir(&shard.primary_node_id).root().join("boundary-diagnostic-handoff-admission-winner.json"),
            serde_json::to_vec_pretty(&serde_json::json!({"original":handoff,"actual_completed":completed,"caller_result":format!("{result:?}"),
                "qualification":"Actual registered Main and exact original completion proof; hook schedules existing bounded work and supplies no authority or result."})).unwrap()).unwrap();
        assert_eq!(completed.as_ref(), Some(&handoff.desired_shard));
        assert_eq!(
            result.unwrap(),
            0,
            "the losing actor has no pending handoff to execute"
        );
        drop(service);
        drop(control_plane_probe);
        drop(control);
        Arc::try_unwrap(runtime).ok().unwrap().close().unwrap();
    }

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
            let mut valid_target = target.clone();
            valid_target.checkpoint_snapshot = Some(snapshot.clone());
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
            let owned_attempts = fixture.service.transport.observe_routed_calls_for_test();
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
                owned_attempts.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "invalid supplied snapshot cannot dispatch through its own transport"
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
            // Calibrate the same observer against a valid native snapshot call.
            // Other host clients retain their own transports and cannot affect it.
            fixture
                .service
                .send_metadata_sync_checkpoint_snapshots(
                    &valid_target,
                    "valid-owned-observation-control",
                    &rejected,
                    &BTreeSet::new(),
                    &mut BTreeMap::new(),
                    &MetadataQuorumSyncPurpose::OrdinaryDurability,
                )
                .unwrap();
            assert_eq!(owned_attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
            fixture.service.process_incoming().unwrap();
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
        successive_handoffs_observe_replica(false);
    }

    #[test]
    fn native_successive_handoffs_wait_for_the_actual_replica_materializer() {
        successive_handoffs_observe_replica(true);
    }

    fn successive_handoffs_observe_replica(delayed_after_reopen: bool) {
        use crate::runtime_host::materialization_test::wait_for_existing_materialization;
        let name = if delayed_after_reopen {
            "native-sync-successive-handoff-materializer"
        } else {
            "native-sync-successive-handoff"
        };
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
        let shard = DfsMetadataShardId::new("root");
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let observed_source = || {
            LocalSobjMetadataBackend::open_for_inspection(
                shard.clone(),
                crate::default_dfs_metadata_shards_root(fixture.runtime.data_dir(&nodes[0]))
                    .join("root"),
            )
            .unwrap()
        };
        let initial_prefix = observed_source()
            .export_table_snapshot()
            .materialized_through_sequence;
        // The negative control places the next accepted append after startup,
        // matching the reproduced durable8/materialized7 late-publication gap.
        if delayed_after_reopen {
            fixture.runtime.stop_node(&nodes[1]).unwrap();
            fixture.runtime.reopen_node(nodes[1].clone()).unwrap();
            wait_for_existing_materialization(
                fixture.runtime.data_dir(&nodes[1]),
                &active.file_system_id,
                &shard,
                &fixture.runtime.runtime_handle(),
                initial_prefix,
                deadline,
                |_| {},
            )
            .unwrap();
        }
        let existing_replica =
            delayed_after_reopen.then(|| fixture.runtime.client(&nodes[1]).unwrap());
        let pause = delayed_after_reopen.then(|| {
            fixture
                .runtime
                .pause_replica_materialization(&nodes[1], &shard)
                .unwrap()
        });
        let expected = fixture
            .service
            .posix_mkdir("/native-after-two-handoffs")
            .unwrap();
        // Capture the actual published source cut, not a requester cursor or an
        // ACK. Inspecting this store cannot ingest work or grant authority.
        let source = LocalSobjMetadataBackend::open_for_inspection(
            shard.clone(),
            crate::default_dfs_metadata_shards_root(fixture.runtime.data_dir(&nodes[0]))
                .join("root"),
        )
        .unwrap();
        let object = source.get_object(&expected.object_id).unwrap().unwrap();
        assert_eq!(object.version, expected.version);
        assert_eq!(object.content_hash, expected.content_hash);
        let published = source.export_table_snapshot();
        assert!(published.prefix_proof.is_well_formed());
        assert_eq!(
            published.prefix_proof.journal_sequence,
            published.materialized_through_sequence
        );
        let target = published.materialized_through_sequence;
        drop(source);
        if !delayed_after_reopen {
            fixture.runtime.stop_node(&nodes[1]).unwrap();
            fixture.runtime.reopen_node(nodes[1].clone()).unwrap();
        }
        let mut replica =
            existing_replica.unwrap_or_else(|| fixture.runtime.client(&nodes[1]).unwrap());
        if let Some(pause) = pause.as_ref() {
            let captured = fixture
                .runtime
                .wait_paused_materialization(
                    pause,
                    deadline.saturating_duration_since(Instant::now()),
                )
                .unwrap();
            assert!(
                captured.materialized_sequence.unwrap() < target,
                "{captured:?}"
            );
            assert!(matches!(
                replica.posix_stat("/native-after-two-handoffs"),
                Err(DfsPosixError::NotFound { .. })
            ));
            assert!(
                wait_for_existing_materialization(
                    fixture.runtime.data_dir(&nodes[1]),
                    &active.file_system_id,
                    &shard,
                    &fixture.runtime.runtime_handle(),
                    target,
                    Instant::now(),
                    |_| panic!("an expired observer must not request work"),
                )
                .is_err()
            );
        }
        let mut saw_gap = false;
        wait_for_existing_materialization(
            fixture.runtime.data_dir(&nodes[1]),
            &active.file_system_id,
            &shard,
            &fixture.runtime.runtime_handle(),
            target,
            deadline,
            |_| {
                saw_gap = true;
                if let Some(pause) = pause.as_ref() {
                    pause.release();
                }
            },
        )
        .unwrap();
        if delayed_after_reopen {
            assert!(
                saw_gap,
                "the actual materializer was paused below the published cut"
            );
        }
        assert_eq!(
            replica.posix_stat("/native-after-two-handoffs").unwrap(),
            expected
        );
        assert!(
            Instant::now() < deadline,
            "replica stat exceeded the original network deadline"
        );
        drop(replica);
        drop(pause);
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

#[cfg(test)]
mod native_selected_data_tests {
    use super::*;
    use crate::*;
    use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin, RpcPhase};
    use std::sync::{Arc, Mutex};

    mod native_owned_repair_return_tests;
    mod native_retired_repair_intake_tests;

    struct GenuineNativeOriginal {
        runtime: DfsTestRuntime,
        shard: DfsMetadataShardConfig,
        promoted: NodeId,
        holder: NodeId,
        birth: crate::repair_queue_store::RepairOriginalBirth,
        // Real C output captured before the existing reply-loss fault. It is
        // never reconstructed from content bytes or a receipt tuple.
        delayed_reply: Option<DfsRoutedMessage>,
        // Negative fixture controls only. Defaults preserve the original
        // first-effect/lost-reply choreography; a new intake case may release
        // exactly one normal retry of the SAME captured logical request.
        claim_window: Arc<std::sync::atomic::AtomicBool>,
        allow_one_same_original_retry: Arc<std::sync::atomic::AtomicBool>,
        held: Option<Box<dyn Send>>,
        held_promoted: Option<Box<dyn Send>>,
    }

    fn genuine_native_original(name: &str) -> GenuineNativeOriginal {
        genuine_native_original_with_forbidden_promoted(name, false)
    }

    // Failure-only observations: never open/recover a queue owner or change the
    // failed predicate. Each source is a separate, racing diagnostic read.
    fn retain_genuine_original_timeout_diagnostic(
        runtime: &DfsTestRuntime,
        owner: &NodeId,
        holder: &NodeId,
        snapshot_path: &std::path::Path,
        capture: &crate::test_runtime::NativeCheckpointedBackgroundRepairCapture,
        replies: &Mutex<Vec<DfsRoutedMessage>>,
    ) -> std::result::Result<(), String> {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let commands = catch_unwind(AssertUnwindSafe(|| capture.records()))
            .map_err(|_| "checkpointed-command observation panicked");
        let captured_replies = replies.try_lock()
            .map(|replies| (replies.len(), replies.iter().take(32).cloned().collect::<Vec<_>>()))
            .map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => "captured replies busy",
                std::sync::TryLockError::Poisoned(_) => "captured replies poisoned",
            });
        let rpc = catch_unwind(AssertUnwindSafe(|| runtime.rpc_observations()))
            .map(|window| {
                let window_count = window.len();
                let mut matching = window.into_iter().filter(|event|
                    event.source == *owner && event.target == *holder
                        && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                        && event.origin == RpcOrigin::RegisteredHost
                ).collect::<Vec<_>>();
                let matching_count = matching.len();
                let omitted = matching_count.saturating_sub(64);
                matching.drain(..omitted);
                (window_count, matching_count, matching)
            })
            .map_err(|_| "current RPC-window observation panicked");
        let mut keys = BTreeSet::new();
        if let Ok(commands) = &commands {
            for captured in commands.iter().take(8) {
                if captured.message.source_node_id != *owner { continue; }
                if let DfsNetworkMessage::RepairReplica(command) = &captured.message.message {
                    keys.insert(crate::repair_queue_store::DfsRepairQueueKey {
                        intent_id: command.intent_id.clone(),
                        missing_replica_node_id: captured.message.target_node_id.clone(),
                    });
                }
            }
        }
        if let Ok((_, replies)) = &captured_replies {
            for routed in replies {
                if routed.target_node_id != *owner { continue; }
                if let DfsNetworkMessage::RepairReplicaResponse(response) = &routed.message {
                    keys.insert(crate::repair_queue_store::DfsRepairQueueKey {
                        intent_id: response.intent_id.clone(),
                        missing_replica_node_id: routed.source_node_id.clone(),
                    });
                }
            }
        }
        if let Ok((_, _, rpc)) = &rpc {
            for event in rpc {
                if let Some(intent_id) = &event.dfs_intent_id {
                    keys.insert(crate::repair_queue_store::DfsRepairQueueKey {
                        intent_id: intent_id.clone(),
                        missing_replica_node_id: event.target.clone(),
                    });
                }
            }
        }
        let observed_key_count = keys.len();
        let queue = keys.into_iter().take(8).map(|key| {
            match catch_unwind(AssertUnwindSafe(|| {
                crate::repair_queue_store::LocalDfsRepairQueueStore::observe_registered_original_for_slot(
                    snapshot_path, &key, crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT as u64,
                )
            })) {
                Ok(observed) => serde_json::json!({"key": key, "observation": observed}),
                Err(_) => serde_json::json!({"key": key, "unavailable": "non-recovering queue observation panicked"}),
            }
        }).collect::<Vec<_>>();
        let diagnostic = serde_json::json!({
            "qualification": "failure-only, per-call racing observations; unavailable/cold/dirty/uncertain is not absence; readable bytes are not durable ACK, Terminal, custody or execution authority; RPC Completed precedes possible reply loss",
            "failed_predicate": "actual lost reply reaches original queue within unchanged 5s budget",
            "owner": owner, "holder": holder, "admitted_queue_snapshot": snapshot_path,
            "checkpointed_commands": match commands {
                Ok(commands) => serde_json::json!({
                    "observed_count": commands.len(),
                    "capture_omitted_count": capture.omitted_count(),
                    "diagnostic_omitted_count": commands.len().saturating_sub(8),
                    "records": commands.iter().take(8).map(|captured| serde_json::json!({
                        "message": captured.message, "descriptor": captured.descriptor,
                    })).collect::<Vec<_>>(),
                }),
                Err(error) => serde_json::json!({"unavailable": error, "capture_omitted_count": capture.omitted_count()}),
            },
            "captured_replies": match captured_replies {
                Ok((count, replies)) => serde_json::json!({"observed_count": count,
                    "diagnostic_omitted_count": count.saturating_sub(32), "records": replies,
                    "qualification": "actual captured responses including negative dispositions; no positive-only filter"}),
                Err(error) => serde_json::json!({"unavailable": error}),
            },
            "rpc": match rpc {
                Ok((window_count, matching_count, records)) => serde_json::json!({
                    "current_window_count": window_count, "matching_count": matching_count,
                    "diagnostic_omitted_count": matching_count.saturating_sub(64),
                    "runtime_dropped_count": runtime.observation_dropped_count(),
                    "records": records.iter().map(|record| format!("{record:?}")).collect::<Vec<_>>(),
                    "selection": "latest owner-to-holder registered Repair observations in current window only; no seed-intent filter; prior checkpoint windows not read",
                }),
                Err(error) => serde_json::json!({"unavailable": error,
                    "runtime_dropped_count": runtime.observation_dropped_count()}),
            },
            "resident_queue": {"observed_key_count": observed_key_count,
                "omitted_key_count": observed_key_count.saturating_sub(8), "records": queue,
                "qualification": "only intent/target keys actually present in retained commands, replies or RPC observations; no enumeration, normal owner lookup, recovery or resync"},
        });
        let encoded = serde_json::to_string_pretty(&diagnostic).map_err(|error| error.to_string())?;
        eprintln!("SELECTED_GENUINE_LOST_REPLY_TIMEOUT {encoded}");
        std::fs::write(runtime.data_dir(owner).root().join("boundary-diagnostic-genuine-original-timeout.json"), encoded)
            .map_err(|error| error.to_string())
    }

    fn genuine_native_original_with_forbidden_promoted(
        name: &str,
        forbid_promoted: bool,
    ) -> GenuineNativeOriginal {
        genuine_native_original_with_receiver_foundation(name, forbid_promoted, false)
    }

    fn genuine_native_original_with_receiver_foundation(
        name: &str,
        forbid_promoted: bool,
        receiver_foundation: bool,
    ) -> GenuineNativeOriginal {
        genuine_native_original_with_receiver_source_probe(
            name,
            forbid_promoted,
            receiver_foundation,
            false,
        )
    }

    // The optional probe is one test-only negative at the SAME first genuine
    // A call. Existing fixtures retain their exact captured-reply-loss arm.
    fn genuine_native_original_with_receiver_source_probe(
        name: &str,
        forbid_promoted: bool,
        receiver_foundation: bool,
        probe_wrong_source: bool,
    ) -> GenuineNativeOriginal {
        genuine_native_original_with_receiver_modes(
            name, forbid_promoted, receiver_foundation, probe_wrong_source, false,
        )
    }

    fn genuine_native_original_with_receiver_cut(
        name: &str,
        forbid_promoted: bool,
        receiver_foundation: bool,
        prepared_only: bool,
    ) -> GenuineNativeOriginal {
        genuine_native_original_with_receiver_modes(
            name, forbid_promoted, receiver_foundation, false, prepared_only,
        )
    }

    fn genuine_native_original_with_receiver_modes(
        name: &str,
        forbid_promoted: bool,
        receiver_foundation: bool,
        probe_wrong_source: bool,
        prepared_only: bool,
    ) -> GenuineNativeOriginal {
        assert!(!probe_wrong_source || receiver_foundation);
        assert!(!prepared_only || receiver_foundation);
        assert!(!(probe_wrong_source && prepared_only), "native fixture modes are independent controls");
        let path = "/services/selected-original-data.bin";
        let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
        let mut runtime = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        for node in &nodes[1..] { runtime.define_node(node.clone()).unwrap(); }
        for node in &nodes {
            if receiver_foundation { runtime.select_original_repair_fixture_before_start(node).unwrap(); }
            else { runtime.select_original_data_fixture_before_start(node).unwrap(); }
        }
        runtime.start_nodes().unwrap();
        let configs = runtime.metadata_shards().unwrap();
        let fs = runtime.file_system().file_system_id;
        let shard = crate::select_metadata_shard_config_for_path(&fs, &configs, path)
            .unwrap().unwrap();
        let owner = shard.primary_node_id.clone();
        let mut peers = nodes.iter().filter(|node| **node != owner).cloned();
        let promoted = peers.next().unwrap();
        let holder = peers.next().unwrap();
        assert!(shard.replica_node_ids.contains(&promoted) && shard.replica_node_ids.contains(&holder));
        if receiver_foundation {
            // Physical fixture representation is selected before startup for
            // all potential receivers. Actual A/C roles come ONLY from the
            // current native assignment above; this does not seed authority.
            runtime.select_original_repair_wire_target(&owner, holder.clone()).unwrap();
        }
        let mut policy = DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        policy.required_node_ids = vec![owner.clone(), holder.clone()];
        if forbid_promoted {
            policy.forbidden_node_ids.push(promoted.clone());
        }
        runtime.apply_placement_policy(&owner, path, DfsPlacementScope::Subtree, policy).unwrap();
        // Candidates are hints. Resolve the actual managed A/C constraints;
        // the first real background Repair will create C's copy below.
        let mut policy_reader = runtime.metadata_primary_service(path).unwrap();
        let planned_policy = policy_reader.managed_write_replication_policy_for_path(
            path, sinomo_core::DfsObjectPolicy::ReplicatedEventual, 2,
        ).unwrap();
        let required = BTreeSet::from([owner.clone(), holder.clone()]);
        assert_eq!(planned_policy.min_commit_replicas, 1);
        assert_eq!(planned_policy.desired_replicas, 2);
        assert_eq!(planned_policy.required_node_ids.iter().cloned().collect::<BTreeSet<_>>(), required);
        assert_eq!(planned_policy.forbidden_node_ids.contains(&promoted), forbid_promoted);
        drop(policy_reader);
        let replies = Arc::new(Mutex::new(Vec::new()));
        // Hold the FIRST background claim before seeding. The actual returned
        // stat supplies its identity before release; no body/request is minted
        // by this gate. After captured reply loss, block another attempt while
        // the ordinary callback persists the exact uncertainty below.
        let claim_window = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let allow_one_same_original_retry = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seeded_identity = Arc::new(Mutex::new(None::<(DfsObjectId, DfsVersion)>));
        let window = Arc::clone(&claim_window);
        let same_original_retry = Arc::clone(&allow_one_same_original_retry);
        let expected_identity = Arc::clone(&seeded_identity);
        let captured_replies = Arc::clone(&replies);
        let expected_owner = owner.clone();
        let expected_holder = holder.clone();
        let context = runtime.component_context(&owner).unwrap();
        let held = Some(Box::new(crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            context.data_dir(), Arc::new(move |point, command, _| {
                let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeReplicaTransferClaim {
                    background_repair: true, kind, intent_id_digest, target_id_digest, request_id_digest, ..
                } = point else { return Ok(()); };
                assert!(command.is_none());
                if !window.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(DfsError::Cancelled { operation: "negative claim boundary around actual captured reply loss" });
                }
                let identity = expected_identity.lock().unwrap();
                let (expected_object, expected_version) = identity.as_ref()
                    .expect("only the actual committed seed can release its claim window");
                let captured_loss = matches!(kind, sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
                    && captured_replies.lock().unwrap().iter().any(|routed: &DfsRoutedMessage| {
                    let DfsNetworkMessage::RepairReplicaResponse(response) = &routed.message else { return false; };
                    routed.source_node_id == expected_holder && routed.target_node_id == expected_owner
                        && &response.object_id == expected_object && &response.version == expected_version
                        && *blake3::hash(response.intent_id.as_bytes()).as_bytes() == intent_id_digest
                        && *blake3::hash(expected_holder.as_str().as_bytes()).as_bytes() == target_id_digest
                });
                if captured_loss {
                    if same_original_retry.load(std::sync::atomic::Ordering::Acquire)
                        && captured_replies.lock().unwrap().iter().any(|routed| {
                            let DfsNetworkMessage::RepairReplicaResponse(response) = &routed.message else { return false; };
                            routed.source_node_id == expected_holder && routed.target_node_id == expected_owner
                                && &response.object_id == expected_object && &response.version == expected_version
                                && *blake3::hash(response.intent_id.as_bytes()).as_bytes() == intent_id_digest
                                && *blake3::hash(expected_holder.as_str().as_bytes()).as_bytes() == target_id_digest
                                && *blake3::hash(response.request_id.as_bytes()).as_bytes() == request_id_digest
                        })
                        && same_original_retry.swap(false, std::sync::atomic::Ordering::AcqRel)
                    {
                        window.store(false, std::sync::atomic::Ordering::Release);
                        return Ok(());
                    }
                    return Err(DfsError::Cancelled { operation: "negative claim boundary around actual captured reply loss" });
                }
                Ok(())
            }),
        )) as Box<dyn Send>);
        drop(context);
        let held_promoted = Some(Box::new(runtime.deny_background_replica_before_claim(&promoted).unwrap()) as Box<dyn Send>);
        // The pending-history arm uses the EXISTING actual receiver cut after
        // Prepared. It cannot fabricate a response, effect or Terminal.
        let held_receiver = prepared_only.then(|| {
            assert!(receiver_foundation);
            let receiver = runtime.client(&holder).unwrap();
            let content = receiver.content().clone();
            drop(receiver);
            let context = runtime.component_context(&holder).unwrap();
            let window = Arc::clone(&claim_window);
            crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(context.data_dir(), Arc::new(move |point, command, _| {
                if matches!(point, crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { .. })
                    && command.is_some_and(|command| matches!(&command.routed_message.message, DfsNetworkMessage::SelectedOriginalRepair(_))) {
                    window.store(false, std::sync::atomic::Ordering::Release);
                    content.arm_original_repair_after_prepared_test_cut()?;
                }
                Ok(())
            }))
        });
        let held = match held_receiver {
            Some(receiver) => Some(Box::new((held, receiver)) as Box<dyn Send>),
            None => held,
        };
        let capture = runtime.capture_checkpointed_background_repairs(&owner, 8).unwrap();
        let mut paused_dispatch = NativeRepairProbeDispatchGate(
            probe_wrong_source.then(|| Arc::new(tokio::sync::Semaphore::new(0))),
        );
        runtime.install_fault(FaultRule {
            source: Some(owner.clone()), target: Some(holder.clone()),
            route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()), call_id: None,
            origin: Some(RpcOrigin::RegisteredHost), remaining_hits: 1,
            action: match &paused_dispatch.0 {
                Some(gate) => FaultAction::PauseBeforeDispatch(Arc::clone(gate)),
                None => FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
                    intent_id: None,
                    captured: Arc::clone(&replies),
                },
            },
        });
        let mut writer = runtime.client(&owner).unwrap();
        assert!(writer.transport.delegates_local_namespace_authority());
        let seed_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let previous = writer.set_transport_operation_deadline(Some(seed_deadline));
        let bytes = vec![0x5a; 4096];
        let stat = writer.posix_write_file(path, &bytes, DfsWriteOptions {
            policy: planned_policy,
            replica_candidates: vec![owner.clone(), holder.clone()],
            wait_for_commit: true, foreground_replica_repair: false,
        }).unwrap();
        assert!(Instant::now() <= seed_deadline, "unchanged registered Main seed budget");
        assert_eq!(writer.transport.operation_deadline(), Some(seed_deadline));
        writer.set_transport_operation_deadline(previous);
        assert_eq!(writer.content().read_object_version(stat.object_id.as_str(), stat.version.as_str()).unwrap().1, bytes);
        drop(writer);
        let seed_owner = runtime.metadata_primary_service(path).unwrap();
        let backend = seed_owner.metadata_backend().unwrap();
        let intents = backend.intents_for_object(&stat.object_id).unwrap();
        assert!(intents.iter().any(|intent| intent.target_version == stat.version
            && intent.status == DfsWriteIntentStatus::Committed
            && intent.policy.min_commit_replicas == 1 && intent.policy.desired_replicas == 2
            && intent.desired_replica_node_ids.iter().cloned().collect::<BTreeSet<_>>() == required));
        let receipts = backend.receipts_for_version(&stat.object_id, &stat.version).unwrap();
        assert_eq!(receipts.iter().map(|receipt| receipt.replica_node_id.clone()).collect::<BTreeSet<_>>(),
            BTreeSet::from([owner.clone()]), "committed W1 seed precedes C's first background effect");
        eprintln!("SELECTED_NATIVE_SEED_RECEIPTS {:?}", receipts.iter().map(|receipt| &receipt.replica_node_id).collect::<Vec<_>>());
        drop(seed_owner);
        assert!(capture.records().is_empty() && replies.lock().unwrap().is_empty());
        assert_eq!(capture.omitted_count(), 0);
        assert_eq!(runtime.observation_dropped_count(), 0);
        assert!(!runtime.rpc_observations().iter().any(|call|
            call.source == owner && call.target == holder && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE),
            "no real A-to-C Repair can precede the closed first-effect gate");
        assert!(Instant::now() <= seed_deadline, "unchanged complete seed preparation budget");
        *seeded_identity.lock().unwrap() = Some((stat.object_id.clone(), stat.version.clone()));
        claim_window.store(true, std::sync::atomic::Ordering::Release);
        let selected_layout = runtime.selected_original_data_layout(&owner).unwrap();
        let queue = selected_layout.queue().unwrap();
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let birth = if probe_wrong_source {
            let checks = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let (birth, carrier, paused_call_id) = loop {
                    let found = capture.records().iter().find_map(|captured| {
                        let birth = queue.original_repair(&captured.message).unwrap()?;
                        let DfsNetworkMessage::RepairReplica(command) = &birth.original.message
                        else {
                            return None;
                        };
                        if birth.closed_at_sequence.is_some()
                            || birth.attempts.is_empty()
                            || command.object_id != stat.object_id
                            || command.version != stat.version
                            || captured.message.source_node_id != owner
                            || captured.message.target_node_id != holder
                        {
                            return None;
                        }
                        let call = runtime.rpc_observations().into_iter().find(|call| {
                            call.source == owner
                                && call.target == holder
                                && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                                && call.origin == RpcOrigin::RegisteredHost
                                && call.phase == RpcPhase::BeforeDispatch
                                && call.dfs_request_id.as_deref()
                                    == Some(command.request_id.as_str())
                                && call.dfs_frame_id == Some(captured.message.frame_id)
                        });
                        call.map(|call| (birth, captured.message.clone(), call.call_id))
                    });
                    assert!(
                        Instant::now() <= deadline,
                        "first genuine A call must pause within the unchanged 5s phase budget"
                    );
                    if let Some(found) = found {
                        break found;
                    }
                    let remaining = deadline.checked_duration_since(Instant::now()).unwrap();
                    runtime.maintenance_tick(&owner, remaining).unwrap();
                };
                // Stop subsequent producer claims. Releasing the already-owned
                // call below does not consult this gate or mint another attempt.
                claim_window.store(false, std::sync::atomic::Ordering::Release);
                probe_selected_original_authenticated_source(
                    &runtime,
                    &owner,
                    &promoted,
                    &holder,
                    &birth,
                    &carrier,
                    paused_call_id,
                    deadline,
                    &mut paused_dispatch,
                );
                birth
            }));
            paused_dispatch.release();
            match checks {
                Ok(birth) => birth,
                Err(error) => {
                    claim_window.store(false, std::sync::atomic::Ordering::Release);
                    runtime.clear_faults();
                    drop(capture);
                    drop(held);
                    drop(held_promoted);
                    if let Err(close_error) = runtime.close() {
                        eprintln!(
                            "native receiver source-probe cleanup also failed: {close_error}"
                        );
                    }
                    std::panic::resume_unwind(error);
                }
            }
        } else {
            loop {
                let found = capture.records().iter().find_map(|captured| {
                    queue.original_repair(&captured.message).unwrap().filter(|birth| {
                        let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else { return false; };
                        birth.closed_at_sequence.is_none() && !birth.attempts.is_empty()
                            && queue.records_for_targets(&command.intent_id, std::slice::from_ref(&holder)).unwrap()
                                .iter().any(|row| row.transfer_request_id.as_deref() == Some(command.request_id.as_str())
                                    && !row.in_flight && row.last_error.is_some()
                                    && (row.transfer_delivery_state == Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
                                        || (prepared_only && row.transfer_delivery_state != Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent))))
                    })
                });
                if let Some(birth) = found {
                    let exact_lost_reply = replies.lock().unwrap().iter().any(|routed| {
                        let DfsNetworkMessage::RepairReplicaResponse(response) = &routed.message else { return false; };
                        if routed.source_node_id != holder || routed.target_node_id != owner
                            || !matches!(response.result, DfsBlobTransferResult::Durable { .. } | DfsBlobTransferResult::Committed { .. })
                        { return false; }
                        let mut checked = birth.clone();
                        checked.observed.push(response.clone());
                        // Validate this candidate reply with the existing full
                        // original validator, without persisting any observation.
                        checked.validate(&birth.root.subject.queue_incarnation).is_ok()
                    });
                    if prepared_only {
                        let material = birth.original_wire_material().unwrap();
                        if let Some(prepared) = runtime.client(&holder).unwrap().content().original_repair_test_snapshot(&material).unwrap() {
                            assert!(prepared.bound_id.is_none() && prepared.terminal_id.is_none() && prepared.response.is_none());
                            assert_eq!(prepared.bytes, vec![0x5a;4096]);
                            assert!(!exact_lost_reply);
                            let command = &material.original.command;
                            let returned_cut = runtime.rpc_observations().iter().any(|call| call.source == owner && call.target == holder
                                && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE && call.origin == RpcOrigin::RegisteredHost
                                && call.phase == RpcPhase::EndedWithoutResponse
                                && call.dfs_request_id.as_deref() == Some(command.request_id.as_str())
                                && call.dfs_intent_id.as_deref() == Some(command.intent_id.as_str())
                                && call.dfs_file_system_id.as_ref() == Some(&command.file_system_id)
                                && call.dfs_object_id.as_ref() == Some(&command.object_id)
                                && call.dfs_version.as_ref() == Some(&command.version)
                                && call.outcome_error.as_deref().is_some_and(|error| error.contains("injected receiver cut after Prepared before content binding")));
                            if returned_cut { break birth; }
                        }
                    } else if exact_lost_reply { break birth; }
                }
                let remaining = deadline.checked_duration_since(Instant::now());
                if remaining.is_none() {
                    let diagnostic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        retain_genuine_original_timeout_diagnostic(
                            &runtime, &owner, &holder, selected_layout.queue_layout().snapshot_path(),
                            &capture, &replies,
                        )
                    }));
                    // Diagnostic failure must not replace the original panic or
                    // bypass the runtime's normal unwinding/fixture preservation.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if !matches!(diagnostic, Ok(Ok(()))) {
                            eprintln!("SELECTED_GENUINE_TIMEOUT_DIAGNOSTIC_UNAVAILABLE {diagnostic:?}");
                        }
                    }));
                }
                let remaining = remaining.expect("actual lost reply reaches original queue within unchanged 5s budget");
                let tick = runtime.maintenance_tick(&owner, remaining).unwrap();
                eprintln!("SELECTED_GENUINE_LOST_REPLY_TICK {:?}", tick.outcome);
            }
        };
        claim_window.store(false, std::sync::atomic::Ordering::Release);
        runtime.clear_faults();
        assert_eq!(capture.omitted_count(), 0);
        drop(capture);
        let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else { panic!("genuine Repair"); };
        assert_eq!(command.object_id, stat.object_id);
        assert_eq!(command.version, stat.version);
        assert_eq!(birth.original.source_node_id, owner);
        assert_eq!(birth.attachment.total_size_bytes, 4096);
        assert_eq!(birth.root.subject.request_id, command.request_id);
        assert_eq!(birth.root.original_command_digest,
            crate::repair_queue_store::original_repair_digest(&birth.original, &birth.attachment).unwrap());
        // C's bytes follow its actual Repair effect, never fixture-supplied
        // content or an inferred A-side receipt. The existing arm loses reply.
        for node in [&owner, &holder] {
            let reader = runtime.client(node).unwrap();
            if prepared_only && node == &holder {
                assert!(reader.content().read_object_version(stat.object_id.as_str(), stat.version.as_str()).is_err(), "Prepared body is not a live binding");
            } else {
                assert_eq!(reader.content().read_object_version(stat.object_id.as_str(), stat.version.as_str()).unwrap().1, bytes);
            }
        }
        let empty_successor = runtime.client(&promoted).unwrap();
        assert!(matches!(empty_successor.content().read_manifest(stat.object_id.as_str(), stat.version.as_str()),
            Err(crate::ContentStoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound),
            "the genuine source/copy cut leaves B without the exact version");
        drop(empty_successor);
        let delayed_reply = replies.lock().unwrap().iter().find(|routed| {
            let DfsNetworkMessage::RepairReplicaResponse(response) = &routed.message else { return false; };
            if routed.source_node_id != holder || routed.target_node_id != owner { return false; }
            let mut checked = birth.clone();
            checked.observed.push(response.clone());
            checked.validate(&birth.root.subject.queue_incarnation).is_ok()
        }).cloned();
        GenuineNativeOriginal {
            runtime,
            shard,
            promoted,
            holder,
            birth,
            delayed_reply,
            claim_window,
            allow_one_same_original_retry,
            held,
            held_promoted,
        }
    }

    struct NativeRepairProbeDispatchGate(Option<Arc<tokio::sync::Semaphore>>);
    impl NativeRepairProbeDispatchGate {
        fn release(&mut self) {
            if let Some(gate) = self.0.take() {
                gate.add_permits(1);
            }
        }
    }
    impl Drop for NativeRepairProbeDispatchGate {
        fn drop(&mut self) {
            self.release();
        }
    }

    fn native_repair_receiver_content_files(root: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        fn visit(root: &Path, path: &Path, files: &mut Vec<(std::path::PathBuf, Vec<u8>)>) {
            let metadata = std::fs::symlink_metadata(path).unwrap();
            assert!(
                !metadata.file_type().is_symlink(),
                "receiver fixture must retain its actual physical files"
            );
            if metadata.is_dir() {
                for entry in std::fs::read_dir(path).unwrap() {
                    visit(root, &entry.unwrap().path(), files);
                }
            } else {
                assert!(metadata.is_file());
                files.push((
                    path.strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(path).unwrap(),
                ));
            }
        }
        let mut files = Vec::new();
        visit(root, root, &mut files);
        files.sort_by(|left, right| left.0.cmp(&right.0));
        files
    }

    fn probe_selected_original_authenticated_source(
        runtime: &DfsTestRuntime,
        owner: &NodeId,
        wrong_source: &NodeId,
        holder: &NodeId,
        birth: &crate::repair_queue_store::RepairOriginalBirth,
        carrier: &DfsRoutedMessage,
        paused_call_id: u64,
        deadline: Instant,
        paused_dispatch: &mut NativeRepairProbeDispatchGate,
    ) {
        let material = birth.original_wire_material().unwrap();
        assert_eq!(material.original.routed(), birth.original);
        let command = &material.original.command;
        assert_eq!(&carrier.source_node_id, owner);
        assert_eq!(&carrier.target_node_id, holder);
        let queue = runtime
            .selected_original_data_layout(owner)
            .unwrap()
            .queue()
            .unwrap();
        let source = runtime.client(owner).unwrap();
        let receiver = runtime.client(holder).unwrap();
        let reader = source
            .content()
            .open_retained_repair_payload(&birth.root)
            .unwrap();
        let selected = DfsRoutedCall::with_attachment(
            carrier.clone(),
            DfsOutboundAttachment::from_reader(
                reader,
                birth.attachment.total_size_bytes,
                birth.attachment.content_hash.as_deref().unwrap(),
            ),
        )
        .with_original_repair(material.clone())
        .unwrap();
        let (expected, call) = crate::dfs_routed_call_rpc_call(selected).unwrap();
        assert_eq!(expected.routed_message, *carrier);
        let queue_before = queue.view().unwrap();
        let birth_before = queue.original_repair(carrier).unwrap().unwrap();
        assert_eq!(&birth_before, birth);
        let content_root = runtime.data_dir(holder).dfs_dir().join("content");
        let files_before = native_repair_receiver_content_files(&content_root);
        let last_call = runtime
            .rpc_observations()
            .iter()
            .map(|call| call.call_id)
            .max()
            .unwrap_or(0);
        assert_eq!(runtime.observation_dropped_count(), 0);
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .expect("same 5s probe budget");
        // The envelope still names A. Only the real authenticated native
        // carrier is B; no claimed origin/head/body/digest is reconstructed.
        let error = match runtime.rpc_result(wrong_source, holder, call, remaining) {
            Err(error) => error,
            Ok(_) => panic!(
                "wrong authenticated source must fail service admission without a domain reply"
            ),
        };
        let expected_error = format!(
            "authenticated DFS message caller {wrong_source} does not match claimed source {owner} for route {DFS_DATA_REPAIR_REPLICA_ROUTE}"
        );
        assert!(
            error.contains(&expected_error),
            "requires the actual authentication rejection, not an arbitrary transport error: {error}"
        );
        assert!(
            Instant::now() <= deadline,
            "wrong-source rejection must finish within its original 5s phase budget"
        );
        // Compare raw files FIRST. The phase snapshot helper below recertifies
        // its writer and is deliberately not used as a non-recovery observer.
        assert_eq!(
            native_repair_receiver_content_files(&content_root),
            files_before
        );
        assert_eq!(
            queue.view().unwrap(),
            queue_before,
            "this bounded fixture requires an unchanged A queue cut; unrelated writer progress would fail this condition, not establish a B-authentication defect"
        );
        assert_eq!(
            queue.original_repair(carrier).unwrap(),
            Some(birth_before.clone())
        );
        assert_eq!(birth_before.original_wire_material().unwrap(), material);
        assert_eq!(birth_before.root, birth.root);
        assert!(
            receiver
                .content()
                .original_repair_test_snapshot(&material)
                .unwrap()
                .is_none()
        );
        assert!(
            matches!(receiver.content().read_manifest(command.object_id.as_str(), command.version.as_str()),
            Err(ContentStoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound)
        );
        let probe_calls = runtime
            .rpc_observations()
            .into_iter()
            .filter(|call| {
                call.call_id > last_call
                    && call.source == *wrong_source
                    && call.target == *holder
                    && call.origin == RpcOrigin::ExplicitFixtureRpc
                    && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                    && call.phase == RpcPhase::EndedWithoutResponse
            })
            .collect::<Vec<_>>();
        assert_eq!(probe_calls.len(), 1);
        assert!(
            probe_calls[0]
                .outcome_error
                .as_ref()
                .is_some_and(|error| error.contains(&expected_error))
        );
        assert!(
            matches!(probe_calls[0].signer.as_ref(), Some(sinomo_core::service::ServiceSigner::Remote { node_id, .. }) if node_id == wrong_source.as_str())
        );
        assert!(
            !runtime
                .rpc_observations()
                .iter()
                .any(|call| call.call_id == paused_call_id
                    && matches!(call.phase, RpcPhase::AfterDispatch | RpcPhase::Completed)),
            "the genuine A call must remain paused throughout the negative probe"
        );
        drop(source);
        paused_dispatch.release();
        let terminal = loop {
            assert!(
                Instant::now() <= deadline,
                "same 5s phase budget covers release and genuine A Terminal"
            );
            let snapshot = receiver
                .content()
                .original_repair_test_snapshot(&material)
                .unwrap();
            let completed = runtime.rpc_observations().iter().any(|call| {
                call.call_id == paused_call_id
                    && call.source == *owner
                    && call.target == *holder
                    && call.origin == RpcOrigin::RegisteredHost
                    && call.phase == RpcPhase::Completed
                    && call.outcome_error.is_none()
            });
            assert!(
                Instant::now() <= deadline,
                "late completion cannot satisfy the original phase budget"
            );
            if let Some(snapshot) =
                snapshot.filter(|snapshot| snapshot.terminal_id.is_some() && completed)
            {
                break snapshot;
            }
            let remaining = deadline.checked_duration_since(Instant::now()).unwrap();
            std::thread::sleep(remaining.min(Duration::from_millis(10)));
        };
        assert_eq!(terminal.material, material);
        assert_eq!(terminal.bytes, vec![0x5a; 4096]);
        assert!(terminal.prepared_id < terminal.bound_id.unwrap());
        assert!(terminal.bound_id.unwrap() < terminal.terminal_id.unwrap());
        assert_eq!(
            terminal.response,
            Some(DfsRepairReplicaResponse {
                request_id: command.request_id.clone(),
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id.clone(),
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: holder.clone(),
                result: DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone()
                },
            })
        );
        assert_eq!(
            receiver
                .content()
                .read_object_version(command.object_id.as_str(), command.version.as_str())
                .unwrap()
                .1,
            terminal.bytes
        );
        assert_eq!(runtime.observation_dropped_count(), 0);
        assert!(
            Instant::now() <= deadline,
            "exact Terminal/body checks retain the unchanged 5s phase budget"
        );
        let report = serde_json::json!({
            "qualification": "Actual authenticated B rejection before any C original phase, followed by the SAME previously paused genuine A call and native content-only Terminal. No successor execution, historical delivery, receiver dedup, causal application or abrupt-process proof.",
            "original": birth_before, "actual_carrier": carrier, "paused_call_id": paused_call_id,
            "wrong_source": wrong_source, "native_rejection": error, "rejection_calls": probe_calls.iter().map(|call| format!("{call:?}")).collect::<Vec<_>>(),
            "receiver_content_root": content_root,
            "raw_content_before_and_after_rejection": selected_rejection_file_evidence(&files_before),
            "queue_head_before_and_after_rejection": queue_before.head_sequence,
            "terminal": { "material": terminal.material, "prepared_id": terminal.prepared_id,
                "bound_id": terminal.bound_id, "terminal_id": terminal.terminal_id, "response": terminal.response,
                "store_incarnation": terminal.store_incarnation },
        });
        std::fs::write(
            runtime
                .data_dir(holder)
                .root()
                .join("native-original-authenticated-source.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn native_original_repair_rejects_wrong_authenticated_source_before_genuine_primary_effect() {
        let mut fixture = genuine_native_original_with_receiver_source_probe(
            "native-original-repair-wrong-source",
            false,
            true,
            true,
        );
        drop(fixture.held.take());
        drop(fixture.held_promoted.take());
        fixture.runtime.close().unwrap();
    }

    #[test]
    fn native_original_repair_receiver_records_exact_content_terminal_before_lost_reply() {
        let mut fixture = genuine_native_original_with_receiver_foundation("native-original-repair-terminal", false, true);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let material = fixture.birth.original_wire_material().unwrap();
            let receiver = fixture.runtime.client(&fixture.holder).unwrap();
            let before = receiver.content().original_repair_test_snapshot(&material).unwrap().unwrap();
            assert_eq!(before.material, material);
            assert_eq!(before.bytes, vec![0x5a; 4096]);
            assert!(before.prepared_id < before.bound_id.unwrap());
            assert!(before.bound_id.unwrap() < before.terminal_id.unwrap());
            let command = &material.original.command;
            assert_eq!(before.response, Some(DfsRepairReplicaResponse {
                request_id: command.request_id.clone(), intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id.clone(), object_id: command.object_id.clone(),
                version: command.version.clone(), replica_node_id: fixture.holder.clone(),
                result: DfsBlobTransferResult::Committed { content_hash: command.content_hash.clone() },
            }));
            // This is the real A queue after the actual C response was lost.
            // A's original identity/body/root and uncertain attempt remain.
            let queue = fixture.runtime.selected_original_data_layout(&fixture.shard.primary_node_id).unwrap().queue().unwrap();
            let retained = queue.original_repair_for_slot(&command.intent_id, &fixture.holder).unwrap().unwrap();
            assert_eq!(retained.original_wire_material().unwrap(), material);
            assert_eq!(retained.root, fixture.birth.root);
            assert!(retained.observed.is_empty());
            assert!(retained.closed_at_sequence.is_none());
            // Explicit fixture-only forcing, not production load/performance
            // evidence. The same real catalog/compactor must retain Terminal.
            receiver.content().compact_original_repair_fixture_for_test().unwrap();
            assert_eq!(receiver.content().original_repair_test_snapshot(&material).unwrap(), Some(before.clone()));
            drop(receiver);
            fixture.runtime.stop_node(&fixture.holder).unwrap();
            fixture.runtime.reopen_node(fixture.holder.clone()).unwrap();
            let reopened = fixture.runtime.client(&fixture.holder).unwrap();
            assert_eq!(reopened.content().original_repair_test_snapshot(&material).unwrap(), Some(before));
            assert_eq!(reopened.content().read_object_version(command.object_id.as_str(), command.version.as_str()).unwrap().1, vec![0x5a;4096]);
        }));
        drop(fixture.held.take());
        drop(fixture.held_promoted.take());
        let closed = fixture.runtime.close();
        if let Err(error) = outcome { std::panic::resume_unwind(error); }
        closed.unwrap();
    }

    #[test]
    fn native_selected_original_data_installs_authenticated_nonexecuting_inventory_and_reopens() {
        let mut fixture = genuine_native_original("native-selected-original-data-install");
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let owner = fixture.shard.primary_node_id.clone();
            let layout_a = fixture.runtime.selected_original_data_layout(&owner).unwrap();
            let layout_b = fixture.runtime.selected_original_data_layout(&fixture.promoted).unwrap();
            let request = fixture.birth.root.subject.request_id.clone();
            let mut source = fixture.runtime.metadata_primary_service("/services/selected-original-data.bin").unwrap();
            let fence = source.metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id).unwrap();
            let control = fixture.runtime.shared_control_plane().unwrap();
            let proposed = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .request_metadata_shard_primary_handoff(&fixture.shard, &fixture.promoted)).unwrap().unwrap();
            fixture.runtime.select_genuine_original_before_handoff(&owner, &request, &proposed).unwrap();
            let mut different = proposed.clone(); different.handoff_id.push_str("-another");
            assert!(layout_a.selected_request(&different).is_err());
            drop(fence);
            let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
            let result = source.reconcile_pending_metadata_shard_membership_handoffs(
                &fixture.runtime.metadata_shards().unwrap(), DEFAULT_DFS_POSIX_SYNC_TIMEOUT);
            drop(source);
            if let Err(error) = result {
                assert_eq!(fixture.runtime.observe_completed_native_handoff(&control, &proposed, deadline).unwrap(),
                    Some(proposed.desired_shard.clone()), "actual handoff error: {error}");
            }
            let shard_id = DfsMetadataShardId::new(&fixture.shard.shard_id);
            let marker = loop {
                let marker = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                    fixture.runtime.data_dir(&fixture.promoted), &shard_id, Some(&layout_b)).unwrap();
                if let Some(marker) = marker.filter(|marker| marker.activated_epoch == Some(proposed.desired_shard.epoch)) { break marker; }
                let remaining = deadline.checked_duration_since(Instant::now()).expect("native DATA installation uses unchanged handoff deadline");
                let tick = fixture.runtime.maintenance_tick(&fixture.promoted, remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)).unwrap();
                eprintln!("SELECTED_DATA_REBIND_TICK {:?}; foreign content claims deliberately held", tick.outcome);
            };
            let cut = marker.repair_custody.as_ref().expect("actual activated custody cut");
            assert_eq!(cut.version, 2);
            assert_eq!(cut.handoff.handoff_id, proposed.handoff_id);
            let installed_queue = layout_b.queue().unwrap();
            let installed = installed_queue.installed_original_handoff_data(&owner, &request).unwrap().unwrap();
            let source_directory = crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(&owner), &shard_id)
                .join("journal/repair-handoffs-v1")
                .join(format!("{}-source", blake3::hash(cut.handoff.handoff_id.as_bytes()).to_hex()));
            let actual_source_cut: DfsRepairHandoffCut = crate::metadata_io::read_sobj_optional(&source_directory.join("cut.sobj")).unwrap().unwrap();
            assert_eq!(actual_source_cut, *cut);
            let mut source_rows = Vec::new();
            let mut sealed_birth = None;
            for ordinal in 0..cut.page_count {
                let page: DfsRepairHandoffPage = crate::metadata_io::read_sobj_optional(&source_directory.join(format!("{ordinal}.sobj"))).unwrap().unwrap();
                page.validate_selected_original_data().unwrap();
                assert_eq!(page.cut, *cut);
                assert_eq!(page.ordinal, ordinal);
                if ordinal == 0 { sealed_birth = page.selected_original_data.map(|birth| *birth); }
                source_rows.extend(page.records);
            }
            let sealed_birth = sealed_birth.unwrap();
            assert_eq!(source_rows.len() as u64, cut.record_count);
            assert_eq!(installed.material.birth, sealed_birth);
            assert_eq!(sealed_birth.original, fixture.birth.original);
            assert_eq!(sealed_birth.attachment, fixture.birth.attachment);
            assert_eq!(sealed_birth.root, fixture.birth.root);
            assert_eq!(sealed_birth.captured_data_digest().unwrap(), cut.selected_original_data.as_ref().unwrap().captured_birth_digest);
            assert_eq!(installed.material.cut, *cut);
            let actual_rows = installed_queue.view().unwrap().records;
            assert!(source_rows.iter().all(|row| actual_rows.contains(row)), "every row in the actual filtered source cut survives; unrelated B rows are permitted");
            let DfsNetworkMessage::RepairReplica(command) = &fixture.birth.original.message else { unreachable!() };
            assert!(installed_queue.original_repair_for_slot(&command.intent_id, &fixture.holder).unwrap().is_none());
            assert!(installed_queue.validate_original_repair_dispatch(&fixture.birth.original).is_err());
            assert!(installed_queue.record_original_repair_attempt(&fixture.birth.original).is_err());
            let (clocks, omitted) = layout_b.receiver_clocks().unwrap();
            assert_eq!(omitted, 0); assert!(!clocks.is_empty());
            assert!(clocks.iter().all(|(_, captured, cutoff)| cutoff.duration_since(*captured) == Duration::from_secs(5)));
            assert_eq!(fixture.runtime.observation_dropped_count(), 0);
            assert!(fixture.runtime.rpc_observations().iter().any(|call| call.source == owner
                && call.target == fixture.promoted && call.route == DFS_METADATA_JOURNAL_BATCH_ROUTE
                && call.origin == RpcOrigin::RegisteredHost && call.phase == RpcPhase::Completed));
            // Route evidence and exact host clock records are separate facts;
            // journal RpcObservation currently does not decode request IDs.
            assert!(fixture.runtime.client(&fixture.promoted).unwrap().content()
                .read_object_version(command.object_id.as_str(), command.version.as_str()).is_err(),
                "DATA installation did not copy object bytes or confer B payload custody");
            let receiver_root = crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(&fixture.promoted), &shard_id);
            let mut prior_checkpoint = marker.clone();
            for (index, directory) in ["/services/selected-post-activation-background", "/services/selected-post-activation-foreground"].iter().enumerate() {
                // Actual Main append exercises the native backend -> append ->
                // scheduler context join at unchanged publication thresholds.
                let mut writer = fixture.runtime.client(&fixture.promoted).unwrap();
                let append_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                let previous = writer.set_transport_operation_deadline(Some(append_deadline));
                writer.posix_mkdir(directory).unwrap();
                assert!(Instant::now() <= append_deadline, "post-activation Main append keeps its existing 5s budget");
                assert_eq!(writer.transport.operation_deadline(), Some(append_deadline));
                writer.set_transport_operation_deadline(previous);
                drop(writer);
                let before_forcing = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                    fixture.runtime.data_dir(&fixture.promoted), &shard_id, Some(&layout_b)).unwrap().unwrap();
                assert_eq!(before_forcing, prior_checkpoint,
                    "this small append must not have independently checkpointed before its phase-specific storage control");
                if index == 0 {
                    let _writer = crate::metadata_lock::metadata_write_lock(receiver_root.clone(),
                        crate::metadata_cursor::journal_write_lock_path(&receiver_root)).unwrap();
                    let state = crate::metadata_table_store::read_metadata_table_state(&receiver_root).unwrap();
                    // Explicit fixture-only physical forcing exercises the real
                    // worker/marker writer. It is not production pressure or
                    // quietness/latency acceptance; production limits are unchanged.
                    crate::metadata_table_apply::schedule_metadata_table_checkpoint_with_layout(
                        &receiver_root, &state.tables, 0, state.table_log_offset, None, Some(Arc::clone(&layout_b))).unwrap();
                    drop(_writer);
                    crate::metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(&receiver_root);
                } else {
                    let _writer = crate::metadata_lock::metadata_write_lock(receiver_root.clone(),
                        crate::metadata_cursor::journal_write_lock_path(&receiver_root)).unwrap();
                    let state = crate::metadata_table_store::read_metadata_table_state(&receiver_root).unwrap();
                    let mut tables = state.tables;
                    let mut offset = state.table_log_offset;
                    let mut sequence = state.materialized_through_sequence.unwrap_or(0);
                    let mut fingerprint = state.snapshot_fingerprint;
                    let mut proof = state.prefix_proof;
                    crate::metadata_table_apply::compact_metadata_table_log_if_needed(&receiver_root,
                        crate::metadata_table_apply::TableMaterializationState {
                            selected_original_data_layout: Some(Arc::clone(&layout_b)), read_snapshot_cache: None,
                            tables: &mut tables, table_log_offset: &mut offset, materialized_through_sequence: &mut sequence,
                            table_snapshot_fingerprint: &mut fingerprint, prefix_proof: &mut proof,
                        }, 0).unwrap();
                }
                let _writer = crate::metadata_lock::metadata_write_lock(receiver_root.clone(),
                    crate::metadata_cursor::journal_write_lock_path(&receiver_root)).unwrap();
                let checkpointed = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                    fixture.runtime.data_dir(&fixture.promoted), &shard_id, Some(&layout_b)).unwrap().unwrap();
                let snapshot_path = crate::metadata_table_store::metadata_table_snapshot_path(&receiver_root);
                let snapshot = crate::metadata_snapshot::read_snapshot(&receiver_root, std::fs::File::open(snapshot_path).unwrap()).unwrap();
                assert_eq!(checkpointed.repair_custody.as_ref(), Some(cut));
                assert!(checkpointed.rebind_prefix_proof.journal_sequence > prior_checkpoint.rebind_prefix_proof.journal_sequence,
                    "each forced writer must advance from its immediately preceding marker");
                assert_eq!(Some(checkpointed.rebind_prefix_proof.clone()), snapshot.prefix_proof,
                    "marker names the actual installed checkpoint prefix");
                prior_checkpoint = checkpointed;
            }
            installed_queue.compact_if_needed(crate::DfsRepairQueueCompactionOptions { force: true, ..Default::default() }).unwrap();
            fixture.runtime.stop_node(&fixture.promoted).unwrap();
            installed_queue.reload_from_disk_on_next_access();
            fixture.runtime.reopen_node(fixture.promoted.clone()).unwrap();
            let reopened = fixture.runtime.selected_original_data_layout(&fixture.promoted).unwrap().queue().unwrap();
            let after = reopened.installed_original_handoff_data(&owner, &request).unwrap().unwrap();
            assert_eq!(after, installed);
            let reopened_marker = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                fixture.runtime.data_dir(&fixture.promoted), &shard_id, Some(&layout_b)).unwrap().unwrap();
            assert_eq!(reopened_marker, prior_checkpoint);
            assert!(reopened.validate_original_repair_dispatch(&fixture.birth.original).is_err());
        }));
        let report = serde_json::json!({
            "positive_assertions_passed":outcome.is_ok(), "initial_original":&fixture.birth,
            "qualification":"Native non-executing DATA control only. No foreign B execution, Terminal/root retirement, C1 or availability acceptance. Initial attempt facts may precede the final sealed source cut."
        });
        if let Err(error) = std::fs::write(fixture.runtime.data_dir(&fixture.shard.primary_node_id).root()
            .join("native-selected-data-result.json"), serde_json::to_vec_pretty(&report).unwrap())
        { eprintln!("native DATA report write failed: {error}"); }
        drop(fixture.held.take());
        drop(fixture.held_promoted.take());
        let closed = fixture.runtime.close();
        if let Err(error) = outcome {
            if let Err(close_error) = closed { eprintln!("native DATA cleanup also failed: {close_error}"); }
            std::panic::resume_unwind(error);
        }
        closed.expect("normal native DATA fixture drain");
    }

    #[test]
    #[cfg(unix)]
    fn native_selected_freeze_before_first_capture_reopens_the_same_original_selection() {
        use std::os::unix::fs::MetadataExt as _;
        let path = "/services/selected-original-data.bin";
        let mut fixture = genuine_native_original("native-selected-freeze-before-capture");
        let owner_node = fixture.shard.primary_node_id.clone();
        let layout = fixture.runtime.selected_original_data_layout(&owner_node).unwrap();
        let queue = layout.queue().unwrap();
        let (old_owner, physical_root, owner_file_identity) = {
            let context = fixture.runtime.component_context(&owner_node).unwrap();
            let owner = crate::runtime_host::retain_component_data_owner(&context).unwrap();
            (Arc::downgrade(&owner), owner.physical_data_root().to_path_buf(), owner.storage_owner_file_identity().unwrap())
        };
        let original_directory = std::fs::symlink_metadata(&physical_root).unwrap();
        assert!(original_directory.is_dir() && !original_directory.file_type().is_symlink());
        let directory_identity = (original_directory.dev(), original_directory.ino());
        let mut registration: Option<SelectedPreCaptureRegistration> = None;
        let mut facts = serde_json::json!({
            "pre_capture_observed": false, "old_owner_stopped_without_export": false,
            "same_physical_owner_reopened": false, "first_real_capture_and_import": false,
        });
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let shard = DfsMetadataShardId::new(&fixture.shard.shard_id);
            let request = fixture.birth.root.subject.request_id.clone();
            let mut source = fixture.runtime.metadata_primary_service(path).unwrap();
            let source_scope = source.metadata_backend().unwrap()
                .publication_owner_scope(&fixture.shard.file_system_id, &shard).unwrap();
            let source_root = source_scope.root().to_path_buf();
            let fence = source.metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id).unwrap();
            let control = fixture.runtime.shared_control_plane().unwrap();
            let proposed = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .request_metadata_shard_primary_handoff(&fixture.shard, &fixture.promoted)).unwrap().unwrap();
            fixture.runtime.select_genuine_original_before_handoff(&owner_node, &request, &proposed).unwrap();
            let (observed_tx, observed_rx) = std::sync::mpsc::sync_channel(1);
            registration = Some(SelectedPreCaptureRegistration::install(SelectedPreCaptureStop {
                layout: Arc::clone(&layout), root: source_root.clone(),
                handoff_id: proposed.handoff_id.clone(), request_id: request.clone(), observed: observed_tx,
                matched_invocations: std::sync::atomic::AtomicUsize::new(0),
            }));
            let configs = fixture.runtime.metadata_shards().unwrap();
            let phase_deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
            drop(fence);
            let observed = std::thread::scope(|scope| {
                let producer = scope.spawn(move || source.reconcile_pending_metadata_shard_membership_handoffs(
                    &configs, phase_deadline.saturating_duration_since(Instant::now())));
                let capture_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                let observation = observed_rx.recv_timeout(capture_deadline.saturating_duration_since(Instant::now()));
                // This source cut returns an error rather than blocking an
                // owner through stop. Join even if observation failed.
                let produced = producer.join().expect("interrupted native source joined before stop");
                facts["source_result"] = serde_json::json!(format!("{produced:?}"));
                let observed = observation.expect("actual native source reached freeze before first capture");
                assert!(Instant::now() <= capture_deadline, "pre-capture observation/join keeps the existing 5s budget");
                observed
            });
            assert_eq!(observed.root, source_root);
            assert_eq!(observed.handoff.handoff_id, proposed.handoff_id);
            assert_eq!(layout.selected_request(&observed.handoff).unwrap(), Some(request.clone()));
            assert_eq!(observed.birth.original, fixture.birth.original);
            assert_eq!(observed.birth.attachment, fixture.birth.attachment);
            assert_eq!(observed.birth.root, fixture.birth.root);
            assert!(observed.freeze.activated_epoch.is_none() && observed.freeze.repair_custody.is_none());
            let expected_birth_bytes = crate::metadata_io::encode_sobj(&observed.birth).unwrap();
            facts["pre_capture_observed"] = serde_json::json!(true);
            facts["observed_original"] = serde_json::to_value(&observed.birth).unwrap();
            let export_directory = source_root.join("journal/repair-handoffs-v1")
                .join(format!("{}-source", blake3::hash(proposed.handoff_id.as_bytes()).to_hex()));
            assert_eq!(std::fs::symlink_metadata(&export_directory).unwrap_err().kind(), std::io::ErrorKind::NotFound);
            let freeze_path = source_root.join("journal/membership-handoff-write-freeze.sobj");
            let freeze_bytes = std::fs::read(&freeze_path).unwrap();
            let default_read = crate::read_local_dfs_metadata_shard_write_freeze(fixture.runtime.data_dir(&owner_node), &shard)
                .expect_err("a selected physical root cannot opt a default observer into its pre-cut freeze");
            assert!(matches!(&default_read, DfsError::MetadataRejection {
                code: DfsMetadataRejectionCode::Other, message,
            } if message.contains("default reader rejects selected original physical layout")), "{default_read:?}");
            assert_eq!(std::fs::read(&freeze_path).unwrap(), freeze_bytes);
            let (active, pending) = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .retiring_metadata_shard_handoff_view_before(&fixture.shard.file_system_id, &fixture.shard.shard_id, phase_deadline)).unwrap();
            assert_eq!(active.as_ref(), Some(&fixture.shard));
            assert_eq!(pending.as_ref(), Some(&observed.handoff));
            let mut different = observed.handoff.clone();
            different.handoff_id.push_str("-not-the-selected-handoff");
            assert!(layout.selected_request(&different).is_err());
            assert!(layout.select_original(&owner_node, &request, &proposed).is_err(),
                "even the old caller cannot reselect behind a pending freeze");

            // Keep the fault registered through actual scope/owner shutdown.
            // Layout/queue handles own no OS DATA owner; the Weak must expire.
            fixture.runtime.stop_node(&owner_node).unwrap();
            assert!(old_owner.upgrade().is_none(), "the original native DATA owner actually drained");
            assert_eq!(std::fs::symlink_metadata(&export_directory).unwrap_err().kind(), std::io::ErrorKind::NotFound,
                "no native owner captured while the stop cut remained registered");
            assert_eq!(std::fs::read(&freeze_path).unwrap(), freeze_bytes);
            assert_eq!(crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                fixture.runtime.data_dir(&owner_node), &shard, Some(&layout)).unwrap(), Some(observed.freeze.clone()));
            facts["old_owner_stopped_without_export"] = serde_json::json!(true);
            let stop = registration.take().unwrap();
            facts["matched_pre_capture_invocations"] = serde_json::json!(stop.0.matched_invocations.load(std::sync::atomic::Ordering::Acquire));
            stop.remove_after_owner_stop(&old_owner);
            drop(stop);
            // Force the SAME admitted queue handle through its actual disk
            // replay on native reopen; never create a competing queue owner.
            queue.reload_from_disk_on_next_access();
            let reopen_deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            fixture.runtime.reopen_node_with_startup_deadline(owner_node.clone(), reopen_deadline).unwrap();
            assert!(tokio::time::Instant::now() <= reopen_deadline, "native reopen keeps the existing 5s startup budget");
            let reopened_layout = fixture.runtime.selected_original_data_layout(&owner_node).unwrap();
            assert!(Arc::ptr_eq(&layout, &reopened_layout), "the external fixture expectation survives; it is not decoded from a marker");
            assert_eq!(reopened_layout.selected_request(&observed.handoff).unwrap(), Some(request.clone()));
            {
                let context = fixture.runtime.component_context(&owner_node).unwrap();
                let new_owner = crate::runtime_host::retain_component_data_owner(&context).unwrap();
                assert!(!std::sync::Weak::ptr_eq(&old_owner, &Arc::downgrade(&new_owner)));
                assert_eq!(new_owner.physical_data_root(), physical_root.as_path());
                assert_eq!(new_owner.storage_owner_file_identity().unwrap(), owner_file_identity);
                assert!(Arc::ptr_eq(&new_owner.selected_original_data_layout().unwrap().unwrap(), &layout));
            }
            let reopened_directory = std::fs::symlink_metadata(&physical_root).unwrap();
            assert!(reopened_directory.is_dir() && !reopened_directory.file_type().is_symlink());
            assert_eq!((reopened_directory.dev(), reopened_directory.ino()), directory_identity);
            let reopened_birth = reopened_layout.queue().unwrap().original_repair(&fixture.birth.original).unwrap().unwrap();
            assert_eq!(crate::metadata_io::encode_sobj(&reopened_birth).unwrap(), expected_birth_bytes,
                "actual cold queue replay preserves the complete original and uncertain history");
            facts["same_physical_owner_reopened"] = serde_json::json!(true);
            assert_eq!(fixture.runtime.observe_completed_native_handoff(&control, &proposed, phase_deadline).unwrap(),
                Some(proposed.desired_shard.clone()), "normal native startup must resume the same selected handoff");
            assert!(Instant::now() <= phase_deadline, "stop/reopen does not renew the original 45s handoff deadline");
            let cut: DfsRepairHandoffCut = crate::metadata_io::read_sobj_optional(&export_directory.join("cut.sobj")).unwrap().unwrap();
            cut.validate_selected_original_data().unwrap();
            assert_eq!(cut.handoff, observed.handoff);
            assert!(cut.queue_sequence >= observed.queue_sequence);
            let mut source_rows = Vec::new();
            for ordinal in 0..cut.page_count {
                let page: DfsRepairHandoffPage = crate::metadata_io::read_sobj_optional(
                    &export_directory.join(format!("{ordinal}.sobj"))).unwrap().unwrap();
                page.validate_selected_original_data().unwrap();
                assert_eq!(page.cut, cut);
                assert_eq!(page.ordinal, ordinal);
                if ordinal == 0 {
                    assert_eq!(crate::metadata_io::encode_sobj(page.selected_original_data.as_deref().unwrap()).unwrap(), expected_birth_bytes);
                }
                source_rows.extend(page.records);
            }
            assert_eq!(source_rows.len() as u64, cut.record_count);
            let promoted_layout = fixture.runtime.selected_original_data_layout(&fixture.promoted).unwrap();
            let promoted_queue = promoted_layout.queue().unwrap();
            let installed = loop {
                if let Some(installed) = promoted_queue.installed_original_handoff_data(&owner_node, &request).unwrap() { break installed; }
                let remaining = phase_deadline.checked_duration_since(Instant::now())
                    .expect("first actual selected import keeps the original handoff budget");
                fixture.runtime.maintenance_tick(&fixture.promoted,
                    remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)).unwrap();
            };
            assert!(Instant::now() <= phase_deadline);
            assert_eq!(installed.material.cut, cut);
            assert_eq!(crate::metadata_io::encode_sobj(&installed.material.birth).unwrap(), expected_birth_bytes);
            let imported_rows = promoted_queue.view().unwrap().records;
            assert!(source_rows.iter().all(|row| imported_rows.contains(row)));
            assert!(promoted_queue.validate_original_repair_dispatch(&fixture.birth.original).is_err());
            assert!(promoted_queue.closed_repair_birth(&fixture.birth.root.subject).unwrap().is_none());
            facts["first_real_capture_and_import"] = serde_json::json!(true);
            assert_eq!(fixture.runtime.observation_dropped_count(), 0);
        }));
        let report = serde_json::json!({
            "assertions_passed": outcome.is_ok(), "initial_original": &fixture.birth, "facts": facts,
            "qualification": "Controlled post-freeze interruption and native component/OS-owner restart with externally retained selected layout. No abrupt process loss, missing-selection recovery, serving floor or successor execution proof."
        });
        if let Err(error) = std::fs::write(fixture.runtime.data_dir(&owner_node).root()
            .join("native-selected-freeze-before-capture.json"), serde_json::to_vec_pretty(&report).unwrap())
        { eprintln!("native selected pre-capture report write failed: {error}"); }
        drop(fixture.held.take());
        drop(fixture.held_promoted.take());
        let closed = fixture.runtime.close();
        // On failed owner drain, retain the exact negative registration rather
        // than silently allowing a surviving source to capture during cleanup.
        if let Some(stop) = registration.take() {
            if closed.is_ok() && old_owner.upgrade().is_none() { stop.remove_after_owner_stop(&old_owner); }
            else { eprintln!("pre-capture fault retained because old DATA owner did not drain"); }
        }
        if let Err(error) = outcome {
            if let Err(close_error) = closed { eprintln!("native pre-capture restart cleanup also failed: {close_error}"); }
            std::panic::resume_unwind(error);
        }
        closed.expect("normal native pre-capture restart fixture drain");
    }

    #[test]
    fn native_selected_install_expiry_after_marker_sync_retains_data_and_cold_rebinds() {
        let path = "/services/selected-original-data.bin";
        let mut fixture = genuine_native_original("native-selected-install-marker-expiry");
        let mut facts = serde_json::json!({
            "actual_marker_hook_reached": false, "expired_instance_remains_fenced": false,
            "exact_import_cursor_marker_retained": false, "fresh_cold_rebind_completed": false,
        });
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let owner = fixture.shard.primary_node_id.clone();
            let promoted = fixture.promoted.clone();
            let shard = DfsMetadataShardId::new(&fixture.shard.shard_id);
            let layout = fixture.runtime.selected_original_data_layout(&promoted).unwrap();
            let request = fixture.birth.root.subject.request_id.clone();
            let mut source = fixture.runtime.metadata_primary_service(path).unwrap();
            let fence = source.metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id).unwrap();
            let control = fixture.runtime.shared_control_plane().unwrap();
            let proposed = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .request_metadata_shard_primary_handoff(&fixture.shard, &promoted)).unwrap().unwrap();
            fixture.runtime.select_genuine_original_before_handoff(&owner, &request, &proposed).unwrap();
            drop(fence);
            let handoff_deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
            let produced = source.reconcile_pending_metadata_shard_membership_handoffs(
                &fixture.runtime.metadata_shards().unwrap(), handoff_deadline.saturating_duration_since(Instant::now()));
            drop(source);
            assert_eq!(fixture.runtime.observe_completed_native_handoff(&control, &proposed, handoff_deadline).unwrap(),
                Some(proposed.desired_shard.clone()), "genuine source handoff result: {produced:?}");
            loop {
                let marker = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                    fixture.runtime.data_dir(&promoted), &shard, Some(&layout)).unwrap();
                if marker.is_some_and(|marker| marker.activated_epoch == Some(proposed.desired_shard.epoch)) { break; }
                let remaining = handoff_deadline.checked_duration_since(Instant::now())
                    .expect("initial selected installation keeps the original 45s budget");
                fixture.runtime.maintenance_tick(&promoted, remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)).unwrap();
            }
            assert!(Instant::now() <= handoff_deadline);
            let active = fixture.runtime.metadata_shards().unwrap().into_iter()
                .find(|current| current.file_system_id == fixture.shard.file_system_id
                    && current.shard_id == fixture.shard.shard_id).unwrap();
            assert_eq!(active, proposed.desired_shard);
            let pending = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard_membership_handoff(&active.file_system_id, &active.shard_id)).unwrap();
            assert!(pending.is_none());
            let proof = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard_membership_handoff_proof(&active.file_system_id, &active.shard_id)).unwrap().unwrap();
            assert_eq!(proof.handoff_id, proposed.handoff_id);
            assert_eq!(proof.desired_shard, active);

            // Real native B admission excludes competing same-shard work while
            // a separate cold handle exercises the actual file-backed writer.
            let native = fixture.runtime.metadata_primary_service(path).unwrap();
            let admission = native.acquire_prepared_execution(&DfsPreparedOperationScope {
                file_system_id: active.file_system_id.clone(), shard_id: shard.clone(),
                owner_epoch: active.epoch, operation_id: request.clone(),
            }).unwrap();
            let scope = native.metadata_backend().unwrap().publication_owner_scope(&active.file_system_id, &shard).unwrap();
            assert!(native.publication_execution.current().is_some_and(|execution|
                execution.admits(&scope, &active.file_system_id, &shard)));
            let root = crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(&promoted), &shard);
            assert_eq!(scope.root(), root.as_path());
            let queue = layout.queue().unwrap();
            let imported = queue.installed_original_handoff_data(&owner, &request).unwrap().unwrap();
            assert_eq!(imported.material.birth.original, fixture.birth.original);
            assert_eq!(imported.material.birth.attachment, fixture.birth.attachment);
            assert_eq!(imported.material.birth.root, fixture.birth.root);
            let before_queue = queue.view().unwrap();
            let before_marker = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                fixture.runtime.data_dir(&promoted), &shard, Some(&layout)).unwrap().unwrap();
            assert_eq!(before_marker.repair_custody.as_ref(), Some(&imported.material.cut));
            let cursor_path = root.join("journal/repair-handoffs-v1")
                .join(format!("{}-received", blake3::hash(proposed.handoff_id.as_bytes()).to_hex()))
                .join("received.sobj");
            let before_cursor = std::fs::read(&cursor_path).unwrap();
            let source_queue = fixture.runtime.selected_original_data_layout(&owner).unwrap().queue().unwrap();
            let source_before = source_queue.original_repair(&fixture.birth.original).unwrap().unwrap();
            let mut cold = LocalSobjMetadataBackend::open_for_inspection_with_layout(
                shard.clone(), root.clone(), Some((active.file_system_id.clone(), Arc::clone(&layout)))).unwrap();
            assert!(cold.metadata_shard_instance_writes_frozen(&shard).unwrap());
            let prefix = DfsMetadataBackend::metadata_prefix_proof(&cold, &shard).unwrap().unwrap();

            struct ClearSelectedInstallHook;
            impl Drop for ClearSelectedInstallHook {
                fn drop(&mut self) {
                    crate::metadata_backend::SELECTED_INSTALL_AFTER_MARKER_SYNC.with(|slot| { slot.borrow_mut().take(); });
                }
            }
            let reached = std::rc::Rc::new(std::cell::Cell::new(false));
            let observed = std::rc::Rc::clone(&reached);
            crate::metadata_backend::SELECTED_INSTALL_AFTER_MARKER_SYNC.with(|slot| {
                assert!(slot.borrow().is_none());
                *slot.borrow_mut() = Some((root.clone(), proposed.handoff_id.clone(), Box::new(move |deadline| {
                    observed.set(true);
                    // The native owner already captured its real 5s cap. Delay
                    // this physical boundary; never supply a shorter deadline.
                    std::thread::sleep(deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5));
                })));
            });
            let clear_hook = ClearSelectedInstallHook;
            let expired = rebind_completed_metadata_shard_membership_handoff(
                &mut cold, &promoted, &active, None, Some(&proof));
            facts["expiry_result"] = serde_json::json!(format!("{expired:?}"));
            facts["actual_marker_hook_reached"] = serde_json::json!(reached.get());
            assert!(reached.get(), "the actual synchronized selected marker boundary must execute");
            assert!(matches!(&expired, Err(DfsError::MetadataRejection {
                code: DfsMetadataRejectionCode::Other, message,
            }) if message.contains("expired after marker sync")), "{expired:?}");
            assert!(cold.metadata_shard_instance_writes_frozen(&shard).unwrap(),
                "a durable activated marker cannot make the rejected cold instance ready");
            facts["expired_instance_remains_fenced"] = serde_json::json!(true);
            assert_eq!(DfsMetadataBackend::metadata_prefix_proof(&cold, &shard).unwrap(), Some(prefix));
            assert_eq!(queue.view().unwrap(), before_queue);
            assert_eq!(queue.installed_original_handoff_data(&owner, &request).unwrap(), Some(imported.clone()));
            assert_eq!(std::fs::read(&cursor_path).unwrap(), before_cursor);
            assert_eq!(crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                fixture.runtime.data_dir(&promoted), &shard, Some(&layout)).unwrap(), Some(before_marker.clone()));
            facts["exact_import_cursor_marker_retained"] = serde_json::json!(true);
            drop(clear_hook);
            drop(cold);

            // Fresh handle; same physical owner, actual control proof and
            // immutable imported A birth. No new original/cursor is allocated.
            let mut reopened = LocalSobjMetadataBackend::open_for_inspection_with_layout(
                shard.clone(), root, Some((active.file_system_id.clone(), Arc::clone(&layout)))).unwrap();
            assert!(reopened.metadata_shard_instance_writes_frozen(&shard).unwrap());
            let rebind_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            assert!(rebind_completed_metadata_shard_membership_handoff(
                &mut reopened, &promoted, &active, None, Some(&proof)).unwrap());
            assert!(Instant::now() <= rebind_deadline, "fresh rebind keeps the existing 5s operation bound");
            assert!(!reopened.metadata_shard_instance_writes_frozen(&shard).unwrap());
            assert_eq!(queue.view().unwrap(), before_queue);
            assert_eq!(queue.installed_original_handoff_data(&owner, &request).unwrap(), Some(imported));
            assert_eq!(std::fs::read(cursor_path).unwrap(), before_cursor);
            assert_eq!(reopened.metadata_shard_write_freeze(&shard).unwrap(), Some(before_marker));
            assert!(queue.validate_original_repair_dispatch(&fixture.birth.original).is_err());
            assert!(queue.closed_repair_birth(&fixture.birth.root.subject).unwrap().is_none());
            assert_eq!(source_queue.original_repair(&fixture.birth.original).unwrap(), Some(source_before));
            facts["fresh_cold_rebind_completed"] = serde_json::json!(true);
            drop(reopened);
            drop(admission);
            drop(native);
        }));
        let report = serde_json::json!({
            "assertions_passed": outcome.is_ok(), "initial_original": &fixture.birth, "facts": facts,
            "qualification": "Native-created selected DATA plus file-backed cold-instance rebind. No first-install crash, process loss, post-replay expiry, successor execution or abrupt-loss proof."
        });
        if let Err(error) = std::fs::write(fixture.runtime.data_dir(&fixture.promoted).root()
            .join("native-selected-install-marker-expiry.json"), serde_json::to_vec_pretty(&report).unwrap())
        { eprintln!("native selected install-expiry report write failed: {error}"); }
        drop(fixture.held.take());
        drop(fixture.held_promoted.take());
        let closed = fixture.runtime.close();
        if let Err(error) = outcome {
            if let Err(close_error) = closed { eprintln!("native selected install-expiry cleanup also failed: {close_error}"); }
            std::panic::resume_unwind(error);
        }
        closed.expect("normal selected install-expiry fixture drain");
    }

    #[test]
    fn native_selected_imported_executor_requires_b_payload_and_retains_disabled_origin() {
        native_selected_imported_executor_control(false);
    }

    #[test]
    fn native_selected_imported_executor_rejects_immutable_forbidden_b_before_retention() {
        native_selected_imported_executor_control(true);
    }

    fn native_selected_imported_executor_control(forbid_promoted: bool) {
        let path = "/services/selected-original-data.bin";
        let name = if forbid_promoted {
            "native-selected-forbidden-disabled-executor"
        } else {
            "native-selected-disabled-executor"
        };
        let mut fixture = genuine_native_original_with_forbidden_promoted(name, forbid_promoted);
        let mut facts = serde_json::json!({
            "immutable_forbidden_b_requested":forbid_promoted,
            "immutable_forbidden_b_rejected_before_retention":false,
            "missing_b_payload_rejected":false,
            "unrelated_main_work_completed":false,
            "fixture_prepared_b_bytes":false,
            "retiring_assignment_rejected":false,
            "native_b_associated":false,
            "native_b_reopen_duplicate_matched":false,
            "dispatch_observation_qualification":"held_promoted pre-claim guard remains installed; no unguarded scheduler coverage"
        });
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let owner = fixture.shard.primary_node_id.clone();
            let promoted = fixture.promoted.clone();
            let layout_b = fixture
                .runtime
                .selected_original_data_layout(&promoted)
                .unwrap();
            let request = fixture.birth.root.subject.request_id.clone();
            let mut source = fixture.runtime.metadata_primary_service(path).unwrap();
            let fence = source
                .metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id)
                .unwrap();
            let control = fixture.runtime.shared_control_plane().unwrap();
            let proposed = control
                .with_client(|kv| {
                    DfsControlPlaneStore::new(kv.as_mut())
                        .request_metadata_shard_primary_handoff(&fixture.shard, &promoted)
                })
                .unwrap()
                .unwrap();
            fixture
                .runtime
                .select_genuine_original_before_handoff(&owner, &request, &proposed)
                .unwrap();
            drop(fence);
            let handoff_deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
            let result = source.reconcile_pending_metadata_shard_membership_handoffs(
                &fixture.runtime.metadata_shards().unwrap(),
                handoff_deadline.saturating_duration_since(Instant::now()),
            );
            drop(source);
            if let Err(error) = result {
                assert_eq!(
                    fixture
                        .runtime
                        .observe_completed_native_handoff(&control, &proposed, handoff_deadline,)
                        .unwrap(),
                    Some(proposed.desired_shard.clone()),
                    "actual handoff error: {error}"
                );
            }
            let shard_id = DfsMetadataShardId::new(&fixture.shard.shard_id);
            let marker = loop {
                let marker =
                    crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                        fixture.runtime.data_dir(&promoted),
                        &shard_id,
                        Some(&layout_b),
                    )
                    .unwrap();
                if let Some(marker) = marker
                    .filter(|marker| marker.activated_epoch == Some(proposed.desired_shard.epoch))
                {
                    break marker;
                }
                let remaining = handoff_deadline
                    .checked_duration_since(Instant::now())
                    .expect("disabled association retains the existing 45s handoff budget");
                let tick = fixture
                    .runtime
                    .maintenance_tick(
                        &promoted,
                        remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                    )
                    .unwrap();
                eprintln!("SELECTED_ASSOCIATION_REBIND_TICK {:?}", tick.outcome);
            };
            assert!(
                Instant::now() <= handoff_deadline,
                "handoff did not renew its 45s caller budget"
            );
            let assignment = proposed.desired_shard.clone();
            assert_eq!(
                fixture
                    .runtime
                    .metadata_shards()
                    .unwrap()
                    .iter()
                    .find(|shard| shard.file_system_id == assignment.file_system_id
                        && shard.shard_id == assignment.shard_id),
                Some(&assignment)
            );
            let queue = layout_b.queue().unwrap();
            let installed = queue
                .installed_original_handoff_data(&owner, &request)
                .unwrap()
                .unwrap();
            assert_eq!(
                marker.repair_custody.as_ref(),
                Some(&installed.material.cut)
            );
            assert_eq!(installed.material.birth.original, fixture.birth.original);
            assert_eq!(
                installed.material.birth.attachment,
                fixture.birth.attachment
            );
            assert_eq!(installed.material.birth.root, fixture.birth.root);
            let DfsNetworkMessage::RepairReplica(command) = &fixture.birth.original.message else {
                panic!("genuine original Repair");
            };
            assert_eq!(
                command.authority_shard.as_ref().unwrap().primary_node_id,
                owner
            );
            assert!(command.authority_shard.as_ref().unwrap().epoch < assignment.epoch);
            let source_queue = fixture
                .runtime
                .selected_original_data_layout(&owner)
                .unwrap()
                .queue()
                .unwrap();
            let source_before = source_queue
                .original_repair(&fixture.birth.original)
                .unwrap()
                .unwrap();
            let original_digest = crate::repair_queue_store::original_repair_digest(
                &fixture.birth.original,
                &fixture.birth.attachment,
            )
            .unwrap();
            assert_eq!(
                original_digest,
                installed.material.birth.root.original_command_digest
            );
            let missing_manifest = fixture
                .runtime
                .data_dir(&promoted)
                .dfs_dir()
                .join("content/objects")
                .join(command.object_id.as_str())
                .join(command.version.as_str())
                .join("manifest.sobj");
            let captured_absence = fixture
                .runtime
                .client(&promoted)
                .unwrap()
                .content()
                .read_manifest(command.object_id.as_str(), command.version.as_str())
                .expect_err("actual metadata installation leaves this exact B manifest absent");
            match &captured_absence {
                ContentStoreError::Io { path, source } => {
                    assert_eq!(path, &missing_manifest);
                    assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
                }
                other => panic!(
                    "corruption/unsupported storage is not a missing-payload control: {other:?}"
                ),
            }
            let expected_missing = DfsError::from(captured_absence);

            let mut successor = fixture.runtime.metadata_primary_service(path).unwrap();
            let association_scope = DfsPreparedOperationScope {
                file_system_id: assignment.file_system_id.clone(),
                shard_id: shard_id.clone(),
                owner_epoch: assignment.epoch,
                operation_id: request.clone(),
            };
            let missing_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            // Use the real retained native provider, never a test-made owner
            // or permit. Holding this exact admission also excludes the same
            // item's maintenance writer from the before/after observations.
            let missing_admission = successor
                .acquire_prepared_execution(&association_scope)
                .unwrap();
            let before_missing = queue.view().unwrap();
            if forbid_promoted {
                // This prohibition was supplied to the real A write before
                // its original Repair existed, then transferred unchanged.
                // Do not manufacture prohibited B bytes to exercise the guard.
                let intent = successor
                    .metadata_backend()
                    .unwrap()
                    .get_intent_on_shard(&shard_id, &command.object_id, &command.intent_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
                assert!(intent.policy.forbidden_node_ids.contains(&promoted));
                let error = successor
                    .associate_installed_repair_executor_disabled(&assignment, &owner, &request)
                    .expect_err("immutable intent forbids B before any content-root retention");
                match error {
                    DfsError::InvalidReplicationPolicy { message } => assert_eq!(
                        message,
                        "immutable Repair intent forbids disabled executor retention on this node"
                    ),
                    other => panic!("native authority or payload error is not the policy rejection: {other:?}"),
                }
                assert!(Instant::now() <= missing_deadline);
                assert_eq!(queue.view().unwrap(), before_missing);
                assert!(queue.imported_executor_association(&owner, &request).unwrap().is_none());
                assert_eq!(
                    queue.installed_original_handoff_data(&owner, &request).unwrap().unwrap(),
                    installed
                );
                assert_eq!(
                    source_queue.original_repair(&fixture.birth.original).unwrap().unwrap(),
                    source_before
                );
                assert_eq!(
                    crate::repair_queue_store::original_repair_digest(
                        &fixture.birth.original, &fixture.birth.attachment,
                    ).unwrap(),
                    original_digest
                );
                assert!(!missing_manifest.exists());
                facts["immutable_forbidden_b_rejected_before_retention"] = serde_json::json!(true);
                return;
            }
            let missing = successor
                .associate_installed_repair_executor_disabled(&assignment, &owner, &request)
                .expect_err("a native B executor still needs its exact local payload");
            assert!(
                Instant::now() <= missing_deadline,
                "missing-payload call keeps the existing 5s operation bound"
            );
            match (missing, expected_missing) {
                (
                    DfsError::ContentStore { message: actual },
                    DfsError::ContentStore { message: expected },
                ) => {
                    assert_eq!(
                        actual, expected,
                        "association must return the exact captured B manifest absence"
                    );
                }
                (actual, expected) => panic!(
                    "native authority/corruption failure is not the captured absence: actual={actual:?}, expected={expected:?}"
                ),
            }
            assert_eq!(queue.view().unwrap(), before_missing);
            assert!(
                queue
                    .imported_executor_association(&owner, &request)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                queue
                    .installed_original_handoff_data(&owner, &request)
                    .unwrap()
                    .unwrap(),
                installed
            );
            facts["missing_b_payload_rejected"] = serde_json::json!(true);
            drop(missing_admission);
            drop(successor);

            // Missing content affects this local join only. Exercise an actual
            // unrelated Main mutation at the unchanged network deadline.
            let mut unrelated = fixture.runtime.client(&promoted).unwrap();
            let unrelated_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let previous = unrelated.set_transport_operation_deadline(Some(unrelated_deadline));
            unrelated
                .posix_mkdir("/services/selected-association-unrelated")
                .unwrap();
            assert!(Instant::now() <= unrelated_deadline);
            assert_eq!(
                unrelated.transport.operation_deadline(),
                Some(unrelated_deadline)
            );
            unrelated.set_transport_operation_deadline(previous);
            drop(unrelated);
            facts["unrelated_main_work_completed"] = serde_json::json!(true);

            // Explicit fixture precondition, not A->B/C->B transport evidence,
            // a receipt, metadata publication or an original-origin rewrite.
            let holder = fixture.runtime.client(&fixture.holder).unwrap();
            let exact_bytes = holder
                .content()
                .read_object_version(command.object_id.as_str(), command.version.as_str())
                .unwrap()
                .1;
            assert_eq!(crate::content_hash(&exact_bytes), command.content_hash);
            assert_eq!(
                exact_bytes.len() as u64,
                fixture.birth.attachment.total_size_bytes
            );
            drop(holder);
            let content_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let b_content = LocalContentStore::new(
                fixture
                    .runtime
                    .data_dir(&promoted)
                    .dfs_dir()
                    .join("content"),
            )
            .unwrap();
            let prepared = b_content
                .put_object_version(
                    command.object_id.as_str(),
                    command.version.as_str(),
                    &exact_bytes,
                )
                .unwrap();
            assert!(Instant::now() <= content_deadline);
            assert_eq!(prepared.content_hash, command.content_hash);
            assert_eq!(
                prepared.size_bytes,
                fixture.birth.attachment.total_size_bytes
            );
            facts["fixture_prepared_b_bytes"] = serde_json::json!(true);

            // The real A->B assignment change has already completed. The
            // retiring A assignment cannot authorize B's new association even
            // now that B has qualifying bytes. No second handoff is invented.
            let mut successor = fixture.runtime.metadata_primary_service(path).unwrap();
            assert!(
                successor
                    .metadata_backend()
                    .unwrap()
                    .get_receipt_on_shard(
                        &shard_id,
                        &command.object_id,
                        &command.version,
                        &promoted,
                    )
                    .unwrap()
                    .is_none(),
                "fixture-prepared B bytes do not mint a receipt"
            );
            let stale_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let stale_admission = successor
                .acquire_prepared_execution(&association_scope)
                .unwrap();
            let before_stale = queue.view().unwrap();
            assert!(
                successor
                    .associate_installed_repair_executor_disabled(&fixture.shard, &owner, &request,)
                    .is_err()
            );
            assert_eq!(queue.view().unwrap(), before_stale);
            assert!(
                queue
                    .imported_executor_association(&owner, &request)
                    .unwrap()
                    .is_none()
            );
            assert!(Instant::now() <= stale_deadline);
            facts["retiring_assignment_rejected"] = serde_json::json!(true);
            drop(stale_admission);

            let b_capture = fixture
                .runtime
                .capture_checkpointed_background_repairs(&promoted, 8)
                .unwrap();
            let association_deadline =
                Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let association_admission = successor
                .acquire_prepared_execution(&association_scope)
                .unwrap();
            let before_association = queue.view().unwrap();
            let association = successor
                .associate_installed_repair_executor_disabled(&assignment, &owner, &request)
                .unwrap()
                .expect("actual native B admission binds the qualified local payload");
            assert!(Instant::now() <= association_deadline);
            assert_eq!(
                association.local_root().subject,
                installed.material.birth.root.subject
            );
            assert_eq!(
                association.local_root().original_command_digest,
                original_digest
            );
            assert_eq!(
                association.local_root().source,
                installed.material.birth.root.source
            );
            assert_ne!(
                association.local_root().store_incarnation,
                installed.material.birth.root.store_incarnation
            );
            b_content
                .inspect_repair_payload_root(association.local_root())
                .unwrap();
            let after_association = queue.view().unwrap();
            assert!(after_association.head_sequence > before_association.head_sequence);
            assert_eq!(after_association.records, before_association.records);
            drop(association_admission);
            let duplicate_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let duplicate_admission = successor
                .acquire_prepared_execution(&association_scope)
                .unwrap();
            let before_duplicate = queue.view().unwrap();
            assert_eq!(
                successor
                    .associate_installed_repair_executor_disabled(&assignment, &owner, &request,)
                    .unwrap(),
                Some(association.clone())
            );
            assert!(Instant::now() <= duplicate_deadline);
            assert_eq!(queue.view().unwrap(), before_duplicate);
            assert_eq!(
                queue
                    .installed_original_handoff_data(&owner, &request)
                    .unwrap()
                    .unwrap(),
                installed
            );
            assert_eq!(
                source_queue
                    .original_repair(&fixture.birth.original)
                    .unwrap()
                    .unwrap(),
                source_before
            );
            assert!(
                successor
                    .metadata_backend()
                    .unwrap()
                    .get_receipt_on_shard(
                        &shard_id,
                        &command.object_id,
                        &command.version,
                        &promoted,
                    )
                    .unwrap()
                    .is_none(),
                "disabled association never publishes the fixture bytes"
            );
            facts["native_b_associated"] = serde_json::json!(true);
            facts["association_data"] = serde_json::to_value(&association).unwrap();

            // The immutable A command is not restamped. Only a candidate B
            // carrier envelope is changed here, and every native lookup must
            // keep refusing it; no carrier is handed to any transport.
            let mut carrier = fixture.birth.original.clone();
            carrier.source_node_id = promoted.clone();
            carrier.frame_id += 1;
            carrier.message_id += 1;
            assert!(
                queue
                    .original_repair_for_slot(&command.intent_id, &fixture.holder)
                    .unwrap()
                    .is_none()
            );
            assert!(queue.original_repair(&carrier).unwrap().is_none());
            assert!(queue.record_original_repair_attempt(&carrier).is_err());
            assert!(queue.validate_original_repair_dispatch(&carrier).is_err());
            assert!(
                queue
                    .closed_repair_birth(&installed.material.birth.root.subject)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(queue.view().unwrap(), before_duplicate);
            // Guarded absence only: held_promoted remains installed. The
            // native lookup/attempt rejections above independently prove that
            // association DATA grants no dispatch permit; this is not an
            // unguarded native scheduling/executor liveness control.
            assert_eq!(b_capture.omitted_count(), 0);
            assert!(
                b_capture
                    .records()
                    .iter()
                    .all(|captured| match &captured.message.message {
                        DfsNetworkMessage::RepairReplica(actual) => actual.request_id != request,
                        _ => true,
                    })
            );
            assert_eq!(fixture.runtime.observation_dropped_count(), 0);
            assert!(
                !fixture
                    .runtime
                    .rpc_observations()
                    .iter()
                    .any(|call| call.source == promoted
                        && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                        && call.dfs_request_id.as_deref() == Some(request.as_str()))
            );
            drop(b_capture); // Do not retain an observation DATA owner across stop/reopen.
            drop(b_content);
            drop(duplicate_admission);
            drop(successor);

            queue
                .compact_if_needed(crate::DfsRepairQueueCompactionOptions {
                    force: true,
                    ..Default::default()
                })
                .unwrap();
            fixture.runtime.stop_node(&promoted).unwrap();
            queue.reload_from_disk_on_next_access();
            fixture.runtime.reopen_node(promoted.clone()).unwrap();
            let reopened = fixture
                .runtime
                .selected_original_data_layout(&promoted)
                .unwrap()
                .queue()
                .unwrap();
            assert_eq!(
                reopened
                    .imported_executor_association(&owner, &request)
                    .unwrap(),
                Some(association.clone())
            );
            assert_eq!(
                reopened
                    .installed_original_handoff_data(&owner, &request)
                    .unwrap()
                    .unwrap(),
                installed
            );
            assert!(
                reopened
                    .validate_original_repair_dispatch(&carrier)
                    .is_err()
            );
            assert!(
                reopened
                    .closed_repair_birth(&installed.material.birth.root.subject)
                    .unwrap()
                    .is_none()
            );
            let reopen_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let mut current = fixture.runtime.metadata_primary_service(path).unwrap();
            let reopen_admission = current
                .acquire_prepared_execution(&association_scope)
                .unwrap();
            let before_reopen_duplicate = reopened.view().unwrap();
            assert_eq!(
                current
                    .associate_installed_repair_executor_disabled(&assignment, &owner, &request,)
                    .unwrap(),
                Some(association)
            );
            assert_eq!(
                reopened.view().unwrap(),
                before_reopen_duplicate,
                "native reopen duplicate must not append or rewrite queue DATA"
            );
            assert!(Instant::now() <= reopen_deadline);
            drop(reopen_admission);
            // The same held_promoted guard also qualifies this observation.
            assert_eq!(fixture.runtime.observation_dropped_count(), 0);
            assert!(
                !fixture
                    .runtime
                    .rpc_observations()
                    .iter()
                    .any(|call| call.source == promoted
                        && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                        && call.dfs_request_id.as_deref() == Some(request.as_str()))
            );
            assert_eq!(
                source_queue
                    .original_repair(&fixture.birth.original)
                    .unwrap()
                    .unwrap(),
                source_before
            );
            facts["native_b_reopen_duplicate_matched"] = serde_json::json!(true);
        }));
        let report = serde_json::json!({
            "native_assertions_passed":outcome.is_ok(), "initial_original":&fixture.birth,
            "facts":facts,
            "qualification":if forbid_promoted {
                "Native immutable-prohibition rejection only. The original A write committed the forbidden-B policy before its genuine Repair existed. After authenticated DATA installation/current B admission, the exact policy rejection precedes absent B content; no B bytes were prepared. No association, successor dispatch, receiver Terminal/dedup, causal application, root retirement, C1 or availability proof."
            } else {
                "Native disabled-association DATA control. B bytes are fixture-prepared through the actual local writer, not transferred. A->B retiring-role rejection is not a second reconfiguration trace. Absence of B dispatch is observed with held_promoted installed; native lookup/attempt rejections do not claim unguarded scheduler coverage. No successor dispatch, receiver Terminal/dedup, causal application, root retirement, C1 or availability proof."
            }
        });
        if let Err(error) = std::fs::write(
            fixture
                .runtime
                .data_dir(&fixture.shard.primary_node_id)
                .root()
                .join("native-selected-association-result.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        ) {
            eprintln!("native disabled association report write failed: {error}");
        }
        drop(fixture.held.take());
        drop(fixture.held_promoted.take());
        let closed = fixture.runtime.close();
        if let Err(error) = outcome {
            if let Err(close_error) = closed {
                eprintln!("native disabled association cleanup also failed: {close_error}");
            }
            std::panic::resume_unwind(error);
        }
        closed.expect("normal native disabled association fixture drain");
    }

    fn receiver_custody_files(root: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        fn visit(base: &Path, path: &Path, result: &mut Vec<(std::path::PathBuf, Vec<u8>)>) {
            if !path.exists() { return; }
            if path.is_file() {
                result.push((path.strip_prefix(base).unwrap().to_path_buf(), std::fs::read(path).unwrap()));
            } else {
                for entry in std::fs::read_dir(path).unwrap() { visit(base, &entry.unwrap().path(), result); }
            }
        }
        let mut result = Vec::new();
        for relative in ["journal/membership-handoff-write-freeze.sobj", "journal/repair-handoffs-v1", "coordination/metadata-prefix-ack-floors"] {
            visit(root, &root.join(relative), &mut result);
        }
        result.sort_by(|a,b| a.0.cmp(&b.0)); result
    }

    /// The real A protocol creates the frozen export. Before-poll expiry pauses
    /// that source before its first send; the after-processing control retains
    /// its decoded receiver pause. Neither gate supplies a successful reply.
    /// Supplying an argument to the host method is not transport authentication;
    /// the separate positive control exercises that wire boundary.
    fn native_selected_receiver_clock_control(after_processing: bool) {
        let name = if after_processing { "native-selected-clock-after-processing" } else { "native-selected-clock-before-poll" };
        let mut fixture = genuine_native_original(name);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let owner = fixture.shard.primary_node_id.clone();
            let mut source = fixture.runtime.metadata_primary_service("/services/selected-original-data.bin").unwrap();
            let fence = source.metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id).unwrap();
            let control = fixture.runtime.shared_control_plane().unwrap();
            let proposed = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .request_metadata_shard_primary_handoff(&fixture.shard, &fixture.promoted)).unwrap().unwrap();
            fixture.runtime.select_genuine_original_before_handoff(&owner, &fixture.birth.root.subject.request_id, &proposed).unwrap();
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let paused_pages = Arc::new(Mutex::new(Vec::new()));
            if after_processing {
                fixture.runtime.install_fault(FaultRule {
                    source: Some(owner.clone()), target: Some(fixture.promoted.clone()),
                    route: Some(DFS_METADATA_JOURNAL_BATCH_ROUTE.into()), call_id: None,
                    origin: Some(RpcOrigin::RegisteredHost), remaining_hits: 16,
                    action: FaultAction::PauseSelectedOriginalCustody {
                        handoff_id: proposed.handoff_id.clone(), gate: Arc::clone(&gate),
                        captured: Arc::clone(&paused_pages),
                    },
                });
            }
            let configs = fixture.runtime.metadata_shards().unwrap();
            let phase_deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
            let (source_observed, source_observation) = std::sync::mpsc::sync_channel(1);
            let source_pause = (!after_processing).then(|| SelectedSourceExportRegistration::install(SelectedSourceExportPause {
                layout: fixture.runtime.selected_original_data_layout(&owner).unwrap(),
                root: crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(&owner),
                    &DfsMetadataShardId::new(&fixture.shard.shard_id)),
                handoff_id: proposed.handoff_id.clone(), deadline: phase_deadline,
                observed: source_observed, observation_sent: std::sync::atomic::AtomicBool::new(false),
                released: std::sync::Mutex::new(false), changed: std::sync::Condvar::new(),
            }));
            drop(fence);
            std::thread::scope(|scope| {
                let producer = scope.spawn(move || source.reconcile_pending_metadata_shard_membership_handoffs(
                    &configs, DEFAULT_DFS_POSIX_SYNC_TIMEOUT));
                let checks = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let shard = DfsMetadataShardId::new(&fixture.shard.shard_id);
                    let source_directory = crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(&owner), &shard)
                        .join("journal/repair-handoffs-v1")
                        .join(format!("{}-source", blake3::hash(proposed.handoff_id.as_bytes()).to_hex()));
                    let capture_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                    let cut: DfsRepairHandoffCut = loop {
                        let captured = crate::metadata_io::read_sobj_optional(&source_directory.join("cut.sobj")).unwrap();
                        assert!(Instant::now() <= capture_deadline, "real protocol source export before unchanged 5s setup budget");
                        if let Some(cut) = captured { break cut; }
                        std::thread::sleep(Duration::from_millis(10));
                    };
                    cut.validate_selected_original_data().unwrap();
                    let page: DfsRepairHandoffPage = crate::metadata_io::read_sobj_optional(&source_directory.join("0.sobj")).unwrap().unwrap();
                    page.validate_selected_original_data().unwrap();
                    assert_eq!(page.cut, cut);
                    if after_processing {
                        loop {
                            let paused = paused_pages.lock().unwrap();
                            assert!(Instant::now() <= capture_deadline,
                                "exact decoded selected page must pause within the existing 5s setup budget");
                            assert!(paused.len() <= 16);
                            if !paused.is_empty() {
                                assert!(paused.iter().all(|command| command.prefix_probe.as_ref()
                                    .and_then(|probe| probe.repair_custody.as_ref())
                                    .is_some_and(|actual| actual.cut == cut)));
                                assert!(paused.iter().any(|command| command.prefix_probe.as_ref()
                                    .and_then(|probe| probe.repair_custody.as_ref())
                                    .is_some_and(|actual| actual == &page)),
                                    "the paused decoded command must include this exact source page");
                                break;
                            }
                            drop(paused);
                            std::thread::sleep(Duration::from_millis(10));
                        }
                    } else {
                        let remaining = capture_deadline.checked_duration_since(Instant::now())
                            .expect("source export observation remains in the existing 5s capture budget");
                        let observed = source_observation.recv_timeout(remaining)
                            .expect("actual source reached the exact selected export before first send");
                        assert!(Instant::now() <= capture_deadline,
                            "source export observation cannot arrive after its original capture deadline");
                        assert_eq!(observed.root, crate::local_dfs_metadata_shard_path(
                            fixture.runtime.data_dir(&owner), &shard));
                        assert_eq!(observed.cut, cut);
                        assert_eq!(observed.page, page);
                        assert_eq!(observed.freeze.handoff_id, proposed.handoff_id);
                        assert!(observed.freeze.activated_epoch.is_none());
                    }
                    let pending = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                        .get_metadata_shard_membership_handoff(&fixture.shard.file_system_id, &fixture.shard.shard_id)).unwrap().unwrap();
                    assert_eq!(pending, cut.handoff);
                    assert!(pending.retiring_primary_fence.is_some());
                    let data = Arc::new(fixture.runtime.data_dir(&fixture.promoted).clone());
                    let handle = fixture.runtime.runtime_handle();
                    let host = Arc::new(crate::runtime_host::DaemonDfsDataPlaneHost::new_on_runtime(
                        crate::runtime_host::DfsRuntimePaths::for_host_role(&data, sinomo_module::ComponentHostRole::Embedded), &handle));
                    let context_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                    let context = loop {
                        let context = handle.block_on(host.retained_metadata_replica_shard_context(
                            Arc::clone(&data), fixture.shard.file_system_id.clone(), shard.clone()).unwrap()).unwrap();
                        assert!(Instant::now() <= context_deadline, "actual retained B context must reach exact pending fence within its normal budget");
                        if let Some(context) = context.filter(|context| context.pending_membership_handoff.as_ref() == Some(&pending)) { break context; }
                    };
                    let request_id = format!("native-selected-clock-{}", proposed.handoff_id);
                    let fence = pending.retiring_primary_fence.as_ref().unwrap();
                    let command = DfsMetadataJournalBatchCommand {
                        request_id: request_id.clone(), source_node_id: Some(owner.clone()),
                        file_system_id: Some(fixture.shard.file_system_id.clone()), authority_epoch: Some(fixture.shard.epoch),
                        verify_prefix_only: true, authority_probe: None, durable_replication: None,
                        prefix_probe: Some(DfsMetadataPrefixProbe {
                            barrier_id: format!("native-selected-clock-barrier-{}", proposed.handoff_id),
                            file_system_id: Some(fixture.shard.file_system_id.clone()), authority_epoch: Some(fixture.shard.epoch),
                            shard_id: shard.clone(), journal_sequence: fence.journal_sequence,
                            state_fingerprint: fence.state_fingerprint.clone(), primary_handoff_id: Some(proposed.handoff_id.clone()),
                            repair_custody: Some(page.clone()),
                        }),
                        batch: DfsMetadataJournalFrameBatch { shard_id: shard.clone(), journal_frames: Vec::new(), next_after_sequence: fence.journal_sequence },
                    };
                    let layout = fixture.runtime.selected_original_data_layout(&fixture.promoted).unwrap();
                    let receiver_root = crate::local_dfs_metadata_shard_path(&data, &shard);
                    let before_files = receiver_custody_files(&receiver_root);
                    let response = if after_processing {
                        let (started, release) = crate::runtime_host::install_daemon_dfs_message_committed_edge_test_hook(&request_id);
                        let actual_host = Arc::clone(&host);
                        let caller = owner.clone();
                        let task = handle.spawn(async move {
                            actual_host.apply_metadata_journal_batch(data, caller, command, Some(context)).await
                        });
                        let at_edge = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            started.recv_timeout(Duration::from_secs(5)).expect("actual post-processing edge reached within its unchanged wait");
                            let (clocks, omitted) = layout.receiver_clocks().unwrap();
                            assert_eq!(omitted, 0);
                            let matching: Vec<_> = clocks.iter().filter(|(id, _, _)| *id == request_id).collect();
                            assert_eq!(matching.len(), 1);
                            let (_, captured, cutoff) = matching[0];
                            assert_eq!(cutoff.duration_since(*captured), Duration::from_secs(5));
                            let received = receiver_root.join("journal/repair-handoffs-v1")
                                .join(format!("{}-received", blake3::hash(proposed.handoff_id.as_bytes()).to_hex()));
                            let staged: DfsRepairHandoffPage = crate::metadata_io::read_sobj_optional(&received.join("0.sobj")).unwrap().unwrap();
                            assert_eq!(staged, page, "actual DATA is synchronized before the selected post-processing fault");
                            assert!(received.join("received.sobj").is_file());
                            assert_ne!(receiver_custody_files(&receiver_root), before_files);
                            std::thread::sleep(cutoff.saturating_duration_since(Instant::now()) + Duration::from_millis(5));
                        }));
                        let _ = release.send(());
                        let response = handle.block_on(task).expect("actual host child drained").unwrap();
                        if let Err(error) = at_edge { std::panic::resume_unwind(error); }
                        assert!(matches!(&response.result, DfsMetadataJournalBatchResult::Rejected { code: DfsMetadataRejectionCode::Other, reason }
                            if reason.contains("expired after processing")), "{response:?}");
                        response
                    } else {
                        let future = host.apply_metadata_journal_batch(data, owner.clone(), command, Some(context));
                        let (clocks, omitted) = layout.receiver_clocks().unwrap();
                        assert_eq!(omitted, 0);
                        let matching: Vec<_> = clocks.iter().filter(|(id, _, _)| *id == request_id).collect();
                        assert_eq!(matching.len(), 1, "entry captures before the returned future is polled");
                        let (_, captured, cutoff) = matching[0];
                        assert_eq!(cutoff.duration_since(*captured), Duration::from_secs(5));
                        std::thread::sleep(cutoff.saturating_duration_since(Instant::now()) + Duration::from_millis(5));
                        let response = handle.block_on(future).unwrap();
                        assert!(matches!(&response.result, DfsMetadataJournalBatchResult::Rejected { code: DfsMetadataRejectionCode::Other, reason }
                            if reason.contains("selected receiver processing budget")), "{response:?}");
                        assert_eq!(receiver_custody_files(&receiver_root), before_files,
                            "expired entry must not freeze/bind/write a floor or stage selected DATA");
                        response
                    };
                    assert!(!response.verified_prefix_only);
                    assert!(response.verified_authority_probe_id.is_none());
                    assert!(layout.queue().unwrap().installed_original_handoff_data(&owner, &fixture.birth.root.subject.request_id).unwrap().is_none(),
                        "staged DATA or a late rejected response never installs queue custody");
                }));
                // Release the source-stage pause even when a receiver
                // assertion fails, then join before propagating that verdict.
                if let Some(pause) = &source_pause { pause.release(); }
                fixture.runtime.clear_faults();
                gate.add_permits(16);
                let produced = producer.join().expect("real source handoff child drained");
                if let Err(error) = checks { std::panic::resume_unwind(error); }
                if !after_processing || produced.is_err() {
                    assert_eq!(fixture.runtime.observe_completed_native_handoff(&control, &proposed, phase_deadline).unwrap(),
                        Some(proposed.desired_shard.clone()), "real handoff after releasing only the negative gate: {produced:?}");
                }
            });
        }));
        fixture.runtime.clear_faults();
        drop(fixture.held.take());
        drop(fixture.held_promoted.take());
        let closed = fixture.runtime.close();
        if let Err(error) = outcome {
            if let Err(close_error) = closed { eprintln!("native selected clock cleanup also failed: {close_error}"); }
            std::panic::resume_unwind(error);
        }
        closed.expect("native selected clock fixture drains normally");
    }

    #[test]
    fn native_selected_receiver_clock_is_captured_before_poll_and_expired_input_cannot_stage() {
        native_selected_receiver_clock_control(false);
    }

    #[test]
    fn native_selected_receiver_expiry_after_processing_retains_data_without_ack() {
        native_selected_receiver_clock_control(true);
    }

    /// Capture only the native receiver's authoritative metadata and selected
    /// custody files. Diagnostic clocks/wakes are permitted; none can stand for
    /// a new journal entry, prefix ACK, installed original or execution grant.
    fn selected_rejection_files(fixture: &GenuineNativeOriginal) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        fn visit(path: &Path, result: &mut Vec<(std::path::PathBuf, Vec<u8>)>) {
            let metadata = match std::fs::symlink_metadata(path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
                Err(error) => panic!("inspect selected rejection path {}: {error}", path.display()),
            };
            assert!(!metadata.file_type().is_symlink(), "fixture path cannot alias another owner");
            if metadata.is_file() {
                result.push((path.to_path_buf(), std::fs::read(path).unwrap()));
            } else {
                assert!(metadata.is_dir());
                for entry in std::fs::read_dir(path).unwrap() {
                    visit(&entry.unwrap().path(), result);
                }
            }
        }
        let data = fixture.runtime.data_dir(&fixture.promoted);
        let root = crate::local_dfs_metadata_shard_path(data, &DfsMetadataShardId::new(&fixture.shard.shard_id));
        let snapshot = crate::default_dfs_repair_attempt_snapshot_path(data);
        let mut files = Vec::new();
        for path in [
            root.join("journal"),
            root.join("records/tables"),
            root.join("coordination/metadata-prefix-ack-floors"),
            snapshot.with_file_name("repair-attempts.active.slog"),
            snapshot,
        ] {
            visit(&path, &mut files);
        }
        files.sort_by(|left, right| left.0.cmp(&right.0));
        files
    }

    fn assert_selected_probe_rejected(
        response: &DfsMetadataJournalBatchResponse,
        command: &DfsMetadataJournalBatchCommand,
        expected_code: DfsMetadataRejectionCode,
        reason_fragment: &str,
    ) {
        assert_eq!(response.request_id, command.request_id);
        assert_eq!(response.shard_id, command.batch.shard_id);
        assert_eq!(response.next_after_sequence, command.batch.next_after_sequence);
        assert_eq!(response.applied_entries, 0);
        assert!(!response.verified_prefix_only);
        assert!(response.verified_authority_probe_id.is_none());
        assert!(matches!(&response.result,
            DfsMetadataJournalBatchResult::Rejected { code, reason }
                if *code == expected_code && reason.contains(reason_fragment)), "{response:?}");
    }

    fn selected_rejection_file_evidence(files: &[(std::path::PathBuf, Vec<u8>)]) -> serde_json::Value {
        serde_json::json!(files.iter().map(|(path, bytes)| serde_json::json!({
            "path": path, "bytes": bytes.len(), "blake3": blake3::hash(bytes).to_hex().to_string(),
        })).collect::<Vec<_>>())
    }

    /// A's normal native protocol owns the freeze and first selected export.
    /// Only its registered delivery is paused; explicit negative RPCs traverse
    /// the actual authenticated Main endpoint and cannot supply successful ACKs.
    fn with_native_selected_rejection_probe(
        name: &str,
        prepare_routed_carrier: bool,
        check: impl FnOnce(&GenuineNativeOriginal, &DfsMetadataJournalBatchCommand,
            Option<&mut NetworkedDfsService<crate::DfsNodeTransport>>, &mut Vec<serde_json::Value>),
    ) {
        let mut fixture = genuine_native_original(name);
        let mut observations = Vec::new();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let owner = fixture.shard.primary_node_id.clone();
            // A normal client requires completed startup. Acquire it before
            // this fixture deliberately parks that same A handoff, then use
            // it only to allocate the routed probe's carrier envelope.
            let mut carrier_allocator = prepare_routed_carrier
                .then(|| fixture.runtime.client(&owner).unwrap());
            let mut source = fixture.runtime.metadata_primary_service("/services/selected-original-data.bin").unwrap();
            let fence = source.metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id).unwrap();
            let control = fixture.runtime.shared_control_plane().unwrap();
            let proposed = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .request_metadata_shard_primary_handoff(&fixture.shard, &fixture.promoted)).unwrap().unwrap();
            fixture.runtime.select_genuine_original_before_handoff(&owner, &fixture.birth.root.subject.request_id, &proposed).unwrap();
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let paused_pages = Arc::new(Mutex::new(Vec::new()));
            fixture.runtime.install_fault(FaultRule {
                source: Some(owner.clone()), target: Some(fixture.promoted.clone()),
                route: Some(DFS_METADATA_JOURNAL_BATCH_ROUTE.into()), call_id: None,
                origin: Some(RpcOrigin::RegisteredHost), remaining_hits: 16,
                action: FaultAction::PauseSelectedOriginalCustody {
                    handoff_id: proposed.handoff_id.clone(), gate: Arc::clone(&gate),
                    captured: Arc::clone(&paused_pages),
                },
            });
            let configs = fixture.runtime.metadata_shards().unwrap();
            let phase_deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
            drop(fence);
            std::thread::scope(|scope| {
                let producer = scope.spawn(move || source.reconcile_pending_metadata_shard_membership_handoffs(
                    &configs, DEFAULT_DFS_POSIX_SYNC_TIMEOUT));
                let checks = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let shard = DfsMetadataShardId::new(&fixture.shard.shard_id);
                    let source_directory = crate::local_dfs_metadata_shard_path(fixture.runtime.data_dir(&owner), &shard)
                        .join("journal/repair-handoffs-v1")
                        .join(format!("{}-source", blake3::hash(proposed.handoff_id.as_bytes()).to_hex()));
                    let capture_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                    let cut: DfsRepairHandoffCut = loop {
                        let captured = crate::metadata_io::read_sobj_optional(&source_directory.join("cut.sobj")).unwrap();
                        assert!(Instant::now() <= capture_deadline, "actual source export uses unchanged 5s setup budget");
                        if let Some(cut) = captured { break cut; }
                        std::thread::sleep(Duration::from_millis(10));
                    };
                    cut.validate_selected_original_data().unwrap();
                    let page: DfsRepairHandoffPage = crate::metadata_io::read_sobj_optional(&source_directory.join("0.sobj")).unwrap().unwrap();
                    page.validate_selected_original_data().unwrap();
                    assert_eq!(page.cut, cut);
                    loop {
                        let paused = paused_pages.lock().unwrap();
                        assert!(Instant::now() <= capture_deadline,
                            "exact decoded selected page must pause within the existing 5s setup budget");
                        assert!(paused.len() <= 16);
                        if !paused.is_empty() {
                            assert!(paused.iter().all(|command| command.prefix_probe.as_ref()
                                .and_then(|probe| probe.repair_custody.as_ref())
                                .is_some_and(|actual| actual.cut == cut)));
                            assert!(paused.iter().any(|command| command.prefix_probe.as_ref()
                                .and_then(|probe| probe.repair_custody.as_ref())
                                .is_some_and(|actual| actual == &page)),
                                "the paused decoded command must include this exact source page");
                            break;
                        }
                        drop(paused);
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    let sealed = page.selected_original_data.as_ref().expect("genuine selected original");
                    assert_eq!(sealed.original, fixture.birth.original);
                    assert_eq!(sealed.attachment, fixture.birth.attachment);
                    assert_eq!(sealed.root, fixture.birth.root);
                    let pending = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                        .get_metadata_shard_membership_handoff(&fixture.shard.file_system_id, &fixture.shard.shard_id)).unwrap().unwrap();
                    assert_eq!(pending, cut.handoff);
                    let data = Arc::new(fixture.runtime.data_dir(&fixture.promoted).clone());
                    let handle = fixture.runtime.runtime_handle();
                    let host = crate::runtime_host::DaemonDfsDataPlaneHost::new_on_runtime(
                        crate::runtime_host::DfsRuntimePaths::for_host_role(&data, sinomo_module::ComponentHostRole::Embedded), &handle);
                    let context_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                    loop {
                        let context = handle.block_on(host.retained_metadata_replica_shard_context(
                            Arc::clone(&data), fixture.shard.file_system_id.clone(), shard.clone()).unwrap()).unwrap();
                        assert!(Instant::now() <= context_deadline, "actual B context reaches the pending fence within its unchanged 5s budget");
                        if context.is_some_and(|context| context.pending_membership_handoff.as_ref() == Some(&pending)) { break; }
                    }
                    let fence = pending.retiring_primary_fence.as_ref().unwrap();
                    // Probe/carrier IDs are independent of the immutable Repair
                    // identity inside page zero; no original request is reborn.
                    let command = DfsMetadataJournalBatchCommand {
                        request_id: format!("selected-negative-probe-{}", proposed.handoff_id),
                        source_node_id: Some(owner.clone()), file_system_id: Some(fixture.shard.file_system_id.clone()),
                        authority_epoch: Some(fixture.shard.epoch), verify_prefix_only: true,
                        authority_probe: None, durable_replication: None,
                        prefix_probe: Some(DfsMetadataPrefixProbe {
                            barrier_id: format!("selected-negative-barrier-{}", proposed.handoff_id),
                            file_system_id: Some(fixture.shard.file_system_id.clone()), authority_epoch: Some(fixture.shard.epoch),
                            shard_id: shard.clone(), journal_sequence: fence.journal_sequence,
                            state_fingerprint: fence.state_fingerprint.clone(), primary_handoff_id: Some(proposed.handoff_id.clone()),
                            repair_custody: Some(page),
                        }),
                        batch: DfsMetadataJournalFrameBatch { shard_id: shard, journal_frames: Vec::new(), next_after_sequence: fence.journal_sequence },
                    };
                    let source_queue = fixture.runtime.selected_original_data_layout(&owner).unwrap().queue().unwrap();
                    let original_before = source_queue.original_repair(&fixture.birth.original).unwrap().unwrap();
                    check(&fixture, &command, carrier_allocator.as_mut(), &mut observations);
                    assert_eq!(source_queue.original_repair(&fixture.birth.original).unwrap(), Some(original_before),
                        "negative probes cannot recreate, replace or settle the source original");
                    let queue = fixture.runtime.selected_original_data_layout(&fixture.promoted).unwrap().queue().unwrap();
                    assert!(queue.installed_original_handoff_data(&owner, &fixture.birth.root.subject.request_id).unwrap().is_none());
                    assert!(queue.validate_original_repair_dispatch(&fixture.birth.original).is_err());
                    assert_eq!(fixture.runtime.observation_dropped_count(), 0);
                }));
                // Always release and join the real producer before propagating
                // a failed negative assertion or closing the native fixture.
                fixture.runtime.clear_faults();
                gate.add_permits(16);
                let produced = producer.join().expect("native source handoff child drained");
                if let Err(error) = checks { std::panic::resume_unwind(error); }
                if let Err(error) = produced {
                    assert_eq!(fixture.runtime.observe_completed_native_handoff(&control, &proposed, phase_deadline).unwrap(),
                        Some(proposed.desired_shard.clone()), "normal handoff after negative-only gate release: {error}");
                }
            });
            drop(carrier_allocator);
        }));
        let report = serde_json::json!({
            "assertions_passed": outcome.is_ok(), "initial_original": &fixture.birth,
            "observations": observations,
            "qualification": "Actual authenticated native input rejection only; no successor execution, Terminal, root retirement or abrupt-loss acceptance."
        });
        if let Err(error) = std::fs::write(fixture.runtime.data_dir(&fixture.promoted).root()
            .join("native-selected-input-rejections.json"), serde_json::to_vec_pretty(&report).unwrap())
        { eprintln!("native selected rejection report write failed: {error}"); }
        fixture.runtime.clear_faults();
        drop(fixture.held.take());
        drop(fixture.held_promoted.take());
        let closed = fixture.runtime.close();
        if let Err(error) = outcome {
            if let Err(close_error) = closed { eprintln!("native selected rejection cleanup also failed: {close_error}"); }
            std::panic::resume_unwind(error);
        }
        closed.expect("native selected rejection fixture drains normally");
    }

    #[test]
    fn native_selected_ingress_rejects_wrong_source_prefix_and_scope_without_mutation() {
        with_native_selected_rejection_probe("native-selected-invalid-ingress", false, |fixture, genuine, _carrier, observations| {
            let owner = &fixture.shard.primary_node_id;
            for arm in ["authenticated-source", "prefix", "filesystem-scope", "epoch"] {
                let mut command = genuine.clone();
                command.request_id.push_str(arm);
                let caller = if arm == "authenticated-source" { &fixture.holder } else { owner };
                let (code, reason) = match arm {
                    "authenticated-source" => (DfsMetadataRejectionCode::Other, "authenticated metadata journal caller"),
                    "prefix" => {
                        let fingerprint = &mut command.prefix_probe.as_mut().unwrap().state_fingerprint;
                        let first = fingerprint.chars().next().unwrap();
                        fingerprint.replace_range(..first.len_utf8(), if first == '0' { "1" } else { "0" });
                        // The real pending-handoff fence rejects this before
                        // the later stored-prefix comparison can run.
                        (DfsMetadataRejectionCode::Other, "not bound to the exact fenced role rotation")
                    }
                    "filesystem-scope" => {
                        command.prefix_probe.as_mut().unwrap().file_system_id = None;
                        (DfsMetadataRejectionCode::Other, "prefix probe authority does not match")
                    }
                    "epoch" => {
                        command.prefix_probe.as_mut().unwrap().authority_epoch = Some(fixture.shard.epoch.checked_add(1).unwrap());
                        (DfsMetadataRejectionCode::Other, "prefix probe authority does not match")
                    }
                    _ => unreachable!(),
                };
                assert_eq!(command.prefix_probe.as_ref().unwrap().repair_custody,
                    genuine.prefix_probe.as_ref().unwrap().repair_custody, "only the hostile outer claim changes");
                let before = selected_rejection_files(fixture);
                let prior_observations = fixture.runtime.rpc_observations().len();
                let request = sinomo_core::network::ObjectRpcRequest::encode_body(
                    DFS_METADATA_JOURNAL_BATCH_ROUTE, DFS_RPC_CATEGORY, &command).unwrap();
                let response = fixture.runtime.rpc_request(caller, &fixture.promoted, request,
                    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT).unwrap_or_else(|error| {
                        observations.push(serde_json::json!({"arm": arm, "command": command, "transport_error": error}));
                        panic!("{arm} requires an actual decoded rejection, not a transport failure: {error}");
                    });
                let response: DfsMetadataJournalBatchResponse = response.decode_body().unwrap_or_else(|error| {
                    observations.push(serde_json::json!({"arm": arm, "command": command, "decode_error": error.to_string()}));
                    panic!("{arm} requires the native metadata response codec: {error}");
                });
                let after = selected_rejection_files(fixture);
                observations.push(serde_json::json!({"arm": arm, "command": command, "response": response,
                    "before_files": selected_rejection_file_evidence(&before),
                    "after_files": selected_rejection_file_evidence(&after)}));
                assert_selected_probe_rejected(&response, &command, code, reason);
                assert!(after == before,
                    "{arm} must not freeze, stage, install, write a prefix ACK or change journal/queue DATA");
                assert!(fixture.runtime.rpc_observations().iter().skip(prior_observations).any(|call| call.source == *caller
                    && call.target == fixture.promoted && call.route == DFS_METADATA_JOURNAL_BATCH_ROUTE
                    && call.origin == RpcOrigin::ExplicitFixtureRpc && call.phase == RpcPhase::Completed));
            }
        });
    }

    #[test]
    fn native_selected_routed_input_without_receiver_budget_rejects_before_effects() {
        with_native_selected_rejection_probe("native-selected-unbudgeted-routed", true, |fixture, genuine, carrier, observations| {
            let owner = &fixture.shard.primary_node_id;
            let layout = fixture.runtime.selected_original_data_layout(&fixture.promoted).unwrap();
            let before_clocks = layout.receiver_clocks().unwrap();
            let caller = carrier.expect("normal A carrier allocator was obtained before the pending handoff");
            let routed_message = caller.next_dfs_routed_message(&fixture.promoted,
                DfsNetworkMessage::MetadataJournalBatch(genuine.clone()));
            let command = DfsMessageCommand {
                request_id: format!("selected-routed-envelope-{}", genuine.request_id), routed_message,
            };
            let call = crate::dfs_message_rpc_call(&command).unwrap();
            let route = call.request.route.clone();
            assert_eq!(route, DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE);
            assert_ne!(route, DFS_METADATA_JOURNAL_BATCH_ROUTE);
            let before = selected_rejection_files(fixture);
            let prior_observations = fixture.runtime.rpc_observations().len();
            let response = fixture.runtime.rpc_call(owner, &fixture.promoted, call,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT).unwrap_or_else(|error| {
                    observations.push(serde_json::json!({"arm": "routed-no-local-clock", "command": command, "transport_error": error}));
                    panic!("unbudgeted routed input requires a decoded rejection, not a transport failure: {error}");
                });
            let response = crate::decode_dfs_message_rpc_response(&response).unwrap_or_else(|error| {
                observations.push(serde_json::json!({"arm": "routed-no-local-clock", "command": command, "decode_error": error.to_string()}));
                panic!("routed input requires the native message response codec: {error}");
            });
            let after = selected_rejection_files(fixture);
            observations.push(serde_json::json!({"arm": "routed-no-local-clock", "command": command, "response": response,
                "before_files": selected_rejection_file_evidence(&before),
                "after_files": selected_rejection_file_evidence(&after)}));
            assert_eq!(response.request_id, command.request_id);
            let replies: Vec<_> = response.outbound_messages.iter().filter_map(|routed| {
                if let DfsNetworkMessage::MetadataJournalBatchResponse(reply) = &routed.message
                    && reply.request_id == genuine.request_id
                {
                    assert_eq!(routed.source_node_id, fixture.promoted);
                    assert_eq!(&routed.target_node_id, owner);
                    Some(reply)
                } else { None }
            }).collect();
            assert_eq!(replies.len(), 1, "actual routed handler returns exactly this original probe rejection");
            assert_selected_probe_rejected(replies[0], genuine, DfsMetadataRejectionCode::Other,
                "selected receiver processing budget unavailable/expired");
            assert_eq!(layout.receiver_clocks().unwrap(), before_clocks,
                "the routed fallback cannot mint the direct receiver's local processing clock");
            assert!(after == before,
                "unbudgeted selected input cannot persist custody, metadata or an ACK");
            assert!(fixture.runtime.rpc_observations().iter().skip(prior_observations).any(|call| call.source == *owner
                && call.target == fixture.promoted && call.route == route
                && call.origin == RpcOrigin::ExplicitFixtureRpc && call.phase == RpcPhase::Completed));
        });
    }


    mod imported_history_query_tests {
        use super::*;

        // Failure DATA only. The maintained helper owns the create-new
        // rpc-observations-NNNNNN.json path and retains its normal drop policy.
        // Neither capture nor reporting may replace the original assertion.
        fn checkpoint_failed_history_rpc(runtime: &DfsTestRuntime, phase: &'static str) {
            let checkpoint = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime.checkpoint_rpc_observation_window()
            }));
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match checkpoint {
                    Ok(Ok(0)) => eprintln!("HISTORY_NATIVE_FAILURE_RPC status=EMPTY_CURRENT_WINDOW phase={phase} records=0 file=NONE"),
                    Ok(Ok(count)) => eprintln!("HISTORY_NATIVE_FAILURE_RPC status=CHECKPOINTED phase={phase} records={count} path_family=rpc-observations-NNNNNN.json dropped={}", runtime.observation_dropped_count()),
                    Ok(Err(error)) => eprintln!("HISTORY_NATIVE_FAILURE_RPC status=UNAVAILABLE phase={phase} error={error}"),
                    Err(_) => eprintln!("HISTORY_NATIVE_FAILURE_RPC status=UNAVAILABLE phase={phase} error=checkpoint-panicked"),
                }
            }));
        }

        fn with_query_fixture(
            name: &str, prospective_receiver: bool,
            check: impl FnOnce(&mut GenuineNativeOriginal, &DfsMetadataShardConfig,
                &mut NetworkedDfsService<crate::DfsNodeTransport>, &LocalDfsRepairQueueStore),
        ) {
            with_query_fixture_cut(name, prospective_receiver, false, check);
        }

        fn with_query_fixture_cut(
            name: &str, prospective_receiver: bool, prepared_only: bool,
            check: impl FnOnce(&mut GenuineNativeOriginal, &DfsMetadataShardConfig,
                &mut NetworkedDfsService<crate::DfsNodeTransport>, &LocalDfsRepairQueueStore),
        ) {
            let fixture = genuine_native_original_with_receiver_cut(name, false, prospective_receiver, prepared_only);
            with_query_fixture_from_original(fixture, check);
        }

        fn with_query_fixture_from_original(
            mut fixture: GenuineNativeOriginal,
            check: impl FnOnce(&mut GenuineNativeOriginal, &DfsMetadataShardConfig,
                &mut NetworkedDfsService<crate::DfsNodeTransport>, &LocalDfsRepairQueueStore),
        ) {
            let path = "/services/selected-original-data.bin";
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let owner = fixture.shard.primary_node_id.clone();
                let request = fixture.birth.root.subject.request_id.clone();
                let mut source = fixture.runtime.metadata_primary_service(path).unwrap();
                let fence = source.metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id).unwrap();
                let control = fixture.runtime.shared_control_plane().unwrap();
                let proposed = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                    .request_metadata_shard_primary_handoff(&fixture.shard, &fixture.promoted)).unwrap().unwrap();
                fixture.runtime.select_genuine_original_before_handoff(&owner, &request, &proposed).unwrap();
                drop(fence);
                let handoff_deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
                let produced = source.reconcile_pending_metadata_shard_membership_handoffs(
                    &fixture.runtime.metadata_shards().unwrap(), handoff_deadline.saturating_duration_since(Instant::now()));
                drop(source);
                assert_eq!(fixture.runtime.observe_completed_native_handoff(&control, &proposed, handoff_deadline).unwrap(),
                    Some(proposed.desired_shard.clone()), "actual native handoff: {produced:?}");
                let layout = fixture.runtime.selected_original_data_layout(&fixture.promoted).unwrap();
                let shard_id = DfsMetadataShardId::new(&fixture.shard.shard_id);
                loop {
                    let marker = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                        fixture.runtime.data_dir(&fixture.promoted), &shard_id, Some(&layout)).unwrap();
                    if marker.is_some_and(|marker| marker.activated_epoch == Some(proposed.desired_shard.epoch)) { break; }
                    let remaining = handoff_deadline.checked_duration_since(Instant::now()).expect("unchanged 45s native handoff budget");
                    fixture.runtime.maintenance_tick(&fixture.promoted, remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)).unwrap();
                }
                assert!(Instant::now() <= handoff_deadline);
                let queue = layout.queue().unwrap();
                let imported = queue.installed_original_handoff_data(&owner, &request).unwrap().unwrap();
                assert_eq!(imported.material.birth.original, fixture.birth.original);
                assert_eq!(imported.material.birth.root, fixture.birth.root);
                assert_eq!(imported.material.birth.attachment, fixture.birth.attachment);
                assert_eq!(imported.material.cut.handoff.handoff_id, proposed.handoff_id);
                assert_eq!(imported.material.cut.handoff.desired_shard, proposed.desired_shard);
                assert!(queue.imported_executor_association(&owner, &request).unwrap().is_none());
                let command = &imported.material.birth.original_wire_material().unwrap().original.command;
                let absent = fixture.runtime.client(&fixture.promoted).unwrap().content()
                    .read_manifest(command.object_id.as_str(), command.version.as_str()).unwrap_err();
                assert!(matches!(absent, ContentStoreError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound));
                let mut successor = fixture.runtime.metadata_primary_service(path).unwrap();
                assert!(successor.publication_execution.current().is_none(), "query owns separate native phases");
                check(&mut fixture, &proposed.desired_shard, &mut successor, &queue);
                drop(successor);
            }));
            crate::service_repair::HISTORY_QUERY_AFTER_ATTEMPT.with(|slot| { slot.borrow_mut().take(); });
            crate::service_repair::HISTORY_QUERY_AFTER_OBSERVATION.with(|slot| { slot.borrow_mut().take(); });
            if outcome.is_err() {
                checkpoint_failed_history_rpc(&fixture.runtime, "query-fixture");
            }
            drop(fixture.held.take());
            drop(fixture.held_promoted.take());
            let closed = fixture.runtime.close();
            if let Err(error) = outcome {
                if let Err(close_error) = closed { eprintln!("native history cleanup also failed: {close_error}"); }
                std::panic::resume_unwind(error);
            }
            closed.expect("normal native history-query fixture drains");
        }

        fn query(fixture: &GenuineNativeOriginal, assignment: &DfsMetadataShardConfig,
            successor: &mut NetworkedDfsService<crate::DfsNodeTransport>) -> DfsOriginalRepairHistoryResponse {
            let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let previous = successor.set_transport_operation_deadline(Some(deadline));
            let returned = successor.query_installed_original_history(assignment, &fixture.shard.primary_node_id,
                &fixture.birth.root.subject.request_id);
            assert_eq!(successor.transport.operation_deadline(), Some(deadline));
            assert!(Instant::now() <= deadline, "query does not renew its existing caller clock");
            successor.set_transport_operation_deadline(previous);
            returned.unwrap()
        }

        fn assert_terminal(response: &DfsOriginalRepairHistoryResponse, fixture: &GenuineNativeOriginal) {
            let command = &fixture.birth.original_wire_material().unwrap().original.command;
            let DfsOriginalRepairHistoryResult::HistoricalTerminal { exact_original_response, .. } = &response.result else {
                panic!("genuine C Terminal was not returned as history: {response:?}");
            };
            assert_eq!(exact_original_response, &DfsRepairReplicaResponse {
                request_id: command.request_id.clone(), intent_id: command.intent_id.clone(), file_system_id: command.file_system_id.clone(),
                object_id: command.object_id.clone(), version: command.version.clone(), replica_node_id: fixture.holder.clone(),
                result: DfsBlobTransferResult::Committed { content_hash: command.content_hash.clone() },
            });
        }

        #[test]
        fn native_imported_history_query_without_b_payload() {
            with_query_fixture("native-imported-history-no-b-bytes", true, |fixture, assignment, successor, queue| {
                let producer = fixture.shard.primary_node_id.clone();
                let id = fixture.birth.root.subject.request_id.clone();
                let material = fixture.birth.original_wire_material().unwrap();
                let command = &material.original.command;
                let before_rows = queue.view().unwrap().records;
                let before_import = queue.installed_original_handoff_data(&producer, &id).unwrap();
                let source_queue = fixture.runtime.selected_original_data_layout(&producer).unwrap().queue().unwrap();
                let before_origin = source_queue.original_repair(&fixture.birth.original).unwrap();
                let before_receipts = successor.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap();
                let before_head = successor.metadata_backend().unwrap().get_object(&command.object_id).unwrap();
                let before_c = fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap();
                let rpc_before = fixture.runtime.rpc_observations().len();
                let response = query(fixture, assignment, successor);
                assert_terminal(&response, fixture);
                let saved = queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap();
                assert_eq!(saved.request().original_material, material);
                assert_eq!(saved.response(), Some(&response));
                assert!(saved.request().query_ref.attempt_sequence > saved.request().import_ref.imported_at_sequence);
                assert!(queue.view().unwrap().head_sequence > saved.request().query_ref.attempt_sequence);
                assert_eq!(queue.view().unwrap().records, before_rows);
                assert_eq!(queue.installed_original_handoff_data(&producer, &id).unwrap(), before_import);
                assert_eq!(source_queue.original_repair(&fixture.birth.original).unwrap(), before_origin);
                assert!(queue.imported_executor_association(&producer, &id).unwrap().is_none());
                assert!(queue.closed_repair_birth(&fixture.birth.root.subject).unwrap().is_none());
                assert_eq!(successor.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap(), before_receipts);
                assert_eq!(successor.metadata_backend().unwrap().get_object(&command.object_id).unwrap(), before_head);
                assert_eq!(fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap(), before_c);
                assert!(matches!(successor.content().read_manifest(command.object_id.as_str(), command.version.as_str()),
                    Err(ContentStoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound));
                let calls = fixture.runtime.rpc_observations();
                assert!(calls.iter().skip(rpc_before).any(|call| call.source == fixture.promoted && call.target == fixture.holder
                    && call.route == DFS_DATA_READ_ROUTE && call.phase == RpcPhase::Completed));
                assert!(!calls.iter().skip(rpc_before).any(|call| call.source == fixture.promoted && call.target == fixture.holder
                    && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE));
            });
        }

        #[test]
        fn native_imported_history_query_missing_history_stays_pending() {
            with_query_fixture_cut("native-imported-history-prepared-only", true, true, |fixture, assignment, successor, queue| {
                let material = fixture.birth.original_wire_material().unwrap();
                let before = fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap().unwrap();
                assert!(before.bound_id.is_none() && before.terminal_id.is_none() && before.response.is_none());
                let response = query(fixture, assignment, successor);
                assert!(matches!(response.result, DfsOriginalRepairHistoryResult::Pending { reason: DfsOriginalRepairHistoryPending::MissingTerminal }));
                let producer = fixture.shard.primary_node_id.clone();
                let id = fixture.birth.root.subject.request_id.clone();
                let pending = queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap();
                assert!(pending.terminal().is_none());
                let before_repeat = queue.view().unwrap();
                assert_eq!(query(fixture, assignment, successor), response);
                assert_eq!(queue.view().unwrap(), before_repeat, "same Pending observation cannot churn its attempt/result records");
                assert_eq!(fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap(), Some(before));
            });
            // This receiver has genuine ordinary content but no prospective
            // original-history representation. Matching bytes cannot upgrade it.
            with_query_fixture("native-imported-history-default-no-history", false, |fixture, assignment, successor, queue| {
                let material = fixture.birth.original_wire_material().unwrap();
                let command = &material.original.command;
                let c = fixture.runtime.client(&fixture.holder).unwrap();
                assert_eq!(c.content().read_object_version(command.object_id.as_str(), command.version.as_str()).unwrap().1, vec![0x5a;4096]);
                let rows = queue.view().unwrap().records;
                let result = successor.query_installed_original_history(assignment, &fixture.shard.primary_node_id, &command.request_id);
                assert!(result.is_err(), "default content cannot mint a history capability: {result:?}");
                let saved = queue.imported_history_query_for_test(&fixture.shard.primary_node_id, &command.request_id).unwrap().unwrap();
                assert!(saved.terminal().is_none());
                assert_eq!(queue.view().unwrap().records, rows);
                assert!(queue.imported_executor_association(&fixture.shard.primary_node_id, &command.request_id).unwrap().is_none());
                assert_eq!(c.content().read_object_version(command.object_id.as_str(), command.version.as_str()).unwrap().1, vec![0x5a;4096]);
            });
        }

        #[test]
        fn native_imported_history_query_binds_origin_attempt_and_reply() {
            with_query_fixture("native-imported-history-exact-correlation", true, |fixture, assignment, successor, queue| {
                crate::service_repair::HISTORY_QUERY_AFTER_ATTEMPT.with(|slot| *slot.borrow_mut() = Some(Box::new(||
                    Err(DfsError::Cancelled { operation: "history certificate initial-observation control" }))));
                assert!(matches!(successor.query_installed_original_history(assignment, &fixture.shard.primary_node_id,
                    &fixture.birth.root.subject.request_id), Err(DfsError::Cancelled { .. })));
                let pending = queue.imported_history_query_for_test(&fixture.shard.primary_node_id, &fixture.birth.root.subject.request_id).unwrap().unwrap();
                let genuine_return = successor.transport.request_original_repair_history_direct(&fixture.holder,
                    &pending.request().routed(), Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT).unwrap().into_routed();
                let before_initial = queue.view().unwrap();
                for fault in ["nonempty-seal", "nonempty-incarnation"] {
                    let mut wrong = genuine_return.clone();
                    let DfsNetworkMessage::OriginalRepairHistoryResponse(reply) = &mut wrong.message else { unreachable!() };
                    let DfsOriginalRepairHistoryResult::HistoricalTerminal { certificate, .. } = &mut reply.result else { unreachable!() };
                    if fault == "nonempty-seal" { certificate.prepared_seal.push_str("changed"); }
                    else { certificate.store_incarnation.push_str("changed"); }
                    assert!(reply.validate_for_request(pending.request()).is_err(), "{fault}");
                    assert!(queue.observe_imported_history_result(&pending, wrong,
                        Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT).is_err(), "initial {fault}");
                    assert_eq!(queue.view().unwrap(), before_initial);
                }
                let response = query(fixture, assignment, successor);
                assert_terminal(&response, fixture);
                let saved = queue.imported_history_query_for_test(&fixture.shard.primary_node_id, &fixture.birth.root.subject.request_id).unwrap().unwrap();
                let carrier = saved.request().routed();
                let command = DfsMessageCommand { request_id: format!("dfs_message_{}", carrier.frame_id), routed_message: carrier };
                let genuine = DfsMessageResponse { request_id: command.request_id.clone(), outbound_messages: vec![saved.returned_for_test().unwrap().clone()] };
                let material = fixture.birth.original_wire_material().unwrap();
                let unrelated = successor.next_dfs_routed_message(&fixture.promoted, DfsNetworkMessage::ReadRequest(DfsReadRequest {
                    request_id: "unrelated-native-core-read".into(), object_id: material.original.command.object_id.clone(),
                    version: Some(material.original.command.version.clone()), content_hash: Some(material.original.command.content_hash.clone()),
                    content_size_bytes: Some(4096), range_offset: None, range_max_bytes: None,
                }));
                successor.transport.test_enqueue_protocol_message(unrelated);
                let queued_before = successor.transport.pending_incoming_count().unwrap();
                assert!(queued_before.is_some_and(|count| count > 0));
                successor.transport.request_original_repair_history_direct(&fixture.holder, &saved.request().routed(),
                    Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT).unwrap();
                assert_eq!(successor.transport.pending_incoming_count().unwrap(), queued_before,
                    "the real direct native call cannot drain an unrelated Core protocol request");
                let queue_before = queue.view().unwrap();
                // These negative DTO probes use a return already produced by
                // the real native C call above; none supplies positive history.
                for malformed in ["outer", "empty", "multiple", "family", "source", "query", "origin", "nonempty-seal", "nonempty-incarnation"] {
                    let mut value = genuine.clone();
                    match malformed {
                        "outer" => value.request_id.push_str("-wrong"),
                        "empty" => value.outbound_messages.clear(),
                        "multiple" => value.outbound_messages.push(value.outbound_messages[0].clone()),
                        "family" => value.outbound_messages[0].message = fixture.birth.original.message.clone(),
                        "source" => value.outbound_messages[0].source_node_id = fixture.shard.primary_node_id.clone(),
                        "query" | "origin" | "nonempty-seal" | "nonempty-incarnation" => {
                            let DfsNetworkMessage::OriginalRepairHistoryResponse(reply) = &mut value.outbound_messages[0].message else { unreachable!() };
                            if malformed == "query" { reply.query_ref.attempt_sequence += 1; }
                            else if malformed == "origin" { reply.original_command_digest.push_str("wrong"); }
                            else {
                                let DfsOriginalRepairHistoryResult::HistoricalTerminal { certificate, .. } = &mut reply.result else { unreachable!() };
                                if malformed == "nonempty-seal" { certificate.prepared_seal.push_str("changed"); }
                                else { certificate.store_incarnation.push_str("changed"); }
                                assert!(queue.observe_imported_history_result(&saved, value.outbound_messages[0].clone(),
                                    Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT).is_err(), "duplicate {malformed}");
                            }
                        }
                        _ => unreachable!(),
                    }
                    assert!(crate::node_transport::validate_original_history_rpc_return(&command, value).is_err(), "{malformed}");
                }
                let mut changed = saved.request().clone(); changed.original_material.subject.queue_incarnation.push_str("-changed");
                assert!(changed.validate().is_err());
                assert_eq!(queue.view().unwrap(), queue_before);
                let counters = (successor.next_frame_id, successor.next_message_id);
                let repeat = query(fixture, assignment, successor);
                assert_eq!(repeat, response);
                assert_eq!((successor.next_frame_id, successor.next_message_id), counters);
                assert_eq!(queue.view().unwrap(), queue_before);
            });
        }

        #[test]
        fn native_imported_history_query_reopen_and_certification_cuts() {
            with_query_fixture("native-imported-history-reopen-cuts", true, |fixture, assignment, successor, queue| {
                let producer = fixture.shard.primary_node_id.clone();
                let id = fixture.birth.root.subject.request_id.clone();
                crate::service_repair::HISTORY_QUERY_AFTER_ATTEMPT.with(|slot| *slot.borrow_mut() = Some(Box::new(|| {
                    Err(DfsError::Cancelled { operation: "native history cut after synchronized attempt before send" })
                })));
                let rpc_before = fixture.runtime.rpc_observations().len();
                assert!(matches!(successor.query_installed_original_history(assignment, &producer, &id), Err(DfsError::Cancelled { .. })));
                let pending = queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap();
                assert!(pending.response().is_none());
                assert!(!fixture.runtime.rpc_observations().iter().skip(rpc_before).any(|call| call.source == fixture.promoted
                    && call.target == fixture.holder && call.route == DFS_DATA_READ_ROUTE));
                queue.compact_if_needed(DfsRepairQueueCompactionOptions::force()).unwrap();
                queue.reload_from_disk_on_next_access();
                assert_eq!(queue.imported_history_query_for_test(&producer, &id).unwrap(), Some(pending.clone()));
                let c_before = fixture.runtime.client(&fixture.holder).unwrap().content()
                    .original_repair_test_snapshot(&pending.request().original_material).unwrap();
                fixture.runtime.stop_node(&fixture.holder).unwrap();
                fixture.runtime.reopen_node(fixture.holder.clone()).unwrap();
                assert_eq!(fixture.runtime.client(&fixture.holder).unwrap().content()
                    .original_repair_test_snapshot(&pending.request().original_material).unwrap(), c_before);
                // Actual authenticated C dispatch enters its existing writer
                // recovery and fails the real catalog certification barrier.
                // The fixture forces uncertainty; this is not an OS crash.
                let receiver = fixture.runtime.client(&fixture.holder).unwrap();
                let content = receiver.content().clone();
                drop(receiver);
                let context = fixture.runtime.component_context(&fixture.holder).unwrap();
                let once = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let observed = Arc::clone(&once);
                let certification_cut = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(context.data_dir(), Arc::new(move |point, command, _| {
                    if matches!(point, crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { .. })
                        && command.is_some_and(|command| matches!(&command.routed_message.message, DfsNetworkMessage::OriginalRepairHistoryRequest(_)))
                        && !observed.swap(true, std::sync::atomic::Ordering::AcqRel) {
                        content.arm_original_history_certification_failure_for_test()?;
                    }
                    Ok(())
                }));
                drop(context);
                let uncertified = successor.query_installed_original_history(assignment, &producer, &id);
                drop(certification_cut);
                assert!(once.load(std::sync::atomic::Ordering::Acquire), "actual C query ingress reached the certification cut");
                let uncertified_error = uncertified.expect_err("a readable Terminal cannot escape failed certification");
                assert!(uncertified_error.to_string().contains("injected catalog recertification failure"),
                    "the actual owning writer barrier, not earlier scope refusal, must reject: {uncertified_error}");
                assert!(queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap().terminal().is_none());
                crate::service_repair::HISTORY_QUERY_AFTER_OBSERVATION.with(|slot| *slot.borrow_mut() = Some(Box::new(|| {
                    Err(DfsError::Cancelled { operation: "native history cut after synchronized result before caller completion" })
                })));
                assert!(matches!(successor.query_installed_original_history(assignment, &producer, &id), Err(DfsError::Cancelled { .. })));
                let recorded = queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap();
                assert_eq!(recorded.request(), pending.request());
                assert!(recorded.terminal().is_some());
                queue.compact_if_needed(DfsRepairQueueCompactionOptions::force()).unwrap();
                queue.reload_from_disk_on_next_access();
                let before_repeat = queue.view().unwrap();
                assert_eq!(queue.imported_history_query_for_test(&producer, &id).unwrap(), Some(recorded.clone()),
                    "cold snapshot replay must retain every delivery and the exact genuine Terminal response");
                let calls_before_repeat = fixture.runtime.rpc_observations().len();
                let response = query(fixture, assignment, successor);
                assert_terminal(&response, fixture);
                assert_eq!(&response, recorded.terminal().unwrap());
                assert_eq!(queue.view().unwrap(), before_repeat);
                assert!(!fixture.runtime.rpc_observations().iter().skip(calls_before_repeat).any(|call| call.source == fixture.promoted
                    && call.target == fixture.holder && call.route == DFS_DATA_READ_ROUTE));
                let mut stale = assignment.clone(); stale.epoch += 1;
                assert!(successor.query_installed_original_history(&stale, &producer, &id).is_err());
                assert_eq!(queue.view().unwrap(), before_repeat);
            });
        }


        type HistoryGate = Arc<(Mutex<bool>, std::sync::Condvar)>;

        fn release_history_gate(gate: &HistoryGate) {
            *gate.0.lock().unwrap() = true;
            gate.1.notify_all();
        }

        // Reuse the real per-root hook. Every matched invocation waits; only
        // the arrival notification is one-shot. No response or authority is supplied.
        fn history_native_gate(fixture: &GenuineNativeOriginal, after_execute: bool, deadline: Instant)
            -> (Box<dyn Send>, std::sync::mpsc::Receiver<()>, HistoryGate) {
            let material = fixture.birth.original_wire_material().unwrap();
            let source = fixture.promoted.clone();
            let receiver = fixture.holder.clone();
            let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
            let held = Arc::clone(&gate);
            let (tx, rx) = std::sync::mpsc::channel();
            let signal = Mutex::new(Some(tx));
            let context = fixture.runtime.component_context(&fixture.holder).unwrap();
            let hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(context.data_dir(), Arc::new(move |point, command, _| {
                let reached = if after_execute {
                    matches!(point, crate::runtime_host::DaemonDfsPreDispatchTestPoint::AfterExecute)
                } else {
                    matches!(point, crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { .. })
                };
                if reached && command.is_some_and(|command| command.routed_message.source_node_id == source
                    && command.routed_message.target_node_id == receiver
                    && matches!(&command.routed_message.message, DfsNetworkMessage::OriginalRepairHistoryRequest(request)
                        if request.original_material == material)) {
                    if let Some(signal) = signal.lock().unwrap().take() { let _ = signal.send(()); }
                    let ready = held.0.lock().unwrap();
                    let (ready, _) = held.1.wait_timeout_while(ready, deadline.saturating_duration_since(Instant::now()), |ready| !*ready).unwrap();
                    if !*ready { return Err(DfsError::Cancelled { operation: "native history negative gate exhausted original caller deadline" }); }
                }
                Ok(())
            }));
            (Box::new(hook), rx, gate)
        }

        #[test]
        fn native_imported_history_query_rejects_physical_replacement_after_capture() {
            with_query_fixture("native-history-physical-after-capture", true, |fixture, assignment, successor, queue| {
                let material = fixture.birth.original_wire_material().unwrap();
                let receiver = fixture.runtime.client(&fixture.holder).unwrap();
                let content = receiver.content().clone();
                let before = content.original_repair_test_snapshot(&material).unwrap();
                drop(receiver);
                let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                let previous = successor.set_transport_operation_deadline(Some(deadline));
                let (hook, reached, gate) = history_native_gate(fixture, false, deadline);
                let producer = fixture.shard.primary_node_id.clone();
                let id = fixture.birth.root.subject.request_id.clone();
                std::thread::scope(|scope| {
                    let mut caller = Some(scope.spawn(|| successor.query_installed_original_history(assignment, &producer, &id)));
                    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        reached.recv_timeout(deadline.saturating_duration_since(Instant::now())).expect("real C captured its exact native read context");
                        content.with_original_history_replaced_root_for_test(|| {
                            release_history_gate(&gate);
                            let result = caller.take().unwrap().join().expect("owned native history caller drained");
                            let error = result.expect_err("matching files in another physical root cannot use C's captured owner");
                            assert!(error.to_string().contains("physically replaced"), "the owner check, not timeout/unrelated refusal, must reject: {error}");
                            assert!(Instant::now() <= deadline, "no renewed physical-fault caller budget");
                        }).unwrap();
                    }));
                    release_history_gate(&gate);
                    if let Some(caller) = caller.take() { let _ = caller.join(); }
                    drop(hook);
                    if let Err(error) = checked { std::panic::resume_unwind(error); }
                });
                successor.set_transport_operation_deadline(previous);
                let pending = queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap();
                assert!(pending.response().is_none());
                assert_eq!(content.original_repair_test_snapshot(&material).unwrap(), before,
                    "restoration reuses the original physical root and genuine retained history");
                assert_eq!(content.read_object_version(material.original.command.object_id.as_str(), material.original.command.version.as_str()).unwrap().1, vec![0x5a;4096]);
                let response = query(fixture, assignment, successor);
                assert_terminal(&response, fixture);
                assert_eq!(queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap().request(), pending.request());
            });
        }

        #[test]
        fn native_imported_history_query_retiring_b_cannot_observe_inflight_return() {
            with_query_fixture("native-history-inflight-b-retirement", true, |fixture, assignment, successor, queue| {
                let producer = fixture.shard.primary_node_id.clone();
                let id = fixture.birth.root.subject.request_id.clone();
                let source_queue = fixture.runtime.selected_original_data_layout(&producer).unwrap().queue().unwrap();
                let original_before = source_queue.original_repair(&fixture.birth.original).unwrap();
                let layout_b = fixture.runtime.selected_original_data_layout(&fixture.promoted).unwrap();
                let shard_id = DfsMetadataShardId::new(&assignment.shard_id);
                let control = fixture.runtime.shared_control_plane().unwrap();
                let configs = fixture.runtime.metadata_shards().unwrap();
                let mut retiring = fixture.runtime.metadata_primary_service("/services/selected-original-data.bin").unwrap();
                let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                let previous = successor.set_transport_operation_deadline(Some(deadline));
                let (hook, reached, gate) = history_native_gate(fixture, true, deadline);
                std::thread::scope(|scope| {
                    let mut caller = Some(scope.spawn(|| successor.query_installed_original_history(assignment, &producer, &id)));
                    let mut retirement = None;
                    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        reached.recv_timeout(deadline.saturating_duration_since(Instant::now())).expect("real C history result reached the native post-execution edge");
                        let pending_query = queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap();
                        assert!(pending_query.response().is_none());
                        let history_request = pending_query.request().clone();
                        assert_eq!(history_request.current_assignment, *assignment);
                        // Explicit ordinary metadata retirement, not automatic
                        // failback and not a new selected-original protocol.
                        let pending = control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                            .request_metadata_shard_primary_handoff(assignment, &producer)).unwrap().unwrap();
                        let retirement_deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
                        retirement = Some(scope.spawn(move || retiring.reconcile_pending_metadata_shard_membership_handoffs(
                            &configs, retirement_deadline.saturating_duration_since(Instant::now()))));
                        loop {
                            let marker = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                                fixture.runtime.data_dir(&fixture.promoted), &shard_id, Some(&layout_b)).unwrap();
                            if marker.is_some_and(|marker| marker.handoff_id == pending.handoff_id && marker.retiring_epoch == assignment.epoch) { break; }
                            assert!(Instant::now() < deadline, "the actual B phase must be released before RPC wait so its native retirement can freeze");
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        release_history_gate(&gate);
                        let result = caller.take().unwrap().join().expect("owned in-flight query caller drained");
                        let error = result.expect_err("B retirement fences durable observation of an already prepared C result");
                        assert!(Instant::now() <= deadline, "retirement must reject under the same original 5s call, not a timeout");
                        let calls = fixture.runtime.rpc_observations();
                        let original = &history_request.original_material.original.command;
                        assert!(calls.iter().any(|decoded| decoded.source == fixture.promoted && decoded.target == fixture.holder
                            && decoded.route == DFS_DATA_READ_ROUTE && decoded.phase == RpcPhase::Decoded
                            && decoded.dfs_frame_id == Some(history_request.carrier.frame_id)
                            && decoded.dfs_request_id.as_deref() == Some(original.request_id.as_str())
                            && decoded.dfs_intent_id.as_deref() == Some(original.intent_id.as_str())
                            && decoded.dfs_file_system_id.as_ref() == Some(&original.file_system_id)
                            && decoded.dfs_object_id.as_ref() == Some(&original.object_id)
                            && decoded.dfs_version.as_ref() == Some(&original.version)
                            && matches!(decoded.signer.as_ref(), Some(sinomo_core::service::ServiceSigner::Remote { node_id, .. }) if node_id == fixture.promoted.as_str())
                            && calls.iter().any(|terminal| terminal.call_id == decoded.call_id && terminal.source == decoded.source
                                && terminal.target == decoded.target && terminal.route == decoded.route && terminal.origin == decoded.origin
                                && terminal.phase == RpcPhase::Completed && terminal.outcome_error.is_none()
                                && matches!(terminal.signer.as_ref(), Some(sinomo_core::service::ServiceSigner::Remote { node_id, .. }) if node_id == fixture.promoted.as_str()))),
                            "THIS persisted query carrier/original must complete its authenticated native C RPC before a B rejection is accepted");
                        let current_b_rejection = matches!(&error, DfsError::StaleOwnerEpoch { local_node_id, .. } if local_node_id == &fixture.promoted)
                            || matches!(&error, DfsError::Metadata { message } if message == "history query has no exact current native B owner/import"
                                || message == "publication configuration is not current" || message.contains("publication configuration advanced")
                                || message == "content-only Repair lacks its current native owner/admission/storage generation");
                        assert!(current_b_rejection, "the NEW B phase, not a C/network/timeout refusal, must fence observation: {error}");
                        let pending_query = queue.imported_history_query_for_test(&producer, &id).unwrap().unwrap();
                        assert_eq!(pending_query.request(), &history_request);
                        assert!(pending_query.response().is_none());
                    }));
                    // Gates and both actual children drain on every verdict.
                    release_history_gate(&gate);
                    if let Some(caller) = caller.take() { let _ = caller.join(); }
                    let retirement_panic = retirement.take().and_then(|retirement| match retirement.join() {
                        Ok(result) => { eprintln!("HISTORY_NATIVE_RETIREMENT_OUTCOME {result:?}; query asserts the actual retiring fence, not full activation"); None }
                        Err(error) => Some(error),
                    });
                    drop(hook);
                    if let Some(error) = retirement_panic {
                        if checked.is_ok() { std::panic::resume_unwind(error); }
                        eprintln!("native retirement child also panicked; retaining the original query assertion");
                    }
                    if let Err(error) = checked { std::panic::resume_unwind(error); }
                });
                successor.set_transport_operation_deadline(previous);
                assert_eq!(source_queue.original_repair(&fixture.birth.original).unwrap(), original_before);
                assert!(queue.closed_repair_birth(&fixture.birth.root.subject).unwrap().is_none());
            });
        }

        #[test]
        fn native_imported_history_query_preserves_applied_c_withdrawal() {
            let path = "/services/selected-original-data.bin";
            let mut fixture = genuine_native_original_with_receiver_foundation(
                "native-imported-history-applied-c-withdrawal", false, true,
            );
            let setup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                // This is a separate delayed-delivery phase; it does not renew
                // either prior genuine seed/first-effect clock.
                let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                let producer = fixture.shard.primary_node_id.clone();
                let original = fixture.birth.original.clone();
                let material = fixture.birth.original_wire_material().unwrap();
                let command = &material.original.command;
                let delayed = fixture.delayed_reply.clone().expect("the real C positive was captured before reply loss");
                let DfsNetworkMessage::RepairReplicaResponse(response) = &delayed.message else { panic!("actual C Repair response") };
                assert_eq!(delayed.source_node_id, fixture.holder);
                assert_eq!(delayed.target_node_id, producer);
                assert!(matches!(&response.result, DfsBlobTransferResult::Committed { content_hash } if content_hash == &command.content_hash));
                let mut validated = fixture.birth.clone();
                validated.observed.push(response.clone());
                validated.validate(&fixture.birth.root.subject.queue_incarnation).unwrap();
                let initial = fixture.runtime.metadata_primary_service(path).unwrap();
                assert!(initial.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap()
                    .iter().all(|receipt| receipt.replica_node_id != fixture.holder));
                drop(initial);
                let c_before = fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap().unwrap();
                assert_eq!(c_before.response.as_ref(), Some(response));
                assert!(c_before.bound_id.is_some() && c_before.terminal_id.is_some());
                assert_eq!(c_before.bytes, vec![0x5a;4096]);
                let layout = fixture.runtime.selected_original_data_layout(&producer).unwrap();
                let source_queue = layout.queue().unwrap();
                let denial = source_queue.deny_original_repair_close_for_test(&original).unwrap();
                assert!(source_queue.deny_original_repair_close_for_test(&original).err().unwrap()
                    .to_string().contains("already installed"));
                source_queue.reload_from_disk_on_next_access();
                source_queue.view().unwrap();
                assert_eq!(denial.hits(), 0, "arming/replay is not application or a synthetic reached fact");
                let mut c = fixture.runtime.client(&fixture.holder).unwrap();
                let previous = c.set_transport_operation_deadline(Some(deadline));
                // Preserve ALL actual C frame/message/effect identities. This
                // is its normal authenticated response route into native A.
                let delivered = c.transport.send_routed_call(&producer, DfsRoutedCall::value(delayed.clone()));
                assert_eq!(c.transport.operation_deadline(), Some(deadline));
                assert!(Instant::now() <= deadline, "the delayed response cannot renew its captured 5s clock");
                c.set_transport_operation_deadline(previous);
                drop(c);
                let error = delivered.expect_err("normal A publication must reach the negative common Close boundary");
                assert!(error.to_string().contains("injected original Repair close denial after actual application"), "{error}");
                assert!(denial.hits() > 0);
                let calls = fixture.runtime.rpc_observations();
                assert!(calls.iter().any(|decoded| decoded.source == fixture.holder && decoded.target == producer
                    && decoded.route == DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE && decoded.phase == RpcPhase::Decoded
                    && decoded.dfs_frame_id == Some(delayed.frame_id)
                    && decoded.dfs_request_id.as_deref() == Some(command.request_id.as_str())
                    && decoded.dfs_intent_id.as_deref() == Some(command.intent_id.as_str())
                    && decoded.dfs_file_system_id.as_ref() == Some(&command.file_system_id)
                    && decoded.dfs_object_id.as_ref() == Some(&command.object_id)
                    && decoded.dfs_version.as_ref() == Some(&command.version)
                    && matches!(decoded.signer.as_ref(), Some(sinomo_core::service::ServiceSigner::Remote { node_id, .. }) if node_id == fixture.holder.as_str())
                    && calls.iter().any(|terminal| terminal.call_id == decoded.call_id && terminal.source == decoded.source
                        && terminal.target == decoded.target && terminal.route == decoded.route && terminal.origin == decoded.origin
                        && terminal.phase == RpcPhase::EndedWithoutResponse
                        && terminal.outcome_error.as_deref().is_some_and(|error| error.contains("injected original Repair close denial after actual application"))
                        && matches!(terminal.signer.as_ref(), Some(sinomo_core::service::ServiceSigner::Remote { node_id, .. }) if node_id == fixture.holder.as_str()))),
                    "the actual authenticated C response identity must join its SAME call's owning Close error");
                let applied = fixture.runtime.metadata_primary_service(path).unwrap();
                let receipts = applied.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap();
                let c_receipt = receipts.iter().find(|receipt| receipt.replica_node_id == fixture.holder).unwrap().clone();
                assert_eq!(c_receipt.receipt.file_system_id, command.file_system_id);
                assert_eq!(c_receipt.receipt.object_id, command.object_id);
                assert_eq!(c_receipt.receipt.committed_version, command.version);
                assert_eq!(c_receipt.receipt.owner_node_id, producer);
                assert_eq!(c_receipt.content_hash.as_deref(), Some(command.content_hash.as_str()));
                assert!(applied.metadata_backend().unwrap().get_object(&command.object_id).unwrap().unwrap()
                    .current_locations.contains(&fixture.holder));
                drop(applied);
                let observed = source_queue.original_repair(&original).unwrap().unwrap();
                assert!(observed.observed.contains(response));
                assert!(observed.closed_at_sequence.is_none());
                assert!(source_queue.closed_repair_birth(&observed.root.subject).unwrap().is_none());
                fixture.runtime.client(&producer).unwrap().content().inspect_repair_payload_root(&observed.root).unwrap();
                // The normal maintenance path must also be denied. No new C
                // response/remote Repair is needed for its already-applied read.
                let before_recovery = denial.hits();
                let reply_count = fixture.runtime.rpc_observations().iter().filter(|call| call.source == fixture.holder && call.target == producer
                    && call.route == DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE).count();
                let remaining = deadline.checked_duration_since(Instant::now()).expect("same delayed-delivery/recovery budget");
                let tick = fixture.runtime.maintenance_tick(&producer, remaining).unwrap();
                assert!(Instant::now() <= deadline);
                let tick_error = tick.outcome.as_ref().expect_err("this explicit native maintenance invocation must reach the original Close denial");
                assert!(tick_error.contains("injected original Repair close denial after actual application"), "{tick_error}");
                assert!(denial.hits() > before_recovery, "actual native observed recovery must hit the common Close cut: {:?}", tick.outcome);
                assert_eq!(fixture.runtime.rpc_observations().iter().filter(|call| call.source == fixture.holder && call.target == producer
                    && call.route == DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE).count(), reply_count);
                let applied = fixture.runtime.metadata_primary_service(path).unwrap();
                assert_eq!(applied.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap(), receipts);
                drop(applied);
                let observed = source_queue.original_repair(&original).unwrap().unwrap();
                assert_eq!(observed.original, fixture.birth.original);
                assert_eq!(observed.root, fixture.birth.root);
                assert_eq!(observed.attachment, fixture.birth.attachment);
                assert_eq!(observed.attempts, fixture.birth.attempts);
                assert!(observed.observed.contains(response) && observed.closed_at_sequence.is_none());
                assert_eq!(fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap(), Some(c_before));
                // Capture the REAL post-delivery state, including known
                // Committed, without changing any original identity/body.
                fixture.birth = observed;
                (denial, c_receipt)
            }));
            let (denial, applied_c_receipt) = match setup {
                Ok(value) => value,
                Err(error) => {
                    // The setup's local denial/senders have already dropped.
                    checkpoint_failed_history_rpc(&fixture.runtime, "same-c-setup");
                    drop(fixture.held.take());
                    drop(fixture.held_promoted.take());
                    if let Err(close_error) = fixture.runtime.close() {
                        eprintln!("same-C setup cleanup also failed: {close_error}");
                    }
                    std::panic::resume_unwind(error);
                }
            };
            with_query_fixture_from_original(fixture, move |fixture, assignment, successor, queue| {
                let producer = fixture.shard.primary_node_id.clone();
                let material = fixture.birth.original_wire_material().unwrap();
                let command = &material.original.command;
                let source_layout = fixture.runtime.selected_original_data_layout(&producer).unwrap();
                let source_queue = source_layout.queue().unwrap();
                let imported = queue.installed_original_handoff_data(&producer, &command.request_id).unwrap().unwrap();
                assert_eq!(imported.material.birth, fixture.birth, "handoff must retain the complete real open+observed source state");
                let freeze = crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                    fixture.runtime.data_dir(&producer), &DfsMetadataShardId::new(&fixture.shard.shard_id), Some(&source_layout)).unwrap().unwrap();
                assert_eq!(freeze.retiring_epoch, fixture.shard.epoch);
                assert_eq!(freeze.handoff_id, imported.material.cut.handoff.handoff_id);
                assert_eq!(assignment.primary_node_id, fixture.promoted);
                assert_ne!(assignment.primary_node_id, producer);
                // The existing helper reached actual B activation; A's real
                // durable freeze/assignment, not this test guard, fences it now.
                denial.release().unwrap();
                let origin = source_queue.original_repair(&fixture.birth.original).unwrap().unwrap();
                assert_eq!(origin, fixture.birth);
                assert!(source_queue.closed_repair_birth(&origin.root.subject).unwrap().is_none());
                let backend = successor.metadata_backend().unwrap();
                let before_receipts = backend.receipts_for_version(&command.object_id, &command.version).unwrap();
                assert_eq!(before_receipts.iter().find(|receipt| receipt.replica_node_id == fixture.holder), Some(&applied_c_receipt));
                assert!(backend.get_object(&command.object_id).unwrap().unwrap().current_locations.contains(&fixture.holder));
                let intent_before = backend.get_intent(&command.object_id, &command.intent_id).unwrap().unwrap();
                assert!(intent_before.desired_replica_node_ids.contains(&fixture.holder));
                let c_before = fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap();
                successor.mark_remote_read_location_missing(&command.object_id, &command.version,
                    &Some(command.content_hash.clone()), &fixture.holder).unwrap();
                let backend = successor.metadata_backend().unwrap();
                let head = backend.get_object(&command.object_id).unwrap().unwrap();
                let version = backend.get_version(&command.object_id, &command.version).unwrap().unwrap();
                let receipts = backend.receipts_for_version(&command.object_id, &command.version).unwrap();
                let intent = backend.get_intent(&command.object_id, &command.intent_id).unwrap().unwrap();
                assert!(!head.current_locations.contains(&fixture.holder));
                assert!(!version.locations.contains(&fixture.holder));
                assert!(receipts.iter().all(|receipt| receipt.replica_node_id != fixture.holder));
                assert_eq!(intent.policy, intent_before.policy);
                assert_eq!(intent.desired_replica_node_ids, intent_before.desired_replica_node_ids);
                assert!(intent.desired_replica_node_ids.contains(&fixture.holder), "desired C plus absent canonical receipt retains the durable deficit");
                let rows = queue.view().unwrap().records;
                let response = query(fixture, assignment, successor);
                assert_terminal(&response, fixture);
                assert_eq!(query(fixture, assignment, successor), response);
                let backend = successor.metadata_backend().unwrap();
                assert_eq!(backend.get_object(&command.object_id).unwrap(), Some(head));
                assert_eq!(backend.get_version(&command.object_id, &command.version).unwrap(), Some(version));
                assert_eq!(backend.receipts_for_version(&command.object_id, &command.version).unwrap(), receipts);
                assert_eq!(backend.get_intent(&command.object_id, &command.intent_id).unwrap(), Some(intent));
                assert_eq!(queue.view().unwrap().records, rows);
                assert_eq!(queue.installed_original_handoff_data(&producer, &command.request_id).unwrap(), Some(imported));
                assert_eq!(source_queue.original_repair(&fixture.birth.original).unwrap(), Some(origin));
                assert!(source_queue.closed_repair_birth(&fixture.birth.root.subject).unwrap().is_none());
                assert!(queue.closed_repair_birth(&fixture.birth.root.subject).unwrap().is_none());
                assert!(queue.imported_executor_association(&producer, &command.request_id).unwrap().is_none());
                fixture.runtime.client(&producer).unwrap().content().inspect_repair_payload_root(&fixture.birth.root).unwrap();
                assert_eq!(fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap(), c_before);
                assert!(matches!(fixture.runtime.client(&fixture.promoted).unwrap().content().read_manifest(command.object_id.as_str(), command.version.as_str()),
                    Err(ContentStoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound));
            });
        }

        #[test]
        fn native_imported_history_query_preserves_later_withdrawal() {
            with_query_fixture("native-imported-history-known-withdrawal", true, |fixture, assignment, successor, queue| {
                let material = fixture.birth.original_wire_material().unwrap();
                let command = &material.original.command;
                // The real lost C reply left C absent from canonical
                // receipts. Withdraw the actually present A holder instead;
                // do not fabricate C application merely to seed this control.
                let producer = fixture.shard.primary_node_id.clone();
                let initial_head = successor.metadata_backend().unwrap().get_object(&command.object_id).unwrap().unwrap();
                let initial_receipts = successor.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap();
                assert!(initial_head.current_locations.contains(&producer));
                assert!(initial_receipts.iter().any(|receipt| receipt.replica_node_id == producer));
                assert!(initial_receipts.iter().all(|receipt| receipt.replica_node_id != fixture.holder));
                let source_queue = fixture.runtime.selected_original_data_layout(&producer).unwrap().queue().unwrap();
                let origin_before = source_queue.original_repair(&fixture.birth.original).unwrap();
                let c_before = fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap();
                // Ordinary authoritative withdrawal, not a query/cache effect.
                successor.mark_remote_read_location_missing(&command.object_id, &command.version,
                    &Some(command.content_hash.clone()), &producer).unwrap();
                let head = successor.metadata_backend().unwrap().get_object(&command.object_id).unwrap().unwrap();
                assert!(!head.current_locations.contains(&producer));
                assert!(!head.current_locations.contains(&fixture.holder));
                let receipts = successor.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap();
                assert!(receipts.iter().all(|receipt| receipt.replica_node_id != producer && receipt.replica_node_id != fixture.holder));
                let rows = queue.view().unwrap().records;
                let response = query(fixture, assignment, successor);
                assert_terminal(&response, fixture);
                assert_eq!(successor.metadata_backend().unwrap().get_object(&command.object_id).unwrap(), Some(head.clone()));
                assert_eq!(successor.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap(), receipts);
                assert_eq!(queue.view().unwrap().records, rows);
                fixture.runtime.stop_node(&fixture.holder).unwrap();
                fixture.runtime.reopen_node(fixture.holder.clone()).unwrap();
                assert_eq!(query(fixture, assignment, successor), response);
                assert_eq!(successor.metadata_backend().unwrap().get_object(&command.object_id).unwrap(), Some(head));
                assert!(queue.closed_repair_birth(&fixture.birth.root.subject).unwrap().is_none());
                assert!(queue.imported_executor_association(&fixture.shard.primary_node_id, &command.request_id).unwrap().is_none());
                assert_eq!(source_queue.original_repair(&fixture.birth.original).unwrap(), origin_before);
                assert_eq!(fixture.runtime.client(&fixture.holder).unwrap().content().original_repair_test_snapshot(&material).unwrap(), c_before);
                assert_eq!(successor.metadata_backend().unwrap().receipts_for_version(&command.object_id, &command.version).unwrap(), receipts);
            });
        }
    }

}
