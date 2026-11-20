//! Resident daemon-handoff policy for crash-safe mounted SIFS mutations.
//!
//! The embedding daemon owns scheduling and wake delivery. This module owns
//! the DFS durability contract: bounded outbox classification, repair-queue
//! retention, recovery claims, conservative full-recovery escalation, and
//! terminal recovery receipts.

use crate::{
    DfsError, DfsMessageCommittedWork, DfsPendingMaintenanceWork,
    MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS, MountedSifsMutationCapturedWork,
    MountedSifsMutationCorruptRecoveryClaim, MountedSifsMutationOutboxPhase,
    MountedSifsMutationOutboxStore, MountedSifsMutationRecoveryClaim,
    MountedSifsMutationRecoveryKind, MountedSifsMutationRecoveryReceipt, dfs_now_unix_nanos,
};

pub const DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE: &str = "/daemon/dfs/mounted-mutation/handoff-v1";
pub const DFS_MOUNTED_MUTATION_RPC_CATEGORY: &str = "daemon";
pub const DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES: usize =
    crate::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES;
use sinomo_core::local::SinomoDataDir;
use std::time::Duration;

pub const MOUNTED_SIFS_MUTATION_REPROBE_INTERVAL: Duration = Duration::from_secs(5);

/// One transition in the mounted-mutation durable handoff protocol.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum MountedSifsMutationHandoffRequest {
    Probe,
    ProbeFuse,
    Arm {
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
        arm_request_id: crate::MountedSifsMutationArmRequestId,
    },
    CommitApplied {
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
        arm_request_id: crate::MountedSifsMutationArmRequestId,
        captured_work: MountedSifsMutationCapturedWork,
    },
    CommitUnknown {
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
        arm_request_id: crate::MountedSifsMutationArmRequestId,
        observation: crate::MountedSifsMutationErrorObservation,
    },
    ProveNoWrite {
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
        arm_request_id: crate::MountedSifsMutationArmRequestId,
        proof_code: String,
    },
    AbortUnstarted {
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
    },
}

impl MountedSifsMutationHandoffRequest {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Probe | Self::ProbeFuse | Self::Arm { .. } | Self::AbortUnstarted { .. } => {
                Ok(())
            }
            Self::CommitApplied { .. } => Ok(()),
            Self::CommitUnknown { observation, .. } => {
                validate_label("error_code", &observation.error_code)
            }
            Self::ProveNoWrite { proof_code, .. } => validate_label("proof_code", proof_code),
        }
    }

    fn phase_and_marker_id(&self) -> (&'static str, Option<String>) {
        match self {
            Self::Probe => ("probe", None),
            Self::ProbeFuse => ("probe-fuse", None),
            Self::Arm { marker_id, .. } => ("arm", Some(marker_id.to_string())),
            Self::CommitApplied { marker_id, .. } => {
                ("commit-applied", Some(marker_id.to_string()))
            }
            Self::CommitUnknown { marker_id, .. } => {
                ("commit-unknown", Some(marker_id.to_string()))
            }
            Self::ProveNoWrite { marker_id, .. } => ("prove-no-write", Some(marker_id.to_string())),
            Self::AbortUnstarted { marker_id, .. } => {
                ("abort-unstarted", Some(marker_id.to_string()))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MountedSifsMutationHandoffPhase {
    Ready,
    Armed,
    DaemonAccepted,
    ProvenNoWrite,
    AbortedUnstarted,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct MountedSifsMutationHandoffResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marker_id: Option<crate::MountedSifsMutationMarkerId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arm_request_id: Option<crate::MountedSifsMutationArmRequestId>,
    pub phase: MountedSifsMutationHandoffPhase,
}

/// Host effects that cross from the DFS handoff protocol into the embedding
/// daemon's retained schedulers.
pub trait MountedSifsMutationHandoffHost {
    fn committed_work(&self, committed_work: DfsMessageCommittedWork);

    fn maintenance_needed(&self);
}

/// Apply one exact durable handoff transition and publish only the resulting
/// host edges. All marker validation and phase matching remain owner policy.
pub fn handle_mounted_sifs_mutation_handoff(
    data_dir: &SinomoDataDir,
    host: &dyn MountedSifsMutationHandoffHost,
    request: MountedSifsMutationHandoffRequest,
) -> Result<MountedSifsMutationHandoffResponse, DfsError> {
    request.validate().map_err(protocol_error)?;
    let (request_phase, request_marker_id) = request.phase_and_marker_id();
    tracing::debug!(
        target: "sinomo.dfs.mounted_mutation",
        phase = request_phase,
        marker_id = request_marker_id.as_deref(),
        "mounted-mutation owner received a handoff request"
    );
    let store = MountedSifsMutationOutboxStore::new(data_dir)?;
    let response = match request {
        MountedSifsMutationHandoffRequest::Probe | MountedSifsMutationHandoffRequest::ProbeFuse => {
            let (status, ready) = if matches!(request, MountedSifsMutationHandoffRequest::ProbeFuse)
            {
                store.fuse_probe_status()?
            } else {
                let status = store.status()?;
                let ready = status.pending_marker_count == 0 && !status.requires_full_recovery();
                (status, ready)
            };
            if !ready {
                if status.exact_recovery_marker_count > 0
                    || status.full_recovery_marker_count > 0
                    || status.malformed_marker_count > 0
                {
                    host.maintenance_needed();
                }
                return Err(protocol_error(format!(
                    "mounted-mutation handoff is not ready: pending={}, live={}, exact_recovery={}, full_recovery={}, unproven={}, malformed={}, overflow={}, directory_scan_overflow={}",
                    status.pending_marker_count,
                    status.live_marker_count,
                    status.exact_recovery_marker_count,
                    status.full_recovery_marker_count,
                    status.unproven_session_marker_count,
                    status.malformed_marker_count,
                    status.marker_limit_exceeded,
                    status.directory_scan_limit_exceeded,
                )));
            }
            MountedSifsMutationHandoffResponse {
                marker_id: None,
                arm_request_id: None,
                phase: MountedSifsMutationHandoffPhase::Ready,
            }
        }
        MountedSifsMutationHandoffRequest::Arm {
            marker_id,
            marker_nonce,
            arm_request_id,
        } => {
            store.arm_live(&marker_id, &marker_nonce, &arm_request_id)?;
            MountedSifsMutationHandoffResponse {
                marker_id: Some(marker_id),
                arm_request_id: Some(arm_request_id),
                phase: MountedSifsMutationHandoffPhase::Armed,
            }
        }
        MountedSifsMutationHandoffRequest::CommitApplied {
            marker_id,
            marker_nonce,
            arm_request_id,
            captured_work,
        } => {
            require_captured_marker(
                &store,
                &marker_id,
                &marker_nonce,
                &arm_request_id,
                &captured_work,
                None,
            )?;
            store.accept_captured_live(
                &marker_id,
                &marker_nonce,
                &arm_request_id,
                &captured_work,
            )?;
            host.committed_work(captured_work.committed_work);
            host.maintenance_needed();
            MountedSifsMutationHandoffResponse {
                marker_id: Some(marker_id),
                arm_request_id: Some(arm_request_id),
                phase: MountedSifsMutationHandoffPhase::DaemonAccepted,
            }
        }
        MountedSifsMutationHandoffRequest::CommitUnknown {
            marker_id,
            marker_nonce,
            arm_request_id,
            observation,
        } => {
            require_captured_marker(
                &store,
                &marker_id,
                &marker_nonce,
                &arm_request_id,
                &observation.captured_work,
                Some(&observation),
            )?;
            store.accept_unknown_live(&marker_id, &marker_nonce, &arm_request_id, &observation)?;
            host.committed_work(observation.captured_work.committed_work);
            host.maintenance_needed();
            MountedSifsMutationHandoffResponse {
                marker_id: Some(marker_id),
                arm_request_id: Some(arm_request_id),
                phase: MountedSifsMutationHandoffPhase::DaemonAccepted,
            }
        }
        MountedSifsMutationHandoffRequest::ProveNoWrite {
            marker_id,
            marker_nonce,
            arm_request_id,
            proof_code,
        } => {
            store.prove_no_write_live(&marker_id, &marker_nonce, &arm_request_id, &proof_code)?;
            MountedSifsMutationHandoffResponse {
                marker_id: Some(marker_id),
                arm_request_id: Some(arm_request_id),
                phase: MountedSifsMutationHandoffPhase::ProvenNoWrite,
            }
        }
        MountedSifsMutationHandoffRequest::AbortUnstarted {
            marker_id,
            marker_nonce,
        } => {
            store.abort_unstarted_live(&marker_id, &marker_nonce)?;
            MountedSifsMutationHandoffResponse {
                marker_id: Some(marker_id),
                arm_request_id: None,
                phase: MountedSifsMutationHandoffPhase::AbortedUnstarted,
            }
        }
    };
    tracing::debug!(
        target: "sinomo.dfs.mounted_mutation",
        phase = request_phase,
        marker_id = request_marker_id.as_deref(),
        response_phase = ?response.phase,
        "mounted-mutation owner completed a handoff request"
    );
    Ok(response)
}

fn validate_label(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("mounted-mutation handoff {field} cannot be empty"));
    }
    let observed = value.len();
    if observed > crate::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES {
        return Err(format!(
            "mounted-mutation handoff {field} is {observed} bytes; max is {}",
            crate::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES
        ));
    }
    Ok(())
}

fn protocol_error(detail: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: detail.into(),
    }
}

fn require_marker_identity(
    marker: &crate::MountedSifsMutationOutboxMarker,
    marker_id: &crate::MountedSifsMutationMarkerId,
    marker_nonce: &crate::MountedSifsMutationNonce,
) -> Result<(), DfsError> {
    if marker.schema_version != crate::MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
        || &marker.marker_id != marker_id
        || &marker.marker_nonce != marker_nonce
    {
        return Err(protocol_error(
            "mounted-mutation marker identity or schema does not match",
        ));
    }
    Ok(())
}

fn require_captured_marker(
    store: &MountedSifsMutationOutboxStore,
    marker_id: &crate::MountedSifsMutationMarkerId,
    marker_nonce: &crate::MountedSifsMutationNonce,
    arm_request_id: &crate::MountedSifsMutationArmRequestId,
    captured_work: &MountedSifsMutationCapturedWork,
    error_observation: Option<&crate::MountedSifsMutationErrorObservation>,
) -> Result<(), DfsError> {
    let marker = store
        .marker(marker_id)?
        .ok_or_else(|| protocol_error(format!("unknown mounted-mutation marker {marker_id}")))?;
    require_marker_identity(&marker, marker_id, marker_nonce)?;
    let matches = match &marker.phase {
        MountedSifsMutationOutboxPhase::Captured {
            arm_request_id: current_arm,
            captured_work: current_work,
        } => {
            error_observation.is_none()
                && current_arm == arm_request_id
                && current_work == captured_work
        }
        MountedSifsMutationOutboxPhase::UnknownAfterError {
            arm_request_id: current_arm,
            observation,
        } => {
            error_observation.is_some_and(|expected| expected == observation)
                && current_arm == arm_request_id
                && &observation.captured_work == captured_work
        }
        MountedSifsMutationOutboxPhase::DaemonAccepted {
            arm_request_id: current_arm,
            captured_work: current_work,
            requires_full_recovery: current_requires_full_recovery,
            error_observation: current_error_observation,
        } => {
            current_arm == arm_request_id
                && current_work == captured_work
                && current_requires_full_recovery == &error_observation.is_some()
                && current_error_observation.as_ref() == error_observation
        }
        _ => false,
    };
    if !matches {
        return Err(protocol_error(
            "mounted-mutation report does not match the exact fsynced marker phase and payload",
        ));
    }
    Ok(())
}

/// Every bounded owner that must be conservatively replayed when the exact
/// post-mutation report is unavailable or corrupt.
pub fn all_mounted_sifs_mutation_committed_work() -> DfsMessageCommittedWork {
    DfsMessageCommittedWork {
        publish_metadata: true,
        repair_maintenance: true,
        namespace_maintenance: true,
        metadata_replica_progress: true,
        metadata_materialization: true,
        semantic_changes: true,
    }
}

/// Read the monotonic startup latch and adapt replica materialization to the
/// global reconciliation owner used after a daemon restart.
pub fn mounted_sifs_mutation_startup_committed_work(
    data_dir: &SinomoDataDir,
) -> Result<Option<DfsMessageCommittedWork>, DfsError> {
    let latch = MountedSifsMutationOutboxStore::new(data_dir)?.daemon_committed_work_latch()?;
    if latch.generation == 0 {
        return Ok(None);
    }
    let mut committed_work = latch.committed_work;
    committed_work.metadata_replica_progress |= committed_work.metadata_materialization;
    Ok(Some(committed_work))
}

/// Project the bounded outbox into the common reactive-maintenance model.
pub fn mounted_sifs_mutation_pending_maintenance_work(
    data_dir: &SinomoDataDir,
    now_unix_nanos: u64,
) -> Result<DfsPendingMaintenanceWork, DfsError> {
    let status = MountedSifsMutationOutboxStore::new(data_dir)?.status()?;
    let overflow_count = status
        .pending_marker_count
        .saturating_sub(MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS)
        .saturating_add(usize::from(status.directory_scan_limit_exceeded));
    let blocked_marker_count = status
        .live_marker_count
        .saturating_add(status.deferred_live_session_marker_count)
        .saturating_add(status.unproven_session_marker_count)
        .saturating_add(status.malformed_marker_count)
        .saturating_add(overflow_count);
    let next_mounted_mutation_reprobe_unix_nanos = (blocked_marker_count > 0).then(|| {
        now_unix_nanos.saturating_add(
            MOUNTED_SIFS_MUTATION_REPROBE_INTERVAL
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64,
        )
    });
    Ok(DfsPendingMaintenanceWork {
        mounted_mutation_live_count: status
            .live_marker_count
            .saturating_add(status.deferred_live_session_marker_count),
        mounted_mutation_recoverable_exact_count: status.exact_recovery_marker_count,
        mounted_mutation_recoverable_full_count: status.full_recovery_marker_count,
        mounted_mutation_overflow_count: overflow_count,
        next_mounted_mutation_reprobe_unix_nanos,
        deferred_work_count: blocked_marker_count,
        ..DfsPendingMaintenanceWork::default()
    })
}

/// Claimed mounted-mutation recovery generation plus the already-durable
/// daemon work edges that its host must schedule.
#[derive(Debug, Default)]
pub struct MountedSifsMutationRecoveryBatch {
    claims: Vec<MountedSifsMutationRecoveryClaim>,
    corrupt_claims: Vec<MountedSifsMutationCorruptRecoveryClaim>,
    requires_full_recovery: bool,
    committed_work: Vec<DfsMessageCommittedWork>,
}

impl MountedSifsMutationRecoveryBatch {
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_claims_for_test(
        claims: Vec<MountedSifsMutationRecoveryClaim>,
        corrupt_claims: Vec<MountedSifsMutationCorruptRecoveryClaim>,
        requires_full_recovery: bool,
    ) -> Self {
        Self {
            claims,
            corrupt_claims,
            requires_full_recovery,
            ..Self::default()
        }
    }

    pub fn prepare(data_dir: &SinomoDataDir) -> Result<Self, DfsError> {
        let store = MountedSifsMutationOutboxStore::new(data_dir)?;
        let candidate_ids = store.recovery_candidate_marker_ids()?;
        let mut batch = Self::default();
        for marker_id in candidate_ids {
            if let Some(claim) = store.try_claim_corrupt_recovery(&marker_id)? {
                batch.requires_full_recovery = true;
                batch.corrupt_claims.push(claim);
                continue;
            }
            let Some(claim) = store.try_claim_recovery(&marker_id)? else {
                continue;
            };
            let recovery_kind = claim.recovery_kind();
            if let Some(captured_work) = captured_work(claim.marker()) {
                store.retain_daemon_committed_work(captured_work.committed_work)?;
                batch.committed_work.push(captured_work.committed_work);
            }
            batch.requires_full_recovery |= recovery_kind == MountedSifsMutationRecoveryKind::Full;
            batch.claims.push(claim);
        }
        if batch.requires_full_recovery {
            // The exact mutation report may be unavailable. Retain every
            // bounded owner before the explicit recovery pass so another
            // crash cannot lose the conservative replay edge.
            let committed_work = all_mounted_sifs_mutation_committed_work();
            store.retain_daemon_committed_work(committed_work)?;
            batch.committed_work.push(committed_work);
        }
        Ok(batch)
    }

    pub fn requires_full_recovery(&self) -> bool {
        self.requires_full_recovery
    }

    pub fn committed_work(&self) -> &[DfsMessageCommittedWork] {
        &self.committed_work
    }

    pub fn complete(self, completed_without_limit: bool) -> Result<(), DfsError> {
        if !completed_without_limit {
            return Ok(());
        }
        let completed_at_unix_nanos = dfs_now_unix_nanos().max(1);
        for claim in self.claims {
            let full_recovery_completed =
                claim.recovery_kind() == MountedSifsMutationRecoveryKind::Full;
            claim.terminalize(recovery_receipt(
                completed_at_unix_nanos,
                full_recovery_completed,
            ))?;
        }
        for claim in self.corrupt_claims {
            claim.terminalize(recovery_receipt(completed_at_unix_nanos, true))?;
        }
        Ok(())
    }
}

fn captured_work(
    marker: &crate::MountedSifsMutationOutboxMarker,
) -> Option<&MountedSifsMutationCapturedWork> {
    match &marker.phase {
        MountedSifsMutationOutboxPhase::Captured { captured_work, .. }
        | MountedSifsMutationOutboxPhase::DaemonAccepted { captured_work, .. } => {
            Some(captured_work)
        }
        MountedSifsMutationOutboxPhase::UnknownAfterError { observation, .. } => {
            Some(&observation.captured_work)
        }
        _ => None,
    }
}

fn recovery_receipt(
    completed_at_unix_nanos: u64,
    full_recovery_completed: bool,
) -> MountedSifsMutationRecoveryReceipt {
    MountedSifsMutationRecoveryReceipt {
        metadata_publish_durably_owned: true,
        maintenance_durably_owned: true,
        metadata_replica_progress_durably_owned: true,
        full_recovery_completed,
        completed_at_unix_nanos,
    }
}
