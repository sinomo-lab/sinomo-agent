//! Typed recovery decisions and the single compatibility adapter for legacy
//! peers and persisted records that only supplied diagnostic text.

use crate::metadata_replication::DFS_METADATA_UNREADABLE_REPLICA_SNAPSHOT_REPAIR_REASON;
use serde::{Deserialize, Serialize};

/// A local rejection before it is projected onto a wire response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataRejection {
    pub code: DfsMetadataRejectionCode,
    pub reason: String,
}

impl DfsMetadataRejection {
    pub fn new(code: DfsMetadataRejectionCode, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }
}

impl From<String> for DfsMetadataRejection {
    fn from(reason: String) -> Self {
        Self::new(DfsMetadataRejectionCode::Other, reason)
    }
}

impl From<&str> for DfsMetadataRejection {
    fn from(reason: &str) -> Self {
        Self::from(reason.to_owned())
    }
}

impl From<&String> for DfsMetadataRejection {
    fn from(reason: &String) -> Self {
        Self::from(reason.clone())
    }
}

impl From<&DfsMetadataRejection> for DfsMetadataRejection {
    fn from(rejection: &DfsMetadataRejection) -> Self {
        rejection.clone()
    }
}

impl std::fmt::Display for DfsMetadataRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl From<crate::MetadataBackendError> for DfsMetadataRejection {
    fn from(error: crate::MetadataBackendError) -> Self {
        Self::new(error.rejection_code(), error.to_string())
    }
}

impl From<crate::DfsError> for DfsMetadataRejection {
    fn from(error: crate::DfsError) -> Self {
        Self::new(error.metadata_rejection_code(), error.to_string())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataRejectionCode {
    ReplicaDiverged,
    ReplicaUnreadable,
    PrefixMismatch,
    PrimaryMismatch,
    /// Explicitly non-repairable here. Unknown future codes fail closed and
    /// never fall back to interpreting the accompanying diagnostic text.
    #[serde(other)]
    Other,
}

impl DfsMetadataRejectionCode {
    pub fn allows_snapshot_repair(self) -> bool {
        matches!(
            self,
            Self::ReplicaDiverged | Self::ReplicaUnreadable | Self::PrefixMismatch
        )
    }

    pub fn suppresses_replication(self) -> bool {
        self == Self::PrimaryMismatch
    }

    pub(crate) fn from_legacy_reason(reason: &str) -> Self {
        if legacy_is_primary_mismatch(reason) {
            Self::PrimaryMismatch
        } else if legacy_allows_snapshot_repair(reason) {
            if reason.contains("metadata journal prefix probe") {
                Self::PrefixMismatch
            } else {
                Self::ReplicaDiverged
            }
        } else {
            Self::Other
        }
    }

    pub(crate) fn resolve(code: Option<Self>, reason: &str) -> Self {
        code.unwrap_or_else(|| Self::from_legacy_reason(reason))
    }
}

// Retained for existing compatibility fixtures. Production decisions resolve
// the optional wire/storage code and then operate on the enum.
#[cfg(test)]
pub(crate) fn metadata_journal_rejection_allows_snapshot_repair(reason: &str) -> bool {
    DfsMetadataRejectionCode::from_legacy_reason(reason).allows_snapshot_repair()
}

#[cfg(test)]
pub(crate) fn metadata_replication_rejection_is_topology_mismatch(reason: &str) -> bool {
    DfsMetadataRejectionCode::from_legacy_reason(reason).suppresses_replication()
}

fn legacy_allows_snapshot_repair(reason: &str) -> bool {
    reason.contains(DFS_METADATA_UNREADABLE_REPLICA_SNAPSHOT_REPAIR_REASON)
        || (reason.contains("metadata table snapshot")
            && reason.contains("semantic checksum mismatch"))
        || (reason.contains("metadata journal sequence")
            && reason.contains("already exists with a different entry"))
        || (reason.contains("metadata journal sequence")
            && reason.contains("already exists with different exact bytes"))
        || (reason.contains("metadata journal sequence")
            && reason.contains("within durable prefix")
            && reason.contains("is missing while replaying shard"))
        // A routed response can materialize a newer authoritative namespace
        // state on a replica before its incremental journal cursor reaches the
        // prepare that produced it. Replaying that older prepare then fails
        // its admission checks even though the primary history is valid. The
        // replica cannot safely skip the entry; install the primary snapshot.
        || (reason.contains("namespace prepare")
            && (reason.contains("version expectation changed")
                || reason.contains("occupant expectation changed")
                || reason.contains("overlaps active prepare")))
        || reason.contains("failed to decode segmented journal entry")
        || (reason.contains("metadata journal prefix probe") && reason.contains("state mismatch"))
        || ((reason.contains("metadata journal batch")
            || reason.contains("metadata authority batch"))
            && reason.contains("starts unseen history")
            && reason.contains("durable replay requires"))
        || (reason.contains("metadata shard")
            && reason.contains("cannot extend prefix")
            && reason.contains("with journal entry"))
}

fn legacy_is_primary_mismatch(reason: &str) -> bool {
    // Missing/not-yet-configured replica state is expected while a member is
    // restarting and hydrating its authoritative shard projection. Persisting
    // that response for the entire shard epoch permanently suppresses catch-up
    // even after the receiver is healthy. Only an exact primary-authority
    // disagreement is terminal for this topology generation; receiver startup
    // rejections remain ordinary retryable failures.
    (reason.contains("metadata journal source")
        && reason.contains("is not shard")
        && reason.contains("primary"))
        || (reason.contains("metadata snapshot source")
            && reason.contains("is not shard")
            && reason.contains("primary"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsMetadataJournalBatchResult, DfsMetadataSnapshotInstallResult};
    use sinomo_core::object::serde_ext;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    enum LegacyRejection {
        Rejected { reason: String },
    }

    #[test]
    fn typed_rejection_overrides_diagnostic_wording_and_unknown_codes_fail_closed() {
        let diagnostic = "metadata journal sequence 1 already exists with a different entry";
        let result =
            DfsMetadataJournalBatchResult::rejected(DfsMetadataRejectionCode::Other, diagnostic);
        assert!(!result.rejection_code().unwrap().allows_snapshot_repair());
        let result = DfsMetadataJournalBatchResult::rejected(
            DfsMetadataRejectionCode::ReplicaDiverged,
            "reworded diagnostic",
        );
        assert!(result.rejection_code().unwrap().allows_snapshot_repair());
        let unknown: DfsMetadataJournalBatchResult = serde_json::from_value(serde_json::json!({
            "rejected": { "reason": diagnostic, "code": "future-rejection-code" }
        }))
        .unwrap();
        assert_eq!(
            unknown.rejection_code(),
            Some(DfsMetadataRejectionCode::Other)
        );
    }

    #[test]
    fn journal_and_snapshot_rejections_remain_compatible_with_legacy_sobj_peers() {
        let reason =
            "metadata journal sequence 1 already exists with a different entry".to_string();
        let legacy = LegacyRejection::Rejected {
            reason: reason.clone(),
        };
        let bytes = serde_ext::to_bytes(&legacy, 1, sinomo_core::object::LengthMode::U32).unwrap();
        let journal: DfsMetadataJournalBatchResult = serde_ext::from_bytes(&bytes).unwrap();
        let snapshot: DfsMetadataSnapshotInstallResult = serde_ext::from_bytes(&bytes).unwrap();
        assert!(journal.rejection_code().unwrap().allows_snapshot_repair());
        assert!(snapshot.rejection_code().unwrap().allows_snapshot_repair());
        let journal = DfsMetadataJournalBatchResult::rejected(
            DfsMetadataRejectionCode::ReplicaDiverged,
            reason.clone(),
        );
        let snapshot = DfsMetadataSnapshotInstallResult::rejected(
            DfsMetadataRejectionCode::ReplicaDiverged,
            reason,
        );
        assert_eq!(
            serde_ext::from_bytes::<LegacyRejection>(
                &serde_ext::to_bytes(&journal, 1, sinomo_core::object::LengthMode::U32).unwrap()
            )
            .unwrap(),
            legacy
        );
        assert_eq!(
            serde_ext::from_bytes::<LegacyRejection>(
                &serde_ext::to_bytes(&snapshot, 1, sinomo_core::object::LengthMode::U32).unwrap()
            )
            .unwrap(),
            legacy
        );
    }

    #[test]
    fn backend_recovery_code_survives_service_error_and_rpc_projection() {
        let backend = crate::MetadataBackendError::replica_diverged("a new diagnostic".to_string());
        let service = crate::DfsError::from(backend);
        let response = crate::dfs_metadata_journal_rejection_for(
            "request".to_string(),
            crate::DfsMetadataShardId::new("root"),
            7,
            service,
        );
        assert_eq!(
            response.result.rejection_code(),
            Some(DfsMetadataRejectionCode::ReplicaDiverged)
        );
    }

    #[test]
    fn stored_suppression_codes_take_precedence_over_legacy_diagnostics() {
        let diagnostic = "metadata journal source node-a is not shard root primary node-b";
        assert!(DfsMetadataRejectionCode::resolve(None, diagnostic).suppresses_replication());
        assert!(
            !DfsMetadataRejectionCode::resolve(Some(DfsMetadataRejectionCode::Other), diagnostic)
                .suppresses_replication()
        );
        assert!(
            DfsMetadataRejectionCode::resolve(
                Some(DfsMetadataRejectionCode::PrimaryMismatch),
                "reworded diagnostic"
            )
            .suppresses_replication()
        );
    }
}
