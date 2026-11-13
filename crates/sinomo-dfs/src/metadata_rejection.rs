//! Typed current recovery decisions. Diagnostic text never grants repair or suppression.

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
    /// A write-set catch-up probe named a record generation this member's
    /// own view has not reached. Nothing is repaired; the primary asks again.
    WriteSetViewBehind,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsMetadataJournalBatchResult, DfsMetadataSnapshotInstallResult};

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
    fn current_rejection_requires_a_typed_code() {
        let missing = serde_json::json!({ "rejected": { "reason": "metadata journal prefix probe state mismatch" } });
        assert!(serde_json::from_value::<DfsMetadataJournalBatchResult>(missing.clone()).is_err());
        assert!(serde_json::from_value::<DfsMetadataSnapshotInstallResult>(missing).is_err());
    }
}
