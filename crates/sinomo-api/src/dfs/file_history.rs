//! Explicitly retained file content. Commit generations are not file history.

use crate::dfs::{
    DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsObjectVersionRecord,
    DfsVersion,
};
use serde::{Deserialize, Serialize};

/// Retain captures within the age window OR among the newest configured count.
/// Zero for both fields disables automatic capture; independent pins still apply.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryRetention {
    #[serde(default)]
    pub keep_last: u32,
    #[serde(default)]
    pub keep_for_seconds: u64,
}

impl DfsFileHistoryRetention {
    pub fn is_disabled(&self) -> bool {
        self.keep_last == 0 && self.keep_for_seconds == 0
    }

    /// `newer_captures` counts other policy captures for the same object.
    pub fn retains(&self, captured_at: u64, now: u64, newer_captures: usize) -> bool {
        newer_captures < self.keep_last as usize
            || (self.keep_for_seconds != 0
                && now.saturating_sub(captured_at)
                    < self.keep_for_seconds.saturating_mul(1_000_000_000))
    }
}

/// A compact, explicitly configured subtree rule. The longest matching path wins.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryRule {
    pub path: String,
    pub retention: DfsFileHistoryRetention,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<DfsFileHistoryRule>,
}

impl DfsFileHistoryPolicy {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn retention_for_path(&self, path: &str) -> DfsFileHistoryRetention {
        self.rules
            .iter()
            .filter(|rule| {
                rule.path == "/"
                    || path == rule.path
                    || path
                        .strip_prefix(&rule.path)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            })
            .max_by_key(|rule| rule.path.len())
            .map(|rule| rule.retention.clone())
            .unwrap_or_default()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.rules.len() > 256 {
            return Err("file history policy has more than 256 rules".into());
        }
        let mut paths = std::collections::BTreeSet::new();
        for rule in &self.rules {
            if !rule.path.starts_with('/')
                || rule.path.contains('\0')
                || (rule.path != "/"
                    && rule.path[1..]
                        .split('/')
                        .any(|part| part.is_empty() || part == "." || part == ".."))
                || rule.path.len() > 4096
            {
                return Err("file history rule requires a normalized absolute path".into());
            }
            if !paths.insert(&rule.path) {
                return Err("file history policy repeats a path".into());
            }
        }
        Ok(())
    }
}

/// Why bytes were explicitly preserved. A write rejection is never a capture.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsFileVersionSource {
    Checkpoint {
        operation_id: String,
    },
    RetentionRule,
    /// A caller explicitly preserves staged local-operation bytes. This does
    /// not publish an alternative current object or mark DFS as conflicted.
    LocalOperation {
        operation_id: String,
        base_version: Option<DfsVersion>,
    },
}

/// Content durability and canonical namespace publication are separate effects.
/// A retained write is always an explicit request, never a conflict fallback.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsWritePublication {
    #[default]
    Current,
    Retained {
        capture: Box<DfsRetainedWritePublication>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRetainedWritePublication {
    pub pin_id: String,
    pub source: DfsFileVersionSource,
}

impl DfsWritePublication {
    pub fn is_current(&self) -> bool {
        matches!(self, Self::Current)
    }

    pub fn validate(&self) -> Result<(), String> {
        let Self::Retained { capture } = self else {
            return Ok(());
        };
        if capture.pin_id.trim().is_empty()
            || capture.pin_id.len() > 256
            || capture.pin_id.contains('\0')
        {
            return Err("retained content requires a bounded nonempty pin identity".into());
        }
        let operation_id = match &capture.source {
            DfsFileVersionSource::Checkpoint { operation_id }
            | DfsFileVersionSource::LocalOperation { operation_id, .. } => operation_id,
            DfsFileVersionSource::RetentionRule => {
                return Err(
                    "retention-rule captures belong to successful canonical publication".into(),
                );
            }
        };
        if operation_id.trim().is_empty() || operation_id.len() > 256 || operation_id.contains('\0')
        {
            return Err("retained content requires an explicit operation identity".into());
        }
        Ok(())
    }
}

/// The explicit rule evaluated when the current content was superseded.
/// Changing configuration affects future captures, not existing guarantees.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileVersionPolicyCapture {
    /// Assigned in authoritative object order, never inferred from wall time.
    pub order: u64,
    pub captured_at_unix_nanos: u64,
    pub retention: DfsFileHistoryRetention,
}

/// Provenance attached to the existing immutable content-version record.
/// Absence means an internal generation, never user-visible file history.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRetainedFileVersion {
    pub object: Box<DfsObjectMetadata>,
    pub captured_at_unix_nanos: u64,
    pub source: DfsFileVersionSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_capture: Option<DfsFileVersionPolicyCapture>,
}

impl DfsRetainedFileVersion {
    /// Validate nested identity before any journal/snapshot payload is applied.
    pub fn validate_for_version(&self, version: &DfsObjectVersionRecord) -> Result<(), String> {
        let object = &self.object;
        if object.file_system_id != version.file_system_id
            || object.object_id != version.object_id
            || object.version != version.version
            || object.content_hash != version.content_hash
            || object.content_size_bytes != version.size_bytes
            || version.content_hash.is_none()
            || version.size_bytes.is_none()
            || object.tombstone_provenance.is_some()
            || !matches!(
                object.kind,
                DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink
            )
        {
            return Err(
                "retained file version does not match its immutable content identity".into(),
            );
        }
        DfsFileHistoryPolicy {
            rules: vec![DfsFileHistoryRule {
                path: object.path.clone(),
                retention: DfsFileHistoryRetention::default(),
            }],
        }
        .validate()?;
        let operation = match &self.source {
            DfsFileVersionSource::Checkpoint { operation_id }
            | DfsFileVersionSource::LocalOperation { operation_id, .. } => Some(operation_id),
            DfsFileVersionSource::RetentionRule => None,
        };
        if operation.is_some_and(|id| id.trim().is_empty() || id.len() > 256 || id.contains('\0')) {
            return Err("file version capture requires a bounded operation identity".into());
        }
        if self.policy_capture.as_ref().is_some_and(|capture| {
            capture.retention.is_disabled()
                || capture.order == 0
                || capture.order > version.history_order
        }) || (matches!(self.source, DfsFileVersionSource::RetentionRule)
            && self.policy_capture.is_none())
        {
            return Err("policy capture requires an enabled explicit retention rule".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileVersionIdentity {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryEntry {
    pub identity: DfsFileVersionIdentity,
    pub captured: DfsRetainedFileVersion,
    pub content_hash: String,
    pub size_bytes: u64,
    pub pins: Vec<String>,
    pub is_current: bool,
    pub current_path: String,
}

/// The page cursor is tied to its query and authority generation by the server.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryCursor {
    pub shard_id: String,
    pub shard_epoch: u64,
    pub object_id: Option<DfsObjectId>,
    pub version: Option<DfsVersion>,
    pub topology: String,
    pub query: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryListRequest {
    pub file_system_id: DfsFileSystemId,
    /// Captured paths make deleted and replaced objects discoverable. An
    /// object selector follows identity across rename and hardlink aliases.
    pub path: Option<String>,
    pub object_id: Option<DfsObjectId>,
    pub after: Option<DfsFileHistoryCursor>,
    pub limit: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryListResponse {
    pub entries: Vec<DfsFileHistoryEntry>,
    pub next_cursor: Option<DfsFileHistoryCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsFileHistoryPinOperation {
    /// Capture the exact current generation. The first pin is mandatory.
    Capture {
        pin_id: String,
    },
    /// Add an independent pin to a version already explicitly retained.
    Pin {
        pin_id: String,
    },
    Unpin {
        pin_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPinRequest {
    /// Routing hint; the authority must also check the stable object identity.
    pub path: String,
    pub identity: DfsFileVersionIdentity,
    pub operation: DfsFileHistoryPinOperation,
    pub operation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryPinResponse {
    pub identity: DfsFileVersionIdentity,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryReadRequest {
    pub identity: DfsFileVersionIdentity,
    pub offset: u64,
    pub max_bytes: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryReadResponse {
    pub identity: DfsFileVersionIdentity,
    pub offset: u64,
    pub size_bytes: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum DfsFileHistoryReadOutcome {
    Found {
        data: DfsFileHistoryReadResponse,
    },
    NotRetained {
        identity: DfsFileVersionIdentity,
    },
    ContentUnavailable {
        identity: DfsFileVersionIdentity,
        detail: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryRestoreRequest {
    pub source: DfsFileVersionIdentity,
    pub destination_path: String,
    /// None means exclusive creation, including recovery of a deleted file.
    /// Overwrite requires the exact currently observed destination identity.
    pub expected_destination: Option<DfsFileVersionIdentity>,
    pub operation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryRestoreResponse {
    pub restored_from: DfsFileVersionIdentity,
    pub committed: DfsFileVersionIdentity,
    pub path: String,
}

/// Explicit preservation of caller-owned local-operation content. This is a
/// separate action from the failed/stale write whose base is recorded here.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPreserveRequest {
    /// Canonical lookup hint; it cannot substitute a different object identity.
    pub path: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub local_operation_id: String,
    pub base_version: Option<DfsVersion>,
    pub pin_id: String,
    pub operation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum DfsFileHistoryPreserveOutcome {
    Retained {
        identity: DfsFileVersionIdentity,
    },
    Pending {
        identity: DfsFileVersionIdentity,
        intent_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPolicyUpdateRequest {
    pub file_system_id: DfsFileSystemId,
    /// Compare-and-set prevents an administrator overwriting another edit.
    pub expected: DfsFileHistoryPolicy,
    pub policy: DfsFileHistoryPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryPolicyRequest {
    pub file_system_id: DfsFileSystemId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_is_disabled_without_an_explicit_rule() {
        let policy = DfsFileHistoryPolicy::default();
        assert!(policy.retention_for_path("/users/a/file").is_disabled());
        assert!(!DfsFileHistoryRetention::default().retains(0, 0, 0));
    }

    #[test]
    fn retention_keeps_the_union_of_count_and_time() {
        let retention = DfsFileHistoryRetention {
            keep_last: 2,
            keep_for_seconds: 10,
        };
        assert!(retention.retains(0, 100_000_000_000, 1));
        assert!(retention.retains(0, 9_000_000_000, 20));
        assert!(!retention.retains(0, 10_000_000_000, 2));
        assert!(
            retention.retains(10, 0, 20),
            "clock rollback retains conservatively"
        );
    }

    #[test]
    fn retention_rules_use_path_components_and_allow_explicit_disable() {
        let policy = DfsFileHistoryPolicy {
            rules: vec![
                DfsFileHistoryRule {
                    path: "/users".into(),
                    retention: DfsFileHistoryRetention {
                        keep_last: 3,
                        keep_for_seconds: 0,
                    },
                },
                DfsFileHistoryRule {
                    path: "/users/cache".into(),
                    retention: DfsFileHistoryRetention::default(),
                },
            ],
        };
        policy.validate().unwrap();
        assert_eq!(policy.retention_for_path("/users/a").keep_last, 3);
        assert!(policy.retention_for_path("/users/cache/file").is_disabled());
        assert!(policy.retention_for_path("/users2/a").is_disabled());
        let mut invalid = policy;
        invalid.rules[0].path = "/users/../services".into();
        assert!(invalid.validate().is_err());
    }
}
