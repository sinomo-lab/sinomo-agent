//! Explicitly retained file content. Commit generations are not file history.

use crate::dfs::metadata_records::{
    DfsContentDurability, DfsModuleMetadataValue, DfsOrderedRetentionGrant,
    DfsRetentionGrantOrigin, DfsRetentionMinimum, DfsRetentionPolicy, DfsRetentionRule,
    DfsRevisionRetentionGrant, DfsRevisionRetentionState,
};
use crate::dfs::model::DfsSubtreePolicy;
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
    /// Internal ownership of repairs for content already explicitly retained.
    /// This is never an incoming write or permission to create a capture.
    RetainedRepair,
}

pub type DfsRetainedWritePublication = crate::dfs::lifecycle::DfsRetainedContentPublication;

impl DfsWritePublication {
    pub fn is_current(&self) -> bool {
        matches!(self, Self::Current)
    }

    pub fn is_retained_repair(&self) -> bool {
        matches!(self, Self::RetainedRepair)
    }

    pub fn validate(&self) -> Result<(), String> {
        if let Self::Retained { capture } = self {
            capture.validate()?;
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
                || capture.order > version.retention_order(DFS_HISTORY_METADATA_NAMESPACE)
        }) || (matches!(self.source, DfsFileVersionSource::RetentionRule)
            && self.policy_capture.is_none())
        {
            return Err("policy capture requires an enabled explicit retention rule".into());
        }
        Ok(())
    }
}

pub type DfsFileVersionIdentity = crate::dfs::lifecycle::DfsRevisionReference;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryEntry {
    pub identity: DfsFileVersionIdentity,
    pub captured: DfsRetainedFileVersion,
    pub content_hash: String,
    pub size_bytes: u64,
    pub pins: Vec<String>,
    pub is_current: bool,
    pub current_path: String,
    /// The object's current namespace entry is a deletion. Its retained
    /// content can be recovered only to an unoccupied path. Older authorities
    /// omit this field.
    #[serde(default)]
    pub deleted: bool,
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

pub type DfsFileHistoryRestoreRequest = crate::dfs::lifecycle::DfsContentCopyRequest;

pub type DfsFileHistoryRestoreResponse = crate::dfs::lifecycle::DfsContentCopyResponse;

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

pub type DfsFileHistoryPreserveOutcome = crate::dfs::lifecycle::DfsPreservedContentOutcome;

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

/// Namespace used by History's independently versioned persisted payloads.
pub const DFS_HISTORY_METADATA_NAMESPACE: &str = "module.dfs.history";

/// Decode-only compatibility for records written before generic retention.
/// New publication paths construct generic grants and opaque module metadata.
#[doc(hidden)]
pub fn migrate_legacy_version_metadata(
    version: &mut DfsObjectVersionRecord,
    history_order: u64,
    retained: Option<Box<DfsRetainedFileVersion>>,
) -> Result<(), String> {
    if history_order == 0 && retained.is_none() {
        return Ok(());
    }
    let grant = retained.as_ref().map(|capture| {
        Box::new(DfsRevisionRetentionGrant {
            object: capture.object.clone(),
            granted_at_unix_nanos: capture.captured_at_unix_nanos,
            origin: if matches!(capture.source, DfsFileVersionSource::RetentionRule) {
                DfsRetentionGrantOrigin::Rule
            } else {
                DfsRetentionGrantOrigin::ExplicitOperation
            },
            minimum: capture
                .policy_capture
                .as_ref()
                .map(|policy| DfsOrderedRetentionGrant {
                    order: policy.order,
                    granted_at_unix_nanos: policy.captured_at_unix_nanos,
                    minimum: DfsRetentionMinimum {
                        keep_last: policy.retention.keep_last,
                        keep_for_seconds: policy.retention.keep_for_seconds,
                    },
                }),
        })
    });
    let state = DfsRevisionRetentionState {
        order: history_order,
        grant,
    };
    if let Some(existing) = version.retention.get(DFS_HISTORY_METADATA_NAMESPACE) {
        if existing != &state {
            return Err("legacy history conflicts with namespaced retention".into());
        }
    } else {
        version
            .retention
            .insert(DFS_HISTORY_METADATA_NAMESPACE.into(), state);
    }
    if let Some(retained) = retained {
        retained.validate_for_version(version)?;
        let metadata = DfsModuleMetadataValue {
            schema_version: 1,
            payload: serde_json::to_value(&retained.source).map_err(|error| error.to_string())?,
        };
        if let Some(existing) = version.module_metadata.get(DFS_HISTORY_METADATA_NAMESPACE) {
            if existing != &metadata {
                return Err("legacy history conflicts with module metadata".into());
            }
        } else {
            version
                .module_metadata
                .insert(DFS_HISTORY_METADATA_NAMESPACE.into(), metadata);
        }
    }
    Ok(())
}

/// Decode-only migration of compact legacy configuration. Providers author
/// their generic policy namespace through the normal configuration API.
#[doc(hidden)]
pub fn migrate_legacy_retention_policy(
    policies: &mut std::collections::BTreeMap<String, DfsRetentionPolicy>,
    history: DfsFileHistoryPolicy,
) -> Result<(), String> {
    if history.is_empty() {
        return Ok(());
    }
    history.validate()?;
    let policy = DfsRetentionPolicy {
        rules: history
            .rules
            .into_iter()
            .map(|rule| DfsRetentionRule {
                path: rule.path,
                minimum: DfsRetentionMinimum {
                    keep_last: rule.retention.keep_last,
                    keep_for_seconds: rule.retention.keep_for_seconds,
                },
            })
            .collect(),
    };
    if let Some(existing) = policies.get(DFS_HISTORY_METADATA_NAMESPACE) {
        if existing != &policy {
            return Err("legacy history policy conflicts with namespaced retention".into());
        }
    } else {
        policies.insert(DFS_HISTORY_METADATA_NAMESPACE.into(), policy);
    }
    Ok(())
}

/// Re-emit a legacy revision solely for archived identity/proof verification.
/// Unknown namespaces fail instead of being dropped from an old representation.
pub(crate) fn serialize_legacy_version<S: serde::Serializer>(
    version: &DfsObjectVersionRecord,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::Error;
    if version
        .retention
        .keys()
        .chain(version.module_metadata.keys())
        .any(|namespace| namespace != DFS_HISTORY_METADATA_NAMESPACE)
    {
        return Err(S::Error::custom(
            "legacy revision cannot encode foreign namespaced metadata",
        ));
    }
    let retained = if let Some(grant) = version.retention_grant(DFS_HISTORY_METADATA_NAMESPACE) {
        let source =
            if let Some(metadata) = version.module_metadata.get(DFS_HISTORY_METADATA_NAMESPACE) {
                let source = metadata
                    .payload
                    .get("source")
                    .unwrap_or(&metadata.payload)
                    .clone();
                serde_json::from_value(source).map_err(S::Error::custom)?
            } else if grant.origin == DfsRetentionGrantOrigin::Rule {
                DfsFileVersionSource::RetentionRule
            } else {
                return Err(S::Error::custom(
                    "legacy explicit reference lacks its provenance",
                ));
            };
        Some(DfsRetainedFileVersion {
            object: grant.object.clone(),
            captured_at_unix_nanos: grant.granted_at_unix_nanos,
            source,
            policy_capture: grant
                .minimum
                .as_ref()
                .map(|minimum| DfsFileVersionPolicyCapture {
                    order: minimum.order,
                    captured_at_unix_nanos: minimum.granted_at_unix_nanos,
                    retention: DfsFileHistoryRetention {
                        keep_last: minimum.minimum.keep_last,
                        keep_for_seconds: minimum.minimum.keep_for_seconds,
                    },
                }),
        })
    } else {
        None
    };
    #[derive(Serialize)]
    struct LegacyVersion<'a> {
        file_system_id: &'a DfsFileSystemId,
        object_id: &'a DfsObjectId,
        version: &'a DfsVersion,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_hash: &'a Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        size_bytes: &'a Option<u64>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        locations: &'a Vec<sinomo_core::node::NodeId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        created_by_event_id: &'a Option<sinomo_core::events::EventId>,
        #[serde(with = "time::serde::rfc3339")]
        created_at: &'a time::OffsetDateTime,
        visible: bool,
        #[serde(skip_serializing_if = "zero")]
        history_order: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        retained: Option<DfsRetainedFileVersion>,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_durability: &'a Option<Box<DfsContentDurability>>,
    }
    fn zero(value: &u64) -> bool {
        *value == 0
    }
    LegacyVersion {
        file_system_id: &version.file_system_id,
        object_id: &version.object_id,
        version: &version.version,
        content_hash: &version.content_hash,
        size_bytes: &version.size_bytes,
        locations: &version.locations,
        created_by_event_id: &version.created_by_event_id,
        created_at: &version.created_at,
        visible: version.visible,
        history_order: version.retention_order(DFS_HISTORY_METADATA_NAMESPACE),
        retained,
        content_durability: &version.content_durability,
    }
    .serialize(serializer)
}

/// Legacy compact configuration retains its exact prior JSON field ordering.
#[allow(clippy::too_many_arguments)]
pub(crate) fn serialize_legacy_file_system<S: serde::Serializer>(
    file_system_id: &DfsFileSystemId,
    display_name: &str,
    owner_node_id: &sinomo_core::node::NodeId,
    subtree_policies: &Vec<DfsSubtreePolicy>,
    retention_policies: &std::collections::BTreeMap<String, DfsRetentionPolicy>,
    module_metadata: &std::collections::BTreeMap<String, DfsModuleMetadataValue>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::Error;
    if !module_metadata.is_empty()
        || retention_policies
            .keys()
            .any(|namespace| namespace != DFS_HISTORY_METADATA_NAMESPACE)
    {
        return Err(S::Error::custom(
            "legacy filesystem cannot encode new namespaced metadata",
        ));
    }
    let file_history = DfsFileHistoryPolicy {
        rules: retention_policies
            .get(DFS_HISTORY_METADATA_NAMESPACE)
            .map(|policy| {
                policy
                    .rules
                    .iter()
                    .map(|rule| DfsFileHistoryRule {
                        path: rule.path.clone(),
                        retention: DfsFileHistoryRetention {
                            keep_last: rule.minimum.keep_last,
                            keep_for_seconds: rule.minimum.keep_for_seconds,
                        },
                    })
                    .collect()
            })
            .unwrap_or_default(),
    };
    #[derive(Serialize)]
    struct LegacyFileSystem<'a> {
        file_system_id: &'a DfsFileSystemId,
        display_name: &'a str,
        owner_node_id: &'a sinomo_core::node::NodeId,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        subtree_policies: &'a Vec<DfsSubtreePolicy>,
        #[serde(skip_serializing_if = "DfsFileHistoryPolicy::is_empty")]
        file_history: DfsFileHistoryPolicy,
    }
    LegacyFileSystem {
        file_system_id,
        display_name,
        owner_node_id,
        subtree_policies,
        file_history,
    }
    .serialize(serializer)
}

/// Compatibility decoder kept beside the original feature wire vocabulary.
#[doc(hidden)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedContentPublicationWire {
    #[serde(
        default,
        deserialize_with = "crate::dfs::metadata_records::deserialize_present_value"
    )]
    namespace: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::dfs::metadata_records::deserialize_present_value"
    )]
    reference_id: Option<String>,
    #[serde(default)]
    metadata: Option<DfsModuleMetadataValue>,
    #[serde(
        default,
        deserialize_with = "crate::dfs::metadata_records::deserialize_present_value"
    )]
    pin_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::dfs::metadata_records::deserialize_present_value"
    )]
    source: Option<DfsFileVersionSource>,
}

impl TryFrom<RetainedContentPublicationWire>
    for crate::dfs::lifecycle::DfsRetainedContentPublication
{
    type Error = String;
    fn try_from(wire: RetainedContentPublicationWire) -> Result<Self, Self::Error> {
        let publication = if let Some(namespace) = wire.namespace {
            if wire.pin_id.is_some() || wire.source.is_some() {
                return Err("retained publication mixes legacy and namespaced fields".into());
            }
            Self {
                metadata_format: crate::dfs::DfsRevisionMetadataFormat::NamespacedV6,
                namespace,
                reference_id: wire
                    .reference_id
                    .ok_or("retained publication omitted its reference identity")?,
                metadata: wire.metadata,
            }
        } else {
            if wire.reference_id.is_some() || wire.metadata.is_some() {
                return Err("retained publication omitted its namespace".into());
            }
            let source = wire
                .source
                .ok_or("legacy retained publication omitted its provenance")?;
            let operation = match &source {
                DfsFileVersionSource::Checkpoint { operation_id }
                | DfsFileVersionSource::LocalOperation { operation_id, .. } => operation_id,
                DfsFileVersionSource::RetentionRule => {
                    return Err(
                        "retention-rule captures belong to successful canonical publication".into(),
                    );
                }
            };
            if operation.trim().is_empty() || operation.len() > 256 || operation.contains('\0') {
                return Err("legacy retained publication requires an operation identity".into());
            }
            Self {
                metadata_format: crate::dfs::DfsRevisionMetadataFormat::Legacy,
                namespace: DFS_HISTORY_METADATA_NAMESPACE.into(),
                reference_id: wire
                    .pin_id
                    .ok_or("legacy retained publication omitted its pin")?,
                metadata: Some(DfsModuleMetadataValue {
                    schema_version: 1,
                    payload: serde_json::to_value(source).map_err(|error| error.to_string())?,
                }),
            }
        };
        publication.validate()?;
        Ok(publication)
    }
}

pub(crate) fn serialize_retained_content_publication<S: serde::Serializer>(
    publication: &crate::dfs::lifecycle::DfsRetainedContentPublication,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::Error;
    if publication.metadata_format == crate::dfs::DfsRevisionMetadataFormat::Legacy {
        if publication.namespace != DFS_HISTORY_METADATA_NAMESPACE {
            return Err(S::Error::custom(
                "legacy publication cannot encode a foreign namespace",
            ));
        }
        let metadata = publication
            .metadata
            .as_ref()
            .ok_or_else(|| S::Error::custom("legacy publication lacks its source"))?;
        if metadata.schema_version != 1 {
            return Err(S::Error::custom(
                "legacy publication cannot encode new metadata schema",
            ));
        }
        let source: DfsFileVersionSource =
            serde_json::from_value(metadata.payload.clone()).map_err(S::Error::custom)?;
        #[derive(Serialize)]
        struct LegacyPublication<'a> {
            pin_id: &'a str,
            source: DfsFileVersionSource,
        }
        LegacyPublication {
            pin_id: &publication.reference_id,
            source,
        }
        .serialize(serializer)
    } else {
        #[derive(Serialize)]
        struct Publication<'a> {
            namespace: &'a str,
            reference_id: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            metadata: &'a Option<DfsModuleMetadataValue>,
        }
        Publication {
            namespace: &publication.namespace,
            reference_id: &publication.reference_id,
            metadata: &publication.metadata,
        }
        .serialize(serializer)
    }
}

/// Decode-only provenance for an admitted pre-v6 capture request. New module
/// requests always supply their own opaque metadata contribution explicitly.
#[doc(hidden)]
pub fn legacy_checkpoint_reference_metadata(operation_id: &str) -> DfsModuleMetadataValue {
    DfsModuleMetadataValue {
        schema_version: 1,
        payload: serde_json::to_value(DfsFileVersionSource::Checkpoint {
            operation_id: operation_id.into(),
        })
        .expect("data-only checkpoint source"),
    }
}
