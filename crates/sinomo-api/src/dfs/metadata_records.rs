//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::DfsWriteReceipt;
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use time::OffsetDateTime;

/// Record field presence while decoding the field's actual wire type. SOBJ
/// encodes a map or string directly here, rather than an Option wrapper; a
/// missing field is supplied by serde's default and remains None.
#[doc(hidden)]
pub fn deserialize_present_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Stable identifier for a DFS metadata shard.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsMetadataShardId(pub String);

impl DfsMetadataShardId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardStatus {
    pub shard_id: DfsMetadataShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    pub journal_next_sequence: u64,
    pub journal_last_sequence: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_lag: Vec<DfsMetadataReplicaLagStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replay_cursors: Vec<DfsMetadataReplayCursorStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataReplicaLagStatus {
    pub target_node_id: NodeId,
    pub replicated_through_sequence: u64,
    pub lag_entries: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataReplayCursorStatus {
    pub source_shard_id: DfsMetadataShardId,
    pub replayed_through_sequence: u64,
}

/// Content replication grant, independent of metadata generations.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentDurability {
    /// Set only when legacy content received a fresh grant under the current
    /// filesystem policy. Absence denotes recovered/original write evidence.
    /// This is audit provenance; the replica set and W/N govern durability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_policy_granted_at_unix_nanos: Option<u64>,
    /// Data-write generation; metadata-only successors may outlive this row.
    pub source_version: DfsVersion,
    pub coordinator_node_id: NodeId,
    pub owner_epoch: u64,
    pub desired_replica_node_ids: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
}

/// Bounded opaque metadata owned by an independently versioned module.
/// The core preserves unknown namespaces and does not interpret their payloads.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsModuleMetadataValue {
    pub schema_version: u32,
    #[serde(with = "module_metadata_payload")]
    pub payload: serde_json::Value,
}

mod module_metadata_payload {
    use serde::de::{Error, SeqAccess, Visitor};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        payload: &serde_json::Value,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let bytes = serde_json::to_vec(payload).map_err(serde::ser::Error::custom)?;
        if bytes.len() > 64 * 1024 {
            return Err(serde::ser::Error::custom(
                "module metadata payload exceeds 64 KiB",
            ));
        }
        serializer.serialize_bytes(&bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<serde_json::Value, D::Error> {
        struct Payload;
        impl<'de> Visitor<'de> for Payload {
            type Value = serde_json::Value;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("bounded canonical JSON bytes")
            }
            fn visit_bytes<E: Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
                if bytes.len() > 64 * 1024 {
                    return Err(E::custom("module metadata payload exceeds 64 KiB"));
                }
                serde_json::from_slice(bytes).map_err(E::custom)
            }
            fn visit_byte_buf<E: Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
                self.visit_bytes(&bytes)
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut bytes =
                    Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(64 * 1024));
                while let Some(byte) = sequence.next_element::<u8>()? {
                    if bytes.len() == 64 * 1024 {
                        return Err(A::Error::custom("module metadata payload exceeds 64 KiB"));
                    }
                    bytes.push(byte);
                }
                self.visit_bytes(&bytes)
            }
        }
        deserializer.deserialize_any(Payload)
    }
}

/// A minimum retention budget. Count and age are independent grants: either
/// one keeps content live. Reconfiguration cannot revoke an accepted minimum.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRetentionMinimum {
    #[serde(default)]
    pub keep_last: u32,
    #[serde(default)]
    pub keep_for_seconds: u64,
}

impl DfsRetentionMinimum {
    pub fn is_disabled(&self) -> bool {
        self.keep_last == 0 && self.keep_for_seconds == 0
    }

    pub fn retains(&self, granted_at: u64, now: u64, newer_grants: usize) -> bool {
        newer_grants < self.keep_last as usize
            || (self.keep_for_seconds != 0
                && now.saturating_sub(granted_at)
                    < self.keep_for_seconds.saturating_mul(1_000_000_000))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsOrderedRetentionGrant {
    pub order: u64,
    pub granted_at_unix_nanos: u64,
    pub minimum: DfsRetentionMinimum,
}

/// Generic origin of the reference effect; module-specific provenance stays
/// in that module's opaque metadata namespace.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsRetentionGrantOrigin {
    Rule,
    ExplicitOperation,
}

/// A namespaced declarative grant contribution. The longest matching path
/// wins within each namespace; different namespaces compose independently.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRetentionRule {
    pub path: String,
    pub minimum: DfsRetentionMinimum,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRetentionPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<DfsRetentionRule>,
}

impl DfsRetentionPolicy {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn minimum_for_path(&self, path: &str) -> DfsRetentionMinimum {
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
            .map(|rule| rule.minimum.clone())
            .unwrap_or_default()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.rules.len() > 256 {
            return Err("retention policy exceeds 256 rules".into());
        }
        let mut paths = std::collections::BTreeSet::new();
        for rule in &self.rules {
            if !rule.path.starts_with('/')
                || rule.path.contains('\0')
                || rule.path.len() > 4096
                || (rule.path != "/"
                    && rule.path[1..]
                        .split('/')
                        .any(|part| part.is_empty() || part == "." || part == ".."))
                || !paths.insert(&rule.path)
            {
                return Err("retention rule requires a distinct normalized absolute path".into());
            }
        }
        Ok(())
    }
}

pub fn validate_retention_policies(
    policies: &std::collections::BTreeMap<String, DfsRetentionPolicy>,
) -> Result<(), String> {
    if policies.len() > 64 {
        return Err("retention configuration exceeds 64 namespaces".into());
    }
    let mut rules = 0usize;
    for (namespace, policy) in policies {
        validate_metadata_namespace(namespace)?;
        policy.validate()?;
        rules = rules.saturating_add(policy.rules.len());
    }
    if rules > 1024 {
        return Err("retention configuration exceeds 1024 aggregate rules".into());
    }
    Ok(())
}

/// Immutable content evidence and a minimum grant owned by one namespace.
/// This snapshot describes bytes; current object authority controls access.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRevisionRetentionGrant {
    pub object: Box<DfsObjectMetadata>,
    pub granted_at_unix_nanos: u64,
    pub origin: DfsRetentionGrantOrigin,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<DfsOrderedRetentionGrant>,
}

/// Object-local order follows successors and handoffs even after older grants
/// are collected. Every namespace has an independent stream and grant.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRevisionRetentionState {
    #[serde(default)]
    pub order: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<Box<DfsRevisionRetentionGrant>>,
}

/// Encoding provenance survives replay, exact digests and snapshots.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DfsRevisionMetadataFormat {
    Legacy,
    #[default]
    NamespacedV6,
}

/// Metadata for one immutable object revision. Product-specific preservation
/// payloads are namespaced; core collection and repair use generic grants.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "DfsObjectVersionWire")]
pub struct DfsObjectVersionRecord {
    #[serde(skip)]
    pub metadata_format: DfsRevisionMetadataFormat,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub visible: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub retention: std::collections::BTreeMap<String, DfsRevisionRetentionState>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub module_metadata: std::collections::BTreeMap<String, DfsModuleMetadataValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_durability: Option<Box<DfsContentDurability>>,
}

#[derive(Serialize)]
struct DfsObjectVersionRef<'a> {
    file_system_id: &'a DfsFileSystemId,
    object_id: &'a DfsObjectId,
    version: &'a DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_hash: &'a Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    size_bytes: &'a Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    locations: &'a Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_by_event_id: &'a Option<EventId>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: &'a OffsetDateTime,
    visible: &'a bool,
    // Presence also distinguishes a cleared V6 row from an older writer's
    // omitted extension state. Preserve it even after the last namespace clears.
    retention: &'a std::collections::BTreeMap<String, DfsRevisionRetentionState>,
    module_metadata: &'a std::collections::BTreeMap<String, DfsModuleMetadataValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_durability: &'a Option<Box<DfsContentDurability>>,
}

impl Serialize for DfsObjectVersionRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.metadata_format == DfsRevisionMetadataFormat::Legacy {
            return crate::dfs::file_history::serialize_legacy_version(self, serializer);
        }
        DfsObjectVersionRef {
            file_system_id: &self.file_system_id,
            object_id: &self.object_id,
            version: &self.version,
            content_hash: &self.content_hash,
            size_bytes: &self.size_bytes,
            locations: &self.locations,
            created_by_event_id: &self.created_by_event_id,
            created_at: &self.created_at,
            visible: &self.visible,
            retention: &self.retention,
            module_metadata: &self.module_metadata,
            content_durability: &self.content_durability,
        }
        .serialize(serializer)
    }
}

impl PartialEq for DfsObjectVersionRecord {
    fn eq(&self, other: &Self) -> bool {
        self.file_system_id == other.file_system_id
            && self.object_id == other.object_id
            && self.version == other.version
            && self.content_hash == other.content_hash
            && self.size_bytes == other.size_bytes
            && self.locations == other.locations
            && self.created_by_event_id == other.created_by_event_id
            && self.created_at == other.created_at
            && self.visible == other.visible
            && self.retention == other.retention
            && self.module_metadata == other.module_metadata
            && self.content_durability == other.content_durability
    }
}
impl Eq for DfsObjectVersionRecord {}

#[derive(Deserialize)]
struct DfsObjectVersionWire {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub visible: bool,
    #[serde(default, deserialize_with = "deserialize_present_value")]
    retention: Option<std::collections::BTreeMap<String, DfsRevisionRetentionState>>,
    #[serde(default, deserialize_with = "deserialize_present_value")]
    module_metadata: Option<std::collections::BTreeMap<String, DfsModuleMetadataValue>>,
    #[serde(default)]
    history_order: u64,
    #[serde(default)]
    retained: Option<Box<DfsRetainedFileVersion>>,
    #[serde(default)]
    content_durability: Option<Box<DfsContentDurability>>,
}

impl TryFrom<DfsObjectVersionWire> for DfsObjectVersionRecord {
    type Error = String;

    fn try_from(wire: DfsObjectVersionWire) -> Result<Self, Self::Error> {
        let metadata_format = if wire.retention.is_some() || wire.module_metadata.is_some() {
            DfsRevisionMetadataFormat::NamespacedV6
        } else {
            DfsRevisionMetadataFormat::Legacy
        };
        let mut record = Self {
            metadata_format,
            file_system_id: wire.file_system_id,
            object_id: wire.object_id,
            version: wire.version,
            content_hash: wire.content_hash,
            size_bytes: wire.size_bytes,
            locations: wire.locations,
            created_by_event_id: wire.created_by_event_id,
            created_at: wire.created_at,
            visible: wire.visible,
            retention: wire.retention.unwrap_or_default(),
            module_metadata: wire.module_metadata.unwrap_or_default(),
            content_durability: wire.content_durability,
        };
        // Compatibility is isolated here: authored records never emit legacy
        // fields, and the packed-v6 gate owns admission of this representation.
        crate::dfs::file_history::migrate_legacy_version_metadata(
            &mut record,
            wire.history_order,
            wire.retained,
        )?;
        record.validate_extension_metadata()?;
        Ok(record)
    }
}

impl DfsObjectVersionRecord {
    pub fn has_retention_grant(&self) -> bool {
        self.retention.values().any(|state| state.grant.is_some())
    }

    pub fn retention_grant(&self, namespace: &str) -> Option<&DfsRevisionRetentionGrant> {
        self.retention.get(namespace)?.grant.as_deref()
    }

    pub fn retention_order(&self, namespace: &str) -> u64 {
        self.retention.get(namespace).map_or(0, |state| state.order)
    }

    pub fn validate_extension_metadata(&self) -> Result<(), String> {
        validate_module_metadata_map(&self.module_metadata)?;
        if self.retention.len() > 64 {
            return Err("revision retention exceeds 64 namespaces".into());
        }
        for (namespace, state) in &self.retention {
            validate_metadata_namespace(namespace)?;
            let Some(grant) = &state.grant else { continue };
            let object = &grant.object;
            if object.file_system_id != self.file_system_id
                || object.object_id != self.object_id
                || object.version != self.version
                || object.content_hash != self.content_hash
                || object.content_size_bytes != self.size_bytes
                || self.content_hash.is_none()
                || self.size_bytes.is_none()
                || object.tombstone_provenance.is_some()
                || !object.path.starts_with('/')
                || object.path.contains('\0')
                || object.path.len() > 4096
                || (object.path != "/"
                    && object.path[1..]
                        .split('/')
                        .any(|part| part.is_empty() || part == "." || part == ".."))
                || !matches!(
                    object.kind,
                    DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink
                )
            {
                return Err("revision grant does not match immutable content identity".into());
            }
            if grant.minimum.as_ref().is_some_and(|minimum| {
                minimum.minimum.is_disabled() || minimum.order == 0 || minimum.order > state.order
            }) || (grant.origin == DfsRetentionGrantOrigin::Rule && grant.minimum.is_none())
            {
                return Err("ordered grant requires a nonzero order and enabled minimum".into());
            }
        }
        Ok(())
    }
}

pub fn validate_metadata_namespace(namespace: &str) -> Result<(), String> {
    if namespace.is_empty()
        || namespace.len() > 256
        || namespace.contains('\0')
        || !namespace.contains('.')
        || namespace.split('.').any(|segment| {
            segment.is_empty()
                || !segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
    {
        return Err("module metadata requires a bounded dotted namespace".into());
    }
    Ok(())
}

pub fn validate_module_metadata_map(
    metadata: &std::collections::BTreeMap<String, DfsModuleMetadataValue>,
) -> Result<(), String> {
    if metadata.len() > 64 {
        return Err("module metadata exceeds 64 namespaces".into());
    }
    let mut total = 0usize;
    for (namespace, value) in metadata {
        validate_metadata_namespace(namespace)?;
        let bytes = serde_json::to_vec(&value.payload).map_err(|error| error.to_string())?;
        if value.schema_version == 0 || bytes.len() > 64 * 1024 {
            return Err("module metadata requires a version and payload of at most 64 KiB".into());
        }
        total = total.saturating_add(bytes.len());
    }
    if total > 256 * 1024 {
        return Err("module metadata exceeds the aggregate 256 KiB budget".into());
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaReceiptRecord {
    pub receipt: DfsWriteReceipt,
    pub replica_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub received_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentRecord {
    #[serde(default, skip_serializing_if = "DfsWritePublication::is_current")]
    pub publication: DfsWritePublication,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<DfsVersion>,
    pub target_version: DfsVersion,
    pub coordinator_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_order_token: Option<String>,
    #[serde(default)]
    pub owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub desired_replica_node_ids: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    pub status: DfsWriteIntentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
    /// Whether committing this intent makes a previously invisible directory
    /// entry visible and therefore owes the parent directory a POSIX mtime /
    /// version update. Older intent records predate this obligation and
    /// deserialize to `false`.
    #[serde(default)]
    pub touch_parent_on_create: bool,
    /// Exact tombstone object occupying this path when a new transient object
    /// was created. Recovery may replace only this predecessor; a different
    /// live object or tombstone proves that a later namespace mutation won.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_predecessor_object_id: Option<DfsObjectId>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteIntentStatus {
    Staging,
    ReceiptsPending,
    Committed,
    Rejected,
    Conflict,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictRecord {
    pub conflict_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<DfsVersion>,
    pub local_version: DfsVersion,
    pub remote_version: DfsVersion,
    pub detected_by_node_id: NodeId,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_intent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    pub status: DfsConflictStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub detected_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsConflictStatus {
    Open,
    Resolved,
    Superseded,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for object scans within one filesystem.
pub struct DfsObjectPageCursor {
    /// Normalized object path at the end of the current page.
    pub path: String,
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[cfg(test)]
mod retention_metadata_tests {
    use super::*;
    use serde_json::json;

    fn legacy_version() -> serde_json::Value {
        let fs = DfsFileSystemId::generate();
        let object_id = DfsObjectId::generate();
        let version = DfsVersion::generate();
        let node = NodeId::generate();
        json!({
            "file_system_id":fs,"object_id":object_id,"version":version,
            "content_hash":"blake3:retained-content","size_bytes":7,
            "locations":[node],"created_at":"2026-10-03T00:00:00Z","visible":false,
            "history_order":3,
            "retained":{
                "object":{"file_system_id":fs,"object_id":object_id,"version":version,
                    "path":"/services/report","kind":"file","owner_node_id":node,
                    "content_hash":"blake3:retained-content","content_size_bytes":7,
                    "policy":"replicated-eventual"},
                "captured_at_unix_nanos":42,"source":{"kind":"retention-rule"},
                "policy_capture":{"order":3,"captured_at_unix_nanos":42,
                    "retention":{"keep_last":2,"keep_for_seconds":60}}
            }
        })
    }

    #[test]
    fn legacy_capture_migrates_without_losing_minimum_or_module_provenance() {
        let mut version: DfsObjectVersionRecord = serde_json::from_value(legacy_version()).unwrap();
        assert_eq!(version.metadata_format, DfsRevisionMetadataFormat::Legacy);
        assert!(
            serde_json::to_value(&version)
                .unwrap()
                .get("retained")
                .is_some()
        );
        let state = &version.retention[DFS_HISTORY_METADATA_NAMESPACE];
        assert_eq!(state.order, 3);
        let minimum = state.grant.as_ref().unwrap().minimum.as_ref().unwrap();
        assert_eq!(minimum.minimum.keep_last, 2);
        assert_eq!(minimum.minimum.keep_for_seconds, 60);
        assert_eq!(minimum.granted_at_unix_nanos, 42);
        version.metadata_format = DfsRevisionMetadataFormat::NamespacedV6;
        let encoded = serde_json::to_value(&version).unwrap();
        assert!(encoded.get("retained").is_none());
        assert!(encoded.get("history_order").is_none());
        assert_eq!(
            serde_json::from_value::<DfsObjectVersionRecord>(encoded).unwrap(),
            version
        );
    }

    #[test]
    fn unknown_module_payload_round_trips_and_obeys_bounds() {
        let mut version: DfsObjectVersionRecord = serde_json::from_value(legacy_version()).unwrap();
        version.module_metadata.insert(
            "vendor.research".into(),
            DfsModuleMetadataValue {
                schema_version: 17,
                payload: json!({"graph":{"nodes":[1,2],"relationship":"derived"}}),
            },
        );
        assert!(
            serde_json::to_vec(&version)
                .unwrap_err()
                .to_string()
                .contains("foreign")
        );
        version.metadata_format = DfsRevisionMetadataFormat::NamespacedV6;
        let encoded = serde_json::to_vec(&version).unwrap();
        assert_eq!(
            serde_json::from_slice::<DfsObjectVersionRecord>(&encoded).unwrap(),
            version
        );
        version
            .module_metadata
            .get_mut("vendor.research")
            .unwrap()
            .payload = json!("x".repeat(65 * 1024));
        assert!(
            version
                .validate_extension_metadata()
                .unwrap_err()
                .contains("64 KiB")
        );
    }

    #[test]
    fn contradictory_legacy_and_generic_grants_are_rejected() {
        let mut wire = legacy_version();
        let migrated: DfsObjectVersionRecord = serde_json::from_value(wire.clone()).unwrap();
        wire["retention"] = serde_json::to_value(migrated.retention).unwrap();
        wire["retention"][DFS_HISTORY_METADATA_NAMESPACE]["order"] = json!(99);
        assert!(
            serde_json::from_value::<DfsObjectVersionRecord>(wire)
                .unwrap_err()
                .to_string()
                .contains("conflicts")
        );
    }

    #[test]
    fn generic_grant_cannot_substitute_foreign_content_identity() {
        let mut version: DfsObjectVersionRecord = serde_json::from_value(legacy_version()).unwrap();
        version
            .retention
            .get_mut(DFS_HISTORY_METADATA_NAMESPACE)
            .unwrap()
            .grant
            .as_mut()
            .unwrap()
            .object
            .content_hash = Some("blake3:foreign".into());
        assert!(
            version
                .validate_extension_metadata()
                .unwrap_err()
                .contains("immutable content")
        );
    }

    #[test]
    fn direct_sobj_namespaced_maps_round_trip_without_option_wrappers() {
        use sinomo_core::object::{LengthMode, serde_ext};
        let mut version: DfsObjectVersionRecord = serde_json::from_value(legacy_version()).unwrap();
        version.metadata_format = DfsRevisionMetadataFormat::NamespacedV6;
        version.module_metadata.insert(
            "org.example.opaque".into(),
            DfsModuleMetadataValue {
                schema_version: 9,
                payload: json!([1, "text", true, null]),
            },
        );
        let bytes = serde_ext::to_bytes(&version, 6, LengthMode::U32).unwrap();
        assert_eq!(
            serde_ext::from_bytes::<DfsObjectVersionRecord>(&bytes).unwrap(),
            version
        );
        let fs = DfsFileSystem {
            metadata_format: DfsRevisionMetadataFormat::NamespacedV6,
            file_system_id: version.file_system_id.clone(),
            display_name: "retention".into(),
            owner_node_id: version.locations[0].clone(),
            subtree_policies: Vec::new(),
            retention_policies: std::collections::BTreeMap::from([(
                "org.example.rule".into(),
                DfsRetentionPolicy {
                    rules: vec![DfsRetentionRule {
                        path: "/services".into(),
                        minimum: DfsRetentionMinimum {
                            keep_last: 2,
                            keep_for_seconds: 60,
                        },
                    }],
                },
            )]),
            module_metadata: version.module_metadata.clone(),
        };
        let bytes = serde_ext::to_bytes(&fs, 6, LengthMode::U32).unwrap();
        assert_eq!(serde_ext::from_bytes::<DfsFileSystem>(&bytes).unwrap(), fs);
        let config = DfsFileSystemConfig::from(&fs);
        let bytes = serde_ext::to_bytes(&config, 6, LengthMode::U32).unwrap();
        assert_eq!(
            serde_ext::from_bytes::<DfsFileSystemConfig>(&bytes).unwrap(),
            config
        );
        // Legacy serializers and their byte identities stay unchanged.
        let legacy: DfsObjectVersionRecord = serde_json::from_value(legacy_version()).unwrap();
        let bytes = serde_ext::to_bytes(&legacy, 3, LengthMode::U32).unwrap();
        let decoded: DfsObjectVersionRecord = serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(
            serde_ext::to_bytes(&decoded, 3, LengthMode::U32).unwrap(),
            bytes
        );
    }

    #[test]
    fn cleared_namespaced_records_preserve_empty_state_and_encoding_provenance() {
        use sinomo_core::object::{LengthMode, serde_ext};
        let mut version: DfsObjectVersionRecord = serde_json::from_value(legacy_version()).unwrap();
        version.metadata_format = DfsRevisionMetadataFormat::NamespacedV6;
        version.retention.clear();
        version.module_metadata.clear();
        let json = serde_json::to_value(&version).unwrap();
        assert_eq!(json["retention"], json!({}));
        assert_eq!(json["module_metadata"], json!({}));
        let decoded: DfsObjectVersionRecord = serde_json::from_value(json).unwrap();
        assert_eq!(
            decoded.metadata_format,
            DfsRevisionMetadataFormat::NamespacedV6
        );
        let decoded: DfsObjectVersionRecord =
            serde_ext::from_value(&serde_ext::to_value(&version).unwrap()).unwrap();
        assert_eq!(
            decoded.metadata_format,
            DfsRevisionMetadataFormat::NamespacedV6
        );
        let bytes = serde_ext::to_bytes(&version, 6, LengthMode::U32).unwrap();
        let decoded: DfsObjectVersionRecord = serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(
            decoded.metadata_format,
            DfsRevisionMetadataFormat::NamespacedV6
        );
        assert!(decoded.retention.is_empty() && decoded.module_metadata.is_empty());

        let fs = DfsFileSystem {
            metadata_format: DfsRevisionMetadataFormat::NamespacedV6,
            file_system_id: version.file_system_id.clone(),
            display_name: "cleared".into(),
            owner_node_id: version.locations[0].clone(),
            subtree_policies: Vec::new(),
            retention_policies: Default::default(),
            module_metadata: Default::default(),
        };
        let decoded: DfsFileSystem =
            serde_json::from_value(serde_json::to_value(&fs).unwrap()).unwrap();
        assert_eq!(
            decoded.metadata_format,
            DfsRevisionMetadataFormat::NamespacedV6
        );
        let bytes = serde_ext::to_bytes(&fs, 6, LengthMode::U32).unwrap();
        let decoded: DfsFileSystem = serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(
            decoded.metadata_format,
            DfsRevisionMetadataFormat::NamespacedV6
        );
        let config = DfsFileSystemConfig::from(&fs);
        let decoded: DfsFileSystemConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(
            decoded.metadata_format,
            DfsRevisionMetadataFormat::NamespacedV6
        );
        let bytes = serde_ext::to_bytes(&config, 6, LengthMode::U32).unwrap();
        let decoded: DfsFileSystemConfig = serde_ext::from_bytes(&bytes).unwrap();
        assert_eq!(
            decoded.metadata_format,
            DfsRevisionMetadataFormat::NamespacedV6
        );
    }
}
