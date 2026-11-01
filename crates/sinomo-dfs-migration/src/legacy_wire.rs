//! Retained DTO conversion. Current DFS never imports this module.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinomo_dfs::*;

type Result<T, E = MetadataBackendError> = std::result::Result<T, E>;
fn invalid(message: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(message.to_string())
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(invalid)
}
fn encoded<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(invalid)
}
macro_rules! source_identifier {
    ($fn:ident,$ty:ty) => {
        fn $fn(value: Value) -> Result<Value> {
            let raw = value
                .as_str()
                .ok_or_else(|| invalid("retained identifier is not text"))?;
            let id = match <$ty>::new(raw) {
                Ok(id) => id,
                Err(_) => <$ty>::from_legacy_named(raw).map_err(invalid)?,
            };
            encoded(id)
        }
    };
}
source_identifier!(source_fs_id, DfsFileSystemId);
source_identifier!(source_object_id, DfsObjectId);
source_identifier!(source_version_id, DfsVersion);
source_identifier!(source_node_id, sinomo_core::node::NodeId);
source_identifier!(source_event_id, sinomo_core::events::EventId);
pub(crate) fn source_identity_fields(value: &mut Value) -> Result<()> {
    field(value, "file_system_id", source_fs_id)?;
    for key in [
        "object_id",
        "link_target_object_id",
        "canonical_object_id",
        "hardlink_object_id",
        "parent_object_id",
        "expected_parent_object_id",
        "source_object_id",
        "target_object_id",
        "expected_object_id",
        "create_predecessor_object_id",
    ] {
        field(value, key, source_object_id)?;
    }
    for key in [
        "version",
        "target_version",
        "base_version",
        "source_version",
        "previous_version",
        "expected_version",
        "committed_version",
        "visible_version",
        "owner_version",
    ] {
        // A table/effect wrapper can carry a complete version in this key.
        // Its schema-specific converter handles that object; an ID row is text.
        if matches!(key, "version" | "visible_version")
            && value.get(key).is_some_and(Value::is_object)
        {
            continue;
        }
        field(value, key, source_version_id)?;
    }
    for key in [
        "owner_node_id",
        "coordinator_node_id",
        "requester_node_id",
        "requesting_node_id",
        "replica_node_id",
        "node_id",
        "source_node_id",
        "target_node_id",
        "primary_node_id",
        "current_primary_node_id",
        "binding_owner_node_id",
    ] {
        field(value, key, source_node_id)?;
    }
    for key in [
        "created_by_event_id",
        "last_modified_by_event_id",
        "event_id",
    ] {
        field(value, key, source_event_id)?;
    }
    for key in [
        "current_locations",
        "locations",
        "desired_replica_node_ids",
        "desired_replica_nodes",
        "required_replica_node_ids",
        "replica_node_ids",
        "confirmed_replica_node_ids",
        "target_node_ids",
        "required_node_ids",
        "forbidden_node_ids",
        "replica_candidates",
    ] {
        if let Some(nodes) = value.get_mut(key).and_then(Value::as_array_mut) {
            for node in nodes {
                *node = source_node_id(node.take())?;
            }
        }
    }
    Ok(())
}
/// Normalize only typed leaf fields owned by the source schema. Opaque module
/// maps, metadata payloads, diagnostic strings and operation IDs are untouched.
pub(crate) fn normalize_source_leaf(mut value: Value) -> Result<Value> {
    if !value.is_object() {
        return Ok(value);
    }
    source_identity_fields(&mut value)?;
    if value.get("type").and_then(Value::as_str) == Some("put-intent")
        && value.get("status").and_then(Value::as_str) == Some("conflict")
    {
        value["status"] = Value::String("rejected".into());
    }
    for key in [
        "receipt",
        "identity",
        "content_reference_identity",
        "origin",
        "digest",
        "state",
        "tombstone_provenance",
        "response",
        "planned",
        "request",
        "result",
        "holder",
        "read_target",
        "write_target",
        "retention",
        "stat",
    ] {
        field(&mut value, key, normalize_source_leaf)?;
    }
    for key in ["applied", "rejected", "Applied", "Rejected"] {
        field(&mut value, key, normalize_source_leaf)?;
    }
    for key in [
        "objects",
        "stats",
        "entries",
        "version_pins",
        "source_pins",
        "copied_pins",
        "holders",
    ] {
        rows(&mut value, key, normalize_source_leaf)?;
    }
    if value
        .get("max_evicted_counter_by_object_and_coordinator")
        .is_some()
        || value.get("evicted_opaque_intent_ids_by_object").is_some()
        || value
            .get("opaque_intent_history_evicted_object_ids")
            .is_some()
    {
        value = normalize_intent_retention_wire(value)?;
    }
    if let Some(map) = value.get_mut("max_evicted_request_order_token_by_requester") {
        *map = source_key_map(map.take(), source_node_id)?;
    }
    rows(
        &mut value,
        "opaque_request_history_evicted_requester_node_ids",
        source_node_id,
    )?;
    Ok(value)
}
fn source_key_map(value: Value, key: fn(Value) -> Result<Value>) -> Result<Value> {
    let input = value
        .as_object()
        .ok_or_else(|| invalid("source typed-key map is not an object"))?;
    let mut output = serde_json::Map::new();
    for (old, value) in input {
        let current = key(Value::String(old.clone()))?
            .as_str()
            .ok_or_else(|| invalid("normalized source key is not text"))?
            .to_owned();
        if let Some(previous) = output.insert(current, value.clone()) {
            if previous != *value {
                return Err(invalid("source typed-key aliases have different values"));
            }
        }
    }
    Ok(Value::Object(output))
}
pub(crate) fn normalize_intent_retention_wire(mut value: Value) -> Result<Value> {
    let map = value
        .as_object_mut()
        .ok_or_else(|| invalid("source intent retention is not an object"))?;
    let mut opaque = std::collections::BTreeSet::new();
    if let Some(existing) = map.remove("opaque_intent_history_evicted_object_ids") {
        for object in existing
            .as_array()
            .ok_or_else(|| invalid("source opaque-object roots are not a list"))?
        {
            opaque.insert(
                source_object_id(object.clone())?
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
        }
    }
    if let Some(old) = map.remove("evicted_opaque_intent_ids_by_object") {
        for (object, ids) in old
            .as_object()
            .ok_or_else(|| invalid("source opaque intent map is not an object"))?
        {
            let ids = ids
                .as_array()
                .ok_or_else(|| invalid("source opaque intent identities are not a list"))?;
            if ids.iter().any(|id| !id.is_string()) {
                return Err(invalid("source opaque intent identity is not text"));
            }
            // Exact old identities stay in source bytes. Current admission must
            // conservatively reject all unknown opaque attempts on this object.
            if !ids.is_empty() {
                opaque.insert(
                    source_object_id(Value::String(object.clone()))?
                        .as_str()
                        .unwrap()
                        .to_owned(),
                );
            }
        }
    }
    if !opaque.is_empty() {
        map.insert(
            "opaque_intent_history_evicted_object_ids".into(),
            encoded(opaque)?,
        );
    }
    if let Some(floors) = map.get_mut("max_evicted_counter_by_object_and_coordinator") {
        let mut floors_value = source_key_map(floors.take(), source_object_id)?;
        for nodes in floors_value.as_object_mut().unwrap().values_mut() {
            *nodes = source_key_map(nodes.take(), source_node_id)?;
        }
        *floors = floors_value;
    }
    Ok(value)
}
pub(crate) fn source_content_identity(
    value: &Value,
) -> Result<
    Option<(
        DfsFileSystemId,
        DfsObjectId,
        DfsVersion,
        crate::MetadataSourceContentLocator,
    )>,
> {
    let (Some(fs), Some(object), Some(version)) = (
        value.get("file_system_id"),
        value.get("object_id"),
        value.get("version"),
    ) else {
        return Ok(None);
    };
    let locator = crate::MetadataSourceContentLocator {
        object_id: object
            .as_str()
            .ok_or_else(|| invalid("source content object locator is not text"))?
            .to_owned(),
        version: version
            .as_str()
            .ok_or_else(|| invalid("source content version locator is not text"))?
            .to_owned(),
        content_version: value
            .get("content_durability")
            .filter(|plan| !plan.is_null())
            .map(|plan| {
                plan.get("source_version")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        invalid("actual source content-durability generation is not text")
                    })
            })
            .transpose()?,
    };
    Ok(Some((
        decode(source_fs_id(fs.clone())?)?,
        decode(source_object_id(object.clone())?)?,
        decode(source_version_id(version.clone())?)?,
        locator,
    )))
}
/// Original length-framed caller provenance, interpreted only during import.
/// The entire original operation key remains in source digest/archive evidence.
pub(crate) fn source_journal_operation_id(operation_id: &str) -> Option<String> {
    let framed = operation_id.strip_prefix("namespace-journal-provenance-v1:")?;
    let (length, framed) = framed.split_once(':')?;
    let length = length.parse::<usize>().ok()?;
    if length == 0 || framed.len() <= length || !framed.is_char_boundary(length) {
        return None;
    }
    let (caller, suffix) = framed.split_at(length);
    let digest = suffix.strip_prefix(':')?;
    if digest.len() != 24 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(caller.to_owned())
}
// Retained source DTOs are decoder-only and remain outside current DFS.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryRetention {
    #[serde(default)]
    pub keep_last: u32,
    #[serde(default)]
    pub keep_for_seconds: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryRule {
    pub path: String,
    pub retention: DfsFileHistoryRetention,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPolicy {
    #[serde(default)]
    pub rules: Vec<DfsFileHistoryRule>,
}
impl DfsFileHistoryPolicy {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.rules.len() > 256 {
            return Err("retained history policy exceeds its source bound".into());
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
                return Err("invalid retained history rule path".into());
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsFileVersionSource {
    Checkpoint {
        operation_id: String,
    },
    RetentionRule,
    LocalOperation {
        operation_id: String,
        base_version: Option<DfsVersion>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileVersionPolicyCapture {
    pub order: u64,
    pub captured_at_unix_nanos: u64,
    pub retention: DfsFileHistoryRetention,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRetainedFileVersion {
    pub object: Box<DfsObjectMetadata>,
    pub captured_at_unix_nanos: u64,
    pub source: DfsFileVersionSource,
    #[serde(default)]
    pub policy_capture: Option<DfsFileVersionPolicyCapture>,
}
impl DfsRetainedFileVersion {
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
            return Err("retained source capture does not match immutable content identity".into());
        }
        DfsFileHistoryPolicy {
            rules: vec![DfsFileHistoryRule {
                path: object.path.clone(),
                retention: Default::default(),
            }],
        }
        .validate()?;
        let operation = match &self.source {
            DfsFileVersionSource::Checkpoint { operation_id }
            | DfsFileVersionSource::LocalOperation { operation_id, .. } => Some(operation_id),
            _ => None,
        };
        if operation.is_some_and(|id| id.trim().is_empty() || id.len() > 256 || id.contains('\0')) {
            return Err("retained source capture has invalid operation identity".into());
        }
        if self.policy_capture.as_ref().is_some_and(|capture| {
            (capture.retention.keep_last == 0 && capture.retention.keep_for_seconds == 0)
                || capture.order == 0
                || capture.order > version.retention_order(DFS_HISTORY_METADATA_NAMESPACE)
        }) || matches!(self.source, DfsFileVersionSource::RetentionRule)
            && self.policy_capture.is_none()
        {
            return Err("retained rule capture has no enabled ordered rule".into());
        }
        Ok(())
    }
}

pub const DFS_HISTORY_METADATA_NAMESPACE: &str = "module.dfs.history";
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespacePrepareStatus {
    Prepared,
    Committed,
    Aborted,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespacePrepareRecord {
    pub operation_id: String,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    #[serde(default)]
    pub owner_epoch: u64,
    pub operation_digest: String,
    #[serde(default)]
    pub expected_object_versions: Vec<DfsNamespaceObjectVersionExpectation>,
    #[serde(default)]
    pub expected_path_occupants: Vec<DfsNamespacePathOccupantExpectation>,
    #[serde(default)]
    pub content_receipts: Vec<DfsReplicaReceiptRecord>,
    pub transaction: DfsNamespaceTransaction,
    #[serde(default)]
    pub completed_request: Option<DfsNamespaceRequestRecord>,
    #[serde(default)]
    pub completed_request_completion: Option<DfsNamespaceRequestCompletion>,
    pub status: DfsNamespacePrepareStatus,
}
pub fn normalize_namespace_prepare_in_place(record: &mut DfsNamespacePrepareRecord) {
    for object in &mut record.transaction.objects {
        object.current_locations.sort();
        object.current_locations.dedup();
        if object.kind != DfsObjectKind::Tombstone {
            object.tombstone_provenance = None;
        }
    }
    for version in &mut record.transaction.versions {
        version.locations.sort();
        version.locations.dedup();
    }
    for state in &mut record.transaction.advisory_lock_states {
        state.authority_rebind_required = true;
    }
    for expected in &mut record.expected_path_occupants {
        expected.path = expected.path.trim_matches('/').to_owned();
    }
    record.expected_object_versions.sort();
    record.expected_path_occupants.sort();
    record.content_receipts.sort_by(|left, right| {
        (
            &left.receipt.object_id,
            &left.receipt.committed_version,
            &left.replica_node_id,
        )
            .cmp(&(
                &right.receipt.object_id,
                &right.receipt.committed_version,
                &right.replica_node_id,
            ))
    });
}
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

/// Convert only known version fields; opaque module payloads are never walked.
pub fn convert_version_wire(mut wire: serde_json::Value) -> Result<DfsObjectVersionRecord, String> {
    source_identity_fields(&mut wire).map_err(|error| error.to_string())?;
    field(&mut wire, "module_metadata", source_module_metadata_map)
        .map_err(|error| error.to_string())?;
    field(&mut wire, "content_durability", |mut value| {
        source_identity_fields(&mut value)?;
        Ok(value)
    })
    .map_err(|error| error.to_string())?;
    let object = wire
        .as_object_mut()
        .ok_or("legacy version is not an object")?;
    let order = object
        .remove("history_order")
        .map(serde_json::from_value::<u64>)
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or(0);
    let mut retained = object.remove("retained").filter(|v| !v.is_null());
    if let Some(retained) = retained.as_mut() {
        normalize_retained_capture_wire(retained).map_err(|error| error.to_string())?;
    }
    let retained = retained
        .map(serde_json::from_value::<DfsRetainedFileVersion>)
        .transpose()
        .map_err(|e| e.to_string())?
        .map(Box::new);
    object.remove("metadata_format");
    object
        .entry("retention")
        .or_insert_with(|| serde_json::json!({}));
    object
        .entry("module_metadata")
        .or_insert_with(|| serde_json::json!({}));
    normalize_retention_grants(object.get_mut("retention").expect("inserted retention"))
        .map_err(|error| error.to_string())?;
    let mut current: DfsObjectVersionRecord =
        serde_json::from_value(wire).map_err(|e| e.to_string())?;
    migrate_legacy_version_metadata(&mut current, order, retained)?;
    current.validate_extension_metadata()?;
    Ok(current)
}
/// Historical minimums become the same generic rule grants as current rows.
#[cfg(test)]
pub fn convert_file_system_wire(wire: serde_json::Value) -> Result<serde_json::Value, String> {
    convert_file_system_wire_with_current_import_defaults(wire, None)
}
/// A missing old count remains unchanged in the retained source archive.
/// A supplied CURRENT count selects new import-policy defaults; it is never
/// evidence of historical W/N or historical replica confirmation.
pub(crate) fn convert_file_system_wire_with_current_import_defaults(
    mut wire: Value,
    current_import_candidate_count: Option<usize>,
) -> Result<Value, String> {
    source_identity_fields(&mut wire).map_err(|error| error.to_string())?;
    let object = wire
        .as_object_mut()
        .ok_or("legacy filesystem is not an object")?;
    let history = object.remove("file_history");
    object.remove("metadata_format");
    if let Some(rules) = object.get_mut("subtree_policies") {
        for rule in rules
            .as_array_mut()
            .ok_or("retained subtree policies are not a list")?
        {
            let fields = rule
                .as_object_mut()
                .ok_or("retained subtree policy is not an object")?;
            let policy: sinomo_core::DfsObjectPolicy = serde_json::from_value(
                fields
                    .get("policy")
                    .cloned()
                    .ok_or("retained subtree policy lacks consistency")?,
            )
            .map_err(|error| error.to_string())?;
            if !fields.contains_key("scope") {
                fields.insert("scope".into(), serde_json::json!("subtree"));
            }
            let n = match fields
                .get("desired_replicas")
                .filter(|value| !value.is_null())
            {
                Some(value) => serde_json::from_value::<usize>(value.clone())
                    .map_err(|error| error.to_string())?,
                None if matches!(
                    policy,
                    sinomo_core::DfsObjectPolicy::LocalOwned
                        | sinomo_core::DfsObjectPolicy::EphemeralLocal
                ) =>
                {
                    1
                }
                None => current_import_candidate_count
                    .ok_or(
                        "new current import policy defaults need actual current candidate-count evidence",
                    )?
                    .clamp(1, 2),
            };
            let w = match fields
                .get("min_commit_replicas")
                .filter(|value| !value.is_null())
            {
                Some(value) => serde_json::from_value::<usize>(value.clone())
                    .map_err(|error| error.to_string())?,
                None if policy == sinomo_core::DfsObjectPolicy::ReplicatedConfirmed => n,
                None => 1,
            };
            DfsReplicationPolicy::new(w, n).map_err(|error| error.to_string())?;
            fields.insert("desired_replicas".into(), serde_json::json!(n));
            fields.insert("min_commit_replicas".into(), serde_json::json!(w));
        }
    }
    object
        .entry("retention_policies")
        .or_insert_with(|| serde_json::json!({}));
    object
        .entry("module_metadata")
        .or_insert_with(|| serde_json::json!({}));
    if let Some(history) = history {
        let history: DfsFileHistoryPolicy =
            serde_json::from_value(history).map_err(|e| e.to_string())?;
        history.validate()?;
        if !history.is_empty() {
            let policy = DfsRetentionPolicy {
                rules: history
                    .rules
                    .into_iter()
                    .map(|r| DfsRetentionRule {
                        path: r.path,
                        minimum: DfsRetentionMinimum {
                            keep_last: r.retention.keep_last,
                            keep_for_seconds: r.retention.keep_for_seconds,
                        },
                    })
                    .collect(),
            };
            let policies = object
                .get_mut("retention_policies")
                .and_then(|v| v.as_object_mut())
                .ok_or("invalid retention policy map")?;
            let encoded = serde_json::to_value(policy).map_err(|e| e.to_string())?;
            if policies
                .get(DFS_HISTORY_METADATA_NAMESPACE)
                .is_some_and(|old| old != &encoded)
            {
                return Err("historical policy conflicts with generic policy".into());
            }
            policies.insert(DFS_HISTORY_METADATA_NAMESPACE.into(), encoded);
        }
    }
    field(&mut wire, "module_metadata", source_module_metadata_map)
        .map_err(|error| error.to_string())?;
    Ok(wire)
}

/// Collapse the retired recursive commit-entry DTO without interpreting another
/// preparation or losing the source's exact journal identity.
pub(crate) fn normalize_prepared_commit_reference(
    mut value: Value,
    expected: &DfsPreparedOperationDigest,
) -> Result<DfsPreparedOperationCommitReference> {
    value = normalize_source_leaf(value)?;
    let fields = value
        .as_object_mut()
        .ok_or_else(|| invalid("retained commit reference is not an object"))?;
    if let Some(mutation) = fields.remove("mutation") {
        let mutation = normalize_source_leaf(mutation)?;
        if mutation.get("type").and_then(Value::as_str) != Some("commit-prepared-operation-v7")
            || mutation.get("file_system_id") != Some(&encoded(expected.file_system_id.clone())?)
            || mutation.get("operation_id").and_then(Value::as_str)
                != Some(expected.operation_id.as_str())
            || mutation.get("digest") != Some(&encoded(expected.clone())?)
            || mutation.as_object().is_none_or(|fields| fields.len() != 4)
        {
            return Err(invalid(
                "retained terminal entry is not the exact original commit",
            ));
        }
        if fields
            .insert("digest".into(), encoded(expected.clone())?)
            .is_some()
        {
            return Err(invalid(
                "retained commit contains two digest representations",
            ));
        }
    }
    let reference: DfsPreparedOperationCommitReference = decode(value)?;
    reference.validate(expected)?;
    Ok(reference)
}
pub(crate) fn normalize_prepared_eviction(
    mut value: Value,
) -> Result<DfsPreparedOperationEviction> {
    let digest: DfsPreparedOperationDigest = decode(normalize_source_leaf(
        value
            .get("digest")
            .cloned()
            .ok_or_else(|| invalid("retained eviction lacks its digest"))?,
    )?)?;
    if let Some(entry) = value
        .get("committed_entry")
        .filter(|entry| !entry.is_null())
        .cloned()
    {
        value["committed_entry"] = encoded(normalize_prepared_commit_reference(entry, &digest)?)?;
    }
    let eviction: DfsPreparedOperationEviction = decode(value)?;
    if (eviction.status == DfsPreparedOperationStatus::Committed)
        != eviction.committed_entry.is_some()
    {
        return Err(invalid(
            "retained eviction terminal status contradicts its commit identity",
        ));
    }
    Ok(eviction)
}

// Only schema-owned fields are visited. Module payloads remain byte-for-byte
// JSON values and unknown operation/result variants fail current decoding.
fn field(value: &mut Value, name: &str, convert: fn(Value) -> Result<Value>) -> Result<()> {
    let map = value
        .as_object_mut()
        .ok_or_else(|| invalid("retained row is not an object"))?;
    if let Some(value) = map.get_mut(name).filter(|value| !value.is_null()) {
        *value = convert(value.take())?;
    }
    Ok(())
}
fn rows(value: &mut Value, name: &str, convert: fn(Value) -> Result<Value>) -> Result<()> {
    let map = value
        .as_object_mut()
        .ok_or_else(|| invalid("retained row is not an object"))?;
    if let Some(value) = map.get_mut(name) {
        let array = value
            .as_array_mut()
            .ok_or_else(|| invalid(format!("{name} is not a list")))?;
        for row in array {
            *row = convert(row.take())?;
        }
    }
    Ok(())
}
fn object_wire(mut value: Value) -> Result<Value> {
    let original_owner = value
        .get("owner_node_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    source_identity_fields(&mut value)?;
    let map = value
        .as_object_mut()
        .ok_or_else(|| invalid("retained object is not an object"))?;
    map.remove("metadata_format");
    map.remove("wire_format");
    // Original object serializers omitted these exact zero/empty defaults.
    // Callers validate original rows/operation digests before this adaptation.
    for field in [
        "mode",
        "uid",
        "gid",
        "accessed_at_unix_nanos",
        "modified_at_unix_nanos",
        "created_at_unix_nanos",
        "changed_at_unix_nanos",
    ] {
        map.entry(field).or_insert(Value::from(0));
    }
    map.entry("current_locations")
        .or_insert_with(|| Value::Array(Vec::new()));
    let mut object = decode::<DfsObjectMetadata>(value)?;
    let fixed = matches!(
        object.path.as_str(),
        "/" | "/sinomo" | "/sinomo/nodes" | "/users" | "/services"
    ) || original_owner.as_ref().is_some_and(|owner| {
        object.path == format!("/sinomo/nodes/{owner}")
            || object.path == format!("/sinomo/nodes/{owner}/files")
    });
    let initial = DfsVersion::from_legacy_named("version_1").map_err(invalid)?;
    if fixed
        && object.file_system_id == sinomo_dfs::sifs_file_system_id()
        && object.kind == DfsObjectKind::Directory
        && object.mode == 0
        && object.version == initial
        && object.accessed_at_unix_nanos == 0
        && object.modified_at_unix_nanos == 0
        && object.created_at_unix_nanos == 0
        && object.changed_at_unix_nanos == 0
    {
        // This EXACT source-only bootstrap form predates persisted POSIX modes.
        // A real chmod changes version/clock and must remain mode000.
        object.mode = 0o755;
    }
    encoded(object)
}
fn version_wire(value: Value) -> Result<Value> {
    encoded(normalize_version(value)?)
}
fn intent_wire(value: Value) -> Result<Value> {
    encoded(normalize_intent(value)?)
}
fn intent_receipt_wire(value: Value) -> Result<Value> {
    encoded(normalize_intent_receipt(value)?)
}
fn transaction_wire(value: Value) -> Result<Value> {
    encoded(normalize_namespace_transaction(value)?)
}
fn request_wire(value: Value) -> Result<Value> {
    encoded(normalize_namespace_request(value)?)
}
fn job_wire(value: Value) -> Result<Value> {
    encoded(normalize_namespace_job(value)?)
}
fn version_pin_wire(mut value: Value) -> Result<Value> {
    source_identity_fields(&mut value)?;
    let map = value
        .as_object_mut()
        .ok_or_else(|| invalid("source version pin is not an object"))?;
    if map.get("namespace").is_none_or(Value::is_null) {
        let original = map
            .get("pin_id")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("source pin omitted reference identity"))?;
        let current =
            sinomo_dfs::content_reference_storage_id(DFS_HISTORY_METADATA_NAMESPACE, original);
        map.insert("pin_id".into(), Value::String(current));
        map.insert(
            "namespace".into(),
            Value::String(DFS_HISTORY_METADATA_NAMESPACE.into()),
        );
    }
    encoded(decode::<DfsMetadataVersionPinRecord>(value)?)
}
fn pin_mutation_wire(mut value: Value) -> Result<Value> {
    rows(&mut value, "add", version_pin_wire)?;
    rows(&mut value, "remove", version_pin_wire)?;
    Ok(value)
}
pub(crate) fn normalize_version_pin(value: Value) -> Result<DfsMetadataVersionPinRecord> {
    decode(version_pin_wire(value)?)
}
pub(crate) fn normalize_pin_transfer(mut value: Value) -> Result<DfsCrossShardPinTransferRecord> {
    source_identity_fields(&mut value)?;
    rows(&mut value, "source_pins", version_pin_wire)?;
    rows(&mut value, "copied_pins", version_pin_wire)?;
    decode(value)
}
fn normalize_retained_capture_wire(value: &mut Value) -> Result<()> {
    field(value, "object", object_wire)?;
    field(value, "source", normalize_source_leaf)
}
fn normalize_retention_grants(value: &mut Value) -> Result<()> {
    let states = value
        .as_object_mut()
        .ok_or_else(|| invalid("retention is not a map"))?;
    for state in states.values_mut() {
        if let Some(grant) = state.get_mut("grant").filter(|grant| !grant.is_null()) {
            field(grant, "object", object_wire)?;
        }
    }
    Ok(())
}
/// Validate a leaf after adaptation from its original recorded SOBJ type.
fn source_module_metadata_wire(value: Value) -> Result<Value> {
    encoded(decode::<DfsModuleMetadataValue>(value)?)
}
fn source_module_metadata_map(mut value: Value) -> Result<Value> {
    let entries = value
        .as_object_mut()
        .ok_or_else(|| invalid("source module metadata is not a namespace map"))?;
    for metadata in entries.values_mut() {
        *metadata = source_module_metadata_wire(metadata.take())?;
    }
    Ok(value)
}

/// Adapt declared module payload leaves using their original SOBJ type.
/// The caller verifies original source digests before using this converted copy.
/// No opaque payload contents are inspected for identifiers or schema fields.
pub(crate) fn normalize_original_module_payloads(
    value: &mut Value,
    original: &crate::metadata::source_document::SourceDocumentContext<'_>,
    declared_payload_paths: &[String],
) -> Result<()> {
    normalize_original_module_payloads_at(value, original, declared_payload_paths, "")
}

pub(crate) fn normalize_original_module_payloads_at(
    value: &mut Value,
    original: &crate::metadata::source_document::SourceDocumentContext<'_>,
    declared_payload_paths: &[String],
    source_prefix: &str,
) -> Result<()> {
    let paths = declared_payload_paths
        .iter()
        .map(|path| (path.clone(), format!("{source_prefix}{path}")))
        .collect::<Vec<_>>();
    normalize_original_module_payload_mappings(value, original, &paths)
}

pub(crate) fn normalize_original_module_payload_mappings(
    value: &mut Value,
    original: &crate::metadata::source_document::SourceDocumentContext<'_>,
    paths: &[(String, String)],
) -> Result<()> {
    for (path, source_path) in paths {
        let payload = value.pointer_mut(path).ok_or_else(|| {
            invalid(format!(
                "declared source module payload is absent at {path}"
            ))
        })?;
        normalize_original_module_leaf(payload, original, source_path)?;
    }
    Ok(())
}
fn normalize_original_module_leaf(
    payload: &mut Value,
    original: &crate::metadata::source_document::SourceDocumentContext<'_>,
    source_path: &str,
) -> Result<()> {
    use crate::metadata::source_document::SourcePayloadWire;
    let bytes = match original.raw_payload_wire(source_path)? {
        SourcePayloadWire::CanonicalBytes(bytes) => {
            if bytes.len() > 64 * 1024 {
                return Err(invalid("source module metadata payload exceeds 64 KiB"));
            }
            let logical: Value = serde_json::from_slice(&bytes).map_err(invalid)?;
            serde_json::to_vec(&logical).map_err(invalid)?
        }
        SourcePayloadWire::Json => serde_json::to_vec(payload).map_err(invalid)?,
    };
    if bytes.len() > 64 * 1024 {
        return Err(invalid("source module metadata payload exceeds 64 KiB"));
    }
    *payload = serde_json::to_value(bytes).map_err(invalid)?;
    Ok(())
}

#[cfg(test)]
fn original_compatibility_fixture(mut value: Value, schema: &str) -> Value {
    use sinomo_core::object::{LengthMode, serde_ext};
    let bytes = serde_ext::to_bytes(&value, 1, LengthMode::U32).unwrap();
    let raw = crate::metadata::source_document::SourceRawDocument::row(bytes);
    let paths = original_module_payload_paths(&value, schema, "").unwrap();
    normalize_original_module_payloads(&mut value, &raw.context().unwrap(), &paths).unwrap();
    value
}

#[cfg(test)]
mod original_payload_kind_tests {
    use super::*;
    use sinomo_core::object::{LengthMode, serde_ext};

    #[test]
    fn omitted_original_posix_fields_materialize_without_changing_source_identity() {
        let root = sinomo_testdir::TestDir::new("original-object-omitted-posix");
        let original = serde_json::json!({
            "object_id":"source-object", "file_system_id":sifs_file_system_id(),
            "path":"/services/source-object", "kind":"file", "owner_node_id":"source-owner",
            "content_hash":null, "content_size_bytes":null, "link_target_object_id":null,
            "version":"version_2", "policy":"replicated-eventual",
            "created_by_event_id":null, "last_modified_by_event_id":null
        });
        let bytes = serde_ext::to_bytes(&original, 1, LengthMode::U32).unwrap();
        let source_hash = blake3::hash(&bytes);
        let path = root.path().join("object.sobj");
        std::fs::write(&path, &bytes).unwrap();
        let captured = crate::metadata::source_document::decode_original_document(&bytes).unwrap();
        assert_eq!(blake3::hash(&std::fs::read(&path).unwrap()), source_hash);
        let current = normalize_object(captured.clone()).unwrap();
        assert_eq!((current.mode, current.uid, current.gid), (0, 0, 0));
        assert_eq!(
            [
                current.accessed_at_unix_nanos,
                current.modified_at_unix_nanos,
                current.created_at_unix_nanos,
                current.changed_at_unix_nanos
            ],
            [0; 4]
        );
        assert!(current.current_locations.is_empty());
        assert_eq!(
            current.object_id,
            DfsObjectId::from_legacy_named("source-object").unwrap()
        );
        let mut explicit = captured;
        explicit["mode"] = Value::from(0o600);
        explicit["uid"] = Value::from(23);
        explicit["changed_at_unix_nanos"] = Value::from(99);
        let current = normalize_object(explicit.clone()).unwrap();
        assert_eq!(
            (current.mode, current.uid, current.changed_at_unix_nanos),
            (0o600, 23, 99)
        );
        explicit["mode"] = Value::Null;
        assert!(normalize_object(explicit).is_err());
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn original_fixed_bootstrap_modes_convert_without_changing_user_chmod_or_source() {
        let root = sinomo_testdir::TestDir::new("original-bootstrap-directory-mode");
        let original = serde_json::json!({
            "object_id":"original-root-directory", "file_system_id":sifs_file_system_id(),
            "path":"/sinomo", "kind":"directory", "owner_node_id":"original-owner",
            "current_locations":[], "content_hash":null, "content_size_bytes":null,
            "link_target_object_id":null, "mode":0, "uid":0, "gid":0,
            "accessed_at_unix_nanos":0, "modified_at_unix_nanos":0,
            "created_at_unix_nanos":0, "changed_at_unix_nanos":0,
            "version":"version_1", "policy":"replicated-eventual",
            "created_by_event_id":null, "last_modified_by_event_id":null
        });
        let bytes = serde_ext::to_bytes(&original, 1, LengthMode::U32).unwrap();
        let path = root.path().join("original-directory.sobj");
        std::fs::write(&path, &bytes).unwrap();
        let captured = crate::metadata::source_document::decode_original_document(
            &std::fs::read(&path).unwrap(),
        )
        .unwrap();
        assert_eq!(normalize_object(captured.clone()).unwrap().mode, 0o755);
        let mut chmod = captured.clone();
        chmod["version"] = serde_json::json!("version_2");
        chmod["changed_at_unix_nanos"] = serde_json::json!(1);
        assert_eq!(normalize_object(chmod).unwrap().mode, 0);
        let mut user_directory = captured.clone();
        user_directory["path"] = serde_json::json!("/user-created-zero-mode");
        assert_eq!(normalize_object(user_directory).unwrap().mode, 0);
        let mut owned_bootstrap = captured;
        owned_bootstrap["path"] = serde_json::json!("/sinomo/nodes/original-owner/files");
        assert_eq!(normalize_object(owned_bootstrap).unwrap().mode, 0o755);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn declared_version_schema_stops_at_opaque_payload_and_escapes_its_namespace() {
        let opaque = serde_json::json!({"schema_version":7,"payload":[110,117,108,108],"module_metadata":{"fake":{"schema_version":1,"payload":false}}});
        let original = serde_json::json!({"retention":{},"module_metadata":{"vendor/path~key":{"schema_version":1,"payload":opaque}}});
        let paths = original_module_payload_paths(&original, "version", "").unwrap();
        assert_eq!(paths, vec!["/module_metadata/vendor~1path~0key/payload"]);
        let bytes = serde_ext::to_bytes(&original, 1, LengthMode::U32).unwrap();
        let raw = crate::metadata::source_document::SourceRawDocument::row(bytes);
        let mut converted = original.clone();
        normalize_original_module_payloads(&mut converted, &raw.context().unwrap(), &paths)
            .unwrap();
        let current: DfsModuleMetadataValue =
            serde_json::from_value(converted["module_metadata"]["vendor/path~key"].clone())
                .unwrap();
        assert_eq!(current.payload, opaque);
        assert_eq!(
            serde_ext::from_bytes::<Value>(&raw.bytes).unwrap(),
            original
        );
    }

    #[test]
    fn original_json_values_keep_their_meaning_even_when_arrays_spell_json_bytes() {
        for payload in [
            serde_json::json!([110, 117, 108, 108]),
            serde_json::json!([123, 125]),
            serde_json::json!([]),
            serde_json::json!("module-owned text"),
            serde_json::json!(false),
            serde_json::json!(42),
            Value::Null,
            serde_json::json!({"schema_version": 9, "payload": [110,117,108,108]}),
        ] {
            let original = serde_json::json!({"metadata":{"schema_version":1,"payload":payload}});
            let bytes = serde_ext::to_bytes(&original, 1, LengthMode::U32).unwrap();
            let raw = crate::metadata::source_document::SourceRawDocument::row(bytes);
            let mut converted = original.clone();
            normalize_original_module_payloads(
                &mut converted,
                &raw.context().unwrap(),
                &["/metadata/payload".into()],
            )
            .unwrap();
            let current: DfsModuleMetadataValue =
                serde_json::from_value(converted["metadata"].clone()).unwrap();
            assert_eq!(current.payload, payload);
            assert_eq!(
                serde_ext::from_bytes::<Value>(&raw.bytes).unwrap(),
                original
            );
        }
    }

    #[test]
    fn actual_binary_payload_uses_its_original_byte_tag_and_preserves_opaque_json() {
        #[derive(Serialize)]
        struct Original {
            metadata: DfsModuleMetadataValue,
        }
        let payload =
            serde_json::json!({"object_id":"opaque-module-id","payload":[110,117,108,108]});
        let original = Original {
            metadata: DfsModuleMetadataValue {
                schema_version: 1,
                payload: payload.clone(),
            },
        };
        let bytes = serde_ext::to_bytes(&original, 1, LengthMode::U32).unwrap();
        let mut converted =
            crate::metadata::source_document::decode_original_document(&bytes).unwrap();
        let raw = crate::metadata::source_document::SourceRawDocument::row(bytes);
        normalize_original_module_payloads(
            &mut converted,
            &raw.context().unwrap(),
            &["/metadata/payload".into()],
        )
        .unwrap();
        let current: DfsModuleMetadataValue =
            serde_json::from_value(converted["metadata"].clone()).unwrap();
        assert_eq!(current.payload, payload);
    }
}
fn publication_wire(mut value: Value) -> Result<Value> {
    let map = value
        .as_object_mut()
        .ok_or_else(|| invalid("retained publication is not an object"))?;
    map.remove("metadata_format");
    map.remove("wire_format");
    if map.contains_key("namespace") {
        if map.contains_key("pin_id") || map.contains_key("source") {
            return Err(invalid(
                "retained publication mixes source and namespaced fields",
            ));
        }
    } else {
        if map.contains_key("reference_id") || map.contains_key("metadata") {
            return Err(invalid("retained publication omitted its namespace"));
        }
        let source: DfsFileVersionSource = decode(
            map.remove("source")
                .ok_or_else(|| invalid("retained publication omitted its source"))?,
        )?;
        let operation = match &source {
            DfsFileVersionSource::Checkpoint { operation_id }
            | DfsFileVersionSource::LocalOperation { operation_id, .. } => operation_id,
            DfsFileVersionSource::RetentionRule => {
                return Err(invalid("rule source is not an explicit publication"));
            }
        };
        if operation.trim().is_empty() || operation.len() > 256 || operation.contains('\0') {
            return Err(invalid(
                "retained publication has invalid operation identity",
            ));
        }
        let reference = map
            .remove("pin_id")
            .ok_or_else(|| invalid("retained publication omitted its pin"))?;
        map.insert(
            "namespace".into(),
            Value::String(DFS_HISTORY_METADATA_NAMESPACE.into()),
        );
        map.insert("reference_id".into(), reference);
        map.insert(
            "metadata".into(),
            encoded(DfsModuleMetadataValue {
                schema_version: 1,
                payload: encoded(source)?,
            })?,
        );
    }
    field(&mut value, "metadata", source_module_metadata_wire)?;
    encoded(decode::<DfsRetainedContentPublication>(value)?)
}
fn write_publication_wire(mut value: Value) -> Result<Value> {
    if value.get("kind").and_then(Value::as_str) == Some("retained") {
        field(&mut value, "capture", publication_wire)?;
    }
    encoded(decode::<DfsWritePublication>(value)?)
}
fn write_options_wire(mut value: Value) -> Result<Value> {
    value
        .as_object_mut()
        .ok_or_else(|| invalid("write options are not an object"))?
        .remove("wire_format");
    field(&mut value, "publication", write_publication_wire)?;
    Ok(value)
}
fn content_reference_wire(mut value: Value) -> Result<Value> {
    let map = value
        .as_object_mut()
        .ok_or_else(|| invalid("source content reference is not an object"))?;
    if map.get("namespace").is_some_and(Value::is_null) {
        return Err(invalid("source content reference has a null namespace"));
    }
    if !map.contains_key("namespace") {
        let operation_id = map
            .get("operation_id")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("source content reference omitted operation identity"))?
            .to_owned();
        let operation = map
            .get_mut("operation")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("source content reference omitted operation"))?;
        if matches!(
            operation.get("operation").and_then(Value::as_str),
            Some("capture" | "capture-current")
        ) {
            if operation
                .get("metadata")
                .is_some_and(|value| !value.is_null())
            {
                return Err(invalid(
                    "source implicit-history capture has unencoded metadata",
                ));
            }
            operation.insert(
                "metadata".into(),
                encoded(DfsModuleMetadataValue {
                    schema_version: 1,
                    payload: encoded(DfsFileVersionSource::Checkpoint { operation_id })?,
                })?,
            );
        }
        map.insert(
            "namespace".into(),
            Value::String(DFS_HISTORY_METADATA_NAMESPACE.into()),
        );
    }
    field(&mut value, "identity", normalize_source_leaf)?;
    let operation = value
        .get_mut("operation")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid("source reference operation is not an object"))?;
    let old = operation
        .get("operation")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("source reference operation omits its tag"))?;
    let current = match old {
        "capture" => "capture-current",
        "pin" => "retain",
        "unpin" => "release",
        other => other,
    }
    .to_owned();
    operation.insert("operation".into(), Value::String(current));
    if let Some(pin) = operation.remove("pin_id") {
        if operation.insert("reference_id".into(), pin).is_some() {
            return Err(invalid("source reference has two reference identifiers"));
        }
    }
    field(&mut value, "operation", |mut operation| {
        field(&mut operation, "metadata", source_module_metadata_wire)?;
        normalize_source_leaf(operation)
    })?;
    let request: DfsContentReferenceRequest = decode(value)?;
    request.validate().map_err(invalid)?;
    encoded(request)
}
fn expectation_wire(mut value: Value) -> Result<Value> {
    source_identity_fields(&mut value)?;
    field(&mut value, "expected", version_wire)?;
    Ok(value)
}

pub(crate) fn source_file_system_id(value: Value) -> Result<DfsFileSystemId> {
    decode(source_fs_id(value)?)
}
pub(crate) fn normalize_file_system_with_current_import_defaults(
    value: Value,
    count: Option<usize>,
) -> Result<DfsFileSystem> {
    decode(convert_file_system_wire_with_current_import_defaults(value, count).map_err(invalid)?)
}
pub(crate) fn normalize_object(value: Value) -> Result<DfsObjectMetadata> {
    decode(object_wire(value)?)
}
pub(crate) fn normalize_version(value: Value) -> Result<DfsObjectVersionRecord> {
    convert_version_wire(value).map_err(invalid)
}
pub(crate) fn normalize_intent(mut value: Value) -> Result<DfsWriteIntentRecord> {
    materialize_original_intent_fields(&mut value)?;
    if value.get("status").and_then(Value::as_str) == Some("conflict") {
        value["status"] = Value::String("rejected".into());
    }
    source_identity_fields(&mut value)?;
    field(&mut value, "publication", write_publication_wire)?;
    field(&mut value, "object_metadata", object_wire)?;
    decode(value)
}
pub(crate) fn normalize_intent_receipt(mut value: Value) -> Result<DfsWriteIntentReceipt> {
    materialize_original_intent_fields(&mut value)?;
    if value.get("status").and_then(Value::as_str) == Some("conflict") {
        value["status"] = Value::String("rejected".into());
    }
    source_identity_fields(&mut value)?;
    field(&mut value, "publication", write_publication_wire)?;
    decode(value)
}

fn materialize_original_intent_fields(value: &mut Value) -> Result<()> {
    let fields = value
        .as_object_mut()
        .ok_or_else(|| invalid("original intent is not a record"))?;
    for key in ["publication", "owner_epoch", "touch_parent_on_create"] {
        if !fields.contains_key(key) {
            let default = original_default("intent", key)
                .ok_or_else(|| invalid("original intent field has no recorded semantic default"))?;
            fields.insert(key.into(), default);
        }
    }
    Ok(())
}
pub(crate) fn normalize_namespace_transaction(mut value: Value) -> Result<DfsNamespaceTransaction> {
    source_identity_fields(&mut value)?;
    rows(&mut value, "advisory_lock_states", normalize_source_leaf)?;
    field(
        &mut value,
        "content_reference_identity",
        normalize_source_leaf,
    )?;
    rows(&mut value, "objects", object_wire)?;
    rows(&mut value, "versions", version_wire)?;
    rows(&mut value, "extension_expectations", expectation_wire)?;
    field(&mut value, "file_version_pins", pin_mutation_wire)?;
    let transaction: DfsNamespaceTransaction = decode(value)?;
    validate_transaction_scope(&transaction).map_err(invalid)?;
    Ok(transaction)
}
fn operation_wire(mut value: Value) -> Result<Value> {
    // DfsNamespaceOperation is externally tagged in the retained and current
    // schemas; its typed fields belong to the sole variant body.
    let variants = value
        .as_object_mut()
        .ok_or_else(|| invalid("source namespace operation is not externally tagged"))?;
    if variants.len() != 1 {
        return Err(invalid("source namespace operation has ambiguous variants"));
    }
    let variant = variants.keys().next().expect("one variant").clone();
    let body = variants.get_mut(&variant).expect("captured variant");
    source_identity_fields(body)?;
    if variant == "lookup-path" {
        body.as_object_mut()
            .ok_or_else(|| invalid("source lookup body is not an object"))?
            .entry("inline_range")
            .or_insert(Value::Null);
    }
    // These fields are typed by the namespace-operation enum. This does not
    // recurse through arbitrary metadata, request options, or result payloads.
    for key in ["object", "alias", "committed_target"] {
        field(body, key, object_wire)?;
    }
    field(body, "transaction", transaction_wire)?;
    field(body, "options", write_options_wire)?;
    field(body, "publication", publication_wire)?;
    rows(body, "versions", version_wire)?;
    rows(body, "intents", intent_wire)?;
    rows(body, "intent_receipts", intent_receipt_wire)?;
    rows(body, "version_pins", version_pin_wire)?;
    if matches!(
        variant.as_str(),
        "file-history-pin-v1" | "content-reference-v1" | "content-reference"
    ) {
        field(body, "request", content_reference_wire)?;
    }
    if matches!(variant.as_str(), "file-history-query-v1" | "metadata-query") {
        field(body, "query", metadata_query_wire)?;
    }
    field(body, "fence", job_wire)?;
    rows(body, "receipts", normalize_source_leaf)?;
    field(
        body,
        "intent_receipt_retention",
        normalize_intent_retention_wire,
    )?;
    let canonical = match variant.as_str() {
        "file-history-pin-v1" | "content-reference-v1" => "content-reference",
        "file-history-query-v1" => "metadata-query",
        "file-history-restore-begin-v1" => "begin-content-copy",
        "reconcile-placement-v1" => "reconcile-placement",
        _ => variant.as_str(),
    };
    if canonical != variant {
        let body = variants.remove(&variant).expect("converted variant");
        variants.insert(canonical.into(), body);
    }
    encoded(decode::<DfsNamespaceOperation>(value)?)
}
fn metadata_query_wire(mut value: Value) -> Result<Value> {
    source_identity_fields(&mut value)?;
    let selection = value
        .get_mut("selection")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid("retained metadata query lacks a typed selection"))?;
    let mut selection_value = Value::Object(std::mem::take(selection));
    source_identity_fields(&mut selection_value)?;
    *selection = selection_value.as_object().unwrap().clone();
    let kind = selection
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("retained metadata query lacks its selection kind"))?
        .to_owned();
    match kind.as_str() {
        "list" => {
            selection.insert("kind".into(), Value::String("retained-revisions".into()));
        }
        "restore-job" => {
            selection.insert("kind".into(), Value::String("namespace-job".into()));
        }
        _ => {}
    }
    if matches!(kind.as_str(), "list" | "retained-revisions" | "version") {
        selection
            .entry("namespace")
            .or_insert_with(|| Value::String(DFS_HISTORY_METADATA_NAMESPACE.into()));
    }
    if kind == "version" {
        selection
            .entry("allow_current")
            .or_insert(Value::Bool(false));
    }
    let mut current = Value::Object(std::mem::take(selection));
    field(&mut current, "identity", normalize_source_leaf)?;
    field(&mut current, "after", normalize_source_leaf)?;
    *selection = current.as_object().unwrap().clone();
    Ok(value)
}
fn descriptor_wire(mut value: Value) -> Result<Value> {
    source_identity_fields(&mut value)?;
    field(&mut value, "version", version_wire)?;
    field(&mut value, "current_object", object_wire)?;
    rows(&mut value, "references", version_pin_wire)?;
    Ok(value)
}
fn metadata_response_wire(mut value: Value) -> Result<Value> {
    source_identity_fields(&mut value)?;
    let result = value
        .get_mut("result")
        .ok_or_else(|| invalid("retained metadata response lacks its result"))?;
    let kind = result
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("retained metadata response lacks its result kind"))?
        .to_owned();
    if let Some(fields) = result.as_object_mut() {
        match kind.as_str() {
            "list" => {
                fields.insert("kind".into(), Value::String("retained-revisions".into()));
            }
            "restore-job" => {
                fields.insert("kind".into(), Value::String("namespace-job".into()));
            }
            _ => {}
        }
    }
    field(result, "object", object_wire)?;
    field(result, "descriptor", descriptor_wire)?;
    rows(result, "entries", descriptor_wire)?;
    field(result, "job", job_wire)?;
    field(result, "receipt", |value| {
        encoded(normalize_namespace_job_receipt(value)?)
    })?;
    Ok(value)
}
fn coordination_wire(mut value: Value) -> Result<Value> {
    let variants = value
        .as_object_mut()
        .ok_or_else(|| invalid("retained coordination result is not externally tagged"))?;
    if variants.len() != 1 {
        return Err(invalid(
            "retained coordination result has ambiguous variants",
        ));
    }
    if let Some(response) = variants.remove("file-history") {
        variants.insert("metadata-query".into(), metadata_response_wire(response)?);
    }
    if let Some(job) = variants.get_mut("namespace-job") {
        *job = job_wire(std::mem::take(job))?;
    }
    Ok(value)
}
pub(crate) fn normalize_request_completion(
    mut value: Value,
) -> Result<DfsNamespaceRequestCompletion> {
    source_identity_fields(&mut value)?;
    field(&mut value, "result", normalize_source_leaf)?;
    field(&mut value, "posix_coordination_result", coordination_wire)?;
    decode(value)
}
pub(crate) fn normalize_namespace_request(mut value: Value) -> Result<DfsNamespaceRequestRecord> {
    source_identity_fields(&mut value)?;
    field(&mut value, "operation", operation_wire)?;
    field(&mut value, "result", normalize_source_leaf)?;
    field(&mut value, "posix_coordination_result", coordination_wire)?;
    decode(value)
}
pub(crate) fn normalize_namespace_request_receipt(
    mut value: Value,
) -> Result<DfsNamespaceRequestReceipt> {
    source_identity_fields(&mut value)?;
    field(&mut value, "result", normalize_source_leaf)?;
    field(&mut value, "posix_coordination_result", coordination_wire)?;
    decode(value)
}
fn rename_source_field(value: &mut Value, old: &str, current: &str) -> Result<()> {
    let fields = value
        .as_object_mut()
        .ok_or_else(|| invalid("retained schema row is not an object"))?;
    if let Some(old_value) = fields.remove(old) {
        if fields.insert(current.into(), old_value).is_some() {
            return Err(invalid(format!(
                "retained row has both {old} and {current}"
            )));
        }
    }
    Ok(())
}
fn content_copy_state_wire(mut value: Value) -> Result<Value> {
    // Retained source jobs predate the checkpointed ordered cleanup stage.
    // Conversion records only its absence; runtime authors the future release
    // through the normal owner and never rewrites the archived source bytes.
    value
        .as_object_mut()
        .ok_or_else(|| invalid("retained content-copy state is not an object"))?
        .entry("source_release")
        .or_insert(Value::Null);
    rename_source_field(&mut value, "source_pin", "source_reference")?;
    rename_source_field(&mut value, "source_pin_applied", "source_reference_applied")?;
    source_identity_fields(&mut value)?;
    field(&mut value, "request", normalize_source_leaf)?;
    field(&mut value, "planned", normalize_source_leaf)?;
    field(&mut value, "result", normalize_source_leaf)?;
    field(&mut value, "options", write_options_wire)?;
    field(&mut value, "source_reference", content_reference_wire)?;
    Ok(value)
}
fn job_frame_wire(mut value: Value) -> Result<Value> {
    if value.get("type").and_then(Value::as_str) == Some("file-history-restore") {
        value["type"] = Value::String("content-copy".into());
    }
    source_identity_fields(&mut value)?;
    for key in ["moved_object", "alias", "committed_target"] {
        field(&mut value, key, object_wire)?;
    }
    field(&mut value, "transaction", transaction_wire)?;
    field(&mut value, "fence", |value| {
        if value.get("pending_frames").is_some() {
            encoded(DfsHardlinkTargetFence::from_job(normalize_namespace_job(
                value,
            )?)?)
        } else {
            let mut value = value;
            field(&mut value, "committed_target", object_wire)?;
            let fence: DfsHardlinkTargetFence = decode(value)?;
            DfsHardlinkTargetFence::from_job(fence.into_job()?).and_then(encoded)
        }
    })?;
    field(&mut value, "state", content_copy_state_wire)?;
    encoded(decode::<DfsNamespaceJobFrame>(value)?)
}
pub(crate) fn normalize_namespace_job(mut value: Value) -> Result<DfsNamespaceJobRecord> {
    if value
        .get("pending_frames")
        .and_then(Value::as_array)
        .is_some_and(|frames| {
            frames.iter().any(|frame| {
                frame.get("type").and_then(Value::as_str)
                    == Some("cross-shard-hardlink-alias-commit")
            })
        })
    {
        validate_original_source_job(&value)?;
        return Err(MetadataBackendError::MissingOriginalSourceCustody {
            family: "namespace job".into(),
            identity: value
                .get("job_id")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("original alias job has no identity"))?
                .into(),
            requirement: "original alias object-version and path-occupant comparisons".into(),
        });
    }
    if value.get("kind").and_then(Value::as_str) == Some("file-history-restore") {
        value["kind"] = Value::String("content-copy".into());
    }
    source_identity_fields(&mut value)?;
    field(&mut value, "origin", normalize_source_leaf)?;
    rows(&mut value, "pending_frames", job_frame_wire)?;
    decode(value)
}
pub(crate) fn normalize_namespace_job_receipt(mut value: Value) -> Result<DfsNamespaceJobReceipt> {
    if value.get("kind").and_then(Value::as_str) == Some("file-history-restore") {
        value["kind"] = Value::String("content-copy".into());
    }
    source_identity_fields(&mut value)?;
    rename_source_field(&mut value, "file_restore", "content_copy")?;
    field(&mut value, "origin", normalize_source_leaf)?;
    field(&mut value, "content_copy", normalize_source_leaf)?;
    decode(value)
}
pub(crate) fn normalize_namespace_prepare(mut value: Value) -> Result<Value> {
    source_identity_fields(&mut value)?;
    rows(
        &mut value,
        "expected_object_versions",
        normalize_source_leaf,
    )?;
    rows(&mut value, "expected_path_occupants", normalize_source_leaf)?;
    rows(&mut value, "content_receipts", normalize_source_leaf)?;
    field(&mut value, "transaction", transaction_wire)?;
    field(&mut value, "completed_request", request_wire)?;
    field(&mut value, "completed_request_completion", |value| {
        encoded(normalize_request_completion(value)?)
    })?;
    let mut prepare: DfsNamespacePrepareRecord = decode(value)?;
    crate::legacy_prepare_codec::validate_namespace_prepare_file_system_scope(
        &prepare,
        &prepare.file_system_id,
    )
    .map_err(invalid)?;
    normalize_namespace_prepare_in_place(&mut prepare);
    encoded(prepare)
}

pub(crate) fn validate_content_reference_outcome_identity(
    identity: &DfsRevisionReference,
    file_system_id: &DfsFileSystemId,
    transaction_id: &str,
    digest: Option<&str>,
) -> Result<(), String> {
    let operation = transaction_id
        .strip_prefix("file-history:")
        .ok_or("reference outcome annotation has no source operation identity")?;
    if identity.file_system_id != *file_system_id
        || operation.trim().is_empty()
        || operation.len() > 256
        || operation.contains('\0')
        || digest.is_none_or(|hash| {
            hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err("invalid reference outcome identity or contribution digest".into());
    }
    Ok(())
}
pub(crate) fn validate_pin_scope(
    pins: &DfsFileVersionPinMutation,
    fs: &DfsFileSystemId,
) -> Result<(), String> {
    if pins.add.len().saturating_add(pins.remove.len()) > 4096 {
        return Err("source pin batch exceeds 4096 entries".into());
    }
    let mut identities = std::collections::BTreeSet::new();
    for pin in pins.add.iter().chain(&pins.remove) {
        if &pin.file_system_id != fs
            || pin.pin_id.trim().is_empty()
            || pin.pin_id.len() > 1024
            || pin.pin_id.contains('\0')
            || !identities.insert((&pin.object_id, &pin.version, &pin.pin_id))
        {
            return Err("invalid, duplicate, or cross-filesystem source pin".into());
        }
    }
    Ok(())
}
fn validate_transaction_scope(tx: &DfsNamespaceTransaction) -> Result<(), String> {
    if tx
        .objects
        .iter()
        .any(|row| row.file_system_id != tx.file_system_id)
        || tx
            .versions
            .iter()
            .any(|row| row.file_system_id != tx.file_system_id)
        || tx
            .advisory_lock_states
            .iter()
            .any(|row| row.file_system_id != tx.file_system_id)
    {
        return Err("source transaction has cross-filesystem rows".into());
    }
    if let Some(pins) = &tx.file_version_pins {
        validate_pin_scope(pins, &tx.file_system_id)?;
    }
    for version in &tx.versions {
        version.validate_extension_metadata()?;
        if let Some(plan) = &version.content_durability {
            sinomo_dfs::validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
                .map_err(|e| e.to_string())?;
            if version.content_hash.is_none() || version.size_bytes.is_none() {
                return Err("source content durability lacks immutable content identity".into());
            }
        }
    }
    for witness in &tx.extension_expectations {
        if let Some(version) = &witness.expected {
            if version.file_system_id != tx.file_system_id
                || version.object_id != witness.object_id
                || version.version != witness.version
            {
                return Err("source extension expectation has foreign identity".into());
            }
            version.validate_extension_metadata()?;
        }
    }
    if let Some(identity) = &tx.content_reference_identity {
        validate_content_reference_outcome_identity(
            identity,
            &tx.file_system_id,
            &tx.transaction_id,
            tx.contribution_digest.as_deref(),
        )?;
        if !tx.objects.is_empty()
            || !tx.advisory_lock_states.is_empty()
            || tx
                .versions
                .iter()
                .any(|row| row.object_id != identity.object_id || row.version != identity.version)
            || tx.file_version_pins.as_ref().is_some_and(|pins| {
                pins.add.iter().chain(&pins.remove).any(|pin| {
                    pin.object_id != identity.object_id || pin.version != identity.version
                })
            })
        {
            return Err("source reference outcome has foreign mutation effects".into());
        }
    }
    Ok(())
}

impl DfsNamespacePrepareRecord {
    // Decoder-local validation of the retired layout-fence shape. Conversion
    // archives this reservation; it cannot mint current handoff authority.
    pub(crate) fn validate_default_layout_migration_fence(&self) -> Result<(), String> {
        const PREFIX: &str = "default-layout-fence-v1:";
        if !self.operation_id.starts_with(PREFIX) {
            return Ok(());
        }
        let tx = &self.transaction;
        if self.operation_id.len() <= PREFIX.len()
            || self.operation_id.len() > 256
            || self.operation_id.contains('\0')
            || !tx.transaction_id.starts_with("default-layout-fence-tx-v1:")
            || tx.transaction_id.len() <= "default-layout-fence-tx-v1:".len()
            || self.status == DfsNamespacePrepareStatus::Committed
            || tx.file_system_id != self.file_system_id
            || !((tx.objects.is_empty() && tx.versions.len() == 1)
                || (tx.objects.len() == 1 && tx.versions.is_empty()))
            || tx.file_version_pins.is_some()
            || !tx.advisory_lock_states.is_empty()
            || !tx.extension_expectations.is_empty()
            || tx.contribution_digest.is_some()
            || tx.content_reference_identity.is_some()
            || !self.content_receipts.is_empty()
            || self.completed_request.is_some()
            || self.completed_request_completion.is_some()
            || self.expected_object_versions.len() != 1
            || self.expected_path_occupants.len() != 1
        {
            return Err("invalid retained layout-fence shape or committed status".into());
        }
        let (object, version, fs) = if let Some(version) = tx.versions.first() {
            (
                &version.object_id,
                &version.version,
                &version.file_system_id,
            )
        } else {
            let object = &tx.objects[0];
            (&object.object_id, &object.version, &object.file_system_id)
        };
        let head = &self.expected_object_versions[0];
        let path = &self.expected_path_occupants[0];
        let relative = path.path.trim_matches('/');
        if fs != &self.file_system_id
            || &head.object_id != object
            || head.expected_version.as_ref() != Some(version)
            || tx
                .objects
                .first()
                .is_some_and(|object| object.path.trim_matches('/') != relative)
            || path.path.contains('\0')
            || relative.split('/').any(|part| {
                part == "." || part == ".." || (!relative.is_empty() && part.is_empty())
            })
        {
            return Err("retained layout fence omitted its exact source head or path".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceGeneration {
    Released,
    CanonicalTags,
}
struct GenerationRouted<'a> {
    value: &'a Value,
    schema: &'a str,
    generation: SourceGeneration,
}
/// Exact generation-8 job serializer, used only for ORIGINAL image inspection.
pub(crate) struct Native8Job<'a>(pub(crate) &'a Value);
impl Serialize for Native8Job<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        GenerationRouted {
            value: self.0,
            schema: "job",
            generation: SourceGeneration::CanonicalTags,
        }
        .serialize(serializer)
    }
}
/// The original native record envelope keeps its original payload digest. The
/// later converter alone adapts the payload and authors a new target digest.
pub(crate) struct Native8Prepare<'a>(pub(crate) &'a Value);
impl Serialize for Native8Prepare<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeMap};
        let row = self.0;
        let digest: DfsPreparedOperationDigest =
            serde_json::from_value(row["digest"].clone()).map_err(S::Error::custom)?;
        let status: sinomo_dfs::DfsPreparedOperationStatus =
            serde_json::from_value(row["status"].clone()).map_err(S::Error::custom)?;
        let committed: Option<sinomo_dfs::DfsPreparedOperationCommitReference> =
            serde_json::from_value(row.get("committed_entry").cloned().unwrap_or(Value::Null))
                .map_err(S::Error::custom)?;
        let payload = row
            .get("payload")
            .ok_or_else(|| S::Error::custom("native8 preparation omits body"))?;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("digest", &digest)?;
        map.serialize_entry(
            "payload",
            &OriginalV7Payload {
                generation: SourceGeneration::CanonicalTags,
                payload,
                had_sequence: true,
                had_effects: true,
                had_provenance: true,
            },
        )?;
        map.serialize_entry("status", &status)?;
        if let Some(committed) = committed {
            map.serialize_entry("committed_entry", &committed)?;
        }
        map.end()
    }
}
struct OriginalV7Payload<'a> {
    generation: SourceGeneration,
    payload: &'a Value,
    had_sequence: bool,
    had_effects: bool,
    had_provenance: bool,
}
impl Serialize for OriginalV7Payload<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::Error;
        use serde::ser::SerializeMap;
        let p = self.payload;
        const FIELDS: &[&str] = &[
            "protocol_version",
            "file_system_id",
            "shard_id",
            "owner_epoch",
            "operation_id",
            "journal_operation_id",
            "expected_journal_sequence",
            "expected_object_versions",
            "expected_path_occupants",
            "transaction",
            "metadata_effects",
            "content_receipts",
            "required_replica_node_ids",
            "prepared_intent",
            "terminal_intent",
            "completed_request",
            "completed_request_completion",
        ];
        if p.as_object()
            .is_none_or(|object| object.keys().any(|key| !FIELDS.contains(&key.as_str())))
        {
            return Err(S::Error::custom(
                "original v7 payload has an unsupported source field",
            ));
        }
        let required = |key: &str| {
            p.get(key)
                .ok_or_else(|| S::Error::custom(format!("original v7 payload omits {key}")))
        };
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("protocol_version", required("protocol_version")?)?;
        map.serialize_entry("file_system_id", required("file_system_id")?)?;
        map.serialize_entry("shard_id", required("shard_id")?)?;
        map.serialize_entry("owner_epoch", required("owner_epoch")?)?;
        map.serialize_entry("operation_id", required("operation_id")?)?;
        if self.had_provenance {
            map.serialize_entry("journal_operation_id", required("journal_operation_id")?)?;
        }
        if self.had_sequence {
            map.serialize_entry(
                "expected_journal_sequence",
                required("expected_journal_sequence")?,
            )?;
        }
        map.serialize_entry(
            "expected_object_versions",
            &GenerationRouted {
                generation: self.generation,
                value: required("expected_object_versions")?,
                schema: "list/object-expectation",
            },
        )?;
        map.serialize_entry(
            "expected_path_occupants",
            &GenerationRouted {
                generation: self.generation,
                value: required("expected_path_occupants")?,
                schema: "list/path-expectation",
            },
        )?;
        map.serialize_entry(
            "transaction",
            &GenerationRouted {
                generation: self.generation,
                value: required("transaction")?,
                schema: "transaction",
            },
        )?;
        if self.had_effects {
            map.serialize_entry(
                "metadata_effects",
                &GenerationRouted {
                    generation: self.generation,
                    value: required("metadata_effects")?,
                    schema: "list/metadata-effect",
                },
            )?;
        }
        map.serialize_entry(
            "content_receipts",
            &GenerationRouted {
                generation: self.generation,
                value: required("content_receipts")?,
                schema: "list/replica-receipt",
            },
        )?;
        map.serialize_entry(
            "required_replica_node_ids",
            required("required_replica_node_ids")?,
        )?;
        map.serialize_entry(
            "prepared_intent",
            &GenerationRouted {
                generation: self.generation,
                value: required("prepared_intent")?,
                schema: "intent",
            },
        )?;
        map.serialize_entry(
            "terminal_intent",
            &GenerationRouted {
                generation: self.generation,
                value: required("terminal_intent")?,
                schema: "intent",
            },
        )?;
        map.serialize_entry(
            "completed_request",
            &GenerationRouted {
                generation: self.generation,
                value: required("completed_request")?,
                schema: "request",
            },
        )?;
        map.serialize_entry(
            "completed_request_completion",
            &GenerationRouted {
                generation: self.generation,
                value: required("completed_request_completion")?,
                schema: "completion",
            },
        )?;
        map.end()
    }
}
/// Check the original routed input before a converter changes IDs or tags.
/// Source custody is not encoded into this semantic command identity.
#[cfg(test)]
pub(crate) fn original_namespace_request_digest(raw: &Value) -> Result<String> {
    source_namespace_request_digest(raw, SourceGeneration::Released)
}
fn source_namespace_request_digest(raw: &Value, generation: SourceGeneration) -> Result<String> {
    let field = |key: &str| {
        raw.get(key)
            .ok_or_else(|| invalid(format!("original routed request omits {key}")))
    };
    let mut hash = blake3::Hasher::new();
    serde_json::to_writer(
        &mut hash,
        &(
            field("file_system_id")?,
            field("requester_node_id")?,
            GenerationRouted {
                value: field("operation")?,
                schema: "operation",
                generation,
            },
        ),
    )
    .map_err(invalid)?;
    Ok(format!("blake3:{}", hash.finalize().to_hex()))
}
pub(crate) fn verify_original_namespace_request(raw: &Value) -> Result<()> {
    let expected = raw
        .get("command_digest")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("original routed command digest is not text"))?;
    for generation in [SourceGeneration::Released, SourceGeneration::CanonicalTags] {
        if source_namespace_request_digest(raw, generation)? == expected {
            return Ok(());
        }
    }
    Err(invalid(
        "original routed request changes its semantic command digest",
    ))
}

/// Native9 kept the original alias tag and only its transaction. The other
/// frames already use the finite typed target fence. These decode-only types
/// reproduce that exact producer preimage; they are never normalized into a
/// current job or used to supply publication authority/comparison predicates.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
enum OriginalNative9AliasFrame {
    CrossShardHardlinkAliasCommit {
        transaction: DfsNamespaceTransaction,
    },
}
#[derive(Deserialize)]
#[serde(untagged)]
enum OriginalNative9FrameInput {
    Alias(OriginalNative9AliasFrame),
    Unchanged(DfsNamespaceJobFrame),
}
struct OriginalNative9Frame(OriginalNative9FrameInput);
impl<'de> Deserialize<'de> for OriginalNative9Frame {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;
        let value = OriginalNative9FrameInput::deserialize(deserializer)?;
        if matches!(
            &value,
            OriginalNative9FrameInput::Unchanged(
                DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { .. }
            )
        ) {
            return Err(D::Error::custom(
                "native9 cannot contain the later read-set frame tag",
            ));
        }
        Ok(Self(value))
    }
}
impl Serialize for OriginalNative9Frame {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match &self.0 {
            OriginalNative9FrameInput::Alias(value) => value.serialize(serializer),
            OriginalNative9FrameInput::Unchanged(value) => value.serialize(serializer),
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalNative9Job {
    job_id: String,
    file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    origin: Option<DfsNamespaceJobOrigin>,
    kind: DfsNamespaceJobKind,
    status: DfsNamespaceJobStatus,
    root_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pending_frames: Vec<OriginalNative9Frame>,
    processed_object_count: u64,
    #[serde(with = "time::serde::rfc3339")]
    created_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: time::OffsetDateTime,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    completed_at: Option<time::OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure_reason: Option<String>,
}
impl OriginalNative9Job {
    fn has_original_alias(&self) -> bool {
        self.pending_frames
            .iter()
            .any(|frame| matches!(&frame.0, OriginalNative9FrameInput::Alias(_)))
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
enum OriginalNative9JobEffect {
    PutNamespaceJob { job: OriginalNative9Job },
}
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum OriginalNative9Effect {
    Job(OriginalNative9JobEffect),
    Unchanged(DfsPreparedMetadataEffect),
}
/// Field order/null behavior is the native9 prepared producer's exact tuple.
/// Roundtrip equality below prevents unknown/omitted fields from being erased.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalNative9Payload {
    protocol_version: u8,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    owner_epoch: u64,
    operation_id: String,
    journal_operation_id: Option<String>,
    expected_journal_sequence: Option<u64>,
    expected_object_versions: Vec<DfsNamespaceObjectVersionExpectation>,
    expected_path_occupants: Vec<DfsNamespacePathOccupantExpectation>,
    transaction: DfsNamespaceTransaction,
    metadata_effects: Vec<OriginalNative9Effect>,
    content_receipts: Vec<DfsReplicaReceiptRecord>,
    required_replica_node_ids: Vec<sinomo_core::node::NodeId>,
    prepared_intent: Option<DfsWriteIntentRecord>,
    terminal_intent: Option<DfsWriteIntentRecord>,
    completed_request: Option<DfsNamespaceRequestRecord>,
    completed_request_completion: Option<DfsNamespaceRequestCompletion>,
}
impl OriginalNative9Payload {
    fn has_original_alias(&self) -> bool {
        self.metadata_effects.iter().any(|effect| {
            matches!(effect,
            OriginalNative9Effect::Job(OriginalNative9JobEffect::PutNamespaceJob { job })
                if job.has_original_alias())
        })
    }
    fn hash(&self) -> Result<String> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"sinomo.dfs.prepared-operation\0v7\0");
        serde_json::to_writer(&mut hash, self).map_err(invalid)?;
        Ok(format!(
            "dfs-prepared-operation-v7:blake3:{}",
            hash.finalize().to_hex()
        ))
    }
}
fn original_native9_alias_payload(raw: &Value) -> Result<Option<OriginalNative9Payload>> {
    let Ok(payload) = serde_json::from_value::<OriginalNative9Payload>(raw.clone()) else {
        return Ok(None);
    };
    if !payload.has_original_alias()
        || payload.metadata_effects.iter().any(|effect| {
            matches!(
                effect,
                OriginalNative9Effect::Unchanged(DfsPreparedMetadataEffect::PutNamespaceJob { .. })
            )
        })
        || serde_json::to_value(&payload).map_err(invalid)? != *raw
    {
        return Ok(None);
    }
    Ok(Some(payload))
}
#[cfg(test)]
pub(crate) fn original_native9_alias_payload_digest(raw: &Value) -> Result<String> {
    original_native9_alias_payload(raw)?
        .ok_or_else(|| invalid("not an exact native9 alias preimage"))?
        .hash()
}

pub(crate) fn validate_original_source_job(raw: &Value) -> Result<()> {
    if let Ok(current) = serde_json::from_value::<DfsNamespaceJobRecord>(raw.clone()) {
        if serde_json::to_value(&current).map_err(invalid)? == *raw {
            return Ok(());
        }
    }
    if let Ok(original) = serde_json::from_value::<OriginalNative9Job>(raw.clone()) {
        if original.has_original_alias()
            && serde_json::to_value(&original).map_err(invalid)? == *raw
        {
            return Ok(());
        }
    }
    serde_json::to_writer(
        std::io::sink(),
        &OriginalRouted {
            value: raw,
            schema: "job",
        },
    )
    .map_err(invalid)
}
pub(crate) fn validate_original_v7_record(row: &Value) -> Result<()> {
    // Both retained V7 record generations use this envelope. A failed current
    // DTO decode must not let the legacy payload proof discard unknown fields.
    const RECORD_FIELDS: &[&str] = &["digest", "payload", "status", "committed_entry"];
    if row.as_object().is_none_or(|fields| {
        fields
            .keys()
            .any(|key| !RECORD_FIELDS.contains(&key.as_str()))
    }) {
        return Err(invalid(
            "retained v7 record has an unsupported source field",
        ));
    }
    // A source can already contain the current unified operation family. Use
    // its exact typed canonical serializer before interpreting released legacy
    // field generations; never normalize a current body to an older shape.
    if let Ok(current) = serde_json::from_value::<DfsPreparedOperationRecord>(row.clone()) {
        if serde_json::to_value(&current).map_err(invalid)? == *row {
            current.validate()?;
            return Ok(());
        }
    }
    let raw = row
        .get("payload")
        .ok_or_else(|| invalid("retained v7 digest lacks original payload"))?;
    require_supported_source_digest_ids(raw)?;
    let had_sequence = raw.get("expected_journal_sequence").is_some();
    let had_effects = raw.get("metadata_effects").is_some();
    let had_provenance = raw.get("journal_operation_id").is_some();
    let digest: DfsPreparedOperationDigest = serde_json::from_value(
        row.get("digest")
            .cloned()
            .ok_or_else(|| invalid("retained v7 record omits original digest"))?,
    )
    .map_err(invalid)?;
    // The exact native9 model keeps its old alias tag and finite fence body.
    // It cannot become current DATA without the separately required original
    // comparison custody. This is hash validation only, never that custody.
    let mut source_hash = None;
    if let Some(original) = original_native9_alias_payload(raw)? {
        let candidate = original.hash()?;
        if candidate == digest.payload_digest {
            let status: DfsPreparedOperationStatus =
                serde_json::from_value(row["status"].clone()).map_err(invalid)?;
            let committed: Option<DfsPreparedOperationCommitReference> =
                serde_json::from_value(row.get("committed_entry").cloned().unwrap_or(Value::Null))
                    .map_err(invalid)?;
            if (status == DfsPreparedOperationStatus::Committed) != committed.is_some()
                || original.expected_journal_sequence.is_some_and(|sequence| {
                    sequence.checked_add(1) != Some(digest.prepare_sequence)
                })
            {
                return Err(invalid(
                    "native9 alias record changes its original slot/terminal identity",
                ));
            }
            if let Some(committed) = committed {
                committed.validate(&digest)?;
            }
            source_hash = Some(candidate);
        }
    }
    // Earlier source generations keep their independently ordered serializers.
    // A current DTO's evolving field requirements cannot decide this proof.
    for generation in if source_hash.is_some() {
        &[][..]
    } else if had_provenance {
        &[SourceGeneration::Released, SourceGeneration::CanonicalTags][..]
    } else {
        &[SourceGeneration::Released][..]
    } {
        let mut hash = blake3::Hasher::new();
        hash.update(b"sinomo.dfs.prepared-operation\0v7\0");
        if serde_json::to_writer(
            &mut hash,
            &OriginalV7Payload {
                payload: raw,
                had_sequence,
                had_effects,
                had_provenance,
                generation: *generation,
            },
        )
        .is_ok()
        {
            let candidate = format!(
                "dfs-prepared-operation-v7:blake3:{}",
                hash.finalize().to_hex()
            );
            if candidate == digest.payload_digest {
                source_hash = Some(candidate);
                break;
            }
        }
    }
    if raw.get("protocol_version").and_then(Value::as_u64) != Some(7)
        || digest.protocol_version != 7
        || digest.prepare_sequence == 0
        || raw.get("file_system_id").and_then(Value::as_str) != Some(digest.file_system_id.as_str())
        || raw.get("shard_id").and_then(Value::as_str) != Some(digest.shard_id.0.as_str())
        || raw.get("owner_epoch").and_then(Value::as_u64) != Some(digest.owner_epoch)
        || raw.get("operation_id").and_then(Value::as_str) != Some(digest.operation_id.as_str())
        || source_hash.as_deref() != Some(digest.payload_digest.as_str())
    {
        return Err(invalid(
            "retained original v7 payload changes its scope or canonical digest",
        ));
    }
    Ok(())
}
fn require_supported_source_digest_ids(value: &Value) -> Result<()> {
    if let Some(fields) = value.as_object() {
        for key in [
            "file_system_id",
            "object_id",
            "version",
            "target_version",
            "base_version",
            "expected_version",
            "expected_object_id",
            "link_target_object_id",
            "owner_node_id",
            "coordinator_node_id",
            "requester_node_id",
            "created_by_event_id",
            "last_modified_by_event_id",
        ] {
            if let Some(raw) = fields.get(key).filter(|value| !value.is_null()) {
                if let Some(raw) = raw.as_str() {
                    sinomo_core::Id::new(raw).map_err(|_|invalid("retained original tuple with named identifiers requires independently authenticated source serialization"))?;
                }
            }
        }
        for key in [
            "current_locations",
            "locations",
            "desired_replica_node_ids",
            "required_replica_node_ids",
        ] {
            if let Some(values) = fields.get(key).and_then(Value::as_array) {
                for value in values {
                    if let Some(raw) = value.as_str() {
                        sinomo_core::Id::new(raw).map_err(|_|invalid("retained original tuple with named nodes requires authenticated source serialization"))?;
                    }
                }
            }
        }
        for key in [
            "transaction",
            "expected",
            "content_durability",
            "retained",
            "object",
            "source",
        ] {
            if let Some(child) = fields.get(key) {
                require_supported_source_digest_ids(child)?;
            }
        }
        for key in [
            "objects",
            "versions",
            "extension_expectations",
            "expected_object_versions",
            "expected_path_occupants",
            "advisory_lock_states",
        ] {
            if let Some(values) = fields.get(key).and_then(Value::as_array) {
                for child in values {
                    require_supported_source_digest_ids(child)?;
                }
            }
        }
        if let Some(pins) = fields
            .get("file_version_pins")
            .filter(|value| !value.is_null())
        {
            for key in ["add", "remove"] {
                if let Some(rows) = pins.get(key).and_then(Value::as_array) {
                    for row in rows {
                        require_supported_source_digest_ids(row)?;
                    }
                }
            }
        }
    }
    Ok(())
}
pub(crate) fn validate_original_namespace_prepare(
    raw: Value,
    forced_legacy: Option<bool>,
) -> Result<Value> {
    verify_raw_namespace_prepare(&raw, forced_legacy)?;
    normalize_namespace_prepare(raw)
}

pub(crate) fn verify_raw_namespace_prepare(raw: &Value, forced_legacy: Option<bool>) -> Result<()> {
    let [nine, eight, seven] = original_namespace_digests(raw, forced_legacy)?;
    let expected = raw.get("operation_digest").and_then(Value::as_str);
    // Released producers added the routed request (dd87a932) and then its
    // compact completion (12dcca1b). An older proof cannot omit a real outcome.
    let no_completion = raw
        .get("completed_request_completion")
        .is_none_or(Value::is_null);
    let no_request = raw.get("completed_request").is_none_or(Value::is_null);
    if expected == Some(nine.as_str())
        || (no_completion && expected == Some(eight.as_str()))
        || (no_completion && no_request && expected == Some(seven.as_str()))
    {
        return Ok(());
    }
    Err(invalid(
        "retained original namespace tuple has a non-canonical source digest",
    ))
}
fn original_namespace_digests(raw: &Value, forced_legacy: Option<bool>) -> Result<[String; 3]> {
    let field = |key: &str| {
        raw.get(key)
            .ok_or_else(|| invalid(format!("original namespace tuple omits {key}")))
    };
    let mut transaction = field("transaction")?.clone();
    if forced_legacy == Some(true) {
        if let Some(rows) = transaction
            .get_mut("versions")
            .and_then(Value::as_array_mut)
        {
            for row in rows {
                if row.get("retention").is_some() || row.get("module_metadata").is_some() {
                    *row = serde_json::to_value(SourceVersionDigest {
                        version: row,
                        legacy: true,
                        original_durability: row.get("content_durability"),
                    })
                    .map_err(invalid)?;
                }
            }
        }
        if let Some(rows) = transaction
            .get_mut("extension_expectations")
            .and_then(Value::as_array_mut)
        {
            for row in rows {
                if let Some(expected) = row.get_mut("expected").filter(|v| !v.is_null()) {
                    *expected = serde_json::to_value(SourceVersionDigest {
                        version: expected,
                        legacy: true,
                        original_durability: expected.get("content_durability"),
                    })
                    .map_err(invalid)?;
                }
            }
        }
    }
    let empty = Value::Array(Vec::new());
    let null = Value::Null;
    // Serialize the shared original body once; append only the known released
    // attachments to the same hash prefix instead of revisiting every object.
    let prefix = serde_json::to_vec(&(
        field("operation_id")?,
        field("file_system_id")?,
        field("shard_id")?,
        field("owner_epoch")?,
        OriginalRouted {
            value: raw.get("expected_object_versions").unwrap_or(&empty),
            schema: "list/object-expectation",
        },
        OriginalRouted {
            value: raw.get("expected_path_occupants").unwrap_or(&empty),
            schema: "list/path-expectation",
        },
        OriginalRouted {
            value: &transaction,
            schema: "transaction",
        },
    ))
    .map_err(invalid)?;
    let mut hash = blake3::Hasher::new();
    hash.update(
        prefix
            .strip_suffix(b"]")
            .ok_or_else(|| invalid("original namespace tuple has no JSON terminator"))?,
    );
    let finish = |prefix: &blake3::Hasher| {
        let mut hash = prefix.clone();
        hash.update(b"]");
        format!("blake3:{}", hash.finalize().to_hex())
    };
    let seven = finish(&hash);
    hash.update(b",");
    serde_json::to_writer(
        &mut hash,
        &OriginalRouted {
            value: raw.get("completed_request").unwrap_or(&null),
            schema: "request",
        },
    )
    .map_err(invalid)?;
    let eight = finish(&hash);
    hash.update(b",");
    serde_json::to_writer(
        &mut hash,
        &OriginalRouted {
            value: raw.get("completed_request_completion").unwrap_or(&null),
            schema: "completion",
        },
    )
    .map_err(invalid)?;
    Ok([finish(&hash), eight, seven])
}
/// Expand and verify ORIGINAL packed source data before any current request,
/// pin or opaque-metadata DTO can reject its historical representation.
pub(crate) fn decode_original_packed_prepare(
    outer: &Value,
) -> Result<crate::legacy_prepare_codec::original::OriginalPackedPrepare> {
    let mut decoded =
        crate::legacy_prepare_codec::original::unpack_original(outer).map_err(invalid)?;
    let forced = outer
        .get("packed_prepare_v1")
        .is_some_and(|value| !value.is_null())
        || outer
            .get("packed_prepare_v2")
            .and_then(|value| value.get("f"))
            .and_then(Value::as_u64)
            != Some(6);
    verify_raw_namespace_prepare(&decoded.value, Some(forced))?;
    let body = if outer
        .get("packed_prepare_v1")
        .is_some_and(|value| !value.is_null())
    {
        "/packed_prepare_v1"
    } else {
        "/packed_prepare_v2"
    };
    for source in decoded.payload_sources.values_mut() {
        *source = format!("{body}{source}");
    }
    for (key, schema, source) in [
        ("completed_request", "request", "q"),
        ("completed_request_completion", "completion", "c"),
    ] {
        if let Some(value) = decoded.value.get(key).filter(|value| !value.is_null()) {
            for path in original_module_payload_paths(value, schema, &format!("/{key}"))? {
                let suffix = path
                    .strip_prefix(&format!("/{key}"))
                    .ok_or_else(|| invalid("packed attachment path differs"))?;
                decoded
                    .payload_sources
                    .insert(path.clone(), format!("{body}/{source}{suffix}"));
            }
        }
    }
    Ok(decoded)
}

pub(crate) fn decode_namespace_prepare_mutation_checked(outer: Value) -> Result<Value> {
    if let Some(raw) = outer.get("prepare").filter(|value| !value.is_null()) {
        if ["packed_prepare_v1", "packed_prepare_v2"]
            .iter()
            .any(|key| outer.get(*key).is_some_and(|value| !value.is_null()))
        {
            return Err(invalid(
                "source prepare has ambiguous canonical/packed bodies",
            ));
        }
        verify_raw_namespace_prepare(raw, None)?;
        return Ok(raw.clone());
    }
    decode_original_packed_prepare(&outer).map(|decoded| decoded.value)
}
#[cfg(test)]
fn decode_namespace_prepare_mutation(outer: Value) -> Result<Value> {
    if let Some(raw) = outer.get("prepare").filter(|value| !value.is_null()) {
        if ["packed_prepare_v1", "packed_prepare_v2"]
            .iter()
            .any(|key| outer.get(*key).is_some_and(|value| !value.is_null()))
            || raw.get("file_system_id") != outer.get("file_system_id")
            || raw.get("operation_id") != outer.get("operation_id")
            || raw.get("status") != outer.get("status")
        {
            return Err(invalid("original mutation body identity differs"));
        }
        return Ok(raw.clone());
    }
    crate::legacy_prepare_codec::original::unpack_original(&outer)
        .map(|decoded| decoded.value)
        .map_err(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn source_capture_keeps_exact_grant_and_opaque_module_payload() {
        let fs = DfsFileSystemId::generate();
        let object_id = DfsObjectId::generate();
        let version = DfsVersion::generate();
        let node = sinomo_core::node::NodeId::generate();
        let hash = "a".repeat(64);
        let object = json!({"file_system_id":fs,"object_id":object_id,"version":version,
            "path":"/services/retained","kind":"file","owner_node_id":node,
            "policy":"replicated-eventual","content_hash":hash,"content_size_bytes":7});
        let opaque =
            json!({"retained":{"history_order":91,"metadata_format":"opaque-vendor-data"}});
        let converted=normalize_version(original_compatibility_fixture(json!({"file_system_id":fs,"object_id":object_id,"version":version,
            "content_hash":hash,"size_bytes":7,"locations":[node],"visible":false,
            "created_at":"2026-10-03T00:00:00Z","history_order":4,
            "retained":{"object":object,"captured_at_unix_nanos":42,"source":{"kind":"retention-rule"},
                "policy_capture":{"order":4,"captured_at_unix_nanos":42,"retention":{"keep_last":2}}},
            "module_metadata":{"vendor.research":{"schema_version":1,"payload":opaque}}
        }), "version")).expect("convert retained source row");
        let state = &converted.retention[DFS_HISTORY_METADATA_NAMESPACE];
        assert_eq!(state.order, 4);
        let grant = state.grant.as_ref().expect("retained source grant");
        assert_eq!(grant.object.content_hash.as_deref(), Some(hash.as_str()));
        assert_eq!(grant.granted_at_unix_nanos, 42);
        assert_eq!(grant.minimum.as_ref().unwrap().minimum.keep_last, 2);
        assert_eq!(converted.module_metadata["vendor.research"].payload, opaque);
        assert_eq!(
            converted.module_metadata[DFS_HISTORY_METADATA_NAMESPACE].payload,
            json!({"kind":"retention-rule"})
        );
        let serialized = encoded(converted).unwrap();
        assert!(serialized.get("history_order").is_none());
        assert!(serialized.get("retained").is_none());
        assert!(serialized.get("metadata_format").is_none());
    }

    #[test]
    fn implicit_source_reference_materializes_its_real_namespace_provenance_and_storage_key() {
        let fs = DfsFileSystemId::generate();
        let object = DfsObjectId::generate();
        let version = DfsVersion::generate();
        let operation=operation_wire(json!({"file-history-pin-v1":{"request":{
            "path":"/services/source","identity":{"file_system_id":fs,"object_id":object,"version":version},
            "operation":{"operation":"capture","pin_id":"source-reference"},"operation_id":"source-capture"
        }}})).unwrap();
        let operation: DfsNamespaceOperation = decode(operation).unwrap();
        let DfsNamespaceOperation::ContentReference { request } = operation else {
            panic!("exact converted variant");
        };
        assert_eq!(request.namespace, DFS_HISTORY_METADATA_NAMESPACE);
        let DfsContentReferenceOperation::CaptureCurrent {
            reference_id,
            metadata,
        } = request.operation
        else {
            panic!("exact source capture");
        };
        assert_eq!(reference_id, "source-reference");
        assert_eq!(
            metadata.unwrap().payload,
            json!({"kind":"checkpoint","operation_id":"source-capture"})
        );
        let pin = normalize_version_pin(
            json!({"file_system_id":fs,"object_id":object,"version":version,
            "pin_id":"source-reference","created_at":"2026-10-03T00:00:00Z"}),
        )
        .unwrap();
        assert_eq!(pin.namespace.as_str(), DFS_HISTORY_METADATA_NAMESPACE);
        assert_eq!(
            pin.pin_id,
            sinomo_dfs::content_reference_storage_id(
                DFS_HISTORY_METADATA_NAMESPACE,
                "source-reference"
            )
        );
        assert_eq!(
            normalize_version_pin(encoded(pin.clone()).unwrap()).unwrap(),
            pin
        );
    }

    fn outer(fs: DfsFileSystemId, body: Value, key: &str) -> Value {
        let mut outer = json!({"type":"put-namespace-prepare","file_system_id":fs,
            "operation_id":"source-operation","status":"prepared"});
        outer[key] = body;
        outer
    }
    fn indexed_body(format: u8) -> Value {
        json!({"f":format,"s":"shard-source","e":7,"g":"source-digest",
            "d":{"l":{"o":[0]}},"x":{},"p":{},"r":{"h":{}},
            "i":"source-transaction","o":{"h":{},"z":{},"a":{},"c":{},"e":{}},
            "v":{"h":{},"z":{},"e":{}},"t":{"s":0,"n":0}})
    }
    #[test]
    fn retained_packed_versions_decode_actual_sobj_and_reject_identity_or_reference_changes() {
        let fs = DfsFileSystemId::generate();
        let original = json!({"f":1,"s":"shard-source","e":7,"d":"source-digest",
            "t":{"i":"source-transaction","o":[],"t":"1970-01-01T00:00:00Z"}});
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &original,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let decoded: Value = sinomo_core::object::serde_ext::from_bytes(&bytes).unwrap();
        let row =
            decode_namespace_prepare_mutation(outer(fs.clone(), decoded, "packed_prepare_v1"))
                .unwrap();
        assert_eq!(row["file_system_id"], encoded(fs.clone()).unwrap());
        assert_eq!(row["operation_id"], "source-operation");
        assert_eq!(row["owner_epoch"], 7);
        assert_eq!(row["operation_digest"], "source-digest");
        for format in 2..=6 {
            let bytes = sinomo_core::object::serde_ext::to_bytes(
                &indexed_body(format),
                1,
                sinomo_core::object::LengthMode::U32,
            )
            .unwrap();
            let decoded: Value = sinomo_core::object::serde_ext::from_bytes(&bytes).unwrap();
            let row =
                decode_namespace_prepare_mutation(outer(fs.clone(), decoded, "packed_prepare_v2"))
                    .unwrap();
            assert_eq!(row["file_system_id"], encoded(fs.clone()).unwrap());
            assert_eq!(row["operation_id"], "source-operation");
            assert_eq!(row["owner_epoch"], 7);
            assert_eq!(row["operation_digest"], "source-digest");
            assert_eq!(row["transaction"]["transaction_id"], "source-transaction");
            let mut ambiguous = outer(fs.clone(), indexed_body(format), "packed_prepare_v2");
            ambiguous["prepare"] = row.clone();
            assert!(decode_namespace_prepare_mutation(ambiguous).is_err());
            let mut mismatch = outer(DfsFileSystemId::generate(), row, "prepare");
            mismatch["status"] = json!("committed");
            assert!(decode_namespace_prepare_mutation(mismatch).is_err());
            let mut invalid = indexed_body(format);
            invalid["x"] = json!({"o":[1]});
            assert!(
                decode_namespace_prepare_mutation(outer(fs.clone(), invalid, "packed_prepare_v2"))
                    .is_err()
            );
        }
        assert!(
            decode_namespace_prepare_mutation(outer(fs, indexed_body(7), "packed_prepare_v2"))
                .is_err()
        );
    }
}
#[test]
fn omitted_source_policy_counts_require_actual_candidates_and_preserve_explicit_counts() {
    let fs = DfsFileSystem::sifs(
        sinomo_core::node::NodeId::from_legacy_named("source-policy-owner").unwrap(),
    );
    let mut wire = encoded(fs).unwrap();
    wire["subtree_policies"] = serde_json::json!([
        {"path_prefix":"/local","policy":"local-owned"},
        {"path_prefix":"/eventual","policy":"replicated-eventual","desired_replicas":null,"min_commit_replicas":null},
        {"path_prefix":"/confirmed","policy":"replicated-confirmed"},
        {"path_prefix":"/explicit","policy":"replicated-confirmed","scope":"self-only","desired_replicas":3,"min_commit_replicas":2}
    ]);
    assert!(convert_file_system_wire(wire.clone()).is_err());
    for candidates in [1, 6] {
        let current: DfsFileSystem = decode(
            convert_file_system_wire_with_current_import_defaults(wire.clone(), Some(candidates))
                .unwrap(),
        )
        .unwrap();
        let n = candidates.clamp(1, 2);
        assert_eq!(
            (
                current.subtree_policies[0].min_commit_replicas,
                current.subtree_policies[0].desired_replicas
            ),
            (1, 1)
        );
        assert_eq!(
            (
                current.subtree_policies[1].min_commit_replicas,
                current.subtree_policies[1].desired_replicas
            ),
            (1, n)
        );
        assert_eq!(
            (
                current.subtree_policies[2].min_commit_replicas,
                current.subtree_policies[2].desired_replicas
            ),
            (n, n)
        );
        assert_eq!(
            (
                current.subtree_policies[3].min_commit_replicas,
                current.subtree_policies[3].desired_replicas
            ),
            (2, 3)
        );
        assert_eq!(
            current.subtree_policies[3].scope,
            DfsPlacementScope::SelfOnly
        );
    }
}

#[cfg(test)]
mod source_identifier_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn named_source_identifiers_normalize_all_typed_references_but_preserve_opaque_values() {
        let raw_object = json!({"file_system_id":"old-fs","object_id":"old-object","version":"old-version",
            "owner_node_id":"old-node","path":"/services/source","kind":"file","policy":"replicated-eventual",
            "current_locations":["old-node"],"created_by_event_id":"old-event"});
        let opaque = json!({"object_id":"vendor-object","version":"vendor-version","owner_node_id":"vendor-node"});
        let version = json!({"file_system_id":"old-fs","object_id":"old-object","version":"old-version",
            "locations":["old-node"],"created_at":"2026-10-03T00:00:00Z","visible":false,
            "module_metadata":{"vendor.example":{"schema_version":1,"payload":opaque}}});
        let tx=normalize_namespace_transaction(original_compatibility_fixture(json!({"transaction_id":"source-tx","file_system_id":"old-fs",
            "objects":[raw_object],"versions":[version],"created_at":"2026-10-03T00:00:00Z",
            "file_version_pins":{"add":[{"file_system_id":"old-fs","object_id":"old-object","version":"old-version",
                "pin_id":"old-pin","created_at":"2026-10-03T00:00:00Z"}],"remove":[]}}), "transaction")).unwrap();
        assert_eq!(
            tx.file_system_id,
            DfsFileSystemId::from_legacy_named("old-fs").unwrap()
        );
        assert_eq!(tx.objects[0].object_id, tx.versions[0].object_id);
        assert_eq!(tx.objects[0].version, tx.versions[0].version);
        assert_eq!(tx.objects[0].owner_node_id, tx.versions[0].locations[0]);
        assert_eq!(
            tx.objects[0].created_by_event_id,
            Some(sinomo_core::events::EventId::from_legacy_named("old-event").unwrap())
        );
        assert_eq!(
            tx.file_version_pins.as_ref().unwrap().add[0].object_id,
            tx.objects[0].object_id
        );
        assert_eq!(
            tx.versions[0].module_metadata["vendor.example"].payload,
            opaque
        );
        let (_, object, rev, locator) = source_content_identity(
            &json!({"file_system_id":"old-fs","object_id":"old-object","version":"old-version"}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(object, tx.objects[0].object_id);
        assert_eq!(rev, tx.objects[0].version);
        assert_eq!(locator.object_id, "old-object");
        assert_eq!(locator.version, "old-version");
    }
    #[test]
    fn retired_exact_opaque_intent_ids_become_conservative_current_object_roots() {
        let converted = normalize_intent_retention_wire(
            json!({"evicted_opaque_intent_ids_by_object":{"old-object":["old-opaque-attempt"]},
            "max_evicted_counter_by_object_and_coordinator":{"old-object":{"old-node":7}}}),
        )
        .unwrap();
        assert!(
            converted
                .get("evicted_opaque_intent_ids_by_object")
                .is_none()
        );
        let state: DfsWriteIntentReceiptRetentionState = decode(converted).unwrap();
        let object = DfsObjectId::from_legacy_named("old-object").unwrap();
        let node = sinomo_core::node::NodeId::from_legacy_named("old-node").unwrap();
        assert!(
            state
                .opaque_intent_history_evicted_object_ids
                .contains(&object)
        );
        assert_eq!(
            state.max_evicted_counter_by_object_and_coordinator[&object][&node],
            7
        );
    }
    #[test]
    fn old_conflicted_intent_is_rejected_and_source_aliases_cannot_override_current_fields() {
        let intent=normalize_intent(json!({"intent_id":"source-conflict","file_system_id":"old-fs","object_id":"old-object",
            "target_version":"old-version","coordinator_node_id":"old-node",
            "policy":DfsReplicationPolicy::new(1,1).unwrap(),"status":"conflict","created_at":"2026-10-03T00:00:00Z"})).unwrap();
        assert_eq!(intent.status, DfsWriteIntentStatus::Rejected);
        let mut state = json!({"source_pin":{"operation_id":"actual-old"}});
        rename_source_field(&mut state, "source_pin", "source_reference").unwrap();
        assert_eq!(state["source_reference"]["operation_id"], "actual-old");
        assert!(
            rename_source_field(
                &mut json!({"source_pin":{},"source_reference":{}}),
                "source_pin",
                "source_reference"
            )
            .is_err()
        );
    }
}

// Decoder-only reproduction of HEAD's original namespace authority serializer.
// This is used only to check retained digests; it cannot author a journal frame.
/// This adapter sees the raw decoded fields of the original source body.
/// It never decodes a current DFS DTO to decide original field order/defaults.
struct SourceVersionDigest<'a> {
    version: &'a Value,
    legacy: bool,
    original_durability: Option<&'a Value>,
}
fn original_legacy_version_from_packed_projection(raw: &Value) -> Result<Value> {
    // Packed <=5 decoding projects its old history capsule into these two
    // namespaced maps. This is a decoder-owned intermediate representation,
    // not evidence that arbitrary source payload arrays encode JSON bytes.
    let fields = raw
        .as_object()
        .ok_or_else(|| invalid("original source version is not an object"))?;
    let mut output = serde_json::Map::new();
    for field in original_fields("legacy-version").expect("declared source version schema") {
        if let Some(value) = fields.get(*field) {
            output.insert((*field).into(), value.clone());
        }
    }
    let retention = fields.get("retention").and_then(Value::as_object);
    let modules = fields.get("module_metadata").and_then(Value::as_object);
    if retention
        .into_iter()
        .flat_map(|map| map.keys())
        .chain(modules.into_iter().flat_map(|map| map.keys()))
        .any(|namespace| namespace != DFS_HISTORY_METADATA_NAMESPACE)
    {
        return Err(invalid(
            "old packed version cannot contain foreign namespaced metadata",
        ));
    }
    if let Some(state) = retention.and_then(|map| map.get(DFS_HISTORY_METADATA_NAMESPACE)) {
        let order = state
            .get("order")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("original projected history order is not u64"))?;
        output.insert("history_order".into(), Value::from(order));
        if let Some(grant) = state.get("grant").filter(|grant| !grant.is_null()) {
            let required = |key: &str| {
                grant
                    .get(key)
                    .cloned()
                    .ok_or_else(|| invalid(format!("original projected grant omits {key}")))
            };
            let source = if let Some(metadata) =
                modules.and_then(|map| map.get(DFS_HISTORY_METADATA_NAMESPACE))
            {
                let payload = metadata
                    .get("payload")
                    .ok_or_else(|| invalid("original projected capture omits payload"))?;
                let decoded = if payload.is_object() {
                    payload.clone()
                } else {
                    // migrate_legacy_version_metadata generated this intermediate
                    // using current module-value byte serialization. Its source
                    // is the old retained capsule, not an arbitrary opaque leaf.
                    let bytes = payload
                        .as_array()
                        .ok_or_else(|| invalid("projected capture has no known source encoding"))?
                        .iter()
                        .map(|byte| {
                            byte.as_u64()
                                .and_then(|byte| u8::try_from(byte).ok())
                                .ok_or_else(|| invalid("projected capture byte is invalid"))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    serde_json::from_slice::<Value>(&bytes).map_err(invalid)?
                };
                decoded.get("source").unwrap_or(&decoded).clone()
            } else if grant.get("origin").and_then(Value::as_str) == Some("rule") {
                serde_json::json!({"kind":"retention-rule"})
            } else {
                return Err(invalid(
                    "original projected capture has no source provenance",
                ));
            };
            let policy = match grant.get("minimum").filter(|value| !value.is_null()) {
                Some(minimum) => serde_json::json!({
                    "order":minimum.get("order").ok_or_else(||invalid("original minimum omits order"))?,
                    "captured_at_unix_nanos":minimum.get("granted_at_unix_nanos").ok_or_else(||invalid("original minimum omits grant time"))?,
                    "retention":minimum.get("minimum").ok_or_else(||invalid("original minimum omits policy"))?,
                }),
                None => Value::Null,
            };
            output.insert("retained".into(),serde_json::json!({"object":required("object")?,"captured_at_unix_nanos":required("granted_at_unix_nanos")?,"source":source,"policy_capture":policy}));
        }
    }
    Ok(Value::Object(output))
}
impl Serialize for SourceVersionDigest<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut version = if self.legacy {
            original_legacy_version_from_packed_projection(self.version)
                .map_err(serde::ser::Error::custom)?
        } else {
            self.version.clone()
        };
        if let Some(durability) = self.original_durability {
            version["content_durability"] = durability.clone();
        }
        OriginalRouted {
            value: &version,
            schema: if self.legacy {
                "legacy-version"
            } else {
                "namespaced-version"
            },
        }
        .serialize(serializer)
    }
}
#[cfg(test)]
mod original_digest_tests {
    use super::*;
    use serde_json::json;
    use sinomo_core::object::{LengthMode, serde_ext};
    #[test]
    fn exact_oracle_july17_namespace_source_uses_its_released_seven_field_digest() {
        let bytes =
            include_bytes!("../tests/fixtures/original-oracle-users-namespace-prepare-seq147.sobj");
        assert_eq!(bytes.len(), 2969);
        let document = crate::metadata::source_document::decode_original_document(bytes).unwrap();
        assert_eq!(document["sequence"], 147);
        let source = &document["mutation"]["prepare"];
        let digests = original_namespace_digests(source, None).unwrap();
        assert_eq!(
            digests,
            [
                "blake3:cd15e8dfcf6e15f63a786a869228032ad607bb80ca26ef41234afb9f9938b79b",
                "blake3:ebe868f8e4d9b9db334aa30167cc703f48ff1064436eea6efefc3750cb666269",
                "blake3:8ef4daec40b641fa919c0a1a067fcc5b78f828ea59ecdaeeb62615e252d24300",
            ]
            .map(str::to_owned)
        );
        assert_eq!(source["operation_digest"], digests[2]);
        verify_raw_namespace_prepare(source, None).unwrap();
        // Missing and codec-expanded null attachments carry no extra outcome.
        for digest in digests {
            let mut reopened = source.clone();
            reopened["completed_request_completion"] = Value::Null;
            reopened["operation_digest"] = json!(digest);
            verify_raw_namespace_prepare(&reopened, None).unwrap();
        }
        let mut changed = source.clone();
        changed["transaction"]["objects"][0]["path"] = json!("/users/changed");
        assert!(verify_raw_namespace_prepare(&changed, None).is_err());
        let mut unknown = source.clone();
        unknown["transaction"]["unsupported_source_field"] = json!(true);
        assert!(verify_raw_namespace_prepare(&unknown, None).is_err());
        let mut unknown_digest = source.clone();
        unknown_digest["operation_digest"] = json!("blake3:unknown");
        assert!(verify_raw_namespace_prepare(&unknown_digest, None).is_err());
    }
    #[test]
    fn original_projected_history_hash_never_decodes_current_version_ids_or_fields() {
        let raw = json!({"file_system_id":"old-source-fs","object_id":"old-source-object","version":"old-source-version","created_at":"1970-01-01T00:00:00Z","visible":false,
            "retention":{"module.dfs.history":{"order":9,"grant":null}},"module_metadata":{}});
        assert!(serde_json::from_value::<DfsObjectVersionRecord>(raw.clone()).is_err());
        assert_eq!(
            serde_json::to_string(&SourceVersionDigest {
                version: &raw,
                legacy: true,
                original_durability: None
            })
            .unwrap(),
            r#"{"file_system_id":"old-source-fs","object_id":"old-source-object","version":"old-source-version","created_at":"1970-01-01T00:00:00Z","visible":false,"history_order":9}"#
        );
    }
    #[test]
    fn causality_slot_source_hash_is_independent_of_current_required_pin_namespace() {
        let payload = json!({"protocol_version":7,"file_system_id":"00000000000000000000000001","shard_id":"root","owner_epoch":7,"operation_id":"source-operation","journal_operation_id":null,"expected_journal_sequence":null,
            "expected_object_versions":[],"expected_path_occupants":[],"transaction":{"transaction_id":"source-transaction","file_system_id":"00000000000000000000000001","objects":[],"created_at":"1970-01-01T00:00:00Z",
                "file_version_pins":{"add":[{"file_system_id":"00000000000000000000000001","object_id":"00000000000000000000000002","version":"00000000000000000000000003","pin_id":"old-reference","created_at":"1970-01-01T00:00:00Z"}],"remove":[]}},
            "metadata_effects":[],"content_receipts":[],"required_replica_node_ids":[],"prepared_intent":null,"terminal_intent":null,"completed_request":null,"completed_request_completion":null});
        assert!(serde_json::from_value::<DfsPreparedOperationPayload>(payload.clone()).is_err());
        let mut hash = blake3::Hasher::new();
        hash.update(b"sinomo.dfs.prepared-operation\0v7\0");
        serde_json::to_writer(
            &mut hash,
            &OriginalV7Payload {
                payload: &payload,
                had_sequence: true,
                had_effects: true,
                had_provenance: true,
                generation: SourceGeneration::Released,
            },
        )
        .unwrap();
        let mut row = json!({"payload":payload,"digest":{"protocol_version":7,"file_system_id":"00000000000000000000000001","shard_id":"root","owner_epoch":7,"operation_id":"source-operation","prepare_sequence":1,"payload_digest":format!("dfs-prepared-operation-v7:blake3:{}",hash.finalize().to_hex())}});
        validate_original_v7_record(&row).unwrap();
        row["payload"]["transaction"]["file_version_pins"]["add"][0]["pin_id"] =
            json!("changed-reference");
        assert!(validate_original_v7_record(&row).is_err());
    }
    #[test]
    fn source_generations_keep_their_actual_reference_tags_and_field_names() {
        let raw = json!({"content-reference":{"request":{"path":"/source","identity":{"file_system_id":"old-fs","object_id":"old-object","version":"old-version"},"operation":{"operation":"retain","reference_id":"old-pin"},"operation_id":"reference-operation"}}});
        let current = serde_json::to_string(&GenerationRouted {
            value: &raw,
            schema: "operation",
            generation: SourceGeneration::CanonicalTags,
        })
        .unwrap();
        assert_eq!(
            current,
            r#"{"content-reference":{"request":{"path":"/source","identity":{"file_system_id":"old-fs","object_id":"old-object","version":"old-version"},"operation":{"operation":"retain","reference_id":"old-pin"},"operation_id":"reference-operation"}}}"#
        );
        let released = serde_json::to_string(&OriginalRouted {
            value: &raw,
            schema: "operation",
        })
        .unwrap();
        assert_eq!(
            released,
            r#"{"file-history-pin-v1":{"request":{"path":"/source","identity":{"file_system_id":"old-fs","object_id":"old-object","version":"old-version"},"operation":{"operation":"pin","pin_id":"old-pin"},"operation_id":"reference-operation"}}}"#
        );
    }
    #[test]
    fn actual_pre_causality_v7_slots_hash_before_missing_slot_conversion() {
        let cases = [
            (
                false,
                false,
                "36aa8ab70ae9b07034a27c7c37fa6e770442b35f12b0023d5824605da45f0b4d",
            ),
            (
                false,
                true,
                "3491e6c0d6d5a10c3e7aca87ee087199e0a7fdcc302477dfecfb6d2447d8bb19",
            ),
            (
                true,
                false,
                "c719a7eba8b814a0be1bab6c872957aa5baa1d29afcfc7e95d9169aeb4c79d91",
            ),
            (
                true,
                true,
                "b2888be959c727ab84f584e26dbf6d0e3017a0da11352678dd0f84079ae5c837",
            ),
        ];
        for (sequence, effects, hash) in cases {
            let mut payload = json!({"protocol_version":7,"file_system_id":"00000000000000000000000001","shard_id":"root","owner_epoch":7,"operation_id":"source-operation",
                "expected_object_versions":[],"expected_path_occupants":[],"transaction":{"transaction_id":"source-transaction","file_system_id":"00000000000000000000000001","objects":[],"created_at":"1970-01-01T00:00:00Z"},
                "content_receipts":[],"required_replica_node_ids":[],"prepared_intent":null,"terminal_intent":null,"completed_request":null,"completed_request_completion":null});
            if sequence {
                payload["expected_journal_sequence"] = Value::Null;
            }
            if effects {
                payload["metadata_effects"] = json!([]);
            }
            let row = json!({"payload":payload,"digest":{"protocol_version":7,"file_system_id":"00000000000000000000000001","shard_id":"root","owner_epoch":7,"operation_id":"source-operation","payload_digest":format!("dfs-prepared-operation-v7:blake3:{hash}"),"prepare_sequence":9},"status":"prepared"});
            let bytes = serde_ext::to_bytes(&row, 1, LengthMode::U32).unwrap();
            let reopened: Value = serde_ext::from_bytes(&bytes).unwrap();
            validate_original_v7_record(&reopened).unwrap();
            let mut unknown = reopened.clone();
            unknown["unrecognized_source_effect"] = json!(true);
            assert!(validate_original_v7_record(&unknown).is_err());
            let mut forged = reopened;
            forged["payload"]["transaction"]["transaction_id"] = json!("changed-transaction");
            assert!(validate_original_v7_record(&forged).is_err());
        }
    }
    #[test]
    fn retained_actual_packed_source_digest_survives_sobj_and_rejects_body_tampering() {
        // This fixture follows the original HEAD ordered nine-field tuple and
        // original transaction serde order, not a Value-map serialization.
        const DIGEST: &str =
            "blake3:dde55703fa352a966defaee7dc79eeca52af01f323ff301bf5ad8a1ec4b5df96";
        for format in 2..=6 {
            let outer = json!({"type":"put-namespace-prepare","file_system_id":"00000000000000000000000001",
                "operation_id":"source-operation","status":"prepared","packed_prepare_v2":{
                "f":format,"s":"root","e":7,"g":DIGEST,"d":{"l":{"o":[0]}},"x":{},"p":{},"r":{"h":{}},
                "i":"source-transaction","o":{"h":{},"z":{},"a":{},"c":{},"e":{}},"v":{"h":{},"z":{},"e":{}},"t":{"s":0,"n":0}}});
            let bytes = serde_ext::to_bytes(&outer, 1, LengthMode::U32).unwrap();
            let reopened: Value = serde_ext::from_bytes(&bytes).unwrap();
            let actual = decode_namespace_prepare_mutation_checked(reopened.clone()).unwrap();
            assert_eq!(actual["operation_digest"], DIGEST);
            let mut forged = reopened;
            forged["packed_prepare_v2"]["i"] = json!("changed-source-transaction");
            assert!(decode_namespace_prepare_mutation_checked(forged).is_err());
        }
    }
}

/// Decode-only ordered JSON view of the original routed serde schema. Keeping
/// recursive job/operation bodies as JSON avoids a second typed protocol graph.
struct OriginalRouted<'a> {
    value: &'a Value,
    schema: &'a str,
}
fn original_fields(schema: &str) -> Option<&'static [&'static str]> {
    Some(match schema {
        "request" => &[
            "file_system_id",
            "requester_node_id",
            "request_id",
            "operation_id",
            "authority_shard_id",
            "operation",
            "command_digest",
            "status",
            "result",
            "posix_coordination_result",
            "recovery_attempts",
            "last_recovery_error",
            "created_at",
            "updated_at",
        ],
        "completion" => &[
            "file_system_id",
            "requester_node_id",
            "request_id",
            "operation_id",
            "authority_shard_id",
            "command_digest",
            "transaction_id",
            "result",
            "posix_coordination_result",
            "created_at",
            "completed_at",
        ],
        "pins" => &["add", "remove"],
        "pin" => &[
            "namespace",
            "file_system_id",
            "object_id",
            "version",
            "pin_id",
            "created_at",
        ],
        "attrs" => &["mode", "uid", "gid"],
        "precondition" => &["object_id", "version"],
        "inline-range" => &["offset", "max_bytes"],
        "revision" => &["file_system_id", "object_id", "version"],
        "metadata-effect/put-file-system" => &["file_system"],
        "metadata-effect/put-object" => &["object"],
        "metadata-effect/put-version" => &["version"],
        "metadata-effect/remove-version" => &["object_id", "version"],
        "metadata-effect/put-version-pin" => &["pin"],
        "metadata-effect/remove-version-pin" => &["object_id", "version", "pin_id"],
        "metadata-effect/reconcile-cross-shard-pin-transfer" => &["versions", "transfer"],
        "metadata-effect/remove-cross-shard-pin-transfer" => &["object_id", "transfer_id"],
        "metadata-effect/finalize-cross-shard-source-eviction" => {
            &["tombstone", "removed_pins", "removed_pin_transfers"]
        }
        "metadata-effect/put-receipt"
        | "metadata-effect/put-intent-receipt"
        | "metadata-effect/put-namespace-transaction-receipt"
        | "metadata-effect/put-namespace-job-receipt"
        | "metadata-effect/put-namespace-request-receipt" => &["receipt"],
        "metadata-effect/remove-receipt" => &["object_id", "version", "replica_node_id"],
        "metadata-effect/put-intent" => &["intent"],
        "metadata-effect/remove-retained-repair-owner" => &["object_id", "intent_id"],
        "metadata-effect/remove-intent-receipt" => &["object_id", "intent_id", "retention"],
        "metadata-effect/merge-intent-receipt-retention" => &["retention"],
        "metadata-effect/put-advisory-lock-state" => &["state"],
        "metadata-effect/merge-content-reference-outcome-retention" => &["object_id", "retention"],
        "metadata-effect/remove-namespace-transaction-receipt" => &["transaction_id", "retention"],
        "metadata-effect/put-namespace-job" => &["job"],
        "metadata-effect/remove-namespace-job-receipt" => &["job_id", "retention"],
        "metadata-effect/put-namespace-request" => &["request"],
        "metadata-effect/remove-namespace-request-receipt" => {
            &["requester_node_id", "request_id", "retention"]
        }
        "metadata-effect/remove-prepared-operation" => &["digest"],
        "metadata-effect/resolve-prepared-operation" => &["digest", "status"],
        "prepared-digest" => &[
            "protocol_version",
            "file_system_id",
            "shard_id",
            "owner_epoch",
            "operation_id",
            "payload_digest",
            "prepare_sequence",
        ],
        "pin-transfer" => &[
            "transfer_id",
            "file_system_id",
            "object_id",
            "source_path",
            "target_path",
            "target_version",
            "source_pins",
            "copied_pins",
            "created_at",
            "updated_at",
        ],
        "namespace-retention" => &[
            "content_reference_outcome_retention",
            "transaction_floor",
            "prepare_floor",
            "job_floor",
            "max_evicted_request_order_token_by_requester",
            "opaque_request_history_evicted_requester_node_ids",
        ],
        "receipt-order" => &["created_at", "identity"],
        "request-receipt" => &[
            "file_system_id",
            "requester_node_id",
            "request_id",
            "operation_id",
            "authority_shard_id",
            "command_digest",
            "result",
            "posix_coordination_result",
            "created_at",
            "retained_at",
        ],
        "legacy-file-system" => &[
            "file_system_id",
            "display_name",
            "owner_node_id",
            "subtree_policies",
            "file_history",
        ],
        "namespaced-file-system" => &[
            "file_system_id",
            "display_name",
            "owner_node_id",
            "subtree_policies",
            "retention_policies",
            "module_metadata",
        ],
        "subtree-policy" => &[
            "path_prefix",
            "policy",
            "scope",
            "min_commit_replicas",
            "desired_replicas",
            "required_node_ids",
            "forbidden_node_ids",
            "min_distinct_zones",
        ],
        "retention-policy" | "history-policy" => &["rules"],
        "retention-rule" => &["path", "minimum"],
        "history-rule" => &["path", "retention"],
        "file-history-query-v1" => &["query"],
        "metadata-query" => &["file_system_id", "shard_id", "epoch", "selection"],
        "metadata-response-original" => &["file_system_id", "shard_id", "epoch", "result"],
        "selection/current-object" | "selection/object-lifecycle" => &["object_id"],
        "selection/version-pin" => &["identity", "pin_id"],
        "selection/restore-job" => &["job_id"],
        "selection/list" => &["namespace", "path", "object_id", "after", "limit"],
        "selection/version" => &["namespace", "allow_current", "identity"],
        "selection/revision-metadata" => &["identity"],
        "version-cursor" => &["object_id", "version"],
        "query-result/current-object" => &["object"],
        "query-result/object-lifecycle" => {
            &["object", "has_content_references", "references_complete"]
        }
        "query-result/version-pin" => &["identity", "pin_id", "present"],
        "query-result/restore-job" => &["job", "receipt"],
        "query-result/list" => &["entries", "after"],
        "query-result/version" | "query-result/revision-metadata" => &["descriptor"],
        "retained-descriptor" => &["version", "current_object", "references"],
        "revision-descriptor" => &["version", "current_object"],
        "job-receipt" => &[
            "file_restore",
            "job_id",
            "file_system_id",
            "origin",
            "kind",
            "status",
            "root_path",
            "target_path",
            "created_at",
            "terminal_at",
            "retained_at",
        ],
        "copy-receipt" => &["request", "requester_node_id", "result"],
        "poll-namespace-changes" => &["request"],
        "change-poll-request" => &["root_path", "cursor"],
        "change-poll-response" => &["batch"],
        "change-cursor" => &[
            "file_system_id",
            "shard_id",
            "authority_epoch",
            "after_sequence",
        ],
        "change-batch" => &["next_cursor", "events", "journal_head_sequence", "has_more"],
        "change-event/upsert" => &[
            "path",
            "object_id",
            "version",
            "content_hash",
            "content_size_bytes",
            "kind",
            "provenance",
        ],
        "change-event/remove" => &["path", "object_id", "version", "content_hash", "provenance"],
        "change-event/rename" => &[
            "from_path",
            "to_path",
            "object_id",
            "version",
            "content_hash",
            "content_size_bytes",
            "kind",
            "provenance",
        ],
        "change-event/revision-metadata-changed" => &["object_id", "version", "provenance"],
        "change-event/rescan-required" => &["reason", "detail"],
        "change-provenance" => &[
            "event_id",
            "shard_id",
            "authority_epoch",
            "journal_sequence",
            "journal_ordinal",
            "journal_created_at",
            "transaction_id",
            "operation_id",
            "requester_node_id",
            "request_id",
            "created_by_event_id",
            "last_modified_by_event_id",
        ],
        "read-range" => &[
            "object_id",
            "version",
            "content_hash",
            "offset",
            "total_size_bytes",
            "bytes",
        ],
        "object-expectation" => &["object_id", "expected_version"],
        "path-expectation" => &["path", "expected_object_id"],
        "commit-local-eventual-batch" => &["transaction", "writes"],
        "local-write-proof" => &[
            "expected_base_version",
            "expected_parent_object_id",
            "expected_parent_version",
            "receipt",
        ],
        "replica-receipt" => &["receipt", "replica_node_id", "content_hash", "received_at"],
        "write-receipt" => &[
            "file_system_id",
            "object_id",
            "committed_version",
            "owner_node_id",
            "event_id",
        ],
        "transaction" => &[
            "content_reference_identity",
            "contribution_digest",
            "extension_expectations",
            "transaction_id",
            "file_system_id",
            "objects",
            "versions",
            "file_version_pins",
            "advisory_lock_states",
            "created_at",
        ],
        "extension-expectation" => &["object_id", "version", "expected"],
        "object" => &[
            "object_id",
            "file_system_id",
            "path",
            "kind",
            "tombstone_provenance",
            "owner_node_id",
            "current_locations",
            "content_hash",
            "content_size_bytes",
            "link_target_object_id",
            "mode",
            "uid",
            "gid",
            "accessed_at_unix_nanos",
            "modified_at_unix_nanos",
            "created_at_unix_nanos",
            "changed_at_unix_nanos",
            "version",
            "policy",
            "created_by_event_id",
            "last_modified_by_event_id",
        ],
        "source-eviction" => &["source_shard_id", "target_shard_id", "pin_transfer_id"],
        "legacy-version" => &[
            "file_system_id",
            "object_id",
            "version",
            "content_hash",
            "size_bytes",
            "locations",
            "created_by_event_id",
            "created_at",
            "visible",
            "history_order",
            "retained",
            "content_durability",
        ],
        "namespaced-version" => &[
            "file_system_id",
            "object_id",
            "version",
            "content_hash",
            "size_bytes",
            "locations",
            "created_by_event_id",
            "created_at",
            "visible",
            "retention",
            "module_metadata",
            "content_durability",
        ],
        "retained-version" => &[
            "object",
            "captured_at_unix_nanos",
            "source",
            "policy_capture",
        ],
        "source/checkpoint" => &["operation_id"],
        "source/retention-rule" => &[],
        "source/local-operation" => &["operation_id", "base_version"],
        "policy-capture" => &["order", "captured_at_unix_nanos", "retention"],
        "retention-minimum" => &["keep_last", "keep_for_seconds"],
        "revision-retention" => &["order", "grant"],
        "revision-grant" => &["object", "granted_at_unix_nanos", "origin", "minimum"],
        "ordered-minimum" => &["order", "granted_at_unix_nanos", "minimum"],
        "adopt-object-at-path" => &[
            "object",
            "source_path",
            "pin_transfer_id",
            "versions",
            "version_pins",
            "version_pin_set_fingerprint",
            "receipts",
            "intents",
            "intent_receipts",
            "intent_receipt_retention",
            "content_reference_outcomes",
            "content_reference_outcome_retention",
            "conflicts",
            "advisory_lock_state",
        ],
        "verify-object-family-adoption" => &["object", "source_path", "pin_transfer_id"],
        "adopt-hardlink-alias" => &["alias", "source_path"],
        "install-hardlink-target-fence" => &["fence", "source_path"],
        "commit-hardlink-target-fence" => &[
            "fence_job_id",
            "source_path",
            "target_path",
            "expected_target",
            "committed_target",
        ],
        "job" => &[
            "job_id",
            "file_system_id",
            "origin",
            "kind",
            "status",
            "root_path",
            "target_path",
            "pending_frames",
            "processed_object_count",
            "created_at",
            "updated_at",
            "completed_at",
            "failure_reason",
        ],
        "job-origin" => &["requester_node_id", "request_id", "operation_id"],
        "job-frame/file-history-restore" => &["state"],
        "job-frame/scan-directory" => &["path", "cursor"],
        "job-frame/complete-directory" => &["path"],
        "job-frame/rename-directory-start" => &["source_object_id"],
        "job-frame/rename-directory-scan" => {
            &["source_object_id", "source_path", "target_path", "cursor"]
        }
        "job-frame/rename-directory-complete" => {
            &["source_object_id", "source_path", "target_path"]
        }
        "job-frame/cross-shard-adopt" => {
            &["moved_object", "old_path", "destination_primary_node_id"]
        }
        "job-frame/cross-shard-complete" => &["moved_object", "old_path"],
        "job-frame/cross-shard-hardlink-adopt" => {
            &["alias", "target_object_id", "destination_primary_node_id"]
        }
        "job-frame/cross-shard-hardlink-complete" => &["target_object_id"],
        "job-frame/cross-shard-hardlink-target-fence-install" => &["fence", "source_path"],
        "job-frame/cross-shard-hardlink-alias-commit" => &["transaction"],
        "job-frame/cross-shard-hardlink-target-fence-commit" => &[
            "fence_job_id",
            "source_path",
            "expected_target",
            "committed_target",
        ],
        "job-frame/hardlink-target-fence-reservation" => {
            &["source_path", "expected_target", "committed_target"]
        }
        "job-frame/cross-shard-dir-start" => &["source_object_id", "source_root", "target_root"],
        "job-frame/cross-shard-dir-scan" => &["source_path", "target_path", "cursor"],
        "job-frame/cross-shard-dir-finish" => &["source_root"],
        "directory-cursor" => &["path", "object_id", "dentry", "shard_id"],
        "dentry-cursor" => &["dirfrag_id", "name_hash", "name"],
        "copy-state" => &[
            "request",
            "requester_node_id",
            "options",
            "write_operation_id",
            "planned",
            "source_pin",
            "source_pin_applied",
            "content_hash",
            "content_size_bytes",
            "result",
            "cleanup_complete",
        ],
        "copy-result/applied" => &["response"],
        "copy-result/rejected" => &["reason"],
        "copy-response" => &["restored_from", "committed", "path"],
        "intent" => &[
            "publication",
            "intent_id",
            "file_system_id",
            "object_id",
            "base_version",
            "target_version",
            "coordinator_node_id",
            "operation_id",
            "operation_order_token",
            "owner_epoch",
            "desired_replica_node_ids",
            "policy",
            "status",
            "content_hash",
            "object_metadata",
            "touch_parent_on_create",
            "create_predecessor_object_id",
            "created_at",
            "updated_at",
        ],
        "intent-receipt" => &[
            "publication",
            "intent_id",
            "file_system_id",
            "object_id",
            "base_version",
            "target_version",
            "coordinator_node_id",
            "operation_id",
            "operation_order_token",
            "owner_epoch",
            "desired_replica_node_ids",
            "policy",
            "status",
            "content_hash",
            "touch_parent_on_create",
            "create_predecessor_object_id",
            "created_at",
            "terminal_at",
            "retained_at",
        ],
        "publication/current" | "publication/retained-repair" => &[],
        "publication/retained" => &["capture"],
        "retained-publication" => &["namespace", "reference_id", "metadata"],
        "legacy-publication" => &["pin_id", "source"],
        "intent-retention" => &[
            "max_evicted_counter_by_object_and_coordinator",
            "max_evicted_operation_order_token_by_object_and_coordinator",
            "evicted_opaque_intent_ids_by_object",
            "opaque_intent_history_evicted_object_ids",
        ],
        "reference-outcome" => &[
            "content_reference_identity",
            "contribution_digest",
            "transaction_id",
            "file_system_id",
            "transaction_digest",
            "created_at",
            "retained_at",
        ],
        "reference-outcome-retention" => &[
            "file_system_id",
            "max_evicted_operation_order_token",
            "opaque_operation_history_evicted",
        ],
        "conflict-record" => &[
            "conflict_id",
            "file_system_id",
            "object_id",
            "path",
            "base_version",
            "local_version",
            "remote_version",
            "detected_by_node_id",
            "reason",
            "local_content_hash",
            "remote_content_hash",
            "created_by_intent_id",
            "strategy",
            "status",
            "detected_at",
        ],
        "lock-state" => &[
            "file_system_id",
            "object_id",
            "authority_epoch",
            "authority_rebind_required",
            "grant_not_before_unix_nanos",
            "grants",
            "waiters",
            "request_outcomes",
            "cancelled_acquires",
            "migration_source_fingerprints",
            "next_queue_sequence",
        ],
        "lock-waiter" => &[
            "request_id",
            "session_id",
            "mode",
            "process_id",
            "authority_epoch",
            "queue_sequence",
            "lease_deadline_unix_nanos",
        ],
        "lock-request-outcome" => &["request_id", "command", "outcome", "recorded_at_unix_nanos"],
        "lock-acquire-tombstone" => &["acquire_request_id", "session_id"],
        "content-durability" => &[
            "current_policy_granted_at_unix_nanos",
            "source_version",
            "coordinator_node_id",
            "owner_epoch",
            "desired_replica_node_ids",
            "policy",
        ],
        "file-history-pin-v1" => &["request"],
        "content-reference-request" => {
            &["path", "identity", "operation", "operation_id", "namespace"]
        }
        "reference-capture" => &["pin_id", "metadata"],
        "reference-pin" | "reference-unpin" => &["pin_id"],
        "module-value" => &["schema_version", "payload"],
        "file-history-restore-begin-v1" => &["path", "request", "options"],
        "copy-request" => &[
            "source",
            "destination_path",
            "expected_destination",
            "operation_id",
        ],
        "write-options" => &[
            "policy",
            "replica_candidates",
            "wait_for_commit",
            "foreground_replica_repair",
        ],
        "file-sync" | "directory-sync" => &["path", "target"],
        "file-sync-target" => &[
            "kind",
            "file_system_id",
            "object_id",
            "path",
            "version",
            "content_hash",
        ],
        "directory-sync-target" => &["file_system_id", "object_id", "path", "version"],
        "coord/file-sync" => &["target", "content_evidence", "metadata_certificate"],
        "coord/directory-sync" => &["target", "metadata_certificate"],
        "file-sync-evidence" => &["intent_id", "required_receipts", "receipt_node_ids"],
        "metadata-certificate" => &["target", "quorum_size", "acknowledged_node_ids"],
        "metadata-sync-target" => &[
            "file_system_id",
            "shard_id",
            "primary_node_id",
            "authority_epoch",
            "replica_node_ids",
            "expected_shard",
            "journal_sequence",
            "barrier_id",
            "state_fingerprint",
        ],
        "source-shard-config" => &[
            "file_system_id",
            "shard_id",
            "primary_node_id",
            "replica_node_ids",
            "epoch",
            "assignment",
            "namespace_prepare_codec",
            "default_policy",
        ],
        "path-prefix" => &["prefix"],
        "name-hash-range" => &["start_inclusive", "end_exclusive"],
        "advisory-lock" => &["path", "object_id", "command", "timeout_millis"],
        "lock-command/acquire" => &["request_id", "session_id", "mode", "blocking", "process_id"],
        "lock-command/query" => &["request_id", "session_id", "mode"],
        "lock-command/renew" | "lock-command/unlock" => &["request_id", "token"],
        "lock-command/convert" => &["request_id", "token", "mode"],
        "lock-command/close" => &["request_id", "session_id"],
        "lock-command/cancel" => &["request_id", "session_id", "acquire_request_id"],
        "lock-token" => &[
            "file_system_id",
            "object_id",
            "session_id",
            "authority_epoch",
            "grant_id",
        ],
        "lock-grant" => &[
            "token",
            "mode",
            "process_id",
            "lease_deadline_unix_nanos",
            "lease_remaining_nanos",
        ],
        "lock-conflict" => &["session_id", "mode", "process_id"],
        "lock-outcome/acquired" | "lock-outcome/renewed" | "lock-outcome/converted" => &["grant"],
        "lock-outcome/waiting" => &["acquire_request_id", "position"],
        "lock-outcome/would-block" | "lock-outcome/query-result" => &["conflict"],
        "lock-outcome/closed" => &["released_grant", "cancelled_waiter_count"],
        "lock-outcome/cancelled" => &["acquire_request_id", "was_waiting"],
        "lock-outcome/wait-expired" => &["acquire_request_id"],
        "lock-outcome/fenced" => &["authority_epoch"],
        "lock-outcome/authority-warming" => &["retry_after_unix_nanos"],
        "lock-outcome/stale-authority" => &["requested_epoch", "state_epoch"],
        "lock-outcome/unlocked"
        | "lock-outcome/lock-lost"
        | "lock-outcome/stale-token"
        | "lock-outcome/session-busy"
        | "lock-outcome/request-mismatch" => &[],
        "coord/object-family-adoption-verified" => &[
            "object_id",
            "target_version",
            "pin_transfer_id",
            "durable_through_sequence",
        ],
        "mkdir"
        | "mkdir-all"
        | "rmdir"
        | "unlink"
        | "touch-directory-modified"
        | "clear-dir-recursive"
        | "sync-path" => &["path"],
        "mkdir-with-attrs" => &["path", "attrs"],
        "mkdir-at-no-links" => &[
            "path",
            "expected_parent_object_id",
            "expected_parent_version",
        ],
        "conditional-rmdir" | "conditional-unlink" => &["path", "expected"],
        "rename" => &["from", "to"],
        "conditional-rename" => &["from", "to", "expected_source", "expected_destination"],
        "rename-single-namespace-atomic" => {
            &["from", "to", "expected_shard_id", "expected_shard_epoch"]
        }
        "rename-directory-bounded" => &["from", "to", "max_steps"],
        "resume-namespace-job-bounded" => &["job_id", "path", "max_steps"],
        "hardlink" => &["existing_path", "link_path"],
        "set-mode" => &["path", "mode"],
        "set-owner" | "set-symlink-owner" => &["path", "uid", "gid"],
        "set-times" => &[
            "path",
            "accessed_at_unix_nanos",
            "modified_at_unix_nanos",
            "follow_symlink",
        ],
        "lookup-path" => &["path", "inline_range"],
        "lookup-paths" => &["paths"],
        "reconcile-placement-v1" => &[
            "job_id",
            "path",
            "object_id",
            "expected_version",
            "object_policy",
            "replication_policy",
            "desired_replica_node_ids",
        ],
        "replication-policy" => &[
            "min_commit_replicas",
            "desired_replicas",
            "required_node_ids",
            "forbidden_node_ids",
            "min_distinct_zones",
            "node_zones",
        ],
        "advisory-lock-migration-prepare" => {
            &["path", "source_path", "object_id", "state_fingerprint"]
        }
        "finalize-object-family-adoption" => &[
            "path",
            "source_path",
            "object_id",
            "target_version",
            "pin_transfer_id",
        ],
        "rejected" => &["reason", "posix_error", "current_primary_node_id"],
        "not-found"
        | "already-exists"
        | "not-directory"
        | "is-directory"
        | "directory-not-empty"
        | "invalid-path"
        | "not-symlink"
        | "missing-parent"
        | "busy" => &["path"],
        "conflict" => &["path", "reason"],
        "symlink-content-pending" => &["path", "stage", "detail"],
        "advisory-lock-migration" => {
            &["object_id", "state_fingerprint", "durable_through_sequence"]
        }
        "advisory-lock-migration-prepared" => &["object_id", "state_fingerprint"],
        "object-family-adoption" => &[
            "object_id",
            "target_version",
            "pin_transfer_id",
            "pin_transfer_active",
            "version_pin_set_fingerprint",
            "advisory_lock_state_fingerprint",
            "content_reference_outcome_fingerprint",
            "durable_through_sequence",
        ],
        "object-family-adoption-finalized" => {
            &["object_id", "pin_transfer_id", "durable_through_sequence"]
        }
        _ => return None,
    })
}
fn original_optional(schema: &str, key: &str) -> bool {
    matches!(
        (schema, key),
        (
            "request",
            "result" | "posix_coordination_result" | "last_recovery_error"
        ) | ("completion", "posix_coordination_result")
            | ("pin", "namespace")
            | ("copy-request", "expected_destination")
            | ("request-receipt", "posix_coordination_result")
            | ("selection/list", "path" | "object_id" | "after")
            | (
                "query-result/current-object" | "query-result/object-lifecycle",
                "object"
            )
            | ("query-result/restore-job", "job" | "receipt")
            | ("query-result/list", "after")
            | (
                "query-result/version" | "query-result/revision-metadata",
                "descriptor"
            )
            | ("local-write-proof", "expected_base_version")
            | ("extension-expectation", "expected")
            | ("source/local-operation", "base_version")
            | (
                "copy-state",
                "source_pin" | "content_hash" | "content_size_bytes" | "result"
            )
    )
}
fn original_default(schema: &str, key: &str) -> Option<Value> {
    if original_empty_list(schema, key) {
        return Some(Value::Array(Vec::new()));
    }
    if original_empty_map(schema, key) {
        return Some(serde_json::json!({}));
    }
    match (schema, key) {
        ("pins", "add" | "remove") => Some(Value::Array(Vec::new())),
        ("replication-policy", "required_node_ids" | "forbidden_node_ids") => {
            Some(Value::Array(Vec::new()))
        }
        ("replication-policy", "node_zones") => Some(serde_json::json!({})),
        ("replication-policy", "min_distinct_zones") => Some(Value::from(0)),
        ("symlink-content-pending", "stage" | "detail") => Some(Value::String(String::new())),
        ("advisory-lock", "timeout_millis") => Some(Value::from(45_000)),
        ("lock-grant", "lease_remaining_nanos") => Some(Value::from(0)),
        ("metadata-sync-target", "barrier_id" | "state_fingerprint") => {
            Some(Value::String(String::new()))
        }
        ("metadata-sync-target", "replica_node_ids") => Some(Value::Array(Vec::new())),
        ("source-shard-config", "replica_node_ids") => Some(Value::Array(Vec::new())),
        ("source-shard-config", "namespace_prepare_codec") => Some(Value::String("legacy".into())),
        ("selection/list" | "selection/version", "namespace") => {
            Some(Value::String(DFS_HISTORY_METADATA_NAMESPACE.into()))
        }
        ("selection/version", "allow_current") => Some(Value::Bool(false)),
        ("subtree-policy", "scope") => Some(Value::String("subtree".into())),
        ("subtree-policy", "min_distinct_zones") => Some(Value::from(0)),
        ("legacy-file-system", "file_history") => Some(serde_json::json!({})),
        (
            "object",
            "mode"
            | "uid"
            | "gid"
            | "accessed_at_unix_nanos"
            | "modified_at_unix_nanos"
            | "created_at_unix_nanos"
            | "changed_at_unix_nanos",
        )
        | ("legacy-version", "history_order")
        | ("retention-minimum", "keep_last" | "keep_for_seconds")
        | ("intent" | "intent-receipt", "owner_epoch")
        | ("lock-state", "grant_not_before_unix_nanos" | "next_queue_sequence") => {
            Some(Value::from(0))
        }
        ("intent" | "intent-receipt", "touch_parent_on_create")
        | ("lock-state", "authority_rebind_required")
        | ("reference-outcome-retention", "opaque_operation_history_evicted") => {
            Some(Value::Bool(false))
        }
        ("intent" | "intent-receipt", "publication") => Some(serde_json::json!({"kind":"current"})),
        _ => None,
    }
}
fn original_empty_list(schema: &str, key: &str) -> bool {
    if matches!(
        (schema, key),
        ("pin-transfer", "source_pins" | "copied_pins")
            | (
                "namespace-retention",
                "opaque_request_history_evicted_requester_node_ids"
            )
            | (
                "legacy-file-system" | "namespaced-file-system",
                "subtree_policies"
            )
            | ("subtree-policy", "required_node_ids" | "forbidden_node_ids")
            | ("history-policy" | "retention-policy", "rules")
    ) {
        return true;
    }
    matches!(
        (schema, key),
        (
            "transaction",
            "extension_expectations" | "versions" | "advisory_lock_states"
        ) | ("object", "current_locations")
            | ("legacy-version" | "namespaced-version", "locations")
            | (
                "adopt-object-at-path",
                "versions"
                    | "version_pins"
                    | "receipts"
                    | "intents"
                    | "intent_receipts"
                    | "content_reference_outcomes"
                    | "conflicts"
            )
            | ("job", "pending_frames")
            | ("intent" | "intent-receipt", "desired_replica_node_ids")
            | (
                "lock-state",
                "grants"
                    | "waiters"
                    | "request_outcomes"
                    | "cancelled_acquires"
                    | "migration_source_fingerprints"
            )
            | (
                "intent-retention",
                "opaque_intent_history_evicted_object_ids"
            )
    )
}
fn original_empty_map(schema: &str, key: &str) -> bool {
    if matches!(
        (schema, key),
        (
            "namespaced-file-system",
            "retention_policies" | "module_metadata"
        ) | (
            "namespace-retention",
            "content_reference_outcome_retention" | "max_evicted_request_order_token_by_requester"
        )
    ) {
        return true;
    }
    matches!(
        (schema, key),
        (
            "intent-retention",
            "max_evicted_counter_by_object_and_coordinator"
                | "max_evicted_operation_order_token_by_object_and_coordinator"
                | "evicted_opaque_intent_ids_by_object"
        ) | ("namespaced-version", "retention" | "module_metadata")
            | ("adopt-object-at-path", "intent_receipt_retention")
    )
}
fn original_omit(schema: &str, key: &str, value: &Value) -> bool {
    if schema == "legacy-file-system"
        && key == "file_history"
        && value
            .get("rules")
            .is_none_or(|rules| rules.as_array().is_some_and(Vec::is_empty))
    {
        return true;
    }
    if schema == "subtree-policy"
        && ((key == "scope" && value.as_str() == Some("subtree"))
            || (key == "min_distinct_zones" && value.as_u64() == Some(0)))
    {
        return true;
    }
    if value.is_null()
        && matches!(
            (schema, key),
            ("subtree-policy", "min_commit_replicas" | "desired_replicas")
                | (
                    "namespace-retention",
                    "transaction_floor" | "prepare_floor" | "job_floor"
                )
        )
    {
        return true;
    }
    if (original_empty_list(schema, key) && value.as_array().is_some_and(Vec::is_empty))
        || (original_empty_map(schema, key)
            && !matches!(schema, "namespaced-version" | "namespaced-file-system")
            && value.as_object().is_some_and(serde_json::Map::is_empty))
    {
        return true;
    }
    if matches!(
        (schema, key),
        (
            "object",
            "mode"
                | "uid"
                | "gid"
                | "accessed_at_unix_nanos"
                | "modified_at_unix_nanos"
                | "created_at_unix_nanos"
                | "changed_at_unix_nanos"
        ) | ("legacy-version", "history_order")
    ) && value.as_u64() == Some(0)
    {
        return true;
    }
    if matches!(schema, "intent" | "intent-receipt")
        && key == "publication"
        && value.get("kind").and_then(Value::as_str) == Some("current")
    {
        return true;
    }
    if schema == "pin" && key == "namespace" && value.is_null() {
        return true;
    }
    if schema == "pins" && value.as_array().is_some_and(Vec::is_empty) {
        return true;
    }
    if matches!(schema, "metadata-sync-target" | "source-shard-config")
        && key == "replica_node_ids"
        && value.as_array().is_some_and(Vec::is_empty)
    {
        return true;
    }
    if schema == "source-shard-config"
        && key == "namespace_prepare_codec"
        && value.as_str() == Some("legacy")
    {
        return true;
    }
    if schema == "replication-policy"
        && match key {
            "required_node_ids" | "forbidden_node_ids" => {
                value.as_array().is_some_and(Vec::is_empty)
            }
            "node_zones" => value.as_object().is_some_and(serde_json::Map::is_empty),
            "min_distinct_zones" => value.as_u64() == Some(0),
            _ => false,
        }
    {
        return true;
    }
    value.is_null()
        && matches!(
            (schema, key),
            (
                "transaction",
                "content_reference_identity" | "contribution_digest" | "file_version_pins"
            ) | ("object-expectation", "expected_version")
                | ("path-expectation", "expected_object_id")
                | ("job-receipt", "file_restore" | "origin" | "target_path")
                | (
                    "change-event/upsert" | "change-event/rename",
                    "content_hash" | "content_size_bytes"
                )
                | ("change-event/remove" | "read-range", "content_hash")
                | (
                    "change-provenance",
                    "transaction_id"
                        | "operation_id"
                        | "requester_node_id"
                        | "request_id"
                        | "created_by_event_id"
                        | "last_modified_by_event_id"
                )
                | (
                    "object",
                    "tombstone_provenance"
                        | "content_hash"
                        | "content_size_bytes"
                        | "link_target_object_id"
                        | "created_by_event_id"
                        | "last_modified_by_event_id"
                )
                | (
                    "legacy-version" | "namespaced-version",
                    "content_hash"
                        | "size_bytes"
                        | "created_by_event_id"
                        | "retained"
                        | "content_durability"
                )
                | ("retained-version", "policy_capture")
                | ("revision-retention", "grant")
                | ("revision-grant", "minimum")
                | ("source-eviction", "pin_transfer_id")
                | (
                    "local-write-proof",
                    "expected_parent_object_id" | "expected_parent_version"
                )
                | ("replica-receipt", "content_hash")
                | (
                    "adopt-object-at-path",
                    "pin_transfer_id"
                        | "version_pin_set_fingerprint"
                        | "content_reference_outcome_retention"
                        | "advisory_lock_state"
                )
                | (
                    "job",
                    "origin" | "target_path" | "completed_at" | "failure_reason"
                )
                | (
                    "job-frame/scan-directory"
                        | "job-frame/rename-directory-scan"
                        | "job-frame/cross-shard-dir-scan",
                    "cursor"
                )
                | ("directory-cursor", "dentry" | "shard_id")
                | (
                    "intent" | "intent-receipt",
                    "base_version"
                        | "operation_id"
                        | "operation_order_token"
                        | "content_hash"
                        | "object_metadata"
                        | "create_predecessor_object_id"
                        | "updated_at"
                )
                | ("retained-publication", "metadata")
                | (
                    "reference-outcome",
                    "content_reference_identity" | "contribution_digest"
                )
                | (
                    "reference-outcome-retention",
                    "max_evicted_operation_order_token"
                )
                | (
                    "conflict-record",
                    "base_version"
                        | "local_content_hash"
                        | "remote_content_hash"
                        | "created_by_intent_id"
                        | "strategy"
                )
                | ("lock-waiter", "process_id")
                | ("content-durability", "current_policy_granted_at_unix_nanos")
                | ("content-reference-request", "namespace")
                | ("reference-capture", "metadata")
                | (
                    "lock-command/acquire" | "lock-grant" | "lock-conflict",
                    "process_id"
                )
                | ("lock-outcome/query-result", "conflict")
                | ("metadata-sync-target", "authority_epoch" | "expected_shard")
                | ("lookup-path", "inline_range")
                | (
                    "set-times",
                    "accessed_at_unix_nanos" | "modified_at_unix_nanos"
                )
                | ("rejected", "posix_error" | "current_primary_node_id")
                | (
                    "object-family-adoption",
                    "advisory_lock_state_fingerprint" | "content_reference_outcome_fingerprint"
                )
        )
}
fn original_child(schema: &str, key: &str) -> &'static str {
    match (schema, key) {
        ("metadata-effect/put-file-system", "file_system") => "file-system",
        ("metadata-effect/put-object", "object")
        | ("metadata-effect/finalize-cross-shard-source-eviction", "tombstone") => "object",
        ("metadata-effect/put-version", "version") => "version",
        ("metadata-effect/put-version-pin", "pin") => "pin",
        ("metadata-effect/reconcile-cross-shard-pin-transfer", "versions") => "list/version",
        ("metadata-effect/reconcile-cross-shard-pin-transfer", "transfer") => "pin-transfer",
        ("metadata-effect/finalize-cross-shard-source-eviction", "removed_pins") => "list/pin",
        ("metadata-effect/finalize-cross-shard-source-eviction", "removed_pin_transfers") => {
            "list/pin-transfer"
        }
        ("metadata-effect/put-receipt", "receipt") => "replica-receipt",
        ("metadata-effect/put-intent", "intent") => "intent",
        ("metadata-effect/put-intent-receipt", "receipt") => "intent-receipt",
        (
            "metadata-effect/remove-intent-receipt"
            | "metadata-effect/merge-intent-receipt-retention",
            "retention",
        ) => "intent-retention",
        ("metadata-effect/put-advisory-lock-state", "state") => "lock-state",
        ("metadata-effect/merge-content-reference-outcome-retention", "retention") => {
            "reference-outcome-retention"
        }
        ("metadata-effect/put-namespace-transaction-receipt", "receipt") => "reference-outcome",
        (
            "metadata-effect/remove-namespace-transaction-receipt"
            | "metadata-effect/remove-namespace-job-receipt"
            | "metadata-effect/remove-namespace-request-receipt",
            "retention",
        ) => "namespace-retention",
        ("metadata-effect/put-namespace-job", "job") => "job",
        ("metadata-effect/put-namespace-job-receipt", "receipt") => "job-receipt",
        ("metadata-effect/put-namespace-request", "request") => "request",
        ("metadata-effect/put-namespace-request-receipt", "receipt") => "request-receipt",
        (
            "metadata-effect/remove-prepared-operation"
            | "metadata-effect/resolve-prepared-operation",
            "digest",
        ) => "prepared-digest",
        ("pin-transfer", "source_pins" | "copied_pins") => "list/pin",
        ("namespace-retention", "transaction_floor" | "prepare_floor" | "job_floor") => {
            "receipt-order"
        }
        ("namespace-retention", "content_reference_outcome_retention") => {
            "map/reference-outcome-retention"
        }
        ("request-receipt", "result") => "result",
        ("request-receipt", "posix_coordination_result") => "coordination",
        ("legacy-file-system" | "namespaced-file-system", "subtree_policies") => {
            "list/subtree-policy"
        }
        ("namespaced-file-system", "retention_policies") => "map/retention-policy",
        ("namespaced-file-system", "module_metadata") => "map/module-value",
        ("legacy-file-system", "file_history") => "history-policy",
        ("history-policy", "rules") => "list/history-rule",
        ("retention-policy", "rules") => "list/retention-rule",
        ("history-rule", "retention") | ("retention-rule", "minimum") => "retention-minimum",
        ("file-history-query-v1", "query") => "metadata-query",
        ("metadata-query", "selection") => "selection",
        ("metadata-response-original", "result") => "query-result",
        (
            "selection/version-pin"
            | "selection/version"
            | "selection/revision-metadata"
            | "query-result/version-pin",
            "identity",
        ) => "revision",
        ("selection/list" | "query-result/list", "after") => "version-cursor",
        ("query-result/current-object" | "query-result/object-lifecycle", "object") => "object",
        ("query-result/restore-job", "job") => "job",
        ("query-result/restore-job", "receipt") => "job-receipt",
        ("query-result/list", "entries") => "list/retained-descriptor",
        ("query-result/version", "descriptor") => "retained-descriptor",
        ("query-result/revision-metadata", "descriptor") => "revision-descriptor",
        ("retained-descriptor" | "revision-descriptor", "version") => "version",
        ("retained-descriptor" | "revision-descriptor", "current_object") => "object",
        ("retained-descriptor", "references") => "list/pin",
        ("job-receipt", "file_restore") => "copy-receipt",
        ("job-receipt", "kind") => "job-kind",
        ("job-receipt", "origin") => "job-origin",
        ("copy-receipt", "request") => "copy-request",
        ("copy-receipt", "result") => "copy-result",
        ("poll-namespace-changes", "request") => "change-poll-request",
        ("change-poll-request", "cursor") | ("change-batch", "next_cursor") => "change-cursor",
        ("change-poll-response", "batch") => "change-batch",
        ("change-batch", "events") => "list/change-event",
        (
            "change-event/upsert"
            | "change-event/remove"
            | "change-event/rename"
            | "change-event/revision-metadata-changed",
            "provenance",
        ) => "change-provenance",
        ("commit-local-eventual-batch", "transaction")
        | ("job-frame/cross-shard-hardlink-alias-commit", "transaction") => "transaction",
        ("commit-local-eventual-batch", "writes") => "list/local-write-proof",
        ("local-write-proof", "receipt") => "replica-receipt",
        ("replica-receipt", "receipt") => "write-receipt",
        ("transaction", "content_reference_identity") => "revision",
        ("transaction", "extension_expectations") => "list/extension-expectation",
        ("transaction", "objects") => "list/object",
        ("transaction" | "adopt-object-at-path", "versions") => "list/version",
        ("transaction", "file_version_pins") => "pins",
        ("transaction", "advisory_lock_states") => "list/lock-state",
        ("extension-expectation", "expected") => "version",
        ("object", "tombstone_provenance") => "tombstone",
        ("legacy-version", "retained") => "retained-version",
        ("legacy-version" | "namespaced-version", "content_durability") => "content-durability",
        ("namespaced-version", "retention") => "map/revision-retention",
        ("namespaced-version", "module_metadata") => "map/module-value",
        ("retained-version", "object") | ("revision-grant", "object") => "object",
        ("retained-version" | "legacy-publication", "source") => "source",
        ("retained-version", "policy_capture") => "policy-capture",
        ("policy-capture", "retention") | ("ordered-minimum", "minimum") => "retention-minimum",
        ("revision-retention", "grant") => "revision-grant",
        ("revision-grant", "minimum") => "ordered-minimum",
        ("adopt-object-at-path" | "verify-object-family-adoption", "object")
        | ("adopt-hardlink-alias", "alias")
        | ("commit-hardlink-target-fence", "committed_target") => "object",
        ("adopt-object-at-path", "version_pins") => "list/pin",
        ("adopt-object-at-path", "receipts") => "list/replica-receipt",
        ("adopt-object-at-path", "intents") => "list/intent",
        ("adopt-object-at-path", "intent_receipts") => "list/intent-receipt",
        ("adopt-object-at-path", "intent_receipt_retention") => "intent-retention",
        ("adopt-object-at-path", "content_reference_outcomes") => "list/reference-outcome",
        ("adopt-object-at-path", "content_reference_outcome_retention") => {
            "reference-outcome-retention"
        }
        ("adopt-object-at-path", "conflicts") => "list/conflict-record",
        ("adopt-object-at-path", "advisory_lock_state") => "lock-state",
        ("install-hardlink-target-fence", "fence")
        | ("job-frame/cross-shard-hardlink-target-fence-install", "fence") => "job",
        ("commit-hardlink-target-fence", "expected_target")
        | (
            "job-frame/cross-shard-hardlink-target-fence-commit"
            | "job-frame/hardlink-target-fence-reservation",
            "expected_target",
        ) => "precondition",
        (
            "job-frame/cross-shard-hardlink-target-fence-commit"
            | "job-frame/hardlink-target-fence-reservation",
            "committed_target",
        )
        | ("job-frame/cross-shard-adopt" | "job-frame/cross-shard-complete", "moved_object")
        | ("job-frame/cross-shard-hardlink-adopt", "alias") => "object",
        ("job", "origin") => "job-origin",
        ("job", "kind") => "job-kind",
        ("job", "pending_frames") => "list/job-frame",
        ("job-frame/file-history-restore", "state") => "copy-state",
        (
            "job-frame/scan-directory"
            | "job-frame/rename-directory-scan"
            | "job-frame/cross-shard-dir-scan",
            "cursor",
        ) => "directory-cursor",
        ("directory-cursor", "dentry") => "dentry-cursor",
        ("copy-state", "request") => "copy-request",
        ("copy-state", "options") => "write-options",
        ("copy-state", "planned") => "revision",
        ("copy-state", "source_pin") => "content-reference-request",
        ("copy-state", "result") => "copy-result",
        ("copy-result/applied", "response") => "copy-response",
        ("copy-response", "restored_from" | "committed") => "revision",
        ("intent" | "intent-receipt", "policy") => "replication-policy",
        ("intent", "object_metadata") => "object",
        ("intent" | "intent-receipt", "publication") => "publication",
        ("publication/retained", "capture") => "retained-publication-dispatch",
        ("retained-publication", "metadata") => "module-value",
        ("reference-outcome", "content_reference_identity") => "revision",
        ("lock-state", "grants") => "list/lock-grant",
        ("lock-state", "waiters") => "list/lock-waiter",
        ("lock-state", "request_outcomes") => "list/lock-request-outcome",
        ("lock-state", "cancelled_acquires") => "list/lock-acquire-tombstone",
        ("lock-request-outcome", "command") => "lock-command",
        ("lock-request-outcome", "outcome") => "lock-outcome",
        ("pins", "add" | "remove") => "pin-list",
        ("request", "operation") => "operation",
        ("request" | "completion", "result") => "result",
        ("request" | "completion", "posix_coordination_result") => "coordination",
        ("file-history-pin-v1", "request") => "content-reference-request",
        ("content-reference-request", "identity") => "revision",
        ("content-reference-request", "operation") => "reference-operation",
        ("reference-capture", "metadata") => "module-value",
        ("file-history-restore-begin-v1", "request") => "copy-request",
        ("file-history-restore-begin-v1", "options") => "write-options",
        ("copy-request", "source" | "expected_destination") => "revision",
        ("write-options", "policy") => "replication-policy",
        ("content-durability", "policy") => "replication-policy",
        ("file-sync" | "coord/file-sync", "target") => "file-sync-target",
        ("directory-sync" | "coord/directory-sync", "target") => "directory-sync-target",
        ("coord/file-sync", "content_evidence") => "file-sync-evidence",
        ("coord/file-sync" | "coord/directory-sync", "metadata_certificate") => {
            "metadata-certificate"
        }
        ("metadata-certificate", "target") => "metadata-sync-target",
        ("metadata-sync-target", "expected_shard") => "source-shard-config",
        ("source-shard-config", "assignment") => "assignment",
        ("advisory-lock", "command") => "lock-command",
        ("lock-command/renew" | "lock-command/unlock" | "lock-command/convert", "token")
        | ("lock-grant", "token") => "lock-token",
        ("lock-outcome/acquired" | "lock-outcome/renewed" | "lock-outcome/converted", "grant") => {
            "lock-grant"
        }
        ("lock-outcome/would-block" | "lock-outcome/query-result", "conflict") => "lock-conflict",
        (_, "attrs") => "attrs",
        (_, "expected" | "expected_source") if schema.starts_with("conditional-") => "precondition",
        ("conditional-rename", "expected_destination") => "destination",
        ("lookup-path", "inline_range") => "inline-range",
        ("reconcile-placement-v1", "replication_policy") => "replication-policy",
        ("rejected", "posix_error") => "posix-error",
        _ => "opaque",
    }
}
fn original_alias(schema: &str, key: &str) -> Option<&'static str> {
    match (schema, key) {
        ("reference-capture" | "reference-pin" | "reference-unpin", "pin_id") => {
            Some("reference_id")
        }
        ("copy-state", "source_pin") => Some("source_reference"),
        ("copy-state", "source_pin_applied") => Some("source_reference_applied"),
        ("job-receipt", "file_restore") => Some("content_copy"),
        _ => None,
    }
}

fn original_struct_schema<'a>(
    value: &'a Value,
    schema: &str,
) -> Result<(String, Option<(&'static str, &'a str)>)> {
    let mut selected = match schema {
        "version" if value.get("retention").is_none() && value.get("module_metadata").is_none() => {
            "legacy-version".to_owned()
        }
        "version" => "namespaced-version".to_owned(),
        "file-system"
            if value.get("retention_policies").is_none()
                && value.get("module_metadata").is_none() =>
        {
            "legacy-file-system".to_owned()
        }
        "file-system" => "namespaced-file-system".to_owned(),
        "retained-publication-dispatch" if value.get("namespace").is_none() => {
            "legacy-publication".to_owned()
        }
        "retained-publication-dispatch" => "retained-publication".to_owned(),
        _ => schema.to_owned(),
    };
    let internal = match schema {
        "lock-command" | "lock-outcome" | "job-frame" | "metadata-effect" => Some(("type", schema)),
        "reference-operation" => Some(("operation", "reference")),
        "source" | "publication" => Some(("kind", schema)),
        "copy-result" => Some(("status", schema)),
        "selection" | "query-result" => Some(("kind", schema)),
        "change-event" => Some(("type", schema)),
        _ => None,
    };
    let mut tag = None;
    if let Some((field, prefix)) = internal {
        let input = value
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("original internal enum omits its tag"))?;
        let canonical = match (schema, input) {
            ("reference-operation", "capture-current") => "capture",
            ("reference-operation", "retain") => "pin",
            ("reference-operation", "release") => "unpin",
            ("job-frame", "content-copy") => "file-history-restore",
            ("selection" | "query-result", "namespace-job") => "restore-job",
            ("selection" | "query-result", "retained-revisions") => "list",
            _ => input,
        };
        selected = if prefix == "reference" {
            format!("reference-{canonical}")
        } else {
            format!("{prefix}/{canonical}")
        };
        tag = Some((field, canonical));
    }
    Ok((selected, tag))
}

/// Locate only payload leaves declared by the original domain schema.
/// Walk only declared module payload leaves, one mutable leaf at a time.
/// The source frame/container bounds already bound breadth; no new aggregate
/// count limit may reject valid maps distributed across a source transaction.
fn visit_original_module_payloads(
    value: &mut Value,
    schema: &str,
    prefix: &str,
    visitor: &mut dyn FnMut(&str, &mut Value) -> Result<()>,
) -> Result<()> {
    fn child_path(parent: &str, key: &str) -> String {
        format!("{parent}/{}", key.replace('~', "~0").replace('/', "~1"))
    }
    fn walk(
        value: &mut Value,
        schema: &str,
        path: &str,
        depth: usize,
        visitor: &mut dyn FnMut(&str, &mut Value) -> Result<()>,
    ) -> Result<()> {
        if depth > 64 {
            return Err(invalid(
                "declared source module schema exceeds its nesting bound",
            ));
        }
        if value.is_null() || matches!(schema, "opaque" | "job-kind") {
            return Ok(());
        }
        if schema == "module-value" {
            if value.get("payload").is_none() {
                return Err(invalid("source module metadata omits payload"));
            }
            let payload = value.get_mut("payload").expect("declared payload checked");
            return visitor(&child_path(path, "payload"), payload);
        }
        if let Some(child) = schema.strip_prefix("map/") {
            for (key, row) in value
                .as_object_mut()
                .ok_or_else(|| invalid("declared source map is not an object"))?
            {
                walk(row, child, &child_path(path, key), depth + 1, visitor)?;
            }
            return Ok(());
        }
        if schema == "pin-list" || schema.starts_with("list/") {
            let child = schema.strip_prefix("list/").unwrap_or("pin");
            for (index, row) in value
                .as_array_mut()
                .ok_or_else(|| invalid("declared source rows are not a list"))?
                .iter_mut()
                .enumerate()
            {
                walk(
                    row,
                    child,
                    &child_path(path, &index.to_string()),
                    depth + 1,
                    visitor,
                )?;
            }
            return Ok(());
        }
        if matches!(
            schema,
            "operation"
                | "result"
                | "coordination"
                | "posix-error"
                | "destination"
                | "assignment"
                | "tombstone"
        ) {
            if value.is_string() {
                return Ok(());
            }
            let variants = value
                .as_object_mut()
                .ok_or_else(|| invalid("declared source enum is not an object"))?;
            if variants.len() != 1 {
                return Err(invalid("declared source enum is ambiguous"));
            }
            let (input, body) = variants.iter_mut().next().unwrap();
            let tag = match (schema, input.as_str()) {
                ("operation", "content-reference-v1" | "content-reference") => {
                    "file-history-pin-v1"
                }
                ("operation", "begin-content-copy") => "file-history-restore-begin-v1",
                ("operation", "metadata-query") => "file-history-query-v1",
                ("coordination", "metadata-query") => "file-history",
                ("operation", "reconcile-placement") => "reconcile-placement-v1",
                _ => input.as_str(),
            };
            let selected = match (schema, tag) {
                ("coordination", "advisory-lock") => "coord/advisory-lock",
                ("coordination", "file-sync") => "coord/file-sync",
                ("coordination", "directory-sync") => "coord/directory-sync",
                ("coordination", "object-family-adoption-verified") => {
                    "coord/object-family-adoption-verified"
                }
                ("coordination", "namespace-job") => "job",
                ("coordination", "file-history") => "metadata-response-original",
                ("coordination", "namespace-changes") => "change-poll-response",
                ("coordination", "lookup-path-range") => "read-range",
                _ => tag,
            };
            return walk(body, selected, &child_path(path, input), depth + 1, visitor);
        }
        let (selected, _) = original_struct_schema(value, schema)?;
        let fields = original_fields(&selected)
            .ok_or_else(|| invalid(format!("unknown original module payload schema {selected}")))?;
        for field in fields {
            let actual = if value.get(*field).is_some() {
                Some(*field)
            } else {
                original_alias(&selected, field).filter(|alias| value.get(*alias).is_some())
            };
            if let Some(key) = actual {
                walk(
                    value.get_mut(key).expect("declared field checked"),
                    original_child(&selected, field),
                    &child_path(path, key),
                    depth + 1,
                    visitor,
                )?;
            }
        }
        Ok(())
    }
    walk(value, schema, prefix, 0, visitor)
}

/// Pointer collection is retained only for source packed-path mapping and tests.
/// Conversion itself adapts each declared leaf with the streaming visitor.
pub(crate) fn original_module_payload_paths(
    value: &Value,
    schema: &str,
    prefix: &str,
) -> Result<Vec<String>> {
    let mut value = value.clone();
    let mut paths = Vec::new();
    visit_original_module_payloads(&mut value, schema, prefix, &mut |path, _| {
        paths.push(path.to_owned());
        Ok(())
    })?;
    Ok(paths)
}

pub(crate) fn normalize_original_module_schema_at(
    value: &mut Value,
    schema: &str,
    value_prefix: &str,
    original: &crate::metadata::source_document::SourceDocumentContext<'_>,
    source_prefix: &str,
    payload_sources: Option<&std::collections::BTreeMap<String, String>>,
) -> Result<()> {
    visit_original_module_payloads(value, schema, value_prefix, &mut |path, payload| {
        let source_path = match payload_sources {
            Some(mapping) => mapping
                .get(path)
                .ok_or_else(|| invalid("packed source omits declared module payload type mapping"))?
                .as_str(),
            None => path,
        };
        normalize_original_module_leaf(payload, original, &format!("{source_prefix}{source_path}"))
    })
}

impl Serialize for OriginalRouted<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        GenerationRouted {
            value: self.value,
            schema: self.schema,
            generation: SourceGeneration::Released,
        }
        .serialize(serializer)
    }
}
fn source_wire_field<'a>(generation: SourceGeneration, schema: &str, key: &'a str) -> &'a str {
    if generation == SourceGeneration::Released {
        return key;
    }
    match (schema, key) {
        ("reference-capture" | "reference-pin" | "reference-unpin", "pin_id") => "reference_id",
        ("copy-state", "source_pin") => "source_reference",
        ("copy-state", "source_pin_applied") => "source_reference_applied",
        ("job-receipt", "file_restore") => "content_copy",
        _ => key,
    }
}
fn source_wire_tag<'a>(generation: SourceGeneration, schema: &str, tag: &'a str) -> &'a str {
    if generation == SourceGeneration::Released {
        return tag;
    }
    match (schema, tag) {
        ("operation", "file-history-pin-v1") => "content-reference",
        ("operation", "file-history-restore-begin-v1") => "begin-content-copy",
        ("operation", "file-history-query-v1") => "metadata-query",
        ("operation", "reconcile-placement-v1") => "reconcile-placement",
        ("coordination", "file-history") => "metadata-query",
        ("reference-operation", "capture") => "capture-current",
        ("reference-operation", "pin") => "retain",
        ("reference-operation", "unpin") => "release",
        ("job-frame" | "job-kind", "file-history-restore") => "content-copy",
        ("selection" | "query-result", "restore-job") => "namespace-job",
        ("selection" | "query-result", "list") => "retained-revisions",
        _ => tag,
    }
}
impl Serialize for GenerationRouted<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeMap};
        if self.schema == "job-kind" {
            return match self.value.as_str() {
                Some("content-copy" | "file-history-restore") => serializer.serialize_str(
                    source_wire_tag(self.generation, "job-kind", "file-history-restore"),
                ),
                Some(value) => serializer.serialize_str(value),
                None => Err(S::Error::custom("original job kind is not text")),
            };
        }
        if let Some(schema) = self.schema.strip_prefix("map/") {
            let values = self
                .value
                .as_object()
                .ok_or_else(|| S::Error::custom("original typed dictionary is not a map"))?;
            let mut map = serializer.serialize_map(Some(values.len()))?;
            for (key, value) in values {
                map.serialize_entry(
                    key,
                    &GenerationRouted {
                        value,
                        schema,
                        generation: self.generation,
                    },
                )?;
            }
            return map.end();
        }
        if self.schema == "pin-list" || self.schema.starts_with("list/") {
            use serde::ser::SerializeSeq;
            let rows = self
                .value
                .as_array()
                .ok_or_else(|| S::Error::custom("original pin rows are not a list"))?;
            let mut seq = serializer.serialize_seq(Some(rows.len()))?;
            for row in rows {
                seq.serialize_element(&GenerationRouted {
                    generation: self.generation,
                    value: row,
                    schema: self.schema.strip_prefix("list/").unwrap_or("pin"),
                })?;
            }
            return seq.end();
        }
        if self.value.is_null() || self.schema == "opaque" {
            return self.value.serialize(serializer);
        }
        if matches!(
            self.schema,
            "operation"
                | "result"
                | "coordination"
                | "posix-error"
                | "destination"
                | "assignment"
                | "tombstone"
        ) {
            if let Some(tag) = self.value.as_str() {
                if matches!(
                    (self.schema, tag),
                    ("result", "applied") | ("destination", "absent") | ("assignment", "root")
                ) {
                    return serializer.serialize_str(tag);
                }
                return Err(S::Error::custom("unsupported original routed unit variant"));
            }
            let variants = self
                .value
                .as_object()
                .ok_or_else(|| S::Error::custom("original routed variant is not an object"))?;
            if variants.len() != 1 {
                return Err(S::Error::custom("original routed variant is ambiguous"));
            }
            let (input, body) = variants.iter().next().unwrap();
            let tag = match (self.schema, input.as_str()) {
                ("operation", "content-reference-v1") => "file-history-pin-v1",
                ("operation", "content-reference") => "file-history-pin-v1",
                ("operation", "begin-content-copy") => "file-history-restore-begin-v1",
                ("operation", "metadata-query") => "file-history-query-v1",
                ("coordination", "metadata-query") => "file-history",
                ("operation", "reconcile-placement") => "reconcile-placement-v1",
                _ => input.as_str(),
            };
            let schema = if self.schema == "destination" && tag == "exact" {
                "precondition"
            } else if self.schema == "coordination" && tag == "advisory-lock" {
                "lock-outcome"
            } else if self.schema == "coordination" && tag == "file-sync" {
                "coord/file-sync"
            } else if self.schema == "coordination" && tag == "directory-sync" {
                "coord/directory-sync"
            } else if self.schema == "coordination" && tag == "object-family-adoption-verified" {
                "coord/object-family-adoption-verified"
            } else if self.schema == "coordination" && tag == "namespace-job" {
                "job"
            } else if self.schema == "coordination" && tag == "file-history" {
                "metadata-response-original"
            } else if self.schema == "coordination" && tag == "namespace-changes" {
                "change-poll-response"
            } else if self.schema == "coordination" && tag == "lookup-path-range" {
                "read-range"
            } else {
                tag
            };
            if original_fields(schema).is_none() && schema != "lock-outcome" {
                return Err(S::Error::custom(format!(
                    "original routed serializer requires source schema for {tag}"
                )));
            }
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry(
                source_wire_tag(self.generation, self.schema, tag),
                &GenerationRouted {
                    generation: self.generation,
                    value: body,
                    schema,
                },
            )?;
            return map.end();
        }
        let (schema, tag) =
            original_struct_schema(self.value, self.schema).map_err(S::Error::custom)?;
        // Native generation 8 serialized the required namespace first. Its
        // field order is part of the original canonical payload digest;
        // released reference requests keep their historical path-first order.
        let fields = if self.generation == SourceGeneration::CanonicalTags
            && schema == "content-reference-request"
        {
            &["namespace", "path", "identity", "operation", "operation_id"][..]
        } else {
            original_fields(&schema)
                .ok_or_else(|| S::Error::custom("unknown original routed struct schema"))?
        };
        let object = self
            .value
            .as_object()
            .ok_or_else(|| S::Error::custom("original routed struct is not an object"))?;
        let unsupported = object
            .keys()
            .filter(|key| {
                !fields.contains(&key.as_str())
                    && !fields
                        .iter()
                        .any(|field| original_alias(&schema, field) == Some(key.as_str()))
                    && tag.is_none_or(|(tag, _)| *key != tag)
            })
            .collect::<Vec<_>>();
        if !unsupported.is_empty() {
            return Err(S::Error::custom(format!(
                "original routed struct {schema} contains unsupported fields {unsupported:?}"
            )));
        }
        let null = Value::Null;
        let mut map = serializer.serialize_map(None)?;
        if let Some((key, tag)) = tag {
            map.serialize_entry(key, source_wire_tag(self.generation, self.schema, tag))?;
        }
        for key in fields {
            let alias = original_alias(&schema, key).and_then(|alias| object.get(alias));
            if object.contains_key(*key) && alias.is_some() {
                return Err(S::Error::custom("original struct repeats a source alias"));
            }
            let default = original_default(&schema, key);
            let value = match object.get(*key).or(alias) {
                None if default.is_some() => default.as_ref().unwrap(),
                Some(value) => value,
                None if original_optional(&schema, key) || original_omit(&schema, key, &null) => {
                    &null
                }
                None => {
                    return Err(S::Error::custom(format!(
                        "original routed struct omits {key}"
                    )));
                }
            };
            if original_omit(&schema, key, value) {
                continue;
            }
            if matches!(
                *key,
                "created_at"
                    | "updated_at"
                    | "completed_at"
                    | "terminal_at"
                    | "retained_at"
                    | "received_at"
                    | "detected_at"
                    | "journal_created_at"
            ) && !value.is_null()
            {
                let raw = value
                    .as_str()
                    .ok_or_else(|| S::Error::custom("original routed timestamp is not text"))?;
                let date = time::OffsetDateTime::parse(
                    raw,
                    &time::format_description::well_known::Rfc3339,
                )
                .map_err(S::Error::custom)?;
                let canonical = date
                    .format(&time::format_description::well_known::Rfc3339)
                    .map_err(S::Error::custom)?;
                map.serialize_entry(source_wire_field(self.generation, &schema, key), &canonical)?;
            } else {
                map.serialize_entry(
                    source_wire_field(self.generation, &schema, key),
                    &GenerationRouted {
                        generation: self.generation,
                        value,
                        schema: original_child(&schema, key),
                    },
                )?;
            }
        }
        map.end()
    }
}

#[cfg(test)]
mod routed_original_digest_tests {
    use super::*;
    use serde_json::json;
    use sinomo_core::object::{LengthMode, serde_ext};
    #[test]
    fn original_nested_reference_lock_and_recursive_job_order_survives_source_reopen() {
        let cases = [
            (
                "operation",
                json!({"file-history-pin-v1":{"request":{"path":"/source","identity":{"file_system_id":"old-fs","object_id":"old-object","version":"old-version"},"operation":{"operation":"pin","pin_id":"old-pin"},"operation_id":"reference-operation"}}}),
                r#"{"file-history-pin-v1":{"request":{"path":"/source","identity":{"file_system_id":"old-fs","object_id":"old-object","version":"old-version"},"operation":{"operation":"pin","pin_id":"old-pin"},"operation_id":"reference-operation"}}}"#,
            ),
            (
                "operation",
                json!({"advisory-lock":{"path":"/source","object_id":"old-object","command":{"type":"acquire","request_id":"request","session_id":"session","mode":"exclusive","blocking":false}}}),
                r#"{"advisory-lock":{"path":"/source","object_id":"old-object","command":{"type":"acquire","request_id":"request","session_id":"session","mode":"exclusive","blocking":false},"timeout_millis":45000}}"#,
            ),
            (
                "coordination",
                json!({"namespace-job":{"job_id":"job","file_system_id":"old-fs","kind":"hardlink-target-fence-across-shard-primary","status":"running","root_path":"/source","pending_frames":[{"type":"cross-shard-hardlink-target-fence-install","fence":{"job_id":"nested","file_system_id":"old-fs","kind":"clear-directory-recursive","status":"running","root_path":"/child","processed_object_count":0,"created_at":"1970-01-01T00:00:00Z","updated_at":"1970-01-01T00:00:00Z"},"source_path":"/source"}],"processed_object_count":0,"created_at":"1970-01-01T00:00:00Z","updated_at":"1970-01-01T00:00:00Z"}}),
                r#"{"namespace-job":{"job_id":"job","file_system_id":"old-fs","kind":"hardlink-target-fence-across-shard-primary","status":"running","root_path":"/source","pending_frames":[{"type":"cross-shard-hardlink-target-fence-install","fence":{"job_id":"nested","file_system_id":"old-fs","kind":"clear-directory-recursive","status":"running","root_path":"/child","processed_object_count":0,"created_at":"1970-01-01T00:00:00Z","updated_at":"1970-01-01T00:00:00Z"},"source_path":"/source"}],"processed_object_count":0,"created_at":"1970-01-01T00:00:00Z","updated_at":"1970-01-01T00:00:00Z"}}"#,
            ),
        ];
        for (schema, source, expected) in cases {
            let bytes = serde_ext::to_bytes(&source, 1, LengthMode::U32).unwrap();
            let reopened: Value = serde_ext::from_bytes(&bytes).unwrap();
            assert_eq!(
                serde_json::to_string(&OriginalRouted {
                    value: &reopened,
                    schema
                })
                .unwrap(),
                expected
            );
            let mut forged = reopened;
            let (_, body) = forged.as_object_mut().unwrap().iter_mut().next().unwrap();
            body["unsupported_source_field"] = json!(true);
            assert!(
                serde_json::to_string(&OriginalRouted {
                    value: &forged,
                    schema
                })
                .is_err()
            );
        }
    }
    fn body(completion: bool) -> Value {
        let fs = "00000000000000000000000001";
        let node = "00000000000000000000000002";
        let mut value = json!({"operation_id":"source-operation","file_system_id":fs,"shard_id":"root","owner_epoch":7,
            "operation_digest":if completion {"blake3:65555d22065cc4cd4df7c7e4311f62dfab7d0456302d8a40754e2df5dfebb87a"} else {"blake3:0433f83fa00fd539fef03072344ee7f4e49ff165f548f9af28870d28611861c7"},
            "transaction":{"transaction_id":"source-transaction","file_system_id":fs,"objects":[],"created_at":"1970-01-01T00:00:00Z"},
            "status":"prepared"});
        if completion {
            value["completed_request_completion"] = json!({"file_system_id":fs,"requester_node_id":node,"request_id":"source-request","operation_id":"source-routed-op",
                "authority_shard_id":"root","command_digest":"source-command-digest","transaction_id":"source-transaction","result":"applied","posix_coordination_result":null,
                "created_at":"1970-01-01T00:00:00Z","completed_at":"1970-01-01T00:00:01Z"});
        } else {
            value["completed_request"] = json!({"file_system_id":fs,"requester_node_id":node,"request_id":"source-request","operation_id":"source-routed-op",
                "authority_shard_id":"root","operation":{"mkdir-all":{"path":"/services/source"}},"command_digest":"source-command-digest","status":"completed","result":"applied",
                "posix_coordination_result":null,"recovery_attempts":0,"last_recovery_error":null,"created_at":"1970-01-01T00:00:00Z","updated_at":"1970-01-01T00:00:01Z"});
        }
        value
    }
    #[test]
    fn released_namespace_tuple_never_discards_non_null_request_or_completion() {
        for completion in [false, true] {
            let source = body(completion);
            let [nine, eight, seven] = original_namespace_digests(&source, None).unwrap();
            assert_eq!(source["operation_digest"], nine);
            let mut older = source.clone();
            older["operation_digest"] = json!(seven);
            assert!(verify_raw_namespace_prepare(&older, None).is_err());
            older["operation_digest"] = json!(eight);
            assert_eq!(
                verify_raw_namespace_prepare(&older, None).is_err(),
                completion
            );
            verify_raw_namespace_prepare(&source, None).unwrap();
        }
    }
    #[test]
    fn actual_original_routed_record_and_compact_completion_hash_before_conversion() {
        for completion in [false, true] {
            let source = body(completion);
            let bytes = serde_ext::to_bytes(&source, 1, LengthMode::U32).unwrap();
            let reopened: Value = serde_ext::from_bytes(&bytes).unwrap();
            let decoded = validate_original_namespace_prepare(reopened.clone(), None).unwrap();
            assert_eq!(decoded["operation_digest"], source["operation_digest"]);
            let mut changed = reopened;
            if completion {
                changed["completed_request_completion"]["completed_at"] =
                    json!("1970-01-01T00:00:02Z");
            } else {
                changed["completed_request"]["operation"]["mkdir-all"]["path"] =
                    json!("/services/changed");
            }
            assert!(validate_original_namespace_prepare(changed, None).is_err());
        }
    }
}

#[cfg(test)]
mod original_pin_digest_tests {
    use super::*;
    use serde_json::json;
    use sinomo_core::object::{LengthMode, serde_ext};
    #[test]
    fn original_implicit_pin_hashes_unqualified_bytes_before_current_namespace_conversion() {
        let raw = json!({"operation_id":"source-operation","file_system_id":"00000000000000000000000001","shard_id":"root","owner_epoch":7,
            "operation_digest":"blake3:c9e4dbee1790a2515f2cbf24e99aa49c20815ac8a3555219ea4b816b615002f1","status":"prepared",
            "transaction":{"transaction_id":"source-transaction","file_system_id":"00000000000000000000000001","objects":[],
                "file_version_pins":{"add":[{"file_system_id":"00000000000000000000000001","object_id":"00000000000000000000000003","version":"00000000000000000000000004",
                    "pin_id":"original-pin","created_at":"1970-01-01T00:00:00Z"}]},"created_at":"1970-01-01T00:00:00Z"}});
        let bytes = serde_ext::to_bytes(&raw, 1, LengthMode::U32).unwrap();
        let reopened: Value = serde_ext::from_bytes(&bytes).unwrap();
        let current = validate_original_namespace_prepare(reopened.clone(), None).unwrap();
        assert_eq!(
            current["transaction"]["file_version_pins"]["add"][0]["namespace"],
            DFS_HISTORY_METADATA_NAMESPACE
        );
        assert_eq!(
            current["transaction"]["file_version_pins"]["add"][0]["pin_id"],
            content_reference_storage_id(DFS_HISTORY_METADATA_NAMESPACE, "original-pin")
        );
        let mut changed = reopened;
        changed["transaction"]["file_version_pins"]["add"][0]["pin_id"] = json!("different-pin");
        assert!(validate_original_namespace_prepare(changed, None).is_err());
    }
}

#[cfg(test)]
mod declared_module_stream_tests {
    use super::*;
    #[test]
    fn original_sobj_transaction_streams_more_than_4096_declared_leaves() {
        // The released map validator allowed64 namespaces and256KiB PER map.
        //65 individually valid version maps already contain4160 declared leaves.
        let opaque = serde_json::json!([null,{"payload":{"operation_id":"unchanged opaque data"}}]);
        // The original normal producer encoded generic payloads as SOBJ Bytes.
        // JSON Value's heterogeneous-list serializer is not that wire contract.
        #[derive(Serialize)]
        struct OriginalModule<'a> {
            schema_version: u32,
            required: bool,
            #[serde(with = "serde_bytes")]
            payload: &'a [u8],
        }
        #[derive(Serialize)]
        struct OriginalVersion<'a> {
            module_metadata: std::collections::BTreeMap<String, OriginalModule<'a>>,
        }
        #[derive(Serialize)]
        struct OriginalTransaction<'a> {
            transaction_id: &'a str,
            versions: Vec<OriginalVersion<'a>>,
        }
        let encoded = serde_json::to_vec(&opaque).unwrap();
        assert!(encoded.len() <= 64 * 1024 && encoded.len() * 64 <= 256 * 1024);
        let original = OriginalTransaction {
            transaction_id: "declared-source-test",
            versions: (0..65)
                .map(|_| OriginalVersion {
                    module_metadata: (0..64)
                        .map(|n| {
                            (
                                format!("source.test{n:02}"),
                                OriginalModule {
                                    schema_version: 1,
                                    required: false,
                                    payload: &encoded,
                                },
                            )
                        })
                        .collect(),
                })
                .collect(),
        };
        let logical_original = serde_json::to_value(&original).unwrap();
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &original,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        assert!(bytes.len() < 64 * 1024 * 1024);
        let source = crate::metadata::source_document::SourceRawDocument::row(bytes.clone());
        let mut converted = source.decoded_value().unwrap();
        normalize_original_module_schema_at(
            &mut converted,
            "transaction",
            "",
            &source.context().unwrap(),
            "",
            None,
        )
        .unwrap();
        for version in converted["versions"].as_array().unwrap() {
            assert_eq!(version["module_metadata"].as_object().unwrap().len(), 64);
            for metadata in version["module_metadata"].as_object().unwrap().values() {
                let encoded: Vec<u8> = serde_json::from_value(metadata["payload"].clone()).unwrap();
                assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), opaque);
            }
        }
        assert_eq!(
            source.bytes, bytes,
            "source bytes remain untouched before/after adaptation"
        );
        assert_eq!(source.decoded_value().unwrap(), logical_original);
    }
    #[test]
    fn streamed_declared_visitor_stops_at_opaque_payload_and_maps_real_byte_leaf() {
        #[derive(Serialize)]
        struct OriginalModule<'a> {
            schema_version: u32,
            required: bool,
            #[serde(with = "serde_bytes")]
            payload: &'a [u8],
        }
        #[derive(Serialize)]
        struct OriginalVersion<'a> {
            module_metadata: std::collections::BTreeMap<&'a str, OriginalModule<'a>>,
        }
        let logical = serde_json::json!({"module_metadata":{"not.a.declared.record":{"payload":[null,{"x":1}]}},"object_id":"leave opaque identity"});
        let encoded = serde_json::to_vec(&logical).unwrap();
        let source_value = OriginalVersion {
            module_metadata: std::collections::BTreeMap::from([(
                "source.bytes",
                OriginalModule {
                    schema_version: 1,
                    required: false,
                    payload: &encoded,
                },
            )]),
        };
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &source_value,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let source = crate::metadata::source_document::SourceRawDocument::row(bytes.clone());
        let mut converted = source.decoded_value().unwrap();
        normalize_original_module_schema_at(
            &mut converted,
            "version",
            "",
            &source.context().unwrap(),
            "",
            None,
        )
        .unwrap();
        let adapted: Vec<u8> =
            serde_json::from_value(converted["module_metadata"]["source.bytes"]["payload"].clone())
                .unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&adapted).unwrap(), logical);
        assert_eq!(source.bytes, bytes);
    }
}
