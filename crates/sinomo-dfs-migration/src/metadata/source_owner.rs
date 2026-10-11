//! Source-only principal validation for an explicitly supplied standalone
//! factory. This reader does not grant that factory's authority.
use super::{Result, StandaloneSourceOwnerEvidence, error, source};
use serde::de::Error as _;
use serde_json::Value;
use sinomo_core::node::NodeId;
use sinomo_dfs::{DfsFileSystemId, DfsMetadataShardId};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

fn fs_id(value: &Value) -> Result<DfsFileSystemId> {
    let raw = value
        .as_str()
        .ok_or_else(|| error("source filesystem principal is not an identifier"))?;
    serde_json::from_value(value.clone())
        .or_else(|_| DfsFileSystemId::from_legacy_named(raw).map_err(serde_json::Error::custom))
        .map_err(error)
}
fn node_id(value: &Value) -> Result<NodeId> {
    let raw = value
        .as_str()
        .ok_or_else(|| error("source node principal is not an identifier"))?;
    serde_json::from_value(value.clone())
        .or_else(|_| NodeId::from_legacy_named(raw).map_err(serde_json::Error::custom))
        .map_err(error)
}
fn owner(row: &Value, owners: &mut BTreeMap<DfsFileSystemId, NodeId>) -> Result<()> {
    owners.insert(
        fs_id(
            row.get("file_system_id")
                .ok_or_else(|| error("source filesystem lacks its actual identity"))?,
        )?,
        node_id(
            row.get("owner_node_id")
                .ok_or_else(|| error("source filesystem lacks its actual owner"))?,
        )?,
    );
    Ok(())
}
fn filesystem(row: &Value) -> Option<&Value> {
    if row.get("type").and_then(Value::as_str) == Some("put-file-system") {
        row.get("file_system")
    } else {
        None
    }
}
fn validate_epochs(row: &Value) -> Result<()> {
    let mut bodies = vec![row];
    for key in [
        "intent",
        "record",
        "prepare",
        "payload",
        "digest",
        "packed_prepare_v1",
        "packed_prepare_v2",
    ] {
        if let Some(body) = row.get(key) {
            bodies.push(body);
            for nested in ["payload", "digest"] {
                if let Some(body) = body.get(nested) {
                    bodies.push(body);
                }
            }
        }
    }
    for body in bodies {
        if body
            .get("owner_epoch")
            .or_else(|| body.get("e"))
            .and_then(Value::as_u64)
            .is_some_and(|epoch| epoch != 0)
        {
            return Err(error(
                "explicit standalone source contains a nonzero authority epoch",
            ));
        }
    }
    Ok(())
}
pub(crate) fn validate_standalone_source_owner_evidence(
    root: &Path,
    evidence: &[StandaloneSourceOwnerEvidence],
    actual_node: &NodeId,
) -> Result<()> {
    if evidence.is_empty()
        || evidence
            .iter()
            .any(|scope| scope.owner_epoch != 0 || scope.owner_node_id != *actual_node)
    {
        return Err(error(
            "explicit standalone source evidence lacks its actual epoch-zero principal",
        ));
    }
    let expected = evidence
        .iter()
        .map(|scope| (scope.file_system_id.clone(), scope.shard_id.clone()))
        .collect::<BTreeSet<_>>();
    if expected.len() != evidence.len() {
        return Err(error("standalone source repeats an owner scope"));
    }
    let inventory = source::discover(root)?;
    let mut cursor = source::SourceCursor::new(inventory.clone());
    let mut owners = BTreeMap::new();
    let mut through = 0;
    let mut tail = BTreeMap::new();
    let mut declared = BTreeSet::new();
    while let Some(record) = source::next(root, &mut cursor)? {
        match record {
            source::SourceRecord::SnapshotHeader(header) => {
                if let Some(sequence) = header
                    .get("materialized_through_sequence")
                    .and_then(Value::as_u64)
                {
                    through = sequence;
                }
            }
            source::SourceRecord::SnapshotRow { table, row, .. }
                if matches!(table.as_str(), "file_systems" | "filesystems") =>
            {
                owner(&row, &mut owners)?
            }
            source::SourceRecord::LooseRow { family, row, .. } if family == "filesystems" => {
                owner(&row, &mut owners)?
            }
            source::SourceRecord::SnapshotRow { table, row, .. } if table == "$ops" => {
                validate_epochs(&row)?;
                if let Some(row) = filesystem(&row) {
                    owner(row, &mut owners)?;
                }
            }
            source::SourceRecord::TableBatch { batch, .. } => {
                let sequence = batch
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        error("source table ownership batch lacks its exact sequence")
                    })?;
                if sequence > through {
                    for op in batch
                        .get("ops")
                        .and_then(Value::as_array)
                        .ok_or_else(|| error("source table ownership batch lacks operations"))?
                    {
                        validate_epochs(op)?;
                        if let Some(row) = filesystem(op) {
                            owner(row, &mut owners)?;
                        }
                    }
                    through = sequence;
                }
            }
            source::SourceRecord::JournalFrame { entry, .. } => {
                let sequence = entry
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| error("source ownership frame lacks its exact sequence"))?;
                let mutation = entry
                    .get("mutation")
                    .ok_or_else(|| error("source ownership frame lacks its actual mutation"))?;
                if let Some(fs) = mutation
                    .get("file_system_id")
                    .or_else(|| filesystem(mutation).and_then(|row| row.get("file_system_id")))
                {
                    let shard: DfsMetadataShardId =
                        serde_json::from_value(entry.get("shard_id").cloned().ok_or_else(
                            || error("source ownership frame lacks its declared shard"),
                        )?)
                        .map_err(error)?;
                    declared.insert((fs_id(fs)?, shard));
                }
                if let Some(row) = filesystem(mutation) {
                    tail.insert(sequence, row.clone());
                }
                validate_epochs(mutation)?;
            }
            source::SourceRecord::SnapshotRow { table, row, .. }
                if matches!(
                    table.as_str(),
                    "intents" | "namespace_prepares" | "prepared_operations_v7"
                ) =>
            {
                validate_epochs(&row)?;
            }
            source::SourceRecord::LooseRow { family, row, .. }
                if matches!(
                    family.as_str(),
                    "intents" | "namespace-prepares" | "prepared-operations-v7"
                ) =>
            {
                validate_epochs(&row)?
            }
            _ => {}
        }
    }
    for (_, row) in tail.range((
        std::ops::Bound::Excluded(through),
        std::ops::Bound::Unbounded,
    )) {
        owner(row, &mut owners)?;
    }
    for scope in evidence {
        if owners.get(&scope.file_system_id) != Some(actual_node)
            || declared
                .iter()
                .filter(|(fs, _)| fs == &scope.file_system_id)
                .any(|pair| !expected.contains(pair))
        {
            return Err(error(
                "standalone import changes an actual source filesystem principal or encoded scope",
            ));
        }
    }
    if owners
        .keys()
        .any(|fs| !evidence.iter().any(|scope| &scope.file_system_id == fs))
    {
        return Err(error(
            "standalone import omits an actual source filesystem principal",
        ));
    }
    inventory.verify(root)
}
