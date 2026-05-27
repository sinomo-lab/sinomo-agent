//! Operator and agent-facing DFS inspection surfaces.

use crate::{
    DfsConflictRecord, DfsConflictStatus, DfsFileSystem, DfsMetadataBackend,
    DfsMetadataShardStatus, DfsObjectMetadata, DfsObjectVersionRecord, DfsReplicaReceiptRecord,
    DfsWriteIntentRecord, DfsWriteIntentStatus, MetadataBackendError,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_node::NodeId;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorStatus {
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub pending_write_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_shards: Vec<DfsMetadataShardStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorFileSystemView {
    pub file_system: DfsFileSystem,
    pub object_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorObjectView {
    pub object: DfsObjectMetadata,
    pub versions: Vec<DfsObjectVersionRecord>,
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    pub intents: Vec<DfsWriteIntentRecord>,
    pub conflicts: Vec<DfsConflictRecord>,
    pub repairs: Vec<DfsOperatorRepairView>,
    pub agent_state: DfsAgentObjectState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorRepairView {
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub missing_replica_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAgentFileSystemSummary {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub objects: Vec<DfsAgentObjectSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAgentObjectSummary {
    pub object_id: DfsObjectId,
    pub path: String,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub state: DfsAgentObjectState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsAgentObjectState {
    Current,
    PendingWrite,
    PendingRepair,
    Conflicted,
    MetadataOnly,
}

#[derive(Debug)]
pub struct DfsOperatorInspector<'a> {
    backend: &'a dyn DfsMetadataBackend,
}

impl<'a> DfsOperatorInspector<'a> {
    pub fn new(backend: &'a dyn DfsMetadataBackend) -> Self {
        Self { backend }
    }

    pub fn status(&self) -> Result<DfsOperatorStatus, MetadataBackendError> {
        let filesystems = self.backend.list_file_systems()?;
        let mut object_count = 0;
        let mut version_count = 0;
        let mut receipt_count = 0;
        let mut intent_count = 0;
        let mut pending_write_count = 0;
        let mut open_conflict_count = 0;
        let mut pending_repair_count = 0;

        for file_system in &filesystems {
            for object in self
                .backend
                .objects_for_file_system(&file_system.file_system_id)?
            {
                object_count += 1;
                let versions = self.backend.versions_for_object(&object.object_id)?;
                version_count += versions.len();
                let mut receipts_by_version = BTreeMap::new();
                for version in &versions {
                    let receipts = self
                        .backend
                        .receipts_for_version(&object.object_id, &version.version)?;
                    receipt_count += receipts.len();
                    receipts_by_version.insert(version.version.clone(), receipts);
                }
                let intents = self.backend.intents_for_object(&object.object_id)?;
                pending_write_count += intents
                    .iter()
                    .filter(|intent| write_intent_is_active(&intent.status))
                    .count();
                intent_count += intents.len();
                open_conflict_count += self
                    .backend
                    .conflicts_for_object(&object.object_id)?
                    .into_iter()
                    .filter(|conflict| conflict.status == DfsConflictStatus::Open)
                    .count();
                pending_repair_count +=
                    repairs_from_intents_and_receipts(&object, &intents, &receipts_by_version)
                        .len();
            }
        }

        Ok(DfsOperatorStatus {
            file_system_count: filesystems.len(),
            object_count,
            version_count,
            receipt_count,
            intent_count,
            pending_write_count,
            open_conflict_count,
            pending_repair_count,
            metadata_shards: self.backend.metadata_shard_statuses()?,
        })
    }

    pub fn file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsOperatorFileSystemView>, MetadataBackendError> {
        let Some(file_system) = self.backend.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let objects = self.backend.objects_for_file_system(file_system_id)?;
        let mut open_conflict_count = 0;
        let mut pending_repair_count = 0;
        for object in &objects {
            let intents = self.backend.intents_for_object(&object.object_id)?;
            open_conflict_count += self
                .backend
                .conflicts_for_object(&object.object_id)?
                .into_iter()
                .filter(|conflict| conflict.status == DfsConflictStatus::Open)
                .count();
            pending_repair_count += self.repairs_for_object(&object, &intents)?.len();
        }
        Ok(Some(DfsOperatorFileSystemView {
            file_system,
            object_count: objects.len(),
            open_conflict_count,
            pending_repair_count,
        }))
    }

    pub fn object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
        let Some(object) = self.backend.get_object(object_id)? else {
            return Ok(None);
        };
        let versions = self.backend.versions_for_object(object_id)?;
        let mut receipts = Vec::new();
        let mut receipts_by_version = BTreeMap::new();
        for version in &versions {
            let version_receipts = self
                .backend
                .receipts_for_version(object_id, &version.version)?;
            receipts_by_version.insert(version.version.clone(), version_receipts.clone());
            receipts.extend(version_receipts);
        }
        let intents = self.backend.intents_for_object(object_id)?;
        let conflicts = self.backend.conflicts_for_object(object_id)?;
        let repairs = repairs_from_intents_and_receipts(&object, &intents, &receipts_by_version);
        let agent_state = agent_state_for(&object, &intents, &conflicts, &repairs);
        Ok(Some(DfsOperatorObjectView {
            object,
            versions,
            receipts,
            intents,
            conflicts,
            repairs,
            agent_state,
        }))
    }

    pub fn object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
        let Some(object) = self.backend.get_object_by_path(file_system_id, path)? else {
            return Ok(None);
        };
        self.object(&object.object_id)
    }

    pub fn conflicts(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
    ) -> Result<Vec<DfsConflictRecord>, MetadataBackendError> {
        let objects = self.filtered_objects(file_system_id, object_id)?;
        let mut conflicts = Vec::new();
        for object in objects {
            conflicts.extend(self.backend.conflicts_for_object(&object.object_id)?);
        }
        conflicts.sort_by(|left, right| {
            left.object_id
                .cmp(&right.object_id)
                .then_with(|| left.conflict_id.cmp(&right.conflict_id))
        });
        Ok(conflicts)
    }

    pub fn repairs(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
    ) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
        let objects = self.filtered_objects(file_system_id, object_id)?;
        let mut repairs = Vec::new();
        for object in objects {
            let intents = self.backend.intents_for_object(&object.object_id)?;
            repairs.extend(self.repairs_for_object(&object, &intents)?);
        }
        repairs.sort_by(|left, right| {
            left.object_id
                .cmp(&right.object_id)
                .then_with(|| left.version.cmp(&right.version))
                .then_with(|| {
                    left.missing_replica_node_id
                        .cmp(&right.missing_replica_node_id)
                })
        });
        Ok(repairs)
    }

    pub fn agent_file_system_summary(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsAgentFileSystemSummary>, MetadataBackendError> {
        let Some(file_system) = self.backend.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let mut objects = Vec::new();
        for object in self.backend.objects_for_file_system(file_system_id)? {
            let intents = self.backend.intents_for_object(&object.object_id)?;
            let conflicts = self.backend.conflicts_for_object(&object.object_id)?;
            let repairs = self.repairs_for_object(&object, &intents)?;
            objects.push(DfsAgentObjectSummary {
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                state: agent_state_for(&object, &intents, &conflicts, &repairs),
            });
        }
        Ok(Some(DfsAgentFileSystemSummary {
            file_system_id: file_system.file_system_id,
            display_name: file_system.display_name,
            objects,
        }))
    }

    fn filtered_objects(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
        object_id: Option<&DfsObjectId>,
    ) -> Result<Vec<DfsObjectMetadata>, MetadataBackendError> {
        if let Some(object_id) = object_id {
            return Ok(self.backend.get_object(object_id)?.into_iter().collect());
        }

        let filesystems = match file_system_id {
            Some(file_system_id) => self
                .backend
                .get_file_system(file_system_id)?
                .into_iter()
                .collect::<Vec<_>>(),
            None => self.backend.list_file_systems()?,
        };
        let mut objects = Vec::new();
        for file_system in filesystems {
            objects.extend(
                self.backend
                    .objects_for_file_system(&file_system.file_system_id)?,
            );
        }
        Ok(objects)
    }

    fn repairs_for_object(
        &self,
        object: &DfsObjectMetadata,
        intents: &[DfsWriteIntentRecord],
    ) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
        let mut receipts_by_version = BTreeMap::<DfsVersion, Vec<DfsReplicaReceiptRecord>>::new();
        for intent in intents
            .iter()
            .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
        {
            if !receipts_by_version.contains_key(&intent.target_version) {
                receipts_by_version.insert(
                    intent.target_version.clone(),
                    self.backend
                        .receipts_for_version(&intent.object_id, &intent.target_version)?,
                );
            }
        }
        Ok(repairs_from_intents_and_receipts(
            object,
            intents,
            &receipts_by_version,
        ))
    }
}

fn repairs_from_intents_and_receipts(
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    receipts_by_version: &BTreeMap<DfsVersion, Vec<DfsReplicaReceiptRecord>>,
) -> Vec<DfsOperatorRepairView> {
    let mut repairs = Vec::new();
    for intent in intents {
        if intent.status != DfsWriteIntentStatus::Committed
            || intent.target_version != object.version
        {
            continue;
        }
        let receipt_nodes: BTreeSet<NodeId> = receipts_by_version
            .get(&intent.target_version)
            .into_iter()
            .flatten()
            .filter(|receipt| {
                intent
                    .content_hash
                    .as_ref()
                    .is_none_or(|hash| receipt.content_hash.as_ref() == Some(hash))
            })
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        for node_id in &intent.desired_replica_node_ids {
            if !receipt_nodes.contains(node_id) {
                repairs.push(DfsOperatorRepairView {
                    intent_id: intent.intent_id.clone(),
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    version: intent.target_version.clone(),
                    content_hash: intent.content_hash.clone(),
                    missing_replica_node_id: node_id.clone(),
                });
            }
        }
    }
    repairs
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPinRecord {
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<DfsObjectId>,
    pub pinned_by_node_id: NodeId,
    pub mode: DfsPinMode,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPinMode {
    Metadata,
    Content,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsPinAddRequest {
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub object_id: Option<DfsObjectId>,
    pub pinned_by_node_id: NodeId,
    pub mode: DfsPinMode,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct LocalDfsPinStore {
    root: PathBuf,
}

impl LocalDfsPinStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn put_pin(&self, record: &DfsPinRecord) -> Result<(), DfsPinStoreError> {
        write_object_atomic(&self.pin_path(record), record)
    }

    pub fn remove_pin(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        node_id: Option<&NodeId>,
    ) -> Result<bool, DfsPinStoreError> {
        let mut removed = false;
        for record in self.list_pins(Some(file_system_id))? {
            if normalize_path(&record.path) != normalize_path(path) {
                continue;
            }
            if node_id.is_some_and(|expected| &record.pinned_by_node_id != expected) {
                continue;
            }
            let path = self.pin_path(&record);
            if path.is_file() {
                fs::remove_file(&path).map_err(|source| DfsPinStoreError::Io { path, source })?;
                removed = true;
            }
        }
        Ok(removed)
    }

    pub fn list_pins(
        &self,
        file_system_id: Option<&DfsFileSystemId>,
    ) -> Result<Vec<DfsPinRecord>, DfsPinStoreError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for entry in read_dir(&self.root)? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("sobj") {
                continue;
            }
            let record: DfsPinRecord = read_object(&path)?;
            if file_system_id.is_none_or(|expected| &record.file_system_id == expected) {
                records.push(record);
            }
        }
        records.sort_by(|left, right| {
            left.file_system_id
                .cmp(&right.file_system_id)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.pinned_by_node_id.cmp(&right.pinned_by_node_id))
        });
        Ok(records)
    }

    pub fn add_pin(&self, request: DfsPinAddRequest) -> Result<DfsPinRecord, DfsPinStoreError> {
        let record = DfsPinRecord {
            file_system_id: request.file_system_id,
            path: request.path,
            object_id: request.object_id,
            pinned_by_node_id: request.pinned_by_node_id,
            mode: request.mode,
            created_at: request.created_at,
        };
        self.put_pin(&record)?;
        Ok(record)
    }

    fn pin_path(&self, record: &DfsPinRecord) -> PathBuf {
        self.root.join(format!(
            "{}-{}-{}.sobj",
            key_segment(&record.file_system_id),
            path_hash(&record.path),
            key_segment(&record.pinned_by_node_id)
        ))
    }
}

#[derive(Debug, Error)]
pub enum DfsPinStoreError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("pin serialization error: {0}")]
    Serde(String),
}

fn agent_state_for(
    object: &DfsObjectMetadata,
    intents: &[DfsWriteIntentRecord],
    conflicts: &[DfsConflictRecord],
    repairs: &[DfsOperatorRepairView],
) -> DfsAgentObjectState {
    if conflicts
        .iter()
        .any(|conflict| conflict.status == DfsConflictStatus::Open)
    {
        return DfsAgentObjectState::Conflicted;
    }
    if intents
        .iter()
        .any(|intent| write_intent_is_active(&intent.status))
    {
        return DfsAgentObjectState::PendingWrite;
    }
    if !repairs.is_empty() {
        return DfsAgentObjectState::PendingRepair;
    }
    if object.content_hash.is_some() {
        DfsAgentObjectState::Current
    } else {
        DfsAgentObjectState::MetadataOnly
    }
}

fn write_intent_is_active(status: &DfsWriteIntentStatus) -> bool {
    matches!(
        status,
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
    )
}

fn read_dir(path: &Path) -> Result<Vec<fs::DirEntry>, DfsPinStoreError> {
    let mut entries = Vec::new();
    let reader = fs::read_dir(path).map_err(|source| DfsPinStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    for entry in reader {
        entries.push(entry.map_err(|source| DfsPinStoreError::Io {
            path: path.to_owned(),
            source,
        })?);
    }
    Ok(entries)
}

fn read_object<T>(path: &Path) -> Result<T, DfsPinStoreError>
where
    T: DeserializeOwned,
{
    let bytes = fs::read(path).map_err(|source| DfsPinStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    sinomo_object_serde::from_bytes(&bytes)
        .map_err(|source| DfsPinStoreError::Serde(source.to_string()))
}

fn write_object_atomic<T>(path: &Path, value: &T) -> Result<(), DfsPinStoreError>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| DfsPinStoreError::Io {
            path: parent.to_owned(),
            source,
        })?;
    }
    let tmp_path = path.with_extension("tmp");
    let bytes = sinomo_object_serde::to_bytes(value, 1, LengthMode::U32)
        .map_err(|source| DfsPinStoreError::Serde(source.to_string()))?;
    fs::write(&tmp_path, bytes).map_err(|source| DfsPinStoreError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    fs::rename(&tmp_path, path).map_err(|source| DfsPinStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

fn normalize_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

fn path_hash(path: &str) -> String {
    blake3::hash(normalize_path(path).as_bytes())
        .to_hex()
        .to_string()
}

fn key_segment(value: &impl ToString) -> String {
    value
        .to_string()
        .trim_matches('/')
        .replace(['/', '\\', ':'], "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsObjectKind, DfsObjectMetadata, DfsReplicaReceiptRecord, DfsReplicationPolicy,
        DfsWriteIntentRecord, LocalJsonMetadataBackend,
    };
    use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
    use sinomo_events::EventId;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn operator_inspector_reports_status_object_conflicts_and_repairs() {
        let root = temp_root("operator_inspector_reports_status_object_conflicts_and_repairs");
        let mut backend =
            LocalJsonMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        let object = DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:new".to_string()),
            content_size_bytes: Some(1024),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_2"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: file_system_id(),
                object_id: object_id(),
                version: version("version_2"),
                content_hash: Some("blake3:new".to_string()),
                size_bytes: Some(3),
                locations: vec![node("node_owner")],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            })
            .expect("version");
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    committed_version: version("version_2"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named("evt_commit").expect("event"),
                },
                replica_node_id: node("node_owner"),
                content_hash: Some("blake3:new".to_string()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("receipt");
        backend
            .put_intent(&DfsWriteIntentRecord {
                intent_id: "dfs_intent_1".to_string(),
                file_system_id: file_system_id(),
                object_id: object_id(),
                base_version: Some(version("version_1")),
                target_version: version("version_2"),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
                policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                status: DfsWriteIntentStatus::Committed,
                content_hash: Some("blake3:new".to_string()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            })
            .expect("intent");
        backend
            .put_conflict(&DfsConflictRecord {
                conflict_id: "conflict_a".to_string(),
                file_system_id: file_system_id(),
                object_id: object_id(),
                path: "artifact.bin".to_string(),
                base_version: Some(version("version_1")),
                local_version: version("version_2_local"),
                remote_version: version("version_2"),
                detected_by_node_id: node("node_laptop"),
                reason: "offline edit".to_string(),
                local_content_hash: Some("blake3:local".to_string()),
                remote_content_hash: Some("blake3:new".to_string()),
                created_by_intent_id: None,
                strategy: Some("branch".to_string()),
                status: DfsConflictStatus::Open,
                detected_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("conflict");

        let inspector = DfsOperatorInspector::new(&backend);
        let status = inspector.status().expect("status");
        assert_eq!(status.file_system_count, 1);
        assert_eq!(status.object_count, 1);
        assert_eq!(status.open_conflict_count, 1);
        assert_eq!(status.pending_repair_count, 1);

        let object = inspector
            .object(&object_id())
            .expect("object")
            .expect("object");
        assert_eq!(object.agent_state, DfsAgentObjectState::Conflicted);
        assert_eq!(
            object.repairs[0].missing_replica_node_id,
            node("node_replica")
        );
        let path_object = inspector
            .object_by_path(&file_system_id(), "/artifact.bin")
            .expect("path object")
            .expect("path object");
        assert_eq!(path_object.object.object_id, object_id());

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_inspector_counts_staging_intents_as_pending_writes() {
        let root = temp_root("operator_inspector_counts_staging_intents_as_pending_writes");
        let mut backend =
            LocalJsonMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        let object = DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:old".to_string()),
            content_size_bytes: Some(512),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_1"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .put_intent(&DfsWriteIntentRecord {
                intent_id: "dfs_intent_staging".to_string(),
                file_system_id: file_system_id(),
                object_id: object_id(),
                base_version: Some(version("version_1")),
                target_version: version("version_2"),
                coordinator_node_id: node("node_owner"),
                owner_epoch: 1,
                desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
                policy: DfsReplicationPolicy::new(2, 2).expect("policy"),
                status: DfsWriteIntentStatus::Staging,
                content_hash: Some("blake3:new".to_string()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            })
            .expect("intent");

        let inspector = DfsOperatorInspector::new(&backend);
        let status = inspector.status().expect("status");
        assert_eq!(status.pending_write_count, 1);
        assert_eq!(
            inspector
                .object(&object_id())
                .expect("object")
                .expect("object")
                .agent_state,
            DfsAgentObjectState::PendingWrite
        );

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn operator_inspector_ignores_superseded_version_repairs() {
        let root = temp_root("operator_inspector_ignores_superseded_version_repairs");
        let mut backend =
            LocalJsonMetadataBackend::new(crate::DfsMetadataShardId::new("shard-root"), &root)
                .expect("backend");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            subtree_policies: Vec::new(),
        };
        let object = DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "artifact.bin".to_string(),
            kind: DfsObjectKind::Artifact,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner"), node("node_replica")],
            content_hash: Some("blake3:new".to_string()),
            content_size_bytes: Some(1024),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version("version_2"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        for (version_id, hash) in [
            (version("version_1"), "blake3:old"),
            (version("version_2"), "blake3:new"),
        ] {
            backend
                .put_version(&DfsObjectVersionRecord {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    version: version_id.clone(),
                    content_hash: Some(hash.to_string()),
                    size_bytes: Some(3),
                    locations: vec![node("node_owner"), node("node_replica")],
                    created_by_event_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    visible: true,
                })
                .expect("version");
            backend
                .put_receipt(&DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: file_system_id(),
                        object_id: object_id(),
                        committed_version: version_id,
                        owner_node_id: node("node_owner"),
                        event_id: EventId::from_legacy_named(format!("evt_{hash}")).expect("event"),
                    },
                    replica_node_id: node("node_owner"),
                    content_hash: Some(hash.to_string()),
                    received_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("owner receipt");
        }
        backend
            .put_receipt(&DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    committed_version: version("version_2"),
                    owner_node_id: node("node_owner"),
                    event_id: EventId::from_legacy_named("evt_current_replica").expect("event"),
                },
                replica_node_id: node("node_replica"),
                content_hash: Some("blake3:new".to_string()),
                received_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("replica receipt");
        for (intent_id, target_version, hash) in [
            ("dfs_intent_old", version("version_1"), "blake3:old"),
            ("dfs_intent_current", version("version_2"), "blake3:new"),
        ] {
            backend
                .put_intent(&DfsWriteIntentRecord {
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id(),
                    object_id: object_id(),
                    base_version: None,
                    target_version,
                    coordinator_node_id: node("node_owner"),
                    owner_epoch: 1,
                    desired_replica_node_ids: vec![node("node_owner"), node("node_replica")],
                    policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
                    status: DfsWriteIntentStatus::Committed,
                    content_hash: Some(hash.to_string()),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    updated_at: Some(OffsetDateTime::UNIX_EPOCH),
                })
                .expect("intent");
        }

        let inspector = DfsOperatorInspector::new(&backend);
        assert_eq!(
            inspector.status().expect("status").pending_repair_count,
            0,
            "a superseded version missing an old receipt must not keep the current object pending repair"
        );
        assert_eq!(
            inspector
                .object(&object_id())
                .expect("object")
                .expect("object")
                .agent_state,
            DfsAgentObjectState::Current
        );

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn local_pin_store_adds_lists_and_removes_records() {
        let root = temp_root("local_pin_store_adds_lists_and_removes_records");
        let store = LocalDfsPinStore::new(&root);
        let record = store
            .add_pin(DfsPinAddRequest {
                file_system_id: file_system_id(),
                path: "src/lib.rs".to_string(),
                object_id: Some(object_id()),
                pinned_by_node_id: node("node_laptop"),
                mode: DfsPinMode::Content,
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("add pin");

        assert_eq!(store.list_pins(None).expect("pins"), vec![record.clone()]);
        assert!(
            store
                .remove_pin(&file_system_id(), "src/lib.rs", Some(&node("node_laptop")))
                .expect("remove")
        );
        assert!(store.list_pins(None).expect("pins").is_empty());

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn local_pin_store_treats_backslash_as_filename_character() {
        let root = temp_root("local_pin_store_treats_backslash_as_filename_character");
        let store = LocalDfsPinStore::new(&root);
        let owner = node("node_laptop");
        let backslash = store
            .add_pin(DfsPinAddRequest {
                file_system_id: file_system_id(),
                path: r"src\a.txt".to_string(),
                object_id: Some(object_id()),
                pinned_by_node_id: owner.clone(),
                mode: DfsPinMode::Content,
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("add backslash pin");
        let slash = store
            .add_pin(DfsPinAddRequest {
                file_system_id: file_system_id(),
                path: "src/a.txt".to_string(),
                object_id: Some(object_id()),
                pinned_by_node_id: owner.clone(),
                mode: DfsPinMode::Content,
                created_at: OffsetDateTime::UNIX_EPOCH,
            })
            .expect("add slash pin");

        assert_eq!(
            store.list_pins(None).expect("pins"),
            vec![slash.clone(), backslash.clone()]
        );
        assert!(
            store
                .remove_pin(&file_system_id(), "src/a.txt", Some(&owner))
                .expect("remove slash")
        );
        assert_eq!(store.list_pins(None).expect("pins"), vec![backslash]);

        fs::remove_dir_all(&root).expect("cleanup");
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("filesystem")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_artifact").expect("object")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("version")
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node")
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-dfs-{name}-{nanos}"))
    }
}
