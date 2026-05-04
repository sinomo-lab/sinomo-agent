//! KV-backed DFS metadata records and key helpers.

use crate::{DfsObjectMetadata, Workspace};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::{
    DfsObjectId, DfsVersion, DfsWriteReceipt, DfsWriteRequest, EventId, NodeId, WorkspaceId,
};
use sinomo_kv::{InProcessKvStore, KvCompare, KvError, KvOp, KvTxn};
use time::OffsetDateTime;

const DFS_PREFIX: &str = "/dfs";

pub fn workspace_key(workspace_id: &WorkspaceId) -> String {
    format!("{DFS_PREFIX}/workspaces/{workspace_id}")
}

pub fn workspace_objects_prefix(workspace_id: &WorkspaceId) -> String {
    format!("{}/objects", workspace_key(workspace_id))
}

pub fn path_key(workspace_id: &WorkspaceId, path: &str) -> String {
    let path = normalize_path(path);
    if path.is_empty() {
        format!("{}/paths", workspace_key(workspace_id))
    } else {
        format!("{}/paths/{path}", workspace_key(workspace_id))
    }
}

pub fn object_key(workspace_id: &WorkspaceId, object_id: &DfsObjectId) -> String {
    format!("{}/objects/{object_id}", workspace_key(workspace_id))
}

pub fn object_versions_prefix(workspace_id: &WorkspaceId, object_id: &DfsObjectId) -> String {
    format!("{}/versions", object_key(workspace_id, object_id))
}

pub fn version_key(
    workspace_id: &WorkspaceId,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> String {
    format!("{}/versions/{version}", object_key(workspace_id, object_id))
}

pub fn intent_key(workspace_id: &WorkspaceId, intent_id: &str) -> String {
    format!(
        "{}/intents/{}",
        workspace_key(workspace_id),
        normalize_segment(intent_id)
    )
}

pub fn receipt_key(
    workspace_id: &WorkspaceId,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    replica_node_id: &NodeId,
) -> String {
    format!(
        "{}/receipts/{replica_node_id}",
        version_key(workspace_id, object_id, version)
    )
}

pub fn replication_job_key(workspace_id: &WorkspaceId, job_id: &str) -> String {
    format!(
        "{}/replication-jobs/{}",
        workspace_key(workspace_id),
        normalize_segment(job_id)
    )
}

pub fn conflict_key(workspace_id: &WorkspaceId, conflict_id: &str) -> String {
    format!(
        "{}/conflicts/{}",
        workspace_key(workspace_id),
        normalize_segment(conflict_id)
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsVersionRecord {
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub visible: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentRecord {
    pub intent_id: String,
    pub request: DfsWriteRequest,
    pub target_path: String,
    pub status: DfsWriteIntentStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_replica_node_ids: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteIntentStatus {
    Pending,
    ReceiptsPending,
    Committed,
    Rejected,
    Conflict,
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
pub struct DfsReplicationJobRecord {
    pub job_id: String,
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub status: DfsReplicationJobStatus,
    pub attempt_count: u32,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsReplicationJobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictRecord {
    pub conflict_id: String,
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<DfsVersion>,
    pub local_version: DfsVersion,
    pub remote_version: DfsVersion,
    pub detected_by_node_id: NodeId,
    pub reason: String,
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

pub struct KvDfsMetadataStore<'a> {
    kv: &'a mut InProcessKvStore,
}

impl<'a> KvDfsMetadataStore<'a> {
    pub fn new(kv: &'a mut InProcessKvStore) -> Self {
        Self { kv }
    }

    pub fn put_workspace(&mut self, workspace: &Workspace) -> Result<(), KvError> {
        self.put_json(&workspace_key(&workspace.workspace_id), workspace)
    }

    pub fn get_workspace(&self, workspace_id: &WorkspaceId) -> Result<Option<Workspace>, KvError> {
        self.get_json(&workspace_key(workspace_id))
    }

    pub fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<(), KvError> {
        self.put_json(&object_key(&object.workspace_id, &object.object_id), object)?;
        self.put_json(
            &path_key(&object.workspace_id, &object.path),
            &object.object_id,
        )
    }

    pub fn get_object(
        &self,
        workspace_id: &WorkspaceId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>, KvError> {
        self.get_json(&object_key(workspace_id, object_id))
    }

    pub fn put_version(&mut self, version: &DfsVersionRecord) -> Result<(), KvError> {
        self.put_json(
            &version_key(&version.workspace_id, &version.object_id, &version.version),
            version,
        )
    }

    pub fn get_version(
        &self,
        workspace_id: &WorkspaceId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsVersionRecord>, KvError> {
        self.get_json(&version_key(workspace_id, object_id, version))
    }

    pub fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<(), KvError> {
        self.put_json(
            &intent_key(&intent.request.workspace_id, &intent.intent_id),
            intent,
        )
    }

    pub fn get_intent(
        &self,
        workspace_id: &WorkspaceId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>, KvError> {
        self.get_json(&intent_key(workspace_id, intent_id))
    }

    pub fn put_receipt(&mut self, receipt: &DfsReplicaReceiptRecord) -> Result<(), KvError> {
        self.put_json(
            &receipt_key(
                &receipt.receipt.workspace_id,
                &receipt.receipt.object_id,
                &receipt.receipt.committed_version,
                &receipt.replica_node_id,
            ),
            receipt,
        )
    }

    pub fn get_receipt(
        &self,
        workspace_id: &WorkspaceId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>, KvError> {
        self.get_json(&receipt_key(
            workspace_id,
            object_id,
            version,
            replica_node_id,
        ))
    }

    pub fn commit_visible_version(
        &mut self,
        expected_object: &DfsObjectMetadata,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsVersionRecord,
        required_receipt_keys: &[String],
    ) -> Result<bool, KvError> {
        let object_key = object_key(&expected_object.workspace_id, &expected_object.object_id);
        let expected_object_value = to_json_vec(expected_object)?;
        let committed_object_value = to_json_vec(committed_object)?;
        let visible_version_value = to_json_vec(visible_version)?;

        let mut compares = Vec::with_capacity(required_receipt_keys.len() + 1);
        compares.push(KvCompare::ValueEquals {
            key: object_key.clone(),
            value: expected_object_value,
        });
        compares.extend(
            required_receipt_keys
                .iter()
                .cloned()
                .map(|key| KvCompare::Exists { key }),
        );

        let response = self.kv.txn(KvTxn {
            compares,
            success: vec![
                KvOp::Put {
                    key: object_key,
                    value: committed_object_value,
                    lease_id: None,
                },
                KvOp::Put {
                    key: version_key(
                        &visible_version.workspace_id,
                        &visible_version.object_id,
                        &visible_version.version,
                    ),
                    value: visible_version_value,
                    lease_id: None,
                },
            ],
            failure: Vec::new(),
        })?;

        Ok(response.succeeded)
    }

    fn put_json<T>(&mut self, key: &str, value: &T) -> Result<(), KvError>
    where
        T: Serialize,
    {
        self.kv.put(key, to_json_vec(value)?, None)?;
        Ok(())
    }

    fn get_json<T>(&self, key: &str) -> Result<Option<T>, KvError>
    where
        T: DeserializeOwned,
    {
        let Some(entry) = self.kv.get(key) else {
            return Ok(None);
        };
        serde_json::from_slice(&entry.value)
            .map(Some)
            .map_err(|source| KvError::Serialization {
                detail: source.to_string(),
            })
    }
}

fn to_json_vec<T>(value: &T) -> Result<Vec<u8>, KvError>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })
}

fn normalize_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

fn normalize_segment(segment: &str) -> String {
    segment.trim_matches('/').replace('/', "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsObjectKind, WorkspaceMount};
    use sinomo_core::DfsObjectPolicy;

    fn node(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn workspace_id() -> WorkspaceId {
        WorkspaceId::new("workspace_repo").expect("valid workspace id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::new("obj_readme").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::new(value).expect("valid dfs version")
    }

    fn workspace() -> Workspace {
        Workspace {
            workspace_id: workspace_id(),
            display_name: "repo".to_string(),
            owner_node_id: node("node_owner"),
            mounts: Vec::<WorkspaceMount>::new(),
        }
    }

    fn object(version: DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            workspace_id: workspace_id(),
            path: "docs/readme.md".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("hash-old".to_string()),
            version,
            policy: DfsObjectPolicy::ReplicatedConfirmed,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    #[test]
    fn key_helpers_build_slash_delimited_keys() {
        let workspace_id = workspace_id();
        let object_id = object_id();
        let version = version("version_2");
        let replica = node("node_replica");

        assert_eq!(
            workspace_key(&workspace_id),
            "/dfs/workspaces/workspace_repo"
        );
        assert_eq!(
            path_key(&workspace_id, "/docs/readme.md"),
            "/dfs/workspaces/workspace_repo/paths/docs/readme.md"
        );
        assert_eq!(
            object_key(&workspace_id, &object_id),
            "/dfs/workspaces/workspace_repo/objects/obj_readme"
        );
        assert_eq!(
            version_key(&workspace_id, &object_id, &version),
            "/dfs/workspaces/workspace_repo/objects/obj_readme/versions/version_2"
        );
        assert_eq!(
            receipt_key(&workspace_id, &object_id, &version, &replica),
            "/dfs/workspaces/workspace_repo/objects/obj_readme/versions/version_2/receipts/node_replica"
        );
        assert_eq!(
            replication_job_key(&workspace_id, "job/1"),
            "/dfs/workspaces/workspace_repo/replication-jobs/job_1"
        );
        assert_eq!(
            conflict_key(&workspace_id, "conflict/1"),
            "/dfs/workspaces/workspace_repo/conflicts/conflict_1"
        );
    }

    #[test]
    fn store_puts_and_gets_json_records() {
        let mut kv = InProcessKvStore::new();
        let mut store = KvDfsMetadataStore::new(&mut kv);
        let workspace = workspace();
        let object = object(version("version_1"));
        let version_record = DfsVersionRecord {
            workspace_id: workspace.workspace_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };

        store.put_workspace(&workspace).expect("put workspace");
        store.put_object(&object).expect("put object");
        store.put_version(&version_record).expect("put version");

        assert_eq!(
            store
                .get_workspace(&workspace.workspace_id)
                .expect("get workspace"),
            Some(workspace)
        );
        assert_eq!(
            store
                .get_object(&object.workspace_id, &object.object_id)
                .expect("get object"),
            Some(object)
        );
        assert_eq!(
            store
                .get_version(
                    &version_record.workspace_id,
                    &version_record.object_id,
                    &version_record.version
                )
                .expect("get version"),
            Some(version_record)
        );
    }

    #[test]
    fn commit_visible_version_requires_expected_object_and_receipts() {
        let mut kv = InProcessKvStore::new();
        let mut store = KvDfsMetadataStore::new(&mut kv);
        let initial_object = object(version("version_1"));
        let mut committed_object = initial_object.clone();
        committed_object.version = version("version_2");
        committed_object.content_hash = Some("hash-new".to_string());
        let visible_version = DfsVersionRecord {
            workspace_id: committed_object.workspace_id.clone(),
            object_id: committed_object.object_id.clone(),
            version: committed_object.version.clone(),
            content_hash: committed_object.content_hash.clone(),
            locations: vec![node("node_owner"), node("node_replica")],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                workspace_id: committed_object.workspace_id.clone(),
                object_id: committed_object.object_id.clone(),
                committed_version: committed_object.version.clone(),
                owner_node_id: node("node_owner"),
                event_id: EventId::new("evt_commit").expect("valid event id"),
            },
            replica_node_id: node("node_replica"),
            content_hash: committed_object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let required_receipt_key = receipt_key(
            &receipt.receipt.workspace_id,
            &receipt.receipt.object_id,
            &receipt.receipt.committed_version,
            &receipt.replica_node_id,
        );

        store
            .put_object(&initial_object)
            .expect("put initial object");
        assert!(
            !store
                .commit_visible_version(
                    &initial_object,
                    &committed_object,
                    &visible_version,
                    std::slice::from_ref(&required_receipt_key)
                )
                .expect("cas without receipt")
        );

        store.put_receipt(&receipt).expect("put receipt");
        assert!(
            store
                .commit_visible_version(
                    &initial_object,
                    &committed_object,
                    &visible_version,
                    std::slice::from_ref(&required_receipt_key)
                )
                .expect("cas with receipt")
        );
        assert_eq!(
            store
                .get_object(&committed_object.workspace_id, &committed_object.object_id)
                .expect("get committed object"),
            Some(committed_object.clone())
        );
        assert_eq!(
            store
                .get_version(
                    &visible_version.workspace_id,
                    &visible_version.object_id,
                    &visible_version.version
                )
                .expect("get visible version"),
            Some(visible_version.clone())
        );
        let mut stale_version = visible_version;
        stale_version.version = version("version_3");
        assert!(
            !store
                .commit_visible_version(
                    &initial_object,
                    &committed_object,
                    &stale_version,
                    &[required_receipt_key]
                )
                .expect("stale cas")
        );
    }
}
