//! Local-first distributed filesystem metadata skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    DfsObjectId, DfsObjectPolicy, DfsVersion, DfsWriteReceipt, DfsWriteRequest, EventId, NodeId,
    WorkspaceId,
};
use std::collections::BTreeMap;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Workspace {
    pub workspace_id: WorkspaceId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<WorkspaceMount>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WorkspaceMount {
    pub path: String,
    pub kind: MountKind,
    pub policy: DfsObjectPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MountKind {
    LocalDirectory,
    GitCheckout,
    TaskStaging,
    ReplicatedNamespace,
    EvidenceBundle,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectMetadata {
    pub object_id: DfsObjectId,
    pub workspace_id: WorkspaceId,
    pub path: String,
    pub kind: DfsObjectKind,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub version: DfsVersion,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectKind {
    File,
    Directory,
    Symlink,
    Artifact,
    Mount,
    Tombstone,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteIntent {
    pub request: DfsWriteRequest,
    pub target_path: String,
    pub status: WriteIntentStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WriteIntentStatus {
    PendingOwner,
    Committed,
    Rejected,
    Conflict,
}

#[derive(Clone, Debug, Default)]
pub struct DfsMetadataStore {
    workspaces: BTreeMap<WorkspaceId, Workspace>,
    objects: BTreeMap<DfsObjectId, DfsObjectMetadata>,
}

impl DfsMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_workspace(&mut self, workspace: Workspace) {
        self.workspaces
            .insert(workspace.workspace_id.clone(), workspace);
    }

    pub fn add_object(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        if !self.workspaces.contains_key(&object.workspace_id) {
            return Err(DfsError::UnknownWorkspace {
                workspace_id: object.workspace_id,
            });
        }
        self.objects.insert(object.object_id.clone(), object);
        Ok(())
    }

    pub fn object(&self, object_id: &DfsObjectId) -> Option<&DfsObjectMetadata> {
        self.objects.get(object_id)
    }

    pub fn route_write(
        &self,
        request: DfsWriteRequest,
        requester_node_id: &NodeId,
    ) -> Result<WriteRoute, DfsError> {
        let object =
            self.objects
                .get(&request.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: request.object_id.clone(),
                })?;

        let action = match object.policy {
            DfsObjectPolicy::LocalOwned => {
                if &object.owner_node_id == requester_node_id {
                    WriteRouteAction::CommitLocally
                } else {
                    WriteRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            DfsObjectPolicy::ReplicatedConfirmed => WriteRouteAction::RequireReplicaReceipts,
            DfsObjectPolicy::ReplicatedEventual => WriteRouteAction::CommitThenReplicate,
            DfsObjectPolicy::AppendOnly => WriteRouteAction::AppendNewVersion,
            DfsObjectPolicy::EphemeralLocal => WriteRouteAction::CommitLocally,
        };

        Ok(WriteRoute {
            request,
            object_id: object.object_id.clone(),
            action,
        })
    }

    pub fn commit_write(
        &mut self,
        receipt: DfsWriteReceipt,
        content_hash: Option<String>,
    ) -> Result<(), DfsError> {
        let object =
            self.objects
                .get_mut(&receipt.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: receipt.object_id.clone(),
                })?;
        object.version = receipt.committed_version;
        object.last_modified_by_event_id = Some(receipt.event_id);
        object.content_hash = content_hash;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteRoute {
    pub request: DfsWriteRequest,
    pub object_id: DfsObjectId,
    pub action: WriteRouteAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WriteRouteAction {
    CommitLocally,
    RouteToOwner { owner_node_id: NodeId },
    RequireReplicaReceipts,
    CommitThenReplicate,
    AppendNewVersion,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DfsError {
    #[error("unknown workspace: {workspace_id}")]
    UnknownWorkspace { workspace_id: WorkspaceId },
    #[error("unknown object: {object_id}")]
    UnknownObject { object_id: DfsObjectId },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_owned_write_routes_to_owner() {
        let owner = NodeId::new("node_desktop").expect("node");
        let remote = NodeId::new("node_mobile").expect("node");
        let workspace_id = WorkspaceId::new("workspace_repo").expect("workspace");
        let object_id = DfsObjectId::new("obj_file").expect("object");
        let mut store = DfsMetadataStore::new();
        store.add_workspace(Workspace {
            workspace_id: workspace_id.clone(),
            display_name: "repo".to_string(),
            owner_node_id: owner.clone(),
            mounts: Vec::new(),
        });
        store
            .add_object(DfsObjectMetadata {
                object_id: object_id.clone(),
                workspace_id,
                path: "src/lib.rs".to_string(),
                kind: DfsObjectKind::File,
                owner_node_id: owner.clone(),
                current_locations: vec![owner.clone()],
                content_hash: None,
                version: DfsVersion::new("version_1").expect("version"),
                policy: DfsObjectPolicy::LocalOwned,
                created_by_event_id: None,
                last_modified_by_event_id: None,
            })
            .expect("add object");

        let route = store
            .route_write(
                DfsWriteRequest {
                    workspace_id: WorkspaceId::new("workspace_repo").expect("workspace"),
                    object_id,
                    target_version: DfsVersion::new("version_2").expect("version"),
                    requesting_node_id: remote.clone(),
                },
                &remote,
            )
            .expect("route write");

        assert_eq!(
            route.action,
            WriteRouteAction::RouteToOwner {
                owner_node_id: owner
            }
        );
    }
}
