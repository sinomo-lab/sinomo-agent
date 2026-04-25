//! Local-first distributed filesystem metadata skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    Confidence, DfsObjectId, DfsObjectPolicy, DfsVersion, DfsWriteReceipt, DfsWriteRequest,
    EventEnvelope, EventId, EventKind, LatencyClass, NodeId, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source, WorkspaceId,
};
use sinomo_network::{
    MeshMessage, MeshMessageBody, MeshMessageId, NetworkFrame, NetworkFrameId, NetworkTransport,
    ProtocolMessage,
};
use std::collections::BTreeMap;
use thiserror::Error;
use time::OffsetDateTime;

pub const DFS_PROTOCOL: &str = "sinomo.dfs.v1";

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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPolicyViolation {
    pub requesting_node_id: NodeId,
    pub executing_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<WorkspaceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<DfsObjectId>,
    pub reason: String,
}

impl DfsPolicyViolation {
    pub fn event(&self) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.executing_node_id.clone(),
            Source::new("dfs.policy").expect("valid source"),
            EventKind::new("dfs.policy.violation").expect("valid event kind"),
            Salience::new(0.9).expect("valid salience"),
            Confidence::new(0.96).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(self.executing_node_id.clone())
        .with_payload(serde_json::json!({
            "requesting_node_id": self.requesting_node_id,
            "executing_node_id": self.executing_node_id,
            "workspace_id": self.workspace_id,
            "object_id": self.object_id,
            "reason": self.reason,
        }))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadData {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadRequest {
    pub request_id: String,
    pub object_id: DfsObjectId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsReadResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsReadResult {
    Found(DfsReadData),
    NotFound,
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteCommand {
    pub request_id: String,
    pub request: DfsWriteRequest,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsWriteResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteResult {
    Committed {
        receipt: DfsWriteReceipt,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
    },
    NotFound,
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNetworkMessage {
    ReadRequest(DfsReadRequest),
    ReadResponse(DfsReadResponse),
    WriteRequest(DfsWriteCommand),
    WriteResponse(DfsWriteResponse),
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

    pub fn route_read(
        &self,
        object_id: &DfsObjectId,
        requester_node_id: &NodeId,
    ) -> Result<ReadRoute, DfsError> {
        let object = self
            .objects
            .get(object_id)
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;

        let action = match &object.policy {
            DfsObjectPolicy::LocalOwned => {
                if &object.owner_node_id == requester_node_id {
                    ReadRouteAction::ReadLocally
                } else {
                    ReadRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            _ => {
                return Err(DfsError::UnsupportedPolicy {
                    policy: object.policy.clone(),
                });
            }
        };

        Ok(ReadRoute {
            object_id: object.object_id.clone(),
            action,
        })
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

        let action = match &object.policy {
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
pub struct ReadRoute {
    pub object_id: DfsObjectId,
    pub action: ReadRouteAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ReadRouteAction {
    ReadLocally,
    RouteToOwner { owner_node_id: NodeId },
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsReadOutcome {
    Local {
        object_id: DfsObjectId,
        data: Option<DfsReadData>,
    },
    Routed {
        request_id: String,
        owner_node_id: NodeId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsWriteOutcome {
    Local {
        receipt: DfsWriteReceipt,
    },
    Routed {
        request_id: String,
        owner_node_id: NodeId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsServiceEvent {
    ReadRequestHandled {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsReadResult,
    },
    ReadResponseReceived {
        request_id: String,
        object_id: DfsObjectId,
        result: DfsReadResult,
    },
    WriteRequestHandled {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsWriteResult,
    },
    WriteResponseReceived {
        request_id: String,
        object_id: DfsObjectId,
        result: DfsWriteResult,
    },
}

#[derive(Clone, Debug)]
pub struct LocalOwnedDfsService<T> {
    node_id: NodeId,
    metadata: DfsMetadataStore,
    content: BTreeMap<DfsObjectId, Vec<u8>>,
    pending_reads: BTreeMap<String, DfsObjectId>,
    pending_writes: BTreeMap<String, DfsObjectId>,
    transport: T,
    next_frame_id: u64,
    next_message_id: u64,
    next_request_id: u64,
}

impl<T: NetworkTransport> LocalOwnedDfsService<T> {
    pub fn new(transport: T) -> Self {
        Self {
            node_id: transport.local_node_id().clone(),
            metadata: DfsMetadataStore::new(),
            content: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            transport,
            next_frame_id: 1,
            next_message_id: 1,
            next_request_id: 1,
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn metadata(&self) -> &DfsMetadataStore {
        &self.metadata
    }

    pub fn add_workspace(&mut self, workspace: Workspace) {
        self.metadata.add_workspace(workspace);
    }

    pub fn add_object_metadata(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        self.metadata.add_object(object)
    }

    pub fn put_local_object(
        &mut self,
        mut object: DfsObjectMetadata,
        bytes: Vec<u8>,
    ) -> Result<(), DfsError> {
        if object.owner_node_id != self.node_id {
            return Err(DfsError::NotObjectOwner {
                object_id: object.object_id,
                owner_node_id: object.owner_node_id,
                local_node_id: self.node_id.clone(),
            });
        }

        object.content_hash = Some(content_hash(&bytes));
        object.current_locations = vec![self.node_id.clone()];
        let object_id = object.object_id.clone();
        self.metadata.add_object(object)?;
        self.content.insert(object_id, bytes);
        Ok(())
    }

    pub fn read_object(&mut self, object_id: &DfsObjectId) -> Result<DfsReadOutcome, DfsError> {
        let route = self.metadata.route_read(object_id, &self.node_id)?;
        match route.action {
            ReadRouteAction::ReadLocally => Ok(DfsReadOutcome::Local {
                object_id: object_id.clone(),
                data: self.local_read(object_id)?,
            }),
            ReadRouteAction::RouteToOwner { owner_node_id } => {
                let request_id = self.next_request_id();
                self.pending_reads
                    .insert(request_id.clone(), object_id.clone());
                self.send_dfs_message(
                    &owner_node_id,
                    DfsNetworkMessage::ReadRequest(DfsReadRequest {
                        request_id: request_id.clone(),
                        object_id: object_id.clone(),
                    }),
                )?;
                Ok(DfsReadOutcome::Routed {
                    request_id,
                    owner_node_id,
                })
            }
        }
    }

    pub fn write_object(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
    ) -> Result<DfsWriteOutcome, DfsError> {
        let route = self.metadata.route_write(request.clone(), &self.node_id)?;
        match route.action {
            WriteRouteAction::CommitLocally => {
                let receipt = self.commit_local_write(request, bytes)?;
                Ok(DfsWriteOutcome::Local { receipt })
            }
            WriteRouteAction::RouteToOwner { owner_node_id } => {
                let request_id = self.next_request_id();
                self.pending_writes
                    .insert(request_id.clone(), request.object_id.clone());
                self.send_dfs_message(
                    &owner_node_id,
                    DfsNetworkMessage::WriteRequest(DfsWriteCommand {
                        request_id: request_id.clone(),
                        request,
                        bytes,
                    }),
                )?;
                Ok(DfsWriteOutcome::Routed {
                    request_id,
                    owner_node_id,
                })
            }
            _ => Err(DfsError::UnsupportedPolicy {
                policy: self
                    .metadata
                    .object(&route.object_id)
                    .map(|object| object.policy.clone())
                    .unwrap_or(DfsObjectPolicy::LocalOwned),
            }),
        }
    }

    pub fn process_incoming(&mut self) -> Result<Vec<DfsServiceEvent>, DfsError> {
        let inbound_frames = self
            .transport
            .drain_incoming()
            .map_err(DfsError::from_network)?;
        let mut events = Vec::new();

        for inbound in inbound_frames {
            let source_node_id = inbound.frame.source_node_id.clone();
            let MeshMessageBody::Protocol(protocol_message) = inbound.frame.message.body else {
                continue;
            };

            if protocol_message.protocol != DFS_PROTOCOL {
                continue;
            }

            let message: DfsNetworkMessage =
                serde_json::from_value(protocol_message.payload).map_err(DfsError::from_serde)?;

            match message {
                DfsNetworkMessage::ReadRequest(request) => {
                    let event = self.handle_read_request(source_node_id, request)?;
                    events.push(event);
                }
                DfsNetworkMessage::ReadResponse(response) => {
                    self.pending_reads.remove(&response.request_id);
                    events.push(DfsServiceEvent::ReadResponseReceived {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        result: response.result,
                    });
                }
                DfsNetworkMessage::WriteRequest(command) => {
                    let event = self.handle_write_request(source_node_id, command)?;
                    events.push(event);
                }
                DfsNetworkMessage::WriteResponse(response) => {
                    self.pending_writes.remove(&response.request_id);
                    if let DfsWriteResult::Committed {
                        receipt,
                        content_hash,
                    } = &response.result
                    {
                        self.metadata
                            .commit_write(receipt.clone(), content_hash.clone())?;
                    }
                    events.push(DfsServiceEvent::WriteResponseReceived {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        result: response.result,
                    });
                }
            }
        }

        Ok(events)
    }

    fn handle_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsReadRequest,
    ) -> Result<DfsServiceEvent, DfsError> {
        let result = match self.metadata.object(&request.object_id) {
            Some(object) if object.owner_node_id != self.node_id => DfsReadResult::Rejected {
                reason: format!(
                    "node {} does not own object {}",
                    self.node_id, request.object_id
                ),
            },
            Some(_) => self
                .local_read(&request.object_id)?
                .map(DfsReadResult::Found)
                .unwrap_or(DfsReadResult::NotFound),
            None => DfsReadResult::NotFound,
        };

        self.send_dfs_message(
            &requester_node_id,
            DfsNetworkMessage::ReadResponse(DfsReadResponse {
                request_id: request.request_id.clone(),
                object_id: request.object_id.clone(),
                result: result.clone(),
            }),
        )?;

        Ok(DfsServiceEvent::ReadRequestHandled {
            request_id: request.request_id,
            object_id: request.object_id,
            requester_node_id,
            result,
        })
    }

    fn handle_write_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsWriteCommand,
    ) -> Result<DfsServiceEvent, DfsError> {
        let object_id = command.request.object_id.clone();
        let result = match self.commit_local_write(command.request, command.bytes) {
            Ok(receipt) => DfsWriteResult::Committed {
                content_hash: self
                    .metadata
                    .object(&object_id)
                    .and_then(|object| object.content_hash.clone()),
                receipt,
            },
            Err(DfsError::UnknownObject { .. }) => DfsWriteResult::NotFound,
            Err(error) => DfsWriteResult::Rejected {
                reason: error.to_string(),
            },
        };

        self.send_dfs_message(
            &requester_node_id,
            DfsNetworkMessage::WriteResponse(DfsWriteResponse {
                request_id: command.request_id.clone(),
                object_id: object_id.clone(),
                result: result.clone(),
            }),
        )?;

        Ok(DfsServiceEvent::WriteRequestHandled {
            request_id: command.request_id,
            object_id,
            requester_node_id,
            result,
        })
    }

    fn local_read(&self, object_id: &DfsObjectId) -> Result<Option<DfsReadData>, DfsError> {
        let object = self
            .metadata
            .object(object_id)
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;

        Ok(self.content.get(object_id).map(|bytes| DfsReadData {
            object_id: object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            bytes: bytes.clone(),
        }))
    }

    fn commit_local_write(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
    ) -> Result<DfsWriteReceipt, DfsError> {
        let object =
            self.metadata
                .object(&request.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: request.object_id.clone(),
                })?;

        if object.policy != DfsObjectPolicy::LocalOwned {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }

        if object.owner_node_id != self.node_id {
            return Err(DfsError::NotObjectOwner {
                object_id: object.object_id.clone(),
                owner_node_id: object.owner_node_id.clone(),
                local_node_id: self.node_id.clone(),
            });
        }

        let content_hash = Some(content_hash(&bytes));
        let receipt = DfsWriteReceipt {
            workspace_id: request.workspace_id,
            object_id: request.object_id.clone(),
            committed_version: request.target_version,
            owner_node_id: self.node_id.clone(),
            event_id: EventId::generate(),
        };
        self.content.insert(request.object_id, bytes);
        self.metadata
            .commit_write(receipt.clone(), content_hash.clone())?;
        Ok(receipt)
    }

    fn send_dfs_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<(), DfsError> {
        let payload = serde_json::to_value(message).map_err(DfsError::from_serde)?;
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: self.node_id.clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: self.node_id.clone(),
                target_node_id: target_node_id.clone(),
                body: MeshMessageBody::Protocol(ProtocolMessage::new(DFS_PROTOCOL, payload)),
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;

        self.transport
            .send(target_node_id, frame)
            .map_err(DfsError::from_network)
    }

    fn next_request_id(&mut self) -> String {
        let request_id = format!("dfs_req_{}", self.next_request_id);
        self.next_request_id += 1;
        request_id
    }
}

fn content_hash(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DfsError {
    #[error("unknown workspace: {workspace_id}")]
    UnknownWorkspace { workspace_id: WorkspaceId },
    #[error("unknown object: {object_id}")]
    UnknownObject { object_id: DfsObjectId },
    #[error("unsupported DFS policy for local-owned service: {policy:?}")]
    UnsupportedPolicy { policy: DfsObjectPolicy },
    #[error("node {local_node_id} cannot own object {object_id}; owner is {owner_node_id}")]
    NotObjectOwner {
        object_id: DfsObjectId,
        owner_node_id: NodeId,
        local_node_id: NodeId,
    },
    #[error("network error: {message}")]
    Network { message: String },
    #[error("serialization error: {message}")]
    Serialization { message: String },
}

impl DfsError {
    fn from_network(error: sinomo_network::NetworkError) -> Self {
        Self::Network {
            message: error.to_string(),
        }
    }

    fn from_serde(error: serde_json::Error) -> Self {
        Self::Serialization {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_network::InProcessNetwork;

    fn workspace(owner: NodeId) -> Workspace {
        Workspace {
            workspace_id: WorkspaceId::new("workspace_repo").expect("workspace"),
            display_name: "repo".to_string(),
            owner_node_id: owner,
            mounts: Vec::new(),
        }
    }

    fn file_metadata(owner: NodeId) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::new("obj_file").expect("object"),
            workspace_id: WorkspaceId::new("workspace_repo").expect("workspace"),
            path: "src/lib.rs".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: owner.clone(),
            current_locations: vec![owner],
            content_hash: None,
            version: DfsVersion::new("version_1").expect("version"),
            policy: DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    #[test]
    fn local_owned_write_routes_to_owner() {
        let owner = NodeId::new("node_desktop").expect("node");
        let remote = NodeId::new("node_mobile").expect("node");
        let workspace_id = WorkspaceId::new("workspace_repo").expect("workspace");
        let object_id = DfsObjectId::new("obj_file").expect("object");
        let mut store = DfsMetadataStore::new();
        store.add_workspace(workspace(owner.clone()));
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

    #[test]
    fn local_owned_read_is_routed_to_owner_node() {
        let owner = NodeId::new("node_desktop").expect("node");
        let remote = NodeId::new("node_mobile").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_dfs = LocalOwnedDfsService::new(network.add_node(owner.clone()));
        let mut remote_dfs = LocalOwnedDfsService::new(network.add_node(remote.clone()));
        let metadata = file_metadata(owner.clone());
        let object_id = metadata.object_id.clone();

        owner_dfs.add_workspace(workspace(owner.clone()));
        remote_dfs.add_workspace(workspace(owner.clone()));
        owner_dfs
            .put_local_object(metadata.clone(), b"owned bytes".to_vec())
            .expect("seed owner object");
        remote_dfs
            .add_object_metadata(metadata)
            .expect("replicate metadata");

        let read = remote_dfs.read_object(&object_id).expect("read routes");
        let request_id = match read {
            DfsReadOutcome::Routed {
                request_id,
                owner_node_id,
            } => {
                assert_eq!(owner_node_id, owner);
                request_id
            }
            DfsReadOutcome::Local { .. } => panic!("remote read should route to owner"),
        };

        let owner_events = owner_dfs.process_incoming().expect("owner handles read");
        assert!(matches!(
            &owner_events[..],
            [DfsServiceEvent::ReadRequestHandled {
                result: DfsReadResult::Found(_),
                ..
            }]
        ));

        let remote_events = remote_dfs
            .process_incoming()
            .expect("remote receives read response");
        assert!(matches!(
            &remote_events[..],
            [DfsServiceEvent::ReadResponseReceived {
                request_id: received_request_id,
                result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                ..
            }] if received_request_id == &request_id && bytes == b"owned bytes"
        ));
    }

    #[test]
    fn local_owned_write_is_routed_to_owner_node_and_committed() {
        let owner = NodeId::new("node_desktop").expect("node");
        let remote = NodeId::new("node_mobile").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_dfs = LocalOwnedDfsService::new(network.add_node(owner.clone()));
        let mut remote_dfs = LocalOwnedDfsService::new(network.add_node(remote.clone()));
        let metadata = file_metadata(owner.clone());
        let object_id = metadata.object_id.clone();
        let workspace_id = metadata.workspace_id.clone();

        owner_dfs.add_workspace(workspace(owner.clone()));
        remote_dfs.add_workspace(workspace(owner.clone()));
        owner_dfs
            .put_local_object(metadata.clone(), b"old bytes".to_vec())
            .expect("seed owner object");
        remote_dfs
            .add_object_metadata(metadata)
            .expect("replicate metadata");

        let target_version = DfsVersion::new("version_2").expect("version");
        let write = remote_dfs
            .write_object(
                DfsWriteRequest {
                    workspace_id,
                    object_id: object_id.clone(),
                    target_version: target_version.clone(),
                    requesting_node_id: remote.clone(),
                },
                b"updated bytes".to_vec(),
            )
            .expect("write routes");
        let request_id = match write {
            DfsWriteOutcome::Routed {
                request_id,
                owner_node_id,
            } => {
                assert_eq!(owner_node_id, owner);
                request_id
            }
            DfsWriteOutcome::Local { .. } => panic!("remote write should route to owner"),
        };

        let owner_events = owner_dfs.process_incoming().expect("owner handles write");
        assert!(matches!(
            &owner_events[..],
            [DfsServiceEvent::WriteRequestHandled {
                result: DfsWriteResult::Committed { .. },
                ..
            }]
        ));

        let owner_read = owner_dfs
            .read_object(&object_id)
            .expect("owner reads local");
        assert!(matches!(
            owner_read,
            DfsReadOutcome::Local {
                data: Some(DfsReadData { bytes, version, .. }),
                ..
            } if bytes == b"updated bytes" && version == target_version
        ));

        let remote_events = remote_dfs
            .process_incoming()
            .expect("remote receives write response");
        assert!(matches!(
            &remote_events[..],
            [DfsServiceEvent::WriteResponseReceived {
                request_id: received_request_id,
                result: DfsWriteResult::Committed { receipt, .. },
                ..
            }] if received_request_id == &request_id
                && receipt.committed_version == target_version
                && receipt.owner_node_id == owner
        ));
        assert_eq!(
            remote_dfs
                .metadata()
                .object(&object_id)
                .expect("metadata updated")
                .version,
            target_version
        );
    }

    #[test]
    fn dfs_policy_violation_event_is_structured_for_supervision() {
        let owner = NodeId::new("node_desktop").expect("node");
        let remote = NodeId::new("node_mobile").expect("node");
        let event = DfsPolicyViolation {
            requesting_node_id: remote,
            executing_node_id: owner.clone(),
            workspace_id: Some(WorkspaceId::new("workspace_repo").expect("workspace")),
            object_id: Some(DfsObjectId::new("obj_file").expect("object")),
            reason: "write attempted against non-owner".to_string(),
        }
        .event();

        assert_eq!(event.kind.as_str(), "dfs.policy.violation");
        assert_eq!(event.target_node_id, Some(owner));
        assert_eq!(event.payload["workspace_id"], "workspace_repo");
    }
}
