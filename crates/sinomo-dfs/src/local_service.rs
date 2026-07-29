//! In-memory local-owned DFS service used by lightweight local tests and adapters.
//!
//! This module owns the simple local-owned request/response service. The durable
//! cluster-aware service remains in the root networked service implementation.

use std::collections::BTreeMap;

use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest};

use crate::{
    DfsError, DfsFileSystem, DfsMessageTransport, DfsMetadataStore, DfsNetworkMessage, DfsObjectId,
    DfsObjectMetadata, DfsReadData, DfsReadOutcome, DfsReadRangeData, DfsReadRequest,
    DfsReadResponse, DfsReadResult, DfsRoutedCall, DfsServiceEvent, DfsWriteCommand,
    DfsWriteOutcome, DfsWriteResponse, DfsWriteResult, ReadRouteAction, WriteRouteAction,
    build_dfs_routed_message, content_hash,
};

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

impl<T: DfsMessageTransport> LocalOwnedDfsService<T> {
    pub fn new(transport: T) -> Self {
        let node_id = transport.local_node_id().clone();
        let mut metadata = DfsMetadataStore::new();
        metadata.add_file_system(DfsFileSystem::sifs(node_id.clone()));
        Self {
            node_id,
            metadata,
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

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) {
        self.metadata.add_file_system(file_system);
    }

    pub fn add_object_metadata(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        self.metadata.add_object(object)
    }

    pub fn add_object_metadata_batch(
        &mut self,
        objects: impl IntoIterator<Item = DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        for object in objects {
            self.metadata.add_object(object)?;
        }
        Ok(())
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
        object.content_size_bytes = Some(bytes.len() as u64);
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
                        version: None,
                        content_hash: None,
                        content_size_bytes: None,
                        range_offset: None,
                        range_max_bytes: None,
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
        if self
            .transport
            .pending_incoming_count()
            .map_err(DfsError::from_network)?
            == Some(0)
        {
            return Ok(Vec::new());
        }
        let inbound_messages = self
            .transport
            .drain_incoming_messages()
            .map_err(DfsError::from_network)?;
        let mut events = Vec::new();

        for inbound in inbound_messages {
            let source_node_id = inbound.source_node_id;
            match inbound.message {
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
                DfsNetworkMessage::BulkReadRequest(_) | DfsNetworkMessage::BulkReadResponse(_) => {}
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
                DfsNetworkMessage::StageBlob(_)
                | DfsNetworkMessage::StagePatch(_)
                | DfsNetworkMessage::StageBlobResponse(_)
                | DfsNetworkMessage::CommitVersion(_)
                | DfsNetworkMessage::CommitVersionResponse(_)
                | DfsNetworkMessage::RepairReplica(_)
                | DfsNetworkMessage::RepairReplicaResponse(_)
                | DfsNetworkMessage::FetchBlob(_)
                | DfsNetworkMessage::FetchBlobResponse(_)
                | DfsNetworkMessage::DistributedWriteRequest(_)
                | DfsNetworkMessage::ExclusiveCreateRequest(_)
                | DfsNetworkMessage::DistributedWriteResponse(_)
                | DfsNetworkMessage::NamespaceRequest(_)
                | DfsNetworkMessage::NamespaceResponse(_)
                | DfsNetworkMessage::MetadataJournalBatch(_)
                | DfsNetworkMessage::MetadataJournalBatchResponse(_)
                | DfsNetworkMessage::MetadataSnapshotInstall(_)
                | DfsNetworkMessage::MetadataSnapshotInstallResponse(_)
                | DfsNetworkMessage::ContentRootCatalogPageRequest(_)
                | DfsNetworkMessage::ContentRootCatalogPageResponse(_) => {}
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
            Some(_) => match (request.range_offset, request.range_max_bytes) {
                (Some(offset), Some(max_bytes)) => self
                    .local_read(&request.object_id)?
                    .map(|data| {
                        let total_size_bytes = data.bytes.len() as u64;
                        let start = usize::try_from(offset)
                            .ok()
                            .map(|offset| offset.min(data.bytes.len()))
                            .unwrap_or(data.bytes.len());
                        let end = start.saturating_add(max_bytes).min(data.bytes.len());
                        DfsReadResult::RangeFound(DfsReadRangeData {
                            object_id: data.object_id,
                            version: data.version,
                            content_hash: data.content_hash,
                            offset: offset.min(total_size_bytes),
                            total_size_bytes,
                            bytes: data.bytes[start..end].to_vec(),
                        })
                    })
                    .unwrap_or(DfsReadResult::NotFound),
                (None, None) => self
                    .local_read(&request.object_id)?
                    .map(DfsReadResult::Found)
                    .unwrap_or(DfsReadResult::NotFound),
                _ => DfsReadResult::Rejected {
                    reason: "DFS read range request must include both offset and max bytes"
                        .to_string(),
                },
            },
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
            file_system_id: request.file_system_id,
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
        let routed_message = build_dfs_routed_message(
            &self.node_id,
            target_node_id,
            self.next_frame_id,
            self.next_message_id,
            message,
        );
        self.next_frame_id += 1;
        self.next_message_id += 1;

        self.transport
            .send_routed_call(target_node_id, DfsRoutedCall::value(routed_message))
            .map_err(DfsError::from_network)
    }

    fn next_request_id(&mut self) -> String {
        let request_id = format!("dfs_req_{}", self.next_request_id);
        self.next_request_id += 1;
        request_id
    }
}
