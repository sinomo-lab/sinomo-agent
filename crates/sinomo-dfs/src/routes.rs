//! DFS object-RPC route constants and typed route/message conversion helpers.

use crate::{
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsFileSystemId, DfsMessageCommand,
    DfsMessageResponse, DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsMetadataSnapshotInstallCommand,
    DfsMetadataSnapshotInstallResponse, DfsNamespaceCommand, DfsNamespaceOperation,
    DfsNetworkMessage, DfsOperatorStatus, DfsRoutedMessage, DfsSmokeReport,
};
use sinomo_core::network::{NetworkError, ObjectRpcCall, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_core::node::NodeId;

pub const DFS_RPC_CATEGORY: &str = "dfs";
pub const DFS_STATUS_ROUTE: &str = "/dfs/status";
pub const DFS_INSPECTION_ROUTE: &str = "/dfs/inspection";
pub const DFS_SMOKE_ROUTE: &str = "/dfs/smoke";
pub const DFS_METADATA_JOURNAL_BATCH_ROUTE: &str = "/dfs/metadata/journal-batch";
pub const DFS_METADATA_SNAPSHOT_INSTALL_ROUTE: &str = "/dfs/metadata/snapshot-install";
pub const DFS_DIRECT_READ_ROUTE: &str = "/dfs/object/read";
pub const DFS_DATA_READ_ROUTE: &str = "/dfs/data/read";
pub const DFS_DATA_READ_RESPONSE_ROUTE: &str = "/dfs/data/read/response";
pub const DFS_DATA_BULK_READ_ROUTE: &str = "/dfs/data/bulk-read";
pub const DFS_DATA_BULK_READ_RESPONSE_ROUTE: &str = "/dfs/data/bulk-read/response";
pub const DFS_DATA_WRITE_ROUTE: &str = "/dfs/data/write";
pub const DFS_DATA_WRITE_RESPONSE_ROUTE: &str = "/dfs/data/write/response";
pub const DFS_DATA_DISTRIBUTED_WRITE_ROUTE: &str = "/dfs/data/distributed-write";
pub const DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE: &str = "/dfs/data/distributed-write/response";
pub const DFS_DATA_NAMESPACE_ROUTE: &str = "/dfs/data/namespace";
pub const DFS_DATA_NAMESPACE_RESPONSE_ROUTE: &str = "/dfs/data/namespace/response";
pub const DFS_DATA_STAGE_BLOB_ROUTE: &str = "/dfs/data/stage-blob";
pub const DFS_DATA_STAGE_PATCH_ROUTE: &str = "/dfs/data/stage-patch";
pub const DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE: &str = "/dfs/data/stage-blob/response";
pub const DFS_DATA_COMMIT_VERSION_ROUTE: &str = "/dfs/data/commit-version";
pub const DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE: &str = "/dfs/data/commit-version/response";
pub const DFS_DATA_REPAIR_REPLICA_ROUTE: &str = "/dfs/data/repair-replica";
pub const DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE: &str = "/dfs/data/repair-replica/response";
pub const DFS_DATA_FETCH_BLOB_ROUTE: &str = "/dfs/data/fetch-blob";
pub const DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE: &str = "/dfs/data/fetch-blob/response";
pub const DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE: &str = "/dfs/data/metadata/journal-batch";
pub const DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE: &str =
    "/dfs/data/metadata/journal-batch/response";
pub const DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE: &str = "/dfs/data/metadata/snapshot-install";
pub const DFS_DATA_METADATA_SNAPSHOT_INSTALL_RESPONSE_ROUTE: &str =
    "/dfs/data/metadata/snapshot-install/response";
pub const DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE: &str =
    "/dfs/data/metadata/content-root-catalog/page";
pub const DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_RESPONSE_ROUTE: &str =
    "/dfs/data/metadata/content-root-catalog/page/response";
pub const DFS_MESSAGE_DATA_PLANE_ROUTES: &[&str] = &[
    DFS_DATA_READ_ROUTE,
    DFS_DATA_READ_RESPONSE_ROUTE,
    DFS_DATA_BULK_READ_ROUTE,
    DFS_DATA_BULK_READ_RESPONSE_ROUTE,
    DFS_DATA_WRITE_ROUTE,
    DFS_DATA_WRITE_RESPONSE_ROUTE,
    DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
    DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE,
    DFS_DATA_NAMESPACE_ROUTE,
    DFS_DATA_NAMESPACE_RESPONSE_ROUTE,
    DFS_DATA_STAGE_BLOB_ROUTE,
    DFS_DATA_STAGE_PATCH_ROUTE,
    DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE,
    DFS_DATA_COMMIT_VERSION_ROUTE,
    DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE,
    DFS_DATA_REPAIR_REPLICA_ROUTE,
    DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE,
    DFS_DATA_FETCH_BLOB_ROUTE,
    DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE,
    DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE,
    DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE,
    DFS_DATA_METADATA_SNAPSHOT_INSTALL_RESPONSE_ROUTE,
    DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE,
    DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_RESPONSE_ROUTE,
];
pub const DFS_CONTROL_PLANE_ROUTES: &[&str] = &[
    DFS_STATUS_ROUTE,
    DFS_INSPECTION_ROUTE,
    DFS_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
];
/// Canonical DFS data-plane object-RPC routes.
pub const DFS_DATA_PLANE_ROUTES: &[&str] = &[
    DFS_SMOKE_ROUTE,
    DFS_DIRECT_READ_ROUTE,
    DFS_DATA_READ_ROUTE,
    DFS_DATA_READ_RESPONSE_ROUTE,
    DFS_DATA_BULK_READ_ROUTE,
    DFS_DATA_BULK_READ_RESPONSE_ROUTE,
    DFS_DATA_WRITE_ROUTE,
    DFS_DATA_WRITE_RESPONSE_ROUTE,
    DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
    DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE,
    DFS_DATA_NAMESPACE_ROUTE,
    DFS_DATA_NAMESPACE_RESPONSE_ROUTE,
    DFS_DATA_STAGE_BLOB_ROUTE,
    DFS_DATA_STAGE_PATCH_ROUTE,
    DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE,
    DFS_DATA_COMMIT_VERSION_ROUTE,
    DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE,
    DFS_DATA_REPAIR_REPLICA_ROUTE,
    DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE,
    DFS_DATA_FETCH_BLOB_ROUTE,
    DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE,
    DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE,
    DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE,
    DFS_DATA_METADATA_SNAPSHOT_INSTALL_RESPONSE_ROUTE,
    DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE,
    DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_RESPONSE_ROUTE,
];
/// Routes registered by the DFS data-plane service.
pub const DFS_DATA_PLANE_SERVICE_ROUTES: &[&str] = DFS_DATA_PLANE_ROUTES;
pub const DFS_OBJECT_RPC_ROUTES: &[&str] = &[
    DFS_STATUS_ROUTE,
    DFS_INSPECTION_ROUTE,
    DFS_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
    DFS_SMOKE_ROUTE,
    DFS_DIRECT_READ_ROUTE,
    DFS_DATA_READ_ROUTE,
    DFS_DATA_READ_RESPONSE_ROUTE,
    DFS_DATA_BULK_READ_ROUTE,
    DFS_DATA_BULK_READ_RESPONSE_ROUTE,
    DFS_DATA_WRITE_ROUTE,
    DFS_DATA_WRITE_RESPONSE_ROUTE,
    DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
    DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE,
    DFS_DATA_NAMESPACE_ROUTE,
    DFS_DATA_NAMESPACE_RESPONSE_ROUTE,
    DFS_DATA_STAGE_BLOB_ROUTE,
    DFS_DATA_STAGE_PATCH_ROUTE,
    DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE,
    DFS_DATA_COMMIT_VERSION_ROUTE,
    DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE,
    DFS_DATA_REPAIR_REPLICA_ROUTE,
    DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE,
    DFS_DATA_FETCH_BLOB_ROUTE,
    DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE,
    DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE,
    DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE,
    DFS_DATA_METADATA_SNAPSHOT_INSTALL_RESPONSE_ROUTE,
    DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE,
    DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_RESPONSE_ROUTE,
];

pub(crate) fn build_dfs_routed_message(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    frame_id: u64,
    message_id: u64,
    message: DfsNetworkMessage,
) -> DfsRoutedMessage {
    DfsRoutedMessage {
        source_node_id: source_node_id.clone(),
        target_node_id: target_node_id.clone(),
        frame_id,
        message_id,
        message,
    }
}

fn dfs_message_command_from_routed_message(message: DfsRoutedMessage) -> DfsMessageCommand {
    DfsMessageCommand {
        request_id: format!("dfs-message-{}-{}", message.frame_id, message.message_id),
        routed_message: message,
    }
}

pub(crate) fn dfs_routed_message_from_object_rpc_request(
    request: &ObjectRpcRequest,
) -> Result<DfsRoutedMessage, NetworkError> {
    Ok(decode_dfs_message_rpc_request(request)?.routed_message)
}

pub fn decode_dfs_message_rpc_request(
    request: &ObjectRpcRequest,
) -> Result<DfsMessageCommand, NetworkError> {
    if !dfs_route_is_message_data_plane(&request.route) {
        return Err(NetworkError::Serialization {
            detail: format!(
                "DFS message RPC requires a typed DFS data-plane route, got {}",
                request.route
            ),
        });
    }
    let command: DfsMessageCommand = request.decode_body()?;
    let expected_route = dfs_message_route_for_network_message(&command.routed_message.message);
    if request.route != expected_route {
        return Err(NetworkError::Serialization {
            detail: format!(
                "DFS data-plane route {} does not match message route {}",
                request.route, expected_route
            ),
        });
    }
    Ok(command)
}

pub fn dfs_route_is_message_data_plane(route: &str) -> bool {
    DFS_MESSAGE_DATA_PLANE_ROUTES.contains(&route)
}

fn dfs_message_route(command: &DfsMessageCommand) -> &'static str {
    dfs_message_route_for_network_message(&command.routed_message.message)
}

fn dfs_message_route_for_network_message(message: &DfsNetworkMessage) -> &'static str {
    match message {
        DfsNetworkMessage::ReadRequest(_) => DFS_DATA_READ_ROUTE,
        DfsNetworkMessage::ReadResponse(_) => DFS_DATA_READ_RESPONSE_ROUTE,
        DfsNetworkMessage::BulkReadRequest(_) => DFS_DATA_BULK_READ_ROUTE,
        DfsNetworkMessage::BulkReadResponse(_) => DFS_DATA_BULK_READ_RESPONSE_ROUTE,
        DfsNetworkMessage::WriteRequest(_) => DFS_DATA_WRITE_ROUTE,
        DfsNetworkMessage::WriteResponse(_) => DFS_DATA_WRITE_RESPONSE_ROUTE,
        DfsNetworkMessage::DistributedWriteRequest(_)
        | DfsNetworkMessage::ExclusiveCreateRequest(_) => DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
        DfsNetworkMessage::DistributedWriteResponse(_) => DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE,
        DfsNetworkMessage::NamespaceRequest(_) => DFS_DATA_NAMESPACE_ROUTE,
        DfsNetworkMessage::NamespaceResponse(_) => DFS_DATA_NAMESPACE_RESPONSE_ROUTE,
        DfsNetworkMessage::StageBlob(_) => DFS_DATA_STAGE_BLOB_ROUTE,
        DfsNetworkMessage::StagePatch(_) => DFS_DATA_STAGE_PATCH_ROUTE,
        DfsNetworkMessage::StageBlobResponse(_) => DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE,
        DfsNetworkMessage::CommitVersion(_) => DFS_DATA_COMMIT_VERSION_ROUTE,
        DfsNetworkMessage::CommitVersionResponse(_) => DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE,
        DfsNetworkMessage::RepairReplica(_) => DFS_DATA_REPAIR_REPLICA_ROUTE,
        DfsNetworkMessage::RepairReplicaResponse(_) => DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE,
        DfsNetworkMessage::FetchBlob(_) => DFS_DATA_FETCH_BLOB_ROUTE,
        DfsNetworkMessage::FetchBlobResponse(_) => DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE,
        DfsNetworkMessage::MetadataJournalBatch(_) => DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
        DfsNetworkMessage::MetadataJournalBatchResponse(_) => {
            DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE
        }
        DfsNetworkMessage::MetadataSnapshotInstall(_) => DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE,
        DfsNetworkMessage::MetadataSnapshotInstallResponse(_) => {
            DFS_DATA_METADATA_SNAPSHOT_INSTALL_RESPONSE_ROUTE
        }
        DfsNetworkMessage::ContentRootCatalogPageRequest(_) => {
            DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE
        }
        DfsNetworkMessage::ContentRootCatalogPageResponse(_) => {
            DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_RESPONSE_ROUTE
        }
    }
}

/// Conservative concurrency identity for one decoded DFS request.
///
/// This is admission metadata only; it is never serialized on the DFS wire.
/// A command is scoped more narrowly than [`GlobalExclusive`](Self::GlobalExclusive)
/// only when the request itself carries enough stable identity to prove that
/// narrower scope without consulting mutable daemon state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsMessageExecutionScope {
    /// The request does not mutate DFS state and may execute alongside other
    /// admitted requests.
    ReadOnly,
    /// The request carries an exact filesystem and metadata-shard identity.
    ExactShard {
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
    },
    /// Candidate path footprint for a namespace request. A scheduler may
    /// resolve and acquire the involved shards atomically, or conservatively
    /// route this scope through its global compatibility lane.
    NamespacePaths {
        file_system_id: DfsFileSystemId,
        paths: Vec<String>,
    },
    /// The request is a response, a legacy write, or otherwise lacks a stable
    /// concurrency identity and must retain the serial compatibility path.
    GlobalExclusive,
}

fn exact_shard_execution_scope(
    file_system_id: Option<&DfsFileSystemId>,
    shard_id: &DfsMetadataShardId,
) -> DfsMessageExecutionScope {
    let Some(file_system_id) = file_system_id else {
        return DfsMessageExecutionScope::GlobalExclusive;
    };
    if shard_id.0.trim().is_empty() {
        return DfsMessageExecutionScope::GlobalExclusive;
    }
    DfsMessageExecutionScope::ExactShard {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.clone(),
    }
}

fn authority_shard_execution_scope(
    authority_shard: Option<&DfsMetadataShardConfig>,
    expected_file_system_id: Option<&DfsFileSystemId>,
) -> DfsMessageExecutionScope {
    let Some(authority_shard) = authority_shard else {
        return DfsMessageExecutionScope::GlobalExclusive;
    };
    if expected_file_system_id.is_some_and(|expected| expected != &authority_shard.file_system_id)
        || authority_shard.shard_id.trim().is_empty()
    {
        return DfsMessageExecutionScope::GlobalExclusive;
    }
    DfsMessageExecutionScope::ExactShard {
        file_system_id: authority_shard.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(authority_shard.shard_id.clone()),
    }
}

/// Classify a direct or routed metadata journal command using its explicit
/// filesystem and batch-shard identity. Rolling-upgrade commands that omit the
/// filesystem remain on the conservative global path.
pub fn dfs_metadata_journal_batch_execution_scope(
    command: &DfsMetadataJournalBatchCommand,
) -> DfsMessageExecutionScope {
    exact_shard_execution_scope(command.file_system_id.as_ref(), &command.batch.shard_id)
}

/// Classify a direct or routed metadata snapshot install using its explicit
/// filesystem and shard identity.
pub fn dfs_metadata_snapshot_install_execution_scope(
    command: &DfsMetadataSnapshotInstallCommand,
) -> DfsMessageExecutionScope {
    exact_shard_execution_scope(command.file_system_id.as_ref(), &command.shard_id)
}

fn dfs_network_message_execution_scope(message: &DfsNetworkMessage) -> DfsMessageExecutionScope {
    match message {
        DfsNetworkMessage::ReadRequest(_)
        | DfsNetworkMessage::BulkReadRequest(_)
        | DfsNetworkMessage::ContentRootCatalogPageRequest(_) => DfsMessageExecutionScope::ReadOnly,
        DfsNetworkMessage::NamespaceRequest(command)
            if matches!(
                command.operation,
                DfsNamespaceOperation::LookupPath { .. }
                    | DfsNamespaceOperation::LookupPaths { .. }
            ) =>
        {
            DfsMessageExecutionScope::ReadOnly
        }
        DfsNetworkMessage::NamespaceRequest(command) => {
            if command.authority_shards.len() == 1 {
                let stamped = &command.authority_shards[0];
                if stamped.file_system_id == command.file_system_id
                    && !stamped.shard_id.trim().is_empty()
                {
                    return DfsMessageExecutionScope::ExactShard {
                        file_system_id: command.file_system_id.clone(),
                        shard_id: DfsMetadataShardId::new(stamped.shard_id.clone()),
                    };
                }
            }
            let paths = command
                .operation
                .involved_paths()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            if paths.is_empty() || paths.iter().any(|path| path.trim().is_empty()) {
                DfsMessageExecutionScope::GlobalExclusive
            } else {
                DfsMessageExecutionScope::NamespacePaths {
                    file_system_id: command.file_system_id.clone(),
                    paths,
                }
            }
        }
        DfsNetworkMessage::DistributedWriteRequest(command)
        | DfsNetworkMessage::ExclusiveCreateRequest(command) => authority_shard_execution_scope(
            command.authority_shard.as_ref(),
            Some(&command.request.file_system_id),
        ),
        // Replica-transfer authority stamps are authorization evidence, not a
        // receiver-validated current routing identity. Keep them on the global
        // compatibility lane so a stale or forged stamp cannot split one
        // object's mutations across different scheduler lanes. Routed writes
        // and namespaces above perform a refreshed full-config comparison
        // before mutation and are therefore safe exact-lane inputs.
        DfsNetworkMessage::StageBlob(_)
        | DfsNetworkMessage::StagePatch(_)
        | DfsNetworkMessage::CommitVersion(_)
        | DfsNetworkMessage::RepairReplica(_)
        | DfsNetworkMessage::FetchBlob(_) => DfsMessageExecutionScope::GlobalExclusive,
        DfsNetworkMessage::MetadataJournalBatch(command) => {
            dfs_metadata_journal_batch_execution_scope(command)
        }
        DfsNetworkMessage::MetadataSnapshotInstall(command) => {
            dfs_metadata_snapshot_install_execution_scope(command)
        }
        DfsNetworkMessage::ReadResponse(_)
        | DfsNetworkMessage::BulkReadResponse(_)
        | DfsNetworkMessage::WriteRequest(_)
        | DfsNetworkMessage::WriteResponse(_)
        | DfsNetworkMessage::DistributedWriteResponse(_)
        | DfsNetworkMessage::NamespaceResponse(_)
        | DfsNetworkMessage::StageBlobResponse(_)
        | DfsNetworkMessage::CommitVersionResponse(_)
        | DfsNetworkMessage::RepairReplicaResponse(_)
        | DfsNetworkMessage::FetchBlobResponse(_)
        | DfsNetworkMessage::MetadataJournalBatchResponse(_)
        | DfsNetworkMessage::MetadataSnapshotInstallResponse(_)
        | DfsNetworkMessage::ContentRootCatalogPageResponse(_) => {
            DfsMessageExecutionScope::GlobalExclusive
        }
    }
}

/// Return the conservative execution scope used to admit one typed DFS
/// message RPC into the daemon's concurrent scheduler.
pub fn dfs_message_execution_scope(command: &DfsMessageCommand) -> DfsMessageExecutionScope {
    dfs_network_message_execution_scope(&command.routed_message.message)
}

fn dfs_network_message_is_read_only_request(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::ReadRequest(_)
            | DfsNetworkMessage::BulkReadRequest(_)
            | DfsNetworkMessage::ContentRootCatalogPageRequest(_)
            | DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                operation: DfsNamespaceOperation::LookupPath { .. }
                    | DfsNamespaceOperation::LookupPaths { .. },
                ..
            })
    )
}

pub fn dfs_message_is_read_only_request(command: &DfsMessageCommand) -> bool {
    dfs_network_message_is_read_only_request(&command.routed_message.message)
}

pub(crate) fn dfs_network_message_response_retry_safe(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::DistributedWriteRequest(_)
            | DfsNetworkMessage::DistributedWriteResponse(_)
            | DfsNetworkMessage::CommitVersion(_)
            | DfsNetworkMessage::RepairReplica(_)
            | DfsNetworkMessage::ContentRootCatalogPageRequest(_)
    ) || matches!(
        message,
        DfsNetworkMessage::NamespaceRequest(command)
            if crate::service_routed_namespace::namespace_operation_response_retry_safe(
                &command.operation
            )
    )
}

fn dfs_network_message_requires_full_metadata_hydrate(message: &DfsNetworkMessage) -> bool {
    match message {
        // Ordinary networked reads carry the exact version and content hash
        // selected by the requester. The serving node can validate and stream
        // that immutable blob directly; rebuilding the complete metadata view
        // would add no authority or freshness evidence and can exceed the RPC
        // budget on slower filesystems. Only the metadata-dependent form needs
        // the legacy object lookup path.
        DfsNetworkMessage::ReadRequest(request) => {
            request.version.is_none() || request.content_hash.is_none()
        }
        // Every bulk item has a mandatory version, hash, and size.
        DfsNetworkMessage::BulkReadRequest(_) => false,
        _ => false,
    }
}

pub fn dfs_message_requires_full_metadata_hydrate(command: &DfsMessageCommand) -> bool {
    dfs_network_message_requires_full_metadata_hydrate(&command.routed_message.message)
}

/// Backward-compatible name for callers that classify daemon message commands.
/// This now means a full backend hydrate; distributed writes perform targeted
/// metadata validation inside the write path instead.
pub fn dfs_message_requires_fresh_metadata(command: &DfsMessageCommand) -> bool {
    dfs_message_requires_full_metadata_hydrate(command)
}

pub(crate) fn dfs_network_message_allows_write_repair_progress(
    message: &DfsNetworkMessage,
) -> bool {
    match message {
        DfsNetworkMessage::DistributedWriteRequest(command)
        | DfsNetworkMessage::ExclusiveCreateRequest(command) => command.foreground_replica_repair,
        _ => false,
    }
}

pub fn dfs_message_allows_write_repair_progress(command: &DfsMessageCommand) -> bool {
    dfs_network_message_allows_write_repair_progress(&command.routed_message.message)
}

pub fn dfs_status_rpc_request() -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(DFS_STATUS_ROUTE, DFS_RPC_CATEGORY, &())
}

pub fn decode_dfs_status_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DfsOperatorStatus, NetworkError> {
    response.decode_body()
}

pub fn dfs_inspection_rpc_request(
    request: &DaemonDfsInspectionRequest,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(DFS_INSPECTION_ROUTE, DFS_RPC_CATEGORY, request)
}

pub fn decode_dfs_inspection_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DaemonDfsInspectionResponse, NetworkError> {
    response.decode_body()
}

pub fn dfs_smoke_rpc_request() -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(DFS_SMOKE_ROUTE, DFS_RPC_CATEGORY, &())
}

pub fn decode_dfs_smoke_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DfsSmokeReport, NetworkError> {
    response.decode_body()
}

pub fn dfs_metadata_journal_batch_rpc_request(
    command: &DfsMetadataJournalBatchCommand,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(DFS_METADATA_JOURNAL_BATCH_ROUTE, DFS_RPC_CATEGORY, command)
}

pub fn dfs_metadata_journal_batch_rpc_call(
    command: &DfsMetadataJournalBatchCommand,
) -> Result<ObjectRpcCall, NetworkError> {
    ObjectRpcCall::encode_borrowed_value(
        DFS_METADATA_JOURNAL_BATCH_ROUTE,
        DFS_RPC_CATEGORY,
        command,
    )
}

pub fn decode_dfs_metadata_journal_batch_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DfsMetadataJournalBatchResponse, NetworkError> {
    response.decode_body()
}

pub fn dfs_metadata_snapshot_install_rpc_request(
    command: &DfsMetadataSnapshotInstallCommand,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(
        DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
        DFS_RPC_CATEGORY,
        command,
    )
}

pub fn dfs_metadata_snapshot_install_rpc_call(
    command: &DfsMetadataSnapshotInstallCommand,
) -> Result<ObjectRpcCall, NetworkError> {
    ObjectRpcCall::encode_borrowed_value(
        DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
        DFS_RPC_CATEGORY,
        command,
    )
}

pub fn decode_dfs_metadata_snapshot_install_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DfsMetadataSnapshotInstallResponse, NetworkError> {
    response.decode_body()
}

pub fn dfs_routed_message_rpc_request(
    message: &DfsRoutedMessage,
) -> Result<ObjectRpcRequest, NetworkError> {
    dfs_message_rpc_request(&dfs_message_command_from_routed_message(message.clone()))
}

pub fn dfs_message_rpc_request(
    command: &DfsMessageCommand,
) -> Result<ObjectRpcRequest, NetworkError> {
    let request =
        ObjectRpcRequest::encode_body(dfs_message_route(command), DFS_RPC_CATEGORY, command)?;
    Ok(prepare_dfs_message_request(request, command))
}

/// Encodes a typed DFS message directly into transport-owned SOBJ chunks.
///
/// The request-only helper above remains the compatibility boundary for code
/// that needs an independently serializable `ObjectRpcRequest`. Routed DFS
/// transport should use this call form so the ordinary value is not first
/// materialized in `ObjectRpcRequest::body`.
pub fn dfs_message_rpc_call(command: &DfsMessageCommand) -> Result<ObjectRpcCall, NetworkError> {
    let mut call = ObjectRpcCall::encode_borrowed_value(
        dfs_message_route(command),
        DFS_RPC_CATEGORY,
        command,
    )?;
    call.request = prepare_dfs_message_request(call.request, command);
    Ok(call)
}

fn prepare_dfs_message_request(
    mut request: ObjectRpcRequest,
    command: &DfsMessageCommand,
) -> ObjectRpcRequest {
    if dfs_network_message_response_retry_safe(&command.routed_message.message) {
        // Ordinary distributed writes and retry-safe namespace operations
        // carry stable request identities and their outcomes are durably
        // reconciled at the metadata primary. Their completed response is also
        // requester-idempotent: a duplicate either settles the same pending
        // request or replays/discards it after it already settled.
        // Repair writes are content-addressed and replay-identifiable;
        // completed duplicate requests take idempotent durable fast paths. If
        // a transport loses only the response, sinomo-network may therefore
        // safely redispatch these commands instead of surfacing an ambiguous
        // timeout. The exceptional lock-bearing cross-shard adoption remains
        // excluded: unlike ordinary admitted mutations and independently
        // idempotent lock commands, it does not yet have a canonical completed
        // response replay.
        request = request.with_response_retry_safe();
    }
    if let Some(traceparent) = crate::current_dfs_traceparent() {
        request = request.with_traceparent(traceparent);
    } else {
        request.ensure_trace_context();
    }
    request
}

pub fn decode_dfs_message_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DfsMessageResponse, NetworkError> {
    response.decode_body()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsContentRootCatalogPageRequest, DfsContentRootCatalogShardIdentity,
        DfsDistributedWriteCommand, DfsMetadataJournalBatch, DfsMetadataShardAssignment,
        DfsMetadataTableSnapshot, DfsNamespaceCommand, DfsNamespaceOperation,
        DfsNamespacePosixError, DfsNamespaceResponse, DfsNamespaceResult, DfsObjectId,
        DfsReadRequest, DfsReplicationPolicy, DfsStageBlobCommand, DfsVersion,
        dfs_content_root_catalog_topology_digest,
    };
    use sinomo_core::node::NodeId;
    use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};

    fn fs_id(value: &str) -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named(value).expect("filesystem id")
    }

    fn object_id(value: &str) -> DfsObjectId {
        DfsObjectId::from_legacy_named(value).expect("object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("version")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node id")
    }

    fn message_command(message: DfsNetworkMessage) -> DfsMessageCommand {
        DfsMessageCommand {
            request_id: "scope-rpc".to_string(),
            routed_message: DfsRoutedMessage {
                source_node_id: node_id("scope-source"),
                target_node_id: node_id("scope-target"),
                frame_id: 1,
                message_id: 2,
                message,
            },
        }
    }

    fn authority_shard(file_system_id: &DfsFileSystemId, shard_id: &str) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.to_string(),
            primary_node_id: node_id("scope-primary"),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn empty_journal_command(
        file_system_id: Option<DfsFileSystemId>,
    ) -> DfsMetadataJournalBatchCommand {
        DfsMetadataJournalBatchCommand {
            request_id: "scope-journal".to_string(),
            source_node_id: Some(node_id("scope-primary")),
            file_system_id,
            authority_epoch: Some(7),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            batch: DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new("scope-shard"),
                entries: Vec::new(),
                next_after_sequence: 0,
            },
        }
    }

    #[test]
    fn read_requests_are_read_only() {
        let command = message_command(DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "scope-read".to_string(),
            object_id: object_id("scope-read-object"),
            version: None,
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
        }));

        assert_eq!(
            dfs_message_execution_scope(&command),
            DfsMessageExecutionScope::ReadOnly
        );
    }

    #[test]
    fn symlink_content_pending_survives_namespace_wire_round_trip() {
        let command = message_command(DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
            request_id: "symlink-pending-response".to_string(),
            result: DfsNamespaceResult::Rejected {
                reason: "symlink content is still converging".to_string(),
                posix_error: Some(DfsNamespacePosixError::SymlinkContentPending {
                    path: "/services/link".to_string(),
                    stage: "fetch-link-body".to_string(),
                    detail: "version v2 is pending at node-b".to_string(),
                }),
                current_primary_node_id: None,
            },
            synced_path_object: None,
            synced_path_objects: Vec::new(),
            metadata_journal_batches: Vec::new(),
            metadata_journal_has_more: None,
            posix_coordination_result: None,
        }));

        let request = dfs_message_rpc_request(&command).expect("namespace response request");
        assert_eq!(request.route, DFS_DATA_NAMESPACE_RESPONSE_ROUTE);
        assert_eq!(
            decode_dfs_message_rpc_request(&request).expect("namespace response round trip"),
            command
        );

        let legacy_without_diagnostics: DfsNamespacePosixError =
            serde_json::from_value(serde_json::json!({
                "symlink-content-pending": {
                    "path": "/services/link"
                }
            }))
            .expect("diagnostic fields are additive");
        assert_eq!(
            legacy_without_diagnostics,
            DfsNamespacePosixError::SymlinkContentPending {
                path: "/services/link".to_string(),
                stage: String::new(),
                detail: String::new(),
            }
        );
    }

    #[test]
    fn content_root_catalog_pages_use_authenticated_read_only_retry_safe_routes() {
        let file_system_id = fs_id("scope-content-root-fs");
        let shard = authority_shard(&file_system_id, "scope-content-root-shard");
        let request = DfsContentRootCatalogPageRequest {
            identity: DfsContentRootCatalogShardIdentity::from_config(&shard),
            expected_topology_digest: dfs_content_root_catalog_topology_digest(&[shard])
                .expect("topology digest"),
            minimum_journal_sequence: 0,
            minimum_state_fingerprint: None,
            after: None,
            max_entries: 16,
        };
        let command = message_command(DfsNetworkMessage::ContentRootCatalogPageRequest(request));

        assert_eq!(
            dfs_message_route(&command),
            DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE
        );
        assert!(dfs_route_is_message_data_plane(
            DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE
        ));
        assert!(dfs_message_is_read_only_request(&command));
        assert_eq!(
            dfs_message_execution_scope(&command),
            DfsMessageExecutionScope::ReadOnly
        );
        assert!(dfs_network_message_response_retry_safe(
            &command.routed_message.message
        ));
    }

    #[test]
    fn namespace_requests_expose_every_involved_path() {
        let file_system_id = fs_id("scope-namespace-fs");
        let command = message_command(DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: "scope-rename".to_string(),
            file_system_id: file_system_id.clone(),
            requesting_node_id: node_id("scope-source"),
            include_metadata_journal_batches: true,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::Rename {
                from: "/source/file".to_string(),
                to: "/destination/file".to_string(),
            },
        }));

        assert_eq!(
            dfs_message_execution_scope(&command),
            DfsMessageExecutionScope::NamespacePaths {
                file_system_id,
                paths: vec!["/source/file".to_string(), "/destination/file".to_string()],
            }
        );

        let empty_lookup =
            message_command(DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: "scope-empty-lookup".to_string(),
                file_system_id: fs_id("scope-empty-fs"),
                requesting_node_id: node_id("scope-source"),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::LookupPaths { paths: Vec::new() },
            }));
        assert_eq!(
            dfs_message_execution_scope(&empty_lookup),
            DfsMessageExecutionScope::ReadOnly
        );
    }

    #[test]
    fn namespace_requests_use_only_one_complete_matching_authority_stamp() {
        let file_system_id = fs_id("scope-stamped-namespace-fs");
        let shard = authority_shard(&file_system_id, "services");
        let namespace_command = |authority_shards| {
            message_command(DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: "scope-stamped-mkdir".to_string(),
                file_system_id: file_system_id.clone(),
                requesting_node_id: node_id("scope-source"),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards,
                operation: DfsNamespaceOperation::Mkdir {
                    path: "/services/new".to_string(),
                },
            }))
        };

        assert_eq!(
            dfs_message_execution_scope(&namespace_command(vec![shard.clone()])),
            DfsMessageExecutionScope::ExactShard {
                file_system_id: file_system_id.clone(),
                shard_id: DfsMetadataShardId::new("services"),
            }
        );
        assert!(matches!(
            dfs_message_execution_scope(&namespace_command(vec![
                shard.clone(),
                authority_shard(&file_system_id, "root"),
            ])),
            DfsMessageExecutionScope::NamespacePaths { .. }
        ));

        let mut wrong_file_system = shard.clone();
        wrong_file_system.file_system_id = fs_id("scope-other-namespace-fs");
        assert!(matches!(
            dfs_message_execution_scope(&namespace_command(vec![wrong_file_system])),
            DfsMessageExecutionScope::NamespacePaths { .. }
        ));

        let mut malformed = shard;
        malformed.shard_id = "  ".to_string();
        assert!(matches!(
            dfs_message_execution_scope(&namespace_command(vec![malformed])),
            DfsMessageExecutionScope::NamespacePaths { .. }
        ));
    }

    #[test]
    fn metadata_replication_requires_an_explicit_filesystem_and_shard() {
        let file_system_id = fs_id("scope-metadata-fs");
        let journal = empty_journal_command(Some(file_system_id.clone()));
        assert_eq!(
            dfs_metadata_journal_batch_execution_scope(&journal),
            DfsMessageExecutionScope::ExactShard {
                file_system_id: file_system_id.clone(),
                shard_id: DfsMetadataShardId::new("scope-shard"),
            }
        );

        let legacy_journal = empty_journal_command(None);
        assert_eq!(
            dfs_metadata_journal_batch_execution_scope(&legacy_journal),
            DfsMessageExecutionScope::GlobalExclusive
        );

        let snapshot = DfsMetadataSnapshotInstallCommand {
            request_id: "scope-snapshot".to_string(),
            source_node_id: node_id("scope-primary"),
            file_system_id: Some(file_system_id.clone()),
            authority_epoch: Some(7),
            primary_handoff_id: None,
            shard_id: DfsMetadataShardId::new("scope-snapshot-shard"),
            next_after_sequence: 0,
            snapshot: DfsMetadataTableSnapshot {
                format_version: 0,
                materialized_through_sequence: None,
                tables: None,
                ops: Vec::new(),
                semantic_state_fingerprint: None,
                prefix_proof: None,
            },
            reason: "scope test".to_string(),
        };
        assert_eq!(
            dfs_metadata_snapshot_install_execution_scope(&snapshot),
            DfsMessageExecutionScope::ExactShard {
                file_system_id,
                shard_id: DfsMetadataShardId::new("scope-snapshot-shard"),
            }
        );
    }

    #[test]
    fn replica_transfer_commands_remain_global_despite_authority_stamp() {
        let file_system_id = fs_id("scope-transfer-fs");
        let mut stage = DfsStageBlobCommand {
            request_id: "scope-stage".to_string(),
            intent_id: "scope-intent".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id("scope-stage-object"),
            expected_base_version: None,
            version: version("scope-stage-version"),
            content_hash: "blake3:scope".to_string(),
            bytes: Vec::new(),
            object_metadata: None,
            authority_shard: Some(authority_shard(&file_system_id, "scope-transfer-shard")),
        };

        assert_eq!(
            dfs_message_execution_scope(&message_command(DfsNetworkMessage::StageBlob(
                stage.clone()
            ))),
            DfsMessageExecutionScope::GlobalExclusive
        );

        stage.authority_shard = None;
        assert_eq!(
            dfs_message_execution_scope(&message_command(DfsNetworkMessage::StageBlob(
                stage.clone()
            ))),
            DfsMessageExecutionScope::GlobalExclusive
        );

        stage.authority_shard = Some(authority_shard(
            &fs_id("scope-other-fs"),
            "scope-transfer-shard",
        ));
        assert_eq!(
            dfs_message_execution_scope(&message_command(DfsNetworkMessage::StageBlob(stage))),
            DfsMessageExecutionScope::GlobalExclusive
        );
    }

    #[test]
    fn distributed_writes_are_exact_only_with_matching_authority() {
        let file_system_id = fs_id("scope-distributed-fs");
        let requester = node_id("scope-requester");
        let mut write = DfsDistributedWriteCommand {
            request_id: "scope-distributed-write".to_string(),
            request: DfsWriteRequest {
                file_system_id: file_system_id.clone(),
                object_id: object_id("scope-distributed-object"),
                target_version: version("scope-distributed-version"),
                requesting_node_id: requester.clone(),
            },
            expected_base_version: None,
            metadata_journal_after_sequence: None,
            bytes: Vec::new(),
            content_hash: Some("blake3:scope".to_string()),
            content_size_bytes: Some(0),
            requester_blob_pre_staged: true,
            desired_replica_nodes: vec![requester],
            policy: DfsReplicationPolicy::new(1, 1).expect("replication policy"),
            object_metadata: None,
            authority_shard: None,
            touch_parent_on_create: true,
            foreground_replica_repair: true,
            exclusive_create: false,
        };

        assert_eq!(
            dfs_message_execution_scope(&message_command(
                DfsNetworkMessage::DistributedWriteRequest(write.clone())
            )),
            DfsMessageExecutionScope::GlobalExclusive
        );

        write.authority_shard = Some(authority_shard(&file_system_id, "scope-distributed-shard"));
        assert_eq!(
            dfs_message_execution_scope(&message_command(
                DfsNetworkMessage::DistributedWriteRequest(write)
            )),
            DfsMessageExecutionScope::ExactShard {
                file_system_id,
                shard_id: DfsMetadataShardId::new("scope-distributed-shard"),
            }
        );
    }
}
