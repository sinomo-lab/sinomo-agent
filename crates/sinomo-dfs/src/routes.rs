//! DFS object-RPC route constants and typed route/message conversion helpers.

use crate::{
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsMessageCommand, DfsMessageResponse,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse, DfsNetworkMessage,
    DfsOperatorStatus, DfsRoutedMessage, DfsSmokeReport,
};
use sinomo_core::network::{NetworkError, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_core::node::NodeId;

pub const DFS_RPC_CATEGORY: &str = "dfs";
pub const DFS_STATUS_ROUTE: &str = "/dfs/status";
pub const DFS_INSPECTION_ROUTE: &str = "/dfs/inspection";
pub const DFS_SMOKE_ROUTE: &str = "/dfs/smoke";
pub const DFS_EXPOSE_CONFIG_ROUTE: &str = "/dfs/expose/config";
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
];
pub const DFS_CONTROL_PLANE_ROUTES: &[&str] = &[
    DFS_STATUS_ROUTE,
    DFS_EXPOSE_CONFIG_ROUTE,
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
];
/// Routes registered by the DFS data-plane service.
pub const DFS_DATA_PLANE_SERVICE_ROUTES: &[&str] = DFS_DATA_PLANE_ROUTES;
pub const DFS_OBJECT_RPC_ROUTES: &[&str] = &[
    DFS_STATUS_ROUTE,
    DFS_EXPOSE_CONFIG_ROUTE,
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
        DfsNetworkMessage::DistributedWriteRequest(_) => DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
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
    }
}

fn dfs_network_message_is_read_only_request(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::ReadRequest(_) | DfsNetworkMessage::BulkReadRequest(_)
    )
}

pub fn dfs_message_is_read_only_request(command: &DfsMessageCommand) -> bool {
    dfs_network_message_is_read_only_request(&command.routed_message.message)
}

fn dfs_network_message_uses_full_response_budget(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::ReadRequest(_)
            | DfsNetworkMessage::ReadResponse(_)
            | DfsNetworkMessage::BulkReadRequest(_)
            | DfsNetworkMessage::BulkReadResponse(_)
            | DfsNetworkMessage::WriteRequest(_)
            | DfsNetworkMessage::WriteResponse(_)
            | DfsNetworkMessage::DistributedWriteRequest(_)
            | DfsNetworkMessage::DistributedWriteResponse(_)
            | DfsNetworkMessage::NamespaceRequest(_)
            | DfsNetworkMessage::NamespaceResponse(_)
            | DfsNetworkMessage::StageBlob(_)
            | DfsNetworkMessage::StagePatch(_)
            | DfsNetworkMessage::StageBlobResponse(_)
            | DfsNetworkMessage::CommitVersion(_)
            | DfsNetworkMessage::CommitVersionResponse(_)
            | DfsNetworkMessage::RepairReplica(_)
            | DfsNetworkMessage::RepairReplicaResponse(_)
            | DfsNetworkMessage::FetchBlob(_)
            | DfsNetworkMessage::FetchBlobResponse(_)
            | DfsNetworkMessage::MetadataJournalBatch(_)
            | DfsNetworkMessage::MetadataJournalBatchResponse(_)
            | DfsNetworkMessage::MetadataSnapshotInstall(_)
            | DfsNetworkMessage::MetadataSnapshotInstallResponse(_)
    )
}

fn dfs_network_message_response_retry_safe(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::DistributedWriteRequest(_) | DfsNetworkMessage::RepairReplica(_)
    )
}

pub fn dfs_message_uses_full_response_budget(command: &DfsMessageCommand) -> bool {
    dfs_network_message_uses_full_response_budget(&command.routed_message.message)
}

fn dfs_network_message_requires_full_metadata_hydrate(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::ReadRequest(_) | DfsNetworkMessage::BulkReadRequest(_)
    )
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
        DfsNetworkMessage::DistributedWriteRequest(command) => command.foreground_replica_repair,
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
    Ok(
        ObjectRpcRequest::encode_body(DFS_METADATA_JOURNAL_BATCH_ROUTE, DFS_RPC_CATEGORY, command)?
            .with_full_response_budget(),
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
    Ok(ObjectRpcRequest::encode_body(
        DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
        DFS_RPC_CATEGORY,
        command,
    )?
    .with_full_response_budget())
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
    let mut request =
        ObjectRpcRequest::encode_body(dfs_message_route(command), DFS_RPC_CATEGORY, command)?;
    if dfs_message_uses_full_response_budget(command) {
        request = request.with_full_response_budget();
    }
    if dfs_network_message_response_retry_safe(&command.routed_message.message) {
        // Distributed writes carry a stable request identity and their outcome
        // is durably reconciled at the metadata primary. Repair writes are
        // content-addressed and replay-identifiable; completed duplicate
        // requests take idempotent durable fast paths. If a transport loses
        // only the response, sinomo-network may therefore safely redispatch
        // these commands instead of surfacing an ambiguous timeout.
        request = request.with_response_retry_safe();
    }
    if let Some(traceparent) = crate::current_dfs_traceparent() {
        request = request.with_traceparent(traceparent);
    } else {
        request.ensure_trace_context();
    }
    Ok(request)
}

pub fn decode_dfs_message_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DfsMessageResponse, NetworkError> {
    response.decode_body()
}
