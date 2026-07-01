//! Local-first distributed filesystem metadata skeleton.

mod cluster_service;
pub mod content_store;
pub mod control_plane;
mod default_cluster;
mod direct_read;
pub mod discovery;
pub mod distributed;
mod endpoint_transport;
mod error;
mod identity;
mod intent;
pub mod local_owned;
mod local_service;
mod local_state;
mod maintenance;
mod message_transport;
pub mod metadata_backend;
mod metadata_records;
mod metadata_replication;
mod model;
mod module_surface;
mod mounted_sifs;
pub mod namespace;
pub mod operator;
pub mod posix;
pub mod protocol;
mod repair;
mod routes;
mod service;
mod service_conflict;
mod service_content;
mod service_core;
mod service_hydration;
mod service_maintenance;
mod service_metadata;
mod service_outbound;
mod service_protocol;
mod service_read;
mod service_repair;
mod service_state;
mod service_trace;
mod service_transfer;
mod service_write;
mod smoke;
mod transfer;

pub use cluster_service::cluster_dfs_data_endpoints;
pub(crate) use cluster_service::{
    object_rpc_request_from_kv_payload, open_cluster_dfs_service_with_message_sender,
    open_cluster_dfs_service_with_peer_transports,
};
pub(crate) use content_store::content_hash;
pub use content_store::{
    BlobRangeReader, BlobReader, ChunkedBlobAssembler, ContentManifest, ContentStoreError,
    ContentStoreGcOptions, ContentStoreGcReport, ContentStoreQuota, ContentStoreQuotaResource,
    ContentStoreQuotaUsage, ContentStoreScrubIssue, ContentStoreScrubReport,
    ContentStoreStagingGcOptions, ContentStoreStagingGcReport, ContentStoreStats,
    ContentStoreVersionGcOptions, ContentStoreVersionGcReport, ContentVersionIdentity,
    LocalContentStore, ObjectVersionReader,
};
pub use control_plane::{
    AsyncDfsControlPlaneKvClient, DfsControlPlaneAsyncStore, DfsControlPlaneKvClient,
    DfsControlPlaneKvFuture, DfsControlPlaneStore, DfsFileSystemConfig, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, SharedDfsControlPlaneKvClient, file_system_config_key,
    metadata_shard_key, metadata_shards_prefix,
};
#[cfg(test)]
pub(crate) use default_cluster::run_default_cluster_dfs_smoke;
pub use default_cluster::{
    DfsControlPlaneDefaultClusterAsyncBootstrapHost, DfsControlPlaneDefaultClusterBootstrapHost,
    DfsDefaultClusterAsyncBootstrapHost, DfsDefaultClusterBootstrapFuture,
    DfsDefaultClusterBootstrapHost, DfsDefaultClusterBootstrapOptions,
    DfsDefaultClusterBootstrapReport, DfsDefaultClusterSmokeOptions,
    default_cluster_metadata_shard_with_async_host, default_cluster_metadata_shard_with_host,
    default_cluster_metadata_shards_with_async_host, default_cluster_metadata_shards_with_host,
    default_cluster_sifs, default_cluster_sifs_owner, default_cluster_sifs_subtree_policies,
    default_metadata_shard_primary_node_id, default_metadata_shard_replica_node_ids,
    dfs_service_endpoint_for_member, dfs_service_endpoint_rank, dfs_service_members,
    ensure_default_cluster_dfs, ensure_default_cluster_dfs_async,
    ensure_default_distributed_dfs_with_async_host, ensure_default_distributed_dfs_with_host,
    metadata_shard_layout_matches, metadata_shard_primary_transfer_ready_from_local_backend,
    metadata_shard_replica_node_ids, run_default_cluster_dfs_smoke_with_peer_transport,
    stable_dfs_service_node_ids,
};
#[cfg(test)]
pub(crate) use direct_read::{
    DFS_DIRECT_READ_STATUS_FOUND, DFS_DIRECT_READ_STATUS_METADATA_KEY,
    dfs_direct_read_rpc_request_from_message_command, dfs_direct_read_rpc_response,
    dfs_message_response_from_direct_read_rpc_response,
};
pub use direct_read::{
    DFS_DIRECT_READ_STREAM_CHUNK_BYTES, DfsDirectReadStreamResponse,
    dfs_direct_read_response_from_rpc_response, dfs_direct_read_rpc_request,
    dfs_direct_read_rpc_request_accepts_streamed_response, handle_dfs_direct_read_rpc_request,
    handle_dfs_direct_read_stream_rpc_request,
};
pub(crate) use direct_read::{
    DFS_DIRECT_REMOTE_READ_MAX_BYTES, dfs_direct_read_request_from_read_request,
    direct_full_read_response_allowed,
};
pub use discovery::{
    DfsClusterView, DfsNodeAdvertisement, content_service_key, content_services_prefix,
    metadata_service_key, metadata_services_prefix,
};
pub use distributed::{
    DfsDistributedError, DfsReplicaCandidate, DfsReplicaPlacementPlan, DfsReplicaPlacementRequest,
    DfsReplicaReceiptState, DfsReplicationPolicy, DfsWriteIntentState,
    DistributedEventualCoordinator, DistributedRepairJob, DistributedReplicaReceipt,
    DistributedWriteIntent, plan_replicas, validate_replication_plan,
};
#[cfg(test)]
pub(crate) use endpoint_transport::{
    AsyncDfsDirectReadSender, AsyncDfsMessageSender, DfsDirectReadSender,
    async_dfs_direct_read_sender_from_peer_transport, dfs_direct_read_sender_from_peer_transport,
};
pub use endpoint_transport::{
    AsyncDfsPeerTransport, DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT, DfsEndpointServedMessageTransport,
    DfsEndpointTransport, DfsObjectRpcSendOptions, DfsPeerTransport, DfsPeerTransportError,
    DfsPeerTransportFuture,
};
pub(crate) use endpoint_transport::{DfsMessageSender, dfs_message_sender_from_peer_transport};
#[cfg(test)]
pub(crate) use endpoint_transport::{
    process_cluster_dfs_message_command, run_cluster_dfs_message_command_with_metadata_publish,
};
pub use error::DfsError;
pub(crate) use error::{
    distributed_write_rejection_current_primary, namespace_rejection_current_primary,
    routed_write_rejection_error,
};
pub use identity::{
    DEFAULT_DFS_NODE_ID, SIFS_FILE_SYSTEM_ID, fallback_dfs_node_id, sifs_file_system_id,
};
pub(crate) use intent::{
    conflict_id_for, distributed_error_creates_conflict, distributed_intent_from_record,
    distributed_intent_matches_record, distributed_intent_matches_routed_write_command,
    routed_write_command_content_hash, write_intent_record,
    write_intent_record_matches_routed_write_command, write_intent_status_requires_hydrate_restore,
};
pub use local_owned::{
    DFS_EXPOSE_CONFIG_KEY, DfsExposeAddRequest, DfsExposeConfig, DfsExposeEntry, DfsExposeFileInfo,
    DfsExposeStore, DfsExposeStoreError, DfsExposedDirectory, LocalOwnedDfsError,
    dfs_expose_config_definition, register_config_definitions as register_dfs_config_definitions,
};
pub use local_service::LocalOwnedDfsService;
pub(crate) use local_state::dfs_path_safe_segment;
pub use local_state::{
    DFS_IMPORTS_NAMESPACE_ROOT, add_dfs_expose_entry, add_local_dfs_pin,
    cluster_dfs_metadata_shards, default_dfs_expose_config_path, default_dfs_metadata_root,
    default_dfs_metadata_shards_root, default_dfs_pin_root, dfs_expose_config_path,
    dfs_import_metadata_root, dfs_import_namespace_root, dfs_import_root,
    ensure_default_cluster_aware_dfs_expose_config, ensure_default_dfs_expose_config,
    inspect_cluster_aware_dfs, inspect_cluster_aware_dfs_status, inspect_dfs_conflicts,
    inspect_dfs_file_system, inspect_dfs_object, inspect_dfs_path, inspect_dfs_repairs,
    inspect_dfs_status, list_local_dfs_pins, load_dfs_expose_config, local_dfs_metadata_shard_path,
    open_cluster_aware_dfs_metadata_backend, open_cluster_sharded_dfs_metadata_backend,
    open_local_metadata_shard_backend, open_local_sharded_metadata_backend,
    open_root_local_metadata_backend, remove_dfs_expose_entry, remove_local_dfs_pin,
    resolve_dfs_metadata_root, resolve_dfs_pin_root,
};
pub(crate) use maintenance::record_anti_entropy_issue;
pub use maintenance::{
    DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_ISSUES,
    DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_OBJECTS,
    DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_JOBS,
    DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_STEPS_PER_JOB, DfsAntiEntropyCursorRecord,
    DfsAntiEntropyIssue, DfsAntiEntropyOptions, DfsAntiEntropyReport,
    DfsLifecycleMaintenanceOptions, DfsLifecycleMaintenanceReport, DfsMaintenanceHost,
    DfsMaintenanceRunOptions, DfsMaintenanceTickObservations, DfsMaintenanceTickOptions,
    DfsMaintenanceTickReport, DfsMetadataReplicationSuppressionRecord,
    DfsMetadataReplicationSuppressionSnapshot, DfsRepairAttemptRecord, DfsRepairAttemptSnapshot,
    LegacyDfsMetadataReplicationCursor, LocalDfsMaintenanceHost, clear_dfs_anti_entropy_cursor,
    default_dfs_anti_entropy_cursor_path,
    default_dfs_metadata_replication_suppression_snapshot_path,
    default_dfs_repair_attempt_snapshot_path, dfs_lifecycle_maintenance_notice_lines,
    dfs_maintenance_tick_observations, dfs_namespace_job_maintenance_notice_lines,
    dfs_repair_maintenance_notice_lines, legacy_dfs_anti_entropy_cursor_path,
    legacy_dfs_metadata_replication_cursor_path, migrate_legacy_dfs_metadata_replication_cursor,
    put_local_dfs_metadata_replication_suppression, read_dfs_anti_entropy_cursor,
    read_dfs_metadata_replication_suppression_snapshot, read_dfs_repair_attempt_snapshot,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport,
    run_cluster_dfs_maintenance_tick_with_peer_transport, run_dfs_lifecycle_maintenance_with_host,
    run_dfs_maintenance_tick_with_async_host, run_dfs_maintenance_tick_with_host,
    write_dfs_anti_entropy_cursor, write_dfs_metadata_replication_suppression_snapshot,
    write_dfs_repair_attempt_snapshot,
};
#[cfg(test)]
pub(crate) use maintenance::{
    run_cluster_dfs_maintenance_tick, write_legacy_dfs_metadata_replication_cursor,
};
pub use message_transport::{
    AsyncDfsMessageTransport, DfsMessageTransport, DfsMessageTransportFuture, DfsTransferProfile,
    DfsTransportIdentity,
};
pub use metadata_backend::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
    DfsConflictPage, DfsConflictPageCursor, DfsConflictRecord, DfsConflictScanCursor,
    DfsConflictScanPage, DfsConflictStatus, DfsDentryPageCursor, DfsDirFragmentId,
    DfsDirFragmentRecord, DfsDirectoryPage, DfsDirectoryPageCursor, DfsInodeId, DfsInodeRecord,
    DfsMetadataBackend, DfsMetadataJournalBatch, DfsMetadataJournalEntry, DfsMetadataMutation,
    DfsMetadataReplayCursorStatus, DfsMetadataReplicaLagStatus, DfsMetadataShardId,
    DfsMetadataShardStatus, DfsMetadataSubscription, DfsMetadataSummary, DfsMetadataTableSnapshot,
    DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobRecord, DfsNamespaceJobStatus,
    DfsNamespaceTransaction, DfsObjectPage, DfsObjectPageCursor, DfsObjectVersionPage,
    DfsObjectVersionPageCursor, DfsObjectVersionRecord, DfsObjectVersionScanCursor,
    DfsObjectVersionScanPage, DfsPathRecord, DfsReplicaReceiptPage, DfsReplicaReceiptPageCursor,
    DfsReplicaReceiptRecord, DfsReplicaReceiptScanCursor, DfsReplicaReceiptScanPage,
    DfsTombstoneCompactionOptions, DfsTombstoneCompactionReport, DfsWriteIntentPage,
    DfsWriteIntentPageCursor, DfsWriteIntentRecord, DfsWriteIntentStatus,
    LocalShardedMetadataBackend, LocalSobjMetadataBackend, MetadataBackendError,
};
pub(crate) use metadata_records::{
    dfs_now_unix_nanos, metadata_journal_batch_file_system_id, object_version_record,
    put_visible_version_record_if_changed, read_conflicts_reason,
    remote_read_rejection_prunes_location,
};
pub use metadata_replication::{
    AsyncDfsMetadataEndpointTransport, AsyncDfsMetadataJournalBatchSender,
    AsyncDfsMetadataJournalReplicationTransport, AsyncDfsMetadataSnapshotInstallSender,
    DEFAULT_DFS_METADATA_REPLICATION_BATCH_TARGET_TIMEOUT,
    DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
    DEFAULT_DFS_METADATA_REPLICATION_TIMEOUT, DFS_METADATA_REPLICATION_MAX_ENDPOINTS_PER_TARGET,
    DfsMetadataEndpointTransport, DfsMetadataJournalBatchSender, DfsMetadataJournalPublisherHost,
    DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationOutcome,
    DfsMetadataJournalReplicationReport, DfsMetadataJournalReplicationTransport,
    DfsMetadataJournalReplicationTransportError, DfsMetadataJournalReplicationTransportFuture,
    DfsMetadataPeerTransportError, DfsMetadataPublishScheduleOptions,
    DfsMetadataPublishScheduleStart, DfsMetadataPublishScheduler, DfsMetadataReplicaShardContext,
    DfsMetadataSnapshotInstallSender, LocalDfsMetadataJournalPublisherHost,
    cluster_dfs_metadata_replica_shard_context, dfs_events_should_publish_metadata,
    dfs_metadata_replication_notice_lines, dfs_metadata_replication_report_needs_retry,
    dfs_metadata_replication_sent_count, materialize_default_sifs_metadata,
    materialize_default_sifs_metadata_on_shards, process_cluster_dfs_metadata_journal_batch,
    process_cluster_dfs_metadata_snapshot_install, process_dfs_metadata_journal_batch,
    process_dfs_metadata_snapshot_install, publish_cluster_dfs_metadata_journal,
    publish_cluster_dfs_metadata_journal_async,
    publish_cluster_dfs_metadata_journal_with_async_peer_transport,
    publish_cluster_dfs_metadata_journal_with_peer_transport,
    publish_cluster_dfs_metadata_journal_with_peer_transports_async,
    publish_dfs_metadata_journal_with_async_host, publish_dfs_metadata_journal_with_host,
    refresh_cluster_dfs_kv_snapshot_with_peer_transport, replicate_dfs_metadata_shard_to_target,
    run_cluster_dfs_metadata_publish_after_events,
    run_cluster_dfs_metadata_publish_after_events_with_async_peer_transport,
    run_cluster_dfs_metadata_publish_after_events_with_peer_transport,
    run_dfs_metadata_publish_after_events, run_dfs_metadata_publish_after_events_async,
    schedule_cluster_dfs_metadata_publish_after_events_async_on_runtime,
    schedule_cluster_dfs_metadata_publish_after_events_with_async_peer_transport_on_runtime,
    schedule_cluster_dfs_metadata_publish_after_events_with_peer_transports_on_runtime,
    schedule_default_dfs_metadata_publish_after_events_async_on_runtime,
    schedule_dfs_metadata_publish_after_events_async_on_runtime,
    validate_dfs_metadata_journal_batch_source, validate_dfs_metadata_snapshot_install_source,
};
pub(crate) use metadata_replication::{
    async_dfs_metadata_journal_batch_sender_from_peer_transport,
    async_dfs_metadata_snapshot_install_sender_from_peer_transport,
    dfs_metadata_journal_batch_sender_from_peer_transport,
    dfs_metadata_snapshot_install_sender_from_peer_transport,
    metadata_journal_rejection_allows_snapshot_repair,
    refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport,
};
#[cfg(test)]
pub(crate) use metadata_replication::{
    metadata_replication_rejection_is_topology_mismatch,
    prepare_dfs_metadata_journal_publish_with_host,
};
pub use model::{
    DfsFileSystem, DfsMetadataStore, DfsObjectKind, DfsObjectMetadata, DfsOperationalMetrics,
    DfsPolicyViolation, DfsReadOutcome, DfsServiceEvent, DfsSubtreePolicy, DfsWriteOutcome,
    ReadRoute, ReadRouteAction, WriteIntent, WriteIntentStatus, WriteRoute, WriteRouteAction,
};
pub(crate) use model::{normalize_current_locations, path_index_key};
pub use module_surface::{
    DFS_POLICY_VIOLATION_EVENT_KIND, DFS_PROTOCOL, SINOMO_DFS_MODULE_LEGACY_ID,
    SINOMO_DFS_MODULE_OWNER, SINOMO_DFS_NATIVE_REGISTRATION_KEY, dfs_module_capabilities,
    dfs_module_id, dfs_module_registration, dfs_module_surfaces, register_storage_paths,
};
pub use mounted_sifs::{MountedSifsService, OpenedMountedSifsService, open_mounted_sifs_service};
pub use namespace::{
    SIFS_NODES_PATH, SIFS_ROOT_PATH, SIFS_SERVICES_PATH, SIFS_SINOMO_PATH, SIFS_USERS_PATH,
    directory_object, distributed_sifs, distributed_sifs_bootstrap_objects,
    distributed_sifs_bootstrap_objects_for_nodes, sifs_node_files_path, sifs_node_path,
};
pub(crate) use namespace::{
    metadata_path, namespace_transaction_id_for_batch, namespace_transaction_id_for_object,
};
pub use operator::{
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsAgentFileSystemSummary,
    DfsAgentObjectState, DfsAgentObjectSummary, DfsOperatorConflictPage,
    DfsOperatorConflictResolutionReport, DfsOperatorConflictVersionRole,
    DfsOperatorConflictVersionView, DfsOperatorConflictVersionsReport, DfsOperatorFileSystemView,
    DfsOperatorInspectionCursor, DfsOperatorInspectionItemCursor, DfsOperatorInspector,
    DfsOperatorObjectView, DfsOperatorRepairPage, DfsOperatorRepairView, DfsOperatorStatus,
    DfsOperatorUseConflictVersionReport, DfsPinAddRequest, DfsPinMode, DfsPinRecord,
    DfsPinStoreError, LocalDfsPinStore, resolve_dfs_conflicts, use_dfs_conflict_version,
};
pub use posix::{
    DfsDirEntry, DfsDirEntryPage, DfsNamespaceJobFailure, DfsNamespaceJobMaintenanceReport,
    DfsPosixError, DfsPosixFileRangeReadStream, DfsPosixFileReadStream, DfsStat, DfsWriteOptions,
};
pub use protocol::{
    DfsBlobTransferResult, DfsBulkReadItem, DfsBulkReadItemResponse, DfsBulkReadRequest,
    DfsBulkReadResponse, DfsCommitVersionCommand, DfsCommitVersionResponse, DfsDirectReadRequest,
    DfsDistributedWriteCommand, DfsDistributedWriteResponse, DfsDistributedWriteResult,
    DfsFetchBlobCommand, DfsFetchBlobResponse, DfsFetchBlobResponseChunk, DfsFetchBlobResult,
    DfsMessageCommand, DfsMessageResponse, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
    DfsMetadataSnapshotInstallResult, DfsNamespaceCommand, DfsNamespaceOperation,
    DfsNamespacePosixError, DfsNamespaceResponse, DfsNamespaceResult, DfsNamespaceShardCursor,
    DfsNetworkMessage, DfsReadData, DfsReadRangeData, DfsReadRequest, DfsReadResponse,
    DfsReadResponseChunk, DfsReadResult, DfsRepairReplicaChunkCommand, DfsRepairReplicaCommand,
    DfsRepairReplicaResponse, DfsRoutedMessage, DfsStageBlobChunkCommand, DfsStageBlobCommand,
    DfsStageBlobResponse, DfsStagePatchChunkCommand, DfsStagePatchCommand, DfsWriteCommand,
    DfsWriteResponse, DfsWriteResult,
};
pub(crate) use repair::{
    repair_attempt_record_indicates_lost_authority, repair_rejection_indicates_lost_authority,
    repair_rejection_indicates_stale_chunk_resume,
};
pub use routes::{
    DFS_CONTROL_PLANE_ROUTES, DFS_DATA_BULK_READ_RESPONSE_ROUTE, DFS_DATA_BULK_READ_ROUTE,
    DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE, DFS_DATA_COMMIT_VERSION_ROUTE,
    DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE, DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
    DFS_DATA_FETCH_BLOB_RESPONSE_CHUNK_ROUTE, DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE,
    DFS_DATA_FETCH_BLOB_ROUTE, DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE,
    DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE, DFS_DATA_METADATA_SNAPSHOT_INSTALL_RESPONSE_ROUTE,
    DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE, DFS_DATA_NAMESPACE_RESPONSE_ROUTE,
    DFS_DATA_NAMESPACE_ROUTE, DFS_DATA_PLANE_ROUTES, DFS_DATA_PLANE_SERVICE_ROUTES,
    DFS_DATA_READ_RESPONSE_CHUNK_ROUTE, DFS_DATA_READ_RESPONSE_ROUTE, DFS_DATA_READ_ROUTE,
    DFS_DATA_REPAIR_REPLICA_CHUNK_ROUTE, DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE,
    DFS_DATA_REPAIR_REPLICA_ROUTE, DFS_DATA_STAGE_BLOB_CHUNK_ROUTE,
    DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE, DFS_DATA_STAGE_BLOB_ROUTE,
    DFS_DATA_STAGE_PATCH_CHUNK_ROUTE, DFS_DATA_STAGE_PATCH_ROUTE, DFS_DATA_WRITE_RESPONSE_ROUTE,
    DFS_DATA_WRITE_ROUTE, DFS_DIRECT_READ_ROUTE, DFS_EXPOSE_CONFIG_ROUTE, DFS_INSPECTION_ROUTE,
    DFS_MESSAGE_DATA_PLANE_ROUTES, DFS_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, DFS_OBJECT_RPC_ROUTES, DFS_RPC_CATEGORY, DFS_SMOKE_ROUTE,
    DFS_STATUS_ROUTE, decode_dfs_inspection_rpc_response, decode_dfs_message_rpc_request,
    decode_dfs_message_rpc_response, decode_dfs_metadata_journal_batch_rpc_response,
    decode_dfs_metadata_snapshot_install_rpc_response, decode_dfs_smoke_rpc_response,
    decode_dfs_status_rpc_response, dfs_inspection_rpc_request,
    dfs_message_allows_write_repair_progress, dfs_message_is_read_only_request,
    dfs_message_requires_fresh_metadata, dfs_message_rpc_request,
    dfs_metadata_journal_batch_rpc_request, dfs_metadata_snapshot_install_rpc_request,
    dfs_route_is_message_data_plane, dfs_routed_message_rpc_request, dfs_smoke_rpc_request,
    dfs_status_rpc_request,
};
pub(crate) use routes::{
    build_dfs_routed_message, dfs_network_message_allows_write_repair_progress,
};
pub(crate) use service_content::object_content_identity;
pub(crate) use service_outbound::read_small_blob_from_reader;
pub(crate) use service_repair::DFS_CHUNK_TRANSFER_IDLE_TIMEOUT;

pub type DfsFileSystemId = sinomo_core::Id;
pub type DfsNodeId = NodeId;
pub type DfsObjectId = sinomo_core::Id;
pub type DfsVersion = sinomo_core::Id;

pub use service::{
    DfsDataPlaneHost, DfsDataPlaneHostError, DfsDataPlaneHostFuture, DfsDataPlaneService,
    DfsService, register_data_plane_services, register_services,
};
pub(crate) use service_state::{
    ActiveReplicaStage, CachedSifsMetadataShards, NetworkedDfsRangeReadStream,
    NetworkedDfsRangeReadStreamOutcome, NetworkedDfsReadStream, NetworkedDfsReadStreamOutcome,
    PendingMetadataReplication, PendingRemoteWriteResponse, PendingRoutedNamespaceOp,
    PendingRoutedNamespaceResult, PendingRoutedWrite, StageBlobSendOutcome,
    retained_networked_dfs_service_events,
};
pub use service_state::{
    DfsConfirmedTimeoutAction, DfsConfirmedTimeoutOutcome, DfsConflictReadView,
    DfsMetadataReplicationOutcome, DfsOwnerHandoffOutcome, DfsReconnectReconciliation,
    NetworkedDfsRangeReadOutcome, NetworkedDfsReadFreshness, NetworkedDfsReadOutcome,
    NetworkedDfsServiceEvent, NetworkedDfsWriteOutcome,
};
pub(crate) use service_trace::{
    dfs_distributed_write_result_trace_label, dfs_namespace_operation_trace_label,
    dfs_network_message_trace_label, dfs_perf_trace, dfs_perf_trace_enabled,
};
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest};
use sinomo_events::EventId;
#[cfg(test)]
use sinomo_network::NetworkError;
use sinomo_node::NodeId;
pub(crate) use smoke::run_cluster_dfs_smoke_with_message_sender;
pub use smoke::{
    DfsSmokeAttempt, DfsSmokeHost, DfsSmokeOptions, DfsSmokeReport, LocalDfsSmokeHost,
    run_dfs_smoke_with_host,
};
#[cfg(test)]
pub(crate) use smoke::{
    dfs_smoke_content_node_ids, dfs_smoke_replica_candidate_sets, run_cluster_dfs_smoke,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
#[cfg(test)]
use transfer::{
    DFS_ADAPTIVE_TRANSFER_WINDOW_MAX_BYTES, DFS_BLOB_CHUNK_BYTES, DFS_LAN_CHUNK_WINDOW_SIZE,
    DFS_LOCAL_CHUNK_WINDOW_SIZE, DFS_REPAIR_RETRY_SUPPRESSED_TICK, RepairRetryState,
    blob_chunk_count, blob_chunk_count_for_size, chunk_hash, fetch_inflight_timeout_ticks,
    repair_replica_chunk_start_and_window, repair_retry_delay_ticks,
    repair_transfer_inflight_timeout_ticks,
};
use transfer::{
    InProgressBlobTransfer, InProgressFetchBlobTransfer, InProgressPatchTransfer,
    InProgressReadMemoryTransfer, InProgressReadTransfer, PendingNetworkedRead,
    PendingNetworkedWrite,
};

pub(crate) const DFS_BLOB_INLINE_LIMIT_BYTES: usize = 1024 * 1024;
const DFS_REMOTE_READ_RETRY_DELAY: Duration = Duration::from_secs(1);
const DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE: usize = 4096;
const DFS_METADATA_OBJECT_VERSION_PAGE_SIZE: usize = 1024;
const DFS_METADATA_OBJECT_INTENT_PAGE_SIZE: usize = 1024;
const DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE: usize = 1024;
#[cfg(not(test))]
const DFS_CONFLICT_SCAN_PAGE_SIZE: usize = 1024;
#[cfg(test)]
const DFS_CONFLICT_SCAN_PAGE_SIZE: usize = 2;

#[derive(Debug)]
pub struct NetworkedDfsService<T> {
    node_id: NodeId,
    metadata: DfsMetadataStore,
    tombstones_by_metadata_path: BTreeMap<(DfsFileSystemId, String), DfsObjectMetadata>,
    content: LocalContentStore,
    coordinator: DistributedEventualCoordinator,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    cached_sifs_metadata_shards: Option<CachedSifsMetadataShards>,
    metadata_backend: Option<Box<dyn DfsMetadataBackend>>,
    active_replica_stages: BTreeMap<DfsObjectId, ActiveReplicaStage>,
    pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    pending_reads: BTreeMap<String, PendingNetworkedRead>,
    repair_attempt_records: BTreeMap<(String, NodeId), DfsRepairAttemptRecord>,
    transient_remote_read_pruned_locations:
        BTreeMap<(DfsObjectId, DfsVersion, Option<String>), BTreeSet<NodeId>>,
    incoming_read_transfers: BTreeMap<String, InProgressReadTransfer>,
    incoming_read_memory_transfers: BTreeMap<String, InProgressReadMemoryTransfer>,
    incoming_fetch_transfers: BTreeMap<String, InProgressFetchBlobTransfer>,
    incoming_blob_transfers: BTreeMap<String, InProgressBlobTransfer>,
    incoming_patch_transfers: BTreeMap<String, InProgressPatchTransfer>,
    metadata_replication_cursors: BTreeMap<(NodeId, DfsMetadataShardId), u64>,
    recent_namespace_freshness_syncs:
        BTreeMap<(DfsFileSystemId, DfsMetadataShardId, NodeId), Instant>,
    pending_metadata_replications: BTreeMap<String, PendingMetadataReplication>,
    pending_routed_writes: BTreeMap<String, PendingRoutedWrite>,
    pending_remote_write_responses: BTreeMap<String, PendingRemoteWriteResponse>,
    deferred_routed_create_parent_touches: BTreeSet<String>,
    deferred_untracked_routed_write_responses: BTreeMap<String, DfsObjectId>,
    pending_routed_namespace_ops: BTreeMap<String, PendingRoutedNamespaceOp>,
    routed_namespace_settle_depth: usize,
    cross_shard_namespace_locks: BTreeSet<(DfsFileSystemId, String)>,
    last_events: Vec<NetworkedDfsServiceEvent>,
    transport: T,
    repair_retry_tick: u64,
    next_frame_id: u64,
    next_message_id: u64,
    next_request_id: u64,
}

#[derive(Clone, Debug)]
pub struct DfsMessageProcessResult {
    pub response: DfsMessageResponse,
    pub events: Vec<NetworkedDfsServiceEvent>,
}

const DFS_DEFERRED_PARENT_TOUCH_DRAIN_LIMIT: usize = 8;
// Cross-shard coordination (rename/hardlink sagas) does several primary-to-
// primary roundtrips plus journal publication before the requester's answer;
// over bridge-relayed segments that legitimately exceeds a small budget.
pub const DFS_ROUTED_NAMESPACE_OP_TIMEOUT: Duration = Duration::from_secs(30);
/// Nested coordination depth (serving a namespace request while settling
/// another) beyond which new coordination rejects as Busy instead of growing
/// the stack.
const DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH: usize = 8;
/// Journal batches a primary ships in one routed-operation response.
pub const DFS_ROUTED_RESPONSE_JOURNAL_BATCH_LIMIT: usize = 16;
const DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT: usize = 3;
const DFS_ROUTED_WRITE_INLINE_BYTES_LIMIT: u64 = DFS_BLOB_INLINE_LIMIT_BYTES as u64;
const DFS_ROUTED_NAMESPACE_IDLE_WAIT: Duration = Duration::from_millis(10);

const DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES: usize = 64;

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    pub async fn process_incoming_async(
        &mut self,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.prune_idle_chunk_transfers();
        if self
            .transport
            .pending_incoming_count_async()
            .await
            .map_err(DfsError::from_network)?
            == Some(0)
        {
            self.flush_deferred_untracked_routed_write_responses_async()
                .await?;
            return Ok(Vec::new());
        }
        let inbound_messages = self
            .transport
            .drain_incoming_messages_async()
            .await
            .map_err(DfsError::from_network)?;
        let events = self.process_routed_messages_with_options(inbound_messages, true)?;
        self.flush_deferred_untracked_routed_write_responses_async()
            .await?;
        Ok(events)
    }

    async fn process_incoming_async_until_quiescent(
        &mut self,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        let mut events = Vec::new();
        for _ in 0..DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES {
            let batch = self.process_incoming_async().await?;
            events.extend(batch);
            if self
                .transport
                .pending_incoming_count_async()
                .await
                .map_err(DfsError::from_network)?
                == Some(0)
            {
                return Ok(events);
            }
        }
        Err(DfsError::Network {
            message: format!(
                "DFS async data-plane maintenance did not quiesce after {DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES} passes"
            ),
        })
    }

    async fn flush_deferred_untracked_routed_write_responses_async(
        &mut self,
    ) -> Result<(), DfsError> {
        if self.deferred_untracked_routed_write_responses.is_empty() {
            return Ok(());
        }
        let pending = self
            .deferred_untracked_routed_write_responses
            .iter()
            .map(|(intent_id, object_id)| (intent_id.clone(), object_id.clone()))
            .collect::<Vec<_>>();
        for (intent_id, object_id) in pending {
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                self.deferred_untracked_routed_write_responses
                    .remove(&intent_id);
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::Committed)
                || intent.requesting_node_id == self.node_id
                || self.pending_remote_write_responses.contains_key(&intent_id)
            {
                self.deferred_untracked_routed_write_responses
                    .remove(&intent_id);
                continue;
            }
            let outcome = self.write_outcome(&intent_id)?;
            if !matches!(outcome, NetworkedDfsWriteOutcome::Committed { .. }) {
                self.deferred_untracked_routed_write_responses
                    .remove(&intent_id);
                continue;
            }
            let result = DfsDistributedWriteResult::Outcome { outcome };
            let metadata_journal_batches =
                self.routed_write_response_metadata_journal_batches(&object_id, &result, None)?;
            let sent = self
                .send_dfs_message_if_routable_async(
                    &intent.requesting_node_id,
                    DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                        request_id: intent.intent_id.clone(),
                        object_id: object_id.clone(),
                        result,
                        metadata_journal_batches,
                    }),
                )
                .await?;
            if sent {
                self.deferred_untracked_routed_write_responses
                    .remove(&intent_id);
            }
        }
        Ok(())
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn handle_confirmed_write_timeout(
        &mut self,
        intent_id: &str,
        action: DfsConfirmedTimeoutAction,
    ) -> Result<DfsConfirmedTimeoutOutcome, DfsError> {
        match action {
            DfsConfirmedTimeoutAction::KeepPending => match self.write_outcome(intent_id)? {
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } => Ok(DfsConfirmedTimeoutOutcome::StillPending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                }),
                outcome @ NetworkedDfsWriteOutcome::Committed { .. } => {
                    Ok(DfsConfirmedTimeoutOutcome::Committed(outcome))
                }
                NetworkedDfsWriteOutcome::Conflict { conflict } => {
                    Ok(DfsConfirmedTimeoutOutcome::Conflict { conflict })
                }
            },
            DfsConfirmedTimeoutAction::Abort => {
                self.abort_pending_write(intent_id)?;
                Ok(DfsConfirmedTimeoutOutcome::Aborted {
                    intent_id: intent_id.to_string(),
                })
            }
            DfsConfirmedTimeoutAction::DowngradeToEventual {
                min_commit_replicas,
            } => {
                let intent = self.coordinator.intent(intent_id).cloned().ok_or_else(|| {
                    DfsError::Distributed {
                        message: format!("unknown intent: {intent_id}"),
                    }
                })?;
                let policy =
                    DfsReplicationPolicy::new(min_commit_replicas, intent.policy.desired_replicas)?;
                let updated_intent = self
                    .coordinator
                    .downgrade_intent_policy(intent_id, policy)?;
                let content_hash = self
                    .pending_writes
                    .get(intent_id)
                    .map(|pending| pending.content_hash.clone());
                self.put_metadata_intent(&updated_intent, content_hash.as_deref())?;
                self.try_apply_committed_intent_if_ready(intent_id)?;
                Ok(DfsConfirmedTimeoutOutcome::Downgraded(
                    self.write_outcome(intent_id)?,
                ))
            }
        }
    }

    fn current_object_for_write(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let memory_object = self.metadata.object(object_id).cloned();
        if let Some(memory_object) = memory_object.as_ref() {
            // Writes must not race an in-flight cross-shard move of the
            // object's path; the move would fork the object between the
            // source and destination shards.
            self.ensure_object_not_in_cross_shard_flight(memory_object)?;
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(backend_object) = backend.get_object(object_id)?
        {
            self.ensure_object_not_in_cross_shard_flight(&backend_object)?;
            if let Some(memory_object) = memory_object.as_ref()
                && self.local_write_view_overrides_backend(
                    object_id,
                    memory_object,
                    Some(&backend_object),
                )
            {
                return Ok(memory_object.clone());
            }
            self.metadata.add_object(backend_object.clone())?;
            return Ok(backend_object);
        }
        if let Some(memory_object) = memory_object {
            return Ok(memory_object);
        }
        self.metadata
            .object(object_id)
            .cloned()
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })
    }

    pub fn abort_pending_write(
        &mut self,
        intent_id: &str,
    ) -> Result<DistributedWriteIntent, DfsError> {
        let content_hash = self
            .pending_writes
            .get(intent_id)
            .map(|pending| pending.content_hash.clone());
        let intent = self.coordinator.abort_intent(intent_id)?;
        self.put_metadata_intent(&intent, content_hash.as_deref())?;
        self.remove_pending_write_state(intent_id);
        Ok(intent)
    }

    pub fn handoff_object_owner(
        &mut self,
        object_id: &DfsObjectId,
        shard_id: &str,
        expected_epoch: u64,
    ) -> Result<DfsOwnerHandoffOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let previous_owner_node_id = object.owner_node_id.clone();
        let Some(expected_hash) = object.content_hash.as_deref() else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason:
                    "candidate cannot prove latest content for an object without a visible hash"
                        .to_string(),
            });
        };
        let manifest = match self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return Ok(DfsOwnerHandoffOutcome::Rejected {
                    reason: format!("candidate is missing latest manifest: {error}"),
                });
            }
        };
        if manifest.content_hash != expected_hash {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "candidate manifest hash does not match visible object hash".to_string(),
            });
        }

        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "owner handoff requires control-plane shard fencing".to_string(),
            });
        };
        let transferred = DfsControlPlaneStore::new(kv.as_mut()).transfer_metadata_shard_primary(
            &object.file_system_id,
            shard_id,
            expected_epoch,
            self.node_id.clone(),
        )?;
        let Some(transferred) = transferred else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "control-plane epoch compare failed".to_string(),
            });
        };

        let file_system_to_persist =
            if let Some(file_system) = self.metadata.filesystems.get_mut(&object.file_system_id) {
                file_system.owner_node_id = self.node_id.clone();
                Some(file_system.clone())
            } else {
                None
            };
        if let Some(file_system) = file_system_to_persist.as_ref() {
            if let Some(backend) = self.metadata_backend.as_mut() {
                backend.ensure_file_system(file_system)?;
            }
            if let Some(kv) = self.control_plane.as_mut() {
                DfsControlPlaneStore::new(kv.as_mut())
                    .put_file_system_config(&DfsFileSystemConfig::from(file_system))?;
            }
        }

        let mut updated_object = object.clone();
        updated_object.owner_node_id = self.node_id.clone();
        if !updated_object.current_locations.contains(&self.node_id) {
            updated_object.current_locations.push(self.node_id.clone());
        }
        self.metadata
            .objects
            .insert(updated_object.object_id.clone(), updated_object.clone());
        self.put_visible_object_in_metadata_backend(&updated_object)?;

        Ok(DfsOwnerHandoffOutcome::Transferred {
            file_system_id: updated_object.file_system_id,
            object_id: updated_object.object_id,
            previous_owner_node_id,
            new_owner_node_id: self.node_id.clone(),
            previous_epoch: expected_epoch,
            new_epoch: transferred.epoch,
        })
    }

    fn select_metadata_shard_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        DfsControlPlaneStore::new(kv.as_mut())
            .select_metadata_shard_for_path(&object.file_system_id, &object.path)
            .map_err(DfsError::from)
    }

    fn ensure_intent_epoch_current(
        &mut self,
        intent: &DistributedWriteIntent,
    ) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(&intent.object_id)?;
        let Some(shard) = self.select_metadata_shard_for_object(&object)? else {
            return Ok(());
        };
        if shard.primary_node_id != self.node_id || shard.epoch != intent.owner_epoch {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: intent.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        Ok(())
    }

    fn apply_committed_intent(&mut self, intent_id: &str) -> Result<(), DfsError> {
        self.apply_committed_intent_with_repair_mode(intent_id, true)
    }

    fn apply_committed_intent_with_repair_mode(
        &mut self,
        intent_id: &str,
        foreground_replica_repair: bool,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Err(DfsError::Distributed {
                message: format!("unknown committed intent: {intent_id}"),
            });
        };
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            self.clear_active_replica_stage(&intent.object_id, &intent.target_version);
            return Ok(());
        }
        if !self.coordinator.is_commit_ready(intent_id)? {
            return Err(DfsError::Distributed {
                message: format!("intent {intent_id} is not ready to commit"),
            });
        }
        self.ensure_intent_epoch_current(&intent)?;
        let pending =
            self.pending_writes
                .get(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing pending write for committed intent: {intent_id}"),
                })?;
        if intent.object_id != pending.object_id || intent.target_version != pending.version {
            return Err(DfsError::Distributed {
                message: format!(
                    "committed intent {intent_id} state does not match pending write: intent {} {}, pending {} {}",
                    intent.object_id, intent.target_version, pending.object_id, pending.version
                ),
            });
        }
        let durable_receipts: Vec<DistributedReplicaReceipt> = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .cloned()
            .collect();
        for receipt in durable_receipts {
            self.put_metadata_receipt(
                &intent.file_system_id,
                &receipt.object_id,
                &receipt.version,
                &receipt.replica_node_id,
                &pending.content_hash,
            )?;
        }
        let write_time = dfs_now_unix_nanos();
        self.commit_visible_version_in_metadata_backend(&intent, &pending, write_time)?;
        self.coordinator
            .commit_intent(intent_id)
            .map_err(DfsError::from)?;
        let committed_intent =
            self.coordinator
                .intent(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing committed intent after finalize: {intent_id}"),
                })?;
        self.put_metadata_intent(&committed_intent, Some(&pending.content_hash))?;
        self.commit_local_metadata(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
            write_time,
        )?;
        self.clear_active_replica_stage(&pending.object_id, &pending.version);

        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        if let Some(object) = self.metadata.objects.get_mut(&pending.object_id) {
            object.current_locations = durable_nodes.iter().cloned().collect();
        }

        let commit_targets: Vec<NodeId> = durable_nodes
            .iter()
            .filter(|node_id| *node_id != &self.node_id && *node_id != &intent.requesting_node_id)
            .cloned()
            .collect();
        let committed_metadata = Some(self.current_read_object_metadata(&pending.object_id)?);
        let authority_shard =
            self.command_authority_shard_for_object(committed_metadata.as_ref())?;
        for replica_node_id in commit_targets {
            let request_id = self.next_request_id();
            self.send_best_effort_message_if_routable(
                &replica_node_id,
                DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: intent.file_system_id.clone(),
                    object_id: pending.object_id.clone(),
                    expected_base_version: intent.base_version.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    object_metadata: committed_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
        }

        if !foreground_replica_repair {
            return Ok(());
        }

        let repair_jobs: Vec<DistributedRepairJob> = self
            .coordinator
            .repair_jobs()
            .iter()
            .filter(|job| job.intent_id == intent_id)
            .cloned()
            .collect();
        if !repair_jobs.is_empty() && !self.pending_repair_is_locally_authoritative(&pending)? {
            self.suppress_repair_retries_for_lost_authority(intent_id, None, true);
            return Ok(());
        }
        for job in repair_jobs {
            let replica_node_id = job.missing_replica_node_id.clone();
            if !self.repair_retry_is_due(intent_id, &replica_node_id) {
                continue;
            }
            if replica_node_id == self.node_id {
                if self
                    .local_object_version_manifest_size_if_matches(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )
                    .is_some()
                {
                    self.record_local_pending_write_receipt_if_desired(
                        intent_id,
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    )?;
                    continue;
                }
                let fetch_requested = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_requested {
                    self.record_repair_retry_scheduled(intent_id, &replica_node_id);
                }
                continue;
            }
            if self.open_pending_write_blob_stream(&pending).is_err() {
                let fetch_requested = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_requested {
                    self.record_repair_retry_scheduled(intent_id, &replica_node_id);
                }
                continue;
            }
            let start_chunk_index = pending
                .repair_next_chunk_indexes
                .get(&replica_node_id)
                .copied()
                .unwrap_or(0);
            let request_id = pending
                .repair_inflight_request_ids
                .get(&replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let max_chunks = self.adaptive_stage_chunk_window(&replica_node_id, pending.size_bytes);
            let outcome = self.send_repair_replica_window_if_routable(
                &replica_node_id,
                request_id.clone(),
                intent_id,
                &intent.file_system_id,
                &pending.object_id,
                intent.base_version.clone(),
                &pending.version,
                &pending.content_hash,
                pending.size_bytes,
                start_chunk_index,
                max_chunks,
                committed_metadata.clone(),
            )?;
            if outcome.sent {
                self.record_repair_window_progress(
                    intent_id,
                    &replica_node_id,
                    request_id,
                    outcome,
                );
            } else {
                self.record_repair_retry_scheduled(intent_id, &replica_node_id);
            }
        }
        Ok(())
    }

    fn try_apply_committed_intent_if_ready(&mut self, intent_id: &str) -> Result<(), DfsError> {
        self.try_apply_committed_intent_if_ready_with_repair_mode(intent_id, true)
    }

    fn try_apply_committed_intent_if_ready_with_repair_mode(
        &mut self,
        intent_id: &str,
        foreground_replica_repair: bool,
    ) -> Result<(), DfsError> {
        if self.coordinator.is_commit_ready(intent_id)? {
            self.apply_committed_intent_with_repair_mode(intent_id, foreground_replica_repair)?;
        }
        Ok(())
    }

    fn normalize_desired_replica_nodes(
        &self,
        object: &DfsObjectMetadata,
        desired_replica_nodes: Vec<NodeId>,
    ) -> Vec<NodeId> {
        let mut normalized = desired_replica_nodes;
        if matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) && !normalized.contains(&object.owner_node_id)
        {
            normalized.insert(0, object.owner_node_id.clone());
        }
        let mut deduped = Vec::new();
        for node_id in normalized {
            if !deduped.contains(&node_id) {
                deduped.push(node_id);
            }
        }
        deduped
    }

    fn ensure_no_backend_pending_write_intent(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        excluding_intent_id: Option<&str>,
    ) -> Result<(), DfsDistributedError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let mut intent_cursor = None;
        loop {
            let page = backend
                .intents_for_object_page(
                    object_id,
                    intent_cursor.as_ref(),
                    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
                )
                .map_err(|error| DfsDistributedError::InvalidPolicy {
                    reason: error.to_string(),
                })?;
            for intent in page.intents {
                if excluding_intent_id == Some(intent.intent_id.as_str()) {
                    continue;
                }
                if matches!(
                    intent.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) {
                    return Err(DfsDistributedError::PendingIntentConflict {
                        object_id: object_id.clone(),
                        pending_intent_id: intent.intent_id.clone(),
                        pending_target_version: intent.target_version.clone(),
                        requested_target_version: target_version.clone(),
                    });
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn record_active_replica_stage(
        &mut self,
        object_id: &DfsObjectId,
        intent_id: &str,
        version: &DfsVersion,
        content_hash: &str,
        requester_node_id: &NodeId,
    ) {
        self.active_replica_stages.insert(
            object_id.clone(),
            ActiveReplicaStage {
                intent_id: intent_id.to_string(),
                version: version.clone(),
                content_hash: content_hash.to_string(),
                requester_node_id: requester_node_id.clone(),
            },
        );
    }

    fn clear_active_replica_stage(&mut self, object_id: &DfsObjectId, version: &DfsVersion) {
        if self
            .active_replica_stages
            .get(object_id)
            .is_some_and(|stage| &stage.version == version)
        {
            self.active_replica_stages.remove(object_id);
        }
    }

    fn restore_pending_write_for_intent_if_missing(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        if self.pending_writes.contains_key(intent_id) {
            return Ok(());
        }
        let Some(record) = (match self.metadata_backend.as_ref() {
            Some(backend) => backend.get_intent(object_id, intent_id)?,
            None => None,
        }) else {
            let _ = self
                .coordinator
                .intent(intent_id)
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("unknown pending write intent: {intent_id}"),
                })?;
            return Ok(());
        };
        if self.coordinator.intent(intent_id).is_none() {
            self.coordinator
                .restore_intent(distributed_intent_from_record(&record))?;
            if let Some(backend) = self.metadata_backend.as_ref() {
                let mut receipt_cursor = None;
                loop {
                    let page = backend.receipts_for_version_page(
                        &record.object_id,
                        &record.target_version,
                        receipt_cursor.as_ref(),
                        DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                    )?;
                    for receipt in page.receipts {
                        if let Some(expected_hash) = record.content_hash.as_deref()
                            && receipt.content_hash.as_deref() != Some(expected_hash)
                        {
                            continue;
                        }
                        match self
                            .coordinator
                            .restore_durable_receipt(intent_id, receipt.replica_node_id)
                        {
                            Ok(()) => {}
                            Err(
                                DfsDistributedError::UnexpectedReplica { .. }
                                | DfsDistributedError::IntentClosed { .. },
                            ) => continue,
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let Some(next_cursor) = page.next_cursor else {
                        break;
                    };
                    receipt_cursor = Some(next_cursor);
                }
            }
        }
        let intent =
            self.coordinator
                .intent(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("unknown pending write intent: {intent_id}"),
                })?;
        if !distributed_intent_matches_record(&intent, &record) {
            return Err(DfsError::Distributed {
                message: format!(
                    "stored intent record does not match coordinator state for {intent_id}"
                ),
            });
        }
        if self
            .current_read_object_metadata(&record.object_id)
            .is_err()
            && let Some(object) = record.object_metadata.clone()
        {
            self.add_transient_object_metadata(object)?;
        }
        let Some(content_hash) = record.content_hash.clone() else {
            return Ok(());
        };
        let is_local_coordinator = record.coordinator_node_id == self.node_id;
        let local_manifest_size = self.local_object_version_manifest_size_if_matches(
            &record.object_id,
            &record.target_version,
            &content_hash,
        );
        let is_local_desired_replica_with_content = record
            .desired_replica_node_ids
            .iter()
            .any(|node_id| node_id == &self.node_id)
            && local_manifest_size.is_some();
        let has_repair_job = self
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.intent_id == intent_id);
        let is_local_authoritative_repair = if has_repair_job {
            let repair_object = self.current_read_object_metadata(&record.object_id)?;
            self.is_local_metadata_primary_for_object(&repair_object)?
        } else {
            false
        };
        if !is_local_coordinator
            && !is_local_desired_replica_with_content
            && !is_local_authoritative_repair
        {
            return Ok(());
        }
        let size_bytes = self
            .pending_write_size_hint(&record.object_id, &record.target_version, &content_hash)?
            .or(local_manifest_size)
            .unwrap_or(0);
        self.pending_writes.insert(
            intent_id.to_string(),
            PendingNetworkedWrite::new(
                record.object_id,
                record.target_version,
                content_hash,
                size_bytes,
            ),
        );
        Ok(())
    }

    fn restore_pending_writes_for_repair_jobs(&mut self) -> Result<(), DfsError> {
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        for job in repair_jobs {
            let object = self.current_read_object_metadata(&job.object_id)?;
            if self.is_local_metadata_primary_for_object(&object)? {
                self.restore_pending_write_for_intent_if_missing(&job.intent_id, &job.object_id)?;
            }
        }
        Ok(())
    }

    fn commit_local_metadata(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        write_time_unix_nanos: u64,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let content_size_bytes = self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
            .ok()
            .filter(|manifest| manifest.content_hash == content_hash)
            .map(|manifest| manifest.size_bytes);
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(backend_object) = backend.get_object(object_id)?
        {
            self.metadata.add_object(backend_object)?;
        }
        let object =
            self.metadata
                .objects
                .get_mut(object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
        let content_unchanged = object
            .content_hash
            .as_ref()
            .is_some_and(|existing| existing == content_hash);
        let already_current = object.version == *version && content_unchanged;
        object.version = version.clone();
        object.content_hash = Some(content_hash.to_string());
        if content_size_bytes.is_some() {
            object.content_size_bytes = content_size_bytes;
        }
        if !content_unchanged {
            object.modified_at_unix_nanos = write_time_unix_nanos;
            object.changed_at_unix_nanos = write_time_unix_nanos;
        }
        if !already_current {
            object.last_modified_by_event_id = Some(EventId::generate());
        }
        if !object.current_locations.contains(&self.node_id) {
            object.current_locations.push(self.node_id.clone());
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        let object = object.clone();
        self.track_metadata_tombstone(&object);
        Ok(object.clone())
    }

    fn store_conflicting_write_content_hash(
        &self,
        request: &DfsWriteRequest,
        bytes: &[u8],
    ) -> Result<Option<String>, DfsError> {
        match self.content.put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            bytes,
        ) {
            Ok(manifest) => Ok(Some(manifest.content_hash)),
            Err(ContentStoreError::VersionHashConflict { new_hash, .. }) => Ok(Some(new_hash)),
            Err(error) => Err(error.into()),
        }
    }

    fn store_conflicting_write_reader_content_hash<R: Read>(
        &self,
        request: &DfsWriteRequest,
        reader: R,
    ) -> Result<Option<String>, DfsError> {
        match self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        ) {
            Ok(manifest) => Ok(Some(manifest.content_hash)),
            Err(ContentStoreError::VersionHashConflict { new_hash, .. }) => Ok(Some(new_hash)),
            Err(error) => Err(error.into()),
        }
    }

    fn ensure_replicated_confirmed_object(&self, object_id: &DfsObjectId) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedConfirmed {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }
        Ok(())
    }

    fn confirmed_locations_for_object(
        &self,
        object: &DfsObjectMetadata,
        required_replica_node_ids: &[NodeId],
    ) -> Result<Vec<NodeId>, DfsError> {
        let mut confirmed_locations: BTreeSet<NodeId> = object
            .current_locations
            .iter()
            .filter(|node_id| required_replica_node_ids.contains(node_id))
            .cloned()
            .collect();

        if let Some(backend) = self.metadata_backend.as_ref() {
            confirmed_locations.clear();
            let mut receipt_cursor = None;
            loop {
                let page = backend.receipts_for_version_page(
                    &object.object_id,
                    &object.version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                for receipt in page.receipts {
                    if !required_replica_node_ids.contains(&receipt.replica_node_id) {
                        continue;
                    }
                    if let Some(expected_hash) = object.content_hash.as_deref() {
                        if receipt.content_hash.as_deref() != Some(expected_hash) {
                            continue;
                        }
                    }
                    confirmed_locations.insert(receipt.replica_node_id);
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
        }

        Ok(confirmed_locations.into_iter().collect())
    }

    fn mark_repaired_location(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let mut object = self.current_read_object_metadata(object_id)?;
        if &object.version != version {
            return Ok(());
        }
        if let Some(expected_hash) = object.content_hash.as_deref() {
            if expected_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: content_hash.to_string(),
                });
            }
        }
        if object.current_locations.contains(replica_node_id) {
            self.metadata.add_object(object.clone())?;
            self.put_visible_object_in_metadata_backend(&object)?;
            return Ok(());
        }
        object.current_locations.push(replica_node_id.clone());
        self.metadata.add_object(object.clone())?;
        self.put_visible_object_in_metadata_backend(&object)?;
        Ok(())
    }

    fn put_metadata_receipt(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        self.ensure_metadata_receipt_accepts(object_id, version, replica_node_id, content_hash)?;
        let object = self.current_read_object_metadata(object_id).ok();
        let inserted = {
            let Some(backend) = self.metadata_backend.as_mut() else {
                return Ok(false);
            };
            let mut existing_matching_receipt = false;
            let mut receipt_cursor = None;
            loop {
                let page = backend.receipts_for_version_page(
                    object_id,
                    version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                for receipt in page.receipts {
                    if receipt.replica_node_id != *replica_node_id {
                        continue;
                    }
                    if receipt.content_hash.as_deref() == Some(content_hash) {
                        existing_matching_receipt = true;
                    }
                    break;
                }
                if existing_matching_receipt {
                    break;
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
            if existing_matching_receipt {
                false
            } else {
                let receipt = DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        committed_version: version.clone(),
                        owner_node_id: self.node_id.clone(),
                        event_id: EventId::generate(),
                    },
                    replica_node_id: replica_node_id.clone(),
                    content_hash: Some(content_hash.to_string()),
                    received_at: OffsetDateTime::now_utc(),
                };
                backend.put_receipt_for_object(object.as_ref(), &receipt)?;
                true
            }
        };
        match self.mark_repaired_location(object_id, version, replica_node_id, content_hash) {
            Ok(()) | Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(inserted)
    }

    fn ensure_metadata_receipt_accepts(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let mut receipt_cursor = None;
        loop {
            let page = backend.receipts_for_version_page(
                object_id,
                version,
                receipt_cursor.as_ref(),
                DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
            )?;
            for receipt in page.receipts {
                if receipt.replica_node_id != *replica_node_id {
                    continue;
                }
                if receipt.content_hash.as_deref() == Some(content_hash) {
                    return Ok(());
                }
                return Err(DfsError::Distributed {
                    message: format!(
                        "conflicting DFS replica receipt for {object_id} {version} from {replica_node_id}: existing {:?}, new {content_hash}",
                        receipt.content_hash
                    ),
                });
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn put_metadata_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: Option<&str>,
    ) -> Result<(), DfsError> {
        let object = self.current_object_for_write(&intent.object_id).ok();
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let mut record = write_intent_record(intent, content_hash, object.clone());
        if let Some(existing) = backend.get_intent(&intent.object_id, &intent.intent_id)? {
            record.created_at = existing.created_at;
            let mut comparable_record = record.clone();
            comparable_record.updated_at = existing.updated_at;
            if comparable_record == existing {
                return Ok(());
            }
        }
        backend.put_intent_for_object(object.as_ref(), &record)?;
        Ok(())
    }

    fn put_metadata_intent_and_receipts(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        replica_node_ids: &[NodeId],
    ) -> Result<(), DfsError> {
        let object = self.current_object_for_write(&intent.object_id).ok();
        for replica_node_id in replica_node_ids {
            self.ensure_metadata_receipt_accepts(
                &intent.object_id,
                &intent.target_version,
                replica_node_id,
                content_hash,
            )?;
        }

        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let owner_node_id = self.node_id.clone();
        let mut intent_record = write_intent_record(intent, Some(content_hash), object.clone());
        if let Some(existing) = backend.get_intent(&intent.object_id, &intent.intent_id)? {
            intent_record.created_at = existing.created_at;
            let mut comparable_record = intent_record.clone();
            comparable_record.updated_at = existing.updated_at;
            if comparable_record == existing && replica_node_ids.is_empty() {
                return Ok(());
            }
        }
        let receipts = replica_node_ids
            .iter()
            .map(|replica_node_id| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    committed_version: intent.target_version.clone(),
                    owner_node_id: owner_node_id.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica_node_id.clone(),
                content_hash: Some(content_hash.to_string()),
                received_at: OffsetDateTime::now_utc(),
            })
            .collect::<Vec<_>>();
        backend.put_intent_and_receipts_for_object(object.as_ref(), &intent_record, &receipts)?;
        for replica_node_id in replica_node_ids {
            match self.mark_repaired_location(
                &intent.object_id,
                &intent.target_version,
                replica_node_id,
                content_hash,
            ) {
                Ok(()) | Err(DfsError::UnknownObject { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn put_visible_object_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let mut object = object.clone();
        normalize_current_locations(&mut object.current_locations);
        let size_bytes = self.object_content_size_hint(&object)?;
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let file_system = backend
            .get_file_system(&object.file_system_id)?
            .unwrap_or_else(|| DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            });
        backend.ensure_file_system(&file_system)?;
        if backend.get_object(&object.object_id)?.as_ref() != Some(&object) {
            backend.put_object(&object)?;
        }
        put_visible_version_record_if_changed(
            backend.as_mut(),
            object_version_record(&object, size_bytes),
        )?;
        Ok(())
    }

    fn put_visible_object_and_version_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        let mut object = object.clone();
        normalize_current_locations(&mut object.current_locations);
        let size_bytes = match size_bytes {
            Some(size_bytes) => Some(size_bytes),
            None => self.object_content_size_hint(&object)?,
        };
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let file_system = backend
            .get_file_system(&object.file_system_id)?
            .unwrap_or_else(|| DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            });
        backend.ensure_file_system(&file_system)?;

        let object_changed = backend.get_object(&object.object_id)?.as_ref() != Some(&object);

        let mut version = object_version_record(&object, size_bytes);
        let version_changed =
            if let Some(existing) = backend.get_version(&version.object_id, &version.version)? {
                version.created_at = existing.created_at;
                existing != version
            } else {
                true
            };

        match (object_changed, version_changed) {
            (true, true) => {
                backend.put_object_and_version(&object, &version)?;
            }
            (true, false) => {
                backend.put_object(&object)?;
            }
            (false, true) => {
                backend.put_version(&version)?;
            }
            (false, false) => {}
        }
        Ok(())
    }

    fn put_object_version_size_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        put_visible_version_record_if_changed(
            backend.as_mut(),
            object_version_record(object, size_bytes),
        )?;
        Ok(())
    }

    fn commit_visible_version_in_metadata_backend(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        write_time_unix_nanos: u64,
    ) -> Result<(), DfsError> {
        let expected_object = self.current_object_for_write(&pending.object_id)?;
        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        let durable_locations: Vec<NodeId> = durable_nodes.iter().cloned().collect();
        if expected_object.version == pending.version
            && expected_object.content_hash.as_deref() == Some(pending.content_hash.as_str())
        {
            let mut current_locations = expected_object.current_locations.clone();
            normalize_current_locations(&mut current_locations);
            let size_is_current = pending.size_bytes == 0
                || expected_object.content_size_bytes == Some(pending.size_bytes);
            if current_locations == durable_locations && size_is_current {
                return Ok(());
            }
            let mut repaired_object = expected_object.clone();
            if pending.size_bytes != 0 {
                repaired_object.content_size_bytes = Some(pending.size_bytes);
            }
            repaired_object.current_locations = durable_locations;
            self.put_visible_object_in_metadata_backend(&repaired_object)?;
            self.metadata.add_object(repaired_object)?;
            return Ok(());
        }
        let mut committed_object = expected_object.clone();
        committed_object.version = pending.version.clone();
        committed_object.content_hash = Some(pending.content_hash.clone());
        committed_object.content_size_bytes = Some(pending.size_bytes);
        committed_object.modified_at_unix_nanos = write_time_unix_nanos;
        committed_object.changed_at_unix_nanos = write_time_unix_nanos;
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        committed_object.current_locations = durable_locations;
        let visible_version = object_version_record(&committed_object, Some(pending.size_bytes));
        let required_replica_node_ids: Vec<NodeId> = durable_nodes.into_iter().collect();
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        if backend.get_object(&pending.object_id)?.is_none() {
            backend.apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: namespace_transaction_id_for_object(
                    "create-visible",
                    &pending.object_id,
                    &pending.version,
                ),
                file_system_id: committed_object.file_system_id.clone(),
                objects: vec![committed_object.clone()],
                versions: vec![visible_version],
                created_at: OffsetDateTime::now_utc(),
            })?;
            return Ok(());
        }
        backend.commit_visible_version(
            &expected_object.version,
            &committed_object,
            &visible_version,
            &required_replica_node_ids,
        )?;
        Ok(())
    }

    fn commit_remote_manifest_ready_intent_batched(
        &mut self,
        intent: &DistributedWriteIntent,
        base_object: &DfsObjectMetadata,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<(), DfsError> {
        let trace_started = Instant::now();
        let trace_intent_id = intent.intent_id.clone();
        let trace_object_id = intent.object_id.clone();
        let trace_version = intent.target_version.clone();
        if !self.coordinator.is_commit_ready(&intent.intent_id)? {
            return Err(DfsError::Distributed {
                message: format!("intent {} is not ready to commit", intent.intent_id),
            });
        }
        let prepare_started = Instant::now();
        self.ensure_intent_epoch_current(intent)?;
        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        let write_time = dfs_now_unix_nanos();
        let mut committed_object = base_object.clone();
        committed_object.version = intent.target_version.clone();
        committed_object.content_hash = Some(content_hash.to_string());
        committed_object.content_size_bytes = Some(content_size_bytes);
        committed_object.modified_at_unix_nanos = write_time;
        committed_object.changed_at_unix_nanos = write_time;
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        committed_object.current_locations = durable_nodes.iter().cloned().collect();
        let visible_version = object_version_record(&committed_object, Some(content_size_bytes));

        let mut committed_intent = intent.clone();
        committed_intent.state = DfsWriteIntentState::Committed;
        let intent_record = write_intent_record(
            &committed_intent,
            Some(content_hash),
            Some(committed_object.clone()),
        );
        let receipts = durable_nodes
            .iter()
            .map(|replica_node_id| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    committed_version: intent.target_version.clone(),
                    owner_node_id: self.node_id.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica_node_id.clone(),
                content_hash: Some(content_hash.to_string()),
                received_at: OffsetDateTime::now_utc(),
            })
            .collect::<Vec<_>>();
        let prepare_ms = prepare_started.elapsed().as_millis();
        let backend_started = Instant::now();
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_object_version_intent_and_receipts(
                &committed_object,
                &visible_version,
                &intent_record,
                &receipts,
            )?;
        }
        let backend_ms = backend_started.elapsed().as_millis();
        let coordinator_started = Instant::now();
        self.coordinator
            .commit_intent(&intent.intent_id)
            .map_err(DfsError::from)?;
        let coordinator_ms = coordinator_started.elapsed().as_millis();
        let memory_started = Instant::now();
        self.track_metadata_tombstone(&committed_object);
        self.metadata.add_object(committed_object)?;
        self.clear_active_replica_stage(&intent.object_id, &intent.target_version);
        let memory_ms = memory_started.elapsed().as_millis();
        dfs_perf_trace(format_args!(
            "remote-manifest-commit-batched intent={} object={} version={} receipts={} prepare_ms={} backend_ms={} coordinator_ms={} memory_ms={} total_ms={}",
            trace_intent_id,
            trace_object_id,
            trace_version,
            receipts.len(),
            prepare_ms,
            backend_ms,
            coordinator_ms,
            memory_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(())
    }

    fn store_command_bytes(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let actual_hash = content_hash(bytes);
        if actual_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash,
            });
        }
        let manifest =
            self.content
                .put_object_version(object_id.as_str(), version.as_str(), bytes)?;
        if manifest.content_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        Ok(())
    }

    fn store_command_patch(
        &self,
        object_id: &DfsObjectId,
        base_version: &DfsVersion,
        target_version: &DfsVersion,
        expected_hash: &str,
        offset: u64,
        expected_patch_hash: &str,
        bytes: &[u8],
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<(), DfsError> {
        let actual_patch_hash = content_hash(bytes);
        if actual_patch_hash != expected_patch_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_patch_hash.to_string(),
                actual: actual_patch_hash,
            });
        }
        let manifest = self
            .content
            .put_object_version_patch_with_info_and_base_identity(
                object_id.as_str(),
                base_version.as_str(),
                target_version.as_str(),
                offset,
                bytes,
                base_identity,
            )?
            .manifest;
        if manifest.content_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        Ok(())
    }

    fn ensure_replica_object_metadata(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let Some(object) = object else {
            return Ok(());
        };
        let persist_authoritative_metadata =
            self.local_metadata_backend_is_authoritative_for_object(object)?;
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            let file_system = DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            };
            if persist_authoritative_metadata {
                self.add_file_system(file_system)?;
            } else {
                self.metadata.add_file_system(file_system);
            }
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(existing) = backend.get_object(&object.object_id)?
        {
            self.metadata.add_object(existing)?;
        } else if self.metadata.object(&object.object_id).is_none() {
            if persist_authoritative_metadata {
                self.add_object_metadata(object.clone())?;
            } else {
                self.add_transient_object_metadata(object.clone())?;
            }
        }
        Ok(())
    }

    fn command_authority_shard_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(object) = object else {
            return Ok(None);
        };
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            return Ok(None);
        };
        Ok((shard.primary_node_id == self.node_id).then_some(shard))
    }

    fn local_metadata_backend_is_authoritative_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<bool, DfsError> {
        Ok(self
            .select_metadata_shard_for_object(object)?
            .is_none_or(|shard| shard.primary_node_id == self.node_id))
    }

    fn command_authority_shard_proves_metadata_primary(
        &self,
        requester_node_id: &NodeId,
        object: &DfsObjectMetadata,
        local_shard: Option<&DfsMetadataShardConfig>,
        authority_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<bool, DfsError> {
        let Some(authority_shard) = authority_shard else {
            return Ok(false);
        };
        if authority_shard.primary_node_id != *requester_node_id
            || authority_shard.file_system_id != object.file_system_id
        {
            return Ok(false);
        }
        let selected = crate::control_plane::select_metadata_shard_config_for_path(
            &object.file_system_id,
            std::slice::from_ref(authority_shard),
            &object.path,
        )
        .map_err(DfsError::from)?;
        if selected.as_ref() != Some(authority_shard) {
            return Ok(false);
        }
        if let Some(local_shard) = local_shard
            && authority_shard.epoch < local_shard.epoch
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn replica_command_metadata_is_authoritative(
        &mut self,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        let object = match self.current_read_object_metadata(object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?
            }
            Err(error) => return Err(error),
        };
        self.local_metadata_backend_is_authoritative_for_object(&object)
    }

    fn put_replica_receipt_if_authoritative(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        if self.replica_command_metadata_is_authoritative(object_id, object_metadata)? {
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        } else {
            self.ensure_metadata_receipt_accepts(
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        }
        Ok(())
    }

    fn commit_replica_metadata_cache(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let persist_authoritative_metadata =
            self.replica_command_metadata_is_authoritative(object_id, object_metadata)?;
        if persist_authoritative_metadata {
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        } else {
            self.ensure_metadata_receipt_accepts(
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        }
        let object =
            self.commit_local_metadata(object_id, version, content_hash, dfs_now_unix_nanos())?;
        if persist_authoritative_metadata {
            self.put_visible_object_in_metadata_backend(&object)?;
        }
        Ok(())
    }

    fn ensure_replica_command_authorized_with_metadata(
        &mut self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
        authority_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<(), DfsError> {
        let (object, object_from_command) = match self.current_read_object_metadata(object_id) {
            Ok(object) => (object, false),
            Err(DfsError::UnknownObject { .. }) => {
                let object = object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?;
                (object, true)
            }
            Err(error) => return Err(error),
        };
        let metadata_shard = self.select_metadata_shard_for_object(&object)?;
        let requester_has_supplied_metadata_authority = self
            .command_authority_shard_proves_metadata_primary(
                requester_node_id,
                &object,
                metadata_shard.as_ref(),
                authority_shard,
            )?;
        let requester_is_metadata_primary = metadata_shard
            .as_ref()
            .is_some_and(|shard| &shard.primary_node_id == requester_node_id)
            || requester_has_supplied_metadata_authority;
        if let Some(backend) = self.metadata_backend.as_ref() {
            if let Some(intent) = backend.get_intent(object_id, intent_id)? {
                let requester_has_intent_authority = &intent.coordinator_node_id
                    == requester_node_id
                    || (requester_is_metadata_primary && object_metadata.is_some());
                if !requester_has_intent_authority {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "source is not coordinator for DFS write intent {intent_id}"
                        ),
                    });
                }
                if !intent.desired_replica_node_ids.contains(&self.node_id) {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "local node is not a desired replica for DFS write intent {intent_id}"
                        ),
                    });
                }
                return Ok(());
            }
        }
        if requester_is_metadata_primary && object_metadata.is_some() {
            return Ok(());
        }

        if !object_from_command || metadata_shard.is_none() {
            if &object.owner_node_id == requester_node_id {
                return Ok(());
            }
            if matches!(
                object.policy,
                DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
            ) && object.current_locations.contains(requester_node_id)
            {
                return Ok(());
            }
        } else if object_metadata.is_some()
            && let Some(shard) = metadata_shard.as_ref()
        {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester_node_id.clone(),
                reason: format!(
                    "source is not current metadata shard primary for {}; primary is {} at epoch {}",
                    object.file_system_id, shard.primary_node_id, shard.epoch
                ),
            });
        }

        Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!(
                "source is neither object owner nor current replica for {object_id}; owner is {}",
                object.owner_node_id
            ),
        })
    }

    fn ensure_replica_command_hash_matches_intent(
        &self,
        intent_id: &str,
        object_id: &DfsObjectId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(intent) = backend.get_intent(object_id, intent_id)? else {
            return Ok(());
        };
        if let Some(expected_hash) = intent.content_hash.as_deref() {
            if expected_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: content_hash.to_string(),
                });
            }
        }
        Ok(())
    }

    fn ensure_blob_fetch_authorized(
        &mut self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        authority_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<(), DfsError> {
        let Some(intent) = ({
            let Some(backend) = self.metadata_backend.as_ref() else {
                return Ok(());
            };
            backend.get_intent(object_id, intent_id)?
        }) else {
            return Ok(());
        };
        let intent_object_metadata = intent.object_metadata.clone();
        let requester_is_metadata_primary = match self
            .current_read_object_metadata(object_id)
            .ok()
            .or(intent_object_metadata)
        {
            Some(object) => {
                let metadata_shard = self.select_metadata_shard_for_object(&object)?;
                metadata_shard
                    .as_ref()
                    .is_some_and(|shard| shard.primary_node_id == *requester_node_id)
                    || self.command_authority_shard_proves_metadata_primary(
                        requester_node_id,
                        &object,
                        metadata_shard.as_ref(),
                        authority_shard,
                    )?
            }
            None => false,
        };
        if &intent.coordinator_node_id != requester_node_id
            && !intent.desired_replica_node_ids.contains(requester_node_id)
            && !requester_is_metadata_primary
        {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester_node_id.clone(),
                reason: format!(
                    "source is not authorized to fetch DFS blob for intent {intent_id}"
                ),
            });
        }
        if !intent.desired_replica_node_ids.contains(&self.node_id) {
            let mut has_receipt = false;
            let mut receipt_cursor = None;
            loop {
                let Some(backend) = self.metadata_backend.as_ref() else {
                    break;
                };
                let page = backend.receipts_for_version_page(
                    object_id,
                    &intent.target_version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                if page
                    .receipts
                    .into_iter()
                    .any(|receipt| receipt.replica_node_id == self.node_id)
                {
                    has_receipt = true;
                    break;
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
            if !has_receipt {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!(
                        "local node is not an expected or durable replica for DFS intent {intent_id}"
                    ),
                });
            }
        }
        Ok(())
    }

    fn write_outcome(&self, intent_id: &str) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if let Some(intent) = self.coordinator.intent(intent_id)
            && matches!(intent.state, DfsWriteIntentState::Committed)
        {
            return self.local_write_outcome(intent_id, intent);
        }
        if let Some(pending) = self.pending_routed_writes.get(intent_id) {
            match &pending.result {
                Some(Ok(outcome)) => return Ok(outcome.clone()),
                Some(Err(error)) => return Err(error.clone()),
                None => {}
            }
            if pending.command.request_id != intent_id
                && let Some(canonical) = self.pending_routed_writes.get(&pending.command.request_id)
            {
                match &canonical.result {
                    Some(Ok(outcome)) => return Ok(outcome.clone()),
                    Some(Err(error)) => return Err(error.clone()),
                    None => {
                        return Ok(NetworkedDfsWriteOutcome::Pending {
                            intent_id: canonical
                                .remote_intent_id
                                .clone()
                                .unwrap_or_else(|| intent_id.to_string()),
                            durable_receipts: canonical.durable_receipts,
                            required_receipts: canonical.required_receipts,
                        });
                    }
                }
            }
            return match &pending.result {
                Some(Ok(_)) | Some(Err(_)) => unreachable!("pending result handled above"),
                None => Ok(NetworkedDfsWriteOutcome::Pending {
                    intent_id: pending
                        .remote_intent_id
                        .clone()
                        .unwrap_or_else(|| intent_id.to_string()),
                    durable_receipts: pending.durable_receipts,
                    required_receipts: pending.required_receipts,
                }),
            };
        }
        if let Some(intent) = self.coordinator.intent(intent_id) {
            return self.local_write_outcome(intent_id, intent);
        }
        Err(DfsError::Distributed {
            message: format!("unknown intent: {intent_id}"),
        })
    }

    fn local_write_outcome(
        &self,
        intent_id: &str,
        intent: &DistributedWriteIntent,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            let pending = self.pending_writes.get(intent_id);
            let content_hash = pending
                .map(|pending| pending.content_hash.clone())
                .or_else(|| {
                    self.metadata_backend
                        .as_ref()
                        .and_then(|backend| {
                            backend
                                .get_intent(&intent.object_id, intent_id)
                                .ok()
                                .flatten()
                        })
                        .and_then(|intent_record| intent_record.content_hash)
                })
                .or_else(|| {
                    self.current_read_object_metadata(&intent.object_id)
                        .ok()
                        .filter(|object| object.version == intent.target_version)
                        .and_then(|object| object.content_hash)
                })
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing content hash for committed intent: {intent_id}"),
                })?;
            Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: intent_id.to_string(),
                version: pending
                    .map(|pending| pending.version.clone())
                    .unwrap_or_else(|| intent.target_version.clone()),
                content_hash,
                repair_jobs: self
                    .coordinator
                    .repair_jobs()
                    .iter()
                    .filter(|job| job.intent_id == intent_id)
                    .cloned()
                    .collect(),
            })
        } else {
            let _pending =
                self.pending_writes
                    .get(intent_id)
                    .ok_or_else(|| DfsError::Distributed {
                        message: format!("missing pending write: {intent_id}"),
                    })?;
            Ok(NetworkedDfsWriteOutcome::Pending {
                intent_id: intent_id.to_string(),
                durable_receipts: self.coordinator.durable_receipt_count(intent_id),
                required_receipts: intent.policy.min_commit_replicas,
            })
        }
    }
}

#[cfg(test)]
mod tests;
