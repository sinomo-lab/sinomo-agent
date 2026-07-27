//! Distributed filesystem services, storage, protocol, and operator APIs.
//!
//! `sinomo-dfs` owns Sinomo filesystem semantics: SIFS bootstrap data,
//! metadata shards and journals, content storage, repair, replication, POSIX
//! operations, object-RPC routes, and the networked DFS service. The public
//! interface is intentionally exported from this crate root so callers do not
//! couple to the internal module layout.

mod advisory_lock;
mod body_rpc;
mod cluster_service;
mod content_store;
mod control_plane;
mod default_cluster;
mod direct_read;
mod discovery;
mod distributed;
mod error;
mod identity;
mod intent;
mod local_service;
mod local_state;
mod maintenance;
mod message_transport;
mod metadata_api;
mod metadata_backend;
mod metadata_cursor;
mod metadata_error;
mod metadata_hardlink;
mod metadata_io;
mod metadata_journal;
mod metadata_keys;
mod metadata_limits;
mod metadata_lock;
mod metadata_namespace_transaction;
mod metadata_object_index;
mod metadata_pages;
mod metadata_read_snapshot;
mod metadata_receipt;
mod metadata_records;
mod metadata_replay;
mod metadata_replication;
mod metadata_shard_index;
mod metadata_shard_migration;
mod metadata_shard_pages;
mod metadata_sharded_backend;
mod metadata_summary;
mod metadata_table_apply;
mod metadata_table_store;
mod metadata_tables;
mod metadata_tombstone;
mod metadata_visible_commit;
mod model;
mod module_surface;
mod mounted_sifs;
mod namespace;
mod node_transport;
mod operator;
mod performance_metrics;
mod placement;
mod posix;
mod posix_model;
mod posix_object;
mod posix_path;
mod protocol;
mod renewable_lease;
mod repair;
mod routes;
mod service;
mod service_async;
mod service_commit;
mod service_conflict;
mod service_content;
mod service_core;
mod service_fetch_transfer;
mod service_hydration;
mod service_maintenance;
mod service_metadata;
mod service_metadata_exchange;
mod service_namespace_coordination;
mod service_outbound;
mod service_ownership;
mod service_posix_coordination;
mod service_prepare_recovery;
mod service_protocol;
mod service_read;
mod service_read_transfer;
mod service_repair;
mod service_replica;
mod service_replica_transfer;
mod service_routed_journal;
mod service_routed_namespace;
mod service_routed_write;
mod service_stage_transfer;
mod service_state;
mod service_sync;
mod service_trace;
mod service_write;
mod smoke;
mod transfer;

pub use advisory_lock::{
    DFS_ADVISORY_LOCK_REQUEST_OUTCOME_LIMIT, DfsAdvisoryLockApplyResult, DfsAdvisoryLockCommand,
    DfsAdvisoryLockConflict, DfsAdvisoryLockGrant, DfsAdvisoryLockMode, DfsAdvisoryLockOutcome,
    DfsAdvisoryLockRequestId, DfsAdvisoryLockRequestOutcomeRecord, DfsAdvisoryLockSessionId,
    DfsAdvisoryLockStateRecord, DfsAdvisoryLockToken, DfsAdvisoryLockTransition,
    DfsAdvisoryLockTransitionContext, DfsAdvisoryLockTransitionError, DfsAdvisoryLockWaiter,
    transition_advisory_lock_state,
};
pub use body_rpc::{
    DfsAttachmentMessageCommand, DfsAttachmentMessageResponse, DfsRpcAttachmentDescriptor,
};
pub use cluster_service::cluster_dfs_peer_node_ids;
pub(crate) use cluster_service::{
    open_cluster_dfs_service_with_message_sender,
    open_cluster_dfs_service_with_message_sender_lazy,
    open_cluster_dfs_service_with_peer_transports_for_nodes,
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state,
};
pub(crate) use content_store::content_hash;
pub use content_store::{
    BlobRangeReader, BlobReader, ContentManifest, ContentStoreError, ContentStoreGcOptions,
    ContentStoreGcReport, ContentStoreQuarantinedBlob, ContentStoreQuota,
    ContentStoreQuotaResource, ContentStoreQuotaUsage, ContentStoreScrubIssue,
    ContentStoreScrubReport, ContentStoreStagingGcOptions, ContentStoreStagingGcReport,
    ContentStoreStats, ContentStoreVersionGcOptions, ContentStoreVersionGcReport,
    ContentVersionIdentity, LocalContentStore, ObjectVersionReader,
};
pub use control_plane::{
    AsyncDfsControlPlaneKvClient, DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT,
    DfsControlPlaneAsyncStore, DfsControlPlaneKvClient, DfsControlPlaneKvFuture,
    DfsControlPlaneStore, DfsFileSystemConfig, DfsMetadataShardAssignment, DfsMetadataShardConfig,
    DfsMetadataShardHeadWatermark, DfsMetadataShardMembershipHandoff,
    DfsMetadataShardMembershipHandoffFence, DfsMetadataShardMembershipHandoffProof,
    DfsPlacementAuthorityPermitHolderKind, DfsPlacementAuthorityPermitStatus,
    DfsPlacementAuthorityProcessIdentity, DfsPlacementAuthorityRecoveryOutcome,
    DfsPlacementAuthorityRecoveryRefusal, SharedDfsControlPlaneKvClient, file_system_config_key,
    metadata_shard_authority_barrier_key, metadata_shard_head_watermark_key, metadata_shard_key,
    metadata_shard_membership_handoff_key, metadata_shard_membership_handoff_proof_key,
    metadata_shards_prefix, placement_authority_permit_key,
    placement_reconciliation_activity_index_key, placement_reconciliation_key,
    placement_reconciliations_prefix,
};
#[cfg(test)]
pub(crate) use default_cluster::run_default_cluster_dfs_smoke;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use default_cluster::test_establish_default_cluster_dfs_replica_progress_subscription;
pub use default_cluster::{
    DfsControlPlaneDefaultClusterAsyncBootstrapHost, DfsControlPlaneDefaultClusterBootstrapHost,
    DfsDefaultClusterAsyncBootstrapHost, DfsDefaultClusterBootstrapFuture,
    DfsDefaultClusterBootstrapHost, DfsDefaultClusterBootstrapOptions,
    DfsDefaultClusterBootstrapReport, DfsDefaultClusterSmokeOptions,
    default_cluster_metadata_shard_with_async_host, default_cluster_metadata_shard_with_host,
    default_cluster_metadata_shards_with_async_host, default_cluster_metadata_shards_with_host,
    default_cluster_sifs, default_cluster_sifs_owner, default_cluster_sifs_subtree_policies,
    default_metadata_shard_primary_node_id, default_metadata_shard_replica_node_ids,
    dfs_service_members, ensure_default_cluster_dfs, ensure_default_cluster_dfs_async,
    ensure_default_distributed_dfs_with_async_host, ensure_default_distributed_dfs_with_host,
    invalidate_default_cluster_dfs_materialization_for_replica_progress,
    metadata_shard_layout_matches, metadata_shard_primary_transfer_ready_from_local_backend,
    metadata_shard_replica_node_ids, run_default_cluster_dfs_smoke_with_peer_transport,
    stable_dfs_service_node_ids, take_default_cluster_dfs_materialization_immediate_retry,
};
pub(crate) use direct_read::dfs_direct_read_request_from_read_request;
#[cfg(test)]
pub(crate) use direct_read::{
    DFS_DIRECT_READ_STATUS_FOUND, DFS_DIRECT_READ_STATUS_METADATA_KEY, dfs_direct_read_rpc_result,
};
pub use direct_read::{
    DfsDirectReadAttachmentResponse, DfsDirectReadClientData, DfsDirectReadClientResponse,
    DfsDirectReadClientResult, dfs_direct_read_response_from_rpc_result, dfs_direct_read_rpc_call,
    dfs_direct_read_rpc_request, handle_dfs_direct_read_rpc_request,
};
pub use discovery::{
    DfsClusterView, DfsNodeAdvertisement, content_service_key, content_services_prefix,
    metadata_service_key, metadata_services_prefix,
};
pub use distributed::{
    DfsDistributedError, DfsReplicaCandidate, DfsReplicaPlacementDecision,
    DfsReplicaPlacementExplanation, DfsReplicaPlacementPlan, DfsReplicaPlacementRequest,
    DfsReplicaReceiptState, DfsReplicationPolicy, DfsWriteIntentState,
    DistributedEventualCoordinator, DistributedRepairJob, DistributedReplicaReceipt,
    DistributedWriteIntent, explain_replicas, plan_replicas, validate_replication_plan,
};
pub use error::DfsError;
pub(crate) use error::{
    dfs_rejection_is_commit_content_repair_pending, dfs_rejection_is_replica_not_desired,
    dfs_replica_not_desired_reason, distributed_write_rejection_current_primary,
    namespace_rejection_current_primary, routed_write_rejection_error,
};
pub use identity::{
    DEFAULT_DFS_NODE_ID, SIFS_FILE_SYSTEM_ID, fallback_dfs_node_id, sifs_file_system_id,
};
pub(crate) use intent::{
    distributed_error_is_stale_or_concurrent_write, distributed_intent_from_record,
    distributed_intent_matches_record, distributed_intent_matches_routed_write_command,
    routed_write_command_content_hash, write_intent_record,
    write_intent_record_matches_routed_write_command, write_intent_status_requires_hydrate_restore,
};
pub use local_service::LocalOwnedDfsService;
pub(crate) use local_state::dfs_path_safe_segment;
pub use local_state::{
    DFS_IMPORTS_NAMESPACE_ROOT, add_local_dfs_pin, cluster_dfs_metadata_shards,
    default_dfs_metadata_root, default_dfs_metadata_shards_root, default_dfs_pin_root,
    dfs_import_metadata_root, dfs_import_namespace_root, dfs_import_root,
    inspect_cluster_aware_dfs, inspect_cluster_aware_dfs_status, inspect_dfs_conflicts,
    inspect_dfs_file_system, inspect_dfs_object, inspect_dfs_path, inspect_dfs_repairs,
    inspect_dfs_status, list_local_dfs_pins, local_dfs_metadata_shard_path,
    open_cluster_aware_dfs_metadata_backend,
    open_cluster_aware_dfs_metadata_backend_for_inspection,
    open_cluster_sharded_dfs_metadata_backend, open_local_metadata_shard_backend,
    open_local_sharded_metadata_backend, open_local_sharded_metadata_backend_for_replica_repair,
    open_root_local_metadata_backend, remove_local_dfs_pin, resolve_dfs_metadata_root,
    resolve_dfs_pin_root,
};
pub(crate) use maintenance::record_anti_entropy_issue;
pub use maintenance::{
    DEFAULT_DFS_CONFLICT_MAINTENANCE_MAX_RECORDS, DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_ISSUES,
    DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_OBJECTS,
    DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_JOBS,
    DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_STEPS_PER_JOB,
    DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE, DEFAULT_DFS_WRITE_INTENT_MAINTENANCE_MAX_RECORDS,
    DfsAntiEntropyCursorRecord, DfsAntiEntropyIssue, DfsAntiEntropyOptions, DfsAntiEntropyReport,
    DfsConflictMaintenanceReport, DfsLifecycleMaintenanceOptions, DfsLifecycleMaintenanceReport,
    DfsMaintenanceExecutionExclusions, DfsMaintenanceHost, DfsMaintenanceRunOptions,
    DfsMaintenanceTickObservations, DfsMaintenanceTickOptions, DfsMaintenanceTickReport,
    DfsMetadataReplicationSuppressionRecord, DfsMetadataReplicationSuppressionSnapshot,
    DfsRepairAttemptRecord, DfsRepairAttemptSnapshot, DfsWriteIntentMaintenanceReport,
    LegacyDfsMetadataReplicationCursor, LocalDfsMaintenanceHost, clear_dfs_anti_entropy_cursor,
    default_dfs_anti_entropy_cursor_path,
    default_dfs_metadata_replication_suppression_snapshot_path,
    default_dfs_repair_attempt_snapshot_path, dfs_events_should_wake_repair_maintenance,
    dfs_lifecycle_maintenance_notice_lines, dfs_maintenance_tick_observations,
    dfs_namespace_job_maintenance_notice_lines, dfs_repair_maintenance_notice_lines,
    dfs_write_intent_maintenance_notice_lines, legacy_dfs_anti_entropy_cursor_path,
    legacy_dfs_metadata_replication_cursor_path, migrate_legacy_dfs_metadata_replication_cursor,
    put_local_dfs_metadata_replication_suppression, read_dfs_anti_entropy_cursor,
    read_dfs_metadata_replication_suppression_snapshot, read_dfs_repair_attempt_snapshot,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions_and_committed_work_tracker,
    run_cluster_dfs_maintenance_tick_with_peer_transport, run_dfs_lifecycle_maintenance_with_host,
    run_dfs_maintenance_tick_with_async_host,
    run_dfs_maintenance_tick_with_async_host_and_execution_exclusions,
    run_dfs_maintenance_tick_with_host, write_dfs_anti_entropy_cursor,
    write_dfs_metadata_replication_suppression_snapshot, write_dfs_repair_attempt_snapshot,
};
#[cfg(test)]
pub(crate) use maintenance::{
    run_cluster_dfs_maintenance_tick, write_legacy_dfs_metadata_replication_cursor,
};
pub(crate) use message_transport::materialize_dfs_routed_call_sync;
pub use message_transport::{
    AsyncDfsMessageTransport, DfsMessageTransport, DfsMessageTransportFuture,
    DfsOutboundAttachment, DfsRoutedCall, DfsRoutedSend, DfsRoutedSendOutcome,
    DfsTransportIdentity,
};
pub use metadata_api::{
    DfsMetadataBackend, DfsMetadataReplicaIngestPermit, DfsMetadataShardWriteFreeze,
};
pub use metadata_backend::LocalSobjMetadataBackend;
pub use metadata_error::MetadataBackendError;
pub use metadata_limits::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
};
pub use metadata_records::{
    DfsConflictPage, DfsConflictPageCursor, DfsConflictRecord, DfsConflictScanCursor,
    DfsConflictScanPage, DfsConflictStatus, DfsDentryPageCursor, DfsDirFragmentId,
    DfsDirFragmentRecord, DfsDirectoryPage, DfsDirectoryPageCursor, DfsInodeId, DfsInodeRecord,
    DfsMetadataAuthorityAckFloor, DfsMetadataJournalBatch, DfsMetadataJournalEntry,
    DfsMetadataMutation, DfsMetadataPrefixAckFloor, DfsMetadataPrefixProof,
    DfsMetadataReplayCursorStatus, DfsMetadataReplicaLagStatus, DfsMetadataShardId,
    DfsMetadataShardMigrationReport, DfsMetadataShardStatus, DfsMetadataSubscription,
    DfsMetadataSummary, DfsMetadataTableSnapshot, DfsNamespaceJobFrame, DfsNamespaceJobKind,
    DfsNamespaceJobPage, DfsNamespaceJobPageCursor, DfsNamespaceJobRecord, DfsNamespaceJobStatus,
    DfsNamespaceObjectVersionExpectation, DfsNamespacePathOccupantExpectation,
    DfsNamespacePrepareRecord, DfsNamespacePrepareStatus, DfsNamespaceRequestRecord,
    DfsNamespaceRequestStatus, DfsNamespaceTransaction, DfsObjectPage, DfsObjectPageCursor,
    DfsObjectVersionPage, DfsObjectVersionPageCursor, DfsObjectVersionRecord,
    DfsObjectVersionScanCursor, DfsObjectVersionScanPage, DfsPathRecord,
    DfsPendingNamespaceRequestPage, DfsPendingNamespaceRequestPageCursor, DfsReplicaReceiptPage,
    DfsReplicaReceiptPageCursor, DfsReplicaReceiptRecord, DfsReplicaReceiptScanCursor,
    DfsReplicaReceiptScanPage, DfsTombstoneCompactionOptions, DfsTombstoneCompactionReport,
    DfsWriteIntentPage, DfsWriteIntentPageCursor, DfsWriteIntentRecord, DfsWriteIntentScanCursor,
    DfsWriteIntentScanPage, DfsWriteIntentStatus,
};
pub(crate) use metadata_records::{
    dfs_now_unix_nanos, metadata_journal_batch_file_system_id, object_version_record,
    put_visible_version_record_if_changed, read_conflicts_reason,
    remote_read_rejection_prunes_location,
};
pub use metadata_replication::{
    AsyncDfsMetadataJournalBatchSender, AsyncDfsMetadataJournalReplicationTransport,
    AsyncDfsMetadataNodeTransport, AsyncDfsMetadataSnapshotInstallSender,
    DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
    DFS_METADATA_REPLICATION_MAX_NODE_ROUTES_PER_TARGET, DfsMetadataJournalBatchSender,
    DfsMetadataJournalPublisherHost, DfsMetadataJournalReplicationOptions,
    DfsMetadataJournalReplicationOutcome, DfsMetadataJournalReplicationReport,
    DfsMetadataJournalReplicationTransport, DfsMetadataJournalReplicationTransportError,
    DfsMetadataJournalReplicationTransportFuture, DfsMetadataNodeTransport,
    DfsMetadataPeerTransportError, DfsMetadataPublishScheduleOptions,
    DfsMetadataPublishScheduleStart, DfsMetadataPublishScheduler, DfsMetadataReplicaShardContext,
    DfsMetadataSnapshotInstallSender, LocalDfsMetadataJournalPublisherHost,
    cluster_dfs_metadata_replica_shard_context, dfs_events_advance_metadata_replica,
    dfs_events_should_publish_metadata, dfs_metadata_replication_notice_lines,
    dfs_metadata_replication_report_needs_retry, dfs_metadata_replication_sent_count,
    materialize_default_sifs_metadata, materialize_default_sifs_metadata_on_shards,
    process_cluster_dfs_metadata_journal_batch, process_cluster_dfs_metadata_snapshot_install,
    process_dfs_metadata_journal_batch, process_dfs_metadata_snapshot_install,
    publish_cluster_dfs_metadata_journal, publish_cluster_dfs_metadata_journal_async,
    publish_cluster_dfs_metadata_journal_async_with_execution_exclusions,
    publish_cluster_dfs_metadata_journal_with_async_peer_transport,
    publish_cluster_dfs_metadata_journal_with_peer_transport,
    publish_cluster_dfs_metadata_journal_with_peer_transports_async,
    publish_cluster_dfs_metadata_journal_with_peer_transports_async_and_execution_exclusions,
    publish_cluster_dfs_metadata_shard_with_peer_transports_async_and_execution_exclusions,
    publish_dfs_metadata_journal_with_async_host,
    publish_dfs_metadata_journal_with_async_host_and_execution_exclusions,
    publish_dfs_metadata_journal_with_host, refresh_cluster_dfs_kv_snapshot_with_peer_transport,
    replicate_dfs_metadata_shard_to_target, run_cluster_dfs_metadata_publish_after_events,
    run_cluster_dfs_metadata_publish_after_events_with_async_peer_transport,
    run_cluster_dfs_metadata_publish_after_events_with_peer_transport,
    run_dfs_metadata_publish_after_events, run_dfs_metadata_publish_after_events_async,
    schedule_cluster_dfs_metadata_publish_after_events_async_on_runtime,
    schedule_cluster_dfs_metadata_publish_after_events_with_async_peer_transport_on_runtime,
    schedule_cluster_dfs_metadata_publish_after_events_with_peer_transports_on_runtime,
    schedule_cluster_dfs_metadata_publish_async_on_runtime,
    schedule_cluster_dfs_metadata_publish_with_async_peer_transport_on_runtime,
    schedule_cluster_dfs_metadata_publish_with_peer_transports_on_runtime,
    schedule_default_dfs_metadata_publish_after_events_async_on_runtime,
    schedule_default_dfs_metadata_publish_async_on_runtime,
    schedule_dfs_metadata_publish_after_events_async_on_runtime,
    schedule_dfs_metadata_publish_async_on_runtime, validate_dfs_metadata_journal_batch_source,
    validate_dfs_metadata_snapshot_install_source,
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
    prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions,
};
pub use metadata_sharded_backend::LocalShardedMetadataBackend;
pub use model::{
    DfsFileSystem, DfsMetadataStore, DfsObjectKind, DfsObjectMetadata, DfsOperationalMetrics,
    DfsPlacementScope, DfsPolicyViolation, DfsReadOutcome, DfsServiceEvent, DfsSubtreePolicy,
    DfsTombstoneProvenance, DfsWriteOutcome, ReadRoute, ReadRouteAction, WriteIntent,
    WriteIntentStatus, WriteRoute, WriteRouteAction,
};
pub(crate) use model::{normalize_current_locations, path_index_key};
pub use module_surface::{
    DFS_POLICY_VIOLATION_EVENT_KIND, DFS_PROTOCOL, SINOMO_DFS_MODULE_LEGACY_ID,
    SINOMO_DFS_MODULE_OWNER, SINOMO_DFS_NATIVE_REGISTRATION_KEY, dfs_module_capabilities,
    dfs_module_id, dfs_module_registration, dfs_module_surfaces, register_storage_paths,
};
pub use mounted_sifs::{
    MountedSifsContentReadContext, MountedSifsService, OpenedMountedSifsService,
    discover_mounted_sifs_content_replica_candidates, open_mounted_sifs_service,
    open_mounted_sifs_service_with_cancellation, open_mounted_sifs_service_with_peer_transports,
    open_mounted_sifs_service_with_peer_transports_and_default_control_plane,
};
pub use namespace::{
    SIFS_NODES_PATH, SIFS_ROOT_PATH, SIFS_SERVICES_PATH, SIFS_SINOMO_PATH, SIFS_USERS_PATH,
    directory_object, distributed_sifs, distributed_sifs_bootstrap_objects,
    distributed_sifs_bootstrap_objects_for_nodes, object_id_for_path, sifs_node_files_path,
    sifs_node_path,
};
pub(crate) use namespace::{
    metadata_path, namespace_transaction_id_for_batch, namespace_transaction_id_for_object,
};
#[cfg(test)]
pub(crate) use node_transport::{
    AsyncDfsDirectReadSender, AsyncDfsMessageSender, DfsDirectReadSender,
    async_dfs_direct_read_sender_from_peer_transport, dfs_direct_read_sender_from_peer_transport,
};
pub use node_transport::{
    AsyncDfsPeerTransport, DfsNodeDetachedSendState, DfsNodeDetachedSendWorker,
    DfsNodeServedMessageTransport, DfsNodeTransport, DfsObjectRpcSendOptions, DfsPeerTransport,
    DfsPeerTransportError, DfsPeerTransportFuture, dfs_message_response_from_rpc_result,
    dfs_message_response_from_rpc_result_sync, dfs_routed_call_rpc_call,
};
pub(crate) use node_transport::{DfsMessageSender, dfs_message_sender_from_peer_transport};
#[cfg(test)]
pub(crate) use node_transport::{
    process_cluster_dfs_message_command, run_cluster_dfs_message_command_with_metadata_publish,
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
pub use placement::{
    DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE, DFS_PLACEMENT_PREVIEW_SCHEMA_VERSION,
    DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION, DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
    DfsExactPlacementPolicy, DfsPlacementBlockingDiagnostic, DfsPlacementImpact,
    DfsPlacementImpactPage, DfsPlacementNodePreview, DfsPlacementNodeReachability,
    DfsPlacementPolicy, DfsPlacementPreview, DfsPlacementPreviewObjectFence,
    DfsPlacementPreviewToken, DfsPlacementReconciliationActivityCursor,
    DfsPlacementReconciliationActivityPage, DfsPlacementReconciliationActivityStatus,
    DfsPlacementReconciliationActivitySummary, DfsPlacementReconciliationJob,
    DfsPlacementReconciliationState, DfsPlacementScopeResolution, DfsResolvedPlacementPolicy,
    MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE, bind_placement_preview_token,
    merge_placement_impact, normalize_placement_rule, placement_reconciliation_activity_page,
    placement_reconciliation_compatibility_diagnostic, placement_rule_matches, preview_placement,
    preview_placement_with_unavailable_nodes, remove_placement_rule, resolve_placement_policy,
    upsert_placement_rule,
};
pub use posix::sync::{DfsPosixDirectorySyncCertificate, DfsPosixFileSyncCertificate};
pub use posix_model::{
    DfsDirEntry, DfsDirEntryPage, DfsNamespaceJobFailure, DfsNamespaceJobMaintenanceReport,
    DfsPosixCreateAttrs, DfsPosixDirectorySyncTarget, DfsPosixError, DfsPosixFilePrefixRead,
    DfsPosixFileRangeReadStream, DfsPosixFileReadStream, DfsPosixFileSyncEvidence,
    DfsPosixFileSyncTarget, DfsPosixSyncKind, DfsStat, DfsWriteOptions,
};
pub use protocol::{
    DfsBlobTransferResult, DfsBulkReadItem, DfsBulkReadItemResponse, DfsBulkReadRequest,
    DfsBulkReadResponse, DfsCanonicalAuthorization, DfsCommitVersionCommand,
    DfsCommitVersionResponse, DfsDirectReadRequest, DfsDistributedWriteCommand,
    DfsDistributedWriteResponse, DfsDistributedWriteResult, DfsFetchBlobCommand,
    DfsFetchBlobResponse, DfsFetchBlobResult, DfsLocalEventualBatchWriteProof, DfsMessageCommand,
    DfsMessageResponse, DfsMetadataAuthorityOperation, DfsMetadataAuthorityProbe,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
    DfsMetadataPrefixProbe, DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
    DfsMetadataSnapshotInstallResult, DfsNamespaceCommand, DfsNamespaceOperation,
    DfsNamespacePosixError, DfsNamespaceResponse, DfsNamespaceResult, DfsNamespaceShardCursor,
    DfsNetworkMessage, DfsPosixCoordinationResult, DfsReadData, DfsReadRangeData, DfsReadRequest,
    DfsReadResponse, DfsReadResult, DfsRepairReplicaCommand, DfsRepairReplicaResponse,
    DfsRoutedMessage, DfsStageBlobCommand, DfsStageBlobResponse, DfsStagePatchCommand,
    DfsWriteCommand, DfsWriteResponse, DfsWriteResult,
};
pub use renewable_lease::{
    DEFAULT_DFS_RENEWABLE_LEASE_RELEASE_TIMEOUT, DfsRenewableExclusiveLease,
    DfsRenewableLeaseAnchor, DfsRenewableLeaseConfig, DfsRenewableLeaseError,
    DfsRenewableLeaseStatus,
};
pub(crate) use repair::{
    repair_attempt_record_indicates_lost_authority,
    repair_attempt_record_indicates_transient_route_failure,
    repair_error_indicates_transient_route_failure, repair_rejection_indicates_lost_authority,
    repair_rejection_indicates_stale_replica_base,
};
pub use routes::{
    DFS_CONTROL_PLANE_ROUTES, DFS_DATA_BULK_READ_RESPONSE_ROUTE, DFS_DATA_BULK_READ_ROUTE,
    DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE, DFS_DATA_COMMIT_VERSION_ROUTE,
    DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE, DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
    DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE, DFS_DATA_FETCH_BLOB_ROUTE,
    DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE, DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_DATA_METADATA_SNAPSHOT_INSTALL_RESPONSE_ROUTE, DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE,
    DFS_DATA_NAMESPACE_RESPONSE_ROUTE, DFS_DATA_NAMESPACE_ROUTE, DFS_DATA_PLANE_ROUTES,
    DFS_DATA_PLANE_SERVICE_ROUTES, DFS_DATA_READ_RESPONSE_ROUTE, DFS_DATA_READ_ROUTE,
    DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE,
    DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE, DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_STAGE_PATCH_ROUTE,
    DFS_DATA_WRITE_RESPONSE_ROUTE, DFS_DATA_WRITE_ROUTE, DFS_DIRECT_READ_ROUTE,
    DFS_INSPECTION_ROUTE, DFS_MESSAGE_DATA_PLANE_ROUTES, DFS_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, DFS_OBJECT_RPC_ROUTES, DFS_RPC_CATEGORY, DFS_SMOKE_ROUTE,
    DFS_STATUS_ROUTE, DfsMessageExecutionScope, decode_dfs_inspection_rpc_response,
    decode_dfs_message_rpc_request, decode_dfs_message_rpc_response,
    decode_dfs_metadata_journal_batch_rpc_response,
    decode_dfs_metadata_snapshot_install_rpc_response, decode_dfs_smoke_rpc_response,
    decode_dfs_status_rpc_response, dfs_inspection_rpc_request,
    dfs_message_allows_write_repair_progress, dfs_message_execution_scope,
    dfs_message_is_read_only_request, dfs_message_requires_fresh_metadata,
    dfs_message_requires_full_metadata_hydrate, dfs_message_rpc_call, dfs_message_rpc_request,
    dfs_metadata_journal_batch_execution_scope, dfs_metadata_journal_batch_rpc_call,
    dfs_metadata_journal_batch_rpc_request, dfs_metadata_snapshot_install_execution_scope,
    dfs_metadata_snapshot_install_rpc_call, dfs_metadata_snapshot_install_rpc_request,
    dfs_route_is_message_data_plane, dfs_routed_message_rpc_request, dfs_smoke_rpc_request,
    dfs_status_rpc_request,
};
pub(crate) use routes::{
    build_dfs_routed_message, dfs_network_message_allows_write_repair_progress,
};
pub(crate) use service_content::object_content_identity;
pub(crate) use service_repair::DFS_PENDING_TRANSFER_STALE_AFTER;
pub use service_sync::{
    DEFAULT_DFS_POSIX_SYNC_TIMEOUT, DfsMetadataQuorumSyncCertificate,
    DfsMetadataQuorumSyncProgress, DfsMetadataQuorumSyncTarget, dfs_metadata_replica_quorum_size,
};

/// Identifier for a DFS filesystem such as the default `sifs`.
pub type DfsFileSystemId = sinomo_core::Id;
/// Identifier for a node participating in DFS placement or routing.
pub type DfsNodeId = NodeId;
/// Identifier for a DFS object within a filesystem.
pub type DfsObjectId = sinomo_core::Id;
/// Identifier for a committed DFS object version.
pub type DfsVersion = sinomo_core::Id;

pub use service::{
    DfsDataPlaneHost, DfsDataPlaneHostError, DfsDataPlaneHostFuture, DfsDataPlaneResponse,
    DfsDataPlaneService, DfsInboundAttachment, DfsService, register_data_plane_services,
    register_services,
};
pub(crate) use service_state::{
    ActiveReplicaStage, DFS_CONFLICT_SCAN_PAGE_SIZE, DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES,
    DFS_DEFERRED_PARENT_TOUCH_DRAIN_LIMIT, DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH,
    DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE, DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE, DFS_METADATA_OBJECT_VERSION_PAGE_SIZE,
    DFS_REMOTE_READ_RETRY_DELAY, DFS_ROUTED_NAMESPACE_IDLE_WAIT,
    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT,
    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BYTE_LIMIT,
    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT, DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT,
    DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT, NetworkedDfsRangeReadStream,
    NetworkedDfsRangeReadStreamOutcome, NetworkedDfsReadStream, NetworkedDfsReadStreamOutcome,
    PendingMetadataAuthorityProbe, PendingMetadataReplication, PendingRemoteWriteResponse,
    PendingRoutedNamespaceOp, PendingRoutedNamespaceResult, PendingRoutedWrite,
    StageBlobSendOutcome, retained_networked_dfs_service_events,
};
pub use service_state::{
    DFS_ROUTED_NAMESPACE_OP_TIMEOUT, DFS_ROUTED_RESPONSE_JOURNAL_BATCH_LIMIT, DfsCancellationFlag,
    DfsCommittedWork, DfsCommittedWorkTracker, DfsConfirmedTimeoutAction,
    DfsConfirmedTimeoutOutcome, DfsConflictReadView, DfsMessageCommittedWork,
    DfsMessageProcessReport, DfsMessageProcessResult, DfsMetadataReplicationOutcome,
    DfsOwnerHandoffOutcome, DfsReconnectReconciliation, NetworkedDfsRangeReadOutcome,
    NetworkedDfsReadFreshness, NetworkedDfsReadOutcome, NetworkedDfsService,
    NetworkedDfsServiceEvent, NetworkedDfsWriteOutcome,
};
pub use service_trace::with_dfs_traceparent;
pub(crate) use service_trace::{
    current_dfs_traceparent, dfs_distributed_write_result_trace_label,
    dfs_namespace_operation_trace_label, dfs_network_message_trace_label, dfs_perf_trace,
    dfs_perf_trace_enabled,
};
use sinomo_core::node::NodeId;
pub(crate) use smoke::run_cluster_dfs_smoke_with_message_sender;
pub use smoke::{
    DfsSmokeAttempt, DfsSmokeHost, DfsSmokeOptions, DfsSmokeReport, LocalDfsSmokeHost,
    run_dfs_smoke_with_host,
};
#[cfg(test)]
pub(crate) use smoke::{
    dfs_smoke_content_node_ids, dfs_smoke_replica_candidate_sets, dfs_smoke_routable_node_ids,
    run_cluster_dfs_smoke,
};
#[cfg(test)]
use transfer::{DFS_REPAIR_RETRY_SUPPRESSED_TICK, RepairRetryState, fetch_inflight_timeout_ticks};
#[cfg(test)]
mod tests;
