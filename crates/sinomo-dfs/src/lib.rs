//! Distributed filesystem services, storage, protocol, and operator APIs.
//!
//! `sinomo-dfs` owns Sinomo filesystem semantics: SIFS bootstrap data,
//! metadata shards and journals, content storage, repair, replication, POSIX
//! operations, object-RPC routes, and the networked DFS service. The public
//! interface is intentionally exported from this crate root so callers do not
//! couple to the internal module layout.

mod advisory_lock;
mod body_rpc;
pub mod browser_api;
mod browser_service;
mod cluster_service;
mod content_root_catalog;
mod content_store;
mod control_plane;
mod default_cluster;
mod default_projection;
mod default_reconciliation;
mod direct_read;
mod discovery;
mod distributed;
mod error;
mod host_filesystem_provider;
mod host_filesystem_provider_client;
mod identity;
mod intent;
mod local_service;
mod local_state;
mod maintenance;
mod maintenance_schedule;
mod message_transport;
mod metadata_api;
mod metadata_authority_projection;
mod metadata_authority_runtime;
mod metadata_backend;
mod metadata_cursor;
mod metadata_error;
mod metadata_hardlink;
mod metadata_history_gc;
mod metadata_io;
mod metadata_journal;
mod metadata_keys;
mod metadata_limits;
mod metadata_lock;
mod metadata_namespace_transaction;
mod metadata_object_index;
mod metadata_pages;
mod metadata_prepare_codec;
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
mod mount_config;
mod mounted_mutation_outbox;
mod mounted_sifs;
mod namespace;
mod namespace_change_feed;
mod node_transport;
mod operator;
mod performance_metrics;
mod placement;
mod placement_environment;
mod posix;
mod posix_model;
mod posix_object;
mod posix_path;
mod protocol;
mod renewable_lease;
mod repair;
mod repair_queue_store;
mod routes;
mod runtime_admission;
mod runtime_authority;
mod runtime_message;
mod runtime_mounted_mutation;
mod runtime_probe;
mod runtime_publish;
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
pub use browser_api::*;
pub use browser_service::{
    DfsBrowserServiceHost, HostedDfsBrowserService, HostedDfsPlacementAuthorityAdminService,
    register_dfs_browser_services,
};
pub use default_projection::*;
#[cfg(feature = "test-support")]
pub use metadata_authority_runtime::DfsMetadataAuthorityDebugSnapshot;
pub use metadata_authority_runtime::{
    DFS_METADATA_AUTHORITY_PREFIX, DfsMetadataAuthorityDeltaObservation, DfsMetadataAuthorityHost,
    DfsMetadataAuthorityReadFuture, DfsMetadataAuthorityRuntime,
    DfsMetadataAuthorityRuntimeRegistry, DfsMetadataAuthorityState, DfsMetadataAuthorityTask,
};
pub use runtime_admission::{
    DFS_MUTATION_SHARD_GATE_LIMIT, DfsMaintenanceForegroundPreemption,
    DfsMaintenancePreemptionOutcome, DfsMetadataLaneKey, DfsMetadataSchedulerRegistry,
    DfsMutationAdmission, DfsMutationGenerationFence, DfsMutationLaneKey, DfsMutationScheduler,
    DfsMutationSchedulerRegistry, acquire_dfs_maintenance_admission_until_foreground_waits,
    run_dfs_maintenance_until_foreground_waits,
};
pub use runtime_authority::{
    DFS_AUTHORITY_PROJECTION_MAX_BYTES, DFS_AUTHORITY_PROJECTION_MAX_ENTRIES,
    DFS_AUTHORITY_PROJECTION_MAX_SCOPES, DfsAuthoritativeProjectionCache,
    dfs_metadata_prefix_probe_is_verification_only,
};
pub use runtime_message::{
    DFS_ATTACHMENT_INGEST_BUFFERED_CHUNKS, DfsCommittedEffects, DfsCommittedEffectsHost,
    DfsDetachedSendRuntime, DfsResidentMetadataScope, DfsRuntimeMaintenanceServiceLane,
    DfsRuntimeMessageCacheEntry, DfsRuntimeMessageExecution, DfsRuntimeMessagePeerSnapshot,
    DfsRuntimeMessageResult, DfsRuntimeMessageService, DfsRuntimeMessageServiceLane,
    DfsRuntimeMessageServiceLaneHandle, DfsRuntimeMessageServiceSignature, DfsRuntimeTask,
    DfsVisibleMutationEvent, dfs_detached_send_runtime_for_identity,
    dfs_metadata_journal_rejection, dfs_metadata_journal_rejection_for,
    dfs_metadata_snapshot_rejection, dfs_metadata_snapshot_rejection_for, dfs_network_message_kind,
    dfs_response_committed_visible_mutation, dfs_runtime_message_cache,
    dfs_runtime_message_supports_exact_service_lane, dfs_runtime_message_uses_content_read_lane,
    dfs_visible_mutation_event, emit_runtime_committed_effects, execute_runtime_message_command,
    ingest_dfs_message_attachment, read_dfs_runtime_message_peer_snapshot,
};
#[cfg(any(test, feature = "test-support"))]
pub use runtime_message::{
    test_clear_dfs_runtime_peer_discovery_cache, test_expire_dfs_runtime_peer_discovery_cache,
};
pub use runtime_mounted_mutation::{
    MOUNTED_SIFS_MUTATION_REPROBE_INTERVAL, MountedSifsMutationHandoffHost,
    MountedSifsMutationHandoffPhase, MountedSifsMutationHandoffRequest,
    MountedSifsMutationHandoffResponse, MountedSifsMutationRecoveryBatch,
    all_mounted_sifs_mutation_committed_work, handle_mounted_sifs_mutation_handoff,
    mounted_sifs_mutation_pending_maintenance_work, mounted_sifs_mutation_startup_committed_work,
    persist_default_dfs_repair_queue_records,
};
pub use runtime_probe::{
    DfsContentVersionGcRuntimeProbeOptions, DfsContentVersionGcRuntimeProbeReport,
    DfsMetadataHistoryGcRuntimeProbeOptions, DfsMetadataHistoryGcRuntimeProbeReport,
    DfsMetadataHydrationRuntimeProbeOptions, DfsMetadataHydrationRuntimeProbeReport,
    run_content_version_gc_runtime_probe, run_metadata_history_gc_runtime_probe,
    run_metadata_hydration_runtime_probe,
};
pub use runtime_publish::{
    DfsMetadataPublishPlanningAttempt, metadata_publish_lanes_from_authority,
    run_metadata_publish_blocking_execution, run_metadata_publish_lane_planning_with_admission,
};
#[cfg(feature = "test-support")]
pub mod browser_test_support {
    pub use super::browser_service::dfs_browser_backend_open_count as daemon_dfs_browser_backend_open_count;
    pub use super::browser_service::{
        DaemonDfsBrowserPlacementConnectivitySnapshot, DfsBrowserBackend,
        DfsBrowserObservedControlPlaneKvClient, daemon_dfs_browser_bulk_file_read_response,
        daemon_dfs_browser_conditional_write_error, daemon_dfs_browser_read_file_prefix_response,
        daemon_dfs_browser_read_file_response, daemon_dfs_browser_route_is_path_access,
        enrich_daemon_dfs_browser_placement_connectivity_from_snapshot,
        extend_daemon_dfs_browser_preview_diagnostics, merge_daemon_dfs_browser_placement_impact,
        mutate_daemon_dfs_browser_placement_config,
        require_local_dfs_placement_authority_break_glass_signer,
        require_matching_daemon_dfs_browser_placement_preview_token,
        require_satisfiable_daemon_dfs_browser_placement, sifs_browser_write_candidates,
    };
}
pub use cluster_service::{
    cluster_dfs_peer_node_ids, discovered_cluster_dfs_peer_node_ids,
    discovered_cluster_dfs_provider_node_ids,
};
pub(crate) use cluster_service::{
    open_cluster_dfs_service_with_message_sender,
    open_cluster_dfs_service_with_message_sender_lazy,
    open_cluster_dfs_service_with_peer_transports_for_nodes,
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state,
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state_until_cancelled,
    open_cluster_dfs_service_with_peer_transports_for_nodes_lazy,
};
pub use content_root_catalog::{
    DEFAULT_DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRIES, DFS_CONTENT_ROOT_CATALOG_FORMAT_VERSION,
    DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES, DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES,
    DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT, DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
    DFS_CONTENT_ROOT_CATALOG_TARGET_PAGE_BYTES, DfsContentRootCatalog,
    DfsContentRootCatalogCollectionValidation, DfsContentRootCatalogCoverage,
    DfsContentRootCatalogCursor, DfsContentRootCatalogEntry, DfsContentRootCatalogFragment,
    DfsContentRootCatalogIncompleteReason, DfsContentRootCatalogPage,
    DfsContentRootCatalogPageRequest, DfsContentRootCatalogShardIdentity,
    LocalDfsContentRootCatalogStore, default_dfs_content_root_catalog_store,
    dfs_content_root_catalog_entries, dfs_content_root_catalog_topology_digest,
    dfs_content_root_catalog_topology_digest_with_file_systems,
};
pub(crate) use content_store::content_hash;
pub use content_store::{
    BlobRangeReader, BlobReader, ContentManifest, ContentStoreError, ContentStoreGcOptions,
    ContentStoreGcReport, ContentStoreQuarantinedBlob, ContentStoreQuota,
    ContentStoreQuotaResource, ContentStoreQuotaUsage, ContentStoreReservationIdentity,
    ContentStoreReservationReconciliationOptions, ContentStoreReservationReconciliationReport,
    ContentStoreReservationRecord, ContentStoreReservationRoots, ContentStoreScrubIssue,
    ContentStoreScrubReport, ContentStoreStagingGcOptions, ContentStoreStagingGcReport,
    ContentStoreStats, ContentStoreVersionGcOptions, ContentStoreVersionGcReport,
    ContentVersionIdentity, LocalContentStore, ObjectVersionReader,
};
pub use control_plane::{
    AsyncDfsControlPlaneKvClient, DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT,
    DfsAuthoritativePrefixProjection, DfsControlPlaneAsyncStore, DfsControlPlaneKvClient,
    DfsControlPlaneKvFuture, DfsControlPlaneStore, DfsFileSystemConfig,
    DfsMetadataNamespacePrepareCodec, DfsMetadataShardAssignment, DfsMetadataShardConfig,
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
    select_metadata_shard_config_for_path_strict_default_sifs_split,
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
    dfs_service_members, ensure_default_cluster_dfs,
    ensure_default_cluster_dfs_async_with_resident_metadata,
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
    DfsDirectReadAttachmentResponse, DfsDirectReadClientData, DfsDirectReadClientRangeData,
    DfsDirectReadClientResponse, DfsDirectReadClientResult, DfsReadStreamCompletion,
    DfsVerifiedReadStream, dfs_direct_read_response_from_rpc_result, dfs_direct_read_rpc_call,
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
pub use host_filesystem_provider::*;
pub use host_filesystem_provider_client::*;
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
    DFS_IMPORTS_NAMESPACE_ROOT, add_local_dfs_pin, cluster_dfs_all_file_system_configs,
    cluster_dfs_all_metadata_shard_head_watermarks,
    cluster_dfs_all_metadata_shard_membership_handoffs, cluster_dfs_all_metadata_shards,
    cluster_dfs_metadata_shards, default_dfs_metadata_root, default_dfs_metadata_shards_root,
    default_dfs_pin_root, dfs_import_metadata_root, dfs_import_namespace_root, dfs_import_root,
    dfs_metadata_shard_membership_handoff_key_is_exact,
    evict_cluster_aware_dfs_metadata_inspection_cache, inspect_cluster_aware_dfs,
    inspect_cluster_aware_dfs_from_metadata, inspect_cluster_aware_dfs_status,
    inspect_dfs_conflicts, inspect_dfs_file_system, inspect_dfs_object, inspect_dfs_path,
    inspect_dfs_repairs, inspect_dfs_status, inspect_dfs_status_from_metadata,
    inspect_dfs_status_from_resident_snapshot, inspect_incomplete_dfs_status, list_local_dfs_pins,
    local_dfs_metadata_shard_path, open_cluster_aware_dfs_metadata_backend,
    open_cluster_aware_dfs_metadata_backend_for_inspection,
    open_cluster_aware_dfs_metadata_backend_for_inspection_with_topology,
    open_cluster_sharded_dfs_metadata_backend, open_local_metadata_shard_backend,
    open_local_sharded_metadata_backend, open_local_sharded_metadata_backend_for_replica_repair,
    open_root_local_metadata_backend, read_local_dfs_metadata_shard_write_freeze,
    remove_local_dfs_pin, resolve_dfs_metadata_root, resolve_dfs_pin_root,
    with_cached_cluster_aware_dfs_metadata_inspection,
    with_cached_cluster_aware_dfs_metadata_inspection_with_topology,
};
pub(crate) use maintenance::record_anti_entropy_issue;
pub use maintenance::{
    DEFAULT_DFS_CONFLICT_MAINTENANCE_MAX_RECORDS,
    DEFAULT_DFS_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS,
    DEFAULT_DFS_CONTENT_RESERVATION_REJECTION_GRACE, DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_AGE,
    DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_PAGES_PER_SHARD,
    DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_ROOTS_PER_SHARD,
    DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_ISSUES,
    DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_OBJECTS,
    DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_JOBS,
    DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_STEPS_PER_JOB,
    DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE, DEFAULT_DFS_WRITE_INTENT_MAINTENANCE_MAX_RECORDS,
    DFS_RESIDENT_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS,
    DFS_RESIDENT_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS,
    DFS_RESIDENT_METADATA_HISTORY_GC_MAX_RECORDS, DFS_RESIDENT_METADATA_HISTORY_GC_MIN_AGE,
    DFS_RESIDENT_METADATA_REPLICATION_ATTEMPT_TIMEOUT,
    DFS_RESIDENT_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
    DFS_RESIDENT_METADATA_REPLICATION_TARGET_TIMEOUT, DFS_RESIDENT_REPAIR_MAX_JOBS_PER_TICK,
    DFS_RESIDENT_TOMBSTONE_COMPACTION_MAX_RECORDS, DFS_RESIDENT_TOMBSTONE_COMPACTION_MIN_AGE,
    DfsAntiEntropyCursorRecord, DfsAntiEntropyIssue, DfsAntiEntropyOptions, DfsAntiEntropyReport,
    DfsConflictMaintenanceReport, DfsLifecycleMaintenanceOptions, DfsLifecycleMaintenanceReport,
    DfsMaintenanceExecutionExclusions, DfsMaintenanceHost, DfsMaintenanceRunOptions,
    DfsMaintenanceTickObservations, DfsMaintenanceTickOptions, DfsMaintenanceTickReport,
    DfsMetadataReplicationSuppressionRecord, DfsMetadataReplicationSuppressionSnapshot,
    DfsPendingMaintenanceWork, DfsRepairAttemptRecord, DfsRepairAttemptSnapshot,
    DfsWriteIntentMaintenanceReport, LegacyDfsMetadataReplicationCursor, LocalDfsMaintenanceHost,
    clear_dfs_anti_entropy_cursor, default_dfs_anti_entropy_cursor_path,
    default_dfs_metadata_replication_suppression_snapshot_path,
    default_dfs_repair_attempt_snapshot_path, dfs_actionable_placement_reconciliation_count,
    dfs_completed_local_primary_handoff_rebind_count, dfs_events_should_wake_repair_maintenance,
    dfs_explicit_disaster_recovery_tick_options, dfs_lifecycle_maintenance_notice_lines,
    dfs_maintenance_report_reached_limit, dfs_maintenance_tick_observations,
    dfs_namespace_job_maintenance_notice_lines, dfs_node_is_offline_for_maintenance,
    dfs_offline_node_ids, dfs_pending_reactive_maintenance_work_from_authority_entries,
    dfs_repair_maintenance_notice_lines, dfs_resident_maintenance_execution_exclusions,
    dfs_resident_metadata_replication_options, dfs_resident_routine_maintenance_tick_options,
    dfs_resident_smoke_options, dfs_write_intent_maintenance_notice_lines,
    legacy_dfs_anti_entropy_cursor_path, legacy_dfs_metadata_replication_cursor_path,
    migrate_legacy_dfs_metadata_replication_cursor, put_local_dfs_metadata_replication_suppression,
    read_dfs_anti_entropy_cursor, read_dfs_metadata_replication_suppression_snapshot,
    read_dfs_repair_attempt_snapshot, run_cluster_dfs_maintenance_tick_with_async_peer_transport,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions_and_committed_work_tracker,
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions_and_committed_work_tracker_until_hydration_cancelled,
    run_cluster_dfs_maintenance_tick_with_peer_transport,
    run_cluster_dfs_maintenance_tick_with_reusable_service_until_hydration_cancelled,
    run_cluster_dfs_reactive_maintenance_tick_with_reusable_service,
    run_dfs_lifecycle_maintenance_with_host, run_dfs_maintenance_tick_with_async_host,
    run_dfs_maintenance_tick_with_async_host_and_execution_exclusions,
    run_dfs_maintenance_tick_with_host, write_dfs_anti_entropy_cursor,
    write_dfs_metadata_replication_suppression_snapshot, write_dfs_repair_attempt_snapshot,
};
#[cfg(test)]
pub(crate) use maintenance::{
    run_cluster_dfs_maintenance_tick, write_legacy_dfs_metadata_replication_cursor,
};
pub use maintenance_schedule::{
    DFS_DEFERRED_WORK_REPROBE_INTERVAL, DFS_MAINTENANCE_LEASE_RETRY_DELAY,
    DFS_MAINTENANCE_MEMBER_PHASE_SLOT, DFS_MAINTENANCE_MEMBER_PHASE_WINDOW,
    DFS_MAINTENANCE_NETWORK_STARTUP_GRACE, DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL,
    DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL, DfsMaintenanceProbeDecision, DfsMaintenanceTickOutcome,
    dfs_failed_maintenance_decision, dfs_initial_maintenance_delay,
    dfs_maintenance_completion_decision, dfs_maintenance_due_in, dfs_maintenance_member_phase,
    dfs_maintenance_notification_decision, dfs_maintenance_probe_decision,
    dfs_maintenance_probe_is_due, dfs_maintenance_revalidation_decision,
    dfs_maintenance_should_launch,
};
pub(crate) use message_transport::DfsMetadataJournalBatchDirectGroup;
pub use message_transport::{
    AsyncDfsMessageTransport, DfsMessageTransport, DfsMessageTransportFuture,
    DfsOutboundAttachment, DfsRoutedCall, DfsRoutedSend, DfsRoutedSendOutcome,
    DfsTransportIdentity, materialize_dfs_routed_call_sync,
};
pub use metadata_api::{
    DfsMetadataBackend, DfsMetadataReplicaApplyReport, DfsMetadataReplicaIngestPermit,
    DfsMetadataShardWriteFreeze, DfsNamespaceRequestAdmissionSnapshot,
    DfsNormalizedNamespacePrepare, DfsObjectLocationUpdate,
};
pub use metadata_backend::LocalSobjMetadataBackend;
pub use metadata_error::MetadataBackendError;
pub use metadata_history_gc::{
    DfsMetadataContentProtection, DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport,
    DfsMetadataHistoryShardStats, DfsMetadataRetainedTableStats, DfsMetadataVersionIdentity,
};
pub use metadata_limits::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
};
pub use metadata_prepare_codec::{DfsPackedNamespacePrepareV1, DfsPackedNamespacePrepareV2};
pub use metadata_records::{
    DfsConflictPage, DfsConflictPageCursor, DfsConflictRecord, DfsConflictScanCursor,
    DfsConflictScanPage, DfsConflictStatus, DfsCrossShardPinTransferRecord,
    DfsCrossShardSourceFamily, DfsCrossShardVersionPinFence, DfsDentryPageCursor, DfsDirFragmentId,
    DfsDirFragmentRecord, DfsDirectoryPage, DfsDirectoryPageCursor, DfsInodeId, DfsInodeRecord,
    DfsMetadataAuthorityAckFloor, DfsMetadataJournalBatch, DfsMetadataJournalEntry,
    DfsMetadataJournalFrameRecord, DfsMetadataMutation, DfsMetadataPrefixAckFloor,
    DfsMetadataPrefixProof, DfsMetadataReplayCursorStatus, DfsMetadataReplicaLagStatus,
    DfsMetadataShardId, DfsMetadataShardMigrationReport, DfsMetadataShardStatus,
    DfsMetadataSubscription, DfsMetadataSummary, DfsMetadataTableSnapshot,
    DfsMetadataVersionPinPage, DfsMetadataVersionPinPageCursor, DfsMetadataVersionPinRecord,
    DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobOrigin, DfsNamespaceJobPage,
    DfsNamespaceJobPageCursor, DfsNamespaceJobReceipt, DfsNamespaceJobRecord,
    DfsNamespaceJobStatus, DfsNamespaceObjectVersionExpectation,
    DfsNamespacePathOccupantExpectation, DfsNamespacePrepareReceipt, DfsNamespacePrepareRecord,
    DfsNamespacePrepareStatus, DfsNamespaceRequestCompletion, DfsNamespaceRequestReceipt,
    DfsNamespaceRequestRecord, DfsNamespaceRequestStatus, DfsNamespaceTransaction,
    DfsNamespaceTransactionReceipt, DfsObjectPage, DfsObjectPageCursor, DfsObjectVersionPage,
    DfsObjectVersionPageCursor, DfsObjectVersionRecord, DfsObjectVersionScanCursor,
    DfsObjectVersionScanPage, DfsObjectWriteIntentHistory, DfsPathRecord,
    DfsPendingNamespaceRequestPage, DfsPendingNamespaceRequestPageCursor, DfsReplicaReceiptPage,
    DfsReplicaReceiptPageCursor, DfsReplicaReceiptRecord, DfsReplicaReceiptScanCursor,
    DfsReplicaReceiptScanPage, DfsTombstoneCompactionOptions, DfsTombstoneCompactionReport,
    DfsWriteIntentPage, DfsWriteIntentPageCursor, DfsWriteIntentReceipt,
    DfsWriteIntentReceiptRetentionState, DfsWriteIntentRecord, DfsWriteIntentScanCursor,
    DfsWriteIntentScanPage, DfsWriteIntentStatus,
};
pub(crate) use metadata_records::{
    dfs_now_unix_nanos, metadata_journal_batch_file_system_id, object_version_record,
    put_visible_version_record_if_changed, read_conflicts_reason,
    remote_read_rejection_prunes_location,
};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use metadata_replication::process_cluster_dfs_metadata_journal_batch_with_replica_context_for_test;
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
    DfsMetadataSnapshotInstallSender, DfsPreparedClusterMetadataJournalPublish,
    LocalDfsMetadataJournalPublisherHost, cluster_dfs_metadata_replica_shard_context,
    dfs_events_advance_metadata_replica, dfs_events_should_publish_metadata,
    dfs_metadata_replication_fault_notice_lines, dfs_metadata_replication_notice_lines,
    dfs_metadata_replication_report_needs_retry, dfs_metadata_replication_sent_count,
    execute_prepared_cluster_dfs_metadata_publish_with_async_peer_transport,
    materialize_default_sifs_metadata, materialize_default_sifs_metadata_on_shards,
    prepare_cluster_dfs_metadata_shard_publish_with_backend_and_authority,
    prepare_cluster_dfs_metadata_shard_publish_with_backend_and_execution_exclusions,
    prepare_cluster_dfs_metadata_shard_publish_with_execution_exclusions,
    process_cluster_dfs_metadata_journal_batch, process_cluster_dfs_metadata_snapshot_install,
    process_dfs_metadata_journal_batch, process_dfs_metadata_snapshot_install,
    publish_cluster_dfs_metadata_journal, publish_cluster_dfs_metadata_journal_async,
    publish_cluster_dfs_metadata_journal_async_with_execution_exclusions,
    publish_cluster_dfs_metadata_journal_with_async_peer_transport,
    publish_cluster_dfs_metadata_journal_with_peer_transport,
    publish_cluster_dfs_metadata_journal_with_peer_transports_async,
    publish_cluster_dfs_metadata_journal_with_peer_transports_async_and_execution_exclusions,
    publish_dfs_metadata_journal_with_async_host_and_execution_exclusions,
    publish_dfs_metadata_journal_with_host, replicate_dfs_metadata_shard_to_target,
    run_cluster_dfs_metadata_publish_after_events,
    run_cluster_dfs_metadata_publish_after_events_with_peer_transport,
    run_dfs_metadata_publish_after_events, run_dfs_metadata_publish_after_events_async,
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
};
#[cfg(test)]
pub(crate) use metadata_replication::{
    metadata_replication_rejection_is_topology_mismatch,
    prepare_dfs_metadata_journal_publish_with_host,
    prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions,
};
pub use metadata_sharded_backend::{
    DfsResidentMetadataStatusSnapshot, LocalShardedMetadataBackend,
    ResidentLocalMetadataShardHandle, ResidentLocalMetadataShardLease,
    ResidentLocalShardedMetadataBackend, ResidentLocalShardedMetadataBackendLease,
};
pub use model::{
    DfsFileSystem, DfsMetadataStore, DfsObjectKind, DfsObjectMetadata, DfsOperationalMetrics,
    DfsPlacementScope, DfsPolicyViolation, DfsReadOutcome, DfsServiceEvent, DfsSubtreePolicy,
    DfsTombstoneProvenance, DfsWriteOutcome, ReadRoute, ReadRouteAction, WriteIntent,
    WriteIntentStatus, WriteRoute, WriteRouteAction,
};
pub(crate) use model::{normalize_current_locations, path_index_key};
pub use module_surface::{
    DFS_POLICY_VIOLATION_EVENT_KIND, DFS_PROTOCOL, SIFS_PATH_ACCESS_CAPABILITY,
    SINOMO_DFS_MODULE_LEGACY_ID, SINOMO_DFS_MODULE_OWNER, SINOMO_DFS_NATIVE_REGISTRATION_KEY,
    dfs_module_capabilities, dfs_module_enabled, dfs_module_id, dfs_module_registration,
    dfs_module_surfaces, register_storage_paths, set_dfs_module_enabled,
};
pub use mount_config::{
    SIFS_MOUNT_CONFIG_KEY, SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE, SIFS_MOUNT_STATUS_CONFIG_KEY,
    SIFS_MOUNT_STATUS_ROUTE, SifsMountAttentionKind, SifsMountAttentionNextAction,
    SifsMountAttentionSummary, SifsMountConfig, SifsMountCursorOnlyRecoveryRequest,
    SifsMountCursorOnlyRecoveryResponse, SifsMountPlatform, SifsMountRetainedLocalEvidence,
    SifsMountRuntimePhase, SifsMountRuntimeStatus, SifsMountStatusRequest, SifsMountStatusResponse,
    SifsMountWritebackMode, sifs_mount_config_definition, sifs_mount_config_definition_for_module,
    sifs_mount_settings_module_id, sifs_mount_status_config_definition,
    sifs_mount_status_config_definition_for_module,
};
pub use mounted_mutation_outbox::{
    MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES, MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS,
    MOUNTED_SIFS_MUTATION_OUTBOX_MAX_REPAIR_RECORDS, MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES,
    MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION, MountedSifsHostMountSessionId,
    MountedSifsHostMountSessionLease, MountedSifsHostMountSessionMarker,
    MountedSifsHostMountSessionPhase, MountedSifsMutationAdapter, MountedSifsMutationArmRequestId,
    MountedSifsMutationCapturedWork, MountedSifsMutationCommittedWorkLatch,
    MountedSifsMutationCorruptRecoveryClaim, MountedSifsMutationCorruptRecoveryReceipt,
    MountedSifsMutationErrorObservation, MountedSifsMutationLease, MountedSifsMutationMarkerId,
    MountedSifsMutationNonce, MountedSifsMutationOrigin,
    MountedSifsMutationOutboxCursorRecoveryGuard,
    MountedSifsMutationOutboxCursorRecoveryInspection, MountedSifsMutationOutboxMarker,
    MountedSifsMutationOutboxPhase, MountedSifsMutationOutboxStatus,
    MountedSifsMutationOutboxStore, MountedSifsMutationRecoveryClaim,
    MountedSifsMutationRecoveryKind, MountedSifsMutationRecoveryReceipt,
    inspect_mounted_sifs_mutation_outbox_for_cursor_recovery,
};
pub use mounted_sifs::{
    MountedSifsContentReadContext, MountedSifsControlPlaneKvClient, MountedSifsMutationReport,
    MountedSifsService, OpenedMountedSifsService, discover_mounted_sifs_content_replica_candidates,
    discover_mounted_sifs_content_replica_candidates_with_control_plane, open_mounted_sifs_service,
    open_mounted_sifs_service_with_cancellation, open_mounted_sifs_service_with_peer_transports,
    open_mounted_sifs_service_with_peer_transports_and_default_control_plane,
    open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation,
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
pub use namespace_change_feed::{
    DfsNamespaceChangeBatch, DfsNamespaceChangeCursor, DfsNamespaceChangeEvent,
    DfsNamespaceChangeProvenance, DfsNamespaceRescanReason,
    dfs_namespace_change_shards_for_subtree, poll_dfs_namespace_changes,
    project_dfs_namespace_change_batch,
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
    DfsOperatorStatusMetric, DfsOperatorStatusOmission, DfsOperatorUseConflictVersionReport,
    DfsPinAddRequest, DfsPinMode, DfsPinRecord, DfsPinStoreError, LocalDfsPinStore,
    resolve_dfs_conflicts, use_dfs_conflict_version,
};
pub use placement::{
    DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE, DFS_PLACEMENT_PREVIEW_SCHEMA_VERSION,
    DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION, DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
    DfsAdaptivePlacementBudget, DfsAdaptivePlacementObservationState, DfsExactPlacementPolicy,
    DfsPlacementBlockingDiagnostic, DfsPlacementImpact, DfsPlacementImpactPage,
    DfsPlacementNodePreview, DfsPlacementNodeReachability, DfsPlacementPolicy, DfsPlacementPreview,
    DfsPlacementPreviewObjectFence, DfsPlacementPreviewToken,
    DfsPlacementReconciliationActivityCursor, DfsPlacementReconciliationActivityPage,
    DfsPlacementReconciliationActivityStatus, DfsPlacementReconciliationActivitySummary,
    DfsPlacementReconciliationJob, DfsPlacementReconciliationState, DfsPlacementScopeResolution,
    DfsResolvedPlacementPolicy, MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE,
    bind_placement_preview_token, merge_placement_impact, normalize_placement_rule,
    placement_reconciliation_activity_page, placement_reconciliation_compatibility_diagnostic,
    placement_rule_matches, preview_placement,
    preview_placement_with_snapshot_and_unavailable_nodes,
    preview_placement_with_unavailable_nodes, publish_dfs_content_advertisement_async,
    publish_dfs_content_advertisement_if_needed, publish_dfs_content_advertisement_if_needed_async,
    remove_placement_rule, resolve_placement_policy, upsert_placement_rule,
};
pub use placement_environment::{
    DEFAULT_DFS_STORAGE_OBSERVATION_TTL, DEFAULT_DFS_STORAGE_RESERVE_BYTES,
    DEFAULT_DFS_STORAGE_SAMPLE_INTERVAL, DFS_NODE_HEALTH_SNAPSHOT_SCHEMA_VERSION,
    DFS_PLACEMENT_SNAPSHOT_SCHEMA_VERSION, DfsContentLocationConfig, DfsContentLocationLifecycle,
    DfsContentLocationObservation, DfsDeviceHealthMonitor, DfsHealthState, DfsNetworkMetering,
    DfsNodeHealthSnapshot, DfsObservationFreshness, DfsPlacementNodeObservation,
    DfsPlacementPathObservation, DfsPlacementPathState, DfsPlacementSnapshot,
    DfsPlacementSnapshotCache, explain_replicas_with_placement_snapshot,
};
pub use posix::sync::{DfsPosixDirectorySyncCertificate, DfsPosixFileSyncCertificate};
pub use posix_model::{
    DfsDirEntry, DfsDirEntryPage, DfsNamespaceJobFailure, DfsNamespaceJobMaintenanceReport,
    DfsPosixCreateAttrs, DfsPosixDirectorySyncTarget, DfsPosixDistributedFilePrefixReadOutcome,
    DfsPosixDistributedFileReadOutcome, DfsPosixDistributedOpenedFileRangeReadOutcome,
    DfsPosixError, DfsPosixFilePrefixRead, DfsPosixFileRangeReadStream, DfsPosixFileReadStream,
    DfsPosixFileSyncEvidence, DfsPosixFileSyncTarget, DfsPosixRemoteFileRangeReadStream,
    DfsPosixRemoteFileReadStream, DfsPosixSyncKind, DfsStat, DfsWriteOptions,
};
pub use protocol::{
    DfsBlobTransferResult, DfsBulkReadItem, DfsBulkReadItemResponse, DfsBulkReadRequest,
    DfsBulkReadResponse, DfsCanonicalAuthorization, DfsCommitVersionCommand,
    DfsCommitVersionResponse, DfsDirectReadRequest, DfsDistributedWriteCommand,
    DfsDistributedWriteResponse, DfsDistributedWriteResult, DfsFetchBlobCommand,
    DfsFetchBlobResponse, DfsFetchBlobResult, DfsLocalEventualBatchWriteProof, DfsMessageCommand,
    DfsMessageResponse, DfsMetadataAuthorityOperation, DfsMetadataAuthorityProbe,
    DfsMetadataDurableReplication, DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataJournalBatchResult, DfsMetadataPrefixProbe, DfsMetadataSnapshotInstallCommand,
    DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult,
    DfsNamespaceChangePollRequest, DfsNamespaceChangePollResponse, DfsNamespaceCommand,
    DfsNamespaceDestinationPrecondition, DfsNamespaceObjectPrecondition, DfsNamespaceOperation,
    DfsNamespacePosixError, DfsNamespaceResponse, DfsNamespaceResult, DfsNamespaceShardCursor,
    DfsNetworkMessage, DfsPosixCoordinationResult, DfsReadData, DfsReadRangeData, DfsReadRequest,
    DfsReadResponse, DfsReadResult, DfsRepairReplicaCommand, DfsRepairReplicaResponse,
    DfsRoutedMessage, DfsStableNamespaceOperationReceipt,
    DfsStableNamespaceOperationReconciliation, DfsStableNamespaceReconciliationRequest,
    DfsStableNamespaceReconciliationResponse, DfsStableWriteOperationReceipt,
    DfsStableWriteOperationReconciliation, DfsStableWriteReconciliationRequest,
    DfsStableWriteReconciliationResponse, DfsStageBlobCommand, DfsStageBlobResponse,
    DfsStagePatchCommand, DfsWriteCommand, DfsWriteResponse, DfsWriteResult,
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
pub use repair_queue_store::{
    DfsRepairQueueBoundedStatus, DfsRepairQueueCompactionOptions, DfsRepairQueueCompactionReport,
    DfsRepairQueueKey, DfsRepairQueueOperation, DfsRepairQueueReconcileOutcome,
    DfsRepairQueueStatusView, DfsRepairQueueView, LocalDfsRepairQueueStore,
};
pub use routes::{
    DFS_CONTROL_PLANE_ROUTES, DFS_DATA_BULK_READ_RESPONSE_ROUTE, DFS_DATA_BULK_READ_ROUTE,
    DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE, DFS_DATA_COMMIT_VERSION_ROUTE,
    DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_RESPONSE_ROUTE, DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE,
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
    dfs_metadata_journal_batch_execution_scope, dfs_metadata_journal_batch_replayable_rpc_call,
    dfs_metadata_journal_batch_rpc_call, dfs_metadata_journal_batch_rpc_request,
    dfs_metadata_snapshot_install_execution_scope, dfs_metadata_snapshot_install_rpc_call,
    dfs_metadata_snapshot_install_rpc_request, dfs_route_is_message_data_plane,
    dfs_routed_message_rpc_request, dfs_smoke_rpc_request, dfs_status_rpc_request,
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

pub use default_reconciliation::{
    DefaultDfsExactRetryGeneration, DefaultDfsParkedHandoff, DefaultDfsPermitGeneration,
    DefaultDfsReconciliationProjection, DefaultDfsReconciliationRuntime,
    DefaultDfsReconciliationState, DefaultDfsReconciliationWork,
    completed_promoted_primary_handoff_matches, default_dfs_locally_owned_pending_handoff,
    notify_default_dfs_reconciliation_replica_progress,
};
#[cfg(any(test, feature = "test-support"))]
pub use default_reconciliation::{
    DefaultDfsReconciliationWakeSubscription, subscribe_default_dfs_reconciliation_replica_progress,
};
pub use metadata_authority_projection::{
    DfsMetadataShardsByFileSystem, dfs_metadata_authority_key_is_exact,
    dfs_metadata_shard_config_key_is_exact, dfs_metadata_shard_config_key_parts,
    metadata_replica_context_from_authority_entries,
    metadata_replica_context_from_authority_projection,
    metadata_replica_context_matches_authority_entries,
    metadata_shard_index_from_authority_entries, metadata_shards_from_authority_projection,
    namespace_command_matches_authority_index,
};

pub use service::{
    DfsDataPlaneHost, DfsDataPlaneHostError, DfsDataPlaneHostFuture, DfsDataPlaneResponse,
    DfsDataPlaneService, DfsInboundAttachment, DfsService, register_data_plane_services,
    register_services, register_services_with_data_plane_host,
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
    DfsConfirmedTimeoutOutcome, DfsConflictReadView, DfsCrossShardNamespaceLockRegistry,
    DfsMessageCommittedWork, DfsMessageProcessReport, DfsMessageProcessResult,
    DfsMetadataReplicationOutcome, DfsOwnerHandoffOutcome, DfsReconnectReconciliation,
    NetworkedDfsRangeReadOutcome, NetworkedDfsReadFreshness, NetworkedDfsReadOutcome,
    NetworkedDfsService, NetworkedDfsServiceEvent, NetworkedDfsWriteOutcome,
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
