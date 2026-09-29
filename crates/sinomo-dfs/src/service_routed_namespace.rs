//! Routed namespace request and response lifecycle.
//!
//! This module owns metadata-primary namespace routing: serving routed
//! namespace commands, collecting response journal batches, applying synced
//! path objects on requesters, and settling pending namespace operations. The
//! generic data-plane dispatcher remains in `service_protocol`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use sinomo_core::node::NodeId;

use crate::protocol::{
    DFS_NAMESPACE_LOOKUP_INLINE_RANGE_MAX_BYTES, DfsNamespaceLookupRangeRequest,
};
use crate::*;
use crate::{posix_model, posix_path};

fn namespace_change_poll_shard_from_snapshot(
    file_system_id: &DfsFileSystemId,
    request: &DfsNamespaceChangePollRequest,
    shards: &[DfsMetadataShardConfig],
) -> Result<DfsMetadataShardConfig, DfsError> {
    if request.cursor.file_system_id != *file_system_id {
        return Err(DfsError::Distributed {
            message: format!(
                "namespace change cursor filesystem {} does not match request filesystem {file_system_id}",
                request.cursor.file_system_id
            ),
        });
    }
    crate::dfs_namespace_change_shards_for_subtree(file_system_id, &request.root_path, shards)?
        .into_iter()
        .find(|shard| shard.shard_id == request.cursor.shard_id.0)
        .ok_or_else(|| DfsError::Distributed {
            message: format!(
                "namespace change cursor shard {} is not assigned to subtree {} in the current topology",
                request.cursor.shard_id.0, request.root_path
            ),
        })
}

fn dfs_error_from_posix_namespace_sync(error: posix_model::DfsPosixError) -> DfsError {
    match error {
        posix_model::DfsPosixError::Dfs(error) => error,
        other => DfsError::Distributed {
            message: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::InProcessKvStore;
    use sinomo_core::network::InProcessNetwork;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn durable_namespace_prepare_keeps_ambiguous_failure_pending() {
        let error = posix_model::DfsPosixError::Dfs(DfsError::Distributed {
            message: "downstream operation failed after durable prepare".to_string(),
        });

        assert!(
            !namespace_execution_must_remain_pending(&error, false, false),
            "an ordinary deterministic distributed rejection remains terminal"
        );
        assert!(
            namespace_execution_must_remain_pending(&error, true, false),
            "once a hidden request-owned prepare is durable, recovery—not a contradictory rejection—owns the terminal result"
        );
        assert!(
            namespace_execution_must_remain_pending(&error, false, true),
            "once a request-owned namespace job is durable, its recovery owns the terminal result"
        );
    }

    #[test]
    fn stable_mkdir_replays_durable_result_after_service_restart() {
        let root = sinomo_testdir::TestDir::new("dfs-stable-mkdir-restart").keep();
        let node_id = NodeId::from_legacy_named("stable-mkdir-primary").expect("node");
        let file_system_id = sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: node_id.clone(),
            replica_node_ids: Vec::new(),
            epoch: 3,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&shard)
                .expect("root shard config");
        });

        let metadata_root = root.join("metadata");
        let content_root = root.join("content");
        let mut backend =
            LocalSobjMetadataBackend::new(shard_id.clone(), &metadata_root).expect("metadata");
        backend
            .put_file_system(&crate::namespace::distributed_sifs(node_id.clone()))
            .expect("filesystem");
        for object in crate::namespace::distributed_sifs_bootstrap_objects(node_id.clone()) {
            backend.put_object(&object).expect("bootstrap object");
        }

        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(node_id.clone()),
            LocalContentStore::new(&content_root).expect("content"),
        )
        .with_control_plane(control_plane.clone());
        service.set_metadata_backend_unhydrated(backend);
        service
            .hydrate_from_metadata_backend()
            .expect("hydrate first service");
        let first = service
            .posix_mkdir_with_create_attrs_and_operation_id(
                "/services/cloud-directory",
                DfsPosixCreateAttrs::new(0o750, 1000, 1000),
                "cloud-create-directory-17",
            )
            .expect("first mkdir");
        drop(service);

        let reopened_backend =
            LocalSobjMetadataBackend::new(shard_id, &metadata_root).expect("reopen metadata");
        let mut restarted = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(&content_root).expect("reopen content"),
        )
        .with_control_plane(control_plane);
        restarted.set_metadata_backend_unhydrated(reopened_backend);
        restarted
            .hydrate_from_metadata_backend()
            .expect("hydrate restarted service");
        let replayed = restarted
            .posix_mkdir_with_create_attrs_and_operation_id(
                "/services/cloud-directory",
                DfsPosixCreateAttrs::new(0o750, 1000, 1000),
                "cloud-create-directory-17",
            )
            .expect("exact mkdir replay");
        assert_eq!(replayed.object_id, first.object_id);
        assert_eq!(replayed.version, first.version);

        assert!(matches!(
            restarted.posix_mkdir_with_create_attrs_and_operation_id(
                "/services/cloud-directory",
                DfsPosixCreateAttrs::new(0o700, 1000, 1000),
                "cloud-create-directory-17",
            ),
            Err(DfsPosixError::Conflict { .. })
        ));
    }

    #[test]
    fn local_eventual_batch_response_does_not_pin_metadata_for_empty_sync_payload() {
        let operation = DfsNamespaceOperation::CommitLocalEventualBatch {
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: "visible-batch-response-sync".to_string(),
                file_system_id: sifs_file_system_id(),
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: time::OffsetDateTime::UNIX_EPOCH,
            },
            writes: Vec::new(),
        };

        assert!(
            !namespace_operation_returns_synced_response_objects(&operation),
            "a routed bulk commit returns no path objects and must not refresh every metadata shard"
        );
        assert!(!namespace_request_returns_synced_response_objects(
            false,
            &DfsNamespaceOperation::MkdirAll {
                path: "/services/no-local-replay".to_string(),
            },
        ));
        assert!(namespace_request_returns_synced_response_objects(
            false,
            &DfsNamespaceOperation::LookupPath {
                path: "/services/lookup.txt".to_string(),
                inline_range: None,
            },
        ));
        assert!(namespace_request_returns_synced_response_objects(
            true,
            &DfsNamespaceOperation::MkdirAll {
                path: "/services/read-own-write".to_string(),
            },
        ));
        assert!(namespace_result_was_fused_into_committed_prepare(
            true,
            &DfsNamespaceResult::Applied,
        ));
    }

    #[test]
    fn lookup_paths_pins_one_consistent_metadata_snapshot() {
        let root = sinomo_testdir::TestDir::new("dfs-lookup-paths-pinned-snapshot").keep();
        let node_id = NodeId::from_legacy_named("node_lookup_paths_pinned").expect("node");
        let file_system_id = sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id, &root).expect("metadata backend");
        backend
            .put_file_system(&DfsFileSystem {
                file_history: Default::default(),
                file_system_id: file_system_id.clone(),
                display_name: "SIFS".to_string(),
                owner_node_id: node_id.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("filesystem");
        let mut paths = Vec::new();
        for index in 0..128 {
            let path = format!("/services/pinned-{index:03}.txt");
            paths.push(path.clone());
            backend
                .put_object(&DfsObjectMetadata {
                    file_system_id: file_system_id.clone(),
                    object_id: DfsObjectId::from_legacy_named(format!(
                        "object_lookup_paths_pinned_{index:03}"
                    ))
                    .expect("object"),
                    path,
                    kind: DfsObjectKind::File,
                    tombstone_provenance: None,
                    owner_node_id: node_id.clone(),
                    current_locations: vec![node_id.clone()],
                    content_hash: Some(format!("blake3:pinned-{index:03}")),
                    content_size_bytes: Some(1),
                    link_target_object_id: None,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    accessed_at_unix_nanos: 1,
                    modified_at_unix_nanos: 1,
                    created_at_unix_nanos: 1,
                    changed_at_unix_nanos: 1,
                    version: DfsVersion::from_legacy_named(format!(
                        "version_lookup_paths_pinned_{index:03}"
                    ))
                    .expect("version"),
                    policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                    created_by_event_id: None,
                    last_modified_by_event_id: None,
                })
                .expect("object");
        }
        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).expect("content"),
        );
        service.set_metadata_backend_unhydrated(backend);

        crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
        let objects = service
            .synced_namespace_objects_for_operation(
                &file_system_id,
                &DfsNamespaceOperation::LookupPaths { paths },
            )
            .expect("lookup paths");

        assert_eq!(objects.len(), 128);
        assert!(
            crate::metadata_read_snapshot::metadata_durable_table_probes() <= 1,
            "one routed LookupPaths request must not reopen the durable table generation per path"
        );
    }

    #[test]
    fn namespace_change_poll_routes_each_cursor_to_its_remote_primary() {
        let root = sinomo_testdir::TestDir::new("dfs-routed-namespace-change-feed").keep();
        let requester = NodeId::from_legacy_named("feed-requester").expect("requester");
        let root_primary = NodeId::from_legacy_named("feed-root-primary").expect("root primary");
        let services_primary =
            NodeId::from_legacy_named("feed-services-primary").expect("services primary");
        let file_system_id =
            DfsFileSystemId::from_legacy_named("feed-filesystem").expect("filesystem id");
        let shard =
            |shard_id: &str, primary_node_id: NodeId, assignment: DfsMetadataShardAssignment| {
                DfsMetadataShardConfig {
                    file_system_id: file_system_id.clone(),
                    shard_id: shard_id.to_string(),
                    primary_node_id,
                    replica_node_ids: Vec::new(),
                    epoch: 17,
                    assignment,
                    namespace_prepare_codec: Default::default(),
                    default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                }
            };
        let root_shard = shard(
            "root",
            root_primary.clone(),
            DfsMetadataShardAssignment::Root,
        );
        let services_shard = shard(
            "services",
            services_primary.clone(),
            DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
        );
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            store
                .put_metadata_shard(&root_shard)
                .expect("root shard config");
            store
                .put_metadata_shard(&services_shard)
                .expect("services shard config");
        });

        let file_system = DfsFileSystem {
            file_history: Default::default(),
            file_system_id: file_system_id.clone(),
            display_name: "Feed test".to_string(),
            owner_node_id: root_primary.clone(),
            subtree_policies: Vec::new(),
        };
        let object = |object_id: &str, path: &str, owner_node_id: NodeId| DfsObjectMetadata {
            file_system_id: file_system_id.clone(),
            object_id: DfsObjectId::from_legacy_named(object_id).expect("object id"),
            path: path.to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: owner_node_id.clone(),
            current_locations: vec![owner_node_id],
            content_hash: Some(format!("blake3:{object_id}")),
            content_size_bytes: Some(1),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named(format!("{object_id}-version"))
                .expect("version"),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };

        let mut root_backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.join("root-metadata"),
        )
        .expect("root backend");
        root_backend
            .put_file_system(&file_system)
            .expect("root filesystem");
        root_backend
            .put_object(&object("feed-root-object", "/docs/a", root_primary.clone()))
            .expect("root object");
        let mut services_backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("services"),
            root.join("services-metadata"),
        )
        .expect("services backend");
        services_backend
            .put_file_system(&file_system)
            .expect("services filesystem");
        services_backend
            .put_object(&object(
                "feed-services-object",
                "/services/b",
                services_primary.clone(),
            ))
            .expect("services object");

        let network = InProcessNetwork::new();
        let mut root_service = NetworkedDfsService::new(
            network.add_node(root_primary.clone()),
            LocalContentStore::new(root.join("root-content")).expect("root content"),
        )
        .with_control_plane(control_plane.clone());
        root_service.set_metadata_backend_unhydrated(root_backend);
        let mut services_service = NetworkedDfsService::new(
            network.add_node(services_primary.clone()),
            LocalContentStore::new(root.join("services-content")).expect("services content"),
        )
        .with_control_plane(control_plane.clone());
        services_service.set_metadata_backend_unhydrated(services_backend);
        let mut requester_service = NetworkedDfsService::new(
            network.add_node(requester),
            LocalContentStore::new(root.join("requester-content")).expect("requester content"),
        )
        .with_control_plane(control_plane);

        let stop = Arc::new(AtomicBool::new(false));
        let root_stop = Arc::clone(&stop);
        let services_stop = Arc::clone(&stop);
        let root_worker = std::thread::spawn(move || {
            while !root_stop.load(Ordering::Acquire) {
                root_service.process_incoming().expect("root primary pump");
                std::thread::yield_now();
            }
        });
        let services_worker = std::thread::spawn(move || {
            while !services_stop.load(Ordering::Acquire) {
                services_service
                    .process_incoming()
                    .expect("services primary pump");
                std::thread::yield_now();
            }
        });

        // Reverse input order to prove output determinism and cursor isolation
        // do not depend on request ordering.
        let result = requester_service.poll_namespace_changes_for_subtree_with_timeout(
            file_system_id.clone(),
            "/",
            &[
                DfsNamespaceChangeCursor::from_shard_config(&services_shard, 0),
                DfsNamespaceChangeCursor::from_shard_config(&root_shard, 0),
            ],
            Duration::from_secs(5),
        );
        stop.store(true, Ordering::Release);
        root_worker.join().expect("root primary worker");
        services_worker.join().expect("services primary worker");
        let batches = result.expect("routed namespace change poll");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].next_cursor.shard_id.0, "root");
        assert_eq!(batches[1].next_cursor.shard_id.0, "services");
        for batch in &batches {
            assert!(!batch.rescan_required(), "unexpected rescan: {batch:?}");
            assert!(batch.next_cursor.after_sequence > 0);
        }
        assert!(batches[0].events.iter().any(|event| matches!(
            event,
            DfsNamespaceChangeEvent::Upsert { path, .. } if path == "/docs/a"
        )));
        assert!(batches[1].events.iter().any(|event| matches!(
            event,
            DfsNamespaceChangeEvent::Upsert { path, .. } if path == "/services/b"
        )));

        let mut stale_root = DfsNamespaceChangeCursor::from_shard_config(&root_shard, 9);
        stale_root.authority_epoch -= 1;
        let isolated_rescans = requester_service
            .poll_namespace_changes_for_subtree_with_timeout(
                file_system_id,
                "/",
                &[stale_root],
                Duration::from_millis(10),
            )
            .expect("stale and missing cursors rescan without network polling");
        assert_eq!(isolated_rescans.len(), 2);
        assert!(matches!(
            isolated_rescans[0].events.as_slice(),
            [DfsNamespaceChangeEvent::RescanRequired {
                reason: DfsNamespaceRescanReason::AuthorityEpochChanged,
                ..
            }]
        ));
        assert_eq!(
            isolated_rescans[0].next_cursor.authority_epoch, root_shard.epoch,
            "an epoch-change rescan must expose the current durable boundary"
        );
        assert_eq!(isolated_rescans[0].next_cursor.after_sequence, 9);
        assert!(matches!(
            isolated_rescans[1].events.as_slice(),
            [DfsNamespaceChangeEvent::RescanRequired {
                reason: DfsNamespaceRescanReason::ShardChanged,
                ..
            }]
        ));
    }
}

fn namespace_authority_shards_match(
    stamped: &[DfsMetadataShardConfig],
    current: &[DfsMetadataShardConfig],
) -> bool {
    if stamped.len() != current.len() {
        return false;
    }
    let mut matched = vec![false; current.len()];
    for stamped_shard in stamped {
        let Some((index, _)) = current
            .iter()
            .enumerate()
            .find(|(index, current_shard)| !matched[*index] && *current_shard == stamped_shard)
        else {
            return false;
        };
        matched[index] = true;
    }
    true
}

fn metadata_path_is_direct_child_of_posix_dir(directory_path: &str, metadata_path: &str) -> bool {
    let child_path = if metadata_path.is_empty() {
        "/".to_string()
    } else {
        format!("/{metadata_path}")
    };
    if child_path == directory_path {
        return false;
    }
    let prefix = if directory_path == "/" {
        "/".to_string()
    } else {
        format!("{}/", directory_path.trim_end_matches('/'))
    };
    let Some(rest) = child_path.strip_prefix(&prefix) else {
        return false;
    };
    !rest.is_empty() && !rest.contains('/')
}

fn namespace_operation_should_replay_journal_batches(operation: &DfsNamespaceOperation) -> bool {
    !matches!(
        operation,
        DfsNamespaceOperation::FileHistoryQuery { .. }
            | DfsNamespaceOperation::PollNamespaceChanges { .. }
            | DfsNamespaceOperation::LookupPath { .. }
            | DfsNamespaceOperation::LookupPaths { .. }
            | DfsNamespaceOperation::AdvisoryLock { .. }
            | DfsNamespaceOperation::FileSync { .. }
            | DfsNamespaceOperation::DirectorySync { .. }
            | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. }
            | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. }
    )
}

fn namespace_operation_returns_synced_response_objects(operation: &DfsNamespaceOperation) -> bool {
    matches!(
        operation,
        DfsNamespaceOperation::SyncPath { .. }
            | DfsNamespaceOperation::LookupPath { .. }
            | DfsNamespaceOperation::LookupPaths { .. }
            | DfsNamespaceOperation::ReconcilePlacement { .. }
            | DfsNamespaceOperation::Mkdir { .. }
            | DfsNamespaceOperation::MkdirWithAttrs { .. }
            | DfsNamespaceOperation::MkdirAtNoLinks { .. }
            | DfsNamespaceOperation::MkdirAll { .. }
    )
}

fn namespace_request_returns_synced_response_objects(
    include_metadata_journal_batches: bool,
    operation: &DfsNamespaceOperation,
) -> bool {
    // Lookup and placement use the owner's object result directly. Other
    // mutations request object payloads only for foreground response replay.
    matches!(
        operation,
        DfsNamespaceOperation::SyncPath { .. }
            | DfsNamespaceOperation::LookupPath { .. }
            | DfsNamespaceOperation::LookupPaths { .. }
            | DfsNamespaceOperation::ReconcilePlacement { .. }
    ) || (include_metadata_journal_batches
        && namespace_operation_returns_synced_response_objects(operation))
}

fn namespace_result_was_fused_into_committed_prepare(
    durable_prepare_recorded: bool,
    result: &DfsNamespaceResult,
) -> bool {
    durable_prepare_recorded && matches!(result, DfsNamespaceResult::Applied)
}

fn namespace_operation_requires_fresh_authority(operation: &DfsNamespaceOperation) -> bool {
    matches!(
        operation,
        DfsNamespaceOperation::AdvisoryLock { .. }
            // Placement has its own durable job, checked config permit and
            // deterministic per-object/version repair identity. A generic
            // namespace receipt duplicates that owner and turns a remote
            // no-op into a journal write back to its maintenance coordinator.
            | DfsNamespaceOperation::ReconcilePlacement { .. }
            | DfsNamespaceOperation::FileSync { .. }
            | DfsNamespaceOperation::DirectorySync { .. }
            | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. }
            | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. }
    )
}

fn namespace_operation_uses_durable_request_admission(operation: &DfsNamespaceOperation) -> bool {
    !namespace_operation_requires_fresh_authority(operation)
        && !matches!(
            operation,
            DfsNamespaceOperation::FileHistoryQuery { .. }
                | DfsNamespaceOperation::PollNamespaceChanges { .. }
                | DfsNamespaceOperation::LookupPath { .. }
                | DfsNamespaceOperation::LookupPaths { .. }
                | DfsNamespaceOperation::SyncPath { .. }
        )
}

/// Operations whose complete mutation is carried by one request-owned
/// namespace prepare can share that prepare's durable table batch instead of
/// forcing a standalone Pending journal write first.
///
/// The transaction planner still verifies that the exact operation is proven
/// complete. Symlink-resolved or otherwise non-proving transactions fall back
/// to a standalone Pending admission before their prepare.
fn namespace_operation_can_fuse_durable_request_admission(
    operation: &DfsNamespaceOperation,
) -> bool {
    matches!(
        operation,
        DfsNamespaceOperation::MkdirAll { .. }
            | DfsNamespaceOperation::CommitLocalEventualBatch { .. }
    )
}

fn namespace_prepare_matches_routed_command(
    prepare: &DfsNamespacePrepareRecord,
    requester_node_id: &NodeId,
    command: &DfsNamespaceCommand,
    shard_id: &DfsMetadataShardId,
    command_digest: &str,
) -> bool {
    let expected_operation_id =
        namespace_request_operation_id(requester_node_id, &command.request_id);
    let attachment_matches = prepare.completed_request.as_ref().is_some_and(|request| {
        request.file_system_id == command.file_system_id
            && request.requester_node_id == *requester_node_id
            && request.request_id == command.request_id
            && request.operation_id == expected_operation_id
            && request.authority_shard_id == *shard_id
            && request.command_digest == command_digest
    }) || prepare.completed_request_completion.as_ref().is_some_and(
        |completion| {
            completion.file_system_id == command.file_system_id
                && completion.requester_node_id == *requester_node_id
                && completion.request_id == command.request_id
                && completion.operation_id == expected_operation_id
                && completion.authority_shard_id == *shard_id
                && completion.command_digest == command_digest
        },
    );
    attachment_matches
        && prepare.file_system_id == command.file_system_id
        && prepare.shard_id == *shard_id
        && prepare
            .operation_id
            .starts_with(&format!("namespace-request:{expected_operation_id}:"))
}

pub(crate) fn namespace_operation_response_retry_safe(operation: &DfsNamespaceOperation) -> bool {
    namespace_operation_uses_durable_request_admission(operation)
        || matches!(
            operation,
            DfsNamespaceOperation::FileHistoryQuery { .. }
                | DfsNamespaceOperation::PollNamespaceChanges { .. }
                | DfsNamespaceOperation::LookupPath { .. }
                | DfsNamespaceOperation::LookupPaths { .. }
                | DfsNamespaceOperation::SyncPath { .. }
                | DfsNamespaceOperation::AdvisoryLock { .. }
                | DfsNamespaceOperation::FileSync { .. }
                | DfsNamespaceOperation::DirectorySync { .. }
                | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. }
                | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
                | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. }
                | DfsNamespaceOperation::ReconcilePlacement { .. }
                | DfsNamespaceOperation::AdoptObjectAtPath {
                    pin_transfer_id: Some(_),
                    ..
                }
        )
}

pub(crate) fn namespace_command_digest(command: &DfsNamespaceCommand) -> Result<String, DfsError> {
    let encoded = serde_json::to_vec(&(
        &command.file_system_id,
        &command.requesting_node_id,
        &command.operation,
    ))
    .map_err(|source| DfsError::Distributed {
        message: format!("failed to encode namespace command digest: {source}"),
    })?;
    Ok(format!("blake3:{}", blake3::hash(&encoded).to_hex()))
}

const DFS_NAMESPACE_OPERATION_ID_MAX_BYTES: usize = 256;

pub(crate) fn validate_namespace_operation_id(operation_id: &str) -> Result<(), DfsError> {
    if operation_id.is_empty()
        || operation_id.len() > DFS_NAMESPACE_OPERATION_ID_MAX_BYTES
        || operation_id.chars().any(char::is_control)
    {
        return Err(DfsError::Serialization {
            message: format!(
                "namespace operation id must contain 1..={DFS_NAMESPACE_OPERATION_ID_MAX_BYTES} bytes and no control characters"
            ),
        });
    }
    Ok(())
}

fn namespace_reconciliation_conflict(
    command: &DfsNamespaceCommand,
    reason: impl Into<String>,
) -> Option<DfsNamespacePosixError> {
    command
        .operation
        .routing_path()
        .map(|path| DfsNamespacePosixError::Conflict {
            path: path.to_string(),
            reason: reason.into(),
        })
}

fn stable_namespace_operation_receipt_from_request(
    operation_id: &str,
    request: &DfsNamespaceRequestRecord,
    result: DfsNamespaceResult,
) -> DfsStableNamespaceOperationReceipt {
    DfsStableNamespaceOperationReceipt {
        operation_id: operation_id.to_string(),
        file_system_id: request.file_system_id.clone(),
        requesting_node_id: request.requester_node_id.clone(),
        authority_shard_id: request.authority_shard_id.clone(),
        command_digest: request.command_digest.clone(),
        result,
        posix_coordination_result: request.posix_coordination_result.clone(),
    }
}

fn stable_namespace_operation_receipt_from_receipt(
    operation_id: &str,
    receipt: &DfsNamespaceRequestReceipt,
) -> DfsStableNamespaceOperationReceipt {
    DfsStableNamespaceOperationReceipt {
        operation_id: operation_id.to_string(),
        file_system_id: receipt.file_system_id.clone(),
        requesting_node_id: receipt.requester_node_id.clone(),
        authority_shard_id: receipt.authority_shard_id.clone(),
        command_digest: receipt.command_digest.clone(),
        result: receipt.result.clone(),
        posix_coordination_result: receipt.posix_coordination_result.clone(),
    }
}

fn classify_stable_namespace_operation(
    operation_id: &str,
    file_system_id: &DfsFileSystemId,
    requester_node_id: &NodeId,
    command_digest: &str,
    command: &DfsNamespaceCommand,
    snapshot: Option<&DfsNamespaceRequestAdmissionSnapshot>,
    locally_authoritative: bool,
) -> DfsStableNamespaceOperationReconciliation {
    let Some(snapshot) = snapshot else {
        return DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
            reason: format!(
                "namespace operation {operation_id} has no locally readable durable authority shard"
            ),
        };
    };
    if let Some(receipt) = snapshot.receipt.as_ref() {
        if receipt.file_system_id != *file_system_id
            || receipt.requester_node_id != *requester_node_id
            || receipt.request_id != operation_id
            || receipt.command_digest != command_digest
        {
            return DfsStableNamespaceOperationReconciliation::Conflict {
                reason: format!(
                    "namespace operation id {operation_id} is bound to a different durable command digest"
                ),
            };
        }
        return match &receipt.result {
            DfsNamespaceResult::Applied => DfsStableNamespaceOperationReconciliation::Applied {
                receipt: stable_namespace_operation_receipt_from_receipt(operation_id, receipt),
            },
            DfsNamespaceResult::Rejected { reason, .. } => {
                DfsStableNamespaceOperationReconciliation::Conflict {
                    reason: format!(
                        "namespace operation {operation_id} terminated without applying: {reason}"
                    ),
                }
            }
        };
    }
    if let Some(request) = snapshot.request.as_ref() {
        if request.file_system_id != *file_system_id
            || request.requester_node_id != *requester_node_id
            || request.request_id != operation_id
            || request.command_digest != command_digest
        {
            return DfsStableNamespaceOperationReconciliation::Conflict {
                reason: format!(
                    "namespace operation id {operation_id} is bound to a different durable command digest"
                ),
            };
        }
        return match request.status {
            DfsNamespaceRequestStatus::Pending => {
                DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
                    reason: format!(
                        "namespace operation {operation_id} has a durable pending admission awaiting recovery"
                    ),
                }
            }
            DfsNamespaceRequestStatus::Completed => match request.result.clone() {
                Some(result @ DfsNamespaceResult::Applied) => {
                    DfsStableNamespaceOperationReconciliation::Applied {
                        receipt: stable_namespace_operation_receipt_from_request(
                            operation_id,
                            request,
                            result,
                        ),
                    }
                }
                Some(DfsNamespaceResult::Rejected { reason, .. }) => {
                    DfsStableNamespaceOperationReconciliation::Conflict {
                        reason: format!(
                            "namespace operation {operation_id} terminated without applying: {reason}"
                        ),
                    }
                }
                None => DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
                    reason: format!(
                        "namespace operation {operation_id} is marked completed without a durable result"
                    ),
                },
            },
        };
    }
    if let Some(prepare) = snapshot.prepared.as_ref() {
        if !namespace_prepare_matches_routed_command(
            prepare,
            requester_node_id,
            command,
            &prepare.shard_id,
            command_digest,
        ) {
            return DfsStableNamespaceOperationReconciliation::Conflict {
                reason: format!(
                    "namespace operation id {operation_id} owns a prepare for a different durable command digest"
                ),
            };
        }
        return DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
            reason: format!(
                "namespace operation {operation_id} has a durable hidden prepare awaiting recovery"
            ),
        };
    }
    if snapshot.history_expired {
        return DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
            reason: format!(
                "namespace operation {operation_id} has no exact receipt and its durable history expired"
            ),
        };
    }
    if locally_authoritative {
        DfsStableNamespaceOperationReconciliation::NotApplied
    } else {
        DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
            reason: format!(
                "namespace operation {operation_id} has no local exact receipt and this node is not the current namespace authority"
            ),
        }
    }
}

fn namespace_request_operation_id(requester_node_id: &NodeId, request_id: &str) -> String {
    let identity = format!("{requester_node_id}\0{request_id}");
    format!(
        "req-{}",
        &blake3::hash(identity.as_bytes()).to_hex().as_str()[..24]
    )
}

enum DurableNamespaceRequestAdmission {
    Execute {
        shard_id: DfsMetadataShardId,
        record: DfsNamespaceRequestRecord,
        persisted: bool,
        replaying_pending: bool,
    },
    Replay {
        result: DfsNamespaceResult,
        posix_coordination_result: Option<DfsPosixCoordinationResult>,
    },
    Reject {
        reason: String,
        posix_error: Option<DfsNamespacePosixError>,
    },
}

pub(crate) const DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT: usize = 64;
const DFS_NAMESPACE_REQUEST_RECOVERY_SCAN_PAGE_LIMIT: usize = 4;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsNamespaceRequestRecoveryReport {
    pub(crate) scanned: usize,
    pub(crate) resolved: usize,
    pub(crate) remaining: usize,
    pub(crate) failures: Vec<String>,
}

fn transaction_has_object_at(
    transaction: &DfsNamespaceTransaction,
    path: &str,
    predicate: impl Fn(&DfsObjectMetadata) -> bool,
) -> bool {
    let Ok(path) = posix_path::normalize_posix_path(path) else {
        return false;
    };
    transaction.objects.iter().any(|object| {
        posix_path::normalize_posix_path(&object.path).is_ok_and(|candidate| candidate == path)
            && predicate(object)
    })
}

fn namespace_transaction_proves_operation_completed(
    operation: &DfsNamespaceOperation,
    transaction: &DfsNamespaceTransaction,
) -> bool {
    match operation {
        DfsNamespaceOperation::FileHistoryPin { request } => {
            if transaction.file_system_id != request.identity.file_system_id
                || transaction.transaction_id != format!("file-history:{}", request.operation_id)
            {
                return false;
            }
            let Some(pins) = &transaction.file_version_pins else {
                return false;
            };
            let (candidates, pin_id) = match &request.operation {
                DfsFileHistoryPinOperation::Capture { pin_id }
                | DfsFileHistoryPinOperation::Pin { pin_id } => (&pins.add, pin_id),
                DfsFileHistoryPinOperation::Unpin { pin_id } => (&pins.remove, pin_id),
            };
            candidates.iter().any(|pin| {
                pin.file_system_id == request.identity.file_system_id
                    && pin.object_id == request.identity.object_id
                    && pin.version == request.identity.version
                    && &pin.pin_id == pin_id
            })
        }
        DfsNamespaceOperation::Mkdir { path }
        | DfsNamespaceOperation::MkdirWithAttrs { path, .. }
        | DfsNamespaceOperation::MkdirAtNoLinks { path, .. }
        | DfsNamespaceOperation::MkdirAll { path } => {
            transaction_has_object_at(transaction, path, |object| {
                object.kind == DfsObjectKind::Directory
            })
        }
        DfsNamespaceOperation::Rmdir { path }
        | DfsNamespaceOperation::ConditionalRmdir { path, .. }
        | DfsNamespaceOperation::Unlink { path }
        | DfsNamespaceOperation::ConditionalUnlink { path, .. } => {
            transaction_has_object_at(transaction, path, |object| {
                object.kind == DfsObjectKind::Tombstone
            })
        }
        DfsNamespaceOperation::Rename { from, to }
        | DfsNamespaceOperation::ConditionalRename { from, to, .. }
        | DfsNamespaceOperation::RenameSingleNamespaceAtomic { from, to, .. } => {
            transaction_has_object_at(transaction, from, |object| {
                object.kind == DfsObjectKind::Tombstone
            }) && transaction_has_object_at(transaction, to, |object| {
                object.kind != DfsObjectKind::Tombstone
            })
        }
        DfsNamespaceOperation::Hardlink { link_path, .. } => {
            transaction_has_object_at(transaction, link_path, |object| {
                object.kind == DfsObjectKind::Hardlink
            })
        }
        DfsNamespaceOperation::CommitHardlinkTargetFence {
            target_path,
            committed_target,
            ..
        } => transaction_has_object_at(transaction, target_path, |object| {
            object == committed_target && object.kind != DfsObjectKind::Tombstone
        }),
        DfsNamespaceOperation::SetMode { .. }
        | DfsNamespaceOperation::SetOwner { .. }
        | DfsNamespaceOperation::SetSymlinkOwner { .. }
        | DfsNamespaceOperation::SetTimes { .. } => !transaction.objects.is_empty(),
        DfsNamespaceOperation::TouchDirectoryModified { path } => {
            transaction_has_object_at(transaction, path, |object| {
                object.kind == DfsObjectKind::Directory
            })
        }
        DfsNamespaceOperation::CommitLocalEventualBatch {
            transaction: expected,
            ..
        } => expected.transaction_id == transaction.transaction_id,
        DfsNamespaceOperation::AdoptObjectAtPath { object, .. } => {
            transaction.objects.iter().any(|candidate| {
                candidate.object_id == object.object_id && candidate.path == object.path
            })
        }
        DfsNamespaceOperation::AdoptHardlinkAlias { alias, .. } => {
            transaction.objects.iter().any(|candidate| {
                candidate.object_id == alias.object_id && candidate.path == alias.path
            })
        }
        DfsNamespaceOperation::BeginFileHistoryRestore { .. }
        | DfsNamespaceOperation::FileHistoryQuery { .. }
        | DfsNamespaceOperation::PollNamespaceChanges { .. }
        | DfsNamespaceOperation::InstallHardlinkTargetFence { .. }
        | DfsNamespaceOperation::RenameDirectoryBounded { .. }
        | DfsNamespaceOperation::ResumeNamespaceJobBounded { .. }
        | DfsNamespaceOperation::ClearDirRecursive { .. }
        | DfsNamespaceOperation::ReconcilePlacement { .. }
        | DfsNamespaceOperation::SyncPath { .. }
        | DfsNamespaceOperation::LookupPath { .. }
        | DfsNamespaceOperation::LookupPaths { .. }
        | DfsNamespaceOperation::AdvisoryLock { .. }
        | DfsNamespaceOperation::FileSync { .. }
        | DfsNamespaceOperation::DirectorySync { .. }
        | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. }
        | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
        | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. } => false,
    }
}

fn namespace_execution_error_is_retriable(error: &posix_model::DfsPosixError) -> bool {
    matches!(
        error,
        posix_model::DfsPosixError::PendingWrite { .. }
            | posix_model::DfsPosixError::SymlinkContentPending { .. }
            | posix_model::DfsPosixError::PartialBulkCommit { .. }
            | posix_model::DfsPosixError::Metadata(
                MetadataBackendError::Io { .. }
                    | MetadataBackendError::MembershipHandoffFrozen { .. }
                    | MetadataBackendError::Conflict { .. }
            )
            | posix_model::DfsPosixError::Dfs(
                DfsError::Network { .. }
                    | DfsError::StaleOwnerEpoch { .. }
                    | DfsError::MetadataShardMembershipHandoffParked { .. }
                    | DfsError::CommitContentRepairPending { .. }
                    | DfsError::Metadata { .. }
                    | DfsError::ContentStore { .. }
                    | DfsError::Busy { .. }
                    | DfsError::Cancelled { .. }
            )
    )
}

fn namespace_execution_must_remain_pending(
    error: &posix_model::DfsPosixError,
    durable_prepare_recorded: bool,
    durable_job_recorded: bool,
) -> bool {
    durable_prepare_recorded
        || durable_job_recorded
        || namespace_execution_error_is_retriable(error)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn namespace_operation_authority_shards_from_snapshot(
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        match operation {
            DfsNamespaceOperation::FileHistoryQuery { query } => {
                let shard = shards
                    .iter()
                    .find(|shard| {
                        shard.file_system_id == *file_system_id
                            && shard.shard_id == query.shard_id.0
                            && shard.epoch == query.epoch
                            && query
                                .selection
                                .supports_codec(shard.namespace_prepare_codec)
                    })
                    .filter(|_| query.file_system_id == *file_system_id)
                    .ok_or_else(|| DfsError::Metadata {
                        message: "history query names a stale or foreign shard authority".into(),
                    })?;
                Ok(vec![shard.clone()])
            }
            DfsNamespaceOperation::PollNamespaceChanges { request } => {
                Ok(vec![namespace_change_poll_shard_from_snapshot(
                    file_system_id,
                    request,
                    shards,
                )?])
            }
            _ => Self::affected_namespace_shards_from_snapshot(file_system_id, operation, shards),
        }
    }

    fn namespace_operation_authority_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(Vec::new());
        }
        let snapshot = self
            .metadata_shard_config_snapshot(file_system_id)?
            .unwrap_or_default();
        Self::namespace_operation_authority_shards_from_snapshot(
            file_system_id,
            operation,
            &snapshot,
        )
    }

    fn namespace_operation_dispatch_shard(
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        authority_shards: &[DfsMetadataShardConfig],
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        if let DfsNamespaceOperation::FileHistoryQuery { query } = operation {
            return authority_shards
                .iter()
                .find(|shard| {
                    shard.shard_id == query.shard_id.0
                        && shard.file_system_id == *file_system_id
                        && shard.epoch == query.epoch
                })
                .cloned()
                .map(Some)
                .ok_or_else(|| DfsError::Metadata {
                    message: "history query authority changed".into(),
                });
        }
        if let DfsNamespaceOperation::PollNamespaceChanges { request } = operation {
            return authority_shards
                .iter()
                .find(|shard| shard.shard_id == request.cursor.shard_id.0)
                .cloned()
                .map(Some)
                .ok_or_else(|| DfsError::Distributed {
                    message: format!(
                        "namespace change cursor shard {} has no current dispatch authority",
                        request.cursor.shard_id.0
                    ),
                });
        }
        let Some(routing_path) = operation.routing_path() else {
            return Ok(None);
        };
        crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
            file_system_id,
            authority_shards,
            routing_path,
        )
        .map_err(DfsError::from)
    }

    /// Inspect the durable receipt/admission for one caller-owned namespace
    /// mutation without routing or executing that mutation.
    ///
    /// Exact receipts on a local replica can prove `Applied`. Only a current
    /// dispatch authority with a history-complete absence may report
    /// `NotApplied`; pending prepares, expired history, and off-authority
    /// absence remain fail-closed.
    fn reconcile_stable_namespace_operation_locally_for_requester(
        &mut self,
        operation_id: &str,
        operation: &DfsNamespaceOperation,
        requester_node_id: &NodeId,
        authority_shards: &[DfsMetadataShardConfig],
        locally_authoritative: bool,
    ) -> Result<DfsStableNamespaceOperationReconciliation, DfsError> {
        let file_system_id = sifs_file_system_id();
        let command = DfsNamespaceCommand {
            request_id: operation_id.to_string(),
            file_system_id: file_system_id.clone(),
            requesting_node_id: requester_node_id.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: authority_shards.to_vec(),
            operation: operation.clone(),
        };
        let command_digest = namespace_command_digest(&command)?;
        let snapshot = if let Some(backend) = self.metadata_backend.as_mut() {
            let local_shard_ids = backend
                .metadata_shard_statuses()?
                .into_iter()
                .map(|status| status.shard_id)
                .collect::<BTreeSet<_>>();
            let local_authority_shard = authority_shards
                .iter()
                .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
                .find(|shard_id| local_shard_ids.contains(shard_id));
            match local_authority_shard {
                Some(shard_id) => Some(backend.namespace_request_admission_snapshot(
                    &shard_id,
                    requester_node_id,
                    operation_id,
                )?),
                None => None,
            }
        } else {
            None
        };
        Ok(classify_stable_namespace_operation(
            operation_id,
            &file_system_id,
            requester_node_id,
            &command_digest,
            &command,
            snapshot.as_ref(),
            locally_authoritative,
        ))
    }

    fn wait_for_stable_namespace_reconciliation(
        &mut self,
        request_id: &str,
        timeout: std::time::Duration,
    ) -> Result<DfsStableNamespaceOperationReconciliation, DfsError> {
        let started = std::time::Instant::now();
        loop {
            self.process_incoming()?;
            if let Some(result) = self
                .stable_namespace_reconciliation_results
                .remove(request_id)
            {
                self.stable_namespace_reconciliation_targets
                    .remove(request_id);
                return Ok(result);
            }
            if started.elapsed() >= timeout {
                self.stable_namespace_reconciliation_targets
                    .remove(request_id);
                return Ok(
                    DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
                        reason: format!(
                            "stable namespace reconciliation {request_id} timed out before the current authority replied"
                        ),
                    },
                );
            }
            self.check_cancelled("stable namespace reconciliation")?;
            let remaining = timeout.saturating_sub(started.elapsed());
            self.transport
                .wait_for_incoming(std::time::Duration::from_millis(10).min(remaining))
                .map_err(DfsError::from_network)?;
        }
    }

    pub fn reconcile_stable_namespace_operation(
        &mut self,
        operation_id: &str,
        operation: &DfsNamespaceOperation,
    ) -> Result<DfsStableNamespaceOperationReconciliation, DfsError> {
        validate_namespace_operation_id(operation_id)?;
        if !namespace_operation_uses_durable_request_admission(operation) {
            return Err(DfsError::Serialization {
                message: format!(
                    "namespace operation {operation_id} does not use durable mutation admission and cannot be reconciled as a stable mutation"
                ),
            });
        }
        let file_system_id = sifs_file_system_id();
        if self.control_plane.is_none() {
            return Ok(
                DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
                    reason: format!(
                        "namespace operation {operation_id} ran without a clustered durable-admission authority"
                    ),
                },
            );
        }

        self.refresh_control_plane_latest()?;
        let authority_shards =
            self.namespace_operation_authority_shards(&file_system_id, operation)?;
        let dispatch = Self::namespace_operation_dispatch_shard(
            &file_system_id,
            operation,
            &authority_shards,
        )?
        .ok_or_else(|| DfsError::Distributed {
            message: format!(
                "stable namespace reconciliation for {operation_id} has no current dispatch authority"
            ),
        })?;
        let requester_node_id = self.node_id.clone();
        if dispatch.primary_node_id == self.node_id {
            return self.reconcile_stable_namespace_operation_locally_for_requester(
                operation_id,
                operation,
                &requester_node_id,
                &authority_shards,
                true,
            );
        }
        let request_id = self.next_request_id();
        let authority_node_id = dispatch.primary_node_id.clone();
        self.stable_namespace_reconciliation_targets
            .insert(request_id.clone(), authority_node_id.clone());
        let sent = self.send_dfs_message_if_routable(
            &authority_node_id,
            DfsNetworkMessage::StableNamespaceReconciliationRequest(
                DfsStableNamespaceReconciliationRequest {
                    request_id: request_id.clone(),
                    operation_id: operation_id.to_string(),
                    requesting_node_id: requester_node_id,
                    operation: Box::new(operation.clone()),
                    authority_shards,
                    dispatch_authority_shard: dispatch,
                },
            ),
        )?;
        if !sent {
            self.stable_namespace_reconciliation_targets
                .remove(&request_id);
            return Ok(
                DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
                    reason: format!(
                        "stable namespace reconciliation authority {authority_node_id} is unroutable"
                    ),
                },
            );
        }
        self.wait_for_stable_namespace_reconciliation(
            &request_id,
            std::time::Duration::from_secs(5),
        )
    }

    pub(crate) fn handle_stable_namespace_reconciliation_request(
        &mut self,
        source_node_id: NodeId,
        request: DfsStableNamespaceReconciliationRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let reconciliation = if source_node_id != request.requesting_node_id {
            DfsStableNamespaceOperationReconciliation::Conflict {
                reason: "stable namespace reconciliation source does not match requester identity"
                    .to_string(),
            }
        } else {
            self.refresh_control_plane_latest()?;
            let file_system_id = sifs_file_system_id();
            let current_authority_shards =
                self.namespace_operation_authority_shards(&file_system_id, &request.operation)?;
            let current_dispatch = Self::namespace_operation_dispatch_shard(
                &file_system_id,
                &request.operation,
                &current_authority_shards,
            )?;
            if current_authority_shards != request.authority_shards
                || current_dispatch.as_ref() != Some(&request.dispatch_authority_shard)
                || request.dispatch_authority_shard.primary_node_id != self.node_id
            {
                DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired {
                    reason: "stable namespace reconciliation authority moved after dispatch"
                        .to_string(),
                }
            } else {
                self.reconcile_stable_namespace_operation_locally_for_requester(
                    &request.operation_id,
                    &request.operation,
                    &request.requesting_node_id,
                    &request.authority_shards,
                    true,
                )?
            }
        };
        self.send_dfs_message_if_routable(
            &source_node_id,
            DfsNetworkMessage::StableNamespaceReconciliationResponse(
                DfsStableNamespaceReconciliationResponse {
                    request_id: request.request_id.clone(),
                    reconciliation,
                },
            ),
        )?;
        Ok(NetworkedDfsServiceEvent::StableReconciliationServed {
            request_id: request.request_id,
            requester_node_id: source_node_id,
            kind: "namespace".to_string(),
        })
    }

    pub(crate) fn handle_stable_namespace_reconciliation_response(
        &mut self,
        authority_node_id: NodeId,
        response: DfsStableNamespaceReconciliationResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let Some(expected_authority) = self
            .stable_namespace_reconciliation_targets
            .get(&response.request_id)
        else {
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: authority_node_id,
                reason: "untracked stable namespace reconciliation response".to_string(),
            });
        };
        if expected_authority != &authority_node_id {
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: authority_node_id,
                reason: format!(
                    "stable namespace reconciliation response did not come from authority {expected_authority}"
                ),
            });
        }
        self.stable_namespace_reconciliation_results
            .insert(response.request_id.clone(), response.reconciliation);
        Ok(NetworkedDfsServiceEvent::StableReconciliationCompleted {
            request_id: response.request_id,
            authority_node_id,
            kind: "namespace".to_string(),
        })
    }

    pub(crate) fn recover_pending_namespace_requests_bounded(
        &mut self,
        max_requests: usize,
    ) -> Result<DfsNamespaceRequestRecoveryReport, DfsError> {
        let mut report = DfsNamespaceRequestRecoveryReport::default();
        if max_requests == 0 || self.control_plane.is_none() || self.metadata_backend.is_none() {
            return Ok(report);
        }
        let mut locally_authoritative = Vec::new();
        let mut locally_authoritative_count = 0usize;
        let mut shard_snapshots = BTreeMap::<DfsFileSystemId, Vec<DfsMetadataShardConfig>>::new();
        let mut cursor = self.pending_namespace_request_recovery_cursor.take();
        let scan_started_after_cursor = cursor.is_some();
        let scan_page_entry_limit =
            max_requests.clamp(1, DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT);
        let mut scanned_pages = 0usize;
        'scan: while scanned_pages < DFS_NAMESPACE_REQUEST_RECOVERY_SCAN_PAGE_LIMIT {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .list_pending_namespace_requests_page(cursor.as_ref(), scan_page_entry_limit)?;
            scanned_pages = scanned_pages.saturating_add(1);
            let page_len = page.requests.len();
            let page_next_cursor = page.next_cursor;
            for (page_index, request) in page.requests.into_iter().enumerate() {
                let request_cursor = DfsPendingNamespaceRequestPageCursor {
                    updated_at: request.updated_at,
                    requester_node_id: request.requester_node_id.clone(),
                    request_id: request.request_id.clone(),
                };
                if !shard_snapshots.contains_key(&request.file_system_id) {
                    let shards = self
                        .metadata_shard_config_snapshot(&request.file_system_id)
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    shard_snapshots.insert(request.file_system_id.clone(), shards);
                }
                let authority_is_local =
                    shard_snapshots
                        .get(&request.file_system_id)
                        .is_some_and(|snapshot| {
                            Self::affected_namespace_shards_from_snapshot(
                                &request.file_system_id,
                                &request.operation,
                                snapshot,
                            )
                            .is_ok_and(|shards| {
                                shards.iter().any(|shard| {
                                    shard.shard_id == request.authority_shard_id.0
                                        && shard.primary_node_id == self.node_id
                                })
                            })
                        });
                if authority_is_local {
                    locally_authoritative_count = locally_authoritative_count.saturating_add(1);
                    locally_authoritative.push(request);
                    if locally_authoritative.len() >= max_requests {
                        let has_unscanned_records =
                            page_index.saturating_add(1) < page_len || page_next_cursor.is_some();
                        cursor = has_unscanned_records.then_some(request_cursor);
                        break 'scan;
                    }
                }
            }
            cursor = page_next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let wrapped_at_end = scan_started_after_cursor && cursor.is_none();
        self.pending_namespace_request_recovery_cursor = cursor;
        report.remaining = locally_authoritative_count.saturating_add(usize::from(
            self.pending_namespace_request_recovery_cursor.is_some() || wrapped_at_end,
        ));

        if !locally_authoritative.is_empty() {
            let request_keys = locally_authoritative
                .iter()
                .map(|request| {
                    (
                        request.requester_node_id.clone(),
                        request.request_id.clone(),
                    )
                })
                .collect::<BTreeSet<_>>();
            let recovered =
                self.recover_durable_namespace_prepares_for_requests(&request_keys, max_requests)?;
            report.scanned = report.scanned.saturating_add(recovered);
            report.resolved = report.resolved.saturating_add(recovered);
            report.remaining = report.remaining.saturating_sub(recovered);
            if recovered > 0 {
                let mut unresolved = Vec::with_capacity(locally_authoritative.len());
                for request in locally_authoritative {
                    let backend = self
                        .metadata_backend
                        .as_ref()
                        .expect("metadata backend checked above");
                    if backend
                        .get_namespace_request_receipt(
                            &request.requester_node_id,
                            &request.request_id,
                        )?
                        .is_some()
                    {
                        continue;
                    }
                    let current = backend
                        .get_namespace_request(&request.requester_node_id, &request.request_id)?
                        .ok_or_else(|| DfsError::Metadata {
                            message: format!(
                                "recovered namespace request {}/{} disappeared from durable metadata",
                                request.requester_node_id, request.request_id
                            ),
                        })?;
                    if current.status != DfsNamespaceRequestStatus::Completed {
                        unresolved.push(current);
                    }
                }
                locally_authoritative = unresolved;
            }
        }

        for mut request in locally_authoritative
            .into_iter()
            .take(max_requests.saturating_sub(report.scanned))
        {
            report.scanned = report.scanned.saturating_add(1);

            let command = DfsNamespaceCommand {
                request_id: request.request_id.clone(),
                file_system_id: request.file_system_id.clone(),
                requesting_node_id: request.requester_node_id.clone(),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: request.operation.clone(),
            };
            let attempt = self.handle_namespace_request_internal(
                request.requester_node_id.clone(),
                command,
                true,
                false,
            );
            let current = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .get_namespace_request(&request.requester_node_id, &request.request_id)?;
            let compacted = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .get_namespace_request_receipt(&request.requester_node_id, &request.request_id)?
                .is_some();
            if compacted
                || current
                    .is_some_and(|current| current.status == DfsNamespaceRequestStatus::Completed)
            {
                report.resolved = report.resolved.saturating_add(1);
                report.remaining = report.remaining.saturating_sub(1);
                continue;
            }

            let detail = match attempt {
                Ok(NetworkedDfsServiceEvent::RemoteNamespaceServed {
                    result: DfsNamespaceResult::Rejected { reason, .. },
                    ..
                }) => reason,
                Ok(_) => "namespace request remained pending after recovery execution".to_string(),
                Err(error) => error.to_string(),
            };
            request.recovery_attempts = request.recovery_attempts.saturating_add(1);
            request.last_recovery_error = Some(detail.clone());
            request.updated_at = time::OffsetDateTime::now_utc();
            match self
                .metadata_backend
                .as_mut()
                .expect("metadata backend checked above")
                .put_namespace_request_on_shard(&request.authority_shard_id, &request)
            {
                Ok(_) => {}
                Err(error) => report.failures.push(format!(
                    "namespace request {}/{} recovery diagnostics failed: {error}",
                    request.requester_node_id, request.request_id
                )),
            }
            report.failures.push(format!(
                "namespace request {}/{} remains recoverable: {detail}",
                request.requester_node_id, request.request_id
            ));
        }
        Ok(report)
    }

    fn admit_durable_namespace_request(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsNamespaceCommand,
        shard_id: DfsMetadataShardId,
        recovering: bool,
    ) -> Result<DurableNamespaceRequestAdmission, DfsError> {
        let digest = namespace_command_digest(command)?;
        let admission = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "routed namespace mutation requires durable request admission".to_string(),
            })?
            .namespace_request_admission_snapshot(
                &shard_id,
                requester_node_id,
                &command.request_id,
            )?;
        let DfsNamespaceRequestAdmissionSnapshot {
            request: existing,
            receipt,
            prepared,
            history_expired,
        } = admission;
        if let Some(receipt) = receipt {
            if receipt.file_system_id != command.file_system_id || receipt.command_digest != digest
            {
                return Ok(DurableNamespaceRequestAdmission::Reject {
                    reason: format!(
                        "namespace request id {}/{} was already used for a different command",
                        requester_node_id, command.request_id
                    ),
                    posix_error: namespace_reconciliation_conflict(
                        command,
                        "durable operation id was already bound to a different command; reconcile the original outcome",
                    ),
                });
            }
            return Ok(DurableNamespaceRequestAdmission::Replay {
                result: receipt.result,
                posix_coordination_result: receipt.posix_coordination_result,
            });
        }
        if let Some(mut existing) = existing {
            if existing.file_system_id != command.file_system_id
                || existing.command_digest != digest
            {
                return Ok(DurableNamespaceRequestAdmission::Reject {
                    reason: format!(
                        "namespace request id {}/{} was already used for a different command",
                        requester_node_id, command.request_id
                    ),
                    posix_error: namespace_reconciliation_conflict(
                        command,
                        "durable operation id was already bound to a different command; reconcile the original outcome",
                    ),
                });
            }
            if existing.status == DfsNamespaceRequestStatus::Pending {
                let owns_prepare = prepared.is_some();
                if owns_prepare {
                    if !recovering {
                        let request_key = BTreeSet::from([(
                            requester_node_id.clone(),
                            command.request_id.clone(),
                        )]);
                        self.recover_durable_namespace_prepares_for_requests(&request_key, 1)?;
                        let recovered = self
                            .metadata_backend
                            .as_mut()
                            .expect("metadata backend checked above")
                            .namespace_request_admission_snapshot(
                                &shard_id,
                                requester_node_id,
                                &command.request_id,
                            )?;
                        if let Some(recovered_request) = recovered.request {
                            existing = recovered_request;
                        } else if let Some(receipt) = recovered.receipt {
                            return Ok(DurableNamespaceRequestAdmission::Replay {
                                result: receipt.result,
                                posix_coordination_result: receipt.posix_coordination_result,
                            });
                        } else {
                            return Err(DfsError::Metadata {
                                message: format!(
                                    "request-owned namespace prepare lost admission {}/{} during recovery",
                                    requester_node_id, command.request_id
                                ),
                            });
                        }
                    }
                    if existing.status == DfsNamespaceRequestStatus::Pending {
                        return Ok(DurableNamespaceRequestAdmission::Reject {
                            reason: format!(
                                "namespace request {}/{} has a durable hidden prepare awaiting recovery",
                                requester_node_id, command.request_id
                            ),
                            posix_error: None,
                        });
                    }
                }
            }
            return match existing.status {
                DfsNamespaceRequestStatus::Completed => {
                    let Some(result) = existing.result else {
                        return Err(DfsError::Metadata {
                            message: format!(
                                "completed namespace request {}/{} has no durable result",
                                requester_node_id, command.request_id
                            ),
                        });
                    };
                    Ok(DurableNamespaceRequestAdmission::Replay {
                        result,
                        posix_coordination_result: existing.posix_coordination_result,
                    })
                }
                DfsNamespaceRequestStatus::Pending => {
                    Ok(DurableNamespaceRequestAdmission::Execute {
                        shard_id: existing.authority_shard_id.clone(),
                        record: existing,
                        persisted: true,
                        replaying_pending: true,
                    })
                }
            };
        }
        if let Some(prepare) = prepared {
            if !namespace_prepare_matches_routed_command(
                &prepare,
                requester_node_id,
                command,
                &shard_id,
                &digest,
            ) {
                return Ok(DurableNamespaceRequestAdmission::Reject {
                    reason: format!(
                        "namespace request id {}/{} was already used for a different command",
                        requester_node_id, command.request_id
                    ),
                    posix_error: namespace_reconciliation_conflict(
                        command,
                        "durable operation id was already bound to a different command; reconcile the original outcome",
                    ),
                });
            }
            if !recovering {
                let request_key =
                    BTreeSet::from([(requester_node_id.clone(), command.request_id.clone())]);
                self.recover_durable_namespace_prepares_for_requests(&request_key, 1)?;
                return self.admit_durable_namespace_request(
                    requester_node_id,
                    command,
                    shard_id,
                    true,
                );
            }
            return Ok(DurableNamespaceRequestAdmission::Reject {
                reason: format!(
                    "namespace request {}/{} has a durable hidden prepare awaiting recovery",
                    requester_node_id, command.request_id
                ),
                posix_error: None,
            });
        }
        if history_expired {
            return Ok(DurableNamespaceRequestAdmission::Reject {
                reason: format!(
                    "namespace request {}/{} idempotency history expired or is unknown",
                    requester_node_id, command.request_id
                ),
                posix_error: namespace_reconciliation_conflict(
                    command,
                    "durable operation history expired; reconcile the authoritative namespace generation before retrying",
                ),
            });
        }

        let now = time::OffsetDateTime::now_utc();
        let record = DfsNamespaceRequestRecord {
            file_system_id: command.file_system_id.clone(),
            requester_node_id: requester_node_id.clone(),
            request_id: command.request_id.clone(),
            operation_id: namespace_request_operation_id(requester_node_id, &command.request_id),
            authority_shard_id: shard_id.clone(),
            operation: command.operation.clone(),
            command_digest: digest,
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: now,
            updated_at: now,
        };
        let persisted = !namespace_operation_can_fuse_durable_request_admission(&command.operation);
        if persisted {
            self.metadata_backend
                .as_mut()
                .expect("metadata backend checked above")
                .put_namespace_request_on_shard(&shard_id, &record)?;
        }
        Ok(DurableNamespaceRequestAdmission::Execute {
            shard_id,
            record,
            persisted,
            replaying_pending: false,
        })
    }

    fn complete_durable_namespace_request(
        &mut self,
        shard_id: &DfsMetadataShardId,
        mut record: DfsNamespaceRequestRecord,
        result: DfsNamespaceResult,
        posix_coordination_result: Option<DfsPosixCoordinationResult>,
    ) -> Result<(DfsNamespaceResult, Option<DfsPosixCoordinationResult>), DfsError> {
        let completion = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "routed namespace mutation lost its durable request backend".to_string(),
            })?
            .namespace_request_admission_snapshot(
                shard_id,
                &record.requester_node_id,
                &record.request_id,
            )?;
        if let Some(receipt) = completion.receipt {
            if receipt.command_digest != record.command_digest
                || receipt.file_system_id != record.file_system_id
            {
                return Err(DfsError::Metadata {
                    message: format!(
                        "namespace request receipt {}/{} conflicts with the active command",
                        record.requester_node_id, record.request_id
                    ),
                });
            }
            return Ok((receipt.result, receipt.posix_coordination_result));
        }
        if let Some(existing) = completion.request
            && existing.status == DfsNamespaceRequestStatus::Completed
        {
            let canonical_result = existing.result.ok_or_else(|| DfsError::Metadata {
                message: format!(
                    "completed namespace request {}/{} has no durable result",
                    record.requester_node_id, record.request_id
                ),
            })?;
            return Ok((canonical_result, existing.posix_coordination_result));
        }
        record.status = DfsNamespaceRequestStatus::Completed;
        record.result = Some(result.clone());
        record.posix_coordination_result = posix_coordination_result.clone();
        record.last_recovery_error = None;
        record.updated_at = time::OffsetDateTime::now_utc();
        self.metadata_backend
            .as_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "routed namespace mutation lost its durable request backend".to_string(),
            })?
            .put_namespace_request_on_shard(shard_id, &record)?;
        Ok((result, posix_coordination_result))
    }

    pub(crate) fn durable_namespace_request_completion_for_transaction(
        &self,
        transaction: &DfsNamespaceTransaction,
        shard_id: &DfsMetadataShardId,
    ) -> Option<DfsNamespaceRequestRecord> {
        let active = self.namespace_execution.request()?;
        if &active.authority_shard_id != shard_id
            || matches!(
                active.operation,
                DfsNamespaceOperation::CommitLocalEventualBatch { .. }
            )
            || !namespace_transaction_proves_operation_completed(&active.operation, transaction)
        {
            return None;
        }
        let mut completed = active.clone();
        completed.status = DfsNamespaceRequestStatus::Completed;
        completed.result = Some(DfsNamespaceResult::Applied);
        completed.posix_coordination_result = None;
        completed.last_recovery_error = None;
        completed.updated_at = time::OffsetDateTime::now_utc();
        Some(completed)
    }

    pub(crate) fn compact_durable_namespace_request_completion_for_transaction(
        &self,
        transaction: &DfsNamespaceTransaction,
        shard_id: &DfsMetadataShardId,
    ) -> Option<DfsNamespaceRequestCompletion> {
        let active = self.namespace_execution.request()?;
        if &active.authority_shard_id != shard_id
            || !matches!(
                active.operation,
                DfsNamespaceOperation::CommitLocalEventualBatch { .. }
            )
            || !namespace_transaction_proves_operation_completed(&active.operation, transaction)
        {
            return None;
        }
        Some(DfsNamespaceRequestCompletion {
            file_system_id: active.file_system_id.clone(),
            requester_node_id: active.requester_node_id.clone(),
            request_id: active.request_id.clone(),
            operation_id: active.operation_id.clone(),
            authority_shard_id: active.authority_shard_id.clone(),
            command_digest: active.command_digest.clone(),
            transaction_id: transaction.transaction_id.clone(),
            result: DfsNamespaceResult::Applied,
            posix_coordination_result: None,
            created_at: active.created_at,
            completed_at: time::OffsetDateTime::now_utc(),
        })
    }

    fn synced_namespace_object_for_operation(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        Ok(self
            .synced_namespace_objects_for_operation(file_system_id, operation)?
            .into_iter()
            .next())
    }

    fn synced_namespace_objects_for_sync_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        let Some(normalized_path) = posix_path::normalized_posix_path_key(path) else {
            return Ok(Vec::new());
        };
        let mut objects = Vec::new();
        let mut include_direct_children = false;
        if let Some(object) =
            self.synced_namespace_object_for_path(file_system_id, &normalized_path)?
        {
            include_direct_children =
                object.kind == DfsObjectKind::Directory && file_system_id == &sifs_file_system_id();
            objects.push(object);
        }
        if include_direct_children {
            let children = self
                .child_objects_for_sifs_dir(&normalized_path)
                .map_err(dfs_error_from_posix_namespace_sync)?;
            let mut children_by_path = BTreeMap::<String, DfsObjectMetadata>::new();
            for object in children {
                let object = self.object_metadata_with_receipt_locations(object)?;
                children_by_path.insert(metadata_path(&object.path), object);
            }
            for tombstone in
                self.direct_child_tombstones_for_sifs_dir(file_system_id, &normalized_path)?
            {
                let path = metadata_path(&tombstone.path);
                children_by_path.insert(path, tombstone);
            }
            objects.extend(children_by_path.into_values());
        }
        Ok(objects)
    }

    fn direct_child_tombstones_for_sifs_dir(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        let mut memory_by_path = BTreeMap::<String, DfsObjectMetadata>::new();
        for ((tombstone_file_system_id, metadata_path), object) in &self.tombstones_by_metadata_path
        {
            if tombstone_file_system_id == file_system_id
                && metadata_path_is_direct_child_of_posix_dir(directory_path, metadata_path)
            {
                memory_by_path.insert(metadata_path.clone(), object.clone());
            }
        }
        for object in self.metadata.objects.values() {
            if object.file_system_id != *file_system_id {
                continue;
            }
            let object_metadata_path = metadata_path(&object.path);
            if !metadata_path_is_direct_child_of_posix_dir(directory_path, &object_metadata_path) {
                continue;
            }
            let replace = memory_by_path
                .get(&object_metadata_path)
                .is_none_or(|existing| {
                    existing.version < object.version
                        || (existing.version == object.version
                            && existing.kind == DfsObjectKind::Tombstone
                            && object.kind != DfsObjectKind::Tombstone)
                });
            if replace {
                memory_by_path.insert(object_metadata_path, object.clone());
            }
        }
        let mut candidate_paths = memory_by_path.keys().cloned().collect::<BTreeSet<_>>();
        if let Some(backend) = self.metadata_backend.as_ref() {
            for object in backend.child_tombstones_for_directory(file_system_id, directory_path)? {
                let object_metadata_path = metadata_path(&object.path);
                if !metadata_path_is_direct_child_of_posix_dir(
                    directory_path,
                    &object_metadata_path,
                ) {
                    continue;
                }
                candidate_paths.insert(object_metadata_path);
            }
        }
        let mut tombstones = Vec::new();
        for path in candidate_paths {
            let memory_object = memory_by_path.get(&path);
            let backend_object = self
                .metadata_backend
                .as_ref()
                .map(|backend| backend.get_object_by_path(file_system_id, &path))
                .transpose()?
                .flatten();
            let authoritative = match (memory_object, backend_object.as_ref()) {
                (Some(memory_object), Some(backend_object))
                    if self.local_write_view_overrides_backend(
                        &memory_object.object_id,
                        memory_object,
                        Some(backend_object),
                    ) =>
                {
                    Some(memory_object.clone())
                }
                (_, Some(backend_object)) => Some(backend_object.clone()),
                (Some(memory_object), None) => Some(memory_object.clone()),
                (None, None) => None,
            };
            if let Some(object) = authoritative
                && object.kind == DfsObjectKind::Tombstone
            {
                tombstones.push(self.object_metadata_with_receipt_locations(object)?);
            }
        }
        Ok(tombstones)
    }

    fn synced_mkdir_all_paths(path: &str) -> Vec<String> {
        let Some(normalized) = posix_path::normalized_posix_path_key(path) else {
            return Vec::new();
        };
        if normalized == "/" {
            return vec![normalized];
        }
        let mut paths = Vec::new();
        let mut current = String::new();
        for part in normalized.trim_matches('/').split('/') {
            current.push('/');
            current.push_str(part);
            paths.push(current.clone());
        }
        paths
    }

    fn synced_namespace_objects_for_operation(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        if !namespace_operation_returns_synced_response_objects(operation) {
            return Ok(Vec::new());
        }
        let point_lookup_path = match operation {
            DfsNamespaceOperation::LookupPath { path, .. }
            | DfsNamespaceOperation::ReconcilePlacement { path, .. } => Some(path.as_str()),
            DfsNamespaceOperation::LookupPaths { paths } if paths.len() == 1 => {
                Some(paths[0].as_str())
            }
            _ => None,
        };
        let pinned_backend = self.metadata_backend.is_some();
        if let Some(backend) = self.metadata_backend.as_mut() {
            if let Some(path) = point_lookup_path {
                backend.begin_consistent_read_snapshot_for_path(file_system_id, path)?;
            } else {
                backend.begin_consistent_read_snapshot()?;
            }
        }
        let result =
            self.synced_namespace_objects_for_operation_from_pinned_view(file_system_id, operation);
        if pinned_backend {
            let backend = self
                .metadata_backend
                .as_mut()
                .expect("pinned metadata backend remains installed");
            if let Some(path) = point_lookup_path {
                backend.end_consistent_read_snapshot_for_path(file_system_id, path);
            } else {
                backend.end_consistent_read_snapshot();
            }
        }
        result
    }

    fn synced_namespace_objects_for_operation_from_pinned_view(
        &self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        let paths: Vec<String> = match operation {
            DfsNamespaceOperation::SyncPath { path } => {
                return self.synced_namespace_objects_for_sync_path(file_system_id, path);
            }
            DfsNamespaceOperation::LookupPath { path, .. }
            | DfsNamespaceOperation::ReconcilePlacement { path, .. }
            | DfsNamespaceOperation::Mkdir { path }
            | DfsNamespaceOperation::MkdirWithAttrs { path, .. }
            | DfsNamespaceOperation::MkdirAtNoLinks { path, .. } => {
                posix_path::normalized_posix_path_key(path)
                    .into_iter()
                    .collect()
            }
            DfsNamespaceOperation::MkdirAll { path } => Self::synced_mkdir_all_paths(path),
            DfsNamespaceOperation::LookupPaths { paths } => paths
                .iter()
                .filter_map(|path| posix_path::normalized_posix_path_key(path))
                .collect(),
            _ => return Ok(Vec::new()),
        };
        let include_hardlink_routing_hints =
            matches!(operation, DfsNamespaceOperation::LookupPaths { .. });
        let mut objects = Vec::new();
        let mut returned_object_ids = BTreeSet::new();
        let mut hardlink_target_ids = BTreeSet::new();
        for path in &paths {
            if let Some(object) = self.synced_namespace_object_for_path(file_system_id, path)? {
                if include_hardlink_routing_hints
                    && object.kind == DfsObjectKind::Hardlink
                    && let Some(target_object_id) = object.link_target_object_id.clone()
                {
                    hardlink_target_ids.insert(target_object_id);
                }
                returned_object_ids.insert(object.object_id.clone());
                objects.push(object);
            }
        }
        // A hardlink record contains only its target object id. Include the
        // immediate target, when this authority's pinned view can name it, as
        // a routing hint for metadata-less LookupPaths callers. The hint is
        // deliberately not a completeness claim: callers must route its path
        // to that path's current shard authority and validate the object id
        // before treating the target record as authoritative.
        for target_object_id in hardlink_target_ids {
            if returned_object_ids.contains(&target_object_id) {
                continue;
            }
            let Ok(Some(target)) = self.object_by_id(&target_object_id) else {
                continue;
            };
            returned_object_ids.insert(target.object_id.clone());
            objects.push(target);
        }
        Ok(objects)
    }

    fn synced_namespace_object_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let normalized_path = match posix_path::normalized_posix_path_key(path) {
            Some(path) => path,
            None => return Ok(None),
        };
        self.authoritative_metadata_object_for_path(file_system_id, &normalized_path)?
            .map(|object| self.object_metadata_with_receipt_locations(object))
            .transpose()
    }

    /// Serves a namespace operation routed from a replica. The local node must
    /// be the metadata-shard primary for every shard the operation touches.
    /// Lock and explicit sync operations run their typed authority/quorum
    /// protocol; ordinary mutations retain their existing response-journal
    /// catch-up path so the requester reads its own write.
    pub(crate) fn handle_namespace_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsNamespaceCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_namespace_request_internal(requester_node_id, command, false, true)
    }

    fn handle_namespace_request_internal(
        &mut self,
        requester_node_id: NodeId,
        mut command: DfsNamespaceCommand,
        recovering: bool,
        send_response: bool,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let _namespace_plan = self.namespace_execution.begin_independent_planning();
        let request_id = command.request_id.clone();
        let trace_started = Instant::now();
        let trace_operation = dfs_namespace_operation_trace_label(&command.operation);
        let trace_involved_paths = command.operation.involved_paths().len();
        let mut trace_affected_ms = 0;
        let mut trace_affected_shards = 0;
        let mut trace_admission_ms = 0;
        let mut trace_execute_ms = 0;
        let mut trace_completion_ms = 0;
        let mut affected_shard_ids: Vec<DfsMetadataShardId> = Vec::new();
        let read_only_lookup = matches!(
            &command.operation,
            DfsNamespaceOperation::FileHistoryQuery { .. }
                | DfsNamespaceOperation::PollNamespaceChanges { .. }
                | DfsNamespaceOperation::LookupPath { .. }
                | DfsNamespaceOperation::LookupPaths { .. }
        );
        if !read_only_lookup {
            // Mutation lanes retain the existing availability behavior: an
            // exact completed handoff can rebind the durable writer before
            // authority planning. Lookups are observation-only. In
            // particular, a stale same-shard-id stamp must reach the complete
            // affected-shard comparison below without first clearing a
            // durable handoff fence or running rebind hydration/recovery.
            for expected in &command.authority_shards {
                self.rebind_completed_local_metadata_shard_membership_handoff_if_frozen(expected)?;
            }
        }
        // The authority the requester should retry against when this node is
        // not the shard primary (the requester routed on a stale shard view).
        let mut authority_redirect: Option<NodeId> = None;
        let mut durable_replay = false;
        let mut posix_coordination_result = None;
        let result = if let Err(error) = validate_namespace_operation_id(&command.request_id) {
            DfsNamespaceResult::Rejected {
                reason: error.to_string(),
                posix_error: None,
                current_primary_node_id: None,
            }
        } else if requester_node_id != command.requesting_node_id {
            DfsNamespaceResult::Rejected {
                reason: format!(
                    "namespace request requester mismatch: frame source {}, payload requester {}",
                    requester_node_id, command.requesting_node_id
                ),
                posix_error: None,
                current_primary_node_id: None,
            }
        } else if let DfsNamespaceOperation::AdvisoryLock { command, .. } = &command.operation
            && !crate::posix::locks::advisory_lock_command_belongs_to_node(
                command,
                &requester_node_id,
            )
        {
            DfsNamespaceResult::Rejected {
                reason: format!(
                    "advisory-lock command is not owned by authenticated requester {requester_node_id}"
                ),
                posix_error: None,
                current_primary_node_id: None,
            }
        } else if let Some(reason) = self.adoption_source_authority_rejection(
            &command.file_system_id,
            &command.operation,
            &requester_node_id,
        )? {
            DfsNamespaceResult::Rejected {
                reason,
                posix_error: None,
                current_primary_node_id: None,
            }
        } else {
            let affected_started = Instant::now();
            let affected = self
                .metadata_shard_config_snapshot(&command.file_system_id)
                .and_then(|snapshot| {
                    let snapshot = snapshot.unwrap_or_default();
                    let affected = Self::namespace_operation_authority_shards_from_snapshot(
                        &command.file_system_id,
                        &command.operation,
                        &snapshot,
                    )?;
                    Ok((snapshot, affected))
                });
            match affected {
                Err(error) => {
                    trace_affected_ms = affected_started.elapsed().as_millis();
                    DfsNamespaceResult::Rejected {
                        reason: error.to_string(),
                        posix_error: None,
                        current_primary_node_id: None,
                    }
                }
                Ok((mut shard_snapshot, mut shards)) => {
                    trace_affected_ms = affected_started.elapsed().as_millis();
                    trace_affected_shards = shards.len();
                    if !recovering
                        && !command.authority_shards.is_empty()
                        && !namespace_authority_shards_match(&command.authority_shards, &shards)
                        && self.refresh_control_plane_latest().is_ok()
                        && let Ok(Some(refreshed_snapshot)) =
                            self.metadata_shard_config_snapshot(&command.file_system_id)
                        && let Ok(refreshed) =
                            Self::namespace_operation_authority_shards_from_snapshot(
                                &command.file_system_id,
                                &command.operation,
                                &refreshed_snapshot,
                            )
                    {
                        // A long-lived lane can trail the sender's committed
                        // view. Refresh once before rejecting; matching stamps
                        // stay on the no-refresh hot path.
                        shard_snapshot = refreshed_snapshot;
                        shards = refreshed;
                        trace_affected_ms = affected_started.elapsed().as_millis();
                        trace_affected_shards = shards.len();
                    }
                    if !recovering
                        && !command.authority_shards.is_empty()
                        && !namespace_authority_shards_match(&command.authority_shards, &shards)
                    {
                        let current_primary_node_id = Self::namespace_operation_dispatch_shard(
                            &command.file_system_id,
                            &command.operation,
                            &shards,
                        )
                        .ok()
                        .flatten()
                        .map(|shard| shard.primary_node_id);
                        DfsNamespaceResult::Rejected {
                            reason: format!(
                                "namespace authority shard stamp does not match the receiver's current affected shard configurations: stamped {:?}, current {:?}",
                                command.authority_shards, shards
                            ),
                            posix_error: None,
                            current_primary_node_id,
                        }
                    } else {
                        // Response batches may only carry shards this node is
                        // primary for; foreign halves of coordinated operations
                        // are synced from their own primaries.
                        affected_shard_ids = shards
                            .iter()
                            .filter(|shard| shard.primary_node_id == self.node_id)
                            .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
                            .collect();
                        let foreign_shard = shards
                            .iter()
                            .find(|shard| shard.primary_node_id != self.node_id)
                            .cloned();
                        // Finish generic pending receipts left by older
                        // placement callers, without creating new ones on the
                        // normal desired-state reconciliation path.
                        let durable_admission =
                            if (namespace_operation_uses_durable_request_admission(
                                &command.operation,
                            ) || (recovering
                                && matches!(
                                    &command.operation,
                                    DfsNamespaceOperation::ReconcilePlacement { .. }
                                )))
                                && !affected_shard_ids.is_empty()
                            {
                                let shard_id = affected_shard_ids
                                    .iter()
                                    .min()
                                    .expect("non-empty affected shard ids")
                                    .clone();
                                let admission_started = Instant::now();
                                let admission = self.admit_durable_namespace_request(
                                    &requester_node_id,
                                    &command,
                                    shard_id,
                                    recovering,
                                )?;
                                trace_admission_ms = admission_started.elapsed().as_millis();
                                Some(admission)
                            } else {
                                None
                            };
                        match durable_admission {
                            Some(DurableNamespaceRequestAdmission::Replay {
                                result,
                                posix_coordination_result: replayed_coordination_result,
                            }) => {
                                durable_replay = true;
                                posix_coordination_result = replayed_coordination_result;
                                result
                            }
                            Some(DurableNamespaceRequestAdmission::Reject {
                                reason,
                                posix_error,
                            }) => {
                                durable_replay = true;
                                DfsNamespaceResult::Rejected {
                                    reason,
                                    posix_error,
                                    current_primary_node_id: None,
                                }
                            }
                            durable_execute => {
                                let replaying_pending_request = durable_execute.as_ref().and_then(
                                    |admission| match admission {
                                        DurableNamespaceRequestAdmission::Execute {
                                            record,
                                            replaying_pending: true,
                                            ..
                                        } => Some(record.clone()),
                                        DurableNamespaceRequestAdmission::Execute { .. }
                                        | DurableNamespaceRequestAdmission::Replay { .. }
                                        | DurableNamespaceRequestAdmission::Reject { .. } => None,
                                    },
                                );
                                self.namespace_execution.begin_request(
                                    durable_execute.as_ref().and_then(
                                        |admission| match admission {
                                            DurableNamespaceRequestAdmission::Execute {
                                                record,
                                                ..
                                            } => Some(record.clone()),
                                            DurableNamespaceRequestAdmission::Replay { .. }
                                            | DurableNamespaceRequestAdmission::Reject { .. } => {
                                                None
                                            }
                                        },
                                    ),
                                );
                                let execute_started = Instant::now();
                                let previous_operation_context = if foreign_shard.is_none() {
                                    self.replace_namespace_operation_context(
                                        command.file_system_id.clone(),
                                        shard_snapshot.clone(),
                                    )
                                } else {
                                    // Cross-shard coordinators explicitly
                                    // refresh authority and may pump nested
                                    // requests while settling. They must never
                                    // inherit an outer local planning snapshot.
                                    self.namespace_execution.replace_context(None)
                                };
                                let outcome = match (&foreign_shard, &mut command.operation) {
                                    (
                                        None,
                                        DfsNamespaceOperation::PollNamespaceChanges { request },
                                    ) => match (shards.first(), self.metadata_backend.as_ref()) {
                                        (Some(shard), Some(backend)) => {
                                            crate::poll_dfs_namespace_changes(
                                                backend.as_ref(),
                                                &request.cursor,
                                                shard,
                                                &request.root_path,
                                            )
                                            .map(|batch| {
                                                Some(DfsPosixCoordinationResult::NamespaceChanges(
                                                    DfsNamespaceChangePollResponse { batch },
                                                ))
                                            })
                                            .map_err(posix_model::DfsPosixError::from)
                                        }
                                        (None, _) => Err(posix_model::DfsPosixError::Dfs(
                                            DfsError::Distributed {
                                                message: format!(
                                                    "namespace change poll for shard {} has no current authority configuration",
                                                    request.cursor.shard_id.0
                                                ),
                                            },
                                        )),
                                        (_, None) => Err(posix_model::DfsPosixError::Dfs(
                                            DfsError::Metadata {
                                                message: format!(
                                                    "metadata primary {} has no resident backend for namespace change shard {}",
                                                    self.node_id, request.cursor.shard_id.0
                                                ),
                                            },
                                        )),
                                    },
                                    (
                                        None,
                                        DfsNamespaceOperation::CommitLocalEventualBatch {
                                            transaction,
                                            writes,
                                        },
                                    ) => {
                                        if self.control_plane.is_some() && shards.len() != 1 {
                                            let shard_ids = shards
                                                .iter()
                                                .map(|shard| shard.shard_id.as_str())
                                                .collect::<Vec<_>>()
                                                .join(", ");
                                            Err(posix_model::DfsPosixError::Dfs(
                                                DfsError::Distributed {
                                                    message: format!(
                                                        "clustered local-eventual batch {} must resolve to exactly one metadata shard, found [{}]",
                                                        transaction.transaction_id, shard_ids
                                                    ),
                                                },
                                            ))
                                        } else {
                                            let placeholder = DfsNamespaceTransaction {
                                                file_version_pins: None,
                                                transaction_id: transaction.transaction_id.clone(),
                                                file_system_id: transaction.file_system_id.clone(),
                                                objects: Vec::new(),
                                                versions: Vec::new(),
                                                advisory_lock_states: Vec::new(),
                                                created_at: transaction.created_at,
                                            };
                                            let transaction =
                                                std::mem::replace(transaction, placeholder);
                                            let writes = std::mem::take(writes);
                                            self.apply_local_eventual_batch_commit(
                                                &requester_node_id,
                                                transaction,
                                                writes,
                                                shards.first().cloned(),
                                            )
                                            .map(
                                                |mutated| {
                                                    durable_replay = !mutated;
                                                    None
                                                },
                                            )
                                        }
                                    }
                                    (None, operation) => {
                                        self.execute_posix_coordination_operation_local(operation)
                                    }
                                    (Some(_), DfsNamespaceOperation::Rename { from, to }) => self
                                        .namespace_coordinator_dispatch_rename_with_pending_replay(
                                            &command.file_system_id,
                                            from,
                                            to,
                                            replaying_pending_request.as_ref(),
                                        )
                                        .map(|()| None),
                                    (
                                        Some(_),
                                        DfsNamespaceOperation::Hardlink {
                                            existing_path,
                                            link_path,
                                        },
                                    ) => self
                                        .namespace_coordinator_dispatch_hardlink(
                                            &command.file_system_id,
                                            existing_path,
                                            link_path,
                                        )
                                        .map(|()| None),
                                    (Some(shard), _) => {
                                        authority_redirect = Some(shard.primary_node_id.clone());
                                        Err(posix_model::DfsPosixError::Dfs(
                                            DfsError::Distributed {
                                                message: format!(
                                                    "node {} is not the metadata primary for shard {} (primary {}, epoch {})",
                                                    self.node_id,
                                                    shard.shard_id,
                                                    shard.primary_node_id,
                                                    shard.epoch
                                                ),
                                            },
                                        ))
                                    }
                                };
                                self.restore_namespace_operation_context(
                                    previous_operation_context,
                                );
                                let durability = self.namespace_execution.finish_request();
                                let durable_prepare_recorded = durability.prepare_recorded;
                                let durable_job_recorded = durability.job_recorded;
                                trace_execute_ms = execute_started.elapsed().as_millis();
                                let retryable_error = outcome.as_ref().err().is_some_and(|error| {
                                    namespace_execution_must_remain_pending(
                                        error,
                                        durable_prepare_recorded,
                                        durable_job_recorded,
                                    )
                                });
                                if retryable_error
                                    && !durable_prepare_recorded
                                    && let Some(DurableNamespaceRequestAdmission::Execute {
                                        shard_id,
                                        record,
                                        persisted: false,
                                        ..
                                    }) = durable_execute.as_ref()
                                {
                                    self.metadata_backend
                                        .as_mut()
                                        .expect("durable namespace request backend")
                                        .put_namespace_request_on_shard(shard_id, record)?;
                                }
                                let mut operation_result = match outcome {
                                    Ok(coordination_result) => {
                                        posix_coordination_result = coordination_result;
                                        DfsNamespaceResult::Applied
                                    }
                                    Err(error) => DfsNamespaceResult::Rejected {
                                        current_primary_node_id: authority_redirect.or_else(|| {
                                            namespace_rejection_current_primary(&error)
                                        }),
                                        posix_error: posix_model::namespace_posix_error_for(&error),
                                        reason: error.to_string(),
                                    },
                                };
                                if let Some(DurableNamespaceRequestAdmission::Execute {
                                    shard_id,
                                    record,
                                    ..
                                }) = durable_execute
                                    && !retryable_error
                                {
                                    let completion_started = Instant::now();
                                    // A successful request-owned prepare
                                    // commits its terminal record or compact
                                    // receipt atomically with the namespace
                                    // transaction. Reopening every retained
                                    // shard to rediscover that same evidence
                                    // is redundant and can scan large journal
                                    // tails. Errors after a durable prepare
                                    // remain retryable above and never enter
                                    // this fast path.
                                    let completion_was_fused =
                                        namespace_result_was_fused_into_committed_prepare(
                                            durable_prepare_recorded,
                                            &operation_result,
                                        );
                                    if !completion_was_fused {
                                        let (canonical_result, canonical_coordination_result) =
                                            self.complete_durable_namespace_request(
                                                &shard_id,
                                                record,
                                                operation_result.clone(),
                                                posix_coordination_result.clone(),
                                            )?;
                                        operation_result = canonical_result;
                                        posix_coordination_result = canonical_coordination_result;
                                    }
                                    trace_completion_ms = completion_started.elapsed().as_millis();
                                }
                                operation_result
                            }
                        }
                    }
                }
            }
        };
        // A crash may follow durable pin publication but precede the local
        // repair queue handoff. Replaying its receipt re-emits only work whose
        // current root and authority remain valid; it never recreates a pin.
        if durable_replay
            && matches!(result, DfsNamespaceResult::Applied)
            && let DfsNamespaceOperation::FileHistoryPin { request } = &command.operation
            && self.reconcile_file_history_repair_root(
                &request.identity.object_id,
                &request.identity.version,
            )?
        {
            self.record_message_committed_work(DfsMessageCommittedWork {
                repair_maintenance: true,
                ..Default::default()
            });
        }
        let mutates_metadata = !durable_replay
            && !matches!(
                &command.operation,
                // Managed placement records each actual metadata write via
                // committed work; polling its progress is not an authored edge.
                DfsNamespaceOperation::ReconcilePlacement { .. }
                    | DfsNamespaceOperation::FileHistoryQuery { .. }
                    | DfsNamespaceOperation::PollNamespaceChanges { .. }
                    | DfsNamespaceOperation::LookupPath { .. }
                    | DfsNamespaceOperation::LookupPaths { .. }
                    | DfsNamespaceOperation::SyncPath { .. }
                    | DfsNamespaceOperation::AdvisoryLock { .. }
                    | DfsNamespaceOperation::FileSync { .. }
                    | DfsNamespaceOperation::DirectorySync { .. }
            );
        if mutates_metadata && matches!(result, DfsNamespaceResult::Applied) {
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                repair_maintenance: false,
                namespace_maintenance: false,
                metadata_replica_progress: false,
                metadata_materialization: false,
            });
        }
        let journal_started = Instant::now();
        let mut metadata_journal_has_more = false;
        let metadata_journal_batches = if command.include_metadata_journal_batches
            && matches!(result, DfsNamespaceResult::Applied)
            && namespace_operation_should_replay_journal_batches(&command.operation)
        {
            let mut batches = Vec::new();
            for shard_id in &affected_shard_ids {
                let after_sequence = command
                    .metadata_journal_after_sequences
                    .iter()
                    .find(|cursor| &cursor.shard_id == shard_id)
                    .map(|cursor| cursor.after_sequence)
                    .unwrap_or(0);
                let mut shard_batches = self.metadata_journal_batches_for_shard_since_limited(
                    shard_id,
                    &command.requesting_node_id,
                    crate::service_trace::dfs_namespace_operation_trace_label(&command.operation),
                    after_sequence,
                    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT.saturating_add(1),
                    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT,
                    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BYTE_LIMIT,
                )?;
                if shard_batches.len() > DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT {
                    metadata_journal_has_more = true;
                    shard_batches.truncate(DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT);
                }
                batches.extend(shard_batches);
            }
            batches
        } else {
            Vec::new()
        };
        let trace_journal_ms = journal_started.elapsed().as_millis();
        let trace_journal_batches = metadata_journal_batches.len();
        let sync_started = Instant::now();
        let synced_path_objects = if matches!(result, DfsNamespaceResult::Applied)
            && namespace_request_returns_synced_response_objects(
                command.include_metadata_journal_batches,
                &command.operation,
            ) {
            self.synced_namespace_objects_for_operation(
                &command.file_system_id,
                &command.operation,
            )?
        } else {
            Vec::new()
        };
        let synced_path_object = match &command.operation {
            DfsNamespaceOperation::SyncPath { .. } | DfsNamespaceOperation::LookupPath { .. } => {
                synced_path_objects.first().cloned()
            }
            _ => None,
        };
        if posix_coordination_result.is_none()
            && let DfsNamespaceOperation::LookupPath {
                inline_range: Some(range),
                ..
            } = &command.operation
            && range.max_bytes <= DFS_NAMESPACE_LOOKUP_INLINE_RANGE_MAX_BYTES
            && let Some(object) = synced_path_object.as_ref()
            && let NetworkedDfsRangeReadOutcome::Found {
                data,
                freshness: NetworkedDfsReadFreshness::Current,
            } = self.read_object_metadata_local_range_with_state(
                object,
                range.offset,
                range.max_bytes,
                false,
            )?
            && object.content_size_bytes == Some(data.total_size_bytes)
        {
            posix_coordination_result = Some(DfsPosixCoordinationResult::LookupPathRange(data));
        }
        let trace_sync_ms = sync_started.elapsed().as_millis();
        let trace_synced_path_objects = synced_path_objects.len();
        let send_started = Instant::now();
        let response = DfsNamespaceResponse {
            request_id: request_id.clone(),
            result: result.clone(),
            synced_path_object,
            synced_path_objects,
            metadata_journal_batches,
            metadata_journal_has_more: Some(metadata_journal_has_more),
            posix_coordination_result,
        };
        if !recovering && send_response {
            self.send_dfs_message_if_routable(
                &requester_node_id,
                DfsNetworkMessage::NamespaceResponse(response),
            )?;
        }
        let trace_send_ms = send_started.elapsed().as_millis();
        dfs_perf_trace(format_args!(
            "namespace-request node={} requester={} request={} operation={} involved_paths={} affected_shards={} result={} affected_ms={} admission_ms={} execute_ms={} completion_ms={} journal_batches={} journal_ms={} synced_path_objects={} sync_ms={} response_send_ms={} total_ms={}",
            self.node_id,
            requester_node_id,
            request_id,
            trace_operation,
            trace_involved_paths,
            trace_affected_shards,
            match &result {
                DfsNamespaceResult::Applied => "applied",
                DfsNamespaceResult::Rejected { .. } => "rejected",
            },
            trace_affected_ms,
            trace_admission_ms,
            trace_execute_ms,
            trace_completion_ms,
            trace_journal_batches,
            trace_journal_ms,
            trace_synced_path_objects,
            trace_sync_ms,
            trace_send_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(NetworkedDfsServiceEvent::RemoteNamespaceServed {
            request_id,
            requester_node_id,
            result,
            mutates_metadata,
        })
    }

    /// Execute a local-primary mutation through the same durable request
    /// admission table as a remotely routed request, without emitting a
    /// synthetic response to this node's own transport.
    pub(crate) fn execute_local_primary_namespace_operation_with_request_id(
        &mut self,
        operation_id: String,
        operation: DfsNamespaceOperation,
    ) -> posix_model::Result<()> {
        self.execute_local_primary_namespace_operation_for_file_system(
            sifs_file_system_id(),
            operation_id,
            operation,
        )
    }

    pub(crate) fn execute_local_primary_namespace_operation_for_file_system(
        &mut self,
        file_system_id: DfsFileSystemId,
        operation_id: String,
        operation: DfsNamespaceOperation,
    ) -> posix_model::Result<()> {
        validate_namespace_operation_id(&operation_id).map_err(DfsPosixError::from)?;
        if self.control_plane.is_none() {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: "durable local-primary namespace admission requires a committed control-plane shard view"
                    .to_string(),
            }));
        }
        let authority_shards = self.affected_namespace_shards(&file_system_id, &operation)?;
        let requester = self.node_id.clone();
        let event = self.handle_namespace_request_internal(
            requester.clone(),
            DfsNamespaceCommand {
                request_id: operation_id,
                file_system_id,
                requesting_node_id: requester.clone(),
                include_metadata_journal_batches: false,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards,
                operation,
            },
            false,
            false,
        )?;
        match event {
            NetworkedDfsServiceEvent::RemoteNamespaceServed {
                result: DfsNamespaceResult::Applied,
                ..
            } => Ok(()),
            NetworkedDfsServiceEvent::RemoteNamespaceServed {
                result:
                    DfsNamespaceResult::Rejected {
                        reason,
                        posix_error,
                        ..
                    },
                ..
            } => Err(posix_model::posix_error_for_namespace_rejection(
                reason,
                posix_error,
            )),
            other => Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "local-primary durable namespace execution returned unexpected event {other:?}"
                ),
            })),
        }
    }

    pub(crate) fn handle_namespace_response(
        &mut self,
        primary_node_id: NodeId,
        response: DfsNamespaceResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        // Late, duplicate, or mismatched responses are discarded; a stray
        // frame must never fail the whole event loop.
        let (
            replay_journal_batches,
            apply_synced_path_object,
            dispatch_authority_shard,
            authority_shards,
        ) = {
            let Some(pending) = self.pending_routed_namespace_ops.get(&response.request_id) else {
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason: "namespace response for an untracked request".to_string(),
                });
            };
            if pending.primary_node_id != primary_node_id {
                let reason = format!(
                    "namespace response did not come from primary {}",
                    pending.primary_node_id
                );
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason,
                });
            }
            (
                pending.replay_journal_batches,
                pending.apply_synced_path_object,
                pending.dispatch_authority_shard.clone(),
                pending.authority_shards.clone(),
            )
        };
        let response_applied = matches!(response.result, DfsNamespaceResult::Applied);
        if let DfsNamespaceResult::Rejected {
            current_primary_node_id: Some(redirected_primary),
            reason: rejection_reason,
            ..
        } = &response.result
            && let Err(error) = self.validate_routed_namespace_authority_redirect(
                &primary_node_id,
                dispatch_authority_shard.as_ref(),
                &authority_shards,
                redirected_primary,
            )
        {
            let reason = format!(
                "routed namespace authority redirect is uncorroborated: {error}; receiver rejection: {rejection_reason}"
            );
            if let Some(pending) = self
                .pending_routed_namespace_ops
                .get_mut(&response.request_id)
            {
                pending.result = Some(PendingRoutedNamespaceResult {
                    result: DfsNamespaceResult::Rejected {
                        reason: reason.clone(),
                        posix_error: None,
                        current_primary_node_id: None,
                    },
                    posix_coordination_result: None,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_has_more: false,
                });
            }
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: primary_node_id,
                reason,
            });
        }
        if response_applied
            && let Err(error) = self.validate_routed_namespace_response_authority(
                &primary_node_id,
                dispatch_authority_shard.as_ref(),
                &authority_shards,
            )
        {
            let reason = format!("routed namespace response authority is stale: {error}");
            if let Some(pending) = self
                .pending_routed_namespace_ops
                .get_mut(&response.request_id)
            {
                pending.result = Some(PendingRoutedNamespaceResult {
                    result: DfsNamespaceResult::Rejected {
                        reason: reason.clone(),
                        posix_error: None,
                        current_primary_node_id: None,
                    },
                    posix_coordination_result: None,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_has_more: false,
                });
            }
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: primary_node_id,
                reason,
            });
        }
        // A rejection carries no authoritative metadata. Ignore any attached
        // journal or path payload before the caller evaluates a corroborated
        // authority redirect.
        if response_applied && replay_journal_batches {
            for batch in &response.metadata_journal_batches {
                let batch_file_system_id = metadata_journal_batch_file_system_id(batch);
                let expected_authority = authority_shards.iter().find(|shard| {
                    batch_file_system_id.as_ref() == Some(&shard.file_system_id)
                        && batch.shard_id.0 == shard.shard_id
                });
                let apply_result = match expected_authority {
                    Some(expected_authority) => self.apply_routed_write_metadata_journal_batch(
                        &primary_node_id,
                        expected_authority,
                        batch,
                    ),
                    None => Err(DfsError::Distributed {
                        message: format!(
                            "routed namespace response journal for shard {} does not match any dispatch authority assignment",
                            batch.shard_id.0
                        ),
                    }),
                };
                if let Err(error) = apply_result {
                    let reason = format!("routed namespace journal replay failed: {error}");
                    if let Some(pending) = self
                        .pending_routed_namespace_ops
                        .get_mut(&response.request_id)
                    {
                        pending.result = Some(PendingRoutedNamespaceResult {
                            result: DfsNamespaceResult::Rejected {
                                reason: reason.clone(),
                                posix_error: None,
                                current_primary_node_id: None,
                            },
                            posix_coordination_result: None,
                            synced_path_object: None,
                            synced_path_objects: Vec::new(),
                            metadata_journal_has_more: false,
                        });
                    }
                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason,
                    });
                }
            }
        }
        if response_applied
            && apply_synced_path_object
            && let Some(object) = response.synced_path_object.as_ref()
            && let Err(error) = self.apply_metadata_object_to_service_state(object.clone())
        {
            let reason = format!("routed namespace path sync failed: {error}");
            if let Some(pending) = self
                .pending_routed_namespace_ops
                .get_mut(&response.request_id)
            {
                pending.result = Some(PendingRoutedNamespaceResult {
                    result: DfsNamespaceResult::Rejected {
                        reason: reason.clone(),
                        posix_error: None,
                        current_primary_node_id: None,
                    },
                    posix_coordination_result: None,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_has_more: false,
                });
            }
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: primary_node_id,
                reason,
            });
        }
        if response_applied && apply_synced_path_object {
            for object in &response.synced_path_objects {
                if let Err(error) = self.apply_metadata_object_to_service_state(object.clone()) {
                    let reason = format!("routed namespace path sync failed: {error}");
                    if let Some(pending) = self
                        .pending_routed_namespace_ops
                        .get_mut(&response.request_id)
                    {
                        pending.result = Some(PendingRoutedNamespaceResult {
                            result: DfsNamespaceResult::Rejected {
                                reason: reason.clone(),
                                posix_error: None,
                                current_primary_node_id: None,
                            },
                            posix_coordination_result: None,
                            synced_path_object: None,
                            synced_path_objects: Vec::new(),
                            metadata_journal_has_more: false,
                        });
                    }
                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason,
                    });
                }
            }
        }
        let pending = self
            .pending_routed_namespace_ops
            .get_mut(&response.request_id)
            .expect("pending routed namespace op was validated before metadata replay");
        let (
            posix_coordination_result,
            synced_path_object,
            synced_path_objects,
            metadata_journal_has_more,
        ) = if response_applied {
            (
                response.posix_coordination_result.clone(),
                response.synced_path_object.clone(),
                response.synced_path_objects.clone(),
                response.metadata_journal_has_more.unwrap_or({
                    response.metadata_journal_batches.len()
                        >= DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT
                }),
            )
        } else {
            (None, None, Vec::new(), false)
        };
        pending.result = Some(PendingRoutedNamespaceResult {
            result: response.result.clone(),
            posix_coordination_result,
            synced_path_object,
            synced_path_objects,
            metadata_journal_has_more,
        });
        Ok(NetworkedDfsServiceEvent::RemoteNamespaceCompleted {
            request_id: response.request_id,
            primary_node_id,
            result: response.result,
        })
    }

    /// Verify that a successful namespace response still comes from the
    /// exact committed authority snapshot used to dispatch it. Namespace
    /// reads and coordination responses can legitimately contain no journal
    /// batches, so authority validation cannot live only in batch replay.
    fn validate_routed_namespace_response_authority(
        &mut self,
        primary_node_id: &NodeId,
        dispatch_authority_shard: Option<&DfsMetadataShardConfig>,
        expected_authority_shards: &[DfsMetadataShardConfig],
    ) -> Result<(), DfsError> {
        // Standalone/local services have no cluster authority view. A
        // clustered dispatch without an affected assignment cannot prove who
        // may complete the request and therefore fails closed.
        if expected_authority_shards.is_empty() {
            return if self.control_plane.is_none() {
                Ok(())
            } else {
                Err(DfsError::Distributed {
                    message: "clustered namespace response has no dispatch shard assignment"
                        .to_string(),
                })
            };
        }
        let Some(dispatch_authority_shard) = dispatch_authority_shard else {
            return Err(DfsError::Distributed {
                message: "clustered namespace response has no exact dispatch authority shard"
                    .to_string(),
            });
        };
        if &dispatch_authority_shard.primary_node_id != primary_node_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "response source {primary_node_id} was not dispatch authority {} for routing shard {}",
                    dispatch_authority_shard.primary_node_id, dispatch_authority_shard.shard_id
                ),
            });
        }
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Err(DfsError::Distributed {
                message: "routed namespace response requires a committed control-plane shard view"
                    .to_string(),
            });
        };
        for expected in expected_authority_shards {
            let Some(current) = DfsControlPlaneStore::new(control_plane.as_mut())
                .get_metadata_shard(&expected.file_system_id, &expected.shard_id)?
            else {
                return Err(DfsError::Distributed {
                    message: format!(
                        "affected namespace shard {} has no current committed assignment",
                        expected.shard_id
                    ),
                });
            };
            if current != *expected {
                return Err(DfsError::Distributed {
                    message: format!(
                        "dispatch authority for namespace shard {} epoch {} is no longer current (current primary {}, epoch {})",
                        expected.shard_id, expected.epoch, current.primary_node_id, current.epoch
                    ),
                });
            }
        }
        Ok(())
    }

    /// Refresh and bind a rejected response's redirect claim to the exact
    /// shard authority that selected the dispatch target. Other shards may be
    /// involved in a coordinated operation and legitimately have different
    /// primaries; they must not be mistaken for successors of the dispatch
    /// authority. Doing this while settling the response protects every
    /// caller, including branches that execute locally when the corroborated
    /// new primary is this node.
    fn validate_routed_namespace_authority_redirect(
        &mut self,
        attempted_primary: &NodeId,
        dispatch_authority_shard: Option<&DfsMetadataShardConfig>,
        expected_authority_shards: &[DfsMetadataShardConfig],
        redirected_primary: &NodeId,
    ) -> Result<(), DfsError> {
        let Some(dispatch_authority_shard) = dispatch_authority_shard else {
            return Err(DfsError::Distributed {
                message: format!(
                    "redirect from {attempted_primary} to {redirected_primary} has no exact dispatch shard assignment to refresh"
                ),
            });
        };
        if &dispatch_authority_shard.primary_node_id != attempted_primary {
            return Err(DfsError::Distributed {
                message: format!(
                    "redirect source {attempted_primary} was not dispatch authority {} for routing shard {}",
                    dispatch_authority_shard.primary_node_id, dispatch_authority_shard.shard_id
                ),
            });
        }
        if !expected_authority_shards.contains(dispatch_authority_shard) {
            return Err(DfsError::Distributed {
                message: format!(
                    "routing shard {} is missing from the immutable affected-shard authority snapshot",
                    dispatch_authority_shard.shard_id
                ),
            });
        }
        self.refresh_control_plane_latest()?;
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Err(DfsError::Distributed {
                message:
                    "namespace authority redirect requires a committed control-plane shard view"
                        .to_string(),
            });
        };
        // Shards that shared one bootstrap primary can later split between
        // different nodes. Only the operation's routing shard selects its
        // coordinator; co-owned destination shards are not redirect targets.
        let Some(current) = DfsControlPlaneStore::new(control_plane.as_mut()).get_metadata_shard(
            &dispatch_authority_shard.file_system_id,
            &dispatch_authority_shard.shard_id,
        )?
        else {
            return Err(DfsError::Distributed {
                message: format!(
                    "redirected dispatch namespace shard {} has no current committed assignment",
                    dispatch_authority_shard.shard_id
                ),
            });
        };
        if &current.primary_node_id != redirected_primary {
            return Err(DfsError::Distributed {
                message: format!(
                    "response named primary {redirected_primary}, but current dispatch namespace shard {} epoch {} names {}",
                    current.shard_id, current.epoch, current.primary_node_id
                ),
            });
        }
        Ok(())
    }

    /// The requester's last applied journal sequence per locally known shard,
    /// reported with routed namespace operations so the primary's response
    /// batches replay exactly the missing entries.
    #[cfg(test)]
    pub(crate) fn local_metadata_shard_cursors(
        &mut self,
    ) -> Result<Vec<DfsNamespaceShardCursor>, DfsError> {
        self.local_metadata_shard_cursors_for(None)
    }

    pub(crate) fn local_metadata_shard_cursors_for(
        &mut self,
        requested_shards: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<Vec<DfsNamespaceShardCursor>, DfsError> {
        let shards = match requested_shards {
            Some(requested) => requested
                .iter()
                .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
                .collect::<BTreeSet<_>>(),
            None => self
                .replication
                .local_metadata_journal_cursors
                .keys()
                .chain(self.replication.routed_replica_projections.keys())
                .cloned()
                .collect(),
        };
        for shard_id in &shards {
            self.refresh_routed_replica_projection(shard_id)?;
        }
        Ok(self
            .replication
            .local_metadata_journal_cursors
            .iter()
            .filter(|(shard_id, _)| shards.contains(shard_id))
            .map(|(shard_id, after_sequence)| DfsNamespaceShardCursor {
                shard_id: shard_id.clone(),
                after_sequence: *after_sequence,
            })
            .collect())
    }

    /// Poll every current metadata shard which can contain an object under
    /// `root_path`.
    ///
    /// Each returned batch retains its own shard-local cursor and order. The
    /// vector is sorted by shard id solely for deterministic transport/API
    /// behavior; it does not establish an order between events on different
    /// shards. Missing, retired, or epoch-stale cursors return an isolated
    /// `RescanRequired` batch and are never silently advanced.
    pub fn poll_namespace_changes_for_subtree(
        &mut self,
        file_system_id: DfsFileSystemId,
        root_path: &str,
        cursors: &[DfsNamespaceChangeCursor],
    ) -> Result<Vec<DfsNamespaceChangeBatch>, DfsError> {
        self.poll_namespace_changes_for_subtree_with_timeout(
            file_system_id,
            root_path,
            cursors,
            DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
        )
    }

    pub fn poll_namespace_changes_for_subtree_with_timeout(
        &mut self,
        file_system_id: DfsFileSystemId,
        root_path: &str,
        cursors: &[DfsNamespaceChangeCursor],
        timeout: Duration,
    ) -> Result<Vec<DfsNamespaceChangeBatch>, DfsError> {
        let root_path = posix_path::normalize_posix_path(root_path)
            .map_err(dfs_error_from_posix_namespace_sync)?;
        if self.control_plane.is_none() {
            return Err(DfsError::Distributed {
                message:
                    "routed namespace change polling requires a committed control-plane shard view"
                        .to_string(),
            });
        }
        self.refresh_control_plane_latest()?;
        let snapshot = self
            .metadata_shard_config_snapshot(&file_system_id)?
            .unwrap_or_default();
        let current_shards =
            crate::dfs_namespace_change_shards_for_subtree(&file_system_id, &root_path, &snapshot)?;

        let mut cursors_by_shard = BTreeMap::new();
        for cursor in cursors {
            if cursor.file_system_id != file_system_id {
                return Err(DfsError::Distributed {
                    message: format!(
                        "namespace change cursor filesystem {} does not match requested filesystem {file_system_id}",
                        cursor.file_system_id
                    ),
                });
            }
            if cursors_by_shard
                .insert(cursor.shard_id.clone(), cursor.clone())
                .is_some()
            {
                return Err(DfsError::Distributed {
                    message: format!(
                        "namespace change subtree poll contains duplicate cursor for shard {}",
                        cursor.shard_id.0
                    ),
                });
            }
        }

        let current_by_shard = current_shards
            .into_iter()
            .map(|shard| (DfsMetadataShardId::new(shard.shard_id.clone()), shard))
            .collect::<BTreeMap<_, _>>();
        let mut batches = BTreeMap::new();

        for (shard_id, cursor) in &cursors_by_shard {
            if !current_by_shard.contains_key(shard_id) {
                batches.insert(
                    shard_id.clone(),
                    DfsNamespaceChangeBatch::require_rescan(
                        cursor,
                        cursor.after_sequence,
                        DfsNamespaceRescanReason::ShardChanged,
                        format!(
                            "cursor shard {} no longer intersects namespace subtree {root_path}",
                            shard_id.0
                        ),
                    ),
                );
            }
        }

        for (shard_id, shard) in current_by_shard {
            let Some(cursor) = cursors_by_shard.get(&shard_id) else {
                let cursor = DfsNamespaceChangeCursor::from_shard_config(&shard, 0);
                batches.insert(
                    shard_id,
                    DfsNamespaceChangeBatch::require_rescan(
                        &cursor,
                        0,
                        DfsNamespaceRescanReason::ShardChanged,
                        format!(
                            "current namespace shard {} has no persisted cursor; snapshot subtree {root_path} before polling",
                            shard.shard_id
                        ),
                    ),
                );
                continue;
            };
            if cursor.authority_epoch != shard.epoch {
                // A full-scan consumer must be able to install the current
                // authority epoch as its new replay boundary. Returning the
                // stale input cursor here would complete the scan and then
                // immediately request the same epoch-change scan forever.
                let current_cursor =
                    DfsNamespaceChangeCursor::from_shard_config(&shard, cursor.after_sequence);
                batches.insert(
                    shard_id,
                    DfsNamespaceChangeBatch::require_rescan(
                        &current_cursor,
                        cursor.after_sequence,
                        DfsNamespaceRescanReason::AuthorityEpochChanged,
                        format!(
                            "cursor authority epoch {} does not match current shard {} epoch {}",
                            cursor.authority_epoch, shard.shard_id, shard.epoch
                        ),
                    ),
                );
                continue;
            }

            let operation = DfsNamespaceOperation::PollNamespaceChanges {
                request: DfsNamespaceChangePollRequest {
                    root_path: root_path.clone(),
                    cursor: cursor.clone(),
                },
            };
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    shard.primary_node_id.clone(),
                    operation,
                    timeout,
                    false,
                    false,
                )?;
            let batch = match (settled.result, settled.posix_coordination_result) {
                (
                    DfsNamespaceResult::Applied,
                    Some(DfsPosixCoordinationResult::NamespaceChanges(response)),
                ) => {
                    let returned = &response.batch.next_cursor;
                    if returned.file_system_id != file_system_id
                        || returned.shard_id != cursor.shard_id
                        || returned.authority_epoch != shard.epoch
                    {
                        return Err(DfsError::Distributed {
                            message: format!(
                                "namespace change primary {} returned a batch for cursor {}/{}/epoch {}, expected {}/{}/epoch {}",
                                shard.primary_node_id,
                                returned.file_system_id,
                                returned.shard_id.0,
                                returned.authority_epoch,
                                file_system_id,
                                cursor.shard_id.0,
                                shard.epoch
                            ),
                        });
                    }
                    response.batch
                }
                (DfsNamespaceResult::Applied, other) => {
                    return Err(DfsError::Distributed {
                        message: format!(
                            "namespace change primary {} applied shard {} poll without the typed namespace-change response: {other:?}",
                            shard.primary_node_id, shard.shard_id
                        ),
                    });
                }
                (DfsNamespaceResult::Rejected { reason, .. }, _) => {
                    DfsNamespaceChangeBatch::require_rescan(
                        cursor,
                        cursor.after_sequence,
                        DfsNamespaceRescanReason::AuthorityEpochChanged,
                        format!(
                            "namespace change authority rejected shard {} poll: {reason}",
                            shard.shard_id
                        ),
                    )
                }
            };
            batches.insert(shard_id, batch);
        }

        Ok(batches.into_values().collect())
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_with_timeout(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        timeout: Duration,
    ) -> Result<DfsNamespaceResult, DfsError> {
        Ok(self
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                file_system_id,
                primary_node_id,
                operation,
                timeout,
                true,
                true,
            )?
            .result)
    }

    pub(crate) fn route_namespace_lookup_path_to_metadata_primary(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        path: String,
        timeout: Duration,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        self.route_namespace_lookup_path_to_metadata_primary_with_inline_range(
            file_system_id,
            primary_node_id,
            path,
            None,
            timeout,
        )
        .map(|(object, _)| object)
    }

    pub(crate) fn route_namespace_lookup_path_range_to_metadata_primary(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        path: String,
        offset: u64,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<(Option<DfsObjectMetadata>, Option<DfsReadRangeData>), DfsError> {
        self.route_namespace_lookup_path_to_metadata_primary_with_inline_range(
            file_system_id,
            primary_node_id,
            path,
            (max_bytes <= DFS_NAMESPACE_LOOKUP_INLINE_RANGE_MAX_BYTES)
                .then_some(DfsNamespaceLookupRangeRequest { offset, max_bytes }),
            timeout,
        )
    }

    fn route_namespace_lookup_path_to_metadata_primary_with_inline_range(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        path: String,
        inline_range: Option<DfsNamespaceLookupRangeRequest>,
        timeout: Duration,
    ) -> Result<(Option<DfsObjectMetadata>, Option<DfsReadRangeData>), DfsError> {
        let mut primary = primary_node_id;
        let operation = DfsNamespaceOperation::LookupPath { path, inline_range };
        for _ in 0..DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT {
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    timeout,
                    false,
                    false,
                )?;
            match settled.result {
                DfsNamespaceResult::Applied => {
                    let inline_range = match settled.posix_coordination_result {
                        Some(DfsPosixCoordinationResult::LookupPathRange(data)) => Some(data),
                        None => None,
                        Some(result) => {
                            return Err(DfsError::Distributed {
                                message: format!(
                                    "routed namespace lookup returned unexpected coordination result {result:?}"
                                ),
                            });
                        }
                    };
                    return Ok((settled.synced_path_object, inline_range));
                }
                DfsNamespaceResult::Rejected {
                    reason: _,
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id => {
                    return self
                        .synced_namespace_object_for_operation(&file_system_id, &operation)
                        .map(|object| (object, None));
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    let next_primary = self.refreshed_primary_after_authority_redirect(
                        &file_system_id,
                        &operation,
                        &primary,
                        current,
                    )?;
                    if next_primary == self.node_id {
                        return self
                            .synced_namespace_object_for_operation(&file_system_id, &operation)
                            .map(|object| (object, None));
                    }
                    primary = next_primary;
                }
                DfsNamespaceResult::Rejected { reason, .. } => {
                    return Err(DfsError::Distributed { message: reason });
                }
            }
        }
        Err(DfsError::Distributed {
            message: format!(
                "routed namespace lookup exceeded {DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        })
    }

    pub(crate) fn route_namespace_lookup_paths_to_metadata_primary(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        paths: Vec<String>,
        timeout: Duration,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut primary = primary_node_id;
        let operation = DfsNamespaceOperation::LookupPaths { paths };
        for _ in 0..DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT {
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    timeout,
                    false,
                    false,
                )?;
            match settled.result {
                DfsNamespaceResult::Applied => return Ok(settled.synced_path_objects),
                DfsNamespaceResult::Rejected {
                    reason: _,
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id => {
                    return self
                        .synced_namespace_objects_for_operation(&file_system_id, &operation);
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    let next_primary = self.refreshed_primary_after_authority_redirect(
                        &file_system_id,
                        &operation,
                        &primary,
                        current,
                    )?;
                    if next_primary == self.node_id {
                        return self
                            .synced_namespace_objects_for_operation(&file_system_id, &operation);
                    }
                    primary = next_primary;
                }
                DfsNamespaceResult::Rejected { reason, .. } => {
                    return Err(DfsError::Distributed { message: reason });
                }
            }
        }
        Err(DfsError::Distributed {
            message: format!(
                "routed namespace lookup exceeded {DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        })
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_without_local_replay(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<DfsNamespaceResult, DfsError> {
        Ok(self
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                file_system_id,
                primary_node_id,
                operation,
                DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                false,
                false,
            )?
            .result)
    }

    pub(crate) fn refreshed_primary_after_authority_redirect(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        attempted_primary: &NodeId,
        redirected_primary: NodeId,
    ) -> Result<NodeId, DfsError> {
        self.refresh_control_plane_latest()?;
        let shards = self.namespace_operation_authority_shards(file_system_id, operation)?;
        let Some(dispatch_shard) =
            Self::namespace_operation_dispatch_shard(file_system_id, operation, &shards)?
        else {
            return Err(DfsError::Distributed {
                message: format!(
                    "namespace authority redirect from {attempted_primary} to {redirected_primary} has no current routing-shard assignment"
                ),
            });
        };
        if dispatch_shard.primary_node_id != redirected_primary {
            return Err(DfsError::Distributed {
                message: format!(
                    "namespace authority redirect from {attempted_primary} named {redirected_primary}, but current routing shard {} names {}",
                    dispatch_shard.shard_id, dispatch_shard.primary_node_id
                ),
            });
        }
        Ok(redirected_primary)
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_with_timeout_and_options(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        timeout: Duration,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let request_id = self.next_request_id();
        self.route_namespace_operation_to_metadata_primary_with_request_id_and_options(
            request_id,
            file_system_id,
            primary_node_id,
            operation,
            timeout,
            replay_journal_batches,
            apply_synced_path_object,
        )
    }

    /// Route one namespace operation using a caller-owned durable identity.
    /// Redirects and response-loss retries must reuse this exact id; the
    /// metadata primary binds it to the command digest and persists the
    /// terminal result before replying.
    pub(crate) fn route_namespace_operation_to_metadata_primary_with_request_id_and_options(
        &mut self,
        request_id: String,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        timeout: Duration,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        validate_namespace_operation_id(&request_id)?;
        let requested_deadline = Instant::now() + timeout;
        let previous_deadline = self
            .transport
            .set_operation_deadline(Some(requested_deadline));
        let effective_deadline = previous_deadline
            .filter(|existing| *existing < requested_deadline)
            .unwrap_or(requested_deadline);
        if effective_deadline != requested_deadline {
            self.transport
                .set_operation_deadline(Some(effective_deadline));
        }
        let result = self.route_namespace_operation_to_metadata_primary_with_deadline(
            request_id,
            file_system_id,
            primary_node_id,
            operation,
            effective_deadline,
            replay_journal_batches,
            apply_synced_path_object,
        );
        self.transport.set_operation_deadline(previous_deadline);
        result
    }

    fn route_namespace_operation_to_metadata_primary_with_deadline(
        &mut self,
        request_id: String,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        deadline: Instant,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let authority_shards = if self.control_plane.is_some() {
            self.namespace_operation_authority_shards(&file_system_id, &operation)?
        } else {
            if replay_journal_batches {
                return Err(DfsError::Distributed {
                    message: "routed namespace journal replay requires a committed control-plane shard view"
                        .to_string(),
                });
            }
            Vec::new()
        };
        // Refresh only this operation's authority shards. Unrelated replicas
        // must not be materialized just to construct a routed request.
        let metadata_journal_after_sequences = if replay_journal_batches {
            self.local_metadata_shard_cursors_for(Some(&authority_shards))?
        } else {
            Vec::new()
        };
        let dispatch_authority_shard = if self.control_plane.is_some() {
            let dispatch = Self::namespace_operation_dispatch_shard(
                &file_system_id,
                &operation,
                &authority_shards,
            )?
            .ok_or_else(|| DfsError::Distributed {
                message: "clustered routed namespace operation has no dispatch shard assignment"
                    .to_string(),
            })?;
            if dispatch.primary_node_id != primary_node_id {
                return Err(DfsError::Distributed {
                    message: format!(
                        "routed namespace target {primary_node_id} is not current dispatch authority {} for routing shard {} epoch {}",
                        dispatch.primary_node_id, dispatch.shard_id, dispatch.epoch
                    ),
                });
            }
            Some(dispatch)
        } else {
            None
        };
        let sent = match self.send_dfs_message_if_routable(
            &primary_node_id,
            DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: request_id.clone(),
                file_system_id: file_system_id.clone(),
                requesting_node_id: self.node_id.clone(),
                include_metadata_journal_batches: replay_journal_batches,
                metadata_journal_after_sequences,
                authority_shards: authority_shards.clone(),
                operation,
            }),
        ) {
            Ok(sent) => sent,
            Err(error @ DfsError::Network { .. }) => {
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if !sent {
            return Err(DfsError::Network {
                message: format!(
                    "metadata primary {primary_node_id} is not routable for namespace operation"
                ),
            });
        }
        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
        else {
            return Err(DfsError::Network {
                message: format!(
                    "routed namespace operation {request_id} timed out before response settle"
                ),
            });
        };
        self.pending_routed_namespace_ops.insert(
            request_id.clone(),
            PendingRoutedNamespaceOp {
                primary_node_id,
                dispatch_authority_shard,
                authority_shards,
                replay_journal_batches,
                apply_synced_path_object,
                result: None,
            },
        );
        self.settle_routed_namespace_op(&request_id, remaining)
    }

    fn settle_routed_namespace_op(
        &mut self,
        request_id: &str,
        timeout: Duration,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        self.routed_namespace_settle_depth = self.routed_namespace_settle_depth.saturating_add(1);
        let result = self.settle_routed_namespace_op_inner(request_id, timeout);
        self.routed_namespace_settle_depth = self.routed_namespace_settle_depth.saturating_sub(1);
        result
    }

    fn settle_routed_namespace_op_inner(
        &mut self,
        request_id: &str,
        timeout: Duration,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let started_at = Instant::now();
        loop {
            if let Err(cancelled) = self.check_cancelled("routed namespace settle") {
                self.pending_routed_namespace_ops.remove(request_id);
                return Err(cancelled);
            }
            let events = match self.process_incoming() {
                Ok(events) => events,
                Err(error) => {
                    self.pending_routed_namespace_ops.remove(request_id);
                    return Err(error);
                }
            };
            if self
                .pending_routed_namespace_ops
                .get(request_id)
                .is_some_and(|pending| pending.result.is_some())
            {
                let pending = self
                    .pending_routed_namespace_ops
                    .remove(request_id)
                    .expect("pending routed namespace op checked above");
                return Ok(pending.result.expect("settled result checked above"));
            }
            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                self.pending_routed_namespace_ops.remove(request_id);
                return Err(DfsError::Network {
                    message: format!(
                        "routed namespace operation {request_id} timed out waiting for the metadata primary"
                    ),
                });
            }
            if events.is_empty() {
                let wait = DFS_ROUTED_NAMESPACE_IDLE_WAIT.min(timeout.saturating_sub(elapsed));
                self.transport
                    .wait_for_incoming(wait)
                    .map_err(DfsError::from_network)?;
            }
        }
    }
}

/// The placement-only async path owns exactly one local response slot and the
/// temporary transport/settle state. Dropping its future abandons local waiting;
/// it never undoes a remote operation or asserts that dispatch was unapplied.
struct AsyncPlacementNamespaceSession<'a, T: DfsMessageTransport> {
    service: &'a mut NetworkedDfsService<T>,
    request_id: String,
    deadline: Instant,
    previous_deadline: Option<Instant>,
    previous_settle_depth: usize,
    pending_owned: bool,
    dispatch_started: bool,
}

impl<T: DfsMessageTransport> Drop for AsyncPlacementNamespaceSession<'_, T> {
    fn drop(&mut self) {
        if self.pending_owned {
            self.service
                .pending_routed_namespace_ops
                .remove(&self.request_id);
        }
        self.service.routed_namespace_settle_depth = self.previous_settle_depth;
        self.service
            .transport
            .set_operation_deadline(self.previous_deadline);
    }
}

impl<T: DfsMessageTransport> AsyncPlacementNamespaceSession<'_, T> {
    fn timeout_error(&self, phase: &str) -> DfsError {
        DfsError::NetworkDelivery {
            delivery_state: if self.dispatch_started {
                sinomo_core::network::NetworkDeliveryState::PossiblyApplied
            } else {
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent
            },
            message: format!(
                "placement namespace operation {} timed out during {phase}",
                self.request_id
            ),
        }
    }
}

impl<T> NetworkedDfsService<T>
where
    T: DfsMessageTransport + AsyncDfsMessageTransport,
{
    /// Async transport/settle seam for managed placement and its read-only
    /// no-op verification. The caller owns
    /// the durable request identity and decides whether an uncertain outcome
    /// can be reconciled/retried; this method never invents a replacement id.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
        &mut self,
        request_id: String,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        timeout: Duration,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let readonly_placement_lookup =
            matches!(&operation, DfsNamespaceOperation::LookupPaths { .. })
                && !replay_journal_batches
                && !apply_synced_path_object;
        if !matches!(&operation, DfsNamespaceOperation::ReconcilePlacement { .. })
            && !readonly_placement_lookup
        {
            return Err(DfsError::Distributed {
                message: "async placement routing accepts only ReconcilePlacement or read-only LookupPaths".to_string(),
            });
        }
        validate_namespace_operation_id(&request_id)?;
        if self.pending_routed_namespace_ops.contains_key(&request_id) {
            return Err(DfsError::Distributed {
                message: format!("placement namespace request {request_id} is already pending"),
            });
        }
        let requested_deadline = Instant::now() + timeout;
        let previous_settle_depth = self.routed_namespace_settle_depth;
        let previous_deadline = self
            .transport
            .set_operation_deadline(Some(requested_deadline));
        let deadline = previous_deadline
            .filter(|existing| *existing < requested_deadline)
            .unwrap_or(requested_deadline);
        if deadline != requested_deadline {
            self.transport.set_operation_deadline(Some(deadline));
        }
        let mut session = AsyncPlacementNamespaceSession {
            service: self,
            request_id,
            deadline,
            previous_deadline,
            previous_settle_depth,
            pending_owned: false,
            dispatch_started: false,
        };
        let authority_shards = if session.service.control_plane.is_some() {
            session
                .service
                .namespace_operation_authority_shards(&file_system_id, &operation)?
        } else {
            if replay_journal_batches {
                return Err(DfsError::Distributed {
                    message: "routed namespace journal replay requires a committed control-plane shard view".to_string(),
                });
            }
            Vec::new()
        };
        // Refresh only this operation's authority shards. Unrelated replicas
        // must not be materialized just to construct a routed request.
        let metadata_journal_after_sequences = if replay_journal_batches {
            session
                .service
                .local_metadata_shard_cursors_for(Some(&authority_shards))?
        } else {
            Vec::new()
        };
        let dispatch_authority_shard = if session.service.control_plane.is_some() {
            let dispatch = Self::namespace_operation_dispatch_shard(
                &file_system_id,
                &operation,
                &authority_shards,
            )?
            .ok_or_else(|| DfsError::Distributed {
                message: "clustered routed namespace operation has no dispatch shard assignment"
                    .to_string(),
            })?;
            if dispatch.primary_node_id != primary_node_id {
                return Err(DfsError::Distributed {
                    message: format!(
                        "routed namespace target {primary_node_id} is not current dispatch authority {} for routing shard {} epoch {}",
                        dispatch.primary_node_id, dispatch.shard_id, dispatch.epoch
                    ),
                });
            }
            Some(dispatch)
        } else {
            None
        };
        if Instant::now() >= session.deadline {
            return Err(session.timeout_error("before dispatch"));
        }
        let command = DfsNamespaceCommand {
            request_id: session.request_id.clone(),
            file_system_id,
            requesting_node_id: session.service.node_id.clone(),
            include_metadata_journal_batches: replay_journal_batches,
            metadata_journal_after_sequences,
            authority_shards: authority_shards.clone(),
            operation,
        };
        session.dispatch_started = true;
        let sent = tokio::time::timeout_at(
            tokio::time::Instant::from_std(session.deadline),
            session.service.send_dfs_message_if_routable_async(
                &primary_node_id,
                DfsNetworkMessage::NamespaceRequest(command),
            ),
        )
        .await;
        let sent = match sent {
            Ok(result) => result?,
            Err(_) => return Err(session.timeout_error("dispatch")),
        };
        if !sent {
            return Err(DfsError::Network {
                message: format!(
                    "metadata primary {primary_node_id} is not routable for namespace operation"
                ),
            });
        }
        if Instant::now() >= session.deadline {
            return Err(session.timeout_error("before response settle"));
        }
        session.service.pending_routed_namespace_ops.insert(
            session.request_id.clone(),
            PendingRoutedNamespaceOp {
                primary_node_id,
                dispatch_authority_shard,
                authority_shards,
                replay_journal_batches,
                apply_synced_path_object,
                result: None,
            },
        );
        session.pending_owned = true;
        session.service.routed_namespace_settle_depth = previous_settle_depth.saturating_add(1);
        loop {
            session.service.check_cancelled("routed namespace settle")?;
            let events = tokio::time::timeout_at(
                tokio::time::Instant::from_std(session.deadline),
                session.service.process_incoming_async(),
            )
            .await;
            let events = match events {
                Ok(result) => result?,
                Err(_) => return Err(session.timeout_error("response processing")),
            };
            if session
                .service
                .pending_routed_namespace_ops
                .get(&session.request_id)
                .is_some_and(|pending| pending.result.is_some())
            {
                let pending = session
                    .service
                    .pending_routed_namespace_ops
                    .remove(&session.request_id)
                    .expect("pending placement namespace op checked above");
                return Ok(pending
                    .result
                    .expect("settled placement result checked above"));
            }
            let Some(remaining) = session
                .deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
            else {
                return Err(session.timeout_error("metadata primary response"));
            };
            if events.is_empty() {
                let waiting = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(session.deadline),
                    session
                        .service
                        .transport
                        .wait_for_incoming_async(DFS_ROUTED_NAMESPACE_IDLE_WAIT.min(remaining)),
                )
                .await;
                match waiting {
                    Ok(result) => {
                        result.map_err(DfsError::from_network)?;
                    }
                    Err(_) => return Err(session.timeout_error("incoming wait")),
                }
            } else {
                // Ready async transports must still allow foreground preemption
                // between complete incoming batches, never inside a sync handler.
                tokio::task::yield_now().await;
            }
        }
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::protocol::DfsNamespaceObjectPrecondition;
    use sinomo_core::DfsObjectPolicy;

    #[test]
    fn caller_operation_ids_are_bounded_and_reconciliation_is_structured() {
        assert!(validate_namespace_operation_id("stable-operation-42").is_ok());
        assert!(validate_namespace_operation_id("").is_err());
        assert!(validate_namespace_operation_id(&"x".repeat(257)).is_err());
        assert!(validate_namespace_operation_id("line\nbreak").is_err());

        let path = "/services/reconcile-me";
        let command = DfsNamespaceCommand {
            request_id: "stable-operation-42".to_string(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: NodeId::from_legacy_named("node_reconcile").expect("node"),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::ConditionalUnlink {
                path: path.to_string(),
                expected: DfsNamespaceObjectPrecondition {
                    object_id: DfsObjectId::from_legacy_named("object_reconcile").expect("object"),
                    version: DfsVersion::from_legacy_named("version_reconcile").expect("version"),
                },
            },
        };
        assert_eq!(
            namespace_reconciliation_conflict(&command, "history expired"),
            Some(DfsNamespacePosixError::Conflict {
                path: path.to_string(),
                reason: "history expired".to_string(),
            })
        );
    }

    fn stable_mkdir_command(
        requester: &NodeId,
        operation_id: &str,
        mode: u32,
    ) -> DfsNamespaceCommand {
        DfsNamespaceCommand {
            request_id: operation_id.to_string(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::MkdirWithAttrs {
                path: "/services/cloud-directory".to_string(),
                attrs: DfsPosixCreateAttrs::new(mode, 1000, 1000),
            },
        }
    }

    #[test]
    fn stable_mkdir_receipt_reconciliation_binds_attrs_and_fails_closed_after_expiry() {
        let requester = NodeId::from_legacy_named("stable-mkdir-node").expect("node");
        let operation_id = "stable-mkdir-9";
        let command = stable_mkdir_command(&requester, operation_id, 0o755);
        let digest = namespace_command_digest(&command).expect("command digest");
        assert_ne!(
            digest,
            namespace_command_digest(&stable_mkdir_command(&requester, operation_id, 0o700,))
                .expect("different attrs digest"),
            "directory create attributes are part of the durable request digest"
        );

        let receipt = DfsNamespaceRequestReceipt {
            file_system_id: command.file_system_id.clone(),
            requester_node_id: requester.clone(),
            request_id: operation_id.to_string(),
            operation_id: namespace_request_operation_id(&requester, operation_id),
            authority_shard_id: DfsMetadataShardId::new("services"),
            command_digest: digest.clone(),
            result: DfsNamespaceResult::Applied,
            posix_coordination_result: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            retained_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        let snapshot = DfsNamespaceRequestAdmissionSnapshot {
            receipt: Some(receipt),
            ..DfsNamespaceRequestAdmissionSnapshot::default()
        };
        let reconciled = classify_stable_namespace_operation(
            operation_id,
            &command.file_system_id,
            &requester,
            &digest,
            &command,
            Some(&snapshot),
            false,
        );
        let DfsStableNamespaceOperationReconciliation::Applied { receipt } = reconciled else {
            panic!("exact stable mkdir receipt must reconcile as applied");
        };
        assert_eq!(receipt.operation_id, operation_id);
        assert_eq!(receipt.command_digest, digest);

        let different = stable_mkdir_command(&requester, operation_id, 0o700);
        let different_digest = namespace_command_digest(&different).expect("different digest");
        assert!(matches!(
            classify_stable_namespace_operation(
                operation_id,
                &different.file_system_id,
                &requester,
                &different_digest,
                &different,
                Some(&snapshot),
                true,
            ),
            DfsStableNamespaceOperationReconciliation::Conflict { .. }
        ));

        assert_eq!(
            classify_stable_namespace_operation(
                operation_id,
                &command.file_system_id,
                &requester,
                &digest,
                &command,
                Some(&DfsNamespaceRequestAdmissionSnapshot::default()),
                true,
            ),
            DfsStableNamespaceOperationReconciliation::NotApplied
        );
        assert!(matches!(
            classify_stable_namespace_operation(
                operation_id,
                &command.file_system_id,
                &requester,
                &digest,
                &command,
                Some(&DfsNamespaceRequestAdmissionSnapshot {
                    history_expired: true,
                    ..DfsNamespaceRequestAdmissionSnapshot::default()
                }),
                true,
            ),
            DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired { .. }
        ));
    }

    fn adoption_object() -> DfsObjectMetadata {
        DfsObjectMetadata {
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            object_id: DfsObjectId::from_legacy_named("object_pin_adoption").expect("object"),
            path: "/services/pinned.txt".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("node_target").expect("node"),
            current_locations: Vec::new(),
            content_hash: Some("blake3:test".to_string()),
            content_size_bytes: Some(4),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named("version_pin_adoption").expect("version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    #[test]
    fn pin_aware_adoption_uses_durable_admission_for_typed_receipt_replay() {
        let object = adoption_object();
        let operation = DfsNamespaceOperation::AdoptObjectAtPath {
            object: object.clone(),
            source_path: "/users/pinned.txt".to_string(),
            pin_transfer_id: Some("pin-transfer".to_string()),
            versions: Vec::new(),
            version_pins: Vec::new(),
            version_pin_set_fingerprint: Some("blake3:pins".to_string()),
            receipts: Vec::new(),
            intents: Vec::new(),
            intent_receipts: Vec::new(),
            intent_receipt_retention: DfsWriteIntentReceiptRetentionState::default(),
            conflicts: Vec::new(),
            advisory_lock_state: None,
        };

        assert!(!namespace_operation_requires_fresh_authority(&operation));
        assert!(namespace_operation_uses_durable_request_admission(
            &operation
        ));
        assert!(!namespace_operation_can_fuse_durable_request_admission(
            &operation
        ));
        assert!(namespace_operation_response_retry_safe(&operation));

        let requester = NodeId::from_legacy_named("node_adoption_source").expect("node");
        let request_id = "xsj_adoption_phase";
        let command = DfsNamespaceCommand {
            request_id: request_id.to_string(),
            file_system_id: object.file_system_id.clone(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation,
        };
        let digest = namespace_command_digest(&command).expect("command digest");
        let typed_result = DfsPosixCoordinationResult::ObjectFamilyAdoption {
            object_id: object.object_id.clone(),
            target_version: object.version.clone(),
            pin_transfer_id: "pin-transfer".to_string(),
            pin_transfer_active: false,
            version_pin_set_fingerprint: "blake3:pins".to_string(),
            advisory_lock_state_fingerprint: None,
            durable_through_sequence: 42,
        };
        let receipt = DfsNamespaceRequestReceipt {
            file_system_id: object.file_system_id,
            requester_node_id: requester.clone(),
            request_id: request_id.to_string(),
            operation_id: namespace_request_operation_id(&requester, request_id),
            authority_shard_id: DfsMetadataShardId::new("services"),
            command_digest: digest.clone(),
            result: DfsNamespaceResult::Applied,
            posix_coordination_result: Some(typed_result.clone()),
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            retained_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        let reconciliation = classify_stable_namespace_operation(
            request_id,
            &command.file_system_id,
            &requester,
            &digest,
            &command,
            Some(&DfsNamespaceRequestAdmissionSnapshot {
                receipt: Some(receipt),
                ..DfsNamespaceRequestAdmissionSnapshot::default()
            }),
            false,
        );
        let DfsStableNamespaceOperationReconciliation::Applied { receipt } = reconciliation else {
            panic!("exact adoption receipt must replay as applied");
        };
        assert_eq!(receipt.posix_coordination_result, Some(typed_result));
    }

    #[test]
    fn pin_transfer_finalization_stamps_only_the_destination_authority() {
        let operation = DfsNamespaceOperation::FinalizeObjectFamilyAdoption {
            path: "/services/pinned.txt".to_string(),
            source_path: "/users/pinned.txt".to_string(),
            object_id: DfsObjectId::from_legacy_named("object_pin_finalize").expect("object"),
            target_version: DfsVersion::from_legacy_named("version_pin_finalize").expect("version"),
            pin_transfer_id: "pin-transfer".to_string(),
        };

        assert_eq!(operation.routing_path(), Some("/services/pinned.txt"));
        assert_eq!(operation.involved_paths(), vec!["/services/pinned.txt"]);
        assert!(!namespace_operation_requires_fresh_authority(&operation));
        assert!(namespace_operation_uses_durable_request_admission(
            &operation
        ));
        assert!(namespace_operation_response_retry_safe(&operation));
    }
}

#[cfg(test)]
mod async_placement_tests;
