//! Native browser policy, projection and durable retry contracts.
use super::*;

fn node(name: &str) -> sinomo_core::node::NodeId {
    sinomo_core::node::NodeId::from_legacy_named(name).expect("node id")
}

fn native_browser_service(
    name: &str,
    local: sinomo_core::node::NodeId,
) -> (
    crate::test_support::DfsTestRuntime,
    NetworkedDfsService<crate::DfsNodeTransport>,
) {
    let mut runtime = crate::test_support::DfsTestRuntime::new(name, local.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let service = runtime.client(&local).unwrap();
    (runtime, service)
}

#[test]
fn hosted_browser_routes_file_work_without_a_private_native_metadata_image() {
    use crate::DaemonDfsBrowserNoLinksOperation as O;
    use crate::DaemonDfsBrowserNoLinksResponse as R;
    use crate::test_runtime::{CapturedServedMessageWork, RpcObservation};
    use crate::test_support::DfsTestRpcPhase as Phase;
    use crate::{DfsNamespaceOperation, DfsNetworkMessage};

    fn observed<T>(
        runtime: &crate::test_support::DfsTestRuntime,
        node: &sinomo_core::node::NodeId,
        action: impl FnOnce() -> T,
    ) -> (T, Vec<CapturedServedMessageWork>) {
        let capture = runtime.capture_served_work(node, 32).unwrap();
        let result = action();
        let records = capture.records();
        drop(capture);
        (result, records)
    }

    fn completed(work: &CapturedServedMessageWork, calls: &[RpcObservation]) -> bool {
        let frame = &work.command.routed_message;
        let request = match &frame.message {
            DfsNetworkMessage::ExclusiveCreateRequest(command)
            | DfsNetworkMessage::DistributedWriteRequest(command) => &command.request_id,
            DfsNetworkMessage::NamespaceRequest(command) => &command.request_id,
            _ => return false,
        };
        work.succeeded
            && calls.iter().any(|decoded| {
                decoded.source == frame.source_node_id
                    && decoded.target == frame.target_node_id
                    && decoded.phase == Phase::Decoded
                    && decoded.dfs_frame_id == Some(frame.frame_id)
                    && decoded.dfs_request_id.as_ref() == Some(request)
                    && calls.iter().any(|returned| {
                        returned.call_id == decoded.call_id
                            && returned.phase == Phase::Completed
                            && returned.outcome_error.is_none()
                    })
            })
    }

    const ROOT: &str = "/services/browser-no-private-native";
    const PATH: &str = "/services/browser-no-private-native/value.bin";
    const FIRST: &[u8] = b"actual browser original bytes";
    const SECOND: &[u8] = b"actual browser replacement bytes";
    let (runtime, _) = native_browser_runtime("native-browser-no-private-native-image", 3);
    let services = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let primary = services.primary_node_id.clone();
    native_browser_policy(
        &runtime.shared_control_plane().unwrap(),
        ROOT,
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 2, 2).unwrap(),
    );
    let data = Arc::new(runtime.data_dir(&primary).clone());
    let handle = runtime.runtime_handle();
    let host = dfs_browser_component_host(&data).unwrap();
    handle.block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            host.ensure_write_configuration_ready(&data, &handle),
        )
        .await
        .unwrap()
        .unwrap();
    });
    // Acquire native evidence before measuring the requester. Main's actual
    // executor and this observer remain separate from the synchronous browser.
    let native_observer = runtime.metadata_primary_service(PATH).unwrap();
    let rpc_begin = runtime.rpc_observations().len();
    crate::metadata_backend::reset_metadata_backend_open_calls();
    crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
    let backend = DfsBrowserBackend::for_component_data_dir(Arc::clone(&data), &handle).unwrap();
    let state = |phase: &'static str| {
        let service = backend.lock_service().unwrap();
        (
            phase,
            service.metadata_backend().is_none(),
            service.transport.delegates_local_namespace_authority(),
            crate::metadata_backend::metadata_backend_open_calls(),
            crate::metadata_backend::metadata_current_tables_snapshot_calls(),
        )
    };
    let mut states = vec![state("opened")];
    let request = |path: &str, operation| crate::DaemonDfsBrowserNoLinksRequest {
        expected_cluster_id: None,
        root: "/services".into(),
        path: path.into(),
        operation,
    };
    assert_eq!(
        backend.no_links(request(ROOT, O::CreateDirAll)).unwrap(),
        R::DirectoryCreated
    );
    backend
        .no_links(request(
            "/services/browser-no-private-native/child",
            O::CreateDirAll,
        ))
        .unwrap();
    let directory = backend
        .namespace_lstat(crate::DaemonDfsBrowserStatRequest {
            path: ROOT.into(),
            max_staleness_millis: 0,
        })
        .unwrap()
        .into_result()
        .unwrap();
    assert_eq!(directory.kind, DfsObjectKind::Directory);
    assert_eq!(directory.link_count, 3);
    states.push(state("directory"));
    let (created, create_work) = observed(&runtime, &primary, || {
        backend
            .no_links(request(
                PATH,
                O::CreateNew {
                    bytes: FIRST.to_vec(),
                },
            ))
            .unwrap()
    });
    let R::Metadata { metadata: created } = created else {
        panic!("actual native create receipt, received {created:?}");
    };
    states.push(state("created"));
    let (replaced, replace_work) = observed(&runtime, &primary, || {
        backend
            .no_links(request(
                PATH,
                O::ReplaceIfVersion {
                    expected_object_id: created.object_id.clone(),
                    expected_version: created.version.clone(),
                    bytes: SECOND.to_vec(),
                },
            ))
            .unwrap()
    });
    let R::Metadata { metadata: replaced } = replaced else {
        panic!("actual native replacement receipt, received {replaced:?}");
    };
    assert_eq!(created.object_id, replaced.object_id);
    assert_ne!(created.version, replaced.version);
    states.push(state("replaced"));
    let (stale, _) = observed(&runtime, &primary, || {
        backend
            .no_links(request(
                PATH,
                O::ReplaceIfVersion {
                    expected_object_id: created.object_id.clone(),
                    expected_version: created.version.clone(),
                    bytes: b"must not replace current bytes".to_vec(),
                },
            ))
            .unwrap()
    });
    assert!(matches!(
        stale,
        R::Failed {
            failure: crate::DaemonDfsBrowserNoLinksFailure::Conflict { .. }
        }
    ));
    states.push(state("stale-cas-rejected"));
    let (read, read_work) = observed(&runtime, &primary, || {
        backend
            .no_links(request(
                PATH,
                O::ReadRange {
                    offset: 0,
                    max_bytes: 4096,
                },
            ))
            .unwrap()
    });
    let R::Read {
        metadata: read_stat,
        bytes,
        truncated,
    } = read
    else {
        panic!("actual browser bytes");
    };
    assert_eq!(bytes, SECOND);
    assert!(!truncated);
    assert_eq!(
        (read_stat.object_id, read_stat.version),
        (replaced.object_id.clone(), replaced.version.clone())
    );
    states.push(state("read"));
    let (stat, stat_work) = observed(&runtime, &primary, || {
        backend
            .stat(crate::DaemonDfsBrowserStatRequest {
                path: PATH.into(),
                max_staleness_millis: 0,
            })
            .unwrap()
            .into_result()
            .unwrap()
    });
    assert_eq!(
        (stat.object_id.clone(), stat.version.clone()),
        (replaced.object_id.clone(), replaced.version.clone())
    );
    assert_eq!(
        stat.content_hash,
        Some(crate::content_store::content_hash(SECOND))
    );
    assert_eq!(stat.size_bytes, Some(SECOND.len() as u64));
    let object_health = stat.health.as_ref().expect("shared Main object inspection");
    assert!(object_health.version_count >= 1);
    assert!(object_health.receipt_count >= 2);
    assert!(object_health.intent_count >= 1);
    states.push(state("stat"));
    let health = backend.health_summary().unwrap();
    assert!(health.file_system_count >= 1);
    assert!(health.object_count >= 1);
    assert!(health.metadata_shard_count >= 1);
    states.push(state("health-summary"));
    let (list, list_work) = observed(&runtime, &primary, || {
        backend
            .no_links(request(
                ROOT,
                O::ListPage {
                    cursor: None,
                    max_entries: 16,
                },
            ))
            .unwrap()
    });
    let R::DirectoryPage {
        entries,
        next_cursor,
        ..
    } = list
    else {
        panic!("actual browser directory page");
    };
    assert!(entries.iter().any(|entry| entry.name == "value.bin"));
    assert!(next_cursor.is_none());
    states.push(state("list"));
    let ((target, certificate), sync_work) = observed(&runtime, &primary, || {
        backend
            .with_mutation_service(|service| {
                let target = service
                    .posix_capture_file_sync_target(PATH, crate::DfsPosixSyncKind::File)
                    .map_err(dfs_browser_service_error)?;
                let certificate = service
                    .posix_sync_file_target(&target)
                    .map_err(dfs_browser_service_error)?;
                Ok((target, certificate))
            })
            .unwrap()
    });
    states.push(state("synced"));
    assert_eq!(certificate.target, target);
    assert_eq!(target.object_id.as_str(), replaced.object_id);
    assert_eq!(target.version.as_str(), replaced.version);
    assert_eq!(certificate.content_evidence.required_receipts, 2);
    assert!(certificate.content_evidence.receipt_node_ids.len() >= 2);
    assert_eq!(
        certificate.metadata_certificate.target.primary_node_id,
        primary
    );
    assert_eq!(
        certificate.metadata_certificate.target.authority_epoch,
        Some(services.epoch)
    );

    // These native inspections are outside the recorded requester TLS window.
    let calls = runtime.rpc_observations();
    let calls = &calls[rpc_begin..];
    let writes = |records: &[CapturedServedMessageWork],
                  receipt: &crate::DaemonDfsBrowserStatResponse| {
        records
            .iter()
            .filter(|work| {
                let frame = &work.command.routed_message;
                frame.source_node_id == primary
                    && frame.target_node_id == primary
                    && matches!(&frame.message,
                    DfsNetworkMessage::ExclusiveCreateRequest(command)
                    | DfsNetworkMessage::DistributedWriteRequest(command)
                    if command.request.object_id.as_str() == receipt.object_id
                        && command.request.target_version.as_str() == receipt.version)
                    && completed(work, calls)
            })
            .count()
    };
    assert_eq!(writes(&create_work, &created), 1);
    assert_eq!(writes(&replace_work, &replaced), 1);
    for records in [&read_work, &stat_work, &list_work] {
        assert!(
            records.iter().any(|work| {
                let frame = &work.command.routed_message;
                frame.source_node_id == primary
                    && frame.target_node_id == primary
                    && matches!(&frame.message, DfsNetworkMessage::NamespaceRequest(command)
                    if command.file_system_id == target.file_system_id
                        && !command.include_metadata_journal_batches)
                    && completed(work, calls)
            }),
            "browser metadata reads must reach actual registered Main"
        );
    }
    assert!(sync_work.iter().any(|work| {
        let frame = &work.command.routed_message;
        frame.source_node_id == primary && frame.target_node_id == primary
            && matches!(&frame.message, DfsNetworkMessage::NamespaceRequest(command)
                if matches!(&command.operation, DfsNamespaceOperation::FileSync { path, target: actual }
                    if path == PATH && actual == &target))
            && completed(work, calls)
    }));
    let object = crate::DfsObjectId::new(&replaced.object_id).unwrap();
    let native_intents = native_observer
        .metadata_backend()
        .unwrap()
        .intents_for_object(&object)
        .unwrap();
    for receipt in [&created, &replaced] {
        assert_eq!(
            native_intents
                .iter()
                .filter(|intent| {
                    intent.status == DfsWriteIntentStatus::Committed
                        && intent.target_version.as_str() == receipt.version
                        && intent.content_hash == receipt.content_hash
                })
                .count(),
            1,
            "actual native original for each browser write"
        );
    }
    let facts = serde_json::json!({
        "services":services,"directory":directory,"created":created,"replaced":replaced,
        "target":target,"certificate":certificate,"requester_states":states,
        "object_health":object_health,"health_summary":health,
        "create_work":create_work.iter().map(|work| &work.command).collect::<Vec<_>>(),
        "replace_work":replace_work.iter().map(|work| &work.command).collect::<Vec<_>>(),
        "sync_work":sync_work.iter().map(|work| &work.command).collect::<Vec<_>>(),
        "qualification":"actual registered self Main with normal native writes; TLS counts cover synchronous requester only, not Main executor or direct native evidence; no memory/RSS or broad acceptance claim"
    });
    let export = std::fs::write(
        data.root().join("browser-no-private-native-image.json"),
        serde_json::to_vec_pretty(&facts).unwrap(),
    );
    if let Err(error) = &export {
        eprintln!("browser no-private-native evidence export failed: {error}");
    }
    for &(phase, _, delegated, opens, snapshots) in &states {
        assert!(
            delegated,
            "{phase}: requester must retain registered Main routing"
        );
        assert_eq!(
            opens, 0,
            "{phase}: requester opened a native metadata image"
        );
        assert_eq!(
            snapshots, 0,
            "{phase}: requester materialized native table snapshots"
        );
    }
    for (phase, absent, _, _, _) in states {
        assert!(
            absent,
            "{phase}: browser must not retain a private native metadata owner"
        );
    }
    export.expect("native no-private-image evidence retained after semantic assertions");
    drop(native_observer);
    drop(backend);
    drop(host);
    runtime.close().unwrap();
}

/// Existing-parent writes send exactly one path command. Both W receipts and
/// the file bytes must be visible after the caller returns.
#[test]
fn browser_write_beneath_existing_parent_routes_one_owner_command() {
    use crate::{DfsNamespaceOperation, DfsNetworkMessage};

    const ROOT: &str = "/services/browser-write-round-trips";
    const FIRST: &str = "/services/browser-write-round-trips/a/b/first.bin";
    const SECOND: &str = "/services/browser-write-round-trips/a/b/second.bin";
    let (runtime, nodes) = native_browser_runtime("native-browser-write-round-trips", 3);
    let services = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let primary = services.primary_node_id.clone();
    native_browser_policy(
        &runtime.shared_control_plane().unwrap(),
        ROOT,
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 2, 2).unwrap(),
    );
    let data = Arc::new(runtime.data_dir(&primary).clone());
    let handle = runtime.runtime_handle();
    let host = dfs_browser_component_host(&data).unwrap();
    handle.block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            host.ensure_write_configuration_ready(&data, &handle),
        )
        .await
        .unwrap()
        .unwrap();
    });
    let backend = DfsBrowserBackend::for_component_data_dir(Arc::clone(&data), &handle).unwrap();
    assert!(
        backend
            .lock_service()
            .unwrap()
            .transport
            .delegates_local_namespace_authority()
    );
    let write = |path: &str, bytes: &[u8]| {
        backend
            .write_file(DaemonDfsBrowserWriteFileRequest {
                path: path.into(),
                bytes: bytes.to_vec(),
            })
            .unwrap();
    };
    // The first write creates the missing parent chain.
    write(FIRST, b"first original");

    let served = |action: &dyn Fn()| {
        let captures = nodes
            .iter()
            .map(|node| runtime.capture_served_work(node, 32).unwrap())
            .collect::<Vec<_>>();
        action();
        let mut operations = Vec::new();
        let mut writes = 0usize;
        for capture in &captures {
            for work in capture.records() {
                match work.command.routed_message.message {
                    DfsNetworkMessage::NamespaceRequest(command) => {
                        operations.push(command.operation)
                    }
                    DfsNetworkMessage::DistributedWriteRequest(_)
                    | DfsNetworkMessage::ExclusiveCreateRequest(_) => writes += 1,
                    _ => {}
                }
            }
        }
        (operations, writes)
    };
    for (label, path, bytes) in [
        ("overwrite", FIRST, b"first replacement".as_slice()),
        ("create", SECOND, b"second original".as_slice()),
    ] {
        let (operations, writes) = served(&|| write(path, bytes));
        assert_eq!(writes, 0, "{label}: unexpected second write dispatch");
        assert!(
            matches!(operations.as_slice(),
            [DfsNamespaceOperation::WriteFile { path: sent, bytes: crate::protocol::DfsOwnerWriteContent::Inline(body), expected: None }]
            if sent == path && body.as_ref() == bytes),
            "{label}: served {operations:?}"
        );
        let mut stored = backend.lock_service().unwrap();
        assert_eq!(stored.posix_read_file_distributed(path).unwrap(), bytes);
        let stat = stored.posix_stat_distributed(path).unwrap();
        assert_eq!(
            stat.current_locations.len(),
            2,
            "{label}: the commit publishes both W receipts: {stat:?}"
        );
    }
    drop(backend);
    drop(host);
    runtime.close().unwrap();
}

#[test]
fn browser_parent_exact_hint_does_not_recreate_a_confirmed_deleted_parent() {
    browser_parent_hint_deletion_control(false);
}

#[test]
fn browser_parent_hint_survives_actual_backend_catchup_without_recreating_parent() {
    browser_parent_hint_deletion_control(true);
}

fn browser_parent_hint_deletion_control(hint_before_deletion: bool) {
    let local = node(if hint_before_deletion {
        "browser_parent_hint_before_deletion"
    } else {
        "browser_parent_confirmed_tombstone"
    });
    let (runtime, mut browser) = native_browser_service(
        if hint_before_deletion { "browser-parent-hint-before-deletion" }
        else { "browser-parent-confirmed-tombstone" }, local.clone(),
    );
    let parent = "/services/browser-parent-confirmed-tombstone/parent";
    let child = format!("{parent}/child");
    browser.posix_mkdir_all(parent).unwrap();
    let old_parent = {
        let owner = runtime.metadata_primary_service(parent).unwrap();
        owner.metadata_backend().unwrap()
            .get_object_by_path(&sifs_file_system_id(), parent).unwrap().unwrap()
    };
    assert_eq!(old_parent.kind, DfsObjectKind::Directory);
    // A browser delegating to its Main keeps no store, so it never catches
    // up from a backend: an observed row stays a transient advisory overlay.
    assert!(browser.metadata_backend().is_none());
    if hint_before_deletion {
        browser.add_transient_object_metadata(old_parent.clone()).unwrap();
        assert!(browser.transient_metadata_object_ids.contains(&old_parent.object_id),
            "a store-less browser keeps the genuinely current row only as an advisory overlay");
    }
    let mut remover = runtime.client(&local).unwrap();
    remover.posix_rmdir(parent).unwrap();
    drop(remover);
    let deleted_parent = {
        let owner = runtime.metadata_primary_service(parent).unwrap();
        assert!(matches!(owner.posix_stat(parent), Err(DfsPosixError::NotFound { path }) if path == parent));
        let backend = owner.metadata_backend().unwrap();
        backend.get_object(&old_parent.object_id).unwrap().unwrap()
    };
    assert_eq!(deleted_parent.kind, DfsObjectKind::Tombstone);
    assert_ne!(deleted_parent.version, old_parent.version);
    // The sole private hint is the genuinely observed old row. It grants no
    // authority and does not recreate its ancestors or overwrite the tombstone.
    if !hint_before_deletion {
        browser.add_transient_object_metadata(old_parent.clone()).unwrap();
    }
    assert_eq!(browser.metadata().object(&old_parent.object_id), Some(&old_parent));
    let capture = runtime.capture_served_work(&local, 32).unwrap();
    prepare_sifs_browser_write_parent(&mut browser, &child).unwrap();
    let reports = capture.records();
    let implicit_creates = reports.iter().filter(|work| matches!(
        &work.command.routed_message.message,
        DfsNetworkMessage::NamespaceRequest(request)
            if matches!(&request.operation, DfsNamespaceOperation::MkdirAll { path } if path == parent)
    )).count();
    let after = {
        let owner = runtime.metadata_primary_service(parent).unwrap();
        let backend = owner.metadata_backend().unwrap();
        assert!(backend.get_object_by_path(&sifs_file_system_id(), &child).unwrap().is_none());
        backend.get_object(&old_parent.object_id).unwrap().unwrap()
    };
    std::fs::write(
        runtime.data_dir(&local).root().join("boundary-diagnostic-cached-parent-preparation.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "old_hint": old_parent, "deleted_parent": deleted_parent, "after_preparation": after,
            "hint_before_deletion": hint_before_deletion,
            "implicit_mkdir_all_calls": implicit_creates,
            "actual_invocations": reports.iter().map(|work| serde_json::json!({
                "command": work.command.routed_message,
                "publish_metadata": work.committed_work.publish_metadata,
                "semantic_changes": work.committed_work.semantic_changes,
                "succeeded": work.succeeded,
            })).collect::<Vec<_>>(),
            "qualification": "Actual registered Main created and removed the parent. The copied old row is advisory DATA only; no fabricated receipt or supplied successful response."
        })).unwrap(),
    ).unwrap();
    drop(capture);
    drop(browser);
    runtime.close().unwrap();
    assert_eq!(implicit_creates, 0, "an old positive hint cannot authorize implicit recreation");
    assert_eq!(after, deleted_parent, "preparation must preserve the actual tombstone exactly");
}

#[test]
fn browser_policy_projection_preserves_links_dot_paths_and_namespace_errors() {
    let local = node("browser_policy_paths");
    let (mut runtime, mut dfs) = native_browser_service(
        "browser_policy_projection_preserves_links_dot_paths_and_namespace_errors",
        local.clone(),
    );
    dfs.posix_mkdir_all("/documents/dir").expect("directory");
    let options = DfsWriteOptions::replicated_eventual(vec![local]);
    let file = dfs
        .posix_write_file("/documents/dir/file", b"body", options.clone())
        .expect("file");
    dfs.posix_hardlink("/documents/dir/file", "/documents/hard")
        .expect("hardlink");
    dfs.posix_symlink("dir/file", "/documents/link", options.clone())
        .expect("symlink");
    dfs.posix_symlink("link", "/documents/chain", options.clone())
        .expect("symlink chain");
    dfs.posix_symlink("dir", "/documents/dir-link", options.clone())
        .expect("directory symlink");
    dfs.posix_symlink("missing", "/documents/dangling", options)
        .expect("dangling symlink");
    // The metadata hint resolves a local metadata view. A browser delegating
    // to its Main has none and fails closed; the projection contract belongs
    // to the store-backed owner.
    assert!(matches!(
        dfs.posix_object_metadata_hint("/documents/dir/file"),
        Err(DfsPosixError::Dfs(crate::DfsError::Distributed { .. }))
    ));
    drop(dfs);
    let mut dfs = runtime.metadata_primary_service("/documents").unwrap();
    let mut object = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&file.object_id)
        .unwrap()
        .unwrap();
    object.policy = sinomo_core::DfsObjectPolicy::ReplicatedConfirmed;
    dfs.add_object_metadata(object)
        .expect("stronger durable policy");
    for path in [
        "/documents/dir/file",
        "/documents/hard",
        "/documents/link",
        "/documents/chain",
        "/documents/dir-link/file",
        "/documents/dir/../link",
        "/documents/dir/./file",
        "/documents/dir/",
        "/documents/missing",
        "/documents/dangling",
        "/documents/dir/file/child",
        "/documents/dir/file/",
    ] {
        assert_eq!(
            dfs.posix_object_metadata_hint(path)
                .map(|object| (object.object_id, object.kind, object.policy))
                .map_err(|e| e.to_string()),
            dfs.posix_stat(path)
                .map(|stat| (stat.object_id, stat.kind, stat.policy))
                .map_err(|e| e.to_string()),
            "metadata hint differs for {path}",
        );
    }
    assert_eq!(
        dfs.posix_object_metadata_hint("/documents/hard")
            .unwrap()
            .policy,
        sinomo_core::DfsObjectPolicy::ReplicatedConfirmed
    );
    // A real bounded rename publishes its old-root tombstone before moving
    // descendants. This exercises a hidden child without authoring an invalid
    // directory tombstone or inventing a maintained parent count.
    let owner = dfs.node_id().clone();
    drop(dfs);
    let mut caller = runtime.client(&owner).unwrap();
    let job = caller.posix_rename_directory_bounded_routed(
        "/documents/dir", "/documents/moved", 1,
    ).expect("publish original directory move");
    assert_eq!(job.status, crate::DfsNamespaceJobStatus::Running);
    drop(caller);
    let dfs = runtime.metadata_primary_service("/documents").unwrap();
    assert!(dfs.metadata_backend().unwrap()
        .get_object(&file.object_id).unwrap().is_some(), "the original child DATA remains present");
    for path in [
        "/documents/dir/file",
        "/documents/link",
        "/documents/dir-link/file",
    ] {
        assert!(dfs.posix_stat(path).is_err(), "fixture must hide {path}");
        assert_eq!(
            dfs.posix_object_metadata_hint(path)
                .map(|object| (object.object_id, object.kind, object.policy))
                .map_err(|e| e.to_string()),
            dfs.posix_stat(path)
                .map(|stat| (stat.object_id, stat.kind, stat.policy))
                .map_err(|e| e.to_string()),
        );
    }
    drop(dfs);
    let mut caller = runtime.client(&owner).unwrap();
    let completed = caller.posix_resume_namespace_job_bounded_routed(
        &job.job_id, &job.root_path, 128,
    ).expect("complete the original directory move");
    assert_eq!(completed.status, crate::DfsNamespaceJobStatus::Completed);
    drop(caller);
    runtime.close().unwrap();
}

#[test]
fn browser_parent_hints_skip_directory_counts_and_preserve_namespace_checks() {
    let local = node("browser_parent_hints");
    let (mut runtime, mut dfs) = native_browser_service(
        "browser_parent_hints_skip_directory_counts_and_preserve_namespace_checks",
        local.clone(),
    );
    dfs.posix_mkdir_all("/documents/dir/child").unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![local]);
    dfs.posix_write_file("/documents/dir/file", b"body", options.clone())
        .unwrap();
    dfs.posix_symlink("dir", "/documents/alias", options.clone())
        .unwrap();
    dfs.posix_symlink("missing", "/documents/dangling", options)
        .unwrap();
    // Parent hints resolve a local metadata view, which only the store-backed
    // owner has; a delegating browser fails closed and routes MkdirAll.
    drop(dfs);
    let mut dfs = runtime.metadata_primary_service("/documents").unwrap();

    crate::posix::reset_posix_directory_link_count_calls();
    assert_eq!(dfs.posix_stat("/documents/dir").unwrap().link_count, 3);
    assert_eq!(crate::posix::posix_directory_link_count_calls(), 0);
    for parent in ["/documents/dir", "/documents/alias"] {
        assert_eq!(
            dfs.posix_object_metadata_hint(parent).unwrap().kind,
            DfsObjectKind::Directory,
        );
        prepare_sifs_browser_write_parent(&mut dfs, &format!("{parent}/new-file")).unwrap();
    }
    assert_eq!(
        dfs.posix_object_metadata_hint("/documents/dir/file")
            .unwrap()
            .kind,
        DfsObjectKind::File,
    );
    assert!(matches!(
        prepare_sifs_browser_write_parent(&mut dfs, "/documents/dir/file/new-file"),
        Err(DfsPosixError::NotDirectory { .. }),
    ));
    assert!(matches!(
        dfs.posix_object_metadata_hint("/documents/dangling"),
        Err(DfsPosixError::NotFound { .. }),
    ));
    let registry = dfs.cross_shard_namespace_lock_registry();
    for locked_path in ["/documents/alias", "/documents/dir"] {
        registry.upsert_job(
            None,
            "parent-hint-flight".into(),
            sifs_file_system_id(),
            locked_path.to_owned(),
        );
        assert!(dfs.posix_object_metadata_hint("/documents/alias").is_err());
        registry.remove_job("parent-hint-flight");
    }
    assert_eq!(crate::posix::posix_directory_link_count_calls(), 0);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn browser_policy_projection_preserves_requested_and_resolved_flight_fences() {
    let local = node("browser_policy_fences");
    let (mut runtime, mut dfs) = native_browser_service(
        "browser_policy_projection_preserves_requested_and_resolved_flight_fences",
        local.clone(),
    );
    dfs.posix_mkdir_all("/documents/target").unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![local]);
    dfs.posix_write_file("/documents/target/file", b"body", options.clone())
        .unwrap();
    dfs.posix_symlink("target/file", "/documents/link", options)
        .unwrap();
    // Flight fences on the local projection apply to the store-backed owner;
    // a delegating browser has no local view and fails closed.
    drop(dfs);
    let dfs = runtime.metadata_primary_service("/documents").unwrap();
    let registry = dfs.cross_shard_namespace_lock_registry();
    for locked_path in ["/documents/link", "/documents/target"] {
        registry.upsert_job(
            None,
            "policy-flight".into(),
            sifs_file_system_id(),
            locked_path.to_owned(),
        );
        assert!(dfs.posix_stat("/documents/link").is_err());
        assert_eq!(
            dfs.posix_object_metadata_hint("/documents/link")
                .map(|object| (object.kind, object.policy))
                .map_err(|e| e.to_string()),
            dfs.posix_stat("/documents/link")
                .map(|stat| (stat.kind, stat.policy))
                .map_err(|e| e.to_string()),
        );
        registry.remove_job("policy-flight");
    }
    assert!(dfs.posix_object_metadata_hint("/documents/link").is_ok());
    drop(dfs);
    runtime.close().unwrap();
}

pub(super) fn native_browser_runtime(
    name: &str,
    count: usize,
) -> (
    crate::test_support::DfsTestRuntime,
    Vec<sinomo_core::node::NodeId>,
) {
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(name, count);
    let mut runtime = crate::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    (runtime, nodes)
}

pub(super) fn native_browser_policy(
    control: &crate::SharedDfsControlPlaneKvClient<Box<dyn crate::DfsControlPlaneKvClient>>,
    path: &str,
    policy: crate::DfsPlacementPolicy,
) {
    control.with_client(|kv| {
        let mut store = DfsControlPlaneStore::new(kv.as_mut());
        let expected = store
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        let mut desired = expected.clone();
        crate::upsert_placement_rule(
            &mut desired.subtree_policies,
            crate::normalize_placement_rule(path, DfsPlacementScope::Subtree, policy).unwrap(),
        );
        assert!(
            store
                .compare_and_put_file_system_config(Some(&expected), &desired)
                .unwrap()
        );
    });
}

fn native_browser_observed_zones(
    service: &NetworkedDfsService<crate::DfsNodeTransport>,
) -> std::collections::BTreeMap<sinomo_core::node::NodeId, String> {
    // Read the normal publisher's retained observation DATA. No test writes
    // health, capacity, zone, or eligibility into the native control plane.
    service
        .control_plane
        .as_deref()
        .unwrap()
        .write_configuration_projection()
        .unwrap()
        .unwrap()
        .content_advertisements()
        .unwrap()
        .into_iter()
        .filter_map(|advertisement| advertisement.zone.map(|zone| (advertisement.node_id, zone)))
        .collect()
}

#[test]
fn browser_options_preserve_policy_and_candidates_without_metadata_advertisement_reads() {
    let (runtime, nodes) = native_browser_runtime("native-browser-policy-and-candidates", 3);
    let candidates = vec![nodes[2].clone(), nodes[0].clone()];
    for (consistency, minimum, zones) in [
        (sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 0),
        (sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2, 2),
    ] {
        let mut policy = DfsPlacementPolicy::new(consistency, minimum, 2).unwrap();
        policy.required_node_ids = vec![nodes[1].clone()];
        policy.forbidden_node_ids = vec![nodes[2].clone()];
        policy.min_distinct_zones = zones;
        native_browser_policy(
            &runtime.shared_control_plane().unwrap(),
            "/documents",
            policy,
        );
        let (mut service, probe) = runtime.client_with_control_plane_probe(&nodes[0]).unwrap();
        let observed_zones = native_browser_observed_zones(&service);
        let actual =
            sifs_browser_write_options(&mut service, "/documents/new", &candidates).unwrap();
        assert_eq!(
            (
                actual.policy.min_commit_replicas,
                actual.policy.desired_replicas
            ),
            (minimum, 2)
        );
        assert_eq!(actual.policy.required_node_ids, vec![nodes[1].clone()]);
        assert_eq!(actual.policy.forbidden_node_ids, vec![nodes[2].clone()]);
        assert_eq!(actual.policy.min_distinct_zones, zones);
        assert_eq!(actual.policy.node_zones, observed_zones);
        assert_eq!(
            &actual.replica_candidates[..candidates.len()],
            candidates.as_slice()
        );
        assert!(actual.replica_candidates.contains(&nodes[1]));
        // A store-less browser routes its lookups; each routed response is
        // checked against the shard's current committed assignment only.
        assert_only_shard_assignment_point_reads(&probe);
        assert_eq!(probe.prefix_read_calls(), 0);
        assert_eq!(probe.txn_calls(), 0);
        let selected = [nodes[0].clone(), nodes[1].clone()];
        if zones == 0 {
            crate::validate_replication_plan(&selected, &actual.policy).unwrap();
        } else {
            assert!(
                crate::validate_replication_plan(&selected, &actual.policy).is_err(),
                "same-process native observations cannot supply independently configured zones"
            );
        }
        assert!(
            crate::validate_replication_plan(&[nodes[0].clone(), nodes[2].clone()], &actual.policy)
                .is_err()
        );
    }
    runtime.close().unwrap();
}

#[test]
fn browser_policy_zone_data_keeps_complete_map_and_rejects_unknown_or_shared_zones() {
    // Pure policy DATA exercises multiple configured zones independently of
    // same-process native ownership. It publishes no advertisement or ACK.
    let nodes = [
        node("zone-data-a"),
        node("zone-data-b"),
        node("zone-data-c"),
    ];
    let zones = std::collections::BTreeMap::from([
        (nodes[0].clone(), "east".to_owned()),
        (nodes[1].clone(), "west".to_owned()),
        (nodes[2].clone(), "central".to_owned()),
    ]);
    for minimum_zones in [0, 2] {
        let mut policy =
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
                .unwrap();
        policy.required_node_ids = vec![nodes[1].clone()];
        policy.forbidden_node_ids = vec![nodes[2].clone()];
        policy.min_distinct_zones = minimum_zones;
        let replication = policy.replication_policy(zones.clone()).unwrap();
        assert_eq!(replication.node_zones, zones);
        crate::validate_replication_plan(&[nodes[0].clone(), nodes[1].clone()], &replication)
            .unwrap();
        assert!(
            crate::validate_replication_plan(&[nodes[0].clone(), nodes[2].clone()], &replication)
                .is_err()
        );
    }
    let mut policy =
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.min_distinct_zones = 2;
    for observed in [
        std::collections::BTreeMap::new(),
        std::collections::BTreeMap::from([
            (nodes[0].clone(), "shared".to_owned()),
            (nodes[1].clone(), "shared".to_owned()),
        ]),
    ] {
        let replication = policy.replication_policy(observed).unwrap();
        assert!(
            crate::validate_replication_plan(&[nodes[0].clone(), nodes[1].clone()], &replication)
                .is_err()
        );
    }
}

#[test]
fn browser_options_keep_confirmed_and_missing_path_defaults() {
    let (runtime, nodes) = native_browser_runtime("native-browser-confirmed-defaults", 2);
    native_browser_policy(
        &runtime.shared_control_plane().unwrap(),
        "/documents",
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap(),
    );
    let mut service = runtime.client(&nodes[0]).unwrap();
    service.posix_mkdir_all("/documents").unwrap();
    let options = sifs_browser_write_options(&mut service, "/documents/file", &nodes).unwrap();
    service
        .posix_write_file("/documents/file", b"actual confirmed bytes", options)
        .unwrap();
    assert_eq!(
        service
            .posix_stat_distributed("/documents/file")
            .unwrap()
            .policy,
        sinomo_core::DfsObjectPolicy::ReplicatedConfirmed
    );
    for path in ["/documents/file", "/documents/missing"] {
        let actual = sifs_browser_write_options(&mut service, path, &nodes).unwrap();
        assert_eq!(
            (
                actual.policy.min_commit_replicas,
                actual.policy.desired_replicas
            ),
            (2, 2)
        );
        assert!(actual.wait_for_commit);
    }
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn browser_options_preserve_local_one_replica_and_reject_local_constraints() {
    let (runtime, nodes) = native_browser_runtime("native-browser-local-policy", 2);
    let mut service = runtime.client(&nodes[0]).unwrap();
    let path = crate::sifs_node_files_path(&nodes[0]);
    assert_eq!(
        service.posix_stat_distributed(&path).unwrap().policy,
        sinomo_core::DfsObjectPolicy::LocalOwned
    );
    let observed_zones = native_browser_observed_zones(&service);
    let actual = sifs_browser_write_options(&mut service, &path, &nodes).unwrap();
    assert_eq!(
        (
            actual.policy.min_commit_replicas,
            actual.policy.desired_replicas
        ),
        (1, 1)
    );
    assert_eq!(actual.policy.min_distinct_zones, 0);
    assert_eq!(actual.policy.node_zones, observed_zones);
    for constraint in ["required", "forbidden", "zones"] {
        let mut policy =
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::LocalOwned, 1, 1).unwrap();
        match constraint {
            "required" => policy.required_node_ids = vec![nodes[0].clone()],
            "forbidden" => policy.forbidden_node_ids = vec![nodes[1].clone()],
            _ => policy.min_distinct_zones = 1,
        }
        assert!(
            crate::normalize_placement_rule(&path, DfsPlacementScope::Subtree, policy).is_err(),
            "normal current policy admission rejects {constraint} before publishing invalid local rules"
        );
    }
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn browser_options_replay_committed_operation_after_actual_reopen_without_changing_zone_policy() {
    let (mut runtime, nodes) = native_browser_runtime("native-browser-stable-policy-reopen", 1);
    let path = "/documents/stable-operation";
    let operation = "native-browser-stable-operation";
    let mut service = runtime.client(&nodes[0]).unwrap();
    service.posix_mkdir_all("/documents").unwrap();
    let options = sifs_browser_write_options(&mut service, path, &nodes).unwrap();
    let created = service
        .posix_create_file_with_operation_id(path, b"exact bytes", options, operation)
        .unwrap();
    let intent_id = crate::intent::stable_write_intent_id(&nodes[0], &created.object_id, operation);
    // The node's durable store; the delegating browser keeps no store.
    let original = runtime
        .node_metadata(&nodes[0])
        .unwrap()
        .get_intent(&created.object_id, &intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(original.status, DfsWriteIntentStatus::Committed);
    drop(service);
    runtime.stop_node(&nodes[0]).unwrap();
    runtime.reopen_node(nodes[0].clone()).unwrap();
    let mut service = runtime.client(&nodes[0]).unwrap();
    let options = sifs_browser_write_options(&mut service, path, &nodes).unwrap();
    let replay = service
        .posix_create_file_with_operation_id(path, b"exact bytes", options.clone(), operation)
        .unwrap();
    assert_eq!(
        (replay.object_id, replay.version),
        (created.object_id.clone(), created.version)
    );
    assert_eq!(
        runtime
            .node_metadata(&nodes[0])
            .unwrap()
            .get_intent(&created.object_id, &intent_id)
            .unwrap()
            .unwrap(),
        original
    );
    assert!(
        service
            .posix_create_file_with_operation_id(path, b"changed", options, operation)
            .is_err()
    );
    assert_eq!(
        service.posix_read_file_distributed(path).unwrap(),
        b"exact bytes"
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn write_configuration_browser_warm_options_and_committed_writes_keep_full_zero_minimum_zones() {
    // Separate normal fixtures preserve the shared runtime's fixed roster.
    // The Docker formation gate exercises sequential membership growth.
    for (name, node_count) in [
        ("native-browser-warm-solo-configuration", 1),
        ("native-browser-warm-write-configuration", 3),
    ] {
        let (runtime, nodes) = native_browser_runtime(name, node_count);
        let path = "/services/native-write";
        let owner = runtime
            .metadata_primary_service(path)
            .unwrap()
            .node_id()
            .clone();
        // Warm-policy assertions require the actual published content roster,
        // not merely mounted metadata routes. Observe normal startup before
        // creating the zero-KV-read probe; do not inject candidate defaults.
        let readiness = runtime.client(&owner).unwrap();
        let wanted = nodes
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let ready_started = std::time::Instant::now();
        let ready_deadline = ready_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            let projection = readiness
                .control_plane
                .as_deref()
                .unwrap()
                .write_configuration_projection()
                .unwrap();
            if projection.as_ref().is_some_and(|projection| {
                projection
                    .content_replica_candidates()
                    .unwrap()
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    == wanted
            }) {
                break;
            }
            assert!(
                std::time::Instant::now() < ready_deadline,
                "normal content roster did not publish before the existing readiness budget"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        eprintln!(
            "NATIVE_BROWSER_ROSTER_READY nodes={} elapsed_ms={}",
            nodes.len(),
            ready_started.elapsed().as_millis()
        );
        drop(readiness);
        let backend = DfsBrowserBackend::for_component_data_dir(
            Arc::new(runtime.data_dir(&owner).clone()),
            &runtime.runtime_handle(),
        )
        .unwrap();
        let native_observer = runtime.metadata_primary_service(path).unwrap();
        let (mut service, probe) = runtime.client_with_control_plane_probe(&owner).unwrap();
        let implicit = service
            .control_plane
            .as_deref()
            .unwrap()
            .write_configuration_projection()
            .unwrap()
            .unwrap();
        let config = implicit
            .file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        assert!(config.subtree_policies.iter().all(|rule| {
            rule.path_prefix != crate::SIFS_USERS_PATH
                && rule.path_prefix != crate::SIFS_SERVICES_PATH
        }));
        let candidates = implicit.content_replica_candidates().unwrap();
        assert_eq!(
            candidates.iter().collect::<std::collections::BTreeSet<_>>(),
            nodes.iter().collect::<std::collections::BTreeSet<_>>()
        );
        let implicit_options = sifs_browser_write_options_with_configuration(
            &mut service,
            path,
            &candidates,
            Some(&implicit),
        )
        .unwrap();
        assert_eq!(
            (
                implicit_options.policy.min_commit_replicas,
                implicit_options.policy.desired_replicas
            ),
            (1, node_count.min(2))
        );
        let initial_bytes = vec![0x44; 4096];
        backend
            .write_file(DaemonDfsBrowserWriteFileRequest {
                path: path.into(),
                bytes: initial_bytes.clone(),
            })
            .unwrap();
        {
            let mut stored = backend.lock_service().unwrap();
            let stat = stored.posix_stat_distributed(path).unwrap();
            assert_eq!(
                stored.posix_read_file_distributed(path).unwrap(),
                initial_bytes
            );
            let intents = native_observer
                .metadata_backend()
                .unwrap()
                .intents_for_object_version_page(&stat.object_id, &stat.version, None, 16)
                .unwrap();
            let actual = intents
                .intents
                .iter()
                .find(|intent| {
                    intent.status == DfsWriteIntentStatus::Committed
                        && intent.publication.is_current()
                })
                .unwrap();
            assert_eq!(actual.file_system_id, stat.file_system_id);
            assert_eq!(actual.object_id, stat.object_id);
            assert_eq!(actual.target_version, stat.version);
            assert_eq!(actual.content_hash, stat.content_hash);
            assert_eq!(actual.policy, implicit_options.policy);
            assert_eq!(actual.desired_replica_node_ids.len(), node_count.min(2));
        }
        // A store-less browser routes its lookups; each routed response is
        // checked against the shard's current committed assignment only.
        assert_only_shard_assignment_point_reads(&probe);
        assert_eq!(probe.prefix_read_calls(), 0);
        assert_eq!(probe.txn_calls(), 0);
        drop(service);
        let mut policy =
            DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1)
                .unwrap();
        policy.required_node_ids = vec![owner.clone()];
        let expected_rule = crate::normalize_placement_rule(
            "/services",
            DfsPlacementScope::Subtree,
            policy.clone(),
        )
        .unwrap();
        runtime
            .apply_placement_policy(&owner, "/services", DfsPlacementScope::Subtree, policy)
            .unwrap();
        let applied = runtime
            .shared_control_plane()
            .unwrap()
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .get_file_system_config(&sifs_file_system_id())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            applied
                .subtree_policies
                .iter()
                .filter(|rule| {
                    rule.path_prefix == expected_rule.path_prefix
                        && rule.scope == expected_rule.scope
                })
                .collect::<Vec<_>>(),
            vec![&expected_rule]
        );
        // Open the next caller after publication, as the normal Apply API specifies.
        // The earlier held implicit view remains separate, unchanged DATA.
        let (mut service, probe) = runtime.client_with_control_plane_probe(&owner).unwrap();
        let observed_zones = native_browser_observed_zones(&service);
        let expected = sifs_browser_write_options(&mut service, path, &candidates).unwrap();
        assert_eq!(
            (
                expected.policy.min_commit_replicas,
                expected.policy.desired_replicas
            ),
            (1, 1)
        );
        assert_eq!(expected.policy.required_node_ids, vec![owner]);
        assert_eq!(
            sifs_browser_write_options_with_configuration(
                &mut service,
                path,
                &candidates,
                Some(&implicit),
            )
            .unwrap(),
            implicit_options
        );
        for _ in 0..3 {
            assert_eq!(
                sifs_browser_write_options(&mut service, path, &candidates).unwrap(),
                expected
            );
        }
        // A store-less browser routes its lookups; each routed response is
        // checked against the shard's current committed assignment only.
        assert_only_shard_assignment_point_reads(&probe);
        assert_eq!(probe.prefix_read_calls(), 0);
        assert_eq!(probe.txn_calls(), 0);
        assert_eq!(expected.policy.min_distinct_zones, 0);
        assert_eq!(expected.policy.node_zones, observed_zones);
        let mut versions = Vec::new();
        for bytes in [vec![0x11; 4096], vec![0x99; 4096]] {
            backend
                .write_file(DaemonDfsBrowserWriteFileRequest {
                    path: path.into(),
                    bytes: bytes.clone(),
                })
                .unwrap();
            let mut stored = backend.lock_service().unwrap();
            let stat = stored.posix_stat_distributed(path).unwrap();
            assert_eq!(stored.posix_read_file_distributed(path).unwrap(), bytes);
            let intents = native_observer
                .metadata_backend()
                .unwrap()
                .intents_for_object_version_page(&stat.object_id, &stat.version, None, 16)
                .unwrap();
            let actual = intents
                .intents
                .iter()
                .find(|intent| intent.status == DfsWriteIntentStatus::Committed)
                .unwrap();
            assert_eq!(actual.policy, expected.policy);
            versions.push(stat.version);
        }
        assert_ne!(versions[0], versions[1]);
        drop(service);
        drop(native_observer);
        drop(backend);
        runtime.close().unwrap();
    }
}

#[test]
fn write_configuration_browser_coherent_updates_keep_retained_view_and_current_policy_separate() {
    let (runtime, nodes) = native_browser_runtime("native-browser-coherent-current-policy", 3);
    native_browser_policy(
        &runtime.shared_control_plane().unwrap(),
        "/documents",
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
    );
    let mut service = runtime.client(&nodes[0]).unwrap();
    let old = service
        .control_plane
        .as_deref()
        .unwrap()
        .write_configuration_projection()
        .unwrap()
        .unwrap();
    let old_candidates = old.content_replica_candidates().unwrap();
    let old_options = sifs_browser_write_options_with_configuration(
        &mut service,
        "/documents/coherent",
        &old_candidates,
        Some(&old),
    )
    .unwrap();
    let mut policy =
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![nodes[0].clone()];
    policy.forbidden_node_ids = vec![nodes[2].clone()];
    policy.min_distinct_zones = 2;
    native_browser_policy(
        &runtime.shared_control_plane().unwrap(),
        "/documents",
        policy,
    );
    assert_eq!(
        sifs_browser_write_options_with_configuration(
            &mut service,
            "/documents/coherent",
            &old_candidates,
            Some(&old)
        )
        .unwrap(),
        old_options
    );
    let (mut current, probe) = runtime.client_with_control_plane_probe(&nodes[0]).unwrap();
    let view = current
        .control_plane
        .as_deref()
        .unwrap()
        .write_configuration_projection()
        .unwrap()
        .unwrap();
    assert!(view.revision() > old.revision());
    let candidates = view.content_replica_candidates().unwrap();
    let observed_zones = native_browser_observed_zones(&current);
    let actual =
        sifs_browser_write_options(&mut current, "/documents/coherent", &candidates).unwrap();
    assert_eq!(
        (
            actual.policy.min_commit_replicas,
            actual.policy.desired_replicas,
            actual.policy.min_distinct_zones
        ),
        (1, 2, 2)
    );
    assert_eq!(actual.policy.forbidden_node_ids, vec![nodes[2].clone()]);
    assert_eq!(actual.policy.node_zones, observed_zones);
    assert!(
        crate::validate_replication_plan(&[nodes[0].clone(), nodes[1].clone()], &actual.policy)
            .is_err(),
        "retained unknown or shared native zones cannot satisfy a two-zone rule"
    );
    assert_ne!(actual.policy, old_options.policy);
    // A store-less browser routes its lookups; each routed response is
    // checked against the shard's current committed assignment only.
    assert_only_shard_assignment_point_reads(&probe);
    assert_eq!(probe.prefix_read_calls(), 0);
    drop(current);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn write_configuration_browser_pending_preserves_payload_without_parent_intent_or_journal_effects()
{
    let (runtime, nodes) = native_browser_runtime("native-browser-configuration-gap", 1);
    let backend = DfsBrowserBackend::for_component_data_dir(
        Arc::new(runtime.data_dir(&nodes[0]).clone()),
        &runtime.runtime_handle(),
    )
    .unwrap();
    let native_observer = runtime.metadata_primary_service("/documents").unwrap();
    let key = crate::file_system_config_key(&sifs_file_system_id());
    let context = runtime.component_context(&nodes[0]).unwrap();
    let owner = crate::runtime_host::retain_component_data_owner(&context).unwrap();
    assert!(
        crate::runtime_host::registered_component_cluster(&context)
            .unwrap()
            .is_some()
    );
    // Configuration publication uses the normal native KV client independently
    // of the DFS metadata projection whose real repair read is paused below.
    let control = crate::SharedDfsControlPlaneKvClient::new(Box::new(
        crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &ClusterPaths::new(context.data_dir()),
            context.runtime().unwrap(),
        ),
    )
        as Box<dyn crate::DfsControlPlaneKvClient>);
    let gap = runtime
        .lose_configuration_delta_and_pause_rebuild(&nodes[0], key.clone())
        .unwrap();
    let first =
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    native_browser_policy(&control, "/documents", first.clone());
    let mut second = first;
    second.required_node_ids = vec![nodes[0].clone()];
    native_browser_policy(&control, "/documents", second);
    let rebuilt = gap.wait_rebuild(std::time::Duration::from_secs(5));
    if rebuilt.is_err() {
        eprintln!(
            "native browser gap boundary: root={} key={key} result={rebuilt:?} lost_delta={:?}",
            runtime.data_dir(&nodes[0]).root().display(),
            gap.dropped_delta().map(|delta| (
                delta.revision,
                delta.compacted_revision,
                delta
                    .events
                    .iter()
                    .map(|event| event.key.clone())
                    .collect::<Vec<_>>(),
                delta.final_key_states.len(),
            )),
        );
    }
    rebuilt.unwrap();
    let actual_lost = gap
        .dropped_delta()
        .expect("one real committed delta was withheld");
    assert!(actual_lost.events.iter().any(|event| event.key == key));
    let parent = "/documents/configuration-handoff/deeper";
    let path = format!("{parent}/file");
    let bytes = vec![0x5a; 4096];
    let mut request = DaemonDfsBrowserWriteFileRequest {
        path: path.clone(),
        bytes: bytes.clone(),
    };
    let allocation = request.bytes.as_ptr();
    let before = native_observer
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    for _ in 0..2 {
        request = match backend.try_write_file(request).unwrap() {
            DfsBrowserWriteAttempt::ConfigurationPending(original) => original,
            DfsBrowserWriteAttempt::Complete(_) => {
                panic!("native configuration gap admitted a write")
            }
        };
        assert_eq!(request.bytes.as_ptr(), allocation);
        assert_eq!(request.path, path);
        assert_eq!(request.bytes, bytes);
        let metadata = native_observer.metadata_backend().unwrap();
        assert_eq!(metadata.journal_last_sequence().unwrap(), before);
        for absent in ["/documents/configuration-handoff", parent, path.as_str()] {
            assert!(
                metadata
                    .get_object_by_path(&sifs_file_system_id(), absent)
                    .unwrap()
                    .is_none()
            );
        }
    }
    gap.release();
    let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
        &ClusterPaths::new(runtime.data_dir(&nodes[0])),
        &sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(
            runtime.data_dir(&nodes[0]),
        ))
        .unwrap()
        .cluster_id,
        &nodes[0],
        Some(&runtime.runtime_handle()),
    )
    .unwrap();
    runtime.runtime_handle().block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            authority.ensure_write_configuration_ready_async(),
        )
        .await
        .unwrap()
        .unwrap();
    });
    assert!(matches!(
        backend.try_write_file(request).unwrap(),
        DfsBrowserWriteAttempt::Complete(_)
    ));
    let mut service = backend.lock_service().unwrap();
    assert_eq!(service.posix_read_file_distributed(&path).unwrap(), bytes);
    let stat = service.posix_stat_distributed(&path).unwrap();
    let intents = native_observer
        .metadata_backend()
        .unwrap()
        .intents_for_object_version_page(&stat.object_id, &stat.version, None, 16)
        .unwrap();
    assert_eq!(
        intents
            .intents
            .iter()
            .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
            .count(),
        1
    );
    drop(service);
    drop(native_observer);
    drop(authority);
    drop(gap);
    drop(backend);
    drop(control);
    drop(owner);
    drop(context);
    runtime.close().unwrap();
}

#[test]
fn write_configuration_browser_unavailable_native_floor_fails_before_parent_or_file_mutation() {
    let (runtime, nodes) = native_browser_runtime("native-browser-floor-return-unavailable", 1);
    let backend = DfsBrowserBackend::for_component_data_dir(
        Arc::new(runtime.data_dir(&nodes[0]).clone()),
        &runtime.runtime_handle(),
    )
    .unwrap();
    let native_observer = runtime.metadata_primary_service("/documents").unwrap();
    let before = native_observer
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let fault = runtime.deny_configuration_floor(&nodes[0]).unwrap();
    let parent = "/documents/native-floor-absent/deeper";
    let path = format!("{parent}/file");
    let error = backend
        .write_file(DaemonDfsBrowserWriteFileRequest {
            path: path.clone(),
            bytes: b"must not publish".to_vec(),
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native configuration floor is unavailable")
    );
    let metadata = native_observer.metadata_backend().unwrap();
    assert_eq!(metadata.journal_last_sequence().unwrap(), before);
    for absent in ["/documents/native-floor-absent", parent, path.as_str()] {
        assert!(
            metadata
                .get_object_by_path(&sifs_file_system_id(), absent)
                .unwrap()
                .is_none()
        );
    }
    drop(fault);
    backend
        .write_file(DaemonDfsBrowserWriteFileRequest {
            path: path.clone(),
            bytes: b"real normal recovery".to_vec(),
        })
        .unwrap();
    assert_eq!(
        backend
            .lock_service()
            .unwrap()
            .posix_read_file_distributed(&path)
            .unwrap(),
        b"real normal recovery"
    );
    drop(native_observer);
    drop(backend);
    runtime.close().unwrap();
}

#[test]
fn durable_external_edit_reconciles_exact_receipt_and_fences_stale_generations() {
    use crate::{
        DaemonDfsBrowserDurableReplaceOutcome as Outcome,
        DaemonDfsBrowserDurableReplaceRequest as Request,
    };
    let (native, nodes) = native_browser_runtime("external-edit", 1);
    let data = Arc::new(native.data_dir(&nodes[0]).clone());
    let backend =
        DfsBrowserBackend::for_component_data_dir(data, &native.runtime_handle()).unwrap();
    let path = "/services/external-edit/example.txt";
    backend
        .write_file(crate::DaemonDfsBrowserWriteFileRequest {
            path: path.into(),
            bytes: b"base".to_vec(),
        })
        .unwrap();
    let base = backend
        .stat(crate::DaemonDfsBrowserStatRequest {
            path: path.into(),
            max_staleness_millis: 0,
        })
        .unwrap()
        .into_result()
        .unwrap();
    let bytes = b"changed";
    let mut request = Request {
        expected_node_id: nodes[0].to_string(),
        path: path.into(),
        expected_object_id: base.object_id,
        expected_version: base.version,
        operation_id: "external-edit-save-1".into(),
        content_hash: format!("blake3:{}", blake3::hash(bytes).to_hex()),
        size_bytes: bytes.len() as u64,
        bytes: None,
    };
    let mut changed = false;
    let mut wrong_node = request.clone();
    wrong_node.expected_node_id = "different-node".into();
    assert!(matches!(
        backend.durable_replace(wrong_node, &mut changed).unwrap(),
        Outcome::Unknown { .. }
    ));
    assert!(matches!(
        backend
            .durable_replace(request.clone(), &mut changed)
            .unwrap(),
        Outcome::NotApplied
    ));
    assert!(!changed);
    request.bytes = Some(bytes.to_vec());
    let Outcome::Applied { receipt, .. } = backend
        .durable_replace(request.clone(), &mut changed)
        .unwrap()
    else {
        panic!("expected committed version")
    };
    assert!(changed);
    for payload in [None, Some(bytes.to_vec())] {
        changed = false;
        request.bytes = payload;
        let Outcome::Applied {
            receipt: replayed, ..
        } = backend
            .durable_replace(request.clone(), &mut changed)
            .unwrap()
        else {
            panic!("missing replay receipt")
        };
        assert_eq!(replayed, receipt);
        assert!(!changed);
    }
    request.operation_id = "external-edit-save-2".into();
    assert!(matches!(
        backend.durable_replace(request, &mut changed).unwrap(),
        Outcome::Conflict { .. }
    ));
    assert!(!changed);
}

#[test]
fn exact_log_range_checks_bounds_and_never_substitutes_a_new_head() {
    use crate::DaemonDfsBrowserReadObjectRangeRequest as Range;
    let (native, nodes) = native_browser_runtime("paged-log", 1);
    let data = Arc::new(native.data_dir(&nodes[0]).clone());
    let backend =
        DfsBrowserBackend::for_component_data_dir(data, &native.runtime_handle()).unwrap();
    let path = "/services/paging/run.log";
    let bytes = b"first\nsecond\nthird\n";
    backend
        .write_file(crate::DaemonDfsBrowserWriteFileRequest {
            path: path.into(),
            bytes: bytes.to_vec(),
        })
        .unwrap();
    let stat = backend
        .stat(crate::DaemonDfsBrowserStatRequest {
            path: path.into(),
            max_staleness_millis: 0,
        })
        .unwrap()
        .into_result()
        .unwrap();
    let range = Range {
        object_id: stat.object_id,
        version: stat.version,
        content_hash: stat.content_hash.unwrap(),
        size_bytes: bytes.len() as u64,
        offset: 6,
        length: 7,
    };
    assert_eq!(
        backend.read_object_range(range.clone()).unwrap().bytes,
        b"second\n"
    );
    let mut wrong = range.clone();
    wrong.content_hash = "blake3:wrong".into();
    assert!(backend.read_object_range(wrong).is_err());
    let mut wrong = range.clone();
    wrong.size_bytes += 1;
    assert!(backend.read_object_range(wrong).is_err());
    let mut wrong = range.clone();
    wrong.length = crate::DFS_BROWSER_RANGE_LIMIT + 1;
    assert!(backend.read_object_range(wrong).is_err());
    backend
        .write_file(crate::DaemonDfsBrowserWriteFileRequest {
            path: path.into(),
            bytes: b"replacement version".to_vec(),
        })
        .unwrap();
    if let Ok(old) = backend.read_object_range(range) {
        assert_eq!(
            old.bytes, b"second\n",
            "a retained version must still read its original bytes"
        );
    }
}

fn assert_only_shard_assignment_point_reads(probe: &crate::test_runtime::DfsTestControlPlaneProbe) {
    let keys = probe.point_read_keys();
    assert!(
        keys.iter()
            .all(|key| crate::dfs_metadata_shard_config_key_is_exact(key)),
        "browser write options read non-assignment keys: {keys:?}"
    );
}

#[test]
fn remote_owner_write_replays_initial_receipt_after_overwrite_and_refuses_changed_condition() {
    let (runtime, nodes) = native_browser_runtime("native-owner-write-replay", 3);
    let directory = "/services/owner-write-replay";
    let path = format!("{directory}/file");
    let shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let primary = shard.primary_node_id;
    let peer = nodes.iter().find(|peer| **peer != primary).unwrap();
    let mut caller = runtime.client(peer).unwrap();
    caller.posix_mkdir_all(directory).unwrap();
    let command = |bytes: &[u8], expected| crate::DfsNamespaceOperation::WriteFile {
        path: path.clone(),
        bytes: crate::protocol::DfsOwnerWriteContent::Inline(bytes::Bytes::copy_from_slice(bytes)),
        expected,
    };
    let mut dispatch = |request_id: &str, operation| {
        caller
            .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
                request_id.into(),
                crate::sifs_file_system_id(),
                primary.clone(),
                operation,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                false,
                false,
            )
            .unwrap()
    };
    let create = command(b"first", None);
    let first = dispatch("path-create", create.clone());
    let Some(crate::DfsPosixCoordinationResult::FileWritten(first_receipt)) =
        first.posix_coordination_result
    else {
        panic!("missing create receipt: {first:?}")
    };
    let expected = crate::DfsNamespaceObjectPrecondition {
        object_id: crate::DfsObjectId::new(first_receipt.object_id.clone()).unwrap(),
        version: crate::DfsVersion::new(first_receipt.version.clone()).unwrap(),
    };
    let replace = command(b"second", Some(expected.clone()));
    let second = dispatch("path-replace", replace.clone());
    assert!(matches!(second.result, crate::DfsNamespaceResult::Applied));
    assert_ne!(
        second.posix_coordination_result,
        Some(crate::DfsPosixCoordinationResult::FileWritten(
            first_receipt.clone()
        ))
    );
    assert_eq!(
        dispatch("path-create", create).posix_coordination_result,
        Some(crate::DfsPosixCoordinationResult::FileWritten(
            first_receipt
        ))
    );
    assert_eq!(
        dispatch("path-replace", replace).posix_coordination_result,
        second.posix_coordination_result
    );
    assert!(matches!(
        dispatch("path-replace", command(b"other", Some(expected.clone()))).result,
        crate::DfsNamespaceResult::Rejected { .. }
    ));
    assert!(matches!(
        dispatch("stale-base", command(b"third", Some(expected))).result,
        crate::DfsNamespaceResult::Rejected { .. }
    ));
    let payload = vec![0x83; 192 * 1024];
    let large = command(&payload, None);
    let first = dispatch("streamed-path-write", large.clone());
    assert!(
        matches!(&first.posix_coordination_result,Some(crate::DfsPosixCoordinationResult::FileWritten(r)) if r.size_bytes == Some(payload.len() as u64))
    );
    assert_eq!(
        dispatch("streamed-path-write", large).posix_coordination_result,
        first.posix_coordination_result
    );
    assert_eq!(caller.posix_read_file_distributed(&path).unwrap(), payload);
    drop(caller);
    runtime.close().unwrap();
}
