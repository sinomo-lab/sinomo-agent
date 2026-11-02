use sinomo_dfs::{
    DfsPosixSyncKind, DfsReplicationPolicy, DfsWriteOptions, test_support::DfsTestRuntime,
};

#[test]
fn native_file_sync_certifies_actual_receipts_and_rejects_another_content_hash() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-file-sync-evidence", 3);
    let mut runtime = DfsTestRuntime::new("native-file-sync-evidence", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/native-file-sync-evidence.bin";
    let mut service = runtime.client(&nodes[0]).unwrap();
    service
        .posix_write_file(
            path,
            b"actual durable bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let mut target = service
        .posix_capture_file_sync_target(path, DfsPosixSyncKind::File)
        .unwrap();
    let certificate = service
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    assert_eq!(certificate.content_evidence.required_receipts, 2);
    assert!(certificate.content_evidence.receipt_node_ids.len() >= 2);
    // A fresh remote client reads through ordinary distributed routing and
    // obtains the actual file-sync certificate regardless of content placement.
    let mut reopened = runtime.client(&nodes[1]).unwrap();
    assert_eq!(
        reopened.posix_read_file_distributed(path).unwrap(),
        b"actual durable bytes"
    );
    let remote_certificate = reopened.posix_sync_file_target(&target).unwrap();
    assert_eq!(remote_certificate.target, target);
    assert_eq!(remote_certificate.content_evidence.required_receipts, 2);
    assert!(remote_certificate.content_evidence.receipt_node_ids.len() >= 2);
    drop(reopened);
    target.content_hash = "0".repeat(64);
    assert!(
        service
            .posix_validate_file_sync_target_local(&target)
            .is_err()
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn native_file_sync_stale_authority_redirects_to_local_main_after_real_handoff() {
    use sinomo_dfs::{
        DFS_DATA_NAMESPACE_ROUTE, DfsControlPlaneStore, DfsMessageTransport,
        open_mounted_sifs_service_with_peer_transports_and_default_control_plane,
        test_support::{
            DfsTestFaultAction, DfsTestFaultRule, DfsTestRpcOrigin, DfsTestRpcPhase,
            metadata_handoff_proof_acknowledgements_are_safe,
        },
    };
    use std::{
        collections::BTreeSet,
        sync::{Arc, mpsc},
        time::{Duration, Instant},
    };

    const NAME: &str = "native-file-sync-stale-authority-to-self";
    const PATH: &str = "/native-file-sync-stale-authority-to-self.bin";
    const BYTES: &[u8] = b"actual immutable file prefix across a native role handoff";
    let (runtime, _) = DfsTestRuntime::start_cluster(NAME, 3).unwrap();
    let before = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap();
    let old_primary = before.primary_node_id.clone();
    let promoted = before.replica_node_ids.first().unwrap().clone();
    assert_ne!(old_primary, promoted);
    let mut writer = runtime.client(&old_primary).unwrap();
    writer
        .posix_write_file(
            PATH,
            BYTES,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![old_primary.clone(), promoted.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let target = writer
        .posix_capture_file_sync_target(PATH, DfsPosixSyncKind::File)
        .unwrap();
    let initial_certificate = writer.posix_sync_file_target(&target).unwrap();
    assert_eq!(initial_certificate.target, target);
    assert_eq!(initial_certificate.content_evidence.required_receipts, 2);
    assert!(initial_certificate.content_evidence.receipt_node_ids.len() >= 2);
    drop(writer);

    // The non-Send Core is constructed and dropped INSIDE its worker. Only
    // ordinary owned DATA/peer adapters cross the thread boundary; the runtime
    // and its actual handoff owner stay on this controller thread.
    let worker_data = runtime.data_dir(&promoted).clone();
    let (worker_sync_peer, worker_async_peer) = runtime.peer_transports(&promoted).unwrap();
    let worker_target = target.clone();
    let worker_node = promoted.clone();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    struct CloseGate(Arc<tokio::sync::Semaphore>);
    impl Drop for CloseGate {
        fn drop(&mut self) {
            self.0.close();
        }
    }
    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (start_tx, start_rx) = mpsc::sync_channel(0);
    let original_ring = std::iter::once(before.primary_node_id.clone())
        .chain(before.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let control = runtime.shared_control_plane().unwrap();

    std::thread::scope(|scope| {
        // Declared inside scope, so a failed controller prerequisite closes the
        // negative gate BEFORE scope joins a still-blocked worker.
        let _close_gate = CloseGate(Arc::clone(&gate));
        let worker = scope.spawn(move || -> Result<_, String> {
            let mut caller =
                open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
                    worker_data,
                    worker_sync_peer,
                    worker_async_peer,
                )
                .map_err(|error| error.to_string())?
                .service;
            assert_eq!(caller.node_id(), &worker_node);
            assert!(caller.metadata_backend().is_some());
            // Finish genuine lookup/prewarm BEFORE installing the hold. After
            // ready, the only caller API is this exact frozen FileSync target.
            let warmed = caller
                .posix_capture_file_sync_target(PATH, DfsPosixSyncKind::File)
                .map_err(|error| error.to_string())?;
            if warmed != worker_target {
                return Err("native prewarm changed the frozen target".into());
            }
            ready_tx.send(()).map_err(|error| error.to_string())?;
            let deadline: Instant = start_rx
                .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                .map_err(|error| error.to_string())?;
            let previous = caller.set_transport_operation_deadline(Some(deadline));
            let effective = previous.map_or(deadline, |previous| previous.min(deadline));
            caller.set_transport_operation_deadline(Some(effective));
            let outcome = caller
                .posix_sync_file_target(&worker_target)
                .map_err(|error| error.to_string());
            caller.set_transport_operation_deadline(previous);
            let last_events = format!("{:?}", caller.last_events());
            let namespace_completed = caller
                .last_events()
                .iter()
                .filter_map(|event| match event {
                    sinomo_dfs::NetworkedDfsServiceEvent::RemoteNamespaceCompleted {
                        request_id,
                        primary_node_id,
                        result,
                    } => Some((request_id.clone(), primary_node_id.clone(), result.clone())),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let delegated = caller
                .into_transport()
                .delegates_local_namespace_authority();
            Ok((
                outcome,
                effective,
                delegated,
                last_events,
                namespace_completed,
            ))
        });
        ready_rx
            .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .expect("ordinary caller construction and actual target prewarm must finish");
        // Observe actual decoded receiver frames. The ordinary peer adapters,
        // streamed value body and native prefix-proof/response owners stay intact.
        let old_receiver = runtime
            .capture_namespace_requests(&old_primary, 32)
            .unwrap();
        let self_receiver = runtime.capture_namespace_requests(&promoted, 32).unwrap();
        assert!(old_receiver.records().is_empty());
        let rpc_begin = runtime.rpc_observations().len();
        runtime.install_fault(DfsTestFaultRule {
            source: Some(promoted.clone()),
            target: Some(old_primary.clone()),
            route: Some(DFS_DATA_NAMESPACE_ROUTE.to_owned()),
            call_id: None,
            origin: Some(DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: DfsTestFaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
        });
        // Only the controller, cleanup and configured rule own this gate now.
        // The worker receives no gate Arc. An extra Arc is the actual compiled
        // Fabric::fault action clone retained by its before-dispatch await.
        let installed_gate_owners = Arc::strong_count(&gate);
        let action_started = Instant::now();
        let deadline = action_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        start_tx.send(deadline).unwrap();
        let held_call = loop {
            let calls = runtime.rpc_observations();
            let matching = calls[rpc_begin..]
                .iter()
                .filter(|event| {
                    event.source == promoted
                        && event.target == old_primary
                        && event.route == DFS_DATA_NAMESPACE_ROUTE
                        && event.phase == DfsTestRpcPhase::BeforeDispatch
                })
                .collect::<Vec<_>>();
            if matching.len() == 1 && Arc::strong_count(&gate) > installed_gate_owners {
                let call = (*matching[0]).clone();
                assert!(!worker.is_finished(), "held ordinary call is not terminal");
                assert!(
                    !calls[rpc_begin..]
                        .iter()
                        .any(|event| event.call_id == call.call_id
                            && matches!(
                                event.phase,
                                DfsTestRpcPhase::AfterDispatch
                                    | DfsTestRpcPhase::Completed
                                    | DfsTestRpcPhase::EndedWithoutResponse
                            ))
                );
                break call;
            }
            assert!(
                Instant::now() < deadline && !worker.is_finished(),
                "SETUP requires a real paused before-dispatch action, not just an installed hook"
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        eprintln!(
            "FORCED_FILE_SYNC_HELD call={held_call:?} gate_owners={} installed_owners={installed_gate_owners}",
            Arc::strong_count(&gate)
        );
        assert!(
            old_receiver.records().is_empty(),
            "the held call must not reach the old receiver before the handoff"
        );
        runtime.assert_fault_budgets().unwrap();
        // The paused real call owns its cloned negative action. Removing the
        // sole configured rule cannot release it and avoids hitting unrelated
        // native handoff traffic with an exhausted rule.
        runtime.clear_faults();
        assert_eq!(gate.available_permits(), 0);
        let after = runtime
            .reassign_metadata_shard(
                &before.file_system_id,
                &before.shard_id,
                promoted.clone(),
                original_ring
                    .iter()
                    .filter(|node| **node != promoted)
                    .cloned()
                    .collect(),
            )
            .unwrap();
        let proof = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard_membership_handoff_proof(
                    &before.file_system_id,
                    &before.shard_id,
                )
            })
            .unwrap()
            .unwrap();
        let pending = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .get_metadata_shard_membership_handoff(&before.file_system_id, &before.shard_id)
            })
            .unwrap();
        assert_eq!(proof.retiring_shard, before);
        assert_eq!(proof.desired_shard, after);
        assert!(metadata_handoff_proof_acknowledgements_are_safe(&proof));
        assert!(pending.is_none());
        assert_eq!(after.primary_node_id, promoted);
        assert_eq!(after.epoch, before.epoch.checked_add(1).unwrap());
        assert_eq!(
            std::iter::once(after.primary_node_id.clone())
                .chain(after.replica_node_ids.iter().cloned())
                .collect::<BTreeSet<_>>(),
            original_ring
        );
        assert!(
            Instant::now() < deadline,
            "real handoff must fit the caller's original window"
        );
        assert!(!worker.is_finished());
        assert!(
            !runtime.rpc_observations()[rpc_begin..]
                .iter()
                .any(|event| event.call_id == held_call.call_id
                    && event.phase == DfsTestRpcPhase::AfterDispatch),
            "old authority was not contacted before the real handoff completed"
        );
        gate.add_permits(1);
        while !worker.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            worker.is_finished(),
            "caller must return within the SAME absolute deadline"
        );
        let (outcome, effective_deadline, delegated, last_events, namespace_completed) =
            worker.join().unwrap().unwrap();
        let returned_at = Instant::now();
        let calls = runtime.rpc_observations();
        let old_receiver_frames = old_receiver.records();
        let self_receiver_frames = self_receiver.records();
        let is_frozen_file_sync = |frame: &sinomo_dfs::DfsRoutedMessage| {
            frame.source_node_id == promoted
                && matches!(&frame.message,
                    sinomo_dfs::DfsNetworkMessage::NamespaceRequest(request)
                    if request.file_system_id == target.file_system_id
                        && matches!(&request.operation,
                            sinomo_dfs::DfsNamespaceOperation::FileSync { path, target: actual_target }
                            if path == PATH && actual_target == &target))
        };
        let old_file_sync_frames = old_receiver_frames
            .iter()
            .filter(|frame| frame.target_node_id == old_primary && is_frozen_file_sync(frame))
            .collect::<Vec<_>>();
        let self_file_sync_frames = self_receiver_frames
            .iter()
            .filter(|frame| frame.target_node_id == promoted && is_frozen_file_sync(frame))
            .collect::<Vec<_>>();
        let old_decoded_is_held_file_sync = old_file_sync_frames.iter().any(|frame| {
            let sinomo_dfs::DfsNetworkMessage::NamespaceRequest(request) = &frame.message else {
                return false;
            };
            calls[rpc_begin..].iter().any(|event| {
                event.call_id == held_call.call_id
                    && event.phase == DfsTestRpcPhase::Decoded
                    && event.dfs_frame_id == Some(frame.frame_id)
                    && event.dfs_request_id.as_ref() == Some(&request.request_id)
            })
        });
        let old_completed = calls[rpc_begin..].iter().any(|event| {
            event.call_id == held_call.call_id
                && event.phase == DfsTestRpcPhase::Completed
                && event.outcome_error.is_none()
        });
        let self_decoded = calls[rpc_begin..]
            .iter()
            .filter(|event| {
                event.call_id > held_call.call_id
                    && event.source == promoted
                    && event.target == promoted
                    && event.route == DFS_DATA_NAMESPACE_ROUTE
                    && event.phase == DfsTestRpcPhase::Decoded
                    && event.dfs_file_system_id.as_ref() == Some(&target.file_system_id)
                    && self_file_sync_frames.iter().any(|frame| {
                        event.dfs_frame_id == Some(frame.frame_id)
                            && matches!(&frame.message,
                                sinomo_dfs::DfsNetworkMessage::NamespaceRequest(request)
                                    if event.dfs_request_id.as_ref() == Some(&request.request_id))
                    })
            })
            .collect::<Vec<_>>();
        let self_completed = self_decoded.iter().any(|decoded| {
            calls[rpc_begin..].iter().any(|event| {
                event.call_id == decoded.call_id
                    && event.phase == DfsTestRpcPhase::Completed
                    && event.outcome_error.is_none()
            })
        });
        let caller_applied_from_self_main =
            namespace_completed
                .iter()
                .any(|(request_id, primary, result)| {
                    primary == &promoted
                        && matches!(result, sinomo_dfs::DfsNamespaceResult::Applied)
                        && self_decoded.iter().any(|decoded| {
                            decoded.dfs_request_id.as_deref() == Some(request_id.as_str())
                                && calls[rpc_begin..].iter().any(|event| {
                                    event.call_id == decoded.call_id
                                        && event.phase == DfsTestRpcPhase::Completed
                                        && event.outcome_error.is_none()
                                })
                        })
                });
        let facts = serde_json::json!({
            "before":before,"after":after,"proof":proof,"pending":pending,
            "held_call":format!("{held_call:?}"),"old_completed":old_completed,
            "actual_old_receiver_frames":old_receiver_frames,
            "actual_self_receiver_frames":self_receiver_frames,
            "old_receiver_file_sync_count":old_file_sync_frames.len(),
            "old_decoded_is_held_file_sync":old_decoded_is_held_file_sync,
            "self_file_sync_frame_count":self_file_sync_frames.len(),
            "self_decoded":self_decoded.iter().map(|event|format!("{event:?}")).collect::<Vec<_>>(),
            "self_completed":self_completed,"delegated":delegated,
            "self_main_decoded_count":self_decoded.len(),
            "namespace_completed":namespace_completed,
            "caller_applied_from_self_main":caller_applied_from_self_main,
            "outcome":format!("{outcome:?}"),"last_events":last_events,
            "elapsed_us":returned_at.duration_since(action_started).as_micros(),
            "effective_deadline_equals_original":effective_deadline==deadline,
            "gate_remaining_permits":gate.available_permits(),
            "rpc":calls[rpc_begin..].iter().map(|event|format!("{event:?}")).collect::<Vec<_>>(),
            "qualification":"actual held stale call, native role-only handoff, unchanged call released to old authority, ordinary redirect to registered self Main; no reply, ACK, assignment or certificate is supplied by test"
        });
        let exported = std::fs::write(
            runtime
                .data_dir(&old_primary)
                .root()
                .join("forced-file-sync-redirect.json"),
            serde_json::to_vec_pretty(&facts).unwrap(),
        );
        eprintln!("FORCED_FILE_SYNC_REDIRECT {facts}");
        assert!(delegated);
        assert_eq!(effective_deadline, deadline);
        assert!(
            old_decoded_is_held_file_sync,
            "the actual paused carrier must decode THIS caller's exact frozen FileSync"
        );
        assert_eq!(old_file_sync_frames.len(), 1);
        assert_eq!(self_file_sync_frames.len(), 1);
        assert_eq!(
            old_file_sync_frames.len() + self_file_sync_frames.len(),
            2,
            "one stale and one current decoded FileSync; receiver captures must be complete"
        );
        assert!(
            old_completed,
            "the real old-target native call must actually return"
        );
        assert!(
            self_completed,
            "a real decoded+completed self Main call must replace local execution"
        );
        assert!(
            caller_applied_from_self_main,
            "caller Applied result must match the actual decoded and completed self Main request"
        );
        let certificate =
            outcome.expect("real current Main must return a typed file-sync certificate");
        assert_eq!(certificate.target, target);
        assert_eq!(certificate.content_evidence.required_receipts, 2);
        assert!(certificate.content_evidence.receipt_node_ids.len() >= 2);
        assert_eq!(
            certificate.metadata_certificate.target.primary_node_id,
            promoted
        );
        assert_eq!(
            certificate.metadata_certificate.target.authority_epoch,
            Some(after.epoch)
        );
        assert_eq!(
            certificate.metadata_certificate.target.expected_shard,
            Some(after)
        );
        assert!(
            certificate.metadata_certificate.acknowledged_node_ids.len()
                >= certificate.metadata_certificate.quorum_size
        );
        assert!(returned_at <= deadline);
        assert_eq!(
            gate.available_permits(),
            0,
            "actual native hold consumed its permit"
        );
        assert_eq!(runtime.observation_dropped_count(), 0);
        runtime.assert_fault_budgets().unwrap();
        exported.expect("export after the unchanged semantic oracles");
    });
    drop(control);
    runtime.close().unwrap();
}

#[test]
fn backendless_main_file_sync_preserves_opened_inode_across_writes_links_and_rename() {
    use sinomo_dfs::{DfsNodeTransport, LocalContentStore, NetworkedDfsService};
    const PATH: &str = "/services/backendless-file-sync.bin";
    const ALIAS: &str = "/services/backendless-file-sync.link";
    const SYMLINK: &str = "/services/backendless-file-sync.symlink";
    const RENAMED: &str = "/backendless-file-sync-renamed.bin";
    let (runtime, nodes) = DfsTestRuntime::start_cluster("backendless-file-sync", 3).unwrap();
    let primary = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap()
        .primary_node_id;
    let mut writer = runtime.client(&primary).unwrap();
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let first = writer
        .posix_write_file(PATH, b"first actual version", options.clone())
        .unwrap();
    let (sync_peer, async_peer) = runtime.peer_transports(&primary).unwrap();
    let transport = DfsNodeTransport::with_peer_transports_for_nodes(
        primary.clone(),
        nodes
            .iter()
            .filter(|node| **node != primary)
            .cloned()
            .collect::<Vec<_>>(),
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        sync_peer,
        async_peer,
    )
    .with_registered_main_namespace_dispatch();
    let content =
        LocalContentStore::new(runtime.data_dir(&primary).dfs_dir().join("content")).unwrap();
    let mut requester = NetworkedDfsService::new(transport, content)
        .with_boxed_control_plane(runtime.control_plane_client().unwrap());
    requester.bootstrap_distributed_sifs().unwrap();
    assert!(requester.metadata_backend().is_none());
    let initial = requester
        .posix_capture_file_sync_target(PATH, DfsPosixSyncKind::File)
        .unwrap();
    assert_eq!(initial.object_id, first.object_id);
    let latest = writer
        .posix_write_file(
            PATH,
            b"another descriptor's committed version",
            options.clone(),
        )
        .unwrap();
    assert_eq!(latest.object_id, first.object_id);
    assert_ne!(latest.version, first.version);
    let current = requester
        .posix_capture_file_sync_target_for_object(&first.object_id, DfsPosixSyncKind::File)
        .unwrap();
    assert_eq!(current.version, latest.version);
    assert_eq!(current.content_hash, latest.content_hash.unwrap());
    writer.posix_hardlink(PATH, ALIAS).unwrap();
    let linked = writer.posix_stat_distributed(PATH).unwrap();
    let alias = requester
        .posix_capture_file_sync_target(ALIAS, DfsPosixSyncKind::File)
        .unwrap();
    assert_eq!(alias.object_id, current.object_id);
    assert_eq!(alias.version, linked.version);
    assert_eq!(alias.path, PATH);
    let raw_alias = requester.posix_namespace_lstat_distributed(ALIAS).unwrap();
    assert_eq!(raw_alias.kind, sinomo_dfs::DfsObjectKind::Hardlink);
    assert_ne!(raw_alias.object_id, current.object_id);
    assert_eq!(raw_alias.path, ALIAS);
    assert_eq!(raw_alias.link_count, 2);
    writer
        .posix_symlink(PATH, SYMLINK, options.clone())
        .unwrap();
    assert_eq!(requester.posix_readlink_distributed(SYMLINK).unwrap(), PATH);
    writer.posix_unlink(SYMLINK).unwrap();
    assert_eq!(
        requester.posix_read_file_distributed(ALIAS).unwrap(),
        b"another descriptor's committed version"
    );
    let alias_write = requester
        .posix_write_file(
            ALIAS,
            b"actual Main write through an alias",
            options.clone(),
        )
        .unwrap();
    assert_eq!(alias_write.object_id, current.object_id);
    assert_eq!(
        writer.posix_read_file_distributed(PATH).unwrap(),
        b"actual Main write through an alias"
    );
    let reader_write = requester
        .posix_write_file_from_reader(
            ALIAS,
            std::io::Cursor::new(b"actual Main reader write through an alias"),
            options,
        )
        .unwrap();
    assert_eq!(reader_write.object_id, current.object_id);
    assert_eq!(
        writer.posix_read_file_distributed(PATH).unwrap(),
        b"actual Main reader write through an alias"
    );
    writer.posix_unlink(ALIAS).unwrap();
    writer.posix_rename(PATH, RENAMED).unwrap();
    let renamed = writer.posix_stat_distributed(RENAMED).unwrap();
    assert_eq!(renamed.object_id, current.object_id);
    let root_primary = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap()
        .primary_node_id;
    let receiver = runtime
        .capture_namespace_requests(&root_primary, 32)
        .unwrap();
    let certificate = requester.posix_sync_file_target(&current).unwrap();
    assert_eq!(
        certificate.target, current,
        "routing must not rewrite the frozen version/hash/path"
    );
    assert_eq!(certificate.content_evidence.required_receipts, 2);
    assert!(certificate.content_evidence.receipt_node_ids.len() >= 2);
    assert!(
        receiver.records().iter().any(|frame| {
            frame.target_node_id == root_primary
                && matches!(
                    &frame.message,
                    sinomo_dfs::DfsNetworkMessage::NamespaceRequest(request)
                        if matches!(&request.operation,
                            sinomo_dfs::DfsNamespaceOperation::FileSync { path, target }
                                if path == RENAMED && target == &current)
                )
        }),
        "Main must receive the frozen target at its current namespace authority"
    );
    drop(receiver);
    let after_rename = requester
        .posix_capture_file_sync_target_for_object(&first.object_id, DfsPosixSyncKind::File)
        .unwrap();
    assert_eq!(after_rename.path, RENAMED);
    assert_eq!(after_rename.version, renamed.version);
    writer.posix_unlink(RENAMED).unwrap();
    let unlinked_certificate = requester.posix_sync_file_target(&current).unwrap();
    assert_eq!(unlinked_certificate.target, current);
    assert_eq!(unlinked_certificate.content_evidence.required_receipts, 2);
    assert!(requester.metadata_backend().is_none());
    drop(requester);
    drop(writer);
    runtime.close().unwrap();
}
