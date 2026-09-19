use super::*;

fn retained_local_publication() -> DfsWritePublication {
    DfsWritePublication::Retained {
        capture: Box::new(DfsRetainedWritePublication {
            pin_id: "preserved-local-operation".into(),
            source: DfsFileVersionSource::LocalOperation {
                operation_id: "explicit-preserve-local".into(),
                base_version: Some(DfsVersion::from_legacy_named("disconnected-base").unwrap()),
            },
        }),
    }
}

#[test]
fn file_history_retained_content_publication_and_restart_never_replace_current_file() {
    let root = temp_root("retained-content-commit");
    let owner = NodeId::from_legacy_named("retained-content-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let path = "/services/preserved-local-file";
    let stat = dfs
        .posix_write_file(
            path,
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let original = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    let target = DfsVersion::from_legacy_named("preserved-local-content").unwrap();
    let bytes = b"disconnected local operation bytes";
    let hash = content_hash(bytes);
    let intent = dfs
        .coordinator
        .create_intent_for_publication(
            stat.file_system_id.clone(),
            stat.object_id.clone(),
            None,
            target.clone(),
            owner.clone(),
            0,
            vec![owner.clone()],
            DfsReplicationPolicy::new(1, 1).unwrap(),
            false,
            None,
            Some("preserve-local-content".into()),
            Some("explicit-preserve-local".into()),
            None,
            retained_local_publication(),
        )
        .unwrap();
    let mut descriptor = original.clone();
    descriptor.version = target.clone();
    descriptor.content_hash = Some(hash.clone());
    descriptor.content_size_bytes = Some(bytes.len() as u64);
    let record = crate::intent::write_intent_record(&intent, Some(&hash), Some(descriptor));
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .put_intent(&record)
        .unwrap();
    dfs.store_command_bytes(&stat.object_id, &target, &hash, bytes)
        .unwrap();
    dfs.writes.pending_writes.insert(
        intent.intent_id.clone(),
        crate::transfer::PendingNetworkedWrite::new(
            stat.object_id.clone(),
            target.clone(),
            hash.clone(),
            bytes.len() as u64,
        ),
    );
    assert!(
        dfs.apply_committed_intent(&intent.intent_id).is_err(),
        "content capture still requires W receipts"
    );
    dfs.put_metadata_receipt(
        &stat.file_system_id,
        &stat.object_id,
        &target,
        &owner,
        &hash,
    )
    .unwrap();
    dfs.coordinator
        .record_durable_receipt(&intent.intent_id, owner.clone())
        .unwrap();
    dfs.apply_committed_intent(&intent.intent_id).unwrap();
    assert!(
        dfs.retained_file_content_is_live(&stat.object_id, &target, &hash)
            .unwrap()
    );
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id).unwrap(),
        original
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"canonical");
    let captured = DfsFileVersionIdentity {
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        version: target.clone(),
    };
    assert!(
        matches!(dfs.read_file_history(DfsFileHistoryReadRequest { identity: captured.clone(), offset: 0, max_bytes: 64 }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == bytes)
    );
    // Replaying the pre-bookkeeping state after a crash must find the existing
    // capture. An independently released pin must not be recreated by replay.
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: path.into(),
        identity: captured,
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "preserved-local-operation".into(),
        },
        operation_id: "release-preserved-local".into(),
    })
    .unwrap();
    assert!(
        !dfs.retained_file_content_is_live(&stat.object_id, &target, &hash)
            .unwrap()
    );
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .put_intent(&record)
        .unwrap();
    drop(dfs);
    let dfs = networked_sifs_with_backend(&root, owner);
    assert_eq!(dfs.posix_stat(path).unwrap().version, stat.version);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"canonical");
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        dfs.conflicts_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn file_history_retained_replica_staging_does_not_publish_or_block_current_content() {
    let root = temp_root("retained-content-stage");
    let owner = NodeId::from_legacy_named("retained-stage-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let path = "/services/stage-local-file";
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file(path, b"canonical", options.clone())
        .unwrap();
    let object = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    let command = DfsStageBlobCommand {
        request_id: "retained-stage".into(),
        intent_id: "explicit-retained-stage".into(),
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        expected_base_version: Some(DfsVersion::from_legacy_named("old-local-base").unwrap()),
        version: DfsVersion::from_legacy_named("retained-stage-target").unwrap(),
        content_hash: content_hash(b"local candidate"),
        bytes: b"local candidate".to_vec(),
        object_metadata: Some(object.clone()),
        authority_shard: None,
    };
    let mut foreign = command.clone();
    foreign.file_system_id =
        DfsFileSystemId::from_legacy_named("foreign-retained-filesystem").unwrap();
    foreign.object_metadata.as_mut().unwrap().file_system_id = foreign.file_system_id.clone();
    assert!(matches!(
        dfs.handle_stage_retained_blob(owner.clone(), foreign, None)
            .unwrap(),
        NetworkedDfsServiceEvent::BlobStageRejected { .. }
    ));
    assert!(matches!(
        dfs.handle_stage_retained_blob(owner.clone(), command.clone(), None)
            .unwrap(),
        NetworkedDfsServiceEvent::BlobStaged { .. }
    ));
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id).unwrap(),
        object
    );
    assert!(
        !dfs.writes
            .active_replica_stages
            .contains_key(&stat.object_id)
    );
    let mut conflicting = command;
    conflicting.content_hash = content_hash(b"other candidate");
    conflicting.bytes = b"other candidate".to_vec();
    assert!(matches!(
        dfs.handle_stage_retained_blob(owner, conflicting, None)
            .unwrap(),
        NetworkedDfsServiceEvent::BlobStageRejected { .. }
    ));
    dfs.posix_write_file(path, b"next canonical", options)
        .unwrap();
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"next canonical");
    assert!(
        dfs.list_file_history(DfsFileHistoryListRequest {
            file_system_id: stat.file_system_id,
            path: Some(path.into()),
            object_id: None,
            after: None,
            limit: 64,
        })
        .unwrap()
        .entries
        .is_empty(),
        "staging is not explicit metadata publication"
    );
}

fn history_identity(stat: &DfsStat) -> DfsFileVersionIdentity {
    DfsFileVersionIdentity {
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        version: stat.version.clone(),
    }
}

fn preserve_request(stat: &DfsStat, operation_id: &str) -> DfsFileHistoryPreserveRequest {
    DfsFileHistoryPreserveRequest {
        path: stat.path.clone(),
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        local_operation_id: "original-local-operation".into(),
        base_version: Some(stat.version.clone()),
        pin_id: "explicit-local-preservation".into(),
        operation_id: operation_id.into(),
    }
}

#[test]
fn file_history_preserve_checks_capability_before_consuming_a_local_reader() {
    let root = temp_root("preserve-capability-gate");
    let owner = NodeId::from_legacy_named("preserve-capability-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file(
            "/services/preserve-capability",
            b"canonical",
            options.clone(),
        )
        .unwrap();
    let mut kv = InProcessKvStore::new();
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV2,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        None,
    )
    .unwrap();
    let mut dfs = dfs.with_control_plane(SharedDfsControlPlaneKvClient::new(kv));
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let mut reader = std::io::Cursor::new(b"candidate");
    assert!(
        dfs.preserve_local_file_version(
            preserve_request(&stat, "unsupported-preserve"),
            &mut reader,
            options
        )
        .unwrap_err()
        .to_string()
        .contains("history-capable")
    );
    assert_eq!(reader.position(), 0);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
}

#[test]
fn file_history_preserve_keeps_local_owned_bytes_on_the_owner() {
    let root = temp_root("preserve-local-owned");
    let owner = NodeId::from_legacy_named("preserve-local-owned-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let stat = dfs
        .posix_write_file(
            "/services/preserve-local-owned",
            b"private",
            options.clone(),
        )
        .unwrap();
    let mut object = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    object.policy = DfsObjectPolicy::LocalOwned;
    dfs.add_object_metadata(object.clone()).unwrap();
    let outcome = dfs
        .preserve_local_file_version(
            preserve_request(&stat, "preserve-private"),
            &b"private candidate"[..],
            options,
        )
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Retained { identity } = outcome else {
        panic!("local owner is durable");
    };
    let version = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&identity.object_id, &identity.version)
        .unwrap()
        .unwrap();
    assert_eq!(version.locations, vec![object.owner_node_id.clone()]);
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id).unwrap(),
        object
    );
    assert_eq!(dfs.posix_read_file(&stat.path).unwrap(), b"private");
}

#[test]
fn file_history_preserve_routes_to_authority_and_keeps_both_current_views() {
    let root = temp_root("preserve-local-routed-api");
    let owner = NodeId::from_legacy_named("preserve-routed-owner").unwrap();
    let requester = NodeId::from_legacy_named("preserve-routed-requester").unwrap();
    let network = InProcessNetwork::new();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("primary-metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let stat = primary
        .posix_write_file(
            "/services/retained-routed",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let mut kv = InProcessKvStore::new();
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        None,
    )
    .unwrap();
    let shared = SharedDfsControlPlaneKvClient::new(kv);
    let primary = primary.with_control_plane(shared.clone());
    let mut client = NetworkedDfsService::new(
        network.add_node(requester.clone()),
        LocalContentStore::new(root.join("requester-content")).unwrap(),
    )
    .with_control_plane(shared);
    let stop = Arc::new(AtomicBool::new(false));
    let primary_stop = stop.clone();
    let pump = std::thread::spawn(move || pump_sifs_service_until_stop(primary, primary_stop));
    let result = (|| {
        let request = preserve_request(&stat, "preserve-routed-once");
        let options = DfsWriteOptions {
            replica_candidates: vec![requester],
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        let outcome = client.preserve_local_file_version(
            request.clone(),
            &b"local staged alternative"[..],
            options.clone(),
        )?;
        let DfsFileHistoryPreserveOutcome::Retained { identity } = outcome else {
            panic!("preservation must reach W");
        };
        assert_eq!(
            client
                .current_read_object_metadata(&stat.object_id)?
                .version,
            stat.version
        );
        let retry = client.preserve_local_file_version(
            request,
            &b"local staged alternative"[..],
            options,
        )?;
        assert_eq!(
            retry,
            DfsFileHistoryPreserveOutcome::Retained {
                identity: identity.clone()
            }
        );
        client.read_file_history(DfsFileHistoryReadRequest {
            identity,
            offset: 0,
            max_bytes: 64,
        })
    })();
    stop.store(true, Ordering::SeqCst);
    let primary = pump.join().unwrap();
    assert!(
        matches!(result.unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"local staged alternative")
    );
    assert_eq!(
        primary.posix_stat(&stat.path).unwrap().version,
        stat.version
    );
    assert_eq!(primary.posix_read_file(&stat.path).unwrap(), b"canonical");
}

#[test]
fn file_history_preserve_api_replays_without_repinning_or_replacing_current_content() {
    let root = temp_root("preserve-local-reader-api");
    let owner = NodeId::from_legacy_named("preserve-reader-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file("/services/preserve-reader", b"base", options.clone())
        .unwrap();
    let request = preserve_request(&stat, "preserve-reader-once");
    let current = dfs
        .posix_write_file(&stat.path, b"canonical moved on", options.clone())
        .unwrap();
    let outcome = dfs
        .preserve_local_file_version(request.clone(), &b"local candidate"[..], options.clone())
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Retained { identity } = outcome else {
        panic!("local W reached");
    };
    assert_ne!(identity.version, current.version);
    assert_eq!(dfs.posix_stat(&stat.path).unwrap().version, current.version);
    assert!(
        matches!(dfs.read_file_history(DfsFileHistoryReadRequest { identity: identity.clone(), offset: 0, max_bytes: 64 }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"local candidate")
    );
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: "release-local-reader".into(),
    })
    .unwrap();
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    assert_eq!(
        dfs.preserve_local_file_version(request.clone(), &b"local candidate"[..], options.clone())
            .unwrap(),
        DfsFileHistoryPreserveOutcome::Retained {
            identity: identity.clone()
        }
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        dfs.preserve_local_file_version(request, &b"changed candidate"[..], options)
            .is_err()
    );
    assert_eq!(
        dfs.posix_read_file(&stat.path).unwrap(),
        b"canonical moved on"
    );
    assert!(
        dfs.conflicts_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn file_history_pending_preservation_does_not_block_current_writes_and_publishes_after_w() {
    let root = temp_root("preserve-local-receipt-threshold");
    let owner = NodeId::from_legacy_named("preserve-threshold-owner").unwrap();
    let remote = NodeId::from_legacy_named("preserve-threshold-remote").unwrap();
    let network = InProcessNetwork::new();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("owner-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("owner-metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let ordinary = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = primary
        .posix_write_file("/services/preserve-threshold", b"base", ordinary.clone())
        .unwrap();
    let request = preserve_request(&stat, "preserve-needs-two-replicas");
    let pending = primary
        .preserve_local_file_version(
            request,
            &b"candidate awaiting W"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote.clone()],
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                wait_for_commit: false,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Pending {
        identity,
        intent_id,
    } = pending
    else {
        panic!("offline replica prevents W");
    };
    assert!(
        primary
            .metadata_backend()
            .unwrap()
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .is_none()
    );
    primary
        .posix_write_file(&stat.path, b"next current content", ordinary)
        .unwrap();
    let current = primary
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    let mut replica = NetworkedDfsService::new(
        network.add_node(remote.clone()),
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    );
    primary.repair_retry_tick = primary.repair_retry_tick.saturating_add(100);
    primary
        .stage_missing_desired_replicas_for_intent(&intent_id, current.clone())
        .unwrap();
    let replica_events = replica.process_incoming().unwrap();
    let primary_events = primary.process_incoming().unwrap();
    assert!(
        matches!(
            primary.write_outcome(&intent_id).unwrap(),
            NetworkedDfsWriteOutcome::Committed { .. }
        ),
        "replica events: {replica_events:?}; primary events: {primary_events:?}"
    );
    assert_eq!(
        primary
            .current_read_object_metadata(&stat.object_id)
            .unwrap(),
        current
    );
    assert_eq!(
        primary.posix_read_file(&stat.path).unwrap(),
        b"next current content"
    );
    assert!(
        matches!(primary.read_file_history(DfsFileHistoryReadRequest { identity, offset: 0, max_bytes: 64 }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"candidate awaiting W")
    );
    assert!(
        replica.metadata.object(&stat.object_id).is_none(),
        "staging historical bytes must not synthesize a current replica object"
    );
}

#[test]
fn file_history_repairs_retained_bytes_without_rewinding_the_replica() {
    let root = temp_root("history-storage-only-repair");
    let owner = NodeId::from_legacy_named("history-repair-owner").unwrap();
    let replica = NodeId::from_legacy_named("history-repair-replica").unwrap();
    let network = InProcessNetwork::new();
    let mut replica_transport = network.add_node(replica.clone());
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("primary-metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let path = "/services/repair-retained";
    let first = primary
        .posix_write_file(path, b"retained bytes", options.clone())
        .unwrap();
    capture_history(&mut primary, &first);
    primary
        .posix_write_file(path, b"new current bytes", options)
        .unwrap();
    let current = primary
        .current_read_object_metadata(&first.object_id)
        .unwrap();
    let outcomes = primary
        .send_repair_replica_attachments_if_routable(
            &[(replica.clone(), "repair-explicit-version".into())],
            "repair-history",
            &first.file_system_id,
            &first.object_id,
            Some(first.version.clone()),
            &first.version,
            &content_hash(b"retained bytes"),
            b"retained bytes".len() as u64,
            Some(current.clone()),
        )
        .unwrap();
    assert!(outcomes[0].sent);
    let mut commands = drain_dfs_messages(&mut replica_transport);
    assert_eq!(commands.len(), 1);
    let DfsNetworkMessage::RepairRetainedBlob(command) = commands.remove(0) else {
        panic!("retained content must use storage-only repair");
    };
    assert_eq!(command.bytes, b"retained bytes");
    let mut replica_service = NetworkedDfsService::new(
        replica_transport,
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("replica-metadata")).unwrap())
    .unwrap();
    replica_service
        .add_file_system(DfsFileSystem::sifs(owner.clone()))
        .unwrap();
    replica_service
        .add_object_metadata(current.clone())
        .unwrap();
    assert!(matches!(
        replica_service
            .handle_repair_retained_blob(owner, command, None)
            .unwrap(),
        NetworkedDfsServiceEvent::ReplicaRepaired { .. }
    ));
    assert_eq!(
        replica_service
            .current_read_object_metadata(&first.object_id)
            .unwrap(),
        current
    );
    assert_eq!(
        replica_service
            .content
            .read_object_version(first.object_id.as_str(), first.version.as_str())
            .unwrap()
            .1,
        b"retained bytes"
    );
    assert!(
        !replica_service
            .writes
            .active_replica_stages
            .contains_key(&first.object_id)
    );
}

fn capture_history<T: DfsMessageTransport>(dfs: &mut NetworkedDfsService<T>, stat: &DfsStat) {
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: history_identity(stat),
        operation: DfsFileHistoryPinOperation::Capture {
            pin_id: "checkpoint".into(),
        },
        operation_id: format!("capture-{}", stat.version),
    })
    .unwrap();
}

#[test]
fn file_history_restore_rejects_wrong_input_before_publishing_a_destination() {
    let root = temp_root("restore-source-identity");
    let owner = NodeId::from_legacy_named("restore-source-identity-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let source = dfs
        .posix_write_file(
            "/services/restore-identity-source",
            b"saved",
            options.clone(),
        )
        .unwrap();
    capture_history(&mut dfs, &source);
    let path = "/services/restore-identity-destination";
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: path.into(),
        expected_destination: None,
        operation_id: "restore-source-identity-once".into(),
    };
    let job = dfs
        .begin_file_restore_job(path, &request, &options)
        .unwrap();
    let job = dfs
        .posix_resume_namespace_job_bounded(&job.job_id, 2)
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job)
        .unwrap()
        .clone();
    let parent_before = dfs.posix_stat("/services").unwrap();
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let input = crate::service_file_restore::FileRestoreInput::Reader {
        reader: Box::new(&b"wrong"[..]),
        content_hash: content_hash(b"saved"),
        size_bytes: 5,
    };
    assert!(
        dfs.posix_restore_file_content_for_requester(
            path,
            &request,
            input,
            options.clone(),
            &state.requester_node_id,
            &state.write_operation_id
        )
        .is_err()
    );
    assert!(dfs.posix_stat(path).is_err());
    assert_eq!(
        dfs.posix_stat("/services").unwrap().version,
        parent_before.version
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    assert!(
        dfs.content
            .read_manifest(
                state.planned.object_id.as_str(),
                state.planned.version.as_str()
            )
            .is_err(),
        "rejected input must remain unbound quarantine"
    );
    let response = dfs.restore_file_history(request, options).unwrap();
    assert_eq!(response.committed, state.planned);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"saved");
}

#[test]
fn file_history_restore_runs_on_the_primary_with_the_original_requester_identity() {
    let root = temp_root("restore-routed-job");
    let owner = NodeId::from_legacy_named("restore-routed-primary").unwrap();
    let requester = NodeId::from_legacy_named("restore-routed-requester").unwrap();
    let network = InProcessNetwork::new();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("primary-metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let source = primary
        .posix_write_file(
            "/services/routed-restore-source",
            b"saved remote content",
            options.clone(),
        )
        .unwrap();
    let destination = primary
        .posix_write_file(
            "/services/routed-restore-destination",
            b"before",
            options.clone(),
        )
        .unwrap();
    let mut kv = InProcessKvStore::new();
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        },
        None,
    )
    .unwrap();
    let shared = SharedDfsControlPlaneKvClient::new(kv);
    let primary = primary.with_control_plane(shared.clone());
    let mut client = NetworkedDfsService::new(
        network.add_node(requester.clone()),
        LocalContentStore::new(root.join("client-content")).unwrap(),
    )
    .with_control_plane(shared);
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: destination.path.clone(),
        expected_destination: Some(history_identity(&destination)),
        operation_id: "routed-restore-once".into(),
    };
    let stop = Arc::new(AtomicBool::new(false));
    let primary_stop = stop.clone();
    let pump = std::thread::spawn(move || pump_sifs_service_until_stop(primary, primary_stop));
    let result = (|| {
        client.file_history_pin(DfsFileHistoryPinRequest {
            path: source.path.clone(),
            identity: history_identity(&source),
            operation: DfsFileHistoryPinOperation::Capture {
                pin_id: "checkpoint".into(),
            },
            operation_id: "routed-checkpoint-before-restore".into(),
        })?;
        let response = client.restore_file_history(request.clone(), options.clone())?;
        assert_eq!(
            client
                .posix_read_file_distributed(&destination.path)
                .map_err(|source| DfsError::Metadata {
                    message: source.to_string()
                })?,
            b"saved remote content"
        );
        assert_eq!(
            client
                .restore_file_history(request.clone(), options)
                .unwrap(),
            response
        );
        Ok::<_, DfsError>(response)
    })();
    stop.store(true, Ordering::SeqCst);
    let primary = pump.join().unwrap();
    let response = result.unwrap();
    let job_id =
        crate::service_restore_jobs::file_restore_job_id(&requester, &request.operation_id);
    let job = primary
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&job_id)
        .unwrap()
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job).unwrap();
    assert_eq!(state.requester_node_id, requester);
    assert_eq!(state.planned, response.committed);
    assert!(state.cleanup_complete);
    let receipts = primary
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&response.committed.object_id, &response.committed.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<Vec<_>>(),
        vec![owner],
        "the executing primary owns the ingested bytes; requester identity is not a storage receipt"
    );
    assert_eq!(
        primary
            .metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn file_history_restore_recovers_a_commit_after_later_edit_and_replays_compacted_result() {
    let root = temp_root("restore-durable-result");
    let owner = NodeId::from_legacy_named("restore-durable-result-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let path = "/services/durable-restore";
    let source = dfs
        .posix_write_file(path, b"saved", options.clone())
        .unwrap();
    capture_history(&mut dfs, &source);
    let edited = dfs
        .posix_write_file(path, b"before restore", options.clone())
        .unwrap();
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: path.into(),
        expected_destination: Some(history_identity(&edited)),
        operation_id: "restore-durable-result-once".into(),
    };
    let job = dfs
        .begin_file_restore_job(path, &request, &options)
        .unwrap();
    let job = dfs
        .posix_resume_namespace_job_bounded(&job.job_id, 2)
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job)
        .unwrap()
        .clone();
    assert!(state.source_pin_applied && state.result.is_none());
    // The ordinary write commits, then the coordinator crashes before it can
    // checkpoint the restore result. A subsequent writer advances the head.
    let descriptor = dfs
        .resolve_file_history_version(&request.source)
        .unwrap()
        .unwrap();
    let input = dfs.open_file_restore_reader(&descriptor).unwrap();
    let committed = dfs
        .posix_restore_file_content_for_requester(
            path,
            &request,
            input,
            options.clone(),
            &state.requester_node_id,
            &state.write_operation_id,
        )
        .unwrap();
    dfs.posix_write_file(path, b"later edit", options.clone())
        .unwrap();
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    let write_intent_id = crate::intent::stable_write_intent_id(
        &owner,
        &state.planned.object_id,
        &state.write_operation_id,
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_intent_receipt(&state.planned.object_id, &write_intent_id)
            .unwrap()
            .is_none(),
        "the unfinished job must retain its full commit witness"
    );
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let response = dfs
        .restore_file_history(request.clone(), options.clone())
        .unwrap();
    assert_eq!(response.committed, history_identity(&committed));
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"later edit");
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: path.into(),
        identity: history_identity(&source),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "release-source-after-restore".into(),
    })
    .unwrap();
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&source),
            offset: 0,
            max_bytes: 64
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_namespace_job(&job.job_id)
            .unwrap()
            .is_none()
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_namespace_job_receipt(&job.job_id)
            .unwrap()
            .unwrap()
            .file_restore
            .is_some()
    );
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    assert_eq!(
        dfs.restore_file_history(request, options).unwrap(),
        response
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"later edit");
}

#[test]
fn file_history_restore_cleanup_survives_restart_and_lost_unpin_reply() {
    let root = temp_root("restore-durable-cleanup");
    let owner = NodeId::from_legacy_named("restore-durable-cleanup-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let path = "/services/restore-cleanup";
    let source = dfs
        .posix_write_file(path, b"saved", options.clone())
        .unwrap();
    capture_history(&mut dfs, &source);
    let edited = dfs
        .posix_write_file(path, b"edited", options.clone())
        .unwrap();
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: path.into(),
        expected_destination: Some(history_identity(&edited)),
        operation_id: "restore-cleanup-once".into(),
    };
    let initial = dfs
        .begin_file_restore_job(path, &request, &options)
        .unwrap();
    let job = dfs
        .posix_resume_namespace_job_bounded(&initial.job_id, 3)
        .unwrap();
    let state = crate::service_restore_jobs::restore_job_state(&job)
        .unwrap()
        .clone();
    assert!(state.result.is_some() && !state.cleanup_complete);
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    assert!(
        dfs.metadata_backend
            .as_mut()
            .unwrap()
            .put_namespace_job(&initial)
            .is_err()
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    let pin = state.source_pin.unwrap();
    let DfsFileHistoryPinOperation::Pin { pin_id } = pin.operation.clone() else {
        panic!("restore pin");
    };
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: pin_id.clone(),
        },
        operation_id: format!("restore-unpin:{}", job.job_id),
        ..pin
    })
    .unwrap();
    dfs.posix_write_file(path, b"after successful restore", options.clone())
        .unwrap();
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    let report = dfs.posix_resume_namespace_jobs_bounded(8, 4).unwrap();
    assert_eq!(report.completed_job_count, 1);
    assert!(
        !dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap()
            .iter()
            .any(|pin| pin.pin_id == pin_id)
    );
    let response = dfs.restore_file_history(request, options).unwrap();
    assert_eq!(response.committed, state.planned);
    assert_eq!(
        dfs.posix_read_file(path).unwrap(),
        b"after successful restore"
    );
}

#[test]
fn file_history_reads_restore_and_deleted_recovery_keep_one_current_view() {
    let root = temp_root("history-restore-and-recovery");
    let owner = NodeId::from_legacy_named("history-restore-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let path = "/services/history-document";
    let original = dfs
        .posix_write_file(path, b"original\n", options.clone())
        .unwrap();
    let identity = history_identity(&original);
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 0,
            max_bytes: 64
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    capture_history(&mut dfs, &original);
    let edited = dfs
        .posix_write_file(path, b"edited\n", options.clone())
        .unwrap();
    let page = dfs
        .list_file_history(DfsFileHistoryListRequest {
            file_system_id: identity.file_system_id.clone(),
            path: Some(path.into()),
            object_id: None,
            after: None,
            limit: 128,
        })
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert!(!page.entries[0].is_current);
    let read = dfs
        .read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 2,
            max_bytes: 3,
        })
        .unwrap();
    let DfsFileHistoryReadOutcome::Found { data } = read else {
        panic!("retained bytes must be readable");
    };
    assert_eq!(data.bytes, b"igi");
    assert_eq!(dfs.posix_stat(path).unwrap().version, edited.version);
    let restored = dfs
        .restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: path.into(),
                expected_destination: Some(history_identity(&edited)),
                operation_id: "restore-history-document".into(),
            },
            options.clone(),
        )
        .unwrap();
    assert_ne!(restored.committed.version, identity.version);
    assert_ne!(restored.committed.version, edited.version);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"original\n");
    assert!(
        dfs.restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: path.into(),
                expected_destination: Some(history_identity(&edited)),
                operation_id: "stale-restore".into()
            },
            options.clone()
        )
        .is_err()
    );
    assert_eq!(
        dfs.posix_stat(path).unwrap().version,
        restored.committed.version
    );
    dfs.posix_unlink(path).unwrap();
    let recovered = dfs
        .restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: path.into(),
                expected_destination: None,
                operation_id: "recover-history-document".into(),
            },
            options,
        )
        .unwrap();
    assert_ne!(recovered.committed.object_id, identity.object_id);
    assert_eq!(
        dfs.posix_stat(path).unwrap().object_id,
        recovered.committed.object_id,
        "releasing an old version pin must not re-publish its deleted object's tombstone"
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"original\n");
    assert!(
        dfs.conflicts_for_object(&identity.object_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn file_history_query_routes_to_primary_and_reads_an_old_replica_range() {
    let root = temp_root("history-remote-query");
    let owner = NodeId::from_legacy_named("history-primary").unwrap();
    let requester = NodeId::from_legacy_named("history-requester").unwrap();
    let network = InProcessNetwork::new();
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .unwrap();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("primary-content")).unwrap(),
    )
    .with_metadata_backend(backend)
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let original = primary
        .posix_write_file(
            "/services/remote-history",
            b"remote-original",
            options.clone(),
        )
        .unwrap();
    capture_history(&mut primary, &original);
    let edited = primary
        .posix_write_file("/services/remote-history", b"current", options)
        .unwrap();
    let journal_before_reads = primary
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let mut kv = InProcessKvStore::new();
    let shard = DfsMetadataShardConfig {
        file_system_id: sifs_file_system_id(),
        shard_id: "root".into(),
        primary_node_id: owner,
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment: DfsMetadataShardAssignment::Root,
        namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV3,
        default_policy: DfsObjectPolicy::ReplicatedEventual,
    };
    kv.put_serde(
        crate::control_plane::metadata_shard_key(&sifs_file_system_id(), "root"),
        &shard,
        None,
    )
    .unwrap();
    let shared = SharedDfsControlPlaneKvClient::new(kv);
    let primary = primary.with_control_plane(shared.clone());
    let mut client = NetworkedDfsService::new(
        network.add_node(requester),
        LocalContentStore::new(root.join("client-content")).unwrap(),
    )
    .with_control_plane(shared);
    let stop = Arc::new(AtomicBool::new(false));
    let primary_stop = stop.clone();
    let pump = std::thread::spawn(move || pump_sifs_service_until_stop(primary, primary_stop));
    let result = (|| {
        let page = client.list_file_history(DfsFileHistoryListRequest {
            file_system_id: sifs_file_system_id(),
            path: Some("/services".into()),
            object_id: None,
            after: None,
            limit: 128,
        })?;
        assert_eq!(page.entries.len(), 1);
        client.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&original),
            offset: 7,
            max_bytes: 8,
        })
    })();
    stop.store(true, Ordering::SeqCst);
    let primary = pump.join().unwrap();
    let DfsFileHistoryReadOutcome::Found { data } = result.unwrap() else {
        panic!("historical replica range unavailable");
    };
    assert_eq!(data.bytes, b"original");
    assert_eq!(
        primary
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        journal_before_reads,
        "history queries must not publish metadata or mint another version"
    );
    assert_eq!(
        primary
            .posix_stat("/services/remote-history")
            .unwrap()
            .version,
        edited.version
    );
}

#[test]
fn file_history_cursor_cannot_be_reused_for_another_query() {
    let root = temp_root("history-query-cursor");
    let owner = NodeId::from_legacy_named("history-cursor-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    for path in ["/services/history-a", "/services/history-b"] {
        let stat = dfs
            .posix_write_file(path, b"content", options.clone())
            .unwrap();
        capture_history(&mut dfs, &stat);
    }
    let mut request = DfsFileHistoryListRequest {
        file_system_id: sifs_file_system_id(),
        path: Some("/services".into()),
        object_id: None,
        after: None,
        limit: 1,
    };
    let page = dfs.list_file_history(request.clone()).unwrap();
    request.after = Some(page.next_cursor.expect("bounded scan has another page"));
    request.path = Some("/users".into());
    assert!(
        dfs.list_file_history(request)
            .unwrap_err()
            .to_string()
            .contains("another query")
    );
}

#[test]
fn file_history_hardlink_capture_uses_canonical_identity_and_follows_promotion() {
    let root = temp_root("history-hardlink-capture");
    let owner = NodeId::from_legacy_named("history-hardlink-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let original = "/services/history-original";
    let alias = "/services/history-alias";
    dfs.posix_write_file(original, b"shared inode", options)
        .unwrap();
    dfs.posix_hardlink(original, alias).unwrap();
    let through_alias = dfs.posix_stat(alias).unwrap();
    capture_history(&mut dfs, &through_alias);
    let identity = history_identity(&through_alias);
    let entry = dfs
        .list_file_history(DfsFileHistoryListRequest {
            file_system_id: sifs_file_system_id(),
            object_id: Some(identity.object_id.clone()),
            path: None,
            after: None,
            limit: 128,
        })
        .unwrap();
    assert_eq!(entry.entries.len(), 1);
    assert_eq!(entry.entries[0].captured.object.path, original);
    dfs.posix_unlink(original).unwrap();
    assert_eq!(dfs.posix_stat(alias).unwrap().object_id, identity.object_id);
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: original.into(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "unpin-after-promotion".into(),
    })
    .unwrap();
    assert_eq!(dfs.posix_read_file(alias).unwrap(), b"shared inode");
    assert!(
        dfs.list_file_history(DfsFileHistoryListRequest {
            file_system_id: sifs_file_system_id(),
            object_id: Some(identity.object_id),
            path: None,
            after: None,
            limit: 128
        })
        .unwrap()
        .entries
        .is_empty()
    );
}

#[test]
fn file_history_policy_applies_to_normal_writes_without_creating_conflicts() {
    let root = temp_root("history-service-retention");
    let owner = NodeId::from_legacy_named("history-policy-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let mut fs = crate::namespace::distributed_sifs(owner);
    fs.file_history.rules.push(DfsFileHistoryRule {
        path: "/services".into(),
        retention: DfsFileHistoryRetention {
            keep_last: 1,
            keep_for_seconds: 0,
        },
    });
    dfs.add_file_system(fs).unwrap();
    let path = "/services/history-policy-file";
    let first = dfs
        .posix_write_file(path, b"first", options.clone())
        .unwrap();
    let second = dfs
        .posix_write_file(path, b"second", options.clone())
        .unwrap();
    let current = dfs.posix_write_file(path, b"current", options).unwrap();
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"current");
    assert!(
        dfs.conflicts_for_object(&first.object_id)
            .unwrap()
            .is_empty()
    );
    let backend = dfs.metadata_backend.as_mut().unwrap();
    for _ in 0..10 {
        backend
            .gc_metadata_history(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                    .with_now_unix_nanos(u64::MAX),
            )
            .unwrap();
    }
    assert!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_version(&second.object_id, &second.version)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .unwrap()
            .history_order,
        2
    );
}

#[test]
fn file_history_service_capture_and_unpin_preserve_the_current_namespace() {
    let root = temp_root("file-history-service");
    let owner = NodeId::from_legacy_named("history-owner").unwrap();
    let content = LocalContentStore::new(root.join("content")).unwrap();
    let mut object = file_metadata(owner.clone());
    object.path = "/src/lib.rs".into();
    let manifest = content
        .put_object_version(object.object_id.as_str(), object.version.as_str(), b"first")
        .unwrap();
    object.content_hash = Some(manifest.content_hash);
    object.content_size_bytes = Some(5);
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .unwrap();
    backend
        .put_file_system(&file_system(owner.clone()))
        .unwrap();
    backend
        .put_object_and_version(
            &object,
            &crate::metadata_records::object_version_record(&object, Some(5)),
        )
        .unwrap();
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(network.add_node(owner), content)
        .with_metadata_backend(backend)
        .unwrap();
    let mut request = DfsFileHistoryPinRequest {
        path: object.path.clone(),
        identity: DfsFileVersionIdentity {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
        },
        operation: DfsFileHistoryPinOperation::Capture {
            pin_id: "before-edit".into(),
        },
        operation_id: "capture-before-edit".into(),
    };
    dfs.file_history_pin(request.clone()).unwrap();
    let backend = dfs.metadata_backend().unwrap();
    assert_eq!(
        backend.get_object(&object.object_id).unwrap(),
        Some(object.clone())
    );
    assert!(
        backend
            .get_version(&object.object_id, &object.version)
            .unwrap()
            .unwrap()
            .retained
            .is_some()
    );
    assert_eq!(
        backend
            .version_pins_for_object(&object.object_id)
            .unwrap()
            .len(),
        1
    );
    assert!(
        backend
            .conflicts_for_object(&object.object_id)
            .unwrap()
            .is_empty()
    );
    request.operation = DfsFileHistoryPinOperation::Unpin {
        pin_id: "before-edit".into(),
    };
    request.operation_id = "release-before-edit".into();
    dfs.file_history_pin(request).unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&object.object_id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap(),
        Some(object)
    );
}

#[test]
fn file_history_cli_capture_defaults_to_observing_current_identity() {
    let args = [
        "history",
        "capture",
        "/users/document",
        "--pin",
        "before-edit",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    let command = crate::cli::parse_dfs_command(&args).unwrap();
    let crate::cli::DfsCliCommand::HistoryPin(options) = command else {
        panic!("history command");
    };
    assert_eq!(options.path, "/users/document");
    assert!(options.object_id.is_none());
    assert!(options.version.is_none());
    assert_eq!(
        options.operation,
        DfsFileHistoryPinOperation::Capture {
            pin_id: "before-edit".into()
        }
    );
    let args = [
        "history",
        "unpin",
        "/users/document",
        "--pin",
        "before-edit",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    assert!(crate::cli::parse_dfs_command(&args).is_err());
}
