use super::*;

#[test]
fn file_history_repin_after_compaction_has_fresh_repair_ownership() {
    let root = temp_root("history-compacted-repair-owner");
    let owner = NodeId::from_legacy_named("history-compacted-owner").unwrap();
    let a = NodeId::from_legacy_named("history-compacted-a").unwrap();
    let b = NodeId::from_legacy_named("history-compacted-b").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let current = dfs
        .posix_write_file(
            "/services/compacted-history",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let request = preserve_request(&current, "preserve-before-compaction");
    let DfsFileHistoryPreserveOutcome::Retained { identity } = dfs
        .preserve_local_file_version(
            request.clone(),
            &b"retained"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner, a, b],
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
    else {
        panic!("W reached");
    };
    let original_intent = dfs.coordinator.repair_jobs()[0].intent_id.clone();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: "release-before-compaction".into(),
    })
    .unwrap();
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
            .get_intent_receipt(&current.object_id, &original_intent)
            .unwrap()
            .is_some()
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .is_some()
    );
    dfs.begin_message_committed_work();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: request.pin_id,
        },
        operation_id: "repin-after-compaction".into(),
    })
    .unwrap();
    let jobs = dfs.coordinator.repair_jobs();
    assert_eq!(
        jobs.len(),
        2,
        "the retained W=1/N=3 obligation must survive intent compaction"
    );
    assert!(
        jobs.iter()
            .all(|job| job.intent_id != original_intent && job.version == identity.version)
    );
    assert_eq!(
        dfs.coordinator.intent(&jobs[0].intent_id).unwrap().policy,
        DfsReplicationPolicy::new(1, 3).unwrap()
    );
    assert_eq!(dfs.take_message_committed_repair_queue_records().len(), 2);
    assert_eq!(
        dfs.posix_stat(&current.path).unwrap().version,
        current.version
    );
    assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"canonical");
}

#[test]
fn file_history_remote_retained_reads_and_recovery_ignore_legacy_conflict_selection() {
    let root = temp_root("history-legacy-remote-source");
    let owner = NodeId::from_legacy_named("history-legacy-owner").unwrap();
    let holder = NodeId::from_legacy_named("history-legacy-holder").unwrap();
    let network = InProcessNetwork::new();
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("original-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    primary.bootstrap_distributed_sifs().unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let current = primary
        .posix_write_file("/services/legacy-source", b"canonical", options.clone())
        .unwrap();
    let bytes = b"explicitly preserved local operation";
    let DfsFileHistoryPreserveOutcome::Retained { identity } = primary
        .preserve_local_file_version(
            preserve_request(&current, "legacy-explicit-preserve"),
            &bytes[..],
            options.clone(),
        )
        .unwrap()
    else {
        panic!("capture reached W");
    };
    let hash = content_hash(bytes);
    let replica = NetworkedDfsService::new(
        network.add_node(holder.clone()),
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    );
    replica
        .store_command_bytes(&identity.object_id, &identity.version, &hash, bytes)
        .unwrap();
    replica
        .store_command_bytes(
            &current.object_id,
            &current.version,
            current.content_hash.as_deref().unwrap(),
            b"canonical",
        )
        .unwrap();
    primary
        .put_metadata_receipt(
            &current.file_system_id,
            &current.object_id,
            &current.version,
            &holder,
            current.content_hash.as_deref().unwrap(),
        )
        .unwrap();
    primary
        .put_metadata_receipt(
            &identity.file_system_id,
            &identity.object_id,
            &identity.version,
            &holder,
            &hash,
        )
        .unwrap();
    let conflict = DfsConflictRecord {
        conflict_id: "legacy-conflict".into(),
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        path: current.path.clone(),
        base_version: None,
        local_version: current.version.clone(),
        remote_version: identity.version.clone(),
        detected_by_node_id: owner.clone(),
        reason: "imported legacy conflict".into(),
        local_content_hash: current.content_hash.clone(),
        remote_content_hash: Some(hash.clone()),
        created_by_intent_id: None,
        strategy: Some("legacy".into()),
        status: DfsConflictStatus::Open,
        detected_at: time::OffsetDateTime::now_utc(),
    };
    primary
        .metadata_backend
        .as_mut()
        .unwrap()
        .put_conflict(&conflict)
        .unwrap();
    drop(primary);
    // The authoritative node has metadata and legacy conflict markers, but
    // its only readable copy of the retained bytes is on the data-only peer.
    let mut primary = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("empty-primary-content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    assert!(
        primary
            .open_content_hash_stream(
                &identity.object_id,
                &identity.version,
                &hash,
                Some(bytes.len() as u64)
            )
            .is_err()
    );
    assert!(matches!(
        primary
            .read_local_object_with_state(&current.object_id, false)
            .unwrap(),
        NetworkedDfsReadOutcome::Conflicted { .. }
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let replica_stop = stop.clone();
    let pump = std::thread::spawn(move || pump_sifs_service_until_stop(replica, replica_stop));
    let result = (|| {
        let range = primary.read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 0,
            max_bytes: 64,
        })?;
        let DfsFileHistoryReadOutcome::Found { data } = range else {
            return Err(DfsError::Metadata {
                message: format!("explicit retained read was blocked: {range:?}"),
            });
        };
        let exact = primary.read_object_version_content(
            &identity.object_id,
            &identity.version,
            &hash,
            Some(bytes.len() as u64),
        )?;
        let exact_current = primary.read_object_version_content(
            &current.object_id,
            &current.version,
            current.content_hash.as_deref().unwrap(),
            current.size_bytes,
        )?;
        let restored = primary.restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: "/services/recovered-legacy-content".into(),
                expected_destination: None,
                operation_id: "legacy-retained-recovery".into(),
            },
            options.clone(),
        )?;
        let recovered =
            primary
                .posix_read_file(&restored.path)
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?;
        let source_before_replace = primary
            .current_read_object_metadata(&current.object_id)?
            .version;
        let replaced = primary.restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: current.path.clone(),
                expected_destination: Some(history_identity(&current)),
                operation_id: "legacy-retained-replace".into(),
            },
            options,
        )?;
        let marker_before_maintenance = primary
            .metadata_backend()
            .unwrap()
            .get_conflict(&current.object_id, &conflict.conflict_id)?;
        let maintenance = primary.reconcile_stale_conflicts_bounded(8)?;
        let replaced_bytes =
            primary
                .posix_read_file(&current.path)
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?;
        Ok::<_, DfsError>((
            data.bytes,
            exact,
            exact_current,
            restored,
            recovered,
            source_before_replace,
            replaced,
            marker_before_maintenance,
            maintenance,
            replaced_bytes,
        ))
    })();
    stop.store(true, Ordering::SeqCst);
    let replica = pump.join().unwrap();
    let (
        range,
        exact,
        exact_current,
        restored,
        recovered,
        source_before_replace,
        replaced,
        marker_before_maintenance,
        maintenance,
        replaced_bytes,
    ) = result.unwrap();
    assert_eq!(range, bytes);
    assert_eq!(exact, bytes);
    assert_eq!(exact_current, b"canonical");
    assert_eq!(recovered, bytes);
    assert_ne!(restored.committed.object_id, current.object_id);
    assert_eq!(
        source_before_replace, current.version,
        "recovery elsewhere never rewrites its source"
    );
    assert_eq!(replaced.committed.object_id, current.object_id);
    assert_ne!(replaced.committed.version, current.version);
    assert_ne!(replaced.committed.version, identity.version);
    assert_eq!(marker_before_maintenance, Some(conflict));
    assert_eq!(maintenance.superseded_conflict_count, 1);
    assert_eq!(replaced_bytes, bytes);
    assert_eq!(
        primary.posix_stat(&current.path).unwrap().version,
        replaced.committed.version
    );
    assert!(replica.metadata.object(&current.object_id).is_none());
}

#[test]
fn file_history_current_artifact_is_readable_without_becoming_a_capture() {
    let root = temp_root("history-current-artifact");
    let owner = NodeId::from_legacy_named("history-artifact-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let stat = dfs
        .posix_write_file(
            "/services/artifact",
            b"artifact bytes",
            DfsWriteOptions::replicated_eventual(vec![owner]),
        )
        .unwrap();
    let mut artifact = dfs.current_read_object_metadata(&stat.object_id).unwrap();
    artifact.kind = DfsObjectKind::Artifact;
    dfs.add_object_metadata(artifact).unwrap();
    assert_eq!(
        dfs.read_object_version_content(
            &stat.object_id,
            &stat.version,
            stat.content_hash.as_deref().unwrap(),
            stat.size_bytes,
        )
        .unwrap(),
        b"artifact bytes"
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap()
            .retained
            .is_none()
    );
}

#[test]
fn file_history_repin_reactivates_only_the_retained_versions_repair() {
    let root = temp_root("history-repin-repair");
    let owner = NodeId::from_legacy_named("history-repin-owner").unwrap();
    let remote = NodeId::from_legacy_named("history-repin-offline").unwrap();
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap();
    dfs.bootstrap_distributed_sifs().unwrap();
    let stat = dfs
        .posix_write_file(
            "/services/repin",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let request = preserve_request(&stat, "repin-preserve");
    let DfsFileHistoryPreserveOutcome::Retained { identity } = dfs
        .preserve_local_file_version(
            request.clone(),
            &b"retained alternative"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote.clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
    else {
        panic!("local receipt reaches W");
    };
    let jobs = dfs.coordinator.repair_jobs().to_vec();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].version, identity.version);
    let intent_id = jobs[0].intent_id.clone();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "independent-root".into(),
        },
        operation_id: "repin-second-root".into(),
    })
    .unwrap();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: "repin-release".into(),
    })
    .unwrap();
    assert_eq!(
        dfs.coordinator.repair_jobs(),
        jobs.as_slice(),
        "another pin still owns the work"
    );
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "independent-root".into(),
        },
        operation_id: "repin-release-last-root".into(),
    })
    .unwrap();
    dfs.retry_pending_repairs_bounded(8).unwrap();
    assert!(
        dfs.coordinator.repair_jobs().is_empty(),
        "released history cannot retain executable repair jobs"
    );
    assert!(dfs.durable_repair_queue_records().is_empty());
    assert!(!dfs.writes.pending_writes.contains_key(&intent_id));
    // A cold lane can encounter a stale queue row after the pin was removed.
    // Later generic receipt restoration must not make that row executable.
    dfs.coordinator = DistributedEventualCoordinator::new();
    dfs.restore_pending_write_for_intent_if_missing(&intent_id, &stat.object_id)
        .unwrap();
    dfs.coordinator
        .restore_durable_receipt(&intent_id, owner.clone())
        .unwrap();
    assert!(dfs.coordinator.repair_jobs().is_empty());
    dfs.coordinator.begin_message_repair_job_capture();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: request.pin_id,
        },
        operation_id: "repin-acquire".into(),
    })
    .unwrap();
    assert_eq!(
        dfs.coordinator.repair_jobs(),
        jobs.as_slice(),
        "an explicit root reactivates its missing N replica"
    );
    assert!(dfs.writes.pending_writes.contains_key(&intent_id));
    assert_eq!(
        dfs.take_message_committed_repair_queue_records().len(),
        1,
        "the pin publication must durably enqueue newly actionable work"
    );
    let mut replica = NetworkedDfsService::new(
        network.add_node(remote.clone()),
        LocalContentStore::new(root.join("replica-content")).unwrap(),
    );
    let scheduled = dfs.retry_pending_repairs_bounded(8).unwrap();
    assert_eq!(scheduled.len(), 1);
    let replica_events = replica.process_incoming().unwrap();
    let primary_events = dfs.process_incoming().unwrap();
    assert!(
        dfs.coordinator.repair_jobs().is_empty(),
        "replica: {replica_events:?}; primary: {primary_events:?}"
    );
    assert!(!dfs.writes.pending_writes.contains_key(&intent_id));
    assert_eq!(
        replica
            .content
            .read_object_version(identity.object_id.as_str(), identity.version.as_str())
            .unwrap()
            .1,
        b"retained alternative"
    );
    assert!(
        replica.metadata.object(&stat.object_id).is_none(),
        "retained repair never publishes a second current view"
    );
    assert_eq!(dfs.posix_read_file(&stat.path).unwrap(), b"canonical");
}

#[test]
fn file_history_repin_after_restart_does_not_rewind_a_canonical_write_intent() {
    let root = temp_root("history-repin-canonical-intent");
    let owner = NodeId::from_legacy_named("history-repin-canonical-owner").unwrap();
    let remote = NodeId::from_legacy_named("history-repin-canonical-remote").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let first = dfs
        .posix_write_file(
            "/services/canonical-repin",
            b"old",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    capture_history(&mut dfs, &first);
    let current = dfs
        .posix_write_file(
            &first.path,
            b"current",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: first.path.clone(),
        identity: history_identity(&first),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "canonical-release".into(),
    })
    .unwrap();
    assert!(dfs.coordinator.repair_jobs().is_empty());
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    assert!(dfs.coordinator.repair_jobs().is_empty());
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: first.path.clone(),
        identity: history_identity(&first),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "canonical-repin".into(),
    })
    .unwrap();
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
    assert_eq!(dfs.coordinator.repair_jobs()[0].version, first.version);
    assert_eq!(
        dfs.coordinator
            .committed_version(&current.file_system_id, &current.object_id),
        Some(&current.version)
    );
    dfs.posix_write_file(
        &current.path,
        b"next",
        DfsWriteOptions::replicated_eventual(vec![owner]),
    )
    .unwrap();
    assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"next");
}

#[test]
fn file_history_expired_rule_retires_repairs_even_during_retry_backoff() {
    let root = temp_root("history-expired-repair");
    let owner = NodeId::from_legacy_named("history-expired-owner").unwrap();
    let remote = NodeId::from_legacy_named("history-expired-offline").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let first = dfs
        .posix_write_file(
            "/services/expiring-history",
            b"old",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let mut fs = dfs
        .metadata_backend()
        .unwrap()
        .get_file_system(&first.file_system_id)
        .unwrap()
        .unwrap();
    fs.file_history.rules.push(DfsFileHistoryRule {
        path: first.path.clone(),
        retention: DfsFileHistoryRetention {
            keep_last: 0,
            keep_for_seconds: 1,
        },
    });
    dfs.add_file_system(fs).unwrap();
    dfs.posix_write_file(
        &first.path,
        b"current",
        DfsWriteOptions::replicated_eventual(vec![owner]),
    )
    .unwrap();
    let retained = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&first.object_id, &first.version)
        .unwrap()
        .unwrap();
    assert!(retained.retained.unwrap().policy_capture.is_some());
    let mut records = dfs.durable_repair_queue_records();
    assert_eq!(records.len(), 1);
    records[0].attempts = 1;
    records[0].next_retry_unix_nanos =
        crate::metadata_records::dfs_now_unix_nanos().saturating_add(60_000_000_000);
    // Exercise the production wall-clock expiry boundary while the transport
    // retry remains far in the future; no network work should run here.
    std::thread::sleep(Duration::from_secs(2));
    assert!(
        dfs.retry_pending_repairs_bounded_with_attempts(8, &records)
            .unwrap()
            .is_empty()
    );
    assert!(dfs.coordinator.repair_jobs().is_empty());
    assert!(dfs.durable_repair_queue_records().is_empty());
    assert_eq!(dfs.posix_read_file(&first.path).unwrap(), b"current");
}

#[test]
fn file_history_pin_receipt_replay_recovers_its_repair_queue_handoff() {
    let root = temp_root("history-pin-replay-repair");
    let owner = NodeId::from_legacy_named("history-pin-replay-owner").unwrap();
    let remote = NodeId::from_legacy_named("history-pin-replay-offline").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let stat = dfs
        .posix_write_file(
            "/services/replay-pin",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Retained { identity } = dfs
        .preserve_local_file_version(
            preserve_request(&stat, "replay-preserve"),
            &b"retained"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
    else {
        panic!("W reached");
    };
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
    let control_plane = SharedDfsControlPlaneKvClient::new(kv);
    let mut dfs = dfs.with_control_plane(control_plane.clone());
    let request = DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity,
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "second-root".into(),
        },
        operation_id: "publish-root-before-queue-handoff".into(),
    };
    let first = dfs.file_history_pin(request.clone()).unwrap();
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    drop(dfs);
    // Reopen exactly like a lazy request lane: no repair inventory or local
    // queue survived, but the completed namespace request is durable.
    let network = InProcessNetwork::new();
    let mut dfs = NetworkedDfsService::new(
        network.add_node(owner),
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend_lazy(open_root_local_metadata_backend(root.join("metadata")).unwrap())
    .unwrap()
    .with_control_plane(control_plane);
    assert!(dfs.coordinator.repair_jobs().is_empty());
    dfs.begin_message_committed_work();
    assert_eq!(dfs.file_history_pin(request).unwrap(), first);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    assert_eq!(dfs.coordinator.repair_jobs().len(), 1);
    assert_eq!(dfs.take_message_committed_repair_queue_records().len(), 1);
    assert!(dfs.take_message_committed_work().repair_maintenance);
    assert_eq!(
        dfs.current_read_object_metadata(&stat.object_id)
            .unwrap()
            .version,
        stat.version
    );
}

#[test]
fn file_history_exact_version_read_rejects_unretained_and_unanchored_bytes() {
    let root = temp_root("history-exact-read-admission");
    let owner = NodeId::from_legacy_named("history-exact-reader").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let first = dfs
        .posix_write_file("/services/exact-read", b"first", options.clone())
        .unwrap();
    let second = dfs
        .posix_write_file("/services/exact-read", b"second", options)
        .unwrap();
    assert_eq!(
        dfs.read_object_version_content(
            &second.object_id,
            &second.version,
            second.content_hash.as_deref().unwrap(),
            second.size_bytes,
        )
        .unwrap(),
        b"second"
    );
    assert!(
        dfs.read_object_version_content(
            &first.object_id,
            &first.version,
            first.content_hash.as_deref().unwrap(),
            first.size_bytes,
        )
        .is_err(),
        "an internal superseded generation is not user-readable history"
    );
    assert!(
        dfs.read_object_version_content(
            &DfsObjectId::from_legacy_named("unanchored-object").unwrap(),
            &first.version,
            first.content_hash.as_deref().unwrap(),
            first.size_bytes,
        )
        .is_err(),
        "a known blob hash is not authority to read arbitrary content"
    );
}

#[test]
fn file_history_exact_version_reads_use_primary_and_remote_content() {
    for codec in [
        DfsMetadataNamespacePrepareCodec::PackedV2,
        DfsMetadataNamespacePrepareCodec::PackedV3,
    ] {
        let root = temp_root(&format!("history-exact-remote-{codec:?}"));
        let owner = NodeId::from_legacy_named("history-exact-primary").unwrap();
        let reader = NodeId::from_legacy_named("history-exact-client").unwrap();
        let network = InProcessNetwork::new();
        let mut primary = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("primary-content")).unwrap(),
        )
        .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
        .unwrap();
        primary.bootstrap_distributed_sifs().unwrap();
        let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
        let bytes = vec![42; crate::file_history_query::DFS_FILE_HISTORY_RANGE_LIMIT as usize + 31];
        let first = primary
            .posix_write_file("/services/exact-remote", &bytes, options.clone())
            .unwrap();
        let stale = primary
            .current_read_object_metadata(&first.object_id)
            .unwrap();
        if codec == DfsMetadataNamespacePrepareCodec::PackedV3 {
            capture_history(&mut primary, &first);
        }
        let current = primary
            .posix_write_file(&first.path, b"current bytes", options.clone())
            .unwrap();
        let empty = primary
            .posix_write_file("/services/exact-empty", b"", options)
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
                namespace_prepare_codec: codec,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            None,
        )
        .unwrap();
        let shared = SharedDfsControlPlaneKvClient::new(kv);
        let primary = primary.with_control_plane(shared.clone());
        let mut client = NetworkedDfsService::new(
            network.add_node(reader),
            LocalContentStore::new(root.join("client-content")).unwrap(),
        )
        .with_control_plane(shared);
        // A cached old head and exact old bytes must not grant read access.
        client.add_transient_object_metadata(stale).unwrap();
        if codec == DfsMetadataNamespacePrepareCodec::PackedV2 {
            client
                .store_command_bytes(
                    &first.object_id,
                    &first.version,
                    first.content_hash.as_deref().unwrap(),
                    &bytes,
                )
                .unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let primary_stop = stop.clone();
        let pump = std::thread::spawn(move || pump_sifs_service_until_stop(primary, primary_stop));
        let result = (|| {
            let read_current = client.read_object_version_content(
                &current.object_id,
                &current.version,
                current.content_hash.as_deref().unwrap(),
                current.size_bytes,
            )?;
            assert_eq!(read_current, b"current bytes");
            assert!(
                client
                    .read_object_version_content(
                        &current.object_id,
                        &current.version,
                        first.content_hash.as_deref().unwrap(),
                        current.size_bytes,
                    )
                    .is_err()
            );
            assert!(
                client
                    .read_object_version_content(
                        &current.object_id,
                        &current.version,
                        current.content_hash.as_deref().unwrap(),
                        Some(1),
                    )
                    .is_err()
            );
            assert!(
                client
                    .read_object_version_content(
                        &empty.object_id,
                        &empty.version,
                        empty.content_hash.as_deref().unwrap(),
                        Some(0),
                    )?
                    .is_empty()
            );
            let retained = client.read_object_version_content(
                &first.object_id,
                &first.version,
                first.content_hash.as_deref().unwrap(),
                first.size_bytes,
            );
            if codec == DfsMetadataNamespacePrepareCodec::PackedV3 {
                assert_eq!(retained?, bytes);
                // Keep a full local copy after a successful remote read, then
                // release its root. Availability must not imply admission.
                client.store_command_bytes(
                    &first.object_id,
                    &first.version,
                    first.content_hash.as_deref().unwrap(),
                    &bytes,
                )?;
                client.file_history_pin(DfsFileHistoryPinRequest {
                    path: first.path.clone(),
                    identity: history_identity(&first),
                    operation: DfsFileHistoryPinOperation::Unpin {
                        pin_id: "checkpoint".into(),
                    },
                    operation_id: "exact-read-release-checkpoint".into(),
                })?;
                assert!(
                    client
                        .read_object_version_content(
                            &first.object_id,
                            &first.version,
                            first.content_hash.as_deref().unwrap(),
                            first.size_bytes,
                        )
                        .is_err(),
                    "cached historical bytes do not survive release of their explicit root"
                );
            } else {
                assert!(
                    retained.is_err(),
                    "older shard codecs still admit only the exact current version"
                );
            }
            Ok::<_, DfsError>(())
        })();
        stop.store(true, Ordering::SeqCst);
        pump.join().unwrap();
        result.unwrap();
    }
}

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
