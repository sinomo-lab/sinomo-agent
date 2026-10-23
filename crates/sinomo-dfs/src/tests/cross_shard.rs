//! Cross shard regression tests.

use super::*;

// Read-only physical DATA inspection; no opener recovery or publication guard.
fn hardlink_read_set_snapshot(
    runtime: &crate::test_runtime::DfsTestRuntime,
    node: &NodeId,
    shard: &str,
) -> std::result::Result<DfsMetadataTableSnapshot, MetadataBackendError> {
    let shard = DfsMetadataShardId::new(shard);
    let root = crate::local_dfs_metadata_shard_path(runtime.data_dir(node), &shard);
    Ok(LocalSobjMetadataBackend::open_for_inspection(shard, root)?.export_table_snapshot())
}

fn recorded_hardlink_alias_frame(
    snapshot: &DfsMetadataTableSnapshot,
    request_id: &str,
) -> DfsNamespaceJobFrame {
    let frames = snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .flat_map(|record| record.payload.metadata_effects.iter())
        .filter_map(|effect| match effect {
            DfsPreparedMetadataEffect::PutNamespaceJob { job }
                if job
                    .origin
                    .as_ref()
                    .is_some_and(|origin| origin.request_id == request_id) =>
            {
                Some(job)
            }
            _ => None,
        })
        .flat_map(|job| job.pending_frames.iter())
        .filter(|frame| {
            matches!(
                frame,
                DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit { .. }
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let first = frames
        .first()
        .expect("original durable alias frame remains in its immutable prepares");
    assert!(
        frames.iter().all(|frame| frame == first),
        "every saved occurrence carries the same original comparisons"
    );
    first.clone()
}

#[test]
fn durable_hardlink_alias_job_fences_parent_changes_and_preserves_original_read_set() {
    let mut cluster = sifs_two_primary_cluster("native-hardlink-original-parent-conflict");
    let canonical_path = "/users/original-parent-canonical";
    let alias_path = "/services/original-parent-alias";
    cluster
        .replica
        .posix_write_file(
            canonical_path,
            b"original parent bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster
        .replica
        .posix_hardlink(canonical_path, alias_path)
        .unwrap();
    let alias = cluster
        .replica
        .posix_namespace_lstat_distributed(alias_path)
        .unwrap();
    let parent = hardlink_read_set_snapshot(
        &cluster._runtime,
        &cluster.services_primary_node,
        "services",
    )
    .unwrap()
    .tables
    .objects
    .into_iter()
    .find(|row| row.path == "/services")
    .unwrap();
    let original_canonical = cluster
        .replica
        .posix_lstat_distributed(canonical_path)
        .unwrap();
    let operation_id = sinomo_core::Id::generate().to_string();
    let pause = cluster
        ._runtime
        .pause_remote_namespace_phase(&cluster.services_primary_node)
        .unwrap();
    let mut requester = cluster._runtime.client(&cluster.replica_node).unwrap();
    std::thread::scope(|scope| {
        let request_id = operation_id.clone();
        let expected = DfsNamespaceObjectPrecondition {
            object_id: alias.object_id.clone(),
            version: alias.version.clone(),
        };
        let pending = scope
            .spawn(move || requester.posix_unlink_if_generation(alias_path, request_id, expected));
        // Preserve all errors and release the genuine invocation before any assertion.
        let captured = pause.wait_captured(Duration::from_secs(5));
        let observation = (|| -> std::result::Result<_, String> {
            let command = captured.as_ref().map_err(Clone::clone)?.clone();
            let before = hardlink_read_set_snapshot(
                &cluster._runtime,
                &cluster.services_primary_node,
                "services",
            )
            .map_err(|error| error.to_string())?;
            let mutation = cluster
                .users_primary
                .posix_mkdir("/services/independent-parent-change");
            let after = hardlink_read_set_snapshot(
                &cluster._runtime,
                &cluster.services_primary_node,
                "services",
            )
            .map_err(|error| error.to_string())?;
            Ok((command, before, mutation, after))
        })();
        pause.release();
        let outcome = pending.join().unwrap();
        let (command, before, mutation, after) = observation.unwrap();
        let DfsNetworkMessage::NamespaceRequest(command) = command.routed_message.message else {
            panic!("captured actual namespace target frame");
        };
        assert!(matches!(
            command.operation,
            DfsNamespaceOperation::InstallHardlinkTargetFence { .. }
        ));
        assert!(
            matches!(&mutation, Err(DfsPosixError::Dfs(DfsError::Busy { path })) if path == "/services"),
            "the actual parent-touching mutation must be fenced: {mutation:?}"
        );
        let recorded = recorded_hardlink_alias_frame(&before, &operation_id);
        let DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit {
            expected_object_versions,
            expected_path_occupants,
            shard_id,
            ..
        } = &recorded
        else {
            unreachable!()
        };
        assert_eq!(shard_id, &DfsMetadataShardId::new("services"));
        assert!(
            expected_object_versions
                .iter()
                .any(|row| row.object_id == parent.object_id
                    && row.expected_version.as_ref() == Some(&parent.version))
        );
        assert!(
            expected_path_occupants
                .iter()
                .any(|row| crate::namespace::metadata_path(&row.path)
                    == crate::namespace::metadata_path("/services")
                    && row.expected_object_id.as_ref() == Some(&parent.object_id))
        );
        let observed_parent = after
            .tables
            .objects
            .iter()
            .find(|row| row.object_id == parent.object_id)
            .unwrap();
        assert_eq!(
            observed_parent, &parent,
            "denied sibling creation leaves the complete original parent untouched"
        );
        assert_eq!(
            recorded_hardlink_alias_frame(&after, &operation_id),
            recorded
        );
        assert_eq!(outcome.unwrap().kind, DfsObjectKind::Tombstone);
        let completed = hardlink_read_set_snapshot(
            &cluster._runtime,
            &cluster.services_primary_node,
            "services",
        )
        .unwrap();
        assert_eq!(
            recorded_hardlink_alias_frame(&completed, &operation_id),
            recorded
        );
        let current_alias = completed
            .tables
            .objects
            .iter()
            .find(|row| row.object_id == alias.object_id)
            .unwrap();
        assert_eq!(current_alias.kind, DfsObjectKind::Tombstone);
        assert_ne!(current_alias.version, alias.version);
        let completed_parent = completed
            .tables
            .objects
            .iter()
            .find(|row| row.object_id == parent.object_id)
            .unwrap();
        assert_ne!(
            completed_parent.version, parent.version,
            "only the original successful alias mutation advances its parent"
        );
        let canonical =
            hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
                .unwrap();
        let current_canonical = canonical
            .tables
            .objects
            .iter()
            .find(|row| row.object_id == original_canonical.object_id)
            .unwrap();
        assert_ne!(current_canonical.version, original_canonical.version);
        assert!(current_canonical.changed_at_unix_nanos > original_canonical.changed_at_unix_nanos);
        assert_eq!(
            cluster
                .replica
                .posix_stat_distributed(canonical_path)
                .unwrap()
                .link_count,
            1
        );
    });
    drop(pause);
    cluster.close();
}
#[test]
fn returning_adoption_checks_the_physical_eviction_head_before_publication() {
    let node = NodeId::generate();
    let (mut runtime, mut service) = native_sifs_service("native-returning-adoption", node.clone());
    let original = service
        .posix_write_file(
            "/services/old-location",
            b"returning object",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    service
        .posix_rename("/services/old-location", "/users/returning-source")
        .unwrap();
    let eviction = service
        .metadata_backend()
        .unwrap()
        .get_object_on_shard(&DfsMetadataShardId::new("services"), &original.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(eviction.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        eviction.tombstone_provenance,
        Some(DfsTombstoneProvenance::SourceEviction { .. })
    ));
    let original_cut = hardlink_read_set_snapshot(&runtime, &node, "services").unwrap();
    let old_path_occupant = original_cut
        .tables
        .objects
        .iter()
        .find(|row| row.path == "/services/old-location" && row.object_id != original.object_id)
        .expect("the first move durably tombstones its original path with a separate identity")
        .clone();
    assert_eq!(old_path_occupant.kind, DfsObjectKind::Tombstone);
    let returned = service
        .posix_rename("/users/returning-source", "/services/returned-location")
        .unwrap();
    let returned_cut = hardlink_read_set_snapshot(&runtime, &node, "services").unwrap();
    let adoption = returned_cut
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| {
            record.status == DfsPreparedOperationStatus::Committed
                && record.payload.transaction.objects.iter().any(|row| {
                    row.object_id == original.object_id
                        && row.version == returned.version
                        && row.path == "/services/returned-location"
                })
        })
        .expect("actual committed returning adoption retains its complete original read set");
    for original_head in [&eviction, &old_path_occupant] {
        assert!(
            adoption
                .payload
                .expected_object_versions
                .iter()
                .any(|expected| {
                    expected.object_id == original_head.object_id
                        && expected.expected_version.as_ref() == Some(&original_head.version)
                })
        );
    }
    assert!(
        adoption
            .payload
            .expected_path_occupants
            .iter()
            .any(|expected| {
                crate::namespace::metadata_path(&expected.path)
                    == crate::namespace::metadata_path("/services/old-location")
                    && expected.expected_object_id.as_ref() == Some(&old_path_occupant.object_id)
            })
    );
    assert!(
        adoption
            .payload
            .expected_path_occupants
            .iter()
            .any(|expected| {
                crate::namespace::metadata_path(&expected.path)
                    == crate::namespace::metadata_path("/services/returned-location")
                    && expected.expected_object_id.is_none()
            })
    );
    let current_head = returned_cut
        .tables
        .objects
        .iter()
        .find(|row| row.object_id == original.object_id)
        .unwrap()
        .clone();
    assert_eq!(current_head.version, returned.version);
    // A native kernel attempt carrying the genuine earlier physical eviction
    // head must still reject after the ordinary return advanced that head.
    // Its body is the actual current row; no receipt, source witness or
    // authority is invented, and the unchanged CAS must reject before append.
    let config = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|cfg| cfg.shard_id == "services")
        .unwrap();
    let mut owner = runtime.metadata_primary_service("/services").unwrap();
    let stale_payload = DfsPreparedOperationPayload::for_namespace_transaction(
        DfsPreparedOperationScope {
            file_system_id: original.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("services"),
            owner_epoch: config.epoch,
            operation_id: format!("returning-old-eviction:{}", sinomo_core::Id::generate()),
        },
        vec![DfsNamespaceObjectVersionExpectation {
            object_id: original.object_id.clone(),
            expected_version: Some(eviction.version.clone()),
        }],
        vec![DfsNamespacePathOccupantExpectation {
            path: current_head.path.clone(),
            expected_object_id: Some(current_head.object_id.clone()),
        }],
        DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            transaction_id: format!("returning-current-row:{}", sinomo_core::Id::generate()),
            file_system_id: original.file_system_id.clone(),
            objects: vec![current_head],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        },
        Vec::new(),
        None,
        None,
        Vec::new(),
        None,
        None,
    )
    .unwrap();
    let denied = owner
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(stale_payload, None)
        .unwrap_err();
    assert!(
        matches!(denied, MetadataBackendError::Conflict { object_id, expected, current }
        if object_id == original.object_id && expected == eviction.version && current == returned.version)
    );
    drop(owner);
    assert_eq!(
        hardlink_read_set_snapshot(&runtime, &node, "services").unwrap(),
        returned_cut,
        "rejected old physical head must not append or change any table/prepare/prefix"
    );
    assert_eq!(returned.object_id, original.object_id);
    assert_ne!(returned.version, eviction.version);
    assert_eq!(
        service
            .posix_read_file("/services/returned-location")
            .unwrap(),
        b"returning object"
    );
    let local_old_path = service.posix_stat("/services/old-location");
    let current_old_path = service.posix_stat_distributed("/services/old-location");
    assert!(
        matches!(&current_old_path, Err(DfsPosixError::NotFound { .. })),
        "the current owner must reject the evicted path: {current_old_path:?}; local observation: {local_old_path:?}"
    );
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut reopened = runtime.client(&node).unwrap();
    assert_eq!(
        reopened
            .posix_stat_distributed("/services/returned-location")
            .unwrap()
            .object_id,
        original.object_id
    );
    assert_eq!(
        reopened
            .posix_read_file("/services/returned-location")
            .unwrap(),
        b"returning object"
    );
    drop(reopened);
    runtime.close().unwrap();
}

#[test]
fn content_reference_release_replay_survives_two_moves_between_disjoint_authorities() {
    reference_release_replay_after_two_moves(false);
}

#[test]
fn content_reference_remains_fenced_by_an_actual_pending_move() {
    let mut cluster = sifs_two_primary_cluster("native-reference-current-move-fence");
    let from = "/users/reference-moving";
    let to = "/services/reference-moved";
    let bytes = b"reference remains protected during the actual move";
    let stat = cluster
        .replica
        .posix_write_file(
            from,
            bytes,
            DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]),
        )
        .unwrap();
    let capture = DfsContentReferenceRequest {
        namespace: "org.example.reference-fence".into(),
        path: from.into(),
        identity: DfsRevisionReference {
            file_system_id: stat.file_system_id.clone(),
            object_id: stat.object_id.clone(),
            version: stat.version.clone(),
        },
        operation: DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "capture-root".into(),
            metadata: None,
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    cluster
        .replica
        .retain_content_revision(capture.clone())
        .unwrap();
    let retain = DfsContentReferenceRequest {
        operation: DfsContentReferenceOperation::Retain {
            reference_id: "during-move".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
        ..capture
    };
    assert!(!cluster.replica.content_reference_present(&retain).unwrap());
    let move_id = sinomo_core::Id::generate().to_string();
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.users_primary_node.clone()),
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    // Use the existing native carrier-failure choreography: the real owner
    // persists its job and returns admission, while the target remains blocked.
    let moved = cluster
        .users_primary
        .posix_rename_with_operation_id(from, to, &move_id);
    assert!(
        moved.is_err(),
        "the actual target carrier is unavailable: {moved:?}"
    );
    let parked = cluster
        .users_primary
        .metadata_backend()
        .unwrap()
        .list_namespace_jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.root_path == from && job.target_path.as_deref() == Some(to))
        .expect("the actual durable move retains its reservation");
    assert_eq!(parked.status, DfsNamespaceJobStatus::Running);
    // Observe from the independent native caller: the targeted source-to-target
    // carrier fault must not also intercept this caller's identity lookup.
    let root = cluster
        ._runtime
        .data_dir(&cluster.users_primary_node)
        .root();
    let observation = crate::native_boundary_diagnostics::capture(root.clone());
    let denied = cluster.replica.retain_content_revision(retain.clone());
    observation.dump(
        "reference-current-guard",
        serde_json::json!({
            "original_move": move_id, "job": parked, "reference": retain,
            "result": format!("{denied:?}"),
        }),
    );
    let observed: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.join("boundary-diagnostic-reference-current-guard.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(observed["dropped"], 0);
    assert!(
        observed["events"].as_array().unwrap().iter().any(|event| {
            event["kind"] == "metadata-query-path-blocked"
                && event["facts"]["node"] == serde_json::json!(cluster.users_primary_node)
                && event["facts"]["path"] == from
                && event["facts"]["locks"]
                    .as_str()
                    .is_some_and(|locks| locks.contains(&parked.job_id))
        }),
        "the exact live Main reservation must actually reject the read"
    );
    // Metadata-query rejection uses the existing string-bearing wire error.
    // The observed actor/job above establishes the cause independently of it.
    let busy = DfsPosixError::Dfs(DfsError::Busy { path: from.into() }).to_string();
    assert!(
        matches!(&denied, Err(DfsError::Metadata { message }) if message == &busy),
        "preserve the current authenticated-query rejection: {denied:?}"
    );
    drop(observation);
    cluster._runtime.clear_faults();
    let moved = cluster
        .users_primary
        .posix_rename_with_operation_id(from, to, &move_id)
        .expect("resume the same original move after restoring the actual carrier");
    assert_eq!(moved.object_id, stat.object_id);
    assert!(!cluster.replica.content_reference_present(&retain).unwrap());
    cluster
        .replica
        .retain_content_revision(retain.clone())
        .unwrap();
    assert!(cluster.replica.content_reference_present(&retain).unwrap());
    assert_eq!(
        cluster.replica.posix_read_file_distributed(to).unwrap(),
        bytes
    );
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster.close();
}

#[test]
fn content_reference_noop_release_replay_survives_two_moves_between_disjoint_authorities() {
    reference_release_replay_after_two_moves(true);
}

fn reference_release_replay_after_two_moves(no_op_release: bool) {
    let label = if no_op_release {
        "native-reference-noop-two-moves"
    } else {
        "native-reference-release-two-moves"
    };
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(label, 4);
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(label, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime
        .start_nodes()
        .expect("normal native membership and provider publication");
    for (shard, target) in [
        ("users", &nodes[0]),
        ("services", &nodes[1]),
        ("root", &nodes[2]),
    ] {
        let config = runtime
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|config| config.shard_id == shard)
            .unwrap();
        if config.primary_node_id != *target {
            let replicas =
                crate::control_plane::topology_preserving_metadata_shard_replicas(&config, target)
                    .unwrap();
            runtime
                .reassign_metadata_shard(&sifs_file_system_id(), shard, target.clone(), replicas)
                .unwrap();
        }
    }
    let mut requester = runtime.client(&nodes[3]).unwrap();
    let bytes = b"original reference data";
    let stat = requester
        .posix_write_file(
            "/users/reference-a",
            bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![nodes[0].clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let identity = DfsRevisionReference {
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        version: stat.version.clone(),
    };
    let capture = DfsContentReferenceRequest {
        namespace: "org.example.reference-moves".into(),
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "capture-root".into(),
            metadata: None,
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    requester.retain_content_revision(capture.clone()).unwrap();
    if !no_op_release {
        requester
            .retain_content_revision(DfsContentReferenceRequest {
                operation: DfsContentReferenceOperation::Retain {
                    reference_id: "reused-leaf".into(),
                },
                operation_id: sinomo_core::Id::generate().to_string(),
                ..capture.clone()
            })
            .unwrap();
    }
    requester
        .posix_rename("/users/reference-a", "/services/reference-b")
        .unwrap();
    let release = DfsContentReferenceRequest {
        operation: DfsContentReferenceOperation::Release {
            reference_id: "reused-leaf".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
        ..capture.clone()
    };
    let before = no_op_release.then(|| requester.revision_metadata(&identity).unwrap().unwrap());
    requester.retain_content_revision(release.clone()).unwrap();
    if let Some(before) = before {
        let after = requester.revision_metadata(&identity).unwrap().unwrap();
        assert_eq!(after.current_object, before.current_object);
        assert_eq!(after.version, before.version);
    }
    let retain = DfsContentReferenceRequest {
        operation: DfsContentReferenceOperation::Retain {
            reference_id: "reused-leaf".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
        ..capture
    };
    requester.retain_content_revision(retain.clone()).unwrap();
    requester.retain_content_revision(release.clone()).unwrap();
    assert!(requester.content_reference_present(&retain).unwrap());
    let transaction_id = format!("file-history:{}", release.operation_id);
    let middle = runtime.client(&nodes[1]).unwrap();
    let admitted = middle
        .metadata_backend()
        .unwrap()
        .get_namespace_transaction_receipt(&transaction_id)
        .unwrap()
        .unwrap();
    drop(middle);
    requester
        .posix_rename("/services/reference-b", "/reference-c")
        .unwrap();
    requester.retain_content_revision(release.clone()).unwrap();
    assert!(requester.content_reference_present(&retain).unwrap());
    let mut changed = release.clone();
    changed.operation = DfsContentReferenceOperation::Release {
        reference_id: "different-leaf".into(),
    };
    assert!(requester.retain_content_revision(changed).is_err());
    assert!(requester.content_reference_present(&retain).unwrap());
    assert_eq!(
        requester
            .read_file_version_content(
                &identity,
                stat.content_hash.as_deref().unwrap(),
                stat.size_bytes
            )
            .unwrap(),
        bytes
    );
    let final_owner = runtime.client(&nodes[2]).unwrap();
    let transferred = final_owner
        .metadata_backend()
        .unwrap()
        .get_namespace_transaction_receipt(&transaction_id)
        .unwrap()
        .unwrap();
    assert!(admitted.same_outcome_identity(&transferred));
    assert_eq!(transferred.content_reference_identity, Some(identity));
    assert_eq!(
        transferred.contribution_digest,
        Some(
            blake3::hash(&serde_json::to_vec(&release).unwrap())
                .to_hex()
                .to_string()
        )
    );
    drop((final_owner, requester));
    runtime.close().expect("normal component owners stopped");
}

#[test]
fn mkdir_all_cross_shard_error_names_the_actual_child_parent_pair() {
    let (runtime, service, current) = crate::test_support::current_native_metadata_service(
        "native-create-pair-validation",
        1,
        "/services",
    )
    .unwrap();
    let child_path = "/services/batched/one/two";
    let parent_path = "/services/batched/one";
    let hash = child_path
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    let mut child = current.clone();
    child.shard_id = "hash-selected-leaf".into();
    child.assignment = DfsMetadataShardAssignment::NameHashRange {
        start_inclusive: hash,
        end_exclusive: hash.wrapping_add(1),
    };
    let before = service
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&DfsMetadataShardId::new(current.shard_id.clone()))
        .unwrap();
    let error = crate::service_commit::ensure_atomic_create_parent_touch_shard(
        &current.file_system_id,
        &current.primary_node_id,
        child_path,
        parent_path,
        &[current.clone(), child],
    )
    .unwrap_err();
    assert!(error.to_string().contains(&format!(
        "created object {child_path} and parent {parent_path}"
    )));
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&DfsMetadataShardId::new(current.shard_id))
            .unwrap(),
        before
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn streamed_routed_write_binds_quarantine_after_hardlink_rebase() {
    let name = "native-streamed-hardlink-storage-binding";
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary = config.primary_node_id;
    drop(service);
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let requester = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&primary, "/services", DfsPlacementScope::Subtree, policy)
        .unwrap();
    let mut writer = runtime.client(&requester).unwrap();
    let target_path = "/services/streamed-target.bin";
    let alias_path = "/services/streamed-alias.bin";
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let original = writer
        .posix_write_file(target_path, b"actual original target", options.clone())
        .unwrap();
    writer.posix_hardlink(target_path, alias_path).unwrap();
    let alias = writer
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), alias_path)
        .unwrap()
        .unwrap();
    assert_eq!(alias.kind, DfsObjectKind::Hardlink);
    assert_eq!(
        alias.link_target_object_id,
        Some(original.object_id.clone())
    );
    assert_ne!(alias.object_id, original.object_id);
    let bytes = vec![0x5a; 2 * 1024 * 1024 + 8192];
    let before = runtime.rpc_observations().len();
    let committed = writer
        .posix_write_file_from_reader(alias_path, std::io::Cursor::new(&bytes), options)
        .unwrap();
    assert_eq!(committed.object_id, original.object_id);
    assert_eq!(writer.posix_read_file(target_path).unwrap(), bytes);
    assert_eq!(writer.posix_read_file(alias_path).unwrap(), bytes);
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(|event| event.source == requester
                && event.target == primary
                && event.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == crate::test_support::DfsTestRpcPhase::Completed)
    );
    for node in &nodes {
        let actual = runtime.client(node).unwrap();
        assert!(
            actual
                .content()
                .read_manifest(alias.object_id.as_str(), committed.version.as_str())
                .is_err(),
            "the streamed write cannot reserve an alias object version"
        );
        let stored = actual
            .content()
            .read_blob(committed.content_hash.as_ref().unwrap())
            .unwrap();
        assert_eq!(stored, bytes);
        assert_eq!(
            content_hash(&stored),
            committed.content_hash.clone().unwrap()
        );
    }
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn sifs_recursive_clear_preserves_hardlinks_outside_cleared_tree() {
    let node_id = NodeId::from_legacy_named("node_sifs_clear_hardlink").expect("node");
    let clear_root = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0";
    let file_path = "/services/sandbox/instances/sandbox_clear_hardlink/tmp/0/cache.txt";
    let outside_alias = "/services/sandbox/instances/sandbox_clear_hardlink/work/cache-alias.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_recursive_clear_preserves_hardlinks_outside_cleared_tree",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(clear_root).expect("mkdir clear root");
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_clear_hardlink/work")
        .expect("mkdir work");
    dfs.posix_write_file(
        file_path,
        b"cache",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write cache");
    dfs.posix_set_times(file_path, Some(12_345), Some(67_890))
        .expect("old file times");
    let before = dfs.posix_stat(file_path).expect("file stat");
    let object_id = before.object_id.clone();
    dfs.posix_hardlink(file_path, outside_alias)
        .expect("hardlink outside");

    dfs.posix_clear_dir_recursive(clear_root)
        .expect("clear recursive");

    assert!(matches!(
        dfs.posix_stat(file_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    let alias_stat = dfs.posix_stat(outside_alias).expect("alias survives");
    assert_eq!(alias_stat.object_id, object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        alias_stat.modified_at_unix_nanos, before.modified_at_unix_nanos,
        "clearing a hardlinked path must not change file content mtime"
    );
    assert!(
        alias_stat.changed_at_unix_nanos > before.changed_at_unix_nanos,
        "promoting a surviving hardlink alias must advance ctime because the canonical path changed"
    );
    assert_ne!(
        alias_stat.version, before.version,
        "promoting a surviving hardlink alias must advance the metadata version"
    );
    assert_eq!(
        dfs.posix_read_file(outside_alias).expect("read alias"),
        b"cache"
    );

    let reloaded = runtime.client(&node_id).unwrap();
    let alias_stat = reloaded
        .posix_stat(outside_alias)
        .expect("reloaded alias survives");
    assert_eq!(alias_stat.object_id, object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        reloaded
            .posix_read_file(outside_alias)
            .expect("reloaded read alias"),
        b"cache"
    );
}

#[test]
fn sifs_posix_hardlink_shares_content_and_link_count() {
    let root = temp_root("sifs-posix-hardlink-shared-content");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink").expect("node");
    let original = "/services/sandbox/instances/sandbox_hardlink/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_hardlink_shares_content_and_link_count",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_hardlink/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"original",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_set_times(original, Some(12_345), Some(67_890))
        .expect("set old times");
    dfs.posix_hardlink(original, link).expect("hardlink");

    let linked_ctime = dfs
        .posix_stat(original)
        .expect("original linked stat")
        .changed_at_unix_nanos;
    assert!(linked_ctime > 67_890);
    assert_eq!(
        dfs.posix_stat(original).expect("original stat").link_count,
        2
    );
    assert_eq!(dfs.posix_stat(link).expect("link stat").link_count, 2);
    dfs.posix_write_file(
        link,
        b"updated",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write link");
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"updated"
    );
    assert_eq!(dfs.posix_read_file(link).expect("read link"), b"updated");

    dfs.posix_unlink(link).expect("unlink alias");
    let remaining_stat = dfs.posix_stat(original).expect("remaining stat");
    assert_eq!(remaining_stat.link_count, 1);
    assert!(remaining_stat.changed_at_unix_nanos >= linked_ctime);
    assert!(matches!(
        dfs.posix_read_file(link),
        Err(DfsPosixError::NotFound { .. })
    ));

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_hardlink_after_extent_append_preserves_opened_range_reads_after_restart() {
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_hardlink_extent/work";
    let original = "/services/sandbox/instances/sandbox_hardlink_extent/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_extent/work/alias.txt";
    let expected = b"alpha-beta";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_hardlink_after_extent_append_preserves_opened_range_reads_after_restart",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"alpha",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write base");
    let appended = dfs
        .posix_write_file_at(
            original,
            5,
            b"-beta",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("append extent patch");
    let appended_hash = appended.content_hash.clone().expect("appended hash");
    assert!(
        !dfs.content
            .has_blob(&appended_hash)
            .expect("aggregate blob lookup"),
        "the regression requires an extent manifest without a monolithic aggregate blob"
    );

    dfs.posix_hardlink(original, link).expect("hardlink");
    let object_id = dfs.posix_stat(original).expect("linked stat").object_id;
    for path in [original, link] {
        assert_eq!(
            dfs.posix_read_file_range_for_opened_object(
                path,
                &object_id,
                0,
                expected.len() + 16,
                false,
            )
            .expect("opened range after metadata-only hardlink bump"),
            expected
        );
    }

    drop(dfs);
    runtime.stop_node(&node_id).unwrap();
    runtime.reopen_node(node_id.clone()).unwrap();
    let mut reopened = runtime.client(&node_id).unwrap();
    for path in [original, link] {
        assert_eq!(
            reopened
                .posix_read_file_range_for_opened_object(
                    path,
                    &object_id,
                    0,
                    expected.len() + 16,
                    false,
                )
                .expect("opened range after content-store restart"),
            expected
        );
    }
}

#[test]
fn sifs_posix_hardlink_preserves_symlink_object() {
    let root = temp_root("sifs-posix-hardlink-symlink");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_symlink").expect("node");
    let work = "/services/sandbox/instances/sandbox_hardlink_symlink/work";
    let target = "/services/sandbox/instances/sandbox_hardlink_symlink/work/target.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link.txt";
    let alias = "/services/sandbox/instances/sandbox_hardlink_symlink/work/link-alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_hardlink_preserves_symlink_object",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        target,
        b"target",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write target");
    dfs.posix_symlink(
        "target.txt",
        link,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink");
    dfs.posix_hardlink(link, alias).expect("hardlink symlink");

    let link_stat = dfs.posix_lstat(link).expect("link lstat");
    let alias_stat = dfs.posix_lstat(alias).expect("alias lstat");
    assert_eq!(link_stat.kind, DfsObjectKind::Symlink);
    assert_eq!(alias_stat.kind, DfsObjectKind::Symlink);
    assert_eq!(link_stat.object_id, alias_stat.object_id);
    assert_eq!(link_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_readlink(alias).expect("alias readlink"),
        "target.txt"
    );

    dfs.posix_unlink(link).expect("unlink original symlink");
    assert!(matches!(
        dfs.posix_readlink(link),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_readlink(alias).expect("remaining alias readlink"),
        "target.txt"
    );
    assert_eq!(
        dfs.posix_lstat(alias).expect("remaining alias").link_count,
        1
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_unlink_promotes_hardlink_when_canonical_removed() {
    let root = temp_root("sifs-posix-hardlink-promote");
    let node_id = NodeId::from_legacy_named("node_sifs_hardlink_promote").expect("node");
    let original = "/services/sandbox/instances/sandbox_hardlink_promote/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_hardlink_promote/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_unlink_promotes_hardlink_when_canonical_removed",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_hardlink_promote/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    let original_object_id = dfs.posix_stat(original).expect("original stat").object_id;
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_unlink(original).expect("unlink canonical");
    assert!(matches!(
        dfs.posix_read_file(original),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_read_file(link).expect("read promoted"),
        b"content"
    );
    let promoted = dfs.posix_stat(link).expect("promoted stat");
    assert_eq!(promoted.object_id, original_object_id);
    assert_eq!(promoted.link_count, 1);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_hardlink_over_canonical_preserves_target() {
    let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink").expect("node");
    let canonical = "/services/sandbox/instances/sandbox_rename_hardlink/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_hardlink/work/alias.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_hardlink_over_canonical_preserves_target",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_rename_hardlink/work")
        .expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    let canonical_object_id = dfs.posix_stat(canonical).expect("canonical stat").object_id;
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    assert_eq!(dfs.posix_stat(alias).expect("alias stat").link_count, 2);

    dfs.posix_rename(alias, canonical)
        .expect("rename alias over canonical");

    let alias_stat = dfs.posix_stat(alias).expect("alias remains");
    let canonical_stat = dfs.posix_stat(canonical).expect("canonical remains");
    assert_eq!(canonical_stat.object_id, canonical_object_id);
    assert_eq!(alias_stat.object_id, canonical_object_id);
    assert_eq!(canonical_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_read_file(canonical).expect("read canonical"),
        b"content"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"content");

    let reloaded = runtime.client(&node_id).unwrap();
    assert_eq!(
        reloaded
            .posix_stat(alias)
            .expect("reloaded alias")
            .link_count,
        2
    );
    assert_eq!(
        reloaded.posix_read_file(canonical).expect("reloaded read"),
        b"content"
    );
}

#[test]
fn sifs_posix_rename_over_canonical_preserves_other_hardlinks() {
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_over_canonical_hardlinks").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/source.txt";
    let canonical = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_over_hardlinks/work/alias.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_over_canonical_preserves_other_hardlinks",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    let old_object_id = dfs.posix_stat(canonical).expect("canonical stat").object_id;
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    dfs.posix_write_file(
        source,
        b"new",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;

    dfs.posix_rename(source, canonical)
        .expect("rename source over canonical");

    assert!(matches!(
        dfs.posix_stat(source),
        Err(DfsPosixError::NotFound { .. })
    ));
    let canonical_stat = dfs
        .posix_stat(canonical)
        .expect("canonical stat after rename");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat after rename");
    assert_eq!(canonical_stat.object_id, source_object_id);
    assert_eq!(canonical_stat.link_count, 1);
    assert_eq!(alias_stat.object_id, old_object_id);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        dfs.posix_read_file(canonical).expect("read canonical"),
        b"new"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"old");

    let reloaded = runtime.client(&node_id).unwrap();
    assert_eq!(
        reloaded
            .posix_stat(alias)
            .expect("reloaded alias")
            .object_id,
        old_object_id
    );
    assert_eq!(
        reloaded
            .posix_read_file(canonical)
            .expect("reloaded canonical"),
        b"new"
    );
    assert_eq!(
        reloaded.posix_read_file(alias).expect("reloaded alias"),
        b"old"
    );
}

#[test]
fn sifs_posix_rename_hardlink_over_canonical_preserves_both_targets() {
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_hardlink_over_other_canonical").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work";
    let source_canonical =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/source.txt";
    let source_alias =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/source-alias.txt";
    let dest_canonical =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/dest.txt";
    let dest_alias =
        "/services/sandbox/instances/sandbox_rename_hardlink_over_other/work/dest-alias.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_hardlink_over_canonical_preserves_both_targets",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        source_canonical,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write source");
    dfs.posix_hardlink(source_canonical, source_alias)
        .expect("source hardlink");
    let source_object_id = dfs
        .posix_stat(source_canonical)
        .expect("source stat")
        .object_id;
    dfs.posix_write_file(
        dest_canonical,
        b"dest",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write dest");
    dfs.posix_hardlink(dest_canonical, dest_alias)
        .expect("dest hardlink");
    let dest_object_id = dfs.posix_stat(dest_canonical).expect("dest stat").object_id;

    dfs.posix_rename(source_alias, dest_canonical)
        .expect("rename source alias over dest canonical");

    let source_stat = dfs
        .posix_stat(source_canonical)
        .expect("source canonical remains");
    let renamed_stat = dfs.posix_stat(dest_canonical).expect("renamed source");
    let dest_alias_stat = dfs.posix_stat(dest_alias).expect("dest alias remains");
    assert_eq!(source_stat.object_id, source_object_id);
    assert_eq!(renamed_stat.object_id, source_object_id);
    assert_eq!(source_stat.link_count, 2);
    assert_eq!(renamed_stat.link_count, 2);
    assert_eq!(dest_alias_stat.object_id, dest_object_id);
    assert_eq!(dest_alias_stat.link_count, 1);
    assert_eq!(
        dfs.posix_read_file(source_canonical)
            .expect("read source canonical"),
        b"source"
    );
    assert_eq!(
        dfs.posix_read_file(dest_canonical)
            .expect("read renamed source"),
        b"source"
    );
    assert_eq!(
        dfs.posix_read_file(dest_alias).expect("read dest alias"),
        b"dest"
    );
    assert!(matches!(
        dfs.posix_stat(source_alias),
        Err(DfsPosixError::NotFound { .. })
    ));

    let reloaded = runtime.client(&node_id).unwrap();
    assert_eq!(
        reloaded
            .posix_stat(dest_canonical)
            .expect("reloaded renamed source")
            .object_id,
        source_object_id
    );
    assert_eq!(
        reloaded
            .posix_stat(dest_alias)
            .expect("reloaded dest alias")
            .object_id,
        dest_object_id
    );
}

#[test]
fn sifs_posix_rename_hardlink_alias_updates_target_ctime() {
    let root = temp_root("sifs-posix-rename-hardlink-alias-ctime");
    let node_id = NodeId::from_legacy_named("node_sifs_rename_hardlink_alias_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_alias_ctime/work";
    let canonical = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/alias.txt";
    let renamed = "/services/sandbox/instances/sandbox_rename_alias_ctime/work/renamed-alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_hardlink_alias_updates_target_ctime",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write canonical");
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    let before = dfs.posix_stat(canonical).expect("before stat");
    std::thread::sleep(std::time::Duration::from_millis(1));

    dfs.posix_rename(alias, renamed)
        .expect("rename hardlink alias");

    let canonical_after = dfs.posix_stat(canonical).expect("canonical remains");
    let renamed_after = dfs.posix_stat(renamed).expect("renamed alias");
    assert_eq!(canonical_after.object_id, before.object_id);
    assert_eq!(renamed_after.object_id, before.object_id);
    assert_eq!(canonical_after.link_count, 2);
    assert_eq!(renamed_after.link_count, 2);
    assert!(
        canonical_after.changed_at_unix_nanos > before.changed_at_unix_nanos,
        "renaming a hardlink alias must advance target inode ctime"
    );
    assert_eq!(
        canonical_after.changed_at_unix_nanos,
        renamed_after.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_rename_over_hardlink_alias_updates_replaced_target_ctime() {
    let root = temp_root("sifs-posix-rename-over-hardlink-alias-ctime");
    let node_id =
        NodeId::from_legacy_named("node_sifs_rename_over_hardlink_alias_ctime").expect("node");
    let work = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work";
    let source = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/source.txt";
    let canonical =
        "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/canonical.txt";
    let alias = "/services/sandbox/instances/sandbox_rename_over_alias_ctime/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_over_hardlink_alias_updates_replaced_target_ctime",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        canonical,
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write canonical");
    dfs.posix_hardlink(canonical, alias).expect("hardlink");
    let old_before = dfs.posix_stat(canonical).expect("old before");
    dfs.posix_write_file(
        source,
        b"new",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write source");
    let source_object_id = dfs.posix_stat(source).expect("source stat").object_id;
    std::thread::sleep(std::time::Duration::from_millis(1));

    dfs.posix_rename(source, alias)
        .expect("rename source over hardlink alias");

    let canonical_after = dfs.posix_stat(canonical).expect("canonical remains");
    let alias_after = dfs.posix_stat(alias).expect("alias replaced");
    assert_eq!(canonical_after.object_id, old_before.object_id);
    assert_eq!(canonical_after.link_count, 1);
    assert!(
        canonical_after.changed_at_unix_nanos > old_before.changed_at_unix_nanos,
        "replacing a hardlink alias must advance the replaced target inode ctime"
    );
    assert_eq!(alias_after.object_id, source_object_id);
    assert_eq!(alias_after.link_count, 1);
    assert_eq!(dfs.posix_read_file(canonical).expect("read old"), b"old");
    assert_eq!(dfs.posix_read_file(alias).expect("read new"), b"new");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_times_updates_hardlink_target_metadata() {
    let root = temp_root("sifs-posix-set-times-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_set_times").expect("node");
    let original = "/services/sandbox/instances/sandbox_set_times/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_set_times/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_set_times_updates_hardlink_target_metadata",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_times/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_set_times(link, Some(12_345), Some(67_890))
        .expect("set times through link");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let link_stat = dfs.posix_stat(link).expect("link stat");
    assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(original_stat.modified_at_unix_nanos, 67_890);
    assert_eq!(link_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(link_stat.modified_at_unix_nanos, 67_890);
    assert!(original_stat.changed_at_unix_nanos > 67_890);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        link_stat.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_set_owner_updates_hardlink_target_metadata() {
    let root = temp_root("sifs-posix-set-owner-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_set_owner").expect("node");
    let original = "/services/sandbox/instances/sandbox_set_owner/work/file.txt";
    let link = "/services/sandbox/instances/sandbox_set_owner/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_set_owner_updates_hardlink_target_metadata",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_set_owner/work")
        .expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, link).expect("hardlink");

    dfs.posix_set_owner(link, 1001, 1002)
        .expect("set owner through link");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let link_stat = dfs.posix_stat(link).expect("link stat");
    assert_eq!(original_stat.uid, 1001);
    assert_eq!(original_stat.gid, 1002);
    assert_eq!(link_stat.uid, 1001);
    assert_eq!(link_stat.gid, 1002);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        link_stat.changed_at_unix_nanos
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_bulk_overwrite_preserves_hardlink_and_symlink_fallback_semantics() {
    let root = temp_root("sifs-posix-bulk-overwrite-links");
    let node_id = NodeId::from_legacy_named("node_sifs_bulk_overwrite_links").expect("node");
    let work = "/services/sandbox/instances/sandbox_bulk_overwrite_links/work";
    let target = "/services/sandbox/instances/sandbox_bulk_overwrite_links/work/target.txt";
    let hardlink = "/services/sandbox/instances/sandbox_bulk_overwrite_links/work/hard.txt";
    let symlink = "/services/sandbox/instances/sandbox_bulk_overwrite_links/work/sym.txt";
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_bulk_overwrite_preserves_hardlink_and_symlink_fallback_semantics",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        target,
        b"initial",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write target");
    dfs.posix_hardlink(target, hardlink).expect("hardlink");
    dfs.posix_symlink(
        "target.txt",
        symlink,
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("symlink");

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    crate::posix::reset_posix_bulk_prep_durable_table_probes();
    dfs.posix_write_files_local_eventual_with_known_parents([(
        hardlink,
        b"through hardlink".as_slice(),
    )])
    .expect("bulk write through hardlink");
    assert_eq!(
        crate::posix::posix_bulk_prep_durable_table_probes(),
        0,
        "hardlink target discovery must remain inside the pinned metadata generation"
    );
    assert_eq!(
        dfs.posix_read_file(target).expect("read hardlink target"),
        b"through hardlink"
    );

    crate::posix::reset_posix_bulk_prep_durable_table_probes();
    dfs.posix_write_files_local_eventual_with_known_parents([(
        symlink,
        b"through symlink".as_slice(),
    )])
    .expect("bulk write through symlink fallback");
    assert_eq!(
        crate::posix::posix_bulk_prep_durable_table_probes(),
        0,
        "symlink classification must not resume per-record durable table polling"
    );
    assert_eq!(
        dfs.posix_read_file(target).expect("read symlink target"),
        b"through symlink"
    );

    crate::metadata_read_snapshot::reset_metadata_durable_table_probes();
    dfs.posix_stat(target).expect("stat after symlink fallback");
    assert!(
        crate::metadata_read_snapshot::metadata_durable_table_probes() > 0,
        "symlink fallback must run after and leave behind no pinned metadata generation"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_merges_bytes_and_preserves_hardlink_identity() {
    let root = temp_root("sifs-posix-write-at-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at/work";
    let original = "/services/sandbox/instances/sandbox_write_at/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_write_at/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_write_at_merges_bytes_and_preserves_hardlink_identity",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"abcdef",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink");

    let stat = dfs
        .posix_write_file_at(
            alias,
            2,
            b"XYZ",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("write at hardlink");

    assert_eq!(stat.size_bytes, Some(6));
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"abXYZf"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"abXYZf");
    let original_stat = dfs.posix_stat(original).expect("original stat");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat");
    assert_eq!(original_stat.object_id, alias_stat.object_id);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);

    let stat = dfs
        .posix_write_file_at(
            original,
            8,
            b"!",
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("sparse write");

    assert_eq!(stat.size_bytes, Some(9));
    assert_eq!(
        dfs.posix_read_file(alias).expect("read sparse alias"),
        b"abXYZf\0\0!"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_truncate_preserves_hardlink_identity() {
    let root = temp_root("sifs-posix-truncate-hardlink");
    let node_id = NodeId::from_legacy_named("node_sifs_truncate").expect("node");
    let work = "/services/sandbox/instances/sandbox_truncate/work";
    let original = "/services/sandbox/instances/sandbox_truncate/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_truncate/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_truncate_preserves_hardlink_identity",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"abcdef",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink");

    let stat = dfs
        .posix_truncate_file(
            alias,
            3,
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("truncate through hardlink");
    assert_eq!(stat.size_bytes, Some(3));
    assert_eq!(dfs.posix_read_file(original).expect("read"), b"abc");
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"abc");
    assert_eq!(
        dfs.posix_stat(original).expect("original stat").object_id,
        dfs.posix_stat(alias).expect("alias stat").object_id
    );

    let stat = dfs
        .posix_truncate_file(
            original,
            5,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("extend sparse through original");
    assert_eq!(stat.size_bytes, Some(5));
    assert_eq!(
        dfs.posix_read_file(alias).expect("read sparse alias"),
        b"abc\0\0"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_relink_after_canonical_unlink_does_not_create_hardlink_cycle() {
    let root = temp_root("sifs-posix-relink-after-canonical-unlink");
    let node_id = NodeId::from_legacy_named("node_sifs_relink_after_unlink").expect("node");
    let work = "/services/sandbox/instances/sandbox_relink_after_unlink/work";
    let original = "/services/sandbox/instances/sandbox_relink_after_unlink/work/file.txt";
    let alias = "/services/sandbox/instances/sandbox_relink_after_unlink/work/alias.txt";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_relink_after_canonical_unlink_does_not_create_hardlink_cycle",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        original,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write original");
    dfs.posix_hardlink(original, alias).expect("hardlink alias");
    dfs.posix_write_file(
        alias,
        b"updated",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write alias");
    dfs.posix_unlink(original).expect("unlink canonical path");

    dfs.posix_hardlink(alias, original)
        .expect("relink original path");

    let original_stat = dfs.posix_stat(original).expect("original stat");
    let alias_stat = dfs.posix_stat(alias).expect("alias stat");
    assert_eq!(original_stat.object_id, alias_stat.object_id);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        dfs.posix_read_file(original).expect("read original"),
        b"updated"
    );
    assert_eq!(dfs.posix_read_file(alias).expect("read alias"), b"updated");

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_cluster_posix_contract_hardlink_link_count_races() {
    let mut cluster = sifs_posix_cluster("sifs-contract-hardlink-races", "/users");
    let scenario = "hardlink-link-count-races";
    let file = "/users/contract-hardlink.txt";
    let alias = "/users/contract-hardlink-alias.txt";
    let replacement = "/users/contract-hardlink-replacement.txt";
    let bytes = b"hardlink identity".to_vec();
    let updated = b"updated through alias".to_vec();
    let mut observations = Vec::new();

    sifs_cluster_primary_posix_write(&mut cluster, file, &bytes);
    let link = cluster
        .primary
        .posix_hardlink(file, alias)
        .expect("hardlink");
    record_posix_observation(
        &mut observations,
        scenario,
        "hardlink",
        &cluster.primary_node,
        alias,
        Instant::now(),
        "committed",
        Some(&link),
    );
    assert_eq!(
        cluster.primary.posix_stat(file).expect("file").link_count,
        2
    );
    let alias_write = cluster.writer.posix_write_file(
        alias,
        &updated,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    let alias_write_request = match &alias_write {
        Ok(stat) => {
            record_posix_observation(
                &mut observations,
                scenario,
                "alias-write",
                &cluster.writer_node,
                alias,
                Instant::now(),
                "committed",
                Some(stat),
            );
            None
        }
        Err(DfsPosixError::PendingWrite { intent_id, .. }) => Some(intent_id.clone()),
        Err(error) => panic!("alias write failed unexpectedly: {error}"),
    };
    sifs_posix_cluster_pump_until(&mut cluster, "alias write", |cluster| {
        alias_write_request.as_ref().is_none_or(|intent| {
            !matches!(
                cluster.writer.write_outcome(intent),
                Ok(NetworkedDfsWriteOutcome::Pending { .. })
            )
        }) && cluster
            .writer
            .posix_stat(alias)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(content_hash(&updated).as_str()))
    });
    sifs_cluster_primary_posix_write(&mut cluster, replacement, b"replacement");
    cluster
        .primary
        .posix_rename(replacement, file)
        .expect("rename over canonical");
    let alias_stat = cluster.primary.posix_stat(alias).expect("alias stat");
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        cluster
            .primary
            .posix_read_file(alias)
            .expect("alias content"),
        updated,
        "alias must retain original object content after canonical replacement"
    );
    assert_ne!(
        cluster
            .primary
            .posix_stat(file)
            .expect("new canonical")
            .object_id,
        alias_stat.object_id,
        "rename-over-canonical must not collapse distinct hardlink targets"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn native_owner_restart_recovers_its_actual_running_cross_shard_job() {
    native_cross_shard_move_recovers_from_real_carrier_failure(
        "native-restarted-job-owner",
        true,
        true,
    );
}

#[test]
fn request_shells_share_live_cross_shard_namespace_guards() {
    let node = NodeId::from_legacy_named("node_shared_cross_shard_guards").expect("node");
    let (runtime, global_shell) =
        native_sifs_service("native-shared-cross-shard-guards", node.clone());
    let file_system = runtime.file_system();
    let registry = global_shell.cross_shard_namespace_lock_registry();
    let exact_shell = runtime
        .client(&node)
        .unwrap()
        .with_cross_shard_namespace_lock_registry(registry.clone());

    registry.upsert_job(
        None,
        "shared-guard-job".to_string(),
        file_system.file_system_id.clone(),
        "/services/moving-tree".to_string(),
    );
    assert!(matches!(
        exact_shell.ensure_path_not_in_cross_shard_flight(
            &file_system.file_system_id,
            "/services/moving-tree/child.txt",
        ),
        Err(DfsError::Busy { .. })
    ));

    registry.remove_job("shared-guard-job");
    exact_shell
        .ensure_path_not_in_cross_shard_flight(
            &file_system.file_system_id,
            "/services/moving-tree/child.txt",
        )
        .expect("unlock is visible to every request shell");
    assert!(
        global_shell.cross_shard_namespace_locks.is_empty(),
        "the global shell observes the same job removal"
    );
    drop(exact_shell);
    drop(global_shell);
    runtime.close().unwrap();
}

#[test]
fn cross_shard_namespace_guards_are_job_owned_and_shard_replaceable() {
    let registry = DfsCrossShardNamespaceLockRegistry::default();
    let file_system_id = sifs_file_system_id();
    let root_shard = DfsMetadataShardId::new("root");
    let services_shard = DfsMetadataShardId::new("services");
    let path = "/services/reused-path";
    registry.replace_jobs([
        (
            Some(root_shard.clone()),
            "older-job".to_string(),
            file_system_id.clone(),
            path.to_string(),
        ),
        (
            Some(services_shard.clone()),
            "newer-job".to_string(),
            file_system_id.clone(),
            path.to_string(),
        ),
    ]);

    registry.remove_job("older-job");
    assert!(
        registry.blocks(&file_system_id, path),
        "a delayed terminal transition must not remove another job's same-path guard"
    );

    registry.replace_shard_jobs(
        &services_shard,
        std::iter::empty::<(String, DfsFileSystemId, Vec<String>)>(),
    );
    assert!(
        !registry.blocks(&file_system_id, path),
        "an exact shard snapshot with no running jobs removes that shard's obsolete guards"
    );
    assert!(root_shard != services_shard);
}

#[test]
fn sifs_cluster_metadata_less_bulk_overwrite_follows_revalidated_hardlink_target() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-metadata-less-hardlink");
    let remote_dir = "/services/bulk-metadata-less-hardlink";
    let target = "/services/bulk-metadata-less-hardlink/target.txt";
    let link = "/services/bulk-metadata-less-hardlink/link.txt";
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote hardlink parent");
    sifs_two_primary_publish_until(&mut cluster, "metadata-less hardlink parent", |cluster| {
        cluster.replica.posix_stat(remote_dir).is_ok()
    });

    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root: _,
        _runtime,
        users_primary,
        services_primary,
        replica_node,
        mut replica,
        ..
    } = cluster;
    let options = DfsWriteOptions::replicated_eventual(vec![replica_node]);
    replica
        .posix_write_file(target, b"before", options)
        .expect("write hardlink target");
    replica
        .posix_hardlink(target, link)
        .expect("create hardlink");
    let target_before = replica.posix_stat(target).expect("stat hardlink target");
    let link_before = replica
        .posix_namespace_lstat_distributed(link)
        .expect("lstat hardlink before metadata eviction");
    assert_eq!(link_before.kind, DfsObjectKind::Hardlink);
    let parent_id = replica
        .posix_stat(remote_dir)
        .expect("stat hardlink parent")
        .object_id;

    for object_id in [&parent_id, &target_before.object_id, &link_before.object_id] {
        replica.metadata.remove_object(object_id);
    }
    replica.metadata_backend = None;

    let stat = replica
        .posix_write_files_local_eventual_with_known_parents([(
            link,
            b"through authoritative hardlink".as_slice(),
        )])
        .expect("metadata-less write through authoritative hardlink")
        .pop()
        .expect("hardlink write stat");
    assert_eq!(stat.path, link);
    assert_eq!(stat.object_id, target_before.object_id);
    assert_eq!(
        replica
            .posix_read_file_distributed(target)
            .expect("read hardlink target"),
        b"through authoritative hardlink"
    );
    let link_after = services_primary
        .posix_namespace_lstat(link)
        .expect("authoritative hardlink after overwrite");
    assert_eq!(link_after.kind, DfsObjectKind::Hardlink);
    assert_eq!(link_after.object_id, link_before.object_id);
    assert_eq!(
        services_primary
            .posix_stat(target)
            .expect("authoritative hardlink target stat")
            .content_hash
            .as_deref(),
        Some(content_hash(b"through authoritative hardlink").as_str())
    );
    drop((users_primary, services_primary, replica, shared_kv));
    _runtime
        .close()
        .expect("normal embedded DFS fixture shutdown");
}

#[test]
fn sifs_cluster_posix_rename_across_shard_primaries_moves_object_transactionally() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-rename");
    let SifsTwoPrimaryCluster {
        root,
        _runtime,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        mut replica,
        rejections: _,
    } = cluster;
    let bytes = b"bytes that move between shard primaries".to_vec();
    let moved_bytes = b"rewritten at the new shard authority".to_vec();

    // Create under /users (routed to the users primary).
    replica
        .posix_write_file(
            "/users/move-me.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("replica routed create");
    assert_eq!(
        replica
            .posix_read_file_distributed("/users/move-me.txt")
            .expect("read"),
        bytes
    );

    // Rename across shard primaries; the source primary coordinates the
    // adoption on the services primary.
    let stat = replica
        .posix_rename("/users/move-me.txt", "/services/moved.txt")
        .expect("cross-primary rename");
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert_eq!(stat.path, "/services/moved.txt");

    // Read-your-write on the requester for both halves.
    assert!(matches!(
        replica.posix_stat("/users/move-me.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        replica
            .posix_read_file_distributed("/services/moved.txt")
            .expect("read moved"),
        bytes
    );

    // The object's write authority moved with it: a write to the new path
    // routes to the services primary and commits there.
    replica
        .posix_write_file(
            "/services/moved.txt",
            &moved_bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("write at new shard authority");
    assert_eq!(
        replica
            .posix_read_file_distributed("/services/moved.txt")
            .expect("read rewritten"),
        moved_bytes
    );

    assert!(matches!(
        users_primary.posix_stat("/users/move-me.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(users_primary.posix_stat("/services/moved.txt").is_ok());
    assert_eq!(
        services_primary
            .posix_stat("/services/moved.txt")
            .expect("services primary stat")
            .content_hash
            .as_deref(),
        Some(content_hash(&moved_bytes).as_str())
    );

    // Convergence: both primaries publish and every node agrees, with no
    // journal rejections anywhere.
    let mut cluster = SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    let expected_hash = content_hash(&moved_bytes);
    sifs_two_primary_publish_until(&mut cluster, "post-rename convergence", |cluster| {
        let services_sees_users_half = matches!(
            cluster.services_primary.posix_stat("/users/move-me.txt"),
            Err(DfsPosixError::NotFound { .. })
        );
        let replica_current = cluster
            .replica
            .posix_stat("/services/moved.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()));
        services_sees_users_half && replica_current
    });
    assert!(
        cluster.rejections.is_empty(),
        "cross-primary rename must keep every replica journal aligned: {:?}",
        cluster.rejections
    );
    let users_entries = cluster
        .users_primary
        .posix_list_dir("/users")
        .expect("users list");
    assert!(
        !users_entries
            .iter()
            .any(|entry| entry.path == "/users/move-me.txt"),
        "old path must not be listed after the move: {users_entries:?}"
    );
    let services_entries = cluster
        .replica
        .posix_list_dir("/services")
        .expect("replica services list");
    assert!(
        services_entries
            .iter()
            .any(|entry| entry.path == "/services/moved.txt"),
        "new path must be listed on replicas: {services_entries:?}"
    );

    let SifsTwoPrimaryCluster {
        root,
        _runtime,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        mut users_primary,
        services_primary,
        replica,
        rejections,
    } = cluster;
    let unlinked = users_primary
        .posix_unlink("/services/moved.txt")
        .expect("users primary routes unlink to services primary after cross-shard move");
    assert_eq!(unlinked.kind, DfsObjectKind::Tombstone);
    let mut cluster = SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections,
    };
    sifs_two_primary_publish_until(&mut cluster, "post-move unlink convergence", |cluster| {
        matches!(
            cluster.replica.posix_stat("/services/moved.txt"),
            Err(DfsPosixError::NotFound { .. })
        ) && matches!(
            cluster.users_primary.posix_stat("/services/moved.txt"),
            Err(DfsPosixError::NotFound { .. })
        )
    });

    cluster.close();
}

#[test]
fn sifs_cluster_cross_shard_rename_replays_caller_owned_operation_id() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-stable-rename-replay");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root: _,
        _runtime,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let source_path = "/users/stable-replay-source.txt";
    let target_path = "/services/stable-replay-target.txt";
    let operation_id = "dfs_test_cross_shard_stable_rename";

    replica
        .posix_write_file(
            source_path,
            b"stable cross-shard rename replay",
            DfsWriteOptions::replicated_eventual(vec![replica_node]),
        )
        .expect("create stable-rename source");
    let first = replica
        .posix_rename_with_operation_id(source_path, target_path, operation_id)
        .expect("first stable cross-shard rename");
    let replay = replica
        .posix_rename_with_operation_id(source_path, target_path, operation_id)
        .expect("replay stable cross-shard rename");

    assert_eq!(replay.object_id, first.object_id);
    assert_eq!(replay.version, first.version);
    assert_eq!(
        replica
            .posix_read_file_distributed(target_path)
            .expect("target remains readable after exact replay"),
        b"stable cross-shard rename replay"
    );
    assert!(matches!(
        replica.posix_stat(source_path),
        Err(DfsPosixError::NotFound { .. })
    ));

    drop((users_primary, services_primary, replica, shared_kv));
    _runtime
        .close()
        .expect("normal embedded DFS fixture shutdown");
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_preserves_durable_version_pin() {
    cross_shard_rename_preserves_pin_and_durability(false);
}

#[test]
fn file_history_cross_shard_metadata_generation_preserves_durability() {
    cross_shard_rename_preserves_pin_and_durability(true);
}

fn cross_shard_rename_preserves_pin_and_durability(with_durability: bool) {
    let cluster = if with_durability {
        sifs_two_primary_cluster("sifs-xshard-content-durability")
    } else {
        sifs_two_primary_cluster("sifs-xshard-pinned-rename")
    };
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root: _,
        _runtime,
        replica_node,
        mut users_primary,
        mut services_primary,
        mut replica,
        ..
    } = cluster;
    let source_path = "/users/pinned-move.txt";
    let target_path = "/services/pinned-moved.txt";
    let bytes = b"pinned metadata version crosses shard primaries".to_vec();

    replica
        .posix_write_file(
            source_path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("create pinned source");

    let source = users_primary
        .posix_stat(source_path)
        .expect("source metadata");
    let source_durability = users_primary
        .metadata_backend()
        .unwrap()
        .get_version(&source.object_id, &source.version)
        .unwrap()
        .unwrap()
        .content_durability;
    assert!(
        source_durability.is_some(),
        "current writes always retain their actual data-generation binding"
    );
    users_primary
        .retain_content_revision(DfsContentReferenceRequest {
            namespace: "module.dfs.history".into(),
            path: source.path.clone(),
            identity: DfsRevisionReference {
                file_system_id: source.file_system_id.clone(),
                object_id: source.object_id.clone(),
                version: source.version.clone(),
            },
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "checkpoint-cross-primary-rename".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .expect("actual guarded source capture");
    let pin = users_primary
        .metadata_backend()
        .unwrap()
        .version_pins_for_object(&source.object_id)
        .unwrap()
        .into_iter()
        .find(|pin| {
            pin.pin_id
                == crate::content_reference_storage_id(
                    "module.dfs.history",
                    "checkpoint-cross-primary-rename",
                )
        })
        .expect("the source capture persisted its genuine pin");

    replica
        .posix_rename(source_path, target_path)
        .expect("pinned cross-primary rename");

    let adopted_target = services_primary
        .posix_stat(target_path)
        .expect("adopted target metadata");
    assert_eq!(
        services_primary
            .metadata_backend()
            .unwrap()
            .get_version(&adopted_target.object_id, &adopted_target.version)
            .unwrap()
            .unwrap()
            .content_durability,
        source_durability,
        "metadata-only cross-shard adoption must carry the data durability plan"
    );
    assert!(
        services_primary
            .metadata_backend()
            .expect("services metadata backend")
            .version_pins_for_object(&source.object_id)
            .expect("target pins")
            .iter()
            .any(|installed| installed == &pin),
        "the destination authority must retain the exact durable source pin"
    );
    let retired_source = users_primary
        .metadata_backend
        .as_mut()
        .expect("users metadata backend")
        .capture_cross_shard_source_family(&source.object_id, source_path)
        .expect("retired physical source family");
    assert!(
        retired_source.version_pins.is_empty(),
        "source pins must be removed atomically with source eviction"
    );
    assert!(matches!(
        retired_source.source_object.tombstone_provenance,
        Some(DfsTombstoneProvenance::SourceEviction { .. })
    ));
    assert_eq!(
        retired_source.source_object.version, adopted_target.version,
        "source-eviction witness and installed target must name the durable job-frame generation"
    );
    assert!(
        services_primary
            .metadata_backend()
            .expect("services metadata backend")
            .cross_shard_pin_transfers_for_object(&source.object_id)
            .expect("target transfer provenance")
            .is_empty(),
        "quorum-durable finalization must retire temporary pin-transfer provenance"
    );

    if with_durability {
        let result = services_primary.retain_content_revision(DfsContentReferenceRequest {
            namespace: "org.example.cross-shard".into(),
            path: target_path.into(),
            identity: DfsFileVersionIdentity {
                file_system_id: source.file_system_id.clone(),
                object_id: source.object_id.clone(),
                version: adopted_target.version.clone(),
            },
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "after-cross-shard-move".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        });
        result.unwrap();
        let captured = services_primary
            .metadata_backend()
            .unwrap()
            .get_version(&adopted_target.object_id, &adopted_target.version)
            .unwrap()
            .unwrap();
        assert!(
            captured
                .retention_grant("org.example.cross-shard")
                .is_some()
        );
        assert_eq!(captured.content_durability, source_durability);
        assert_eq!(
            services_primary.posix_stat(target_path).unwrap().version,
            adopted_target.version
        );
    }

    drop((users_primary, services_primary, replica, shared_kv));
    _runtime
        .close()
        .expect("normal embedded DFS fixture shutdown");
}

#[test]
fn sifs_cluster_posix_rename_across_shard_primaries_rejects_invalid_destinations() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-errno");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root: _,
        _runtime,
        replica_node,
        mut users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;

    let bytes = b"survives every failed rename".to_vec();
    replica
        .posix_write_file(
            "/users/keep-me.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("replica create");

    // Source missing: validated by the source primary.
    assert!(matches!(
        replica.posix_rename("/users/absent.txt", "/services/x.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    // Directory sources move through the cross-shard subtree saga; renaming
    // a directory into its own subtree stays rejected.
    replica.posix_mkdir("/users/srcdir").expect("mkdir srcdir");
    let dir_stat = replica
        .posix_rename("/users/srcdir", "/services/srcdir")
        .expect("cross-primary directory rename");
    assert_eq!(dir_stat.kind, DfsObjectKind::Directory);
    let invalid_subtree = replica.posix_rename("/services/srcdir", "/services/srcdir/inner");
    assert!(
        matches!(&invalid_subtree, Err(DfsPosixError::InvalidPath { .. })),
        "renaming the actual directory into its own subtree must be rejected: {invalid_subtree:?}"
    );
    // Missing destination parent: validated by the destination primary and
    // round-tripped through two hops.
    assert!(matches!(
        replica.posix_rename("/users/keep-me.txt", "/services/no-parent/keep.txt"),
        Err(DfsPosixError::MissingParent { .. })
    ));
    // Directory destination: also a destination-primary validation.
    replica
        .posix_mkdir("/services/destdir")
        .expect("mkdir destdir");
    let directory_destination = replica.posix_rename("/users/keep-me.txt", "/services/destdir");
    assert!(
        matches!(
            &directory_destination,
            Err(DfsPosixError::IsDirectory { .. })
        ),
        "an existing directory destination must be rejected: {directory_destination:?}"
    );

    // Aborted sagas must release their locks and leave the source intact:
    // the source is still readable and writable.
    assert_eq!(
        replica
            .posix_read_file_distributed("/users/keep-me.txt")
            .expect("read the current source through its owner"),
        bytes
    );
    let updated = b"still writable after aborted moves".to_vec();
    replica
        .posix_write_file(
            "/users/keep-me.txt",
            &updated,
            DfsWriteOptions::replicated_eventual(vec![replica_node]),
        )
        .expect("source writable after aborts");
    assert_eq!(
        replica
            .posix_read_file_distributed("/users/keep-me.txt")
            .expect("read updated"),
        updated
    );

    // No saga may be left running on the coordinator.
    let report = users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance resume");
    assert_eq!(
        report.remaining_running_job_count, 0,
        "aborted cross-shard renames must not leave running jobs: {report:?}"
    );

    drop((users_primary, services_primary, replica, shared_kv));
    _runtime
        .close()
        .expect("normal embedded DFS fixture shutdown");
}

#[test]
fn sifs_cluster_posix_hardlink_across_shard_primaries_links_and_promotes() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-hardlink");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root,
        _runtime,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let bytes = b"content shared across shard primaries".to_vec();

    replica
        .posix_write_file(
            "/users/canonical.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("replica create canonical");

    // Hardlink across primaries: the target's primary coordinates the alias
    // adoption on the services primary.
    let link_stat = replica
        .posix_hardlink("/users/canonical.txt", "/services/alias")
        .expect("cross-primary hardlink");
    assert_eq!(link_stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(link_stat.link_count, 2);
    assert_eq!(
        replica
            .posix_read_file_distributed("/services/alias")
            .expect("read through alias"),
        bytes
    );
    assert_eq!(
        replica
            .posix_stat_distributed("/users/canonical.txt")
            .expect("canonical stat")
            .link_count,
        2
    );

    // The alias path is owned by the services primary while its canonical
    // inode is owned by the users primary. Alias unlink must coordinate the
    // canonical ctime transition on that authority before tombstoning the
    // alias generation locally.
    let canonical_before_alias_unlink = replica
        .posix_stat_distributed("/users/canonical.txt")
        .expect("canonical before alias unlink");
    let alias_unlinked = replica
        .posix_unlink("/services/alias")
        .expect("unlink alias through alias primary");
    assert_eq!(alias_unlinked.kind, DfsObjectKind::Tombstone);
    let canonical_after_alias_unlink = replica
        .posix_lstat_distributed("/users/canonical.txt")
        .expect("canonical after alias unlink");
    assert_eq!(canonical_after_alias_unlink.link_count, 1);
    assert_ne!(
        canonical_after_alias_unlink.version, canonical_before_alias_unlink.version,
        "alias-primary unlink must advance the canonical target generation",
    );
    assert!(
        canonical_after_alias_unlink.changed_at_unix_nanos
            > canonical_before_alias_unlink.changed_at_unix_nanos,
        "alias-primary unlink must advance canonical ctime",
    );

    let recreated_alias = replica
        .posix_hardlink("/users/canonical.txt", "/services/alias-for-rename")
        .expect("recreate cross-primary alias");
    let canonical_before_alias_rename = replica
        .posix_stat_distributed("/users/canonical.txt")
        .expect("canonical before alias rename");
    let physical_alias_scope =
        hardlink_read_set_snapshot(&_runtime, &services_primary_node, "services").unwrap();
    assert!(
        physical_alias_scope
            .tables
            .objects
            .iter()
            .any(|object| object.path == "/services/alias-for-rename"
                && object.kind == DfsObjectKind::Hardlink)
    );
    assert!(
        !physical_alias_scope
            .tables
            .objects
            .iter()
            .any(|object| object.object_id == canonical_before_alias_rename.object_id),
        "the alias-only physical writer must not contain a fabricated canonical inode"
    );
    let rename_entry = Arc::new(Mutex::new(Vec::new()));
    let observed_rename = Arc::clone(&rename_entry);
    let rename_hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        _runtime.data_dir(&services_primary_node),
        Arc::new(move |point, command, _| {
            if let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute {
                admission_held,
            } = point
                && let Some(command) = command
                && matches!(&command.routed_message.message,
                    DfsNetworkMessage::NamespaceRequest(request)
                        if matches!(&request.operation, DfsNamespaceOperation::Rename { from, to }
                            if from == "/services/alias-for-rename" && to == "/services/renamed-alias"))
            {
                observed_rename
                    .lock()
                    .unwrap()
                    .push((admission_held, command.clone()));
            }
            Ok(())
        }),
    );
    let renamed_alias = replica
        .posix_rename("/services/alias-for-rename", "/services/renamed-alias")
        .expect("rename alias through alias primary");
    drop(rename_hook);
    let rename_entries = rename_entry.lock().unwrap();
    assert!(
        !rename_entries.is_empty(),
        "actual owning Main must execute the rename"
    );
    for (admission_held, command) in rename_entries.iter() {
        assert!(*admission_held);
        assert_eq!(command.routed_message.source_node_id, replica_node);
        assert_eq!(command.routed_message.target_node_id, services_primary_node);
        assert_eq!(
            crate::dfs_message_execution_scope(command),
            crate::DfsMessageExecutionScope::GlobalExclusive,
            "the received alias path stamps do not cover its foreign canonical inode"
        );
    }
    drop(rename_entries);
    assert_eq!(renamed_alias.object_id, recreated_alias.object_id);
    assert_eq!(renamed_alias.link_count, 2);
    let canonical_after_alias_rename = replica
        .posix_lstat_distributed("/users/canonical.txt")
        .expect("canonical after alias rename");
    assert_eq!(canonical_after_alias_rename.link_count, 2);
    assert_ne!(
        canonical_after_alias_rename.version, canonical_before_alias_rename.version,
        "alias-primary rename must advance the canonical target generation",
    );
    assert!(
        canonical_after_alias_rename.changed_at_unix_nanos
            > canonical_before_alias_rename.changed_at_unix_nanos,
        "alias-primary rename must advance canonical ctime",
    );

    // Unlinking the canonical promotes the foreign alias through the
    // cross-shard move saga.
    let unlinked = replica
        .posix_unlink("/users/canonical.txt")
        .expect("unlink canonical with foreign alias");
    assert_eq!(unlinked.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        replica.posix_stat_distributed("/users/canonical.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));

    // Convergence: the alias becomes the canonical record everywhere with
    // the content intact and a link count of one.
    let mut cluster = SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    let expected_hash = content_hash(&bytes);
    sifs_two_primary_publish_until(&mut cluster, "post-promotion convergence", |cluster| {
        let replica_alias = cluster
            .replica
            .posix_stat_distributed("/services/renamed-alias")
            .is_ok_and(|stat| {
                stat.content_hash.as_deref() == Some(expected_hash.as_str())
                    && stat.kind == DfsObjectKind::File
                    && stat.link_count == 1
            });
        let canonical_gone = matches!(
            cluster
                .replica
                .posix_stat_distributed("/users/canonical.txt"),
            Err(DfsPosixError::NotFound { .. })
        );
        replica_alias && canonical_gone
    });
    assert_eq!(
        cluster
            .replica
            .posix_read_file_distributed("/services/renamed-alias")
            .expect("read promoted alias"),
        bytes
    );
    assert!(
        cluster.rejections.is_empty(),
        "cross-primary hardlink and promotion must keep journals aligned: {:?}",
        cluster.rejections
    );

    cluster.close();
}

#[test]
fn cross_primary_hardlink_alias_unlink_fences_the_crash_gap_and_recovers_exactly_once() {
    let mut cluster = sifs_two_primary_cluster("native-hardlink-alias-crash-gap");
    let canonical_path = "/users/fenced-canonical";
    let alias_path = "/services/fenced-alias";
    cluster
        .replica
        .posix_write_file(
            canonical_path,
            b"fenced hardlink payload",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster
        .replica
        .posix_hardlink(canonical_path, alias_path)
        .unwrap();
    let alias = cluster
        .replica
        .posix_namespace_lstat_distributed(alias_path)
        .unwrap();
    let expected = DfsNamespaceObjectPrecondition {
        object_id: alias.object_id,
        version: alias.version,
    };
    let before = cluster
        .users_primary
        .posix_lstat_distributed(canonical_path)
        .unwrap();
    let original_parent = hardlink_read_set_snapshot(
        &cluster._runtime,
        &cluster.services_primary_node,
        "services",
    )
    .unwrap()
    .tables
    .objects
    .into_iter()
    .find(|row| row.path == "/services")
    .unwrap();
    let operation_id = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::ConditionalUnlink {
        path: alias_path.into(),
        expected: expected.clone(),
    };
    let (observed_tx, observed_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let hook = crate::posix::install_hardlink_alias_after_commit_test_hook(alias_path, move || {
        observed_tx.send(()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bounded actual after-commit interception");
        Err(DfsPosixError::Dfs(DfsError::Network {
            message: "actual alias committed; requester completion lost".into(),
        }))
    });
    std::thread::scope(|scope| {
        let requester = &mut cluster.replica;
        let pending_operation_id = operation_id.clone();
        let expected = expected.clone();
        let pending = scope.spawn(move || {
            requester.posix_unlink_if_generation(alias_path, pending_operation_id, expected)
        });
        let observed = observed_rx.recv_timeout(Duration::from_secs(5));
        // Always release the actual native request before propagating a failed assertion.
        let state = (|| -> std::result::Result<_, String> {
            observed.as_ref().map_err(|error| error.to_string())?;
            let alias_cut = hardlink_read_set_snapshot(
                &cluster._runtime,
                &cluster.services_primary_node,
                "services",
            )
            .map_err(|error| error.to_string())?;
            let canonical_cut =
                hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
                    .map_err(|error| error.to_string())?;
            let alias = alias_cut
                .tables
                .objects
                .iter()
                .find(|row| row.path == alias_path)
                .ok_or("actual alias physical row missing")?
                .clone();
            let canonical = canonical_cut
                .tables
                .objects
                .iter()
                .find(|row| row.path == canonical_path)
                .ok_or("actual canonical physical row missing")?
                .clone();
            let request = alias_cut
                .tables
                .namespace_requests
                .iter()
                .find(|row| {
                    row.requester_node_id == cluster.replica_node && row.request_id == operation_id
                })
                .ok_or("actual outer Pending receipt missing")?
                .clone();
            Ok((alias, canonical, request))
        })();
        release_tx.send(()).ok();
        let outcome = pending.join().unwrap();
        observed.expect("real guarded alias commit reached");
        let (alias, canonical, request) = state.unwrap();
        assert_eq!(alias.kind, DfsObjectKind::Tombstone);
        assert_eq!(canonical.version, before.version);
        assert_eq!(request.status, DfsNamespaceRequestStatus::Pending);
        assert!(
            outcome.is_err(),
            "lost completion does not fabricate an outer terminal response"
        );
    });
    drop(hook);
    let source_before_cold = hardlink_read_set_snapshot(
        &cluster._runtime,
        &cluster.services_primary_node,
        "services",
    )
    .unwrap();
    let original_frame = recorded_hardlink_alias_frame(&source_before_cold, &operation_id);
    let DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit {
        transaction,
        shard_id,
        expected_object_versions,
        expected_path_occupants,
    } = &original_frame
    else {
        unreachable!()
    };
    assert_eq!(shard_id, &DfsMetadataShardId::new("services"));
    assert!(
        expected_object_versions
            .iter()
            .any(|row| row.object_id == original_parent.object_id
                && row.expected_version.as_ref() == Some(&original_parent.version))
    );
    let prepared_operation_id = crate::metadata_records::compound_prepared_operation_id(
        &operation_id,
        &transaction.transaction_id,
    );
    let original_prepare = source_before_cold
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| record.payload.operation_id == prepared_operation_id)
        .unwrap()
        .clone();
    original_prepare.validate().unwrap();
    assert_eq!(
        original_prepare.status,
        DfsPreparedOperationStatus::Committed
    );
    assert_eq!(
        &original_prepare.payload.expected_object_versions,
        expected_object_versions
    );
    assert_eq!(
        &original_prepare.payload.expected_path_occupants,
        expected_path_occupants
    );
    // Restart both actual authorities, retaining their volumes and native frames.
    let users = cluster.users_primary_node.clone();
    let services = cluster.services_primary_node.clone();
    drop(cluster.users_primary);
    drop(cluster.services_primary);
    cluster._runtime.stop_node(&services).unwrap();
    cluster._runtime.stop_node(&users).unwrap();
    cluster._runtime.reopen_node(users.clone()).unwrap();
    cluster._runtime.reopen_node(services.clone()).unwrap();
    cluster.users_primary = cluster._runtime.client(&users).unwrap();
    cluster.services_primary = cluster._runtime.client(&services).unwrap();
    let restored_cut =
        hardlink_read_set_snapshot(&cluster._runtime, &services, "services").unwrap();
    assert_eq!(
        recorded_hardlink_alias_frame(&restored_cut, &operation_id),
        original_frame
    );
    assert_eq!(
        restored_cut
            .tables
            .prepared_operations_v7
            .iter()
            .find(|record| record.payload.operation_id == prepared_operation_id)
            .unwrap(),
        &original_prepare
    );
    let replay = cluster
        .replica
        .posix_unlink_if_generation(alias_path, operation_id.clone(), expected.clone())
        .unwrap();
    assert_eq!(replay.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        cluster
            .replica
            .reconcile_stable_namespace_operation(&operation_id, &operation)
            .unwrap(),
        DfsStableNamespaceOperationReconciliation::Applied { .. }
    ));
    let completed_cut =
        hardlink_read_set_snapshot(&cluster._runtime, &services, "services").unwrap();
    assert_eq!(
        recorded_hardlink_alias_frame(&completed_cut, &operation_id),
        original_frame
    );
    assert_eq!(
        completed_cut
            .tables
            .prepared_operations_v7
            .iter()
            .find(|record| record.payload.operation_id == prepared_operation_id)
            .unwrap(),
        &original_prepare
    );
    let mut changed = expected;
    changed.version = DfsVersion::generate();
    assert!(matches!(
        cluster
            .replica
            .posix_unlink_if_generation(alias_path, operation_id, changed),
        Err(DfsPosixError::Conflict { .. })
    ));
    let canonical = cluster
        .users_primary
        .posix_stat_distributed(canonical_path)
        .unwrap();
    assert_eq!(canonical.link_count, 1);
    assert_ne!(canonical.version, before.version);
    assert_eq!(
        cluster
            .replica
            .posix_read_file_distributed(canonical_path)
            .unwrap(),
        b"fenced hardlink payload"
    );
    assert!(matches!(
        cluster.replica.posix_stat_distributed(alias_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    cluster.close();
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_recovers_after_destination_outage() {
    native_cross_shard_move_recovers_from_real_carrier_failure(
        "sifs_cluster_posix_cross_shard_rename_recovers_after_destination_outage",
        true,
        false,
    );
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_retry_resumes_existing_job() {
    native_cross_shard_move_recovers_from_real_carrier_failure(
        "sifs_cluster_posix_cross_shard_rename_retry_resumes_existing_job",
        false,
        false,
    );
}

#[test]
fn sifs_cluster_posix_cross_shard_rename_pending_replay_reconciles_completed_job() {
    native_cross_shard_move_recovers_from_real_carrier_failure(
        "sifs_cluster_posix_cross_shard_rename_pending_replay_reconciles_completed_job",
        false,
        true,
    );
}

#[test]
fn sifs_cluster_posix_directory_rename_across_shard_primaries_moves_subtree() {
    let cluster = sifs_two_primary_cluster("sifs-xshard-dir-rename");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        root,
        _runtime,
        replica_node,
        mut users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;
    let bytes_a = b"alpha child crossing shard authorities".to_vec();
    let bytes_b = b"nested child content".to_vec();
    let bytes_a2 = b"rewritten under the new shard authority".to_vec();

    // Build a small subtree under /users from the replica.
    replica.posix_mkdir("/users/proj").expect("mkdir proj");
    replica
        .posix_write_file(
            "/users/proj/a.txt",
            &bytes_a,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("create a.txt");
    replica.posix_mkdir("/users/proj/sub").expect("mkdir sub");
    replica
        .posix_write_file(
            "/users/proj/sub/b.txt",
            &bytes_b,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("create b.txt");

    // The whole subtree moves between shard primaries.
    let stat = replica
        .posix_rename("/users/proj", "/services/proj")
        .expect("cross-primary directory rename");
    assert_eq!(stat.kind, DfsObjectKind::Directory);

    // Read-your-write on the requester for the full subtree, both halves.
    assert!(matches!(
        replica.posix_stat("/users/proj"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        replica.posix_stat("/users/proj/a.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        replica
            .posix_read_file_distributed("/services/proj/a.txt")
            .expect("read a"),
        bytes_a
    );
    assert_eq!(
        replica
            .posix_read_file_distributed("/services/proj/sub/b.txt")
            .expect("read b"),
        bytes_b
    );
    let names: Vec<String> = replica
        .posix_list_dir("/services/proj")
        .expect("list moved dir")
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert_eq!(names, vec!["a.txt".to_string(), "sub".to_string()]);

    // Write authority moved with every child: a write through the new path
    // commits at the services primary.
    replica
        .posix_write_file(
            "/services/proj/a.txt",
            &bytes_a2,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("write at new authority");
    assert_eq!(
        replica
            .posix_read_file_distributed("/services/proj/a.txt")
            .expect("read rewritten"),
        bytes_a2
    );

    // No saga may be left running on the coordinator.
    let report = users_primary
        .posix_resume_namespace_jobs_bounded(8, 512)
        .expect("maintenance resume");
    assert_eq!(
        (report.remaining_running_job_count, report.failed_job_count),
        (0, 0),
        "directory move must leave no running or failed jobs: {report:?}"
    );

    // Convergence: both primaries publish; every node agrees on the moved
    // subtree with no journal rejections.
    let mut cluster = SifsTwoPrimaryCluster {
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        shared_kv,
        users_primary,
        services_primary,
        replica,
        rejections: Vec::new(),
    };
    let expected_b = content_hash(&bytes_b);
    sifs_two_primary_publish_until(&mut cluster, "post-dir-move convergence", |cluster| {
        let services_sees_old_gone = matches!(
            cluster.services_primary.posix_stat("/users/proj"),
            Err(DfsPosixError::NotFound { .. })
        );
        let users_sees_nested = cluster
            .users_primary
            .posix_stat("/services/proj/sub/b.txt")
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_b.as_str()));
        services_sees_old_gone && users_sees_nested
    });
    assert!(
        cluster.rejections.is_empty(),
        "cross-primary directory rename must keep journals aligned: {:?}",
        cluster.rejections
    );
    let listing = |service: &NetworkedDfsService<DfsNodeTransport>| {
        service
            .posix_list_dir("/services/proj")
            .expect("list")
            .into_iter()
            .map(|entry| (entry.name, entry.kind, entry.object_id))
            .collect::<Vec<_>>()
    };
    let users_view = listing(&cluster.users_primary);
    assert_eq!(users_view, listing(&cluster.services_primary));
    assert_eq!(users_view, listing(&cluster.replica));

    cluster.close();
}

#[test]
fn sifs_cluster_posix_directory_rename_across_shard_primaries_recovers_with_subtree_lock() {
    let mut cluster = sifs_two_primary_cluster("native-subtree-carrier-recovery");
    let from = "/users/recovery-tree";
    let to = "/services/recovery-tree";
    cluster.users_primary.posix_mkdir_all(from).unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]);
    cluster
        .users_primary
        .posix_write_file(&format!("{from}/child"), b"frozen child", options.clone())
        .unwrap();
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.users_primary_node.clone()),
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let operation = sinomo_core::Id::generate().to_string();
    assert!(
        cluster
            .users_primary
            .posix_rename_with_operation_id(from, to, &operation)
            .is_err()
    );
    let blocked_write =
        cluster
            .users_primary
            .posix_write_file(&format!("{from}/child"), b"blocked", options);
    assert!(
        matches!(
            &blocked_write,
            Err(DfsPosixError::Dfs(DfsError::Busy { .. }))
        ),
        "the same subtree write remains Busy: {blocked_write:?}"
    );
    let blocked_mkdir = cluster
        .users_primary
        .posix_mkdir(&format!("{from}/new-dir"));
    assert!(
        matches!(
            &blocked_mkdir,
            Err(DfsPosixError::Dfs(DfsError::Busy { .. }))
        ),
        "the same subtree mkdir remains Busy: {blocked_mkdir:?}"
    );
    cluster._runtime.clear_faults();
    cluster
        .users_primary
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_read_file_distributed(&format!("{to}/child"))
            .unwrap(),
        b"frozen child"
    );
    assert!(matches!(
        cluster.replica.posix_stat_distributed(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    cluster
        .users_primary
        .posix_mkdir(from)
        .expect("completed real job releases the source subtree reservation");
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster.close();
}

#[test]
fn sifs_cluster_posix_cross_shard_move_fences_open_write_intents() {
    let mut cluster = sifs_two_primary_cluster("native-cross-shard-open-write-fence");
    let from = "/users/write-fenced";
    let to = "/services/write-fenced";
    let original = cluster
        .users_primary
        .posix_write_file(
            from,
            b"original",
            DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]),
        )
        .unwrap();
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: None,
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let before_transfer = cluster._runtime.rpc_observations().len();
    let target = DfsVersion::generate();
    let content_nodes = vec![
        cluster.users_primary_node.clone(),
        cluster.services_primary_node.clone(),
    ];
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = content_nodes.clone();
    let outcome = cluster
        .users_primary
        .write_distributed_eventual(
            DfsWriteRequest {
                file_system_id: original.file_system_id.clone(),
                object_id: original.object_id.clone(),
                target_version: target.clone(),
                requesting_node_id: cluster.users_primary_node.clone(),
            },
            b"pending actual W2 replacement".to_vec(),
            content_nodes,
            policy,
        )
        .unwrap();
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    assert!(
        cluster._runtime.rpc_observations()[before_transfer..]
            .iter()
            .any(|event| event.source == cluster.users_primary_node
                && event.target == cluster.services_primary_node
                && event.route == DFS_DATA_STAGE_BLOB_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch),
        "the original pending write must reach the actual faulted required target"
    );
    assert_eq!(
        cluster.users_primary.posix_stat(from).unwrap().version,
        original.version,
        "the genuinely incomplete W2 write cannot advance the visible head"
    );
    let rename = cluster.users_primary.posix_rename(from, to);
    assert!(
        matches!(&rename, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "users={}, services={}, replica={}; actual rename={rename:?}; actual transfers={:?}",
        cluster.users_primary_node,
        cluster.services_primary_node,
        cluster.replica_node,
        cluster._runtime.rpc_observations()[before_transfer..]
            .iter()
            .filter(|event| [
                DFS_DATA_STAGE_BLOB_ROUTE,
                DFS_DATA_REPAIR_REPLICA_ROUTE,
                DFS_DATA_FETCH_BLOB_ROUTE
            ]
            .contains(&event.route.as_str()))
            .map(|event| (
                &event.source,
                &event.target,
                &event.route,
                event.phase,
                &event.dfs_intent_id,
                &event.dfs_request_id
            ))
            .collect::<Vec<_>>()
    );
    assert!(
        !cluster
            .users_primary
            .metadata_backend()
            .unwrap()
            .list_namespace_jobs()
            .unwrap()
            .iter()
            .any(|job| job.root_path == from && job.status == DfsNamespaceJobStatus::Running)
    );
    cluster._runtime.clear_faults();
    let deadline = Instant::now() + DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
    while cluster.users_primary.posix_stat(from).unwrap().version != target {
        cluster
            .users_primary
            .retry_pending_repairs_bounded(8)
            .unwrap();
        cluster.users_primary.process_incoming().unwrap();
        assert!(
            Instant::now() < deadline,
            "actual stage completion must settle within its normal deadline"
        );
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    cluster.users_primary.posix_rename(from, to).unwrap();
    assert_eq!(
        cluster.replica.posix_read_file_distributed(to).unwrap(),
        b"pending actual W2 replacement"
    );
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster.close();
}

#[test]
fn sifs_cluster_posix_cross_shard_move_allows_committed_pending_repair() {
    let mut cluster = sifs_two_primary_cluster("native-move-with-real-repair");
    let from = "/users/repair-pending";
    let to = "/services/repair-pending";
    // The repair queue belongs to the actual effect producer, not the routed
    // Main requester shell held by the common fixture.
    cluster.users_primary = cluster
        ._runtime
        .metadata_primary_service(from)
        .expect("actual users Core repair producer");
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: None,
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let source = cluster
        .users_primary
        .posix_write_file(
            from,
            b"committed repair-pending bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: vec![
                    cluster.users_primary_node.clone(),
                    cluster.services_primary_node.clone(),
                ],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert!(
        cluster
            .users_primary
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.object_id == source.object_id)
    );
    let moved = cluster.replica.posix_rename(from, to).unwrap();
    assert_eq!(moved.object_id, source.object_id);
    assert_eq!(moved.content_hash, source.content_hash);
    cluster._runtime.clear_faults();
    assert_eq!(
        cluster.replica.posix_read_file_distributed(to).unwrap(),
        b"committed repair-pending bytes"
    );
    assert!(matches!(
        cluster.replica.posix_stat(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster.close();
}

#[test]
fn sifs_cluster_posix_rejects_adoption_from_non_source_primary() {
    let mut cluster = sifs_two_primary_cluster("native-rogue-source-adoption");
    let from = "/users/rogue-source";
    let to = "/services/rogue-target";
    let source = cluster
        .users_primary
        .posix_write_file(
            from,
            b"actual source bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]),
        )
        .unwrap();
    let pause = cluster
        ._runtime
        .pause_remote_namespace_phase(&cluster.users_primary_node)
        .unwrap();
    let mut requester = cluster._runtime.client(&cluster.replica_node).unwrap();
    let shard = DfsMetadataShardId::new("services");
    let before = cluster
        .services_primary
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap();
    std::thread::scope(|scope| {
        let original = scope.spawn(move || requester.posix_rename(from, to));
        let captured = pause.wait_captured(Duration::from_secs(5));
        let outcome = captured.as_ref().ok().map(|captured| {
            let mut tampered = captured.clone();
            tampered.routed_message.source_node_id = cluster.replica_node.clone();
            let DfsNetworkMessage::NamespaceRequest(command) = &mut tampered.routed_message.message
            else {
                panic!("actual namespace source phase");
            };
            assert!(matches!(
                command.operation,
                DfsNamespaceOperation::AdoptObjectAtPath { .. }
            ));
            command.requesting_node_id = cluster.replica_node.clone();
            let request = ObjectRpcRequest::encode_body(
                DFS_DATA_NAMESPACE_ROUTE,
                DFS_RPC_CATEGORY,
                &tampered,
            )
            .unwrap();
            cluster._runtime.rpc_request(
                &cluster.replica_node,
                &cluster.services_primary_node,
                request,
                Duration::from_secs(5),
            )
        });
        let unchanged = cluster
            .services_primary
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap();
        pause.release();
        let completed = original.join().unwrap();
        captured.expect("original native job/source offer captured");
        let rejected = outcome
            .unwrap()
            .expect_err("the actual source offer rejects a different authenticated coordinator");
        assert_eq!(
            rejected,
            "network I/O error during shared DFS native service dispatch: invalid DFS runtime operation: metadata backend error: source adoption requester is not its original coordinator"
        );
        assert_eq!(
            unchanged, before,
            "rejected signer cannot consume the real source/job offer or publish a destination"
        );
        assert_eq!(completed.unwrap().object_id, source.object_id);
    });
    assert_eq!(
        cluster.replica.posix_read_file_distributed(to).unwrap(),
        b"actual source bytes"
    );
    assert_eq!(cluster._runtime.observation_dropped_count(), 0);
    cluster.close();
}

#[test]
fn native_namespace_job_resume_honors_step_budget() {
    let node = NodeId::generate();
    let (runtime, mut service) =
        native_sifs_service("native-namespace-job-step-budget", node.clone());
    let from = "/services/budget-source";
    let to = "/services/budget-target";
    service.posix_mkdir_all(from).unwrap();
    for index in 0..3 {
        service
            .posix_write_file(
                &format!("{from}/file-{index}"),
                format!("budget {index}").as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
    }
    drop(service);
    let mut service = runtime
        .metadata_primary_service(from)
        .expect("actual services Core bounded namespace producer");
    let mut job = service.posix_rename_directory_bounded(from, to, 1).unwrap();
    assert_eq!(job.status, DfsNamespaceJobStatus::Running);
    let mut rounds = 1;
    while job.status == DfsNamespaceJobStatus::Running {
        let previous = job.processed_object_count;
        job = service
            .posix_resume_namespace_job_bounded(&job.job_id, 1)
            .unwrap();
        assert!(
            job.processed_object_count.saturating_sub(previous) <= 1,
            "one execution step may process at most one actual namespace object"
        );
        rounds += 1;
        assert!(rounds < 64, "the bounded current job must converge");
    }
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
    assert!(rounds >= 3);
    for index in 0..3 {
        assert_eq!(
            service
                .posix_read_file(&format!("{to}/file-{index}"))
                .unwrap(),
            format!("budget {index}").as_bytes()
        );
    }
    assert!(matches!(
        service.posix_stat(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn same_node_cross_shard_pin_family_survives_exact_replay_and_owner_restart() {
    let node = NodeId::generate();
    let (mut runtime, mut service) =
        native_sifs_service("native-same-node-pinned-move", node.clone());
    let source_path = "/users/pinned-source";
    let target_path = "/services/pinned-target";
    let source = service
        .posix_write_file(
            source_path,
            b"pinned source bytes",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let identity = DfsRevisionReference {
        file_system_id: source.file_system_id.clone(),
        object_id: source.object_id.clone(),
        version: source.version.clone(),
    };
    service
        .retain_content_revision(DfsContentReferenceRequest {
            namespace: "org.example.native-pin".into(),
            path: source_path.into(),
            identity: identity.clone(),
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "current-pin".into(),
                metadata: None,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
    let pins = service
        .metadata_backend()
        .unwrap()
        .version_pins_for_object(&source.object_id)
        .unwrap();
    assert_eq!(pins.len(), 1);
    let operation = sinomo_core::Id::generate().to_string();
    let moved = service
        .posix_rename_with_operation_id(source_path, target_path, &operation)
        .unwrap();
    assert_eq!(moved.object_id, source.object_id);
    assert_eq!(
        service
            .posix_rename_with_operation_id(source_path, target_path, &operation)
            .unwrap(),
        moved
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap(),
        pins
    );
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .cross_shard_pin_transfers_for_object(&source.object_id)
            .unwrap()
            .is_empty()
    );
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut service = runtime.client(&node).unwrap();
    assert_eq!(
        service.posix_read_file(target_path).unwrap(),
        b"pinned source bytes"
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap(),
        pins
    );
    assert_eq!(
        service
            .posix_rename_with_operation_id(source_path, target_path, &operation)
            .unwrap()
            .object_id,
        source.object_id
    );
    assert_eq!(
        service
            .read_file_version_content(
                &identity,
                source.content_hash.as_deref().unwrap(),
                source.size_bytes
            )
            .unwrap(),
        b"pinned source bytes"
    );
    drop(service);
    runtime.close().unwrap();
}

// Shared fault choreography uses existing native transports, jobs and owners.
fn native_cross_shard_move_recovers_from_real_carrier_failure(
    name: &str,
    restart_source: bool,
    explicit_resume: bool,
) {
    let mut cluster = sifs_two_primary_cluster(name);
    let from = "/users/native-carrier-source";
    let to = "/services/native-carrier-target";
    let original = cluster
        .users_primary
        .posix_write_file(
            from,
            b"real carrier fault data",
            DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]),
        )
        .unwrap();
    let operation = sinomo_core::Id::generate().to_string();
    let baseline = cluster._runtime.rpc_observations().len();
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.users_primary_node.clone()),
            target: Some(cluster.services_primary_node.clone()),
            route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let started = Instant::now();
    assert!(
        cluster
            .users_primary
            .posix_rename_with_operation_id(from, to, &operation)
            .is_err()
    );
    assert!(
        started.elapsed() < DEFAULT_DFS_POSIX_SYNC_TIMEOUT,
        "failed real carrier is bounded by the normal operation deadline"
    );
    assert!(
        cluster
            ._runtime
            .rpc_observations()
            .iter()
            .skip(baseline)
            .any(|event| event.source == cluster.users_primary_node
                && event.target == cluster.services_primary_node
                && event.route == DFS_DATA_NAMESPACE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse)
    );
    let jobs = cluster
        .users_primary
        .metadata_backend()
        .unwrap()
        .list_namespace_jobs()
        .unwrap();
    let parked = jobs
        .into_iter()
        .find(|job| job.root_path == from && job.target_path.as_deref() == Some(to))
        .expect("actual durable coordinator after transport failure");
    assert_eq!(parked.status, DfsNamespaceJobStatus::Running);
    let blocked_write = cluster.users_primary.posix_write_file(
        from,
        b"must not race parked job",
        DfsWriteOptions::replicated_eventual(vec![cluster.users_primary_node.clone()]),
    );
    assert!(
        matches!(
            &blocked_write,
            Err(DfsPosixError::Dfs(DfsError::Busy { .. }))
        ),
        "the same write against the actual parked job remains Busy: {blocked_write:?}"
    );
    cluster._runtime.clear_faults();
    if restart_source {
        let node = cluster.users_primary_node.clone();
        drop(cluster.users_primary);
        cluster
            ._runtime
            .stop_node(&node)
            .expect("real source component shutdown");
        cluster
            ._runtime
            .reopen_node(node.clone())
            .expect("real source owner restart");
        cluster.users_primary = cluster._runtime.client(&node).unwrap();
    }
    if explicit_resume {
        let terminal = cluster
            .users_primary
            .posix_resume_namespace_job_bounded_routed(&parked.job_id, from, 512)
            .unwrap();
        assert_eq!(terminal.status, DfsNamespaceJobStatus::Completed);
    }
    let completed = cluster
        .users_primary
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(completed.object_id, original.object_id);
    assert_eq!(
        cluster
            .users_primary
            .posix_rename_with_operation_id(from, to, &operation)
            .unwrap()
            .object_id,
        original.object_id
    );
    assert_eq!(
        cluster.replica.posix_read_file_distributed(to).unwrap(),
        b"real carrier fault data"
    );
    assert!(matches!(
        cluster.replica.posix_stat_distributed(from),
        Err(DfsPosixError::NotFound { .. })
    ));
    let terminal = cluster
        .users_primary
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&parked.job_id)
        .unwrap();
    if let Some(terminal) = terminal {
        assert_eq!(terminal.status, DfsNamespaceJobStatus::Completed);
    }
    cluster._runtime.assert_fault_budgets().unwrap();
    assert_eq!(cluster._runtime.observation_dropped_count(), 0);
    cluster.close();
}

#[test]
fn same_node_distinct_shard_hardlink_alias_uses_durable_saga_and_survives_cold_replay() {
    let node = NodeId::generate();
    let (mut runtime, mut service) =
        native_sifs_service("native-same-node-alias-original-readset", node.clone());
    let canonical_path = "/users/local-physical-canonical";
    let alias_path = "/services/local-physical-alias";
    let renamed_path = "/services/local-physical-renamed";
    let configs = runtime.metadata_shards().unwrap();
    let source = crate::select_metadata_shard_config_for_path(
        &sifs_file_system_id(),
        &configs,
        canonical_path,
    )
    .unwrap()
    .unwrap();
    let destination =
        crate::select_metadata_shard_config_for_path(&sifs_file_system_id(), &configs, alias_path)
            .unwrap()
            .unwrap();
    assert_ne!(source.shard_id, destination.shard_id);
    assert_eq!(source.primary_node_id, node);
    assert_eq!(destination.primary_node_id, node);
    let canonical = service
        .posix_write_file(
            canonical_path,
            b"co-located distinct writer bytes",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let cursor = runtime
        .rpc_observations()
        .iter()
        .map(|event| event.call_id)
        .max()
        .unwrap_or(0);
    let actual_namespace_heads = runtime.capture_namespace_requests(&node, 32).unwrap();
    service.posix_hardlink(canonical_path, alias_path).unwrap();
    assert_eq!(
        service
            .posix_stat_distributed(canonical_path)
            .unwrap()
            .link_count,
        2
    );
    let alias = service
        .posix_namespace_lstat_distributed(alias_path)
        .unwrap();
    assert_eq!(alias.kind, DfsObjectKind::Hardlink);
    let alias_only = hardlink_read_set_snapshot(&runtime, &node, "services").unwrap();
    assert!(
        alias_only
            .tables
            .objects
            .iter()
            .any(|object| object.path == alias_path && object.kind == DfsObjectKind::Hardlink)
    );
    assert!(
        !alias_only
            .tables
            .objects
            .iter()
            .any(|object| object.path == canonical_path),
        "co-located primaries retain separate physical writers"
    );
    let renamed = service.posix_rename(alias_path, renamed_path).unwrap();
    assert_eq!(renamed.object_id, canonical.object_id);
    let renamed_alias = service
        .posix_namespace_lstat_distributed(renamed_path)
        .unwrap();
    assert_eq!(renamed_alias.object_id, alias.object_id);
    assert_ne!(renamed_alias.version, alias.version);
    let operation_id = sinomo_core::Id::generate().to_string();
    let expected = DfsNamespaceObjectPrecondition {
        object_id: renamed_alias.object_id.clone(),
        version: renamed_alias.version.clone(),
    };
    let deleted = service
        .posix_unlink_if_generation(renamed_path, operation_id.clone(), expected.clone())
        .unwrap();
    assert_eq!(deleted.kind, DfsObjectKind::Tombstone);
    assert_eq!(
        service
            .posix_stat_distributed(canonical_path)
            .unwrap()
            .link_count,
        1
    );
    let source_cut = hardlink_read_set_snapshot(&runtime, &node, "services").unwrap();
    let original_frame = recorded_hardlink_alias_frame(&source_cut, &operation_id);
    let DfsNamespaceJobFrame::CrossShardHardlinkAliasCommit {
        transaction,
        shard_id,
        ..
    } = &original_frame
    else {
        unreachable!()
    };
    assert_eq!(shard_id, &DfsMetadataShardId::new(destination.shard_id));
    let prepared_id = crate::metadata_records::compound_prepared_operation_id(
        &operation_id,
        &transaction.transaction_id,
    );
    let original = source_cut
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| record.payload.operation_id == prepared_id)
        .unwrap()
        .clone();
    original.validate().unwrap();
    assert_eq!(original.status, DfsPreparedOperationStatus::Committed);
    let heads = actual_namespace_heads.records();
    drop(actual_namespace_heads);
    let observations = runtime.rpc_observations();
    let window = observations
        .iter()
        .filter(|event| event.call_id > cursor)
        .collect::<Vec<_>>();
    // Preserve actual calls and native command bodies BEFORE classification.
    // RegisteredHost includes ordinary same-node caller delivery to local Main;
    // source==target alone does not identify a saga mutation or a peer socket.
    let evidence = serde_json::json!({
        "cursor": cursor,
        "node": node,
        "omitted_rpc_observations": runtime.observation_dropped_count(),
        "native_namespace_heads": heads,
        "calls": window.iter().map(|event| serde_json::json!({
            "call_id": event.call_id, "source": event.source, "target": event.target,
            "route": event.route, "origin": format!("{:?}", event.origin),
            "phase": format!("{:?}", event.phase), "frame_id": event.dfs_frame_id,
            "request_id": event.dfs_request_id, "file_system_id": event.dfs_file_system_id,
            "streamed_value": event.streamed_value, "body_bytes": event.body_bytes,
            "options": format!("{:?}", event.options),
            "observe_connectivity": event.observe_connectivity,
        })).collect::<Vec<_>>(),
    });
    let evidence_path = runtime
        .data_dir(&node)
        .root()
        .join("same-node-alias-call-boundaries.json");
    let mut evidence_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&evidence_path)
        .unwrap();
    std::io::Write::write_all(
        &mut evidence_file,
        &serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    evidence_file.sync_all().unwrap();
    drop(evidence_file);
    eprintln!(
        "actual same-node alias call boundaries: {}",
        evidence_path.display()
    );
    let self_calls = window
        .iter()
        .copied()
        .filter(|event| {
            event.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                && event.route == DFS_DATA_NAMESPACE_ROUTE
                && event.phase == crate::test_runtime::RpcPhase::Constructed
                && event.source == event.target
        })
        .collect::<Vec<_>>();
    assert!(
        !self_calls.is_empty(),
        "normal caller/read delivery reaches the actual local Main"
    );
    let mut caller_hardlink = false;
    let mut caller_rename = false;
    let mut caller_unlink = false;
    let mut owner_read = false;
    for event in &self_calls {
        assert_eq!(event.source, node);
        let decoded = window
            .iter()
            .filter(|phase| {
                phase.call_id == event.call_id
                    && phase.phase == crate::test_runtime::RpcPhase::Decoded
            })
            .copied()
            .collect::<Vec<_>>();
        let decoded_head = decoded
            .first()
            .expect("same carrier invocation has an actual decoded native namespace head");
        assert!(
            decoded
                .iter()
                .all(|head| head.dfs_frame_id == decoded_head.dfs_frame_id
                    && head.dfs_request_id == decoded_head.dfs_request_id
                    && head.dfs_file_system_id == decoded_head.dfs_file_system_id),
            "one actual call changes its decoded identity"
        );
        let request_id = decoded_head
            .dfs_request_id
            .as_ref()
            .expect("actual decoded namespace request id");
        let frame_id = decoded_head
            .dfs_frame_id
            .expect("actual decoded namespace frame id");
        let fs = decoded_head
            .dfs_file_system_id
            .as_ref()
            .expect("actual decoded namespace filesystem");
        let matched = heads
            .iter()
            .filter(|head| {
                head.source_node_id == event.source
                    && head.target_node_id == event.target
                    && head.frame_id == frame_id
                    && matches!(&head.message, DfsNetworkMessage::NamespaceRequest(request)
                    if &request.request_id == request_id && &request.file_system_id == fs)
            })
            .collect::<Vec<_>>();
        let first = matched
            .first()
            .expect("same actual call has a native BeforeExecute head");
        assert!(
            matched.iter().all(|head| *head == *first),
            "one incoming identity changes its full native command"
        );
        let DfsNetworkMessage::NamespaceRequest(request) = &first.message else {
            unreachable!()
        };
        assert_eq!(request.requesting_node_id, node);
        assert!(window.iter().any(|phase| phase.call_id == event.call_id
            && phase.phase == crate::test_runtime::RpcPhase::BeforeDispatch));
        assert!(window.iter().any(|phase| phase.call_id == event.call_id
            && phase.phase == crate::test_runtime::RpcPhase::Completed));
        match &request.operation {
            DfsNamespaceOperation::LookupPath { .. }
            | DfsNamespaceOperation::LookupPaths { .. }
            | DfsNamespaceOperation::PollNamespaceChanges { .. } => owner_read = true,
            DfsNamespaceOperation::MetadataQuery { query }
                if matches!(query.selection, DfsMetadataSelection::StatAtPathV1 { .. }) =>
            {
                let DfsMetadataSelection::StatAtPathV1 { request: stat } = &query.selection else {
                    unreachable!()
                };
                assert_eq!(query.file_system_id, request.file_system_id);
                assert!(!stat.read_id.is_empty());
                assert_eq!(
                    stat.catalog_digest,
                    crate::stat_projection::stat_catalog_digest(&configs).unwrap()
                );
                assert_eq!(request.authority_shards, configs);
                assert!(
                    request
                        .authority_shards
                        .iter()
                        .all(|shard| shard.primary_node_id == node)
                );
                assert!(!request.include_metadata_journal_batches);
                assert!(request.metadata_journal_after_sequences.is_empty());
                let selected =
                    crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                        &query.file_system_id,
                        &configs,
                        &stat.path,
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(query.shard_id.0, selected.shard_id);
                assert_eq!(query.epoch, selected.epoch);
                owner_read = true;
            }
            DfsNamespaceOperation::Hardlink {
                existing_path,
                link_path,
            } if existing_path == canonical_path && link_path == alias_path => {
                caller_hardlink = true
            }
            DfsNamespaceOperation::Rename { from, to }
                if from == alias_path && to == renamed_path =>
            {
                caller_rename = true
            }
            DfsNamespaceOperation::ConditionalUnlink {
                path,
                expected: actual,
            } if path == renamed_path
                && actual == &expected
                && request.request_id == operation_id =>
            {
                caller_unlink = true
            }
            operation => panic!(
                "unexpected self carrier mutation phase: call {} {:?}",
                event.call_id, operation
            ),
        }
    }
    assert!(
        caller_hardlink && caller_rename && caller_unlink && owner_read,
        "observe exact normal caller effects and read-only local Main dispatch separately"
    );
    // These actual durable mutation phase commands must remain local Core work;
    // an RPC origin label or successful outcome cannot exempt a self carrier call.
    for head in &heads {
        if matches!(&head.message, DfsNetworkMessage::NamespaceRequest(request)
            if matches!(request.operation,
                DfsNamespaceOperation::InstallHardlinkTargetFence { .. }
                    | DfsNamespaceOperation::CommitHardlinkTargetFence { .. }
                    | DfsNamespaceOperation::AdoptHardlinkAlias { .. }
                    | DfsNamespaceOperation::AdoptObjectAtPath { .. }))
        {
            assert!(
                !self_calls
                    .iter()
                    .any(|event| event.source == head.source_node_id
                        && event.target == head.target_node_id
                        && window.iter().any(|decoded| decoded.call_id == event.call_id
                            && decoded.phase == crate::test_runtime::RpcPhase::Decoded
                            && decoded.dfs_frame_id == Some(head.frame_id)
                            && matches!(&head.message, DfsNetworkMessage::NamespaceRequest(request)
                            if decoded.dfs_request_id.as_ref() == Some(&request.request_id)))),
                "co-located saga mutation phases cannot dispatch through the self carrier"
            );
        }
    }
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let restored = hardlink_read_set_snapshot(&runtime, &node, "services").unwrap();
    assert_eq!(
        recorded_hardlink_alias_frame(&restored, &operation_id),
        original_frame
    );
    assert_eq!(
        restored
            .tables
            .prepared_operations_v7
            .iter()
            .find(|record| record.payload.operation_id == prepared_id)
            .unwrap(),
        &original
    );
    let mut reopened = runtime.client(&node).unwrap();
    assert_eq!(
        reopened
            .posix_unlink_if_generation(renamed_path, operation_id, expected)
            .unwrap()
            .kind,
        DfsObjectKind::Tombstone
    );
    assert_eq!(
        reopened
            .posix_stat_distributed(canonical_path)
            .unwrap()
            .link_count,
        1
    );
    assert_eq!(
        reopened
            .posix_read_file_distributed(canonical_path)
            .unwrap(),
        b"co-located distinct writer bytes"
    );
    assert!(matches!(
        reopened.posix_stat_distributed(renamed_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(reopened);
    runtime.close().unwrap();
}
