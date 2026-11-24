use super::*;

pub(super) fn foreign_parent_cluster(name: &str) -> SifsTwoPrimaryCluster {
    let mut cluster = sifs_two_primary_cluster(name);
    let configs = cluster._runtime.metadata_shards().unwrap();
    for config in configs {
        let primary = if config.shard_id == "root" {
            cluster.replica_node.clone()
        } else {
            config.primary_node_id
        };
        cluster
            ._runtime
            .reassign_metadata_shard(
                &config.file_system_id,
                &config.shard_id,
                primary,
                Vec::new(),
            )
            .unwrap();
    }
    cluster
}

fn directory_row(
    cluster: &SifsTwoPrimaryCluster,
    owner: &NodeId,
    shard: &str,
    path: &str,
) -> DfsObjectMetadata {
    hardlink_read_set_snapshot(&cluster._runtime, owner, shard)
        .unwrap()
        .tables
        .objects
        .into_iter()
        .find(|object| object.path == path && object.kind == DfsObjectKind::Directory)
        .unwrap()
}

#[test]
fn native_foreign_parent_directory_move_counts_are_owned_and_replay_exactly() {
    let mut cluster = foreign_parent_cluster("native-foreign-parent-counts");
    cluster.replica.posix_mkdir("/users/counted-child").unwrap();
    let root_before = directory_row(&cluster, &cluster.replica_node, "root", "/");
    let target_before = directory_row(
        &cluster,
        &cluster.services_primary_node,
        "services",
        "/services",
    );
    let source_before = directory_row(&cluster, &cluster.users_primary_node, "users", "/users");
    let operation_id = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::Rename {
        from: "/users".into(),
        to: "/services/moved-users".into(),
    };
    cluster
        .replica
        .route_namespace_operation_with_request_id(
            operation_id.clone(),
            cluster.users_primary_node.clone(),
            operation.clone(),
        )
        .unwrap();
    let root_after = directory_row(&cluster, &cluster.replica_node, "root", "/");
    let target_after = directory_row(
        &cluster,
        &cluster.services_primary_node,
        "services",
        "/services",
    );
    let moved = directory_row(
        &cluster,
        &cluster.services_primary_node,
        "services",
        "/services/moved-users",
    );
    assert_eq!(
        root_after.namespace_link_count,
        root_before.namespace_link_count.map(|count| count - 1)
    );
    assert_eq!(
        target_after.namespace_link_count,
        target_before.namespace_link_count.map(|count| count + 1)
    );
    assert_eq!(moved.object_id, source_before.object_id);
    assert_eq!(
        moved.namespace_link_count,
        source_before.namespace_link_count
    );
    assert_eq!(
        directory_row(
            &cluster,
            &cluster.services_primary_node,
            "services",
            "/services/moved-users/counted-child"
        )
        .namespace_link_count,
        Some(2)
    );
    cluster
        .replica
        .route_namespace_operation_with_request_id(
            operation_id,
            cluster.users_primary_node.clone(),
            operation,
        )
        .unwrap();
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/"),
        root_after
    );
    let backend = cluster
        ._runtime
        .node_metadata(&cluster.replica_node)
        .unwrap();
    assert!(
        backend
            .parent_count_reservations(
                &DfsMetadataShardId::new("root"),
                &sifs_file_system_id(),
                &root_after.object_id
            )
            .unwrap()
            .is_empty()
    );
    drop(backend);
    cluster.close();
}

fn original_parent_job(cluster: &SifsTwoPrimaryCluster, request_id: &str) -> DfsNamespaceJobRecord {
    hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
        .unwrap()
        .tables
        .namespace_jobs
        .into_iter()
        .find(|job| {
            job.kind == DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                && job
                    .origin
                    .as_ref()
                    .is_some_and(|origin| origin.request_id == request_id)
        })
        .unwrap()
}

#[test]
fn native_rejected_root_adoption_releases_issued_parent_reservations() {
    rejected_root_adoption_case(false);
}

#[test]
fn native_rejected_root_adoption_withdrawal_restarts_exactly() {
    rejected_root_adoption_case(true);
}

fn rejected_root_adoption_case(interrupt_withdrawal: bool) {
    let mut cluster = foreign_parent_cluster(if interrupt_withdrawal {
        "native-parent-rejected-root-withdrawal-restart"
    } else { "native-parent-rejected-root-adoption" });
    let abort_hook = interrupt_withdrawal.then(|| crate::posix::install_parent_count_test_hook(
        &cluster.users_primary_node, "/users", crate::posix::ParentCountTestPoint::Aborted,
        || Err(DfsPosixError::Dfs(DfsError::Network {
            message: "parent withdrawal committed before its coordinator checkpoint".into(),
        })),
    ));
    let root_before = directory_row(&cluster, &cluster.replica_node, "root", "/");
    let source_before = directory_row(&cluster, &cluster.users_primary_node, "users", "/users");
    let target = "/services/conflicting-users";
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let hook = crate::posix::install_parent_count_test_hook(
        &cluster.users_primary_node, "/users", crate::posix::ParentCountTestPoint::Issued,
        move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            Ok(())
        },
    );
    let request = sinomo_core::Id::generate().to_string();
    let mut caller = cluster._runtime.client(&cluster.replica_node).unwrap();
    let primary = cluster.users_primary_node.clone();
    let conflict = std::thread::scope(|scope| {
        let id = request.clone();
        let call = scope.spawn(move || caller.route_namespace_operation_with_request_id(
            id, primary, DfsNamespaceOperation::Rename { from: "/users".into(), to: target.into() }));
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let conflict = (|| -> std::result::Result<_, String> {
            entered.map_err(|failure| failure.to_string())?;
            let issued = original_parent_job(&cluster, &request);
            assert_eq!(issued.directory_move_counts().unwrap().unwrap().parent_participants.as_ref().unwrap().phase,
                crate::DfsParentCountPhase::Issued);
            let mut writer = cluster._runtime.client(&cluster.services_primary_node).unwrap();
            writer.posix_mkdir(target).map_err(|failure| failure.to_string())?;
            Ok(directory_row(&cluster, &cluster.services_primary_node, "services", target))
        })();
        let _ = release_tx.send(());
        assert!(call.join().unwrap().is_err(), "the changed destination must reject the original adoption");
        conflict.unwrap()
    });
    drop(hook);
    drop(abort_hook);
    if interrupt_withdrawal {
        let pending = original_parent_job(&cluster, &request);
        assert_eq!(pending.directory_move_counts().unwrap().unwrap().parent_participants.as_ref().unwrap().phase,
            crate::DfsParentCountPhase::Aborting);
        drop(cluster.users_primary);
        drop(cluster.services_primary);
        drop(cluster.replica);
        cluster._runtime.stop_node(&cluster.users_primary_node).unwrap();
        cluster._runtime.stop_node(&cluster.replica_node).unwrap();
        cluster._runtime.reopen_node(cluster.replica_node.clone()).unwrap();
        cluster._runtime.reopen_node(cluster.users_primary_node.clone()).unwrap();
        cluster.users_primary = cluster._runtime.client(&cluster.users_primary_node).unwrap();
        cluster.services_primary = cluster._runtime.client(&cluster.services_primary_node).unwrap();
        cluster.replica = cluster._runtime.client(&cluster.replica_node).unwrap();
        assert!(cluster.replica.route_namespace_operation_with_request_id(
            request.clone(), cluster.users_primary_node.clone(),
            DfsNamespaceOperation::Rename { from: "/users".into(), to: target.into() }).is_err());
    }
    let job = original_parent_job(&cluster, &request);
    assert_eq!(job.status, DfsNamespaceJobStatus::Failed, "known root rejection must settle every parent: {:?}", job.directory_move_counts());
    assert_eq!(directory_row(&cluster, &cluster.replica_node, "root", "/"), root_before);
    assert_eq!(directory_row(&cluster, &cluster.users_primary_node, "users", "/users"), source_before);
    assert_eq!(directory_row(&cluster, &cluster.services_primary_node, "services", target), conflict);
    let backend = cluster._runtime.node_metadata(&cluster.replica_node).unwrap();
    assert!(backend.parent_count_reservations(&DfsMetadataShardId::new("root"), &root_before.file_system_id, &root_before.object_id).unwrap().is_empty());
    drop(backend);
    cluster.replica.route_namespace_operation_with_request_id(
        sinomo_core::Id::generate().to_string(), cluster.replica_node.clone(),
        DfsNamespaceOperation::SetMode { path: "/".into(), mode: 0o710 }).unwrap();
    cluster.close();
}

#[test]
fn native_parent_reservation_blocks_writes_but_not_committed_stat() {
    let mut cluster = foreign_parent_cluster("native-parent-write-only-reservation");
    let root_before = directory_row(&cluster, &cluster.replica_node, "root", "/");
    cluster
        .replica
        .posix_write_file(
            "/services/unrelated-parent-row",
            b"unrelated",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();

    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let hook = crate::posix::install_parent_count_test_hook(
        &cluster.users_primary_node,
        "/users",
        crate::posix::ParentCountTestPoint::Reserved,
        move || {
            entered_tx.send(()).unwrap();
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("bounded actual parent reservation pause");
            Ok(())
        },
    );
    let request = sinomo_core::Id::generate().to_string();
    let mut client = cluster._runtime.client(&cluster.replica_node).unwrap();
    let primary = cluster.users_primary_node.clone();
    std::thread::scope(|scope| {
        let id = request.clone();
        let call = scope.spawn(move || {
            client.route_namespace_operation_with_request_id(
                id,
                primary,
                DfsNamespaceOperation::Rename {
                    from: "/users".into(),
                    to: "/services/write-only-users".into(),
                },
            )
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let observation = (|| -> std::result::Result<_, String> {
            entered.map_err(|error| error.to_string())?;
            let job = original_parent_job(&cluster, &request);
            let plan = job
                .directory_move_counts()
                .map_err(|error| error.to_string())?
                .unwrap()
                .parent_participants
                .as_ref()
                .unwrap();
            assert_eq!(plan.phase, crate::DfsParentCountPhase::Preparing);
            let participant_id = &plan.participants[0].participant_id;
            let before_participant = cluster
                ._runtime
                .node_metadata(&cluster.replica_node)
                .unwrap()
                .get_namespace_job(participant_id)
                .unwrap();
            let maintenance = cluster
                ._runtime
                .with_main_maintenance_service(&cluster.replica_node, |owner| {
                    owner.posix_resume_namespace_jobs_bounded(4, 4)
                })
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            assert_eq!(maintenance.failed_job_count, 0);
            assert_eq!(
                cluster
                    ._runtime
                    .node_metadata(&cluster.replica_node)
                    .unwrap()
                    .get_namespace_job(participant_id)
                    .unwrap(),
                before_participant
            );
            let mut independent = cluster._runtime.client(&cluster.replica_node).unwrap();
            independent
                .route_namespace_operation_with_request_id(
                    sinomo_core::Id::generate().to_string(),
                    cluster.services_primary_node.clone(),
                    DfsNamespaceOperation::SetMode {
                        path: "/services/unrelated-parent-row".into(),
                        mode: 0o600,
                    },
                )
                .map_err(|error| error.to_string())?;

            let mut observer = cluster
                ._runtime
                .client(&cluster.services_primary_node)
                .unwrap();
            let stat = observer
                .posix_stat_distributed("/")
                .map_err(|error| error.to_string())?;
            let mutation = observer.route_namespace_operation_with_request_id(
                sinomo_core::Id::generate().to_string(),
                cluster.replica_node.clone(),
                DfsNamespaceOperation::SetMode {
                    path: "/".into(),
                    mode: 0o700,
                },
            );
            Ok((
                stat,
                mutation,
                directory_row(&cluster, &cluster.replica_node, "root", "/"),
            ))
        })();
        let _ = release_tx.send(());
        let result = call.join().unwrap();
        let (stat, mutation, unchanged) = observation.unwrap();
        assert_eq!(stat.link_count, root_before.namespace_link_count.unwrap());
        assert!(
            mutation.is_err(),
            "ordinary parent write crossed reservation: {mutation:?}"
        );
        assert_eq!(unchanged, root_before);
        result.unwrap();
    });
    drop(hook);
    cluster.close();
}

#[test]
fn native_parent_abort_is_terminal_before_late_reserve_or_apply() {
    let mut cluster = foreign_parent_cluster("native-parent-before-effect-abort");
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let hook = crate::posix::install_parent_count_test_hook(
        &cluster.users_primary_node,
        "/users",
        crate::posix::ParentCountTestPoint::Declared,
        move || {
            entered_tx.send(()).unwrap();
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("bounded original declaration pause");
            Ok(())
        },
    );
    let request = sinomo_core::Id::generate().to_string();
    let mut caller = cluster._runtime.client(&cluster.replica_node).unwrap();
    let primary = cluster.users_primary_node.clone();
    let (declared, changed) = std::thread::scope(|scope| {
        let id = request.clone();
        let call = scope.spawn(move || {
            caller.route_namespace_operation_with_request_id(
                id,
                primary,
                DfsNamespaceOperation::Rename {
                    from: "/users".into(),
                    to: "/services/aborted-users".into(),
                },
            )
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let observation = (|| -> std::result::Result<_, String> {
            entered.map_err(|error| error.to_string())?;
            let declared = original_parent_job(&cluster, &request);
            let mut writer = cluster
                ._runtime
                .client(&cluster.services_primary_node)
                .unwrap();
            writer
                .route_namespace_operation_with_request_id(
                    sinomo_core::Id::generate().to_string(),
                    cluster.replica_node.clone(),
                    DfsNamespaceOperation::SetMode {
                        path: "/".into(),
                        mode: 0o710,
                    },
                )
                .map_err(|error| error.to_string())?;
            Ok((
                declared,
                directory_row(&cluster, &cluster.replica_node, "root", "/"),
            ))
        })();
        let _ = release_tx.send(());
        let result = call.join().unwrap();
        assert!(
            result.is_err(),
            "changed parent must reject before namespace issuance"
        );
        assert!(matches!(result, Err(DfsPosixError::Conflict { .. })),
            "settled pre-effect parent contention must retain a structured conflict: {result:?}");
        observation.unwrap()
    });
    drop(hook);
    let participant = declared
        .directory_move_counts()
        .unwrap()
        .unwrap()
        .parent_participants
        .as_ref()
        .unwrap()
        .participants[0]
        .clone();
    let final_job = original_parent_job(&cluster, &request);
    assert_eq!(final_job.status, DfsNamespaceJobStatus::Failed);
    let terminal_request = cluster
        ._runtime
        .node_metadata(&cluster.users_primary_node)
        .unwrap()
        .get_namespace_request(&cluster.replica_node, &request)
        .unwrap()
        .unwrap();
    assert_eq!(
        terminal_request.status,
        DfsNamespaceRequestStatus::Completed,
        "fully withdrawn pre-effect abort has a terminal caller outcome"
    );
    assert!(matches!(
        terminal_request.result,
        Some(DfsNamespaceResult::Rejected { .. })
    ));

    assert_eq!(final_job.processed_object_count, 0);
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/"),
        changed
    );
    assert_eq!(
        directory_row(&cluster, &cluster.users_primary_node, "users", "/users").kind,
        DfsObjectKind::Directory
    );
    let mut sender = cluster
        ._runtime
        .client(&cluster.users_primary_node)
        .unwrap();
    for action in [
        crate::DfsParentCountAction::Reserve,
        crate::DfsParentCountAction::Apply,
    ] {
        let result = sender
            .route_namespace_operation_with_request_id_and_result(
                sinomo_core::Id::generate().to_string(),
                cluster.replica_node.clone(),
                DfsNamespaceOperation::ParentCountParticipant {
                    coordinator_job: Box::new(declared.clone()),
                    participant: Box::new(participant.clone()),
                    action,
                },
            )
            .unwrap();
        assert!(matches!(
            result,
            Some(DfsPosixCoordinationResult::ParentCountParticipant {
                outcome: crate::DfsParentCountOutcome::Aborted,
                ..
            })
        ));
    }
    let mut wrong = participant.clone();
    wrong.successor.namespace_link_count =
        wrong.successor.namespace_link_count.map(|count| count + 1);
    assert!(
        sender
            .route_namespace_operation_with_request_id_and_result(
                sinomo_core::Id::generate().to_string(),
                cluster.replica_node.clone(),
                DfsNamespaceOperation::ParentCountParticipant {
                    coordinator_job: Box::new(declared.clone()),
                    participant: Box::new(wrong),
                    action: crate::DfsParentCountAction::Reserve
                }
            )
            .is_err()
    );
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/"),
        changed
    );
    assert!(
        cluster
            ._runtime
            .node_metadata(&cluster.replica_node)
            .unwrap()
            .parent_count_reservations(
                &DfsMetadataShardId::new("root"),
                &sifs_file_system_id(),
                &changed.object_id
            )
            .unwrap()
            .is_empty()
    );
    for (label, mut altered, mut supplied) in [
        ("wrong epoch", participant.clone(), declared.clone()),
        ("wrong job", participant.clone(), declared.clone()),
        (
            "new participant identity",
            participant.clone(),
            declared.clone(),
        ),
    ] {
        match label {
            "wrong epoch" => {
                altered.parent_scope.epoch += 1;
                altered.participant_id = altered.identity().unwrap();
            }
            "wrong job" => {
                supplied.job_id = format!("other-original-job:{}", sinomo_core::Id::generate());
                altered.binding.job_id = supplied.job_id.clone();
                altered.participant_id = altered.identity().unwrap();
            }
            _ => altered.participant_id = sinomo_core::Id::generate().to_string(),
        }
        for frame in &mut supplied.pending_frames {
            if let DfsNamespaceJobFrame::CrossShardDirFinish {
                count_plan: Some(counts),
                ..
            } = frame
            {
                counts.parent_participants.as_mut().unwrap().participants[0] = altered.clone();
            }
        }
        assert!(
            sender
                .route_namespace_operation_with_request_id_and_result(
                    sinomo_core::Id::generate().to_string(),
                    cluster.replica_node.clone(),
                    DfsNamespaceOperation::ParentCountParticipant {
                        coordinator_job: Box::new(supplied),
                        participant: Box::new(altered),
                        action: crate::DfsParentCountAction::Reserve
                    }
                )
                .is_err(),
            "{label}"
        );
    }
    let mut undeclared = participant.clone();
    undeclared.expected = directory_row(
        &cluster,
        &cluster.services_primary_node,
        "services",
        "/services",
    );
    undeclared.successor = undeclared.expected.clone();
    undeclared.successor.version = crate::namespace::next_version(&undeclared.expected.version);
    undeclared.successor.namespace_link_count = undeclared
        .expected
        .namespace_link_count
        .map(|count| count + 1);
    undeclared.delta = 1;
    undeclared.parent_scope = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|scope| scope.shard_id == "services")
        .unwrap();
    undeclared.participant_id = undeclared.identity().unwrap();
    assert!(
        sender
            .route_namespace_operation_with_request_id_and_result(
                sinomo_core::Id::generate().to_string(),
                cluster.services_primary_node.clone(),
                DfsNamespaceOperation::ParentCountParticipant {
                    coordinator_job: Box::new(declared.clone()),
                    participant: Box::new(undeclared),
                    action: crate::DfsParentCountAction::Reserve
                }
            )
            .is_err(),
        "undeclared parent"
    );
    assert!(
        cluster
            .replica
            .route_namespace_operation_with_request_id(
                request.clone(),
                cluster.users_primary_node.clone(),
                DfsNamespaceOperation::Rename {
                    from: "/users".into(),
                    to: "/services/aborted-users".into()
                },
            )
            .is_err(),
        "same caller identity must retain the original aborted outcome"
    );
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/"),
        changed
    );
    let distinct = sinomo_core::Id::generate().to_string();
    cluster
        .replica
        .route_namespace_operation_with_request_id(
            distinct.clone(),
            cluster.users_primary_node.clone(),
            DfsNamespaceOperation::Rename {
                from: "/users".into(),
                to: "/services/aborted-users".into(),
            },
        )
        .unwrap();
    assert_ne!(
        original_parent_job(&cluster, &distinct).job_id,
        final_job.job_id
    );
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/").namespace_link_count,
        changed.namespace_link_count.map(|count| count - 1)
    );
    drop(sender);
    cluster.close();
}

#[test]
fn native_parent_applied_reply_loss_restarts_without_resetting_later_parent() {
    let mut cluster = foreign_parent_cluster("native-parent-applied-reply-loss");
    let original = directory_row(&cluster, &cluster.replica_node, "root", "/");
    let hook = crate::posix::install_parent_count_test_hook(
        &cluster.users_primary_node,
        "/users",
        crate::posix::ParentCountTestPoint::Applied,
        || {
            Err(DfsPosixError::Dfs(DfsError::Network {
                message: "original parent committed before coordinator checkpoint".into(),
            }))
        },
    );
    let request = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::Rename {
        from: "/users".into(),
        to: "/services/replayed-users".into(),
    };
    assert!(
        cluster
            .replica
            .route_namespace_operation_with_request_id(
                request.clone(),
                cluster.users_primary_node.clone(),
                operation.clone()
            )
            .is_err()
    );
    drop(hook);
    let after = directory_row(&cluster, &cluster.replica_node, "root", "/");
    assert_eq!(
        after.namespace_link_count,
        original.namespace_link_count.map(|count| count - 1)
    );
    let mut writer = cluster
        ._runtime
        .client(&cluster.services_primary_node)
        .unwrap();
    writer
        .route_namespace_operation_with_request_id(
            sinomo_core::Id::generate().to_string(),
            cluster.replica_node.clone(),
            DfsNamespaceOperation::SetMode {
                path: "/".into(),
                mode: 0o701,
            },
        )
        .unwrap();
    drop(writer);
    let later = directory_row(&cluster, &cluster.replica_node, "root", "/");
    drop(cluster.users_primary);
    drop(cluster.services_primary);
    drop(cluster.replica);
    cluster
        ._runtime
        .stop_node(&cluster.users_primary_node)
        .unwrap();
    cluster._runtime.stop_node(&cluster.replica_node).unwrap();
    cluster
        ._runtime
        .reopen_node(cluster.replica_node.clone())
        .unwrap();
    cluster
        ._runtime
        .reopen_node(cluster.users_primary_node.clone())
        .unwrap();
    cluster.users_primary = cluster
        ._runtime
        .client(&cluster.users_primary_node)
        .unwrap();
    cluster.services_primary = cluster
        ._runtime
        .client(&cluster.services_primary_node)
        .unwrap();
    cluster.replica = cluster._runtime.client(&cluster.replica_node).unwrap();
    cluster
        .replica
        .route_namespace_operation_with_request_id(
            request,
            cluster.users_primary_node.clone(),
            operation,
        )
        .unwrap();
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/"),
        later
    );
    cluster.close();
}

fn hold_parent_prepare(
    cluster: &SifsTwoPrimaryCluster,
    participant: &crate::DfsParentCountParticipant,
    reservation: bool,
) -> DfsPreparedOperationRecord {
    let mut owner = cluster
        ._runtime
        .metadata_primary_service(&participant.expected.path)
        .unwrap();
    let operation_id = if reservation {
        participant.operation_id(&crate::DfsParentCountAction::Reserve)
    } else {
        format!(
            "ordinary-before-parent-reserve:{}",
            sinomo_core::Id::generate()
        )
    };
    let scope = owner
        .metadata_effect_scope_for_object(&participant.expected, operation_id)
        .unwrap();
    let effect = if reservation {
        DfsPreparedMetadataEffect::PutNamespaceJob {
            job: participant.job(crate::DfsParentCountState::Reserved),
        }
    } else {
        let mut changed = participant.expected.clone();
        changed.version = crate::namespace::next_version(&changed.version);
        changed.mode = 0o705;
        DfsPreparedMetadataEffect::PutObject { object: changed }
    };
    let payload = DfsPreparedOperationPayload::for_metadata_effects(
        scope.clone(),
        vec![DfsNamespaceObjectVersionExpectation {
            object_id: participant.expected.object_id.clone(),
            expected_version: Some(participant.expected.version.clone()),
        }],
        vec![DfsNamespacePathOccupantExpectation {
            path: participant.expected.path.clone(),
            expected_object_id: Some(participant.expected.object_id.clone()),
        }],
        vec![effect],
        participant.binding.created_at,
    )
    .unwrap();
    let execution = owner.acquire_prepared_execution(&scope).unwrap();
    let digest = owner
        .metadata_backend_mut()
        .unwrap()
        .prepare_operation(payload, None)
        .unwrap();
    let record = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.status, DfsPreparedOperationStatus::Prepared);
    drop(execution);
    drop(owner);
    record
}

#[test]
fn native_reserve_rejects_an_older_parent_prepare_without_resolving_it() {
    let mut cluster = foreign_parent_cluster("native-parent-existing-prepare");
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let hook = crate::posix::install_parent_count_test_hook(
        &cluster.users_primary_node,
        "/users",
        crate::posix::ParentCountTestPoint::Declared,
        move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            Ok(())
        },
    );
    let request = sinomo_core::Id::generate().to_string();
    let mut caller = cluster._runtime.client(&cluster.replica_node).unwrap();
    let source = cluster.users_primary_node.clone();
    let (participant, original) = std::thread::scope(|scope| {
        let id = request.clone();
        let call = scope.spawn(move || {
            caller.route_namespace_operation_with_request_id(
                id,
                source,
                DfsNamespaceOperation::Rename {
                    from: "/users".into(),
                    to: "/services/blocked-by-prepare".into(),
                },
            )
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let prepared = (|| -> std::result::Result<_, String> {
            entered.map_err(|error| error.to_string())?;
            let job = original_parent_job(&cluster, &request);
            let participant = job
                .directory_move_counts()
                .unwrap()
                .unwrap()
                .parent_participants
                .as_ref()
                .unwrap()
                .participants[0]
                .clone();
            let record = hold_parent_prepare(&cluster, &participant, false);
            Ok((participant, record))
        })();
        let _ = release_tx.send(());
        let result = call.join().unwrap();
        assert!(result.is_err());
        prepared.unwrap()
    });
    drop(hook);
    let job = original_parent_job(&cluster, &request);
    assert_eq!(job.status, DfsNamespaceJobStatus::Failed);
    assert_eq!(
        job.directory_move_counts()
            .unwrap()
            .unwrap()
            .parent_participants
            .as_ref()
            .unwrap()
            .rejection,
        Some(crate::DfsParentCountRejection::Contended)
    );
    let backend = cluster
        ._runtime
        .node_metadata(&cluster.replica_node)
        .unwrap();
    assert_eq!(
        backend
            .prepared_operation_record(&original.operation_id)
            .unwrap()
            .unwrap()
            .status,
        DfsPreparedOperationStatus::Prepared
    );
    assert_eq!(
        backend
            .get_object(&participant.expected.object_id)
            .unwrap()
            .unwrap(),
        participant.expected
    );
    drop(backend);
    let mut owner = cluster._runtime.metadata_primary_service("/").unwrap();
    owner
        .publish_metadata_effect_payload(original.payload.as_ref().clone())
        .unwrap();
    let after = owner
        .metadata_backend()
        .unwrap()
        .get_object(&participant.expected.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.mode, 0o705);
    assert_eq!(
        after.namespace_link_count,
        participant.expected.namespace_link_count
    );
    drop(owner);
    cluster.close();
}

#[test]
fn native_abort_resolves_its_exact_uncommitted_reserve_prepare() {
    let mut cluster = foreign_parent_cluster("native-parent-reserve-prepare-abort");
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let hook = crate::posix::install_parent_count_test_hook(
        &cluster.users_primary_node,
        "/users",
        crate::posix::ParentCountTestPoint::Declared,
        move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            Err(DfsPosixError::Dfs(DfsError::Network {
                message: "reservation delivery interrupted before namespace issuance".into(),
            }))
        },
    );
    let request = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::Rename {
        from: "/users".into(),
        to: "/services/prepared-reserve-aborted".into(),
    };
    let mut caller = cluster._runtime.client(&cluster.replica_node).unwrap();
    let source = cluster.users_primary_node.clone();
    let (participant, original) = std::thread::scope(|scope| {
        let id = request.clone();
        let op = operation.clone();
        let call =
            scope.spawn(move || caller.route_namespace_operation_with_request_id(id, source, op));
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let prepared = (|| -> std::result::Result<_, String> {
            entered.map_err(|error| error.to_string())?;
            let job = original_parent_job(&cluster, &request);
            let participant = job
                .directory_move_counts()
                .unwrap()
                .unwrap()
                .parent_participants
                .as_ref()
                .unwrap()
                .participants[0]
                .clone();
            let record = hold_parent_prepare(&cluster, &participant, true);
            Ok((participant, record))
        })();
        let _ = release_tx.send(());
        assert!(call.join().unwrap().is_err());
        prepared.unwrap()
    });
    drop(hook);
    let _ = cluster.replica.route_namespace_operation_with_request_id(
        request.clone(),
        cluster.users_primary_node.clone(),
        operation,
    );
    let job = original_parent_job(&cluster, &request);
    assert_eq!(
        job.status,
        DfsNamespaceJobStatus::Failed,
        "the original abort must settle all declared reservations"
    );
    let backend = cluster
        ._runtime
        .node_metadata(&cluster.replica_node)
        .unwrap();
    let resolved = backend
        .prepared_operation_record(&original.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.digest, original.digest);
    assert_eq!(resolved.payload, original.payload);
    assert_eq!(resolved.status, DfsPreparedOperationStatus::Aborted);
    assert!(
        backend
            .parent_count_reservations(
                &DfsMetadataShardId::new("root"),
                &sifs_file_system_id(),
                &participant.expected.object_id
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        backend
            .get_object(&participant.expected.object_id)
            .unwrap()
            .unwrap(),
        participant.expected
    );
    drop(backend);
    let mut writer = cluster
        ._runtime
        .client(&cluster.services_primary_node)
        .unwrap();
    writer
        .route_namespace_operation_with_request_id(
            sinomo_core::Id::generate().to_string(),
            cluster.replica_node.clone(),
            DfsNamespaceOperation::SetMode {
                path: "/".into(),
                mode: 0o704,
            },
        )
        .unwrap();
    let later = directory_row(&cluster, &cluster.replica_node, "root", "/");
    let mut owner = cluster._runtime.metadata_primary_service("/").unwrap();
    let prefix_before = owner
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&DfsMetadataShardId::new("root"))
        .unwrap();
    assert!(
        owner
            .publish_metadata_effect_payload(original.payload.as_ref().clone())
            .is_err()
    );
    assert_eq!(
        owner
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&DfsMetadataShardId::new("root"))
            .unwrap(),
        prefix_before,
        "late original publication after actual abort cannot append a commit marker"
    );
    drop(owner);
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/"),
        later
    );
    drop(writer);
    cluster.close();
}

fn native_parent_job_survives_handoff(coordinator_handoff: bool) {
    let mut cluster = foreign_parent_cluster(if coordinator_handoff {
        "native-parent-coordinator-handoff"
    } else {
        "native-parent-owner-handoff"
    });
    cluster
        .replica
        .posix_rename("/services", "/users/old-services")
        .unwrap();
    cluster
        .replica
        .posix_mkdir("/users/handoff-source")
        .unwrap();
    let before = directory_row(&cluster, &cluster.replica_node, "root", "/");
    let hook = crate::posix::install_parent_count_test_hook(
        &cluster.users_primary_node,
        "/users/handoff-source",
        crate::posix::ParentCountTestPoint::Settling,
        || {
            Err(DfsPosixError::Dfs(DfsError::Network {
                message: "original namespace effects settled before parent count publication"
                    .into(),
            }))
        },
    );
    let request = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::Rename {
        from: "/users/handoff-source".into(),
        to: "/services".into(),
    };
    assert!(
        cluster
            .replica
            .route_namespace_operation_with_request_id(
                request.clone(),
                cluster.users_primary_node.clone(),
                operation.clone()
            )
            .is_err()
    );
    drop(hook);
    let original = original_parent_job(&cluster, &request);
    let plan = original
        .directory_move_counts()
        .unwrap()
        .unwrap()
        .parent_participants
        .as_ref()
        .unwrap();
    assert_eq!(plan.phase, crate::DfsParentCountPhase::Settling);
    let participant = plan.participants[0].clone();
    let old_owner = if coordinator_handoff {
        cluster.users_primary_node.clone()
    } else {
        cluster.replica_node.clone()
    };
    let mut stale_sender = cluster
        ._runtime
        .client(if coordinator_handoff {
            &old_owner
        } else {
            &cluster.users_primary_node
        })
        .unwrap();
    // Hold the actual command and its reply fence across reassignment. A
    // normal requester would bind a fresh dispatch to the successor.
    let (stale_command, pending) = stale_sender
        .prepare_routed_namespace_request(
            &sinomo_core::Id::generate().to_string(),
            sifs_file_system_id(),
            &cluster.replica_node,
            DfsNamespaceOperation::ParentCountParticipant {
                coordinator_job: Box::new(original),
                participant: Box::new(participant.clone()),
                action: crate::DfsParentCountAction::Reserve,
            },
            false,
            false,
        )
        .unwrap();
    let moved_shard = if coordinator_handoff { "users" } else { "root" };
    let current = cluster
        ._runtime
        .reassign_metadata_shard(
            &sifs_file_system_id(),
            moved_shard,
            cluster.services_primary_node.clone(),
            vec![old_owner.clone()],
        )
        .unwrap();
    assert!(
        current.epoch
            > if coordinator_handoff {
                participant.binding.coordinator_scope.epoch
            } else {
                participant.parent_scope.epoch
            }
    );
    let refused = stale_sender.dispatch_prepared_namespace_request(
        stale_command,
        pending,
        Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
    );
    // The receiver answers the stale command itself, with a typed refusal:
    // a retired coordinator is not its source shard's primary, and a retired
    // parent owner refuses the stamp for its authority. That refusal names
    // the primary of the receiver's own record, which can still be the
    // receiver at an intermediate epoch of the handoff.
    let refusal = match &refused {
        Ok(reply) => &reply.result,
        Err(error) => panic!("the stale phase is answered, not lost: {error}"),
    };
    assert!(
        match refusal {
            DfsNamespaceResult::Rejected {
                reason,
                current_primary_node_id,
                ..
            } if coordinator_handoff => {
                current_primary_node_id.is_none()
                    && reason.contains("is not the primary of source shard users")
            }
            DfsNamespaceResult::Rejected {
                reason,
                current_primary_node_id,
                ..
            } => {
                current_primary_node_id.is_some()
                    && reason.contains("namespace authority shard stamp does not match")
            }
            DfsNamespaceResult::Applied => false,
        },
        "old coordinator or parent owner cannot authorize exact phase replay: {refused:?}"
    );
    drop(stale_sender);
    let coordinator = if coordinator_handoff {
        cluster.services_primary_node.clone()
    } else {
        cluster.users_primary_node.clone()
    };
    cluster
        .replica
        .route_namespace_operation_with_request_id(request, coordinator, operation)
        .unwrap();
    let parent_owner = if coordinator_handoff {
        cluster.replica_node.clone()
    } else {
        cluster.services_primary_node.clone()
    };
    let after = directory_row(&cluster, &parent_owner, "root", "/");
    assert_eq!(
        after.namespace_link_count,
        before.namespace_link_count.map(|count| count + 1)
    );
    assert_eq!(
        after, participant.successor,
        "certified handoff retains the originally sealed successor"
    );
    assert!(
        cluster
            ._runtime
            .node_metadata(&parent_owner)
            .unwrap()
            .parent_count_reservations(
                &DfsMetadataShardId::new("root"),
                &sifs_file_system_id(),
                &after.object_id
            )
            .unwrap()
            .is_empty()
    );
    cluster.close();
}

#[test]
fn native_parent_new_owner_resumes_exact_participant_after_handoff() {
    native_parent_job_survives_handoff(false);
}

#[test]
fn native_parent_new_coordinator_resumes_original_job_after_handoff() {
    native_parent_job_survives_handoff(true);
}

// A completed reassignment can precede delivery of its configuration projection
// to the original coordinator. Observe the held native Prepare, not merely an
// error reply from an earlier authority-stamp check. Every retry retains the
// original caller identity and requires its actual durable Running/Pending state.
fn await_held_parent_prepare(
    cluster: &mut SifsTwoPrimaryCluster,
    request: &str,
    operation: &DfsNamespaceOperation,
    parent_owner: &NodeId,
    native_operation: &str,
) -> crate::DfsPreparedOperationRecord {
    let requested_deadline = std::time::Instant::now() + std::time::Duration::from_secs(45);
    let previous_deadline = cluster
        .replica
        .transport
        .set_operation_deadline(Some(requested_deadline));
    let deadline = previous_deadline.map_or(requested_deadline, |existing| {
        existing.min(requested_deadline)
    });
    cluster
        .replica
        .transport
        .set_operation_deadline(Some(deadline));
    // Restore the caller's transport budget even if a diagnostic assertion fails.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "held Prepare deadline expired before dispatch"
            );
            let outcome = cluster.replica.route_namespace_operation_with_request_id(
                request.to_owned(),
                cluster.users_primary_node.clone(),
                operation.clone(),
            );
            if let Some(record) = cluster
                ._runtime
                .node_metadata(parent_owner)
                .unwrap()
                .prepared_operation_record(native_operation)
                .unwrap()
            {
                assert_eq!(
                    record.status,
                    DfsPreparedOperationStatus::Prepared,
                    "held phase: {outcome:?}"
                );
                assert!(
                    std::time::Instant::now() < deadline,
                    "held Prepare arrived after its deadline: {outcome:?}"
                );
                return record;
            }
            let job = original_parent_job(cluster, request);
            let caller = cluster
                ._runtime
                .node_metadata(&cluster.users_primary_node)
                .unwrap()
                .get_namespace_request(&cluster.replica_node, request)
                .unwrap()
                .unwrap();
            assert_eq!(
                job.status,
                crate::DfsNamespaceJobStatus::Running,
                "{outcome:?}"
            );
            assert_eq!(
                caller.status,
                DfsNamespaceRequestStatus::Pending,
                "{outcome:?}; {caller:?}"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "held native Prepare not reached: {outcome:?}; job={job:?}; request={caller:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }));
    cluster
        .replica
        .transport
        .set_operation_deadline(previous_deadline);
    match result {
        Ok(record) => record,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[test]
fn native_hidden_parent_prepare_handoff_and_restart_preserve_one_logical_successor() {
    let mut cluster = foreign_parent_cluster("native-parent-hidden-prepare-handoff");
    // Keep both configured bootstrap paths present during later lifecycle
    // reconciliation. Empty /services is first moved by the normal API;
    // the tested non-bootstrap source then replaces that absent path.
    cluster
        .replica
        .posix_rename("/services", "/users/old-services")
        .unwrap();
    cluster.replica.posix_mkdir("/users/hidden-source").unwrap();

    let old_scope = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|scope| scope.shard_id == "root")
        .unwrap();
    let old_root =
        crate::default_dfs_metadata_shards_root(cluster._runtime.data_dir(&cluster.replica_node))
            .join("root");
    let new_root = crate::default_dfs_metadata_shards_root(
        cluster._runtime.data_dir(&cluster.services_primary_node),
    )
    .join("root");
    let hold_old = crate::posix::hold_parent_apply_commit(&old_root, None);
    let hold_new = crate::posix::hold_parent_apply_commit(&new_root, None);
    let request = sinomo_core::Id::generate().to_string();
    let operation = DfsNamespaceOperation::Rename {
        from: "/users/hidden-source".into(),
        to: "/services".into(),
    };
    // Each call of this request below is one attempt whose outcome the test
    // reads: this one, and every one `await_held_parent_prepare` makes.
    cluster.replica.single_attempt_for_test(true);
    let initial_outcome = cluster.replica.route_namespace_operation_with_request_id(
        request.clone(),
        cluster.users_primary_node.clone(),
        operation.clone(),
    );
    assert!(initial_outcome.is_err());
    let original_request = cluster
        ._runtime
        .node_metadata(&cluster.users_primary_node)
        .unwrap()
        .get_namespace_request(&cluster.replica_node, &request)
        .unwrap()
        .unwrap();
    assert_eq!(
        original_request.status,
        DfsNamespaceRequestStatus::Pending,
        "a real Running parent coordinator cannot be sealed as an outer rejection after a transient phase error: {:?}",
        original_request.result
    );
    let original_job = original_parent_job(&cluster, &request);
    let plan = original_job
        .directory_move_counts()
        .unwrap()
        .unwrap()
        .parent_participants
        .as_ref()
        .unwrap();
    assert_eq!(
        plan.phase,
        crate::DfsParentCountPhase::Settling,
        "initial={initial_outcome:?}; frames={:?}",
        original_job.pending_frames
    );
    let participant = plan.participants[0].clone();
    let original = cluster
        ._runtime
        .node_metadata(&cluster.replica_node)
        .unwrap()
        .prepared_operation_record(&participant.operation_id(&crate::DfsParentCountAction::Apply))
        .unwrap()
        .unwrap();
    assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
    assert!(original.committed_entry.is_none());
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/"),
        participant.expected
    );
    {
        let mut owner = cluster._runtime.metadata_primary_service("/").unwrap();
        assert!(
            crate::service_sync::completed_metadata_handoff_evidence(
                owner.metadata_backend_mut().unwrap(),
                &cluster.replica_node,
                &old_scope,
                None,
                None,
                false
            )
            .unwrap()
            .is_none()
        );
        let mut wrong_proof = cluster
            .shared_kv
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .get_metadata_shard_membership_handoff_proof(&old_scope.file_system_id, "root")
            })
            .unwrap()
            .unwrap();
        wrong_proof.journal_sequence += 1;
        assert!(
            crate::service_sync::completed_metadata_handoff_evidence(
                owner.metadata_backend_mut().unwrap(),
                &cluster.replica_node,
                &old_scope,
                None,
                Some(&wrong_proof),
                false
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            owner
                .metadata_backend()
                .unwrap()
                .prepared_operation_record(&original.operation_id)
                .unwrap()
                .unwrap(),
            original
        );
    }
    let active = cluster
        ._runtime
        .reassign_metadata_shard(
            &sifs_file_system_id(),
            "root",
            cluster.services_primary_node.clone(),
            vec![cluster.replica_node.clone()],
        )
        .unwrap();
    // Reassignment may first expand membership and then transfer the primary.
    assert!(active.epoch > old_scope.epoch);
    let current = await_held_parent_prepare(
        &mut cluster,
        &request,
        &operation,
        &active.primary_node_id,
        &participant.operation_id_at_epoch(&crate::DfsParentCountAction::Apply, active.epoch),
    );
    let backend = cluster
        ._runtime
        .node_metadata(&cluster.services_primary_node)
        .unwrap();
    let retired = backend
        .prepared_operation_record(&original.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        retired.status,
        DfsPreparedOperationStatus::Prepared,
        "old attempt remains Prepared until its exact replacement commits"
    );
    assert_eq!(retired.digest, original.digest);
    assert_eq!(retired.payload, original.payload);
    assert_eq!(current.status, DfsPreparedOperationStatus::Prepared);
    assert!(
        current
            .metadata_effects
            .iter()
            .any(|effect| matches!(effect,
        DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status }
            if *digest == original.digest && *status == DfsPreparedOperationStatus::Superseded)),
        "replacement preserves the exact old attempt resolution edge"
    );

    assert_eq!(
        current.transaction.objects,
        vec![participant.successor.clone()]
    );
    assert_eq!(
        current.expected_object_versions,
        original.expected_object_versions
    );
    assert_eq!(
        current.expected_path_occupants,
        original.expected_path_occupants
    );
    assert_ne!(current.operation_id, original.operation_id);
    assert_eq!(
        backend
            .get_object(&participant.expected.object_id)
            .unwrap()
            .unwrap(),
        participant.expected
    );
    assert_eq!(
        backend
            .parent_count_reservations(
                &DfsMetadataShardId::new("root"),
                &sifs_file_system_id(),
                &participant.expected.object_id
            )
            .unwrap()
            .len(),
        1
    );
    drop(backend);
    let intermediate = current;
    let active_again = cluster
        ._runtime
        .reassign_metadata_shard(
            &sifs_file_system_id(),
            "root",
            cluster.replica_node.clone(),
            vec![cluster.services_primary_node.clone()],
        )
        .unwrap();
    assert!(active_again.epoch > active.epoch);
    let current = await_held_parent_prepare(
        &mut cluster,
        &request,
        &operation,
        &active_again.primary_node_id,
        &participant.operation_id_at_epoch(&crate::DfsParentCountAction::Apply, active_again.epoch),
    );
    cluster.replica.single_attempt_for_test(false);
    let backend = cluster
        ._runtime
        .node_metadata(&cluster.replica_node)
        .unwrap();
    assert_eq!(current.status, DfsPreparedOperationStatus::Prepared);
    let mut unchanged = current.payload.as_ref().clone();
    unchanged.operation_id = intermediate.operation_id.clone();
    unchanged.owner_epoch = intermediate.owner_epoch;
    unchanged.metadata_effects.retain(|effect| !matches!(effect,
        DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status: DfsPreparedOperationStatus::Superseded } if *digest == intermediate.digest));
    assert_eq!(
        unchanged, *intermediate.payload,
        "second handoff retains every inherited edge and semantic field"
    );
    assert_eq!(
        backend
            .prepared_operation_record(&original.operation_id)
            .unwrap()
            .unwrap()
            .status,
        DfsPreparedOperationStatus::Prepared
    );
    assert_eq!(
        backend
            .prepared_operation_record(&intermediate.operation_id)
            .unwrap()
            .unwrap()
            .status,
        DfsPreparedOperationStatus::Prepared
    );
    assert_eq!(
        backend
            .get_object(&participant.expected.object_id)
            .unwrap()
            .unwrap(),
        participant.expected
    );
    drop(backend);
    drop(cluster.replica);
    cluster._runtime.stop_node(&cluster.replica_node).unwrap();
    drop(hold_old);
    cluster
        ._runtime
        .reopen_node(cluster.replica_node.clone())
        .unwrap();
    cluster.replica = cluster._runtime.client(&cluster.replica_node).unwrap();
    cluster
        .replica
        .route_namespace_operation_with_request_id(
            request,
            cluster.users_primary_node.clone(),
            operation,
        )
        .unwrap();
    assert_eq!(
        directory_row(&cluster, &cluster.replica_node, "root", "/"),
        participant.successor
    );
    let final_backend = cluster
        ._runtime
        .node_metadata(&cluster.replica_node)
        .unwrap();
    let retired = final_backend
        .prepared_operation_record(&original.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(retired.status, DfsPreparedOperationStatus::Superseded);
    assert_eq!(retired.digest, original.digest);
    assert_eq!(retired.payload, original.payload);
    let retired_intermediate = final_backend
        .prepared_operation_record(&intermediate.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        retired_intermediate.status,
        DfsPreparedOperationStatus::Superseded
    );
    assert_eq!(retired_intermediate.payload, intermediate.payload);
    let committed = final_backend
        .prepared_operation_record(&current.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.status, DfsPreparedOperationStatus::Committed);
    assert!(committed.committed_entry.is_some());
    cluster.replica.posix_sync_directory("/").unwrap();
    drop(final_backend);
    drop(hold_new);
    cluster.close();
}

#[test]
fn native_parent_apply_records_directory_sync_generation() {
    let mut cluster = foreign_parent_cluster("native-parent-directory-sync-generation");
    cluster
        .replica
        .route_namespace_operation_with_request_id(
            sinomo_core::Id::generate().to_string(),
            cluster.users_primary_node.clone(),
            DfsNamespaceOperation::Rename {
                from: "/users".into(),
                to: "/services/synced-users".into(),
            },
        )
        .unwrap();
    cluster.replica.posix_sync_directory("/").unwrap();
    cluster.close();
}
