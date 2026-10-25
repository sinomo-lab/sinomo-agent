//! Routing regression tests.

use super::*;

#[test]
fn outsider_fixture_observes_completed_assignment_after_a_withheld_native_delta() {
    let mut cluster = sifs_posix_cluster("outsider-completed-configuration", "/users");
    let fs = sifs_file_system_id();
    let configs = cluster._runtime.metadata_shards().unwrap();
    let current = configs
        .iter()
        .find(|config| config.shard_id == "users")
        .unwrap();
    assert_eq!(current.primary_node_id, cluster.primary_node);
    let mut desired = current.clone();
    desired.replica_node_ids = vec![cluster.writer_node.clone()];
    desired.epoch = current.epoch.checked_add(1).unwrap();
    let fault = cluster
        ._runtime
        .lose_configuration_delta_and_pause_rebuild(
            &cluster.writer_node,
            crate::metadata_shard_key(&fs, "users"),
        )
        .unwrap();
    // Retain the already-open native primary while injecting this loss.
    // The general runtime.client() fixture opener waits for every node's
    // configuration readiness, including the rebuild deliberately paused here.
    let observation_deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    DfsControlPlaneStore::new(cluster.primary.control_plane.as_mut().unwrap().as_mut())
        .begin_metadata_shard_membership_handoff(current, &desired)
        .expect("submit the actual native outsider handoff")
        .expect("the source assignment is unchanged");
    cluster
        .primary
        .reconcile_pending_metadata_shard_membership_handoffs(&configs, Duration::from_secs(5))
        .expect("complete the actual native outsider handoff");
    let active = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|config| config.shard_id == "users")
        .unwrap();
    assert_eq!(
        active, desired,
        "the requested native handoff really activated"
    );
    // Owner activation does not imply delivery to this replica's observer.
    // Observe the actual negative hook under the existing fixture bound;
    // do not drive maintenance or release it before the delta was withheld.
    let dropped = loop {
        if let Some(delta) = fault.dropped_delta() {
            break delta;
        }
        assert!(
            Instant::now() < observation_deadline,
            "control did not observe the actual committed users assignment"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    fault.release();
    drop(fault);

    sifs_cluster_observe_completed_assignments(&mut cluster, &[active.clone()]);
    let capture = cluster
        ._runtime
        .capture_namespace_requests(&cluster.primary_node, 32)
        .unwrap();
    let result = cluster.writer.posix_write_file(
        "/users/observed-completed-assignment",
        b"first",
        DfsWriteOptions::replicated_eventual(vec![cluster.writer_node.clone()]),
    );
    let requests = capture.records();
    std::fs::write(
        cluster.root.join("boundary-diagnostic-outsider-configuration.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "actual_withheld_delta": dropped,
            "installed": active,
            "requests": requests,
            "result": format!("{result:?}"),
            "qualification": "One fresh write after real handoff and exact native authority observation. No supplied positive reply or historical write replay."
        })).unwrap(),
    ).unwrap();
    let stat = result.expect("first write uses the observed completed configuration");
    assert_eq!(stat.size_bytes, Some(5));
    let stamped = requests
        .iter()
        .filter_map(|message| {
            let DfsNetworkMessage::NamespaceRequest(request) = &message.message else {
                return None;
            };
            request
                .authority_shards
                .iter()
                .find(|shard| shard.shard_id == "users")
        })
        .collect::<Vec<_>>();
    assert!(!stamped.is_empty(), "actual native request was captured");
    assert!(stamped.iter().all(|shard| **shard == active));
    drop(capture);
    let SifsPosixCluster {
        primary,
        writer,
        observer,
        _runtime,
        ..
    } = cluster;
    drop((primary, writer, observer));
    _runtime.close().expect("normal native fixture drain");
}

#[test]
fn routed_mkdir_all_uses_applied_result_without_a_discarded_stat_projection() {
    use crate::test_runtime::{FaultAction, FaultRule, StatReplyFault};
    let mut cluster = sifs_outsider_cluster("mkdir-applied-without-stat");
    let path = "/users/mkdir-applied/child";
    assert!(
        cluster
            .observer
            .transport
            .delegates_local_namespace_authority()
    );
    let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    cluster._runtime.install_fault(FaultRule {
        source: Some(cluster.observer_node.clone()),
        target: Some(cluster.primary_node.clone()),
        route: None,
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: FaultAction::AlterStatReplyAfterDispatch {
            change: StatReplyFault::Unsupported,
            captured: captured.clone(),
        },
    });
    let result = cluster.observer.posix_mkdir_all(path);
    {
        let owner = cluster._runtime.metadata_primary_service(path).unwrap();
        let object = owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&sifs_file_system_id(), path)
            .unwrap()
            .expect("the real namespace owner committed the directory");
        assert_eq!(object.kind, DfsObjectKind::Directory);
    }
    assert!(
        result.is_ok(),
        "the applied unit mutation failed: {result:?}"
    );
    assert!(
        captured.lock().unwrap().is_empty(),
        "mkdir must not request discarded stat fields"
    );
    assert!(
        cluster.observer.posix_stat_distributed(path).is_err(),
        "a separately requested stat must still reject an unsupported reply"
    );
    assert_eq!(captured.lock().unwrap().len(), 1);
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster._runtime.clear_faults();
    assert_eq!(
        cluster.observer.posix_stat_distributed(path).unwrap().kind,
        DfsObjectKind::Directory
    );
    drop((cluster.primary, cluster.writer, cluster.observer));
    cluster._runtime.close().unwrap();
}

#[test]
fn browser_parent_exact_hint_does_not_require_cached_ancestors_or_recreate_deleted_parent() {
    let mut cluster = sifs_outsider_cluster("browser-parent-data-only-hint");
    let fs = sifs_file_system_id();
    let ancestor = "/users/hint-without-local-ancestors";
    let parent = "/users/hint-without-local-ancestors/parent";
    let child = "/users/hint-without-local-ancestors/parent/child";
    cluster.writer.posix_mkdir_all(parent).unwrap();
    let old = {
        let owner = cluster._runtime.metadata_primary_service(parent).unwrap();
        owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&fs, parent)
            .unwrap()
            .unwrap()
    };
    assert_eq!(old.kind, DfsObjectKind::Directory);
    cluster.writer.posix_rmdir(parent).unwrap();
    cluster
        .observer
        .add_transient_object_metadata(old.clone())
        .unwrap();
    assert_eq!(
        cluster.observer.metadata().object(&old.object_id),
        Some(&old)
    );
    assert!(
        cluster
            .observer
            .metadata()
            .object_by_normalized_posix_path(&fs, ancestor)
            .is_none()
    );
    assert_eq!(
        cluster
            .observer
            .posix_object_metadata_hint(parent)
            .unwrap()
            .kind,
        DfsObjectKind::Directory,
        "a missing ancestor projection alone does not invalidate the exact DATA hint"
    );
    let capture = cluster
        ._runtime
        .capture_served_work(&cluster.primary_node, 32)
        .unwrap();
    crate::browser_service::prepare_sifs_browser_write_parent(&mut cluster.observer, child)
        .unwrap();
    assert!(!capture.records().iter().any(|row|
        matches!(&row.command.routed_message.message,
            DfsNetworkMessage::NamespaceRequest(command)
                if command.requesting_node_id == cluster.observer_node &&
                    matches!(&command.operation, DfsNamespaceOperation::MkdirAll { path } if path == parent))),
        "an exact directory hint must not recreate the deleted parent when ancestors are uncached");
    let result = cluster.observer.posix_write_file(
        child,
        b"must not publish",
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![cluster.primary_node.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
    );
    assert!(
        result.is_err(),
        "the DATA hint grants no parent or write authority: {result:?}"
    );
    let owner = cluster._runtime.metadata_primary_service(parent).unwrap();
    let backend = owner.metadata_backend().unwrap();
    let tombstone = backend.get_object(&old.object_id).unwrap().unwrap();
    assert_eq!(tombstone.kind, DfsObjectKind::Tombstone);
    assert!(backend.get_object_by_path(&fs, child).unwrap().is_none());
    drop(owner);
    assert!(matches!(
        cluster.observer.posix_stat_distributed(parent),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(capture);
    drop((cluster.primary, cluster.writer, cluster.observer));
    cluster._runtime.close().unwrap();
}

#[test]
fn authoritative_stat_partial_catalog_keeps_its_existing_exact_physical_scope() {
    let cluster = sifs_two_primary_cluster("stat-exact-partial-catalog");
    let path = "/users/stat-existing-source";
    let alias = "/services/stat-other-owner-alias";
    let mut client = cluster._runtime.client(&cluster.replica_node).unwrap();
    let original = client
        .posix_write_file(
            path,
            b"exact source custody",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![cluster.users_primary_node.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    client.posix_hardlink(path, alias).unwrap();
    drop(client);
    let mut core = cluster._runtime.metadata_primary_service(path).unwrap();
    assert!(!core.transport.delegates_local_namespace_authority());
    let users = DfsMetadataShardId::new("users");
    let root = DfsMetadataShardId::new("root");
    assert!(
        core.metadata_backend()
            .unwrap()
            .has_physical_read_scope(&users)
    );
    assert!(
        !core
            .metadata_backend()
            .unwrap()
            .has_physical_read_scope(&root)
    );
    let catalog = cluster._runtime.metadata_shards().unwrap();
    assert_eq!(
        catalog
            .iter()
            .find(|s| s.shard_id == "root")
            .unwrap()
            .primary_node_id,
        cluster.users_primary_node
    );
    assert_ne!(cluster.services_primary_node, cluster.users_primary_node);
    let capture = cluster
        ._runtime
        .capture_served_work(&cluster.users_primary_node, 32)
        .unwrap();
    let stat = core.posix_stat_distributed(path).unwrap();
    assert_eq!(stat.object_id, original.object_id);
    assert_eq!(stat.content_hash, original.content_hash);
    assert_eq!(stat.link_count, 2);
    let mut exact_scope_reentries = 0;
    let mut foreign_scope_reads = 0;
    for row in capture.records() {
        let DfsNetworkMessage::NamespaceRequest(command) = row.command.routed_message.message
        else {
            continue;
        };
        if command.requesting_node_id != cluster.users_primary_node {
            continue;
        }
        let DfsNamespaceOperation::MetadataQuery { query } = command.operation else {
            continue;
        };
        assert!(!command.include_metadata_journal_batches);
        assert!(command.metadata_journal_after_sequences.is_empty());
        match query.selection {
            DfsMetadataSelection::StatAtPathV1 { request } if request.path == path => {
                assert_eq!(query.shard_id, users);
                exact_scope_reentries += 1;
            }
            DfsMetadataSelection::StatProjectionV1 { .. } if query.shard_id == root => {
                foreign_scope_reads += 1;
            }
            _ => {}
        }
    }
    assert_eq!(
        exact_scope_reentries, 0,
        "a partial catalog must not force an existing exact-shard source back through Main"
    );
    assert!(
        foreign_scope_reads > 0,
        "the same node's different physical root must still use registered Main dispatch"
    );
    assert!(core.pending_routed_namespace_ops.is_empty());
    assert!(core.transport.operation_deadline().is_none());
    drop(capture);
    drop(core);
    cluster.close();
}

#[test]
fn authoritative_stat_restarts_changed_cross_owner_view_within_existing_read_budget() {
    assert_stat_generation_race("canonical-generation", StatGenerationChange::Link, 1, true);
}

#[test]
fn authoritative_stat_restarts_only_after_typed_forward_prefix_refusal() {
    assert_stat_generation_race(
        "prefix-generation",
        StatGenerationChange::UnrelatedFile,
        1,
        true,
    );
}

#[test]
fn authoritative_stat_continuous_generation_changes_exhaust_original_read_rounds() {
    assert_stat_generation_race(
        "generation-round-limit",
        StatGenerationChange::Mode,
        crate::posix::POSIX_ROUTED_SYNC_RETRY_ROUNDS,
        false,
    );
}

#[derive(Clone, Copy)]
enum StatGenerationChange {
    Link,
    UnrelatedFile,
    Mode,
}

fn assert_stat_generation_race(
    name: &str,
    change: StatGenerationChange,
    changes: usize,
    ready: bool,
) {
    use crate::test_runtime::{FaultAction, FaultRule};
    use std::sync::Arc;

    let mut cluster = sifs_two_primary_cluster(name);
    let path = "/users/stat-generation-target";
    let original = cluster
        .replica
        .posix_write_file(
            path,
            b"generation-bound stat",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    assert_eq!(original.link_count, 1);
    let capture = cluster
        ._runtime
        .capture_served_work(&cluster.services_primary_node, 32)
        .unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let rounds = crate::posix::POSIX_ROUTED_SYNC_RETRY_ROUNDS;
    struct Release(Arc<tokio::sync::Semaphore>, usize);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.add_permits(self.1);
        }
    }
    cluster._runtime.install_fault(FaultRule {
        source: Some(cluster.replica_node.clone()),
        target: Some(cluster.services_primary_node.clone()),
        route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: rounds * 2,
        action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
    });
    let requester = cluster.replica_node.clone();
    let mut pages = 0;
    let mut publications = 0;
    let result = std::thread::scope(|scope| {
        // Release on unwind before joining the real blocked read. All fixture
        // changes use another authenticated client; no reply data is fabricated.
        let release = Release(Arc::clone(&gate), rounds * 2);
        let reader = &mut cluster.replica;
        let read = scope.spawn(move || reader.posix_stat_distributed(path));
        let mut writer = cluster
            ._runtime
            .client(&cluster.users_primary_node)
            .unwrap();
        let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
        let mut seen = BTreeSet::new();
        loop {
            for record in capture.records() {
                let DfsNetworkMessage::NamespaceRequest(command) =
                    record.command.routed_message.message
                else {
                    continue;
                };
                if command.requesting_node_id != requester
                    || !seen.insert(command.request_id.clone())
                {
                    continue;
                }
                assert!(record.succeeded);
                let DfsNamespaceOperation::MetadataQuery { query } = command.operation else {
                    panic!("the paused operation must be an actual stat page or fence")
                };
                let DfsMetadataSelection::StatProjectionV1 { request } = query.selection else {
                    panic!("only services-shard projection responses may be paused")
                };
                assert_eq!(request.object_id, original.object_id);
                assert_eq!(request.limit, crate::file_history_query::HISTORY_PAGE_LIMIT);
                if !request.fence_only {
                    assert!(request.expected_prefix.is_none() && request.after.is_none());
                    pages += 1;
                    assert!(
                        pages <= rounds,
                        "stat increased its existing read-round budget"
                    );
                    if publications < changes {
                        let changed_path =
                            format!("/services/stat-generation-change-{publications}");
                        match change {
                            StatGenerationChange::Link => {
                                writer.posix_hardlink(path, &changed_path).unwrap();
                            }
                            StatGenerationChange::Mode => {
                                // Real canonical publications, with the original
                                // 32-report observation cap across all four rounds.
                                writer
                                    .posix_set_mode(
                                        path,
                                        if publications % 2 == 0 { 0o600 } else { 0o640 },
                                    )
                                    .unwrap();
                            }
                            StatGenerationChange::UnrelatedFile => {
                                writer
                                    .posix_write_file(
                                        &changed_path,
                                        b"independent publication advances this physical prefix",
                                        DfsWriteOptions::replicated_eventual(vec![
                                            cluster.users_primary_node.clone(),
                                        ]),
                                    )
                                    .unwrap();
                            }
                        }
                        publications += 1;
                    }
                } else {
                    assert!(request.expected_prefix.is_some() && request.after.is_none());
                }
                gate.add_permits(1);
            }
            if read.is_finished() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "native stat collection did not finish"
            );
            std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
        }
        drop(writer);
        drop(release);
        read.join().unwrap()
    });
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster._runtime.clear_faults();
    assert_eq!(publications, changes);
    assert!(cluster.replica.transport.operation_deadline().is_none());
    assert!(cluster.replica.pending_routed_namespace_ops.is_empty());
    assert_eq!(cluster.replica.routed_namespace_settle_depth, 0);
    let expected_links = 1 + if matches!(change, StatGenerationChange::Link) {
        changes as u64
    } else {
        0
    };
    if ready {
        let stat = result.expect("fresh collection after the native generation changed");
        assert_eq!(stat.object_id, original.object_id);
        assert_eq!(stat.content_hash, original.content_hash);
        assert_eq!(
            stat.link_count, expected_links,
            "stale pages never supply the count"
        );
        assert!(pages >= 2, "the old collection must be discarded");
    } else {
        assert!(
            matches!(
                result,
                Err(DfsPosixError::Dfs(DfsError::MetadataRejection {
                    code: DfsMetadataRejectionCode::PrefixMismatch,
                    ..
                }))
            ),
            "continuous changes must fail closed, without a partial stat: {result:?}"
        );
        assert_eq!(
            pages, rounds,
            "the original four-round bound must be observable"
        );
    }
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(path)
            .unwrap()
            .link_count,
        expected_links
    );
    if matches!(change, StatGenerationChange::Mode) {
        cluster.replica.posix_set_mode(path, original.mode).unwrap();
    } else {
        for index in 0..changes {
            cluster
                .replica
                .posix_unlink(&format!("/services/stat-generation-change-{index}"))
                .unwrap();
        }
    }
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(path)
            .unwrap()
            .link_count,
        1
    );
    cluster.close();
}

#[test]
fn authoritative_stat_rejects_same_version_hash_change_on_actual_canonical_page() {
    use crate::test_runtime::{FaultAction, FaultRule, StatReplyFault};
    let mut cluster = sifs_two_primary_cluster("stat-same-version-hash");
    let path = "/users/stat-immutable-content";
    let original = cluster
        .replica
        .posix_write_file(
            path,
            b"immutable version bytes",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    cluster._runtime.install_fault(FaultRule {
        source: Some(cluster.replica_node.clone()),
        target: Some(cluster.users_primary_node.clone()),
        route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: FaultAction::AlterStatReplyAfterDispatch {
            change: StatReplyFault::SameVersionHashChanged,
            captured: captured.clone(),
        },
    });
    let result = cluster.replica.posix_stat_distributed(path);
    let Err(DfsPosixError::Dfs(error)) = result else {
        panic!("same-version hash corruption must not become a fresh-read retry: {result:?}")
    };
    assert_ne!(
        error.metadata_rejection_code(),
        DfsMetadataRejectionCode::PrefixMismatch
    );
    let records = captured.lock().unwrap();
    assert_eq!(
        records.len(),
        1,
        "must exercise an actual native canonical page"
    );
    let DfsNetworkMessage::NamespaceResponse(response) = &records[0].message else {
        panic!("wrong captured response")
    };
    let Some(DfsPosixCoordinationResult::MetadataQuery(response)) =
        &response.posix_coordination_result
    else {
        panic!("missing captured native query")
    };
    let DfsMetadataQueryResult::StatProjectionV1 { page } = &response.result else {
        panic!("fault must leave the initial path view intact")
    };
    let object = page.object.as_ref().unwrap();
    assert_eq!(object.object_id, original.object_id);
    assert_eq!(object.version, original.version);
    assert_eq!(object.content_hash, original.content_hash);
    drop(records);
    cluster._runtime.assert_fault_budgets().unwrap();
    cluster._runtime.clear_faults();
    assert!(cluster.replica.pending_routed_namespace_ops.is_empty());
    assert!(cluster.replica.transport.operation_deadline().is_none());
    let healthy = cluster.replica.posix_stat_distributed(path).unwrap();
    assert_eq!(healthy.version, original.version);
    assert_eq!(healthy.content_hash, original.content_hash);
    cluster.close();
}

#[test]
fn authoritative_lstat_preserves_alias_absence_and_original_canonical_read_predicates() {
    let mut cluster = sifs_outsider_cluster("stat-original-read-predicates");
    let canonical = "/users/stat-predicate-target";
    let alias = "/services/stat-predicate-alias";
    let absent = "/services/stat-predicate-absent";
    cluster
        .writer
        .posix_write_file(
            canonical,
            b"first",
            DfsWriteOptions::replicated_eventual(vec![cluster.writer_node.clone()]),
        )
        .unwrap();
    cluster.writer.posix_hardlink(canonical, alias).unwrap();
    let alias_row = {
        let owner = cluster._runtime.metadata_primary_service(alias).unwrap();
        owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&sifs_file_system_id(), alias)
            .unwrap()
            .unwrap()
    };
    let planning = cluster.observer.namespace_execution.begin_planning();
    let first = cluster.observer.posix_lstat_distributed(alias).unwrap();
    assert!(matches!(
        cluster.observer.posix_lstat_distributed(absent),
        Err(DfsPosixError::NotFound { .. })
    ));
    let fs = sifs_file_system_id();
    let reads = cluster
        .observer
        .namespace_execution
        .captured_reads()
        .unwrap();
    assert_eq!(
        reads
            .paths
            .get(&(fs.clone(), crate::namespace::metadata_path(alias)))
            .cloned(),
        Some(Some(alias_row.object_id.clone())),
        "record the alias dentry, not its canonical inode, at the requested path"
    );
    assert_eq!(
        reads
            .objects
            .get(&(fs.clone(), alias_row.object_id.clone()))
            .cloned(),
        Some(Some(alias_row.version))
    );
    assert_eq!(
        reads
            .paths
            .get(&(fs.clone(), crate::namespace::metadata_path(absent)))
            .cloned(),
        Some(None),
        "authoritative absence is an observation, not a missing predicate"
    );
    assert_eq!(
        reads
            .objects
            .get(&(fs.clone(), first.object_id.clone()))
            .cloned(),
        Some(Some(first.version.clone()))
    );
    cluster
        .writer
        .posix_write_file(
            canonical,
            b"later",
            DfsWriteOptions::replicated_eventual(vec![cluster.writer_node.clone()]),
        )
        .unwrap();
    let later = cluster.observer.posix_lstat_distributed(alias).unwrap();
    assert_ne!(first.version, later.version);
    let reads = cluster
        .observer
        .namespace_execution
        .captured_reads()
        .unwrap();
    let mut expectations = vec![DfsNamespaceObjectVersionExpectation {
        object_id: first.object_id.clone(),
        expected_version: Some(later.version),
    }];
    reads
        .bind_recorded_expectations(&fs, &mut expectations, &mut [])
        .unwrap();
    assert_eq!(
        expectations[0].expected_version,
        Some(first.version),
        "a later stat must not rebase the original comparison"
    );
    drop(planning);
}

#[test]
fn authoritative_stat_rejects_actual_corrupted_replies_and_restores_deadline() {
    use crate::test_runtime::{FaultAction, FaultRule, StatReplyFault};
    let mut cluster = sifs_outsider_cluster("stat-negative-replies");
    let path = "/users/stat-negative";
    cluster
        .writer
        .posix_write_file(
            path,
            b"negative stat",
            DfsWriteOptions::replicated_eventual(vec![cluster.writer_node.clone()]),
        )
        .unwrap();
    cluster
        .writer
        .posix_hardlink(path, "/services/stat-negative-alias")
        .unwrap();
    for change in [
        StatReplyFault::Unsupported,
        StatReplyFault::Incomplete,
        StatReplyFault::WrongRequest,
        StatReplyFault::WrongEpoch,
        StatReplyFault::WrongSource,
        StatReplyFault::WrongTarget,
        StatReplyFault::MalformedPrefix,
        StatReplyFault::StalePrefix,
    ] {
        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        cluster._runtime.install_fault(FaultRule {
            source: Some(cluster.observer_node.clone()),
            target: Some(cluster.primary_node.clone()),
            route: None,
            call_id: None,
            origin: None,
            remaining_hits: 1,
            action: FaultAction::AlterStatReplyAfterDispatch {
                change: change.clone(),
                captured: captured.clone(),
            },
        });
        let before = cluster
            .observer
            .metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        cluster
            .observer
            .set_transport_operation_deadline(Some(deadline));
        assert!(
            cluster.observer.posix_stat_distributed(path).is_err(),
            "accepted {change:?} reply"
        );
        assert_eq!(
            captured.lock().unwrap().len(),
            1,
            "must exercise a real native reply"
        );
        cluster._runtime.assert_fault_budgets().unwrap();
        assert_eq!(
            cluster.observer.transport.operation_deadline(),
            Some(deadline)
        );
        assert!(cluster.observer.pending_routed_namespace_ops.is_empty());
        assert_eq!(cluster.observer.routed_namespace_settle_depth, 0);
        assert_eq!(
            before,
            cluster
                .observer
                .metadata_backend()
                .unwrap()
                .metadata_shard_statuses()
                .unwrap()
        );
        cluster._runtime.clear_faults();
        cluster.observer.set_transport_operation_deadline(None);
        assert_eq!(
            cluster
                .observer
                .posix_stat_distributed(path)
                .unwrap()
                .link_count,
            2
        );
    }
    let flag = DfsCancellationFlag::default();
    cluster.observer.set_cancellation(Some(flag.clone()));
    let deadline = Instant::now() + Duration::from_secs(1);
    cluster
        .observer
        .set_transport_operation_deadline(Some(deadline));
    flag.cancel();
    assert!(cluster.observer.posix_stat_distributed(path).is_err());
    assert_eq!(
        cluster.observer.transport.operation_deadline(),
        Some(deadline)
    );
    assert!(cluster.observer.pending_routed_namespace_ops.is_empty());
    cluster.observer.set_cancellation(None);
    cluster.observer.set_transport_operation_deadline(None);
    assert_eq!(
        cluster
            .observer
            .posix_stat_distributed(path)
            .unwrap()
            .link_count,
        2
    );
}

#[test]
fn authoritative_stat_alias_projection_keeps_guards_in_the_owning_physical_scope() {
    let mut cluster = sifs_two_primary_cluster("stat-physical-guard-scope");
    let path = "/users/stat-guard-source";
    let alias = "/services/stat-guard-alias";
    cluster
        .replica
        .posix_write_file(
            path,
            b"guard scope",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster.replica.posix_hardlink(path, alias).unwrap();
    let object = cluster.replica.file_history_capture_object(path).unwrap();
    let shards = cluster._runtime.metadata_shards().unwrap();
    let users = shards.iter().find(|s| s.shard_id == "users").unwrap();
    let services = shards.iter().find(|s| s.shard_id == "services").unwrap();
    let mut user_owner = cluster._runtime.metadata_primary_service(path).unwrap();
    let mut alias_owner = cluster._runtime.metadata_primary_service(alias).unwrap();
    let job = user_owner
        .metadata_backend()
        .unwrap()
        .list_namespace_jobs_page(None, crate::file_history_query::HISTORY_PAGE_LIMIT)
        .unwrap()
        .jobs
        .into_iter()
        .find(|j| j.root_path == path && j.target_path.as_deref() == Some(alias))
        .unwrap();
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
    // Reproduce only the restrictive foreign-replica guard observed in the
    // failing native run. This lends no authority or DATA: both query services
    // retain their real registered provider and exact physical backend.
    alias_owner.cross_shard_namespace_locks.upsert_job(
        Some(DfsMetadataShardId::new("users")),
        job.job_id.clone(),
        job.file_system_id.clone(),
        vec![job.root_path.clone()],
    );
    let request = DfsStatProjectionRequest {
        object_id: object.object_id.clone(),
        object_path: path.into(),
        object_kind: object.kind.clone(),
        object_version: object.version.clone(),
        limit: crate::file_history_query::HISTORY_PAGE_LIMIT,
        after: None,
        expected_prefix: None,
        fence_only: false,
    };
    let query = |owner: &mut NetworkedDfsService<DfsNodeTransport>,
                 shard: &DfsMetadataShardConfig| {
        owner.query_metadata_authority_impl(
            shard,
            DfsMetadataSelection::StatProjectionV1 {
                request: Box::new(request.clone()),
            },
        )
    };
    let DfsMetadataQueryResult::StatProjectionV1 { page } =
        query(&mut alias_owner, services).unwrap()
    else {
        panic!("current native alias projection");
    };
    assert!(
        page.object.is_none(),
        "services cannot supply the users object body"
    );
    assert_eq!(
        page.entries
            .iter()
            .map(|o| o.path.as_str())
            .collect::<Vec<_>>(),
        vec![alias]
    );
    assert!(page.after.is_none());
    assert!(page.complete);
    assert!(
        alias_owner
            .cross_shard_namespace_locks
            .blocks(&job.file_system_id, path),
        "a foreign guard must be preserved, not deleted to pass the read"
    );
    user_owner.cross_shard_namespace_locks.upsert_job(
        Some(DfsMetadataShardId::new("users")),
        job.job_id.clone(),
        job.file_system_id.clone(),
        vec![job.root_path.clone()],
    );
    assert!(
        matches!(query(&mut user_owner, users), Err(DfsError::Busy { path: blocked }) if blocked == path),
        "the canonical authority still enforces its own namespace reservation"
    );
    alias_owner.cross_shard_namespace_locks.upsert_job(
        Some(DfsMetadataShardId::new("services")),
        format!("{}:local-negative", job.job_id),
        job.file_system_id.clone(),
        vec![alias.to_owned()],
    );
    assert!(
        matches!(query(&mut alias_owner, services), Err(DfsError::Busy { path: blocked }) if blocked == alias),
        "every returned alias remains protected by its own physical authority's guard"
    );
    drop((user_owner, alias_owner));
    cluster.close();
}

#[test]
fn authoritative_stat_cross_shard_pages_are_complete_and_rollover_rejects_old_scope() {
    let mut cluster = sifs_two_primary_cluster("stat-pages-rollover");
    let diagnostics = [
        &cluster.users_primary_node,
        &cluster.services_primary_node,
        &cluster.replica_node,
    ]
    .into_iter()
    .map(|node| crate::native_boundary_diagnostics::capture(cluster._runtime.data_dir(node).root()))
    .collect::<Vec<_>>();
    cluster
        ._runtime
        .checkpoint_rpc_observation_window()
        .unwrap();
    let path = "/users/stat-many-aliases";
    cluster
        .replica
        .posix_write_file(
            path,
            b"paged stat",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    for index in 0..130 {
        let result = cluster
            .replica
            .posix_hardlink(path, &format!("/services/stat-page-{index:03}"));
        if result.is_err() {
            for diagnostic in &diagnostics {
                diagnostic.dump(
                    "stat-alias-failure",
                    serde_json::json!({
                        "index": index, "result": format!("{result:?}"),
                        "rpc": format!("{:?}", cluster._runtime.rpc_observations()),
                    }),
                );
            }
        }
        result.unwrap();
        if (index + 1) % 4 == 0 {
            cluster
                ._runtime
                .checkpoint_rpc_observation_window()
                .unwrap();
            for diagnostic in &diagnostics {
                diagnostic.checkpoint(&format!("stat-alias-window-{}", index + 1));
            }
        }
    }
    cluster
        ._runtime
        .checkpoint_rpc_observation_window()
        .unwrap();
    cluster
        .replica
        .posix_hardlink(path, "/users/stat-local-alias")
        .unwrap();
    let object = cluster.replica.file_history_capture_object(path).unwrap();
    let shards = cluster._runtime.metadata_shards().unwrap();
    let old = shards
        .iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap()
        .clone();
    let mut request = DfsStatProjectionRequest {
        object_id: object.object_id.clone(),
        object_path: path.into(),
        object_kind: object.kind.clone(),
        object_version: object.version.clone(),
        limit: crate::file_history_query::HISTORY_PAGE_LIMIT,
        after: None,
        expected_prefix: None,
        fence_only: false,
    };
    let query = |caller: &mut NetworkedDfsService<DfsNodeTransport>,
                 request: &DfsStatProjectionRequest| {
        caller.query_metadata_authority_impl(
            &old,
            DfsMetadataSelection::StatProjectionV1 {
                request: Box::new(request.clone()),
            },
        )
    };
    let DfsMetadataQueryResult::StatProjectionV1 { page: first } =
        query(&mut cluster.replica, &request).unwrap()
    else {
        panic!("wrong projection")
    };
    assert_eq!(first.entries.len(), 128);
    assert!(!first.complete && first.after.is_some());
    request.after = first.after.clone();
    request.expected_prefix = Some(first.prefix.clone());
    let DfsMetadataQueryResult::StatProjectionV1 { page: second } =
        query(&mut cluster.replica, &request).unwrap()
    else {
        panic!("wrong continuation")
    };
    assert_eq!(second.entries.len(), 2);
    assert!(second.complete && second.after.is_none());
    assert_eq!(first.prefix, second.prefix);
    let ids = first
        .entries
        .iter()
        .chain(&second.entries)
        .map(|row| row.object_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 130);
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(path)
            .unwrap()
            .link_count,
        132
    );
    assert_eq!(
        cluster
            .replica
            .posix_lstat_distributed("/services/stat-page-129")
            .unwrap()
            .link_count,
        132
    );
    cluster
        .replica
        .posix_unlink("/services/stat-page-129")
        .unwrap();
    assert!(
        query(&mut cluster.replica, &request).is_err(),
        "old page prefix cannot span unlink"
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(path)
            .unwrap()
            .link_count,
        131
    );
    let new = cluster
        ._runtime
        .reassign_metadata_shard(
            &old.file_system_id,
            &old.shard_id,
            cluster.users_primary_node.clone(),
            vec![
                cluster.services_primary_node.clone(),
                cluster.replica_node.clone(),
            ],
        )
        .unwrap();
    assert!(new.epoch > old.epoch);
    request.after = None;
    request.expected_prefix = None;
    assert!(
        query(&mut cluster.replica, &request).is_err(),
        "old authority epoch cannot supply stat evidence"
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(path)
            .unwrap()
            .link_count,
        131
    );
    cluster.close();
}

#[test]
fn authoritative_stat_outsider_counts_link_and_unlink_without_local_replay() {
    let mut cluster = sifs_outsider_cluster("stat-outsider-ryw");
    let path = "/users/stat-outsider-target";
    let alias = "/users/stat-outsider-alias";
    cluster
        .writer
        .posix_write_file(
            path,
            b"outsider stat",
            DfsWriteOptions::replicated_eventual(vec![cluster.writer_node.clone()]),
        )
        .unwrap();
    cluster.writer.posix_hardlink(path, alias).unwrap();
    for expected in [2, 1] {
        let before = cluster
            .observer
            .metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap();
        assert!(
            cluster
                ._runtime
                .metadata_shards()
                .unwrap()
                .iter()
                .all(|shard| {
                    shard.primary_node_id != cluster.observer_node
                        && !shard.replica_node_ids.contains(&cluster.observer_node)
                })
        );
        let stat = cluster.observer.posix_stat_distributed(path).unwrap();
        assert_eq!(stat.link_count, expected);
        assert_eq!(stat.size_bytes, Some(13));
        if expected == 2 {
            let alias_stat = cluster.observer.posix_lstat_distributed(alias).unwrap();
            assert_eq!(alias_stat.object_id, stat.object_id);
            assert_eq!(alias_stat.link_count, 2);
        }
        assert_eq!(
            before,
            cluster
                .observer
                .metadata_backend()
                .unwrap()
                .metadata_shard_statuses()
                .unwrap(),
            "stat cannot ingest caller journals or manufacture a local replica"
        );
        if expected == 2 {
            cluster.writer.posix_unlink(alias).unwrap();
        }
    }
    assert_sifs_cluster_no_posix_leaks(&cluster, "authoritative stat outsider");
}

#[test]
fn browser_exact_parent_hint_without_replica_cannot_recreate_a_deleted_parent() {
    let mut cluster = sifs_outsider_cluster("browser-exact-parent-outsider");
    let parent = "/users/browser-exact-parent-outsider/parent";
    let child = format!("{parent}/child");
    cluster.writer.posix_mkdir_all(parent).unwrap();
    let old_parent = {
        let owner = cluster._runtime.metadata_primary_service(parent).unwrap();
        owner.metadata_backend().unwrap()
            .get_object_by_path(&sifs_file_system_id(), parent).unwrap().unwrap()
    };
    assert_eq!(old_parent.kind, DfsObjectKind::Directory);
    cluster.writer.posix_rmdir(parent).unwrap();
    let tombstone = {
        let owner = cluster._runtime.metadata_primary_service(parent).unwrap();
        let row = owner.metadata_backend().unwrap().get_object(&old_parent.object_id).unwrap().unwrap();
        assert_eq!(row.kind, DfsObjectKind::Tombstone);
        row
    };
    assert!(cluster._runtime.metadata_shards().unwrap().iter().all(|shard| {
        shard.primary_node_id != cluster.observer_node
            && !shard.replica_node_ids.contains(&cluster.observer_node)
    }));
    cluster.observer.add_transient_object_metadata(old_parent.clone()).unwrap();
    assert_eq!(cluster.observer.metadata().object(&old_parent.object_id), Some(&old_parent));
    let before = cluster.observer.metadata_backend().unwrap().metadata_shard_statuses().unwrap();
    let capture = cluster._runtime.capture_served_work(&cluster.primary_node, 32).unwrap();
    crate::browser_service::prepare_sifs_browser_write_parent(&mut cluster.observer, &child).unwrap();
    let reports = capture.records();
    let implicit_creates = reports.iter().filter(|work| {
        work.command.routed_message.source_node_id == cluster.observer_node
            && matches!(&work.command.routed_message.message,
                DfsNetworkMessage::NamespaceRequest(request)
                    if matches!(&request.operation, DfsNamespaceOperation::MkdirAll { path } if path == parent))
    }).count();
    let after = {
        let owner = cluster._runtime.metadata_primary_service(parent).unwrap();
        owner.metadata_backend().unwrap().get_object(&old_parent.object_id).unwrap().unwrap()
    };
    std::fs::write(
        cluster._runtime.data_dir(&cluster.observer_node).root().join("boundary-diagnostic-outsider-parent-hint.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "caller": cluster.observer_node, "primary": cluster.primary_node,
            "old_hint": old_parent, "tombstone": tombstone, "after_preparation": after,
            "implicit_mkdir_all_calls": implicit_creates,
            "actual_invocations": reports.iter().map(|work| serde_json::json!({
                "command": work.command.routed_message,
                "publish_metadata": work.committed_work.publish_metadata,
                "semantic_changes": work.committed_work.semantic_changes,
                "succeeded": work.succeeded,
            })).collect::<Vec<_>>(),
            "qualification": "Normal authenticated member removed from metadata assignment through native handoff, with a genuinely observed old parent row as advisory DATA. Original Main metadata never removed or replaced."
        })).unwrap(),
    ).unwrap();
    assert_eq!(implicit_creates, 0, "an exact parent hint must not cause an implicit Main recreation");
    assert_eq!(after, tombstone);
    assert_eq!(before, cluster.observer.metadata_backend().unwrap().metadata_shard_statuses().unwrap());
    let create = cluster.observer.posix_write_file(
        &child, b"must remain absent",
        DfsWriteOptions::replicated_eventual(vec![cluster.observer_node.clone()]),
    );
    match create {
        Err(DfsPosixError::MissingParent { path }) | Err(DfsPosixError::NotFound { path }) => {
            assert!(path == parent || path == child);
        }
        other => panic!("the actual Main must reject the missing parent: {other:?}"),
    }
    let owner = cluster._runtime.metadata_primary_service(parent).unwrap();
    assert_eq!(owner.metadata_backend().unwrap().get_object(&old_parent.object_id).unwrap(), Some(tombstone));
    assert!(owner.metadata_backend().unwrap().get_object_by_path(&sifs_file_system_id(), &child).unwrap().is_none());
    drop(owner);
    drop(capture);
    let SifsPosixCluster { primary, writer, observer, _runtime, .. } = cluster;
    drop((primary, writer, observer));
    _runtime.close().unwrap();
}

#[test]
fn authoritative_stat_counts_cross_shard_alias_and_fails_closed_on_outage() {
    let mut cluster = sifs_two_primary_cluster("stat-cross-shard-outage");
    let path = "/users/stat-cross-target";
    let alias = "/services/stat-cross-alias";
    cluster
        .replica
        .posix_write_file(
            path,
            b"cross stat",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    cluster.replica.posix_hardlink(path, alias).unwrap();
    let before = cluster
        .replica
        .metadata_backend()
        .unwrap()
        .metadata_shard_statuses()
        .unwrap();
    let canonical = cluster.replica.posix_stat_distributed(path).unwrap();
    assert_eq!(canonical.link_count, 2);
    let link = cluster.replica.posix_lstat_distributed(alias).unwrap();
    assert_eq!(link.object_id, canonical.object_id);
    assert_eq!(link.link_count, 2);
    assert_eq!(
        before,
        cluster
            .replica
            .metadata_backend()
            .unwrap()
            .metadata_shard_statuses()
            .unwrap()
    );
    cluster.replica.posix_unlink(alias).unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat_distributed(path)
            .unwrap()
            .link_count,
        1
    );
    cluster
        ._runtime
        .stop_node(&cluster.services_primary_node)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    cluster
        .replica
        .set_transport_operation_deadline(Some(deadline));
    assert!(
        cluster.replica.posix_stat_distributed(path).is_err(),
        "missing alias authority must not be treated as an empty shard or stale local count"
    );
    assert_eq!(
        cluster.replica.transport.operation_deadline(),
        Some(deadline)
    );
    assert!(cluster.replica.pending_routed_namespace_ops.is_empty());
    cluster.close();
}

fn native_pending_routed_write(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    NetworkedDfsService<DfsNodeTransport>,
    DfsWriteRequest,
    DfsVersion,
    Vec<u8>,
    Vec<NodeId>,
    String,
    DfsDistributedWriteResponse,
) {
    let (runtime, primary, _, requester, base, _) =
        native_read_fixture(name, 1, false, b"actual routed predecessor".to_vec());
    let nodes = vec![primary.clone(), requester.clone()];
    let path = base.path.clone();
    let mut placement =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    placement.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(&primary, &path, DfsPlacementScope::Subtree, placement)
        .unwrap();
    deny_native_read_repair(&runtime, &primary, &requester);
    let mut client = runtime.client(&requester).unwrap();
    let request = DfsWriteRequest {
        file_system_id: base.file_system_id,
        object_id: base.object_id,
        target_version: crate::namespace::next_version(&base.version),
        requesting_node_id: requester.clone(),
    };
    let bytes = vec![0x74; 2 * 1024 * 1024 + 73];
    let operation = sinomo_core::Id::generate().to_string();
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let outcome = client
        .write_distributed_bytes_with_base(
            request.clone(),
            Some(base.version.clone()),
            bytes.clone(),
            nodes.clone(),
            policy,
            DfsDistributedWriteOptions {
                operation_id: Some(operation.clone()),
                ..DfsDistributedWriteOptions::default()
            },
        )
        .unwrap();
    assert!(matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }));
    assert!(client.writes.pending_routed_writes.contains_key(&operation));
    let queued = client.transport.drain_incoming_messages().unwrap();
    let response = queued
        .into_iter()
        .find_map(|routed| {
            if routed.source_node_id != primary || routed.target_node_id != requester {
                return None;
            }
            let DfsNetworkMessage::DistributedWriteResponse(response) = routed.message else {
                return None;
            };
            (response.request_id == operation && response.object_id == request.object_id)
                .then_some(response)
        })
        .expect("normal routed request produced its own native response");
    assert!(matches!(
        &response.result,
        DfsDistributedWriteResult::Outcome {
            outcome: NetworkedDfsWriteOutcome::Pending { .. }
        }
    ));
    (
        runtime,
        client,
        request,
        base.version,
        bytes,
        nodes,
        operation,
        response,
    )
}

fn complete_native_routed_write(
    runtime: &crate::test_runtime::DfsTestRuntime,
    client: &mut NetworkedDfsService<DfsNodeTransport>,
    request: &DfsWriteRequest,
    base: &DfsVersion,
    bytes: &[u8],
    nodes: &[NodeId],
    operation: &str,
) -> (NodeId, DfsDistributedWriteResponse) {
    let primary = client.writes.pending_routed_writes[operation]
        .primary_node_id
        .clone();
    let config = client.writes.pending_routed_writes[operation]
        .authority_shard
        .clone();
    runtime.clear_faults();
    let mut owner = runtime.client(&primary).unwrap();
    owner.hydrate_from_metadata_backend().unwrap();
    owner.retry_pending_repairs().unwrap();
    owner.process_incoming().unwrap();
    owner.flush_detached_transport_sends().unwrap();
    let publication = runtime
        .maintenance_tick(&primary, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
    assert_eq!(publication.omitted_kv_event_count, 0);
    owner.process_incoming().unwrap();
    let actual = owner
        .metadata_backend()
        .unwrap()
        .get_object(&request.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(actual.file_system_id, config.file_system_id);
    assert_eq!(
        actual.content_hash.as_deref(),
        Some(content_hash(bytes).as_str())
    );
    drop(owner);
    let mut policy = DfsReplicationPolicy::new(2, 2).unwrap();
    policy.required_node_ids = nodes.to_vec();
    // Replay the SAME real operation through ordinary dispatch. Only its
    // actual committed receipt may generate the terminal response.
    client
        .write_distributed_bytes_with_base(
            request.clone(),
            Some(base.clone()),
            bytes.to_vec(),
            nodes.to_vec(),
            policy,
            DfsDistributedWriteOptions {
                operation_id: Some(operation.into()),
                ..DfsDistributedWriteOptions::default()
            },
        )
        .unwrap();
    let response = client
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .find_map(|routed| {
            if routed.source_node_id != primary || routed.target_node_id != *client.node_id() {
                return None;
            }
            let DfsNetworkMessage::DistributedWriteResponse(response) = routed.message else {
                return None;
            };
            (response.request_id == operation
                && response.object_id == request.object_id
                && matches!(
                    &response.result,
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. }
                    }
                ))
            .then_some(response)
        })
        .expect("actual native stable replay returns its exact committed result");
    (primary, response)
}

#[test]
fn routed_write_response_tracks_remote_intent_alias() {
    let (runtime, mut client, request, base, bytes, nodes, operation, pending_response) =
        native_pending_routed_write("native-routed-alias");
    let primary = client.writes.pending_routed_writes[&operation]
        .primary_node_id
        .clone();
    let DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Pending { intent_id, .. },
    } = &pending_response.result
    else {
        unreachable!()
    };
    let alias = intent_id.clone();
    client
        .handle_distributed_write_response(primary.clone(), pending_response.clone())
        .unwrap();
    assert!(
        matches!(client.write_outcome(&operation).unwrap(), NetworkedDfsWriteOutcome::Pending { intent_id, .. } if intent_id == alias)
    );
    assert!(
        client.writes.pending_routed_writes[&operation]
            .command
            .is_some()
    );
    assert!(
        client.writes.pending_routed_writes[&alias]
            .command
            .is_none()
    );
    let mut rejected = pending_response;
    rejected.result = DfsDistributedWriteResult::Rejected {
        reason: "exclusive retry observed the pending create".into(),
        current_primary_node_id: None,
    };
    assert!(matches!(
        client
            .handle_distributed_write_response(primary.clone(), rejected)
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    assert!(matches!(
        client.write_outcome(&alias).unwrap(),
        NetworkedDfsWriteOutcome::Pending { .. }
    ));
    let (actual_primary, terminal) = complete_native_routed_write(
        &runtime,
        &mut client,
        &request,
        &base,
        &bytes,
        &nodes,
        &operation,
    );
    assert_eq!(actual_primary, primary);
    let DfsDistributedWriteResult::Outcome {
        outcome:
            NetworkedDfsWriteOutcome::Committed {
                intent_id,
                version,
                content_hash: hash,
                ..
            },
    } = &terminal.result
    else {
        unreachable!()
    };
    assert_eq!(intent_id, &alias);
    assert_eq!(hash, &content_hash(&bytes));
    let actual_version = version.clone();
    client
        .handle_distributed_write_response(primary, terminal)
        .unwrap();
    assert!(
        client
            .writes
            .pending_routed_writes
            .values()
            .all(|pending| pending.command.is_none())
    );
    assert!(
        matches!(client.take_write_outcome(&alias).unwrap(), NetworkedDfsWriteOutcome::Committed { version, .. } if version == actual_version)
    );
    assert!(client.writes.pending_routed_writes.is_empty());
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn routed_write_stale_pending_response_does_not_downgrade_completion() {
    let (runtime, mut client, request, base, bytes, nodes, operation, pending_response) =
        native_pending_routed_write("native-routed-late-pending");
    let primary = client.writes.pending_routed_writes[&operation]
        .primary_node_id
        .clone();
    client
        .handle_distributed_write_response(primary.clone(), pending_response.clone())
        .unwrap();
    let (actual_primary, terminal) = complete_native_routed_write(
        &runtime,
        &mut client,
        &request,
        &base,
        &bytes,
        &nodes,
        &operation,
    );
    assert_eq!(actual_primary, primary);
    let DfsDistributedWriteResult::Outcome {
        outcome: NetworkedDfsWriteOutcome::Committed {
            intent_id, version, ..
        },
    } = &terminal.result
    else {
        unreachable!()
    };
    let alias = intent_id.clone();
    let actual_version = version.clone();
    client
        .handle_distributed_write_response(primary.clone(), terminal)
        .unwrap();
    assert!(matches!(
        client
            .handle_distributed_write_response(primary, pending_response)
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    for id in [&operation, &alias] {
        assert!(
            matches!(client.write_outcome(id).unwrap(), NetworkedDfsWriteOutcome::Committed { intent_id, version, .. } if intent_id == alias && version == actual_version)
        );
    }
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn local_write_outcome_wins_over_stale_routed_alias_shadow() {
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    let (runtime, mut client, request, base, bytes, nodes, operation, pending_response) =
        native_pending_routed_write("native-local-committed-before-routed-reply");
    let primary = client.writes.pending_routed_writes[&operation]
        .primary_node_id
        .clone();
    client
        .handle_distributed_write_response(primary.clone(), pending_response)
        .unwrap();
    let (_, terminal) = complete_native_routed_write(
        &runtime,
        &mut client,
        &request,
        &base,
        &bytes,
        &nodes,
        &operation,
    );
    let DfsDistributedWriteResult::Outcome {
        outcome:
            NetworkedDfsWriteOutcome::Committed {
                intent_id,
                version,
                content_hash: hash,
                ..
            },
    } = &terminal.result
    else {
        unreachable!()
    };
    let pending = &client.writes.pending_routed_writes[intent_id];
    assert!(
        pending.result.is_none(),
        "the actual terminal response has not been consumed"
    );
    assert_eq!(pending.canonical_request_id, operation);
    assert!(
        pending.remote_intent_id.is_none(),
        "retry registration carries no unconsumed remote receipt evidence"
    );
    // Reconstruct local committed custody from the actual replicated native
    // record while the genuine terminal response stays unconsumed.
    // Owner commit proves publication, not that this replica has already
    // applied its detached commit notification. Establish this test's local
    // custody premise through normal bounded maintenance before hydrating.
    let mut observed = Vec::new();
    loop {
        let record = client
            .metadata_backend()
            .unwrap()
            .get_intent(&request.object_id, intent_id)
            .unwrap()
            .expect("actual local native intent");
        observed.push(record.status.clone());
        if record.status == DfsWriteIntentStatus::Committed {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "local committed custody unavailable within the original fixture deadline: {observed:?}"
        );
        let tick = runtime
            .maintenance_tick(
                &primary,
                remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .unwrap();
        assert!(tick.outcome.is_ok(), "{:?}", tick.outcome);
        assert_eq!(tick.omitted_kv_event_count, 0);
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "replica catch-up exceeded the original fixture deadline"
        );
        let tick = runtime
            .maintenance_tick(
                client.node_id(),
                remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .unwrap();
        assert!(tick.outcome.is_ok(), "{:?}", tick.outcome);
        assert_eq!(tick.omitted_kv_event_count, 0);
    }
    eprintln!("NATIVE_LOCAL_COMMITTED_CUSTODY {observed:?}");
    client.hydrate_from_metadata_backend().unwrap();
    let recorded = client
        .metadata_backend()
        .unwrap()
        .get_intent(&request.object_id, intent_id)
        .unwrap()
        .expect("actual committed local native intent");
    assert_eq!(recorded.status, DfsWriteIntentStatus::Committed);
    assert_eq!(recorded.target_version, *version);
    assert_eq!(recorded.content_hash, Some(hash.clone()));
    assert!(
        client.writes.pending_routed_writes[intent_id]
            .result
            .is_none()
    );
    assert!(
        matches!(client.write_outcome(intent_id).unwrap(), NetworkedDfsWriteOutcome::Committed { intent_id: actual_intent, version: actual_version, content_hash: actual_hash, .. }
        if actual_intent == *intent_id && actual_version == *version && actual_hash == *hash)
    );
    drop(client);
    runtime.close().unwrap();
}

#[test]
fn native_routed_namespace_replays_a_bounded_actual_backlog() {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-routing-backlog", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-routing-backlog", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let primary = shard.primary_node_id.clone();
    let mut source = runtime.client(&primary).unwrap();
    let mut checkpointed = 0;
    for index in 0..(DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT / 2 + 8) {
        source
            .posix_write_file(
                &format!("/services/native-backlog-{index}.bin"),
                &(index as u64).to_le_bytes(),
                DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
            )
            .unwrap();
        if (index + 1) % 32 == 0 {
            checkpointed += runtime.checkpoint_rpc_observation_window().unwrap();
        }
    }
    checkpointed += runtime.checkpoint_rpc_observation_window().unwrap();
    assert!(checkpointed > 0);
    drop(source);
    let current_shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let requester = nodes
        .iter()
        .find(|node| **node != current_shard.primary_node_id)
        .unwrap()
        .clone();
    let global_observations_before = runtime.rpc_observations().len();
    let global_dropped_before = runtime.observation_dropped_count();
    let capture = runtime
        .capture_namespace_requests(&current_shard.primary_node_id, 32)
        .unwrap();
    let mut client = runtime.client(&requester).unwrap();
    client
        .posix_mkdir_all("/services/native-after-backlog/deep")
        .unwrap();
    let rows = client.posix_list_dir_distributed("/services").unwrap();
    assert!(rows.iter().any(|row| row.name == "native-after-backlog"));
    let actual = client
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(
            &DfsMetadataShardId::new("services"),
            0,
            DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT,
            DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    assert!(actual.entries.len() > DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
    assert!(actual.entries.len() <= DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_ENTRY_LIMIT);
    for entry in actual.entries {
        assert!(matches!(
            entry.mutation,
            DfsMetadataMutation::PrepareOperationV7 { .. }
                | DfsMetadataMutation::CommitPreparedOperationV7 { .. }
        ));
    }
    // This window observes only real post-backlog Main dispatches. Its records()
    // checks zero omissions independently of the long-lived transport vector.
    let requests = capture.records();
    let exact_owner_request = |message: &crate::DfsRoutedMessage| {
        if message.source_node_id != requester
            || message.target_node_id != current_shard.primary_node_id
        {
            return false;
        }
        let DfsNetworkMessage::NamespaceRequest(request) = &message.message else {
            return false;
        };
        request.requesting_node_id == requester
            && request.file_system_id == current_shard.file_system_id
            && request.authority_shards == vec![current_shard.clone()]
    };
    assert!(
        requests
            .iter()
            .filter(|message| exact_owner_request(message))
            .any(|message| {
                matches!(&message.message, DfsNetworkMessage::NamespaceRequest(request)
            if matches!(&request.operation, DfsNamespaceOperation::MkdirAll { path }
                if path == "/services/native-after-backlog/deep"))
            })
    );
    assert!(requests.iter().filter(|message| exact_owner_request(message)).any(|message| {
        matches!(&message.message, DfsNetworkMessage::NamespaceRequest(request)
            if matches!(&request.operation, DfsNamespaceOperation::MetadataQuery { query }
                if query.file_system_id == current_shard.file_system_id
                    && query.shard_id.0 == current_shard.shard_id
                    && query.epoch == current_shard.epoch
                    && matches!(&query.selection, crate::DfsMetadataSelection::DirectoryPage { path, .. }
                        if path == "/services")))
    }));
    let global_dropped_after = runtime.observation_dropped_count();
    drop(client);
    let coverage = runtime.close_with_namespace_capture(capture).unwrap();
    assert_eq!(coverage.namespace_omitted, Some(0));
    assert_eq!(global_dropped_before, 0);
    assert_eq!(global_dropped_after, 0);
    assert_eq!(coverage.lifetime_omitted, 0);
    assert!(coverage.lifetime_records >= checkpointed);
    assert_eq!(coverage.full_lifetime_coverage, "available");
    println!(
        "backlog observation coverage: before_records={global_observations_before} before_dropped={global_dropped_before} after_dropped={global_dropped_after} final={}",
        serde_json::to_string(&coverage).unwrap()
    );
}

#[test]
fn dfs_peer_rpc_helpers_preserve_route_contracts() {
    let shard_id = DfsMetadataShardId::new("rpc-helper-shard");
    let local = NodeId::from_legacy_named("node_dfs_rpc_helper_local").expect("local");
    let remote = NodeId::from_legacy_named("node_dfs_rpc_helper_remote").expect("remote");

    let status_request = dfs_status_rpc_request().expect("status request");
    assert_eq!(status_request.route, DFS_STATUS_ROUTE);
    assert_eq!(status_request.category, DFS_RPC_CATEGORY);
    status_request
        .decode_body::<()>()
        .expect("status empty body");
    let status = DfsOperatorStatus {
        storage_migrations: Vec::new(),
        incomplete_reasons: Vec::new(),
        bounded_omissions: Vec::new(),
        file_system_count: 1,
        object_count: 2,
        version_count: 3,
        receipt_count: 4,
        intent_count: 5,
        pending_write_count: 6,

        pending_repair_count: 8,
        repair_attempt_count: 0,
        pending_namespace_job_count: 9,
        failed_namespace_job_count: 10,
        cross_shard_namespace_lock_count: Some(11),
        metadata_shards: Vec::new(),
        repair_attempts: Vec::new(),
    };
    let status_rpc_response =
        sinomo_core::network::ObjectRpcResponse::encode_body(DFS_STATUS_ROUTE, &status)
            .expect("status response");
    assert_eq!(
        decode_dfs_status_rpc_response(&status_rpc_response).expect("status response body"),
        status
    );

    let inspection_request = DaemonDfsInspectionRequest::Object {
        object_id: DfsObjectId::from_legacy_named("object_rpc_helper").expect("object"),
    };
    let inspection_rpc_request =
        dfs_inspection_rpc_request(&inspection_request).expect("inspection request");
    assert_eq!(inspection_rpc_request.route, DFS_INSPECTION_ROUTE);
    assert_eq!(inspection_rpc_request.category, DFS_RPC_CATEGORY);
    assert_eq!(
        inspection_rpc_request
            .decode_body::<DaemonDfsInspectionRequest>()
            .expect("inspection body"),
        inspection_request
    );
    let inspection_response = DaemonDfsInspectionResponse::Object { object: None };
    let inspection_rpc_response = sinomo_core::network::ObjectRpcResponse::encode_body(
        DFS_INSPECTION_ROUTE,
        &inspection_response,
    )
    .expect("inspection response");
    assert_eq!(
        decode_dfs_inspection_rpc_response(&inspection_rpc_response)
            .expect("inspection response body"),
        inspection_response
    );

    let smoke_request = dfs_smoke_rpc_request().expect("smoke request");
    assert_eq!(smoke_request.route, DFS_SMOKE_ROUTE);
    assert_eq!(smoke_request.category, DFS_RPC_CATEGORY);
    smoke_request.decode_body::<()>().expect("smoke empty body");
    let smoke_response = DfsSmokeReport {
        path: "/smoke.txt".to_string(),
        file_system_id: sifs_file_system_id(),
        object_id: DfsObjectId::from_legacy_named("object_smoke_rpc_helper").expect("object"),
        version: DfsVersion::from_legacy_named("version_smoke_rpc_helper").expect("version"),
        content_hash: Some("hash".to_string()),
        size_bytes: Some(4),
        requested_replica_node_ids: vec![local.clone()],
        committed_replica_node_ids: vec![local.clone()],
        pending_repair_count: 0,
        metadata_replication: DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        },
        attempts: vec![DfsSmokeAttempt {
            replica_node_ids: vec![local.clone()],
            path: "/smoke.txt".to_string(),
            committed: true,
            detail: "ok".to_string(),
        }],
    };
    let smoke_rpc_response =
        sinomo_core::network::ObjectRpcResponse::encode_body(DFS_SMOKE_ROUTE, &smoke_response)
            .expect("smoke response");
    assert_eq!(
        decode_dfs_smoke_rpc_response(&smoke_rpc_response).expect("smoke response body"),
        smoke_response
    );

    let journal_command = DfsMetadataJournalBatchCommand {
        request_id: "journal-request".to_string(),
        source_node_id: Some(local.clone()),
        file_system_id: None,
        authority_epoch: None,
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        durable_replication: None,
        batch: DfsMetadataJournalFrameBatch {
            shard_id: shard_id.clone(),
            journal_frames: Vec::new(),
            next_after_sequence: 7,
        },
    };
    let journal_request =
        dfs_metadata_journal_batch_rpc_request(&journal_command).expect("journal request");
    assert_eq!(journal_request.route, DFS_METADATA_JOURNAL_BATCH_ROUTE);
    assert_eq!(journal_request.category, DFS_RPC_CATEGORY);
    assert_eq!(
        journal_request
            .decode_body::<DfsMetadataJournalBatchCommand>()
            .expect("journal body"),
        journal_command
    );
    let journal_response = DfsMetadataJournalBatchResponse {
        request_id: "journal-request".to_string(),
        shard_id: shard_id.clone(),
        next_after_sequence: 7,
        applied_entries: 0,
        verified_prefix_only: false,
        verified_authority_probe_id: None,
        result: DfsMetadataJournalBatchResult::Applied,
    };
    let journal_rpc_response = sinomo_core::network::ObjectRpcResponse::encode_body(
        DFS_METADATA_JOURNAL_BATCH_ROUTE,
        &journal_response,
    )
    .expect("journal response");
    assert_eq!(
        decode_dfs_metadata_journal_batch_rpc_response(&journal_rpc_response)
            .expect("journal response body"),
        journal_response
    );

    let snapshot_root = temp_root("rpc_helper_snapshot");
    let snapshot = LocalSobjMetadataBackend::new(shard_id.clone(), &snapshot_root)
        .expect("metadata backend")
        .export_snapshot_image()
        .expect("native snapshot image");
    let snapshot_command = DfsMetadataSnapshotInstallCommand {
        request_id: "snapshot-request".to_string(),
        source_node_id: local.clone(),
        file_system_id: None,
        authority_epoch: None,
        primary_handoff_id: None,
        shard_id: shard_id.clone(),
        next_after_sequence: 9,
        snapshot: snapshot.descriptor().clone(),
        image: Some(snapshot),
        reason: "test".to_string(),
    };
    let snapshot_request =
        dfs_metadata_snapshot_install_rpc_request(&snapshot_command).expect("snapshot request");
    assert_eq!(snapshot_request.route, DFS_METADATA_SNAPSHOT_INSTALL_ROUTE);
    assert_eq!(snapshot_request.category, DFS_RPC_CATEGORY);
    assert_eq!(
        snapshot_request
            .decode_body::<DfsMetadataSnapshotInstallCommand>()
            .expect("snapshot body"),
        snapshot_command
    );
    let snapshot_response = DfsMetadataSnapshotInstallResponse {
        request_id: "snapshot-request".to_string(),
        shard_id: shard_id.clone(),
        next_after_sequence: 9,
        result: DfsMetadataSnapshotInstallResult::Installed {
            archived_existing: false,
        },
    };
    let snapshot_rpc_response = sinomo_core::network::ObjectRpcResponse::encode_body(
        DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
        &snapshot_response,
    )
    .expect("snapshot response");
    assert_eq!(
        decode_dfs_metadata_snapshot_install_rpc_response(&snapshot_rpc_response)
            .expect("snapshot response body"),
        snapshot_response
    );

    let message_command = DfsMessageCommand {
        request_id: "message-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 2,
            message_id: 3,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: "message-read".to_string(),
                object_id: DfsObjectId::generate(),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        },
    };
    let message_request = dfs_message_rpc_request(&message_command).expect("message request");
    assert_eq!(message_request.route, DFS_DATA_READ_ROUTE);
    assert_eq!(message_request.category, DFS_RPC_CATEGORY);
    assert_eq!(
        message_request
            .decode_body::<DfsMessageCommand>()
            .expect("message body"),
        message_command
    );
    assert_eq!(
        decode_dfs_message_rpc_request(&message_request).expect("validated message request"),
        message_command
    );
    let correlated_traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
    let correlated_request = crate::with_dfs_traceparent(Some(correlated_traceparent), || {
        dfs_message_rpc_request(&message_command).expect("correlated message request")
    });
    assert_eq!(
        correlated_request.traceparent(),
        Some(correlated_traceparent)
    );
    let namespace_command = DfsMessageCommand {
        request_id: "namespace-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 6,
            message_id: 7,
            message: DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: "namespace-mkdir".to_string(),
                file_system_id: sifs_file_system_id(),
                requesting_node_id: local.clone(),
                include_metadata_journal_batches: true,
                metadata_journal_after_sequences: Vec::new(),
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::Mkdir {
                    path: "/rpc-helper".to_string(),
                },
            }),
        },
    };
    let namespace_request =
        dfs_message_rpc_request(&namespace_command).expect("namespace message request");
    assert_eq!(namespace_request.route, DFS_DATA_NAMESPACE_ROUTE);
    assert!(
        namespace_request.is_response_retry_safe(),
        "durably admitted namespace mutations must reconcile an exact retry after a lost response"
    );
    let distributed_command = DfsMessageCommand {
        request_id: "distributed-write-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 8,
            message_id: 9,
            message: DfsNetworkMessage::DistributedWriteRequest(test_routed_write_command(
                "distributed-write-request",
                &local,
                &DfsObjectId::generate(),
            )),
        },
    };
    let distributed_request =
        dfs_message_rpc_request(&distributed_command).expect("distributed write message request");
    assert!(
        distributed_request.is_response_retry_safe(),
        "distributed write request IDs make a lost response safe to redispatch"
    );
    let distributed_response = dfs_message_rpc_request(&DfsMessageCommand {
        request_id: "distributed-write-response".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: local.clone(),
            frame_id: 9,
            message_id: 10,
            message: DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                request_id: "distributed-write-response".to_string(),
                object_id: DfsObjectId::generate(),
                result: DfsDistributedWriteResult::Rejected {
                    reason: "test completed response".to_string(),
                    current_primary_node_id: None,
                },
                metadata_journal_batches: Vec::new(),
            }),
        },
    })
    .expect("distributed write response message request");
    assert!(
        distributed_response.is_response_retry_safe(),
        "a completed response is requester-idempotent and must survive an ambiguous detached send"
    );
    let mut exclusive_create =
        test_routed_write_command("exclusive-create-request", &local, &DfsObjectId::generate());
    exclusive_create.exclusive_create = true;
    let exclusive_request = dfs_message_rpc_request(&DfsMessageCommand {
        request_id: "exclusive-create-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 10,
            message_id: 11,
            message: DfsNetworkMessage::ExclusiveCreateRequest(exclusive_create),
        },
    })
    .expect("exclusive create message request");
    assert!(
        !exclusive_request.is_response_retry_safe(),
        "exclusive create cannot be redispatched until its success response is durably replayable"
    );
    let repair_object_id = DfsObjectId::generate();
    let repair_version = DfsVersion::generate();
    let repair_request = dfs_message_rpc_request(&DfsMessageCommand {
        request_id: "repair-request".to_string(),
        routed_message: DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 10,
            message_id: 11,
            message: DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                background_repair: false,
                request_id: "repair-request".to_string(),
                intent_id: "repair-intent".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: repair_object_id.clone(),
                expected_base_version: None,
                version: repair_version.clone(),
                content_hash: "blake3:repair".to_string(),
                bytes: b"repair".to_vec(),
                object_metadata: None,
                authority_shard: None,
            }),
        },
    })
    .expect("repair message request");
    assert!(
        repair_request.is_response_retry_safe(),
        "content-addressed repair commands are safe to redispatch after a lost response"
    );
    let mismatched_route_request = sinomo_core::network::ObjectRpcRequest::encode_body(
        DFS_DATA_WRITE_ROUTE,
        DFS_RPC_CATEGORY,
        &message_command,
    )
    .expect("mismatched route request");
    let mismatched = decode_dfs_message_rpc_request(&mismatched_route_request)
        .expect_err("typed DFS route should match message body");
    assert!(
        mismatched
            .to_string()
            .contains("does not match message route"),
        "unexpected mismatch error: {mismatched}"
    );
    let message_response = DfsMessageResponse {
        request_id: "message-request".to_string(),
        outbound_messages: vec![DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: local.clone(),
            frame_id: 4,
            message_id: 5,
            message: DfsNetworkMessage::ReadResponse(DfsReadResponse {
                request_id: "message-read".to_string(),
                object_id: DfsObjectId::generate(),
                result: DfsReadResult::NotFound,
            }),
        }],
    };
    let message_rpc_response = sinomo_core::network::ObjectRpcResponse::encode_body(
        DFS_DATA_READ_RESPONSE_ROUTE,
        &message_response,
    )
    .expect("message response");
    assert_eq!(
        decode_dfs_message_rpc_response(&message_rpc_response).expect("message response body"),
        message_response
    );
    assert_eq!(
        test_dfs_message_response_from_routed_messages(
            "message-request",
            message_response.outbound_messages.clone()
        ),
        message_response
    );
    std::fs::remove_dir_all(snapshot_root).ok();
}

#[test]
fn networked_dfs_builds_routed_messages_without_transport_send() {
    let root = temp_root("networked-dfs-builds-routed-messages");
    let local = NodeId::from_legacy_named("node_build_frame_local").expect("local");
    let remote = NodeId::from_legacy_named("node_build_frame_remote").expect("remote");
    let object_id = DfsObjectId::from_legacy_named("obj_build_frame").expect("object");
    let content = LocalContentStore::new(root.join("content")).expect("content");
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(local.clone()),
        content,
    );

    let first_routed_message = dfs.next_dfs_routed_message(
        &remote,
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "read-1".to_string(),
            object_id: object_id.clone(),
            version: None,
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
        }),
    );

    assert_eq!(first_routed_message.frame_id, 1);
    assert_eq!(first_routed_message.source_node_id, local);
    assert_eq!(first_routed_message.target_node_id, remote);
    assert_eq!(first_routed_message.message_id, 1);
    match &first_routed_message.message {
        DfsNetworkMessage::ReadRequest(request) => {
            assert_eq!(request.request_id, "read-1");
            assert_eq!(request.object_id, object_id);
        }
        other => panic!("unexpected DFS payload: {other:?}"),
    }

    let second_routed_message = dfs.next_dfs_routed_message(
        &first_routed_message.target_node_id.clone(),
        DfsNetworkMessage::ReadRequest(DfsReadRequest {
            request_id: "read-2".to_string(),
            object_id: DfsObjectId::from_legacy_named("obj_build_frame_second").expect("object"),
            version: None,
            content_hash: None,
            content_size_bytes: None,
            range_offset: None,
            range_max_bytes: None,
        }),
    );
    assert_eq!(second_routed_message.frame_id, 2);
    assert_eq!(second_routed_message.message_id, 2);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn local_owned_read_is_routed_to_owner_node() {
    let name = "native-local-owned-routed-read";
    let (runtime, mut owner_dfs, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let owner = config.primary_node_id;
    let remote = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != owner)
        .unwrap();
    let path = "/services/native-local-owned-read";
    let apply = runtime
        .apply_placement_policy(
            &owner,
            path,
            DfsPlacementScope::SelfOnly,
            DfsPlacementPolicy::new(DfsObjectPolicy::LocalOwned, 1, 1).unwrap(),
        )
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let job = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&apply.file_system_id, &apply.job_id)
            })
            .unwrap()
            .unwrap();
        assert_eq!(job.path, path);
        assert_eq!(job.scanned_object_count, 0);
        assert_eq!(job.scheduled_repair_count, 0);
        assert!(job.last_error.is_none());
        if job.state == DfsPlacementReconciliationState::Completed {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "normal empty LocalOwned Apply must complete"
        );
        runtime
            .maintenance_tick(&owner, remaining)
            .unwrap()
            .outcome
            .unwrap();
    }
    drop(control);
    let stat = owner_dfs
        .posix_write_file(
            path,
            b"owned bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    assert_eq!(stat.policy, DfsObjectPolicy::LocalOwned);
    // The facade's SyncPath does not replay into Main's store. Certify the
    // actual author cut on both metadata members before the object-id read.
    // The barrier and existing lookup share the original five-second budget.
    let sync_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let object = owner_dfs
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    let target = owner_dfs
        .capture_metadata_quorum_sync_target(&object)
        .unwrap();
    let certificate = owner_dfs
        .sync_metadata_shard_to_quorum(
            &target,
            sync_deadline.saturating_duration_since(Instant::now()),
        )
        .unwrap();
    assert_eq!(
        certificate
            .acknowledged_node_ids
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner.clone(), remote.clone()])
    );
    let mut reader = runtime.client(&remote).unwrap();
    let remaining = sync_deadline.saturating_duration_since(Instant::now());
    assert!(
        !remaining.is_zero(),
        "metadata setup retains its original sync budget"
    );
    let synced = reader
        .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
            stat.file_system_id.clone(),
            owner.clone(),
            DfsNamespaceOperation::SyncPath { path: path.into() },
            remaining,
            true,
            true,
        )
        .unwrap();
    assert_eq!(synced.result, DfsNamespaceResult::Applied);
    assert!(!synced.metadata_journal_has_more);
    assert_eq!(
        reader
            .request_remote_object_read_without_cache(&stat.object_id)
            .unwrap(),
        vec![owner.clone()]
    );
    let request_id = reader
        .reads
        .pending_reads
        .keys()
        .next()
        .expect("actual pending read")
        .clone();
    let events = reader.process_incoming().unwrap();
    assert!(events.iter().any(|event| matches!(event,
        NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: actual, source_node_id,
            result: DfsReadResult::Found(DfsReadData { bytes, .. }), ..
        } if actual == &request_id && source_node_id == &owner && bytes == b"owned bytes")));
    assert!(
        !runtime
            .rpc_observations()
            .iter()
            .any(|event| event.target == remote && event.route == DFS_DATA_STAGE_BLOB_ROUTE)
    );
    drop(reader);
    drop(owner_dfs);
    runtime.close().unwrap();
}

#[test]
fn replicated_eventual_write_is_routed_to_metadata_primary_and_committed() {
    let mut cluster = sifs_posix_cluster("routed-primary-write-commit", "/users");
    let path = "/users/routed-primary-write.txt";
    sifs_cluster_primary_posix_write(&mut cluster, path, b"old bytes");
    let original_hash = content_hash(b"old bytes");
    sifs_cluster_publish_metadata_until(&mut cluster, "seed writer metadata", |cluster| {
        cluster
            .writer
            .posix_stat(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(original_hash.as_str()))
    });
    let original = cluster
        .writer
        .posix_stat(path)
        .expect("original writer stat");
    let object_id = original.object_id.clone();
    let target_version = crate::namespace::next_version(&original.version);
    let primary_node = cluster.primary_node.clone();
    let request = DfsWriteRequest {
        file_system_id: sifs_file_system_id(),
        object_id: object_id.clone(),
        target_version: target_version.clone(),
        requesting_node_id: cluster.writer_node.clone(),
    };
    assert_ne!(cluster.writer_node, primary_node);
    let outcome = sifs_cluster_writer_call_with_pump(&mut cluster, "routed overwrite", |writer| {
        writer.write_distributed_eventual(
            request,
            b"updated bytes".to_vec(),
            vec![primary_node],
            DfsReplicationPolicy::new(1, 1).expect("single-owner content policy"),
        )
    })
    .expect("write routes to metadata primary");
    if let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = outcome {
        sifs_posix_cluster_pump_until(&mut cluster, "routed overwrite response", |cluster| {
            matches!(
                cluster.writer.write_outcome(&intent_id),
                Ok(NetworkedDfsWriteOutcome::Committed { .. })
            )
        });
    } else {
        assert!(matches!(
            outcome,
            NetworkedDfsWriteOutcome::Committed { .. }
        ));
    }
    assert_eq!(
        cluster
            .primary
            .read_local_object(&object_id)
            .expect("primary read"),
        Some(DfsReadData {
            object_id: object_id.clone(),
            version: target_version.clone(),
            content_hash: Some(content_hash(b"updated bytes")),
            bytes: b"updated bytes".to_vec(),
        }),
    );
    let writer_stat = cluster
        .writer
        .posix_stat(path)
        .expect("writer metadata updated");
    assert_eq!(writer_stat.object_id, object_id);
    assert_eq!(writer_stat.version, target_version);
    assert_eq!(
        writer_stat.content_hash.as_deref(),
        Some(content_hash(b"updated bytes").as_str()),
    );
}

#[test]
fn sharded_lazy_reader_refreshes_after_remote_primary_overwrite() {
    let name = "native-lazy-reader-remote-primary";
    let (runtime, primary, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary_node = config.primary_node_id;
    let writer_node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary_node)
        .unwrap();
    let path = "/services/current-read-remote-primary.txt";
    let old = b"old remote primary reader payload".to_vec();
    let new = b"new remote primary reader payload".to_vec();
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    placement.required_node_ids = vec![writer_node.clone()];
    placement.forbidden_node_ids = vec![primary_node.clone()];
    runtime
        .apply_placement_policy(&primary_node, path, DfsPlacementScope::SelfOnly, placement)
        .unwrap();
    let mut writer = runtime.client(&writer_node).unwrap();
    let (_, _, policy) = writer
        .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 1)
        .unwrap();
    assert_eq!(policy.required_node_ids, vec![writer_node.clone()]);
    let options = DfsWriteOptions {
        policy,
        replica_candidates: vec![writer_node.clone()],
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    writer
        .posix_write_file(path, &old, options.clone())
        .unwrap();
    let mut reader = crate::runtime_host::open_dfs_runtime_service(
        runtime.data_dir(&writer_node),
        Some(&runtime.runtime_handle()),
    )
    .unwrap();
    let mut stale_stream = reader.posix_open_file_stream(path).unwrap();
    let replacement = writer.posix_write_file(path, &new, options).unwrap();
    let mut stale = Vec::new();
    stale_stream.reader.read_to_end(&mut stale).unwrap();
    assert_eq!(
        stale, old,
        "opened content retains its actual pre-overwrite generation"
    );
    let mut current_stream = reader.posix_open_current_file_stream(path).unwrap();
    assert_eq!(current_stream.object_id, replacement.object_id);
    assert_eq!(current_stream.version, replacement.version);
    let mut current = Vec::new();
    current_stream.reader.read_to_end(&mut current).unwrap();
    assert_eq!(
        current, new,
        "fresh authority selects the current exact local body"
    );
    drop(current_stream);
    drop(stale_stream);
    drop(reader);
    drop(writer);
    drop(primary);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_posix_routed_create_is_read_your_write_on_writer() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-ryw", "/users");
    let path = "/users/read-your-write.txt";
    let bytes = b"hello from the writer node";

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);

    // The ordinary client resolves current metadata through the real Main
    // authority. A routed commit does not promise a client-local content cache.
    assert_eq!(
        cluster
            .writer
            .posix_read_file_distributed(path)
            .expect("writer read"),
        bytes.to_vec()
    );
    let stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("writer stat");
    assert_eq!(stat.kind, DfsObjectKind::File);
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(bytes).as_str())
    );

    // The writer must immediately list the committed entry through current authority.
    let entries = cluster
        .writer
        .posix_list_dir_distributed("/users")
        .expect("writer list /users");
    assert!(
        entries
            .iter()
            .any(|entry| entry.path == path && entry.kind == DfsObjectKind::File),
        "writer directory listing must contain the routed write: {entries:?}"
    );

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_write_is_visible_on_primary() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-primary-vis", "/users");
    let path = "/users/primary-visibility.txt";
    let bytes = b"routed bytes visible on the primary";

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);

    // The shard primary serialized the write, so its metadata must already be
    // authoritative: correct kind, size, and content hash.
    let stat = cluster.primary.posix_stat(path).expect("primary stat");
    assert_eq!(stat.kind, DfsObjectKind::File);
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(
        stat.content_hash.as_deref(),
        Some(content_hash(bytes).as_str())
    );

    let entries = cluster
        .primary
        .posix_list_dir("/users")
        .expect("primary list /users");
    assert!(
        entries.iter().any(|entry| entry.path == path),
        "primary directory listing must contain the routed write: {entries:?}"
    );

    // The content was staged on the writer; the primary must be able to make
    // it locally readable through the remote-read path.
    let read = sifs_cluster_fetch_for_local_read(&mut cluster, |c| &mut c.primary, path);
    assert_eq!(read, bytes.to_vec());

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_overwrite_advances_version_monotonically() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-overwrite", "/users");
    let path = "/users/overwrite.txt";
    let first = b"first version".to_vec();
    let second = b"the second, longer version of the content".to_vec();
    let third = b"3rd".to_vec();

    sifs_cluster_routed_posix_write(&mut cluster, path, &first);
    let first_stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("first stat");

    sifs_cluster_routed_posix_write(&mut cluster, path, &second);
    let second_stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("second stat");
    assert_eq!(
        cluster
            .writer
            .posix_read_file_distributed(path)
            .expect("second read"),
        second
    );
    assert_eq!(second_stat.size_bytes, Some(second.len() as u64));
    assert!(
        second_stat.version > first_stat.version,
        "overwrite must advance the visible version: {:?} -> {:?}",
        first_stat.version,
        second_stat.version
    );
    assert!(
        second_stat.modified_at_unix_nanos >= first_stat.modified_at_unix_nanos,
        "overwrite must not move mtime backwards"
    );

    // Shrinking overwrite: size must follow the latest committed content.
    sifs_cluster_routed_posix_write(&mut cluster, path, &third);
    let third_stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("third stat");
    assert_eq!(third_stat.size_bytes, Some(third.len() as u64));
    assert!(third_stat.version > second_stat.version);
    assert_eq!(
        cluster
            .writer
            .posix_read_file_distributed(path)
            .expect("third read"),
        third
    );

    // The primary observes the same latest version.
    let primary_stat = cluster
        .primary
        .posix_stat_distributed(path)
        .expect("primary stat");
    assert_eq!(
        primary_stat.content_hash.as_deref(),
        Some(content_hash(&third).as_str())
    );
    assert_eq!(primary_stat.size_bytes, Some(third.len() as u64));

    drop(cluster);
}

#[test]
fn sifs_cluster_posix_routed_create_updates_parent_directory_mtime_cluster_wide() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-parent-mtime", "/users");
    let parent = "/users";
    let path = "/users/parent-mtime.txt";
    let bytes = b"parent mtime must advance everywhere";

    let writer_parent_before = cluster
        .writer
        .posix_stat(parent)
        .expect("writer parent stat before");

    sifs_cluster_routed_posix_write(&mut cluster, path, bytes);

    // POSIX: creating an entry updates the parent directory mtime. The shard
    // primary journals that update, so the writer must observe it through the
    // routed-write response replay without appending to its own journal.
    let primary_parent = cluster
        .primary
        .posix_stat(parent)
        .expect("primary parent stat");
    sifs_posix_cluster_pump_until(&mut cluster, "writer parent mtime replay", |cluster| {
        cluster
            .writer
            .posix_stat(parent)
            .is_ok_and(|stat| stat.modified_at_unix_nanos == primary_parent.modified_at_unix_nanos)
    });
    let writer_parent = cluster
        .writer
        .posix_stat(parent)
        .expect("writer parent stat after");
    assert!(
        writer_parent.modified_at_unix_nanos > writer_parent_before.modified_at_unix_nanos,
        "routed create must advance the parent directory mtime on the writer"
    );
    assert_eq!(
        writer_parent.version, primary_parent.version,
        "writer must replay the primary's parent directory record, not invent its own"
    );

    // The observer converges to the identical parent record after publication.
    sifs_cluster_publish_metadata_until(&mut cluster, "observer parent mtime", |cluster| {
        cluster.observer.posix_stat(parent).is_ok_and(|stat| {
            stat.modified_at_unix_nanos == primary_parent.modified_at_unix_nanos
                && stat.version == primary_parent.version
        })
    });

    assert!(
        cluster.rejections.is_empty(),
        "parent mtime propagation must not fork any replica journal: {:?}",
        cluster.rejections
    );

    drop(cluster);
}

#[test]
fn sifs_cluster_retried_pending_routed_create_preserves_parent_mtime_touch() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-retried-create-parent-mtime", "/users");
    let parent = "/users";
    let path = "/users/retried-parent-mtime.txt";
    let bytes = b"retried pending create must retain parent touch";
    let writer_parent_before = cluster
        .writer
        .posix_stat_distributed(parent)
        .expect("writer parent stat before");
    let captured = Arc::new(Mutex::new(
        crate::test_runtime::StageExchangeCapture::default(),
    ));
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.primary_node.clone()),
            target: Some(cluster.observer_node.clone()),
            route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: crate::test_runtime::FaultAction::CaptureStageExchangeAndLoseAfterDispatch(
                Arc::clone(&captured),
            ),
        });
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    let writer_deadline = cluster.writer.transport.operation_deadline();
    let observer_deadline = cluster.observer.transport.operation_deadline();
    cluster.writer.set_transport_operation_deadline(Some(
        writer_deadline.map_or(deadline, |current| current.min(deadline)),
    ));
    cluster.observer.set_transport_operation_deadline(Some(
        observer_deadline.map_or(deadline, |current| current.min(deadline)),
    ));
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(3, 3).expect("three-receipt policy"),
        replica_candidates: vec![
            cluster.writer_node.clone(),
            cluster.observer_node.clone(),
            cluster.primary_node.clone(),
        ],
        wait_for_commit: false,
        foreground_replica_repair: true,
    };
    let error = cluster
        .writer
        .posix_write_file(path, bytes, options)
        .expect_err("losing one actual required Stage reply leaves this create Pending");
    assert!(
        matches!(error, DfsPosixError::PendingWrite { .. }),
        "{error:?}"
    );
    let pending = cluster
        .writer
        .writes
        .pending_routed_writes
        .values()
        .find(|pending| {
            pending.command.as_ref().is_some_and(|command| {
                command
                    .object_metadata
                    .as_ref()
                    .is_some_and(|object| object.path == path)
            })
        })
        .expect("the actual requester retains its original pending create");
    let command = pending.command.as_ref().unwrap().clone();
    let attachment = pending
        .attachment_bytes
        .clone()
        .expect("the SAME original routed create retains its owned attachment");
    assert_eq!(attachment.as_ref(), bytes.as_slice());
    let command_bytes = serde_json::to_vec(&command).unwrap();
    let attachment_digest = content_hash(attachment.as_ref());
    let exchange = {
        let capture = captured.lock().unwrap();
        assert_eq!(capture.dropped_count, 0);
        assert_eq!(capture.exchanges.len(), 1);
        capture.exchanges[0].clone()
    };
    let DfsNetworkMessage::StageBlob(stage) = &exchange.command.routed_message.message else {
        panic!("the selected native Stage head must be retained");
    };
    let DfsNetworkMessage::StageBlobResponse(reply) = &exchange.response.message else {
        panic!("the selected actual Stage reply must be retained");
    };
    assert_eq!(exchange.source_node_id, cluster.primary_node);
    assert_eq!(exchange.target_node_id, cluster.observer_node);
    assert_eq!(exchange.response.source_node_id, cluster.observer_node);
    assert_eq!(exchange.response.target_node_id, cluster.primary_node);
    assert_eq!(exchange.response_request_id, exchange.command.request_id);
    assert_eq!(stage.object_id, command.request.object_id);
    assert_eq!(stage.version, command.request.target_version);
    assert_eq!(stage.file_system_id, command.request.file_system_id);
    assert_eq!(stage.content_hash, content_hash(bytes));
    assert_eq!(reply.request_id, stage.request_id);
    assert_eq!(reply.intent_id, stage.intent_id);
    assert_eq!(reply.object_id, stage.object_id);
    assert_eq!(reply.version, stage.version);
    assert_eq!(reply.file_system_id, stage.file_system_id);
    assert_eq!(reply.replica_node_id, cluster.observer_node);
    assert!(matches!(&reply.result,
        DfsBlobTransferResult::Durable { content_hash } |
        DfsBlobTransferResult::Committed { content_hash }
            if content_hash == &stage.content_hash));
    assert_eq!(
        cluster
            .observer
            .content()
            .read_blob(&stage.content_hash)
            .unwrap(),
        bytes
    );
    // The single actual lost delivery is now retained. Remove only this case's
    // own exact capture rule before any new owner read or recovery traffic can
    // hit its exhausted budget; unrelated faults and all native state remain.
    let removed = cluster._runtime.remove_faults_matching(|rule| {
        rule.source.as_ref() == Some(&cluster.primary_node)
            && rule.target.as_ref() == Some(&cluster.observer_node)
            && rule.route.as_deref() == Some(DFS_DATA_STAGE_BLOB_ROUTE)
            && rule.origin == Some(crate::test_runtime::RpcOrigin::RegisteredHost)
            && rule.call_id.is_none()
            && rule.remaining_hits == 0
            && matches!(&rule.action,
                crate::test_runtime::FaultAction::CaptureStageExchangeAndLoseAfterDispatch(owned)
                    if Arc::ptr_eq(owned, &captured))
    });
    assert_eq!(
        removed, 1,
        "only the verified single lost Stage rule is owned here"
    );
    cluster._runtime.assert_fault_budgets().unwrap();
    // Keep the missing receipt unavailable while exercising Pending replay.
    // Normal recovery may otherwise obtain another real acknowledgement as
    // soon as the one-shot loss rule is removed.
    cluster
        ._runtime
        .install_fault(crate::test_runtime::FaultRule {
            source: Some(cluster.primary_node.clone()),
            target: Some(cluster.observer_node.clone()),
            route: Some(DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::BudgetExhausted,
        });
    let intent_id = stage.intent_id.clone();
    // The pre-W3 owned DATA is the real durable write reservation. The
    // committed repair queue does not promise a pre-commit Stage row.
    let original_pending = {
        let mut owner = cluster._runtime.metadata_primary_service(path).unwrap();
        owner
            .metadata_backend()
            .unwrap()
            .get_intent(&stage.object_id, &intent_id)
            .unwrap()
            .expect("normal Main persists the actual pending write reservation")
    };
    let original_custody = serde_json::json!({
        "classification": "actual original native pending request DATA before duplicate dispatch",
        "command": command,
        "commandJsonBlake3": blake3::hash(&command_bytes).to_hex().to_string(),
        "originalAttachmentSize": attachment.len(),
        "originalAttachmentHash": attachment_digest,
        "originalAttachment": attachment.as_ref(),
        "capturedStageCommand": exchange.command,
        "capturedStageReply": exchange.response,
        "originalPendingIntent": original_pending,
    });
    let original_raw = serde_json::to_vec(&original_custody)
        .expect("serialize primitive original request custody");
    eprintln!(
        "SINOMO_NATIVE_PENDING_CREATE_ORIGINAL_RAW {}",
        String::from_utf8_lossy(&original_raw)
    );
    let original_output = cluster
        .root
        .join("pending-create-original-request-custody.json");
    let original_saved = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&original_output)
        .and_then(|mut file| std::io::Write::write_all(&mut file, &original_raw));
    eprintln!("SINOMO_NATIVE_PENDING_CREATE_ORIGINAL_FILE {original_output:?}: {original_saved:?}");
    assert_eq!(
        original_pending.file_system_id,
        command.request.file_system_id
    );
    assert_eq!(original_pending.object_id, command.request.object_id);
    assert_eq!(
        original_pending.target_version,
        command.request.target_version
    );
    assert_eq!(
        original_pending.operation_id.as_deref(),
        Some(command.request_id.as_str())
    );
    assert_eq!(original_pending.policy.min_commit_replicas, 3);
    assert_eq!(original_pending.policy.desired_replicas, 3);
    assert!(matches!(
        original_pending.status,
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
    ));
    assert!(original_pending.touch_parent_on_create);
    // Discard only already-returned DATA before the second native request.
    // The SAME complete immutable command and request id are redispatched.
    let _already_returned = cluster.writer.transport.drain_incoming_messages().unwrap();
    assert_eq!(serde_json::to_vec(&command).unwrap(), command_bytes);
    assert_eq!(attachment.as_ref(), bytes.as_slice());
    assert_eq!(content_hash(attachment.as_ref()), attachment_digest);
    let routed = cluster.writer.next_dfs_routed_message(
        &cluster.primary_node,
        DfsNetworkMessage::DistributedWriteRequest(command.clone()),
    );
    cluster
        .writer
        .transport
        .send_routed_call(
            &cluster.primary_node,
            DfsRoutedCall::with_attachment(
                routed,
                DfsOutboundAttachment::from_shared_bytes_with_transport_integrity(
                    attachment.clone(),
                ),
            ),
        )
        .expect("redispatch the SAME actual native head AND original owned attachment");
    let duplicate = cluster
        .writer
        .transport
        .drain_incoming_messages()
        .unwrap()
        .into_iter()
        .filter(|routed| {
            routed.source_node_id == cluster.primary_node
                && routed.target_node_id == cluster.writer_node
        })
        .find_map(|routed| match routed.message {
            DfsNetworkMessage::DistributedWriteResponse(response)
                if response.request_id == command.request_id
                    && response.object_id == stage.object_id =>
            {
                Some(response)
            }
            _ => None,
        })
        .expect("the actual Main returns the same pending create's native response");
    let retried_pending_observation = (|| -> Result<Option<DfsWriteIntentRecord>, String> {
        let mut owner = cluster._runtime.metadata_primary_service(path)?;
        owner
            .metadata_backend()
            .ok_or_else(|| "actual pending inspection has no native backend".to_owned())?
            .get_intent(&stage.object_id, &intent_id)
            .map_err(|error| error.to_string())
    })();
    let custody = serde_json::json!({
        "classification": "actual native duplicate DATA before strict acceptance; separate observation cuts",
        "command": command,
        "commandJsonBlake3": blake3::hash(&command_bytes).to_hex().to_string(),
        "originalAttachmentSize": attachment.len(),
        "originalAttachmentHash": attachment_digest,
        "originalAttachment": attachment.as_ref(),
        "capturedStageCommand": exchange.command,
        "capturedStageReply": exchange.response,
        "originalPendingIntent": original_pending,
        "actualDuplicateResponse": duplicate,
        "afterDuplicateIntent": retried_pending_observation,
    });
    let raw = serde_json::to_vec(&custody).expect("serialize actual typed duplicate custody");
    eprintln!(
        "SINOMO_NATIVE_PENDING_CREATE_DUPLICATE_RAW {}",
        String::from_utf8_lossy(&raw)
    );
    let output = cluster.root.join("pending-create-duplicate-custody.json");
    let saved = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)
        .and_then(|mut file| std::io::Write::write_all(&mut file, &raw));
    eprintln!("SINOMO_NATIVE_PENDING_CREATE_DUPLICATE_FILE {output:?}: {saved:?}");
    assert!(
        matches!(&duplicate.result,
        DfsDistributedWriteResult::Outcome {
            outcome: NetworkedDfsWriteOutcome::Pending { intent_id: actual, .. }
        } if actual == &intent_id),
        "the SAME complete request must remain Pending: {duplicate:?}"
    );
    cluster
        .writer
        .handle_distributed_write_response(cluster.primary_node.clone(), duplicate)
        .unwrap();
    let retried_pending = retried_pending_observation
        .unwrap()
        .expect("the SAME normal pending reservation survives response replay");
    assert_eq!(retried_pending, original_pending);
    assert!(
        retried_pending.touch_parent_on_create,
        "a response retry must preserve the original create's parent-touch obligation"
    );
    assert_eq!(serde_json::to_vec(&command).unwrap(), command_bytes);
    assert_eq!(content_hash(attachment.as_ref()), attachment_digest);
    assert!(
        Instant::now() < deadline,
        "the pending/retry phase exhausted its original window"
    );
    cluster._runtime.assert_fault_budgets().unwrap();
    let released = cluster._runtime.remove_faults_matching(|rule| {
        rule.source.as_ref() == Some(&cluster.primary_node)
            && rule.target.as_ref() == Some(&cluster.observer_node)
            && rule.route.as_deref() == Some(DFS_DATA_STAGE_BLOB_ROUTE)
            && rule.origin == Some(crate::test_runtime::RpcOrigin::RegisteredHost)
            && rule.call_id.is_none()
            && rule.remaining_hits <= 32
            && matches!(
                rule.action,
                crate::test_runtime::FaultAction::BudgetExhausted
            )
    });
    assert_eq!(
        released, 1,
        "release only this Pending phase's owned no-send rule"
    );
    // Only the real receiver resends its unchanged actual healthy reply through
    // ordinary native admission. No receipt, ACK, pending job or state is made up.
    assert!(
        cluster
            .observer
            .send_dfs_message_if_routable(&cluster.primary_node, exchange.response.message.clone(),)
            .expect("the real native receiver resends its exact captured Stage acknowledgement")
    );
    cluster.writer.process_incoming().unwrap();
    let current = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("the actual create commits");
    assert_eq!(current.object_id, command.request.object_id);
    assert_eq!(current.version, command.request.target_version);
    assert_eq!(
        current.content_hash.as_deref(),
        Some(stage.content_hash.as_str())
    );
    assert_eq!(
        cluster.writer.posix_read_file_distributed(path).unwrap(),
        bytes
    );
    let primary_parent = cluster
        .primary
        .posix_stat_distributed(parent)
        .expect("primary parent stat after retry");
    let writer_parent = cluster
        .writer
        .posix_stat_distributed(parent)
        .expect("writer parent stat after retry");
    assert!(
        writer_parent.modified_at_unix_nanos > writer_parent_before.modified_at_unix_nanos,
        "retried routed create must advance the parent directory mtime"
    );
    assert_eq!(writer_parent.version, primary_parent.version);
    assert_eq!(
        writer_parent.modified_at_unix_nanos,
        primary_parent.modified_at_unix_nanos
    );
    let kernel = {
        let mut owner = cluster._runtime.metadata_primary_service(path).unwrap();
        owner
            .metadata_backend()
            .unwrap()
            .prepared_operation_record(&format!("write:{intent_id}"))
            .unwrap()
            .expect("actual committed create retains its original unified kernel body")
    };
    kernel.validate().unwrap();
    assert_eq!(kernel.status, DfsPreparedOperationStatus::Committed);
    assert!(
        kernel
            .payload
            .terminal_intent
            .as_ref()
            .is_some_and(|record| record.intent_id == intent_id
                && record.touch_parent_on_create
                && record.target_version == current.version)
    );
    assert!(
        kernel
            .payload
            .transaction
            .objects
            .iter()
            .any(|object| object.path == parent
                && object.object_id == writer_parent.object_id
                && object.version == writer_parent.version
                && object.modified_at_unix_nanos == writer_parent.modified_at_unix_nanos)
    );
    assert_eq!(
        cluster
            .writer
            .posix_list_dir_distributed(parent)
            .unwrap()
            .iter()
            .filter(|entry| entry.name == "retried-parent-mtime.txt")
            .count(),
        1
    );
    assert!(
        Instant::now() < deadline,
        "actual completion exceeded the original single window"
    );
    cluster
        .writer
        .set_transport_operation_deadline(writer_deadline);
    cluster
        .observer
        .set_transport_operation_deadline(observer_deadline);
    let SifsPosixCluster {
        _runtime,
        primary,
        writer,
        observer,
        ..
    } = cluster;
    drop((primary, writer, observer));
    _runtime.close().unwrap();
}

#[test]
fn routed_attribute_results_observe_current_authority_after_mutation() {
    let mut cluster = sifs_posix_cluster("routed-current-attributes", "/users");
    let path = "/users/current-attributes";
    cluster
        .writer
        .posix_write_file(
            path,
            b"attribute read-your-write",
            DfsWriteOptions::replicated_eventual(vec![cluster.writer_node.clone()]),
        )
        .unwrap();
    // Prime this actual requester's existing local read view before the remote
    // mutation. A setter result must not reuse that older attribute generation.
    let before = cluster.writer.posix_stat(path).unwrap();
    assert_ne!(before.accessed_at_unix_nanos, 123_000_000_000);
    let times = cluster
        .writer
        .posix_set_times(path, Some(123_000_000_000), Some(456_000_000_000))
        .unwrap();
    let owner = cluster._runtime.metadata_primary_service(path).unwrap();
    let actual = owner.posix_stat(path).unwrap();
    assert_eq!(
        (actual.accessed_at_unix_nanos, actual.modified_at_unix_nanos),
        (123_000_000_000, 456_000_000_000)
    );
    assert_eq!(
        times, actual,
        "a successful routed setter must return its actual current author view"
    );
    drop(owner);
    let mode = cluster.writer.posix_set_mode(path, 0o641).unwrap();
    assert_eq!(mode.mode, 0o641);
    let link = "/users/current-attributes-link";
    cluster
        .writer
        .posix_symlink(
            path,
            link,
            DfsWriteOptions::replicated_eventual(vec![cluster.writer_node.clone()]),
        )
        .unwrap();
    let link_times = cluster
        .writer
        .posix_set_symlink_times(link, Some(789_000_000_000), Some(987_000_000_000))
        .unwrap();
    assert_eq!(link_times.kind, DfsObjectKind::Symlink);
    assert_eq!(
        (
            link_times.accessed_at_unix_nanos,
            link_times.modified_at_unix_nanos
        ),
        (789_000_000_000, 987_000_000_000)
    );
    let owner = cluster._runtime.metadata_primary_service(path).unwrap();
    let target = owner.posix_stat(path).unwrap();
    assert_eq!(
        (target.accessed_at_unix_nanos, target.modified_at_unix_nanos),
        (123_000_000_000, 456_000_000_000),
        "lstat semantics must leave the link target unchanged"
    );
    drop(owner);
    let SifsPosixCluster {
        primary,
        writer,
        observer,
        _runtime,
        ..
    } = cluster;
    drop((primary, writer, observer));
    _runtime.close().unwrap();
}

#[test]
fn sifs_cluster_posix_routed_namespace_ops_have_read_your_write_semantics() {
    // Namespace mutations route to the actual metadata primary. Public
    // read-your-write observes current authority without assuming that the
    // caller replays metadata or materializes content in its own cache.
    let cluster = sifs_posix_cluster("sifs-cluster-ns-ryw", "/users");
    let SifsPosixCluster {
        _runtime,
        root,
        primary_node,
        writer_node,
        observer_node,
        mut primary,
        mut writer,
        mut observer,
        rejections: _,
    } = cluster;

    // mkdir routes and is immediately visible through the current public view.
    let dir_stat = writer
        .posix_mkdir("/users/routed-dir")
        .expect("routed mkdir");
    assert_eq!(dir_stat.kind, DfsObjectKind::Directory);
    assert!(
        writer
            .posix_list_dir_distributed("/users")
            .expect("writer list /users")
            .iter()
            .any(|entry| entry.path == "/users/routed-dir"),
        "routed mkdir must be currently listed after it returns"
    );

    let attrs_dir = writer
        .posix_mkdir_with_create_attrs(
            "/users/routed-attrs",
            DfsPosixCreateAttrs::new(0o2750, 4101, 4201),
        )
        .expect("routed mkdir with attrs");
    assert_eq!(
        (attrs_dir.mode, attrs_dir.uid, attrs_dir.gid),
        (0o2750, 4101, 4201)
    );
    // Copy the genuine published parent BEFORE the later child mutations.
    // Public distributed observation does not materialize a full row in the
    // writer's client view. The ordinary owning Core supplies this immutable
    // old row; it remains private client DATA, not publication authority.
    let parent_scope = crate::select_metadata_shard_config_for_path(
        &sifs_file_system_id(),
        &_runtime
            .metadata_shards()
            .expect("actual parent assignment"),
        "/users/routed-attrs",
    )
    .expect("actual parent routing")
    .expect("assigned parent physical scope");
    assert_eq!(parent_scope.primary_node_id, primary_node);
    let parent_shard = DfsMetadataShardId::new(parent_scope.shard_id.clone());
    let (observer_parent, parent_cut_before) = {
        let owner = _runtime
            .metadata_primary_service("/users/routed-attrs")
            .expect("actual owning Core for the published parent");
        let parent = owner
            .metadata_backend()
            .expect("actual owning parent backend")
            .get_object_by_path(&sifs_file_system_id(), "/users/routed-attrs")
            .expect("actual parent physical lookup")
            .expect("actual committed parent row");
        assert_eq!(parent.file_system_id, attrs_dir.file_system_id);
        assert_eq!(parent.path, attrs_dir.path);
        assert_eq!(parent.object_id, attrs_dir.object_id);
        assert_eq!(parent.version, attrs_dir.version);
        assert_eq!(parent.kind, DfsObjectKind::Directory);
        assert_eq!(
            (parent.mode, parent.uid, parent.gid),
            (attrs_dir.mode, attrs_dir.uid, attrs_dir.gid)
        );
        let cut = owner
            .metadata_backend()
            .expect("same actual parent backend")
            .native_held_snapshot_for_scope(&parent_scope.file_system_id, &parent_shard)
            .expect("already-loaded actual parent scope");
        assert!(
            cut.tables.objects.contains(&parent),
            "the genuine parent row must belong to this same held Core cut"
        );
        drop(owner);
        (parent, cut)
    };
    assert!(
        observer.transport.delegates_local_namespace_authority(),
        "the copied row is client DATA; actual authority stays with Main"
    );
    observer
        .add_transient_object_metadata(observer_parent.clone())
        .expect("retain genuine old parent as private creator DATA");
    assert_eq!(
        observer.metadata().object(&observer_parent.object_id),
        Some(&observer_parent)
    );
    let attrs_child_dir = writer
        .posix_mkdir_with_create_attrs(
            "/users/routed-attrs/child",
            DfsPosixCreateAttrs::new(0o750, 4104, 4204),
        )
        .expect("routed child mkdir with attrs");
    assert_eq!(
        (
            attrs_child_dir.mode,
            attrs_child_dir.uid,
            attrs_child_dir.gid
        ),
        (0o2750, 4104, 4201)
    );
    let attrs_file = writer
        .posix_write_file_with_create_attrs(
            "/users/routed-attrs/file",
            b"attrs",
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
            DfsPosixCreateAttrs::new(0o640, 4102, 4202),
        )
        .expect("routed file with attrs");
    assert_eq!(
        (attrs_file.mode, attrs_file.uid, attrs_file.gid),
        (0o640, 4102, 4201)
    );
    // The public shell may retain an older read view. Inspect a NEW normal
    // owning Core after the real children return and bind every row to that
    // same loaded physical cut; no recovery, replay or synthetic row is used.
    let (current_parent, parent_cut_after) = {
        let owner = _runtime
            .metadata_primary_service("/users/routed-attrs")
            .expect("fresh actual owning Core after child mutations");
        let backend = owner
            .metadata_backend()
            .expect("fresh actual parent backend");
        let parent = backend
            .get_object_by_path(&sifs_file_system_id(), "/users/routed-attrs")
            .expect("fresh actual parent lookup")
            .expect("actual parent remains published");
        let child = backend
            .get_object_by_path(&sifs_file_system_id(), "/users/routed-attrs/child")
            .expect("fresh actual child lookup")
            .expect("actual child directory is published");
        let file = backend
            .get_object_by_path(&sifs_file_system_id(), "/users/routed-attrs/file")
            .expect("fresh actual file lookup")
            .expect("actual attributed file is published");
        assert_eq!(child.object_id, attrs_child_dir.object_id);
        assert_eq!(child.version, attrs_child_dir.version);
        assert_eq!(
            (child.mode, child.uid, child.gid),
            (
                attrs_child_dir.mode,
                attrs_child_dir.uid,
                attrs_child_dir.gid
            )
        );
        assert_eq!(file.object_id, attrs_file.object_id);
        assert_eq!(file.version, attrs_file.version);
        assert_eq!(
            (file.mode, file.uid, file.gid),
            (attrs_file.mode, attrs_file.uid, attrs_file.gid)
        );
        assert_eq!(file.content_hash, attrs_file.content_hash);
        let cut = backend
            .native_held_snapshot_for_scope(&parent_scope.file_system_id, &parent_shard)
            .expect("fresh already-loaded exact parent scope");
        for row in [&parent, &child, &file] {
            assert!(
                cut.tables.objects.contains(row),
                "each actual row must come from this same Core cut"
            );
        }
        drop(owner);
        (parent, cut)
    };
    assert_eq!(
        parent_cut_after.backend_root,
        parent_cut_before.backend_root
    );
    assert_eq!(parent_cut_after.shard_id, parent_cut_before.shard_id);
    assert!(
        parent_cut_after
            .certified_prefix
            .as_ref()
            .expect("actual after prefix")
            .journal_sequence
            > parent_cut_before
                .certified_prefix
                .as_ref()
                .expect("actual before prefix")
                .journal_sequence,
        "the physical cut must contain the later child publications"
    );
    assert_eq!(current_parent.object_id, observer_parent.object_id);
    assert_eq!(
        (current_parent.mode, current_parent.uid, current_parent.gid),
        (
            observer_parent.mode,
            observer_parent.uid,
            observer_parent.gid
        )
    );
    assert!(
        current_parent.version != observer_parent.version
            && current_parent.modified_at_unix_nanos > observer_parent.modified_at_unix_nanos
            && current_parent.changed_at_unix_nanos > observer_parent.changed_at_unix_nanos,
        "normal create parent-touch must advance the actual parent; before_prefix={:?} after_prefix={:?} old_parent={:?} actual_parent={:?}",
        parent_cut_before.certified_prefix,
        parent_cut_after.certified_prefix,
        observer_parent,
        current_parent
    );
    assert_eq!(
        observer.metadata().object(&observer_parent.object_id),
        Some(&observer_parent)
    );
    let raced_file_create = observer.posix_create_file_with_attrs(
        "/users/routed-attrs/file",
        DfsWriteOptions::replicated_eventual(vec![observer_node.clone()]),
        DfsPosixCreateAttrs::new(0o600, 5102, 5202),
    );
    assert!(
        matches!(raced_file_create, Err(DfsPosixError::AlreadyExists { .. })),
        "stale routed exclusive create must lose at the metadata primary: {raced_file_create:?}"
    );
    let parent_before_symlink = writer
        .posix_set_times("/users/routed-attrs", None, Some(123_000_000))
        .expect("set routed parent mtime before symlink")
        .modified_at_unix_nanos;
    let attrs_link = writer
        .posix_symlink_with_create_attrs(
            "file",
            "/users/routed-attrs/link",
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
            DfsPosixCreateAttrs::new(0o777, 4103, 4203),
        )
        .expect("routed symlink with attrs");
    assert_eq!(
        (attrs_link.mode, attrs_link.uid, attrs_link.gid),
        (0o777, 4103, 4201)
    );
    assert!(matches!(
        observer.posix_symlink_with_create_attrs(
            "other",
            "/users/routed-attrs/link",
            DfsWriteOptions::replicated_eventual(vec![observer_node]),
            DfsPosixCreateAttrs::new(0o777, 5103, 5203),
        ),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    let parent_after_symlink = writer
        .posix_stat_distributed("/users/routed-attrs")
        .expect("routed parent after symlink")
        .modified_at_unix_nanos;
    assert!(
        parent_after_symlink > parent_before_symlink,
        "routed symlink create must publish its parent mtime update"
    );

    // mkdir_all creates the whole chain through the primary.
    writer
        .posix_mkdir_all("/users/routed-dir/a/b")
        .expect("routed mkdir_all");
    assert_eq!(
        writer
            .posix_stat_distributed("/users/routed-dir/a/b")
            .expect("nested dir stat")
            .kind,
        DfsObjectKind::Directory
    );

    // Content write into the routed directory (already-routed write path).
    let bytes = b"namespace routed file content".to_vec();
    let file_stat = writer
        .posix_write_file(
            "/users/routed-dir/file.txt",
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .expect("routed content write");
    assert_eq!(
        file_stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert!(
        writer
            .posix_list_dir_distributed("/users/routed-dir")
            .expect("writer list routed dir after write")
            .iter()
            .any(|entry| entry.path == "/users/routed-dir/file.txt"),
        "routed content write must be currently listed after it returns"
    );

    // chmod / utimens route and read back their own effect.
    let mode_stat = writer
        .posix_set_mode("/users/routed-dir/file.txt", 0o600)
        .expect("routed set_mode");
    assert_eq!(mode_stat.mode, 0o600);
    assert_eq!(
        writer
            .posix_stat_distributed("/users/routed-dir/file.txt")
            .expect("stat after chmod")
            .mode,
        0o600
    );
    let times_stat = writer
        .posix_set_times(
            "/users/routed-dir/file.txt",
            Some(123_000_000_000),
            Some(456_000_000_000),
        )
        .expect("routed set_times");
    assert_eq!(times_stat.accessed_at_unix_nanos, 123_000_000_000);
    assert_eq!(times_stat.modified_at_unix_nanos, 456_000_000_000);

    // The renamed destination must immediately resolve to the committed
    // current bytes through normal authority and content reads.
    let renamed_stat = writer
        .posix_rename(
            "/users/routed-dir/file.txt",
            "/users/routed-dir/renamed.txt",
        )
        .expect("routed rename");
    assert_eq!(
        renamed_stat.content_hash.as_deref(),
        Some(content_hash(&bytes).as_str())
    );
    assert_eq!(
        writer
            .posix_read_file_distributed("/users/routed-dir/renamed.txt")
            .expect("read renamed"),
        bytes
    );
    assert!(matches!(
        writer.posix_stat_distributed("/users/routed-dir/file.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));

    // A canonical renamed file can be unlinked immediately after the caller
    // observes the rename result. This is the same shape as the cluster
    // read-your-write probe: write -> rename -> read -> unlink.
    let delete_bytes = b"rename then delete canonical file".to_vec();
    writer
        .posix_write_file(
            "/users/routed-dir/delete-source.txt",
            &delete_bytes,
            DfsWriteOptions::replicated_eventual(vec![writer_node.clone()]),
        )
        .expect("routed delete-source write");
    let delete_renamed = "/users/routed-dir/delete-renamed.txt";
    writer
        .posix_rename("/users/routed-dir/delete-source.txt", delete_renamed)
        .expect("routed rename before canonical unlink");
    assert_eq!(
        writer
            .posix_read_file_distributed(delete_renamed)
            .expect("read renamed before canonical unlink"),
        delete_bytes
    );
    writer
        .posix_unlink(delete_renamed)
        .expect("unlink canonical renamed file");
    assert!(matches!(
        writer.posix_stat_distributed(delete_renamed),
        Err(DfsPosixError::NotFound { .. })
    ));

    // hardlink routes and updates the link count everywhere it is observed.
    let link_stat = writer
        .posix_hardlink("/users/routed-dir/renamed.txt", "/users/routed-dir/link")
        .expect("routed hardlink");
    assert_eq!(link_stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(link_stat.link_count, 2);
    assert_eq!(
        writer
            .posix_stat_distributed("/users/routed-dir/renamed.txt")
            .expect("stat canonical after hardlink")
            .link_count,
        2
    );

    // unlink routes; the alias disappears and the link count drops.
    let unlinked = writer
        .posix_unlink("/users/routed-dir/link")
        .expect("routed unlink");
    assert_eq!(unlinked.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        writer.posix_stat_distributed("/users/routed-dir/link"),
        Err(DfsPosixError::NotFound { .. })
    ));
    let after_alias_unlink = writer
        .posix_stat_distributed("/users/routed-dir/renamed.txt")
        .expect("stat after alias unlink");
    let actual_primary = if after_alias_unlink.link_count != 1 {
        match _runtime.metadata_primary_service("/users/routed-dir/renamed.txt") {
            Ok(service) => format!("{:?}", service.posix_stat("/users/routed-dir/renamed.txt")),
            Err(error) => format!("unavailable: {error}"),
        }
    } else {
        String::new()
    };
    assert_eq!(
        after_alias_unlink.link_count, 1,
        "actual caller={after_alias_unlink:?}; actual primary={actual_primary}"
    );

    // POSIX error mapping survives the wire round trip.
    assert!(matches!(
        writer.posix_mkdir("/users/routed-dir"),
        Err(DfsPosixError::AlreadyExists { .. })
    ));
    assert!(matches!(
        writer.posix_rmdir("/users/routed-dir"),
        Err(DfsPosixError::DirectoryNotEmpty { .. })
    ));
    assert!(matches!(
        writer.posix_unlink("/users/routed-dir"),
        Err(DfsPosixError::IsDirectory { .. })
    ));
    assert!(matches!(
        writer.posix_unlink("/users/routed-missing.txt"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        writer.posix_rename("/users/routed-dir", "/users/routed-dir/inside"),
        Err(DfsPosixError::InvalidPath { .. })
    ));

    // Recursive clear routes, then the emptied directory can be removed.
    writer
        .posix_clear_dir_recursive("/users/routed-dir")
        .expect("routed recursive clear");
    assert!(
        writer
            .posix_list_dir_distributed("/users/routed-dir")
            .expect("list cleared dir")
            .is_empty(),
        "recursive clear must empty the directory on the requester"
    );
    writer
        .posix_rmdir("/users/routed-dir")
        .expect("routed rmdir after clear");
    assert!(matches!(
        writer.posix_stat_distributed("/users/routed-dir"),
        Err(DfsPosixError::NotFound { .. })
    ));

    assert!(
        writer.pending_routed_namespace_ops.is_empty(),
        "settled routed namespace ops must not leak pending state"
    );

    // The primary holds the same authoritative view.
    assert!(matches!(
        primary.posix_stat_distributed("/users/routed-dir"),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        primary
            .posix_stat_distributed("/users/routed-attrs")
            .expect("primary routed symlink parent")
            .modified_at_unix_nanos,
        parent_after_symlink,
        "requester and metadata primary must agree on the routed symlink parent generation"
    );

    // Journals stayed aligned: publication to both replicas applies cleanly.
    let mut rejections = Vec::new();
    let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
    loop {
        let publication = _runtime
            .maintenance_tick(
                &primary_node,
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
            )
            .expect("ordinary Main publication after routed namespace ops");
        assert!(publication.outcome.is_ok(), "{:?}", publication.outcome);
        assert_eq!(publication.omitted_kv_event_count, 0);
        pump_dfs_services_collect_rejections(
            &mut [
                ("primary", &mut primary),
                ("writer", &mut writer),
                ("observer", &mut observer),
            ],
            &mut rejections,
        );
        if matches!(
            observer.posix_stat("/users/routed-dir"),
            Err(DfsPosixError::NotFound { .. })
        ) && observer
            .posix_stat("/users/routed-dir/renamed.txt")
            .is_err()
        {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "observer did not converge after routed namespace ops; rejections: {rejections:?}"
            );
        }
        std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
    }
    assert!(
        rejections.is_empty(),
        "routed namespace ops must keep replica journals aligned: {rejections:?}"
    );

    drop(primary);
    drop(writer);
    drop(observer);
    _runtime
        .close()
        .expect("normal Main and native owner drain");
}

#[test]
fn sifs_cluster_posix_mkdir_all_remote_primary_falls_back_when_local_chain_exists() {
    let name = "native-mkdir-all-local-chain";
    let (runtime, mut primary, config) =
        crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
    let primary_node = config.primary_node_id;
    let observer = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
        .into_iter()
        .find(|node| *node != primary_node)
        .unwrap();
    let path = "/services/native-existing-chain";
    primary.posix_mkdir_all(path).unwrap();
    primary.posix_sync_directory(path).unwrap();
    let mut local = crate::runtime_host::open_dfs_runtime_service(
        runtime.data_dir(&observer),
        Some(&runtime.runtime_handle()),
    )
    .unwrap();
    local.hydrate_from_metadata_backend().unwrap();
    assert_eq!(
        local.posix_stat(path).unwrap().kind,
        DfsObjectKind::Directory
    );
    let shard = DfsMetadataShardId::new(config.shard_id);
    let before = primary
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&shard)
        .unwrap();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(observer.clone()),
        target: Some(primary_node),
        route: None,
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });
    local
        .posix_mkdir_all(path)
        .expect("the actual locally mirrored existing chain is idempotent");
    local
        .posix_mkdir_all_without_local_replay(path)
        .expect("the actual existing chain needs no replay or remote mutation");
    assert_eq!(
        primary
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&shard)
            .unwrap(),
        before
    );
    drop(local);
    drop(primary);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_routed_mkdir_all_allows_immediate_child_mkdir() {
    let cluster = sifs_two_primary_cluster("sifs-routed-mkdir-all-child");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        users_primary,
        services_primary,
        mut replica,
        rejections,
        ..
    } = cluster;
    assert!(
        rejections.is_empty(),
        "two-primary setup must not reject journal batches: {rejections:?}"
    );

    let base = "/services/cluster-tests";
    let child = "/services/cluster-tests/dir-race-local";
    replica
        .posix_mkdir_all(base)
        .expect("routed mkdir_all base");
    replica
        .posix_mkdir(child)
        .expect("immediate child mkdir sees routed mkdir_all parent");
    assert!(replica.posix_stat_distributed(base).is_ok());
    assert!(replica.posix_stat_distributed(child).is_ok());

    drop(users_primary);
    drop(services_primary);
    drop(replica);
    _runtime
        .close()
        .expect("normal Main and native owner drain");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_routed_lookup_under_split_prefix_uses_exact_path_primary() {
    let cluster = sifs_two_primary_cluster("sifs-routed-lookup-split-prefix");
    let SifsTwoPrimaryCluster {
        shared_kv,
        users_primary_node,
        _runtime,
        root,
        mut services_primary,
        mut replica,
        services_primary_node,
        rejections,
        ..
    } = cluster;
    assert!(
        rejections.is_empty(),
        "two-primary setup must not reject journal batches: {rejections:?}"
    );

    let dir = "/services/cluster-tests/latency-regression";
    let path = "/services/cluster-tests/latency-regression/tiny.txt";
    let bytes = b"split-prefix routed lookup".to_vec();
    services_primary
        .posix_mkdir_all(dir)
        .expect("services primary mkdir_all dir");
    let mut options = sifs_cluster_write_options(&services_primary_node);
    options.wait_for_commit = true;
    services_primary
        .posix_write_file(path, &bytes, options)
        .expect("services primary write file");

    crate::posix::reset_posix_metadata_raw_lookup_count();
    let stat = replica
        .posix_stat_distributed(path)
        .expect("distributed stat follows exact path primary");
    let component_count = path.trim_matches('/').split('/').count();
    let stat_lookup_count = crate::posix::posix_metadata_raw_lookup_count();
    let stat_lookup_bound = component_count + 4;
    assert!(
        stat_lookup_count <= stat_lookup_bound,
        "distributed stat performed {stat_lookup_count} raw metadata lookups for {component_count} path components; expected no more than {stat_lookup_bound} including root and fixed authority checks"
    );
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    crate::posix::reset_posix_metadata_raw_lookup_count();
    crate::service_read::reset_remote_metadata_range_read_request_count();
    let prefix = replica
        .posix_read_file_prefix_distributed(path, bytes.len())
        .expect("distributed prefix read after exact-primary stat");
    assert_eq!(prefix.bytes, bytes);
    assert!(!prefix.truncated);
    assert_eq!(
        crate::service_read::remote_metadata_range_read_request_count(),
        0,
        "the authoritative lookup should return a small local range without a second content RPC"
    );
    let read_lookup_count = crate::posix::posix_metadata_raw_lookup_count();
    let read_lookup_bound = component_count + 6;
    assert!(
        read_lookup_count <= read_lookup_bound,
        "distributed read performed {read_lookup_count} raw metadata lookups for {component_count} path components; expected no more than {read_lookup_bound} including root and fixed authority/content checks"
    );

    assert_eq!(
        services_primary
            .posix_stat_distributed(path)
            .expect("services primary current stat")
            .size_bytes,
        Some(bytes.len() as u64)
    );
    drop(services_primary);
    drop(replica);
    _runtime
        .close()
        .expect("normal Main and native owner drain");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_routed_child_create_rejects_parent_removed_on_primary() {
    exercise_routed_child_create_with_removed_parent(true);
}

#[test]
fn routed_create_with_foreground_repair_disabled_rejects_parent_removed_on_primary() {
    exercise_routed_child_create_with_removed_parent(false);
}

fn exercise_routed_child_create_with_removed_parent(foreground_replica_repair: bool) {
    let mut cluster = sifs_two_primary_cluster(&format!(
        "sifs-routed-create-removed-parent-{foreground_replica_repair}"
    ));
    let fs = sifs_file_system_id();
    let dir = "/services/cluster-tests/stale-parent";
    let child = "/services/cluster-tests/stale-parent/child.txt";
    cluster
        .services_primary
        .posix_mkdir_all(dir)
        .expect("services primary creates parent");
    let parent_scope = crate::select_metadata_shard_config_for_path(
        &fs,
        &cluster
            ._runtime
            .metadata_shards()
            .expect("actual parent topology"),
        dir,
    )
    .expect("actual parent routing")
    .expect("assigned current parent scope");
    assert_eq!(parent_scope.primary_node_id, cluster.services_primary_node);
    // The actual old full row is retained only as a private caller hint.
    let old_parent = {
        let owner = cluster
            ._runtime
            .metadata_primary_service(dir)
            .expect("actual owning Core before parent removal");
        let row = owner
            .metadata_backend()
            .expect("actual parent backend")
            .get_object_by_path(&fs, dir)
            .expect("actual old parent lookup")
            .expect("actual published parent directory");
        assert_eq!(row.file_system_id, fs);
        assert_eq!(row.path, dir);
        assert_eq!(row.kind, DfsObjectKind::Directory);
        drop(owner);
        row
    };
    cluster
        .services_primary
        .posix_rmdir(dir)
        .expect("services primary removes parent");
    let deleted_parent = {
        let owner = cluster
            ._runtime
            .metadata_primary_service(dir)
            .expect("fresh owning Core after parent removal");
        let backend = owner
            .metadata_backend()
            .expect("actual removed-parent backend");
        let row = backend
            .get_object(&old_parent.object_id)
            .expect("actual removed parent identity")
            .expect("actual retained tombstone");
        assert_eq!(row.kind, DfsObjectKind::Tombstone);
        assert_ne!(row.version, old_parent.version);
        assert!(
            backend
                .get_object_by_path(&fs, child)
                .expect("actual child absence")
                .is_none()
        );
        drop(owner);
        row
    };
    assert!(
        cluster
            .replica
            .transport
            .delegates_local_namespace_authority()
    );
    cluster
        .replica
        .add_transient_object_metadata(old_parent.clone())
        .expect("retain genuine old parent as caller DATA");
    assert_eq!(
        cluster.replica.metadata().object(&old_parent.object_id),
        Some(&old_parent)
    );

    // The capture observes the actual registered Main, not a public shell's
    // inbox. One existing hook sees both Namespace and Write invocations.
    let capture = cluster
        ._runtime
        .capture_served_work(&cluster.services_primary_node, 32)
        .expect("actual current-primary Main invocation capture");
    let browser_before = capture.records();
    crate::browser_service::prepare_sifs_browser_write_parent(&mut cluster.replica, child)
        .expect("genuine cached parent skips only redundant recursive creation");
    let browser_after = capture.records();
    let mkdir_for_cached_parent = |work: &crate::test_runtime::CapturedServedMessageWork| {
        work.command.routed_message.source_node_id == cluster.replica_node
            && matches!(&work.command.routed_message.message,
                DfsNetworkMessage::NamespaceRequest(request)
                    if matches!(&request.operation, DfsNamespaceOperation::MkdirAll { path } if path == dir))
    };
    assert_eq!(
        browser_after
            .iter()
            .filter(|work| mkdir_for_cached_parent(work))
            .count(),
        browser_before
            .iter()
            .filter(|work| mkdir_for_cached_parent(work))
            .count(),
        "a cached parent hint must not execute a redundant Main MkdirAll"
    );

    let mut options = sifs_cluster_write_options(&cluster.replica_node.clone());
    options.foreground_replica_repair = foreground_replica_repair;
    if !foreground_replica_repair {
        options.wait_for_commit = true;
    }
    let create = cluster.replica.posix_write_file(child, b"child", options);
    let reports = capture.records();
    let matching_write = reports.iter().find_map(|work| {
        if work.command.routed_message.source_node_id != cluster.replica_node
            || work.command.routed_message.target_node_id != cluster.services_primary_node
        {
            return None;
        }
        let DfsNetworkMessage::DistributedWriteRequest(command) =
            &work.command.routed_message.message
        else {
            return None;
        };
        command
            .object_metadata
            .as_ref()
            .filter(|object| object.path == child)
            .map(|_| (work, command))
    });
    let authenticated_parent_lookup = reports.iter().any(|work| {
        work.command.routed_message.source_node_id == cluster.replica_node
            && work.command.routed_message.target_node_id == cluster.services_primary_node
            && matches!(&work.command.routed_message.message,
                DfsNetworkMessage::NamespaceRequest(request)
                    if request.requesting_node_id == cluster.replica_node && request.file_system_id == fs
                        && request.authority_shards.contains(&parent_scope)
                        && (matches!(&request.operation, DfsNamespaceOperation::LookupPath { path, .. } if path == dir)
                            || matches!(&request.operation, DfsNamespaceOperation::LookupPaths { paths } if paths.iter().any(|path| path == dir))))
    });
    assert!(
        authenticated_parent_lookup || matching_write.is_some(),
        "the actual current primary must validate this caller through a native lookup or write"
    );
    if let Some((work, command)) = matching_write {
        assert_eq!(command.request.requesting_node_id, cluster.replica_node);
        assert_eq!(command.request.file_system_id, fs);
        assert_eq!(command.foreground_replica_repair, foreground_replica_repair);
        assert!(
            !command.requester_blob_pre_staged,
            "normal remote-primary creation streams bytes for either repair flag"
        );
        assert_eq!(command.authority_shard.as_ref(), Some(&parent_scope));
        assert!(
            !work.committed_work.publish_metadata && !work.committed_work.semantic_changes,
            "the rejected create must not publish a namespace mutation"
        );
    }
    let expected_rejection = DfsError::Distributed {
        message: format!("parent directory does not exist: {dir}"),
    }
    .to_string();
    let strict_rejection = |error: &DfsError| matches!(error, DfsError::Distributed { message } if message == &expected_rejection);
    match &create {
        Err(DfsPosixError::MissingParent { path }) | Err(DfsPosixError::NotFound { path }) => {
            assert!(
                path == dir || path == child,
                "rejection must name this exact parent/create"
            );
        }
        Err(DfsPosixError::Dfs(error)) => assert!(
            strict_rejection(error),
            "unexpected native rejection: {error:?}"
        ),
        Err(DfsPosixError::PendingWrite { .. }) => {
            let (_, command) =
                matching_write.expect("pending create must have an actual Main write invocation");
            let deadline = Instant::now() + SIFS_CLUSTER_PUMP_DEADLINE;
            loop {
                match cluster.replica.write_outcome(&command.request_id) {
                    Err(error) => {
                        assert!(
                            strict_rejection(&error),
                            "unexpected routed rejection: {error:?}"
                        );
                        break;
                    }
                    Ok(NetworkedDfsWriteOutcome::Pending { .. }) => {}
                    Ok(outcome) => {
                        panic!("removed-parent create unexpectedly settled: {outcome:?}")
                    }
                }
                cluster
                    .replica
                    .process_incoming()
                    .expect("normal requester response processing");
                assert!(
                    Instant::now() < deadline,
                    "removed-parent create did not settle"
                );
                std::thread::sleep(SIFS_CLUSTER_PUMP_IDLE_SLEEP);
            }
        }
        other => panic!("removed-parent create must have the exact typed rejection: {other:?}"),
    }

    // A rejected native create must leave neither a child row nor a canonically
    // bound object version. Stream/quarantine bytes alone are not publication.
    let owner = cluster
        ._runtime
        .metadata_primary_service(dir)
        .expect("fresh current-primary no-orphan inspection");
    let backend = owner
        .metadata_backend()
        .expect("actual final parent backend");
    assert_eq!(
        backend
            .get_object(&old_parent.object_id)
            .expect("final removed parent"),
        Some(deleted_parent)
    );
    assert!(
        backend
            .get_object_by_path(&fs, child)
            .expect("actual final child lookup")
            .is_none()
    );
    assert!(
        cluster
            .replica
            .metadata()
            .object_by_normalized_posix_path(&fs, child)
            .is_none(),
        "the failed create must clear its transient child DATA"
    );
    if let Some((_, command)) = matching_write {
        for node in [&cluster.replica_node, &cluster.services_primary_node] {
            let store = crate::runtime_host::open_dfs_runtime_content_store(
                cluster._runtime.data_dir(node),
            )
            .expect("actual native content inspection");
            assert!(
                !store
                    .object_version_is_deleted(
                        command.request.object_id.as_str(),
                        command.request.target_version.as_str()
                    )
                    .expect("actual new rejected tuple retirement state")
            );
            assert!(
                matches!(store.read_manifest(
                command.request.object_id.as_str(), command.request.target_version.as_str()),
                Err(ContentStoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound),
                "a rejected create must have no live canonical object-version binding"
            );
        }
    }
    drop(owner);
    cluster
        .replica
        .remove_transient_object_metadata(&old_parent.object_id);
    assert!(
        !cluster
            .replica
            .transient_metadata_object_ids
            .contains(&old_parent.object_id)
    );
    assert!(cluster.replica.pending_routed_namespace_ops.is_empty());
    drop(capture);
    cluster.close();
}

#[test]
fn sifs_cluster_bulk_local_eventual_remote_primary_large_batch_is_fully_visible() {
    let mut cluster = sifs_two_primary_cluster("sifs-bulk-remote-primary-large");
    let remote_dir = "/services/bulk-remote-large";
    cluster
        .services_primary
        .posix_mkdir_all(remote_dir)
        .expect("remote bulk parent");
    sifs_two_primary_publish_until(&mut cluster, "remote bulk parent", |cluster| {
        cluster.users_primary.posix_stat(remote_dir).is_ok()
            && cluster.replica.posix_stat(remote_dir).is_ok()
    });

    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = cluster;

    // Read from the actual /users shard owner. The /services corpus is still
    // remote, so bulk reads and every distributed stat cross the real carrier.
    drop(users_primary);
    let mut users_primary = _runtime.metadata_primary_service("/users").unwrap();
    let batch = (0..64)
        .map(|index| {
            (
                format!("{remote_dir}/bulk-{index:06}.bin"),
                format!("bulk remote primary payload {index:06} ")
                    .repeat(128)
                    .into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let stats = replica
        .posix_write_files_local_eventual_with_known_parents(
            batch
                .iter()
                .map(|(path, bytes)| (path.as_str(), bytes.as_slice())),
        )
        .expect("remote-primary bulk write");
    assert_eq!(stats.len(), batch.len());

    let read_paths = batch
        .iter()
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let file_system_id = sifs_file_system_id();
    for (path, _) in &batch {
        assert!(
            users_primary
                .metadata()
                .object_by_normalized_posix_path(&file_system_id, path)
                .is_none(),
            "reader unexpectedly had remote-primary metadata before bulk read for {path}"
        );
    }
    let read_results = users_primary.posix_read_files_distributed(&read_paths);
    for ((path, bytes), result) in batch.iter().zip(read_results) {
        assert_eq!(
            result.expect("bulk remote read"),
            *bytes,
            "remote primary bulk read returned wrong bytes for {path}"
        );
    }
    for (path, _) in &batch {
        assert!(
            users_primary
                .metadata()
                .object_by_normalized_posix_path(&file_system_id, path)
                .is_none(),
            "bulk read should not materialize remote-primary metadata on the reader for {path}"
        );
    }

    for (path, bytes) in &batch {
        assert_eq!(
            users_primary
                .posix_stat_distributed(path)
                .expect("remote-primary stat through primary")
                .content_hash
                .as_deref(),
            Some(content_hash(bytes).as_str()),
            "missing remote-primary bulk path {path}"
        );
    }

    // The final check is the authoritative local view, after the unchanged
    // 64-file remote write/read/stat workload above has exercised Main routing.
    let author = _runtime.metadata_primary_service(remote_dir).unwrap();
    for (path, bytes) in &batch {
        assert_eq!(
            author
                .posix_stat(path)
                .expect("remote primary local stat")
                .content_hash
                .as_deref(),
            Some(content_hash(bytes).as_str()),
            "remote primary did not materialize {path}"
        );
    }

    drop(author);
    assert_eq!(_runtime.observation_dropped_count(), 0);
    drop(users_primary);
    drop(services_primary);
    drop(replica);
    _runtime
        .close()
        .expect("normal Main and native owner drain");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_remote_primary_deferred_publish_write_persists_for_fresh_replica_reader() {
    let cluster = sifs_two_primary_cluster("sifs-remote-primary-deferred-fresh");
    let SifsTwoPrimaryCluster {
        shared_kv,
        _runtime,
        root,
        users_primary_node,
        services_primary_node,
        replica_node,
        services_primary,
        mut replica,
        ..
    } = cluster;

    let path = "/services/fresh-replica-writer.bin";
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
        replica_candidates: vec![replica_node.clone(), services_primary_node.clone()],
        wait_for_commit: false,
        foreground_replica_repair: false,
    };
    let seed = vec![b'A'; 4096];
    let writer_a = vec![b'B'; 4096];
    let writer_b = vec![b'C'; 4096];

    replica
        .posix_write_file(path, &seed, options.clone())
        .expect("seed remote-primary file");
    replica
        .process_incoming_until_pending_routed_writes_ready(Duration::from_secs(1))
        .expect("seed response replay");
    replica
        .posix_write_file(path, &writer_a, options.clone())
        .expect("first remote-primary overwrite");
    replica
        .process_incoming_until_pending_routed_writes_ready(Duration::from_secs(1))
        .expect("first overwrite response replay");
    replica
        .posix_write_file(path, &writer_b, options)
        .expect("second remote-primary overwrite");
    replica
        .process_incoming_until_pending_routed_writes_ready(Duration::from_secs(1))
        .expect("second overwrite response replay");

    drop(replica);

    let actual_configs = _runtime
        .metadata_shards()
        .expect("actual committed assignments");
    let reopened = open_local_sharded_metadata_backend(
        default_dfs_metadata_shards_root(_runtime.data_dir(&replica_node)),
        actual_configs,
    )
    .expect("reopen this runtime's actual replica metadata");
    let stored = reopened
        .get_object_by_path(&sifs_file_system_id(), path)
        .expect("fresh metadata lookup")
        .expect("fresh metadata object");
    assert_eq!(
        stored.content_hash.as_deref(),
        Some(content_hash(&writer_b).as_str()),
        "fresh replica metadata must persist the committed remote-primary write, not the seed"
    );

    drop(reopened);
    drop(services_primary);
    _runtime
        .close()
        .expect("normal Main and native owner drain");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sifs_cluster_posix_services_routed_rename_then_unlink_is_read_your_write() {
    let cluster = sifs_two_primary_cluster("sifs-services-rename-unlink-ryw");
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        users_primary,
        mut services_primary,
        mut replica,
        replica_node,
        rejections,
        ..
    } = cluster;
    assert!(
        rejections.is_empty(),
        "two-primary setup must not reject journal batches: {rejections:?}"
    );

    let base = "/services/cluster-tests";
    let path = "/services/cluster-tests/ryw.txt";
    let renamed = "/services/cluster-tests/ryw-renamed.txt";
    let bytes = b"services routed rename unlink read-your-write".to_vec();

    replica
        .posix_mkdir_all(base)
        .expect("routed mkdir_all base");
    replica
        .posix_write_file(
            path,
            &bytes,
            DfsWriteOptions::replicated_eventual(vec![replica_node.clone()]),
        )
        .expect("routed services write");
    assert_eq!(
        replica
            .posix_read_file_distributed(path)
            .expect("read before rename"),
        bytes
    );

    replica
        .posix_rename(path, renamed)
        .expect("routed services rename");
    assert_eq!(
        replica
            .posix_read_file_distributed(renamed)
            .expect("read renamed before unlink"),
        bytes
    );
    assert!(matches!(
        replica.posix_stat_distributed(path),
        Err(DfsPosixError::NotFound { .. })
    ));

    let unlinked = replica
        .posix_unlink(renamed)
        .expect("unlink routed renamed services file");
    assert_eq!(unlinked.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        replica.posix_stat_distributed(renamed),
        Err(DfsPosixError::NotFound { .. })
    ));

    assert!(matches!(
        services_primary.posix_stat_distributed(renamed),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(users_primary);
    drop(services_primary);
    drop(replica);
    _runtime
        .close()
        .expect("normal Main and native owner drain");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn late_routed_responses_are_discarded_without_failing_the_event_loop() {
    let (runtime, mut client, request, _, _, _, operation, actual) =
        native_pending_routed_write("native-late-routed-response");
    let primary = client.writes.pending_routed_writes[&operation]
        .primary_node_id
        .clone();
    let mut untracked = actual.clone();
    untracked.request_id = sinomo_core::Id::generate().to_string();
    assert!(matches!(
        client
            .handle_distributed_write_response(primary.clone(), untracked)
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    assert!(matches!(
        client
            .handle_namespace_response(
                primary.clone(),
                DfsNamespaceResponse {
                    request_id: sinomo_core::Id::generate().to_string(),
                    committed_work: Default::default(),
                    result: DfsNamespaceResult::Applied,
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_batches: Vec::new(),
                    metadata_journal_has_more: false,
                    posix_coordination_result: None,
                }
            )
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    let pending = client.writes.pending_routed_writes[&operation].clone();
    let wrong_source = client.node_id().clone();
    assert_ne!(wrong_source, primary);
    assert!(matches!(
        client
            .handle_distributed_write_response(wrong_source, actual.clone())
            .unwrap(),
        NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. }
    ));
    assert_eq!(client.writes.pending_routed_writes[&operation], pending);
    let mut rejected = actual;
    let path = pending
        .command
        .as_ref()
        .unwrap()
        .object_metadata
        .as_ref()
        .unwrap()
        .path
        .clone();
    rejected.result = DfsDistributedWriteResult::Rejected {
        reason: format!("path {path} is moving between metadata shards; retry the operation"),
        current_primary_node_id: None,
    };
    assert_eq!(rejected.object_id, request.object_id);
    client
        .handle_distributed_write_response(primary, rejected)
        .unwrap();
    assert!(
        matches!(client.write_outcome(&operation), Err(DfsError::Busy { path: busy }) if busy == path)
    );
    drop(client);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn sifs_non_replica_routed_create_is_listed_after_blocking_write() {
    let cluster = sifs_outsider_cluster("sifs-outsider-create-list-ryw");
    let SifsPosixCluster {
        root,
        primary,
        observer_node,
        mut observer,
        _runtime,
        ..
    } = cluster;

    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        let mut primary = primary;
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    let dir = "/users/outsider-list";
    let path = "/users/outsider-list/tiny.txt";
    let bytes = b"tiny routed outsider create".to_vec();

    observer
        .posix_mkdir_all(dir)
        .expect("routed mkdir from non-replica requester");
    let mut options = DfsWriteOptions::replicated_eventual(vec![observer_node.clone()]);
    options.foreground_replica_repair = false;
    observer
        .posix_write_file(path, &bytes, options)
        .expect("committed routed create from non-replica requester");

    assert_eq!(
        observer
            .posix_read_file_distributed(path)
            .expect("outsider reads its own routed create from the primary"),
        bytes
    );
    let entries = observer
        .posix_list_dir_distributed(dir)
        .expect("outsider lists directory after routed create");
    assert!(
        entries.iter().any(|entry| entry.path == path),
        "committed routed create must be visible to an immediate distributed list: {entries:?}"
    );

    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    drop(_runtime);
}

#[test]
fn sifs_cross_primary_dot_paths_route_to_the_resolved_namespace() {
    let SifsTwoPrimaryCluster {
        shared_kv,
        services_primary_node,
        users_primary_node,
        _runtime,
        root,
        replica_node,
        users_primary,
        services_primary,
        mut replica,
        ..
    } = sifs_two_primary_cluster("sifs-dot-component-routing");
    let result = (|| -> Result<_, DfsPosixError> {
        let options = DfsWriteOptions::replicated_eventual(vec![replica_node]);
        replica.posix_mkdir("/services/sub")?;
        replica.posix_symlink("/services/sub", "/users/link", options.clone())?;
        replica.posix_write_file("/users/victim", b"must survive", options.clone())?;
        replica.posix_write_file("/services/victim", b"resolved target", options)?;
        let read = replica.posix_read_file_distributed("/users/link/../victim")?;
        replica.posix_rename("/users/link/../victim", "/users/link/../renamed")?;
        replica.posix_hardlink("/users/link/../renamed", "/users/link/../alias")?;
        replica.posix_unlink("/users/link/../alias")?;
        replica.posix_unlink("/users/link/../renamed")?;
        let survivor = replica.posix_read_file_distributed("/users/victim")?;
        Ok((read, survivor))
    })();
    let (read, survivor) = result.expect("dot paths resolve before shard admission");
    assert_eq!(read, b"resolved target");
    assert_eq!(survivor, b"must survive");
    drop(users_primary);
    drop(services_primary);
    drop(replica);
    _runtime
        .close()
        .expect("normal embedded DFS fixture shutdown");
}

#[test]
fn native_routed_stale_unlink_preserves_typed_conflict_and_actual_winner() {
    let mut cluster = sifs_two_primary_cluster("native-routed-stale-unlink");
    let path = "/services/stale-caller";
    let first = cluster
        .replica
        .posix_write_file(
            path,
            b"first generation",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    let winner = cluster
        .services_primary
        .posix_write_file(
            path,
            b"actual concurrent winner",
            DfsWriteOptions::replicated_eventual(vec![cluster.services_primary_node.clone()]),
        )
        .unwrap();
    let error = cluster
        .replica
        .posix_unlink_if_generation(
            path,
            sinomo_core::Id::generate().to_string(),
            DfsNamespaceObjectPrecondition {
                object_id: first.object_id,
                version: first.version,
            },
        )
        .unwrap_err();
    assert!(
        matches!(error, DfsPosixError::Conflict { ref path, .. } if path == "/services/stale-caller")
    );
    assert_eq!(
        cluster.replica.posix_stat(path).unwrap().version,
        winner.version
    );
    assert_eq!(
        cluster.replica.posix_read_file_distributed(path).unwrap(),
        b"actual concurrent winner"
    );
    cluster.close();
}

#[test]
fn native_routed_stale_rename_cannot_replace_a_current_generation() {
    let mut cluster = sifs_two_primary_cluster("native-routed-stale-rename");
    let from = "/services/stale-rename-source";
    let to = "/services/stale-rename-target";
    let first = cluster
        .replica
        .posix_write_file(
            from,
            b"first generation",
            DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]),
        )
        .unwrap();
    let winner = cluster
        .services_primary
        .posix_write_file(
            from,
            b"winning generation",
            DfsWriteOptions::replicated_eventual(vec![cluster.services_primary_node.clone()]),
        )
        .unwrap();
    let error = cluster
        .replica
        .posix_rename_if_generation(
            from,
            to,
            sinomo_core::Id::generate().to_string(),
            DfsNamespaceObjectPrecondition {
                object_id: first.object_id,
                version: first.version,
            },
            DfsNamespaceDestinationPrecondition::Absent,
        )
        .unwrap_err();
    assert!(
        matches!(error, DfsPosixError::Conflict { ref path, .. } if path == "/services/stale-rename-source")
    );
    assert_eq!(
        cluster.replica.posix_stat(from).unwrap().version,
        winner.version
    );
    assert!(matches!(
        cluster.replica.posix_stat(to),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        cluster.replica.posix_read_file_distributed(from).unwrap(),
        b"winning generation"
    );
    cluster.close();
}
