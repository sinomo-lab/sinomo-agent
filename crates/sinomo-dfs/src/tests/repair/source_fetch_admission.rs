//! The executor's fetch-first repair and fetch admission. The fixture leaves
//! a placement need unrepaired, with the content on one holder only: the
//! primary fetches it before it can send the replica. A fetch is served only
//! to a caller with the intent's permission.

use super::*;

struct NativeSourceFetch {
    holder: NodeId,
    target: NodeId,
    shard: DfsMetadataShardConfig,
    stat: DfsStat,
    intent: String,
    execution: crate::publication_guard::DfsPublicationContextRestore,
    core: NetworkedDfsService<DfsNodeTransport>,
    caller: NetworkedDfsService<DfsNodeTransport>,
    holds: Vec<crate::runtime_host::DaemonDfsPreDispatchTestHookGuard>,
    native: crate::test_runtime::DfsTestRuntime,
}

impl NativeSourceFetch {
    fn new(name: &str) -> Self {
        let path = format!("/services/{name}.bin");
        let (native, initial, original) =
            crate::test_support::current_native_metadata_service(name, 3, &path).unwrap();
        drop(initial);
        let target = original.primary_node_id.clone();
        let primary = original.replica_node_ids[0].clone();
        let holder = original.replica_node_ids[1].clone();
        let shard = native
            .reassign_metadata_shard(
                &original.file_system_id,
                &original.shard_id,
                primary.clone(),
                vec![target.clone(), holder.clone()],
            )
            .unwrap();
        let bytes = vec![0x6b; 4096];
        let mut writer = native.client(&holder).unwrap();
        let mut initial_policy = DfsReplicationPolicy::new(1, 1).unwrap();
        initial_policy.required_node_ids = vec![holder.clone()];
        let stat = writer
            .posix_write_file(
                &path,
                &bytes,
                DfsWriteOptions {
                    policy: initial_policy,
                    replica_candidates: vec![holder.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        assert_eq!(writer.posix_read_file_distributed(&path).unwrap(), bytes);
        drop(writer);
        // Keep repair claims parked while the normal configuration owner
        // observes Apply. A resident-preflight hold here would also prevent
        // that owner from making the newly published job visible.
        let background = [&primary, &target]
            .into_iter()
            .map(|node| native.deny_background_replica_before_claim(node).unwrap())
            .collect::<Vec<_>>();
        // Fetch precedes a repair claim. Preserve the absent-primary-content
        // prerequisite through real failed sends while ordinary owners catch up.
        native.install_fault(crate::test_runtime::FaultRule {
            source: Some(primary.clone()),
            target: Some(holder.clone()),
            route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
            call_id: None,
            origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
        });
        let mut placement =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
        placement.required_node_ids = vec![target.clone()];
        let accepted = native
            .apply_placement_policy(&primary, &path, DfsPlacementScope::SelfOnly, placement)
            .unwrap();
        let mut publisher = native.configuration_publisher(&primary);
        let published = DfsControlPlaneStore::new(publisher.as_mut())
            .placement_reconciliation(&stat.file_system_id, &accepted.job_id)
            .unwrap()
            .expect("ordinary Apply stored its actual authoritative placement job");
        assert_eq!(published.accepted_config, accepted.accepted_config);
        assert_eq!(published.candidate_snapshot, accepted.candidate_snapshot);
        drop(publisher);
        // Reopen through normal startup completion after the actual publication,
        // as the placement producer requires. No projection or job is seeded.
        let mut caller = native.client(&target).unwrap();
        let mut core = native.metadata_primary_service(&path).unwrap();
        let retained = DfsControlPlaneStore::new(core.control_plane.as_mut().unwrap().as_mut())
            .placement_reconciliation(&stat.file_system_id, &accepted.job_id)
            .unwrap()
            .expect("the ordinary current owner observes the published placement job");
        assert_eq!(retained.accepted_config, accepted.accepted_config);
        assert_eq!(retained.candidate_snapshot, accepted.candidate_snapshot);
        let mut policy = DfsReplicationPolicy::new(1, 1).unwrap();
        policy.required_node_ids = vec![target.clone()];
        // Use the actual accepted policy and the ordinary encoded placement
        // producer, rather than seeding a repair row or a replica receipt.
        caller
            .route_namespace_operation(
                primary.clone(),
                DfsNamespaceOperation::ReconcilePlacement {
                    job_id: accepted.job_id,
                    path: path.clone(),
                    object_id: stat.object_id.clone(),
                    expected_version: stat.version.clone(),
                    object_policy: DfsObjectPolicy::ReplicatedEventual,
                    replication_policy: policy,
                    desired_replica_node_ids: vec![target.clone()],
                },
            )
            .unwrap();
        let execution = native_fixture_write_admission(&native, &core, &path);
        // The admitted Core now excludes competing Main work while replacing
        // the narrow setup holds. All real dispatch and receiver guards remain.
        drop(background);
        let holds = [&primary, &target].into_iter().map(|node| {
            crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
                native.data_dir(node), Arc::new(|point, command, _| {
                    if matches!(point, crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight)
                        && command.is_none() {
                        return Err(DfsError::Cancelled { operation: "private native source-fetch maintenance turn" });
                    }
                    Ok(())
                }),
            )
        }).collect::<Vec<_>>();
        native.remove_faults_matching(|rule| {
            rule.source.as_ref() == Some(&primary)
                && rule.target.as_ref() == Some(&holder)
                && rule.route.as_deref() == Some(DFS_DATA_FETCH_BLOB_ROUTE)
                && rule.call_id.is_none()
                && rule.origin == Some(crate::test_runtime::RpcOrigin::RegisteredHost)
                && matches!(
                    &rule.action,
                    crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch
                )
        });
        core.metadata_backend
            .as_mut()
            .unwrap()
            .refresh_metadata_shard_attestation_view(&DfsMetadataShardId::new(&shard.shard_id))
            .unwrap();
        let object = core.current_read_object_metadata(&stat.object_id).unwrap();
        // The source must know the real committed placement intent before
        // exercising its intent-permission checks. Use normal native prefix
        // synchronization, with actual receiver acknowledgements only.
        let metadata_target = core.capture_metadata_quorum_sync_target(&object).unwrap();
        let metadata_sync = core
            .sync_metadata_shard_to_quorum(
                &metadata_target,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap();
        assert!(metadata_sync.acknowledged_node_ids.contains(&holder));
        assert_eq!(object.current_locations, vec![holder.clone()]);
        assert!(
            core.local_object_version_manifest_size_if_matches(
                &stat.object_id,
                &stat.version,
                stat.content_hash.as_deref().unwrap(),
            )
            .is_none()
        );
        assert!(
            core.backend_receipt_for_replica(&stat.object_id, &stat.version, &target)
                .unwrap()
                .is_none()
        );
        let intent = core
            .metadata_backend()
            .unwrap()
            .intents_for_object(&stat.object_id)
            .unwrap()
            .into_iter()
            .find(|intent| {
                intent.status == DfsWriteIntentStatus::Committed
                    && intent.desired_replica_node_ids == vec![target.clone()]
            })
            .expect("the real placement producer committed its target obligation")
            .intent_id;
        // The shard tables derive the target as a need owned by that intent.
        assert!(
            super::indexed_object_repair_needs(
                &native,
                &core,
                &stat.file_system_id,
                &path,
                &stat.object_id,
                &stat.version,
            )
            .contains(&(target.clone(), Some(intent.clone())))
        );
        native.checkpoint_rpc_observation_window().unwrap();
        Self {
            holder,
            target,
            shard,
            stat,
            intent,
            execution,
            core,
            caller,
            holds,
            native,
        }
    }

    fn primary(&self) -> NodeId {
        self.shard.primary_node_id.clone()
    }

    /// One executor pass on the primary, as maintenance runs it, then the
    /// replies it drew.
    fn pass(&mut self, excluded: &[NodeId]) -> usize {
        let root = self.native.data_dir(&self.primary()).root();
        let shards = self.native.metadata_shards().unwrap();
        let pass = self
            .core
            .run_repair_executor_pass(
                &root,
                &shards,
                &crate::DfsMaintenanceExecutionExclusions::new(excluded.iter().cloned()),
                8,
            )
            .unwrap();
        self.core.process_incoming_until_quiescent().unwrap();
        pass.scheduled.len()
    }

    /// The executor's status row for the target's need.
    fn status(&self) -> Option<crate::DfsRepairAttemptRecord> {
        crate::repair_executor::repair_executor_status_sample(
            &self.native.data_dir(&self.primary()).root(),
            16,
        )
        .into_iter()
        .find(|record| {
            record.object_id == self.stat.object_id
                && record.version == self.stat.version
                && record.missing_replica_node_id == self.target
        })
    }

    fn fetches_from_holder(&self) -> usize {
        self.native
            .rpc_observations()
            .iter()
            .filter(|event| {
                event.source == self.primary()
                    && event.target == self.holder
                    && event.route == DFS_DATA_FETCH_BLOB_ROUTE
                    && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            })
            .count()
    }

    /// Let the need's backoff run out, as the executor's timer does at its
    /// end, and serve passes until the target holds a receipt.
    fn retry_until_repaired(&mut self, excluded: &[NodeId]) {
        crate::repair_executor::elapse_repair_executor_backoffs_for_test(
            &self.native.data_dir(&self.primary()).root(),
        );
        for _ in 0..4 {
            if self.target_has_receipt() {
                return;
            }
            self.pass(excluded);
        }
        assert!(
            self.target_has_receipt(),
            "the need is repaired from the holder's copy"
        );
    }

    fn clear_fetch_faults(&self) {
        self.native.remove_faults_matching(|rule| {
            rule.route.as_deref() == Some(DFS_DATA_FETCH_BLOB_ROUTE)
        });
    }

    fn target_has_receipt(&self) -> bool {
        self.core
            .backend_receipt_for_replica(&self.stat.object_id, &self.stat.version, &self.target)
            .unwrap()
            .is_some()
    }

    fn close(self) {
        let Self {
            execution,
            core,
            caller,
            holds,
            native,
            ..
        } = self;
        drop(execution);
        drop(core);
        drop(caller);
        drop(holds);
        native.close().unwrap();
    }
}

#[test]
fn native_fetch_rejects_a_member_without_original_intent_permission() {
    let case = NativeSourceFetch::new("native-async-unauthorized-fetch");
    let command = DfsMessageCommand {
        request_id: "native-unauthorized-source-fetch".into(),
        routed_message: crate::routes::build_dfs_routed_message(
            &case.holder,
            &case.holder,
            1,
            1,
            DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                request_id: "native-unauthorized-source-fetch".into(),
                intent_id: case.intent.clone(),
                object_id: case.stat.object_id.clone(),
                version: case.stat.version.clone(),
                content_hash: case.stat.content_hash.clone().unwrap(),
                authority_shard: Some(case.shard.clone()),
            }),
        ),
    };
    let response: DfsMessageResponse = case
        .native
        .rpc_request(
            &case.holder,
            &case.holder,
            crate::dfs_message_rpc_request(&command).unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap()
        .decode_body()
        .unwrap();
    let rejected = response.outbound_messages.iter().any(|message| {
        matches!(&message.message, DfsNetworkMessage::FetchBlobResponse(reply)
            if reply.request_id == "native-unauthorized-source-fetch"
                && matches!(reply.result, DfsFetchBlobResult::Rejected { .. }))
    });
    let repaired = case.target_has_receipt();
    case.close();
    assert!(
        rejected && !repaired,
        "authenticated membership alone must not authorize this fetch"
    );
}

#[test]
fn executor_fetch_rejected_by_its_source_backs_off_with_the_reason_then_completes() {
    let mut case = NativeSourceFetch::new("native-executor-fetch-rejected");
    let captured = Arc::new(Mutex::new(Vec::new()));
    case.native.install_fault(crate::test_runtime::FaultRule {
        source: Some(case.primary()),
        target: Some(case.holder.clone()),
        route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::AlterFetchReplyAfterDispatch {
            change: crate::test_runtime::FetchReplyFault::NotFound,
            captured: Arc::clone(&captured),
        },
    });
    // The primary lacks the content, so the executor fetches it first; the
    // source's reply says it has none. The next pass serves that outcome.
    assert_eq!(case.pass(&[]), 1);
    assert_eq!(
        captured.lock().unwrap().len(),
        1,
        "the rejected reply arrived"
    );
    assert_eq!(case.pass(&[]), 0, "the failed attempt backs off");
    case.clear_fetch_faults();
    let status = case.status().expect("the need stays tracked");
    assert!(!status.in_flight, "the rejected fetch ended its attempt");
    assert!(status.next_retry_unix_nanos > 0, "it backs off");
    assert_eq!(status.last_error.as_deref(), Some("blob not found"));
    assert!(!case.target_has_receipt());
    case.retry_until_repaired(&[]);
    // The next pass serves the receipt's index change.
    case.pass(&[]);
    assert!(
        case.status().is_none(),
        "the settled need and its error are gone"
    );
    case.close();
}

#[test]
fn executor_fetch_from_an_unroutable_source_fails_and_creates_no_receipt() {
    let mut case = NativeSourceFetch::new("native-executor-fetch-unroutable");
    case.native.install_fault(crate::test_runtime::FaultRule {
        source: Some(case.primary()),
        target: Some(case.holder.clone()),
        route: Some(DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
    });
    assert_eq!(case.pass(&[]), 1);
    case.clear_fetch_faults();
    let status = case.status().expect("the need stays tracked");
    assert!(!status.in_flight);
    assert!(
        status.last_error.is_some(),
        "the send failure is the need's error"
    );
    assert!(
        !case.target_has_receipt(),
        "an unsent fetch creates no receipt"
    );
    case.retry_until_repaired(&[]);
    case.close();
}

#[test]
fn executor_fetches_from_an_excluded_source_for_an_admitted_target() {
    let mut case = NativeSourceFetch::new("native-executor-fetch-excluded-source");
    // Exclusion bounds new destinations; reading a recorded holder is not
    // one, so the fetch still reads from it.
    let holder = case.holder.clone();
    assert_eq!(case.pass(std::slice::from_ref(&holder)), 1);
    assert_eq!(case.fetches_from_holder(), 1);
    case.retry_until_repaired(std::slice::from_ref(&holder));
    case.close();
}

#[test]
fn executor_parks_an_attempted_excluded_target_even_with_a_live_source() {
    let mut case = NativeSourceFetch::new("native-executor-excluded-target");
    let target = case.target.clone();
    // The first attempt is never deferred by a cached projection: the
    // executor fetches the content and sends the replica, which fails.
    case.native.install_fault(crate::test_runtime::FaultRule {
        source: Some(case.primary()),
        target: Some(target.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: crate::test_runtime::FaultAction::TransientRouteFailureBeforeDispatch,
    });
    let sends_to_target = |case: &NativeSourceFetch| {
        case.native
            .rpc_observations()
            .iter()
            .filter(|event| {
                event.source == case.primary()
                    && event.target == case.target
                    && event.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                    && event.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            })
            .count()
    };
    for _ in 0..4 {
        if sends_to_target(&case) > 0 {
            break;
        }
        case.pass(std::slice::from_ref(&target));
    }
    assert_eq!(sends_to_target(&case), 1, "the first attempt was made");
    case.native.remove_faults_matching(|rule| {
        rule.route.as_deref() == Some(DFS_DATA_REPAIR_REPLICA_ROUTE)
    });
    // Attempted and still excluded: when its backoff ends the need parks
    // without another send while a live source holds the content.
    crate::repair_executor::elapse_repair_executor_backoffs_for_test(
        &case.native.data_dir(&case.primary()).root(),
    );
    case.pass(std::slice::from_ref(&target));
    assert_eq!(
        sends_to_target(&case),
        1,
        "an attempted excluded target is not sent again"
    );
    assert!(!case.target_has_receipt());
    let status = case.status().expect("the need stays tracked, parked");
    assert!(!status.in_flight);
    assert!(status.last_error.is_some());
    // Reachability releases it.
    crate::repair_executor::repair_executor_reachability_changed(
        &case.native.data_dir(&case.primary()).root(),
    );
    case.pass(&[]);
    assert!(
        case.target_has_receipt(),
        "the reachable target is repaired"
    );
    case.close();
}
