//! Native filesystem-definition ownership across ordinary content publication.
//! The parent retained V7 follower-outage baseline remains unchanged.
use super::*;

struct Fixture {
    native: DfsTestRuntime,
    nodes: Vec<NodeId>,
    assignment: DfsMetadataShardConfig,
    backend: Arc<DfsBrowserBackend>,
    data: Arc<sinomo_core::local::SinomoDataDir>,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let (native, nodes) = DfsTestRuntime::start_cluster(label, 3).unwrap();
        let initial = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|shard| shard.shard_id == "root")
            .unwrap();
        let primary = initial.primary_node_id.clone();
        let assignment = native
            .reassign_metadata_shard(
                &initial.file_system_id,
                "root",
                primary.clone(),
                nodes
                    .iter()
                    .filter(|node| **node != primary)
                    .cloned()
                    .collect(),
            )
            .unwrap();
        let data = Arc::new(native.data_dir(&primary).clone());
        let backend =
            DfsBrowserBackend::for_component_data_dir(Arc::clone(&data), &native.runtime_handle())
                .unwrap();
        backend
            .with_mutation_service(|service| {
                service
                    .posix_mkdir_all(PARENT)
                    .map(|_| ())
                    .map_err(dfs_browser_service_error)
            })
            .unwrap();
        let cached_requester_definition = backend
            .with_mutation_service(|service| {
                Ok(service
                    .metadata
                    .filesystems
                    .get(&sifs_file_system_id())
                    .cloned())
            })
            .unwrap();
        let accepted_config = native.shared_control_plane().unwrap().with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            let expected = store
                .get_file_system_config(&sifs_file_system_id())
                .unwrap()
                .unwrap();
            let mut desired = expected.clone();
            upsert_placement_rule(
                &mut desired.subtree_policies,
                normalize_placement_rule(
                    PARENT,
                    DfsPlacementScope::Subtree,
                    DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 2, 3)
                        .unwrap(),
                )
                .unwrap(),
            );
            assert!(
                store
                    .compare_and_put_file_system_config(Some(&expected), &desired)
                    .unwrap()
            );
            desired
        });
        let accepted_definition: DfsFileSystem = accepted_config.clone().into();
        // Publish through the existing explicit owning API. A metadata-only
        // mkdir is not a filesystem-definition synchronization operation.
        let mut owner = native.metadata_primary_service(PARENT).unwrap();
        let definition_publication = owner.add_file_system(accepted_definition.clone());
        drop(owner);
        evidence(
            &native,
            &primary,
            "filesystem-explicit-setup-owner.json",
            serde_json::json!({"accepted_config":accepted_config,"accepted_definition":accepted_definition,"publication":format!("{definition_publication:?}"),"retained_requester_cache":cached_requester_definition}),
        );
        definition_publication.unwrap();
        let fixture = Self {
            native,
            nodes,
            assignment,
            backend,
            data,
        };
        fixture.ready();
        fixture.checkpoint(
            "filesystem-projection-setup",
            serde_json::json!({"assignment":fixture.assignment,"native":fixture.cut()}),
        );
        fixture
    }
    fn ready(&self) {
        let host = dfs_browser_component_host(&self.data).unwrap();
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let mut last = None;
        let ready = loop {
            let readiness = self.native.runtime_handle().block_on(async {
                tokio::time::timeout_at(
                    deadline,
                    host.ensure_write_configuration_ready(
                        &self.data,
                        &self.native.runtime_handle(),
                    ),
                )
                .await
            });
            if !matches!(readiness, Ok(Ok(()))) {
                last = Some(format!("{readiness:?}"));
                break false;
            }
            let observation = self.backend.try_with_configured_mutation_service(
                &[PATH],
                (),
                |service, configuration, ()| {
                    let options = self.backend.write_options_for_path_with_configuration(
                        service,
                        PATH,
                        configuration,
                    )?;
                    Ok((options.policy, options.replica_candidates))
                },
            );
            let matches = matches!(&observation, Ok(DfsBrowserMutationAttempt::Complete((policy,candidates)))
                if policy.min_commit_replicas==2 && policy.desired_replicas==3
                    && candidates.iter().cloned().collect::<BTreeSet<_>>()==self.nodes.iter().cloned().collect());
            last = Some(format!("{observation:?}"));
            if matches {
                break true;
            }
            if tokio::time::Instant::now() >= deadline {
                break false;
            }
            std::thread::yield_now();
        };
        evidence(
            &self.native,
            &self.assignment.primary_node_id,
            "filesystem-projection-ready.json",
            serde_json::json!({"ready":ready,"last":last}),
        );
        assert!(
            ready,
            "actual same-browser W2/N3/all3 policy did not become ready"
        );
    }
    fn cut(&self) -> DfsMetadataTableSnapshot {
        fresh_cut(
            &self.native,
            &self.assignment.primary_node_id,
            &self.assignment,
        )
        .unwrap()
    }
    fn fs(&self) -> DfsFileSystem {
        self.cut()
            .tables
            .file_systems
            .iter()
            .find(|fs| fs.file_system_id == sifs_file_system_id())
            .unwrap()
            .clone()
    }
    fn pipeline(
        &self,
        after: u64,
        through: u64,
        object: &DfsObjectId,
        operation: &str,
    ) -> std::result::Result<Vec<DfsPreparedOperationRecord>, String> {
        let root = default_dfs_metadata_shards_root(
            self.native.data_dir(&self.assignment.primary_node_id),
        )
        .join(crate::metadata_keys::key_segment(&self.assignment.shard_id));
        let mut reader = LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new(&self.assignment.shard_id),
            root,
        )
        .map_err(|e| e.to_string())?;
        reader
            .begin_inspection_snapshot()
            .map_err(|e| e.to_string())?;
        let batch = reader
            .journal_batch_for_shard_since_limited(
                &DfsMetadataShardId::new(&self.assignment.shard_id),
                after,
                256,
                4 * 1024 * 1024,
            )
            .map_err(|e| e.to_string());
        reader.end_inspection_snapshot();
        let batch = batch?.ok_or("actual native original journal batch is unavailable")?;
        if batch.next_after_sequence < through {
            return Err(
                "actual original journal inspection truncated before observed native cut".into(),
            );
        }
        let mut records = Vec::new();
        for entry in batch.entries {
            if entry.sequence > through {
                continue;
            }
            if let DfsMetadataMutation::PrepareOperationV7 { record, .. } = entry.mutation {
                let matches = |intent: &DfsWriteIntentRecord| {
                    intent.object_id == *object && intent.operation_id.as_deref() == Some(operation)
                };
                if record.prepared_intent.as_ref().is_some_and(matches) || record.metadata_effects.iter().any(|effect|matches!(effect,DfsPreparedMetadataEffect::PutIntent { intent } if matches(intent))) {
                    record.validate().map_err(|e|e.to_string())?;records.push(*record);
                }
            }
        }
        Ok(records)
    }

    fn checkpoint(&self, name: &str, value: serde_json::Value) {
        let raw = self.native.checkpoint_rpc_observation_window();
        let exported = if raw.as_ref().is_ok_and(|count| *count > 0) {
            self.native
                .export_latest_rpc_observation_checkpoint(
                    &self.data.root().join(format!("{name}-rpc.json")),
                )
                .map_err(|error| error.to_string())
        } else {
            Ok(0)
        };
        evidence(
            &self.native,
            &self.assignment.primary_node_id,
            &format!("{name}.json"),
            serde_json::json!({"state":value,"raw":format!("{raw:?}"),"export":format!("{exported:?}")}),
        );
        raw.unwrap();
        exported.unwrap();
    }
    fn create(
        &self,
        operation: &str,
    ) -> (
        std::result::Result<
            DfsBrowserMutationAttempt<
                (),
                (
                    DfsReplicationPolicy,
                    Vec<NodeId>,
                    crate::posix_model::Result<crate::DfsStat>,
                ),
            >,
            ServiceError,
        >,
        std::time::Duration,
    ) {
        let started = Instant::now();
        let deadline = started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let result = self.backend.try_with_configured_mutation_service(
            &[PATH],
            (),
            |service, configuration, ()| {
                let options = self.backend.write_options_for_path_with_configuration(
                    service,
                    PATH,
                    configuration,
                )?;
                let actual = (options.policy.clone(), options.replica_candidates.clone());
                let previous = service.set_transport_operation_deadline(Some(deadline));
                let outcome =
                    service.posix_create_file_with_operation_id(PATH, BYTES, options, operation);
                service.set_transport_operation_deadline(previous);
                Ok((actual.0, actual.1, outcome))
            },
        );
        (result, started.elapsed())
    }
}

#[test]
fn ordinary_w2_create_and_overwrite_each_publish_one_recoverable_write() {
    let fixture = Fixture::new("native-one-publication-w2");
    for bytes in [b"first".as_slice(), b"replacement".as_slice()] {
        let before = fixture.cut().materialized_through_sequence;
        fixture
            .backend
            .write_file(crate::DaemonDfsBrowserWriteFileRequest {
                path: PATH.into(),
                bytes: bytes.to_vec(),
            })
            .expect("owner commits the requested bytes");
        let cut = fixture.cut();
        let object = cut.tables.objects.iter().find(|o| o.path == PATH).unwrap();
        let records: Vec<_> = cut.tables.prepared_operations_v7.iter()
            .filter(|p| p.digest.prepare_sequence > before)
            .filter(|p| p.terminal_intent.as_ref().is_some_and(|i| i.object_id == object.object_id)
                || p.metadata_effects.iter().any(|e| matches!(e, DfsPreparedMetadataEffect::PutIntent {intent} if intent.object_id == object.object_id)))
            .collect();
        assert_eq!(records.len(), 1, "one native write transaction");
        let record = records[0];
        assert_eq!(record.status, crate::DfsPreparedOperationStatus::Committed);
        assert!(record.completed_request_completion.is_some());
        assert_eq!(record.content_receipts.len(), 2);
        assert_eq!(
            record.terminal_intent.as_ref().unwrap().target_version,
            object.version
        );
        assert_eq!(object.content_hash, Some(crate::content_hash(bytes)));
    }
    fixture.native.close().unwrap();
}

#[test]
fn cold_table_reader_joins_a_private_write_cut_and_observes_subsequent_publication() {
    let fixture = Fixture::new("native-private-projection-reader-handoff");
    let write = |bytes: &[u8]| {
        fixture
            .backend
            .write_file(crate::DaemonDfsBrowserWriteFileRequest {
                path: PATH.into(),
                bytes: bytes.to_vec(),
            })
            .unwrap()
    };
    let shard = DfsMetadataShardId::new(&fixture.assignment.shard_id);
    let primary = &fixture.assignment.primary_node_id;
    let root = crate::local_dfs_metadata_shard_path(fixture.native.data_dir(primary), &shard);
    write(b"wal-only first version");
    let mut reader = LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &root).unwrap();
    reader.begin_stat_read_snapshot().unwrap();
    let get = |reader: &LocalSobjMetadataBackend| {
        reader
            .get_object_by_path(&sifs_file_system_id(), PATH)
            .unwrap()
            .unwrap()
    };
    let first = get(&reader);
    assert_eq!(
        first.content_hash,
        Some(crate::content_hash(b"wal-only first version"))
    );
    write(b"published with a pinned reader");
    assert_eq!(get(&reader).version, first.version);
    reader.end_stat_read_snapshot();
    reader.begin_stat_read_snapshot().unwrap();
    assert_eq!(
        get(&reader).content_hash,
        Some(crate::content_hash(b"published with a pinned reader"))
    );
    reader.end_stat_read_snapshot();
    drop(reader);
    write(b"last version before reopen");
    let reopened = LocalSobjMetadataBackend::new(shard, root).unwrap();
    assert_eq!(
        get(&reopened).content_hash,
        Some(crate::content_hash(b"last version before reopen"))
    );
    drop(reopened);
    fixture.native.close().unwrap();
}

#[test]
fn healthy_current_create_preserves_the_actual_w2_n3_native_definition() {
    let fixture = Fixture::new("native-filesystem-definition-current");
    let before = fixture.cut();
    let before_fs = fixture.fs();
    assert!(
        before_fs
            .subtree_policies
            .iter()
            .any(|policy| policy.path_prefix == PARENT
                && policy.min_commit_replicas == 2
                && policy.desired_replicas == 3)
    );
    let operation = format!("native-fs-current-{}", sinomo_core::Id::generate());
    let (result, elapsed) = fixture.create(&operation);
    let after = fixture.cut();
    let expected_object = crate::intent::stable_create_object_id(
        &fixture.assignment.primary_node_id,
        PATH,
        &operation,
    );
    let expected_version = crate::intent::stable_write_target_version(
        &fixture.assignment.primary_node_id,
        &expected_object,
        &operation,
    );
    let physical = fixture
        .nodes
        .iter()
        .map(|node| {
            let store = LocalContentStore::open_for_inspection(
                fixture.native.data_dir(node).dfs_dir().join("content"),
            );
            (
                node.clone(),
                store
                    .read_object_version(expected_object.as_str(), expected_version.as_str())
                    .map_err(|error| error.to_string()),
            )
        })
        .collect::<Vec<_>>();
    let pipeline = fixture.pipeline(
        before.materialized_through_sequence,
        after.materialized_through_sequence,
        &expected_object,
        &operation,
    );
    fixture.checkpoint("filesystem-current-result",serde_json::json!({"operation":operation,"before":before,"after":after,"result":format!("{result:?}"),"elapsed_nanos":elapsed.as_nanos(),"physical":physical,"all_original_native_preparations":pipeline}));
    let pipeline = pipeline.expect("complete actual original prepare journal coverage");
    assert!(
        !pipeline.is_empty(),
        "actual Original must have native prepared bodies"
    );
    for original in &pipeline {
        for effect in &original.metadata_effects {
            if let DfsPreparedMetadataEffect::PutFileSystem { file_system } = effect {
                assert_eq!(
                    file_system, &before_fs,
                    "ordinary Original prepared a conflicting cached filesystem definition, even if a later publication heals the net state"
                );
            }
        }
    }
    let Ok(DfsBrowserMutationAttempt::Complete((policy, candidates, Ok(stat)))) = result else {
        panic!("normal healthy original create did not commit");
    };
    assert_eq!(policy, DfsReplicationPolicy::new(2, 3).unwrap());
    assert_eq!(
        candidates.iter().cloned().collect::<BTreeSet<_>>(),
        fixture.nodes.iter().cloned().collect()
    );
    assert_eq!(
        fixture.fs(),
        before_fs,
        "ordinary Current must not replace an existing filesystem definition from its cache"
    );
    let intent = after
        .tables
        .intents
        .iter()
        .find(|intent| {
            intent.object_id == stat.object_id
                && intent.operation_id.as_deref() == Some(operation.as_str())
        })
        .unwrap();
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    assert_eq!(intent.policy, policy);
    assert_eq!(intent.desired_replica_node_ids.len(), 3);
    let receipt_nodes = after
        .tables
        .receipts
        .iter()
        .filter(|receipt| {
            receipt.receipt.object_id == stat.object_id
                && receipt.receipt.committed_version == stat.version
                && receipt.content_hash.as_deref() == Some(crate::content_hash(BYTES).as_str())
        })
        .map(|receipt| receipt.replica_node_id.clone())
        .collect::<BTreeSet<_>>();
    assert!(
        receipt_nodes.len() >= 2,
        "actual native W2 receipts must be stored"
    );
    for node in receipt_nodes {
        let (_, read) = physical.iter().find(|(stored, _)| stored == &node).unwrap();
        assert_eq!(read.as_ref().unwrap().1.as_slice(), BYTES);
    }

    assert!(elapsed < sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    drop(fixture.backend);
    fixture.native.close().unwrap();
}

#[test]
fn explicit_filesystem_producer_still_publishes_the_exact_requested_definition() {
    let fixture = Fixture::new("native-filesystem-definition-explicit");
    let before = fixture.cut();
    let mut desired = fixture.fs();
    desired.subtree_policies.push(DfsSubtreePolicy::new(
        "/explicit-native-filesystem-owner",
        sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        2,
        3,
    ));
    let mut owner = fixture.native.metadata_primary_service(PARENT).unwrap();
    let outcome = owner.add_file_system(desired.clone());
    drop(owner);
    let after = fixture.cut();
    fixture.checkpoint("filesystem-explicit-result",serde_json::json!({"before":before,"desired":desired,"after":after,"outcome":format!("{outcome:?}")}));
    outcome.unwrap();
    assert_eq!(fixture.fs(), desired);
    assert!(after.tables.prepared_operations_v7.iter().any(|record| record.status==DfsPreparedOperationStatus::Committed
        && record.payload.metadata_effects.iter().any(|effect|matches!(effect,DfsPreparedMetadataEffect::PutFileSystem { file_system } if file_system==&desired))));
    drop(fixture.backend);
    fixture.native.close().unwrap();
}

#[test]
fn recorded_original_after_configuration_update_and_restart_does_not_replan_filesystem() {
    let mut fixture = Fixture::new("native-filesystem-definition-replay");
    let operation = format!("native-fs-replay-{}", sinomo_core::Id::generate());
    let (first, elapsed) = fixture.create(&operation);
    let first_cut = fixture.cut();
    fixture.checkpoint("filesystem-replay-first",serde_json::json!({"first":format!("{first:?}"),"cut":first_cut,"elapsed_nanos":elapsed.as_nanos()}));
    let Ok(DfsBrowserMutationAttempt::Complete((_, _, Ok(stat)))) = first else {
        panic!("original did not commit");
    };
    let originals = first_cut
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            let matches=|intent:&DfsWriteIntentRecord|intent.object_id==stat.object_id && intent.operation_id.as_deref()==Some(operation.as_str());
            record.prepared_intent.as_ref().is_some_and(matches) || record.metadata_effects.iter().any(|effect|matches!(effect,DfsPreparedMetadataEffect::PutIntent { intent } if matches(intent)))
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        !originals.is_empty(),
        "actual original immutable body must be retained"
    );
    let mut desired = fixture.fs();
    desired.subtree_policies.push(DfsSubtreePolicy::new(
        "/later-native-filesystem-owner",
        sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        2,
        3,
    ));
    let mut owner = fixture.native.metadata_primary_service(PARENT).unwrap();
    owner.add_file_system(desired.clone()).unwrap();
    drop(owner);
    let before_restart = fixture.cut();
    assert_eq!(fixture.fs(), desired);
    drop(fixture.backend);
    fixture
        .native
        .stop_node(&fixture.assignment.primary_node_id)
        .unwrap();
    fixture
        .native
        .reopen_node(fixture.assignment.primary_node_id.clone())
        .unwrap();
    fixture.backend = DfsBrowserBackend::for_component_data_dir(
        Arc::clone(&fixture.data),
        &fixture.native.runtime_handle(),
    )
    .unwrap();
    fixture.ready();
    let (replay, replay_elapsed) = fixture.create(&operation);
    let after = fixture.cut();
    fixture.checkpoint("filesystem-replay-result",serde_json::json!({"operation":operation,"originals":originals,"before_restart":before_restart,"after":after,"replay":format!("{replay:?}"),"elapsed_nanos":replay_elapsed.as_nanos()}));
    let Ok(DfsBrowserMutationAttempt::Complete((_, _, Ok(replayed)))) = replay else {
        panic!("same Original replay failed");
    };
    assert_eq!(replayed.object_id, stat.object_id);
    assert_eq!(replayed.version, stat.version);
    assert_eq!(fixture.fs(), desired);
    for original in originals {
        if let Some(record) = after
            .tables
            .prepared_operations_v7
            .iter()
            .find(|record| record.digest == original.digest)
        {
            assert_eq!(record, &original);
        } else {
            let terminal = after
                .tables
                .prepared_operation_evictions
                .iter()
                .find(|terminal| terminal.digest == original.digest)
                .expect("exact retained original terminal identity");
            assert_eq!(terminal.status, original.status);
            assert_eq!(terminal.committed_entry, original.committed_entry);
        }
    }
    drop(fixture.backend);
    fixture.native.close().unwrap();
}
