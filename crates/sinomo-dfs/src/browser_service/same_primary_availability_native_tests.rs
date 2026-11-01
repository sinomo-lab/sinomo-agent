//! Fresh W2/N3 availability with one follower partitioned after formation.
//! A failed original remains a requirement failure with its native evidence.
//! The native fixture has three DFS participants but its default KV authority
//! has one voter. This is not a real KV-majority or abrupt-takeover proof.
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
use crate::*;
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

const PARENT: &str = "/same-primary-follower-baseline";
const PATH: &str = "/same-primary-follower-baseline/fresh-after-partition.txt";
const BYTES: &[u8] = b"one original fresh W2 N3 create after follower transport partition";

fn evidence(native: &DfsTestRuntime, node: &NodeId, name: &str, value: serde_json::Value) {
    std::fs::write(
        native.data_dir(node).root().join(name),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
}

// A Core shell's export_table_snapshot exports its resident writer image; a
// lock-free getter does not refresh that image. Open the ACTUAL selected owner
// path afresh and pin its existing read-only inspection cut for every sample.
fn fresh_cut(
    native: &DfsTestRuntime,
    node: &NodeId,
    shard: &DfsMetadataShardConfig,
) -> Result<DfsMetadataTableSnapshot, String> {
    let root = default_dfs_metadata_shards_root(native.data_dir(node))
        .join(crate::metadata_keys::key_segment(&shard.shard_id));
    let mut reader = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(shard.shard_id.clone()),
        root,
    )
    .map_err(|error| error.to_string())?;
    reader
        .begin_inspection_snapshot()
        .map_err(|error| error.to_string())?;
    let snapshot = reader.export_table_snapshot();
    reader.end_inspection_snapshot();
    Ok(snapshot)
}

// A closed existing pre-dispatch gate is a negative-only immediate route
// failure. It cannot execute the receiver or provide a successful ACK. The
// private Arc identifies ONLY these four owned fault rules on cleanup/unwind.
struct OwnedFollowerPartition<'a> {
    native: &'a DfsTestRuntime,
    gate: Arc<tokio::sync::Semaphore>,
}
impl<'a> OwnedFollowerPartition<'a> {
    fn install(native: &'a DfsTestRuntime, nodes: &[NodeId], follower: &NodeId) -> Self {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        gate.close();
        for other in nodes.iter().filter(|node| *node != follower) {
            for (source, target) in [(other, follower), (follower, other)] {
                native.install_fault(FaultRule {
                    source: Some(source.clone()),
                    target: Some(target.clone()),
                    route: None,
                    call_id: None,
                    origin: None,
                    remaining_hits: 32,
                    action: FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
                });
            }
        }
        Self { native, gate }
    }
}
impl Drop for OwnedFollowerPartition<'_> {
    fn drop(&mut self) {
        self.native.remove_faults_matching(|rule| {
            matches!(&rule.action, FaultAction::PauseBeforeDispatch(gate)
                if Arc::ptr_eq(gate, &self.gate))
        });
    }
}

#[test]
fn native_fresh_w2_n3_create_survives_a_same_primary_follower_partition() {
    let (native, nodes) =
        DfsTestRuntime::start_cluster("native-same-primary-follower-baseline", 3).unwrap();
    let assignment = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap();
    let primary = assignment.primary_node_id.clone();
    let expected_nodes = nodes.iter().cloned().collect::<BTreeSet<_>>();
    let assigned_nodes = std::iter::once(assignment.primary_node_id.clone())
        .chain(assignment.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    // The embedded default control-plane voter is nodes[0]. Keep it with the
    // surviving pair: this test intentionally isolates a DFS follower, not KV.
    let follower = nodes
        .iter()
        .find(|node| **node != primary && **node != nodes[0])
        .unwrap()
        .clone();
    let survivors = nodes
        .iter()
        .filter(|node| **node != follower)
        .cloned()
        .collect::<BTreeSet<_>>();
    native.shared_control_plane().unwrap().with_client(|kv| {
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
    });
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
    let host = dfs_browser_component_host(&data).unwrap();
    let setup_deadline =
        tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut last_configuration = None;
    let mut last_cuts = Vec::new();
    let mut setup_error = None;
    let setup_ready = loop {
        let readiness = native.runtime_handle().block_on(async {
            tokio::time::timeout_at(
                setup_deadline,
                host.ensure_write_configuration_ready(&data, &native.runtime_handle()),
            )
            .await
        });
        if !matches!(readiness, Ok(Ok(()))) {
            setup_error = Some(format!("{readiness:?}"));
            break false;
        }
        let configured = backend.try_with_configured_mutation_service(
            &[PATH],
            (),
            |service, configuration, ()| {
                let options = backend.write_options_for_path_with_configuration(
                    service,
                    PATH,
                    configuration,
                )?;
                Ok((options.policy, options.replica_candidates))
            },
        );
        let configuration_ready = match configured {
            Ok(DfsBrowserMutationAttempt::Complete(actual)) => {
                let ready = actual.0.min_commit_replicas == 2
                    && actual.0.desired_replicas == 3
                    && actual.1.iter().cloned().collect::<BTreeSet<_>>() == expected_nodes;
                last_configuration = Some(actual);
                ready
            }
            Ok(DfsBrowserMutationAttempt::ConfigurationPending(())) => false,
            Err(error) => {
                setup_error = Some(error.to_string());
                false
            }
        };
        last_cuts = nodes
            .iter()
            .map(|node| (node.clone(), fresh_cut(&native, node, &assignment)))
            .collect::<Vec<_>>();
        let cuts_ready = last_cuts.iter().all(|(_, cut)| {
            cut.as_ref().is_ok_and(|cut| {
                cut.prefix_proof.is_well_formed()
                    && cut.tables.objects.iter().any(|object| {
                        object.path == PARENT && object.kind == DfsObjectKind::Directory
                    })
            })
        }) && last_cuts
            .windows(2)
            .all(|pair| match (&pair[0].1, &pair[1].1) {
                (Ok(left), Ok(right)) => left.prefix_proof == right.prefix_proof,
                _ => false,
            });
        if configuration_ready && cuts_ready && assigned_nodes == expected_nodes {
            break true;
        }
        if tokio::time::Instant::now() >= setup_deadline {
            break false;
        }
        std::thread::yield_now();
    };
    let setup_raw = native.checkpoint_rpc_observation_window();
    if setup_raw.as_ref().is_ok_and(|count| *count > 0) {
        native
            .export_latest_rpc_observation_checkpoint(
                &data.root().join("same-primary-offline-setup-rpc.json"),
            )
            .unwrap();
    }
    evidence(
        &native,
        &primary,
        "same-primary-offline-setup.json",
        serde_json::json!({
            "ready":setup_ready, "error":setup_error, "assignment":assignment,
            "actual_options":last_configuration, "native_cuts":last_cuts,
            "expected_nodes":expected_nodes, "assigned_nodes":assigned_nodes,
            "follower":follower, "survivors":survivors,
            "raw_checkpoint_result":format!("{setup_raw:?}"),
            "qualification":"three real native metadata participants and same browser's current W2/N3 provider projection, before any transport fault; embedded default one-voter KV is NOT a real KV-majority proof"
        }),
    );
    assert!(
        setup_ready,
        "formation/policy/native prefix qualification failed before partition"
    );
    setup_raw.unwrap();
    drop(host);
    let before = fresh_cut(&native, &primary, &assignment).unwrap();
    assert!(
        !before
            .tables
            .objects
            .iter()
            .any(|object| object.path == PATH)
    );
    let operation = format!("same-primary-offline-{}", sinomo_core::Id::generate());
    let expected_object = crate::intent::stable_create_object_id(&primary, PATH, &operation);
    let expected_version =
        crate::intent::stable_write_target_version(&primary, &expected_object, &operation);
    let expected_hash = crate::content_hash(BYTES);
    let traces = nodes
        .iter()
        .map(|node| {
            (
                node.clone(),
                crate::native_shard_trace::capture_native_shard_trace(
                    &native.data_dir(node).root(),
                    DfsMetadataShardId::new(&assignment.shard_id),
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let partition = OwnedFollowerPartition::install(&native, &nodes, &follower);
    let started = Instant::now();
    let deadline = started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    // EXACTLY ONE fresh semantic create, AFTER partition. Its options are read
    // from the same production coherent view, never supplied candidate hints.
    // ConfigurationPending/error is retained; there is no post-effect replan.
    let result =
        backend.try_with_configured_mutation_service(&[PATH], (), |service, configuration, ()| {
            let options =
                backend.write_options_for_path_with_configuration(service, PATH, configuration)?;
            if options.policy.min_commit_replicas != 2
                || options.policy.desired_replicas != 3
                || options
                    .replica_candidates
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    != expected_nodes
            {
                return Err(dfs_browser_service_error(
                    "original W2/N3 named provider projection changed before dispatch",
                ));
            }
            let actual_options = (options.policy.clone(), options.replica_candidates.clone());
            let previous = service.set_transport_operation_deadline(Some(deadline));
            let outcome =
                service.posix_create_file_with_operation_id(PATH, BYTES, options, &operation);
            service.set_transport_operation_deadline(previous);
            Ok((actual_options, outcome))
        });
    let original_elapsed = started.elapsed();
    // Capture all genuine durable state and the original error BEFORE asserting
    // success. A current-V7 failure is the wanted baseline, not fixture PASS.
    let after = fresh_cut(&native, &primary, &assignment);
    let after_assignment = native.metadata_shards().map(|shards| {
        shards
            .into_iter()
            .find(|shard| shard.shard_id == assignment.shard_id)
    });
    let physical = survivors
        .iter()
        .map(|node| {
            let store = LocalContentStore::open_for_inspection(
                native.data_dir(node).dfs_dir().join("content"),
            );
            (
                node.clone(),
                store
                    .read_object_version(expected_object.as_str(), expected_version.as_str())
                    .map_err(|error| error.to_string()),
            )
        })
        .collect::<Vec<_>>();
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(&data));
    let queue_cut = queue.view();
    let raw = native.rpc_observations();
    let failed_routes = raw
        .iter()
        .filter(|event| {
            (event.source == follower || event.target == follower)
                && event.phase == RpcPhase::EndedWithoutResponse
                && event.outcome_error.is_some()
        })
        .map(|event| format!("{event:?}"))
        .collect::<Vec<_>>();
    let raw_checkpoint = native.checkpoint_rpc_observation_window();
    if raw_checkpoint.as_ref().is_ok_and(|count| *count > 0) {
        native
            .export_latest_rpc_observation_checkpoint(
                &data.root().join("same-primary-offline-original-rpc.json"),
            )
            .unwrap();
    }
    let result_debug = format!("{result:#?}");
    let blocking_stage = match &result {
        Ok(DfsBrowserMutationAttempt::ConfigurationPending(())) => {
            "configuration-before-native-create"
        }
        Err(_) => "configured-owner-admission-before-native-result",
        Ok(DfsBrowserMutationAttempt::Complete((_, Ok(_)))) => "native-create-returned-success",
        Ok(DfsBrowserMutationAttempt::Complete((
            _,
            Err(DfsPosixError::Dfs(DfsError::MetadataRecoveryQuorumUnavailable { .. })),
        ))) => "native-metadata-authority-full-assigned",
        Ok(DfsBrowserMutationAttempt::Complete((_, Err(_)))) => {
            "native-create-returned-error-see-exact-original-result"
        }
    };
    let own_prepared_pipeline = after.as_ref().ok().map(|snapshot| {
        snapshot
            .tables
            .prepared_operations_v7
            .iter()
            .filter(|record| {
                record
                    .expected_object_versions
                    .iter()
                    .any(|expected| expected.object_id == expected_object)
                    || record
                        .transaction
                        .objects
                        .iter()
                        .any(|object| object.object_id == expected_object)
                    || record
                        .metadata_effects
                        .iter()
                        .any(|effect| effect.affected_object_ids().contains(&expected_object))
            })
            .cloned()
            .collect::<Vec<_>>()
    });
    let mut actual_original_intents = std::collections::BTreeMap::new();
    if let Ok(snapshot) = &after {
        for intent in
            snapshot.tables.intents.iter().chain(
                snapshot
                    .tables
                    .prepared_operations_v7
                    .iter()
                    .flat_map(|record| {
                        record
                            .prepared_intent
                            .iter()
                            .chain(record.terminal_intent.iter())
                            .chain(record.metadata_effects.iter().filter_map(
                                |effect| match effect {
                                    DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
                                    _ => None,
                                },
                            ))
                    }),
            )
        {
            if intent.operation_id.as_deref() == Some(operation.as_str())
                && intent.object_id == expected_object
                && intent.target_version == expected_version
                && intent.content_hash.as_deref() == Some(expected_hash.as_str())
            {
                actual_original_intents.insert(intent.intent_id.clone(), intent.clone());
            }
        }
    }
    // Existing native RPC-attempt observation retains the ACTUAL encoded
    // authority command and same-attempt terminal. Decode its genuine batch,
    // and join that frame to the native body already linked to our stable
    // frontend token/object. Endpoint-filtered errors alone prove no causality.
    let mut original_authority_carriers = Vec::new();
    let mut native_trace_quality = Vec::new();
    for (node, trace) in &traces {
        trace.close();
        let mut bytes = Vec::new();
        trace.export_raw(&mut bytes).unwrap();
        std::fs::write(
            data.root()
                .join(format!("same-primary-offline-native-{node}.json")),
            &bytes,
        )
        .unwrap();
        let observed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        native_trace_quality.push(serde_json::json!({
            "node":node,"in_flight":observed["in_flight"],
            "dropped":observed["dropped"],"incomplete":observed["incomplete"]
        }));
        let packets = observed["boundary_packets"].as_array().unwrap();
        for packet in packets {
            let facts = &packet["facts"];
            let command = match facts["phase"].as_str() {
                Some("direct-journal-rpc-attempt-started") => {
                    serde_json::from_value::<DfsMetadataJournalBatchCommand>(
                        facts["command"].clone(),
                    )
                    .ok()
                }
                Some("rpc-attempt-started") => {
                    serde_json::from_value::<DfsMessageCommand>(facts["command"].clone())
                        .ok()
                        .and_then(|command| match command.routed_message.message {
                            DfsNetworkMessage::MetadataJournalBatch(command) => Some(command),
                            _ => None,
                        })
                }
                _ => None,
            };
            let Some(command) = command else {
                continue;
            };
            let Some(probe) = command.authority_probe.as_ref() else {
                continue;
            };
            let Some(record) = own_prepared_pipeline.as_ref().and_then(|records| {
                records.iter().find(|record| {
                    probe.operation
                        == DfsMetadataAuthorityOperation::PreparedOperation {
                            operation_id: record.digest.operation_id.clone(),
                            file_system_id: record.digest.file_system_id.clone(),
                            shard_id: record.digest.shard_id.clone(),
                            owner_epoch: record.digest.owner_epoch,
                            prepare_sequence: record.digest.prepare_sequence,
                            payload_digest: record.digest.payload_digest.clone(),
                        }
                })
            }) else {
                continue;
            };
            let decoded = command.batch.decoded();
            let exact_frame = decoded.as_ref().is_ok_and(|batch| {
                batch.entries.iter().any(|entry| {
                matches!(&entry.mutation, DfsMetadataMutation::PrepareOperationV7 { record:wire,.. }
                    if wire.digest == record.digest && wire.payload == record.payload)
            })
            });
            let terminals = packets
                .iter()
                .filter(|terminal| {
                    let ended = &terminal["facts"];
                    ended["attempt_id"] == facts["attempt_id"]
                        && matches!(
                            ended["phase"].as_str(),
                            Some(
                                "direct-journal-rpc-attempt-returned"
                                    | "direct-journal-rpc-attempt-abandoned"
                                    | "rpc-attempt-returned"
                                    | "rpc-attempt-abandoned"
                            )
                        )
                })
                .cloned()
                .collect::<Vec<_>>();
            original_authority_carriers.push(serde_json::json!({
                "actual_observer":node,"frontend_operation":operation,
                "native_original_digest":record.digest,"exact_prepared_frame":exact_frame,
                "actual_started":packet,"actual_same_attempt_terminal":terminals,
                "actual_decoded_batch":decoded.map_err(|error| error.to_string()),
                "expected_assignment_matches":probe.expected_shard == assignment,
                "actual_command_source_matches":command.source_node_id.as_ref() == Some(&primary),
                "scope":"same native attempt ID joins actual encoded original authority request and its terminal; no optional RPC scalar hint or endpoint-only attribution is manufactured"
            }));
        }
    }
    let queue_json = match &queue_cut {
        Ok(cut) => serde_json::json!({"head":cut.head_sequence,"records":cut.records}),
        Err(error) => serde_json::json!({"error":error.to_string()}),
    };
    evidence(
        &native,
        &primary,
        "same-primary-offline-original.json",
        serde_json::json!({
            "operation":operation, "expected_object":expected_object,
            "expected_version":expected_version, "expected_hash":expected_hash,
            "bytes":BYTES, "result":result_debug,
            "original_elapsed_nanos":original_elapsed.as_nanos(),
            "per_action_budget_nanos":sinomo_core::network::DEFAULT_NETWORK_TIMEOUT.as_nanos(),
            "before":before, "after":after, "assignment_before":assignment,
            "assignment_after":after_assignment, "physical_survivor_reads":physical,
            "queue":queue_json, "actual_failed_follower_calls":failed_routes,
            "raw_checkpoint_result":format!("{raw_checkpoint:?}"),
            "fault_budget_result":native.assert_fault_budgets(),
        "blocking_stage":blocking_stage, "own_prepared_pipeline":own_prepared_pipeline,
        "original_authority_carriers":original_authority_carriers,"native_trace_quality":native_trace_quality,
        "actual_original_intents":actual_original_intents,
        "follower_error_list_limit":"endpoint-only errors may include unrelated background calls; original cause is established by actual native body/digest/frame/attempt joins above, not by this list",
        "outcome_limit":"exact original result and complete prepared bodies/effects are retained at this fresh native cut; transport failure or absent current path does not establish NotApplied",
            "verdict":"requirement baseline; expected FAIL under unchanged V7 FullAssigned. No heal/replay/new semantic identity and no production change. Future success must reach all assertions below."
        }),
    );
    raw_checkpoint.unwrap();
    native.assert_fault_budgets().unwrap();
    assert_eq!(
        actual_original_intents.len(),
        1,
        "actual native intent must bind the original frontend token/object/target/hash; no optional-hint or background-original substitute"
    );
    assert!(
        native_trace_quality.iter().all(|quality| {
            quality["in_flight"].as_str() == Some("0")
                && quality["dropped"].as_str() == Some("0")
                && quality["incomplete"].as_str() == Some("0")
        }),
        "native original carrier evidence must be complete; actual quality exported"
    );
    assert!(
        original_authority_carriers.iter().any(|carrier| {
            carrier["actual_observer"] == serde_json::json!(&primary)
                && carrier["actual_started"]["facts"]["target_node_id"]
                    == serde_json::json!(&follower)
                && carrier["exact_prepared_frame"].as_bool() == Some(true)
                && carrier["expected_assignment_matches"].as_bool() == Some(true)
                && carrier["actual_command_source_matches"].as_bool() == Some(true)
                && carrier["actual_same_attempt_terminal"]
                    .as_array()
                    .is_some_and(|terminals| !terminals.is_empty())
        }),
        "actual original authority carrier to the isolated follower must join the exact native body/frame and its own terminal; no background-fault substitution"
    );
    let (_, created) = match result {
        Ok(DfsBrowserMutationAttempt::Complete(actual)) => actual,
        other => panic!("original never completed; actual stage exported: {other:#?}"),
    };
    let created =
        created.expect("fresh surviving-pair create must commit; original failure exported");
    assert!(original_elapsed <= sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    assert_eq!(after_assignment.unwrap(), Some(assignment.clone()));
    assert!(
        !failed_routes.is_empty(),
        "partition must intercept an actual original-phase native carrier"
    );
    assert_eq!(created.object_id, expected_object);
    assert_eq!(created.version, expected_version);
    assert_eq!(
        created.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    let after = after.unwrap();
    let current = after
        .tables
        .objects
        .iter()
        .find(|object| object.path == PATH)
        .unwrap();
    assert_eq!(current.object_id, expected_object);
    assert_eq!(current.version, expected_version);
    let intents = after
        .tables
        .intents
        .iter()
        .filter(|intent| {
            intent.object_id == expected_object
                && intent.operation_id.as_deref() == Some(operation.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        intents.len(),
        1,
        "one actual original user intent, not replacement writes"
    );
    let intent = intents[0];
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    assert_eq!(intent.policy.min_commit_replicas, 2);
    assert_eq!(intent.policy.desired_replicas, 3);
    assert_eq!(
        intent
            .desired_replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_nodes
    );
    assert_eq!(intent.target_version, expected_version);
    assert_eq!(intent.content_hash.as_deref(), Some(expected_hash.as_str()));
    let receipts = after
        .tables
        .receipts
        .iter()
        .filter(|receipt| {
            receipt.receipt.object_id == expected_object
                && receipt.receipt.committed_version == expected_version
                && receipt.content_hash.as_deref() == Some(expected_hash.as_str())
        })
        .map(|receipt| receipt.replica_node_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(receipts, survivors);
    for (_, read) in physical {
        let (manifest, bytes) = read.unwrap();
        assert_eq!(manifest.content_hash, expected_hash);
        assert_eq!(manifest.size_bytes, BYTES.len() as u64);
        assert_eq!(bytes, BYTES);
    }
    let queue_cut = queue_cut.unwrap();
    assert!(
        queue_cut.records.iter().any(|row| {
            row.intent_id == intent.intent_id
                && row.object_id == expected_object
                && row.version == expected_version
                && row.content_hash == expected_hash
                && row.missing_replica_node_id == follower
        }),
        "the exact named missing N target must already have durable ownership"
    );
    let original = after
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| record.operation_id == format!("write:{}", intent.intent_id))
        .unwrap();
    assert_eq!(original.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(original.terminal_intent.as_ref(), Some(intent));
    let later = fresh_cut(&native, &primary, &assignment).unwrap();
    let same = later
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| record.digest == original.digest)
        .unwrap();
    assert_eq!(
        same.payload, original.payload,
        "read-only observation must retain the complete original body"
    );
    drop(partition);
    drop(backend);
    native.close().unwrap();
}

#[cfg(test)]
mod filesystem_projection;
