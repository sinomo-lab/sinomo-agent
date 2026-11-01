//! Real POSIX history, actual three-member role rotation and one fresh write.
//! Observations carry no history, cursor, authority, response or clock input.
use super::*;
use serde_json::{Value, json};

struct RuntimeOwner(Option<crate::test_runtime::DfsTestRuntime>);
impl RuntimeOwner {
    fn get(&self) -> &crate::test_runtime::DfsTestRuntime {
        self.0.as_ref().unwrap()
    }
}
impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        if let Some(native) = self.0.take() {
            let result = native.close();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}
#[derive(serde::Serialize)]
struct Cut {
    prefix: DfsMetadataPrefixProof,
    freeze: Option<crate::DfsMetadataShardWriteFreeze>,
    outgoing_cursors: BTreeMap<NodeId, u64>,
    path_object: Option<DfsObjectMetadata>,
    tables: DfsMetadataTableSnapshot,
}
fn cut(
    native: &crate::test_runtime::DfsTestRuntime,
    node: &NodeId,
    shard: &DfsMetadataShardConfig,
    path: &str,
) -> Cut {
    let id = DfsMetadataShardId::new(&shard.shard_id);
    let mut reader = LocalSobjMetadataBackend::open_for_inspection(
        id.clone(),
        crate::local_dfs_metadata_shard_path(native.data_dir(node), &id),
    )
    .unwrap();
    reader.begin_inspection_snapshot().unwrap();
    let result = Cut {
        prefix: DfsMetadataBackend::metadata_prefix_proof(&reader, &id)
            .unwrap()
            .unwrap(),
        freeze: reader.metadata_shard_write_freeze(&id).unwrap(),
        outgoing_cursors: std::iter::once(&shard.primary_node_id)
            .chain(&shard.replica_node_ids)
            .map(|peer| {
                (
                    peer.clone(),
                    reader.metadata_replication_cursor(peer, &id).unwrap(),
                )
            })
            .collect(),
        path_object: reader
            .get_object_by_path_on_shard(&id, &shard.file_system_id, path)
            .unwrap(),
        tables: reader.export_shard_table_snapshot(&id).unwrap().unwrap(),
    };
    reader.end_inspection_snapshot();
    result
}
fn save(native: &crate::test_runtime::DfsTestRuntime, owner: &NodeId, name: &str, value: Value) {
    std::fs::write(
        native.data_dir(owner).root().join(name),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
}
fn run(large: bool) {
    let name = if large {
        "native-promoted-real-long-history"
    } else {
        "native-promoted-real-small-history"
    };
    let parent = format!("/services/{name}");
    let path = format!("{parent}/first-after-promotion.txt");
    let bytes = b"one actual fresh user write after real role rotation";
    let (native, initial, original) =
        crate::test_support::current_native_metadata_service(name, 3, &parent).unwrap();
    drop(initial);
    let owner = RuntimeOwner(Some(native));
    let native = owner.get();
    let old = original.primary_node_id.clone();
    let promoted = original.replica_node_ids[0].clone();
    let requester = original
        .replica_node_ids
        .iter()
        .find(|node| **node != promoted)
        .unwrap()
        .clone();
    let members = std::iter::once(old.clone())
        .chain(original.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(members.len(), 3);
    // Actual requester handle is opened while healthy; a generic cold-browser
    // startup is a separate contract from this exact authority/cursor control.
    let mut foreign = native.client(&requester).unwrap();
    let mut writer = native.client(&old).unwrap();
    writer.posix_mkdir_all(&parent).unwrap();
    let first = cut(native, &old, &original, &path).prefix;
    let goal = if large {
        DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT as u64 + 16
    } else {
        0
    };
    let setup_started = Instant::now();
    let setup_deadline = setup_started + Duration::from_secs(60);
    let mut seed_results = Vec::new();
    let mut current = first.clone();
    let mut seed_error = None;
    for index in 0..600 {
        if current
            .journal_sequence
            .saturating_sub(first.journal_sequence)
            >= goal
        {
            break;
        }
        if setup_deadline.saturating_duration_since(Instant::now())
            < sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
        {
            seed_error =
                Some("real history setup lacks another complete normal action budget".to_owned());
            break;
        }
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let previous = writer.transport.set_operation_deadline(Some(deadline));
        let result = writer.posix_set_mode(&parent, if index % 2 == 0 { 0o700 } else { 0o750 });
        writer.transport.set_operation_deadline(previous);
        let failed = result.is_err();
        seed_results.push(format!("{result:?}"));
        if failed {
            seed_error = Some("actual POSIX history producer failed".to_owned());
            break;
        }
        current = cut(native, &old, &original, &path).prefix;
        if index % 16 == 15 {
            native.checkpoint_rpc_observation_window().unwrap();
        }
    }
    save(
        native,
        &old,
        "promoted-cursor-seed.json",
        json!({
            "qualification":"actual namespace-only chmod producers; no supplied journal/frame/cursor/ACK",
            "large":large,"initial_prefix":first,"actual_prefix":current,"requested_additional_frames":goal,
            "producer_results":seed_results,"setup_error":seed_error,"setup_elapsed_ms":setup_started.elapsed().as_millis(),
        }),
    );
    assert!(seed_error.is_none());
    assert!(
        current
            .journal_sequence
            .saturating_sub(first.journal_sequence)
            >= goal,
        "the actual large-history prerequisite must be reached by real POSIX producers"
    );
    drop(writer);
    let control = native.shared_control_plane().unwrap();
    let active_result = native.reassign_metadata_shard(
        &original.file_system_id,
        &original.shard_id,
        promoted.clone(),
        members
            .iter()
            .filter(|node| **node != promoted)
            .cloned()
            .collect(),
    );
    let actual_proof = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard_membership_handoff_proof(
            &original.file_system_id,
            &original.shard_id,
        )
    });
    save(
        native,
        &old,
        "promoted-cursor-handoff.json",
        json!({
            "original":original,"actual_assignment_result":active_result,"actual_completed_proof":actual_proof.as_ref().map_err(|error| error.to_string()),
            "qualification":"ordinary cooperative freeze/joint activation; original proposal watermark is not substituted for its actual frozen native prefix",
        }),
    );
    let active = active_result.unwrap();
    let proof = actual_proof
        .unwrap()
        .expect("actual stored completed role proof");
    assert_eq!(proof.retiring_shard, original);
    assert_eq!(proof.desired_shard, active);
    assert_eq!(active.primary_node_id, promoted);
    assert_eq!(
        std::iter::once(active.primary_node_id.clone())
            .chain(active.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>(),
        members
    );
    assert!(
        crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
            &original,
            &active,
            &proof.acknowledged_node_ids.iter().cloned().collect()
        )
    );
    let before = members
        .iter()
        .map(|node| (node.clone(), cut(native, node, &active, &path)))
        .collect::<Vec<_>>();
    let prefix_qualified = before.iter().all(|(_, cut)| {
        cut.prefix.journal_sequence == proof.journal_sequence
            && cut.prefix.digest == proof.state_fingerprint
    });
    let retired = &before
        .iter()
        .find(|(node, _)| *node == old)
        .unwrap()
        .1
        .freeze;
    save(
        native,
        &old,
        "promoted-cursor-before-first-write.json",
        json!({
            "actual_completed_proof":proof,"all_actual_native_cuts":before,"exact_frozen_prefix_on_all3":prefix_qualified,
            "qualification":"all-online native physical cuts, actual advisory cursors retained verbatim; no cursor reset or new namespace effect",
        }),
    );
    assert!(
        prefix_qualified,
        "all3 must already hold the exact actual frozen prefix"
    );
    let retired = retired
        .as_ref()
        .expect("actual old writer retains its native activated fence");
    assert_eq!(retired.handoff_id, proof.handoff_id);
    assert_eq!(retired.activated_epoch, Some(active.epoch));
    native.checkpoint_rpc_observation_window().unwrap();
    let diagnostics = members
        .iter()
        .map(|node| {
            (
                node.clone(),
                crate::native_boundary_diagnostics::capture_all_operations(
                    native.data_dir(node).root(),
                ),
            )
        })
        .collect::<Vec<_>>();
    let traces = members
        .iter()
        .map(|node| {
            (
                node.clone(),
                crate::native_shard_trace::capture_native_shard_trace(
                    &native.data_dir(node).root(),
                    DfsMetadataShardId::new(&active.shard_id),
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let deadline = started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let previous = foreign.transport.set_operation_deadline(Some(deadline));
    // ONE ordinary fresh write. The default5s action budget is installed once;
    // no semantic retry, manual maintenance drive or caller resend follows.
    let result = foreign.posix_write_file(
        &path,
        bytes,
        DfsWriteOptions::replicated_eventual(vec![promoted.clone()]),
    );
    foreign.transport.set_operation_deadline(previous);
    let elapsed = started.elapsed();
    let after = members
        .iter()
        .map(|node| (node.clone(), cut(native, node, &active, &path)))
        .collect::<Vec<_>>();
    let actual = &after.iter().find(|(node, _)| *node == promoted).unwrap().1;
    let own = actual
        .tables
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record
                .payload
                .terminal_intent
                .as_ref()
                .is_some_and(|intent| {
                    intent
                        .object_metadata
                        .as_ref()
                        .is_some_and(|object| object.path == path)
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    let pipeline = actual
        .tables
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record
                .payload
                .prepared_intent
                .as_ref()
                .into_iter()
                .chain(record.payload.terminal_intent.as_ref())
                .any(|intent| {
                    intent
                        .object_metadata
                        .as_ref()
                        .is_some_and(|object| object.path == path)
                })
                || record.payload.metadata_effects.iter().any(|effect| {
                    matches!(effect,
                DfsPreparedMetadataEffect::PutIntent { intent }
                    if intent.object_metadata.as_ref().is_some_and(|object| object.path == path))
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    let subjects = pipeline
        .iter()
        .flat_map(|record| {
            record
                .payload
                .prepared_intent
                .iter()
                .chain(record.payload.terminal_intent.iter())
                .chain(record.payload.metadata_effects.iter().filter_map(|effect| {
                    if let DfsPreparedMetadataEffect::PutIntent { intent } = effect {
                        Some(intent)
                    } else {
                        None
                    }
                }))
        })
        .map(|intent| {
            (
                intent.intent_id.clone(),
                intent.object_id.clone(),
                intent.target_version.clone(),
                intent.operation_id.clone(),
                intent.content_hash.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut raw = Vec::new();
    for (node, trace) in &traces {
        trace.close();
        let mut encoded = Vec::new();
        trace.export_raw(&mut encoded).unwrap();
        std::fs::write(
            native
                .data_dir(node)
                .root()
                .join("promoted-cursor-native-trace.json"),
            &encoded,
        )
        .unwrap();
        raw.push((
            node.clone(),
            serde_json::from_slice::<Value>(&encoded).unwrap(),
        ));
    }
    let mut diagnostic_rows = Vec::new();
    for (node, capture) in &diagnostics {
        capture.dump(
            "promoted-cursor-first-write",
            json!({"actual_result":format!("{result:?}")}),
        );
        let value: Value = serde_json::from_slice(
            &std::fs::read(
                native
                    .data_dir(node)
                    .root()
                    .join("boundary-diagnostic-promoted-cursor-first-write.json"),
            )
            .unwrap(),
        )
        .unwrap();
        diagnostic_rows.push((node.clone(), value));
    }
    let mut accepted_targets = BTreeSet::new();
    let mut exact_carriers = Vec::new();
    for (observer, value) in &raw {
        if *observer != promoted {
            continue;
        }
        let packets = value["boundary_packets"].as_array().unwrap();
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
                            DfsNetworkMessage::MetadataJournalBatch(value) => Some(value),
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
            if !own.iter().any(|record| {
                probe.operation
                    == DfsMetadataAuthorityOperation::PreparedOperation {
                        operation_id: record.digest.operation_id.clone(),
                        file_system_id: record.digest.file_system_id.clone(),
                        shard_id: record.digest.shard_id.clone(),
                        owner_epoch: record.digest.owner_epoch,
                        prepare_sequence: record.digest.prepare_sequence,
                        payload_digest: record.digest.payload_digest.clone(),
                    }
            }) {
                continue;
            }
            let Some(expected) = metadata_authority_probe_confirmation_id(probe) else {
                continue;
            };
            for ack in packets {
                let accepted = &ack["facts"];
                if accepted["phase"] != "source-journal-response-accepted" {
                    continue;
                }
                let Ok(response) = serde_json::from_value::<DfsMetadataJournalBatchResponse>(
                    accepted["response"].clone(),
                ) else {
                    continue;
                };
                let Ok(target) =
                    serde_json::from_value::<NodeId>(accepted["expected_target_node_id"].clone())
                else {
                    continue;
                };
                if accepted["authenticated_source_node_id"] == json!(&target)
                    && response.request_id == command.request_id
                    && response.shard_id == command.batch.shard_id
                    && response.next_after_sequence == command.batch.next_after_sequence
                    && !response.verified_prefix_only
                    && response.result == DfsMetadataJournalBatchResult::Applied
                    && response.verified_authority_probe_id.as_deref() == Some(expected.as_str())
                    && accepted["expected_confirmation_id"] == json!(&expected)
                    && active.replica_node_ids.contains(&target)
                {
                    accepted_targets.insert(target.clone());
                    exact_carriers.push(json!({"actual_start":packet,"actual_accepted":ack,"expected_nonce_prefix":expected,"target":target}));
                }
            }
        }
    }
    let zero_selected = diagnostic_rows
        .iter()
        .filter(|(node, _)| *node == promoted)
        .flat_map(|(_, value)| value["events"].as_array().unwrap())
        .filter(|event| event["kind"] == "participant-final-result")
        .filter(|event| {
            let Ok(operation) = serde_json::from_value::<DfsMetadataAuthorityOperation>(
                event["facts"]["operation"].clone(),
            ) else {
                return false;
            };
            pipeline.iter().any(|record| {
                operation
                    == DfsMetadataAuthorityOperation::PreparedOperation {
                        operation_id: record.digest.operation_id.clone(),
                        file_system_id: record.digest.file_system_id.clone(),
                        shard_id: record.digest.shard_id.clone(),
                        owner_epoch: record.digest.owner_epoch,
                        prepare_sequence: record.digest.prepare_sequence,
                        payload_digest: record.digest.payload_digest.clone(),
                    }
            })
        })
        .flat_map(|event| {
            event["facts"]["actual_batch_selections"]
                .as_array()
                .into_iter()
                .flatten()
        })
        .any(|selection| selection["replicated_through_sequence"].as_u64() == Some(0));
    let physical = own
        .iter()
        .flat_map(|record| record.payload.terminal_intent.as_ref())
        .flat_map(|intent| {
            intent.desired_replica_node_ids.iter().map(|node| {
                let read = LocalContentStore::open_for_inspection(
                    native.data_dir(node).dfs_dir().join("content"),
                )
                .read_object_version(intent.object_id.as_str(), intent.target_version.as_str())
                .map_err(|error| error.to_string());
                (node.clone(), read)
            })
        })
        .collect::<Vec<_>>();
    save(
        native,
        &old,
        "promoted-cursor-first-write.json",
        json!({
            "qualification":"actual captured carrier/nonces/native outcomes; no authority supplied; an old-source PASS stays PASS",
            "large":large,"actual_first_result":format!("{result:?}"),"elapsed_nanos":elapsed.as_nanos().to_string(),
            "default_action_budget_nanos":sinomo_core::network::DEFAULT_NETWORK_TIMEOUT.as_nanos().to_string(),
            "actual_completed_handoff":proof,"before":before,"after":after,"own_native_final_bodies":own,"own_native_pipeline_bodies":pipeline,"actual_original_subjects":subjects,
            "actual_exact_authority_carriers":exact_carriers,"accepted_foreign_members":accepted_targets,
            "actual_zero_cursor_used_at_selection":zero_selected,
        "zero_cursor_specific_coverage_limit": if zero_selected {
            "actual zero scheduling cursor selected for this exact original pipeline"
        } else {
            "zero-specific prerequisite unreached; functional long-history write is tested with the actual observed cursor; no zero-path or six-node Docker-under-load claim"
        },"actual_physical_bytes":physical,
            "native_trace_quality":raw.iter().map(|(node,value)|json!({"node":node,"dropped":value["dropped"],
                "in_flight":value["in_flight"],"incomplete":value["incomplete"]})).collect::<Vec<_>>(),
            "diagnostic_quality":diagnostic_rows.iter().map(|(node,value)|json!({"node":node,"dropped":value["dropped"]})).collect::<Vec<_>>(),
            "rpc_window":format!("{:#?}",native.rpc_observations()),"omitted_rpc_observations":native.observation_dropped_count(),
            "uncertainty_limit":"failure or absent head is not NotApplied; no later commit is attributed to the original handler without invocation evidence",
        }),
    );
    result.expect("the original one fresh write must finish under its unchanged budget");
    assert!(elapsed <= sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    assert_eq!(own.len(), 1);
    assert_eq!(
        subjects.len(),
        1,
        "one original semantic write must retain one actual native intent/object/target/operation/hash tuple"
    );
    let terminal = own[0].payload.terminal_intent.as_ref().unwrap();
    assert_eq!(terminal.policy.min_commit_replicas, 1);
    assert_eq!(terminal.policy.desired_replicas, 1);
    assert_eq!(own[0].status, DfsPreparedOperationStatus::Committed);
    assert_eq!(
        accepted_targets,
        active.replica_node_ids.iter().cloned().collect()
    );
    assert!(
        raw.iter()
            .all(|(_, value)| value["dropped"].as_str() == Some("0"))
    );
    assert!(
        diagnostic_rows
            .iter()
            .all(|(_, value)| value["dropped"].as_u64() == Some(0))
    );
    assert_eq!(native.observation_dropped_count(), 0);
    assert_eq!(
        physical.len(),
        1,
        "actual committed N1 plan supplies the physical target; candidate hints are not native target authority"
    );
    for (_, result) in physical {
        let (manifest, actual_bytes) = result.unwrap();
        assert_eq!(manifest.object_id, terminal.object_id.as_str());
        assert_eq!(manifest.version, terminal.target_version.as_str());
        assert_eq!(
            terminal.content_hash.as_deref(),
            Some(manifest.content_hash.as_str())
        );
        assert_eq!(actual_bytes, bytes);
    }
    native.checkpoint_rpc_observation_window().unwrap();
    native.assert_fault_budgets().unwrap();
    drop(traces);
    drop(diagnostics);
    drop(control);
    drop(foreign);
    drop(owner);
}
#[test]
fn native_all_online_promoted_primary_first_write_small_history() {
    run(false);
}
#[test]
fn native_all_online_promoted_primary_first_write_after_long_native_history() {
    run(true);
}
