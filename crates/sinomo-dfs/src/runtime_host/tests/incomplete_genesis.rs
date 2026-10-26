//! Live P0 reproduction. Three native nodes; this new scope is assigned {A}.
use super::*;
use crate::test_runtime::{DfsTestRuntime, RpcObservation, RpcPhase};
use crate::{
    DfsDistributedWriteResult, DfsMessageCommand, DfsMessageResponse, DfsMetadataJournalEntry,
    DfsNamespaceResult, DfsPreparedOperationStatus, NetworkedDfsWriteOutcome,
};
use serde::Serialize;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug)]
struct NativeCall {
    response: Result<DfsMessageResponse, String>,
    elapsed: Duration,
    expired: bool,
}

fn native_call(
    native: &DfsTestRuntime,
    source: &NodeId,
    command: &DfsMessageCommand,
    payload: Option<&[u8]>,
) -> NativeCall {
    let started = Instant::now();
    let deadline = started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    // Registry lookup only: reuse the real registered owner, never a Core
    // readiness constructor or a fabricated signer/projection.
    let host = registered_dfs_runtime_host(native.data_dir(source))
        .unwrap()
        .unwrap();
    let rpc = if let Some(bytes) = payload {
        let DfsNetworkMessage::DistributedWriteRequest(write) = &command.routed_message.message
        else {
            panic!("write attachment requires its actual write head")
        };
        assert!(write.bytes.is_empty());
        assert_eq!(
            write.content_hash.as_deref(),
            Some(crate::content_hash(bytes).as_str())
        );
        assert_eq!(write.content_size_bytes, Some(bytes.len() as u64));
        let (actual, rpc) = crate::dfs_routed_call_rpc_call(crate::DfsRoutedCall::with_attachment(
            command.routed_message.clone(),
            crate::DfsOutboundAttachment {
                descriptor: crate::DfsRpcAttachmentDescriptor {
                    total_size_bytes: bytes.len() as u64,
                    content_hash: write.content_hash.clone(),
                },
                source: sinomo_core::network::ObjectRpcBodySource::from_reader(
                    std::io::Cursor::new(bytes.to_vec()),
                    Some(bytes.len() as u64),
                ),
            },
        ))
        .unwrap();
        assert_eq!(
            &actual, command,
            "canonical transport identity is fixed for both original calls"
        );
        rpc
    } else {
        crate::dfs_message_rpc_call(command).unwrap()
    };
    let pending = host.object_rpc.call_async(
        &command.routed_message.target_node_id,
        rpc,
        crate::DfsObjectRpcSendOptions::default()
            .with_overall_timeout(deadline.saturating_duration_since(Instant::now()))
            .cluster_rpc_options(),
        true,
    );
    let response = native.runtime_handle().block_on(async {
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            let reply = pending.await.map_err(|error| error.to_string())?;
            let value = reply
                .into_materialized_value_response(2 * 1024 * 1024)
                .await
                .map_err(|error| error.to_string())?;
            crate::decode_dfs_message_rpc_response(&value).map_err(|error| error.to_string())
        })
        .await
        .map_err(|_| "original five-second native request timed out".to_string())?
    });
    let elapsed = started.elapsed();
    let expired = Instant::now() > deadline
        || response.as_ref().err().is_some_and(|error| {
            error.contains("timed out")
                || error.contains("timeout")
                || error.contains("deadline expired")
        });
    NativeCall {
        response,
        elapsed,
        expired,
    }
}

fn command(source: &NodeId, message: DfsNetworkMessage, frame: u64) -> DfsMessageCommand {
    DfsMessageCommand {
        request_id: format!("incomplete-genesis-native-{frame}"),
        routed_message: crate::build_dfs_routed_message(source, source, frame, frame, message),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ActualFrame {
    entry: DfsMetadataJournalEntry,
    bytes: Vec<u8>,
}

fn actual_frames(root: &Path) -> Result<Vec<ActualFrame>, String> {
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let guard = crate::metadata_lock::try_metadata_inspection_lock(
            root.to_owned(),
            crate::metadata_cursor::journal_write_lock_path(root),
        )
        .map_err(|e| e.to_string())?;
        if let Some(_guard) = guard {
            let entries = crate::metadata_journal::durable_journal_entries_since_limited(
                root,
                0,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            )
            .map_err(|e| e.to_string())?;
            if entries.len() >= DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT {
                return Err(
                    "bounded native reproduction cannot observe its complete frame prefix".into(),
                );
            }
            let frames = crate::metadata_journal::read_segmented_journal_frames_at_sequences(
                root,
                &entries
                    .iter()
                    .map(|entry| entry.sequence)
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.to_string())?;
            let result = entries
                .into_iter()
                .zip(frames)
                .map(|(entry, frame)| {
                    let frame = frame.ok_or("actual frame unavailable")?;
                    if frame.entry() != &entry {
                        return Err("actual frame body differs from durable entry".into());
                    }
                    Ok(ActualFrame {
                        entry,
                        bytes: frame.exact_bytes().to_vec(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            if Instant::now() > deadline {
                return Err("actual frame inspection exceeded its original5s".into());
            }
            return Ok(result);
        }
        if Instant::now() >= deadline {
            return Err("actual frame inspection busy at its original5s".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn legacy_markers(
    native: &DfsTestRuntime,
    nodes: &[NodeId],
    shards: &[DfsMetadataShardConfig],
) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let mut result = BTreeMap::new();
    for node in nodes {
        for shard in shards {
            let path = crate::local_dfs_metadata_shard_path(
                native.data_dir(node),
                &DfsMetadataShardId::new(&shard.shard_id),
            )
            .join("journal/membership-handoff-write-freeze.sobj");
            let bytes = match fs::read(&path) {
                Ok(bytes) => Some(bytes),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("legacy marker unavailable {}: {error}", path.display()),
            };
            result.insert(path, bytes);
        }
    }
    result
}

fn attribute_setup_rebinds(
    before: &BTreeMap<PathBuf, Option<Vec<u8>>>,
    after: &BTreeMap<PathBuf, Option<Vec<u8>>>,
) -> serde_json::Value {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    let mut changes = Vec::new();
    for (path, old) in before {
        let new = &after[path];
        if old == new {
            continue;
        }
        let old: crate::DfsMetadataShardWriteFreeze = sinomo_core::object::serde_ext::from_bytes(
            old.as_ref()
                .expect("setup cannot introduce a cooperative marker"),
        )
        .unwrap();
        let mut new: crate::DfsMetadataShardWriteFreeze =
            sinomo_core::object::serde_ext::from_bytes(
                new.as_ref()
                    .expect("setup cannot remove a cooperative marker"),
            )
            .unwrap();
        let observed = new.clone();
        assert!(
            old.activated_epoch.is_some(),
            "only an actual activated setup checkpoint may advance"
        );
        assert!(
            new.rebind_prefix_proof.journal_sequence >= old.rebind_prefix_proof.journal_sequence
        );
        let root = path.parent().unwrap().parent().unwrap();
        let id = DfsMetadataShardId::new(root.file_name().unwrap().to_str().unwrap());
        let cut = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let current = loop {
            if let Some(_guard) = crate::metadata_lock::try_metadata_inspection_lock(
                root.to_owned(),
                crate::metadata_cursor::journal_write_lock_path(root),
            )
            .unwrap()
            {
                break crate::metadata_journal::admission::current_owned_materialized_prefix(
                    root, &id,
                )
                .unwrap()
                .expect("actual setup owner prefix must remain observable");
            }
            assert!(
                Instant::now() < cut,
                "setup checkpoint inspection keeps its5s bound"
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(
            new.rebind_prefix_proof, current,
            "setup change must name the actual owned materialized prefix"
        );
        new.rebind_prefix_proof = old.rebind_prefix_proof.clone();
        assert_eq!(
            new, old,
            "setup may not rewrite original cooperative identity/cut/custody"
        );
        changes.push(serde_json::json!({"path":path,"before":old,"after":observed,"actual_owned_prefix":current,
            "attribution":"setup catch-up checkpoint, not a probe effect or authority certificate"}));
    }
    serde_json::Value::Array(changes)
}

fn wire_facts(observations: &[RpcObservation]) -> serde_json::Value {
    serde_json::Value::Array(observations.iter().map(|item|serde_json::json!({
        "call_id":item.call_id,"source":item.source,"target":item.target,"route":item.route,
        "phase":format!("{:?}",item.phase),"origin":format!("{:?}",item.origin),
        "request":item.dfs_request_id,"object":item.dfs_object_id,"version":item.dfs_version,
        "intent":item.dfs_intent_id,"error":item.outcome_error,"streamed_value":item.streamed_value,
        "body_bytes":item.body_bytes,
    })).collect())
}

fn content_facts(
    stores: &[(NodeId, crate::LocalContentStore)],
    object: &DfsObjectId,
    version: &DfsVersion,
    hash: &str,
) -> serde_json::Value {
    serde_json::Value::Array(stores.iter().map(|(node,store)| {
        // A cold/error result is unavailable, never global absence or authority.
        let manifest=store.read_manifest(object.as_str(),version.as_str()).map(|m|
            serde_json::json!({"hash":m.content_hash,"size":m.size_bytes})).map_err(|e|e.to_string());
        let blob=store.read_blob(hash).map(|bytes|serde_json::json!({"hash":crate::content_hash(&bytes),"size":bytes.len()}))
            .map_err(|e|e.to_string());
        serde_json::json!({"node":node,"manifest":manifest,"blob":blob,"errors_mean":"unavailable, not absence"})
    }).collect())
}

fn effect_routes(observations: &[RpcObservation], source: &NodeId) -> BTreeSet<u64> {
    observations
        .iter()
        .filter(|item| {
            item.source == *source
                && item.target != *source
                && [
                    crate::DFS_DATA_STAGE_BLOB_ROUTE,
                    crate::DFS_DATA_STAGE_PATCH_ROUTE,
                    crate::DFS_DATA_COMMIT_VERSION_ROUTE,
                ]
                .contains(&item.route.as_str())
                && matches!(
                    item.phase,
                    RpcPhase::BeforeDispatch | RpcPhase::AfterDispatch | RpcPhase::Completed
                )
        })
        .map(|item| item.call_id)
        .collect()
}

fn write_committed(call: &NativeCall, expected: &DfsDistributedWriteCommand) -> bool {
    !call.expired && call.response.as_ref().is_ok_and(|response|response.outbound_messages.iter().any(|frame|
        matches!(&frame.message,DfsNetworkMessage::DistributedWriteResponse(reply)
            if reply.request_id==expected.request_id && reply.object_id==expected.request.object_id
                && matches!(&reply.result,DfsDistributedWriteResult::Outcome {
                    outcome:NetworkedDfsWriteOutcome::Committed {version,content_hash,..}}
                    if version==&expected.request.target_version && content_hash==expected.content_hash.as_ref().unwrap()))))
}

fn refused(call: &NativeCall) -> bool {
    !call.expired
        && match &call.response {
            Err(_) => true,
            Ok(response) => response
                .outbound_messages
                .iter()
                .any(|frame| match &frame.message {
                    DfsNetworkMessage::DistributedWriteResponse(reply) => {
                        matches!(reply.result, DfsDistributedWriteResult::Rejected { .. })
                    }
                    DfsNetworkMessage::NamespaceResponse(reply) => {
                        matches!(reply.result, DfsNamespaceResult::Rejected { .. })
                    }
                    _ => false,
                }),
        }
}

#[test]
fn native_incomplete_singleton_genesis_refuses_stamped_write_and_global_mutation_before_effects() {
    let (mut native, nodes) =
        DfsTestRuntime::start_cluster("native-incomplete-genesis-p0", 3).unwrap();
    let mut report = serde_json::json!({"fixture_nodes":3,"assigned_metadata_nodes":1,
        "scope":"existing singleton genesis, not fixed-three metadata initialization",
        "reopen":"not exercised: normal reopen may refuse readiness while hold is active; no SIGKILL or fresh adoption",
        "quietness":"explicit operation activity; no quiet-window/writeback proof"});
    let mut final_captures = Vec::new();
    let mut evidence_root = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let primary = native.root_shard().unwrap().primary_node_id;
        evidence_root = Some(
            native
                .data_dir(&primary)
                .root()
                .parent()
                .unwrap()
                .to_owned(),
        );
        let prefix = crate::sifs_node_path(&primary);
        let write_path = format!("{prefix}/incomplete-write.bin");
        let rename_source = "/services/incomplete-genesis-rename-source";
        let rename_target = format!("{prefix}/incomplete-renamed");
        let mut setup = native.client(&primary).unwrap();
        assert!(setup.transport.delegates_local_namespace_authority());
        let setup_cut = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let previous = setup.set_transport_operation_deadline(Some(setup_cut));
        let setup_result = setup.posix_mkdir(rename_source);
        setup.set_transport_operation_deadline(previous);
        setup_result.unwrap();
        assert!(Instant::now() <= setup_cut);
        drop(setup);
        let legacy = native.metadata_shards().unwrap();
        let setup_markers = legacy_markers(&native, &nodes, &legacy);
        let host = registered_dfs_runtime_host(native.data_dir(&primary))
            .unwrap()
            .unwrap();
        let stores = nodes
            .iter()
            .map(|node| {
                let client = native.client(node).unwrap();
                (node.clone(), client.content().clone())
            })
            .collect::<Vec<_>>();
        final_captures = nodes
            .iter()
            .map(|node| native.capture_served_work(node, 32).unwrap())
            .collect::<Vec<_>>();
        let phase = native
            .begin_deferred_native_genesis("incomplete-native-node-scope", &prefix)
            .unwrap();
        let shard = phase.shard().clone();
        assert_eq!(shard.primary_node_id, primary);
        assert!(shard.replica_node_ids.is_empty());
        assert_eq!(shard.epoch, 1);
        let original = phase.original_record().clone();
        let prepared_markers = legacy_markers(&native, &nodes, &legacy);
        let setup_rebinds = attribute_setup_rebinds(&setup_markers, &prepared_markers);
        assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
        assert!(original.committed_entry.is_none());
        assert!(phase.deferral_hits() > 0);
        assert!(
            original
                .payload
                .transaction
                .objects
                .iter()
                .any(|object| object.path == prefix
                    && object.kind == DfsObjectKind::Directory
                    && object.policy == DfsObjectPolicy::ReplicatedEventual),
            "the real genesis body supplies the non-LocalOwned bootstrap parent"
        );
        let bytes = vec![0x5d; 4096];
        let hash = crate::content_hash(&bytes);
        let object = DfsObjectId::generate();
        let target_version = DfsVersion::generate();
        let mut routed = test_dfs_write_routed_message(&primary, &primary, 90, &write_path);
        let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
            unreachable!()
        };
        write.request_id = format!("incomplete-write-{}", sinomo_core::Id::generate());
        write.request = sinomo_core::DfsWriteRequest {
            file_system_id: shard.file_system_id.clone(),
            object_id: object.clone(),
            target_version: target_version.clone(),
            requesting_node_id: primary.clone(),
        };
        write.expected_base_version = None;
        write.bytes.clear();
        write.content_hash = Some(hash.clone());
        write.content_size_bytes = Some(bytes.len() as u64);
        write.requester_blob_pre_staged = false;
        write.desired_replica_nodes = nodes
            .iter()
            .filter(|node| **node != primary)
            .cloned()
            .collect();
        write.policy = DfsReplicationPolicy::new(1, 2).unwrap();
        write.authority_shard = Some(shard.clone());
        write.touch_parent_on_create = true;
        write.foreground_replica_repair = true;
        write.exclusive_create = false;
        let metadata = write.object_metadata.as_mut().unwrap();
        metadata.object_id = object.clone();
        metadata.current_locations.clear();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        let write_body = write.clone();
        let write_command = test_dfs_message_command("dfs_message_90", routed);
        assert!(matches!(
            dfs_message_execution_scope(&write_command),
            DfsMessageExecutionScope::ExactShard { .. }
        ));
        let mut rename = DfsNamespaceCommand {
            request_id: format!("incomplete-rename-{}", sinomo_core::Id::generate()),
            file_system_id: shard.file_system_id.clone(),
            requesting_node_id: primary.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::Rename {
                from: rename_source.into(),
                to: rename_target.clone(),
            },
        };
        rename.authority_shards=crate::NetworkedDfsService::<crate::DfsNodeTransport>::affected_namespace_shards_from_snapshot(
            &shard.file_system_id,&rename.operation,&native.metadata_shards().unwrap()).unwrap();
        assert!(!rename.authority_shards.is_empty());
        let rename_command = command(
            &primary,
            DfsNetworkMessage::NamespaceRequest(rename.clone()),
            91,
        );
        assert!(matches!(
            dfs_message_execution_scope(&rename_command),
            DfsMessageExecutionScope::GlobalExclusive
        ));
        let root = crate::local_dfs_metadata_shard_path(
            native.data_dir(&primary),
            &DfsMetadataShardId::new(&shard.shard_id),
        );
        let before_frames = actual_frames(&root);
        let before_content = content_facts(&stores, &object, &target_version, &hash);
        let before_candidates = stores
            .iter()
            .map(|(node, store)| {
                (
                    node.clone(),
                    store
                        .blob_storage_candidate_exists(&hash)
                        .map_err(|error| error.to_string()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let markers = legacy_markers(&native, &nodes, &legacy);
        let prepared_rebinds = attribute_setup_rebinds(&prepared_markers, &markers);
        let prior = native
            .rpc_observations()
            .into_iter()
            .map(|event| event.call_id)
            .collect::<BTreeSet<_>>();
        let first_write = native_call(&native, &primary, &write_command, Some(&bytes));
        let first_rename = native_call(&native, &primary, &rename_command, None);
        let before_completion = native
            .rpc_observations()
            .into_iter()
            .filter(|event| !prior.contains(&event.call_id))
            .collect::<Vec<_>>();
        let current_original = native.read_deferred_native_genesis(&phase);
        let after_frames = actual_frames(&root);
        let after_content = content_facts(&stores, &object, &target_version, &hash);
        let after_candidates = stores
            .iter()
            .map(|(node, store)| {
                (
                    node.clone(),
                    store
                        .blob_storage_candidate_exists(&hash)
                        .map_err(|error| error.to_string()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let markers_after = legacy_markers(&native, &nodes, &legacy);
        report["original"] = serde_json::to_value(&original).unwrap();
        report["write_command"] = serde_json::to_value(&write_command).unwrap();
        report["rename_command"] = serde_json::to_value(&rename_command).unwrap();
        report["write_payload"] = serde_json::json!({"bytes":bytes,"hash":hash,"size":bytes.len(),"transport":"canonical owned-reader attachment; inline head empty"});
        report["legacy_marker_boundaries"] = serde_json::json!({"after_setup":setup_markers,"after_retained_handles_and_phase":prepared_markers,
            "immediately_before_probes":markers,"immediately_after_probes":markers_after,
            "setup_rebinds":setup_rebinds,"prepared_rebinds":prepared_rebinds});
        report["before_completion"] = serde_json::json!({"write":first_write.response,"write_elapsed":format!("{:?}",first_write.elapsed),
            "rename":first_rename.response,"rename_elapsed":format!("{:?}",first_rename.elapsed),
            "write_expired":first_write.expired,"rename_expired":first_rename.expired,
            "rpc":wire_facts(&before_completion),"content_before":before_content,"content_after":after_content,
            "frames_before":before_frames,"frames_after":after_frames,"readback":current_original,
            "local_hash_candidate_before":before_candidates,"local_hash_candidate_after":after_candidates,
            "content_candidate_scope":"advisory presence in these actual local stores only; no global absence/custody claim",
            "post_timeout_drain":"no non-stopping complete owner-drain API; timeout is FAILED, never no-effect proof"});
        // Collect owner completion and request validity even after an observed
        // unsafe effect/refusal; no early semantic assertion hides that result.
        let completed = native.complete_deferred_native_genesis(&phase);
        report["completion"] = serde_json::to_value(&completed).unwrap();
        let completion_frames = actual_frames(&root);
        report["completion_frames"] = serde_json::to_value(&completion_frames).unwrap();
        let mut valid_write = None;
        let mut valid_rename = None;
        let mut sync = None;
        let mut valid_start = BTreeSet::new();
        if completed.is_ok() {
            let synced = native.sync_completed_native_genesis(&phase, &prefix);
            report["assigned_sync"] = serde_json::to_value(&synced).unwrap();
            if synced.is_ok() {
                valid_start = native
                    .rpc_observations()
                    .into_iter()
                    .map(|event| event.call_id)
                    .collect();
                let current_stamps=crate::NetworkedDfsService::<crate::DfsNodeTransport>::affected_namespace_shards_from_snapshot(
                    &shard.file_system_id,&rename.operation,&native.metadata_shards().unwrap()).unwrap();
                assert_eq!(
                    current_stamps, rename.authority_shards,
                    "the same request body retains its actual authority assignment"
                );
                valid_write = Some(native_call(&native, &primary, &write_command, Some(&bytes)));
                valid_rename = Some(native_call(&native, &primary, &rename_command, None));
            }
            sync = Some(synced);
        }
        report["post_write"] = serde_json::json!(valid_write.as_ref().map(|call| &call.response));
        report["post_rename"] = serde_json::json!(valid_rename.as_ref().map(|call| &call.response));
        let all = native.rpc_observations();
        let post_control = all
            .iter()
            .filter(|event| !valid_start.contains(&event.call_id))
            .cloned()
            .collect::<Vec<_>>();
        report["all_rpc"] = wire_facts(&all);
        report["post_control_rpc"] = wire_facts(&post_control);
        report["served_commands"]=serde_json::json!(final_captures.iter().flat_map(|capture|capture.records()).map(|record|
            serde_json::json!({"command":record.command,"succeeded":record.succeeded,"repair_records":record.repair_record_count})).collect::<Vec<_>>());
        report["post_content"] = content_facts(&stores, &object, &target_version, &hash);
        report["checkpoint"] = serde_json::json!(native.checkpoint_rpc_observation_window());
        let path = native
            .data_dir(&primary)
            .root()
            .parent()
            .unwrap()
            .join("incomplete-genesis-native-control.json");
        let encoded = serde_json::to_vec_pretty(&report).unwrap();
        assert!(
            encoded.len() <= 4 * 1024 * 1024,
            "bounded evidence report overflow"
        );
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(&encoded).unwrap();
        file.sync_all().unwrap();
        eprintln!("INCOMPLETE_GENESIS_NATIVE_REPORT {}", path.display());
        assert_eq!(native.observation_dropped_count(), 0);
        let committed = completed.as_ref().unwrap();
        assert_eq!(committed.digest, original.digest);
        assert_eq!(committed.payload, original.payload);
        assert_eq!(committed.status, DfsPreparedOperationStatus::Committed);
        assert!(committed.committed_entry.is_some());
        let committed_frame = completion_frames
            .as_ref()
            .unwrap()
            .iter()
            .find(|frame| {
                frame.entry.sequence == committed.committed_entry.as_ref().unwrap().sequence
            })
            .unwrap();
        assert_eq!(
            crate::DfsPreparedOperationCommitReference::from_journal_entry(&committed_frame.entry)
                .unwrap(),
            **committed.committed_entry.as_ref().unwrap()
        );
        let certificate = sync.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(certificate.acknowledged_node_ids, vec![primary.clone()]);
        assert_eq!(certificate.target.expected_shard.as_ref(), Some(&shard));
        assert_eq!(
            certificate.quorum_size, 1,
            "full assigned set is singleton, not a fixed3 proof"
        );
        assert!(
            write_committed(valid_write.as_ref().unwrap(), &write_body),
            "same original write shape must be valid after exact owner completion"
        );
        let rename_call = valid_rename.as_ref().unwrap();
        assert!(!rename_call.expired);
        assert!(rename_call.response.as_ref().unwrap().outbound_messages.iter().any(|frame|
            matches!(&frame.message,DfsNetworkMessage::NamespaceResponse(reply) if reply.request_id==rename.request_id && matches!(reply.result,DfsNamespaceResult::Applied))));
        assert!(
            !effect_routes(&post_control, &primary).is_empty(),
            "valid post-completion control must use the real remote content path"
        );
        // Only valid post-completion inputs can support a gate conclusion.
        assert_eq!(
            current_original.as_ref().unwrap(),
            &original,
            "original genesis remains unchanged while incomplete"
        );
        assert_eq!(
            before_frames.as_ref().unwrap(),
            after_frames.as_ref().unwrap(),
            "no premature canonical publication"
        );
        assert_eq!(
            markers_after, markers,
            "incomplete probes preserve all existing cooperative bytes"
        );
        assert!(
            before_candidates
                .values()
                .all(|value| matches!(value, Ok(false))),
            "known test hash's initial local presence must be available and empty; no cold/error absence inference"
        );
        assert!(
            after_candidates
                .values()
                .all(|value| matches!(value, Ok(false))),
            "incomplete request introduced locally observable content staging or made observation unavailable"
        );
        assert!(
            refused(&first_write),
            "actual pre-completion write: {first_write:?}"
        );
        assert!(
            refused(&first_rename),
            "actual pre-completion global mutation: {first_rename:?}"
        );
        assert!(
            effect_routes(&before_completion, &primary).is_empty(),
            "incomplete scope emitted actual content-effect RPCs; retained evidence distinguishes delivery/effect"
        );
        drop(phase);
        drop(stores);
        drop(host);
    }));
    // The normal owner stop/close is the available complete drain. Do not
    // call it SIGKILL or reconstruct native authority from retained files.
    let closed = native.close();
    // These observers retain records/hook registration, never a DATA owner or
    // admission. Read them after actual close joins tasks, including late
    // successful effects; timeout is still a failed/unproven refusal.
    let after_close = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> Result<(), String> {
            let facts = final_captures
                .iter()
                .flat_map(|capture| capture.records())
                .map(|record| {
                    serde_json::json!({"command":record.command,"succeeded":record.succeeded,
                "repair_records":record.repair_record_count})
                })
                .collect::<Vec<_>>();
            let bytes=serde_json::to_vec_pretty(&serde_json::json!({"normal_close":closed,
            "served_after_close":facts,
            "qualification":"successful served work after normal owner close; not full failed-RPC wire coverage, SIGKILL or global absence"}))
            .map_err(|error|error.to_string())?;
            if bytes.len() > 4 * 1024 * 1024 {
                return Err("bounded after-close evidence overflow".into());
            }
            let path = evidence_root
                .as_ref()
                .ok_or("owned evidence root unavailable")?
                .join("incomplete-genesis-after-close.json");
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|error| error.to_string())?;
            file.write_all(&bytes)
                .and_then(|_| file.sync_all())
                .map_err(|error| error.to_string())
        },
    ));
    drop(final_captures);
    if let Err(panic) = result {
        eprintln!("INCOMPLETE_GENESIS_AFTER_CLOSE_DIAGNOSTIC {after_close:?}");
        std::panic::resume_unwind(panic);
    }
    after_close
        .expect("after-close evidence callback must not panic")
        .unwrap();
    closed.unwrap();
}

#[test]
fn native_held_genesis_preserves_unrelated_ready_scope_mutation_and_owner_completion() {
    let (native, nodes) =
        DfsTestRuntime::start_cluster("native-incomplete-genesis-unrelated", 3).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let primary = native.root_shard().unwrap().primary_node_id;
        let prefix = crate::sifs_node_path(&primary);
        let phase = native
            .begin_deferred_native_genesis("held-unrelated-target", &prefix)
            .unwrap();
        let target = phase.shard().clone();
        let original = phase.original_record().clone();
        assert_eq!(target.primary_node_id, primary);
        assert!(target.replica_node_ids.is_empty());
        assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
        assert!(original.committed_entry.is_none());
        let target_root = crate::local_dfs_metadata_shard_path(
            native.data_dir(&primary),
            &DfsMetadataShardId::new(&target.shard_id),
        );
        let target_before = actual_frames(&target_root);
        let configurations = native.metadata_shards().unwrap();
        let path = "/services/ready-mutation-during-held-genesis";
        let operation = DfsNamespaceOperation::Mkdir { path: path.into() };
        let stamps = crate::NetworkedDfsService::<crate::DfsNodeTransport>::affected_namespace_shards_from_snapshot(
            &target.file_system_id, &operation, &configurations,
        ).unwrap();
        assert_eq!(
            stamps.len(),
            1,
            "valid mutation is confined to one existing ready scope"
        );
        let ready = stamps[0].clone();
        assert_ne!(ready.shard_id, target.shard_id);
        assert_eq!(ready.primary_node_id, primary);
        assert_eq!(
            ready.replica_node_ids.len(),
            nodes.len() - 1,
            "ready legacy scope keeps its full assigned set"
        );
        let ready_id = DfsMetadataShardId::new(&ready.shard_id);
        let ready_root = crate::local_dfs_metadata_shard_path(native.data_dir(&primary), &ready_id);
        let ready_before = actual_frames(&ready_root);
        let request = DfsNamespaceCommand {
            request_id: format!("unrelated-ready-{}", sinomo_core::Id::generate()),
            file_system_id: target.file_system_id.clone(),
            requesting_node_id: primary.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: stamps,
            operation,
        };
        let request = command(&primary, DfsNetworkMessage::NamespaceRequest(request), 192);
        assert!(matches!(
            dfs_message_execution_scope(&request),
            DfsMessageExecutionScope::ExactShard { .. }
        ));
        let prior = native
            .rpc_observations()
            .into_iter()
            .map(|event| event.call_id)
            .collect::<BTreeSet<_>>();
        let applied = native_call(&native, &primary, &request, None);
        let read_cut = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let actual_object = loop {
            let observed = crate::local_state::with_current_owned_dfs_shard_inspection(
                native.data_dir(&primary),
                &ready_id,
                |backend| {
                    backend
                        .get_object_by_path(&ready.file_system_id, path)
                        .map_err(crate::DfsError::from)
                },
            );
            match observed {
                Ok(Some(Some(object))) => break Ok(object),
                Err(error) => break Err(error.to_string()),
                Ok(_) => {}
            }
            if Instant::now() >= read_cut {
                break Err("actual ready-scope object unavailable within its5s read window".into());
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        let ready_after = actual_frames(&ready_root);
        let target_after = actual_frames(&target_root);
        let still_prepared = native.read_deferred_native_genesis(&phase);
        let target_config_after = native.metadata_shards().map(|shards| {
            shards
                .into_iter()
                .find(|shard| shard.shard_id == target.shard_id)
        });
        let wire = native
            .rpc_observations()
            .into_iter()
            .filter(|event| !prior.contains(&event.call_id))
            .collect::<Vec<_>>();
        // Ready-scope semantic progress is intentional and outside the other
        // reproduction's fixture, marker baseline and target-effect window.
        // Completion remains the SAME explicit owner operation, not a write
        // offered as a target-readiness probe.
        let completed = native.complete_deferred_native_genesis(&phase);
        let synced = completed
            .as_ref()
            .ok()
            .map(|_| native.sync_completed_native_genesis(&phase, &prefix));
        let facts = serde_json::json!({
            "scope":"three native nodes; held target assigned{A}; unrelated ready legacy scope unchanged",
            "target":target,"ready_scope":ready,"request":request,"actual_result":applied.response,
            "elapsed":format!("{:?}",applied.elapsed),"expired":applied.expired,"actual_object":actual_object,
            "ready_frames_before":ready_before,"ready_frames_after":ready_after,
            "target_original":original,"target_readback_while_held":still_prepared,
            "target_frames_before":target_before,"target_frames_after":target_after,
            "target_cfg_while_held":target_config_after,"rpc":wire_facts(&wire),
            "same_genesis_completion":completed,"ordinary_assigned_sync":synced,
            "quietness":"explicit semantic progress, not a quiet window",
        });
        let bytes = serde_json::to_vec_pretty(&facts).unwrap();
        assert!(
            bytes.len() <= 4 * 1024 * 1024,
            "bounded native evidence overflow"
        );
        let evidence = native
            .data_dir(&primary)
            .root()
            .parent()
            .unwrap()
            .join("incomplete-genesis-unrelated-ready-scope.json");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&evidence)
            .unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        eprintln!("INCOMPLETE_GENESIS_UNRELATED_REPORT {}", evidence.display());
        assert!(
            !applied.expired,
            "unrelated ready mutation retains original5s bound"
        );
        let DfsNetworkMessage::NamespaceRequest(expected) = &request.routed_message.message else {
            unreachable!()
        };
        assert!(applied.response.as_ref().unwrap().outbound_messages.iter().any(|frame|
            matches!(&frame.message,DfsNetworkMessage::NamespaceResponse(reply)
                if reply.request_id==expected.request_id && matches!(reply.result,DfsNamespaceResult::Applied))),
            "held target must not impose a blanket global mutation block");
        let object = actual_object.as_ref().unwrap();
        assert_eq!(object.path, path);
        assert_eq!(object.kind, DfsObjectKind::Directory);
        assert_eq!(object.file_system_id, ready.file_system_id);
        let ready_before = ready_before.as_ref().unwrap();
        let ready_after = ready_after.as_ref().unwrap();
        let old_head = ready_before.last().map_or(0, |frame| frame.entry.sequence);
        assert!(
            ready_after
                .iter()
                .any(|frame| frame.entry.sequence > old_head
                    && matches!(
                        frame.entry.mutation,
                        crate::DfsMetadataMutation::CommitPreparedOperationV7 { .. }
                    )),
            "actual Applied result must have a new canonical ready-scope commit"
        );
        assert_eq!(still_prepared.as_ref().unwrap(), &original);
        assert_eq!(
            still_prepared.as_ref().unwrap().status,
            DfsPreparedOperationStatus::Prepared
        );
        assert_eq!(
            target_before.as_ref().unwrap(),
            target_after.as_ref().unwrap()
        );
        assert_eq!(
            target_config_after.as_ref().unwrap().as_ref(),
            Some(&target)
        );
        let completed = completed.as_ref().unwrap();
        assert_eq!(completed.digest, original.digest);
        assert_eq!(completed.payload, original.payload);
        assert_eq!(completed.status, DfsPreparedOperationStatus::Committed);
        assert!(completed.committed_entry.is_some());
        let synced = synced.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(synced.target.expected_shard.as_ref(), Some(&target));
        assert_eq!(synced.acknowledged_node_ids, vec![primary]);
        assert_eq!(synced.quorum_size, 1);
        assert_eq!(native.observation_dropped_count(), 0);
        drop(phase);
    }));
    let closed = native.close();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    closed.unwrap();
}

#[test]
fn native_cold_owner_preserves_held_genesis_before_stamped_write_effects() {
    let (native, nodes) = DfsTestRuntime::start_cluster("native-cold-held-genesis", 3).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let primary = native.root_shard().unwrap().primary_node_id;
        let prefix = crate::sifs_node_path(&primary);
        let path = format!("{prefix}/cold-held-write.bin");
        // Retain only real content readers before the held phase/cache cut.
        // Opening a Core service after eviction would change the cold premise.
        let stores = nodes
            .iter()
            .map(|node| {
                let client = native.client(node).unwrap();
                (node.clone(), client.content().clone())
            })
            .collect::<Vec<_>>();
        let legacy = native.metadata_shards().unwrap();
        let setup_markers = legacy_markers(&native, &nodes, &legacy);
        let phase = native
            .begin_deferred_native_genesis("cold-held-native-node-scope", &prefix)
            .unwrap();
        let shard = phase.shard().clone();
        let original = phase.original_record().clone();
        assert_eq!(shard.primary_node_id, primary);
        assert!(shard.replica_node_ids.is_empty());
        assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
        assert!(original.committed_entry.is_none());
        assert!(phase.deferral_hits() > 0);
        let prepared_markers = legacy_markers(&native, &nodes, &legacy);
        let setup_rebinds = attribute_setup_rebinds(&setup_markers, &prepared_markers);
        let root = crate::local_dfs_metadata_shard_path(
            native.data_dir(&primary),
            &DfsMetadataShardId::new(&shard.shard_id),
        );
        let bytes = vec![0x6b; 4096];
        let hash = crate::content_hash(&bytes);
        let object = DfsObjectId::generate();
        let version = DfsVersion::generate();
        let mut routed = test_dfs_write_routed_message(&primary, &primary, 293, &path);
        let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
            unreachable!()
        };
        write.request_id = format!("cold-held-write-{}", sinomo_core::Id::generate());
        write.request = sinomo_core::DfsWriteRequest {
            file_system_id: shard.file_system_id.clone(),
            object_id: object.clone(),
            target_version: version.clone(),
            requesting_node_id: primary.clone(),
        };
        write.expected_base_version = None;
        write.bytes.clear();
        write.content_hash = Some(hash.clone());
        write.content_size_bytes = Some(bytes.len() as u64);
        write.requester_blob_pre_staged = false;
        write.desired_replica_nodes = nodes
            .iter()
            .filter(|node| **node != primary)
            .cloned()
            .collect();
        write.policy = DfsReplicationPolicy::new(1, 2).unwrap();
        write.authority_shard = Some(shard.clone());
        write.touch_parent_on_create = true;
        write.foreground_replica_repair = true;
        write.exclusive_create = false;
        let metadata = write.object_metadata.as_mut().unwrap();
        metadata.object_id = object.clone();
        metadata.current_locations.clear();
        metadata.policy = DfsObjectPolicy::ReplicatedEventual;
        let body = write.clone();
        let request = test_dfs_message_command("dfs_message_293", routed);
        assert!(matches!(
            dfs_message_execution_scope(&request),
            DfsMessageExecutionScope::ExactShard { .. }
        ));
        let before_frames = actual_frames(&root);
        let before_content = content_facts(&stores, &object, &version, &hash);
        let candidates = || {
            stores
                .iter()
                .map(|(node, store)| {
                    (
                        node.clone(),
                        store
                            .blob_storage_candidate_exists(&hash)
                            .map_err(|error| error.to_string()),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let before_candidates = candidates();
        let markers = legacy_markers(&native, &nodes, &legacy);
        let prepared_rebinds = attribute_setup_rebinds(&prepared_markers, &markers);
        let cold_opens = Arc::new(AtomicUsize::new(0));
        let observed_opens = Arc::clone(&cold_opens);
        let hook = super::super::message_host::install_daemon_dfs_pre_dispatch_test_hook(
            native.data_dir(&primary),
            Arc::new(move |point, _, _| {
                if point
                    == super::super::message_host::DaemonDfsPreDispatchTestPoint::BeforeResidentBackendOpen
                {
                    observed_opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                Ok(())
            }),
        );
        let scheduler = daemon_dfs_mutation_scheduler(native.data_dir(&primary)).unwrap();
        let eviction_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let admission = native.runtime_handle().block_on(async {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(eviction_deadline),
                scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive),
            )
            .await
            .expect("cache eviction admission keeps its original5s")
            .unwrap()
        });
        let evicted = evict_daemon_dfs_message_service_cache(native.data_dir(&primary)).unwrap();
        drop(admission);
        assert!(evicted, "evict the actual mounted owner's warm cache");
        assert!(Instant::now() <= eviction_deadline);
        let prior = native
            .rpc_observations()
            .into_iter()
            .map(|event| event.call_id)
            .collect::<BTreeSet<_>>();
        // This registry-only caller neither opens Core nor recovers Prepared.
        let held = native_call(&native, &primary, &request, Some(&bytes));
        let held_cold_opens = cold_opens.load(std::sync::atomic::Ordering::SeqCst);
        drop(hook);
        let while_held = native.read_deferred_native_genesis(&phase);
        let after_frames = actual_frames(&root);
        let after_content = content_facts(&stores, &object, &version, &hash);
        let after_candidates = candidates();
        let markers_after = legacy_markers(&native, &nodes, &legacy);
        let current_assignment = native.metadata_shards().map(|shards| {
            shards
                .into_iter()
                .find(|current| current.shard_id == shard.shard_id)
        });
        let held_wire = native
            .rpc_observations()
            .into_iter()
            .filter(|event| !prior.contains(&event.call_id))
            .collect::<Vec<_>>();
        // Preserve the valid post-control even if the held request was unsafe.
        // Only this explicit normal owner may complete the SAME genesis.
        let completed = native.complete_deferred_native_genesis(&phase);
        let synced = completed
            .as_ref()
            .ok()
            .map(|_| native.sync_completed_native_genesis(&phase, &prefix));
        let post = synced
            .as_ref()
            .filter(|sync| sync.is_ok())
            .map(|_| native_call(&native, &primary, &request, Some(&bytes)));
        let report = serde_json::json!({
            "scope":"actual three-node fixture; held scope assigned{A}; cache eviction is not an OS crash/reopen",
            "request":request,"body":body,"payload":bytes,"payload_hash":hash,
            "original":original,"while_held":while_held,"current_assignment_while_held":current_assignment,
            "evicted":evicted,"cold_open_observations_while_held":held_cold_opens,
            "cold_observer_scope":"actual root cold-open boundary, not a fabricated per-call authority witness",
            "held_result":held.response,"held_elapsed":format!("{:?}",held.elapsed),"held_expired":held.expired,
            "frames_before":before_frames,"frames_after":after_frames,
            "content_before":before_content,"content_after":after_content,
            "candidates_before":before_candidates,"candidates_after":after_candidates,
            "legacy_markers":{"setup":setup_markers,"prepared":prepared_markers,
                "before_probe":markers,"after_probe":markers_after,
                "setup_rebinds":setup_rebinds,"prepared_rebinds":prepared_rebinds},
            "held_rpc":wire_facts(&held_wire),"same_genesis_completion":completed,"assigned_sync":synced,
            "same_request_after_sync":post.as_ref().map(|call|&call.response),
            "post_elapsed":post.as_ref().map(|call|format!("{:?}",call.elapsed)),
            "quietness":"explicit operation activity; no quiet-window or writeback claim",
        });
        let encoded = serde_json::to_vec_pretty(&report).unwrap();
        assert!(encoded.len() <= 4 * 1024 * 1024);
        let evidence = native
            .data_dir(&primary)
            .root()
            .parent()
            .unwrap()
            .join("incomplete-genesis-cold-held-native-control.json");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&evidence)
            .unwrap();
        file.write_all(&encoded).unwrap();
        file.sync_all().unwrap();
        eprintln!("INCOMPLETE_GENESIS_COLD_HELD_REPORT {}", evidence.display());
        let committed = completed.as_ref().unwrap();
        assert_eq!(committed.digest, original.digest);
        assert_eq!(committed.payload, original.payload);
        assert_eq!(committed.status, DfsPreparedOperationStatus::Committed);
        assert!(committed.committed_entry.is_some());
        let certificate = synced.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(certificate.target.expected_shard.as_ref(), Some(&shard));
        assert_eq!(certificate.acknowledged_node_ids, vec![primary]);
        assert_eq!(certificate.quorum_size, 1);
        assert!(
            write_committed(post.as_ref().unwrap(), &body),
            "the SAME stamped request/body must succeed after explicit owner completion and sync"
        );
        assert!(
            held_cold_opens > 0,
            "actual evicted root must cold-open while genesis is held"
        );
        assert!(refused(&held), "held cold-owner request: {held:?}");
        assert!(
            held.response.as_ref().err().is_some_and(|error| error
                .contains(&original.digest.operation_id)
                && error.contains(
                    "remains prepared; mutation deferred before content or request binding"
                )),
            "a cold/unavailable/authentication error is not the known-Prepared refusal: {held:?}"
        );
        assert_eq!(while_held.as_ref().unwrap(), &original);
        assert_eq!(current_assignment.as_ref().unwrap().as_ref(), Some(&shard));
        assert_eq!(
            before_frames.as_ref().unwrap(),
            after_frames.as_ref().unwrap()
        );
        assert_eq!(markers_after, markers);
        assert!(
            before_candidates
                .values()
                .all(|value| matches!(value, Ok(false)))
        );
        assert!(
            after_candidates
                .values()
                .all(|value| matches!(value, Ok(false))),
            "cold/error inspection cannot count as absence or pre-effect refusal"
        );
        assert_eq!(before_content, after_content);
        assert!(effect_routes(&held_wire, &shard.primary_node_id).is_empty());
        assert_eq!(native.observation_dropped_count(), 0);
        drop(phase);
        drop(stores);
    }));
    let closed = native.close();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    closed.unwrap();
}
