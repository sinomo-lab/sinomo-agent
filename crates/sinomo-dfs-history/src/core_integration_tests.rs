use super::*;
use sinomo_core::events::EventId;
use sinomo_core::network::NetworkError;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use sinomo_dfs::test_support::{
    self as core_test, TestDistributedWriteOptions as DfsDistributedWriteOptions,
    TestWriteRepair as DfsWriteRepair,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

use crate::{DfsHistoryExt, decode_history_capture, decode_history_policy, install_history_policy};

trait HistoryCaptureFixture {
    fn history_capture(&self) -> Option<DfsRetainedFileVersion>;
}

impl HistoryCaptureFixture for DfsObjectVersionRecord {
    fn history_capture(&self) -> Option<DfsRetainedFileVersion> {
        decode_history_capture(self).expect("valid module-owned capture metadata")
    }
}

#[test]
fn native_history_policy_survives_metadata_refresh_on_real_shards() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "native-history-policy-shards",
        3,
    );
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "native-history-policy-shards",
        nodes[0].clone(),
    )
    .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&nodes[0]).unwrap();
    let mut configured = runtime.file_system();
    install_history_policy(
        &mut configured,
        DfsFileHistoryPolicy {
            rules: vec![DfsFileHistoryRule {
                path: "/services".into(),
                retention: DfsFileHistoryRetention {
                    keep_last: 1,
                    keep_for_seconds: 0,
                },
            }],
        },
    )
    .unwrap();
    apply_history_fixture_policy(&mut dfs, &configured);
    let options = DfsWriteOptions::replicated_eventual(nodes.clone());
    let old = dfs
        .posix_write_file("/services/history-policy.txt", b"captured", options.clone())
        .unwrap();
    let captured = dfs.posix_set_mode(&old.path, 0o640).unwrap();
    let current = dfs
        .posix_write_file(&old.path, b"current", options)
        .unwrap();
    dfs.posix_set_times(&current.path, Some(12), Some(34))
        .unwrap();
    let actual = dfs
        .revision_metadata(&history_identity(&captured))
        .unwrap()
        .expect("the current primary retains the exact captured revision")
        .version;
    assert!(actual.retention_grant(HISTORY_MODULE_ID).is_some());
    assert_eq!(
        dfs.posix_read_file_distributed(&current.path).unwrap(),
        b"current"
    );
    assert_eq!(
        dfs.file_history_policy(&configured.file_system_id).unwrap(),
        decode_history_policy(configured.retention_policies.get(HISTORY_MODULE_ID))
    );
    drop(dfs);
    for node in &nodes {
        runtime.stop_node(node).unwrap();
    }
    for node in &nodes {
        runtime.reopen_node(node.clone()).unwrap();
    }
    let mut reader = runtime.client(&nodes[2]).unwrap();
    assert_eq!(
        reader
            .posix_read_file_distributed("/services/history-policy.txt")
            .unwrap(),
        b"current"
    );
    assert!(
        reader
            .revision_metadata(&history_identity(&captured))
            .unwrap()
            .expect("the reopened current primary retains the exact captured revision")
            .version
            .retention_grant(HISTORY_MODULE_ID)
            .is_some()
    );
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn native_history_retained_content_survives_actual_metadata_authority_handoff() {
    let nodes =
        sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids("native-history-handoff", 3);
    let mut runtime =
        sinomo_dfs::test_support::DfsTestRuntime::new("native-history-handoff", nodes[0].clone())
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
    let promoted = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut writer = runtime.client(&primary).unwrap();
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let saved = writer
        .posix_write_file(
            "/services/native-retained-handoff",
            b"retained",
            options.clone(),
        )
        .unwrap();
    capture_history(&mut writer, &saved);
    let current = writer
        .posix_write_file(&saved.path, b"current", options)
        .unwrap();
    writer
        .posix_sync_file(&saved.path, sinomo_dfs::DfsPosixSyncKind::File)
        .unwrap();
    drop(writer);
    let actual_before = runtime.metadata_primary_service(&saved.path).unwrap();
    let before_object = actual_before
        .metadata_backend()
        .unwrap()
        .get_object(&saved.object_id)
        .unwrap()
        .unwrap();
    let before_retained = actual_before
        .metadata_backend()
        .unwrap()
        .get_version(&saved.object_id, &saved.version)
        .unwrap()
        .unwrap();
    let before_current = actual_before
        .metadata_backend()
        .unwrap()
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    let before_prefix = actual_before
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&DfsMetadataShardId::new(shard.shard_id.clone()))
        .unwrap()
        .unwrap();
    drop(actual_before);

    // The managed default planner may perform a later legitimate handoff.
    // Retain the actual proposal and every proof before the shared normal transfer.
    let proof_key =
        metadata_shard_membership_handoff_proof_key(&shard.file_system_id, &shard.shard_id);
    let handoff_key =
        sinomo_dfs::metadata_shard_membership_handoff_key(&shard.file_system_id, &shard.shard_id);
    let requested_handoffs = Arc::new(std::sync::Mutex::new(Vec::<
        Result<sinomo_dfs::DfsMetadataShardMembershipHandoff, String>,
    >::new()));
    let observed = Arc::new(std::sync::Mutex::new(Vec::<
        Result<DfsMetadataShardMembershipHandoffProof, String>,
    >::new()));
    let overflow = Arc::new(AtomicBool::new(false));
    let subscriptions = nodes
        .iter()
        .map(|node| {
            let observed = Arc::clone(&observed);
            let overflow = Arc::clone(&overflow);
            let proof_key = proof_key.clone();
            let handoff_key = handoff_key.clone();
            let requested_handoffs = Arc::clone(&requested_handoffs);
            sinomo_core::cluster::subscribe_cluster_kv_committed_deltas(
                &sinomo_core::network::ClusterPaths::new(runtime.data_dir(node)),
                move |delta| {
                    for event in delta
                        .events
                        .iter()
                        .filter(|event| event.key == proof_key || event.key == handoff_key)
                    {
                        if let sinomo_core::kv::KvEventKind::Put { value } = &event.kind {
                            if value.len() > 16 * 1024 {
                                overflow.store(true, Ordering::Release);
                                continue;
                            }
                            if event.key == handoff_key {
                                let handoff = sinomo_core::kv::decode_kv_value::<
                                    sinomo_dfs::DfsMetadataShardMembershipHandoff,
                                >(value)
                                .map_err(|error| error.to_string());
                                let mut requested = requested_handoffs
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner());
                                if requested.len() == 64 {
                                    overflow.store(true, Ordering::Release);
                                } else {
                                    requested.push(handoff);
                                }
                                continue;
                            }
                            let proof = sinomo_core::kv::decode_kv_value::<
                                DfsMetadataShardMembershipHandoffProof,
                            >(value)
                            .map_err(|error| error.to_string());
                            let mut observed =
                                observed.lock().unwrap_or_else(|error| error.into_inner());
                            if observed.len() == 64 {
                                overflow.store(true, Ordering::Release);
                            } else {
                                observed.push(proof);
                            }
                        }
                    }
                },
            )
        })
        .collect::<Vec<_>>();
    let control = runtime.shared_control_plane().unwrap();
    let configs = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).list_metadata_shards(&shard.file_system_id)
        })
        .unwrap();
    let retiring = configs
        .iter()
        .find(|cfg| cfg.shard_id == shard.shard_id)
        .unwrap();
    assert_eq!(retiring.primary_node_id, primary);
    let members = std::iter::once(retiring.primary_node_id.clone())
        .chain(retiring.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    assert!(members.contains(&promoted));
    let transfer_deadline = Instant::now() + Duration::from_secs(5);
    let transfer_result = runtime.reassign_metadata_shard(
        &shard.file_system_id,
        &shard.shard_id,
        promoted.clone(),
        members
            .iter()
            .filter(|node| **node != promoted)
            .cloned()
            .collect(),
    );
    assert!(!overflow.load(Ordering::Acquire));
    let requested = requested_handoffs
        .lock()
        .unwrap()
        .iter()
        .map(|handoff| handoff.clone().unwrap())
        .find(|handoff| {
            handoff.retiring_shard == *retiring && handoff.desired_shard.primary_node_id == promoted
        })
        .expect("the normal transfer must retain its actual committed proposal");
    let transferred = &requested.desired_shard;
    assert_eq!(transferred.primary_node_id, promoted);
    assert_eq!(transferred.epoch, retiring.epoch.checked_add(1).unwrap());
    assert_eq!(transferred.file_system_id, retiring.file_system_id);
    assert_eq!(transferred.shard_id, retiring.shard_id);
    assert_eq!(transferred.assignment, retiring.assignment);
    assert_eq!(transferred.default_policy, retiring.default_policy);
    assert_eq!(
        std::iter::once(transferred.primary_node_id.clone())
            .chain(transferred.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>(),
        members,
    );
    if let Ok(actual) = &transfer_result {
        assert_eq!(actual, transferred);
    }
    assert_eq!(
        requested.cause,
        sinomo_dfs::DfsMetadataShardHandoffCause::Membership
    );

    let actual_lineage = |deadline| {
        let keys = [
            metadata_shard_key(&shard.file_system_id, &shard.shard_id),
            handoff_key.clone(),
            proof_key.clone(),
        ];
        let entries = control
            .with_client(|kv| kv.get_authoritative_consistent_before(&keys, deadline))
            .unwrap();
        assert!(Instant::now() <= deadline);
        assert_eq!(entries.len(), keys.len());
        let installed_entry = entries[0].as_ref().unwrap();
        assert!(
            entries[1].is_none(),
            "the coherent assignment has no pending handoff"
        );
        let latest_entry = entries[2].as_ref().unwrap();
        assert_eq!(installed_entry.key, keys[0]);
        assert_eq!(latest_entry.key, keys[2]);
        let installed = installed_entry.decode::<DfsMetadataShardConfig>().unwrap();
        let latest = latest_entry
            .decode::<DfsMetadataShardMembershipHandoffProof>()
            .unwrap();
        assert!(
            !overflow.load(Ordering::Acquire),
            "actual handoff proof capture must remain complete"
        );
        let proofs = observed
            .lock()
            .unwrap()
            .iter()
            .map(|proof| proof.clone().unwrap())
            .collect::<Vec<_>>();
        let first = proofs
            .iter()
            .find(|proof| proof.handoff_id == requested.handoff_id)
            .expect("the requested primary transfer must have its genuine committed proof");
        assert_eq!(first.cause, requested.cause);
        assert_eq!(first.retiring_shard, requested.retiring_shard);
        assert_eq!(first.desired_shard, requested.desired_shard);
        assert_eq!(first.desired_shard.primary_node_id, promoted);
        assert_eq!(first.journal_sequence, before_prefix.journal_sequence);
        assert_eq!(first.state_fingerprint, before_prefix.digest);
        let proposals = requested_handoffs
            .lock()
            .unwrap()
            .iter()
            .map(|proposal| proposal.clone().unwrap())
            .collect::<Vec<_>>();
        let mut prior = first;
        let mut validated_chain = Vec::new();
        for _ in 0..proofs.len() {
            let proposal = proposals
                .iter()
                .find(|proposal| {
                    proposal.handoff_id == prior.handoff_id
                        && proposal.cause == prior.cause
                        && proposal.retiring_shard == prior.retiring_shard
                        && proposal.desired_shard == prior.desired_shard
                        && proposal.retiring_primary_fence.is_some()
                })
                .expect("each committed proof must retain its exact actual retiring fence");
            if prior.handoff_id == requested.handoff_id {
                assert_eq!(
                    proposal.required_prepared_sequence,
                    requested.required_prepared_sequence
                );
                assert_eq!(
                    proposal.required_head_watermark,
                    requested.required_head_watermark
                );
            }
            let fence = proposal.retiring_primary_fence.as_ref().unwrap();
            assert_eq!(fence.handoff_id, prior.handoff_id);
            assert_eq!(fence.primary_node_id, prior.retiring_shard.primary_node_id);
            assert_eq!(fence.journal_sequence, prior.journal_sequence);
            assert_eq!(fence.state_fingerprint, prior.state_fingerprint);
            assert!(prior.journal_sequence >= proposal.required_prepared_sequence);
            if let Some(head) = &proposal.required_head_watermark {
                assert_eq!(head.file_system_id, prior.retiring_shard.file_system_id);
                assert_eq!(head.shard_id, prior.retiring_shard.shard_id);
                assert_eq!(head.primary_node_id, prior.retiring_shard.primary_node_id);
                assert_eq!(head.epoch, prior.retiring_shard.epoch);
                assert!(prior.journal_sequence >= head.journal_sequence);
                if prior.journal_sequence == head.journal_sequence {
                    assert_eq!(prior.state_fingerprint, head.state_fingerprint);
                }
            }
            assert!(core_test::metadata_handoff_proof_acknowledgements_are_safe(
                prior
            ));
            validated_chain.push((*prior).clone());
            if prior.desired_shard == installed {
                break;
            }
            let successor = proofs
                .iter()
                .find(|proof| proof.retiring_shard == prior.desired_shard)
                .expect("a changed assignment must retain the whole actual successor proof");
            assert_ne!(successor.handoff_id, prior.handoff_id);
            assert_eq!(successor.cause, prior.cause);
            assert_eq!(
                successor.desired_shard.epoch,
                prior.desired_shard.epoch.checked_add(1).unwrap()
            );
            assert_eq!(successor.journal_sequence, first.journal_sequence);
            assert_eq!(successor.state_fingerprint, first.state_fingerprint);
            prior = successor;
        }
        assert_eq!(
            prior.desired_shard, installed,
            "the bounded actual proof chain must reach the coherent installed assignment"
        );
        assert_eq!(latest, *prior);
        (installed, validated_chain)
    };
    let (installed, validated_chain) = actual_lineage(transfer_deadline);
    let marker = sinomo_dfs::read_local_dfs_metadata_shard_write_freeze(
        runtime.data_dir(&requested.retiring_shard.primary_node_id),
        &DfsMetadataShardId::new(shard.shard_id.clone()),
    )
    .unwrap()
    .expect("the original source retains its actual durable retirement marker");
    // This same local marker may name a completed successor. Match it only to
    // a full proof already validated in the original-to-current chain above.
    let marker_proof = validated_chain
        .iter()
        .find(|proof| proof.handoff_id == marker.handoff_id)
        .expect("the actual source marker belongs to the complete validated handoff chain");
    let original_source = &requested.retiring_shard.primary_node_id;
    assert!(
        marker_proof.retiring_shard.primary_node_id == *original_source
            || marker_proof
                .retiring_shard
                .replica_node_ids
                .contains(original_source)
    );
    assert!(
        marker_proof.desired_shard.primary_node_id == *original_source
            || marker_proof
                .desired_shard
                .replica_node_ids
                .contains(original_source)
    );
    assert_eq!(marker.retiring_epoch, marker_proof.retiring_shard.epoch);
    assert_eq!(
        marker.activated_epoch,
        Some(marker_proof.desired_shard.epoch)
    );
    assert_eq!(marker.journal_sequence, marker_proof.journal_sequence);
    assert_eq!(marker.state_fingerprint, marker_proof.state_fingerprint);
    assert_eq!(marker.journal_sequence, before_prefix.journal_sequence);
    assert_eq!(marker.state_fingerprint, before_prefix.digest);
    assert_eq!(marker.rebind_prefix_proof, before_prefix);
    assert!(Instant::now() <= transfer_deadline);
    // The same bounded coherent cut, full original-to-current proof chain and
    // actual source marker prove completion even if the caller returned an error.
    // No successor is required when the original desired assignment is current.
    let target = runtime.client(&promoted).unwrap();
    assert_eq!(
        target
            .metadata_backend()
            .unwrap()
            .get_object(&saved.object_id)
            .unwrap()
            .unwrap(),
        before_object
    );
    assert_eq!(
        target
            .metadata_backend()
            .unwrap()
            .get_version(&saved.object_id, &saved.version)
            .unwrap()
            .unwrap(),
        before_retained
    );
    assert_eq!(
        target
            .metadata_backend()
            .unwrap()
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .unwrap(),
        before_current
    );
    drop(target);
    let mut reader = runtime.client(&installed.primary_node_id).unwrap();
    assert_eq!(
        reader.posix_read_file_distributed(&saved.path).unwrap(),
        b"current"
    );
    assert!(
        matches!(reader.read_file_history(DfsFileHistoryReadRequest { identity: history_identity(&saved), offset: 0, max_bytes: 64 }).unwrap(),
        DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"retained")
    );
    drop(reader);
    runtime.stop_node(&promoted).unwrap();
    runtime.reopen_node(promoted.clone()).unwrap();
    let target = runtime.client(&promoted).unwrap();
    assert_eq!(
        target
            .metadata_backend()
            .unwrap()
            .get_object(&saved.object_id)
            .unwrap()
            .unwrap(),
        before_object
    );
    assert_eq!(
        target
            .metadata_backend()
            .unwrap()
            .get_version(&saved.object_id, &saved.version)
            .unwrap()
            .unwrap(),
        before_retained
    );
    assert_eq!(
        target
            .metadata_backend()
            .unwrap()
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .unwrap(),
        before_current
    );
    drop(target);
    let (installed, _) = actual_lineage(Instant::now() + Duration::from_secs(5));
    let mut reader = runtime.client(&installed.primary_node_id).unwrap();
    assert_eq!(
        reader.posix_read_file_distributed(&saved.path).unwrap(),
        b"current"
    );
    assert!(
        matches!(reader.read_file_history(DfsFileHistoryReadRequest { identity: history_identity(&saved), offset: 0, max_bytes: 64 }).unwrap(),
        DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"retained")
    );
    drop(reader);
    drop(actual_lineage);
    drop(subscriptions);
    runtime.close().unwrap();
}

#[test]
fn file_history_committed_repair_cannot_override_a_later_lower_sorting_head() {
    let root = temp_root("history-repair-read-view");
    let (mut runtime, owner, remotes, mut writer) = native_history_repair_services(
        "file_history_committed_repair_cannot_override_a_later_lower_sorting_head",
        3,
    );
    let remote = remotes[0].clone();
    deny_history_content_targets(&runtime, &owner, &[remote.clone()]);
    let path = "/services/repair-read-view";
    let initial = writer
        .posix_write_file(
            path,
            b"seed",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    // Stable restore identities are opaque and may sort after later writes.
    let restored_version = DfsVersion::new("7zzzzzzzzzzzzzzzzzzzzzzzzz").unwrap();
    let outcome = writer
        .test_write_distributed_bytes_with_base(
            DfsWriteRequest {
                file_system_id: initial.file_system_id.clone(),
                object_id: initial.object_id.clone(),
                target_version: restored_version.clone(),
                requesting_node_id: owner.clone(),
            },
            Some(initial.version),
            b"restored".to_vec(),
            vec![owner.clone(), remote],
            DfsReplicationPolicy::new(1, 2).unwrap(),
            DfsDistributedWriteOptions {
                repair: DfsWriteRepair::Background,
                ..Default::default()
            },
        )
        .unwrap();
    // The routed low-level entry returns its initial pending registration.
    // Consume the same real Main reply before inspecting this caller's result.
    let outcome = match outcome {
        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => {
            writer.process_incoming().unwrap();
            writer.test_write_outcome(&intent_id).unwrap()
        }
        completed => completed,
    };
    let NetworkedDfsWriteOutcome::Committed {
        intent_id,
        repair_jobs,
        ..
    } = outcome
    else {
        panic!("the actual local receipt must reach W1");
    };
    assert_eq!(
        repair_jobs.len(),
        1,
        "one real denied content target remains"
    );
    let mut reader = runtime.client(&owner).unwrap();
    let committed = reader
        .metadata_backend()
        .unwrap()
        .get_intent(&initial.object_id, &intent_id)
        .unwrap()
        .expect("actual committed repair obligation");
    assert_eq!(committed.target_version, restored_version);
    assert!(
        committed
            .desired_replica_node_ids
            .iter()
            .any(|node| node != &owner)
    );
    assert_eq!(committed.policy, DfsReplicationPolicy::new(1, 2).unwrap());
    assert_eq!(reader.posix_read_file(path).unwrap(), b"restored");
    let later = writer
        .posix_write_file(
            path,
            b"after restore",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    assert!(later.version < restored_version);
    assert_eq!(
        reader
            .metadata_backend()
            .unwrap()
            .get_object(&later.object_id)
            .unwrap()
            .unwrap()
            .version,
        later.version
    );
    assert_eq!(reader.posix_read_file(path).unwrap(), b"after restore");
    let mut streamed = Vec::new();
    std::io::Read::read_to_end(
        &mut reader.posix_open_current_file_stream(path).unwrap().reader,
        &mut streamed,
    )
    .unwrap();
    assert_eq!(streamed, b"after restore");
    drop(reader);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn file_history_policy_update_is_honored_by_fast_existing_file_writes() {
    let owner = NodeId::generate();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_policy_update_is_honored_by_fast_existing_file_writes",
        owner.clone(),
    );
    let options = DfsWriteOptions {
        foreground_replica_repair: false,
        ..DfsWriteOptions::replicated_eventual(vec![owner.clone()])
    };
    let path = "/services/policy-after-create";
    let before = dfs
        .posix_write_file(path, b"before-policy", options.clone())
        .unwrap();
    let fs = dfs
        .metadata_backend()
        .unwrap()
        .get_file_system(&before.file_system_id)
        .unwrap()
        .unwrap();
    // Both clients share the already-mounted native owner and actual KV
    // configuration. A setting update never creates fixture membership/proofs.
    let mut admin = runtime.client(&owner).unwrap();
    let policy = DfsFileHistoryPolicy {
        rules: vec![DfsFileHistoryRule {
            path: path.into(),
            retention: DfsFileHistoryRetention {
                keep_last: 1,
                keep_for_seconds: 0,
            },
        }],
    };
    admin
        .update_file_history_policy(DfsFileHistoryPolicyUpdateRequest {
            file_system_id: fs.file_system_id.clone(),
            expected: Default::default(),
            policy: policy.clone(),
        })
        .unwrap();
    dfs.posix_write_file(path, b"after-policy", options.clone())
        .unwrap();
    assert_eq!(
        history_entries(&mut dfs, path),
        vec![history_identity(&before)]
    );
    admin
        .update_file_history_policy(DfsFileHistoryPolicyUpdateRequest {
            file_system_id: fs.file_system_id.clone(),
            expected: policy,
            policy: Default::default(),
        })
        .unwrap();
    dfs.posix_write_file(path, b"after-disable", options.clone())
        .unwrap();
    assert_eq!(
        history_entries(&mut dfs, path),
        vec![history_identity(&before)],
        "disabling future captures must retain the previously granted minimum without capturing another version"
    );
    admin
        .update_file_history_policy(DfsFileHistoryPolicyUpdateRequest {
            file_system_id: fs.file_system_id,
            expected: Default::default(),
            policy: DfsFileHistoryPolicy {
                rules: vec![DfsFileHistoryRule {
                    path: "/services/unrelated-retention".into(),
                    retention: DfsFileHistoryRetention {
                        keep_last: 1,
                        keep_for_seconds: 0,
                    },
                }],
            },
        })
        .unwrap();
    let ordinary = "/services/unrelated-write";
    dfs.posix_write_file(ordinary, b"create", options.clone())
        .unwrap();
    dfs.posix_write_file(ordinary, b"overwrite", options)
        .unwrap();
    assert_eq!(dfs.posix_read_file(ordinary).unwrap(), b"overwrite");
    assert!(history_entries(&mut dfs, ordinary).is_empty());
    drop(admin);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_count_retention_filters_reads_and_pages_before_gc() {
    let root = temp_root("history-live-grants");
    let owner = NodeId::generate();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_count_retention_filters_reads_and_pages_before_gc",
        owner.clone(),
    );
    let path = "/services/live-grants";
    let mut fs = dfs
        .metadata_backend()
        .unwrap()
        .get_file_system(&sifs_file_system_id())
        .unwrap()
        .unwrap();
    {
        let mut policy = decode_history_policy(fs.retention_policies.get(HISTORY_MODULE_ID));
        policy.rules.push(DfsFileHistoryRule {
            path: path.into(),
            retention: DfsFileHistoryRetention {
                keep_last: 1,
                keep_for_seconds: 0,
            },
        });
        install_history_policy(&mut fs, policy).unwrap();
    }
    apply_history_fixture_policy(&mut dfs, &fs);
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let first = dfs
        .posix_write_file(path, b"first", options.clone())
        .unwrap();
    let second = dfs
        .posix_write_file(path, b"second", options.clone())
        .unwrap();
    dfs.posix_write_file(path, b"current", options).unwrap();
    assert_eq!(
        history_entries(&mut dfs, path),
        vec![history_identity(&second)]
    );
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&first),
            offset: 0,
            max_bytes: 64,
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    assert!(matches!(dfs.read_file_history(DfsFileHistoryReadRequest {
        identity: history_identity(&second), offset: 0, max_bytes: 64,
    }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"second"));
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: path.into(),
        identity: history_identity(&first),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "independent".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    assert_eq!(
        history_entries(&mut dfs, path).len(),
        2,
        "an independent pin still retains the older capture"
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"current");
    drop(dfs);
    runtime.close().unwrap();
}

// The module intentionally reads one bounded native shard per call. Follow
// only its returned cursor when an earlier actual shard has no matching rows.
pub(super) fn next_history_entries_page<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    mut request: DfsFileHistoryListRequest,
) -> Result<DfsFileHistoryListResponse, DfsError> {
    loop {
        let page = dfs.list_file_history(request.clone())?;
        if !page.entries.is_empty() || page.next_cursor.is_none() {
            return Ok(page);
        }
        let next = page.next_cursor.expect("actual next shard cursor");
        assert_ne!(request.after.as_ref(), Some(&next));
        request.after = Some(next);
    }
}

fn history_entries<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    path: &str,
) -> Vec<DfsFileVersionIdentity> {
    let mut request = DfsFileHistoryListRequest {
        file_system_id: sifs_file_system_id(),
        path: Some(path.into()),
        object_id: None,
        after: None,
        limit: 1,
    };
    let mut entries = Vec::new();
    loop {
        let page = dfs.list_file_history(request.clone()).unwrap();
        entries.extend(page.entries.into_iter().map(|entry| entry.identity));
        let Some(after) = page.next_cursor else { break };
        assert_ne!(request.after.as_ref(), Some(&after));
        request.after = Some(after);
    }
    entries
}

#[test]
fn file_history_repair_status_includes_metadata_successors_and_pinned_history() {
    let root = temp_root("history-repair-status-roots");
    let (mut runtime, owner, remotes, mut dfs) = native_history_repair_services(
        "file_history_repair_status_includes_metadata_successors_and_pinned_history",
        3,
    );
    let missing = remotes[0].clone();
    deny_history_content_targets(&runtime, &owner, &[missing.clone()]);
    let original = dfs
        .posix_write_file(
            "/services/status-history",
            b"old",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), missing],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let successor = dfs.posix_set_mode(&original.path, 0o640).unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_repair_count,
        1,
        "the current metadata successor still owns the unfinished data write"
    );
    capture_history(&mut dfs, &successor);
    dfs.posix_write_file(
        &original.path,
        b"current",
        DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .unwrap();
    native_history_owner_tick(&runtime, &owner);
    assert_history_repair_inventory(&dfs, &successor.object_id);
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_repair_count
            > 0,
        "retained history repairs must remain visible after the head changes"
    );
    dfs.posix_unlink(&original.path).unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    dfs.posix_write_file(
        &original.path,
        b"recreated",
        DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: successor.path.clone(),
        identity: history_identity(&successor),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    assert_history_repair_inventory(&dfs, &successor.object_id);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_repair_count,
        0
    );
    drop(dfs);
    runtime.close().unwrap();
}

fn assert_history_repair_inventory<T: DfsMessageTransport>(
    dfs: &NetworkedDfsService<T>,
    object_id: &DfsObjectId,
) {
    let backend = dfs.metadata_backend().unwrap();
    let inspector = DfsOperatorInspector::new(backend);
    let count = backend.metadata_summary().unwrap().pending_repair_count;
    let all = inspector.repairs(None, None).unwrap();
    assert_eq!(all.len(), count, "repair inventory must match summary");
    let exact = inspector.repairs(None, Some(object_id)).unwrap();
    assert_eq!(exact, all);
    assert_eq!(
        inspector.object(object_id).unwrap().unwrap().repairs.len(),
        count
    );
    for filter in [None, Some(object_id)] {
        let mut collected = Vec::new();
        let mut cursor = None;
        loop {
            let page = inspector
                .repairs_page(None, filter, Some(1), cursor.as_ref())
                .unwrap();
            collected.extend(page.repairs);
            let Some(next) = page.next_cursor else { break };
            assert_ne!(cursor.as_ref(), Some(&next), "repair cursor must advance");
            cursor = Some(next);
        }
        assert_eq!(collected, all);
    }
}

#[test]
fn file_history_rule_delete_and_bulk_replace_schedule_the_captured_generation() {
    for delete in [true, false] {
        let root = temp_root("history-rule-namespace-demand");
        let (mut runtime, owner, remotes, mut dfs) = native_history_repair_services(
            "file_history_rule_delete_and_bulk_replace_schedule_the_captured_generation",
            3,
        );
        let missing = remotes[0].clone();
        deny_history_content_targets(&runtime, &owner, &[missing.clone()]);
        let first = dfs
            .posix_write_file(
                "/services/rule-namespace",
                b"old",
                DfsWriteOptions {
                    replica_candidates: vec![owner.clone(), missing],
                    policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        let captured = dfs.posix_set_mode(&first.path, 0o640).unwrap();
        let mut fs = dfs
            .metadata_backend()
            .unwrap()
            .get_file_system(&first.file_system_id)
            .unwrap()
            .unwrap();
        {
            let mut policy = decode_history_policy(fs.retention_policies.get(HISTORY_MODULE_ID));
            policy.rules.push(DfsFileHistoryRule {
                path: first.path.clone(),
                retention: DfsFileHistoryRetention {
                    keep_last: 1,
                    keep_for_seconds: 0,
                },
            });
            install_history_policy(&mut fs, policy).unwrap();
        }
        apply_history_fixture_policy(&mut dfs, &fs);
        if delete {
            dfs.posix_unlink(&first.path).unwrap();
            assert!(dfs.posix_stat(&first.path).is_err());
        } else {
            dfs.posix_write_files_local_eventual_with_known_parents(vec![(
                first.path.as_str(),
                b"new".as_slice(),
            )])
            .unwrap();
            assert_eq!(dfs.posix_read_file(&first.path).unwrap(), b"new");
        }
        assert!(
            native_history_queue(&runtime, &owner)
                .iter()
                .any(|record| record.version == captured.version),
            "delete={delete}"
        );
        let backend = dfs.metadata_backend().unwrap();
        let version = backend
            .get_version(&captured.object_id, &captured.version)
            .unwrap()
            .unwrap();
        let repair = backend
            .get_intent(
                &captured.object_id,
                &core_test::retained_repair_intent_id(&version),
            )
            .unwrap()
            .unwrap();
        assert_eq!(repair.policy, DfsReplicationPolicy::new(1, 2).unwrap());
        let current = backend.get_object(&captured.object_id).unwrap().unwrap();
        assert_eq!(
            native_history_main_current_version(&dfs, &current.object_id),
            Some(current.version.clone())
        );
        drop(dfs);
        runtime.close().unwrap();
    }
}

#[test]
fn native_history_rule_capture_publishes_with_its_recorded_durability_owner() {
    let nodes =
        sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids("native-history-rule-owner", 3);
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "native-history-rule-owner",
        nodes[0].clone(),
    )
    .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&nodes[0]).unwrap();
    let path = "/services/native-rule-capture";
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let first = dfs
        .posix_write_file(path, b"retained", options.clone())
        .unwrap();
    let captured = dfs.posix_set_mode(path, 0o640).unwrap();
    let mut fs = runtime.file_system();
    install_history_policy(
        &mut fs,
        DfsFileHistoryPolicy {
            rules: vec![DfsFileHistoryRule {
                path: path.into(),
                retention: DfsFileHistoryRetention {
                    keep_last: 2,
                    keep_for_seconds: 0,
                },
            }],
        },
    )
    .unwrap();
    apply_history_fixture_policy(&mut dfs, &fs);
    let current = dfs.posix_write_file(path, b"current", options).unwrap();
    dfs.posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
        .unwrap();
    let actual = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&captured.object_id, &captured.version)
        .unwrap()
        .unwrap();
    assert!(actual.history_capture().unwrap().policy_capture.is_some());
    assert_eq!(actual.content_hash, first.content_hash);
    assert!(
        actual.content_durability.is_some(),
        "normal metadata successor retains the recorded data-generation certificate"
    );
    drop(dfs);
    for node in &nodes {
        let reader = runtime.client(node).unwrap();
        let version = reader
            .metadata_backend()
            .unwrap()
            .get_version(&captured.object_id, &captured.version)
            .unwrap()
            .unwrap();
        assert!(version.retention_grant(HISTORY_MODULE_ID).is_some());
        assert_eq!(version.content_durability, actual.content_durability);
        drop(reader);
    }
    for node in &nodes {
        runtime.stop_node(node).unwrap();
    }
    for node in &nodes {
        runtime.reopen_node(node.clone()).unwrap();
    }
    let mut reader = runtime.client(&nodes[2]).unwrap();
    assert_eq!(
        reader.posix_stat_distributed(path).unwrap().version,
        current.version
    );
    assert_eq!(
        reader.posix_read_file_distributed(path).unwrap(),
        b"current"
    );
    assert!(
        matches!(reader.read_file_history(DfsFileHistoryReadRequest { identity: history_identity(&captured), offset: 0, max_bytes: 64 }).unwrap(),
        DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"retained")
    );
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn file_history_local_batch_plan_survives_metadata_changes_and_gc() {
    let root = temp_root("history-local-batch-durability");
    let owner = NodeId::from_legacy_named("history-local-batch-owner").unwrap();
    let (mut runtime, mut dfs) = native_history_core_service(
        "file_history_local_batch_plan_survives_metadata_changes_and_gc",
        owner.clone(),
    );
    let written = dfs
        .posix_write_files_local_eventual_with_known_parents(vec![(
            "/services/batch-history",
            b"batch content".as_slice(),
        )])
        .unwrap()
        .remove(0);
    let metadata = dfs.posix_set_mode(&written.path, 0o640).unwrap();
    for _ in 0..8 {
        runtime
            .gc_metadata_history_on_current_primaries(DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
    }
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    let version = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&metadata.object_id, &metadata.version)
        .unwrap()
        .unwrap();
    assert!(version.history_capture().is_none());
    let plan = version.content_durability.as_ref().unwrap();
    assert_eq!(plan.source_version, written.version);
    let source = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&written.object_id, &written.version)
        .unwrap()
        .expect("GC retains the actual source of the live metadata successor");
    assert_eq!(source.content_hash, version.content_hash);
    let receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&written.object_id, &written.version)
        .unwrap();
    assert_eq!(
        receipts.len(),
        1,
        "the recorded local W receipt survives GC"
    );
    assert_eq!(receipts[0].replica_node_id, owner);
    assert_eq!(receipts[0].content_hash, version.content_hash);
    assert_eq!(plan.policy, DfsReplicationPolicy::new(1, 1).unwrap());
    assert_eq!(plan.desired_replica_node_ids, vec![owner.clone()]);
    capture_history(&mut dfs, &metadata);
    assert!(matches!(dfs.read_file_history(DfsFileHistoryReadRequest {
        identity: history_identity(&metadata), offset: 0, max_bytes: 64,
    }).unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"batch content"));
    assert_eq!(
        dfs.posix_stat(&metadata.path).unwrap().version,
        metadata.version
    );
    dfs.posix_write_file(
        &metadata.path,
        b"new data generation",
        DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .unwrap();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: metadata.path.clone(),
        identity: history_identity(&metadata),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&metadata),
            offset: 0,
            max_bytes: 64,
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    for _ in 0..8 {
        runtime
            .gc_metadata_history_on_current_primaries(DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
    }
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&written.object_id, &written.version)
            .unwrap()
            .is_none(),
        "the recorded generation loses its GC root after its final live user"
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .receipts_for_version(&written.object_id, &written.version)
            .unwrap()
            .is_empty(),
        "dead data-generation receipts remain collectible"
    );
    assert_eq!(
        dfs.posix_read_file(&metadata.path).unwrap(),
        b"new data generation"
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_capture_after_chmod_preserves_the_content_replication_plan() {
    capture_after_metadata_change_preserves_durability(false);
}

#[test]
fn file_history_capture_after_rename_and_write_history_gc_preserves_durability() {
    capture_after_metadata_change_preserves_durability(true);
}

fn capture_after_metadata_change_preserves_durability(collect_before_capture: bool) {
    let name = if collect_before_capture {
        "native-history-metadata-collected"
    } else {
        "native-history-metadata-pending"
    };
    let (mut runtime, owner, remotes, mut dfs) = native_history_repair_services(name, 3);
    let remote_a = remotes[0].clone();
    let remote_b = remotes[1].clone();
    deny_history_content_targets(&runtime, &owner, &remotes);
    let original = dfs
        .posix_write_file(
            "/services/history-metadata-generation",
            b"retained content",
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remote_a.clone(), remote_b.clone()],
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let original_intent_id = native_history_repairs(&dfs)[0].intent_id.clone();
    if collect_before_capture {
        fail_native_history_repairs(&runtime, &owner, &mut dfs);
        runtime.clear_faults();
        let (repairs, _settled) = drain_native_history_repairs(&runtime, &owner, &mut dfs);
        assert!(repairs.is_empty(), "the original write must first reach N");
    }
    let mut metadata = dfs.posix_set_mode(&original.path, 0o640).unwrap();
    if collect_before_capture {
        dfs.posix_rename(&original.path, "/services/renamed-history-source")
            .unwrap();
        metadata = dfs.posix_stat("/services/renamed-history-source").unwrap();
    }
    for _ in 0..8 {
        runtime
            .gc_metadata_history_on_current_primaries(DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
    }
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    native_history_owner_tick(&runtime, &owner);
    if collect_before_capture {
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent(&original.object_id, &original_intent_id)
                .unwrap()
                .is_none(),
            "the completed original write must lose its executable owner through GC"
        );
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent_receipt(&original.object_id, &original_intent_id)
                .unwrap()
                .is_none(),
            "repeated GC must collect its terminal retry witness"
        );
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_version(&original.object_id, &original.version)
                .unwrap()
                .is_some(),
            "the current successor still needs the original data-generation witness"
        );
    } else {
        assert_eq!(
            native_history_repairs(&dfs).len(),
            2,
            "current metadata-only generations must keep their pending N work after GC and restart"
        );
        assert_eq!(
            native_history_main_current_version(&dfs, &metadata.object_id),
            Some(metadata.version.clone())
        );
    }
    assert_ne!(metadata.version, original.version);
    assert_eq!(metadata.content_hash, original.content_hash);
    let record = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&metadata.object_id, &metadata.version)
        .unwrap()
        .unwrap();
    assert!(
        record.history_capture().is_none(),
        "metadata-only changes do not capture history"
    );
    let recorded_plan = record.content_durability.as_ref().unwrap();
    assert_eq!(
        recorded_plan.policy,
        DfsReplicationPolicy::new(1, 3).unwrap()
    );
    assert_eq!(recorded_plan.source_version, original.version);
    assert_eq!(recorded_plan.coordinator_node_id, owner);
    assert_eq!(
        recorded_plan
            .desired_replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [owner.clone(), remote_a.clone(), remote_b.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
    capture_history(&mut dfs, &metadata);
    let current = dfs
        .posix_write_file(
            &metadata.path,
            b"current content",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    native_history_owner_tick(&runtime, &owner);
    for _ in 0..8 {
        runtime
            .gc_metadata_history_on_current_primaries(DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
    }
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: history_identity(&metadata),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "after-restart".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    assert_native_history_recorded_or_settled_obligation(&dfs, &record);
    assert_eq!(
        dfs.posix_read_file(&current.path).unwrap(),
        b"current content"
    );
    assert_eq!(
        dfs.posix_stat(&current.path).unwrap().version,
        current.version
    );
    drop(dfs);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn file_history_compacted_queue_requires_actual_matching_target_records() {
    let (runtime, owner, remotes, mut dfs) =
        native_history_repair_services("native-history-compacted-queue-negatives", 2);
    deny_history_content_targets(&runtime, &owner, &remotes);
    let current = dfs
        .posix_write_file(
            "/services/compacted-queue-negative",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let request = preserve_request(&current, "preserve-queue-negative");
    let DfsFileHistoryPreserveOutcome::Retained { identity } = dfs
        .preserve_local_file_version(
            request.clone(),
            &b"retained"[..],
            DfsWriteOptions {
                replica_candidates: vec![owner.clone(), remotes[0].clone()],
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
    else {
        panic!("actual native W1 preservation")
    };
    let old = native_history_queue(&runtime, &owner)
        .into_iter()
        .find(|row| row.object_id == identity.object_id && row.version == identity.version)
        .expect("actual Main produced the original queue record");
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    for _ in 0..2 {
        runtime
            .gc_metadata_history_on_current_primaries(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                    .with_now_unix_nanos(core_test::now_unix_nanos()),
            )
            .unwrap();
    }
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_intent(&identity.object_id, &old.intent_id)
            .unwrap()
            .is_none()
    );
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: request.pin_id,
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    let actual_owner = native_history_repairs(&dfs)
        .into_iter()
        .find(|row| row.version == identity.version)
        .unwrap();
    assert_ne!(actual_owner.intent_id, old.intent_id);
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_intent(&identity.object_id, &actual_owner.intent_id)
            .unwrap()
            .is_some()
    );
    let canonical = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    assert!(!canonical.has_retention_grant());
    let receipts = dfs
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&identity.object_id, &identity.version)
        .unwrap();
    let before = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&current.object_id)
        .unwrap();
    // Establish the real failed dispatch while the original carrier denial is
    // still active, before malformed queue DATA can block ordinary restoration.
    fail_native_history_repairs(&runtime, &owner, &mut dfs);
    let store = LocalDfsRepairQueueStore::new(
        sinomo_dfs::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&owner)),
    );
    for fault in ["wrong-hash", "nonretained-target", "missing-target"] {
        // Negative scheduling DATA copied from a real queue producer; the new
        // negative key leaves every real stable identity and owner untouched.
        let mut bad = old.clone();
        bad.intent_id = format!("{}:negative:{fault}", old.intent_id);
        match fault {
            "wrong-hash" => bad.content_hash = content_hash(b"unrelated negative bytes"),
            "nonretained-target" => {
                bad.version = canonical.version.clone();
                bad.content_hash = canonical.content_hash.clone().unwrap();
            }
            "missing-target" => {
                bad.version = sinomo_core::Id::generate();
                assert!(
                    dfs.metadata_backend()
                        .unwrap()
                        .get_version(&bad.object_id, &bad.version)
                        .unwrap()
                        .is_none()
                );
            }
            _ => unreachable!(),
        }
        let mut queued = store.view().unwrap().records;
        queued.push(bad.clone());
        store.replace_records(&queued).unwrap();
        let pass = runtime
            .maintenance_tick(&owner, Duration::from_secs(5))
            .unwrap();
        assert_eq!(pass.omitted_kv_event_count, 0);
        let error = pass
            .outcome
            .expect_err("mismatched queued DATA must not restore an owner");
        assert!(error.contains(&bad.intent_id), "{error}");
        assert_eq!(
            dfs.metadata_backend()
                .unwrap()
                .receipts_for_version(&identity.object_id, &identity.version)
                .unwrap(),
            receipts
        );
        assert_eq!(
            dfs.metadata_backend()
                .unwrap()
                .get_object(&current.object_id)
                .unwrap(),
            before
        );
        let healed = store
            .view()
            .unwrap()
            .records
            .into_iter()
            .filter(|row| row.intent_id != bad.intent_id)
            .collect::<Vec<_>>();
        store.replace_records(&healed).unwrap();
    }
    runtime.clear_faults();
    let (repairs, settled) = drain_native_history_repairs(&runtime, &owner, &mut dfs);
    assert!(repairs.is_empty());
    assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"canonical");
    let target = settled
        .tables
        .versions
        .iter()
        .find(|row| row.object_id == identity.object_id && row.version == identity.version)
        .unwrap();
    let holder = runtime.client(&remotes[0]).unwrap();
    let hash = target.content_hash.as_deref().unwrap();
    let bytes = holder.content().read_blob(hash).unwrap();
    assert_eq!(bytes, b"retained");
    assert_eq!(content_hash(&bytes), hash);
    assert_eq!(Some(bytes.len() as u64), target.size_bytes);
    assert!(
        settled
            .tables
            .receipts
            .iter()
            .any(|receipt| receipt.replica_node_id == remotes[0]
                && receipt.receipt.file_system_id == identity.file_system_id
                && receipt.receipt.object_id == identity.object_id
                && receipt.receipt.committed_version == identity.version
                && receipt.content_hash.as_deref() == Some(hash))
    );
    drop(holder);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_repin_after_compaction_has_fresh_repair_ownership() {
    repin_after_gc_has_fresh_repair_ownership(false, false);
}

#[test]
fn file_history_repin_after_retry_receipt_eviction_keeps_replication_policy() {
    repin_after_gc_has_fresh_repair_ownership(true, false);
}

#[test]
fn file_history_clustered_pin_operations_remain_usable_after_gc() {
    repin_after_gc_has_fresh_repair_ownership(true, true);
}

fn repin_after_gc_has_fresh_repair_ownership(evict_receipt: bool, clustered: bool) {
    let name = match (evict_receipt, clustered) {
        (false, false) => "native-history-compacted-repair-owner",
        (true, false) => "native-history-compacted-repair-receipt",
        (_, true) => "native-history-compacted-repair-clustered",
    };
    let (mut runtime, owner, remotes, mut dfs) = native_history_repair_services(name, 3);
    let a = remotes[0].clone();
    let b = remotes[1].clone();
    deny_history_content_targets(&runtime, &owner, &remotes);
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
                replica_candidates: vec![owner.clone(), a.clone(), b.clone()],
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap()
    else {
        panic!("W reached");
    };
    let original_intent = native_history_repairs(&dfs)[0].intent_id.clone();
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    })
    .unwrap();
    runtime
        .gc_metadata_history_on_current_primaries(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(core_test::now_unix_nanos()),
        )
        .unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_intent_receipt(&current.object_id, &original_intent)
            .unwrap()
            .is_some()
    );
    {
        let actual_core = runtime.metadata_primary_service(&current.path).unwrap();
        let backend = actual_core.metadata_backend().unwrap();
        let stored = backend
            .object_write_intent_history(&identity.object_id)
            .unwrap();
        assert!(
            !stored
                .intents
                .iter()
                .any(|record| record.intent_id == original_intent),
            "actual GC removes the stored full original owner"
        );
        let compact = stored
            .receipts
            .iter()
            .find(|receipt| receipt.intent_id == original_intent)
            .unwrap();
        let projected = backend
            .get_intent(&identity.object_id, &original_intent)
            .unwrap()
            .unwrap();
        assert_eq!(projected.intent_id, compact.intent_id);
        assert_eq!(projected.target_version, compact.target_version);
        assert_eq!(projected.content_hash, compact.content_hash);
        assert_eq!(
            projected.publication, compact.publication,
            "a valid compact logical projection is present despite stored full absence"
        );
    }
    if evict_receipt {
        runtime
            .gc_metadata_history_on_current_primaries(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                    .with_now_unix_nanos(core_test::now_unix_nanos()),
            )
            .unwrap();
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent_receipt(&current.object_id, &original_intent)
                .unwrap()
                .is_none()
        );
        let stored = runtime
            .metadata_primary_service(&current.path)
            .unwrap()
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&identity.object_id)
            .unwrap();
        let retained_owner = stored
            .intents
            .iter()
            .find(|record| {
                record.target_version == identity.version && record.publication.is_retained_repair()
            })
            .expect("actual full owner survives retry-receipt eviction");
        let before_restart = native_history_queue(&runtime, &owner)
            .into_iter()
            .filter(|row| row.intent_id == retained_owner.intent_id)
            .collect::<Vec<_>>();
        assert_eq!(before_restart.len(), 2);
        drop(dfs);
        runtime.stop_node(&owner).unwrap();
        runtime.reopen_node(owner.clone()).unwrap();
        native_history_owner_tick(&runtime, &owner);
        let dormant = native_history_queue(&runtime, &owner)
            .into_iter()
            .filter(|row| row.intent_id == retained_owner.intent_id)
            .collect::<Vec<_>>();
        assert_eq!(dormant.len(), before_restart.len());
        for original in &before_restart {
            let actual = dormant
                .iter()
                .find(|row| row.missing_replica_node_id == original.missing_replica_node_id)
                .unwrap();
            let mut expected = original.clone();
            expected.next_retry_unix_nanos = u64::MAX;
            assert_eq!(
                actual, &expected,
                "inactive native restart parks scheduling without discarding or changing custody"
            );
        }
        dfs = runtime.client(&owner).unwrap();
    }
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .is_some()
    );
    // Keep missing-N denied through the original live GC cut. Once each
    // target has a real failed native attempt, a negative preclaim guard blocks
    // only later attempts; it preserves that first native backoff and custody.
    let negative_scope = if evict_receipt {
        let core = runtime.metadata_primary_service(&current.path).unwrap();
        let stored = core
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&identity.object_id)
            .unwrap();
        let actual = stored
            .intents
            .into_iter()
            .find(|row| {
                row.publication.is_retained_repair()
                    && row.status == DfsWriteIntentStatus::Committed
                    && row.file_system_id == identity.file_system_id
                    && row.object_id == identity.object_id
                    && row.target_version == identity.version
            })
            .expect("actual stored full retained owner before live repin");
        drop(core);
        Some(actual)
    } else {
        None
    };
    let negative_cut = runtime.rpc_observations().len();
    let negative_guard = negative_scope.as_ref().map(|scope| {
        runtime
            .deny_repair_claims_after_failed_rpc(&owner, scope)
            .unwrap()
    });
    let repin = DfsFileHistoryPinRequest {
        path: current.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    dfs.file_history_pin(repin.clone()).unwrap();
    let jobs = native_history_repairs(&dfs);
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
        native_history_intent_policy(&dfs, &jobs[0]),
        DfsReplicationPolicy::new(1, 3).unwrap()
    );
    let repair_intent = jobs[0].intent_id.clone();
    {
        let actual_core = runtime.metadata_primary_service(&current.path).unwrap();
        let stored = actual_core
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&identity.object_id)
            .unwrap();
        assert!(
            stored
                .intents
                .iter()
                .any(|record| record.intent_id == repair_intent
                    && record.publication.is_retained_repair()
                    && record.status == DfsWriteIntentStatus::Committed
                    && record.target_version == identity.version),
            "the replacement is an actual stored FULL RetainedRepair owner"
        );
        assert!(
            !stored
                .receipts
                .iter()
                .any(|receipt| receipt.intent_id == repair_intent),
            "a compact projection cannot substitute for the stored replacement owner"
        );
    }

    assert_eq!(native_history_queue(&runtime, &owner).len(), 2);
    assert_eq!(
        dfs.posix_stat(&current.path).unwrap().version,
        current.version
    );
    assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"canonical");
    if evict_receipt {
        let gc = || {
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(core_test::now_unix_nanos())
        };
        for _ in 0..8 {
            runtime
                .gc_metadata_history_on_current_primaries(gc())
                .unwrap();
        }
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent(&current.object_id, &repair_intent)
                .unwrap()
                .is_some()
        );
        let before =
            current_history_shard_sequence(&mut dfs, &current.file_system_id, &current.path);
        assert!(
            dfs.file_history_pin(repin)
                .unwrap_err()
                .to_string()
                .contains("expired")
        );
        assert_eq!(
            current_history_shard_sequence(&mut dfs, &current.file_system_id, &current.path),
            before,
            "rejected repin cannot author another operation on its actual owning shard"
        );
        // The ordinary producer ran while the original denials remained active.
        // Inspect its exact failure instead of requesting an intentionally
        // denied second claim after the eight GC passes.
        let scope = negative_scope.as_ref().unwrap();
        let observations = runtime.rpc_observations();
        let failed = observations
            .iter()
            .skip(negative_cut)
            .filter(|event| {
                event.source == owner
                    && event.origin == sinomo_dfs::test_support::DfsTestRpcOrigin::RegisteredHost
                    && event.phase
                        == sinomo_dfs::test_support::DfsTestRpcPhase::EndedWithoutResponse
                    && event.route == sinomo_dfs::DFS_DATA_REPAIR_REPLICA_ROUTE
                    && event.dfs_intent_id.as_deref() == Some(scope.intent_id.as_str())
                    && event.dfs_file_system_id.as_ref() == Some(&identity.file_system_id)
                    && event.dfs_object_id.as_ref() == Some(&identity.object_id)
                    && event.dfs_version.as_ref() == Some(&identity.version)
            })
            .collect::<Vec<_>>();
        assert!(
            !failed.is_empty(),
            "the missing replica must be grounded in an actual failed native content RPC for this full owner"
        );
        for target in [&a, &b] {
            assert!(
                failed
                    .iter()
                    .filter(|event| &event.target == target)
                    .count()
                    <= 1,
                "the negative GC cut prevents additional native attempts for the same target"
            );
        }
        let actual_core = runtime.metadata_primary_service(&current.path).unwrap();
        let missing = native_history_repairs(&actual_core);
        assert_eq!(
            missing.len(),
            2,
            "both actual missing-N obligations remain unresolved throughout live GC"
        );
        assert_eq!(
            missing
                .iter()
                .map(|row| (
                    row.intent_id.clone(),
                    row.file_system_id.clone(),
                    row.object_id.clone(),
                    row.version.clone(),
                    row.content_hash.clone(),
                    row.missing_replica_node_id.clone(),
                ))
                .collect::<std::collections::BTreeSet<_>>(),
            [&a, &b]
                .into_iter()
                .map(|target| (
                    scope.intent_id.clone(),
                    identity.file_system_id.clone(),
                    identity.object_id.clone(),
                    identity.version.clone(),
                    scope.content_hash.clone(),
                    target.clone(),
                ))
                .collect::<std::collections::BTreeSet<_>>()
        );
        drop(actual_core);

        let current_head = dfs
            .test_current_read_object_metadata(&current.object_id)
            .unwrap();
        let peer_heads_before = [&a, &b]
            .map(|node| native_history_settled_peer_head(&runtime, &owner, node, &current_head));
        drop(negative_guard);
        runtime.clear_faults();
        let (repairs, settled) = drain_native_history_repairs(&runtime, &owner, &mut dfs);
        assert!(repairs.is_empty());
        for (index, replica_node) in [&a, &b].into_iter().enumerate() {
            let replica = runtime.client(replica_node).unwrap();
            assert_native_history_holder_bytes(
                &settled,
                &owner,
                &replica,
                replica_node,
                &identity,
                b"retained",
            );
            assert_eq!(
                replica
                    .metadata_backend()
                    .unwrap()
                    .get_object(&current.object_id)
                    .unwrap(),
                peer_heads_before[index],
                "retained repair leaves the actual local current view unchanged"
            );
        }
        assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"canonical");
        dfs.file_history_pin(DfsFileHistoryPinRequest {
            path: current.path.clone(),
            identity: identity.clone(),
            operation: DfsFileHistoryPinOperation::Unpin {
                pin_id: request.pin_id,
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
        for _ in 0..8 {
            runtime
                .gc_metadata_history_on_current_primaries(gc())
                .unwrap();
        }
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_version(&identity.object_id, &identity.version)
                .unwrap()
                .is_none(),
            "maintenance ownership must not become a retention root"
        );
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent(&current.object_id, &repair_intent)
                .unwrap()
                .is_none()
        );
        assert!(
            dfs.metadata_backend()
                .unwrap()
                .get_intent_receipt(&current.object_id, &repair_intent)
                .unwrap()
                .is_none()
        );
    }
    drop(dfs);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn file_history_current_artifact_is_readable_without_becoming_a_capture() {
    let root = temp_root("history-current-artifact");
    let owner = NodeId::from_legacy_named("history-artifact-owner").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_current_artifact_is_readable_without_becoming_a_capture",
        owner.clone(),
    );
    let stat = dfs
        .posix_write_file(
            "/services/artifact",
            b"artifact bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let mut artifact = dfs
        .test_current_read_object_metadata(&stat.object_id)
        .unwrap();
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
            .history_capture()
            .is_none()
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_repin_reactivates_only_the_retained_versions_repair() {
    let (mut runtime, owner, remotes, mut dfs) =
        native_history_repair_services("native-history-explicit-repin-repair", 2);
    let remote = remotes[0].clone();
    deny_history_content_targets(&runtime, &owner, &remotes);
    let stat = dfs
        .posix_write_file(
            "/services/repin",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    // The normal preserve operation can dispatch missing-N work before its
    // caller returns. Retain that original observation cut for this owner.
    let negative_cut = runtime.rpc_observations().len();
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
    let jobs = native_history_repairs(&dfs);
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
        native_history_repairs(&dfs),
        jobs.as_slice(),
        "another pin still owns the work"
    );
    // Observe the first genuine denied transfer while the retained root is
    // live. Keep only repeat claims denied across release and cold restart;
    // repin must wake the original persisted backoff before the healing cut.
    let actual_owner = {
        let core = runtime.metadata_primary_service(&stat.path).unwrap();
        let stored = core
            .metadata_backend()
            .unwrap()
            .object_write_intent_history(&identity.object_id)
            .unwrap();
        stored
            .intents
            .into_iter()
            .find(|record| record.intent_id == intent_id)
            .expect("actual full owner for the retained version")
    };
    assert_eq!(actual_owner.status, DfsWriteIntentStatus::Committed);
    assert!(matches!(
        actual_owner.publication,
        DfsWritePublication::Retained { .. }
    ));
    assert_eq!(actual_owner.file_system_id, identity.file_system_id);
    assert_eq!(actual_owner.object_id, identity.object_id);
    assert_eq!(actual_owner.target_version, identity.version);
    let negative_guard = runtime
        .deny_repair_claims_after_failed_rpc(&owner, &actual_owner)
        .unwrap();
    fail_native_history_repairs(&runtime, &owner, &mut dfs);
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .skip(negative_cut)
            .any(|event| {
                event.source == owner
                    && event.target == remote
                    && event.origin == sinomo_dfs::test_support::DfsTestRpcOrigin::RegisteredHost
                    && event.phase
                        == sinomo_dfs::test_support::DfsTestRpcPhase::EndedWithoutResponse
                    && event.route == sinomo_dfs::DFS_DATA_REPAIR_REPLICA_ROUTE
                    && event.dfs_intent_id.as_deref() == Some(intent_id.as_str())
                    && event.dfs_file_system_id.as_ref() == Some(&identity.file_system_id)
                    && event.dfs_object_id.as_ref() == Some(&identity.object_id)
                    && event.dfs_version.as_ref() == Some(&identity.version)
            }),
        "the original live retained owner must have its own failed carrier attempt"
    );
    let before_release = native_history_queue(&runtime, &owner);
    assert_eq!(before_release.len(), 1);
    assert_eq!(before_release[0].intent_id, intent_id);
    assert_eq!(before_release[0].object_id, identity.object_id);
    assert_eq!(before_release[0].version, identity.version);
    assert_eq!(before_release[0].missing_replica_node_id, remote);
    assert_eq!(
        before_release[0].transfer_kind,
        Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
    );
    assert_eq!(
        before_release[0].transfer_delivery_state,
        Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
    );
    assert!(!before_release[0].in_flight && before_release[0].transfer_request_id.is_none());
    assert!(before_release[0].completed_transfer_request_id.is_none());
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "independent-root".into(),
        },
        operation_id: "repin-release-last-root".into(),
    })
    .unwrap();
    native_history_owner_tick(&runtime, &owner);
    assert!(
        native_history_repairs(&dfs).is_empty(),
        "released history cannot retain executable repair jobs"
    );
    let mut dormant = before_release;
    dormant[0].next_retry_unix_nanos = u64::MAX;
    assert_eq!(
        native_history_queue(&runtime, &owner),
        dormant,
        "the stored retained owner preserves every transfer fact while its last root is released"
    );
    // A real cold request lane rebuilds only from stored rows and real receipts.
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    native_history_owner_tick(&runtime, &owner);
    assert!(
        native_history_repairs(&dfs).is_empty(),
        "released roots cannot reactivate work after actual restart"
    );
    assert_eq!(
        native_history_queue(&runtime, &owner),
        dormant,
        "normal restart preserves the same dormant physical custody"
    );
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
        native_history_repairs(&dfs),
        jobs.as_slice(),
        "an explicit root reactivates its missing N replica"
    );
    assert!(
        native_history_queue(&runtime, &owner)
            .iter()
            .any(|record| record.intent_id == intent_id)
    );
    assert_eq!(
        native_history_queue(&runtime, &owner).len(),
        1,
        "the pin publication must durably enqueue newly actionable work"
    );
    let reactivated = native_history_queue(&runtime, &owner);
    let mut expected_reactivated = dormant;
    assert_ne!(reactivated[0].next_retry_unix_nanos, u64::MAX);
    expected_reactivated[0].next_retry_unix_nanos = reactivated[0].next_retry_unix_nanos;
    assert_eq!(
        reactivated, expected_reactivated,
        "repin wakes only scheduling and preserves the whole original failed transfer"
    );
    let current_head = dfs
        .test_current_read_object_metadata(&stat.object_id)
        .unwrap();
    let peer_head_before =
        native_history_settled_peer_head(&runtime, &owner, &remote, &current_head);
    runtime.clear_faults();
    drop(negative_guard);
    let (repairs, settled) = drain_native_history_repairs(&runtime, &owner, &mut dfs);
    assert!(repairs.is_empty());
    assert!(
        !native_history_queue(&runtime, &owner)
            .iter()
            .any(|record| record.intent_id == intent_id)
    );
    let replica = runtime.client(&remote).unwrap();
    assert_native_history_holder_bytes(
        &settled,
        &owner,
        &replica,
        &remote,
        &identity,
        b"retained alternative",
    );
    assert_eq!(
        replica
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap(),
        peer_head_before,
        "retained repair leaves the actual local current view unchanged"
    );
    assert_eq!(dfs.posix_read_file(&stat.path).unwrap(), b"canonical");
    drop(replica);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_repin_after_restart_does_not_rewind_a_canonical_write_intent() {
    let root = temp_root("history-repin-canonical-intent");
    let (mut runtime, owner, remotes, mut dfs) = native_history_repair_services(
        "file_history_repin_after_restart_does_not_rewind_a_canonical_write_intent",
        3,
    );
    let remote = remotes[0].clone();
    // Re-pin selects its repair target from the current eligible members.
    // Keep every remote content target unavailable so background repair cannot
    // satisfy the retained obligation before the canonical intent checks.
    deny_history_content_targets(&runtime, &owner, &remotes);
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
    assert!(native_history_repairs(&dfs).is_empty());
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    assert!(native_history_repairs(&dfs).is_empty());
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: first.path.clone(),
        identity: history_identity(&first),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "checkpoint".into(),
        },
        operation_id: "canonical-repin".into(),
    })
    .unwrap();
    assert_eq!(native_history_repairs(&dfs).len(), 1);
    assert_eq!(native_history_repairs(&dfs)[0].version, first.version);
    assert_eq!(
        native_history_main_current_version(&dfs, &current.object_id),
        Some(current.version.clone())
    );
    dfs.posix_write_file(
        &current.path,
        b"next",
        DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .unwrap();
    assert_eq!(dfs.posix_read_file(&current.path).unwrap(), b"next");
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_expired_rule_retires_repairs_even_during_retry_backoff() {
    let root = temp_root("history-expired-repair");
    let (runtime, owner, remotes, mut dfs) = native_history_repair_services(
        "file_history_expired_rule_retires_repairs_even_during_retry_backoff",
        3,
    );
    let remote = remotes[0].clone();
    // Managed placement can select any eligible remote despite candidate hints.
    // Keep the real W1/N2 plan, and deny every actual missing-N target.
    let denied_targets = remotes.clone();
    deny_history_content_targets(&runtime, &owner, &denied_targets);
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
    {
        let mut policy = decode_history_policy(fs.retention_policies.get(HISTORY_MODULE_ID));
        policy.rules.push(DfsFileHistoryRule {
            path: first.path.clone(),
            retention: DfsFileHistoryRetention {
                keep_last: 0,
                keep_for_seconds: 1,
            },
        });
        install_history_policy(&mut fs, policy).unwrap();
    }
    apply_history_fixture_policy(&mut dfs, &fs);
    dfs.posix_write_file(
        &first.path,
        b"current",
        DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .unwrap();
    let retained = dfs
        .metadata_backend()
        .unwrap()
        .get_version(&first.object_id, &first.version)
        .unwrap()
        .unwrap();
    assert!(retained.history_capture().unwrap().policy_capture.is_some());
    let recorded_plan = retained.content_durability.as_ref().unwrap();
    assert!(
        recorded_plan
            .desired_replica_node_ids
            .iter()
            .all(|node| node == &owner || denied_targets.contains(node)),
        "expiry/backoff setup must block every actual captured target: plan={:?} denied={denied_targets:?}",
        recorded_plan.desired_replica_node_ids,
    );
    let store = LocalDfsRepairQueueStore::new(
        sinomo_dfs::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&owner)),
    );
    let mut records = store.view().unwrap().records;
    assert_eq!(records.len(), 1);
    let original = records[0].clone();
    records[0].attempts = 1;
    records[0].next_retry_unix_nanos = core_test::now_unix_nanos().saturating_add(60_000_000_000);
    // Negative scheduling only: retain the real Main-generated identity and
    // durable ownership while delaying its retry through the existing queue.
    store.replace_records(&records).unwrap();
    let actual = store.view().unwrap().records;
    assert_eq!(actual, records);
    let mut preserved_transfer = actual[0].clone();
    preserved_transfer.attempts = original.attempts;
    preserved_transfer.next_retry_unix_nanos = original.next_retry_unix_nanos;
    assert_eq!(
        preserved_transfer, original,
        "the scheduling fixture preserves every actual native transfer fact"
    );
    assert_eq!(
        actual[0].transfer_kind,
        Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
    );

    assert_eq!(actual[0].intent_id, original.intent_id);
    assert_eq!(actual[0].object_id, original.object_id);
    assert_eq!(actual[0].version, original.version);
    assert_eq!(actual[0].content_hash, original.content_hash);
    assert_eq!(
        actual[0].missing_replica_node_id,
        original.missing_replica_node_id
    );
    // Exercise the production wall-clock expiry boundary while the transport
    // retry remains far in the future; no network work should run here.
    std::thread::sleep(Duration::from_secs(2));
    assert!(history_entries(&mut dfs, &first.path).is_empty());
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&first),
            offset: 0,
            max_bytes: 64,
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    assert_history_repair_inventory(&dfs, &first.object_id);
    assert_eq!(
        dfs.metadata_backend()
            .unwrap()
            .metadata_summary()
            .unwrap()
            .pending_repair_count,
        0
    );
    // The actual Main must retire an expired retained target before consulting
    // the queue's still-future network retry. The facade owns no repair jobs.
    native_history_owner_tick(&runtime, &owner);
    assert!(native_history_repairs(&dfs).is_empty());
    let current = native_history_queue(&runtime, &owner);
    eprintln!(
        "native expired repair: original={original:?} scheduled={actual:?} after={current:?}"
    );
    if actual[0].transfer_request_id.is_none()
        && !actual[0].in_flight
        && actual[0].transfer_kind.is_some()
        && (actual[0].transfer_delivery_state
            == Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
            || actual[0].completed_transfer_request_id.is_some())
    {
        // Main's exact accepted terminal also settles the old carrier claim.
        // PossiblyApplied remains its delivery history, not unresolved custody
        // after that recorded completion. Neither outcome proves replica bytes.
        assert!(current.is_empty());
    } else {
        assert_eq!(
            current, actual,
            "expiry withdraws execution while retaining every actual uncertain transfer fact"
        );
    }
    assert_eq!(dfs.posix_read_file(&first.path).unwrap(), b"current");
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_pin_receipt_replay_recovers_its_repair_queue_handoff() {
    let (mut runtime, owner, remotes, mut dfs) =
        native_history_repair_services("native-history-pin-receipt-replay", 2);
    let remote = remotes[0].clone();
    deny_history_content_targets(&runtime, &owner, &remotes);
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
    let request = DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity,
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "second-root".into(),
        },
        operation_id: "publish-root-before-queue-handoff".into(),
    };
    let first = dfs.file_history_pin(request.clone()).unwrap();
    let before = current_history_shard_sequence(&mut dfs, &stat.file_system_id, &stat.path);
    let expected_jobs = native_history_repairs(&dfs);
    assert_eq!(expected_jobs.len(), 1);
    drop(dfs);
    // Restart the actual host. An independent physical client may hydrate
    // during completed-handoff rebind; the replay below enters the real Main
    // handler with the original requester and command identity instead.
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    let replay = DfsContentReferenceRequest {
        namespace: HISTORY_MODULE_ID.into(),
        path: request.path.clone(),
        identity: request.identity.clone(),
        operation_id: request.operation_id.clone(),
        operation: DfsContentReferenceOperation::Retain {
            reference_id: "second-root".into(),
        },
    };
    let actual = dfs
        .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
            request.operation_id.clone(),
            request.identity.file_system_id.clone(),
            owner.clone(),
            DfsNamespaceOperation::ContentReference { request: replay },
            Duration::from_secs(5),
            false,
            false,
        )
        .unwrap();
    assert!(matches!(actual.result, DfsNamespaceResult::Applied));
    assert_eq!(dfs.file_history_pin(request).unwrap(), first);
    assert_eq!(
        current_history_shard_sequence(&mut dfs, &stat.file_system_id, &stat.path),
        before,
        "the actual Main replay must reuse its durable publication"
    );
    assert_eq!(native_history_repairs(&dfs), expected_jobs.as_slice());
    let queue = LocalDfsRepairQueueStore::new(
        sinomo_dfs::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&owner)),
    )
    .view()
    .unwrap();
    assert_eq!(
        queue.records.len(),
        1,
        "Main must checkpoint the replayed repair handoff"
    );
    assert_eq!(queue.records[0].intent_id, expected_jobs[0].intent_id);
    assert_eq!(queue.records[0].object_id, expected_jobs[0].object_id);
    assert_eq!(queue.records[0].version, expected_jobs[0].version);
    assert_eq!(
        queue.records[0].missing_replica_node_id,
        expected_jobs[0].missing_replica_node_id
    );
    assert!(
        runtime.rpc_observations().iter().any(|event| {
            event.source == owner
                && event.target == owner
                && event.route == sinomo_dfs::DFS_DATA_NAMESPACE_ROUTE
                && event.origin == sinomo_dfs::test_support::DfsTestRpcOrigin::RegisteredHost
                && event.phase == sinomo_dfs::test_support::DfsTestRpcPhase::Completed
        }),
        "the receipt must pass through the actual authenticated Main handler"
    );
    assert_eq!(
        dfs.test_current_read_object_metadata(&stat.object_id)
            .unwrap()
            .version,
        stat.version
    );
    drop(dfs);
    runtime.clear_faults();
    runtime.close().unwrap();
}

#[test]
fn file_history_exact_version_read_rejects_unretained_and_unanchored_bytes() {
    let root = temp_root("history-exact-read-admission");
    let owner = NodeId::from_legacy_named("history-exact-reader").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_exact_version_read_rejects_unretained_and_unanchored_bytes",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
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
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_exact_version_reads_use_primary_and_remote_content() {
    {
        let (mut runtime, owner, requester, mut primary, mut client) =
            native_history_remote_services(
                "file_history_exact_version_reads_use_primary_and_remote_content",
            );
        let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
        let bytes = vec![42; (64 * 1024) as usize + 31];
        let first = primary
            .posix_write_file("/services/exact-remote", &bytes, options.clone())
            .unwrap();
        let stale = primary
            .test_current_read_object_metadata(&first.object_id)
            .unwrap();

        capture_history(&mut primary, &first);

        let current = primary
            .posix_write_file(&first.path, b"current bytes", options.clone())
            .unwrap();
        let empty = primary
            .posix_write_file("/services/exact-empty", b"", options)
            .unwrap();

        // A cached old head and exact old bytes must not grant read access.
        client.add_transient_object_metadata(stale).unwrap();
        client
            .test_store_command_bytes(
                &first.object_id,
                &first.version,
                first.content_hash.as_deref().unwrap(),
                &bytes,
            )
            .unwrap();

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

            assert_eq!(retained?, bytes);
            // Keep a full local copy after a successful remote read, then
            // release its root. Availability must not imply admission.
            client.test_store_command_bytes(
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

            Ok::<_, DfsError>(())
        })();

        result.unwrap();
        drop(client);
        drop(primary);
        runtime.close().unwrap();
    }
}

#[test]
fn native_history_retained_publication_and_restart_preserve_current_head() {
    let owner = NodeId::generate();
    let (mut runtime, mut dfs) =
        native_history_core_service("native-retained-current-head", owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file(
            "/services/preserved-local-file",
            b"canonical",
            options.clone(),
        )
        .unwrap();
    let request = preserve_request(&stat, "native-preserve-once");
    let outcome = dfs
        .preserve_local_file_version(
            request.clone(),
            &b"disconnected local bytes"[..],
            options.clone(),
        )
        .unwrap();
    let DfsFileHistoryPreserveOutcome::Retained { identity } = outcome.clone() else {
        panic!("actual W1 receipt required")
    };
    assert_ne!(identity.version, stat.version);
    assert_eq!(dfs.posix_stat(&stat.path).unwrap().version, stat.version);
    assert_eq!(dfs.posix_read_file(&stat.path).unwrap(), b"canonical");
    assert!(
        matches!(dfs.read_file_history(DfsFileHistoryReadRequest { identity: identity.clone(), offset: 0, max_bytes: 64 }).unwrap(),
        DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"disconnected local bytes")
    );
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: stat.path.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: request.pin_id.clone(),
        },
        operation_id: "native-release-retained".into(),
    })
    .unwrap();
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    assert_eq!(
        dfs.preserve_local_file_version(request, &b"disconnected local bytes"[..], options)
            .unwrap(),
        outcome
    );
    assert_eq!(dfs.posix_stat(&stat.path).unwrap().version, stat.version);
    assert_eq!(dfs.posix_read_file(&stat.path).unwrap(), b"canonical");
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&stat.object_id)
            .unwrap()
            .is_empty()
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_retained_replica_staging_does_not_publish_or_block_current_content() {
    use sinomo_core::network::ObjectRpcBodySource;
    use sinomo_dfs::test_support::{
        DfsTestFaultAction, DfsTestFaultRule, DfsTestRpcOrigin, DfsTestStageExchangeCapture,
    };
    use std::sync::Mutex;
    let (runtime, owner, remotes, mut dfs) =
        native_history_repair_services("native-history-genuine-retained-stage", 2);
    let remote = remotes[0].clone();
    let path = "/services/stage-local-file";
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file(path, b"canonical", options.clone())
        .unwrap();
    let object = dfs
        .test_current_read_object_metadata(&stat.object_id)
        .unwrap();
    let captured = Arc::new(Mutex::new(DfsTestStageExchangeCapture::default()));
    runtime.install_fault(DfsTestFaultRule {
        source: Some(owner.clone()),
        target: Some(remote.clone()),
        route: Some(sinomo_dfs::DFS_DATA_STAGE_BLOB_ROUTE.into()),
        call_id: None,
        origin: Some(DfsTestRpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: DfsTestFaultAction::CaptureStageExchangeAndLoseAfterDispatch(Arc::clone(&captured)),
    });
    let pending = dfs
        .preserve_local_file_version(
            preserve_request(&stat, "retained-stage"),
            &b"local candidate"[..],
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
        panic!("losing the genuine remote Stage reply prevents W");
    };
    let exchange = {
        let capture = captured.lock().unwrap();
        assert_eq!(
            capture.dropped_count, 0,
            "all native Stage observations must be retained"
        );
        assert!(
            !capture.exchanges.is_empty(),
            "ordinary preservation must dispatch a real Stage"
        );
        capture.exchanges[0].clone()
    };
    assert_eq!(exchange.source_node_id, owner);
    assert_eq!(exchange.target_node_id, remote);
    assert_eq!(
        exchange.observation.origin,
        DfsTestRpcOrigin::RegisteredHost
    );
    assert!(
        matches!(&exchange.signer, sinomo_core::service::ServiceSigner::Remote { node_id, .. } if node_id == owner.as_str())
    );
    let DfsNetworkMessage::StageRetainedBlob(command) = &exchange.command.routed_message.message
    else {
        panic!("actual retained producer command");
    };
    // Public Pending identifies the caller's original operation alias;
    // actual Stage uses the Main producer's durable intent identity. Bind
    // both through that real stored record, never by guessing equal strings.
    let durable_intent = dfs
        .metadata_backend()
        .unwrap()
        .get_intent(&command.object_id, &command.intent_id)
        .unwrap()
        .expect("actual Main retained producer saved this captured Stage intent");
    assert_eq!(durable_intent.intent_id, command.intent_id);
    assert_eq!(
        durable_intent.operation_id.as_deref(),
        Some("retained-stage")
    );
    assert_eq!(intent_id, "retained-stage");
    assert_eq!(durable_intent.object_id, identity.object_id);
    assert_eq!(durable_intent.file_system_id, identity.file_system_id);
    assert_eq!(durable_intent.target_version, identity.version);
    assert_eq!(durable_intent.coordinator_node_id, exchange.source_node_id);
    assert_eq!(command.version, identity.version);
    assert!(command.authority_shard.is_some());
    assert!(matches!(&exchange.response.message,
        DfsNetworkMessage::StageBlobResponse(reply)
        if matches!(&reply.result, DfsBlobTransferResult::Durable { content_hash: hash }
            if hash == &command.content_hash)));
    let peer = runtime.client(&remote).unwrap();
    assert_eq!(
        peer.content().read_blob(&command.content_hash).unwrap(),
        b"local candidate"
    );
    assert!(
        peer.metadata_backend()
            .unwrap()
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .is_none()
    );
    drop(peer);
    assert_eq!(
        dfs.test_current_read_object_metadata(&stat.object_id)
            .unwrap(),
        object
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_version(&identity.object_id, &identity.version)
            .unwrap()
            .is_none(),
        "actual replica staging is not retained metadata publication"
    );

    // Negative replay uses the SAME captured command/routing IDs and the
    // existing native authenticated receiver, with no inserted pending state.
    runtime.clear_faults();
    // Retain only a negative transport denial for subsequent automatic Main
    // retries. Explicit native replay below does not match RegisteredHost.
    // Thus the original lost reply stays unobserved by the source while this
    // test checks the receiver's staging boundary independently.
    deny_history_content_targets(&runtime, &owner, &[remote.clone()]);
    let replay = |routed: DfsRoutedMessage, bytes: Vec<u8>| {
        let mut request = exchange.request.clone();
        if routed != exchange.command.routed_message {
            let mut command = exchange.command.clone();
            command.routed_message = routed;
            let mut attachment = exchange
                .attachment
                .clone()
                .expect("normal reader Stage has an attachment");
            attachment.total_size_bytes = bytes.len() as u64;
            if let DfsNetworkMessage::StageRetainedBlob(stage) = &command.routed_message.message {
                attachment.content_hash = Some(stage.content_hash.clone());
            }
            request.body = sinomo_core::network::SinomoObjectCodec::network_frame()
                .encode(&DfsAttachmentMessageCommand {
                    command,
                    attachment,
                })
                .unwrap();
        }
        // Exact positive replay preserves the original raw head and metadata,
        // including the original outer request ID, frame/message IDs and route.
        let call =
            sinomo_core::network::ObjectRpcCall::new(request, ObjectRpcBodySource::bytes(bytes));
        let response = runtime
            .rpc_call(&owner, &remote, call, Duration::from_secs(5))
            .unwrap();
        let reply = decode_dfs_message_rpc_response(&response).unwrap();
        assert_eq!(reply.request_id, exchange.command.request_id);
        reply
            .outbound_messages
            .into_iter()
            .find_map(|routed| {
                let DfsNetworkMessage::StageBlobResponse(reply) = routed.message else {
                    return None;
                };
                Some(reply.result)
            })
            .expect("actual native Stage response")
    };
    let mut foreign = exchange.command.routed_message.clone();
    let DfsNetworkMessage::StageRetainedBlob(foreign_command) = &mut foreign.message else {
        unreachable!()
    };
    foreign_command.file_system_id = DfsFileSystemId::generate();
    foreign_command
        .object_metadata
        .as_mut()
        .unwrap()
        .file_system_id = foreign_command.file_system_id.clone();
    assert!(matches!(
        replay(foreign, b"local candidate".to_vec()),
        DfsBlobTransferResult::Rejected { .. }
    ));
    assert!(
        matches!(
            replay(
                exchange.command.routed_message.clone(),
                b"local candidate".to_vec()
            ),
            DfsBlobTransferResult::Durable { .. }
        ),
        "an exact native retry keeps its original durable staged bytes"
    );
    let mut conflicting = exchange.command.routed_message.clone();
    let DfsNetworkMessage::StageRetainedBlob(conflict) = &mut conflicting.message else {
        unreachable!()
    };
    conflict.content_hash = content_hash(b"other candidate");
    // Corrupt both claimed identity and body; it cannot replace the original
    // intent's staged content merely by keeping the same request/intent IDs.
    assert!(matches!(
        replay(conflicting, b"other candidate".to_vec()),
        DfsBlobTransferResult::Rejected { .. }
    ));
    dfs.posix_write_file(path, b"next canonical", options)
        .unwrap();
    assert_eq!(
        dfs.posix_read_file(path).unwrap(),
        b"next canonical",
        "pending retained staging must not block the actual current writer"
    );
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
    drop(dfs);
    runtime.close().unwrap();
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
fn file_history_preserve_keeps_local_owned_bytes_on_the_owner() {
    let root = temp_root("preserve-local-owned");
    let owner = NodeId::from_legacy_named("preserve-local-owned-owner").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_preserve_keeps_local_owned_bytes_on_the_owner",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = dfs
        .posix_write_file(
            "/services/preserve-local-owned",
            b"private",
            options.clone(),
        )
        .unwrap();
    let mut object = dfs
        .test_current_read_object_metadata(&stat.object_id)
        .unwrap();
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
        dfs.test_current_read_object_metadata(&stat.object_id)
            .unwrap(),
        object
    );
    assert_eq!(dfs.posix_read_file(&stat.path).unwrap(), b"private");
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_preserve_routes_to_authority_and_keeps_both_current_views() {
    let (mut runtime, owner, requester, mut primary, mut client) = native_history_remote_services(
        "file_history_preserve_routes_to_authority_and_keeps_both_current_views",
    );
    let stat = primary
        .posix_write_file(
            "/services/retained-routed",
            b"canonical",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();

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
                .test_current_read_object_metadata(&stat.object_id)?
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

    assert!(
        matches!(result.unwrap(), DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"local staged alternative")
    );
    assert_eq!(
        primary.posix_stat(&stat.path).unwrap().version,
        stat.version
    );
    assert_eq!(primary.posix_read_file(&stat.path).unwrap(), b"canonical");
    drop(client);
    drop(primary);
    runtime.close().unwrap();
}

#[test]
fn file_history_preserve_api_replays_without_repinning_or_replacing_current_content() {
    let root = temp_root("preserve-local-reader-api");
    let owner = NodeId::from_legacy_named("preserve-reader-owner").unwrap();
    let (mut runtime, mut dfs) = native_history_core_service(
        "file_history_preserve_api_replays_without_repinning_or_replacing_current_content",
        owner.clone(),
    );
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
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
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
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_pending_preservation_does_not_block_current_writes_and_publishes_after_w() {
    let (runtime, owner, remotes, mut primary) =
        native_history_repair_services("native-history-preserve-awaits-real-W", 2);
    let remote = remotes[0].clone();
    deny_history_content_targets(&runtime, &owner, &remotes);
    let ordinary = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let stat = primary
        .posix_write_file("/services/preserve-threshold", b"base", ordinary.clone())
        .unwrap();
    let request = preserve_request(&stat, "preserve-needs-two-replicas");
    let preservation_options = DfsWriteOptions {
        replica_candidates: vec![owner.clone(), remote.clone()],
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        wait_for_commit: false,
        foreground_replica_repair: false,
    };
    let pending = primary
        .preserve_local_file_version(
            request.clone(),
            &b"candidate awaiting W"[..],
            preservation_options.clone(),
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
        .test_current_read_object_metadata(&stat.object_id)
        .unwrap();
    fail_native_history_repairs(&runtime, &owner, &mut primary);
    let peer_head_before = native_history_settled_peer_head(&runtime, &owner, &remote, &current);
    runtime.clear_faults();
    // The old fixture advanced a private retry tick by 100. Use only the
    // ordinary Main's finite repair passes, within the SAME existing network
    // budget and eight-work bound. Exact published DATA, not caller state,
    // determines whether the original operation has reached W.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed_progress = Vec::new();
    runtime.request_repair_reconciliation(&owner).unwrap();
    for _ in 0..8 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "Main progress used the existing network budget: {observed_progress:?}"
        );
        let pass = runtime.maintenance_tick(&owner, remaining).unwrap();
        pass.outcome.unwrap();
        assert_eq!(pass.omitted_kv_event_count, 0);
        let backend = primary.metadata_backend().unwrap();
        let published = backend
            .get_version(&identity.object_id, &identity.version)
            .unwrap();
        let receipts = backend
            .receipts_for_version(&identity.object_id, &identity.version)
            .unwrap();
        let queued = native_history_queue(&runtime, &owner)
            .into_iter()
            .filter(|record| record.intent_id == intent_id)
            .collect::<Vec<_>>();
        observed_progress.push((published.is_some(), receipts, queued));
        if published.is_some() {
            break;
        }
    }
    let completed = primary
        .preserve_local_file_version(request, &b"candidate awaiting W"[..], preservation_options)
        .unwrap();
    match completed {
        DfsFileHistoryPreserveOutcome::Pending {
            identity: actual_identity,
            intent_id: canonical_id,
        } => {
            assert_eq!(actual_identity, identity);
            assert_eq!(
                canonical_id, intent_id,
                "the original caller retains the same pending request key"
            );
            primary.process_incoming().unwrap();
            let NetworkedDfsWriteOutcome::Committed { version, .. } =
                primary.test_write_outcome(&canonical_id).unwrap()
            else {
                panic!("the same actual Main reply must confirm W before retained completion");
            };
            assert_eq!(version, actual_identity.version);
        }
        DfsFileHistoryPreserveOutcome::Retained {
            identity: actual_identity,
        } => {
            assert_eq!(actual_identity, identity);
        }
    }
    let backend = primary.metadata_backend().unwrap();
    let published = backend
        .get_version(&identity.object_id, &identity.version)
        .unwrap()
        .expect("actual retained publication after W");
    assert_eq!(
        published.content_durability.as_ref().unwrap().policy,
        DfsReplicationPolicy::new(2, 2).unwrap()
    );
    let receipts = backend
        .receipts_for_version(&identity.object_id, &identity.version)
        .unwrap();
    let durable_nodes = receipts
        .into_iter()
        .filter(|receipt| receipt.content_hash == published.content_hash)
        .map(|receipt| receipt.replica_node_id)
        .collect::<BTreeSet<_>>();
    assert!(
        durable_nodes.contains(&owner) && durable_nodes.contains(&remote),
        "{intent_id} reaches W through both actual receiver receipts"
    );
    assert_eq!(
        primary
            .test_current_read_object_metadata(&stat.object_id)
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
    let replica = runtime.client(&remote).unwrap();
    assert_eq!(
        replica
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap(),
        peer_head_before,
        "retained staging leaves the actual local current view unchanged"
    );
    drop(replica);
    drop(primary);
    runtime.close().unwrap();
}

#[test]
fn file_history_repairs_retained_bytes_without_rewinding_the_replica() {
    let (runtime, owner, remotes, mut primary) =
        native_history_repair_services("native-history-storage-only-repair", 3);
    deny_history_content_targets(&runtime, &owner, &remotes);
    let path = "/services/repair-retained";
    let first = primary
        .posix_write_file(
            path,
            b"retained bytes",
            DfsWriteOptions {
                replica_candidates: std::iter::once(owner.clone())
                    .chain(remotes.iter().cloned())
                    .collect(),
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    capture_history(&mut primary, &first);
    primary
        .posix_write_file(
            path,
            b"new current bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    let current = primary
        .test_current_read_object_metadata(&first.object_id)
        .unwrap();
    fail_native_history_repairs(&runtime, &owner, &mut primary);
    let before = native_history_settled_peer_head(&runtime, &owner, &remotes[0], &current);
    let peer = runtime.client(&remotes[0]).unwrap();
    assert!(
        matches!(peer.content().read_blob(first.content_hash.as_deref().unwrap()),
            Err(sinomo_dfs::ContentStoreError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound),
        "the denied target must have actual NotFound for the exact retained blob",
    );
    drop(peer);
    runtime.clear_faults();
    let (_repairs, settled) = drain_native_history_repairs(&runtime, &owner, &mut primary);
    let peer = runtime.client(&remotes[0]).unwrap();
    assert_native_history_holder_bytes(
        &settled,
        &owner,
        &peer,
        &remotes[0],
        &history_identity(&first),
        b"retained bytes",
    );
    assert_eq!(
        peer.metadata_backend()
            .unwrap()
            .get_object(&first.object_id)
            .unwrap(),
        before,
        "the genuine retained repair changes no current metadata"
    );
    assert_eq!(primary.posix_read_file(path).unwrap(), b"new current bytes");
    assert_eq!(
        primary
            .test_current_read_object_metadata(&first.object_id)
            .unwrap(),
        current
    );
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| event.source == owner
                && event.target == remotes[0]
                && event.route == sinomo_dfs::DFS_DATA_REPAIR_REPLICA_ROUTE
                && event.phase == sinomo_dfs::test_support::DfsTestRpcPhase::Completed),
        "the bytes must arrive through the real authenticated repair endpoint"
    );
    drop(peer);
    drop(primary);
    runtime.close().unwrap();
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
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_restore_rejects_wrong_input_before_publishing_a_destination",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
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
    // Reject bad input before admitting a valid autonomous copy job.
    let requester = dfs.node_id().clone();
    let job_id = core_test::content_copy_job_id(&requester, &request.operation_id);
    let write_operation_id = format!("restore-write:{job_id}");
    let planned_object = sinomo_dfs::stable_create_object_id(&requester, path, &write_operation_id);
    let planned_version =
        sinomo_dfs::stable_write_target_version(&requester, &planned_object, &write_operation_id);
    let parent_before = dfs.posix_stat("/services").unwrap();
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let input = VerifiedContentInput::Reader {
        reader: Box::new(&b"wrong"[..]),
        content_hash: content_hash(b"saved"),
        size_bytes: 5,
    };
    let rejection = dfs
        .test_posix_copy_file_content_for_requester(
            path,
            &request,
            input,
            options.clone(),
            &requester,
            &write_operation_id,
        )
        .unwrap_err();
    assert!(matches!(
        rejection,
        sinomo_dfs::DfsPosixError::Dfs(sinomo_dfs::DfsError::ContentStore { .. })
    ));
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
        dfs.content()
            .read_manifest(planned_object.as_str(), planned_version.as_str())
            .is_err(),
        "rejected input must remain unbound quarantine"
    );
    let job = begin_history_restore_job(&mut dfs, path, &request, &options);
    let job = dfs
        .posix_resume_namespace_job_bounded(&job.job_id, 2)
        .unwrap();
    let state = core_test::content_copy_job_state(&job).unwrap().clone();
    assert_eq!(job.job_id, job_id);
    assert_eq!(state.requester_node_id, requester);
    assert_eq!(state.write_operation_id, write_operation_id);
    assert_eq!(state.planned.object_id, planned_object);
    assert_eq!(state.planned.version, planned_version);
    let response = dfs.restore_file_history(request, options).unwrap();
    assert_eq!(response.committed, state.planned);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"saved");
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_restore_runs_on_the_primary_with_the_original_requester_identity() {
    let (mut runtime, owner, requester, mut primary, mut client) = native_history_remote_services(
        "file_history_restore_runs_on_the_primary_with_the_original_requester_identity",
    );
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

    // Formation can move a real primary after the initial source writes.
    // Select distinct requester/executor roles from the current assignment,
    // rather than interpreting the old fixture-owner label as receipt proof.
    let executing = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| {
            shard.file_system_id == destination.file_system_id && shard.shard_id == "services"
        })
        .expect("actual destination authority");
    let actual_owner = executing.primary_node_id;
    let actual_requester = [owner, requester]
        .into_iter()
        .find(|node| *node != actual_owner)
        .expect("a real requester distinct from current execution primary");
    drop(client);
    drop(primary);
    let owner = actual_owner;
    let requester = actual_requester;
    assert_ne!(owner, requester);
    let mut primary = runtime.client(&owner).unwrap();
    let mut client = runtime.client(&requester).unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);

    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&source),
        destination_path: destination.path.clone(),
        expected_destination: Some(history_identity(&destination)),
        operation_id: "routed-restore-once".into(),
    };

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

    let response = result.unwrap();
    let job_id = core_test::content_copy_job_id(&requester, &request.operation_id);
    let mut actual_primary = runtime.metadata_primary_service(&destination.path).unwrap();
    let job = actual_primary
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&job_id)
        .unwrap()
        .unwrap();
    // The normal Main may already have completed its cleanup continuation.
    // Observe the actual stored cut before this SAME bounded resume.
    let checkpoint = core_test::content_copy_job_state(&job).unwrap();
    let source_release = checkpoint
        .source_release
        .clone()
        .expect("the release request is persisted before its normal effect");
    let source_reference = checkpoint.source_reference.clone().unwrap();
    assert_eq!(checkpoint.requester_node_id, requester);
    assert_eq!(checkpoint.request, request);
    assert_eq!(checkpoint.planned, response.committed);
    assert!(matches!(&checkpoint.result,
        Some(DfsContentCopyJobResult::Applied { response: actual })
            if actual.restored_from == response.restored_from
                && actual.committed == response.committed && actual.path == response.path));
    let release_before = actual_primary
        .metadata_backend()
        .unwrap()
        .get_namespace_request(&owner, &source_release.operation_id)
        .unwrap();
    // The effect opener owns only the destination shard. Query the complete
    // filesystem through the normal requester, preserving its authority routing.
    if checkpoint.cleanup_complete {
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(job.completed_at.is_some());
        let receipt = release_before
            .as_ref()
            .expect("actual completed release receipt");
        assert_eq!(
            receipt.file_system_id,
            source_release.identity.file_system_id
        );
        assert_eq!(receipt.requester_node_id, owner);
        assert_eq!(receipt.request_id, source_release.operation_id);
        assert_eq!(receipt.status, DfsNamespaceRequestStatus::Completed);
        assert_eq!(receipt.result, Some(DfsNamespaceResult::Applied));
        assert_eq!(
            receipt.operation,
            DfsNamespaceOperation::ContentReference {
                request: source_release.clone()
            }
        );
        assert!(!client.content_reference_present(&source_reference).unwrap());
    } else {
        assert_eq!(job.status, DfsNamespaceJobStatus::Running);
        assert!(client.content_reference_present(&source_reference).unwrap());
    }
    drop(actual_primary);
    let complete = client
        .posix_resume_namespace_job_bounded(&job_id, 1)
        .unwrap();
    assert_eq!(complete.status, DfsNamespaceJobStatus::Completed);
    let mut actual_primary = runtime.metadata_primary_service(&destination.path).unwrap();
    let after = actual_primary
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&job_id)
        .unwrap()
        .unwrap();
    let release_after = actual_primary
        .metadata_backend()
        .unwrap()
        .get_namespace_request(&owner, &source_release.operation_id)
        .unwrap();
    let receipt = release_after
        .as_ref()
        .expect("actual completed release receipt after resume");
    assert_eq!(
        receipt.file_system_id,
        source_release.identity.file_system_id
    );
    assert_eq!(receipt.requester_node_id, owner);
    assert_eq!(receipt.request_id, source_release.operation_id);
    assert_eq!(receipt.status, DfsNamespaceRequestStatus::Completed);
    assert_eq!(receipt.result, Some(DfsNamespaceResult::Applied));
    assert_eq!(
        receipt.operation,
        DfsNamespaceOperation::ContentReference {
            request: source_release.clone()
        }
    );
    if checkpoint.cleanup_complete {
        assert_eq!(
            after, job,
            "already completed cleanup is a whole-job idempotent resume"
        );
        assert_eq!(
            release_after, release_before,
            "resume preserves its whole actual release receipt"
        );
    }
    assert!(!client.content_reference_present(&source_reference).unwrap());
    let state = core_test::content_copy_job_state(&after).unwrap();
    assert_eq!(state.source_release.as_ref(), Some(&source_release));
    assert_eq!(state.requester_node_id, requester);
    assert_eq!(state.planned, response.committed);
    assert!(state.cleanup_complete);
    let receipts = actual_primary
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&response.committed.object_id, &response.committed.version)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<Vec<_>>(),
        vec![owner.clone()],
        "the executing primary owns the ingested bytes; requester identity is not a storage receipt"
    );
    assert_eq!(
        actual_primary
            .metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap()
            .len(),
        1
    );
    drop(actual_primary);
    drop(client);
    drop(primary);
    runtime.close().unwrap();
}

#[test]
fn file_history_restore_recovers_a_commit_after_later_edit_and_replays_compacted_result() {
    let root = temp_root("restore-durable-result");
    let owner = NodeId::from_legacy_named("restore-durable-result-owner").unwrap();
    let (mut runtime, mut dfs) = native_history_core_service(
        "file_history_restore_recovers_a_commit_after_later_edit_and_replays_compacted_result",
        owner.clone(),
    );
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
    let admitted = begin_history_restore_job(&mut dfs, path, &request, &options);
    let job = dfs
        .posix_resume_namespace_job_bounded(&admitted.job_id, 2)
        .unwrap();
    let state = assert_restore_checkpoint_matches_admission(&admitted, &job).clone();
    assert!(state.source_reference_applied);
    // Exercise the same stable write whose result may still need a checkpoint.
    // The normal Main owner may independently finish this admitted job first.
    let descriptor = dfs
        .resolve_retained_revision(&request.source)
        .unwrap()
        .unwrap();
    let input = dfs
        .test_open_verified_content_input(
            &request.source,
            descriptor.version.content_hash.as_deref().unwrap(),
            descriptor.version.size_bytes.unwrap(),
        )
        .unwrap();
    let committed = dfs
        .test_posix_copy_file_content_for_requester(
            path,
            &request,
            input,
            options.clone(),
            &state.requester_node_id,
            &state.write_operation_id,
        )
        .unwrap();
    assert_eq!(history_identity(&committed), state.planned);
    let before_edit =
        restore_job_observation(dfs.metadata_backend_mut().unwrap(), &job, &source.object_id);
    let observed = before_edit
        .job
        .as_ref()
        .expect("actual full job before later edit and GC");
    let reservation = state.source_reference.as_ref().unwrap();
    let reservation_key =
        content_reference_storage_id(&reservation.namespace, reservation.operation.reference_id());
    assert_restore_job_observation(&job, observed, &before_edit, &edited, &reservation_key);
    let original_write = before_edit
        .target_write
        .as_ref()
        .expect("actual full stable write before GC");
    assert_eq!(
        original_write.intent_id,
        core_test::stable_write_intent_id(
            &state.requester_node_id,
            &state.planned.object_id,
            &state.write_operation_id,
        )
    );
    assert_eq!(original_write.file_system_id, state.planned.file_system_id);
    assert_eq!(original_write.object_id, state.planned.object_id);
    assert_eq!(original_write.target_version, state.planned.version);
    assert_eq!(original_write.coordinator_node_id, state.requester_node_id);
    assert_eq!(
        original_write.operation_id.as_deref(),
        Some(state.write_operation_id.as_str())
    );
    assert_eq!(original_write.content_hash, state.content_hash);
    assert_eq!(original_write.policy, state.options.policy);
    assert!(original_write.publication.is_current());
    assert_eq!(original_write.status, DfsWriteIntentStatus::Committed);
    assert_eq!(original_write.target_version, committed.version);
    let later = dfs
        .posix_write_file(path, b"later edit", options.clone())
        .unwrap();
    runtime
        .gc_metadata_history_on_current_primaries(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    let after_gc =
        restore_job_observation(dfs.metadata_backend_mut().unwrap(), &job, &source.object_id);
    assert_eq!(
        restore_observed_revision(&after_gc.current),
        history_identity(&later)
    );
    if let Some(current) = &after_gc.job {
        let actual = assert_restore_checkpoint_matches_admission(observed, current);
        if current.status == DfsNamespaceJobStatus::Running && actual.result.is_none() {
            assert!(
                after_gc.write_receipt.is_none(),
                "the unfinished job must retain its full commit witness"
            );
            assert_eq!(after_gc.target_write.as_ref(), Some(original_write));
            let pin = after_gc
                .pins
                .iter()
                .find(|pin| content_reference_record_id(pin) == reservation_key)
                .expect("unfinished original job retains its exact source pin");
            assert_eq!(pin.namespace, reservation.namespace);
            assert_eq!(pin.file_system_id, state.request.source.file_system_id);
            assert_eq!(pin.object_id, state.request.source.object_id);
            assert_eq!(pin.version, state.request.source.version);
            let retained_source = after_gc
                .source_version
                .as_ref()
                .expect("actual pinned source survived GC");
            assert_eq!(
                retained_source.file_system_id,
                state.request.source.file_system_id
            );
            assert_eq!(retained_source.object_id, state.request.source.object_id);
            assert_eq!(retained_source.version, state.request.source.version);
            assert_eq!(retained_source.content_hash, state.content_hash);
            assert_eq!(retained_source.size_bytes, state.content_size_bytes);
        } else {
            // A full real checkpoint remains required while cleanup is pending.
            // A compact write cannot invent completion of the admitted job.
            assert!(matches!(
                actual.result,
                Some(sinomo_dfs::DfsContentCopyJobResult::Applied { .. })
            ));
            if let Some(receipt) = &after_gc.write_receipt {
                assert_eq!(
                    Some(receipt),
                    core_test::write_intent_receipt(original_write, receipt.retained_at).as_ref()
                );
            } else {
                assert_eq!(after_gc.target_write.as_ref(), Some(original_write));
            }
        }
    } else {
        // Compacted-only evidence must join a full terminal job actually seen
        // before this edit/GC. An unobserved Running->compacted race fails.
        assert_restore_compacted_receipt(observed, after_gc.receipt.as_ref().unwrap());
        let receipt = after_gc
            .write_receipt
            .as_ref()
            .expect("actual compact original write");
        assert_eq!(
            Some(receipt),
            core_test::write_intent_receipt(original_write, receipt.retained_at).as_ref()
        );
    }
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    let response = dfs
        .restore_file_history(request.clone(), options.clone())
        .unwrap();
    assert_eq!(response.committed, history_identity(&committed));
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"later edit");
    let recovered =
        restore_job_observation(dfs.metadata_backend_mut().unwrap(), &job, &source.object_id);
    assert_eq!(
        restore_observed_revision(&recovered.current),
        history_identity(&later)
    );
    let completed = if let Some(current) = &recovered.job {
        let actual = assert_restore_checkpoint_matches_admission(observed, current);
        assert_eq!(current.status, DfsNamespaceJobStatus::Completed);
        assert!(matches!(
            actual.result,
            Some(sinomo_dfs::DfsContentCopyJobResult::Applied { .. })
        ));
        current
    } else {
        assert_restore_compacted_receipt(observed, recovered.receipt.as_ref().unwrap());
        observed
    };
    assert!(
        !recovered
            .pins
            .iter()
            .any(|pin| content_reference_record_id(pin) == reservation_key)
    );
    dfs.file_history_pin(DfsFileHistoryPinRequest {
        path: path.into(),
        identity: history_identity(&source),
        operation: DfsFileHistoryPinOperation::Unpin {
            pin_id: "checkpoint".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
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
    // Compact this actual terminal job. A second generic expiry sweep would
    // retire its already-compacted receipt if Main completed before the first GC.
    runtime
        .gc_metadata_history_on_current_primaries(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX)
                .with_namespace_job_ids([job.job_id.clone()]),
        )
        .unwrap();
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .get_namespace_job(&job.job_id)
            .unwrap()
            .is_none()
    );
    let compacted = dfs
        .metadata_backend()
        .unwrap()
        .get_namespace_job_receipt(&job.job_id)
        .unwrap()
        .unwrap();
    assert_restore_compacted_receipt(completed, &compacted);
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    assert_eq!(
        dfs.restore_file_history(request, options).unwrap(),
        response
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"later edit");
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_cancelled_restore_keeps_its_source_until_recovery() {
    let root = temp_root("restore-cancelled-source-owner");
    let owner = NodeId::generate();
    let (mut runtime, mut dfs) = native_history_core_service(
        "file_history_cancelled_restore_keeps_its_source_until_recovery",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let path = "/services/cancelled-restore";
    let saved = dfs
        .posix_write_file(path, b"saved", options.clone())
        .unwrap();
    capture_history(&mut dfs, &saved);
    let edited = dfs
        .posix_write_file(path, b"edited", options.clone())
        .unwrap();
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&saved),
        destination_path: path.into(),
        expected_destination: Some(history_identity(&edited)),
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    let admitted = begin_history_restore_job(&mut dfs, path, &request, &options);
    let job = dfs
        .posix_resume_namespace_job_bounded(&admitted.job_id, 2)
        .unwrap();
    let state = assert_restore_checkpoint_matches_admission(&admitted, &job).clone();
    assert!(state.source_reference_applied);
    let reservation = state.source_reference.clone().unwrap();
    let DfsContentReferenceOperation::Retain {
        reference_id: job_pin,
    } = reservation.operation.clone()
    else {
        panic!("source pin")
    };
    let pins = dfs
        .metadata_backend()
        .unwrap()
        .version_pins_for_object(&saved.object_id)
        .unwrap();
    let reservation_key = content_reference_storage_id(&reservation.namespace, &job_pin);
    for pin in pins
        .into_iter()
        .filter(|pin| content_reference_record_id(pin) != reservation_key)
    {
        let namespace = pin.namespace.clone();
        let prefix = format!("ref/{namespace}/");
        let reference_id = pin
            .pin_id
            .strip_prefix(&prefix)
            .unwrap_or(&pin.pin_id)
            .to_owned();
        dfs.retain_content_revision(DfsContentReferenceRequest {
            namespace,
            path: path.into(),
            identity: history_identity(&saved),
            operation: DfsContentReferenceOperation::Release { reference_id },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
    }
    let before_cancel =
        restore_job_observation(dfs.metadata_backend_mut().unwrap(), &job, &saved.object_id);
    let before = before_cancel
        .job
        .as_ref()
        .expect("actual full job before GC");
    assert_restore_job_observation(&job, before, &before_cancel, &edited, &reservation_key);
    let cancellation = DfsCancellationFlag::new();
    cancellation.cancel();
    dfs.set_cancellation(Some(cancellation));
    // The caller's flag rejects before job lookup, routing or publication. It
    // does not cancel the independently admitted Main job.
    assert!(matches!(
        dfs.posix_resume_namespace_job_bounded(&job.job_id, 4),
        Err(sinomo_dfs::DfsPosixError::Dfs(
            sinomo_dfs::DfsError::Cancelled {
                operation: "namespace job resume"
            }
        ))
    ));
    let after_cancel =
        restore_job_observation(dfs.metadata_backend_mut().unwrap(), &job, &saved.object_id);
    let persisted = after_cancel
        .job
        .as_ref()
        .expect("actual full job before GC");
    assert_restore_job_observation(before, persisted, &after_cancel, &edited, &reservation_key);
    if restore_observed_revision(&after_cancel.current) == state.planned {
        let (_, bytes) = dfs
            .content()
            .read_object_version(
                &state.planned.object_id.to_string(),
                &state.planned.version.to_string(),
            )
            .unwrap();
        assert_eq!(bytes, b"saved");
    }
    runtime
        .gc_metadata_history_on_current_primaries(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                .with_now_unix_nanos(u64::MAX),
        )
        .unwrap();
    let after_gc =
        restore_job_observation(dfs.metadata_backend_mut().unwrap(), &job, &saved.object_id);
    if let Some(current) = &after_gc.job {
        assert_restore_job_observation(persisted, current, &after_gc, &edited, &reservation_key);
    } else {
        // A receipt can join a full terminal body already observed above; it
        // cannot reconstruct unobserved cleanup or source-release evidence.
        assert_restore_compacted_receipt(persisted, after_gc.receipt.as_ref().unwrap());
        assert_eq!(restore_observed_revision(&after_gc.current), state.planned);
        assert!(
            !after_gc
                .pins
                .iter()
                .any(|pin| content_reference_record_id(pin) == reservation_key)
        );
    }
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    let reopened =
        restore_job_observation(dfs.metadata_backend_mut().unwrap(), &job, &saved.object_id);
    if let Some(current) = &reopened.job {
        assert_restore_job_observation(persisted, current, &reopened, &edited, &reservation_key);
        let resumed = dfs
            .posix_resume_namespace_job_bounded(&job.job_id, 4)
            .unwrap();
        assert_restore_checkpoint_matches_admission(current, &resumed);
        assert_eq!(resumed.status, DfsNamespaceJobStatus::Completed);
    } else {
        let terminal = after_gc.job.as_ref().unwrap_or(persisted);
        assert_restore_compacted_receipt(terminal, reopened.receipt.as_ref().unwrap());
        assert_eq!(restore_observed_revision(&reopened.current), state.planned);
    }
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"saved");
    assert!(
        !dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&saved.object_id)
            .unwrap()
            .iter()
            .any(|pin| content_reference_record_id(pin) == reservation_key)
    );
    assert_eq!(
        dfs.restore_file_history(request, options)
            .unwrap()
            .committed,
        state.planned
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_restore_cleanup_survives_restart_and_lost_unpin_reply() {
    let root = temp_root("restore-durable-cleanup");
    let owner = NodeId::from_legacy_named("restore-durable-cleanup-owner").unwrap();
    let (mut runtime, mut dfs) =
        native_history_core_service("native-history-restore-cleanup", owner.clone());
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
    let initial = begin_history_restore_job(&mut dfs, path, &request, &options);
    let job = dfs
        .posix_resume_namespace_job_bounded(&initial.job_id, 3)
        .unwrap();
    let state = assert_restore_checkpoint_matches_admission(&initial, &job).clone();
    assert!(state.result.is_some());
    let mut missing_release_field = serde_json::to_value(&state).unwrap();
    missing_release_field
        .as_object_mut()
        .unwrap()
        .remove("source_release");
    assert!(
        serde_json::from_value::<DfsContentCopyJobState>(missing_release_field).is_err(),
        "current job wire requires its explicit cleanup stage field"
    );
    let persisted = dfs
        .metadata_backend()
        .unwrap()
        .get_namespace_job(&initial.job_id)
        .unwrap()
        .unwrap();
    assert_restore_checkpoint_matches_admission(&job, &persisted);
    let prior = persisted;
    let job = dfs
        .posix_resume_namespace_job_bounded(&prior.job_id, 1)
        .unwrap();
    let checkpointed = assert_restore_checkpoint_matches_admission(&prior, &job);
    let release = checkpointed
        .source_release
        .clone()
        .expect("normal job producer checkpointed the exact cleanup request before its effect");
    assert!(sinomo_dfs::stable_operation_order_token(&release.operation_id).is_some());
    let mut wrong_release = job.clone();
    let [DfsNamespaceJobFrame::ContentCopy { state: wrong_state }] =
        wrong_release.pending_frames.as_mut_slice()
    else {
        panic!("actual content-copy frame");
    };
    wrong_state
        .source_release
        .as_mut()
        .unwrap()
        .identity
        .version = DfsVersion::generate();
    assert!(
        sinomo_dfs::validate_content_copy_job(&wrong_release).is_err(),
        "cleanup cannot change the admitted source revision"
    );
    let pin = state.source_reference.unwrap();
    let DfsContentReferenceOperation::Retain {
        reference_id: pin_id,
    } = pin.operation.clone()
    else {
        panic!("restore pin");
    };
    let reservation_key = content_reference_storage_id(&pin.namespace, &pin_id);
    dfs.retain_content_revision(release).unwrap();
    dfs.posix_write_file(path, b"after successful restore", options.clone())
        .unwrap();
    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    let report = dfs.posix_resume_namespace_jobs_bounded(8, 4).unwrap();
    assert!(report.failures.is_empty());
    assert_eq!(report.failed_job_count, 0);
    assert_eq!(report.remaining_running_job_count, 0);
    assert_eq!(report.scanned_job_count, 1);
    assert!(report.completed_job_count <= 1);
    assert_eq!(report.resumed_job_count, report.completed_job_count);
    let completed = dfs.namespace_job(&initial.job_id).unwrap().unwrap();
    assert_restore_checkpoint_matches_admission(&job, &completed);
    assert_eq!(completed.status, DfsNamespaceJobStatus::Completed);
    assert!(
        !dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&source.object_id)
            .unwrap()
            .iter()
            .any(|pin| content_reference_record_id(pin) == reservation_key)
    );
    let response = dfs.restore_file_history(request, options).unwrap();
    assert_eq!(response.committed, state.planned);
    assert_eq!(
        dfs.posix_read_file(path).unwrap(),
        b"after successful restore"
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_reads_restore_and_deleted_recovery_keep_one_current_view() {
    let root = temp_root("history-restore-and-recovery");
    let owner = NodeId::from_legacy_named("history-restore-owner").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_reads_restore_and_deleted_recovery_keep_one_current_view",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
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
    let page = crate::core_integration_tests::next_history_entries_page(
        &mut dfs,
        DfsFileHistoryListRequest {
            file_system_id: identity.file_system_id.clone(),
            path: Some(path.into()),
            object_id: None,
            after: None,
            limit: 128,
        },
    )
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

    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_query_routes_to_primary_and_reads_an_old_replica_range() {
    let (mut runtime, owner, requester, mut primary, mut client) = native_history_remote_services(
        "file_history_query_routes_to_primary_and_reads_an_old_replica_range",
    );
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

    let result = (|| {
        let page = crate::core_integration_tests::next_history_entries_page(
            &mut client,
            DfsFileHistoryListRequest {
                file_system_id: sifs_file_system_id(),
                path: Some("/services".into()),
                object_id: None,
                after: None,
                limit: 128,
            },
        )?;
        assert_eq!(page.entries.len(), 1);
        client.read_file_history(DfsFileHistoryReadRequest {
            identity: history_identity(&original),
            offset: 7,
            max_bytes: 8,
        })
    })();

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
    drop(client);
    drop(primary);
    runtime.close().unwrap();
}

#[test]
fn file_history_cursor_cannot_be_reused_for_another_query() {
    let root = temp_root("history-query-cursor");
    let owner = NodeId::from_legacy_named("history-cursor-owner").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_cursor_cannot_be_reused_for_another_query",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
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
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_hardlink_capture_uses_canonical_identity_and_follows_promotion() {
    let root = temp_root("history-hardlink-capture");
    let owner = NodeId::from_legacy_named("history-hardlink-owner").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_hardlink_capture_uses_canonical_identity_and_follows_promotion",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let original = "/services/history-original";
    let alias = "/services/history-alias";
    dfs.posix_write_file(original, b"shared inode", options)
        .unwrap();
    dfs.posix_hardlink(original, alias).unwrap();
    let through_alias = dfs.posix_stat(alias).unwrap();
    capture_history(&mut dfs, &through_alias);
    let identity = history_identity(&through_alias);
    let entry = crate::core_integration_tests::next_history_entries_page(
        &mut dfs,
        DfsFileHistoryListRequest {
            file_system_id: sifs_file_system_id(),
            object_id: Some(identity.object_id.clone()),
            path: None,
            after: None,
            limit: 128,
        },
    )
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
        crate::core_integration_tests::next_history_entries_page(
            &mut dfs,
            DfsFileHistoryListRequest {
                file_system_id: sifs_file_system_id(),
                object_id: Some(identity.object_id),
                path: None,
                after: None,
                limit: 128
            }
        )
        .unwrap()
        .entries
        .is_empty()
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_policy_applies_to_normal_writes_without_creating_conflicts() {
    let root = temp_root("history-service-retention");
    let owner = NodeId::from_legacy_named("history-policy-owner").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_policy_applies_to_normal_writes_without_creating_conflicts",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let mut fs = sinomo_dfs::distributed_sifs(owner);
    {
        let mut policy = decode_history_policy(fs.retention_policies.get(HISTORY_MODULE_ID));
        policy.rules.push(DfsFileHistoryRule {
            path: "/services".into(),
            retention: DfsFileHistoryRetention {
                keep_last: 1,
                keep_for_seconds: 0,
            },
        });
        install_history_policy(&mut fs, policy).unwrap();
    }
    apply_history_fixture_policy(&mut dfs, &fs);
    let path = "/services/history-policy-file";
    let first = dfs
        .posix_write_file(path, b"first", options.clone())
        .unwrap();
    let second = dfs
        .posix_write_file(path, b"second", options.clone())
        .unwrap();
    let current = dfs.posix_write_file(path, b"current", options).unwrap();
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"current");

    for _ in 0..10 {
        runtime
            .gc_metadata_history_on_current_primaries(
                DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
                    .with_now_unix_nanos(u64::MAX),
            )
            .unwrap();
    }
    let backend = dfs.metadata_backend().unwrap();
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
            .retention_order(HISTORY_MODULE_ID),
        2
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_service_capture_and_unpin_preserve_the_current_namespace() {
    let root = temp_root("file-history-service");
    let owner = NodeId::from_legacy_named("history-owner").unwrap();
    let (runtime, mut dfs) =
        native_history_core_service("history-current-capture-unpin", owner.clone());
    dfs.posix_mkdir_all("/src").unwrap();
    let stat = dfs
        .posix_write_file(
            "/src/lib.rs",
            b"first",
            DfsWriteOptions::replicated_eventual(vec![owner]),
        )
        .unwrap();
    let object = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
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
            .history_capture()
            .is_some()
    );
    assert_eq!(
        backend
            .version_pins_for_object(&object.object_id)
            .unwrap()
            .len(),
        1
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
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn native_history_restore_joins_real_configuration_rebuild_before_browser_admission() {
    let name = "history-restore-native-configuration-gap";
    let nodes = core_test::DfsTestRuntime::ordered_node_ids(name, 2);
    let mut runtime = core_test::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime
        .configure_component_factories(vec![Arc::new(crate::component_factory().unwrap())])
        .unwrap();
    runtime.start_nodes().unwrap();
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let path = "/services/configuration-gap-restore.txt";
    let options = DfsWriteOptions::replicated_eventual(nodes.clone());
    let saved = writer
        .posix_write_file(path, b"saved", options.clone())
        .unwrap();
    capture_history(&mut writer, &saved);
    let current = writer.posix_write_file(path, b"current", options).unwrap();
    let request = DfsFileHistoryRestoreRequest {
        source: history_identity(&saved),
        destination_path: path.into(),
        expected_destination: Some(history_identity(&saved)),
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    let data_dir = Arc::new(runtime.data_dir(&nodes[1]).clone());
    let handle = runtime.runtime_handle();
    let host = sinomo_dfs::dfs_browser_component_host(&data_dir).unwrap();
    let control = host
        .control_plane_client(&sinomo_core::network::ClusterPaths::new(&data_dir), &handle)
        .unwrap();
    assert!(matches!(
        control.write_configuration_readiness().unwrap(),
        sinomo_dfs::DfsWriteConfigurationReadiness::Ready(Some(_))
    ));
    // Withhold a real committed delta and pause the existing owner's rebuild.
    // The other real node publishes both settings through the normal API.
    let gap = runtime
        .lose_configuration_delta_and_pause_rebuild(
            &nodes[1],
            sinomo_dfs::file_system_config_key(&saved.file_system_id),
        )
        .unwrap();
    let mut policy = writer.file_history_policy(&saved.file_system_id).unwrap();
    for keep_last in [1, 2] {
        let next = DfsFileHistoryPolicy {
            rules: vec![DfsFileHistoryRule {
                path: "/services/unrelated-gap-policy".into(),
                retention: DfsFileHistoryRetention {
                    keep_last,
                    keep_for_seconds: 0,
                },
            }],
        };
        writer
            .update_file_history_policy(DfsFileHistoryPolicyUpdateRequest {
                file_system_id: saved.file_system_id.clone(),
                expected: policy,
                policy: next.clone(),
            })
            .unwrap();
        policy = next;
    }
    gap.wait_rebuild(Duration::from_secs(5)).unwrap();
    assert!(gap.dropped_delta().is_some());
    assert!(matches!(
        control.write_configuration_readiness().unwrap(),
        sinomo_dfs::DfsWriteConfigurationReadiness::Pending
    ));
    let call = sinomo_core::network::ObjectRpcCall::value(
        sinomo_core::network::ObjectRpcRequest::encode_body(
            sinomo_api::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE,
            "daemon",
            &request,
        )
        .unwrap(),
    );
    let mut response =
        Box::pin(runtime.rpc_result_future(&nodes[0], &nodes[1], call, Duration::from_secs(5)));
    handle.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        match tokio::time::timeout(Duration::from_millis(100), response.as_mut()).await {
            Err(_) => {}
            Ok(Err(error)) => {
                panic!("restore failed before its real configuration owner completed: {error}")
            }
            Ok(Ok(_)) => panic!("restore replied while the real configuration was incomplete"),
        }
        // Readiness must not retain the browser FIFO needed by other work.
        tokio::time::timeout_at(
            deadline,
            sinomo_dfs::run_blocking_browser_task(
                Arc::clone(&data_dir),
                "history-configuration-admission-witness",
                "history configuration admission witness",
                || Ok(()),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        gap.release();
        let result = tokio::time::timeout_at(deadline, response).await.unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("stale destination expectation was accepted"),
        };
        assert!(
            error.contains("changed"),
            "the unchanged destination check must run after readiness: {error}"
        );
    });
    let after = writer.posix_stat_distributed(path).unwrap();
    assert_eq!(history_identity(&after), history_identity(&current));
    assert_eq!(
        writer.posix_read_file_distributed(path).unwrap(),
        b"current"
    );
    drop(control);
    drop(host);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn native_history_registered_provider_replays_exact_capture_and_release() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "history-registered-pin-replay",
        2,
    );
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "history-registered-pin-replay",
        nodes[0].clone(),
    )
    .unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime
        .configure_component_factories(vec![Arc::new(crate::component_factory().unwrap())])
        .unwrap();
    runtime.start_nodes().unwrap();
    let owner = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap()
        .primary_node_id;
    let requester = nodes.iter().find(|node| **node != owner).unwrap().clone();
    let mut writer = runtime.client(&requester).unwrap();
    let path = "/services/registered-pin-replay.txt";
    let bytes = b"retained-original";
    let stat = writer
        .posix_write_file(path, bytes, DfsWriteOptions::replicated_eventual(nodes))
        .unwrap();
    let identity = history_identity(&stat);
    drop(writer);

    // Use the ordinary mounted History provider and its Browser mutation path.
    // Exact replay is required; ordinary Main recovery may race this caller,
    // but this case does not pause an executor or claim a forced recovery cut.
    let deadline = Instant::now() + Duration::from_secs(5);
    let remaining = || {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .expect("registered History replay retains its original five-second window")
    };
    let pin_rpc = |request: &DfsFileHistoryPinRequest| -> DfsFileHistoryPinResponse {
        runtime
            .rpc_request(
                &requester,
                &owner,
                sinomo_core::network::ObjectRpcRequest::encode_body(
                    sinomo_api::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
                    "daemon",
                    request,
                )
                .unwrap(),
                remaining(),
            )
            .unwrap()
            .decode_body()
            .unwrap()
    };
    let mut request = DfsFileHistoryPinRequest {
        path: path.into(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Capture {
            pin_id: "registered-replay".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    // Keep the real first response only for verifier equality. The second call
    // is an independent authenticated RPC with the SAME semantic operation ID.
    let capture = pin_rpc(&request);
    assert_eq!(capture.identity, identity);
    assert_eq!(pin_rpc(&request), capture);
    let read: DfsFileHistoryReadOutcome = runtime
        .rpc_request(
            &requester,
            &owner,
            sinomo_core::network::ObjectRpcRequest::encode_body(
                sinomo_api::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE,
                "daemon",
                &DfsFileHistoryReadRequest {
                    identity: identity.clone(),
                    offset: 0,
                    max_bytes: 64,
                },
            )
            .unwrap(),
            remaining(),
        )
        .unwrap()
        .decode_body()
        .unwrap();
    assert!(matches!(read, DfsFileHistoryReadOutcome::Found { data } if data.bytes == bytes));
    let mut inspector = runtime.client(&owner).unwrap();
    let references = inspector
        .metadata_backend()
        .unwrap()
        .version_pins_for_object(&identity.object_id)
        .unwrap();
    assert_eq!(references.len(), 1);
    assert_eq!(references[0].file_system_id, identity.file_system_id);
    assert_eq!(references[0].object_id, identity.object_id);
    assert_eq!(references[0].version, identity.version);
    assert_eq!(references[0].namespace, HISTORY_MODULE_ID);
    assert_eq!(
        references[0].pin_id,
        "ref/module.dfs.history/registered-replay"
    );

    request.operation = DfsFileHistoryPinOperation::Unpin {
        pin_id: "registered-replay".into(),
    };
    request.operation_id = sinomo_core::Id::generate().to_string();
    let release = pin_rpc(&request);
    assert_eq!(release.identity, identity);
    assert_eq!(pin_rpc(&request), release);
    assert!(
        inspector
            .metadata_backend()
            .unwrap()
            .version_pins_for_object(&identity.object_id)
            .unwrap()
            .is_empty()
    );
    let current = inspector.posix_stat_distributed(path).unwrap();
    assert_eq!(current.file_system_id, stat.file_system_id);
    assert_eq!(current.object_id, stat.object_id);
    assert_eq!(current.version, stat.version);
    assert_eq!(current.content_hash, stat.content_hash);
    assert_eq!(current.size_bytes, stat.size_bytes);
    assert_eq!(current.mode, stat.mode);
    assert_eq!(inspector.posix_read_file_distributed(path).unwrap(), bytes);
    drop(inspector);
    runtime.close().unwrap();
    assert!(Instant::now() < deadline);
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
    let command = parse_history_fixture_command(&args).unwrap();
    let crate::cli::DfsHistoryCliCommand::Pin(options) = command else {
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
    assert!(parse_history_fixture_command(&args).is_err());
}

#[test]
fn file_history_object_listing_scans_only_that_object_and_marks_deletions() {
    let root = temp_root("history-object-listing");
    let owner = NodeId::from_legacy_named("history-object-listing-owner").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_object_listing_scans_only_that_object_and_marks_deletions",
        owner.clone(),
    );
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let mut captured = Vec::new();
    for index in 0..6 {
        let stat = dfs
            .posix_write_file(
                &format!("/services/object-listing-{index}"),
                b"content",
                options.clone(),
            )
            .unwrap();
        capture_history(&mut dfs, &stat);
        captured.push(stat);
    }
    // The global scan orders records by object, so a one-record page of that
    // scan would reach this object only after every lower-sorting object.
    let target = captured
        .iter()
        .max_by(|left, right| left.object_id.cmp(&right.object_id))
        .unwrap()
        .clone();
    let request = DfsFileHistoryListRequest {
        file_system_id: sifs_file_system_id(),
        path: None,
        object_id: Some(target.object_id.clone()),
        after: None,
        limit: 1,
    };
    let page = crate::core_integration_tests::next_history_entries_page(&mut dfs, request.clone())
        .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.identity.clone())
            .collect::<Vec<_>>(),
        vec![history_identity(&target)],
        "an exact object filter must read the object-local version index"
    );
    assert!(page.entries[0].is_current && !page.entries[0].deleted);

    dfs.posix_unlink(&target.path).unwrap();
    let mut request = request;
    request.limit = 128;
    let mut entries = Vec::new();
    loop {
        let page =
            crate::core_integration_tests::next_history_entries_page(&mut dfs, request.clone())
                .unwrap();
        entries.extend(page.entries);
        let Some(after) = page.next_cursor else { break };
        request.after = Some(after);
    }
    assert_eq!(entries.len(), 1);
    assert!(entries[0].deleted, "deleted objects are reported as such");
    assert!(!entries[0].is_current);
    assert_eq!(entries[0].current_path, target.path);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn file_history_capture_rejects_directories_with_a_clear_reason() {
    let root = temp_root("history-directory-capture");
    let owner = NodeId::from_legacy_named("history-directory-capture-owner").unwrap();
    let (runtime, mut dfs) = native_history_core_service(
        "file_history_capture_rejects_directories_with_a_clear_reason",
        owner.clone(),
    );
    let directory = dfs.posix_mkdir("/services/history-directory").unwrap();
    let error = dfs
        .file_history_pin(DfsFileHistoryPinRequest {
            path: directory.path.clone(),
            identity: history_identity(&directory),
            operation: DfsFileHistoryPinOperation::Capture {
                pin_id: "directory".into(),
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("only file content can be captured"),
        "{error}"
    );
    drop(dfs);
    runtime.close().unwrap();
}

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-dfs-{name}"))
}

fn content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn parse_history_fixture_command(
    args: &[String],
) -> Result<crate::cli::DfsHistoryCliCommand, sinomo_cli_registry::CliParseError> {
    let mut spec = sinomo_dfs::cli::dfs_cli_spec();
    spec.children.push(crate::cli::cli_spec());
    let sinomo_cli_registry::CliCommandParseOutcome::Matches(matches) = spec.parse(args)? else {
        panic!("fixture requested command help");
    };
    crate::cli::parse_matches(&matches)
}

// Desired policy fixtures are DATA. Publish them only through the existing
// authoritative module setting producer, retaining every other config field.
fn apply_history_fixture_policy<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    desired: &DfsFileSystem,
) {
    let expected = dfs.file_history_policy(&desired.file_system_id).unwrap();
    dfs.update_file_history_policy(DfsFileHistoryPolicyUpdateRequest {
        file_system_id: desired.file_system_id.clone(),
        expected,
        policy: decode_history_policy(desired.retention_policies.get(HISTORY_MODULE_ID)),
    })
    .unwrap();
}

// Read the job, visible head and source pin from the same existing metadata
// generation. Ending this read scope never pauses the autonomous Main owner.
struct RestoreJobObservation {
    job: Option<DfsNamespaceJobRecord>,
    receipt: Option<DfsNamespaceJobReceipt>,
    current: DfsObjectMetadata,
    pins: Vec<DfsMetadataVersionPinRecord>,
    source_version: Option<DfsObjectVersionRecord>,
    release_outcome: Option<DfsNamespaceTransactionReceipt>,
    target_write: Option<DfsWriteIntentRecord>,
    write_receipt: Option<DfsWriteIntentReceipt>,
}

fn restore_job_observation(
    backend: &mut dyn DfsMetadataBackend,
    job: &DfsNamespaceJobRecord,
    source_object_id: &DfsObjectId,
) -> RestoreJobObservation {
    let state = core_test::content_copy_job_state(job).unwrap();
    backend
        .begin_consistent_read_snapshot_for_path(&job.file_system_id, &job.root_path)
        .unwrap();
    let read =
        (|| -> std::result::Result<RestoreJobObservation, sinomo_dfs::MetadataBackendError> {
            let actual_job = backend.get_namespace_job(&job.job_id)?;
            let release_outcome = match actual_job.as_ref().and_then(|actual| {
                core_test::content_copy_job_state(actual)
                    .ok()?
                    .source_release
                    .as_ref()
            }) {
                Some(release) => backend.get_namespace_transaction_receipt(&format!(
                    "file-history:{}",
                    release.operation_id
                ))?,
                None => None,
            };
            Ok(RestoreJobObservation {
                job: actual_job,
                receipt: backend.get_namespace_job_receipt(&job.job_id)?,
                current: backend
                    .get_object_by_path(&job.file_system_id, &job.root_path)?
                    .ok_or_else(|| {
                        sinomo_dfs::MetadataBackendError::Serde(
                            "actual restore destination is absent".into(),
                        )
                    })?,
                pins: backend.version_pins_for_object(source_object_id)?,
                source_version: backend
                    .get_version(source_object_id, &state.request.source.version)?,
                release_outcome,
                write_receipt: backend.get_intent_receipt(
                    &state.planned.object_id,
                    &core_test::stable_write_intent_id(
                        &state.requester_node_id,
                        &state.planned.object_id,
                        &state.write_operation_id,
                    ),
                )?,
                target_write: backend.get_intent(
                    &state.planned.object_id,
                    &core_test::stable_write_intent_id(
                        &state.requester_node_id,
                        &state.planned.object_id,
                        &state.write_operation_id,
                    ),
                )?,
            })
        })();
    backend.end_consistent_read_snapshot_for_path(&job.file_system_id, &job.root_path);
    read.unwrap()
}

fn restore_observed_revision(object: &DfsObjectMetadata) -> DfsRevisionReference {
    DfsRevisionReference {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
    }
}

fn assert_restore_job_observation(
    admitted: &DfsNamespaceJobRecord,
    current: &DfsNamespaceJobRecord,
    observation: &RestoreJobObservation,
    edited: &DfsStat,
    reservation_key: &str,
) {
    let state = assert_restore_checkpoint_matches_admission(admitted, current);
    assert_eq!(observation.current.file_system_id, current.file_system_id);
    assert_eq!(observation.current.path, current.root_path);
    assert_eq!(observation.current.kind, DfsObjectKind::File);
    let actual_pin = observation
        .pins
        .iter()
        .find(|pin| content_reference_record_id(pin) == reservation_key);
    match current.status {
        DfsNamespaceJobStatus::Running => {
            match &state.result {
                None => {
                    if restore_observed_revision(&observation.current) == state.planned {
                        // The ordinary stable write can commit before the job
                        // checkpoints its result. Read its real immutable
                        // commit witness; never manufacture a job result.
                        let committed = observation
                            .target_write
                            .as_ref()
                            .expect("published target retains its actual stable write outcome");
                        assert_eq!(
                            committed.intent_id,
                            core_test::stable_write_intent_id(
                                &state.requester_node_id,
                                &state.planned.object_id,
                                &state.write_operation_id
                            )
                        );
                        assert_eq!(committed.file_system_id, state.planned.file_system_id);
                        assert_eq!(committed.object_id, state.planned.object_id);
                        assert_eq!(committed.target_version, state.planned.version);
                        assert_eq!(committed.coordinator_node_id, state.requester_node_id);
                        assert_eq!(
                            committed.operation_id.as_deref(),
                            Some(state.write_operation_id.as_str())
                        );
                        assert_eq!(committed.content_hash, state.content_hash);
                        assert_eq!(committed.policy, state.options.policy);
                        assert!(committed.publication.is_current());
                        assert_eq!(committed.status, DfsWriteIntentStatus::Committed);
                        assert_restore_published_head(state, observation);
                    } else {
                        assert_eq!(observation.current.file_system_id, edited.file_system_id);
                        assert_eq!(observation.current.object_id, edited.object_id);
                        assert_eq!(observation.current.version, edited.version);
                        assert_eq!(observation.current.path, edited.path);
                    }
                }
                Some(sinomo_dfs::DfsContentCopyJobResult::Applied { response }) => {
                    assert_eq!(response.restored_from, state.request.source);
                    assert_eq!(response.committed, state.planned);
                    assert_eq!(response.path, current.root_path);
                    assert_restore_published_head(state, observation);
                }
                Some(sinomo_dfs::DfsContentCopyJobResult::Rejected { .. }) => {
                    panic!("restore observation must not hide a rejected result");
                }
            }
            if let Some(pin) = actual_pin {
                assert_eq!(
                    pin.namespace,
                    state.source_reference.as_ref().unwrap().namespace
                );
                assert_eq!(pin.file_system_id, state.request.source.file_system_id);
                assert_eq!(pin.object_id, state.request.source.object_id);
                assert_eq!(pin.version, state.request.source.version);
                let source = observation
                    .source_version
                    .as_ref()
                    .expect("actual pinned source version survives GC");
                assert_eq!(source.file_system_id, pin.file_system_id);
                assert_eq!(source.object_id, pin.object_id);
                assert_eq!(source.version, pin.version);
                assert_eq!(source.content_hash, state.content_hash);
                assert_eq!(source.size_bytes, state.content_size_bytes);
            } else {
                assert!(matches!(
                    state.result,
                    Some(sinomo_dfs::DfsContentCopyJobResult::Applied { .. })
                ));
                let release = state
                    .source_release
                    .as_ref()
                    .expect("actual release request before pin removal");
                let outcome = observation
                    .release_outcome
                    .as_ref()
                    .expect("absent running-job pin requires its actual committed release outcome");
                assert_eq!(
                    outcome.transaction_id,
                    format!("file-history:{}", release.operation_id)
                );
                assert_eq!(outcome.file_system_id, release.identity.file_system_id);
                assert_eq!(
                    outcome.content_reference_identity.as_ref(),
                    Some(&release.identity)
                );
                let digest = blake3::hash(&serde_json::to_vec(release).unwrap())
                    .to_hex()
                    .to_string();
                assert_eq!(outcome.contribution_digest.as_ref(), Some(&digest));
            }
        }
        DfsNamespaceJobStatus::Completed => {
            assert_eq!(
                restore_observed_revision(&observation.current),
                state.planned
            );
            assert_eq!(observation.current.content_hash, state.content_hash);
            assert_eq!(
                observation.current.content_size_bytes,
                state.content_size_bytes
            );
            assert!(
                actual_pin.is_none(),
                "completed original job has released its source pin"
            );
        }
        DfsNamespaceJobStatus::Failed => unreachable!(),
    }
}

fn assert_restore_published_head(
    state: &DfsContentCopyJobState,
    observation: &RestoreJobObservation,
) {
    assert_eq!(
        restore_observed_revision(&observation.current),
        state.planned
    );
    assert_eq!(observation.current.content_hash, state.content_hash);
    assert_eq!(
        observation.current.content_size_bytes,
        state.content_size_bytes
    );
}

fn assert_restore_compacted_receipt(
    observed_terminal: &DfsNamespaceJobRecord,
    receipt: &DfsNamespaceJobReceipt,
) {
    let state = assert_restore_checkpoint_matches_admission(observed_terminal, observed_terminal);
    assert_eq!(observed_terminal.status, DfsNamespaceJobStatus::Completed);
    assert_eq!(receipt.job_id, observed_terminal.job_id);
    assert_eq!(receipt.file_system_id, observed_terminal.file_system_id);
    assert_eq!(receipt.origin, observed_terminal.origin);
    assert_eq!(receipt.kind, observed_terminal.kind);
    assert_eq!(receipt.status, observed_terminal.status);
    assert_eq!(receipt.root_path, observed_terminal.root_path);
    assert_eq!(receipt.target_path, observed_terminal.target_path);
    assert_eq!(receipt.created_at, observed_terminal.created_at);
    assert_eq!(receipt.terminal_at, observed_terminal.completed_at.unwrap());
    let actual = receipt
        .content_copy
        .as_ref()
        .expect("actual compact content-copy result");
    assert_eq!(actual.request, state.request);
    assert_eq!(actual.requester_node_id, state.requester_node_id);
    assert_eq!(&actual.result, state.result.as_ref().unwrap());
}

// The native owner may finish this same durable job between bounded calls.
fn assert_restore_checkpoint_matches_admission<'a>(
    admitted: &DfsNamespaceJobRecord,
    current: &'a DfsNamespaceJobRecord,
) -> &'a DfsContentCopyJobState {
    sinomo_dfs::validate_content_copy_job(admitted).unwrap();
    sinomo_dfs::validate_content_copy_job(current).unwrap();
    assert_eq!(current.job_id, admitted.job_id);
    assert_eq!(current.file_system_id, admitted.file_system_id);
    assert_eq!(current.origin, admitted.origin);
    assert_eq!(current.kind, admitted.kind);
    assert_eq!(current.root_path, admitted.root_path);
    assert_eq!(current.created_at, admitted.created_at);
    assert!(current.processed_object_count >= admitted.processed_object_count);
    let before = core_test::content_copy_job_state(admitted).unwrap();
    let actual = core_test::content_copy_job_state(current).unwrap();
    assert_eq!(actual.request, before.request);
    assert_eq!(actual.requester_node_id, before.requester_node_id);
    assert_eq!(actual.options, before.options);
    assert_eq!(actual.write_operation_id, before.write_operation_id);
    assert_eq!(actual.planned, before.planned);
    if before.source_reference.is_some() {
        assert_eq!(actual.source_reference, before.source_reference);
        assert_eq!(actual.content_hash, before.content_hash);
        assert_eq!(actual.content_size_bytes, before.content_size_bytes);
    }
    if before.source_reference_applied {
        assert!(actual.source_reference_applied);
    }
    if before.source_release.is_some() {
        assert_eq!(actual.source_release, before.source_release);
    }
    if before.result.is_some() {
        assert_eq!(actual.result, before.result);
    }
    match current.status {
        DfsNamespaceJobStatus::Running => {
            assert!(!actual.cleanup_complete);
            assert!(current.completed_at.is_none());
        }
        DfsNamespaceJobStatus::Completed => {
            let Some(sinomo_dfs::DfsContentCopyJobResult::Applied { response }) = &actual.result
            else {
                panic!("completed restore must retain its actual applied result");
            };
            assert_eq!(response.restored_from, before.request.source);
            assert_eq!(response.committed, before.planned);
            assert_eq!(response.path, current.root_path);
            assert!(actual.source_reference_applied);
            assert!(actual.source_release.is_some());
            assert!(actual.cleanup_complete);
            assert!(current.completed_at.is_some());
        }
        DfsNamespaceJobStatus::Failed => panic!("restore checkpoint must not hide a failed job"),
    }
    actual
}

// Exercise the SAME normal Main namespace admission and current content-copy
// factory. No fixture constructs a job origin, cursor or pending state.
fn begin_history_restore_job<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    path: &str,
    request: &DfsFileHistoryRestoreRequest,
    options: &DfsWriteOptions,
) -> DfsNamespaceJobRecord {
    let job_id = crate::validation::file_restore_job_id(dfs.node_id(), &request.operation_id);
    let order = sinomo_dfs::stable_operation_order_token(&request.operation_id)
        .unwrap_or_else(|| "opaque".into());
    dfs.execute_local_primary_namespace_operation_for_file_system(
        sifs_file_system_id(),
        format!("restore-begin:{job_id}_{order}"),
        sinomo_dfs::DfsNamespaceOperation::BeginContentCopy {
            path: path.into(),
            request: request.clone(),
            options: options.clone(),
        },
    )
    .unwrap();
    dfs.namespace_job(&job_id)
        .unwrap()
        .expect("normal admitted content-copy job")
}

fn native_history_core_service(
    name: &str,
    node: NodeId,
) -> (
    sinomo_dfs::test_support::DfsTestRuntime,
    NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
) {
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(name, node.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let service = runtime.client(&node).unwrap();
    (runtime, service)
}

fn native_history_remote_services(
    name: &str,
) -> (
    sinomo_dfs::test_support::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
    NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
) {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 2);
    let mut runtime =
        sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime.start_nodes().unwrap();
    let owner = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap()
        .primary_node_id;
    let requester = nodes.into_iter().find(|node| *node != owner).unwrap();
    let primary = runtime.client(&owner).unwrap();
    let client = runtime.client(&requester).unwrap();
    (runtime, owner, requester, primary, client)
}

fn native_history_repairs<T: DfsMessageTransport>(
    service: &NetworkedDfsService<T>,
) -> Vec<sinomo_dfs::DfsOperatorRepairView> {
    // This is the ordinary DATA inspection of stored intents, exact receipts
    // and live retention roots, never a job inferred from an RPC outcome.
    sinomo_dfs::DfsOperatorInspector::new(service.metadata_backend().unwrap())
        .repairs(None, None)
        .unwrap()
}

// Normal repair stores an immutable hash-addressed blob. A raw version
// manifest is not the receiver's completion contract. Bind the actual local
// bytes to the Main's stored receipt and exact retained version, without fetch.
fn assert_native_history_holder_bytes(
    primary: &sinomo_dfs::DfsMetadataTableSnapshot,
    primary_node: &NodeId,
    holder: &NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
    holder_node: &NodeId,
    identity: &DfsFileVersionIdentity,
    expected: &[u8],
) {
    let version = primary
        .tables
        .versions
        .iter()
        .find(|version| {
            version.object_id == identity.object_id && version.version == identity.version
        })
        .expect("actual retained target version");
    assert_eq!(version.file_system_id, identity.file_system_id);
    assert_eq!(version.object_id, identity.object_id);
    assert_eq!(version.version, identity.version);
    assert!(version.has_retention_grant());
    let hash = version.content_hash.as_deref().expect("actual target hash");
    assert_eq!(version.size_bytes, Some(expected.len() as u64));
    let receipt = primary
        .tables
        .receipts
        .iter()
        .find(|receipt| {
            &receipt.replica_node_id == holder_node
                && receipt.receipt.object_id == identity.object_id
                && receipt.receipt.committed_version == identity.version
        })
        .expect("actual Main persisted this holder's repair receipt");
    assert_eq!(receipt.receipt.file_system_id, identity.file_system_id);
    assert_eq!(receipt.receipt.object_id, identity.object_id);
    assert_eq!(receipt.receipt.committed_version, identity.version);
    assert_eq!(&receipt.receipt.owner_node_id, primary_node);
    assert_eq!(&receipt.replica_node_id, holder_node);
    assert_eq!(receipt.content_hash.as_deref(), Some(hash));
    assert!(
        version
            .content_durability
            .as_ref()
            .unwrap()
            .desired_replica_node_ids
            .contains(holder_node)
    );
    let bytes = holder
        .content()
        .read_blob(hash)
        .expect("exact local stored repair blob");
    assert_eq!(bytes, expected);
    assert_eq!(bytes.len() as u64, version.size_bytes.unwrap());
    assert_eq!(content_hash(&bytes), hash);
}

fn native_history_queue(
    runtime: &sinomo_dfs::test_support::DfsTestRuntime,
    owner: &NodeId,
) -> Vec<sinomo_dfs::DfsRepairAttemptRecord> {
    LocalDfsRepairQueueStore::new(sinomo_dfs::default_dfs_repair_attempt_snapshot_path(
        runtime.data_dir(owner),
    ))
    .view()
    .unwrap()
    .records
}

// Both metadata-only capture fixtures require the same immutable obligation.
// Completed N is proven by exact target receipts; pending N needs its real owner.
fn assert_native_history_recorded_or_settled_obligation(
    service: &NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
    expected: &DfsObjectVersionRecord,
) {
    let backend = service.metadata_backend().unwrap();
    let actual = backend
        .get_version(&expected.object_id, &expected.version)
        .unwrap()
        .unwrap();
    assert_eq!(actual.file_system_id, expected.file_system_id);
    assert_eq!(actual.object_id, expected.object_id);
    assert_eq!(actual.version, expected.version);
    assert!(
        expected.visible,
        "the original metadata generation was published"
    );
    assert_eq!(actual.visible, expected.visible);
    assert_eq!(actual.content_hash, expected.content_hash);
    assert_eq!(actual.size_bytes, expected.size_bytes);
    assert_eq!(actual.created_by_event_id, expected.created_by_event_id);
    assert_eq!(actual.content_durability, expected.content_durability);
    let plan = expected.content_durability.as_ref().unwrap();
    let grant = actual
        .retention_grant(HISTORY_MODULE_ID)
        .expect("the actual retained generation keeps its live module grant");
    assert_eq!(grant.object.file_system_id, expected.file_system_id);
    assert_eq!(grant.object.object_id, expected.object_id);
    assert_eq!(grant.object.version, expected.version);
    assert_eq!(grant.object.content_hash, expected.content_hash);
    assert_eq!(grant.object.content_size_bytes, expected.size_bytes);
    assert_eq!(grant.object.owner_node_id, plan.coordinator_node_id);
    let desired = plan
        .desired_replica_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let durable = backend
        .receipts_for_version(&expected.object_id, &expected.version)
        .unwrap()
        .into_iter()
        .filter(|row| {
            row.receipt.file_system_id == expected.file_system_id
                && row.receipt.object_id == expected.object_id
                && row.receipt.committed_version == expected.version
                && row.receipt.owner_node_id == plan.coordinator_node_id
                && row.content_hash == expected.content_hash
                && desired.contains(&row.replica_node_id)
        })
        .map(|row| row.replica_node_id)
        .collect::<BTreeSet<_>>();
    if durable == desired {
        return;
    }
    let jobs = native_history_repairs(service);
    for target in desired.difference(&durable) {
        let job = jobs
            .iter()
            .find(|job| {
                job.file_system_id == expected.file_system_id
                    && job.object_id == expected.object_id
                    && job.version == expected.version
                    && job.content_hash == expected.content_hash
                    && &job.missing_replica_node_id == target
            })
            .expect("every actual missing target retains an exact pending repair");
        let owner = backend
            .get_intent(&job.object_id, &job.intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(owner.intent_id, job.intent_id);
        assert_eq!(owner.file_system_id, expected.file_system_id);
        assert_eq!(owner.object_id, expected.object_id);
        assert_eq!(owner.target_version, expected.version);
        assert_eq!(owner.content_hash, expected.content_hash);
        assert_eq!(owner.coordinator_node_id, plan.coordinator_node_id);
        assert_eq!(owner.owner_epoch, plan.owner_epoch);
        assert_eq!(owner.policy, plan.policy);
        assert_eq!(
            owner.desired_replica_node_ids,
            plan.desired_replica_node_ids
        );
        assert_eq!(owner.status, DfsWriteIntentStatus::Committed);
        assert!(owner.publication.is_retained_repair());
    }
}

fn native_history_intent_policy<T: DfsMessageTransport>(
    service: &NetworkedDfsService<T>,
    repair: &sinomo_dfs::DfsOperatorRepairView,
) -> DfsReplicationPolicy {
    service
        .metadata_backend()
        .unwrap()
        .get_intent(&repair.object_id, &repair.intent_id)
        .unwrap()
        .unwrap()
        .policy
}

fn native_history_main_current_version(
    service: &NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
    object_id: &DfsObjectId,
) -> Option<DfsVersion> {
    // The client's physical metadata backend reads the actual durable Main
    // publication. Its private coordinator is not the served writer's state.
    service
        .metadata_backend()
        .unwrap()
        .get_object(object_id)
        .unwrap()
        .map(|object| object.version)
}

fn native_history_owner_tick(runtime: &sinomo_dfs::test_support::DfsTestRuntime, owner: &NodeId) {
    runtime.request_repair_reconciliation(owner).unwrap();
    let pass = runtime
        .maintenance_tick(owner, Duration::from_secs(5))
        .unwrap();
    pass.outcome.unwrap();
    assert_eq!(pass.omitted_kv_event_count, 0);
}

fn native_history_repair_services(
    name: &str,
    count: usize,
) -> (
    sinomo_dfs::test_support::DfsTestRuntime,
    NodeId,
    Vec<NodeId>,
    NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
) {
    let (runtime, service, config) =
        sinomo_dfs::test_support::current_native_metadata_service(name, count, "/services")
            .unwrap();
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, count);
    let others = nodes
        .into_iter()
        .filter(|node| *node != config.primary_node_id)
        .collect();
    (runtime, config.primary_node_id, others, service)
}

fn deny_history_content_targets(
    runtime: &sinomo_dfs::test_support::DfsTestRuntime,
    owner: &NodeId,
    targets: &[NodeId],
) {
    for target in targets {
        for route in [
            sinomo_dfs::DFS_DATA_STAGE_BLOB_ROUTE,
            sinomo_dfs::DFS_DATA_REPAIR_REPLICA_ROUTE,
        ] {
            runtime.install_fault(sinomo_dfs::test_support::DfsTestFaultRule {
                source: Some(owner.clone()),
                target: Some(target.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: Some(sinomo_dfs::test_support::DfsTestRpcOrigin::RegisteredHost),
                remaining_hits: 32,
                action: sinomo_dfs::test_support::DfsTestFaultAction::BudgetExhausted,
            });
        }
    }
}

fn current_history_shard_sequence<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> u64 {
    let backend = service.metadata_backend_mut().unwrap();
    // Other native owners can advance the physical shard after this client
    // opens. A status counter alone reads its cached next_sequence; refresh
    // the normal exact-path read view before selecting the comparison cut.
    backend
        .begin_consistent_read_snapshot_for_path(file_system_id, path)
        .unwrap();
    let shard = backend
        .metadata_shard_id_for_path(file_system_id, path)
        .unwrap()
        .expect("actual owning shard");
    let sequence = backend
        .metadata_shard_statuses()
        .unwrap()
        .into_iter()
        .find(|status| status.shard_id == shard)
        .expect("actual shard status")
        .journal_last_sequence;
    backend.end_consistent_read_snapshot_for_path(file_system_id, path);
    sequence
}

fn native_history_settled_peer_head(
    runtime: &sinomo_dfs::test_support::DfsTestRuntime,
    owner: &NodeId,
    peer_node: &NodeId,
    current: &DfsObjectMetadata,
) -> Option<DfsObjectMetadata> {
    let mut peer = runtime.client(peer_node).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "ordinary current metadata replication must settle before retained-only repair"
        );
        let synced = peer
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                current.file_system_id.clone(),
                owner.clone(),
                DfsNamespaceOperation::SyncPath {
                    path: current.path.clone(),
                },
                remaining,
                true,
                true,
            )
            .unwrap();
        assert!(matches!(synced.result, DfsNamespaceResult::Applied));
        let observed = peer
            .metadata_backend()
            .unwrap()
            .get_object(&current.object_id)
            .unwrap();
        if observed.as_ref() == Some(current) {
            break;
        }
    }
    let actual = peer
        .metadata_backend()
        .unwrap()
        .get_object(&current.object_id)
        .unwrap();
    assert_eq!(
        actual.as_ref(),
        Some(current),
        "the before snapshot must be the genuinely replicated current head"
    );
    actual
}

fn fail_native_history_repairs(
    runtime: &sinomo_dfs::test_support::DfsTestRuntime,
    owner: &NodeId,
    service: &mut NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
) {
    // Ask the actual Main to reconstruct/dispatch its own persisted work
    // while the original native denial is still active.
    let deadline = Instant::now() + Duration::from_secs(5);
    service.process_incoming().unwrap();
    runtime.request_repair_reconciliation(owner).unwrap();
    let remaining = deadline.saturating_duration_since(Instant::now());
    assert!(
        !remaining.is_zero(),
        "original native failure-observation budget"
    );
    let pass = runtime.maintenance_tick(owner, remaining).unwrap();
    pass.outcome.unwrap();
    assert_eq!(pass.omitted_kv_event_count, 0);
    // The owner releases admission before its real deferred receiver work.
    // Observe that child's outcome within the SAME deadline; do not infer an
    // RPC failure from scheduling or issue another maintenance invocation.
    loop {
        if runtime.rpc_observations().iter().any(|event| {
            &event.source == owner
                && event.origin == sinomo_dfs::test_support::DfsTestRpcOrigin::RegisteredHost
                && event.phase == sinomo_dfs::test_support::DfsTestRpcPhase::EndedWithoutResponse
                && [
                    sinomo_dfs::DFS_DATA_STAGE_BLOB_ROUTE,
                    sinomo_dfs::DFS_DATA_REPAIR_REPLICA_ROUTE,
                ]
                .contains(&event.route.as_str())
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the missing replica must be grounded in an actual failed native content RPC"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn drain_native_history_repairs(
    runtime: &sinomo_dfs::test_support::DfsTestRuntime,
    owner: &NodeId,
    service: &mut NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
) -> (
    Vec<sinomo_dfs::DfsOperatorRepairView>,
    sinomo_dfs::DfsMetadataTableSnapshot,
) {
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|event| &event.source == owner
                && event.origin == sinomo_dfs::test_support::DfsTestRpcOrigin::RegisteredHost
                && event.phase == sinomo_dfs::test_support::DfsTestRpcPhase::EndedWithoutResponse
                && [
                    sinomo_dfs::DFS_DATA_STAGE_BLOB_ROUTE,
                    sinomo_dfs::DFS_DATA_REPAIR_REPLICA_ROUTE
                ]
                .contains(&event.route.as_str())),
        "the missing replica must be grounded in an actual failed native content RPC"
    );
    // The caller budget measures a real eligible repair operation. Observe
    // finite known retry policy for only the current normal LIVE repair plan;
    // retired, parked and unresolved custody cannot delay this prerequisite.
    let current = runtime.metadata_primary_service("/services").unwrap();
    let backend = current.metadata_backend().unwrap();
    let repairs = native_history_repairs(&current);
    let mut retry_due = core_test::now_unix_nanos();
    let mut settled_retry_count = 0;
    for record in native_history_queue(runtime, owner) {
        if record.transfer_kind != Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair)
            || record.in_flight
            || record.transfer_request_id.is_some()
            || record.next_retry_unix_nanos == u64::MAX
            || !(record.transfer_delivery_state
                == Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
                || record.completed_transfer_request_id.is_some())
        {
            continue;
        }
        let Some(repair) = repairs.iter().find(|repair| {
            repair.intent_id == record.intent_id
                && repair.object_id == record.object_id
                && repair.version == record.version
                && repair.content_hash.as_deref() == Some(record.content_hash.as_str())
                && repair.missing_replica_node_id == record.missing_replica_node_id
        }) else {
            continue;
        };
        let intent = backend
            .get_intent(&repair.object_id, &repair.intent_id)
            .unwrap()
            .expect("the ordinary LIVE repair plan retains its exact immutable owner");
        assert_eq!(intent.intent_id, repair.intent_id);
        assert_eq!(intent.file_system_id, repair.file_system_id);
        assert_eq!(intent.object_id, repair.object_id);
        assert_eq!(intent.target_version, repair.version);
        assert_eq!(intent.content_hash, repair.content_hash);
        assert_eq!(&intent.coordinator_node_id, owner);
        intent.policy.validate().unwrap();
        assert!(
            intent
                .desired_replica_node_ids
                .contains(&repair.missing_replica_node_id)
        );
        assert!(
            !intent
                .policy
                .forbidden_node_ids
                .contains(&repair.missing_replica_node_id)
        );
        assert!(record.attempts > 0);
        assert!(record.last_attempt_unix_nanos > 0);
        assert!(record.next_retry_unix_nanos >= record.last_attempt_unix_nanos);
        retry_due = retry_due.max(record.next_retry_unix_nanos);
        settled_retry_count += 1;
    }
    drop(current);
    let eligibility_started = Instant::now();
    runtime.runtime_handle().block_on(async {
        let remaining = Duration::from_nanos(retry_due.saturating_sub(core_test::now_unix_nanos()));
        tokio::time::sleep_until(tokio::time::Instant::now() + remaining).await;
    });
    assert!(core_test::now_unix_nanos() >= retry_due);
    println!(
        "actual native history LIVE settled retry jobs={settled_retry_count} due_unix_nanos={retry_due} prerequisite_wait_seconds={:.6}; included in total test wall time",
        eligibility_started.elapsed().as_secs_f64()
    );
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let settled = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "actual Main repair receipts must settle within the existing network budget"
        );
        runtime.request_repair_reconciliation(owner).unwrap();
        let pass = runtime.maintenance_tick(owner, remaining).unwrap();
        pass.outcome.expect("actual Main repair pass");
        assert_eq!(pass.omitted_kv_event_count, 0);
        let current = runtime.metadata_primary_service("/services").unwrap();
        let pending = native_history_repairs(&current);
        if pending.is_empty() {
            let snapshot = current
                .metadata_backend()
                .unwrap()
                .export_table_snapshot()
                .unwrap();
            drop(current);
            assert!(
                Instant::now() <= deadline,
                "actual settled receipt observation must fit the original network budget"
            );
            break (pending, snapshot);
        }
        drop(current);
        // A checkpointed pass may hand payloads to the normal native task owner
        // before its actual receiver replies. Observe that owner, not a private
        // caller inbox or a one-pass scheduling report.
    };
    service.process_incoming().unwrap();
    settled
}
