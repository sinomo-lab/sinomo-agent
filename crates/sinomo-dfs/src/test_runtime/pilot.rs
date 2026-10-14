//! Real journal and snapshot replies must survive receiver shutdown and reopen.
//! Source records use the normal client; receiver results come from native ingress.
#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use super::{DfsTestRuntime, FaultAction, FaultRule, RpcOrigin};
use crate::metadata_replication::{
    dfs_metadata_durable_journal_batch_command, prepare_dfs_metadata_journal_frame_batch,
};
use crate::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
    DfsMessageTransport, DfsMetadataBackend, DfsMetadataJournalBatchResult, DfsMetadataShardId,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResult, LocalSobjMetadataBackend,
    decode_dfs_metadata_snapshot_install_rpc_response, default_dfs_metadata_shards_root,
    dfs_metadata_snapshot_install_rpc_request,
};
use tokio::sync::Semaphore;

#[test]
fn observation_checkpoints_preserve_full_events_and_reject_missing_or_changed_evidence() {
    let node = DfsTestRuntime::ordered_node_ids("observation-checkpoints", 1).remove(0);
    let mut runtime = DfsTestRuntime::new("observation-checkpoints", node.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let mut client = runtime.client(&node).unwrap();
    client
        .posix_mkdir_all("/users/observation-checkpoint")
        .unwrap();
    let before = runtime.rpc_observations();
    assert!(!before.is_empty());
    let count = runtime.checkpoint_rpc_observation_window().unwrap();
    assert_eq!(count, before.len());
    assert!(runtime.rpc_observations().is_empty());
    {
        let seals = runtime.fabric.observation_checkpoints.lock().unwrap();
        let seal = &seals[0];
        let bytes = std::fs::read(&seal.path).unwrap();
        let restored: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            restored,
            before
                .iter()
                .map(|event| format!("{event:?}"))
                .collect::<Vec<_>>()
        );
        std::fs::write(&seal.path, b"[]").unwrap();
        assert!(seal.verify().is_err(), "changed evidence must fail closed");
        std::fs::remove_file(&seal.path).unwrap();
        assert!(seal.verify().is_err(), "missing evidence must fail closed");
        std::fs::write(&seal.path, bytes).unwrap();
        seal.verify().unwrap();
    }
    client
        .posix_stat_distributed("/users/observation-checkpoint")
        .unwrap();
    let next = runtime.rpc_observations();
    assert!(!next.is_empty());
    let collision = runtime
        .root
        .as_ref()
        .unwrap()
        .join("rpc-observations-000001.json");
    std::fs::write(&collision, b"existing recovery evidence").unwrap();
    assert!(runtime.checkpoint_rpc_observation_window().is_err());
    assert_eq!(
        format!("{:?}", runtime.rpc_observations()),
        format!("{next:?}")
    );
    assert_eq!(
        std::fs::read(&collision).unwrap(),
        b"existing recovery evidence"
    );
    std::fs::remove_file(collision).unwrap();
    assert_eq!(
        runtime.checkpoint_rpc_observation_window().unwrap(),
        next.len()
    );
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(client);
    let coverage = runtime.close_inner(None).unwrap();
    assert!(coverage.lifetime_records >= count + next.len());
    assert_eq!(coverage.lifetime_omitted, 0);
    assert_eq!(coverage.full_lifetime_coverage, "available");
}

#[test]
fn embedded_runtime_journal_and_snapshot_roundtrip_survive_receiver_reopen() {
    let nodes = DfsTestRuntime::ordered_node_ids("embedded-replication-roundtrip", 3);
    let seed = nodes[0].clone();
    let mut runtime = DfsTestRuntime::new("embedded-replication-roundtrip", seed).unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    // All three assigned metadata participants are required for publication;
    // the native Raft profile remains its actual one voter.
    runtime.define_node(nodes[2].clone()).unwrap();

    // Initial topology uses the normal planner before Embedded mount; consume
    // its retained root assignment and default filesystem identity below.
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let replica = shard.replica_node_ids[0].clone();
    assert_eq!(shard.replica_node_ids.len(), 2);
    assert_ne!(primary, replica);
    assert_eq!(
        std::iter::once(primary.clone())
            .chain(shard.replica_node_ids.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        nodes.iter().cloned().collect()
    );
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let mut file_system = runtime.file_system();
    file_system.display_name = "embedded journal roundtrip".to_string();
    let mut primary_client = runtime.client(&primary).unwrap();
    // This normal service method performs the accepted primary config commit
    // and ensures the filesystem in the SAME client's durable metadata backend.
    primary_client.add_file_system(file_system.clone()).unwrap();

    // Export the actual native Root cut, not a mounted requester's private image.
    let mut journal_source = runtime.metadata_primary_service("/").unwrap();
    let mut journal_head = 0;
    loop {
        let prepared = {
            let backend = journal_source
                .metadata_backend()
                .expect("real primary backend");
            let Some(batch) = backend
                .journal_batch_for_shard_since_limited(
                    &shard_id,
                    journal_head,
                    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
                )
                .unwrap()
            else {
                break;
            };
            prepare_dfs_metadata_journal_frame_batch(backend, &batch).unwrap()
        };
        let command = dfs_metadata_durable_journal_batch_command(
            &primary,
            &replica,
            &shard.file_system_id,
            shard.epoch,
            prepared,
            false,
        );
        let durability = command.durable_replication.as_ref().unwrap();
        let confirmation_id = durability.confirmation_id.clone();
        let frame_count = command.batch.journal_frames.len();
        assert!(frame_count > 0);
        assert!(command.batch.next_after_sequence > journal_head);
        // DfsNodeTransport's actual direct object-RPC lane invokes the real
        // receiver. No response is synthesized or injected by this pilot.
        let response = DfsMessageTransport::send_metadata_journal_batch_direct(
            &mut journal_source.transport,
            &replica,
            &command,
        )
        .unwrap()
        .expect("normal direct metadata lane is available");
        assert_eq!(response.request_id, command.request_id);
        assert_eq!(response.shard_id, shard_id);
        assert_eq!(
            response.next_after_sequence,
            command.batch.next_after_sequence
        );
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
        assert!(
            response.applied_entries <= frame_count,
            "replaying an already durable prefix only counts newly applied frames"
        );
        assert_eq!(response.verified_authority_probe_id, Some(confirmation_id));
        journal_head = command.batch.next_after_sequence;
    }
    assert!(journal_head > 0);
    let journal_prefix = journal_source
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap()
        .prefix_proof;
    assert_eq!(journal_prefix.journal_sequence, journal_head);
    assert!(journal_prefix.is_well_formed());

    drop(journal_source);
    runtime.stop_node(&replica).unwrap();
    let replica_root = default_dfs_metadata_shards_root(runtime.data_dir(&replica)).join("root");
    {
        // Stop must quiesce hosted work and release writer ownership before
        // ordinary reopen. This reads receiver storage, not a sender hint/cache.
        let receiver = LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).unwrap();
        assert_eq!(
            receiver.get_file_system(&shard.file_system_id).unwrap(),
            Some(file_system.clone())
        );
        assert_eq!(
            receiver.metadata_replay_cursor(&shard_id).unwrap(),
            journal_head
        );
        let floor = receiver
            .metadata_prefix_ack_floor(&shard_id)
            .unwrap()
            .unwrap();
        assert_eq!(floor.journal_sequence, journal_head);
        assert_eq!(floor.durable_prefix_proof, journal_prefix);
    }

    // Observe only the next actual publication's work, excluding setup.
    let _ = primary_client.take_message_committed_work();
    let journal_file_system = file_system.clone();
    file_system.display_name = "embedded snapshot roundtrip".to_string();
    let denied = primary_client.add_file_system(file_system.clone());
    assert!(
        matches!(&denied,
        Err(crate::DfsError::MetadataRecoveryQuorumUnavailable {
            file_system_id, shard_id: denied_shard, required: 3, verified: 2,
        }) if file_system_id == &shard.file_system_id && denied_shard == &shard.shard_id),
        "a stopped assigned receiver cannot be replaced by a permit or majority: {denied:?}"
    );
    assert_eq!(runtime.root_shard().unwrap(), shard);
    let denied_prepare = {
        let root = default_dfs_metadata_shards_root(runtime.data_dir(&primary)).join("root");
        let unchanged =
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), root).unwrap();
        assert_eq!(
            unchanged.get_file_system(&shard.file_system_id).unwrap(),
            Some(journal_file_system)
        );
        // A failed ALL-assigned probe leaves its actual hidden prepare durable.
        // It must not publish the filesystem row or append a terminal commit.
        let hidden = unchanged
            .journal_batch_for_shard_since_limited(
                &shard_id,
                journal_head,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .unwrap()
            .expect("the failed native probe retains its exact hidden preparation");
        assert_eq!(hidden.entries.len(), 1);
        let entry = &hidden.entries[0];
        assert_eq!(entry.sequence, journal_head + 1);
        assert_eq!(entry.shard_id, shard_id);
        let crate::DfsMetadataMutation::PrepareOperationV7 {
            file_system_id,
            operation_id,
            record,
        } = &entry.mutation
        else {
            panic!("denied publication cannot produce a terminal frame: {entry:?}")
        };
        assert_eq!(file_system_id, &shard.file_system_id);
        assert_eq!(operation_id, &record.digest.operation_id);
        assert_eq!(record.digest.file_system_id, shard.file_system_id);
        assert_eq!(record.digest.shard_id, shard_id);
        assert_eq!(record.digest.owner_epoch, shard.epoch);
        assert_eq!(record.digest.prepare_sequence, entry.sequence);
        record.validate().unwrap();
        assert_eq!(record.status, crate::DfsPreparedOperationStatus::Prepared);
        assert!(record.committed_entry.is_none());
        assert_eq!(
            record.payload.metadata_effects,
            vec![crate::DfsPreparedMetadataEffect::PutFileSystem {
                file_system: file_system.clone()
            },]
        );
        assert_eq!(
            unchanged.prepared_operation_record(operation_id).unwrap(),
            Some(record.as_ref().clone())
        );
        assert!(
            unchanged
                .committed_prepared_operation(&record.digest)
                .unwrap()
                .is_none()
        );
        let frames = unchanged.journal_frames_for_batch(&hidden).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].sequence, entry.sequence);
        let hidden_prefix = crate::metadata_journal::metadata_prefix_proof_after_exact_frame(
            &journal_prefix,
            entry,
            &shard_id,
            &frames[0].exact_bytes,
        )
        .unwrap();
        assert_eq!(
            unchanged.metadata_replay_cursor(&shard_id).unwrap(),
            entry.sequence
        );
        assert_eq!(unchanged.metadata_prefix_proof().unwrap(), hidden_prefix);
        record.digest.clone()
    };
    // Restore this actual receiver, then complete through ordinary full-set
    // publication/recovery, without changing the assignment or permission.
    runtime.reopen_node(replica.clone()).unwrap();
    primary_client.add_file_system(file_system.clone()).unwrap();
    let recovered_work = primary_client.take_message_committed_work();
    // Another normal current owner may already have settled the preparation
    // during reopen. This call may then be quiet; any work it reports is journal
    // publication, rather than an invented namespace-object change event.
    assert!(
        recovered_work == crate::DfsMessageCommittedWork::default()
            || recovered_work
                == crate::DfsMessageCommittedWork {
                    publish_metadata: true,
                    ..Default::default()
                },
        "filesystem recovery reports only its actual journal work: {recovered_work:?}"
    );
    assert!(
        !recovered_work.semantic_changes,
        "filesystem/configuration effects do not invent namespace-object change events"
    );
    primary_client.add_file_system(file_system.clone()).unwrap();
    assert_eq!(
        primary_client.take_message_committed_work(),
        crate::DfsMessageCommittedWork::default(),
        "exact filesystem replay with no active preparation is quiet"
    );
    let snapshot_source = runtime.metadata_primary_service("/").unwrap();
    let recovered = snapshot_source
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&denied_prepare.operation_id)
        .unwrap()
        .expect("ordinary full-set recovery preserves the same hidden body");
    assert_eq!(recovered.digest, denied_prepare);
    assert_eq!(
        recovered.status,
        crate::DfsPreparedOperationStatus::Committed
    );
    assert!(recovered.committed_entry.is_some());
    assert!(
        snapshot_source
            .metadata_backend()
            .unwrap()
            .committed_prepared_operation(&denied_prepare)
            .unwrap()
            .is_some()
    );
    let snapshot = snapshot_source
        .metadata_backend()
        .unwrap()
        .export_shard_snapshot_image(&shard_id)
        .unwrap()
        .unwrap();
    let snapshot_head = snapshot.materialized_through_sequence;
    assert!(snapshot_head > journal_head);
    let snapshot_prefix = snapshot.prefix_proof.clone();
    assert!(snapshot_prefix.is_well_formed());
    assert_eq!(snapshot_prefix.journal_sequence, snapshot_head);
    runtime.stop_node(&replica).unwrap();
    let command = DfsMetadataSnapshotInstallCommand {
        request_id: "embedded-real-snapshot-roundtrip".to_string(),
        source_node_id: primary.clone(),
        file_system_id: Some(shard.file_system_id.clone()),
        authority_epoch: Some(shard.epoch),
        primary_handoff_id: None,
        shard_id: shard_id.clone(),
        next_after_sequence: snapshot_head,
        snapshot: snapshot.descriptor().clone(),
        image: Some(snapshot),
        reason: "normal Embedded receiver durable snapshot pilot".to_string(),
    };
    // These bounded, fail-closed rules isolate the actual snapshot effect from
    // registered-host background catch-up. The explicit fixture RPC still uses
    // the same authenticated carrier and real receiver; only its origin differs.
    // If normal host preparation requires held traffic, reopen must fail and
    // preserve that precondition failure, not fabricate authority or readiness.
    let receiver_outgoing_gate = Arc::new(Semaphore::new(0));
    let primary_replication_gate = Arc::new(Semaphore::new(0));
    for route in [
        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
        crate::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
        crate::DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE,
    ] {
        runtime.install_fault(FaultRule {
            source: Some(replica.clone()),
            target: None,
            route: Some(route.into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: FaultAction::PauseBeforeDispatch(Arc::clone(&receiver_outgoing_gate)),
        });
        runtime.install_fault(FaultRule {
            source: Some(primary.clone()),
            target: Some(replica.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: FaultAction::PauseBeforeDispatch(Arc::clone(&primary_replication_gate)),
        });
    }
    runtime.reopen_node(replica.clone()).unwrap();
    {
        let before =
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &replica_root).unwrap();
        assert_eq!(
            before.get_file_system(&shard.file_system_id).unwrap(),
            Some(file_system.clone())
        );
        assert_eq!(
            before.metadata_replay_cursor(&shard_id).unwrap(),
            snapshot_head
        );
        assert_eq!(
            before.metadata_prefix_proof().unwrap(),
            snapshot_prefix.clone()
        );
    }
    runtime.assert_fault_budgets().unwrap();
    assert_eq!(runtime.observation_dropped_count(), 0);
    let response = runtime
        .rpc_request(
            &primary,
            &replica,
            dfs_metadata_snapshot_install_rpc_request(&command).unwrap(),
            Duration::from_secs(3),
        )
        .unwrap();
    let installed = decode_dfs_metadata_snapshot_install_rpc_response(&response).unwrap();
    assert_eq!(installed.request_id, command.request_id);
    assert_eq!(installed.shard_id, shard_id);
    assert_eq!(installed.next_after_sequence, snapshot_head);
    assert!(matches!(
        installed.result,
        DfsMetadataSnapshotInstallResult::Installed { .. }
    ));
    runtime.assert_fault_budgets().unwrap();
    assert_eq!(runtime.observation_dropped_count(), 0);

    runtime.stop_node(&replica).unwrap();
    {
        let receiver = LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).unwrap();
        assert_eq!(
            receiver.get_file_system(&shard.file_system_id).unwrap(),
            Some(file_system)
        );
        assert_eq!(
            receiver.metadata_replay_cursor(&shard_id).unwrap(),
            snapshot_head
        );
        let durable_image = receiver
            .export_shard_table_snapshot(&shard_id)
            .unwrap()
            .unwrap();
        assert_eq!(durable_image.materialized_through_sequence, snapshot_head);
        assert_eq!(durable_image.prefix_proof, snapshot_prefix);
    }
    // Release the held image and actual source before normal owner teardown.
    drop(command);
    drop(snapshot_source);
    drop(primary_client);
    runtime
        .close()
        .expect("embedded DFS case closes every owned task and producer");
}
