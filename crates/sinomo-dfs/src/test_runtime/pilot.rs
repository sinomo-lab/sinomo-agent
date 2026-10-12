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
fn embedded_runtime_journal_and_snapshot_roundtrip_survive_receiver_reopen() {
    let nodes = DfsTestRuntime::ordered_node_ids("embedded-replication-roundtrip", 3);
    let primary = nodes[0].clone();
    let replica = nodes[1].clone();
    let mut runtime =
        DfsTestRuntime::new("embedded-replication-roundtrip", primary.clone()).unwrap();
    runtime.define_node(replica.clone()).unwrap();
    // A live third metadata replica supplies the real recovery majority while
    // this receiver is stopped; the Raft profile remains its actual one voter.
    runtime.define_node(nodes[2].clone()).unwrap();

    // Initial topology uses the normal planner before Embedded mount; consume
    // its retained root assignment and default filesystem identity below.
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let mut file_system = runtime.file_system();
    file_system.display_name = "embedded journal roundtrip".to_string();
    let mut primary_client = runtime.client(&primary).unwrap();
    // This normal service method performs the accepted primary config commit
    // and ensures the filesystem in the SAME client's durable metadata backend.
    primary_client.add_file_system(file_system.clone()).unwrap();

    let mut journal_head = 0;
    loop {
        let prepared = {
            let backend = primary_client
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
            &mut primary_client.transport,
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
    let journal_prefix = primary_client
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard_id)
        .unwrap()
        .unwrap()
        .prefix_proof;
    assert_eq!(journal_prefix.journal_sequence, journal_head);
    assert!(journal_prefix.is_well_formed());

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

    // Normal publication obtains genuine source + third-replica metadata ACKs
    // and an operation-bound native permit while this receiver is stopped.
    let journal_file_system = file_system.clone();
    file_system.display_name = "embedded snapshot roundtrip".to_string();
    primary_client.add_file_system(file_system.clone()).unwrap();
    let snapshot = primary_client
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
    runtime.install_fault(FaultRule {
        source: Some(replica.clone()),
        target: None,
        route: None,
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(receiver_outgoing_gate),
    });
    runtime.install_fault(FaultRule {
        source: Some(primary.clone()),
        target: Some(replica.clone()),
        route: None,
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(primary_replication_gate),
    });
    runtime.reopen_node(replica.clone()).unwrap();
    {
        let before =
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &replica_root).unwrap();
        assert_eq!(
            before.get_file_system(&shard.file_system_id).unwrap(),
            Some(journal_file_system)
        );
        assert_eq!(
            before.metadata_replay_cursor(&shard_id).unwrap(),
            journal_head
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
    // Release the live source client before the owning facility's teardown.
    drop(primary_client);
    runtime
        .close()
        .expect("embedded DFS case closes every owned task and producer");
}
