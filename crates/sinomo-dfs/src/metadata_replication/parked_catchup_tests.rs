use super::*;

#[test]
fn native_catchup_parks_one_receiver_and_resumes_one_acknowledged_page_at_a_time() {
    use std::sync::atomic::AtomicBool;
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-parked-page", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-parked-page", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = &shard.primary_node_id;
    let target = &shard.replica_node_ids[0];
    let healthy = &shard.replica_node_ids[1];
    let data = runtime.data_dir(primary).clone();
    let id = DfsMetadataShardId::new(&shard.shard_id);
    let slot = crate::runtime_host::daemon_dfs_message_service_cache()
        .root(&data.root())
        .unwrap();
    let resident = slot
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .bounded_status_resident_current()
        .unwrap();
    let handle = resident.scope_shard(&shard.file_system_id, &id).unwrap();
    let (_, peer) = runtime.peer_transports(primary).unwrap();
    let send = async_dfs_metadata_journal_batch_sender_from_peer_transport(&data, peer.clone());
    let unavailable = Arc::new(AtomicBool::new(false));
    let fault = unavailable.clone();
    let refused = target.clone();
    let sender: AsyncDfsMetadataJournalBatchSender = Arc::new(move |node, command, timeout| {
        if node == &refused && fault.load(Ordering::Acquire) {
            // Inject only the negative reply. Every cursor advance below
            // comes from the real native receiver and its exact confirmation.
            return Box::pin(async move {
                Ok(crate::runtime_message::dfs_metadata_journal_rejection(
                    command,
                    DfsMetadataRejection::new(
                        DfsMetadataRejectionCode::StoragePreparationPending,
                        "preparation pending",
                    ),
                ))
            });
        }
        send(node, command, timeout)
    });
    let transport = AsyncDfsMetadataNodeTransport::new(
        shard.replica_node_ids.clone(),
        sender,
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer),
    );
    let options = DfsMetadataJournalReplicationOptions {
        background_publication: true,
        batch_entry_limit: 1,
        ..Default::default()
    };
    let turn = |options| {
        let mut lease = handle.checkout().unwrap();
        let plan = prepare_cluster_dfs_metadata_shard_publish_with_backend_and_authority(
            &data,
            primary,
            shard.replica_node_ids.clone(),
            shard.file_system_id.clone(),
            id.clone(),
            Some(shard.clone()),
            &mut lease,
            &Default::default(),
            options,
        )
        .unwrap()
        .unwrap();
        for target in &plan.targets {
            assert!(target.commands.len() + usize::from(target.pinned_page.is_some()) <= 1);
        }
        drop(lease);
        runtime
            .runtime_handle()
            .block_on(execute_prepared_dfs_metadata_journal_publish_async(
                plan, &transport, options,
            ))
            .unwrap()
    };
    let cursor = |node| {
        handle
            .checkout()
            .unwrap()
            .metadata_replication_cursor(node, &id)
            .unwrap()
    };
    let head = || handle.checkout().unwrap().journal_last_sequence();
    let append = |number| {
        let mut lease = handle.checkout().unwrap();
        let backend: &mut dyn DfsMetadataBackend = &mut *lease;
        let mut fs = backend
            .get_file_system_on_shard(&id, &shard.file_system_id)
            .unwrap()
            .unwrap();
        fs.display_name.push_str(&format!("-{number}"));
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: shard.file_system_id.clone(),
                shard_id: id.clone(),
                owner_epoch: shard.epoch,
                operation_id: format!("parked-page-{number}"),
            },
            Vec::new(),
            Vec::new(),
            vec![crate::DfsPreparedMetadataEffect::PutFileSystem { file_system: fs }],
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        backend.prepare_operation(payload, None).unwrap();
    };
    for _ in 0..=head() {
        if cursor(target) == head() && cursor(healthy) == head() {
            break;
        }
        turn(options);
    }
    let original = head();
    assert_eq!((cursor(target), cursor(healthy)), (original, original));
    unavailable.store(true, Ordering::Release);
    append(1);
    let rejected = turn(options);
    assert!(rejected.outcomes.iter().any(|x| matches!(x,
        DfsMetadataJournalReplicationOutcome::StoragePreparationPending { target_node_id, after_sequence, .. }
        if target_node_id == target && *after_sequence == original)), "{rejected:?}");
    unavailable.store(false, Ordering::Release);
    for number in 2..=4 {
        append(number);
        let report = turn(options);
        assert!(report.outcomes.iter().any(|x| matches!(x,
            DfsMetadataJournalReplicationOutcome::StoragePreparationPending { target_node_id, .. }
            if target_node_id == target)));
        assert_eq!(cursor(target), original);
        assert_eq!(cursor(healthy), head());
    }
    // A foreground durability request still reaches this receiver. Its real
    // ACK changes the cursor and immediately releases the parked background
    // lane, even though no new commit or retry deadline has arrived.
    let repair = turn(DfsMetadataJournalReplicationOptions {
        background_publication: false,
        max_batches_per_target: 1,
        ..options
    });
    assert!(repair.outcomes.iter().any(|o| matches!(o,
        DfsMetadataJournalReplicationOutcome::Applied { target_node_id, entries: 1, next_after_sequence, .. }
        if target_node_id == target && *next_after_sequence == original + 1)), "{repair:?}");
    for next in original + 2..=head() {
        let report = turn(options);
        assert!(report.outcomes.iter().any(|o| matches!(o,
            DfsMetadataJournalReplicationOutcome::Applied { target_node_id, entries: 1, next_after_sequence, .. }
            if target_node_id == target && *next_after_sequence == next)), "{report:?}");
    }
    assert_eq!(cursor(target), head());
    drop(transport);
    drop(handle);
    drop(resident);
    runtime.close().unwrap();
}
