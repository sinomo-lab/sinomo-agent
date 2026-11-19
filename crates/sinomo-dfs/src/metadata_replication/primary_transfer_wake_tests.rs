use super::*;
use crate::default_reconciliation::{
    DefaultDfsReconciliationProjection, DefaultDfsReconciliationRuntime,
};
use sinomo_core::kv::{KvRevision, encode_kv_value};

fn native_checkpoint_fixture(
    name: &str,
) -> (
    crate::test_runtime::DfsTestRuntime,
    SinomoDataDir,
    DfsMetadataShardConfig,
    NodeId,
    u64,
    DefaultDfsReconciliationRuntime,
    Vec<(String, Vec<u8>)>,
) {
    let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let target = shard.replica_node_ids[0].clone();
    let data_dir = runtime.data_dir(&shard.primary_node_id).clone();
    let mut writer = runtime.client(&shard.primary_node_id).unwrap();
    writer
        .posix_write_file(
            "/native-checkpoint.bin",
            b"actual acknowledged metadata",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![shard.primary_node_id.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    // A W1 write return does not finish the producer's asynchronous journal
    // publication. Observe the actual full-shard durability barrier before
    // this fixture treats the saved peer cursor as a completed checkpoint.
    let certificate = writer
        .posix_sync_directory("/")
        .unwrap()
        .metadata_certificate;
    assert_eq!(certificate.target.file_system_id, shard.file_system_id);
    assert_eq!(
        certificate.target.shard_id,
        DfsMetadataShardId::new(shard.shard_id.clone())
    );
    assert_eq!(
        certificate
            .acknowledged_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        std::iter::once(shard.primary_node_id.clone())
            .chain(shard.replica_node_ids.iter().cloned())
            .collect()
    );
    drop(writer);
    // The browser retains its pre-write inspection image. Select the actual
    // owning Root journal that produced the acknowledged native write instead.
    let source = runtime
        .metadata_primary_service("/native-checkpoint.bin")
        .unwrap();
    let head = source
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    assert_eq!(certificate.target.journal_sequence, head);
    drop(source);
    let mut reactor = DefaultDfsReconciliationRuntime::new(
        &ClusterPaths::new(&data_dir),
        &shard.primary_node_id,
        None,
        None,
    )
    .unwrap();
    let inputs = vec![(
        metadata_shard_key(&shard.file_system_id, &shard.shard_id),
        encode_kv_value(&shard).unwrap(),
    )];
    reactor
        .projection_sender()
        .send_replace(DefaultDfsReconciliationProjection {
            revision: Some(KvRevision(1)),
            inputs: inputs.clone(),
            placement_authority_permit: None,
        });
    let initial = reactor.pending_work().unwrap();
    reactor.complete_prior_process_recovery(&initial);
    reactor.complete_default_reconciliation(&initial);
    let selection =
        crate::default_reconciliation::capture_primary_transfer_selection(&data_dir).unwrap();
    selection
        .register(&shard, &target, head)
        .unwrap()
        .keep_waiting();
    (runtime, data_dir, shard, target, head, reactor, inputs)
}

#[test]
fn native_verified_checkpoint_wakes_exact_selection_once_without_configuration_change() {
    let (runtime, data_dir, shard, target, head, mut reactor, inputs) =
        native_checkpoint_fixture("native-transfer-checkpoint-wake");
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    assert_eq!(
        metadata_cursor::metadata_replication_cursor(
            &local_dfs_metadata_shard_path(&data_dir, &shard_id),
            &target,
            &shard_id
        )
        .unwrap(),
        head
    );
    put_local_dfs_metadata_replication_cursor(&data_dir, &target, &shard_id, head, None).unwrap();
    let work = reactor
        .pending_work()
        .expect("actual durable checkpoint satisfies the exact registered dependency");
    assert!(work.reconcile_defaults);
    assert_eq!(work.inputs, inputs);
    reactor.complete_default_reconciliation(&work);
    put_local_dfs_metadata_replication_cursor(&data_dir, &target, &shard_id, head, None).unwrap();
    assert!(
        reactor.pending_work().is_none(),
        "no dependency remains to wake steady state"
    );
    drop(reactor);
    runtime.close().unwrap();
}

#[test]
fn native_cancelled_checkpoint_waiter_preserves_completed_cursor_notification() {
    let (runtime, data_dir, shard, target, head, mut reactor, inputs) =
        native_checkpoint_fixture("native-transfer-checkpoint-cancel");
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let executor = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    executor.block_on(async {
        let (entered, paused) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let _hook = install_metadata_cursor_checkpoint_test_hook(&data_dir, move || {
            entered.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(10)).unwrap();
        });
        let publish = tokio::spawn(put_local_dfs_metadata_replication_cursor_async(
            data_dir.clone(),
            target.clone(),
            shard_id.clone(),
            head,
            None,
            None,
        ));
        tokio::time::timeout(Duration::from_secs(5), paused)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            metadata_cursor::metadata_replication_cursor(
                &local_dfs_metadata_shard_path(&data_dir, &shard_id),
                &target,
                &shard_id
            )
            .unwrap(),
            head
        );
        assert!(reactor.pending_work().is_none());
        let writer_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(metadata_cursor::journal_write_lock_path(
                &local_dfs_metadata_shard_path(&data_dir, &shard_id),
            ))
            .unwrap();
        fs2::FileExt::try_lock_exclusive(&writer_file).expect("notification owns no writer flock");
        fs2::FileExt::unlock(&writer_file).unwrap();
        publish.abort();
        assert!(publish.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), reactor.changed())
            .await
            .unwrap();
        let work = reactor.pending_work().unwrap();
        assert_eq!(work.inputs, inputs);
        reactor.complete_default_reconciliation(&work);
        assert!(reactor.pending_work().is_none());
    });
    drop(executor);
    drop(reactor);
    runtime.close().unwrap();
}
