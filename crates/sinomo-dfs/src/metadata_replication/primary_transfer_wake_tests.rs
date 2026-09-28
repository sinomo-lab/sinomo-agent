use super::*;
use crate::default_reconciliation::{
    DefaultDfsReconciliationProjection, DefaultDfsReconciliationRuntime,
};
use sinomo_core::kv::{KvRevision, encode_kv_value};

pub(super) struct TransferFixture {
    pub(super) _root: sinomo_testdir::TestDir,
    pub(super) data_dir: SinomoDataDir,
    pub(super) reactor: DefaultDfsReconciliationRuntime,
    pub(super) shard: DfsMetadataShardConfig,
    pub(super) target: NodeId,
    pub(super) resident: crate::ResidentLocalShardedMetadataBackend,
    pub(super) inputs: Vec<(String, Vec<u8>)>,
    pub(super) receiver: Arc<Mutex<LocalSobjMetadataBackend>>,
}

impl TransferFixture {
    pub(super) fn new(name: &str) -> Self {
        Self::with_seeded_backend(name, true)
    }

    fn with_seeded_backend(name: &str, seeded: bool) -> Self {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().unwrap();
        let primary = NodeId::from_legacy_named(format!("{name}-primary")).unwrap();
        let target = NodeId::from_legacy_named(format!("{name}-target")).unwrap();
        let shard = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "users".into(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![target.clone()],
            epoch: 2,
            assignment: crate::DfsMetadataShardAssignment::PathPrefix {
                prefix: "/users".into(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut root_shard = shard.clone();
        root_shard.shard_id = "root".into();
        root_shard.assignment = crate::DfsMetadataShardAssignment::Root;
        let open_source = || {
            let resident = LocalShardedMetadataBackend::new(
                crate::default_dfs_metadata_shards_root(&data_dir),
                vec![root_shard.clone(), shard.clone()],
            )
            .unwrap()
            .into_resident();
            let handle = resident
                .scope_shard(&shard.file_system_id, &DfsMetadataShardId::new("users"))
                .unwrap();
            handle
                .checkout()
                .unwrap()
                .put_file_system(&distributed_sifs(primary.clone()))
                .unwrap();
            resident
        };
        let mut resident = seeded.then(open_source);
        let mut reactor = DefaultDfsReconciliationRuntime::new(
            &ClusterPaths::new(&data_dir),
            &primary,
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
        assert!(
            !crate::default_cluster::metadata_shard_primary_transfer_ready_from_local_backend(
                &data_dir, &primary, &shard, &target,
            )
            .unwrap(),
            "selection must initially be blocked by the actual lagging hint"
        );
        reactor.complete_prior_process_recovery(&initial);
        reactor.complete_default_reconciliation(&initial);
        assert!(
            reactor.pending_work().is_none(),
            "completed default generation is idle"
        );
        if resident.is_none() {
            // The lifecycle can create/materialize the physical shard after
            // the selector returned false without another control-plane edit.
            resident = Some(open_source());
        }
        let resident = resident.unwrap();
        let receiver = Arc::new(Mutex::new(
            LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("users"),
                root.path().join("receiver"),
            )
            .unwrap(),
        ));
        Self {
            _root: root,
            data_dir,
            reactor,
            shard,
            target,
            resident,
            inputs,
            receiver,
        }
    }

    pub(super) fn prepare(&self) -> DfsPreparedClusterMetadataJournalPublish {
        let shard_id = DfsMetadataShardId::new(self.shard.shard_id.clone());
        let handle = self
            .resident
            .scope_shard(&self.shard.file_system_id, &shard_id)
            .unwrap();
        let mut lease = handle.checkout().unwrap();
        let mut plan = prepare_cluster_dfs_metadata_shard_publish_with_backend_and_authority(
            &self.data_dir,
            &self.shard.primary_node_id,
            vec![self.target.clone()],
            self.shard.file_system_id.clone(),
            shard_id,
            Some(self.shard.clone()),
            &mut lease,
            &DfsMaintenanceExecutionExclusions::default(),
            DfsMetadataJournalReplicationOptions::default(),
        )
        .unwrap()
        .unwrap();
        // The scheduling edge must not depend on an optional operator/status
        // observer or its cached snapshot being initialized.
        plan.resident_cursor_status_observer = None;
        plan
    }

    pub(super) fn transport(&self) -> AsyncDfsMetadataNodeTransport {
        let receiver = Arc::clone(&self.receiver);
        let context = crate::DfsMetadataReplicaShardContext {
            local_node_id: self.target.clone(),
            shard: self.shard.clone(),
            pending_membership_handoff: None,
        };
        let sender: AsyncDfsMetadataJournalBatchSender =
            Arc::new(move |target, command, _timeout| {
                let receiver = Arc::clone(&receiver);
                let context = context.clone();
                let command = command.clone();
                assert_eq!(target, &context.local_node_id);
                Box::pin(async move {
                    let response = process_dfs_metadata_journal_batch(
                        &mut *receiver.lock().unwrap(),
                        Some(&context),
                        command,
                    );
                    assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
                    Ok(response)
                })
            });
        let snapshot_sender: AsyncDfsMetadataSnapshotInstallSender = Arc::new(move |_, _, _| {
            Box::pin(async { panic!("the intact one-entry prefix must not require a snapshot") })
        });
        AsyncDfsMetadataNodeTransport::new(vec![self.target.clone()], sender, snapshot_sender)
    }

    pub(super) fn checkpoint(&self) -> u64 {
        metadata_cursor::metadata_replication_cursor(
            &local_dfs_metadata_shard_path(&self.data_dir, &DfsMetadataShardId::new("users")),
            &self.target,
            &DfsMetadataShardId::new("users"),
        )
        .unwrap()
    }
}

#[test]
fn verified_publisher_checkpoint_reawakens_blocked_primary_selection_without_kv_change() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut fixture = TransferFixture::new("dfs-primary-selection-publisher-wake");
        assert_eq!(fixture.checkpoint(), 0);
        let plan = fixture.prepare();
        assert_eq!(plan.targets.len(), 1);
        let report = execute_prepared_dfs_metadata_journal_publish_async(
            plan, &fixture.transport(), DfsMetadataJournalReplicationOptions::default(),
        ).await.unwrap();
        assert!(matches!(report.outcomes.as_slice(), [DfsMetadataJournalReplicationOutcome::Applied { next_after_sequence: 1, .. }]));
        assert_eq!(fixture.checkpoint(), 1);
        let floor = fixture.receiver.lock().unwrap().metadata_prefix_ack_floor(&DfsMetadataShardId::new("users")).unwrap().unwrap();
        assert_eq!(floor.journal_sequence, 1, "actual receiver synced the exact prefix before ACK");
        let retry = fixture.reactor.pending_work().expect(
            "successful background checkpoint must wake the already-completed default selection generation",
        );
        assert!(retry.reconcile_defaults);
        assert_eq!(retry.inputs, fixture.inputs, "no unrelated KV/input event supplied the wake");
        assert!(crate::default_cluster::metadata_shard_primary_transfer_ready_from_local_backend(
            &fixture.data_dir, &fixture.shard.primary_node_id, &fixture.shard, &fixture.target,
        ).unwrap(), "the retried selector now observes its exact target caught up");
        fixture.reactor.complete_default_reconciliation(&retry);
        put_local_dfs_metadata_replication_cursor(&fixture.data_dir, &fixture.target, &DfsMetadataShardId::new("users"), 1).unwrap();
        assert!(fixture.reactor.pending_work().is_none(), "repeated/no-wait ACK does not wake steady state");
    });
}

#[test]
fn observed_hint_discharges_selection_wait_when_retry_plans_no_changes() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut fixture = TransferFixture::new("dfs-primary-selection-visible-checkpoint");
        let _hook = install_metadata_cursor_checkpoint_test_hook(&fixture.data_dir, || {
            panic!("injected lost completion after visible cursor checkpoint");
        });
        let result = execute_prepared_dfs_metadata_journal_publish_async(
            fixture.prepare(),
            &fixture.transport(),
            DfsMetadataJournalReplicationOptions::default(),
        )
        .await;
        assert!(
            result.is_err(),
            "injected blocking-task failure must propagate"
        );
        assert_eq!(fixture.checkpoint(), 1);
        assert!(
            fixture.reactor.pending_work().is_none(),
            "failure occurred before notification"
        );
        let plan = fixture.prepare();
        assert!(plan.targets.is_empty());
        assert!(matches!(
            plan.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::NoChanges {
                after_sequence: 1,
                ..
            }]
        ));
        let work = fixture.reactor.pending_work().expect(
            "observing the already-published scheduling hint must not strand the waiting selector",
        );
        assert_eq!(work.inputs, fixture.inputs);
        fixture.reactor.complete_default_reconciliation(&work);
        let _ = fixture.prepare();
        assert!(
            fixture.reactor.pending_work().is_none(),
            "unchanged hint without dependency is inert"
        );
    });
}

#[test]
fn cancelled_publisher_after_hint_write_still_wakes_exact_transfer_selection() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut fixture = TransferFixture::new("dfs-transfer-checkpoint-cancel");
        assert_eq!(fixture.checkpoint(), 0);
        let plan = fixture.prepare();
        let transport = fixture.transport();
        let (entered, paused) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let _hook = install_metadata_cursor_checkpoint_test_hook(&fixture.data_dir, move || {
            entered.send(()).unwrap();
            released
                .recv_timeout(Duration::from_secs(10))
                .expect("test releases checkpoint worker");
        });
        let publish = tokio::spawn(async move {
            execute_prepared_dfs_metadata_journal_publish_async(
                plan,
                &transport,
                DfsMetadataJournalReplicationOptions::default(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), paused)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fixture.checkpoint(), 1);
        let shard_id = DfsMetadataShardId::new("users");
        assert_eq!(
            fixture
                .receiver
                .lock()
                .unwrap()
                .metadata_prefix_ack_floor(&shard_id)
                .unwrap()
                .unwrap()
                .journal_sequence,
            1,
            "real receiver synced the exact prefix before the validated sender checkpoint"
        );
        assert!(
            fixture.reactor.pending_work().is_none(),
            "notification is still paused after successful hint write"
        );
        let source_root = local_dfs_metadata_shard_path(&fixture.data_dir, &shard_id);
        // A nonblocking lock attempt proves the notification boundary owns no
        // journal writer flock; a regression cannot deadlock this assertion.
        let writer_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(metadata_cursor::journal_write_lock_path(&source_root))
            .unwrap();
        fs2::FileExt::try_lock_exclusive(&writer_file)
            .expect("writer lock released before notification");
        fs2::FileExt::unlock(&writer_file).unwrap();
        publish.abort();
        match publish.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("publisher waiter must be canceled while checkpoint worker is paused"),
        }
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), fixture.reactor.changed())
            .await
            .expect("blocking checkpoint must retain notification after async waiter cancellation");
        let work = fixture
            .reactor
            .pending_work()
            .expect("same KV inputs now have registered replica progress");
        assert_eq!(work.inputs, fixture.inputs);
        assert!(work.reconcile_defaults);
        fixture.reactor.complete_default_reconciliation(&work);
        assert!(fixture.reactor.pending_work().is_none());
        assert!(
            crate::default_cluster::metadata_shard_primary_transfer_ready_from_local_backend(
                &fixture.data_dir,
                &fixture.shard.primary_node_id,
                &fixture.shard,
                &fixture.target,
            )
            .unwrap()
        );
        put_local_dfs_metadata_replication_cursor_async(
            fixture.data_dir.clone(),
            fixture.target.clone(),
            shard_id,
            1,
            None,
        )
        .await
        .unwrap();
        assert!(
            fixture.reactor.pending_work().is_none(),
            "ordinary repeat checkpoint cannot become a polling wake"
        );
    });
}

#[test]
fn publisher_checkpoint_wakes_selection_blocked_before_local_shard_creation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut fixture = TransferFixture::with_seeded_backend("dfs-transfer-created-later", false);
        let report = execute_prepared_dfs_metadata_journal_publish_async(
            fixture.prepare(),
            &fixture.transport(),
            DfsMetadataJournalReplicationOptions::default(),
        )
        .await
        .unwrap();
        assert!(matches!(
            report.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::Applied {
                next_after_sequence: 1,
                ..
            }]
        ));
        let work = fixture
            .reactor
            .pending_work()
            .expect("later backend publication wakes the selector without a KV edit");
        assert_eq!(work.inputs, fixture.inputs);
        assert!(
            crate::default_cluster::metadata_shard_primary_transfer_ready_from_local_backend(
                &fixture.data_dir,
                &fixture.shard.primary_node_id,
                &fixture.shard,
                &fixture.target,
            )
            .unwrap()
        );
    });
}
