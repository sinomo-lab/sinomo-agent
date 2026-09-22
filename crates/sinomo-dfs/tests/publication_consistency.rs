//! Two service lanes share durable metadata under one unchanged primary. Each
//! gate pauses an operation after its input was read and lets a competing write
//! commit before the paused operation resumes. Fresh reads and restart must
//! preserve that committed winner.
use std::time::Duration;

#[derive(Clone, Copy, PartialEq)]
enum Race {
    LateReceipt,
    ReadRepair,
    MetadataChange,
}

fn run_case(case: Race) {
    if matches!(case, Race::ReadRepair | Race::MetadataChange) {
        let metadata_race = case == Race::MetadataChange;
        use sinomo_core::kv::{
            InProcessKvStore, KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse,
        };
        use sinomo_core::network::InProcessNetwork;
        use sinomo_core::node::NodeId;
        use sinomo_dfs::*;
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        };
        #[derive(Debug)]
        struct GateKv {
            inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
            armed: Arc<AtomicBool>,
            gate_config: bool,
            entered: std::sync::mpsc::Sender<()>,
            resume: Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl DfsControlPlaneKvClient for GateKv {
            fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
                if self.gate_config
                    && key.ends_with("/config")
                    && self.armed.swap(false, Ordering::SeqCst)
                {
                    self.entered.send(()).unwrap();
                    self.resume
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(30))
                        .unwrap();
                }
                self.inner.get(key)
            }
            fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
                if !self.gate_config
                    && prefix.contains("/metadata-shards/")
                    && self.armed.swap(false, Ordering::SeqCst)
                {
                    self.entered.send(()).unwrap();
                    self.resume
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(30))
                        .unwrap();
                }
                self.inner.range_prefix(prefix)
            }
            fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
                self.inner.put(key, value)
            }
            fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
                self.inner.txn(txn)
            }
        }
        let test_dir = sinomo_testdir::TestDir::new("dfs-publication-consistency");
        let root = test_dir.path().to_path_buf();
        std::fs::create_dir_all(&root).unwrap();
        let owner = NodeId::from_legacy_named("node_prune_primary").unwrap();
        let remote = NodeId::from_legacy_named("node_prune_missing_replica").unwrap();
        let control = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&DfsMetadataShardConfig {
                    file_system_id: sifs_file_system_id(),
                    shard_id: "root".into(),
                    primary_node_id: owner.clone(),
                    replica_node_ids: vec![],
                    epoch: 1,
                    assignment: DfsMetadataShardAssignment::Root,
                    namespace_prepare_codec: Default::default(),
                    default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                })
                .unwrap()
        });
        let net_b = InProcessNetwork::new();
        let mut writer = NetworkedDfsService::new(
            net_b.add_node(owner.clone()),
            LocalContentStore::new(root.join("content-primary")).unwrap(),
        )
        .with_control_plane(control.clone())
        .with_metadata_backend(
            open_root_local_metadata_backend(root.join("metadata-primary")).unwrap(),
        )
        .unwrap();
        writer.bootstrap_distributed_sifs().unwrap();
        let options = DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![owner.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        writer
            .posix_write_file("/race.txt", b"base", options.clone())
            .unwrap();
        let base = writer.posix_stat("/race.txt").unwrap();
        {
            let mut backend =
                open_root_local_metadata_backend(root.join("metadata-primary")).unwrap();
            backend
                .add_object_location_if_current(
                    &base.object_id,
                    &base.version,
                    base.content_hash.as_deref().unwrap(),
                    &remote,
                )
                .unwrap();
        }
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let armed = Arc::new(AtomicBool::new(false));
        let net_a = InProcessNetwork::new();
        let mut reader = NetworkedDfsService::new(
            net_a.add_node(owner.clone()),
            LocalContentStore::new(root.join("content-primary")).unwrap(),
        )
        .with_control_plane(GateKv {
            inner: control.clone(),
            armed: armed.clone(),
            gate_config: metadata_race,
            entered: entered_tx,
            resume: Mutex::new(resume_rx),
        })
        .with_metadata_backend(
            open_root_local_metadata_backend(root.join("metadata-primary")).unwrap(),
        )
        .unwrap();
        let mut missing = NetworkedDfsService::new(
            net_a.add_node(remote.clone()),
            LocalContentStore::new(root.join("content-missing-replica")).unwrap(),
        )
        .with_control_plane(control.clone());
        missing
            .add_file_system(DfsFileSystem::sifs(owner.clone()))
            .unwrap();
        let source_events = if metadata_race {
            vec![]
        } else {
            let targets = reader
                .request_remote_object_read_without_cache(&base.object_id)
                .unwrap();
            assert!(targets.contains(&remote));
            missing.process_incoming().unwrap()
        };
        armed.store(true, Ordering::SeqCst);
        let read_reply = std::thread::spawn(move || {
            let result = if metadata_race {
                reader
                    .posix_set_mode("/race.txt", 0o600)
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            } else {
                reader
                    .process_incoming()
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            };
            (reader, result)
        });
        entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        writer
            .posix_write_file("/race.txt", b"winner", options.clone())
            .unwrap();
        let expected = writer.posix_stat("/race.txt").unwrap().version;
        let read_before = writer.posix_read_file_distributed("/race.txt").unwrap();
        assert_eq!(read_before, b"winner");
        resume_tx.send(()).unwrap();
        let (mut reader, read_events) = read_reply.join().unwrap();
        if metadata_race {
            assert!(read_events.is_err(), "stale chmod must be rejected");
        }
        let after = reader
            .metadata_backend()
            .unwrap()
            .get_object(&base.object_id)
            .unwrap()
            .unwrap();
        let read = reader.posix_read_file_distributed("/race.txt");
        let evidence = serde_json::json!({"root":root,"metadata_race":metadata_race,"mode_after":after.mode,"expected":expected,"original":base.version,"after":after.version,"before_prune":"winner","read":read.map(|b|String::from_utf8_lossy(&b).into_owned()).map_err(|e|e.to_string()),"source_events":format!("{source_events:?}"),"read_events":read_events});
        println!("{}", serde_json::to_string_pretty(&evidence).unwrap());
        std::fs::write(
            root.join("evidence.json"),
            serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();
        if metadata_race {
            assert_eq!(
                evidence["read"]["Ok"],
                serde_json::json!("winner"),
                "a metadata attribute update must not overwrite a concurrent committed payload"
            );
        } else {
            assert_eq!(
                after.version, expected,
                "pruning a missing replica must not roll back a concurrent committed write"
            );
        }
        drop(reader);
        drop(writer);
        let mut restarted = NetworkedDfsService::new(
            InProcessNetwork::new().add_node(owner),
            LocalContentStore::new(root.join("content-primary")).unwrap(),
        )
        .with_control_plane(control)
        .with_metadata_backend(
            open_root_local_metadata_backend(root.join("metadata-primary")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            restarted.posix_read_file_distributed("/race.txt").unwrap(),
            b"winner"
        );
        assert_eq!(restarted.posix_stat("/race.txt").unwrap().version, expected);
        restarted.posix_set_mode("/race.txt", 0o600).unwrap();
        assert_eq!(
            restarted.posix_read_file_distributed("/race.txt").unwrap(),
            b"winner"
        );
        restarted
            .posix_write_file("/race.txt", b"next", options)
            .unwrap();
        assert_eq!(
            restarted.posix_read_file_distributed("/race.txt").unwrap(),
            b"next"
        );
        return;
    }
    if case == Race::LateReceipt {
        use sinomo_core::DfsWriteRequest;
        use sinomo_core::network::InProcessNetwork;
        use sinomo_core::node::NodeId;
        use sinomo_dfs::*;
        use std::io::{Cursor, Read};
        struct GatedReader {
            body: Cursor<Vec<u8>>,
            entered: Option<std::sync::mpsc::Sender<()>>,
            resume: std::sync::mpsc::Receiver<()>,
        }
        impl Read for GatedReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if let Some(entered) = self.entered.take() {
                    entered.send(()).unwrap();
                    self.resume.recv_timeout(Duration::from_secs(30)).unwrap();
                }
                self.body.read(buf)
            }
        }
        let test_dir = sinomo_testdir::TestDir::new("dfs-publication-consistency");
        let root = test_dir.path().to_path_buf();
        std::fs::create_dir_all(&root).unwrap();
        let owner = NodeId::from_legacy_named("node_consistency_primary").unwrap();
        let replica = NodeId::from_legacy_named("node_consistency_content_replica").unwrap();
        let control = SharedDfsControlPlaneKvClient::new(sinomo_core::kv::InProcessKvStore::new());
        control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&DfsMetadataShardConfig {
                    file_system_id: sifs_file_system_id(),
                    shard_id: "root".into(),
                    primary_node_id: owner.clone(),
                    replica_node_ids: vec![],
                    epoch: 1,
                    assignment: DfsMetadataShardAssignment::Root,
                    namespace_prepare_codec: Default::default(),
                    default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                })
                .unwrap()
        });
        let network_a = InProcessNetwork::new();
        let network_b = InProcessNetwork::new();
        let mut a = NetworkedDfsService::new(
            network_a.add_node(owner.clone()),
            LocalContentStore::new(root.join("content-primary")).unwrap(),
        )
        .with_control_plane(control.clone())
        .with_metadata_backend(
            open_root_local_metadata_backend(root.join("metadata-primary")).unwrap(),
        )
        .unwrap();
        a.bootstrap_distributed_sifs().unwrap();
        a.posix_write_file(
            "/race.txt",
            b"base",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![owner.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
        let base = a.posix_stat("/race.txt").unwrap();
        let mut b = NetworkedDfsService::new(
            network_b.add_node(owner.clone()),
            LocalContentStore::new(root.join("content-primary")).unwrap(),
        )
        .with_control_plane(control.clone())
        .with_metadata_backend(
            open_root_local_metadata_backend(root.join("metadata-primary")).unwrap(),
        )
        .unwrap();
        let mut receiver = NetworkedDfsService::new(
            network_a.add_node(replica.clone()),
            LocalContentStore::new(root.join("content-replica")).unwrap(),
        )
        .with_control_plane(control.clone());
        receiver
            .add_file_system(DfsFileSystem::sifs(owner.clone()))
            .unwrap();
        let delayed_version = DfsVersion::generate();
        let winning_version = DfsVersion::generate();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let request_b = DfsWriteRequest {
            file_system_id: sifs_file_system_id(),
            object_id: base.object_id.clone(),
            target_version: winning_version.clone(),
            requesting_node_id: owner.clone(),
        };
        let base_b = base.version.clone();
        let owner_b = owner.clone();
        let winner = std::thread::spawn(move || {
            b.write_distributed_eventual_from_reader_with_base(
                request_b,
                Some(base_b),
                GatedReader {
                    body: Cursor::new(b"winner".to_vec()),
                    entered: Some(entered_tx),
                    resume: resume_rx,
                },
                vec![owner_b],
                DfsReplicationPolicy::new(1, 1).unwrap(),
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let pending = a
            .write_distributed_eventual_from_reader_with_base(
                DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id: base.object_id.clone(),
                    target_version: delayed_version.clone(),
                    requesting_node_id: owner.clone(),
                },
                Some(base.version),
                Cursor::new(b"delayed stale data".to_vec()),
                vec![owner.clone(), replica],
                DfsReplicationPolicy::new(2, 2).unwrap(),
            )
            .unwrap();
        assert!(
            matches!(pending, NetworkedDfsWriteOutcome::Pending { .. }),
            "delayed writer must await content receipt: {pending:?}"
        );
        let NetworkedDfsWriteOutcome::Pending {
            intent_id: delayed_intent_id,
            ..
        } = pending
        else {
            unreachable!()
        };
        let pending_record = a
            .metadata_backend()
            .unwrap()
            .get_intent(&base.object_id, &delayed_intent_id)
            .unwrap()
            .unwrap();
        resume_tx.send(()).unwrap();
        let committed = winner.join().unwrap().unwrap();
        assert!(
            matches!(committed, NetworkedDfsWriteOutcome::Committed { .. }),
            "winner must have completed: {committed:?}"
        );
        let NetworkedDfsWriteOutcome::Committed {
            intent_id: winning_intent_id,
            ..
        } = committed
        else {
            unreachable!()
        };
        let before = a
            .metadata_backend()
            .unwrap()
            .get_object(&base.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(before.version, winning_version);
        let read_before = a.posix_read_file_distributed("/race.txt").unwrap();
        assert_eq!(read_before, b"winner");
        let replica_events = receiver.process_incoming();
        let writer_events = a.process_incoming();
        let after = a
            .metadata_backend()
            .unwrap()
            .get_object(&base.object_id)
            .unwrap()
            .unwrap();
        let read = a.posix_read_file_distributed("/race.txt");
        let evidence = serde_json::json!({"root":root,"before":before.version,"expected":winning_version,"stale_target":delayed_version,"after":after.version,"read_before_receipt":String::from_utf8_lossy(&read_before),"replica_events":format!("{replica_events:?}"),"writer_events":format!("{writer_events:?}"),"read":read.map(|b|String::from_utf8_lossy(&b).into_owned()).map_err(|e|e.to_string())});
        println!("{}", serde_json::to_string_pretty(&evidence).unwrap());
        std::fs::write(
            root.join("evidence.json"),
            serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();
        assert_eq!(
            after.version, winning_version,
            "a delayed receipt must not rebase and publish an old-base write over an already committed winner"
        );
        assert_eq!(evidence["read"]["Ok"], serde_json::json!("winner"));
        let terminal = a
            .metadata_backend()
            .unwrap()
            .get_intent(&base.object_id, &delayed_intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, DfsWriteIntentStatus::Aborted);
        // A duplicate delayed prepare cannot reopen the rejected old-base write.
        let mut backend = open_root_local_metadata_backend(root.join("metadata-primary")).unwrap();
        assert!(backend.put_intent(&pending_record).is_err());
        assert_eq!(
            backend
                .get_intent(&base.object_id, &delayed_intent_id)
                .unwrap()
                .unwrap(),
            terminal
        );
        let committed_record = backend
            .get_intent(&base.object_id, &winning_intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(committed_record.status, DfsWriteIntentStatus::Committed);
        let mut duplicated_prepare = committed_record.clone();
        duplicated_prepare.status = DfsWriteIntentStatus::ReceiptsPending;
        backend.put_intent(&duplicated_prepare).unwrap();
        assert_eq!(
            backend
                .get_intent(&base.object_id, &winning_intent_id)
                .unwrap()
                .unwrap(),
            committed_record
        );
        drop(backend);
        drop(a);
        let mut restarted = NetworkedDfsService::new(
            InProcessNetwork::new().add_node(owner.clone()),
            LocalContentStore::new(root.join("content-primary")).unwrap(),
        )
        .with_control_plane(control)
        .with_metadata_backend(
            open_root_local_metadata_backend(root.join("metadata-primary")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            restarted.posix_read_file_distributed("/race.txt").unwrap(),
            b"winner"
        );
        restarted
            .posix_write_file(
                "/race.txt",
                b"next",
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![owner],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        assert_eq!(
            restarted.posix_read_file_distributed("/race.txt").unwrap(),
            b"next"
        );
        return;
    }
}

#[test]
fn delayed_receipt_keeps_original_base_precondition() {
    run_case(Race::LateReceipt);
}

#[test]
fn read_repair_cannot_revert_a_concurrent_commit() {
    run_case(Race::ReadRepair);
}

#[test]
fn metadata_change_cannot_replace_concurrently_committed_contents() {
    run_case(Race::MetadataChange);
}
