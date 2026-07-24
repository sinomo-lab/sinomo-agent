use crate::internal::*;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

fn node_id(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).expect("node id")
}

fn temp_root(prefix: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(prefix)
}

fn put_command(key: &str, value: &[u8]) -> KvCommand {
    KvCommand::Op(KvOp::Put {
        key: key.to_string(),
        value: value.to_vec(),
        lease_id: None,
    })
}

fn put_event(revision: u64, key: &str, value: &[u8]) -> KvEvent {
    KvEvent {
        revision: KvRevision(revision),
        key: key.to_string(),
        kind: KvEventKind::Put {
            value: value.to_vec(),
        },
    }
}

fn entry_value<'a>(store: &'a InProcessKvStore, key: &str) -> &'a [u8] {
    store
        .get(key)
        .unwrap_or_else(|| panic!("missing key {key}"))
        .value
        .as_slice()
}

fn append_raw_wal_tail(path: &Path, bytes: &[u8]) {
    let mut wal = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open wal for damage");
    wal.write_all(bytes).expect("append damaged wal tail");
    wal.sync_data().expect("sync damaged wal tail");
}

#[derive(Clone, Copy)]
enum WalTailDamage {
    PartialRecord,
    CorruptRecord,
}

impl WalTailDamage {
    fn suffix(self) -> &'static str {
        match self {
            Self::PartialRecord => "partial",
            Self::CorruptRecord => "corrupt",
        }
    }
}

#[test]
fn wal_tail_partial_and_corrupt_records_recover_safe_prefix() {
    for damage in [WalTailDamage::PartialRecord, WalTailDamage::CorruptRecord] {
        let prefix = format!("sinomo-kv-power-loss-wal-{}", damage.suffix());
        let root = temp_root(&prefix);
        let paths = KvWalPaths::new(root.join("raft"));
        let local = node_id(&format!("kv_power_loss_wal_{}", damage.suffix()));

        {
            let mut node =
                DurableRaftKvNode::open(local.clone(), paths.clone()).expect("open node");
            for (key, value) in [
                ("/power-loss/wal/one", b"one".as_slice()),
                ("/power-loss/wal/two", b"two".as_slice()),
            ] {
                let entry = node
                    .append_local_command(1, put_command(key, value))
                    .expect("append safe entry");
                node.commit_to(entry.index).expect("commit safe entry");
            }
        }

        match damage {
            WalTailDamage::PartialRecord => {
                let lost = KvLogEntry {
                    index: 3,
                    term: 1,
                    request_id: KvRequestId::generate(),
                    command: put_command("/power-loss/wal/lost-partial", b"lost"),
                    revision: KvRevision::ZERO,
                };
                let encoded = encode_wal_record(&lost, &paths.wal).expect("encode lost entry");
                append_raw_wal_tail(&paths.wal, &encoded[..encoded.len() / 2]);
            }
            WalTailDamage::CorruptRecord => {
                let marker = b"not-a-sinomo-kv-wal-record";
                let mut corrupt = Vec::new();
                corrupt.extend_from_slice(&(marker.len() as u32).to_be_bytes());
                corrupt.extend_from_slice(marker);
                append_raw_wal_tail(&paths.wal, &corrupt);
            }
        }

        let recovered =
            DurableRaftKvNode::open(local, paths.clone()).expect("recover damaged wal tail");
        assert_eq!(recovered.last_log_index(), 2);
        assert_eq!(
            entry_value(recovered.store(), "/power-loss/wal/one"),
            b"one"
        );
        assert_eq!(
            entry_value(recovered.store(), "/power-loss/wal/two"),
            b"two"
        );
        assert!(
            recovered
                .store()
                .get("/power-loss/wal/lost-partial")
                .is_none(),
            "partial tail must not be applied"
        );
        drop(recovered);

        let repaired = read_wal_entries(&paths.wal, None, 0).expect("read repaired wal");
        assert_eq!(
            repaired
                .entries
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(!repaired.truncated);
    }
}

#[test]
fn hard_state_commit_index_ahead_of_wal_is_repaired_on_reopen() {
    let root = temp_root("sinomo-kv-power-loss-hard-state-ahead");
    let paths = KvWalPaths::new(root.join("raft"));
    let local = node_id("kv_power_loss_hard_state_ahead");
    let committed_index;

    {
        let mut node = DurableRaftKvNode::open(local.clone(), paths.clone()).expect("open node");
        let entry = node
            .append_local_command(1, put_command("/power-loss/hard-state/kept", b"kept"))
            .expect("append committed entry");
        node.commit_to(entry.index).expect("commit entry");
        committed_index = entry.index;
    }

    let mut hard_state = paths.read_persisted_hard_state().expect("read hard state");
    hard_state.commit_index = committed_index + 4;
    paths
        .write_persisted_hard_state(&hard_state)
        .expect("write ahead hard state");

    let recovered = DurableRaftKvNode::open(local, paths.clone()).expect("repair hard state");
    assert_eq!(recovered.hard_state().commit_index, committed_index);
    assert_eq!(
        entry_value(recovered.store(), "/power-loss/hard-state/kept"),
        b"kept"
    );
    drop(recovered);

    assert_eq!(
        paths
            .read_persisted_hard_state()
            .expect("read repaired hard state")
            .commit_index,
        committed_index
    );
}

#[test]
fn snapshot_crash_before_wal_rewrite_keeps_replacement_entries() {
    let root = temp_root("sinomo-kv-power-loss-snapshot-before-wal-rewrite");
    let paths = KvWalPaths::new(root.join("raft"));
    let follower_id = node_id("kv_power_loss_snapshot_rewrite_follower");
    let replacement_leader_id = node_id("kv_power_loss_snapshot_rewrite_leader");
    let replacements = vec![
        KvLogEntry {
            index: 5,
            term: 3,
            request_id: KvRequestId::generate(),
            command: put_command("/power-loss/snapshot/5", b"replacement-5"),
            revision: KvRevision::ZERO,
        },
        KvLogEntry {
            index: 6,
            term: 3,
            request_id: KvRequestId::generate(),
            command: put_command("/power-loss/snapshot/6", b"replacement-6"),
            revision: KvRevision::ZERO,
        },
        KvLogEntry {
            index: 7,
            term: 3,
            request_id: KvRequestId::generate(),
            command: put_command("/power-loss/snapshot/7", b"replacement-7"),
            revision: KvRevision::ZERO,
        },
    ];

    let wal_before_snapshot_rewrite = {
        let mut node =
            DurableRaftKvNode::open(follower_id.clone(), paths.clone()).expect("open follower");
        for index in 1..=6 {
            let entry = node
                .append_local_command(
                    2,
                    put_command(
                        &format!("/power-loss/snapshot/{index}"),
                        format!("stale-{index}").as_bytes(),
                    ),
                )
                .expect("append old-term entry");
            if index <= 4 {
                node.commit_to(entry.index).expect("commit common prefix");
            }
        }

        let response = node
            .append_entries(RaftAppendEntriesRequest {
                term: 3,
                leader_id: replacement_leader_id,
                prev_log_index: 4,
                prev_log_term: 2,
                entries: replacements.clone(),
                leader_commit: 7,
            })
            .expect("replace old-term suffix");
        assert!(response.success);
        assert_eq!(node.applied_index(), 7);
        assert_eq!(
            entry_value(node.store(), "/power-loss/snapshot/5"),
            b"replacement-5"
        );
        assert_eq!(
            entry_value(node.store(), "/power-loss/snapshot/6"),
            b"replacement-6"
        );

        // Conflict replacement is append-only until snapshot compaction rewrites the
        // WAL, so the durable file currently contains A5,A6 followed by B5,B6,B7.
        let wal = fs::read(&paths.wal).expect("read pre-compaction wal");
        let before = read_wal_entries(&paths.wal, None, 0).expect("read wal records");
        assert_eq!(
            before
                .entries
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 5, 6, 7]
        );

        let snapshot = node.save_snapshot().expect("persist snapshot");
        assert_eq!(snapshot.last_included_index, 7);
        wal
    };

    // Model power loss after the snapshot replacement is durable but before the
    // second, independent WAL replacement lands.
    {
        let mut wal = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&paths.wal)
            .expect("restore pre-rewrite wal");
        wal.write_all(&wal_before_snapshot_rewrite)
            .expect("restore append-only wal");
        wal.sync_all().expect("sync restored wal");
    }

    let recovered =
        DurableRaftKvNode::open(follower_id.clone(), paths.clone()).expect("recover follower");
    assert_eq!(recovered.applied_index(), 7);
    for replacement in &replacements {
        let loaded = recovered
            .log_entry_at(replacement.index)
            .expect("replacement retained for replication");
        assert_eq!(loaded.term, replacement.term);
        assert_eq!(loaded.request_id, replacement.request_id);
        assert_eq!(loaded.command, replacement.command);
    }
    assert_eq!(
        entry_value(recovered.store(), "/power-loss/snapshot/5"),
        b"replacement-5"
    );
    assert_eq!(
        entry_value(recovered.store(), "/power-loss/snapshot/6"),
        b"replacement-6"
    );
    drop(recovered);

    let repaired = read_wal_entries(&paths.wal, None, 0).expect("read repaired wal");
    assert_eq!(
        repaired
            .entries
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7]
    );
    for replacement in replacements {
        let persisted = repaired
            .entries
            .iter()
            .find(|entry| entry.index == replacement.index)
            .expect("replacement persisted in repaired wal");
        assert_eq!(persisted.term, replacement.term);
        assert_eq!(persisted.request_id, replacement.request_id);
        assert_eq!(persisted.command, replacement.command);
    }
    assert!(!repaired.truncated);

    let reopened = DurableRaftKvNode::open(follower_id, paths).expect("reopen repaired follower");
    assert_eq!(reopened.last_log_index(), 7);
    assert_eq!(reopened.term_at(5), Some(3));
    assert_eq!(reopened.term_at(6), Some(3));
}

#[test]
fn snapshot_wal_compaction_ignores_leftover_artifacts_after_reopen() {
    let root = temp_root("sinomo-kv-power-loss-compaction-artifacts");
    let paths = KvWalPaths::new(root.join("raft"));
    let local = node_id("kv_power_loss_compaction_artifacts");
    let command_count = RAFT_COMPACTION_RETAINED_ENTRIES + 3;

    {
        let mut node = DurableRaftKvNode::open(local.clone(), paths.clone()).expect("open node");
        for index in 1..=command_count {
            let entry = node
                .append_local_command(
                    1,
                    put_command(
                        &format!("/power-loss/compact/{index:03}"),
                        format!("value-{index}").as_bytes(),
                    ),
                )
                .expect("append compacted entry");
            node.commit_to(entry.index).expect("commit compacted entry");
        }
        let snapshot = node.save_snapshot().expect("save compacted snapshot");
        assert_eq!(snapshot.last_included_index, command_count);
    }

    let compacted = read_wal_entries(&paths.wal, None, 0).expect("read compacted wal");
    assert!(
        compacted.entries.len() <= RAFT_COMPACTION_RETAINED_ENTRIES as usize,
        "snapshot compaction should retain only the bounded WAL tail"
    );

    let leftover_wal = paths.root.join(".wal.sobjlog.tmp-power-loss");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&leftover_wal)
            .expect("create leftover wal temp");
        write_wal_record(
            &mut file,
            &KvLogEntry {
                index: command_count + 1,
                term: 1,
                request_id: KvRequestId::generate(),
                command: put_command("/power-loss/compact/temp-only", b"ignored"),
                revision: KvRevision::ZERO,
            },
            &leftover_wal,
        )
        .expect("write leftover wal temp");
        file.sync_all().expect("sync leftover wal temp");
    }

    let mut stale_store = InProcessKvStore::new();
    stale_store
        .put("/power-loss/compact/temp-only", b"ignored".to_vec(), None)
        .expect("build stale snapshot");
    write_object_file(
        &paths.root.join(".snapshot.sobj.tmp-power-loss"),
        &RaftSnapshot {
            last_included_index: command_count + 99,
            last_included_term: 9,
            kv: stale_store.snapshot().expect("snapshot"),
        },
    )
    .expect("write leftover snapshot temp");

    let recovered =
        DurableRaftKvNode::open(local, paths.clone()).expect("reopen after compaction artifacts");
    assert_eq!(recovered.snapshot_index(), command_count);
    assert_eq!(
        entry_value(
            recovered.store(),
            &format!("/power-loss/compact/{command_count:03}")
        ),
        format!("value-{command_count}").as_bytes()
    );
    assert!(
        recovered
            .store()
            .get("/power-loss/compact/temp-only")
            .is_none(),
        "leftover compaction artifacts must not be replayed"
    );
}

#[test]
fn materialized_snapshot_paths_ignore_stale_cache_and_fail_closed_on_corrupt_raft() {
    let root = temp_root("sinomo-kv-power-loss-materialized-fallback");
    let paths = ClusterKvPaths::new(root.join("cluster"));
    let local = node_id("kv_power_loss_materialized_fallback");
    let cache = paths.local_cache();

    {
        let mut node = cache.open_raft_node(local.clone()).expect("open raft node");
        let entry = node
            .append_local_command(1, put_command("/power-loss/materialized/live", b"live"))
            .expect("append live raft entry");
        node.commit_to(entry.index).expect("commit live raft entry");
    }

    let mut stale_store = InProcessKvStore::new();
    stale_store
        .put("/power-loss/materialized/stale", b"stale".to_vec(), None)
        .expect("build stale materialized snapshot");
    write_cluster_kv_snapshot(&paths, &stale_store.snapshot().expect("snapshot"))
        .expect("write stale materialized snapshot");

    let live = read_cluster_kv_current_snapshot_for_node(&paths, local.clone())
        .expect("read live raft over stale materialized snapshot");
    assert_eq!(
        live.get("/power-loss/materialized/live")
            .expect("live key")
            .value,
        b"live"
    );
    assert!(live.get("/power-loss/materialized/stale").is_none());

    fs::write(&paths.snapshot, b"corrupt materialized snapshot")
        .expect("corrupt materialized snapshot file");
    let live = read_cluster_kv_current_snapshot_for_node(&paths, local.clone())
        .expect("read live raft over corrupt materialized snapshot");
    assert_eq!(
        live.get("/power-loss/materialized/live")
            .expect("live key after corrupt materialized")
            .value,
        b"live"
    );
    assert!(live.get("/power-loss/materialized/stale").is_none());

    write_cluster_kv_snapshot(&paths, &live).expect("write good materialized fallback");
    let raft_paths = paths.raft_wal_paths(&local);
    fs::write(&raft_paths.snapshot, b"corrupt raft snapshot")
        .expect("corrupt durable raft snapshot file");

    let error = cache
        .open_raft_node(local)
        .expect_err("an unanchored materialized snapshot must not recover corrupt Raft state");
    assert!(
        error.to_string().contains("invalid magic"),
        "corrupt Raft error should be preserved, got {error}"
    );
    assert_eq!(
        fs::read(&raft_paths.snapshot).expect("read preserved corrupt Raft snapshot"),
        b"corrupt raft snapshot"
    );
    let recovered = InProcessKvStore::restore(
        cache
            .cached_snapshot()
            .expect("materialized read cache remains available"),
    )
    .expect("restore materialized read cache");
    assert_eq!(
        entry_value(&recovered, "/power-loss/materialized/live"),
        b"live"
    );
    assert!(recovered.get("/power-loss/materialized/stale").is_none());
}

#[test]
fn watch_event_log_corrupt_tail_recovers_safe_prefix_on_next_append() {
    let root = temp_root("sinomo-kv-power-loss-watch-log");
    let paths = ClusterKvPaths::new(root.join("cluster"));
    fs::create_dir_all(&paths.root).expect("create cluster root");

    let first = put_event(1, "/power-loss/watch/a", b"a");
    let second = put_event(2, "/power-loss/watch/b", b"b");
    append_cluster_kv_watch_events_to_storage(&paths, &[first.clone(), second.clone()], None)
        .expect("append safe watch events");

    let marker = b"not-a-watch-event-record";
    let mut log = OpenOptions::new()
        .append(true)
        .open(&paths.watch_events_log)
        .expect("open watch log");
    log.write_all(&(marker.len() as u32).to_be_bytes())
        .expect("write corrupt watch record length");
    log.write_all(marker).expect("write corrupt watch record");
    log.sync_data().expect("sync corrupt watch record");
    drop(log);

    let damaged = read_cluster_kv_watch_events(&paths).expect("read damaged watch log");
    assert_eq!(damaged.events, vec![first.clone(), second.clone()]);
    assert!(!damaged.append_safe);

    let third = put_event(3, "/power-loss/watch/c", b"c");
    let repair =
        append_cluster_kv_watch_events_to_storage(&paths, std::slice::from_ref(&third), None)
            .expect("append after damaged watch log");
    assert!(repair.append_safe);

    let recovered = read_cluster_kv_watch_events(&paths).expect("read repaired watch log");
    assert_eq!(recovered.events, vec![first, second, third]);
    assert!(recovered.append_safe);
    assert!(
        !fs::read(&paths.watch_events_log)
            .expect("read repaired watch log bytes")
            .windows(marker.len())
            .any(|window| window == marker),
        "corrupt watch-event tail should be removed during repair"
    );
}

#[test]
fn hard_state_commit_replay_applies_txn_with_lease_atomically_after_reopen() {
    let root = temp_root("sinomo-kv-power-loss-txn-lease");
    let paths = KvWalPaths::new(root.join("raft"));
    let local = node_id("kv_power_loss_txn_lease");
    let lease_id = LeaseId(7001);
    let key = "/power-loss/txn-lease/key";
    let entry_index;

    {
        let mut node = DurableRaftKvNode::open(local.clone(), paths.clone()).expect("open node");
        let entry = node
            .append_local_command(
                7,
                KvCommand::Txn(KvTxn {
                    compares: vec![KvCompare::Missing {
                        key: key.to_string(),
                    }],
                    success: vec![
                        KvOp::LeaseGrant {
                            lease_id,
                            ttl_ticks: 30,
                        },
                        KvOp::Put {
                            key: key.to_string(),
                            value: b"leased".to_vec(),
                            lease_id: Some(lease_id),
                        },
                    ],
                    failure: Vec::new(),
                }),
            )
            .expect("append uncommitted txn");
        entry_index = entry.index;
        assert!(
            node.store().get(key).is_none(),
            "appended but uncommitted txn should not affect the store before replay"
        );
    }

    let mut hard_state = paths
        .read_persisted_hard_state()
        .expect("read hard state before emulated crash");
    hard_state.commit_index = entry_index;
    paths
        .write_persisted_hard_state(&hard_state)
        .expect("persist commit index without in-memory apply");

    let recovered = DurableRaftKvNode::open(local, paths).expect("replay committed txn");
    assert_eq!(recovered.hard_state().commit_index, entry_index);
    let entry = recovered.store().get(key).expect("leased key after replay");
    assert_eq!(entry.value, b"leased");
    assert_eq!(entry.lease_id, Some(lease_id));

    let snapshot = recovered.store().snapshot().expect("snapshot");
    let lease = snapshot
        .leases
        .iter()
        .find(|lease| lease.lease_id == lease_id)
        .expect("lease after replay");
    assert_eq!(lease.ttl_ticks, 30);
    assert_eq!(lease.remaining_ticks, 30);

    let response = recovered
        .txn_response_at(entry_index)
        .expect("txn response after replay");
    assert!(response.succeeded);
    assert_eq!(response.revision, KvRevision(1));
    assert_eq!(response.responses.len(), 2);
}
