use sinomo_core::DfsObjectPolicy;
use sinomo_dfs::{
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, DfsMetadataBackend, DfsMetadataMutation,
    DfsMetadataShardId, DfsObjectKind, DfsReplicationPolicy, DfsVersion, LocalSobjMetadataBackend,
    NetworkedDfsReadFreshness, NetworkedDfsReadOutcome, sifs_node_files_path,
};
use std::io::{Read, Result as IoResult};

const TEST_LARGE_DFS_BYTES: usize = 1024 * 1024;

#[test]
fn native_file_sync_repairs_full_committed_w1_n3_owner_after_healing_receivers() {
    use sinomo_dfs::test_support::{
        DfsTestFaultAction, DfsTestFaultRule, DfsTestRpcOrigin, DfsTestRpcPhase, DfsTestRuntime,
    };
    let (runtime, nodes) = DfsTestRuntime::start_cluster("native-compact-outcome-file-sync", 3)
        .expect("normal three-node startup");
    let shard = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let primary = shard.primary_node_id;
    let requester = nodes.iter().find(|node| **node != primary).unwrap();
    let path = "/services/native-compact-outcome-file-sync.bin";
    let mut policy =
        sinomo_dfs::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 3).unwrap();
    policy.required_node_ids = nodes.clone();
    runtime
        .apply_placement_policy(
            &primary,
            path,
            sinomo_dfs::DfsPlacementScope::Subtree,
            policy,
        )
        .unwrap();
    let mut writer = runtime.client(&primary).unwrap();
    for route in [
        sinomo_dfs::DFS_DATA_STAGE_BLOB_ROUTE,
        sinomo_dfs::DFS_DATA_REPAIR_REPLICA_ROUTE,
    ] {
        runtime.install_fault(DfsTestFaultRule {
            source: Some(primary.clone()),
            target: None,
            route: Some(route.into()),
            call_id: None,
            origin: Some(DfsTestRpcOrigin::RegisteredHost),
            remaining_hits: 32,
            action: DfsTestFaultAction::BudgetExhausted,
        });
    }
    let bytes = b"visible W1 still owes the exact remaining N copies";
    let stat = writer
        .posix_write_file(
            path,
            bytes,
            sinomo_dfs::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    {
        let primary_service = runtime.metadata_primary_service(path).unwrap();
        let backend = primary_service.metadata_backend().unwrap();
        let receipts = backend
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        assert_eq!(
            receipts.len(),
            1,
            "denied receivers have no durability receipts"
        );
        assert_eq!(receipts[0].replica_node_id, primary);
        assert_eq!(receipts[0].receipt.file_system_id, stat.file_system_id);
        assert_eq!(receipts[0].receipt.object_id, stat.object_id);
        assert_eq!(receipts[0].receipt.committed_version, stat.version);
        assert_eq!(receipts[0].content_hash, stat.content_hash);
        let history = backend
            .object_write_intent_history(&stat.object_id)
            .unwrap();
        assert_eq!(history.intents.len(), 1);
        assert!(history.receipts.is_empty());
        let actual = &history.intents[0];
        assert_eq!(actual.status, sinomo_dfs::DfsWriteIntentStatus::Committed);
        assert!(actual.publication.is_current());
        assert_eq!(actual.file_system_id, stat.file_system_id);
        assert_eq!(actual.object_id, stat.object_id);
        assert_eq!(actual.target_version, stat.version);
        assert_eq!(actual.content_hash, stat.content_hash);
        assert_eq!(actual.policy, DfsReplicationPolicy::new(1, 3).unwrap());
        assert_eq!(
            actual
                .desired_replica_node_ids
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            nodes
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
        );
        let repairs = sinomo_dfs::DfsOperatorInspector::new(backend)
            .repairs(Some(&stat.file_system_id), Some(&stat.object_id))
            .unwrap();
        assert_eq!(repairs.len(), 2, "the actual full owner still owes N-W");
        assert!(
            repairs
                .iter()
                .all(|repair| repair.intent_id == actual.intent_id
                    && repair.file_system_id == stat.file_system_id
                    && repair.object_id == stat.object_id
                    && repair.version == stat.version
                    && repair.content_hash == stat.content_hash
                    && repair.missing_replica_node_id != primary)
        );
    }
    assert_eq!(writer.posix_read_file(path).unwrap(), bytes);
    drop(writer);
    let before = runtime.rpc_observations().len();
    runtime.clear_faults();
    let mut syncer = runtime.client(requester).unwrap();
    let certificate = syncer
        .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
        .expect("fresh served barrier repairs the actual full owner's N-W plan");
    assert_eq!(certificate.target.version, stat.version);
    assert_eq!(certificate.content_evidence.required_receipts, 3);
    assert_eq!(
        certificate
            .content_evidence
            .receipt_node_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        nodes
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    );
    assert!(
        runtime.rpc_observations()[before..]
            .iter()
            .any(
                |event| event.route == sinomo_dfs::DFS_DATA_REPAIR_REPLICA_ROUTE
                    && event.phase == DfsTestRpcPhase::Completed
            )
    );
    assert_eq!(syncer.posix_stat(path).unwrap().version, stat.version);
    drop(syncer);
    for node in &nodes {
        let replica = runtime.client(node).unwrap();
        let actual = replica
            .content()
            .read_blob(stat.content_hash.as_ref().unwrap())
            .unwrap();
        assert_eq!(actual, bytes);
        assert_eq!(content_hash(&actual), stat.content_hash.clone().unwrap());
    }
    runtime.close().unwrap();
}

fn large_test_bytes(extra_bytes: usize) -> Vec<u8> {
    (0..(TEST_LARGE_DFS_BYTES + extra_bytes))
        .map(|index| (index % 251) as u8)
        .collect()
}

#[test]
fn networked_dfs_supports_three_nodes_two_replicas_with_cross_reads() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "three-node-two-replica-cross-read-native",
        3,
    );
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "three-node-two-replica-cross-read-native",
        nodes[0].clone(),
    )
    .expect("normal runtime");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("node identity");
    }
    runtime.start_nodes().expect("real native startup");
    let paths = ["/users/cross-a.bin", "/users/cross-b.bin"];
    let bytes = [b"owned by a".as_slice(), b"owned by b".as_slice()];
    for (index, path) in paths.iter().enumerate() {
        let mut policy =
            sinomo_dfs::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
                .unwrap();
        policy.required_node_ids = vec![nodes[index].clone(), nodes[2].clone()];
        runtime
            .apply_placement_policy(
                &nodes[0],
                path,
                sinomo_dfs::DfsPlacementScope::Subtree,
                policy,
            )
            .expect("normal explicit replica policy Apply");
    }
    let mut stats = Vec::new();
    for index in 0..2 {
        let mut writer = runtime.client(&nodes[index]).expect("normal writer");
        let stat = writer
            .posix_write_file(
                paths[index],
                bytes[index],
                sinomo_dfs::DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                    replica_candidates: nodes.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .expect("normal two-replica publication");
        writer
            .posix_sync_file(paths[index], sinomo_dfs::DfsPosixSyncKind::File)
            .expect("real two-replica durability");
        let receipts = writer
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        let actual = receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(actual.contains(&nodes[index]));
        assert!(actual.contains(&nodes[2]));
        stats.push(stat);
        drop(writer);
    }
    let shared_replica = runtime.client(&nodes[2]).expect("shared real replica");
    for index in 0..2 {
        assert_eq!(
            shared_replica
                .content()
                .read_object_version(
                    stats[index].object_id.as_str(),
                    stats[index].version.as_str()
                )
                .expect("durable shared replica bytes")
                .1,
            bytes[index]
        );
        let mut cross_reader = runtime.client(&nodes[1 - index]).expect("cross reader");
        assert_eq!(
            cross_reader
                .posix_read_file_distributed(paths[index])
                .expect("normal cross read"),
            bytes[index]
        );
        drop(cross_reader);
    }
    drop(shared_replica);
    runtime.close().expect("normal shutdown");
}

#[test]
fn networked_dfs_transfers_large_writes() {
    let nodes =
        sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids("native-large-byte-write", 3);
    let mut runtime =
        sinomo_dfs::test_support::DfsTestRuntime::new("native-large-byte-write", nodes[0].clone())
            .expect("normal runtime");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("node identity");
    }
    runtime.start_nodes().expect("real native startup");
    let path = "/users/large-native.bin";
    runtime
        .apply_placement_policy(
            &nodes[0],
            path,
            sinomo_dfs::DfsPlacementScope::Subtree,
            sinomo_dfs::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
                .unwrap(),
        )
        .expect("normal confirmed W2/N2 policy Apply");
    let mut writer = runtime.client(&nodes[0]).expect("writer client");
    let bytes = large_test_bytes(4_464);
    let options = sinomo_dfs::DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let stat = writer
        .posix_write_file(path, &bytes, options)
        .expect("normal large publication");
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(stat.content_hash, Some(content_hash(&bytes)));
    writer
        .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
        .expect("real W2 durability");
    let receipts = writer
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .expect("actual replica receipts");
    let acknowledged = receipts
        .iter()
        .map(|receipt| receipt.replica_node_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(acknowledged.len() >= 2, "no fabricated content ACKs");
    for replica in acknowledged {
        let client = runtime.client(&replica).expect("real content replica");
        let (manifest, actual) = client
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .expect("durable replica bytes");
        assert_eq!(actual, bytes);
        assert_eq!(manifest.content_hash, content_hash(&bytes));
        drop(client);
    }
    drop(writer);
    runtime.close().expect("normal shutdown");
}

#[test]
fn networked_dfs_streams_large_writes_from_reader() {
    let nodes =
        sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids("native-large-reader-write", 3);
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "native-large-reader-write",
        nodes[0].clone(),
    )
    .expect("normal runtime");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("node identity");
    }
    runtime.start_nodes().expect("real native startup");
    let path = "/users/large-native.bin";
    runtime
        .apply_placement_policy(
            &nodes[0],
            path,
            sinomo_dfs::DfsPlacementScope::Subtree,
            sinomo_dfs::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
                .unwrap(),
        )
        .expect("normal confirmed W2/N2 policy Apply");
    let mut writer = runtime.client(&nodes[0]).expect("writer client");
    let bytes = large_test_bytes(4_464);
    let options = sinomo_dfs::DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let stat = writer
        .posix_write_file_from_reader(path, std::io::Cursor::new(&bytes), options)
        .expect("normal large publication");
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(stat.content_hash, Some(content_hash(&bytes)));
    writer
        .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
        .expect("real W2 durability");
    let receipts = writer
        .metadata_backend()
        .unwrap()
        .receipts_for_version(&stat.object_id, &stat.version)
        .expect("actual replica receipts");
    let acknowledged = receipts
        .iter()
        .map(|receipt| receipt.replica_node_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(acknowledged.len() >= 2, "no fabricated content ACKs");
    for replica in acknowledged {
        let client = runtime.client(&replica).expect("real content replica");
        let (manifest, actual) = client
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .expect("durable replica bytes");
        assert_eq!(actual, bytes);
        assert_eq!(manifest.content_hash, content_hash(&bytes));
        drop(client);
    }
    drop(writer);
    runtime.close().expect("normal shutdown");
}

#[test]
fn networked_dfs_replicates_configured_metadata_shard_to_replicas() {
    let nodes =
        sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids("configured-metadata-native", 3);
    let owner = nodes[0].clone();
    let mut runtime =
        sinomo_dfs::test_support::DfsTestRuntime::new("configured-metadata-native", owner.clone())
            .expect("normal runtime");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("node identity");
    }
    runtime.start_nodes().expect("real native startup");
    // The normal managed root uses every eligible metadata service. Consume
    // that actual published assignment instead of a conflicting smaller ring.
    let shard = runtime
        .root_shard()
        .expect("actual managed root assignment");
    assert_eq!(shard.primary_node_id, owner);
    assert_eq!(
        shard
            .replica_node_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        nodes[1..]
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    );
    let path = "/configured-metadata-native.bin";
    let bytes = b"normal Main publication reaches every configured metadata replica";
    let mut owner_dfs = runtime.client(&owner).expect("owner client");
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let stat = owner_dfs
        .posix_write_file(
            path,
            bytes,
            sinomo_dfs::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![owner.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .expect("ordinary Main root publication");
    owner_dfs
        .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
        .expect("ordinary Main durability barrier");
    drop(owner_dfs);
    let root_shard = DfsMetadataShardId::new("root");
    let primary = runtime
        .metadata_primary_service(path)
        .expect("actual current owner");
    let backend = primary.metadata_backend().unwrap();
    let expected = backend
        .export_shard_table_snapshot(&root_shard)
        .expect("owner snapshot")
        .expect("root shard");
    let object = backend.get_object(&stat.object_id).unwrap().unwrap();
    assert_eq!(object.version, stat.version);
    assert_eq!(object.content_hash, stat.content_hash);
    let prepared =
        expected
            .tables
            .prepared_operations_v7
            .iter()
            .find(|record| {
                record.payload.transaction.objects.iter().any(|object| {
                    object.object_id == stat.object_id && object.version == stat.version
                })
            })
            .expect("actual complete prepared body for the normal Main write")
            .clone();
    assert_eq!(
        prepared.status,
        sinomo_dfs::DfsPreparedOperationStatus::Committed
    );
    let file_system = backend
        .get_file_system(&stat.file_system_id)
        .unwrap()
        .unwrap();
    let journal = backend
        .journal_batch_for_shard_since_limited(
            &root_shard,
            0,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .expect("current journal")
        .expect("root journal");
    assert!(journal.entries.iter().any(|entry| matches!(
        &entry.mutation,
        DfsMetadataMutation::PrepareOperationV7 { record, .. } if record.digest == prepared.digest
    )));
    assert!(journal.entries.iter().any(|entry| matches!(
        &entry.mutation,
        DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } if digest == &prepared.digest
    )));
    drop(primary);

    // Publication's prepare proof and File sync's majority do not substitute
    // for an individual receiver's complete committed prefix. Observe each
    // actual physical replica before quiescing it under the same budget that
    // began before the original Main write dispatch.
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "all configured metadata replicas must catch up within the existing network budget"
        );
        let mut complete = true;
        for replica in &shard.replica_node_ids {
            let metadata_path =
                sinomo_dfs::default_dfs_metadata_shards_root(runtime.data_dir(replica))
                    .join("root");
            let backend =
                LocalSobjMetadataBackend::open_for_inspection(root_shard.clone(), metadata_path)
                    .expect("actual live receiver inspection");
            let actual = backend
                .export_shard_table_snapshot(&root_shard)
                .unwrap()
                .unwrap();
            complete &= actual.prefix_proof == expected.prefix_proof
                && actual.materialized_through_sequence == expected.materialized_through_sequence;
        }
        if complete {
            assert!(std::time::Instant::now() <= deadline);
            break;
        }
        std::thread::yield_now();
    }
    for replica in &shard.replica_node_ids {
        runtime
            .stop_node(replica)
            .expect("quiesce actual configured receiver");
        {
            let metadata_path =
                sinomo_dfs::default_dfs_metadata_shards_root(runtime.data_dir(replica))
                    .join("root");
            let backend =
                LocalSobjMetadataBackend::open_for_inspection(root_shard.clone(), metadata_path)
                    .expect("durable receiver inspection");
            assert_eq!(
                backend.get_file_system(&stat.file_system_id).unwrap(),
                Some(file_system.clone())
            );
            assert_eq!(
                backend.get_object(&stat.object_id).unwrap(),
                Some(object.clone())
            );
            let actual = backend
                .export_shard_table_snapshot(&root_shard)
                .unwrap()
                .unwrap();
            assert_eq!(
                actual.prefix_proof, expected.prefix_proof,
                "ACK survives receiver shutdown"
            );
            assert_eq!(
                actual.materialized_through_sequence,
                expected.materialized_through_sequence
            );
            assert_eq!(
                actual
                    .tables
                    .prepared_operations_v7
                    .iter()
                    .find(|record| record.digest == prepared.digest),
                Some(&prepared)
            );
        }
        runtime
            .reopen_node(replica.clone())
            .expect("normal receiver reopen");
        let mut receiver = runtime.client(replica).expect("receiver client");
        assert_eq!(receiver.posix_read_file_distributed(path).unwrap(), bytes);
        drop(receiver);
        let metadata_path =
            sinomo_dfs::default_dfs_metadata_shards_root(runtime.data_dir(replica)).join("root");
        let backend =
            LocalSobjMetadataBackend::open_for_inspection(root_shard.clone(), metadata_path)
                .expect("actual reopened receiver inspection");
        assert_eq!(
            backend.get_object(&stat.object_id).unwrap(),
            Some(object.clone())
        );
        let actual = backend
            .export_shard_table_snapshot(&root_shard)
            .unwrap()
            .unwrap();
        assert_eq!(actual.prefix_proof, expected.prefix_proof);
        assert_eq!(
            actual.materialized_through_sequence,
            expected.materialized_through_sequence
        );
        assert_eq!(
            actual
                .tables
                .prepared_operations_v7
                .iter()
                .find(|record| record.digest == prepared.digest),
            Some(&prepared)
        );
    }
    runtime.close().expect("normal shutdown");
}

#[test]
fn posix_paths_treat_backslash_as_filename_character() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "posix_paths_treat_backslash_as_filename_character",
        1,
    );
    let owner = nodes[0].clone();
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "posix_paths_treat_backslash_as_filename_character",
        owner.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_mkdir_all("/services/names/a")
        .expect("mkdir slash parent");

    let backslash_stat = dfs
        .posix_write_file(
            r"/services/names/a\b.txt",
            b"backslash",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("write backslash filename");
    let slash_stat = dfs
        .posix_write_file(
            "/services/names/a/b.txt",
            b"slash",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("write slash path");

    assert_ne!(backslash_stat.object_id, slash_stat.object_id);
    assert_eq!(
        dfs.posix_read_file(r"/services/names/a\b.txt")
            .expect("read backslash filename"),
        b"backslash"
    );
    assert_eq!(
        dfs.posix_read_file("/services/names/a/b.txt")
            .expect("read slash path"),
        b"slash"
    );
    assert_eq!(
        dfs.posix_list_dir("/services/names")
            .expect("list names")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["a".to_string(), r"a\b.txt".to_string()]
    );

    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let reloaded = runtime.client(&owner).unwrap();
    assert_eq!(
        reloaded
            .posix_read_file(r"/services/names/a\b.txt")
            .expect("read backslash after reload"),
        b"backslash"
    );
    assert_eq!(
        reloaded
            .posix_read_file("/services/names/a/b.txt")
            .expect("read slash after reload"),
        b"slash"
    );

    drop(reloaded);
    runtime.close().unwrap();
}

#[test]
fn posix_paths_resolve_parent_components_within_sifs_root() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "posix_paths_resolve_parent_components_within_sifs_root",
        1,
    );
    let owner = nodes[0].clone();
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "posix_paths_resolve_parent_components_within_sifs_root",
        owner.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_mkdir_all("/services/paths")
        .expect("mkdir services paths");

    assert!(matches!(
        dfs.posix_write_file(
            "/services/paths/tmp/../file.txt",
            b"missing component must not be skipped",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        ),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    dfs.posix_mkdir("/services/paths/tmp")
        .expect("parent traversal requires an existing directory");

    let stat = dfs
        .posix_write_file(
            "/services/paths/tmp/../file.txt",
            b"normalized",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("write through parent component");

    assert_eq!(stat.path, "/services/paths/file.txt");
    assert_eq!(
        dfs.posix_read_file("/../services/paths/./file.txt")
            .expect("read with root parent and current component"),
        b"normalized"
    );
    assert!(
        matches!(
            dfs.posix_stat("/services/paths/tmp/file.txt"),
            Err(sinomo_dfs::DfsPosixError::NotFound { .. })
        ),
        "parent components must be resolved instead of becoming path data"
    );
    dfs.posix_rmdir("/services/paths/tmp")
        .expect("the intermediate directory stays empty");
    assert_eq!(
        dfs.posix_list_dir("/services/paths/../paths")
            .expect("list normalized path")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["file.txt".to_string()]
    );

    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let reloaded = runtime.client(&owner).unwrap();
    assert_eq!(
        reloaded
            .posix_read_file("/services/paths/../paths/file.txt")
            .expect("read normalized path after reload"),
        b"normalized"
    );

    drop(reloaded);
    runtime.close().unwrap();
}

#[test]
fn posix_paths_reject_nul_bytes_before_metadata_mutation() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "posix_paths_reject_nul_bytes_before_metadata_mutation",
        1,
    );
    let owner = nodes[0].clone();
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "posix_paths_reject_nul_bytes_before_metadata_mutation",
        owner.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_mkdir_all("/services/nul")
        .expect("mkdir nul parent");

    for result in [
        dfs.posix_write_file(
            "/services/nul/bad\0name.txt",
            b"bad",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        ),
        dfs.posix_mkdir("/services/nul/bad\0dir"),
        dfs.posix_symlink(
            "target",
            "/services/nul/bad\0link",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        ),
    ] {
        assert!(matches!(
            result,
            Err(sinomo_dfs::DfsPosixError::InvalidPath { .. })
        ));
    }

    assert!(
        dfs.posix_list_dir("/services/nul")
            .expect("list nul parent")
            .is_empty(),
        "invalid paths must not create metadata entries"
    );

    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let reloaded = runtime.client(&owner).unwrap();
    assert!(
        reloaded
            .posix_list_dir("/services/nul")
            .expect("reload list nul parent")
            .is_empty()
    );

    drop(reloaded);
    runtime.close().unwrap();
}

#[test]
fn posix_symlink_rejects_invalid_targets_before_metadata_mutation() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "posix_symlink_rejects_invalid_targets_before_metadata_mutation",
        1,
    );
    let owner = nodes[0].clone();
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "posix_symlink_rejects_invalid_targets_before_metadata_mutation",
        owner.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_mkdir_all("/services/links")
        .expect("mkdir links parent");

    for (target, link_path) in [
        ("", "/services/links/empty-target"),
        ("bad\0target", "/services/links/nul-target"),
    ] {
        assert!(matches!(
            dfs.posix_symlink(
                target,
                link_path,
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()])
            ),
            Err(sinomo_dfs::DfsPosixError::InvalidPath { .. })
        ));
    }

    assert!(
        dfs.posix_list_dir("/services/links")
            .expect("list links parent")
            .is_empty(),
        "invalid symlink targets must not create metadata entries"
    );

    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let reloaded = runtime.client(&owner).unwrap();
    assert!(
        reloaded
            .posix_list_dir("/services/links")
            .expect("reload list links parent")
            .is_empty()
    );

    drop(reloaded);
    runtime.close().unwrap();
}

#[test]
fn posix_stat_read_and_write_follow_symlink_targets() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "posix_stat_read_and_write_follow_symlink_targets",
        1,
    );
    let owner = nodes[0].clone();
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "posix_stat_read_and_write_follow_symlink_targets",
        owner.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_mkdir_all("/services/symlink-targets/real-dir")
        .expect("mkdir symlink targets");
    dfs.posix_write_file(
        "/services/symlink-targets/target.txt",
        b"target",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("write target");
    dfs.posix_symlink(
        "target.txt",
        "/services/symlink-targets/link.txt",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("create file symlink");

    assert_eq!(
        dfs.posix_lstat("/services/symlink-targets/link.txt")
            .expect("lstat link")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_stat("/services/symlink-targets/link.txt")
            .expect("stat follows link")
            .kind,
        DfsObjectKind::File
    );
    assert_eq!(
        dfs.posix_read_file("/services/symlink-targets/link.txt")
            .expect("read follows link"),
        b"target"
    );
    dfs.posix_write_file(
        "/services/symlink-targets/link.txt",
        b"updated",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("write follows link");
    assert_eq!(
        dfs.posix_read_file("/services/symlink-targets/target.txt")
            .expect("read updated target"),
        b"updated"
    );
    assert_eq!(
        dfs.posix_readlink("/services/symlink-targets/link.txt")
            .expect("readlink preserves target"),
        "target.txt"
    );
    dfs.posix_set_mode("/services/symlink-targets/link.txt", 0o600)
        .expect("chmod follows link");
    assert_eq!(
        dfs.posix_stat("/services/symlink-targets/target.txt")
            .expect("target mode")
            .mode,
        0o600
    );
    assert_ne!(
        dfs.posix_lstat("/services/symlink-targets/link.txt")
            .expect("link mode remains independent")
            .mode,
        0o600
    );
    dfs.posix_set_times(
        "/services/symlink-targets/link.txt",
        Some(11_111),
        Some(22_222),
    )
    .expect("utimens follows link");
    assert_eq!(
        dfs.posix_stat("/services/symlink-targets/target.txt")
            .expect("target atime")
            .accessed_at_unix_nanos,
        11_111
    );
    dfs.posix_set_symlink_times(
        "/services/symlink-targets/link.txt",
        Some(33_333),
        Some(44_444),
    )
    .expect("lutimens updates link");
    assert_eq!(
        dfs.posix_lstat("/services/symlink-targets/link.txt")
            .expect("link atime")
            .accessed_at_unix_nanos,
        33_333
    );
    assert_eq!(
        dfs.posix_stat("/services/symlink-targets/target.txt")
            .expect("target atime unchanged by lutimens")
            .accessed_at_unix_nanos,
        11_111
    );

    dfs.posix_write_file(
        "/services/symlink-targets/real-dir/nested.txt",
        b"nested",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("write nested target");
    dfs.posix_symlink(
        "real-dir",
        "/services/symlink-targets/dir-link",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("create directory symlink");
    assert_eq!(
        dfs.posix_read_file("/services/symlink-targets/dir-link/nested.txt")
            .expect("read through intermediate symlink"),
        b"nested"
    );
    assert_eq!(
        dfs.posix_list_dir("/services/symlink-targets/dir-link")
            .expect("list follows directory symlink")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["nested.txt".to_string()]
    );

    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let reloaded = runtime.client(&owner).unwrap();
    assert_eq!(
        reloaded
            .posix_read_file("/services/symlink-targets/link.txt")
            .expect("reloaded read follows link"),
        b"updated"
    );
    assert_eq!(
        reloaded
            .posix_read_file("/services/symlink-targets/dir-link/nested.txt")
            .expect("reloaded read through intermediate symlink"),
        b"nested"
    );
    assert_eq!(
        reloaded
            .posix_list_dir("/services/symlink-targets/dir-link")
            .expect("reloaded list follows directory symlink")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        vec!["nested.txt".to_string()]
    );
    assert_eq!(
        reloaded
            .posix_lstat("/services/symlink-targets/link.txt")
            .expect("reloaded lstat link")
            .kind,
        DfsObjectKind::Symlink
    );

    drop(reloaded);
    runtime.close().unwrap();
}

#[test]
fn posix_operations_follow_intermediate_parent_symlinks() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "posix_operations_follow_intermediate_parent_symlinks",
        1,
    );
    let owner = nodes[0].clone();
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "posix_operations_follow_intermediate_parent_symlinks",
        owner.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_mkdir_all("/services/parents/real")
        .expect("mkdir real parent");
    dfs.posix_symlink(
        "real",
        "/services/parents/link",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("create parent symlink");

    dfs.posix_mkdir("/services/parents/link/created")
        .expect("mkdir through parent symlink");
    dfs.posix_write_file(
        "/services/parents/real/original.txt",
        b"original",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("write hardlink target");
    dfs.posix_hardlink(
        "/services/parents/link/original.txt",
        "/services/parents/link/original-alias.txt",
    )
    .expect("hardlink through parent symlink");
    assert_eq!(
        dfs.posix_stat("/services/parents/real/original.txt")
            .expect("original link count")
            .link_count,
        2
    );
    assert_eq!(
        dfs.posix_read_file("/services/parents/real/original-alias.txt")
            .expect("read hardlink alias"),
        b"original"
    );
    assert_eq!(
        dfs.posix_stat("/services/parents/real/created")
            .expect("created target dir")
            .kind,
        DfsObjectKind::Directory
    );
    assert_eq!(
        dfs.posix_stat("/services/parents/link/created")
            .expect("stat through parent symlink")
            .kind,
        DfsObjectKind::Directory
    );

    dfs.posix_symlink(
        "created",
        "/services/parents/link/created-link",
        sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
    )
    .expect("create symlink through parent symlink");
    assert_eq!(
        dfs.posix_lstat("/services/parents/link/created-link")
            .expect("lstat follows parent symlink only")
            .kind,
        DfsObjectKind::Symlink
    );
    assert_eq!(
        dfs.posix_lstat("/services/parents/real/created-link")
            .expect("canonical lstat")
            .kind,
        DfsObjectKind::Symlink
    );

    dfs.posix_rename(
        "/services/parents/link/created",
        "/services/parents/link/renamed",
    )
    .expect("rename through parent symlink");
    assert!(matches!(
        dfs.posix_stat("/services/parents/real/created"),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        dfs.posix_stat("/services/parents/real/renamed")
            .expect("renamed target dir")
            .kind,
        DfsObjectKind::Directory
    );

    dfs.posix_unlink("/services/parents/link/created-link")
        .expect("unlink through parent symlink");
    assert!(matches!(
        dfs.posix_lstat("/services/parents/real/created-link"),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));

    drop(dfs);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let reloaded = runtime.client(&owner).unwrap();
    assert_eq!(
        reloaded
            .posix_stat("/services/parents/link/renamed")
            .expect("reloaded stat through parent symlink")
            .kind,
        DfsObjectKind::Directory
    );

    drop(reloaded);
    runtime.close().unwrap();
}

#[test]
fn networked_dfs_transfers_large_remote_read() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "networked-large-read-native",
        2,
    );
    let owner = nodes[0].clone();
    let reader = nodes[1].clone();
    let mut runtime =
        sinomo_dfs::test_support::DfsTestRuntime::new("networked-large-read-native", owner.clone())
            .expect("normal runtime");
    runtime
        .define_node(reader.clone())
        .expect("reader identity");
    runtime.start_nodes().expect("real native startup");
    let mut owner_dfs = runtime.client(&owner).expect("owner client");
    let mut reader_dfs = runtime.client(&reader).expect("reader client");
    let path = format!("{}/large.bin", sifs_node_files_path(&owner));
    let bytes = (0..200_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let stat = owner_dfs
        .posix_write_file(
            &path,
            &bytes,
            sinomo_dfs::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("content policy"),
                replica_candidates: vec![owner.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .expect("normal publication");
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(stat.content_hash, Some(content_hash(&bytes)));
    assert!(
        reader_dfs
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .is_err(),
        "reader starts without content"
    );
    assert_eq!(
        reader_dfs
            .posix_read_file_distributed(&path)
            .expect("native streamed read"),
        bytes
    );
    assert!(
        reader_dfs
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .is_err(),
        "the current distributed full-read path returns exact bytes without creating a reader-owned content binding",
    );
    assert_eq!(
        reader_dfs
            .posix_read_file_distributed(&path)
            .expect("repeat exact native read"),
        bytes
    );
    drop(reader_dfs);
    drop(owner_dfs);
    runtime.close().expect("normal shutdown");
}

#[test]
fn networked_dfs_remote_range_read_returns_slice_without_full_cache() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "networked-range-read-native",
        2,
    );
    let owner = nodes[0].clone();
    let reader = nodes[1].clone();
    let mut runtime =
        sinomo_dfs::test_support::DfsTestRuntime::new("networked-range-read-native", owner.clone())
            .expect("normal runtime");
    runtime
        .define_node(reader.clone())
        .expect("reader identity");
    runtime.start_nodes().expect("real native startup");
    let mut owner_dfs = runtime.client(&owner).expect("owner client");
    let mut reader_dfs = runtime.client(&reader).expect("reader client");
    let path = format!("{}/range.bin", sifs_node_files_path(&owner));
    let bytes = (0..200_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let stat = owner_dfs
        .posix_write_file(
            &path,
            &bytes,
            sinomo_dfs::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).expect("content policy"),
                replica_candidates: vec![owner.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .expect("normal publication");
    assert_eq!(stat.size_bytes, Some(bytes.len() as u64));
    assert_eq!(stat.content_hash, Some(content_hash(&bytes)));
    assert!(
        reader_dfs
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .is_err(),
        "reader starts without content"
    );
    let offset = 70_003usize;
    let length = 4096usize;
    assert_eq!(
        reader_dfs
            .posix_read_file_range_distributed(&path, offset as u64, length)
            .expect("native streamed range"),
        bytes[offset..offset + length]
    );
    assert!(
        reader_dfs
            .content()
            .read_object_version(stat.object_id.as_str(), stat.version.as_str())
            .is_err(),
        "range read does not populate a full cache"
    );
    drop(reader_dfs);
    drop(owner_dfs);
    runtime.close().expect("normal shutdown");
}

#[test]
fn posix_write_file_from_reader_streams_content() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "posix_write_file_from_reader_streams_content",
        1,
    );
    let owner = nodes[0].clone();
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "posix_write_file_from_reader_streams_content",
        owner.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_mkdir_all("/services/web").expect("mkdir service");

    let bytes = 128 * 1024 + 17;
    let stat = dfs
        .posix_write_file_from_reader(
            "/services/web/stream.bin",
            PatternReader::new(bytes),
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("streaming POSIX write");

    assert_eq!(stat.size_bytes, Some(bytes as u64));
    assert_eq!(
        dfs.posix_read_file("/services/web/stream.bin")
            .expect("read streamed POSIX file"),
        pattern_bytes(bytes)
    );
    let backend_object = dfs
        .metadata_backend()
        .expect("metadata backend")
        .get_object_by_path(
            &sinomo_dfs::sifs_file_system_id(),
            "/services/web/stream.bin",
        )
        .expect("backend path lookup")
        .expect("streamed object metadata");
    assert_eq!(backend_object.content_size_bytes, Some(bytes as u64));
    assert_eq!(backend_object.content_hash, stat.content_hash);

    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn posix_open_file_stream_reads_file_and_range_incrementally() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "posix_open_file_stream_reads_file_and_range_incrementally",
        1,
    );
    let owner = nodes[0].clone();
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "posix_open_file_stream_reads_file_and_range_incrementally",
        owner.clone(),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut dfs = runtime.client(&owner).unwrap();
    dfs.posix_mkdir_all("/services/web").expect("mkdir service");
    let bytes = 256 * 1024 + 31;
    let stat = dfs
        .posix_write_file_from_reader(
            "/services/web/stream-read.bin",
            PatternReader::new(bytes),
            sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .expect("streaming POSIX write");

    let mut stream = dfs
        .posix_open_file_stream("/services/web/stream-read.bin")
        .expect("open file stream");
    assert_eq!(stream.object_id, stat.object_id);
    assert_eq!(stream.version, stat.version);
    assert_eq!(stream.content_hash, stat.content_hash);
    assert_eq!(stream.size_bytes, bytes as u64);
    let mut streamed = Vec::new();
    let mut buffer = [0_u8; 4093];
    loop {
        let read = stream.reader.read(&mut buffer).expect("read stream");
        if read == 0 {
            break;
        }
        streamed.extend_from_slice(&buffer[..read]);
    }
    assert_eq!(streamed, pattern_bytes(bytes));

    let offset = 4097_u64;
    let range_len = 8192 + 13;
    let mut range_stream = dfs
        .posix_open_file_range_stream("/services/web/stream-read.bin", offset, range_len)
        .expect("open range stream");
    assert_eq!(range_stream.object_id, stat.object_id);
    assert_eq!(range_stream.version, stat.version);
    assert_eq!(range_stream.content_hash, stat.content_hash);
    assert_eq!(range_stream.offset, offset);
    assert_eq!(range_stream.total_size_bytes, bytes as u64);
    assert_eq!(range_stream.range_size_bytes, range_len as u64);
    let mut streamed_range = Vec::new();
    range_stream
        .reader
        .read_to_end(&mut streamed_range)
        .expect("read range stream");
    let expected = pattern_bytes(bytes);
    let range_start = offset as usize;
    assert_eq!(
        streamed_range,
        expected[range_start..range_start + range_len].to_vec()
    );

    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn networked_dfs_confirmed_read_uses_backend_current_metadata() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "confirmed-read-current-native",
        3,
    );
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "confirmed-read-current-native",
        nodes[0].clone(),
    )
    .expect("normal runtime");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("node identity");
    }
    runtime.start_nodes().expect("real native startup");
    let bytes = b"confirmed backend bytes";
    let path = "/users/confirmed-current.bin";
    runtime
        .apply_placement_policy(
            &nodes[0],
            path,
            sinomo_dfs::DfsPlacementScope::Subtree,
            sinomo_dfs::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
                .unwrap(),
        )
        .expect("normal confirmed W2/N2 policy Apply");
    let mut writer = runtime.client(&nodes[0]).expect("writer client");
    let stat = writer
        .posix_write_file(
            path,
            bytes,
            sinomo_dfs::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("content policy"),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("normal replicated publication");
    writer
        .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
        .expect("real durability certificate");
    let holder = writer
        .metadata_backend()
        .expect("current writer backend")
        .receipts_for_version(&stat.object_id, &stat.version)
        .expect("actual recorded content owners")
        .into_iter()
        .find(|receipt| receipt.replica_node_id != *writer.node_id())
        .expect("real second content holder")
        .replica_node_id;
    let mut reader = runtime.client(&holder).expect("actual holder client");
    assert_eq!(
        reader
            .posix_read_file(path)
            .expect("holder's current bytes"),
        bytes
    );
    let backend = reader.metadata_backend().expect("current backend");
    let current = backend
        .get_object(&stat.object_id)
        .expect("current object")
        .expect("object");
    let receipts = backend
        .receipts_for_version(&stat.object_id, &stat.version)
        .expect("real durable receipts");
    let mut acknowledged = receipts
        .iter()
        .map(|receipt| receipt.replica_node_id.clone())
        .collect::<Vec<_>>();
    acknowledged.sort();
    acknowledged.dedup();
    assert!(
        acknowledged.len() >= 2,
        "confirmed read requires actual replica ACKs"
    );
    let mut stale = current;
    stale.version = DfsVersion::generate();
    stale.content_hash = Some(content_hash(b"stale bytes"));
    stale.content_size_bytes = Some(11);
    reader
        .add_transient_object_metadata(stale)
        .expect("transient stale overlay");
    assert!(
        matches!(reader.read_local_object_confirmed(&stat.object_id,&acknowledged).expect("current confirmed read"),
        NetworkedDfsReadOutcome::Found { data, freshness:NetworkedDfsReadFreshness::Confirmed { replica_node_ids } }
            if data.bytes == bytes && data.version == stat.version && replica_node_ids == acknowledged)
    );
    drop(reader);
    drop(writer);
    runtime.close().expect("normal shutdown");
}

#[test]
fn networked_dfs_reconnect_reconciliation_uses_backend_current_metadata() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "reconnect-backend-current-native",
        3,
    );
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "reconnect-backend-current-native",
        nodes[0].clone(),
    )
    .expect("normal runtime");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("node identity");
    }
    runtime.start_nodes().expect("real native startup");
    let mut dfs = runtime.client(&nodes[0]).expect("normal client");
    let path = format!("{}/reconnect.bin", sifs_node_files_path(&nodes[0]));
    let options = sinomo_dfs::DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![nodes[0].clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    let old = dfs
        .posix_write_file(&path, b"old reconnect bytes", options.clone())
        .expect("first normal publication");
    let mut stale = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&old.object_id)
        .unwrap()
        .unwrap();
    let bytes = b"backend-current reconnect bytes";
    let current = dfs
        .posix_write_file(&path, bytes, options)
        .expect("current normal publication");
    stale.version = old.version;
    dfs.add_transient_object_metadata(stale)
        .expect("stale transient overlay");
    let before = dfs
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let report = dfs
        .reconcile_local_manifests(&current.object_id)
        .expect("normal reconnect reconciliation");
    assert!(!report.missing_current);
    assert_eq!(
        report.refreshed_receipts, 0,
        "already durable current receipt is idempotent"
    );
    let backend = dfs.metadata_backend().unwrap();
    let actual = backend.get_object(&current.object_id).unwrap().unwrap();
    assert_eq!(actual.version, current.version);
    assert_eq!(actual.content_hash, Some(content_hash(bytes)));
    assert_eq!(
        backend.journal_last_sequence().unwrap(),
        before,
        "stale transient state cannot republish metadata"
    );
    assert_eq!(
        dfs.posix_read_file(&path).expect("current local read"),
        bytes
    );
    drop(dfs);
    runtime.close().expect("normal shutdown");
}

#[test]
fn networked_dfs_native_handoff_preserves_latest_content_and_routes_old_primary() {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(
        "native-handoff-latest-content",
        3,
    );
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(
        "native-handoff-latest-content",
        nodes[0].clone(),
    )
    .expect("normal runtime");
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).expect("node identity");
    }
    let trace = sinomo_dfs::test_support::NativeShardTraceArtifactSession::from_environment(
        "native-handoff-latest-content",
        nodes
            .iter()
            .map(|node| (node.clone(), runtime.data_dir(node).root().to_owned())),
        DfsMetadataShardId::new("root"),
    )
    .expect("actual native capture setup");
    runtime.start_nodes().expect("real native startup");
    let original = runtime.root_shard().expect("actual root assignment");
    let old_primary = original.primary_node_id.clone();
    let new_primary = nodes
        .iter()
        .find(|node| **node != old_primary)
        .unwrap()
        .clone();
    let mut writer = runtime.client(&old_primary).expect("old primary client");
    let path = "/handoff-current.bin";
    let options = sinomo_dfs::DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let old = writer
        .posix_write_file(path, b"old handoff bytes", options.clone())
        .expect("first publication");
    let bytes = b"latest handoff bytes";
    let current = writer
        .posix_write_file(path, bytes, options)
        .expect("current publication");
    assert_ne!(old.version, current.version);
    writer
        .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
        .expect("real content/metadata certificate");
    drop(writer);
    let replicas = nodes
        .iter()
        .filter(|node| **node != new_primary)
        .cloned()
        .collect();
    let activated = runtime
        .reassign_metadata_shard(
            &original.file_system_id,
            "root",
            new_primary.clone(),
            replicas,
        )
        .expect("native fenced whole-shard handoff");
    assert_eq!(activated.primary_node_id, new_primary);
    assert!(activated.epoch > original.epoch);
    let mut old_client = runtime
        .client(&old_primary)
        .expect("old primary remains a normal client");
    let actual = old_client
        .posix_stat_distributed(path)
        .expect("old primary routes to current authority");
    assert_eq!(actual.version, current.version);
    assert_eq!(actual.content_hash, Some(content_hash(bytes)));
    assert_eq!(
        old_client
            .posix_read_file_distributed(path)
            .expect("read current bytes after handoff"),
        bytes
    );
    let mut new_client = runtime.client(&new_primary).expect("new primary client");
    assert_eq!(
        new_client
            .posix_read_file_distributed(path)
            .expect("new primary reads latest content"),
        bytes
    );
    drop(old_client);
    drop(new_client);
    if let Some(trace) = &trace {
        trace.close().expect("close actual handoff capture");
    }
    runtime.close().expect("normal shutdown");
    if let Some(trace) = trace {
        trace.export().expect("export actual handoff capture");
    }
}

fn content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

#[test]
fn native_pending_write_survives_restart_without_publishing_or_admitting_a_second_writer() {
    use sinomo_dfs::test_support::{DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime};
    let nodes = DfsTestRuntime::ordered_node_ids("native-pending-restart", 3);
    let mut runtime = DfsTestRuntime::new("native-pending-restart", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/native-pending-restart.bin";
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let original = writer
        .posix_write_file(
            path,
            b"original",
            sinomo_dfs::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![nodes[0].clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    drop(writer);
    let mut policy =
        sinomo_dfs::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.required_node_ids = vec![nodes[0].clone(), nodes[2].clone()];
    runtime
        .apply_placement_policy(
            &nodes[0],
            path,
            sinomo_dfs::DfsPlacementScope::Subtree,
            policy,
        )
        .unwrap();
    let mut writer = runtime.client(&nodes[0]).unwrap();
    runtime.install_fault(DfsTestFaultRule {
        source: None,
        // Candidate IDs are suggestions to the normal placement planner.
        // Deny every actual remote stage selected by that same planner.
        target: None,
        route: Some(sinomo_dfs::DFS_DATA_STAGE_BLOB_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: DfsTestFaultAction::BudgetExhausted,
    });
    let target = sinomo_dfs::DfsVersion::generate();
    let request = sinomo_core::DfsWriteRequest {
        file_system_id: original.file_system_id.clone(),
        object_id: original.object_id.clone(),
        target_version: target.clone(),
        requesting_node_id: nodes[0].clone(),
    };
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let registered = writer
        .write_distributed_eventual(
            request,
            b"pending replacement with a different length".to_vec(),
            vec![nodes[0].clone(), nodes[2].clone()],
            DfsReplicationPolicy::new(2, 2).unwrap(),
        )
        .unwrap();
    let sinomo_dfs::NetworkedDfsWriteOutcome::Pending {
        intent_id: request_id,
        durable_receipts,
        required_receipts,
    } = registered
    else {
        panic!("normal routed registration has no receipt evidence");
    };
    assert_eq!((durable_receipts, required_receipts), (0, 2));
    let actual_outcome = loop {
        assert!(
            std::time::Instant::now() < deadline,
            "actual Main pending response must fit the original network budget"
        );
        let actual = writer
            .process_incoming()
            .unwrap()
            .into_iter()
            .find_map(|event| {
                if let sinomo_dfs::NetworkedDfsServiceEvent::RemoteWriteCompleted {
                    request_id: received_request_id,
                    object_id,
                    result,
                    ..
                } = event
                    && received_request_id == request_id
                    && object_id == original.object_id
                {
                    Some(result)
                } else {
                    None
                }
            });
        if let Some(result) = actual {
            assert!(std::time::Instant::now() <= deadline);
            let sinomo_dfs::DfsDistributedWriteResult::Outcome { outcome } = result else {
                panic!("actual Main must accept the original pending write: {result:?}");
            };
            break outcome;
        }
        std::thread::yield_now();
    };
    let sinomo_dfs::NetworkedDfsWriteOutcome::Pending {
        intent_id,
        durable_receipts,
        required_receipts,
    } = actual_outcome
    else {
        panic!("a failed real content stage must leave W2 pending");
    };
    assert_eq!((durable_receipts, required_receipts), (1, 2));
    let current_primary = runtime.root_shard().unwrap().primary_node_id;
    let inspected = runtime
        .rpc_request(
            &nodes[0],
            &current_primary,
            sinomo_dfs::dfs_inspection_rpc_request(
                &sinomo_dfs::DaemonDfsInspectionRequest::Object {
                    object_id: original.object_id.clone(),
                },
            )
            .unwrap(),
            deadline.saturating_duration_since(std::time::Instant::now()),
        )
        .unwrap();
    let sinomo_dfs::DaemonDfsInspectionResponse::Object {
        object: Some(primary),
    } = sinomo_dfs::decode_dfs_inspection_rpc_response(&inspected).unwrap()
    else {
        panic!("actual resident owner must inspect the original object");
    };
    assert!(std::time::Instant::now() <= deadline);
    let before = primary
        .intents
        .iter()
        .find(|intent| intent.intent_id == intent_id)
        .unwrap()
        .clone();
    assert_eq!(
        before.status,
        sinomo_dfs::DfsWriteIntentStatus::ReceiptsPending
    );
    assert_eq!(primary.object.version, original.version);
    assert!(
        primary
            .versions
            .iter()
            .all(|version| version.version != target)
    );
    let receipts = primary
        .receipts
        .iter()
        .filter(|receipt| receipt.receipt.committed_version == target)
        .collect::<Vec<_>>();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].replica_node_id, nodes[0]);
    assert_eq!(receipts[0].content_hash, before.content_hash);
    drop(primary);
    let second_deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let second = writer.write_distributed_eventual(
        sinomo_core::DfsWriteRequest {
            file_system_id: original.file_system_id.clone(),
            object_id: original.object_id.clone(),
            target_version: sinomo_dfs::DfsVersion::generate(),
            requesting_node_id: nodes[0].clone(),
        },
        b"competing writer".to_vec(),
        vec![nodes[0].clone(), nodes[2].clone()],
        DfsReplicationPolicy::new(2, 2).unwrap(),
    );
    let sinomo_dfs::NetworkedDfsWriteOutcome::Pending {
        intent_id: second_request_id,
        durable_receipts: 0,
        required_receipts: 2,
    } = second.unwrap()
    else {
        panic!("second writer registration does not prove admission");
    };
    assert_ne!(
        second_request_id, request_id,
        "a second writer has a fresh operation identity"
    );
    loop {
        assert!(
            std::time::Instant::now() < second_deadline,
            "actual second-writer rejection must fit the original network budget"
        );
        let rejected = writer
            .process_incoming()
            .unwrap()
            .into_iter()
            .find_map(|event| {
                if let sinomo_dfs::NetworkedDfsServiceEvent::RemoteWriteCompleted {
                    request_id,
                    object_id,
                    result,
                    ..
                } = event
                    && request_id == second_request_id
                    && object_id == original.object_id
                {
                    Some(result)
                } else {
                    None
                }
            });
        if let Some(result) = rejected {
            assert!(std::time::Instant::now() <= second_deadline);
            assert!(
                matches!(
                    result,
                    sinomo_dfs::DfsDistributedWriteResult::Rejected { .. }
                ),
                "actual pending custody excludes a second writer: {result:?}"
            );
            break;
        }
        std::thread::yield_now();
    }
    drop(writer);
    runtime.stop_node(&nodes[0]).unwrap();
    runtime.reopen_node(nodes[0].clone()).unwrap();
    let mut recovered = runtime.client(&nodes[0]).unwrap();
    let current_primary = runtime.root_shard().unwrap().primary_node_id;
    let inspected = runtime
        .rpc_request(
            &nodes[0],
            &current_primary,
            sinomo_dfs::dfs_inspection_rpc_request(
                &sinomo_dfs::DaemonDfsInspectionRequest::Object {
                    object_id: original.object_id.clone(),
                },
            )
            .unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    let sinomo_dfs::DaemonDfsInspectionResponse::Object {
        object: Some(actual),
    } = sinomo_dfs::decode_dfs_inspection_rpc_response(&inspected).unwrap()
    else {
        panic!("actual reopened resident owner must inspect the original object");
    };
    let actual = actual
        .intents
        .into_iter()
        .find(|intent| intent.intent_id == intent_id)
        .unwrap();
    assert_eq!(
        actual, before,
        "restart retains the exact pending identity and captured content"
    );
    assert_eq!(recovered.posix_read_file(path).unwrap(), b"original");
    // Restart has no private requester registry. Reuse the original immutable
    // durable locator through the existing public authenticated Main route.
    let abort = recovered
        .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
            format!("abort:{}", blake3::hash(intent_id.as_bytes()).to_hex()),
            before.file_system_id.clone(),
            current_primary,
            sinomo_dfs::DfsNamespaceOperation::AbortPendingWrite {
                path: path.into(),
                object_id: before.object_id.clone(),
                target_version: before.target_version.clone(),
                intent_id: before.intent_id.clone(),
                write_operation_id: before
                    .operation_id
                    .clone()
                    .expect("actual original write identity"),
            },
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            false,
            false,
        )
        .unwrap();
    assert!(matches!(
        abort.result,
        sinomo_dfs::DfsNamespaceResult::Applied
    ));
    let Some(sinomo_dfs::DfsPosixCoordinationResult::AbortedWriteIntent(terminal)) =
        abort.posix_coordination_result
    else {
        panic!("actual abort must return its typed durable outcome");
    };
    assert_eq!(terminal.intent_id, before.intent_id);
    assert_eq!(terminal.file_system_id, before.file_system_id);
    assert_eq!(terminal.object_id, before.object_id);
    assert_eq!(terminal.target_version, before.target_version);
    assert_eq!(terminal.requesting_node_id, nodes[0]);
    assert_eq!(terminal.operation_id, before.operation_id);
    assert_eq!(terminal.state, sinomo_dfs::DfsWriteIntentState::Aborted);
    let current_primary = runtime.root_shard().unwrap().primary_node_id;
    let inspected = runtime
        .rpc_request(
            &nodes[0],
            &current_primary,
            sinomo_dfs::dfs_inspection_rpc_request(
                &sinomo_dfs::DaemonDfsInspectionRequest::Object {
                    object_id: original.object_id.clone(),
                },
            )
            .unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    let sinomo_dfs::DaemonDfsInspectionResponse::Object {
        object: Some(actual),
    } = sinomo_dfs::decode_dfs_inspection_rpc_response(&inspected).unwrap()
    else {
        panic!("actual resident owner must retain the aborted intent and original object");
    };
    assert_eq!(
        actual
            .intents
            .iter()
            .find(|intent| intent.intent_id == intent_id)
            .unwrap()
            .status,
        sinomo_dfs::DfsWriteIntentStatus::Aborted
    );
    assert_eq!(recovered.posix_read_file(path).unwrap(), b"original");
    drop(recovered);
    runtime.close().unwrap();
}

#[test]
fn native_create_seals_parent_version_content_and_terminal_intent_in_one_visibility_body() {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let node = DfsTestRuntime::ordered_node_ids("native-atomic-create-body", 1).remove(0);
    let mut runtime = DfsTestRuntime::new("native-atomic-create-body", node.clone()).unwrap();
    let trace = sinomo_dfs::test_support::NativeShardTraceArtifactSession::from_environment(
        "native-atomic-create-body",
        [(node.clone(), runtime.data_dir(&node).root().to_owned())],
        DfsMetadataShardId::new("root"),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let mut writer = runtime.client(&node).unwrap();
    let fs_id = runtime.file_system().file_system_id;
    let root = DfsMetadataShardId::new("root");
    let parent = writer
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&fs_id, "/")
        .unwrap()
        .unwrap();
    let before = writer
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&root)
        .unwrap()
        .unwrap()
        .prefix_proof
        .journal_sequence;
    let bytes = b"single complete visibility body";
    let publication = writer.posix_write_file(
        "/native-atomic.bin",
        bytes,
        sinomo_dfs::DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![node],
            wait_for_commit: true,
            foreground_replica_repair: false,
        },
    );
    let stat = match publication {
        Ok(stat) => stat,
        Err(error) => {
            drop(writer);
            let capture_close = trace.as_ref().map(|trace| trace.close()).transpose();
            let runtime_close = runtime.close();
            let export = trace.map(|trace| trace.export()).transpose();
            capture_close.expect("close genuine failed-publication capture");
            runtime_close.expect("close native runtime after rejected publication");
            export.expect("export actual rejected-publication trace before asserting");
            panic!("native publication failed: {error:?}");
        }
    };
    let batch = writer
        .metadata_backend()
        .unwrap()
        .journal_batch_for_shard_since_limited(
            &root,
            before,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )
        .unwrap()
        .unwrap();
    let bodies = batch
        .entries
        .iter()
        .filter_map(|entry| match &entry.mutation {
            DfsMetadataMutation::PrepareOperationV7 { record, .. }
                if record.payload.transaction.objects.iter().any(|object| {
                    object.object_id == stat.object_id && object.version == stat.version
                }) =>
            {
                Some(record)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        bodies.len(),
        1,
        "one sealed visibility body owns the complete create"
    );
    let body = bodies[0];
    assert!(
        body.payload
            .transaction
            .objects
            .iter()
            .any(|object| object.object_id == parent.object_id && object.version != parent.version)
    );
    assert!(
        body.payload
            .transaction
            .versions
            .iter()
            .any(|version| version.object_id == stat.object_id
                && version.version == stat.version
                && version.size_bytes == Some(bytes.len() as u64)
                && version.content_hash == stat.content_hash)
    );
    let terminal = body
        .payload
        .terminal_intent
        .as_ref()
        .expect("sealed terminal intent");
    assert_eq!(terminal.status, sinomo_dfs::DfsWriteIntentStatus::Committed);
    assert_eq!(terminal.target_version, stat.version);
    assert_eq!(body.payload.content_receipts.len(), 1);
    let commits = batch.entries.iter().filter(|entry| matches!(&entry.mutation,
        DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } if digest == &body.digest
    )).count();
    assert_eq!(
        commits, 1,
        "the visibility commit references that exact sealed digest"
    );
    assert_eq!(writer.posix_read_file("/native-atomic.bin").unwrap(), bytes);
    drop(writer);
    if let Some(trace) = &trace {
        trace.close().unwrap();
    }
    runtime.close().unwrap();
    if let Some(trace) = trace {
        trace.export().unwrap();
    }
}

#[test]
fn native_bounded_journal_pages_reconstruct_the_actual_durable_suffix_without_mutation() {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let node = DfsTestRuntime::ordered_node_ids("native-bounded-journal", 1).remove(0);
    let mut runtime = DfsTestRuntime::new("native-bounded-journal", node.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let mut writer = runtime.client(&node).unwrap();
    for index in 0..3 {
        writer
            .posix_write_file(
                &format!("/native-bounded-{index}.bin"),
                &[index],
                sinomo_dfs::DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![node.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
    }
    let root = DfsMetadataShardId::new("root");
    let backend = writer.metadata_backend().unwrap();
    let before = backend.export_shard_table_snapshot(&root).unwrap().unwrap();
    let all = backend
        .journal_batch_for_shard_since_limited(&root, 0, usize::MAX, usize::MAX)
        .unwrap()
        .unwrap();
    assert!(
        all.entries.len() > 3,
        "exercise several real prepare and commit pages"
    );
    let mut after = 0;
    let mut assembled = Vec::new();
    loop {
        let Some(page) = backend
            .journal_batch_for_shard_since_limited(
                &root,
                after,
                1,
                sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .unwrap()
        else {
            break;
        };
        if page.entries.is_empty() {
            break;
        }
        assert_eq!(page.entries.len(), 1);
        assert!(page.next_after_sequence > after);
        after = page.next_after_sequence;
        assembled.extend(page.entries);
    }
    assert_eq!(assembled, all.entries);
    assert_eq!(after, all.next_after_sequence);
    let actual = backend.export_shard_table_snapshot(&root).unwrap().unwrap();
    assert_eq!(
        actual, before,
        "bounded observation must not author or rematerialize state"
    );
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn native_metadata_authority_dispatches_the_actual_replica_set_before_waiting() {
    use sinomo_dfs::test_support::{
        DfsTestFaultAction, DfsTestFaultRule, DfsTestRpcPhase, DfsTestRuntime,
    };
    let nodes = DfsTestRuntime::ordered_node_ids("native-authority-dispatch", 3);
    let mut runtime = DfsTestRuntime::new("native-authority-dispatch", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let targets = shard
        .replica_node_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(targets.len(), 2, "exercise a real full-shard fanout");
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    for target in &targets {
        runtime.install_fault(DfsTestFaultRule {
            source: Some(primary.clone()),
            target: Some(target.clone()),
            route: Some(sinomo_dfs::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: DfsTestFaultAction::PauseBeforeDispatch(gate.clone()),
        });
    }
    let baseline = runtime.rpc_observations().len();
    let mut writer = runtime.client(&primary).unwrap();
    let path = "/native-authority-dispatch.bin";
    let bytes = b"normal Main metadata authority dispatch witness";
    let content_owner = primary.clone();
    std::thread::scope(|scope| {
        let publication = scope.spawn(move || {
            writer.posix_write_file(
                path,
                bytes,
                sinomo_dfs::DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![content_owner],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(900);
        let mut dispatched = std::collections::BTreeSet::new();
        while std::time::Instant::now() < deadline {
            for event in runtime.rpc_observations().into_iter().skip(baseline) {
                if event.source == primary
                    && event.route == sinomo_dfs::DFS_METADATA_JOURNAL_BATCH_ROUTE
                    && event.phase == DfsTestRpcPhase::BeforeDispatch
                    && targets.contains(&event.target)
                {
                    dispatched.insert(event.target);
                }
            }
            if dispatched == targets {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        runtime.clear_faults();
        gate.add_permits(64);
        let result = publication.join().expect("native publication worker");
        assert_eq!(
            dispatched, targets,
            "one paused replica must not serialize dispatch to the others"
        );
        result.expect("actual native receivers authorize the full publication after release");
    });
    assert_eq!(runtime.observation_dropped_count(), 0);
    let mut reader = runtime.client(&primary).unwrap();
    assert_eq!(reader.posix_read_file(path).unwrap(), bytes);
    drop(reader);
    runtime.close().unwrap();
}

struct PatternReader {
    remaining: usize,
    index: usize,
}

impl PatternReader {
    fn new(bytes: usize) -> Self {
        Self {
            remaining: bytes,
            index: 0,
        }
    }
}

impl Read for PatternReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let to_fill = buf.len().min(self.remaining);
        for byte in &mut buf[..to_fill] {
            *byte = (self.index % 251) as u8;
            self.index = self.index.saturating_add(1);
        }
        self.remaining -= to_fill;
        Ok(to_fill)
    }
}

fn pattern_bytes(bytes: usize) -> Vec<u8> {
    (0..bytes).map(|index| (index % 251) as u8).collect()
}

fn native_three_shards(
    name: &str,
) -> (
    sinomo_dfs::test_support::DfsTestRuntime,
    Vec<sinomo_core::node::NodeId>,
) {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 3);
    let mut runtime =
        sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    // Startup certifies the current assignment. Establish this cross-node
    // scenario through ordinary exact-prefix handoff, never a forced KV owner.
    for (shard_id, target) in [("services", &nodes[1]), ("users", &nodes[2])] {
        let current = runtime.metadata_shards().unwrap();
        let previous = current
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .unwrap();
        let replicas = nodes
            .iter()
            .filter(|node| *node != target)
            .cloned()
            .collect::<Vec<_>>();
        let installed = runtime
            .reassign_metadata_shard(&previous.file_system_id, shard_id, target.clone(), replicas)
            .expect("normal current membership/primary handoff");
        assert_eq!(installed.primary_node_id, *target);
        assert!(installed.epoch >= previous.epoch);
        if installed != *previous {
            let control = runtime.shared_control_plane().unwrap();
            control.with_client(|kv| {
                let mut store = sinomo_dfs::DfsControlPlaneStore::new(kv.as_mut());
                assert!(
                    store
                        .get_metadata_shard_membership_handoff(&installed.file_system_id, shard_id)
                        .unwrap()
                        .is_none()
                );
                let proof = store
                    .get_metadata_shard_membership_handoff_proof(
                        &installed.file_system_id,
                        shard_id,
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(proof.desired_shard, installed);
                assert_eq!(proof.retiring_shard, *previous);
            });
        }
    }
    let shards = runtime.metadata_shards().unwrap();
    let services = shards
        .iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let users = shards
        .iter()
        .find(|shard| shard.shard_id == "users")
        .unwrap();
    assert_ne!(services.shard_id, users.shard_id);
    assert_ne!(
        services.primary_node_id, users.primary_node_id,
        "the actual completed native handoffs establish distinct real authorities"
    );
    (runtime, nodes)
}

#[test]
fn native_cross_shard_rename_keeps_identity_bytes_and_retry_after_restart() {
    let (mut runtime, nodes) = native_three_shards("native-cross-shard-rename");
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let from = "/services/native-move.bin";
    let to = "/users/native-move.bin";
    let bytes = b"cross-shard bytes with one canonical owner";
    let options = sinomo_dfs::DfsWriteOptions::replicated_eventual(nodes.clone());
    let original = writer.posix_write_file(from, bytes, options).unwrap();
    writer
        .posix_sync_file(from, sinomo_dfs::DfsPosixSyncKind::File)
        .unwrap();
    let operation = sinomo_core::Id::generate().to_string();
    let moved = writer
        .posix_rename_with_operation_id(from, to, &operation)
        .expect("normal fenced source/target adoption");
    assert_eq!(moved.object_id, original.object_id);
    assert_eq!(moved.content_hash, original.content_hash);
    assert!(matches!(
        writer.posix_stat_distributed(from),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    assert_eq!(writer.posix_read_file_distributed(to).unwrap(), bytes);
    let replay = writer
        .posix_rename_with_operation_id(from, to, &operation)
        .expect("exact normal retry");
    assert_eq!(replay.object_id, moved.object_id);
    assert_eq!(replay.version, moved.version);
    drop(writer);
    for node in &nodes {
        runtime.stop_node(node).unwrap();
    }
    for node in &nodes {
        runtime.reopen_node(node.clone()).unwrap();
    }
    let mut reopened = runtime.client(&nodes[2]).unwrap();
    let actual = reopened.posix_stat_distributed(to).unwrap();
    assert_eq!(actual.object_id, moved.object_id);
    assert_eq!(actual.version, moved.version);
    assert_eq!(reopened.posix_read_file_distributed(to).unwrap(), bytes);
    assert!(matches!(
        reopened.posix_stat_distributed(from),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    drop(reopened);
    runtime.close().unwrap();
}

#[test]
fn native_cross_shard_hardlink_keeps_target_after_canonical_unlink() {
    let (runtime, nodes) = native_three_shards("native-cross-shard-hardlink");
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let canonical = "/services/native-link-target.bin";
    let alias = "/users/native-link-alias.bin";
    let options = sinomo_dfs::DfsWriteOptions::replicated_eventual(nodes.clone());
    let original = writer
        .posix_write_file(canonical, b"original hardlink bytes", options.clone())
        .unwrap();
    writer
        .posix_sync_file(canonical, sinomo_dfs::DfsPosixSyncKind::File)
        .unwrap();
    writer
        .posix_hardlink(canonical, alias)
        .expect("real cross-shard target/alias fences");
    assert_eq!(
        writer.posix_stat_distributed(alias).unwrap().object_id,
        original.object_id
    );
    assert_eq!(
        writer.posix_read_file_distributed(alias).unwrap(),
        b"original hardlink bytes"
    );
    let updated = b"updated through the surviving alias";
    writer.posix_write_file(alias, updated, options).unwrap();
    assert_eq!(
        writer.posix_read_file_distributed(canonical).unwrap(),
        updated
    );
    writer.posix_unlink(canonical).unwrap();
    assert!(matches!(
        writer.posix_stat_distributed(canonical),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    let surviving = writer.posix_stat_distributed(alias).unwrap();
    assert_eq!(surviving.object_id, original.object_id);
    assert_eq!(surviving.kind, sinomo_dfs::DfsObjectKind::File);
    assert_eq!(surviving.content_hash, Some(content_hash(updated)));
    assert_eq!(surviving.size_bytes, Some(updated.len() as u64));
    assert_eq!(surviving.link_count, 1);
    assert_eq!(writer.posix_read_file_distributed(alias).unwrap(), updated);
    drop(writer);
    let mut reader = runtime.client(&nodes[2]).unwrap();
    assert_eq!(reader.posix_read_file_distributed(alias).unwrap(), updated);
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn native_cross_shard_move_keeps_capture_and_recorded_data_generation() {
    let (runtime, nodes) = native_three_shards("native-cross-shard-capture");
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let from = "/services/native-captured.bin";
    let to = "/users/native-captured.bin";
    let options = sinomo_dfs::DfsWriteOptions::replicated_eventual(nodes.clone());
    let original = writer
        .posix_write_file(from, b"captured bytes", options.clone())
        .unwrap();
    writer
        .posix_sync_file(from, sinomo_dfs::DfsPosixSyncKind::File)
        .unwrap();
    let request = sinomo_dfs::DfsContentReferenceRequest {
        namespace: "module.tests".into(),
        path: from.into(),
        identity: sinomo_dfs::DfsRevisionReference {
            file_system_id: runtime.file_system().file_system_id,
            object_id: original.object_id.clone(),
            version: original.version.clone(),
        },
        operation: sinomo_dfs::DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "before-move".into(),
            metadata: None,
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    writer
        .retain_content_revision(request.clone())
        .expect("normal immutable reference capture");
    writer
        .posix_write_file(from, b"new current bytes", options)
        .unwrap();
    let mode = writer
        .posix_set_mode(from, 0o600)
        .expect("metadata generation keeps genuine data evidence");
    let moved = writer.posix_rename(from, to).unwrap();
    assert_eq!(moved.object_id, original.object_id);
    assert_eq!(moved.mode, mode.mode);
    assert_eq!(
        writer.posix_read_file_distributed(to).unwrap(),
        b"new current bytes"
    );
    let mut located = request;
    located.path = to.into();
    assert!(
        writer
            .content_reference_present(&located)
            .expect("capture follows object identity")
    );
    let retained = writer
        .metadata_backend()
        .unwrap()
        .get_version(&original.object_id, &original.version)
        .unwrap()
        .unwrap();
    assert!(retained.has_retention_grant());
    let data = retained
        .content_durability
        .as_ref()
        .expect("recorded data-generation durability");
    assert_eq!(data.source_version, original.version);
    assert_eq!(retained.content_hash, original.content_hash);
    drop(writer);
    runtime.close().unwrap();
}

#[test]
fn native_replica_rejects_wrong_signer_filesystem_and_epoch_without_mutating_its_cut() {
    let (runtime, nodes) = native_three_shards("native-replica-negative-scope");
    let source = runtime.root_shard().unwrap();
    let shard = DfsMetadataShardId::new(source.shard_id.clone());
    let target = source.replica_node_ids[0].clone();
    let client = runtime.client(&source.primary_node_id).unwrap();
    let backend = client.metadata_backend().unwrap();
    let decoded = backend
        .journal_batch_for_shard_since_limited(&shard, 0, 256, 16 * 1024 * 1024)
        .unwrap()
        .unwrap();
    let frames = sinomo_dfs::DfsMetadataJournalFrameBatch {
        shard_id: shard.clone(),
        next_after_sequence: decoded.next_after_sequence,
        journal_frames: backend.journal_frames_for_batch(&decoded).unwrap(),
    };
    assert!(!frames.journal_frames.is_empty());
    let command =
        sinomo_dfs::test_support::native_replica_journal_command(&source, &target, frames);
    drop(client);
    let target_client = runtime.client(&target).unwrap();
    let before = target_client
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard)
        .unwrap()
        .unwrap();
    drop(target_client);
    for mismatch in 0..3 {
        let mut invalid = command.clone();
        match mismatch {
            0 => {
                invalid.source_node_id = Some(
                    nodes
                        .iter()
                        .find(|node| **node != source.primary_node_id)
                        .unwrap()
                        .clone(),
                )
            }
            1 => invalid.file_system_id = Some(sinomo_dfs::DfsFileSystemId::generate()),
            2 => invalid.authority_epoch = Some(source.epoch.saturating_sub(1)),
            _ => unreachable!(),
        }
        let request = sinomo_dfs::dfs_metadata_journal_batch_rpc_request(&invalid).unwrap();
        let reply = runtime
            .rpc_request(
                &source.primary_node_id,
                &target,
                request,
                std::time::Duration::from_secs(5),
            )
            .unwrap();
        let outcome = sinomo_dfs::decode_dfs_metadata_journal_batch_rpc_response(&reply).unwrap();
        assert!(
            matches!(
                outcome.result,
                sinomo_dfs::DfsMetadataJournalBatchResult::Rejected { .. }
            ),
            "mismatch {mismatch} must reject"
        );
        let actual = runtime.client(&target).unwrap();
        let after = actual
            .metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&shard)
            .unwrap()
            .unwrap();
        assert_eq!(after.prefix_proof, before.prefix_proof);
        assert_eq!(
            after.semantic_state_fingerprint,
            before.semantic_state_fingerprint
        );
    }
    runtime.close().unwrap();
}

#[test]
fn native_unknown_adoption_retains_source_owner_until_exact_reopen_reconciliation() {
    use sinomo_dfs::test_support::{
        DfsTestFaultAction as Action, DfsTestFaultRule as Rule, DfsTestRpcOrigin as Origin,
        DfsTestSourceAdoptionFaultPhase as SourcePhase,
    };
    let (mut runtime, nodes) = native_three_shards("native-unknown-adoption-source-owner");
    let assignments = runtime.metadata_shards().unwrap();
    let source = assignments
        .iter()
        .find(|cfg| cfg.shard_id == "services")
        .unwrap()
        .primary_node_id
        .clone();
    let target = assignments
        .iter()
        .find(|cfg| cfg.shard_id == "users")
        .unwrap()
        .primary_node_id
        .clone();
    let from = "/services/unknown-source.bin";
    let to = "/users/unknown-target.bin";
    let bytes = b"native retained custody and exact outcome";
    let mut requester = runtime.client(&nodes[0]).unwrap();
    let original = requester
        .posix_write_file(
            from,
            bytes,
            sinomo_dfs::DfsWriteOptions::replicated_eventual(nodes.clone()),
        )
        .unwrap();
    requester
        .posix_sync_file(from, sinomo_dfs::DfsPosixSyncKind::File)
        .unwrap();
    let pause = runtime.pause_remote_namespace_phase(&source).unwrap();
    let operation = sinomo_core::Id::generate().to_string();
    let context = sinomo_module::ComponentContext::new(std::sync::Arc::new(
        runtime.data_dir(&source).clone(),
    ))
    .with_host_role(sinomo_module::ComponentHostRole::Embedded);
    let actual_owner = sinomo_dfs::runtime_host::retain_component_data_owner(&context).unwrap();
    let old_owner = std::sync::Arc::downgrade(&actual_owner);
    drop(actual_owner);
    drop(context);
    std::thread::scope(|scope| {
        let op = operation.clone();
        let foreground =
            scope.spawn(move || requester.posix_rename_with_operation_id(from, to, &op));
        let captured = pause
            .wait_captured(std::time::Duration::from_secs(5))
            .unwrap();
        let sinomo_dfs::DfsNetworkMessage::NamespaceRequest(command) =
            &captured.routed_message.message
        else {
            panic!("native adoption phase");
        };
        let sinomo_dfs::DfsNamespaceOperation::AdoptObjectAtPath { source_offer, .. } =
            &command.operation
        else {
            panic!("actual source offer");
        };
        assert_eq!(source_offer.source_node_id, source);
        assert_eq!(source_offer.object_id, original.object_id);
        assert_eq!(captured.routed_message.target_node_id, target);
        for (sender, receiver, phase) in [
            (&target, &source, SourcePhase::Release),
            (&source, &target, SourcePhase::InspectOutcome),
        ] {
            runtime.install_fault(Rule {
                source: Some(sender.clone()),
                target: Some(receiver.clone()),
                route: Some("/dfs/source-adoption-v7".into()),
                call_id: None,
                origin: Some(Origin::RegisteredHost),
                remaining_hits: 1,
                action: Action::BudgetExhaustedForSourcePhase(phase),
            });
        }
        runtime.install_fault(Rule {
            source: Some(source.clone()),
            target: Some(target.clone()),
            route: Some(sinomo_dfs::DFS_DATA_NAMESPACE_ROUTE.into()),
            call_id: None,
            origin: Some(Origin::RegisteredHost),
            remaining_hits: 1,
            action: Action::LoseReplyAfterDispatch,
        });
        pause.release();
        let outcome = foreground.join().unwrap();
        assert!(
            outcome.is_err(),
            "the actual committed receiver's reply was lost"
        );
    });
    drop(pause);
    // Read the actual published target before stopping the source. This proves
    // a durable effect despite the foreground's uncertain result.
    let mut target_reader = runtime.client(&target).unwrap();
    let committed = target_reader.posix_stat_distributed(to).unwrap();
    assert_eq!(committed.object_id, original.object_id);
    assert_eq!(
        target_reader.posix_read_file_distributed(to).unwrap(),
        bytes
    );
    drop(target_reader);
    runtime.stop_node(&source).unwrap();
    assert!(
        old_owner.upgrade().is_some(),
        "native actor retains its original actual owner after source scope stop"
    );
    let paths = sinomo_dfs::runtime_host::DfsRuntimePaths::new(runtime.data_dir(&source));
    assert!(
        matches!(
            sinomo_dfs::runtime_host::DfsDataOwnerLock::acquire(&paths),
            Err(sinomo_dfs::runtime_host::DfsRuntimeError::AlreadyRunning { .. })
        ),
        "old native custody must still fence its physical volume"
    );
    runtime.clear_faults();
    runtime.reopen_node(source.clone()).unwrap();
    let mut resumed = runtime.client(&nodes[0]).unwrap();
    let actual = resumed
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    assert_eq!(actual.object_id, committed.object_id);
    assert_eq!(actual.version, committed.version);
    assert_eq!(resumed.posix_read_file_distributed(to).unwrap(), bytes);
    assert!(matches!(
        resumed.posix_stat_distributed(from),
        Err(sinomo_dfs::DfsPosixError::NotFound { .. })
    ));
    drop(resumed);
    assert_eq!(runtime.observation_dropped_count(), 0);
    runtime.close().unwrap();
}

#[test]
fn native_cross_shard_directory_children_keep_identity_and_retry_after_restart() {
    let (mut runtime, nodes) = native_three_shards("native-directory-source-frames");
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let from = "/services/native-tree";
    let to = "/users/native-tree";
    writer.posix_mkdir_all(&format!("{from}/nested")).unwrap();
    let children = [
        ("first.bin", b"first real bytes".as_slice()),
        ("nested/second.bin", b"second real bytes".as_slice()),
    ];
    let mut original = Vec::new();
    for (suffix, bytes) in children {
        let path = format!("{from}/{suffix}");
        original.push(
            writer
                .posix_write_file(
                    &path,
                    bytes,
                    sinomo_dfs::DfsWriteOptions::replicated_eventual(nodes.clone()),
                )
                .unwrap(),
        );
        writer
            .posix_sync_file(&path, sinomo_dfs::DfsPosixSyncKind::File)
            .unwrap();
    }
    let operation = sinomo_core::Id::generate().to_string();
    let moved = writer
        .posix_rename_with_operation_id(from, to, &operation)
        .unwrap();
    for ((suffix, bytes), old) in children.iter().zip(&original) {
        let path = format!("{to}/{suffix}");
        let actual = writer.posix_stat_distributed(&path).unwrap();
        assert_eq!(actual.object_id, old.object_id);
        assert_eq!(actual.content_hash, old.content_hash);
        assert_eq!(writer.posix_read_file_distributed(&path).unwrap(), *bytes);
        assert!(matches!(
            writer.posix_stat_distributed(&format!("{from}/{suffix}")),
            Err(sinomo_dfs::DfsPosixError::NotFound { .. })
        ));
    }
    assert_eq!(
        writer
            .posix_rename_with_operation_id(from, to, &operation)
            .unwrap()
            .version,
        moved.version
    );
    drop(writer);
    for node in &nodes {
        runtime.stop_node(node).unwrap();
    }
    for node in &nodes {
        runtime.reopen_node(node.clone()).unwrap();
    }
    let mut reader = runtime.client(&nodes[2]).unwrap();
    for ((suffix, bytes), old) in children.iter().zip(&original) {
        let path = format!("{to}/{suffix}");
        assert_eq!(
            reader.posix_stat_distributed(&path).unwrap().object_id,
            old.object_id
        );
        assert_eq!(reader.posix_read_file_distributed(&path).unwrap(), *bytes);
    }
    drop(reader);
    assert_eq!(runtime.observation_dropped_count(), 0);
    runtime.close().unwrap();
}

#[test]
fn native_whole_shard_healthy_w2_capture_uses_actual_receiver_and_full_metadata_proof() {
    use sinomo_dfs::test_support::{DfsTestRuntime, NativeShardTraceArtifactSession};
    let nodes = DfsTestRuntime::ordered_node_ids("native-whole-shard-healthy-w2", 3);
    let mut runtime =
        DfsTestRuntime::new("native-whole-shard-healthy-w2", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    let trace = NativeShardTraceArtifactSession::from_environment(
        "native-whole-shard-healthy-w2",
        nodes
            .iter()
            .map(|node| (node.clone(), runtime.data_dir(node).root().to_owned())),
        DfsMetadataShardId::new("root"),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let cfg = runtime.root_shard().unwrap();
    let mut client = runtime.client(&cfg.primary_node_id).unwrap();
    let bytes = b"actual receiver durable return and native metadata";
    let stat = client
        .posix_write_file(
            "/native-whole-shard-w2.bin",
            bytes,
            sinomo_dfs::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(
        client
            .posix_read_file("/native-whole-shard-w2.bin")
            .unwrap(),
        bytes
    );
    let (receipts, snapshot) = {
        // The write ran on Main. Observe its current CoreEffect metadata owner
        // instead of the delegated caller's earlier private snapshot.
        let actual_primary = runtime.metadata_primary_service(&stat.path).unwrap();
        let backend = actual_primary.metadata_backend().unwrap();
        (
            backend
                .receipts_for_version(&stat.object_id, &stat.version)
                .unwrap(),
            backend
                .export_shard_table_snapshot(&DfsMetadataShardId::new("root"))
                .unwrap()
                .unwrap(),
        )
    };
    assert!(receipts.len() >= 2);
    let committed = snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .find(|record| {
            record.status == sinomo_dfs::DfsPreparedOperationStatus::Committed
                && record.payload.transaction.objects.iter().any(|object| {
                    object.object_id == stat.object_id && object.version == stat.version
                })
        })
        .expect("actual complete committed visibility body");
    let expected_hash = stat.content_hash.as_deref().unwrap();
    for receipt in &receipts {
        assert_eq!(receipt.receipt.file_system_id, cfg.file_system_id);
        assert_eq!(receipt.receipt.object_id, stat.object_id);
        assert_eq!(receipt.receipt.committed_version, stat.version);
        assert_eq!(receipt.content_hash.as_deref(), Some(expected_hash));
        assert!(
            committed
                .payload
                .content_receipts
                .iter()
                .any(|actual| actual == receipt),
            "the holder receipt belongs to this exact immutable committed body"
        );
        let holder = runtime.client(&receipt.replica_node_id).unwrap();
        // The normal coordinator may persist a hash-addressed blob without a
        // version manifest. The real receipt supplies its exact data identity;
        // this local read cannot fetch or borrow another holder's bytes.
        let persisted = holder.content().read_blob(expected_hash).unwrap();
        assert_eq!(persisted, bytes);
        assert_eq!(persisted.len() as u64, stat.size_bytes.unwrap());
        assert_eq!(content_hash(&persisted), expected_hash);
        drop(holder);
    }
    drop(client);
    let primary_data_root = runtime.data_dir(&cfg.primary_node_id).root().to_owned();
    if let Some(trace) = &trace {
        trace.close().unwrap();
    }
    runtime.close().unwrap();
    if let Some(trace) = trace {
        let output = trace.export().unwrap();
        // Decode only the bounded KV facts; the existing serde reader skips
        // all large kernel/held-byte arrays in this SAME closed raw artifact.
        #[derive(serde::Deserialize)]
        struct ActualKvFacts {
            actual_mount_root: std::path::PathBuf,
            actual_mount_label: String,
            closed: bool,
            actual_owner_stopped: bool,
            in_flight: String,
            dropped: String,
            incomplete: String,
            original_control_commits: Vec<serde_json::Value>,
            control_commits: Vec<serde_json::Value>,
        }
        let actual: ActualKvFacts = serde_json::from_reader(std::io::BufReader::new(
            std::fs::File::open(output.join(format!("{}.raw.json", cfg.primary_node_id))).unwrap(),
        ))
        .unwrap();
        assert_eq!(actual.actual_mount_root, primary_data_root);
        assert!(actual.closed && actual.actual_owner_stopped);
        assert_eq!(
            (
                actual.in_flight.as_str(),
                actual.dropped.as_str(),
                actual.incomplete.as_str()
            ),
            ("0", "0", "0")
        );
        let segment = cfg.shard_id.trim_matches('/').replace('/', "_");
        let pending_key = format!(
            "/dfs/filesystems/{}/metadata-membership-handoffs/{segment}",
            cfg.file_system_id
        );
        let proof_key = format!(
            "/dfs/filesystems/{}/metadata-membership-handoff-proofs/{segment}",
            cfg.file_system_id
        );
        let root = primary_data_root.join("cluster");
        assert!(actual.control_commits.iter().any(|call| {
            let request = &call["commit"]["request"];
            let result = &call["commit"]["result"];
            let quorum = &result["authority"]["basis"]["current-term-quorum"];
            let handoff = request["txn"]["success"].as_array().unwrap().iter().any(|op| {
                op.get("Put").and_then(|put| put["key"].as_str())
                    .is_some_and(|key| key == pending_key || key == proof_key)
            });
            if !handoff || result["replayed_committed_request"] != false
                || result["txn_response"]["succeeded"] != true
                || request["request_id"] != result["request_id"]
                || result["authority"]["term"].as_u64().unwrap() == 0
                || result["authority"]["commit_index"].as_u64().unwrap() == 0
                || !quorum.is_object()
                || quorum["required_quorum"].as_u64().unwrap() == 0
                || quorum["voter_acks"].as_u64().unwrap() < quorum["required_quorum"].as_u64().unwrap()
                || result.get("submitted_command_digest").is_none_or(|digest| digest.is_null())
                || result["submitted_command_digest"] != result["command_digest"]
            { return false; }
            actual.original_control_commits.iter().any(|observed| {
                let original = &observed["original"];
                let earlier = observed["owner_ordinal"].as_str().unwrap().parse::<u64>().unwrap()
                    < call["owner_ordinal"].as_str().unwrap().parse::<u64>().unwrap();
                earlier
                    && observed["owner_label"] == actual.actual_mount_label
                    && call["owner_label"] == actual.actual_mount_label
                    && original["root"].as_str() == Some(root.to_str().unwrap())
                    && original["node_id"].as_str() == Some(cfg.primary_node_id.as_str())
                    && original["request"] == serde_json::json!({
                        "request_id": request["request_id"], "command": {"Txn": request["txn"]}})
                    && original["authority"] == result["authority"]
                    && original["committed"] == serde_json::json!({
                        "request_id": result["request_id"], "command_digest": result["command_digest"],
                        "term": result["authority"]["term"], "log_index": result["authority"]["commit_index"],
                        "revision": result["authority"]["revision"], "txn_response": result["txn_response"]})
            })
        }), "normal handoff must capture an earlier OWN original certificate with its exact full caller tuple");
    }
}

#[test]
fn native_whole_shard_partial_metadata_probe_uses_actual_operation_bound_native_permit() {
    use sinomo_dfs::test_support::{
        DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime, NativeShardTraceArtifactSession,
    };
    let nodes = DfsTestRuntime::ordered_node_ids("native-whole-shard-degraded", 3);
    let mut runtime = DfsTestRuntime::new("native-whole-shard-degraded", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    let trace = NativeShardTraceArtifactSession::from_environment(
        "native-whole-shard-degraded",
        nodes
            .iter()
            .map(|node| (node.clone(), runtime.data_dir(node).root().to_owned())),
        DfsMetadataShardId::new("root"),
    )
    .unwrap();
    runtime.start_nodes().unwrap();
    let cfg = runtime.root_shard().unwrap();
    assert_eq!(cfg.replica_node_ids.len(), 2);
    let unavailable = cfg.replica_node_ids.last().unwrap().clone();
    for route in [
        sinomo_dfs::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        sinomo_dfs::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    ] {
        runtime.install_fault(DfsTestFaultRule {
            source: Some(cfg.primary_node_id.clone()),
            target: Some(unavailable.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: DfsTestFaultAction::BudgetExhausted,
        });
    }
    let mut client = runtime.client(&cfg.primary_node_id).unwrap();
    let bytes = b"real partial metadata probe and native permit";
    let stat = client
        .posix_write_file(
            "/native-whole-shard-degraded.bin",
            bytes,
            sinomo_dfs::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![cfg.primary_node_id.clone()],
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    assert_eq!(
        client
            .posix_read_file("/native-whole-shard-degraded.bin")
            .unwrap(),
        bytes
    );
    let shard = DfsMetadataShardId::new("root");
    // Main published this operation through the real receiver path. The
    // requester's cached shard view is not its current durable body inventory.
    let snapshot = runtime
        .metadata_primary_service("/native-whole-shard-degraded.bin")
        .unwrap()
        .metadata_backend()
        .unwrap()
        .export_shard_table_snapshot(&shard)
        .unwrap()
        .unwrap();
    let record =
        snapshot
            .tables
            .prepared_operations_v7
            .iter()
            .find(|record| {
                record.payload.transaction.objects.iter().any(|object| {
                    object.object_id == stat.object_id && object.version == stat.version
                })
            })
            .unwrap();
    assert_eq!(
        record.status,
        sinomo_dfs::DfsPreparedOperationStatus::Committed
    );
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ActualOperationFloor {
        operation_digest: String,
        prepared_sequence: u64,
        authority_epoch: u64,
        permit_id: String,
    }
    let control = runtime.shared_control_plane().unwrap();
    let entry = control
        .with_client(|kv| {
            kv.get(&sinomo_dfs::metadata_shard_operation_floor_key(
                &cfg.file_system_id,
                "root",
            ))
        })
        .unwrap()
        .unwrap();
    let floor: ActualOperationFloor = sinomo_core::object::serde_ext::from_bytes(&entry.value)
        .expect("decode the actual committed operation floor");
    assert!(!floor.permit_id.is_empty());
    assert_eq!(floor.operation_digest, record.digest.payload_digest);
    assert_eq!(floor.prepared_sequence, record.digest.prepare_sequence);
    assert_eq!(floor.authority_epoch, record.digest.owner_epoch);
    drop(client);
    if let Some(trace) = &trace {
        trace.close().unwrap();
    }
    runtime.close().unwrap();
    if let Some(trace) = trace {
        trace.export().unwrap();
    }
}

#[test]
fn native_grouped_main_kv_commit_keeps_each_actual_original_for_exact_replay() {
    use sinomo_core::kv::{
        KvAuthorityBasis, KvCommand, KvCommandRequest, KvCompare, KvOp, KvTxn, KvTxnRequest,
    };
    use sinomo_dfs::test_support::{DfsTestRuntime, NativeShardTraceArtifactSession};
    use std::sync::{Arc, Mutex};
    let name = "native-grouped-original-main-replay";
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let trace = NativeShardTraceArtifactSession::from_environment(
        name,
        [(node.clone(), native.data_dir(&node).root())],
        DfsMetadataShardId::new("root"),
    )
    .unwrap();
    native.start_nodes().unwrap();
    let context = native.component_context(&node).unwrap();
    let cluster = context.cluster_access().unwrap().clone();
    let runtime = native.runtime_handle();
    let shard = native.root_shard().unwrap();
    let config_key = sinomo_dfs::metadata_shard_key(&shard.file_system_id, "root");
    let current = runtime
        .block_on(cluster.kv_get_authoritative_async(&config_key))
        .unwrap();
    let entry = current.value.expect("actual mounted root assignment");
    let requests = (0..32)
        .map(|index| {
            KvTxnRequest::generated(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: config_key.clone(),
                    value: entry.value.clone(),
                }],
                success: vec![KvOp::Put {
                    key: format!(
                        "/dfs/filesystems/{}/native-group-observer/{index}",
                        shard.file_system_id
                    ),
                    value: vec![index as u8],
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
        })
        .collect::<Vec<_>>();
    let ids = requests
        .iter()
        .map(|request| request.request_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let actual = Arc::clone(&observed);
    let paths = sinomo_core::network::ClusterPaths::new(context.data_dir());
    let subscription =
        sinomo_core::cluster::subscribe_cluster_kv_committed_observations(&paths, move |event| {
            if !matches!(
                &event.value,
                sinomo_core::kv::KvCommittedObservationValue::Delta(_)
            ) {
                return;
            }
            if let Some(originals) = event.original_current_term_commits.as_ref() {
                let selected = originals
                    .iter()
                    .filter(|fact| ids.contains(&fact.request.request_id))
                    .cloned()
                    .collect::<Vec<_>>();
                if !selected.is_empty() {
                    actual.lock().unwrap().push(selected);
                }
            }
        });
    subscription.include_original_current_term_commits(true);
    let outcomes = runtime.block_on(async {
        let mut calls = tokio::task::JoinSet::new();
        for request in requests.clone() {
            let cluster = Arc::clone(&cluster);
            calls.spawn(async move { cluster.kv_commit_compact_async(request).await });
        }
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
            let mut outcomes = Vec::new();
            while let Some(outcome) = calls.join_next().await {
                outcomes.push(outcome.unwrap().unwrap());
            }
            outcomes
        })
        .await
        .expect("normal bounded grouped Main commits")
    });
    assert_eq!(outcomes.len(), requests.len());
    for outcome in &outcomes {
        assert!(outcome.txn_response.as_ref().unwrap().succeeded);
        let request = requests
            .iter()
            .find(|request| request.request_id == outcome.request_id)
            .unwrap();
        assert_eq!(
            outcome.command_digest,
            KvCommandRequest::new(
                request.request_id.clone(),
                KvCommand::Txn(request.txn.clone())
            )
            .digest()
            .unwrap()
        );
    }
    let groups = observed.lock().unwrap().clone();
    let group = groups
        .iter()
        .find(|group| group.len() >= 2)
        .expect("actual certified group must contain two different requests");
    let pair = &group[..2];
    assert_ne!(pair[0].request.request_id, pair[1].request.request_id);
    assert_eq!(pair[0].authority, pair[1].authority);
    let original_count = groups.iter().map(Vec::len).sum::<usize>();
    for fact in pair {
        let request = requests
            .iter()
            .find(|request| request.request_id == fact.request.request_id)
            .unwrap();
        assert_eq!(fact.root, paths.root);
        assert_eq!(fact.node_id, node);
        assert_eq!(
            fact.request,
            KvCommandRequest::new(
                request.request_id.clone(),
                KvCommand::Txn(request.txn.clone())
            )
        );
        assert_eq!(
            fact.committed.command_digest,
            fact.request.digest().unwrap()
        );
        assert_eq!(fact.committed.term, fact.authority.term);
        assert!(fact.committed.log_index <= fact.authority.commit_index);
        assert!(fact.committed.revision <= fact.authority.revision);
        assert!(matches!(
            fact.authority.basis,
            KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 1,
                required_quorum: 1
            }
        ));
        let replay = runtime
            .block_on(cluster.kv_commit_compact_async(request.clone()))
            .unwrap();
        assert!(replay.replayed_committed_request);
        assert_eq!(
            replay.authority.basis,
            KvAuthorityBasis::ExactCommittedRequest
        );
        assert_eq!(replay.request_id, fact.committed.request_id);
        assert_eq!(replay.command_digest, fact.committed.command_digest);
        assert_eq!(replay.txn_response, fact.committed.txn_response);
        assert_eq!(replay.authority.term, fact.committed.term);
        assert_eq!(replay.authority.commit_index, fact.committed.log_index);
        assert_eq!(replay.authority.revision, fact.committed.revision);
        let key = match &request.txn.success[0] {
            KvOp::Put { key, .. } => key,
            _ => unreachable!(),
        };
        assert_eq!(
            runtime
                .block_on(cluster.kv_get_authoritative_async(key))
                .unwrap()
                .value
                .unwrap()
                .value,
            match &request.txn.success[0] {
                KvOp::Put { value, .. } => value.clone(),
                _ => unreachable!(),
            }
        );
    }
    assert_eq!(
        observed.lock().unwrap().iter().map(Vec::len).sum::<usize>(),
        original_count,
        "exact replay must not mint another original quorum observation"
    );
    let mut changed = requests
        .iter()
        .find(|request| request.request_id == pair[0].request.request_id)
        .unwrap()
        .clone();
    let KvOp::Put { value, .. } = &mut changed.txn.success[0] else {
        unreachable!()
    };
    value.push(0xff);
    assert!(
        runtime
            .block_on(cluster.kv_commit_compact_async(changed))
            .is_err()
    );
    assert_eq!(
        observed.lock().unwrap().iter().map(Vec::len).sum::<usize>(),
        original_count
    );
    drop(subscription);
    drop(cluster);
    drop(context);
    if let Some(trace) = &trace {
        trace.close().unwrap();
    }
    native.close().unwrap();
    if let Some(trace) = trace {
        trace.export().unwrap();
    }
}
