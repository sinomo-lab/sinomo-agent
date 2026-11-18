//! A legacy shard whose rows live only in the combined `root` copy, with no
//! copy of its own, converts per shard from that copy.
//!
//! The fixture writes through a native three-node cluster in which each node
//! is the primary of one shard, then models `root` and `users` as one
//! released format-3 `root` copy on every node: the copy's snapshot holds
//! both shards' rows behind `root`'s prefix, beside `root`'s journal, and
//! `users` has no directory. Neither shard has a storage record, as on a
//! deployment that predates it.
//!
//! The users primary's floor challenges are refused until the test heals
//! them, so users parks at its install while root converts and activates.
//! Root's activation moves each node's combined copy to its retained place,
//! the copy users' capture read included. Root and services then serve
//! reads and writes from every node while users is still closed. Once
//! healed, users installs from the moved copy and activates with only its
//! own rows, and the combined copy holds the same bytes it held before.
use super::*;
use sinomo_dfs::DfsMetadataTableSnapshot;

fn metadata_dir(native: &DfsTestRuntime, node: &NodeId) -> PathBuf {
    native.data_dir(node).dfs_dir().join("metadata")
}

/// Every file under `root` but its writer lock, with its bytes.
fn contents(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            } else {
                let relative = entry.path().strip_prefix(root).unwrap().to_owned();
                if relative != Path::new("journal/write.lock") {
                    files.insert(relative, std::fs::read(entry.path()).unwrap());
                }
            }
        }
    }
    files
}

fn snapshot(native: &DfsTestRuntime, node: &NodeId, shard: &str) -> DfsMetadataTableSnapshot {
    let runtime = native.runtime_handle();
    let _entered = runtime.enter();
    let backend = sinomo_dfs::LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new(shard),
        metadata_dir(native, node).join(shard),
    )
    .unwrap();
    let snapshot = backend.export_table_snapshot().unwrap();
    assert_eq!(
        backend.journal_last_sequence(),
        snapshot.materialized_through_sequence
    );
    snapshot
}

/// Replace `node`'s current copies of root and users with one released
/// format-3 `root` copy holding both shards' rows behind root's prefix and
/// root's journal; users keeps no directory. The current copies are kept
/// beside the node's data root. Returns the combined copy's files.
fn model_combined_root(native: &DfsTestRuntime, node: &NodeId) -> BTreeMap<PathBuf, Vec<u8>> {
    use sinomo_core::storage::migration::stream::{
        MigrationStreamReader, MigrationStreamWriter, StreamCursor,
    };
    #[derive(Serialize)]
    struct Released3Header<'a> {
        format_version: u16,
        materialized_through_sequence: Option<u64>,
        prefix_proof: Option<&'a sinomo_dfs::DfsMetadataPrefixProof>,
        generation: String,
    }
    let fs = sinomo_dfs::sifs_file_system_id();
    let root = snapshot(native, node, "root");
    let users = snapshot(native, node, "users");
    let mut tables = *root.tables.clone();
    let rows = *users.tables;
    macro_rules! combine {
        ($($field:ident),* $(,)?) => {
            $(tables.$field.extend(rows.$field);)*
        };
    }
    combine!(
        objects,
        file_system_objects,
        paths,
        inodes,
        dirfrags,
        dentries,
        hardlinks,
        compacted_tombstones,
        versions,
        version_pins,
        cross_shard_pin_transfers,
        receipts,
        receipt_withdrawals,
        intents,
        intent_receipts,
        advisory_lock_states,
        namespace_transaction_receipts,
        prepared_operations_v7,
        prepared_operation_evictions,
        namespace_jobs,
        namespace_job_receipts,
        namespace_request_receipts,
        namespace_requests,
    );
    let data_root = native.data_dir(node).root().to_owned();
    let image = data_root.join("combined-root-image");
    let writer = sinomo_dfs::DfsMetadataCandidateWriter::open_generation(
        &image,
        sinomo_dfs::DfsMetadataGenerationIdentity {
            file_system_id: fs.clone(),
            shard_id: DfsMetadataShardId::new("root"),
            generation: "combined-root-fixture".into(),
            origin: sinomo_dfs::DfsMetadataGenerationOrigin::StorageTransition {
                transition_id: "combined-root-fixture".into(),
            },
        },
    )
    .unwrap();
    let rows = sinomo_dfs::DfsMetadataCandidateState::new(tables)
        .unwrap()
        .snapshot();
    writer.write(rows, root.prefix_proof.clone()).unwrap();
    drop(writer);
    let metadata = metadata_dir(native, node);
    let baseline = data_root.join("combined-current-baseline");
    std::fs::create_dir_all(&baseline).unwrap();
    for shard in ["root", "users"] {
        std::fs::rename(metadata.join(shard), baseline.join(shard)).unwrap();
    }
    let combined = metadata.join("root");
    let generation = blake3::hash(
        &serde_json::to_vec(&(
            3u16,
            Some(root.materialized_through_sequence),
            Some(&root.prefix_proof),
        ))
        .unwrap(),
    )
    .to_hex()
    .to_string();
    let header = Released3Header {
        format_version: 3,
        materialized_through_sequence: Some(root.materialized_through_sequence),
        prefix_proof: Some(&root.prefix_proof),
        generation,
    };
    let snapshot_path = combined.join("records/tables/snapshot.sobj");
    std::fs::create_dir_all(snapshot_path.parent().unwrap()).unwrap();
    let mut reader =
        MigrationStreamReader::open(&image.join("records/tables/snapshot.sobj")).unwrap();
    let mut writer = MigrationStreamWriter::resume(
        &snapshot_path,
        &serde_ext::to_bytes(&header, 3, LengthMode::U32).unwrap(),
        &StreamCursor::default(),
    )
    .unwrap();
    while let Some(record) = reader.next_record(64 * 1024 * 1024).unwrap() {
        writer.append(&record).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    copy_tree(&baseline.join("root/journal"), &combined.join("journal"));
    assert!(!crate::metadata::current_root(&combined).unwrap());
    assert!(
        crate::metadata::original_native_root_format(&combined)
            .unwrap()
            .is_none()
    );
    assert!(!metadata.join("users").exists());
    contents(&combined)
}

/// A three-node cluster whose root and users shards are modeled as one
/// released format-3 root copy on every node, with no storage record for
/// either, while services is current.
struct CombinedCluster {
    native: DfsTestRuntime,
    nodes: Vec<NodeId>,
    users: sinomo_dfs::DfsMetadataShardConfig,
    options: sinomo_dfs::DfsWriteOptions,
    /// Each node's combined copy, as modeled.
    combined: BTreeMap<NodeId, BTreeMap<PathBuf, Vec<u8>>>,
    filesystem: KvEntry,
    services: DfsMetadataShardStorage,
}

fn combined_cluster(name: &str) -> CombinedCluster {
    let (mut native, nodes) = native_migration_cluster(name, 3);
    let fs = sinomo_dfs::sifs_file_system_id();
    let mut users = None;
    for (shard, primary, replicas) in [
        ("root", 0, [1, 2]),
        ("services", 1, [0, 2]),
        ("users", 2, [0, 1]),
    ] {
        let config = native
            .reassign_metadata_shard(
                &fs,
                shard,
                nodes[primary].clone(),
                replicas.map(|index| nodes[index].clone()).to_vec(),
            )
            .unwrap();
        if shard == "users" {
            users = Some(config);
        }
    }
    let users = users.unwrap();
    let options = sinomo_dfs::DfsWriteOptions {
        policy: sinomo_dfs::DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let write = |path: &str, body: &[u8]| {
        let mut client = native.client(&nodes[0]).unwrap();
        client
            .posix_write_file(path, body, options.clone())
            .unwrap();
        client
            .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
            .unwrap();
    };
    write("/users/combined-doc", b"users bytes in the combined root");
    native
        .client(&nodes[0])
        .unwrap()
        .posix_mkdir("/users/combined-dir")
        .unwrap();
    write("/root-file", b"root bytes");
    write("/services/file", b"services bytes");

    let owner = canonical_migration_runtime(&native.component_context(&nodes[0]).unwrap());
    let filesystem = entry(&owner, sinomo_dfs::file_system_config_key(&fs)).unwrap();
    let services = storage(&owner, &fs, "services").unwrap();
    assert!(services.is_active());
    // A deployment that predates storage records has none for root or users.
    for shard in ["root", "users"] {
        let key = sinomo_dfs::metadata_shard_storage_key(&fs, shard);
        let born = entry(&owner, key.clone()).expect("a born shard carries a storage record");
        let committed = owner
            .authority
            .commit(KvTxnRequest::new(
                KvRequestId::generate(),
                KvTxn {
                    compares: vec![KvCompare::ValueEquals {
                        key: key.clone(),
                        value: born.value,
                    }],
                    success: vec![KvOp::Delete { key: key.clone() }],
                    failure: Vec::new(),
                },
            ))
            .unwrap();
        NativeImportAuthority::require_committed(&committed).unwrap();
    }
    drop(owner);

    for node in nodes.iter().rev() {
        native.stop_node(node).unwrap();
    }
    let combined = nodes
        .iter()
        .map(|node| (node.clone(), model_combined_root(&native, node)))
        .collect::<BTreeMap<_, _>>();

    CombinedCluster { native, nodes, users, options, combined, filesystem, services }
}

/// The paths of the objects in `shard`'s current copy on `node`.
fn image_paths(native: &DfsTestRuntime, node: &NodeId, shard: &str) -> BTreeSet<String> {
    let runtime = native.runtime_handle();
    let _entered = runtime.enter();
    sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(shard),
        metadata_dir(native, node).join(shard),
    )
    .unwrap()
    .export_table_snapshot()
    .unwrap()
    .tables
    .objects
    .iter()
    .map(|object| object.path.clone())
    .collect()
}

fn under_users(path: &str) -> bool {
    path == "/users" || path.starts_with("/users/")
}

/// A legacy filesystem whose users rows live only in the combined root copy
/// converts users per shard from that copy while root and services serve:
/// users' image holds only users' rows, root's conversion moving the copy
/// under users' attempt does not lose it, and the copy is not changed.
#[test]
fn native_a_shard_whose_rows_live_only_in_the_combined_root_converts_per_shard() {
    let CombinedCluster { mut native, nodes, users, options, combined, filesystem, services } =
        combined_cluster("shard-conversion-combined-root");
    let fs = sinomo_dfs::sifs_file_system_id();
    let primary = users.primary_node_id.clone();

    // Users' floor challenges are refused until the test heals them.
    for receiver in &users.replica_node_ids {
        native.install_fault(DfsTestFaultRule {
            source: Some(primary.clone()),
            target: Some(receiver.clone()),
            route: Some(FLOORS_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: DfsTestFaultAction::BudgetExhausted,
        });
    }
    for node in nodes.clone() {
        let failure = native
            .reopen_node_with_startup_action(node, Box::new(|_| Err(PAUSED.into())))
            .unwrap_err();
        assert!(failure.contains(PAUSED), "{failure}");
    }
    let owners = nodes
        .iter()
        .map(|node| canonical_migration_runtime(&native.component_context(node).unwrap()))
        .collect::<Vec<_>>();
    let owner_of = |node: &NodeId| {
        &owners[nodes
            .iter()
            .position(|candidate| candidate == node)
            .unwrap()]
    };
    // Users has no directory, so it is no discovered unit: opening it asks
    // its primary's owner, as the DFS host does.
    let users_root = metadata_dir(&native, &primary).join("users");
    let demanded = owner_of(&primary)
        .demand(
            &users_root,
            &sinomo_dfs::DfsStoragePreparationScope::Metadata(DfsMetadataShardId::new("users")),
        )
        .unwrap();
    assert!(matches!(
        demanded,
        sinomo_dfs::DfsStoragePreparation::Pending { .. }
    ));

    // Users records a sealed attempt from the combined copy and parks at its
    // install, while root converts and activates on every member.
    let deadline = Instant::now() + Duration::from_secs(180);
    let attempt = loop {
        let sealed = storage(&owners[0], &fs, "users")
            .and_then(|record| record.converting_attempt().cloned())
            .filter(|attempt| attempt.sealed().is_some());
        let root_active = storage(&owners[0], &fs, "root").is_some_and(|record| record.is_active());
        if let Some(attempt) = sealed
            && root_active
        {
            break attempt;
        }
        assert!(
            Instant::now() < deadline,
            "users did not park sealed while root activated: users {:?}, root {:?}, scheduler {}",
            storage(&owners[0], &fs, "users"),
            storage(&owners[0], &fs, "root"),
            owner_of(&primary).diagnostic_scheduler()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(attempt.source.source_format, Some(3));
    let root_record = storage(&owners[0], &fs, "root").unwrap();
    let (root_attempt, _) = root_record.converted_activation().unwrap();
    let retained = format!(
        ".metadata-retained/shard-conversion-{}",
        root_attempt.attempt_id
    );
    for node in &nodes {
        while !metadata_dir(&native, node).join(&retained).is_dir() {
            assert!(
                Instant::now() < deadline,
                "{node} did not activate root: {}",
                owner_of(node).diagnostic_scheduler()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    // Converted first, root's image holds root's rows and none of users'.
    for node in &nodes {
        let root_copy = metadata_dir(&native, node).join("root");
        while !sinomo_dfs::inspect_metadata_storage_generation(&root_copy)
            .is_ok_and(|header| header.identity == root_record.generation)
        {
            assert!(
                Instant::now() < deadline,
                "{node} did not install root's image: {}",
                owner_of(node).diagnostic_scheduler()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let paths = image_paths(&native, node, "root");
        assert!(paths.contains("/root-file"), "{node}: {paths:?}");
        assert!(
            !paths.iter().any(|path| under_users(path)),
            "root's image holds users' rows on {node}: {paths:?}"
        );
    }

    // Root and services serve reads and writes from every node while users
    // is closed, and nothing writes the filesystem record.
    await_startup(&native);
    for node in &nodes {
        let mut client = native.client(node).unwrap();
        assert_eq!(
            client.posix_read_file_distributed("/root-file").unwrap(),
            b"root bytes"
        );
        assert_eq!(
            client
                .posix_read_file_distributed("/services/file")
                .unwrap(),
            b"services bytes"
        );
        drop(client);
        write_and_read(
            &native,
            node,
            &format!("/written-while-users-converts-{node}"),
            &options,
        );
        write_and_read(
            &native,
            node,
            &format!("/services/written-while-users-converts-{node}"),
            &options,
        );
    }
    assert!(storage(&owners[0], &fs, "users").is_some_and(|record| !record.is_active()));
    assert!(
        sinomo_dfs::inspect_metadata_storage_generation(&users_root).is_err(),
        "users' copy appears only with its image"
    );

    // Healed, users installs from the moved copy and activates.
    native.clear_faults();
    owner_of(&primary).work.notify();
    let record = loop {
        if let Some(record) =
            storage(&owners[0], &fs, "users").filter(DfsMetadataShardStorage::is_active)
        {
            break record;
        }
        assert!(
            Instant::now() < deadline,
            "users did not activate: {}",
            owner_of(&primary).diagnostic_scheduler()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let (activated, _) = record.converted_activation().unwrap();
    assert_eq!(activated.attempt_id, attempt.attempt_id);
    while !sinomo_dfs::inspect_metadata_storage_generation(&users_root)
        .is_ok_and(|header| header.identity == record.generation)
    {
        assert!(
            Instant::now() < deadline,
            "the primary did not activate users: {}",
            owner_of(&primary).diagnostic_scheduler()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // The image holds users' rows and no other shard's.
    let image = {
        let runtime = native.runtime_handle();
        let _entered = runtime.enter();
        sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new("users"),
            &users_root,
        )
        .unwrap()
        .export_table_snapshot()
        .unwrap()
    };
    let paths = image
        .tables
        .objects
        .iter()
        .map(|object| object.path.clone())
        .collect::<BTreeSet<_>>();
    assert!(
        paths.contains("/users/combined-doc") && paths.contains("/users/combined-dir"),
        "{paths:?}"
    );
    assert!(
        paths
            .iter()
            .all(|path| path == "/users" || path.starts_with("/users/")),
        "users' image holds another shard's rows: {paths:?}"
    );
    // Each node's combined copy is where root's conversion retained it,
    // with the bytes it held before.
    for (node, before) in &combined {
        assert_eq!(
            contents(&metadata_dir(&native, node).join(&retained)),
            *before,
            "{node}'s combined copy changed"
        );
    }
    native.clear_faults();
    let now = entry(&owners[0], sinomo_dfs::file_system_config_key(&fs)).unwrap();
    assert_eq!(
        (&now.value, now.modify_revision),
        (&filesystem.value, filesystem.modify_revision)
    );
    assert_eq!(storage(&owners[0], &fs, "services").unwrap(), services);

    await_startup(&native);
    for node in &nodes {
        let mut client = native.client(node).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/users/combined-doc")
                .unwrap(),
            b"users bytes in the combined root"
        );
        assert_eq!(
            client.posix_read_file_distributed("/root-file").unwrap(),
            b"root bytes"
        );
        drop(client);
        write_and_read(
            &native,
            node,
            &format!("/users/written-after-conversion-{node}"),
            &options,
        );
    }
    drop(owners);
    native.close().unwrap();
}

/// The same filesystem converts root last: users converts from the live
/// combined copy and activates while root parks at its install, then root
/// converts from that copy too and activates with one replica refusing its
/// install. Root's first commit repairs that replica by snapshot. On every
/// member root's image holds root's rows and none of users', and the
/// combined copy is kept beside it with the bytes it held before.
#[test]
fn native_root_converting_after_a_shard_of_its_combined_copy_keeps_only_its_own_rows() {
    let CombinedCluster { mut native, nodes, users, options, combined, .. } =
        combined_cluster("shard-conversion-combined-root-last");
    let fs = sinomo_dfs::sifs_file_system_id();
    let users_primary = users.primary_node_id.clone();
    let root_primary = nodes[0].clone();

    // Root's floor challenges are refused until the test heals them.
    for receiver in &nodes[1..] {
        native.install_fault(DfsTestFaultRule {
            source: Some(root_primary.clone()),
            target: Some(receiver.clone()),
            route: Some(FLOORS_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: DfsTestFaultAction::BudgetExhausted,
        });
    }
    for node in nodes.clone() {
        let failure = native
            .reopen_node_with_startup_action(node, Box::new(|_| Err(PAUSED.into())))
            .unwrap_err();
        assert!(failure.contains(PAUSED), "{failure}");
    }
    let owners = nodes
        .iter()
        .map(|node| canonical_migration_runtime(&native.component_context(node).unwrap()))
        .collect::<Vec<_>>();
    let owner_of = |node: &NodeId| {
        &owners[nodes
            .iter()
            .position(|candidate| candidate == node)
            .unwrap()]
    };
    let users_root = metadata_dir(&native, &users_primary).join("users");
    owner_of(&users_primary)
        .demand(
            &users_root,
            &sinomo_dfs::DfsStoragePreparationScope::Metadata(DfsMetadataShardId::new("users")),
        )
        .unwrap();

    // Users activates from the live combined copy while root parks sealed.
    let deadline = Instant::now() + Duration::from_secs(180);
    let users_record = loop {
        let active = storage(&owners[0], &fs, "users").filter(DfsMetadataShardStorage::is_active);
        let root_sealed = storage(&owners[0], &fs, "root")
            .and_then(|record| record.converting_attempt().cloned())
            .is_some_and(|attempt| attempt.sealed().is_some());
        if let Some(active) = active
            && root_sealed
        {
            break active;
        }
        assert!(
            Instant::now() < deadline,
            "users did not activate while root parked sealed: users {:?}, root {:?}, scheduler {}",
            storage(&owners[0], &fs, "users"),
            storage(&owners[0], &fs, "root"),
            owner_of(&users_primary).diagnostic_scheduler()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    while !sinomo_dfs::inspect_metadata_storage_generation(&users_root)
        .is_ok_and(|header| header.identity == users_record.generation)
    {
        assert!(
            Instant::now() < deadline,
            "the primary did not activate users: {}",
            owner_of(&users_primary).diagnostic_scheduler()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let paths = image_paths(&native, &users_primary, "users");
    assert!(paths.contains("/users/combined-doc"), "{paths:?}");
    assert!(paths.iter().all(|path| under_users(path)), "{paths:?}");
    assert!(storage(&owners[0], &fs, "root").is_some_and(|record| !record.is_active()));
    for (node, before) in &combined {
        assert_eq!(
            contents(&metadata_dir(&native, node).join("root")),
            *before,
            "{node}'s combined copy changed while users converted"
        );
    }

    // Healed toward users' primary only, root installs there and activates
    // with its primary and that majority of its members. The third member
    // misses the install and keeps the combined legacy copy.
    native.clear_faults();
    let missed = nodes[1].clone();
    native.install_fault(DfsTestFaultRule {
        source: Some(root_primary.clone()),
        target: Some(missed.clone()),
        route: Some(FLOORS_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: DfsTestFaultAction::BudgetExhausted,
    });
    owner_of(&root_primary).work.notify();
    let deadline = Instant::now() + Duration::from_secs(180);
    let root_record = loop {
        if let Some(record) =
            storage(&owners[0], &fs, "root").filter(DfsMetadataShardStorage::is_active)
        {
            break record;
        }
        assert!(
            Instant::now() < deadline,
            "root did not activate: {}",
            owner_of(&root_primary).diagnostic_scheduler()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    native.clear_faults();
    let (root_attempt, _) = root_record.converted_activation().unwrap();
    let retained = format!(
        ".metadata-retained/shard-conversion-{}",
        root_attempt.attempt_id
    );

    // Root is on the all-member commit rule, so its first commit repairs
    // the member that missed the install by snapshot, once that member's
    // unit has kept its combined copy aside.
    await_startup(&native);
    write_and_read(&native, &root_primary, "/first-root-commit", &options);
    let deadline = Instant::now() + Duration::from_secs(300);
    for node in &nodes {
        let root_copy = metadata_dir(&native, node).join("root");
        while !sinomo_dfs::inspect_metadata_storage_generation(&root_copy)
            .is_ok_and(|header| header.identity == root_record.generation)
        {
            assert!(
                Instant::now() < deadline,
                "{node} did not install root's image: {}",
                owner_of(node).diagnostic_scheduler()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let paths = image_paths(&native, node, "root");
        assert!(paths.contains("/root-file"), "{node}: {paths:?}");
        assert!(
            !paths.iter().any(|path| under_users(path)),
            "root's image holds users' rows on {node}: {paths:?}"
        );
        // Every member keeps its combined copy beside root's image, the
        // repaired one included.
        let retained = metadata_dir(&native, node).join(&retained);
        assert_eq!(contents(&retained), combined[node], "{node}'s combined copy changed");
    }

    await_startup(&native);
    for node in &nodes {
        let mut client = native.client(node).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/users/combined-doc")
                .unwrap(),
            b"users bytes in the combined root"
        );
        assert_eq!(
            client.posix_read_file_distributed("/root-file").unwrap(),
            b"root bytes"
        );
        drop(client);
        write_and_read(&native, node, &format!("/users/written-after-root-{node}"), &options);
        write_and_read(&native, node, &format!("/written-after-root-{node}"), &options);
    }
    drop(owners);
    native.close().unwrap();
}
