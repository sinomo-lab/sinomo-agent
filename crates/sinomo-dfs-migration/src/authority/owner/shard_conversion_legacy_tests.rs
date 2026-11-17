//! A legacy shard without a storage record converts per shard on its own
//! migration units while the rest of the filesystem serves.
//!
//! The fixture writes through a native three-node cluster in which each node
//! is the primary of one shard, then models the `users` shard as released
//! format-3 storage on every member: each copy's snapshot keeps its own rows
//! behind a format-3 header, its journal is kept, and its acknowledgement
//! floors are rewritten as released plain rows. One
//! receiver keeps only a height-only authority floor, which only a local
//! checkpoint capture of its copy anchors; the other keeps floors it
//! acknowledged before the last writes, with prefix proofs, which the primary
//! proves ancestors by streaming its journal frames after them. The shard's
//! storage record is removed, as on a deployment that predates it.
//!
//! The primary's floor challenges to the receivers are refused by the
//! transport, so its conversion parks with the shard Converting and sealed.
//! Meanwhile the other shards serve reads and writes from every node, and
//! nothing writes the filesystem record. Once the transport heals the primary
//! is woken, installs on both receivers, and the shard activates on every
//! member, each receiver keeping its legacy copy beside the image.
use super::*;
use crate::authority::placement_work::native_tests::{
    canonical_migration_runtime, native_migration_cluster,
};
use crate::authority::staged_service::conversion_install::{
    CHUNK_ROUTE, ConversionSourceCut, FLOORS_ROUTE, FloorsRequest, StagedAttempt,
};
use crate::metadata::ack_floor::LegacyAcknowledgement;
use sinomo_core::node::NodeId;
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_dfs::test_support::{DfsTestFaultAction, DfsTestFaultRule, DfsTestRuntime};
use sinomo_dfs::{DfsFileSystemId, DfsMetadataShardId, DfsMetadataShardStorage};
use std::time::{Duration, Instant};

const PAUSED: &str = "startup is not awaited while users converts";

fn entry(owner: &MigrationOwner, key: String) -> Option<KvEntry> {
    let (rows, _) = owner.authority.read_exact(&[key]).unwrap();
    rows.into_iter().next().unwrap().entry
}

fn storage(
    owner: &MigrationOwner,
    fs: &DfsFileSystemId,
    shard: &str,
) -> Option<DfsMetadataShardStorage> {
    entry(owner, sinomo_dfs::metadata_shard_storage_key(fs, shard))
        .map(|entry| entry.decode().unwrap())
}

fn users_root(native: &DfsTestRuntime, node: &NodeId) -> PathBuf {
    native.data_dir(node).dfs_dir().join("metadata/users")
}

fn copy_tree(source: &Path, target: &Path) {
    if !source.exists() {
        return;
    }
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let path = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &path);
        } else {
            std::fs::copy(entry.path(), &path).unwrap();
        }
    }
}

/// A node's prefix and authority floors of `users`, as released rows that
/// keep their prefix proofs.
fn acknowledged(root: &Path) -> Vec<(&'static str, LegacyAcknowledgement)> {
    let (prefix, authority) = sinomo_dfs::read_current_metadata_acknowledgement_floors(
        root,
        &DfsMetadataShardId::new("users"),
    )
    .unwrap();
    let prefix = prefix.map(|floor| LegacyAcknowledgement {
        shard_id: floor.shard_id,
        journal_sequence: floor.journal_sequence,
        state_fingerprint: floor.state_fingerprint,
        durable_prefix_proof: Some(floor.durable_prefix_proof),
    });
    let authority = authority.map(|floor| LegacyAcknowledgement {
        shard_id: floor.shard_id,
        journal_sequence: floor.journal_sequence,
        state_fingerprint: String::new(),
        durable_prefix_proof: Some(floor.durable_prefix_proof),
    });
    [("prefix", prefix), ("authority", authority)]
        .into_iter()
        .filter_map(|(kind, floor)| floor.map(|floor| (kind, floor)))
        .collect()
}

/// Replace `node`'s current copy of `users` with released format-3 storage
/// of the same rows and journal, retaining `floors` as plain rows. The
/// current copy is kept beside the node's data root. Returns the modeled
/// floor files and the copy's checkpoint prefix.
fn model_legacy_users(
    native: &DfsTestRuntime,
    node: &NodeId,
    floors: &[(&'static str, LegacyAcknowledgement)],
) -> (Vec<(String, Vec<u8>)>, sinomo_dfs::DfsMetadataPrefixProof) {
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
    let root = users_root(native, node);
    let runtime = native.runtime_handle();
    let entered = runtime.enter();
    let backend =
        sinomo_dfs::LocalSobjMetadataBackend::new(DfsMetadataShardId::new("users"), &root).unwrap();
    let snapshot = backend.export_table_snapshot().unwrap();
    let image = backend.export_snapshot_image().unwrap();
    assert_eq!(
        backend.journal_last_sequence(),
        snapshot.materialized_through_sequence
    );
    drop(backend);
    drop(entered);
    let current_image = native
        .data_dir(node)
        .root()
        .join("legacy-users-checkpoint-image");
    sinomo_dfs::write_metadata_candidate_image(&current_image, &image, &fs).unwrap();
    let baseline = native
        .data_dir(node)
        .root()
        .join("legacy-users-current-baseline");
    std::fs::rename(&root, &baseline).unwrap();
    let generation = blake3::hash(
        &serde_json::to_vec(&(
            3u16,
            Some(snapshot.materialized_through_sequence),
            Some(&snapshot.prefix_proof),
        ))
        .unwrap(),
    )
    .to_hex()
    .to_string();
    let header = Released3Header {
        format_version: 3,
        materialized_through_sequence: Some(snapshot.materialized_through_sequence),
        prefix_proof: Some(&snapshot.prefix_proof),
        generation,
    };
    let snapshot_path = root.join("records/tables/snapshot.sobj");
    std::fs::create_dir_all(snapshot_path.parent().unwrap()).unwrap();
    let mut reader =
        MigrationStreamReader::open(&current_image.join("records/tables/snapshot.sobj")).unwrap();
    let mut writer = MigrationStreamWriter::resume(
        &snapshot_path,
        &serde_ext::to_bytes(&header, 3, LengthMode::U32).unwrap(),
        &StreamCursor::default(),
    )
    .unwrap();
    let mut rows = 0;
    while let Some(record) = reader.next_record(64 * 1024 * 1024).unwrap() {
        writer.append(&record).unwrap();
        rows += 1;
    }
    assert!(rows > 0);
    writer.finish().unwrap();
    drop(writer);
    copy_tree(&baseline.join("journal"), &root.join("journal"));
    let mut written = Vec::new();
    for (kind, floor) in floors {
        let relative = format!("coordination/metadata-{kind}-ack-floors/users.sobj");
        let path = root.join(&relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let bytes = serde_ext::to_bytes(floor, 1, LengthMode::U32).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        written.push((relative, bytes));
    }
    assert!(!crate::metadata::current_root(&root).unwrap());
    assert!(crate::metadata::original_source_present(&root).unwrap());
    assert!(
        crate::metadata::original_native_root_format(&root)
            .unwrap()
            .is_none()
    );
    (written, snapshot.prefix_proof)
}

/// Wait until every node's startup settles; a conversion step that changes
/// a unit's readiness starts another startup generation.
fn await_startup(native: &DfsTestRuntime) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while let Err(failure) = native.wait_startup_completion() {
        assert!(
            Instant::now() < deadline,
            "startup did not settle: {failure}"
        );
    }
}

fn write_and_read(
    native: &DfsTestRuntime,
    node: &NodeId,
    path: &str,
    options: &sinomo_dfs::DfsWriteOptions,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let written = native.client(node).and_then(|mut client| {
            client
                .posix_write_file(path, path.as_bytes(), options.clone())
                .and_then(|_| client.posix_read_file_distributed(path))
                .map_err(|failure| format!("{failure:?}"))
        });
        match written {
            Ok(bytes) => {
                assert_eq!(bytes, path.as_bytes());
                return;
            }
            Err(failure) => assert!(
                Instant::now() < deadline,
                "{node} does not write {path}: {failure:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A legacy shard whose members retain floors only a local checkpoint
/// capture anchors, and floors below the cut with prefix proofs, converts per
/// shard: the stage reconciles every member's floors, the other shards serve
/// reads and writes while it converts, and no filesystem key is written.
#[test]
fn native_a_legacy_shard_converts_per_shard_while_the_filesystem_serves() {
    let (mut native, nodes) = native_migration_cluster("shard-conversion-legacy-floors", 3);
    let fs = sinomo_dfs::sifs_file_system_id();
    // Every node is the primary of one shard, so none starts as a passive
    // node, whose startup prepares every shard's storage.
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
    assert_eq!(users.replica_node_ids.len(), 2);
    let primary = users.primary_node_id.clone();
    let (checkpointed, streamed) = (
        users.replica_node_ids[0].clone(),
        users.replica_node_ids[1].clone(),
    );
    let options = sinomo_dfs::DfsWriteOptions {
        policy: sinomo_dfs::DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let write = |path: &str, body: &[u8]| {
        let mut client = native.client(&primary).unwrap();
        client
            .posix_write_file(path, body, options.clone())
            .unwrap();
        client
            .posix_sync_file(path, sinomo_dfs::DfsPosixSyncKind::File)
            .unwrap();
    };
    write("/users/legacy-doc", b"users bytes before the last writes");
    native
        .client(&primary)
        .unwrap()
        .posix_mkdir("/users/legacy-dir")
        .unwrap();
    // The floors the streamed receiver acknowledged before the last writes.
    let earlier = acknowledged(&users_root(&native, &streamed));
    assert!(
        !earlier.is_empty(),
        "the receiver acknowledged the first writes"
    );
    write(
        "/users/legacy-later",
        b"users bytes after the receiver's retained floors",
    );
    write("/root-file", b"root bytes");
    write("/services/file", b"services bytes");

    let owner = canonical_migration_runtime(&native.component_context(&nodes[0]).unwrap());
    let filesystem = entry(&owner, sinomo_dfs::file_system_config_key(&fs)).unwrap();
    let others = ["root", "services"].map(|shard| storage(&owner, &fs, shard).unwrap());
    assert!(others.iter().all(DfsMetadataShardStorage::is_active));
    // A deployment that predates storage records has none for the shard.
    let key = sinomo_dfs::metadata_shard_storage_key(&fs, "users");
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
    drop(owner);

    for node in nodes.iter().rev() {
        native.stop_node(node).unwrap();
    }
    let checkpoint_floor = acknowledged(&users_root(&native, &checkpointed))
        .into_iter()
        .find(|(kind, _)| *kind == "authority")
        .map(|(kind, mut floor)| {
            floor.durable_prefix_proof = None;
            (kind, floor)
        })
        .expect("the receiver retained an authority floor");
    let primary_floors = acknowledged(&users_root(&native, &primary));
    let (checkpointed_files, head) = model_legacy_users(
        &native,
        &checkpointed,
        std::slice::from_ref(&checkpoint_floor),
    );
    let (streamed_files, streamed_head) = model_legacy_users(&native, &streamed, &earlier);
    let (_, primary_head) = model_legacy_users(&native, &primary, &primary_floors);
    assert_eq!((&streamed_head, &primary_head), (&head, &head));
    assert!(checkpoint_floor.1.journal_sequence <= head.journal_sequence);
    assert!(
        earlier
            .iter()
            .all(|(_, floor)| floor.journal_sequence < head.journal_sequence),
        "the streamed receiver's floors are below the cut"
    );

    // The primary's floor challenges are refused until the test heals them.
    for receiver in [&checkpointed, &streamed] {
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

    // The primary's own unit converts the record-less legacy copy per shard:
    // it records a sealed attempt, and its install round acknowledges only
    // the primary itself, both challenges being refused.
    let deadline = Instant::now() + Duration::from_secs(120);
    let attempt = loop {
        let sealed = storage(&owners[0], &fs, "users")
            .and_then(|record| record.converting_attempt().cloned())
            .filter(|attempt| attempt.sealed().is_some());
        let installed = |attempt: &sinomo_dfs::DfsMetadataShardConversion| {
            let acks = owner_of(&primary)
                .authority
                .data_root
                .join("dfs/migration-owner/shards")
                .join(fs.to_string())
                .join("users")
                .join(&attempt.attempt_id)
                .join("install-acks.json");
            std::fs::read(acks).ok().map(|bytes| {
                serde_json::from_slice::<BTreeMap<NodeId, serde_json::Value>>(&bytes)
                    .unwrap()
                    .into_keys()
                    .collect::<Vec<_>>()
            })
        };
        if let Some(attempt) = sealed
            && installed(&attempt) == Some(vec![primary.clone()])
        {
            break attempt;
        }
        assert!(
            Instant::now() < deadline,
            "users did not park at its install: record {:?}, scheduler {}",
            storage(&owners[0], &fs, "users"),
            owner_of(&primary).diagnostic_scheduler()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(
        (attempt.epoch, &attempt.primary_node_id),
        (users.epoch, &primary)
    );
    assert_eq!(attempt.source.source_format, Some(3));

    // While users converts, the other shards serve reads and writes from
    // every node, and nothing writes the filesystem record.
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
    let now = entry(&owners[0], sinomo_dfs::file_system_config_key(&fs)).unwrap();
    assert_eq!(
        (&now.value, now.modify_revision),
        (&filesystem.value, filesystem.modify_revision)
    );

    // Healed, the primary installs on both receivers and activates. The
    // ancestry chunks it sends the streamed receiver pass a gate that only
    // counts them.
    native.clear_faults();
    const CHUNK_BUDGET: usize = 32;
    let chunks = Arc::new(tokio::sync::Semaphore::new(CHUNK_BUDGET));
    native.install_fault(DfsTestFaultRule {
        source: Some(primary.clone()),
        target: Some(streamed.clone()),
        route: Some(CHUNK_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: CHUNK_BUDGET,
        action: DfsTestFaultAction::PauseBeforeDispatch(Arc::clone(&chunks)),
    });
    owner_of(&primary).work.notify();
    let deadline = Instant::now() + Duration::from_secs(120);
    let record = loop {
        if let Some(record) =
            storage(&owners[0], &fs, "users").filter(DfsMetadataShardStorage::is_active)
        {
            break record;
        }
        if Instant::now() >= deadline {
            // Each receiver's own answer to the attempt's floor challenge.
            let sealed = attempt.sealed().unwrap().0.clone();
            let source: ConversionSourceCut = serde_json::from_slice(
                &std::fs::read(
                    owner_of(&primary)
                        .authority
                        .data_root
                        .join("dfs/migration-owner/shards")
                        .join(fs.to_string())
                        .join("users")
                        .join(&attempt.attempt_id)
                        .join("source-cut.json"),
                )
                .unwrap(),
            )
            .unwrap();
            let request = FloorsRequest {
                attempt: StagedAttempt {
                    file_system_id: fs.clone(),
                    shard_id: "users".into(),
                    attempt_id: attempt.attempt_id.clone(),
                    epoch: attempt.epoch,
                    seal: sealed,
                },
                source,
            };
            let answers = [&checkpointed, &streamed].map(|receiver| {
                let authority = &owner_of(receiver).authority;
                authority
                    .receive_conversion_floors(
                        &authority.component_context,
                        primary.clone(),
                        request.clone(),
                    )
                    .map_err(|failure| failure.to_string())
            });
            panic!("users did not activate; the receivers answer {answers:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let (activated, digest) = record
        .converted_activation()
        .expect("an activated conversion");
    assert_eq!(activated.attempt_id, attempt.attempt_id);
    let digest = digest.to_owned();
    for node in &nodes {
        let root = users_root(&native, node);
        while !sinomo_dfs::inspect_metadata_storage_generation(&root).is_ok_and(|header| {
            header.identity == record.generation
                && matches!(&header.state, sinomo_dfs::DfsMetadataGenerationState::Active { activation_digest, .. }
                    if *activation_digest == digest)
        }) {
            assert!(
                Instant::now() < deadline,
                "{node} did not activate users: {}",
                owner_of(node).diagnostic_scheduler()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    // Each receiver staged the image through the conversion stage and keeps
    // its legacy copy, floors unchanged, beside it.
    let retained = format!(".metadata-retained/shard-conversion-{}", attempt.attempt_id);
    for (receiver, files) in [
        (&checkpointed, &checkpointed_files),
        (&streamed, &streamed_files),
    ] {
        let kept = native
            .data_dir(receiver)
            .dfs_dir()
            .join("metadata")
            .join(&retained);
        assert!(
            kept.join("records/tables/snapshot.sobj").is_file(),
            "{receiver} kept its legacy copy"
        );
        for (relative, bytes) in files {
            assert_eq!(
                std::fs::read(kept.join(relative)).unwrap(),
                *bytes,
                "{receiver} {relative}"
            );
        }
    }
    // The streamed receiver's floors were proved by the primary's frames.
    assert!(
        chunks.available_permits() < CHUNK_BUDGET,
        "the primary streamed ancestry to the receiver whose floors are below the cut"
    );
    native.clear_faults();
    let now = entry(&owners[0], sinomo_dfs::file_system_config_key(&fs)).unwrap();
    assert_eq!(
        (&now.value, now.modify_revision),
        (&filesystem.value, filesystem.modify_revision)
    );
    assert_eq!(
        ["root", "services"].map(|shard| storage(&owners[0], &fs, shard).unwrap()),
        others
    );

    await_startup(&native);
    for node in &nodes {
        let mut client = native.client(node).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/users/legacy-doc")
                .unwrap(),
            b"users bytes before the last writes"
        );
        assert_eq!(
            client
                .posix_read_file_distributed("/users/legacy-later")
                .unwrap(),
            b"users bytes after the receiver's retained floors"
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

/// A shard whose rows live only in the combined root copy.
mod combined;
