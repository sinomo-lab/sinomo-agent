use super::*;
use std::sync::{Arc, Mutex};

#[test]
fn command_outputs_persist_through_local_sandbox_filesystem() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_command_outputs_local");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let record = completed_command_record("sandbox_command_outputs_local");

    persist_command_outputs(&mut sandbox_fs, &record, b"hello\n", b"warn\n").expect("persist");

    assert_eq!(
        sandbox_fs
            .read(
                &sandbox_command_stdout_log_path(&record.command_id).expect("stdout"),
                None
            )
            .expect("stdout"),
        b"hello\n"
    );
    assert_eq!(
        sandbox_fs
            .read(
                &sandbox_command_stderr_log_path(&record.command_id).expect("stderr"),
                None
            )
            .expect("stderr"),
        b"warn\n"
    );
    assert_eq!(
        load_command_record(&mut sandbox_fs, &record.command_id).expect("record"),
        record
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn command_outputs_persist_through_sifs_sandbox_filesystem() {
    let runtime_root = unique_test_dir("sinomo-sandbox-command-outputs-sifs");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_command_outputs").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sifs_command_outputs").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let record = completed_command_record("sandbox_sifs_command_outputs");

    persist_command_outputs(&mut sandbox_fs, &record, b"hello\n", b"warn\n").expect("persist");

    assert_eq!(
        sandbox_fs
            .read(
                &sandbox_command_stdout_log_path(&record.command_id).expect("stdout"),
                None
            )
            .expect("stdout"),
        b"hello\n"
    );
    assert_eq!(
        sandbox_fs
            .read(
                &sandbox_command_stderr_log_path(&record.command_id).expect("stderr"),
                None
            )
            .expect("stderr"),
        b"warn\n"
    );
    assert_eq!(
        load_command_record(&mut sandbox_fs, &record.command_id).expect("record"),
        record
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn command_record_events_append_from_local_sandbox_logs() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_command_events_local");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let record = completed_command_record("sandbox_command_events_local");
    persist_command_outputs(&mut sandbox_fs, &record, b"hello\n", b"warn\n").expect("persist");
    let mut event_store = sinomo_events::InMemoryEventStore::new();
    let node_id = NodeId::from_legacy_named("node_sandbox_events").expect("node");

    let receipts =
        append_sandbox_command_record_events(&mut sandbox_fs, node_id.clone(), &mut event_store)
            .expect("append events");

    assert_eq!(receipts.len(), 4);
    let events = event_store
        .replay(sinomo_events::ReplayQuery::by_kind_prefix(
            "sandbox.command",
        ))
        .expect("replay");
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].event.origin_node_id, node_id);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.command.started");
    assert_eq!(
        events[0].event.payload["command_id"],
        record.command_id.to_string()
    );
    assert_eq!(events[0].event.payload["argv"][0], "echo");
    assert_eq!(events[1].event.origin_node_id, node_id);
    assert_eq!(events[1].event.kind.as_str(), "sandbox.command.output");
    assert_eq!(
        events[1].event.payload["command_id"],
        record.command_id.to_string()
    );
    assert_eq!(events[1].event.payload["stream"], "stdout");
    assert_eq!(events[1].event.payload["bytes"][0], b'h');
    assert_eq!(events[2].event.origin_node_id, node_id);
    assert_eq!(events[2].event.kind.as_str(), "sandbox.command.output");
    assert_eq!(
        events[2].event.payload["command_id"],
        record.command_id.to_string()
    );
    assert_eq!(events[2].event.payload["stream"], "stderr");
    assert_eq!(events[2].event.payload["bytes"][0], b'w');
    assert_eq!(events[3].event.origin_node_id, node_id);
    assert_eq!(events[3].event.kind.as_str(), "sandbox.command.completed");
    assert_eq!(
        events[3].event.payload["command_id"],
        record.command_id.to_string()
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn failed_command_record_events_append_raw_output_before_status() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_command_events_failed");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_command_events_failed").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("sandbox_command_events_failed_command")
        .expect("command");
    let now = OffsetDateTime::now_utc();
    let record = SandboxCommandRecord {
        sandbox_id: sandbox_id.clone(),
        command_id: command_id.clone(),
        status: SandboxCommandRecordStatus::Failed,
        workspace_root: sandbox_instance_root(&sandbox_id),
        toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("toolchain"),
        argv: vec!["false".to_string()],
        cwd: SandboxPath::new("/work").expect("cwd"),
        env: BTreeMap::new(),
        started_at: now,
        completed_at: Some(now),
        response: Some(SandboxCommandResponse {
            command_id: command_id.clone(),
            exit_status: 1,
            stdout_ref: sandbox_command_stdout_path(&sandbox_id, &command_id),
            stderr_ref: sandbox_command_stderr_path(&sandbox_id, &command_id),
            changed_paths: Vec::new(),
            resource_usage: SandboxResourceUsage::default(),
        }),
        error: Some("command exited with status 1".to_string()),
    };
    persist_command_outputs(&mut sandbox_fs, &record, b"", b"failed\n").expect("persist");
    let mut event_store = sinomo_events::InMemoryEventStore::new();
    let node_id = NodeId::from_legacy_named("node_sandbox_failed_events").expect("node");

    let receipts =
        append_sandbox_command_record_events(&mut sandbox_fs, node_id.clone(), &mut event_store)
            .expect("append events");

    assert_eq!(receipts.len(), 3);
    let events = event_store
        .replay(sinomo_events::ReplayQuery::by_kind_prefix(
            "sandbox.command",
        ))
        .expect("replay");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event.origin_node_id, node_id);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.command.started");
    assert_eq!(
        events[0].event.payload["command_id"],
        command_id.to_string()
    );
    assert_eq!(events[0].event.payload["argv"][0], "false");
    assert_eq!(events[1].event.origin_node_id, node_id);
    assert_eq!(events[1].event.kind.as_str(), "sandbox.command.output");
    assert_eq!(
        events[1].event.payload["command_id"],
        command_id.to_string()
    );
    assert_eq!(events[1].event.payload["stream"], "stderr");
    assert_eq!(events[1].event.payload["bytes"][0], b'f');
    assert_eq!(events[2].event.origin_node_id, node_id);
    assert_eq!(events[2].event.kind.as_str(), "sandbox.command.failed");
    assert_eq!(
        events[2].event.payload["command_id"],
        command_id.to_string()
    );
    assert_eq!(events[2].event.payload["exit_status"], 1);
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn command_record_events_append_from_sifs_sandbox_logs() {
    let runtime_root = unique_test_dir("sinomo-sandbox-command-events-sifs");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_command_events").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id.clone()),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sifs_command_events").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let record = completed_command_record("sandbox_sifs_command_events");
    persist_command_outputs(&mut sandbox_fs, &record, b"hello\n", b"").expect("persist");
    let mut event_store = sinomo_events::InMemoryEventStore::new();

    let receipts =
        append_sandbox_command_record_events(&mut sandbox_fs, node_id.clone(), &mut event_store)
            .expect("append events");

    assert_eq!(receipts.len(), 3);
    let events = event_store
        .replay(sinomo_events::ReplayQuery::by_kind_prefix(
            "sandbox.command",
        ))
        .expect("replay");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event.origin_node_id, node_id);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.command.started");
    assert_eq!(
        events[0].event.payload["command_id"],
        record.command_id.to_string()
    );
    assert_eq!(events[0].event.payload["argv"][0], "echo");
    assert_eq!(events[1].event.origin_node_id, node_id);
    assert_eq!(events[1].event.kind.as_str(), "sandbox.command.output");
    assert_eq!(
        events[1].event.payload["command_id"],
        record.command_id.to_string()
    );
    assert_eq!(events[1].event.payload["stream"], "stdout");
    assert_eq!(events[1].event.payload["bytes"][0], b'h');
    assert_eq!(events[2].event.origin_node_id, node_id);
    assert_eq!(events[2].event.kind.as_str(), "sandbox.command.completed");
    assert_eq!(
        events[2].event.payload["command_id"],
        record.command_id.to_string()
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn command_record_events_fetch_remote_sifs_log_content() {
    let runtime_root = unique_test_dir("sinomo-sandbox-command-events-sifs-remote");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_command_events_owner").expect("owner");
    let reader = NodeId::from_legacy_named("node_sifs_command_events_reader").expect("reader");
    let metadata_root = runtime_root.join("metadata");
    let owner_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(runtime_root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    owner_dfs
        .bootstrap_distributed_sifs()
        .expect("owner bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_command_events_remote").expect("sandbox");
    let mut owner_store =
        SifsSandboxInstanceStore::new(owner_dfs).with_replica_candidates(vec![owner.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone()]);
    let record = completed_command_record("sandbox_sifs_command_events_remote");
    let stdout_path = sandbox_command_stdout_log_path(&record.command_id).expect("stdout");
    owner_fs
        .write(&stdout_path, b"remote hello\n")
        .expect("write stdout");
    persist_command_record(&mut owner_fs, &record).expect("persist record");
    let owner_dfs = Arc::new(Mutex::new(owner_fs.into_inner()));

    let reader_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("reader metadata");
    let reader_dfs = NetworkedDfsService::new(
        network.add_node(reader.clone()),
        LocalContentStore::new(runtime_root.join("reader-content")).expect("reader content"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let mut reader_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);
    let mut event_store = sinomo_events::InMemoryEventStore::new();

    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut served = 0usize;
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves remote log reads");
                served += events
                    .iter()
                    .filter(|event| {
                        matches!(
                            event,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed { .. }
                        )
                    })
                    .count();
                if served >= 2 {
                    return served;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote log read requests"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };

    let receipts =
        append_sandbox_command_record_events(&mut reader_fs, reader.clone(), &mut event_store)
            .expect("append events");

    assert_eq!(owner_worker.join().expect("owner worker"), 2);
    assert_eq!(receipts.len(), 3);
    let events = event_store
        .replay(sinomo_events::ReplayQuery::by_kind_prefix(
            "sandbox.command",
        ))
        .expect("replay");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event.origin_node_id, reader);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.command.started");
    assert_eq!(events[1].event.kind.as_str(), "sandbox.command.output");
    assert_eq!(events[1].event.payload["stream"], "stdout");
    assert_eq!(events[1].event.payload["bytes"][0], b'r');
    assert_eq!(events[2].event.kind.as_str(), "sandbox.command.completed");
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_workspace_sync_copies_local_tree_to_sifs() {
    let (_local_manifest, local_layout, local_root) =
        create_test_instance("sandbox_sync_local_to_sifs_local");
    let mut local_fs = LocalSandboxFileSystem::new(local_layout);
    local_fs
        .create_dir_all(&SandboxPath::new("/work/src/empty").expect("empty"))
        .expect("empty dir");
    local_fs
        .write(
            &SandboxPath::new("/work/src/main.rs").expect("file"),
            b"fn main() {}\n",
        )
        .expect("write file");

    let runtime_root = unique_test_dir("sinomo-sandbox-sync-local-to-sifs");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sync_local_to_sifs").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sync_local_to_sifs").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);

    sync_sandbox_workspace_roots(&mut local_fs, &mut sifs_fs).expect("sync");

    assert_eq!(
        sifs_fs
            .read(&SandboxPath::new("/work/src/main.rs").expect("file"), None)
            .expect("read"),
        b"fn main() {}\n"
    );
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/work/src/empty").expect("empty"))
            .expect("empty")
            .is_dir
    );
    fs::remove_dir_all(local_root).ok();
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_workspace_sync_replaces_local_tree_from_sifs() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sync-sifs-to-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sync_sifs_to_local").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sync_sifs_to_local").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    sifs_fs
        .create_dir_all(&SandboxPath::new("/work/src/empty").expect("empty"))
        .expect("empty dir");
    sifs_fs
        .write(
            &SandboxPath::new("/work/src/main.rs").expect("file"),
            b"fn main() {}\n",
        )
        .expect("write file");

    let (_local_manifest, local_layout, local_root) =
        create_test_instance("sandbox_sync_sifs_to_local_staged");
    let mut local_fs = LocalSandboxFileSystem::new(local_layout);
    local_fs
        .write(
            &SandboxPath::new("/work/stale.txt").expect("stale"),
            b"stale",
        )
        .expect("stale");

    sync_sandbox_workspace_roots(&mut sifs_fs, &mut local_fs).expect("sync");

    assert_eq!(
        local_fs
            .read(&SandboxPath::new("/work/src/main.rs").expect("file"), None)
            .expect("read"),
        b"fn main() {}\n"
    );
    assert!(
        local_fs
            .stat(&SandboxPath::new("/work/src/empty").expect("empty"))
            .expect("empty")
            .is_dir
    );
    assert!(matches!(
        local_fs
            .read(&SandboxPath::new("/work/stale.txt").expect("stale"), None)
            .expect_err("stale removed"),
        SandboxFsError::NotFound { .. }
    ));
    fs::remove_dir_all(local_root).ok();
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_workspace_sync_fetches_remote_sifs_source_content() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sync-sifs-remote-to-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sync_sifs_remote_owner").expect("owner");
    let reader = NodeId::from_legacy_named("node_sync_sifs_remote_reader").expect("reader");
    let metadata_root = runtime_root.join("metadata");
    let owner_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(runtime_root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    owner_dfs
        .bootstrap_distributed_sifs()
        .expect("owner bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sync_sifs_remote_to_local").expect("sandbox");
    let mut owner_store =
        SifsSandboxInstanceStore::new(owner_dfs).with_replica_candidates(vec![owner.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone()]);
    owner_fs
        .write(
            &SandboxPath::new("/work/src/main.rs").expect("file"),
            b"fn main() { println!(\"remote\"); }\n",
        )
        .expect("owner write");
    let owner_dfs = Arc::new(Mutex::new(owner_fs.into_inner()));

    let reader_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("reader metadata");
    let reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(runtime_root.join("reader-content")).expect("reader content"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let mut reader_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);
    let (_local_manifest, local_layout, local_root) =
        create_test_instance("sandbox_sync_sifs_remote_to_local_staged");
    let mut local_fs = LocalSandboxFileSystem::new(local_layout);

    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves remote sync read");
                if events.iter().any(|event| {
                    matches!(
                        event,
                        sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed { .. }
                    )
                }) {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote sync read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };

    sync_sandbox_workspace_roots(&mut reader_fs, &mut local_fs).expect("sync");
    owner_worker.join().expect("owner worker");
    assert_eq!(
        local_fs
            .read(&SandboxPath::new("/work/src/main.rs").expect("file"), None)
            .expect("read"),
        b"fn main() { println!(\"remote\"); }\n"
    );

    fs::remove_dir_all(local_root).ok();
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_workspace_sync_preserves_sifs_symlinks() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sync-sifs-symlink-to-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sync_sifs_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sync_sifs_symlink").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("target");
    sifs_fs
        .symlink(
            Path::new("target.txt"),
            &SandboxPath::new("/work/link.txt").expect("link"),
        )
        .expect("symlink");

    let (_local_manifest, local_layout, local_root) =
        create_test_instance("sandbox_sync_sifs_symlink_to_local_staged");
    let mut local_fs = LocalSandboxFileSystem::new(local_layout);

    sync_sandbox_workspace_roots(&mut sifs_fs, &mut local_fs).expect("sync");

    let link = SandboxPath::new("/work/link.txt").expect("link");
    assert!(local_fs.stat(&link).expect("link stat").is_symlink);
    assert_eq!(
        local_fs.readlink(&link).expect("readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        local_fs
            .read(&SandboxPath::new("/work/target.txt").expect("target"), None)
            .expect("target read"),
        b"target"
    );

    fs::remove_dir_all(local_root).ok();
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_workspace_sync_preserves_sifs_file_metadata() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sync-sifs-metadata-to-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sync_sifs_metadata").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sync_sifs_metadata").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let dir = SandboxPath::new("/work/src").expect("dir");
    let file = SandboxPath::new("/work/src/main.rs").expect("file");
    sifs_fs.create_dir_all(&dir).expect("dir");
    sifs_fs.write(&file, b"fn main() {}\n").expect("file");
    sifs_fs.set_mode(&file, 0o640).expect("file mode");
    sifs_fs.set_owner(&file, 1001, 1002).expect("file owner");
    sifs_fs
        .set_times(&file, Some(12_345), Some(67_890))
        .expect("file times");
    sifs_fs.set_mode(&dir, 0o750).expect("dir mode");
    sifs_fs.set_owner(&dir, 1003, 1004).expect("dir owner");
    sifs_fs
        .set_times(&dir, Some(22_222), Some(88_888))
        .expect("dir times");

    let (_local_manifest, local_layout, local_root) =
        create_test_instance("sandbox_sync_sifs_metadata_to_local_staged");
    let mut local_fs = LocalSandboxFileSystem::new(local_layout);

    sync_sandbox_workspace_roots(&mut sifs_fs, &mut local_fs).expect("sync");

    let file_metadata = local_fs.stat(&file).expect("file metadata");
    assert_eq!(file_metadata.mode, 0o640);
    assert_eq!(file_metadata.uid, 1001);
    assert_eq!(file_metadata.gid, 1002);
    assert_eq!(file_metadata.accessed_at_unix_nanos, 12_345);
    assert_eq!(file_metadata.modified_at_unix_nanos, 67_890);
    let dir_metadata = local_fs.stat(&dir).expect("dir metadata");
    assert_eq!(dir_metadata.mode, 0o750);
    assert_eq!(dir_metadata.uid, 1003);
    assert_eq!(dir_metadata.gid, 1004);
    assert_eq!(dir_metadata.accessed_at_unix_nanos, 22_222);
    assert_eq!(dir_metadata.modified_at_unix_nanos, 88_888);

    fs::remove_dir_all(local_root).ok();
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
#[cfg(unix)]
fn sandbox_workspace_sync_preserves_sifs_hardlinks() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sync-sifs-hardlink-to-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sync_sifs_hardlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sync_sifs_hardlink").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sifs_fs.write(&original, b"linked").expect("original");
    sifs_fs.hard_link(&original, &alias).expect("hardlink");

    let (_local_manifest, local_layout, local_root) =
        create_test_instance("sandbox_sync_sifs_hardlink_to_local_staged");
    let mut local_fs = LocalSandboxFileSystem::new(local_layout);

    sync_sandbox_workspace_roots(&mut sifs_fs, &mut local_fs).expect("sync");

    let original_metadata = local_fs.stat(&original).expect("original stat");
    let alias_metadata = local_fs.stat(&alias).expect("alias stat");
    assert_eq!(original_metadata.inode, alias_metadata.inode);
    assert_eq!(original_metadata.link_count, 2);
    assert_eq!(alias_metadata.link_count, 2);
    local_fs.write(&alias, b"updated").expect("write alias");
    assert_eq!(
        local_fs.read(&original, None).expect("read original"),
        b"updated"
    );

    fs::remove_dir_all(local_root).ok();
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
#[cfg(unix)]
fn sandbox_workspace_sync_preserves_sifs_hardlinked_symlinks() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sync-sifs-hardlinked-symlink-to-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sync_sifs_hardlinked_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sync_sifs_hardlinked_symlink").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let target = SandboxPath::new("/work/target.txt").expect("target");
    let link = SandboxPath::new("/work/link.txt").expect("link");
    let alias = SandboxPath::new("/work/link-alias.txt").expect("alias");
    sifs_fs.write(&target, b"target").expect("target");
    sifs_fs
        .symlink(Path::new("target.txt"), &link)
        .expect("symlink");
    sifs_fs.hard_link(&link, &alias).expect("hardlink symlink");

    let (_local_manifest, local_layout, local_root) =
        create_test_instance("sandbox_sync_sifs_hardlinked_symlink_to_local_staged");
    let mut local_fs = LocalSandboxFileSystem::new(local_layout);

    sync_sandbox_workspace_roots(&mut sifs_fs, &mut local_fs).expect("sync");

    let link_metadata = local_fs.stat(&link).expect("link stat");
    let alias_metadata = local_fs.stat(&alias).expect("alias stat");
    assert!(link_metadata.is_symlink);
    assert!(alias_metadata.is_symlink);
    assert_eq!(link_metadata.inode, alias_metadata.inode);
    assert_eq!(link_metadata.link_count, 2);
    assert_eq!(alias_metadata.link_count, 2);
    assert_eq!(
        local_fs.readlink(&alias).expect("alias readlink"),
        PathBuf::from("target.txt")
    );
    local_fs.unlink(&link).expect("unlink link");
    assert_eq!(
        local_fs.readlink(&alias).expect("remaining alias readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        local_fs.stat(&alias).expect("remaining alias").link_count,
        1
    );

    fs::remove_dir_all(local_root).ok();
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
#[cfg(unix)]
fn local_sandbox_filesystem_follows_final_symlink_for_read_and_chmod() {
    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_local_final_symlink_follow");
    let mut local_fs = LocalSandboxFileSystem::new(layout);
    let target = SandboxPath::new("/work/target.txt").expect("target");
    let link = SandboxPath::new("/work/link.txt").expect("link");

    local_fs.write(&target, b"target").expect("write target");
    local_fs
        .symlink(Path::new("target.txt"), &link)
        .expect("symlink");

    assert!(local_fs.stat(&link).expect("link stat").is_symlink);
    assert!(
        !local_fs
            .stat_follow(&link)
            .expect("followed link stat")
            .is_symlink
    );
    assert_eq!(
        local_fs.read(&link, None).expect("read through symlink"),
        b"target"
    );

    local_fs
        .set_mode(&link, 0o600)
        .expect("chmod symlink target");
    assert_eq!(
        local_fs.stat(&target).expect("target stat").mode & 0o777,
        0o600
    );

    fs::remove_dir_all(runtime_root).ok();
}

#[test]
#[cfg(unix)]
fn sandbox_workspace_sync_preserves_identical_destination_files() {
    let (_source_manifest, source_layout, source_root) =
        create_test_instance("sandbox_sync_preserve_source");
    let mut source_fs = LocalSandboxFileSystem::new(source_layout);
    source_fs
        .write(
            &SandboxPath::new("/work/src/main.rs").expect("file"),
            b"fn main() {}\n",
        )
        .expect("source file");

    let (_destination_manifest, destination_layout, destination_root) =
        create_test_instance("sandbox_sync_preserve_destination");
    let mut destination_fs = LocalSandboxFileSystem::new(destination_layout);
    let unchanged_path = SandboxPath::new("/work/src/main.rs").expect("file");
    destination_fs
        .write(&unchanged_path, b"fn main() {}\n")
        .expect("destination file");
    destination_fs
        .write(
            &SandboxPath::new("/work/stale.txt").expect("stale"),
            b"stale",
        )
        .expect("stale");
    let unchanged_host_path = destination_fs
        .host_path(&unchanged_path)
        .expect("unchanged host path");
    let mut readonly_permissions = fs::metadata(&unchanged_host_path)
        .expect("unchanged metadata")
        .permissions();
    readonly_permissions.set_mode(0o444);
    fs::set_permissions(&unchanged_host_path, readonly_permissions).expect("readonly");

    sync_sandbox_workspace_roots(&mut source_fs, &mut destination_fs).expect("sync");

    assert_eq!(
        destination_fs
            .read(&unchanged_path, None)
            .expect("unchanged read"),
        b"fn main() {}\n"
    );
    let destination_metadata = destination_fs.stat(&unchanged_path).expect("metadata");
    assert_eq!(
        destination_metadata.mode,
        source_fs.stat(&unchanged_path).expect("source").mode
    );
    assert!(matches!(
        destination_fs
            .read(&SandboxPath::new("/work/stale.txt").expect("stale"), None)
            .expect_err("stale removed"),
        SandboxFsError::NotFound { .. }
    ));

    let mut writable_permissions = fs::metadata(&unchanged_host_path)
        .expect("unchanged metadata")
        .permissions();
    writable_permissions.set_mode(0o644);
    fs::set_permissions(&unchanged_host_path, writable_permissions).ok();
    fs::remove_dir_all(source_root).ok();
    fs::remove_dir_all(destination_root).ok();
}
