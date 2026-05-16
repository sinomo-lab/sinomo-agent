use super::*;

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
        load_command_record(&sandbox_fs, &record.command_id).expect("record"),
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
        load_command_record(&sandbox_fs, &record.command_id).expect("record"),
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
        append_sandbox_command_record_events(&sandbox_fs, node_id.clone(), &mut event_store)
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
        append_sandbox_command_record_events(&sandbox_fs, node_id.clone(), &mut event_store)
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
        append_sandbox_command_record_events(&sandbox_fs, node_id.clone(), &mut event_store)
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

    sync_sandbox_workspace_roots(&local_fs, &mut sifs_fs).expect("sync");

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

    sync_sandbox_workspace_roots(&sifs_fs, &mut local_fs).expect("sync");

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

    sync_sandbox_workspace_roots(&source_fs, &mut destination_fs).expect("sync");

    assert_eq!(
        destination_fs
            .read(&unchanged_path, None)
            .expect("unchanged read"),
        b"fn main() {}\n"
    );
    assert_eq!(
        fs::metadata(&unchanged_host_path)
            .expect("unchanged metadata")
            .permissions()
            .mode()
            & 0o777,
        0o444
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
