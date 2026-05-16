use super::*;

#[test]
fn local_sandbox_instance_store_creates_phase2_layout_and_manifest() {
    let runtime_root = unique_test_dir("sinomo-sandbox-instance-store");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_instance_store").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let now = OffsetDateTime::now_utc();

    let (manifest, layout) = store
        .create(sandbox_id.clone(), toolchain_id.clone(), now)
        .expect("create");

    assert_eq!(
        manifest.workspace_root,
        format!("/services/sandbox/instances/{sandbox_id}")
    );
    assert_eq!(manifest.toolchain_id, toolchain_id);
    assert_eq!(manifest.temp_policy, SandboxTempPolicy::ClearOnSessionStart);
    assert!(layout.manifest.exists());
    for dir in layout.required_dirs() {
        assert!(dir.is_dir(), "missing {}", dir.display());
    }

    let (loaded, loaded_layout) = store.load(&sandbox_id).expect("load");
    assert_eq!(loaded, manifest);
    assert_eq!(loaded_layout, layout);
    assert!(matches!(
        store
            .create(sandbox_id.clone(), SandboxToolchainId::generate(), now)
            .expect_err("duplicate"),
        SandboxStoreError::AlreadyExists { .. }
    ));
    fs::remove_dir_all(&runtime_root).ok();
}

#[test]
fn sifs_sandbox_instance_store_creates_loads_and_saves_manifest() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-instance-store");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_instance").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_instance").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let now = OffsetDateTime::now_utc();

    let mut manifest = store
        .create(sandbox_id.clone(), toolchain_id.clone(), now)
        .expect("create");

    assert_eq!(
        manifest.workspace_root,
        format!("/services/sandbox/instances/{sandbox_id}")
    );
    assert_eq!(manifest.toolchain_id, toolchain_id);
    assert_eq!(
        store.load(&sandbox_id).expect("load created manifest"),
        manifest
    );
    assert!(matches!(
        store
            .create(sandbox_id.clone(), SandboxToolchainId::generate(), now)
            .expect_err("duplicate"),
        SandboxStoreError::AlreadyExists { .. }
    ));

    manifest.cwd = SandboxPath::new("/work/src").expect("cwd");
    manifest.temp_policy = SandboxTempPolicy::RetainAcrossSessions;
    store.save_manifest(&manifest).expect("save");
    assert_eq!(store.load(&sandbox_id).expect("load saved"), manifest);

    let dfs = store.into_inner();
    for path in [
        sandbox_manifest_path(&sandbox_id),
        format!("{}/root", sandbox_instance_root(&sandbox_id)),
        format!("{}/home", sandbox_instance_root(&sandbox_id)),
        format!("{}/tmp", sandbox_instance_root(&sandbox_id)),
        format!("{}/work", sandbox_instance_root(&sandbox_id)),
        format!("{}/checkpoints", sandbox_instance_root(&sandbox_id)),
        format!("{}/logs", sandbox_instance_root(&sandbox_id)),
        format!("{}/artifacts", sandbox_instance_root(&sandbox_id)),
    ] {
        dfs.posix_stat(&path).unwrap_or_else(|error| {
            panic!("{path} should exist after create: {error}");
        });
    }
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_instance_store_reports_missing_manifest() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-instance-missing");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_instance_missing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let store = SifsSandboxInstanceStore::new(dfs);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_missing").expect("sandbox");

    assert!(matches!(
        store.load(&sandbox_id).expect_err("missing"),
        SandboxStoreError::NotFound { .. }
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_instance_store_checkpoints_resumes_and_destroys() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-instance-checkpoint");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_instance_checkpoint").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_checkpoint").expect("sandbox");
    let manifest = store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");

    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    sandbox_fs
        .write(
            &SandboxPath::new("/work/main.rs").expect("work file"),
            b"fn main() {}\n",
        )
        .expect("write work");
    sandbox_fs
        .write(
            &SandboxPath::new("/tmp/cache.txt").expect("tmp file"),
            b"cached",
        )
        .expect("write tmp");
    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());

    let checkpoint = store
        .create_checkpoint(&sandbox_id, OffsetDateTime::now_utc())
        .expect("checkpoint");

    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    sandbox_fs
        .write(
            &SandboxPath::new("/work/main.rs").expect("work file"),
            b"mutated\n",
        )
        .expect("mutate work");
    sandbox_fs
        .unlink(&SandboxPath::new("/tmp/cache.txt").expect("tmp file"))
        .expect("remove tmp");
    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());

    let resumed = store
        .resume_checkpoint(
            &sandbox_id,
            &checkpoint.checkpoint_id,
            OffsetDateTime::now_utc(),
        )
        .expect("resume");

    let sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &resumed);
    assert_eq!(
        sandbox_fs
            .read(&SandboxPath::new("/work/main.rs").expect("work file"), None)
            .expect("read restored work"),
        b"fn main() {}\n"
    );
    assert_eq!(
        sandbox_fs
            .read(&SandboxPath::new("/tmp/cache.txt").expect("tmp file"), None)
            .expect("read restored tmp"),
        b"cached"
    );
    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());
    assert!(
        store
            .delete_checkpoint(&sandbox_id, &checkpoint.checkpoint_id)
            .expect("delete checkpoint")
    );
    assert!(
        !store
            .delete_checkpoint(&sandbox_id, &checkpoint.checkpoint_id)
            .expect("delete missing checkpoint")
    );
    store.destroy(&sandbox_id).expect("destroy");
    assert!(matches!(
        store.load(&sandbox_id).expect_err("destroyed"),
        SandboxStoreError::NotFound { .. }
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_instance_reboot_applies_tmp_policy_without_clearing_work() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-instance-reboot");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_instance_reboot").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_reboot").expect("sandbox");
    let manifest = store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");

    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    sandbox_fs
        .write(&SandboxPath::new("/work/state.txt").expect("work"), b"work")
        .expect("write work");
    sandbox_fs
        .write(&SandboxPath::new("/tmp/cache.txt").expect("tmp"), b"tmp")
        .expect("write tmp");
    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());

    let (rebooted, cleared_tmp) = store
        .reboot_session(&sandbox_id, OffsetDateTime::now_utc())
        .expect("reboot");
    assert!(cleared_tmp);
    assert_eq!(rebooted.temp_epoch, 1);
    assert!(matches!(
        store
            .dfs
            .posix_read_file(&format!("{}/tmp/0/cache.txt", manifest.workspace_root)),
        Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. })
    ));
    let sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &rebooted);
    assert_eq!(
        sandbox_fs
            .read(&SandboxPath::new("/work/state.txt").expect("work"), None)
            .expect("read work"),
        b"work"
    );
    assert!(matches!(
        sandbox_fs.read(&SandboxPath::new("/tmp/cache.txt").expect("tmp"), None),
        Err(SandboxFsError::NotFound { .. })
    ));

    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());
    let mut manifest = store.load(&sandbox_id).expect("load");
    manifest.temp_policy = SandboxTempPolicy::RetainAcrossSessions;
    store.save_manifest(&manifest).expect("save retain");
    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    sandbox_fs
        .write(&SandboxPath::new("/tmp/keep.txt").expect("tmp"), b"keep")
        .expect("write retained tmp");
    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());

    let (retained, cleared_tmp) = store
        .reboot_session(&sandbox_id, OffsetDateTime::now_utc())
        .expect("reboot retain");
    assert!(!cleared_tmp);
    assert_eq!(retained.temp_epoch, 1);
    let sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &retained);
    assert_eq!(
        sandbox_fs
            .read(&SandboxPath::new("/tmp/keep.txt").expect("tmp"), None)
            .expect("read retained tmp"),
        b"keep"
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_sandbox_instance_store_saves_updated_manifest() {
    let runtime_root = unique_test_dir("sinomo-sandbox-instance-save");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_instance_save").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let now = OffsetDateTime::now_utc();
    let (mut manifest, _) = store
        .create(sandbox_id.clone(), toolchain_id, now)
        .expect("create");

    manifest.cwd = SandboxPath::new("/home").expect("cwd");
    manifest.temp_policy = SandboxTempPolicy::RetainAcrossSessions;
    store.save_manifest(&manifest).expect("save");

    let (loaded, _) = store.load(&sandbox_id).expect("load");
    assert_eq!(loaded.cwd.as_str(), "/home");
    assert_eq!(loaded.temp_policy, SandboxTempPolicy::RetainAcrossSessions);
    fs::remove_dir_all(&runtime_root).ok();
}

#[test]
fn local_sandbox_instance_reboot_applies_tmp_policy_without_clearing_work() {
    let runtime_root = unique_test_dir("sinomo-sandbox-instance-reboot");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_reboot").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let (_manifest, layout) = store
        .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
        .expect("create");
    fs::write(layout.work.join("state.txt"), b"work").expect("write work");
    fs::write(layout.tmp.join("cache.txt"), b"tmp").expect("write tmp");

    let (rebooted, layout, cleared_tmp) = store
        .reboot_session(&sandbox_id, OffsetDateTime::now_utc())
        .expect("reboot");
    assert!(cleared_tmp);
    assert_eq!(rebooted.temp_policy, SandboxTempPolicy::ClearOnSessionStart);
    assert_eq!(
        fs::read(layout.work.join("state.txt")).expect("read work"),
        b"work"
    );
    assert!(!layout.tmp.join("cache.txt").exists());

    let mut manifest = rebooted;
    manifest.temp_policy = SandboxTempPolicy::RetainAcrossSessions;
    store.save_manifest(&manifest).expect("save retain");
    fs::write(layout.tmp.join("keep.txt"), b"keep").expect("write retained tmp");
    let (retained, layout, cleared_tmp) = store
        .reboot_session(&sandbox_id, OffsetDateTime::now_utc())
        .expect("reboot retain");
    assert!(!cleared_tmp);
    assert_eq!(
        retained.temp_policy,
        SandboxTempPolicy::RetainAcrossSessions
    );
    assert_eq!(
        fs::read(layout.tmp.join("keep.txt")).expect("read retained tmp"),
        b"keep"
    );
    fs::remove_dir_all(&runtime_root).ok();
}

#[test]
fn local_sandbox_instance_store_checkpoints_resumes_and_destroys() {
    let runtime_root = unique_test_dir("sinomo-sandbox-instance-checkpoint");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_checkpoint").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let (_manifest, layout) = store
        .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
        .expect("create");
    fs::write(layout.work.join("main.rs"), b"before").expect("write");
    fs::write(layout.tmp.join("scratch.txt"), b"tmp-before").expect("write tmp");

    let checkpoint = store
        .create_checkpoint(&sandbox_id, OffsetDateTime::now_utc())
        .expect("checkpoint");
    assert_eq!(checkpoint.sandbox_id, sandbox_id);
    assert!(checkpoint.manifest_ref.contains("/checkpoints/"));

    fs::write(layout.work.join("main.rs"), b"after").expect("overwrite");
    fs::remove_file(layout.tmp.join("scratch.txt")).expect("remove tmp");
    store
        .resume_checkpoint(
            &sandbox_id,
            &checkpoint.checkpoint_id,
            OffsetDateTime::now_utc(),
        )
        .expect("resume");
    assert_eq!(
        fs::read(layout.work.join("main.rs")).expect("work"),
        b"before"
    );
    assert_eq!(
        fs::read(layout.tmp.join("scratch.txt")).expect("tmp"),
        b"tmp-before"
    );
    assert!(
        store
            .delete_checkpoint(&sandbox_id, &checkpoint.checkpoint_id)
            .expect("delete checkpoint")
    );
    assert!(
        !layout
            .checkpoints
            .join(checkpoint.checkpoint_id.as_str())
            .exists()
    );

    let checkpoint = store
        .create_checkpoint(&sandbox_id, OffsetDateTime::now_utc())
        .expect("checkpoint before destroy");
    store.destroy(&sandbox_id).expect("destroy");
    assert!(!layout.root.exists());
    assert!(
        !layout
            .checkpoints
            .join(checkpoint.checkpoint_id.as_str())
            .exists()
    );
    assert!(matches!(
        store.load(&sandbox_id).expect_err("destroyed"),
        SandboxStoreError::NotFound { .. }
    ));
    fs::remove_dir_all(&runtime_root).ok();
}

#[cfg(unix)]
#[test]
fn local_sandbox_instance_checkpoint_rejects_symlink_escape() {
    use std::os::unix::fs::symlink;

    let runtime_root = unique_test_dir("sinomo-sandbox-instance-checkpoint-symlink");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_checkpoint_symlink").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let (_manifest, layout) = store
        .create(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
        .expect("create");
    symlink("/etc/passwd", layout.work.join("escape")).expect("symlink");

    assert!(matches!(
        store
            .create_checkpoint(&sandbox_id, OffsetDateTime::now_utc())
            .expect_err("symlink rejected"),
        SandboxStoreError::Io { .. }
    ));
    fs::remove_dir_all(&runtime_root).ok();
}

#[test]
fn local_sandbox_instance_store_reports_missing_manifest() {
    let runtime_root = unique_test_dir("sinomo-sandbox-instance-missing");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_missing").expect("sandbox");

    assert!(matches!(
        store.load(&sandbox_id).expect_err("missing"),
        SandboxStoreError::NotFound { .. }
    ));
}

#[test]
fn local_sandbox_filesystem_reads_writes_lists_and_stats_virtual_paths() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_local_fs");
    let mut fs = LocalSandboxFileSystem::new(layout.clone());
    let path = SandboxPath::new("/work/src/main.rs").expect("path");

    fs.write(&path, b"fn main() {}\n").expect("write");
    assert_eq!(fs.read(&path, None).expect("read"), b"fn main() {}\n");
    assert_eq!(
        fs.read(
            &path,
            Some(ByteRange {
                start: 3,
                end: Some(7)
            })
        )
        .expect("range"),
        b"main"
    );
    let metadata = fs.stat(&path).expect("stat");
    assert!(!metadata.is_dir);
    assert_eq!(metadata.len, 13);

    let entries = fs
        .list(&SandboxPath::new("/work/src").expect("dir"))
        .expect("list");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path.as_str(), "/work/src/main.rs");
    assert!(
        fs.list(&SandboxPath::new("/").expect("root"))
            .expect("root list")
            .iter()
            .any(|entry| entry.path.as_str() == "/work")
    );
    assert!(
        fs.list(&SandboxPath::new("/").expect("root"))
            .expect("root list")
            .iter()
            .any(|entry| entry.path.as_str() == "/dev" && entry.metadata.is_dir)
    );

    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_sandbox_filesystem_exposes_virtual_devices() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_local_fs_dev");
    let mut fs = LocalSandboxFileSystem::new(layout);
    let dev = SandboxPath::new("/dev").expect("dev");
    let null = SandboxPath::new("/dev/null").expect("null");
    let zero = SandboxPath::new("/dev/zero").expect("zero");
    let random = SandboxPath::new("/dev/random").expect("random");
    let urandom = SandboxPath::new("/dev/urandom").expect("urandom");

    assert!(fs.stat(&dev).expect("dev stat").is_dir);
    assert!(!fs.stat(&null).expect("null stat").is_dir);
    assert!(!fs.stat(&random).expect("random stat").is_dir);
    assert_eq!(fs.read(&null, None).expect("read null"), Vec::<u8>::new());
    fs.write(&null, b"discard").expect("write null");
    fs.write(&zero, b"discard").expect("write zero");
    fs.write(&random, b"discard").expect("write random");
    fs.write(&urandom, b"discard").expect("write urandom");
    assert_eq!(
        fs.read(
            &zero,
            Some(ByteRange {
                start: 3,
                end: Some(8)
            })
        )
        .expect("read zero"),
        vec![0; 5]
    );
    assert!(matches!(
        fs.read(&zero, None),
        Err(SandboxFsError::Unsupported { .. })
    ));
    assert_eq!(
        fs.list(&dev)
            .expect("list dev")
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>(),
        vec![null.clone(), zero.clone(), random.clone(), urandom.clone()]
    );
    assert!(matches!(
        fs.unlink(&null),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_sandbox_filesystem_renames_and_unlinks_inside_instance() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_local_fs_rename");
    let mut fs = LocalSandboxFileSystem::new(layout);
    let from = SandboxPath::new("/tmp/a.txt").expect("from");
    let to = SandboxPath::new("/tmp/nested/b.txt").expect("to");

    fs.write(&from, b"move me").expect("write");
    fs.rename(&from, &to).expect("rename");
    assert!(matches!(
        fs.read(&from, None),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(fs.read(&to, None).expect("read"), b"move me");
    fs.unlink(&to).expect("unlink");
    assert!(matches!(
        fs.read(&to, None),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_sandbox_filesystem_reports_changed_paths_since_snapshot() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_local_fs_changed");
    let mut fs = LocalSandboxFileSystem::new(layout);
    let created = SandboxPath::new("/work/new.txt").expect("created");
    let removed = SandboxPath::new("/tmp/remove.txt").expect("removed");

    fs.write(&removed, b"remove me").expect("write removed");
    let before = fs.snapshot().expect("snapshot with removed");
    fs.write(&created, b"created").expect("write created");
    fs.write(&removed, b"changed").expect("modify removed");
    fs.unlink(&removed).expect("remove");

    let changed = fs.changed_paths_since(&before).expect("changed");
    assert!(changed.iter().any(|path| path == &created));
    assert!(changed.iter().any(|path| path == &removed));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_sandbox_filesystem_reports_usage_and_quota_violations() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_local_fs_quota");
    let mut fs = LocalSandboxFileSystem::new(layout);
    fs.write(&SandboxPath::new("/work/a.txt").expect("a"), b"12345")
        .expect("write a");
    fs.write(&SandboxPath::new("/tmp/b.txt").expect("b"), b"123")
        .expect("write b");
    let snapshot = fs.snapshot().expect("snapshot");
    assert_eq!(
        snapshot.workspace_usage(),
        SandboxWorkspaceUsage {
            file_count: 2,
            total_bytes: 8,
        }
    );

    let mut limits = SandboxResourceLimits {
        max_file_count: 1,
        ..SandboxResourceLimits::default()
    };
    assert!(matches!(
        fs.validate_resource_limits(&limits)
            .expect_err("file count"),
        SandboxFsError::QuotaExceeded { ref kind, .. } if kind == "file-count"
    ));
    limits.max_file_count = 10;
    limits.max_workspace_bytes = 7;
    assert!(matches!(
        fs.validate_resource_limits(&limits)
            .expect_err("workspace bytes"),
        SandboxFsError::QuotaExceeded { ref kind, .. } if kind == "workspace-bytes"
    ));
    let mut write_limits = SandboxResourceLimits {
        max_file_count: 2,
        max_workspace_bytes: 10,
        ..SandboxResourceLimits::default()
    };
    assert!(matches!(
        validate_sandbox_write_resource_limits(
            &fs,
            &SandboxPath::new("/work/c.txt").expect("c"),
            3,
            &write_limits,
        )
        .expect_err("prospective file count"),
        SandboxFsError::QuotaExceeded { ref kind, .. } if kind == "file-count"
    ));
    write_limits.max_written_bytes_per_command = 2;
    assert!(matches!(
        validate_sandbox_write_resource_limits(
            &fs,
            &SandboxPath::new("/work/a.txt").expect("a"),
            3,
            &write_limits,
        )
        .expect_err("prospective written bytes"),
        SandboxFsError::QuotaExceeded { ref kind, .. } if kind == "written-bytes-per-command"
    ));
    write_limits.max_written_bytes_per_command =
        SandboxResourceLimits::default().max_written_bytes_per_command;
    write_limits.max_file_count = 3;
    assert!(matches!(
        validate_sandbox_write_resource_limits(
            &fs,
            &SandboxPath::new("/work/a.txt").expect("a"),
            8,
            &write_limits,
        )
        .expect_err("prospective workspace bytes"),
        SandboxFsError::QuotaExceeded { ref kind, .. } if kind == "workspace-bytes"
    ));
    let usage = validate_sandbox_write_resource_limits(
        &fs,
        &SandboxPath::new("/work/a.txt").expect("a"),
        2,
        &write_limits,
    )
    .expect("prospective overwrite");
    assert_eq!(
        usage,
        SandboxWorkspaceUsage {
            file_count: 2,
            total_bytes: 5,
        }
    );
    assert!(matches!(
        validate_written_bytes(9, &SandboxResourceLimits {
            max_written_bytes_per_command: 8,
            ..SandboxResourceLimits::default()
        })
        .expect_err("written bytes"),
        SandboxFsError::QuotaExceeded { ref kind, .. } if kind == "written-bytes-per-command"
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_filesystem_maps_virtual_paths_to_sifs_workspace() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-fs");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id.clone(),
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/main.rs").expect("file");
    fs.write(&file, b"fn main() {}\n").expect("write");
    assert_eq!(
        fs.read(
            &file,
            Some(ByteRange {
                start: 3,
                end: Some(7)
            })
        )
        .expect("read range"),
        b"main"
    );
    assert_eq!(fs.stat(&file).expect("stat").len, 13);
    assert!(
        fs.list(&SandboxPath::new("/work").expect("work"))
            .expect("list")
            .iter()
            .any(|entry| entry.path == file)
    );
    assert!(
        fs.list(&SandboxPath::new("/").expect("root"))
            .expect("root list")
            .iter()
            .any(|entry| entry.path.as_str() == "/dev" && entry.metadata.is_dir)
    );

    let moved = SandboxPath::new("/work/src/main.rs").expect("moved");
    fs.rename(&file, &moved).expect("rename");
    assert!(matches!(
        fs.read(&file, None).expect_err("old missing"),
        SandboxFsError::NotFound { .. }
    ));
    assert_eq!(
        fs.read(&moved, None).expect("moved read"),
        b"fn main() {}\n"
    );
    fs.unlink(&moved).expect("unlink");
    assert!(matches!(
        fs.read(&moved, None).expect_err("removed"),
        SandboxFsError::NotFound { .. }
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_filesystem_exposes_virtual_devices_without_persisting_them() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-fs-dev");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_dev").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_dev").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let dev = SandboxPath::new("/dev").expect("dev");
    let null = SandboxPath::new("/dev/null").expect("null");
    let zero = SandboxPath::new("/dev/zero").expect("zero");
    let random = SandboxPath::new("/dev/random").expect("random");
    let urandom = SandboxPath::new("/dev/urandom").expect("urandom");
    assert!(fs.stat(&dev).expect("dev stat").is_dir);
    assert_eq!(fs.read(&null, None).expect("read null"), Vec::<u8>::new());
    fs.write(&null, b"discard").expect("write null");
    fs.write(&zero, b"discard").expect("write zero");
    fs.write(&random, b"discard").expect("write random");
    fs.write(&urandom, b"discard").expect("write urandom");
    assert_eq!(
        fs.read(
            &zero,
            Some(ByteRange {
                start: 0,
                end: Some(4)
            })
        )
        .expect("read zero"),
        vec![0; 4]
    );
    assert_eq!(
        fs.list(&dev)
            .expect("list dev")
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>(),
        vec![null.clone(), zero.clone(), random.clone(), urandom.clone()]
    );
    assert!(matches!(
        fs.unlink(&zero),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.dfs
            .posix_stat(&format!("{}/dev", manifest.workspace_root)),
        Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. })
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_workspace_can_resume_and_read_from_replica_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-cross-node-resume");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_resume_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_resume_replica").expect("replica");
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
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("replica metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_cross_node_resume").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let file = SandboxPath::new("/work/progress.txt").expect("file");
    owner_fs.write(&file, b"progress").expect("write");
    let owner_dfs = owner_fs.into_inner();
    replica_dfs
        .process_incoming()
        .expect("replica receives staged content");

    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let resumed_manifest = replica_store.load(&sandbox_id).expect("load from metadata");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &resumed_manifest);

    assert_eq!(resumed_manifest.workspace_root, manifest.workspace_root);
    assert_eq!(
        replica_fs.read(&file, None).expect("replica read"),
        b"progress"
    );

    drop(owner_dfs);
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_sandbox_filesystem_rejects_unknown_virtual_root() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_local_fs_unknown");
    let mut fs = LocalSandboxFileSystem::new(layout);
    let path = SandboxPath::new("/etc/passwd").expect("path");

    assert!(matches!(
        fs.write(&path, b"nope"),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(runtime_root).ok();
}

#[cfg(unix)]
#[test]
fn local_sandbox_filesystem_rejects_symlink_escape() {
    use std::os::unix::fs::symlink;

    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_local_fs_symlink");
    let outside = unique_test_dir("sinomo-sandbox-outside");
    fs::create_dir_all(&outside).expect("outside");
    let link = layout.work.join("escape");
    symlink(&outside, &link).expect("symlink");
    let mut fs = LocalSandboxFileSystem::new(layout);

    let path = SandboxPath::new("/work/escape/file.txt").expect("path");
    assert!(matches!(
        fs.write(&path, b"blocked"),
        Err(SandboxFsError::Path(SandboxPathError::EscapesRoot { .. }))
    ));
    assert!(!outside.join("file.txt").exists());

    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(outside).ok();
}
