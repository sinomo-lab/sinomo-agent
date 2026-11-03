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
    let node_id = NodeId::from_legacy_named("node_sifs_instance").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_instance_store_creates_loads_and_saves_manifest",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
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
        sandbox_checkpoint_root(&sandbox_id),
        format!("{}/logs", sandbox_instance_root(&sandbox_id)),
        format!("{}/artifacts", sandbox_instance_root(&sandbox_id)),
    ] {
        dfs.posix_stat(&path).unwrap_or_else(|error| {
            panic!("{path} should exist after create: {error}");
        });
    }

    drop(dfs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_instance_store_reports_missing_manifest() {
    let node_id = NodeId::from_legacy_named("node_sifs_instance_missing").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_instance_store_reports_missing_manifest",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_missing").expect("sandbox");

    assert!(matches!(
        store.load(&sandbox_id).expect_err("missing"),
        SandboxStoreError::NotFound { .. }
    ));

    drop(store);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_instance_store_checkpoints_resumes_and_destroys() {
    let node_id = NodeId::from_legacy_named("node_sifs_instance_checkpoint").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_instance_store_checkpoints_resumes_and_destroys",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
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
    let resume_transaction_id = resumed
        .pending_reconciliation
        .as_ref()
        .expect("resume transaction")
        .transaction_id
        .clone();
    let resumed = store
        .clear_reconciliation_transaction(
            &sandbox_id,
            &resume_transaction_id,
            OffsetDateTime::now_utc(),
        )
        .expect("finalize resume");

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

    drop(store);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_instance_checkpoint_preserves_symlinks_without_copying_targets() {
    let node_id = NodeId::from_legacy_named("node_sifs_instance_checkpoint_symlink").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_instance_checkpoint_preserves_symlinks_without_copying_targets",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_checkpoint_symlink").expect("sandbox");
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
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");
    sandbox_fs
        .symlink(
            Path::new("target.txt"),
            &SandboxPath::new("/work/link.txt").expect("link"),
        )
        .expect("symlink");
    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());

    let checkpoint = store
        .create_checkpoint(&sandbox_id, OffsetDateTime::now_utc())
        .expect("checkpoint");

    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    sandbox_fs
        .unlink(&SandboxPath::new("/work/link.txt").expect("link"))
        .expect("remove link");
    sandbox_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"mutated",
        )
        .expect("mutate target");
    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());

    let resumed = store
        .resume_checkpoint(
            &sandbox_id,
            &checkpoint.checkpoint_id,
            OffsetDateTime::now_utc(),
        )
        .expect("resume");
    let sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &resumed);
    let link = SandboxPath::new("/work/link.txt").expect("link");
    let link_metadata = sandbox_fs.stat(&link).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(
        sandbox_fs.readlink(&link).expect("readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        sandbox_fs
            .read(&SandboxPath::new("/work/target.txt").expect("target"), None)
            .expect("read restored target"),
        b"target"
    );

    drop(sandbox_fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_instance_checkpoint_fetches_remote_manifest_file_and_symlink() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-instance-checkpoint-remote-symlink");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_checkpoint_remote_owner").expect("owner");
    let reader = NodeId::from_legacy_named("node_sifs_checkpoint_remote_reader").expect("reader");
    let metadata_root = runtime_root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
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
        SandboxId::from_legacy_named("sandbox_sifs_checkpoint_remote_symlink").expect("sandbox");
    let mut owner_store =
        SifsSandboxInstanceStore::new(owner_dfs).with_replica_candidates(vec![owner.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone()]);
    owner_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");
    owner_fs
        .symlink(
            Path::new("target.txt"),
            &SandboxPath::new("/work/link.txt").expect("link"),
        )
        .expect("symlink");
    let owner_dfs = Arc::new(Mutex::new(owner_fs.into_inner()));

    let reader_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("reader metadata");
    let reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(runtime_root.join("reader-content")).expect("reader content"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let mut reader_store = SifsSandboxInstanceStore::new(reader_dfs);

    let stop_owner = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let stop_owner = Arc::clone(&stop_owner);
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut served = 0usize;
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves remote checkpoint reads");
                served += events
                    .iter()
                    .filter(|event| {
                        matches!(
                            event,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed { .. }
                        )
                    })
                    .count();
                if stop_owner.load(std::sync::atomic::Ordering::Acquire) && events.is_empty() {
                    return served;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote checkpoint read requests"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let resumed = reader_store
        .create_checkpoint(&sandbox_id, OffsetDateTime::now_utc())
        .and_then(|checkpoint| {
            reader_store.resume_checkpoint(
                &sandbox_id,
                &checkpoint.checkpoint_id,
                OffsetDateTime::now_utc(),
            )
        });
    stop_owner.store(true, std::sync::atomic::Ordering::Release);
    let served = owner_worker.join().expect("owner worker");
    let resumed = resumed.expect("reader checkpoint and resume");
    assert!(
        served >= 4,
        "checkpoint should fetch its remote manifest, generation link, file, and symlink content"
    );
    let reader_fs = SifsSandboxFileSystem::new(reader_store.into_inner(), &resumed);
    let link = SandboxPath::new("/work/link.txt").expect("link");
    assert_eq!(
        reader_fs.readlink(&link).expect("reader checkpoint link"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        reader_fs
            .read(&SandboxPath::new("/work/target.txt").expect("target"), None)
            .expect("reader checkpoint target"),
        b"target"
    );

    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_instance_checkpoint_preserves_hardlinks() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-instance-checkpoint-hardlink");
    let network = InProcessNetwork::new();
    let node_id =
        NodeId::from_legacy_named("node_sifs_instance_checkpoint_hardlink").expect("node");
    let metadata_root = runtime_root.join("metadata");
    let metadata_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("metadata");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id.clone()),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    )
    .with_metadata_backend(metadata_backend)
    .expect("metadata backend");
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_checkpoint_hardlink").expect("sandbox");
    let manifest = store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");

    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sandbox_fs
        .write(&original, b"linked")
        .expect("write original");
    sandbox_fs.hard_link(&original, &alias).expect("hardlink");
    let checkpoint = {
        let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());
        store
            .create_checkpoint(&sandbox_id, OffsetDateTime::now_utc())
            .expect("checkpoint")
    };

    let reload_metadata_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("reload metadata");
    let reload_dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("reload content"),
    )
    .with_metadata_backend(reload_metadata_backend)
    .expect("reload metadata backend");
    let mut store = SifsSandboxInstanceStore::new(reload_dfs);
    let loaded = store.load(&sandbox_id).expect("load manifest");
    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &loaded);
    sandbox_fs.write(&alias, b"mutated").expect("mutate alias");
    sandbox_fs.unlink(&original).expect("unlink original");
    let mut store = SifsSandboxInstanceStore::new(sandbox_fs.into_inner());

    let resumed = store
        .resume_checkpoint(
            &sandbox_id,
            &checkpoint.checkpoint_id,
            OffsetDateTime::now_utc(),
        )
        .expect("resume");
    let sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &resumed);
    let original_stat = sandbox_fs.stat(&original).expect("original stat");
    let alias_stat = sandbox_fs.stat(&alias).expect("alias stat");
    assert_eq!(original_stat.inode, alias_stat.inode);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(
        sandbox_fs.read(&original, None).expect("read original"),
        b"linked"
    );
    assert_eq!(
        sandbox_fs.read(&alias, None).expect("read alias"),
        b"linked"
    );

    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_instance_reboot_applies_tmp_policy_without_clearing_work() {
    let node_id = NodeId::from_legacy_named("node_sifs_instance_reboot").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_instance_reboot_applies_tmp_policy_without_clearing_work",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
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
    assert_eq!(
        store
            .dfs
            .posix_read_file(&format!("{}/tmp/0/cache.txt", manifest.workspace_root))
            .expect("prior temp generation remains immutable"),
        b"tmp"
    );
    let reboot_transaction_id = rebooted
        .pending_reconciliation
        .as_ref()
        .expect("reboot transaction")
        .transaction_id
        .clone();
    let rebooted = store
        .clear_reconciliation_transaction(
            &sandbox_id,
            &reboot_transaction_id,
            OffsetDateTime::now_utc(),
        )
        .expect("finalize reboot");
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
    let retained_transaction_id = retained
        .pending_reconciliation
        .as_ref()
        .expect("retained reboot transaction")
        .transaction_id
        .clone();
    let retained = store
        .clear_reconciliation_transaction(
            &sandbox_id,
            &retained_transaction_id,
            OffsetDateTime::now_utc(),
        )
        .expect("finalize retained reboot");
    let sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &retained);
    assert_eq!(
        sandbox_fs
            .read(&SandboxPath::new("/tmp/keep.txt").expect("tmp"), None)
            .expect("read retained tmp"),
        b"keep"
    );

    drop(sandbox_fs);
    native.close().expect("normal native DFS shutdown");
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
fn local_sandbox_instance_checkpoint_preserves_symlinks_without_copying_targets() {
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

    let checkpoint = store
        .create_checkpoint(&sandbox_id, OffsetDateTime::now_utc())
        .expect("checkpoint");
    let checkpoint_link = layout
        .checkpoints
        .join(checkpoint.checkpoint_id.as_str())
        .join("work")
        .join("escape");
    assert_eq!(
        fs::symlink_metadata(&checkpoint_link)
            .expect("checkpoint link metadata")
            .file_type()
            .is_symlink(),
        true
    );
    assert_eq!(
        fs::read_link(checkpoint_link).expect("checkpoint link target"),
        std::path::PathBuf::from("/etc/passwd")
    );
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
        SANDBOX_MVP_VIRTUAL_DEVICES
            .iter()
            .map(|path| SandboxPath::new(path).expect("static virtual device path"))
            .filter(|path| {
                path.as_str()
                    .strip_prefix("/dev/")
                    .is_some_and(|suffix| !suffix.contains('/'))
            })
            .collect::<Vec<_>>()
    );
    assert!(matches!(
        fs.unlink(&null),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.set_mode(&null, 0o600),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.set_owner(&null, 1000, 1000),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.set_times(&null, Some(1), Some(2)),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.set_symlink_times(&null, Some(1), Some(2)),
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
    let parent = SandboxPath::new("/tmp/nested").expect("parent");

    fs.write(&from, b"move me").expect("write");
    assert!(matches!(
        fs.rename(&from, &to),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(fs.read(&from, None).expect("source remains"), b"move me");
    fs.create_dir(&parent).expect("mkdir parent");
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
            &mut fs,
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
            &mut fs,
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
            &mut fs,
            &SandboxPath::new("/work/a.txt").expect("a"),
            8,
            &write_limits,
        )
        .expect_err("prospective workspace bytes"),
        SandboxFsError::QuotaExceeded { ref kind, .. } if kind == "workspace-bytes"
    ));
    let usage = validate_sandbox_write_resource_limits(
        &mut fs,
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
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_maps_virtual_paths_to_sifs_workspace",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
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
    assert_eq!(fs.stat(&file).expect("file link count").link_count, 1);
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
    assert!(matches!(
        fs.rename(&file, &moved),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(
        fs.read(&file, None).expect("source remains"),
        b"fn main() {}\n"
    );
    fs.create_dir(&SandboxPath::new("/work/src").expect("src dir"))
        .expect("mkdir src");
    fs.rename(&file, &moved).expect("rename");
    assert_eq!(
        fs.stat(&SandboxPath::new("/work").expect("work"))
            .expect("work stat")
            .link_count,
        3
    );
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

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_lists_manifest_mount_points() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_mount_list").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_lists_manifest_mount_points",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let shared_dir = "/services/shared/readonly-list";
    dfs.posix_mkdir_all(shared_dir).expect("shared dir");
    dfs.posix_write_file(
        &format!("{shared_dir}/source.txt"),
        b"source",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("shared source");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_mount_list").expect("sandbox");
    let mut manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        mount_path: SandboxPath::new("/readonly").expect("readonly mount"),
        source_sifs_path: shared_dir.to_string(),
        mode: SandboxMountMode::ReadOnly,
    });
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let root_entries = fs
        .list(&SandboxPath::new("/").expect("root"))
        .expect("root list");
    assert!(
        root_entries
            .iter()
            .any(|entry| entry.path.as_str() == "/readonly" && entry.metadata.is_dir),
        "manifest mount point should be visible in root listing: {root_entries:?}"
    );
    let mounted_entries = fs
        .list(&SandboxPath::new("/readonly").expect("readonly"))
        .expect("mounted list");
    assert!(
        mounted_entries
            .iter()
            .any(|entry| entry.path.as_str() == "/readonly/source.txt"),
        "mounted directory should list mounted SIFS content: {mounted_entries:?}"
    );

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_lists_nested_manifest_mount_ancestors() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_nested_mount").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_lists_nested_manifest_mount_ancestors",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let shared_dir = "/services/shared/nested-readonly-list";
    dfs.posix_mkdir_all(shared_dir).expect("shared dir");
    dfs.posix_write_file(
        &format!("{shared_dir}/source.txt"),
        b"source",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("shared source");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_nested_mount_list").expect("sandbox");
    let mut manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        mount_path: SandboxPath::new("/opt/pkg").expect("nested mount"),
        source_sifs_path: shared_dir.to_string(),
        mode: SandboxMountMode::ReadOnly,
    });
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let root_entries = fs
        .list(&SandboxPath::new("/").expect("root"))
        .expect("root list");
    assert!(
        root_entries
            .iter()
            .any(|entry| entry.path.as_str() == "/opt" && entry.metadata.is_dir),
        "nested mount ancestor should be visible in root listing: {root_entries:?}"
    );
    let opt_metadata = fs
        .stat(&SandboxPath::new("/opt").expect("opt"))
        .expect("synthetic opt stat");
    assert!(opt_metadata.is_dir);
    assert_eq!(opt_metadata.mode & 0o222, 0);
    fs.create_dir_all(&SandboxPath::new("/opt").expect("opt"))
        .expect("mkdir -p synthetic ancestor is a no-op");
    let opt_entries = fs
        .list(&SandboxPath::new("/opt").expect("opt"))
        .expect("opt list");
    assert!(
        opt_entries
            .iter()
            .any(|entry| entry.path.as_str() == "/opt/pkg" && entry.metadata.is_dir),
        "nested mount point should be visible through synthetic ancestor: {opt_entries:?}"
    );
    assert!(
        fs.create_dir(&SandboxPath::new("/opt/new").expect("new"))
            .is_err(),
        "synthetic mount ancestors should not become writable workspace directories"
    );
    assert!(matches!(
        fs.create_dir(&SandboxPath::new("/opt").expect("opt")),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    fs.write(
        &SandboxPath::new("/work/source.txt").expect("source"),
        b"source",
    )
    .expect("write source");
    assert!(matches!(
        fs.rename(
            &SandboxPath::new("/work/source.txt").expect("source"),
            &SandboxPath::new("/opt").expect("opt")
        ),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.symlink(
            Path::new("/work/source.txt"),
            &SandboxPath::new("/opt").expect("opt")
        ),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.hard_link(
            &SandboxPath::new("/work/source.txt").expect("source"),
            &SandboxPath::new("/opt").expect("opt")
        ),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.write(
            &SandboxPath::new("/opt/new/file.txt").expect("file under synthetic ancestor"),
            b"blocked",
        ),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert_eq!(
        fs.read(&SandboxPath::new("/work/source.txt").expect("source"), None)
            .expect("source remains"),
        b"source"
    );
    assert!(fs.stat(&SandboxPath::new("/opt").expect("opt")).is_ok());
    let mounted_entries = fs
        .list(&SandboxPath::new("/opt/pkg").expect("pkg"))
        .expect("mounted list");
    assert!(
        mounted_entries
            .iter()
            .any(|entry| entry.path.as_str() == "/opt/pkg/source.txt"),
        "mounted nested directory should list mounted SIFS content: {mounted_entries:?}"
    );
    fs.create_dir_all(&SandboxPath::new("/opt/pkg").expect("pkg"))
        .expect("mkdir -p mounted directory is a no-op");
    assert!(matches!(
        fs.create_dir_all(&SandboxPath::new("/opt/pkg/new").expect("new under mount")),
        Err(SandboxFsError::ReadOnly { .. })
    ));

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_treats_mount_roots_and_ancestors_as_busy() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_mount_busy").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_treats_mount_roots_and_ancestors_as_busy",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let shared_dir = "/services/shared/mount-busy";
    dfs.posix_mkdir_all(shared_dir).expect("shared dir");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_mount_busy").expect("sandbox");
    let mut manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        mount_path: SandboxPath::new("/work/vendor/pkg").expect("mount path"),
        source_sifs_path: shared_dir.to_string(),
        mode: SandboxMountMode::ReadWrite,
    });
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let vendor = SandboxPath::new("/work/vendor").expect("vendor");
    let mount_root = SandboxPath::new("/work/vendor/pkg").expect("mount root");
    let renamed_vendor = SandboxPath::new("/work/vendor-renamed").expect("renamed vendor");
    let source_dir = SandboxPath::new("/work/source-dir").expect("source dir");
    fs.create_dir_all(&vendor).expect("vendor dir");
    fs.create_dir_all(&source_dir).expect("source dir");

    assert!(matches!(
        fs.remove_dir(&mount_root),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.rename(&mount_root, &SandboxPath::new("/work/pkg2").expect("pkg2")),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.rename(&source_dir, &mount_root),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.remove_dir(&vendor),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.rename(&vendor, &renamed_vendor),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(
        fs.stat(&vendor).expect("vendor still present").is_dir,
        "failed namespace operations must leave the mount ancestor in place"
    );
    assert!(
        fs.stat(&mount_root)
            .expect("mount root still present")
            .is_dir,
        "failed namespace operations must leave the mount root in place"
    );
    let dfs = fs.into_inner();
    assert!(
        dfs.posix_stat(shared_dir)
            .expect("shared mount source")
            .kind
            == DfsObjectKind::Directory,
        "failed namespace operations must not remove the mounted SIFS source"
    );

    drop(dfs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_clamps_absolute_parent_components_to_sandbox_root() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_parent_components").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_clamps_absolute_parent_components_to_sandbox_root",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_parent_components").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let normalized = SandboxPath::new("/../../work/./parent/../file.txt")
        .expect("absolute parent components clamp at root");
    assert_eq!(normalized.as_str(), "/work/file.txt");
    fs.write(&normalized, b"root-clamped").expect("write");
    assert_eq!(
        fs.read(
            &SandboxPath::new("/work/file.txt").expect("canonical"),
            None
        )
        .expect("canonical read"),
        b"root-clamped"
    );
    assert!(matches!(
        fs.read(
            &SandboxPath::new("/work/parent/file.txt").expect("unresolved parent"),
            None
        ),
        Err(SandboxFsError::NotFound { .. })
    ));

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_preserves_symlink_targets() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_symlink").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_preserves_symlink_targets",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_symlink").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let link = SandboxPath::new("/work/lib").expect("link");
    fs.symlink(Path::new("../shared/lib"), &link)
        .expect("symlink");

    let metadata = fs.stat(&link).expect("stat link");
    assert!(!metadata.is_dir);
    assert!(metadata.is_symlink);
    assert_eq!(metadata.len, "../shared/lib".len() as u64);
    assert_eq!(
        fs.readlink(&link).expect("readlink"),
        PathBuf::from("../shared/lib")
    );

    let target = SandboxPath::new("/shared/lib").expect("target");
    fs.write(&link, b"linked content")
        .expect("write through link");
    assert_eq!(
        fs.read(&target, None).expect("read target"),
        b"linked content"
    );
    assert_eq!(
        fs.read(&link, None).expect("read through link"),
        b"linked content"
    );
    assert_eq!(fs.stat_follow(&link).expect("stat follow").path, link);
    fs.set_times(&link, Some(12_345), Some(67_890))
        .expect("set times follows symlink target");
    assert_eq!(
        fs.stat_follow(&link)
            .expect("target atime through link")
            .accessed_at_unix_nanos,
        12_345
    );
    assert_ne!(
        fs.stat(&link)
            .expect("link atime remains independent")
            .accessed_at_unix_nanos,
        12_345
    );
    fs.set_symlink_times(&link, Some(22_222), Some(88_888))
        .expect("set symlink times updates link");
    assert_eq!(
        fs.stat(&link).expect("link atime").accessed_at_unix_nanos,
        22_222
    );
    assert_eq!(
        fs.stat_follow(&link)
            .expect("target atime unchanged by symlink times")
            .accessed_at_unix_nanos,
        12_345
    );
    assert!(matches!(
        fs.remove_dir(&link),
        Err(SandboxFsError::NotDirectory { .. })
    ));
    fs.unlink(&link).expect("unlink symlink");
    assert!(matches!(
        fs.stat(&link),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(
        fs.read(&target, None).expect("target survives link unlink"),
        b"linked content"
    );

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_unlinks_symlink_to_directory_without_removing_target() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_unlink_dir_symlink").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_unlinks_symlink_to_directory_without_removing_target",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_unlink_dir_symlink").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let target = SandboxPath::new("/work/target-dir").expect("target dir");
    let link = SandboxPath::new("/work/dir-link").expect("dir link");
    fs.create_dir(&target).expect("create target dir");
    fs.symlink(Path::new("target-dir"), &link)
        .expect("create directory symlink");

    assert!(fs.stat_follow(&link).expect("stat follows dir link").is_dir);
    assert!(matches!(
        fs.remove_dir(&link),
        Err(SandboxFsError::NotDirectory { .. })
    ));
    fs.unlink(&link).expect("unlink directory symlink");
    assert!(matches!(
        fs.stat(&link),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(fs.stat(&target).expect("target survives unlink").is_dir);

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_working_directory_validation_follows_symlink_targets() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_symlink_cwd").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_working_directory_validation_follows_symlink_targets",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_symlink_cwd").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    fs.create_dir_all(&SandboxPath::new("/work/project/src").expect("src dir"))
        .expect("create source dir");
    fs.symlink(
        Path::new("project/src"),
        &SandboxPath::new("/work/current").expect("cwd link"),
    )
    .expect("create cwd symlink");
    validate_sandbox_working_directory(
        &mut fs,
        &SandboxPath::new("/work/current").expect("cwd symlink path"),
    )
    .expect("cwd validation follows symlink to directory");

    fs.symlink(
        Path::new("missing"),
        &SandboxPath::new("/work/broken").expect("broken cwd link"),
    )
    .expect("create broken cwd symlink");
    assert!(matches!(
        validate_sandbox_working_directory(
            &mut fs,
            &SandboxPath::new("/work/broken").expect("broken cwd symlink path")
        ),
        Err(SandboxFsError::NotFound { .. })
    ));

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_hardlink_shares_content_and_unlink_state() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_hardlink").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_hardlink_shares_content_and_unlink_state",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_hardlink").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/file.txt").expect("original");
    let link = SandboxPath::new("/work/alias.txt").expect("link");
    fs.write(&original, b"original").expect("write original");
    fs.hard_link(&original, &link).expect("hardlink");

    let original_metadata = fs.stat(&original).expect("original stat");
    let link_metadata = fs.stat(&link).expect("link stat");
    assert_eq!(original_metadata.link_count, 2);
    assert_eq!(link_metadata.link_count, 2);
    assert_eq!(original_metadata.inode, link_metadata.inode);
    fs.set_mode(&link, 0o750)
        .expect("set mode through hardlink");
    assert_eq!(fs.stat(&original).expect("original mode").mode, 0o750);
    assert_eq!(fs.stat(&link).expect("link mode").mode, 0o750);
    fs.set_owner(&link, 1001, 1002)
        .expect("set owner through hardlink");
    assert_eq!(fs.stat(&original).expect("original uid").uid, 1001);
    assert_eq!(fs.stat(&original).expect("original gid").gid, 1002);
    assert_eq!(fs.stat(&link).expect("link uid").uid, 1001);
    assert_eq!(fs.stat(&link).expect("link gid").gid, 1002);
    fs.set_times(&link, Some(12_345), Some(67_890))
        .expect("set times through hardlink");
    assert_eq!(
        fs.stat(&original)
            .expect("original times")
            .accessed_at_unix_nanos,
        12_345
    );
    assert_eq!(
        fs.stat(&link).expect("link times").modified_at_unix_nanos,
        67_890
    );
    fs.write(&link, b"updated").expect("write link");
    assert_eq!(fs.read(&original, None).expect("read original"), b"updated");
    assert_eq!(fs.read(&link, None).expect("read link"), b"updated");
    let updated_metadata = fs.stat(&original).expect("updated stat");
    assert_eq!(updated_metadata.accessed_at_unix_nanos, 12_345);
    assert!(updated_metadata.modified_at_unix_nanos > 67_890);
    fs.set_times(&link, Some(22_222), Some(88_888))
        .expect("reset times before mutating read");
    assert_eq!(
        fs.read_file(&original, None).expect("mutating read"),
        b"updated"
    );
    let read_metadata = fs.stat(&link).expect("read stat");
    assert!(read_metadata.accessed_at_unix_nanos > 22_222);
    assert_eq!(read_metadata.modified_at_unix_nanos, 88_888);

    fs.unlink(&original).expect("unlink original");
    assert!(matches!(
        fs.read(&original, None),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(fs.read(&link, None).expect("link remains"), b"updated");
    assert_eq!(fs.stat(&link).expect("remaining stat").link_count, 1);

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_hardlink_preserves_symlink_object() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_hardlink_symlink").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_hardlink_preserves_symlink_object",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_hardlink_symlink").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let target = SandboxPath::new("/work/target.txt").expect("target");
    let symlink = SandboxPath::new("/work/link.txt").expect("symlink");
    let hardlink = SandboxPath::new("/work/link-alias.txt").expect("hardlink");
    fs.write(&target, b"target").expect("write target");
    fs.symlink(Path::new("target.txt"), &symlink)
        .expect("symlink");
    fs.hard_link(&symlink, &hardlink)
        .expect("hardlink symlink object");

    let symlink_metadata = fs.stat(&symlink).expect("symlink stat");
    let hardlink_metadata = fs.stat(&hardlink).expect("hardlink stat");
    assert!(symlink_metadata.is_symlink);
    assert!(hardlink_metadata.is_symlink);
    assert_eq!(symlink_metadata.inode, hardlink_metadata.inode);
    assert_eq!(symlink_metadata.link_count, 2);
    assert_eq!(hardlink_metadata.link_count, 2);
    assert_eq!(
        fs.readlink(&hardlink).expect("readlink alias"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        fs.read(&hardlink, None).expect("read through alias"),
        b"target"
    );

    fs.unlink(&symlink).expect("unlink original symlink");
    assert!(matches!(
        fs.stat(&symlink),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(
        fs.readlink(&hardlink).expect("remaining alias readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(fs.stat(&hardlink).expect("remaining alias").link_count, 1);

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_link_operations_do_not_create_missing_parents() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_link_missing_parent").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_link_operations_do_not_create_missing_parents",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_link_missing_parent").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let missing_parent = SandboxPath::new("/work/missing").expect("missing parent");
    let missing_symlink = SandboxPath::new("/work/missing/link.txt").expect("missing symlink");
    let missing_hardlink = SandboxPath::new("/work/missing/alias.txt").expect("missing hardlink");
    fs.write(&original, b"content").expect("write original");

    assert!(matches!(
        fs.symlink(Path::new("target.txt"), &missing_symlink),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(matches!(
        fs.stat(&missing_parent),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(matches!(
        fs.hard_link(&original, &missing_hardlink),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(matches!(
        fs.stat(&missing_parent),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(
        fs.read(&original, None).expect("original remains"),
        b"content"
    );

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_rename_over_canonical_preserves_other_hardlinks() {
    let node_id =
        NodeId::from_legacy_named("node_sifs_sandbox_rename_over_hardlink").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_rename_over_canonical_preserves_other_hardlinks",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_rename_over_hardlink").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let source = SandboxPath::new("/work/source.txt").expect("source");
    let canonical = SandboxPath::new("/work/canonical.txt").expect("canonical");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    fs.write(&canonical, b"old").expect("write canonical");
    let old_inode = fs.stat(&canonical).expect("canonical stat").inode;
    fs.hard_link(&canonical, &alias).expect("hardlink");
    fs.write(&source, b"new").expect("write source");
    let source_inode = fs.stat(&source).expect("source stat").inode;

    fs.rename(&source, &canonical)
        .expect("rename source over canonical");

    assert!(matches!(
        fs.stat(&source),
        Err(SandboxFsError::NotFound { .. })
    ));
    let canonical_stat = fs.stat(&canonical).expect("canonical after rename");
    let alias_stat = fs.stat(&alias).expect("alias after rename");
    assert_eq!(canonical_stat.inode, source_inode);
    assert_eq!(canonical_stat.link_count, 1);
    assert_eq!(alias_stat.inode, old_inode);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(fs.read(&canonical, None).expect("read canonical"), b"new");
    assert_eq!(fs.read(&alias, None).expect("read alias"), b"old");

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_rename_hardlink_over_canonical_preserves_both_targets() {
    let node_id =
        NodeId::from_legacy_named("node_sifs_sandbox_rename_alias_over_hardlink").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_rename_hardlink_over_canonical_preserves_both_targets",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_rename_alias_over_hardlink")
        .expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let source = SandboxPath::new("/work/source.txt").expect("source");
    let source_alias = SandboxPath::new("/work/source-alias.txt").expect("source alias");
    let dest = SandboxPath::new("/work/dest.txt").expect("dest");
    let dest_alias = SandboxPath::new("/work/dest-alias.txt").expect("dest alias");
    fs.write(&source, b"source").expect("write source");
    fs.hard_link(&source, &source_alias)
        .expect("source hardlink");
    let source_inode = fs.stat(&source).expect("source stat").inode;
    fs.write(&dest, b"dest").expect("write dest");
    fs.hard_link(&dest, &dest_alias).expect("dest hardlink");
    let dest_inode = fs.stat(&dest).expect("dest stat").inode;

    fs.rename(&source_alias, &dest)
        .expect("rename source alias over dest");

    assert!(matches!(
        fs.stat(&source_alias),
        Err(SandboxFsError::NotFound { .. })
    ));
    let source_stat = fs.stat(&source).expect("source remains");
    let renamed_stat = fs.stat(&dest).expect("renamed source alias");
    let dest_alias_stat = fs.stat(&dest_alias).expect("dest alias remains");
    assert_eq!(source_stat.inode, source_inode);
    assert_eq!(renamed_stat.inode, source_inode);
    assert_eq!(source_stat.link_count, 2);
    assert_eq!(renamed_stat.link_count, 2);
    assert_eq!(dest_alias_stat.inode, dest_inode);
    assert_eq!(dest_alias_stat.link_count, 1);
    assert_eq!(fs.read(&source, None).expect("read source"), b"source");
    assert_eq!(fs.read(&dest, None).expect("read renamed"), b"source");
    assert_eq!(
        fs.read(&dest_alias, None).expect("read dest alias"),
        b"dest"
    );

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_create_dir_is_not_recursive() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_mkdir").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_create_dir_is_not_recursive",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_mkdir").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let nested = SandboxPath::new("/work/missing/leaf").expect("nested");
    assert!(matches!(
        fs.create_dir(&nested),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(matches!(
        fs.stat(&SandboxPath::new("/work/missing").expect("missing")),
        Err(SandboxFsError::NotFound { .. })
    ));

    let parent = SandboxPath::new("/work/parent").expect("parent");
    fs.create_dir(&parent).expect("mkdir parent");
    assert!(fs.stat(&parent).expect("parent stat").is_dir);
    assert!(matches!(
        fs.create_dir(&parent),
        Err(SandboxFsError::AlreadyExists { .. })
    ));

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_create_dir_all_respects_parent_permissions() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_mkdir_all_perms").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_create_dir_all_respects_parent_permissions",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_mkdir_all_perms").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let locked = SandboxPath::new("/work/locked").expect("locked");
    let nested = SandboxPath::new("/work/locked/a/b").expect("nested");
    fs.create_dir(&locked).expect("mkdir locked");
    fs.set_mode(&locked, 0o555).expect("chmod locked");

    assert!(matches!(
        fs.create_dir_all(&nested),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.stat(&SandboxPath::new("/work/locked/a").expect("missing")),
        Err(SandboxFsError::NotFound { .. })
    ));

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_denies_hardlink_from_read_only_mount() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_hardlink_ro_source").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_denies_hardlink_from_read_only_mount",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let shared_dir = "/services/shared/readonly";
    let shared_file = "/services/shared/readonly/source.txt";
    dfs.posix_mkdir_all(shared_dir).expect("shared dir");
    dfs.posix_write_file(
        shared_file,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("shared source");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_hardlink_ro_source").expect("sandbox");
    let mut manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        mount_path: SandboxPath::new("/readonly").expect("readonly mount"),
        source_sifs_path: shared_dir.to_string(),
        mode: SandboxMountMode::ReadOnly,
    });
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    assert!(matches!(
        fs.hard_link(
            &SandboxPath::new("/readonly/source.txt").expect("source"),
            &SandboxPath::new("/work/alias.txt").expect("alias")
        ),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.stat(&SandboxPath::new("/work/alias.txt").expect("alias")),
        Err(SandboxFsError::NotFound { .. })
    ));
    let dfs = fs.into_inner();
    assert_eq!(
        dfs.posix_stat(shared_file).expect("source stat").link_count,
        1
    );

    drop(dfs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_denies_rename_across_mount_boundary() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_rename_cross_mount").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_denies_rename_across_mount_boundary",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let shared_dir = "/services/shared/readwrite";
    dfs.posix_mkdir_all(shared_dir).expect("shared dir");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_rename_cross_mount").expect("sandbox");
    let mut manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        mount_path: SandboxPath::new("/shared").expect("shared mount"),
        source_sifs_path: shared_dir.to_string(),
        mode: SandboxMountMode::ReadWrite,
    });
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let workspace_file = SandboxPath::new("/work/source.txt").expect("workspace file");
    fs.write(&workspace_file, b"workspace")
        .expect("write workspace file");
    assert!(matches!(
        fs.rename(
            &workspace_file,
            &SandboxPath::new("/shared/source.txt").expect("shared destination")
        ),
        Err(SandboxFsError::Unsupported { .. })
    ));
    assert_eq!(
        fs.read(&workspace_file, None)
            .expect("workspace file remains"),
        b"workspace"
    );
    assert!(matches!(
        fs.stat(&SandboxPath::new("/shared/source.txt").expect("shared destination")),
        Err(SandboxFsError::NotFound { .. })
    ));

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_denies_hardlink_across_mount_boundary() {
    let node_id =
        NodeId::from_legacy_named("node_sifs_sandbox_hardlink_cross_mount").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_denies_hardlink_across_mount_boundary",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let shared_dir = "/services/shared/readwrite-hardlink";
    let shared_file = "/services/shared/readwrite-hardlink/source.txt";
    dfs.posix_mkdir_all(shared_dir).expect("shared dir");
    dfs.posix_write_file(
        shared_file,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("shared source");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_hardlink_cross_mount").expect("sandbox");
    let mut manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        mount_path: SandboxPath::new("/shared").expect("shared mount"),
        source_sifs_path: shared_dir.to_string(),
        mode: SandboxMountMode::ReadWrite,
    });
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    assert!(matches!(
        fs.hard_link(
            &SandboxPath::new("/shared/source.txt").expect("source"),
            &SandboxPath::new("/work/alias.txt").expect("alias")
        ),
        Err(SandboxFsError::Unsupported { .. })
    ));
    assert!(matches!(
        fs.stat(&SandboxPath::new("/work/alias.txt").expect("alias")),
        Err(SandboxFsError::NotFound { .. })
    ));
    let dfs = fs.into_inner();
    assert_eq!(
        dfs.posix_stat(shared_file).expect("source stat").link_count,
        1
    );

    drop(dfs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_write_file_is_not_recursive() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_write_file").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_write_file_is_not_recursive",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_write_file").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let nested = SandboxPath::new("/work/missing/file.txt").expect("nested");
    assert!(matches!(
        fs.write_file(&nested, b"content"),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(matches!(
        fs.stat(&SandboxPath::new("/work/missing").expect("missing")),
        Err(SandboxFsError::NotFound { .. })
    ));
    fs.write(&nested, b"content")
        .expect("convenience write still creates parents");
    assert_eq!(fs.read(&nested, None).expect("read"), b"content");

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_follows_intermediate_symlinks_for_mutations() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_symlink_parent").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_follows_intermediate_symlinks_for_mutations",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_fs_symlink_parent").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let real_dir = SandboxPath::new("/work/real").expect("real dir");
    let link_dir = SandboxPath::new("/work/link").expect("link dir");
    fs.create_dir_all(&real_dir).expect("mkdir real");
    fs.symlink(Path::new("real"), &link_dir)
        .expect("symlink dir");

    let nested_via_link = SandboxPath::new("/work/link/nested").expect("nested via link");
    fs.create_dir_all(&nested_via_link)
        .expect("mkdir through symlink parent");
    assert!(
        fs.stat(&SandboxPath::new("/work/real/nested").unwrap())
            .unwrap()
            .is_dir
    );

    let file_via_link = SandboxPath::new("/work/link/file.txt").expect("file via link");
    let file_real = SandboxPath::new("/work/real/file.txt").expect("file real");
    fs.write(&file_via_link, b"via link")
        .expect("write via link");
    assert_eq!(fs.read(&file_real, None).expect("read real"), b"via link");

    let child_link_via_link = SandboxPath::new("/work/link/child-link").expect("child link");
    fs.symlink(Path::new("file.txt"), &child_link_via_link)
        .expect("create symlink through symlink parent");
    let child_link_real = SandboxPath::new("/work/real/child-link").expect("child link real");
    assert_eq!(
        fs.readlink(&child_link_via_link)
            .expect("readlink via link"),
        PathBuf::from("file.txt")
    );
    assert!(
        fs.stat(&child_link_real)
            .expect("stat real child link")
            .is_symlink
    );

    let renamed_via_link = SandboxPath::new("/work/link/renamed.txt").expect("renamed via link");
    let renamed_real = SandboxPath::new("/work/real/renamed.txt").expect("renamed real");
    fs.rename(&file_via_link, &renamed_via_link)
        .expect("rename through symlink parent");
    assert!(matches!(
        fs.stat(&file_real),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(
        fs.read(&renamed_real, None).expect("read renamed"),
        b"via link"
    );

    fs.unlink(&renamed_via_link)
        .expect("unlink through symlink parent");
    assert!(matches!(
        fs.stat(&renamed_real),
        Err(SandboxFsError::NotFound { .. })
    ));
    fs.remove_dir(&nested_via_link)
        .expect("remove dir through symlink parent");
    assert!(matches!(
        fs.stat(&SandboxPath::new("/work/real/nested").unwrap()),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(fs.stat(&link_dir).expect("link still exists").is_symlink);

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_exposes_virtual_devices_without_persisting_them() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_dev").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_exposes_virtual_devices_without_persisting_them",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
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
        SANDBOX_MVP_VIRTUAL_DEVICES
            .iter()
            .map(|path| SandboxPath::new(path).expect("static virtual device path"))
            .filter(|path| {
                path.as_str()
                    .strip_prefix("/dev/")
                    .is_some_and(|suffix| !suffix.contains('/'))
            })
            .collect::<Vec<_>>()
    );
    assert!(matches!(
        fs.unlink(&zero),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.set_mode(&zero, 0o600),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.set_owner(&zero, 1000, 1000),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.set_times(&zero, Some(1), Some(2)),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.set_symlink_times(&zero, Some(1), Some(2)),
        Err(SandboxFsError::ReadOnly { .. })
    ));
    assert!(matches!(
        fs.dfs
            .posix_stat(&format!("{}/dev", manifest.workspace_root)),
        Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. })
    ));

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_filesystem_reports_directory_not_empty() {
    let node_id = NodeId::from_legacy_named("node_sifs_sandbox_rmdir").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_filesystem_reports_directory_not_empty",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_fs_rmdir").expect("sandbox");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id,
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut fs = SifsSandboxFileSystem::new(dfs, &manifest);

    fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/dir").expect("dir");
    let file = SandboxPath::new("/work/dir/file.txt").expect("file");
    fs.create_dir_all(&dir).expect("mkdir");
    fs.write(&file, b"content").expect("write");

    assert!(matches!(
        fs.remove_dir(&dir),
        Err(SandboxFsError::DirectoryNotEmpty { .. })
    ));
    assert!(matches!(
        fs.unlink(&dir),
        Err(SandboxFsError::IsDirectory { .. })
    ));

    drop(fs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_workspace_can_resume_and_read_from_replica_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-cross-node-resume");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_resume_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_resume_replica").expect("replica");
    let metadata_root = runtime_root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
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
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
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
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
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
fn sifs_sandbox_read_file_requests_and_completes_remote_content_fetch() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-remote-readthrough");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_remote_read_owner").expect("owner");
    let reader = NodeId::from_legacy_named("node_sifs_remote_read_reader").expect("reader");
    let metadata_root = runtime_root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
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
        SandboxId::from_legacy_named("sandbox_sifs_remote_readthrough").expect("sandbox");
    let mut owner_store =
        SifsSandboxInstanceStore::new(owner_dfs).with_replica_candidates(vec![owner.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone()]);
    let file = SandboxPath::new("/work/remote.txt").expect("file");
    let link = SandboxPath::new("/work/remote-link.txt").expect("link");
    owner_fs
        .write(&file, b"remote progress")
        .expect("owner write");
    owner_fs
        .symlink(Path::new("remote.txt"), &link)
        .expect("owner symlink");
    let owner_dfs = Arc::new(Mutex::new(owner_fs.into_inner()));

    let reader_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("reader metadata");
    let reader_dfs = NetworkedDfsService::new(
        network.add_node(reader),
        LocalContentStore::new(runtime_root.join("reader-content")).expect("reader content"),
    )
    .with_metadata_backend(reader_backend)
    .expect("reader metadata backend");
    let stop_owner = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let owner_worker = {
        let owner_dfs = Arc::clone(&owner_dfs);
        let stop_owner = Arc::clone(&stop_owner);
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut served_events = Vec::new();
            loop {
                let events = owner_dfs
                    .lock()
                    .expect("owner dfs lock")
                    .process_incoming()
                    .expect("owner serves remote read");
                let served_now = events
                    .iter()
                    .filter(|event| {
                        matches!(
                            event,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed { .. }
                        )
                    })
                    .count();
                served_events.extend(events);
                if stop_owner.load(std::sync::atomic::Ordering::Acquire) && served_now == 0 {
                    return served_events;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner did not receive remote read request"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };
    let reader_store = SifsSandboxInstanceStore::new(reader_dfs);
    let mut reader_fs = SifsSandboxFileSystem::new(reader_store.into_inner(), &manifest);

    // Mutable metadata resolution may hydrate the remote generation-link body.
    let file_stat = reader_fs.stat_mut(&file);
    let link_stat = reader_fs.stat_mut(&link);
    let sandbox_direct_contents = reader_fs.read_file(&file, None);
    let contents = reader_fs.read_file(&link, None);
    stop_owner.store(true, std::sync::atomic::Ordering::Release);
    let owner_events = owner_worker.join().expect("owner worker");

    assert_eq!(
        file_stat.expect("remote file stat").len,
        b"remote progress".len() as u64
    );
    assert_eq!(
        link_stat.expect("remote symlink lstat").len,
        "remote.txt".len() as u64
    );
    assert_eq!(
        sandbox_direct_contents.expect("sandbox distributed file read"),
        b"remote progress"
    );

    assert_eq!(
        contents.unwrap_or_else(|error| {
            panic!(
                "reader should resolve remote symlink and remote target: {error}; \
                 owner events: {owner_events:?}"
            )
        }),
        b"remote progress"
    );
    assert!(
        owner_events.iter().any(|event| matches!(
            event,
            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed { .. }
        )),
        "owner did not serve remote read: {owner_events:?}"
    );

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

#[cfg(unix)]
#[test]
fn local_sandbox_filesystem_set_symlink_times_rejects_parent_symlink_escape() {
    use std::os::unix::fs::symlink;

    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_local_fs_symlink_times_escape");
    let outside = unique_test_dir("sinomo-sandbox-outside-times");
    fs::create_dir_all(&outside).expect("outside");
    let link = layout.work.join("escape");
    symlink(&outside, &link).expect("symlink");
    let mut fs = LocalSandboxFileSystem::new(layout);

    let path = SandboxPath::new("/work/escape/file.txt").expect("path");
    assert!(matches!(
        fs.set_symlink_times(&path, Some(1), Some(2)),
        Err(SandboxFsError::Path(SandboxPathError::EscapesRoot { .. }))
    ));

    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(outside).ok();
}
