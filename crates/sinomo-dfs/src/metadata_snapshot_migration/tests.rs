use super::*;
use crate::metadata_table_store::{
    encode_metadata_table_snapshot, metadata_tables_from_table_snapshot,
    read_metadata_table_checkpoint_state, read_metadata_table_state,
};
use crate::{
    DfsMetadataBackend, DfsMetadataShardId, DfsObjectKind, DfsObjectMetadata,
    LocalSobjMetadataBackend,
};
use sinomo_core::{DfsObjectPolicy, Id};
use std::io::{Seek, SeekFrom, Write};
use std::time::Duration;

fn object(index: usize) -> DfsObjectMetadata {
    let owner = Id::from_legacy_named("migration-owner").unwrap();
    DfsObjectMetadata {
        object_id: Id::from_legacy_named(format!("migration-object-{index}")).unwrap(),
        file_system_id: Id::from_legacy_named("migration-fs").unwrap(),
        path: format!("/file-{index}"),
        kind: DfsObjectKind::File,
        tombstone_provenance: None,
        owner_node_id: owner.clone(),
        current_locations: vec![owner],
        content_hash: Some(format!("blake3:{}", blake3::hash(b"content").to_hex())),
        content_size_bytes: Some(7),
        link_target_object_id: None,
        mode: 0o600,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: Id::from_legacy_named(format!("migration-version-{index}")).unwrap(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    }
}

fn legacy_fixture(root: &Path, count: usize) -> (LocalSobjMetadataBackend, DfsMetadataTableState) {
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root).unwrap();
    let first = object(0);
    backend
        .put_file_system(&crate::DfsFileSystem {
            file_system_id: first.file_system_id.clone(),
            display_name: "migration".into(),
            owner_node_id: first.owner_node_id.clone(),
            subtree_policies: Vec::new(),
            file_history: Default::default(),
        })
        .unwrap();
    for index in 0..count {
        backend.put_object(&object(index)).unwrap();
    }
    let mut captured =
        crate::metadata_records::object_version_record(&first, first.content_size_bytes);
    captured.retained = Some(Box::new(crate::DfsRetainedFileVersion {
        object: Box::new(first.clone()),
        captured_at_unix_nanos: 1,
        source: crate::DfsFileVersionSource::Checkpoint {
            operation_id: "migration-checkpoint".into(),
        },
        policy_capture: None,
    }));
    backend
        .capture_file_version(
            &captured,
            &DfsMetadataVersionPinRecord {
                file_system_id: first.file_system_id.clone(),
                object_id: first.object_id.clone(),
                version: first.version.clone(),
                pin_id: "migration-pin".into(),
                created_at: time::OffsetDateTime::UNIX_EPOCH,
            },
            Some(&first.version),
        )
        .unwrap();
    let snapshot = backend.export_table_snapshot();
    let sequence = snapshot.materialized_through_sequence;
    let proof = snapshot.prefix_proof.clone();
    let tables = metadata_tables_from_table_snapshot(snapshot, "fixture").unwrap();
    // A real pre-upgrade V2 image and its original authenticated journal.
    crate::metadata_io::write_file_atomic(
        &metadata_table_snapshot_path(root),
        &encode_metadata_table_snapshot(&tables, sequence, proof.as_ref()).unwrap(),
    )
    .unwrap();
    crate::metadata_table_store::truncate_metadata_table_log(root).unwrap();
    crate::metadata_table_store::remove_metadata_table_checkpoint_log(root).unwrap();
    crate::metadata_table_apply::request_metadata_table_format_migration(root).unwrap();
    let state = read_metadata_table_checkpoint_state(root).unwrap();
    assert_eq!(state.snapshot_format_version, Some(2));
    (backend, state)
}

fn migration<'a>(root: &'a Path, state: &'a DfsMetadataTableState) -> SnapshotMigration<'a> {
    SnapshotMigration::new(
        root,
        &state.tables,
        state.materialized_through_sequence.unwrap(),
        state.prefix_proof.as_ref().unwrap(),
        state.snapshot_fingerprint.clone(),
        u32::from(state.snapshot_format_version.unwrap()),
    )
    .unwrap()
}

fn small_budget() -> MigrationBudget {
    MigrationBudget {
        max_records: 3,
        max_bytes: 64 * 1024,
        max_duration: Duration::from_secs(1),
    }
}

fn finish(root: &Path, state: &DfsMetadataTableState) -> MigrationJob {
    for _ in 0..2000 {
        let mut driver = migration(root, state);
        match driver
            .advance(small_budget(), &AtomicBool::new(false))
            .unwrap()
        {
            MigrationAdvance::Complete(job) => return job,
            MigrationAdvance::Busy => std::thread::yield_now(),
            _ => {}
        }
    }
    panic!("snapshot migration did not finish");
}

struct SlowPreparation<'a, 'b> {
    inner: &'a mut SnapshotMigration<'b>,
    cancel_after_preparation: Option<&'a AtomicBool>,
}

impl SlowPreparation<'_, '_> {
    fn prepare(&self, step: &MigrationStep<'_>) {
        // Model a checkpoint-file open that consumes the incoming deadline.
        std::thread::sleep(step.budget.max_duration + Duration::from_millis(10));
        assert!(step.should_yield());
        if let Some(cancelled) = self.cancel_after_preparation {
            cancelled.store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

impl MigrationDriver for SlowPreparation<'_, '_> {
    fn source_is_current(&mut self, job: &MigrationJob) -> std::result::Result<bool, String> {
        self.inner.source_is_current(job)
    }

    fn discard_staging(&mut self, job: &MigrationJob) -> std::result::Result<(), String> {
        self.inner.discard_staging(job)
    }

    fn copy_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> std::result::Result<MigrationBatch, String> {
        self.prepare(step);
        self.inner.copy_batch(job, step)
    }

    fn verify_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> std::result::Result<MigrationBatch, String> {
        self.prepare(step);
        self.inner.verify_batch(job, step)
    }

    fn publish(&mut self, job: &MigrationJob) -> std::result::Result<MigrationPublication, String> {
        self.inner.publish(job)
    }
}

#[test]
fn slow_preparation_does_not_starve_snapshot_copy_or_verification() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-slow-preparation");
    let (_backend, state) = legacy_fixture(&root, 8);
    let original = fs::read(metadata_table_snapshot_path(&root)).unwrap();
    let cancelled = AtomicBool::new(false);
    let budget = MigrationBudget {
        max_records: 2,
        max_duration: Duration::from_millis(50),
        ..small_budget()
    };
    let mut driver = migration(&root, &state);
    let store = driver.store();
    let identity = driver.identity();
    let delayed_advance = |driver: &mut SnapshotMigration<'_>| {
        store
            .advance(
                &identity,
                budget,
                &cancelled,
                &mut SlowPreparation {
                    inner: driver,
                    cancel_after_preparation: None,
                },
            )
            .unwrap()
    };
    let MigrationAdvance::Progress(copied) = delayed_advance(&mut driver) else {
        panic!("slow setup must leave a record-work budget for copying")
    };
    assert_eq!(
        copied.copied_records, 2,
        "record bound must remain enforced"
    );
    assert!(copied.copied_bytes <= budget.max_bytes as u64);
    driver
        .advance(MigrationBudget::default(), &cancelled)
        .unwrap();
    assert_eq!(
        store.read().unwrap().unwrap().phase,
        sinomo_core::storage::migration::MigrationPhase::Verifying
    );
    let MigrationAdvance::Progress(verified) = delayed_advance(&mut driver) else {
        panic!("slow setup must leave a record-work budget for verification")
    };
    assert_eq!(verified.verified_records, 2);
    assert_eq!(
        fs::read(metadata_table_snapshot_path(&root)).unwrap(),
        original
    );
    let complete = finish(&root, &state);
    assert_eq!(complete.copied_records, complete.verified_records);
    let reopened = read_metadata_table_state(&root).unwrap();
    assert_eq!(reopened.snapshot_format_version, Some(3));
    assert_eq!(reopened.tables, state.tables);
    assert_eq!(reopened.prefix_proof, state.prefix_proof);
}

#[test]
fn cancellation_during_snapshot_preparation_preserves_resume() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-cancel-preparation");
    let (_backend, state) = legacy_fixture(&root, 8);
    let original = fs::read(metadata_table_snapshot_path(&root)).unwrap();
    let cancelled = AtomicBool::new(false);
    let mut driver = migration(&root, &state);
    let store = driver.store();
    store
        .advance(
            &driver.identity(),
            MigrationBudget::default(),
            &cancelled,
            &mut SlowPreparation {
                inner: &mut driver,
                cancel_after_preparation: Some(&cancelled),
            },
        )
        .unwrap();
    let job = store.read().unwrap().unwrap();
    assert_eq!(job.copied_records, 0, "setup must not reset cancellation");
    assert_eq!(
        fs::read(metadata_table_snapshot_path(&root)).unwrap(),
        original
    );
    let complete = finish(&root, &state);
    assert_eq!(complete.copied_records, complete.verified_records);
    assert_eq!(
        read_metadata_table_state(&root).unwrap().tables,
        state.tables
    );
}

#[test]
fn legacy_reader_and_bounded_migration_preserve_full_metadata_and_prefix() {
    let root = sinomo_testdir::TestDir::new("dfs-snapshot-migration");
    let (_backend, state) = legacy_fixture(&root, 31);
    let original = fs::read(metadata_table_snapshot_path(&root)).unwrap();
    let before = crate::metadata_table_store::load_or_migrate_metadata_tables(&root).unwrap();
    assert_eq!(
        fs::read(metadata_table_snapshot_path(&root)).unwrap(),
        original,
        "read path must not rewrite the old snapshot"
    );
    let mut driver = migration(&root, &state);
    let MigrationAdvance::Progress(first) = driver
        .advance(small_budget(), &AtomicBool::new(false))
        .unwrap()
    else {
        panic!("expected first batch")
    };
    assert_eq!(first.copied_records, 3);
    assert_eq!(
        first.phase,
        sinomo_core::storage::migration::MigrationPhase::Copying
    );
    assert_eq!(
        fs::read(metadata_table_snapshot_path(&root)).unwrap(),
        original
    );
    let job = finish(&root, &state);
    assert!(job.copied_records > 31);
    assert_eq!(job.copied_records, job.verified_records);
    let after = read_metadata_table_state(&root).unwrap();
    assert_eq!(after.snapshot_format_version, Some(3));
    assert_eq!(after.tables, before.tables);
    assert_eq!(after.prefix_proof, before.prefix_proof);
    assert_eq!(
        after.materialized_through_sequence,
        before.materialized_through_sequence
    );
    assert_eq!(status(&root).unwrap(), Some(job));
}

#[test]
fn foreground_updates_and_pin_removal_survive_background_conversion() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-concurrent-write");
    let (mut backend, state) = legacy_fixture(&root, 12);
    migration(&root, &state)
        .advance(small_budget(), &AtomicBool::new(false))
        .unwrap();
    let first = object(0);
    let mut next = first.clone();
    next.mode = 0o640;
    next.version = Id::generate();
    backend.put_object(&next).unwrap();
    backend
        .remove_version_pin(
            &first.file_system_id,
            &first.object_id,
            &first.version,
            "migration-pin",
        )
        .unwrap();
    finish(&root, &state);
    crate::metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(&root);
    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    assert_eq!(reopened.get_object(&first.object_id).unwrap(), Some(next));
    assert!(
        reopened
            .version_pins_for_object(&first.object_id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened.journal_last_sequence(),
        backend.journal_last_sequence()
    );
}

#[test]
fn corrupted_staging_output_cannot_replace_readable_legacy_data() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-corrupt-stage");
    let (_backend, state) = legacy_fixture(&root, 6);
    let original = fs::read(metadata_table_snapshot_path(&root)).unwrap();
    let mut driver = migration(&root, &state);
    while driver
        .store()
        .read()
        .unwrap()
        .is_none_or(|job| job.phase == sinomo_core::storage::migration::MigrationPhase::Copying)
    {
        driver
            .advance(small_budget(), &AtomicBool::new(false))
            .unwrap();
    }
    let reader = MigrationStreamReader::open(&driver.stage).unwrap();
    let first_payload = reader.cursor().offset + 8;
    drop(reader);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(&driver.stage)
        .unwrap();
    file.seek(SeekFrom::Start(first_payload)).unwrap();
    file.write_all(b"BAD").unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(
        driver
            .advance(small_budget(), &AtomicBool::new(false))
            .is_err()
    );
    assert!(
        driver
            .store()
            .read()
            .unwrap()
            .unwrap()
            .last_error
            .unwrap()
            .contains("checksum")
    );
    assert_eq!(
        fs::read(metadata_table_snapshot_path(&root)).unwrap(),
        original
    );
    assert_eq!(
        read_metadata_table_state(&root).unwrap().tables,
        state.tables
    );
}

#[test]
fn superseded_generation_retires_only_its_unpublished_staging_file() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-superseded");
    let (_backend, state) = legacy_fixture(&root, 8);
    let mut driver = migration(&root, &state);
    driver
        .advance(small_budget(), &AtomicBool::new(false))
        .unwrap();
    assert!(driver.stage.is_file());
    let next_sequence = state.materialized_through_sequence.unwrap() + 1;
    let proof = crate::metadata_records::dfs_metadata_prefix_proof_seed(
        &DfsMetadataShardId::new("root"),
        next_sequence,
        &crate::metadata_table_store::snapshot_semantic_fingerprint(
            &state.tables,
            Some(next_sequence),
        )
        .unwrap(),
    );
    {
        let _lock = crate::metadata_lock::metadata_checkpoint_write_lock(
            root.path().to_path_buf(),
            crate::metadata_cursor::journal_write_lock_path(&root),
        )
        .unwrap();
        persist_snapshot(&root, &state.tables, Some(next_sequence), Some(&proof)).unwrap();
    }
    let installed = fs::read(metadata_table_snapshot_path(&root)).unwrap();
    assert!(matches!(
        driver
            .advance(small_budget(), &AtomicBool::new(false))
            .unwrap(),
        MigrationAdvance::Superseded(_)
    ));
    assert!(!driver.stage.exists());
    assert_eq!(
        fs::read(metadata_table_snapshot_path(&root)).unwrap(),
        installed
    );
    assert_eq!(
        status(&root).unwrap().unwrap().phase,
        sinomo_core::storage::migration::MigrationPhase::Superseded
    );
}

#[test]
fn publication_before_job_completion_resumes_with_original_source_identity() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-published-restart");
    let (_backend, state) = legacy_fixture(&root, 4);
    let mut driver = migration(&root, &state);
    loop {
        let result = driver
            .advance(small_budget(), &AtomicBool::new(false))
            .unwrap();
        if let MigrationAdvance::Progress(job) = result
            && job.phase == sinomo_core::storage::migration::MigrationPhase::Publishing
        {
            break;
        }
    }
    let job = driver.store().read().unwrap().unwrap();
    driver.publish(&job).unwrap(); // Simulate process death before the runner checkpoints completion.
    drop(driver);
    let installed = read_metadata_table_checkpoint_state(&root).unwrap();
    assert_eq!(installed.snapshot_format_version, Some(3));
    let completed = finish(&root, &installed);
    assert_eq!(completed.identity.source_version, 2);
    assert_eq!(
        completed.phase,
        sinomo_core::storage::migration::MigrationPhase::Complete
    );
    assert_eq!(
        read_metadata_table_state(&root).unwrap().tables,
        state.tables
    );
}

#[test]
fn legacy_v1_snapshot_is_read_without_rewrite_and_migrates_in_batches() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-v1-snapshot");
    let (_backend, state) = legacy_fixture(&root, 4);
    let mut ops = crate::metadata_table_store::metadata_table_snapshot_ops(&state.tables);
    ops.extend(
        state
            .tables
            .version_pins
            .values()
            .cloned()
            .map(|pin| DfsMetadataTableOp::PutVersionPin { pin }),
    );
    let legacy = DfsMetadataTableSnapshot {
        format_version: 1,
        materialized_through_sequence: state.materialized_through_sequence,
        tables: None,
        ops,
        semantic_state_fingerprint: None,
        prefix_proof: state.prefix_proof.clone(),
    };
    write_sobj_atomic(&metadata_table_snapshot_path(&root), &legacy).unwrap();
    let bytes = fs::read(metadata_table_snapshot_path(&root)).unwrap();
    let legacy_state = crate::metadata_table_store::load_or_migrate_metadata_tables(&root).unwrap();
    assert_eq!(legacy_state.snapshot_format_version, Some(1));
    assert_eq!(
        fs::read(metadata_table_snapshot_path(&root)).unwrap(),
        bytes
    );
    assert_eq!(finish(&root, &legacy_state).identity.source_version, 1);
    assert_eq!(
        read_metadata_table_state(&root).unwrap().tables,
        state.tables
    );
}

#[test]
fn log_only_store_below_compaction_threshold_is_migrated_on_reopen() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-log-only");
    let mut backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    let file = object(0);
    backend
        .put_file_system(&crate::DfsFileSystem {
            file_system_id: file.file_system_id.clone(),
            display_name: "log-only".into(),
            owner_node_id: file.owner_node_id.clone(),
            subtree_policies: Vec::new(),
            file_history: Default::default(),
        })
        .unwrap();
    backend.put_object(&file).unwrap();
    crate::metadata_io::remove_file_if_exists(&metadata_table_snapshot_path(&root)).unwrap();
    let expected_head = backend.journal_last_sequence();
    drop(backend);
    // Preserve real batch framing and payloads, but reproduce the old writer's
    // document version. A current-format log must not trigger eager compaction.
    let path = crate::metadata_table_store::metadata_table_path(&root);
    assert!(
        !crate::metadata_table_store::metadata_table_log_needs_format_migration(&path).unwrap()
    );
    let mut bytes = fs::read(&path).unwrap();
    let mut offset = 0;
    while offset < bytes.len() {
        let length = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        bytes[offset + 8..offset + 10].copy_from_slice(&1u16.to_le_bytes());
        offset += 4 + length;
    }
    crate::metadata_io::write_file_atomic(&path, &bytes).unwrap();
    assert!(crate::metadata_table_store::metadata_table_log_needs_format_migration(&path).unwrap());
    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    crate::metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(&root);
    assert_eq!(reopened.get_object(&file.object_id).unwrap(), Some(file));
    assert_eq!(reopened.journal_last_sequence(), expected_head);
    assert_eq!(
        read_metadata_table_state(&root)
            .unwrap()
            .snapshot_format_version,
        Some(3)
    );
    assert_eq!(
        reopened.storage_migration_status().unwrap().unwrap().phase,
        sinomo_core::storage::migration::MigrationPhase::Complete
    );
}

#[test]
fn process_death_during_copy_resumes_automatically_on_writer_reopen() {
    let root = sinomo_testdir::TestDir::new("dfs-migration-process-reopen");
    let (backend, state) = legacy_fixture(&root, 20);
    drop(backend);
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "metadata_snapshot_migration::tests::snapshot_migration_crash_child",
            "--nocapture",
        ])
        .env("SINOMO_DFS_MIGRATION_CHILD_ROOT", root.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert_eq!(result.code(), Some(19));
    let partial = status(&root).unwrap().unwrap();
    assert_eq!(partial.copied_records, 3);
    assert_eq!(
        partial.phase,
        sinomo_core::storage::migration::MigrationPhase::Copying
    );
    assert_eq!(
        read_metadata_table_state(&root).unwrap().tables,
        state.tables
    );
    let reopened = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &root).unwrap();
    crate::metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(&root);
    assert_eq!(
        reopened.storage_migration_status().unwrap().unwrap().phase,
        sinomo_core::storage::migration::MigrationPhase::Complete
    );
    let migrated = read_metadata_table_state(&root).unwrap();
    assert_eq!(migrated.snapshot_format_version, Some(3));
    assert_eq!(migrated.tables, state.tables);
    assert_eq!(migrated.prefix_proof, state.prefix_proof);
}

#[test]
fn snapshot_migration_crash_child() {
    let Some(root) = std::env::var_os("SINOMO_DFS_MIGRATION_CHILD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let state = read_metadata_table_checkpoint_state(&root).unwrap();
    let mut driver = migration(&root, &state);
    driver
        .advance(small_budget(), &AtomicBool::new(false))
        .unwrap();
    struct CrashAfterCopy<'a, 'b>(&'a mut SnapshotMigration<'b>);
    impl MigrationDriver for CrashAfterCopy<'_, '_> {
        fn copy_batch(
            &mut self,
            job: &MigrationJob,
            step: &MigrationStep<'_>,
        ) -> std::result::Result<MigrationBatch, String> {
            self.0.copy_batch(job, step)?;
            std::process::exit(19);
        }
        fn verify_batch(
            &mut self,
            job: &MigrationJob,
            step: &MigrationStep<'_>,
        ) -> std::result::Result<MigrationBatch, String> {
            self.0.verify_batch(job, step)
        }
        fn publish(
            &mut self,
            job: &MigrationJob,
        ) -> std::result::Result<MigrationPublication, String> {
            self.0.publish(job)
        }
    }
    driver
        .store()
        .advance(
            &driver.identity(),
            small_budget(),
            &AtomicBool::new(false),
            &mut CrashAfterCopy(&mut driver),
        )
        .unwrap();
    panic!("migration child did not reach its crash point");
}
