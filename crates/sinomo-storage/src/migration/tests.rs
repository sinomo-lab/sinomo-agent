use super::stream::{MigrationStreamReader, MigrationStreamWriter, StreamCursor};
use super::*;
use crate::durable_file::install_staged_file;
use std::io::{Seek, SeekFrom, Write};
use std::process::Command;

#[derive(Default, Serialize, Deserialize)]
struct SettingsCursor {
    index: usize,
    stream: StreamCursor,
}

/// A second owner with no DFS dependencies: immutable settings generation V1
/// becomes a V2 stream while the caller's active generation stays readable.
struct SettingsMigration {
    stage: PathBuf,
    active: PathBuf,
    fail_copy_once: bool,
    fail_publish_once: bool,
    crash: Option<String>,
}

impl SettingsMigration {
    fn new(root: &Path) -> Self {
        Self {
            stage: root.join("settings.stage"),
            active: root.join("settings.v2"),
            fail_copy_once: false,
            fail_publish_once: false,
            crash: None,
        }
    }
}

fn encoded<T: Serialize>(value: &T) -> Vec<u8> {
    serde_ext::to_bytes(value, 1, LengthMode::U32).unwrap()
}

fn cursor<T: DeserializeOwned + Default>(job: &MigrationJob) -> T {
    if job.cursor.is_empty() {
        T::default()
    } else {
        serde_ext::from_bytes(&job.cursor).unwrap()
    }
}

use serde::de::DeserializeOwned;

impl MigrationDriver for SettingsMigration {
    fn copy_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> Result<MigrationBatch, String> {
        let mut position: SettingsCursor = cursor(job);
        let mut stream =
            MigrationStreamWriter::resume(&self.stage, b"settings/v2/source-A", &position.stream)
                .map_err(|e| e.to_string())?;
        let (mut records, mut bytes) = (0, 0);
        while position.index < 19 && records < step.budget.max_records && !step.should_yield() {
            let value = format!("v2/key-{}=enabled", position.index).into_bytes();
            if bytes + value.len() > step.budget.max_bytes {
                break;
            }
            stream.append(&value).map_err(|e| e.to_string())?;
            records += 1;
            bytes += value.len();
            position.index += 1;
        }
        position.stream = stream.checkpoint().map_err(|e| e.to_string())?;
        if self.crash.as_deref() == Some("copy") {
            std::process::exit(19);
        }
        if std::mem::take(&mut self.fail_copy_once) {
            return Err("injected interruption after durable output".into());
        }
        let complete = position.index == 19;
        if complete {
            stream.finish().map_err(|e| e.to_string())?;
        }
        Ok(MigrationBatch {
            cursor: encoded(&position),
            records,
            bytes,
            complete,
        })
    }

    fn verify_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> Result<MigrationBatch, String> {
        let mut reader = MigrationStreamReader::resume(&self.stage, &cursor::<StreamCursor>(job))
            .map_err(|e| e.to_string())?;
        let (mut records, mut bytes, mut complete) = (0, 0, false);
        while records < step.budget.max_records && !step.should_yield() {
            if reader.next_record_len().map_err(|e| e.to_string())? as usize
                > step.budget.max_bytes - bytes
            {
                break;
            }
            let Some(value) = reader
                .next_record(step.budget.max_bytes)
                .map_err(|e| e.to_string())?
            else {
                complete = true;
                break;
            };
            assert!(value.starts_with(b"v2/key-"));
            records += 1;
            bytes += value.len();
        }
        Ok(MigrationBatch {
            cursor: encoded(reader.cursor()),
            records,
            bytes,
            complete,
        })
    }

    fn publish(&mut self, _: &MigrationJob) -> Result<MigrationPublication, String> {
        if self.active.is_file() {
            return Ok(MigrationPublication::Published);
        }
        install_staged_file(&self.stage, &self.active, false).map_err(|e| e.to_string())?;
        if self.crash.as_deref() == Some("publish") {
            std::process::exit(19);
        }
        if std::mem::take(&mut self.fail_publish_once) {
            return Err("injected interruption after publication".into());
        }
        Ok(MigrationPublication::Published)
    }
}

fn identity() -> MigrationIdentity {
    MigrationIdentity {
        owner: "settings".into(),
        migration: "settings-v2".into(),
        scope: "account".into(),
        source_version: 1,
        target_version: 2,
        source_generation: "source-A".into(),
    }
}

fn budget() -> MigrationBudget {
    MigrationBudget {
        max_records: 3,
        max_bytes: 128,
        max_duration: Duration::from_secs(1),
    }
}

fn finish(root: &Path) -> MigrationJob {
    for _ in 0..50 {
        // Every step uses fresh handles, as after a process restart.
        let store = MigrationJobStore::new(root.join("migration.sobj"));
        let mut driver = SettingsMigration::new(root);
        let before = store.read().unwrap();
        let result = store
            .advance(&identity(), budget(), &AtomicBool::new(false), &mut driver)
            .unwrap();
        let after = store.read().unwrap().unwrap();
        assert!(after.copied_records - before.map_or(0, |j| j.copied_records) <= 3);
        if let MigrationAdvance::Complete(job) = result {
            return job;
        }
    }
    panic!("migration failed to finish");
}

fn verify_output(root: &Path) {
    let mut reader = MigrationStreamReader::open(&root.join("settings.v2")).unwrap();
    for index in 0..19 {
        assert_eq!(
            reader.next_record(128).unwrap().unwrap(),
            format!("v2/key-{index}=enabled").into_bytes()
        );
    }
    assert!(reader.next_record(128).unwrap().is_none());
}

#[test]
fn settings_adapter_resumes_bounded_copy_and_verification() {
    let root = sinomo_testdir::TestDir::new("migration-settings-resume");
    let done = finish(&root);
    assert_eq!(done.copied_records, 19);
    assert_eq!(done.verified_records, 19);
    verify_output(&root);
    assert_eq!(finish(&root), done, "completed replay has no new effects");
}

#[test]
fn durable_output_before_progress_is_replayed_without_duplicates() {
    let root = sinomo_testdir::TestDir::new("migration-output-before-cursor");
    let store = MigrationJobStore::new(root.join("migration.sobj"));
    let mut driver = SettingsMigration::new(&root);
    driver.fail_copy_once = true;
    assert!(
        store
            .advance(&identity(), budget(), &AtomicBool::new(false), &mut driver)
            .is_err()
    );
    let job = store.read().unwrap().unwrap();
    assert_eq!(job.copied_records, 0);
    assert_eq!(job.phase, MigrationPhase::Copying);
    assert!(job.last_error.is_some());
    finish(&root);
    verify_output(&root);
}

#[test]
fn publication_before_completion_is_reconciled_after_reopen() {
    let root = sinomo_testdir::TestDir::new("migration-publish-before-cursor");
    let store = MigrationJobStore::new(root.join("migration.sobj"));
    let mut driver = SettingsMigration::new(&root);
    driver.fail_publish_once = true;
    loop {
        if store
            .advance(&identity(), budget(), &AtomicBool::new(false), &mut driver)
            .is_err()
        {
            break;
        }
    }
    assert_eq!(
        store.read().unwrap().unwrap().phase,
        MigrationPhase::Publishing
    );
    verify_output(&root);
    assert_eq!(finish(&root).phase, MigrationPhase::Complete);
}

#[test]
fn cancellation_and_another_executor_never_advance_the_job() {
    let root = sinomo_testdir::TestDir::new("migration-admission");
    let store = MigrationJobStore::new(root.join("migration.sobj"));
    let mut driver = SettingsMigration::new(&root);
    assert_eq!(
        store
            .advance(&identity(), budget(), &AtomicBool::new(true), &mut driver)
            .unwrap(),
        MigrationAdvance::Yielded
    );
    assert!(store.read().unwrap().is_none());
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(store.path.with_extension("lock"))
        .unwrap();
    lock.lock_exclusive().unwrap();
    assert_eq!(
        store
            .advance(&identity(), budget(), &AtomicBool::new(false), &mut driver)
            .unwrap(),
        MigrationAdvance::Busy
    );
    assert!(store.read().unwrap().is_none());
    drop(lock);
    finish(&root);
}

#[test]
fn changed_source_generation_fails_before_effects() {
    let root = sinomo_testdir::TestDir::new("migration-source-identity");
    let store = MigrationJobStore::new(root.join("migration.sobj"));
    let mut driver = SettingsMigration::new(&root);
    store
        .advance(&identity(), budget(), &AtomicBool::new(false), &mut driver)
        .unwrap();
    let before = fs::read(&driver.stage).unwrap();
    let mut changed = identity();
    changed.source_generation = "source-B".into();
    let result = store.advance(&changed, budget(), &AtomicBool::new(false), &mut driver);
    assert!(
        matches!(result, Err(MigrationError::IdentityChanged)),
        "{result:?}"
    );
    assert_eq!(fs::read(&driver.stage).unwrap(), before);
}

#[test]
fn syntactically_valid_checkpoint_corruption_fails_before_data_effects() {
    let root = sinomo_testdir::TestDir::new("migration-checkpoint-checksum");
    let store = MigrationJobStore::new(root.join("migration.sobj"));
    let mut driver = SettingsMigration::new(&root);
    store
        .advance(&identity(), budget(), &AtomicBool::new(false), &mut driver)
        .unwrap();
    let before = fs::read(&driver.stage).unwrap();
    let mut envelope: JobEnvelope =
        serde_ext::from_bytes(&fs::read(store.path()).unwrap()).unwrap();
    envelope.job.copied_records += 1;
    fs::write(store.path(), encoded(&envelope)).unwrap();
    assert!(
        store
            .advance(&identity(), budget(), &AtomicBool::new(false), &mut driver)
            .unwrap_err()
            .to_string()
            .contains("checksum")
    );
    assert_eq!(fs::read(&driver.stage).unwrap(), before);
}

#[test]
fn stream_detects_truncation_corruption_and_uncommitted_tail() {
    let root = sinomo_testdir::TestDir::new("migration-stream-integrity");
    let path = root.join("records.stage");
    let mut writer =
        MigrationStreamWriter::resume(&path, b"header", &StreamCursor::default()).unwrap();
    writer.append(b"first").unwrap();
    let checkpoint = writer.checkpoint().unwrap();
    writer.append(b"uncheckpointed").unwrap();
    writer.checkpoint().unwrap();
    drop(writer);
    let mut writer = MigrationStreamWriter::resume(&path, b"header", &checkpoint).unwrap();
    writer.append(b"second").unwrap();
    writer.finish().unwrap();
    drop(writer);
    let mut reader = MigrationStreamReader::open(&path).unwrap();
    assert_eq!(reader.next_record(64).unwrap().unwrap(), b"first");
    assert_eq!(reader.next_record(64).unwrap().unwrap(), b"second");
    assert!(reader.next_record(64).unwrap().is_none());
    let clean = fs::read(&path).unwrap();
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(checkpoint.offset + 8)).unwrap();
    file.write_all(b"X").unwrap();
    drop(file);
    let mut reader = MigrationStreamReader::open(&path).unwrap();
    reader.next_record(64).unwrap();
    assert!(reader.next_record(64).is_err());
    fs::write(&path, &clean[..clean.len() - 1]).unwrap();
    let mut reader = MigrationStreamReader::open(&path).unwrap();
    reader.next_record(64).unwrap();
    reader.next_record(64).unwrap();
    assert!(
        reader.next_record(64).is_err(),
        "torn footer must not look complete"
    );
    fs::write(&path, &clean[..checkpoint.offset as usize - 1]).unwrap();
    assert!(MigrationStreamWriter::resume(&path, b"header", &checkpoint).is_err());
}

#[test]
fn process_death_releases_executor_and_preserves_restart_progress() {
    for point in ["copy", "publish"] {
        let root = sinomo_testdir::TestDir::new("migration-process-death");
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "migration::tests::migration_crash_child",
                "--nocapture",
            ])
            .env("SINOMO_MIGRATION_CHILD_ROOT", root.path())
            .env("SINOMO_MIGRATION_CHILD_POINT", point)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(19));
        assert_eq!(finish(&root).phase, MigrationPhase::Complete);
        verify_output(&root);
    }
}

#[test]
fn migration_crash_child() {
    let Some(root) = std::env::var_os("SINOMO_MIGRATION_CHILD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let store = MigrationJobStore::new(root.join("migration.sobj"));
    let mut driver = SettingsMigration::new(&root);
    driver.crash = std::env::var("SINOMO_MIGRATION_CHILD_POINT").ok();
    for _ in 0..50 {
        store
            .advance(&identity(), budget(), &AtomicBool::new(false), &mut driver)
            .unwrap();
    }
    panic!("child did not reach crash point");
}

#[test]
fn owner_transaction_checkpoint_can_replace_the_local_file_store() {
    #[derive(Default)]
    struct TransactionCheckpoint {
        job: Option<MigrationJob>,
        reject_progress_once: bool,
    }
    impl MigrationCheckpoint for TransactionCheckpoint {
        fn read(&mut self) -> Result<Option<MigrationJob>, MigrationError> {
            Ok(self.job.clone())
        }
        fn compare_and_store(
            &mut self,
            previous: Option<&MigrationJob>,
            next: &MigrationJob,
        ) -> Result<(), MigrationError> {
            if self.job.as_ref() != previous
                || (previous.is_some() && std::mem::take(&mut self.reject_progress_once))
            {
                return Err(MigrationError::CheckpointConflict);
            }
            self.job = Some(next.clone());
            Ok(())
        }
    }
    let root = sinomo_testdir::TestDir::new("migration-transaction-checkpoint");
    let mut repository = TransactionCheckpoint {
        reject_progress_once: true,
        ..Default::default()
    };
    let mut driver = SettingsMigration::new(&root);
    assert!(matches!(
        advance_migration(
            &mut repository,
            &identity(),
            budget(),
            &AtomicBool::new(false),
            &mut driver
        ),
        Err(MigrationError::CheckpointConflict)
    ));
    assert_eq!(repository.job.as_ref().unwrap().copied_records, 0);
    for _ in 0..50 {
        if matches!(
            advance_migration(
                &mut repository,
                &identity(),
                budget(),
                &AtomicBool::new(false),
                &mut driver
            )
            .unwrap(),
            MigrationAdvance::Complete(_)
        ) {
            verify_output(&root);
            return;
        }
    }
    panic!("transaction-backed migration did not finish");
}
