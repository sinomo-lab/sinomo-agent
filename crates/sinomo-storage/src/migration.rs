//! Restartable, owner-driven storage migrations.
//!
//! The owner supplies decoding, authority and atomic publication. This runner
//! persists admission and progress, serializes local executors across processes,
//! and never advances a cursor before the driver's output is durable. A driver
//! must make each `(identity, phase, revision)` idempotent: a process can die
//! after writing output and before the progress checkpoint. Distributed owners
//! must additionally fence effects through their existing authority protocol;
//! the local job lock is not distributed mutation authority.

pub mod stream;
#[cfg(test)]
mod tests;

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_object::{LengthMode, serde_ext};
use thiserror::Error;

use crate::durable_file::{DurableAtomicFileOptions, write_file_atomic};

const JOB_FORMAT: u16 = 1;
const MAX_JOB_BYTES: u64 = 128 * 1024;
const MAX_CURSOR_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationIdentity {
    pub owner: String,
    pub migration: String,
    pub scope: String,
    pub source_version: u32,
    pub target_version: u32,
    /// Immutable snapshot/generation identity, not a wall-clock timestamp.
    pub source_generation: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MigrationPhase {
    Copying,
    Verifying,
    Publishing,
    Complete,
    Superseded,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationJob {
    format_version: u16,
    pub identity: MigrationIdentity,
    pub phase: MigrationPhase,
    pub revision: u64,
    /// Owner-defined bounded cursor. It is reset between copy and verification.
    pub cursor: Vec<u8>,
    pub copied_records: u64,
    pub copied_bytes: u64,
    pub verified_records: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl MigrationJob {
    /// Stable effect identity for the owner's transactional/idempotent writes.
    /// An error or restart leaves this identity unchanged until progress commits.
    pub fn batch_id(&self) -> Result<String, MigrationError> {
        #[derive(Serialize)]
        struct BatchIdentity<'a> {
            identity: &'a MigrationIdentity,
            phase: MigrationPhase,
            revision: u64,
        }
        let bytes = serde_ext::to_bytes(
            &BatchIdentity {
                identity: &self.identity,
                phase: self.phase,
                revision: self.revision,
            },
            JOB_FORMAT,
            LengthMode::U32,
        )
        .map_err(storage)?;
        Ok(format!("migration:{}", blake3::hash(&bytes).to_hex()))
    }
}

#[derive(Serialize, Deserialize)]
struct JobEnvelope {
    job: MigrationJob,
    checksum: String,
}

#[derive(Clone, Copy, Debug)]
pub struct MigrationBudget {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_duration: Duration,
}

impl Default for MigrationBudget {
    fn default() -> Self {
        Self {
            max_records: 256,
            max_bytes: 8 * 1024 * 1024,
            max_duration: Duration::from_millis(50),
        }
    }
}

/// Cooperative deadline/cancellation at record boundaries. A single blocking
/// I/O or decoder call cannot be forcibly preempted by the runner.
pub struct MigrationStep<'a> {
    pub budget: MigrationBudget,
    deadline: Instant,
    cancelled: &'a AtomicBool,
}

impl MigrationStep<'_> {
    pub fn should_yield(&self) -> bool {
        self.cancelled.load(Ordering::Acquire) || Instant::now() >= self.deadline
    }

    /// Start the record-work deadline after mandatory batch preparation.
    ///
    /// Opening checkpoint files can itself exceed the cooperative time budget.
    /// Drivers may call this once after setup, before processing any records,
    /// so a slow open cannot make every retry yield without progress. Record
    /// and byte limits and the original cancellation signal remain unchanged.
    pub fn after_preparation(&self) -> Result<Self, MigrationError> {
        Ok(Self {
            budget: self.budget,
            deadline: Instant::now()
                .checked_add(self.budget.max_duration)
                .ok_or_else(|| MigrationError::Contract("deadline overflow".into()))?,
            cancelled: self.cancelled,
        })
    }
}

#[derive(Clone, Debug)]
pub struct MigrationBatch {
    pub cursor: Vec<u8>,
    pub records: usize,
    pub bytes: usize,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationPublication {
    Published,
    Superseded,
}

pub trait MigrationDriver {
    /// Return false only when this immutable source has been replaced. Loss of
    /// execution authority should return an error, so a new owner can resume.
    fn source_is_current(&mut self, _job: &MigrationJob) -> Result<bool, String> {
        Ok(true)
    }

    /// Idempotent cleanup of unpublished, owner-private output after the
    /// superseded state is durable. Never remove the active generation.
    fn discard_staging(&mut self, _job: &MigrationJob) -> Result<(), String> {
        Ok(())
    }
    /// Output must be durable before returning. Replaying the same revision
    /// must not duplicate effects or overwrite a concurrent foreground update.
    fn copy_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> Result<MigrationBatch, String>;

    fn verify_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> Result<MigrationBatch, String>;

    /// Atomically publish only if the owner's source/authority preconditions
    /// still hold. Must recognize an already published result after a crash.
    fn publish(&mut self, job: &MigrationJob) -> Result<MigrationPublication, String>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationAdvance {
    Busy,
    Yielded,
    Progress(MigrationJob),
    Complete(MigrationJob),
    Superseded(MigrationJob),
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("migration storage: {0}")]
    Storage(String),
    #[error("migration contract: {0}")]
    Contract(String),
    #[error("migration source or implementation identity changed")]
    IdentityChanged,
    #[error("migration progress changed before checkpoint publication")]
    CheckpointConflict,
    #[error("migration owner: {0}")]
    Driver(String),
}

fn storage(error: impl std::fmt::Display) -> MigrationError {
    MigrationError::Storage(error.to_string())
}

struct MigrationExecutorLock(File);

impl Drop for MigrationExecutorLock {
    fn drop(&mut self) {
        // Explicit unlock also releases a descriptor briefly inherited by a
        // concurrently spawned child before exec closes its descriptors.
        let _ = FileExt::unlock(&self.0);
    }
}

#[derive(Clone, Debug)]
pub struct MigrationJobStore {
    path: PathBuf,
}

impl MigrationJobStore {
    /// `path` belongs to the calling module's registered storage subtree.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Observation does not admit, retry, or perform migration work.
    pub fn read(&self) -> Result<Option<MigrationJob>, MigrationError> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(storage(error)),
        };
        let mut bytes = Vec::new();
        file.take(MAX_JOB_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(storage)?;
        if bytes.len() as u64 > MAX_JOB_BYTES {
            return Err(MigrationError::Contract("job exceeds size limit".into()));
        }
        let envelope: JobEnvelope = serde_ext::from_bytes(&bytes).map_err(storage)?;
        let job = envelope.job;
        let encoded = serde_ext::to_bytes(&job, JOB_FORMAT, LengthMode::U32).map_err(storage)?;
        if blake3::hash(&encoded).to_hex().as_str() != envelope.checksum {
            return Err(MigrationError::Contract(
                "job checkpoint checksum mismatch".into(),
            ));
        }
        validate_job(&job)?;
        Ok(Some(job))
    }

    fn persist(&self, job: &MigrationJob) -> Result<(), MigrationError> {
        validate_job(job)?;
        let encoded = serde_ext::to_bytes(job, JOB_FORMAT, LengthMode::U32).map_err(storage)?;
        let envelope = JobEnvelope {
            job: job.clone(),
            checksum: blake3::hash(&encoded).to_hex().to_string(),
        };
        let bytes = serde_ext::to_bytes(&envelope, JOB_FORMAT, LengthMode::U32).map_err(storage)?;
        if bytes.len() as u64 > MAX_JOB_BYTES {
            return Err(MigrationError::Contract("job exceeds size limit".into()));
        }
        write_file_atomic(
            &self.path,
            &bytes,
            DurableAtomicFileOptions::new("migration.sobj", "progress").sync_all(),
        )
        .map_err(storage)
    }

    pub fn advance(
        &self,
        identity: &MigrationIdentity,
        budget: MigrationBudget,
        cancelled: &AtomicBool,
        driver: &mut impl MigrationDriver,
    ) -> Result<MigrationAdvance, MigrationError> {
        validate_identity(identity)?;
        if budget.max_records == 0 || budget.max_bytes == 0 || budget.max_duration.is_zero() {
            return Err(MigrationError::Contract("empty batch budget".into()));
        }
        if cancelled.load(Ordering::Acquire) {
            return Ok(MigrationAdvance::Yielded);
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(storage)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.path.with_extension("lock"))
            .map_err(storage)?;
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Ok(MigrationAdvance::Busy);
            }
            Err(error) => return Err(storage(error)),
        }
        let _executor = MigrationExecutorLock(lock);
        // Dropping the descriptor releases the process lock, including after
        // process death. No time-based lease permits two local executors.
        advance_migration(
            &mut LockedFileCheckpoint { store: self },
            identity,
            budget,
            cancelled,
            driver,
        )
    }
}

/// Pluggable durable progress storage. The owner must serialize/fence the
/// executor for the duration of `advance_migration`; `compare_and_store` must
/// also atomically reject a changed predecessor. Replicated owners implement
/// this with their existing authoritative transaction path, not local files.
pub trait MigrationCheckpoint {
    fn read(&mut self) -> Result<Option<MigrationJob>, MigrationError>;
    fn compare_and_store(
        &mut self,
        previous: Option<&MigrationJob>,
        next: &MigrationJob,
    ) -> Result<(), MigrationError>;
}

struct LockedFileCheckpoint<'a> {
    store: &'a MigrationJobStore,
}

impl MigrationCheckpoint for LockedFileCheckpoint<'_> {
    fn read(&mut self) -> Result<Option<MigrationJob>, MigrationError> {
        self.store.read()
    }
    fn compare_and_store(
        &mut self,
        previous: Option<&MigrationJob>,
        next: &MigrationJob,
    ) -> Result<(), MigrationError> {
        if self.store.read()?.as_ref() != previous {
            return Err(MigrationError::CheckpointConflict);
        }
        self.store.persist(next)
    }
}

/// Run one bounded step using the owner's durable progress transaction. This
/// function never manufactures domain authority; driver effects require the
/// same owner fence as their ordinary write path.
pub fn advance_migration(
    checkpoint: &mut impl MigrationCheckpoint,
    identity: &MigrationIdentity,
    budget: MigrationBudget,
    cancelled: &AtomicBool,
    driver: &mut impl MigrationDriver,
) -> Result<MigrationAdvance, MigrationError> {
    validate_identity(identity)?;
    if budget.max_records == 0 || budget.max_bytes == 0 || budget.max_duration.is_zero() {
        return Err(MigrationError::Contract("empty batch budget".into()));
    }
    if cancelled.load(Ordering::Acquire) {
        return Ok(MigrationAdvance::Yielded);
    }
    let mut job = match checkpoint.read()? {
        Some(job) if &job.identity != identity => return Err(MigrationError::IdentityChanged),
        Some(job) => job,
        None => {
            let job = MigrationJob {
                format_version: JOB_FORMAT,
                identity: identity.clone(),
                phase: MigrationPhase::Copying,
                revision: 0,
                cursor: Vec::new(),
                copied_records: 0,
                copied_bytes: 0,
                verified_records: 0,
                last_error: None,
            };
            checkpoint.compare_and_store(None, &job)?;
            job
        }
    };
    validate_job(&job)?;
    let previous = job.clone();
    if job.phase == MigrationPhase::Complete {
        return Ok(MigrationAdvance::Complete(job));
    }
    if job.phase == MigrationPhase::Superseded {
        driver
            .discard_staging(&job)
            .map_err(MigrationError::Driver)?;
        return Ok(MigrationAdvance::Superseded(job));
    }
    let step = MigrationStep {
        budget,
        deadline: Instant::now()
            .checked_add(budget.max_duration)
            .ok_or_else(|| MigrationError::Contract("deadline overflow".into()))?,
        cancelled,
    };
    if step.should_yield() {
        return Ok(MigrationAdvance::Yielded);
    }
    if job.revision == u64::MAX {
        return Err(MigrationError::Contract("revision exhausted".into()));
    }
    let source_current = match driver.source_is_current(&job) {
        Ok(current) => current,
        Err(detail) => {
            job.last_error = Some(detail.chars().take(2048).collect());
            checkpoint.compare_and_store(Some(&previous), &job)?;
            return Err(MigrationError::Driver(detail));
        }
    };
    if !source_current {
        job.phase = MigrationPhase::Superseded;
        job.revision += 1;
        job.last_error = None;
        checkpoint.compare_and_store(Some(&previous), &job)?;
        driver
            .discard_staging(&job)
            .map_err(MigrationError::Driver)?;
        return Ok(MigrationAdvance::Superseded(job));
    }
    enum StepResult {
        Batch(MigrationBatch),
        Publication(MigrationPublication),
    }
    let outcome = match job.phase {
        MigrationPhase::Copying => driver.copy_batch(&job, &step).map(StepResult::Batch),
        MigrationPhase::Verifying => driver.verify_batch(&job, &step).map(StepResult::Batch),
        MigrationPhase::Publishing => driver.publish(&job).map(StepResult::Publication),
        MigrationPhase::Complete | MigrationPhase::Superseded => unreachable!(),
    };
    let batch = match outcome {
        Ok(batch) => batch,
        Err(error) => {
            job.last_error = Some(error.chars().take(2048).collect());
            checkpoint.compare_and_store(Some(&previous), &job)?;
            return Err(MigrationError::Driver(error));
        }
    };
    if let StepResult::Batch(batch) = batch {
        if batch.records > budget.max_records
            || batch.bytes > budget.max_bytes
            || batch.cursor.len() > MAX_CURSOR_BYTES
        {
            return Err(MigrationError::Contract(
                "driver exceeded batch bounds".into(),
            ));
        }
        if !batch.complete && batch.cursor == job.cursor {
            if batch.records == 0 && batch.bytes == 0 {
                return Ok(MigrationAdvance::Yielded);
            }
            return Err(MigrationError::Contract(
                "batch made no cursor progress".into(),
            ));
        }
        match job.phase {
            MigrationPhase::Copying => {
                job.copied_records = checked_add(job.copied_records, batch.records)?;
                job.copied_bytes = checked_add(job.copied_bytes, batch.bytes)?;
            }
            MigrationPhase::Verifying => {
                job.verified_records = checked_add(job.verified_records, batch.records)?
            }
            _ => unreachable!(),
        }
        job.cursor = batch.cursor;
        if batch.complete {
            job.phase = match job.phase {
                MigrationPhase::Copying => MigrationPhase::Verifying,
                MigrationPhase::Verifying => MigrationPhase::Publishing,
                _ => unreachable!(),
            };
            job.cursor.clear();
        }
    } else {
        job.phase = match batch {
            StepResult::Publication(MigrationPublication::Published) => MigrationPhase::Complete,
            StepResult::Publication(MigrationPublication::Superseded) => MigrationPhase::Superseded,
            StepResult::Batch(_) => unreachable!(),
        };
        job.cursor.clear();
    }
    job.revision += 1;
    job.last_error = None;
    // Even cancellation during the callback cannot discard its durable
    // result; checkpoint it before yielding to the next executor.
    checkpoint.compare_and_store(Some(&previous), &job)?;
    if job.phase == MigrationPhase::Complete {
        Ok(MigrationAdvance::Complete(job))
    } else if job.phase == MigrationPhase::Superseded {
        driver
            .discard_staging(&job)
            .map_err(MigrationError::Driver)?;
        Ok(MigrationAdvance::Superseded(job))
    } else {
        Ok(MigrationAdvance::Progress(job))
    }
}

fn checked_add(value: u64, delta: usize) -> Result<u64, MigrationError> {
    value
        .checked_add(u64::try_from(delta).map_err(storage)?)
        .ok_or_else(|| MigrationError::Contract("progress counter overflow".into()))
}

fn validate_identity(identity: &MigrationIdentity) -> Result<(), MigrationError> {
    for value in [
        &identity.owner,
        &identity.migration,
        &identity.scope,
        &identity.source_generation,
    ] {
        if value.is_empty() || value.len() > 4096 {
            return Err(MigrationError::Contract(
                "empty or oversized identity".into(),
            ));
        }
    }
    Ok(())
}

fn validate_job(job: &MigrationJob) -> Result<(), MigrationError> {
    validate_identity(&job.identity)?;
    if job.format_version != JOB_FORMAT || job.cursor.len() > MAX_CURSOR_BYTES {
        return Err(MigrationError::Contract(
            "unsupported or invalid job record".into(),
        ));
    }
    Ok(())
}
