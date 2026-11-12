//! Local acknowledgement evidence, independent of the canonical journal.
//!
//! A mandatory current format pointer names the checksummed append log. Updates append
//! one checksummed record to an established file instead of replacing a file
//! and dirtying its directory for every ACK. Every mutation reopens and syncs
//! the log, including idempotent retries: readable bytes after a failed sync
//! are not evidence of durability. No process-local cache can certify an ACK.
//!
//! Recovery uses the shared append-log crash contract: a successfully synced
//! record survives a process/power loss, while its unfinished successor may
//! leave an incomplete EOF frame. Complete invalid records and missing
//! initialized logs fail closed. This is not a certificate against arbitrary
//! subsequent deletion or truncation of already-synced storage bytes.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;

use fs2::FileExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_core::storage::durable_file::{self, DurableFileSync, DurableLengthEndian};

#[cfg(unix)]
mod validation_cache;

use crate::DfsMetadataShardId;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{metadata_durable_file_error, write_sobj_atomic};

const FORMAT: u32 = 1;
const MAX_RECORD_BYTES: usize = 16 * 1024;
const MAX_LOG_RECORDS: usize = 128;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq)]
enum FailurePoint {
    LogSync,
    DirectorySync,
}
#[cfg(test)]
thread_local! {
    static FAIL_NEXT: std::cell::Cell<Option<FailurePoint>> = const { std::cell::Cell::new(None) };
    static RECORD_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
fn fail_if_requested(point: FailurePoint) -> Result<()> {
    FAIL_NEXT.with(|failure| {
        if failure.get() == Some(point) {
            failure.set(None);
            Err(invalid("injected acknowledgement durability failure"))
        } else {
            Ok(())
        }
    })
}

pub(crate) trait AckFloor: Clone + Eq + Serialize + DeserializeOwned + 'static {
    const KIND: &'static str;
    fn shard(&self) -> &DfsMetadataShardId;
    fn validate(&self) -> Result<()>;
    fn merge(current: &Self, incoming: &Self) -> Result<Self>;
}

#[derive(Serialize, Deserialize)]
struct FormatPointer {
    ack_floor_log_format: u32,
    kind: String,
    shard_id: DfsMetadataShardId,
}

#[derive(Serialize, Deserialize)]
struct Record {
    format: u32,
    kind: String,
    shard_id: DfsMetadataShardId,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
    checksum: String,
}

#[derive(Clone)]
struct Loaded<T> {
    floor: Option<T>,
    initialized: bool,
    records: usize,
    complete_len: u64,
    file_len: u64,
}

fn invalid(message: impl Into<String>) -> MetadataBackendError {
    MetadataBackendError::Serde(message.into())
}

fn io_error(path: &Path, source: io::Error) -> MetadataBackendError {
    MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_ext::to_bytes(value, 1, LengthMode::U32).map_err(|error| invalid(error.to_string()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_ext::from_bytes(bytes).map_err(|error| invalid(error.to_string()))
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(path, error)),
    }
}

fn record_bytes<T: AckFloor>(floor: &T) -> Result<Vec<u8>> {
    let payload = encode(floor)?;
    let record = Record {
        format: FORMAT,
        kind: T::KIND.to_owned(),
        shard_id: floor.shard().clone(),
        checksum: blake3::hash(&payload).to_hex().to_string(),
        payload,
    };
    let bytes = encode(&record)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(invalid(
            "metadata acknowledgement record exceeds its size limit",
        ));
    }
    Ok(bytes)
}

fn parse_record<T: AckFloor>(bytes: &[u8], shard: &DfsMetadataShardId) -> Result<T> {
    #[cfg(test)]
    RECORD_DECODES.with(|count| count.set(count.get() + 1));
    let record: Record = decode(bytes)?;
    if record.format != FORMAT || record.kind != T::KIND || &record.shard_id != shard {
        return Err(invalid(
            "metadata acknowledgement log has a foreign format, kind or shard",
        ));
    }
    if blake3::hash(&record.payload).to_hex().as_str() != record.checksum {
        return Err(invalid("metadata acknowledgement log checksum mismatch"));
    }
    let floor: T = decode(&record.payload)?;
    if floor.shard() != shard {
        return Err(invalid(
            "metadata acknowledgement log payload names another shard",
        ));
    }
    floor.validate()?;
    Ok(floor)
}

// Floor writers already hold the metadata writer fence, which also excludes
// concurrent updates. This additional lock lets readers observe
// a coherent append/rotation without materializing metadata tables.
fn lock(path: &Path, exclusive: bool) -> Result<Option<File>> {
    let lock_path = path.with_extension("lock");
    let file = if exclusive {
        let parent = path
            .parent()
            .ok_or_else(|| invalid("acknowledgement path has no parent"))?;
        fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
    } else {
        OpenOptions::new().read(true).open(&lock_path)
    };
    let file = match file {
        Ok(file) => file,
        Err(error) if !exclusive && error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(&lock_path, error)),
    };
    if exclusive {
        FileExt::lock_exclusive(&file)
    } else {
        FileExt::lock_shared(&file)
    }
    .map_err(|error| io_error(&lock_path, error))?;
    Ok(Some(file))
}

#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.metadata.ack_floor.read",
    skip_all, fields(stage = "dfs.metadata.ack_floor.read", kind = T::KIND,
    records = tracing::field::Empty, decoded_records = tracing::field::Empty,
    reused_validation = false, log_bytes = tracing::field::Empty, outcome = "error"))]
fn load<T: AckFloor>(
    path: &Path,
    shard: &DfsMetadataShardId,
    _held_lock: Option<&File>,
) -> Result<Loaded<T>> {
    let mut initialized = false;
    let pointer_bytes = read_optional(path)?;
    if let Some(bytes) = pointer_bytes.as_deref() {
        let pointer: FormatPointer = decode(bytes)?;
        if pointer.ack_floor_log_format != FORMAT
            || pointer.kind != T::KIND
            || &pointer.shard_id != shard
        {
            return Err(invalid(
                "unsupported current metadata acknowledgement format pointer",
            ));
        }
        initialized = true;
    }
    let log_path = path.with_extension("slog");
    let log = match File::open(&log_path) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound && !initialized => None,
        Err(error) => return Err(io_error(&log_path, error)),
    };
    let mut loaded = Loaded {
        floor: None,
        initialized,
        records: 0,
        complete_len: 0,
        file_len: 0,
    };
    if let Some(mut log) = log {
        #[cfg(unix)]
        let before =
            validation_cache::source::<T>(path, shard, _held_lock, &log, pointer_bytes.as_deref())?;
        #[cfg(unix)]
        if let Some(source) = before.as_ref()
            && let Some(cached) = validation_cache::reuse::<T>(source, &mut log)?
        {
            let current_pointer = read_optional(path)?;
            let after = validation_cache::source::<T>(
                path,
                shard,
                _held_lock,
                &log,
                current_pointer.as_deref(),
            )?;
            if after.as_ref() != Some(source) {
                return Err(invalid(
                    "acknowledgement source changed during locked validation",
                ));
            }
            let span = tracing::Span::current();
            span.record("records", cached.records);
            span.record("decoded_records", 0usize);
            span.record("reused_validation", true);
            span.record("log_bytes", cached.file_len);
            span.record("outcome", "ok");
            return Ok(cached);
        }
        #[cfg(unix)]
        let identity_handle = log
            .try_clone()
            .map_err(|error| io_error(&log_path, error))?;
        #[cfg(unix)]
        let mut validated_bytes = blake3::Hasher::new();
        let mut records = durable_file::DurableRecordReader::new(
            log,
            &log_path,
            durable_file::DurableRecordReadOptions::new(DurableLengthEndian::Little)
                .max_record_bytes(MAX_RECORD_BYTES),
        )
        .map_err(metadata_durable_file_error)?;
        loaded.file_len = records.file_len();
        while let Some(record) = records.next_record().map_err(metadata_durable_file_error)? {
            let floor = parse_record::<T>(&record.bytes, shard)?;
            #[cfg(unix)]
            {
                validated_bytes.update(&(record.bytes.len() as u32).to_le_bytes());
                validated_bytes.update(&record.bytes);
            }
            if let Some(previous) = loaded.floor.as_ref() {
                // A complete log entry cannot regress or discard enrichment.
                if T::merge(previous, &floor)? != floor {
                    return Err(invalid(
                        "metadata acknowledgement log regresses durable evidence",
                    ));
                }
            }
            loaded.floor = Some(floor);
            loaded.complete_len = record.record_end;
            loaded.records += 1;
            if loaded.records > MAX_LOG_RECORDS {
                return Err(invalid(
                    "metadata acknowledgement log exceeds its bounded checkpoint interval",
                ));
            }
        }
        #[cfg(unix)]
        {
            let current_pointer = read_optional(path)?;
            let after = validation_cache::source::<T>(
                path,
                shard,
                _held_lock,
                &identity_handle,
                current_pointer.as_deref(),
            )?;
            validation_cache::remember::<T>(before, after, validated_bytes.finalize(), &loaded);
        }
    }
    if initialized && loaded.floor.is_none() {
        return Err(invalid(
            "initialized metadata acknowledgement log has no complete floor",
        ));
    }
    let span = tracing::Span::current();
    span.record("records", loaded.records);
    span.record("decoded_records", loaded.records);
    span.record("log_bytes", loaded.file_len);
    span.record("outcome", "ok");
    Ok(loaded)
}

pub(crate) fn read<T: AckFloor>(path: &Path, shard: &DfsMetadataShardId) -> Result<Option<T>> {
    let _lock = lock(path, false)?;
    load(path, shard, _lock.as_ref()).map(|loaded| loaded.floor)
}

/// Persist before ACK. Retried equal/older requests still sync the current
/// file; another handle or a previous failed sync may have left readable but
/// uncertified bytes. The clean directory sync also certifies a replacement
/// whose earlier rename succeeded but whose directory barrier failed.
#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.metadata.ack_floor.write",
    skip_all, fields(stage = "dfs.metadata.ack_floor.write", kind = T::KIND, outcome = "error"))]
pub(crate) fn put<T: AckFloor>(path: &Path, incoming: &T) -> Result<()> {
    incoming.validate()?;
    let _lock = lock(path, true)?;
    let loaded = load::<T>(path, incoming.shard(), _lock.as_ref())?;
    // Decoded DATA never certifies a write or a barrier. Drop it before any
    // append, rotation, truncation or equal-value resynchronization can fail.
    #[cfg(unix)]
    validation_cache::forget::<T>();
    let floor = match loaded.floor.as_ref() {
        Some(current) => T::merge(current, incoming)?,
        None => incoming.clone(),
    };
    let log_path = path.with_extension("slog");
    let parent = path
        .parent()
        .ok_or_else(|| invalid("acknowledgement path has no parent"))?;
    if !loaded.initialized || loaded.records >= MAX_LOG_RECORDS {
        let bytes = record_bytes(&floor)?;
        let mut frame = Vec::with_capacity(bytes.len() + 4);
        frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        frame.extend_from_slice(&bytes);
        durable_file::write_file_atomic(
            &log_path,
            &frame,
            durable_file::DurableAtomicFileOptions::new("floor.slog", "checkpoint").sync_all(),
        )
        .map_err(metadata_durable_file_error)?;
        if !loaded.initialized {
            // First publication must also persist every newly-created ancestor
            // down to the already-owned metadata root (coordination/kind/shard).
            if let Some(coordination) = parent.parent() {
                durable_file::sync_directory(coordination).map_err(metadata_durable_file_error)?;
                if let Some(root) = coordination.parent() {
                    durable_file::sync_directory(root).map_err(metadata_durable_file_error)?;
                }
            }
            write_sobj_atomic(
                path,
                &FormatPointer {
                    ack_floor_log_format: FORMAT,
                    kind: T::KIND.to_owned(),
                    shard_id: incoming.shard().clone(),
                },
            )?;
        }
    } else {
        // Never create a missing initialized log: losing it is lost ACK evidence.
        let mut log = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| io_error(&log_path, error))?;
        if loaded.complete_len != loaded.file_len {
            // No ACK can have named an incomplete EOF frame. The portable
            // helper also handles Windows append-handle truncation rights.
            durable_file::truncate_open_file_and_sync(
                &mut log,
                &log_path,
                loaded.complete_len,
                DurableFileSync::All,
            )
            .map_err(metadata_durable_file_error)?;
        }
        let record = if loaded.floor.as_ref() != Some(&floor) {
            Some(record_bytes(&floor)?)
        } else {
            None
        };
        durable_file::append_length_prefixed_records(
            &mut log,
            &log_path,
            record,
            durable_file::DurableRecordAppendOptions::new(DurableLengthEndian::Little),
        )
        .map_err(metadata_durable_file_error)?;
        #[cfg(test)]
        fail_if_requested(FailurePoint::LogSync)?;
        durable_file::append_length_prefixed_records(
            &mut log,
            &log_path,
            std::iter::empty::<&[u8]>(),
            durable_file::DurableRecordAppendOptions::new(DurableLengthEndian::Little)
                .sync(DurableFileSync::All),
        )
        .map_err(metadata_durable_file_error)?;
    }
    #[cfg(test)]
    fail_if_requested(FailurePoint::DirectorySync)?;
    durable_file::sync_directory(parent).map_err(metadata_durable_file_error)?;
    tracing::Span::current().record("outcome", "ok");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsMetadataAuthorityAckFloor, DfsMetadataPrefixAckFloor};
    use std::io::Write;

    fn floor(sequence: u64) -> DfsMetadataAuthorityAckFloor {
        DfsMetadataAuthorityAckFloor {
            shard_id: DfsMetadataShardId::new("floor-test"),
            journal_sequence: sequence,
            durable_prefix_proof: crate::DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: sequence,
                digest: format!("dfs-metadata-prefix-v2:blake3:{}", "a".repeat(64)),
            },
            authority_epoch: None,
        }
    }

    fn sidecar(root: &Path) -> std::path::PathBuf {
        root.join("coordination/metadata-authority-ack-floors/floor-test.sobj")
    }

    fn current(path: &Path) -> DfsMetadataAuthorityAckFloor {
        read(path, &floor(1).shard_id).unwrap().unwrap()
    }

    fn append_raw<T: AckFloor>(path: &Path, floor: &T) {
        let log_path = path.with_extension("slog");
        let mut log = OpenOptions::new().append(true).open(&log_path).unwrap();
        durable_file::append_length_prefixed_records(
            &mut log,
            &log_path,
            [record_bytes(floor).unwrap()],
            durable_file::DurableRecordAppendOptions::new(DurableLengthEndian::Little),
        )
        .unwrap();
    }

    #[test]
    fn updates_keep_the_format_fence_and_replay_without_an_index() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-append");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        let pointer = fs::read(&path).unwrap();
        assert!(
            decode::<DfsMetadataAuthorityAckFloor>(&pointer).is_err(),
            "legacy readers must fail closed"
        );
        let first_len = fs::metadata(path.with_extension("slog")).unwrap().len();
        put(&path, &floor(2)).unwrap();
        assert_eq!(fs::read(&path).unwrap(), pointer);
        assert!(fs::metadata(path.with_extension("slog")).unwrap().len() > first_len);
        put(&path, &floor(1)).unwrap();
        assert_eq!(current(&path), floor(2));
    }

    #[test]
    fn failed_sync_cannot_turn_readable_bytes_into_an_idempotent_ack() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-failed-sync");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        FAIL_NEXT.with(|point| point.set(Some(FailurePoint::LogSync)));
        assert!(put(&path, &floor(2)).is_err());
        assert_eq!(
            current(&path),
            floor(2),
            "a readable complete record may still be unsynced"
        );
        let len = fs::metadata(path.with_extension("slog")).unwrap().len();
        FAIL_NEXT.with(|point| point.set(Some(FailurePoint::LogSync)));
        assert!(
            put(&path, &floor(2)).is_err(),
            "retry must reach the sync barrier"
        );
        assert_eq!(
            fs::metadata(path.with_extension("slog")).unwrap().len(),
            len
        );
        put(&path, &floor(2)).unwrap();
        assert_eq!(current(&path), floor(2));
    }

    #[test]
    fn failed_directory_sync_must_be_retried_even_for_an_older_request() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-failed-directory");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        FAIL_NEXT.with(|point| point.set(Some(FailurePoint::DirectorySync)));
        assert!(put(&path, &floor(2)).is_err());
        FAIL_NEXT.with(|point| point.set(Some(FailurePoint::DirectorySync)));
        assert!(put(&path, &floor(1)).is_err());
        put(&path, &floor(1)).unwrap();
        assert_eq!(current(&path), floor(2));
    }

    #[test]
    fn an_incomplete_unacknowledged_tail_is_repaired_before_the_next_append() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-torn-tail");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        let log_path = path.with_extension("slog");
        let initial = fs::read(&log_path).unwrap();
        let bytes = record_bytes(&floor(2)).unwrap();
        let mut frame = (bytes.len() as u32).to_le_bytes().to_vec();
        frame.extend(bytes);
        for cut in [1, 3, 4, frame.len() / 2, frame.len() - 1] {
            let mut torn = initial.clone();
            torn.extend_from_slice(&frame[..cut]);
            fs::write(&log_path, torn).unwrap();
            assert_eq!(current(&path), floor(1));
            put(&path, &floor(3)).unwrap();
            assert_eq!(current(&path), floor(3));
        }
    }

    #[test]
    fn initialized_log_loss_and_complete_record_corruption_never_fall_back() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-corruption");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        let log_path = path.with_extension("slog");
        let initial = fs::read(&log_path).unwrap();
        fs::remove_file(&log_path).unwrap();
        assert!(read::<DfsMetadataAuthorityAckFloor>(&path, &floor(1).shard_id).is_err());
        assert!(put(&path, &floor(2)).is_err());
        fs::write(&log_path, &initial).unwrap();
        let mut corrupt: Record = decode(&record_bytes(&floor(2)).unwrap()).unwrap();
        corrupt.payload[0] ^= 1;
        let bytes = encode(&corrupt).unwrap();
        let mut log = OpenOptions::new().append(true).open(&log_path).unwrap();
        log.write_all(&(bytes.len() as u32).to_le_bytes()).unwrap();
        log.write_all(&bytes).unwrap();
        assert!(read::<DfsMetadataAuthorityAckFloor>(&path, &floor(1).shard_id).is_err());
        assert!(put(&path, &floor(3)).is_err());
        fs::write(&log_path, []).unwrap();
        assert!(
            put(&path, &floor(3)).is_err(),
            "empty initialized log is missing evidence"
        );
    }

    #[test]
    fn wrong_kind_shard_or_regressing_complete_record_is_rejected() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-foreign");
        let path = sidecar(&root);
        put(&path, &floor(2)).unwrap();
        let log_path = path.with_extension("slog");
        let initial = fs::read(&log_path).unwrap();
        append_raw(
            &path,
            &DfsMetadataPrefixAckFloor {
                shard_id: floor(1).shard_id,
                journal_sequence: 3,
                state_fingerprint: "exact-fingerprint".into(),
                durable_prefix_proof: floor(3).durable_prefix_proof,
                authority_epoch: None,
            },
        );
        assert!(put(&path, &floor(3)).is_err());
        fs::write(&log_path, &initial).unwrap();
        let mut foreign = floor(3);
        foreign.shard_id = DfsMetadataShardId::new("another-shard");
        append_raw(&path, &foreign);
        assert!(put(&path, &floor(3)).is_err());
        fs::write(&log_path, &initial).unwrap();
        append_raw(&path, &floor(1));
        assert!(put(&path, &floor(3)).is_err());
    }

    #[test]
    fn rotation_is_bounded_and_preserves_the_latest_floor_after_reopen() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-rotation");
        let path = sidecar(&root);
        for sequence in 1..=(MAX_LOG_RECORDS as u64 + 5) {
            put(&path, &floor(sequence)).unwrap();
        }
        assert_eq!(current(&path), floor(MAX_LOG_RECORDS as u64 + 5));
        assert_eq!(
            load::<DfsMetadataAuthorityAckFloor>(&path, &floor(1).shard_id, None)
                .unwrap()
                .records,
            5
        );
        FAIL_NEXT.with(|point| point.set(Some(FailurePoint::LogSync)));
        assert!(
            put(&path, &floor(1)).is_err(),
            "reopened evidence must be recertified before ACK"
        );
        put(&path, &floor(1)).unwrap();
    }

    #[test]
    fn independent_handles_cannot_overwrite_a_newer_floor() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-handles");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        std::thread::scope(|scope| {
            for sequence in 2..=12 {
                let path = &path;
                scope.spawn(move || put(path, &floor(sequence)).unwrap());
            }
        });
        assert_eq!(current(&path), floor(12));
    }

    #[test]
    fn an_uncertified_replacement_requires_the_directory_barrier_before_ack() {
        let root = sinomo_testdir::TestDir::new("ack-floor-log-uncertified-rotation");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        let log_path = path.with_extension("slog");
        let temporary = path.with_extension("rotation-test");
        let bytes = record_bytes(&floor(2)).unwrap();
        let mut replacement = File::create(&temporary).unwrap();
        replacement
            .write_all(&(bytes.len() as u32).to_le_bytes())
            .unwrap();
        replacement.write_all(&bytes).unwrap();
        replacement.sync_all().unwrap();
        drop(replacement);
        // Reproduce an interrupted rotation after replacement but before the
        // parent sync. Reopening a valid file cannot certify that namespace.
        fs::rename(&temporary, &log_path).unwrap();
        FAIL_NEXT.with(|point| point.set(Some(FailurePoint::DirectorySync)));
        assert!(put(&path, &floor(2)).is_err());
        put(&path, &floor(2)).unwrap();
        assert_eq!(current(&path), floor(2));
    }
    #[cfg(unix)]
    #[test]
    fn unchanged_authority_log_is_decoded_once_across_prefix_read_and_ack_write() {
        let root = sinomo_testdir::TestDir::new("ack-floor-same-source-validation");
        let authority = sidecar(&root);
        let prefix = root.join("coordination/metadata-prefix-ack-floors/floor-test.sobj");
        for sequence in 1..=76 {
            put(&authority, &floor(sequence)).unwrap();
        }
        for sequence in 1..=50 {
            put(
                &prefix,
                &DfsMetadataPrefixAckFloor {
                    shard_id: floor(sequence).shard_id,
                    journal_sequence: sequence,
                    state_fingerprint: String::new(),
                    durable_prefix_proof: floor(sequence).durable_prefix_proof,
                    authority_epoch: None,
                },
            )
            .unwrap();
        }
        RECORD_DECODES.with(|count| count.set(0));
        assert_eq!(
            read::<DfsMetadataAuthorityAckFloor>(&authority, &floor(76).shard_id).unwrap(),
            Some(floor(76))
        );
        assert_eq!(
            read::<DfsMetadataPrefixAckFloor>(&prefix, &floor(50).shard_id)
                .unwrap()
                .unwrap()
                .journal_sequence,
            50
        );
        let first = RECORD_DECODES.with(|count| count.get());
        assert_eq!(
            first, 126,
            "the actual initial logs are completely decoded and validated"
        );
        put(&authority, &floor(77)).unwrap();
        assert_eq!(
            RECORD_DECODES.with(|count| count.get()),
            first,
            "the already validated unchanged authority history is not decoded again to publish its next ACK"
        );
        assert_eq!(current(&authority), floor(77));
        // Decoded state cannot substitute for the next durability barrier.
        FAIL_NEXT.with(|point| point.set(Some(FailurePoint::LogSync)));
        assert!(put(&authority, &floor(77)).is_err());
        put(&authority, &floor(77)).unwrap();
        assert_eq!(current(&authority), floor(77));
    }

    #[test]
    fn remembered_validation_rejects_same_length_corruption_with_restored_mtime() {
        let root = sinomo_testdir::TestDir::new("ack-floor-validation-corruption");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        assert_eq!(current(&path), floor(1));
        let log_path = path.with_extension("slog");
        let original = fs::read(&log_path).unwrap();
        let modified = fs::metadata(&log_path).unwrap().modified().unwrap();
        let mut corrupt = original.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(&log_path, &corrupt).unwrap();
        File::options()
            .write(true)
            .open(&log_path)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        assert_eq!(
            fs::metadata(&log_path).unwrap().len(),
            original.len() as u64
        );
        assert_eq!(
            fs::metadata(&log_path).unwrap().modified().unwrap(),
            modified
        );
        assert!(read::<DfsMetadataAuthorityAckFloor>(&path, &floor(1).shard_id).is_err());
        assert!(put(&path, &floor(2)).is_err());
        fs::write(&log_path, original).unwrap();
        assert_eq!(current(&path), floor(1));
    }

    #[cfg(unix)]
    #[test]
    fn identical_bytes_under_replaced_physical_custody_are_decoded_again() {
        use std::os::unix::fs::MetadataExt;
        let root = sinomo_testdir::TestDir::new("ack-floor-validation-physical-generation");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        // Each replacement happens after the prior reader released its lock.
        // These are owned disposable files, with no concurrent live writer.
        for replaced in [
            path.with_extension("slog"),
            path.clone(),
            path.with_extension("lock"),
        ] {
            assert_eq!(current(&path), floor(1));
            let before_inode = fs::metadata(&replaced).unwrap().ino();
            let bytes = fs::read(&replaced).unwrap();
            let saved = replaced.with_extension(format!(
                "{}.preserved",
                replaced.extension().unwrap().to_str().unwrap()
            ));
            fs::rename(&replaced, &saved).unwrap();
            fs::write(&replaced, &bytes).unwrap();
            assert_ne!(fs::metadata(&replaced).unwrap().ino(), before_inode);
            RECORD_DECODES.with(|count| count.set(0));
            assert_eq!(current(&path), floor(1));
            assert_eq!(
                RECORD_DECODES.with(|count| count.get()),
                1,
                "identical bytes cannot substitute for the current physical source generation"
            );
            assert_eq!(fs::read(saved).unwrap(), bytes);
        }
    }

    #[test]
    fn remembered_full_log_rotates_at_the_existing_record_boundary() {
        let root = sinomo_testdir::TestDir::new("ack-floor-validation-rotation");
        let path = sidecar(&root);
        for sequence in 1..=MAX_LOG_RECORDS as u64 {
            put(&path, &floor(sequence)).unwrap();
        }
        assert_eq!(current(&path), floor(MAX_LOG_RECORDS as u64));
        put(&path, &floor(MAX_LOG_RECORDS as u64 + 1)).unwrap();
        assert_eq!(current(&path), floor(MAX_LOG_RECORDS as u64 + 1));
        let held = lock(&path, false).unwrap();
        let loaded =
            load::<DfsMetadataAuthorityAckFloor>(&path, &floor(1).shard_id, held.as_ref()).unwrap();
        assert_eq!(loaded.records, 1);
        assert_eq!(loaded.complete_len, loaded.file_len);
    }

    #[cfg(unix)]
    #[test]
    fn remembered_generation_alone_cannot_hide_changed_physical_bytes() {
        let root = sinomo_testdir::TestDir::new("ack-floor-validation-rehash");
        let path = sidecar(&root);
        put(&path, &floor(1)).unwrap();
        assert_eq!(current(&path), floor(1));
        let held = lock(&path, false).unwrap();
        let log_path = path.with_extension("slog");
        let mut opened = File::open(&log_path).unwrap();
        let pointer = read_optional(&path).unwrap();
        let before = validation_cache::source::<DfsMetadataAuthorityAckFloor>(
            &path,
            &floor(1).shard_id,
            held.as_ref(),
            &opened,
            pointer.as_deref(),
        )
        .unwrap()
        .unwrap();
        // A negative storage fault in this isolated, owned fixture deliberately
        // changes bytes after the captured generation. No other writer exists.
        let mut corrupt = fs::read(&log_path).unwrap();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(&log_path, corrupt).unwrap();
        assert!(
            validation_cache::reuse::<DfsMetadataAuthorityAckFloor>(&before, &mut opened)
                .unwrap()
                .is_none(),
            "the actual opened bytes must be compared even for an equal captured generation"
        );
        drop(held);
        assert!(read::<DfsMetadataAuthorityAckFloor>(&path, &floor(1).shard_id).is_err());
    }
}
