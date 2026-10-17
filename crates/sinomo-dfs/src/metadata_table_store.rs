//! Durable storage for materialized metadata tables.
//!
//! This module owns the current table log, snapshots, and repair archives.
//! The backend owns
//! metadata behavior; this layer only turns table batches and snapshots into a
//! durable `DfsMetadataTables` image.

use crate::metadata_io::encode_sobj;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Instant, UNIX_EPOCH};

use serde::Serializer;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use time::OffsetDateTime;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{
    create_dir_all, metadata_durable_file_error, read_dir, remove_file_if_exists, sync_dir,
    write_file_atomic, write_sobj_atomic,
};
use crate::metadata_journal::segmented_journal_entry_at_sequence;
use crate::metadata_keys::path_record_for_object;
use crate::metadata_records::{
    DfsDirFragmentRecord, DfsHardlinkIndexSnapshotRecord, DfsInodeRecord, DfsMetadataMutation,
    DfsMetadataPrefixProof, DfsMetadataTableBatch, DfsMetadataTableOp, DfsMetadataTableSnapshot,
    DfsMetadataTableSnapshotTables, DfsNamespaceTransaction, DfsObjectVersionRecord, DfsPathRecord,
    DfsReplicaReceiptRecord, DfsWriteIntentRecord, dfs_metadata_table_snapshot_state_fingerprint,
    normalized_object_metadata, normalized_version_record,
};
use crate::metadata_tables::{
    DfsMetadataTableState, DfsMetadataTables, MetadataTableReplay,
    MetadataTableSnapshotFingerprint, max_optional_sequence,
};
use crate::{DfsFileSystem, DfsObjectKind, DfsObjectMetadata};
use crate::{DfsObjectId, DfsVersion};

/// Current storage schema. Older snapshots are decoded only by the external
/// converter; the prepared-operation protocol remains independently versioned.
/// Generation 10 requires original alias comparisons in durable job frames.
/// Prior native declarations are admitted only by the external MIG converter.
pub const METADATA_TABLE_SNAPSHOT_FORMAT_VERSION: u16 = 10;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MetadataTableFileGeneration {
    is_file: bool,
    len: u64,
    modified_unix_nanos: Option<u128>,
    created_unix_nanos: Option<u128>,
    platform_identity: Option<String>,
    #[cfg(unix)]
    changed: (i64, i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MetadataTableStorageGeneration {
    snapshot: Option<MetadataTableFileGeneration>,
    checkpoint_log: Option<MetadataTableFileGeneration>,
    active_log: Option<MetadataTableFileGeneration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataTableLiveBorrowKind {
    Unchanged,
    CheckpointRotation,
}

impl MetadataTableFileGeneration {
    fn is_same_file_as(&self, other: &Self) -> bool {
        if self.is_file != other.is_file {
            return false;
        }
        match (&self.platform_identity, &other.platform_identity) {
            (Some(left), Some(right)) => left == right,
            _ => self == other,
        }
    }

    fn has_same_platform_identity_as(&self, other: &Self) -> bool {
        matches!(
            (&self.platform_identity, &other.platform_identity),
            (Some(left), Some(right)) if left == right
        )
    }
}

impl MetadataTableStorageGeneration {
    pub(crate) fn active_log_len(&self) -> Option<u64> {
        self.active_log.as_ref().map(|log| log.len)
    }

    pub(crate) fn snapshot_source_changed(&self, current: &Self) -> bool {
        self.snapshot != current.snapshot
    }

    pub(crate) fn is_compatible_with(&self, after: &Self) -> bool {
        self.snapshot == after.snapshot
            && self.checkpoint_log == after.checkpoint_log
            && match (&self.active_log, &after.active_log) {
                (Some(before), Some(after)) => before.is_same_file_as(after),
                (None, None) => true,
                _ => false,
            }
    }

    /// Return whether `current` still names the immutable snapshot/checkpoint
    /// generations and active append-only log prefix from which a live backend
    /// loaded its tables.
    ///
    /// The active log may have grown through writes performed by that same
    /// backend, so its file identity is stable while `table_log_offset` proves
    /// the exact materialized prefix represented by the live tables.
    pub(crate) fn live_table_borrow_kind(
        &self,
        current: &Self,
        table_log_offset: u64,
    ) -> Option<MetadataTableLiveBorrowKind> {
        if self.snapshot != current.snapshot {
            return None;
        }
        let unchanged_generation = self.checkpoint_log == current.checkpoint_log
            && match (&self.active_log, &current.active_log) {
                (Some(loaded), Some(current)) => {
                    loaded.is_same_file_as(current) && current.len == table_log_offset
                }
                (None, None) => table_log_offset == 0,
                _ => false,
            };
        if unchanged_generation {
            return Some(MetadataTableLiveBorrowKind::Unchanged);
        }

        // A checkpoint worker atomically renames the active log to the
        // checkpoint pathname, then publishes a new empty active log. The
        // loaded backend's exact in-memory tables remain valid across that
        // representation-only transition when the renamed file is precisely
        // the prefix it materialized and no newer active suffix exists.
        //
        // File identity is load-bearing: equal length alone cannot distinguish
        // a renamed generation from an unrelated replacement. Callers also
        // sample and compare the complete storage generation before and after
        // borrowing, so a concurrent suffix append or checkpoint replacement
        // takes the stable owned-snapshot fallback.
        let checkpoint_rotation = self.checkpoint_log.is_none()
            && match (
                &self.active_log,
                &current.checkpoint_log,
                &current.active_log,
            ) {
                (Some(loaded_active), Some(current_checkpoint), Some(current_active)) => {
                    table_log_offset > 0
                        && loaded_active.is_file
                        && current_checkpoint.is_file
                        && current_active.is_file
                        && loaded_active.has_same_platform_identity_as(current_checkpoint)
                        && current_checkpoint.len == table_log_offset
                        && current_active.len == 0
                }
                _ => false,
            };
        checkpoint_rotation.then_some(MetadataTableLiveBorrowKind::CheckpointRotation)
    }

    pub(crate) fn permits_live_table_borrow(&self, current: &Self, table_log_offset: u64) -> bool {
        self.live_table_borrow_kind(current, table_log_offset)
            .is_some()
    }
}

enum MetadataTableStateRead {
    Stable(DfsMetadataTableState),
    GenerationChanged,
}

#[cfg(test)]
thread_local! {
    static METADATA_TABLE_JOURNAL_REFERENCE_RESOLUTIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_TABLE_GENERATION_OPEN_HOOK:
        std::cell::RefCell<Option<MetadataTableGenerationOpenHook>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct MetadataTableGenerationOpenHook {
    path: PathBuf,
    action: Box<dyn FnOnce()>,
}

fn metadata_table_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "dfs metadata table store perf");
}

#[cfg(unix)]
fn metadata_table_file_platform_identity(_path: &Path, metadata: &fs::Metadata) -> Option<String> {
    use std::os::unix::fs::MetadataExt as _;

    Some(format!("unix:{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(windows)]
fn metadata_table_file_platform_identity(path: &Path, _metadata: &fs::Metadata) -> Option<String> {
    crate::metadata_io::windows_path_identity(path)
}

#[cfg(not(any(unix, windows)))]
fn metadata_table_file_platform_identity(_path: &Path, _metadata: &fs::Metadata) -> Option<String> {
    None
}

fn metadata_table_file_generation(path: &Path) -> Result<Option<MetadataTableFileGeneration>> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.to_owned(),
                source,
            });
        }
    };
    let modified_unix_nanos = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    let created_unix_nanos = metadata
        .created()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    let platform_identity = metadata_table_file_platform_identity(path, &metadata);
    Ok(Some(MetadataTableFileGeneration {
        is_file: metadata.is_file(),
        len: metadata.len(),
        modified_unix_nanos,
        created_unix_nanos,
        platform_identity,
        #[cfg(unix)]
        changed: {
            use std::os::unix::fs::MetadataExt;
            (metadata.ctime(), metadata.ctime_nsec())
        },
    }))
}

pub(crate) fn metadata_table_storage_generation(
    root: &Path,
) -> Result<MetadataTableStorageGeneration> {
    Ok(MetadataTableStorageGeneration {
        snapshot: metadata_table_file_generation(&metadata_table_snapshot_path(root))?,
        checkpoint_log: metadata_table_file_generation(&metadata_table_checkpoint_log_path(root))?,
        active_log: metadata_table_file_generation(&metadata_table_path(root))?,
    })
}

pub(crate) fn metadata_table_path(root: &Path) -> PathBuf {
    root.join("records")
        .join("tables")
        .join("materialized.slog")
}

pub(crate) fn metadata_table_snapshot_path(root: &Path) -> PathBuf {
    root.join("records").join("tables").join("snapshot.sobj")
}

pub(crate) fn metadata_table_checkpoint_log_path(root: &Path) -> PathBuf {
    root.join("records")
        .join("tables")
        .join("materialized.checkpoint.slog")
}

pub(crate) fn metadata_table_checkpoint_fingerprint(
    root: &Path,
) -> Result<Option<MetadataTableFileGeneration>> {
    metadata_table_file_generation(&metadata_table_checkpoint_log_path(root))
}

/// Probe the oldest surviving batch without scanning a potentially huge log.
/// Writers append current-format batches; format versions cannot regress within
/// a generation. Recovery has already validated/truncated the log at admission.

pub(crate) fn load_metadata_tables(root: &Path) -> Result<DfsMetadataTableState> {
    read_metadata_table_state(root)
}

pub(crate) fn read_metadata_table_state(root: &Path) -> Result<DfsMetadataTableState> {
    read_metadata_table_state_with_tail_policy(root, true)
}

pub(crate) fn read_metadata_table_state_lenient(root: &Path) -> Result<DfsMetadataTableState> {
    read_metadata_table_state_with_tail_policy(root, false)
}

/// Read only the last self-contained table snapshot for a non-primary replica
/// that could not replay its rebuildable table logs.
///
/// The caller must remain repair-only until an authenticated authoritative
/// snapshot replaces this state. In particular, this intentionally ignores
/// both the rotated checkpoint log and active `materialized.slog`, which may
/// contain journal references that cannot be resolved from a damaged or
/// missing local journal suffix.
pub(crate) fn read_metadata_table_snapshot_for_replica_repair(
    root: &Path,
) -> Result<DfsMetadataTableState> {
    read_metadata_table_snapshot(root, false)
}

fn read_metadata_table_state_with_tail_policy(
    root: &Path,
    truncate_torn_tail: bool,
) -> Result<DfsMetadataTableState> {
    const MAX_GENERATION_TRANSITION_ATTEMPTS: usize = 4;

    for attempt in 0..MAX_GENERATION_TRANSITION_ATTEMPTS {
        match read_metadata_table_state_once(root, truncate_torn_tail) {
            Ok(MetadataTableStateRead::Stable(state)) => return Ok(state),
            Ok(MetadataTableStateRead::GenerationChanged)
                if attempt + 1 < MAX_GENERATION_TRANSITION_ATTEMPTS =>
            {
                std::thread::yield_now();
            }
            Ok(MetadataTableStateRead::GenerationChanged) => {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata table generation at {} changed during {MAX_GENERATION_TRANSITION_ATTEMPTS} consecutive read attempts",
                    root.display()
                )));
            }
            Err(error)
                if attempt + 1 < MAX_GENERATION_TRANSITION_ATTEMPTS
                    && is_metadata_table_generation_not_found(root, &error) =>
            {
                // Installing a checkpoint snapshot removes the rotated log,
                // while rotating the active log briefly removes its pathname
                // before publishing the new empty generation. Restart from
                // the snapshot so an old snapshot is never combined with a
                // newer suffix after either transition.
                std::thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the bounded metadata table read loop always returns")
}

fn read_metadata_table_state_once(
    root: &Path,
    truncate_torn_tail: bool,
) -> Result<MetadataTableStateRead> {
    let generation_before = metadata_table_storage_generation(root)?;
    let mut state = read_metadata_table_snapshot(root, truncate_torn_tail)?;
    let checkpoint_path = metadata_table_checkpoint_log_path(root);
    if generation_before
        .checkpoint_log
        .as_ref()
        .is_some_and(|generation| generation.is_file)
    {
        #[cfg(test)]
        run_metadata_table_generation_open_hook(&checkpoint_path);
        let replay = replay_metadata_table_batches_after_checkpoint_from(
            root,
            &checkpoint_path,
            0,
            state.materialized_through_sequence,
            &mut state.tables,
        )?;
        state.materialized_through_sequence = max_optional_sequence(
            state.materialized_through_sequence,
            replay.materialized_through_sequence,
        );
        if replay.prefix_proof.is_some() {
            state.prefix_proof = replay.prefix_proof;
        }
        if truncate_torn_tail {
            truncate_torn_metadata_table_log_tail(&checkpoint_path, replay.offset)?;
        }
    }
    let table_path = metadata_table_path(root);
    if generation_before
        .active_log
        .as_ref()
        .is_some_and(|generation| generation.is_file)
    {
        #[cfg(test)]
        run_metadata_table_generation_open_hook(&table_path);
        let replay = replay_metadata_table_batches_after_checkpoint_from(
            root,
            &table_path,
            0,
            state.materialized_through_sequence,
            &mut state.tables,
        )?;
        state.table_log_offset = replay.offset;
        state.materialized_through_sequence = max_optional_sequence(
            state.materialized_through_sequence,
            replay.materialized_through_sequence,
        );
        if replay.prefix_proof.is_some() {
            state.prefix_proof = replay.prefix_proof;
        }
        if truncate_torn_tail {
            truncate_torn_metadata_table_log_tail(&table_path, state.table_log_offset)?;
        }
    }
    let generation_after = metadata_table_storage_generation(root)?;
    if !generation_before.is_compatible_with(&generation_after) {
        return Ok(MetadataTableStateRead::GenerationChanged);
    }
    Ok(MetadataTableStateRead::Stable(state))
}

fn is_metadata_table_generation_not_found(root: &Path, error: &MetadataBackendError) -> bool {
    let MetadataBackendError::Io { path, source } = error else {
        return false;
    };
    source.kind() == io::ErrorKind::NotFound
        && (path == &metadata_table_checkpoint_log_path(root) || path == &metadata_table_path(root))
}

#[cfg(test)]
fn install_metadata_table_generation_open_hook(path: PathBuf, action: impl FnOnce() + 'static) {
    METADATA_TABLE_GENERATION_OPEN_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(MetadataTableGenerationOpenHook {
            path,
            action: Box::new(action),
        });
    });
}

#[cfg(test)]
fn run_metadata_table_generation_open_hook(path: &Path) {
    let action = METADATA_TABLE_GENERATION_OPEN_HOOK.with(|hook| {
        let mut hook = hook.borrow_mut();
        if hook.as_ref().is_none_or(|hook| hook.path != path) {
            return None;
        }
        hook.take().map(|hook| hook.action)
    });
    if let Some(action) = action {
        action();
    }
}

/// Read the immutable snapshot plus the table-log generation rotated for one
/// background checkpoint, excluding the active log where newer writes land.
pub(crate) fn read_metadata_table_checkpoint_state(root: &Path) -> Result<DfsMetadataTableState> {
    let mut state = read_metadata_table_snapshot(root, false)?;
    let checkpoint_path = metadata_table_checkpoint_log_path(root);
    if !checkpoint_path.is_file() {
        return Ok(state);
    }
    let replay = replay_metadata_table_batches_after_checkpoint_from(
        root,
        &checkpoint_path,
        0,
        state.materialized_through_sequence,
        &mut state.tables,
    )?;
    state.materialized_through_sequence = max_optional_sequence(
        state.materialized_through_sequence,
        replay.materialized_through_sequence,
    );
    if replay.prefix_proof.is_some() {
        state.prefix_proof = replay.prefix_proof;
    }
    state.table_log_offset = 0;
    Ok(state)
}

/// Cut a crash-torn table-log suffix after replay stops at the last complete
/// record. Live readers keep the lenient replay behavior and never truncate.
fn truncate_torn_metadata_table_log_tail(path: &Path, offset: u64) -> Result<()> {
    let len = fs::metadata(path)
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    if offset >= len {
        return Ok(());
    }
    eprintln!(
        "sinomo dfs: truncating torn metadata table log tail at {} (keeping {} of {} bytes)",
        path.display(),
        offset,
        len
    );
    durable_file::truncate_file_and_sync(path, offset, durable_file::DurableFileSync::All)
        .map_err(metadata_durable_file_error)?;
    Ok(())
}

fn read_metadata_table_snapshot(
    root: &Path,
    _persist_verification: bool,
) -> Result<DfsMetadataTableState> {
    let path = metadata_table_snapshot_path(root);
    let mut file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(DfsMetadataTableState::default());
        }
        Err(source) => return Err(MetadataBackendError::Io { path, source }),
    };
    if !sinomo_core::storage::migration::stream::is_stream_file(&mut file).map_err(|source| {
        MetadataBackendError::Io {
            path: path.clone(),
            source,
        }
    })? {
        return Err(MetadataBackendError::Serde(
            "unsupported metadata snapshot storage format".into(),
        ));
    }
    crate::metadata_snapshot::read_snapshot(root, file)
}

pub(crate) fn metadata_table_snapshot_fingerprint(
    root: &Path,
) -> Result<Option<MetadataTableSnapshotFingerprint>> {
    let path = metadata_table_snapshot_path(root);
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(MetadataBackendError::Io { path, source }),
    };
    let modified_unix_nanos = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    Ok(Some(MetadataTableSnapshotFingerprint {
        len: metadata.len(),
        modified_unix_nanos,
    }))
}

pub(crate) fn metadata_tables_from_table_snapshot(
    snapshot: DfsMetadataTableSnapshot,
    description: &str,
) -> Result<DfsMetadataTables> {
    if snapshot.format_version != METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
        || !snapshot.prefix_proof.is_well_formed()
        || snapshot.prefix_proof.journal_sequence != snapshot.materialized_through_sequence
    {
        return Err(MetadataBackendError::Serde(format!(
            "unsupported or invalid {description}"
        )));
    }
    let checksum = dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
        .map_err(|e| MetadataBackendError::Serde(e.to_string()))?;
    if checksum != snapshot.semantic_state_fingerprint {
        return Err(MetadataBackendError::replica_diverged(format!(
            "{description} semantic checksum mismatch"
        )));
    }
    metadata_tables_from_snapshot(*snapshot.tables)
}

pub(crate) fn replay_metadata_table_batches_from(
    root: &Path,
    path: &Path,
    offset: u64,
    tables: &mut DfsMetadataTables,
) -> Result<MetadataTableReplay> {
    replay_metadata_table_batches_after_checkpoint_from(root, path, offset, None, tables)
}

/// Read only the active table generation's compact replay-pressure metadata.
///
/// A background checkpoint rotates the active log without mutating live
/// backend counters. The next writer uses this bounded scan to rebase those
/// counters onto the new generation without resolving authoritative journal
/// references or rebuilding the materialized tables a second time.
pub(crate) fn current_metadata_table_log_replay(root: &Path) -> Result<MetadataTableReplay> {
    let path = metadata_table_path(root);
    let mut file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(MetadataTableReplay::default());
        }
        Err(source) => return Err(MetadataBackendError::Io { path, source }),
    };
    replay_metadata_table_batches(
        &mut file,
        root,
        &path,
        Some(u64::MAX),
        &mut DfsMetadataTables::default(),
    )
}

fn replay_metadata_table_batches_after_checkpoint_from(
    root: &Path,
    path: &Path,
    offset: u64,
    checkpoint_sequence: Option<u64>,
    tables: &mut DfsMetadataTables,
) -> Result<MetadataTableReplay> {
    let mut file = fs::File::open(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    let len = file
        .metadata()
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    if offset > len {
        return Err(MetadataBackendError::Serde(format!(
            "DFS metadata table cursor {} is beyond table log length {} at {}",
            offset,
            len,
            path.display()
        )));
    }
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?;
    replay_metadata_table_batches(&mut file, root, path, checkpoint_sequence, tables)
}

fn decode_current_metadata_table_batch(bytes: &[u8], path: &Path) -> Result<DfsMetadataTableBatch> {
    if bytes.len() < 6
        || &bytes[..4] != sinomo_core::object::MAGIC
        || u16::from_le_bytes([bytes[4], bytes[5]]) != METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
    {
        return Err(MetadataBackendError::Serde(format!(
            "unsupported current metadata table-log format at {}",
            path.display()
        )));
    }
    let batch: DfsMetadataTableBatch = sinomo_object_serde::from_bytes(bytes).map_err(|error| {
        MetadataBackendError::replica_diverged(format!(
            "invalid complete table batch at {}: {error}",
            path.display()
        ))
    })?;
    let proof = batch.prefix_proof.as_ref().ok_or_else(|| {
        MetadataBackendError::replica_diverged("table batch has no exact prefix".into())
    })?;
    if !proof.is_well_formed() || proof.journal_sequence != batch.sequence {
        return Err(MetadataBackendError::replica_diverged(
            "table batch prefix/sequence is inconsistent".into(),
        ));
    }
    Ok(batch)
}

/// Read at most the first complete record in each current table-log role.
/// This is layout DATA, not a hydrated head or author admission. Ordinary
/// replay uses the same decoder for every batch and validates the full state.
#[cfg(feature = "storage-conversion")]
pub fn read_current_metadata_table_log_prefix(
    root: &Path,
) -> Result<Option<DfsMetadataPrefixProof>> {
    let before = metadata_table_storage_generation(root)?;
    let mut prefix = None;
    for path in [
        metadata_table_checkpoint_log_path(root),
        metadata_table_path(root),
    ] {
        let mut file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
            Err(source) => return Err(MetadataBackendError::Io { path, source }),
        };
        if let Some(record) = durable_file::read_length_prefixed_record(
            &mut file,
            &path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
        )
        .map_err(metadata_durable_file_error)?
        {
            prefix = decode_current_metadata_table_batch(&record.bytes, &path)?.prefix_proof;
        }
    }
    require_stable_current_metadata_layout(root, &before)?;
    Ok(prefix)
}

#[cfg(feature = "storage-conversion")]
fn require_stable_current_metadata_layout(
    root: &Path,
    before: &MetadataTableStorageGeneration,
) -> Result<()> {
    if !before.is_compatible_with(&metadata_table_storage_generation(root)?) {
        // A live producer replaced the observed layout. No prefix or Ready is
        // established by this attempt; the normal bounded lifecycle reselects
        // the current generation. Stable malformed records remain decoder errors.
        return Err(MetadataBackendError::StoragePreparationPending {
            root: root.to_owned(),
            processed_records: 0,
        });
    }
    Ok(())
}

#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.metadata.table_replay",
    skip_all, fields(stage = "dfs.metadata.table_replay", root = %root.display(), path = %path.display(),
        starting_offset = tracing::field::Empty, ending_offset = tracing::field::Empty,
        checkpoint_sequence = tracing::field::Empty, materialized_through_sequence = tracing::field::Empty,
        decoded_batches = tracing::field::Empty, applied_batches = tracing::field::Empty,
        outcome = "error"))]
fn replay_metadata_table_batches(
    file: &mut fs::File,
    root: &Path,
    path: &Path,
    checkpoint_sequence: Option<u64>,
    tables: &mut DfsMetadataTables,
) -> Result<MetadataTableReplay> {
    crate::metadata_records::with_prepared_payload_validation(|| {
        let mut offset = file
            .stream_position()
            .map_err(|source| MetadataBackendError::Io {
                path: path.to_owned(),
                source,
            })?;
        let mut through = None;
        let mut prefix = None;
        let span = tracing::Span::current();
        span.record("starting_offset", offset);
        if let Some(sequence) = checkpoint_sequence {
            span.record("checkpoint_sequence", sequence);
        }
        let mut decoded_batches = 0usize;
        let mut applied_batches = 0usize;
        while let Some(record) = durable_file::read_length_prefixed_record(
            file,
            path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
        )
        .map_err(metadata_durable_file_error)?
        {
            let batch = decode_current_metadata_table_batch(&record.bytes, path)?;
            decoded_batches += 1;
            let proof = batch
                .prefix_proof
                .as_ref()
                .expect("validated current prefix");
            if through.is_some_and(|old| batch.sequence <= old) {
                return Err(MetadataBackendError::replica_diverged(
                    "table batch prefix/sequence is inconsistent".into(),
                ));
            }
            through = Some(batch.sequence);
            prefix = Some(proof.clone());
            if checkpoint_sequence.is_none_or(|checkpoint| batch.sequence > checkpoint) {
                for op in batch.ops {
                    tables.apply_checked(op)?;
                }
                applied_batches += 1;
            }
            offset = record.record_end;
        }
        span.record("ending_offset", offset);
        span.record("decoded_batches", decoded_batches);
        span.record("applied_batches", applied_batches);
        if let Some(sequence) = through {
            span.record("materialized_through_sequence", sequence);
        }
        span.record("outcome", "ok");
        Ok(MetadataTableReplay {
            offset,
            materialized_through_sequence: through,
            prefix_proof: prefix,
        })
    })
}

pub(crate) fn append_metadata_table_batch(
    root: &Path,
    batch: &DfsMetadataTableBatch,
) -> Result<u64> {
    let proof = batch.prefix_proof.as_ref().ok_or_else(|| {
        MetadataBackendError::Serde("current table batch lacks its exact prefix".into())
    })?;
    if !proof.is_well_formed() || proof.journal_sequence != batch.sequence {
        return Err(MetadataBackendError::Serde(
            "current table batch prefix/cut mismatch".into(),
        ));
    }

    let trace_started = Instant::now();
    let path = metadata_table_path(root);
    let create_dir_started = Instant::now();
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let create_dir_ms = create_dir_started.elapsed().as_millis();
    let encode_started = Instant::now();
    let bytes = sinomo_object_serde::to_bytes(
        batch,
        crate::metadata_snapshot::SNAPSHOT_FORMAT,
        LengthMode::U32,
    )
    .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    u32::try_from(bytes.len()).map_err(|_| {
        MetadataBackendError::Serde(format!(
            "DFS metadata table batch {} is too large: {} bytes",
            batch.sequence,
            bytes.len()
        ))
    })?;
    let encode_ms = encode_started.elapsed().as_millis();
    let write_started = Instant::now();
    let mut append = durable_file::open_file_for_append_with_options(
        &path,
        durable_file::DurableAppendFileOptions::new().without_parent_sync(),
    )
    .map_err(metadata_durable_file_error)?;
    // The shard journal is synchronized before this function is called and is
    // the replay/replication source of truth. `materialized.slog` is only a
    // rebuildable read cache: reopen truncates a torn tail, rewinds a cursor
    // that is ahead of the surviving table head, and replays the durable
    // journal. Synchronizing this second copy would add one storage barrier to
    // every metadata mutation without making the acknowledged state safer.
    let record_bytes = durable_file::append_length_prefixed_records(
        &mut append.file,
        &path,
        [bytes.as_slice()],
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Little),
    )
    .map_err(metadata_durable_file_error)?;
    let write_ms = write_started.elapsed().as_millis();
    metadata_table_perf_trace(format_args!(
        "metadata-table-batch sequence={} ops={} bytes={} create_dir_ms={} encode_ms={} write_ms={} durability=journal-replay total_ms={}",
        batch.sequence,
        batch.ops.len(),
        record_bytes,
        create_dir_ms,
        encode_ms,
        write_ms,
        trace_started.elapsed().as_millis()
    ));
    Ok(record_bytes)
}

/// Rotate the active materialized log into an immutable checkpoint generation.
///
/// The metadata writer lock must be held by the caller. Reopen always replays
/// the checkpoint generation before the active log, so a crash after the
/// rename and before a worker finishes loses no table state.
#[cfg(test)]
pub(crate) fn rotate_metadata_table_log_for_checkpoint(
    root: &Path,
    max_log_bytes: u64,
) -> Result<bool> {
    rotate_metadata_table_log_for_checkpoint_at_replay_boundary(root, max_log_bytes, None)
}

/// Rotate a compact-reference generation once the scheduler's reconstructed
/// replay-pressure boundary is still present.
///
/// `minimum_replay_pressure_offset` is captured after a complete batch append.
/// If another process already replaced/truncated that generation before this
/// worker acquires the writer lock, the worker leaves the new suffix alone.
pub(crate) fn rotate_metadata_table_log_for_checkpoint_at_replay_boundary(
    root: &Path,
    max_log_bytes: u64,
    minimum_replay_pressure_offset: Option<u64>,
) -> Result<bool> {
    let checkpoint_path = metadata_table_checkpoint_log_path(root);
    if checkpoint_path.is_file() {
        return Ok(true);
    }
    let active_path = metadata_table_path(root);
    let len = match fs::metadata(&active_path) {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: active_path.clone(),
                source,
            });
        }
    };
    let replay_pressure_boundary_present = minimum_replay_pressure_offset
        .is_some_and(|minimum_offset| minimum_offset > 0 && len >= minimum_offset);
    if len <= max_log_bytes && !replay_pressure_boundary_present {
        return Ok(false);
    }
    if let Some(parent) = active_path.parent() {
        create_dir_all(parent.to_path_buf())?;
    }
    fs::rename(&active_path, &checkpoint_path).map_err(|source| MetadataBackendError::Io {
        path: active_path.clone(),
        source,
    })?;
    if let Some(parent) = active_path.parent() {
        sync_dir(parent)?;
    }
    // New writes may resume immediately in a distinct generation while the
    // worker decodes and serializes the immutable checkpoint log.
    write_file_atomic(&active_path, &[])?;
    Ok(true)
}

pub(crate) fn remove_metadata_table_checkpoint_log(root: &Path) -> Result<()> {
    remove_file_if_exists(&metadata_table_checkpoint_log_path(root))
}

struct BorrowedMapValues<'a, K, V>(&'a BTreeMap<K, V>);

impl<K, V> Serialize for BorrowedMapValues<'_, K, V>
where
    K: Ord,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0.values() {
            sequence.serialize_element(value)?;
        }
        sequence.end()
    }
}

struct BorrowedHardlinkValues<'a>(&'a DfsMetadataTables);

#[derive(Serialize)]
struct BorrowedHardlinkSnapshotRecord<'a> {
    target_object_id: &'a DfsObjectId,
    hardlink_object_id: &'a DfsObjectId,
    path_record: &'a DfsPathRecord,
}

impl Serialize for BorrowedHardlinkValues<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.hardlinks.len()))?;
        for ((target_object_id, hardlink_object_id), path_record) in &self.0.hardlinks {
            sequence.serialize_element(&BorrowedHardlinkSnapshotRecord {
                target_object_id,
                hardlink_object_id,
                path_record,
            })?;
        }
        sequence.end()
    }
}

struct BorrowedMetadataTableSnapshotTables<'a>(&'a DfsMetadataTables);

impl Serialize for BorrowedMetadataTableSnapshotTables<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let tables = self.0;
        let field_count = 2 + [
            !tables.file_systems.is_empty(),
            !tables.objects.is_empty(),
            !tables.file_system_objects.is_empty(),
            !tables.paths.is_empty(),
            !tables.inodes.is_empty(),
            !tables.dirfrags.is_empty(),
            !tables.dentries.is_empty(),
            !tables.hardlinks.is_empty(),
            !tables.compacted_tombstones.is_empty(),
            !tables.versions.is_empty(),
            !tables.version_pins.is_empty(),
            !tables.cross_shard_pin_transfers.is_empty(),
            !tables.receipts.is_empty(),
            !tables.intents.is_empty(),
            !tables.intent_receipts.is_empty(),
            !tables.intent_receipt_retention.is_empty(),
            !tables.advisory_lock_states.is_empty(),
            !tables.namespace_transaction_receipts.is_empty(),
            !tables.prepared_operations_v7.is_empty(),
            !tables.namespace_jobs.is_empty(),
            !tables.namespace_job_receipts.is_empty(),
            !tables.namespace_request_receipts.is_empty(),
            !tables.namespace_receipt_retention.is_empty(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        let mut state =
            serializer.serialize_struct("DfsMetadataTableSnapshotTables", field_count)?;
        state.serialize_field(
            "prepared_operation_evictions",
            &BorrowedMapValues(&tables.prepared_operation_evictions),
        )?;
        if !tables.file_systems.is_empty() {
            state.serialize_field("file_systems", &BorrowedMapValues(&tables.file_systems))?;
        }
        if !tables.objects.is_empty() {
            state.serialize_field("objects", &BorrowedMapValues(&tables.objects))?;
        }
        if !tables.file_system_objects.is_empty() {
            state.serialize_field(
                "file_system_objects",
                &BorrowedMapValues(&tables.file_system_objects),
            )?;
        }
        if !tables.paths.is_empty() {
            state.serialize_field("paths", &BorrowedMapValues(&tables.paths))?;
        }
        if !tables.inodes.is_empty() {
            state.serialize_field("inodes", &BorrowedMapValues(&tables.inodes))?;
        }
        if !tables.dirfrags.is_empty() {
            state.serialize_field("dirfrags", &BorrowedMapValues(&tables.dirfrags))?;
        }
        if !tables.dentries.is_empty() {
            state.serialize_field("dentries", &BorrowedMapValues(&tables.dentries))?;
        }
        if !tables.hardlinks.is_empty() {
            state.serialize_field("hardlinks", &BorrowedHardlinkValues(tables))?;
        }
        if !tables.compacted_tombstones.is_empty() {
            state.serialize_field(
                "compacted_tombstones",
                &BorrowedMapValues(&tables.compacted_tombstones),
            )?;
        }
        if !tables.versions.is_empty() {
            state.serialize_field("versions", &BorrowedMapValues(&tables.versions))?;
        }
        if !tables.version_pins.is_empty() {
            state.serialize_field("version_pins", &BorrowedMapValues(&tables.version_pins))?;
        }
        if !tables.cross_shard_pin_transfers.is_empty() {
            state.serialize_field(
                "cross_shard_pin_transfers",
                &BorrowedMapValues(&tables.cross_shard_pin_transfers),
            )?;
        }
        if !tables.receipts.is_empty() {
            state.serialize_field("receipts", &BorrowedMapValues(&tables.receipts))?;
        }
        if !tables.intents.is_empty() {
            state.serialize_field("intents", &BorrowedMapValues(&tables.intents))?;
        }
        if !tables.intent_receipts.is_empty() {
            state.serialize_field(
                "intent_receipts",
                &BorrowedMapValues(&tables.intent_receipts),
            )?;
        }
        if !tables.intent_receipt_retention.is_empty() {
            state.serialize_field("intent_receipt_retention", &tables.intent_receipt_retention)?;
        }

        if !tables.advisory_lock_states.is_empty() {
            state.serialize_field(
                "advisory_lock_states",
                &BorrowedMapValues(&tables.advisory_lock_states),
            )?;
        }

        if !tables.namespace_transaction_receipts.is_empty() {
            state.serialize_field(
                "namespace_transaction_receipts",
                &BorrowedMapValues(&tables.namespace_transaction_receipts),
            )?;
        }

        if !tables.prepared_operations_v7.is_empty() {
            state.serialize_field(
                "prepared_operations_v7",
                &BorrowedMapValues(&tables.prepared_operations_v7),
            )?;
        }
        if !tables.namespace_jobs.is_empty() {
            state.serialize_field("namespace_jobs", &BorrowedMapValues(&tables.namespace_jobs))?;
        }
        if !tables.namespace_job_receipts.is_empty() {
            state.serialize_field(
                "namespace_job_receipts",
                &BorrowedMapValues(&tables.namespace_job_receipts),
            )?;
        }
        if !tables.namespace_request_receipts.is_empty() {
            state.serialize_field(
                "namespace_request_receipts",
                &BorrowedMapValues(&tables.namespace_request_receipts),
            )?;
        }
        if !tables.namespace_receipt_retention.is_empty() {
            state.serialize_field(
                "namespace_receipt_retention",
                &tables.namespace_receipt_retention,
            )?;
        }
        state.serialize_field(
            "namespace_requests",
            &BorrowedMapValues(&tables.namespace_requests),
        )?;
        state.end()
    }
}

#[derive(Serialize)]
struct BorrowedSemanticSnapshot<'a> {
    format_version: u16,
    materialized_through_sequence: u64,
    tables: BorrowedMetadataTableSnapshotTables<'a>,
}

struct Blake3Writer {
    hasher: blake3::Hasher,
}

impl Blake3Writer {
    fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    fn finalize(self) -> blake3::Hash {
        self.hasher.finalize()
    }
}

impl Write for Blake3Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.hasher.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn streamed_json_blake3(value: &impl Serialize) -> Result<blake3::Hash> {
    let mut writer = Blake3Writer::new();
    serde_json::to_writer(&mut writer, value)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    Ok(writer.finalize())
}

pub(crate) fn persist_metadata_table_snapshot(
    root: &Path,
    tables: &DfsMetadataTables,
    materialized_through_sequence: Option<u64>,
    prefix_proof: Option<&DfsMetadataPrefixProof>,
) -> Result<()> {
    if let Some(proof) = prefix_proof
        && (!proof.is_well_formed()
            || Some(proof.journal_sequence) != materialized_through_sequence)
    {
        return Err(MetadataBackendError::Serde(format!(
            "cannot persist metadata snapshot sequence {materialized_through_sequence:?} with prefix proof sequence {}",
            proof.journal_sequence
        )));
    }
    crate::metadata_snapshot::persist_snapshot(
        root,
        tables,
        materialized_through_sequence,
        prefix_proof,
    )?;
    truncate_metadata_table_log(root)?;
    remove_metadata_table_checkpoint_log(root)?;
    Ok(())
}

pub(crate) fn snapshot_semantic_fingerprint(
    tables: &DfsMetadataTables,
    sequence: Option<u64>,
) -> Result<String> {
    let snapshot = BorrowedSemanticSnapshot {
        format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        materialized_through_sequence: sequence.ok_or_else(|| {
            MetadataBackendError::Serde("snapshot has no materialization sequence".into())
        })?,
        tables: BorrowedMetadataTableSnapshotTables(tables),
    };
    Ok(format!(
        "blake3:{}",
        streamed_json_blake3(&snapshot)?.to_hex()
    ))
}

pub(crate) fn metadata_tables_snapshot(
    tables: &DfsMetadataTables,
) -> DfsMetadataTableSnapshotTables {
    DfsMetadataTableSnapshotTables {
        prepared_operation_evictions: tables
            .prepared_operation_evictions
            .values()
            .cloned()
            .collect(),
        file_systems: tables.file_systems.values().cloned().collect(),
        objects: tables.objects.values().cloned().collect(),
        file_system_objects: tables.file_system_objects.values().cloned().collect(),
        paths: tables.paths.values().cloned().collect(),
        inodes: tables.inodes.values().cloned().collect(),
        dirfrags: tables.dirfrags.values().cloned().collect(),
        dentries: tables.dentries.values().cloned().collect(),
        hardlinks: tables
            .hardlinks
            .iter()
            .map(|((target_object_id, hardlink_object_id), path_record)| {
                DfsHardlinkIndexSnapshotRecord {
                    target_object_id: target_object_id.clone(),
                    hardlink_object_id: hardlink_object_id.clone(),
                    path_record: path_record.clone(),
                }
            })
            .collect(),
        compacted_tombstones: tables.compacted_tombstones.values().cloned().collect(),
        versions: tables.versions.values().cloned().collect(),
        version_pins: tables.version_pins.values().cloned().collect(),
        cross_shard_pin_transfers: tables.cross_shard_pin_transfers.values().cloned().collect(),
        receipts: tables.receipts.values().cloned().collect(),
        intents: tables.intents.values().cloned().collect(),
        intent_receipts: tables.intent_receipts.values().cloned().collect(),
        intent_receipt_retention: tables.intent_receipt_retention.clone(),
        advisory_lock_states: tables.advisory_lock_states.values().cloned().collect(),
        namespace_transaction_receipts: tables
            .namespace_transaction_receipts
            .values()
            .cloned()
            .collect(),
        prepared_operations_v7: tables.prepared_operations_v7.values().cloned().collect(),
        namespace_jobs: tables.namespace_jobs.values().cloned().collect(),
        namespace_job_receipts: tables.namespace_job_receipts.values().cloned().collect(),
        namespace_request_receipts: tables
            .namespace_request_receipts
            .values()
            .cloned()
            .collect(),
        namespace_receipt_retention: tables.namespace_receipt_retention.clone(),
        namespace_requests: tables.namespace_requests.values().cloned().collect(),
    }
}

pub(crate) fn metadata_tables_from_snapshot(
    snapshot: DfsMetadataTableSnapshotTables,
) -> Result<DfsMetadataTables> {
    let mut tables = DfsMetadataTables::default();
    for record in snapshot.prepared_operation_evictions {
        record.validate()?;
        if tables
            .prepared_operation_evictions
            .insert(record.digest.operation_id.clone(), record)
            .is_some()
        {
            return Err(MetadataBackendError::Serde(
                "snapshot repeats an evicted operation identity".into(),
            ));
        }
    }
    tables.namespace_receipt_retention = snapshot.namespace_receipt_retention;
    tables.intent_receipt_retention = snapshot.intent_receipt_retention;
    for file_system in snapshot.file_systems {
        tables.apply_checked(DfsMetadataTableOp::PutFileSystem { file_system })?;
    }
    for object in snapshot.objects {
        tables.apply_checked(DfsMetadataTableOp::PutObject { object })?;
    }
    for path_record in snapshot.file_system_objects {
        tables.apply_checked(DfsMetadataTableOp::PutFileSystemObject { path_record })?;
    }
    for path_record in snapshot.paths {
        tables.apply_checked(DfsMetadataTableOp::PutPath { path_record })?;
    }
    for inode in snapshot.inodes {
        tables.apply_checked(DfsMetadataTableOp::PutInode { inode })?;
    }
    for dirfrag in snapshot.dirfrags {
        tables.apply_checked(DfsMetadataTableOp::PutDirFragment { dirfrag })?;
    }
    for path_record in snapshot.dentries {
        tables.apply_checked(DfsMetadataTableOp::PutDentry { path_record })?;
    }
    for hardlink in snapshot.hardlinks {
        tables.apply_checked(DfsMetadataTableOp::PutHardlink {
            target_object_id: hardlink.target_object_id,
            hardlink_object_id: hardlink.hardlink_object_id,
            path_record: hardlink.path_record,
        })?;
    }
    for object in snapshot.compacted_tombstones {
        tables.apply_checked(DfsMetadataTableOp::PutCompactedTombstone { object })?;
    }
    for version in snapshot.versions {
        tables.apply_checked(DfsMetadataTableOp::PutVersion { version })?;
    }
    for pin in snapshot.version_pins {
        tables.apply_checked(DfsMetadataTableOp::PutVersionPin { pin })?;
    }
    for transfer in snapshot.cross_shard_pin_transfers {
        tables.cross_shard_pin_transfers.insert(
            (transfer.object_id.clone(), transfer.transfer_id.clone()),
            transfer,
        );
    }
    for receipt in snapshot.receipts {
        tables.apply_checked(DfsMetadataTableOp::PutReceipt { receipt })?;
    }
    for intent in snapshot.intents {
        tables.apply_checked(DfsMetadataTableOp::PutIntent { intent })?;
    }
    for receipt in snapshot.intent_receipts {
        tables.apply_checked(DfsMetadataTableOp::PutIntentReceipt { receipt })?;
    }
    for state in snapshot.advisory_lock_states {
        tables.apply_checked(DfsMetadataTableOp::PutAdvisoryLockState { state })?;
    }

    for receipt in snapshot.namespace_transaction_receipts {
        tables.apply_checked(DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt })?;
    }
    for record in snapshot.prepared_operations_v7 {
        record.validate()?;
        if tables
            .prepared_operations_v7
            .insert(record.digest.operation_id.clone(), record)
            .is_some()
        {
            return Err(MetadataBackendError::Serde(
                "snapshot repeats a prepared operation identity".into(),
            ));
        }
    }
    for job in snapshot.namespace_jobs {
        tables.apply_checked(DfsMetadataTableOp::PutNamespaceJob { job })?;
    }
    for receipt in snapshot.namespace_job_receipts {
        tables.apply_checked(DfsMetadataTableOp::PutNamespaceJobReceipt { receipt })?;
    }
    for request in snapshot.namespace_requests {
        tables.apply_checked(DfsMetadataTableOp::put_namespace_request(request))?;
    }
    for receipt in snapshot.namespace_request_receipts {
        tables.apply_checked(DfsMetadataTableOp::put_namespace_request_receipt(receipt))?;
    }
    tables.rebuild_prepared_operation_reservations()?;
    Ok(tables)
}

pub(crate) fn archive_metadata_backend_state(root: &Path) -> Result<bool> {
    let mut archived = false;
    let archive_root = root.join("replica-repair-archives").join(format!(
        "{}",
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let records = root.join("records");
    if records.exists() {
        if !archived {
            create_dir_all(archive_root.clone())?;
        }
        move_metadata_archive_path(&records, &archive_root.join("records"))?;
        archived = true;
    }
    let journal = root.join("journal");
    if journal.exists() {
        for entry in read_dir(&journal)? {
            let source = entry.path();
            if matches!(
                source.file_name().and_then(|name| name.to_str()),
                Some("write.lock" | "membership-handoff-write-freeze.sobj")
            ) {
                continue;
            }
            if !archived {
                create_dir_all(archive_root.clone())?;
            }
            let destination = archive_root.join("journal").join(entry.file_name());
            move_metadata_archive_path(&source, &destination)?;
            archived = true;
        }
    }
    if archived {
        sync_dir(root)?;
    }
    Ok(archived)
}

fn move_metadata_archive_path(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        create_dir_all(parent.to_owned())?;
    }
    fs::rename(source, destination).map_err(|source_error| MetadataBackendError::Io {
        path: source.to_path_buf(),
        source: source_error,
    })
}

pub(crate) fn truncate_metadata_table_log(root: &Path) -> Result<()> {
    let path = metadata_table_path(root);
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    file.sync_all().map_err(|source| MetadataBackendError::Io {
        path: path.clone(),
        source,
    })?;
    drop(file);
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "storage-conversion")]
    #[test]
    fn current_layout_change_yields_typed_pending_without_accepting_the_old_generation() {
        let directory = sinomo_testdir::TestDir::new("current-layout-replaced-during-observation");
        let root = directory.path();
        let before = super::metadata_table_storage_generation(root).unwrap();
        let snapshot = super::metadata_table_snapshot_path(root);
        std::fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        // Negative physical replacement only; these bytes grant no layout or
        // publication authority and are never accepted as a current snapshot.
        let replacement = b"unclassified replacement generation";
        std::fs::write(&snapshot, replacement).unwrap();
        let result = super::require_stable_current_metadata_layout(root, &before);
        assert!(
            matches!(result, Err(crate::MetadataBackendError::StoragePreparationPending {
            root: actual, processed_records: 0,
        }) if actual == root)
        );
        assert_eq!(std::fs::read(snapshot).unwrap(), replacement);
    }
    use super::*;
    fn genesis() -> DfsMetadataPrefixProof {
        crate::metadata_records::dfs_metadata_prefix_proof_genesis(&crate::DfsMetadataShardId::new(
            "root",
        ))
    }
    #[test]
    fn current_wire_checksum_matches_native_stream_and_includes_empty_fields() {
        let root = sinomo_testdir::TestDir::new("current-snapshot-checksum");
        let tables = DfsMetadataTables::default();
        let proof = genesis();
        let stage =
            crate::metadata_snapshot::stage_snapshot(root.path(), &tables, 0, &proof).unwrap();
        let image = crate::metadata_snapshot::read_snapshot(
            root.path(),
            fs::File::open(stage.path()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            snapshot_semantic_fingerprint(&tables, Some(0)).unwrap(),
            snapshot_semantic_fingerprint(&image.tables, image.materialized_through_sequence)
                .unwrap()
        );
        stage.install(root.path()).unwrap();
        let loaded = read_metadata_table_state_lenient(root.path()).unwrap();
        assert_eq!(loaded.prefix_proof, Some(proof));
        assert_eq!(
            loaded.snapshot_format_version,
            Some(METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
        );
    }
    #[test]
    fn native_stage_is_unpublished_until_atomic_install_and_drop_removes_it() {
        let root = sinomo_testdir::TestDir::new("current-snapshot-stage");
        let tables = DfsMetadataTables::default();
        let proof = genesis();
        persist_metadata_table_snapshot(root.path(), &tables, Some(0), Some(&proof)).unwrap();
        let before = metadata_table_storage_generation(root.path()).unwrap();
        let stage =
            crate::metadata_snapshot::stage_snapshot(root.path(), &tables, 0, &proof).unwrap();
        let staged_path = stage.path().to_owned();
        assert!(staged_path.is_file());
        assert_eq!(
            metadata_table_storage_generation(root.path()).unwrap(),
            before
        );
        stage.install(root.path()).unwrap();
        assert!(!staged_path.exists());
        assert_ne!(
            metadata_table_storage_generation(root.path()).unwrap(),
            before
        );
        let discarded =
            crate::metadata_snapshot::stage_snapshot(root.path(), &tables, 0, &proof).unwrap();
        let path = discarded.path().to_owned();
        drop(discarded);
        assert!(!path.exists());
    }
    #[test]
    fn complete_unsupported_table_record_is_rejected_without_truncating_it() {
        let root = sinomo_testdir::TestDir::new("current-table-complete-corruption");
        let tables = DfsMetadataTables::default();
        let proof = genesis();
        persist_metadata_table_snapshot(root.path(), &tables, Some(0), Some(&proof)).unwrap();
        let path = metadata_table_path(root.path());
        let mut bytes = 5u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(b"wrong");
        write_file_atomic(&path, &bytes).unwrap();
        assert!(read_metadata_table_state(root.path()).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    #[test]
    fn malformed_snapshot_footer_cannot_become_a_readable_checkpoint() {
        let root = sinomo_testdir::TestDir::new("current-snapshot-corrupt-footer");
        let tables = DfsMetadataTables::default();
        let proof = genesis();
        persist_metadata_table_snapshot(root.path(), &tables, Some(0), Some(&proof)).unwrap();
        let path = metadata_table_snapshot_path(root.path());
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        write_file_atomic(&path, &bytes).unwrap();
        assert!(read_metadata_table_state_lenient(root.path()).is_err());
    }
    #[test]
    fn retired_checkpoint_read_restarts_at_the_installed_native_generation() {
        let root = sinomo_testdir::TestDir::new("current-snapshot-generation-race");
        let tables = DfsMetadataTables::default();
        let proof = genesis();
        persist_metadata_table_snapshot(root.path(), &tables, Some(0), Some(&proof)).unwrap();
        let checkpoint = metadata_table_checkpoint_log_path(root.path());
        write_file_atomic(&checkpoint, &[]).unwrap();
        let stage =
            crate::metadata_snapshot::stage_snapshot(root.path(), &tables, 0, &proof).unwrap();
        let owned = root.to_path_buf();
        install_metadata_table_generation_open_hook(checkpoint.clone(), move || {
            stage.install(&owned).unwrap();
            remove_metadata_table_checkpoint_log(&owned).unwrap();
        });
        assert_eq!(
            read_metadata_table_state_lenient(root.path())
                .unwrap()
                .snapshot_format_version,
            Some(METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
        );
        assert!(!checkpoint.exists());
    }
}
