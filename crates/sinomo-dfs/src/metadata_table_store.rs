//! Durable storage for materialized metadata tables.
//!
//! This module owns the on-disk table log, compact snapshots, legacy table
//! import, snapshot migration, and repair-archive movement. The backend owns
//! metadata behavior; this layer only turns table batches and snapshots into a
//! durable `DfsMetadataTables` image.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use time::OffsetDateTime;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{
    StagedFileReplacement, create_dir_all, encode_sobj, metadata_durable_file_error, read_dir,
    read_sobj, read_sobj_optional, read_sobj_tree, remove_file_if_exists, stage_file_replacement,
    sync_dir, write_file_atomic, write_sobj_atomic,
};
use crate::metadata_journal::segmented_journal_entry_at_sequence;
use crate::metadata_keys::path_record_for_object;
use crate::metadata_records::{
    DfsConflictRecord, DfsDirFragmentRecord, DfsHardlinkIndexSnapshotRecord, DfsInodeRecord,
    DfsMetadataMutation, DfsMetadataPrefixProof, DfsMetadataTableBatch, DfsMetadataTableOp,
    DfsMetadataTableSnapshot, DfsMetadataTableSnapshotTables, DfsNamespaceTransaction,
    DfsObjectVersionRecord, DfsPathRecord, DfsReplicaReceiptRecord, DfsVisibleCommitSnapshotRecord,
    DfsVisibleVersionCommitRecord, DfsWriteIntentRecord,
    dfs_metadata_table_snapshot_state_fingerprint, normalized_namespace_prepare,
    normalized_object_metadata, normalized_version_record,
};
use crate::metadata_replay::decoded_namespace_prepare_payload;
use crate::metadata_tables::{
    DfsMetadataTableState, DfsMetadataTables, MetadataTableReplay,
    MetadataTableSnapshotFingerprint, max_optional_sequence,
};
use crate::{DfsFileSystem, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion};

pub(crate) const LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub(crate) const METADATA_TABLE_SNAPSHOT_FORMAT_VERSION: u16 = 2;
const METADATA_TABLE_SNAPSHOT_STAGE_STALE_AFTER: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MetadataTableFileGeneration {
    is_file: bool,
    len: u64,
    modified_unix_nanos: Option<u128>,
    created_unix_nanos: Option<u128>,
    platform_identity: Option<String>,
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
    fn is_compatible_with(&self, after: &Self) -> bool {
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

#[cfg(test)]
pub(crate) fn reset_metadata_table_journal_reference_resolutions() {
    METADATA_TABLE_JOURNAL_REFERENCE_RESOLUTIONS.set(0);
}

#[cfg(test)]
pub(crate) fn metadata_table_journal_reference_resolutions() -> usize {
    METADATA_TABLE_JOURNAL_REFERENCE_RESOLUTIONS.get()
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
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let file = fs::File::open(path).ok()?;
    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    // SAFETY: `file` owns a live OS handle and `information` points to writable
    // storage for the exact structure required by GetFileInformationByHandle.
    let has_information = unsafe {
        GetFileInformationByHandle(file.as_raw_handle() as HANDLE, information.as_mut_ptr()) != 0
    };
    if !has_information {
        return None;
    }
    // SAFETY: a successful GetFileInformationByHandle call initialized the
    // complete BY_HANDLE_FILE_INFORMATION value.
    let information = unsafe { information.assume_init() };
    let file_index =
        (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow);
    Some(format!(
        "windows:{}:{file_index}",
        information.dwVolumeSerialNumber
    ))
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

fn metadata_table_snapshot_verification_path(root: &Path) -> PathBuf {
    root.join("records")
        .join("tables")
        .join("snapshot.verified.sobj")
}

pub(crate) fn metadata_table_checkpoint_log_path(root: &Path) -> PathBuf {
    root.join("records")
        .join("tables")
        .join("materialized.checkpoint.slog")
}

pub(crate) fn load_or_migrate_metadata_tables(root: &Path) -> Result<DfsMetadataTableState> {
    let mut state = read_metadata_table_state(root)?;
    if state.snapshot_format_version.is_some_and(|version| {
        version < METADATA_TABLE_SNAPSHOT_FORMAT_VERSION && state.tables.has_materialized_state()
    }) {
        persist_metadata_table_snapshot(
            root,
            &state.tables,
            state.materialized_through_sequence,
            state.prefix_proof.as_ref(),
        )?;
        state.table_log_offset = 0;
        state.table_log_journal_reference_replay_cost_bytes = 0;
        state.table_log_unpriced_namespace_prepare_references = 0;
        state.snapshot_format_version = Some(METADATA_TABLE_SNAPSHOT_FORMAT_VERSION);
        state.snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
        return Ok(state);
    }
    if state.tables.has_materialized_state() {
        return Ok(state);
    }
    state.tables = load_legacy_metadata_tables(root)?;
    if state.tables.has_materialized_state() {
        persist_metadata_table_snapshot(
            root,
            &state.tables,
            state.materialized_through_sequence,
            state.prefix_proof.as_ref(),
        )?;
        state.table_log_offset = 0;
        state.table_log_journal_reference_replay_cost_bytes = 0;
        state.table_log_unpriced_namespace_prepare_references = 0;
        state.snapshot_format_version = Some(METADATA_TABLE_SNAPSHOT_FORMAT_VERSION);
        state.snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
    }
    Ok(state)
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
        state.table_log_journal_reference_replay_cost_bytes =
            replay.journal_reference_replay_cost_bytes;
        state.table_log_unpriced_namespace_prepare_references =
            replay.unpriced_namespace_prepare_references;
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
    state.table_log_journal_reference_replay_cost_bytes = 0;
    state.table_log_unpriced_namespace_prepare_references = 0;
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataTableSnapshotVerification {
    snapshot_len: u64,
    snapshot_blake3: String,
}

fn snapshot_verification_from_bytes(snapshot_bytes: &[u8]) -> DfsMetadataTableSnapshotVerification {
    DfsMetadataTableSnapshotVerification {
        snapshot_len: snapshot_bytes.len() as u64,
        snapshot_blake3: blake3::hash(snapshot_bytes).to_hex().to_string(),
    }
}

fn persist_snapshot_verification(
    root: &Path,
    verification: &DfsMetadataTableSnapshotVerification,
) -> Result<()> {
    write_sobj_atomic(
        &metadata_table_snapshot_verification_path(root),
        verification,
    )
}

fn read_metadata_table_snapshot(
    root: &Path,
    persist_verification: bool,
) -> Result<DfsMetadataTableState> {
    let path = metadata_table_snapshot_path(root);
    if !path.is_file() {
        return Ok(DfsMetadataTableState::default());
    }
    let snapshot: DfsMetadataTableSnapshot = read_sobj(&path)?;
    let format_version = snapshot.format_version;
    let materialized_through_sequence = snapshot.materialized_through_sequence;
    let prefix_proof = snapshot.prefix_proof.clone();
    let persisted_verification = read_sobj_optional::<DfsMetadataTableSnapshotVerification>(
        &metadata_table_snapshot_verification_path(root),
    )
    .unwrap_or(None);
    let mut current_verification = None;
    let snapshot_previously_verified = if let Some(expected) = persisted_verification.as_ref() {
        let (hash, snapshot_len) =
            durable_file::file_blake3_hash_and_size(&path).map_err(metadata_durable_file_error)?;
        let verification = DfsMetadataTableSnapshotVerification {
            snapshot_len,
            snapshot_blake3: hash.to_hex().to_string(),
        };
        let matches = &verification == expected;
        current_verification = Some(verification);
        matches
    } else {
        false
    };
    if !snapshot_previously_verified
        && let Some(expected) = snapshot.semantic_state_fingerprint.as_ref()
    {
        let actual =
            dfs_metadata_table_snapshot_state_fingerprint(&snapshot).map_err(|source| {
                MetadataBackendError::Serde(format!(
                    "encode DFS metadata table snapshot semantic checksum at {}: {source}",
                    path.display()
                ))
            })?;
        if &actual != expected {
            return Err(MetadataBackendError::Serde(format!(
                "DFS metadata table snapshot semantic checksum mismatch at {}",
                path.display()
            )));
        }
        if persist_verification {
            let verification = match current_verification {
                Some(verification) => verification,
                None => {
                    let (hash, snapshot_len) = durable_file::file_blake3_hash_and_size(&path)
                        .map_err(metadata_durable_file_error)?;
                    DfsMetadataTableSnapshotVerification {
                        snapshot_len,
                        snapshot_blake3: hash.to_hex().to_string(),
                    }
                }
            };
            let _ = persist_snapshot_verification(root, &verification);
        }
    }
    if let Some(proof) = prefix_proof.as_ref()
        && (!proof.is_well_formed()
            || Some(proof.journal_sequence) != materialized_through_sequence)
    {
        return Err(MetadataBackendError::Serde(format!(
            "DFS metadata table snapshot has an invalid prefix proof at {}",
            path.display()
        )));
    }
    let tables = metadata_tables_from_table_snapshot(
        snapshot,
        &format!("DFS metadata table snapshot at {}", path.display()),
    )?;
    Ok(DfsMetadataTableState {
        tables,
        table_log_offset: 0,
        table_log_journal_reference_replay_cost_bytes: 0,
        table_log_unpriced_namespace_prepare_references: 0,
        materialized_through_sequence,
        snapshot_format_version: Some(format_version),
        snapshot_fingerprint: metadata_table_snapshot_fingerprint(root)?,
        prefix_proof,
    })
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
    match snapshot.format_version {
        LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION => {
            let mut tables = DfsMetadataTables::default();
            for op in snapshot.ops {
                tables.apply_checked(op)?;
            }
            Ok(tables)
        }
        METADATA_TABLE_SNAPSHOT_FORMAT_VERSION => Ok(metadata_tables_from_snapshot(
            snapshot.tables.ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "{description} version {} is missing structured tables",
                    snapshot.format_version
                ))
            })?,
        )),
        _ => Err(MetadataBackendError::Serde(format!(
            "unsupported {description} version {}",
            snapshot.format_version
        ))),
    }
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
        Err(source) => {
            return Err(MetadataBackendError::Io {
                path: path.clone(),
                source,
            });
        }
    };
    let mut replay = MetadataTableReplay::default();
    loop {
        let Some(record) = durable_file::read_length_prefixed_record(
            &mut file,
            &path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
        )
        .map_err(metadata_durable_file_error)?
        else {
            break;
        };
        let batch: DfsMetadataTableBatch = match sinomo_object_serde::from_bytes(&record.bytes) {
            Ok(batch) => batch,
            Err(_) if record.record_end >= record.file_len => {
                file.seek(SeekFrom::Start(record.record_start))
                    .map_err(|source| MetadataBackendError::Io {
                        path: path.clone(),
                        source,
                    })?;
                break;
            }
            Err(source) => {
                return Err(MetadataBackendError::Serde(format!(
                    "failed to decode metadata table batch at {}: {source}",
                    path.display()
                )));
            }
        };
        replay.materialized_through_sequence =
            max_optional_sequence(replay.materialized_through_sequence, Some(batch.sequence));
        if let Some(proof) = batch.prefix_proof.as_ref() {
            if !proof.is_well_formed() || proof.journal_sequence != batch.sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata table batch {} has an invalid prefix proof at {}",
                    batch.sequence,
                    path.display()
                )));
            }
            replay.prefix_proof = Some(proof.clone());
        }
        replay.journal_reference_replay_cost_bytes = replay
            .journal_reference_replay_cost_bytes
            .saturating_add(batch.journal_reference_replay_cost_bytes);
        if batch.journal_reference_replay_cost_bytes == 0 {
            let unpriced_references = u64::try_from(
                batch
                    .ops
                    .iter()
                    .filter(|op| {
                        matches!(
                            op,
                            DfsMetadataTableOp::PutNamespacePrepareJournalReference { .. }
                        )
                    })
                    .count(),
            )
            .unwrap_or(u64::MAX);
            replay.unpriced_namespace_prepare_references = replay
                .unpriced_namespace_prepare_references
                .saturating_add(unpriced_references);
        }
        // Only advance past fully decoded records: a torn record at the tail
        // must leave the cursor at the last complete-record boundary.
        replay.offset = record.record_end;
    }
    Ok(replay)
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

fn replay_metadata_table_batches(
    file: &mut fs::File,
    root: &Path,
    path: &Path,
    checkpoint_sequence: Option<u64>,
    tables: &mut DfsMetadataTables,
) -> Result<MetadataTableReplay> {
    let mut offset = file
        .stream_position()
        .map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?;
    let mut materialized_through_sequence = None;
    let mut prefix_proof = None;
    let mut journal_reference_replay_cost_bytes = 0u64;
    let mut unpriced_namespace_prepare_references = 0u64;
    loop {
        let Some(record) = durable_file::read_length_prefixed_record(
            file,
            path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
        )
        .map_err(metadata_durable_file_error)?
        else {
            break;
        };
        let batch: DfsMetadataTableBatch = match sinomo_object_serde::from_bytes(&record.bytes) {
            Ok(batch) => batch,
            Err(_) if record.record_end >= record.file_len => {
                file.seek(SeekFrom::Start(record.record_start))
                    .map_err(|source| MetadataBackendError::Io {
                        path: path.to_owned(),
                        source,
                    })?;
                break;
            }
            Err(source) => {
                return Err(MetadataBackendError::Serde(format!(
                    "failed to decode metadata table batch at {}: {source}",
                    path.display()
                )));
            }
        };
        materialized_through_sequence =
            max_optional_sequence(materialized_through_sequence, Some(batch.sequence));
        if let Some(proof) = batch.prefix_proof.as_ref() {
            if !proof.is_well_formed() || proof.journal_sequence != batch.sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata table batch {} has an invalid prefix proof at {}",
                    batch.sequence,
                    path.display()
                )));
            }
            prefix_proof = Some(proof.clone());
        }
        if checkpoint_sequence.is_none_or(|checkpoint| batch.sequence > checkpoint) {
            journal_reference_replay_cost_bytes = journal_reference_replay_cost_bytes
                .saturating_add(batch.journal_reference_replay_cost_bytes);
            if batch.journal_reference_replay_cost_bytes == 0 {
                let unpriced_references = u64::try_from(
                    batch
                        .ops
                        .iter()
                        .filter(|op| {
                            matches!(
                                op,
                                DfsMetadataTableOp::PutNamespacePrepareJournalReference { .. }
                            )
                        })
                        .count(),
                )
                .unwrap_or(u64::MAX);
                unpriced_namespace_prepare_references =
                    unpriced_namespace_prepare_references.saturating_add(unpriced_references);
            }
            for op in batch.ops {
                tables.apply_checked(resolve_metadata_table_op_from_journal(
                    root,
                    batch.sequence,
                    op,
                )?)?;
            }
        }
        // Only advance past fully decoded records: a torn record at the tail
        // must leave the cursor at the last complete-record boundary.
        offset = record.record_end;
    }
    Ok(MetadataTableReplay {
        offset,
        journal_reference_replay_cost_bytes,
        unpriced_namespace_prepare_references,
        materialized_through_sequence,
        prefix_proof,
    })
}

fn resolve_metadata_table_op_from_journal(
    root: &Path,
    batch_sequence: u64,
    op: DfsMetadataTableOp,
) -> Result<DfsMetadataTableOp> {
    let DfsMetadataTableOp::PutNamespacePrepareJournalReference {
        shard_id,
        journal_sequence,
        file_system_id,
        operation_id,
        operation_digest,
        owner_epoch,
        status,
    } = op
    else {
        return Ok(op);
    };
    #[cfg(test)]
    METADATA_TABLE_JOURNAL_REFERENCE_RESOLUTIONS.set(
        METADATA_TABLE_JOURNAL_REFERENCE_RESOLUTIONS
            .get()
            .saturating_add(1),
    );
    if journal_sequence == 0 || journal_sequence > batch_sequence {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table batch {batch_sequence} has invalid namespace prepare {operation_id} journal reference {journal_sequence}"
        )));
    }
    let entry = segmented_journal_entry_at_sequence(root, journal_sequence)?.ok_or_else(|| {
        MetadataBackendError::Serde(format!(
            "metadata table batch {batch_sequence} cannot resolve namespace prepare {operation_id} from missing journal sequence {journal_sequence}"
        ))
    })?;
    if entry.shard_id != shard_id {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table namespace prepare {operation_id} journal reference names shard {}, entry {journal_sequence} belongs to {}",
            shard_id.0, entry.shard_id.0
        )));
    }
    let DfsMetadataMutation::PutNamespacePrepare {
        file_system_id: journal_file_system_id,
        operation_id: journal_operation_id,
        status: journal_status,
        ..
    } = &entry.mutation
    else {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table namespace prepare {operation_id} reference {journal_sequence} does not name a payload-complete put-namespace-prepare entry"
        )));
    };
    let prepare = decoded_namespace_prepare_payload(&entry)?.ok_or_else(|| {
        MetadataBackendError::Serde(format!(
            "metadata table namespace prepare {operation_id} reference {journal_sequence} does not name a payload-complete put-namespace-prepare entry"
        ))
    })?;
    let prepare = normalized_namespace_prepare(&prepare);
    if journal_file_system_id != &file_system_id
        || journal_operation_id != &operation_id
        || journal_status != &status
        || prepare.file_system_id != file_system_id
        || prepare.shard_id != shard_id
        || prepare.operation_id != operation_id
        || prepare.operation_digest != operation_digest
        || prepare.owner_epoch != owner_epoch
        || prepare.status != status
    {
        return Err(MetadataBackendError::Serde(format!(
            "metadata table namespace prepare {operation_id} reference {journal_sequence} scope/digest/epoch/status fence mismatch"
        )));
    }
    Ok(DfsMetadataTableOp::PutNamespacePrepare { prepare })
}

pub(crate) fn append_metadata_table_batch(
    root: &Path,
    batch: &DfsMetadataTableBatch,
) -> Result<u64> {
    let trace_started = Instant::now();
    let path = metadata_table_path(root);
    let create_dir_started = Instant::now();
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let create_dir_ms = create_dir_started.elapsed().as_millis();
    let encode_started = Instant::now();
    let bytes = sinomo_object_serde::to_bytes(batch, 1, LengthMode::U32)
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

#[cfg(test)]
pub(crate) fn install_encoded_metadata_table_snapshot(
    root: &Path,
    snapshot_bytes: &[u8],
) -> Result<()> {
    // Callers may install bytes received from another authority. Only the
    // normal read path may issue a verification receipt after checking the
    // embedded semantic fingerprint.
    write_file_atomic(&metadata_table_snapshot_path(root), snapshot_bytes)
}

/// A fully encoded and synced checkpoint snapshot that is not visible yet.
///
/// Callers stage this after folding the immutable checkpoint generation, then
/// enter the shard writer critical section, revalidate the snapshot/checkpoint
/// generation, and install it with [`install_staged_metadata_table_snapshot`].
#[derive(Debug)]
pub(crate) struct StagedMetadataTableSnapshot {
    replacement: StagedFileReplacement,
}

pub(crate) fn stage_encoded_metadata_table_snapshot(
    root: &Path,
    checkpoint_sequence: u64,
    snapshot_bytes: Vec<u8>,
) -> Result<StagedMetadataTableSnapshot> {
    let stale_before = SystemTime::now()
        .checked_sub(METADATA_TABLE_SNAPSHOT_STAGE_STALE_AFTER)
        .unwrap_or(UNIX_EPOCH);
    if let Err(error) = remove_metadata_table_snapshot_stages_older_than(root, stale_before) {
        tracing::warn!(
            target: "sinomo.dfs",
            root = %root.display(),
            %error,
            "could not remove stale unpublished metadata snapshot stages"
        );
    }
    let replacement = stage_file_replacement(
        &metadata_table_snapshot_path(root),
        &snapshot_bytes,
        &format!("checkpoint-{checkpoint_sequence}"),
    )?;
    // The large encoded Vec is intentionally consumed here. Only the synced
    // staging file remains while the caller waits to reacquire the shard lock.
    Ok(StagedMetadataTableSnapshot { replacement })
}

pub(crate) fn install_staged_metadata_table_snapshot(
    staged: StagedMetadataTableSnapshot,
) -> Result<()> {
    staged.replacement.install()
}

#[cfg(test)]
impl StagedMetadataTableSnapshot {
    fn path(&self) -> &Path {
        self.replacement.path()
    }
}

fn remove_metadata_table_snapshot_stages_older_than(
    root: &Path,
    stale_before: SystemTime,
) -> Result<usize> {
    let snapshot_path = metadata_table_snapshot_path(root);
    let parent = snapshot_path.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Ok(0);
    }
    let snapshot_name = snapshot_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("metadata");
    let prefix = format!(".{snapshot_name}.checkpoint-");
    let mut removed = 0_usize;
    for entry in read_dir(parent)? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&prefix) || !name.ends_with(".stage") {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|source| MetadataBackendError::Io {
                path: entry.path(),
                source,
            })?;
        if !file_type.is_file() {
            continue;
        }
        let metadata = entry
            .metadata()
            .map_err(|source| MetadataBackendError::Io {
                path: entry.path(),
                source,
            })?;
        if !metadata
            .modified()
            .ok()
            .is_some_and(|modified| modified <= stale_before)
        {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed = removed.saturating_add(1),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: entry.path(),
                    source,
                });
            }
        }
    }
    Ok(removed)
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

struct BorrowedVisibleCommitValues<'a>(&'a DfsMetadataTables);

#[derive(Serialize)]
struct BorrowedVisibleCommitSnapshotRecord<'a> {
    object_id: &'a DfsObjectId,
    version: &'a DfsVersion,
    commit: &'a DfsVisibleVersionCommitRecord,
}

impl Serialize for BorrowedVisibleCommitValues<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.visible_commits.len()))?;
        for ((object_id, version), commit) in &self.0.visible_commits {
            sequence.serialize_element(&BorrowedVisibleCommitSnapshotRecord {
                object_id,
                version,
                commit,
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
        let field_count = 1 + [
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
            !tables.conflicts.is_empty(),
            !tables.advisory_lock_states.is_empty(),
            !tables.visible_commits.is_empty(),
            !tables.namespace_transactions.is_empty(),
            !tables.namespace_transaction_receipts.is_empty(),
            !tables.namespace_prepares.is_empty(),
            !tables.namespace_prepare_receipts.is_empty(),
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
        if !tables.conflicts.is_empty() {
            state.serialize_field("conflicts", &BorrowedMapValues(&tables.conflicts))?;
        }
        if !tables.advisory_lock_states.is_empty() {
            state.serialize_field(
                "advisory_lock_states",
                &BorrowedMapValues(&tables.advisory_lock_states),
            )?;
        }
        if !tables.visible_commits.is_empty() {
            state.serialize_field("visible_commits", &BorrowedVisibleCommitValues(tables))?;
        }
        if !tables.namespace_transactions.is_empty() {
            state.serialize_field(
                "namespace_transactions",
                &BorrowedMapValues(&tables.namespace_transactions),
            )?;
        }
        if !tables.namespace_transaction_receipts.is_empty() {
            state.serialize_field(
                "namespace_transaction_receipts",
                &BorrowedMapValues(&tables.namespace_transaction_receipts),
            )?;
        }
        if !tables.namespace_prepares.is_empty() {
            state.serialize_field(
                "namespace_prepares",
                &BorrowedMapValues(&tables.namespace_prepares),
            )?;
        }
        if !tables.namespace_prepare_receipts.is_empty() {
            state.serialize_field(
                "namespace_prepare_receipts",
                &BorrowedMapValues(&tables.namespace_prepare_receipts),
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

struct BorrowedMetadataTableSnapshot<'a> {
    format_version: u16,
    materialized_through_sequence: Option<u64>,
    tables: &'a DfsMetadataTables,
    semantic_state_fingerprint: Option<&'a str>,
    prefix_proof: Option<&'a DfsMetadataPrefixProof>,
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

impl Serialize for BorrowedMetadataTableSnapshot<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let field_count = 2
            + usize::from(self.materialized_through_sequence.is_some())
            + usize::from(self.semantic_state_fingerprint.is_some())
            + usize::from(self.prefix_proof.is_some());
        let mut state = serializer.serialize_struct("DfsMetadataTableSnapshot", field_count)?;
        state.serialize_field("format_version", &self.format_version)?;
        if self.materialized_through_sequence.is_some() {
            state.serialize_field(
                "materialized_through_sequence",
                &self.materialized_through_sequence,
            )?;
        }
        let borrowed_tables = Some(BorrowedMetadataTableSnapshotTables(self.tables));
        state.serialize_field("tables", &borrowed_tables)?;
        if self.semantic_state_fingerprint.is_some() {
            state.serialize_field(
                "semantic_state_fingerprint",
                &self.semantic_state_fingerprint,
            )?;
        }
        if self.prefix_proof.is_some() {
            state.serialize_field("prefix_proof", &self.prefix_proof)?;
        }
        state.end()
    }
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
    let snapshot_bytes =
        encode_metadata_table_snapshot(tables, materialized_through_sequence, prefix_proof)?;
    write_file_atomic(&metadata_table_snapshot_path(root), &snapshot_bytes)?;
    let _ = persist_snapshot_verification(root, &snapshot_verification_from_bytes(&snapshot_bytes));
    truncate_metadata_table_log(root)?;
    remove_metadata_table_checkpoint_log(root)?;
    Ok(())
}

pub(crate) fn encode_metadata_table_snapshot(
    tables: &DfsMetadataTables,
    materialized_through_sequence: Option<u64>,
    prefix_proof: Option<&DfsMetadataPrefixProof>,
) -> Result<Vec<u8>> {
    if let Some(proof) = prefix_proof
        && (!proof.is_well_formed()
            || Some(proof.journal_sequence) != materialized_through_sequence)
    {
        return Err(MetadataBackendError::Serde(format!(
            "cannot encode metadata snapshot sequence {materialized_through_sequence:?} with prefix proof sequence {}",
            proof.journal_sequence
        )));
    }
    let semantic_snapshot = BorrowedMetadataTableSnapshot {
        format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        materialized_through_sequence,
        semantic_state_fingerprint: None,
        prefix_proof: None,
        tables,
    };
    let semantic_state_fingerprint = format!(
        "blake3:{}",
        streamed_json_blake3(&semantic_snapshot)?.to_hex()
    );
    let snapshot = BorrowedMetadataTableSnapshot {
        format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        materialized_through_sequence,
        tables,
        semantic_state_fingerprint: Some(&semantic_state_fingerprint),
        prefix_proof,
    };
    encode_sobj(&snapshot)
}

pub(crate) fn metadata_tables_snapshot(
    tables: &DfsMetadataTables,
) -> DfsMetadataTableSnapshotTables {
    DfsMetadataTableSnapshotTables {
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
        conflicts: tables.conflicts.values().cloned().collect(),
        advisory_lock_states: tables.advisory_lock_states.values().cloned().collect(),
        visible_commits: tables
            .visible_commits
            .iter()
            .map(
                |((object_id, version), commit)| DfsVisibleCommitSnapshotRecord {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    commit: commit.clone(),
                },
            )
            .collect(),
        namespace_transactions: tables.namespace_transactions.values().cloned().collect(),
        namespace_transaction_receipts: tables
            .namespace_transaction_receipts
            .values()
            .cloned()
            .collect(),
        namespace_prepares: tables.namespace_prepares.values().cloned().collect(),
        namespace_prepare_receipts: tables
            .namespace_prepare_receipts
            .values()
            .cloned()
            .collect(),
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

fn metadata_tables_from_snapshot(snapshot: DfsMetadataTableSnapshotTables) -> DfsMetadataTables {
    let mut tables = DfsMetadataTables::default();
    tables.namespace_receipt_retention = snapshot.namespace_receipt_retention;
    tables.intent_receipt_retention = snapshot.intent_receipt_retention;
    for file_system in snapshot.file_systems {
        tables.apply(DfsMetadataTableOp::PutFileSystem { file_system });
    }
    for object in snapshot.objects {
        tables.apply(DfsMetadataTableOp::PutObject { object });
    }
    for path_record in snapshot.file_system_objects {
        tables.apply(DfsMetadataTableOp::PutFileSystemObject { path_record });
    }
    for path_record in snapshot.paths {
        tables.apply(DfsMetadataTableOp::PutPath { path_record });
    }
    for inode in snapshot.inodes {
        tables.apply(DfsMetadataTableOp::PutInode { inode });
    }
    for dirfrag in snapshot.dirfrags {
        tables.apply(DfsMetadataTableOp::PutDirFragment { dirfrag });
    }
    for path_record in snapshot.dentries {
        tables.apply(DfsMetadataTableOp::PutDentry { path_record });
    }
    for hardlink in snapshot.hardlinks {
        tables.apply(DfsMetadataTableOp::PutHardlink {
            target_object_id: hardlink.target_object_id,
            hardlink_object_id: hardlink.hardlink_object_id,
            path_record: hardlink.path_record,
        });
    }
    for object in snapshot.compacted_tombstones {
        tables.apply(DfsMetadataTableOp::PutCompactedTombstone { object });
    }
    for version in snapshot.versions {
        tables.apply(DfsMetadataTableOp::PutVersion { version });
    }
    for pin in snapshot.version_pins {
        tables.apply(DfsMetadataTableOp::PutVersionPin { pin });
    }
    for transfer in snapshot.cross_shard_pin_transfers {
        tables.cross_shard_pin_transfers.insert(
            (transfer.object_id.clone(), transfer.transfer_id.clone()),
            transfer,
        );
    }
    for receipt in snapshot.receipts {
        tables.apply(DfsMetadataTableOp::PutReceipt { receipt });
    }
    for intent in snapshot.intents {
        tables.apply(DfsMetadataTableOp::PutIntent { intent });
    }
    for receipt in snapshot.intent_receipts {
        tables.apply(DfsMetadataTableOp::PutIntentReceipt { receipt });
    }
    for conflict in snapshot.conflicts {
        tables.apply(DfsMetadataTableOp::PutConflict { conflict });
    }
    for state in snapshot.advisory_lock_states {
        tables.apply(DfsMetadataTableOp::PutAdvisoryLockState { state });
    }
    for visible_commit in snapshot.visible_commits {
        tables.apply(DfsMetadataTableOp::PutVisibleCommit {
            object_id: visible_commit.object_id,
            version: visible_commit.version,
            commit: visible_commit.commit,
        });
    }
    for transaction in snapshot.namespace_transactions {
        tables.apply(DfsMetadataTableOp::PutNamespaceTransaction { transaction });
    }
    for receipt in snapshot.namespace_transaction_receipts {
        tables.apply(DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt });
    }
    for prepare in snapshot.namespace_prepares {
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare { prepare });
    }
    for receipt in snapshot.namespace_prepare_receipts {
        tables.apply(DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt });
    }
    for job in snapshot.namespace_jobs {
        tables.apply(DfsMetadataTableOp::PutNamespaceJob { job });
    }
    for receipt in snapshot.namespace_job_receipts {
        tables.apply(DfsMetadataTableOp::PutNamespaceJobReceipt { receipt });
    }
    for request in snapshot.namespace_requests {
        tables.apply(DfsMetadataTableOp::PutNamespaceRequest { request });
    }
    for receipt in snapshot.namespace_request_receipts {
        tables.apply(DfsMetadataTableOp::PutNamespaceRequestReceipt { receipt });
    }
    tables
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

#[cfg(test)]
pub(crate) fn metadata_table_snapshot_ops(tables: &DfsMetadataTables) -> Vec<DfsMetadataTableOp> {
    let mut ops = Vec::new();
    ops.extend(
        tables
            .file_systems
            .values()
            .cloned()
            .map(|file_system| DfsMetadataTableOp::PutFileSystem { file_system }),
    );
    ops.extend(
        tables
            .objects
            .values()
            .cloned()
            .map(|object| DfsMetadataTableOp::PutObject { object }),
    );
    ops.extend(
        tables
            .inodes
            .values()
            .cloned()
            .map(|inode| DfsMetadataTableOp::PutInode { inode }),
    );
    ops.extend(
        tables
            .file_system_objects
            .values()
            .cloned()
            .map(|path_record| DfsMetadataTableOp::PutFileSystemObject { path_record }),
    );
    ops.extend(
        tables
            .paths
            .values()
            .cloned()
            .map(|path_record| DfsMetadataTableOp::PutPath { path_record }),
    );
    ops.extend(
        tables
            .dirfrags
            .values()
            .cloned()
            .map(|dirfrag| DfsMetadataTableOp::PutDirFragment { dirfrag }),
    );
    ops.extend(
        tables
            .dentries
            .values()
            .cloned()
            .map(|path_record| DfsMetadataTableOp::PutDentry { path_record }),
    );
    ops.extend(tables.hardlinks.iter().map(
        |((target_object_id, hardlink_object_id), path_record)| DfsMetadataTableOp::PutHardlink {
            target_object_id: target_object_id.clone(),
            hardlink_object_id: hardlink_object_id.clone(),
            path_record: path_record.clone(),
        },
    ));
    ops.extend(
        tables
            .compacted_tombstones
            .values()
            .cloned()
            .map(|object| DfsMetadataTableOp::PutCompactedTombstone { object }),
    );
    ops.extend(
        tables
            .versions
            .values()
            .cloned()
            .map(|version| DfsMetadataTableOp::PutVersion { version }),
    );
    ops.extend(
        tables
            .receipts
            .values()
            .cloned()
            .map(|receipt| DfsMetadataTableOp::PutReceipt { receipt }),
    );
    ops.extend(
        tables
            .intents
            .values()
            .cloned()
            .map(|intent| DfsMetadataTableOp::PutIntent { intent }),
    );
    ops.extend(
        tables
            .conflicts
            .values()
            .cloned()
            .map(|conflict| DfsMetadataTableOp::PutConflict { conflict }),
    );
    ops.extend(
        tables
            .advisory_lock_states
            .values()
            .cloned()
            .map(|state| DfsMetadataTableOp::PutAdvisoryLockState { state }),
    );
    ops.extend(
        tables
            .visible_commits
            .iter()
            .map(
                |((object_id, version), commit)| DfsMetadataTableOp::PutVisibleCommit {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    commit: commit.clone(),
                },
            ),
    );
    ops.extend(
        tables
            .namespace_transactions
            .values()
            .cloned()
            .map(|transaction| DfsMetadataTableOp::PutNamespaceTransaction { transaction }),
    );
    ops.extend(
        tables
            .namespace_prepares
            .values()
            .cloned()
            .map(|prepare| DfsMetadataTableOp::PutNamespacePrepare { prepare }),
    );
    ops.extend(
        tables
            .namespace_jobs
            .values()
            .cloned()
            .map(|job| DfsMetadataTableOp::PutNamespaceJob { job }),
    );
    ops.extend(
        tables
            .namespace_job_receipts
            .values()
            .cloned()
            .map(|receipt| DfsMetadataTableOp::PutNamespaceJobReceipt { receipt }),
    );
    ops.extend(
        tables
            .namespace_requests
            .values()
            .cloned()
            .map(|request| DfsMetadataTableOp::PutNamespaceRequest { request }),
    );
    ops
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

fn load_legacy_metadata_tables(root: &Path) -> Result<DfsMetadataTables> {
    let records = root.join("records");
    let mut tables = DfsMetadataTables::default();
    for file_system in read_sobj_tree::<DfsFileSystem>(&records.join("filesystems"))? {
        tables.apply(DfsMetadataTableOp::PutFileSystem { file_system });
    }
    for object in read_sobj_tree::<DfsObjectMetadata>(&records.join("objects"))? {
        let object = normalized_object_metadata(&object);
        let path_record = path_record_for_object(&object);
        tables.apply(DfsMetadataTableOp::PutObject {
            object: object.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutInode {
            inode: DfsInodeRecord::from(&object),
        });
        tables.apply(DfsMetadataTableOp::PutFileSystemObject {
            path_record: path_record.clone(),
        });
        tables.apply(DfsMetadataTableOp::PutPath {
            path_record: path_record.clone(),
        });
        if object.kind != DfsObjectKind::Tombstone {
            tables.apply(DfsMetadataTableOp::PutDentry {
                path_record: path_record.clone(),
            });
        }
        if object.kind == DfsObjectKind::Hardlink
            && let Some(target_object_id) = object.link_target_object_id.as_ref()
        {
            tables.apply(DfsMetadataTableOp::PutHardlink {
                target_object_id: target_object_id.clone(),
                hardlink_object_id: object.object_id.clone(),
                path_record,
            });
        }
    }
    for path_record in read_sobj_tree::<DfsPathRecord>(&records.join("filesystem-objects"))? {
        tables.apply(DfsMetadataTableOp::PutFileSystemObject { path_record });
    }
    for path_record in read_sobj_tree::<DfsPathRecord>(&records.join("paths"))? {
        tables.apply(DfsMetadataTableOp::PutPath { path_record });
    }
    for inode in read_sobj_tree::<DfsInodeRecord>(&records.join("inodes"))? {
        tables.apply(DfsMetadataTableOp::PutInode { inode });
    }
    for dirfrag in read_sobj_tree::<DfsDirFragmentRecord>(&records.join("dirfrags"))? {
        tables.apply(DfsMetadataTableOp::PutDirFragment { dirfrag });
    }
    for path_record in read_sobj_tree::<DfsPathRecord>(&records.join("dentries"))? {
        tables.apply(DfsMetadataTableOp::PutDentry { path_record });
    }
    for version in read_sobj_tree::<DfsObjectVersionRecord>(&records.join("versions"))? {
        tables.apply(DfsMetadataTableOp::PutVersion {
            version: normalized_version_record(&version),
        });
    }
    for receipt in read_sobj_tree::<DfsReplicaReceiptRecord>(&records.join("receipts"))? {
        tables.apply(DfsMetadataTableOp::PutReceipt { receipt });
    }
    for intent in read_sobj_tree::<DfsWriteIntentRecord>(&records.join("intents"))? {
        tables.apply(DfsMetadataTableOp::PutIntent { intent });
    }
    for conflict in read_sobj_tree::<DfsConflictRecord>(&records.join("conflicts"))? {
        tables.apply(DfsMetadataTableOp::PutConflict { conflict });
    }
    for commit in read_sobj_tree::<DfsVisibleVersionCommitRecord>(&records.join("visible-commits"))?
    {
        tables.apply(DfsMetadataTableOp::PutVisibleCommit {
            object_id: commit.committed_object.object_id.clone(),
            version: commit.committed_object.version.clone(),
            commit,
        });
    }
    for transaction in
        read_sobj_tree::<DfsNamespaceTransaction>(&records.join("namespace-transactions"))?
    {
        tables.apply(DfsMetadataTableOp::PutNamespaceTransaction { transaction });
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_semantic_snapshot_fingerprint_preserves_historical_json_bytes() {
        let tables = DfsMetadataTables::default();
        let semantic_snapshot = BorrowedMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: Some(7),
            tables: &tables,
            semantic_state_fingerprint: None,
            prefix_proof: None,
        };
        let historical_bytes =
            serde_json::to_vec(&semantic_snapshot).expect("encode historical semantic JSON");
        let streamed =
            streamed_json_blake3(&semantic_snapshot).expect("stream semantic JSON fingerprint");
        assert_eq!(streamed, blake3::hash(&historical_bytes));

        let snapshot_bytes = encode_metadata_table_snapshot(&tables, Some(7), None)
            .expect("encode snapshot with streamed fingerprint");
        let snapshot: DfsMetadataTableSnapshot =
            sinomo_object_serde::from_bytes(&snapshot_bytes).expect("decode encoded snapshot");
        let expected = format!("blake3:{}", blake3::hash(&historical_bytes).to_hex());
        assert_eq!(
            snapshot.semantic_state_fingerprint.as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn staged_snapshot_is_invisible_until_atomic_install() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-staged-snapshot");
        let tables = DfsMetadataTables::default();
        persist_metadata_table_snapshot(root.path(), &tables, Some(1), None)
            .expect("persist current snapshot");
        let replacement = encode_metadata_table_snapshot(&tables, Some(2), None)
            .expect("encode replacement snapshot");
        let staged = stage_encoded_metadata_table_snapshot(root.path(), 2, replacement)
            .expect("stage replacement snapshot");
        assert!(staged.path().is_file());
        assert_eq!(
            read_metadata_table_state_lenient(root.path())
                .expect("read before install")
                .materialized_through_sequence,
            Some(1)
        );

        install_staged_metadata_table_snapshot(staged).expect("install staged snapshot");
        assert_eq!(
            read_metadata_table_state_lenient(root.path())
                .expect("read after install")
                .materialized_through_sequence,
            Some(2)
        );
    }

    #[test]
    fn stale_and_crash_orphan_snapshot_stages_never_replace_current_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-stale-snapshot-stage");
        let tables = DfsMetadataTables::default();
        persist_metadata_table_snapshot(root.path(), &tables, Some(1), None)
            .expect("persist current snapshot");
        let stale_bytes =
            encode_metadata_table_snapshot(&tables, Some(2), None).expect("encode stale snapshot");
        let stale = stage_encoded_metadata_table_snapshot(root.path(), 2, stale_bytes.clone())
            .expect("stage stale snapshot");
        let stale_path = stale.path().to_path_buf();

        let current_bytes =
            encode_metadata_table_snapshot(&tables, Some(3), None).expect("encode newer snapshot");
        install_encoded_metadata_table_snapshot(root.path(), &current_bytes)
            .expect("install newer snapshot");
        drop(stale);
        assert!(
            !stale_path.exists(),
            "discarded stale stage must be cleaned"
        );

        let orphan = stage_encoded_metadata_table_snapshot(root.path(), 4, stale_bytes)
            .expect("stage crash orphan");
        let orphan_path = orphan.path().to_path_buf();
        std::mem::forget(orphan);
        assert!(orphan_path.is_file());
        assert_eq!(
            read_metadata_table_state_lenient(root.path())
                .expect("restart-style read with orphan stage")
                .materialized_through_sequence,
            Some(3),
            "loaders must ignore an unpublished stage left by a crash"
        );
        assert_eq!(
            remove_metadata_table_snapshot_stages_older_than(
                root.path(),
                SystemTime::now() + Duration::from_secs(1),
            )
            .expect("reap crash orphan"),
            1
        );
        assert!(!orphan_path.exists());
    }

    #[test]
    fn table_state_read_restarts_when_checkpoint_generation_is_retired_before_open() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-checkpoint-read-race");
        let tables = DfsMetadataTables::default();
        persist_metadata_table_snapshot(root.path(), &tables, None, None)
            .expect("persist initial snapshot");
        let checkpoint_path = metadata_table_checkpoint_log_path(root.path());
        write_file_atomic(&checkpoint_path, &[]).expect("publish checkpoint generation");

        let replacement = encode_metadata_table_snapshot(&tables, Some(7), None)
            .expect("encode installed checkpoint snapshot");
        let root_path = root.path().to_path_buf();
        install_metadata_table_generation_open_hook(checkpoint_path.clone(), move || {
            install_encoded_metadata_table_snapshot(&root_path, &replacement)
                .expect("install replacement snapshot during read");
            remove_metadata_table_checkpoint_log(&root_path)
                .expect("retire checkpoint generation during read");
        });

        let state = read_metadata_table_state_lenient(root.path())
            .expect("restart read across checkpoint retirement");
        assert_eq!(
            state.materialized_through_sequence,
            Some(7),
            "the retry must restart at the newly installed snapshot"
        );
        assert!(!checkpoint_path.exists());
    }

    #[test]
    fn table_state_read_restarts_when_active_generation_rotates_before_open() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-active-read-race");
        let tables = DfsMetadataTables::default();
        persist_metadata_table_snapshot(root.path(), &tables, None, None)
            .expect("persist initial snapshot");
        append_metadata_table_batch(
            root.path(),
            &DfsMetadataTableBatch {
                sequence: 11,
                ops: Vec::new(),
                prefix_proof: None,
                journal_reference_replay_cost_bytes: 0,
            },
        )
        .expect("append active generation");

        let active_path = metadata_table_path(root.path());
        let checkpoint_path = metadata_table_checkpoint_log_path(root.path());
        let root_path = root.path().to_path_buf();
        install_metadata_table_generation_open_hook(active_path, move || {
            assert!(
                rotate_metadata_table_log_for_checkpoint(&root_path, 0)
                    .expect("rotate active generation during read")
            );
        });

        let state =
            read_metadata_table_state_lenient(root.path()).expect("restart read across rotation");
        assert_eq!(
            state.materialized_through_sequence,
            Some(11),
            "the retry must replay the rotated generation from the checkpoint"
        );
        assert!(checkpoint_path.is_file());
    }

    #[test]
    fn table_state_read_accepts_append_to_same_active_generation() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-active-append");
        let tables = DfsMetadataTables::default();
        persist_metadata_table_snapshot(root.path(), &tables, None, None)
            .expect("persist initial snapshot");
        append_metadata_table_batch(
            root.path(),
            &DfsMetadataTableBatch {
                sequence: 11,
                ops: Vec::new(),
                prefix_proof: None,
                journal_reference_replay_cost_bytes: 0,
            },
        )
        .expect("append initial active batch");

        let active_path = metadata_table_path(root.path());
        let root_path = root.path().to_path_buf();
        install_metadata_table_generation_open_hook(active_path, move || {
            append_metadata_table_batch(
                &root_path,
                &DfsMetadataTableBatch {
                    sequence: 12,
                    ops: Vec::new(),
                    prefix_proof: None,
                    journal_reference_replay_cost_bytes: 0,
                },
            )
            .expect("append to same active generation during read");
        });

        let state = match read_metadata_table_state_once(root.path(), false)
            .expect("read append-only active generation")
        {
            MetadataTableStateRead::Stable(state) => state,
            MetadataTableStateRead::GenerationChanged => {
                panic!("an append to the same active file is not a generation transition")
            }
        };
        assert_eq!(state.materialized_through_sequence, Some(12));
    }

    #[test]
    fn table_generation_retry_does_not_mask_other_missing_files() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-read-missing-journal");
        let missing_journal = root.path().join("journal").join("segments").join("1.slog");
        let error = MetadataBackendError::Io {
            path: missing_journal,
            source: io::Error::from(io::ErrorKind::NotFound),
        };
        assert!(
            !is_metadata_table_generation_not_found(root.path(), &error),
            "missing authoritative journal data must remain fail closed"
        );
    }

    #[test]
    fn verified_snapshot_receipt_skips_reencoding_unchanged_semantic_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-table-snapshot-verification");
        let tables = DfsMetadataTables::default();
        persist_metadata_table_snapshot(root.path(), &tables, None, None)
            .expect("persist verified snapshot");

        crate::metadata_records::reset_dfs_metadata_full_snapshot_fingerprint_calls();
        read_metadata_table_state(root.path()).expect("read verified snapshot");
        assert_eq!(
            crate::metadata_records::dfs_metadata_full_snapshot_fingerprint_calls(),
            0,
            "a raw-content-matched receipt must avoid rebuilding the semantic snapshot"
        );

        let replacement = encode_metadata_table_snapshot(&tables, Some(1), None)
            .expect("encode replacement snapshot");
        write_file_atomic(&metadata_table_snapshot_path(root.path()), &replacement)
            .expect("replace snapshot without its verification receipt");
        crate::metadata_records::reset_dfs_metadata_full_snapshot_fingerprint_calls();
        let replaced =
            read_metadata_table_state(root.path()).expect("validate replacement snapshot");
        assert_eq!(replaced.materialized_through_sequence, Some(1));
        assert_eq!(
            crate::metadata_records::dfs_metadata_full_snapshot_fingerprint_calls(),
            1,
            "changed snapshot bytes must invalidate the old verification receipt"
        );

        crate::metadata_records::reset_dfs_metadata_full_snapshot_fingerprint_calls();
        read_metadata_table_state(root.path()).expect("read repaired verification receipt");
        assert_eq!(
            crate::metadata_records::dfs_metadata_full_snapshot_fingerprint_calls(),
            0,
            "successful validation must repair the derived receipt for later opens"
        );
    }
}
