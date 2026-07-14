//! Durable storage for materialized metadata tables.
//!
//! This module owns the on-disk table log, compact snapshots, legacy table
//! import, snapshot migration, and repair-archive movement. The backend owns
//! metadata behavior; this layer only turns table batches and snapshots into a
//! durable `DfsMetadataTables` image.

use std::fs;
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Instant, UNIX_EPOCH};

use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use time::OffsetDateTime;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{
    create_dir_all, metadata_durable_file_error, read_dir, read_sobj, read_sobj_tree, sync_dir,
    write_sobj_atomic,
};
use crate::metadata_keys::path_record_for_object;
use crate::metadata_records::{
    DfsConflictRecord, DfsDirFragmentRecord, DfsHardlinkIndexSnapshotRecord, DfsInodeRecord,
    DfsMetadataPrefixProof, DfsMetadataTableBatch, DfsMetadataTableOp, DfsMetadataTableSnapshot,
    DfsMetadataTableSnapshotTables, DfsNamespaceTransaction, DfsObjectVersionRecord, DfsPathRecord,
    DfsReplicaReceiptRecord, DfsVisibleCommitSnapshotRecord, DfsVisibleVersionCommitRecord,
    DfsWriteIntentRecord, dfs_metadata_table_snapshot_state_fingerprint,
    normalized_object_metadata, normalized_version_record,
};
use crate::metadata_tables::{
    DfsMetadataTableState, DfsMetadataTables, MetadataTableReplay,
    MetadataTableSnapshotFingerprint, max_optional_sequence,
};
use crate::{DfsFileSystem, DfsObjectKind, DfsObjectMetadata};

pub(crate) const LEGACY_METADATA_TABLE_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub(crate) const METADATA_TABLE_SNAPSHOT_FORMAT_VERSION: u16 = 2;

fn metadata_table_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "dfs metadata table store perf");
}

pub(crate) fn metadata_table_path(root: &Path) -> PathBuf {
    root.join("records")
        .join("tables")
        .join("materialized.slog")
}

pub(crate) fn metadata_table_snapshot_path(root: &Path) -> PathBuf {
    root.join("records").join("tables").join("snapshot.sobj")
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

fn read_metadata_table_state_with_tail_policy(
    root: &Path,
    truncate_torn_tail: bool,
) -> Result<DfsMetadataTableState> {
    let mut state = read_metadata_table_snapshot(root)?;
    let table_path = metadata_table_path(root);
    if table_path.is_file() {
        let replay = replay_metadata_table_batches_from(&table_path, 0, &mut state.tables)?;
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

fn read_metadata_table_snapshot(root: &Path) -> Result<DfsMetadataTableState> {
    let path = metadata_table_snapshot_path(root);
    if !path.is_file() {
        return Ok(DfsMetadataTableState::default());
    }
    let snapshot: DfsMetadataTableSnapshot = read_sobj(&path)?;
    let format_version = snapshot.format_version;
    let materialized_through_sequence = snapshot.materialized_through_sequence;
    let prefix_proof = snapshot.prefix_proof.clone();
    if let Some(expected) = snapshot.semantic_state_fingerprint.as_ref() {
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
                tables.apply(op);
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
    path: &Path,
    offset: u64,
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
    replay_metadata_table_batches(&mut file, path, tables)
}

fn replay_metadata_table_batches(
    file: &mut fs::File,
    path: &Path,
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
        for op in batch.ops {
            tables.apply(op);
        }
        // Only advance past fully decoded records: a torn record at the tail
        // must leave the cursor at the last complete-record boundary.
        offset = record.record_end;
    }
    Ok(MetadataTableReplay {
        offset,
        materialized_through_sequence,
        prefix_proof,
    })
}

pub(crate) fn append_metadata_table_batch(
    root: &Path,
    batch: &DfsMetadataTableBatch,
) -> Result<u64> {
    let trace_started = Instant::now();
    let path = metadata_table_path(root);
    let existed = path.exists();
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
    let write_sync_started = Instant::now();
    let mut append = durable_file::open_file_for_append_with_options(
        &path,
        durable_file::DurableAppendFileOptions::new().without_parent_sync(),
    )
    .map_err(metadata_durable_file_error)?;
    let record_bytes = durable_file::append_length_prefixed_records(
        &mut append.file,
        &path,
        [bytes.as_slice()],
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Little)
            .sync(durable_file::DurableFileSync::All),
    )
    .map_err(metadata_durable_file_error)?;
    let write_sync_ms = write_sync_started.elapsed().as_millis();
    let sync_dir_started = Instant::now();
    if !existed && let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    let sync_dir_ms = sync_dir_started.elapsed().as_millis();
    metadata_table_perf_trace(format_args!(
        "metadata-table-batch sequence={} ops={} bytes={} existed={} create_dir_ms={} encode_ms={} write_sync_ms={} sync_dir_ms={} total_ms={}",
        batch.sequence,
        batch.ops.len(),
        record_bytes,
        existed,
        create_dir_ms,
        encode_ms,
        write_sync_ms,
        sync_dir_ms,
        trace_started.elapsed().as_millis()
    ));
    Ok(record_bytes)
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
    let mut snapshot = DfsMetadataTableSnapshot {
        format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        materialized_through_sequence,
        tables: Some(metadata_tables_snapshot(tables)),
        ops: Vec::new(),
        semantic_state_fingerprint: None,
        prefix_proof: prefix_proof.cloned(),
    };
    snapshot.semantic_state_fingerprint = Some(
        dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
            .map_err(|source| MetadataBackendError::Serde(source.to_string()))?,
    );
    write_sobj_atomic(&metadata_table_snapshot_path(root), &snapshot)?;
    truncate_metadata_table_log(root)?;
    Ok(())
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
        receipts: tables.receipts.values().cloned().collect(),
        intents: tables.intents.values().cloned().collect(),
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
        namespace_prepares: tables.namespace_prepares.values().cloned().collect(),
        namespace_jobs: tables.namespace_jobs.values().cloned().collect(),
    }
}

fn metadata_tables_from_snapshot(snapshot: DfsMetadataTableSnapshotTables) -> DfsMetadataTables {
    let mut tables = DfsMetadataTables::default();
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
    for receipt in snapshot.receipts {
        tables.apply(DfsMetadataTableOp::PutReceipt { receipt });
    }
    for intent in snapshot.intents {
        tables.apply(DfsMetadataTableOp::PutIntent { intent });
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
    for prepare in snapshot.namespace_prepares {
        tables.apply(DfsMetadataTableOp::PutNamespacePrepare { prepare });
    }
    for job in snapshot.namespace_jobs {
        tables.apply(DfsMetadataTableOp::PutNamespaceJob { job });
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
            if source.file_name().and_then(|name| name.to_str()) == Some("write.lock") {
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
