//! Physical metadata upgrade. Journal identities and file content are unchanged.
//! A sealed checkpoint is copied into a checksummed V3 record stream; active
//! writes continue in the ordinary table/journal suffix. Publication compares
//! the source snapshot generation under the existing metadata writer lock.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_core::storage::{
    durable_file,
    migration::{
        MigrationAdvance, MigrationBatch, MigrationBudget, MigrationDriver, MigrationIdentity,
        MigrationJob, MigrationJobStore, MigrationPublication, MigrationStep,
        stream::{MigrationStreamReader, MigrationStreamWriter, StreamCursor},
    },
};

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::write_sobj_atomic;
use crate::metadata_records::*;
use crate::metadata_table_store::{
    metadata_table_snapshot_fingerprint, metadata_table_snapshot_path,
};
use crate::metadata_tables::{
    DfsMetadataTableState, DfsMetadataTables, MetadataTableSnapshotFingerprint,
};

pub(crate) const SNAPSHOT_FORMAT: u16 = 3;
#[cfg(feature = "migration-bench")]
pub(crate) mod benchmark;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct SnapshotHeader {
    format_version: u16,
    materialized_through_sequence: Option<u64>,
    prefix_proof: Option<DfsMetadataPrefixProof>,
    generation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum SnapshotRecord {
    Table {
        op: DfsMetadataTableOp,
    },
    Transfer {
        transfer: DfsCrossShardPinTransferRecord,
    },
    IntentRetention {
        state: DfsWriteIntentReceiptRetentionState,
    },
    NamespaceRetention {
        state: DfsNamespaceReceiptRetentionState,
    },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct CopyCursor {
    family: u8,
    key: String,
    stream: StreamCursor,
}

fn error(message: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(message.to_string())
}

fn encode<T: Serialize>(value: &T) -> std::result::Result<Vec<u8>, String> {
    serde_ext::to_bytes(value, SNAPSHOT_FORMAT, LengthMode::U32).map_err(|e| e.to_string())
}

fn decode_cursor<T: DeserializeOwned + Default>(bytes: &[u8]) -> std::result::Result<T, String> {
    if bytes.is_empty() {
        Ok(T::default())
    } else {
        serde_ext::from_bytes(bytes).map_err(|e| e.to_string())
    }
}

fn next_map<K: Ord + Serialize + DeserializeOwned, V>(
    map: &BTreeMap<K, V>,
    after: &str,
    convert: impl FnOnce(&K, &V) -> SnapshotRecord,
) -> std::result::Result<Option<(SnapshotRecord, String)>, String> {
    let key: Option<K> = if after.is_empty() {
        None
    } else {
        Some(serde_json::from_str(after).map_err(|e| e.to_string())?)
    };
    let next = match key.as_ref() {
        Some(key) => map.range((Excluded(key), Unbounded)).next(),
        None => map.iter().next(),
    };
    next.map(|(key, value)| {
        Ok((
            convert(key, value),
            serde_json::to_string(key).map_err(|e| e.to_string())?,
        ))
    })
    .transpose()
}

fn next_record(
    t: &DfsMetadataTables,
    cursor: &mut CopyCursor,
) -> std::result::Result<Option<SnapshotRecord>, String> {
    macro_rules! row {
        ($field:ident, $value:ident, $op:expr) => {
            next_map(&t.$field, &cursor.key, |_, $value| SnapshotRecord::Table {
                op: $op,
            })?
        };
    }
    loop {
        let next = match cursor.family {
            0 => {
                if cursor.key.is_empty() {
                    Some((
                        SnapshotRecord::IntentRetention {
                            state: t.intent_receipt_retention.clone(),
                        },
                        "singleton".into(),
                    ))
                } else {
                    None
                }
            }
            1 => {
                if cursor.key.is_empty() {
                    Some((
                        SnapshotRecord::NamespaceRetention {
                            state: t.namespace_receipt_retention.clone(),
                        },
                        "singleton".into(),
                    ))
                } else {
                    None
                }
            }
            2 => row!(
                file_systems,
                v,
                DfsMetadataTableOp::PutFileSystem {
                    file_system: v.clone()
                }
            ),
            3 => row!(
                objects,
                v,
                DfsMetadataTableOp::PutObject { object: v.clone() }
            ),
            4 => row!(
                file_system_objects,
                v,
                DfsMetadataTableOp::PutFileSystemObject {
                    path_record: v.clone()
                }
            ),
            5 => row!(
                paths,
                v,
                DfsMetadataTableOp::PutPath {
                    path_record: v.clone()
                }
            ),
            6 => row!(inodes, v, DfsMetadataTableOp::PutInode { inode: v.clone() }),
            7 => row!(
                dirfrags,
                v,
                DfsMetadataTableOp::PutDirFragment { dirfrag: v.clone() }
            ),
            8 => row!(
                dentries,
                v,
                DfsMetadataTableOp::PutDentry {
                    path_record: v.clone()
                }
            ),
            9 => next_map(&t.hardlinks, &cursor.key, |(target, object), v| {
                SnapshotRecord::Table {
                    op: DfsMetadataTableOp::PutHardlink {
                        target_object_id: target.clone(),
                        hardlink_object_id: object.clone(),
                        path_record: v.clone(),
                    },
                }
            })?,
            10 => row!(
                compacted_tombstones,
                v,
                DfsMetadataTableOp::PutCompactedTombstone { object: v.clone() }
            ),
            11 => row!(
                versions,
                v,
                DfsMetadataTableOp::PutVersion { version: v.clone() }
            ),
            12 => row!(
                version_pins,
                v,
                DfsMetadataTableOp::PutVersionPin { pin: v.clone() }
            ),
            13 => next_map(&t.cross_shard_pin_transfers, &cursor.key, |_, v| {
                SnapshotRecord::Transfer {
                    transfer: v.clone(),
                }
            })?,
            14 => row!(
                receipts,
                v,
                DfsMetadataTableOp::PutReceipt { receipt: v.clone() }
            ),
            15 => row!(
                intents,
                v,
                DfsMetadataTableOp::PutIntent { intent: v.clone() }
            ),
            16 => row!(
                intent_receipts,
                v,
                DfsMetadataTableOp::PutIntentReceipt { receipt: v.clone() }
            ),
            17 => row!(
                conflicts,
                v,
                DfsMetadataTableOp::PutConflict {
                    conflict: v.clone()
                }
            ),
            18 => row!(
                advisory_lock_states,
                v,
                DfsMetadataTableOp::PutAdvisoryLockState { state: v.clone() }
            ),
            19 => next_map(&t.visible_commits, &cursor.key, |(object, version), v| {
                SnapshotRecord::Table {
                    op: DfsMetadataTableOp::PutVisibleCommit {
                        object_id: object.clone(),
                        version: version.clone(),
                        commit: v.clone(),
                    },
                }
            })?,
            20 => row!(
                namespace_transactions,
                v,
                DfsMetadataTableOp::PutNamespaceTransaction {
                    transaction: v.clone()
                }
            ),
            21 => row!(
                namespace_transaction_receipts,
                v,
                DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt: v.clone() }
            ),
            22 => row!(
                namespace_prepares,
                v,
                DfsMetadataTableOp::put_namespace_prepare(v.clone())
            ),
            23 => row!(
                namespace_prepare_receipts,
                v,
                DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt: v.clone() }
            ),
            24 => row!(
                namespace_jobs,
                v,
                DfsMetadataTableOp::PutNamespaceJob { job: v.clone() }
            ),
            25 => row!(
                namespace_job_receipts,
                v,
                DfsMetadataTableOp::PutNamespaceJobReceipt { receipt: v.clone() }
            ),
            26 => row!(
                namespace_requests,
                v,
                DfsMetadataTableOp::put_namespace_request(v.clone())
            ),
            27 => row!(
                namespace_request_receipts,
                v,
                DfsMetadataTableOp::put_namespace_request_receipt(v.clone())
            ),
            28 => return Ok(None),
            _ => return Err("unknown metadata snapshot cursor family".into()),
        };
        if let Some((record, key)) = next {
            cursor.key = key;
            return Ok(Some(record));
        }
        cursor.family += 1;
        cursor.key.clear();
    }
}

fn snapshot_header(
    sequence: Option<u64>,
    proof: Option<&DfsMetadataPrefixProof>,
) -> Result<SnapshotHeader> {
    if let Some(proof) = proof
        && (!proof.is_well_formed() || sequence != Some(proof.journal_sequence))
    {
        return Err(error("invalid snapshot migration prefix proof"));
    }
    let identity = serde_json::to_vec(&(SNAPSHOT_FORMAT, sequence, proof)).map_err(error)?;
    Ok(SnapshotHeader {
        format_version: SNAPSHOT_FORMAT,
        materialized_through_sequence: sequence,
        prefix_proof: proof.cloned(),
        generation: blake3::hash(&identity).to_hex().to_string(),
    })
}

fn apply_record(tables: &mut DfsMetadataTables, record: SnapshotRecord) -> Result<()> {
    match record {
        SnapshotRecord::Table { op } => {
            tables.apply_checked(op)?;
        }
        SnapshotRecord::Transfer { transfer } => {
            tables.cross_shard_pin_transfers.insert(
                (transfer.object_id.clone(), transfer.transfer_id.clone()),
                transfer,
            );
        }
        SnapshotRecord::IntentRetention { state } => tables.intent_receipt_retention = state,
        SnapshotRecord::NamespaceRetention { state } => tables.namespace_receipt_retention = state,
    }
    Ok(())
}

pub(crate) fn read_snapshot(root: &Path, file: fs::File) -> Result<DfsMetadataTableState> {
    read_snapshot_inner(root, file).map_err(|source| {
        MetadataBackendError::replica_diverged(format!(
            "DFS metadata snapshot stream at {}: {source}",
            metadata_table_snapshot_path(root).display(),
        ))
    })
}

fn read_snapshot_inner(root: &Path, file: fs::File) -> Result<DfsMetadataTableState> {
    let mut reader = MigrationStreamReader::from_file(file).map_err(error)?;
    let header: SnapshotHeader = serde_ext::from_bytes(&reader.header).map_err(error)?;
    if header
        != snapshot_header(
            header.materialized_through_sequence,
            header.prefix_proof.as_ref(),
        )?
    {
        return Err(error("invalid metadata snapshot stream header"));
    }
    let mut tables = DfsMetadataTables::default();
    while let Some(bytes) = reader
        .next_record(sinomo_core::storage::migration::stream::MAX_RECORD_BYTES as usize)
        .map_err(error)?
    {
        apply_record(&mut tables, serde_ext::from_bytes(&bytes).map_err(error)?)?;
    }
    Ok(DfsMetadataTableState {
        tables,
        materialized_through_sequence: header.materialized_through_sequence,
        prefix_proof: header.prefix_proof,
        snapshot_format_version: Some(SNAPSHOT_FORMAT),
        snapshot_fingerprint: metadata_table_snapshot_fingerprint(root)?,
        ..Default::default()
    })
}

/// New snapshots, including replica installs, are written in current format.
/// The caller already owns publication authority and the metadata write lock.
pub(crate) fn persist_snapshot(
    root: &Path,
    tables: &DfsMetadataTables,
    sequence: Option<u64>,
    proof: Option<&DfsMetadataPrefixProof>,
) -> Result<()> {
    let header = snapshot_header(sequence, proof)?;
    let path = metadata_table_snapshot_path(root);
    if path.is_file()
        && !sinomo_core::storage::migration::stream::is_stream(&path).map_err(error)?
        && !crate::metadata_table_store::metadata_table_checkpoint_log_path(root).is_file()
        && let (Some(sequence), Some(proof)) = (sequence, proof)
    {
        // Pre-proof legacy bootstrap must publish its first durable anchor
        // before admitting writers. It retains that existing initialization
        // fence, but now its potentially large conversion is restartable too.
        let source_version = crate::metadata_table_store::legacy_snapshot_format(root)?;
        let mut migration = SnapshotMigration::new(
            root,
            tables,
            sequence,
            proof,
            metadata_table_snapshot_fingerprint(root)?,
            u32::from(source_version),
        )?;
        migration.publication_lock_held = true;
        migration.run()?;
        return Ok(());
    }
    let staged = path.with_extension(format!("{}.stage", sinomo_core::Id::generate()));
    fs::create_dir_all(path.parent().expect("snapshot parent")).map_err(error)?;
    let result = (|| {
        let mut writer = MigrationStreamWriter::resume(
            &staged,
            &encode(&header).map_err(error)?,
            &StreamCursor::default(),
        )
        .map_err(error)?;
        let mut cursor = CopyCursor::default();
        while let Some(record) = next_record(tables, &mut cursor).map_err(error)? {
            writer
                .append(&encode(&record).map_err(error)?)
                .map_err(error)?;
        }
        writer.finish().map_err(error)?;
        drop(writer);
        durable_file::install_staged_file(&staged, &path, true)
            .map_err(crate::metadata_io::metadata_durable_file_error)
    })();
    if result.is_err() {
        let _ = durable_file::remove_file_if_exists_and_sync_parent(&staged);
    }
    result
}

pub(crate) struct SnapshotMigration<'a> {
    root: &'a Path,
    tables: &'a DfsMetadataTables,
    header: SnapshotHeader,
    stage: PathBuf,
    source_snapshot: Option<MetadataTableSnapshotFingerprint>,
    source_version: u32,
    publication_lock_held: bool,
}

impl<'a> SnapshotMigration<'a> {
    pub(crate) fn new(
        root: &'a Path,
        tables: &'a DfsMetadataTables,
        sequence: u64,
        proof: &DfsMetadataPrefixProof,
        source_snapshot: Option<MetadataTableSnapshotFingerprint>,
        source_version: u32,
    ) -> Result<Self> {
        let header = snapshot_header(Some(sequence), Some(proof))?;
        let stage = root
            .join("records/tables")
            .join(format!("snapshot-{}.migration", header.generation));
        let mut migration = Self {
            root,
            tables,
            header,
            stage,
            source_snapshot,
            source_version,
            publication_lock_held: false,
        };
        // Publication may have succeeded before the completion checkpoint.
        // Reopening now observes V3 as the source layout, but must resume the
        // originally admitted migration, not invent a different identity.
        if let Some(job) = migration.store().read().map_err(error)? {
            let mut expected = migration.identity();
            expected.source_version = job.identity.source_version;
            if job.identity != expected {
                return Err(error("snapshot migration identity changed"));
            }
            migration.source_version = job.identity.source_version;
        }
        Ok(migration)
    }

    pub(crate) fn identity(&self) -> MigrationIdentity {
        MigrationIdentity {
            owner: "sinomo-dfs".into(),
            migration: "metadata-snapshot-v3".into(),
            scope: self.root.to_string_lossy().into(),
            source_version: self.source_version,
            target_version: u32::from(SNAPSHOT_FORMAT),
            source_generation: self.header.generation.clone(),
        }
    }

    pub(crate) fn store(&self) -> MigrationJobStore {
        MigrationJobStore::new(
            self.root
                .join("records/tables/migrations")
                .join(format!("{}.sobj", self.header.generation)),
        )
    }

    pub(crate) fn is_installed(&self) -> Result<bool> {
        let path = metadata_table_snapshot_path(self.root);
        Ok(path.is_file()
            && sinomo_core::storage::migration::stream::is_stream(&path).map_err(error)?
            && sinomo_core::storage::migration::stream::read_header(&path).map_err(error)?
                == encode(&self.header).map_err(error)?)
    }

    pub(crate) fn run(&mut self) -> Result<MigrationJob> {
        let cancelled = AtomicBool::new(false);
        loop {
            match self.advance(MigrationBudget::default(), &cancelled)? {
                MigrationAdvance::Complete(job) | MigrationAdvance::Superseded(job) => {
                    return Ok(job);
                }
                MigrationAdvance::Busy => {
                    return Err(error(
                        "metadata snapshot migration is owned by another executor",
                    ));
                }
                MigrationAdvance::Progress(_) | MigrationAdvance::Yielded => {
                    std::thread::yield_now()
                }
            }
        }
    }

    pub(crate) fn advance(
        &mut self,
        budget: MigrationBudget,
        cancelled: &AtomicBool,
    ) -> Result<MigrationAdvance> {
        if !self.root.is_dir() {
            return Err(error("metadata migration storage root no longer exists"));
        }
        let pointer = self.root.join("records/tables/migration-current.sobj");
        if current_generation(self.root)?.is_none() || self.store().read().map_err(error)?.is_none()
        {
            write_sobj_atomic(&pointer, &self.header.generation)?;
        }
        self.store()
            .advance(&self.identity(), budget, cancelled, self)
            .map_err(error)
    }
}

impl MigrationDriver for SnapshotMigration<'_> {
    fn source_is_current(&mut self, _job: &MigrationJob) -> std::result::Result<bool, String> {
        Ok(
            metadata_table_snapshot_fingerprint(self.root).map_err(|e| e.to_string())?
                == self.source_snapshot
                || self.is_installed().map_err(|e| e.to_string())?,
        )
    }

    fn discard_staging(&mut self, _job: &MigrationJob) -> std::result::Result<(), String> {
        durable_file::remove_file_if_exists_and_sync_parent(&self.stage)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn copy_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> std::result::Result<MigrationBatch, String> {
        let mut cursor: CopyCursor = decode_cursor(&job.cursor)?;
        let mut writer =
            MigrationStreamWriter::resume(&self.stage, &encode(&self.header)?, &cursor.stream)
                .map_err(|e| e.to_string())?;
        let (mut records, mut bytes, mut complete) = (0, 0, false);
        while records < step.budget.max_records && !step.should_yield() {
            let mut next = cursor.clone();
            let Some(record) = next_record(self.tables, &mut next)? else {
                complete = true;
                break;
            };
            let encoded = encode(&record)?;
            if encoded.len() > step.budget.max_bytes {
                return Err("snapshot record exceeds migration byte budget".into());
            }
            if encoded.len() > step.budget.max_bytes - bytes {
                break;
            }
            writer.append(&encoded).map_err(|e| e.to_string())?;
            cursor = next;
            records += 1;
            bytes += encoded.len();
        }
        cursor.stream = writer.checkpoint().map_err(|e| e.to_string())?;
        if complete {
            writer.finish().map_err(|e| e.to_string())?;
        }
        Ok(MigrationBatch {
            cursor: encode(&cursor)?,
            records,
            bytes,
            complete,
        })
    }

    fn verify_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> std::result::Result<MigrationBatch, String> {
        let mut cursor: CopyCursor = decode_cursor(&job.cursor)?;
        let mut reader = MigrationStreamReader::resume(&self.stage, &cursor.stream)
            .map_err(|e| e.to_string())?;
        if reader.header != encode(&self.header)? {
            return Err("snapshot verification source changed".into());
        }
        let (mut records, mut bytes, mut complete) = (0, 0, false);
        while records < step.budget.max_records && !step.should_yield() {
            let len = reader.next_record_len().map_err(|e| e.to_string())? as usize;
            if len > step.budget.max_bytes {
                return Err("snapshot record exceeds verification budget".into());
            }
            if len > step.budget.max_bytes - bytes {
                break;
            }
            let Some(encoded) = reader
                .next_record(step.budget.max_bytes)
                .map_err(|e| e.to_string())?
            else {
                complete = true;
                break;
            };
            let expected = next_record(self.tables, &mut cursor)?
                .ok_or_else(|| "migration output contains extra records".to_string())?;
            if encoded != encode(&expected)? {
                return Err("migration output differs from its immutable source".into());
            }
            records += 1;
            bytes += encoded.len();
        }
        if complete
            && (reader.cursor().records != job.copied_records
                || next_record(self.tables, &mut cursor)?.is_some())
        {
            return Err("snapshot verification record count mismatch".into());
        }
        cursor.stream = reader.cursor().clone();
        Ok(MigrationBatch {
            cursor: encode(&cursor)?,
            records,
            bytes,
            complete,
        })
    }

    fn publish(
        &mut self,
        _job: &MigrationJob,
    ) -> std::result::Result<MigrationPublication, String> {
        let _lock = if self.publication_lock_held {
            None
        } else {
            Some(
                crate::metadata_lock::metadata_checkpoint_write_lock(
                    self.root.to_path_buf(),
                    crate::metadata_cursor::journal_write_lock_path(self.root),
                )
                .map_err(|e| e.to_string())?,
            )
        };
        let destination = metadata_table_snapshot_path(self.root);
        if destination.is_file()
            && sinomo_core::storage::migration::stream::is_stream(&destination)
                .map_err(|e| e.to_string())?
            && sinomo_core::storage::migration::stream::read_header(&destination)
                .map_err(|e| e.to_string())?
                == encode(&self.header)?
        {
            write_sobj_atomic(
                &self.root.join("records/tables/migration-current.sobj"),
                &self.header.generation,
            )
            .map_err(|e| e.to_string())?;
            return Ok(MigrationPublication::Published);
        }
        if metadata_table_snapshot_fingerprint(self.root).map_err(|e| e.to_string())?
            != self.source_snapshot
            || (!self.publication_lock_held
                && !crate::metadata_table_store::metadata_table_checkpoint_log_path(self.root)
                    .is_file())
        {
            return Ok(MigrationPublication::Superseded);
        }
        durable_file::install_staged_file(&self.stage, &destination, true)
            .map_err(|e| e.to_string())?;
        write_sobj_atomic(
            &self.root.join("records/tables/migration-current.sobj"),
            &self.header.generation,
        )
        .map_err(|e| e.to_string())?;
        Ok(MigrationPublication::Published)
    }
}

pub(crate) fn status(root: &Path) -> Result<Option<MigrationJob>> {
    let Some(generation) = current_generation(root)? else {
        return Ok(None);
    };
    if generation.len() != 64 || !generation.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(error("invalid snapshot migration generation"));
    }
    MigrationJobStore::new(
        root.join("records/tables/migrations")
            .join(format!("{generation}.sobj")),
    )
    .read()
    .map_err(error)
}

fn current_generation(root: &Path) -> Result<Option<String>> {
    let path = root.join("records/tables/migration-current.sobj");
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(MetadataBackendError::Io { path, source }),
    };
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes).map_err(error)?;
    if bytes.len() > 4096 {
        return Err(error("snapshot migration pointer exceeds its size bound"));
    }
    serde_ext::from_bytes(&bytes).map(Some).map_err(error)
}
