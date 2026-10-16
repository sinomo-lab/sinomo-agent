//! Current metadata snapshots, staged outside the writer and published at an exact cut.
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_records::*;
use crate::metadata_table_store::{
    metadata_table_snapshot_fingerprint, metadata_table_snapshot_path,
    snapshot_semantic_fingerprint,
};
use crate::metadata_tables::{DfsMetadataTableState, DfsMetadataTables};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_core::storage::{
    durable_file,
    migration::stream::{MigrationStreamReader, MigrationStreamWriter, StreamCursor},
};
use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::{Path, PathBuf};
pub(crate) const SNAPSHOT_FORMAT: u16 =
    crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION;
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct SnapshotHeader {
    format_version: u16,
    materialized_through_sequence: u64,
    prefix_proof: DfsMetadataPrefixProof,
    semantic_state_fingerprint: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    Prepared {
        record: DfsPreparedOperationRecord,
    },
    Eviction {
        record: DfsPreparedOperationEviction,
    },
}
#[derive(Default)]
struct CopyCursor {
    family: u8,
    key: String,
}
fn error(detail: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(detail.to_string())
}
fn encode<T: Serialize>(value: &T) -> std::result::Result<Vec<u8>, String> {
    serde_ext::to_bytes(value, SNAPSHOT_FORMAT, LengthMode::U32).map_err(|e| e.to_string())
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
                advisory_lock_states,
                v,
                DfsMetadataTableOp::PutAdvisoryLockState { state: v.clone() }
            ),

            18 => row!(
                namespace_transaction_receipts,
                v,
                DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt: v.clone() }
            ),

            19 => row!(
                namespace_jobs,
                v,
                DfsMetadataTableOp::PutNamespaceJob { job: v.clone() }
            ),
            20 => row!(
                namespace_job_receipts,
                v,
                DfsMetadataTableOp::PutNamespaceJobReceipt { receipt: v.clone() }
            ),
            21 => row!(
                namespace_requests,
                v,
                DfsMetadataTableOp::put_namespace_request(v.clone())
            ),
            22 => row!(
                namespace_request_receipts,
                v,
                DfsMetadataTableOp::put_namespace_request_receipt(v.clone())
            ),
            23 => next_map(&t.prepared_operations_v7, &cursor.key, |_, record| {
                SnapshotRecord::Prepared {
                    record: record.clone(),
                }
            })?,
            24 => next_map(&t.prepared_operation_evictions, &cursor.key, |_, record| {
                SnapshotRecord::Eviction {
                    record: record.clone(),
                }
            })?,
            25 => return Ok(None),
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

fn header(
    tables: &DfsMetadataTables,
    sequence: u64,
    proof: &DfsMetadataPrefixProof,
) -> Result<SnapshotHeader> {
    if !proof.is_well_formed() || proof.journal_sequence != sequence {
        return Err(error(
            "current snapshot requires its exact certified journal prefix",
        ));
    }
    Ok(SnapshotHeader {
        format_version: SNAPSHOT_FORMAT,
        materialized_through_sequence: sequence,
        prefix_proof: proof.clone(),
        semantic_state_fingerprint: snapshot_semantic_fingerprint(tables, Some(sequence))?,
    })
}
fn apply_record(tables: &mut DfsMetadataTables, record: SnapshotRecord) -> Result<()> {
    match record {
        SnapshotRecord::Eviction { record } => {
            record.validate()?;
            if tables
                .prepared_operation_evictions
                .insert(record.digest.operation_id.clone(), record)
                .is_some()
            {
                return Err(error("snapshot repeats an evicted operation identity"));
            }
        }
        SnapshotRecord::Table { op } => {
            tables.apply_checked(op)?;
        }
        SnapshotRecord::Transfer { transfer } => {
            let key = (transfer.object_id.clone(), transfer.transfer_id.clone());
            if tables
                .cross_shard_pin_transfers
                .insert(key, transfer)
                .is_some()
            {
                return Err(error("snapshot repeats a transfer identity"));
            }
        }
        SnapshotRecord::IntentRetention { state } => tables.intent_receipt_retention = state,
        SnapshotRecord::NamespaceRetention { state } => tables.namespace_receipt_retention = state,
        SnapshotRecord::Prepared { record } => {
            record.validate()?;
            if tables
                .prepared_operations_v7
                .insert(record.digest.operation_id.clone(), record)
                .is_some()
            {
                return Err(error("snapshot repeats a prepared operation identity"));
            }
        }
    }
    Ok(())
}
pub(crate) fn read_snapshot(root: &Path, file: fs::File) -> Result<DfsMetadataTableState> {
    read_snapshot_with_verified_fingerprint(root, file).map(|(state, _)| state)
}

/// Retain the semantic result checked by the same decoder. This is image DATA,
/// not authority; callers still check the actual file, prefix and native owner.
pub(crate) fn read_snapshot_with_verified_fingerprint(
    root: &Path,
    file: fs::File,
) -> Result<(DfsMetadataTableState, String)> {
    let (mut state, descriptor) = decode_snapshot_image_with_descriptor(file, None)?;
    state.snapshot_fingerprint = metadata_table_snapshot_fingerprint(root)?;
    Ok((state, descriptor.semantic_state_fingerprint))
}

/// SAME row decoder for native storage and a durably staged incoming image.
/// Wire receivers pass the exact command descriptor; this never supplies a
/// replica permit, configuration, or writer capability from decoded data.
pub(crate) fn decode_snapshot_image(
    file: fs::File,
    expected: Option<&crate::metadata_records::DfsMetadataSnapshotImageDescriptor>,
) -> Result<DfsMetadataTableState> {
    decode_snapshot_image_with_descriptor(file, expected).map(|(state, _)| state)
}

fn decode_snapshot_image_with_descriptor(
    file: fs::File,
    expected: Option<&crate::metadata_records::DfsMetadataSnapshotImageDescriptor>,
) -> Result<(DfsMetadataTableState, DfsMetadataSnapshotImageDescriptor)> {
    let len = file.metadata().map_err(error)?.len();
    if expected.is_some_and(|expected| expected.total_size_bytes != len) {
        return Err(error(
            "native snapshot image length differs from its command",
        ));
    }
    let mut reader = MigrationStreamReader::from_file(file).map_err(error)?;
    let stored: SnapshotHeader = serde_ext::from_bytes(&reader.header).map_err(error)?;
    if stored.format_version != SNAPSHOT_FORMAT
        || !stored.prefix_proof.is_well_formed()
        || stored.prefix_proof.journal_sequence != stored.materialized_through_sequence
    {
        return Err(error("unsupported or invalid current metadata snapshot"));
    }
    if expected.is_some_and(|expected| {
        expected.format_version != stored.format_version
            || expected.materialized_through_sequence != stored.materialized_through_sequence
            || expected.semantic_state_fingerprint != stored.semantic_state_fingerprint
            || expected.prefix_proof != stored.prefix_proof
    }) {
        return Err(error("native snapshot header differs from its command"));
    }
    let mut tables = DfsMetadataTables::default();
    while let Some(bytes) = reader
        .next_record(sinomo_core::storage::migration::stream::MAX_RECORD_BYTES as usize)
        .map_err(error)?
    {
        apply_record(&mut tables, serde_ext::from_bytes(&bytes).map_err(error)?)?;
    }
    // next_record consumed and validated the real terminal footer/EOF. The
    // descriptor binds the existing stream chain, not a second serialization.
    if expected.is_some_and(|expected| {
        expected.stream_record_count != reader.cursor().records
            || expected.stream_digest != reader.cursor().digest
    }) {
        return Err(error("native snapshot footer differs from its command"));
    }
    tables.rebuild_prepared_operation_reservations()?;
    if header(
        &tables,
        stored.materialized_through_sequence,
        &stored.prefix_proof,
    )? != stored
    {
        return Err(MetadataBackendError::replica_diverged(
            "current snapshot semantic checksum mismatch".into(),
        ));
    }
    let descriptor = DfsMetadataSnapshotImageDescriptor {
        format_version: stored.format_version,
        materialized_through_sequence: stored.materialized_through_sequence,
        semantic_state_fingerprint: stored.semantic_state_fingerprint,
        prefix_proof: stored.prefix_proof.clone(),
        total_size_bytes: len,
        stream_record_count: reader.cursor().records,
        stream_digest: reader.cursor().digest.clone(),
    };
    Ok((
        DfsMetadataTableState {
            tables,
            materialized_through_sequence: Some(stored.materialized_through_sequence),
            prefix_proof: Some(stored.prefix_proof),
            snapshot_format_version: Some(SNAPSHOT_FORMAT),
            snapshot_fingerprint: None,
            ..Default::default()
        },
        descriptor,
    ))
}

/// Compare every physical byte through the native framed stream chain. The
/// expected descriptor comes from the full row/semantic decoder below, so an
/// equal stream retains all of those validations without rebuilding the tables.
fn verify_snapshot_stream_identity(
    file: fs::File,
    expected: &DfsMetadataSnapshotImageDescriptor,
) -> Result<()> {
    if file.metadata().map_err(error)?.len() != expected.total_size_bytes {
        return Err(error("copied native image length changed"));
    }
    let mut reader = MigrationStreamReader::from_file(file).map_err(error)?;
    let stored: SnapshotHeader = serde_ext::from_bytes(&reader.header).map_err(error)?;
    if stored.format_version != expected.format_version
        || stored.materialized_through_sequence != expected.materialized_through_sequence
        || stored.semantic_state_fingerprint != expected.semantic_state_fingerprint
        || stored.prefix_proof != expected.prefix_proof
    {
        return Err(error("copied native image header changed"));
    }
    while reader
        .next_record(sinomo_core::storage::migration::stream::MAX_RECORD_BYTES as usize)
        .map_err(error)?
        .is_some()
    {}
    if reader.cursor().records != expected.stream_record_count
        || reader.cursor().digest != expected.stream_digest
    {
        return Err(error(
            "copied native image differs from its fully validated source",
        ));
    }
    Ok(())
}

/// Retain validated DATA across an exact immutable copy. The opened source
/// handle survives pathname replacement; the copied physical stream must match
/// the full decoder's header, record chain and terminal EOF before reuse.
pub(crate) fn copy_verified_snapshot(
    root: &Path,
    source: fs::File,
) -> Result<(StagedSnapshot, DfsMetadataTableState)> {
    let (state, descriptor) =
        decode_snapshot_image_with_descriptor(source.try_clone().map_err(error)?, None)?;
    Ok((copy_snapshot_stream(root, source, &descriptor)?, state))
}

/// Copy an already validated immutable image, checking its entire physical
/// output against the same native descriptor before exposing or installing it.
pub(crate) fn copy_snapshot_stream(
    root: &Path,
    mut source: fs::File,
    descriptor: &DfsMetadataSnapshotImageDescriptor,
) -> Result<StagedSnapshot> {
    use std::io::{Read, Seek, SeekFrom};
    let (stage, mut output) = StagedSnapshot::begin_received(root, &descriptor)?;
    let copied = (|| {
        source.seek(SeekFrom::Start(0)).map_err(error)?;
        let limit = descriptor
            .total_size_bytes
            .checked_add(1)
            .ok_or_else(|| error("native image copy length overflow"))?;
        if std::io::copy(&mut source.take(limit), &mut output).map_err(error)?
            != descriptor.total_size_bytes
        {
            return Err(error("native source length changed during capture"));
        }
        output.sync_all().map_err(error)
    })();
    // Close on every result before StagedSnapshot cleanup, including Windows.
    drop(output);
    copied?;
    verify_snapshot_stream_identity(fs::File::open(stage.path()).map_err(error)?, &descriptor)?;
    Ok(stage)
}
#[derive(Debug)]
pub(crate) struct StagedSnapshot {
    path: PathBuf,
    pub(crate) len: u64,
    native_header: SnapshotHeader,
    stream_cursor: StreamCursor,
}
impl StagedSnapshot {
    pub(crate) fn begin_received(
        root: &Path,
        descriptor: &crate::metadata_records::DfsMetadataSnapshotImageDescriptor,
    ) -> Result<(Self, fs::File)> {
        descriptor.validate()?;
        let path = root.join(format!(
            ".metadata-snapshot-receive-{}.stage",
            sinomo_core::Id::generate()
        ));
        let file = durable_file::create_private_staged_file(&path).map_err(error)?;
        let stage = Self {
            path,
            len: descriptor.total_size_bytes,
            native_header: SnapshotHeader {
                format_version: descriptor.format_version,
                materialized_through_sequence: descriptor.materialized_through_sequence,
                prefix_proof: descriptor.prefix_proof.clone(),
                semantic_state_fingerprint: descriptor.semantic_state_fingerprint.clone(),
            },
            stream_cursor: StreamCursor {
                offset: 0, // Received images never resume a writer cursor.
                records: descriptor.stream_record_count,
                digest: descriptor.stream_digest.clone(),
            },
        };
        Ok((stage, file))
    }
    pub(crate) fn image_descriptor(
        &self,
    ) -> crate::metadata_records::DfsMetadataSnapshotImageDescriptor {
        crate::metadata_records::DfsMetadataSnapshotImageDescriptor {
            format_version: self.native_header.format_version,
            materialized_through_sequence: self.native_header.materialized_through_sequence,
            semantic_state_fingerprint: self.native_header.semantic_state_fingerprint.clone(),
            prefix_proof: self.native_header.prefix_proof.clone(),
            total_size_bytes: self.len,
            stream_record_count: self.stream_cursor.records,
            stream_digest: self.stream_cursor.digest.clone(),
        }
    }
    pub(crate) fn attachment_source(
        self: &std::sync::Arc<Self>,
    ) -> std::result::Result<
        sinomo_core::network::ObjectRpcBodySource,
        sinomo_core::network::NetworkError,
    > {
        // ReplayableFile opens an independent reader for each send. An OS
        // File::try_clone would share an offset across concurrent targets.
        sinomo_core::network::ObjectRpcBodySource::file(self.path.clone())?
            .with_lifetime_guard(std::sync::Arc::clone(self))
    }
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn install(self, root: &Path) -> Result<()> {
        durable_file::install_staged_file(&self.path, &metadata_table_snapshot_path(root), true)
            .map_err(crate::metadata_io::metadata_durable_file_error)
    }
}
impl Drop for StagedSnapshot {
    fn drop(&mut self) {
        let _ = durable_file::remove_file_if_exists_and_sync_parent(&self.path);
    }
}
pub(crate) fn stage_snapshot(
    root: &Path,
    tables: &DfsMetadataTables,
    sequence: u64,
    proof: &DfsMetadataPrefixProof,
) -> Result<StagedSnapshot> {
    let path = metadata_table_snapshot_path(root)
        .with_extension(format!("{}.stage", sinomo_core::Id::generate()));
    fs::create_dir_all(
        path.parent()
            .ok_or_else(|| error("snapshot has no parent"))?,
    )
    .map_err(error)?;
    let native_header = header(tables, sequence, proof)?;
    let mut stage = StagedSnapshot {
        path,
        len: 0,
        native_header,
        stream_cursor: StreamCursor::default(),
    };
    let mut writer = MigrationStreamWriter::resume(
        &stage.path,
        &encode(&stage.native_header).map_err(error)?,
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
    stage.stream_cursor = writer.cursor().clone();
    drop(writer);
    stage.len = fs::metadata(&stage.path).map_err(error)?.len();
    Ok(stage)
}
pub(crate) fn persist_snapshot(
    root: &Path,
    tables: &DfsMetadataTables,
    sequence: Option<u64>,
    proof: Option<&DfsMetadataPrefixProof>,
) -> Result<()> {
    let sequence =
        sequence.ok_or_else(|| error("current snapshot has no materialization sequence"))?;
    let proof = proof.ok_or_else(|| error("current snapshot has no certified prefix"))?;
    stage_snapshot(root, tables, sequence, proof)?.install(root)
}

#[cfg(test)]
mod verified_copy_tests {
    use super::*;

    #[test]
    fn verified_snapshot_copy_keeps_exact_native_bytes_and_rejects_changed_output() {
        let name = "native-verified-snapshot-copy";
        let node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let (native, client) =
            crate::test_runtime::DfsTestRuntime::start_single_node(name, node.clone()).unwrap();
        drop(client);
        let root = crate::default_dfs_metadata_shards_root(native.data_dir(&node)).join("root");
        let backend = crate::LocalSobjMetadataBackend::open_for_inspection(
            crate::DfsMetadataShardId::new("root"),
            &root,
        )
        .unwrap();
        let snapshot = backend.export_table_snapshot();
        let sequence = snapshot.materialized_through_sequence;
        let proof = snapshot.prefix_proof.clone();
        let tables = crate::metadata_table_store::metadata_tables_from_table_snapshot(
            snapshot,
            "actual native copy fixture",
        )
        .unwrap();
        let directory = sinomo_testdir::TestDir::new(name);
        let original = stage_snapshot(directory.path(), &tables, sequence, &proof).unwrap();
        let original_bytes = fs::read(original.path()).unwrap();
        let (copied, state) =
            copy_verified_snapshot(directory.path(), fs::File::open(original.path()).unwrap())
                .unwrap();
        assert_eq!(fs::read(copied.path()).unwrap(), original_bytes);
        assert_eq!(copied.image_descriptor(), original.image_descriptor());
        assert_eq!(state.prefix_proof.as_ref(), Some(&proof));
        assert_eq!(state.materialized_through_sequence, Some(sequence));
        #[cfg(feature = "storage-conversion")]
        {
            let image = crate::DfsMetadataSnapshotImage::capture_existing(
                directory.path(),
                fs::File::open(original.path()).unwrap(),
            )
            .unwrap();
            let fresh = directory.join("fresh-candidate");
            assert!(!fresh.exists());
            let identity = crate::write_metadata_candidate_image(
                &fresh,
                &image,
                &native.file_system().file_system_id,
            )
            .unwrap();
            assert_eq!(
                fs::read(metadata_table_snapshot_path(&fresh)).unwrap(),
                original_bytes
            );
            assert_eq!(
                identity.state_fingerprint,
                image.descriptor().semantic_state_fingerprint
            );
            assert_eq!(identity.prefix_proof, proof);
        }
        let expected = copied.image_descriptor();
        for variant in 0..3 {
            let mut changed = original_bytes.clone();
            match variant {
                0 => {
                    changed.pop();
                }
                1 => {
                    *changed.last_mut().unwrap() ^= 1;
                }
                _ => changed.push(0),
            }
            fs::write(copied.path(), changed).unwrap();
            assert!(
                verify_snapshot_stream_identity(fs::File::open(copied.path()).unwrap(), &expected)
                    .is_err()
            );
        }
        fs::write(copied.path(), &original_bytes).unwrap();
        let mut wrong_count = expected.clone();
        wrong_count.stream_record_count += 1;
        assert!(
            verify_snapshot_stream_identity(fs::File::open(copied.path()).unwrap(), &wrong_count)
                .is_err()
        );
        let mut wrong_digest = expected.clone();
        wrong_digest.stream_digest = "0".repeat(64);
        assert!(
            verify_snapshot_stream_identity(fs::File::open(copied.path()).unwrap(), &wrong_digest)
                .is_err()
        );
        assert_eq!(fs::read(original.path()).unwrap(), original_bytes);
        drop((copied, original, backend));
        native.close().unwrap();
    }
}
