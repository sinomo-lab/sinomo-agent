//! Current-format candidate images. This module only stages hidden storage;
//! activation uses the normal shard authority and snapshot installation path.
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_records::{
    DfsMetadataPrefixProof, DfsMetadataTableSnapshot, DfsMetadataTableSnapshotTables,
};
use crate::metadata_table_store::{
    metadata_table_snapshot_path, metadata_tables_from_table_snapshot, metadata_tables_snapshot,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataCandidateIdentity {
    pub state_fingerprint: String,
    pub file_fingerprint: String,
    pub prefix_proof: DfsMetadataPrefixProof,
}

/// An exclusively owned staging directory. It cannot overwrite a populated
/// metadata root: a durable candidate stamp must precede every image write.
pub struct DfsMetadataCandidateWriter {
    root: PathBuf,
    _lock: File,
}
impl DfsMetadataCandidateWriter {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|source| MetadataBackendError::Io {
            path: root.clone(),
            source,
        })?;
        let stamp = root.join("metadata-candidate-v7.json");
        let lock_path = root.join("metadata-candidate.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| MetadataBackendError::Io {
                path: lock_path.clone(),
                source,
            })?;
        lock.try_lock_exclusive()
            .map_err(|source| MetadataBackendError::Io {
                path: lock_path.clone(),
                source,
            })?;
        if stamp.exists() {
            let bytes = fs::read(&stamp).map_err(|source| MetadataBackendError::Io {
                path: stamp.clone(),
                source,
            })?;
            if bytes != b"{\"format_version\":7,\"candidate\":true}" {
                return Err(MetadataBackendError::Serde(
                    "invalid current candidate stamp".into(),
                ));
            }
        } else {
            for entry in fs::read_dir(&root).map_err(|source| MetadataBackendError::Io {
                path: root.clone(),
                source,
            })? {
                let entry = entry.map_err(|source| MetadataBackendError::Io {
                    path: root.clone(),
                    source,
                })?;
                if entry.path() != lock_path {
                    return Err(MetadataBackendError::Serde(
                        "candidate writer requires an empty staging root".into(),
                    ));
                }
            }
            crate::metadata_io::write_file_atomic(
                &stamp,
                b"{\"format_version\":7,\"candidate\":true}",
            )?;
        }
        Ok(Self { root, _lock: lock })
    }
    pub fn write(
        &self,
        tables: DfsMetadataTableSnapshotTables,
        proof: DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataCandidateIdentity> {
        let mut snapshot = DfsMetadataTableSnapshot {
            format_version: crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: proof.journal_sequence,
            tables: Box::new(tables),
            semantic_state_fingerprint: String::new(),
            prefix_proof: proof.clone(),
        };
        snapshot.semantic_state_fingerprint =
            crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(&snapshot)
                .map_err(|e| MetadataBackendError::Serde(e.to_string()))?;
        let tables = metadata_tables_from_table_snapshot(snapshot, "current candidate")?;
        crate::metadata_snapshot::persist_snapshot(
            &self.root,
            &tables,
            Some(proof.journal_sequence),
            Some(&proof),
        )?;
        inspect_metadata_candidate(&self.root)
    }
    /// Append actual current frames to this hidden image. The caller carries
    /// the exact preceding conversion cut; this creates no publication permit.
    pub fn append(
        &self,
        entry: &crate::DfsMetadataJournalEntry,
        previous: &DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataPrefixProof> {
        let frame = crate::metadata_journal::DfsMetadataJournalFrame::encode(entry)?;
        let proof = crate::metadata_journal::metadata_prefix_proof_after_exact_frame(
            previous,
            entry,
            &entry.shard_id,
            frame.exact_bytes(),
        )?;
        match &entry.mutation {
            crate::DfsMetadataMutation::PrepareOperationV7 {
                record,
                file_system_id,
                operation_id,
            } => {
                record.validate()?;
                if record.digest.prepare_sequence != entry.sequence
                    || &record.digest.file_system_id != file_system_id
                    || &record.digest.operation_id != operation_id
                    || record.digest.shard_id != entry.shard_id
                {
                    return Err(MetadataBackendError::Serde(
                        "candidate prepare frame scope mismatch".into(),
                    ));
                }
            }
            crate::DfsMetadataMutation::CommitPreparedOperationV7 {
                digest,
                file_system_id,
                operation_id,
            } => {
                let prepared = crate::metadata_journal::segmented_journal_entry_at_sequence(
                    &self.root,
                    digest.prepare_sequence,
                )?
                .ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "candidate commit has no durable prepare frame".into(),
                    )
                })?;
                if !matches!(&prepared.mutation, crate::DfsMetadataMutation::PrepareOperationV7 { record, .. } if record.digest == *digest && record.digest.shard_id == entry.shard_id)
                {
                    return Err(MetadataBackendError::Serde(
                        "candidate commit names another durable body".into(),
                    ));
                }
                if &digest.file_system_id != file_system_id
                    || &digest.operation_id != operation_id
                    || digest.shard_id != entry.shard_id
                    || digest.prepare_sequence >= entry.sequence
                {
                    return Err(MetadataBackendError::Serde(
                        "candidate commit frame scope mismatch".into(),
                    ));
                }
            }
        }
        crate::metadata_journal::append_segmented_journal_exact_frame_with_record_bytes(
            &self.root,
            entry,
            frame.exact_bytes(),
        )?;
        crate::metadata_cursor::write_journal_head(&self.root, entry.sequence.saturating_add(1))?;
        Ok(proof)
    }
    pub fn prepare(
        &self,
        payload: crate::DfsPreparedOperationPayload,
        previous: &DfsMetadataPrefixProof,
    ) -> Result<(crate::DfsPreparedOperationRecord, DfsMetadataPrefixProof)> {
        let sequence = previous.journal_sequence.checked_add(1).ok_or_else(|| {
            MetadataBackendError::Serde("candidate journal sequence overflow".into())
        })?;
        let digest = crate::DfsPreparedOperationDigest::at_sequence(&payload, sequence)?;
        let record = crate::DfsPreparedOperationRecord {
            digest,
            payload: std::sync::Arc::new(payload),
            status: crate::DfsPreparedOperationStatus::Prepared,
            committed_entry: None,
        };
        let entry = crate::DfsMetadataJournalEntry {
            shard_id: record.digest.shard_id.clone(),
            sequence,
            created_at: record.transaction.created_at,
            mutation: crate::DfsMetadataMutation::PrepareOperationV7 {
                file_system_id: record.digest.file_system_id.clone(),
                operation_id: record.digest.operation_id.clone(),
                record: Box::new(record.clone()),
            },
        };
        let proof = self.append(&entry, previous)?;
        Ok((record, proof))
    }
    pub fn commit(
        &self,
        mut record: crate::DfsPreparedOperationRecord,
        previous: &DfsMetadataPrefixProof,
        at: time::OffsetDateTime,
    ) -> Result<(crate::DfsPreparedOperationRecord, DfsMetadataPrefixProof)> {
        record.validate()?;
        if record.status != crate::DfsPreparedOperationStatus::Prepared {
            return Err(MetadataBackendError::Serde(
                "candidate commit requires its exact prepared body".into(),
            ));
        }
        let entry = crate::DfsMetadataJournalEntry {
            shard_id: record.digest.shard_id.clone(),
            sequence: previous.journal_sequence.checked_add(1).ok_or_else(|| {
                MetadataBackendError::Serde("candidate journal sequence overflow".into())
            })?,
            created_at: at,
            mutation: crate::DfsMetadataMutation::CommitPreparedOperationV7 {
                file_system_id: record.digest.file_system_id.clone(),
                operation_id: record.digest.operation_id.clone(),
                digest: record.digest.clone(),
            },
        };
        let proof = self.append(&entry, previous)?;
        record.status = crate::DfsPreparedOperationStatus::Committed;
        record.committed_entry = Some(Box::new(
            crate::DfsPreparedOperationCommitReference::from_journal_entry(&entry)?,
        ));
        record.validate()?;
        Ok((record, proof))
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// Read the actual native current image and verify both framing and semantic
/// checksum. Returned hashes describe storage, never publication authority.
pub fn read_metadata_candidate(root: &Path) -> Result<DfsMetadataTableSnapshot> {
    let path = metadata_table_snapshot_path(root);
    let file = File::open(&path).map_err(|source| MetadataBackendError::Io { path, source })?;
    let (state, fingerprint) =
        crate::metadata_snapshot::read_snapshot_with_verified_fingerprint(root, file)?;
    let proof = state
        .prefix_proof
        .ok_or_else(|| MetadataBackendError::Serde("current image has no prefix".into()))?;
    let sequence = state
        .materialized_through_sequence
        .ok_or_else(|| MetadataBackendError::Serde("current image has no cut".into()))?;
    Ok(DfsMetadataTableSnapshot {
        format_version: crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        materialized_through_sequence: sequence,
        semantic_state_fingerprint: fingerprint,
        tables: Box::new(metadata_tables_snapshot(&state.tables)),
        prefix_proof: proof,
    })
}

/// Immutable DATA export of an actual unmounted current candidate. The same
/// native record reader validates the existing image before capture; no writer
/// or publication capability is constructed from this data.
pub fn read_metadata_candidate_image(root: &Path) -> Result<crate::DfsMetadataSnapshotImage> {
    let path = metadata_table_snapshot_path(root);
    let file = File::open(&path).map_err(|source| MetadataBackendError::Io { path, source })?;
    let state = crate::metadata_snapshot::read_snapshot(root, file)?;
    let proof = state
        .prefix_proof
        .as_ref()
        .ok_or_else(|| MetadataBackendError::Serde("current candidate has no prefix".into()))?;
    let sequence = state
        .materialized_through_sequence
        .ok_or_else(|| MetadataBackendError::Serde("current candidate has no cut".into()))?;
    crate::DfsMetadataSnapshotImage::capture(root, &state.tables, sequence, proof)
}

/// Stage actual native image DATA with the ordinary candidate lock/parser and
/// row invariants. Activation remains the existing certified native operation.
pub fn write_metadata_candidate_image(
    root: &Path,
    image: &crate::DfsMetadataSnapshotImage,
    fs: &crate::DfsFileSystemId,
) -> Result<DfsMetadataCandidateIdentity> {
    image.validate_file_system(fs)?;
    let state = image.state()?;
    let writer = DfsMetadataCandidateWriter::open(root)?;
    crate::metadata_snapshot::persist_snapshot(
        &writer.root,
        &state.tables,
        Some(image.materialized_through_sequence),
        Some(&image.prefix_proof),
    )?;
    inspect_metadata_candidate(root)
}

/// Reuse the normal owned chunk worker for a hidden import candidate. Its real
/// Main/Embedded data owner outlives a cancelled waiter and staged-file cleanup.
pub async fn receive_metadata_candidate_image(
    root: PathBuf,
    descriptor: crate::DfsMetadataSnapshotImageDescriptor,
    body: sinomo_core::network::ObjectRpcIncomingBody,
    owner: std::sync::Arc<crate::runtime_host::DfsDataOwnerLock>,
) -> Result<crate::DfsMetadataSnapshotImage> {
    let attachment = crate::DfsInboundAttachment {
        descriptor: crate::DfsRpcAttachmentDescriptor {
            total_size_bytes: descriptor.total_size_bytes,
            content_hash: None,
        },
        body,
    };
    crate::runtime_message::ingest_native_snapshot_attachment(
        root, descriptor, attachment, owner, None,
    )
    .await
    .map_err(|error| {
        MetadataBackendError::Serde(format!("receive actual current candidate image: {error}"))
    })
}
pub fn inspect_metadata_candidate(root: &Path) -> Result<DfsMetadataCandidateIdentity> {
    use std::io::Read;
    // Inspect the actual native rows once and retain that verified result. An
    // identity query needs no second table projection or semantic serialization.
    let path = metadata_table_snapshot_path(root);
    let file = File::open(&path).map_err(|source| MetadataBackendError::Io {
        path: path.clone(),
        source,
    })?;
    let (state, fingerprint) =
        crate::metadata_snapshot::read_snapshot_with_verified_fingerprint(root, file)?;
    let proof = state
        .prefix_proof
        .ok_or_else(|| MetadataBackendError::Serde("current image has no prefix".into()))?;
    state
        .materialized_through_sequence
        .ok_or_else(|| MetadataBackendError::Serde("current image has no cut".into()))?;
    let mut file = File::open(&path).map_err(|source| MetadataBackendError::Io {
        path: path.clone(),
        source,
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(DfsMetadataCandidateIdentity {
        state_fingerprint: fingerprint,
        file_fingerprint: format!("blake3:{}", hasher.finalize().to_hex()),
        prefix_proof: proof,
    })
}

/// Current table projection used while assembling an unmounted candidate.
/// It shares the ordinary namespace planner and checked row projector.
pub struct DfsMetadataCandidateState {
    tables: crate::metadata_tables::DfsMetadataTables,
}
impl DfsMetadataCandidateState {
    pub fn new(tables: DfsMetadataTableSnapshotTables) -> Result<Self> {
        Ok(Self {
            tables: crate::metadata_table_store::metadata_tables_from_snapshot(tables)?,
        })
    }
    pub fn apply(&mut self, effect: crate::DfsPreparedMetadataEffect) -> Result<()> {
        if let crate::DfsPreparedMetadataEffect::PutObject { object } = &effect {
            let transaction = crate::DfsNamespaceTransaction {
                content_reference_identity: None,
                contribution_digest: None,
                extension_expectations: Vec::new(),
                transaction_id: "candidate-object-projection".into(),
                file_system_id: object.file_system_id.clone(),
                objects: vec![object.clone()],
                versions: Vec::new(),
                file_version_pins: None,
                advisory_lock_states: Vec::new(),
                created_at: time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(
                    object.created_at_unix_nanos,
                ))
                .map_err(|e| MetadataBackendError::Serde(e.to_string()))?,
            };
            for op in crate::metadata_namespace_transaction::plan_namespace_transaction_table_ops(
                &self.tables,
                &transaction,
            ) {
                if !matches!(
                    op,
                    crate::metadata_records::DfsMetadataTableOp::PutNamespaceTransactionReceipt { .. }
                ) {
                    self.tables.apply_checked(op)?;
                }
            }
            return Ok(());
        }
        self.tables.apply_checked(effect.into_table_op())
    }
    pub fn apply_namespace(&mut self, transaction: &crate::DfsNamespaceTransaction) -> Result<()> {
        for op in crate::metadata_namespace_transaction::plan_namespace_transaction_table_ops(
            &self.tables,
            transaction,
        ) {
            self.tables.apply_checked(op)?;
        }
        Ok(())
    }
    pub fn apply_row(&mut self, row: crate::DfsMetadataCandidateRow) -> Result<()> {
        self.tables.apply_checked(row)
    }
    pub fn file_system(&self, id: &crate::DfsFileSystemId) -> Option<crate::DfsFileSystem> {
        self.tables.file_systems.get(id).cloned()
    }
    pub fn object(&self, id: &crate::DfsObjectId) -> Option<crate::DfsObjectMetadata> {
        self.tables.objects.get(id).cloned()
    }
    pub fn version(
        &self,
        id: &crate::DfsObjectId,
        version: &crate::DfsVersion,
    ) -> Option<crate::DfsObjectVersionRecord> {
        self.tables
            .versions
            .get(&(id.clone(), version.clone()))
            .cloned()
    }
    pub fn intent(
        &self,
        id: &crate::DfsObjectId,
        intent: &str,
    ) -> Option<crate::DfsWriteIntentRecord> {
        self.tables
            .intents
            .get(&(id.clone(), intent.to_owned()))
            .cloned()
    }
    pub fn receipt(
        &self,
        id: &crate::DfsObjectId,
        version: &crate::DfsVersion,
        node: &sinomo_core::node::NodeId,
    ) -> Option<crate::DfsReplicaReceiptRecord> {
        self.tables
            .receipts
            .get(&(id.clone(), version.clone(), node.clone()))
            .cloned()
    }
    pub fn job(&self, id: &str) -> Option<crate::DfsNamespaceJobRecord> {
        self.tables.namespace_jobs.get(id).cloned()
    }
    pub fn advisory(&self, id: &crate::DfsObjectId) -> Option<crate::DfsAdvisoryLockStateRecord> {
        self.tables.advisory_lock_states.get(id).cloned()
    }
    pub fn intents_for_version(
        &self,
        id: &crate::DfsObjectId,
        version: &crate::DfsVersion,
    ) -> Vec<crate::DfsWriteIntentRecord> {
        self.tables
            .intents
            .values()
            .filter(|intent| intent.object_id == *id && intent.target_version == *version)
            .cloned()
            .collect()
    }
    pub fn receipts_for_version(
        &self,
        id: &crate::DfsObjectId,
        version: &crate::DfsVersion,
    ) -> Vec<crate::DfsReplicaReceiptRecord> {
        self.tables
            .receipts
            .values()
            .filter(|receipt| {
                receipt.receipt.object_id == *id && receipt.receipt.committed_version == *version
            })
            .cloned()
            .collect()
    }
    pub fn snapshot(&self) -> DfsMetadataTableSnapshotTables {
        metadata_tables_snapshot(&self.tables)
    }
    pub fn restore_prepared(&mut self, record: crate::DfsPreparedOperationRecord) -> Result<()> {
        record.validate()?;
        if self
            .tables
            .prepared_operations_v7
            .insert(record.digest.operation_id.clone(), record)
            .is_some()
        {
            return Err(MetadataBackendError::Serde(
                "candidate repeats a current operation identity".into(),
            ));
        }
        self.tables.rebuild_prepared_operation_reservations()
    }
}
