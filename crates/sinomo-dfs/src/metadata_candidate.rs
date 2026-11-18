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

/// Fully decoded candidate DATA: the identity and prepared reservations of one
/// unmounted current image. This has no writer or authority capability and
/// cannot be restored from an untrusted descriptor.
pub struct DfsMetadataCandidateInspection {
    descriptor: crate::DfsMetadataSnapshotImageDescriptor,
    identity: DfsMetadataCandidateIdentity,
    prepared_body_count: usize,
    prepared_reservations: Vec<crate::DfsPreparedOperationDigest>,
}
impl DfsMetadataCandidateInspection {
    pub fn capture(root: &Path) -> Result<Self> {
        let path = metadata_table_snapshot_path(root);
        let file = File::open(&path).map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
        let (state, descriptor) =
            crate::metadata_snapshot::decode_snapshot_image_with_descriptor(file, None)?;
        if crate::storage_generation::read_generation(root)?.identity != descriptor.generation {
            return Err(MetadataBackendError::Serde("candidate envelope differs from embedded snapshot generation".into()));
        }
        let identity = DfsMetadataCandidateIdentity {
            state_fingerprint: descriptor.semantic_state_fingerprint.clone(),
            prefix_proof: descriptor.prefix_proof.clone(),
            file_fingerprint: candidate_file_fingerprint(root)?,
        };
        let mut prepared_reservations = Vec::new();
        for record in state.tables.prepared_operations_v7.values() {
            // The full current snapshot decoder already validated this body.
            if record.status == crate::DfsPreparedOperationStatus::Prepared {
                prepared_reservations.push(record.digest.clone());
            }
        }
        Ok(Self {
            descriptor,
            identity,
            prepared_body_count: state.tables.prepared_operations_v7.len(),
            prepared_reservations,
        })
    }

    pub fn identity(&self) -> &DfsMetadataCandidateIdentity { &self.identity }

    /// Current-format DATA validation only. Native activation separately proves
    /// that every inherited reservation belongs to a retired authority epoch.
    fn validate_prepared_reservations(&self) -> Result<()> {
        if self.prepared_body_count != self.prepared_reservations.len()
            || self.prepared_reservations.iter().any(|digest|
                digest.file_system_id != self.descriptor.generation.file_system_id
                    || digest.shard_id != self.descriptor.generation.shard_id
                    || digest.owner_epoch == 0
                    || digest.prepare_sequence > self.identity.prefix_proof.journal_sequence) {
            return Err(MetadataBackendError::Serde("storage transition contains a terminal body or a foreign preparation".into()));
        }
        Ok(())
    }
    pub(crate) fn validate_retiring_reservations(&self, retiring: &crate::DfsMetadataShardConfig) -> Result<()> {
        self.validate_prepared_reservations()?;
        if self.descriptor.generation.file_system_id != retiring.file_system_id
            || self.descriptor.generation.shard_id.0 != retiring.shard_id
            || self.prepared_reservations.iter().any(|digest| digest.owner_epoch > retiring.epoch) {
            return Err(MetadataBackendError::Serde("inherited preparation is not fenced by the retiring native assignment".into()));
        }
        Ok(())
    }
}

/// An exclusively owned staging directory. It cannot overwrite a populated
/// metadata root: its staged generation must precede every image write.
pub struct DfsMetadataCandidateWriter {
    root: PathBuf,
    _lock: CandidateWriteLock,
}

struct CandidateWriteLock(File);

impl Drop for CandidateWriteLock {
    fn drop(&mut self) {
        // Closing one descriptor need not release its lock while an incidental
        // inherited/duplicated descriptor keeps the open file description alive.
        // The staging writer, not that descriptor, owns this custody lifetime.
        let _ = FileExt::unlock(&self.0);
    }
}

#[cfg(all(test, unix))]
mod lock_lifetime_tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn assert_busy(root: &Path) {
        assert!(matches!(
            DfsMetadataCandidateWriter::open(root),
            Err(MetadataBackendError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn candidate_writer_releases_its_lock_despite_an_inherited_description() {
        let root = sinomo_testdir::TestDir::new("candidate-lock-retirement");
        let writer = DfsMetadataCandidateWriter::open(root.path()).unwrap();
        // try_clone retains the same open file description, as an unrelated
        // fork child does before exec. It does not create a second writer.
        let inherited = writer._lock.0.try_clone().unwrap();
        let physical = inherited.metadata().unwrap();
        assert_busy(root.path());
        drop(writer);
        let successor = DfsMetadataCandidateWriter::open(root.path())
            .expect("writer drop ends custody even while an inherited descriptor lives");
        let current = successor._lock.0.metadata().unwrap();
        assert_eq!(
            (physical.dev(), physical.ino()),
            (current.dev(), current.ino())
        );
        assert_busy(root.path());
        drop(inherited);
        assert_busy(root.path());
        drop(successor);
        DfsMetadataCandidateWriter::open(root.path()).expect("successor released its own custody");
    }
}

impl DfsMetadataCandidateWriter {
    fn require_unsealed_generation(&self) -> Result<crate::DfsMetadataGenerationIdentity> {
        let envelope = crate::storage_generation::read_generation(&self.root)?;
        if !matches!(envelope.state, crate::storage_generation::GenerationState::Staged { seal: None }) {
            return Err(MetadataBackendError::Serde("candidate publication requires its unsealed staged generation".into()));
        }
        Ok(envelope.identity)
    }
    /// Create or resume a current hidden image under its immutable generation.
    /// This never changes an existing identity or grants serving authority.
    pub fn open_generation(
        root: impl AsRef<Path>,
        identity: crate::DfsMetadataGenerationIdentity,
    ) -> Result<Self> {
        identity.validate()?;
        let writer = Self::open(root.as_ref().to_owned())?;
        let path = writer.root.join("metadata-generation.sobj");
        if path.exists() {
            let current = crate::storage_generation::read_generation(&writer.root)?;
            if current.identity != identity || !matches!(current.state, crate::storage_generation::GenerationState::Staged { .. }) {
                return Err(MetadataBackendError::Serde("candidate generation differs from its existing staged identity".into()));
            }
        } else {
            if metadata_table_snapshot_path(&writer.root).exists() {
                return Err(MetadataBackendError::Serde("retained image has no current generation identity".into()));
            }
            crate::storage_generation::write_generation(&writer.root, &crate::storage_generation::GenerationEnvelope {
                format_version: crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
                identity,
                state: crate::storage_generation::GenerationState::Staged { seal: None },
            })?;
        }
        Ok(writer)
    }

    /// Bind the complete hidden image to the exact cut that native activation
    /// must subsequently inspect. Sealing does not authorize installation.
    pub fn seal_storage_generation(
        &self,
        expected: &DfsMetadataCandidateIdentity,
    ) -> Result<crate::DfsMetadataGenerationSeal> {
        let inspection = DfsMetadataCandidateInspection::capture(&self.root)?;
        if inspection.identity() != expected {
            return Err(MetadataBackendError::Serde("candidate changed before generation sealing".into()));
        }
        let mut envelope = crate::storage_generation::read_generation(&self.root)?;
        match envelope.identity.origin {
            crate::DfsMetadataGenerationOrigin::StorageTransition { .. } => inspection.validate_prepared_reservations()?,
            _ => {}
        }
        let crate::storage_generation::GenerationState::Staged { seal } = &mut envelope.state else {
            return Err(MetadataBackendError::Serde("active metadata generation cannot be resealed".into()));
        };
        let expected = crate::DfsMetadataGenerationSeal {
            prefix_proof: expected.prefix_proof.clone(),
            state_fingerprint: expected.state_fingerprint.clone(),
            file_fingerprint: expected.file_fingerprint.clone(),
        };
        if seal.as_ref().is_some_and(|old| old != &expected) {
            return Err(MetadataBackendError::Serde("sealed metadata generation changed".into()));
        }
        *seal = Some(expected.clone());
        crate::storage_generation::write_generation(&self.root, &envelope)?;
        Ok(expected)
    }
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|source| MetadataBackendError::Io {
            path: root.clone(),
            source,
        })?;
        let generation = root.join("metadata-generation.sobj");
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
        // Install the guard immediately after acquisition so a rejected identity
        // or failed initialization also ends the acquired custody explicitly.
        let lock = CandidateWriteLock(lock);
        if generation.exists() {
            if !matches!(crate::storage_generation::read_generation(&root)?.state,
                crate::storage_generation::GenerationState::Staged { .. }) {
                return Err(MetadataBackendError::Serde(
                    "candidate writer cannot open an active generation".into(),
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
        }
        Ok(Self { root, _lock: lock })
    }
    pub fn write(
        &self,
        tables: DfsMetadataTableSnapshotTables,
        proof: DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataCandidateIdentity> {
        self.require_unsealed_generation()?;
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
        let generation = self.require_unsealed_generation()?;
        if generation.shard_id != entry.shard_id {
            return Err(MetadataBackendError::Serde("candidate frame belongs to another generation scope".into()));
        }
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
        // The exact frame is already durable, including a new segment's name.
        // Use the ordinary journal's reconstructible head hint: recovery checks
        // the actual tail, and commit above still requires its durable prepare.
        crate::metadata_cursor::write_journal_head_hint(
            &self.root,
            entry.sequence.saturating_add(1),
        )?;
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
    crate::DfsMetadataSnapshotImage::capture_existing(root, file)
}

/// Stage actual native image DATA with the ordinary candidate lock/parser and
/// row invariants. Activation remains the existing certified native operation.
pub fn write_metadata_candidate_image(
    root: &Path,
    image: &crate::DfsMetadataSnapshotImage,
    fs: &crate::DfsFileSystemId,
) -> Result<DfsMetadataCandidateIdentity> {
    Ok(write_metadata_candidate_image_inspected(root, image, fs)?.identity)
}

/// Return the full decoder's private inspection of the completed copy. A
/// receiver may retain this immutable DATA result, but every reuse must verify
/// the entire actual candidate stream and bytes through `verify`.
pub fn write_metadata_candidate_image_inspected(
    root: &Path,
    image: &crate::DfsMetadataSnapshotImage,
    fs: &crate::DfsFileSystemId,
) -> Result<DfsMetadataCandidateInspection> {
    image.validate_file_system(fs)?;
    if image.generation.file_system_id != *fs {
        return Err(MetadataBackendError::Serde("candidate image generation names another filesystem".into()));
    }
    let writer = DfsMetadataCandidateWriter::open_generation(root, image.generation.clone())?;
    if matches!(crate::storage_generation::read_generation(root)?.state,
        crate::storage_generation::GenerationState::Staged { seal: Some(_) }) {
        let inspected = DfsMetadataCandidateInspection::capture(root)?;
        if inspected.descriptor != *image.descriptor() {
            return Err(MetadataBackendError::Serde("sealed received image differs from its exact replay".into()));
        }
        writer.seal_storage_generation(inspected.identity())?;
        return Ok(inspected);
    }
    writer.require_unsealed_generation()?;
    image.install_candidate_copy(&writer.root)?;
    let inspected = DfsMetadataCandidateInspection::capture(root)?;
    writer.seal_storage_generation(inspected.identity())?;
    Ok(inspected)
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
    Ok(DfsMetadataCandidateInspection::capture(root)?.identity)
}

fn candidate_file_fingerprint(root: &Path) -> Result<String> {
    use std::io::Read;
    let path = metadata_table_snapshot_path(root);
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
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
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
    /// Reconstruct one exact native source event as DATA. The caller retains
    /// source custody and validates the contiguous original prefix separately.
    /// This shares ordinary replay (including Prepare receipts), never author
    /// admission or normalization of historical payloads/digests.
    pub fn replay_native_journal_entry(&mut self, entry: &crate::DfsMetadataJournalEntry) -> Result<()> {
        let op = crate::metadata_replay::table_op_for_native_journal_entry(entry)?;
        self.tables.apply_checked(op)
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

/// Construct current staged DATA for physical codec/cursor tests. This never
/// activates a generation, qualifies native authority, or opens a serving backend.
#[cfg(test)]
pub(crate) fn stage_current_test_generation(root: &Path, shard: &str) {
    drop(DfsMetadataCandidateWriter::open_generation(root, crate::DfsMetadataGenerationIdentity {
        file_system_id: crate::DfsFileSystemId::from_legacy_named("physical-data-fixture").unwrap(),
        shard_id: crate::DfsMetadataShardId::new(shard),
        generation: "physical-reader-generation".into(),
        origin: crate::DfsMetadataGenerationOrigin::StorageTransition {
            transition_id: "physical-reader-candidate".into(),
        },
    }).unwrap());
}
