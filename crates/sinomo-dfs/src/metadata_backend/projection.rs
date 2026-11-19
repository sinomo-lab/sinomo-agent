//! The journal is sufficient persistence while one handle owns the live table
//! view. Independent handles keep a shared subscription on the physical root;
//! they need a published table checkpoint on the transition to shared access.
use super::*;

#[derive(Clone, Debug)]
pub(super) struct TableReader(Arc<ReaderLease>);

#[derive(Debug)]
struct ReaderLease {
    file: fs::File,
    lost: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
pub(super) struct PrivateProjection(Arc<ReaderLease>);

impl ReaderLease {
    fn restore(&self) -> std::io::Result<()> {
        loop {
            match fs2::FileExt::lock_shared(&self.file) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => {
                    if result.is_err() {
                        self.lost.store(true, std::sync::atomic::Ordering::Release);
                    }
                    return result;
                }
            }
        }
    }
}

impl Drop for PrivateProjection {
    fn drop(&mut self) {
        // Admission and read pins reject a lost subscription, including a
        // failed downgrade on a cancellation or unwinding path.
        if let Err(error) = self.0.restore() {
            tracing::error!(target: "sinomo.dfs.metadata", %error, "lost table projection subscription");
        }
    }
}

impl TableReader {
    pub(super) fn subscribe(root: &Path) -> Result<Option<Self>> {
        Self::register(root, false)
    }

    pub(super) fn register(root: &Path, try_writer: bool) -> Result<Option<Self>> {
        #[cfg(unix)]
        {
            let file = match fs::File::open(root) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(source) => {
                    return Err(MetadataBackendError::Io {
                        path: root.to_owned(),
                        source,
                    });
                }
            };
            let lease = ReaderLease {
                file,
                lost: false.into(),
            };
            let registration = if try_writer {
                fs2::FileExt::try_lock_shared(&lease.file)
            } else {
                lease.restore()
            };
            match registration {
                Err(error) if try_writer && error.kind() == std::io::ErrorKind::WouldBlock => {
                    Err(MetadataBackendError::StoragePreparationPending {
                        root: root.to_owned(),
                        processed_records: 0,
                    })
                }
                Ok(()) => Ok(Some(Self(Arc::new(lease)))),
                Err(e) if e.kind() == std::io::ErrorKind::Unsupported => Ok(None),
                Err(source) => Err(MetadataBackendError::Io {
                    path: root.to_owned(),
                    source,
                }),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (root, try_writer);
            Ok(None)
        }
    }

    fn private(&self, root: &Path) -> Result<Option<PrivateProjection>> {
        #[cfg(unix)]
        {
            if Arc::strong_count(&self.0) != 1 {
                return Ok(None);
            }
            use std::os::unix::fs::MetadataExt;
            let io = |source| MetadataBackendError::Io {
                path: root.to_owned(),
                source,
            };
            let opened = self.0.file.metadata().map_err(io)?;
            let current = fs::metadata(root).map_err(io)?;
            if (opened.dev(), opened.ino()) != (current.dev(), current.ino()) {
                return Err(MetadataBackendError::Serde(
                    "metadata projection root was replaced".into(),
                ));
            }
            match fs2::FileExt::try_lock_exclusive(&self.0.file) {
                Ok(()) => Ok(Some(PrivateProjection(Arc::clone(&self.0)))),
                Err(error) => {
                    // Conversion can drop a shared flock even when it fails.
                    self.0.restore().map_err(io)?;
                    if error.kind() == std::io::ErrorKind::WouldBlock {
                        Ok(None)
                    } else {
                        Err(io(error))
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = root;
            Ok(None)
        }
    }
}

/// A bounded write-behind interval. The WAL already owns these exact
/// decisions; retained operations are used only if a second handle needs
/// the ordinary incremental table stream before the next checkpoint.
#[derive(Clone, Debug, Default)]
pub(super) struct BufferedProjection {
    pub(super) bytes: u64,
    ops: Vec<DfsMetadataTableOp>,
    proof: Option<DfsMetadataPrefixProof>,
}
impl BufferedProjection {
    fn push(&mut self, op: DfsMetadataTableOp, bytes: u64, proof: &DfsMetadataPrefixProof) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.ops.push(op);
        self.proof = Some(proof.clone());
    }
}
impl LocalSobjMetadataBackend {
    pub(super) fn validate_projection_subscription(&self) -> Result<()> {
        if self
            .table_reader
            .as_ref()
            .is_some_and(|r| r.0.lost.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(MetadataBackendError::Serde(
                "metadata read projection lost its physical subscription".into(),
            ));
        }
        Ok(())
    }

    /// Called under the canonical writer, before either the journal or the
    /// live tables change. Checkpoint a private suffix before a shared delta
    /// could otherwise skip its dependencies.
    pub(super) fn begin_authored_projection(&mut self) -> Result<Option<PrivateProjection>> {
        // Only a serving handle with a real checkpoint can anchor a private
        // interval. Bootstrap and a repair-only image retain their owning path.
        if self.replica_repair_only || self.table_snapshot_fingerprint.is_none() {
            return Ok(None);
        }
        metadata_table_apply::validate_checkpoint_layout(
            &self.root,
            self.selected_original_data_layout
                .as_ref()
                .map(|(_, layout)| layout.as_ref()),
        )?;
        if self.buffered_projection.bytes >= DEFAULT_METADATA_TABLE_COMPACT_AFTER_BYTES {
            self.checkpoint_private_projection()?;
        }
        let guard = self
            .table_reader
            .as_ref()
            .map(|r| r.private(&self.root))
            .transpose()?
            .flatten();
        if guard.is_none() {
            self.publish_buffered_projection()?;
        }
        Ok(guard)
    }

    pub(super) fn checkpoint_private_projection(&mut self) -> Result<()> {
        if self.buffered_projection.bytes > 0 {
            let root = self.root.clone();
            metadata_table_apply::checkpoint_materialized_tables(
                &root,
                self.table_materialization_state(),
            )?;
            self.record_owned_metadata_materialization()?;
            self.buffered_projection = BufferedProjection::default();
        }
        Ok(())
    }

    /// Existing independent readers have already pinned their cut. Publish
    /// only the previously private interval, never a full database snapshot
    /// simply because a second reader joined.
    pub(super) fn publish_buffered_projection(&mut self) -> Result<()> {
        let Some(proof) = self.buffered_projection.proof.as_ref() else {
            return Ok(());
        };
        let batch = crate::metadata_records::DfsMetadataTableBatch {
            sequence: proof.journal_sequence,
            prefix_proof: Some(proof.clone()),
            ops: std::mem::take(&mut self.buffered_projection.ops),
        };
        let written =
            match crate::metadata_table_store::append_metadata_table_batch(&self.root, &batch) {
                Ok(written) => written,
                Err(error) => {
                    self.buffered_projection.ops = batch.ops;
                    return Err(error);
                }
            };
        self.table_log_offset += written;
        self.buffered_projection = BufferedProjection::default();
        self.table_storage_generation = Some(metadata_table_storage_generation(&self.root)?);
        Ok(())
    }

    pub(super) fn finish_private_projection(
        &mut self,
        seq: u64,
        record_bytes: u64,
        op: DfsMetadataTableOp,
    ) -> Result<()> {
        if self
            .prefix_proof
            .as_ref()
            .is_none_or(|p| !p.is_well_formed() || p.journal_sequence != seq)
            || self.materialized_through_sequence.checked_add(1) != Some(seq)
        {
            return Err(MetadataBackendError::Serde(
                "authored projection lost its exact journal cut".into(),
            ));
        }
        self.materialized_through_sequence = seq;
        self.buffered_projection.push(
            op,
            record_bytes,
            self.prefix_proof.as_ref().expect("validated native cut"),
        );
        self.record_owned_metadata_materialization()?;
        self.announce_repair_index_change();
        Ok(())
    }
}

/// Joining readers take the root subscription first. No existing writer can
/// extend a private suffix after that point. Only explicitly authored frames
/// qualify: a deferred replica ingest remains with its materialization owner.
pub(super) fn hydrate_private_projection(
    root: &Path,
    shard: &DfsMetadataShardId,
    state: &mut crate::metadata_tables::DfsMetadataTableState,
) -> Result<BufferedProjection> {
    let Some(mut proof) = state.prefix_proof.clone() else {
        return Ok(BufferedProjection::default());
    };
    if Some(proof.journal_sequence) != state.materialized_through_sequence
        || !proof.is_well_formed()
    {
        return Err(MetadataBackendError::Serde(
            "inspection has no coherent table prefix".into(),
        ));
    }
    let mut buffered = BufferedProjection::default();
    crate::metadata_records::with_prepared_payload_validation(|| -> Result<()> {
        crate::metadata_journal::visit_private_projection(root, proof.journal_sequence, |frame| {
            let op = crate::metadata_replay::table_op_for_native_journal_entry(frame.entry())?;
            proof = crate::metadata_journal::metadata_prefix_proof_after_exact_frame(
                &proof,
                frame.entry(),
                shard,
                frame.exact_bytes(),
            )?;
            state.tables.apply_checked(op.clone())?;
            buffered.push(op, frame.exact_bytes().len() as u64 + 12, &proof);
            Ok(())
        })?;
        Ok(())
    })?;
    state.materialized_through_sequence = Some(proof.journal_sequence);
    state.prefix_proof = Some(proof);
    Ok(buffered)
}

pub(super) fn read_inspection_state(
    root: &Path,
    shard: &DfsMetadataShardId,
) -> Result<(
    crate::metadata_tables::DfsMetadataTableState,
    BufferedProjection,
    Option<MetadataTableStorageGeneration>,
)> {
    for _ in 0..4 {
        let before = metadata_table_storage_generation(root)?;
        let mut state = read_metadata_table_state_lenient(root)?;
        let result = hydrate_private_projection(root, shard, &mut state);
        let after = metadata_table_storage_generation(root)?;
        let observed_len = after.active_log_len().unwrap_or(0);
        if before.is_compatible_with(&after)
            && before.active_log_len().unwrap_or(0) <= observed_len
            && state.table_log_offset <= observed_len
        {
            // The reader subscription prevents any new private suffix. An
            // append to the shared table log leaves the cut we read intact.
            let loaded_generation = (state.table_log_offset == observed_len).then_some(after);
            return Ok((state, result?, loaded_generation));
        }
    }
    Err(MetadataBackendError::Serde(
        "metadata checkpoint changed during cold read-view attachment".into(),
    ))
}
