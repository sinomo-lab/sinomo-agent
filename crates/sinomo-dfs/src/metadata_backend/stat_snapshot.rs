//! Stat borrows the ordinary validated materialized read view. It never waits
//! for a source's retained writer, replays its journal, or acquires authority.
use super::*;

#[derive(Clone, Debug)]
pub(super) struct StatReadSnapshot {
    pub(super) tables: Option<Arc<DfsMetadataTables>>,
    pub(super) prefix: DfsMetadataPrefixProof,
}

impl LocalSobjMetadataBackend {
    #[tracing::instrument(
        target = "sinomo.perf",
        name = "dfs.metadata.read_snapshot",
        skip_all,
        fields(stage = "dfs.metadata.read_snapshot", phase = "stat-view")
    )]
    pub(crate) fn pin_stat_read_snapshot(&mut self) -> Result<()> {
        let snapshot = metadata_read_snapshot::pin_current_tables_snapshot(
            &self.root,
            &self.tables,
            self.table_log_offset,
            self.materialized_through_sequence,
            &self.table_snapshot_fingerprint,
            &self.table_storage_generation,
            &self.prefix_proof,
            &self.read_snapshot_cache,
        )?;
        let (tables, prefix, sequence) = match snapshot {
            DfsMetadataTablesSnapshot::Borrowed(_) => (
                None,
                self.prefix_proof.clone(),
                self.materialized_through_sequence,
            ),
            DfsMetadataTablesSnapshot::Shared(tables) => {
                let cache = self
                    .read_snapshot_cache
                    .lock()
                    .map_err(|_| MetadataBackendError::Serde("stat read cache poisoned".into()))?;
                let cache = cache
                    .as_ref()
                    .filter(|c| Arc::ptr_eq(&c.tables, &tables))
                    .ok_or_else(|| {
                        MetadataBackendError::Serde(
                            "stat table/proof cache generation changed".into(),
                        )
                    })?;
                (
                    Some(tables),
                    cache.prefix_proof.clone(),
                    cache.materialized_through_sequence.unwrap_or(0),
                )
            }
        };
        let prefix = prefix
            .filter(|p| p.is_well_formed() && p.journal_sequence == sequence)
            .ok_or_else(|| {
                MetadataBackendError::Serde("stat view has no exact materialized prefix".into())
            })?;
        self.stat_read_snapshots
            .push(StatReadSnapshot { tables, prefix });
        Ok(())
    }

    pub(crate) fn unpin_stat_read_snapshot(&mut self) {
        debug_assert!(!self.stat_read_snapshots.is_empty());
        self.stat_read_snapshots.pop();
    }
}
