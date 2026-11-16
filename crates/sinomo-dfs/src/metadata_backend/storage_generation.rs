use super::*;

impl LocalSobjMetadataBackend {
    /// Complete only the generation belonging to the original native genesis.
    /// The bootstrap caller retains its existing native assignment authority;
    /// this kernel additionally proves the exact committed durable outcome.
    pub(crate) fn activate_native_genesis_storage_generation(
        &mut self,
        shard: &crate::DfsMetadataShardConfig,
    ) -> Result<()> {
        let _lock = self.metadata_write_lock()?;
        self.refresh_after_metadata_write_lock()?;
        let mut envelope = crate::storage_generation::read_generation(&self.root)?;
        if envelope.identity.file_system_id != shard.file_system_id
            || envelope.identity.shard_id != self.shard_id
            || self.shard_id.0 != shard.shard_id
            || !envelope.identity.is_native_genesis()
            || shard.epoch != 1
            || !shard.replica_node_ids.is_empty()
        {
            return Err(MetadataBackendError::Serde(
                "native genesis activation differs from staged storage scope".into(),
            ));
        }
        let operation_id = crate::default_cluster::genesis::genesis_operation_id(shard);
        let (digest, status, committed) = if let Some(record) =
            self.tables.prepared_operations_v7.get(&operation_id)
        {
            record.validate()?;
            (
                &record.digest,
                record.status,
                record.committed_entry.as_ref(),
            )
        } else if let Some(record) = self.tables.prepared_operation_evictions.get(&operation_id) {
            record.validate()?;
            (
                &record.digest,
                record.status,
                record.committed_entry.as_ref(),
            )
        } else {
            return Err(MetadataBackendError::Serde(
                "native genesis activation has no exact original outcome".into(),
            ));
        };
        if status != crate::DfsPreparedOperationStatus::Committed
            || digest.file_system_id != shard.file_system_id
            || digest.shard_id != self.shard_id
            || digest.owner_epoch != 1
            || committed.is_none_or(|entry| entry.sequence > self.materialized_through_sequence)
        {
            return Err(MetadataBackendError::Serde(
                "native genesis activation is not durably committed at this unit".into(),
            ));
        }
        let activation_digest = crate::storage_generation::native_genesis_activation_digest(
            &envelope.identity,
            &self.tables,
        )?;
        let active = crate::storage_generation::GenerationState::Active {
            activation_digest,
            standalone_seal: None,
        };
        if envelope.state == active {
            return Ok(());
        }
        if matches!(
            envelope.state,
            crate::storage_generation::GenerationState::Active { .. }
        ) && envelope.state != active
        {
            return Err(MetadataBackendError::Serde(
                "native genesis generation has a different active receipt".into(),
            ));
        }
        envelope.state = active;
        crate::storage_generation::write_generation(&self.root, &envelope)
    }
}
