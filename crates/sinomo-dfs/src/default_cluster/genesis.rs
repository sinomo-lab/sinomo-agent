use super::*;
use crate::{
    DfsPreparedMetadataEffect, DfsPreparedOperationPayload, DfsPreparedOperationScope,
    DfsPreparedOperationStatus,
};

pub(super) fn genesis_operation_id(shard: &DfsMetadataShardConfig) -> String {
    format!("genesis:{}:{}:1", shard.file_system_id, shard.shard_id)
}

// GC collects the immutable body but retains the same unified operation's
// exact terminal digest and commit reference. Neither representation authorizes
// publication; cold bootstrap only verifies the original genesis outcome.
pub(super) fn native_genesis_committed(
    backend: &crate::LocalSobjMetadataBackend,
    shard: &DfsMetadataShardConfig,
) -> Result<bool, DfsError> {
    let operation_id = genesis_operation_id(shard);
    let tables = backend.current_tables_snapshot()?;
    let (digest, status) = if let Some(record) = tables.prepared_operations_v7.get(&operation_id) {
        record.validate()?;
        (&record.digest, record.status)
    } else if let Some(witness) = tables.prepared_operation_evictions.get(&operation_id) {
        witness.validate()?;
        (&witness.digest, witness.status)
    } else {
        return Err(DfsError::Metadata {
            message: "initial native assignment has no retained genesis; certified import/repair is required".into(),
        });
    };
    if shard.epoch != 1
        || !shard.replica_node_ids.is_empty()
        || digest.file_system_id != shard.file_system_id
        || digest.shard_id != DfsMetadataShardId::new(shard.shard_id.clone())
        || backend.shard_id() != &digest.shard_id
        || digest.owner_epoch != shard.epoch
        || digest.operation_id != operation_id
    {
        return Err(DfsError::Metadata {
            message: "retained genesis outcome differs from the initial native assignment".into(),
        });
    }
    Ok(status == DfsPreparedOperationStatus::Committed
        && backend.committed_prepared_operation(digest)?.is_some())
}

impl DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    pub(super) async fn prepare_native_genesis(
        &mut self,
        file_system: &DfsFileSystem,
        shards: &[DfsMetadataShardConfig],
        bootstrap_nodes: Vec<NodeId>,
        fallback_node: NodeId,
    ) -> Result<(), DfsError> {
        let mut missing = Vec::new();
        for shard in shards {
            if self
                .control_plane
                .get_async(&metadata_shard_key(&shard.file_system_id, &shard.shard_id))
                .await?
                .is_none()
            {
                if shard.primary_node_id != self.local_node_id
                    || shard.epoch != 1
                    || !shard.replica_node_ids.is_empty()
                {
                    return Err(DfsError::Metadata {
                        message:
                            "only the deterministic native singleton owner may prepare genesis"
                                .into(),
                    });
                }
                missing.push(shard.clone());
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let mut backend = open_local_sharded_metadata_backend_for_replica_repair(
            default_dfs_metadata_shards_root(&self.data_dir),
            shards.to_vec(),
            &self.local_node_id,
        )?;
        for shard in missing {
            let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            let physical = backend.shard_backend_mut(&shard_id)?;
            let operation_id = genesis_operation_id(&shard);
            if let Some(record) = physical.prepared_operation_record(&operation_id)? {
                record.validate()?;
                if record.digest.file_system_id != shard.file_system_id
                    || record.digest.shard_id != shard_id
                    || record.digest.owner_epoch != 1
                    || record.status != DfsPreparedOperationStatus::Prepared
                {
                    return Err(DfsError::Metadata {
                        message: "retained genesis differs from the missing native assignment"
                            .into(),
                    });
                }
                continue;
            }
            let status = physical.metadata_shard_statuses()?;
            let [status] = status.as_slice() else {
                return Err(DfsError::Metadata {
                    message: "genesis requires one actual physical shard".into(),
                });
            };
            if status.journal_last_sequence != 0
                || !physical.list_file_systems()?.is_empty()
                || !physical.list_prepared_operations()?.is_empty()
                || physical.metadata_authority_ack_floor(&shard_id)?.is_some()
                || physical.metadata_prefix_ack_floor(&shard_id)?.is_some()
            {
                return Err(DfsError::Metadata { message: "retained metadata cannot be re-seeded as native genesis; import or repair its certified history".into() });
            }
            let objects = crate::distributed_sifs_bootstrap_objects_for_nodes(bootstrap_nodes.clone(), fallback_node.clone())
                .into_iter().filter_map(|object| {
                    match crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(&object.file_system_id, shards, &object.path) {
                        Ok(Some(selected)) if selected.shard_id == shard.shard_id => Some(Ok(object)),
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    }
                }).collect::<Result<Vec<_>, sinomo_core::kv::KvError>>()?;
            let versions = objects
                .iter()
                .map(|object| crate::object_version_record(object, None))
                .collect();
            let expected_objects = objects
                .iter()
                .map(|object| crate::DfsNamespaceObjectVersionExpectation {
                    object_id: object.object_id.clone(),
                    expected_version: None,
                })
                .collect();
            let expected_paths = objects
                .iter()
                .map(|object| crate::DfsNamespacePathOccupantExpectation {
                    path: object.path.clone(),
                    expected_object_id: None,
                })
                .collect();
            let mut payload = DfsPreparedOperationPayload::for_namespace_transaction(
                DfsPreparedOperationScope {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id,
                    owner_epoch: 1,
                    operation_id: operation_id.clone(),
                },
                expected_objects,
                expected_paths,
                crate::DfsNamespaceTransaction {
                    transaction_id: operation_id,
                    file_system_id: shard.file_system_id.clone(),
                    objects,
                    versions,
                    advisory_lock_states: Vec::new(),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    content_reference_identity: None,
                    extension_expectations: Vec::new(),
                    contribution_digest: None,
                    file_version_pins: None,
                },
                Vec::new(),
                None,
                None,
                Vec::new(),
                None,
                None,
            )?;
            payload
                .metadata_effects
                .push(DfsPreparedMetadataEffect::PutFileSystem {
                    file_system: file_system.clone(),
                });
            // This hidden body precedes the native assignment CAS. No guard is
            // minted and no namespace row becomes visible at this boundary.
            physical.prepare_operation(payload, None)?;
        }
        self.genesis_backend = Some(backend);
        Ok(())
    }

    pub(super) fn publish_current_bootstrap(
        &mut self,
        publication_provider: Arc<crate::DfsPublicationExecutionProvider>,
        file_system: &DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        bootstrap_nodes: Vec<NodeId>,
        fallback_node: NodeId,
        evidence: Vec<(
            Option<DfsMetadataShardMembershipHandoff>,
            Option<DfsMetadataShardMembershipHandoffProof>,
        )>,
    ) -> Result<(), DfsError> {
        let runtime = tokio::runtime::Handle::current();
        let transport = crate::runtime_host::daemon_dfs_node_transport_on_runtime(
            self.local_node_id.clone(),
            shards
                .iter()
                .flat_map(|shard| shard.replica_node_ids.iter().cloned()),
            &self.data_dir,
            &runtime,
        );
        let content = crate::LocalContentStore::new(self.data_dir.dfs_dir().join("content"))?;
        let service = crate::NetworkedDfsService::new(transport, content);
        let mut service = if let Some(backend) = self.genesis_backend.take() {
            service.with_metadata_backend_lazy(backend)?
        } else if let Some(resident) = self.resident_metadata_backend.as_ref().filter(|resident| {
            resident
                .matches_topology(&default_dfs_metadata_shards_root(&self.data_dir), &shards)
                .unwrap_or(false)
        }) {
            service.with_resident_sharded_metadata_backend_lazy(resident.checkout_global()?)?
        } else {
            service.with_metadata_backend_lazy(
                open_local_sharded_metadata_backend_for_replica_repair(
                    default_dfs_metadata_shards_root(&self.data_dir),
                    shards.clone(),
                    &self.local_node_id,
                )?,
            )?
        };
        service = service.with_boxed_control_plane(
            crate::runtime_host::daemon_dfs_control_plane_client_on_runtime(
                &self.cluster_paths,
                Some(&runtime),
            )
            .ok_or_else(|| DfsError::Metadata {
                message: "native bootstrap has no actual cluster authority client".into(),
            })?,
        );
        service
            .publication_provider
            .retain_runtime_provider(publication_provider);
        let _execution = service
            .publication_execution
            .install(Arc::clone(&self.publication_owners));
        service.metadata.add_file_system(file_system.clone());
        for (shard, (pending, proof)) in shards.iter().zip(&evidence) {
            if shard.primary_node_id != self.local_node_id || pending.is_some() {
                continue;
            }
            #[cfg(test)]
            if proof.is_some()
                && let Some(hook) =
                    crate::runtime_host::daemon_dfs_pre_dispatch_test_hook(&self.data_dir)
            {
                hook(crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeCompletedPrimaryRebind {
                    shard_id: &crate::DfsMetadataShardId::new(&shard.shard_id),
                }, None, None)?;
            }
            rebind_completed_metadata_shard_membership_handoff(
                service
                    .metadata_backend
                    .as_mut()
                    .expect("bootstrap backend")
                    .as_mut(),
                &self.local_node_id,
                shard,
                pending.as_ref(),
                proof.as_ref(),
            )?;
            let operation_id = genesis_operation_id(shard);
            let retained = service
                .metadata_backend
                .as_ref()
                .expect("bootstrap backend")
                .prepared_operation_record(&operation_id)?;
            if let Some(record) = retained {
                if record.status == DfsPreparedOperationStatus::Prepared {
                    // Only the exact initial singleton can finish its retained
                    // genesis. New owners recover through normal certified handoff.
                    if shard.epoch != record.digest.owner_epoch
                        || !shard.replica_node_ids.is_empty()
                    {
                        return Err(DfsError::Metadata {
                            message: "unfinished genesis cannot be expanded or rebound".into(),
                        });
                    }
                    service.commit_stored_prepared_operation(&record.digest)?;
                }
            } else if service
                .metadata_backend
                .as_ref()
                .expect("bootstrap backend")
                .get_file_system(&shard.file_system_id)?
                .is_none()
            {
                return Err(DfsError::Metadata { message: "existing assignment has no retained genesis or certified metadata image; import/catch-up required".into() });
            }
        }
        for object in
            crate::distributed_sifs_bootstrap_objects_for_nodes(bootstrap_nodes, fallback_node)
        {
            let selected = crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(&object.file_system_id, &shards, &object.path)?.ok_or_else(|| DfsError::Metadata { message: "current bootstrap object has no exact assignment".into() })?;
            if selected.primary_node_id != self.local_node_id
                || evidence.iter().zip(&shards).any(|((pending, _), shard)| {
                    shard.shard_id == selected.shard_id && pending.is_some()
                })
            {
                continue;
            }
            if service
                .metadata_backend
                .as_ref()
                .expect("bootstrap backend")
                .get_object_by_path(&object.file_system_id, &object.path)?
                .is_none()
            {
                service.add_object_metadata(object)?;
            }
        }
        Ok(())
    }
}
