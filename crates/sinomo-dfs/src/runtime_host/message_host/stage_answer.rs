use super::*;
use crate::DfsMetadataBackend;

/// Borrow the existing replica-transfer owner with only the shard certified by
/// its native intent. The ordinary global service keeps all active-stage and
/// pending-write fences; this does not create another transfer-state owner.
pub(super) async fn prepare_stage_answer(
    data_dir: &SinomoDataDir,
    peer: &DaemonDfsMessagePeerSnapshot,
    command: &DfsMessageCommand,
    preflight: Option<&DaemonDfsMetadataAuthorityPreflight>,
    data_owner: Arc<DaemonDataOwnerLock>,
    runtime: &tokio::runtime::Handle,
) -> Result<
    Option<(
        DfsMessageExecutionScope,
        ResidentLocalMetadataShardHandle,
        DaemonDfsMutationAdmission,
    )>,
    DaemonError,
> {
    let crate::DfsNetworkMessage::StageBlob(stage) = &command.routed_message.message else {
        return Ok(None);
    };
    let (Some(stamped), Some(preflight)) = (stage.authority_shard.as_ref(), preflight) else {
        return Ok(None);
    };
    if stamped.file_system_id != stage.file_system_id
        || stamped.primary_node_id != command.routed_message.source_node_id
        || stamped.primary_node_id == peer.local_node_id
        || !stamped.replica_node_ids.contains(&peer.local_node_id)
    {
        return Ok(None);
    }
    let Some(projection) = preflight
        .authority
        .retained_complete_prefix_for_generation(preflight.generation)
    else {
        return Ok(None);
    };
    let shards =
        daemon_dfs_metadata_shards_from_authority_projection(&projection, &stage.file_system_id)?;
    if !shards.contains(stamped) {
        return Ok(None);
    }
    let key = DaemonDfsMutationLaneKey {
        file_system_id: stamped.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(&stamped.shard_id),
    };
    let scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: key.file_system_id.clone(),
        shard_id: key.shard_id.clone(),
    };
    let scheduler = daemon_dfs_mutation_scheduler(data_dir)?;
    let admission = admit_daemon_dfs_peer_answer(&scheduler, &scope, true).await?;
    let data_dir = data_dir.clone();
    let peer = peer.clone();
    let stage = stage.clone();
    let stamped = stamped.clone();
    let preflight = preflight.clone();
    let runtime = runtime.clone();
    // Cancellation cannot release admission or data ownership while the native
    // proof is being read or already-accepted replica journal is materialized.
    let prepared = tokio::task::spawn_blocking(move || {
        let _owner = data_owner;
        let result = (|| -> Result<Option<ResidentLocalMetadataShardHandle>, crate::DfsError> {
            let Some(slot) = daemon_dfs_message_service_cache().existing_root(&data_dir.root())?
            else {
                return Ok(None);
            };
            let shard = {
                let cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
                let Some(entry) = cache.as_ref() else {
                    return Ok(None);
                };
                let Some(shard) = entry.exact_admission_handle(
                    &peer,
                    &preflight.authority,
                    preflight.generation,
                    &key,
                )?
                else {
                    return Ok(None);
                };
                if entry
                    .resident_metadata_backend
                    .as_ref()
                    .map(|backend| backend.shard_config(&key.shard_id))
                    .transpose()?
                    .as_ref()
                    != Some(&stamped)
                    || !shard.is_open()
                {
                    return Ok(None);
                }
                shard
            };
            let mut backend = shard.checkout()?;
            // Ingest may have ACKed the intent before its ordinary replay task
            // ran. Use that same resident owner's replay, never a second view.
            if backend.try_materialize_resident_journal()?.is_none() {
                return Ok(None);
            }
            let mut intent = backend.get_intent(&stage.object_id, &stage.intent_id)?;
            if intent.is_none() {
                let observed = backend.write_operation_admission_snapshot(
                    &key.shard_id,
                    &stage.file_system_id,
                    &stage.object_id,
                    &stage.intent_id,
                )?;
                intent = observed.intent;
                if intent.is_none() {
                    for prepared in observed.prepared {
                        let Some(original) = crate::service_routed_write::prepared_intent_for_write(
                            &prepared,
                            &stage.object_id,
                            &stage.intent_id,
                        ) else {
                            continue;
                        };
                        if prepared.status != crate::DfsPreparedOperationStatus::Prepared
                            || prepared.digest.file_system_id != key.file_system_id
                            || prepared.digest.shard_id != key.shard_id
                            || prepared.digest.owner_epoch != stamped.epoch
                        {
                            return Ok(None);
                        }
                        crate::service_routed_write::validate_retained_write_opening(
                            &prepared,
                            original,
                            &crate::intent::distributed_intent_from_record(original),
                            &stage.content_hash,
                        )?;
                        if intent.replace(original.clone()).is_some() {
                            return Ok(None);
                        }
                    }
                }
            }
            // A durable original prepare can precede replica visibility. It
            // supplies only the transfer's exact footprint, never publication
            // authority or a replacement applied-intent record.
            let Some(intent) = intent else {
                return Ok(None);
            };
            if !intent.publication.is_current()
                || intent.file_system_id != stage.file_system_id
                || intent.object_id != stage.object_id
                || intent.target_version != stage.version
                || intent.base_version != stage.expected_base_version
                || intent.owner_epoch != stamped.epoch
                || intent.content_hash.as_deref() != Some(stage.content_hash.as_str())
                || !intent
                    .desired_replica_node_ids
                    .contains(&peer.local_node_id)
                || !matches!(
                    intent.status,
                    crate::DfsWriteIntentStatus::Staging
                        | crate::DfsWriteIntentStatus::ReceiptsPending
                        | crate::DfsWriteIntentStatus::Committed
                )
            {
                return Ok(None);
            }
            let object = backend.get_object(&stage.object_id)?;
            let object = object.as_ref().or_else(|| {
                (intent.status != crate::DfsWriteIntentStatus::Committed)
                    .then_some(intent.object_metadata.as_ref())
                    .flatten()
            });
            let Some(object) = object else {
                return Ok(None);
            };
            if object.object_id != stage.object_id
                || object.file_system_id != stage.file_system_id
                || stage.object_metadata.as_ref().is_some_and(|supplied| {
                    supplied.object_id != stage.object_id
                        || supplied.file_system_id != stage.file_system_id
                        || supplied.path != object.path
                })
                || crate::control_plane::select_metadata_shard_config_for_path(
                    &object.file_system_id,
                    &shards,
                    &object.path,
                )?
                .as_ref()
                    != Some(&stamped)
                || preflight
                    .authority
                    .retained_complete_prefix_revision_for_generation(preflight.generation)
                    .is_none()
            {
                return Ok(None);
            }
            drop(backend);
            // First use still installs the one primary service. Its initial
            // exact borrow must not wait for a publisher on another shard.
            // Global hydration remains owned by its first global admission.
            let mut cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
            let Some(entry) = cache.as_mut() else {
                return Ok(None);
            };
            if entry.primary_service.is_none() {
                let resident = entry.resident_metadata_backend.clone().ok_or_else(|| {
                    crate::DfsError::Metadata {
                        message: "certified stage lost its resident metadata owner".into(),
                    }
                })?;
                let service = open_daemon_dfs_message_service_lane_with_content(
                    &data_dir,
                    &ClusterPaths::new(&data_dir),
                    &peer.local_node_id,
                    peer.peer_node_ids.clone(),
                    &runtime,
                    entry.content_store.clone(),
                    Some(projection),
                    Some(DaemonDfsResidentMetadataScope::ExactShard(shard.clone())),
                    entry.cross_shard_namespace_locks.clone(),
                )?;
                {
                    let mut lane = service.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
                    lane.resident_metadata_scope =
                        Some(DaemonDfsResidentMetadataScope::Global(resident));
                    lane.needs_global_hydration = true;
                }
                entry.primary_service = Some(service);
            }
            Ok(Some(shard))
        })();
        result.map(|shard| shard.map(|shard| (shard, admission)))
    })
    .await
    .map_err(|error| DaemonError::Network {
        address: "DFS stage admission".into(),
        detail: error.to_string(),
    })??;
    Ok(prepared.map(|(shard, admission)| (scope, shard, admission)))
}
