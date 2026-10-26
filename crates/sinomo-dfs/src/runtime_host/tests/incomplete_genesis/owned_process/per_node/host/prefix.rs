//! Read-only source selection and DATA projection of one real native round.
//! No backend/cache creation, source mutation or capability deserialization.
use super::*;
use crate::runtime_host::{NativeMetadataPrefixPhysicalOwner, message_host};
use crate::{DfsMetadataQuorumSyncTarget, DfsMetadataShardId};
use serde_json::{Value, json};

struct Selection {
    target: DfsMetadataQuorumSyncTarget,
    authority_generation: u64,
    physical: NativeMetadataPrefixPhysicalOwner,
}

fn refuse(detail: impl std::fmt::Display) -> NetworkError {
    NetworkError::delivery_failed(
        "select existing native PREFIX owner",
        NetworkDeliveryState::DefinitelyNotSent,
        detail.to_string(),
    )
}

fn check_deadline(deadline: Instant) -> Result<(), NetworkError> {
    if Instant::now() >= deadline {
        return Err(NetworkError::timeout(
            "native PREFIX selection",
            NetworkTimeoutPhase::Overall,
            OP_CAP,
            NetworkDeliveryState::DefinitelyNotSent,
        ));
    }
    Ok(())
}

fn physical_identity(path: &Path, directory: bool) -> Result<(u64, u64), NetworkError> {
    let file = open_retained_path(path, directory, None).map_err(refuse)?;
    let metadata = file.metadata().map_err(refuse)?;
    Ok((metadata.dev(), metadata.ino()))
}

fn select(
    data: &SinomoDataDir,
    source: &NodeId,
    cluster: &sinomo_core::Id,
    peers: &BTreeSet<NodeId>,
    deadline: Instant,
) -> Result<Selection, NetworkError> {
    check_deadline(deadline)?;
    let host = crate::runtime_host::registered_dfs_runtime_host(data)
        .map_err(refuse)?
        .ok_or_else(|| refuse("actual registered native host is unavailable"))?;
    if host.shutting_down.is_cancelled() {
        return Err(refuse("actual registered native host is shutting down"));
    }
    let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(data),
    )
    .map_err(refuse)?;
    if owner.physical_data_root() != data.root()
        || owner
            .selected_original_data_layout()
            .map_err(refuse)?
            .is_some()
    {
        return Err(refuse(
            "native PREFIX driver requires its actual default P0 DATA owner",
        ));
    }
    let identity = sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data))
        .map_err(refuse)?;
    if &identity.local_node_id != source || &identity.cluster_id != cluster {
        return Err(refuse(
            "actual native identity differs from this mounted driver",
        ));
    }
    let slot = message_host::daemon_dfs_message_service_cache()
        .try_existing_root(&data.root())
        .map_err(refuse)?
        .ok_or_else(|| refuse("no existing native source authority cache; no warming fallback"))?;
    let (authority, generation) = {
        let cache = slot
            .try_lock()
            .map_err(|_| refuse("existing native source authority cache is busy"))?;
        let entry = cache
            .as_ref()
            .ok_or_else(|| refuse("existing native source authority generation is unavailable"))?;
        if entry.signature.local_node_id != *source || entry.signature.cluster_id != *cluster {
            return Err(refuse(
                "existing authority cache belongs to another native identity",
            ));
        }
        (
            Arc::clone(&entry.metadata_authority_generation),
            entry.signature.metadata_authority_generation,
        )
    };
    message_host::daemon_dfs_require_retained_authority(&authority, generation)
        .map_err(|error| refuse(error.into_dfs_error()))?;
    let projection = authority
        .retained_complete_prefix_for_generation(generation)
        .ok_or_else(|| refuse("no existing complete current authority projection"))?;
    let shard = crate::metadata_shards_from_authority_projection(
        &projection,
        &crate::sifs_file_system_id(),
    )
    .map_err(refuse)?
    .into_iter()
    .find(|shard| {
        shard.primary_node_id == *source
            && shard.epoch > 0
            && shard.replica_node_ids.len() == 2
            && std::iter::once(shard.primary_node_id.clone())
                .chain(shard.replica_node_ids.iter().cloned())
                .collect::<BTreeSet<_>>()
                == *peers
            && peers.len() == 3
    })
    .ok_or_else(|| {
        refuse("no retained local-primary full-three SIFS assignment for this exact fixture")
    })?;
    let id = DfsMetadataShardId::new(&shard.shard_id);
    let pending_key =
        crate::metadata_shard_membership_handoff_key(&shard.file_system_id, &shard.shard_id);
    if projection
        .entries()
        .iter()
        .any(|entry| entry.key == pending_key)
    {
        return Err(refuse("selected native scope has a pending handoff record"));
    }
    let replica = crate::metadata_replica_context_from_authority_projection(
        &projection,
        source,
        &shard.file_system_id,
        &id,
    )
    .map_err(refuse)?
    .ok_or_else(|| refuse("selected scope lacks its retained native replica context"))?;
    if replica.shard != shard || replica.pending_membership_handoff.is_some() {
        return Err(refuse("selected scope changed or is pending handoff"));
    }
    let root = crate::local_dfs_metadata_shard_path(data, &id);
    if !owner.owns_backend_root(&root) {
        return Err(refuse(
            "selected shard is outside the actual physical DATA owner",
        ));
    }
    let physical = NativeMetadataPrefixPhysicalOwner {
        data_root: data.root(),
        data_identity: physical_identity(&data.root(), true)?,
        owner_identity: owner.storage_owner_file_identity().map_err(refuse)?,
        shard_root: root.clone(),
        shard_identity: physical_identity(&root, true)?,
    };
    if physical_identity(&data.root().join("daemon/data-owner.lock"), false)?
        != physical.owner_identity
    {
        return Err(refuse(
            "actual DATA-owner descriptor differs from its current pathname",
        ));
    }
    let prefix = {
        let _fence = crate::metadata_lock::try_metadata_inspection_lock(
            root.clone(),
            crate::metadata_cursor::journal_write_lock_path(&root),
        )
        .map_err(refuse)?
        .ok_or_else(|| {
            refuse("selected native shard is busy or lacks its existing inspection fence")
        })?;
        if crate::metadata_api::read_handoff_freeze_with_layout(
            &root.join("journal/membership-handoff-write-freeze.sobj"),
            None,
        )
        .map_err(refuse)?
        .is_some_and(|freeze| freeze.activated_epoch != Some(shard.epoch))
        {
            return Err(refuse(
                "selected native source is fenced at another or pending epoch",
            ));
        }
        let prefix = crate::metadata_journal::admission::current_owned_materialized_prefix(&root, &id)
            .map_err(refuse)?.ok_or_else(|| refuse("selected source has no existing owned certified materialized prefix; no recovery fallback"))?;
        if physical_identity(&root, true)? != physical.shard_identity
            || physical_identity(&data.root(), true)? != physical.data_identity
        {
            return Err(refuse(
                "actual source physical identity changed during selection",
            ));
        }
        prefix
    }; // No inspection/writer/cache/authority lock survives into the RPC.
    message_host::daemon_dfs_require_retained_authority(&authority, generation)
        .map_err(|error| refuse(error.into_dfs_error()))?;
    {
        let cache = slot
            .try_lock()
            .map_err(|_| refuse("native authority cache changed while selecting"))?;
        let entry = cache
            .as_ref()
            .ok_or_else(|| refuse("native authority cache disappeared while selecting"))?;
        if !Arc::ptr_eq(&authority, &entry.metadata_authority_generation)
            || entry.signature.metadata_authority_generation != generation
        {
            return Err(refuse(
                "native authority generation was replaced during selection",
            ));
        }
    }
    let current_host = crate::runtime_host::registered_dfs_runtime_host(data)
        .map_err(refuse)?
        .ok_or_else(|| refuse("native host disappeared while selecting"))?;
    let current_owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(data),
    )
    .map_err(refuse)?;
    if !Arc::ptr_eq(&host, &current_host)
        || !Arc::ptr_eq(&owner, &current_owner)
        || host.shutting_down.is_cancelled()
    {
        return Err(refuse(
            "actual native host or physical owner changed during selection",
        ));
    }
    check_deadline(deadline)?;
    Ok(Selection {
        target: DfsMetadataQuorumSyncTarget {
            file_system_id: shard.file_system_id.clone(),
            shard_id: id,
            primary_node_id: source.clone(),
            authority_epoch: Some(shard.epoch),
            replica_node_ids: shard.replica_node_ids.clone(),
            expected_shard: Some(shard),
            journal_sequence: prefix.journal_sequence,
            state_fingerprint: prefix.digest,
            barrier_id: format!("native-process-prefix-{}", sinomo_core::Id::generate()),
            checkpoint_snapshot: None,
        },
        authority_generation: generation,
        physical,
    })
}

fn physical_data(physical: &NativeMetadataPrefixPhysicalOwner) -> Value {
    json!({"data_root":physical.data_root,"data_identity":physical.data_identity,
        "owner_identity":physical.owner_identity,"shard_root":physical.shard_root,
        "shard_identity":physical.shard_identity})
}

pub(super) fn probe(
    request: &DriverRequest,
    deadline: Instant,
    root: &Path,
    runtime: &tokio::runtime::Handle,
    source: &NodeId,
    cluster: &sinomo_core::Id,
    peers: &BTreeSet<NodeId>,
    contacts: &OwnedContacts,
) -> Result<ProbeReply, NetworkError> {
    if request.target != *source {
        return Err(refuse(
            "PREFIX round is a local observation; peer targets come only from actual assignment",
        ));
    }
    if runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return Err(refuse(
            "native PREFIX capture requires its actual multi-thread daemon runtime",
        ));
    }
    let data = SinomoDataDir::new(root);
    let selected = select(&data, source, cluster, peers, deadline)?;
    let mut transport = crate::runtime_host::data_plane::daemon_dfs_native_prefix_contact_scoped_transport_for_test(
        source.clone(), selected.target.replica_node_ids.clone(), &data, runtime, contacts.clone(),
    ).map_err(refuse)?;
    check_deadline(deadline)?;
    transport.set_operation_deadline(Some(deadline));
    // Synchronous owned bridge: the actual registered adapter owns its
    // block_in_place and original RPC deadline. No detached blocking worker.
    let round = transport.capture_metadata_prefix_full_assignment_round(&selected.target)?;
    let mut all_bindings_valid = true;
    let observations = round.peer_outcomes().iter().map(|peer| {
        let binding = peer.native_return().map(|native| native.validate_binding(peer.peer(), peer.command(), root));
        let binding_valid = binding.as_ref().map(|result| result.is_ok());
        if binding_valid != Some(true) { all_bindings_valid = false; }
        json!({
            "peer":peer.peer(), "command":peer.command(), "status":format!("{:?}",peer.status()),
            "attempted":peer.attempted(), "response":peer.response(),
            "native_present":peer.native_return().is_some(),
            "native_attempt_id":peer.native_return().map(|native| native.attempt_id()),
            "binding_valid":binding_valid,
            "binding_error":binding.and_then(Result::err).map(|error|error.to_string()),
            "error":peer.error().map(|error|error.to_string()),
            "delivery_state":peer.delivery_state().map(|state|format!("{state:?}")),
            "unattempted_reason":peer.unattempted_reason(),
        })
    }).collect::<Vec<_>>();
    let common_source = round.common_source_observation().map(|common| json!({
        "assignment":common.assignment(), "prefix":common.prefix(),
        "source_node_id":common.source_node_id(), "physical":physical_data(common.physical_owner()),
    }));
    let expired = Instant::now() >= deadline;
    let mut value = json!({
        "classification":"historical-data-only", "family":"native-prefix-full-assignment",
        "round_id":round.round_id(), "complete":round.is_complete() && all_bindings_valid && !expired,
        "round_complete":round.is_complete(), "deadline_expired":expired,
        "finalization_error":round.finalization_error().map(|error|error.to_string()),
        "target":round.target(), "common_source":common_source, "peers":observations,
        "selection":{
            "authority_generation":selected.authority_generation,
            "assignment":selected.target.expected_shard,
            "prefix":{"journal_sequence":selected.target.journal_sequence,"digest":selected.target.state_fingerprint},
            "physical":physical_data(&selected.physical),
        },
    });
    let mut body =
        crate::metadata_io::encode_sobj(&value).map_err(|error| invalid(error.to_string()))?;
    if Instant::now() >= deadline {
        value["complete"] = json!(false);
        value["deadline_expired"] = json!(true);
        body =
            crate::metadata_io::encode_sobj(&value).map_err(|error| invalid(error.to_string()))?;
    }
    if body.len() > MAX_REPLY {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: body.len(),
            max_bytes: MAX_REPLY,
        });
    }
    Ok(ProbeReply {
        responder_node_id: None,
        route: DRIVER_ROUTE.into(),
        body,
    })
}
