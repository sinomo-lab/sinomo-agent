//! Explicit fixture topology request through the existing native handoff owner.
//! Returned configuration/pending identities are DATA, never activation proof.
use super::*;
use serde_json::json;

pub(super) async fn request(
    request: &DriverRequest,
    deadline: Instant,
    root: &Path,
    runtime: &tokio::runtime::Handle,
    source: &NodeId,
    cluster: &sinomo_core::Id,
    peers: &BTreeSet<NodeId>,
) -> Result<ProbeReply, NetworkError> {
    if request.target != *source || peers.len() != 3 || !peers.contains(source) {
        return Err(invalid(
            "fixture topology request has a foreign local scope",
        ));
    }
    let data = SinomoDataDir::new(root);
    let paths = ClusterPaths::new(&data);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
    if state.local_node_id != *source || state.cluster_id != *cluster {
        return Err(invalid(
            "fixture topology request changed its native identity",
        ));
    }
    let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(&data),
    )
    .map_err(|e| invalid(e.to_string()))?;
    if owner.physical_data_root() != root
        || owner
            .selected_original_data_layout()
            .map_err(|e| invalid(e.to_string()))?
            .is_some()
    {
        return Err(invalid(
            "fixture topology request has another physical owner/layout",
        ));
    }
    let mut client =
        crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, runtime);
    let mut store = crate::DfsControlPlaneAsyncStore::new(&mut client);
    // Genuine configuration reads/transaction, not namespace metadata in KV.
    // The enclosing native driver owns the unchanged absolute five-second cap.
    let current = store
        .get_metadata_shard(&crate::sifs_file_system_id(), "root")
        .await
        .map_err(|e| invalid(e.to_string()))?;
    let value = if let Some(current) = current {
        if current.primary_node_id != *source {
            json!({"state":"NotPrimary","mutation_attempted":false,"current":current})
        } else {
            let members = std::iter::once(current.primary_node_id.clone())
                .chain(current.replica_node_ids.iter().cloned())
                .collect::<BTreeSet<_>>();
            if !members.is_subset(peers) {
                return Err(invalid(
                    "fixture refuses to remove a recorded foreign member",
                ));
            }
            let pending = store
                .get_metadata_shard_membership_handoff(&current.file_system_id, &current.shard_id)
                .await
                .map_err(|e| invalid(e.to_string()))?;
            if pending.is_some() || (current.epoch == 1 && current.replica_node_ids.is_empty()) {
                // The normal initial genesis/expansion and any in-flight handoff
                // keep their sole owner and original identity. Never force them.
                json!({"state":"Waiting","mutation_attempted":false,"current":current,"pending":pending})
            } else if &members == peers {
                json!({"state":"AlreadyConfigured","mutation_attempted":false,"current":current})
            } else {
                let mut desired = current.clone();
                desired.replica_node_ids =
                    peers.iter().filter(|n| **n != *source).cloned().collect();
                desired.epoch = current
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| invalid("fixture metadata epoch exhausted"))?;
                if Instant::now() >= deadline {
                    return Err(invalid("fixture topology deadline expired before request"));
                }
                // This only requests a guarded handoff. The native reconciler
                // must supply all actual freeze/prefix/participant/custody proof.
                // Any error/timeout is uncertain: the parent must not resubmit.
                let pending = store
                    .begin_metadata_shard_membership_handoff(&current, &desired)
                    .await
                    .map_err(|e| invalid(e.to_string()))?
                    .ok_or_else(|| invalid("fixture topology changed during guarded request"))?;
                json!({"state":"Requested","mutation_attempted":true,
                    "current":current,"desired":desired,"pending":pending})
            }
        }
    } else {
        json!({"state":"Waiting","mutation_attempted":false,"current":null})
    };
    let body = crate::metadata_io::encode_sobj(&value).map_err(|e| invalid(e.to_string()))?;
    Ok(ProbeReply {
        responder_node_id: None,
        route: DRIVER_ROUTE.into(),
        body,
    })
}


// Explicit startup activity, separate from the read-only PREFIX observation.
// Reuse the actual maintenance admission/service and existing full-assignment
// sync; do not turn the ordinary quorum policy into an all-member requirement.
pub(super) async fn prepare_prefix_baseline(
    request: &DriverRequest,
    deadline: Instant,
    root: &Path,
    runtime: &tokio::runtime::Handle,
    source: &NodeId,
    cluster: &sinomo_core::Id,
    peers: &BTreeSet<NodeId>,
) -> Result<ProbeReply, NetworkError> {
    use crate::runtime_host::message_host;
    if request.target != *source || peers.len() != 3 || !peers.contains(source) {
        return Err(invalid("full-assignment setup has a foreign fixture scope"));
    }
    let data = SinomoDataDir::new(root);
    let paths = ClusterPaths::new(&data);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
    if state.local_node_id != *source || state.cluster_id != *cluster {
        return Err(invalid("full-assignment setup native identity changed"));
    }
    let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(&data),
    ).map_err(|e| invalid(e.to_string()))?;
    if owner.physical_data_root() != root || owner.selected_original_data_layout()
        .map_err(|e| invalid(e.to_string()))?.is_some() {
        return Err(invalid("full-assignment setup requires its actual default DATA owner"));
    }
    let peer = message_host::read_daemon_dfs_message_peer_snapshot(&paths)
        .map_err(|e| invalid(e.to_string()))?.ok_or_else(|| invalid("native peer projection unavailable"))?;
    let preflight = message_host::warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
        &paths, &peer, runtime, tokio::time::Instant::from_std(deadline), None,
    ).await.map_err(|e| invalid(e.to_string()))?;
    let scheduler = message_host::daemon_dfs_mutation_scheduler(&data).map_err(|e| invalid(e.to_string()))?;
    let admission = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline),
        scheduler.admit(&crate::runtime_host::DfsMessageExecutionScope::GlobalExclusive))
        .await.map_err(|_| invalid("original setup deadline expired waiting for admission"))?
        .map_err(|e| invalid(e.to_string()))?;
    let preflight = preflight.for_admission(&admission);
    // No detached task: this driver retains its actual admission and DATA owner
    // through native bounded dispatch and lane restoration, including errors.
    let value = tokio::task::block_in_place(|| {
        let mut retained = message_host::take_daemon_dfs_message_mutation_service_for_maintenance(
            &data, &paths, &state, runtime, Some(&preflight),
        ).map_err(|e| invalid(e.to_string()))?;
        let result = (|| {
            let service = &mut retained.lane.service;
            let object = service.metadata_backend().ok_or_else(|| invalid("native setup backend absent"))?
                .get_object_by_path(&crate::sifs_file_system_id(), "/")
                .map_err(|e| invalid(e.to_string()))?.ok_or_else(|| invalid("native root genesis absent"))?;
            let target = service.capture_metadata_quorum_sync_target(&object).map_err(|e| invalid(e.to_string()))?;
            let members = std::iter::once(target.primary_node_id.clone())
                .chain(target.replica_node_ids.iter().cloned()).collect::<BTreeSet<_>>();
            if target.primary_node_id != *source || &members != peers {
                return Err(invalid("native setup target is not the actual complete fixture assignment"));
            }
            let backend = service.metadata_backend().ok_or_else(|| invalid("native setup backend absent"))?;
            let instance_frozen = backend.metadata_shard_instance_writes_frozen(&target.shard_id)
                .map_err(|e| invalid(e.to_string()))?;
            let freeze = backend.metadata_shard_write_freeze(&target.shard_id)
                .map_err(|e| invalid(e.to_string()))?;
            if instance_frozen || freeze.as_ref().is_some_and(|f| f.activated_epoch != target.authority_epoch) {
                // The normal native owner still owes its completed-generation
                // rebind. Observe only; do not sync, repair or finish it here.
                return Ok(json!({"state":"Waiting", "classification":"fixture-startup-activity-data-only",
                    "sync_attempted":false,"target":target,"instance_frozen":instance_frozen,
                    "actual_freeze":freeze}));
            }
            let remaining = deadline.checked_duration_since(Instant::now())
                .ok_or_else(|| invalid("original setup deadline expired before sync"))?;
            let previous = service.set_transport_operation_deadline(Some(deadline));
            let result = service.sync_metadata_shard_exact_prefix_full_assignment(&target, remaining);
            service.set_transport_operation_deadline(previous);
            let certificate = result.map_err(|e| invalid(e.to_string()))?;
            if certificate.acknowledged_node_ids.iter().cloned().collect::<BTreeSet<_>>() != members
                || Instant::now() >= deadline {
                return Err(invalid("full-assignment setup lacks every actual ACK within the original deadline"));
            }
            Ok(json!({"state":"FullAssignmentSynchronized", "classification":"fixture-startup-activity-data-only",
                "sync_attempted":true,"target":certificate.target,"acknowledged_node_ids":certificate.acknowledged_node_ids,
                "quorum_size":certificate.quorum_size,"does_not_confer_serving_authority":true}))
        })();
        let restored = message_host::restore_daemon_dfs_message_mutation_service_after_maintenance(
            &data, &paths, runtime, retained,
        ).map_err(|e| invalid(e.to_string()));
        let value = result?;
        if !restored? && value["state"] != "Waiting" {
            return Err(invalid("native setup lane changed before restoration"));
        }
        Ok(value)
    })?;
    drop(admission);
    drop(owner);
    Ok(ProbeReply { responder_node_id: None, route: DRIVER_ROUTE.into(),
        body: crate::metadata_io::encode_sobj(&value).map_err(|e| invalid(e.to_string()))? })
}
