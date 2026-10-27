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
