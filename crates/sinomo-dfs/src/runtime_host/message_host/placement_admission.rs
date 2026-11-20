//! The Raft permit fences policy Apply. Only the local metadata effect needs
//! shard admission. The permit stays in the blocking worker if the RPC leaves.
use super::*;

pub(crate) struct PlacementDispatch {
    #[cfg(test)]
    hook: Option<(Arc<DaemonDfsPreDispatchTestHook>, DfsMessageCommand)>,
    cut: Option<crate::placement::AdmittedPlacement>,
    control: DaemonDfsControlPlaneKvClient,
    permit: Option<crate::control_plane::DfsPlacementAuthorityPermit>,
}

impl PlacementDispatch {
    pub(super) fn take_cut(&mut self) -> Option<crate::placement::AdmittedPlacement> {
        self.cut.take()
    }

    pub(super) fn release(mut self) -> Result<(), crate::DfsError> {
        if let Some(permit) = self.permit.as_mut() {
            #[cfg(test)]
            if let Some((hook, command)) = self.hook.as_ref() {
                hook(
                    DaemonDfsPreDispatchTestPoint::PlacementPermitQuorum { releasing: true },
                    Some(command),
                    None,
                )?;
            }
            let _span = tracing::trace_span!(target: "sinomo.perf", "dfs.repair.authority.release", stage = "dfs.repair.authority.release").entered();
            crate::placement::release_placement_permit(&mut self.control, permit)?;
        }
        Ok(())
    }
}

pub(super) async fn acquire(
    data: &SinomoDataDir,
    paths: &ClusterPaths,
    peer: &DaemonDfsMessagePeerSnapshot,
    message: &DfsMessageCommand,
    preflight: Option<&DaemonDfsMetadataAuthorityPreflight>,
    owner: &Arc<DaemonDataOwnerLock>,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<PlacementDispatch>, DaemonError> {
    let crate::DfsNetworkMessage::NamespaceRequest(command) = &message.routed_message.message
    else {
        return Ok(None);
    };
    let crate::DfsNamespaceOperation::ReconcilePlacement { path, .. } = &command.operation else {
        return Ok(None);
    };
    let Some(preflight) = preflight else {
        return Ok(None);
    };
    let Some(shards) = preflight
        .authority
        .configuration_snapshot()
        .metadata_shards(&command.file_system_id)?
    else {
        return Ok(None);
    };
    let shard = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
        &command.file_system_id,
        &shards,
        path,
    )?;
    if command.requesting_node_id != message.routed_message.source_node_id
        || command.file_system_id != crate::sifs_file_system_id()
        || !shard.is_some_and(|shard| {
            shard.primary_node_id == peer.local_node_id
                && command.authority_shards.as_slice() == [shard]
        })
    {
        // Normal receiver validation retains the typed redirect/rejection.
        return Ok(None);
    }
    #[cfg(test)]
    let hook = daemon_dfs_pre_dispatch_test_hook(data).map(|hook| (hook, message.clone()));
    let command = command.clone();
    let local = peer.local_node_id.clone();
    let paths = paths.clone();
    let handle = runtime.clone();
    let owner = Arc::clone(owner);
    let host = registered_dfs_runtime_host(data)?.ok_or_else(|| DaemonError::Parse {
        detail: "placement admission requires a registered DFS runtime".into(),
    })?;
    #[cfg(test)]
    let root = data.root();
    crate::runtime_publish::run_owned_dfs_blocking(&host, move || {
        let _owner = owner;
        let crate::DfsNamespaceOperation::ReconcilePlacement { job_id, path, .. } = &command.operation else { unreachable!() };
        let mut control = DaemonDfsControlPlaneKvClient::new_daemon_client_on_runtime(&paths, &handle);
        let _span = tracing::trace_span!(target: "sinomo.perf", "dfs.repair.authority.acquire", stage = "dfs.repair.authority.acquire", job_id, item_count = 1).entered();
        #[cfg(test)]
        if let Some((hook, command)) = hook.as_ref() {
            hook(DaemonDfsPreDispatchTestPoint::PlacementPermitQuorum {releasing:false}, Some(command), None)?;
        }
        let acquired = crate::placement::acquire_placement_permit(&mut control, &local, &command.file_system_id, job_id, crate::placement::PlacementPermitScope::Job(Some(path)),
            #[cfg(test)] crate::test_runtime::observe_placement_authority_call(&root, &_owner, job_id, Some(path)),
        );
        let (job, permit) = match acquired {
            Ok(Some((job, config, permit))) => (Ok(Some((job, config))), Some(permit)),
            Ok(None) => (Ok(None), None),
            Err(error) => (Err(error), None),
        };
        Ok::<_, crate::DfsError>(PlacementDispatch { #[cfg(test)] hook, cut: Some(crate::placement::AdmittedPlacement { command, job }), control, permit })
    }).await.map_err(|error| DaemonError::Network { address: "DFS placement admission".into(), detail: error.to_string() })?.map(Some).map_err(Into::into)
}
