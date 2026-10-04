use super::*;

/// DFS-owned adapter for browser, placement, and administrative services.
/// It binds DFS semantics to generic component host mechanisms and contains no
/// daemon types.
pub(crate) struct RuntimeDfsBrowserServiceHost {
    paths: DfsRuntimePaths,
    runtime: tokio::runtime::Handle,
    events: ComponentEventClient,
    task_owner: Arc<DfsRuntimeHost>,
    data_owner: std::sync::Weak<crate::runtime_host::DfsDataOwnerLock>,
    metadata_authority: Option<Arc<DaemonDfsMetadataAuthorityGeneration>>,
}

impl RuntimeDfsBrowserServiceHost {
    pub(crate) fn new(
        paths: DfsRuntimePaths,
        runtime: tokio::runtime::Handle,
        events: ComponentEventClient,
        task_owner: Arc<DfsRuntimeHost>,
        data_owner: std::sync::Weak<crate::runtime_host::DfsDataOwnerLock>,
        metadata_authority: Option<Arc<DaemonDfsMetadataAuthorityGeneration>>,
    ) -> Self {
        Self {
            paths,
            runtime,
            events,
            task_owner,
            data_owner,
            metadata_authority,
        }
    }
}

impl crate::DfsBrowserServiceHost for RuntimeDfsBrowserServiceHost {
    fn ensure_write_configuration_ready<'a>(
        &'a self,
        _data_dir: &'a SinomoDataDir,
        _runtime: &'a tokio::runtime::Handle,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), sinomo_core::service::ServiceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let authority = self.metadata_authority.as_ref().ok_or_else(|| {
                sinomo_core::service::ServiceError::Service(
                    "daemon write configuration owner is unavailable".into(),
                )
            })?;
            authority
                .ensure_write_configuration_ready_async()
                .await
                .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))
        })
    }
    fn operation_guard(
        &self,
    ) -> Result<Option<Arc<dyn Send + Sync>>, sinomo_core::service::ServiceError> {
        let owner = self.data_owner.upgrade().ok_or_else(|| {
            sinomo_core::service::ServiceError::Service("DFS component is no longer active".into())
        })?;
        Ok(Some(owner))
    }
    fn control_plane_client(
        &self,
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Box<dyn crate::DfsControlPlaneKvClient>> {
        daemon_dfs_control_plane_client_on_runtime(cluster_paths, Some(runtime))
    }

    fn peer_transports(
        &self,
        data_dir: &SinomoDataDir,
        runtime: &tokio::runtime::Handle,
    ) -> (
        Arc<dyn crate::DfsPeerTransport>,
        Arc<dyn crate::AsyncDfsPeerTransport>,
    ) {
        daemon_dfs_peer_transports_on_runtime(data_dir, runtime)
    }

    fn placement_snapshot_cache(
        &self,
        data_dir: &SinomoDataDir,
        node_id: &NodeId,
    ) -> crate::DfsPlacementSnapshotCache {
        cached_daemon_dfs_placement_snapshot_cache(data_dir, node_id)
    }

    fn schedule_metadata_publish(
        &self,
        _data_dir: &SinomoDataDir,
        runtime: &tokio::runtime::Handle,
        traceparent: Option<&str>,
    ) {
        notify_daemon_dfs_maintenance_work();
        match schedule_daemon_dfs_metadata_publish_on_runtime(
            &self.paths,
            runtime,
            traceparent.map(ToString::to_string),
        ) {
            Ok(start) => tracing::trace!(
                target: "sinomo.dfs.perf",
                traceparent = %traceparent.unwrap_or_default(),
                ?start,
                "dfs-browser-metadata-publish-scheduled"
            ),
            Err(error) => tracing::warn!(
                traceparent = %traceparent.unwrap_or_default(),
                "DFS browser metadata publish could not be scheduled; maintenance will retry: {error}"
            ),
        }
    }

    fn notify_maintenance(&self) {
        notify_daemon_dfs_maintenance_work();
    }

    fn publish_namespace_change(
        &self,
        origin_node_id: sinomo_core::Id,
        operation: &str,
        paths: Vec<String>,
    ) {
        let events = self.events.clone();
        let operation = operation.to_string();
        let task = self.runtime.spawn(async move {
            publish_dfs_namespace_change(&events, origin_node_id, &operation, paths).await;
        });
        self.task_owner.own_task(task);
    }

    fn publish_placement_change(
        &self,
        origin_node_id: sinomo_core::Id,
        operation: &str,
        paths: Vec<String>,
    ) {
        let events = self.events.clone();
        let operation = operation.to_string();
        let task = self.runtime.spawn(async move {
            publish_dfs_placement_change(&events, origin_node_id, &operation, paths).await;
        });
        self.task_owner.own_task(task);
    }

    fn inspect_placement_authority(
        &self,
        runtime: &tokio::runtime::Handle,
        file_system_id: &crate::DfsFileSystemId,
    ) -> Result<Option<crate::DfsPlacementAuthorityPermitStatus>, sinomo_core::service::ServiceError>
    {
        inspect_dfs_placement_authority_permit(&self.paths, runtime, file_system_id)
            .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))
    }

    fn recover_removed_placement_authority(
        &self,
        runtime: &tokio::runtime::Handle,
        file_system_id: &crate::DfsFileSystemId,
        expected_recovery_token: &str,
        acknowledge_owner_stopped: bool,
    ) -> Result<crate::DfsPlacementAuthorityRecoveryOutcome, sinomo_core::service::ServiceError>
    {
        recover_removed_node_dfs_placement_authority_permit(
            &self.paths,
            runtime,
            file_system_id,
            expected_recovery_token,
            acknowledge_owner_stopped,
        )
        .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))
    }

    fn break_glass_recover_placement_authority(
        &self,
        runtime: &tokio::runtime::Handle,
        file_system_id: &crate::DfsFileSystemId,
        expected_recovery_token: &str,
        acknowledgement: &str,
    ) -> Result<crate::DfsPlacementAuthorityRecoveryOutcome, sinomo_core::service::ServiceError>
    {
        break_glass_recover_dfs_placement_authority_permit(
            &self.paths,
            runtime,
            file_system_id,
            expected_recovery_token,
            acknowledgement,
        )
        .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))
    }
}
