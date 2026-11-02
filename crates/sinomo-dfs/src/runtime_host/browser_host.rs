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
    data_plane: Arc<dyn crate::DfsDataPlaneHost>,
}

impl RuntimeDfsBrowserServiceHost {
    pub(crate) fn new(
        paths: DfsRuntimePaths,
        runtime: tokio::runtime::Handle,
        events: ComponentEventClient,
        task_owner: Arc<DfsRuntimeHost>,
        data_owner: std::sync::Weak<crate::runtime_host::DfsDataOwnerLock>,
        metadata_authority: Option<Arc<DaemonDfsMetadataAuthorityGeneration>>,
        data_plane: Arc<dyn crate::DfsDataPlaneHost>,
    ) -> Self {
        Self {
            paths,
            runtime,
            events,
            task_owner,
            data_owner,
            metadata_authority,
            data_plane,
        }
    }
}

impl crate::DfsBrowserServiceHost for RuntimeDfsBrowserServiceHost {
    fn data_plane_host(&self) -> Option<Arc<dyn crate::DfsDataPlaneHost>> {
        Some(Arc::clone(&self.data_plane))
    }

    fn browser_open_runtime(&self) -> Option<tokio::runtime::Handle> {
        Some(self.runtime.clone())
    }

    fn browser_open_configuration_readiness(
        &self,
    ) -> Result<crate::DfsWriteConfigurationReadiness, sinomo_core::service::ServiceError> {
        self.metadata_authority.as_ref().ok_or_else(|| {
            sinomo_core::service::ServiceError::Service(
                "daemon write configuration owner is unavailable".into(),
            )
        })?.write_configuration_readiness()
            .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))
    }

    fn ready_browser_open_configuration<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<
        Output = Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, sinomo_core::service::ServiceError>,
    > + Send + 'a>> {
        Box::pin(async move {
            self.metadata_authority.as_ref().ok_or_else(|| {
                sinomo_core::service::ServiceError::Service(
                    "daemon write configuration owner is unavailable".into(),
                )
            })?.ready_write_configuration_projection_async().await
                .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))
        })
    }

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
            #[cfg(any(test, feature = "test-support"))]
            if crate::test_runtime::browser_content_preflight_is_denied(authority.as_ref()) {
                return Err(sinomo_core::service::ServiceError::Service(
                    "browser content readiness denied at injected preflight".into(),
                ));
            }
            let configuration = authority.ready_write_configuration_projection_async().await
                .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))?;
            if let Some(configuration) = configuration {
                super::placement_environment::refresh_live_content_owner_observations(
                    _data_dir, &self.task_owner, &configuration, authority,
                    tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                    #[cfg(test)] None,
                ).await;
            }
            Ok(())
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
    fn publication_execution_provider(
        &self,
        _paths: &[&str],
    ) -> Result<
        Option<Arc<crate::DfsPublicationExecutionProvider>>,
        sinomo_core::service::ServiceError,
    > {
        let authority = self.metadata_authority.as_ref().ok_or_else(|| {
            sinomo_core::service::ServiceError::Service("browser has no configuration owner".into())
        })?;
        let owner = self.data_owner.upgrade().ok_or_else(|| {
            sinomo_core::service::ServiceError::Service("DFS component is no longer active".into())
        })?;
        let data_dir = SinomoDataDir::new(
            daemon_data_dir(&self.paths)
                .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))?,
        );
        let scheduler = daemon_dfs_mutation_scheduler(&data_dir)
            .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))?;
        Ok(Some(
            crate::publication_guard::DfsPublicationExecutionProvider::from_runtime(
                data_dir.root(),
                owner,
                scheduler,
                Arc::clone(authority),
                self.runtime.clone(),
            ),
        ))
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
        data_dir: &SinomoDataDir,
        runtime: &tokio::runtime::Handle,
        traceparent: Option<&str>,
    ) {
        notify_daemon_dfs_maintenance_work(data_dir);
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
        notify_daemon_dfs_maintenance_work_for_paths(&self.paths);
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
