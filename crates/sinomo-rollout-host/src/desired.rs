use std::time::Duration;

use crate::RolloutHostError as DaemonError;
use sinomo_api::{
    DaemonPaths, SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT,
    SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
};
use sinomo_core::ClusterEndpointRecord;
use sinomo_core::kv::{
    ClusterKvWatchCancelRequest, ClusterKvWatchDrainRequest, ClusterKvWatchRegistration, KvEntry,
};
use sinomo_core::network::ClusterPaths;
use sinomo_rollout::{
    ClusterRolloutBuildRequest, DesiredRolloutReactor, DesiredRolloutReactorFuture,
    DesiredRolloutReactorHost, DesiredRolloutReconcileDecision, SelfRolloutPaths,
    desired_rollout_reconcile_decision, read_rollout_status,
    record_desired_rollout_stale_handoff_failure_if_needed,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub(crate) async fn run_until_stopped(
    paths: DaemonPaths,
    cluster_paths: ClusterPaths,
    runtime: tokio::runtime::Handle,
    stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    let host = DesiredRolloutHost {
        paths: &paths,
        cluster_paths: &cluster_paths,
        runtime: &runtime,
    };
    DesiredRolloutReactor::new()
        .run_until_stopped_async(&host, stop_rx)
        .await;
}

struct DesiredRolloutHost<'a> {
    paths: &'a DaemonPaths,
    cluster_paths: &'a ClusterPaths,
    runtime: &'a tokio::runtime::Handle,
}

impl DesiredRolloutReactorHost for DesiredRolloutHost<'_> {
    type Error = DaemonError;

    fn protocol_error(&self, detail: String) -> Self::Error {
        DaemonError::Parse { detail }
    }

    fn unavailable_error(&self, detail: String) -> Self::Error {
        DaemonError::Network {
            address: "cluster rollout desired watch".to_string(),
            detail,
        }
    }

    fn watch_targets(&self) -> Result<Vec<ClusterEndpointRecord>, Self::Error> {
        sinomo_daemon::cluster_state_authority_watch_targets(self.cluster_paths).map_err(Into::into)
    }

    fn start_watch<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
        prefix: &'a str,
        timeout: Duration,
    ) -> DesiredRolloutReactorFuture<
        'a,
        sinomo_core::kv::KvAuthoritativeRead<ClusterKvWatchRegistration>,
        Self::Error,
    > {
        Box::pin(async move {
            sinomo_daemon::start_cluster_kv_authoritative_watch_from_endpoint_async(
                endpoint,
                prefix,
                timeout,
                self.runtime,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn read_local_entry<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
        key: &'a str,
        timeout: Duration,
    ) -> DesiredRolloutReactorFuture<'a, sinomo_core::kv::KvLocalRead<Option<KvEntry>>, Self::Error>
    {
        Box::pin(async move {
            sinomo_daemon::fetch_cluster_kv_local_entry_from_endpoint_async(
                endpoint,
                key,
                timeout,
                self.runtime,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn drain_watch<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchDrainRequest,
        timeout: Duration,
    ) -> DesiredRolloutReactorFuture<'a, sinomo_core::kv::ClusterKvWatchDrainResponse, Self::Error>
    {
        Box::pin(async move {
            sinomo_daemon::drain_cluster_kv_watch_from_endpoint_async(
                endpoint,
                request,
                timeout,
                self.runtime,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn cancel_watch<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchCancelRequest,
        timeout: Duration,
    ) -> DesiredRolloutReactorFuture<'a, sinomo_core::kv::ClusterKvWatchCancelResponse, Self::Error>
    {
        Box::pin(async move {
            sinomo_daemon::cancel_cluster_kv_watch_from_endpoint_async(
                endpoint,
                request,
                timeout,
                self.runtime,
            )
            .await
            .map_err(Into::into)
        })
    }

    fn reconcile_desired(
        &self,
        desired: Option<sinomo_core::ClusterRolloutDesiredRecord>,
    ) -> Result<(), Self::Error> {
        reconcile_desired_rollout_record(self.paths, desired)
    }
}

fn reconcile_desired_rollout_record(
    paths: &DaemonPaths,
    desired: Option<sinomo_core::ClusterRolloutDesiredRecord>,
) -> Result<(), DaemonError> {
    let Some(desired) = desired else {
        return Ok(());
    };
    if desired.state != "active"
        || desired.mode.as_deref() != Some("cluster-remote-build")
        || (desired.source_bundle.is_none() && desired.remote_repo.is_none())
    {
        return Ok(());
    }
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let current_status = read_rollout_status(&rollout_paths)?;
    let now = OffsetDateTime::now_utc();
    if let Some(status) = current_status.as_ref()
        && record_desired_rollout_stale_handoff_failure_if_needed(
            &rollout_paths,
            status,
            desired.mode.as_deref(),
            desired.requested_at.as_deref(),
            now,
        )?
    {
        return Ok(());
    }
    match desired_rollout_reconcile_decision(
        current_status.as_ref(),
        desired.mode.as_deref(),
        desired.requested_at.as_deref(),
        now,
    ) {
        DesiredRolloutReconcileDecision::Complete
        | DesiredRolloutReconcileDecision::TerminalFailure
        | DesiredRolloutReconcileDecision::Wait => return Ok(()),
        DesiredRolloutReconcileDecision::Retry => {}
    }

    let requested_at = desired.requested_at.clone().unwrap_or_else(|| {
        OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string())
    });
    let response = super::runtime::handle_daemon_rollout_build_request(
        paths,
        ClusterRolloutBuildRequest {
            source: desired
                .source
                .unwrap_or_else(|| "cluster desired rollout".to_string()),
            remote_repo: desired.remote_repo,
            source_bundle: desired.source_bundle,
            source_head: desired.source_head,
            force_source_overwrite: desired.force_source_overwrite,
            host: desired
                .host
                .unwrap_or_else(|| SINOMO_DAEMON_DEFAULT_HOST.to_string()),
            host_override: None,
            port: desired.port.unwrap_or(SINOMO_DAEMON_DEFAULT_PORT),
            shadow_port: desired
                .shadow_port
                .unwrap_or(SINOMO_SHADOW_DAEMON_DEFAULT_PORT),
            requested_at,
        },
    )?;
    tracing::warn!(
        status = response.status,
        detail = response.detail,
        "desired rollout reconciliation completed"
    );
    Ok(())
}
