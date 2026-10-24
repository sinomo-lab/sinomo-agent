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
            sinomo_daemon::start_cluster_kv_authoritative_watch_from_node_async(
                self.cluster_paths,
                &endpoint.node_id,
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
            sinomo_daemon::fetch_cluster_kv_local_entry_from_node_async(
                self.cluster_paths,
                &endpoint.node_id,
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
            sinomo_daemon::drain_cluster_kv_watch_from_node_async(
                self.cluster_paths,
                &endpoint.node_id,
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
            sinomo_daemon::cancel_cluster_kv_watch_from_node_async(
                self.cluster_paths,
                &endpoint.node_id,
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

#[cfg(test)]
mod native_watch_route_tests {
    use super::*;
    use sinomo_core::cluster::ClusterKvPathsExt;

    /// Runs only inside the lane's already initialized disposable node6, using
    /// its actual Main, membership, keys and remote authority. No test server
    /// manufactures a watch registration, read proof, drain or cancellation.
    #[tokio::test]
    #[ignore = "requires the explicit owned segmented Docker fixture"]
    async fn desired_watch_uses_real_main_route_when_authority_endpoint_is_unreachable() {
        assert_eq!(
            std::env::var("SINOMO_TEST_EXTERNAL_MAIN_WATCH").as_deref(),
            Ok("1")
        );
        let root = std::env::var_os("SINOMO_TEST_WATCH_DATA_DIR")
            .expect("explicit disposable DATA path; never use an ambient user root");
        let data = sinomo_core::local::SinomoDataDir::new(std::path::PathBuf::from(root));
        assert_eq!(
            std::env::var_os("SINOMO_DATA_DIR").as_deref(),
            Some(data.root().as_os_str()),
            "the direct control and routed callback must use the same actual DATA identity"
        );
        // Match the shadow's actual startup order: deny local voter ownership
        // before any state/routing read, then install its ordinary read model.
        // Otherwise this external fixture follows the Main reader path rather
        // than the materialized projection used by the host under test.
        let shadow_paths = DaemonPaths::for_shadow(&data);
        sinomo_daemon::deny_shadow_daemon_local_raft_access(&shadow_paths)
            .expect("install the real shadow's local Raft ownership denial");
        sinomo_core::cluster::install_kv_cluster_directory();
        let cluster_paths = ClusterPaths::new(&data);
        assert!(sinomo_core::kv::process_local_raft_access_is_denied(
            &cluster_paths.kv_paths().raft
        ));
        let (routing_version, members, endpoints, connectivity) =
            sinomo_core::network::cluster_directory_for(&cluster_paths)
                .routing_records_snapshot()
                .expect("the real shadow's observed routing projection")
                .into_parts();
        println!(
            "watch_route_shadow_projection version={routing_version:?} members={} endpoints={} live_connectivity={} local_raft_denied=true",
            members.len(),
            endpoints.len(),
            connectivity.len()
        );
        let state = sinomo_core::cluster::read_cluster_state(&cluster_paths).unwrap();
        let main = sinomo_daemon::read_daemon_status(&DaemonPaths::new(&data)).unwrap();
        assert_eq!(main.cluster.as_ref().unwrap().node_id, state.local_node_id);
        assert_eq!(main.cluster.as_ref().unwrap().process, "daemon-main");
        let runtime = tokio::runtime::Handle::current();
        let host = DesiredRolloutHost {
            paths: &shadow_paths,
            cluster_paths: &cluster_paths,
            runtime: &runtime,
        };
        let endpoint = host
            .watch_targets()
            .unwrap()
            .into_iter()
            .find(|endpoint| {
                endpoint.node_id != state.local_node_id
                    && endpoint.transport == sinomo_core::ClusterTransportKind::LanTcp
            })
            .expect("actual remote authority endpoint");
        let prefix = format!(
            "/test/dev-debug/watch-route/{}/",
            sinomo_core::Id::generate()
        );
        println!(
            "watch_route_fixture local={} target={} endpoint={} prefix={}",
            state.local_node_id, endpoint.node_id, endpoint.address, prefix
        );
        // The old endpoint-only call cannot use the segmented route. Require
        // an actual pre-delivery failure, never reinterpret uncertainty.
        let direct = sinomo_daemon::start_cluster_kv_authoritative_watch_from_endpoint_async(
            &endpoint,
            &prefix,
            sinomo_rollout::DESIRED_ROLLOUT_WATCH_START_TIMEOUT,
            &runtime,
        )
        .await;
        match direct {
            Err(sinomo_daemon::DaemonError::NetworkDelivery {
                address,
                detail,
                delivery_state,
            }) => {
                println!(
                    "watch_route_direct_refusal address={address} delivery={delivery_state:?} detail={detail}"
                );
                assert_eq!(
                    delivery_state,
                    sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent
                );
            }
            Ok(unexpected) => {
                let cancelled = host
                    .cancel_watch(
                        &endpoint,
                        &ClusterKvWatchCancelRequest {
                            watch_id: unexpected.value.watch_id,
                        },
                        sinomo_rollout::DESIRED_ROLLOUT_WATCH_CANCEL_TIMEOUT,
                    )
                    .await;
                panic!(
                    "fixture unexpectedly has direct authority reachability; original watch cancellation={cancelled:?}"
                );
            }
            Err(error) => {
                panic!("direct outcome is not the required pre-delivery refusal: {error}")
            }
        }
        let started = host
            .start_watch(
                &endpoint,
                &prefix,
                sinomo_rollout::DESIRED_ROLLOUT_WATCH_START_TIMEOUT,
            )
            .await
            .expect("same real authority is reachable through the node-addressed Main carrier");
        let watch_id = started.value.watch_id;
        println!(
            "watch_route_registered watch_id={watch_id:?} authority={:?}",
            started.authority
        );
        let verification = async {
            if started.value.prefix != prefix
                || started.value.next_revision != started.authority.revision.next()
            {
                return Err("watch prefix/barrier provenance changed".to_string());
            }
            let read = host
                .read_local_entry(
                    &endpoint,
                    &format!("{prefix}missing"),
                    sinomo_rollout::DESIRED_ROLLOUT_WATCH_READ_TIMEOUT,
                )
                .await
                .map_err(|error| error.to_string())?;
            if read.revision < started.authority.revision || read.value.is_some() {
                return Err("fresh exact-key watch catch-up is inconsistent".to_string());
            }
            let drain = host
                .drain_watch(
                    &endpoint,
                    &ClusterKvWatchDrainRequest {
                        watch_id,
                        wait_timeout_millis: sinomo_rollout::DESIRED_ROLLOUT_WATCH_DRAIN_WAIT
                            .as_millis()
                            .try_into()
                            .unwrap(),
                    },
                    sinomo_rollout::DESIRED_ROLLOUT_WATCH_DRAIN_TIMEOUT,
                )
                .await
                .map_err(|error| error.to_string())?;
            if drain.watch_id != watch_id || !drain.events.is_empty() {
                return Err(
                    "drain changed identity or invented events for the fresh prefix".to_string(),
                );
            }
            Ok::<(), String>(())
        }
        .await;
        // Always drain the exact registration we actually received, even when
        // the read/drain assertions failed. No alternate owner or fresh ID.
        let cancelled = host
            .cancel_watch(
                &endpoint,
                &ClusterKvWatchCancelRequest { watch_id },
                sinomo_rollout::DESIRED_ROLLOUT_WATCH_CANCEL_TIMEOUT,
            )
            .await
            .expect("cancel original actual registration");
        assert_eq!(cancelled.watch_id, watch_id);
        assert!(cancelled.cancelled);
        verification.expect("real watch read/drain provenance");
    }
}
