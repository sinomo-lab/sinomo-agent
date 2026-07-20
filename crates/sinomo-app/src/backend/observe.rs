//! Typed live-observation adapter for the graphical Cluster dashboard.
//!
//! The app always enters observation through its connected daemon. Local reads
//! use that daemon's ordinary observe route; remote reads ask the same daemon to
//! route the object RPC through its persistent cluster transports.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::node::NodeId;
use sinomo_runtime::observe::{
    OBSERVE_METRICS_ROUTE, OBSERVE_RPC_CATEGORY, ObserveMetricsRequest, ObserveMetricsResponse,
};
use time::OffsetDateTime;

use crate::backend::{Backend, Connection};

const OBSERVE_DASHBOARD_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const OBSERVE_DASHBOARD_MAX_WORKERS: usize = 8;
const OBSERVE_DASHBOARD_MAX_TARGETS: usize = 32;
const OBSERVE_DASHBOARD_MAX_METRICS_PER_TARGET: usize = 4_096;

#[derive(Clone, Debug)]
pub struct ObserveTargetSample {
    pub node_id: String,
    pub response: Result<ObserveMetricsResponse, String>,
}

#[derive(Clone, Debug)]
pub struct ObserveSampleBatch {
    pub generation: u64,
    pub requested_at: OffsetDateTime,
    pub received_at: OffsetDateTime,
    pub targets: Vec<ObserveTargetSample>,
}

impl Backend {
    /// Collect one bounded dashboard sample from every authorized cluster
    /// member. A daemon without cluster identity is represented by a single
    /// synthetic `local` target.
    pub fn observe_cluster_metrics(&self, generation: u64) -> ObserveSampleBatch {
        let requested_at = OffsetDateTime::now_utc();
        let targets = match self.observe_dashboard_targets() {
            Ok(targets) => targets,
            Err(error) => {
                return ObserveSampleBatch {
                    generation,
                    requested_at,
                    received_at: OffsetDateTime::now_utc(),
                    targets: vec![ObserveTargetSample {
                        node_id: self
                            .observe_local_node_id()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "local".to_string()),
                        response: Err(error),
                    }],
                };
            }
        };

        let local_sample_id = targets
            .first()
            .and_then(Option::as_ref)
            .map(ToString::to_string)
            .unwrap_or_else(|| "local".to_string());
        let mut samples = Vec::with_capacity(targets.len());
        for chunk in targets.chunks(OBSERVE_DASHBOARD_MAX_WORKERS) {
            let results = Mutex::new(Vec::with_capacity(chunk.len()));
            std::thread::scope(|scope| {
                for target in chunk.iter().cloned() {
                    let results = &results;
                    scope.spawn(move || {
                        let node_id = target
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "local".to_string());
                        let response = self.observe_metrics_for_target(target.as_ref());
                        results
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(ObserveTargetSample { node_id, response });
                    });
                }
            });
            samples.extend(
                results
                    .into_inner()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
        samples.sort_by(|left, right| {
            (left.node_id != local_sample_id)
                .cmp(&(right.node_id != local_sample_id))
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        ObserveSampleBatch {
            generation,
            requested_at,
            received_at: OffsetDateTime::now_utc(),
            targets: samples,
        }
    }

    fn observe_metrics_for_target(
        &self,
        target: Option<&NodeId>,
    ) -> Result<ObserveMetricsResponse, String> {
        let address = self.connection.daemon_address().ok_or_else(|| {
            "live observation requires a running Sinomo daemon; reconnect the app after starting it"
                .to_string()
        })?;
        let request = ObjectRpcRequest::encode_body(
            OBSERVE_METRICS_ROUTE,
            OBSERVE_RPC_CATEGORY,
            &ObserveMetricsRequest {
                name_prefix: Some("sinomo.".to_string()),
                limit: OBSERVE_DASHBOARD_MAX_METRICS_PER_TARGET,
            },
        )
        .map_err(|error| error.to_string())?
        .with_trace_context_if_missing();
        let client = DaemonApiClient::at_address(address.to_string())
            .with_timeout(OBSERVE_DASHBOARD_RPC_TIMEOUT);
        let response = if target.is_none() || target == self.observe_local_node_id() {
            client.call_request(&request)
        } else {
            client.object_rpc_to_target_with_deadline(
                target.expect("remote target checked above"),
                &request,
                OBSERVE_DASHBOARD_RPC_TIMEOUT,
                Some(OBSERVE_DASHBOARD_RPC_TIMEOUT),
                true,
            )
        }
        .map_err(|error| error.to_string())?;
        response.decode_body().map_err(|error| error.to_string())
    }

    fn observe_dashboard_targets(&self) -> Result<Vec<Option<NodeId>>, String> {
        let Some(local_node_id) = self.observe_local_node_id().cloned() else {
            if self.connection.daemon_address().is_some() {
                return Ok(vec![None]);
            }
            return Err(
                "live observation is unavailable because the app has no daemon connection"
                    .to_string(),
            );
        };
        let paths = sinomo_runtime::network::ClusterPaths::new(self.data_dir.as_ref());
        let state = sinomo_runtime::cluster::read_cluster_state_without_kv(&paths)
            .map_err(|error| format!("cluster observation target discovery failed: {error}"))?;
        let authorized = state
            .members
            .iter()
            .filter(|member| member.state.is_authorized())
            .map(|member| member.node_id.clone());
        Ok(bounded_observation_targets(local_node_id, authorized))
    }

    fn observe_local_node_id(&self) -> Option<&NodeId> {
        match &self.connection {
            Connection::Daemon(status) | Connection::InProcessDaemon(status) => {
                status.cluster.as_ref().map(|cluster| &cluster.node_id)
            }
            Connection::LocalFallback { .. } => None,
        }
    }
}

fn bounded_observation_targets(
    local_node_id: NodeId,
    authorized: impl IntoIterator<Item = NodeId>,
) -> Vec<Option<NodeId>> {
    let mut remotes = authorized.into_iter().collect::<BTreeSet<_>>();
    remotes.remove(&local_node_id);
    std::iter::once(Some(local_node_id))
        .chain(
            remotes
                .into_iter()
                .take(OBSERVE_DASHBOARD_MAX_TARGETS.saturating_sub(1))
                .map(Some),
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemonless_dashboard_sample_is_bounded_error() {
        let backend = Backend::local_fallback();
        let batch = backend.observe_cluster_metrics(7);

        assert_eq!(batch.generation, 7);
        assert_eq!(batch.targets.len(), 1);
        assert_eq!(batch.targets[0].node_id, "local");
        assert!(batch.targets[0].response.is_err());
    }

    #[test]
    fn observation_target_bound_keeps_local_first() {
        let local = NodeId::from_legacy_named("node_observe_local_z")
            .expect("valid local observation node id");
        let authorized = (0..64)
            .map(|index| {
                NodeId::from_legacy_named(format!("node_observe_remote_{index:02}"))
                    .expect("valid remote observation node id")
            })
            .chain(std::iter::once(local.clone()));

        let targets = bounded_observation_targets(local.clone(), authorized);

        assert_eq!(targets.len(), OBSERVE_DASHBOARD_MAX_TARGETS);
        assert_eq!(targets.first().and_then(Option::as_ref), Some(&local));
        assert_eq!(
            targets
                .iter()
                .filter(|target| target.as_ref() == Some(&local))
                .count(),
            1
        );
    }
}
