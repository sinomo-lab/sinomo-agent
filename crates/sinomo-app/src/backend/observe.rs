//! Typed live-observation adapter for the graphical Cluster dashboard.
//!
//! The app always enters observation through its connected daemon. Local reads
//! use that daemon's ordinary observe route; remote reads ask the same daemon to
//! route the object RPC through its persistent cluster transports.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::mpsc;
use std::time::Duration;

use sinomo_client::api::DaemonApiClient;
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
pub struct ObserveTargetSample<Response = ObserveMetricsResponse> {
    pub node_id: String,
    pub response: Result<Response, String>,
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
    /// synthetic `local` target. This synchronous facade waits for tasks on
    /// `app_runtime`, so callers must invoke it from an ordinary OS worker,
    /// never from one of that runtime's worker threads.
    pub fn observe_cluster_metrics(&self, generation: u64) -> ObserveSampleBatch {
        let requested_at = OffsetDateTime::now_utc();
        let targets = match self.observe_dashboard_targets() {
            Ok(mut targets) => {
                targets.truncate(OBSERVE_DASHBOARD_MAX_TARGETS);
                targets
            }
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

        let disconnected_targets = targets.clone();
        let address = self.connection.daemon_address().map(ToString::to_string);
        let local_node_id = self.observe_local_node_id().cloned();
        let (sample_tx, sample_rx) = mpsc::sync_channel(1);
        // The dashboard API remains synchronous for the UI refresh worker, but
        // daemon futures are created and polled only on the app runtime. This
        // keeps deeply composed routed-RPC polls off short-lived OS threads.
        self.app_runtime.spawn(async move {
            let samples = collect_bounded_observation_samples(targets, move |target| {
                let address = address.clone();
                let local_node_id = local_node_id.clone();
                async move { observe_metrics_for_target(address, local_node_id, target).await }
            })
            .await;
            let _ = sample_tx.send(samples);
        });
        let samples = sample_rx.recv().unwrap_or_else(|_| {
            disconnected_targets
                .into_iter()
                .map(|target| ObserveTargetSample {
                    node_id: observation_target_label(target.as_ref()),
                    response: Err(
                        "cluster observation task stopped before returning a sample".to_string()
                    ),
                })
                .collect()
        });
        ObserveSampleBatch {
            generation,
            requested_at,
            received_at: OffsetDateTime::now_utc(),
            targets: samples,
        }
    }

    /// Called on an ordinary UI worker, with all remote reads routed through
    /// the connected daemon. Read-only observation needs no host-management grant.
    pub fn observe_cluster_logs(
        &self,
        generation: u64,
        node: Option<String>,
        request: sinomo_runtime::observe::ObserveLogsRequest,
    ) -> ObserveLogBatch {
        let discovered = match node.as_deref() {
            Some("local") => Ok(vec![self.observe_local_node_id().cloned()]),
            Some(node) => node
                .parse::<NodeId>()
                .map(|node| vec![Some(node)])
                .map_err(|error| error.to_string()),
            None => self.observe_dashboard_targets(),
        };
        let mut targets = match discovered {
            Ok(targets) => targets,
            Err(error) => {
                return ObserveLogBatch {
                    generation,
                    received_at: OffsetDateTime::now_utc(),
                    omitted_nodes: 0,
                    targets: vec![ObserveTargetSample {
                        node_id: node.unwrap_or_else(|| "local".into()),
                        response: Err(error),
                    }],
                };
            }
        };
        let omitted_nodes = targets.len().saturating_sub(OBSERVE_DASHBOARD_MAX_TARGETS);
        targets.truncate(OBSERVE_DASHBOARD_MAX_TARGETS);
        let disconnected_targets = targets.clone();
        let address = self.connection.daemon_address().map(ToString::to_string);
        let local = self.observe_local_node_id().cloned();
        let (tx, rx) = mpsc::sync_channel(1);
        self.app_runtime.spawn(async move {
            let samples = collect_bounded_observation_samples(targets, move |target| {
                let (address, local, query) = (address.clone(), local.clone(), request.clone());
                async move { observe_logs_for_target(address, local, target, query).await }
            })
            .await;
            let _ = tx.send(samples);
        });
        let targets = rx.recv().unwrap_or_else(|_| {
            disconnected_targets
                .into_iter()
                .map(|target| ObserveTargetSample {
                    node_id: observation_target_label(target.as_ref()),
                    response: Err("log collection task stopped".into()),
                })
                .collect()
        });
        ObserveLogBatch {
            generation,
            received_at: OffsetDateTime::now_utc(),
            omitted_nodes,
            targets,
        }
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
        Ok(observation_targets(local_node_id, authorized))
    }

    fn observe_local_node_id(&self) -> Option<&NodeId> {
        match &self.connection {
            Connection::Daemon(status) | Connection::AppOwnedDaemon(status) => {
                status.cluster.as_ref().map(|cluster| &cluster.node_id)
            }
            Connection::LocalFallback { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObserveLogBatch {
    pub generation: u64,
    pub received_at: OffsetDateTime,
    pub omitted_nodes: usize,
    pub targets: Vec<ObserveTargetSample<sinomo_runtime::observe::ObserveLogsResponse>>,
}

async fn observe_logs_for_target(
    address: Option<String>,
    local: Option<NodeId>,
    target: Option<NodeId>,
    query: sinomo_runtime::observe::ObserveLogsRequest,
) -> Result<sinomo_runtime::observe::ObserveLogsResponse, String> {
    let address =
        address.ok_or_else(|| "log collection requires a running Sinomo daemon".to_string())?;
    let request = ObjectRpcRequest::encode_body(
        sinomo_runtime::observe::OBSERVE_LOGS_ROUTE,
        OBSERVE_RPC_CATEGORY,
        &query,
    )
    .map_err(|e| e.to_string())?
    .with_trace_context_if_missing();
    let client = DaemonApiClient::at_address(address).with_timeout(OBSERVE_DASHBOARD_RPC_TIMEOUT);
    let response = match target.as_ref() {
        Some(node) if Some(node) != local.as_ref() => {
            Box::pin(client.object_rpc_to_target_with_deadline_async(
                node,
                &request,
                OBSERVE_DASHBOARD_RPC_TIMEOUT,
                Some(OBSERVE_DASHBOARD_RPC_TIMEOUT),
                true,
            ))
            .await
        }
        _ => client.call_request_async(&request).await,
    }
    .map_err(|e| e.to_string())?;
    response.decode_body().map_err(|e| e.to_string())
}

async fn observe_metrics_for_target(
    address: Option<String>,
    local_node_id: Option<NodeId>,
    target: Option<NodeId>,
) -> Result<ObserveMetricsResponse, String> {
    let address = address.ok_or_else(|| {
        "live observation requires a running Sinomo daemon; reconnect the app after starting it"
            .to_string()
    })?;
    let request = ObjectRpcRequest::encode_body(
        OBSERVE_METRICS_ROUTE,
        OBSERVE_RPC_CATEGORY,
        &ObserveMetricsRequest {
            name_prefix: None,
            limit: OBSERVE_DASHBOARD_MAX_METRICS_PER_TARGET,
        },
    )
    .map_err(|error| error.to_string())?
    .with_trace_context_if_missing();
    let client = DaemonApiClient::at_address(address).with_timeout(OBSERVE_DASHBOARD_RPC_TIMEOUT);
    let response = match target.as_ref() {
        Some(target_node_id) if Some(target_node_id) != local_node_id.as_ref() => {
            client
                .object_rpc_to_target_with_deadline_async(
                    target_node_id,
                    &request,
                    OBSERVE_DASHBOARD_RPC_TIMEOUT,
                    Some(OBSERVE_DASHBOARD_RPC_TIMEOUT),
                    true,
                )
                .await
        }
        _ => client.call_request_async(&request).await,
    }
    .map_err(|error| error.to_string())?;
    response.decode_body().map_err(|error| error.to_string())
}

async fn collect_bounded_observation_samples<Observe, ObserveFuture, Response>(
    targets: Vec<Option<NodeId>>,
    observe: Observe,
) -> Vec<ObserveTargetSample<Response>>
where
    Observe: Fn(Option<NodeId>) -> ObserveFuture + Clone + Send + 'static,
    ObserveFuture: Future<Output = Result<Response, String>> + Send + 'static,
    Response: Send + 'static,
{
    let mut samples = Vec::with_capacity(targets.len());
    // Each chunk preserves input order and bounds both routed traffic and live
    // task state. The next chunk starts only after every task in this one ends.
    for chunk in targets.chunks(OBSERVE_DASHBOARD_MAX_WORKERS) {
        let jobs = chunk
            .iter()
            .cloned()
            .map(|target| {
                let node_id = observation_target_label(target.as_ref());
                let observe = observe.clone();
                (
                    node_id,
                    tokio::spawn(async move { Box::pin(observe(target)).await }),
                )
            })
            .collect::<Vec<_>>();
        for (node_id, job) in jobs {
            let response = match job.await {
                Ok(response) => response,
                Err(error) => Err(format!("cluster observation task failed: {error}")),
            };
            samples.push(ObserveTargetSample { node_id, response });
        }
    }
    samples
}

fn observation_target_label(target: Option<&NodeId>) -> String {
    target
        .map(ToString::to_string)
        .unwrap_or_else(|| "local".to_string())
}

fn observation_targets(
    local_node_id: NodeId,
    authorized: impl IntoIterator<Item = NodeId>,
) -> Vec<Option<NodeId>> {
    let mut remotes = authorized.into_iter().collect::<BTreeSet<_>>();
    remotes.remove(&local_node_id);
    std::iter::once(Some(local_node_id))
        .chain(remotes.into_iter().map(Some))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

        let mut targets = observation_targets(local.clone(), authorized);
        assert_eq!(targets.len(), 65);
        targets.truncate(OBSERVE_DASHBOARD_MAX_TARGETS);

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

    #[test]
    fn async_observation_fanout_is_bounded_and_keeps_target_order() {
        let targets = std::iter::once(None)
            .chain((0..15).map(|index| {
                Some(
                    NodeId::from_legacy_named(format!("node_observe_async_{index:02}"))
                        .expect("valid async observation node id"),
                )
            }))
            .collect::<Vec<_>>();
        let expected_labels = targets
            .iter()
            .map(|target| observation_target_label(target.as_ref()))
            .collect::<Vec<_>>();
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let batch_barrier = Arc::new(tokio::sync::Barrier::new(OBSERVE_DASHBOARD_MAX_WORKERS));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .expect("observation test runtime");

        let samples = runtime.block_on(collect_bounded_observation_samples(targets, {
            let active = Arc::clone(&active);
            let maximum = Arc::clone(&maximum);
            let batch_barrier = Arc::clone(&batch_barrier);
            move |target| {
                let active = Arc::clone(&active);
                let maximum = Arc::clone(&maximum);
                let batch_barrier = Arc::clone(&batch_barrier);
                async move {
                    let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now_active, Ordering::SeqCst);
                    batch_barrier.wait().await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Err::<ObserveMetricsResponse, _>(format!(
                        "sampled {}",
                        observation_target_label(target.as_ref())
                    ))
                }
            }
        }));

        assert_eq!(
            samples
                .iter()
                .map(|sample| sample.node_id.clone())
                .collect::<Vec<_>>(),
            expected_labels
        );
        assert_eq!(
            maximum.load(Ordering::SeqCst),
            OBSERVE_DASHBOARD_MAX_WORKERS
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }
}
