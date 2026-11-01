//! Lifecycle-owned publication of live service-registry generations.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sinomo_core::service::ServiceRegistrySnapshot;

use crate::{ComponentDescriptor, ComponentError, StaticComponentFactory};

pub const SERVICE_CATALOG_PUBLISHER_COMPONENT_ID: &str = "sinomo.cluster.service-catalog-publisher";
const SERVICE_CATALOG_RETRY_INITIAL: Duration = Duration::from_millis(250);
const SERVICE_CATALOG_RETRY_MAX: Duration = Duration::from_secs(30);
const SERVICE_CATALOG_PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
struct ServiceCatalogPublisherTiming {
    publish_timeout: Duration,
    retry_initial: Duration,
    retry_max: Duration,
}

impl ServiceCatalogPublisherTiming {
    const PRODUCTION: Self = Self {
        publish_timeout: SERVICE_CATALOG_PUBLISH_TIMEOUT,
        retry_initial: SERVICE_CATALOG_RETRY_INITIAL,
        retry_max: SERVICE_CATALOG_RETRY_MAX,
    };
}

pub type ServiceCatalogPublishFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

pub trait ServiceCatalogPublisherHost: Send + Sync {
    fn publish<'a>(
        &'a self,
        snapshot: Arc<ServiceRegistrySnapshot>,
    ) -> ServiceCatalogPublishFuture<'a>;
}

pub fn service_catalog_publisher_component_factory(
    host: Arc<dyn ServiceCatalogPublisherHost>,
) -> Result<StaticComponentFactory, ComponentError> {
    StaticComponentFactory::new(
        ComponentDescriptor::new(
            SERVICE_CATALOG_PUBLISHER_COMPONENT_ID,
            env!("CARGO_PKG_VERSION"),
        ),
        move |context, scope| {
            let runtime = context
                .runtime()
                .ok_or_else(|| ComponentError::RuntimeUnavailable {
                    component_id: SERVICE_CATALOG_PUBLISHER_COMPONENT_ID.to_string(),
                })?;
            let registry = context.service_registry_snapshot_store()?;
            let host = Arc::clone(&host);
            let shutdown = scope.shutdown_receiver();
            scope.own_task(
                "cluster service catalog publisher",
                runtime.spawn(run_service_catalog_publisher(
                    host, registry, None, shutdown,
                )),
            );
            Ok(())
        },
    )
}

/// Product-selectable cluster service-catalog component. It derives all host
/// mechanism from [`crate::ComponentContext`], so the daemon neither knows
/// about this component nor appends a fallback factory.
pub fn cluster_service_catalog_publisher_component_factory()
-> Result<StaticComponentFactory, ComponentError> {
    StaticComponentFactory::new(
        ComponentDescriptor::new(
            SERVICE_CATALOG_PUBLISHER_COMPONENT_ID,
            env!("CARGO_PKG_VERSION"),
        ),
        move |context, scope| {
            let runtime = context
                .runtime()
                .ok_or_else(|| ComponentError::RuntimeUnavailable {
                    component_id: SERVICE_CATALOG_PUBLISHER_COMPONENT_ID.to_string(),
                })?;
            let cluster = context.cluster_access().ok_or_else(|| {
                ComponentError::component(
                    SERVICE_CATALOG_PUBLISHER_COMPONENT_ID,
                    "cluster access is unavailable",
                )
            })?;
            let cluster_paths = sinomo_core::network::ClusterPaths::new(context.data_dir());
            let state =
                sinomo_core::cluster::read_cluster_state(&cluster_paths).map_err(|error| {
                    ComponentError::component(SERVICE_CATALOG_PUBLISHER_COMPONENT_ID, error)
                })?;
            let registry = context.service_registry_snapshot_store()?;
            let records_changed = Arc::new(tokio::sync::Notify::new());
            let subscription = subscribe_local_service_record_changes(
                &cluster_paths,
                state.local_node_id.clone(),
                Arc::clone(&records_changed),
            );
            let host: Arc<dyn ServiceCatalogPublisherHost> =
                Arc::new(ComponentServiceCatalogPublisherHost {
                    cluster,
                    local_node_id: state.local_node_id,
                });
            let shutdown = scope.shutdown_receiver();
            scope.own_task(
                "cluster service catalog publisher",
                runtime.spawn(async move {
                    let _subscription = subscription;
                    run_service_catalog_publisher(host, registry, Some(records_changed), shutdown)
                        .await;
                }),
            );
            Ok(())
        },
    )
}

struct ComponentServiceCatalogPublisherHost {
    cluster: Arc<dyn sinomo_core::service::ClusterAccess>,
    local_node_id: sinomo_core::node::NodeId,
}

/// This node owns its service records. Wake the publisher when a committed
/// change reaches them through any path other than its own registry: a late
/// commit from the process it replaced, or a local view that caught up with one.
fn subscribe_local_service_record_changes(
    cluster_paths: &sinomo_core::network::ClusterPaths,
    local_node_id: sinomo_core::node::NodeId,
    records_changed: Arc<tokio::sync::Notify>,
) -> sinomo_core::kv::KvCommittedObservationSubscription {
    sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        cluster_paths,
        move |observation| {
            let touched = match &observation.value {
                sinomo_core::kv::KvCommittedObservationValue::Delta(delta) => {
                    events_touch_local_service_records(&delta.events, &local_node_id)
                }
                // An installed or discontinuous view can change any record.
                sinomo_core::kv::KvCommittedObservationValue::Snapshot(_)
                | sinomo_core::kv::KvCommittedObservationValue::Discontinuity(_) => true,
            };
            if touched {
                records_changed.notify_one();
            }
        },
    )
}

fn events_touch_local_service_records(
    events: &[sinomo_core::kv::KvEvent],
    local_node_id: &sinomo_core::node::NodeId,
) -> bool {
    events.iter().any(|event| {
        sinomo_core::service::service_instance_key_belongs_to_node(&event.key, local_node_id)
    })
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ServiceCatalogLogSummary {
    service_ids: Vec<String>,
    provider_capabilities: Vec<String>,
    route_count: usize,
}

fn service_catalog_log_summary(
    records: &[sinomo_core::service::ServiceInstanceRecord],
) -> ServiceCatalogLogSummary {
    let service_ids = records
        .iter()
        .map(|record| record.service_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let provider_capabilities = records
        .iter()
        .flat_map(|record| record.provider_capabilities.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let route_count = records.iter().map(|record| record.routes.len()).sum();
    ServiceCatalogLogSummary {
        service_ids,
        provider_capabilities,
        route_count,
    }
}

impl ServiceCatalogPublisherHost for ComponentServiceCatalogPublisherHost {
    fn publish<'a>(
        &'a self,
        snapshot: Arc<ServiceRegistrySnapshot>,
    ) -> ServiceCatalogPublishFuture<'a> {
        Box::pin(async move {
            let records = sinomo_core::service::service_instance_records_from_registry(
                snapshot.registry(),
                &self.local_node_id,
                snapshot.version(),
                time::OffsetDateTime::now_utc(),
            );
            let summary = service_catalog_log_summary(&records);
            let publication = sinomo_core::service::publish_local_service_instances(
                self.cluster.as_ref(),
                snapshot.registry(),
                &self.local_node_id,
                snapshot.version(),
                time::OffsetDateTime::now_utc(),
            )
            .await
            .map_err(|error| error.to_string())?;
            tracing::info!(
                node_id = %self.local_node_id,
                registry_version = snapshot.version(),
                registry_route_count = snapshot.registry().route_count(),
                published_route_count = summary.route_count,
                published_service_count = summary.service_ids.len(),
                published_provider_capability_count = summary.provider_capabilities.len(),
                catalog_changed = publication.is_some(),
                published_service_ids = ?summary.service_ids,
                published_provider_capabilities = ?summary.provider_capabilities,
                "reconciled component service catalog"
            );
            Ok(())
        })
    }
}

async fn run_service_catalog_publisher(
    host: Arc<dyn ServiceCatalogPublisherHost>,
    registry: sinomo_core::service::ServiceRegistrySnapshotStore,
    records_changed: Option<Arc<tokio::sync::Notify>>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    run_service_catalog_publisher_with_timing(
        host,
        registry,
        records_changed,
        shutdown,
        ServiceCatalogPublisherTiming::PRODUCTION,
    )
    .await;
}

async fn local_service_records_changed(records_changed: Option<&tokio::sync::Notify>) {
    match records_changed {
        Some(records_changed) => records_changed.notified().await,
        None => std::future::pending().await,
    }
}

async fn run_service_catalog_publisher_with_timing(
    host: Arc<dyn ServiceCatalogPublisherHost>,
    registry: sinomo_core::service::ServiceRegistrySnapshotStore,
    records_changed: Option<Arc<tokio::sync::Notify>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    timing: ServiceCatalogPublisherTiming,
) {
    let mut versions = registry.subscribe_versions();
    let mut retry = timing.retry_initial;
    if registry.active_snapshot().version() == 1 {
        tokio::select! {
            changed = versions.changed() => {
                if changed.is_err() {
                    return;
                }
                let _ = versions.borrow_and_update();
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
    loop {
        if *shutdown.borrow() {
            break;
        }
        let snapshot = registry.active_snapshot();
        if snapshot.host_stopping() {
            // Withdrawing local admission is not a change to this node's offer;
            // the replacement process owns the same records.
            tokio::select! {
                changed = versions.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let _ = versions.borrow_and_update();
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
            continue;
        }
        let publication = match tokio::time::timeout(
            timing.publish_timeout,
            host.publish(snapshot.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(format!(
                "component service catalog publication exceeded its {}ms attempt timeout",
                timing.publish_timeout.as_millis()
            )),
        };
        match publication {
            Ok(()) => {
                retry = timing.retry_initial;
                tracing::debug!(
                    version = snapshot.version(),
                    "published component service catalog generation"
                );
                tokio::select! {
                    changed = versions.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let _ = versions.borrow_and_update();
                    }
                    () = local_service_records_changed(records_changed.as_deref()) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    version = snapshot.version(),
                    error = %error,
                    "component service catalog publication failed"
                );
                tokio::select! {
                    _ = tokio::time::sleep(retry) => {
                        retry = retry.saturating_mul(2).min(timing.retry_max);
                    }
                    changed = versions.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let _ = versions.borrow_and_update();
                        retry = timing.retry_initial;
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingHost(AtomicUsize);

    impl ServiceCatalogPublisherHost for CountingHost {
        fn publish<'a>(
            &'a self,
            _snapshot: Arc<ServiceRegistrySnapshot>,
        ) -> ServiceCatalogPublishFuture<'a> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    async fn wait_for_publications(host: &CountingHost, count: usize, context: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.0.load(Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect(context);
    }

    struct HangingOnceHost(AtomicUsize);

    impl ServiceCatalogPublisherHost for HangingOnceHost {
        fn publish<'a>(
            &'a self,
            _snapshot: Arc<ServiceRegistrySnapshot>,
        ) -> ServiceCatalogPublishFuture<'a> {
            Box::pin(async move {
                let call = self.0.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
    }

    #[test]
    fn catalog_log_summary_lists_published_services_and_provider_capabilities() {
        let node = sinomo_core::node::NodeId::from_legacy_named("catalog_log_summary_node")
            .expect("node id");
        let mut first = sinomo_core::service::ServiceInstanceRecord::new("alpha", node.clone());
        first.routes = vec!["/alpha/read".to_string(), "/alpha/write".to_string()];
        first.provider_capabilities = vec!["alpha.write".to_string(), "alpha.read".to_string()];
        let mut second = sinomo_core::service::ServiceInstanceRecord::new("beta", node);
        second.routes = vec!["/beta/read".to_string()];
        second.provider_capabilities = vec!["alpha.read".to_string(), "beta.read".to_string()];

        let summary = service_catalog_log_summary(&[second, first]);

        assert_eq!(summary.service_ids, ["alpha", "beta"]);
        assert_eq!(
            summary.provider_capabilities,
            ["alpha.read", "alpha.write", "beta.read"]
        );
        assert_eq!(summary.route_count, 3);
    }

    #[tokio::test]
    async fn registry_generation_change_triggers_publication() {
        let host = Arc::new(CountingHost::default());
        let registry = sinomo_core::service::ServiceRegistrySnapshotStore::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_service_catalog_publisher(
            host.clone(),
            registry.clone(),
            None,
            shutdown_rx,
        ));
        registry
            .replace_registry(sinomo_core::service::ServiceRegistry::default())
            .expect("publish reconciled registry");
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.0.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial publication");
        registry
            .replace_registry(sinomo_core::service::ServiceRegistry::default())
            .expect("replace registry");
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.0.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("generation publication");
        shutdown_tx.send_replace(true);
        task.await.expect("publisher task");
    }

    #[tokio::test]
    async fn stable_registry_parks_after_successful_publication() {
        let host = Arc::new(CountingHost::default());
        let registry = sinomo_core::service::ServiceRegistrySnapshotStore::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_service_catalog_publisher(
            host.clone(),
            registry.clone(),
            None,
            shutdown_rx,
        ));
        registry
            .replace_registry(sinomo_core::service::ServiceRegistry::default())
            .expect("publish reconciled registry");
        while host.0.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            host.0.load(Ordering::SeqCst),
            1,
            "an unchanged service generation must remain parked"
        );

        shutdown_tx.send_replace(true);
        task.await.expect("publisher task");
    }

    #[tokio::test]
    async fn timed_out_publication_is_cancelled_and_retried() {
        let host = Arc::new(HangingOnceHost(AtomicUsize::new(0)));
        let registry = sinomo_core::service::ServiceRegistrySnapshotStore::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_service_catalog_publisher_with_timing(
            host.clone(),
            registry.clone(),
            None,
            shutdown_rx,
            ServiceCatalogPublisherTiming {
                publish_timeout: Duration::from_millis(10),
                retry_initial: Duration::from_millis(1),
                retry_max: Duration::from_millis(2),
            },
        ));
        registry
            .replace_registry(sinomo_core::service::ServiceRegistry::default())
            .expect("publish reconciled registry");

        tokio::time::timeout(Duration::from_secs(1), async {
            while host.0.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publisher retried after cancelling the stuck attempt");

        shutdown_tx.send_replace(true);
        task.await.expect("publisher task");
    }

    #[tokio::test]
    async fn host_stop_withdrawal_is_not_published() {
        let host = Arc::new(CountingHost::default());
        let registry = sinomo_core::service::ServiceRegistrySnapshotStore::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_service_catalog_publisher(
            host.clone(),
            registry.clone(),
            None,
            shutdown_rx,
        ));
        registry
            .replace_registry(sinomo_core::service::ServiceRegistry::default())
            .expect("publish reconciled registry");
        wait_for_publications(&host, 1, "initial publication").await;

        registry
            .withdraw_for_host_stop(sinomo_core::service::ServiceRegistry::default())
            .expect("withdraw admission");
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            host.0.load(Ordering::SeqCst),
            1,
            "a stopping host must not publish its admission withdrawal"
        );

        registry
            .replace_registry(sinomo_core::service::ServiceRegistry::default())
            .expect("reinstall registry");
        wait_for_publications(&host, 2, "a later live generation publishes").await;
        shutdown_tx.send_replace(true);
        task.await.expect("publisher task");
    }

    #[tokio::test]
    async fn committed_change_to_local_records_reconciles_without_polling() {
        let host = Arc::new(CountingHost::default());
        let registry = sinomo_core::service::ServiceRegistrySnapshotStore::default();
        let records_changed = Arc::new(tokio::sync::Notify::new());
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_service_catalog_publisher(
            host.clone(),
            registry.clone(),
            Some(Arc::clone(&records_changed)),
            shutdown_rx,
        ));
        registry
            .replace_registry(sinomo_core::service::ServiceRegistry::default())
            .expect("publish reconciled registry");
        wait_for_publications(&host, 1, "initial publication").await;

        records_changed.notify_one();
        wait_for_publications(&host, 2, "a committed record change reconciles").await;
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            host.0.load(Ordering::SeqCst),
            2,
            "without registry or record changes the publisher stays parked"
        );
        shutdown_tx.send_replace(true);
        task.await.expect("publisher task");
    }

    #[test]
    fn only_local_service_record_events_wake_the_publisher() {
        let local =
            sinomo_core::node::NodeId::from_legacy_named("catalog_wake_local").expect("local node");
        let remote = sinomo_core::node::NodeId::from_legacy_named("catalog_wake_remote")
            .expect("remote node");
        let event = |key: String| sinomo_core::kv::KvEvent {
            revision: sinomo_core::kv::KvRevision(1),
            key,
            kind: sinomo_core::kv::KvEventKind::Delete,
        };
        let remote_record = event(sinomo_core::service::cluster_service_instance_key(
            "dfs", &remote,
        ));
        let local_record = event(sinomo_core::service::cluster_service_instance_key(
            "dfs", &local,
        ));
        let unrelated = event(format!("/cluster/members/{local}"));

        assert!(!events_touch_local_service_records(
            &[remote_record.clone(), unrelated.clone()],
            &local
        ));
        assert!(events_touch_local_service_records(
            &[remote_record, unrelated, local_record],
            &local
        ));
    }
}
