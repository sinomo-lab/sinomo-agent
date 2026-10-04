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
const SERVICE_CATALOG_STARTUP_RECONCILE_INTERVAL: Duration = Duration::from_secs(2);
const SERVICE_CATALOG_STARTUP_RECONCILE_WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
struct ServiceCatalogPublisherTiming {
    publish_timeout: Duration,
    retry_initial: Duration,
    retry_max: Duration,
    startup_reconcile_interval: Duration,
    startup_reconcile_window: Duration,
}

impl ServiceCatalogPublisherTiming {
    const PRODUCTION: Self = Self {
        publish_timeout: SERVICE_CATALOG_PUBLISH_TIMEOUT,
        retry_initial: SERVICE_CATALOG_RETRY_INITIAL,
        retry_max: SERVICE_CATALOG_RETRY_MAX,
        startup_reconcile_interval: SERVICE_CATALOG_STARTUP_RECONCILE_INTERVAL,
        startup_reconcile_window: SERVICE_CATALOG_STARTUP_RECONCILE_WINDOW,
    };
}

pub type ServiceCatalogPublishFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

pub trait ServiceCatalogPublisherHost: Send + Sync {
    fn publish<'a>(
        &'a self,
        snapshot: Arc<ServiceRegistrySnapshot>,
        authoritative_recheck: bool,
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
                runtime.spawn(run_service_catalog_publisher(host, registry, shutdown)),
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
            let host: Arc<dyn ServiceCatalogPublisherHost> =
                Arc::new(ComponentServiceCatalogPublisherHost {
                    cluster,
                    local_node_id: state.local_node_id,
                });
            scope.own_task(
                "cluster service catalog publisher",
                runtime.spawn(run_service_catalog_publisher(
                    host,
                    registry,
                    scope.shutdown_receiver(),
                )),
            );
            Ok(())
        },
    )
}

struct ComponentServiceCatalogPublisherHost {
    cluster: Arc<dyn sinomo_core::service::ClusterAccess>,
    local_node_id: sinomo_core::node::NodeId,
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
        authoritative_recheck: bool,
    ) -> ServiceCatalogPublishFuture<'a> {
        Box::pin(async move {
            let records = sinomo_core::service::service_instance_records_from_registry(
                snapshot.registry(),
                &self.local_node_id,
                snapshot.version(),
                time::OffsetDateTime::now_utc(),
            );
            let summary = service_catalog_log_summary(&records);
            let publication =
                sinomo_core::service::publish_local_service_instances_with_authoritative_recheck(
                    self.cluster.as_ref(),
                    snapshot.registry(),
                    &self.local_node_id,
                    snapshot.version(),
                    time::OffsetDateTime::now_utc(),
                    authoritative_recheck,
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
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    run_service_catalog_publisher_with_timing(
        host,
        registry,
        shutdown,
        ServiceCatalogPublisherTiming::PRODUCTION,
    )
    .await;
}

async fn run_service_catalog_publisher_with_timing(
    host: Arc<dyn ServiceCatalogPublisherHost>,
    registry: sinomo_core::service::ServiceRegistrySnapshotStore,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    timing: ServiceCatalogPublisherTiming,
) {
    let mut versions = registry.subscribe_versions();
    let mut retry = timing.retry_initial;
    let mut startup_reconcile_deadline = None;
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
        let publication = match tokio::time::timeout(
            timing.publish_timeout,
            host.publish(
                snapshot.clone(),
                startup_reconcile_deadline
                    .is_none_or(|deadline| deadline > tokio::time::Instant::now()),
            ),
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
                // A retiring main can publish its teardown registry after the
                // replacement main has already published the same node-scoped
                // records. Reconcile the surviving registry for a bounded
                // overlap window so that late deletion is repaired without
                // turning steady-state service discovery into a polling loop.
                let startup_reconcile_deadline =
                    *startup_reconcile_deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now() + timing.startup_reconcile_window
                    });
                tracing::debug!(
                    version = snapshot.version(),
                    "published component service catalog generation"
                );
                if let Some(remaining) =
                    startup_reconcile_deadline.checked_duration_since(tokio::time::Instant::now())
                {
                    tokio::select! {
                        _ = tokio::time::sleep(timing.startup_reconcile_interval.min(remaining)) => {}
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
                } else {
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

    struct CountingHost(AtomicUsize);

    impl ServiceCatalogPublisherHost for CountingHost {
        fn publish<'a>(
            &'a self,
            _snapshot: Arc<ServiceRegistrySnapshot>,
            _authoritative_recheck: bool,
        ) -> ServiceCatalogPublishFuture<'a> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    struct HangingOnceHost(AtomicUsize);

    impl ServiceCatalogPublisherHost for HangingOnceHost {
        fn publish<'a>(
            &'a self,
            _snapshot: Arc<ServiceRegistrySnapshot>,
            _authoritative_recheck: bool,
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
        let host = Arc::new(CountingHost(AtomicUsize::new(0)));
        let registry = sinomo_core::service::ServiceRegistrySnapshotStore::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_service_catalog_publisher(
            host.clone(),
            registry.clone(),
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
        let host = Arc::new(CountingHost(AtomicUsize::new(0)));
        let registry = sinomo_core::service::ServiceRegistrySnapshotStore::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_service_catalog_publisher(
            host.clone(),
            registry.clone(),
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
            shutdown_rx,
            ServiceCatalogPublisherTiming {
                publish_timeout: Duration::from_millis(10),
                retry_initial: Duration::from_millis(1),
                retry_max: Duration::from_millis(2),
                startup_reconcile_interval: Duration::from_secs(1),
                startup_reconcile_window: Duration::from_secs(1),
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
    async fn stable_registry_is_reconciled_during_process_replacement_window() {
        let host = Arc::new(CountingHost(AtomicUsize::new(0)));
        let registry = sinomo_core::service::ServiceRegistrySnapshotStore::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let reconcile_interval = Duration::from_millis(5);
        let reconcile_window = Duration::from_millis(20);
        let task = tokio::spawn(run_service_catalog_publisher_with_timing(
            host.clone(),
            registry.clone(),
            shutdown_rx,
            ServiceCatalogPublisherTiming {
                publish_timeout: Duration::from_secs(1),
                retry_initial: Duration::from_millis(1),
                retry_max: Duration::from_millis(2),
                startup_reconcile_interval: reconcile_interval,
                startup_reconcile_window: reconcile_window,
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
        .expect("stable startup registry was reconciled again");

        tokio::time::sleep(reconcile_window + reconcile_interval).await;
        let settled_count = host.0.load(Ordering::SeqCst);
        tokio::time::sleep(reconcile_interval.saturating_mul(3)).await;
        assert_eq!(
            host.0.load(Ordering::SeqCst),
            settled_count,
            "the publisher must park again after the process-replacement window"
        );

        shutdown_tx.send_replace(true);
        task.await.expect("publisher task");
    }
}
