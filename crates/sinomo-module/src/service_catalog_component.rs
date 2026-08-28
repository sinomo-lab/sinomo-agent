//! Lifecycle-owned publication of live service-registry generations.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sinomo_core::service::ServiceRegistrySnapshot;

use crate::{ComponentDescriptor, ComponentError, StaticComponentFactory};

pub const SERVICE_CATALOG_PUBLISHER_COMPONENT_ID: &str = "sinomo.cluster.service-catalog-publisher";
const SERVICE_CATALOG_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const SERVICE_CATALOG_RETRY_INITIAL: Duration = Duration::from_millis(250);
const SERVICE_CATALOG_RETRY_MAX: Duration = Duration::from_secs(30);

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
                runtime.spawn(run_service_catalog_publisher(host, registry, shutdown)),
            );
            Ok(())
        },
    )
}

async fn run_service_catalog_publisher(
    host: Arc<dyn ServiceCatalogPublisherHost>,
    registry: sinomo_core::service::ServiceRegistrySnapshotStore,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut versions = registry.subscribe_versions();
    let mut retry = SERVICE_CATALOG_RETRY_INITIAL;
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
        match host.publish(snapshot.clone()).await {
            Ok(()) => {
                retry = SERVICE_CATALOG_RETRY_INITIAL;
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
                    _ = tokio::time::sleep(SERVICE_CATALOG_REFRESH_INTERVAL) => {}
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
                        retry = retry.saturating_mul(2).min(SERVICE_CATALOG_RETRY_MAX);
                    }
                    changed = versions.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let _ = versions.borrow_and_update();
                        retry = SERVICE_CATALOG_RETRY_INITIAL;
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
        ) -> ServiceCatalogPublishFuture<'a> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
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
}
