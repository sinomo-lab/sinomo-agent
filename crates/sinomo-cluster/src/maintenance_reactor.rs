//! Generic lifecycle driver for cluster/network maintenance.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type ClusterMaintenanceFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub trait ClusterMaintenanceReactorHost: Send {
    type Work: Send + 'static;
    type Error: std::fmt::Display + Send + 'static;

    fn identity_is_current<'a>(&'a mut self) -> ClusterMaintenanceFuture<'a, bool, Self::Error>;
    fn inhibited(&self) -> bool;
    fn plan<'a>(&'a mut self) -> ClusterMaintenanceFuture<'a, Option<Self::Work>, Self::Error>;
    fn execute<'a>(
        &'a mut self,
        work: Self::Work,
        deadline: Instant,
    ) -> ClusterMaintenanceFuture<'a, (), Self::Error>;
    fn next_due(&self, now: Instant) -> Duration;
    fn record_plan_failure(&mut self, now: Instant);
    fn observe_phase(&mut self, timed_out: bool, elapsed: Duration);
    fn on_wake(&mut self);
    fn wait_for_external_change<'a>(&'a mut self) -> ClusterMaintenanceFuture<'a, (), Self::Error>;
}

#[derive(Clone, Copy, Debug)]
pub struct ClusterMaintenanceReactorConfig {
    pub phase_timeout: Duration,
    pub execution_tail_reserve: Duration,
    pub inhibited_retry: Duration,
    pub identity_retry: Duration,
}

impl Default for ClusterMaintenanceReactorConfig {
    fn default() -> Self {
        Self {
            phase_timeout: Duration::from_secs(10),
            execution_tail_reserve: Duration::from_millis(500),
            inhibited_retry: Duration::from_secs(15),
            identity_retry: Duration::from_secs(15),
        }
    }
}

pub async fn run_cluster_maintenance_reactor<H>(
    host: &mut H,
    config: ClusterMaintenanceReactorConfig,
    wake: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) where
    H: ClusterMaintenanceReactorHost,
{
    loop {
        if *shutdown.borrow() {
            break;
        }
        match host.identity_is_current().await {
            Ok(true) => {}
            Ok(false) => break,
            Err(error) => {
                tracing::warn!(%error, "cluster maintenance identity check failed");
                if wait_for_reactor_wake(host, config.identity_retry, &wake, &mut shutdown).await {
                    break;
                }
                continue;
            }
        }

        if host.inhibited() {
            if wait_for_reactor_wake(host, config.inhibited_retry, &wake, &mut shutdown).await {
                break;
            }
            continue;
        }

        let phase_started = Instant::now();
        let plan = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            plan = tokio::time::timeout(config.phase_timeout, host.plan()) => plan,
        };
        let (work, timed_out) = match plan {
            Ok(Ok(work)) => (work, false),
            Ok(Err(error)) => {
                host.record_plan_failure(Instant::now());
                tracing::warn!(%error, "cluster maintenance tick planning failed");
                (None, false)
            }
            Err(_) => {
                host.record_plan_failure(Instant::now());
                tracing::warn!("cluster maintenance tick planning timed out");
                (None, true)
            }
        };
        if let Some(work) = work {
            let deadline = Instant::now()
                .checked_add(
                    config
                        .phase_timeout
                        .saturating_sub(config.execution_tail_reserve),
                )
                .unwrap_or_else(Instant::now);
            let execution = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
                execution = host.execute(work, deadline) => execution,
            };
            if let Err(error) = execution {
                tracing::warn!(%error, "cluster maintenance pending work failed");
            }
        }
        host.observe_phase(timed_out, phase_started.elapsed());

        let due = nonzero_duration(host.next_due(Instant::now()));
        if wait_for_reactor_wake(host, due, &wake, &mut shutdown).await {
            break;
        }
    }
}

async fn wait_for_reactor_wake<H>(
    host: &mut H,
    delay: Duration,
    wake: &tokio::sync::Notify,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool
where
    H: ClusterMaintenanceReactorHost,
{
    tokio::select! {
        _ = tokio::time::sleep(nonzero_duration(delay)) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        _ = wake.notified() => {
            host.on_wake();
            false
        },
        changed = host.wait_for_external_change() => {
            if let Err(error) = changed {
                tracing::warn!(%error, "cluster maintenance external-change watcher failed");
            }
            false
        }
    }
}

fn nonzero_duration(duration: Duration) -> Duration {
    if duration.is_zero() {
        Duration::from_millis(1)
    } else {
        duration
    }
}
