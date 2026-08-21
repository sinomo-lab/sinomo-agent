//! Durable desired-rollout watch and reconciliation reactor.
//!
//! The rollout crate owns watch lifecycle, projection, retry, and exactly-once
//! reconciliation ordering. Process hosts supply endpoint discovery and RPC
//! transport through [`DesiredRolloutReactorHost`].

use sinomo_core::kv::{
    ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse, ClusterKvWatchDrainRequest,
    ClusterKvWatchDrainResponse, ClusterKvWatchRegistration, KvAuthoritativeRead, KvEvent,
    KvEventKind, KvLocalRead, KvRevision, KvSnapshot, WatchId,
};
use sinomo_core::{ClusterEndpointRecord, ClusterRolloutDesiredRecord};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS: i64 = 120;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DesiredRolloutReconcileDecision {
    Complete,
    TerminalFailure,
    Wait,
    Retry,
}

pub fn desired_rollout_reconcile_decision(
    status: Option<&sinomo_core::ClusterNodeRolloutStatus>,
    mode: Option<&str>,
    requested_at: Option<&str>,
    now: OffsetDateTime,
) -> DesiredRolloutReconcileDecision {
    let Some(status) = status else {
        return DesiredRolloutReconcileDecision::Retry;
    };
    if status.mode.as_deref() != mode || status.requested_at.as_deref() != requested_at {
        return DesiredRolloutReconcileDecision::Retry;
    }
    match status.status.as_str() {
        "accepted" | "healthy" | "recovered" => DesiredRolloutReconcileDecision::Complete,
        "main-staged" if rollout_progress_status_is_stale(status, now) => {
            DesiredRolloutReconcileDecision::TerminalFailure
        }
        "main-staged" => DesiredRolloutReconcileDecision::Wait,
        "failed" | "rejected" | "rolled-back" if desired_rollout_failure_is_hard(status) => {
            DesiredRolloutReconcileDecision::TerminalFailure
        }
        "failed" | "rejected" | "rolled-back" if rollout_progress_status_is_stale(status, now) => {
            DesiredRolloutReconcileDecision::Retry
        }
        "failed" | "rejected" | "rolled-back" => DesiredRolloutReconcileDecision::Wait,
        _ if rollout_progress_status_is_stale(status, now) => {
            DesiredRolloutReconcileDecision::Retry
        }
        _ => DesiredRolloutReconcileDecision::Wait,
    }
}

pub fn desired_rollout_failure_is_hard(status: &sinomo_core::ClusterNodeRolloutStatus) -> bool {
    let Some(phase) = status.phase.as_deref() else {
        return false;
    };
    matches!(
        phase,
        "activation-failed"
            | "build-failed"
            | "module-install-failed"
            | "rejected"
            | "shadow-handoff-failed"
            | "shadow-finalized"
            | "source-rejected"
            | "staging-failed"
    )
}

fn rollout_progress_status_timestamp(
    status: &sinomo_core::ClusterNodeRolloutStatus,
) -> Option<OffsetDateTime> {
    [
        status.updated_at.as_deref(),
        status.staged_at.as_deref(),
        status.failed_at.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter_map(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
    .max()
}

pub fn rollout_progress_status_is_stale(
    status: &sinomo_core::ClusterNodeRolloutStatus,
    now: OffsetDateTime,
) -> bool {
    let Some(updated_at) = rollout_progress_status_timestamp(status) else {
        return true;
    };
    now - updated_at >= time::Duration::seconds(DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS)
}

pub fn desired_rollout_stale_handoff_failure_status(
    status: &sinomo_core::ClusterNodeRolloutStatus,
    now: OffsetDateTime,
) -> sinomo_core::ClusterNodeRolloutStatus {
    let detail = format!(
        "candidate main handoff did not finalize within {}s; leaving desired rollout failed until a new rollout is requested",
        DESIRED_ROLLOUT_STALE_PROGRESS_AFTER_SECONDS
    );
    let mut failed =
        crate::cluster_rollout_progress_status(crate::ClusterRolloutProgressUpdate::new(
            "failed",
            status
                .mode
                .clone()
                .unwrap_or_else(|| "cluster-remote-build".to_string()),
            Some(detail.clone()),
            status
                .source
                .clone()
                .unwrap_or_else(|| "cluster desired rollout".to_string()),
            status.requested_at.clone().unwrap_or_else(|| {
                now.format(&Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string())
            }),
            "shadow-handoff-failed",
            status
                .progress_current
                .unwrap_or(crate::CLUSTER_ROLLOUT_PROGRESS_TOTAL),
            detail,
        ));
    failed.staged_at = status.staged_at.clone();
    failed
}

pub fn desired_rollout_stale_handoff_should_be_recorded(
    status: &sinomo_core::ClusterNodeRolloutStatus,
    now: OffsetDateTime,
) -> bool {
    status.status == "main-staged" && rollout_progress_status_is_stale(status, now)
}

pub fn record_desired_rollout_stale_handoff_failure_if_needed(
    rollout_paths: &crate::SelfRolloutPaths,
    status: &sinomo_core::ClusterNodeRolloutStatus,
    mode: Option<&str>,
    requested_at: Option<&str>,
    now: OffsetDateTime,
) -> Result<bool, crate::RolloutError> {
    if !desired_rollout_stale_handoff_should_be_recorded(status, now)
        || status.mode.as_deref() != mode
        || status.requested_at.as_deref() != requested_at
    {
        return Ok(false);
    }
    let failed = desired_rollout_stale_handoff_failure_status(status, now);
    crate::write_rollout_status(rollout_paths, &failed)?;
    tracing::warn!(
        "sinomo desired rollout failed after stale handoff: {}",
        failed
            .detail
            .as_deref()
            .unwrap_or("candidate main handoff did not finalize")
    );
    Ok(true)
}

pub const DESIRED_ROLLOUT_WATCH_START_TIMEOUT: Duration = Duration::from_secs(30);
pub const DESIRED_ROLLOUT_WATCH_READ_TIMEOUT: Duration = Duration::from_secs(5);
pub const DESIRED_ROLLOUT_WATCH_DRAIN_WAIT: Duration = Duration::from_secs(25);
pub const DESIRED_ROLLOUT_WATCH_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
pub const DESIRED_ROLLOUT_WATCH_CANCEL_TIMEOUT: Duration = Duration::from_secs(2);
pub const DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_BUDGET: Duration = Duration::from_millis(2500);
pub const DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_ATTEMPT: Duration = Duration::from_millis(750);
pub const DESIRED_ROLLOUT_RETRY_MIN: Duration = Duration::from_millis(250);
pub const DESIRED_ROLLOUT_RETRY_MAX: Duration = Duration::from_secs(5);
pub const DESIRED_ROLLOUT_WATCH_RECREATE_AFTER_FAILURES: u32 = 3;

pub type DesiredRolloutReactorFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Host effects required by the rollout-owned durable watch reactor.
pub trait DesiredRolloutReactorHost: Send + Sync {
    type Error: std::fmt::Display + Send;

    fn protocol_error(&self, detail: String) -> Self::Error;
    fn unavailable_error(&self, detail: String) -> Self::Error;
    fn watch_targets(&self) -> Result<Vec<ClusterEndpointRecord>, Self::Error>;

    fn start_watch<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
        prefix: &'a str,
        timeout: Duration,
    ) -> DesiredRolloutReactorFuture<'a, KvAuthoritativeRead<ClusterKvWatchRegistration>, Self::Error>;

    fn read_local_snapshot<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
        timeout: Duration,
    ) -> DesiredRolloutReactorFuture<'a, KvLocalRead<KvSnapshot>, Self::Error>;

    fn drain_watch<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchDrainRequest,
        timeout: Duration,
    ) -> DesiredRolloutReactorFuture<'a, ClusterKvWatchDrainResponse, Self::Error>;

    fn cancel_watch<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchCancelRequest,
        timeout: Duration,
    ) -> DesiredRolloutReactorFuture<'a, ClusterKvWatchCancelResponse, Self::Error>;

    fn reconcile_desired(
        &self,
        desired: Option<ClusterRolloutDesiredRecord>,
    ) -> Result<(), Self::Error>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DesiredRolloutProjection {
    initialized: bool,
    revision: Option<KvRevision>,
    generation: u64,
    encoded_desired: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesiredRolloutReconcileWork {
    pub generation: u64,
    pub desired: Option<ClusterRolloutDesiredRecord>,
}

#[derive(Clone, Debug)]
struct DesiredRolloutActiveWatch {
    endpoint: ClusterEndpointRecord,
    registration: ClusterKvWatchRegistration,
    authority_revision: KvRevision,
    caught_up: bool,
    retiring: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DesiredRolloutWaitOutcome {
    Work,
    Stopped,
}

/// Retained reactor for a process hosting durable rollout intent.
#[derive(Debug, Default)]
pub struct DesiredRolloutReactor {
    projection: DesiredRolloutProjection,
    last_reconciled_generation: u64,
    active_watch: Option<DesiredRolloutActiveWatch>,
    watch_failure_streak: u32,
    watch_retry_at: Option<Instant>,
    reconcile_failure_streak: u32,
    reconcile_retry_at: Option<Instant>,
}

impl DesiredRolloutReactor {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run_until_stopped_async<H>(
        mut self,
        host: &H,
        mut stop_rx: tokio::sync::watch::Receiver<bool>,
    ) where
        H: DesiredRolloutReactorHost,
    {
        loop {
            if stop_requested(&stop_rx) {
                break;
            }
            if let Err(error) = self.reconcile_pending(host) {
                tracing::warn!(
                    "sinomo desired rollout reconciliation deferred after failure: {error}"
                );
            }
            if stop_requested(&stop_rx) {
                break;
            }
            match self.wait_for_work_async(host, &mut stop_rx).await {
                Ok(DesiredRolloutWaitOutcome::Work) => {}
                Ok(DesiredRolloutWaitOutcome::Stopped) => break,
                Err(error) => {
                    tracing::warn!("sinomo durable rollout watch deferred after failure: {error}")
                }
            }
        }
        if let Err(error) = self.shutdown_with_retry_async(host).await {
            tracing::warn!("sinomo could not cancel its durable rollout watch: {error}");
        }
    }

    pub fn reconcile_pending<H>(&mut self, host: &H) -> Result<bool, H::Error>
    where
        H: DesiredRolloutReactorHost,
    {
        let now = Instant::now();
        if !self.reconcile_is_due(now) {
            return Ok(false);
        }
        let generation = self.projection.generation;
        let desired = match self
            .projection
            .encoded_desired
            .as_ref()
            .map(|encoded| sinomo_core::kv::decode_kv_value(encoded))
            .transpose()
        {
            Ok(desired) => desired,
            Err(error) => {
                self.record_reconcile_failure(now);
                return Err(host
                    .protocol_error(format!("failed to decode desired rollout record: {error}")));
            }
        };
        if let Err(error) = host.reconcile_desired(desired) {
            self.record_reconcile_failure(now);
            return Err(error);
        }
        self.last_reconciled_generation = self.last_reconciled_generation.max(generation);
        self.reconcile_failure_streak = 0;
        self.reconcile_retry_at = None;
        Ok(true)
    }

    async fn wait_for_work_async<H>(
        &mut self,
        host: &H,
        stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<DesiredRolloutWaitOutcome, H::Error>
    where
        H: DesiredRolloutReactorHost,
    {
        loop {
            if stop_requested(stop_rx) {
                return Ok(DesiredRolloutWaitOutcome::Stopped);
            }
            let now = Instant::now();
            if self.reconcile_is_due(now) {
                return Ok(DesiredRolloutWaitOutcome::Work);
            }
            if let Some(retry_at) = self.watch_retry_at.filter(|retry_at| *retry_at > now) {
                let wake_at = self
                    .pending_reconcile_retry_at()
                    .map(|reconcile_at| retry_at.min(reconcile_at))
                    .unwrap_or(retry_at);
                tokio::select! {
                    biased;
                    changed = stop_rx.changed() => {
                        if changed.is_err() || stop_requested(stop_rx) {
                            return Ok(DesiredRolloutWaitOutcome::Stopped);
                        }
                    }
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)) => {}
                }
                continue;
            }

            if self.active_watch.is_none() {
                if let Err(error) = self.start_watch_async(host).await {
                    self.record_watch_failure(Instant::now());
                    return Err(error);
                }
                continue;
            }

            if self
                .active_watch
                .as_ref()
                .is_some_and(|watch| watch.retiring)
            {
                if let Err(error) = self.cancel_active_watch_async(host).await {
                    self.record_watch_failure(Instant::now());
                    return Err(error);
                }
                continue;
            }

            if !self
                .active_watch
                .as_ref()
                .is_some_and(|watch| watch.caught_up)
            {
                let catch_up_result = {
                    let catch_up = self.catch_up_watch_async(host);
                    tokio::pin!(catch_up);
                    tokio::select! {
                        biased;
                        _ = stop_rx.changed() => None,
                        result = &mut catch_up => Some(result),
                    }
                };
                let Some(catch_up_result) = catch_up_result else {
                    return Ok(DesiredRolloutWaitOutcome::Stopped);
                };
                if let Err(error) = catch_up_result {
                    self.handle_active_watch_failure_async(host).await;
                    return Err(error);
                }
                return Ok(DesiredRolloutWaitOutcome::Work);
            }

            let watch = self.active_watch.as_ref().expect("active rollout watch");
            let endpoint = watch.endpoint.clone();
            let request = ClusterKvWatchDrainRequest {
                watch_id: watch.registration.watch_id,
                wait_timeout_millis: DESIRED_ROLLOUT_WATCH_DRAIN_WAIT.as_millis() as u64,
            };
            let drain = host.drain_watch(&endpoint, &request, DESIRED_ROLLOUT_WATCH_DRAIN_TIMEOUT);
            tokio::pin!(drain);
            let reconcile_retry_at = self.pending_reconcile_retry_at();
            let response = tokio::select! {
                biased;
                changed = stop_rx.changed() => {
                    if changed.is_err() || stop_requested(stop_rx) {
                        return Ok(DesiredRolloutWaitOutcome::Stopped);
                    }
                    continue;
                }
                _ = optional_deadline(reconcile_retry_at) => {
                    return Ok(DesiredRolloutWaitOutcome::Work);
                }
                response = &mut drain => response,
            };
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    self.handle_active_watch_failure_async(host).await;
                    return Err(error);
                }
            };
            if response.watch_id != request.watch_id {
                let error = host.protocol_error(format!(
                    "rollout desired watch {:?} returned response for {:?}",
                    request.watch_id, response.watch_id
                ));
                self.handle_active_watch_failure_async(host).await;
                return Err(error);
            }
            let previous_next_revision = self
                .active_watch
                .as_ref()
                .expect("active rollout watch")
                .registration
                .next_revision;
            if response.next_revision < previous_next_revision {
                let error = host.protocol_error(format!(
                    "rollout desired watch cursor regressed from {:?} to {:?}",
                    previous_next_revision, response.next_revision
                ));
                self.handle_active_watch_failure_async(host).await;
                return Err(error);
            }
            self.active_watch
                .as_mut()
                .expect("active rollout watch")
                .registration
                .next_revision = response.next_revision;
            self.watch_failure_streak = 0;
            self.watch_retry_at = None;
            if self.observe_watch_events(&response.events) {
                return Ok(DesiredRolloutWaitOutcome::Work);
            }
        }
    }

    async fn shutdown_with_retry_async<H>(&mut self, host: &H) -> Result<(), H::Error>
    where
        H: DesiredRolloutReactorHost,
    {
        if self.active_watch.is_none() {
            return Ok(());
        }
        self.mark_active_watch_retiring();
        let started = Instant::now();
        let deadline = started
            .checked_add(DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_BUDGET)
            .unwrap_or(started);
        let mut last_error = None;
        loop {
            let now = Instant::now();
            let Some(remaining) = deadline.checked_duration_since(now) else {
                break;
            };
            if remaining.is_zero() {
                break;
            }
            match self
                .cancel_active_watch_with_timeout_async(
                    host,
                    remaining.min(DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_ATTEMPT),
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    self.record_watch_failure(Instant::now());
                }
            }
            let now = Instant::now();
            let Some(remaining) = deadline.checked_duration_since(now) else {
                break;
            };
            tokio::time::sleep(remaining.min(DESIRED_ROLLOUT_RETRY_MIN)).await;
        }
        Err(last_error.unwrap_or_else(|| {
            host.unavailable_error(format!(
                "desired rollout watch cancellation did not complete within {}ms",
                DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_BUDGET.as_millis()
            ))
        }))
    }

    fn mark_active_watch_retiring(&mut self) {
        if let Some(watch) = self.active_watch.as_mut() {
            watch.retiring = true;
        }
    }

    async fn cancel_active_watch_async<H>(&mut self, host: &H) -> Result<(), H::Error>
    where
        H: DesiredRolloutReactorHost,
    {
        self.cancel_active_watch_with_timeout_async(host, DESIRED_ROLLOUT_WATCH_CANCEL_TIMEOUT)
            .await
    }

    async fn cancel_active_watch_with_timeout_async<H>(
        &mut self,
        host: &H,
        timeout: Duration,
    ) -> Result<(), H::Error>
    where
        H: DesiredRolloutReactorHost,
    {
        let watch = self.active_watch.as_ref().ok_or_else(|| {
            host.protocol_error(
                "rollout desired watch cancellation has no owned registration".to_string(),
            )
        })?;
        let endpoint = watch.endpoint.clone();
        let watch_id = watch.registration.watch_id;
        let result = host
            .cancel_watch(
                &endpoint,
                &ClusterKvWatchCancelRequest { watch_id },
                timeout,
            )
            .await;
        self.complete_active_watch_cancel(host, watch_id, result)
    }

    fn complete_active_watch_cancel<H>(
        &mut self,
        host: &H,
        expected_watch_id: WatchId,
        result: Result<ClusterKvWatchCancelResponse, H::Error>,
    ) -> Result<(), H::Error>
    where
        H: DesiredRolloutReactorHost,
    {
        let response = result?;
        if response.watch_id != expected_watch_id {
            return Err(host.protocol_error(format!(
                "rollout desired watch {:?} returned cancellation for {:?}",
                expected_watch_id, response.watch_id
            )));
        }
        if !self
            .active_watch
            .as_ref()
            .is_some_and(|watch| watch.retiring && watch.registration.watch_id == expected_watch_id)
        {
            return Err(host.protocol_error(format!(
                "rollout desired watch {:?} cancellation no longer owns the retiring registration",
                expected_watch_id
            )));
        }
        self.active_watch = None;
        self.watch_failure_streak = 0;
        self.watch_retry_at = None;
        Ok(())
    }

    async fn start_watch_async<H>(&mut self, host: &H) -> Result<(), H::Error>
    where
        H: DesiredRolloutReactorHost,
    {
        let desired_key = sinomo_core::cluster_rollout_desired_key();
        let targets = host.watch_targets()?;
        let mut last_error = None;
        for endpoint in targets {
            match host
                .start_watch(&endpoint, &desired_key, DESIRED_ROLLOUT_WATCH_START_TIMEOUT)
                .await
            {
                Ok(started) => {
                    if started.value.prefix != desired_key {
                        let unexpected_prefix = started.value.prefix.clone();
                        self.active_watch = Some(DesiredRolloutActiveWatch {
                            endpoint,
                            registration: started.value,
                            authority_revision: started.authority.revision,
                            caught_up: false,
                            retiring: true,
                        });
                        return Err(host.protocol_error(format!(
                            "rollout watch registered unexpected prefix {unexpected_prefix}"
                        )));
                    }
                    self.active_watch = Some(DesiredRolloutActiveWatch {
                        endpoint,
                        registration: started.value,
                        authority_revision: started.authority.revision,
                        caught_up: false,
                        retiring: false,
                    });
                    self.watch_failure_streak = 0;
                    self.watch_retry_at = None;
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            host.unavailable_error(
                "no reachable KV authority endpoint can host the desired rollout watch".to_string(),
            )
        }))
    }

    async fn catch_up_watch_async<H>(&mut self, host: &H) -> Result<(), H::Error>
    where
        H: DesiredRolloutReactorHost,
    {
        let watch = self.active_watch.as_ref().expect("active rollout watch");
        let endpoint = watch.endpoint.clone();
        let authority_revision = watch.authority_revision;
        let local = host
            .read_local_snapshot(&endpoint, DESIRED_ROLLOUT_WATCH_READ_TIMEOUT)
            .await?;
        if local.revision < authority_revision {
            return Err(host.protocol_error(format!(
                "rollout watch authority is at {:?}, but its local snapshot is only {:?}",
                authority_revision, local.revision
            )));
        }
        if self
            .projection
            .revision
            .is_some_and(|revision| local.revision < revision)
        {
            return Err(host.protocol_error(format!(
                "rollout watch catch-up snapshot regressed from {:?} to {:?}",
                self.projection.revision, local.revision
            )));
        }
        self.observe_authoritative_snapshot(&local.value);
        self.active_watch
            .as_mut()
            .expect("active rollout watch")
            .caught_up = true;
        self.watch_failure_streak = 0;
        self.watch_retry_at = None;
        Ok(())
    }

    pub fn observe_authoritative_snapshot(&mut self, snapshot: &KvSnapshot) -> bool {
        let encoded_desired = snapshot
            .get(&sinomo_core::cluster_rollout_desired_key())
            .map(|entry| entry.value.clone());
        let desired_changed =
            !self.projection.initialized || self.projection.encoded_desired != encoded_desired;
        self.projection.initialized = true;
        self.projection.revision = Some(snapshot.revision);
        if desired_changed {
            self.projection.encoded_desired = encoded_desired;
            self.projection.generation = self.projection.generation.saturating_add(1);
            self.reconcile_failure_streak = 0;
            self.reconcile_retry_at = None;
        }
        desired_changed
    }

    pub fn observe_watch_events(&mut self, events: &[KvEvent]) -> bool {
        let desired_key = sinomo_core::cluster_rollout_desired_key();
        let mut desired_changed = false;
        for event in events.iter().filter(|event| event.key == desired_key) {
            if self
                .projection
                .revision
                .is_some_and(|revision| event.revision <= revision)
            {
                continue;
            }
            let encoded_desired = match &event.kind {
                KvEventKind::Put { value } => Some(value.clone()),
                KvEventKind::Delete => None,
            };
            self.projection.revision = Some(event.revision);
            if self.projection.encoded_desired == encoded_desired {
                continue;
            }
            self.projection.encoded_desired = encoded_desired;
            self.projection.generation = self.projection.generation.saturating_add(1);
            self.reconcile_failure_streak = 0;
            self.reconcile_retry_at = None;
            desired_changed = true;
        }
        desired_changed
    }

    #[doc(hidden)]
    pub fn pending_reconcile_work(
        &self,
    ) -> Result<Option<DesiredRolloutReconcileWork>, sinomo_core::kv::KvError> {
        if !self.projection.initialized
            || self.projection.generation <= self.last_reconciled_generation
        {
            return Ok(None);
        }
        let desired = self
            .projection
            .encoded_desired
            .as_ref()
            .map(|encoded| sinomo_core::kv::decode_kv_value(encoded))
            .transpose()?;
        Ok(Some(DesiredRolloutReconcileWork {
            generation: self.projection.generation,
            desired,
        }))
    }

    #[doc(hidden)]
    pub fn mark_reconciled(&mut self, generation: u64) {
        self.last_reconciled_generation = self.last_reconciled_generation.max(generation);
    }

    fn reconcile_is_due(&self, now: Instant) -> bool {
        self.projection.initialized
            && self.projection.generation > self.last_reconciled_generation
            && self
                .reconcile_retry_at
                .is_none_or(|retry_at| retry_at <= now)
    }

    fn pending_reconcile_retry_at(&self) -> Option<Instant> {
        (self.projection.initialized
            && self.projection.generation > self.last_reconciled_generation)
            .then_some(self.reconcile_retry_at)
            .flatten()
    }

    fn record_reconcile_failure(&mut self, now: Instant) {
        self.reconcile_failure_streak = self.reconcile_failure_streak.saturating_add(1);
        self.reconcile_retry_at = now.checked_add(retry_delay(self.reconcile_failure_streak));
    }

    fn record_watch_failure(&mut self, now: Instant) {
        self.watch_failure_streak = self.watch_failure_streak.saturating_add(1);
        self.watch_retry_at = now.checked_add(retry_delay(self.watch_failure_streak));
    }

    async fn handle_active_watch_failure_async<H>(&mut self, host: &H)
    where
        H: DesiredRolloutReactorHost,
    {
        self.record_watch_failure(Instant::now());
        if self.watch_failure_streak < DESIRED_ROLLOUT_WATCH_RECREATE_AFTER_FAILURES
            || self.active_watch.is_none()
        {
            return;
        }
        self.mark_active_watch_retiring();
        if let Err(error) = self.cancel_active_watch_async(host).await {
            tracing::warn!(
                "sinomo retained retiring rollout watch after cancellation failure: {error}"
            );
        }
    }
}

fn retry_delay(failure_streak: u32) -> Duration {
    let shift = failure_streak.saturating_sub(1).min(8);
    DESIRED_ROLLOUT_RETRY_MIN
        .saturating_mul(1_u32 << shift)
        .min(DESIRED_ROLLOUT_RETRY_MAX)
}

fn stop_requested(stop_rx: &tokio::sync::watch::Receiver<bool>) -> bool {
    *stop_rx.borrow() || stop_rx.has_changed().is_err()
}

async fn optional_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::{InProcessKvStore, KvCommand};
    use time::OffsetDateTime;

    struct TestHost;

    impl DesiredRolloutReactorHost for TestHost {
        type Error = String;

        fn protocol_error(&self, detail: String) -> Self::Error {
            detail
        }

        fn unavailable_error(&self, detail: String) -> Self::Error {
            detail
        }

        fn watch_targets(&self) -> Result<Vec<ClusterEndpointRecord>, Self::Error> {
            unreachable!("transport is not used by state-only tests")
        }

        fn start_watch<'a>(
            &'a self,
            _endpoint: &'a ClusterEndpointRecord,
            _prefix: &'a str,
            _timeout: Duration,
        ) -> DesiredRolloutReactorFuture<
            'a,
            KvAuthoritativeRead<ClusterKvWatchRegistration>,
            Self::Error,
        > {
            Box::pin(async { unreachable!("transport is not used by state-only tests") })
        }

        fn read_local_snapshot<'a>(
            &'a self,
            _endpoint: &'a ClusterEndpointRecord,
            _timeout: Duration,
        ) -> DesiredRolloutReactorFuture<'a, KvLocalRead<KvSnapshot>, Self::Error> {
            Box::pin(async { unreachable!("transport is not used by state-only tests") })
        }

        fn drain_watch<'a>(
            &'a self,
            _endpoint: &'a ClusterEndpointRecord,
            _request: &'a ClusterKvWatchDrainRequest,
            _timeout: Duration,
        ) -> DesiredRolloutReactorFuture<'a, ClusterKvWatchDrainResponse, Self::Error> {
            Box::pin(async { unreachable!("transport is not used by state-only tests") })
        }

        fn cancel_watch<'a>(
            &'a self,
            _endpoint: &'a ClusterEndpointRecord,
            _request: &'a ClusterKvWatchCancelRequest,
            _timeout: Duration,
        ) -> DesiredRolloutReactorFuture<'a, ClusterKvWatchCancelResponse, Self::Error> {
            Box::pin(async { unreachable!("transport is not used by state-only tests") })
        }

        fn reconcile_desired(
            &self,
            _desired: Option<ClusterRolloutDesiredRecord>,
        ) -> Result<(), Self::Error> {
            unreachable!("reconciliation is not used by state-only tests")
        }
    }

    fn desired(source: &str) -> ClusterRolloutDesiredRecord {
        ClusterRolloutDesiredRecord {
            cluster_id: sinomo_core::Id::generate(),
            state: "active".to_string(),
            source: Some(source.to_string()),
            mode: Some("cluster-remote-build".to_string()),
            source_head: Some("abc123".to_string()),
            source_bundle: Some(vec![1, 2, 3]),
            force_source_overwrite: false,
            remote_repo: None,
            host: None,
            port: None,
            shadow_port: None,
            requested_at: None,
            updated_at: OffsetDateTime::now_utc(),
        }
    }

    #[test]
    fn newer_desired_change_remains_pending_after_older_generation_completes() {
        let mut store = InProcessKvStore::new();
        let first_desired = desired("first");
        store
            .apply_command(KvCommand::Op(
                crate::build_cluster_rollout_desired_put_op(&first_desired).expect("desired put"),
            ))
            .expect("apply desired");
        let initial = store.snapshot().expect("snapshot");
        let mut reactor = DesiredRolloutReactor::new();
        assert!(reactor.observe_authoritative_snapshot(&initial));
        let first = reactor
            .pending_reconcile_work()
            .expect("work")
            .expect("desired");

        let second_desired = desired("second");
        assert!(reactor.observe_watch_events(&[KvEvent {
            revision: initial.revision.next(),
            key: sinomo_core::cluster_rollout_desired_key(),
            kind: KvEventKind::Put {
                value: sinomo_core::kv::encode_kv_value(&second_desired).expect("encode desired"),
            },
        }]));
        reactor.mark_reconciled(first.generation);

        let next = reactor
            .pending_reconcile_work()
            .expect("work")
            .expect("new desired");
        assert!(next.generation > first.generation);
        assert_eq!(next.desired, Some(second_desired));
    }

    #[test]
    fn unrelated_events_do_not_retrigger_reconciliation() {
        let mut store = InProcessKvStore::new();
        store
            .apply_command(KvCommand::Op(
                crate::build_cluster_rollout_desired_put_op(&desired("stable"))
                    .expect("desired put"),
            ))
            .expect("apply desired");
        let snapshot = store.snapshot().expect("snapshot");
        let mut reactor = DesiredRolloutReactor::new();
        reactor.observe_authoritative_snapshot(&snapshot);
        let work = reactor
            .pending_reconcile_work()
            .expect("work")
            .expect("desired");
        reactor.mark_reconciled(work.generation);

        assert!(!reactor.observe_watch_events(&[KvEvent {
            revision: snapshot.revision.next(),
            key: "/cluster/unrelated".to_string(),
            kind: KvEventKind::Put { value: vec![9] },
        }]));
        assert_eq!(reactor.pending_reconcile_work().expect("work"), None);
    }

    #[test]
    fn cancel_failure_retains_watch_until_authority_proves_it_absent() {
        assert!(
            DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_BUDGET
                >= DESIRED_ROLLOUT_WATCH_SHUTDOWN_CANCEL_ATTEMPT * 2 + DESIRED_ROLLOUT_RETRY_MIN,
            "shutdown budget must permit at least one cancellation retry"
        );
        let watch_id = WatchId(41);
        let mut reactor = DesiredRolloutReactor::new();
        reactor.active_watch = Some(DesiredRolloutActiveWatch {
            endpoint: sinomo_core::network::daemon_loopback_endpoint(
                &sinomo_core::node::NodeId::generate(),
                "127.0.0.1:1".to_string(),
                0,
            ),
            registration: ClusterKvWatchRegistration {
                watch_id,
                prefix: sinomo_core::cluster_rollout_desired_key(),
                next_revision: KvRevision(2),
            },
            authority_revision: KvRevision(1),
            caught_up: true,
            retiring: false,
        });
        reactor.watch_failure_streak = 3;
        reactor.watch_retry_at = Instant::now().checked_add(Duration::from_secs(1));
        reactor.mark_active_watch_retiring();

        assert!(
            reactor
                .complete_active_watch_cancel(
                    &TestHost,
                    watch_id,
                    Err("injected cancel failure".to_string()),
                )
                .is_err()
        );
        let retained = reactor
            .active_watch
            .as_ref()
            .expect("failed cancellation retains registration");
        assert_eq!(retained.registration.watch_id, watch_id);
        assert!(retained.retiring);
        assert_eq!(reactor.watch_failure_streak, 3);

        reactor
            .complete_active_watch_cancel(
                &TestHost,
                watch_id,
                Ok(ClusterKvWatchCancelResponse {
                    watch_id,
                    cancelled: false,
                }),
            )
            .expect("retry proves watch absent");
        assert!(reactor.active_watch.is_none());
        assert_eq!(reactor.watch_failure_streak, 0);
        assert_eq!(reactor.watch_retry_at, None);
    }
}
