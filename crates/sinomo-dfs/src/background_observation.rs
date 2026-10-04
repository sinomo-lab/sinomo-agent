//! Bounded-label accounting for background work, independent of trace retention.

use std::time::Instant;

pub(crate) struct DfsBackgroundObservation {
    worker: &'static str,
    phase: &'static str,
    outcome: &'static str,
    started: Instant,
}

impl DfsBackgroundObservation {
    pub(crate) fn new(worker: &'static str, phase: &'static str) -> Self {
        metrics::counter!("sinomo.dfs.background.started", "worker" => worker, "phase" => phase)
            .increment(1);
        Self {
            worker,
            phase,
            outcome: "unfinished",
            started: Instant::now(),
        }
    }

    pub(crate) fn finish(&mut self, outcome: &'static str) {
        self.outcome = outcome;
        tracing::Span::current().record("outcome", outcome);
    }
}

impl Drop for DfsBackgroundObservation {
    fn drop(&mut self) {
        metrics::counter!("sinomo.dfs.background.completed",
            "worker" => self.worker, "phase" => self.phase, "outcome" => self.outcome)
        .increment(1);
        metrics::histogram!("sinomo.dfs.background.duration_seconds",
            "worker" => self.worker, "phase" => self.phase, "outcome" => self.outcome)
        .record(self.started.elapsed().as_secs_f64());
    }
}

pub(crate) fn hydration_items(kind: &'static str, count: usize) {
    metrics::counter!("sinomo.dfs.background.items", "worker" => "hydration", "kind" => kind)
        .increment(count as u64);
}
