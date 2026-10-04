//! Context propagation at service blocking-work boundaries.

/// Wrap a blocking job before handing it to an executor. The job retains the
/// caller's tracing dispatcher and span ancestry across the thread boundary.
/// Queue/work lifetimes are recorded only when `sinomo.perf` tracing is enabled.
/// `completed` describes execution, not the success of an arbitrary return type.
/// The caller continues to own scheduling, cancellation and durable capabilities.
pub fn observed_blocking_task<F, T>(owner: &'static str, task: F) -> impl FnOnce() -> T + Send
where
    F: FnOnce() -> T + Send,
{
    let dispatcher = tracing::dispatcher::get_default(Clone::clone);
    let parent = tracing::Span::current();
    let span = tracing::trace_span!(target: "sinomo.perf", parent: &parent, "service.blocking",
        stage = "service.blocking", owner, completed = false);
    let queue = tracing::trace_span!(target: "sinomo.perf", parent: &span,
        "service.blocking.queue", stage = "service.blocking.queue", owner, completed = false);
    move || {
        tracing::dispatcher::with_default(&dispatcher, || {
            parent.in_scope(|| {
                let _entered = span.enter();
                queue.record("completed", true);
                drop(queue);
                let work = tracing::trace_span!(target: "sinomo.perf", "service.blocking.work",
            stage = "service.blocking.work", owner, completed = false);
                let result = work.in_scope(task);
                work.record("completed", true);
                span.record("completed", true);
                result
            })
        })
    }
}
