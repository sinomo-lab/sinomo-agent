//! Bounded retries for DFS browser reads.

use std::time::{Duration, Instant};

/// Compatibility classifier for reads rejected at admission or discarded
/// because the shard assignment changed while the read was in flight.
pub fn dfs_browser_read_should_retry(error: &str) -> bool {
    dfs_browser_admission_is_busy(error)
        || (error.contains("routed namespace response authority is stale: distributed DFS error: dispatch authority for namespace shard ")
            && error.contains(" is no longer current (current primary "))
}

pub fn dfs_browser_admission_is_busy(error: &str) -> bool {
    (error.contains("DFS browser service is busy") && error.contains("retry the read"))
        || (error.contains("object rpc route /daemon/dfs/browser/")
            && error.contains(" is saturated:"))
}

/// Each new read receives the remainder of the original RPC deadline and
/// resolves its current owner. The three-second retry budget does not shorten
/// an admitted content fetch.
pub fn retry_dfs_browser_read<T>(
    timeout: Duration,
    read: impl FnMut(Duration) -> Result<T, String>,
) -> Result<T, String> {
    let started = Instant::now();
    retry_with_clock(timeout, read, || started.elapsed(), std::thread::sleep)
}

fn retry_with_clock<T>(
    timeout: Duration,
    mut read: impl FnMut(Duration) -> Result<T, String>,
    elapsed: impl Fn() -> Duration,
    mut sleep: impl FnMut(Duration),
) -> Result<T, String> {
    let budget = timeout.min(Duration::from_secs(3));
    let mut delay = Duration::from_millis(25);
    loop {
        match read(timeout.saturating_sub(elapsed())) {
            Ok(value) => return Ok(value),
            Err(error) if dfs_browser_read_should_retry(&error) => {
                let remaining = budget.saturating_sub(elapsed());
                if remaining.is_zero() {
                    return Err(error);
                }
                sleep(delay.min(remaining));
                if elapsed() >= budget {
                    return Err(error);
                }
                delay = (delay * 2).min(Duration::from_millis(250));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const BUSY: &str = "DFS browser service is busy after waiting 100ms; retry the read";

    #[test]
    fn read_reissues_a_result_discarded_after_its_shard_assignment_changed() {
        let stale = "routed namespace response authority is stale: distributed DFS error: dispatch authority for namespace shard services epoch 2 is no longer current (current primary node-a, epoch 2)";
        let mut replies = [Err(stale.to_owned()), Ok("current listing")].into_iter();
        assert_eq!(
            retry_with_clock(
                Duration::from_secs(1),
                |_| replies.next().unwrap(),
                || Duration::ZERO,
                |_| {},
            ),
            Ok("current listing")
        );
        assert!(!dfs_browser_admission_is_busy(stale));
        assert!(!dfs_browser_read_should_retry(
            "routed namespace response authority is stale: clustered namespace response has no dispatch shard assignment"
        ));
    }

    #[test]
    fn read_recovers_from_wrapped_backpressure_with_one_original_deadline() {
        let elapsed = Cell::new(Duration::ZERO);
        let mut deadlines = Vec::new();
        let result = retry_with_clock(
            Duration::from_secs(1),
            |remaining| {
                deadlines.push(remaining);
                elapsed.set(elapsed.get() + Duration::from_millis(100));
                if deadlines.len() < 3 {
                    Err(format!("daemon-forwarded network request failed: {BUSY}"))
                } else {
                    Ok(b"committed winner".to_vec())
                }
            },
            || elapsed.get(),
            |delay| elapsed.set(elapsed.get() + delay),
        )
        .unwrap();
        assert_eq!(result, b"committed winner");
        assert_eq!(
            deadlines,
            [
                Duration::from_millis(1000),
                Duration::from_millis(875),
                Duration::from_millis(725)
            ]
        );
    }

    #[test]
    fn busy_read_stops_at_the_shorter_caller_deadline() {
        let elapsed = Cell::new(Duration::ZERO);
        let mut attempts = 0;
        let error = retry_with_clock::<()>(
            Duration::from_millis(40),
            |_| {
                attempts += 1;
                Err(BUSY.into())
            },
            || elapsed.get(),
            |delay| elapsed.set(elapsed.get() + delay),
        )
        .unwrap_err();
        assert_eq!(error, BUSY);
        assert_eq!(elapsed.get(), Duration::from_millis(40));
        assert_eq!(attempts, 2);
    }

    #[test]
    fn saturated_read_has_a_bounded_admission_budget() {
        let elapsed = Cell::new(Duration::ZERO);
        let mut attempts = 0;
        let error = "object rpc route /daemon/dfs/browser/list is saturated: too many requests";
        let result = retry_with_clock::<()>(
            Duration::from_secs(30),
            |_| {
                attempts += 1;
                Err(error.into())
            },
            || elapsed.get(),
            |delay| elapsed.set(elapsed.get() + delay),
        );
        assert_eq!(result.unwrap_err(), error);
        assert_eq!(elapsed.get(), Duration::from_secs(3));
        assert!(attempts < 20);
    }

    #[test]
    fn admitted_read_retains_its_content_fetch_timeout_and_other_errors_are_not_retried() {
        let value = retry_with_clock(
            Duration::from_secs(30),
            |remaining| {
                assert_eq!(remaining, Duration::from_secs(30));
                Ok(7)
            },
            || Duration::ZERO,
            |_| panic!("admitted read must not sleep"),
        )
        .unwrap();
        assert_eq!(value, 7);
        for error in [
            "SIFS path not found",
            "base version conflict",
            "metadata shard is frozen",
            "network timeout (PossiblyApplied)",
        ] {
            assert_eq!(
                retry_with_clock::<()>(
                    Duration::from_secs(30),
                    |_| Err(error.into()),
                    || Duration::ZERO,
                    |_| panic!("non-admission failure must not retry")
                )
                .unwrap_err(),
                error
            );
        }
    }
}
