use crate::{DEFAULT_NETWORK_TIMEOUT, DaemonRpcTimeouts};
use std::time::Duration;

/// Health limits for one network path. These limits restart after validated
/// progress and never act as an implicit whole-call deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkHealthPolicy {
    pub connect_timeout: Duration,
    pub no_progress_timeout: Duration,
}

impl NetworkHealthPolicy {
    pub const fn new(connect_timeout: Duration, no_progress_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            no_progress_timeout,
        }
    }
}

impl Default for NetworkHealthPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_NETWORK_TIMEOUT, DEFAULT_NETWORK_TIMEOUT)
    }
}

impl From<NetworkHealthPolicy> for DaemonRpcTimeouts {
    fn from(policy: NetworkHealthPolicy) -> Self {
        Self {
            connect_timeout: Some(policy.connect_timeout),
            idle_timeout: Some(policy.no_progress_timeout),
        }
    }
}

/// Lifetime policy for an RPC call. Calls are unbounded by default while the
/// health policy continues to observe meaningful progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct NetworkCallPolicy {
    pub health: NetworkHealthPolicy,
    pub overall_timeout: Option<Duration>,
}

impl NetworkCallPolicy {
    pub const fn with_overall_timeout(mut self, timeout: Duration) -> Self {
        self.overall_timeout = Some(timeout);
        self
    }
}
