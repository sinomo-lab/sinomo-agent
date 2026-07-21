//! Reusable measurements for one network path.
//!
//! The estimator is transport-neutral: callers feed it round-trip and delivery
//! samples, then consume a smoothed bandwidth-delay product (BDP). Object RPC
//! uses the same public interface as higher-level crates; it does not hide a
//! second transfer-specific estimator.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

const RTT_NEW_SAMPLE_WEIGHT: u64 = 8;
const BANDWIDTH_NEW_SAMPLE_WEIGHT: u64 = 4;
const SHARED_NETWORK_PATH_CAPACITY: usize = 4_096;
const SHARED_NETWORK_PATH_OVERFLOW_BUCKETS: usize = 64;
pub(crate) const NETWORK_PEER_RECEIVE_CREDIT_BYTES: usize = 256 * 1024 * 1024;

/// Stable key for measurements shared by users of `sinomo-network`.
///
/// Endpoint-aware callers should include the transport and endpoint in the
/// key. Canonical object RPC currently uses the immediate peer public key, so
/// all calls to that peer can reuse observations across sessions.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NetworkPathKey(Arc<str>);

impl NetworkPathKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Arc::from(value.into()))
    }

    pub fn for_peer_public_key(public_key: &[u8]) -> Self {
        Self::new(format!(
            "peer:ed25519:{}",
            URL_SAFE_NO_PAD.encode(public_key)
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Latest smoothed observations for one path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NetworkPathEstimate {
    pub round_trip_time: Option<Duration>,
    pub bandwidth_bytes_per_second: Option<u64>,
    pub bandwidth_delay_product_bytes: Option<u64>,
    pub round_trip_samples: u64,
    pub bandwidth_samples: u64,
}

impl NetworkPathEstimate {
    /// Returns `multiplier × BDP`, bounded by the caller's resource policy.
    /// If either half of BDP has not been measured yet, `minimum_bytes` is
    /// returned.
    pub fn recommended_window_bytes(
        self,
        multiplier: u32,
        minimum_bytes: u64,
        maximum_bytes: u64,
    ) -> u64 {
        self.recommended_window_bytes_with_default_rtt(
            multiplier,
            minimum_bytes,
            maximum_bytes,
            None,
        )
    }

    /// Like [`Self::recommended_window_bytes`], but permits a conservative RTT
    /// until an active probe or transfer handshake produces the first sample.
    /// If the bounds are equal or reversed, the maximum remains authoritative
    /// as the caller's resource cap.
    pub fn recommended_window_bytes_with_default_rtt(
        self,
        multiplier: u32,
        minimum_bytes: u64,
        maximum_bytes: u64,
        default_round_trip_time: Option<Duration>,
    ) -> u64 {
        if maximum_bytes <= minimum_bytes {
            return maximum_bytes;
        }
        let bdp = self.bandwidth_delay_product_bytes.or_else(|| {
            let bandwidth = self.bandwidth_bytes_per_second?;
            bandwidth_delay_product_bytes(bandwidth, default_round_trip_time?)
        });
        let Some(bdp) = bdp else {
            return minimum_bytes;
        };
        bdp.saturating_mul(u64::from(multiplier.max(1)))
            .clamp(minimum_bytes, maximum_bytes)
    }
}

#[derive(Debug, Default)]
struct NetworkPathEstimatorState {
    round_trip_time_nanos: Option<u64>,
    bandwidth_bytes_per_second: Option<u64>,
    round_trip_samples: u64,
    bandwidth_samples: u64,
}

/// Thread-safe EWMA estimator for RTT, delivery bandwidth, and BDP.
#[derive(Clone, Default)]
pub struct NetworkPathEstimator {
    state: Arc<Mutex<NetworkPathEstimatorState>>,
}

impl fmt::Debug for NetworkPathEstimator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetworkPathEstimator")
            .field("estimate", &self.snapshot())
            .finish()
    }
}

impl NetworkPathEstimator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_round_trip_time(&self, sample: Duration) {
        let sample_nanos = u64::try_from(sample.as_nanos()).unwrap_or(u64::MAX).max(1);
        if let Ok(mut state) = self.state.lock() {
            state.round_trip_time_nanos = Some(ewma(
                state.round_trip_time_nanos,
                sample_nanos,
                RTT_NEW_SAMPLE_WEIGHT,
            ));
            state.round_trip_samples = state.round_trip_samples.saturating_add(1);
        }
    }

    /// Records bytes delivered during an observed wall-clock interval.
    pub fn record_delivery(&self, delivered_bytes: u64, elapsed: Duration) {
        if delivered_bytes == 0 || elapsed.is_zero() {
            return;
        }
        let elapsed_nanos = elapsed.as_nanos().max(1);
        let bytes_per_second = u64::try_from(
            u128::from(delivered_bytes)
                .saturating_mul(1_000_000_000)
                .checked_div(elapsed_nanos)
                .unwrap_or(0),
        )
        .unwrap_or(u64::MAX)
        .max(1);
        self.record_bandwidth_bytes_per_second(bytes_per_second);
    }

    pub fn record_bandwidth_bytes_per_second(&self, sample: u64) {
        if sample == 0 {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            state.bandwidth_bytes_per_second = Some(ewma(
                state.bandwidth_bytes_per_second,
                sample,
                BANDWIDTH_NEW_SAMPLE_WEIGHT,
            ));
            state.bandwidth_samples = state.bandwidth_samples.saturating_add(1);
        }
    }

    pub fn snapshot(&self) -> NetworkPathEstimate {
        let Ok(state) = self.state.lock() else {
            return NetworkPathEstimate::default();
        };
        let round_trip_time = state.round_trip_time_nanos.map(Duration::from_nanos);
        NetworkPathEstimate {
            round_trip_time,
            bandwidth_bytes_per_second: state.bandwidth_bytes_per_second,
            bandwidth_delay_product_bytes: state
                .bandwidth_bytes_per_second
                .and_then(|bandwidth| bandwidth_delay_product_bytes(bandwidth, round_trip_time?)),
            round_trip_samples: state.round_trip_samples,
            bandwidth_samples: state.bandwidth_samples,
        }
    }

    fn registry_only(&self) -> bool {
        Arc::strong_count(&self.state) == 1
    }
}

fn bandwidth_delay_product_bytes(bandwidth: u64, round_trip_time: Duration) -> Option<u64> {
    u64::try_from(
        u128::from(bandwidth)
            .saturating_mul(round_trip_time.as_nanos())
            .checked_div(1_000_000_000)?,
    )
    .ok()
}

fn ewma(previous: Option<u64>, sample: u64, new_sample_weight: u64) -> u64 {
    let Some(previous) = previous else {
        return sample;
    };
    let old_weight = new_sample_weight.saturating_sub(1);
    u64::try_from(
        u128::from(previous)
            .saturating_mul(u128::from(old_weight))
            .saturating_add(u128::from(sample))
            / u128::from(new_sample_weight.max(1)),
    )
    .unwrap_or(u64::MAX)
}

#[derive(Debug)]
struct NetworkPeerReceiveCredit {
    semaphore: Arc<Semaphore>,
    limit_bytes: usize,
}

impl NetworkPeerReceiveCredit {
    fn new(limit_bytes: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limit_bytes)),
            limit_bytes,
        }
    }

    async fn reserve(
        &self,
        bytes: u64,
    ) -> Result<NetworkPeerReceiveCreditLease, crate::NetworkError> {
        let bytes = usize::try_from(bytes).map_err(|_| crate::NetworkError::ResourceExhausted {
            operation: "reserve peer receive credit".to_string(),
            requested_bytes: usize::MAX,
        })?;
        if bytes == 0 || bytes > self.limit_bytes || bytes > u32::MAX as usize {
            return Err(crate::NetworkError::ResourceExhausted {
                operation: "reserve peer receive credit".to_string(),
                requested_bytes: bytes,
            });
        }
        let permit = Arc::clone(&self.semaphore)
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| crate::NetworkError::Io {
                operation: "reserve peer receive credit".to_string(),
                detail: "peer receive credit budget is closed".to_string(),
            })?;
        Ok(NetworkPeerReceiveCreditLease {
            semaphore: Arc::clone(&self.semaphore),
            permits: vec![permit],
            reserved_bytes: bytes as u64,
            limit_bytes: self.limit_bytes as u64,
        })
    }
}

#[derive(Debug)]
pub(crate) struct NetworkPeerReceiveCreditLease {
    semaphore: Arc<Semaphore>,
    permits: Vec<OwnedSemaphorePermit>,
    reserved_bytes: u64,
    limit_bytes: u64,
}

impl NetworkPeerReceiveCreditLease {
    pub(crate) fn try_grow_to(&mut self, desired_bytes: u64) -> u64 {
        let desired_bytes = desired_bytes.min(self.limit_bytes);
        let additional = desired_bytes.saturating_sub(self.reserved_bytes);
        let Ok(additional) = u32::try_from(additional) else {
            return self.reserved_bytes;
        };
        if additional == 0 {
            return self.reserved_bytes;
        }
        match Arc::clone(&self.semaphore).try_acquire_many_owned(additional) {
            Ok(permit) => {
                self.permits.push(permit);
                self.reserved_bytes = self.reserved_bytes.saturating_add(u64::from(additional));
            }
            Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {}
        }
        self.reserved_bytes
    }
}

#[derive(Clone, Debug)]
struct SharedNetworkPathResources {
    estimator: NetworkPathEstimator,
    receive_credit: Arc<NetworkPeerReceiveCredit>,
}

impl SharedNetworkPathResources {
    fn new() -> Self {
        Self {
            estimator: NetworkPathEstimator::new(),
            receive_credit: Arc::new(NetworkPeerReceiveCredit::new(
                NETWORK_PEER_RECEIVE_CREDIT_BYTES,
            )),
        }
    }

    fn registry_only(&self) -> bool {
        self.estimator.registry_only() && Arc::strong_count(&self.receive_credit) == 1
    }
}

#[derive(Debug)]
struct SharedNetworkPathEntry {
    resources: SharedNetworkPathResources,
    last_used: u64,
}

#[derive(Debug)]
struct SharedNetworkPathRegistry {
    entries: HashMap<NetworkPathKey, SharedNetworkPathEntry>,
    overflow: Vec<SharedNetworkPathResources>,
    clock: u64,
}

impl SharedNetworkPathRegistry {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            overflow: (0..SHARED_NETWORK_PATH_OVERFLOW_BUCKETS)
                .map(|_| SharedNetworkPathResources::new())
                .collect(),
            clock: 0,
        }
    }

    fn resources(&mut self, key: &NetworkPathKey) -> SharedNetworkPathResources {
        self.clock = self.clock.wrapping_add(1).max(1);
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_used = self.clock;
            return entry.resources.clone();
        }
        if self.entries.len() >= SHARED_NETWORK_PATH_CAPACITY {
            let evict = self
                .entries
                .iter()
                .filter(|(_, entry)| entry.resources.registry_only())
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            if let Some(evict) = evict {
                self.entries.remove(&evict);
            } else {
                let bucket = path_overflow_bucket(key.as_str(), self.overflow.len());
                return self.overflow[bucket].clone();
            }
        }
        let resources = SharedNetworkPathResources::new();
        self.entries.insert(
            key.clone(),
            SharedNetworkPathEntry {
                resources: resources.clone(),
                last_used: self.clock,
            },
        );
        resources
    }
}

fn path_overflow_bucket(key: &str, buckets: usize) -> usize {
    let hash = key
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            hash.wrapping_mul(0x0000_0100_0000_01b3) ^ u64::from(*byte)
        });
    usize::try_from(hash).unwrap_or(0) % buckets.max(1)
}

fn shared_network_path_registry() -> &'static Mutex<SharedNetworkPathRegistry> {
    static REGISTRY: OnceLock<Mutex<SharedNetworkPathRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(SharedNetworkPathRegistry::new()))
}

fn shared_network_path_resources(key: &NetworkPathKey) -> SharedNetworkPathResources {
    shared_network_path_registry()
        .lock()
        .map(|mut registry| registry.resources(key))
        .unwrap_or_else(|_| SharedNetworkPathResources::new())
}

/// Returns the process-shared estimator for a stable network path key.
pub fn shared_network_path_estimator(key: &NetworkPathKey) -> NetworkPathEstimator {
    shared_network_path_resources(key).estimator
}

pub(crate) async fn reserve_shared_network_peer_receive_credit(
    key: &NetworkPathKey,
    bytes: u64,
) -> Result<NetworkPeerReceiveCreditLease, crate::NetworkError> {
    shared_network_path_resources(key)
        .receive_credit
        .reserve(bytes)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommended_window_uses_measured_bdp() {
        let estimator = NetworkPathEstimator::new();
        estimator.record_round_trip_time(Duration::from_millis(2));
        estimator.record_bandwidth_bytes_per_second(10_000_000_000);
        let estimate = estimator.snapshot();

        assert_eq!(estimate.bandwidth_delay_product_bytes, Some(20_000_000));
        assert_eq!(
            estimate.recommended_window_bytes(2, 16 * 1024 * 1024, 64 * 1024 * 1024),
            40_000_000
        );
    }

    #[test]
    fn recommended_window_honors_maximum_when_bounds_are_reversed() {
        let estimate = NetworkPathEstimate {
            bandwidth_delay_product_bytes: Some(32 * 1024 * 1024),
            ..NetworkPathEstimate::default()
        };

        assert_eq!(
            estimate.recommended_window_bytes(2, 64 * 1024 * 1024, 16 * 1024 * 1024),
            16 * 1024 * 1024
        );
        assert_eq!(
            estimate.recommended_window_bytes_with_default_rtt(
                2,
                64 * 1024 * 1024,
                16 * 1024 * 1024,
                Some(Duration::from_millis(2)),
            ),
            16 * 1024 * 1024
        );
    }

    #[test]
    fn recommended_window_returns_equal_bound() {
        let estimate = NetworkPathEstimate {
            bandwidth_delay_product_bytes: Some(32 * 1024 * 1024),
            ..NetworkPathEstimate::default()
        };

        assert_eq!(
            estimate.recommended_window_bytes(2, 8 * 1024 * 1024, 8 * 1024 * 1024),
            8 * 1024 * 1024
        );
    }

    #[test]
    fn estimator_can_use_default_rtt_until_the_first_probe() {
        let estimator = NetworkPathEstimator::new();
        estimator.record_delivery(10_000_000, Duration::from_millis(1));
        let estimate = estimator.snapshot();

        assert_eq!(estimate.round_trip_time, None);
        assert_eq!(
            estimate.recommended_window_bytes_with_default_rtt(
                2,
                16 * 1024 * 1024,
                64 * 1024 * 1024,
                Some(Duration::from_millis(2)),
            ),
            40_000_000
        );
    }

    #[test]
    fn recommended_window_uses_minimum_without_an_estimate() {
        let estimate = NetworkPathEstimator::new().snapshot();

        assert_eq!(
            estimate.recommended_window_bytes_with_default_rtt(
                2,
                16 * 1024 * 1024,
                64 * 1024 * 1024,
                Some(Duration::from_millis(2)),
            ),
            16 * 1024 * 1024
        );
    }

    #[test]
    fn shared_estimator_reuses_the_same_path_state() {
        let key = NetworkPathKey::new(format!("test-path:{}", sinomo_base::Id::generate()));
        let first = shared_network_path_estimator(&key);
        first.record_round_trip_time(Duration::from_millis(7));
        let second = shared_network_path_estimator(&key);

        assert_eq!(
            second.snapshot().round_trip_time,
            Some(Duration::from_millis(7))
        );
    }

    #[tokio::test]
    async fn peer_receive_credit_bounds_aggregate_outstanding_windows() {
        let budget = NetworkPeerReceiveCredit::new(64 * 1024 * 1024);
        let first = budget.reserve(48 * 1024 * 1024).await.expect("first lease");
        let second = budget
            .reserve(16 * 1024 * 1024)
            .await
            .expect("second lease");
        assert!(
            tokio::time::timeout(Duration::from_millis(5), budget.reserve(16 * 1024 * 1024),)
                .await
                .is_err(),
            "aggregate credit must wait at the peer cap"
        );
        drop(first);
        budget
            .reserve(16 * 1024 * 1024)
            .await
            .expect("released peer credit is reusable");
        drop(second);
    }
}
