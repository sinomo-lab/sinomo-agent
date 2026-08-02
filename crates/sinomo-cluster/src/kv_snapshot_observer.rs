use crate::kv_local_store::ClusterKvPathsExt;
use sinomo_kv::{ClusterKvCommittedDelta, KvSnapshot};
use sinomo_network::ClusterPaths;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, Weak};

type ClusterKvCommittedSnapshotCallback = dyn Fn(&KvSnapshot) + Send + Sync + 'static;
type ClusterKvCommittedDeltaCallback = dyn Fn(&ClusterKvCommittedDelta) + Send + Sync + 'static;

static CLUSTER_KV_COMMITTED_SNAPSHOT_OBSERVERS: LazyLock<
    Mutex<HashMap<PathBuf, Vec<Weak<ClusterKvCommittedSnapshotCallback>>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_COMMITTED_DELTA_OBSERVERS: LazyLock<
    Mutex<HashMap<PathBuf, Vec<Weak<ClusterKvCommittedDeltaCallback>>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Keeps one path-scoped committed cluster-KV snapshot callback registered.
///
/// Registration is intentionally generic: consumers decide which exact
/// projection of the committed snapshot matters to them. The cluster layer
/// only publishes snapshots after its local committed/read-model view changes.
pub struct ClusterKvCommittedSnapshotSubscription {
    _callback: Arc<ClusterKvCommittedSnapshotCallback>,
}

pub struct ClusterKvCommittedDeltaSubscription {
    _callback: Arc<ClusterKvCommittedDeltaCallback>,
}

impl std::fmt::Debug for ClusterKvCommittedDeltaSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterKvCommittedDeltaSubscription")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ClusterKvCommittedSnapshotSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterKvCommittedSnapshotSubscription")
            .finish_non_exhaustive()
    }
}

pub fn subscribe_cluster_kv_committed_deltas(
    paths: &ClusterPaths,
    callback: impl Fn(&ClusterKvCommittedDelta) + Send + Sync + 'static,
) -> ClusterKvCommittedDeltaSubscription {
    let callback: Arc<ClusterKvCommittedDeltaCallback> = Arc::new(callback);
    let key = cluster_kv_observer_key(paths);
    if let Ok(mut observers) = CLUSTER_KV_COMMITTED_DELTA_OBSERVERS.lock() {
        let callbacks = observers.entry(key).or_default();
        callbacks.retain(|callback| callback.strong_count() > 0);
        callbacks.push(Arc::downgrade(&callback));
    }
    ClusterKvCommittedDeltaSubscription {
        _callback: callback,
    }
}

/// Subscribes to committed cluster-KV snapshots observed at `paths`.
///
/// The callback is synchronous and must stay bounded. It is invoked after the
/// commit/read-model operation has completed and after the global observer
/// registry lock has been released. Dropping the returned subscription makes
/// the callback eligible for removal at the next notification.
pub fn subscribe_cluster_kv_committed_snapshots(
    paths: &ClusterPaths,
    callback: impl Fn(&KvSnapshot) + Send + Sync + 'static,
) -> ClusterKvCommittedSnapshotSubscription {
    let callback: Arc<ClusterKvCommittedSnapshotCallback> = Arc::new(callback);
    let key = cluster_kv_observer_key(paths);
    if let Ok(mut observers) = CLUSTER_KV_COMMITTED_SNAPSHOT_OBSERVERS.lock() {
        let callbacks = observers.entry(key).or_default();
        callbacks.retain(|callback| callback.strong_count() > 0);
        callbacks.push(Arc::downgrade(&callback));
    }
    ClusterKvCommittedSnapshotSubscription {
        _callback: callback,
    }
}

pub(crate) fn notify_cluster_kv_committed_snapshot_observers(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) {
    let callbacks = {
        let Ok(mut observers) = CLUSTER_KV_COMMITTED_SNAPSHOT_OBSERVERS.lock() else {
            return;
        };
        let key = cluster_kv_observer_key(paths);
        let Some(registered) = observers.get_mut(&key) else {
            return;
        };
        let callbacks = registered
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        registered.retain(|callback| callback.strong_count() > 0);
        if registered.is_empty() {
            observers.remove(&key);
        }
        callbacks
    };
    for callback in callbacks {
        callback(snapshot);
    }
}

pub(crate) fn notify_cluster_kv_committed_delta_observers(
    paths: &ClusterPaths,
    delta: &ClusterKvCommittedDelta,
) {
    let callbacks = {
        let Ok(mut observers) = CLUSTER_KV_COMMITTED_DELTA_OBSERVERS.lock() else {
            return;
        };
        let key = cluster_kv_observer_key(paths);
        let Some(registered) = observers.get_mut(&key) else {
            return;
        };
        let callbacks = registered
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        registered.retain(|callback| callback.strong_count() > 0);
        if registered.is_empty() {
            observers.remove(&key);
        }
        callbacks
    };
    for callback in callbacks {
        callback(delta);
    }
}

fn cluster_kv_observer_key(paths: &ClusterPaths) -> PathBuf {
    paths.kv_paths().snapshot
}
