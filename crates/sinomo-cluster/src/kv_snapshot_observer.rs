use crate::kv_local_store::ClusterKvPathsExt;
use sinomo_kv::{
    ClusterKvCommittedDelta, KvCommittedObservation, KvCommittedObservationValue, KvSnapshot,
};
use sinomo_network::ClusterPaths;

/// One lifecycle-owned, ordered observation subscription. Callbacks update
/// projections or wake owned work; they must not synchronously mutate the same
/// KV root or establish a cold authority barrier. Such nested admission fails
/// before effect rather than deadlocking ordered publication acknowledgement.
pub fn subscribe_cluster_kv_committed_observations(
    paths: &ClusterPaths,
    callback: impl Fn(&KvCommittedObservation) + Send + Sync + 'static,
) -> sinomo_kv::KvCommittedObservationSubscription {
    let callback_paths = paths.clone();
    sinomo_kv::subscribe_committed_observations(&paths.kv_paths(), move |observation| {
        observation.prepare_once(|| match &observation.value {
            KvCommittedObservationValue::Delta(delta) => {
                crate::kv_local_store::invalidate_cluster_kv_observed_read_models_for_events(
                    &callback_paths,
                    &delta.events,
                );
            }
            KvCommittedObservationValue::Snapshot(_)
                if observation.kind == sinomo_kv::KvCommittedObservationKind::Replacement =>
            {
                crate::kv_local_store::invalidate_cluster_kv_observed_read_models(&callback_paths);
            }
            KvCommittedObservationValue::Discontinuity(_) => {
                crate::kv_local_store::invalidate_cluster_kv_observed_read_models(&callback_paths);
            }
            _ => {}
        });
        callback(observation);
    })
}

pub struct ClusterKvCommittedSnapshotSubscription {
    _subscription: sinomo_kv::KvCommittedObservationSubscription,
}

pub struct ClusterKvCommittedDeltaSubscription {
    _subscription: sinomo_kv::KvCommittedObservationSubscription,
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
    ClusterKvCommittedDeltaSubscription {
        _subscription: subscribe_cluster_kv_committed_observations(paths, move |observation| {
            if let KvCommittedObservationValue::Delta(delta) = &observation.value
                && !delta.events.is_empty()
            {
                callback(delta);
            }
        }),
    }
}

pub fn subscribe_cluster_kv_committed_snapshots(
    paths: &ClusterPaths,
    callback: impl Fn(&KvSnapshot) + Send + Sync + 'static,
) -> ClusterKvCommittedSnapshotSubscription {
    ClusterKvCommittedSnapshotSubscription {
        _subscription: subscribe_cluster_kv_committed_observations(paths, move |observation| {
            if let KvCommittedObservationValue::Snapshot(snapshot) = &observation.value {
                callback(snapshot);
            }
        }),
    }
}
