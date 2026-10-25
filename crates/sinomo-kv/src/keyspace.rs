use crate::internal::*;

/// Dedicated keyspace for the quorum-commit performance probe. The probe
/// measures the ordinary quorum/WAL hot path, so its disposable values do not
/// require an eagerly materialized cross-process snapshot after every commit.
pub const CLUSTER_KV_QUORUM_COMMIT_BENCHMARK_PREFIX: &str =
    "/cluster/benchmarks/quorum-commit/";

/// Availability/projection class for a cluster KV key.
///
/// This does not weaken write semantics: every class still requires a
/// quorum-backed commit. It only selects the safe default read authority and
/// whether a derived/materialized projection may lag the applied Raft view.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterKvKeyspaceClass {
    Critical,
    Operational,
    Telemetry,
}

impl ClusterKvKeyspaceClass {
    pub const fn default_read_authority(self) -> KvReadAuthority {
        match self {
            Self::Critical => KvReadAuthority::Authoritative,
            Self::Operational | Self::Telemetry => KvReadAuthority::Local,
        }
    }

    pub const fn may_defer_materialized_projection(self) -> bool {
        matches!(self, Self::Operational | Self::Telemetry)
    }
}

/// Classifies a cluster KV key. Unknown and newly introduced prefixes fail
/// closed as critical until they are deliberately classified.
pub fn cluster_kv_keyspace_class(key: &str) -> ClusterKvKeyspaceClass {
    if key.starts_with("/cluster/endpoints/")
        || key.starts_with("/cluster/policy/capabilities/")
        || is_dfs_metadata_authority_barrier_key(key)
    {
        return ClusterKvKeyspaceClass::Operational;
    }
    if key.starts_with("/cluster/node-status/")
        || key.starts_with("/cluster/network/connectivity/")
        || key.starts_with("/cluster/network/info/")
        || key.starts_with("/cluster/health/")
        || key.starts_with("/cluster/health-probes/")
        || key.starts_with("/cluster/module-rollouts/nodes/")
        || key.starts_with(CLUSTER_KV_QUORUM_COMMIT_BENCHMARK_PREFIX)
    {
        return ClusterKvKeyspaceClass::Telemetry;
    }
    ClusterKvKeyspaceClass::Critical
}

/// DFS authority barriers contain fresh write-only nonces whose committed
/// values are never consumed. Their Raft commits still prove quorum authority,
/// while the derived materialized projection may safely coalesce nonce churn.
/// Keep the match exact so every other DFS control-plane record fails closed.
fn is_dfs_metadata_authority_barrier_key(key: &str) -> bool {
    let Some(suffix) = key.strip_prefix("/dfs/filesystems/") else {
        return false;
    };
    let Some((file_system_id, suffix)) = suffix.split_once('/') else {
        return false;
    };
    let Some(shard_id) = suffix.strip_prefix("metadata-authority-barriers/") else {
        return false;
    };
    !file_system_id.is_empty() && !shard_id.is_empty() && !shard_id.contains('/')
}

pub fn cluster_kv_default_read_authority_for_key(key: &str) -> KvReadAuthority {
    cluster_kv_keyspace_class(key).default_read_authority()
}

pub fn cluster_kv_key_may_defer_materialized_projection(key: &str) -> bool {
    // Independent shadow/client routing consumes committed connectivity from
    // this projection. Deferring a renewal can leave those readers with only
    // expired observations even though the applied Raft view is current.
    // Keep local read authority and ordinary telemetry deferral unchanged.
    !key.starts_with("/cluster/network/connectivity/")
        && cluster_kv_keyspace_class(key).may_defer_materialized_projection()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_commit_benchmark_subtree_is_telemetry_and_deferrable() {
        let key = format!(
            "{CLUSTER_KV_QUORUM_COMMIT_BENCHMARK_PREFIX}node-a/123-456/measured/0000/0000"
        );

        assert_eq!(
            cluster_kv_keyspace_class(&key),
            ClusterKvKeyspaceClass::Telemetry
        );
        assert_eq!(
            cluster_kv_default_read_authority_for_key(&key),
            KvReadAuthority::Local
        );
        assert!(cluster_kv_key_may_defer_materialized_projection(&key));
    }

    #[test]
    fn quorum_commit_benchmark_classification_does_not_widen_to_neighboring_keys() {
        for key in [
            "/cluster/benchmarks/quorum-commit",
            "/cluster/benchmarks/quorum-commit-other/run",
            "/cluster/benchmarks/range-read/run",
            "/cluster/benchmarks/unknown/run",
        ] {
            assert_eq!(
                cluster_kv_keyspace_class(key),
                ClusterKvKeyspaceClass::Critical,
                "neighboring or unknown key {key} must fail closed"
            );
            assert!(!cluster_kv_key_may_defer_materialized_projection(key));
        }
    }
}
