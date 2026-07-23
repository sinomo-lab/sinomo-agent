use crate::internal::*;

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
    cluster_kv_keyspace_class(key).may_defer_materialized_projection()
}
