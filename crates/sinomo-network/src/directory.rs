//! Provider seam between the routing mechanism in this crate and the cluster
//! control plane that owns cluster state (KV-backed today, `sinomo-cluster`
//! after the split).
//!
//! Staying network code must not read cluster KV directly. Everything it needs
//! from cluster state arrives through [`ClusterDirectory`]: membership records,
//! endpoint records, node statuses, connectivity records published by peers,
//! the raft voter check, and a monotonic version stamp used to invalidate
//! routing caches.

use crate::connectivity::ClusterConnectivityRecord;
use crate::error::NetworkError;
use crate::paths::ClusterPaths;
use arc_swap::ArcSwap;
use sinomo_base::{ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeStatusRecord};
use sinomo_node::NodeId;
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

/// Version stamp for cached routing state. Monotonic for a given backing
/// store; the KV-backed implementation uses the local KV snapshot revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirectoryVersion(pub u64);

/// Read-only view of cluster state as the network layer consumes it.
///
/// Implementations wrap whatever store owns cluster state; errors are
/// reported as [`NetworkError::Directory`].
pub trait ClusterDirectory: Send + Sync {
    /// Authorized cluster member records, including endpoint/capability
    /// overlays from the backing store.
    fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError>;

    /// Endpoint records for all nodes as published in the backing store.
    fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError>;

    /// Latest node status record per node id.
    fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError>;

    /// Live connectivity records published by cluster peers.
    fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>;

    /// Monotonic stamp of the backing store used for routing-cache
    /// invalidation.
    fn version(&self) -> Result<DirectoryVersion, NetworkError>;
}

/// Directory with no cluster state: empty results and version 0.
///
/// Useful for tests and pre-cluster code paths that route purely from the
/// bootstrap state and local observations.
pub struct EmptyClusterDirectory;

impl ClusterDirectory for EmptyClusterDirectory {
    fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
        Ok(Vec::new())
    }

    fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        Ok(Vec::new())
    }

    fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError> {
        Ok(BTreeMap::new())
    }

    fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
        Ok(Vec::new())
    }

    fn version(&self) -> Result<DirectoryVersion, NetworkError> {
        Ok(DirectoryVersion(0))
    }
}

/// Builds the [`ClusterDirectory`] for a cluster data root. Registered once at
/// process startup by whoever owns cluster state (the daemon registers the
/// KV-backed directory from `sinomo-cluster`).
pub type ClusterDirectoryFactory =
    Arc<dyn Fn(&ClusterPaths) -> Arc<dyn ClusterDirectory> + Send + Sync>;

static CLUSTER_DIRECTORY_PROVIDER: LazyLock<ArcSwap<Option<ClusterDirectoryFactory>>> =
    LazyLock::new(|| ArcSwap::from_pointee(None));

/// Registers the process-wide cluster directory provider. Routing, endpoint
/// selection, and cache invalidation consult directories produced by this
/// factory; without a registered provider they see an empty directory and
/// route purely from bootstrap state and local observations.
pub fn set_cluster_directory_provider(factory: ClusterDirectoryFactory) {
    CLUSTER_DIRECTORY_PROVIDER.store(Arc::new(Some(factory)));
}

/// The cluster directory for `paths` from the registered provider, or an
/// empty directory when no provider is registered.
pub fn cluster_directory_for(paths: &ClusterPaths) -> Arc<dyn ClusterDirectory> {
    match CLUSTER_DIRECTORY_PROVIDER.load().as_ref() {
        Some(factory) => factory(paths),
        None => default_cluster_directory(paths),
    }
}

fn default_cluster_directory(_paths: &ClusterPaths) -> Arc<dyn ClusterDirectory> {
    Arc::new(EmptyClusterDirectory)
}
