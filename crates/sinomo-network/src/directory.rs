//! Provider seam between the routing mechanism in this crate and the cluster
//! control plane that owns stable cluster state.
//!
//! Staying network code must not read cluster KV directly. Everything it needs
//! from cluster state arrives through [`ClusterDirectory`]: membership records,
//! endpoint records, node statuses, the raft voter check, and a monotonic
//! version stamp used to invalidate routing caches. Cluster connectivity is
//! supplied by this directory when a cluster KV-backed provider is available.
//! The local connectivity file is only a bootstrap fallback before that graph
//! can be read. While the current node has not published any source-owned
//! record, its accepted local snapshot may also complete a sparse directory
//! graph with imported relay edges. After that source appears in the directory,
//! only its own newer observations are overlaid.

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

/// Authorized routing identity and endpoints for one directory member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryNode {
    pub node_id: NodeId,
    pub public_key: String,
    pub endpoints: Vec<ClusterEndpointRecord>,
}

impl DirectoryNode {
    pub fn try_new(
        node_id: NodeId,
        public_key: impl Into<String>,
        endpoints: Vec<ClusterEndpointRecord>,
    ) -> Result<Self, NetworkError> {
        if let Some(endpoint) = endpoints
            .iter()
            .find(|endpoint| endpoint.node_id != node_id)
        {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "directory endpoint {} belongs to {}, not member {}",
                    endpoint.endpoint_id, endpoint.node_id, node_id
                ),
            });
        }
        Ok(Self {
            node_id,
            public_key: public_key.into(),
            endpoints,
        })
    }
}

/// Immutable authorized directory input consumed by pure route planning.
#[derive(Clone, Debug)]
pub struct DirectorySnapshot {
    version: DirectoryVersion,
    nodes: Arc<BTreeMap<NodeId, DirectoryNode>>,
}

impl DirectorySnapshot {
    pub fn try_new(
        version: DirectoryVersion,
        nodes: impl IntoIterator<Item = DirectoryNode>,
    ) -> Result<Self, NetworkError> {
        let mut indexed = BTreeMap::new();
        for node in nodes {
            if indexed.insert(node.node_id.clone(), node).is_some() {
                return Err(NetworkError::Serialization {
                    detail: "directory snapshot contains a duplicate node".to_string(),
                });
            }
        }
        Ok(Self {
            version,
            nodes: Arc::new(indexed),
        })
    }

    pub fn try_from_members(
        version: DirectoryVersion,
        members: impl IntoIterator<Item = ClusterMemberRecord>,
    ) -> Result<Self, NetworkError> {
        Self::try_from_records(version, members, std::iter::empty())
    }

    pub fn try_from_records(
        version: DirectoryVersion,
        members: impl IntoIterator<Item = ClusterMemberRecord>,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
    ) -> Result<Self, NetworkError> {
        let mut nodes = BTreeMap::<NodeId, DirectoryNode>::new();
        for member in members {
            if !member.state.is_authorized() {
                continue;
            }
            let node =
                DirectoryNode::try_new(member.node_id, member.node_public_key, member.endpoints)?;
            if nodes.insert(node.node_id.clone(), node).is_some() {
                return Err(NetworkError::Serialization {
                    detail: "directory records contain a duplicate authorized member".to_string(),
                });
            }
        }
        for endpoint in endpoints {
            let Some(node) = nodes.get_mut(&endpoint.node_id) else {
                continue;
            };
            if let Some(existing) = node
                .endpoints
                .iter_mut()
                .find(|existing| existing.endpoint_id == endpoint.endpoint_id)
            {
                *existing = endpoint;
            } else {
                node.endpoints.push(endpoint);
            }
        }
        for node in nodes.values_mut() {
            node.endpoints.sort_by(|left, right| {
                left.endpoint_id
                    .cmp(&right.endpoint_id)
                    .then_with(|| left.address.cmp(&right.address))
            });
        }
        Self::try_new(version, nodes.into_values())
    }

    pub fn version(&self) -> DirectoryVersion {
        self.version
    }

    pub fn nodes(&self) -> &BTreeMap<NodeId, DirectoryNode> {
        &self.nodes
    }

    pub fn node(&self, node_id: &NodeId) -> Option<&DirectoryNode> {
        self.nodes.get(node_id)
    }

    pub fn is_authorized(&self, node_id: &NodeId) -> bool {
        self.nodes.contains_key(node_id)
    }
}

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

    /// Connectivity records from the backing directory.
    ///
    /// Production KV-backed directories return the authoritative cluster
    /// connectivity graph. The local connectivity file is used only when the
    /// directory graph is unavailable or empty during bootstrap. Until the
    /// current node's source first appears there, its accepted local snapshot
    /// may complete a sparse graph with imported relay edges; afterwards only
    /// the current node's own newer observations are overlaid.
    fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>;

    /// Monotonic stamp of the backing store used for routing-cache
    /// invalidation.
    fn version(&self) -> Result<DirectoryVersion, NetworkError>;

    /// Captures one immutable authorized routing view.
    ///
    /// Providers with transactional snapshot support may override this to
    /// guarantee all fields came from one backing revision. The default keeps
    /// existing providers source-compatible while still validating and
    /// normalizing their public records at the new boundary.
    fn snapshot(&self) -> Result<DirectorySnapshot, NetworkError> {
        let before = self.version()?;
        let members = self.members()?;
        let endpoints = self.endpoints()?;
        let after = self.version()?;
        if before != after {
            return Err(NetworkError::Directory {
                detail: format!(
                    "directory changed while capturing routing snapshot: {} -> {}",
                    before.0, after.0
                ),
            });
        }
        DirectorySnapshot::try_from_records(after, members, endpoints)
    }
}

/// Directory with no cluster state: empty results and version 0.
///
/// Useful for tests and pre-cluster code paths that do not have a KV-backed
/// directory yet.
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
/// may use local bootstrap caches until the KV-backed directory is available.
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
