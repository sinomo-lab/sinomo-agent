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
use sinomo_base::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeStatusRecord,
};
use sinomo_node::NodeId;
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

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
    raft_voter: bool,
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
            raft_voter: false,
        })
    }

    /// Whether this accepted membership record carries a Raft vote.
    pub fn has_raft_vote(&self) -> bool {
        self.raft_voter
    }
}

/// Immutable authorized directory input consumed by pure route planning.
#[derive(Clone, Debug)]
pub struct DirectorySnapshot {
    version: DirectoryVersion,
    nodes: Arc<BTreeMap<NodeId, DirectoryNode>>,
}

/// One immutable set of raw directory records from a single backing-store
/// revision.
///
/// Wrappers which must preserve record metadata (for example, bootstrap/KV
/// freshness arbitration) consume this form before projecting authorized
/// [`DirectoryNode`] values.
#[derive(Clone, Debug)]
pub struct DirectoryRecordsSnapshot {
    version: DirectoryVersion,
    members: Vec<ClusterMemberRecord>,
    endpoints: Vec<ClusterEndpointRecord>,
}

/// One immutable routing input captured from a single directory generation.
///
/// Membership, endpoints, and connectivity evidence must move together. A
/// route planned from a connectivity record whose endpoint belongs only to a
/// newer directory generation can otherwise transiently publish zero
/// executable candidates even though both individual reads were valid.
#[derive(Clone, Debug)]
pub struct DirectoryRoutingRecordsSnapshot {
    version: DirectoryVersion,
    members: Vec<ClusterMemberRecord>,
    endpoints: Vec<ClusterEndpointRecord>,
    connectivity_records: Vec<ClusterConnectivityRecord>,
}

impl DirectoryRecordsSnapshot {
    pub fn new(
        version: DirectoryVersion,
        members: Vec<ClusterMemberRecord>,
        endpoints: Vec<ClusterEndpointRecord>,
    ) -> Self {
        Self {
            version,
            members,
            endpoints,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        DirectoryVersion,
        Vec<ClusterMemberRecord>,
        Vec<ClusterEndpointRecord>,
    ) {
        (self.version, self.members, self.endpoints)
    }
}

impl DirectoryRoutingRecordsSnapshot {
    pub fn new(
        version: DirectoryVersion,
        members: Vec<ClusterMemberRecord>,
        endpoints: Vec<ClusterEndpointRecord>,
        connectivity_records: Vec<ClusterConnectivityRecord>,
    ) -> Self {
        Self {
            version,
            members,
            endpoints,
            connectivity_records,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        DirectoryVersion,
        Vec<ClusterMemberRecord>,
        Vec<ClusterEndpointRecord>,
        Vec<ClusterConnectivityRecord>,
    ) {
        (
            self.version,
            self.members,
            self.endpoints,
            self.connectivity_records,
        )
    }
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
            let raft_voter = member.has_raft_vote();
            let mut node =
                DirectoryNode::try_new(member.node_id, member.node_public_key, member.endpoints)?;
            node.raft_voter = raft_voter;
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

    /// Monotonic generation of route-relevant membership, endpoint,
    /// capability, and connectivity inputs. Transactional providers should
    /// keep this stable across unrelated KV revisions so active carriers and
    /// prepared route state are not discarded by application writes.
    fn routing_version(&self) -> Result<DirectoryVersion, NetworkError> {
        self.version()
    }

    /// Captures raw membership and endpoint records from one directory
    /// generation.
    ///
    /// Transactional providers override this method. The compatibility
    /// default rejects a capture if its generation changes between reads.
    fn records_snapshot(&self) -> Result<DirectoryRecordsSnapshot, NetworkError> {
        let before = self.version()?;
        let members = self.members()?;
        let endpoints = self.endpoints()?;
        let after = self.version()?;
        if before != after {
            return Err(NetworkError::Directory {
                detail: format!(
                    "directory changed while capturing routing records: {} -> {}",
                    before.0, after.0
                ),
            });
        }
        Ok(DirectoryRecordsSnapshot::new(after, members, endpoints))
    }

    /// Captures membership and endpoint records no older than the local
    /// owner's applied state, for authorizing peers.
    ///
    /// [`Self::records_snapshot`] may serve an earlier observation while a
    /// local mutation is in flight. Providers with such a fallback wait for
    /// the mutation until `deadline` instead.
    fn current_records_snapshot(
        &self,
        deadline: Instant,
    ) -> Result<DirectoryRecordsSnapshot, NetworkError> {
        let _ = deadline;
        self.records_snapshot()
    }

    /// Captures every raw input needed to build one route-planning generation.
    ///
    /// Providers backed by a transactional store should override this method.
    /// The compatibility default fences separate reads with the provider's
    /// monotonic version and fails closed if the generation changes.
    fn routing_records_snapshot(&self) -> Result<DirectoryRoutingRecordsSnapshot, NetworkError> {
        let records = self.records_snapshot()?;
        let connectivity_records = self.connectivity_records()?;
        let after = self.version()?;
        let (version, members, endpoints) = records.into_parts();
        if version != after {
            return Err(NetworkError::Directory {
                detail: format!(
                    "directory changed while capturing routing records: {} -> {}",
                    version.0, after.0
                ),
            });
        }
        Ok(DirectoryRoutingRecordsSnapshot::new(
            after,
            members,
            endpoints,
            connectivity_records,
        ))
    }

    /// Captures one immutable authorized routing view.
    ///
    /// Providers with transactional snapshot support may override this to
    /// guarantee all fields came from one backing revision. The default keeps
    /// existing providers source-compatible while still validating and
    /// normalizing their public records at the new boundary.
    fn snapshot(&self) -> Result<DirectorySnapshot, NetworkError> {
        let (version, members, endpoints) = self.records_snapshot()?.into_parts();
        DirectorySnapshot::try_from_records(version, members, endpoints)
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

/// Directory view used while accepted bootstrap state and its asynchronously
/// materialized cluster-KV projection converge.
///
/// Membership records are merged by their per-member `updated_at` timestamp,
/// with the KV-backed record winning ties. That admits members already present
/// in either accepted projection without allowing an older active record to
/// override a newer explicit removal.
struct BootstrapAwareClusterDirectory {
    inner: Arc<dyn ClusterDirectory>,
    bootstrap_version: DirectoryVersion,
    bootstrap_members: Vec<ClusterMemberRecord>,
}

impl BootstrapAwareClusterDirectory {
    fn routing_merge_version(&self, inner: DirectoryVersion) -> DirectoryVersion {
        // The bootstrap revision is a safe generation only before the
        // route-specific KV projection exists. Once the provider exposes a
        // nonzero routing generation, replacing it with the full cluster KV
        // revision would invalidate routes for every unrelated application
        // write and defeat persistent carrier reuse.
        if inner.0 == 0 {
            inner.max(self.bootstrap_version)
        } else {
            inner
        }
    }

    fn merge_records(
        &self,
        version: DirectoryVersion,
        inner_members: Vec<ClusterMemberRecord>,
        inner_endpoints: Vec<ClusterEndpointRecord>,
    ) -> (
        DirectoryVersion,
        Vec<ClusterMemberRecord>,
        Vec<ClusterEndpointRecord>,
    ) {
        let mut members = self
            .bootstrap_members
            .iter()
            .cloned()
            .map(|member| (member.node_id.clone(), member))
            .collect::<BTreeMap<_, _>>();
        for member in inner_members {
            match members.entry(member.node_id.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(member);
                }
                std::collections::btree_map::Entry::Occupied(mut entry)
                    if member.updated_at >= entry.get().updated_at =>
                {
                    entry.insert(member);
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
        // Membership freshness selects identity and authority, not caller-local
        // address observations. Retain observed dialing hints only for the same
        // still-authorized key; they cannot restore a removed or rotated peer.
        for bootstrap in &self.bootstrap_members {
            if let Some(member) = members.get_mut(&bootstrap.node_id)
                && bootstrap.state.is_authorized()
                && member.state.is_authorized()
                && member.cluster_id == bootstrap.cluster_id
                && member.node_public_key == bootstrap.node_public_key
            {
                for endpoint in &bootstrap.endpoints {
                    if endpoint.source == sinomo_base::ClusterEndpointSource::Observed
                        && crate::is_cluster_route_hint_endpoint(endpoint)
                        && endpoint.node_id == member.node_id
                        && !member.endpoints.iter().any(|known| {
                            known.endpoint_id == endpoint.endpoint_id
                                || known.address == endpoint.address
                        })
                    {
                        member.endpoints.push(endpoint.clone());
                    }
                }
            }
        }
        (
            version.max(self.bootstrap_version),
            members.into_values().collect(),
            inner_endpoints,
        )
    }

    fn records(
        &self,
    ) -> Result<
        (
            DirectoryVersion,
            Vec<ClusterMemberRecord>,
            Vec<ClusterEndpointRecord>,
        ),
        NetworkError,
    > {
        let (version, inner_members, inner_endpoints) = self.inner.records_snapshot()?.into_parts();
        Ok(self.merge_records(version, inner_members, inner_endpoints))
    }
}

impl ClusterDirectory for BootstrapAwareClusterDirectory {
    fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
        self.records().map(|(_, members, _)| members)
    }

    fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let (_, members, mut endpoints) = self.records()?;
        endpoints.extend(
            members
                .into_iter()
                .flat_map(|member| member.endpoints.into_iter()),
        );
        Ok(endpoints)
    }

    fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError> {
        self.inner.node_statuses()
    }

    fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
        self.inner.connectivity_records()
    }

    fn version(&self) -> Result<DirectoryVersion, NetworkError> {
        Ok(self.inner.version()?.max(self.bootstrap_version))
    }

    fn routing_version(&self) -> Result<DirectoryVersion, NetworkError> {
        Ok(self.routing_merge_version(self.inner.routing_version()?))
    }

    fn records_snapshot(&self) -> Result<DirectoryRecordsSnapshot, NetworkError> {
        let (version, members, endpoints) = self.records()?;
        Ok(DirectoryRecordsSnapshot::new(version, members, endpoints))
    }

    fn current_records_snapshot(
        &self,
        deadline: Instant,
    ) -> Result<DirectoryRecordsSnapshot, NetworkError> {
        let (version, members, endpoints) =
            self.inner.current_records_snapshot(deadline)?.into_parts();
        let (version, members, endpoints) = self.merge_records(version, members, endpoints);
        Ok(DirectoryRecordsSnapshot::new(version, members, endpoints))
    }

    fn routing_records_snapshot(&self) -> Result<DirectoryRoutingRecordsSnapshot, NetworkError> {
        let (version, members, endpoints, connectivity_records) =
            self.inner.routing_records_snapshot()?.into_parts();
        let routing_version = self.routing_merge_version(version);
        let (_, members, endpoints) = self.merge_records(version, members, endpoints);
        Ok(DirectoryRoutingRecordsSnapshot::new(
            routing_version,
            members,
            endpoints,
            connectivity_records,
        ))
    }

    fn snapshot(&self) -> Result<DirectorySnapshot, NetworkError> {
        let (version, members, endpoints) = self.records_snapshot()?.into_parts();
        DirectorySnapshot::try_from_records(version, members, endpoints)
    }
}

/// Combines the KV-backed directory with the accepted bootstrap membership
/// state without weakening the rule that a newer KV revision is authoritative.
pub fn cluster_directory_with_bootstrap_state(
    inner: Arc<dyn ClusterDirectory>,
    state: &ClusterBootstrapState,
) -> Arc<dyn ClusterDirectory> {
    Arc::new(BootstrapAwareClusterDirectory {
        inner,
        bootstrap_version: DirectoryVersion(state.kv_revision),
        bootstrap_members: state.members.clone(),
    })
}

/// Builds an immutable routing directory from one already-authoritative
/// bootstrap-state fence without consulting the process-wide directory
/// provider.
///
/// This is intentionally narrower than [`cluster_directory_with_bootstrap_state`].
/// Raft control messages whose membership was captured under the resident
/// operation fence must route from that exact member/endpoint view: consulting
/// the KV-backed provider again can both mix generations and synchronously
/// enter cold snapshot/WAL hydration before the control message is dispatched.
/// Connectivity remains local routing evidence and is overlaid by the routed
/// client after this immutable directory snapshot is captured.
pub(crate) fn cluster_directory_from_fenced_bootstrap_state(
    state: &ClusterBootstrapState,
) -> Arc<dyn ClusterDirectory> {
    cluster_directory_with_bootstrap_state(Arc::new(EmptyClusterDirectory), state)
}

/// Exact accepted membership plus optional locally observed connectivity.
///
/// The connectivity provider cannot add members, endpoints, statuses, or a
/// routing generation to the accepted fence. It is consulted only when a cold
/// routed connector captures its connectivity records, and failure falls back
/// to the ordinary local bootstrap overlay. This lets a restarted node recover
/// a multi-hop authority route from its last observed KV projection without
/// making that projection an authorization source.
struct FencedBootstrapConnectivityDirectory {
    exact: Arc<dyn ClusterDirectory>,
    connectivity: Arc<dyn ClusterDirectory>,
}

impl FencedBootstrapConnectivityDirectory {
    fn observed_connectivity_records(&self) -> Vec<ClusterConnectivityRecord> {
        self.connectivity
            .routing_records_snapshot()
            .map(|snapshot| snapshot.into_parts().3)
            .unwrap_or_default()
    }
}

impl ClusterDirectory for FencedBootstrapConnectivityDirectory {
    fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
        self.exact.members()
    }

    fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        self.exact.endpoints()
    }

    fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError> {
        self.exact.node_statuses()
    }

    fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
        Ok(self.observed_connectivity_records())
    }

    fn version(&self) -> Result<DirectoryVersion, NetworkError> {
        self.exact.version()
    }

    fn routing_version(&self) -> Result<DirectoryVersion, NetworkError> {
        self.exact.routing_version()
    }

    fn records_snapshot(&self) -> Result<DirectoryRecordsSnapshot, NetworkError> {
        self.exact.records_snapshot()
    }

    fn routing_records_snapshot(&self) -> Result<DirectoryRoutingRecordsSnapshot, NetworkError> {
        let (version, members, endpoints) = self.exact.records_snapshot()?.into_parts();
        Ok(DirectoryRoutingRecordsSnapshot::new(
            version,
            members,
            endpoints,
            self.observed_connectivity_records(),
        ))
    }

    fn snapshot(&self) -> Result<DirectorySnapshot, NetworkError> {
        self.exact.snapshot()
    }
}

pub(crate) fn cluster_directory_from_fenced_bootstrap_state_with_connectivity(
    state: &ClusterBootstrapState,
    connectivity: Arc<dyn ClusterDirectory>,
) -> Arc<dyn ClusterDirectory> {
    Arc::new(FencedBootstrapConnectivityDirectory {
        exact: cluster_directory_from_fenced_bootstrap_state(state),
        connectivity,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::Id;
    use time::OffsetDateTime;

    #[derive(Clone)]
    struct FixedDirectory {
        version: DirectoryVersion,
        members: Vec<ClusterMemberRecord>,
    }

    impl ClusterDirectory for FixedDirectory {
        fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
            Ok(self.members.clone())
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
            Ok(self.version)
        }
    }

    #[derive(Clone)]
    struct TransactionalOnlyDirectory(DirectoryRecordsSnapshot);

    impl ClusterDirectory for TransactionalOnlyDirectory {
        fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
            panic!("transactional directory members must not be read separately")
        }

        fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            panic!("transactional directory endpoints must not be read separately")
        }

        fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError> {
            Ok(BTreeMap::new())
        }

        fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn version(&self) -> Result<DirectoryVersion, NetworkError> {
            panic!("transactional directory version must not be read separately")
        }

        fn records_snapshot(&self) -> Result<DirectoryRecordsSnapshot, NetworkError> {
            Ok(self.0.clone())
        }
    }

    #[derive(Clone)]
    struct TransactionalRoutingOnlyDirectory(DirectoryRoutingRecordsSnapshot);

    impl ClusterDirectory for TransactionalRoutingOnlyDirectory {
        fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
            panic!("transactional routing directory members must not be read separately")
        }

        fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            panic!("transactional routing directory endpoints must not be read separately")
        }

        fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError> {
            Ok(BTreeMap::new())
        }

        fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
            panic!("transactional routing connectivity must not be read separately")
        }

        fn version(&self) -> Result<DirectoryVersion, NetworkError> {
            panic!("transactional routing version must not be read separately")
        }

        fn routing_version(&self) -> Result<DirectoryVersion, NetworkError> {
            Ok(self.0.version)
        }

        fn routing_records_snapshot(
            &self,
        ) -> Result<DirectoryRoutingRecordsSnapshot, NetworkError> {
            Ok(self.0.clone())
        }
    }

    fn id(name: &str) -> Id {
        Id::from_legacy_named(name).expect("test id")
    }

    fn member(cluster_id: &Id, name: &str, key: &str) -> ClusterMemberRecord {
        ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            id(name),
            name,
            key,
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn bootstrap_state(revision: u64, members: Vec<ClusterMemberRecord>) -> ClusterBootstrapState {
        let local_node_id = members.first().expect("local member").node_id.clone();
        ClusterBootstrapState {
            cluster_id: members.first().expect("member").cluster_id.clone(),
            cluster_name: "test-cluster".to_string(),
            local_node_id,
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: revision,
            members,
            initialized_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn committed_membership_keeps_observed_routes_without_restoring_old_authority() {
        let cluster = id("observed-route-cluster");
        let local = member(&cluster, "observed-route-local", "local-key");
        let mut peer = member(&cluster, "observed-route-peer", "peer-key");
        let mut observed = crate::daemon_lan_endpoint(&peer.node_id, "127.0.0.1:32353".into(), 0);
        observed.source = sinomo_base::ClusterEndpointSource::Observed;
        let retired = observed.clone();
        observed.endpoint_id =
            crate::cluster_route_hint_endpoint_id(&peer.node_id, &observed.address);
        peer.endpoints.push(observed.clone());
        peer.endpoints.push(retired.clone());
        let state = bootstrap_state(1, vec![local, peer.clone()]);
        let mut committed = peer.clone();
        committed.endpoints.clear();
        committed.updated_at += time::Duration::seconds(1);
        for changed_key in [false, true] {
            let mut current = committed.clone();
            if changed_key {
                current.node_public_key = "rotated-key".into();
            }
            let directory = cluster_directory_with_bootstrap_state(
                Arc::new(FixedDirectory {
                    version: DirectoryVersion(2),
                    members: vec![current],
                }),
                &state,
            );
            let snapshot = directory.snapshot().unwrap();
            assert_eq!(
                snapshot
                    .node(&peer.node_id)
                    .unwrap()
                    .endpoints
                    .contains(&observed),
                !changed_key
            );
            assert!(
                !snapshot
                    .node(&peer.node_id)
                    .unwrap()
                    .endpoints
                    .contains(&retired),
                "retired advertisements are not operator hints"
            );
        }
        committed.state = sinomo_base::ClusterMemberState::Removed;
        let directory = cluster_directory_with_bootstrap_state(
            Arc::new(FixedDirectory {
                version: DirectoryVersion(3),
                members: vec![committed],
            }),
            &state,
        );
        assert!(!directory.snapshot().unwrap().is_authorized(&peer.node_id));
    }

    #[test]
    fn bootstrap_members_authorize_before_empty_directory_materializes() {
        let cluster_id = id("directory-bootstrap-empty-cluster");
        let local = member(&cluster_id, "directory-bootstrap-empty-local", "local-key");
        let target = member(
            &cluster_id,
            "directory-bootstrap-empty-target",
            "target-key",
        );
        let state = bootstrap_state(0, vec![local.clone(), target.clone()]);
        let directory = cluster_directory_with_bootstrap_state(
            Arc::new(FixedDirectory {
                version: DirectoryVersion(0),
                members: Vec::new(),
            }),
            &state,
        );

        let snapshot = directory.snapshot().expect("bootstrap snapshot");
        assert!(snapshot.is_authorized(&local.node_id));
        assert!(snapshot.is_authorized(&target.node_id));
    }

    #[test]
    fn fenced_bootstrap_directory_carries_member_endpoints_without_provider_state() {
        let cluster_id = id("directory-fenced-bootstrap-cluster");
        let local = member(&cluster_id, "directory-fenced-bootstrap-local", "local-key");
        let mut target = member(
            &cluster_id,
            "directory-fenced-bootstrap-target",
            "target-key",
        );
        let target_endpoint =
            ClusterEndpointRecord::loopback(target.node_id.clone(), "127.0.0.1:45678");
        target.endpoints.push(target_endpoint.clone());
        let state = bootstrap_state(17, vec![local, target.clone()]);

        let directory = cluster_directory_from_fenced_bootstrap_state(&state);
        assert_eq!(directory.routing_version(), Ok(DirectoryVersion(17)));
        let (version, members, endpoints, connectivity) = directory
            .routing_records_snapshot()
            .expect("fenced bootstrap routing snapshot")
            .into_parts();
        assert_eq!(version, DirectoryVersion(17));
        assert!(endpoints.is_empty());
        assert!(connectivity.is_empty());

        let snapshot = DirectorySnapshot::try_from_records(version, members, endpoints)
            .expect("fenced bootstrap directory snapshot");
        assert_eq!(
            snapshot
                .node(&target.node_id)
                .expect("target member")
                .endpoints,
            vec![target_endpoint]
        );
    }

    #[test]
    fn bootstrap_wrapper_preserves_transactional_inner_snapshot() {
        let cluster_id = id("directory-bootstrap-transactional-cluster");
        let local = member(
            &cluster_id,
            "directory-bootstrap-transactional-local",
            "local-key",
        );
        let target = member(
            &cluster_id,
            "directory-bootstrap-transactional-target",
            "target-key",
        );
        let state = bootstrap_state(3, vec![local.clone()]);
        let directory = cluster_directory_with_bootstrap_state(
            Arc::new(TransactionalOnlyDirectory(DirectoryRecordsSnapshot::new(
                DirectoryVersion(7),
                vec![local, target.clone()],
                Vec::new(),
            ))),
            &state,
        );

        let snapshot = directory.snapshot().expect("transactional snapshot");
        assert_eq!(snapshot.version(), DirectoryVersion(7));
        assert!(snapshot.is_authorized(&target.node_id));
    }

    #[test]
    fn bootstrap_wrapper_preserves_transactional_routing_snapshot() {
        let cluster_id = id("directory-bootstrap-routing-cluster");
        let local = member(
            &cluster_id,
            "directory-bootstrap-routing-local",
            "local-key",
        );
        let target = member(
            &cluster_id,
            "directory-bootstrap-routing-target",
            "target-key",
        );
        let state = bootstrap_state(3, vec![local.clone()]);
        let directory = cluster_directory_with_bootstrap_state(
            Arc::new(TransactionalRoutingOnlyDirectory(
                DirectoryRoutingRecordsSnapshot::new(
                    DirectoryVersion(7),
                    vec![local, target.clone()],
                    Vec::new(),
                    Vec::new(),
                ),
            )),
            &state,
        );

        let (version, members, _, connectivity) = directory
            .routing_records_snapshot()
            .expect("transactional routing snapshot")
            .into_parts();
        assert_eq!(version, DirectoryVersion(7));
        assert!(
            members
                .iter()
                .any(|member| member.node_id == target.node_id)
        );
        assert!(connectivity.is_empty());
    }

    #[test]
    fn materialized_routing_generation_ignores_unrelated_bootstrap_revision() {
        let cluster_id = id("directory-materialized-routing-cluster");
        let local = member(
            &cluster_id,
            "directory-materialized-routing-local",
            "local-key",
        );
        let mut state = bootstrap_state(99, vec![local.clone()]);
        let directory = cluster_directory_with_bootstrap_state(
            Arc::new(TransactionalRoutingOnlyDirectory(
                DirectoryRoutingRecordsSnapshot::new(
                    DirectoryVersion(7),
                    vec![local],
                    Vec::new(),
                    Vec::new(),
                ),
            )),
            &state,
        );

        assert_eq!(
            directory.routing_version().expect("routing version"),
            DirectoryVersion(7)
        );
        assert_eq!(
            directory
                .routing_records_snapshot()
                .expect("routing snapshot")
                .version,
            DirectoryVersion(7)
        );

        state.kv_revision = 100;
        let refreshed = cluster_directory_with_bootstrap_state(
            Arc::new(TransactionalRoutingOnlyDirectory(
                DirectoryRoutingRecordsSnapshot::new(
                    DirectoryVersion(7),
                    state.members.clone(),
                    Vec::new(),
                    Vec::new(),
                ),
            )),
            &state,
        );
        assert_eq!(
            refreshed.routing_version().expect("stable routing version"),
            DirectoryVersion(7),
            "an unrelated cluster KV revision must not invalidate routing"
        );
    }

    #[test]
    fn bootstrap_revision_versions_routes_until_projection_materializes() {
        let cluster_id = id("directory-bootstrap-routing-cluster");
        let local = member(
            &cluster_id,
            "directory-bootstrap-routing-local",
            "local-key",
        );
        let state = bootstrap_state(11, vec![local]);
        let directory = cluster_directory_with_bootstrap_state(
            Arc::new(TransactionalRoutingOnlyDirectory(
                DirectoryRoutingRecordsSnapshot::new(
                    DirectoryVersion(0),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                ),
            )),
            &state,
        );

        assert_eq!(
            directory
                .routing_version()
                .expect("bootstrap routing version"),
            DirectoryVersion(11)
        );
        assert_eq!(
            directory
                .routing_records_snapshot()
                .expect("bootstrap routing snapshot")
                .version,
            DirectoryVersion(11)
        );
    }

    #[test]
    fn accepted_projections_merge_members_during_materialization() {
        let cluster_id = id("directory-bootstrap-partial-cluster");
        let local = member(
            &cluster_id,
            "directory-bootstrap-partial-local",
            "local-key",
        );
        let target = member(
            &cluster_id,
            "directory-bootstrap-partial-target",
            "target-key",
        );
        let directory_only = member(
            &cluster_id,
            "directory-bootstrap-partial-directory-only",
            "directory-only-key",
        );
        let state = bootstrap_state(7, vec![local.clone(), target.clone()]);
        let directory = cluster_directory_with_bootstrap_state(
            Arc::new(FixedDirectory {
                version: DirectoryVersion(7),
                members: vec![local, directory_only.clone()],
            }),
            &state,
        );

        let snapshot = directory.snapshot().expect("bootstrap snapshot");
        assert!(snapshot.is_authorized(&target.node_id));
        assert!(snapshot.is_authorized(&directory_only.node_id));
        assert_eq!(snapshot.version(), DirectoryVersion(7));
    }

    #[test]
    fn newer_removed_record_cannot_be_overridden_by_stale_bootstrap_membership() {
        let cluster_id = id("directory-bootstrap-newer-cluster");
        let local = member(&cluster_id, "directory-bootstrap-newer-local", "local-key");
        let stale_active = member(
            &cluster_id,
            "directory-bootstrap-newer-removed",
            "removed-key",
        );
        let state = bootstrap_state(7, vec![local.clone(), stale_active.clone()]);
        let mut removed = stale_active.clone();
        removed.state = sinomo_base::ClusterMemberState::Removed;
        removed.updated_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1);
        let directory = cluster_directory_with_bootstrap_state(
            Arc::new(FixedDirectory {
                version: DirectoryVersion(8),
                members: vec![removed],
            }),
            &state,
        );

        let snapshot = directory.snapshot().expect("directory snapshot");
        assert!(snapshot.is_authorized(&local.node_id));
        assert!(!snapshot.is_authorized(&stale_active.node_id));
        assert_eq!(snapshot.version(), DirectoryVersion(8));
    }
}
