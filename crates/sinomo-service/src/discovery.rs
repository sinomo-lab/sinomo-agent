//! Service registration records, discovery, and generic routing target
//! selection.
//!
//! The daemon publishes one [`ServiceInstanceRecord`] per locally registered
//! service under `/cluster/services/<service>/<node_id>`. Other nodes discover
//! service instances by reading that prefix from a cluster-KV snapshot and
//! pick a target with [`ServiceTargetSelector`], which consults the network
//! layer's [`ClusterDirectory`] for reachability instead of owning its own
//! routing rules.

use crate::{ClusterAccess, ServiceError, ServiceRegistry};
use serde::{Deserialize, Serialize};
use sinomo_kv::{ClusterKvTxnCommitResult, KvEntry, KvOp, KvTxn, KvTxnRequest};
use sinomo_network::{ClusterDirectory, NetworkError};
use sinomo_node::NodeId;
use std::sync::atomic::{AtomicUsize, Ordering};
use time::OffsetDateTime;

/// Cluster-KV key prefix for all service instance records.
pub const CLUSTER_SERVICES_KV_PREFIX: &str = "/cluster/services/";

/// Key for one service instance record.
pub fn cluster_service_instance_key(service: &str, node_id: &NodeId) -> String {
    format!("{CLUSTER_SERVICES_KV_PREFIX}{service}/{node_id}")
}

/// Key prefix for all instances of one service.
pub fn cluster_service_prefix(service: &str) -> String {
    format!("{CLUSTER_SERVICES_KV_PREFIX}{service}/")
}

/// One node's registration of one service: which routes it serves, which
/// capabilities it exports, and when the registration was last refreshed.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ServiceInstanceRecord {
    pub service: String,
    pub node_id: NodeId,
    /// Service registry snapshot version that produced this record.
    pub registry_version: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl ServiceInstanceRecord {
    fn stable_eq(&self, other: &Self) -> bool {
        self.service == other.service
            && self.node_id == other.node_id
            && self.routes == other.routes
            && self.capabilities == other.capabilities
    }
}

/// Builds the instance records for every service in a local registry.
pub fn service_instance_records_from_registry(
    registry: &ServiceRegistry,
    node_id: &NodeId,
    registry_version: u64,
    now: OffsetDateTime,
) -> Vec<ServiceInstanceRecord> {
    let mut by_service: std::collections::BTreeMap<&str, ServiceInstanceRecord> =
        std::collections::BTreeMap::new();
    for route in registry.route_categories() {
        let Some(service) = registry.category_for_route(&route.route) else {
            continue;
        };
        let entry = by_service
            .entry(service)
            .or_insert_with(|| ServiceInstanceRecord {
                service: service.to_string(),
                node_id: node_id.clone(),
                registry_version,
                routes: Vec::new(),
                capabilities: Vec::new(),
                updated_at: now,
            });
        entry.routes.push(route.route.clone());
        for capability in &route.required_capabilities {
            if !entry.capabilities.contains(capability) {
                entry.capabilities.push(capability.clone());
            }
        }
    }
    let mut records: Vec<ServiceInstanceRecord> = by_service.into_values().collect();
    for record in &mut records {
        record.routes.sort();
        record.routes.dedup();
        record.capabilities.sort();
        record.capabilities.dedup();
    }
    records
}

/// Decodes all instances of one service from KV entries under its prefix.
pub fn service_instances_from_entries(
    entries: &[KvEntry],
) -> Result<Vec<ServiceInstanceRecord>, ServiceError> {
    let mut records = Vec::new();
    for entry in entries {
        let record: ServiceInstanceRecord = entry.decode()?;
        records.push(record);
    }
    records.sort_by(|left, right| {
        left.service
            .cmp(&right.service)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    Ok(records)
}

/// Reads the registered instances of `service` through a [`ClusterAccess`].
pub fn discover_service_instances(
    cluster: &dyn ClusterAccess,
    service: &str,
) -> Result<Vec<ServiceInstanceRecord>, ServiceError> {
    let read = cluster.kv_range_prefix_local(&cluster_service_prefix(service))?;
    service_instances_from_entries(&read.value)
}

/// Reconciles the local registry's source-owned instance records against the
/// applied local cluster-KV view. A fully unchanged batch returns `None`
/// without creating a transaction; changed records and records for services
/// removed from the local registry are committed together.
pub async fn publish_local_service_instances(
    cluster: &dyn ClusterAccess,
    registry: &ServiceRegistry,
    node_id: &NodeId,
    registry_version: u64,
    now: OffsetDateTime,
) -> Result<Option<ClusterKvTxnCommitResult>, ServiceError> {
    let records = service_instance_records_from_registry(registry, node_id, registry_version, now);
    let mut existing_by_key: std::collections::BTreeMap<String, KvEntry> = cluster
        .kv_range_prefix_local(CLUSTER_SERVICES_KV_PREFIX)?
        .value
        .into_iter()
        .filter(|entry| service_instance_key_belongs_to_node(&entry.key, node_id))
        .map(|entry| (entry.key.clone(), entry))
        .collect();
    let mut ops = Vec::new();
    for record in records {
        let key = cluster_service_instance_key(&record.service, node_id);
        let unchanged = existing_by_key
            .remove(&key)
            .and_then(|entry| entry.decode::<ServiceInstanceRecord>().ok())
            .is_some_and(|stored| stored.stable_eq(&record));
        if unchanged {
            continue;
        }
        ops.push(KvOp::put_serde(key, &record, None)?);
    }
    ops.extend(existing_by_key.into_keys().map(|key| KvOp::Delete { key }));
    if ops.is_empty() {
        return Ok(None);
    }
    let txn = KvTxn {
        compares: Vec::new(),
        success: ops,
        failure: Vec::new(),
    };
    cluster
        .kv_commit_async(KvTxnRequest::generated(txn))
        .await
        .map(Some)
}

fn service_instance_key_belongs_to_node(key: &str, node_id: &NodeId) -> bool {
    key.strip_prefix(CLUSTER_SERVICES_KV_PREFIX)
        .and_then(|relative| relative.rsplit_once('/'))
        .is_some_and(|(service, owner)| !service.is_empty() && owner == node_id.as_str())
}

/// How [`ServiceTargetSelector`] orders candidate nodes for a service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetSelectionPolicy {
    /// Prefer the local node when it serves the service; otherwise fall back
    /// to `LowestCost` ordering.
    LocalFirst,
    /// Order candidates by the best advertised endpoint (highest priority,
    /// lowest observed latency) from the cluster directory.
    LowestCost,
    /// Rotate through candidates across calls.
    RoundRobin,
    /// Return every registered instance in stable order.
    All,
}

/// Generic routing target selection over discovered service instances.
///
/// The selector ranks *nodes*; delivering to the chosen node is the network
/// layer's job (canonical routing algorithm). Reachability and endpoint cost
/// come from the [`ClusterDirectory`] provider so this crate stays free of
/// transport rules.
pub struct ServiceTargetSelector {
    policy: TargetSelectionPolicy,
    round_robin_cursor: AtomicUsize,
}

impl ServiceTargetSelector {
    pub fn new(policy: TargetSelectionPolicy) -> Self {
        Self {
            policy,
            round_robin_cursor: AtomicUsize::new(0),
        }
    }

    pub fn policy(&self) -> TargetSelectionPolicy {
        self.policy
    }

    /// Orders candidate nodes for `instances` of one service. Unreachable or
    /// unknown nodes are kept (last) rather than dropped: the caller's send
    /// path owns final delivery decisions and failure handling.
    pub fn select_targets(
        &self,
        instances: &[ServiceInstanceRecord],
        directory: &dyn ClusterDirectory,
        local_node_id: &NodeId,
    ) -> Result<Vec<NodeId>, NetworkError> {
        let mut candidates: Vec<NodeId> = instances
            .iter()
            .map(|instance| instance.node_id.clone())
            .collect();
        candidates.sort();
        candidates.dedup();
        if candidates.is_empty() {
            return Ok(candidates);
        }
        match self.policy {
            TargetSelectionPolicy::All => Ok(candidates),
            TargetSelectionPolicy::RoundRobin => {
                let start =
                    self.round_robin_cursor.fetch_add(1, Ordering::Relaxed) % candidates.len();
                candidates.rotate_left(start);
                Ok(candidates)
            }
            TargetSelectionPolicy::LocalFirst => {
                let mut ordered = order_by_endpoint_cost(candidates, directory)?;
                if let Some(position) = ordered
                    .iter()
                    .position(|candidate| candidate == local_node_id)
                {
                    let local = ordered.remove(position);
                    ordered.insert(0, local);
                }
                Ok(ordered)
            }
            TargetSelectionPolicy::LowestCost => order_by_endpoint_cost(candidates, directory),
        }
    }
}

/// Orders nodes by their best advertised endpoint: higher endpoint priority
/// wins, then lower observed latency, then stable node-id order.
fn order_by_endpoint_cost(
    mut candidates: Vec<NodeId>,
    directory: &dyn ClusterDirectory,
) -> Result<Vec<NodeId>, NetworkError> {
    let endpoints = directory.endpoints()?;
    let best: std::collections::BTreeMap<NodeId, (u16, u32)> = candidates
        .iter()
        .map(|node_id| {
            let score = endpoints
                .iter()
                .filter(|endpoint| endpoint.node_id == *node_id)
                .map(|endpoint| {
                    (
                        endpoint.priority,
                        endpoint.last_observed_latency_ms.unwrap_or(u32::MAX),
                    )
                })
                .max_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)))
                .unwrap_or((0, u32::MAX));
            (node_id.clone(), score)
        })
        .collect();
    candidates.sort_by(|left, right| {
        let (lp, ll) = best.get(left).copied().unwrap_or((0, u32::MAX));
        let (rp, rl) = best.get(right).copied().unwrap_or((0, u32::MAX));
        rp.cmp(&lp)
            .then_with(|| ll.cmp(&rl))
            .then_with(|| left.cmp(right))
    });
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Service, ServiceContext, ServiceDisposition, ServiceFuture};
    use sinomo_base::{ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource};
    use sinomo_kv::{
        KvAuthoritativeRead, KvAuthorityBasis, KvAuthorityProof, KvLocalRead, KvRevision,
        KvSnapshot, KvTxnResponse, KvVersion,
    };
    use sinomo_network::ObjectRpcRequest;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    struct FixedDirectory(Vec<ClusterEndpointRecord>);

    struct PublishedService {
        routes: &'static [&'static str],
    }

    impl Service for PublishedService {
        fn name(&self) -> &'static str {
            "dfs"
        }

        fn routes(&self) -> &'static [&'static str] {
            self.routes
        }

        fn handle(&self, _ctx: ServiceContext, _request: ObjectRpcRequest) -> ServiceFuture<'_> {
            Box::pin(async { Ok(ServiceDisposition::Continue) })
        }
    }

    #[derive(Default)]
    struct RecordingClusterAccess {
        entries: Mutex<BTreeMap<String, KvEntry>>,
        commits: Mutex<Vec<KvTxnRequest>>,
    }

    impl RecordingClusterAccess {
        fn with_instances(records: impl IntoIterator<Item = ServiceInstanceRecord>) -> Self {
            let entries = records
                .into_iter()
                .map(|record| {
                    let key = cluster_service_instance_key(&record.service, &record.node_id);
                    let entry = KvEntry {
                        key: key.clone(),
                        value: sinomo_kv::encode_kv_value(&record).expect("instance record"),
                        create_revision: KvRevision(1),
                        modify_revision: KvRevision(1),
                        version: KvVersion(1),
                        lease_id: None,
                    };
                    (key, entry)
                })
                .collect();
            Self {
                entries: Mutex::new(entries),
                commits: Mutex::new(Vec::new()),
            }
        }

        fn committed_requests(&self) -> Vec<KvTxnRequest> {
            self.commits.lock().expect("commit lock").clone()
        }

        fn local_read<T>(&self, value: T) -> KvLocalRead<T> {
            KvLocalRead {
                value,
                revision: KvRevision(1),
                observed_term: 1,
                applied_index: 1,
                known_leader: None,
            }
        }
    }

    impl ClusterAccess for RecordingClusterAccess {
        fn kv_get_local(&self, key: &str) -> Result<KvLocalRead<Option<KvEntry>>, ServiceError> {
            let value = self.entries.lock().expect("entry lock").get(key).cloned();
            Ok(self.local_read(value))
        }

        fn kv_range_prefix_local(
            &self,
            prefix: &str,
        ) -> Result<KvLocalRead<Vec<KvEntry>>, ServiceError> {
            let value = self
                .entries
                .lock()
                .expect("entry lock")
                .values()
                .filter(|entry| entry.key.starts_with(prefix))
                .cloned()
                .collect();
            Ok(self.local_read(value))
        }

        fn kv_get_authoritative_async<'a>(
            &'a self,
            key: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<KvAuthoritativeRead<Option<KvEntry>>, ServiceError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let read = self.kv_get_local(key)?;
                Ok(KvAuthoritativeRead {
                    value: read.value,
                    authority: KvAuthorityProof {
                        term: 1,
                        commit_index: 1,
                        revision: read.revision,
                        basis: KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                })
            })
        }

        fn kv_commit_async<'a>(
            &'a self,
            request: KvTxnRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<ClusterKvTxnCommitResult, ServiceError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let request_id = request.request_id.clone();
                self.commits.lock().expect("commit lock").push(request);
                Ok(ClusterKvTxnCommitResult {
                    request_id,
                    snapshot: KvSnapshot {
                        revision: KvRevision(1),
                        compacted_revision: KvRevision::ZERO,
                        entries: Vec::new(),
                        leases: Vec::new(),
                        events: Vec::new(),
                        committed_requests: Vec::new(),
                        expired_requests: Vec::new(),
                    },
                    txn_response: Some(KvTxnResponse {
                        succeeded: true,
                        revision: KvRevision(1),
                        responses: Vec::new(),
                    }),
                    authority: KvAuthorityProof {
                        term: 1,
                        commit_index: 1,
                        revision: KvRevision(1),
                        basis: KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                    replayed_committed_request: false,
                })
            })
        }
    }

    impl ClusterDirectory for FixedDirectory {
        fn members(&self) -> Result<Vec<sinomo_base::ClusterMemberRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            Ok(self.0.clone())
        }

        fn node_statuses(
            &self,
        ) -> Result<
            std::collections::BTreeMap<NodeId, sinomo_base::ClusterNodeStatusRecord>,
            NetworkError,
        > {
            Ok(std::collections::BTreeMap::new())
        }

        fn connectivity_records(
            &self,
        ) -> Result<Vec<sinomo_network::ClusterConnectivityRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn version(&self) -> Result<sinomo_network::DirectoryVersion, NetworkError> {
            Ok(sinomo_network::DirectoryVersion(0))
        }
    }

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn published_registry(routes: &'static [&'static str]) -> ServiceRegistry {
        ServiceRegistry::builder()
            .register(PublishedService { routes })
            .build()
    }

    fn published_instance(
        node_id: &NodeId,
        routes: impl IntoIterator<Item = &'static str>,
    ) -> ServiceInstanceRecord {
        ServiceInstanceRecord {
            service: "dfs".to_string(),
            node_id: node_id.clone(),
            registry_version: 1,
            routes: routes.into_iter().map(str::to_string).collect(),
            capabilities: Vec::new(),
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn instance(service: &str, node_id: &NodeId) -> ServiceInstanceRecord {
        ServiceInstanceRecord {
            service: service.to_string(),
            node_id: node_id.clone(),
            registry_version: 1,
            routes: vec![format!("/{service}/status")],
            capabilities: Vec::new(),
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn unchanged_source_owned_instances_do_not_submit_a_transaction() {
        let local = node("node_publish_unchanged");
        let registry = published_registry(&["/dfs/read"]);
        let cluster =
            RecordingClusterAccess::with_instances([published_instance(&local, ["/dfs/read"])]);

        let result = publish_local_service_instances(
            &cluster,
            &registry,
            &local,
            2,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await
        .expect("publish unchanged instances");

        assert!(result.is_none());
        assert!(
            cluster.committed_requests().is_empty(),
            "an unchanged applied source-owned record must not produce a Raft transaction"
        );
    }

    #[tokio::test]
    async fn route_removal_commits_the_reconciled_instance_record() {
        let local = node("node_publish_changed");
        let registry = published_registry(&["/dfs/read"]);
        let cluster = RecordingClusterAccess::with_instances([published_instance(
            &local,
            ["/dfs/read", "/dfs/write"],
        )]);

        let result = publish_local_service_instances(
            &cluster,
            &registry,
            &local,
            2,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await
        .expect("publish changed instances");

        assert!(result.is_some());
        let commits = cluster.committed_requests();
        assert_eq!(commits.len(), 1);
        let [KvOp::Put { key, value, .. }] = commits[0].txn.success.as_slice() else {
            panic!("route removal should publish one replacement record");
        };
        assert_eq!(key, &cluster_service_instance_key("dfs", &local));
        let record: ServiceInstanceRecord =
            sinomo_kv::decode_kv_value(value).expect("published instance record");
        assert_eq!(record.routes, vec!["/dfs/read"]);
    }

    #[tokio::test]
    async fn removed_local_service_is_deleted_without_touching_remote_owner() {
        let local = node("node_publish_removed_local");
        let remote = node("node_publish_removed_remote");
        let cluster = RecordingClusterAccess::with_instances([
            published_instance(&local, ["/dfs/read"]),
            published_instance(&remote, ["/dfs/read"]),
        ]);

        let result = publish_local_service_instances(
            &cluster,
            &ServiceRegistry::default(),
            &local,
            2,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await
        .expect("remove local instance");

        assert!(result.is_some());
        let commits = cluster.committed_requests();
        assert_eq!(commits.len(), 1);
        assert_eq!(
            commits[0].txn.success,
            vec![KvOp::Delete {
                key: cluster_service_instance_key("dfs", &local),
            }]
        );
        assert!(commits[0].txn.success.iter().all(|op| match op {
            KvOp::Delete { key } => key != &cluster_service_instance_key("dfs", &remote),
            _ => true,
        }));
    }

    fn endpoint(node_id: &NodeId, priority: u16, latency: Option<u32>) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: sinomo_base::Id::from_legacy_named(&format!("ep_{node_id}_{priority}"))
                .expect("endpoint id"),
            node_id: node_id.clone(),
            transport: sinomo_base::ClusterTransportKind::LanTcp,
            address: "192.0.2.1:1".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority,
            last_observed_latency_ms: latency,
        }
    }

    #[test]
    fn lowest_cost_orders_by_priority_then_latency() {
        let a = node("node_selector_a");
        let b = node("node_selector_b");
        let c = node("node_selector_c");
        let directory = FixedDirectory(vec![
            endpoint(&a, 10, Some(50)),
            endpoint(&b, 90, Some(200)),
            endpoint(&c, 90, Some(20)),
        ]);
        let selector = ServiceTargetSelector::new(TargetSelectionPolicy::LowestCost);
        let ordered = selector
            .select_targets(
                &[
                    instance("dfs", &a),
                    instance("dfs", &b),
                    instance("dfs", &c),
                ],
                &directory,
                &node("node_selector_local"),
            )
            .expect("targets");
        assert_eq!(ordered, vec![c.clone(), b.clone(), a.clone()]);
    }

    #[test]
    fn local_first_puts_local_instance_ahead() {
        let local = node("node_selector_local");
        let remote = node("node_selector_remote");
        let directory = FixedDirectory(vec![
            endpoint(&local, 10, Some(1)),
            endpoint(&remote, 90, Some(1)),
        ]);
        let selector = ServiceTargetSelector::new(TargetSelectionPolicy::LocalFirst);
        let ordered = selector
            .select_targets(
                &[instance("dfs", &remote), instance("dfs", &local)],
                &directory,
                &local,
            )
            .expect("targets");
        assert_eq!(ordered.first(), Some(&local));
    }

    #[test]
    fn round_robin_rotates_between_calls() {
        let a = node("node_selector_rr_a");
        let b = node("node_selector_rr_b");
        let directory = FixedDirectory(Vec::new());
        let selector = ServiceTargetSelector::new(TargetSelectionPolicy::RoundRobin);
        let instances = [instance("dfs", &a), instance("dfs", &b)];
        let first = selector
            .select_targets(&instances, &directory, &a)
            .expect("first");
        let second = selector
            .select_targets(&instances, &directory, &a)
            .expect("second");
        assert_ne!(first.first(), second.first());
    }

    #[test]
    fn instance_key_layout_is_stable() {
        let node_id = node("node_selector_key");
        assert_eq!(
            cluster_service_instance_key("dfs", &node_id),
            format!("/cluster/services/dfs/{node_id}")
        );
    }
}
