//! Service registration records, discovery, and generic routing target
//! selection.
//!
//! The daemon publishes one [`ServiceInstanceRecord`] per locally registered
//! service under `/cluster/services/<service>/<node_id>`. Other nodes discover
//! service instances by reading that prefix from a cluster-KV snapshot and
//! pick a target with [`ServiceTargetSelector`], which consults the network
//! layer's [`ClusterDirectory`] for reachability instead of owning its own
//! routing rules.

use crate::{
    ClusterAccess, ServiceDiscoveryHealth, ServiceDiscoveryLocality, ServiceError, ServiceRegistry,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use sinomo_kv::ClusterKvTxnCommitResult;
use sinomo_kv::{ClusterKvTxnCompactCommitResult, KvEntry, KvOp, KvTxn, KvTxnRequest};
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
#[serde(from = "ServiceInstanceRecordWire")]
pub struct ServiceInstanceRecord {
    #[serde(rename = "service", alias = "service_id")]
    pub service_id: String,
    pub interface_id: String,
    pub interface_version: String,
    pub node_id: NodeId,
    /// Service registry snapshot version that produced this record.
    pub registry_version: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caller_requirements: Vec<String>,
    pub component_id: String,
    pub component_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub component_generation: u64,
    #[serde(default)]
    pub health: ServiceDiscoveryHealth,
    #[serde(default)]
    pub locality: ServiceDiscoveryLocality,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Deserialize)]
struct ServiceInstanceRecordWire {
    #[serde(alias = "service")]
    service_id: String,
    #[serde(default)]
    interface_id: String,
    #[serde(default)]
    interface_version: String,
    node_id: NodeId,
    registry_version: u64,
    #[serde(default)]
    routes: Vec<String>,
    #[serde(default)]
    provider_capabilities: Vec<String>,
    #[serde(default)]
    caller_requirements: Vec<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    component_id: String,
    #[serde(default)]
    component_version: String,
    #[serde(default)]
    artifact_id: Option<String>,
    #[serde(default)]
    component_generation: u64,
    #[serde(default)]
    health: ServiceDiscoveryHealth,
    #[serde(default = "legacy_service_instance_locality")]
    locality: ServiceDiscoveryLocality,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

fn legacy_service_instance_locality() -> ServiceDiscoveryLocality {
    ServiceDiscoveryLocality::Cluster
}

#[derive(Deserialize)]
struct RollingCompatibleServiceInstanceMarker {
    service: String,
}

fn service_instance_record_has_rolling_compatible_wire(value: &[u8]) -> bool {
    sinomo_kv::decode_kv_value::<RollingCompatibleServiceInstanceMarker>(value)
        .is_ok_and(|record| !record.service.is_empty())
}

impl From<ServiceInstanceRecordWire> for ServiceInstanceRecord {
    fn from(wire: ServiceInstanceRecordWire) -> Self {
        let ServiceInstanceRecordWire {
            service_id,
            interface_id,
            interface_version,
            node_id,
            registry_version,
            routes,
            mut provider_capabilities,
            mut caller_requirements,
            capabilities,
            component_id,
            component_version,
            artifact_id,
            component_generation,
            health,
            locality,
            updated_at,
        } = wire;
        // Pre-component discovery used one `capabilities` list for both target
        // selection and route authorization requirements. Preserve both views
        // while old nodes remain in the cluster. Provider capabilities are
        // routing metadata only; the destination still authorizes the caller.
        provider_capabilities.extend(capabilities.iter().cloned());
        provider_capabilities.sort();
        provider_capabilities.dedup();
        caller_requirements.extend(capabilities);
        caller_requirements.sort();
        caller_requirements.dedup();
        Self {
            interface_id: if interface_id.is_empty() {
                format!("sinomo.object-rpc/{service_id}")
            } else {
                interface_id
            },
            interface_version: if interface_version.is_empty() {
                "legacy".to_string()
            } else {
                interface_version
            },
            component_id: if component_id.is_empty() {
                service_id.clone()
            } else {
                component_id
            },
            component_version: if component_version.is_empty() {
                "legacy".to_string()
            } else {
                component_version
            },
            service_id,
            node_id,
            registry_version,
            routes,
            provider_capabilities,
            caller_requirements,
            artifact_id,
            component_generation: component_generation.max(1),
            health,
            locality,
            updated_at,
        }
    }
}

impl ServiceInstanceRecord {
    pub fn new(service_id: impl Into<String>, node_id: NodeId) -> Self {
        let service_id = service_id.into();
        Self {
            interface_id: format!("sinomo.object-rpc/{service_id}"),
            interface_version: "legacy".to_string(),
            component_id: service_id.clone(),
            component_version: "legacy".to_string(),
            service_id,
            node_id,
            registry_version: 1,
            routes: Vec::new(),
            provider_capabilities: Vec::new(),
            caller_requirements: Vec::new(),
            artifact_id: None,
            component_generation: 1,
            health: ServiceDiscoveryHealth::Ready,
            locality: ServiceDiscoveryLocality::Cluster,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn stable_eq(&self, other: &Self) -> bool {
        self.service_id == other.service_id
            && self.interface_id == other.interface_id
            && self.interface_version == other.interface_version
            && self.node_id == other.node_id
            && self.routes == other.routes
            && self.provider_capabilities == other.provider_capabilities
            && self.caller_requirements == other.caller_requirements
            && self.component_id == other.component_id
            && self.component_version == other.component_version
            && self.artifact_id == other.artifact_id
            && self.component_generation == other.component_generation
            && self.health == other.health
            && self.locality == other.locality
    }
}

/// Builds the instance records for every service in a local registry.
pub fn service_instance_records_from_registry(
    registry: &ServiceRegistry,
    node_id: &NodeId,
    registry_version: u64,
    now: OffsetDateTime,
) -> Vec<ServiceInstanceRecord> {
    let mut by_service: std::collections::BTreeMap<String, ServiceInstanceRecord> =
        std::collections::BTreeMap::new();
    for route in registry.route_categories() {
        let identity = route
            .discovery
            .clone()
            .unwrap_or_else(|| crate::ServiceDiscoveryIdentity {
                service_id: route.category.clone(),
                interface_id: format!("sinomo.object-rpc/{}", route.category),
                interface_version: "legacy".to_string(),
                provider_capabilities: Vec::new(),
                caller_requirements: route.required_capabilities.clone(),
                component_id: registry
                    .service_name_for_route(&route.route)
                    .unwrap_or("legacy")
                    .to_string(),
                component_version: "legacy".to_string(),
                artifact_id: None,
                component_generation: 1,
                health: ServiceDiscoveryHealth::Ready,
                locality: ServiceDiscoveryLocality::Cluster,
            });
        let entry = by_service
            .entry(identity.service_id.clone())
            .or_insert_with(|| ServiceInstanceRecord {
                service_id: identity.service_id.clone(),
                interface_id: identity.interface_id.clone(),
                interface_version: identity.interface_version.clone(),
                node_id: node_id.clone(),
                registry_version,
                routes: Vec::new(),
                provider_capabilities: identity.provider_capabilities.clone(),
                caller_requirements: identity.caller_requirements.clone(),
                component_id: identity.component_id.clone(),
                component_version: identity.component_version.clone(),
                artifact_id: identity.artifact_id.clone(),
                component_generation: identity.component_generation,
                health: identity.health,
                locality: identity.locality,
                updated_at: now,
            });
        entry.routes.push(route.route.clone());
        for capability in &identity.provider_capabilities {
            if !entry.provider_capabilities.contains(capability) {
                entry.provider_capabilities.push(capability.clone());
            }
        }
        for requirement in &identity.caller_requirements {
            if !entry.caller_requirements.contains(requirement) {
                entry.caller_requirements.push(requirement.clone());
            }
        }
    }
    let mut records: Vec<ServiceInstanceRecord> = by_service.into_values().collect();
    for record in &mut records {
        record.routes.sort();
        record.routes.dedup();
        record.provider_capabilities.sort();
        record.provider_capabilities.dedup();
        record.caller_requirements.sort();
        record.caller_requirements.dedup();
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
        left.service_id
            .cmp(&right.service_id)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    Ok(records)
}

/// Keeps only service instances which advertise `capability`.
///
/// Capabilities in discovery records describe provider operations. They are
/// routing metadata, not caller authorization grants; the selected route is
/// still authorized normally by the destination node.
pub fn service_instances_providing_capability(
    instances: impl IntoIterator<Item = ServiceInstanceRecord>,
    capability: &str,
) -> Vec<ServiceInstanceRecord> {
    let mut providers = instances
        .into_iter()
        .filter(|instance| {
            instance
                .provider_capabilities
                .iter()
                .any(|provided| provided == capability)
        })
        .collect::<Vec<_>>();
    providers.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.service_id.cmp(&right.service_id))
    });
    providers
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
) -> Result<Option<ClusterKvTxnCompactCommitResult>, ServiceError> {
    let records = service_instance_records_from_registry(registry, node_id, registry_version, now);
    let existing = cluster
        .kv_range_prefix_local(CLUSTER_SERVICES_KV_PREFIX)?
        .value;
    let mut existing_by_key: std::collections::BTreeMap<String, KvEntry> = existing
        .into_iter()
        .filter(|entry| service_instance_key_belongs_to_node(&entry.key, node_id))
        .map(|entry| (entry.key.clone(), entry))
        .collect();
    let mut ops = Vec::new();
    for record in records {
        let key = cluster_service_instance_key(&record.service_id, node_id);
        let unchanged = existing_by_key.remove(&key).is_some_and(|entry| {
            service_instance_record_has_rolling_compatible_wire(&entry.value)
                && entry
                    .decode::<ServiceInstanceRecord>()
                    .is_ok_and(|stored| stored.stable_eq(&record))
        });
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
        .kv_commit_compact_async(KvTxnRequest::generated(txn))
        .await
        .map(Some)
}

/// Whether `key` is one of `node_id`'s service instance records.
pub fn service_instance_key_belongs_to_node(key: &str, node_id: &NodeId) -> bool {
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
                    let key = cluster_service_instance_key(&record.service_id, &record.node_id);
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
                ..Default::default()
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
                let command_digest = request
                    .clone()
                    .into_command_request()
                    .digest()
                    .expect("transaction digest");
                self.commits.lock().expect("commit lock").push(request);
                Ok(ClusterKvTxnCommitResult {
                    request_id,
                    command_digest,
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
        let mut instance = ServiceInstanceRecord::new("dfs", node_id.clone());
        instance.routes = routes.into_iter().map(str::to_string).collect();
        instance
    }

    fn instance(service: &str, node_id: &NodeId) -> ServiceInstanceRecord {
        let mut instance = ServiceInstanceRecord::new(service, node_id.clone());
        instance.routes = vec![format!("/{service}/status")];
        instance
    }

    #[test]
    fn legacy_instance_records_decode_into_component_discovery_identity() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct LegacyServiceInstanceRecord {
            service: String,
            node_id: NodeId,
            registry_version: u64,
            #[serde(default)]
            routes: Vec<String>,
            #[serde(default)]
            capabilities: Vec<String>,
            #[serde(with = "time::serde::rfc3339")]
            updated_at: OffsetDateTime,
        }

        let node_id = node("legacy-service-instance-node");
        let legacy = LegacyServiceInstanceRecord {
            service: "dfs".to_string(),
            node_id: node_id.clone(),
            registry_version: 7,
            routes: vec!["/dfs/status".to_string()],
            capabilities: vec!["cluster.dfs.read".to_string()],
            updated_at: OffsetDateTime::UNIX_EPOCH,
        };
        let encoded = sinomo_kv::encode_kv_value(&legacy).expect("encode legacy record");
        let decoded: ServiceInstanceRecord =
            sinomo_kv::decode_kv_value(&encoded).expect("decode legacy record");

        assert_eq!(decoded.service_id, "dfs");
        assert_eq!(decoded.interface_id, "sinomo.object-rpc/dfs");
        assert_eq!(decoded.interface_version, "legacy");
        assert_eq!(decoded.node_id, node_id);
        assert_eq!(decoded.registry_version, 7);
        assert_eq!(decoded.routes, ["/dfs/status"]);
        assert_eq!(decoded.provider_capabilities, ["cluster.dfs.read"]);
        assert_eq!(decoded.caller_requirements, ["cluster.dfs.read"]);
        assert_eq!(decoded.component_id, "dfs");
        assert_eq!(decoded.component_version, "legacy");
        assert_eq!(decoded.component_generation, 1);
        assert_eq!(decoded.health, crate::ServiceDiscoveryHealth::Ready);
        assert_eq!(decoded.locality, crate::ServiceDiscoveryLocality::Cluster);

        let current = ServiceInstanceRecord::new("dfs", node("current-service-instance-node"));
        let encoded = sinomo_kv::encode_kv_value(&current).expect("encode current record");
        let decoded: LegacyServiceInstanceRecord =
            sinomo_kv::decode_kv_value(&encoded).expect("decode current record with legacy schema");
        assert_eq!(decoded.service, "dfs");
        assert!(service_instance_record_has_rolling_compatible_wire(
            &encoded
        ));

        #[derive(serde::Serialize)]
        struct ServiceIdOnlyMarker<'a> {
            service_id: &'a str,
        }
        let noncanonical = sinomo_kv::encode_kv_value(&ServiceIdOnlyMarker { service_id: "dfs" })
            .expect("encode service-id-only marker");
        assert!(!service_instance_record_has_rolling_compatible_wire(
            &noncanonical
        ));
    }

    #[test]
    fn capability_filter_returns_only_provider_instances_in_stable_node_order() {
        let first = node("node_capability_first");
        let second = node("node_capability_second");
        let unrelated = node("node_capability_unrelated");
        let mut first_provider = instance("daemon", &first);
        first_provider.provider_capabilities = vec!["sifs.path.access".to_string()];
        let mut second_provider = instance("dfs", &second);
        second_provider.provider_capabilities = vec![
            "dfs.object.transfer".to_string(),
            "sifs.path.access".to_string(),
        ];

        let providers = service_instances_providing_capability(
            [
                second_provider.clone(),
                instance("daemon", &unrelated),
                first_provider.clone(),
            ],
            "sifs.path.access",
        );

        let mut expected = vec![first_provider, second_provider];
        expected.sort_by(|left, right| {
            left.node_id
                .cmp(&right.node_id)
                .then_with(|| left.service_id.cmp(&right.service_id))
        });
        assert_eq!(providers, expected);
    }

    #[test]
    fn publication_preserves_explicit_interface_provider_authority_and_generation_identity() {
        let route = crate::ServiceRoute::new("/fixture/echo", "legacy-category")
            .require_capability("fixture.echo.invoke")
            .with_discovery_identity(crate::ServiceDiscoveryIdentity {
                service_id: "external-echo".to_string(),
                interface_id: "sinomo.fixture.echo".to_string(),
                interface_version: "1.2.0".to_string(),
                provider_capabilities: vec!["echo.streaming".to_string()],
                caller_requirements: vec!["fixture.echo.invoke".to_string()],
                component_id: "fixture.external.echo".to_string(),
                component_version: "2.0.0".to_string(),
                artifact_id: Some("artifact.echo".to_string()),
                component_generation: 7,
                health: crate::ServiceDiscoveryHealth::Degraded,
                locality: crate::ServiceDiscoveryLocality::Cluster,
            });
        let registry = ServiceRegistry::builder()
            .register_dynamic("external-echo", route, |_, _| async {
                Ok(ServiceDisposition::Continue)
            })
            .build();
        let records = service_instance_records_from_registry(
            &registry,
            &node("node_explicit_service_identity"),
            9,
            OffsetDateTime::UNIX_EPOCH,
        );
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.service_id, "external-echo");
        assert_eq!(record.interface_id, "sinomo.fixture.echo");
        assert_eq!(record.interface_version, "1.2.0");
        assert_eq!(record.provider_capabilities, ["echo.streaming"]);
        assert_eq!(record.caller_requirements, ["fixture.echo.invoke"]);
        assert_eq!(record.component_id, "fixture.external.echo");
        assert_eq!(record.artifact_id.as_deref(), Some("artifact.echo"));
        assert_eq!(record.component_generation, 7);
        assert_eq!(record.health, crate::ServiceDiscoveryHealth::Degraded);
        assert_eq!(record.locality, crate::ServiceDiscoveryLocality::Cluster);
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
            endpoint_id: sinomo_base::Id::from_legacy_named(format!("ep_{node_id}_{priority}"))
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
