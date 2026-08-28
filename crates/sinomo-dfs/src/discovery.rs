//! DFS node discovery and service advertisements stored in the DFS control plane.
//!
//! These records are intentionally compact. They advertise which nodes host DFS
//! metadata and content services; namespace, inode, object, version, and receipt
//! metadata remains in DFS metadata shards. DFS data traffic is routed by
//! `sinomo-network`, so service advertisements carry node identity only.

use crate::control_plane::DfsControlPlaneStore;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::kv::{KvEntry, KvError, encode_kv_value};
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use time::OffsetDateTime;

const DFS_KV_PREFIX: &str = "/dfs";

pub fn metadata_service_key(node_id: &NodeId) -> String {
    format!("{DFS_KV_PREFIX}/services/metadata/{node_id}")
}

pub fn content_service_key(node_id: &NodeId) -> String {
    format!("{DFS_KV_PREFIX}/services/content/{node_id}")
}

pub fn metadata_services_prefix() -> &'static str {
    "/dfs/services/metadata/"
}

pub fn content_services_prefix() -> &'static str {
    "/dfs/services/content/"
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNodeAdvertisement {
    pub node_id: NodeId,
    #[serde(default)]
    pub capacity_weight: u32,
    #[serde(default)]
    pub accepts_metadata: bool,
    #[serde(default)]
    pub accepts_content: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    /// Sanitized aggregate storage state. Physical locations remain node-local
    /// observation data and never become independent replica identities.
    #[serde(default)]
    pub storage_health: crate::DfsHealthState,
    #[serde(default)]
    pub storage_freshness: crate::DfsObservationFreshness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_capacity_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserved_free_bytes: Option<u64>,
    #[serde(default)]
    pub content_location_count: u32,
    #[serde(default)]
    pub network_metering: crate::DfsNetworkMetering,
    #[serde(default)]
    pub advertisement_generation: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    #[serde(
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub expires_at: Option<OffsetDateTime>,
}

impl DfsNodeAdvertisement {
    pub fn metadata(node_id: NodeId) -> Self {
        Self {
            node_id,
            capacity_weight: 1,
            accepts_metadata: true,
            accepts_content: false,
            zone: None,
            storage_health: crate::DfsHealthState::Unknown,
            storage_freshness: crate::DfsObservationFreshness::NeverSampled,
            usable_capacity_bytes: None,
            reserved_free_bytes: None,
            content_location_count: 0,
            network_metering: crate::DfsNetworkMetering::Unknown,
            advertisement_generation: 0,
            updated_at: OffsetDateTime::now_utc(),
            expires_at: None,
        }
    }

    pub fn content(node_id: NodeId) -> Self {
        Self {
            node_id,
            capacity_weight: 1,
            accepts_metadata: false,
            accepts_content: true,
            zone: None,
            storage_health: crate::DfsHealthState::Unknown,
            storage_freshness: crate::DfsObservationFreshness::NeverSampled,
            usable_capacity_bytes: None,
            reserved_free_bytes: None,
            content_location_count: 1,
            network_metering: crate::DfsNetworkMetering::Unknown,
            advertisement_generation: 0,
            updated_at: OffsetDateTime::now_utc(),
            expires_at: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsClusterView {
    pub metadata_nodes: Vec<DfsNodeAdvertisement>,
    pub content_nodes: Vec<DfsNodeAdvertisement>,
}

impl DfsClusterView {
    pub fn eligible_content_node_ids(&self) -> Vec<NodeId> {
        self.content_nodes
            .iter()
            .filter(|node| node.accepts_content)
            .map(|node| node.node_id.clone())
            .collect()
    }
}

impl<'a> DfsControlPlaneStore<'a> {
    pub fn advertise_metadata_service(
        &mut self,
        advertisement: &DfsNodeAdvertisement,
    ) -> Result<(), KvError> {
        self.put_control_value(&metadata_service_key(&advertisement.node_id), advertisement)
    }

    pub fn advertise_content_service(
        &mut self,
        advertisement: &DfsNodeAdvertisement,
    ) -> Result<(), KvError> {
        self.put_control_value(&content_service_key(&advertisement.node_id), advertisement)
    }

    pub fn metadata_service(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<DfsNodeAdvertisement>, KvError> {
        self.get_control_value(&metadata_service_key(node_id))
    }

    pub fn content_service(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<DfsNodeAdvertisement>, KvError> {
        self.get_control_value(&content_service_key(node_id))
    }

    pub fn cluster_view(&self) -> Result<DfsClusterView, KvError> {
        Ok(DfsClusterView {
            metadata_nodes: self.list_control_values(metadata_services_prefix())?,
            content_nodes: self.list_control_values(content_services_prefix())?,
        })
    }

    pub fn cluster_view_for_node_ids(
        &self,
        node_ids: impl IntoIterator<Item = NodeId>,
    ) -> Result<DfsClusterView, KvError> {
        let mut metadata_nodes = Vec::new();
        let mut content_nodes = Vec::new();
        for node_id in node_ids.into_iter().collect::<BTreeSet<_>>() {
            if let Some(advertisement) = self.metadata_service(&node_id)? {
                metadata_nodes.push(advertisement);
            }
            if let Some(advertisement) = self.content_service(&node_id)? {
                content_nodes.push(advertisement);
            }
        }
        Ok(DfsClusterView {
            metadata_nodes,
            content_nodes,
        })
    }

    fn put_control_value<T>(&mut self, key: &str, value: &T) -> Result<(), KvError>
    where
        T: Serialize,
    {
        self.kv_put(key.to_string(), encode_kv_value(value)?)
            .map(|_| ())
    }

    fn get_control_value<T>(&self, key: &str) -> Result<Option<T>, KvError>
    where
        T: DeserializeOwned,
    {
        let Some(entry) = self.kv_get(key)? else {
            return Ok(None);
        };
        entry.decode().map(Some)
    }

    fn list_control_values<T>(&self, prefix: &str) -> Result<Vec<T>, KvError>
    where
        T: DeserializeOwned,
    {
        let mut values = Vec::new();
        for entry in self.kv_range_prefix(prefix)? {
            values.push(entry.decode()?);
        }
        Ok(values)
    }
}

impl<'a> DfsControlPlaneStore<'a> {
    fn kv_get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.kv.get(key)
    }

    fn kv_put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_core::kv::KvRevision, KvError> {
        self.kv.put(key, value)
    }

    fn kv_range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        // The DFS control-plane trait intentionally exposes only the operations
        // needed by production daemon clients. Until daemon range support is
        // passed through here, cluster views can still be built by direct lookup
        // through advertised membership records. In-process tests use the
        // default empty range plus explicit gets.
        self.kv.range_prefix(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_scoped_cluster_view_uses_point_reads_and_deduplicates_nodes() {
        let first = NodeId::from_legacy_named("dfs-discovery-point-first").expect("first node");
        let second = NodeId::from_legacy_named("dfs-discovery-point-second").expect("second node");
        let missing =
            NodeId::from_legacy_named("dfs-discovery-point-missing").expect("missing node");
        let mut kv = sinomo_core::kv::InProcessKvStore::new();
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store
            .advertise_metadata_service(&DfsNodeAdvertisement::metadata(first.clone()))
            .expect("advertise metadata");
        store
            .advertise_content_service(&DfsNodeAdvertisement::content(first.clone()))
            .expect("advertise first content");
        store
            .advertise_content_service(&DfsNodeAdvertisement::content(second.clone()))
            .expect("advertise second content");

        let view = store
            .cluster_view_for_node_ids([second.clone(), first.clone(), first.clone(), missing])
            .expect("member-scoped view");
        assert_eq!(
            view.metadata_nodes
                .iter()
                .map(|node| node.node_id.clone())
                .collect::<Vec<_>>(),
            vec![first.clone()]
        );
        let mut expected_content = vec![first, second];
        expected_content.sort();
        assert_eq!(
            view.content_nodes
                .iter()
                .map(|node| node.node_id.clone())
                .collect::<Vec<_>>(),
            expected_content
        );
    }
}
