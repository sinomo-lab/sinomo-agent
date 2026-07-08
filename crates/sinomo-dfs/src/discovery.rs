//! DFS node discovery and service advertisements stored in the DFS control plane.
//!
//! These records are intentionally compact. They advertise which nodes host DFS
//! metadata and content services; namespace, inode, object, version, and receipt
//! metadata remains in DFS metadata shards. DFS data traffic is routed by
//! `sinomo-network`, so the advertisement endpoint is a logical node endpoint
//! rather than a concrete dial target.

use crate::control_plane::DfsControlPlaneStore;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::kv::{KvEntry, KvError, encode_kv_value};
use sinomo_core::node::NodeId;
use time::OffsetDateTime;

const DFS_KV_PREFIX: &str = "/dfs";
const DFS_CLUSTER_NODE_ENDPOINT_SCHEME: &str = "cluster-node";

pub fn dfs_cluster_node_endpoint(node_id: &NodeId) -> String {
    format!("{DFS_CLUSTER_NODE_ENDPOINT_SCHEME}://{node_id}")
}

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
    pub endpoint: String,
    #[serde(default)]
    pub capacity_weight: u32,
    #[serde(default)]
    pub accepts_metadata: bool,
    #[serde(default)]
    pub accepts_content: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl DfsNodeAdvertisement {
    pub fn metadata(node_id: NodeId, endpoint: impl Into<String>) -> Self {
        Self {
            node_id,
            endpoint: endpoint.into(),
            capacity_weight: 1,
            accepts_metadata: true,
            accepts_content: false,
            zone: None,
            updated_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn content(node_id: NodeId, endpoint: impl Into<String>) -> Self {
        Self {
            node_id,
            endpoint: endpoint.into(),
            capacity_weight: 1,
            accepts_metadata: false,
            accepts_content: true,
            zone: None,
            updated_at: OffsetDateTime::now_utc(),
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
