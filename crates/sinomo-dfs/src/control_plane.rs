//! Small DFS control-plane records stored in `sinomo-kv`.
//!
//! This module intentionally does not store per-file or per-object metadata.
//! `sinomo-kv` is for compact, cluster-wide configuration such as DfsFileSystem
//! roots and metadata-shard placement. File/object namespace metadata belongs
//! in the DFS metadata service and its sharded local stores.

use crate::DfsFileSystemId;
use crate::{DfsFileSystem, DfsSubtreePolicy};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::DfsObjectPolicy;
use sinomo_kv::{
    InProcessRaftKvCluster, KvCompare, KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse,
};
use sinomo_node::NodeId;
use std::sync::{Arc, Mutex};

const DFS_KV_PREFIX: &str = "/dfs";

pub fn file_system_config_key(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/config")
}

pub fn metadata_shards_prefix(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-shards")
}

pub fn metadata_shard_key(file_system_id: &DfsFileSystemId, shard_id: &str) -> String {
    format!(
        "{}/{}",
        metadata_shards_prefix(file_system_id),
        normalize_segment(shard_id)
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileSystemConfig {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
}

impl From<&DfsFileSystem> for DfsFileSystemConfig {
    fn from(file_system: &DfsFileSystem) -> Self {
        Self {
            file_system_id: file_system.file_system_id.clone(),
            display_name: file_system.display_name.clone(),
            owner_node_id: file_system.owner_node_id.clone(),
            subtree_policies: file_system.subtree_policies.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardConfig {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub primary_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_node_ids: Vec<NodeId>,
    pub epoch: u64,
    pub assignment: DfsMetadataShardAssignment,
    pub default_policy: DfsObjectPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataShardAssignment {
    Root,
    PathPrefix {
        prefix: String,
    },
    NameHashRange {
        start_inclusive: u64,
        end_exclusive: u64,
    },
}

pub trait DfsControlPlaneKvClient: std::fmt::Debug {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError>;
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError>;
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError>;
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError>;
}

#[derive(Clone, Debug)]
pub struct SharedDfsControlPlaneKvClient<C> {
    inner: Arc<Mutex<C>>,
}

impl<C> SharedDfsControlPlaneKvClient<C> {
    pub fn new(client: C) -> Self {
        Self {
            inner: Arc::new(Mutex::new(client)),
        }
    }

    pub fn with_client<R>(&self, f: impl FnOnce(&mut C) -> R) -> R {
        let mut client = self.inner.lock().expect("control-plane KV client lock");
        f(&mut client)
    }
}

impl<C> DfsControlPlaneKvClient for SharedDfsControlPlaneKvClient<C>
where
    C: DfsControlPlaneKvClient,
{
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .get(key)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .range_prefix(prefix)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .put(key, value)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .txn(txn)
    }
}

impl DfsControlPlaneKvClient for InProcessRaftKvCluster {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        let leader = self
            .store(self.leader_id())
            .ok_or_else(|| KvError::UnknownNode {
                node_id: self.leader_id().clone(),
            })?;
        Ok(leader.get(key).cloned())
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        let leader = self
            .store(self.leader_id())
            .ok_or_else(|| KvError::UnknownNode {
                node_id: self.leader_id().clone(),
            })?;
        Ok(leader.range_prefix(prefix))
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        InProcessRaftKvCluster::put(self, key, value)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        InProcessRaftKvCluster::txn_response(self, txn)
    }
}

pub struct DfsControlPlaneStore<'a> {
    pub(crate) kv: &'a mut dyn DfsControlPlaneKvClient,
}

impl<'a> DfsControlPlaneStore<'a> {
    pub fn new(kv: &'a mut dyn DfsControlPlaneKvClient) -> Self {
        Self { kv }
    }

    pub fn put_file_system_config(&mut self, config: &DfsFileSystemConfig) -> Result<(), KvError> {
        self.put_serde(&file_system_config_key(&config.file_system_id), config)
    }

    pub fn get_file_system_config(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystemConfig>, KvError> {
        self.get_serde(&file_system_config_key(file_system_id))
    }

    pub fn put_metadata_shard(&mut self, shard: &DfsMetadataShardConfig) -> Result<(), KvError> {
        self.put_serde(
            &metadata_shard_key(&shard.file_system_id, &shard.shard_id),
            shard,
        )
    }

    pub fn get_metadata_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        self.get_serde(&metadata_shard_key(file_system_id, shard_id))
    }

    pub fn transfer_metadata_shard_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        expected_epoch: u64,
        new_primary_node_id: NodeId,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        let key = metadata_shard_key(file_system_id, shard_id);
        let Some(entry) = self.kv.get(&key)? else {
            return Ok(None);
        };
        let mut current: DfsMetadataShardConfig = entry.decode()?;
        if current.epoch != expected_epoch {
            return Ok(None);
        }

        current.primary_node_id = new_primary_node_id;
        current.epoch = current.epoch.saturating_add(1);
        let response = self.kv.txn(KvTxn {
            compares: vec![KvCompare::ValueEquals {
                key: key.clone(),
                value: entry.value,
            }],
            success: vec![KvOp::put_serde(key, &current, None)?],
            failure: Vec::new(),
        })?;

        Ok(response.succeeded.then_some(current))
    }

    fn put_serde<T>(&mut self, key: &str, value: &T) -> Result<(), KvError>
    where
        T: Serialize,
    {
        self.kv
            .put(key.to_string(), sinomo_kv::encode_kv_value(value)?)?;
        Ok(())
    }

    fn get_serde<T>(&self, key: &str) -> Result<Option<T>, KvError>
    where
        T: DeserializeOwned,
    {
        let Some(entry) = self.kv.get(key)? else {
            return Ok(None);
        };
        entry.decode().map(Some)
    }
}

fn normalize_segment(segment: &str) -> String {
    segment.trim_matches('/').replace('/', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
    }

    #[test]
    fn key_helpers_use_control_plane_namespace() {
        let file_system_id = file_system_id();

        assert_eq!(
            file_system_config_key(&file_system_id),
            format!("/dfs/filesystems/{file_system_id}/config")
        );
        assert_eq!(
            metadata_shard_key(&file_system_id, "root/frag"),
            format!("/dfs/filesystems/{file_system_id}/metadata-shards/root_frag")
        );
    }

    #[test]
    fn store_puts_and_gets_only_control_plane_records() {
        let node_owner = node("node_owner");
        let node_replica = node("node_replica");
        let mut kv = InProcessRaftKvCluster::new(vec![
            node_owner.clone(),
            node_replica.clone(),
            node("node_witness"),
        ])
        .expect("kv cluster");
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node_owner.clone(),
            subtree_policies: vec![DfsSubtreePolicy {
                path_prefix: ".".to_string(),
                policy: DfsObjectPolicy::ReplicatedEventual,
            }],
        };
        let file_system_config = DfsFileSystemConfig::from(&file_system);
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_owner.clone(),
            replica_node_ids: vec![node_replica],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };

        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&file_system_config)
            .expect("put filesystem config");
        control
            .put_metadata_shard(&shard)
            .expect("put shard config");

        assert_eq!(
            control
                .get_file_system_config(&file_system.file_system_id)
                .expect("get filesystem config"),
            Some(file_system_config)
        );
        assert_eq!(
            control
                .get_metadata_shard(&file_system.file_system_id, "root")
                .expect("get shard config"),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_uses_epoch_compare() {
        let node_owner = node("node_owner");
        let node_replica = node("node_replica");
        let mut kv = InProcessRaftKvCluster::new(vec![
            node_owner.clone(),
            node_replica.clone(),
            node("node_witness"),
        ])
        .expect("kv cluster");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_owner.clone(),
            replica_node_ids: vec![node_replica.clone()],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control.put_metadata_shard(&shard).expect("put shard");

        let transferred = control
            .transfer_metadata_shard_primary(&file_system_id, "root", 7, node_replica.clone())
            .expect("transfer")
            .expect("epoch match");
        assert_eq!(transferred.primary_node_id, node_replica);
        assert_eq!(transferred.epoch, 8);
        assert!(
            control
                .transfer_metadata_shard_primary(&file_system_id, "root", 7, node_owner)
                .expect("stale transfer")
                .is_none()
        );
    }
}
