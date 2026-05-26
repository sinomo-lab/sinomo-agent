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

pub trait DfsControlPlaneKvClient: std::fmt::Debug + Send {
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
    C: DfsControlPlaneKvClient + Send,
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

    pub fn list_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>, KvError> {
        let mut shards: Vec<DfsMetadataShardConfig> = self
            .kv
            .range_prefix(&format!("{}/", metadata_shards_prefix(file_system_id)))?
            .into_iter()
            .map(|entry| entry.decode())
            .collect::<Result<Vec<_>, _>>()?;
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        Ok(shards)
    }

    pub fn select_metadata_shard_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_for_path(self.list_metadata_shards(file_system_id)?, path)
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

fn select_metadata_shard_for_path(
    shards: Vec<DfsMetadataShardConfig>,
    path: &str,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    let path = normalize_metadata_selection_path(path)?;
    let path_hash = metadata_name_hash(&path);
    let mut root = None;
    let mut prefix_match = None::<(usize, String, DfsMetadataShardConfig)>;
    let mut hash_match = None::<(u128, String, DfsMetadataShardConfig)>;

    for shard in shards {
        match &shard.assignment {
            DfsMetadataShardAssignment::Root => {
                if root
                    .as_ref()
                    .is_none_or(|current: &DfsMetadataShardConfig| {
                        shard.shard_id < current.shard_id
                    })
                {
                    root = Some(shard);
                }
            }
            DfsMetadataShardAssignment::PathPrefix { prefix } => {
                let prefix = normalize_metadata_selection_path(prefix)?;
                if metadata_path_matches_prefix(&path, &prefix) {
                    let candidate = (prefix.len(), shard.shard_id.clone(), shard);
                    if prefix_match.as_ref().is_none_or(|current| {
                        candidate.0 > current.0
                            || (candidate.0 == current.0 && candidate.1 < current.1)
                    }) {
                        prefix_match = Some(candidate);
                    }
                }
            }
            DfsMetadataShardAssignment::NameHashRange {
                start_inclusive,
                end_exclusive,
            } => {
                if metadata_hash_in_range(path_hash, *start_inclusive, *end_exclusive) {
                    let candidate = (
                        metadata_hash_range_width(*start_inclusive, *end_exclusive),
                        shard.shard_id.clone(),
                        shard,
                    );
                    if hash_match
                        .as_ref()
                        .is_none_or(|current| candidate.0 < current.0 || candidate.1 < current.1)
                    {
                        hash_match = Some(candidate);
                    }
                }
            }
        }
    }

    Ok(prefix_match
        .map(|(_, _, shard)| shard)
        .or_else(|| hash_match.map(|(_, _, shard)| shard))
        .or(root))
}

fn normalize_metadata_selection_path(path: &str) -> Result<String, KvError> {
    if path.bytes().any(|byte| byte == 0) {
        return Err(KvError::Serialization {
            detail: format!("metadata shard path contains a nul byte: {path:?}"),
        });
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn metadata_path_matches_prefix(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn metadata_name_hash(path: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in path.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn metadata_hash_in_range(hash: u64, start_inclusive: u64, end_exclusive: u64) -> bool {
    if start_inclusive == end_exclusive {
        return false;
    }
    if start_inclusive < end_exclusive {
        hash >= start_inclusive && hash < end_exclusive
    } else {
        hash >= start_inclusive || hash < end_exclusive
    }
}

fn metadata_hash_range_width(start_inclusive: u64, end_exclusive: u64) -> u128 {
    if start_inclusive <= end_exclusive {
        u128::from(end_exclusive - start_inclusive)
    } else {
        (u128::from(u64::MAX) - u128::from(start_inclusive) + 1) + u128::from(end_exclusive)
    }
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

    #[test]
    fn metadata_shard_selection_prefers_longest_path_prefix() {
        let node_owner = node("node_shard_select_owner");
        let node_replica = node("node_shard_select_replica");
        let mut kv = InProcessRaftKvCluster::new(vec![node_owner.clone(), node_replica.clone()])
            .expect("kv cluster");
        let file_system_id = file_system_id();
        let mut control = DfsControlPlaneStore::new(&mut kv);
        for shard in [
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: vec![node_replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: vec![node_replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services-sandbox".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: vec![node_replica],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services/sandbox".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ] {
            control.put_metadata_shard(&shard).expect("put shard");
        }

        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/sandbox/instances/one")
                .expect("select sandbox")
                .map(|shard| shard.shard_id),
            Some("services-sandbox".to_string())
        );
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/api")
                .expect("select services")
                .map(|shard| shard.shard_id),
            Some("services".to_string())
        );
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services2/api")
                .expect("select root")
                .map(|shard| shard.shard_id),
            Some("root".to_string())
        );
    }

    #[test]
    fn metadata_shard_selection_uses_hash_range_before_root() {
        let node_owner = node("node_shard_hash_owner");
        let node_replica = node("node_shard_hash_replica");
        let mut kv = InProcessRaftKvCluster::new(vec![node_owner.clone(), node_replica.clone()])
            .expect("kv cluster");
        let file_system_id = file_system_id();
        let selected_path = "/users/alice/project";
        let hash = metadata_name_hash(selected_path);
        let (start_inclusive, end_exclusive) = if hash == u64::MAX {
            (hash, 0)
        } else {
            (hash, hash + 1)
        };
        let mut control = DfsControlPlaneStore::new(&mut kv);
        for shard in [
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: vec![node_replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "hash-selected".to_string(),
                primary_node_id: node_owner,
                replica_node_ids: vec![node_replica],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive,
                    end_exclusive,
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ] {
            control.put_metadata_shard(&shard).expect("put shard");
        }

        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, selected_path)
                .expect("select hash")
                .map(|shard| shard.shard_id),
            Some("hash-selected".to_string())
        );
    }

    #[test]
    fn metadata_shard_selection_rejects_nul_paths() {
        let node_owner = node("node_shard_invalid_owner");
        let mut kv = InProcessRaftKvCluster::new(vec![node_owner.clone()]).expect("kv cluster");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_owner,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control.put_metadata_shard(&shard).expect("put shard");

        assert!(matches!(
            control.select_metadata_shard_for_path(&file_system_id, "/bad\0path"),
            Err(KvError::Serialization { .. })
        ));
    }
}
